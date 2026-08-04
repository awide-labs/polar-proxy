#!/usr/bin/env bash
# TAP test for PolarDB config-file round-trip fields.
# Covers:
#   - pgsql_query_rules.replica_eligible is saved/loaded without shifting log/apply/comment.
#   - pgsql_replication_hostgroups PolarDB policy fields are saved/loaded.
#   - txn_split_enabled is persisted and accepted only for PolarDB pairs.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"
PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_test_polardb_config_roundtrip")}"
PROXYSQL_CONFIG_FILE="${PROXYSQL_CONFIG_FILE:-$PROXYSQL_DATA_DIR/proxysql.cnf}"
PROXYSQL_START_LOG="${PROXYSQL_START_LOG:-${PROXYSQL_DATA_DIR}.start.log}"
PROXYSQL_TXN_SPLIT_CHECK_LOG="${PROXYSQL_TXN_SPLIT_CHECK_LOG:-${PROXYSQL_DATA_DIR}.txn_split_check.log}"

PLAN=77
FAIL=0
STARTED_PROXY=0

# admin_sql, counter, proxy_sql and global/runtime variable accessors come from
# lib/tap_polardb.sh. The helper below is config-specific because it uses the
# MySQL admin surface.
mysql_admin_sql() {
	env MYSQL_PWD="$PROXYSQL_MYSQL_ADMIN_PASSWORD" mysql -h "$PROXYSQL_HOST" \
		-P "$PROXYSQL_MYSQL_ADMIN_PORT" -u "$PROXYSQL_MYSQL_ADMIN_USER" \
		--batch --skip-column-names -e "$1"
}

# Return the complete profile-owned policy in a stable, human-readable order.
# The table name is fixed by this test and is never supplied externally.
policy_bundle() {
	local table="$1"
	admin_sql "SELECT
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_profile' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_consistency_mode' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_read_target' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_action_read_fallback' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_action_missing_lsn' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_action_lsn_timeout' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_action_replica_error' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_action_replica_loss' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_proxy_protocol' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_monitor_lsn_updates' THEN variable_value END), '') || '|' ||
		coalesce(max(CASE WHEN variable_name='pgsql-polardb_lazy_warmup_split' THEN variable_value END), '')
		FROM ${table}
		WHERE variable_name LIKE 'pgsql-polardb%';" | tr -d '\r[:space:]'
}

cleanup() {
	if [ "$STARTED_PROXY" -eq 1 ]; then
		PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop --data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

plan "$PLAN"

polardb_require_proxysql_or_skip_all polardb
polardb_require_command_or_skip_all psql psql
polardb_require_command_or_skip_all mysql "mysql client"

if PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" start \
	--data-dir "$PROXYSQL_DATA_DIR" \
	--admin-port "$PROXYSQL_ADMIN_PORT" \
	--proxy-port "$PROXYSQL_PORT" \
	--mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT" >"$PROXYSQL_START_LOG" 2>&1; then
	STARTED_PROXY=1
	ok 0 "start ProxySQL for config round-trip"
else
	diag "ProxySQL start failed; log follows"
	sed 's/^/# /' "$PROXYSQL_START_LOG" 2>/dev/null || true
	ok 1 "start ProxySQL for config round-trip"
	for _ in $(seq 2 "$PLAN"); do ok 1 "dependent check skipped after start failure"; done
	exit 1
fi

admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, multiplex, replica_eligible, log, apply, comment) VALUES (9201, 1, '^SELECT t1', 0, -1, 0, 0, 're_minus_one');" >/dev/null
admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, multiplex, replica_eligible, log, apply, comment) VALUES (9202, 1, '^SELECT t2', 1, 0, 1, 0, 're_zero');" >/dev/null
admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, multiplex, replica_eligible, log, apply, comment) VALUES (9203, 1, '^SELECT t3', 2, 1, 0, 1, 're_one');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
admin_sql "LOAD PGSQL QUERY RULES FROM CONFIG;" >/dev/null

re_values=$(admin_sql "SELECT group_concat(replica_eligible, ',') FROM (SELECT replica_eligible FROM pgsql_query_rules WHERE rule_id BETWEEN 9201 AND 9203 ORDER BY rule_id);" | tr -d '[:space:]')
[ "$re_values" = "-1,0,1" ]
ok $? "query-rule replica_eligible survives config round-trip"

rule_fields=$(admin_sql "SELECT group_concat(log || ':' || apply || ':' || comment, '|') FROM (SELECT log, apply, comment FROM pgsql_query_rules WHERE rule_id BETWEEN 9201 AND 9203 ORDER BY rule_id);" | tr -d '\r')
[ "$rule_fields" = "0:0:re_minus_one|1:0:re_zero|0:1:re_one" ]
ok $? "query-rule fields after replica_eligible are not shifted"

admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
cluster_qr=$(mysql_admin_sql "PROXY_SELECT rule_id, username, database, flagIN, client_addr, proxy_addr, proxy_port, digest, match_digest, match_pattern, negate_match_pattern, re_modifiers, flagOUT, replace_pattern, destination_hostgroup, cache_ttl, cache_empty_result, cache_timeout, reconnect, timeout, retries, delay, next_query_flagIN, mirror_flagOUT, mirror_hostgroup, error_msg, ok_msg, sticky_conn, multiplex, replica_eligible, log, apply, attributes, comment FROM runtime_pgsql_query_rules ORDER BY rule_id" |
	awk -F'\t' '$1 >= 9201 && $1 <= 9203 { if (out != "") out = out "|"; out = out $30 ":" $31 ":" $32 ":" $34 } END { print out }' |
	tr -d '\r')
[ "$cluster_qr" = "-1:0:0:re_minus_one|0:1:0:re_zero|1:0:1:re_one" ]
ok $? "cluster query-rule surface includes replica_eligible without shifting fields"

admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (101, 102, 'polardb', 1, 'global_lsn', 12345, 0, 'v15_wait', 'polardb_hg_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null

hg_policy=$(admin_sql "SELECT check_type || '|' || txn_split_enabled || '|' || consistency_mode || '|' || max_lag_bytes || '|' || lsn_wait_timeout_ms || '|' || proxy_protocol || '|' || comment FROM pgsql_replication_hostgroups WHERE writer_hostgroup=101 AND reader_hostgroup=102;" | tr -d '\r')
[ "$hg_policy" = "polardb|1|global_lsn|12345|0|v15_wait|polardb_hg_roundtrip" ]
ok $? "PolarDB replication-hostgroup policy and v15_wait survive config round-trip"

admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (103, 104, 'read_only', 0, 'default', -1, -1, 'default', 'read_only_hg_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null

read_only_policy=$(admin_sql "SELECT check_type || '|' || txn_split_enabled || '|' || consistency_mode || '|' || max_lag_bytes || '|' || lsn_wait_timeout_ms || '|' || proxy_protocol || '|' || comment FROM pgsql_replication_hostgroups WHERE writer_hostgroup=103 AND reader_hostgroup=104;" | tr -d '\r')
[ "$read_only_policy" = "read_only|0|default|-1|-1|default|read_only_hg_roundtrip" ]
ok $? "read_only replication-hostgroup defaults survive config round-trip"

if admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, comment) VALUES (107, 108, 'read_only', 1, 'invalid_split_non_polardb');" >"$PROXYSQL_TXN_SPLIT_CHECK_LOG" 2>&1; then
	diag "unexpected accepted row:"
	sed 's/^/# /' "$PROXYSQL_TXN_SPLIT_CHECK_LOG" 2>/dev/null || true
	ok 1 "txn_split_enabled=1 is rejected outside PolarDB hostgroups"
else
	ok 0 "txn_split_enabled=1 is rejected outside PolarDB hostgroups"
fi

quote_payload=$(printf "%064s" "" | tr ' ' "'")
quote_payload_sql=$(printf "%s" "$quote_payload" | sed "s/'/''/g")
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (105, 106, 'polardb', 1, 'session_lsn', -1, 0, 'v15', 'quote_${quote_payload_sql}_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null
quote_count=$(admin_sql "SELECT length(comment) - length(replace(comment, '''', '')) FROM pgsql_replication_hostgroups WHERE writer_hostgroup=105 AND reader_hostgroup=106;" | tr -d '[:space:]')
[ "$quote_count" = "64" ]
ok $? "quote-heavy replication-hostgroup comment survives config round-trip"

# The row above proves that the config-file parser preserves global_lsn. Change
# this synthetic row back to session_lsn before loading it into runtime; the separate
# global_lsn TAP covers the live invariant and timeout-action rejection paths.
admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='session_lsn' WHERE writer_hostgroup=101 AND reader_hostgroup=102;" >/dev/null
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (109, 110, 'polardb', 0, 'default', -1, -1, 'default', 'polardb_profile_inheritance');" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
cluster_hg=$(mysql_admin_sql "PROXY_SELECT writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment FROM runtime_pgsql_replication_hostgroups ORDER BY writer_hostgroup" |
	awk -F'\t' '$1 == 101 || $1 == 103 { if (out != "") out = out ";"; out = out $3 "|" $4 "|" $5 "|" $6 "|" $7 "|" $8 "|" $9 } END { print out }' |
	tr -d '\r')
[ "$cluster_hg" = "polardb|1|session_lsn|12345|0|v15_wait|polardb_hg_roundtrip;read_only|0|default|-1|-1|default|read_only_hg_roundtrip" ]
ok $? "cluster replication-hostgroup surface includes PolarDB policy columns"

[ "$(global_var pgsql-bounded_local_connection_cache)" = "0" ]
ok $? "worker-local connection cache defaults to ProxySQL 3.0.7 behavior"

set_global_var pgsql-bounded_local_connection_cache 1
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-bounded_local_connection_cache)" = "1" ]
bounded_cache_enabled=$?
set_global_var pgsql-bounded_local_connection_cache 0
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$bounded_cache_enabled" -eq 0 ] &&
	[ "$(runtime_var pgsql-bounded_local_connection_cache)" = "0" ]
ok $? "worker-local connection cache switches between 3.0.9 and 3.0.7 behavior"

initial_policy=$(policy_bundle global_variables)
initial_wait_ms=$(global_var pgsql-polardb_lsn_wait_timeout_ms)
initial_warmup_max=$(
	global_var pgsql-polardb_split_warmup_max_connections_per_request
)
if [ "$initial_policy" = \
		"session_fallback|session_lsn|replica|primary|primary|primary|primary|replica_then_primary|v15|true|true" ] &&
		[ "$initial_wait_ms" = "1000" ] &&
		[ "$initial_warmup_max" = "1" ]; then
	ok 0 "PolarDB starts with one coherent session_fallback profile"
else
	diag "initial policy: $initial_policy"
	diag "initial LSN wait timeout: $initial_wait_ms"
	diag "initial split warmup maximum: $initial_warmup_max"
	ok 1 "PolarDB starts with one coherent session_fallback profile"
fi

# Profiles own behavior, not topology, timeout length, warmup limits, identity,
# lag controls, or I/O tuning. Give those preserved fields distinctive values
# before cycling every named profile.
set_global_var pgsql-polardb_lsn_wait_timeout_ms 4321
set_global_var pgsql-polardb_split_warmup_max_connections_per_request 4
set_global_var pgsql-polardb_proxy_identity_port 15432
set_global_var pgsql-polardb_proxy_identity_host 127.0.0.2
set_global_var pgsql-polardb_proxy_identity_mode client
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
hg_before_profiles=$(admin_sql "SELECT group_concat(
	writer_hostgroup || ':' || reader_hostgroup || ':' || consistency_mode || ':' ||
	lsn_wait_timeout_ms || ':' || proxy_protocol, '|')
	FROM (SELECT * FROM runtime_pgsql_replication_hostgroups ORDER BY writer_hostgroup);" |
	tr -d '\r[:space:]')

PROFILE_MATRIX=$(cat <<'EOF'
off|off|primary|primary|primary|primary|primary|replica_then_primary|off|false|false
eventual|eventual|replica|primary|primary|primary|primary|replica_then_primary|off|false|false
session_warning|session_lsn|replica|primary|warning|warning|primary|replica_then_primary|v15|true|true
session_fallback|session_lsn|replica|primary|primary|primary|primary|replica_then_primary|v15|true|true
session_error|session_lsn|replica|error|error|error|error|replica_then_error|v15|true|true
global_fallback|global_lsn|replica|primary|primary|primary|primary|replica_then_primary|v15|true|true
global_error|global_lsn|replica|error|error|error|error|replica_then_error|v15|true|true
EOF
)

# Exercise every source -> target transition, including reloading the same
# profile. Each target must publish its whole bundle; no setting may leak from
# the source profile.
while IFS='|' read -r source_profile _; do
	set_global_var pgsql-polardb_profile "$source_profile"
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
	while IFS='|' read -r target_profile consistency target fallback missing \
			timeout replica_error replica_loss protocol monitor warmup; do
		set_global_var pgsql-polardb_profile "$target_profile"
		admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
		expected="$target_profile|$consistency|$target|$fallback|$missing|$timeout|$replica_error|$replica_loss|$protocol|$monitor|$warmup"
		actual=$(policy_bundle runtime_global_variables)
		if [ "$actual" = "$expected" ]; then
			ok 0 "profile switch $source_profile -> $target_profile publishes one complete policy"
		else
			diag "source=$source_profile target=$target_profile"
			diag "expected policy: $expected"
			diag "actual policy:   $actual"
			ok 1 "profile switch $source_profile -> $target_profile publishes one complete policy"
		fi
	done <<<"$PROFILE_MATRIX"
done <<<"$PROFILE_MATRIX"

policy_before_staged_profile=$(policy_bundle runtime_global_variables)
set_global_var pgsql-polardb_profile session_fallback
set_global_var pgsql-polardb_action_replica_error disconnect
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(policy_bundle runtime_global_variables)" = "$policy_before_staged_profile" ]
ok $? "a profile switch staged with an individual override changes no runtime policy"

# A profile is one complete policy change. Apply it first, then apply a custom
# override in a second load so both operations are explicit and deterministic.
set_global_var pgsql-polardb_action_replica_error error
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_profile)" = "session_fallback" ] &&
	[ "$(runtime_var pgsql-polardb_action_replica_error)" = "primary" ]
ok $? "the named profile loads after the staged override is repaired"

set_global_var pgsql-polardb_action_replica_error disconnect
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_profile)" = "custom" ] &&
	[ "$(runtime_var pgsql-polardb_consistency_mode)" = "session_lsn" ] &&
	[ "$(runtime_var pgsql-polardb_action_replica_error)" = "disconnect" ]
ok $? "an individual override loaded after the profile creates the intended custom policy"

set_global_var pgsql-polardb_profile global_error
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

hg_after_profiles=$(admin_sql "SELECT group_concat(
	writer_hostgroup || ':' || reader_hostgroup || ':' || consistency_mode || ':' ||
	lsn_wait_timeout_ms || ':' || proxy_protocol, '|')
	FROM (SELECT * FROM runtime_pgsql_replication_hostgroups ORDER BY writer_hostgroup);" |
	tr -d '\r[:space:]')
[ "$hg_after_profiles" = "$hg_before_profiles" ] &&
	[ "$(runtime_var pgsql-polardb_lsn_wait_timeout_ms)" = "4321" ] &&
	[ "$(runtime_var pgsql-polardb_split_warmup_max_connections_per_request)" = "4" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_mode)" = "client" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_port)" = "15432" ]
ok $? "profile switches preserve hostgroup overrides, numeric tuning, and identity"

# A named profile fixes its global consistency mode. Changing that setting
# directly is an explicit custom policy, not a partly modified named profile.
set_global_var pgsql-polardb_profile session_fallback
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
set_global_var pgsql-polardb_consistency_mode eventual
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_profile)" = "custom" ] &&
	[ "$(runtime_var pgsql-polardb_consistency_mode)" = "eventual" ]
ok $? "changing a named profile's global consistency mode marks it custom"
set_global_var pgsql-polardb_profile global_error
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

# Explicit hostgroup overrides survive profile switches, but the resulting
# effective policy must remain usable. A session_lsn row that inherits RFQ=off
# would route its first read to a replica and later reads through the
# missing-LSN action, so core rejects the switch and keeps the old runtime
# bundle.
admin_sql "UPDATE pgsql_replication_hostgroups SET proxy_protocol='default' WHERE writer_hostgroup=101 AND reader_hostgroup=102;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
set_global_var pgsql-polardb_profile eventual
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(runtime_var pgsql-polardb_profile)" = "global_error" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_protocol)" = "v15" ] &&
	[ "$(admin_sql "SELECT proxy_protocol FROM runtime_pgsql_replication_hostgroups WHERE writer_hostgroup=101;" | tr -d '[:space:]')" = "default" ]
ok $? "profile switch rejects an inherited RFQ-off session_lsn hostgroup"

# Repair MEMORY explicitly, then prove the inverse load order is protected too:
# under eventual, an incompatible hostgroup row cannot replace runtime.
set_global_var pgsql-polardb_profile global_error
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
admin_sql "UPDATE pgsql_replication_hostgroups SET proxy_protocol='legacy' WHERE writer_hostgroup=101 AND reader_hostgroup=102;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
set_global_var pgsql-polardb_profile eventual
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='session_lsn', proxy_protocol='default' WHERE writer_hostgroup=109 AND reader_hostgroup=110;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(admin_sql "SELECT consistency_mode || '|' || proxy_protocol FROM runtime_pgsql_replication_hostgroups WHERE writer_hostgroup=109;" | tr -d '[:space:]')" = "default|default" ] &&
	[ "$(runtime_var pgsql-polardb_profile)" = "eventual" ]
ok $? "hostgroup load rejects session_lsn when its inherited RFQ protocol is off"
admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='default', proxy_protocol='default' WHERE writer_hostgroup=109 AND reader_hostgroup=110;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
set_global_var pgsql-polardb_profile global_error
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

set_global_var pgsql-polardb_action_lsn_timeout primary
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_profile)" = "custom" ] &&
	[ "$(runtime_var pgsql-polardb_consistency_mode)" = "global_lsn" ] &&
	[ "$(runtime_var pgsql-polardb_action_lsn_timeout)" = "primary" ]
ok $? "an individual profile-owned override marks the effective profile custom"

policy_before_invalid=$(policy_bundle runtime_global_variables)
set_global_var pgsql-polardb_action_lsn_timeout warning
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(policy_bundle runtime_global_variables)" = "$policy_before_invalid" ]
ok $? "an invalid global_lsn warning bundle changes no runtime policy"

set_global_var pgsql-polardb_action_lsn_timeout primary
set_global_var pgsql-polardb_action_missing_lsn warning
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(policy_bundle runtime_global_variables)" = "$policy_before_invalid" ]
ok $? "global_lsn with a missing-LSN warning changes no runtime policy"

# Repair the staged value, then exercise a valid custom bundle together with
# the independent tuning variables.
set_global_var pgsql-polardb_consistency_mode eventual
set_global_var pgsql-polardb_read_target replica
set_global_var pgsql-polardb_action_read_fallback primary
set_global_var pgsql-polardb_proxy_protocol legacy
set_global_var pgsql-polardb_action_missing_lsn warning
set_global_var pgsql-polardb_action_lsn_timeout primary
set_global_var pgsql-polardb_action_replica_loss primary
set_global_var pgsql-polardb_action_replica_error disconnect
set_global_var pgsql-polardb_max_reader_lsn_gap_bytes 12345
set_global_var pgsql-polardb_max_reader_lag_ms 0
set_global_var pgsql-polardb_reader_lsn_max_age_ms 6000
set_global_var pgsql-polardb_lag_cap_freshness_ms 125
set_global_var pgsql-polardb_reader_lsn_lag_range_bytes 4096
set_global_var pgsql-polardb_reader_prefer_freshest_below_target 1
set_global_var pgsql-polardb_reader_prefer_less_loaded 1
set_global_var pgsql-polardb_reader_connection_retention 1
set_global_var pgsql-polardb_output_coalesce_bytes 262144
set_global_var pgsql-polardb_output_coalesce_packets 64
set_global_var pgsql-polardb_writev_direct 0
set_global_var pgsql-polardb_result_fast_forward 1
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_profile)" = "custom" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_protocol)" = "legacy" ] &&
	[ "$(runtime_var pgsql-polardb_consistency_mode)" = "eventual" ] &&
	[ "$(runtime_var pgsql-polardb_read_target)" = "replica" ] &&
	[ "$(runtime_var pgsql-polardb_action_read_fallback)" = "primary" ] &&
	[ "$(runtime_var pgsql-polardb_action_missing_lsn)" = "warning" ] &&
	[ "$(runtime_var pgsql-polardb_action_lsn_timeout)" = "primary" ] &&
	[ "$(runtime_var pgsql-polardb_action_replica_loss)" = "primary" ] &&
	[ "$(runtime_var pgsql-polardb_action_replica_error)" = "disconnect" ] &&
	[ "$(runtime_var pgsql-polardb_max_reader_lsn_gap_bytes)" = "12345" ] &&
	[ "$(runtime_var pgsql-polardb_max_reader_lag_ms)" = "0" ] &&
	[ "$(runtime_var pgsql-polardb_lsn_wait_timeout_ms)" = "4321" ] &&
	[ "$(runtime_var pgsql-polardb_reader_lsn_max_age_ms)" = "6000" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_mode)" = "client" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ] &&
	[ "$(runtime_var pgsql-polardb_proxy_identity_port)" = "15432" ] &&
	[ "$(runtime_var pgsql-polardb_lag_cap_freshness_ms)" = "125" ] &&
	[ "$(runtime_var pgsql-polardb_reader_lsn_lag_range_bytes)" = "4096" ] &&
	[ "$(runtime_var pgsql-polardb_reader_prefer_freshest_below_target)" = "true" ] &&
	[ "$(runtime_var pgsql-polardb_reader_prefer_less_loaded)" = "true" ] &&
	[ "$(runtime_var pgsql-polardb_reader_connection_retention)" = "1" ] &&
	[ "$(runtime_var pgsql-polardb_output_coalesce_bytes)" = "262144" ] &&
	[ "$(runtime_var pgsql-polardb_output_coalesce_packets)" = "64" ] &&
	[ "$(runtime_var pgsql-polardb_writev_direct)" = "false" ] &&
	[ "$(runtime_var pgsql-polardb_result_fast_forward)" = "true" ] &&
	[ "$(runtime_var pgsql-polardb_split_warmup_max_connections_per_request)" = "4" ]
ok $? "a valid custom policy and independent tuning load together"

admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='global_lsn' WHERE writer_hostgroup=101 AND reader_hostgroup=102;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(admin_sql "SELECT consistency_mode FROM runtime_pgsql_replication_hostgroups WHERE writer_hostgroup=101 AND reader_hostgroup=102;" | tr -d '[:space:]')" = "session_lsn" ]
ok $? "a global_lsn hostgroup cannot activate a missing-LSN warning"
admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='session_lsn' WHERE writer_hostgroup=101 AND reader_hostgroup=102;" >/dev/null

set_global_var pgsql-polardb_proxy_identity_mode proxy
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_proxy_identity_mode)" = "proxy" ] &&
	[ "$(global_var pgsql-polardb_proxy_identity_mode)" = "proxy" ]
ok $? "proxy identity mode accepts proxy"

policy_before_invalid=$(policy_bundle runtime_global_variables)
set_global_var pgsql-polardb_action_missing_lsn invalid
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
[ "$(policy_bundle runtime_global_variables)" = "$policy_before_invalid" ]
ok $? "an invalid profile-owned value changes no runtime policy"
set_global_var pgsql-polardb_action_missing_lsn warning

set_global_var pgsql-polardb_proxy_identity_host 0.0.0.0
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ] &&
	[ "$(global_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ]
ok $? "RFQ startup fallback identity rejects wildcard host at runtime load"

if grep -q 'replica_eligible=-1' "$PROXYSQL_CONFIG_FILE" && grep -q 'check_type="polardb"' "$PROXYSQL_CONFIG_FILE" && grep -q 'txn_split_enabled=1' "$PROXYSQL_CONFIG_FILE" && grep -q 'lsn_wait_timeout_ms=0' "$PROXYSQL_CONFIG_FILE" && grep -q 'proxy_protocol="legacy"' "$PROXYSQL_CONFIG_FILE"; then
	ok 0 "saved config file contains PolarDB fields"
else
	diag "saved config excerpt:"
	grep -nE 'replica_eligible|check_type|txn_split_enabled|consistency_mode|max_lag_bytes|lsn_wait_timeout_ms|proxy_protocol' "$PROXYSQL_CONFIG_FILE" | sed 's/^/# /' || true
	ok 1 "saved config file contains PolarDB fields"
fi

if [ "$FAIL" -eq 0 ]; then
	exit 0
fi
exit 1
