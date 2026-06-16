#!/usr/bin/env bash
# TAP test for PolarDB config-file round-trip fields.
# Covers:
#   - pgsql_query_rules.replica_eligible is saved/loaded without shifting log/apply/comment.
#   - pgsql_replication_hostgroups PolarDB policy fields are saved/loaded.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXYSQL_HOST="${PROXYSQL_HOST:-127.0.0.1}"
PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT:-18132}"
PROXYSQL_MYSQL_ADMIN_PORT="${PROXYSQL_MYSQL_ADMIN_PORT:-16033}"
PROXYSQL_PORT="${PROXYSQL_PORT:-18433}"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"
PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-/tmp/proxysql_test_polardb_config_roundtrip}"
PROXYSQL_CONFIG_FILE="${PROXYSQL_CONFIG_FILE:-$PROXYSQL_DATA_DIR/proxysql.cnf}"

PLAN=13
FAIL=0
STARTED_PROXY=0

# admin_sql, counter, proxy_sql and global/runtime variable accessors come from
# lib/tap_polardb.sh. The helper below is config-specific because it uses the
# MySQL admin surface.
mysql_admin_sql() {
    env MYSQL_PWD=admin mysql -h "$PROXYSQL_HOST" -P "$PROXYSQL_MYSQL_ADMIN_PORT" \
        -u admin --batch --skip-column-names -e "$1"
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
    --proxy-port "$PROXYSQL_PORT" >/tmp/proxysql_config_roundtrip_start.log 2>&1; then
    STARTED_PROXY=1
    ok 0 "start ProxySQL for config round-trip"
else
    diag "ProxySQL start failed; log follows"
    sed 's/^/# /' /tmp/proxysql_config_roundtrip_start.log 2>/dev/null || true
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
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (101, 102, 'polardb', 'lsn', 12345, 0, 'legacy', 'polardb_hg_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null

hg_policy=$(admin_sql "SELECT check_type || '|' || consistency_mode || '|' || max_lag_bytes || '|' || lsn_wait_timeout_ms || '|' || proxy_protocol || '|' || comment FROM pgsql_replication_hostgroups WHERE writer_hostgroup=101 AND reader_hostgroup=102;" | tr -d '\r')
[ "$hg_policy" = "polardb|lsn|12345|0|legacy|polardb_hg_roundtrip" ]
ok $? "PolarDB replication-hostgroup policy and proxy_protocol survive config round-trip"

admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (103, 104, 'read_only', 'default', -1, -1, 'default', 'read_only_hg_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null

read_only_policy=$(admin_sql "SELECT check_type || '|' || consistency_mode || '|' || max_lag_bytes || '|' || lsn_wait_timeout_ms || '|' || proxy_protocol || '|' || comment FROM pgsql_replication_hostgroups WHERE writer_hostgroup=103 AND reader_hostgroup=104;" | tr -d '\r')
[ "$read_only_policy" = "read_only|default|-1|-1|default|read_only_hg_roundtrip" ]
ok $? "read_only replication-hostgroup defaults survive config round-trip"

quote_payload=$(printf "%064s" "" | tr ' ' "'")
quote_payload_sql=$(printf "%s" "$quote_payload" | sed "s/'/''/g")
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES (105, 106, 'polardb', 'lsn', -1, 0, 'v15', 'quote_${quote_payload_sql}_roundtrip');" >/dev/null
admin_sql "SAVE CONFIG TO FILE $PROXYSQL_CONFIG_FILE;" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
admin_sql "LOAD PGSQL SERVERS FROM CONFIG;" >/dev/null
quote_count=$(admin_sql "SELECT length(comment) - length(replace(comment, '''', '')) FROM pgsql_replication_hostgroups WHERE writer_hostgroup=105 AND reader_hostgroup=106;" | tr -d '[:space:]')
[ "$quote_count" = "64" ]
ok $? "quote-heavy replication-hostgroup comment survives config round-trip"

admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
cluster_hg=$(mysql_admin_sql "PROXY_SELECT writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment FROM runtime_pgsql_replication_hostgroups ORDER BY writer_hostgroup" |
    awk -F'\t' '$1 == 101 || $1 == 103 { if (out != "") out = out ";"; out = out $3 "|" $4 "|" $5 "|" $6 "|" $7 "|" $8 } END { print out }' |
    tr -d '\r')
[ "$cluster_hg" = "polardb|lsn|12345|0|legacy|polardb_hg_roundtrip;read_only|default|-1|-1|default|read_only_hg_roundtrip" ]
ok $? "cluster replication-hostgroup surface includes PolarDB policy columns"

[ "$(global_var pgsql-polardb_proxy_protocol)" = "v15" ] &&
    [ "$(global_var pgsql-polardb_route_rfq_policy)" = "strict" ] &&
    [ "$(global_var pgsql-polardb_session_lsn_baseline)" = "observed" ] &&
    [ "$(global_var pgsql-polardb_proxy_identity_host)" = "" ] &&
    [ "$(global_var pgsql-polardb_proxy_identity_port)" = "0" ]
ok $? "RFQ startup global variables exist with expected defaults"

set_global_var pgsql-polardb_proxy_protocol legacy
set_global_var pgsql-polardb_route_rfq_policy best_effort
set_global_var pgsql-polardb_session_lsn_baseline primary
set_global_var pgsql-polardb_proxy_identity_host 127.0.0.2
set_global_var pgsql-polardb_proxy_identity_port 15432
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_proxy_protocol)" = "legacy" ] &&
    [ "$(runtime_var pgsql-polardb_route_rfq_policy)" = "best_effort" ] &&
    [ "$(runtime_var pgsql-polardb_session_lsn_baseline)" = "primary" ] &&
    [ "$(runtime_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ] &&
    [ "$(runtime_var pgsql-polardb_proxy_identity_port)" = "15432" ]
ok $? "RFQ startup global variables load to runtime"

set_global_var pgsql-polardb_proxy_protocol bogus
set_global_var pgsql-polardb_route_rfq_policy bogus
set_global_var pgsql-polardb_session_lsn_baseline bogus
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_proxy_protocol)" = "legacy" ] &&
    [ "$(runtime_var pgsql-polardb_route_rfq_policy)" = "best_effort" ] &&
    [ "$(runtime_var pgsql-polardb_session_lsn_baseline)" = "primary" ] &&
    [ "$(global_var pgsql-polardb_proxy_protocol)" = "legacy" ] &&
    [ "$(global_var pgsql-polardb_route_rfq_policy)" = "best_effort" ] &&
    [ "$(global_var pgsql-polardb_session_lsn_baseline)" = "primary" ]
ok $? "RFQ startup word variables reject invalid runtime-load values"

set_global_var pgsql-polardb_proxy_identity_host 0.0.0.0
admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
[ "$(runtime_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ] &&
    [ "$(global_var pgsql-polardb_proxy_identity_host)" = "127.0.0.2" ]
ok $? "RFQ startup fallback identity rejects wildcard host at runtime load"

if grep -q 'replica_eligible=-1' "$PROXYSQL_CONFIG_FILE" && grep -q 'check_type="polardb"' "$PROXYSQL_CONFIG_FILE" && grep -q 'lsn_wait_timeout_ms=0' "$PROXYSQL_CONFIG_FILE" && grep -q 'proxy_protocol="legacy"' "$PROXYSQL_CONFIG_FILE"; then
    ok 0 "saved config file contains PolarDB fields"
else
    diag "saved config excerpt:"
    grep -nE 'replica_eligible|check_type|consistency_mode|max_lag_bytes|lsn_wait_timeout_ms|proxy_protocol' "$PROXYSQL_CONFIG_FILE" | sed 's/^/# /' || true
    ok 1 "saved config file contains PolarDB fields"
fi

if [ "$FAIL" -eq 0 ]; then
    exit 0
fi
exit 1
