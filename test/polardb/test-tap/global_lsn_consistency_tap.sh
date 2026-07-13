#!/usr/bin/env bash
# TAP integration test for PolarDB GLOBAL_LSN consistency.
#
# GLOBAL_LSN is stricter than session LSN:
#   - every automatic read uses max(session target, writer mirror LSN);
#   - a missing writer mirror fails closed to the writer;
#   - best_effort timeout/degraded reader paths must not create a stale read;
#   - transaction-split waits fold in the global writer mirror target;
#   - non-READ COMMITTED transactions remain on the writer.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_global_lsn_consistency_tap")}"
PROXYSQL_START_LOG="${PROXYSQL_DATA_DIR}.start.log"
PROXYSQL_PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}"
DIRECT_PGSSLMODE="${DIRECT_PGSSLMODE:-$PGSSLMODE}"
TEST_TABLE="${TEST_TABLE:-$(polardb_test_identifier polardb_global_lsn_consistency_tap)}"

WRITER_HG="$POLARDB_WRITER_HG"
READER_HG="$POLARDB_READER_HG"
MISSING_WRITER_HG="$POLARDB_GLOBAL_LSN_MISSING_WRITER_HG"
MISSING_READER_HG="$POLARDB_GLOBAL_LSN_MISSING_READER_HG"

PLAN=16
FAIL=0
STARTED_PROXY=0
PRIMARY_SERVER_PORT="${PRIMARY_SERVER_PORT:-}"
REPLICA_SERVER_PORT="${REPLICA_SERVER_PORT:-}"
REPLICA_SERVER_ENDPOINTS=""
REPLAY_LAG_SET=0
GLOBAL_LSN_SETUP_PLAN=6
GLOBAL_LSN_TAP_GROUPS="${GLOBAL_LSN_TAP_GROUPS:-}"
GLOBAL_LSN_TAP_CASES="${GLOBAL_LSN_TAP_CASES:-}"

# case_id | group | tag | plan_count | function
# 1       | global-core    | parallel          | 1 | case_global_variable_alias
# 2       | global-core    | parallel          | 1 | case_hostgroup_alias
# 3       | global-core    | parallel          | 1 | case_session_set_global_lsn
# 4       | global-core    | parallel          | 1 | case_cross_session_global_read
# 5       | global-core    | parallel          | 1 | case_missing_writer_mirror_fails_closed
# 6       | global-timeout | backend_exclusive | 1 | case_global_lsn_best_effort_timeout_is_strict
# 7       | global-split   | parallel          | 1 | case_global_lsn_transaction_split_uses_replica
# 8       | global-split   | parallel          | 1 | case_global_lsn_begin_warmup_requests_reader
# 9       | global-split   | parallel          | 2 | case_non_read_committed_transaction_stays_primary
global_lsn_cases_for_group() {
	case "$1" in
	global-core) printf '%s\n' "1 2 3 4 5" ;;
	global-timeout) printf '%s\n' "6" ;;
	global-split) printf '%s\n' "7 8 9" ;;
	*)
		diag "unknown GLOBAL_LSN TAP group: $1"
		return 1
		;;
	esac
}

global_lsn_append_case_once() {
	local list="$1"
	local case_id="$2"
	local item

	for item in $list; do
		[ "$item" = "$case_id" ] && {
			printf '%s\n' "$list"
			return 0
		}
	done
	if [ -n "$list" ]; then
		printf '%s %s\n' "$list" "$case_id"
	else
		printf '%s\n' "$case_id"
	fi
}

global_lsn_expand_groups() {
	local group case_id group_cases selected=""

	for group in ${GLOBAL_LSN_TAP_GROUPS//,/ }; do
		group_cases=$(global_lsn_cases_for_group "$group") || return 1
		for case_id in $group_cases; do
			selected=$(global_lsn_append_case_once "$selected" "$case_id")
		done
	done
	printf '%s\n' "$selected"
}

if [ -z "$GLOBAL_LSN_TAP_CASES" ] && [ -n "$GLOBAL_LSN_TAP_GROUPS" ]; then
	GLOBAL_LSN_TAP_CASES="$(global_lsn_expand_groups)" || exit 1
fi

global_lsn_case_selected() {
	local wanted

	[ -z "$GLOBAL_LSN_TAP_CASES" ] && return 0
	for wanted in ${GLOBAL_LSN_TAP_CASES//,/ }; do
		[ "$wanted" = "$1" ] && return 0
	done
	return 1
}

global_lsn_case_plan() {
	case "$1" in
	9) printf '%s\n' 2 ;;
	*) printf '%s\n' 1 ;;
	esac
}

global_lsn_selected_plan() {
	local case_id planned="$GLOBAL_LSN_SETUP_PLAN"

	if [ -z "$GLOBAL_LSN_TAP_CASES" ]; then
		printf '%s\n' "$PLAN"
		return 0
	fi
	for case_id in ${GLOBAL_LSN_TAP_CASES//,/ }; do
		planned=$((planned + $(global_lsn_case_plan "$case_id")))
	done
	printf '%s\n' "$planned"
}

run_selected_global_lsn_case() {
	local case_id="$1"
	shift

	if global_lsn_case_selected "$case_id"; then
		"$@"
	fi
}

proxy_command_sequence() {
	local args=()
	local sql
	for sql in "$@"; do
		args+=("-c" "$sql")
	done
	PGPASSWORD="$PGPASSWORD" PGSSLMODE="$PROXYSQL_PGSSLMODE" psql -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
		-U "$PGUSER" -d "$PGDB" -A -t -q -v ON_ERROR_STOP=1 "${args[@]}"
}

direct_sql() {
	local host="$1"
	local port="$2"
	local sql="$3"
	PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$DIRECT_PGSSLMODE" psql -h "$host" -p "$port" \
		-U "$PGUSER_DIRECT" -d "$PGDB" -A -t -q -v ON_ERROR_STOP=1 -c "$sql"
}

backend_setting_is() {
	local host="$1"
	local port="$2"
	local name="$3"
	local expected="$4"

	[ "$(direct_sql "$host" "$port" "SELECT current_setting('$name', true);" 2>/dev/null | tr -d '[:space:]')" = "$expected" ]
}

wait_for_backend_setting() {
	local host="$1"
	local port="$2"
	local name="$3"
	local expected="$4"
	local timeout_sec="${5:-15}"

	wait_until backend_setting_is "$host" "$port" "$name" "$expected" -- "$timeout_sec" 0.5
}

set_replay_lag_bytes() {
	local lag_bytes="$1"

	if ! polardb_run_dcs "polar_replay_min_lag_size=$lag_bytes" >/dev/null 2>&1; then
		return 1
	fi
	if ! wait_for_backend_setting "$REPLICA_HOST" "$REPLICA_PORT" polar_replay_min_lag_size "$lag_bytes" 15; then
		diag "polar_replay_min_lag_size=$lag_bytes was pushed through $POLARDB_DCS_MODE DCS but is not visible on $REPLICA_HOST:$REPLICA_PORT"
		return 1
	fi
	if [ "$lag_bytes" != "0" ]; then
		REPLAY_LAG_SET=1
	else
		REPLAY_LAG_SET=0
	fi
	sleep 1
	return 0
}

replica_replay_reaches_lsn() {
	local target_lsn="$1"

	[ "$(direct_sql "$REPLICA_HOST" "$REPLICA_PORT" "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '$target_lsn'::pg_lsn) >= 0;" 2>/dev/null | tr -d '[:space:]')" = "t" ]
}

wait_for_replica_replay_catchup() {
	local timeout_sec="${1:-20}"
	local target_lsn

	target_lsn=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT pg_current_wal_lsn();" 2>/dev/null | tr -d '[:space:]') || return 1
	[ -n "$target_lsn" ] || return 1
	wait_until replica_replay_reaches_lsn "$target_lsn" -- "$timeout_sec" 0.5
}

append_token_once() {
	local list="$1"
	local token="$2"
	local item

	[ -n "$token" ] || {
		printf '%s\n' "$list"
		return 0
	}
	for item in $list; do
		if [ "$item" = "$token" ]; then
			printf '%s\n' "$list"
			return 0
		fi
	done
	if [ -n "$list" ]; then
		printf '%s %s\n' "$list" "$token"
	else
		printf '%s\n' "$token"
	fi
}

endpoint_in_list() {
	local endpoint="$1"
	local list="$2"
	local item
	for item in $list; do
		[ "$item" = "$endpoint" ] && return 0
	done
	return 1
}

payload_endpoint() {
	local payload="$1"
	printf '%s\n' "${payload%%|*}"
}

payload_value() {
	local payload="$1"
	printf '%s\n' "${payload#*|}"
}

detect_topology() {
	if [ "${POLARDB_AUTODETECT:-1}" = "1" ]; then
		local topology_error topology_error_file
		topology_error_file="$(mktemp "${TMPDIR:-/tmp}/polardb-topology.XXXXXX")"
		if polardb_detect_topology 2>"$topology_error_file"; then
			rm -f "$topology_error_file"
			diag "topology selected: writer=$PRIMARY_HOST:$PRIMARY_PORT readers=${POLARDB_REPLICA_ENDPOINTS:-$REPLICA_HOST:$REPLICA_PORT}"
			return 0
		fi
		topology_error="$(sed -n '1,120p' "$topology_error_file" 2>/dev/null || true)"
		rm -f "$topology_error_file"
		[ -z "$topology_error" ] || diag "$topology_error"
		diag "cannot detect writer/reader from POLARDB_ENDPOINTS='$POLARDB_ENDPOINTS'"
		return 1
	fi

	if polardb_validate_manual_topology; then
		diag "topology from env: writer=$PRIMARY_HOST:$PRIMARY_PORT readers=${POLARDB_REPLICA_ENDPOINTS:-$REPLICA_HOST:$REPLICA_PORT}"
		return 0
	fi
	diag "manual topology is incomplete or failed role validation"
	return 1
}

detect_backend_ports() {
	if [ -n "$PRIMARY_SERVER_ENDPOINT" ] && [ -n "$REPLICA_SERVER_ENDPOINT" ]; then
		REPLICA_SERVER_ENDPOINTS=$(append_token_once "$REPLICA_SERVER_ENDPOINTS" "$REPLICA_SERVER_ENDPOINT")
		diag "server-reported backend endpoints from env: writer=$PRIMARY_SERVER_ENDPOINT readers=$REPLICA_SERVER_ENDPOINTS"
		return 0
	fi

	local primary_server_addr endpoint host port replica_addr replica_port replica_endpoint
	primary_server_addr=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT host(inet_server_addr());" 2>/dev/null | tr -d '[:space:]')
	PRIMARY_SERVER_PORT=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]')
	PRIMARY_SERVER_ENDPOINT="${primary_server_addr}:${PRIMARY_SERVER_PORT}"

	for endpoint in $(polardb_each_replica_endpoint); do
		host=$(polardb_endpoint_host "$endpoint")
		port=$(polardb_endpoint_port "$endpoint")
		replica_addr=$(direct_sql "$host" "$port" "SELECT host(inet_server_addr());" 2>/dev/null | tr -d '[:space:]')
		replica_port=$(direct_sql "$host" "$port" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]')
		replica_endpoint="${replica_addr}:${replica_port}"
		REPLICA_SERVER_ENDPOINTS=$(append_token_once "$REPLICA_SERVER_ENDPOINTS" "$replica_endpoint")
		if [ -z "$REPLICA_SERVER_ENDPOINT" ]; then
			REPLICA_SERVER_ENDPOINT="$replica_endpoint"
			REPLICA_SERVER_PORT="$replica_port"
		fi
	done

	diag "server-reported backend endpoints: writer=$PRIMARY_SERVER_ENDPOINT readers=$REPLICA_SERVER_ENDPOINTS"
	[ -n "$primary_server_addr" ] && [ -n "$PRIMARY_SERVER_PORT" ] && [ -n "$REPLICA_SERVER_ENDPOINTS" ]
}

start_proxy() {
	rm -f "$PROXYSQL_START_LOG"
	PROXYSQL_PGSQL_THREADS="${PROXYSQL_PGSQL_THREADS:-1}" \
		PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" restart \
		--data-dir "$PROXYSQL_DATA_DIR" \
		--admin-port "$PROXYSQL_ADMIN_PORT" \
		--proxy-port "$PROXYSQL_PORT" \
		--mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT" >"$PROXYSQL_START_LOG" 2>&1
	STARTED_PROXY=1
}

stop_proxy() {
	if [ "$STARTED_PROXY" -eq 1 ]; then
		PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop --data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
	fi
}

cleanup() {
	tap_stop_wal_pulse
	if [ "$REPLAY_LAG_SET" -eq 1 ]; then
		set_replay_lag_bytes 0 >/dev/null 2>&1 || true
		wait_for_replica_replay_catchup 20 >/dev/null 2>&1 || true
	fi
	if [ "$STARTED_PROXY" -eq 1 ]; then
		admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null 2>&1 || true
		admin_sql "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null 2>&1 || true
		admin_sql "UPDATE global_variables SET variable_value='strict' WHERE variable_name='pgsql-polardb_route_rfq_policy';" >/dev/null 2>&1 || true
		admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null 2>&1 || true
		admin_sql "UPDATE pgsql_users SET default_hostgroup=$WRITER_HG WHERE username='$PGUSER';" >/dev/null 2>&1 || true
		admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup IN ($MISSING_WRITER_HG) OR reader_hostgroup IN ($MISSING_READER_HG);" >/dev/null 2>&1 || true
		admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id IN ($MISSING_WRITER_HG,$MISSING_READER_HG);" >/dev/null 2>&1 || true
		admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
		admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null 2>&1 || true
		admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null 2>&1 || true
	fi
	if [ -n "${PRIMARY_HOST:-}" ] && [ -n "${PRIMARY_PORT:-}" ]; then
		direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP TABLE IF EXISTS $TEST_TABLE;" >/dev/null 2>&1 || true
	fi
	stop_proxy
}
trap cleanup EXIT

insert_reader_servers() {
	local reader_hg="$1"
	local max_connections="${2:-100}"
	local endpoint host port

	for endpoint in $(polardb_each_replica_endpoint); do
		host=$(polardb_endpoint_host "$endpoint")
		port=$(polardb_endpoint_port "$endpoint")
		admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($reader_hg, '$host', $port, 'ONLINE', 1000, $max_connections);" >/dev/null
	done
}

set_default_hostgroup() {
	local writer_hg="$1"
	admin_sql "UPDATE pgsql_users SET default_hostgroup=$writer_hg WHERE username='$PGUSER';" >/dev/null
	admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
}

set_global_lsn_policy() {
	set_default_hostgroup "$WRITER_HG"
	set_select_rule_auto "global_lsn_auto_select"
	admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='global_lsn', max_lag_bytes=-1, lsn_wait_timeout_ms=5000, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='global_lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_route_rfq_policy';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name IN ('pgsql-polardb_lag_ms','pgsql-polardb_lag_bytes');" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='5000' WHERE variable_name='pgsql-polardb_lsn_freshness_ms';" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
}

configure_proxy() {
	admin_sql "DELETE FROM pgsql_servers;" >/dev/null
	admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, 100);" >/dev/null
	insert_reader_servers "$READER_HG" 100

	admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
	admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES ($WRITER_HG, $READER_HG, 'polardb', 'global_lsn', -1, 5000, 'v15', 'global_lsn_consistency_tap');" >/dev/null

	admin_sql "DELETE FROM pgsql_users;" >/dev/null
	admin_sql "INSERT INTO pgsql_users (username, password, active, default_hostgroup) VALUES ('$PGUSER', '$PGPASSWORD', 1, $WRITER_HG);" >/dev/null

	set_select_rule_auto "global_lsn_auto_select"
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
	set_global_lsn_policy
}

snapshot_global_counters() {
	local prefix="$1"
	local value

	value=$(counter PolarDB_Global_LSN_Routing)
	printf -v "${prefix}_global" '%s' "$value"
	value=$(counter PolarDB_Session_LSN_Routing)
	printf -v "${prefix}_session" '%s' "$value"
	value=$(counter PolarDB_Wait_LSN_Sent)
	printf -v "${prefix}_wait" '%s' "$value"
	value=$(counter PolarDB_Wait_Wrap_Bypassed)
	printf -v "${prefix}_bypass" '%s' "$value"
	value=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
	printf -v "${prefix}_degraded" '%s' "$value"
	value=$(counter PolarDB_Primary_LSN_Unknown)
	printf -v "${prefix}_primary_unknown" '%s' "$value"
}

snapshot_split_counters() {
	local prefix="$1"
	local value

	value=$(counter PolarDB_Split_Reads_Success)
	printf -v "${prefix}_success" '%s' "$value"
	value=$(counter PolarDB_Split_LSN_Wait_Count)
	printf -v "${prefix}_wait" '%s' "$value"
	value=$(counter PolarDB_Split_Reads_Total)
	printf -v "${prefix}_total" '%s' "$value"
	value=$(counter PolarDB_Split_Reads_Fallback)
	printf -v "${prefix}_fallback" '%s' "$value"
	value=$(counter PolarDB_Queries_In_Splittable_Txn)
	printf -v "${prefix}_splittable" '%s' "$value"
	value=$(counter PolarDB_Queries_Split_Eligible)
	printf -v "${prefix}_eligible" '%s' "$value"
	value=$(counter PolarDB_XIDs_Received)
	printf -v "${prefix}_xids" '%s' "$value"
	value=$(counter PolarDB_Txn_Became_Splittable)
	printf -v "${prefix}_became" '%s' "$value"
	value=$(counter PolarDB_Split_WAL_Pending)
	printf -v "${prefix}_wal_pending" '%s' "$value"
	value=$(counter PolarDB_Split_Rejected_Multistatement)
	printf -v "${prefix}_multi" '%s' "$value"
	value=$(counter PolarDB_Split_Rejected_Not_Select)
	printf -v "${prefix}_not_select" '%s' "$value"
	value=$(counter PolarDB_Split_Rejected_Write_LSN_Unknown)
	printf -v "${prefix}_write_unknown" '%s' "$value"
	value=$(counter PolarDB_Split_Rejected_Observed_LSN_Unknown)
	printf -v "${prefix}_observed_unknown" '%s' "$value"
	value=$(counter PolarDB_Split_Invariant_Violations)
	printf -v "${prefix}_invariant" '%s' "$value"
	value=$(counter PolarDB_Split_Blocked_Reads)
	printf -v "${prefix}_blocked" '%s' "$value"
	value=$(counter PolarDB_Split_No_Backend)
	printf -v "${prefix}_no_backend" '%s' "$value"
}

global_delta() {
	local before_prefix="$1"
	local after_prefix="$2"
	local field="$3"
	local before_var="${before_prefix}_${field}"
	local after_var="${after_prefix}_${field}"

	echo $((${!after_var} - ${!before_var}))
}

split_delta() {
	local before_prefix="$1"
	local after_prefix="$2"
	local field="$3"
	local before_var="${before_prefix}_${field}"
	local after_var="${after_prefix}_${field}"

	echo $((${!after_var} - ${!before_var}))
}

global_protect_delta() {
	local before_prefix="$1"
	local after_prefix="$2"

	echo $(($(global_delta "$before_prefix" "$after_prefix" wait) + $(global_delta "$before_prefix" "$after_prefix" bypass)))
}

first_replica_payload_from_output() {
	local out="$1"
	local line endpoint

	while IFS= read -r line; do
		case "$line" in
			*"|"*)
				endpoint=$(payload_endpoint "$line")
				if endpoint_in_list "$endpoint" "$REPLICA_SERVER_ENDPOINTS"; then
					printf '%s\n' "$line"
					return 0
				fi
				;;
		esac
	done <<<"$out"
	return 1
}

global_lsn_transaction_read_script() {
	local begin_sql="$1"
	local row_id="$2"
	local marker="$3"
	local session_sql="${4:-}"
	local i

	[ -n "$session_sql" ] && printf '%s\n' "$session_sql"
	printf '%s\n' "$begin_sql"
	printf "INSERT INTO %s VALUES (%s, '%s') ON CONFLICT (id) DO UPDATE SET data='%s';\n" "$TEST_TABLE" "$row_id" "$marker" "$marker"
	printf '\\! sleep 1\n'
	for i in 1 2 3 4 5 6; do
		printf "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM %s WHERE id=%s;\n" "$TEST_TABLE" "$row_id"
		[ "$i" -lt 6 ] && printf '\\! sleep 1\n'
	done
	printf 'COMMIT;\n'
}

case_global_variable_alias() {
	local runtime_value

	admin_sql "UPDATE global_variables SET variable_value='global' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
	runtime_value=$(runtime_var "pgsql-polardb_consistency_mode")
	set_global_lsn_policy
	if [ "$runtime_value" = "global" ]; then
		ok 0 "global variable accepts GLOBAL_LSN alias"
	else
		diag "runtime pgsql-polardb_consistency_mode='$runtime_value' expected=global"
		ok 1 "global variable accepts GLOBAL_LSN alias"
	fi
}

case_hostgroup_alias() {
	local runtime_mode

	admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='lsn_global' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	runtime_mode=$(admin_sql "SELECT consistency_mode FROM runtime_pgsql_replication_hostgroups WHERE writer_hostgroup=$WRITER_HG;" 2>/dev/null | tr -d '[:space:]')
	set_global_lsn_policy
	if [ "$runtime_mode" = "lsn_global" ]; then
		ok 0 "replication hostgroup accepts GLOBAL_LSN alias"
	else
		diag "runtime consistency_mode='$runtime_mode' expected=lsn_global"
		ok 1 "replication hostgroup accepts GLOBAL_LSN alias"
	fi
}

case_session_set_global_lsn() {
	local out rc

	out=$(proxy_command_sequence \
		"SET proxysql.polardb_consistency_mode TO 'global_lsn';" \
		"SELECT 1;" \
		2>&1)
	rc=$?
	if [ "$rc" -eq 0 ] && printf '%s\n' "$out" | grep -qx "1"; then
		ok 0 "session SET accepts GLOBAL_LSN mode"
	else
		diag "session SET output: $out"
		ok 1 "session SET accepts GLOBAL_LSN mode"
	fi
}

case_cross_session_global_read() {
	local marker out result endpoint value global_count session_count protect_count

	set_global_lsn_policy
	marker="global_cross_$$"
	if ! proxy_script "INSERT INTO $TEST_TABLE VALUES (1, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';" >/dev/null 2>&1; then
		ok 1 "GLOBAL_LSN protects a new session read after another session commits"
		return
	fi

	snapshot_global_counters cross_before
	out=$(
		proxy_script 2>&1 <<SQL
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=1;
SQL
	)
	snapshot_global_counters cross_after
	result=$(last_endpoint_payload_from_output "$out")
	endpoint=$(payload_endpoint "$result")
	value=$(payload_value "$result")
	global_count=$(global_delta cross_before cross_after global)
	session_count=$(global_delta cross_before cross_after session)
	protect_count=$(global_protect_delta cross_before cross_after)

	if endpoint_in_list "$endpoint" "$REPLICA_SERVER_ENDPOINTS" &&
		[ "$value" = "$marker" ] &&
		[ "$global_count" -ge 1 ] &&
		[ "$session_count" -eq 0 ] &&
		[ "$protect_count" -ge 1 ]; then
		ok 0 "GLOBAL_LSN protects a new session read after another session commits"
	else
		diag "cross-session output: $out"
		diag "result=$result endpoint=$endpoint value=$value replica_endpoints='$REPLICA_SERVER_ENDPOINTS'"
		diag "global_delta=$global_count session_delta=$session_count protect_delta=$protect_count"
		ok 1 "GLOBAL_LSN protects a new session read after another session commits"
	fi
}

configure_missing_writer_mirror_pair() {
	admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='off' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='global_lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_route_rfq_policy';" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

	admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id IN ($MISSING_WRITER_HG,$MISSING_READER_HG);" >/dev/null
	admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($MISSING_WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, 100);" >/dev/null
	insert_reader_servers "$MISSING_READER_HG" 100
	admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup=$MISSING_WRITER_HG;" >/dev/null
	admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES ($MISSING_WRITER_HG, $MISSING_READER_HG, 'polardb', 'global_lsn', -1, 5000, 'off', 'global_lsn_missing_mirror_pair');" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	set_default_hostgroup "$MISSING_WRITER_HG"
	set_select_rule_auto "global_lsn_missing_mirror_auto_select"
}

case_missing_writer_mirror_fails_closed() {
	local out endpoint global_count protect_count degraded_count primary_unknown_count

	configure_missing_writer_mirror_pair
	snapshot_global_counters missing_before
	out=$(
		proxy_script 2>&1 <<SQL
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
	)
	snapshot_global_counters missing_after
	endpoint=$(last_endpoint_from_output "$out")
	global_count=$(global_delta missing_before missing_after global)
	protect_count=$(global_protect_delta missing_before missing_after)
	degraded_count=$(global_delta missing_before missing_after degraded)
	primary_unknown_count=$(global_delta missing_before missing_after primary_unknown)
	set_global_lsn_policy

	if [ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
		[ "$global_count" -eq 0 ] &&
		[ "$protect_count" -eq 0 ] &&
		[ "$degraded_count" -eq 0 ] &&
		[ "$primary_unknown_count" -ge 1 ]; then
		ok 0 "GLOBAL_LSN missing writer mirror fails closed without best-effort degradation"
	else
		diag "missing mirror output: $out"
		diag "endpoint=$endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT global_delta=$global_count protect_delta=$protect_count degraded_delta=$degraded_count primary_unknown_delta=$primary_unknown_count"
		ok 1 "GLOBAL_LSN missing writer mirror fails closed without best-effort degradation"
	fi
}

case_global_lsn_best_effort_timeout_is_strict() {
	local marker out rc timeout_before timeout_after lsn_timeout_before lsn_timeout_after
	local retry_before retry_after degraded_before degraded_after global_count wait_count
	local timeout_delta lsn_timeout_delta retry_delta degraded_delta warning_count

	set_global_lsn_policy
	admin_sql "UPDATE pgsql_replication_hostgroups SET lsn_wait_timeout_ms=100 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

	if ! set_replay_lag_bytes "${POLARDB_GLOBAL_LSN_TIMEOUT_REPLAY_LAG_BYTES:-50000}"; then
		set_global_lsn_policy
		skip_ok "GLOBAL_LSN best_effort timeout is forced strict under replay lag" "cannot set polar_replay_min_lag_size through managed DCS"
		return
	fi

	marker="global_timeout_$$"
	timeout_before=$(counter PolarDB_Wait_Error_Timeout)
	lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
	retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
	degraded_before=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
	snapshot_global_counters timeout_before_snap

	tap_start_wal_pulse "$PRIMARY_HOST" "$PRIMARY_PORT" "$TEST_TABLE" 0
	out=$(
		proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (4, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
\! sleep 0.5
SELECT data FROM $TEST_TABLE WHERE id=4;
SQL
	)
	rc=$?
	tap_stop_wal_pulse

	timeout_after=$(counter PolarDB_Wait_Error_Timeout)
	lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
	retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
	degraded_after=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
	snapshot_global_counters timeout_after_snap
	set_replay_lag_bytes 0 >/dev/null 2>&1 || true
	wait_for_replica_replay_catchup 20 >/dev/null 2>&1 || true
	set_global_lsn_policy

	timeout_delta=$((timeout_after - timeout_before))
	lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before))
	retry_delta=$((retry_after - retry_before))
	degraded_delta=$((degraded_after - degraded_before))
	global_count=$(global_delta timeout_before_snap timeout_after_snap global)
	wait_count=$(global_delta timeout_before_snap timeout_after_snap wait)
	warning_count=$(printf '%s\n' "$out" | grep -Eic 'WARNING:.*LSN wait timeout')

	if [ "$rc" -eq 0 ] &&
		printf '%s\n' "$out" | grep -qx "$marker" &&
		[ "$timeout_delta" -ge 1 ] &&
		[ "$lsn_timeout_delta" -ge 1 ] &&
		[ "$retry_delta" -ge 1 ] &&
		[ "$global_count" -ge 1 ] &&
		[ "$wait_count" -ge 1 ] &&
		[ "$degraded_delta" -eq 0 ] &&
		[ "$warning_count" -eq 0 ]; then
		ok 0 "GLOBAL_LSN best_effort timeout is forced strict under replay lag"
	else
		diag "global strict-timeout output: $out"
		diag "rc=$rc timeout_delta=$timeout_delta lsn_timeout_delta=$lsn_timeout_delta retry_delta=$retry_delta degraded_delta=$degraded_delta global_delta=$global_count wait_delta=$wait_count warning_count=$warning_count"
		ok 1 "GLOBAL_LSN best_effort timeout is forced strict under replay lag"
	fi
}

case_global_lsn_transaction_split_uses_replica() {
	local marker out result endpoint value success_count wait_count

	set_global_lsn_policy
	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=1, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	marker="global_split_$$"
	snapshot_split_counters split_before
	# A transaction write can initially report WAL pending. The small external
	# WAL pulse lets the backend clear that marker before the later SELECTs assert
	# split routing; without it this test can validate only fail-closed behavior.
	tap_start_wal_pulse "$PRIMARY_HOST" "$PRIMARY_PORT" "$TEST_TABLE" 200
	sleep 0.3
	out=$(
		proxy_script 2>&1 <<SQL
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES (3, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
\! sleep 1
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=3;
COMMIT;
SQL
	)
	tap_stop_wal_pulse
	snapshot_split_counters split_after
	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	result=$(first_replica_payload_from_output "$out" || true)
	if [ -z "$result" ]; then
		result=$(last_endpoint_payload_from_output "$out")
	fi
	endpoint=$(payload_endpoint "$result")
	value=$(payload_value "$result")
	success_count=$(split_delta split_before split_after success)
	wait_count=$(split_delta split_before split_after wait)

	if endpoint_in_list "$endpoint" "$REPLICA_SERVER_ENDPOINTS" &&
		[ "$value" = "$marker" ] &&
		[ "$success_count" -ge 1 ] &&
		[ "$wait_count" -ge 1 ]; then
		ok 0 "GLOBAL_LSN transaction split read uses replica with LSN wait"
	else
		diag "global transaction-split output: $out"
		diag "result=$result endpoint=$endpoint value=$value replica_endpoints='$REPLICA_SERVER_ENDPOINTS'"
		diag "split_success_delta=$success_count split_wait_delta=$wait_count"
		diag "split_total_delta=$(split_delta split_before split_after total) fallback_delta=$(split_delta split_before split_after fallback) splittable_delta=$(split_delta split_before split_after splittable) eligible_delta=$(split_delta split_before split_after eligible)"
		diag "split_xids_delta=$(split_delta split_before split_after xids) became_delta=$(split_delta split_before split_after became) wal_pending_delta=$(split_delta split_before split_after wal_pending)"
		diag "split_rejects multi=$(split_delta split_before split_after multi) not_select=$(split_delta split_before split_after not_select) write_unknown=$(split_delta split_before split_after write_unknown) observed_unknown=$(split_delta split_before split_after observed_unknown) invariant=$(split_delta split_before split_after invariant) blocked=$(split_delta split_before split_after blocked) no_backend=$(split_delta split_before split_after no_backend)"
		ok 1 "GLOBAL_LSN transaction split read uses replica with LSN wait"
	fi
}

case_global_lsn_begin_warmup_requests_reader() {
	local before after out rc

	set_global_lsn_policy
	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=1, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_lazy_warmup_split';" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

	before=$(counter PolarDB_Split_Warmup_Requested)
	out=$(
		proxy_script 2>&1 <<SQL
SET proxysql.polardb_txn_split_warmup TO 'begin';
BEGIN;
COMMIT;
SQL
	)
	rc=$?
	after=$(counter PolarDB_Split_Warmup_Requested)

	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	if [ "$rc" -eq 0 ] && [ "$after" -gt "$before" ]; then
		ok 0 "GLOBAL_LSN BEGIN transaction split warmup queues a reader request"
	else
		diag "GLOBAL_LSN BEGIN warmup output: $out"
		diag "warmup_requested_before=$before after=$after rc=$rc"
		ok 1 "GLOBAL_LSN BEGIN transaction split warmup queues a reader request"
	fi
}

case_non_read_committed_transaction_stays_primary() {
	local rc_marker rr_marker out result endpoint value rc success_count split_total

	set_global_lsn_policy
	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=1, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	rc_marker="global_rc_split_$$"
	snapshot_split_counters rc_split_before
	# This uses the common PostgreSQL spelling that previously fell through to
	# lock_hostgroup. If that parser regresses, the read stays on the writer and
	# this positive-control split assertion fails.
	tap_start_wal_pulse "$PRIMARY_HOST" "$PRIMARY_PORT" "$TEST_TABLE" 200
	out=$(
		global_lsn_transaction_read_script \
			"BEGIN;" \
			5 \
			"$rc_marker" \
			"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;" |
			proxy_script 2>&1
	)
	rc=$?
	tap_stop_wal_pulse
	snapshot_split_counters rc_split_after
	result=$(first_replica_payload_from_output "$out" || true)
	if [ -z "$result" ]; then
		result=$(last_endpoint_payload_from_output "$out")
	fi
	endpoint=$(payload_endpoint "$result")
	value=$(payload_value "$result")
	success_count=$(split_delta rc_split_before rc_split_after success)
	if [ "$rc" -eq 0 ] &&
		endpoint_in_list "$endpoint" "$REPLICA_SERVER_ENDPOINTS" &&
		[ "$value" = "$rc_marker" ] &&
		[ "$success_count" -ge 1 ]; then
		ok 0 "GLOBAL_LSN READ COMMITTED transaction split can use a replica"
	else
		diag "READ COMMITTED transaction output: $out"
		diag "result=$result endpoint=$endpoint value=$value replica_endpoints='$REPLICA_SERVER_ENDPOINTS'"
		diag "split_success_delta=$success_count split_total_delta=$(split_delta rc_split_before rc_split_after total) blocked_delta=$(split_delta rc_split_before rc_split_after blocked)"
		ok 1 "GLOBAL_LSN READ COMMITTED transaction split can use a replica"
	fi

	rr_marker="global_rr_$$"
	snapshot_split_counters rr_split_before
	out=$(
		global_lsn_transaction_read_script \
			"BEGIN ISOLATION LEVEL REPEATABLE READ;" \
			6 \
			"$rr_marker" |
			proxy_script 2>&1
	)
	rc=$?
	snapshot_split_counters rr_split_after
	result=$(last_endpoint_payload_from_output "$out")
	endpoint=$(payload_endpoint "$result")
	value=$(payload_value "$result")
	success_count=$(split_delta rr_split_before rr_split_after success)
	split_total=$(split_delta rr_split_before rr_split_after total)
	admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	if [ "$rc" -eq 0 ] &&
		[ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
		[ "$value" = "$rr_marker" ] &&
		[ "$success_count" -eq 0 ] &&
		[ "$split_total" -eq 0 ]; then
		ok 0 "GLOBAL_LSN keeps non-READ COMMITTED transaction reads on writer"
	else
		diag "non-RC transaction output: $out"
		diag "result=$result endpoint=$endpoint value=$value expected_writer=$PRIMARY_SERVER_ENDPOINT"
		diag "split_success_delta=$success_count split_total_delta=$split_total blocked_delta=$(split_delta rr_split_before rr_split_after blocked)"
		ok 1 "GLOBAL_LSN keeps non-READ COMMITTED transaction reads on writer"
	fi
}

PLAN="$(global_lsn_selected_plan)"
plan "$PLAN"
if [ -n "$GLOBAL_LSN_TAP_GROUPS" ]; then
	diag "selected GLOBAL_LSN TAP groups: $GLOBAL_LSN_TAP_GROUPS"
fi
if [ -n "$GLOBAL_LSN_TAP_CASES" ]; then
	diag "selected GLOBAL_LSN TAP cases: $GLOBAL_LSN_TAP_CASES"
fi
diag "test profile: POLARDB_TEST_ENV=$POLARDB_TEST_ENV writer=${PRIMARY_HOST:-auto}:${PRIMARY_PORT:-auto} readers=${POLARDB_REPLICA_ENDPOINTS:-${REPLICA_HOST:-auto}:${REPLICA_PORT:-auto}}"
polardb_require_proxysql_or_skip_all polardb
polardb_require_command_or_skip_all psql psql

if ! detect_topology; then
	skip_ok "detect PolarDB writer/reader topology" "PolarDB topology unavailable"
	polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
	exit 0
fi
ok 0 "detect PolarDB writer/reader topology"

if detect_backend_ports; then
	ok 0 "detect backend ports reported by PostgreSQL"
else
	ok 1 "detect backend ports reported by PostgreSQL"
	exit 1
fi

ok 0 "built POLARDB_PROXY=1 ProxySQL binary exists"

if start_proxy; then
	ok 0 "start ProxySQL test instance"
else
	diag "ProxySQL start log: $PROXYSQL_START_LOG"
	sed 's/^/#   /' "$PROXYSQL_START_LOG" 2>/dev/null || true
	ok 1 "start ProxySQL test instance"
	exit 1
fi

if configure_proxy; then
	ok 0 "configure GLOBAL_LSN writer/reader hostgroups and SELECT rule"
else
	ok 1 "configure GLOBAL_LSN writer/reader hostgroups and SELECT rule"
	exit 1
fi

if direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP TABLE IF EXISTS $TEST_TABLE; CREATE TABLE $TEST_TABLE(id int PRIMARY KEY, data text);" >/dev/null 2>&1; then
	ok 0 "create test table on writer"
else
	ok 1 "create test table on writer"
	exit 1
fi

run_selected_global_lsn_case 1 case_global_variable_alias
run_selected_global_lsn_case 2 case_hostgroup_alias
run_selected_global_lsn_case 3 case_session_set_global_lsn
run_selected_global_lsn_case 4 case_cross_session_global_read
run_selected_global_lsn_case 5 case_missing_writer_mirror_fails_closed
run_selected_global_lsn_case 6 case_global_lsn_best_effort_timeout_is_strict
run_selected_global_lsn_case 7 case_global_lsn_transaction_split_uses_replica
run_selected_global_lsn_case 8 case_global_lsn_begin_warmup_requests_reader
run_selected_global_lsn_case 9 case_non_read_committed_transaction_stays_primary

exit "$FAIL"
