#!/usr/bin/env bash
# TAP integration test for PolarDB LSN session consistency.
#
# Covers:
#   - live write -> read RYW
#   - wrapper result filtering
#   - no-write read
#   - same-session read -> read monotonic target protection
#   - same-session read-only -> write -> protected read transition
#   - background writes coexist with protected read-only session reads
#   - same-session off -> lsn -> off mode transition
#   - mode=off and mode=primary
#   - manual hostgroup routes bypass the PolarDB planner
#   - explicit transaction routing to primary
#   - transaction-split pre-write routing stays safe before split evidence exists
#   - monitor_lsn_updates=off
#   - admin query-rule replica_eligible save/load
#   - lag-cap in-cap and stale-cache routing behavior
#   - cold replica pool create-new for consistency waits
#   - RFQ-vs-monitor LSN stat separation
#   - freshness-controlled lag cap fallback to primary
#   - wrapper SET timeout/error attribution
#   - extended protocol stays outside v1 PolarDB wait wrapping
#   - debug fault injection shows wrapper-finalize failure stops before RunQuery
#
# Requires the writer and reader endpoints configured in `.env` and a built
# POLARDB_PROXY=1 ProxySQL binary.
#
# Strict/best-effort timeout edge checks intentionally alter managed replay lag.
# Enable them explicitly with POLARDB_TIMEOUT_EDGE_TESTS=1.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"
PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_lsn_session_consistency_tap")}"
PROXYSQL_START_LOG="${PROXYSQL_DATA_DIR}.start.log"
POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE="${POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE:-${PROXYSQL_DATA_DIR}.reader_acquire_fault}"
POLARDB_DEBUG_MONITOR_HEALTH_FILE="${POLARDB_DEBUG_MONITOR_HEALTH_FILE:-${PROXYSQL_DATA_DIR}.monitor_health_fault}"
POLARDB_DEBUG_STARTUP_IDENTITY_FILE="${POLARDB_DEBUG_STARTUP_IDENTITY_FILE:-${PROXYSQL_DATA_DIR}.startup_identity_fault}"
POLARDB_DEBUG_WRAP_SET_ERROR_FILE="${POLARDB_DEBUG_WRAP_SET_ERROR_FILE:-${PROXYSQL_DATA_DIR}.wrap_set_error_fault}"
PRIMARY_SERVER_PORT="${PRIMARY_SERVER_PORT:-}"
REPLICA_SERVER_PORT="${REPLICA_SERVER_PORT:-}"
REPLICA_SERVER_ENDPOINTS="${REPLICA_SERVER_ENDPOINTS:-}"
TEST_TABLE="${TEST_TABLE:-$(polardb_test_identifier polardb_lsn_session_consistency_tap)}"
EXTENDED_HELPER="${EXTENDED_HELPER:-$PROXYSQL_ROOT/test/polardb/bin/proxysql_extended_protocol_test}"
EXTENDED_HELPER_RUNNER="${EXTENDED_HELPER_RUNNER:-$PROXYSQL_ROOT/test/polardb/test-c/run_helper.sh}"
MONITOR_HEALTH_SUPPORTED=0
PROXYSQL_PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}"
DIRECT_PGSSLMODE="${DIRECT_PGSSLMODE:-$PGSSLMODE}"
export POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE="${POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE:-1}"

# Primary/replica hostgroups come from env.sh (defaults 10/11) so the
# topology ids are never hardcoded. The throwaway hostgroup-pair ids used to
# isolate per-scenario proxy_protocol policies (12..40) stay as literals.
WRITER_HG="$POLARDB_WRITER_HG"
READER_HG="$POLARDB_READER_HG"

PLAN=70
FAIL=0
STARTED_PROXY=0
TIMEOUT_EDGE_LAG_SET=0
EXTENDED_HELPER_BUILD_LOG="${PROXYSQL_DATA_DIR}.extended-helper-build.log"

# admin_sql, proxy_sql, proxy_script, counter, counter_delta, global/runtime
# variable helpers, trace helpers, debug-fault file helpers, extract_endpoint,
# set_select_rule_auto, set_select_rule_manual_reader, wait_until and the
# assert_routed_read/assert_waited_read helpers all come from lib/tap_polardb.sh.
# The remaining helpers below are lsn-specific.
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

pool_value() {
    local hg="$1"
    local col="$2"
    local v
    v=$(admin_sql "SELECT COALESCE(SUM($col),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$hg;" 2>/dev/null | tr -d '[:space:]')
    printf '%s\n' "${v:-0}"
}

set_route_rfq_policy() {
    set_global_var_runtime "pgsql-polardb_route_rfq_policy" "$1"
}

set_session_lsn_baseline() {
    set_global_var_runtime "pgsql-polardb_session_lsn_baseline" "$1"
}

set_wait_mode() {
    local mode="$1"
    set_global_var_runtime "pgsql-polardb_wait_timeout_mode" "$mode"
}

polardb_dcs_set() {
    polardb_run_dcs "$@"
}

_backend_setting_is() {
    [ "$(direct_sql "$1" "$2" "SELECT current_setting('$3', true);" 2>/dev/null | tr -d '[:space:]')" = "$4" ]
}
wait_for_backend_setting() {
    local host="$1"
    local port="$2"
    local name="$3"
    local expected="$4"
    local timeout_sec="${5:-15}"
    wait_until _backend_setting_is "$host" "$port" "$name" "$expected" -- "$timeout_sec" 0.5
}

set_replay_lag_bytes() {
    local lag_bytes="$1"

    if ! polardb_dcs_set "polar_replay_min_lag_size=$lag_bytes" >/dev/null 2>&1; then
        return 1
    fi
    [ "$lag_bytes" != "0" ] && TIMEOUT_EDGE_LAG_SET=1
    if ! wait_for_backend_setting "$REPLICA_HOST" "$REPLICA_PORT" polar_replay_min_lag_size "$lag_bytes" 15; then
        diag "polar_replay_min_lag_size=$lag_bytes was pushed through $POLARDB_DCS_MODE DCS but is not visible on $REPLICA_HOST:$REPLICA_PORT"
        return 1
    fi
    sleep 1
    return 0
}

_replica_replay_reaches_lsn() {
    local target_lsn="$1"
    [ "$(direct_sql "$REPLICA_HOST" "$REPLICA_PORT" "SELECT pg_wal_lsn_diff(pg_last_wal_replay_lsn(), '$target_lsn'::pg_lsn) >= 0;" 2>/dev/null | tr -d '[:space:]')" = "t" ]
}

wait_for_replica_replay_catchup() {
    local timeout_sec="${1:-20}"
    local target_lsn

    target_lsn=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT pg_current_wal_lsn();" 2>/dev/null | tr -d '[:space:]') || return 1
    [ -n "$target_lsn" ] || return 1
    wait_until _replica_replay_reaches_lsn "$target_lsn" -- "$timeout_sec" 0.5
}

set_hg_policy() {
    local mode="$1"
    local max_lag_bytes="${2:--1}"
    local timeout_ms="${3:-5000}"
    admin_sql "UPDATE pgsql_replication_hostgroups SET consistency_mode='$mode', max_lag_bytes=$max_lag_bytes, lsn_wait_timeout_ms=$timeout_ms WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

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

insert_first_reader_server() {
    local reader_hg="$1"
    local max_connections="${2:-100}"

    admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($reader_hg, '$REPLICA_HOST', $REPLICA_PORT, 'ONLINE', 1000, $max_connections);" >/dev/null
}

set_hg_pair_policy() {
    local writer_hg="$1"
    local reader_hg="$2"
    local proxy_protocol="$3"
    local mode="${4:-lsn}"
    local max_lag_bytes="${5:--1}"
    local timeout_ms="${6:-5000}"
    local max_connections="${7:-100}"

    admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id IN ($writer_hg,$reader_hg);" >/dev/null
    admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($writer_hg, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, $max_connections);" >/dev/null
    insert_reader_servers "$reader_hg" "$max_connections"
    admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup=$writer_hg;" >/dev/null
    admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES ($writer_hg, $reader_hg, 'polardb', '$mode', $max_lag_bytes, $timeout_ms, '$proxy_protocol', 'lsn_session_consistency_extra_pair');" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

set_hg_pair_policy_one_reader() {
    local writer_hg="$1"
    local reader_hg="$2"
    local proxy_protocol="$3"
    local mode="${4:-lsn}"
    local max_lag_bytes="${5:--1}"
    local timeout_ms="${6:-5000}"
    local max_connections="${7:-100}"

    admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id IN ($writer_hg,$reader_hg);" >/dev/null
    admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($writer_hg, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, $max_connections);" >/dev/null
    insert_first_reader_server "$reader_hg" "$max_connections"
    admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup=$writer_hg;" >/dev/null
    admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES ($writer_hg, $reader_hg, 'polardb', '$mode', $max_lag_bytes, $timeout_ms, '$proxy_protocol', 'lsn_session_consistency_single_reader_pair');" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

set_pair_proxy_protocol() {
    local writer_hg="$1"
    local proxy_protocol="$2"
    admin_sql "UPDATE pgsql_replication_hostgroups SET proxy_protocol='$proxy_protocol' WHERE writer_hostgroup=$writer_hg;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

set_default_hostgroup() {
    local writer_hg="$1"
    admin_sql "UPDATE pgsql_users SET default_hostgroup=$writer_hg WHERE username='$PGUSER';" >/dev/null
    admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
}

remove_extra_hg_pairs() {
    admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup BETWEEN 13 AND 40;" >/dev/null
    admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id BETWEEN 13 AND 40;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

debug_reader_acquire_faults_available() {
    debug_fault_file_available POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE "$PROXYSQL_DATA_DIR/proxysql_strings"
}

set_debug_reader_acquire_fault() {
    set_debug_fault_file POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE "$1"
}

clear_debug_reader_acquire_fault() {
    clear_debug_fault_file POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE
}

debug_monitor_health_faults_available() {
    debug_fault_file_available POLARDB_DEBUG_MONITOR_HEALTH_FILE "$PROXYSQL_DATA_DIR/proxysql_strings"
}

debug_startup_identity_faults_available() {
    debug_fault_file_available POLARDB_DEBUG_STARTUP_IDENTITY_FILE "$PROXYSQL_DATA_DIR/proxysql_strings"
}

debug_wrap_set_error_faults_available() {
    debug_fault_file_available POLARDB_DEBUG_WRAP_SET_ERROR_FILE "$PROXYSQL_DATA_DIR/proxysql_strings"
}

set_debug_monitor_health() {
    local endpoint="$1"
    local node_type="$2"
    local is_available="$3"
    local lsn="$4"
    printf '%s|%s|%s|%s\n' "$endpoint" "$node_type" "$is_available" "$lsn" >"$POLARDB_DEBUG_MONITOR_HEALTH_FILE"
}

set_debug_startup_identity_fault() {
    set_debug_fault_file POLARDB_DEBUG_STARTUP_IDENTITY_FILE "$1"
}

set_debug_wrap_set_error() {
    set_debug_fault_file POLARDB_DEBUG_WRAP_SET_ERROR_FILE 1
}

lsn_trace_log() {
    printf '%s/proxysql.log\n' "$PROXYSQL_DATA_DIR"
}

lsn_trace_count() {
    local pattern="$1"
    tap_trace_count "$(lsn_trace_log)" "$pattern"
}

lsn_trace_delta() {
    local pattern="$1"
    local before="$2"
    local after

    if ! tap_trace_checks_enabled "$(lsn_trace_log)"; then
        echo -1
        return 0
    fi
    after=$(lsn_trace_count "$pattern")
    echo $((after - before))
}

runtime_server_status() {
    local hg="$1"
    local host="$2"
    local port="$3"
    admin_sql "SELECT status FROM runtime_pgsql_servers WHERE hostgroup_id=$hg AND hostname='$host' AND port=$port LIMIT 1;" 2>/dev/null | tr -d '[:space:]'
}

_counter_gt() { [ "$(counter "$1")" -gt "$2" ]; }
wait_for_counter_increment() {
    local name="$1"
    local before="$2"
    local timeout_sec="${3:-20}"
    wait_until _counter_gt "$name" "$before" -- "$timeout_sec" 1
}

_runtime_server_status_is() { [ "$(runtime_server_status "$1" "$2" "$3")" = "$4" ]; }
wait_for_runtime_server_status() {
    local hg="$1"
    local host="$2"
    local port="$3"
    local expected="$4"
    local timeout_sec="${5:-20}"
    wait_until _runtime_server_status_is "$hg" "$host" "$port" "$expected" -- "$timeout_sec" 1
}

restore_reader_online() {
    admin_sql "UPDATE pgsql_servers SET status='ONLINE' WHERE hostgroup_id=$READER_HG AND hostname='$REPLICA_HOST' AND port=$REPLICA_PORT;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

set_lsn_mode() {
    local mode="$1"
    set_global_var_runtime "pgsql-polardb_consistency_mode" "$mode"
    set_hg_policy "$mode" -1 5000
}

# set_select_rule_auto / set_select_rule_manual_reader come from
# lib/tap_polardb.sh (their default comments match the values used here).
clear_select_rules() {
    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

backend_endpoint() {
    proxy_sql "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>/dev/null | tr -d '[:space:]'
}

ensure_extended_helper() {
    [ -x "$EXTENDED_HELPER" ] && return 0
    "$EXTENDED_HELPER_RUNNER" --build-only extended_protocol >"$EXTENDED_HELPER_BUILD_LOG" 2>&1
}

run_extended_query() {
    local sql="$1"
    if [ "$EXTENDED_HELPER" != "$PROXYSQL_ROOT/test/polardb/bin/proxysql_extended_protocol_test" ]; then
        env \
            LD_LIBRARY_PATH="$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq:${LD_LIBRARY_PATH:-}" \
            PROXYSQL_HOST="$PROXYSQL_HOST" \
            PROXYSQL_PORT="$PROXYSQL_PORT" \
            PGDB="$PGDB" \
            PGUSER="$PGUSER" \
            PGPASSWORD="$PGPASSWORD" \
            PGSSLMODE="$PROXYSQL_PGSSLMODE" \
            POLARDB_EXTENDED_SETUP_SQL="${POLARDB_EXTENDED_SETUP_SQL:-}" \
            POLARDB_EXTENDED_SETUP_SQL2="${POLARDB_EXTENDED_SETUP_SQL2:-}" \
            POLARDB_EXTENDED_REPORT_NOTICES="${POLARDB_EXTENDED_REPORT_NOTICES:-}" \
            POLARDB_EXTENDED_PREPARED="${POLARDB_EXTENDED_PREPARED:-}" \
            "$EXTENDED_HELPER" "$sql"
        return
    fi

    env \
        PROXYSQL_HOST="$PROXYSQL_HOST" \
        PROXYSQL_PORT="$PROXYSQL_PORT" \
        PGDB="$PGDB" \
        PGUSER="$PGUSER" \
        PGPASSWORD="$PGPASSWORD" \
        PGSSLMODE="$PROXYSQL_PGSSLMODE" \
        POLARDB_EXTENDED_SETUP_SQL="${POLARDB_EXTENDED_SETUP_SQL:-}" \
        POLARDB_EXTENDED_SETUP_SQL2="${POLARDB_EXTENDED_SETUP_SQL2:-}" \
        POLARDB_EXTENDED_REPORT_NOTICES="${POLARDB_EXTENDED_REPORT_NOTICES:-}" \
        POLARDB_EXTENDED_PREPARED="${POLARDB_EXTENDED_PREPARED:-}" \
        "$EXTENDED_HELPER_RUNNER" extended_protocol "$sql"
}

run_protocol_rfq_lsn_scope() {
    local protocol="$1"
    local writer_hg="$2"
    local reader_hg="$3"
    local row_id="$4"
    local marker out result query_lsn_delta wait_delta bypass_delta protect_delta

    set_global_var_runtime "pgsql-polardb_proxy_protocol" "$protocol"
    set_hg_pair_policy "$writer_hg" "$reader_hg" default lsn -1 5000
    set_default_hostgroup "$writer_hg"

    snapshot_consistency_counters protocol_before
    marker="protocol_${protocol}_$$"
    out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES ($row_id, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=$row_id;
SQL
    )
    snapshot_consistency_counters protocol_after
    query_lsn_delta=$(snapshot_counter_delta protocol_before protocol_after query_lsn)
    wait_delta=$(snapshot_counter_delta protocol_before protocol_after wait)
    bypass_delta=$(snapshot_counter_delta protocol_before protocol_after bypass)
    protect_delta=$(snapshot_protect_delta protocol_before protocol_after)
    result=$(last_endpoint_payload_from_output "$out")
    if reader_payload_matches "$result" "$marker" &&
        [ "$query_lsn_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "protocol $protocol: RFQ LSN supports protected reader read"
    else
        diag "$protocol output: $out"
        diag "result=$result expected=$REPLICA_SERVER_ENDPOINT|$marker query_lsn_delta=$query_lsn_delta wait_delta=$wait_delta bypass_delta=$bypass_delta"
        ok 1 "protocol $protocol: RFQ LSN supports protected reader read"
    fi
}

run_protocol_off_scope() {
    local writer_hg="$1"
    local reader_hg="$2"
    local row_id="$3"
    local write_missing_before write_missing_after wait_before wait_after
    local marker out endpoint

    set_global_var_runtime "pgsql-polardb_proxy_protocol" "off"
    set_hg_pair_policy "$writer_hg" "$reader_hg" default lsn -1 5000
    set_default_hostgroup "$writer_hg"

    write_missing_before=$(counter PolarDB_Write_Missing_LSN)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    marker="protocol_off_$$"
    out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES ($row_id, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    write_missing_after=$(counter PolarDB_Write_Missing_LSN)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(last_endpoint_from_output "$out")
    if [ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ $((write_missing_after - write_missing_before)) -ge 1 ] &&
        [ $((wait_after - wait_before)) -eq 0 ]; then
        ok 0 "protocol off: missing writer RFQ LSN keeps automatic read on writer"
    else
        diag "off output: $out"
        diag "endpoint=$endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT write_missing_delta=$((write_missing_after - write_missing_before)) wait_delta=$((wait_after - wait_before))"
        ok 1 "protocol off: missing writer RFQ LSN keeps automatic read on writer"
    fi
}

run_protocol_hg_override_scope() {
    local global_protocol="$1"
    local hg_protocol="$2"
    local writer_hg="$3"
    local reader_hg="$4"
    local row_id="$5"
    local marker out result query_lsn_delta wait_delta bypass_delta protect_delta

    set_global_var_runtime "pgsql-polardb_proxy_protocol" "$global_protocol"
    set_hg_pair_policy "$writer_hg" "$reader_hg" "$hg_protocol" lsn -1 5000
    set_default_hostgroup "$writer_hg"

    snapshot_consistency_counters protocol_override_before
    marker="protocol_override_${hg_protocol}_$$"
    out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES ($row_id, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=$row_id;
SQL
    )
    snapshot_consistency_counters protocol_override_after
    query_lsn_delta=$(snapshot_counter_delta protocol_override_before protocol_override_after query_lsn)
    wait_delta=$(snapshot_counter_delta protocol_override_before protocol_override_after wait)
    bypass_delta=$(snapshot_counter_delta protocol_override_before protocol_override_after bypass)
    protect_delta=$(snapshot_protect_delta protocol_override_before protocol_override_after)
    result=$(last_endpoint_payload_from_output "$out")
    local label
    if [ "$hg_protocol" = "default" ]; then
        label="protocol default: HG inherits global $global_protocol for protected reader read"
    else
        label="protocol override: HG $hg_protocol overrides global $global_protocol for protected reader read"
    fi

    if reader_payload_matches "$result" "$marker" &&
        [ "$query_lsn_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "$label"
    else
        diag "override output: $out"
        diag "result=$result expected=$REPLICA_SERVER_ENDPOINT|$marker query_lsn_delta=$query_lsn_delta wait_delta=$wait_delta bypass_delta=$bypass_delta"
        ok 1 "$label"
    fi
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
        topology_error="$(cat "$topology_error_file" 2>/dev/null || true)"
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
        REPLICA_SERVER_ENDPOINTS="${REPLICA_SERVER_ENDPOINTS:-$REPLICA_SERVER_ENDPOINT}"
        export REPLICA_SERVER_ENDPOINTS
        diag "server-reported backend endpoints from env: writer=$PRIMARY_SERVER_ENDPOINT readers=$REPLICA_SERVER_ENDPOINTS"
        return 0
    fi

    local primary_server_addr endpoint host port replica_server_addr replica_server_port replica_server_endpoint
    primary_server_addr=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT host(inet_server_addr());" 2>/dev/null | tr -d '[:space:]')
    PRIMARY_SERVER_PORT=$(direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]')
    REPLICA_SERVER_PORT=$(direct_sql "$REPLICA_HOST" "$REPLICA_PORT" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]')
    PRIMARY_SERVER_ENDPOINT="${primary_server_addr}:${PRIMARY_SERVER_PORT}"
    REPLICA_SERVER_ENDPOINTS=""
    for endpoint in $(polardb_each_replica_endpoint); do
        host=$(polardb_endpoint_host "$endpoint")
        port=$(polardb_endpoint_port "$endpoint")
        replica_server_addr=$(direct_sql "$host" "$port" "SELECT host(inet_server_addr());" 2>/dev/null | tr -d '[:space:]')
        replica_server_port=$(direct_sql "$host" "$port" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]')
        if [ -n "$replica_server_addr" ] && [ -n "$replica_server_port" ]; then
            replica_server_endpoint="${replica_server_addr}:${replica_server_port}"
            REPLICA_SERVER_ENDPOINTS=$(polardb_append_endpoint_once "$REPLICA_SERVER_ENDPOINTS" "$replica_server_endpoint")
            if [ -z "$REPLICA_SERVER_ENDPOINT" ]; then
                REPLICA_SERVER_ENDPOINT="$replica_server_endpoint"
                REPLICA_SERVER_PORT="$replica_server_port"
            fi
        fi
    done
    export PRIMARY_SERVER_ENDPOINT REPLICA_SERVER_ENDPOINT REPLICA_SERVER_ENDPOINTS
    diag "server-reported backend endpoints: writer=$PRIMARY_SERVER_ENDPOINT readers=$REPLICA_SERVER_ENDPOINTS"
    [ -n "$primary_server_addr" ] && [ -n "$PRIMARY_SERVER_PORT" ] && [ -n "$REPLICA_SERVER_ENDPOINTS" ]
}

reader_endpoint_matches() {
    local endpoint="$1"
    local reader

    for reader in $REPLICA_SERVER_ENDPOINTS; do
        [ "$endpoint" = "$reader" ] && return 0
    done
    return 1
}

reader_payload_matches() {
    local result="$1"
    local payload="$2"
    local reader

    for reader in $REPLICA_SERVER_ENDPOINTS; do
        [ "$result" = "$reader|$payload" ] && return 0
    done
    return 1
}

reader_expectation() {
    printf '%s\n' "${REPLICA_SERVER_ENDPOINTS:-$REPLICA_SERVER_ENDPOINT}"
}

detect_monitor_health_support() {
    if direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT polar_node_type(), polar_is_available();" >/dev/null 2>&1; then
        MONITOR_HEALTH_SUPPORTED=1
        diag "PolarDB monitor health functions are available"
    else
        MONITOR_HEALTH_SUPPORTED=0
        diag "PolarDB monitor health functions are unavailable; monitor LSN assertions will be skipped"
    fi
}

start_proxy() {
    rm -f "$PROXYSQL_START_LOG"
    rm -f "$POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE"
    rm -f "$POLARDB_DEBUG_MONITOR_HEALTH_FILE"
    rm -f "$POLARDB_DEBUG_STARTUP_IDENTITY_FILE"
    rm -f "$POLARDB_DEBUG_WRAP_SET_ERROR_FILE"
    POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE="$POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE" \
        POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE="$POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE" \
        POLARDB_DEBUG_MONITOR_HEALTH_FILE="$POLARDB_DEBUG_MONITOR_HEALTH_FILE" \
        POLARDB_DEBUG_STARTUP_IDENTITY_FILE="$POLARDB_DEBUG_STARTUP_IDENTITY_FILE" \
        POLARDB_DEBUG_WRAP_SET_ERROR_FILE="$POLARDB_DEBUG_WRAP_SET_ERROR_FILE" \
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
    if [ "$TIMEOUT_EDGE_LAG_SET" -eq 1 ]; then
        set_replay_lag_bytes 0 >/dev/null 2>&1 || true
    fi
    rm -f "$POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE"
    rm -f "$POLARDB_DEBUG_MONITOR_HEALTH_FILE"
    rm -f "$POLARDB_DEBUG_STARTUP_IDENTITY_FILE"
    rm -f "$POLARDB_DEBUG_WRAP_SET_ERROR_FILE"
    admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name IN ('pgsql-polardb_lag_ms','pgsql-polardb_lag_bytes');" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='5000' WHERE variable_name='pgsql-polardb_lsn_freshness_ms';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='strict' WHERE variable_name='pgsql-polardb_route_rfq_policy';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='observed' WHERE variable_name='pgsql-polardb_session_lsn_baseline';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='' WHERE variable_name='pgsql-polardb_proxy_identity_host';" >/dev/null 2>&1 || true
    admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name='pgsql-polardb_proxy_identity_port';" >/dev/null 2>&1 || true
    admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null 2>&1 || true
    admin_sql "UPDATE pgsql_users SET default_hostgroup=$WRITER_HG WHERE username='$PGUSER'; LOAD PGSQL USERS TO RUNTIME;" >/dev/null 2>&1 || true
    remove_extra_hg_pairs >/dev/null 2>&1 || true
    direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP TABLE IF EXISTS $TEST_TABLE;" >/dev/null 2>&1 || true
    stop_proxy
}
trap cleanup EXIT

configure_proxy() {
    admin_sql "DELETE FROM pgsql_servers;" >/dev/null
    admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, 100);" >/dev/null
    insert_reader_servers "$READER_HG" 100

    admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
    admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, comment) VALUES ($WRITER_HG, $READER_HG, 'polardb', 'lsn', -1, 5000, 'lsn_session_consistency_tap');" >/dev/null

    admin_sql "DELETE FROM pgsql_users;" >/dev/null
    admin_sql "INSERT INTO pgsql_users (username, password, active, default_hostgroup) VALUES ('$PGUSER', '$PGPASSWORD', 1, $WRITER_HG);" >/dev/null

    set_select_rule_auto

    admin_sql "UPDATE global_variables SET variable_value='lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name IN ('pgsql-polardb_lag_ms','pgsql-polardb_lag_bytes');" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='1000' WHERE variable_name='pgsql-polardb_lag_wait_ms';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='5000' WHERE variable_name='pgsql-polardb_lsn_freshness_ms';" >/dev/null

    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
    admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
    admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
}

wait_for_monitor_increment() {
    local before="$1"
    local timeout_sec="${2:-20}"
    wait_until _counter_gt PolarDB_LSN_Updates_From_Monitor "$before" -- "$timeout_sec" 1
}

# ---------------------------------------------------------------------------
# Scenario functions. Each wraps one (or one if/else) inline scenario verbatim,
# including the set_*/config setup lines that precede it. They are invoked in
# original top-to-bottom order by the driver call list at the end of the file.
# ---------------------------------------------------------------------------

# CF-4: replica_eligible values -1/0/1 in query rules must round-trip
# intact through a runtime-to-memory SAVE/LOAD cycle.
case_cf4_replica_eligible_roundtrip() {
    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, replica_eligible, apply, comment) VALUES (9101, 1, '^SELECT cf4_unset', -1, 0, 'cf4_re_unset');" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, replica_eligible, apply, comment) VALUES (9102, 1, '^SELECT cf4_primary', 0, 0, 'cf4_re_zero');" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, replica_eligible, apply, comment) VALUES (9103, 1, '^SELECT cf4_auto', 1, 0, 'cf4_re_one');" >/dev/null
    admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "SAVE PGSQL QUERY RULES TO MEMORY;" >/dev/null
    cf4_values=$(admin_sql "SELECT group_concat(replica_eligible, ',') FROM (SELECT replica_eligible FROM pgsql_query_rules WHERE rule_id BETWEEN 9101 AND 9103 ORDER BY rule_id);" 2>/dev/null | tr -d '[:space:]')
    if [ "$cf4_values" = "-1,0,1" ]; then
        ok 0 "CF-4: replica_eligible -1/0/1 survives runtime-to-memory save/load"
    else
        diag "cf4 saved replica_eligible values=$cf4_values expected=-1,0,1"
        ok 1 "CF-4: replica_eligible -1/0/1 survives runtime-to-memory save/load"
    fi
    set_select_rule_auto
}

# CF-7: when the LSN wait wrapper cannot be finalized safely, the proxy must
# return an internal error before any backend dispatch (debug fault).
case_cf7_wrapper_finalize_fail() {
    set_lsn_mode lsn
    cf7_abort_before=$(counter PolarDB_Wait_Wrap_Safety_Abort)
    cf7_sent_before=$(counter PolarDB_Wait_LSN_Sent)
    cf7_trace_pattern="PolarDB WRAP: finalize failed: debug fault injection"
    cf7_trace_before=$(lsn_trace_count "$cf7_trace_pattern")
    cf7_marker="cf7_finalize_fail_$$"
    cf7_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (700, '$cf7_marker') ON CONFLICT (id) DO UPDATE SET data='$cf7_marker';
SELECT data FROM $TEST_TABLE WHERE id=700;
SQL
    )
    cf7_rc=$?
    cf7_abort_after=$(counter PolarDB_Wait_Wrap_Safety_Abort)
    cf7_sent_after=$(counter PolarDB_Wait_LSN_Sent)
    cf7_abort_delta=$((cf7_abort_after - cf7_abort_before))
    cf7_sent_delta=$((cf7_sent_after - cf7_sent_before))
    cf7_trace_delta=$(lsn_trace_delta "$cf7_trace_pattern" "$cf7_trace_before")
    if [ "$cf7_rc" -ne 0 ] &&
        [ "$cf7_abort_delta" -eq 1 ] &&
        [ "$cf7_sent_delta" -eq 0 ] &&
        { [ "$cf7_trace_delta" -eq 1 ] || [ "$cf7_trace_delta" -eq -1 ]; } &&
        printf '%s\n' "$cf7_out" | grep -Fq "PolarDB LSN wait wrapper could not be built safely"; then
        ok 0 "CF-7: wrapper finalize failure returns internal error before backend dispatch"
    elif [ "$cf7_rc" -eq 0 ] && [ "$cf7_abort_delta" -eq 0 ]; then
        skip_ok "CF-7: wrapper finalize failure returns internal error before backend dispatch" "requires POLARDB_DEBUG binary with POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE=1"
    else
        diag "cf7 output: $cf7_out"
        diag "cf7_rc=$cf7_rc safety_abort_delta=$cf7_abort_delta wait_sent_delta=$cf7_sent_delta trace_delta=$cf7_trace_delta pattern='$cf7_trace_pattern' (-1 means trace unavailable)"
        ok 1 "CF-7: wrapper finalize failure returns internal error before backend dispatch"
    fi
}

# Write, then read (read-your-writes): a write raises the session's target to the
# write's LSN, so the next read returns the new row from a replica (served either by
# adding a wait, or by a replica already past that LSN). The write is the floor for
# the next read.
case_ryw_write_read() {
    snapshot_consistency_counters ryw_before
    marker="lsn_tap_$$"
    ryw_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (1, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
SELECT data FROM $TEST_TABLE WHERE id=1;
SQL
    )
    snapshot_consistency_counters ryw_after
    prepared_delta=$(snapshot_counter_delta ryw_before ryw_after prepared)
    wait_delta=$(snapshot_counter_delta ryw_before ryw_after wait)
    bypass_delta=$(snapshot_counter_delta ryw_before ryw_after bypass)
    protect_delta=$(snapshot_protect_delta ryw_before ryw_after)
    query_lsn_delta=$(snapshot_counter_delta ryw_before ryw_after query_lsn)
    marker_count=$(printf '%s\n' "$ryw_out" | grep -c "^$marker$" || true)
    if [ "$marker_count" -eq 1 ] &&
        [ "$prepared_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ] &&
        [ "$query_lsn_delta" -ge 1 ]; then
        ok 0 "RYW: write advances LSN and following read is protected on reader"
    else
        diag "ryw output: $ryw_out"
        diag "prepared delta=$prepared_delta wait-sent delta=$wait_delta bypass_delta=$bypass_delta query-lsn delta=$query_lsn_delta marker_count=$marker_count"
        ok 1 "RYW: write advances LSN and following read is protected on reader"
    fi
}

# Wrapper filter: the client must see only its own SELECT result, never the
# internal wait-wrapper SET/wait statements.
case_wrapper_filter() {
    if [ "$marker_count" -eq 1 ] && ! printf '%s\n' "$ryw_out" | grep -Eq 'polar_xact_split_wait_lsn|^SET$'; then
        ok 0 "wrapper filter: client sees only user SELECT result"
    else
        diag "ryw output: $ryw_out"
        ok 1 "wrapper filter: client sees only user SELECT result"
    fi
}

# The planner's wait spec is the only stored wait target. This full proxy-path
# check makes sure backend reader selection still receives that target: the read
# must return from a reader, must be protected by wait-or-bypass accounting, and
# must move the target-specific selection counters.
case_route_wait_spec_reader_selection() {
    set_select_rule_auto
    set_lsn_mode lsn
    set_global_var_runtime "pgsql-polardb_proxy_protocol" "v15"
    set_global_var_runtime "pgsql-polardb_route_rfq_policy" "strict"
    set_global_var_runtime "pgsql-polardb_session_lsn_baseline" "observed"
    set_hg_policy lsn -1 5000

    snapshot_consistency_counters route_wait_before
    route_no_wait_before=$(counter PolarDB_Route_No_Wait_Target)
    preferred_before=$(counter PolarDB_Target_LSN_Preferred)
    fallback_wait_before=$(counter PolarDB_Target_LSN_Fallback_Wait)

    route_wait_marker="route_wait_spec_$$"
    route_wait_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (9700, '$route_wait_marker') ON CONFLICT (id) DO UPDATE SET data='$route_wait_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9700;
SQL
    )

    snapshot_consistency_counters route_wait_after
    route_no_wait_after=$(counter PolarDB_Route_No_Wait_Target)
    preferred_after=$(counter PolarDB_Target_LSN_Preferred)
    fallback_wait_after=$(counter PolarDB_Target_LSN_Fallback_Wait)

    route_wait_result=$(last_endpoint_payload_from_output "$route_wait_out")
    query_lsn_delta=$(snapshot_counter_delta route_wait_before route_wait_after query_lsn)
    protect_delta=$(snapshot_protect_delta route_wait_before route_wait_after)
    wrap_or_bypass_delta=$(snapshot_wrap_or_bypass_delta route_wait_before route_wait_after)
    route_no_wait_delta=$((route_no_wait_after - route_no_wait_before))
    target_select_delta=$((preferred_after - preferred_before + fallback_wait_after - fallback_wait_before))

    if reader_payload_matches "$route_wait_result" "$route_wait_marker" &&
        [ "$query_lsn_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ] &&
        [ "$wrap_or_bypass_delta" -ge 1 ] &&
        [ "$route_no_wait_delta" -eq 0 ] &&
        [ "$target_select_delta" -ge 1 ]; then
        ok 0 "route wait spec reaches reader selection on live topology"
    else
        diag "route-wait output: $route_wait_out"
        diag "result='$route_wait_result' expected_reader_payload='$(reader_expectation)|$route_wait_marker'"
        diag "query_lsn_delta=$query_lsn_delta protect_delta=$protect_delta wrap_or_bypass_delta=$wrap_or_bypass_delta route_no_wait_delta=$route_no_wait_delta"
        diag "target_select_delta=$target_select_delta"
        diag "preferred_delta=$((preferred_after - preferred_before)) fallback_wait_delta=$((fallback_wait_after - fallback_wait_before))"
        ok 1 "route wait spec reaches reader selection on live topology"
    fi
}

# No prior write: the first read has no target yet, so it goes to a replica with no
# wait. The LSN that read observes becomes the session's baseline, so every later
# read is monotonic: it returns the same data or newer, never older.
case_no_write_read() {
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(backend_endpoint)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if reader_endpoint_matches "$endpoint" && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "no-write read: reader route without wait"
    else
        diag "endpoint=$endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "no-write read: reader route without wait"
    fi
}

# Read, then read: the first positioned reader RFQ becomes the session's observed
# target, so the second automatic reader read is protected by a wait or by a
# selected reader that already reached that target.
case_read_then_read_monotonic() {
    set_select_rule_auto
    set_lsn_mode lsn
    snapshot_consistency_counters read_read_before
    read_read_out=$(proxy_command_sequence \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        2>&1)
    snapshot_consistency_counters read_read_after
    first_endpoint=$(first_endpoint_from_output "$read_read_out")
    second_endpoint=$(last_endpoint_from_output "$read_read_out")
    protect_delta=$(snapshot_protect_delta read_read_before read_read_after)
    wrap_or_bypass_delta=$(snapshot_wrap_or_bypass_delta read_read_before read_read_after)
    query_lsn_delta=$(snapshot_counter_delta read_read_before read_read_after query_lsn)
    if reader_endpoint_matches "$first_endpoint" &&
        reader_endpoint_matches "$second_endpoint" &&
        [ "$wrap_or_bypass_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ] &&
        [ "$query_lsn_delta" -ge 1 ]; then
        ok 0 "read-after-read: observed LSN protects the next reader read"
    else
        diag "read-after-read output: $read_read_out"
        diag "first_endpoint=$first_endpoint second_endpoint=$second_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT"
        diag "wrap_or_bypass_delta=$wrap_or_bypass_delta protect_delta=$protect_delta query_lsn_delta=$query_lsn_delta"
        ok 1 "read-after-read: observed LSN protects the next reader read"
    fi
}

# Read, then write, then read: a client can start read-only, write later (which
# raises the target to the write's LSN), and the next read is protected against
# that write.
case_read_then_write_then_read() {
    set_select_rule_auto
    set_lsn_mode lsn
    snapshot_consistency_counters transition_before
    transition_marker="read_then_write_$$"
    transition_out=$(proxy_command_sequence \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        "INSERT INTO $TEST_TABLE VALUES (9710, '$transition_marker') ON CONFLICT (id) DO UPDATE SET data='$transition_marker';" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9710;" \
        2>&1)
    snapshot_consistency_counters transition_after
    first_endpoint=$(first_endpoint_from_output "$transition_out")
    second_result=$(last_endpoint_payload_from_output "$transition_out")
    protect_delta=$(snapshot_protect_delta transition_before transition_after)
    wrap_or_bypass_delta=$(snapshot_wrap_or_bypass_delta transition_before transition_after)
    query_lsn_delta=$(snapshot_counter_delta transition_before transition_after query_lsn)
    if reader_endpoint_matches "$first_endpoint" &&
        reader_payload_matches "$second_result" "$transition_marker" &&
        [ "$wrap_or_bypass_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ] &&
        [ "$query_lsn_delta" -ge 1 ]; then
        ok 0 "session transition: read-only session can write and protect its next reader read"
    else
        diag "transition output: $transition_out"
        diag "first_endpoint=$first_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT second_result=$second_result expected=$REPLICA_SERVER_ENDPOINT|$transition_marker"
        diag "wrap_or_bypass_delta=$wrap_or_bypass_delta protect_delta=$protect_delta query_lsn_delta=$query_lsn_delta"
        ok 1 "session transition: read-only session can write and protect its next reader read"
    fi
}

# Background writes: writes from other sessions only push the replica's LSN
# forward. A read-only session picks up that newer data on its later reads but
# never goes backward: its target only climbs, so reads stay monotonic, served by
# a wait or by a replica already caught up.
case_background_writes_read_only_session() {
    set_select_rule_auto
    set_lsn_mode lsn
    snapshot_consistency_counters background_before
    bg_marker="background_read_only_$$"
    (
        i=0
        while [ "$i" -lt 3 ]; do
            i=$((i + 1))
            row_id=$((9720 + i))
            proxy_sql "INSERT INTO $TEST_TABLE VALUES ($row_id, '${bg_marker}_$i') ON CONFLICT (id) DO UPDATE SET data='${bg_marker}_$i';" >/dev/null 2>&1 || exit 1
            sleep 0.1
        done
    ) &
    bg_pid=$!
    readonly_out=$(proxy_command_sequence \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        "SELECT pg_sleep(0.3);" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        2>&1)
    bg_rc=0
    wait "$bg_pid" || bg_rc=$?
    snapshot_consistency_counters background_after
    first_endpoint=$(first_endpoint_from_output "$readonly_out")
    last_endpoint=$(last_endpoint_from_output "$readonly_out")
    prepared_delta=$(snapshot_counter_delta background_before background_after prepared)
    wait_delta=$(snapshot_counter_delta background_before background_after wait)
    bypass_delta=$(snapshot_counter_delta background_before background_after bypass)
    protect_delta=$(snapshot_protect_delta background_before background_after)
    wrap_or_bypass_delta=$(snapshot_wrap_or_bypass_delta background_before background_after)
    if [ "$bg_rc" -eq 0 ] &&
        reader_endpoint_matches "$first_endpoint" &&
        reader_endpoint_matches "$last_endpoint" &&
        [ "$wrap_or_bypass_delta" -ge 1 ] &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "read-only session: background writes coexist with protected reader reads"
    else
        diag "background-read-only output: $readonly_out"
        diag "bg_rc=$bg_rc first_endpoint=$first_endpoint last_endpoint=$last_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT"
        diag "prepared_delta=$prepared_delta wait_delta=$wait_delta bypass_delta=$bypass_delta wrap_or_bypass_delta=$wrap_or_bypass_delta protect_delta=$protect_delta"
        ok 1 "read-only session: background writes coexist with protected reader reads"
    fi
}

# mode=off: PolarDB performs no reroute and no wait override.
case_mode_off() {
    set_lsn_mode off
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(backend_endpoint)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if [ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "mode=off: no PolarDB reroute or wait override"
    else
        diag "endpoint=$endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "mode=off: no PolarDB reroute or wait override"
    fi
}

# mode=primary: an automatically eligible read is forced to the primary with
# no wait.
case_mode_primary() {
    set_lsn_mode primary
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(backend_endpoint)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if [ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "automatic route: replica_eligible=1 plus mode=primary forces writer without wait"
    else
        diag "endpoint=$endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "automatic route: replica_eligible=1 plus mode=primary forces writer without wait"
    fi
}

# Manual query-rule route: a destination_hostgroup=replica rule bypasses the
# PolarDB planner (mode=primary) with no wait.
case_manual_rule_route() {
    set_select_rule_manual_reader
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(backend_endpoint)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if reader_endpoint_matches "$endpoint" && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "manual query-rule route: destination_hostgroup reader bypasses mode=primary without wait"
    else
        diag "endpoint=$endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "manual query-rule route: destination_hostgroup reader bypasses mode=primary without wait"
    fi
}

# Manual SQL hostgroup hint: an inline /* hostgroup=replica */ comment bypasses
# mode=primary with no wait.
case_manual_sql_hint() {
    clear_select_rules
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    endpoint=$(proxy_sql "/* hostgroup=$READER_HG */ SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>/dev/null | tr -d '[:space:]')
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if reader_endpoint_matches "$endpoint" && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "manual SQL hostgroup hint: reader bypasses mode=primary without wait"
    else
        diag "endpoint=$endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "manual SQL hostgroup hint: reader bypasses mode=primary without wait"
    fi
}

# CF-3: the extended protocol follows a manual replica route and is not wrapped
# with an LSN wait.
case_cf3_extended_manual_reader() {
    set_select_rule_manual_reader
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    if ensure_extended_helper; then
        extended_out=$(run_extended_query "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
        extended_rc=$?
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        extended_endpoint=$(first_endpoint_from_output "$extended_out")
        if [ "$extended_rc" -eq 0 ] &&
            reader_endpoint_matches "$extended_endpoint" &&
            [ "$wait_after" -eq "$wait_before" ]; then
            ok 0 "CF-3: extended protocol follows manual reader route without wait wrapper"
        else
            diag "extended output: $extended_out"
            diag "extended_rc=$extended_rc endpoint=$extended_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
            ok 1 "CF-3: extended protocol follows manual reader route without wait wrapper"
        fi
    else
        sed 's/^/#   /' "$EXTENDED_HELPER_BUILD_LOG" 2>/dev/null || true
        skip_ok "CF-3: extended protocol follows manual reader route without wait wrapper" "cannot build proxysql_extended_protocol_test"
    fi
}

# CF-3: an automatic extended-protocol read with no prior write can use the
# replica and is not wait-wrapped.
case_cf3_extended_auto_no_write() {
    set_select_rule_auto
    set_lsn_mode lsn
    prepared_before=$(counter PolarDB_Wait_Wrap_Prepared)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    if ensure_extended_helper; then
        extended_out=$(run_extended_query "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
        extended_rc=$?
        prepared_after=$(counter PolarDB_Wait_Wrap_Prepared)
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        extended_endpoint=$(first_endpoint_from_output "$extended_out")
        if [ "$extended_rc" -eq 0 ] &&
            reader_endpoint_matches "$extended_endpoint" &&
            [ $((prepared_after - prepared_before)) -eq 0 ] &&
            [ $((wait_after - wait_before)) -eq 0 ]; then
            ok 0 "CF-3: automatic extended protocol without prior write can use reader without wait wrapper"
        else
            diag "extended-no-write output: $extended_out"
            diag "extended_rc=$extended_rc endpoint=$extended_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT prepared_delta=$((prepared_after - prepared_before)) wait_delta=$((wait_after - wait_before))"
            ok 1 "CF-3: automatic extended protocol without prior write can use reader without wait wrapper"
        fi
    else
        sed 's/^/#   /' "$EXTENDED_HELPER_BUILD_LOG" 2>/dev/null || true
        skip_ok "CF-3: automatic extended protocol without prior write can use reader without wait wrapper" "cannot build proxysql_extended_protocol_test"
    fi
}

# CF-3: an automatic extended-protocol read after a write is forced to the
# primary with no wait wrapper (extended protocol stays outside v1 wrapping).
case_cf3_extended_auto_after_write() {
    prepared_before=$(counter PolarDB_Wait_Wrap_Prepared)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    extended_marker="extended_after_write_$$"
    extended_setup_sql="INSERT INTO $TEST_TABLE VALUES (701, '$extended_marker') ON CONFLICT (id) DO UPDATE SET data='$extended_marker';"
    if ensure_extended_helper; then
        extended_out=$(POLARDB_EXTENDED_SETUP_SQL="$extended_setup_sql" \
            run_extended_query "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || (SELECT COUNT(*)::text FROM $TEST_TABLE WHERE id=701);" 2>&1)
        extended_rc=$?
        prepared_after=$(counter PolarDB_Wait_Wrap_Prepared)
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        extended_result=$(first_endpoint_payload_from_output "$extended_out")
        extended_endpoint=${extended_result%%|*}
        extended_count=${extended_result##*|}
        if [ "$extended_rc" -eq 0 ] &&
            [ "$extended_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
            [ "$extended_count" = "1" ] &&
            [ $((prepared_after - prepared_before)) -eq 0 ] &&
            [ $((wait_after - wait_before)) -eq 0 ]; then
            ok 0 "CF-3: automatic extended protocol after write forces writer without wait wrapper"
        else
            diag "extended-after-write output: $extended_out"
            diag "extended_rc=$extended_rc endpoint=$extended_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT count=$extended_count prepared_delta=$((prepared_after - prepared_before)) wait_delta=$((wait_after - wait_before))"
            ok 1 "CF-3: automatic extended protocol after write forces writer without wait wrapper"
        fi
    else
        sed 's/^/#   /' "$EXTENDED_HELPER_BUILD_LOG" 2>/dev/null || true
        skip_ok "CF-3: automatic extended protocol after write forces writer without wait wrapper" "cannot build proxysql_extended_protocol_test"
    fi
}

# Explicit transaction: a SELECT inside BEGIN/COMMIT stays on the primary with
# no wait.
case_explicit_txn_writer() {
    set_select_rule_auto
    set_lsn_mode lsn
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    txn_out=$(
        proxy_script 2>&1 <<SQL
BEGIN;
SELECT host(inet_server_addr()) || ':' || inet_server_port();
COMMIT;
SQL
    )
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    txn_endpoint=$(first_endpoint_from_output "$txn_out")
    if [ "$txn_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "explicit transaction: SELECT stays on writer without wait"
    else
        diag "txn output: $txn_out"
        diag "txn_endpoint=$txn_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "explicit transaction: SELECT stays on writer without wait"
    fi
}

# Transaction split: before the first write in a transaction there are no XIDs to
# export, so this is not a split read. The primary transaction backend stays open.
# A suitable reader may handle the SELECT through the normal wait path, but a
# cold or unavailable reader pool can safely leave it on the primary.
case_txn_split_prewrite_uses_reader_wait() {
    set_select_rule_auto
    set_lsn_mode lsn
    admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=1, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

    row_id=9801
    marker="txn_prewrite_reader_wait"
    prepared_before=$(counter PolarDB_Wait_Wrap_Prepared)
    split_plan_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES ($row_id, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
BEGIN;
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=$row_id;
COMMIT;
SQL
    )
    prepared_after=$(counter PolarDB_Wait_Wrap_Prepared)
    split_plan_result=$(last_endpoint_payload_from_output "$split_plan_out")

    admin_sql "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0, proxy_protocol='default' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

    if { reader_payload_matches "$split_plan_result" "$marker" &&
            [ "$prepared_after" -gt "$prepared_before" ]; } ||
        [ "$split_plan_result" = "$PRIMARY_SERVER_ENDPOINT|$marker" ]; then
        ok 0 "transaction split: pre-write read is safe before first write"
    else
        diag "split pre-write output: $split_plan_out"
        diag "result=$split_plan_result expected='$PRIMARY_SERVER_ENDPOINT|$marker or one of: $(reader_expectation)|$marker' prepared_delta=$((prepared_after - prepared_before))"
        ok 1 "transaction split: pre-write read is safe before first write"
    fi
}

# Proxy-protocol RFQ scope: exercise v15/legacy/off/HG-override proxy protocol
# variants to confirm RFQ-LSN driven protected replica reads per protocol.
case_protocol_rfq_scope() {
    set_select_rule_auto
    set_lsn_mode lsn
    run_protocol_rfq_lsn_scope v15 13 14 1301
    run_protocol_rfq_lsn_scope legacy 15 16 1501
    run_protocol_off_scope 17 18 1701
    run_protocol_hg_override_scope off legacy 19 20 1901
    run_protocol_hg_override_scope v15 default 21 22 2101
}

# Startup identity safety: when RFQ startup identity cannot be formed,
# the backend connection is rejected instead of opening a replica connection that
# cannot report RFQ LSN. The debug fault makes that path deterministic.
case_startup_identity_safety() {
    if debug_startup_identity_faults_available; then
        set_global_var_runtime "pgsql-polardb_proxy_protocol" "v15"
        set_hg_pair_policy 35 36 default lsn -1 5000
        set_default_hostgroup 35
        set_debug_startup_identity_fault none
        startup_identity_out=$(proxy_sql "SELECT 1;" 2>&1)
        startup_identity_rc=$?
        startup_identity_result=$(printf '%s\n' "$startup_identity_out" | grep -E '^1$' | tail -1 | tr -d '[:space:]')
        if [ "$startup_identity_rc" -eq 0 ] &&
            [ "$startup_identity_result" = "1" ] &&
            wait_for_trace "debug forced missing startup identity" 5 &&
            wait_for_trace "no startup identity is available" 5; then
            ok 0 "startup identity safety: missing identity rejects backend connection without aborting proxy"
        else
            diag "startup identity output: $startup_identity_out"
            diag "startup_identity_rc=$startup_identity_rc startup_identity_result=$startup_identity_result"
            ok 1 "startup identity safety: missing identity rejects backend connection without aborting proxy"
        fi
    else
        skip_ok "startup identity safety: missing identity rejects backend connection without aborting proxy" "requires POLARDB_DEBUG startup identity fault support"
    fi
}

# Startup identity fallback: the listener/proxy address can supply the RFQ
# identity when the primary source is unavailable (debug fault).
case_startup_identity_listener() {
    if debug_startup_identity_faults_available; then
        set_global_var_runtime "pgsql-polardb_proxy_protocol" "v15"
        set_hg_pair_policy 37 38 default lsn -1 5000
        set_default_hostgroup 37
        set_debug_startup_identity_fault listener_proxy
        listener_trace_before=$(trace_count "identity_source=2")
        listener_identity_out=$(proxy_sql "SELECT 1;" 2>&1)
        listener_identity_rc=$?
        listener_identity_result=$(printf '%s\n' "$listener_identity_out" | grep -E '^1$' | tail -1 | tr -d '[:space:]')
        if [ "$listener_identity_rc" -eq 0 ] &&
            [ "$listener_identity_result" = "1" ] &&
            wait_for_trace_increment "identity_source=2" "$listener_trace_before" 5; then
            ok 0 "startup identity fallback: listener/proxy address can supply RFQ identity"
        else
            diag "listener identity output: $listener_identity_out"
            diag "listener_identity_rc=$listener_identity_rc listener_identity_result=$listener_identity_result"
            ok 1 "startup identity fallback: listener/proxy address can supply RFQ identity"
        fi
    else
        skip_ok "startup identity fallback: listener/proxy address can supply RFQ identity" "requires POLARDB_DEBUG startup identity fault support"
    fi
}

# Startup identity fallback: an explicitly configured address can supply the
# RFQ identity (debug fault).
case_startup_identity_configured() {
    if debug_startup_identity_faults_available; then
        set_global_var_runtime "pgsql-polardb_proxy_identity_host" "127.0.0.2"
        set_global_var_runtime "pgsql-polardb_proxy_identity_port" "15432"
        set_hg_pair_policy 39 40 default lsn -1 5000
        set_default_hostgroup 39
        set_debug_startup_identity_fault configured_fallback
        configured_trace_before=$(trace_count "identity_source=3 identity=127.0.0.2:15432")
        configured_identity_out=$(proxy_sql "SELECT 1;" 2>&1)
        configured_identity_rc=$?
        configured_identity_result=$(printf '%s\n' "$configured_identity_out" | grep -E '^1$' | tail -1 | tr -d '[:space:]')
        if [ "$configured_identity_rc" -eq 0 ] &&
            [ "$configured_identity_result" = "1" ] &&
            wait_for_trace_increment "identity_source=3 identity=127.0.0.2:15432" "$configured_trace_before" 5; then
            ok 0 "startup identity fallback: configured address can supply RFQ identity"
        else
            diag "configured identity output: $configured_identity_out"
            diag "configured_identity_rc=$configured_identity_rc configured_identity_result=$configured_identity_result"
            ok 1 "startup identity fallback: configured address can supply RFQ identity"
        fi
        set_global_var_runtime "pgsql-polardb_proxy_identity_host" ""
        set_global_var_runtime "pgsql-polardb_proxy_identity_port" "0"
    else
        skip_ok "startup identity fallback: configured address can supply RFQ identity" "requires POLARDB_DEBUG startup identity fault support"
    fi
}

# RFQ availability policy: with proxy_protocol off so no RFQ LSN exists, strict
# policy forces the primary while best_effort degrades to the replica and warns once.
case_rfq_strict_and_best_effort() {
    set_global_var_runtime "pgsql-polardb_proxy_protocol" "v15"
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_select_rule_auto
    set_lsn_mode lsn
    set_pair_proxy_protocol 10 off
    set_session_lsn_baseline primary
    set_route_rfq_policy strict
    fallback_before=$(counter PolarDB_Consistency_Writer_Fallback)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    rfq_strict_out=$(proxy_sql "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
    fallback_after=$(counter PolarDB_Consistency_Writer_Fallback)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    rfq_strict_endpoint=$(last_endpoint_from_output "$rfq_strict_out")
    if [ "$rfq_strict_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ $((fallback_after - fallback_before)) -eq 1 ] &&
        [ $((wait_after - wait_before)) -eq 0 ]; then
        ok 0 "RFQ unavailable strict: reader acquisition redirects consistency-target read to writer"
    else
        diag "rfq-strict output: $rfq_strict_out"
        diag "endpoint=$rfq_strict_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT fallback_delta=$((fallback_after - fallback_before)) wait_delta=$((wait_after - wait_before))"
        ok 1 "RFQ unavailable strict: reader acquisition redirects consistency-target read to writer"
    fi

    set_route_rfq_policy best_effort
    degrade_before=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    rfq_best_out=$(proxy_sql "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
    degrade_after=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    rfq_best_endpoint=$(last_endpoint_from_output "$rfq_best_out")
    rfq_best_warning_count=$(printf '%s\n' "$rfq_best_out" | grep -c "PolarDB best_effort RFQ route has no enforceable LSN wait target" || true)
    if reader_endpoint_matches "$rfq_best_endpoint" &&
        [ $((degrade_after - degrade_before)) -eq 1 ] &&
        [ $((wait_after - wait_before)) -eq 0 ] &&
        [ "$rfq_best_warning_count" -eq 1 ]; then
        ok 0 "RFQ unavailable best_effort: degraded reader route is counted and warned once"
    else
        diag "rfq-best-effort output: $rfq_best_out"
        diag "endpoint=$rfq_best_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT degrade_delta=$((degrade_after - degrade_before)) wait_delta=$((wait_after - wait_before)) warning_count=$rfq_best_warning_count"
        ok 1 "RFQ unavailable best_effort: degraded reader route is counted and warned once"
    fi
    set_pair_proxy_protocol 10 default
    set_session_lsn_baseline observed
    set_route_rfq_policy strict
}

# Primary baseline unknown: with no observable primary LSN, strict policy forces
# the primary and increments the primary-unknown counter.
case_primary_baseline_unknown() {
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "0"
    set_hg_pair_policy 27 28 off lsn -1 5000
    set_default_hostgroup 27
    set_session_lsn_baseline primary
    set_route_rfq_policy strict
    primary_unknown_before=$(counter PolarDB_Primary_LSN_Unknown)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    primary_unknown_out=$(proxy_sql "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
    primary_unknown_after=$(counter PolarDB_Primary_LSN_Unknown)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    primary_unknown_endpoint=$(last_endpoint_from_output "$primary_unknown_out")
    if [ "$primary_unknown_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ $((primary_unknown_after - primary_unknown_before)) -eq 1 ] &&
        [ $((wait_after - wait_before)) -eq 0 ]; then
        ok 0 "primary baseline unknown: strict policy forces writer and increments counter"
    else
        diag "primary-unknown output: $primary_unknown_out"
        diag "endpoint=$primary_unknown_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT primary_unknown_delta=$((primary_unknown_after - primary_unknown_before)) wait_delta=$((wait_after - wait_before))"
        ok 1 "primary baseline unknown: strict policy forces writer and increments counter"
    fi
}

# Plan-stage best_effort RFQ degradation: a write whose LSN is unknown degrades
# to the replica, warns exactly once, and records the reason.
case_plan_stage_best_effort_degrade() {
    set_session_lsn_baseline observed
    set_route_rfq_policy best_effort
    write_missing_before=$(counter PolarDB_Write_Missing_LSN)
    degrade_before=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    plan_degrade_marker="plan_degrade_$$"
    plan_degrade_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (2701, '$plan_degrade_marker') ON CONFLICT (id) DO UPDATE SET data='$plan_degrade_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    write_missing_after=$(counter PolarDB_Write_Missing_LSN)
    degrade_after=$(counter PolarDB_RFQ_Best_Effort_Degraded_Routes)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    plan_degrade_endpoint=$(last_endpoint_from_output "$plan_degrade_out")
    plan_degrade_warning_count=$(printf '%s\n' "$plan_degrade_out" | grep -c "PolarDB best_effort RFQ route has no enforceable LSN wait target" || true)
    plan_degrade_detail_count=$(printf '%s\n' "$plan_degrade_out" | grep -c "reason=write_lsn_unknown" || true)
    if reader_endpoint_matches "$plan_degrade_endpoint" &&
        [ $((write_missing_after - write_missing_before)) -ge 1 ] &&
        [ $((degrade_after - degrade_before)) -eq 1 ] &&
        [ $((wait_after - wait_before)) -eq 0 ] &&
        [ "$plan_degrade_warning_count" -eq 1 ] &&
        [ "$plan_degrade_detail_count" -eq 1 ]; then
        ok 0 "plan-stage best_effort RFQ degradation warns once and routes to reader"
    else
        diag "plan-degrade output: $plan_degrade_out"
        diag "endpoint=$plan_degrade_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT write_missing_delta=$((write_missing_after - write_missing_before)) degrade_delta=$((degrade_after - degrade_before)) wait_delta=$((wait_after - wait_before)) warning_count=$plan_degrade_warning_count detail_count=$plan_degrade_detail_count"
        ok 1 "plan-stage best_effort RFQ degradation warns once and routes to reader"
    fi
}

# Missing primary RFQ LSN: a write with no RFQ LSN sets a sticky flag so a later
# automatic read stays on the primary.
case_missing_writer_rfq_state() {
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "1"
    set_session_lsn_baseline observed
    set_route_rfq_policy strict

    set_hg_pair_policy 23 24 off lsn -1 5000
    set_default_hostgroup 23
    write_missing_before=$(counter PolarDB_Write_Missing_LSN)
    missing_write_marker="missing_write_$$"
    missing_write_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (2301, '$missing_write_marker') ON CONFLICT (id) DO UPDATE SET data='$missing_write_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    write_missing_after=$(counter PolarDB_Write_Missing_LSN)
    missing_write_endpoint=$(last_endpoint_from_output "$missing_write_out")
    if [ "$missing_write_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ $((write_missing_after - write_missing_before)) -ge 1 ]; then
        ok 0 "missing writer RFQ LSN: write sticky flag keeps later automatic read on writer"
    else
        diag "missing-write output: $missing_write_out"
        diag "endpoint=$missing_write_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT write_missing_delta=$((write_missing_after - write_missing_before))"
        ok 1 "missing writer RFQ LSN: write sticky flag keeps later automatic read on writer"
    fi
}

# Missing replica RFQ LSN: an observed replica read with no RFQ LSN sets a sticky flag on the
# session so a later automatic read moves to the primary.
case_missing_reader_rfq_state() {
    set_hg_pair_policy 25 26 off lsn -1 5000
    set_default_hostgroup 25
    read_missing_before=$(counter PolarDB_Read_Missing_LSN)
    missing_read_out=$(
        proxy_script 2>&1 <<SQL
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    read_missing_after=$(counter PolarDB_Read_Missing_LSN)
    missing_read_first=$(first_endpoint_from_output "$missing_read_out")
    missing_read_second=$(last_endpoint_from_output "$missing_read_out")
    if reader_endpoint_matches "$missing_read_first" &&
        [ "$missing_read_second" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ $((read_missing_after - read_missing_before)) -ge 1 ]; then
        ok 0 "missing reader RFQ LSN: observed sticky flag keeps later automatic read on writer"
    else
        diag "missing-read output: $missing_read_out"
        diag "first=$missing_read_first expected_reader=$REPLICA_SERVER_ENDPOINT second=$missing_read_second expected_writer=$PRIMARY_SERVER_ENDPOINT read_missing_delta=$((read_missing_after - read_missing_before))"
        ok 1 "missing reader RFQ LSN: observed sticky flag keeps later automatic read on writer"
    fi
}

# route=primary hint: an eligible read carrying the hint stays on the primary
# with no wait.
case_route_primary_hint() {
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_select_rule_auto
    set_lsn_mode lsn
    set_route_rfq_policy strict
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    hint_endpoint=$(proxy_sql "/* route=primary */ SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>/dev/null | tr -d '[:space:]')
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if [ "$hint_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "route=primary hint: eligible read stays on writer without wait"
    else
        diag "hint_endpoint=$hint_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "route=primary hint: eligible read stays on writer without wait"
    fi
}

# Multi-statement simple query: an automatic read in a multi-statement batch
# stays on the primary with no wait.
case_multi_statement() {
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    multi_out=$(proxy_sql "SELECT 1; SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    multi_endpoint=$(first_endpoint_from_output "$multi_out")
    if [ "$multi_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "multi-statement simple query: automatic read stays on writer without wait"
    else
        diag "multi output: $multi_out"
        diag "endpoint=$multi_endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "multi-statement simple query: automatic read stays on writer without wait"
    fi
}

# Session consistency override: SET ... TO 'off'/'primary' overrides the global
# lsn mode until RESET restores it.
case_session_consistency_override() {
    snapshot_consistency_counters override_before
    override_out=$(proxy_command_sequence \
        "SET proxysql.polardb_consistency_mode TO 'off';" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        "SET proxysql.polardb_consistency_mode TO 'primary';" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        "RESET proxysql.polardb_consistency_mode;" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        2>&1)
    snapshot_consistency_counters override_after
    wait_delta=$(snapshot_counter_delta override_before override_after wait)
    bypass_delta=$(snapshot_counter_delta override_before override_after bypass)
    protect_delta=$(snapshot_protect_delta override_before override_after)
    override_endpoints=$(endpoint_list_from_output "$override_out")
    set -- $override_endpoints
    override_first="${1:-}"
    override_second="${2:-}"
    override_third="${3:-}"
    if [ "$override_first" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ "$override_second" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        reader_endpoint_matches "$override_third" &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "session consistency override: off and primary override global lsn until reset"
    else
        diag "override output: $override_out"
        diag "endpoints='$override_endpoints' expected='$PRIMARY_SERVER_ENDPOINT $PRIMARY_SERVER_ENDPOINT <one of: $(reader_expectation)>' wait_delta=$wait_delta bypass_delta=$bypass_delta"
        ok 1 "session consistency override: off and primary override global lsn until reset"
    fi
}

# Session mode can be enabled and disabled on one existing client connection.
# The middle LSN-mode read is protected; the final off-mode read leaves routing
# to the default hostgroup and must not reuse stale wait state from the protected
# read.
case_session_enable_then_disable() {
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_select_rule_auto
    set_lsn_mode off

    snapshot_consistency_counters mode_toggle_before
    mode_toggle_marker="mode_toggle_$$"
    mode_toggle_out=$(proxy_command_sequence \
        "SET proxysql.polardb_consistency_mode TO 'lsn';" \
        "INSERT INTO $TEST_TABLE VALUES (9711, '$mode_toggle_marker') ON CONFLICT (id) DO UPDATE SET data='$mode_toggle_marker';" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9711;" \
        "SET proxysql.polardb_consistency_mode TO 'off';" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port();" \
        2>&1)
    snapshot_consistency_counters mode_toggle_after
    set_lsn_mode lsn

    protected_result=$(last_endpoint_payload_from_output "$mode_toggle_out")
    final_endpoint=$(last_endpoint_from_output "$mode_toggle_out")
    protect_delta=$(snapshot_protect_delta mode_toggle_before mode_toggle_after)
    wrap_or_bypass_delta=$(snapshot_wrap_or_bypass_delta mode_toggle_before mode_toggle_after)
    query_lsn_delta=$(snapshot_counter_delta mode_toggle_before mode_toggle_after query_lsn)
    if reader_payload_matches "$protected_result" "$mode_toggle_marker" &&
        [ "$final_endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] &&
        [ "$protect_delta" -eq 1 ] &&
        [ "$wrap_or_bypass_delta" -ge 1 ] &&
        [ "$query_lsn_delta" -ge 1 ]; then
        ok 0 "session mode transition: lsn protects read and off disables later wait"
    else
        diag "mode-toggle output: $mode_toggle_out"
        diag "protected_result='$protected_result' expected_reader_payload='$(reader_expectation)|$mode_toggle_marker' final_endpoint='$final_endpoint' expected_final='$PRIMARY_SERVER_ENDPOINT'"
        diag "protect_delta=$protect_delta wrap_or_bypass_delta=$wrap_or_bypass_delta query_lsn_delta=$query_lsn_delta"
        ok 1 "session mode transition: lsn protects read and off disables later wait"
    fi
}

# Query cache must not satisfy a consistency-protected read after this session
# has written. The first session deliberately runs with consistency disabled to
# put the old value into cache; the second session writes a new value and must
# read the new value from the backend, not the old cached tuple.
case_query_cache_bypassed_after_session_write() {
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_lsn_mode lsn
    set_session_lsn_baseline observed

    local row_id=9731
    local old_marker="cache_old_$$"
    local new_marker="cache_new_$$"
    local select_sql="SELECT data FROM $TEST_TABLE WHERE id=$row_id"
    direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
        "INSERT INTO $TEST_TABLE VALUES ($row_id, '$old_marker') ON CONFLICT (id) DO UPDATE SET data='$old_marker';" >/dev/null
    wait_for_replica_replay_catchup 20 >/dev/null 2>&1 || true

    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, replica_eligible, cache_ttl, apply, comment) VALUES ($POLARDB_SELECT_RULE_ID, 1, '^SELECT data FROM', 1, 60000, 0, 'cache_consistency_bypass');" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='256' WHERE variable_name='pgsql-query_cache_size_MB';" >/dev/null
    admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
    admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

    cache_set_before=$(counter Query_Cache_count_SET)
    cache_hit_before=$(counter Query_Cache_count_GET_OK)
    seed_out=$(proxy_command_sequence \
        "SET proxysql.polardb_consistency_mode TO 'off';" \
        "$select_sql" \
        2>&1)
    cache_set_after_seed=$(counter Query_Cache_count_SET)
    cache_hit_after_seed=$(counter Query_Cache_count_GET_OK)
    rw_out=$(proxy_command_sequence \
        "UPDATE $TEST_TABLE SET data='$new_marker' WHERE id=$row_id;" \
        "$select_sql" \
        2>&1)
    cache_hit_after_read=$(counter Query_Cache_count_GET_OK)
    set_select_rule_auto

    seed_result=$(printf '%s\n' "$seed_out" | grep -E "^($old_marker|$new_marker)$" | tail -1 | tr -d '[:space:]')
    read_result=$(printf '%s\n' "$rw_out" | grep -E "^($old_marker|$new_marker)$" | tail -1 | tr -d '[:space:]')
    cache_set_delta=$((cache_set_after_seed - cache_set_before))
    seed_hit_delta=$((cache_hit_after_seed - cache_hit_before))
    protected_hit_delta=$((cache_hit_after_read - cache_hit_after_seed))
    if [ "$seed_result" = "$old_marker" ] &&
        [ "$read_result" = "$new_marker" ] &&
        [ "$cache_set_delta" -ge 1 ] &&
        [ "$seed_hit_delta" -eq 0 ] &&
        [ "$protected_hit_delta" -eq 0 ]; then
        ok 0 "query cache: protected read after same-session write bypasses stale cached value"
    else
        diag "cache seed output: $seed_out"
        diag "cache rw output: $rw_out"
        diag "seed_result=$seed_result expected=$old_marker read_result=$read_result expected=$new_marker"
        diag "cache_set_delta=$cache_set_delta seed_hit_delta=$seed_hit_delta protected_hit_delta=$protected_hit_delta"
        ok 1 "query cache: protected read after same-session write bypasses stale cached value"
    fi
}

# RESET ALL must preserve the session LSN evidence so the next protected read
# is still served safely from the replica.
case_reset_all_preserves_lsn() {
    snapshot_consistency_counters reset_before
    reset_marker="reset_preserves_lsn_$$"
    reset_out=$(proxy_command_sequence \
        "INSERT INTO $TEST_TABLE VALUES (9701, '$reset_marker') ON CONFLICT (id) DO UPDATE SET data='$reset_marker';" \
        "RESET ALL;" \
        "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9701;" \
        2>&1)
    snapshot_consistency_counters reset_after
    wait_delta=$(snapshot_counter_delta reset_before reset_after wait)
    bypass_delta=$(snapshot_counter_delta reset_before reset_after bypass)
    protect_delta=$(snapshot_protect_delta reset_before reset_after)
    reset_result=$(last_endpoint_payload_from_output "$reset_out")
    if reader_payload_matches "$reset_result" "$reset_marker" &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "RESET ALL preserves session LSN evidence for the next protected read"
    else
        diag "reset output: $reset_out"
        diag "reset_result='$reset_result' expected_reader_payload='$(reader_expectation)|$reset_marker' wait_delta=$wait_delta bypass_delta=$bypass_delta"
        ok 1 "RESET ALL preserves session LSN evidence for the next protected read"
    fi
}

# Replica acquisition faults (debug): reader_busy falls through to a normal retry
# while reader_lsn_unknown forces a primary fallback.
case_reader_acquire_faults() {
    if debug_reader_acquire_faults_available; then
        set_hg_pair_policy 33 34 v15 lsn -1 5000
        set_default_hostgroup 33
        wait_before=$(counter PolarDB_Wait_LSN_Sent)
        reader_busy_marker="reader_busy_retry_$$"
        set_debug_reader_acquire_fault reader_busy
        reader_busy_out=$(proxy_command_sequence \
            "INSERT INTO $TEST_TABLE VALUES (9702, '$reader_busy_marker') ON CONFLICT (id) DO UPDATE SET data='$reader_busy_marker';" \
            "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9702;" \
            2>&1)
        clear_debug_reader_acquire_fault
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        reader_busy_result=$(last_endpoint_payload_from_output "$reader_busy_out")
        if reader_payload_matches "$reader_busy_result" "$reader_busy_marker" &&
            [ $((wait_after - wait_before)) -ge 1 ] &&
            wait_for_trace "get_MyConn_polardb_reader status=reader_busy" 5; then
            ok 0 "debug reader-busy acquisition falls through to normal retry"
        else
            diag "reader-busy output: $reader_busy_out"
            diag "reader_busy_result='$reader_busy_result' expected_reader_payload='$(reader_expectation)|$reader_busy_marker' wait_delta=$((wait_after - wait_before))"
            ok 1 "debug reader-busy acquisition falls through to normal retry"
        fi

        wait_before=$(counter PolarDB_Wait_LSN_Sent)
        fallback_before=$(counter PolarDB_Consistency_Writer_Fallback)
        reader_unknown_marker="reader_lsn_unknown_$$"
        set_debug_reader_acquire_fault reader_lsn_unknown
        reader_unknown_out=$(proxy_command_sequence \
            "INSERT INTO $TEST_TABLE VALUES (9703, '$reader_unknown_marker') ON CONFLICT (id) DO UPDATE SET data='$reader_unknown_marker';" \
            "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=9703;" \
            2>&1)
        clear_debug_reader_acquire_fault
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        fallback_after=$(counter PolarDB_Consistency_Writer_Fallback)
        reader_unknown_result=$(last_endpoint_payload_from_output "$reader_unknown_out")
        expected_reader_unknown="$PRIMARY_SERVER_ENDPOINT|$reader_unknown_marker"
        if [ "$reader_unknown_result" = "$expected_reader_unknown" ] &&
            [ $((wait_after - wait_before)) -eq 0 ] &&
            [ $((fallback_after - fallback_before)) -eq 1 ] &&
            wait_for_trace "get_MyConn_polardb_reader status=reader_lsn_unknown" 5; then
            ok 0 "debug reader LSN unknown forces writer fallback"
        else
            diag "reader-lsn-unknown output: $reader_unknown_out"
            diag "reader_unknown_result='$reader_unknown_result' expected='$expected_reader_unknown' wait_delta=$((wait_after - wait_before)) fallback_delta=$((fallback_after - fallback_before))"
            ok 1 "debug reader LSN unknown forces writer fallback"
        fi
        set_default_hostgroup "$WRITER_HG"
        remove_extra_hg_pairs
        set_select_rule_auto
        set_lsn_mode lsn
        set_route_rfq_policy strict
    else
        skip_ok "debug reader-busy acquisition falls through to normal retry" "requires POLARDB_DEBUG reader acquisition fault support"
        skip_ok "debug reader LSN unknown forces writer fallback" "requires POLARDB_DEBUG reader acquisition fault support"
    fi
}

# Monitor baseline: with monitor LSN updates enabled, the monitor advances the
# LSN cache (skipped when PolarDB health functions are unavailable).
case_monitor_baseline() {
    monitor_before=$(counter PolarDB_LSN_Updates_From_Monitor)
    if [ "$MONITOR_HEALTH_SUPPORTED" -eq 1 ]; then
        direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "INSERT INTO $TEST_TABLE VALUES (901, 'monitor_enabled_$$') ON CONFLICT (id) DO UPDATE SET data='monitor_enabled_$$';" >/dev/null 2>&1 || true
        if wait_for_monitor_increment "$monitor_before" 20; then
            ok 0 "monitor baseline: monitor updates LSN cache when enabled"
        else
            diag "monitor counter did not increase from $monitor_before within 20s"
            ok 1 "monitor baseline: monitor updates LSN cache when enabled"
        fi
    else
        skip_ok "monitor baseline: monitor updates LSN cache when enabled" "PolarDB monitor health functions are unavailable"
    fi
}

# monitor_lsn_updates=false suppresses monitor LSN cache updates (skipped when
# PolarDB health functions are unavailable).
case_monitor_disabled() {
    monitor_enabled_after=$(counter PolarDB_LSN_Updates_From_Monitor)
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "0"
    if [ "$MONITOR_HEALTH_SUPPORTED" -eq 1 ]; then
        direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "INSERT INTO $TEST_TABLE VALUES (902, 'monitor_disabled_$$') ON CONFLICT (id) DO UPDATE SET data='monitor_disabled_$$';" >/dev/null 2>&1 || true
        sleep 8
        monitor_disabled_after=$(counter PolarDB_LSN_Updates_From_Monitor)
        if [ "$monitor_disabled_after" -eq "$monitor_enabled_after" ]; then
            ok 0 "monitor_lsn_updates=false suppresses monitor LSN cache updates"
        else
            diag "monitor counter before_disable=$monitor_enabled_after after_disable=$monitor_disabled_after"
            ok 1 "monitor_lsn_updates=false suppresses monitor LSN cache updates"
        fi
    else
        skip_ok "monitor_lsn_updates=false suppresses monitor LSN cache updates" "PolarDB monitor health functions are unavailable"
    fi
}

# Monitor health faults (debug): invalid roles are counted and the row is treated as unusable,
# invalid field values are counted without changing role, an unavailable row
# shuns the replica, and recovery restores the replica route.
case_monitor_health_faults() {
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "1"
    if [ "$MONITOR_HEALTH_SUPPORTED" -eq 1 ] && debug_monitor_health_faults_available; then
        invalid_role_before=$(counter PolarDB_Monitor_Health_Invalid_Role)
        invalid_values_before=$(counter PolarDB_Monitor_Health_Invalid_Values)
        set_debug_monitor_health "$REPLICA_HOST:$REPLICA_PORT" "unknown" "t" "0/1"
        if wait_for_counter_increment PolarDB_Monitor_Health_Invalid_Role "$invalid_role_before" 20; then
            invalid_role_after=$(counter PolarDB_Monitor_Health_Invalid_Role)
            invalid_values_after=$(counter PolarDB_Monitor_Health_Invalid_Values)
            if wait_for_runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT" "SHUNNED" 20; then
                invalid_role_status="SHUNNED"
            else
                invalid_role_status=$(runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT")
            fi
            invalid_role_writer_status=$(runtime_server_status "$WRITER_HG" "$REPLICA_HOST" "$REPLICA_PORT")
            if [ "$invalid_role_status" = "SHUNNED" ] &&
                [ "$invalid_role_writer_status" != "ONLINE" ] &&
                [ $((invalid_role_after - invalid_role_before)) -ge 1 ] &&
                [ "$invalid_values_after" -eq "$invalid_values_before" ]; then
                ok 0 "debug invalid monitor role is counted and marks the row unusable"
            else
                diag "invalid_role before=$invalid_role_before after=$invalid_role_after invalid_values before=$invalid_values_before after=$invalid_values_after reader_status=$invalid_role_status writer_status=$invalid_role_writer_status"
                ok 1 "debug invalid monitor role is counted and marks the row unusable"
            fi
        else
            diag "invalid-role counter did not increase from $invalid_role_before"
            ok 1 "debug invalid monitor role is counted and marks the row unusable"
        fi

        rm -f "$POLARDB_DEBUG_MONITOR_HEALTH_FILE"
        restore_reader_online
        wait_for_runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT" "ONLINE" 20 || \
            diag "reader status after invalid-role restore: $(runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT")"

        invalid_role_before=$(counter PolarDB_Monitor_Health_Invalid_Role)
        invalid_values_before=$(counter PolarDB_Monitor_Health_Invalid_Values)
        set_debug_monitor_health "$REPLICA_HOST:$REPLICA_PORT" "replica" "maybe" "FFFFFFFF/FFFFFFFFjunk"
        if wait_for_counter_increment PolarDB_Monitor_Health_Invalid_Values "$invalid_values_before" 20; then
            invalid_role_after=$(counter PolarDB_Monitor_Health_Invalid_Role)
            invalid_values_after=$(counter PolarDB_Monitor_Health_Invalid_Values)
            invalid_values_status=$(runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT")
            if [ "$invalid_values_status" != "SHUNNED" ] &&
                [ "$invalid_role_after" -eq "$invalid_role_before" ] &&
                [ $((invalid_values_after - invalid_values_before)) -ge 1 ]; then
                ok 0 "debug invalid monitor values are counted without shunning reader"
            else
                diag "invalid_values before=$invalid_values_before after=$invalid_values_after invalid_role before=$invalid_role_before after=$invalid_role_after status=$invalid_values_status"
                ok 1 "debug invalid monitor values are counted without shunning reader"
            fi
        else
            diag "invalid-values counter did not increase from $invalid_values_before"
            ok 1 "debug invalid monitor values are counted without shunning reader"
        fi

        set_debug_monitor_health "$REPLICA_HOST:$REPLICA_PORT" "replica" "f" "FFFFFFFF/FFFFFFFF"
        if wait_for_runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT" "SHUNNED" 20; then
            ok 0 "debug unavailable monitor health row shuns reader"
        else
            diag "reader status after unavailable health row: $(runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT")"
            ok 1 "debug unavailable monitor health row shuns reader"
        fi

        restore_reader_online
        if wait_for_runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT" "ONLINE" 20; then
            ok 0 "reader route restored after debug monitor shun"
        else
            diag "reader status after restore: $(runtime_server_status "$READER_HG" "$REPLICA_HOST" "$REPLICA_PORT")"
            ok 1 "reader route restored after debug monitor shun"
        fi
    else
        skip_ok "debug invalid monitor role is counted and marks the row unusable" "requires PolarDB monitor health functions and POLARDB_DEBUG monitor fault support"
        skip_ok "debug invalid monitor values are counted without shunning reader" "requires PolarDB monitor health functions and POLARDB_DEBUG monitor fault support"
        skip_ok "debug unavailable monitor health row shuns reader" "requires PolarDB monitor health functions and POLARDB_DEBUG monitor fault support"
        skip_ok "reader route restored after debug monitor shun" "requires PolarDB monitor health functions and POLARDB_DEBUG monitor fault support"
    fi
}

# CF-8: query RFQ LSN updates must increment the RFQ counter but not the monitor
# LSN counter (the two LSN stat sources stay separate).
case_cf8_rfq_vs_monitor() {
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "0"
    rfq_before=$(counter PolarDB_Server_LSN_Updates_From_RFQ)
    monitor_before=$(counter PolarDB_LSN_Updates_From_Monitor)
    cf8_marker="cf8_rfq_monitor_split_$$"
    cf8_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (903, '$cf8_marker') ON CONFLICT (id) DO UPDATE SET data='$cf8_marker';
SELECT data FROM $TEST_TABLE WHERE id=903;
SQL
    )
    rfq_after=$(counter PolarDB_Server_LSN_Updates_From_RFQ)
    monitor_after=$(counter PolarDB_LSN_Updates_From_Monitor)
    cf8_marker_count=$(printf '%s\n' "$cf8_out" | grep -c "^$cf8_marker$" || true)
    if [ "$cf8_marker_count" -eq 1 ] &&
        [ $((rfq_after - rfq_before)) -ge 1 ] &&
        [ "$monitor_after" -eq "$monitor_before" ]; then
        ok 0 "CF-8: query RFQ LSN updates do not increment monitor LSN counter"
    else
        diag "cf8 output: $cf8_out"
        diag "rfq_delta=$((rfq_after - rfq_before)) monitor_delta=$((monitor_after - monitor_before)) marker_count=$cf8_marker_count"
        ok 1 "CF-8: query RFQ LSN updates do not increment monitor LSN counter"
    fi
}

# Freshness-controlled byte lag cap: with stale cached replica LSN, enabling the byte
# lag cap uses the primary instead of a stale replica and increments the writer-fallback counter.
case_lag_cap_freshness() {
    # With monitor updates disabled and a tiny freshness window, cached replica LSN
    # data becomes stale. Enabling the byte lag cap must use the primary
    # for a read that follows a write in the same session.
    set_lsn_mode lsn
    set_hg_policy lsn 1 5000
    set_global_var_runtime "pgsql-polardb_lsn_freshness_ms" "1"
    set_global_var_runtime "pgsql-polardb_lag_ms" "0"
    sleep 2
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    fallback_before=$(counter PolarDB_Consistency_Writer_Fallback)
    lag_marker="lag_cap_$$"
    cap_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (2, '$lag_marker') ON CONFLICT (id) DO UPDATE SET data='$lag_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    fallback_after=$(counter PolarDB_Consistency_Writer_Fallback)
    endpoint=$(first_endpoint_from_output "$cap_out")
    if [ "$endpoint" = "$PRIMARY_SERVER_ENDPOINT" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "freshness-controlled byte lag cap skips stale reader and forces writer"
    else
        diag "cap output: $cap_out"
        diag "endpoint=$endpoint expected_writer=$PRIMARY_SERVER_ENDPOINT wait_delta=$((wait_after - wait_before))"
        ok 1 "freshness-controlled byte lag cap skips stale reader and forces writer"
    fi
    if [ $((fallback_after - fallback_before)) -eq 1 ]; then
        ok 0 "freshness-controlled byte lag cap increments writer-fallback counter"
    else
        diag "fallback_delta=$((fallback_after - fallback_before))"
        ok 1 "freshness-controlled byte lag cap increments writer-fallback counter"
    fi
}

# Reset caps: disabling the byte lag cap restores the replica route.
case_reset_caps() {
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "1"
    set_global_var_runtime "pgsql-polardb_lag_ms" "0"
    set_global_var_runtime "pgsql-polardb_lsn_freshness_ms" "5000"
    set_hg_policy lsn -1 5000
    sleep 2
    endpoint=$(backend_endpoint)
    if reader_endpoint_matches "$endpoint"; then
        ok 0 "reset caps: reader route restored after disabling byte lag cap"
    else
        diag "endpoint=$endpoint expected_reader=$REPLICA_SERVER_ENDPOINT"
        ok 1 "reset caps: reader route restored after disabling byte lag cap"
    fi
}

# CF-1: a replica inside the byte lag cap is allowed. If the selected replica
# already reached the consistency target, the wait wrapper may be bypassed;
# otherwise the backend wait remains the condition.
case_cf1_in_cap() {
    local cf1_lag_cap_freshness_saved

    if [ "$MONITOR_HEALTH_SUPPORTED" -ne 1 ]; then
        skip_ok "CF-1: byte lag cap allows in-cap protected reader" "PolarDB monitor health functions are unavailable"
        skip_ok "CF-1: consistency-target reader selector records preferred or wait-capable selection" "PolarDB monitor health functions are unavailable"
        return
    fi

    cf1_lag_cap_freshness_saved=$(admin_sql "SELECT variable_value FROM runtime_global_variables WHERE variable_name='pgsql-polardb_lag_cap_freshness_ms';" 2>/dev/null | tr -d '[:space:]')
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_select_rule_auto
    set_lsn_mode lsn
    set_global_var_runtime "pgsql-polardb_proxy_protocol" "v15"
    set_global_var_runtime "pgsql-polardb_monitor_lsn_updates" "1"
    set_global_var_runtime "pgsql-polardb_lag_ms" "0"
    set_global_var_runtime "pgsql-polardb_lag_bytes" "0"
    set_global_var_runtime "pgsql-polardb_lsn_freshness_ms" "5000"
    set_global_var_runtime "pgsql-polardb_lag_cap_freshness_ms" "5000"
    set_session_lsn_baseline observed
    set_route_rfq_policy strict
    set_hg_policy lsn 2147483647 5000
    cf1_monitor_before=$(counter PolarDB_LSN_Updates_From_Monitor)
    direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "INSERT INTO $TEST_TABLE VALUES (908, 'cf1_monitor_$$') ON CONFLICT (id) DO UPDATE SET data='cf1_monitor_$$';" >/dev/null 2>&1 || true
    if ! wait_for_monitor_increment "$cf1_monitor_before" 20; then
        diag "CF-1: monitor LSN counter did not increase from $cf1_monitor_before before byte-lag check"
    fi
    snapshot_consistency_counters cf1_before
    writer_fallback_before=$(counter PolarDB_Consistency_Writer_Fallback)
    lag_unknown_before=$(counter PolarDB_Lag_Cap_LSN_Unknown)
    lag_stale_before=$(counter PolarDB_Lag_Cap_LSN_Stale)
    lag_rejected_before=$(counter PolarDB_Lag_Cap_Rejected)
    lag_accepted_before=$(counter PolarDB_Lag_Cap_Accepted)
    preferred_before=$(counter PolarDB_Target_LSN_Preferred)
    fallback_wait_before=$(counter PolarDB_Target_LSN_Fallback_Wait)
    cf1_marker="cf1_in_cap_$$"
    cf1_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (904, '$cf1_marker') ON CONFLICT (id) DO UPDATE SET data='$cf1_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    snapshot_consistency_counters cf1_after
    wait_delta=$(snapshot_counter_delta cf1_before cf1_after wait)
    bypass_delta=$(snapshot_counter_delta cf1_before cf1_after bypass)
    protect_delta=$(snapshot_protect_delta cf1_before cf1_after)
    writer_fallback_after=$(counter PolarDB_Consistency_Writer_Fallback)
    lag_unknown_after=$(counter PolarDB_Lag_Cap_LSN_Unknown)
    lag_stale_after=$(counter PolarDB_Lag_Cap_LSN_Stale)
    lag_rejected_after=$(counter PolarDB_Lag_Cap_Rejected)
    lag_accepted_after=$(counter PolarDB_Lag_Cap_Accepted)
    preferred_after=$(counter PolarDB_Target_LSN_Preferred)
    fallback_wait_after=$(counter PolarDB_Target_LSN_Fallback_Wait)
    cf1_endpoint=$(first_endpoint_from_output "$cf1_out")
    if reader_endpoint_matches "$cf1_endpoint" &&
        [ "$protect_delta" -ge 1 ]; then
        ok 0 "CF-1: byte lag cap allows in-cap protected reader"
    else
        diag "cf1 output: $cf1_out"
        diag "endpoint=$cf1_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$wait_delta bypass_delta=$bypass_delta"
        diag "writer_fallback_delta=$((writer_fallback_after - writer_fallback_before)) lag_unknown_delta=$((lag_unknown_after - lag_unknown_before)) lag_stale_delta=$((lag_stale_after - lag_stale_before)) lag_rejected_delta=$((lag_rejected_after - lag_rejected_before)) lag_accepted_delta=$((lag_accepted_after - lag_accepted_before))"
        ok 1 "CF-1: byte lag cap allows in-cap protected reader"
    fi
    if [ $((preferred_after - preferred_before + fallback_wait_after - fallback_wait_before)) -ge 1 ]; then
        ok 0 "CF-1: consistency-target reader selector records preferred or wait-capable selection"
    else
        diag "preferred_delta=$((preferred_after - preferred_before)) fallback_wait_delta=$((fallback_wait_after - fallback_wait_before))"
        ok 1 "CF-1: consistency-target reader selector records preferred or wait-capable selection"
    fi
    if [ -n "$cf1_lag_cap_freshness_saved" ]; then
        set_global_var_runtime "pgsql-polardb_lag_cap_freshness_ms" "$cf1_lag_cap_freshness_saved"
    fi
}

# CF-2: the smart selector creates a backend connection for a cold replica pool
# during a consistency wait.
case_cf2_cold_reader() {
    admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id=12;" >/dev/null
    insert_reader_servers 12 100
    admin_sql "UPDATE pgsql_replication_hostgroups SET reader_hostgroup=12, consistency_mode='lsn', max_lag_bytes=-1, lsn_wait_timeout_ms=5000 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
    sleep 1
    pool_ok_before=$(pool_value 12 ConnOK)
    pool_queries_before=$(pool_value 12 Queries)
    snapshot_consistency_counters cf2_before
    cf2_marker="cf2_cold_reader_$$"
    cf2_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (905, '$cf2_marker') ON CONFLICT (id) DO UPDATE SET data='$cf2_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port();
SQL
    )
    snapshot_consistency_counters cf2_after
    wait_delta=$(snapshot_counter_delta cf2_before cf2_after wait)
    bypass_delta=$(snapshot_counter_delta cf2_before cf2_after bypass)
    protect_delta=$(snapshot_protect_delta cf2_before cf2_after)
    pool_ok_after=$(pool_value 12 ConnOK)
    pool_queries_after=$(pool_value 12 Queries)
    cf2_endpoint=$(first_endpoint_from_output "$cf2_out")
    if reader_endpoint_matches "$cf2_endpoint" &&
        [ "$protect_delta" -ge 1 ] &&
        { [ $((pool_ok_after - pool_ok_before)) -ge 1 ] || [ $((pool_queries_after - pool_queries_before)) -ge 1 ]; }; then
        ok 0 "CF-2: smart selector creates backend for cold reader pool"
    else
        diag "cf2 output: $cf2_out"
        diag "endpoint=$cf2_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT wait_delta=$wait_delta bypass_delta=$bypass_delta conn_ok_delta=$((pool_ok_after - pool_ok_before)) query_delta=$((pool_queries_after - pool_queries_before))"
        ok 1 "CF-2: smart selector creates backend for cold reader pool"
    fi
    admin_sql "UPDATE pgsql_replication_hostgroups SET reader_hostgroup=$READER_HG, consistency_mode='lsn', max_lag_bytes=-1, lsn_wait_timeout_ms=5000 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id=12;" >/dev/null
    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
    set_hg_policy lsn -1 5000
}

# RFQ profile mismatch: a connection stored under the old profile key must not
# be returned for the new profile. ReaderPool v2 finds only the requested key;
# it does not scan or remove unrelated free connections.
case_rfq_profile_mismatch() {
    set_hg_pair_policy_one_reader 29 30 off lsn -1 5000 1
    set_default_hostgroup 29
    rfq_mismatch_warm=$(proxy_sql "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>&1)
    rfq_mismatch_warm_endpoint=$(last_endpoint_from_output "$rfq_mismatch_warm")
    rfq_mismatch_free_before=$(pool_value 30 ConnFree)
    set_pair_proxy_protocol 29 v15
    rfq_skipped_before=$(counter PolarDB_RFQ_Profile_Skipped)
    rfq_evicted_before=$(counter PolarDB_RFQ_Profile_Evicted)
    snapshot_consistency_counters rfq_mismatch_before
    rfq_mismatch_marker="rfq_profile_mismatch_$$"
    rfq_mismatch_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (2901, '$rfq_mismatch_marker') ON CONFLICT (id) DO UPDATE SET data='$rfq_mismatch_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=2901;
SQL
    )
    snapshot_consistency_counters rfq_mismatch_after
    wait_delta=$(snapshot_counter_delta rfq_mismatch_before rfq_mismatch_after wait)
    bypass_delta=$(snapshot_counter_delta rfq_mismatch_before rfq_mismatch_after bypass)
    protect_delta=$(snapshot_protect_delta rfq_mismatch_before rfq_mismatch_after)
    rfq_skipped_after=$(counter PolarDB_RFQ_Profile_Skipped)
    rfq_evicted_after=$(counter PolarDB_RFQ_Profile_Evicted)
    rfq_mismatch_free_after=$(pool_value 30 ConnFree)
    rfq_mismatch_result=$(last_endpoint_payload_from_output "$rfq_mismatch_out")
    if reader_endpoint_matches "$rfq_mismatch_warm_endpoint" &&
        reader_payload_matches "$rfq_mismatch_result" "$rfq_mismatch_marker" &&
        [ "$protect_delta" -ge 1 ] &&
        [ $((rfq_skipped_after - rfq_skipped_before)) -eq 0 ] &&
        [ $((rfq_evicted_after - rfq_evicted_before)) -eq 0 ] &&
        [ "$rfq_mismatch_free_after" -ge "$rfq_mismatch_free_before" ]; then
        ok 0 "RFQ profile mismatch: new profile does not scan or remove old profile entries"
    else
        diag "rfq-mismatch warm output: $rfq_mismatch_warm"
        diag "rfq-mismatch output: $rfq_mismatch_out"
        diag "warm_endpoint=$rfq_mismatch_warm_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT result=$rfq_mismatch_result expected=$REPLICA_SERVER_ENDPOINT|$rfq_mismatch_marker wait_delta=$wait_delta bypass_delta=$bypass_delta free_before=$rfq_mismatch_free_before free_after=$rfq_mismatch_free_after skipped_delta=$((rfq_skipped_after - rfq_skipped_before)) evicted_delta=$((rfq_evicted_after - rfq_evicted_before))"
        ok 1 "RFQ profile mismatch: new profile does not scan or remove old profile entries"
    fi
}

# RFQ profile match: a compatible pooled replica is reused without eviction.
case_rfq_profile_reuse() {
    set_hg_pair_policy_one_reader 31 32 v15 lsn -1 5000 1
    set_default_hostgroup 31
    rfq_skipped_before=$(counter PolarDB_RFQ_Profile_Skipped)
    rfq_evicted_before=$(counter PolarDB_RFQ_Profile_Evicted)
    snapshot_consistency_counters rfq_reuse_before
    rfq_reuse_marker="rfq_profile_reuse_$$"
    # RFQ reuse keys include the startup client identity, so warm and measure
    # in one frontend session. Separate psql processes correctly force eviction.
    rfq_reuse_out=$(
        proxy_script 2>&1 <<SQL
SELECT host(inet_server_addr()) || ':' || inet_server_port();
INSERT INTO $TEST_TABLE VALUES (2902, '$rfq_reuse_marker') ON CONFLICT (id) DO UPDATE SET data='$rfq_reuse_marker';
SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|' || data FROM $TEST_TABLE WHERE id=2902;
SQL
    )
    snapshot_consistency_counters rfq_reuse_after
    wait_delta=$(snapshot_counter_delta rfq_reuse_before rfq_reuse_after wait)
    bypass_delta=$(snapshot_counter_delta rfq_reuse_before rfq_reuse_after bypass)
    protect_delta=$(snapshot_protect_delta rfq_reuse_before rfq_reuse_after)
    rfq_skipped_after=$(counter PolarDB_RFQ_Profile_Skipped)
    rfq_evicted_after=$(counter PolarDB_RFQ_Profile_Evicted)
    rfq_reuse_warm_endpoint=$(first_endpoint_from_output "$rfq_reuse_out")
    rfq_reuse_result=$(last_endpoint_payload_from_output "$rfq_reuse_out")
    if reader_endpoint_matches "$rfq_reuse_warm_endpoint" &&
        reader_payload_matches "$rfq_reuse_result" "$rfq_reuse_marker" &&
        [ "$protect_delta" -ge 1 ] &&
        [ $((rfq_skipped_after - rfq_skipped_before)) -eq 0 ] &&
        [ $((rfq_evicted_after - rfq_evicted_before)) -eq 0 ]; then
        ok 0 "RFQ profile match: compatible pooled reader is reused without eviction"
    else
        diag "rfq-reuse output: $rfq_reuse_out"
        diag "warm_endpoint=$rfq_reuse_warm_endpoint expected_reader=$REPLICA_SERVER_ENDPOINT result=$rfq_reuse_result expected=$REPLICA_SERVER_ENDPOINT|$rfq_reuse_marker wait_delta=$wait_delta bypass_delta=$bypass_delta skipped_delta=$((rfq_skipped_after - rfq_skipped_before)) evicted_delta=$((rfq_evicted_after - rfq_evicted_before))"
        ok 1 "RFQ profile match: compatible pooled reader is reused without eviction"
    fi
    set_default_hostgroup "$WRITER_HG"
    remove_extra_hg_pairs
    set_select_rule_auto
    set_hg_policy lsn -1 5000
}

# CF-6: a user-raised WARNING that looks like a timeout during a wrapped read must
# not be counted as a wait timeout.
case_cf6_fake_timeout_notice() {
    timeout_before=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    cf6_marker="cf6_fake_timeout_notice_$$"
    cf6_out=$(
        proxy_script 2>&1 <<SQL
CREATE OR REPLACE FUNCTION ${TEST_TABLE}_fake_timeout_notice() RETURNS text LANGUAGE plpgsql AS \$\$
BEGIN
    RAISE WARNING 'LSN wait timeout after 777 ms';
    RETURN 'cf6_fake_notice_ok';
END;
\$\$;
INSERT INTO $TEST_TABLE VALUES (906, '$cf6_marker') ON CONFLICT (id) DO UPDATE SET data='$cf6_marker';
SELECT ${TEST_TABLE}_fake_timeout_notice();
SQL
    )
    timeout_after=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    if printf '%s\n' "$cf6_out" | grep -q '^cf6_fake_notice_ok$' &&
        printf '%s\n' "$cf6_out" | grep -q 'LSN wait timeout after 777 ms' &&
        [ $((wait_after - wait_before)) -ge 1 ] &&
        [ $((timeout_after - timeout_before)) -eq 0 ] &&
        [ $((lsn_timeout_after - lsn_timeout_before)) -eq 0 ]; then
        ok 0 "CF-6: user timeout-looking WARNING during wrapped read is not counted"
    else
        diag "cf6 output: $cf6_out"
        diag "wait_delta=$((wait_after - wait_before)) timeout_delta=$((timeout_after - timeout_before)) lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before))"
        ok 1 "CF-6: user timeout-looking WARNING during wrapped read is not counted"
    fi
}

# Wrapper SET error (debug): a failed wrapper SET is surfaced as an error but is
# not counted as a wait timeout nor retried.
case_wrapper_set_error() {
    if debug_wrap_set_error_faults_available; then
        timeout_before=$(counter PolarDB_Wait_Error_Timeout)
        lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
        retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
        wait_before=$(counter PolarDB_Wait_LSN_Sent)
        wrapper_set_error_trace_pattern="PolarDB WAIT WRAP: debug forced invalid wrapper SET"
        wrapper_set_error_trace_before=$(lsn_trace_count "$wrapper_set_error_trace_pattern")
        set_debug_wrap_set_error
        wrapper_set_error_marker="wrapper_set_error_$$"
        wrapper_set_error_out=$(
            proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (907, '$wrapper_set_error_marker') ON CONFLICT (id) DO UPDATE SET data='$wrapper_set_error_marker';
SELECT data FROM $TEST_TABLE WHERE id=907;
SQL
        )
        wrapper_set_error_rc=$?
        timeout_after=$(counter PolarDB_Wait_Error_Timeout)
        lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
        retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
        wait_after=$(counter PolarDB_Wait_LSN_Sent)
        wrapper_set_error_trace_delta=$(lsn_trace_delta "$wrapper_set_error_trace_pattern" "$wrapper_set_error_trace_before")
        if [ "$wrapper_set_error_rc" -ne 0 ] &&
            [ $((wait_after - wait_before)) -ge 1 ] &&
            [ $((timeout_after - timeout_before)) -eq 0 ] &&
            [ $((lsn_timeout_after - lsn_timeout_before)) -eq 0 ] &&
            [ $((retry_after - retry_before)) -eq 0 ] &&
            { [ "$wrapper_set_error_trace_delta" -eq 1 ] || [ "$wrapper_set_error_trace_delta" -eq -1 ]; } &&
            printf '%s\n' "$wrapper_set_error_out" | grep -Eiq 'ERROR|not-a-lsn|invalid'; then
            ok 0 "debug wrapper SET error is not counted as wait timeout or retried"
        else
            diag "wrapper-set-error output: $wrapper_set_error_out"
            diag "wrapper_set_error_rc=$wrapper_set_error_rc wait_delta=$((wait_after - wait_before)) timeout_delta=$((timeout_after - timeout_before)) lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before)) retry_delta=$((retry_after - retry_before)) trace_delta=$wrapper_set_error_trace_delta pattern='$wrapper_set_error_trace_pattern' (-1 means trace unavailable)"
            ok 1 "debug wrapper SET error is not counted as wait timeout or retried"
        fi
    else
        skip_ok "debug wrapper SET error is not counted as wait timeout or retried" "requires POLARDB_DEBUG wrapper SET fault support"
    fi
}

# Timeout edge tests (opt-in): under forced replay lag, strict timeout retries on
# the primary, best_effort forwards one WARNING, and a following extended read has no
# leftover LSN wait notice.
case_timeout_edge_tests() {
    if [ "${POLARDB_TIMEOUT_EDGE_TESTS:-0}" = "1" ]; then
        if set_replay_lag_bytes "${POLARDB_TIMEOUT_EDGE_REPLAY_LAG_BYTES:-104857600}"; then
            set_select_rule_auto
            set_lsn_mode lsn
            set_hg_policy lsn -1 1

            set_wait_mode strict
            timeout_before=$(counter PolarDB_Wait_Error_Timeout)
            lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
            retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
            strict_marker="strict_timeout_$$"
            strict_out=$(
                proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (301, '$strict_marker') ON CONFLICT (id) DO UPDATE SET data='$strict_marker';
SELECT data FROM $TEST_TABLE WHERE id=301;
SQL
            )
            strict_rc=$?
            timeout_after=$(counter PolarDB_Wait_Error_Timeout)
            lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
            retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
            strict_timeout_delta=$((timeout_after - timeout_before))
            strict_lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before))
            strict_retry_delta=$((retry_after - retry_before))
            if [ "$strict_rc" -eq 0 ] &&
                [ "$strict_timeout_delta" -eq 1 ] &&
                [ "$strict_lsn_timeout_delta" -eq 1 ] &&
                [ "$strict_retry_delta" -eq 1 ] &&
                printf '%s\n' "$strict_out" | grep -q "^$strict_marker$"; then
                ok 0 "strict wait timeout: timed-out reader read retries on writer"
            else
                diag "strict output: $strict_out"
                diag "strict_rc=$strict_rc timeout_delta=$strict_timeout_delta lsn_timeout_delta=$strict_lsn_timeout_delta retry_delta=$strict_retry_delta"
                ok 1 "strict wait timeout: timed-out reader read retries on writer"
            fi

            set_wait_mode best_effort
            timeout_before=$(counter PolarDB_Wait_Error_Timeout)
            lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
            best_marker="best_effort_timeout_$$"
            best_out=$(
                proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (302, '$best_marker') ON CONFLICT (id) DO UPDATE SET data='$best_marker';
SELECT data FROM $TEST_TABLE WHERE id=302;
SQL
            )
            best_rc=$?
            timeout_after=$(counter PolarDB_Wait_Error_Timeout)
            lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
            best_timeout_delta=$((timeout_after - timeout_before))
            best_lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before))
            best_warning_count=$(printf '%s\n' "$best_out" | grep -Eic 'WARNING:.*LSN wait timeout')
            if [ "$best_rc" -eq 0 ] &&
                [ "$best_timeout_delta" -eq 1 ] &&
                [ "$best_lsn_timeout_delta" -eq 1 ] &&
                [ "$best_warning_count" -eq 1 ]; then
                ok 0 "best-effort wait timeout: timeout notice is forwarded and query continues"
            else
                diag "best-effort output: $best_out"
                diag "best_rc=$best_rc timeout_delta=$best_timeout_delta lsn_timeout_delta=$best_lsn_timeout_delta warning_count=$best_warning_count"
                ok 1 "best-effort wait timeout: timeout notice is forwarded and query continues"
            fi

            wait_before=$(counter PolarDB_Wait_LSN_Sent)
            extended_transition_marker="best_effort_then_extended_$$"
            extended_transition_setup_sql="INSERT INTO $TEST_TABLE VALUES (304, '$extended_transition_marker') ON CONFLICT (id) DO UPDATE SET data='$extended_transition_marker';"
            extended_transition_wait_sql="SELECT data FROM $TEST_TABLE WHERE id=304;"
            if ensure_extended_helper; then
                extended_transition_out=$(POLARDB_EXTENDED_SETUP_SQL="$extended_transition_setup_sql" \
                    POLARDB_EXTENDED_SETUP_SQL2="$extended_transition_wait_sql" \
                    POLARDB_EXTENDED_REPORT_NOTICES=1 \
                    run_extended_query "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=304;" 2>&1)
                extended_transition_rc=$?
                wait_after=$(counter PolarDB_Wait_LSN_Sent)
                setup_notice_count=$(printf '%s\n' "$extended_transition_out" | sed -n 's/^setup_notices=//p' | tail -1)
                final_notice_count=$(printf '%s\n' "$extended_transition_out" | sed -n 's/^final_notices=//p' | tail -1)
                setup_notice_count="${setup_notice_count:-0}"
                final_notice_count="${final_notice_count:-0}"
                if [ "$extended_transition_rc" -eq 0 ] &&
                    [ "$setup_notice_count" -ge 1 ] &&
                    [ "$final_notice_count" -eq 0 ] &&
                    [ $((wait_after - wait_before)) -ge 1 ]; then
                    ok 0 "best-effort timeout followed by extended protocol has no leftover LSN wait notice"
                else
                    diag "best-effort-to-extended output: $extended_transition_out"
                    diag "extended_transition_rc=$extended_transition_rc setup_notices=$setup_notice_count final_notices=$final_notice_count wait_delta=$((wait_after - wait_before))"
                    ok 1 "best-effort timeout followed by extended protocol has no leftover LSN wait notice"
                fi
            else
                sed 's/^/#   /' "$EXTENDED_HELPER_BUILD_LOG" 2>/dev/null || true
                skip_ok "best-effort timeout followed by extended protocol has no leftover LSN wait notice" "cannot build proxysql_extended_protocol_test"
            fi

            set_replay_lag_bytes 0 >/dev/null 2>&1 || true
            TIMEOUT_EDGE_LAG_SET=0
            set_wait_mode best_effort
            set_hg_policy lsn -1 5000
            if ! wait_for_replica_replay_catchup 20; then
                diag "replica did not catch up after disabling polar_replay_min_lag_size"
            fi
        else
            skip_ok "strict wait timeout: timed-out reader read retries on writer" "cannot set polar_replay_min_lag_size through managed DCS"
            skip_ok "best-effort wait timeout: timeout notice is forwarded and query continues" "cannot set polar_replay_min_lag_size through managed DCS"
            skip_ok "best-effort timeout followed by extended protocol has no leftover LSN wait notice" "cannot set polar_replay_min_lag_size through managed DCS"
        fi
    else
        skip_ok "strict wait timeout: timed-out reader read retries on writer" "set POLARDB_TIMEOUT_EDGE_TESTS=1 to alter managed replay lag"
        skip_ok "best-effort wait timeout: timeout notice is forwarded and query continues" "set POLARDB_TIMEOUT_EDGE_TESTS=1 to alter managed replay lag"
        skip_ok "best-effort timeout followed by extended protocol has no leftover LSN wait notice" "set POLARDB_TIMEOUT_EDGE_TESTS=1 to alter managed replay lag"
    fi
}

# A user query error after the wrapper is consumed must not be counted as a wait
# timeout nor retried.
case_user_error_after_wait() {
    set_select_rule_auto
    set_lsn_mode lsn
    set_wait_mode best_effort
    set_hg_policy lsn -1 5000
    timeout_before=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    user_error_marker="user_error_after_wait_$$"
    user_error_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (303, '$user_error_marker') ON CONFLICT (id) DO UPDATE SET data='$user_error_marker';
SELECT * FROM ${TEST_TABLE}_missing_relation;
SQL
    )
    user_error_rc=$?
    timeout_after=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    if [ "$user_error_rc" -ne 0 ] &&
        [ $((wait_after - wait_before)) -ge 1 ] &&
        [ $((timeout_after - timeout_before)) -eq 0 ] &&
        [ $((lsn_timeout_after - lsn_timeout_before)) -eq 0 ] &&
        [ $((retry_after - retry_before)) -eq 0 ] &&
        printf '%s\n' "$user_error_out" | grep -Eiq 'does not exist|ERROR'; then
        ok 0 "user query error after wrapper consumption is not counted or retried"
    else
        diag "user-error output: $user_error_out"
        diag "user_error_rc=$user_error_rc wait_delta=$((wait_after - wait_before)) timeout_delta=$((timeout_after - timeout_before)) lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before)) retry_delta=$((retry_after - retry_before))"
        ok 1 "user query error after wrapper consumption is not counted or retried"
    fi
}

# A best_effort syntax error after wrapper consumption must not be counted nor
# retried.
case_best_effort_syntax_error() {
    timeout_before=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    syntax_error_marker="syntax_error_after_wait_$$"
    syntax_error_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (305, '$syntax_error_marker') ON CONFLICT (id) DO UPDATE SET data='$syntax_error_marker';
SELECT data FROM;
SQL
    )
    syntax_error_rc=$?
    timeout_after=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    if [ "$syntax_error_rc" -ne 0 ] &&
        [ $((wait_after - wait_before)) -ge 1 ] &&
        [ $((timeout_after - timeout_before)) -eq 0 ] &&
        [ $((lsn_timeout_after - lsn_timeout_before)) -eq 0 ] &&
        [ $((retry_after - retry_before)) -eq 0 ] &&
        printf '%s\n' "$syntax_error_out" | grep -Eiq 'syntax error|ERROR'; then
        ok 0 "best-effort syntax error after wrapper consumption is not counted or retried"
    else
        diag "syntax-error output: $syntax_error_out"
        diag "syntax_error_rc=$syntax_error_rc wait_delta=$((wait_after - wait_before)) timeout_delta=$((timeout_after - timeout_before)) lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before)) retry_delta=$((retry_after - retry_before))"
        ok 1 "best-effort syntax error after wrapper consumption is not counted or retried"
    fi
}

# A strict syntax error after wrapper consumption must not be counted nor retried.
case_strict_syntax_error() {
    set_wait_mode strict
    timeout_before=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_before=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_before=$(counter PolarDB_Wait_LSN_Sent)
    retry_before=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    strict_syntax_error_marker="strict_syntax_error_after_wait_$$"
    strict_syntax_error_out=$(
        proxy_script 2>&1 <<SQL
INSERT INTO $TEST_TABLE VALUES (306, '$strict_syntax_error_marker') ON CONFLICT (id) DO UPDATE SET data='$strict_syntax_error_marker';
SELECT data FROM;
SQL
    )
    strict_syntax_error_rc=$?
    timeout_after=$(counter PolarDB_Wait_Error_Timeout)
    lsn_timeout_after=$(counter PolarDB_Wait_Error_LSN_Wait_Timeout)
    wait_after=$(counter PolarDB_Wait_LSN_Sent)
    retry_after=$(counter PolarDB_Wait_Reads_Retried_On_Writer)
    if [ "$strict_syntax_error_rc" -ne 0 ] &&
        [ $((wait_after - wait_before)) -ge 1 ] &&
        [ $((timeout_after - timeout_before)) -eq 0 ] &&
        [ $((lsn_timeout_after - lsn_timeout_before)) -eq 0 ] &&
        [ $((retry_after - retry_before)) -eq 0 ] &&
        printf '%s\n' "$strict_syntax_error_out" | grep -Eiq 'syntax error|ERROR'; then
        ok 0 "strict syntax error after wrapper consumption is not counted or retried"
    else
        diag "strict-syntax-error output: $strict_syntax_error_out"
        diag "strict_syntax_error_rc=$strict_syntax_error_rc wait_delta=$((wait_after - wait_before)) timeout_delta=$((timeout_after - timeout_before)) lsn_timeout_delta=$((lsn_timeout_after - lsn_timeout_before)) retry_delta=$((retry_after - retry_before))"
        ok 1 "strict syntax error after wrapper consumption is not counted or retried"
    fi
}

plan "$PLAN"
diag "test profile: POLARDB_TEST_ENV=$POLARDB_TEST_ENV POLARDB_DCS_MODE=$POLARDB_DCS_MODE writer=${PRIMARY_HOST:-auto}:${PRIMARY_PORT:-auto} readers=${POLARDB_REPLICA_ENDPOINTS:-${REPLICA_HOST:-auto}:${REPLICA_PORT:-auto}}"
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
detect_monitor_health_support

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
    ok 0 "configure writer/reader hostgroups and replica_eligible SELECT rule"
else
    ok 1 "configure writer/reader hostgroups and replica_eligible SELECT rule"
    exit 1
fi

lag_ms_reject_out=$(admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_lag_ms'; LOAD PGSQL VARIABLES TO RUNTIME;" 2>&1)
lag_ms_runtime=$(admin_sql "SELECT variable_value FROM runtime_global_variables WHERE variable_name='pgsql-polardb_lag_ms';" 2>/dev/null | tr -d '[:space:]')
lag_ms_admin=$(admin_sql "SELECT variable_value FROM global_variables WHERE variable_name='pgsql-polardb_lag_ms';" 2>/dev/null | tr -d '[:space:]')
admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name='pgsql-polardb_lag_ms'; LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
if [ "$lag_ms_runtime" = "0" ]; then
    ok 0 "T8: pgsql-polardb_lag_ms rejects nonzero value until ms-lag producer exists"
else
    diag "lag_ms=1 load output: $lag_ms_reject_out"
    diag "lag_ms runtime=$lag_ms_runtime admin=$lag_ms_admin expected_runtime=0"
    ok 1 "T8: pgsql-polardb_lag_ms rejects nonzero value until ms-lag producer exists"
fi

if direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP TABLE IF EXISTS $TEST_TABLE; CREATE TABLE $TEST_TABLE(id int PRIMARY KEY, data text);" >/dev/null 2>&1; then
    ok 0 "create test table on writer"
else
    ok 1 "create test table on writer"
    exit 1
fi

# ==== Config round-trip and wrapper safety ====
case_cf4_replica_eligible_roundtrip
case_cf7_wrapper_finalize_fail

# ==== RYW and basic routing modes ====
case_ryw_write_read
case_wrapper_filter
case_route_wait_spec_reader_selection
case_no_write_read
case_read_then_read_monotonic
case_read_then_write_then_read
case_background_writes_read_only_session
case_mode_off
case_mode_primary
case_manual_rule_route
case_manual_sql_hint

# ==== Extended protocol routing ====
case_cf3_extended_manual_reader
case_cf3_extended_auto_no_write
case_cf3_extended_auto_after_write
case_explicit_txn_writer
case_txn_split_prewrite_uses_reader_wait

# ==== Proxy-protocol RFQ scope ====
case_protocol_rfq_scope

# ==== Startup identity fallback ====
case_startup_identity_safety
case_startup_identity_listener
case_startup_identity_configured

# ==== RFQ availability policy ====
case_rfq_strict_and_best_effort
case_primary_baseline_unknown
case_plan_stage_best_effort_degrade
case_missing_writer_rfq_state
case_missing_reader_rfq_state

# ==== Route hints and session overrides ====
case_route_primary_hint
case_multi_statement
case_session_consistency_override
case_session_enable_then_disable
case_query_cache_bypassed_after_session_write
case_reset_all_preserves_lsn

# ==== Replica acquisition faults ====
case_reader_acquire_faults

# ==== Monitor LSN updates ====
case_monitor_baseline
case_monitor_disabled
case_monitor_health_faults
case_cf8_rfq_vs_monitor

# ==== Lag-cap and freshness routing ====
case_lag_cap_freshness
case_reset_caps
case_cf1_in_cap
case_cf2_cold_reader

# ==== RFQ connection-profile reuse ====
case_rfq_profile_mismatch
case_rfq_profile_reuse

# ==== Wrapper error attribution ====
case_cf6_fake_timeout_notice
case_wrapper_set_error
case_timeout_edge_tests
case_user_error_after_wait
case_best_effort_syntax_error
case_strict_syntax_error

if [ "$FAIL" -eq 0 ]; then
    diag "all LSN session-consistency checks passed"
else
    diag "$FAIL TAP checks failed"
fi

exit "$FAIL"
