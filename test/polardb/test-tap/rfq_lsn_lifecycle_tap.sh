#!/usr/bin/env bash
# Focused RFQ-LSN lifecycle diagnostic.
#
# The CH-benCHmark connection shape runs SET SESSION CHARACTERISTICS, SET
# search_path, then analytical SELECTs. A PolarDB15 proxy connection that
# requested RFQ LSNs may return either zero or a positioned value for setup
# statements. In both cases payload presence must survive, so setup does not
# poison the session as missing-LSN and force later OLAP reads to the writer.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_rfq_lsn_lifecycle_tap")}"
PROXYSQL_START_LOG="${PROXYSQL_DATA_DIR}.start.log"
PROXYSQL_PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}"
WRITER_HG="$POLARDB_WRITER_HG"
READER_HG="$POLARDB_READER_HG"
PLAN=8
FAIL=0
STARTED_PROXY=0

plan "$PLAN"

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
    PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$PGSSLMODE" psql -h "$host" -p "$port" \
        -U "$PGUSER_DIRECT" -d "$PGDB" -A -t -q -v ON_ERROR_STOP=1 -c "$sql"
}

backend_endpoint_for() {
    direct_sql "$1" "$2" "SELECT host(inet_server_addr()) || ':' || inet_server_port();" 2>/dev/null | tr -d '[:space:]'
}

detect_backend_endpoints() {
    local endpoint host port reader_endpoint

    PRIMARY_SERVER_ENDPOINT=$(backend_endpoint_for "$PRIMARY_HOST" "$PRIMARY_PORT")
    REPLICA_SERVER_ENDPOINTS=""
    for endpoint in $(polardb_each_replica_endpoint); do
        host=$(polardb_endpoint_host "$endpoint")
        port=$(polardb_endpoint_port "$endpoint")
        reader_endpoint=$(backend_endpoint_for "$host" "$port")
        if [ -n "$reader_endpoint" ]; then
            REPLICA_SERVER_ENDPOINTS=$(polardb_append_endpoint_once "$REPLICA_SERVER_ENDPOINTS" "$reader_endpoint")
        fi
    done
    export PRIMARY_SERVER_ENDPOINT REPLICA_SERVER_ENDPOINTS
    [ -n "$PRIMARY_SERVER_ENDPOINT" ] && [ -n "$REPLICA_SERVER_ENDPOINTS" ]
}

reader_endpoint_matches() {
    local endpoint="$1"
    local reader

    for reader in $REPLICA_SERVER_ENDPOINTS; do
        [ "$endpoint" = "$reader" ] && return 0
    done
    return 1
}

insert_reader_servers() {
    local reader_hg="$1"
    local endpoint host port

    for endpoint in $(polardb_each_replica_endpoint); do
        host=$(polardb_endpoint_host "$endpoint")
        port=$(polardb_endpoint_port "$endpoint")
        admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($reader_hg, '$host', $port, 'ONLINE', 1000, 100);" >/dev/null
    done
}

start_proxy() {
    rm -f "$PROXYSQL_START_LOG"
    if PROXYSQL_DEBUG="${PROXYSQL_DEBUG:-1}" \
        PROXYSQL_PGSQL_THREADS="${PROXYSQL_PGSQL_THREADS:-1}" \
        PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" restart \
        --data-dir "$PROXYSQL_DATA_DIR" \
        --admin-port "$PROXYSQL_ADMIN_PORT" \
        --proxy-port "$PROXYSQL_PORT" \
        --mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT" >"$PROXYSQL_START_LOG" 2>&1; then
        STARTED_PROXY=1
        return 0
    fi
    return 1
}

stop_proxy() {
    if [ "$STARTED_PROXY" -eq 1 ]; then
        PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop --data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    stop_proxy
}
trap cleanup EXIT

configure_proxy() {
    admin_sql "DELETE FROM pgsql_servers;" >/dev/null
    admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, 100);" >/dev/null
    insert_reader_servers "$READER_HG"

    admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null
    admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment) VALUES ($WRITER_HG, $READER_HG, 'polardb', 'session_lsn', -1, 5000, 'v15', 'rfq_lsn_lifecycle_tap');" >/dev/null

    admin_sql "DELETE FROM pgsql_users;" >/dev/null
    admin_sql "INSERT INTO pgsql_users (username, password, active, default_hostgroup) VALUES ('$PGUSER', '$PGPASSWORD', 1, $WRITER_HG);" >/dev/null

    set_select_rule_auto "rfq_lsn_lifecycle_select"

    admin_sql "UPDATE global_variables SET variable_value='session_lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='primary' WHERE variable_name='pgsql-polardb_action_missing_lsn';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_monitor_lsn_updates';" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='0' WHERE variable_name IN ('pgsql-polardb_max_reader_lag_ms','pgsql-polardb_max_reader_lsn_gap_bytes');" >/dev/null
    admin_sql "UPDATE global_variables SET variable_value='5000' WHERE variable_name='pgsql-polardb_reader_lsn_max_age_ms';" >/dev/null

    admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
    admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
    admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
}

if ! polardb_detect_topology >/dev/null 2>&1; then
    diag "PolarDB topology is not configured/detectable"
    polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
    exit 0
fi

if ! detect_backend_endpoints; then
    diag "cannot detect backend-reported writer/reader endpoints"
    polardb_skip_remaining "backend endpoint detection failed" "$PLAN"
    exit 0
fi

polardb_require_command_or_skip_all psql "psql"
polardb_require_proxysql_or_skip_all polardb-debug

ok 0 "built POLARDB_PROXY=1 ProxySQL binary exists"

if start_proxy; then
    ok 0 "start debug ProxySQL test instance"
else
    diag "ProxySQL start log: $PROXYSQL_START_LOG"
    sed 's/^/#   /' "$PROXYSQL_START_LOG" 2>/dev/null || true
    ok 1 "start debug ProxySQL test instance"
    exit "$FAIL"
fi

if configure_proxy; then
    ok 0 "configure V15 RFQ-LSN writer/reader hostgroups"
else
    ok 1 "configure V15 RFQ-LSN writer/reader hostgroups"
    exit "$FAIL"
fi

write_missing_before=$(counter PolarDB_Write_Missing_LSN)
read_missing_before=$(counter PolarDB_Read_Missing_LSN)
rfq_updates_before=$(counter PolarDB_Server_LSN_Updates_From_RFQ)
writer_queries_before=$(admin_sql "SELECT COALESCE(SUM(Queries),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$WRITER_HG;" 2>/dev/null | tr -d '[:space:]')
reader_queries_before=$(admin_sql "SELECT COALESCE(SUM(Queries),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG;" 2>/dev/null | tr -d '[:space:]')

out=$(proxy_command_sequence \
    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;" \
    "SET search_path TO public;" \
    "SELECT host(inet_server_addr()) || ':' || inet_server_port() || '|rfq_lifecycle';" \
    2>&1)
rc=$?

write_missing_after=$(counter PolarDB_Write_Missing_LSN)
read_missing_after=$(counter PolarDB_Read_Missing_LSN)
rfq_updates_after=$(counter PolarDB_Server_LSN_Updates_From_RFQ)
writer_queries_after=$(admin_sql "SELECT COALESCE(SUM(Queries),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$WRITER_HG;" 2>/dev/null | tr -d '[:space:]')
reader_queries_after=$(admin_sql "SELECT COALESCE(SUM(Queries),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG;" 2>/dev/null | tr -d '[:space:]')

write_missing_delta=$((write_missing_after - write_missing_before))
read_missing_delta=$((read_missing_after - read_missing_before))
rfq_update_delta=$((rfq_updates_after - rfq_updates_before))
writer_query_delta=$((writer_queries_after - writer_queries_before))
reader_query_delta=$((reader_queries_after - reader_queries_before))
select_result=$(printf '%s\n' "$out" | grep -E '^[0-9.]+:[0-9]+\|rfq_lifecycle$' | tail -1 | tr -d '[:space:]')
select_endpoint="${select_result%%|*}"

if [ "$rc" -eq 0 ] && [ -n "$select_result" ]; then
    ok 0 "CH-style setup SQL and SELECT complete through ProxySQL"
else
    diag "query rc=$rc output: $out"
    ok 1 "CH-style setup SQL and SELECT complete through ProxySQL"
fi

if [ "$write_missing_delta" -eq 0 ]; then
    ok 0 "SET setup statements do not mark write LSN unknown"
else
    diag "write_missing_delta=$write_missing_delta output: $out"
    ok 1 "SET setup statements do not mark write LSN unknown"
fi

if [ "$read_missing_delta" -eq 0 ]; then
    ok 0 "SELECT result does not mark observed LSN unknown"
else
    diag "read_missing_delta=$read_missing_delta output: $out"
    ok 1 "SELECT result does not mark observed LSN unknown"
fi

if reader_endpoint_matches "$select_endpoint"; then
    ok 0 "SELECT is not forced to writer after setup RFQ payloads"
else
    diag "select_endpoint=$select_endpoint expected_readers=$REPLICA_SERVER_ENDPOINTS writer=$PRIMARY_SERVER_ENDPOINT"
    diag "writer_query_delta=$writer_query_delta reader_query_delta=$reader_query_delta"
    diag "rfq_update_delta=$rfq_update_delta"
    ok 1 "SELECT is not forced to writer after setup RFQ payloads"
fi

trace_file="$PROXYSQL_DATA_DIR/proxysql.log"
if tap_trace_checks_enabled "$trace_file"; then
    if tap_trace_must_count_ge "$trace_file" "PolarDB PROCESS_RESULT: rfq probe requested_lsn=1 payload_present=1" 2 &&
        tap_trace_must_count_ge "$trace_file" "PolarDB PROCESS_RESULT: accepted read RFQ LSN" 1; then
        ok 0 "debug trace records RFQ probe for backend-dispatched setup/read statements"
    else
        ok 1 "debug trace records RFQ probe for backend-dispatched setup/read statements"
    fi
else
    if tap_debug_traces_required; then
        ok 1 "debug trace records RFQ probe for backend-dispatched setup/read statements"
    else
        skip_ok "debug trace records RFQ probe for backend-dispatched setup/read statements" "ProxySQL debug trace unavailable"
    fi
fi

diag "deltas: write_missing=$write_missing_delta read_missing=$read_missing_delta rfq_updates=$rfq_update_delta writer_queries=$writer_query_delta reader_queries=$reader_query_delta"
diag "logs: $trace_file"

exit "$FAIL"
