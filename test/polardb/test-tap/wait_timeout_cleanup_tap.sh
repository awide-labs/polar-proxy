#!/usr/bin/env bash
# TAP integration test for PolarDB LSN wait-timeout behavior.
#
# This test covers the timeout branches that cannot be confirmed by unit tests:
#   - finite best_effort/strict wait that reaches the target;
#   - finite best_effort timeout: exactly one WARNING + stale read. The patched
#     PolarDB-15 backend consumes the wait target after the first wait, so there
#     is no leftover target and the proxy needs no clear-prefix wrapper;
#   - finite strict timeout: timeout is counted, then the read is retried once
#     on the writer before any user result is sent;
#   - lost reader connection during a wait-wrapped read: the read is retried once
#     on the writer before any user result is sent;
#   - a straight read after a best_effort timeout runs with no leftover wait;
#   - finite best_effort timeout followed by strict timeout;
#   - timeout 0 in best_effort/strict: no PolarDB timeout branch, waits until
#     catchup, still interruptible by normal PostgreSQL/client/admin paths.
#
# Requires a patched (consume-on-wait) PolarDB-15 backend. Run with a
# POLARDB_DEBUG=1 binary for full request-flow traces:
#
#   make -C "$PROXYSQL_ROOT" polardb-debug
#   cp test/polardb/.env.example test/polardb/.env
#   # edit POLARDB_ENDPOINTS and DCS settings in test/polardb/.env
#   POLARDB_TIMEOUT_EDGE_TESTS=1 make -C test/polardb tap-wait-timeout

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}"
DIRECT_PGSSLMODE="${DIRECT_PGSSLMODE:-$PGSSLMODE}"

TEST_TABLE="${TEST_TABLE:-$(polardb_test_identifier consistency_test)}"

# Topology hostgroups and the SELECT routing rule id come from env.sh so the
# test never hardcodes 10/11/10000 (see env.sh note).
WRITER_HG="$POLARDB_WRITER_HG"
READER_HG="$POLARDB_READER_HG"
SELECT_RULE_ID="$POLARDB_SELECT_RULE_ID"
# Lower bound (microseconds) that a timeout-0 wait must exceed to show it
# waited past any finite timeout used elsewhere in this test (5 seconds).
TIMEOUT0_MIN_WAIT_US=5000000

PLAN=69
FAIL=0
LOGGING_STARTED=0
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_wait_timeout_cleanup_tap")}"
export PROXYSQL_DATA_DIR

counter_value() {
    get_counter "$1" | tr -d '[:space:]'
}

counter_delta() {
    local name="$1"
    local before="$2"
    local after
    after=$(counter_value "$name")
    echo $((after - before))
}

proxy_trace_log() {
    printf '%s\n' "${PROXYSQL_LOG:-${PROXYSQL_DATA_DIR}/proxysql.log}"
}

proxy_trace_count() {
    local pattern="$1"
    tap_trace_count "$(proxy_trace_log)" "$pattern"
}

proxy_trace_delta() {
    local pattern="$1"
    local before="$2"
    local after

    if ! tap_trace_checks_enabled "$(proxy_trace_log)"; then
        echo -1
        return 0
    fi
    after=$(proxy_trace_count "$pattern")
    echo $((after - before))
}

# Snapshot every extracted-tree PolarDB counter so a scenario can assert its full
# delta vector: lockstep pairs, exact timeout count, sent/bypassed accounting,
# sum_us, etc.
snap_all_counters() {
    S_ROUTING=$(counter_value "PolarDB_Session_LSN_Routing")
    S_PREPARED=$(counter_value "PolarDB_Wait_Wrap_Prepared")
    S_BYPASSED=$(counter_value "PolarDB_Wait_Wrap_Bypassed")
    S_SENT=$(counter_value "PolarDB_Wait_LSN_Sent")
    S_SUMUS=$(counter_value "PolarDB_Wait_LSN_Sum_Us")
    S_ABORT=$(counter_value "PolarDB_Wait_Wrap_Safety_Abort")
    S_ELTO=$(counter_value "PolarDB_Wait_Error_LSN_Wait_Timeout")
    S_RFQ_LSN=$(counter_value "PolarDB_Server_LSN_Updates_From_RFQ")
}

process_alive_not_zombie() {
    local pid="$1"
    local stat

    stat=$(ps -p "$pid" -o stat= 2>/dev/null | tr -d '[:space:]')
    [ -n "$stat" ] && [ "${stat#Z}" = "$stat" ]
}

# proxy_script (single-arg form) and debug-fault file helpers come from
# lib/tap_polardb.sh. counter_value/counter_delta below stay local because they
# read counters through the harness get_counter() path, not admin_sql.

configure_wait_policy() {
    local mode="$1"
    local timeout_ms="$2"

    proxysql_admin "UPDATE global_variables SET variable_value='lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
    proxysql_admin "UPDATE global_variables SET variable_value='$mode' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
    proxysql_admin "UPDATE global_variables SET variable_value='1000' WHERE variable_name='pgsql-polardb_lag_wait_ms';" >/dev/null
    proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
    proxysql_admin "UPDATE pgsql_replication_hostgroups SET consistency_mode='lsn', max_lag_bytes=-1, lsn_wait_timeout_ms=$timeout_ms WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
    proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

# Local rule helpers (NOT the tap_polardb.sh versions): wait_timeout drives the
# admin surface through harness proxysql_admin(), and the auto rule matches on
# match_digest rather than match_pattern, so these stay specialized.
set_select_rule_auto() {
    proxysql_admin "DELETE FROM pgsql_query_rules;" >/dev/null
    proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_digest, replica_eligible, apply, comment) VALUES ($SELECT_RULE_ID, 1, '^SELECT', 1, 0, 'wait_timeout_cleanup_auto_select');" >/dev/null
    proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

set_select_rule_manual_reader() {
    proxysql_admin "DELETE FROM pgsql_query_rules;" >/dev/null
    proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply, comment) VALUES ($SELECT_RULE_ID, 1, '^SELECT', $READER_HG, 1, 'wait_timeout_cleanup_manual_reader');" >/dev/null
    proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

require_replay_lag_enabled() {
    local bytes="$1"
    local context="$2"
    if enable_replay_lag "$bytes"; then
        return 0
    fi
    diag "$context: could not enable replay lag bytes=$bytes"
    polardb_skip_remaining "replay lag setup failed" "$PLAN"
    exit 0
}

run_waiting_query_until_catchup() {
    local mode="$1"
    local id="$2"
    local out_file="$3"
    local hold_seconds="${TIMEOUT0_HOLD_SECONDS:-6.2}"

    configure_wait_policy "$mode" 0
    require_replay_lag_enabled 50000 "timeout wait setup"
    start_wal_generator 0 0

    proxy_script "INSERT INTO $TEST_TABLE VALUES ($id, 'timeout0_$mode') ON CONFLICT (id) DO UPDATE SET data='timeout0_$mode';
\\! sleep 0.5
    SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $id;" >"$out_file" 2>&1 &
    local pid=$!

    sleep "$hold_seconds"
    if process_alive_not_zombie "$pid"; then
        ok 0 "timeout 0/$mode: query remains waiting while replay lag is held for ${hold_seconds}s"
    else
        ok 1 "timeout 0/$mode: query should still be waiting before catchup"
    fi

    stop_wal_generator
    if ! disable_replay_lag; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        diag "timeout wait catchup: could not disable replay lag"
        polardb_skip_remaining "replay lag cleanup failed" "$PLAN"
        exit 0
    fi

    local waited=0
    while process_alive_not_zombie "$pid" && [ "$waited" -lt 20 ]; do
        sleep 0.5
        waited=$((waited + 1))
    done
    if process_alive_not_zombie "$pid"; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        ok 1 "timeout 0/$mode: query completed after replay catchup"
    else
        wait "$pid" 2>/dev/null || true
        ok 0 "timeout 0/$mode: query completed after replay catchup"
    fi
}

terminate_active_reader_wait() {
    local max_wait="${1:-10}"
    local waited=0
    local pid terminated

    while [ "$waited" -lt $((max_wait * 10)) ]; do
        pid=$(polardb_direct_sql "$REPLICA_HOST" "$REPLICA_PORT" \
            "SELECT pid FROM pg_stat_activity
             WHERE usename = '$PGUSER'
               AND datname = '$PGDB'
               AND state = 'active'
               AND query LIKE '%polar_xact_split_wait_lsn%'
             ORDER BY query_start
             LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
        if [ -n "$pid" ]; then
            terminated=$(polardb_direct_sql "$REPLICA_HOST" "$REPLICA_PORT" \
                "SELECT pg_terminate_backend($pid);" 2>/dev/null | tr -d '[:space:]')
            if [ "$terminated" = "t" ]; then
                return 0
            fi
        fi
        sleep 0.1
        waited=$((waited + 1))
    done

    return 1
}

debug_retry_faults_available() {
    debug_fault_file_ready POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE "$RUN_DIR/proxysql_strings"
}

set_debug_retry_fault() {
    local fault_name="$1"

    set_debug_fault_file POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE "$fault_name" || return 1
    grep -qx "$fault_name" "$POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE"
}

set_debug_retry_fault_or_skip() {
    local fault_name="$1"
    local label="$2"

    if ! debug_retry_faults_available; then
        skip_ok "$label" "requires POLARDB_DEBUG retry fault support"
        return 1
    fi
    if ! set_debug_retry_fault "$fault_name"; then
        skip_ok "$label" "cannot create POLARDB_DEBUG wait-retry fault trigger"
        return 1
    fi
    return 0
}

run_strict_timeout_no_retry_fault() {
    local fault_name="$1"
    local label="$2"
    local row_id="$3"
    local out timeout_before retry_before out_file rc
    local delta_timeout delta_retry
    local trace_pattern trace_before trace_delta

    if ! set_debug_retry_fault_or_skip "$fault_name" "$label"; then
        return 0
    fi

    configure_wait_policy strict 100
    require_replay_lag_enabled 50000 "$label setup"
    start_wal_generator 0 0
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    retry_before=$(counter_value "PolarDB_Wait_Reads_Retried_On_Writer")
    case "$fault_name" in
    writer_busy)
        trace_pattern="PolarDB WAIT: failed wait-read cleaned wrapper; primary retry declined"
        ;;
    *)
        trace_pattern="PolarDB WAIT: failed wait-read cleaned wrapper; normal error path will not redispatch wrapped packet"
        ;;
    esac
    trace_before=$(proxy_trace_count "$trace_pattern")
    out_file="$RUN_DIR/${fault_name}_no_retry.out"
    proxy_script "INSERT INTO $TEST_TABLE VALUES ($row_id, '$fault_name') ON CONFLICT (id) DO UPDATE SET data='$fault_name';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;" >"$out_file" 2>&1
    rc=$?
    stop_wal_generator
    out=$(cat "$out_file")
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    delta_retry=$(counter_delta "PolarDB_Wait_Reads_Retried_On_Writer" "$retry_before")
    trace_delta=$(proxy_trace_delta "$trace_pattern" "$trace_before")
    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

    if [ "$delta_timeout" -eq 1 ] &&
        [ "$delta_retry" -eq 0 ] &&
        { [ "$trace_delta" -eq 1 ] || [ "$trace_delta" -eq -1 ]; } &&
        printf '%s\n' "$out" | grep -q "ERROR:  LSN wait timeout" &&
        ! printf '%s\n' "$out" | grep -Eq "polar_xact_split_wait_lsn|polar_proxy_wait_timeout_ms|polar_consistency_mode|^SET$"; then
        ok 0 "$label"
    else
        diag "$label output: $out"
        diag "rc=$rc timeout_delta=$delta_timeout retry_delta=$delta_retry trace_delta=$trace_delta pattern='$trace_pattern'"
        ok 1 "$label"
    fi
}

# A reader connection lost mid wait-wrapped read must be retried once on the
# writer, hiding the backend failure and counting connection-loss (not timeout).
# Driven with timeout 0 so the only recoverable condition is the terminated
# reader -- this shows the retry path is independent of the wait_timeout mode
# (Branch 6 = strict, Branch 6b = best_effort use the same flow). Emits 6 ok().
run_reader_connection_loss_branch() {
    local mode="$1"
    local label_prefix="$2"
    local row_id="$3"
    local marker="$4"
    local out_basename="$5"
    local conn_lost_before retry_before timeout_before out_file reader_loss_pid
    local waited out delta_conn_lost delta_retry delta_timeout
    local trace_pattern trace_before trace_delta

    configure_wait_policy "$mode" 0
    require_replay_lag_enabled 50000 "${label_prefix}reader connection loss setup"
    start_wal_generator 0 0
    conn_lost_before=$(counter_value "PolarDB_Wait_Error_Connection_Lost")
    retry_before=$(counter_value "PolarDB_Wait_Reads_Retried_On_Writer")
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    trace_pattern="PolarDB WAIT: reader connection lost before user result; redirecting original unwrapped query"
    trace_before=$(proxy_trace_count "$trace_pattern")
    out_file="$RUN_DIR/${out_basename}.out"
    proxy_script "INSERT INTO $TEST_TABLE VALUES ($row_id, '$marker') ON CONFLICT (id) DO UPDATE SET data='$marker';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;" >"$out_file" 2>&1 &
    reader_loss_pid=$!
    terminate_active_reader_wait 10
    ok $? "${label_prefix}reader connection loss: terminated active wait backend"

    waited=0
    while process_alive_not_zombie "$reader_loss_pid" && [ "$waited" -lt 20 ]; do
        sleep 0.5
        waited=$((waited + 1))
    done
    if process_alive_not_zombie "$reader_loss_pid"; then
        kill "$reader_loss_pid" 2>/dev/null || true
        wait "$reader_loss_pid" 2>/dev/null || true
        ok 1 "${label_prefix}reader connection loss: query completed after writer retry"
    else
        wait "$reader_loss_pid" 2>/dev/null || true
        ok 0 "${label_prefix}reader connection loss: query completed after writer retry"
    fi
    stop_wal_generator
    out=$(cat "$out_file")
    delta_conn_lost=$(counter_delta "PolarDB_Wait_Error_Connection_Lost" "$conn_lost_before")
    delta_retry=$(counter_delta "PolarDB_Wait_Reads_Retried_On_Writer" "$retry_before")
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    trace_delta=$(proxy_trace_delta "$trace_pattern" "$trace_before")
    echo "$out" | grep -q '^1$'
    ok $? "${label_prefix}reader connection loss: writer retry returns result"
    ! echo "$out" | grep -Eq "ERROR|FATAL|server closed the connection|terminating connection"
    ok $? "${label_prefix}reader connection loss: backend failure is hidden from client"
    [ "$delta_conn_lost" -eq 1 ]
    ok $? "${label_prefix}reader connection loss increments connection-loss counter exactly once"
    [ "$delta_retry" -eq 1 ] && { [ "$trace_delta" -eq 1 ] || [ "$trace_delta" -eq -1 ]; }
    ok $? "${label_prefix}reader connection loss increments writer-retry counter exactly once and traces primary retry"
    [ "$trace_delta" -eq 1 ] || diag "${label_prefix}reader connection loss retry trace_delta=$trace_delta pattern='$trace_pattern' (-1 means trace unavailable)"
    [ "$delta_timeout" -eq 0 ]
    ok $? "${label_prefix}reader connection loss does not increment timeout counter"
    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true
}

cleanup() {
    stop_wal_generator
    disable_replay_lag >/dev/null 2>&1 || true
    PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$DIRECT_PGSSLMODE" psql \
        -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" \
        -U "$PGUSER_DIRECT" -d "$PGDB" \
        -c "DROP FUNCTION IF EXISTS polardb_lsn_timeout_warning_spoof();" \
        >/dev/null 2>&1 || true
    if [ "$PROXYSQL_STARTED" = "1" ]; then
        extract_logs || true
        stop_proxysql
    fi
    if [ "$LOGGING_STARTED" = "1" ]; then
        stop_logging || true
    fi
}

run_wait_timeout_cleanup_tap() {
    plan "$PLAN"
    polardb_require_proxysql_or_skip_all polardb
    polardb_require_command_or_skip_all psql psql

    topology_error_file="$(mktemp "${TMPDIR:-/tmp}/polardb-topology.XXXXXX")"
    if ! polardb_detect_topology 2>"$topology_error_file"; then
        topology_error="$(cat "$topology_error_file" 2>/dev/null || true)"
        rm -f "$topology_error_file"
        [ -z "${topology_error:-}" ] || diag "$topology_error"
        skip_ok "detect PolarDB writer/reader topology" "PolarDB topology unavailable"
        polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
        exit 0
    fi
    rm -f "$topology_error_file"

    # This suite kills the active wait on the direct replica connection. Keep the
    # ProxySQL reader hostgroup to that same replica so the fault injection cannot
    # land on another valid reader and silently skip the branch under test.
    POLARDB_REPLICA_ENDPOINTS="$REPLICA_HOST:$REPLICA_PORT"
    export POLARDB_REPLICA_ENDPOINTS

    HARNESS_AUTODETECT="$POLARDB_AUTODETECT"
    POLARDB_AUTODETECT=0
    # shellcheck source=../lib/scenario_harness.sh
    source "$SCRIPT_DIR/../lib/scenario_harness.sh"
    POLARDB_AUTODETECT="$HARNESS_AUTODETECT"
    trap cleanup EXIT

    init_logging "wait_timeout_cleanup"
    LOGGING_STARTED=1

    export POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE="$RUN_DIR/wait_retry_fault"
    : >"$POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE"
    export PROXYSQL_DEBUG=1

    if ! start_proxysql; then
        ok 1 "ProxySQL starts"
        exit 1
    fi
    ok 0 "ProxySQL starts"

    setup_test_table
    ok 0 "test table created"

    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15
    ok $? "replica replay reaches primary before test"

    proxy_script "/* hostgroup=$READER_HG */ SELECT 1;" >/dev/null
    ok $? "reader backend warmed"
    set_select_rule_auto

    # Marker handling check for ProxySQL notice handling. The SELECT below is a
    # normal consistency-wrapped read, but the warning text comes from user SQL, not
    # PolarDB's LSN wait code. ProxySQL must forward the user warning, return the
    # result, and not account it as PolarDB_Wait_Error_Timeout. This shows timeout
    # recognition is based on the backend detail marker, not the visible text.
    PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$DIRECT_PGSSLMODE" psql \
        -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" \
        -U "$PGUSER_DIRECT" -d "$PGDB" \
        -v ON_ERROR_STOP=1 \
        -c "CREATE OR REPLACE FUNCTION polardb_lsn_timeout_warning_spoof()
        RETURNS integer
        LANGUAGE plpgsql
        AS \$\$
        BEGIN
            RAISE WARNING 'LSN wait timeout after 123 ms';
            RETURN 1;
        END;
        \$\$;" >/dev/null
    ok $? "spoof warning function created on primary"
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    configure_wait_policy best_effort 5000
    out=$(proxy_script "SELECT polardb_lsn_timeout_warning_spoof();")
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    echo "$out" | grep -q "LSN wait timeout after 123 ms"
    ok $? "user warning with LSN timeout text is forwarded"
    echo "$out" | grep -q '^1$'
    ok $? "wrapped read with user warning still returns result"
    [ "$delta_timeout" -eq 0 ]
    ok $? "user warning text is not counted as PolarDB wait timeout"

    # Branch 1: finite best_effort success (target reached, no clear).
    configure_wait_policy best_effort 5000
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    snap_all_counters

    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1201, 'be_success') ON CONFLICT (id) DO UPDATE SET data='be_success';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1201;")

    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    echo "$out" | grep -q '^1$'
    ok $? "finite best_effort success returns fresh row"
    [ "$delta_timeout" -eq 0 ]
    ok $? "finite best_effort success does not count timeout"

    # Whole counter-flow vector for one committed best_effort consistency read.
    d_routing=$(counter_delta "PolarDB_Session_LSN_Routing" "$S_ROUTING")
    d_prepared=$(counter_delta "PolarDB_Wait_Wrap_Prepared" "$S_PREPARED")
    d_bypassed=$(counter_delta "PolarDB_Wait_Wrap_Bypassed" "$S_BYPASSED")
    d_sent=$(counter_delta "PolarDB_Wait_LSN_Sent" "$S_SENT")
    d_abort=$(counter_delta "PolarDB_Wait_Wrap_Safety_Abort" "$S_ABORT")
    d_elto=$(counter_delta "PolarDB_Wait_Error_LSN_Wait_Timeout" "$S_ELTO")
    d_rfq_lsn=$(counter_delta "PolarDB_Server_LSN_Updates_From_RFQ" "$S_RFQ_LSN")
    diag "be_success deltas: routing=$d_routing prepared=$d_prepared bypassed=$d_bypassed sent=$d_sent abort=$d_abort eto=$delta_timeout elto=$d_elto rfq_lsn=$d_rfq_lsn"

    [ "$d_routing" -ge 1 ] && [ "$d_routing" -eq "$d_prepared" ]
    ok $? "be_success: Session_LSN_Routing == Wait_Wrap_Prepared (lockstep)"
    [ $((d_sent + d_bypassed)) -eq "$d_prepared" ] && [ "$d_abort" -eq 0 ]
    ok $? "be_success: Wait_LSN_Sent + Wait_Wrap_Bypassed == prepared, no safety abort"
    [ "$d_rfq_lsn" -ge 1 ]
    ok $? "be_success: Server_LSN_Updates_From_RFQ advanced (writer RFQ + replica RFQ; ~2/cycle)"
    [ "$delta_timeout" -eq "$d_elto" ]
    ok $? "be_success: Wait_Error_Timeout == Wait_Error_LSN_Wait_Timeout (lockstep)"

    # Branch 2: finite strict success (target reached, no clear).
    configure_wait_policy strict 5000
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")

    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1202, 'strict_success') ON CONFLICT (id) DO UPDATE SET data='strict_success';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1202;")

    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    echo "$out" | grep -q '^1$'
    ok $? "finite strict success returns fresh row"
    [ "$delta_timeout" -eq 0 ]
    ok $? "finite strict success does not count timeout"

    # Branch 3: finite best_effort timeout commits with exactly one WARNING; the next
    # straight reader query runs with no leftover wait (backend consumed the target).
    configure_wait_policy best_effort 100
    require_replay_lag_enabled 50000 "best_effort timeout setup"
    start_wal_generator 0 0
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    snap_all_counters

    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1203, 'be_timeout') ON CONFLICT (id) DO UPDATE SET data='be_timeout';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1203;")
    stop_wal_generator

    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    warn_count=$(echo "$out" | grep -c "WARNING:  LSN wait timeout")
    diag "be_timeout: client-visible 'LSN wait timeout' WARNING count = $warn_count"

    [ "$warn_count" -ge 1 ]
    ok $? "finite best_effort timeout emits WARNING"
    [ "$warn_count" -eq 1 ]
    ok $? "best_effort timeout WARNING reaches client exactly once (no duplicate forward)"
    [ "$delta_timeout" -eq 1 ]
    ok $? "finite best_effort timeout increments timeout counter exactly once"

    d_routing=$(counter_delta "PolarDB_Session_LSN_Routing" "$S_ROUTING")
    d_prepared=$(counter_delta "PolarDB_Wait_Wrap_Prepared" "$S_PREPARED")
    d_sumus=$(counter_delta "PolarDB_Wait_LSN_Sum_Us" "$S_SUMUS")
    d_elto=$(counter_delta "PolarDB_Wait_Error_LSN_Wait_Timeout" "$S_ELTO")
    diag "be_timeout deltas: routing=$d_routing prepared=$d_prepared sumus=$d_sumus eto=$delta_timeout elto=$d_elto"

    [ "$delta_timeout" -eq "$d_elto" ]
    ok $? "be_timeout: Wait_Error_Timeout == Wait_Error_LSN_Wait_Timeout (lockstep)"
    [ "$d_routing" -ge 1 ] && [ "$d_routing" -eq "$d_prepared" ]
    ok $? "be_timeout: Session_LSN_Routing == Wait_Wrap_Prepared (lockstep)"
    [ "$d_sumus" -gt 0 ]
    ok $? "be_timeout: Wait_LSN_Sum_Us increased (real wait elapsed)"

    # The best_effort timeout WARNING must be emitted before rows even when the
    # result is large enough to stream through threshold_resultset_size.
    old_threshold=$(global_var "pgsql-threshold_resultset_size")
    set_global_var_runtime "pgsql-threshold_resultset_size" "1024"
    configure_wait_policy best_effort 100
    require_replay_lag_enabled 50000 "best_effort streamed timeout setup"
    start_wal_generator 0 0
    out_file="$RUN_DIR/best_effort_stream_notice_order.out"
    proxy_script "INSERT INTO $TEST_TABLE VALUES (1215, 'be_stream_notice') ON CONFLICT (id) DO UPDATE SET data='be_stream_notice';
\\! sleep 0.5
SELECT g FROM generate_series(1, 3000) AS g;" >"$out_file" 2>&1
    stop_wal_generator
    set_global_var_runtime "pgsql-threshold_resultset_size" "${old_threshold:-4194304}"
    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

    stream_warn_count=$(grep -c "WARNING:  LSN wait timeout" "$out_file" || true)
    stream_warn_line=$(grep -n "WARNING:  LSN wait timeout" "$out_file" | head -1 | cut -d: -f1)
    stream_first_row_line=$(grep -n '^1$' "$out_file" | head -1 | cut -d: -f1)
    stream_tail_line=$(grep -n '^3000$' "$out_file" | head -1 | cut -d: -f1)
    [ "$stream_warn_count" -eq 1 ]
    ok $? "best_effort streamed timeout emits WARNING exactly once"
    [ -n "$stream_warn_line" ] && [ -n "$stream_first_row_line" ] &&
        [ "$stream_warn_line" -lt "$stream_first_row_line" ]
    ok $? "best_effort streamed timeout WARNING precedes first row"
    [ -n "$stream_tail_line" ] && [ "$stream_first_row_line" -lt "$stream_tail_line" ]
    ok $? "best_effort streamed timeout returns full large result"

    set_select_rule_manual_reader
    out=$(proxy_script "SELECT 42;")
    set_select_rule_auto

    echo "$out" | grep -q '^42$'
    ok $? "straight reader query after best_effort timeout returns"
    ! echo "$out" | grep -q "LSN wait timeout"
    ok $? "straight read after best_effort timeout has no leftover wait (backend consumed target)"

    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

    # Branch 4: finite strict timeout is retried on the writer before any user
    # result is sent; the following straight read runs cleanly.
    configure_wait_policy strict 100
    require_replay_lag_enabled 50000 "strict timeout setup"
    start_wal_generator 0 0
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    retry_before=$(counter_value "PolarDB_Wait_Reads_Retried_On_Writer")
    retry_trace_pattern="PolarDB WAIT: strict wait timeout before user result; redirecting original unwrapped query"
    retry_trace_before=$(proxy_trace_count "$retry_trace_pattern")

    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1204, 'strict_timeout') ON CONFLICT (id) DO UPDATE SET data='strict_timeout';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1204;")
    stop_wal_generator

    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    delta_retry=$(counter_delta "PolarDB_Wait_Reads_Retried_On_Writer" "$retry_before")
    retry_trace_delta=$(proxy_trace_delta "$retry_trace_pattern" "$retry_trace_before")
    echo "$out" | grep -q '^1$'
    ok $? "finite strict timeout retries on writer and returns result"
    ! echo "$out" | grep -q "ERROR:  LSN wait timeout"
    ok $? "finite strict timeout does not reach client after retry"
    [ "$delta_timeout" -eq 1 ]
    ok $? "finite strict timeout increments timeout counter exactly once"
    [ "$delta_retry" -eq 1 ] && { [ "$retry_trace_delta" -eq 1 ] || [ "$retry_trace_delta" -eq -1 ]; }
    ok $? "finite strict timeout increments writer-retry counter exactly once and traces primary retry"
    [ "$retry_trace_delta" -eq 1 ] || diag "strict retry trace_delta=$retry_trace_delta pattern='$retry_trace_pattern' (-1 means trace unavailable)"

    set_select_rule_manual_reader
    out=$(proxy_script "SELECT 43;")
    set_select_rule_auto

    echo "$out" | grep -q '^43$'
    ok $? "straight reader query after strict timeout returns"

    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

    run_strict_timeout_no_retry_fault \
        result_started \
        "strict timeout with result already started does not retry" \
        1210
    run_strict_timeout_no_retry_fault \
        fallback_unknown \
        "strict timeout with unknown fallback writer does not retry" \
        1211
	    run_strict_timeout_no_retry_fault \
	        writer_busy \
	        "strict timeout with declined primary retry returns clean error without wrapper results" \
	        1212

    # A reader can also disappear after the proxy has begun forwarding a result.
    # The debug marker stands in for the "result already started" capture bit so the
    # test can show the hard safety check without depending on packet timing.
    if set_debug_retry_fault_or_skip result_started \
        "reader connection loss after result started is not retried"; then
        configure_wait_policy strict 0
        require_replay_lag_enabled 50000 "reader connection loss after result-started setup"
        start_wal_generator 0 0
        conn_lost_before=$(counter_value "PolarDB_Wait_Error_Connection_Lost")
        retry_before=$(counter_value "PolarDB_Wait_Reads_Retried_On_Writer")
        timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
        out_file="$RUN_DIR/reader_connection_lost_result_started.out"
        proxy_script "INSERT INTO $TEST_TABLE VALUES (1213, 'reader_connection_lost_after_result_started') ON CONFLICT (id) DO UPDATE SET data='reader_connection_lost_after_result_started';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1213;" >"$out_file" 2>&1 &
        reader_loss_pid=$!
        terminate_active_reader_wait 10
        terminated_rc=$?

        waited=0
        while process_alive_not_zombie "$reader_loss_pid" && [ "$waited" -lt 20 ]; do
            sleep 0.5
            waited=$((waited + 1))
        done
        if process_alive_not_zombie "$reader_loss_pid"; then
            kill "$reader_loss_pid" 2>/dev/null || true
            wait "$reader_loss_pid" 2>/dev/null || true
        else
            wait "$reader_loss_pid" 2>/dev/null || true
        fi
        stop_wal_generator
        out=$(cat "$out_file")
        delta_conn_lost=$(counter_delta "PolarDB_Wait_Error_Connection_Lost" "$conn_lost_before")
        delta_retry=$(counter_delta "PolarDB_Wait_Reads_Retried_On_Writer" "$retry_before")
        delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
        disable_replay_lag >/dev/null 2>&1 || true
        wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true
        if [ "$terminated_rc" -eq 0 ] &&
            [ "$delta_conn_lost" -eq 1 ] &&
            [ "$delta_retry" -eq 0 ] &&
            [ "$delta_timeout" -eq 0 ] &&
            printf '%s\n' "$out" | grep -Eq "ERROR|FATAL|server closed the connection|terminating connection"; then
            ok 0 "reader connection loss after result started is not retried"
        else
            diag "reader-loss-result-started output: $out"
            diag "terminated_rc=$terminated_rc conn_lost_delta=$delta_conn_lost retry_delta=$delta_retry timeout_delta=$delta_timeout"
            ok 1 "reader connection loss after result started is not retried"
        fi
    fi

    # Branch 5: best_effort timeout followed by a strict timeout, then a straight
    # reader query. With the consume-on-wait backend each wait target is consumed per
    # read, so order does not matter and the straight reader query runs cleanly.
    configure_wait_policy best_effort 100
    require_replay_lag_enabled 50000 "best_effort precursor timeout setup"
    start_wal_generator 0 0
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1205, 'be_timeout_before_strict') ON CONFLICT (id) DO UPDATE SET data='be_timeout_before_strict';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1205;")
    stop_wal_generator
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    echo "$out" | grep -q "WARNING:  LSN wait timeout"
    ok $? "best_effort precursor timeout emits WARNING"
    [ "$delta_timeout" -eq 1 ]
    ok $? "best_effort precursor timeout increments timeout counter exactly once"

    configure_wait_policy strict 100
    require_replay_lag_enabled 50000 "strict after best_effort timeout setup"
    start_wal_generator 0 0
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    retry_before=$(counter_value "PolarDB_Wait_Reads_Retried_On_Writer")
    retry_trace_pattern="PolarDB WAIT: strict wait timeout before user result; redirecting original unwrapped query"
    retry_trace_before=$(proxy_trace_count "$retry_trace_pattern")
    out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES (1206, 'strict_after_be_timeout') ON CONFLICT (id) DO UPDATE SET data='strict_after_be_timeout';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = 1206;")
    stop_wal_generator
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    delta_retry=$(counter_delta "PolarDB_Wait_Reads_Retried_On_Writer" "$retry_before")
    retry_trace_delta=$(proxy_trace_delta "$retry_trace_pattern" "$retry_trace_before")
    echo "$out" | grep -q '^1$'
    ok $? "strict timeout after best_effort flag retries on writer"
    ! echo "$out" | grep -q "ERROR:  LSN wait timeout"
    ok $? "strict timeout after best_effort flag does not reach client"
    [ "$delta_timeout" -eq 1 ]
    ok $? "strict timeout after best_effort flag increments timeout counter exactly once"
    [ "$delta_retry" -eq 1 ] && { [ "$retry_trace_delta" -eq 1 ] || [ "$retry_trace_delta" -eq -1 ]; }
    ok $? "strict timeout after best_effort flag increments writer-retry counter exactly once and traces primary retry"
    [ "$retry_trace_delta" -eq 1 ] || diag "strict-after-best-effort retry trace_delta=$retry_trace_delta pattern='$retry_trace_pattern' (-1 means trace unavailable)"

    set_select_rule_manual_reader
    out=$(proxy_script "SELECT 44;")
    set_select_rule_auto
    echo "$out" | grep -q '^44$'
    ok $? "straight reader query after strict abort with prior flag returns"
    disable_replay_lag >/dev/null 2>&1 || true
    wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

    # Branch 6: lost reader connection during a wait-wrapped read, strict timeout 0.
    run_reader_connection_loss_branch \
        strict "" 1209 reader_connection_lost reader_connection_lost

    # Branch 6b: same recovery under best_effort timeout 0 -- the retry is
    # independent of the wait_timeout mode.
    run_reader_connection_loss_branch \
        best_effort "best_effort " 1214 best_effort_reader_connection_lost \
        best_effort_reader_connection_lost

    # Branch 7: timeout 0 in best_effort waits until catchup; no timeout warning,
    # no timeout counter. Hold lag longer than any finite timeout used in this test
    # so the assertion shows timeout 0 reached the backend as "wait indefinitely",
    # without depending on debug query-text traces.
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    sumus_before=$(counter_value "PolarDB_Wait_LSN_Sum_Us")
    out_file="$RUN_DIR/timeout0_best_effort.out"
    run_waiting_query_until_catchup best_effort 1207 "$out_file"
    out=$(cat "$out_file")
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    d_sumus_timeout0=$(counter_delta "PolarDB_Wait_LSN_Sum_Us" "$sumus_before")
    echo "$out" | grep -q '^1$'
    ok $? "timeout 0/best_effort returns fresh row after catchup"
    ! echo "$out" | grep -q "LSN wait timeout"
    ok $? "timeout 0/best_effort emits no timeout warning"
    [ "$delta_timeout" -eq 0 ]
    ok $? "timeout 0/best_effort does not increment timeout counter"
    [ "$d_sumus_timeout0" -ge "$TIMEOUT0_MIN_WAIT_US" ]
    ok $? "timeout 0/best_effort waits beyond finite timeout"

    # Branch 8: timeout 0 in strict behaves the same for the wait itself.
    timeout_before=$(counter_value "PolarDB_Wait_Error_Timeout")
    sumus_before=$(counter_value "PolarDB_Wait_LSN_Sum_Us")
    out_file="$RUN_DIR/timeout0_strict.out"
    run_waiting_query_until_catchup strict 1208 "$out_file"
    out=$(cat "$out_file")
    delta_timeout=$(counter_delta "PolarDB_Wait_Error_Timeout" "$timeout_before")
    d_sumus_timeout0=$(counter_delta "PolarDB_Wait_LSN_Sum_Us" "$sumus_before")
    echo "$out" | grep -q '^1$'
    ok $? "timeout 0/strict returns fresh row after catchup"
    ! echo "$out" | grep -q "LSN wait timeout"
    ok $? "timeout 0/strict emits no timeout error"
    [ "$delta_timeout" -eq 0 ]
    ok $? "timeout 0/strict does not increment timeout counter"
    [ "$d_sumus_timeout0" -ge "$TIMEOUT0_MIN_WAIT_US" ]
    ok $? "timeout 0/strict waits beyond finite timeout"

    diag "timeout cleanup assertions completed: failed=$FAIL"
    [ "$FAIL" -eq 0 ]
}

run_wait_timeout_cleanup_tap
exit $?
