#!/bin/bash
# shellcheck disable=SC1091 # Source paths are built from the script directory.
# shellcheck disable=SC2034 # Harness metadata is consumed by harness.sh.
# Bench 2: LSN offload benchmark.
#
# Compares three autocommit read-after-write modes while the writer has an
# artificial query delay and the reader has controlled replay lag:
#   off     - eligible reads go to the reader without LSN wait; may be stale
#   lsn     - eligible reads go to the reader with polar_xact_split_wait_lsn
#   primary - all reads stay on the writer
#
# The benchmark validates correctness from query results, not only counters:
# every worker writes a unique marker and immediately reads it back on the same
# client session. It also reports writer/reader hostgroup query deltas and wait
# counters so offload behavior is visible.

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/scenario_harness.sh
source "$BENCH_DIR/../lib/scenario_harness.sh"
# shellcheck source=../lib/bench_harness.sh
source "$BENCH_DIR/../lib/bench_harness.sh"

CASE_NUM="bench2"
CASE_NAME="LSN Offload Benchmark"
CONSISTENCY_MODE=session_lsn
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=102

BENCH2_CLIENTS="${BENCH2_CLIENTS:-4}"
BENCH2_ITERS="${BENCH2_ITERS:-10}"
BENCH2_PRIMARY_DELAY_US="${BENCH2_PRIMARY_DELAY_US:-200000}"
BENCH2_REPLAY_LAG_BYTES="${BENCH2_REPLAY_LAG_BYTES:-2000}"
BENCH2_WAIT_TIMEOUT_MS="${BENCH2_WAIT_TIMEOUT_MS:-5000}"
BENCH2_LSN_WAIT_TIMEOUT_ACTION="${BENCH2_LSN_WAIT_TIMEOUT_ACTION:-primary}"
BENCH2_WAL_SLEEP_SEC="${BENCH2_WAL_SLEEP_SEC:-0.01}"
BENCH2_TABLE="${BENCH2_TABLE:-polardb_bench2_lsn_offload}"
BENCH2_LOAD_TABLE="${BENCH2_LOAD_TABLE:-polardb_bench2_load}"
BENCH2_RESULT_TABLE="${BENCH2_RESULT_TABLE:-polardb_bench2_results}"

bench2_modes=()

declare -A B2_ELAPSED_MS B2_TPS B2_FRESH B2_STALE B2_BAD B2_ERRORS
declare -A B2_WRITER_Q B2_READER_Q B2_WAIT_PREPARED B2_WAIT_SENT

bench2_configure_mode() {
    local mode="$1"
    local consistency="$2"
    local read_target="$3"

    polardb_bench_configure_mode "$mode" "$consistency" "$read_target" \
        "$BENCH2_LSN_WAIT_TIMEOUT_ACTION" "$BENCH2_WAIT_TIMEOUT_MS" -1 0
}

bench2_write_worker_script() {
    local mode="$1"
    local worker="$2"
    local sql_file="$3"
    local pre_sql=""
    local post_sql=""

    if [ "$mode" = "primary" ]; then
        pre_sql="SET polar_query_delay_us = $BENCH2_PRIMARY_DELAY_US;"
        post_sql="SET polar_query_delay_us = 0;"
    fi
    POLARDB_BENCH_RESULT_TABLE="$BENCH2_RESULT_TABLE" \
        polardb_bench_write_ryw_worker_script "$mode" "$worker" "$sql_file" \
        "$BENCH2_TABLE" "$BENCH2_ITERS" bench2 "$pre_sql" "$post_sql"
}

bench2_run_mode() {
    local mode="$1"
    local consistency="$2"
    local read_target="$3"
    local expected=$((BENCH2_CLIENTS * BENCH2_ITERS))
    local t0 t1 elapsed wait_prepared_before wait_prepared_after wait_sent_before wait_sent_after
    local writer_before writer_after reader_before reader_after

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$(ts)] MODE: $mode"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    bench2_modes+=("$mode")
    bench2_configure_mode "$mode" "$consistency" "$read_target" || return 1

    polardb_bench_truncate "$BENCH2_TABLE" || return 1
    polardb_bench_truncate "$BENCH2_RESULT_TABLE" || return 1
    sleep 1

    wait_prepared_before=$(polardb_bench_counter PolarDB_Wait_Wrap_Prepared)
    wait_sent_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    writer_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    t0=$(date +%s%3N)
    POLARDB_BENCH_RESULT_TABLE="$BENCH2_RESULT_TABLE" \
        polardb_bench_run_workers "$mode" "$BENCH2_CLIENTS" bench2_write_worker_script polardb_bench_mark_fail
    t1=$(date +%s%3N)
    elapsed=$((t1 - t0))

    wait_prepared_after=$(polardb_bench_counter PolarDB_Wait_Wrap_Prepared)
    wait_sent_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    writer_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    B2_ELAPSED_MS[$mode]="$elapsed"
    B2_TPS[$mode]=$(polardb_bench_tps "$expected" "$elapsed")
    B2_WAIT_PREPARED[$mode]=$((wait_prepared_after - wait_prepared_before))
    B2_WAIT_SENT[$mode]=$((wait_sent_after - wait_sent_before))
    B2_WRITER_Q[$mode]=$((writer_after - writer_before))
    B2_READER_Q[$mode]=$((reader_after - reader_before))

    POLARDB_BENCH_RESULT_TABLE="$BENCH2_RESULT_TABLE" \
        polardb_bench_capture_ryw_results "$mode" "$expected" polardb_bench_mark_fail \
        B2_FRESH B2_STALE B2_BAD B2_ERRORS

    echo "[$(ts)]   elapsed=${elapsed}ms tps=${B2_TPS[$mode]} writer_q=${B2_WRITER_Q[$mode]} reader_q=${B2_READER_Q[$mode]} wait_prepared=${B2_WAIT_PREPARED[$mode]} wait_sent=${B2_WAIT_SENT[$mode]}"
}

bench2_print_summary() {
    local mode

    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  BENCH 2 LSN OFFLOAD RESULTS"
    echo "════════════════════════════════════════════════════════════════"
    printf "%-10s %8s %8s %8s %8s %10s %8s %8s %8s %8s\n" \
        "MODE" "FRESH" "STALE" "BAD" "ERROR" "ELAPSED" "TPS" "WRITER" "READER" "WAITS"
    printf "%-10s %8s %8s %8s %8s %10s %8s %8s %8s %8s\n" \
        "────────" "─────" "─────" "───" "─────" "────────" "───" "──────" "──────" "─────"
    for mode in "${bench2_modes[@]}"; do
        printf "%-10s %8d %8d %8d %8d %8dms %8d %8d %8d %8d\n" \
            "$mode" \
            "${B2_FRESH[$mode]:-0}" \
            "${B2_STALE[$mode]:-0}" \
            "${B2_BAD[$mode]:-0}" \
            "${B2_ERRORS[$mode]:-0}" \
            "${B2_ELAPSED_MS[$mode]:-0}" \
            "${B2_TPS[$mode]:-0}" \
            "${B2_WRITER_Q[$mode]:-0}" \
            "${B2_READER_Q[$mode]:-0}" \
            "${B2_WAIT_SENT[$mode]:-0}"
    done
}

bench2_verify_expectations() {
    if [ "${B2_STALE[lsn]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: lsn mode returned zero stale reads"
    else
        polardb_bench_mark_fail "lsn mode returned stale reads: ${B2_STALE[lsn]}"
    fi

    if [ "${B2_STALE[primary]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: primary mode returned zero stale reads"
    else
        polardb_bench_mark_fail "primary mode returned stale reads: ${B2_STALE[primary]}"
    fi

    if [ "${B2_READER_Q[lsn]:-0}" -gt 0 ] && [ "${B2_WAIT_SENT[lsn]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: lsn mode offloaded reads and sent LSN waits"
    else
        polardb_bench_mark_fail "lsn mode did not show reader offload + wait counters"
    fi

    if [ "${B2_READER_Q[primary]:-0}" -eq 0 ] && [ "${B2_WAIT_SENT[primary]:-0}" -eq 0 ]; then
        echo "[$(ts)]   PASS: primary mode stayed on writer without waits"
    else
        polardb_bench_mark_fail "primary mode unexpectedly used reader/waits: reader=${B2_READER_Q[primary]:-0} waits=${B2_WAIT_SENT[primary]:-0}"
    fi

    if [ "${B2_STALE[off]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: off mode exposed stale reads under forced replay lag"
    else
        echo "[$(ts)]   WARN: off mode did not expose stale reads in this run"
    fi

    if [ "${B2_TPS[lsn]:-0}" -gt "${B2_TPS[primary]:-0}" ]; then
        echo "[$(ts)]   PASS: lsn mode (${B2_TPS[lsn]} tps) faster than primary (${B2_TPS[primary]} tps)"
    else
        polardb_bench_mark_fail "lsn mode (${B2_TPS[lsn]:-0} tps) not faster than primary (${B2_TPS[primary]:-0} tps)"
    fi
}

run_bench2_lsn_offload() {
    local primary_delay replica_delay

    init_logging "bench2_lsn_offload"
    PASS=0
    FAIL=0
    polardb_bench_reset_failures

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 2: $CASE_NAME"
    echo "================================================================"
    echo "[$(ts)] clients=$BENCH2_CLIENTS iters=$BENCH2_ITERS primary_session_delay_us=$BENCH2_PRIMARY_DELAY_US replay_lag_bytes=$BENCH2_REPLAY_LAG_BYTES lsn_wait_timeout_action=$BENCH2_LSN_WAIT_TIMEOUT_ACTION timeout=${BENCH2_WAIT_TIMEOUT_MS}ms"

    primary_delay=$(get_polar_query_delay_us primary)
    replica_delay=$(get_polar_query_delay_us replica)
    if [ -z "$primary_delay" ] || [ -z "$replica_delay" ]; then
        echo "[$(ts)] FATAL: polar_query_delay_us is not available on both backends"
        return 1
    fi
    echo "[$(ts)] Initial polar_query_delay_us: primary=$primary_delay replica=$replica_delay"

    unset PROXYSQL_DEBUG
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi
    setup_reader_routing "$POLARDB_BENCH_READER_HG" 5 || echo "[$(ts)]   WARN: reader warmup did not create an idle pooled connection"

    set_polar_proxy_wait_timeout_ms "$BENCH2_WAIT_TIMEOUT_MS" || return 1

    echo "[$(ts)] Creating benchmark tables"
    polardb_bench_create_ryw_tables "$BENCH2_TABLE" "$BENCH2_LOAD_TABLE" "$BENCH2_RESULT_TABLE" || return 1

    enable_replay_lag "$BENCH2_REPLAY_LAG_BYTES" || return 1
    polardb_bench_start_wal_generator "$BENCH2_LOAD_TABLE" 1000 "$BENCH2_WAL_SLEEP_SEC" "WAL generator for replay-lag movement"
    sleep 2

    snapshot "B"
    snapshot_pool "B"

    bench2_run_mode off off replica || return 1
    bench2_run_mode lsn session_lsn replica || return 1
    bench2_run_mode primary eventual primary || return 1

    snapshot "A"
    snapshot_pool "A"

    bench2_print_summary
    bench2_verify_expectations

    echo ""
    echo "--- Global Stats (stats_pgsql_global) ---"
    print_deltas
    echo ""
    echo "--- Per-Hostgroup Routing (stats_pgsql_connection_pool) ---"
    print_pool_deltas
    print_errors

    echo "[$(ts)] Extracting logs"
    extract_logs

    polardb_bench_stop_wal_generator
    disable_replay_lag 2>/dev/null || true
    stop_proxysql
    polardb_bench_drop_tables "$BENCH2_TABLE" "$BENCH2_LOAD_TABLE" "$BENCH2_RESULT_TABLE"

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 2: Offload Bench Failed=$(polardb_bench_fail_count)"
    echo "================================================================"
    echo "[$(ts)] Run dir: $RUN_DIR"

    if [ "$(polardb_bench_fail_count)" -eq 0 ]; then
        echo "[$(ts)] RESULT: PASS"
        return 0
    fi

    echo "[$(ts)] RESULT: FAIL"
    return 1
}

trap 'polardb_bench_stop_wal_generator; disable_replay_lag 2>/dev/null || true; polardb_bench_drop_tables "$BENCH2_TABLE" "$BENCH2_LOAD_TABLE" "$BENCH2_RESULT_TABLE"; cleanup_all' EXIT
run_test "Bench 2: LSN offload benchmark" run_bench2_lsn_offload
