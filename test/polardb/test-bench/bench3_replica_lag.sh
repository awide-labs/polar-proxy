#!/bin/bash
# shellcheck disable=SC1091 # Source paths are built from the script directory.
# shellcheck disable=SC2034 # Harness metadata is consumed by harness.sh.
# Bench 3: replica lag correctness benchmark.
#
# Modes:
#   eventual       - forced reader route, no wait; stale reads expected
#   session_smart  - LSN mode with a positive max_lag_bytes cap; unsafe reader
#                    lag/freshness forces writer instead of waiting
#   session        - LSN mode without lag cap; reader + LSN wait
#   primary_only   - primary mode; writer only

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/scenario_harness.sh
source "$BENCH_DIR/../lib/scenario_harness.sh"
# shellcheck source=../lib/bench_harness.sh
source "$BENCH_DIR/../lib/bench_harness.sh"

CASE_NUM="bench3"
CASE_NAME="Replica Lag Benchmark"
CONSISTENCY_MODE=1
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=103

BENCH3_CLIENTS="${BENCH3_CLIENTS:-4}"
BENCH3_ITERS="${BENCH3_ITERS:-10}"
BENCH3_REPLAY_LAG_BYTES="${BENCH3_REPLAY_LAG_BYTES:-50000}"
BENCH3_SMART_MAX_LAG_BYTES="${BENCH3_SMART_MAX_LAG_BYTES:-10000}"
BENCH3_WAIT_TIMEOUT_MS="${BENCH3_WAIT_TIMEOUT_MS:-5000}"
BENCH3_WAIT_MODE="${BENCH3_WAIT_MODE:-strict}"
BENCH3_WAL_SLEEP_SEC="${BENCH3_WAL_SLEEP_SEC:-0.02}"
BENCH3_WAL_BYTES="${BENCH3_WAL_BYTES:-200}"
BENCH3_TABLE="${BENCH3_TABLE:-polardb_bench3_replica_lag}"
BENCH3_LOAD_TABLE="${BENCH3_LOAD_TABLE:-polardb_bench3_load}"

bench3_modes=()

declare -A B3_ELAPSED B3_TPS B3_FRESH B3_STALE B3_BAD B3_ERRORS
declare -A B3_WRITER_Q B3_READER_Q B3_WAITS B3_WAIT_US

bench3_configure_mode() {
    local mode="$1"
    local consistency="$2"
    local max_lag_bytes="$3"

    polardb_bench_configure_mode "$mode" "$consistency" "$BENCH3_WAIT_MODE" "$BENCH3_WAIT_TIMEOUT_MS" "$max_lag_bytes" 1
}

bench3_worker_script() {
    local mode="$1"
    local worker="$2"
    local sql_file="$3"

    polardb_bench_write_ryw_worker_script "$mode" "$worker" "$sql_file" \
        "$BENCH3_TABLE" "$BENCH3_ITERS" bench3
}

bench3_run_mode() {
    local mode="$1"
    local consistency="$2"
    local max_lag_bytes="$3"
    local expected=$((BENCH3_CLIENTS * BENCH3_ITERS))
    local t0 t1 elapsed
    local wait_before wait_after wait_us_before wait_us_after writer_before writer_after reader_before reader_after

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$(ts)] MODE: $mode"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    bench3_modes+=("$mode")
    bench3_configure_mode "$mode" "$consistency" "$max_lag_bytes" || return 1

    polardb_bench_truncate "$BENCH3_TABLE" || return 1
    sleep 1

    wait_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    t0=$(date +%s%3N)
    polardb_bench_run_workers "$mode" "$BENCH3_CLIENTS" bench3_worker_script polardb_bench_mark_fail
    t1=$(date +%s%3N)
    elapsed=$((t1 - t0))

    wait_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    B3_ELAPSED[$mode]="$elapsed"
    B3_TPS[$mode]=$(polardb_bench_tps "$expected" "$elapsed")
    B3_WAITS[$mode]=$((wait_after - wait_before))
    B3_WAIT_US[$mode]=$((wait_us_after - wait_us_before))
    B3_WRITER_Q[$mode]=$((writer_after - writer_before))
    B3_READER_Q[$mode]=$((reader_after - reader_before))

    polardb_bench_capture_ryw_results "$mode" "$expected" polardb_bench_mark_fail \
        B3_FRESH B3_STALE B3_BAD B3_ERRORS
    echo "[$(ts)]   elapsed=${elapsed}ms tps=${B3_TPS[$mode]} writer_q=${B3_WRITER_Q[$mode]} reader_q=${B3_READER_Q[$mode]} waits=${B3_WAITS[$mode]} wait_us=${B3_WAIT_US[$mode]}"
}

bench3_print_summary() {
    local mode avg_wait_us
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  BENCH 3 REPLICA LAG RESULTS"
    echo "════════════════════════════════════════════════════════════════"
    printf "%-16s %8s %8s %8s %8s %10s %8s %8s %8s %8s %12s\n" \
        "MODE" "FRESH" "STALE" "BAD" "ERROR" "ELAPSED" "TPS" "WRITER" "READER" "WAITS" "AVG_WAIT_US"
    for mode in "${bench3_modes[@]}"; do
        avg_wait_us=0
        if [ "${B3_WAITS[$mode]:-0}" -gt 0 ]; then
            avg_wait_us=$((B3_WAIT_US[$mode] / B3_WAITS[$mode]))
        fi
        printf "%-16s %8d %8d %8d %8d %8dms %8d %8d %8d %8d %12d\n" \
            "$mode" "${B3_FRESH[$mode]:-0}" "${B3_STALE[$mode]:-0}" \
            "${B3_BAD[$mode]:-0}" "${B3_ERRORS[$mode]:-0}" \
            "${B3_ELAPSED[$mode]:-0}" "${B3_TPS[$mode]:-0}" \
            "${B3_WRITER_Q[$mode]:-0}" "${B3_READER_Q[$mode]:-0}" \
            "${B3_WAITS[$mode]:-0}" "$avg_wait_us"
    done
}

bench3_enable_lag_window() {
    enable_replay_lag "$BENCH3_REPLAY_LAG_BYTES" || return 1
    polardb_bench_start_wal_generator "$BENCH3_LOAD_TABLE" "$BENCH3_WAL_BYTES" "$BENCH3_WAL_SLEEP_SEC" "WAL generator"
    sleep 2
}

bench3_disable_lag_window() {
    polardb_bench_stop_wal_generator
    disable_replay_lag 2>/dev/null || true
    sleep 2
}

run_bench3_replica_lag_bench() {
    init_logging "bench3_replica_lag"
    PASS=0
    FAIL=0
    polardb_bench_reset_failures

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 3: Replica lag benchmark"
    echo "================================================================"
    echo "[$(ts)] clients=$BENCH3_CLIENTS iters=$BENCH3_ITERS replay_lag_bytes=$BENCH3_REPLAY_LAG_BYTES smart_max_lag_bytes=$BENCH3_SMART_MAX_LAG_BYTES wait_mode=$BENCH3_WAIT_MODE timeout=${BENCH3_WAIT_TIMEOUT_MS}ms"

    unset PROXYSQL_DEBUG
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi
    setup_reader_routing "$POLARDB_BENCH_READER_HG" 5 || echo "[$(ts)]   WARN: reader warmup did not create an idle pooled connection"
    set_polar_proxy_wait_timeout_ms "$BENCH3_WAIT_TIMEOUT_MS" || return 1

    echo "[$(ts)] Creating benchmark tables"
    polardb_bench_create_ryw_tables "$BENCH3_TABLE" "$BENCH3_LOAD_TABLE" || return 1

    snapshot "B"
    snapshot_pool "B"

    bench3_enable_lag_window
    bench3_run_mode eventual off -1 || return 1
    bench3_disable_lag_window

    bench3_enable_lag_window
    bench3_run_mode session_smart lsn "$BENCH3_SMART_MAX_LAG_BYTES" || return 1
    bench3_disable_lag_window

    bench3_enable_lag_window
    bench3_run_mode session lsn -1 || return 1
    bench3_disable_lag_window

    bench3_run_mode primary_only primary -1 || return 1

    snapshot "A"
    snapshot_pool "A"
    bench3_print_summary

    if [ "${B3_STALE[eventual]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: eventual exposed stale reads"
    else
        polardb_bench_mark_fail "eventual did not expose stale reads"
    fi
    if [ "${B3_STALE[session_smart]:-999}" -eq 0 ] && [ "${B3_ERRORS[session_smart]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: session_smart has zero stale reads/errors"
    else
        polardb_bench_mark_fail "session_smart stale=${B3_STALE[session_smart]:-?} errors=${B3_ERRORS[session_smart]:-?}"
    fi
    if [ "${B3_STALE[session]:-999}" -eq 0 ] && [ "${B3_ERRORS[session]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: session has zero stale reads/errors"
    else
        polardb_bench_mark_fail "session stale=${B3_STALE[session]:-?} errors=${B3_ERRORS[session]:-?}"
    fi
    if [ "${B3_STALE[primary_only]:-999}" -eq 0 ] && [ "${B3_ERRORS[primary_only]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: primary_only has zero stale reads/errors"
    else
        polardb_bench_mark_fail "primary_only stale=${B3_STALE[primary_only]:-?} errors=${B3_ERRORS[primary_only]:-?}"
    fi
    if [ "${B3_READER_Q[eventual]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: eventual routes to reader"
    else
        polardb_bench_mark_fail "eventual did not route to reader"
    fi
    if [ "${B3_READER_Q[session]:-0}" -gt 0 ] && [ "${B3_WAITS[session]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: session routes to reader with waits"
    else
        polardb_bench_mark_fail "session did not use reader + waits"
    fi
    if [ "${B3_READER_Q[session_smart]:-0}" -eq 0 ] && [ "${B3_WAITS[session_smart]:-0}" -eq 0 ]; then
        echo "[$(ts)]   PASS: session_smart lag cap forced writer without waits"
    else
        polardb_bench_mark_fail "session_smart did not force writer without waits"
    fi

    echo ""
    echo "--- Global Stats (stats_pgsql_global) ---"
    print_deltas
    echo ""
    echo "--- Per-Hostgroup Routing (stats_pgsql_connection_pool) ---"
    print_pool_deltas
    print_errors
    extract_logs

    bench3_disable_lag_window
    stop_proxysql
    polardb_bench_drop_tables "$BENCH3_TABLE" "$BENCH3_LOAD_TABLE"

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 3: Replica lag Failed=$(polardb_bench_fail_count)"
    echo "================================================================"
    echo "[$(ts)] Run dir: $RUN_DIR"

    [ "$(polardb_bench_fail_count)" -eq 0 ]
}

trap 'bench3_disable_lag_window 2>/dev/null || true; cleanup_all' EXIT
run_test "Bench 3: replica lag benchmark" run_bench3_replica_lag_bench
