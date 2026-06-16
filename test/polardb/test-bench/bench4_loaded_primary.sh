#!/bin/bash
# shellcheck disable=SC1091 # Source paths are built from the script directory.
# shellcheck disable=SC2034 # Harness metadata is consumed by harness.sh.
# Bench 4: loaded writer vs LSN offload benchmark.
#
# This reproduces the original production Phase 2 scenario as closely as the
# managed cluster allows:
#   - writer has polar_query_delay_us=50ms
#   - reader has small replay lag
#   - fast WAL generator keeps lag moving so LSN waits should be cheaper than
#     writer reads
#
# Important: the original script used ALTER SYSTEM, but Patroni-managed clusters
# must use config/DCS. polar_query_delay_us is scope-visible, but the server code
# only sleeps for SELECT on the primary (!RecoveryInProgress()), so setting the
# value through DCS still reproduces the writer-delay benchmark semantics.

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/scenario_harness.sh
source "$BENCH_DIR/../lib/scenario_harness.sh"
# shellcheck source=../lib/bench_harness.sh
source "$BENCH_DIR/../lib/bench_harness.sh"

CASE_NUM="bench4"
CASE_NAME="Loaded Primary Benchmark"
CONSISTENCY_MODE=1
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=104

BENCH4_CLIENTS="${BENCH4_CLIENTS:-4}"
BENCH4_ITERS="${BENCH4_ITERS:-10}"
BENCH4_PRIMARY_DELAY_US="${BENCH4_PRIMARY_DELAY_US:-50000}"
BENCH4_REPLAY_LAG_BYTES="${BENCH4_REPLAY_LAG_BYTES:-2000}"
BENCH4_WAIT_TIMEOUT_MS="${BENCH4_WAIT_TIMEOUT_MS:-5000}"
BENCH4_WAIT_MODE="${BENCH4_WAIT_MODE:-strict}"
BENCH4_WAL_SLEEP_SEC="${BENCH4_WAL_SLEEP_SEC:-0.01}"
BENCH4_WAL_BYTES="${BENCH4_WAL_BYTES:-1000}"
BENCH4_TABLE="${BENCH4_TABLE:-polardb_bench4_loaded_primary}"
BENCH4_LOAD_TABLE="${BENCH4_LOAD_TABLE:-polardb_bench4_load}"

bench4_modes=()

declare -A B4_ELAPSED B4_TPS B4_FRESH B4_STALE B4_BAD B4_ERRORS
declare -A B4_WRITER_Q B4_READER_Q B4_WAITS B4_WAIT_US

bench4_configure_mode() {
    local mode="$1"
    local consistency="$2"

    polardb_bench_configure_mode "$mode" "$consistency" "$BENCH4_WAIT_MODE" "$BENCH4_WAIT_TIMEOUT_MS" -1 0
}

bench4_worker_script() {
    local mode="$1"
    local worker="$2"
    local sql_file="$3"

    polardb_bench_write_ryw_worker_script "$mode" "$worker" "$sql_file" \
        "$BENCH4_TABLE" "$BENCH4_ITERS" bench4
}

bench4_run_mode() {
    local mode="$1"
    local consistency="$2"
    local expected=$((BENCH4_CLIENTS * BENCH4_ITERS))
    local t0 t1 elapsed
    local wait_before wait_after wait_us_before wait_us_after writer_before writer_after reader_before reader_after

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$(ts)] MODE: $mode"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    bench4_modes+=("$mode")
    bench4_configure_mode "$mode" "$consistency" || return 1

    polardb_bench_truncate "$BENCH4_TABLE" || return 1
    sleep 1

    wait_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    t0=$(date +%s%3N)
    polardb_bench_run_workers "$mode" "$BENCH4_CLIENTS" bench4_worker_script polardb_bench_mark_fail
    t1=$(date +%s%3N)
    elapsed=$((t1 - t0))

    wait_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    B4_ELAPSED[$mode]="$elapsed"
    B4_TPS[$mode]=$(polardb_bench_tps "$expected" "$elapsed")
    B4_WAITS[$mode]=$((wait_after - wait_before))
    B4_WAIT_US[$mode]=$((wait_us_after - wait_us_before))
    B4_WRITER_Q[$mode]=$((writer_after - writer_before))
    B4_READER_Q[$mode]=$((reader_after - reader_before))

    polardb_bench_capture_ryw_results "$mode" "$expected" polardb_bench_mark_fail \
        B4_FRESH B4_STALE B4_BAD B4_ERRORS
    echo "[$(ts)]   elapsed=${elapsed}ms tps=${B4_TPS[$mode]} writer_q=${B4_WRITER_Q[$mode]} reader_q=${B4_READER_Q[$mode]} waits=${B4_WAITS[$mode]} wait_us=${B4_WAIT_US[$mode]}"
}

bench4_print_summary() {
    local mode avg_wait_us=0
    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  BENCH 4 LOADED PRIMARY RESULTS"
    echo "════════════════════════════════════════════════════════════════"
    printf "%-16s %8s %8s %8s %8s %10s %8s %8s %8s %8s %12s\n" \
        "MODE" "FRESH" "STALE" "BAD" "ERROR" "ELAPSED" "TPS" "WRITER" "READER" "WAITS" "AVG_WAIT_US"
    for mode in "${bench4_modes[@]}"; do
        avg_wait_us=0
        if [ "${B4_WAITS[$mode]:-0}" -gt 0 ]; then
            avg_wait_us=$((B4_WAIT_US[$mode] / B4_WAITS[$mode]))
        fi
        printf "%-16s %8d %8d %8d %8d %8dms %8d %8d %8d %8d %12d\n" \
            "$mode" "${B4_FRESH[$mode]:-0}" "${B4_STALE[$mode]:-0}" \
            "${B4_BAD[$mode]:-0}" "${B4_ERRORS[$mode]:-0}" \
            "${B4_ELAPSED[$mode]:-0}" "${B4_TPS[$mode]:-0}" \
            "${B4_WRITER_Q[$mode]:-0}" "${B4_READER_Q[$mode]:-0}" \
            "${B4_WAITS[$mode]:-0}" "$avg_wait_us"
    done
}

run_bench4_loaded_primary_bench() {
    local primary_delay replica_delay

    init_logging "bench4_loaded_primary"
    PASS=0
    FAIL=0
    polardb_bench_reset_failures

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 4: Loaded primary benchmark"
    echo "================================================================"
    echo "[$(ts)] clients=$BENCH4_CLIENTS iters=$BENCH4_ITERS primary_delay_us=$BENCH4_PRIMARY_DELAY_US replay_lag_bytes=$BENCH4_REPLAY_LAG_BYTES wait_mode=$BENCH4_WAIT_MODE timeout=${BENCH4_WAIT_TIMEOUT_MS}ms"

    unset PROXYSQL_DEBUG
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi
    setup_reader_routing "$POLARDB_BENCH_READER_HG" 5 || echo "[$(ts)]   WARN: reader warmup did not create an idle pooled connection"

    set_polar_proxy_wait_timeout_ms "$BENCH4_WAIT_TIMEOUT_MS" || return 1
    set_polar_query_delay_us "$BENCH4_PRIMARY_DELAY_US" || return 1
    primary_delay=$(get_polar_query_delay_us primary)
    replica_delay=$(get_polar_query_delay_us replica)
    echo "[$(ts)] Actual polar_query_delay_us after DCS: primary=${primary_delay:-?} replica=${replica_delay:-?}"
    if [ "$primary_delay" != "$BENCH4_PRIMARY_DELAY_US" ]; then
        polardb_bench_mark_fail "writer did not apply polar_query_delay_us=$BENCH4_PRIMARY_DELAY_US"
    fi
    if [ "$replica_delay" = "$BENCH4_PRIMARY_DELAY_US" ]; then
        echo "[$(ts)]   NOTE: DCS exposes polar_query_delay_us on the reader too; server applies the sleep only on primary SELECTs"
    fi

    echo "[$(ts)] Creating benchmark tables"
    polardb_bench_create_ryw_tables "$BENCH4_TABLE" "$BENCH4_LOAD_TABLE" || return 1

    enable_replay_lag "$BENCH4_REPLAY_LAG_BYTES" || return 1
    polardb_bench_start_wal_generator "$BENCH4_LOAD_TABLE" "$BENCH4_WAL_BYTES" "$BENCH4_WAL_SLEEP_SEC" "fast WAL generator"
    sleep 2

    snapshot "B"
    snapshot_pool "B"

    bench4_run_mode loaded_primary primary || return 1
    bench4_run_mode loaded_session lsn || return 1
    bench4_run_mode loaded_eventual off || return 1

    snapshot "A"
    snapshot_pool "A"
    bench4_print_summary

    if [ "${B4_STALE[loaded_session]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: loaded_session has zero stale reads"
    else
        polardb_bench_mark_fail "loaded_session stale=${B4_STALE[loaded_session]:-?}"
    fi
    if [ "${B4_STALE[loaded_primary]:-999}" -eq 0 ]; then
        echo "[$(ts)]   PASS: loaded_primary has zero stale reads"
    else
        polardb_bench_mark_fail "loaded_primary stale=${B4_STALE[loaded_primary]:-?}"
    fi
    if [ "${B4_STALE[loaded_eventual]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: loaded_eventual exposed stale reads"
    else
        echo "[$(ts)]   WARN: loaded_eventual did not expose stale reads"
    fi
    if [ "${B4_READER_Q[loaded_session]:-0}" -gt 0 ] && [ "${B4_WAITS[loaded_session]:-0}" -gt 0 ]; then
        echo "[$(ts)]   PASS: loaded_session used reader + waits"
    else
        polardb_bench_mark_fail "loaded_session did not use reader + waits"
    fi
    if [ "${B4_READER_Q[loaded_primary]:-0}" -eq 0 ] && [ "${B4_WAITS[loaded_primary]:-0}" -eq 0 ]; then
        echo "[$(ts)]   PASS: loaded_primary stayed on writer"
    else
        polardb_bench_mark_fail "loaded_primary used reader/waits"
    fi

    if [ "${B4_TPS[loaded_session]:-0}" -gt "${B4_TPS[loaded_primary]:-0}" ]; then
        echo "[$(ts)]   PASS: loaded_session faster than loaded_primary"
    else
        polardb_bench_mark_fail "loaded_session not faster than loaded_primary"
    fi

    echo ""
    echo "--- Global Stats (stats_pgsql_global) ---"
    print_deltas
    echo ""
    echo "--- Per-Hostgroup Routing (stats_pgsql_connection_pool) ---"
    print_pool_deltas
    print_errors
    extract_logs

    polardb_bench_stop_wal_generator
    disable_replay_lag 2>/dev/null || true
    reset_polar_query_delay_us 2>/dev/null || true
    stop_proxysql
    polardb_bench_drop_tables "$BENCH4_TABLE" "$BENCH4_LOAD_TABLE"

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 4: Loaded primary Failed=$(polardb_bench_fail_count)"
    echo "================================================================"
    echo "[$(ts)] Run dir: $RUN_DIR"

    [ "$(polardb_bench_fail_count)" -eq 0 ]
}

trap 'polardb_bench_stop_wal_generator; disable_replay_lag 2>/dev/null || true; reset_polar_query_delay_us 2>/dev/null || true; cleanup_all' EXIT
run_test "Bench 4: loaded primary benchmark" run_bench4_loaded_primary_bench
