#!/bin/bash
# shellcheck disable=SC1091 # Source paths are built from the script directory.
# shellcheck disable=SC2034 # Harness metadata is consumed by harness.sh.
# Bench 5: consistency scenario-shape benchmark.
#
# This project-owned bench validates the scenario families used for performance
# comparisons before those shapes are moved to large real datasets. It can run
# each shape in isolation or as a mixed workload:
#   - session-readonly, session-write-read, session-read-write-read,
#     session-write-many-reads, session-mixed
#   - txn-readonly, txn-readonly-long, txn-write-read, txn-read-write-read,
#     txn-leading-reads-write-read, txn-write-many-reads, txn-locking, txn-mixed
#   - txn-write-read-long, txn-read-write-read-long, txn-write-many-reads-long
#
# Modes:
#   primary - writer-only consistency baseline
#   lsn     - session LSN consistency, split disabled
#   split   - session LSN consistency, transaction split enabled
#   off     - eventual-consistency control, forced reader route

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/scenario_harness.sh
source "$BENCH_DIR/../lib/scenario_harness.sh"
# shellcheck source=../lib/bench_harness.sh
source "$BENCH_DIR/../lib/bench_harness.sh"

CASE_NUM="bench5"
CASE_NAME="Consistency Shape Matrix"
CONSISTENCY_MODE=1
SPLIT_ENABLED=1
XACT_SPLIT=1
TEST_ID=105

BENCH5_CLIENTS="${BENCH5_CLIENTS:-2}"
BENCH5_ITERS="${BENCH5_ITERS:-4}"
BENCH5_PRIMARY_DELAY_US="${BENCH5_PRIMARY_DELAY_US:-50000}"
BENCH5_REPLAY_LAG_BYTES="${BENCH5_REPLAY_LAG_BYTES:-0}"
BENCH5_WAIT_TIMEOUT_MS="${BENCH5_WAIT_TIMEOUT_MS:-5000}"
BENCH5_WAIT_MODE="${BENCH5_WAIT_MODE:-strict}"
BENCH5_WAL_SLEEP_SEC="${BENCH5_WAL_SLEEP_SEC:-0.01}"
BENCH5_WAL_BYTES="${BENCH5_WAL_BYTES:-1000}"
BENCH5_WAL_GENERATOR="${BENCH5_WAL_GENERATOR:-0}"
BENCH5_MAX_LAG_BYTES="${BENCH5_MAX_LAG_BYTES:--1}"
BENCH5_LAZY_WARMUP_SPLIT="${BENCH5_LAZY_WARMUP_SPLIT:-1}"
BENCH5_PROXY_IDENTITY_MODE="${BENCH5_PROXY_IDENTITY_MODE:-proxy}"
BENCH5_TXN_SPLIT_WARMUP_MODE="${BENCH5_TXN_SPLIT_WARMUP_MODE:-default}"
BENCH5_TXN_SPLIT_SELECT_WAIT_MS="${BENCH5_TXN_SPLIT_SELECT_WAIT_MS:-0}"
BENCH5_TXN_LONG_WORK_MS="${BENCH5_TXN_LONG_WORK_MS:-1500}"
BENCH5_SPLIT_WARMUP_WAIT_SEC="${BENCH5_SPLIT_WARMUP_WAIT_SEC:-0}"
BENCH5_TABLE="${BENCH5_TABLE:-polardb_bench5_shapes}"
BENCH5_LOAD_TABLE="${BENCH5_LOAD_TABLE:-polardb_bench5_load}"
BENCH5_RESULT_TABLE="${BENCH5_RESULT_TABLE:-polardb_bench5_results}"
BENCH5_CASES="${BENCH5_CASES:-session-readonly:primary session-readonly:lsn session-write-read:primary session-write-read:lsn session-read-write-read:lsn session-write-many-reads:lsn session-mixed:primary session-mixed:lsn txn-readonly:primary txn-readonly:split txn-readonly-long:primary txn-readonly-long:split txn-write-read:primary txn-write-read:split txn-read-write-read:primary txn-read-write-read:split txn-leading-reads-write-read:primary txn-leading-reads-write-read:split txn-write-many-reads:split txn-locking:split txn-mixed:primary txn-mixed:split}"

bench5_cases=()
BENCH5_CURRENT_SHAPE=""
BENCH5_CURRENT_MODE=""
BENCH5_CURRENT_WARMUP_MODE=""

declare -A B5_ELAPSED B5_TPS B5_FRESH B5_STALE B5_BAD B5_ERRORS
declare -A B5_WRITER_Q B5_READER_Q B5_WAITS B5_WAIT_US
declare -A B5_SPLIT_TOTAL B5_SPLIT_SUCCESS B5_SPLIT_FALLBACK B5_SPLIT_RETRIED B5_SPLIT_LOCKING_REJECT
declare -A B5_WARMUP_REQUESTED B5_WARMUP_CREATED B5_WARMUP_AVG_US

bench5_case_key() {
    printf '%s:%s:%s' "$1" "$2" "$3"
}

bench5_shape_is_transaction() {
    case "$1" in
    txn-*) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_shape_is_split_candidate() {
    case "$1" in
    txn-write-read|txn-read-write-read|txn-leading-reads-write-read|txn-write-many-reads|txn-mixed|txn-write-read-long|txn-read-write-read-long|txn-write-many-reads-long) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_shape_is_txn_wait_candidate() {
    case "$1" in
    txn-readonly|txn-readonly-long|txn-read-write-read|txn-leading-reads-write-read|txn-read-write-read-long) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_shape_is_split_veto() {
    case "$1" in
    txn-locking) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_shape_has_write() {
    case "$1" in
    session-write-read|session-read-write-read|session-write-many-reads|session-mixed|txn-write-read|txn-read-write-read|txn-leading-reads-write-read|txn-write-many-reads|txn-locking|txn-mixed|txn-write-read-long|txn-read-write-read-long|txn-write-many-reads-long) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_shape_is_supported() {
    case "$1" in
    session-readonly|session-write-read|session-read-write-read|session-write-many-reads|session-mixed|txn-readonly|txn-readonly-long|txn-write-read|txn-read-write-read|txn-leading-reads-write-read|txn-write-many-reads|txn-locking|txn-mixed|txn-write-read-long|txn-read-write-read-long|txn-write-many-reads-long) return 0 ;;
    *) return 1 ;;
    esac
}

bench5_configure_case() {
    local shape="$1"
    local mode="$2"
    local consistency txn_split max_lag

    case "$mode" in
    primary)
        consistency="primary"
        txn_split=0
        max_lag=-1
        ;;
    lsn)
        consistency="lsn"
        txn_split=0
        max_lag="$BENCH5_MAX_LAG_BYTES"
        ;;
    split)
        consistency="lsn"
        txn_split=1
        max_lag="$BENCH5_MAX_LAG_BYTES"
        ;;
    off)
        consistency="off"
        txn_split=0
        max_lag=-1
        ;;
    *)
        polardb_bench_mark_fail "unknown bench5 mode: $mode"
        return 1
        ;;
    esac

    if ! bench5_shape_is_supported "$shape"; then
        polardb_bench_mark_fail "unknown bench5 shape: $shape"
        return 1
    fi

    if ! bench5_shape_is_transaction "$shape" && [ "$mode" = "split" ]; then
        echo "[$(ts)]   NOTE: shape=$shape is not a transaction; split mode acts like session LSN with txn_split_enabled=1"
    fi

    polardb_bench_configure_mode "$mode" "$consistency" "$BENCH5_WAIT_MODE" "$BENCH5_WAIT_TIMEOUT_MS" "$max_lag" 1 || return 1
    polardb_bench_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=$txn_split, proxy_protocol='v15' WHERE writer_hostgroup=${POLARDB_BENCH_WRITER_HG}" || return 1
    polardb_bench_admin "UPDATE global_variables SET variable_value='$BENCH5_LAZY_WARMUP_SPLIT' WHERE variable_name='pgsql-polardb_lazy_warmup_split'" || return 1
    polardb_bench_admin "UPDATE global_variables SET variable_value='$BENCH5_PROXY_IDENTITY_MODE' WHERE variable_name='pgsql-polardb_proxy_identity_mode'" || return 1
    polardb_bench_admin "UPDATE global_variables SET variable_value='$BENCH5_WAIT_TIMEOUT_MS' WHERE variable_name='pgsql-connect_timeout_server'" || return 1
    polardb_bench_admin "LOAD PGSQL VARIABLES TO RUNTIME" || return 1
    polardb_bench_admin "LOAD PGSQL SERVERS TO RUNTIME" || return 1
}

bench5_validate_warmup_mode() {
    local warmup_mode="$1"

    case "$warmup_mode" in
    default|off|demand|begin|both) return 0 ;;
    *)
        polardb_bench_mark_fail "unknown transaction split warmup mode: $warmup_mode"
        return 1
        ;;
    esac
}

bench5_validate_proxy_identity_mode() {
    local identity_mode="$1"

    case "$identity_mode" in
    proxy|client) return 0 ;;
    *)
        polardb_bench_mark_fail "unknown proxy identity mode: $identity_mode"
        return 1
        ;;
    esac
}

bench5_parse_case_item() {
    local item="$1"
    local shape_var="$2"
    local mode_var="$3"
    local warmup_var="$4"
    local parsed_shape rest parsed_mode parsed_warmup_mode

    parsed_shape="${item%%:*}"
    rest="${item#*:}"
    if [ -z "$parsed_shape" ] || [ "$rest" = "$item" ]; then
        return 1
    fi

    parsed_mode="${rest%%:*}"
    if [ -z "$parsed_mode" ]; then
        return 1
    fi

    if [ "$parsed_mode" = "$rest" ]; then
        parsed_warmup_mode="$BENCH5_TXN_SPLIT_WARMUP_MODE"
    else
        parsed_warmup_mode="${rest#*:}"
    fi
    if [ -z "$parsed_warmup_mode" ] || [[ "$parsed_warmup_mode" == *:* ]]; then
        return 1
    fi

    printf -v "$shape_var" '%s' "$parsed_shape"
    printf -v "$mode_var" '%s' "$parsed_mode"
    printf -v "$warmup_var" '%s' "$parsed_warmup_mode"
}

bench5_prepare_case_table() {
    polardb_bench_validate_identifier "$BENCH5_TABLE" || return 1
    polardb_bench_primary_sql "TRUNCATE ${BENCH5_TABLE}; INSERT INTO ${BENCH5_TABLE}(id, worker_id, iter, marker) VALUES (0, 0, 0, 'seed') ON CONFLICT (id) DO UPDATE SET marker='seed', updated_at=now();" >/dev/null
}

bench5_worker_script() {
    local _label="$1"
    local worker="$2"
    local sql_file="$3"
    local pre_sql=""

    if [ "$BENCH5_CURRENT_WARMUP_MODE" != "default" ]; then
        pre_sql="SET proxysql.polardb_txn_split_warmup TO '${BENCH5_CURRENT_WARMUP_MODE}';"
    fi

    POLARDB_BENCH_TXN_SPLIT_SELECT_WAIT_MS="$BENCH5_TXN_SPLIT_SELECT_WAIT_MS" \
    POLARDB_BENCH_TXN_LONG_WORK_MS="$BENCH5_TXN_LONG_WORK_MS" \
    POLARDB_BENCH_SPLIT_WARMUP_WAIT_SEC="$BENCH5_SPLIT_WARMUP_WAIT_SEC" \
        POLARDB_BENCH_RESULT_TABLE="$BENCH5_RESULT_TABLE" \
            polardb_bench_write_shape_worker_script "$BENCH5_CURRENT_SHAPE" "$BENCH5_CURRENT_MODE" "$worker" "$sql_file" \
            "$BENCH5_TABLE" "$BENCH5_ITERS" bench5 "$pre_sql"
}

bench5_run_case() {
    local shape="$1"
    local mode="$2"
    local warmup_mode="$3"
    local key expected t0 t1 elapsed
    local wait_before wait_after wait_us_before wait_us_after writer_before writer_after reader_before reader_after
    local split_total_before split_total_after split_success_before split_success_after split_fallback_before split_fallback_after
    local split_retried_before split_retried_after split_locking_before split_locking_after
    local warmup_req_before warmup_req_after warmup_created_before warmup_created_after
    local warmup_sum_before warmup_sum_after warmup_count_before warmup_count_after
    local warmup_count_delta warmup_sum_delta

    key=$(bench5_case_key "$shape" "$mode" "$warmup_mode")
    expected=$((BENCH5_CLIENTS * BENCH5_ITERS))

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$(ts)] CASE: shape=$shape mode=$mode warmup=$warmup_mode"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    bench5_cases+=("$key")
    BENCH5_CURRENT_SHAPE="$shape"
    BENCH5_CURRENT_MODE="$mode"
    BENCH5_CURRENT_WARMUP_MODE="$warmup_mode"
    bench5_validate_warmup_mode "$warmup_mode" || return 1
    bench5_configure_case "$shape" "$mode" || return 1
    bench5_prepare_case_table || return 1
    polardb_bench_truncate "$BENCH5_RESULT_TABLE" || return 1
    sleep 1

    wait_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")
    split_total_before=$(polardb_bench_counter PolarDB_Split_Reads_Total)
    split_success_before=$(polardb_bench_counter PolarDB_Split_Reads_Success)
    split_fallback_before=$(polardb_bench_counter PolarDB_Split_Reads_Fallback)
    split_retried_before=$(polardb_bench_counter PolarDB_Split_Reads_Retried)
    split_locking_before=$(polardb_bench_counter PolarDB_Split_Rejected_For_Update)
    warmup_req_before=$(polardb_bench_counter PolarDB_Split_Warmup_Requested)
    warmup_created_before=$(polardb_bench_counter PolarDB_Split_Warmup_Created)
    warmup_sum_before=$(polardb_bench_counter PolarDB_Split_Warmup_Sum_Us)
    warmup_count_before=$(polardb_bench_counter PolarDB_Split_Warmup_Count)

    t0=$(date +%s%3N)
    POLARDB_BENCH_RESULT_TABLE="$BENCH5_RESULT_TABLE" \
        polardb_bench_run_workers "$key" "$BENCH5_CLIENTS" bench5_worker_script polardb_bench_mark_fail
    t1=$(date +%s%3N)
    elapsed=$((t1 - t0))

    wait_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    wait_us_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sum_Us)
    writer_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")
    split_total_after=$(polardb_bench_counter PolarDB_Split_Reads_Total)
    split_success_after=$(polardb_bench_counter PolarDB_Split_Reads_Success)
    split_fallback_after=$(polardb_bench_counter PolarDB_Split_Reads_Fallback)
    split_retried_after=$(polardb_bench_counter PolarDB_Split_Reads_Retried)
    split_locking_after=$(polardb_bench_counter PolarDB_Split_Rejected_For_Update)
    warmup_req_after=$(polardb_bench_counter PolarDB_Split_Warmup_Requested)
    warmup_created_after=$(polardb_bench_counter PolarDB_Split_Warmup_Created)
    warmup_sum_after=$(polardb_bench_counter PolarDB_Split_Warmup_Sum_Us)
    warmup_count_after=$(polardb_bench_counter PolarDB_Split_Warmup_Count)

    B5_ELAPSED[$key]="$elapsed"
    B5_TPS[$key]=$(polardb_bench_tps "$expected" "$elapsed")
    B5_WAITS[$key]=$((wait_after - wait_before))
    B5_WAIT_US[$key]=$((wait_us_after - wait_us_before))
    B5_WRITER_Q[$key]=$((writer_after - writer_before))
    B5_READER_Q[$key]=$((reader_after - reader_before))
    B5_SPLIT_TOTAL[$key]=$((split_total_after - split_total_before))
    B5_SPLIT_SUCCESS[$key]=$((split_success_after - split_success_before))
    B5_SPLIT_FALLBACK[$key]=$((split_fallback_after - split_fallback_before))
    B5_SPLIT_RETRIED[$key]=$((split_retried_after - split_retried_before))
    B5_SPLIT_LOCKING_REJECT[$key]=$((split_locking_after - split_locking_before))
    B5_WARMUP_REQUESTED[$key]=$((warmup_req_after - warmup_req_before))
    B5_WARMUP_CREATED[$key]=$((warmup_created_after - warmup_created_before))
    warmup_count_delta=$((warmup_count_after - warmup_count_before))
    warmup_sum_delta=$((warmup_sum_after - warmup_sum_before))
    if [ "$warmup_count_delta" -gt 0 ]; then
        B5_WARMUP_AVG_US[$key]=$((warmup_sum_delta / warmup_count_delta))
    else
        B5_WARMUP_AVG_US[$key]=0
    fi

    POLARDB_BENCH_RESULT_TABLE="$BENCH5_RESULT_TABLE" \
        polardb_bench_capture_ryw_results "$mode" "$expected" polardb_bench_mark_fail \
        B5_FRESH B5_STALE B5_BAD B5_ERRORS "$key"

    echo "[$(ts)]   elapsed=${elapsed}ms tps=${B5_TPS[$key]} writer_q=${B5_WRITER_Q[$key]} reader_q=${B5_READER_Q[$key]} waits=${B5_WAITS[$key]} split_total=${B5_SPLIT_TOTAL[$key]} split_success=${B5_SPLIT_SUCCESS[$key]} split_fallback=${B5_SPLIT_FALLBACK[$key]} warmup_created=${B5_WARMUP_CREATED[$key]} warmup_avg_us=${B5_WARMUP_AVG_US[$key]}"
}

bench5_print_summary() {
    local key summary_file="$RUN_DIR/bench5_summary.tsv"

    echo ""
    echo "════════════════════════════════════════════════════════════════════════════════════════════════════════"
    echo "  BENCH 5 CONSISTENCY SHAPE RESULTS"
    echo "════════════════════════════════════════════════════════════════════════════════════════════════════════"
    printf "%-30s %8s %8s %8s %8s %10s %8s %8s %8s %8s %8s %8s %8s %8s %8s %10s\n" \
        "CASE" "FRESH" "STALE" "BAD" "ERROR" "ELAPSED" "TPS" "WRITER" "READER" "WAITS" "SPLIT" "OK" "FALLBK" "WREQ" "WNEW" "WAVG_US"
    printf "case\tfresh\tstale\tbad\terror\telapsed_ms\ttps\twriter_q\treader_q\twaits\tsplit_total\tsplit_success\tsplit_fallback\twarmup_requested\twarmup_created\twarmup_avg_us\n" >"$summary_file"
    for key in "${bench5_cases[@]}"; do
        printf "%-30s %8d %8d %8d %8d %8dms %8d %8d %8d %8d %8d %8d %8d %8d %8d %10d\n" \
            "$key" "${B5_FRESH[$key]:-0}" "${B5_STALE[$key]:-0}" \
            "${B5_BAD[$key]:-0}" "${B5_ERRORS[$key]:-0}" \
            "${B5_ELAPSED[$key]:-0}" "${B5_TPS[$key]:-0}" \
            "${B5_WRITER_Q[$key]:-0}" "${B5_READER_Q[$key]:-0}" \
            "${B5_WAITS[$key]:-0}" "${B5_SPLIT_TOTAL[$key]:-0}" \
            "${B5_SPLIT_SUCCESS[$key]:-0}" "${B5_SPLIT_FALLBACK[$key]:-0}" \
            "${B5_WARMUP_REQUESTED[$key]:-0}" "${B5_WARMUP_CREATED[$key]:-0}" \
            "${B5_WARMUP_AVG_US[$key]:-0}"
        printf "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" \
            "$key" "${B5_FRESH[$key]:-0}" "${B5_STALE[$key]:-0}" \
            "${B5_BAD[$key]:-0}" "${B5_ERRORS[$key]:-0}" \
            "${B5_ELAPSED[$key]:-0}" "${B5_TPS[$key]:-0}" \
            "${B5_WRITER_Q[$key]:-0}" "${B5_READER_Q[$key]:-0}" \
            "${B5_WAITS[$key]:-0}" "${B5_SPLIT_TOTAL[$key]:-0}" \
            "${B5_SPLIT_SUCCESS[$key]:-0}" "${B5_SPLIT_FALLBACK[$key]:-0}" \
            "${B5_WARMUP_REQUESTED[$key]:-0}" "${B5_WARMUP_CREATED[$key]:-0}" \
            "${B5_WARMUP_AVG_US[$key]:-0}" >>"$summary_file"
    done
    echo "[$(ts)]   summary_tsv=$summary_file"
}

bench5_verify_case() {
    local shape="$1"
    local mode="$2"
    local warmup_mode="$3"
    local key

    key=$(bench5_case_key "$shape" "$mode" "$warmup_mode")

    if [ "${B5_BAD[$key]:-0}" -ne 0 ] || [ "${B5_ERRORS[$key]:-0}" -ne 0 ]; then
        polardb_bench_mark_fail "$key returned bad rows or errors"
    fi
    if [ "$mode" != "off" ] && [ "${B5_STALE[$key]:-0}" -ne 0 ]; then
        polardb_bench_mark_fail "$key returned stale rows"
    fi
    if [ "$mode" = "primary" ]; then
        if [ "${B5_READER_Q[$key]:-0}" -ne 0 ] || [ "${B5_WAITS[$key]:-0}" -ne 0 ] || [ "${B5_SPLIT_TOTAL[$key]:-0}" -ne 0 ]; then
            polardb_bench_mark_fail "$key unexpectedly used reader, LSN wait, or split"
        fi
        return 0
    fi
    if [ "$mode" = "lsn" ] && bench5_shape_has_write "$shape"; then
        if [ "${B5_READER_Q[$key]:-0}" -le 0 ] || [ "${B5_WAITS[$key]:-0}" -le 0 ]; then
            polardb_bench_mark_fail "$key did not use reader LSN waits"
        fi
    fi
    if [ "$mode" = "split" ] && bench5_shape_is_split_candidate "$shape"; then
        if [ "$warmup_mode" = "off" ]; then
            if [ "${B5_WARMUP_REQUESTED[$key]:-0}" -ne 0 ]; then
                polardb_bench_mark_fail "$key requested split warmup while session warmup mode is off"
            fi
        fi
    fi
    if [ "$mode" = "split" ] && bench5_shape_is_txn_wait_candidate "$shape"; then
        if [ "${B5_READER_Q[$key]:-0}" -le 0 ] || [ "${B5_WAITS[$key]:-0}" -le 0 ]; then
            if [ "$warmup_mode" = "off" ]; then
                return 0
            fi
            if [ "${B5_WARMUP_REQUESTED[$key]:-0}" -le 0 ]; then
                polardb_bench_mark_fail "$key neither offloaded transaction reads nor requested cold-pool warmup"
            fi
        fi
    fi
    if [ "$mode" = "split" ] && bench5_shape_is_split_veto "$shape"; then
        if [ "${B5_SPLIT_TOTAL[$key]:-0}" -ne 0 ] || [ "${B5_SPLIT_LOCKING_REJECT[$key]:-0}" -le 0 ]; then
            polardb_bench_mark_fail "$key did not keep locking read on writer with a split veto"
        fi
    fi
    if [ "$mode" = "split" ] &&
            { [ "$shape" = "txn-readonly" ] || [ "$shape" = "txn-readonly-long" ]; }; then
        if [ "${B5_SPLIT_TOTAL[$key]:-0}" -ne 0 ]; then
            polardb_bench_mark_fail "$key split a read-only transaction without write evidence"
        fi
    fi
}

bench5_verify_expectations() {
    local item shape mode warmup_mode

    for item in $BENCH5_CASES; do
        if ! bench5_parse_case_item "$item" shape mode warmup_mode; then
            polardb_bench_mark_fail "invalid BENCH5_CASES item: $item"
            continue
        fi
        bench5_verify_case "$shape" "$mode" "$warmup_mode"
    done
}

run_bench5_consistency_shapes() {
    local primary_delay replica_delay item shape mode warmup_mode

    init_logging "bench5_consistency_shapes"
    PASS=0
    FAIL=0
    polardb_bench_reset_failures

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 5: $CASE_NAME"
    echo "================================================================"
    echo "[$(ts)] clients=$BENCH5_CLIENTS iters=$BENCH5_ITERS primary_delay_us=$BENCH5_PRIMARY_DELAY_US replay_lag_bytes=$BENCH5_REPLAY_LAG_BYTES wait_mode=$BENCH5_WAIT_MODE timeout=${BENCH5_WAIT_TIMEOUT_MS}ms wal_generator=$BENCH5_WAL_GENERATOR lazy_warmup_split=$BENCH5_LAZY_WARMUP_SPLIT proxy_identity_mode=$BENCH5_PROXY_IDENTITY_MODE txn_split_warmup_mode=$BENCH5_TXN_SPLIT_WARMUP_MODE txn_long_work_ms=$BENCH5_TXN_LONG_WORK_MS split_select_wait_ms=$BENCH5_TXN_SPLIT_SELECT_WAIT_MS split_warmup_wait_sec=$BENCH5_SPLIT_WARMUP_WAIT_SEC"
    echo "[$(ts)] cases=$BENCH5_CASES"

    bench5_validate_warmup_mode "$BENCH5_TXN_SPLIT_WARMUP_MODE" || return 1
    bench5_validate_proxy_identity_mode "$BENCH5_PROXY_IDENTITY_MODE" || return 1

    unset PROXYSQL_DEBUG
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi
    setup_reader_routing "$POLARDB_BENCH_READER_HG" 5 || echo "[$(ts)]   WARN: reader warmup did not create an idle pooled connection"
    set_polar_proxy_wait_timeout_ms "$BENCH5_WAIT_TIMEOUT_MS" || return 1
    set_polar_query_delay_us "$BENCH5_PRIMARY_DELAY_US" || return 1
    primary_delay=$(get_polar_query_delay_us primary)
    replica_delay=$(get_polar_query_delay_us replica)
    echo "[$(ts)] Actual polar_query_delay_us after DCS: primary=${primary_delay:-?} replica=${replica_delay:-?}"

    echo "[$(ts)] Creating benchmark tables"
    polardb_bench_create_ryw_tables "$BENCH5_TABLE" "$BENCH5_LOAD_TABLE" "$BENCH5_RESULT_TABLE" || return 1
    bench5_prepare_case_table || return 1

    if [ "$BENCH5_REPLAY_LAG_BYTES" -gt 0 ]; then
        enable_replay_lag "$BENCH5_REPLAY_LAG_BYTES" || return 1
    fi
    if [ "$BENCH5_WAL_GENERATOR" = "1" ]; then
        # Optional background WAL load is for deliberate lag/load experiments.
        # Success-path split checks should normally run without this crutch so
        # the backend RFQ 'w' marker and per-session LSN behavior are visible.
        polardb_bench_start_wal_generator "$BENCH5_LOAD_TABLE" "$BENCH5_WAL_BYTES" "$BENCH5_WAL_SLEEP_SEC" "bench5 WAL movement generator"
    fi

    snapshot "B"
    snapshot_pool "B"

    for item in $BENCH5_CASES; do
        if ! bench5_parse_case_item "$item" shape mode warmup_mode; then
            polardb_bench_mark_fail "invalid BENCH5_CASES item: $item"
            continue
        fi
        bench5_run_case "$shape" "$mode" "$warmup_mode" || return 1
    done

    snapshot "A"
    snapshot_pool "A"
    bench5_print_summary
    bench5_verify_expectations

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
    polardb_bench_drop_tables "$BENCH5_TABLE" "$BENCH5_LOAD_TABLE" "$BENCH5_RESULT_TABLE"

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 5: Consistency Shapes Failed=$(polardb_bench_fail_count)"
    echo "================================================================"
    echo "[$(ts)] Run dir: $RUN_DIR"

    [ "$(polardb_bench_fail_count)" -eq 0 ]
}

trap 'polardb_bench_stop_wal_generator; disable_replay_lag 2>/dev/null || true; reset_polar_query_delay_us 2>/dev/null || true; polardb_bench_drop_tables "$BENCH5_TABLE" "$BENCH5_LOAD_TABLE" "$BENCH5_RESULT_TABLE"; cleanup_all' EXIT
run_test "Bench 5: consistency scenario shapes" run_bench5_consistency_shapes
