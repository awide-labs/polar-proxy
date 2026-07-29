#!/bin/bash
# shellcheck disable=SC1091 # Source paths are built from the script directory.
# shellcheck disable=SC2034 # Harness metadata is consumed by harness.sh.
# Bench 1: LSN session-consistency stress.
#
# Runs mixed background load while several long-lived sessions repeatedly do:
#   INSERT unique marker -> SELECT marker, inet_server_port()
#
# The validation is value-based, not only counter-based: each RYW session must
# read back exactly the marker it wrote, and the read must be served by the
# configured reader backend.

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/scenario_harness.sh
source "$BENCH_DIR/../lib/scenario_harness.sh"
# shellcheck source=../lib/bench_harness.sh
source "$BENCH_DIR/../lib/bench_harness.sh"

CASE_NUM="bench1"
CASE_NAME="LSN Stress"
CONSISTENCY_MODE=session_lsn
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=101

BENCH1_RYW_CLIENTS="${BENCH1_RYW_CLIENTS:-4}"
BENCH1_RYW_ITERS="${BENCH1_RYW_ITERS:-50}"
BENCH1_ADHOC_CLIENTS="${BENCH1_ADHOC_CLIENTS:-1}"
BENCH1_TXN_CLIENTS="${BENCH1_TXN_CLIENTS:-1}"
BENCH1_DURATION_SEC="${BENCH1_DURATION_SEC:-10}"
BENCH1_WAIT_TIMEOUT_MS="${BENCH1_WAIT_TIMEOUT_MS:-10000}"
BENCH1_LSN_WAIT_TIMEOUT_ACTION="${BENCH1_LSN_WAIT_TIMEOUT_ACTION:-primary}"
BENCH1_EXPECT_REPLICA="${BENCH1_EXPECT_REPLICA:-1}"
BENCH1_REPLICA_SERVER_PORT="${BENCH1_REPLICA_SERVER_PORT:-}"

BENCH1_RYW_TABLE="${BENCH1_RYW_TABLE:-polardb_bench1_lsn_ryw}"
BENCH1_NOISE_TABLE="${BENCH1_NOISE_TABLE:-polardb_bench1_lsn_noise}"

bench1_noise_pids=()
bench1_ryw_pids=()

bench1_cleanup_background() {
    local pid
    for pid in "${bench1_noise_pids[@]:-}" "${bench1_ryw_pids[@]:-}"; do
        if [ -n "$pid" ]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}

bench1_write_pgbench_scripts() {
    cat >"$RUN_DIR/bench1_adhoc.sql" <<EOF
\set id random(1, 10000)
SELECT count(*) FROM $BENCH1_NOISE_TABLE WHERE id = :id;
SELECT inet_server_port();
EOF

    cat >"$RUN_DIR/bench1_txn.sql" <<EOF
\set id random(1, 10000)
BEGIN;
INSERT INTO $BENCH1_NOISE_TABLE(id, val) VALUES (:id, 1)
  ON CONFLICT (id) DO UPDATE SET val = $BENCH1_NOISE_TABLE.val + 1;
SELECT val FROM $BENCH1_NOISE_TABLE WHERE id = :id;
COMMIT;
EOF
}

bench1_start_pgbench_load() {
    local mode="$1"
    local clients="$2"
    local script="$3"
    local log="$4"

    [ "$clients" -le 0 ] && return 0

    echo "[$(ts)]   Starting $mode load: clients=$clients duration=${BENCH1_DURATION_SEC}s script=$script"
    (
        polardb_bench_pgbench_simple "$BENCH1_DURATION_SEC" "$clients" "$script"
    ) >"$log" 2>&1 &
    bench1_noise_pids+=("$!")
}

bench1_write_ryw_script() {
    local client="$1"
    local sql_file="$2"
    local iter id marker

    {
        echo "\\set QUIET 1"
        for iter in $(seq 1 "$BENCH1_RYW_ITERS"); do
            id=$((client * 1000000 + iter))
            marker="bench1_lsn_${client}_${iter}_$$"
            cat <<EOF
INSERT INTO $BENCH1_RYW_TABLE(id, client_id, iter, marker)
VALUES ($id, $client, $iter, '$marker')
ON CONFLICT (id) DO UPDATE
  SET client_id = EXCLUDED.client_id,
      iter = EXCLUDED.iter,
      marker = EXCLUDED.marker,
      updated_at = now();
SELECT 'RYW' AS tag,
       $client AS client_id,
       $iter AS iter,
       CASE WHEN marker = '$marker' THEN 'ok' ELSE 'bad' END AS status,
       marker,
       inet_server_port() AS backend_port
  FROM $BENCH1_RYW_TABLE
 WHERE id = $id;
EOF
        done
    } >"$sql_file"
}

bench1_start_ryw_workers() {
    local client sql_file log_file
    for client in $(seq 1 "$BENCH1_RYW_CLIENTS"); do
        sql_file="$RUN_DIR/ryw_client_${client}.sql"
        log_file="$RUN_DIR/ryw_client_${client}.out"
        bench1_write_ryw_script "$client" "$sql_file"
        echo "[$(ts)]   Starting RYW validator client=$client iterations=$BENCH1_RYW_ITERS"
        (
            polardb_bench_proxy_psql_file "$sql_file"
        ) >"$log_file" 2>&1 &
        bench1_ryw_pids+=("$!")
    done
}

bench1_wait_for_workers() {
    local pid rc

    for pid in "${bench1_ryw_pids[@]:-}"; do
        wait "$pid"
        rc=$?
        if [ "$rc" -ne 0 ]; then
            polardb_bench_mark_fail "RYW worker pid=$pid exited rc=$rc"
        fi
    done

    for pid in "${bench1_noise_pids[@]:-}"; do
        wait "$pid"
        rc=$?
        if [ "$rc" -ne 0 ]; then
            polardb_bench_mark_fail "background load pid=$pid exited rc=$rc"
        fi
    done
}

bench1_validate_ryw_outputs() {
    local expected_total actual_total bad_count wrong_port_count errors_count
    local combined="$RUN_DIR/ryw_combined.out"

    cat "$RUN_DIR"/ryw_client_*.out >"$combined" 2>/dev/null || true

    expected_total=$((BENCH1_RYW_CLIENTS * BENCH1_RYW_ITERS))
    actual_total=$(grep -c '^RYW|' "$combined" 2>/dev/null || true)
    bad_count=$(awk -F'|' '$1 == "RYW" && $4 != "ok" { c++ } END { print c+0 }' "$combined" 2>/dev/null)
    wrong_port_count=0
    if [ "$BENCH1_EXPECT_REPLICA" = "1" ]; then
        wrong_port_count=$(awk -F'|' -v port="$BENCH1_REPLICA_SERVER_PORT" \
            '$1 == "RYW" && $6 != port { c++ } END { print c+0 }' "$combined" 2>/dev/null)
    fi
    errors_count=$(grep -ciE 'ERROR|FATAL|could not|connection .*failed' "$combined" 2>/dev/null || true)

    echo "[$(ts)]   RYW rows: expected=$expected_total actual=$actual_total bad_values=$bad_count wrong_reader_port=$wrong_port_count errors=$errors_count"
    if [ "$actual_total" -ne "$expected_total" ]; then
        polardb_bench_mark_fail "RYW result row count mismatch expected=$expected_total actual=$actual_total"
    fi
    if [ "$bad_count" -ne 0 ]; then
        polardb_bench_mark_fail "RYW value mismatches found: $bad_count"
        awk -F'|' '$1 == "RYW" && $4 != "ok" { print }' "$combined" | head -20 | sed 's/^/    /'
    fi
    if [ "$wrong_port_count" -ne 0 ]; then
        polardb_bench_mark_fail "RYW reads not served by replica backend port $BENCH1_REPLICA_SERVER_PORT: $wrong_port_count"
        awk -F'|' -v port="$BENCH1_REPLICA_SERVER_PORT" '$1 == "RYW" && $6 != port { print }' "$combined" | head -20 | sed 's/^/    /'
    fi
    if [ "$errors_count" -ne 0 ]; then
        polardb_bench_mark_fail "RYW worker output contains errors: $errors_count"
        grep -niE 'ERROR|FATAL|could not|connection .*failed' "$combined" | head -20 | sed 's/^/    /'
    fi
}

run_bench1_lsn_stress() {
    local wait_prepared_before wait_prepared_after wait_sent_before wait_sent_after
    local lsn_before lsn_after writer_q_before writer_q_after reader_q_before reader_q_after
    local expected_total

    init_logging "bench1_lsn_stress"
    PASS=0
    FAIL=0
    polardb_bench_reset_failures

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 1: $CASE_NAME"
    echo "================================================================"
    echo "[$(ts)] clients: ryw=$BENCH1_RYW_CLIENTS iters=$BENCH1_RYW_ITERS adhoc=$BENCH1_ADHOC_CLIENTS txn=$BENCH1_TXN_CLIENTS duration=${BENCH1_DURATION_SEC}s"
    echo "[$(ts)] mode=session_lsn lsn_wait_timeout_action=$BENCH1_LSN_WAIT_TIMEOUT_ACTION timeout=${BENCH1_WAIT_TIMEOUT_MS}ms expect_replica=$BENCH1_EXPECT_REPLICA"

    export PROXYSQL_DEBUG=1
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi

    echo "[$(ts)] Configuring LSN consistency policy"
    polardb_bench_configure_mode session_lsn session_lsn replica \
        "$BENCH1_LSN_WAIT_TIMEOUT_ACTION" "$BENCH1_WAIT_TIMEOUT_MS" -1 0 || return 1
    setup_reader_routing "$POLARDB_BENCH_READER_HG" 5 || polardb_bench_mark_fail "reader routing warmup failed"

    if [ -z "$BENCH1_REPLICA_SERVER_PORT" ]; then
        BENCH1_REPLICA_SERVER_PORT=$(polardb_bench_backend_reported_port "$REPLICA_HOST" "$REPLICA_PORT")
    fi
    if [ -z "$BENCH1_REPLICA_SERVER_PORT" ]; then
        polardb_bench_mark_fail "could not detect replica backend-reported port"
    else
        echo "[$(ts)] Replica backend-reported port: $BENCH1_REPLICA_SERVER_PORT (host port: $REPLICA_PORT)"
    fi

    polardb_require_pgbench || return 1

    echo "[$(ts)] Creating benchmark tables"
    query_proxy "DROP TABLE IF EXISTS $BENCH1_RYW_TABLE; DROP TABLE IF EXISTS $BENCH1_NOISE_TABLE; CREATE TABLE $BENCH1_RYW_TABLE(id int PRIMARY KEY, client_id int, iter int, marker text, updated_at timestamp default now()); CREATE TABLE $BENCH1_NOISE_TABLE(id int PRIMARY KEY, val int, updated_at timestamp default now());"

    bench1_write_pgbench_scripts

    snapshot "B"
    snapshot_pool "B"
    wait_prepared_before=$(polardb_bench_counter PolarDB_Wait_Wrap_Prepared)
    wait_sent_before=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    lsn_before=$(polardb_bench_counter PolarDB_Server_LSN_Updates_From_RFQ)
    writer_q_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_q_before=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")

    echo "[$(ts)] Starting background load and RYW validators"
    bench1_start_pgbench_load "adhoc-read" "$BENCH1_ADHOC_CLIENTS" "$RUN_DIR/bench1_adhoc.sql" "$RUN_DIR/bench1_adhoc_pgbench.out"
    bench1_start_pgbench_load "explicit-txn" "$BENCH1_TXN_CLIENTS" "$RUN_DIR/bench1_txn.sql" "$RUN_DIR/bench1_txn_pgbench.out"
    bench1_start_ryw_workers

    bench1_wait_for_workers

    wait_prepared_after=$(polardb_bench_counter PolarDB_Wait_Wrap_Prepared)
    wait_sent_after=$(polardb_bench_counter PolarDB_Wait_LSN_Sent)
    lsn_after=$(polardb_bench_counter PolarDB_Server_LSN_Updates_From_RFQ)
    writer_q_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_WRITER_HG")
    reader_q_after=$(polardb_bench_pool_queries "$POLARDB_BENCH_READER_HG")
    snapshot "A"
    snapshot_pool "A"

    echo "[$(ts)] Validating RYW outputs"
    bench1_validate_ryw_outputs

    expected_total=$((BENCH1_RYW_CLIENTS * BENCH1_RYW_ITERS))
    echo "[$(ts)] Counter deltas: wait_prepared=$((wait_prepared_after - wait_prepared_before)) wait_sent=$((wait_sent_after - wait_sent_before)) lsn_updates=$((lsn_after - lsn_before)) writer_queries=$((writer_q_after - writer_q_before)) reader_queries=$((reader_q_after - reader_q_before))"

    if [ $((wait_prepared_after - wait_prepared_before)) -lt "$expected_total" ]; then
        polardb_bench_mark_fail "wait-wrap prepared delta below RYW operations"
    fi
    if [ $((wait_sent_after - wait_sent_before)) -lt "$expected_total" ]; then
        polardb_bench_mark_fail "wait-wrap sent delta below RYW operations"
    fi
    if [ $((lsn_after - lsn_before)) -lt "$expected_total" ]; then
        polardb_bench_mark_fail "query LSN update delta below RYW writes"
    fi
    if [ $((reader_q_after - reader_q_before)) -lt "$expected_total" ]; then
        polardb_bench_mark_fail "reader hostgroup query delta below RYW reads"
    fi
    if [ $((writer_q_after - writer_q_before)) -le 0 ]; then
        polardb_bench_mark_fail "writer hostgroup did not receive background/write traffic"
    fi

    echo ""
    echo "--- Global Stats (stats_pgsql_global) ---"
    print_deltas
    echo ""
    echo "--- Per-Hostgroup Routing (stats_pgsql_connection_pool) ---"
    print_pool_deltas
    print_errors

    echo "[$(ts)] Extracting logs"
    extract_logs
    stop_proxysql
    polardb_bench_drop_tables "$BENCH1_RYW_TABLE" "$BENCH1_NOISE_TABLE"

    echo ""
    echo "================================================================"
    echo "[$(ts)] BENCH 1: LSN Stress Failed=$(polardb_bench_fail_count)"
    echo "================================================================"
    echo "[$(ts)] Run dir: $RUN_DIR"

    if [ "$(polardb_bench_fail_count)" -eq 0 ]; then
        echo "[$(ts)] RESULT: PASS"
        return 0
    fi

    echo "[$(ts)] RESULT: FAIL"
    return 1
}

trap 'bench1_cleanup_background; polardb_bench_drop_tables "$BENCH1_RYW_TABLE" "$BENCH1_NOISE_TABLE"; cleanup_all' EXIT
run_test "Bench 1: LSN stress" run_bench1_lsn_stress
