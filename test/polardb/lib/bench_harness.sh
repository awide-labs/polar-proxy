#!/usr/bin/env bash
# Benchmark-only helpers for PolarDB scripts.
#
# These helpers are intentionally small and rely on the committed suite harness:
#   common/env.sh      environment and bundled pgbench path
#   lib/scenario_harness.sh  shared ProxySQL/admin/cluster/logging helpers

if [ -n "${POLARDB_BENCHMARK_LIB_LOADED:-}" ]; then
    return 0
fi
POLARDB_BENCHMARK_LIB_LOADED=1

POLARDB_LIBPQ_PATH="${POLARDB_LIBPQ_PATH:-$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq}"
export POLARDB_LIBPQ_PATH

POLARDB_BENCH_WRITER_HG="${POLARDB_BENCH_WRITER_HG:-$POLARDB_WRITER_HG}"
POLARDB_BENCH_READER_HG="${POLARDB_BENCH_READER_HG:-$POLARDB_READER_HG}"
POLARDB_BENCH_RULE_ID="${POLARDB_BENCH_RULE_ID:-$POLARDB_SELECT_RULE_ID}"
POLARDB_BENCH_FAIL_COUNT=0

polardb_bench_reset_failures() {
    POLARDB_BENCH_FAIL_COUNT=0
}

polardb_bench_mark_fail() {
    echo "[$(ts)]   FAIL: $*"
    POLARDB_BENCH_FAIL_COUNT=$((POLARDB_BENCH_FAIL_COUNT + 1))
}

polardb_bench_fail_count() {
    echo "$POLARDB_BENCH_FAIL_COUNT"
}

polardb_bench_validate_identifier() {
    local ident="$1"
    case "$ident" in
    "" | *[!A-Za-z0-9_]*)
        echo "[$(ts)]   FAIL: unsafe SQL identifier for benchmark helper: $ident" >&2
        return 1
        ;;
    esac
}

polardb_bench_admin() {
    local sql="$1"
    if ! proxysql_admin "$sql" >/dev/null; then
        echo "[$(ts)]   FAIL: ProxySQL admin command failed: $sql" >&2
        return 1
    fi
}

polardb_bench_counter() {
    local name="$1"
    local v
    v=$(proxysql_admin "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$name';" 2>/dev/null |
        tr -d '[:space:]')
    echo "${v:-0}"
}

polardb_bench_pool_queries() {
    local hg="$1"
    local v
    v=$(proxysql_admin "SELECT COALESCE(SUM(Queries),0) FROM stats_pgsql_connection_pool WHERE hostgroup=$hg;" 2>/dev/null |
        tr -d '[:space:]')
    echo "${v:-0}"
}

polardb_bench_primary_sql() {
    polardb_primary_sql "$1"
}

polardb_bench_drop_tables() {
    local sql=""
    local table

    [ "$#" -eq 0 ] && return 0
    for table in "$@"; do
        if ! polardb_bench_validate_identifier "$table"; then
            continue
        fi
        sql="${sql}DROP TABLE IF EXISTS ${table}; "
    done
    [ -z "$sql" ] && return 0
    polardb_bench_primary_sql "$sql" >/dev/null 2>&1 || true
}

polardb_bench_create_ryw_tables() {
    local main_table="$1"
    local load_table="$2"
    local result_table="${3:-}"
    local sql

    polardb_bench_validate_identifier "$main_table" || return 1
    polardb_bench_validate_identifier "$load_table" || return 1
    sql="DROP TABLE IF EXISTS ${main_table}; DROP TABLE IF EXISTS ${load_table}; CREATE TABLE ${main_table}(id int PRIMARY KEY, worker_id int, iter int, marker text, updated_at timestamp default now()); CREATE TABLE ${load_table}(id serial PRIMARY KEY, data text);"
    if [ -n "$result_table" ]; then
        polardb_bench_validate_identifier "$result_table" || return 1
        sql="${sql} DROP TABLE IF EXISTS ${result_table}; CREATE TABLE ${result_table}(tag text, mode text, worker_id int, iter int, status text, observed_marker text, backend_port text, created_at timestamp default now());"
    fi
    polardb_bench_primary_sql "$sql" >/dev/null
}

polardb_bench_truncate() {
    local table="$1"

    polardb_bench_validate_identifier "$table" || return 1
    polardb_bench_primary_sql "TRUNCATE ${table};" >/dev/null
}

polardb_bench_tps() {
    local rows="$1"
    local elapsed_ms="$2"

    if [ "$elapsed_ms" -gt 0 ]; then
        echo $((rows * 1000 / elapsed_ms))
    else
        echo 0
    fi
}

polardb_bench_pgbench_sleep_ms() {
    local ms="$1"

    printf '\\sleep %d ms\n' "$ms"
}

polardb_bench_emit_result_insert_from_vars() {
    local mode="$1"
    local worker="$2"
    local iter="$3"
    local result_table="${POLARDB_BENCH_RESULT_TABLE:-}"

    [ -z "$result_table" ] && return 0
    cat <<EOF
INSERT INTO $result_table(tag, mode, worker_id, iter, status, observed_marker, backend_port)
VALUES ('RYW', '$mode', $worker, $iter, :bench_status, :bench_marker, :bench_port);
EOF
}

polardb_bench_emit_marker_check_iter() {
    local mode="$1"
    local worker="$2"
    local iter="$3"
    local table="$4"
    local id="$5"
    local marker="$6"
    local lock_clause="${7:-}"
    local defer_result="${8:-0}"
    local result_table="${POLARDB_BENCH_RESULT_TABLE:-}"

    if [ -n "$result_table" ]; then
        cat <<EOF
SELECT quote_literal(CASE
         WHEN t.marker = '$marker' THEN 'fresh'
         WHEN t.marker IS NULL THEN 'stale'
         ELSE 'bad'
       END) AS bench_status,
       quote_literal(COALESCE(t.marker, '')) AS bench_marker,
       quote_literal(p.backend_port) AS bench_port
  FROM (SELECT inet_server_port()::text AS backend_port) p
  LEFT JOIN (SELECT marker FROM $table WHERE id = $id${lock_clause}) t ON true \gset
EOF
        if [ "$defer_result" != "1" ]; then
            polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        fi
        return 0
    fi

    cat <<EOF
SELECT 'RYW' AS tag,
       '$mode' AS mode,
       $worker AS worker_id,
       $iter AS iter,
       CASE
         WHEN t.marker = '$marker' THEN 'fresh'
         WHEN t.marker IS NULL THEN 'stale'
         ELSE 'bad'
       END AS status,
       COALESCE(t.marker, '') AS observed_marker,
       p.backend_port
  FROM (SELECT inet_server_port()::text AS backend_port) p
  LEFT JOIN (SELECT marker FROM $table WHERE id = $id${lock_clause}) t ON true;
EOF
}

polardb_bench_write_ryw_worker_script() {
    local mode="$1"
    local worker="$2"
    local sql_file="$3"
    local table="$4"
    local iters="$5"
    local marker_prefix="$6"
    local pre_sql="${7:-}"
    local post_sql="${8:-}"
    local iter id marker

    polardb_bench_validate_identifier "$table" || return 1
    {
        echo "\set QUIET 1"
        if [ -n "$pre_sql" ]; then
            echo "$pre_sql"
        fi
        for iter in $(seq 1 "$iters"); do
            id=$((worker * 1000000 + iter))
            marker="${marker_prefix}_${mode}_${worker}_${iter}_$$"
            cat <<EOF
INSERT INTO $table(id, worker_id, iter, marker)
VALUES ($id, $worker, $iter, '$marker')
ON CONFLICT (id) DO UPDATE
  SET worker_id = EXCLUDED.worker_id,
      iter = EXCLUDED.iter,
      marker = EXCLUDED.marker,
      updated_at = now();
EOF
            polardb_bench_emit_marker_check_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker"
        done
        if [ -n "$post_sql" ]; then
            echo "$post_sql"
        fi
    } >"$sql_file"
}

polardb_bench_emit_readonly_iter() {
    local mode="$1"
    local worker="$2"
    local iter="$3"
    local table="$4"
    local defer_result="${5:-0}"
    local result_table="${POLARDB_BENCH_RESULT_TABLE:-}"

    if [ -n "$result_table" ]; then
        cat <<EOF
SELECT quote_literal('fresh') AS bench_status,
       quote_literal(COALESCE(marker, '')) AS bench_marker,
       quote_literal(inet_server_port()::text) AS bench_port
  FROM $table
 WHERE id = 0 \gset
EOF
        if [ "$defer_result" != "1" ]; then
            polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        fi
        return 0
    fi
    cat <<EOF
SELECT 'RYW' AS tag,
       '$mode' AS mode,
       $worker AS worker_id,
       $iter AS iter,
       'fresh' AS status,
       marker AS observed_marker,
       inet_server_port()::text AS backend_port
  FROM $table
 WHERE id = 0;
EOF
}

polardb_bench_emit_write_read_iter() {
    local mode="$1"
    local worker="$2"
    local iter="$3"
    local table="$4"
    local id="$5"
    local marker="$6"
    local mid_sql="${7:-}"
    local defer_result="${8:-0}"

    cat <<EOF
INSERT INTO $table(id, worker_id, iter, marker)
VALUES ($id, $worker, $iter, '$marker')
ON CONFLICT (id) DO UPDATE
  SET worker_id = EXCLUDED.worker_id,
      iter = EXCLUDED.iter,
      marker = EXCLUDED.marker,
      updated_at = now();
EOF
    if [ -n "$mid_sql" ]; then
        printf '%s\n' "$mid_sql"
    fi
    polardb_bench_emit_marker_check_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "" "$defer_result"
}

polardb_bench_emit_shape_iter() {
    local shape="$1"
    local mode="$2"
    local worker="$3"
    local iter="$4"
    local table="$5"
    local id="$6"
    local marker="$7"
    local choice mid_sql=""

    # Split reads use transaction RFQ evidence from the write. A small pause
    # mirrors the committed TAP path and lets WAL state settle before the read.
    if [ "$mode" = "split" ]; then
        case "$shape" in
        txn-write-read|txn-read-write-read|txn-leading-reads-write-read|txn-write-many-reads|txn-locking|txn-mixed)
            if [ "${POLARDB_BENCH_TXN_SPLIT_SELECT_WAIT_MS:-0}" != "0" ]; then
                mid_sql="$(polardb_bench_pgbench_sleep_ms "${POLARDB_BENCH_TXN_SPLIT_SELECT_WAIT_MS:-0}")"
            fi
            ;;
        esac
    fi

    case "$shape" in
    session-readonly)
        polardb_bench_emit_readonly_iter "$mode" "$worker" "$iter" "$table"
        ;;
    session-write-read)
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker"
        ;;
    session-read-write-read)
        cat <<EOF
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker"
        ;;
    session-write-many-reads)
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker"
        cat <<EOF
SELECT marker FROM $table WHERE id = $id;
SELECT marker FROM $table WHERE id = $id;
EOF
        ;;
    txn-readonly)
        cat <<EOF
BEGIN;
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_emit_readonly_iter "$mode" "$worker" "$iter" "$table" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-readonly-long)
        cat <<EOF
BEGIN;
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_pgbench_sleep_ms "${POLARDB_BENCH_TXN_LONG_WORK_MS:-1500}"
        polardb_bench_emit_readonly_iter "$mode" "$worker" "$iter" "$table" 1
        echo "SELECT marker FROM $table WHERE id = 0;"
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-write-read)
        echo "BEGIN;"
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-write-read-long)
        echo "BEGIN;"
        polardb_bench_pgbench_sleep_ms "${POLARDB_BENCH_TXN_LONG_WORK_MS:-1500}"
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-read-write-read)
        cat <<EOF
BEGIN;
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-leading-reads-write-read)
        cat <<EOF
BEGIN;
SELECT marker FROM $table WHERE id = 0;
SELECT marker FROM $table WHERE id = 0;
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-read-write-read-long)
        cat <<EOF
BEGIN;
SELECT marker FROM $table WHERE id = 0;
EOF
        polardb_bench_pgbench_sleep_ms "${POLARDB_BENCH_TXN_LONG_WORK_MS:-1500}"
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-write-many-reads)
        echo "BEGIN;"
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        cat <<EOF
SELECT marker FROM $table WHERE id = $id;
SELECT marker FROM $table WHERE id = $id;
COMMIT;
EOF
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-write-many-reads-long)
        echo "BEGIN;"
        polardb_bench_pgbench_sleep_ms "${POLARDB_BENCH_TXN_LONG_WORK_MS:-1500}"
        polardb_bench_emit_write_read_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" "$mid_sql" 1
        cat <<EOF
SELECT marker FROM $table WHERE id = $id;
SELECT marker FROM $table WHERE id = $id;
COMMIT;
EOF
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    txn-locking)
        cat <<EOF
BEGIN;
INSERT INTO $table(id, worker_id, iter, marker)
VALUES ($id, $worker, $iter, '$marker')
ON CONFLICT (id) DO UPDATE
  SET worker_id = EXCLUDED.worker_id,
      iter = EXCLUDED.iter,
      marker = EXCLUDED.marker,
      updated_at = now();
EOF
        if [ -n "$mid_sql" ]; then
            printf '%s\n' "$mid_sql"
        fi
        polardb_bench_emit_marker_check_iter "$mode" "$worker" "$iter" "$table" "$id" "$marker" " FOR UPDATE" 1
        echo "COMMIT;"
        polardb_bench_emit_result_insert_from_vars "$mode" "$worker" "$iter"
        ;;
    session-mixed)
        choice=$((iter % 3))
        case "$choice" in
        0) polardb_bench_emit_shape_iter session-readonly "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        1) polardb_bench_emit_shape_iter session-write-read "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        *) polardb_bench_emit_shape_iter session-read-write-read "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        esac
        ;;
    txn-mixed)
        choice=$((iter % 4))
        case "$choice" in
        0) polardb_bench_emit_shape_iter txn-readonly "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        1) polardb_bench_emit_shape_iter txn-write-read "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        2) polardb_bench_emit_shape_iter txn-read-write-read "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        *) polardb_bench_emit_shape_iter txn-locking "$mode" "$worker" "$iter" "$table" "$id" "$marker" ;;
        esac
        ;;
    *)
        echo "SELECT 'RYW' AS tag, '$mode' AS mode, $worker AS worker_id, $iter AS iter, 'bad' AS status, 'unknown shape: $shape' AS observed_marker, '' AS backend_port;"
        return 1
        ;;
    esac
}

polardb_bench_write_shape_worker_script() {
    local shape="$1"
    local mode="$2"
    local worker="$3"
    local sql_file="$4"
    local table="$5"
    local iters="$6"
    local marker_prefix="$7"
    local pre_sql="${8:-}"
    local post_sql="${9:-}"
    local iter id marker

    polardb_bench_validate_identifier "$table" || return 1
    {
        echo "\set QUIET 1"
        if [ -n "$pre_sql" ]; then
            echo "$pre_sql"
        fi
        for iter in $(seq 1 "$iters"); do
            id=$((worker * 1000000 + iter))
            marker="${marker_prefix}_${shape}_${mode}_${worker}_${iter}_$$"
            polardb_bench_emit_shape_iter "$shape" "$mode" "$worker" "$iter" "$table" "$id" "$marker"
            if [ "$mode" = "split" ] && [ "$iter" -eq 1 ] && [ "${POLARDB_BENCH_SPLIT_WARMUP_WAIT_SEC:-0}" != "0" ]; then
                case "$shape" in
                txn-readonly|txn-readonly-long|txn-write-read|txn-read-write-read|txn-leading-reads-write-read|txn-write-many-reads|txn-mixed)
                    echo "\\sleep ${POLARDB_BENCH_SPLIT_WARMUP_WAIT_SEC} s"
                    ;;
                esac
            fi
        done
        if [ -n "$post_sql" ]; then
            echo "$post_sql"
        fi
    } >"$sql_file"
}

polardb_bench_configure_mode() {
    local label="$1"
    local consistency="$2"
    local wait_mode="$3"
    local timeout_ms="$4"
    local max_lag_bytes="$5"
    local reset_lag_globals="${6:-0}"
    local rule_sql

    echo "[$(ts)] Configuring ${label}: consistency=${consistency} wait_mode=${wait_mode} timeout=${timeout_ms}ms max_lag_bytes=${max_lag_bytes}"
    polardb_bench_admin "UPDATE global_variables SET variable_value='${consistency}' WHERE variable_name='pgsql-polardb_consistency_mode'" || return 1
    polardb_bench_admin "UPDATE global_variables SET variable_value='${wait_mode}' WHERE variable_name='pgsql-polardb_wait_timeout_mode'" || return 1
    if [ "$reset_lag_globals" = "1" ]; then
        polardb_bench_admin "UPDATE global_variables SET variable_value='0' WHERE variable_name='pgsql-polardb_lag_bytes'" || return 1
        polardb_bench_admin "UPDATE global_variables SET variable_value='0' WHERE variable_name='pgsql-polardb_lag_ms'" || return 1
    fi
    polardb_bench_admin "LOAD PGSQL VARIABLES TO RUNTIME" || return 1
    polardb_bench_admin "UPDATE pgsql_replication_hostgroups SET consistency_mode='${consistency}', max_lag_bytes=${max_lag_bytes}, lsn_wait_timeout_ms=${timeout_ms} WHERE writer_hostgroup=${POLARDB_BENCH_WRITER_HG}" || return 1
    polardb_bench_admin "LOAD PGSQL SERVERS TO RUNTIME" || return 1

    # The eventual-consistency/off baseline must explicitly route reads to the
    # reader. All other modes use replica_eligible=1 so the PolarDB planner can
    # choose writer, reader+wait, or writer fallback according to policy.
    polardb_bench_admin "DELETE FROM pgsql_query_rules" || return 1
    if [ "$consistency" = "off" ]; then
        rule_sql="INSERT INTO pgsql_query_rules (rule_id, active, match_digest, destination_hostgroup, apply) VALUES (${POLARDB_BENCH_RULE_ID}, 1, '^SELECT', ${POLARDB_BENCH_READER_HG}, 1)"
    else
        rule_sql="INSERT INTO pgsql_query_rules (rule_id, active, match_digest, replica_eligible, apply) VALUES (${POLARDB_BENCH_RULE_ID}, 1, '^SELECT', 1, 0)"
    fi
    polardb_bench_admin "$rule_sql" || return 1
    polardb_bench_admin "LOAD PGSQL QUERY RULES TO RUNTIME" || return 1
}

POLARDB_BENCH_WAL_PID=""

polardb_bench_start_wal_generator() {
    local table="$1"
    local wal_bytes="${2:-1000}"
    local sleep_sec="${3:-0.01}"
    local label="${4:-WAL generator}"

    polardb_bench_validate_identifier "$table" || return 1
    polardb_bench_stop_wal_generator
    echo "[$(ts)] Starting $label (${wal_bytes} bytes every ${sleep_sec}s)"
    (
        while true; do
            if ! polardb_bench_primary_sql "INSERT INTO ${table}(data) VALUES (repeat('x', ${wal_bytes}));" >/dev/null 2>&1; then
                echo "[$(ts)]   WARN: $label insert failed" >&2
            fi
            sleep "$sleep_sec"
        done
    ) &
    POLARDB_BENCH_WAL_PID=$!
}

polardb_bench_stop_wal_generator() {
    if [ -n "${POLARDB_BENCH_WAL_PID:-}" ]; then
        kill "$POLARDB_BENCH_WAL_PID" 2>/dev/null || true
        wait "$POLARDB_BENCH_WAL_PID" 2>/dev/null || true
        POLARDB_BENCH_WAL_PID=""
    fi
}

polardb_bench_proxy_psql_file() {
    local sql_file="$1"
    polardb_proxy_psql -X -A -t -q -v ON_ERROR_STOP=1 -f "$sql_file"
}

polardb_bench_pgbench_file() {
    local protocol_mode="$1"
    local sql_file="$2"

    LD_LIBRARY_PATH="$(polardb_bench_libpq_ld_path)" \
    PGPASSWORD="$PGPASSWORD" PGSSLMODE="$PGSSLMODE" "$PGBENCH_BIN" \
        -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
        -U "$PGUSER" -d "$PGDB" \
        -n -t 1 -c 1 -j 1 -M "$protocol_mode" \
        -f "$sql_file"
}

polardb_bench_pgbench_simple() {
    local duration_sec="$1"
    local clients="$2"
    local script="$3"
    LD_LIBRARY_PATH="$(polardb_bench_libpq_ld_path)" \
    PGPASSWORD="$PGPASSWORD" PGSSLMODE="$PGSSLMODE" "$PGBENCH_BIN" \
        -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
        -U "$PGUSER" -d "$PGDB" \
        -n -T "$duration_sec" -c "$clients" -j "$clients" -M simple \
        -f "$script"
}

polardb_bench_parse_ryw_results() {
    local mode="$1"
    local expected="$2"
    local fail_func="$3"
    local artifact_label="${4:-$mode}"
    local combined="$RUN_DIR/${artifact_label}_combined.out"

    if [ -n "${POLARDB_BENCH_RESULT_TABLE:-}" ]; then
        polardb_bench_validate_identifier "$POLARDB_BENCH_RESULT_TABLE" || return 1
        polardb_bench_primary_sql "SELECT tag || '|' || mode || '|' || worker_id || '|' || iter || '|' || status || '|' || COALESCE(observed_marker, '') || '|' || COALESCE(backend_port, '') FROM ${POLARDB_BENCH_RESULT_TABLE} ORDER BY worker_id, iter;" >"$combined" 2>/dev/null || true
    else
        cat "$RUN_DIR"/"${artifact_label}"_worker_*.out >"$combined" 2>/dev/null || true
    fi
    POLARDB_BENCH_ACTUAL=$(grep -c '^RYW|' "$combined" 2>/dev/null || true)
    POLARDB_BENCH_FRESH=$(awk -F'|' '$1 == "RYW" && $5 == "fresh" { c++ } END { print c+0 }' "$combined" 2>/dev/null)
    POLARDB_BENCH_STALE=$(awk -F'|' '$1 == "RYW" && $5 == "stale" { c++ } END { print c+0 }' "$combined" 2>/dev/null)
    POLARDB_BENCH_BAD=$(awk -F'|' '$1 == "RYW" && $5 == "bad" { c++ } END { print c+0 }' "$combined" 2>/dev/null)
    POLARDB_BENCH_ERRORS=$(grep -ciE 'ERROR|FATAL|could not|connection .*failed' "$combined" 2>/dev/null || true)

    echo "[$(ts)]   rows: expected=$expected actual=$POLARDB_BENCH_ACTUAL fresh=$POLARDB_BENCH_FRESH stale=$POLARDB_BENCH_STALE bad=$POLARDB_BENCH_BAD errors=$POLARDB_BENCH_ERRORS"
    if [ "$POLARDB_BENCH_ACTUAL" -ne "$expected" ]; then
        "$fail_func" "mode=$mode row count mismatch expected=$expected actual=$POLARDB_BENCH_ACTUAL"
    fi
    if [ "$POLARDB_BENCH_BAD" -ne 0 ] || [ "$POLARDB_BENCH_ERRORS" -ne 0 ]; then
        "$fail_func" "mode=$mode bad/errors found bad=$POLARDB_BENCH_BAD errors=$POLARDB_BENCH_ERRORS"
    fi
}

polardb_bench_capture_ryw_results() {
    local mode="$1"
    local expected="$2"
    local fail_func="$3"
    local -n fresh_ref="$4"
    local -n stale_ref="$5"
    local -n bad_ref="$6"
    local -n errors_ref="$7"
    local artifact_label="${8:-$mode}"

    polardb_bench_parse_ryw_results "$mode" "$expected" "$fail_func" "$artifact_label"
    # shellcheck disable=SC2034 # Namerefs write into caller-owned result arrays.
    fresh_ref["$artifact_label"]="$POLARDB_BENCH_FRESH"
    # shellcheck disable=SC2034 # Namerefs write into caller-owned result arrays.
    stale_ref["$artifact_label"]="$POLARDB_BENCH_STALE"
    # shellcheck disable=SC2034 # Namerefs write into caller-owned result arrays.
    bad_ref["$artifact_label"]="$POLARDB_BENCH_BAD"
    # shellcheck disable=SC2034 # Namerefs write into caller-owned result arrays.
    errors_ref["$artifact_label"]="$POLARDB_BENCH_ERRORS"
}

polardb_bench_run_workers() {
    local mode="$1"
    local clients="$2"
    local writer_func="$3"
    local fail_func="$4"
    local pids=()
    local worker sql_file log_file pid rc

    if ! polardb_require_pgbench; then
        "$fail_func" "pgbench is not available"
        return 1
    fi

    for worker in $(seq 1 "$clients"); do
        sql_file="$RUN_DIR/${mode}_worker_${worker}.sql"
        log_file="$RUN_DIR/${mode}_worker_${worker}.out"
        "$writer_func" "$mode" "$worker" "$sql_file"
        (
            polardb_bench_pgbench_file simple "$sql_file"
        ) >"$log_file" 2>&1 &
        pids+=("$!")
    done

    for pid in "${pids[@]}"; do
        wait "$pid"
        rc=$?
        if [ "$rc" -ne 0 ]; then
            "$fail_func" "mode=$mode worker pid=$pid exited rc=$rc"
        fi
    done
}

polardb_bench_backend_reported_port() {
    local host="$1"
    local port="$2"
    polardb_direct_sql "$host" "$port" "SELECT inet_server_port();" 2>/dev/null | tr -d '[:space:]'
}

polardb_bench_libpq_ld_path() {
    if [ -n "${LD_LIBRARY_PATH:-}" ]; then
        printf '%s:%s\n' "$POLARDB_LIBPQ_PATH" "$LD_LIBRARY_PATH"
    else
        printf '%s\n' "$POLARDB_LIBPQ_PATH"
    fi
}
