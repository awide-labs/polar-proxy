#!/usr/bin/env bash
# Verify synchronous PolarDB frontend output optimizations preserve every row.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_frontend_output_tap")}"
PROXYSQL_START_LOG="${PROXYSQL_START_LOG:-${PROXYSQL_DATA_DIR}.start.log}"
PROXYSQL_CONNECT_HOST="${PROXYSQL_CONNECT_HOST:-$PROXYSQL_HOST}"
WRITER_HG="$POLARDB_WRITER_HG"
PLAN=13
FAIL=0
STARTED_PROXY=0

EXPECTED_OUTPUT="${PROXYSQL_DATA_DIR}.expected"
COALESCE_EXPECTED_OUTPUT="${PROXYSQL_DATA_DIR}.coalesce.expected"
MODE_OUTPUT="${PROXYSQL_DATA_DIR}.mode"
RESULT_QUERY="SELECT i::text || ':' || repeat(md5(i::text), 8) FROM generate_series(1, 4096) AS rows(i) ORDER BY i;"
COALESCE_QUERY="SELECT i::text || ':' || repeat(md5(i::text), 256), pg_sleep(CASE WHEN i % 4 = 0 THEN 0.01 ELSE 0 END) FROM generate_series(1, 128) AS rows(i) ORDER BY i;"

cleanup() {
	rm -f "$EXPECTED_OUTPUT" "$COALESCE_EXPECTED_OUTPUT" "$MODE_OUTPUT"
	if [ "$STARTED_PROXY" -eq 1 ]; then
		PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop \
			--data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

start_proxy() {
	rm -f "$PROXYSQL_START_LOG"
	PROXYSQL_PGSQL_THREADS=1 PROXYSQL_BINARY="$PROXYSQL_BINARY" \
		"$PROXYSQL_WRAPPER" start \
		--data-dir "$PROXYSQL_DATA_DIR" \
		--admin-port "$PROXYSQL_ADMIN_PORT" \
		--proxy-port "$PROXYSQL_PORT" \
		--mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT" \
		>"$PROXYSQL_START_LOG" 2>&1
}

configure_proxy() {
	admin_sql "DELETE FROM pgsql_replication_hostgroups;" >/dev/null || return 1
	admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null || return 1
	admin_sql "DELETE FROM pgsql_servers;" >/dev/null || return 1
	admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections, use_ssl, comment) VALUES ($WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, 20, 0, 'frontend_output_writer');" >/dev/null || return 1
	admin_sql "DELETE FROM pgsql_users;" >/dev/null || return 1
	admin_sql "INSERT INTO pgsql_users (username, password, active, use_ssl, default_hostgroup, transaction_persistent, max_connections, comment) VALUES ('$PGUSER', '$PGPASSWORD', 1, 0, $WRITER_HG, 1, 20, 'frontend_output_user');" >/dev/null || return 1
	admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null || return 1
	admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null || return 1
	set_global_var pgsql-threshold_resultset_size 32768 || return 1
	set_output_mode 0 0 0 0
}

# Apply one synchronous output configuration. The four arguments control direct
# writev, row-run result forwarding, byte coalescing, and packet coalescing.
set_output_mode() {
	set_global_var pgsql-polardb_writev_direct "$1" || return 1
	set_global_var pgsql-polardb_result_fast_forward "$2" || return 1
	set_global_var pgsql-polardb_output_coalesce_bytes "$3" || return 1
	set_global_var pgsql-polardb_output_coalesce_packets "$4" || return 1
	admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
}

direct_query_to_file() {
	local query="${2:-$RESULT_QUERY}"
	PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$PGSSLMODE" \
		timeout 30 psql -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" \
		-U "$PGUSER_DIRECT" -d "$PGDB" -X -A -t -q \
		-v ON_ERROR_STOP=1 -c "$query" >"$1"
}

proxy_query_to_file() {
	local query="${2:-$RESULT_QUERY}"
	PGPASSWORD="$PGPASSWORD" PGSSLMODE=disable \
		timeout 30 psql -h "$PROXYSQL_CONNECT_HOST" -p "$PROXYSQL_PORT" \
		-U "$PGUSER" -d "$PGDB" -X -A -t -q -v ON_ERROR_STOP=1 \
		-c "$query" >"$1"
}

output_matches_reference() {
	local expected="${1:-$EXPECTED_OUTPUT}"
	cmp -s "$expected" "$MODE_OUTPUT"
}

counter_increased() {
	local name="$1"
	local before="$2"
	[ "$(counter_delta "$name" "$before")" -gt 0 ]
}

plan "$PLAN"

polardb_require_command_or_skip_all psql psql
polardb_require_command_or_skip_all cmp cmp
polardb_require_command_or_skip_all timeout timeout
polardb_require_proxysql_or_skip_all polardb

if polardb_detect_topology; then
	ok 0 "detect PolarDB writer"
else
	skip_ok "detect PolarDB writer" "PolarDB topology unavailable"
	polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
	exit 0
fi

PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop \
	--data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
if start_proxy; then
	STARTED_PROXY=1
	ok 0 "start ProxySQL for synchronous frontend output"
else
	diag "ProxySQL start failed; log follows"
	sed 's/^/# /' "$PROXYSQL_START_LOG" 2>/dev/null || true
	ok 1 "start ProxySQL for synchronous frontend output"
	polardb_skip_remaining "ProxySQL start failed" "$PLAN"
	exit "$FAIL"
fi

if configure_proxy; then
	ok 0 "configure writer for synchronous frontend output"
else
	ok 1 "configure writer for synchronous frontend output"
	polardb_skip_remaining "ProxySQL configuration failed" "$PLAN"
	exit "$FAIL"
fi

if direct_query_to_file "$EXPECTED_OUTPUT" &&
	[ "$(wc -l <"$EXPECTED_OUTPUT")" -eq 4096 ]; then
	ok 0 "build complete direct-backend reference result"
else
	ok 1 "build complete direct-backend reference result"
	polardb_skip_remaining "reference result unavailable" "$PLAN"
	exit "$FAIL"
fi

if proxy_query_to_file "$MODE_OUTPUT" && output_matches_reference; then
	ok 0 "buffered frontend output matches the reference"
else
	ok 1 "buffered frontend output matches the reference"
fi

writev_before=$(counter PolarDB_WriteV_Attempts)
writev_bytes_before=$(counter PolarDB_WriteV_Bytes)
writev_errors_before=$(counter PolarDB_WriteV_Errors)
set_output_mode 1 0 0 0
if proxy_query_to_file "$MODE_OUTPUT" && output_matches_reference; then
	ok 0 "direct writev output matches the reference"
else
	ok 1 "direct writev output matches the reference"
fi
if counter_increased PolarDB_WriteV_Attempts "$writev_before" &&
	counter_increased PolarDB_WriteV_Bytes "$writev_bytes_before" &&
	[ "$(counter_delta PolarDB_WriteV_Errors "$writev_errors_before")" -eq 0 ]; then
	ok 0 "direct writev handled data without send errors"
else
	ok 1 "direct writev handled data without send errors"
fi

row_run_attempts_before=$(counter PolarDB_Result_Row_Run_Attempts)
row_run_used_before=$(counter PolarDB_Result_Row_Run_Used)
row_run_frames_before=$(counter PolarDB_Result_Row_Run_Frames)
set_output_mode 0 1 0 0
if proxy_query_to_file "$MODE_OUTPUT" && output_matches_reference; then
	ok 0 "row-run output matches the reference"
else
	ok 1 "row-run output matches the reference"
fi
if counter_increased PolarDB_Result_Row_Run_Attempts "$row_run_attempts_before" &&
	counter_increased PolarDB_Result_Row_Run_Used "$row_run_used_before" &&
	counter_increased PolarDB_Result_Row_Run_Frames "$row_run_frames_before"; then
	ok 0 "row-run forwarding handled backend rows"
else
	ok 1 "row-run forwarding handled backend rows"
fi

coalesce_hold_before=$(counter PolarDB_Output_Coalesce_Hold)
coalesce_budget_before=$(counter PolarDB_Output_Coalesce_Flush_Budget)
coalesce_backpressure_before=$(counter PolarDB_Output_Coalesce_Flush_Backpressure)
set_output_mode 0 0 131072 16
if direct_query_to_file "$COALESCE_EXPECTED_OUTPUT" "$COALESCE_QUERY" &&
	proxy_query_to_file "$MODE_OUTPUT" "$COALESCE_QUERY" &&
	output_matches_reference "$COALESCE_EXPECTED_OUTPUT"; then
	ok 0 "coalesced output matches the reference"
else
	ok 1 "coalesced output matches the reference"
fi
if counter_increased PolarDB_Output_Coalesce_Hold "$coalesce_hold_before" &&
	counter_increased PolarDB_Output_Coalesce_Flush_Budget "$coalesce_budget_before"; then
	ok 0 "coalescing held and released streaming output"
else
	diag "coalesce deltas: hold=$(counter_delta PolarDB_Output_Coalesce_Hold "$coalesce_hold_before") budget=$(counter_delta PolarDB_Output_Coalesce_Flush_Budget "$coalesce_budget_before") backpressure=$(counter_delta PolarDB_Output_Coalesce_Flush_Backpressure "$coalesce_backpressure_before")"
	ok 1 "coalescing held and released streaming output"
fi

combined_writev_before=$(counter PolarDB_WriteV_Attempts)
combined_row_run_before=$(counter PolarDB_Result_Row_Run_Used)
combined_coalesce_before=$(counter PolarDB_Output_Coalesce_Hold)
set_output_mode 1 1 131072 16
if proxy_query_to_file "$MODE_OUTPUT" "$COALESCE_QUERY" &&
	output_matches_reference "$COALESCE_EXPECTED_OUTPUT"; then
	ok 0 "combined synchronous output matches the reference"
else
	ok 1 "combined synchronous output matches the reference"
fi
if counter_increased PolarDB_WriteV_Attempts "$combined_writev_before" &&
	counter_increased PolarDB_Result_Row_Run_Used "$combined_row_run_before" &&
	counter_increased PolarDB_Output_Coalesce_Hold "$combined_coalesce_before"; then
	ok 0 "combined run exercised all synchronous optimizations"
else
	diag "combined deltas: writev=$(counter_delta PolarDB_WriteV_Attempts "$combined_writev_before") row_run=$(counter_delta PolarDB_Result_Row_Run_Used "$combined_row_run_before") coalesce=$(counter_delta PolarDB_Output_Coalesce_Hold "$combined_coalesce_before")"
	ok 1 "combined run exercised all synchronous optimizations"
fi

exit "$FAIL"
