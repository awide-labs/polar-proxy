#!/usr/bin/env bash
# PolarDB hot-path profile worker for PGO/BOLT training.
#
# This script deliberately reuses bench5 instead of copying benchmark internals.
# The profile mix is selected here; SQL generation, ProxySQL lifecycle,
# topology setup, counters, and result validation remain owned by the shared
# PolarDB benchmark harness.

set -euo pipefail

PGO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARDB_TEST_DIR="$(cd "$PGO_DIR/.." && pwd)"
PROXYSQL_ROOT="${PROXYSQL_ROOT:-$(cd "$POLARDB_TEST_DIR/../.." && pwd)}"
export PROXYSQL_ROOT
MAKE_BIN="${MAKE:-$(command -v make)}"

# shellcheck source=../common/env.sh
source "$POLARDB_TEST_DIR/common/env.sh"

profile="${1:-core}"
if [ $# -gt 0 ]; then
	shift
fi

if [ ! -x "$PROXYSQL_BINARY" ]; then
	echo "ProxySQL binary not found or not executable: $PROXYSQL_BINARY" >&2
	echo "Build an instrumented binary first, for example:" >&2
	echo "  make -C $PROXYSQL_ROOT polardb-pgo-gcc-generate" >&2
	exit 1
fi

run_bench5_profile() {
	local label="$1"
	local cases="$2"
	local clients="$3"
	local iters="$4"
	local long_work_ms="$5"
	shift 5

	echo "=== PolarDB hot-path profile: $label ==="
	echo "    clients=$clients iters=$iters long_work_ms=$long_work_ms"
	echo "    cases=$cases"

	PROXYSQL_BINARY="$PROXYSQL_BINARY" \
		BENCH5_CLIENTS="$clients" \
		BENCH5_ITERS="$iters" \
		BENCH5_CASES="$cases" \
		BENCH5_TXN_LONG_WORK_MS="$long_work_ms" \
		BENCH5_WAIT_MODE="${POLARDB_HOTPATH_WAIT_MODE:-strict}" \
		BENCH5_WAIT_TIMEOUT_MS="${POLARDB_HOTPATH_WAIT_TIMEOUT_MS:-5000}" \
		BENCH5_MAX_LAG_BYTES="${POLARDB_HOTPATH_MAX_LAG_BYTES:--1}" \
		BENCH5_LAZY_WARMUP_SPLIT="${POLARDB_HOTPATH_LAZY_WARMUP_SPLIT:-1}" \
		BENCH5_PROXY_IDENTITY_MODE="${POLARDB_HOTPATH_PROXY_IDENTITY_MODE:-proxy}" \
		BENCH5_TXN_SPLIT_WARMUP_MODE="${POLARDB_HOTPATH_TXN_SPLIT_WARMUP_MODE:-default}" \
		BENCH5_TXN_SPLIT_SELECT_WAIT_MS="${POLARDB_HOTPATH_TXN_SPLIT_SELECT_WAIT_MS:-0}" \
		"$MAKE_BIN" -C "$POLARDB_TEST_DIR" bench5 "$@"
}

smoke_cases_default="session-write-many-reads:lsn txn-write-read:split txn-locking:split"

core_cases_default="txn-write-many-reads:split txn-read-write-read:split txn-leading-reads-write-read:split session-write-many-reads:lsn session-mixed:lsn txn-locking:split session-write-read:primary"

full_cases_default="txn-write-many-reads:split txn-read-write-read:split txn-leading-reads-write-read:split txn-write-read-long:split txn-read-write-read-long:split session-write-many-reads:lsn session-mixed:lsn txn-locking:split session-readonly:lsn session-write-read:primary"

# Edge is intentionally separate from core/full. It is useful to collect a
# small amount of fallback/veto profile, but timeout and fault paths should not
# dominate profiles intended for steady-state production throughput.
edge_cases_default="txn-locking:split session-readonly:off txn-readonly:split txn-write-read:primary"

case "$profile" in
smoke)
	run_bench5_profile \
		smoke \
		"${POLARDB_HOTPATH_SMOKE_CASES:-$smoke_cases_default}" \
		"${POLARDB_HOTPATH_SMOKE_CLIENTS:-2}" \
		"${POLARDB_HOTPATH_SMOKE_ITERS:-4}" \
		"${POLARDB_HOTPATH_SMOKE_LONG_WORK_MS:-100}" \
		"$@"
	;;
core)
	run_bench5_profile \
		core \
		"${POLARDB_HOTPATH_CORE_CASES:-${POLARDB_HOTPATH_CASES:-$core_cases_default}}" \
		"${POLARDB_HOTPATH_CORE_CLIENTS:-${POLARDB_HOTPATH_CLIENTS:-16}}" \
		"${POLARDB_HOTPATH_CORE_ITERS:-${POLARDB_HOTPATH_ITERS:-100}}" \
		"${POLARDB_HOTPATH_CORE_LONG_WORK_MS:-${POLARDB_HOTPATH_LONG_WORK_MS:-250}}" \
		"$@"
	;;
full)
	run_bench5_profile \
		full \
		"${POLARDB_HOTPATH_FULL_CASES:-${POLARDB_HOTPATH_CASES:-$full_cases_default}}" \
		"${POLARDB_HOTPATH_FULL_CLIENTS:-${POLARDB_HOTPATH_CLIENTS:-32}}" \
		"${POLARDB_HOTPATH_FULL_ITERS:-${POLARDB_HOTPATH_ITERS:-300}}" \
		"${POLARDB_HOTPATH_FULL_LONG_WORK_MS:-${POLARDB_HOTPATH_LONG_WORK_MS:-500}}" \
		"$@"
	;;
edge)
	run_bench5_profile \
		edge \
		"${POLARDB_HOTPATH_EDGE_CASES:-$edge_cases_default}" \
		"${POLARDB_HOTPATH_EDGE_CLIENTS:-4}" \
		"${POLARDB_HOTPATH_EDGE_ITERS:-20}" \
		"${POLARDB_HOTPATH_EDGE_LONG_WORK_MS:-100}" \
		"$@"
	;;
*)
	echo "Usage: $0 [smoke|core|full|edge]" >&2
	exit 2
	;;
esac
