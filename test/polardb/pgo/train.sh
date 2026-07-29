#!/usr/bin/env bash
# PolarDB PGO/BOLT training workload.
#
# This is intentionally part of test/polardb because the workload depends on the
# same live PolarDB topology, ProxySQL lifecycle, and scenario/bench helpers as
# the integration suite. Keep it focused on core + PolarDB paths; do not add
# ClickHouse or unrelated ProxySQL feature coverage here.

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

run_target() {
	local target="$1"
	shift || true
	echo "=== PolarDB PGO train: make -C $POLARDB_TEST_DIR $target $* ==="
	PROXYSQL_BINARY="$PROXYSQL_BINARY" "$MAKE_BIN" -C "$POLARDB_TEST_DIR" "$target" "$@"
}

run_hotpath() {
	local profile="$1"
	shift || true
	echo "=== PolarDB PGO train: hot-path profile=$profile $* ==="
	PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PGO_DIR/hotpath_worker.sh" "$profile" "$@"
}

train_smoke() {
	run_target test-case1
	run_target test-case2-use-writer-success
	run_target test-case5
	run_hotpath smoke
}

train_core() {
	train_smoke
	run_target test-case2-stale-with-warning-success
	run_target tap-split
	run_target tap-split-failure-policy
	run_hotpath core
}

train_full() {
	train_smoke
	run_target test-case2-stale-with-warning-success
	run_target test-case2-stale-with-warning-failure
	run_target test-case2-use-writer-failure
	run_target tap-split
	run_target tap-split-failure-policy
	run_hotpath full
	run_hotpath edge
}

case "$profile" in
smoke) train_smoke "$@" ;;
core) train_core "$@" ;;
full) train_full "$@" ;;
*)
	echo "Usage: $0 [smoke|core|full]" >&2
	exit 2
	;;
esac
