#!/usr/bin/env bash
# TAP test: a PolarDB consistency error must not leak the
# 'pgsql-connect_timeout_server_max' connect deadline into a later, unrelated
# query on the same session.
#
# For actual test logic see stale_connect_deadline_runner.cpp

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_test_polardb_stale_connect_deadline")}"
PROXYSQL_START_LOG="${PROXYSQL_START_LOG:-${PROXYSQL_DATA_DIR}.start.log}"

RUNNER_BIN="/tmp/stale_connect_deadline_runner"
LIBPQ_DIR="$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq"

cleanup() {
	"$PROXYSQL_WRAPPER" stop --data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
}

# Build & Run
plan 2

polardb_require_proxysql_or_skip_all polardb
polardb_require_command_or_skip_all g++ g++

if ! polardb_detect_topology; then
	diag "Cannot detect a PolarDB writer + at least one reader for this test."
	polardb_skip_remaining "PolarDB topology unavailable"
	exit 1
fi

if "$PROXYSQL_WRAPPER" start \
	--data-dir "$PROXYSQL_DATA_DIR" \
	--admin-port "$PROXYSQL_ADMIN_PORT" \
	--proxy-port "$PROXYSQL_PORT" \
    > "$PROXYSQL_START_LOG" 2>&1; then
	ok 0 "start ProxySQL"
else
	diag "ProxySQL start failed; log follows"
	sed 's/^/# /' "$PROXYSQL_START_LOG" 2>/dev/null || true
	ok 1 "start ProxySQL for stale connect deadline test"
	polardb_skip_remaining "ProxySQL failed to start"
	exit 1
fi

trap cleanup EXIT

if g++ -std=c++17 \
	-I"$PROXYSQL_ROOT/deps/postgresql/postgresql/src/include" -I"$LIBPQ_DIR" \
	-L"$LIBPQ_DIR" -Wl,-rpath,"$LIBPQ_DIR" \
	"$SCRIPT_DIR/stale_connect_deadline_runner.cpp" -lpq \
	-o "$RUNNER_BIN" 2>&1; then
	ok 0 "build stale_connect_deadline_runner"
else
	diag "Build of stale_connect_deadline_runner failed"
	ok 1 "build stale_connect_deadline_runner"
	exit 1
fi

if ! "$RUNNER_BIN"; then
	FAIL=1
fi

rm -f "$RUNNER_BIN"

if [ "$FAIL" -eq 0 ]; then
	exit 0
fi
exit 1
