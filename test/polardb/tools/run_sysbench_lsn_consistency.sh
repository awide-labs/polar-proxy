#!/usr/bin/env bash
# Run a client-visible RFQ LSN consistency check with sysbench Lua.
#
# Native mode uses the sysbench-polar-rfq submodule build and is the default.
# Preload mode is available for diagnostics with an unmodified system sysbench.
# Preloading libpq is intentional because some system binaries link to private
# libpq sonames that LD_LIBRARY_PATH cannot override.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARDB_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=../common/env.sh
. "$POLARDB_TEST_DIR/common/env.sh"

DEFAULT_SYSBENCH_BIN="$PROXYSQL_ROOT/test/polardb/bin/sysbench-polar-rfq"
SYSBENCH_RFQ_MODE="${SYSBENCH_RFQ_MODE:-native}"
case "$SYSBENCH_RFQ_MODE" in
native)
    SYSBENCH_BIN="${SYSBENCH_BIN:-$DEFAULT_SYSBENCH_BIN}"
    ;;
preload)
    SYSBENCH_BIN="${SYSBENCH_BIN:-sysbench}"
    ;;
*)
    echo "SYSBENCH_RFQ_MODE must be native or preload: $SYSBENCH_RFQ_MODE" >&2
    exit 2
    ;;
esac
THREADS="${THREADS:-1}"
EVENTS="${EVENTS:-3}"
TRACE_SO="$PROXYSQL_ROOT/test/polardb/bin/polardb_lsn_trace.so"
CLIENT_HOST="$POLARDB_RFQ_CLIENT_HOST"
CLIENT_PORT="$POLARDB_RFQ_CLIENT_PORT"
DEFAULT_LUA_SCRIPT="$PROXYSQL_ROOT/test/polardb/bin/sysbench-polar-rfq.d/sysbench_lsn_consistency.lua"
if [ -z "${SYSBENCH_LUA_SCRIPT:-}" ] && [ -r "$DEFAULT_LUA_SCRIPT" ]; then
    LUA_SCRIPT="$DEFAULT_LUA_SCRIPT"
else
    LUA_SCRIPT="${SYSBENCH_LUA_SCRIPT:-$SCRIPT_DIR/sysbench_lsn_consistency.lua}"
fi
LIBPQ_DIR="$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq"
PATCHED_LIBPQ="$LIBPQ_DIR/libpq.so.5"

if ! command -v "$SYSBENCH_BIN" >/dev/null 2>&1; then
    echo "SYSBENCH_BIN is not executable or not in PATH: $SYSBENCH_BIN" >&2
    echo "Run: make -C $PROXYSQL_ROOT/test/polardb build-sysbench-polar-rfq" >&2
    echo "Or set SYSBENCH_BIN=/path/to/sysbench." >&2
    exit 1
fi

if [ "$SYSBENCH_RFQ_MODE" = "native" ]; then
    if ! command -v nm >/dev/null 2>&1 ||
        ! nm -D "$SYSBENCH_BIN" 2>/dev/null | grep -q ' db_polar_has_lsn$'; then
        echo "Native sysbench does not export db_polar_has_lsn: $SYSBENCH_BIN" >&2
        echo "Run: make -C $PROXYSQL_ROOT/test/polardb rfq-bench-tools-check" >&2
        exit 1
    fi
else
    make -C "$PROXYSQL_ROOT/test" polardb/bin/polardb_lsn_trace.so >/dev/null

    if [ ! -r "$PATCHED_LIBPQ" ]; then
        echo "Missing patched libpq: $PATCHED_LIBPQ" >&2
        echo "Run: make -C $PROXYSQL_ROOT/test/polardb build" >&2
        exit 1
    fi
fi

# sysbench's pgsql driver builds a libpq conninfo string as
# "dbname=<pgsql-db> sslmode=<pgsql-sslmode>". Passing extra conninfo tokens in
# pgsql-db is therefore enough for patched libpq to send the PolarDB startup
# parameters.
pgsql_db="$PGDB _polar_proxy_send_lsn=true _polar_proxy_client_host=$CLIENT_HOST _polar_proxy_client_port=$CLIENT_PORT"

echo "== sysbench RFQ-LSN consistency check =="
echo "sysbench=$SYSBENCH_BIN"
echo "threads=$THREADS events=$EVENTS"
echo "mode=$SYSBENCH_RFQ_MODE"
if [ "$SYSBENCH_RFQ_MODE" = "preload" ]; then
    echo "patched_libpq=$LIBPQ_DIR"
fi
if ldd "$SYSBENCH_BIN" 2>/dev/null | grep -q 'libpq'; then
    echo "sysbench_libpq=$(ldd "$SYSBENCH_BIN" 2>/dev/null | grep 'libpq' | head -1 | sed 's/^/[linked] /')"
fi
echo "Verdicts:"
echo "  WRITE_LSN_OK         write RFQ exposed non-zero LSN"
echo "  CONSISTENT           read RFQ LSN >= session target captured before read"
echo "  STALE_BY_LSN         read RFQ LSN < target"
echo "  UNKNOWN_NO_RFQ_LSN   client did not receive RFQ LSN"
echo "Scopes:"
echo "  scope=session        connection-wide context"
echo "  scope=begin          BEGIN froze a transaction start target"
echo "  scope=txn            transaction-local context"
echo

if [ "$SYSBENCH_RFQ_MODE" = "native" ]; then
    "$SYSBENCH_BIN" "$LUA_SCRIPT" \
        --db-driver=pgsql \
        --pgsql-host="$PROXYSQL_HOST" \
        --pgsql-port="$PROXYSQL_PORT" \
        --pgsql-user="$PGUSER" \
        --pgsql-password="$PGPASSWORD" \
        --pgsql-db="$pgsql_db" \
        --pgsql-sslmode="$PGSSLMODE" \
        --threads="$THREADS" \
        --events="$EVENTS" \
        run
else
    LD_LIBRARY_PATH="$LIBPQ_DIR:${LD_LIBRARY_PATH:-}" \
    LD_PRELOAD="$TRACE_SO:$PATCHED_LIBPQ" \
    POLARDB_LSN_TRACE=1 \
    "$SYSBENCH_BIN" "$LUA_SCRIPT" \
        --db-driver=pgsql \
        --pgsql-host="$PROXYSQL_HOST" \
        --pgsql-port="$PROXYSQL_PORT" \
        --pgsql-user="$PGUSER" \
        --pgsql-password="$PGPASSWORD" \
        --pgsql-db="$pgsql_db" \
        --pgsql-sslmode="$PGSSLMODE" \
        --threads="$THREADS" \
        --events="$EVENTS" \
        run
fi
