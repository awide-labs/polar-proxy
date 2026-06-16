#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"

usage() {
    cat >&2 <<'EOF'
Usage:
  test/polardb/test-c/run_helper.sh [--build-only] <helper> [helper-args...]

Helpers:
  libpq_lsn          Direct PolarDB libpq RFQ-LSN smoke test.
  extended_protocol ProxySQL extended-protocol helper.

The runner loads test/polardb/.env (or POLARDB_ENV_FILE), builds the selected
C helper against vendored PolarDB-patched libpq, sets LD_LIBRARY_PATH, and runs
the helper binary.
EOF
}

build_only=0
if [ "${1:-}" = "--build-only" ]; then
    build_only=1
    shift
fi

helper="${1:-}"
if [ -z "$helper" ]; then
    usage
    exit 2
fi
shift

polardb_prepare_direct_helper_env() {
    if [ "${POLARDB_AUTODETECT:-1}" = "1" ]; then
        polardb_detect_topology >/dev/null 2>&1 || true
    fi

    if [ -n "${PRIMARY_HOST:-}" ] && [ -n "${PRIMARY_PORT:-}" ]; then
        export POLARDB_HOST="$PRIMARY_HOST"
        export POLARDB_PORT="$PRIMARY_PORT"
    fi
    if [ -n "${REPLICA_HOST:-}" ] && [ -n "${REPLICA_PORT:-}" ]; then
        export POLARDB_REPLICA_HOST="$REPLICA_HOST"
        export POLARDB_REPLICA_PORT="$REPLICA_PORT"
    fi
    export POLARDB_USER="${POLARDB_USER:-$PGUSER_DIRECT}"
    export POLARDB_PASSWORD="${POLARDB_PASSWORD:-$PGPASSWORD_DIRECT}"
    export POLARDB_DB="${POLARDB_DB:-$PGDB}"
}

case "$helper" in
libpq_lsn | libpq-lsn)
    target="polardb/bin/libpq_lsn_test"
    binary="$PROXYSQL_ROOT/test/polardb/bin/libpq_lsn_test"
    polardb_prepare_direct_helper_env
    ;;
extended_protocol | extended-protocol | extended)
    target="polardb/bin/proxysql_extended_protocol_test"
    binary="$PROXYSQL_ROOT/test/polardb/bin/proxysql_extended_protocol_test"
    ;;
*)
    echo "Unknown PolarDB C helper: $helper" >&2
    usage
    exit 2
    ;;
esac

if [ "$build_only" = "1" ]; then
    make -C "$PROXYSQL_ROOT/test" "$target"
    exit 0
fi

build_log="$(mktemp "${TMPDIR:-/tmp}/polardb-c-helper-build.XXXXXX")"
cleanup() { rm -f "$build_log"; }
trap cleanup EXIT
if ! make -C "$PROXYSQL_ROOT/test" "$target" >"$build_log" 2>&1; then
    cat "$build_log" >&2
    exit 1
fi
export LD_LIBRARY_PATH="$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq:${LD_LIBRARY_PATH:-}"
# Do not exec: the EXIT trap must remove the temporary build log.
"$binary" "$@"
