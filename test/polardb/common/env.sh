#!/usr/bin/env bash
# Shared environment loader for PolarDB tests.
#
# The committed tests are intentionally environment-neutral. Local endpoints,
# credentials, and DCS control details belong in an untracked .env file, not in
# the test scripts. By default this loader sources, if present:
#   test/polardb/.env
# Set POLARDB_ENV_FILE to load a specific file instead.

if [ -n "${POLARDB_TEST_ENV_LOADED:-}" ]; then
    return 0
fi
POLARDB_TEST_ENV_LOADED=1

POLARDB_TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROXYSQL_ROOT="${PROXYSQL_ROOT:-$(cd "$POLARDB_TEST_DIR/../.." && pwd)}"
export POLARDB_RUNTIME_DIR="${POLARDB_RUNTIME_DIR:-${TMPDIR:-/tmp}/proxysql-polardb-${UID}}"
export POLARDB_OUTPUT_DIR="${POLARDB_OUTPUT_DIR:-$POLARDB_TEST_DIR/test_output}"

_POLARDB_CALLER_SET_PROXYSQL_PORT="${PROXYSQL_PORT+x}"
_POLARDB_CALLER_SET_PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT+x}"
_POLARDB_CALLER_SET_PROXYSQL_MYSQL_ADMIN_PORT="${PROXYSQL_MYSQL_ADMIN_PORT+x}"
_POLARDB_CALLER_PROXYSQL_PORT="${PROXYSQL_PORT:-}"
_POLARDB_CALLER_PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT:-}"
_POLARDB_CALLER_PROXYSQL_MYSQL_ADMIN_PORT="${PROXYSQL_MYSQL_ADMIN_PORT:-}"
_POLARDB_CALLER_TEST_SHARD="${POLARDB_TEST_SHARD:-}"
_POLARDB_CALLER_SHARD_STRIDE="${POLARDB_PROXY_SHARD_STRIDE:-}"
_POLARDB_CALLER_PARALLEL_PORT_BASE="${POLARDB_PARALLEL_PORT_BASE:-}"
_POLARDB_CALLER_SET_TIMEOUT_EDGE_TESTS="${POLARDB_TIMEOUT_EDGE_TESTS+x}"
_POLARDB_CALLER_TIMEOUT_EDGE_TESTS="${POLARDB_TIMEOUT_EDGE_TESTS:-}"

polardb_source_env_file() {
    local f="$1"
    [ -n "$f" ] || return 0
    [ -f "$f" ] || return 0
    set -a
    # shellcheck source=/dev/null
    . "$f"
    set +a
}

if [ -n "${POLARDB_ENV_FILE:-}" ]; then
    polardb_source_env_file "$POLARDB_ENV_FILE"
else
    polardb_source_env_file "$POLARDB_TEST_DIR/.env"
fi

# A scheduler or direct caller can explicitly disable the backend-mutating
# timeout cases even when an environment file enables them. The caller owns
# this test selection; the topology file supplies only the default.
if [ -n "$_POLARDB_CALLER_SET_TIMEOUT_EDGE_TESTS" ]; then
    POLARDB_TIMEOUT_EDGE_TESTS="$_POLARDB_CALLER_TIMEOUT_EDGE_TESTS"
    export POLARDB_TIMEOUT_EDGE_TESTS
fi

# Parallel TAP runners need shardable defaults even when a local .env still has
# legacy exact ProxySQL ports/data-dir settings. Treat exact ports as base ports
# and let POLARDB_TEST_SHARD derive the concrete listener ports below.
if [ "${POLARDB_PARALLEL_RUN:-0}" = "1" ]; then
    [ -n "${PROXYSQL_PORT:-}" ] && [ -z "${PROXYSQL_PORT_BASE:-}" ] && PROXYSQL_PORT_BASE="$PROXYSQL_PORT"
    [ -n "${PROXYSQL_ADMIN_PORT:-}" ] && [ -z "${PROXYSQL_ADMIN_PORT_BASE:-}" ] && PROXYSQL_ADMIN_PORT_BASE="$PROXYSQL_ADMIN_PORT"
    [ -n "${PROXYSQL_MYSQL_ADMIN_PORT:-}" ] && [ -z "${PROXYSQL_MYSQL_ADMIN_PORT_BASE:-}" ] && PROXYSQL_MYSQL_ADMIN_PORT_BASE="$PROXYSQL_MYSQL_ADMIN_PORT"
    unset PROXYSQL_PORT PROXYSQL_ADMIN_PORT PROXYSQL_MYSQL_ADMIN_PORT PROXYSQL_DATA_DIR
    _POLARDB_CALLER_SET_PROXYSQL_PORT=""
    _POLARDB_CALLER_SET_PROXYSQL_ADMIN_PORT=""
    _POLARDB_CALLER_SET_PROXYSQL_MYSQL_ADMIN_PORT=""
    [ -n "$_POLARDB_CALLER_TEST_SHARD" ] && POLARDB_TEST_SHARD="$_POLARDB_CALLER_TEST_SHARD"
    [ -n "$_POLARDB_CALLER_SHARD_STRIDE" ] && POLARDB_PROXY_SHARD_STRIDE="$_POLARDB_CALLER_SHARD_STRIDE"
    [ -n "$_POLARDB_CALLER_PARALLEL_PORT_BASE" ] && POLARDB_PARALLEL_PORT_BASE="$_POLARDB_CALLER_PARALLEL_PORT_BASE"
fi

if [ -z "${POLARDB_TEST_ENV:-}" ]; then
    case "${POLARDB_MODE:-}" in
    host | hw | hardware | polarctl) POLARDB_TEST_ENV=host ;;
    docker | local) POLARDB_TEST_ENV=docker ;;
    "")
        case "${POLARDB_DCS_MODE:-}" in
        host | hw | hardware | polarctl) POLARDB_TEST_ENV=host ;;
        docker) POLARDB_TEST_ENV=docker ;;
        *) POLARDB_TEST_ENV=docker ;;
        esac
        ;;
    *) POLARDB_TEST_ENV="$POLARDB_MODE" ;;
    esac
fi
case "$POLARDB_TEST_ENV" in
host | hw | hardware | polarctl) POLARDB_TEST_ENV=host ;;
docker | local) POLARDB_TEST_ENV=docker ;;
*)
    echo "Unsupported POLARDB_TEST_ENV/POLARDB_MODE/POLARDB_DCS_MODE: $POLARDB_TEST_ENV" >&2
    return 1
    ;;
esac
export POLARDB_TEST_ENV PROXYSQL_ROOT

# ProxySQL endpoints used by integration tests and C helpers.
export POLARDB_TEST_SHARD="${POLARDB_TEST_SHARD:-0}"
export POLARDB_PROXY_SHARD_STRIDE="${POLARDB_PROXY_SHARD_STRIDE:-100}"
export POLARDB_PARALLEL_PORT_BASE="${POLARDB_PARALLEL_PORT_BASE:-24000}"
case "$POLARDB_TEST_SHARD" in
"" | *[!0-9]*)
    echo "POLARDB_TEST_SHARD must be a non-negative integer: $POLARDB_TEST_SHARD" >&2
    return 1
    ;;
esac
case "$POLARDB_PROXY_SHARD_STRIDE" in
"" | *[!0-9]*)
    echo "POLARDB_PROXY_SHARD_STRIDE must be a positive integer: $POLARDB_PROXY_SHARD_STRIDE" >&2
    return 1
    ;;
0)
    echo "POLARDB_PROXY_SHARD_STRIDE must be greater than zero" >&2
    return 1
    ;;
esac

polardb_proxy_sharded_port() {
    local base="$1"
    printf '%s\n' $((base + POLARDB_TEST_SHARD * POLARDB_PROXY_SHARD_STRIDE))
}

polardb_proxy_parallel_port() {
    local offset="$1"
    printf '%s\n' $((POLARDB_PARALLEL_PORT_BASE + POLARDB_TEST_SHARD * POLARDB_PROXY_SHARD_STRIDE + offset))
}

polardb_proxy_sharded_data_dir() {
    local base="$1"
    if [ "$POLARDB_TEST_SHARD" = "0" ]; then
        printf '%s\n' "$base"
    else
        printf '%s.shard%s\n' "$base" "$POLARDB_TEST_SHARD"
    fi
}

polardb_sql_identifier_sanitize() {
    local raw="$1"
    local safe

    safe=$(printf '%s' "$raw" | tr -c 'A-Za-z0-9_' '_' | sed -e 's/^_*//' -e 's/_*$//')
    [ -n "$safe" ] || safe="x"
    case "$safe" in
    [0-9]*) safe="t_$safe" ;;
    esac
    printf '%s\n' "$safe"
}

polardb_test_object_suffix() {
    local suffix="${POLARDB_TEST_OBJECT_SUFFIX:-}"

    if [ -z "$suffix" ]; then
        suffix="s${POLARDB_TEST_SHARD}"
        if [ -n "${POLARDB_TAP_GROUP:-}" ]; then
            suffix="${suffix}_${POLARDB_TAP_GROUP}"
        fi
    fi
    polardb_sql_identifier_sanitize "$suffix"
}

polardb_test_identifier() {
    local base
    local suffix
    local max_base

    base=$(polardb_sql_identifier_sanitize "$1")
    suffix=$(polardb_test_object_suffix)
    max_base=$((63 - ${#suffix} - 1))
    if [ "$max_base" -lt 1 ]; then
        suffix=$(printf '%s' "$suffix" | cut -c1-30)
        max_base=$((63 - ${#suffix} - 1))
    fi
    if [ "${#base}" -gt "$max_base" ]; then
        base=$(printf '%s' "$base" | cut -c1-"$max_base")
    fi
    printf '%s_%s\n' "$base" "$suffix"
}

# ProxySQL endpoints used by integration tests and C helpers.
export PROXYSQL_HOST="${PROXYSQL_HOST:-127.0.0.1}"
export PROXYSQL_LISTEN_HOST="${PROXYSQL_LISTEN_HOST:-$PROXYSQL_HOST}"
if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_PORT" ]; then
    PROXYSQL_PORT="$_POLARDB_CALLER_PROXYSQL_PORT"
fi
if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_ADMIN_PORT" ]; then
    PROXYSQL_ADMIN_PORT="$_POLARDB_CALLER_PROXYSQL_ADMIN_PORT"
fi
if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_MYSQL_ADMIN_PORT" ]; then
    PROXYSQL_MYSQL_ADMIN_PORT="$_POLARDB_CALLER_PROXYSQL_MYSQL_ADMIN_PORT"
fi
export PROXYSQL_PORT_BASE="${PROXYSQL_PORT_BASE:-${PROXYSQL_PORT:-16433}}"
export PROXYSQL_ADMIN_PORT_BASE="${PROXYSQL_ADMIN_PORT_BASE:-${PROXYSQL_ADMIN_PORT:-16132}}"
export PROXYSQL_MYSQL_ADMIN_PORT_BASE="${PROXYSQL_MYSQL_ADMIN_PORT_BASE:-${PROXYSQL_MYSQL_ADMIN_PORT:-16033}}"
if [ "${POLARDB_PARALLEL_RUN:-0}" = "1" ]; then
    case "$POLARDB_PARALLEL_PORT_BASE" in
    "" | *[!0-9]*)
        echo "POLARDB_PARALLEL_PORT_BASE must be a non-negative integer: $POLARDB_PARALLEL_PORT_BASE" >&2
        return 1
        ;;
    esac
    if [ "$POLARDB_PROXY_SHARD_STRIDE" -lt 3 ]; then
        echo "POLARDB_PROXY_SHARD_STRIDE must be at least 3 for parallel tests: $POLARDB_PROXY_SHARD_STRIDE" >&2
        return 1
    fi

    # One block belongs to one test process. Listener types use fixed slots in
    # that block, so a PostgreSQL port can never become another test's MySQL
    # admin port.
    PROXYSQL_MYSQL_ADMIN_PORT="$(polardb_proxy_parallel_port 0)"
    PROXYSQL_ADMIN_PORT="$(polardb_proxy_parallel_port 1)"
    PROXYSQL_PORT="$(polardb_proxy_parallel_port 2)"
    export PROXYSQL_MYSQL_ADMIN_PORT PROXYSQL_ADMIN_PORT PROXYSQL_PORT
    if [ "$PROXYSQL_PORT" -gt 65535 ]; then
        echo "Parallel ProxySQL port is outside the TCP port range: $PROXYSQL_PORT" >&2
        return 1
    fi
else
    if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_PORT" ]; then
        export PROXYSQL_PORT="${PROXYSQL_PORT:-$(polardb_proxy_sharded_port "$PROXYSQL_PORT_BASE")}"
    else
        PROXYSQL_PORT="$(polardb_proxy_sharded_port "$PROXYSQL_PORT_BASE")"
        export PROXYSQL_PORT
    fi
    if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_ADMIN_PORT" ]; then
        export PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT:-$(polardb_proxy_sharded_port "$PROXYSQL_ADMIN_PORT_BASE")}"
    else
        PROXYSQL_ADMIN_PORT="$(polardb_proxy_sharded_port "$PROXYSQL_ADMIN_PORT_BASE")"
        export PROXYSQL_ADMIN_PORT
    fi
    if [ -n "$_POLARDB_CALLER_SET_PROXYSQL_MYSQL_ADMIN_PORT" ]; then
        export PROXYSQL_MYSQL_ADMIN_PORT="${PROXYSQL_MYSQL_ADMIN_PORT:-$(polardb_proxy_sharded_port "$PROXYSQL_MYSQL_ADMIN_PORT_BASE")}"
    else
        PROXYSQL_MYSQL_ADMIN_PORT="$(polardb_proxy_sharded_port "$PROXYSQL_MYSQL_ADMIN_PORT_BASE")"
        export PROXYSQL_MYSQL_ADMIN_PORT
    fi
fi
export PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
export PROXYSQL_ADMIN_USER="${PROXYSQL_ADMIN_USER:-admin}"
export PROXYSQL_ADMIN_PASSWORD="${PROXYSQL_ADMIN_PASSWORD:-admin}"
export PROXYSQL_ADMIN_DATABASE="${PROXYSQL_ADMIN_DATABASE:-main}"
export PROXYSQL_ADMIN_PGSSLMODE="${PROXYSQL_ADMIN_PGSSLMODE:-disable}"
export PROXYSQL_MYSQL_ADMIN_USER="${PROXYSQL_MYSQL_ADMIN_USER:-$PROXYSQL_ADMIN_USER}"
export PROXYSQL_MYSQL_ADMIN_PASSWORD="${PROXYSQL_MYSQL_ADMIN_PASSWORD:-$PROXYSQL_ADMIN_PASSWORD}"
unset _POLARDB_CALLER_SET_PROXYSQL_PORT _POLARDB_CALLER_SET_PROXYSQL_ADMIN_PORT _POLARDB_CALLER_SET_PROXYSQL_MYSQL_ADMIN_PORT
unset _POLARDB_CALLER_PROXYSQL_PORT _POLARDB_CALLER_PROXYSQL_ADMIN_PORT _POLARDB_CALLER_PROXYSQL_MYSQL_ADMIN_PORT
unset _POLARDB_CALLER_TEST_SHARD _POLARDB_CALLER_SHARD_STRIDE _POLARDB_CALLER_PARALLEL_PORT_BASE

# ProxySQL hostgroups/rules used by the PolarDB harness. Keep these in .env for
# non-default topologies; scripts should not hardcode 10/11/10000 directly.
export POLARDB_WRITER_HG="${POLARDB_WRITER_HG:-10}"
export POLARDB_READER_HG="${POLARDB_READER_HG:-11}"
export POLARDB_SELECT_RULE_ID="${POLARDB_SELECT_RULE_ID:-10000}"
export POLARDB_GLOBAL_LSN_MISSING_WRITER_HG="${POLARDB_GLOBAL_LSN_MISSING_WRITER_HG:-31}"
export POLARDB_GLOBAL_LSN_MISSING_READER_HG="${POLARDB_GLOBAL_LSN_MISSING_READER_HG:-32}"

# PostgreSQL/PolarDB credentials. Direct credentials default to the same values
# as the ProxySQL-facing credentials unless explicitly overridden.
export PGDB="${PGDB:-postgres}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGUSER_DIRECT="${PGUSER_DIRECT:-$PGUSER}"
export PGPASSWORD_DIRECT="${PGPASSWORD_DIRECT:-$PGPASSWORD}"
export PGSSLMODE="${PGSSLMODE:-disable}"
export PROXYSQL_MONITOR_USER="${PROXYSQL_MONITOR_USER:-$PGUSER_DIRECT}"
export PROXYSQL_MONITOR_PASSWORD="${PROXYSQL_MONITOR_PASSWORD:-$PGPASSWORD_DIRECT}"
export POLARDB_RFQ_CLIENT_HOST="${POLARDB_RFQ_CLIENT_HOST:-192.0.2.10}"
export POLARDB_RFQ_CLIENT_PORT="${POLARDB_RFQ_CLIENT_PORT:-54321}"

# Direct endpoint psql helpers. Callers pass host/port explicitly so primary,
# replica, and autodetected endpoints share one implementation.
polardb_direct_psql() {
    local host="$1"
    local port="$2"
    shift 2

    PGPASSWORD="$PGPASSWORD_DIRECT" PGSSLMODE="$PGSSLMODE" psql \
        -h "$host" -p "$port" \
        -U "$PGUSER_DIRECT" -d "$PGDB" \
        "$@"
}

polardb_direct_sql() {
    local host="$1"
    local port="$2"
    local sql="$3"

    polardb_direct_psql "$host" "$port" -A -t -q -v ON_ERROR_STOP=1 -c "$sql"
}

# Candidate direct PolarDB endpoints for topology autodetection. Prefer the
# modern host:port list; retain POLARDB_PROBE_HOST/POLARDB_HOST_PORTS only as a
# compatibility adapter for older local scripts.
if [ -z "${POLARDB_ENDPOINTS:-}" ] && [ -n "${POLARDB_PROBE_HOST:-}" ] && [ -n "${POLARDB_HOST_PORTS:-}" ]; then
    _polardb_eps=""
    for _polardb_port in $POLARDB_HOST_PORTS; do
        _polardb_eps="$_polardb_eps ${POLARDB_PROBE_HOST}:${_polardb_port}"
    done
    POLARDB_ENDPOINTS="${_polardb_eps# }"
    unset _polardb_eps _polardb_port
fi
if [ -z "${POLARDB_ENDPOINTS:-}" ] && [ "$POLARDB_TEST_ENV" = "docker" ]; then
    POLARDB_ENDPOINTS="127.0.0.1:60432 127.0.0.1:60442 127.0.0.1:60452"
fi
export POLARDB_ENDPOINTS="${POLARDB_ENDPOINTS:-}"

export POLARDB_AUTODETECT="${POLARDB_AUTODETECT:-1}"
export POLARDB_SKIP_ROLE_CHECK="${POLARDB_SKIP_ROLE_CHECK:-0}"
export PRIMARY_HOST="${PRIMARY_HOST:-}"
export PRIMARY_PORT="${PRIMARY_PORT:-}"
export REPLICA_HOST="${REPLICA_HOST:-}"
export REPLICA_PORT="${REPLICA_PORT:-}"
export REPLICA2_HOST="${REPLICA2_HOST:-}"
export REPLICA2_PORT="${REPLICA2_PORT:-}"
export POLARDB_REPLICA_ENDPOINTS="${POLARDB_REPLICA_ENDPOINTS:-}"
export PRIMARY_SERVER_ENDPOINT="${PRIMARY_SERVER_ENDPOINT:-}"
export REPLICA_SERVER_ENDPOINT="${REPLICA_SERVER_ENDPOINT:-}"
export PRIMARY_INSTANCE="${PRIMARY_INSTANCE:-}"
export REPLICA_INSTANCE="${REPLICA_INSTANCE:-}"

# DCS / cluster-control settings. Replay-lag tests call polardb_run_dcs(); if a
# test environment cannot expose a DCS command, leave POLARDB_DCS_MODE=none and
# timeout-edge checks will fail or skip at their own call sites.
case "${POLARDB_DCS_MODE:-}" in
host | hw | hardware | polarctl) POLARDB_DCS_MODE=polarctl ;;
docker) POLARDB_DCS_MODE=docker ;;
none | off) POLARDB_DCS_MODE=none ;;
"")
    if [ "$POLARDB_TEST_ENV" = "docker" ]; then
        POLARDB_DCS_MODE=docker
    else
        POLARDB_DCS_MODE=polarctl
    fi
    ;;
*) ;;
esac
export POLARDB_DCS_MODE
export POLARDB_DCS_BIN="${POLARDB_DCS_BIN:-polarctl}"
export POLARDB_DCS_SCOPE="${POLARDB_DCS_SCOPE:-}"
export POLARDB_DCS_SUDO="${POLARDB_DCS_SUDO:-0}"
export POLARDB_DOCKER_BIN="${POLARDB_DOCKER_BIN:-docker}"
export POLARDB_DOCKER_CONTAINER="${POLARDB_DOCKER_CONTAINER:-}"
export POLARDB_DOCKER_SUDO="${POLARDB_DOCKER_SUDO:-0}"
export POLARDB_BUNDLED_PGBENCH="${POLARDB_BUNDLED_PGBENCH:-$PROXYSQL_ROOT/deps/postgresql/postgresql/src/bin/pgbench/pgbench}"
export PGBENCH_BIN="${PGBENCH_BIN:-$POLARDB_BUNDLED_PGBENCH}"

polardb_require_pgbench() {
    if [ ! -x "$PGBENCH_BIN" ]; then
        echo "PGBENCH_BIN is not executable: $PGBENCH_BIN" >&2
        echo "Run: make -C $PROXYSQL_ROOT/test/polardb build" >&2
        return 1
    fi
}

polardb_endpoint_host() { printf '%s\n' "${1%:*}"; }
polardb_endpoint_port() { printf '%s\n' "${1##*:}"; }

polardb_append_endpoint_once() {
    local list="$1"
    local endpoint="$2"
    local item

    [ -n "$endpoint" ] || {
        printf '%s\n' "$list"
        return 0
    }
    for item in $list; do
        if [ "$item" = "$endpoint" ]; then
            printf '%s\n' "$list"
            return 0
        fi
    done
    if [ -n "$list" ]; then
        printf '%s %s\n' "$list" "$endpoint"
    else
        printf '%s\n' "$endpoint"
    fi
}

polardb_build_manual_replica_endpoints() {
    local endpoints=""

    if [ -n "$REPLICA_HOST" ] && [ -n "$REPLICA_PORT" ]; then
        endpoints=$(polardb_append_endpoint_once "$endpoints" "$REPLICA_HOST:$REPLICA_PORT")
    fi
    if [ -n "$REPLICA2_HOST" ] && [ -n "$REPLICA2_PORT" ]; then
        endpoints=$(polardb_append_endpoint_once "$endpoints" "$REPLICA2_HOST:$REPLICA2_PORT")
    fi
    printf '%s\n' "$endpoints"
}

polardb_each_replica_endpoint() {
    local endpoints="${POLARDB_REPLICA_ENDPOINTS:-}"

    if [ -z "$endpoints" ]; then
        endpoints=$(polardb_build_manual_replica_endpoints)
    fi
    printf '%s\n' $endpoints
}

polardb_validate_manual_topology() {
    [ -n "$PRIMARY_HOST" ] && [ -n "$PRIMARY_PORT" ] && [ -n "$REPLICA_HOST" ] && [ -n "$REPLICA_PORT" ] || return 1
    if { [ -n "$REPLICA2_HOST" ] && [ -z "$REPLICA2_PORT" ]; } ||
        { [ -z "$REPLICA2_HOST" ] && [ -n "$REPLICA2_PORT" ]; }; then
        return 1
    fi
    POLARDB_REPLICA_ENDPOINTS=$(polardb_build_manual_replica_endpoints)
    [ -n "$POLARDB_REPLICA_ENDPOINTS" ] || return 1
    if [ "$POLARDB_SKIP_ROLE_CHECK" = "1" ]; then
        export POLARDB_REPLICA_ENDPOINTS
        return 0
    fi
    local writer_role reader_role endpoint host port
    writer_role=$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    [ "$writer_role" = "f" ] || return 1
    for endpoint in $POLARDB_REPLICA_ENDPOINTS; do
        host=$(polardb_endpoint_host "$endpoint")
        port=$(polardb_endpoint_port "$endpoint")
        reader_role=$(polardb_direct_sql "$host" "$port" "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
        [ "$reader_role" = "t" ] || return 1
    done
    export POLARDB_REPLICA_ENDPOINTS
    return 0
}

polardb_detect_topology() {
    if [ -n "$PRIMARY_HOST" ] && [ -n "$PRIMARY_PORT" ] && [ -n "$REPLICA_HOST" ] && [ -n "$REPLICA_PORT" ]; then
        if polardb_validate_manual_topology; then
            export PRIMARY_HOST PRIMARY_PORT REPLICA_HOST REPLICA_PORT REPLICA2_HOST REPLICA2_PORT POLARDB_REPLICA_ENDPOINTS
            return 0
        fi
        echo "Configured PolarDB topology failed role validation. Set POLARDB_SKIP_ROLE_CHECK=1 to bypass." >&2
        return 1
    fi

    if [ -z "$POLARDB_ENDPOINTS" ]; then
        echo "POLARDB_ENDPOINTS is empty; set it in test/polardb/.env or disable autodetection with manual PRIMARY_/REPLICA_ variables." >&2
        return 1
    fi

    POLARDB_REPLICA_ENDPOINTS=""
    local endpoint host port role idx=0 writer_count=0 reader_count=0
    for endpoint in $POLARDB_ENDPOINTS; do
        idx=$((idx + 1))
        host=$(polardb_endpoint_host "$endpoint")
        port=$(polardb_endpoint_port "$endpoint")
        role=$(polardb_direct_sql "$host" "$port" "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
        case "$role" in
        f)
            writer_count=$((writer_count + 1))
            if [ -z "$PRIMARY_HOST" ]; then
                PRIMARY_HOST="$host"
                PRIMARY_PORT="$port"
                PRIMARY_INSTANCE="${PRIMARY_INSTANCE:-$idx}"
            fi
            ;;
        t)
            reader_count=$((reader_count + 1))
            POLARDB_REPLICA_ENDPOINTS=$(polardb_append_endpoint_once "$POLARDB_REPLICA_ENDPOINTS" "$host:$port")
            if [ -z "$REPLICA_HOST" ]; then
                REPLICA_HOST="$host"
                REPLICA_PORT="$port"
                REPLICA_INSTANCE="${REPLICA_INSTANCE:-$idx}"
            elif [ -z "$REPLICA2_HOST" ]; then
                REPLICA2_HOST="$host"
                REPLICA2_PORT="$port"
            fi
            ;;
        esac
    done

    if [ "$writer_count" -ne 1 ] || [ "$reader_count" -lt 1 ]; then
        echo "Cannot detect a single writer and at least one reader from POLARDB_ENDPOINTS='$POLARDB_ENDPOINTS' (writers=$writer_count readers=$reader_count)." >&2
        return 1
    fi

    export PRIMARY_HOST PRIMARY_PORT REPLICA_HOST REPLICA_PORT REPLICA2_HOST REPLICA2_PORT POLARDB_REPLICA_ENDPOINTS PRIMARY_INSTANCE REPLICA_INSTANCE
    return 0
}

polardb_maybe_sudo() {
    local use_sudo="$1"
    shift
    if [ "$use_sudo" = "1" ]; then
        sudo "$@"
    else
        "$@"
    fi
}

polardb_run_dcs() {
    case "$POLARDB_DCS_MODE" in
    docker)
        if [ -z "$POLARDB_DOCKER_CONTAINER" ]; then
            echo "POLARDB_DOCKER_CONTAINER is required when POLARDB_DCS_MODE=docker." >&2
            return 1
        fi
        polardb_maybe_sudo "$POLARDB_DOCKER_SUDO" "$POLARDB_DOCKER_BIN" exec "$POLARDB_DOCKER_CONTAINER" polarctl dcs "$@"
        ;;
    polarctl | host)
        if [ -z "$POLARDB_DCS_SCOPE" ]; then
            echo "POLARDB_DCS_SCOPE is required when POLARDB_DCS_MODE=polarctl." >&2
            return 1
        fi
        polardb_maybe_sudo "$POLARDB_DCS_SUDO" "$POLARDB_DCS_BIN" -c "$POLARDB_DCS_SCOPE" dcs "$@"
        ;;
    none)
        echo "No DCS command configured (POLARDB_DCS_MODE=none)." >&2
        return 1
        ;;
    *)
        echo "Unsupported POLARDB_DCS_MODE='$POLARDB_DCS_MODE'." >&2
        return 1
        ;;
    esac
}
