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
export PROXYSQL_HOST="${PROXYSQL_HOST:-127.0.0.1}"
export PROXYSQL_PORT="${PROXYSQL_PORT:-16433}"
export PROXYSQL_ADMIN_PORT="${PROXYSQL_ADMIN_PORT:-16132}"
export PROXYSQL_MYSQL_ADMIN_PORT="${PROXYSQL_MYSQL_ADMIN_PORT:-16033}"
export PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"

# ProxySQL hostgroups/rules used by the PolarDB harness. Keep these in .env for
# non-default topologies; scripts should not hardcode 10/11/10000 directly.
export POLARDB_WRITER_HG="${POLARDB_WRITER_HG:-10}"
export POLARDB_READER_HG="${POLARDB_READER_HG:-11}"
export POLARDB_SELECT_RULE_ID="${POLARDB_SELECT_RULE_ID:-10000}"

# PostgreSQL/PolarDB credentials. Direct credentials default to the same values
# as the ProxySQL-facing credentials unless explicitly overridden.
export PGDB="${PGDB:-postgres}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGUSER_DIRECT="${PGUSER_DIRECT:-$PGUSER}"
export PGPASSWORD_DIRECT="${PGPASSWORD_DIRECT:-$PGPASSWORD}"
export PGSSLMODE="${PGSSLMODE:-disable}"

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

polardb_validate_manual_topology() {
    [ -n "$PRIMARY_HOST" ] && [ -n "$PRIMARY_PORT" ] && [ -n "$REPLICA_HOST" ] && [ -n "$REPLICA_PORT" ] || return 1
    if [ "$POLARDB_SKIP_ROLE_CHECK" = "1" ]; then
        return 0
    fi
    local writer_role reader_role
    writer_role=$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    reader_role=$(polardb_direct_sql "$REPLICA_HOST" "$REPLICA_PORT" "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    [ "$writer_role" = "f" ] && [ "$reader_role" = "t" ]
}

polardb_detect_topology() {
    if [ -n "$PRIMARY_HOST" ] && [ -n "$PRIMARY_PORT" ] && [ -n "$REPLICA_HOST" ] && [ -n "$REPLICA_PORT" ]; then
        if polardb_validate_manual_topology; then
            export PRIMARY_HOST PRIMARY_PORT REPLICA_HOST REPLICA_PORT
            return 0
        fi
        echo "Configured PolarDB topology failed role validation. Set POLARDB_SKIP_ROLE_CHECK=1 to bypass." >&2
        return 1
    fi

    if [ -z "$POLARDB_ENDPOINTS" ]; then
        echo "POLARDB_ENDPOINTS is empty; set it in test/polardb/.env or disable autodetection with manual PRIMARY_/REPLICA_ variables." >&2
        return 1
    fi

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
            if [ -z "$REPLICA_HOST" ]; then
                REPLICA_HOST="$host"
                REPLICA_PORT="$port"
                REPLICA_INSTANCE="${REPLICA_INSTANCE:-$idx}"
            fi
            ;;
        esac
    done

    if [ "$writer_count" -ne 1 ] || [ "$reader_count" -lt 1 ]; then
        echo "Cannot detect a single writer and at least one reader from POLARDB_ENDPOINTS='$POLARDB_ENDPOINTS' (writers=$writer_count readers=$reader_count)." >&2
        return 1
    fi

    export PRIMARY_HOST PRIMARY_PORT REPLICA_HOST REPLICA_PORT PRIMARY_INSTANCE REPLICA_INSTANCE
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
