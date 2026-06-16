#!/bin/bash
#
# proxysql_lifecycle.sh - ProxySQL lifecycle helper for PolarDB tests
#
# Usage:
#   ./proxysql_lifecycle.sh start [--config FILE] [--data-dir DIR] [--admin-port PORT] [--proxy-port PORT]
#   ./proxysql_lifecycle.sh stop
#   ./proxysql_lifecycle.sh status
#   ./proxysql_lifecycle.sh restart [options...]
#
# This script ensures clean ProxySQL lifecycle management:
#   - Kills any existing test instances before starting
#   - Waits for ProxySQL to be ready
#   - Provides consistent port configuration
#   - Handles cleanup properly

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROXYSQL_ROOT="${PROXYSQL_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

# Default configuration
PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
DEFAULT_DATA_DIR="${PROXYSQL_DATA_DIR:-/tmp/proxysql_test_data}"
DEFAULT_ADMIN_PORT=16132
DEFAULT_PROXY_PORT=16433
DEFAULT_MYSQL_ADMIN_PORT=16033

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_pid_exit() {
    local pid="$1"
    local max_wait="${2:-1}"
    local waited=0

    while [ "$waited" -lt "$max_wait" ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 1
}

# Check if binary exists
check_binary() {
    if [ ! -x "$PROXYSQL_BINARY" ]; then
        log_error "ProxySQL binary not found or not executable: $PROXYSQL_BINARY"
        log_error "Build ProxySQL first: make -C $PROXYSQL_ROOT polardb"
        log_error "Use polardb-debug for tests that require debug traces or fault injection."
        exit 1
    fi
}

pid_file_for_data_dir() {
    printf '%s/proxysql.pid\n' "$1"
}

require_option_value() {
    local option="$1"
    local value="${2:-}"
    if [ -z "$value" ]; then
        log_error "Missing value for $option"
        exit 1
    fi
    printf '%s\n' "$value"
}

proxysql_data_dir_from_start_args() {
    local data_dir="$DEFAULT_DATA_DIR"
    while [ $# -gt 0 ]; do
        case "$1" in
        --data-dir)
            data_dir=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        --config | --admin-port | --proxy-port)
            require_option_value "$1" "${2:-}" >/dev/null
            shift 2
            ;;
        *)
            shift
            ;;
        esac
    done
    printf '%s\n' "$data_dir"
}

parse_data_dir_option() {
    local data_dir="$DEFAULT_DATA_DIR"
    while [ $# -gt 0 ]; do
        case "$1" in
        --data-dir)
            data_dir=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
        esac
    done
    printf '%s\n' "$data_dir"
}

proxysql_pid_matches_data_dir() {
    local pid="$1"
    local data_dir="$2"
    local arg prev=""

    # Test ProxySQL is started by this wrapper with: -D "$data_dir".
    # Use argv parsing and the data directory as the ownership boundary instead
    # of matching old harness names, ports, or free-form command text.
    [ -r "/proc/$pid/cmdline" ] || return 1
    while IFS= read -r -d '' arg; do
        if [ "$prev" = "-D" ] && [ "$arg" = "$data_dir" ]; then
            return 0
        fi
        case "$arg" in
        -D*) [ "${arg#-D}" = "$data_dir" ] && return 0 ;;
        esac
        prev="$arg"
    done <"/proc/$pid/cmdline"
    return 1
}

# Find and kill the ProxySQL test instance owned by one data directory.
kill_test_instances() {
    local data_dir="${1:-$DEFAULT_DATA_DIR}"
    local pid_file
    pid_file=$(pid_file_for_data_dir "$data_dir")

    log_info "Checking for existing test ProxySQL instances for data dir: $data_dir"

    # Kill by PID file first. This is the precise instance this wrapper started.
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log_info "Killing ProxySQL (PID: $pid) from PID file..."
            kill "$pid" 2>/dev/null || true
            if ! wait_for_pid_exit "$pid" "${PROXYSQL_STOP_GRACE_SECONDS:-1}"; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi

    # Defensive scan for a foreground ProxySQL process using the same data dir.
    # This handles stale/missing PID files without touching unrelated instances.
    local pids=""
    pids=$(pgrep -f "^[^ ]*proxysql -f" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        local pid
        for pid in $pids; do
            if proxysql_pid_matches_data_dir "$pid" "$data_dir"; then
                log_info "Killing test ProxySQL (PID: $pid)"
                kill "$pid" 2>/dev/null || true
            fi
        done
        for pid in $pids; do
            if proxysql_pid_matches_data_dir "$pid" "$data_dir" &&
                ! wait_for_pid_exit "$pid" "${PROXYSQL_STOP_GRACE_SECONDS:-1}"; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        done
    fi

    sleep 1
    log_info "Cleanup complete"
}

# Wait for ProxySQL admin to be ready
wait_for_ready() {
    local admin_port="$1"
    local max_wait=30
    local waited=0

    log_info "Waiting for ProxySQL admin to be ready on port $admin_port..."

    while [ $waited -lt $max_wait ]; do
        if PGPASSWORD="admin" psql -h 127.0.0.1 -p "$admin_port" -U admin -d main -c "SELECT 1" >/dev/null 2>&1; then
            log_info "ProxySQL admin is ready!"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    log_error "ProxySQL admin did not become ready within ${max_wait}s"
    return 1
}

# Start ProxySQL
cmd_start() {
    local config_file=""
    local data_dir="$DEFAULT_DATA_DIR"
    local admin_port="$DEFAULT_ADMIN_PORT"
    local proxy_port="$DEFAULT_PROXY_PORT"

    # Parse options
    while [ $# -gt 0 ]; do
        case "$1" in
        --config)
            config_file=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        --data-dir)
            data_dir=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        --admin-port)
            admin_port=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        --proxy-port)
            proxy_port=$(require_option_value "$1" "${2:-}")
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
        esac
    done

    check_binary
    kill_test_instances "$data_dir"

    # Create data directory (try sudo if regular rm fails due to root-owned files)
    if ! rm -rf "$data_dir" 2>/dev/null; then
        log_warn "Cannot remove $data_dir as user, trying with sudo..."
        sudo rm -rf "$data_dir" 2>/dev/null || true
    fi
    if [ -e "$data_dir" ]; then
        log_error "Failed to remove existing data dir: $data_dir"
        log_error "Please remove it manually (may require sudo) and retry."
        exit 1
    fi
    mkdir -p "$data_dir"

    # Create config if not provided
    if [ -z "$config_file" ]; then
        config_file="$data_dir/proxysql.cnf"
        log_info "Creating default config at $config_file"

        cat >"$config_file" <<EOF
datadir="$data_dir"
admin_variables=
{
    admin_credentials="admin:admin"
    mysql_ifaces="127.0.0.1:$DEFAULT_MYSQL_ADMIN_PORT"
    pgsql_ifaces="127.0.0.1:$admin_port"
}
mysql_variables=
{
    interfaces=""
}
pgsql_variables=
{
    threads=${PROXYSQL_PGSQL_THREADS:-4}
    interfaces="127.0.0.1:$proxy_port"
    monitor_username="postgres"
    monitor_password="postgres"
    polardb_consistency_mode="off"
    polardb_lag_ms=0
    polardb_lag_bytes=0
}
EOF
        if [ -n "$PROXYSQL_DEBUG" ] && [ "$PROXYSQL_DEBUG" != "0" ]; then
            # Ensure gdbg is enabled from config for builds without admin_variables table.
            sed -i "/admin_variables=/,/}/ s/}/    debug=1\\n}/" "$config_file"
        fi
    fi

    log_info "Starting ProxySQL..."
    log_info "  Binary: $PROXYSQL_BINARY"
    log_info "  Config: $config_file"
    log_info "  Data dir: $data_dir"
    log_info "  Admin port: $admin_port"
    log_info "  Proxy port: $proxy_port"

    if [ -n "$PROXYSQL_DEBUG" ] && [ "$PROXYSQL_DEBUG" != "0" ]; then
        log_info "  Debug: enabled (PROXYSQL_DEBUG=$PROXYSQL_DEBUG)"
        "$PROXYSQL_BINARY" -f -c "$config_file" -D "$data_dir" --debug="$PROXYSQL_DEBUG" >"$data_dir/proxysql.log" 2>&1 &
    else
        "$PROXYSQL_BINARY" -f -c "$config_file" -D "$data_dir" >"$data_dir/proxysql.log" 2>&1 &
    fi
    local pid=$!
    local pid_file
    pid_file=$(pid_file_for_data_dir "$data_dir")
    echo "$pid" >"$pid_file"

    log_info "ProxySQL started with PID: $pid"

    if wait_for_ready "$admin_port"; then
        log_info "ProxySQL is ready!"
        echo ""
        echo "Connection info:"
        echo "  Admin: PGPASSWORD=admin psql -h 127.0.0.1 -p $admin_port -U admin -d main"
        echo "  Proxy: PGPASSWORD=postgres psql -h 127.0.0.1 -p $proxy_port -U postgres -d postgres"
        echo "  Logs:  tail -f $data_dir/proxysql.log"
        return 0
    else
        log_error "ProxySQL failed to start properly"
        tail -20 "$data_dir/proxysql.log"
        return 1
    fi
}

# Stop ProxySQL
cmd_stop() {
    local data_dir
    data_dir=$(parse_data_dir_option "$@")

    log_info "Stopping ProxySQL for data dir: $data_dir"
    kill_test_instances "$data_dir"
    log_info "ProxySQL stopped"
}

# Status
cmd_status() {
    local data_dir pid_file
    data_dir=$(parse_data_dir_option "$@")
    pid_file=$(pid_file_for_data_dir "$data_dir")

    echo "=== ProxySQL Test Instance Status ==="
    echo "Data dir: $data_dir"

    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "Status: RUNNING (PID: $pid)"
        else
            echo "Status: STOPPED (stale PID file)"
        fi
    else
        echo "Status: STOPPED (no PID file)"
    fi

    echo ""
    echo "=== Matching ProxySQL Processes ==="
    local pids="" found=0
    pids=$(pgrep -f "^[^ ]*proxysql -f" 2>/dev/null || true)
    for pid in $pids; do
        local cmdline
        if proxysql_pid_matches_data_dir "$pid" "$data_dir"; then
            cmdline=$(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null || true)
            found=1
            printf '%s %s\n' "$pid" "$cmdline"
        fi
    done
    [ "$found" -eq 1 ] || echo "No matching test instance found"
}

# Main
case "${1:-}" in
start)
    shift
    cmd_start "$@"
    ;;
stop)
    shift
    cmd_stop "$@"
    ;;
status)
    shift
    cmd_status "$@"
    ;;
restart)
    shift
    restart_data_dir=$(proxysql_data_dir_from_start_args "$@")
    cmd_stop --data-dir "$restart_data_dir"
    sleep 2
    cmd_start "$@"
    ;;
*)
    echo "Usage: $0 {start|stop|status|restart} [options]"
    echo ""
    echo "Commands:"
    echo "  start   Start ProxySQL (kills existing test instances first)"
    echo "  stop    Stop ProxySQL test instance"
    echo "  status  Show status of ProxySQL instances"
    echo "  restart Stop then start ProxySQL"
    echo ""
    echo "Options:"
    echo "  --data-dir DIR     Use specified data directory (default: $DEFAULT_DATA_DIR)"
    echo ""
    echo "Options for start/restart only:"
    echo "  --config FILE      Use specified config file"
    echo "  --admin-port PORT  Admin port (default: $DEFAULT_ADMIN_PORT)"
    echo "  --proxy-port PORT  Proxy port (default: $DEFAULT_PROXY_PORT)"
    exit 1
    ;;
esac
