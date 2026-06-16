#!/usr/bin/env bash
#
# scenario_harness.sh - shared PolarDB scenario test harness
#
# Combines shared topology, ProxySQL, stats, logging, and scenario helpers.
#
# POSTPONED SURFACE:
# CSN and transaction-split helper blocks are kept as near-term scaffolding, not
# as active v1 coverage. The committed v1 branch exercises off/lsn/primary LSN
# session consistency only. Do not run CONSISTENCY_MODE=2/4 or XACT_SPLIT=1 from
# this harness until the matching code path and assertions are restored.
#
# ACTIVE SLICE (what the committed TAP scripts actually source from here):
#   init_logging / stop_logging, start_proxysql / stop_proxysql,
#   enable_replay_lag / disable_replay_lag, wait_for_replica_lsn_catchup,
#   get_counter, extract_logs, setup_test_table, start_wal_generator /
#   stop_wal_generator, proxysql_admin (+ their logging/topology dependencies).
#   Everything else below is reference material or postponed scaffold.
#
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# DEAD/POSTPONED REFERENCE (counter catalogue) -- documentation only, no code.
# This ~160-line block enumerates stats_pgsql_global counters with their C++
# source-line citations. It is NOT consulted by the active TAP slice above; it
# documents counters expected by the postponed CSN/transaction-split feature
# work. Treat the lib/*.cpp:NNN line numbers as stale hints, not guarantees.
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
#
# =============================================================================
# STAT PRESET FORMAT
# =============================================================================
#
# Each stat definition is a pipe-separated string:
#
#   "COUNTER_NAME|success_expect|failure_expect|description"
#
# Where:
#   COUNTER_NAME    - ProxySQL stats variable name (from stats_pgsql_global)
#   success_expect  - Expected delta when EXPECT_OUTCOME=success
#   failure_expect  - Expected delta when EXPECT_OUTCOME=failure
#   description     - Human-readable description for output
#
# Expectation values:
#   0   - Delta must be exactly 0
#   +   - Delta must be >= 1 (counter increased)
#   -   - Delta must be <= -1 (counter decreased)
#   N   - Delta must equal N (specific number)
#   ?   - Skip this check (don't verify)
#
# =============================================================================
# HOW TO ADD NEW STATS
# =============================================================================
#
# 1. Find the counter name in ProxySQL:
#      SELECT * FROM stats_pgsql_global WHERE Variable_Name LIKE '%polardb%';
#
# 2. Add to appropriate preset array:
#      LSN_STATS+=("PolarDB_New_Counter|+|0|Description of counter")
#
# 3. Or create new preset for new scenario:
#      MY_SCENARIO_STATS=(
#          "Counter1|+|0|Description"
#          "Counter2|0|+|Description"
#      )
#
# 4. Add to verify section in run_consistency_test():
#      case "$CONSISTENCY_MODE" in
#          X) verify_preset "$outcome" "${MY_SCENARIO_STATS[@]}" ;;
#      esac
#
# =============================================================================
# AVAILABLE COUNTERS (stats_pgsql_global)
# =============================================================================
#
# Query: SELECT Variable_Name, Variable_Value FROM stats_pgsql_global;
#
# Stats are accumulated from two sources:
#   1. PgHGM->status struct  (include/PgSQL_HostGroups_Manager.h:760-879)
#   2. GloPgMon counters     (include/PgSQL_Monitor.hpp:36-43)
#
# Exposed via SQL3_GlobalStatus() in lib/PgSQL_Thread.cpp:4201-4815
#
# =============================================================================
# GENERAL PROXYSQL STATS
# =============================================================================
# Source: PgSQL_Thread.cpp:4214-4430 (computed values)
#
#   ProxySQL_Uptime                    - Uptime in seconds
#   Active_Transactions                - Currently active transactions
#   Servers_table_version              - Servers table version
#   PgSQL_Thread_Workers               - Number of worker threads
#
# =============================================================================
# CLIENT CONNECTION STATS
# =============================================================================
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:764-766)
#
#   Client_Connections_aborted         - Aborted client connections
#   Client_Connections_connected       - Currently connected clients
#   Client_Connections_created         - Total connections created
#   Client_Connections_non_idle        - Non-idle connections (IDLE_THREADS)
#
# =============================================================================
# SERVER CONNECTION STATS
# =============================================================================
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:767-770)
#
#   Server_Connections_aborted         - Aborted backend connections
#   Server_Connections_connected       - Currently connected backends
#   Server_Connections_created         - Total backend connections created
#   Server_Connections_delayed         - Delayed backend connections
#
# =============================================================================
# ACCESS DENIED STATS
# =============================================================================
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:789-791)
#
#   Access_Denied_Wrong_Password       - Auth failures: wrong password
#   Access_Denied_Max_Connections      - Auth failures: max connections
#   Access_Denied_Max_User_Connections - Auth failures: max user connections
#
# =============================================================================
# MEMORY/BUFFER STATS
# =============================================================================
# Source: PgSQL_Thread.cpp:4281-4294 (computed from sessions)
#
#   pgsql_backend_buffers_bytes        - Backend buffer memory
#   pgsql_frontend_buffers_bytes       - Frontend buffer memory
#   pgsql_session_internal_bytes       - Session internal memory
#
# =============================================================================
# TRANSACTION STATS
# =============================================================================
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:777-792)
#
#   Commit                             - COMMIT commands
#   Commit_filtered                    - Filtered COMMIT commands
#   Rollback                           - ROLLBACK commands
#   Rollback_filtered                  - Filtered ROLLBACK commands
#   Backend_reset_connection           - Backend connection resets
#   Backend_set_client_encoding        - Backend encoding changes
#   Frontend_set_client_encoding       - Frontend encoding changes
#   Selects_for_update__autocommit0    - SELECT FOR UPDATE in autocommit=0
#
# =============================================================================
# MONITOR STATS
# =============================================================================
# Source: GloPgMon (include/PgSQL_Monitor.hpp:36-43)
#
#   PgSQL_Monitor_connect_check_OK     - Successful connect checks
#   PgSQL_Monitor_connect_check_ERR    - Failed connect checks
#   PgSQL_Monitor_ping_check_OK        - Successful ping checks
#   PgSQL_Monitor_ping_check_ERR       - Failed ping checks
#   PgSQL_Monitor_read_only_check_OK   - Successful read_only checks
#   PgSQL_Monitor_read_only_check_ERR  - Failed read_only checks
#   PgSQL_Monitor_ssl_connections_OK   - Successful SSL connections
#   PgSQL_Monitor_non_ssl_connections_OK - Successful non-SSL connections
#
# =============================================================================
# POLARDB SPLIT READ STATS
# =============================================================================
# Postponed scaffold: this block documents counters expected by the near-term
# CSN/transaction-split feature work. The active v1 harness must not assert or
# require these counters unless that feature is restored.
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:795-879)
# All PolarDB stats are atomic counters for thread-safe access.
#
# --- Split Execution (lines 817-821) ---
#   PolarDB_Split_Reads_Total          - Total split read attempts
#   PolarDB_Split_Reads_Success        - Successful splits to replica
#   PolarDB_Split_Reads_Fallback       - Fallback to primary
#   PolarDB_Split_Reads_Error          - Split read errors
#   PolarDB_Split_No_Backend           - No backend available
#
# --- Split Decision (lines 811-815) ---
#   PolarDB_Queries_In_Splittable_Txn  - Queries in splittable transactions
#   PolarDB_Queries_Split_Eligible     - Passed all split checks
#   PolarDB_Split_Rejected_Not_Select  - Rejected: not SELECT
#   PolarDB_Split_Rejected_For_Update  - Rejected: FOR UPDATE/SHARE
#
# --- Transaction Lifecycle (lines 801-809) ---
#   PolarDB_XIDs_Received              - XID parse events from primary
#   PolarDB_Txn_Became_Splittable      - Transactions became splittable
#   PolarDB_Txn_Lost_Splittable        - Lost splittable (write after read)
#   PolarDB_Txn_Committed_With_Split   - Committed with split activity
#   PolarDB_Txn_Committed_No_Split     - Committed without splits
#
# --- LSN Tracking (lines 803, 832-835, 845-846) ---
#   PolarDB_Server_LSN_Updates_From_RFQ - per-server LSN cache update from query RFQ
#   PolarDB_LSN_Updates_From_Monitor   - LSN from monitor thread
#   PolarDB_Monitor_Health_Invalid_Role - Monitor role is not a routed primary/reader role
#   PolarDB_Monitor_Health_Invalid_Values - Monitor availability or LSN text was invalid
#   PolarDB_LSN_Stale_Count            - Stale LSN detections
#   PolarDB_Write_Missing_LSN          - Writer RFQ missing LSN fail-safe events
#   PolarDB_Split_LSN_Wait_Count       - Number of LSN waits
#   PolarDB_Split_LSN_Wait_Sum_Us      - Total LSN wait time (us)
#
# --- CSN Tracking (lines 848-853) ---
#   PolarDB_CSN_Updates_From_Query     - CSN from INSERT/UPDATE/DELETE
#   PolarDB_CSN_Updates_From_Monitor   - CSN from monitor thread
#   PolarDB_CSN_Stale_Count            - Stale CSN detections
#   (PolarDB_Split_CSN_Wait_Count removed - split always uses LSN)
#   PolarDB_Global_CSN_Routing         - Global CSN routing (mode=4)
#
# --- Connection Pool (lines 823-830, 855-857) ---
#   PolarDB_Split_Pool_Hit             - Connection from pool
#   PolarDB_Split_Pool_Empty           - Pool was empty
#   PolarDB_Split_Pool_Contention      - Pool lock contention
#   PolarDB_Split_Conn_Reused          - Reused existing connection
#   PolarDB_Split_Conn_Cleanup_Success - Cleanup succeeded
#   PolarDB_Split_Conn_Cleanup_Failed  - Cleanup failed
#   PolarDB_Reader_Pool_Conns_Free     - Free pool connections
#   PolarDB_Reader_Pool_Conns_Used     - Used pool connections
#
# --- Errors (lines 837-842, 859-861) ---
#   PolarDB_Split_Error_Connection_Lost - Connection lost
#   PolarDB_Split_Error_Query_Failed   - Query execution failed
#   PolarDB_Split_Error_Timeout        - General timeout
#   PolarDB_Split_Error_LSN_Wait_Timeout - LSN wait timeout
#   PolarDB_Split_Send_Failed          - Failed to send query
#
# --- Warmup (lines 867-871) ---
#   PolarDB_Split_Warmup_Requested     - Warmup requests queued
#   PolarDB_Split_Warmup_Created       - Warmup connections created
#   PolarDB_Split_Warmup_Failed        - Warmup failures
#   PolarDB_Warmup_Pending             - Currently pending warmups
#
# --- Latency (lines 863-865) ---
#   PolarDB_Split_Latency_Sum_Us       - Sum of split latencies (us)
#   PolarDB_Split_Latency_Count        - Number of latency samples
#
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# END DEAD/POSTPONED REFERENCE (counter catalogue).
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# =============================================================================

if [ -n "${POLARDB_HARNESS_LOADED:-}" ]; then
    return 0
fi
POLARDB_HARNESS_LOADED=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARDB_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=../common/env.sh
source "$POLARDB_TEST_DIR/common/env.sh"

#------------------------------------------------------------------------------
# COMMON TEST HELPERS
#------------------------------------------------------------------------------

# Keep all psql invocation details behind functions. Older versions of this
# harness used string-built P/R/PROXY commands and eval; these wrappers preserve
# the same behavior without word-splitting surprises. Primary/replica helpers are
# semantic aliases over the shared direct-endpoint implementation from env.sh.
polardb_primary_psql() { polardb_direct_psql "$PRIMARY_HOST" "$PRIMARY_PORT" "$@"; }
polardb_replica_psql() { polardb_direct_psql "$REPLICA_HOST" "$REPLICA_PORT" "$@"; }
polardb_primary_sql() { polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "$1"; }
polardb_replica_sql() { polardb_direct_sql "$REPLICA_HOST" "$REPLICA_PORT" "$1"; }

polardb_proxy_psql() {
    PGPASSWORD="$PGPASSWORD" PGSSLMODE="$PGSSLMODE" psql \
        -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
        -U "$PGUSER" -d "$PGDB" \
        "$@"
}

polardb_proxy_sql() { polardb_proxy_psql -A -t -q -v ON_ERROR_STOP=1 -c "$1"; }

polardb_libpq_ld_path() {
    local libpq_path="$PROXYSQL_ROOT/deps/postgresql/postgresql/src/interfaces/libpq"
    if [ -n "${LD_LIBRARY_PATH:-}" ]; then
        printf '%s:%s\n' "$libpq_path" "$LD_LIBRARY_PATH"
    else
        printf '%s\n' "$libpq_path"
    fi
}

polardb_pgbench_script() {
    local protocol_mode="$1"
    local sql_file="$2"

    LD_LIBRARY_PATH="$(polardb_libpq_ld_path)" \
    PGPASSWORD="${PGPASSWORD:-postgres}" PGSSLMODE="$PGSSLMODE" "$PGBENCH_BIN" \
        -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
        -U "${PGUSER:-postgres}" -d "${PGDB:-postgres}" \
        -n -t 1 -c 1 -M "$protocol_mode" \
        -f "$sql_file"
}

# State tracking
WAL_GEN_PID=""
TXN_PID=""
POLARDB_BG_PIDS=()
TEST_TABLE="consistency_test"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

#------------------------------------------------------------------------------
# Output Functions
#------------------------------------------------------------------------------

ts() { date "+%H:%M:%S.%3N"; }

log_header() {
    echo ""
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================${NC}"
}

log_section() {
    echo ""
    echo -e "${CYAN}--- $1 ---${NC}"
}

log_info() {
    echo -e "[$(ts)] $1"
}

log_success() {
    echo -e "${GREEN}[$(ts)] ✓ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}[$(ts)] ⚠ $1${NC}"
}

log_error() {
    echo -e "${RED}[$(ts)] ✗ $1${NC}"
}

log_result() {
    local expected="$1"
    local actual="$2"
    local msg="$3"

    if [ "$expected" = "$actual" ]; then
        log_success "$msg (expected: $expected, got: $actual)"
        return 0
    else
        log_error "$msg (expected: $expected, got: $actual)"
        return 1
    fi
}

#------------------------------------------------------------------------------
# Cluster Topology Auto-Detection
#------------------------------------------------------------------------------

# Select writer/reader endpoints from .env-provided POLARDB_ENDPOINTS or from
# manual PRIMARY_*/REPLICA_* variables. The committed scripts do not embed lab
# addresses; environment-specific details belong in test/polardb/.env.
detect_cluster_topology() {
    if polardb_detect_topology; then
        log_info "Topology: writer=$PRIMARY_HOST:$PRIMARY_PORT reader=$REPLICA_HOST:$REPLICA_PORT"
        return 0
    fi
    log_error "Topology detection failed; set POLARDB_ENDPOINTS or PRIMARY_/REPLICA_ variables"
    return 1
}

if [ "${POLARDB_AUTODETECT:-1}" = "1" ]; then
    detect_cluster_topology || true
fi

polardb_register_bg_pid() {
    local pid="$1"
    [ -n "$pid" ] || return 0
    POLARDB_BG_PIDS+=("$pid")
}

polardb_forget_bg_pid() {
    local target="$1"
    local kept=()
    local pid

    for pid in "${POLARDB_BG_PIDS[@]:-}"; do
        [ "$pid" = "$target" ] || kept+=("$pid")
    done
    POLARDB_BG_PIDS=("${kept[@]}")
}

polardb_stop_pid() {
    local pid="$1"
    [ -n "$pid" ] || return 0
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
}

polardb_stop_registered_pids() {
    local pid

    for pid in "${POLARDB_BG_PIDS[@]:-}"; do
        polardb_stop_pid "$pid"
    done
    POLARDB_BG_PIDS=()
}

#------------------------------------------------------------------------------
# Setup/Teardown Functions
#------------------------------------------------------------------------------

setup_test_table() {
    log_info "Creating test table $TEST_TABLE on primary..."
    polardb_primary_sql "DROP TABLE IF EXISTS $TEST_TABLE; CREATE TABLE $TEST_TABLE(id int PRIMARY KEY, data text, ts timestamp default now());" >/dev/null
}

cleanup_all() {
    log_info "Cleaning up..."

    # Stop only harness-owned background processes. Do not kill every job in the
    # caller shell: tests can be sourced or run under wrappers with their own jobs.
    stop_wal_generator
    stop_uncommitted_transaction
    polardb_stop_registered_pids

    # Reset replica settings
    polardb_set_local_sighup_guc polar_replay_min_lag_size 0 >/dev/null 2>&1 || true

    # Drop test table
    polardb_primary_sql "DROP TABLE IF EXISTS $TEST_TABLE;" 2>/dev/null || true
}

#------------------------------------------------------------------------------
# PolarDB Global Settings (direct to primary/replica)
#------------------------------------------------------------------------------

polardb_dcs_set() {
    polardb_run_dcs "$@"
}

polardb_set_local_sighup_guc() {
    local key="$1"
    local value="$2"

    if [ "${POLARDB_DCS_MODE:-docker}" != "docker" ]; then
        polardb_dcs_set "$key=$value"
        return $?
    fi

    if [ -z "$POLARDB_DOCKER_CONTAINER" ]; then
        echo "POLARDB_DOCKER_CONTAINER is required when POLARDB_DCS_MODE=docker." >&2
        return 1
    fi

    polardb_maybe_sudo "$POLARDB_DOCKER_SUDO" "$POLARDB_DOCKER_BIN" exec \
        -e POLAR_GUC_KEY="$key" \
        -e POLAR_GUC_VALUE="$value" \
        "$POLARDB_DOCKER_CONTAINER" bash -lc '
set -euo pipefail
f=/etc/polardb/patroni.scope.d/polardb-cluster/instances/instance1/polardb.d/polardb-local.conf
tmp="${f}.tmp.$$"
mkdir -p "$(dirname "$f")"
touch "$f"
grep -vE "^[[:space:]]*${POLAR_GUC_KEY}[[:space:]]*=" "$f" > "$tmp" || true
printf "%s = %s\n" "$POLAR_GUC_KEY" "$POLAR_GUC_VALUE" >> "$tmp"
chown postgres:postgres "$tmp"
chmod 0644 "$tmp"
mv "$tmp" "$f"
for member in instance1 instance2 instance3; do
    polarctl reload "$member" --force >/dev/null
done
'
}

wait_for_guc_value() {
    local host="$1"
    local port="$2"
    local name="$3"
    local expected="$4"
    local timeout_sec="${5:-10}"
    local start_ts current

    start_ts=$(date +%s)
    while true; do
        current=$(polardb_direct_sql "$host" "$port" "SELECT current_setting('$name', true);" 2>/dev/null | tr -d '[:space:]')
        if [ "$current" = "$expected" ]; then
            return 0
        fi
        if [[ "$expected" =~ ^[0-9]+ms$ ]]; then
            local expected_ms="${expected%ms}"
            if [ "$current" = "${expected_ms}ms" ]; then
                return 0
            fi
            if [ $((expected_ms % 1000)) -eq 0 ] && [ "$current" = "$((expected_ms / 1000))s" ]; then
                return 0
            fi
        fi
        if [[ "$expected" =~ ^[0-9]+$ ]] && [ "$current" = "$expected" ]; then
            return 0
        fi
        if [ $(( $(date +%s) - start_ts )) -ge "$timeout_sec" ]; then
            log_warning "Timed out waiting for $name=$expected on $host:$port (current=${current:-<empty>})"
            return 1
        fi
        sleep 0.5
    done
}

set_polar_proxy_wait_timeout_ms() {
    local timeout_ms="$1"
    if [ -z "$timeout_ms" ] || [ "$timeout_ms" = "0" ]; then
        log_info "polar_proxy_wait_timeout_ms not set (timeout_ms=$timeout_ms)"
        return 0
    fi

    log_info "Setting polar_proxy_wait_timeout_ms=$timeout_ms through Patroni DCS"
    polardb_dcs_set "polar_proxy_wait_timeout_ms=$timeout_ms" >/dev/null || return 1
    wait_for_guc_value "$PRIMARY_HOST" "$PRIMARY_PORT" polar_proxy_wait_timeout_ms "${timeout_ms}ms" 10 || return 1
    wait_for_guc_value "$REPLICA_HOST" "$REPLICA_PORT" polar_proxy_wait_timeout_ms "${timeout_ms}ms" 10 || return 1
}

get_polar_query_delay_us() {
    local target="$1"
    local host port
    local raw

    case "$target" in
        primary|writer) host="$PRIMARY_HOST"; port="$PRIMARY_PORT" ;;
        replica|reader) host="$REPLICA_HOST"; port="$REPLICA_PORT" ;;
        *) return 1 ;;
    esac

    raw=$(polardb_direct_sql "$host" "$port" "SHOW polar_query_delay_us;" 2>/dev/null | tr -d '[:space:]')
    normalize_duration_to_us "$raw"
}

normalize_duration_to_us() {
    local raw="$1"

    case "$raw" in
        "") echo "" ;;
        *us) awk -v v="${raw%us}" 'BEGIN { printf "%d\n", v }' ;;
        *ms) awk -v v="${raw%ms}" 'BEGIN { printf "%d\n", v * 1000 }' ;;
        *s)  awk -v v="${raw%s}"  'BEGIN { printf "%d\n", v * 1000000 }' ;;
        *)   awk -v v="$raw"      'BEGIN { printf "%d\n", v }' ;;
    esac
}

wait_for_polar_query_delay_us() {
    local host="$1"
    local port="$2"
    local expected_us="$3"
    local timeout_sec="${4:-15}"
    local start_ts current

    start_ts=$(date +%s)
    while true; do
        current=$(polardb_direct_sql "$host" "$port" "SHOW polar_query_delay_us;" 2>/dev/null | tr -d '[:space:]')
        current=$(normalize_duration_to_us "$current")
        if [ "$current" = "$expected_us" ]; then
            return 0
        fi
        if [ $(( $(date +%s) - start_ts )) -ge "$timeout_sec" ]; then
            log_warning "Timed out waiting for polar_query_delay_us=$expected_us on $host:$port (current=${current:-<empty>})"
            return 1
        fi
        sleep 0.5
    done
}

set_polar_query_delay_us() {
    local delay_us="$1"

    log_info "Setting polar_query_delay_us=$delay_us through Patroni DCS"
    polardb_dcs_set "polar_query_delay_us=$delay_us" >/dev/null || return 1
    wait_for_polar_query_delay_us "$PRIMARY_HOST" "$PRIMARY_PORT" "$delay_us" 15 || return 1
    wait_for_polar_query_delay_us "$REPLICA_HOST" "$REPLICA_PORT" "$delay_us" 15 || return 1
}

reset_polar_query_delay_us() {
    set_polar_query_delay_us 0
}

#------------------------------------------------------------------------------
# WAL/Lag Control Functions
#------------------------------------------------------------------------------

set_replay_lag_guc() {
    local lag_bytes="$1"
    local attempts="${POLARDB_REPLAY_LAG_SET_ATTEMPTS:-3}"
    local delay_sec="${POLARDB_REPLAY_LAG_SET_RETRY_DELAY:-1}"
    local attempt

    for ((attempt = 1; attempt <= attempts; attempt++)); do
        if polardb_set_local_sighup_guc polar_replay_min_lag_size "$lag_bytes" >/dev/null &&
                wait_for_guc_value "$REPLICA_HOST" "$REPLICA_PORT" polar_replay_min_lag_size "$lag_bytes" 15; then
            return 0
        fi
        if [ "$attempt" -lt "$attempts" ]; then
            log_warning "polar_replay_min_lag_size=$lag_bytes attempt $attempt/$attempts failed; retrying"
            sleep "$delay_sec"
        fi
    done

    return 1
}

enable_replay_lag() {
    local lag_bytes="${1:-5000}"
    log_info "Enabling replay lag through managed local config: $lag_bytes bytes"
    set_replay_lag_guc "$lag_bytes" || return 1
    sleep 1
}

disable_replay_lag() {
    log_info "Disabling replay lag through managed local config"
    set_replay_lag_guc 0 || return 1
    sleep 1
}

start_wal_generator() {
    local delay_sec="${1:-0}"
    local max_iters="${2:-0}"  # 0 = run forever
    log_info "Starting WAL generator (delay=${delay_sec}s, max_iters=${max_iters})..."
    (
        [ "$delay_sec" != "0" ] && sleep "$delay_sec"
        local i=0
        while true; do
            polardb_primary_sql "INSERT INTO $TEST_TABLE SELECT 0, repeat('x', 200) ON CONFLICT (id) DO UPDATE SET data=repeat('x',200);" >/dev/null 2>&1
            i=$((i+1))
            if [ "$max_iters" -gt 0 ] && [ "$i" -ge "$max_iters" ]; then
                break
            fi
            sleep 0.05
        done
    ) &
    WAL_GEN_PID=$!
}

stop_wal_generator() {
    if [ -n "$WAL_GEN_PID" ]; then
        log_info "Stopping WAL generator..."
        kill "$WAL_GEN_PID" 2>/dev/null
        wait "$WAL_GEN_PID" 2>/dev/null || true
        polardb_forget_bg_pid "$WAL_GEN_PID"
        WAL_GEN_PID=""
    fi
}

#------------------------------------------------------------------------------
# LSN/CSN Functions (direct to PolarDB)
#------------------------------------------------------------------------------

# Active LSN getters (used by wait_for_replica_lsn_catchup, the LSN TAP slice).
get_primary_lsn() { polardb_primary_sql "SELECT pg_current_wal_lsn();"; }
get_replica_replay_lsn() { polardb_replica_sql "SELECT pg_last_wal_replay_lsn();"; }
get_replica_receive_lsn() { polardb_replica_sql "SELECT pg_last_wal_receive_lsn();"; }
# POSTPONED scaffold (CSN): no active TAP slice reads these; kept for the
# postponed CSN consistency feature work alongside wait_for_replica_csn_catchup.
get_primary_csn() { polardb_primary_sql "SELECT polar_get_csn();" 2>/dev/null; }
get_replica_csn() { polardb_replica_sql "SELECT polar_get_csn();" 2>/dev/null; }

#------------------------------------------------------------------------------
# Catch-up Helpers (replica)
#------------------------------------------------------------------------------

wait_for_replica_lsn_catchup() {
    local timeout_sec="${1:-10}"
    local start_ts=$(date +%s)
    while true; do
        local p=$(get_primary_lsn)
        local r=$(get_replica_replay_lsn)
        local pd=$(lsn_to_decimal "$p")
        local rd=$(lsn_to_decimal "$r")
        [ "$rd" -ge "$pd" ] && return 0
        if [ $(( $(date +%s) - start_ts )) -ge "$timeout_sec" ]; then
            return 1
        fi
        sleep 0.2
    done
}

# POSTPONED scaffold (CSN catch-up): inactive in v1; restored with CSN feature.
wait_for_replica_csn_catchup() {
    local timeout_sec="${1:-10}"
    local start_ts=$(date +%s)
    while true; do
        local p=$(get_primary_csn)
        local r=$(get_replica_csn)
        [ -n "$p" ] && [ -n "$r" ] && [ "$r" -ge "$p" ] && return 0
        if [ $(( $(date +%s) - start_ts )) -ge "$timeout_sec" ]; then
            return 1
        fi
        sleep 0.2
    done
}

lsn_to_decimal() {
    local lsn="$1"
    [ -z "$lsn" ] && echo "0" && return
    local hi=$(echo "$lsn" | cut -d'/' -f1)
    local lo=$(echo "$lsn" | cut -d'/' -f2)
    printf "%d" $((16#$hi * 4294967296 + 16#$lo))
}

#------------------------------------------------------------------------------
# ProxySQL Admin Functions
#------------------------------------------------------------------------------

proxysql_admin() {
    local query="$1"
    PGPASSWORD=admin PGSSLMODE="$PGSSLMODE" psql \
        -h "$PROXYSQL_HOST" -p "$PROXYSQL_ADMIN_PORT" \
        -U admin -d main \
        -A -t -c "$query"
}

set_consistency_mode() {
    local mode="$1"  # 0=off, 1=lsn, 2=csn, 3=primary, 4=csn_global
    log_info "Setting ProxySQL consistency mode to $mode"
    proxysql_admin "UPDATE global_variables SET variable_value='$mode' WHERE variable_name='pgsql-polardb_consistency_mode'; LOAD PGSQL VARIABLES TO RUNTIME;"
}

# Postponed CSN/split scaffold. Kept for the next feature round, but inactive in
# v1 because this branch has no pgsql-polardb_split_mode runtime surface.
# Disabling split is therefore a no-op; enabling it fails loudly so a postponed
# scenario cannot accidentally look active.
set_split_mode() {
    local enabled="$1"  # 0=disabled, 1=enabled
    if [ "$enabled" = "0" ]; then
        log_info "Split mode disabled (v1 no-op; split support postponed)"
        return 0
    fi
    log_error "Transaction split support is postponed in this v1 harness"
    return 1
}

# Postponed transaction-split hostgroup flag. Disabling is kept as a harmless
# compatibility no-op for existing LSN cases; enabling is rejected until the
# transaction-split feature and assertions are restored.
set_txn_split_enabled() {
    local writer_hg="$1"
    local enabled="$2"  # 0=disabled, 1=enabled
    if [ "$enabled" = "0" ]; then
        log_info "txn_split_enabled disabled for writer_hostgroup=$writer_hg (v1 no-op)"
        return 0
    fi
    log_error "txn_split_enabled=$enabled requested for writer_hostgroup=$writer_hg, but transaction split is postponed in v1"
    return 1
}

# Set ProxySQL lag threshold (pgsql-polardb_lag_bytes)
set_lag_threshold() {
    local lag_bytes="$1"
    log_info "Setting ProxySQL lag threshold to ${lag_bytes} bytes"
    proxysql_admin "UPDATE global_variables SET variable_value='$lag_bytes' WHERE variable_name='pgsql-polardb_lag_bytes'; LOAD PGSQL VARIABLES TO RUNTIME;"
}

# Show current ProxySQL PolarDB configuration
show_proxysql_config() {
    log_section "ProxySQL PolarDB Configuration"
    echo "Variables:"
    proxysql_admin "SELECT variable_name, variable_value FROM global_variables WHERE variable_name LIKE 'pgsql-polardb%';"
    echo ""
    echo "Hostgroups:"
    proxysql_admin "SELECT writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms FROM runtime_pgsql_replication_hostgroups;"
}

# Configure ProxySQL for specific test scenario
# Usage: configure_proxysql <scenario>
#   eventual      - no consistency, no split
#   session_lsn   - LSN-based session consistency
#   session_csn   - CSN-based session consistency
#   global_csn    - Global CSN consistency
#   split_auto    - Automatic transaction split
configure_proxysql() {
    local scenario="$1"
    local writer_hg="${2:-$POLARDB_WRITER_HG}"

    log_section "Configuring ProxySQL for: $scenario"

    case "$scenario" in
        eventual)
            set_consistency_mode 0
            set_split_mode 0
            set_txn_split_enabled "$writer_hg" 0
            ;;
        session_lsn)
            set_consistency_mode 1
            set_split_mode 0
            set_txn_split_enabled "$writer_hg" 0
            ;;
        session_csn)
            set_consistency_mode 2
            set_split_mode 0
            set_txn_split_enabled "$writer_hg" 0
            ;;
        global_csn)
            set_consistency_mode 4
            set_split_mode 0
            set_txn_split_enabled "$writer_hg" 0
            ;;
        primary_only)
            set_consistency_mode 3
            set_split_mode 0
            set_txn_split_enabled "$writer_hg" 0
            ;;
        split_auto)
            set_consistency_mode 2  # CSN for committed data consistency
            set_split_mode 1
            set_txn_split_enabled "$writer_hg" 1
            ;;
        split_only)
            set_consistency_mode 0  # No wait for committed
            set_split_mode 1
            set_txn_split_enabled "$writer_hg" 1
            ;;
        *)
            log_error "Unknown scenario: $scenario"
            return 1
            ;;
    esac

    show_proxysql_config
}

get_split_stats() {
    proxysql_admin "SELECT Variable_Name, Variable_Value FROM stats_pgsql_global WHERE Variable_Name LIKE '%polardb%';"
}

#------------------------------------------------------------------------------
# Counter Verification Functions
#------------------------------------------------------------------------------

# Get single counter value
get_counter() {
    local name="$1"
    local val=$(proxysql_admin "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$name';" | tr -d ' \n')
    echo "${val:-0}"
}

# Snapshot all PolarDB counters
snapshot_counters() {
    local prefix="${1:-}"
    export ${prefix}SPLIT_TOTAL=$(get_counter "PolarDB_Split_Reads_Total")
    export ${prefix}SPLIT_SUCCESS=$(get_counter "PolarDB_Split_Reads_Success")
    export ${prefix}SPLIT_FALLBACK=$(get_counter "PolarDB_Split_Reads_Fallback")
    export ${prefix}SPLIT_ERROR=$(get_counter "PolarDB_Split_Reads_Error")
    export ${prefix}LSN_WAIT=$(get_counter "PolarDB_Split_LSN_Wait_Count")
    export ${prefix}XIDS_RECV=$(get_counter "PolarDB_XIDs_Received")
    export ${prefix}TXN_SPLITTABLE=$(get_counter "PolarDB_Txn_Became_Splittable")
    export ${prefix}CSN_UPDATES=$(get_counter "PolarDB_CSN_Updates_From_Query")
}

# Compare counter delta and assert
assert_counter_delta() {
    local counter_name="$1"
    local expected_min="$2"
    local expected_max="${3:-999999}"
    local before_var="BEFORE_${counter_name}"
    local after_val=$(get_counter "PolarDB_${counter_name}")
    local before_val="${!before_var:-0}"
    local delta=$((after_val - before_val))

    if [ "$delta" -ge "$expected_min" ] && [ "$delta" -le "$expected_max" ]; then
        log_success "Counter $counter_name: delta=$delta (expected: >=$expected_min)"
        return 0
    else
        log_error "Counter $counter_name: delta=$delta (expected: >=$expected_min)"
        return 1
    fi
}

# Assert counter did NOT change
assert_counter_unchanged() {
    local counter_name="$1"
    assert_counter_delta "$counter_name" 0 0
}

# Print counter diff
print_counter_diff() {
    log_section "Counter Changes"
    local counters="Split_Reads_Total Split_Reads_Success Split_Reads_Fallback Split_Reads_Error"
    counters="$counters Split_LSN_Wait_Count XIDs_Received Txn_Became_Splittable"

    for c in $counters; do
        local before_var="BEFORE_${c}"
        local after=$(get_counter "PolarDB_${c}")
        local before="${!before_var:-0}"
        local delta=$((after - before))
        if [ "$delta" -ne 0 ]; then
            log_info "$c: $before -> $after (delta: $delta)"
        fi
    done
}

#------------------------------------------------------------------------------
# Query Execution Functions via ProxySQL
#------------------------------------------------------------------------------

# Execute query via ProxySQL (no special wait settings)
query_proxy() {
    local query="$1"
    polardb_proxy_sql "$query"
}

# Postponed CSN scaffold. Kept for the next feature round; v1 scripts should not
# call this helper.
query_proxy_with_csn_wait() {
    local csn="$1"
    local query="$2"
    local mode="${3:-best_effort}"
    local timeout="${4:-5000}"

    polardb_proxy_sql "
        SET polar_consistency_mode = '$mode';
        SET polar_proxy_wait_timeout_ms = $timeout;
        SET polar_wait_csn = '$csn';
        $query
    " 2>&1
}

# Execute query via ProxySQL with session-level LSN wait
query_proxy_with_lsn_wait() {
    local lsn="$1"
    local query="$2"
    local mode="${3:-best_effort}"
    local timeout="${4:-5000}"
    local xids="${5:-1,0}"

    polardb_proxy_sql "
        SET polar_consistency_mode = '$mode';
        SET polar_proxy_wait_timeout_ms = $timeout;
        SET polar_xact_split_xids = '$xids';
        SET polar_xact_split_wait_lsn = '$lsn';
        $query
    " 2>&1
}

# Postponed transaction-split scaffold. Kept for the next feature round; v1
# scripts should not call this helper.
# Split always uses LSN wait (CSN doesn't advance mid-transaction)
query_proxy_with_split() {
    local xids="$1"
    local lsn="$2"
    local query="$3"
    local mode="${4:-best_effort}"
    local timeout="${5:-5000}"

    polardb_proxy_sql "
        SET polar_consistency_mode = '$mode';
        SET polar_proxy_wait_timeout_ms = $timeout;
        SET polar_xact_split_xids = '$xids';
        SET polar_xact_split_wait_lsn = '$lsn';
        $query
    " 2>&1
}

#------------------------------------------------------------------------------
# Transaction Functions
#------------------------------------------------------------------------------

# Start a long-running transaction with INSERT
start_uncommitted_transaction() {
    local id="$1"
    local sleep_time="${2:-15}"

    log_info "Starting uncommitted transaction (id=$id, sleep=${sleep_time}s)..."
    (
        polardb_primary_sql "BEGIN; INSERT INTO $TEST_TABLE VALUES ($id, 'uncommitted_data'); SELECT pg_sleep($sleep_time); COMMIT;"
    ) &
    TXN_PID=$!
    polardb_register_bg_pid "$TXN_PID"
    sleep 2
}

# Get XID of active transaction
get_active_xid() {
    polardb_primary_sql "SELECT backend_xid FROM pg_stat_activity WHERE state='active' AND query LIKE '%pg_sleep%' LIMIT 1;"
}

# Stop uncommitted transaction
stop_uncommitted_transaction() {
    if [ -n "$TXN_PID" ]; then
        log_info "Stopping uncommitted transaction..."
        kill "$TXN_PID" 2>/dev/null
        wait "$TXN_PID" 2>/dev/null || true
        polardb_forget_bg_pid "$TXN_PID"
        TXN_PID=""
    fi
}

#------------------------------------------------------------------------------
# Assertion Functions
#------------------------------------------------------------------------------

assert_count() {
    local expected="$1"
    local actual="$2"
    local msg="$3"

    if [ "$expected" = "$actual" ]; then
        log_success "ASSERT PASSED: $msg (count=$actual)"
        return 0
    else
        log_error "ASSERT FAILED: $msg (expected=$expected, actual=$actual)"
        return 1
    fi
}

assert_contains() {
    local needle="$1"
    local haystack="$2"
    local msg="$3"

    if echo "$haystack" | grep -q "$needle"; then
        log_success "ASSERT PASSED: $msg (contains '$needle')"
        return 0
    else
        log_error "ASSERT FAILED: $msg (does not contain '$needle')"
        return 1
    fi
}

assert_error() {
    local result="$1"
    local msg="$2"

    if echo "$result" | grep -q "ERROR"; then
        log_success "ASSERT PASSED: $msg (got ERROR as expected)"
        return 0
    else
        log_error "ASSERT FAILED: $msg (expected ERROR but got: $result)"
        return 1
    fi
}

assert_warning() {
    local result="$1"
    local msg="$2"

    if echo "$result" | grep -q "WARNING"; then
        log_success "ASSERT PASSED: $msg (got WARNING as expected)"
        return 0
    else
        log_error "ASSERT FAILED: $msg (expected WARNING but got: $result)"
        return 1
    fi
}

#------------------------------------------------------------------------------
# Check Prerequisites
#------------------------------------------------------------------------------

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check ProxySQL is running
    if ! query_proxy "SELECT 1" >/dev/null 2>&1; then
        log_error "Cannot connect to ProxySQL on $PROXYSQL_HOST:$PROXYSQL_PORT"
        log_info "Start ProxySQL with: make -C test/polardb tap"
        return 1
    fi
    log_info "ProxySQL is accessible"

    # Check CSN enabled on PolarDB
    local csn_enabled
    csn_enabled=$(polardb_primary_sql "SHOW polar_csn_enable;" 2>/dev/null | tr -d ' ')
    if [ "$csn_enabled" != "on" ]; then
        log_error "polar_csn_enable must be ON"
        return 1
    fi
    log_info "polar_csn_enable = $csn_enabled"

    # Check consistency mode available
    local mode
    mode=$(polardb_replica_sql "SHOW polar_consistency_mode;" 2>/dev/null | tr -d ' ')
    log_info "polar_consistency_mode = $mode"

    # Check replica is in replica mode
    local is_replica
    is_replica=$(polardb_replica_sql "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d ' ')
    if [ "$is_replica" != "t" ]; then
        log_error "Replica is not in recovery mode"
        return 1
    fi
    log_info "Replica is in recovery mode"

    return 0
}

#------------------------------------------------------------------------------
# Query Rules Functions
#------------------------------------------------------------------------------

# Add a query rule to ProxySQL
# Usage: add_query_rule <rule_id> <pattern> <dest_hg> <comment>
add_query_rule() {
    local rule_id="$1"
    local pattern="$2"
    local dest_hg="$3"
    local comment="$4"

    log_info "Adding query rule: $comment (pattern='$pattern' -> HG $dest_hg)"
    proxysql_admin "DELETE FROM pgsql_query_rules WHERE comment='$comment';" >/dev/null 2>&1
    proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply, comment) VALUES ($rule_id, 1, '$pattern', $dest_hg, 1, '$comment');" >/dev/null 2>&1
    proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null 2>&1
}

# Add rule to route SELECTs to reader hostgroup
# Usage: add_select_to_reader_rule [reader_hg]
add_select_to_reader_rule() {
    local reader_hg="${1:-$POLARDB_READER_HG}"
    add_query_rule 10 "^SELECT" "$reader_hg" "select_to_reader"
}

# Add warmup rule (routes queries with /*WARMUP*/ comment to reader)
# Usage: add_warmup_rule [reader_hg]
add_warmup_rule() {
    local reader_hg="${1:-$POLARDB_READER_HG}"
    add_query_rule 1 ".*WARMUP.*" "$reader_hg" "warmup_rule"
}

# Remove a query rule by comment
# Usage: remove_query_rule <comment>
remove_query_rule() {
    local comment="$1"
    log_info "Removing query rule: $comment"
    proxysql_admin "DELETE FROM pgsql_query_rules WHERE comment='$comment'; LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null 2>&1
}

#------------------------------------------------------------------------------
# Connection Pool Warmup Functions
#------------------------------------------------------------------------------
# With the PolarDB pipeline routing model, SELECTs are routed to reader HGs
# via replica_eligible=1 from auto-installed query rules (10000-10011).
# Custom destination_hostgroup rules are NOT needed and would conflict with
# the pipeline (it overrides destination_hostgroup based on the routing plan).

# Ensure auto-installed PolarDB query rules are active
# Usage: ensure_polardb_rules_loaded
ensure_polardb_rules_loaded() {
    proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null 2>&1
}

# Warmup a specific hostgroup by running a query to it
# Usage: warmup_hostgroup <hostgroup> [max_wait_sec]
warmup_hostgroup() {
    local hg="$1"
    local max_wait="${2:-10}"

    log_info "Warming up hostgroup $hg..."

    # Ensure auto-rules are loaded (sets replica_eligible=1 for SELECTs)
    ensure_polardb_rules_loaded

    # Send a regular SELECT — pipeline routes it to reader via replica_eligible
    query_proxy "SELECT 1" >/dev/null 2>&1 || true

    # Wait for pooled connection to become available
    local waited=0
    while [ "$waited" -lt "$max_wait" ]; do
        local conn_free
        conn_free=$(proxysql_admin "SELECT ConnFree FROM stats_pgsql_connection_pool WHERE hostgroup=$hg;" 2>/dev/null | grep -E '^[0-9]+$' | head -1)
        if [ -n "$conn_free" ] && [ "$conn_free" -gt 0 ]; then
            log_info "Warmup done: HG $hg has $conn_free free connections (waited ${waited}s)"
            return 0
        fi
        # Retry sending SELECT in case the first one didn't create a reader connection
        query_proxy "SELECT 1" >/dev/null 2>&1 || true
        sleep 1
        waited=$((waited + 1))
    done

    log_warning "No pooled connections in HG $hg after ${max_wait}s"
    return 1
}

# Warmup replica pool (HG 11 by default)
# Usage: warmup_replica_pool [reader_hg] [max_wait_sec]
warmup_replica_pool() {
    local reader_hg="${1:-$POLARDB_READER_HG}"
    local max_wait="${2:-10}"
    warmup_hostgroup "$reader_hg" "$max_wait"
}

# Setup reader routing: ensure auto-rules + warmup
# Usage: setup_reader_routing [reader_hg] [max_wait_sec]
setup_reader_routing() {
    local reader_hg="${1:-$POLARDB_READER_HG}"
    local max_wait="${2:-10}"

    log_info "Setting up reader routing to HG $reader_hg..."
    ensure_polardb_rules_loaded
    warmup_replica_pool "$reader_hg" "$max_wait"
}

# Get connection pool stats for a hostgroup
# Usage: get_pool_stats <hostgroup>
get_pool_stats() {
    local hg="$1"
    proxysql_admin "SELECT srv_host, srv_port, ConnUsed, ConnFree, ConnOK, ConnERR, Queries FROM stats_pgsql_connection_pool WHERE hostgroup=$hg;"
}

# Check if hostgroup has free connections
# Usage: has_free_connections <hostgroup>
has_free_connections() {
    local hg="$1"
    local conn_free
    conn_free=$(proxysql_admin "SELECT SUM(ConnFree) FROM stats_pgsql_connection_pool WHERE hostgroup=$hg;" 2>/dev/null | grep -E '^[0-9]+$' | head -1)
    [ -n "$conn_free" ] && [ "$conn_free" -gt 0 ]
}

#------------------------------------------------------------------------------
# OUTPUT LOGGING
#------------------------------------------------------------------------------
# All test output is logged to test/polardb/test_output/run_NNNN_caseX.log.
# The run counter is test/polardb/.runid and is incremented by each run.

OUTPUT_DIR="${POLARDB_OUTPUT_DIR:-$POLARDB_TEST_DIR/test_output}"
RUNID_FILE="$POLARDB_TEST_DIR/.runid"

# Get next run ID
get_next_runid() {
    local runid=1
    if [ -f "$RUNID_FILE" ]; then
        runid=$(cat "$RUNID_FILE")
        runid=$((runid + 1))
    fi
    echo "$runid" > "$RUNID_FILE"
    echo "$runid"
}

# Log file locations. ProxySQL logs default to the wrapper data directory.
# Backend log collection is optional: set POLARDB_LOG_DIR or explicit
# POLARDB_PRIMARY_LOG/POLARDB_REPLICA_LOG in test/polardb/.env when a local
# environment exposes server logs on the filesystem.
PROXYSQL_LOG="${PROXYSQL_LOG:-${PROXYSQL_DATA_DIR:-/tmp/proxysql_test_data}/proxysql.log}"
POLARDB_LOG_DIR="${POLARDB_LOG_DIR:-}"
if [ -n "$POLARDB_LOG_DIR" ]; then
    POLARDB_PRIMARY_LOG="${POLARDB_PRIMARY_LOG:-$POLARDB_LOG_DIR/patroni-instance${PRIMARY_INSTANCE:-1}.err}"
    POLARDB_REPLICA_LOG="${POLARDB_REPLICA_LOG:-$POLARDB_LOG_DIR/patroni-instance${REPLICA_INSTANCE:-2}.err}"
else
    POLARDB_PRIMARY_LOG="${POLARDB_PRIMARY_LOG:-}"
    POLARDB_REPLICA_LOG="${POLARDB_REPLICA_LOG:-}"
fi

# Initialize logging - call at start of test
init_logging() {
    local case_name="$1"
    mkdir -p "$OUTPUT_DIR"

    RUN_ID=$(get_next_runid)
    RUN_ID_PADDED=$(printf "%04d" "$RUN_ID")
    RUN_DIR="$OUTPUT_DIR/run_${RUN_ID_PADDED}_${case_name}"
    mkdir -p "$RUN_DIR"

    LOG_FILE="$RUN_DIR/test.log"

    # Capture start time for log extraction (UTC for PolarDB logs)
    START_TIME_UTC=$(date -u '+%Y-%m-%d %H:%M:%S')
    START_TIME_LOCAL=$(date '+%Y-%m-%d %H:%M:%S')

    # Start logging - tee to both stdout and file via named pipe.
    # Process substitution (exec > >(tee ...)) causes orphan tee on exit.
    _LOG_PIPE="$RUN_DIR/.log_pipe"
    mkfifo "$_LOG_PIPE"
    tee -a "$LOG_FILE" < "$_LOG_PIPE" &
    _LOG_TEE_PID=$!
    exec > "$_LOG_PIPE" 2>&1

    echo "================================================================================"
    echo "Run ID: $RUN_ID"
    echo "Run dir: $RUN_DIR"
    echo "Started: $START_TIME_LOCAL (UTC: $START_TIME_UTC)"
    echo "================================================================================"
}

stop_logging() {
    # Close stdout to send EOF to tee, then wait for it to finish
    exec 1>/dev/null 2>/dev/null
    if [ -n "$_LOG_TEE_PID" ]; then
        wait "$_LOG_TEE_PID" 2>/dev/null || true
        _LOG_TEE_PID=""
    fi
    [ -n "$_LOG_PIPE" ] && rm -f "$_LOG_PIPE"
}

#------------------------------------------------------------------------------
# LOG EXTRACTION - Capture debug logs from ProxySQL and PolarDB
#------------------------------------------------------------------------------
# Extracts relevant log entries from test period and saves to run directory

extract_logs() {
    local end_time_utc=$(date -u '+%Y-%m-%d %H:%M:%S')
    local end_time_local=$(date '+%Y-%m-%d %H:%M:%S')

    # UTC for PolarDB (they use UTC timestamps)
    local start_utc_cmp=$(echo "$START_TIME_UTC" | tr -d ':-' | tr ' ' '_')
    local end_utc_cmp=$(echo "$end_time_utc" | tr -d ':-' | tr ' ' '_')

    # LOCAL for ProxySQL (it uses local timestamps)
    local start_local_cmp=$(echo "$START_TIME_LOCAL" | tr -d ':-' | tr ' ' '_')
    local end_local_cmp=$(echo "$end_time_local" | tr -d ':-' | tr ' ' '_')

    echo ""
    echo "[$(ts)] Extracting FULL logs between timestamps:"
    echo "[$(ts)]   UTC:   $START_TIME_UTC to $end_time_utc"
    echo "[$(ts)]   Local: $START_TIME_LOCAL to $end_time_local"

    # --- Helper function for time-range extraction ---
    # Also captures lines WITHOUT timestamp that follow timestamped lines (e.g., LIBPQ debug)
    extract_time_range() {
        local log_file="$1"
        local output_file="$2"
        local start="$3"
        local end="$4"

        awk -v start="$start" -v end="$end" '
        {
            # Extract timestamp: "2026-02-04 19:21:00"
            if (match($0, /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}/)) {
                ts = substr($0, RSTART, RLENGTH)
                # Convert to comparable: "20260204_192100"
                gsub(/-/, "", ts)
                gsub(/:/, "", ts)
                gsub(/ /, "_", ts)
                current_ts = ts
            }
            # Print if within range (includes non-timestamped continuation lines)
            if (current_ts >= start && current_ts <= end) {
                print
            }
        }' "$log_file" > "$output_file" 2>/dev/null
    }

    # --- Extract all logs in PARALLEL ---
    local pids=""

    # ProxySQL (LOCAL time)
    if [ -f "$PROXYSQL_LOG" ]; then
        (
            extract_time_range "$PROXYSQL_LOG" "$RUN_DIR/proxysql.log" "$start_local_cmp" "$end_local_cmp"
            grep -aiE "polar|split|lsn|csn|xid|replica|hostgroup|hg_id|backend|LIBPQ|\[(H|RQ|SH|W|S)" \
                "$RUN_DIR/proxysql.log" > "$RUN_DIR/proxysql_debug.log" 2>/dev/null || true
        ) &
        pids="$pids $!"
    fi

    # PolarDB Primary (UTC time)
    if [ -n "$POLARDB_PRIMARY_LOG" ] && [ -f "$POLARDB_PRIMARY_LOG" ]; then
        (
            extract_time_range "$POLARDB_PRIMARY_LOG" "$RUN_DIR/polardb_primary.log" "$start_utc_cmp" "$end_utc_cmp"
            grep -E "SPLIT_DEBUG|VISIBILITY_DEBUG|BUFMGR_DEBUG|polar_|CSN|LSN" \
                "$RUN_DIR/polardb_primary.log" > "$RUN_DIR/polardb_primary_debug.log" 2>/dev/null || true
        ) &
        pids="$pids $!"
    fi

    # PolarDB Replica (UTC time)
    if [ -n "$POLARDB_REPLICA_LOG" ] && [ -f "$POLARDB_REPLICA_LOG" ]; then
        (
            extract_time_range "$POLARDB_REPLICA_LOG" "$RUN_DIR/polardb_replica.log" "$start_utc_cmp" "$end_utc_cmp"
            grep -E "SPLIT_DEBUG|VISIBILITY_DEBUG|BUFMGR_DEBUG|polar_|CSN|LSN" \
                "$RUN_DIR/polardb_replica.log" > "$RUN_DIR/polardb_replica_debug.log" 2>/dev/null || true
        ) &
        pids="$pids $!"
    fi

    # Wait for all extractions to complete
    for pid in $pids; do
        wait "$pid" 2>/dev/null || true
    done

    # Report results
    if [ -f "$RUN_DIR/proxysql.log" ]; then
        local proxy_lines=$(wc -l < "$RUN_DIR/proxysql.log" 2>/dev/null || echo 0)
        local proxy_debug=$(wc -l < "$RUN_DIR/proxysql_debug.log" 2>/dev/null || echo 0)
        echo "[$(ts)]   ProxySQL:      $proxy_lines lines, $proxy_debug debug"
    else
        echo "[$(ts)]   ProxySQL: NOT FOUND"
    fi

    if [ -f "$RUN_DIR/polardb_primary.log" ]; then
        local prim_lines=$(wc -l < "$RUN_DIR/polardb_primary.log" 2>/dev/null || echo 0)
        local prim_debug=$(wc -l < "$RUN_DIR/polardb_primary_debug.log" 2>/dev/null || echo 0)
        echo "[$(ts)]   Primary:       $prim_lines lines, $prim_debug debug"
    else
        echo "[$(ts)]   Primary: NOT FOUND"
    fi

    if [ -f "$RUN_DIR/polardb_replica.log" ]; then
        local repl_lines=$(wc -l < "$RUN_DIR/polardb_replica.log" 2>/dev/null || echo 0)
        local repl_debug=$(wc -l < "$RUN_DIR/polardb_replica_debug.log" 2>/dev/null || echo 0)
        echo "[$(ts)]   Replica:       $repl_lines lines, $repl_debug debug"
    else
        echo "[$(ts)]   Replica: NOT FOUND"
    fi

    # --- Summary file ---
    cat > "$RUN_DIR/README.txt" << EOF
Run ID: $RUN_ID
Case: ${CASE_NUM:-?} - ${CASE_NAME:-?}
Time Range (UTC): $START_TIME_UTC to $end_time_utc
Time Range (Local): $START_TIME_LOCAL to $(date '+%Y-%m-%d %H:%M:%S')
Mode: consistency=${CONSISTENCY_MODE:-?} split=${SPLIT_ENABLED:-?} xact_split=${XACT_SPLIT:-?}

Files (FULL logs between timestamps):
  test.log                  - Main test output
  proxysql.log              - ProxySQL log (time range)
  proxysql_debug.log        - ProxySQL PolarDB request-flow and FSM entries
  polardb_primary.log       - Primary PostgreSQL log (time range)
  polardb_primary_debug.log - Primary SPLIT_DEBUG/VISIBILITY_DEBUG entries
  polardb_replica.log       - Replica PostgreSQL log (time range)
  polardb_replica_debug.log - Replica SPLIT_DEBUG/VISIBILITY_DEBUG entries
EOF

    echo "[$(ts)]   Logs saved to: $RUN_DIR"
}

#------------------------------------------------------------------------------
# PROXYSQL STARTUP AND CONFIGURATION
#------------------------------------------------------------------------------
# Uses the ProxySQL lifecycle helper to start ProxySQL with proper cleanup.

WRAPPER_SCRIPT="$POLARDB_TEST_DIR/common/proxysql_lifecycle.sh"
PROXYSQL_STARTED=0

# ProxySQL ports (match lib.sh)
PROXYSQL_ADMIN_PORT_LOCAL=${PROXYSQL_ADMIN_PORT:-16132}
PROXYSQL_PORT_LOCAL=${PROXYSQL_PORT:-16433}

start_proxysql() {
    echo "[$(ts)] Starting ProxySQL via wrapper..."

    if [ ! -x "$WRAPPER_SCRIPT" ]; then
        echo "[$(ts)] ERROR: Wrapper script not found: $WRAPPER_SCRIPT"
        return 1
    fi

    if ! "$WRAPPER_SCRIPT" start --admin-port "$PROXYSQL_ADMIN_PORT_LOCAL" --proxy-port "$PROXYSQL_PORT_LOCAL"; then
        echo "[$(ts)] ERROR: ProxySQL failed to start"
        return 1
    fi

    PROXYSQL_STARTED=1
    echo "[$(ts)] ProxySQL started on ports admin=$PROXYSQL_ADMIN_PORT_LOCAL proxy=$PROXYSQL_PORT_LOCAL"

    # Configure servers
    echo "[$(ts)] Configuring PolarDB servers..."
    proxysql_admin "DELETE FROM pgsql_servers;"
    proxysql_admin "INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES ($POLARDB_WRITER_HG, '$PRIMARY_HOST', $PRIMARY_PORT);"
    proxysql_admin "INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES ($POLARDB_READER_HG, '$REPLICA_HOST', $REPLICA_PORT);"
    proxysql_admin "DELETE FROM pgsql_replication_hostgroups;"
    # The LSN session-consistency schema stores only PolarDB topology and policy
    # columns used by this harness; STEP 1 overrides consistency_mode per
    # scenario.
    proxysql_admin "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, comment) VALUES ($POLARDB_WRITER_HG, $POLARDB_READER_HG, 'polardb', 'default', -1, -1, '');"
    proxysql_admin "DELETE FROM pgsql_users;"
    # Use postgres user (available in PolarDB) instead of testuser
    proxysql_admin "INSERT INTO pgsql_users (username, password, default_hostgroup) VALUES ('postgres', 'postgres', $POLARDB_WRITER_HG);"

    # SELECT routing eligibility is explicit: replica_eligible=1 marks plain
    # SELECTs for the PolarDB LSN router without forcing a destination
    # hostgroup. apply=0 lets rule processing continue.
    proxysql_admin "DELETE FROM pgsql_query_rules;"
    proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_digest, replica_eligible, apply) VALUES ($POLARDB_SELECT_RULE_ID, 1, '^SELECT', 1, 0);"

    proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;"
    proxysql_admin "LOAD PGSQL USERS TO RUNTIME;"
    proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;"

    echo "[$(ts)] Waiting for monitor (3s)..."
    sleep 3

    # Verify connection
    if query_proxy "SELECT 1" >/dev/null 2>&1; then
        echo "[$(ts)] ProxySQL connection verified"
    else
        echo "[$(ts)] ERROR: Cannot connect through ProxySQL"
        return 1
    fi

    return 0
}

stop_proxysql() {
    if [ "$PROXYSQL_STARTED" = "1" ]; then
        echo "[$(ts)] Stopping ProxySQL..."
        "$WRAPPER_SCRIPT" stop >/dev/null 2>&1 || true
        PROXYSQL_STARTED=0
    fi
}

# =============================================================================
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# DEAD/POSTPONED SCAFFOLD (old preset-driven scenario runner).
#
# Everything from here to EOF -- run_test, the snapshot/delta/pool/error/stat
# print helpers, verify_preset, parse_common_args, enable_proxysql_debug, and
# run_consistency_test -- belongs to the previous preset-driven CSN/split test
# driver. The committed TAP scripts (config_roundtrip / lsn_session_consistency
# / wait_timeout_cleanup) do NOT call any of it; they drive scenarios directly
# and assert via lib/tap_core.sh. This block is retained (not deleted) for the
# postponed CSN/transaction-split feature round. Do not wire it into the active
# TAP slice without restoring the matching code paths.
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# =============================================================================

#------------------------------------------------------------------------------
# Scenario runner entry point
#------------------------------------------------------------------------------
run_test() {
    local test_name="$1"
    local test_func="$2"

    # Run the test function (it handles everything including ProxySQL startup)
    if $test_func; then
        return 0
    else
        return 1
    fi
}

#------------------------------------------------------------------------------
# STAT PRESETS
# Format: "COUNTER|success_expect|failure_expect|description"
#   success_expect/failure_expect: 0, +, -, or specific number
#   + means delta >= 1, - means delta <= -1, 0 means delta == 0
#------------------------------------------------------------------------------

# Common stats checked for all tests
# Note: PgSQL Questions counter is not exposed in stats_pgsql_global
# Use RFQ LSN updates as proof that query responses are publishing server LSNs.
COMMON_STATS=(
    "PolarDB_Server_LSN_Updates_From_RFQ|+|+|Query RFQ published server LSN"
)

# Eventual mode (mode=0): no waits
EVENTUAL_STATS=(
    "PolarDB_Split_LSN_Wait_Count|0|0|No LSN wait (split)"
    "PolarDB_Wait_Wrap_Prepared|0|0|No wait wrapper prepared"
    "PolarDB_Wait_LSN_Sent|0|0|No LSN wait (non-split)"
)

# LSN mode (mode=1): LSN tracking (non-split waits)
LSN_STATS=(
    "PolarDB_Server_LSN_Updates_From_RFQ|+|+|Server LSN updated from query RFQ"
    "PolarDB_Wait_Wrap_Prepared|+|+|Wait wrapper prepared"
    "PolarDB_Wait_LSN_Sent|+|+|LSN wait wrapped (non-split)"
    "PolarDB_Wait_Error_LSN_Wait_Timeout|0|+|LSN timeout"
)

# Deferred full-feature template: CSN mode (mode=2), non-split waits.
# When CSN returns, the successful-send stat should mirror PolarDB_Wait_LSN_Sent.
CSN_STATS=(
    "PolarDB_CSN_Updates_From_Query|+|+|CSN captured from INSERT"
    "PolarDB_Wait_CSN_Sent|+|+|CSN wait wrapped (non-split)"
    "PolarDB_Wait_Error_Timeout|0|+|CSN timeout"
)

# Primary mode (mode=3): no splits
PRIMARY_STATS=(
    "PolarDB_Split_Reads_Total|0|0|No split reads"
)

# Global CSN mode (mode=4): global routing (non-split waits)
GLOBAL_CSN_STATS=(
    "PolarDB_Global_CSN_Routing|+|+|Global CSN routing"
    "PolarDB_Wait_CSN_Sent|+|+|CSN wait wrapped (non-split)"
    "PolarDB_Wait_Error_Timeout|0|+|CSN timeout"
)

# Split LSN wait stats (split path)
SPLIT_LSN_STATS=(
    "PolarDB_Split_LSN_Wait_Count|+|+|LSN wait wrapped (split)"
    "PolarDB_Split_Error_LSN_Wait_Timeout|0|+|LSN timeout (split)"
)

# Split with CSN consistency mode: split always uses LSN wait internally,
# even when the consistency mode is CSN/CSN_GLOBAL. CSN doesn't advance
# mid-transaction, so only LSN can guarantee replica visibility for split reads.
# Cases 7/8 still verify split behavior under CSN modes — they just check LSN wait stats.

# Split transaction stats (when XACT_SPLIT=1, SPLIT_VARIANT=basic)
SPLIT_STATS=(
    "PolarDB_XIDs_Received|+|+|XIDs captured"
    "PolarDB_Txn_Became_Splittable|+|+|Transaction splittable"
    "PolarDB_Split_Reads_Total|+|+|Split read occurred"
    "PolarDB_Split_Reads_Success|+|0|Split success"
    "PolarDB_Txn_Committed_With_Split|+|?|Commit classified as split"
    "PolarDB_Txn_Lost_Splittable|0|0|Splittable not lost mid-txn"
    "PolarDB_Split_WAL_Pending|?|?|WAL pending (timing-dependent)"
    "PolarDB_Split_Invariant_Violations|0|0|Invariant violations (should never fire)"
)

# Pre-write read + split stats (SPLIT_VARIANT=prewrite)
# Pre-write SELECT routes via consistency wait (no XIDs yet).
# Post-write SELECT routes via split. In strict failure, pgbench aborts on
# the pre-write timeout ERROR before reaching the split path → use ? for split stats.
SPLIT_PREWRITE_STATS=(
    "PolarDB_Wait_LSN_Sent|+|+|Pre-write SELECT used consistency wait"
    "PolarDB_Split_LSN_Wait_Count|+|?|Post-write SELECT used split wait"
    "PolarDB_Split_Error_LSN_Wait_Timeout|0|?|Split LSN timeout"
    "PolarDB_XIDs_Received|+|?|XIDs captured after INSERT"
    "PolarDB_Txn_Became_Splittable|+|?|Transaction became splittable"
    "PolarDB_Split_Reads_Total|+|?|Split read attempted"
    "PolarDB_Split_Reads_Success|+|0|Split success"
    "PolarDB_Txn_Committed_With_Split|+|?|Commit classified as split"
    "PolarDB_Txn_Lost_Splittable|0|0|Splittable not lost mid-txn"
)

# Multiple split reads stats (SPLIT_VARIANT=multi)
# Two SELECTs after INSERT → two split reads within one txn.
SPLIT_MULTI_STATS=(
    "PolarDB_Split_LSN_Wait_Count|+|+|Split LSN waits (2 per txn)"
    "PolarDB_Split_Error_LSN_Wait_Timeout|0|+|Split LSN timeout"
    "PolarDB_XIDs_Received|+|+|XIDs captured"
    "PolarDB_Txn_Became_Splittable|+|+|Transaction became splittable"
    "PolarDB_Split_Reads_Total|+|+|Split reads attempted (2 per txn)"
    "PolarDB_Split_Reads_Success|+|0|Split successes"
    "PolarDB_Txn_Committed_With_Split|+|?|Commit classified as split"
    "PolarDB_Txn_Lost_Splittable|0|0|Splittable not lost mid-txn"
)

# Read-only transaction stats (SPLIT_VARIANT=readonly)
# Autocommit INSERT sets session_write_lsn, then BEGIN..SELECT..SELECT..COMMIT.
# No writes in txn → FSM stays TXN_ON_PRIMARY → no XIDs → no split.
# Reads use consistency wait (REPLICA_WITH_WAIT).
READONLY_TXN_STATS=(
    "PolarDB_Wait_LSN_Sent|+|+|Consistency waits for reads in txn"
    "PolarDB_Wait_Error_LSN_Wait_Timeout|0|+|LSN timeout"
    "PolarDB_Split_Reads_Total|0|0|No split reads (no XIDs in txn)"
    "PolarDB_Txn_Became_Splittable|0|0|Never became splittable"
    "PolarDB_XIDs_Received|0|0|No XIDs (no writes in txn)"
    "PolarDB_Txn_Committed_With_Split|0|0|No split commit (no splits)"
    "PolarDB_Txn_Committed_No_Split|0|0|No no-split commit (was_splittable never set)"
)

# Extended protocol veto stats (SPLIT_VARIANT=extended)
# Same split-eligible flow as basic, but -M extended forces primary (RC4).
# Split is vetoed but basic consistency routing still works in extended protocol.
EXTENDED_VETO_STATS=(
    "PolarDB_Split_Reads_Total|0|0|No split reads (extended protocol)"
    "PolarDB_Split_LSN_Wait_Count|0|0|No split LSN waits"
    "PolarDB_Wait_LSN_Sent|?|?|Consistency waits may occur (not vetoed)"
    "PolarDB_XIDs_Received|0|0|No XIDs (extended protocol bypasses split tracking)"
    "PolarDB_Txn_Became_Splittable|0|0|Never became splittable"
)

# Combined workload stats (SPLIT_VARIANT=combined)
# Mixed script: autocommit read, write+read (consistency wait), split txn, read-only txn.
# Uses invariant checks rather than exact counts.
COMBINED_WORKLOAD_STATS=(
    "PolarDB_Wait_LSN_Sent|+|+|Consistency waits occurred"
    "PolarDB_Server_LSN_Updates_From_RFQ|+|+|Query RFQs refreshed server LSN cache"
    "PolarDB_XIDs_Received|+|?|XIDs captured in split txn"
    "PolarDB_Txn_Became_Splittable|+|?|Split txn became splittable"
    "PolarDB_Split_Reads_Total|+|?|Split read attempted"
    "PolarDB_Split_Reads_Success|+|0|Split success"
    "PolarDB_Txn_Committed_With_Split|+|?|Split txn committed with split"
    "PolarDB_Txn_Lost_Splittable|0|0|Splittable not lost mid-txn"
)

# Result expectations
RESULT_STATS=(
    "count|1|?|COUNT result"
    "warning|0|?|WARNING in output"
    "error|0|?|ERROR in output"
)

#------------------------------------------------------------------------------
# Counter storage and helpers
#------------------------------------------------------------------------------
declare -A BEFORE AFTER

snapshot() {
    local prefix="$1"
    # All PolarDB counters from PgSQL_Thread.cpp - see AVAILABLE COUNTERS above
    local counters="
        PolarDB_Split_Reads_Total PolarDB_Split_Reads_Success PolarDB_Split_Reads_Fallback
        PolarDB_Split_Reads_Error PolarDB_Split_Rejected_Not_Select PolarDB_Split_Rejected_For_Update
        PolarDB_Txn_Became_Splittable PolarDB_Txn_Lost_Splittable PolarDB_Txn_Committed_With_Split
        PolarDB_Txn_Committed_No_Split PolarDB_Queries_In_Splittable_Txn PolarDB_Queries_Split_Eligible
        PolarDB_XIDs_Received
        PolarDB_Server_LSN_Updates_From_RFQ PolarDB_LSN_Updates_From_Monitor
        PolarDB_Monitor_Health_Invalid_Role PolarDB_Monitor_Health_Invalid_Values
        PolarDB_LSN_Stale_Count
        PolarDB_Write_Missing_LSN
        PolarDB_Split_LSN_Wait_Count PolarDB_Wait_Wrap_Prepared PolarDB_Wait_LSN_Sent PolarDB_Wait_Wrap_Safety_Abort
        PolarDB_Split_LSN_Wait_Sum_Us PolarDB_Split_Error_LSN_Wait_Timeout
        PolarDB_Wait_Error_LSN_Wait_Timeout PolarDB_Wait_Error_Timeout
        PolarDB_Wait_Error_Connection_Lost PolarDB_Wait_Reads_Retried_On_Writer
        PolarDB_CSN_Updates_From_Query PolarDB_CSN_Updates_From_Monitor PolarDB_CSN_Stale_Count
        PolarDB_Wait_CSN_Sent PolarDB_Global_CSN_Routing
        PolarDB_Split_Error_Connection_Lost PolarDB_Split_Error_Query_Failed PolarDB_Split_Error_Timeout
        PolarDB_Split_No_Backend PolarDB_Split_Send_Failed
        PolarDB_Split_Pool_Hit PolarDB_Split_Pool_Empty PolarDB_Split_Pool_Contention
        PolarDB_Split_Conn_Reused PolarDB_Split_Conn_Cleanup_Success PolarDB_Split_Conn_Cleanup_Failed
        PolarDB_Reader_Pool_Conns_Free PolarDB_Reader_Pool_Conns_Used
        PolarDB_Split_Warmup_Requested PolarDB_Split_Warmup_Created PolarDB_Split_Warmup_Failed
        PolarDB_Warmup_Pending
        PolarDB_Split_Latency_Sum_Us PolarDB_Split_Latency_Count
    "

    for c in $counters; do
        local v=$(proxysql_admin "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$c';" 2>/dev/null | tr -d ' \n')
        if [ "$prefix" = "B" ]; then BEFORE[$c]="${v:-0}"; else AFTER[$c]="${v:-0}"; fi
    done
}

delta() { echo $(( ${AFTER[$1]:-0} - ${BEFORE[$1]:-0} )); }

print_deltas() {
    echo "------------------------------------------------------------"
    printf "%-42s %8s %8s %6s\n" "COUNTER" "BEFORE" "AFTER" "DELTA"
    echo "------------------------------------------------------------"
    for c in "${!BEFORE[@]}"; do
        local d=$(delta "$c")
        [ "$d" -ne 0 ] && printf "%-42s %8d %8d %+6d\n" "$c" "${BEFORE[$c]}" "${AFTER[$c]}" "$d"
    done
    echo "------------------------------------------------------------"
}

#------------------------------------------------------------------------------
# Per-hostgroup connection pool stats (stats_pgsql_connection_pool)
# Tracks queries, bytes, errors per server to verify routing
#------------------------------------------------------------------------------
declare -A POOL_BEFORE POOL_AFTER

snapshot_pool() {
    local prefix="$1"
    # Get per-hostgroup stats: hostgroup|Queries|ConnOK|ConnERR|Bytes_sent|Bytes_recv
    local data=$(proxysql_admin "SELECT hostgroup, srv_host, srv_port, Queries, ConnOK, ConnERR, Bytes_data_sent, Bytes_data_recv FROM stats_pgsql_connection_pool;" 2>/dev/null)

    while IFS='|' read -r hg host port queries connok connerr sent recv; do
        [ -z "$hg" ] && continue
        local key="${hg}_${host}_${port}"
        if [ "$prefix" = "B" ]; then
            POOL_BEFORE["${key}_queries"]="${queries:-0}"
            POOL_BEFORE["${key}_connok"]="${connok:-0}"
            POOL_BEFORE["${key}_connerr"]="${connerr:-0}"
            POOL_BEFORE["${key}_sent"]="${sent:-0}"
            POOL_BEFORE["${key}_recv"]="${recv:-0}"
        else
            POOL_AFTER["${key}_queries"]="${queries:-0}"
            POOL_AFTER["${key}_connok"]="${connok:-0}"
            POOL_AFTER["${key}_connerr"]="${connerr:-0}"
            POOL_AFTER["${key}_sent"]="${sent:-0}"
            POOL_AFTER["${key}_recv"]="${recv:-0}"
        fi
    done <<< "$data"
}

print_pool_deltas() {
    printf "%-5s %-20s %8s %8s %8s\n" "HG" "SERVER" "QUERIES" "CONN_OK" "CONN_ERR"
    echo "--------------------------------------------------------------"

    # Get unique keys from BEFORE (all configured servers)
    local keys=$(printf '%s\n' "${!POOL_BEFORE[@]}" | sed 's/_queries$//' | sed 's/_connok$//' | sed 's/_connerr$//' | sed 's/_sent$//' | sed 's/_recv$//' | sort -u)

    for key in $keys; do
        local q_before="${POOL_BEFORE[${key}_queries]:-0}"
        local q_after="${POOL_AFTER[${key}_queries]:-0}"
        local q_delta=$((q_after - q_before))

        local ok_before="${POOL_BEFORE[${key}_connok]:-0}"
        local ok_after="${POOL_AFTER[${key}_connok]:-0}"
        local ok_delta=$((ok_after - ok_before))

        local err_before="${POOL_BEFORE[${key}_connerr]:-0}"
        local err_after="${POOL_AFTER[${key}_connerr]:-0}"
        local err_delta=$((err_after - err_before))

        # Parse key: HG_HOST_PORT
        local hg=$(echo "$key" | cut -d'_' -f1)
        local server=$(echo "$key" | cut -d'_' -f2-)

        # Show ALL servers, mark those with activity
        if [ "$q_delta" -ne 0 ] || [ "$ok_delta" -ne 0 ] || [ "$err_delta" -ne 0 ]; then
            printf "%-5s %-20s %8d %8d %8d  <--\n" "$hg" "$server" "$q_delta" "$ok_delta" "$err_delta"
        else
            printf "%-5s %-20s %8d %8d %8d\n" "$hg" "$server" "$q_delta" "$ok_delta" "$err_delta"
        fi
    done
    echo "--------------------------------------------------------------"
}

# Get query delta for specific hostgroup
get_hg_queries_delta() {
    local target_hg="$1"
    local total=0

    local keys=$(printf '%s\n' "${!POOL_BEFORE[@]}" | grep "_queries$" | sort -u)
    for key in $keys; do
        local hg=$(echo "$key" | cut -d'_' -f1)
        if [ "$hg" = "$target_hg" ]; then
            local before="${POOL_BEFORE[$key]:-0}"
            local after="${POOL_AFTER[$key]:-0}"
            total=$((total + after - before))
        fi
    done
    echo "$total"
}

#------------------------------------------------------------------------------
# Error tracking (stats_pgsql_errors)
#------------------------------------------------------------------------------
print_errors() {
    echo ""
    echo "--- Errors (stats_pgsql_errors) ---"
    local errors=$(proxysql_admin "SELECT hostgroup, hostname, port, sqlstate, count_star, last_error FROM stats_pgsql_errors WHERE count_star > 0;" 2>/dev/null)
    if [ -n "$errors" ]; then
        printf "%-5s %-20s %-8s %8s %s\n" "HG" "SERVER" "SQLSTATE" "COUNT" "LAST_ERROR"
        echo "--------------------------------------------------------------"
        echo "$errors" | while IFS='|' read -r hg host port state count err; do
            [ -z "$hg" ] && continue
            printf "%-5s %-15s:%-4s %-8s %8s %s\n" "$hg" "$host" "$port" "$state" "$count" "$err"
        done
    else
        echo "(no errors)"
    fi
    echo ""
}

#------------------------------------------------------------------------------
# Check a stat against expectation
# Returns: 0=pass, 1=fail, 2=skip (expectation is ?)
#------------------------------------------------------------------------------
check_stat() {
    local name="$1"
    local expect="$2"
    local actual="$3"

    [ "$expect" = "?" ] && return 2
    case "$expect" in
        0)  [ "$actual" -eq 0 ]; return $? ;;
        +)  [ "$actual" -ge 1 ]; return $? ;;
        -)  [ "$actual" -le -1 ]; return $? ;;
        *)  [ "$actual" -eq "$expect" ]; return $? ;;
    esac
}

#------------------------------------------------------------------------------
# Verify stats from a preset
# Args: outcome (success/failure), preset_array_name
#------------------------------------------------------------------------------
verify_preset() {
    local outcome="$1"
    shift
    local preset=("$@")

    for stat in "${preset[@]}"; do
        IFS='|' read -r name success_exp failure_exp desc <<< "$stat"
        local expect=$([[ "$outcome" = "success" ]] && echo "$success_exp" || echo "$failure_exp")
        local actual=$(delta "$name")

        # Format expected for display
        local expect_desc
        case "$expect" in
            0)  expect_desc="=0" ;;
            +)  expect_desc=">=1" ;;
            -)  expect_desc="<=-1" ;;
            ?)  expect_desc="skip" ;;
            *)  expect_desc="=$expect" ;;
        esac

        check_stat "$name" "$expect" "$actual"
        local rc=$?

        case $rc in
            0) echo "[$(ts)]   PASS: $desc | delta=$actual (want $expect_desc)"; PASS=$((PASS+1)) ;;
            1) echo "[$(ts)]   FAIL: $desc | delta=$actual (want $expect_desc)"; FAIL=$((FAIL+1)) ;;
            2) ;; # skip
        esac
    done
}

#------------------------------------------------------------------------------
# Main test runner
#------------------------------------------------------------------------------
parse_common_args() {
    POLAR_MODE="best_effort"
    EXPECT_OUTCOME="success"
    for arg in "$@"; do
        case $arg in
            -a) POLAR_MODE="best_effort" ;;
            -b) POLAR_MODE="strict" ;;
            -success|-s) EXPECT_OUTCOME="success" ;;
            -failure|-f) EXPECT_OUTCOME="failure" ;;
        esac
    done
}

enable_proxysql_debug() {
    # Some ProxySQL versions store admin variables in admin_variables,
    # others only expose debug in global_variables. Detect table at runtime.
    if proxysql_admin "SELECT name FROM sqlite_master WHERE type='table' AND name='admin_variables';" | grep -q admin_variables; then
        proxysql_admin "UPDATE admin_variables SET variable_value='1' WHERE variable_name='debug'"
        proxysql_admin "LOAD ADMIN VARIABLES TO RUNTIME"
        proxysql_admin "SELECT variable_name, variable_value FROM runtime_admin_variables WHERE variable_name='debug';" | sed 's/^/    /'
    else
        proxysql_admin "UPDATE global_variables SET variable_value='1' WHERE variable_name='debug'"
        proxysql_admin "LOAD ADMIN VARIABLES TO RUNTIME"
        proxysql_admin "SELECT variable_name, variable_value FROM runtime_global_variables WHERE variable_name='debug';" | sed 's/^/    /'
    fi
}

run_consistency_test() {
    local polar_mode="${POLAR_MODE:-best_effort}"
    local outcome="${EXPECT_OUTCOME:-success}"
    local timeout_ms=$([[ "$outcome" = "failure" ]] && echo 100 || echo 5000)
    local test_id="${TEST_ID:-$CASE_NUM}"
    local global_wait_timeout_ms="${POLARDB_GLOBAL_WAIT_TIMEOUT_MS:-}"

    # Build case identifier for log file
    local case_id="case${CASE_NUM}"
    [ -n "$polar_mode" ] && [ "$polar_mode" != "best_effort" ] && case_id="${case_id}_${polar_mode}"
    [ "$outcome" = "failure" ] && case_id="${case_id}_failure"

    # Initialize logging
    init_logging "$case_id"

    PASS=0 FAIL=0

    if [ "$XACT_SPLIT" = "1" ] || [ "$CONSISTENCY_MODE" = "2" ] || [ "$CONSISTENCY_MODE" = "4" ]; then
        echo "[$(ts)] POSTPONED: CSN and transaction-split scenarios are kept as scaffold but are not active in v1"
        return 1
    fi

    echo ""
    echo "================================================================"
    echo "[$(ts)] CASE $CASE_NUM: $CASE_NAME"
    echo "================================================================"
    echo "[$(ts)] mode=$CONSISTENCY_MODE split=$SPLIT_ENABLED xact=$XACT_SPLIT"
    echo "[$(ts)] polar_mode=$polar_mode outcome=$outcome timeout=${timeout_ms}ms"
    echo ""

    if ! polardb_require_pgbench; then
        echo "[$(ts)] FATAL: pgbench is not available; build bundled pgbench with: make -C $PROXYSQL_ROOT/test/polardb build"
        return 1
    fi

    # STEP 0: Start ProxySQL
    echo "[$(ts)] STEP 0: Start ProxySQL"
    export PROXYSQL_DEBUG=1
    if ! start_proxysql; then
        echo "[$(ts)] FATAL: Cannot start ProxySQL"
        return 1
    fi

    echo "[$(ts)] ProxySQL debug enabled via PROXYSQL_DEBUG=1"

    # Auto-assign global polar_proxy_wait_timeout_ms per test when not explicitly set:
    # - consistency modes with waits (LSN/CSN/CSN_GLOBAL): success -> 5000ms, failure -> 10ms
    # - modes without waits (OFF/PRIMARY): leave unset
    if [ -z "$global_wait_timeout_ms" ]; then
        if [ "$CONSISTENCY_MODE" = "1" ] || [ "$CONSISTENCY_MODE" = "2" ] || [ "$CONSISTENCY_MODE" = "4" ]; then
            if [ "$outcome" = "failure" ]; then
                global_wait_timeout_ms=10
            else
                # CSN modes can lag behind for a while in steady state.
                # Use a larger timeout to avoid false WARN/ERROR in success runs.
                if [ "$CONSISTENCY_MODE" = "2" ] || [ "$CONSISTENCY_MODE" = "4" ]; then
                    global_wait_timeout_ms=2000
                else
                    global_wait_timeout_ms=5000
                fi
            fi
        fi
    fi

    if [ -n "$global_wait_timeout_ms" ]; then
        echo "[$(ts)] Applying global polar_proxy_wait_timeout_ms=$global_wait_timeout_ms (auto)"
        if ! set_polar_proxy_wait_timeout_ms "$global_wait_timeout_ms"; then
            echo "[$(ts)]   FAIL: could not apply polar_proxy_wait_timeout_ms=$global_wait_timeout_ms"
            return 1
        fi
    fi

    # Create test table
    echo "[$(ts)] Creating test table..."
    query_proxy "DROP TABLE IF EXISTS $TEST_TABLE; CREATE TABLE $TEST_TABLE(id int PRIMARY KEY, data text);"

    # STEP 1: Configure
    echo ""
    echo "[$(ts)] STEP 1: Configure ProxySQL"
    # Note: UPDATE and LOAD must be separate commands (combined with semicolon doesn't work)
    #
    # The PolarDB LSN runtime uses string-valued knobs:
    #   pgsql-polardb_consistency_mode   in {off, lsn, primary}
    #   pgsql-polardb_wait_timeout_mode  in {best_effort, strict}
    # (The source tree used integer knobs plus a split_mode knob; neither exists
    # here, so the numeric CONSISTENCY_MODE / polar_mode are mapped to strings.)
    local cmode="off"
    case "$CONSISTENCY_MODE" in
        0) cmode="off" ;;
        1) cmode="lsn" ;;
        3) cmode="primary" ;;
        *) echo "[$(ts)] WARN: CONSISTENCY_MODE=$CONSISTENCY_MODE has no LSN-only mapping; using 'off'"; cmode="off" ;;
    esac
    local wait_mode="best_effort"
    [[ "$polar_mode" = "best_effort" ]] && wait_mode="best_effort"
    [[ "$polar_mode" = "strict" ]] && wait_mode="strict"
    local max_lag_bytes="${POLARDB_MAX_LAG_BYTES:--1}"

    proxysql_admin "UPDATE global_variables SET variable_value='$cmode' WHERE variable_name='pgsql-polardb_consistency_mode'"
    proxysql_admin "UPDATE global_variables SET variable_value='$wait_mode' WHERE variable_name='pgsql-polardb_wait_timeout_mode'"
    proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME"

    # Per-HG policy (dev schema has no txn_split_enabled column). Mirror the global
    # consistency_mode so routing resolves the same effective mode whether it reads
    # the HG column (consistency_mode_enum) or the global fallback.
    proxysql_admin "UPDATE pgsql_replication_hostgroups SET lsn_wait_timeout_ms=$timeout_ms, consistency_mode='$cmode', max_lag_bytes=$max_lag_bytes"
    proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME"

    proxysql_admin "SELECT variable_name, variable_value FROM runtime_global_variables WHERE variable_name LIKE 'pgsql-polardb%';" | sed 's/^/    /'
    proxysql_admin "SELECT writer_hostgroup, lsn_wait_timeout_ms, consistency_mode, max_lag_bytes FROM runtime_pgsql_replication_hostgroups;" | sed 's/^/    /'

    # STEP 2: Baseline
    echo ""
    echo "[$(ts)] STEP 2: Baseline counters"
    snapshot "B"
    snapshot_pool "B"

    # STEP 3: LSN state. The committed PR benchmark surface is LSN-only;
    # dormant CSN helpers must not probe missing backend functions here.
    echo ""
    echo "[$(ts)] STEP 3: LSN state"
    echo "[$(ts)]   Primary: LSN=$(get_primary_lsn)"
    echo "[$(ts)]   Replica: LSN=$(get_replica_replay_lsn)"

    # STEP 4: Setup lag
    echo ""
    echo "[$(ts)] STEP 4: Setup '$outcome'"
    local early_fail=0
    if [ "$outcome" = "failure" ]; then
        # Force replica lag even for small writes.
        # Use a sizable min lag so replay waits until the gap is large enough.
        local replay_lag_bytes="${POLARDB_REPLAY_LAG_BYTES:-50000}"
        if enable_replay_lag "$replay_lag_bytes"; then
            echo "[$(ts)]   LAG ENABLED"
        else
            echo "[$(ts)]   FAIL: could not enable replay lag"
            early_fail=1
        fi
    else
        if disable_replay_lag; then
            echo "[$(ts)]   LAG DISABLED"
        else
            echo "[$(ts)]   FAIL: could not disable replay lag"
            early_fail=1
        fi
        # For success paths, wait for replica to catch up before running waits.
        if [ "$CONSISTENCY_MODE" = "1" ]; then
            if ! wait_for_replica_lsn_catchup 3; then
                echo "[$(ts)]   FAIL: replica LSN not caught up after 3s"
                early_fail=1
            fi
        elif [ "$CONSISTENCY_MODE" = "2" ] || [ "$CONSISTENCY_MODE" = "4" ]; then
            if ! wait_for_replica_csn_catchup 3; then
                echo "[$(ts)]   FAIL: replica CSN not caught up after 3s"
                early_fail=1
            fi
        fi
    fi

    # STEP 5: Execute
    echo ""
    echo "[$(ts)] STEP 5: Execute query"
    local t0 t1 result count has_warn has_err pgbench_out pgbench_exit
    local split_sql="$RUN_DIR/split_test.sql"
    local session_sql="$RUN_DIR/session_test.sql"

  if [ "$early_fail" -eq 1 ]; then
    echo "[$(ts)]   SKIPPED (early failure in STEP 4)"
    FAIL=$((FAIL+1))
    t0=0; t1=0; count=""; has_warn=0; has_err=0; result=""
  elif [ "$XACT_SPLIT" = "1" ]; then
        # Pre-warm the replica pool using lib functions
        if ! warmup_replica_pool "$POLARDB_READER_HG" 3; then
            echo "[$(ts)]   FAIL: replica warmup failed"
            early_fail=1; FAIL=$((FAIL+1))
        fi

      if [ "$early_fail" -eq 0 ]; then
        # Split transaction via pgbench
        # pgbench sends each statement individually over persistent connection
        # This allows ProxySQL to capture XIDs after INSERT and route SELECT to replica
        echo "[$(ts)]   Split transaction via pgbench (statements sent individually)"

        local sleep_ms=200
        [ "$outcome" = "failure" ] && sleep_ms=500

        case "${SPLIT_VARIANT:-basic}" in
            prewrite)
                # Pre-write read + split: autocommit INSERT sets session_write_lsn,
                # then BEGIN → SELECT (consistency wait, no XIDs) → INSERT (gets XIDs) → SELECT → SELECT (split) → COMMIT
                # Two post-write SELECTs: first may hit wal_pending, second splits.
                cat > "$split_sql" << EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'prewrite_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'prewrite_setup$CASE_NUM';
\sleep ${sleep_ms}ms
BEGIN;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
            multi)
                # Multiple splits: BEGIN → INSERT → SELECT → SELECT → COMMIT
                # Two SELECTs after INSERT → FSM cycles TXN_SPLITTABLE → SPLIT_READ_ACTIVE twice
                cat > "$split_sql" << EOSQL
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
            readonly)
                # Read-only txn: autocommit INSERT sets session_write_lsn,
                # then BEGIN → SELECT → SELECT → COMMIT (no writes in txn, no split)
                cat > "$split_sql" << EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'readonly_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'readonly_setup$CASE_NUM';
\sleep ${sleep_ms}ms
BEGIN;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
            extended)
                # Extended protocol veto: same script as basic, but run with -M extended.
                # Extended protocol forces primary (RC4 veto) for wrapped queries.
                cat > "$split_sql" << EOSQL
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'ext_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'ext_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
            combined)
                # Combined workload: mix of autocommit, consistency-wait, and split patterns.
                # Phase 1: Autocommit INSERT (sets session_write_lsn)
                # Phase 2: Autocommit SELECT (consistency wait using session LSN)
                # Phase 3: Split txn (BEGIN → INSERT → SELECT → SELECT → COMMIT)
                # Phase 4: Read-only txn (BEGIN → SELECT → COMMIT, no split)
                cat > "$split_sql" << EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'combined_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'combined_setup$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'combined_split$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'combined_split$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
\sleep ${sleep_ms}ms
BEGIN;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
            *)
                # basic (case 6): BEGIN → INSERT → sleep → SELECT → SELECT → COMMIT
                # Two SELECTs: first may hit wal_pending (stays on primary, flushes WAL),
                # second sees wal_pending=0 and splits to replica.
                cat > "$split_sql" << EOSQL
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
                ;;
        esac
        echo "[$(ts)]   Script:"
        cat "$split_sql" | sed 's/^/    /'

        # WAL generator: concurrent writes flush WAL buffers, clearing wal_pending.
        # - Success mode: start BEFORE pgbench so server is already busy when test
        #   SELECT arrives (wal_pending=0 → split happens).
        # - Failure mode: sustained WAL keeps replica behind the wait target.
        if [ "$outcome" = "failure" ]; then
            start_wal_generator 0.01 1000
        else
            start_wal_generator 0 200
            sleep 0.3  # let WAL generator warm up so wal_pending clears
        fi

        t0=$(date +%s%3N)
        # -n: no vacuum, -t 1: one transaction, -c 1: one client
        # Use our built libpq which has PQpipelineStatus (required by pg16 pgbench)
        local pgbench_mode="simple"
        [ "${SPLIT_VARIANT:-basic}" = "extended" ] && pgbench_mode="extended"
        echo "[$(ts)]   Protocol mode: -M $pgbench_mode"
        pgbench_out=$(polardb_pgbench_script "$pgbench_mode" "$split_sql" 2>&1)
        pgbench_exit=$?
        t1=$(date +%s%3N)

        echo "[$(ts)]   pgbench exit code: $pgbench_exit"
        echo "[$(ts)]   pgbench output:"
        echo "$pgbench_out" | sed 's/^/    /'

        stop_wal_generator

        # Keep the generated SQL in RUN_DIR for debugging.

        # Verify the data is there
        result=$(query_proxy "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=$test_id;" 2>&1)
        count=$(echo "$result" | grep -E '^[0-9]+$' | head -1)
      fi  # inner early_fail guard for split block
    else
        # Non-split: simple INSERT then SELECT
        echo "[$(ts)]   INSERT then SELECT (non-split)"

        # For consistency modes that need reader routing (LSN=1, CSN=2, CSN_GLOBAL=4),
        # setup query rules to send SELECTs to HG 11 and warmup the pool
        if [ "$CONSISTENCY_MODE" = "1" ] || [ "$CONSISTENCY_MODE" = "2" ] || [ "$CONSISTENCY_MODE" = "4" ]; then
            if ! setup_reader_routing "$POLARDB_READER_HG" 3; then
                echo "[$(ts)]   FAIL: reader routing warmup failed"
                early_fail=1; FAIL=$((FAIL+1))
            fi
        fi

      if [ "$early_fail" -eq 0 ]; then
        if [ "$CONSISTENCY_MODE" = "1" ] || [ "$CONSISTENCY_MODE" = "2" ]; then
            # For session LSN/CSN, keep the same session so ProxySQL can apply wait logic.
            if [ "$outcome" = "failure" ]; then
                cat > "$session_sql" << EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data='case$CASE_NUM';
\\sleep 500ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
EOSQL
            else
                cat > "$session_sql" << EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data='case$CASE_NUM';
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
EOSQL
            fi
            echo "[$(ts)]   Script (session, non-split):"
            cat "$session_sql" | sed 's/^/    /'

            # For failure scenarios, keep WAL moving so the replica stays behind
            # long enough for the wait to hit timeout and emit WARNING/ERROR.
            if [ "$outcome" = "failure" ]; then
                start_wal_generator 0.01 1000
            fi

            t0=$(date +%s%3N)
            pgbench_out=$(polardb_pgbench_script simple "$session_sql" 2>&1)
            pgbench_exit=$?
            t1=$(date +%s%3N)

            if [ "$outcome" = "failure" ]; then
                stop_wal_generator
            fi

            echo "[$(ts)]   pgbench exit code: $pgbench_exit"
            echo "[$(ts)]   pgbench output:"
            echo "$pgbench_out" | sed 's/^/    /'

            # Keep the generated SQL in RUN_DIR for debugging.

            # Verify committed data on the primary. Failure variants may leave
            # replay lag enabled until cleanup, so a fresh proxy session can
            # legitimately read a stale replica and return 0.
            if [ "$outcome" = "failure" ]; then
                result=$(polardb_primary_sql "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=$test_id;" 2>&1)
            else
                result=$(query_proxy "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=$test_id;" 2>&1)
            fi
            count=$(echo "$result" | grep -E '^[0-9]+$' | head -1)
        else
            query_proxy "INSERT INTO $TEST_TABLE VALUES ($test_id,'case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data='case$CASE_NUM';"
            t0=$(date +%s%3N)
            echo "[DEBUG] About to run SELECT..."
            result=$(query_proxy "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=$test_id;" 2>&1)
            t1=$(date +%s%3N)
            echo "[DEBUG] Raw result: '$result'"
            echo "[DEBUG] Result length: ${#result}"
            count=$(echo "$result" | grep -E '^[0-9]+$' | head -1)
            echo "[DEBUG] Extracted count: '$count'"
        fi
      fi  # inner early_fail guard for non-split block
    fi

  if [ "$early_fail" -eq 0 ]; then
    local qtime=$((t1-t0))
    if [ "$XACT_SPLIT" = "1" ] || [ "$CONSISTENCY_MODE" = "1" ] || [ "$CONSISTENCY_MODE" = "2" ]; then
        # In split tests, the wait/timeout happens inside pgbench.
        # Use pgbench output to detect WARNING/ERROR, since the follow-up
        # query runs on a new session and won't see the split error.
        has_warn=$(echo "$pgbench_out" | grep -c WARNING || true)
        has_err=$(echo "$pgbench_out" | grep -c ERROR || true)
    else
        has_warn=$(echo "$result" | grep -c WARNING || true)
        has_err=$(echo "$result" | grep -c ERROR || true)
    fi

    echo "[$(ts)]   Time: ${qtime}ms COUNT=$count WARN=$has_warn ERR=$has_err"
    [ -n "$result" ] && echo "$result" | head -3 | sed 's/^/    /'

    # STEP 6: Post-query counters
    echo ""
    echo "[$(ts)] STEP 6: Counters"
    snapshot "A"
    snapshot_pool "A"

    echo ""
    echo "--- Global Stats (stats_pgsql_global) ---"
    print_deltas

    echo ""
    echo "--- Per-Hostgroup Routing (stats_pgsql_connection_pool) ---"
    echo "    HG 10 = writer (primary), HG 11 = reader (replica)"
    print_pool_deltas

    print_errors

    # STEP 7: Verify
    echo ""
    echo "[$(ts)] STEP 7: Verify"

    # Common stats (skip for extended protocol — no query wrapping means no LSN updates)
    if [ "${SPLIT_VARIANT:-basic}" != "extended" ]; then
        verify_preset "$outcome" "${COMMON_STATS[@]}"
    fi

    if [ "$XACT_SPLIT" = "1" ]; then
        # Split path: select preset based on variant
        case "${SPLIT_VARIANT:-basic}" in
            prewrite)
                verify_preset "$outcome" "${SPLIT_PREWRITE_STATS[@]}"
                ;;
            multi)
                verify_preset "$outcome" "${SPLIT_MULTI_STATS[@]}"
                ;;
            readonly)
                verify_preset "$outcome" "${READONLY_TXN_STATS[@]}"
                ;;
            extended)
                verify_preset "$outcome" "${EXTENDED_VETO_STATS[@]}"
                ;;
            combined)
                verify_preset "$outcome" "${COMBINED_WORKLOAD_STATS[@]}"
                ;;
            *)
                # basic (case 6): standard split + LSN wait
                verify_preset "$outcome" "${SPLIT_LSN_STATS[@]}"
                verify_preset "$outcome" "${SPLIT_STATS[@]}"
                ;;
        esac
    else
        # Non-split path: use wait/timeout counters
        case "$CONSISTENCY_MODE" in
            0) verify_preset "$outcome" "${EVENTUAL_STATS[@]}" ;;
            1) verify_preset "$outcome" "${LSN_STATS[@]}" ;;
            2) verify_preset "$outcome" "${CSN_STATS[@]}" ;;
            3) verify_preset "$outcome" "${PRIMARY_STATS[@]}" ;;
            4) verify_preset "$outcome" "${GLOBAL_CSN_STATS[@]}" ;;
        esac
    fi

    # Result checks
    if [ "$outcome" = "success" ]; then
        if [ "$count" = "1" ]; then
            echo "[$(ts)]   PASS: COUNT=1"
            PASS=$((PASS+1))
        else
            echo "[$(ts)]   FAIL: COUNT=$count"
            FAIL=$((FAIL+1))
        fi
        if [ "$has_warn" = "0" ] && [ "$has_err" = "0" ]; then
            echo "[$(ts)]   PASS: No WARN/ERR"
            PASS=$((PASS+1))
        else
            echo "[$(ts)]   FAIL: WARN=$has_warn ERR=$has_err"
            FAIL=$((FAIL+1))
        fi
    else
        if [ "$qtime" -ge $((timeout_ms-20)) ]; then
            echo "[$(ts)]   PASS: Timeout ${qtime}ms"
            PASS=$((PASS+1))
        else
            echo "[$(ts)]   WARN: Fast ${qtime}ms"
        fi
        if [ "$polar_mode" = "best_effort" ]; then
            if [ "$has_warn" -ge 1 ]; then
                echo "[$(ts)]   PASS: Got WARNING"
                PASS=$((PASS+1))
            else
                echo "[$(ts)]   FAIL: No WARNING"
                FAIL=$((FAIL+1))
            fi
        else
            local retry_delta
            retry_delta=$(delta "PolarDB_Wait_Reads_Retried_On_Writer")
            if [ "$has_err" = "0" ] && [ "$has_warn" = "0" ] && [ "$retry_delta" -ge 1 ]; then
                echo "[$(ts)]   PASS: Strict timeout retried on writer"
                PASS=$((PASS+1))
            else
                echo "[$(ts)]   FAIL: Strict timeout retry mismatch WARN=$has_warn ERR=$has_err retry_delta=$retry_delta"
                FAIL=$((FAIL+1))
            fi
        fi
    fi
  fi  # early_fail guard

    # STEP 8: Cleanup and Log Extraction (ALWAYS runs — even on early failure)
    echo ""
    echo "[$(ts)] STEP 8: Cleanup"
    stop_wal_generator 2>/dev/null || true
    disable_replay_lag 2>/dev/null || true
    proxysql_admin "UPDATE pgsql_replication_hostgroups SET lsn_wait_timeout_ms=5000; LOAD PGSQL SERVERS TO RUNTIME;" 2>/dev/null || true

    # Extract logs BEFORE stopping ProxySQL (while log is still being written)
    extract_logs

    stop_proxysql

    # Summary
    echo ""
    echo "================================================================"
    echo "[$(ts)] CASE $CASE_NUM: Passed=$PASS Failed=$FAIL"
    echo "================================================================"
    echo "[$(ts)] Finished: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "[$(ts)] Run dir: $RUN_DIR"

    if [ "$FAIL" -eq 0 ]; then
        echo "[$(ts)] RESULT: PASS"
        return 0
    else
        echo "[$(ts)] RESULT: FAIL"
        return 1
    fi
}
