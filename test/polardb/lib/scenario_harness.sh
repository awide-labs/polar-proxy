#!/usr/bin/env bash
#
# scenario_harness.sh - shared PolarDB scenario test harness
#
# Combines shared topology, ProxySQL, stats, logging, and scenario helpers.
#
# This file contains the helpers and runner used by test-bench/test-case*.sh.
# General TAP tests use lib/tap_polardb.sh instead. Keep helpers here only when
# the scenario runner or one of those case scripts calls them; future feature
# designs belong in the architecture documents until an executable test exists.
#
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# COUNTER CATALOGUE -- active LSN + split counters.
# This block enumerates stats_pgsql_global counters with their C++ source-line
# citations. Treat the lib/*.cpp:NNN line numbers as stale hints, not guarantees.
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
#   P+  - Delta must be >= 1 when the profile-only counter exists
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
# Transaction-split counters are active for XACT_SPLIT=1 cases.
# Source: PgHGM->status (PgSQL_HostGroups_Manager.h:795-879)
# All PolarDB stats are atomic counters for thread-safe access.
#
# --- Split Execution (lines 817-821) ---
#   PolarDB_Split_Reads_Total          - Total split read attempts
#   PolarDB_Split_Reads_Success        - Successful splits to replica
#   PolarDB_Split_Reads_Fallback       - Plan-time fallback to primary without replica dispatch
#   PolarDB_Split_Reads_Error          - Split read errors
#   PolarDB_Split_No_Backend           - No backend available
#
# --- Split Decision (lines 811-815) ---
#   PolarDB_Queries_In_Splittable_Txn  - Queries in splittable transactions
#   PolarDB_Queries_Split_Eligible     - Passed all split checks
#   PolarDB_Split_Rejected_Not_Select  - Rejected: not SELECT
#   PolarDB_Split_Rejected_For_Update  - Rejected: FOR UPDATE/SHARE
#   PolarDB_Split_Rejected_Write_LSN_Unknown - Rejected: prior write RFQ missed LSN
#   PolarDB_Split_Rejected_Observed_LSN_Unknown - Rejected: tracked read RFQ missed LSN
#   PolarDB_Split_Rejected_No_Marker   - Rejected: no usable split or pre-write RFQ marker
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
#   PolarDB_Wait_Wrap_Bypassed         - Wait wrapper skipped because selected reader was already fresh
#   PolarDB_Split_LSN_Wait_Sum_Us      - Total LSN wait time (us)
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
#   PolarDB_Split_Warmup_Requested     - Warmup base requests queued
#   PolarDB_Split_Warmup_Target_Attempts - Target backend connect attempts produced by warmup
#   PolarDB_Split_Warmup_Created       - Warmup connections created
#   PolarDB_Split_Warmup_Failed        - Warmup base requests rejected before target work
#   PolarDB_Split_Warmup_Target_Failed - Warmup target backends failed before pooling
#   PolarDB_Split_Warmup_Already_Warm  - Compatible free backend already existed
#   PolarDB_Split_Warmup_Dedup_Queued  - Request deduped in queue
#   PolarDB_Split_Warmup_Dedup_Inflight - Request deduped in drain
#   PolarDB_Split_Warmup_Queue_Full    - Queue-limit drop
#   PolarDB_Split_Warmup_No_Target     - No eligible reader target for a base request
#   PolarDB_Split_Warmup_Bad_Request   - Invalid warmup request
#   PolarDB_Split_Warmup_Connect_Failed - Reader warmup connect failed
#   PolarDB_Split_Warmup_Add_Failed - Connected reader could not be added to the pool
#   PolarDB_Warmup_Pending             - Currently pending warmups
#
# --- Latency (lines 863-865) ---
#   PolarDB_Split_Latency_Sum_Us       - Sum of split latencies (us)
#   PolarDB_Split_Latency_Count        - Number of latency samples
#
# ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
# END COUNTER CATALOGUE.
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
# harness used string-built commands and eval; these wrappers preserve the same
# behavior without word-splitting surprises.
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
	PGPASSWORD="$PGPASSWORD" PGSSLMODE="$PGSSLMODE" "$PGBENCH_BIN" \
		-h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
		-U "$PGUSER" -d "$PGDB" \
		-n -t 1 -c 1 -M "$protocol_mode" \
		-f "$sql_file"
}

# State tracking
WAL_GEN_PID=""
TEST_TABLE="${TEST_TABLE:-$(polardb_test_identifier consistency_test)}"

# Colors
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

#------------------------------------------------------------------------------
# Output Functions
#------------------------------------------------------------------------------

ts() { date "+%H:%M:%S.%3N"; }

log_info() {
	echo -e "[$(ts)] $1"
}

log_warning() {
	echo -e "${YELLOW}[$(ts)] ⚠ $1${NC}"
}

log_error() {
	echo -e "${RED}[$(ts)] ✗ $1${NC}"
}

#------------------------------------------------------------------------------
# Cluster Topology Auto-Detection
#------------------------------------------------------------------------------

# Select writer/reader endpoints from .env-provided POLARDB_ENDPOINTS or from
# manual PRIMARY_*/REPLICA_* variables. The committed scripts do not embed lab
# addresses; environment-specific details belong in test/polardb/.env.
detect_cluster_topology() {
	if polardb_detect_topology; then
		log_info "Topology: writer=$PRIMARY_HOST:$PRIMARY_PORT readers=${POLARDB_REPLICA_ENDPOINTS:-$REPLICA_HOST:$REPLICA_PORT}"
		return 0
	fi
	log_error "Topology detection failed; set POLARDB_ENDPOINTS or PRIMARY_/REPLICA_ variables"
	return 1
}

if [ "${POLARDB_AUTODETECT:-1}" = "1" ]; then
	detect_cluster_topology || true
fi

polardb_register_pgsql_servers() {
	local writer_hg="$1"
	local reader_hg="$2"
	local max_connections="${3:-100}"
	local endpoint host port

	proxysql_admin "DELETE FROM pgsql_servers;"
	proxysql_admin "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($writer_hg, '$PRIMARY_HOST', $PRIMARY_PORT, 'ONLINE', 1000, $max_connections);"
	for endpoint in $(polardb_each_replica_endpoint); do
		host=$(polardb_endpoint_host "$endpoint")
		port=$(polardb_endpoint_port "$endpoint")
		proxysql_admin "INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, weight, max_connections) VALUES ($reader_hg, '$host', $port, 'ONLINE', 1000, $max_connections);"
	done
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

	# Stop only the WAL generator started by this harness.
	stop_wal_generator

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
	local current

	current=$(polardb_direct_sql "$REPLICA_HOST" "$REPLICA_PORT" \
		"SELECT current_setting('$key', true);" 2>/dev/null | tr -d '[:space:]')
	if [ "$current" = "$value" ]; then
		return 0
	fi

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
			if [ "$expected_ms" -eq 0 ] &&
				{ [ "$current" = "0" ] || [ "$current" = "0s" ]; }; then
				return 0
			fi
			if [ $((expected_ms % 1000)) -eq 0 ] && [ "$current" = "$((expected_ms / 1000))s" ]; then
				return 0
			fi
		fi
		if [[ "$expected" =~ ^[0-9]+$ ]] && [ "$current" = "$expected" ]; then
			return 0
		fi
		if [ $(($(date +%s) - start_ts)) -ge "$timeout_sec" ]; then
			log_warning "Timed out waiting for $name=$expected on $host:$port (current=${current:-<empty>})"
			return 1
		fi
		sleep 0.5
	done
}

set_polar_proxy_wait_timeout_ms() {
	local timeout_ms="$1"
	if [ -z "$timeout_ms" ]; then
		log_info "polar_proxy_wait_timeout_ms not set"
		return 0
	fi

	log_info "Setting polar_proxy_wait_timeout_ms=$timeout_ms through Patroni DCS"
	polardb_dcs_set "polar_proxy_wait_timeout_ms=$timeout_ms" >/dev/null || return 1
	wait_for_guc_value "$PRIMARY_HOST" "$PRIMARY_PORT" polar_proxy_wait_timeout_ms "${timeout_ms}ms" 10 || return 1
	wait_for_guc_value "$REPLICA_HOST" "$REPLICA_PORT" polar_proxy_wait_timeout_ms "${timeout_ms}ms" 10 || return 1
}

get_polar_proxy_wait_timeout_ms() {
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"SELECT (extract(epoch FROM current_setting('polar_proxy_wait_timeout_ms')::interval) * 1000)::bigint;" \
		2>/dev/null | tr -d '[:space:]'
}

restore_polar_proxy_wait_timeout_ms() {
	local timeout_ms="$1"

	[ -n "$timeout_ms" ] || return 0
	log_info "Restoring polar_proxy_wait_timeout_ms=$timeout_ms through Patroni DCS"
	set_polar_proxy_wait_timeout_ms "$timeout_ms"
}

get_polar_query_delay_us() {
	local target="$1"
	local host port
	local raw

	case "$target" in
	primary | writer)
		host="$PRIMARY_HOST"
		port="$PRIMARY_PORT"
		;;
	replica | reader)
		host="$REPLICA_HOST"
		port="$REPLICA_PORT"
		;;
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
	*s) awk -v v="${raw%s}" 'BEGIN { printf "%d\n", v * 1000000 }' ;;
	*) awk -v v="$raw" 'BEGIN { printf "%d\n", v }' ;;
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
		if [ $(($(date +%s) - start_ts)) -ge "$timeout_sec" ]; then
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
	local max_iters="${2:-0}" # 0 = run forever
	log_info "Starting WAL generator (delay=${delay_sec}s, max_iters=${max_iters})..."
	(
		[ "$delay_sec" != "0" ] && sleep "$delay_sec"
		local i=0
		while true; do
			polardb_primary_sql "INSERT INTO $TEST_TABLE SELECT 0, repeat('x', 200) ON CONFLICT (id) DO UPDATE SET data=repeat('x',200);" >/dev/null 2>&1
			i=$((i + 1))
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
		WAL_GEN_PID=""
	fi
}

#------------------------------------------------------------------------------
# LSN Functions (direct to PolarDB)
#------------------------------------------------------------------------------

# Active LSN getters (used by wait_for_replica_lsn_catchup, the LSN TAP slice).
get_primary_lsn() { polardb_primary_sql "SELECT pg_current_wal_lsn();"; }
get_replica_replay_lsn() { polardb_replica_sql "SELECT pg_last_wal_replay_lsn();"; }
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
		if [ $(($(date +%s) - start_ts)) -ge "$timeout_sec" ]; then
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
	# One ProxySQL admin operation per call. In particular, issue the MEMORY
	# UPDATE and LOAD ... TO RUNTIME as separate calls; a combined string can
	# execute only its first statement.
	PGPASSWORD="$PROXYSQL_ADMIN_PASSWORD" PGSSLMODE="$PROXYSQL_ADMIN_PGSSLMODE" psql \
		-h "$PROXYSQL_HOST" -p "$PROXYSQL_ADMIN_PORT" \
		-U "$PROXYSQL_ADMIN_USER" -d "$PROXYSQL_ADMIN_DATABASE" \
		-A -t -c "$query"
}

polardb_consistency_uses_lsn_wait() {
	case "$1" in
	session_lsn | global_lsn) return 0 ;;
	*) return 1 ;;
	esac
}

# Configure the global and hostgroup policy used by one scenario, then load
# each table once. Keeping the updates together prevents tests from observing a
# partially applied policy.
configure_polardb_scenario_policy() {
	local consistency_mode="$1"
	local timeout_action="$2"
	local transaction_split="$3"
	local wait_timeout_ms="$4"
	local split_variant="$5"
	local max_lag_bytes="${POLARDB_MAX_LAG_BYTES:--1}"
	local connect_timeout_ms="${POLARDB_SPLIT_CONNECT_TIMEOUT_MS:-5000}"
	local lazy_warmup_split="${POLARDB_LAZY_WARMUP_SPLIT:-1}"
	local proxy_identity_mode="${POLARDB_PROXY_IDENTITY_MODE:-proxy}"
	local read_target="${READ_TARGET:-replica}"
	local read_fallback_action="${READ_FALLBACK_ACTION:-primary}"

	[ "$split_variant" = "lazy_disabled" ] && lazy_warmup_split=0
	case "$split_variant" in
	proxy_identity_client) proxy_identity_mode="client" ;;
	proxy_identity_proxy) proxy_identity_mode="proxy" ;;
	esac
	proxysql_admin "UPDATE global_variables SET variable_value='$consistency_mode' WHERE variable_name='pgsql-polardb_consistency_mode'"
	proxysql_admin "UPDATE global_variables SET variable_value='$read_target' WHERE variable_name='pgsql-polardb_read_target'"
	proxysql_admin "UPDATE global_variables SET variable_value='$read_fallback_action' WHERE variable_name='pgsql-polardb_action_read_fallback'"
	proxysql_admin "UPDATE global_variables SET variable_value='$timeout_action' WHERE variable_name='pgsql-polardb_action_lsn_timeout'"
	proxysql_admin "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol'"
	proxysql_admin "UPDATE global_variables SET variable_value='$lazy_warmup_split' WHERE variable_name='pgsql-polardb_lazy_warmup_split'"
	proxysql_admin "UPDATE global_variables SET variable_value='$proxy_identity_mode' WHERE variable_name='pgsql-polardb_proxy_identity_mode'"
	if [ "$transaction_split" = "1" ]; then
		# Keep backend creation timing stable in split tests so they measure the
		# split behavior rather than temporary host load.
		proxysql_admin "UPDATE global_variables SET variable_value='$connect_timeout_ms' WHERE variable_name='pgsql-connect_timeout_server'"
	fi
	proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME"

	proxysql_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=$transaction_split, lsn_wait_timeout_ms=$wait_timeout_ms, consistency_mode='$consistency_mode', max_lag_bytes=$max_lag_bytes, proxy_protocol='v15'"
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME"
}

show_polardb_runtime_policy() {
	proxysql_admin "SELECT variable_name, variable_value FROM runtime_global_variables WHERE variable_name LIKE 'pgsql-polardb%';"
	proxysql_admin "SELECT writer_hostgroup, txn_split_enabled, lsn_wait_timeout_ms, consistency_mode, max_lag_bytes, proxy_protocol FROM runtime_pgsql_replication_hostgroups;"
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

#------------------------------------------------------------------------------
# Query Execution Functions via ProxySQL
#------------------------------------------------------------------------------

# Execute query via ProxySQL (no special wait settings)
query_proxy() {
	local query="$1"
	polardb_proxy_sql "$query"
}

wait_for_proxy_frontend() {
	local timeout_sec="${1:-10}"
	local waited=0

	while [ "$waited" -lt "$timeout_sec" ]; do
		# VALUES reaches the PostgreSQL frontend and the default primary
		# hostgroup without matching this harness's replica-eligible ^SELECT
		# rule. Readiness must not warm the replica pool before cold-pool tests.
		if query_proxy "VALUES (1)" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
		waited=$((waited + 1))
	done
	return 1
}

#------------------------------------------------------------------------------
# Query Rules Functions
#------------------------------------------------------------------------------

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
		conn_free=$(hostgroup_free_connection_count "$hg")
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

hostgroup_free_connection_count() {
	local hostgroup="$1"
	proxysql_admin "SELECT COALESCE(SUM(ConnFree), 0) FROM stats_pgsql_connection_pool WHERE hostgroup=$hostgroup;" \
		2>/dev/null | grep -E '^[0-9]+$' | head -1
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
	if command -v flock >/dev/null 2>&1; then
		touch "$RUNID_FILE"
		exec 8<>"$RUNID_FILE"
		flock 8
		if [ -s "$RUNID_FILE" ]; then
			runid=$(cat "$RUNID_FILE")
			runid=$((runid + 1))
		fi
		echo "$runid" >"$RUNID_FILE"
		echo "$runid"
		return 0
	fi

	if [ -f "$RUNID_FILE" ]; then
		runid=$(cat "$RUNID_FILE")
		runid=$((runid + 1))
	fi
	echo "$runid" >"$RUNID_FILE"
	echo "$runid"
}

# Log file locations. ProxySQL logs default to the wrapper data directory.
# Backend log collection is optional: set POLARDB_LOG_DIR or explicit
# POLARDB_PRIMARY_LOG/POLARDB_REPLICA_LOG in test/polardb/.env when a local
# environment exposes server logs on the filesystem.
PROXYSQL_DEFAULT_DATA_DIR="$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_test_data")"
PROXYSQL_LOG="${PROXYSQL_LOG:-${PROXYSQL_DATA_DIR:-$PROXYSQL_DEFAULT_DATA_DIR}/proxysql.log}"
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
	tee -a "$LOG_FILE" <"$_LOG_PIPE" &
	_LOG_TEE_PID=$!
	exec >"$_LOG_PIPE" 2>&1

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
        }' "$log_file" >"$output_file" 2>/dev/null
	}

	# --- Extract all logs in PARALLEL ---
	local pids=""

	# ProxySQL (LOCAL time)
	if [ -f "$PROXYSQL_LOG" ]; then
		(
			extract_time_range "$PROXYSQL_LOG" "$RUN_DIR/proxysql.log" "$start_local_cmp" "$end_local_cmp"
			grep -aiE "polar|split|lsn|csn|xid|replica|hostgroup|hg_id|backend|LIBPQ|\[(H|RQ|SH|W|S)" \
				"$RUN_DIR/proxysql.log" >"$RUN_DIR/proxysql_debug.log" 2>/dev/null || true
		) &
		pids="$pids $!"
	fi

	# PolarDB Primary (UTC time)
	if [ -n "$POLARDB_PRIMARY_LOG" ] && [ -f "$POLARDB_PRIMARY_LOG" ]; then
		(
			extract_time_range "$POLARDB_PRIMARY_LOG" "$RUN_DIR/polardb_primary.log" "$start_utc_cmp" "$end_utc_cmp"
			grep -E "SPLIT_DEBUG|VISIBILITY_DEBUG|BUFMGR_DEBUG|polar_|CSN|LSN" \
				"$RUN_DIR/polardb_primary.log" >"$RUN_DIR/polardb_primary_debug.log" 2>/dev/null || true
		) &
		pids="$pids $!"
	fi

	# PolarDB Replica (UTC time)
	if [ -n "$POLARDB_REPLICA_LOG" ] && [ -f "$POLARDB_REPLICA_LOG" ]; then
		(
			extract_time_range "$POLARDB_REPLICA_LOG" "$RUN_DIR/polardb_replica.log" "$start_utc_cmp" "$end_utc_cmp"
			grep -E "SPLIT_DEBUG|VISIBILITY_DEBUG|BUFMGR_DEBUG|polar_|CSN|LSN" \
				"$RUN_DIR/polardb_replica.log" >"$RUN_DIR/polardb_replica_debug.log" 2>/dev/null || true
		) &
		pids="$pids $!"
	fi

	# Wait for all extractions to complete
	for pid in $pids; do
		wait "$pid" 2>/dev/null || true
	done

	# Report results
	if [ -f "$RUN_DIR/proxysql.log" ]; then
		local proxy_lines=$(wc -l <"$RUN_DIR/proxysql.log" 2>/dev/null || echo 0)
		local proxy_debug=$(wc -l <"$RUN_DIR/proxysql_debug.log" 2>/dev/null || echo 0)
		echo "[$(ts)]   ProxySQL:      $proxy_lines lines, $proxy_debug debug"
	else
		echo "[$(ts)]   ProxySQL: NOT FOUND"
	fi

	if [ -f "$RUN_DIR/polardb_primary.log" ]; then
		local prim_lines=$(wc -l <"$RUN_DIR/polardb_primary.log" 2>/dev/null || echo 0)
		local prim_debug=$(wc -l <"$RUN_DIR/polardb_primary_debug.log" 2>/dev/null || echo 0)
		echo "[$(ts)]   Primary:       $prim_lines lines, $prim_debug debug"
	else
		echo "[$(ts)]   Primary: NOT FOUND"
	fi

	if [ -f "$RUN_DIR/polardb_replica.log" ]; then
		local repl_lines=$(wc -l <"$RUN_DIR/polardb_replica.log" 2>/dev/null || echo 0)
		local repl_debug=$(wc -l <"$RUN_DIR/polardb_replica_debug.log" 2>/dev/null || echo 0)
		echo "[$(ts)]   Replica:       $repl_lines lines, $repl_debug debug"
	else
		echo "[$(ts)]   Replica: NOT FOUND"
	fi

	# --- Summary file ---
	cat >"$RUN_DIR/README.txt" <<EOF
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

# ProxySQL instance defaults. POLARDB_TEST_SHARD in env.sh changes these only
# when the caller did not provide explicit PROXYSQL_* values.
PROXYSQL_DATA_DIR_LOCAL=${PROXYSQL_DATA_DIR:-$PROXYSQL_DEFAULT_DATA_DIR}
PROXYSQL_ADMIN_PORT_LOCAL=$PROXYSQL_ADMIN_PORT
PROXYSQL_PORT_LOCAL=$PROXYSQL_PORT
PROXYSQL_MYSQL_ADMIN_PORT_LOCAL=$PROXYSQL_MYSQL_ADMIN_PORT

start_proxysql() {
	echo "[$(ts)] Starting ProxySQL via wrapper..."

	if [ ! -x "$WRAPPER_SCRIPT" ]; then
		echo "[$(ts)] ERROR: Wrapper script not found: $WRAPPER_SCRIPT"
		return 1
	fi

	if ! "$WRAPPER_SCRIPT" start \
		--data-dir "$PROXYSQL_DATA_DIR_LOCAL" \
		--admin-port "$PROXYSQL_ADMIN_PORT_LOCAL" \
		--proxy-port "$PROXYSQL_PORT_LOCAL" \
		--mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT_LOCAL"; then
		echo "[$(ts)] ERROR: ProxySQL failed to start"
		# Keep the next TAP case isolated even if the lifecycle helper started a
		# process that never reached readiness.
		"$WRAPPER_SCRIPT" stop --data-dir "$PROXYSQL_DATA_DIR_LOCAL" >/dev/null 2>&1 || true
		PROXYSQL_STARTED=0
		return 1
	fi

	PROXYSQL_STARTED=1
	echo "[$(ts)] ProxySQL started on ports admin=$PROXYSQL_ADMIN_PORT_LOCAL proxy=$PROXYSQL_PORT_LOCAL mysql_admin=$PROXYSQL_MYSQL_ADMIN_PORT_LOCAL"

	# Configure servers
	echo "[$(ts)] Configuring PolarDB servers..."
	polardb_register_pgsql_servers "$POLARDB_WRITER_HG" "$POLARDB_READER_HG" 100
	proxysql_admin "DELETE FROM pgsql_replication_hostgroups;"
	# The LSN session-consistency schema stores only PolarDB topology and policy
	# columns used by this harness; STEP 1 overrides consistency_mode per
	# scenario.
	proxysql_admin "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, comment) VALUES ($POLARDB_WRITER_HG, $POLARDB_READER_HG, 'polardb', 'default', -1, -1, '');"
	proxysql_admin "DELETE FROM pgsql_users;"
	proxysql_admin "INSERT INTO pgsql_users (username, password, default_hostgroup) VALUES ('$PGUSER', '$PGPASSWORD', $POLARDB_WRITER_HG);"

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

	# Admin readiness can precede the PostgreSQL frontend listener in debug
	# builds. Keep this bounded so a broken start still fails this case quickly.
	if wait_for_proxy_frontend 10; then
		echo "[$(ts)] ProxySQL connection verified"
	else
		echo "[$(ts)] ERROR: Cannot connect through ProxySQL"
		# Admin readiness is not enough for scenario tests; clean up a process
		# that cannot accept frontend PostgreSQL traffic before the next case.
		stop_proxysql >/dev/null 2>&1 || true
		return 1
	fi

	return 0
}

stop_proxysql() {
	if [ "$PROXYSQL_STARTED" = "1" ]; then
		echo "[$(ts)] Stopping ProxySQL..."
		"$WRAPPER_SCRIPT" stop --data-dir "$PROXYSQL_DATA_DIR_LOCAL" >/dev/null 2>&1 || true
		PROXYSQL_STARTED=0
	fi
}

# =============================================================================
# vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
# SCENARIO RUNNER.
#
# Everything from here to EOF -- run_test, the snapshot/delta/pool/error/stat
# print helpers, verify_preset, parse_common_args, enable_proxysql_debug, and
# run_consistency_test -- is the scenario-style runner. The active TAP slice uses
# it for LSN/global_lsn transaction split.
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
# Use RFQ LSN updates as confirmation that query responses record server LSNs.
COMMON_STATS=(
	"PolarDB_Server_LSN_Updates_From_RFQ|+|+|Query RFQ recorded server LSN"
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
	"PolarDB_Session_LSN_Routing|+|+|Session-LSN reader route planned"
	"PolarDB_Wait_Wrap_Prepared|?|+|Behind reader activates a wait wrapper"
	"PolarDB_Wait_Wrap_Bypassed|?|?|Target-ready reader dispatches directly"
	"PolarDB_Wait_LSN_Sent|?|+|LSN wait sent when the selected reader is behind"
	"PolarDB_Wait_Error_LSN_Wait_Timeout|0|+|LSN timeout"
)

# Global LSN mode (mode=2): every automatic read uses the group LSN as part
# of the target, then waits through the normal LSN wrapper path.
GLOBAL_LSN_STATS=(
	"PolarDB_Global_LSN_Routing|+|+|Global LSN routing"
	"PolarDB_Wait_Wrap_Prepared|?|+|Behind reader activates a wait wrapper"
	"PolarDB_Wait_Wrap_Bypassed|?|?|Target-ready reader dispatches directly"
	"PolarDB_Wait_LSN_Sent|?|+|LSN wait sent when the selected reader is behind"
	"PolarDB_Wait_Error_LSN_Wait_Timeout|0|+|LSN timeout"
)

# Primary mode (mode=3): no splits
PRIMARY_STATS=(
	"PolarDB_Split_Reads_Total|0|0|No split reads"
)

# Split LSN wait stats (split path)
SPLIT_LSN_STATS=(
	"PolarDB_Split_LSN_Wait_Count|+|+|LSN wait wrapped (split)"
	"PolarDB_Split_Error_LSN_Wait_Timeout|0|+|LSN timeout (split)"
)

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
# Before RFQ returns split XIDs, safe in-transaction reads can use the normal
# consistency-wait reader path. After the write RFQ marks the transaction
# splittable, the later SELECTs may use transaction split.
SPLIT_PREWRITE_STATS=(
	"PolarDB_Wait_Wrap_Prepared|?|?|Pre-write transaction read may wait or bypass on a fresh reader"
	"PolarDB_Wait_LSN_Sent|?|?|Pre-write transaction wait may send or bypass the wait SET"
	"PolarDB_Split_LSN_Wait_Count|?|?|Post-write split read may wait or bypass on a fresh reader"
	"PolarDB_Wait_Wrap_Bypassed|?|?|Fresh-reader wait bypass"
	"PolarDB_Txn_Reader_Reuse_Bypass_Checked|+|?|Retained reader checked before the second pre-write read"
	"PolarDB_Txn_Reader_Reuse_Bypass_Allowed|+|?|Retained reader RFQ LSN allowed the second pre-write wait bypass"
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
	"PolarDB_Split_LSN_Wait_Count|?|?|Split LSN wait may be bypassed on a fresh reader"
	"PolarDB_Wait_Wrap_Bypassed|?|?|Fresh-reader wait bypass"
	"PolarDB_Split_Error_LSN_Wait_Timeout|0|+|Split LSN timeout"
	"PolarDB_XIDs_Received|+|+|XIDs captured"
	"PolarDB_Txn_Became_Splittable|+|+|Transaction became splittable"
	"PolarDB_Split_Reads_Total|+|+|Split reads attempted (2 per txn)"
	"PolarDB_Split_Reads_Success|+|0|Split successes"
	"PolarDB_Txn_Committed_With_Split|+|?|Commit classified as split"
	"PolarDB_Txn_Lost_Splittable|0|0|Splittable not lost mid-txn"
)

# Completed split readers must return cleanly and remain usable by later work on
# the same frontend session.
SPLIT_REUSE_AFTER_COMMIT_STATS=(
	"PolarDB_XIDs_Received|+|+|XIDs captured in both transactions"
	"PolarDB_Txn_Became_Splittable|+|+|Both transactions became splittable"
	"PolarDB_Split_Reads_Total|+|+|Split reads attempted across transactions"
	"PolarDB_Split_Reads_Success|+|0|Split reads completed"
	"PolarDB_Split_Pool_Hit|+|+|Split reader acquired from the compatible pool"
	"PolarDB_Split_Conn_Reused|+|+|Attached split reader reused within a transaction"
	"PolarDB_Split_Conn_Cleanup_Success|+|+|Completed split reader returned for reuse"
	"PolarDB_Split_Conn_Cleanup_Failed|0|0|Completed split reader cleanup did not fail"
	"PolarDB_Txn_Lost_Splittable|0|0|Splittable state remained valid"
)

# Split timeout stats (SPLIT_VARIANT=timeout). The split wait is always strict
# internally: timeout on the replica retries the original read on the primary
# transaction backend, blocks later split attempts in that transaction, and keeps
# the client transaction alive.
# Counter taxonomy: Split_Reads_Fallback is only for plan-time declines that never
# dispatch to a replica; post-dispatch primary retry is Split_Reads_Retried.
SPLIT_TIMEOUT_STATS=(
	"PolarDB_XIDs_Received|+|+|XIDs captured"
	"PolarDB_Txn_Became_Splittable|+|+|Transaction became splittable"
	"PolarDB_Split_Reads_Total|+|1|Exactly one split read was dispatched before the transaction was blocked"
	"PolarDB_Split_LSN_Wait_Count|+|1|Exactly one split LSN wait was prepared"
	"PolarDB_Split_Error_LSN_Wait_Timeout|0|+|Split LSN timeout was recorded"
	"PolarDB_Split_Reads_Fallback|0|0|Timed-out split read was not counted as plan-time fallback"
	"PolarDB_Split_Reads_Retried|0|+|Timed-out split read was redispatched to primary"
	"PolarDB_Split_Reads_Forwarded|0|0|Timed-out split read was not forwarded to the client"
	"PolarDB_Reader_Terminations|0|0|Timed-out split read did not close the client session"
	"PolarDB_Split_Reads_Success|+|0|No replica split success after timeout"
	"PolarDB_Txn_Committed_No_Split|+|+|Transaction committed after primary retry without split"
	"PolarDB_Txn_Committed_With_Split|0|0|No split commit after timeout"
)

# Lazy split warmup starts with an empty split-only reader pool. The first split
# candidate falls back to the primary and queues demand; the HGM maintenance
# loop creates a connected reader-pool entry using the real startup client
# identity captured from the session that requested warmup. The next transaction
# from the same DB profile and client identity can then split onto that warmed
# replica connection without retrying the query.
SPLIT_LAZY_STATS=(
	"PolarDB_Split_Pool_Empty|+|?|Cold split pool was observed"
	"PolarDB_Split_Warmup_Requested|+|?|Lazy split warmup was requested"
	"PolarDB_Split_Warmup_Created|+|?|Lazy split warmup created a connected pool entry"
	"PolarDB_Split_Pool_Hit|+|?|Warmed split pool was used"
	"PolarDB_Split_Reads_Fallback|+|?|Cold split read fell back to primary"
	"PolarDB_Split_Reads_Total|+|+|Split read attempted after warmup"
	"PolarDB_Split_Reads_Success|+|0|Split success after warmup"
)

# Lazy warmup disabled keeps the split-only pool cold. Eligible split reads still
# take the safe primary fallback, but they must not enqueue or add a warmed
# replica connection.
SPLIT_LAZY_DISABLED_STATS=(
	"PolarDB_Split_Pool_Empty|+|?|Cold split pool was observed"
	"PolarDB_Split_Warmup_Requested|0|0|Lazy split warmup was not requested"
	"PolarDB_Split_Warmup_Created|0|0|Lazy split warmup did not add a pool entry"
	"PolarDB_Split_Pool_Hit|0|0|No warmed split pool was used"
	"PolarDB_Split_Reads_Fallback|+|?|Cold split reads fell back to primary"
	"PolarDB_Split_Reads_Total|0|0|No split read was dispatched"
	"PolarDB_Split_Reads_Success|0|0|No split success without a warmed pool"
)

# Session-level warmup mode=off keeps the global lazy-warmup feature enabled
# but suppresses this session's demand and begin requests.
SPLIT_WARMUP_OFF_STATS=(
	"PolarDB_Split_Pool_Empty|+|?|Cold split pool was observed"
	"PolarDB_Split_Warmup_Requested|0|0|Session warmup mode=off did not request warmup"
	"PolarDB_Split_Warmup_Created|0|0|Session warmup mode=off did not add a pool entry"
	"PolarDB_Split_Pool_Hit|0|0|No warmed split pool was used"
	"PolarDB_Split_Reads_Fallback|+|?|Cold split reads fell back to primary"
	"PolarDB_Split_Reads_Total|0|0|No split read was dispatched"
	"PolarDB_Split_Reads_Success|0|0|No split success without a warmed pool"
)

# Warmup mode behavior is independent from replica catch-up timing. These cases
# show queue/create/reuse/dispatch; success vs retry is covered by split
# execution and failure-policy cases.
SPLIT_WARMUP_MODE_STATS=(
	"PolarDB_Split_Warmup_Requested|+|?|Split warmup was requested"
	"PolarDB_Split_Warmup_Created|+|?|Split warmup created a connected pool entry"
	"PolarDB_Split_Pool_Hit|+|?|Warmed split pool was used"
	"PolarDB_Split_Reads_Total|+|+|Split read was dispatched after warmup"
)

# warmup_begin_prompt issues an extra BEGIN after the first real warmup has
# added a compatible idle reader. That prompt must not create another
# backend. This stays in TAP because the check needs a real libpq connection.
SPLIT_WARMUP_BEGIN_PROMPT_STATS=(
	"${SPLIT_WARMUP_MODE_STATS[@]}"
	"PolarDB_Split_Warmup_Already_Warm|P+|?|Compatible warm reader skipped duplicate warmup"
)

# Cross-frontend strict identity test. The first frontend connection queues and
# creates a warmed reader, then exits. The second frontend connection uses a new
# source port. With strict identity matching, that warmed reader must not be
# reused, so the second transaction falls back to the writer too.
SPLIT_PROXY_IDENTITY_CLIENT_STATS=(
	"PolarDB_Split_Pool_Empty|+|?|Cold split pool was observed"
	"PolarDB_Split_Warmup_Requested|+|?|Client identity queued warmup"
	"PolarDB_Split_Warmup_Created|+|?|Client identity created a warmed reader"
	"PolarDB_Split_Pool_Hit|0|0|Client identity did not reuse a different-frontend reader"
	"PolarDB_Split_Reads_Fallback|+|?|Different-frontend client-identity transaction fell back to primary"
	"PolarDB_Split_Reads_Total|0|0|No split dispatch under client-identity cross-frontend mismatch"
	"PolarDB_Split_Reads_Success|0|0|No split success under client-identity cross-frontend mismatch"
)

# Per-query route=primary veto with lazy warmup enabled. The transaction is
# already split-readable, but the hinted SELECT must stay on the primary and
# must not even attempt split backend acquisition or demand warmup.
SPLIT_ROUTE_PRIMARY_STATS=(
	"PolarDB_XIDs_Received|+|+|XIDs captured"
	"PolarDB_Txn_Became_Splittable|+|+|Transaction became splittable"
	"PolarDB_Split_Reads_Total|0|0|No split read was dispatched"
	"PolarDB_Split_LSN_Wait_Count|0|0|No split LSN wait was prepared"
	"PolarDB_Split_Pool_Empty|0|0|No split backend acquisition was attempted"
	"PolarDB_Split_Warmup_Requested|0|0|Lazy warmup was not requested"
	"PolarDB_Split_Warmup_Created|0|0|Lazy warmup did not add a pool entry"
	"PolarDB_Txn_Committed_No_Split|+|?|Splittable transaction committed without split"
	"PolarDB_Txn_Committed_With_Split|0|0|No split commit"
)

# Locking-read veto stats (SPLIT_VARIANT=for_update). The query shape is vetoed
# before replica dispatch; whether the backend has already promoted the txn from
# WAL-pending to splittable is timing-dependent and not part of this contract.
SPLIT_FOR_UPDATE_STATS=(
	"PolarDB_XIDs_Received|+|+|XIDs captured"
	"PolarDB_Txn_Became_Splittable|?|?|Transaction may become splittable before veto"
	"PolarDB_Split_Rejected_For_Update|+|+|Locking read was rejected for split"
	"PolarDB_Split_Reads_Total|0|0|No split read was dispatched"
	"PolarDB_Split_LSN_Wait_Count|0|0|No split LSN wait was prepared"
	"PolarDB_Txn_Committed_No_Split|?|?|Commit classification depends on splittable timing"
)

# Read-only transaction stats (SPLIT_VARIANT=readonly)
# Autocommit INSERT sets session_write_lsn, then BEGIN..SELECT..SELECT..COMMIT.
# No writes in txn → FSM stays TXN_ON_PRIMARY → no XIDs → no split.
# Safe in-transaction reads without split evidence use reader wait protection.
READONLY_TXN_STATS=(
	"PolarDB_Session_LSN_Routing|+|?|Read-only transaction reads used reader consistency routing"
	"PolarDB_Wait_Wrap_Prepared|?|?|A behind transaction reader may activate a wait"
	"PolarDB_Wait_Wrap_Bypassed|?|?|A target-ready transaction reader may dispatch directly"
	"PolarDB_Wait_LSN_Sent|?|?|Read-only transaction wait may send or bypass the wait SET"
	"PolarDB_Wait_Error_LSN_Wait_Timeout|0|0|No LSN timeout for primary reads"
	"PolarDB_Split_Reads_Total|0|0|No split reads (no XIDs in txn)"
	"PolarDB_Txn_Became_Splittable|0|0|Never became splittable"
	"PolarDB_XIDs_Received|0|0|No XIDs (no writes in txn)"
	"PolarDB_Txn_Committed_With_Split|0|0|No split commit (no splits)"
	"PolarDB_Txn_Committed_No_Split|0|0|No no-split commit (was_splittable never set)"
)

# Read-only transaction stats for cases that stay on the primary. Higher
# isolation levels and transaction-local SET state keep pre-write reads on the
# primary because the temporary reader cannot share that snapshot or local GUC state.
READONLY_TXN_PRIMARY_STATS=(
	"PolarDB_Wait_Wrap_Prepared|0|0|Pre-write reader wait was vetoed"
	"PolarDB_Wait_LSN_Sent|0|0|No transaction reader wait was sent"
	"PolarDB_Split_Reads_Total|0|0|No split reads"
	"PolarDB_Txn_Became_Splittable|0|0|Never became splittable"
	"PolarDB_XIDs_Received|0|0|No XIDs (no writes in txn)"
	"PolarDB_Txn_Committed_With_Split|0|0|No split commit"
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
# Mixed script: autocommit read, write+protected read, split txn, read-only txn.
# The protected read may either send a wait SET or bypass it when the selected
# reader is already fresh; Session_LSN_Routing is the stable route counter.
COMBINED_WORKLOAD_STATS=(
	"PolarDB_Session_LSN_Routing|+|+|Session consistency route was planned"
	"PolarDB_Wait_Wrap_Prepared|?|?|A behind reader may activate a wait wrapper"
	"PolarDB_Wait_Wrap_Bypassed|?|?|A target-ready reader may dispatch directly"
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
declare -A BEFORE AFTER BEFORE_PRESENT AFTER_PRESENT

snapshot() {
	local prefix="$1"
	local raw c name value
	local -A wanted=()
	# All PolarDB counters from PgSQL_Thread.cpp - see AVAILABLE COUNTERS above
	local counters="
        PolarDB_Split_Reads_Total PolarDB_Split_Reads_Success PolarDB_Split_Reads_Fallback
        PolarDB_Split_Reads_Error PolarDB_Split_Rejected_Not_Select PolarDB_Split_Rejected_For_Update
        PolarDB_Split_Rejected_Write_LSN_Unknown PolarDB_Split_Rejected_Observed_LSN_Unknown
        PolarDB_Split_Rejected_No_Marker
        PolarDB_Txn_Became_Splittable PolarDB_Txn_Lost_Splittable PolarDB_Txn_Committed_With_Split
        PolarDB_Txn_Committed_No_Split PolarDB_Queries_In_Splittable_Txn PolarDB_Queries_Split_Eligible
        PolarDB_XIDs_Received PolarDB_Split_WAL_Pending PolarDB_Split_Invariant_Violations
        PolarDB_Server_LSN_Updates_From_RFQ PolarDB_LSN_Updates_From_Monitor
        PolarDB_Monitor_Health_Invalid_Role PolarDB_Monitor_Health_Invalid_Values
        PolarDB_LSN_Stale_Count
        PolarDB_Write_Missing_LSN
        PolarDB_Split_LSN_Wait_Count PolarDB_Wait_Wrap_Prepared PolarDB_Wait_Wrap_Bypassed
        PolarDB_Txn_Reader_Reuse_Bypass_Checked PolarDB_Txn_Reader_Reuse_Bypass_Allowed
        PolarDB_Wait_LSN_Sent PolarDB_Wait_Wrap_Safety_Abort
        PolarDB_Split_LSN_Wait_Sum_Us PolarDB_Split_Error_LSN_Wait_Timeout
        PolarDB_Wait_Error_LSN_Wait_Timeout PolarDB_Wait_Error_Timeout
        PolarDB_Wait_Error_Connection_Lost PolarDB_Wait_Reads_Retried_On_Writer
        PolarDB_Session_LSN_Routing PolarDB_Global_LSN_Routing
        PolarDB_Split_Error_Connection_Lost PolarDB_Split_Error_Query_Failed PolarDB_Split_Error_Timeout
        PolarDB_Split_No_Backend PolarDB_Split_Send_Failed
        PolarDB_Split_Reads_Retried PolarDB_Split_Reads_Forwarded PolarDB_Reader_Terminations
        PolarDB_Split_Pool_Hit PolarDB_Split_Pool_Empty PolarDB_Split_Pool_Contention
        PolarDB_Split_Conn_Reused PolarDB_Split_Conn_Cleanup_Success PolarDB_Split_Conn_Cleanup_Failed
        PolarDB_Reader_Pool_Conns_Free PolarDB_Reader_Pool_Conns_Used
        PolarDB_Split_Warmup_Requested PolarDB_Split_Warmup_Created PolarDB_Split_Warmup_Failed
        PolarDB_Split_Warmup_Already_Warm PolarDB_Split_Warmup_Dedup_Queued
        PolarDB_Split_Warmup_Dedup_Inflight PolarDB_Split_Warmup_Queue_Full
        PolarDB_Split_Warmup_No_Target PolarDB_Split_Warmup_Bad_Request
        PolarDB_Split_Warmup_Connect_Failed PolarDB_Split_Warmup_Add_Failed
        PolarDB_Warmup_Pending
        PolarDB_Split_Latency_Sum_Us PolarDB_Split_Latency_Count
    "

	for c in $counters; do
		wanted["$c"]=1
		if [ "$prefix" = "B" ]; then
			BEFORE[$c]=0
			BEFORE_PRESENT[$c]=0
		else
			AFTER[$c]=0
			AFTER_PRESENT[$c]=0
		fi
	done

	# Read one coherent status snapshot. Querying once per counter repeatedly
	# rebuilt the admin table and could mix values from different refreshes.
	raw=$(proxysql_admin \
		"SELECT Variable_Name, Variable_Value FROM stats_pgsql_global WHERE Variable_Name LIKE 'PolarDB_%';" \
		2>/dev/null) || raw=""
	while IFS='|' read -r name value; do
		[ -n "$name" ] || continue
		[ -n "${wanted[$name]+present}" ] || continue
		if [ "$prefix" = "B" ]; then
			BEFORE[$name]="${value:-0}"
			BEFORE_PRESENT[$name]=1
		else
			AFTER[$name]="${value:-0}"
			AFTER_PRESENT[$name]=1
		fi
	done <<<"$raw"
}

delta() { echo $((${AFTER[$1]:-0} - ${BEFORE[$1]:-0})); }
counter_present_in_snapshots() {
	local name="$1"
	[ "${BEFORE_PRESENT[$name]:-0}" -eq 1 ] && [ "${AFTER_PRESENT[$name]:-0}" -eq 1 ]
}

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
	done <<<"$data"
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

# Get successful backend connection delta for a specific hostgroup.
get_hg_connok_delta() {
	local target_hg="$1"
	local total=0

	local keys=$(printf '%s\n' "${!POOL_BEFORE[@]}" | grep "_connok$" | sort -u)
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
# Returns: 0=pass, 1=fail, 2=skip, 3=required counter absent
#------------------------------------------------------------------------------
check_stat() {
	local name="$1"
	local expect="$2"
	local actual="$3"

	[ "$expect" = "?" ] && return 2
	if ! counter_present_in_snapshots "$name"; then
		[[ "$expect" == P* ]] && return 2
		return 3
	fi
	case "$expect" in
	0)
		[ "$actual" -eq 0 ]
		return $?
		;;
	+)
		[ "$actual" -ge 1 ]
		return $?
		;;
	P+)
		[ "$actual" -ge 1 ]
		return $?
		;;
	-)
		[ "$actual" -le -1 ]
		return $?
		;;
	*)
		[ "$actual" -eq "$expect" ]
		return $?
		;;
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
		IFS='|' read -r name success_exp failure_exp desc <<<"$stat"
		local expect=$([[ "$outcome" = "success" ]] && echo "$success_exp" || echo "$failure_exp")
		local actual=$(delta "$name")

		# Format expected for display
		local expect_desc
		case "$expect" in
		0) expect_desc="=0" ;;
		+) expect_desc=">=1" ;;
		P+) expect_desc=">=1 if POLARDB_PROFILE counter exists" ;;
		-) expect_desc="<=-1" ;;
		?) expect_desc="skip" ;;
		*) expect_desc="=$expect" ;;
		esac

		check_stat "$name" "$expect" "$actual"
		local rc=$?

		case $rc in
		0)
			echo "[$(ts)]   PASS: $desc | delta=$actual (want $expect_desc)"
			PASS=$((PASS + 1))
			;;
		1)
			echo "[$(ts)]   FAIL: $desc | delta=$actual (want $expect_desc)"
			FAIL=$((FAIL + 1))
			;;
		3)
			echo "[$(ts)]   FAIL: $desc | required counter $name is absent"
			FAIL=$((FAIL + 1))
			;;
		2) ;; # skip
		esac
	done
}

verify_split_wait_or_bypass() {
	local desc="$1"
	local waits bypasses total

	waits=$(delta "PolarDB_Split_LSN_Wait_Count")
	bypasses=$(delta "PolarDB_Wait_Wrap_Bypassed")
	total=$((waits + bypasses))
	if [ "$total" -ge 1 ]; then
		echo "[$(ts)]   PASS: $desc | waits=$waits bypasses=$bypasses (want total >=1)"
		PASS=$((PASS + 1))
	else
		echo "[$(ts)]   FAIL: $desc | waits=$waits bypasses=$bypasses (want total >=1)"
		FAIL=$((FAIL + 1))
	fi
}

#------------------------------------------------------------------------------
# Main test runner
#------------------------------------------------------------------------------
parse_common_args() {
	LSN_WAIT_TIMEOUT_ACTION="warning"
	EXPECT_OUTCOME="success"
	for arg in "$@"; do
		case $arg in
		-a) LSN_WAIT_TIMEOUT_ACTION="warning" ;;
		-b) LSN_WAIT_TIMEOUT_ACTION="primary" ;;
		-success | -s) EXPECT_OUTCOME="success" ;;
		-failure | -f) EXPECT_OUTCOME="failure" ;;
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
	local timeout_action="${LSN_WAIT_TIMEOUT_ACTION:-warning}"
	local outcome="${EXPECT_OUTCOME:-success}"
	local timeout_ms=$([[ "$outcome" = "failure" ]] && echo 100 || echo 1000)
	local test_id="${TEST_ID:-$CASE_NUM}"
	local global_wait_timeout_ms="${POLARDB_GLOBAL_WAIT_TIMEOUT_MS:-}"

	# Build case identifier for log file
	local case_id="case${CASE_NUM}"
	[ "$timeout_action" != "warning" ] &&
		case_id="${case_id}_${timeout_action}"
	[ "$outcome" = "failure" ] && case_id="${case_id}_failure"

	# Initialize logging
	init_logging "$case_id"

	PASS=0 FAIL=0

	echo ""
	echo "================================================================"
	echo "[$(ts)] CASE $CASE_NUM: $CASE_NAME"
	echo "================================================================"
	echo "[$(ts)] mode=$CONSISTENCY_MODE split=$SPLIT_ENABLED xact=$XACT_SPLIT"
	echo "[$(ts)] lsn_wait_timeout_action=$timeout_action outcome=$outcome timeout=${timeout_ms}ms"
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
	# - consistency modes with waits (LSN/GLOBAL_LSN): success -> 1000ms, failure -> 10ms
	# - modes without waits (OFF/EVENTUAL): leave unset
	if [ -z "$global_wait_timeout_ms" ]; then
		if polardb_consistency_uses_lsn_wait "$CONSISTENCY_MODE"; then
			if [ "$outcome" = "failure" ]; then
				global_wait_timeout_ms=10
			else
				global_wait_timeout_ms=1000
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
	# Per-HG policy. txn_split_enabled requests/observes RFQ XID data and allows
	# split-readable transaction reads to take a replica connection from the
	# pool. Mirror the global consistency mode in the hostgroup policy.
	configure_polardb_scenario_policy \
		"$CONSISTENCY_MODE" "$timeout_action" "$XACT_SPLIT" "$timeout_ms" \
		"${SPLIT_VARIANT:-basic}"

	show_polardb_runtime_policy | sed 's/^/    /'

	# STEP 2: Baseline
	echo ""
	echo "[$(ts)] STEP 2: Baseline counters"
	snapshot "B"
	snapshot_pool "B"

	# STEP 3: LSN state.
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
		if polardb_consistency_uses_lsn_wait "$CONSISTENCY_MODE"; then
			if ! wait_for_replica_lsn_catchup 3; then
				echo "[$(ts)]   FAIL: replica LSN not caught up after 3s"
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
		FAIL=$((FAIL + 1))
		t0=0
		t1=0
		count=""
		has_warn=0
		has_err=0
		result=""
	elif [ "$XACT_SPLIT" = "1" ]; then
		local lazy_split=0
		local split_warmup_mode="${SPLIT_WARMUP_MODE:-}"
		case "${SPLIT_VARIANT:-basic}" in
		lazy | lazy_disabled) lazy_split=1 ;;
		warmup_off)
			lazy_split=1
			split_warmup_mode="off"
			;;
		warmup_demand)
			lazy_split=1
			split_warmup_mode="demand"
			;;
		warmup_begin | warmup_begin_prompt)
			lazy_split=1
			split_warmup_mode="begin"
			;;
		warmup_both)
			lazy_split=1
			split_warmup_mode="both"
			;;
		proxy_identity_client | proxy_identity_proxy)
			lazy_split=1
			split_warmup_mode="demand"
			;;
		esac
		if [ "$lazy_split" -eq 1 ]; then
			echo "[$(ts)]   Lazy split warmup: starting with a cold split pool"
		fi
		local split_session_preamble=""
		if [ -n "$split_warmup_mode" ]; then
			split_session_preamble="SET proxysql.polardb_txn_split_warmup TO '$split_warmup_mode';"
			echo "[$(ts)]   Session split warmup mode: $split_warmup_mode"
		fi

		if [ "$early_fail" -eq 0 ]; then
			# Split reader reuse requires the DB profile and PolarDB startup
			# client identity to match. The generated pgbench scripts therefore
			# warm direct split cases through the same frontend session that will
			# later run the transaction; an external psql/pgbench prewarm would
			# use a different client source port and must not be reused.
			snapshot "B"
			snapshot_pool "B"

			# Split transaction via pgbench
			# pgbench sends each statement individually over persistent connection
			# This allows ProxySQL to capture XIDs after INSERT and route SELECT to replica
			echo "[$(ts)]   Split transaction via pgbench (statements sent individually)"

			local sleep_ms=200
			[ "$outcome" = "failure" ] && sleep_ms=500
			local split_run_kind="single"
			local split_seed_sql="$RUN_DIR/split_identity_seed.sql"
			local split_probe_sql="$RUN_DIR/split_identity_probe.sql"

			case "${SPLIT_VARIANT:-basic}" in
			prewrite)
				# Pre-write read + split: BEGIN stays on primary. The first SELECT
				# can use the reader wait path because it has no transaction writes
				# yet. INSERT returns RFQ XIDs; later SELECTs can use split.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'prewrite_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'prewrite_setup$CASE_NUM';
\sleep ${sleep_ms}ms
BEGIN;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
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
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			reuse_after_commit)
				# After each write, the first read can complete the writer's
				# WAL-pending stage, the second attaches a split reader, and the
				# third reuses it. COMMIT then returns the completed split reader
				# for the autocommit read and the next transaction.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_reuse_first$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_reuse_first$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_reuse_second$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_reuse_second$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			timeout)
				# Timeout path: the first split read dispatches to a replica and
				# times out waiting for the transaction LSN. Split wait is strict
				# on the wire when the configured timeout action is writer.
				# The original SELECT then retries on the primary transaction
				# backend. The next SELECT shows the transaction stays alive, but
				# split is blocked for the rest of that transaction.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_timeout_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_timeout_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			warmup_begin_prompt)
				# First transaction creates a real warmed reader. The short
				# BEGIN/COMMIT then asks for begin-time warmup again while that
				# reader is idle, so the drain can show it skips duplicate
				# backend creation. The primary can report wal_pending for either
				# of the first two final reads, so a third read verifies that the
				# warmed reader is used after the split-readable boundary.
				local warmup_wait_sec="${POLARDB_SPLIT_WARMUP_WAIT_SEC:-3}"
				cat >"$split_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_prompt_cold$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_prompt_cold$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
\sleep ${warmup_wait_sec}s
BEGIN;
COMMIT;
\sleep ${warmup_wait_sec}s
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_prompt_warm$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_prompt_warm$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			lazy | lazy_disabled | warmup_off | warmup_demand | warmup_begin | warmup_both)
				# First transaction starts with a cold split pool and falls back
				# while queuing warmup if pgsql-polardb_lazy_warmup_split=1.
				# Keep the same frontend connection alive so the enabled case can
				# reuse the warmed backend with the same PolarDB startup client
				# identity. The disabled/off variants must keep falling back to primary.
				local warmup_wait_sec="${POLARDB_SPLIT_WARMUP_WAIT_SEC:-3}"
				cat >"$split_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_lazy_cold$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_lazy_cold$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
\sleep ${warmup_wait_sec}s
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_lazy_warm$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_lazy_warm$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			proxy_identity_client | proxy_identity_proxy)
				# Cross-frontend identity policy: client mode uses the actual
				# frontend endpoint and must not reuse the seeded reader from a
				# different frontend. Proxy mode uses ProxySQL's listener identity
				# and may reuse it.
				split_run_kind="cross_frontend"
				cat >"$split_seed_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_identity_seed$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_identity_seed$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				cat >"$split_probe_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_identity_probe$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_identity_probe$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			route_primary)
				# The write RFQ makes the transaction split-readable. The hinted
				# SELECT must still stay on the primary, even with lazy warmup
				# enabled, and must not queue a split warmup request.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_route_primary$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_route_primary$CASE_NUM';
\sleep ${sleep_ms}ms
/* route=primary */ SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			for_update)
				# Locking SELECT veto: INSERT produces split RFQ evidence, then
				# SELECT ... FOR UPDATE must stay on the primary.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_locking_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_locking_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT * FROM $TEST_TABLE WHERE id = $test_id FOR UPDATE;
COMMIT;
EOSQL
				;;
			readonly)
				# Read-only txn: autocommit INSERT sets session_write_lsn,
				# then BEGIN → SELECT → SELECT → COMMIT. SELECTs can use the
				# reader wait path; no in-txn writes means no split XIDs.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
INSERT INTO $TEST_TABLE VALUES ($test_id, 'readonly_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'readonly_setup$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT 1;
BEGIN;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			readonly_repeatable)
				# REPEATABLE READ fixes the transaction snapshot at the first
				# statement. A pre-write reader wait would create that snapshot on
				# the replica and later continue the transaction on the primary, so
				# the read must stay on the primary.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
INSERT INTO $TEST_TABLE VALUES ($test_id, 'readonly_rr_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'readonly_rr_setup$CASE_NUM';
\sleep ${sleep_ms}ms
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			readonly_set_local)
				# SET LOCAL changes transaction-local backend state on the primary
				# transaction connection. Until that state is replayed to temporary
				# readers, pre-write reader waits stay on the primary.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
INSERT INTO $TEST_TABLE VALUES ($test_id, 'readonly_local_setup$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'readonly_local_setup$CASE_NUM';
\sleep ${sleep_ms}ms
BEGIN;
SET LOCAL search_path TO public;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			extended)
				# Extended protocol veto: same script as basic, but run with -M extended.
				# Extended protocol forces primary (RC4 veto) for wrapped queries.
				cat >"$split_sql" <<EOSQL
$split_session_preamble
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
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
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
				cat >"$split_sql" <<EOSQL
$split_session_preamble
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($test_id, 'split_test_case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data = 'split_test_case$CASE_NUM';
\sleep ${sleep_ms}ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
COMMIT;
EOSQL
				;;
			esac
			if [ "$split_run_kind" = "cross_frontend" ]; then
				echo "[$(ts)]   Seed script:"
				cat "$split_seed_sql" | sed 's/^/    /'
				echo "[$(ts)]   Probe script:"
				cat "$split_probe_sql" | sed 's/^/    /'
			else
				echo "[$(ts)]   Script:"
				cat "$split_sql" | sed 's/^/    /'
			fi

			# Failure cases keep sustained WAL to hold the replica behind the
			# target. Existing success TAP cases assert that split-dispatch
			# counters fire, so they keep a short opt-out WAL pulse by default.
			# No-background-WAL behavior is covered by the direct 'w' marker
			# probe and by bench5 with BENCH5_WAL_GENERATOR=0.
			if [ "$outcome" = "failure" ]; then
				start_wal_generator 0.01 1000
			elif [ "${POLARDB_SPLIT_SUCCESS_WAL_GENERATOR:-1}" = "1" ]; then
				if [ "${SPLIT_VARIANT:-basic}" = "lazy" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "lazy_disabled" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "warmup_off" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "warmup_demand" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "warmup_begin" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "warmup_begin_prompt" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "warmup_both" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "proxy_identity_client" ] ||
					[ "${SPLIT_VARIANT:-basic}" = "proxy_identity_proxy" ]; then
					# Warmup-focused TAP scripts include a sleep between the cold
					# and warm transactions. Keep the bounded pulse alive across
					# that window so this test exercises warmup reuse, not only
					# the backend WAL-pending back-pressure path.
					start_wal_generator 0.01 500
				else
					start_wal_generator 0 200
				fi
				sleep 0.3
			fi

			t0=$(date +%s%3N)
			# -n: no vacuum, -t 1: one transaction, -c 1: one client
			# Use our built libpq which has PQpipelineStatus (required by pg16 pgbench)
			local pgbench_mode="simple"
			[ "${SPLIT_VARIANT:-basic}" = "extended" ] && pgbench_mode="extended"
			echo "[$(ts)]   Protocol mode: -M $pgbench_mode"
			if [ "$split_run_kind" = "cross_frontend" ]; then
				local warmup_wait_sec="${POLARDB_SPLIT_WARMUP_WAIT_SEC:-3}"
				local seed_out probe_out seed_exit probe_exit
				seed_out=$(polardb_pgbench_script "$pgbench_mode" "$split_seed_sql" 2>&1)
				seed_exit=$?
				echo "[$(ts)]   Waiting ${warmup_wait_sec}s for cross-frontend warmup"
				sleep "$warmup_wait_sec"
				probe_out=$(polardb_pgbench_script "$pgbench_mode" "$split_probe_sql" 2>&1)
				probe_exit=$?
				pgbench_out=$(printf '%s\n%s\n%s\n%s\n' '--- seed pgbench ---' "$seed_out" '--- probe pgbench ---' "$probe_out")
				if [ "$seed_exit" -eq 0 ] && [ "$probe_exit" -eq 0 ]; then
					pgbench_exit=0
				else
					pgbench_exit=1
				fi
			else
				pgbench_out=$(polardb_pgbench_script "$pgbench_mode" "$split_sql" 2>&1)
				pgbench_exit=$?
			fi
			t1=$(date +%s%3N)

			echo "[$(ts)]   pgbench exit code: $pgbench_exit"
			echo "[$(ts)]   pgbench output:"
			echo "$pgbench_out" | sed 's/^/    /'

			stop_wal_generator

			# Keep the generated SQL in RUN_DIR for debugging.

			# Verify the data is there
			result=$(polardb_primary_sql "SELECT COUNT(*) FROM $TEST_TABLE WHERE id=$test_id;" 2>&1)
			count=$(echo "$result" | grep -E '^[0-9]+$' | head -1)
		fi # inner early_fail check for split block
	else
		# Non-split: simple INSERT then SELECT
		echo "[$(ts)]   INSERT then SELECT (non-split)"

		# LSN and global-LSN modes route SELECTs to readers and warm the pool.
		if polardb_consistency_uses_lsn_wait "$CONSISTENCY_MODE"; then
			if ! setup_reader_routing "$POLARDB_READER_HG" 3; then
				echo "[$(ts)]   FAIL: reader routing warmup failed"
				early_fail=1
				FAIL=$((FAIL + 1))
			fi
		fi

		if [ "$early_fail" -eq 0 ]; then
			if polardb_consistency_uses_lsn_wait "$CONSISTENCY_MODE"; then
				# For session/global LSN, keep the same session so ProxySQL can apply wait logic.
				if [ "$outcome" = "failure" ]; then
					cat >"$session_sql" <<EOSQL
INSERT INTO $TEST_TABLE VALUES ($test_id, 'case$CASE_NUM') ON CONFLICT (id) DO UPDATE SET data='case$CASE_NUM';
\\sleep 500ms
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $test_id;
EOSQL
				else
					cat >"$session_sql" <<EOSQL
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
		fi # inner early_fail check for non-split block
	fi

	if [ "$early_fail" -eq 0 ]; then
		local qtime=$((t1 - t0))
		if [ "$XACT_SPLIT" = "1" ] ||
				polardb_consistency_uses_lsn_wait "$CONSISTENCY_MODE"; then
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
			local reader_query_delta
			local reader_connok_delta
			reader_query_delta=$(get_hg_queries_delta "$POLARDB_READER_HG")
			reader_connok_delta=$(get_hg_connok_delta "$POLARDB_READER_HG")
			case "${SPLIT_VARIANT:-basic}" in
			extended | for_update | route_primary | readonly_repeatable | readonly_set_local | proxy_identity_client)
				if [ "$reader_query_delta" -eq 0 ]; then
					echo "[$(ts)]   PASS: no replica split dispatch for ${SPLIT_VARIANT:-basic} variant"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: reader HG $POLARDB_READER_HG query delta=$reader_query_delta, expected 0"
					FAIL=$((FAIL + 1))
				fi
				;;
			lazy_disabled | warmup_off)
				if [ "$reader_query_delta" -eq 0 ]; then
					echo "[$(ts)]   PASS: split warmup disabled/off kept split reads on primary"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: reader HG $POLARDB_READER_HG query delta=$reader_query_delta with split warmup disabled/off"
					FAIL=$((FAIL + 1))
				fi
				;;
			*)
				if [ "$reader_query_delta" -ge 1 ] || [ "$reader_connok_delta" -ge 1 ]; then
					echo "[$(ts)]   PASS: transaction split used reader HG $POLARDB_READER_HG (queries +$reader_query_delta conn_ok +$reader_connok_delta)"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: transaction split did not use reader HG $POLARDB_READER_HG (queries +$reader_query_delta conn_ok +$reader_connok_delta)"
					FAIL=$((FAIL + 1))
				fi
				;;
			esac
			case "${SPLIT_VARIANT:-basic}" in
			prewrite)
				verify_preset "$outcome" "${SPLIT_PREWRITE_STATS[@]}"
				verify_split_wait_or_bypass "Post-write split read waited or used a fresh reader"
				;;
			multi)
				verify_preset "$outcome" "${SPLIT_MULTI_STATS[@]}"
				verify_split_wait_or_bypass "Split read waited or used a fresh reader"
				;;
			reuse_after_commit)
				verify_preset "$outcome" "${SPLIT_REUSE_AFTER_COMMIT_STATS[@]}"
				verify_split_wait_or_bypass "Cross-transaction split reads waited or used a fresh reader"
				;;
			timeout) verify_preset "$outcome" "${SPLIT_TIMEOUT_STATS[@]}" ;;
			for_update) verify_preset "$outcome" "${SPLIT_FOR_UPDATE_STATS[@]}" ;;
			readonly) verify_preset "$outcome" "${READONLY_TXN_STATS[@]}" ;;
			readonly_repeatable | readonly_set_local) verify_preset "$outcome" "${READONLY_TXN_PRIMARY_STATS[@]}" ;;
			extended) verify_preset "$outcome" "${EXTENDED_VETO_STATS[@]}" ;;
			combined) verify_preset "$outcome" "${COMBINED_WORKLOAD_STATS[@]}" ;;
			route_primary) verify_preset "$outcome" "${SPLIT_ROUTE_PRIMARY_STATS[@]}" ;;
			lazy_disabled) verify_preset "$outcome" "${SPLIT_LAZY_DISABLED_STATS[@]}" ;;
			lazy) verify_preset "$outcome" "${SPLIT_LAZY_STATS[@]}" ;;
			warmup_off) verify_preset "$outcome" "${SPLIT_WARMUP_OFF_STATS[@]}" ;;
			proxy_identity_client) verify_preset "$outcome" "${SPLIT_PROXY_IDENTITY_CLIENT_STATS[@]}" ;;
			warmup_begin_prompt) verify_preset "$outcome" "${SPLIT_WARMUP_BEGIN_PROMPT_STATS[@]}" ;;
			warmup_demand | warmup_begin | warmup_both | proxy_identity_proxy) verify_preset "$outcome" "${SPLIT_WARMUP_MODE_STATS[@]}" ;;
			*) verify_preset "$outcome" "${SPLIT_STATS[@]}" ;;
			esac
		else
			# Non-split path: use wait/timeout counters
			if [ "${READ_TARGET:-replica}" = "primary" ]; then
				verify_preset "$outcome" "${PRIMARY_STATS[@]}"
			else
				case "$CONSISTENCY_MODE" in
				off | eventual) verify_preset "$outcome" "${EVENTUAL_STATS[@]}" ;;
				session_lsn) verify_preset "$outcome" "${LSN_STATS[@]}" ;;
				global_lsn) verify_preset "$outcome" "${GLOBAL_LSN_STATS[@]}" ;;
				esac
			fi
		fi

		# Result checks
		if [ "$outcome" = "success" ]; then
			if [ "$count" = "1" ]; then
				echo "[$(ts)]   PASS: COUNT=1"
				PASS=$((PASS + 1))
			else
				echo "[$(ts)]   FAIL: COUNT=$count"
				FAIL=$((FAIL + 1))
			fi
			if [ "$has_warn" = "0" ] && [ "$has_err" = "0" ]; then
				echo "[$(ts)]   PASS: No WARN/ERR"
				PASS=$((PASS + 1))
			else
				echo "[$(ts)]   FAIL: WARN=$has_warn ERR=$has_err"
				FAIL=$((FAIL + 1))
			fi
		else
			if [ "$qtime" -ge $((timeout_ms - 20)) ]; then
				echo "[$(ts)]   PASS: Timeout ${qtime}ms"
				PASS=$((PASS + 1))
			else
				echo "[$(ts)]   WARN: Fast ${qtime}ms"
			fi
			if [ "$XACT_SPLIT" = "1" ] && [ "${SPLIT_VARIANT:-basic}" = "timeout" ]; then
				if [ "$count" = "1" ] && [ "$has_err" = "0" ] && [ "$has_warn" = "0" ]; then
					echo "[$(ts)]   PASS: Split timeout retried on primary and transaction stayed usable"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: Split timeout primary retry mismatch COUNT=$count WARN=$has_warn ERR=$has_err"
					FAIL=$((FAIL + 1))
				fi
			elif [ "$timeout_action" = "warning" ]; then
				if [ "$has_warn" -ge 1 ]; then
					echo "[$(ts)]   PASS: Got WARNING"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: No WARNING"
					FAIL=$((FAIL + 1))
				fi
			else
				local retry_delta
				retry_delta=$(delta "PolarDB_Wait_Reads_Retried_On_Writer")
				if [ "$has_err" = "0" ] && [ "$has_warn" = "0" ] && [ "$retry_delta" -ge 1 ]; then
					echo "[$(ts)]   PASS: Strict timeout retried on writer"
					PASS=$((PASS + 1))
				else
					echo "[$(ts)]   FAIL: Strict timeout retry mismatch WARN=$has_warn ERR=$has_err retry_delta=$retry_delta"
					FAIL=$((FAIL + 1))
				fi
			fi
		fi
	fi # early_fail check

	# STEP 8: Cleanup and Log Extraction (ALWAYS runs — even on early failure)
	echo ""
	echo "[$(ts)] STEP 8: Cleanup"
	stop_wal_generator 2>/dev/null || true
	disable_replay_lag 2>/dev/null || true
	proxysql_admin "UPDATE pgsql_replication_hostgroups SET lsn_wait_timeout_ms=1000;" 2>/dev/null || true
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" 2>/dev/null || true

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
