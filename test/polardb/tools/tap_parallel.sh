#!/usr/bin/env bash
# Conservative PolarDB TAP scheduler.
#
# Runs known parallel-safe TAP groups with distinct ProxySQL shards and backend
# object suffixes, then runs backend-exclusive groups sequentially.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARDB_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TAP_DIR="$POLARDB_TEST_DIR/test-tap"
export POLARDB_PARALLEL_RUN=1
# shellcheck source=../common/env.sh
source "$POLARDB_TEST_DIR/common/env.sh"

MODE="${1:-all}"
case "$MODE" in
all | safe | exclusive | list) ;;
*)
	echo "usage: $0 [all|safe|exclusive|list]" >&2
	exit 2
	;;
esac

DEFAULT_SAFE_JOBS="config rfq-lifecycle lsn-session global-core global-split split-core split-warmup"
DEFAULT_EXCLUSIVE_JOBS="global-timeout split-timeout split-failure-policy wait-timeout"
SAFE_JOBS="${POLARDB_TAP_SAFE_JOBS:-$DEFAULT_SAFE_JOBS}"
EXCLUSIVE_JOBS="${POLARDB_TAP_EXCLUSIVE_JOBS:-$DEFAULT_EXCLUSIVE_JOBS}"
PARALLEL_JOBS="${POLARDB_TAP_PARALLEL_JOBS:-${JOBS:-4}}"
SHARD_BASE="${POLARDB_TAP_SHARD_BASE:-1}"
EXCLUSIVE_SHARD_BASE="${POLARDB_TAP_EXCLUSIVE_SHARD_BASE:-$((SHARD_BASE + 100))}"
LOG_DIR="$POLARDB_OUTPUT_DIR/tap_parallel"
LOCK_DIR="$POLARDB_RUNTIME_DIR"
BACKEND_EXCLUSIVE_LOCK="${POLARDB_BACKEND_EXCLUSIVE_LOCK:-$LOCK_DIR/polardb_tap_backend_${UID}.lock}"
RUN_LOCK="${POLARDB_TAP_RUN_LOCK:-$LOCK_DIR/polardb_tap_${UID}_${POLARDB_PARALLEL_PORT_BASE}.lock}"

case "$PARALLEL_JOBS" in
"" | *[!0-9]* | 0)
	echo "POLARDB_TAP_PARALLEL_JOBS must be a positive integer: $PARALLEL_JOBS" >&2
	exit 2
	;;
esac
case "$SHARD_BASE" in
"" | *[!0-9]*)
	echo "POLARDB_TAP_SHARD_BASE must be a non-negative integer: $SHARD_BASE" >&2
	exit 2
	;;
esac
case "$EXCLUSIVE_SHARD_BASE" in
"" | *[!0-9]*)
	echo "POLARDB_TAP_EXCLUSIVE_SHARD_BASE must be a non-negative integer: $EXCLUSIVE_SHARD_BASE" >&2
	exit 2
	;;
esac
case "$POLARDB_PARALLEL_PORT_BASE" in
"" | *[!0-9]*)
	echo "POLARDB_PARALLEL_PORT_BASE must be a non-negative integer: $POLARDB_PARALLEL_PORT_BASE" >&2
	exit 2
	;;
esac
case "$POLARDB_PROXY_SHARD_STRIDE" in
"" | *[!0-9]*)
	echo "POLARDB_PROXY_SHARD_STRIDE must be a positive integer: $POLARDB_PROXY_SHARD_STRIDE" >&2
	exit 2
	;;
esac
if [ "$POLARDB_PROXY_SHARD_STRIDE" -lt 3 ]; then
	echo "POLARDB_PROXY_SHARD_STRIDE must be at least 3: $POLARDB_PROXY_SHARD_STRIDE" >&2
	exit 2
fi

parallel_port() {
	local shard="$1"
	local offset="$2"
	printf '%s\n' $((POLARDB_PARALLEL_PORT_BASE + shard * POLARDB_PROXY_SHARD_STRIDE + offset))
}

port_is_listening() {
	local port="$1"
	command -v ss >/dev/null 2>&1 || return 1
	ss -H -ltn "sport = :$port" 2>/dev/null | grep -q .
}

check_job_ports() {
	local job="$1"
	local shard="$2"
	local mysql_admin_port pg_admin_port proxy_port port owner

	mysql_admin_port=$(parallel_port "$shard" 0)
	pg_admin_port=$(parallel_port "$shard" 1)
	proxy_port=$(parallel_port "$shard" 2)
	printf '[tap] ports job=%s shard=%s mysql_admin=%s pg_admin=%s pg=%s\n' \
		"$job" "$shard" "$mysql_admin_port" "$pg_admin_port" "$proxy_port"

	for port in "$mysql_admin_port" "$pg_admin_port" "$proxy_port"; do
		if [ "$port" -gt 65535 ]; then
			echo "[tap] port outside TCP range: job=$job shard=$shard port=$port" >&2
			return 1
		fi
		owner="${seen_ports[$port]:-}"
		if [ -n "$owner" ]; then
			echo "[tap] duplicate port: $port is assigned to $owner and $job" >&2
			return 1
		fi
		seen_ports[$port]="$job"
		if port_is_listening "$port"; then
			echo "[tap] port already has a listener: job=$job shard=$shard port=$port" >&2
			ss -H -ltnp "sport = :$port" 2>/dev/null >&2 || true
			return 1
		fi
	done
}

check_selected_ports() {
	local shard job
	declare -gA seen_ports=()

	if [ "$MODE" = "all" ] || [ "$MODE" = "safe" ]; then
		shard="$SHARD_BASE"
		for job in $SAFE_JOBS; do
			check_job_ports "$job" "$shard" || return 1
			shard=$((shard + 1))
		done
	fi
	if [ "$MODE" = "all" ] || [ "$MODE" = "exclusive" ]; then
		shard="$EXCLUSIVE_SHARD_BASE"
		for job in $EXCLUSIVE_JOBS; do
			check_job_ports "$job" "$shard" || return 1
			shard=$((shard + 1))
		done
	fi
}

safe_job_log_name() {
	printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

list_manifest() {
	cat <<'EOF'
case_id | group | tag | plan_count | function
config | config | parallel | 16 | config_roundtrip_tap.sh
rfq-lifecycle | rfq-lifecycle | parallel | 8 | rfq_lsn_lifecycle_tap.sh
lsn-session | lsn-session | parallel | 70 | lsn_session_consistency_tap.sh
global:1-5 | global-core | parallel | 11 total | GLOBAL_LSN selected cases
global:7-9 | global-split | parallel | 10 total | GLOBAL_LSN selected cases
global:6 | global-timeout | backend_exclusive | 7 total | GLOBAL_LSN timeout case
split:6,7,8,10,12,13,14,21,22 | split-core | parallel | 12 total | txn_split core selected cases
split:9,11,17,18,19,20,23,24,25 | split-warmup | parallel | 10 total | txn_split warmup selected cases
split:15,16 | split-timeout | backend_exclusive | 3 total | txn_split timeout selected cases
split-failure-policy | split-failure-policy | backend_exclusive | 19 | txn_split_failure_policy_tap.sh
wait-timeout | wait-timeout | backend_exclusive | 69 | wait_timeout_cleanup_tap.sh
EOF
}

job_command() {
	local job="$1"

	case "$job" in
	config)
		exec "$TAP_DIR/config_roundtrip_tap.sh"
		;;
	rfq-lifecycle)
		exec "$TAP_DIR/rfq_lsn_lifecycle_tap.sh"
		;;
	lsn-session)
		export POLARDB_TIMEOUT_EDGE_TESTS=0
		exec "$TAP_DIR/lsn_session_consistency_tap.sh"
		;;
	global-core)
		export GLOBAL_LSN_TAP_GROUPS="global-core"
		exec "$TAP_DIR/global_lsn_consistency_tap.sh"
		;;
	global-split)
		export GLOBAL_LSN_TAP_GROUPS="global-split"
		exec "$TAP_DIR/global_lsn_consistency_tap.sh"
		;;
	global-timeout)
		export GLOBAL_LSN_TAP_GROUPS="global-timeout"
		exec "$TAP_DIR/global_lsn_consistency_tap.sh"
		;;
	split-core)
		export TXN_SPLIT_TAP_GROUPS="split-core-positive"
		exec "$TAP_DIR/txn_split_tap.sh"
		;;
	split-warmup)
		export TXN_SPLIT_TAP_GROUPS="split-warmup"
		exec "$TAP_DIR/txn_split_tap.sh"
		;;
	split-timeout)
		export TXN_SPLIT_TAP_GROUPS="split-timeout"
		exec "$TAP_DIR/txn_split_tap.sh"
		;;
	split-failure-policy)
		exec "$TAP_DIR/txn_split_failure_policy_tap.sh"
		;;
	wait-timeout)
		exec "$TAP_DIR/wait_timeout_cleanup_tap.sh"
		;;
	*)
		echo "unknown TAP job: $job" >&2
		exit 2
		;;
	esac
}

run_job_body() {
	local job="$1"
	local shard="$2"

	export POLARDB_PARALLEL_RUN=1
	export POLARDB_TEST_SHARD="$shard"
	export POLARDB_TAP_GROUP="$job"
	export POLARDB_TEST_OBJECT_SUFFIX="s${shard}_${job//-/_}"
	unset PROXYSQL_PORT PROXYSQL_ADMIN_PORT PROXYSQL_MYSQL_ADMIN_PORT PROXYSQL_DATA_DIR

	printf '# job=%s shard=%s object_suffix=%s\n' "$job" "$POLARDB_TEST_SHARD" "$POLARDB_TEST_OBJECT_SUFFIX"
	job_command "$job"
}

run_job_capture() {
	local job="$1"
	local shard="$2"
	local tag="$3"
	local log="$4"

	if [ "$tag" = "backend_exclusive" ] && command -v flock >/dev/null 2>&1; then
		(
			exec 7>"$BACKEND_EXCLUSIVE_LOCK"
			printf '# waiting for backend-exclusive lock: %s\n' "$BACKEND_EXCLUSIVE_LOCK"
			flock 7
			printf '# acquired backend-exclusive lock: %s\n' "$BACKEND_EXCLUSIVE_LOCK"
			run_job_body "$job" "$shard"
		) >"$log" 2>&1
	else
		run_job_body "$job" "$shard" >"$log" 2>&1
	fi
}

report_job() {
	local job="$1"
	local status="$2"
	local log="$3"

	if [ "$status" -eq 0 ]; then
		printf '[tap] ok %s log=%s\n' "$job" "$log"
	else
		printf '[tap] FAIL %s status=%s log=%s\n' "$job" "$status" "$log"
		printf '[tap] last log lines for %s:\n' "$job"
		tail -80 "$log" 2>/dev/null | sed 's/^/[tap]   /'
	fi
}

active_pids=()
active_names=()
active_logs=()
failures=0

cleanup_active_jobs() {
	local pid

	for pid in "${active_pids[@]:-}"; do
		kill "$pid" 2>/dev/null || true
	done
}

trap 'cleanup_active_jobs; exit 130' INT TERM

wait_active_batch() {
	local i pid name log status

	for i in "${!active_pids[@]}"; do
		pid="${active_pids[$i]}"
		name="${active_names[$i]}"
		log="${active_logs[$i]}"
		if wait "$pid"; then
			status=0
		else
			status=$?
		fi
		report_job "$name" "$status" "$log"
		[ "$status" -eq 0 ] || failures=$((failures + 1))
	done
	active_pids=()
	active_names=()
	active_logs=()
}

run_parallel_jobs() {
	local shard="$SHARD_BASE"
	local job log safe_name

	for job in "$@"; do
		safe_name=$(safe_job_log_name "$job")
		log="$LOG_DIR/${safe_name}.log"
		printf '[tap] start parallel %s shard=%s log=%s\n' "$job" "$shard" "$log"
		run_job_capture "$job" "$shard" "parallel" "$log" &
		active_pids+=("$!")
		active_names+=("$job")
		active_logs+=("$log")
		shard=$((shard + 1))
		if [ "${#active_pids[@]}" -ge "$PARALLEL_JOBS" ]; then
			wait_active_batch
		fi
	done
	wait_active_batch
}

run_sequential_jobs() {
	local shard="$EXCLUSIVE_SHARD_BASE"
	local job log safe_name status

	for job in "$@"; do
		safe_name=$(safe_job_log_name "$job")
		log="$LOG_DIR/${safe_name}.log"
		printf '[tap] start backend_exclusive %s shard=%s log=%s\n' "$job" "$shard" "$log"
		if run_job_capture "$job" "$shard" "backend_exclusive" "$log"; then
			status=0
		else
			status=$?
		fi
		report_job "$job" "$status" "$log"
		[ "$status" -eq 0 ] || failures=$((failures + 1))
		shard=$((shard + 1))
	done
}

if [ "$MODE" = "list" ]; then
	list_manifest
	exit 0
fi

mkdir -p "$LOCK_DIR" "$LOG_DIR"
if command -v flock >/dev/null 2>&1; then
	exec 8>"$RUN_LOCK"
	if ! flock -n 8; then
		echo "[tap] another PolarDB TAP scheduler holds $RUN_LOCK" >&2
		exit 1
	fi
fi

if ! check_selected_ports; then
	echo "[tap] listener isolation check failed" >&2
	exit 1
fi

printf '[tap] mode=%s parallel_jobs=%s output=%s\n' "$MODE" "$PARALLEL_JOBS" "$LOG_DIR"

if [ "$MODE" = "all" ] || [ "$MODE" = "safe" ]; then
	# shellcheck disable=SC2086
	run_parallel_jobs $SAFE_JOBS
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "exclusive" ]; then
	# shellcheck disable=SC2086
	run_sequential_jobs $EXCLUSIVE_JOBS
fi

if [ "$failures" -gt 0 ]; then
	printf '[tap] completed with %s failed job(s)\n' "$failures"
	exit 1
fi
printf '[tap] completed successfully\n'
