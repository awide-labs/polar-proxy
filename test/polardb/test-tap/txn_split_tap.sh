#!/usr/bin/env bash
# TAP integration test for PolarDB transaction-split reads.
#
# This script is intentionally a TAP adapter over lib/scenario_harness.sh. The
# harness owns ProxySQL lifecycle, split SQL generation, WAL generation, pgbench
# execution, log capture, and routing evidence. This file only maps selected
# split scenarios into TAP results.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"

PLAN=23
FAIL=0
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_txn_split_tap")}"
export PROXYSQL_DATA_DIR
LAST_SPLIT_RUN_DIR=""
TXN_SPLIT_TAP_GROUPS="${TXN_SPLIT_TAP_GROUPS:-}"
TXN_SPLIT_TAP_CASES="${TXN_SPLIT_TAP_CASES:-}"

# case_id | group | tag | plan_count | function
# 6,7 carry one extra tied probe each; the scheduler must keep the probe in the
# same group as the scenario case. Timeout cases mutate backend replay lag and
# stay backend-exclusive on a shared topology.
split_cases_for_group() {
	case "$1" in
	split-core-positive) printf '%s\n' "6 7 8 10 12 13 14 21 22" ;;
	split-warmup) printf '%s\n' "9 11 17 18 19 20 23 24 25" ;;
	split-timeout) printf '%s\n' "15 16" ;;
	*)
		diag "unknown txn_split TAP group: $1"
		return 1
		;;
	esac
}

split_append_case_once() {
	local list="$1"
	local case_num="$2"
	local item

	for item in $list; do
		[ "$item" = "$case_num" ] && {
			printf '%s\n' "$list"
			return 0
		}
	done
	if [ -n "$list" ]; then
		printf '%s %s\n' "$list" "$case_num"
	else
		printf '%s\n' "$case_num"
	fi
}

split_expand_groups() {
	local group case_num group_cases selected=""

	for group in ${TXN_SPLIT_TAP_GROUPS//,/ }; do
		group_cases=$(split_cases_for_group "$group") || return 1
		for case_num in $group_cases; do
			selected=$(split_append_case_once "$selected" "$case_num")
		done
	done
	printf '%s\n' "$selected"
}

if [ -z "$TXN_SPLIT_TAP_CASES" ] && [ -n "$TXN_SPLIT_TAP_GROUPS" ]; then
	TXN_SPLIT_TAP_CASES="$(split_expand_groups)" || exit 1
fi

split_case_selected() {
	local wanted

	[ -z "$TXN_SPLIT_TAP_CASES" ] && return 0
	for wanted in ${TXN_SPLIT_TAP_CASES//,/ }; do
		[ "$wanted" = "$1" ] && return 0
	done
	return 1
}

split_case_plan() {
	local planned=1
	local wanted

	if [ -z "$TXN_SPLIT_TAP_CASES" ]; then
		printf '%s\n' "$PLAN"
		return
	fi
	for wanted in ${TXN_SPLIT_TAP_CASES//,/ }; do
		case "$wanted" in
		6 | 7)
			planned=$((planned + 2))
			;;
		*)
			planned=$((planned + 1))
			;;
		esac
	done
	printf '%s\n' "$planned"
}

run_selected_split_case() {
	local case_num="$1"

	if split_case_selected "$case_num"; then
		run_split_case "$@"
	fi
}

run_selected_split_case_with_warmup_wait() {
	local case_num="$2"

	if split_case_selected "$case_num"; then
		run_split_case_with_warmup_wait "$@"
	fi
}

verify_split_trace() {
	local split_variant="${1:-basic}"
	local run_dir="$2"
	local log_file="$run_dir/proxysql_debug.log"
	local trace_ok=0
	local debug_expected=0

	if [ -n "$run_dir" ] && grep -q "Debug: enabled" "$run_dir/test.log" 2>/dev/null; then
		debug_expected=1
	fi

	# The scenario harness already checks the public behavior with backend-visible
	# results and ProxySQL counters. Trace checks are extra diagnostics: keep them
	# strict when this build emits PolarDB trace lines, but do not fail a real
	# integration scenario only because the runtime did not write those optional
	# strings.
	if [ -z "$run_dir" ] || [ ! -f "$log_file" ]; then
		diag "missing ProxySQL debug log for split trace verification: $log_file"
		return 0
	fi
	if ! tap_trace_checks_enabled "$log_file"; then
		if [ "$debug_expected" -eq 1 ]; then
			diag "split trace verification skipped: debug enabled but no PolarDB trace lines in $log_file"
		else
			diag "split trace verification skipped: no PolarDB trace lines in $log_file"
		fi
		return 0
	fi

	case "$split_variant" in
	basic | multi | combined)
		tap_trace_must_count_ge "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: prepared" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: completed split read" 1 || trace_ok=1
		;;
	prewrite)
		tap_trace_must_count_ge "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: prepared" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: completed split read" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB PLAN: transaction pre-write read -> reader wait path" 1 || trace_ok=1
		# The pre-write read is deliberately before split evidence exists. It
		# may prepare a reader wait, bypass on an already-fresh reader, or stay
		# on the writer if the cold pool is not ready. The required evidence for
		# this mixed case is the planner decision above plus the later split
		# dispatch traces; readonly cases assert the concrete TXN_WAIT branch.
		;;
	lazy | warmup_demand | warmup_begin | warmup_begin_prompt | warmup_both)
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: queued split pool request" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: added connected split pool connection" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: prepared" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: completed split read" 1 || trace_ok=1
		case "$split_variant" in
		warmup_demand)
			tap_trace_must_have "$log_file" "PolarDB SET: txn_split_warmup_mode=demand" || trace_ok=1
			tap_trace_must_have "$log_file" "PolarDB WARMUP: requested split pool reason=demand" || trace_ok=1
			;;
		warmup_begin | warmup_begin_prompt)
			tap_trace_must_have "$log_file" "PolarDB SET: txn_split_warmup_mode=begin" || trace_ok=1
			tap_trace_must_have "$log_file" "PolarDB WARMUP: requested split pool reason=begin" || trace_ok=1
			;;
		warmup_both)
			tap_trace_must_have "$log_file" "PolarDB SET: txn_split_warmup_mode=both" || trace_ok=1
			tap_trace_must_have "$log_file" "PolarDB WARMUP: requested split pool reason=begin" || trace_ok=1
			;;
		esac
		;;
	proxy_identity_client)
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: queued split pool request" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: added connected split pool connection" 1 || trace_ok=1
		tap_trace_must_have "$log_file" "identity_mode=client" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB TXN_SPLIT: prepared" || trace_ok=1
		;;
	proxy_identity_proxy)
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: queued split pool request" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB WARMUP: added connected split pool connection" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: prepared" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_SPLIT: completed split read" 1 || trace_ok=1
		tap_trace_must_have "$log_file" "identity_mode=proxy" || trace_ok=1
		;;
	lazy_disabled | warmup_off)
		if [ "$split_variant" = "warmup_off" ]; then
			tap_trace_must_have "$log_file" "PolarDB SET: txn_split_warmup_mode=off" || trace_ok=1
			tap_trace_must_have "$log_file" "PolarDB WARMUP: demand request suppressed mode=off" || trace_ok=1
			tap_trace_must_not_have "$log_file" "PolarDB WARMUP: queued split pool request" || trace_ok=1
		else
			tap_trace_must_have "$log_file" "PolarDB WARMUP: split lazy warmup disabled; skip request" || trace_ok=1
		fi
		tap_trace_must_not_have "$log_file" "PolarDB WARMUP: added connected split pool connection" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB TXN_SPLIT: prepared" || trace_ok=1
		;;
	for_update)
		tap_trace_must_have "$log_file" "reason=split_locking_read" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" || trace_ok=1
		;;
	readonly)
		tap_trace_must_have "$log_file" "PolarDB TXN_SPLIT: observed primary RFQ status=T" || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB PLAN: transaction pre-write read -> reader wait path" 1 || trace_ok=1
		tap_trace_must_count_ge "$log_file" "PolarDB TXN_WAIT: prepared" 1 || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" || trace_ok=1
		;;
	readonly_repeatable)
		tap_trace_must_have "$log_file" "PolarDB TXN_WAIT: pre-write reader waits blocked by isolation" || trace_ok=1
		tap_trace_must_have "$log_file" "PolarDB PLAN: transaction pre-write reader wait blocked read_committed=0" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB TXN_WAIT: prepared" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" || trace_ok=1
		;;
	readonly_set_local)
		tap_trace_must_have "$log_file" "PolarDB TXN_WAIT: pre-write reader waits blocked by transaction local state" || trace_ok=1
		tap_trace_must_have "$log_file" "PolarDB PLAN: transaction pre-write reader wait blocked read_committed=1 local_state_clean=0" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB TXN_WAIT: prepared" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" || trace_ok=1
		;;
	route_primary)
		tap_trace_must_have "$log_file" "PolarDB PLAN: route=primary hint -> FORCE_PRIMARY" || trace_ok=1
		tap_trace_must_not_have "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" || trace_ok=1
		;;
	timeout)
		tap_trace_must_count_eq "$log_file" "PolarDB PLAN: REPLICA_TXN_SPLIT planned" 1 || trace_ok=1
		tap_trace_must_count_eq "$log_file" "PolarDB FAILURE: wrapper-result handler action=retry" 1 || trace_ok=1
		tap_trace_must_count_eq "$log_file" "PolarDB FAILURE: policy kind=wait_timeout action=retry target=writer" 1 || trace_ok=1
		tap_trace_must_count_eq "$log_file" "PolarDB FAILURE: retry split read on writer_hg" 1 || trace_ok=1
		;;
	esac

	return "$trace_ok"
}

run_split_case() {
	local case_num="$1"
	local case_name="$2"
	local split_variant="$3"
	local test_id="$4"
	local polar_mode="${5:-best_effort}"
	local expect_outcome="${6:-success}"
	local output rc run_dir

	CASE_NUM="$case_num"
	CASE_NAME="$case_name"
	CONSISTENCY_MODE=1
	SPLIT_ENABLED=1
	XACT_SPLIT=1
	TEST_ID="$test_id"
	POLAR_MODE="$polar_mode"
	EXPECT_OUTCOME="$expect_outcome"
	if [ -n "$split_variant" ]; then
		SPLIT_VARIANT="$split_variant"
	else
		unset SPLIT_VARIANT
	fi

	output=$(run_test "Case $case_num: $case_name" run_consistency_test 2>&1)
	rc=$?
	run_dir=$(printf '%s\n' "$output" | sed -n 's/.*Run dir: //p' | tail -1)
	LAST_SPLIT_RUN_DIR="$run_dir"
	[ -n "$run_dir" ] && diag "case $case_num run dir: $run_dir"

	if [ "$rc" -eq 0 ] && ! verify_split_trace "$split_variant" "$run_dir"; then
		rc=1
	fi

	if [ "$rc" -eq 0 ]; then
		ok 0 "transaction split case $case_num: $case_name"
	else
		diag "case $case_num output follows"
		printf '%s\n' "$output" | sed 's/^/#   /'
		ok 1 "transaction split case $case_num: $case_name"
	fi
}

run_split_case_with_warmup_wait() {
	local warmup_wait_sec="$1"
	shift
	local old_wait="${POLARDB_SPLIT_WARMUP_WAIT_SEC-}"
	local had_wait=0
	[ "${POLARDB_SPLIT_WARMUP_WAIT_SEC+x}" = "x" ] && had_wait=1

	POLARDB_SPLIT_WARMUP_WAIT_SEC="$warmup_wait_sec"
	run_split_case "$@"
	if [ "$had_wait" -eq 1 ]; then
		POLARDB_SPLIT_WARMUP_WAIT_SEC="$old_wait"
	else
		unset POLARDB_SPLIT_WARMUP_WAIT_SEC
	fi
}

assert_backend_isolation_report_consumed() {
	local run_dir="$1"
	local log_file="$run_dir/proxysql_debug.log"
	local output rc
	local debug_expected=0

	if [ -n "$run_dir" ] && grep -q "Debug: enabled" "$run_dir/test.log" 2>/dev/null; then
		debug_expected=1
	fi

	output=$("$SCRIPT_DIR/../test-c/run_helper.sh" libpq_xact --probe-isolation-report 2>&1)
	rc=$?
	printf '%s\n' "$output" | sed 's/^/# isolation probe: /'

	case "$rc" in
	0)
		;;
	2)
		skip_ok "backend isolation ParameterStatus consumed by ProxySQL" \
			"backend does not report default_transaction_isolation"
		return
		;;
	*)
		ok 1 "backend isolation ParameterStatus consumed by ProxySQL"
		return
		;;
	esac

	if [ -z "$run_dir" ] || [ ! -f "$log_file" ]; then
		skip_ok "backend isolation ParameterStatus consumed by ProxySQL" \
			"pre-write case debug log unavailable"
		return
	fi
	if ! tap_trace_checks_enabled "$log_file"; then
		if [ "$debug_expected" -eq 1 ]; then
			diag "backend isolation trace skipped: debug enabled but no PolarDB trace lines in $log_file"
		fi
		skip_ok "backend isolation ParameterStatus consumed by ProxySQL" \
			"ProxySQL PolarDB trace unavailable"
		return
	fi

	if tap_trace_must_have "$log_file" \
			"PolarDB TXN_WAIT: backend default_transaction_isolation"; then
		ok 0 "backend isolation ParameterStatus consumed by ProxySQL"
	else
		ok 1 "backend isolation ParameterStatus consumed by ProxySQL"
	fi
}

assert_backend_w_marker_supported() {
	local output rc

	output=$("$SCRIPT_DIR/../test-c/run_helper.sh" libpq_xact --require-w-marker 2>&1)
	rc=$?
	printf '%s\n' "$output" | sed 's/^/# w-marker probe: /'

	if [ "$rc" -eq 0 ]; then
		ok 0 "backend emits transaction WAL-pending RFQ marker"
	else
		ok 1 "backend emits transaction WAL-pending RFQ marker"
	fi
}

PLAN="$(split_case_plan)"
plan "$PLAN"
if [ -n "$TXN_SPLIT_TAP_CASES" ]; then
	diag "selected split TAP cases: $TXN_SPLIT_TAP_CASES"
fi
if [ -n "$TXN_SPLIT_TAP_GROUPS" ]; then
	diag "selected split TAP groups: $TXN_SPLIT_TAP_GROUPS"
fi
diag "test profile: POLARDB_TEST_ENV=$POLARDB_TEST_ENV POLARDB_DCS_MODE=$POLARDB_DCS_MODE writer=${PRIMARY_HOST:-auto}:${PRIMARY_PORT:-auto} readers=${POLARDB_REPLICA_ENDPOINTS:-${REPLICA_HOST:-auto}:${REPLICA_PORT:-auto}}"
polardb_require_proxysql_or_skip_all polardb
polardb_require_command_or_skip_all psql psql
if ! pgbench_check=$(polardb_require_pgbench 2>&1); then
	diag "$pgbench_check"
	polardb_skip_remaining "pgbench unavailable" "$PLAN"
	exit 0
fi

topology_error_file="$(mktemp "${TMPDIR:-/tmp}/polardb-topology.XXXXXX")"
if polardb_detect_topology 2>"$topology_error_file"; then
	rm -f "$topology_error_file"
	ok 0 "detect PolarDB primary/replica topology"
	diag "topology selected: primary=$PRIMARY_HOST:$PRIMARY_PORT replicas=${POLARDB_REPLICA_ENDPOINTS:-$REPLICA_HOST:$REPLICA_PORT}"
else
	topology_error="$(cat "$topology_error_file" 2>/dev/null || true)"
	rm -f "$topology_error_file"
	[ -z "${topology_error:-}" ] || diag "$topology_error"
	skip_ok "detect PolarDB primary/replica topology" "PolarDB topology unavailable"
	polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
	exit 0
fi

HARNESS_AUTODETECT="$POLARDB_AUTODETECT"
POLARDB_AUTODETECT=0
# shellcheck source=../lib/scenario_harness.sh
source "$SCRIPT_DIR/../lib/scenario_harness.sh"
POLARDB_AUTODETECT="$HARNESS_AUTODETECT"

cleanup() {
	stop_wal_generator >/dev/null 2>&1 || true
	disable_replay_lag >/dev/null 2>&1 || true
	stop_proxysql >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_selected_split_case 6 "Split + LSN" "" 6
if split_case_selected 6; then
	assert_backend_w_marker_supported
fi
run_selected_split_case 7 "Pre-Write Read Then Split" "prewrite" 7
if split_case_selected 7; then
	assert_backend_isolation_report_consumed "$LAST_SPLIT_RUN_DIR"
fi
run_selected_split_case 8 "Multi Split" "multi" 8
run_selected_split_case 9 "Lazy Split Warmup" "lazy" 9
run_selected_split_case 11 "Lazy Split Warmup Disabled" "lazy_disabled" 11
run_selected_split_case 17 "Split Warmup Mode Off" "warmup_off" 17
run_selected_split_case 18 "Split Warmup Mode Demand" "warmup_demand" 18
run_selected_split_case 19 "Split Warmup Mode Begin" "warmup_begin" 19
run_selected_split_case 20 "Split Warmup Mode Both" "warmup_both" 20
run_selected_split_case_with_warmup_wait 3 23 "Split Warmup Begin Prompt Drain" "warmup_begin_prompt" 23
run_selected_split_case_with_warmup_wait 3 24 "Split Proxy Identity Client Cross-Frontend" "proxy_identity_client" 24
run_selected_split_case_with_warmup_wait 3 25 "Split Proxy Identity Proxy Cross-Frontend" "proxy_identity_proxy" 25
run_selected_split_case 10 "Locking Read Veto" "for_update" 10
run_selected_split_case 12 "Read-Only Transaction" "readonly" 12
run_selected_split_case 21 "Read-Only Transaction Repeatable Read Veto" "readonly_repeatable" 21
run_selected_split_case 22 "Read-Only Transaction SET LOCAL Veto" "readonly_set_local" 22
run_selected_split_case 13 "Combined Split Workload" "combined" 13
run_selected_split_case 14 "Route Primary Split Veto" "route_primary" 14
run_selected_split_case 15 "Split Timeout Best Effort Primary Retry" "timeout" 15 "best_effort" "failure"
run_selected_split_case 16 "Split Timeout Strict Primary Retry" "timeout" 16 "strict" "failure"

diag "transaction-split TAP completed: failed=$FAIL"
exit "$FAIL"
