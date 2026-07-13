#!/usr/bin/env bash
# TAP integration test for PolarDB reader-failure policy.
#
# The normal split and wait-timeout TAPs show successful routing plus default
# retry. This script drives the remaining common policy matrix with DEBUG-only
# fault injection: retry, forward, terminate, timeout/death/reusable-error
# classification, retry-decline cleanup, writer-state loss, the writer route
# overriding a later manual reader route, and session-consistency wait-read
# handling through the same reader-action selector.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

POLICY_SETUP_PLAN=3
POLICY_CASE_LAST=21
PLAN=21
FAIL=0
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_txn_split_failure_policy_tap")}"
export PROXYSQL_DATA_DIR
export POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE="$PROXYSQL_DATA_DIR/split_failure_fault"
export POLARDB_DEBUG_POST_SEND_OFFLINE_FILE="$PROXYSQL_DATA_DIR/post_send_offline_fault"

WRITER_HG="$POLARDB_WRITER_HG"
READER_HG="$POLARDB_READER_HG"
SELECT_RULE_ID="$POLARDB_SELECT_RULE_ID"
TEST_TABLE="${TEST_TABLE:-$(polardb_test_identifier consistency_test)}"
LOGGING_STARTED=0
PROXYSQL_STARTED=0
OFFLINE_READER_HOST=""
OFFLINE_READER_PORT=""
POLARDB_SPLIT_PREWARM_WAIT_HELPER="$PROXYSQL_DATA_DIR/wait_split_warmup.sh"
POLARDB_SPLIT_TXN_WAIT_HELPER="$PROXYSQL_DATA_DIR/wait_split_txn_ready.sh"
export POLARDB_SPLIT_PREWARM_WAIT_HELPER
export POLARDB_SPLIT_TXN_WAIT_HELPER

policy_case_selected() {
	local case_num="$1"
	local wanted

	if [ -z "${TXN_SPLIT_FAILURE_POLICY_TAP_CASES:-}" ]; then
		return 0
	fi
	for wanted in ${TXN_SPLIT_FAILURE_POLICY_TAP_CASES//,/ }; do
		[ "$wanted" = "$case_num" ] && return 0
	done
	return 1
}

policy_case_plan() {
	local plan="$POLICY_SETUP_PLAN"
	local case_num

	if [ -z "${TXN_SPLIT_FAILURE_POLICY_TAP_CASES:-}" ]; then
		echo "$PLAN"
		return 0
	fi
	for case_num in $(seq 4 "$POLICY_CASE_LAST"); do
		if policy_case_selected "$case_num"; then
			plan=$((plan + 1))
		fi
	done
	echo "$plan"
}

run_selected_policy_case() {
	local case_num="$1"
	shift

	if policy_case_selected "$case_num"; then
		run_policy_case "$@"
	fi
}

run_selected_session_lsn_timeout_policy_case() {
	local case_num="$1"
	shift

	if policy_case_selected "$case_num"; then
		run_session_lsn_timeout_policy_case "$@"
	fi
}

run_selected_session_lsn_best_effort_skips_failure_policy() {
	local case_num="$1"

	if policy_case_selected "$case_num"; then
		run_session_lsn_best_effort_skips_failure_policy
	fi
}

policy_trace_log() {
	printf '%s\n' "${PROXYSQL_LOG:-${PROXYSQL_DATA_DIR}/proxysql.log}"
}

policy_trace_count() {
	tap_trace_count "$(policy_trace_log)" "$1"
}

policy_trace_delta() {
	local pattern="$1"
	local before="$2"
	local after

	if ! tap_trace_checks_enabled "$(policy_trace_log)"; then
		echo -1
		return 0
	fi
	after=$(policy_trace_count "$pattern")
	echo $((after - before))
}

policy_counter() {
	get_counter "$1" | tr -d '[:space:]'
}

write_split_prewarm_wait_helper() {
	cat >"$POLARDB_SPLIT_PREWARM_WAIT_HELPER" <<'EOSH'
#!/usr/bin/env bash
set -u

created_before="${1:-0}"
already_before="${2:-0}"
deadline=$((SECONDS + ${POLARDB_SPLIT_PREWARM_WAIT_SEC:-10}))

counter() {
	local name="$1"
	local value
	value=$(PGPASSWORD="$PROXYSQL_ADMIN_PASSWORD" PGSSLMODE="$PROXYSQL_ADMIN_PGSSLMODE" psql \
		-h "$PROXYSQL_HOST" -p "$PROXYSQL_ADMIN_PORT" \
		-U "$PROXYSQL_ADMIN_USER" -d "$PROXYSQL_ADMIN_DATABASE" -A -t -q -v ON_ERROR_STOP=1 \
		-c "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$name';" \
		2>/dev/null | tr -d '[:space:]')
	printf '%s\n' "${value:-0}"
}

while :; do
	created=$(counter PolarDB_Split_Warmup_Created)
	already=$(counter PolarDB_Split_Warmup_Already_Warm)
	pending=$(counter PolarDB_Warmup_Pending)
	failed=$(counter PolarDB_Split_Warmup_Failed)
	no_target=$(counter PolarDB_Split_Warmup_No_Target)
	bad_request=$(counter PolarDB_Split_Warmup_Bad_Request)
	connect_failed=$(counter PolarDB_Split_Warmup_Connect_Failed)
	add_failed=$(counter PolarDB_Split_Warmup_Add_Failed)

	if [ "${created:-0}" -gt "${created_before:-0}" ] ||
		[ "${already:-0}" -gt "${already_before:-0}" ]; then
		printf 'POLARDB_SPLIT_PREWARM_READY created=%s already_warm=%s pending=%s\n' \
			"${created:-0}" "${already:-0}" "${pending:-0}"
		exit 0
	fi

	if [ "$SECONDS" -ge "$deadline" ]; then
		printf 'POLARDB_SPLIT_PREWARM_TIMEOUT created=%s already_warm=%s pending=%s failed=%s no_target=%s bad_request=%s connect_failed=%s add_failed=%s\n' \
			"${created:-0}" "${already:-0}" "${pending:-0}" "${failed:-0}" \
			"${no_target:-0}" "${bad_request:-0}" "${connect_failed:-0}" \
			"${add_failed:-0}"
		exit 0
	fi

	sleep 0.1
done
EOSH
	chmod +x "$POLARDB_SPLIT_PREWARM_WAIT_HELPER"
}

write_split_txn_wait_helper() {
	cat >"$POLARDB_SPLIT_TXN_WAIT_HELPER" <<'EOSH'
#!/usr/bin/env bash
set -u

became_before="${1:-0}"
attempt="${2:-1}"
max_attempts="${3:-1}"

counter() {
	local name="$1"
	local value
	value=$(PGPASSWORD="$PROXYSQL_ADMIN_PASSWORD" PGSSLMODE="$PROXYSQL_ADMIN_PGSSLMODE" psql \
		-h "$PROXYSQL_HOST" -p "$PROXYSQL_ADMIN_PORT" \
		-U "$PROXYSQL_ADMIN_USER" -d "$PROXYSQL_ADMIN_DATABASE" -A -t -q -v ON_ERROR_STOP=1 \
		-c "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$name';" \
		2>/dev/null | tr -d '[:space:]')
	printf '%s\n' "${value:-0}"
}

became=$(counter PolarDB_Txn_Became_Splittable)
wal_pending=$(counter PolarDB_Split_WAL_Pending)
if [ "${became:-0}" -gt "${became_before:-0}" ]; then
	printf 'POLARDB_SPLIT_TXN_READY became=%s wal_pending=%s attempt=%s\n' \
		"${became:-0}" "${wal_pending:-0}" "$attempt"
	exit 0
fi

if [ "$attempt" -ge "$max_attempts" ]; then
	printf 'POLARDB_SPLIT_TXN_NOT_READY became=%s wal_pending=%s attempts=%s\n' \
		"${became:-0}" "${wal_pending:-0}" "$max_attempts"
	exit 0
fi

sleep "${POLARDB_SPLIT_TXN_PROBE_DELAY_SEC:-0.1}"
EOSH
	chmod +x "$POLARDB_SPLIT_TXN_WAIT_HELPER"
}

configure_split_policy() {
	local timeout_action="$1"
	local death_action="$2"
	local error_action="$3"
	local manual_reader_rule="${4:-0}"
	local warmup_max_connections="${5:-1}"
	local manual_rule_id=$((SELECT_RULE_ID - 1))

	proxysql_admin "UPDATE global_variables SET variable_value='lsn' WHERE variable_name='pgsql-polardb_consistency_mode';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='strict' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='v15' WHERE variable_name='pgsql-polardb_proxy_protocol';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='1' WHERE variable_name='pgsql-polardb_lazy_warmup_split';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='$death_action' WHERE variable_name='pgsql-polardb_reader_death_action';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='$timeout_action' WHERE variable_name='pgsql-polardb_reader_timeout_action';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='$error_action' WHERE variable_name='pgsql-polardb_reader_error_action';" >/dev/null
	proxysql_admin "UPDATE global_variables SET variable_value='$warmup_max_connections' WHERE variable_name='pgsql-polardb_split_warmup_max_connections_per_request';" >/dev/null
	proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

	proxysql_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=1, lsn_wait_timeout_ms=100, consistency_mode='lsn', max_lag_bytes=-1, proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	proxysql_admin "DELETE FROM pgsql_query_rules;" >/dev/null
	if [ "$manual_reader_rule" = "1" ]; then
		proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply, comment) VALUES ($manual_rule_id, 1, '^SELECT 42', $READER_HG, 1, 'split_failure_manual_reader');" >/dev/null
	fi
	proxysql_admin "INSERT INTO pgsql_query_rules (rule_id, active, match_digest, replica_eligible, apply, comment) VALUES ($SELECT_RULE_ID, 1, '^SELECT', 1, 0, 'split_failure_auto_select');" >/dev/null
	proxysql_admin "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

reset_case_table() {
	query_proxy "DROP TABLE IF EXISTS $TEST_TABLE; CREATE TABLE $TEST_TABLE(id int PRIMARY KEY, data text);" >/dev/null
}

run_split_failure_sql() {
	local row_id="$1"
	local include_manual_probe="${2:-0}"
	local fault="${3:--}"
	local warmup_created_before="${4:-0}"
	local warmup_already_before="${5:-0}"
	local txn_became_before="${6:-0}"
	local manual_sql=""
	local fault_sql=""
	local txn_probe_sql=""
	local txn_probe_count="${POLARDB_SPLIT_TXN_READY_PROBES:-20}"

	if [ "$include_manual_probe" = "1" ]; then
		manual_sql="SELECT 42;"
	fi

	if [ "$fault" = "-" ]; then
		fault_sql="\\! : > \"\$POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE\" || echo POLARDB_SPLIT_FAULT_FILE_WRITE_FAILED"
	else
		fault_sql="\\! printf '%s\\n' '$fault' > \"\$POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE\" || echo POLARDB_SPLIT_FAULT_FILE_WRITE_FAILED"
	fi
	for ((probe_i = 1; probe_i <= txn_probe_count; probe_i++)); do
		txn_probe_sql="${txn_probe_sql}
/* route=primary */ SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;
\\! \"\$POLARDB_SPLIT_TXN_WAIT_HELPER\" $txn_became_before $probe_i $txn_probe_count"
	done

	proxy_script "\\set ON_ERROR_STOP off
\\! : > \"\$POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE\" || echo POLARDB_SPLIT_FAULT_FILE_WRITE_FAILED
SET proxysql.polardb_txn_split_warmup TO 'begin';
BEGIN;
COMMIT;
\\! \"\$POLARDB_SPLIT_PREWARM_WAIT_HELPER\" $warmup_created_before $warmup_already_before
SELECT 1;
BEGIN;
INSERT INTO $TEST_TABLE VALUES ($row_id, 'split_failure$row_id') ON CONFLICT (id) DO UPDATE SET data='split_failure$row_id';
$txn_probe_sql
$fault_sql
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;
$manual_sql
COMMIT;"
}

run_policy_case() {
	local label="$1"
	local row_id="$2"
	local fault="$3"
	local timeout_action="$4"
	local death_action="$5"
	local error_action="$6"
	local expected_outcome="$7"
	local include_manual_probe="${8:-0}"
	local extra_trace="${9:-}"
	local extra_counter="${10:-}"
	local out rc
	local retry_before reader_retry_before forward_before term_before
	local trace_action_before trace_retry_before trace_reader_retry_before trace_forward_before trace_term_before
	local trace_manual_before trace_extra_before trace_prepare_before trace_decline_before
	local reader_pool_destroy_failure_before reader_pool_destroy_failure_delta
	local extra_counter_before extra_counter_delta
	local retry_delta reader_retry_delta forward_delta term_delta
	local action_trace_delta retry_trace_delta reader_retry_trace_delta forward_trace_delta term_trace_delta
	local manual_trace_delta extra_trace_delta prepare_trace_delta decline_trace_delta
	local expected_kind="wait_timeout"
	local expected_action="$timeout_action"
	local warmup_max_connections=1
	local warmup_created_before warmup_already_before txn_became_before
	local prewarm_failed=0
	local txn_ready_failed=0

	diag "starting policy case: $label"
	diag "resetting table for policy case: $label"
	reset_case_table || {
		ok 1 "$label"
		return
	}
	diag "table reset completed for policy case: $label"
	if [ "$expected_outcome" = "retry_reader_then_writer" ] ||
			printf '%s\n' "$extra_trace" | grep -q "target=other_reader"; then
		warmup_max_connections=2
	fi
	configure_split_policy "$timeout_action" "$death_action" "$error_action" \
		"$include_manual_probe" "$warmup_max_connections" || {
		ok 1 "$label"
		return
	}
	if [ "$expected_outcome" = "retry_reader_then_writer" ]; then
		local reader_online_count
		reader_online_count=$(
			proxysql_admin "SELECT COUNT(*) FROM runtime_pgsql_servers WHERE hostgroup_id=$READER_HG AND status='ONLINE';" 2>/dev/null |
				tr -d '[:space:]'
		)
		if [ "${reader_online_count:-0}" -lt 2 ]; then
			skip_ok "$label" "requires at least two online reader endpoints"
			return
		fi
	fi
	if ! set_polar_proxy_wait_timeout_ms 10 >/dev/null 2>&1; then
		skip_ok "$label" "cannot set polar_proxy_wait_timeout_ms through DCS"
		return
	fi
	if ! enable_replay_lag 50000 >/dev/null 2>&1; then
		skip_ok "$label" "cannot enable replica replay lag"
		return
	fi

	retry_before=$(policy_counter "PolarDB_Split_Reads_Retried")
	reader_retry_before=$(policy_counter "PolarDB_Split_Reads_Retried_On_Reader")
	forward_before=$(policy_counter "PolarDB_Split_Reads_Forwarded")
	term_before=$(policy_counter "PolarDB_Reader_Terminations")
	if [ "$fault" = "death" ] || [ "$fault" = "death_twice" ]; then
		expected_kind="connection_lost"
		expected_action="$death_action"
	elif [ "$fault" = "sql_error" ]; then
		expected_kind="reusable_error"
		expected_action="$error_action"
	fi
	trace_action_before=$(policy_trace_count "PolarDB FAILURE: policy kind=$expected_kind action=$expected_action")
	trace_retry_before=$(policy_trace_count "PolarDB FAILURE: retry split read on writer_hg")
	trace_reader_retry_before=$(policy_trace_count "PolarDB FAILURE: retry split read on other_reader_hg")
	trace_forward_before=$(policy_trace_count "PolarDB FAILURE: forwarded reader error")
	trace_term_before=$(policy_trace_count "PolarDB FAILURE: terminating session after reader failure")
	trace_manual_before=$(policy_trace_count "PolarDB PIPELINE: reader-failure writer route writer_hg")
	trace_prepare_before=$(policy_trace_count "PolarDB TXN_SPLIT: prepared")
	trace_decline_before=$(policy_trace_count "PolarDB TXN_SPLIT: prepare declined")
	trace_extra_before=0
	[ -n "$extra_trace" ] && trace_extra_before=$(policy_trace_count "$extra_trace")
	reader_pool_destroy_failure_before=0
	if [ "$extra_trace" = "PolarDB READER_POOL: destroy used" ]; then
		reader_pool_destroy_failure_before=$(policy_counter "PolarDB_Reader_Pool_Destroy_From_Failure")
	fi
	extra_counter_before=0
	[ -n "$extra_counter" ] && extra_counter_before=$(policy_counter "$extra_counter")
	warmup_created_before=$(policy_counter "PolarDB_Split_Warmup_Created")
	warmup_already_before=$(policy_counter "PolarDB_Split_Warmup_Already_Warm")
	txn_became_before=$(policy_counter "PolarDB_Txn_Became_Splittable")

	start_wal_generator 0.01 1000
	out=$(run_split_failure_sql "$row_id" "$include_manual_probe" "$fault" \
		"$warmup_created_before" "$warmup_already_before" \
		"$txn_became_before" 2>&1)
	rc=$?
	clear_debug_fault_file POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE >/dev/null 2>&1 || true
	stop_wal_generator
	disable_replay_lag >/dev/null 2>&1 || true
	wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

	retry_delta=$(($(policy_counter "PolarDB_Split_Reads_Retried") - retry_before))
	reader_retry_delta=$(($(policy_counter "PolarDB_Split_Reads_Retried_On_Reader") - reader_retry_before))
	forward_delta=$(($(policy_counter "PolarDB_Split_Reads_Forwarded") - forward_before))
	term_delta=$(($(policy_counter "PolarDB_Reader_Terminations") - term_before))
	action_trace_delta=$(policy_trace_delta "PolarDB FAILURE: policy kind=$expected_kind action=$expected_action" "$trace_action_before")
	retry_trace_delta=$(policy_trace_delta "PolarDB FAILURE: retry split read on writer_hg" "$trace_retry_before")
	reader_retry_trace_delta=$(policy_trace_delta "PolarDB FAILURE: retry split read on other_reader_hg" "$trace_reader_retry_before")
	forward_trace_delta=$(policy_trace_delta "PolarDB FAILURE: forwarded reader error" "$trace_forward_before")
	term_trace_delta=$(policy_trace_delta "PolarDB FAILURE: terminating session after reader failure" "$trace_term_before")
	manual_trace_delta=$(policy_trace_delta "PolarDB PIPELINE: reader-failure writer route writer_hg" "$trace_manual_before")
	prepare_trace_delta=$(policy_trace_delta "PolarDB TXN_SPLIT: prepared" "$trace_prepare_before")
	decline_trace_delta=$(policy_trace_delta "PolarDB TXN_SPLIT: prepare declined" "$trace_decline_before")
	extra_trace_delta=0
	[ -n "$extra_trace" ] && extra_trace_delta=$(policy_trace_delta "$extra_trace" "$trace_extra_before")
	reader_pool_destroy_failure_delta=0
	if [ "$extra_trace" = "PolarDB READER_POOL: destroy used" ]; then
		reader_pool_destroy_failure_delta=$(($(policy_counter "PolarDB_Reader_Pool_Destroy_From_Failure") - reader_pool_destroy_failure_before))
	fi
	extra_counter_delta=0
	[ -n "$extra_counter" ] && extra_counter_delta=$(($(policy_counter "$extra_counter") - extra_counter_before))

	local pass=1
	if printf '%s\n' "$out" | grep -q "POLARDB_SPLIT_PREWARM_TIMEOUT"; then
		prewarm_failed=1
	fi
	if printf '%s\n' "$out" | grep -q "POLARDB_SPLIT_FAULT_FILE_WRITE_FAILED"; then
		prewarm_failed=1
	fi
	if ! printf '%s\n' "$out" | grep -q "POLARDB_SPLIT_TXN_READY"; then
		txn_ready_failed=1
	fi
	if printf '%s\n' "$out" | grep -q "POLARDB_SPLIT_TXN_NOT_READY"; then
		txn_ready_failed=1
	fi
	case "$expected_outcome" in
	retry)
		[ "$retry_delta" -ge 1 ] && [ "$forward_delta" -eq 0 ] &&
			[ "$reader_retry_delta" -eq 0 ] && [ "$term_delta" -eq 0 ] &&
			[ "$retry_trace_delta" -ge 1 ] &&
			! printf '%s\n' "$out" | grep -q "ERROR" && pass=0
		;;
	retry_any)
		{ [ "$retry_delta" -ge 1 ] || [ "$reader_retry_delta" -ge 1 ]; } &&
			[ "$forward_delta" -eq 0 ] && [ "$term_delta" -eq 0 ] &&
			{ [ "$retry_trace_delta" -ge 1 ] || [ "$reader_retry_trace_delta" -ge 1 ]; } &&
			! printf '%s\n' "$out" | grep -q "ERROR" && pass=0
		;;
	retry_reader_then_writer)
		[ "$reader_retry_delta" -eq 1 ] &&
			[ "$retry_delta" -ge 1 ] &&
			[ "$forward_delta" -eq 0 ] &&
			[ "$term_delta" -eq 0 ] &&
			[ "$reader_retry_trace_delta" -eq 1 ] &&
			[ "$retry_trace_delta" -ge 1 ] &&
			! printf '%s\n' "$out" | grep -q "ERROR" && pass=0
		;;
	forward)
		[ "$forward_delta" -ge 1 ] && [ "$retry_delta" -eq 0 ] &&
			[ "$reader_retry_delta" -eq 0 ] &&
			[ "$term_delta" -eq 0 ] && [ "$forward_trace_delta" -ge 1 ] &&
			printf '%s\n' "$out" | grep -q "ERROR" && pass=0
		if [ "$include_manual_probe" = "1" ]; then
			[ "$manual_trace_delta" -ge 1 ] &&
				printf '%s\n' "$out" | grep -qx "42" || pass=1
		fi
		;;
	terminate)
		[ "$term_delta" -ge 1 ] && [ "$retry_delta" -eq 0 ] &&
			[ "$reader_retry_delta" -eq 0 ] &&
			[ "$term_trace_delta" -ge 1 ] &&
			{ [ "$rc" -ne 0 ] ||
				printf '%s\n' "$out" | grep -Eqi "server closed|terminat|connection"; } &&
			pass=0
		;;
	*)
		pass=1
		;;
	esac
	[ "$prewarm_failed" -eq 0 ] || pass=1
	[ "$txn_ready_failed" -eq 0 ] || pass=1
	{ [ "$action_trace_delta" -ge 1 ] || [ "$expected_outcome" = "terminate" ]; } || pass=1
	[ "$prepare_trace_delta" -ge 1 ] || pass=1
	{ [ -z "$extra_trace" ] || [ "$extra_trace_delta" -ge 1 ]; } || pass=1
	if [ "$extra_trace" = "PolarDB READER_POOL: destroy used" ] &&
			[ "$reader_pool_destroy_failure_delta" -lt 1 ]; then
		pass=1
	fi
	{ [ -z "$extra_counter" ] || [ "$extra_counter_delta" -ge 1 ]; } || pass=1

	if [ "$pass" -eq 0 ]; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc retry_delta=$retry_delta reader_retry_delta=$reader_retry_delta forward_delta=$forward_delta term_delta=$term_delta"
		diag "trace action=$action_trace_delta retry=$retry_trace_delta reader_retry=$reader_retry_trace_delta forward=$forward_trace_delta terminate=$term_trace_delta manual=$manual_trace_delta prepare=$prepare_trace_delta decline=$decline_trace_delta extra=$extra_trace_delta reader_pool_destroy_failure=$reader_pool_destroy_failure_delta extra_counter=$extra_counter extra_counter_delta=$extra_counter_delta prewarm_failed=$prewarm_failed txn_ready_failed=$txn_ready_failed"
		ok 1 "$label"
	fi
}

run_session_lsn_timeout_policy_case() {
	local label="$1"
	local row_id="$2"
	local timeout_action="$3"
	local out rc
	local wait_retry_before term_before split_retry_before split_forward_before
	local trace_action_before trace_retry_before trace_forward_before trace_term_before
	local wait_retry_delta term_delta split_retry_delta
	local split_forward_delta action_trace_delta retry_trace_delta forward_trace_delta term_trace_delta

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy "$timeout_action" retry forward 0 || {
		ok 1 "$label"
		return
	}
	proxysql_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0, lsn_wait_timeout_ms=100, consistency_mode='lsn', proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	if ! set_polar_proxy_wait_timeout_ms 10 >/dev/null 2>&1; then
		skip_ok "$label" "cannot set polar_proxy_wait_timeout_ms through DCS"
		return
	fi
	if ! enable_replay_lag 50000 >/dev/null 2>&1; then
		skip_ok "$label" "cannot enable replica replay lag"
		return
	fi

	wait_retry_before=$(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer")
	term_before=$(policy_counter "PolarDB_Reader_Terminations")
	split_retry_before=$(policy_counter "PolarDB_Split_Reads_Retried")
	split_forward_before=$(policy_counter "PolarDB_Split_Reads_Forwarded")
	trace_action_before=$(policy_trace_count "PolarDB WAIT: policy kind=wait_timeout action=$timeout_action")
	trace_retry_before=$(policy_trace_count "PolarDB WAIT: strict wait timeout before user result; redirecting original")
	trace_forward_before=$(policy_trace_count "PolarDB WAIT: policy action=forward; normal error path will forward clean reader error")
	trace_term_before=$(policy_trace_count "PolarDB WAIT: terminating session after reader failure")

	start_wal_generator 0.01 1000
	out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES ($row_id, 'regular_wait') ON CONFLICT (id) DO UPDATE SET data='regular_wait';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;" 2>&1)
	rc=$?
	stop_wal_generator
	disable_replay_lag >/dev/null 2>&1 || true
	wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

	wait_retry_delta=$(($(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer") - wait_retry_before))
	term_delta=$(($(policy_counter "PolarDB_Reader_Terminations") - term_before))
	split_retry_delta=$(($(policy_counter "PolarDB_Split_Reads_Retried") - split_retry_before))
	split_forward_delta=$(($(policy_counter "PolarDB_Split_Reads_Forwarded") - split_forward_before))
	action_trace_delta=$(policy_trace_delta "PolarDB WAIT: policy kind=wait_timeout action=$timeout_action" "$trace_action_before")
	retry_trace_delta=$(policy_trace_delta "PolarDB WAIT: strict wait timeout before user result; redirecting original" "$trace_retry_before")
	forward_trace_delta=$(policy_trace_delta "PolarDB WAIT: policy action=forward; normal error path will forward clean reader error" "$trace_forward_before")
	term_trace_delta=$(policy_trace_delta "PolarDB WAIT: terminating session after reader failure" "$trace_term_before")

	local pass=1
	case "$timeout_action" in
	retry)
		[ "$rc" -eq 0 ] &&
			[ "$wait_retry_delta" -ge 1 ] &&
			[ "$term_delta" -eq 0 ] &&
			[ "$split_retry_delta" -eq 0 ] &&
			[ "$split_forward_delta" -eq 0 ] &&
			[ "$retry_trace_delta" -ge 1 ] &&
			printf '%s\n' "$out" | grep -qx "1" &&
			! printf '%s\n' "$out" | grep -q "ERROR" &&
			pass=0
		;;
	forward)
		[ "$wait_retry_delta" -eq 0 ] &&
			[ "$term_delta" -eq 0 ] &&
			[ "$split_retry_delta" -eq 0 ] &&
			[ "$split_forward_delta" -eq 0 ] &&
			[ "$forward_trace_delta" -ge 1 ] &&
			printf '%s\n' "$out" | grep -q "ERROR" &&
			pass=0
		;;
	terminate)
		[ "$wait_retry_delta" -eq 0 ] &&
			[ "$term_delta" -ge 1 ] &&
			[ "$split_retry_delta" -eq 0 ] &&
			[ "$term_trace_delta" -ge 1 ] &&
			{ [ "$rc" -ne 0 ] ||
				printf '%s\n' "$out" | grep -Eqi "server closed|terminat|connection"; } &&
			pass=0
		;;
	esac
	[ "$action_trace_delta" -ge 1 ] || pass=1

	if [ "$pass" -eq 0 ]; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc wait_retry_delta=$wait_retry_delta term_delta=$term_delta split_retry_delta=$split_retry_delta split_forward_delta=$split_forward_delta"
		diag "trace action=$action_trace_delta retry=$retry_trace_delta forward=$forward_trace_delta terminate=$term_trace_delta"
		ok 1 "$label"
	fi
}

run_session_lsn_best_effort_skips_failure_policy() {
	local label="session LSN best_effort stale warning skips failure policy"
	local row_id=44
	local out rc
	local wait_timeout_before term_before split_forward_before
	local wait_timeout_delta term_delta split_forward_delta

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy terminate terminate terminate 0 || {
		ok 1 "$label"
		return
	}
	proxysql_admin "UPDATE global_variables SET variable_value='best_effort' WHERE variable_name='pgsql-polardb_wait_timeout_mode';" >/dev/null
	proxysql_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0, lsn_wait_timeout_ms=100, consistency_mode='lsn', proxy_protocol='v15' WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	if ! set_polar_proxy_wait_timeout_ms 10 >/dev/null 2>&1; then
		skip_ok "$label" "cannot set polar_proxy_wait_timeout_ms through DCS"
		return
	fi
	if ! enable_replay_lag 50000 >/dev/null 2>&1; then
		skip_ok "$label" "cannot enable replica replay lag"
		return
	fi

	wait_timeout_before=$(policy_counter "PolarDB_Wait_Error_Timeout")
	term_before=$(policy_counter "PolarDB_Reader_Terminations")
	split_forward_before=$(policy_counter "PolarDB_Split_Reads_Forwarded")

	start_wal_generator 0.01 1000
	out=$(proxy_script "INSERT INTO $TEST_TABLE VALUES ($row_id, 'regular_wait_best_effort') ON CONFLICT (id) DO UPDATE SET data='regular_wait_best_effort';
\\! sleep 0.5
SELECT COUNT(*) FROM $TEST_TABLE WHERE id = $row_id;" 2>&1)
	rc=$?
	stop_wal_generator
	disable_replay_lag >/dev/null 2>&1 || true
	wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true

	wait_timeout_delta=$(($(policy_counter "PolarDB_Wait_Error_Timeout") - wait_timeout_before))
	term_delta=$(($(policy_counter "PolarDB_Reader_Terminations") - term_before))
	split_forward_delta=$(($(policy_counter "PolarDB_Split_Reads_Forwarded") - split_forward_before))

	if [ "$rc" -eq 0 ] &&
		[ "$wait_timeout_delta" -ge 1 ] &&
		[ "$term_delta" -eq 0 ] &&
		[ "$split_forward_delta" -eq 0 ] &&
		printf '%s\n' "$out" | grep -q "WARNING" &&
		! printf '%s\n' "$out" | grep -q "ERROR"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc wait_timeout_delta=$wait_timeout_delta term_delta=$term_delta split_forward_delta=$split_forward_delta"
		ok 1 "$label"
	fi
}

restore_offline_reader() {
	if [ -z "$OFFLINE_READER_HOST" ] || [ -z "$OFFLINE_READER_PORT" ]; then
		return 0
	fi
	proxysql_admin "UPDATE pgsql_servers SET status='ONLINE' WHERE hostgroup_id=$READER_HG AND hostname='$OFFLINE_READER_HOST' AND port=$OFFLINE_READER_PORT;" >/dev/null 2>&1 || true
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null 2>&1 || true
	OFFLINE_READER_HOST=""
	OFFLINE_READER_PORT=""
}

wait_for_active_reader_endpoint() {
	local endpoint=""
	local attempt
	for attempt in $(seq 1 100); do
		endpoint=$(proxysql_admin "SELECT srv_host || '|' || srv_port FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG AND ConnUsed > 0 ORDER BY ConnUsed DESC LIMIT 1;" 2>/dev/null | tr -d '[:space:]')
		if [ -n "$endpoint" ]; then
			printf '%s\n' "$endpoint"
			return 0
		fi
		sleep 0.05
	done
	return 1
}

wait_for_offline_reader_unused() {
	local host="$1"
	local port="$2"
	local remaining=""
	local attempt
	for attempt in $(seq 1 100); do
		remaining=$(proxysql_admin "SELECT COALESCE(SUM(ConnUsed), 0) FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG AND srv_host='$host' AND srv_port=$port;" 2>/dev/null | tr -d '[:space:]')
		if [ "${remaining:-0}" -eq 0 ]; then
			printf '0\n'
			return 0
		fi
		sleep 0.05
	done
	printf '%s\n' "${remaining:-0}"
	return 1
}

run_offline_hard_active_reader_case() {
	local label="OFFLINE_HARD during active transaction reader retries without client error"
	local out_file="$PROXYSQL_DATA_DIR/offline_hard_active_reader.out"
	local endpoint=""
	local query_pid query_rc=0 proxy_alive=0 reader_unused=0 reader_online=0
	local offline_status="" offline_not_online=0
	local queries_before=0 queries_after=0 avoided_offline_reader=0
	local retry_before retry_after retry_delta
	local missing_before missing_after missing_delta
	local output=""

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy retry retry forward 0 2 || {
		ok 1 "$label"
		return
	}
	proxysql_admin "UPDATE pgsql_replication_hostgroups SET lsn_wait_timeout_ms=5000 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
	warmup_replica_pool "$READER_HG" 10 >/dev/null 2>&1 || true

	retry_before=$(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer")
	missing_before=$(policy_counter "PolarDB_Wait_Retry_Declined_Original_Query_Missing")
	rm -f "$out_file"
	proxy_script "\\set ON_ERROR_STOP on
BEGIN;
SELECT 1 FROM pg_sleep(5);
COMMIT;" >"$out_file" 2>&1 &
	query_pid=$!

	if ! endpoint=$(wait_for_active_reader_endpoint); then
		wait "$query_pid" || query_rc=$?
		output=$(cat "$out_file" 2>/dev/null || true)
		diag "$label: no active reader was observed; rc=$query_rc output=$output"
		ok 1 "$label"
		return
	fi
	IFS='|' read -r OFFLINE_READER_HOST OFFLINE_READER_PORT <<<"$endpoint"
	proxysql_admin "UPDATE pgsql_servers SET status='OFFLINE_HARD' WHERE hostgroup_id=$READER_HG AND hostname='$OFFLINE_READER_HOST' AND port=$OFFLINE_READER_PORT;" >/dev/null
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null

	wait "$query_pid" || query_rc=$?
	output=$(cat "$out_file" 2>/dev/null || true)
	proxysql_admin "SELECT 1;" >/dev/null 2>&1 && proxy_alive=1
	if [ "$(wait_for_offline_reader_unused "$OFFLINE_READER_HOST" "$OFFLINE_READER_PORT")" -eq 0 ]; then
		reader_unused=1
	fi
	offline_status=$(proxysql_admin "SELECT status FROM runtime_pgsql_servers WHERE hostgroup_id=$READER_HG AND hostname='$OFFLINE_READER_HOST' AND port=$OFFLINE_READER_PORT;" 2>/dev/null | tr -d '[:space:]')
	if [ -z "$offline_status" ] || [ "$offline_status" = "OFFLINE_HARD" ]; then
		offline_not_online=1
	fi
	queries_before=$(proxysql_admin "SELECT COALESCE(SUM(Queries), 0) FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG AND srv_host='$OFFLINE_READER_HOST' AND srv_port=$OFFLINE_READER_PORT;" 2>/dev/null | tr -d '[:space:]')
	query_proxy "SELECT 1;" >/dev/null 2>&1 || true
	queries_after=$(proxysql_admin "SELECT COALESCE(SUM(Queries), 0) FROM stats_pgsql_connection_pool WHERE hostgroup=$READER_HG AND srv_host='$OFFLINE_READER_HOST' AND srv_port=$OFFLINE_READER_PORT;" 2>/dev/null | tr -d '[:space:]')
	[ "${queries_after:-0}" -eq "${queries_before:-0}" ] && avoided_offline_reader=1
	restore_offline_reader
	reader_online=$(proxysql_admin "SELECT COUNT(*) FROM runtime_pgsql_servers WHERE hostgroup_id=$READER_HG AND status='ONLINE';" 2>/dev/null | tr -d '[:space:]')
	retry_after=$(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer")
	missing_after=$(policy_counter "PolarDB_Wait_Retry_Declined_Original_Query_Missing")
	retry_delta=$((retry_after - retry_before))
	missing_delta=$((missing_after - missing_before))

	if [ "$query_rc" -eq 0 ] && [ "$proxy_alive" -eq 1 ] &&
			[ "$reader_unused" -eq 1 ] && [ "$offline_not_online" -eq 1 ] &&
			[ "$avoided_offline_reader" -eq 1 ] && [ "${reader_online:-0}" -ge 1 ] &&
			[ "$retry_delta" -ge 1 ] && [ "$missing_delta" -eq 0 ] &&
			printf '%s\n' "$output" | grep -qx "1" &&
			! printf '%s\n' "$output" | grep -Eqi "ERROR|server closed|connection failed"; then
		ok 0 "$label"
	else
		diag "$label output: $output"
		diag "rc=$query_rc proxy_alive=$proxy_alive reader_unused=$reader_unused offline_status=$offline_status avoided_offline_reader=$avoided_offline_reader reader_online=${reader_online:-0} retry_delta=$retry_delta missing_query_delta=$missing_delta endpoint=$endpoint"
		ok 1 "$label"
	fi
	rm -f "$out_file"
}

run_injected_post_send_offline_case() {
	local label="post-send offline injection retries transaction reader without client error"
	local out rc
	local proxy_alive=0
	local retry_before retry_after retry_delta
	local missing_before missing_after missing_delta

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy retry retry forward 0 2 || {
		ok 1 "$label"
		return
	}
	warmup_replica_pool "$READER_HG" 10 >/dev/null 2>&1 || true
	retry_before=$(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer")
	missing_before=$(policy_counter "PolarDB_Wait_Retry_Declined_Original_Query_Missing")

	out=$(proxy_script "\\set ON_ERROR_STOP on
BEGIN;
\\! printf '%s\\n' offline_no_error > \"\$POLARDB_DEBUG_POST_SEND_OFFLINE_FILE\"
SELECT 1;
COMMIT;" 2>&1)
	rc=$?
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null 2>&1 || true
	proxysql_admin "SELECT 1;" >/dev/null 2>&1 && proxy_alive=1
	retry_after=$(policy_counter "PolarDB_Wait_Reads_Retried_On_Writer")
	missing_after=$(policy_counter "PolarDB_Wait_Retry_Declined_Original_Query_Missing")
	retry_delta=$((retry_after - retry_before))
	missing_delta=$((missing_after - missing_before))

	if [ "$rc" -eq 0 ] && [ "$proxy_alive" -eq 1 ] &&
			[ "$retry_delta" -ge 1 ] && [ "$missing_delta" -eq 0 ] &&
			printf '%s\n' "$out" | grep -qx "1" &&
			! printf '%s\n' "$out" | grep -Eqi "ERROR|server closed|connection failed"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc proxy_alive=$proxy_alive retry_delta=$retry_delta missing_query_delta=$missing_delta"
		ok 1 "$label"
	fi
}

cleanup() {
	restore_offline_reader
	stop_wal_generator >/dev/null 2>&1 || true
	disable_replay_lag >/dev/null 2>&1 || true
	stop_proxysql >/dev/null 2>&1 || true
}
trap cleanup EXIT

PLAN="$(policy_case_plan)"
plan "$PLAN"
if [ -n "${TXN_SPLIT_FAILURE_POLICY_TAP_CASES:-}" ]; then
	diag "selected split failure policy TAP cases: $TXN_SPLIT_FAILURE_POLICY_TAP_CASES"
fi
diag "test profile: POLARDB_TEST_ENV=$POLARDB_TEST_ENV POLARDB_DCS_MODE=$POLARDB_DCS_MODE writer=${PRIMARY_HOST:-auto}:${PRIMARY_PORT:-auto} reader=${REPLICA_HOST:-auto}:${REPLICA_PORT:-auto}"
polardb_require_proxysql_or_skip_all polardb-debug
polardb_require_command_or_skip_all psql psql

topology_error_file="$(mktemp "${TMPDIR:-/tmp}/polardb-topology.XXXXXX")"
if polardb_detect_topology 2>"$topology_error_file"; then
	rm -f "$topology_error_file"
	ok 0 "detect PolarDB primary/replica topology"
	diag "topology selected: primary=$PRIMARY_HOST:$PRIMARY_PORT replica=$REPLICA_HOST:$REPLICA_PORT"
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

export PROXYSQL_DEBUG=1
if start_proxysql >/dev/null 2>&1; then
	ok 0 "start debug ProxySQL for split failure policy"
else
	ok 1 "start debug ProxySQL for split failure policy"
	polardb_skip_remaining "ProxySQL failed to start" "$PLAN"
	exit "$FAIL"
fi
write_split_prewarm_wait_helper
write_split_txn_wait_helper

if debug_fault_file_ready POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE "$PROXYSQL_DATA_DIR/proxysql_strings"; then
	ok 0 "debug reader-failure fault support is available"
else
	skip_ok "debug reader-failure fault support is available" "requires POLARDB_DEBUG split failure fault support"
	polardb_skip_remaining "POLARDB_DEBUG split failure fault support missing" "$PLAN"
	exit 0
fi

run_selected_policy_case 4 "split timeout policy retry redispatches on writer" \
	31 "-" retry retry forward retry 0
run_selected_policy_case 5 "split timeout policy forward keeps transaction on writer and overrides manual reader route" \
	32 "-" forward retry forward forward 1
run_selected_policy_case 6 "split timeout policy terminate closes the client session" \
	33 "-" terminate retry forward terminate 0 \
	"PolarDB READER_POOL: destroy used"
run_selected_policy_case 7 "split reader death retry tries another reader before writer fallback" \
	30 death retry retry forward retry_any 0 \
	"PolarDB FAILURE: policy kind=connection_lost action=retry target=other_reader"
run_selected_policy_case 8 "split reader retry limit falls back to writer after second death" \
	45 death_twice retry retry forward retry_reader_then_writer 0 \
	"PolarDB FAILURE: policy kind=connection_lost action=retry target=writer"
run_selected_policy_case 9 "split reader death uses death-action policy" \
	34 death retry forward terminate forward 0 \
	"PolarDB FAILURE: policy kind=connection_lost action=forward target=writer"
run_selected_policy_case 10 "split reusable SQL error uses error-action policy" \
	35 sql_error retry retry terminate terminate 0 \
	"PolarDB FAILURE: policy kind=reusable_error action=terminate target=writer" \
	"PolarDB_Reader_Pool_Data_Stream_Active_Transaction_Destroy"
run_selected_policy_case 11 "split retry without retry packet forwards clean reader error" \
	36 no_retry_packet retry retry forward forward 0 \
	"PolarDB FAILURE: retry declined reason=no_retry_packet"
run_selected_policy_case 12 "split retry with busy writer forwards clean reader error" \
	37 writer_busy retry retry forward forward 0 \
	"PolarDB FAILURE: retry declined reason=writer_busy debug=1"
run_selected_policy_case 13 "split retry after user rows started forwards instead of redispatching" \
	38 result_started retry retry forward forward 0 \
	"PolarDB FAILURE: forwarding reader error after retry declined (action=retry target=writer writer_state=live result_started=1)"
run_selected_policy_case 14 "split retry with writer not started forwards and keeps writer hostgroup" \
	39 writer_not_started retry retry forward forward 0 \
	"PolarDB FAILURE: writer_state=not_started"
run_selected_policy_case 15 "split writer-state loss terminates before policy action" \
	40 writer_lost retry retry forward terminate 0 \
	"PolarDB FAILURE: terminating because writer transaction state is lost"
run_selected_session_lsn_timeout_policy_case 16 \
	"session LSN strict timeout policy retry redispatches on writer" \
	41 retry
run_selected_session_lsn_timeout_policy_case 17 \
	"session LSN strict timeout policy forward returns clean error" \
	42 forward
run_selected_session_lsn_timeout_policy_case 18 \
	"session LSN strict timeout policy terminate closes the client session" \
	43 terminate
run_selected_session_lsn_best_effort_skips_failure_policy 19
if policy_case_selected 20; then
	run_injected_post_send_offline_case
fi
if policy_case_selected 21; then
	run_offline_hard_active_reader_case
fi

set_polar_proxy_wait_timeout_ms 5000 >/dev/null 2>&1 || true
diag "reader-failure policy TAP completed: failed=$FAIL"
exit "$FAIL"
