#!/usr/bin/env bash
# Focused TAP integration coverage for backend connect-deadline ownership.
#
# Each case drives one real retry transition with an expired source and target
# deadline. The DEBUG-only fault is inserted immediately before the transition;
# normal routing, connection ownership and protocol handling remain unchanged.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_connect_deadline_retry_tap")}"
export PROXYSQL_DATA_DIR
export TXN_SPLIT_FAILURE_POLICY_TAP_LIBRARY_ONLY=1

# Reuse the established split-failure fixture without running its case matrix.
# shellcheck source=txn_split_failure_policy_tap.sh
source "$SCRIPT_DIR/txn_split_failure_policy_tap.sh"
unset TXN_SPLIT_FAILURE_POLICY_TAP_LIBRARY_ONLY

PLAN=10
FAIL=0
REPAIR_CLEAR_TRACE="PolarDB CONNECT_DEADLINE DEBUG: retry repaired source_cleared=1 target_ready=1 target_deadline=clear"
REPAIR_ARMED_TRACE="PolarDB CONNECT_DEADLINE DEBUG: retry repaired source_cleared=1 target_ready=0 target_deadline=armed"
READY_ACCEPT_TRACE="PolarDB CONNECT_DEADLINE DEBUG: accepted ready backend before expired deadline"

runtime_online_reader_count() {
	proxysql_admin "SELECT COUNT(*) FROM runtime_pgsql_servers WHERE hostgroup_id=$READER_HG AND status='ONLINE';" \
		2>/dev/null | tr -d '[:space:]'
}

restore_runtime_readers() {
	local expected

	expected=$(proxysql_admin \
		"SELECT COUNT(*) FROM pgsql_servers WHERE hostgroup_id=$READER_HG;" \
		2>/dev/null | tr -d '[:space:]')
	[ "${expected:-0}" -gt 0 ] || return 1
	proxysql_admin "UPDATE pgsql_servers SET status='OFFLINE_HARD' WHERE hostgroup_id=$READER_HG;" >/dev/null || return 1
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null || return 1
	proxysql_admin "UPDATE pgsql_servers SET status='ONLINE' WHERE hostgroup_id=$READER_HG;" >/dev/null || return 1
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null || return 1

	local _attempt
	for _attempt in $(seq 1 50); do
		[ "$(runtime_online_reader_count)" -eq "$expected" ] && return 0
		sleep 0.1
	done
	return 1
}

keep_one_runtime_reader() {
	local endpoint host port

	endpoint=$(proxysql_admin \
		"SELECT hostname || '|' || port FROM pgsql_servers WHERE hostgroup_id=$READER_HG ORDER BY hostname, port LIMIT 1;" \
		2>/dev/null | tr -d '[:space:]')
	[ -n "$endpoint" ] || return 1
	host="${endpoint%|*}"
	port="${endpoint##*|}"
	proxysql_admin \
		"UPDATE pgsql_servers SET status=CASE WHEN hostname='$host' AND port=$port THEN 'ONLINE' ELSE 'OFFLINE_HARD' END WHERE hostgroup_id=$READER_HG;" \
		>/dev/null || return 1
	proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null || return 1
	[ "$(runtime_online_reader_count)" -eq 1 ]
}

set_deadline_fault() {
	set_debug_fault_file POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE "$1"
}

deadline_trace_delta() {
	local pattern="$1"
	local before="$2"
	policy_trace_delta "$pattern" "$before"
}

disable_transaction_split() {
	proxysql_admin "UPDATE pgsql_replication_hostgroups SET txn_split_enabled=0 WHERE writer_hostgroup=$WRITER_HG;" >/dev/null &&
		proxysql_admin "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
}

run_split_loss_deadline_case() {
	local label="$1"
	local loss_action="$2"
	local expected_node="$3"
	local deadline_fault="$4"
	local expected_deadline_trace="$5"
	local require_peer="$6"
	local no_peer="${7:-0}"
	local row_id=$((201 + require_peer))
	local out rc deadline_before deadline_delta
	local prepared_before prepared_delta writer_retry_before writer_retry_delta
	local reader_retry_before reader_retry_delta
	local max_connect_before max_connect_delta node_ok
	local warmup_created_before warmup_already_before txn_became_before

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy primary "$loss_action" error 0 2 || {
		ok 1 "$label"
		return
	}
	if [ "$require_peer" = "1" ]; then
		if [ "$(runtime_online_reader_count)" -lt 2 ]; then
			skip_ok "$label" "requires two online reader endpoints"
			return
		fi
		if ! prepare_two_reader_retry_connections; then
			diag "$label: could not prepare exact pooled connections on two readers"
			ok 1 "$label"
			return
		fi
	else
		warmup_replica_pool "$READER_HG" 10 >/dev/null 2>&1 || true
	fi
	if ! set_polar_proxy_wait_timeout_ms 10 >/dev/null 2>&1; then
		skip_ok "$label" "cannot set polar_proxy_wait_timeout_ms through DCS"
		return
	fi
	if ! enable_replay_lag 50000 >/dev/null 2>&1; then
		skip_ok "$label" "cannot enable replica replay lag"
		return
	fi
	if [ "$no_peer" = "1" ] && ! keep_one_runtime_reader; then
		ok 1 "$label"
		return
	fi

	deadline_before=$(policy_trace_count "$expected_deadline_trace")
	prepared_before=$(policy_trace_count "PolarDB TXN_SPLIT: prepared")
	writer_retry_before=$(policy_counter "PolarDB_Split_Reads_Retried")
	reader_retry_before=$(policy_counter "PolarDB_Split_Reads_Retried_On_Reader")
	max_connect_before=$(policy_counter "max_connect_timeouts")
	warmup_created_before=$(policy_counter "PolarDB_Split_Warmup_Created")
	warmup_already_before=$(policy_counter "PolarDB_Split_Warmup_Already_Warm")
	txn_became_before=$(policy_counter "PolarDB_Txn_Became_Splittable")
	start_wal_generator 0.01 1000
	out=$(run_split_failure_sql "$row_id" 0 death \
		"$warmup_created_before" "$warmup_already_before" \
		"$txn_became_before" "-" "$deadline_fault" "-" \
		"SELECT polar_node_type();" 2>&1)
	rc=$?
	clear_debug_fault_file POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE >/dev/null 2>&1 || true
	stop_wal_generator
	disable_replay_lag >/dev/null 2>&1 || true
	wait_for_replica_lsn_catchup 15 >/dev/null 2>&1 || true
	deadline_delta=$(deadline_trace_delta \
		"$expected_deadline_trace" "$deadline_before")
	prepared_delta=$(deadline_trace_delta \
		"PolarDB TXN_SPLIT: prepared" "$prepared_before")
	writer_retry_delta=$(($(policy_counter "PolarDB_Split_Reads_Retried") - writer_retry_before))
	reader_retry_delta=$(($(policy_counter "PolarDB_Split_Reads_Retried_On_Reader") - reader_retry_before))
	max_connect_delta=$(($(policy_counter "max_connect_timeouts") - max_connect_before))
	restore_runtime_readers >/dev/null 2>&1 || true
	clear_debug_fault_file POLARDB_DEBUG_POST_SEND_OFFLINE_FILE >/dev/null 2>&1 || true
	clear_debug_fault_file POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE >/dev/null 2>&1 || true

	local retry_ok=0
	if [ "$require_peer" = "1" ]; then
		[ "$reader_retry_delta" -ge 1 ] && retry_ok=1
	else
		[ "$writer_retry_delta" -ge 1 ] && [ "$reader_retry_delta" -eq 0 ] &&
			retry_ok=1
	fi
	node_ok=0
	if printf '%s\n' "$out" | grep -qx "$expected_node"; then
		node_ok=1
	elif [ "$require_peer" = "1" ] &&
			printf '%s\n' "$out" | grep -qx primary; then
		# The peer retry is the transition under test. Its later LSN wait may
		# still use the configured writer fallback.
		node_ok=1
	fi
	if [ "$rc" -eq 0 ] && [ "$node_ok" -eq 1 ] &&
			[ "$deadline_delta" -eq 1 ] && [ "$prepared_delta" -ge 1 ] &&
			[ "$retry_ok" -eq 1 ] && [ "$max_connect_delta" -eq 0 ] &&
			! printf '%s\n' "$out" | grep -Fq "Max connect timeout reached"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc node_ok=$node_ok deadline_delta=$deadline_delta prepared_delta=$prepared_delta writer_retry_delta=$writer_retry_delta reader_retry_delta=$reader_retry_delta max_connect_delta=$max_connect_delta"
		ok 1 "$label"
	fi
}

run_ordinary_loss_deadline_case() {
	local label="$1"
	local loss_action="$2"
	local expected_node="$3"
	local expected_repair_trace="$4"
	local require_peer="$5"
	local out rc repair_before repair_delta retry_before retry_delta

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy primary "$loss_action" error 0 2 || {
		ok 1 "$label"
		return
	}
	if [ "$require_peer" = "1" ]; then
		if [ "$(runtime_online_reader_count)" -lt 2 ]; then
			skip_ok "$label" "requires two online reader endpoints"
			return
		fi
		if ! prepare_two_reader_retry_connections; then
			diag "$label: could not prepare exact pooled connections on two readers"
			ok 1 "$label"
			return
		fi
	fi
	disable_transaction_split || {
		ok 1 "$label"
		return
	}

	repair_before=$(policy_trace_count "$expected_repair_trace")
	retry_before=$(policy_trace_count \
		"PolarDB FAILURE: reader failed before user result; retrying original query on another reader_hg")
	out=$(proxy_script "\\set ON_ERROR_STOP on
\\! printf '%s\\n' retry_expired > \"\$POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE\"
\\! printf '%s\\n' offline_no_error > \"\$POLARDB_DEBUG_POST_SEND_OFFLINE_FILE\"
SELECT polar_node_type();" 2>&1)
	rc=$?
	repair_delta=$(deadline_trace_delta "$expected_repair_trace" "$repair_before")
	retry_delta=$(deadline_trace_delta \
		"PolarDB FAILURE: reader failed before user result; retrying original query on another reader_hg" \
		"$retry_before")
	restore_runtime_readers >/dev/null 2>&1 || true
	clear_debug_fault_file POLARDB_DEBUG_POST_SEND_OFFLINE_FILE >/dev/null 2>&1 || true

	if [ "$rc" -eq 0 ] && printf '%s\n' "$out" | grep -qx "$expected_node" &&
			[ "$repair_delta" -eq 1 ] &&
			{ [ "$require_peer" = "0" ] || [ "$retry_delta" -ge 1 ]; } &&
			! printf '%s\n' "$out" | grep -Fq "Max connect timeout reached"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc repair_delta=$repair_delta reader_retry_delta=$retry_delta"
		ok 1 "$label"
	fi
}

run_capacity_redirect_deadline_case() {
	local label="capacity fallback replaces the reader deadline and arms the writer"
	local old_timeout out rc repair_before repair_delta redirect_before redirect_delta

	reset_case_table || {
		ok 1 "$label"
		return
	}
	configure_split_policy primary replica_then_primary error 0 1 || {
		ok 1 "$label"
		return
	}
	disable_transaction_split || {
		ok 1 "$label"
		return
	}
	old_timeout=$(proxysql_admin \
		"SELECT variable_value FROM runtime_global_variables WHERE variable_name='pgsql-connect_timeout_server_max';" \
		2>/dev/null | tr -d '[:space:]')
	proxysql_admin "UPDATE global_variables SET variable_value='50' WHERE variable_name='pgsql-connect_timeout_server_max';" >/dev/null
	proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null

	repair_before=$(policy_trace_count "$REPAIR_ARMED_TRACE")
	redirect_before=$(policy_trace_count "replica capacity deadline reached")
	out=$(proxy_script "\\set ON_ERROR_STOP on
INSERT INTO $TEST_TABLE VALUES (105, 'capacity_deadline') ON CONFLICT (id) DO UPDATE SET data='capacity_deadline';
\\! printf '%s\\n' reader_busy_until_deadline > \"\$POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE\"
\\! printf '%s\\n' retry_expired > \"\$POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE\"
SELECT polar_node_type();" 2>&1)
	rc=$?
	repair_delta=$(deadline_trace_delta "$REPAIR_ARMED_TRACE" "$repair_before")
	redirect_delta=$(deadline_trace_delta "replica capacity deadline reached" "$redirect_before")
	clear_debug_fault_file POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE >/dev/null 2>&1 || true
	if [ -n "$old_timeout" ]; then
		proxysql_admin "UPDATE global_variables SET variable_value='$old_timeout' WHERE variable_name='pgsql-connect_timeout_server_max';" >/dev/null
		proxysql_admin "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
	fi

	if [ "$rc" -eq 0 ] && printf '%s\n' "$out" | grep -qx primary &&
			[ "$repair_delta" -eq 1 ] && [ "$redirect_delta" -ge 1 ] &&
			! printf '%s\n' "$out" | grep -Fq "Max connect timeout reached"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc repair_delta=$repair_delta redirect_delta=$redirect_delta"
		ok 1 "$label"
	fi
}

run_ready_backend_deadline_case() {
	local label="CONNECTING_SERVER accepts an idle backend before its stale deadline"
	local out rc trace_before trace_delta

	configure_split_policy primary replica_then_primary error 0 1 >/dev/null || {
		ok 1 "$label"
		return
	}
	disable_transaction_split >/dev/null || {
		ok 1 "$label"
		return
	}
	query_proxy "SELECT 1;" >/dev/null 2>&1 || true
	trace_before=$(policy_trace_count "$READY_ACCEPT_TRACE")
	set_deadline_fault ready_expired
	out=$(query_proxy "SELECT 1;" 2>&1)
	rc=$?
	trace_delta=$(deadline_trace_delta "$READY_ACCEPT_TRACE" "$trace_before")

	if [ "$rc" -eq 0 ] && printf '%s\n' "$out" | grep -qx 1 &&
			[ "$trace_delta" -eq 1 ] &&
			! printf '%s\n' "$out" | grep -Fq "Max connect timeout reached"; then
		ok 0 "$label"
	else
		diag "$label output: $out"
		diag "rc=$rc ready_accept_trace_delta=$trace_delta"
		ok 1 "$label"
	fi
}

# shellcheck disable=SC2317
deadline_cleanup() {
	restore_runtime_readers >/dev/null 2>&1 || true
	cleanup
}
trap deadline_cleanup EXIT

plan "$PLAN"
diag "focused connect-deadline retry coverage"
polardb_require_proxysql_or_skip_all polardb-debug
polardb_require_command_or_skip_all psql psql

topology_error_file="$(mktemp "${TMPDIR:-/tmp}/polardb-topology.XXXXXX")"
if polardb_detect_topology 2>"$topology_error_file"; then
	rm -f "$topology_error_file"
	ok 0 "detect PolarDB primary/replica topology"
else
	diag "$(cat "$topology_error_file" 2>/dev/null || true)"
	rm -f "$topology_error_file"
	skip_ok "detect PolarDB primary/replica topology" "PolarDB topology unavailable"
	polardb_skip_remaining "PolarDB topology unavailable" "$PLAN"
	exit 0
fi

HARNESS_AUTODETECT="$POLARDB_AUTODETECT"
POLARDB_AUTODETECT=0
# shellcheck source=../lib/scenario_harness.sh
source "$SCRIPT_DIR/../lib/scenario_harness.sh"
POLARDB_AUTODETECT="$HARNESS_AUTODETECT"
# Used by cleanup() imported from txn_split_failure_policy_tap.sh.
# shellcheck disable=SC2034
ORIGINAL_POLAR_PROXY_WAIT_TIMEOUT_MS="$(get_polar_proxy_wait_timeout_ms || true)"

export PROXYSQL_DEBUG=1
if start_proxysql >/dev/null 2>&1; then
	ok 0 "start debug ProxySQL for connect-deadline cases"
else
	ok 1 "start debug ProxySQL for connect-deadline cases"
	polardb_skip_remaining "ProxySQL failed to start" "$PLAN"
	exit "$FAIL"
fi
write_split_prewarm_wait_helper
write_split_txn_wait_helper

if debug_fault_file_ready \
		POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE \
		"$PROXYSQL_DATA_DIR/proxysql_strings"; then
	ok 0 "connect-deadline fault support is available"
else
	skip_ok "connect-deadline fault support is available" \
		"requires the focused POLARDB_DEBUG fault hook"
	polardb_skip_remaining "connect-deadline fault support missing" "$PLAN"
	exit 0
fi

if debug_fault_file_ready POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE \
		"$PROXYSQL_DATA_DIR/proxysql_strings" &&
		debug_fault_file_ready POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE \
		"$PROXYSQL_DATA_DIR/proxysql_strings" &&
		debug_fault_file_ready POLARDB_DEBUG_POST_SEND_OFFLINE_FILE \
		"$PROXYSQL_DATA_DIR/proxysql_strings"; then
	ok 0 "retry-path fault support is available"
else
	skip_ok "retry-path fault support is available" \
		"requires split, reader-acquire and post-send POLARDB_DEBUG hooks"
	polardb_skip_remaining "retry-path fault support missing" "$PLAN"
	exit 0
fi

run_split_loss_deadline_case \
	"split peer miss preserves retry state for the ready writer" \
	replica_then_primary primary retry_expired "$REPAIR_CLEAR_TRACE" 0 1

run_split_loss_deadline_case \
	"split reader-to-reader retry clears expired deadlines on both ready streams" \
	replica_then_error replica retry_expired "$REPAIR_CLEAR_TRACE" 1

run_ordinary_loss_deadline_case \
	"ordinary reader-to-reader retry clears the failed stream deadline" \
	replica_then_error replica "$REPAIR_CLEAR_TRACE" 1

run_ordinary_loss_deadline_case \
	"ordinary reader-to-writer retry arms a fresh deadline on an unconnected writer" \
	primary primary "$REPAIR_ARMED_TRACE" 0

run_capacity_redirect_deadline_case
run_ready_backend_deadline_case

if [ "$FAIL" -eq 0 ]; then
	diag "all connect-deadline retry cases passed"
else
	diag "$FAIL connect-deadline retry cases failed"
fi
exit "$FAIL"
