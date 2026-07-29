/**
 * @file polardb_status_policy_unit-t.cpp
 * @brief Unit tests for the PolarDB status-name and policy helpers.
 *
 * Domain: route-action-reason names and NoticeResponse packet helpers,
 * reader-status names and writer-redirect policy, wrapper-error accounting
 * policy, and server-LSN cache reset.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

#include <cstring>

template <typename Value>
struct PolarDB_NameCase {
	Value value;
	const char* expected;
};

template <typename Value, size_t Count>
static void check_name_cases(
		const PolarDB_NameCase<Value> (&cases)[Count],
		const char* (*name_function)(Value),
		const char* category) {
	for (const PolarDB_NameCase<Value>& test_case : cases) {
		ok(strcmp(name_function(test_case.value), test_case.expected) == 0,
			"%s helper names %s", category, test_case.expected);
	}
}

// ---- route-action-reason names & NoticeResponse packet helpers ----

static void test_degraded_rfq_notice_packet_helpers() {
	using Reason = PolarDB_Query_RoutePlan::RouteActionReason;
	const PolarDB_NameCase<Reason> reason_cases[] = {
		{Reason::NONE, "none"},
		{Reason::EXTENDED_PROTOCOL, "extended_protocol"},
		{Reason::IN_TRANSACTION, "in_transaction"},
		{Reason::MULTI_STATEMENT, "multi_statement"},
		{Reason::READ_TARGET_PRIMARY, "read_target_primary"},
		{Reason::HINT_PRIMARY, "hint_primary"},
		{Reason::WRITE_LSN_UNKNOWN, "write_lsn_unknown"},
		{Reason::OBSERVED_LSN_UNKNOWN, "observed_lsn_unknown"},
		{Reason::GROUP_LSN_UNKNOWN, "group_lsn_unknown"},
		{Reason::INVALID_POLICY, "invalid_policy"},
		{Reason::READER_RFQ_UNAVAILABLE, "reader_rfq_unavailable"},
		{Reason::READ_FALLBACK_ERROR, "read_fallback_error"},
		{Reason::READER_FAILURE_FORCE_WRITER, "reader_failure_force_writer"},
		{Reason::WAL_PENDING, "wal_pending"},
		{Reason::SPLIT_BLOCKED, "split_blocked"},
		{Reason::SPLIT_WRITE_LSN_UNKNOWN, "split_write_lsn_unknown"},
		{Reason::SPLIT_OBSERVED_LSN_UNKNOWN, "split_observed_lsn_unknown"},
		{Reason::NO_TXN_LSN, "no_txn_lsn"},
		{Reason::INVARIANT_VIOLATION, "invariant_violation"},
		{Reason::HG_SPLIT_DISABLED, "hg_split_disabled"},
	};
	check_name_cases(reason_cases, polardb_route_action_reason_name, "action reason");

	const char* severity = "WARNING";
	const char* severity_nonlocalized = "WARNING";
	const char* sqlstate = "01000";
	const char* message = "PolarDB reader route has no enforceable LSN wait target; read may be stale";
	const char* detail = "reason=write_lsn_unknown reader_hg=20 writer_hg=10";
	const unsigned int size = polardb_notice_response_packet_size(
		severity, sqlstate, message, detail, severity_nonlocalized);
	unsigned char pkt[256] = {0};
	const unsigned int written = polardb_write_notice_response_packet(
		pkt, sizeof(pkt), severity, sqlstate, message, detail,
		severity_nonlocalized);
	ok(written == size, "NoticeResponse helper writes the computed packet size");
	ok(pkt[0] == 'N', "NoticeResponse helper writes PostgreSQL notice type");
	ok(notice_packet_has_field(pkt, written, 'S', severity),
		"NoticeResponse helper includes severity");
	ok(notice_packet_has_field(pkt, written, 'V', severity_nonlocalized),
		"NoticeResponse helper includes non-localized severity");
	ok(notice_packet_has_field(pkt, written, 'C', sqlstate),
		"NoticeResponse helper includes SQLSTATE");
	ok(notice_packet_has_field(pkt, written, 'M', message),
		"NoticeResponse helper includes message");
	ok(notice_packet_has_field(pkt, written, 'D', detail),
		"NoticeResponse helper includes detail");
	ok(polardb_write_notice_response_packet(
		pkt, 4, severity, sqlstate, message, detail) == 0,
		"NoticeResponse helper rejects undersized output buffers");
}

static void test_failure_action_names() {
	const PolarDB_NameCase<PolarDB_FailureAction> cases[] = {
		{PolarDB_FailureAction::PASSTHROUGH, "passthrough"},
		{PolarDB_FailureAction::RETRY, "retry"},
		{PolarDB_FailureAction::FORWARD, "forward"},
		{PolarDB_FailureAction::TERMINATE, "terminate"},
	};
	check_name_cases(cases, polardb_failure_action_name, "failure action");
}

static void test_reader_action_policy_mapping() {
	const PolarDB_NameCase<PolarDB_ReaderAction> action_cases[] = {
		{PolarDB_ReaderAction::RETRY, "retry"},
		{PolarDB_ReaderAction::RETURN_ERROR, "error"},
		{PolarDB_ReaderAction::DISCONNECT_CLIENT, "disconnect"},
	};
	check_name_cases(action_cases, polardb_reader_action_name, "reader action");

	const PolarDB_NameCase<PolarDB_ReaderFailureKind> failure_cases[] = {
		{PolarDB_ReaderFailureKind::CONNECTION_LOST, "connection_lost"},
		{PolarDB_ReaderFailureKind::WAIT_TIMEOUT, "wait_timeout"},
		{PolarDB_ReaderFailureKind::REUSABLE_ERROR, "reusable_error"},
	};
	check_name_cases(
		failure_cases, polardb_reader_failure_kind_name, "reader failure kind");

	const PolarDB_NameCase<PolarDB_RetryTarget> target_cases[] = {
		{PolarDB_RetryTarget::WRITER, "writer"},
		{PolarDB_RetryTarget::OTHER_READER, "other_reader"},
	};
	check_name_cases(target_cases, polardb_retry_target_name, "retry target");

	const PolarDB_NameCase<PolarDB_ReaderFailureRoute> route_cases[] = {
		{PolarDB_ReaderFailureRoute::NONE, "none"},
		{PolarDB_ReaderFailureRoute::FORCE_WRITER, "force_writer"},
		{PolarDB_ReaderFailureRoute::SKIP_READER, "skip_reader"},
	};
	check_name_cases(
		route_cases, polardb_reader_failure_route_name, "reader-failure route");
}

// ---- reader-status names ----

static void test_reader_status_names() {
	const PolarDB_NameCase<PolarDB_ReaderStatus> cases[] = {
		{PolarDB_ReaderStatus::ACQUIRED, "acquired"},
		{PolarDB_ReaderStatus::READER_UNAVAILABLE, "reader_unavailable"},
		{PolarDB_ReaderStatus::READER_BUSY, "reader_busy"},
		{PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE,
			"retry_after_config_change"},
		{PolarDB_ReaderStatus::RFQ_UNAVAILABLE, "rfq_unavailable"},
		{PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN, "group_lsn_unknown"},
		{PolarDB_ReaderStatus::READER_LSN_UNKNOWN, "reader_lsn_unknown"},
		{PolarDB_ReaderStatus::READER_LSN_STALE, "reader_lsn_stale"},
		{PolarDB_ReaderStatus::READER_LAG_EXCEEDED, "reader_lag_exceeded"},
	};
	check_name_cases(cases, polardb_reader_status_name, "reader status");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_UNAVAILABLE),
		"reader unavailable uses normal no-connection handling");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_BUSY),
		"reader busy uses normal no-connection handling");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE),
		"configuration-change retry uses normal no-connection handling");
	ok(polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN),
		"unknown group LSN redirects this consistency read to writer");
	ok(polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_LSN_UNKNOWN),
		"unknown reader LSN redirects this consistency read to writer");
	ok(polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_LSN_STALE),
		"stale reader LSN redirects this consistency read to writer");
	ok(polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_LAG_EXCEEDED),
		"reader lag cap excess redirects this consistency read to writer");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::ACQUIRED),
		"split warmup is not requested for an already acquired reader");
	ok(polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::READER_UNAVAILABLE),
		"split warmup can help when no usable reader backend is available");
	ok(polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::READER_BUSY),
		"split warmup can help when readers have no available pooled match");
	ok(polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::RFQ_UNAVAILABLE),
		"split warmup can help when no RFQ-LSN-capable reader backend is available");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE),
		"split warmup is not requested for configuration churn");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN),
		"split warmup cannot fix a missing group LSN sample");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::READER_LSN_UNKNOWN),
		"split warmup cannot fix a missing reader LSN sample");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::READER_LSN_STALE),
		"split warmup cannot fix a stale reader LSN sample");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::READER_LAG_EXCEEDED),
		"split warmup cannot fix a reader rejected by byte-lag policy");
}

// ---- wrapper error accounting & server-LSN cache reset ----

static void test_wrapper_error_accounting_policy() {
	// Each case asserts the full (mark_wrapper_failed, mark_timeout_error,
	// account_wait_timeout) triple as separate invariants so a single failing
	// flag is reported precisely rather than masked by a compound &&.
	PolarDB_WrapperErrorAccounting accounting =
		polardb_wrapper_error_accounting(false, true, true, true);
	ok(!accounting.mark_wrapper_failed,
		"wrapper error accounting ignores non-wrapped query errors: no wrapper failure");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting ignores non-wrapped query errors: no timeout error");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting ignores non-wrapped query errors: no wait-timeout accounting");

	accounting = polardb_wrapper_error_accounting(true, false, false, true);
	ok(accounting.mark_wrapper_failed,
		"wrapper error accounting marks wrapper SET failure when no wait is active");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting (no active wait) does not mark timeout error");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting (no active wait) does not account wait timeout");

	accounting = polardb_wrapper_error_accounting(true, false, true, true);
	ok(accounting.mark_wrapper_failed,
		"wrapper error accounting marks wrapper SET failure with timeout flag but no active wait");
	ok(accounting.mark_timeout_error,
		"wrapper error accounting records timeout marker without active session wait");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting does not account wait timeout without active wait");

	accounting = polardb_wrapper_error_accounting(true, false, false, false);
	ok(!accounting.mark_wrapper_failed,
		"wrapper error accounting leaves consumed user-query errors alone when no wait is active");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting consumed user-query errors without active wait are not timeouts");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting consumed user-query errors without active wait do not account wait timeout");

	accounting = polardb_wrapper_error_accounting(true, true, false, false);
	ok(!accounting.mark_wrapper_failed,
		"wrapper error accounting leaves consumed user-query errors alone: no wrapper failure");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting leaves consumed user-query errors alone: no timeout error");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting leaves consumed user-query errors alone: no wait-timeout accounting");

	accounting = polardb_wrapper_error_accounting(true, true, false, true);
	ok(accounting.mark_wrapper_failed,
		"wrapper error accounting marks non-timeout wrapper SET failure");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting non-timeout wrapper SET failure is not a timeout error");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting non-timeout wrapper SET failure does not account wait timeout");

	accounting = polardb_wrapper_error_accounting(true, true, true, false);
	ok(!accounting.mark_wrapper_failed,
		"wrapper error accounting strict timeout after wrapper SET results is not a wrapper failure");
	ok(accounting.mark_timeout_error,
		"wrapper error accounting counts strict timeout after wrapper SET results");
	ok(accounting.account_wait_timeout,
		"wrapper error accounting accounts wait timeout after wrapper SET results");

	accounting = polardb_wrapper_error_accounting(true, true, true, true);
	ok(accounting.mark_wrapper_failed,
		"wrapper error accounting counts strict timeout during wrapper SET consumption: wrapper failure");
	ok(accounting.mark_timeout_error,
		"wrapper error accounting counts strict timeout during wrapper SET consumption: timeout error");
	ok(accounting.account_wait_timeout,
		"wrapper error accounting counts strict timeout during wrapper SET consumption: wait-timeout accounting");
}

static void test_wait_timeout_result_policy() {
	ok(polardb_should_handle_wait_timeout_result(
			true, true, true, true, true, false),
		"wait-timeout result policy handles ordinary finalized simple-query wait timeout");
	ok(!polardb_should_handle_wait_timeout_result(
			true, true, true, true, true, true),
		"wait-timeout result policy refuses retry after user-result transfer started");
	ok(!polardb_should_handle_wait_timeout_result(
			true, true, false, true, true, false),
		"wait-timeout result policy requires structured timeout marker");
	ok(!polardb_should_handle_wait_timeout_result(
			true, true, true, true, false, false),
		"wait-timeout result policy requires consistency-wait wrapper kind");
	ok(!polardb_should_handle_wait_timeout_result(
			false, true, true, true, true, false),
		"wait-timeout result policy is simple-query only");
	ok(!polardb_should_handle_wait_timeout_result(
			true, false, true, true, true, false),
		"wait-timeout result policy requires active wait state");
}

static void test_effective_lsn_freshness_policy() {
	bool clamped = true;
	ok(polardb_effective_lsn_freshness_ms(5000, 1000, 0, 250, &clamped) == 5000 &&
			!clamped,
		"effective freshness uses configured value when byte-lag cap is disabled");
	ok(polardb_effective_lsn_freshness_ms(5000, 0, 104857600, 250, &clamped) == 5000 &&
			!clamped,
		"effective freshness uses configured value for indefinite waits");
	ok(polardb_effective_lsn_freshness_ms(5000, 1000, 104857600, 250, &clamped) == 250 &&
			clamped,
		"effective freshness clamps to wait-timeout fraction under byte-lag cap");
	ok(polardb_effective_lsn_freshness_ms(5000, 4000, 104857600, 300, &clamped) == 300 &&
			clamped,
		"effective freshness applies configured lag-cap freshness ceiling");
	ok(polardb_effective_lsn_freshness_ms(100, 1000, 104857600, 250, &clamped) == 100 &&
			!clamped,
		"effective freshness does not raise an already tighter configured freshness");
}

static void test_server_lsn_cache_reset_policy() {
	std::atomic<uint64_t> current_lsn{900};
	std::atomic<unsigned long long> updated_at{123456};
	polardb_reset_server_lsn_cache(current_lsn, updated_at);
	ok(current_lsn.load(std::memory_order_relaxed) == 0,
		"server LSN cache reset clears cached LSN");
	ok(updated_at.load(std::memory_order_relaxed) == 0,
		"server LSN cache reset clears freshness timestamp");

	current_lsn.store(0, std::memory_order_relaxed);
	updated_at.store(55, std::memory_order_relaxed);
	polardb_reset_server_lsn_cache(current_lsn, updated_at);
	ok(updated_at.load(std::memory_order_relaxed) == 0,
		"server LSN cache reset clears timestamp even when LSN is already zero");

	current_lsn.store(77, std::memory_order_relaxed);
	updated_at.store(0, std::memory_order_relaxed);
	polardb_reset_server_lsn_cache(current_lsn, updated_at);
	ok(current_lsn.load(std::memory_order_relaxed) == 0,
		"server LSN cache reset clears LSN even when timestamp is already zero");
}

int main() {
	plan(107);
	test_degraded_rfq_notice_packet_helpers();
	test_failure_action_names();
	test_reader_action_policy_mapping();
	test_reader_status_names();
	test_wrapper_error_accounting_policy();
	test_wait_timeout_result_policy();
	test_effective_lsn_freshness_policy();
	test_server_lsn_cache_reset_policy();
	return exit_status();
}
