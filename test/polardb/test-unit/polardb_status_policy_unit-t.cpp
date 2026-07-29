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

// ---- route-action-reason names & NoticeResponse packet helpers ----

static void test_degraded_rfq_notice_packet_helpers() {
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::NONE), "none") == 0,
		"action reason helper names none");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::EXTENDED_PROTOCOL), "extended_protocol") == 0,
		"action reason helper names extended protocol");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION), "in_transaction") == 0,
		"action reason helper names transaction check");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT), "multi_statement") == 0,
		"action reason helper names multi-statement check");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::MODE_PRIMARY), "mode_primary") == 0,
		"action reason helper names primary mode");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::HINT_PRIMARY), "hint_primary") == 0,
		"action reason helper names primary hint");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN), "write_lsn_unknown") == 0,
		"action reason helper names missing write LSN");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::OBSERVED_LSN_UNKNOWN), "observed_lsn_unknown") == 0,
		"action reason helper names missing observed LSN");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::PRIMARY_LSN_UNKNOWN), "primary_lsn_unknown") == 0,
		"action reason helper names missing primary mirror LSN");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::READER_FAILURE_FORCE_WRITER), "reader_failure_force_writer") == 0,
		"action reason helper names reader-failure writer route");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING), "wal_pending") == 0,
		"action reason helper names split WAL-pending veto");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_BLOCKED), "split_blocked") == 0,
		"action reason helper names split-blocked veto");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_WRITE_LSN_UNKNOWN), "split_write_lsn_unknown") == 0,
		"action reason helper names split write-LSN unknown veto");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_OBSERVED_LSN_UNKNOWN), "split_observed_lsn_unknown") == 0,
		"action reason helper names split observed-LSN unknown veto");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::NO_TXN_LSN), "no_txn_lsn") == 0,
		"action reason helper names missing transaction LSN");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::INVARIANT_VIOLATION), "invariant_violation") == 0,
		"action reason helper names invariant fallback");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::HG_SPLIT_DISABLED), "hg_split_disabled") == 0,
		"action reason helper names hostgroup split-disabled veto");

	const char* severity = "WARNING";
	const char* severity_nonlocalized = "WARNING";
	const char* sqlstate = "01000";
	const char* message = "PolarDB best_effort RFQ route has no enforceable LSN wait target; read may be stale";
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
	ok(strcmp(polardb_failure_action_name(
			PolarDB_FailureAction::PASSTHROUGH), "passthrough") == 0,
		"failure action helper names passthrough");
	ok(strcmp(polardb_failure_action_name(
			PolarDB_FailureAction::RETRY), "retry") == 0,
		"failure action helper names retry");
	ok(strcmp(polardb_failure_action_name(
			PolarDB_FailureAction::FORWARD), "forward") == 0,
		"failure action helper names forward");
	ok(strcmp(polardb_failure_action_name(
			PolarDB_FailureAction::TERMINATE), "terminate") == 0,
		"failure action helper names terminate");
}

static void test_reader_action_policy_mapping() {
	ok(polardb_reader_action_from_string("retry", -1) ==
			static_cast<int>(PolarDB_ReaderAction::RETRY),
		"reader action parser accepts retry");
	ok(polardb_reader_action_from_string("forward", -1) ==
			static_cast<int>(PolarDB_ReaderAction::FORWARD),
		"reader action parser accepts forward");
	ok(polardb_reader_action_from_string("terminate", -1) ==
			static_cast<int>(PolarDB_ReaderAction::TERMINATE),
		"reader action parser accepts terminate");
	ok(polardb_reader_action_from_string("invalid", 7) == 7,
		"reader action parser returns default for invalid value");
	ok(strcmp(polardb_reader_action_name(
			PolarDB_ReaderAction::RETRY), "retry") == 0,
		"reader action name helper names retry");
	ok(strcmp(polardb_reader_action_name(
			PolarDB_ReaderAction::FORWARD), "forward") == 0,
		"reader action name helper names forward");
	ok(strcmp(polardb_reader_action_name(
			PolarDB_ReaderAction::TERMINATE), "terminate") == 0,
		"reader action name helper names terminate");
	ok(strcmp(polardb_reader_failure_kind_name(
			PolarDB_ReaderFailureKind::CONNECTION_LOST), "connection_lost") == 0,
		"reader failure kind names connection_lost");
	ok(strcmp(polardb_reader_failure_kind_name(
			PolarDB_ReaderFailureKind::WAIT_TIMEOUT), "wait_timeout") == 0,
		"reader failure kind names wait_timeout");
	ok(strcmp(polardb_reader_failure_kind_name(
			PolarDB_ReaderFailureKind::REUSABLE_ERROR), "reusable_error") == 0,
		"reader failure kind names reusable_error");
	ok(strcmp(polardb_retry_target_name(
			PolarDB_RetryTarget::WRITER), "writer") == 0,
		"retry target names writer");
	ok(strcmp(polardb_retry_target_name(
			PolarDB_RetryTarget::OTHER_READER), "other_reader") == 0,
		"retry target names other_reader");
	ok(strcmp(polardb_reader_failure_route_name(
			PolarDB_ReaderFailureRoute::NONE), "none") == 0,
		"reader-failure route names none");
	ok(strcmp(polardb_reader_failure_route_name(
			PolarDB_ReaderFailureRoute::FORCE_WRITER), "force_writer") == 0,
		"reader-failure route names force_writer");
	ok(strcmp(polardb_reader_failure_route_name(
			PolarDB_ReaderFailureRoute::SKIP_READER), "skip_reader") == 0,
		"reader-failure route names skip_reader");
}

// ---- reader-status names ----

static void test_reader_status_names() {
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::ACQUIRED),
			"acquired") == 0,
		"acquired status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::READER_UNAVAILABLE),
			"reader_unavailable") == 0,
		"reader unavailable status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::READER_BUSY),
			"reader_busy") == 0,
		"reader busy status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::RETRY_CURRENT_STATE),
			"retry_current_state") == 0,
		"current-state retry status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::RFQ_UNAVAILABLE),
			"rfq_unavailable") == 0,
		"RFQ unavailable status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN),
			"primary_lsn_unknown") == 0,
		"primary LSN unknown status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::READER_LSN_UNKNOWN),
			"reader_lsn_unknown") == 0,
		"reader LSN unknown status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::READER_LSN_STALE),
			"reader_lsn_stale") == 0,
		"reader LSN stale status has stable lowercase name");
	ok(strcmp(polardb_reader_status_name(
			PolarDB_ReaderStatus::READER_LAG_EXCEEDED),
			"reader_lag_exceeded") == 0,
		"reader lag cap status has stable lowercase name");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_UNAVAILABLE),
		"reader unavailable uses normal no-connection handling");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::READER_BUSY),
		"reader busy uses normal no-connection handling");
	ok(!polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::RETRY_CURRENT_STATE),
		"current-state retry returns through normal no-connection handling");
	ok(polardb_reader_status_redirects_to_writer(
			PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN),
		"unknown primary LSN redirects this consistency read to writer");
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
			PolarDB_ReaderStatus::RETRY_CURRENT_STATE),
		"split warmup is not requested for configuration churn");
	ok(!polardb_reader_status_split_warmup_can_help(
			PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN),
		"split warmup cannot fix a missing primary LSN sample");
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
	// 102 ok() in this file = 102.
	plan(105);
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
