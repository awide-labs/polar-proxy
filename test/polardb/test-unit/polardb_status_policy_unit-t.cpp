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
		"action reason helper names transaction guard");
	ok(strcmp(polardb_route_action_reason_name(
		PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT), "multi_statement") == 0,
		"action reason helper names multi-statement guard");
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
		"wrapper error accounting marks wrapper failure when no wait is active");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting (no active wait) does not mark timeout error");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting (no active wait) does not account wait timeout");

	accounting = polardb_wrapper_error_accounting(true, false, true, true);
	ok(accounting.mark_wrapper_failed,
		"wrapper error accounting marks wrapper failure even with timeout flag but no active wait");
	ok(!accounting.mark_timeout_error,
		"wrapper error accounting does not count timeout without active wait");
	ok(!accounting.account_wait_timeout,
		"wrapper error accounting does not account wait timeout without active wait");

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
	// 56 ok() in this file = 56.
	plan(56);
	test_degraded_rfq_notice_packet_helpers();
	test_reader_status_names();
	test_wrapper_error_accounting_policy();
	test_server_lsn_cache_reset_policy();
	return exit_status();
}
