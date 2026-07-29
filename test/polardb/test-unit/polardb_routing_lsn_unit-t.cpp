/**
 * @file polardb_routing_lsn_unit-t.cpp
 * @brief Unit tests for the PolarDB monotonic-LSN routing core.
 *
 * Domain: SESSION_LSN monotonic target and wait-plan construction, zero-target
 * first reads, RFQ-unavailable route policy, query-shape checks, and the
 * positioned-RFQ source / writer-scope match matrix.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"
#include "proxysql_structs.h"

#include <cstring>
#include <string_view>
#include <type_traits>

// ---- SESSION_LSN monotonic target & wait-plan construction ----

static void test_route_action_values_are_append_only() {
	using RouteAction = PolarDB_Query_RoutePlan::RouteAction;
	ok((int)RouteAction::PASSTHROUGH == 0,
		"route action passthrough keeps stable value");
	ok((int)RouteAction::REPLICA_WITH_WAIT == 1,
		"route action replica-with-wait keeps stable value");
	ok((int)RouteAction::FORCE_PRIMARY == 2,
		"route action force-primary keeps stable value");
	ok((int)RouteAction::REPLICA_TXN_SPLIT == 3,
		"reserved transaction-split route action is appended");
}

static void test_session_lsn_target_uses_max_position() {
	PolarDB_SessionConsistency session;
	ok(session.target() == 0,
		"empty session has no monotonic LSN target");
	session.observed_lsn = 140;
	ok(session.write_lsn == 0 && session.target() == 140,
		"read-only autocommit session uses its last observed LSN");
	session.write_lsn = 120;
	ok(session.target() == 140,
		"older write LSN cannot lower the observed read target");
	session.write_lsn = 160;
	ok(session.target() == 160,
		"write LSN remains the target when it is newer than observed LSN");
}

static void test_wait_plan_uses_monotonic_session_lsn() {
	PolarDB_SessionConsistency session;
	session.write_lsn = 0;
	session.observed_lsn = 240;

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::SESSION_LSN,
		session.target(),
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/true);

	ok(wait_plan.has_wait(), "SESSION_LSN with monotonic target builds an LSN wait");
	ok(wait_plan.spec.type == PolarDB_WaitType::LSN, "wait plan uses LSN wait type");
	ok(wait_plan.spec.target == 240,
		"later autocommit read waits for the read-only session observation");
	ok(wait_plan.spec.timeout_ms == 750, "wait plan preserves resolved timeout");
	ok(wait_plan.spec.mode == PolarDB_WaitMode::STRICT, "wait plan preserves wait mode");
	ok(wait_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::REPLICA,
		"wait plan keeps replica route hint when preferred");
}

static void test_global_lsn_target_uses_session_and_group_max() {
	PolarDB_SessionConsistency session;
	session.write_lsn = 120;
	session.observed_lsn = 140;

	bool group_lsn_unknown = true;
	uint64_t target = session.target_with_global_lsn(200, &group_lsn_unknown);
	ok(!group_lsn_unknown, "GLOBAL_LSN has a target when the group LSN is known");
	ok(target == 200, "GLOBAL_LSN target can advance to the group LSN");

	target = session.target_with_global_lsn(130, &group_lsn_unknown);
	ok(!group_lsn_unknown, "GLOBAL_LSN stays valid when the group LSN is older");
	ok(target == 140, "GLOBAL_LSN target preserves newer session-observed LSN");

	target = session.target_with_global_lsn(0, &group_lsn_unknown);
	ok(group_lsn_unknown, "GLOBAL_LSN reports a missing group LSN");
	ok(target == 0, "GLOBAL_LSN does not invent a missing group LSN");

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::GLOBAL_LSN,
		200,
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/true);
	ok(wait_plan.has_wait(), "GLOBAL_LSN with target builds an LSN wait");
	ok(wait_plan.spec.target == 200, "GLOBAL_LSN wait uses computed max target");
	ok(polardb_consistency_mode_disallows_degraded_reader(
			PolarDB_ConsistencyMode::GLOBAL_LSN),
		"GLOBAL_LSN disallows degraded reader acquisition");
	ok(!polardb_consistency_mode_disallows_degraded_reader(
			PolarDB_ConsistencyMode::SESSION_LSN),
		"SESSION_LSN can still use configured RFQ best-effort degradation");

	target = polardb_target_with_global_lsn(900, 1200, &group_lsn_unknown);
	ok(!group_lsn_unknown, "GLOBAL_LSN helper accepts a known group LSN");
	ok(target == 1200, "GLOBAL_LSN helper advances a split target to the group LSN");

	target = polardb_target_with_global_lsn(1300, 1200, &group_lsn_unknown);
	ok(!group_lsn_unknown, "GLOBAL_LSN helper accepts an older group LSN");
	ok(target == 1300, "GLOBAL_LSN helper preserves newer local split target");

	target = polardb_target_with_global_lsn(900, 0, &group_lsn_unknown);
	ok(group_lsn_unknown, "GLOBAL_LSN helper reports a missing group LSN");
	ok(target == 0, "GLOBAL_LSN helper does not route a split without a group LSN");
}

static void test_wait_plan_modes_and_zero_target() {
	PolarDB_Query_WaitPlan off_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::OFF,
		700,
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/false);
	ok(!off_plan.has_wait(), "OFF mode builds no wait");
	ok(off_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::NONE,
		"OFF mode with no reader preference has no route hint");

	PolarDB_Query_WaitPlan eventual_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::EVENTUAL,
		700,
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/true);
	ok(!eventual_plan.has_wait(), "EVENTUAL mode builds no wait");
	ok(eventual_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::REPLICA,
		"EVENTUAL mode keeps the placement-selected reader hint");

	PolarDB_Query_WaitPlan zero_target = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::SESSION_LSN,
		0,
		750,
		PolarDB_WaitMode::BEST_EFFORT,
		/*prefer_replica=*/true);
	ok(!zero_target.has_wait(), "SESSION_LSN with zero target builds no active wait");
	ok(zero_target.spec.type == PolarDB_WaitType::NONE,
		"SESSION_LSN zero target does not synthesize an LSN wait");
	ok(zero_target.route_hint == PolarDB_Query_ConsistencyRouteHint::REPLICA,
		"SESSION_LSN zero target remains reader-eligible");
}

// ---- missing-LSN action ----

static void test_missing_lsn_action() {
	PolarDB_Query_RoutePlan primary = PolarDB_Query_RoutePlan::missing_lsn(
		PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
		static_cast<int>(PolarDB_MissingLsnAction::PRIMARY),
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR),
		10,
		20);
	ok(primary.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"primary action sends a missing-LSN read to the primary");
	ok(primary.target_hg == 10, "primary action targets the primary hostgroup");
	ok(primary.action_reason ==
			PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
		"primary action preserves the missing-LSN reason");
	ok(!primary.degraded_rfq_route,
		"primary action does not mark a degraded reader route");

	PolarDB_Query_RoutePlan warning =
		PolarDB_Query_RoutePlan::missing_lsn(
		PolarDB_Query_RoutePlan::RouteActionReason::OBSERVED_LSN_UNKNOWN,
		static_cast<int>(PolarDB_MissingLsnAction::WARNING),
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR),
		10,
		20);
	ok(warning.action ==
			PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH,
		"warning routes without an LSN wait");
	ok(warning.target_hg == 20,
		"warning targets the reader hostgroup");
	ok(warning.action_reason ==
			PolarDB_Query_RoutePlan::RouteActionReason::OBSERVED_LSN_UNKNOWN,
		"warning preserves the missing-LSN reason");
	ok(warning.degraded_rfq_route,
		"warning marks the degraded reader route");

	PolarDB_Query_RoutePlan reader_disallowed =
		PolarDB_Query_RoutePlan::missing_lsn(
			PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
			static_cast<int>(PolarDB_MissingLsnAction::WARNING),
			static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY),
			10,
		20,
		false);
	ok(reader_disallowed.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"warning uses the writer when a reader without a target is forbidden");
	ok(reader_disallowed.target_hg == 10,
		"reader-disallowed path targets the writer");
	ok(!reader_disallowed.degraded_rfq_route,
		"reader-disallowed path is not marked as a degraded reader route");

	PolarDB_Query_RoutePlan reader_disallowed_error =
		PolarDB_Query_RoutePlan::missing_lsn(
			PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
			static_cast<int>(PolarDB_MissingLsnAction::WARNING),
			static_cast<int>(PolarDB_ReadFallbackAction::ERROR),
			10,
			20,
			false);
	ok(reader_disallowed_error.action ==
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR,
		"unsafe warning returns an error when primary fallback is disabled");
	ok(reader_disallowed_error.target_hg == -1,
		"unsafe warning error does not select a backend hostgroup");

	PolarDB_Query_RoutePlan primary_action_with_unsafe_reader =
		PolarDB_Query_RoutePlan::missing_lsn(
			PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
			static_cast<int>(PolarDB_MissingLsnAction::PRIMARY),
			static_cast<int>(PolarDB_ReadFallbackAction::ERROR),
			10,
			20,
			false);
	ok(primary_action_with_unsafe_reader.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"the missing-LSN primary action is independent of reader availability policy");
	ok(primary_action_with_unsafe_reader.target_hg == 10,
		"the missing-LSN primary action selects the primary");

	PolarDB_Query_RoutePlan error_plan =
		PolarDB_Query_RoutePlan::missing_lsn(
			PolarDB_Query_RoutePlan::RouteActionReason::GROUP_LSN_UNKNOWN,
			static_cast<int>(PolarDB_MissingLsnAction::ERROR),
			static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY),
			10,
			20);
	ok(error_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR,
		"error ends a request whose LSN target is unavailable");
	ok(error_plan.target_hg == -1,
		"error does not select a backend hostgroup");
	ok(error_plan.action_reason ==
			PolarDB_Query_RoutePlan::RouteActionReason::GROUP_LSN_UNKNOWN,
		"error preserves the missing-LSN reason");
	ok(!error_plan.degraded_rfq_route,
		"error is not marked as a degraded reader route");
}

// ---- query shapes that require primary ----

static void test_query_shapes_require_primary_without_degradation() {
	auto reason = polardb_writer_required_reason(true, false, false);
	ok(reason == PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION,
		"writer-required helper detects explicit transaction");
	PolarDB_Query_RoutePlan plan =
		PolarDB_Query_RoutePlan::force_primary(10, reason);
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"transaction query shape forces primary");
	ok(plan.target_hg == 10, "transaction query shape targets writer");
	ok(plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION,
		"transaction query shape reports transaction reason");
	ok(!plan.degraded_rfq_route,
		"transaction query shape does not mark RFQ best-effort degradation");

	reason = polardb_writer_required_reason(false, true, false);
	ok(reason == PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT,
		"writer-required helper detects multi-statement query");
	plan = PolarDB_Query_RoutePlan::force_primary(10, reason);
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"multi-statement query shape forces primary");
	ok(plan.target_hg == 10, "multi-statement query shape targets writer");
	ok(plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT,
		"multi-statement query shape reports multi-statement reason");
	ok(!plan.degraded_rfq_route,
		"multi-statement query shape does not mark RFQ best-effort degradation");

	reason = polardb_writer_required_reason(false, false, true);
	ok(reason == PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_LOCKING_READ,
		"writer-required helper detects autocommit locking SELECT");
	plan = PolarDB_Query_RoutePlan::force_primary(10, reason);
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"locking SELECT query shape forces primary");
	ok(plan.target_hg == 10, "locking SELECT query shape targets writer");
	ok(plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_LOCKING_READ,
		"locking SELECT query shape reports locking-read reason");
	ok(!plan.degraded_rfq_route,
		"locking SELECT query shape does not mark RFQ best-effort degradation");

	ok(polardb_writer_required_reason(false, false, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::NONE,
		"writer-required helper does not apply to ordinary autocommit single statement");
}

static void test_txn_split_query_shape_classifier() {
	ok(PolarDB_Protocol::is_txn_split_safe_read("SELECT 1"),
		"transaction split classifier accepts plain SELECT");
	ok(PolarDB_Protocol::is_txn_split_safe_read(" \t\nSELECT 1"),
		"transaction split classifier accepts SELECT with leading whitespace");
	ok(!PolarDB_Protocol::is_txn_split_safe_read("SHOW server_version"),
		"transaction split classifier rejects SHOW");
	ok(!PolarDB_Protocol::is_txn_split_safe_read("WITH q AS (SELECT 1) SELECT * FROM q"),
		"transaction split classifier rejects WITH");
	ok(!PolarDB_Protocol::is_txn_split_safe_read("SELECT * FROM t FOR UPDATE"),
		"transaction split classifier rejects locking SELECT");
	ok(PolarDB_Protocol::is_locking_select_query("SELECT * FROM t FOR SHARE"),
		"transaction split classifier identifies locking SELECT");
	ok(PolarDB_Protocol::is_locking_select_query("SELECT * FROM t FOR NO KEY UPDATE"),
		"transaction split classifier identifies NO KEY UPDATE");
	ok(PolarDB_Protocol::is_locking_select_query("SELECT * FROM t FOR KEY SHARE"),
		"transaction split classifier identifies KEY SHARE");
	ok(!PolarDB_Protocol::is_locking_select_query("SELECT * FROM t FOR XML"),
		"transaction split classifier does not reject non-locking FOR token");
	ok(!PolarDB_Protocol::is_locking_select_query(
			"SELECT substring(name from 1 for 3) FROM t"),
		"transaction split classifier ignores non-locking FOR expression");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t WHERE note='x for' FOR UPDATE"),
		"transaction split classifier finds locking FOR after string literal");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t WHERE note=E'O\\'Brien' FOR UPDATE"),
		"transaction split classifier finds locking FOR after escaped string literal");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t /* for */ FOR UPDATE"),
		"transaction split classifier finds locking FOR after comment");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t FOR /* comment */ UPDATE"),
		"transaction split classifier accepts block comment inside locking clause");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t FOR -- comment\n UPDATE"),
		"transaction split classifier accepts line comment inside locking clause");
	ok(PolarDB_Protocol::is_locking_select_query(
			"/* route comment */ SELECT * FROM t FOR UPDATE"),
		"locking SELECT classifier accepts ProxySQL's SELECT command classification after a leading comment");
	ok(PolarDB_Protocol::is_locking_select_query(
			"WITH q AS (SELECT 1) SELECT * FROM t FOR SHARE"),
		"locking SELECT classifier finds a locking clause after a WITH query prefix");
	ok(PolarDB_Protocol::is_locking_select_query(
			"SELECT * FROM t WHERE note=$tag$x for$tag$ FOR UPDATE"),
		"transaction split classifier finds locking FOR after dollar quote");
	ok(!PolarDB_Protocol::is_locking_select_query("SELECT 'for update'"),
		"transaction split classifier ignores locking words in string literal");
	ok(!PolarDB_Protocol::is_locking_select_query("SELECT E'for update'"),
		"transaction split classifier ignores locking words in escaped string literal");
	ok(!PolarDB_Protocol::is_locking_select_query("SELECT \"for\" FROM t"),
		"transaction split classifier ignores quoted identifier");
	ok(!PolarDB_Protocol::is_locking_select_query("SELECT $$for update$$"),
		"transaction split classifier ignores dollar-quoted body");
	ok(!PolarDB_Protocol::is_write_query(
			"SELECT substring(name from 1 for 3) FROM t"),
		"result classifier keeps non-locking FOR expression as read");
	ok(PolarDB_Protocol::is_write_query(
			"SELECT * FROM t FOR UPDATE", PGSQL_QUERY_SELECT),
		"routing write classifier treats locking SELECT as writer-only");
	ok(!PolarDB_Protocol::should_advance_session_write_lsn(
			"SELECT * FROM t FOR UPDATE", PGSQL_QUERY_SELECT),
		"result LSN classifier keeps locking SELECT out of write LSN state");
	ok(PolarDB_Protocol::should_advance_session_write_lsn(
			"UPDATE t SET v = 1", PGSQL_QUERY_UPDATE),
		"result LSN classifier accepts UPDATE as write-producing");
	const char* modifying_cte =
		"WITH changed AS (UPDATE t SET v = 2 RETURNING v) SELECT * FROM changed";
	ok(!PolarDB_Protocol::is_write_query(
			modifying_cte, PGSQL_QUERY_SELECT),
		"routing classifier documents the current parser limitation for a data-modifying CTE");
	ok(!PolarDB_Protocol::should_advance_session_write_lsn(
			modifying_cte, PGSQL_QUERY_SELECT),
		"result classifier documents that parser-classified CTE does not advance write LSN");
	ok(PolarDB_Protocol::should_advance_session_write_lsn(
			modifying_cte, PGSQL_QUERY_UNKNOWN),
		"result classifier treats the same CTE as a write when command type is unknown");
	ok(!PolarDB_Protocol::should_advance_session_write_lsn(
			"/* comment */ SELECT * FROM t FOR UPDATE",
			PGSQL_QUERY_SELECT),
		"result LSN classifier trusts ProxySQL SELECT command classification");
	ok(PolarDB_Protocol::is_txn_split_safe_select(
			true, "/* comment */ SELECT 1"),
		"transaction split hot path uses ProxySQL SELECT classification");
	ok(!PolarDB_Protocol::is_txn_split_safe_select(
			true, "/* comment */ SELECT * FROM t FOR UPDATE"),
		"transaction split hot path rejects commented locking SELECT");
	ok(!PolarDB_Protocol::is_txn_split_safe_select(
			false, "SHOW server_version"),
		"transaction split hot path rejects non-SELECT command classification");
}

static void test_zero_lsn_safe_statement_classifier() {
	ok(polardb_rfq_lsn_payload_state(false, 0) ==
			PolarDB_RfqLsnPayloadState::MISSING,
		"RFQ payload classifier distinguishes an absent payload");
	ok(polardb_rfq_lsn_payload_state(true, 0) ==
			PolarDB_RfqLsnPayloadState::ZERO,
		"RFQ payload classifier preserves a present zero value");
	ok(polardb_rfq_lsn_payload_state(true, 42) ==
			PolarDB_RfqLsnPayloadState::POSITIONED,
		"RFQ payload classifier recognizes a positioned LSN");
	ok(polardb_zero_lsn_payload_can_skip_wait_target(
			"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"),
		"zero RFQ-LSN classifier allows session SET statements");
	ok(polardb_zero_lsn_payload_can_skip_wait_target("RESET ALL"),
		"zero RFQ-LSN classifier allows RESET statements");
	ok(polardb_zero_lsn_payload_can_skip_wait_target("BEGIN"),
		"zero RFQ-LSN classifier allows transaction control statements");
	ok(!polardb_zero_lsn_payload_can_skip_wait_target("SELECT 1"),
		"zero RFQ-LSN classifier leaves ordinary reads to the caller");
	ok(!polardb_zero_lsn_payload_can_skip_wait_target("SELECT * FROM t FOR UPDATE"),
		"zero RFQ-LSN classifier does not whitelist locking SELECT");
	ok(!polardb_zero_lsn_payload_can_skip_wait_target("SHOW server_version"),
		"zero RFQ-LSN classifier does not whitelist SHOW");
	ok(!polardb_zero_lsn_payload_can_skip_wait_target("EXPLAIN SELECT 1"),
		"zero RFQ-LSN classifier does not whitelist EXPLAIN");
	ok(!polardb_zero_lsn_payload_can_skip_wait_target("INSERT INTO t VALUES (1)"),
		"zero RFQ-LSN classifier does not whitelist writes");
}

// ---- transaction-split planning eligibility ----

static void test_txn_split_rejection_reason() {
	PolarDB_TransactionSplitState state;
	auto snapshot = [&]() {
		return polardb_transaction_split_snapshot(state);
	};

	ok(sizeof(PolarDB_TransactionSplitSnapshot) <= 32,
		"transaction split route snapshot stays within 32 bytes");
	ok(std::is_trivially_destructible<PolarDB_TransactionSplitSnapshot>::value,
		"transaction split route snapshot owns no request-lifetime state");
	ok(snapshot().xids.data() == state.xids.data(),
		"transaction split route snapshot views the session-owned XID string");

	ok(polardb_txn_split_rejection_reason(false, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::HG_SPLIT_DISABLED,
		"transaction split planning rejects disabled hostgroup policy");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, true, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT,
		"transaction split planning rejects multi-statement query");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, true, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::EXTENDED_PROTOCOL,
		"transaction split planning rejects extended protocol");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, false, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_NOT_SELECT,
		"transaction split planning rejects non-SELECT statement shape");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, false, true) ==
			PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_LOCKING_READ,
		"transaction split planning rejects locking SELECT statement shape");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), true, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_WRITE_LSN_UNKNOWN,
		"transaction split planning rejects prior write with unknown LSN");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, true, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_OBSERVED_LSN_UNKNOWN,
		"transaction split planning rejects prior observed read with unknown LSN");

	state.stage = PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	state.blocked = true;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_BLOCKED,
		"transaction split planning rejects blocked transaction");

	state.blocked = false;
	state.wal_pending = true;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING,
		"transaction split planning rejects WAL-pending transaction");

	state.xids = "10,11";
	state.primary_lsn = 900;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING,
		"transaction split planning rejects WAL-pending transaction with XIDs and LSN");

	state.wal_pending = false;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION,
		"transaction split planning keeps non-splittable transaction on primary");

	state.stage = PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	state.xids.clear();
	state.primary_lsn = 0;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::INVARIANT_VIOLATION,
		"transaction split planning rejects splittable state without XIDs");

	state.xids = "10,11";
	PolarDB_TransactionSplitSnapshot missing_xids = snapshot();
	missing_xids.xids = std::string_view();
	ok(polardb_txn_split_rejection_reason(true, missing_xids, false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::INVARIANT_VIOLATION,
		"transaction split planning reads XIDs from the route-context view");
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::NO_TXN_LSN,
		"transaction split planning rejects splittable state without primary LSN");

	state.primary_lsn = 900;
	ok(polardb_txn_split_rejection_reason(true, snapshot(), false, false, false, false, true, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::NONE,
		"transaction split planning accepts complete RFQ evidence");
}

static void test_txn_split_route_plan_factory() {
	PolarDB_WaitSpec wait;
	wait.type = PolarDB_WaitType::LSN;
	wait.target = 900;
	wait.timeout_ms = 750;
	wait.mode = PolarDB_WaitMode::STRICT;

	PolarDB_Query_RoutePlan plan =
		PolarDB_Query_RoutePlan::replica_txn_split(20, 10, wait, "10,11");
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT,
		"transaction split plan uses split route action");
	ok(plan.target_hg == 20,
		"transaction split plan targets reader hostgroup");
	ok(plan.wait_spec.type == PolarDB_WaitType::LSN,
		"transaction split plan uses LSN wait type");
	ok(plan.wait_spec.target == 900,
		"transaction split plan carries primary LSN wait target");
	ok(plan.wait_spec.timeout_ms == 750,
		"transaction split plan carries wait timeout");
	ok(plan.wait_spec.mode == PolarDB_WaitMode::STRICT,
		"transaction split plan preserves wait mode");
	ok(plan.txn_xids == "10,11",
		"transaction split plan carries XID view");
	ok(plan.reader.fallback_writer_hg == 10,
		"transaction split plan records writer fallback");
	ok(plan.reader.require_replica,
		"transaction split plan excludes the physical writer endpoint");
}

// ---- backend hostgroup classification & writer-scope match matrix ----

static void test_backend_writer_hostgroup_classification() {
	ok(polardb_backend_is_writer_hostgroup(true, 10, 10),
		"writer hostgroup classification accepts the configured writer");
	ok(!polardb_backend_is_writer_hostgroup(true, 20, 10),
		"writer hostgroup classification rejects a reader hostgroup");
	ok(!polardb_backend_is_writer_hostgroup(false, 10, 10),
		"writer hostgroup classification rejects a non-PolarDB hostgroup");
	ok(!polardb_backend_is_writer_hostgroup(true, -1, 10),
		"writer hostgroup classification rejects a missing backend hostgroup");
	ok(!polardb_backend_is_writer_hostgroup(true, 10, -1),
		"writer hostgroup classification rejects a missing writer hostgroup");
}

static void test_rfq_result_update_group_epoch_check() {
	// The RFQ-result-update check accepts a positioned RFQ only when its writer
	// scope still matches the current writer scope.
	check_writer_scope_match_matrix("RFQ result update");
}

static void test_session_lsn_scope_check() {
	// The session-LSN scope check discards session LSN state captured under a
	// different writer scope; same match matrix as the RFQ-result-update check.
	check_writer_scope_match_matrix("session LSN scope");
}

int main() {
	plan(164);
	test_route_action_values_are_append_only();
	test_session_lsn_target_uses_max_position();
	test_wait_plan_uses_monotonic_session_lsn();
	test_global_lsn_target_uses_session_and_group_max();
	test_wait_plan_modes_and_zero_target();
	test_missing_lsn_action();
	test_query_shapes_require_primary_without_degradation();
	test_txn_split_query_shape_classifier();
	test_zero_lsn_safe_statement_classifier();
	test_txn_split_rejection_reason();
	test_txn_split_route_plan_factory();
	test_backend_writer_hostgroup_classification();
	test_rfq_result_update_group_epoch_check();
	test_session_lsn_scope_check();
	return exit_status();
}
