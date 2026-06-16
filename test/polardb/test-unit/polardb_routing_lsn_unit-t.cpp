/**
 * @file polardb_routing_lsn_unit-t.cpp
 * @brief Unit tests for the PolarDB monotonic-LSN routing core.
 *
 * Domain: SESSION_LSN monotonic target and wait-plan construction, first-read
 * baseline seeding, RFQ-unavailable route policy, query-shape guards, and the
 * positioned-RFQ source / writer-scope match matrix.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

#include <cstring>

// ---- SESSION_LSN monotonic target & wait-plan construction ----

static void test_session_lsn_target_uses_max_position() {
	PolarDB_SessionConsistency session;
	ok(session.target() == 0,
		"empty session has no monotonic LSN target");
	session.write_lsn = 120;
	ok(session.target() == 120,
		"write LSN is the target when no observed LSN exists");
	session.observed_lsn = 140;
	ok(session.target() == 140,
		"observed LSN can advance the monotonic target beyond write LSN");
	session.write_lsn = 160;
	ok(session.target() == 160,
		"write LSN remains the target when it is newer than observed LSN");
}

static void test_wait_plan_uses_monotonic_session_lsn() {
	PolarDB_SessionConsistency session;
	session.write_lsn = 200;
	session.observed_lsn = 240;

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::SESSION_LSN,
		session.target(),
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/true);

	ok(wait_plan.has_wait(), "SESSION_LSN with monotonic target builds an LSN wait");
	ok(wait_plan.spec.type == PolarDB_WaitType::LSN, "wait plan uses LSN wait type");
	ok(wait_plan.spec.target == 240, "wait target is max(write_lsn, observed_lsn)");
	ok(wait_plan.spec.timeout_ms == 750, "wait plan preserves resolved timeout");
	ok(wait_plan.spec.mode == PolarDB_WaitMode::STRICT, "wait plan preserves wait mode");
	ok(wait_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::REPLICA,
		"wait plan keeps replica route hint when preferred");
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

	PolarDB_Query_WaitPlan primary_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::PRIMARY_ONLY,
		700,
		750,
		PolarDB_WaitMode::STRICT,
		/*prefer_replica=*/true);
	ok(!primary_plan.has_wait(), "PRIMARY mode builds no wait");
	ok(primary_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::PRIMARY,
		"PRIMARY mode returns primary route hint");

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

// ---- first-read baseline (observed / primary) seeding ----

static void test_first_read_observed_baseline_has_no_wait_target() {
	PolarDB_SessionConsistency session;
	bool primary_lsn_unknown = false;
	uint64_t session_lsn = session.target_with_baseline(
		(int)PolarDB_SessionLsnBaseline::OBSERVED,
		900,
		&primary_lsn_unknown);

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::SESSION_LSN,
		session_lsn,
		POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT,
		/*prefer_replica=*/true);

	ok(!primary_lsn_unknown, "observed baseline does not require primary mirror");
	ok(!wait_plan.has_wait(), "OBSERVED baseline first read has no wait");
	ok(wait_plan.spec.target == 0, "OBSERVED baseline does not synthesize a target");
	ok(wait_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::REPLICA,
		"OBSERVED baseline leaves first read eligible for reader passthrough");
}

static void test_first_read_primary_baseline_uses_primary_target() {
	PolarDB_SessionConsistency session;
	bool primary_lsn_unknown = false;
	uint64_t target = session.target_with_baseline(
		(int)PolarDB_SessionLsnBaseline::PRIMARY,
		900,
		&primary_lsn_unknown);
	ok(!primary_lsn_unknown, "PRIMARY baseline with mirror LSN is known");
	ok(target == 900, "PRIMARY baseline seeds first-read target from primary mirror");

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		PolarDB_ConsistencyMode::SESSION_LSN,
		target,
		POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT,
		/*prefer_replica=*/true);
	ok(wait_plan.has_wait(), "PRIMARY baseline first read builds a wait");
	ok(wait_plan.spec.target == 900, "PRIMARY baseline wait uses the mirror LSN");

	session.write_lsn = 120;
	target = session.target_with_baseline(
		(int)PolarDB_SessionLsnBaseline::PRIMARY,
		900,
		&primary_lsn_unknown);
	ok(target == 120, "PRIMARY baseline does not override an existing session target");
}

static void test_first_read_primary_baseline_unknown() {
	PolarDB_SessionConsistency session;
	bool primary_lsn_unknown = false;
	uint64_t target = session.target_with_baseline(
		(int)PolarDB_SessionLsnBaseline::PRIMARY,
		0,
		&primary_lsn_unknown);
	ok(primary_lsn_unknown, "PRIMARY baseline reports unknown when mirror LSN is zero");
	ok(target == 0, "PRIMARY baseline unknown does not invent a target");
}

// ---- RFQ-unavailable route policy ----

static void test_rfq_unavailable_route_policy() {
	PolarDB_Query_RoutePlan strict = PolarDB_Query_RoutePlan::rfq_unavailable(
		PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
		(int)PolarDB_RfqRoutePolicy::STRICT,
		10,
		20);
	ok(strict.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"STRICT RFQ-unavailable policy forces primary");
	ok(strict.target_hg == 10, "STRICT RFQ-unavailable policy targets writer");
	ok(strict.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
		"STRICT RFQ-unavailable policy preserves action reason");
	ok(!strict.degraded_rfq_route, "STRICT RFQ-unavailable policy is not degraded");

	PolarDB_Query_RoutePlan best_effort = PolarDB_Query_RoutePlan::rfq_unavailable(
		PolarDB_Query_RoutePlan::RouteActionReason::PRIMARY_LSN_UNKNOWN,
		(int)PolarDB_RfqRoutePolicy::BEST_EFFORT,
		10,
		20);
	ok(best_effort.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH,
		"BEST_EFFORT RFQ-unavailable policy routes without wait");
	ok(best_effort.target_hg == 20, "BEST_EFFORT RFQ-unavailable policy targets reader");
	ok(best_effort.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::PRIMARY_LSN_UNKNOWN,
		"BEST_EFFORT RFQ-unavailable policy preserves primary-unknown reason");
	ok(best_effort.degraded_rfq_route, "BEST_EFFORT RFQ-unavailable policy marks degraded route");

	PolarDB_Query_RoutePlan best_effort_no_degrade = PolarDB_Query_RoutePlan::rfq_unavailable(
		PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
		(int)PolarDB_RfqRoutePolicy::BEST_EFFORT,
		10,
		20,
		false);
	ok(best_effort_no_degrade.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"BEST_EFFORT RFQ-unavailable policy can use writer when degradation is disabled");
	ok(best_effort_no_degrade.target_hg == 10,
		"BEST_EFFORT no-degrade path targets writer");
	ok(!best_effort_no_degrade.degraded_rfq_route,
		"BEST_EFFORT no-degrade path does not mark degraded route");
}

// ---- hard query-shape guards (force primary) ----

static void test_hard_query_shape_guards_force_primary_without_degradation() {
	auto reason = polardb_writer_required_reason(true, false);
	ok(reason == PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION,
		"hard guard detects explicit transaction");
	PolarDB_Query_RoutePlan plan =
		PolarDB_Query_RoutePlan::force_primary(10, reason);
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"transaction hard guard forces primary");
	ok(plan.target_hg == 10, "transaction hard guard targets writer");
	ok(plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION,
		"transaction hard guard reports transaction reason");
	ok(!plan.degraded_rfq_route,
		"transaction hard guard does not mark RFQ best-effort degradation");

	reason = polardb_writer_required_reason(false, true);
	ok(reason == PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT,
		"hard guard detects multi-statement query");
	plan = PolarDB_Query_RoutePlan::force_primary(10, reason);
	ok(plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY,
		"multi-statement hard guard forces primary");
	ok(plan.target_hg == 10, "multi-statement hard guard targets writer");
	ok(plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT,
		"multi-statement hard guard reports multi-statement reason");
	ok(!plan.degraded_rfq_route,
		"multi-statement hard guard does not mark RFQ best-effort degradation");

	ok(polardb_writer_required_reason(false, false) ==
			PolarDB_Query_RoutePlan::RouteActionReason::NONE,
		"hard guard does not apply to ordinary autocommit single statement");
}

// ---- positioned-RFQ source classification & writer-scope match matrix ----

static void test_positioned_rfq_primary_source_classification() {
	ok(polardb_positioned_rfq_from_primary(true, 10, 10),
		"positioned RFQ from writer hostgroup is primary-sourced");
	ok(!polardb_positioned_rfq_from_primary(true, 20, 10),
		"positioned RFQ from reader hostgroup is not primary-sourced");
	ok(!polardb_positioned_rfq_from_primary(false, 10, 10),
		"non-PolarDB hostgroup is not primary-sourced for latch repair");
	ok(!polardb_positioned_rfq_from_primary(true, -1, 10),
		"missing backend hostgroup is not primary-sourced");
	ok(!polardb_positioned_rfq_from_primary(true, 10, -1),
		"missing writer hostgroup is not primary-sourced");
}

static void test_rfq_result_update_group_epoch_guard() {
	// The RFQ-result-update guard accepts a positioned RFQ only when its writer
	// scope still matches the current writer scope.
	check_writer_scope_match_matrix("RFQ result update");
}

static void test_session_lsn_scope_guard() {
	// The session-LSN scope guard discards session LSN state captured under a
	// different writer scope; same match matrix as the RFQ-result-update guard.
	check_writer_scope_match_matrix("session LSN scope");
}

int main() {
	// 55 ok() in this file + 2 calls to check_writer_scope_match_matrix()
	// (5 assertions each, defined in polardb_unit_common.h) = 65.
	plan(65);
	test_session_lsn_target_uses_max_position();
	test_wait_plan_uses_monotonic_session_lsn();
	test_wait_plan_modes_and_zero_target();
	test_first_read_observed_baseline_has_no_wait_target();
	test_first_read_primary_baseline_uses_primary_target();
	test_first_read_primary_baseline_unknown();
	test_rfq_unavailable_route_policy();
	test_hard_query_shape_guards_force_primary_without_degradation();
	test_positioned_rfq_primary_source_classification();
	test_rfq_result_update_group_epoch_guard();
	test_session_lsn_scope_guard();
	return exit_status();
}
