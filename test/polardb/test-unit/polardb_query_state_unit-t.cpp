/**
 * @file polardb_query_state_unit-t.cpp
 * @brief Unit tests for PolarDB_QueryState named-reset semantics.
 *
 * Domain: scoped per-query reset helpers (reset_reader_target, reset_wait,
 * reset_dispatch_wrapper, reset_for_new_query) and exactly which fields each one
 * clears versus preserves.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

#include <cstring>

// ---- QueryState named-reset subsets ----

static void test_query_state_named_reset_subsets() {
	const int fallback_writer_hg = 20;

	PolarDB_QueryState query;
	query.reader_plan.consistency_target_lsn = 500;
	query.request_writer_scope = PolarDB_WriterScope{10, 7};
	query.wait.prepare_from_spec(PolarDB_WaitSpec::lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS, PolarDB_WaitMode::BEST_EFFORT));
	query.wait.timeout_error = true;
	query.wait.fallback_writer_hg = fallback_writer_hg;
	query.wait.wait_stage = PolarDB_WaitStage::WAITING;
	query.wait.wait_started_at_us = 42;
	query.wrapped_query_buf = "wrapped";
	query.dispatch_wrapper_stmts = 3;
	query.dispatch_wrapper_kind = PolarDB_Query_WrapperKind::CONSISTENCY_WAIT;

	query.reset_reader_target();
	ok(query.reader_plan.consistency_target_lsn == 0,
		"reader-target reset clears reader plan");
	ok(query.request_writer_scope.valid(),
		"reader-target reset preserves request writer scope");
	// reader-target reset preserves staged wait state (each field separately).
	ok(query.wait.spec.has_wait(),
		"reader-target reset preserves staged wait spec");
	ok(query.wait.wait_stage == PolarDB_WaitStage::WAITING,
		"reader-target reset preserves staged wait stage");
	ok(query.wait.timeout_error,
		"reader-target reset preserves staged timeout-error flag");
	ok(query.wait.fallback_writer_hg == fallback_writer_hg,
		"reader-target reset preserves staged fallback writer hostgroup");

	query.reset_wait();
	// wait reset clears wait spec and runtime wait state (each field separately).
	ok(!query.wait.spec.has_wait(),
		"wait reset clears wait spec");
	ok(query.wait.wait_stage == PolarDB_WaitStage::IDLE,
		"wait reset returns wait stage to idle");
	ok(query.wait.wait_started_at_us == 0,
		"wait reset clears wait start timestamp");
	ok(!query.wait.timeout_error,
		"wait reset clears timeout-error flag");
	ok(query.wait.fallback_writer_hg == -1,
		"wait reset clears fallback writer hostgroup");
	ok(query.request_writer_scope.valid(),
		"wait reset preserves request writer scope");

	query.reset_dispatch_wrapper();
	// dispatch-wrapper reset clears only wrapper handoff metadata.
	ok(query.dispatch_wrapper_stmts == 0,
		"dispatch-wrapper reset clears wrapper statement count");
	ok(query.dispatch_wrapper_kind == PolarDB_Query_WrapperKind::NONE,
		"dispatch-wrapper reset clears wrapper kind");
	ok(query.request_writer_scope.valid(),
		"dispatch-wrapper reset preserves request writer scope");
	ok(query.wrapped_query_buf == "wrapped",
		"dispatch-wrapper reset preserves wrapped query buffer");

	query.reset_for_new_query();
	// full query reset clears target, request scope, and wrapper buffer.
	ok(!query.reader_plan.has_consistency_target_lsn(),
		"full query reset clears reader target");
	ok(!query.request_writer_scope.valid(),
		"full query reset clears request writer scope");
	ok(query.wrapped_query_buf.empty(),
		"full query reset clears wrapper buffer");
	ok(query.wait.wait_stage == PolarDB_WaitStage::IDLE,
		"full query reset clears wait state");
	ok(query.dispatch_wrapper_kind == PolarDB_Query_WrapperKind::NONE,
		"full query reset clears dispatch state");
}

static void test_reader_plan_consistency_target_helpers() {
	PolarDB_Query_ReaderPlan plan;

	ok(!plan.has_consistency_target_lsn(),
		"reader plan: zero consistency target means no target");
	ok(!plan.reader_lsn_reaches_consistency_target(100),
		"reader plan: reader cannot reach an absent consistency target");

	plan.consistency_target_lsn = 100;
	ok(plan.has_consistency_target_lsn(),
		"reader plan: nonzero consistency target is present");
	ok(!plan.reader_lsn_reaches_consistency_target(99),
		"reader plan: reader below consistency target does not reach it");
	ok(plan.reader_lsn_reaches_consistency_target(100),
		"reader plan: reader at consistency target reaches it");
	ok(plan.reader_lsn_reaches_consistency_target(101),
		"reader plan: reader above consistency target reaches it");
}

int main() {
	// 21 named-reset checks + 6 reader-plan helper checks = 27.
	plan(27);
	test_query_state_named_reset_subsets();
	test_reader_plan_consistency_target_helpers();
	return exit_status();
}
