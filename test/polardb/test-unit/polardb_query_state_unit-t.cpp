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
	query.reader_plan.primary_lsn = 700;
	query.reader_plan.max_lag_bytes = 100;
	query.reader_plan.fallback_writer_hg = fallback_writer_hg;
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
	ok(query.reader_plan.fallback_writer_hg == -1,
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
	ok(query.reader_plan.fallback_writer_hg == -1,
		"full query reset clears reader plan");
	ok(!query.request_writer_scope.valid(),
		"full query reset clears request writer scope");
	ok(query.wrapped_query_buf.empty(),
		"full query reset clears wrapper buffer");
	ok(query.wait.wait_stage == PolarDB_WaitStage::IDLE,
		"full query reset clears wait state");
	ok(query.dispatch_wrapper_kind == PolarDB_Query_WrapperKind::NONE,
		"full query reset clears dispatch state");
}

static void test_reader_plan_lag_cap_helpers() {
	PolarDB_Query_ReaderPlan plan;

	ok(plan.within_byte_cap(0),
		"reader plan: byte cap disabled accepts missing replica LSN");

	plan.primary_lsn = 200;
	plan.max_lag_bytes = 50;
	ok(!plan.within_byte_cap(0),
		"reader plan: byte cap rejects missing replica LSN");
	ok(!plan.within_byte_cap(149),
		"reader plan: byte cap rejects replica below cap");
	ok(plan.within_byte_cap(150),
		"reader plan: byte cap accepts replica at cap boundary");
	ok(plan.within_byte_cap(200),
		"reader plan: byte cap accepts replica at primary LSN");
	ok(plan.within_byte_cap(201),
		"reader plan: byte cap accepts replica ahead of primary LSN");
}

static void test_transaction_split_state_reset_contract() {
	PolarDB_TransactionSplitState state;

	ok(state.stage == PolarDB_TransactionSplitStage::NONE,
		"transaction split state: default stage is none");
	ok(!state.active(),
		"transaction split state: default state is inactive");
	ok(!state.has_backend_evidence(),
		"transaction split state: default has no backend evidence");

	state.stage = PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	state.xids = "10,11";
	state.primary_lsn = 500;
	state.splittable = true;
	state.wal_pending = true;
	state.blocked = true;

	ok(state.active(),
		"transaction split state: non-none stage is active");
	ok(state.has_backend_evidence(),
		"transaction split state: xids/LSN/RFQ flags count as backend evidence");

	state.reset();
	ok(state.stage == PolarDB_TransactionSplitStage::NONE,
		"transaction split state: reset clears stage");
	ok(!state.active(),
		"transaction split state: reset returns state to inactive");
	ok(state.xids.empty(),
		"transaction split state: reset clears RFQ xids");
	ok(state.primary_lsn == 0,
		"transaction split state: reset clears primary LSN");
	ok(!state.splittable,
		"transaction split state: reset clears splittable flag");
	ok(!state.wal_pending,
		"transaction split state: reset clears WAL-pending flag");
	ok(!state.blocked,
		"transaction split state: reset clears split-blocked flag");
	ok(!state.has_backend_evidence(),
		"transaction split state: reset clears backend evidence");
}

static void test_transaction_split_state_rfq_observation() {
	PolarDB_TransactionSplitState state;

	state.observe_primary_rfq('T', "10,11", true, false, 700, false);
	ok(!state.active(),
		"transaction split observation: disabled policy leaves state inactive");
	ok(!state.has_backend_evidence(),
		"transaction split observation: disabled policy clears backend evidence");

	state.observe_primary_rfq('T', nullptr, false, false, 100, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: open transaction starts on primary");
	ok(state.primary_lsn == 100,
		"transaction split observation: primary LSN is recorded");
	ok(state.xids.empty(),
		"transaction split observation: absent XIDs are not invented");
	ok(!state.splittable,
		"transaction split observation: absent splittable marker stays false");

	state.observe_primary_rfq('T', "10,11", true, false, 200, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_SPLITTABLE,
		"transaction split observation: XIDs plus splittable marker allow split state");
	ok(state.xids == "10,11",
		"transaction split observation: XID list is saved");
	ok(state.primary_lsn == 200,
		"transaction split observation: primary LSN advances");
	ok(state.splittable,
		"transaction split observation: splittable flag is saved");
	ok(!state.wal_pending,
		"transaction split observation: WAL-pending flag stays false");

	state.observe_primary_rfq('T', nullptr, false, true, 150, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: WAL-pending state returns to primary-only");
	ok(state.xids == "10,11",
		"transaction split observation: XIDs are preserved when RFQ omits them mid-transaction");
	ok(state.primary_lsn == 200,
		"transaction split observation: older primary LSN does not move backward");
	ok(!state.splittable,
		"transaction split observation: splittable flag follows latest RFQ");
	ok(state.wal_pending,
		"transaction split observation: WAL-pending flag follows latest RFQ");

	state.observe_primary_rfq('T', "12", true, false, 250, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_SPLITTABLE,
		"transaction split observation: later splittable RFQ can restore split state");
	ok(state.xids == "12",
		"transaction split observation: later XID list replaces earlier list");

	state.blocked = true;
	state.observe_primary_rfq('T', "12", true, false, 260, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: blocked transaction stays primary-only");
	ok(state.blocked,
		"transaction split observation: blocked marker is preserved");

	state.blocked = false;
	state.observe_primary_rfq('E', "13", true, false, 270, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: failed transaction stays primary-only");
	ok(state.primary_lsn == 270,
		"transaction split observation: failed transaction still records primary LSN");

	state.observe_primary_rfq('I', nullptr, false, false, 0, true);
	ok(!state.active(),
		"transaction split observation: idle RFQ resets state");
	ok(state.xids.empty(),
		"transaction split observation: idle RFQ clears XIDs");
}

int main() {
	// 21 named-reset + 6 reader-plan + 13 reset + 24 RFQ-observation checks = 64.
	plan(64);
	test_query_state_named_reset_subsets();
	test_reader_plan_lag_cap_helpers();
	test_transaction_split_state_reset_contract();
	test_transaction_split_state_rfq_observation();
	return exit_status();
}
