/**
 * @file polardb_query_state_unit-t.cpp
 * @brief Unit tests for PolarDB_QueryState named-reset semantics.
 *
 * Domain: scoped per-query reset helpers (reset_reader_plan, reset_wait,
 * clear_reader_route, reset_dispatch_wrapper, reset_for_new_query) and exactly
 * which fields each one clears versus preserves.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

#include <cstring>

// ---- QueryState named-reset subsets ----

static void test_query_state_named_reset_subsets() {
	const int fallback_writer_hg = 20;

	PolarDB_QueryState query;
	query.reader_plan.group_lsn = 700;
	query.reader_plan.max_lag_bytes = 100;
	query.reader_plan.fallback_writer_hg = fallback_writer_hg;
	query.reader_plan.require_replica = true;
	query.reader_wait_spec = PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	query.request_writer_scope = PolarDB_WriterScope{10, 7};
	query.wait.prepare_from_spec(PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS, PolarDB_WaitMode::BEST_EFFORT));
	query.wait.timeout_error = true;
	query.wait.fallback_writer_hg = fallback_writer_hg;
	query.wait.wait_stage = PolarDB_WaitStage::WAITING;
	query.wait.wait_started_at_us = 42;
#if POLARDB_PROFILE
	query.wait_profile.active = true;
	query.wait_profile.prepared_at_us = 41;
	query.wait_profile.target_source =
		PolarDB_WaitProfileTargetSource::OBSERVED;
#endif // POLARDB_PROFILE
	query.original_query = "SELECT original";
	query.wrapped_query_buf = "wrapped";
	query.dispatch_wrapper_stmts = 3;
	query.dispatch_wrapper_kind = PolarDB_Query_WrapperKind::CONSISTENCY_WAIT;
	query.keep_session_lsn = true;
	query.wait_bypass_target = 500;

	query.reset_reader_plan();
	ok(query.reader_plan.fallback_writer_hg == -1 &&
			!query.reader_plan.require_replica,
		"reader-target reset clears reader plan");
	ok(!query.reader_wait_spec.has_wait(),
		"reader-target reset clears pending reader wait input");
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
	ok(query.keep_session_lsn,
		"reader-target reset keeps the per-query LSN choice");

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
	ok(query.keep_session_lsn,
		"wait reset keeps the per-query LSN choice");
	ok(query.wait_bypass_target == 500,
		"wait reset preserves the target already confirmed by the backend");
	ok(query.original_query == "SELECT original",
		"wait reset preserves the original SQL through RequestEnd");
#if POLARDB_PROFILE
	ok(!query.wait_profile.active,
		"wait reset clears profile-only wait correlation state");
	ok(query.wait_profile.target_source ==
			PolarDB_WaitProfileTargetSource::UNKNOWN,
		"wait reset clears profile-only target attribution");
	#endif // POLARDB_PROFILE

	query.reader_plan.fallback_writer_hg = fallback_writer_hg;
	query.reader_wait_spec = PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	query.wait.prepare_from_spec(PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS, PolarDB_WaitMode::BEST_EFFORT));
	query.wait_bypass_target = 500;
	query.clear_reader_route();
	ok(query.reader_plan.fallback_writer_hg == -1 &&
			!query.reader_wait_spec.has_wait() &&
			!query.wait.spec.has_wait() &&
			query.wait_bypass_target == 0,
		"reader-route clear removes the reader plan, wait, and satisfied target");
	ok(query.request_writer_scope.valid() && query.keep_session_lsn,
		"reader-route clear preserves request scope and response LSN choice");

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
	ok(query.keep_session_lsn,
		"dispatch-wrapper reset keeps the per-query LSN choice");

	query.reset_for_new_query();
	// full query reset clears target, request scope, and wrapper buffer.
	ok(query.reader_plan.fallback_writer_hg == -1,
		"full query reset clears reader plan");
	ok(!query.request_writer_scope.valid(),
		"full query reset clears request writer scope");
	ok(query.wrapped_query_buf.empty(),
		"full query reset clears wrapper buffer");
	ok(query.original_query.empty(),
		"full query reset clears the stable original SQL");
	ok(query.wait.wait_stage == PolarDB_WaitStage::IDLE,
		"full query reset clears wait state");
	ok(query.dispatch_wrapper_kind == PolarDB_Query_WrapperKind::NONE,
		"full query reset clears dispatch state");
	ok(!query.keep_session_lsn,
		"full query reset clears the per-query LSN choice");
	ok(query.wait_bypass_target == 0,
		"full query reset clears the confirmed backend target");
}

static void test_query_state_extended_message_reset() {
	PolarDB_QueryState query;
	query.request_writer_scope = PolarDB_WriterScope{10, 7};
	query.reader_plan.fallback_writer_hg = 10;
	query.reader_plan.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	query.reader_plan.consistency_mode =
		PolarDB_ConsistencyMode::SESSION_LSN;
	query.reader_wait_spec = PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	query.profile_enabled = true;
	query.txn_split_enabled = true;
	query.backend_isolation_status_needed = true;
	query.reader_retry_attempts = 1;
	query.wait.prepare_from_spec(PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT));
	query.original_query = "SELECT original";
	query.wrapped_query_buf = "temporary";
	query.keep_session_lsn = true;
	query.wait_bypass_target = 500;

	query.reset_between_extended_messages();

	ok(query.request_writer_scope.matches(PolarDB_WriterScope{10, 7}),
		"extended-message reset preserves writer scope");
	ok(query.reader_plan.fallback_writer_hg == 10 &&
			query.reader_plan.read_target ==
				static_cast<int>(PolarDB_ReadTarget::REPLICA),
		"extended-message reset preserves selected reader policy");
	ok(query.reader_wait_spec.has_wait() &&
			query.reader_wait_spec.target == 500,
		"extended-message reset preserves reader-selection wait input");
	ok(query.effective_consistency_mode ==
			static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN) &&
			query.profile_enabled,
		"extended-message reset preserves response consistency policy");
	ok(query.txn_split_enabled &&
			query.backend_isolation_status_needed,
		"extended-message reset preserves request policy flags");
	ok(query.reader_retry_attempts == 0,
		"extended-message reset clears retry progress");
	ok(!query.wait.spec.has_wait() && query.original_query.empty() &&
			query.wrapped_query_buf.empty(),
		"extended-message reset clears wait and query buffers");
	ok(!query.keep_session_lsn && query.wait_bypass_target == 0,
		"extended-message reset clears response-only state");
}

static void test_query_wait_transitions() {
	PolarDB_QueryState query;
	const PolarDB_WaitSpec wait_spec = PolarDB_WaitSpec::from_lsn(
		500, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::STRICT);
	query.wait_bypass_target = 400;

	query.begin_wait(wait_spec, 42, 10);
	ok(query.wait.spec.target == 500 &&
			query.wait.wait_stage == PolarDB_WaitStage::WAITING &&
			query.wait.wait_started_at_us == 42 &&
			query.wait.fallback_writer_hg == 10,
		"wait transition: begin records the complete active wait");
	ok(query.wait_bypass_target == 0,
		"wait transition: begin clears the satisfied target");

	query.wait.timeout_error = true;
	query.mark_wait_satisfied(500);
	ok(!query.wait.spec.has_wait() &&
			query.wait.wait_stage == PolarDB_WaitStage::IDLE &&
			query.wait.wait_started_at_us == 0 &&
			!query.wait.timeout_error &&
			query.wait.fallback_writer_hg == -1,
		"wait transition: satisfied target clears active wait state");
	ok(query.wait_bypass_target == 500,
		"wait transition: satisfied target records the target");
}

static void test_reader_plan_lag_cap_helpers() {
	PolarDB_Query_ReaderPlan plan;

	ok(plan.within_byte_cap(0),
		"reader plan: byte cap disabled accepts missing replica LSN");

	plan.group_lsn = 200;
	plan.max_lag_bytes = 50;
	ok(!plan.within_byte_cap(0),
		"reader plan: byte cap rejects missing replica LSN");
	ok(!plan.within_byte_cap(149),
		"reader plan: byte cap rejects replica below cap");
	ok(plan.within_byte_cap(150),
		"reader plan: byte cap accepts replica at cap boundary");
	ok(plan.within_byte_cap(200),
		"reader plan: byte cap accepts replica at group LSN");
	ok(plan.within_byte_cap(201),
		"reader plan: byte cap accepts replica ahead of group LSN");
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
	state.failed = true;
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
	ok(!state.failed,
		"transaction split state: reset clears failed-transaction flag");
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
	ok(!state.failed,
		"transaction split observation: active transaction is not marked failed");

	state.observe_primary_rfq('T', "", true, false, 150, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: empty-XID marker remains in pre-write stage");
	ok(state.xids.empty(),
		"transaction split observation: empty-XID marker does not invent transaction IDs");
	ok(state.splittable,
		"transaction split observation: explicit empty-XID marker is preserved");
	ok(state.primary_lsn == 150,
		"transaction split observation: pre-write marker advances the primary LSN");

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

	state.observe_primary_rfq('T', "", true, false, 255, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: explicit empty XIDs return to pre-write stage");
	ok(state.xids.empty(),
		"transaction split observation: explicit empty XIDs clear an older list");
	ok(state.splittable,
		"transaction split observation: empty-XID split authorization is retained");
	ok(state.primary_lsn == 255,
		"transaction split observation: empty-XID RFQ advances the primary LSN");

	state.blocked = true;
	state.observe_primary_rfq('T', "12", true, false, 260, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: blocked transaction stays primary-only");
	ok(state.blocked,
		"transaction split observation: blocked marker is preserved");

	state.blocked = false;
	state.observe_primary_rfq('E', "13", true, true, 270, true);
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY,
		"transaction split observation: failed transaction stays primary-only");
	ok(state.primary_lsn == 270,
		"transaction split observation: failed transaction still records primary LSN");
	ok(state.xids == "13",
		"transaction split observation: failed RFQ retains its XIDs for primary handling");
	ok(!state.splittable,
		"transaction split observation: failed RFQ cannot authorize reader routing");
	ok(state.failed,
		"transaction split observation: failed RFQ is retained as an eligibility fact");

	state.observe_primary_rfq('T', "13", false, true, 275, true);
	ok(!state.failed,
		"transaction split observation: recovered transaction clears failed status");

	state.observe_primary_rfq('I', nullptr, false, false, 0, true);
	ok(!state.active(),
		"transaction split observation: idle RFQ resets state");
	ok(state.xids.empty(),
		"transaction split observation: idle RFQ clears XIDs");
}

static void test_transaction_split_read_transitions() {
	PolarDB_TransactionSplitState state;
	state.observe_primary_rfq('T', "", true, false, 100, true);

	state.begin_split_read();
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_SPLIT_READ_ACTIVE &&
			state.was_splittable,
		"transaction split read: begin records the active read");

	state.complete_split_read();
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_SPLITTABLE &&
			state.was_splittable && state.did_split && !state.blocked,
		"transaction split read: completion restores splittable state");

	state.begin_split_read();
	state.fail_split_read();
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_ON_PRIMARY &&
			state.blocked,
		"transaction split read: failure keeps later reads on the writer");

	state.begin_split_read();
	ok(state.stage == PolarDB_TransactionSplitStage::TXN_SPLIT_READ_ACTIVE &&
			!state.blocked,
		"transaction split read: retry begins without the failure block");
}

static void test_no_write_xids_marker() {
	ok(polardb_rfq_is_prewrite_split_candidate('T', "", true, false),
		"no-write-XID marker: active split-safe transaction with empty XIDs matches");
	ok(!polardb_rfq_is_prewrite_split_candidate('I', "", true, false),
		"no-write-XID marker: idle status does not match");
	ok(!polardb_rfq_is_prewrite_split_candidate('T', nullptr, true, false),
		"no-write-XID marker: missing XID payload does not match");
	ok(!polardb_rfq_is_prewrite_split_candidate('T', "10", true, false),
		"no-write-XID marker: a write XID does not match");
	ok(!polardb_rfq_is_prewrite_split_candidate('T', "", false, false),
		"no-write-XID marker: split denial does not match");
	ok(!polardb_rfq_is_prewrite_split_candidate('T', "", true, true),
		"no-write-XID marker: pending WAL does not match");
}

int main() {
#if POLARDB_PROFILE
	plan(113);
#else
	plan(111);
#endif // POLARDB_PROFILE
	test_query_state_named_reset_subsets();
	test_query_state_extended_message_reset();
	test_query_wait_transitions();
	test_reader_plan_lag_cap_helpers();
	test_transaction_split_state_reset_contract();
	test_transaction_split_state_rfq_observation();
	test_transaction_split_read_transitions();
	test_no_write_xids_marker();
	return exit_status();
}
