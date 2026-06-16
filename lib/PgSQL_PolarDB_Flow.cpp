/**
 * @file PgSQL_PolarDB_Flow.cpp
 * @brief PolarDB read routing pipeline: collect -> plan -> execute -> process_result.
 *
 * This file holds the four request/response stage functions. The pure decision
 * helpers they call live in their own domain files:
 *   - Consistency.cpp: polardb_resolve_*(), PolarDB_Query_WaitPlan::build_consistency()
 *   - Wrap.cpp:        building and injecting the SET-statement prefix that makes
 *                      the replica wait for the session's target LSN
 *
 * The read-your-writes path works like this. For an autocommit, single-statement,
 * replica-eligible read, polardb_plan() returns REPLICA_WITH_WAIT. polardb_execute()
 * then records the wait intent and a copy of the original query, but does not yet
 * build any SQL. The read is sent to a reader hostgroup, and the actual wrapped
 * query is built exactly once, later (at ASYNC_IDLE, by the finalize step), after
 * the backend connection exists. The
 * wrapper is three SET statements in front of the user query: a consistency mode,
 * a wait timeout, and the wait step itself ("SET polar_xact_split_wait_lsn =
 * '<lsn>'"). On that last statement the backend blocks until its replay position
 * reaches the target LSN, which is what makes the replica read see the session's
 * own earlier writes.
 *
 * Rationale and the end-to-end traces: see doc/polardb-arch/06-ROUTING-PIPELINE.md.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Connection.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Query_Processor.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

#include <cctype>
#include <cstring>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY

/**
 * @brief Read the current writer hostgroup and epoch for one hostgroup.
 *
 * Looks up the PolarDB topology for @p hostgroup_id and, if it belongs to a
 * PolarDB replication group, fills @p writer_scope with that group's writer
 * hostgroup and its current epoch. The epoch identifies the live writer; it
 * changes on failover so that LSN state from an old timeline can be discarded.
 *
 * @return true if a valid writer scope was written; false for a non-PolarDB
 *         hostgroup or one with no writer epoch (caller treats this as "no scope").
 */
static bool polardb_snapshot_writer_for_hg(
		int hostgroup_id, PolarDB_WriterScope* writer_scope) {
	if (hostgroup_id < 0 || !writer_scope) {
		return false;
	}

	auto hg_config = PgHGM->get_polardb_hg_config((unsigned int)hostgroup_id);
	if (!hg_config.is_polardb_hostgroup || !hg_config.writer_epoch) {
		return false;
	}

	writer_scope->hg = hg_config.writer_hostgroup;
	// Acquire load: pairs with the failover writer-epoch bump so we never read a
	// torn or stale epoch alongside the hostgroup it scopes.
	writer_scope->epoch = hg_config.writer_epoch->load(std::memory_order_acquire);
	return true;
}

/**
 * @brief Detect "manual routing" from the query rules and report its writer scope.
 *
 * A query rule is doing manual routing when it pins a destination hostgroup and
 * leaves replica_eligible unset (-1). In that case the operator is choosing the
 * backend, so the PolarDB pipeline must not override the route or add a wait
 * wrapper. When replica_eligible is set (0 or 1), the rule opted into automatic
 * routing even if it also names a default destination, so this is NOT manual mode.
 *
 * @param scope_hg         Out: hostgroup whose writer epoch should scope any LSN
 *                         state captured for this manual request (-1 if not manual).
 * @param dest_hg          Out: the rule's destination hostgroup (-1 if none).
 * @param replica_eligible Out: the rule's replica_eligible value (-1/0/1).
 * @return true if this is manual routing; false otherwise.
 *
 * Rationale for the both-set case (destination + replica_eligible=1):
 * see doc/polardb-arch/06-ROUTING-PIPELINE.md (section E.5).
 */
bool PgSQL_Session::polardb_manual_route_scope(
		int* scope_hg, int* dest_hg, int* replica_eligible) const {
	const int rule_replica_eligible = qpo ? qpo->replica_eligible : -1;
	const int rule_dest_hg = qpo ? qpo->destination_hostgroup : -1;
	if (replica_eligible) {
		*replica_eligible = rule_replica_eligible;
	}
	if (dest_hg) {
		*dest_hg = rule_dest_hg;
	}
	// Not manual: either the rule opted into auto routing (replica_eligible set),
	// or it named no destination at all (nothing to honor as a manual route).
	if (rule_replica_eligible >= 0 || rule_dest_hg < 0) {
		if (scope_hg) {
			*scope_hg = -1;
		}
		return false;
	}
	if (scope_hg) {
		// Inside a sticky (persistent-hostgroup) transaction the connection is
		// already pinned, so scope LSN state to where the session currently is;
		// otherwise scope it to the manual destination the rule selected.
		*scope_hg = (transaction_persistent_hostgroup >= 0)
			? current_hostgroup : rule_dest_hg;
	}
	return true;
}

bool PgSQL_Session::polardb_query_cache_disabled_for_current_rule() const {
	if (!qpo || qpo->replica_eligible != 1 ||
			!PgHGM->status.polardb_active.load(std::memory_order_relaxed)) {
		return false;
	}

	// Cache lookup happens before collect()/plan(), so no reader plan exists yet.
	// Use the same cheap inputs the planner will later read, and disable cache
	// only when this automatic PolarDB route may need writer routing or a replica
	// wait for the current session.
	int route_hg = current_hostgroup;
	if (qpo->destination_hostgroup >= 0 && transaction_persistent_hostgroup == -1) {
		route_hg = qpo->destination_hostgroup;
	}
	if (route_hg < 0) {
		return false;
	}

	const auto hg_config = PgHGM->get_polardb_hg_config((unsigned int)route_hg);
	if (!hg_config.is_polardb_hostgroup) {
		return false;
	}

	const int mode = polardb_resolve_consistency_mode(
		polardb_config.session_consistency_mode,
		hg_config.policy.consistency_mode,
		pgsql_thread___polardb_consistency_mode);
	switch (polardb_consistency_from_int(mode)) {
	case PolarDB_ConsistencyMode::OFF:
		return false;
	case PolarDB_ConsistencyMode::PRIMARY_ONLY:
		return true;
	case PolarDB_ConsistencyMode::SESSION_LSN:
		break;
	}

	// In LSN mode, cache is safe until the session has an LSN target or a missing
	// LSN latch. PRIMARY baseline can create a first-read target from the writer
	// mirror, so it also bypasses cache before the planner runs.
	if (polardb_session_consistency.target() > 0 ||
			polardb_session_consistency.write_unknown ||
			polardb_session_consistency.observed_unknown) {
		return true;
	}

	return polardb_session_lsn_baseline_from_int(
		pgsql_thread___polardb_session_lsn_baseline) ==
			PolarDB_SessionLsnBaseline::PRIMARY;
}

/**
 * @brief Record the writer scope this request runs under, for later LSN attribution.
 *
 * Snapshots the writer hostgroup+epoch for @p scope_hg into the per-query state.
 * The response path uses it to decide whether an RFQ LSN belongs to the current
 * writer timeline before it touches session LSN state. Automatic routes capture
 * this during collect(); this helper exists for the manual-routing path, which
 * skips collect() but still needs the scope attached.
 *
 * @return true if a valid scope was captured; false if @p scope_hg is not a
 *         PolarDB hostgroup (per-query scope is left reset).
 */
bool PgSQL_Session::polardb_capture_request_writer_scope(int scope_hg) {
	polardb_query.request_writer_scope.reset();

	PolarDB_WriterScope request_scope;
	if (!polardb_snapshot_writer_for_hg(scope_hg, &request_scope)) {
		return false;
	}

	polardb_query.request_writer_scope = request_scope;
	return true;
}

/**
 * @brief Bind the session's LSN state to a writer scope, discarding stale state.
 *
 * The session's write_lsn / observed_lsn / missing-LSN latches are only meaningful
 * for one writer hostgroup and epoch. If the current request runs under a different
 * writer scope, the old LSN values name a position on another replication group or
 * an old timeline (for example after a failover bumped the writer epoch). Waiting
 * on them would be meaningless or wrong, so this function clears them and then
 * stamps the new scope onto the session.
 *
 * It clears (and re-binds) when either:
 *   - the session already had a scope and it does not match @p writer_scope, or
 *   - the session carried LSN state with no scope attached yet.
 *
 * Invariant upheld: session LSN state is never compared against, or waited on,
 * across a writer-scope change. Caller passes @p stage only for trace context.
 * Rationale: see doc/polardb-arch/06-ROUTING-PIPELINE.md (writer-epoch scoping).
 */
static void polardb_scope_session_lsn(
		PgSQL_Session* sess,
		const PolarDB_WriterScope& writer_scope,
		const char* stage) {
	if (!sess || !writer_scope.valid()) {
		return;
	}

	const bool had_lsn_state =
		sess->polardb_session_consistency.has_lsn_state();
	const bool scope_attached =
		sess->polardb_session_consistency.writer_scope.valid();
	// Scope changed under us: same field could be a different group whose epoch
	// number collides, so matches() compares hostgroup AND epoch, not the scalar.
	const bool scope_mismatch = scope_attached &&
		!sess->polardb_session_consistency.writer_scope.matches(writer_scope);
	// LSN state exists but was never bound to a scope (e.g. first scoping after a
	// manual route captured a target). Treat as needing (re)binding.
	const bool unscoped_state =
		!scope_attached && had_lsn_state;

	if (scope_mismatch || unscoped_state) {
		POLARDB_TRACE(
			"PolarDB %s: scope session LSN state to writer_hg=%d "
			"writer_epoch=%lu (previous_valid=%d previous_hg=%d "
			"previous_epoch=%lu write_lsn=%lu observed_lsn=%lu "
			"write_unknown=%d observed_unknown=%d)\n",
			stage ? stage : "LSN",
			writer_scope.hg,
			(unsigned long)writer_scope.epoch,
			scope_attached ? 1 : 0,
			sess->polardb_session_consistency.writer_scope.hg,
			(unsigned long)sess->polardb_session_consistency.writer_scope.epoch,
			(unsigned long)sess->polardb_session_consistency.write_lsn,
			(unsigned long)sess->polardb_session_consistency.observed_lsn,
			sess->polardb_session_consistency.write_unknown ? 1 : 0,
			sess->polardb_session_consistency.observed_unknown ? 1 : 0);
		if (had_lsn_state) {
			// Drop write/observed LSNs and both missing-LSN latches; they belong
			// to the old scope. Also clear the one-shot degraded-route warning so a
			// fresh degradation under the new scope is reported again. Count only
			// when state was actually discarded (not on the first bind).
			sess->polardb_session_consistency.reset_lsn_state();
			sess->polardb_rfq_degraded_route_warning_sent = false;
			PgHGM->status.polardb_session_target_epoch_reset.fetch_add(
				1, std::memory_order_relaxed);
		}
	}

	// Stamp the (possibly new) scope so later RFQ LSNs are attributed correctly.
	sess->polardb_session_consistency.writer_scope = writer_scope;
}

// ======================================================================
// Stage 1: collect — snapshot routing inputs and repair stale writer epochs
// ======================================================================

/**
 * @brief Collect every routing input into an immutable per-query snapshot.
 *
 * Fills PolarDB_Query_RouteCtx by reading session, HostGroups_Manager and thread
 * state. polardb_plan() then decides routing purely from this snapshot.
 *
 * Side effect (the one mutation in this stage): if the replication group's writer
 * scope changed since this session last collected a PolarDB context, it first
 * clears the session's LSN targets and missing-LSN latches, then copies them into
 * @p route_ctx. Those old targets name a position on another replication group or
 * an old writer timeline and must not be waited on. See polardb_scope_session_lsn().
 *
 * @param route_ctx               Output context; every field is filled on return.
 * @param current_hg              Current hostgroup from the query processor.
 * @param qpo_replica_eligible    replica_eligible from the matched query rule (-1/0/1).
 * @param qpo_force_primary_hint  true if the query carried a leading SQL comment with route=primary.
 */
void PgSQL_Session::polardb_collect(PolarDB_Query_RouteCtx& route_ctx,
                                    int current_hg,
                                    int qpo_replica_eligible,
                                    bool qpo_force_primary_hint)
{
	route_ctx = PolarDB_Query_RouteCtx{};  // zero-init all fields

	auto hg_config = PgHGM->get_polardb_hg_config(current_hg);
	route_ctx.is_polar_hg = hg_config.is_polardb_hostgroup;
	if (!route_ctx.is_polar_hg) {
		POLARDB_TRACE(
			"PolarDB COLLECT: hg=%d is_polar=false, skip\n", current_hg);
		return;  // fast path — non-PolarDB HG
	}

	// Hostgroup topology
	auto policy = hg_config.policy;
	route_ctx.writer_scope.hg = hg_config.writer_hostgroup;
	if (route_ctx.writer_scope.hg < 0) route_ctx.writer_scope.hg = current_hg;  // already the writer HG
	route_ctx.reader_hg = hg_config.reader_hostgroup;
	if (hg_config.writer_epoch) {
		route_ctx.writer_scope.epoch = hg_config.writer_epoch->load(std::memory_order_acquire);
		polardb_query.request_writer_scope = route_ctx.writer_scope;
	}

	// Consistency mode resolution (no mutation)
	route_ctx.effective_consistency_mode = polardb_resolve_consistency_mode(
		polardb_config.session_consistency_mode,
		policy.consistency_mode,
		pgsql_thread___polardb_consistency_mode);
	const bool lsn_mode = route_ctx.effective_consistency_mode >= 0 &&
		polardb_consistency_from_int(route_ctx.effective_consistency_mode) ==
			PolarDB_ConsistencyMode::SESSION_LSN;

	// Wait config + lag-cap input
	route_ctx.wait_timeout_ms = polardb_resolve_wait_timeout_ms(policy.lsn_wait_timeout_ms);
	route_ctx.wait_timeout_mode = pgsql_thread___polardb_wait_timeout_mode;
	route_ctx.route_rfq_policy = pgsql_thread___polardb_route_rfq_policy;
	route_ctx.session_lsn_baseline = pgsql_thread___polardb_session_lsn_baseline;
	route_ctx.max_lag_bytes = policy.max_lag_bytes;

	// Session LSN positions. SESSION_LSN waits on max(write_lsn, observed_lsn).
	// Re-bind the session LSN state to the current writer scope first, so a scope
	// change clears both components and both missing-LSN latches before we snapshot
	// them. The snapshot taken into route_ctx is then valid for this writer.
	if (polardb_query.request_writer_scope.valid()) {
		polardb_scope_session_lsn(this, route_ctx.writer_scope, "COLLECT");
	}
	route_ctx.session = polardb_session_consistency;

	// Query eligibility (from query rules)
	route_ctx.replica_eligible = (qpo_replica_eligible == 1);

	// Multi-statement check. A wrapped read must be a single statement: the wrapper
	// prepends a fixed number of SET statements, and the connection layer counts
	// exactly that many result sets before it forwards the user result. A second
	// user statement would shift that count, so a multi-statement read is never
	// offloaded to a replica. Done here as a direct scan because regex-based
	// multi-statement detection in query rules is not reliable enough to trust.
	// Only scan when it could matter (LSN mode and an eligible read).
	route_ctx.is_multi_statement = false;
	if (lsn_mode && route_ctx.replica_eligible && CurrentQuery.QueryPointer) {
		route_ctx.is_multi_statement = polardb_query_has_multiple_statements(
			(const char*)CurrentQuery.QueryPointer, CurrentQuery.QueryLength);
	}

	// Protocol detection. The wait wrapper is plain SQL text, so it can only be
	// prepended to a simple-query packet; extended protocol cannot be wrapped.
	route_ctx.is_extended_protocol = (extended_query_phase != EXTQ_PHASE_IDLE);

	// Transaction state. The read-your-writes wait covers autocommit reads only,
	// so any read inside an explicit transaction is routed to the writer. The
	// backend reports an open/aborted transaction ('T'/'E') via active_transactions.
	route_ctx.in_transaction = (active_transactions > 0);

	// Per-query routing hint parsed from the query's first comment
	// (a leading SQL comment "/* route=primary */"); plan turns it into a force-writer route.
	route_ctx.force_primary_hint = qpo_force_primary_hint;

	{
		[[maybe_unused]] const char* query_text =
			CurrentQuery.QueryPointer ? (const char*)CurrentQuery.QueryPointer : "(null)";
		int query_len = CurrentQuery.QueryPointer ? (int)CurrentQuery.QueryLength : 0;
		if (query_len > 120) query_len = 120;
		POLARDB_TRACE(
			"PolarDB COLLECT: hg=%d writer=%d reader=%d mode=%d "
			"replica_eligible=%d multi_stmt=%d extended=%d in_txn=%d "
			"write_lsn=%lu observed_lsn=%lu write_lsn_unknown=%d "
			"observed_lsn_unknown=%d timeout_ms=%u timeout_mode=%d "
			"rfq_policy=%d lsn_baseline=%d writer_hg=%d writer_epoch=%lu Q='%.*s'\n",
			current_hg, route_ctx.writer_scope.hg, route_ctx.reader_hg,
			route_ctx.effective_consistency_mode,
			route_ctx.replica_eligible, route_ctx.is_multi_statement, route_ctx.is_extended_protocol,
			route_ctx.in_transaction, (unsigned long)route_ctx.session.write_lsn,
			(unsigned long)route_ctx.session.observed_lsn,
			route_ctx.session.write_unknown ? 1 : 0,
			route_ctx.session.observed_unknown ? 1 : 0,
			route_ctx.wait_timeout_ms, route_ctx.wait_timeout_mode,
			route_ctx.route_rfq_policy, route_ctx.session_lsn_baseline,
			route_ctx.writer_scope.hg, (unsigned long)route_ctx.writer_scope.epoch,
			query_len, query_text);
	}
}

// ======================================================================
// Stage 2: plan — deterministic LSN routing decision
// ======================================================================

/**
 * @brief Whether a reader hostgroup is within the configured lag-cap safety
 *        bound.
 *
 * The lag cap is a SAFETY check, not a consistency gate: the
 * polar_xact_split_wait_lsn statement in the wrapped query is what actually waits
 * for the session target. The cap only avoids picking a replica so far behind
 * that the wait would likely time out.
 *
 * Rule:
 *   - max_lag_bytes <= 0: no lag-cap condition — any online reader is fine.
 *   - max_lag_bytes  > 0: attach the cap and current primary mirror to the
 *     query reader plan. Per-reader freshness and byte-lag are enforced later,
 *     when the actual candidate set is scanned.
 *
 * @param route_ctx Routing context (provides reader_hg + max_lag_bytes).
 */
void PgSQL_Session::polardb_reader_lag_plan(
		PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx)
{
	// Resolve the byte cap: HG policy (>=0) overrides, else the global thread var.
	int max_lag = (route_ctx.max_lag_bytes >= 0) ? route_ctx.max_lag_bytes
	                                        : pgsql_thread___polardb_lag_bytes;
	if (max_lag <= 0) {
		// No lag-cap condition — the wrapped query's wait gate
		// (SET polar_xact_split_wait_lsn) is what enforces consistency.
		return;
	}

	// Cap enabled: require a known writer-HG primary LSN mirror. Per-reader
	// freshness and byte-lag are enforced later by backend acquisition, so the
	// chosen server is the server that satisfied the cap.
	uint64_t primary_lsn = PgHGM->get_polardb_primary_lsn(route_ctx.writer_scope.hg);
	plan.reader.primary_lsn = primary_lsn;
	plan.reader.max_lag_bytes = max_lag;
	POLARDB_TRACE(
		"PolarDB LAG-CAP: primary=%lu max=%d, acquisition will enforce per-reader cap\n",
		(unsigned long)primary_lsn, max_lag);
}

/**
 * @brief Deterministic LSN routing decision.
 *
 * Decision tree (mode = resolved consistency mode, OFF / LSN / PRIMARY only):
 *   1. Fast paths: non-PolarDB HG, no reader HG, not replica-eligible.
 *   2. Mode decisions: PRIMARY -> force writer; OFF -> passthrough (no reroute).
 *   3. Core rule: explicit txn / multi-statement -> writer (the LSN feature is
 *      autocommit + simple-query only).
 *      Extended protocol normally keeps regular ProxySQL qpo routing and does
 *      not enter this planner; if it does, the defensive branch below fails
 *      closed to the writer instead of producing a wait wrapper.
 *   4. Consistency subdecision: build a query consistency snapshot and ask
 *      PolarDB_Query_WaitPlan::build_consistency() for the wait payload. The final
 *      route still belongs to this function.
 *   5. Unknown-LSN rules: missing write or observed RFQ latches are handled
 *      before building a wait target, according to the RFQ route policy.
 *   6. Session target rule: protected reads wait on the session consistency
 *      target, with the configured baseline providing an initial
 *      observed/primary target for first reads. If there is no target, the
 *      reader is trivially consistent: passthrough, no wait.
 *   7. Lag-cap rule: set reader metadata; backend acquisition picks a reader
 *      within the cap or redirects this query to the writer.
 *
 * No mutations. Reads HGM only for primary-baseline and lag-cap snapshots.
 *
 * @param route_ctx Immutable routing context from polardb_collect().
 * @return Routing plan consumed by polardb_execute().
 */
PolarDB_Query_RoutePlan PgSQL_Session::polardb_plan(const PolarDB_Query_RouteCtx& route_ctx)
{
	PolarDB_Query_RoutePlan plan;

	// --- Fast paths ---
	if (!route_ctx.is_polar_hg) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		POLARDB_TRACE(
			"PolarDB PLAN: not a PolarDB HG -> PASSTHROUGH\n");
		return plan;                                              // non-PolarDB HG
	}
	if (route_ctx.reader_hg < 0) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB PLAN: no reader_hg configured -> PASSTHROUGH writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}
	if (!route_ctx.replica_eligible) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.writer_scope.hg;                    // query rule did not opt in
		POLARDB_TRACE(
			"PolarDB PLAN: replica_eligible=false -> PASSTHROUGH writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}

	// --- Level 0: routing hint override (/* route=primary */) ---
	// Placed after the eligibility fast-paths (a non-eligible read is already
	// writer-bound) and before the mode decisions, so an explicit per-query hint pins a
	// replica-eligible read to the writer regardless of consistency mode / RYW / lag.
	if (route_ctx.force_primary_hint) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::HINT_PRIMARY);
		POLARDB_TRACE(
			"PolarDB PLAN: route=primary hint -> FORCE_PRIMARY writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}

	// --- Mode decisions ---
	PolarDB_ConsistencyMode mode = PolarDB_ConsistencyMode::OFF;
	if (route_ctx.effective_consistency_mode >= 0) {
		mode = polardb_consistency_from_int(route_ctx.effective_consistency_mode);
	}

	if (mode == PolarDB_ConsistencyMode::PRIMARY_ONLY) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::MODE_PRIMARY);
		POLARDB_TRACE(
			"PolarDB PLAN: mode=PRIMARY -> FORCE_PRIMARY writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}
	// mode=off: PolarDB consistency DISABLED — no wait wrapping AND no PolarDB
	// reroute. Leave routing to the query rules (target_hg = -1 means the caller
	// does NOT override current_hostgroup). NOT a reader force-route.
	if (mode == PolarDB_ConsistencyMode::OFF) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = -1;
		POLARDB_TRACE(
			"PolarDB PLAN: mode=OFF -> PASSTHROUGH (query rules own routing)\n");
		return plan;
	}

	// Autocommit-only rule: explicit transactions and unsafe query shapes stay on the writer.
	auto required_reason =
		polardb_writer_required_reason(route_ctx.in_transaction, route_ctx.is_multi_statement);
	if (required_reason != PolarDB_Query_RoutePlan::RouteActionReason::NONE) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg, required_reason);
		POLARDB_TRACE(
			"PolarDB PLAN: hard query-shape guard reason=%d -> FORCE_PRIMARY writer=%d\n",
			(int)plan.action_reason, route_ctx.writer_scope.hg);
		return plan;
	}

	// A prior RFQ finished without an LSN while write or observed session state
	// needed to be latched. In LSN mode, ProxySQL cannot build the exact wait
	// target for later automatic reads. Do not guess from monitor/global LSN:
	// apply the RFQ route policy only after hard query-shape guards have had a
	// chance to force the writer.
	//
	// Extended protocol is different: it carries no wait wrapper, and local
	// Parse/Bind completions can bypass the normal result-wire notice flush.
	// Therefore best_effort degradation is disabled there; an unknown-target
	// extended read forces the writer rather than risk a silent stale result,
	// until a dedicated extended-protocol wait model exists.
	if (route_ctx.session.write_unknown) {
		plan = PolarDB_Query_RoutePlan::rfq_unavailable(
				PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
				route_ctx.route_rfq_policy,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				!route_ctx.is_extended_protocol);
		POLARDB_TRACE(
			"PolarDB PLAN: session write LSN unknown policy=%d -> action=%d target=%d\n",
			route_ctx.route_rfq_policy, (int)plan.action, plan.target_hg);
		return plan;
	}
	if (route_ctx.session.observed_unknown) {
		plan = PolarDB_Query_RoutePlan::rfq_unavailable(
				PolarDB_Query_RoutePlan::RouteActionReason::OBSERVED_LSN_UNKNOWN,
				route_ctx.route_rfq_policy,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				!route_ctx.is_extended_protocol);
		POLARDB_TRACE(
			"PolarDB PLAN: session observed LSN unknown policy=%d -> action=%d target=%d\n",
			route_ctx.route_rfq_policy, (int)plan.action, plan.target_hg);
		return plan;
	}
	// Consistency subdecision. The session target is max(write, observed),
	// optionally seeded from the configured baseline, with no global-LSN fallback.
	bool primary_lsn_unknown = false;
	uint64_t primary_lsn = 0;
	if (polardb_session_lsn_baseline_from_int(route_ctx.session_lsn_baseline) ==
			PolarDB_SessionLsnBaseline::PRIMARY &&
			route_ctx.session.target() == 0) {
		primary_lsn = PgHGM->get_polardb_primary_lsn(route_ctx.writer_scope.hg);
	}
	uint64_t session_lsn = route_ctx.session.target_with_baseline(
		route_ctx.session_lsn_baseline,
		primary_lsn,
		&primary_lsn_unknown);
	if (primary_lsn_unknown) {
		plan = PolarDB_Query_RoutePlan::rfq_unavailable(
				PolarDB_Query_RoutePlan::RouteActionReason::PRIMARY_LSN_UNKNOWN,
				route_ctx.route_rfq_policy,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				!route_ctx.is_extended_protocol);
		POLARDB_TRACE(
			"PolarDB PLAN: primary baseline LSN unknown policy=%d -> action=%d target=%d\n",
			route_ctx.route_rfq_policy, (int)plan.action, plan.target_hg);
		return plan;
	}
	PolarDB_WaitMode wait_mode =
		(route_ctx.wait_timeout_mode == (int)PolarDB_WaitMode::STRICT)
		? PolarDB_WaitMode::STRICT : PolarDB_WaitMode::BEST_EFFORT;

	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		mode, session_lsn, route_ctx.wait_timeout_ms, wait_mode, /*prefer_replica=*/true);

	if (wait_plan.route_hint == PolarDB_Query_ConsistencyRouteHint::PRIMARY) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::MODE_PRIMARY);
		POLARDB_TRACE(
			"PolarDB PLAN: consistency helper requested PRIMARY -> FORCE_PRIMARY writer=%d\n",
			route_ctx.writer_scope.hg);
		return plan;
	}
	// REPLICA means the consistency policy allows a reader. It is not enough to
	// route by itself: this planner still applies query-shape, reader availability,
	// byte-lag, and cached-LSN freshness checks before choosing PASSTHROUGH reader
	// or REPLICA_WITH_WAIT.

	// If max(write, observed) plus any first-read baseline yields no target, no
	// wait is needed. The write LSN alone is not the full session target.
	if (!wait_plan.has_wait()) {
		// No session target -> reader is trivially consistent, no wait. This is
		// also safe for extended protocol: no wrapper is needed.
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.reader_hg;
		POLARDB_TRACE(
			"PolarDB PLAN: no consistency wait needed (session_lsn=%lu) -> reader=%d\n",
			(unsigned long)session_lsn, route_ctx.reader_hg);
		return plan;
	}

	if (route_ctx.is_extended_protocol) {
		// Extended protocol cannot be wait-wrapped. If the session already has a
		// write LSN, sending the read to a replica without a wait could serve
		// stale data, so automatic replica-eligible extended reads stay on the
		// writer. Manual destination_hostgroup rules bypass this planner earlier
		// and remain authoritative.
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::EXTENDED_PROTOCOL);
		POLARDB_TRACE(
			"PolarDB PLAN: extended protocol with session_lsn=%lu cannot wait-wrap -> FORCE_PRIMARY writer=%d\n",
			(unsigned long)session_lsn, route_ctx.writer_scope.hg);
		return plan;
	}

	// Attach lag-cap inputs; actual reader LSN/cap checks happen during backend
	// acquisition, where the selected server is known.
	polardb_reader_lag_plan(plan, route_ctx);

	plan.action          = PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT;
	plan.target_hg       = route_ctx.reader_hg;
	plan.wait_spec            = wait_plan.spec;
	plan.reader.consistency_target_lsn = wait_plan.spec.target;
	plan.reader.fallback_writer_hg = route_ctx.writer_scope.hg;
	plan.reader.route_rfq_policy = route_ctx.route_rfq_policy;
	plan.reader.allow_best_effort_degrade = !route_ctx.is_extended_protocol;
	POLARDB_TRACE(
		"PolarDB PLAN: REPLICA_WITH_WAIT reader=%d wait_target=%lu timeout_ms=%u wait_mode=%d\n",
		route_ctx.reader_hg, (unsigned long)plan.wait_spec.target, plan.wait_spec.timeout_ms, (int)plan.wait_spec.mode);
	return plan;
}

// ======================================================================
// Stage 3: execute — apply side effects from the plan
// ======================================================================

void PgSQL_Session::polardb_account_route_plan(
	const PolarDB_Query_RoutePlan& plan,
	const PolarDB_Query_RouteCtx& route_ctx)
{
	if (plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::PRIMARY_LSN_UNKNOWN) {
		POLARDB_THREAD_COUNT_ONE(thread, primary_lsn_unknown);
	}
	if (plan.degraded_rfq_route) {
		POLARDB_THREAD_COUNT_ONE(thread, rfq_best_effort_degraded_routes);
		// The per-query client notice is always enqueued, but the operator log
		// warning is fired at most once per run of degraded routes: the latch
		// suppresses repeats until a non-degraded route clears it below. This keeps
		// a sustained degradation from flooding the log.
		polardb_enqueue_degraded_rfq_notice(plan, route_ctx);
		if (!polardb_rfq_degraded_route_warning_sent) {
			polardb_rfq_degraded_route_warning_sent = true;
			proxy_warning(
				"PolarDB PLAN: RFQ-unavailable route degraded by best_effort policy "
				"(reason=%s reader_hg=%d writer_hg=%d sess=%p)\n",
				polardb_route_action_reason_name(plan.action_reason),
				plan.target_hg, route_ctx.writer_scope.hg, this);
		}
	} else {
		polardb_rfq_degraded_route_warning_sent = false;
	}
}

/**
 * @brief Execute the routing decision — apply side effects.
 *
 * PASSTHROUGH and FORCE_PRIMARY just resolve the target HG.
 * REPLICA_WITH_WAIT prepares the query wait state (the actual query wrapping is deferred
 * to finalize_wait_timeout_injection() in PgSQL_PolarDB_Wrap.cpp, called once at
 * ASYNC_IDLE after the backend connection exists, so the wrapped query is built
 * exactly once). The query wait state is reset on entry so stale state never leaks.
 *
 * @param plan Routing plan from polardb_plan().
 * @param route_ctx  Route context from polardb_collect().
 * @param pkt  Client packet ('Q' + len + query + NUL); its query is snapshotted
 *             here and replaced with the wrapped SQL later by the finalize step.
 * @return Execution result with the final target hostgroup.
 */
PolarDB_Query_ExecuteResult PgSQL_Session::polardb_execute(
	const PolarDB_Query_RoutePlan& plan,
	const PolarDB_Query_RouteCtx& route_ctx,
	PtrSize_t& pkt)
{
	PolarDB_Query_ExecuteResult result;
	result.final_target_hg = plan.target_hg;

	// Reset per-query wait state. The reader target is reset before the pipeline
	// (Session.cpp), so it is clean on entry here.
	polardb_query.reset_wait();

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH) {
		POLARDB_TRACE(
			"PolarDB EXECUTE: PASSTHROUGH target_hg=%d\n", plan.target_hg);
		return result;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY) {
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: FORCE_PRIMARY writer=%d reason=%d\n",
			route_ctx.writer_scope.hg, (int)plan.action_reason);
		return result;
	}

	// --- REPLICA_WITH_WAIT ---

	// Guard: a wait-wrapped read needs a query-bearing simple-query packet:
	// 'Q' (1) + length (4) + at least one query byte + NUL = 7 bytes.
	// Empty-query packets are valid PostgreSQL, but they have no SQL text to wrap
	// and remain on the primary. The guard also prevents a size_t underflow.
	if (pkt.size < 7) {
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: malformed packet (size=%u < 7), fallback to primary\n",
			(unsigned)pkt.size);
		return result;
	}

	const char* orig_query = (const char*)pkt.ptr + 5;  // skip 'Q' + length
	size_t orig_len = pkt.size - 5 - 1;                  // exclude 'Q', length, NUL

	// Safety: if waits are disabled for this session, fall back to the writer.
	// Sending to a replica without the wait prefix would silently break RYW.
	if (polardb_wait_disabled) {
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: wait disabled, fallback to primary hg=%d\n",
			route_ctx.writer_scope.hg);
		return result;
	}

	// Record the per-query reader target for backend acquisition.
	polardb_query.reader_plan = plan.reader;
	POLARDB_THREAD_COUNT_ONE(thread, session_lsn_routing);

	// Prepare the query wait state: save intent only. The query wrapping is deferred to
	// finalize_wait_timeout_injection(), which runs after the connection is
	// established, so the wrapped query is built exactly once.
	polardb_query.wait.prepare_from_spec(plan.wait_spec);
	polardb_query.wait.wait_stage = PolarDB_WaitStage::WAITING;
	polardb_query.wait.wait_started_at_us = monotonic_time();
	polardb_query.wait.fallback_writer_hg = plan.reader.fallback_writer_hg;
	polardb_query.wait.original_query.assign(orig_query, orig_len);

	// Stats.
	POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_prepared);

	POLARDB_TRACE(
		"PolarDB EXECUTE: REPLICA_WITH_WAIT prepared target=%lu timeout=%u reader_hg=%d query='%.80s'\n",
		(unsigned long)plan.wait_spec.target, plan.wait_spec.timeout_ms, route_ctx.reader_hg, orig_query);

	return result;
}

/**
 * @brief Apply PolarDB automatic routing for PostgreSQL extended protocol.
 *
 * This is intentionally routing-only: no PolarDB wait SQL is injected into
 * Parse/Bind/Execute streams. Manual destination_hostgroup rules are left
 * untouched; automatic replica_eligible=1 reads may use a reader only when the
 * session has no wait target. If the session has a write/observed target, or a
 * missing-LSN latch is set, the planner returns FORCE_PRIMARY and this helper
 * pins the extended read to the writer.
 */
void PgSQL_Session::polardb_apply_extended_route()
{
	polardb_query.request_writer_scope.reset();

	if (!PgHGM->status.polardb_active.load(std::memory_order_relaxed) || !qpo) {
		return;
	}

	int replica_eligible = -1;
	int dest_hg = -1;
	int manual_scope_hg = -1;
	bool manual_mode = polardb_manual_route_scope(
		&manual_scope_hg, &dest_hg, &replica_eligible);
	if (manual_mode) {
		polardb_capture_request_writer_scope(manual_scope_hg);
		POLARDB_TRACE(
			"PolarDB EXTENDED: manual destination_hostgroup=%d scope_hg=%d, "
			"routing left unchanged\n",
			dest_hg, manual_scope_hg);
		return;
	}

	// Extended protocol never carries a wait wrapper. Clear any request-local
	// wait state before planning so an earlier simple-query wait cannot leak into
	// this request.
	polardb_query.reset_reader_target();
	polardb_query.reset_wait();

	PolarDB_Query_RouteCtx polardb_route_ctx;
	polardb_collect(
		polardb_route_ctx, current_hostgroup, replica_eligible,
		qpo->force_primary_hint);
	if (!polardb_route_ctx.is_polar_hg) {
		return;
	}

	PolarDB_Query_RoutePlan plan = polardb_plan(polardb_route_ctx);
	polardb_account_route_plan(plan, polardb_route_ctx);
	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT) {
		// Defensive only: polardb_plan() should return FORCE_PRIMARY for extended
		// protocol once a wait target exists. Never try to wrap extended protocol.
		current_hostgroup = polardb_route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: planner requested wait wrapper; force writer=%d\n",
			polardb_route_ctx.writer_scope.hg);
		return;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY) {
		current_hostgroup = polardb_route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: FORCE_PRIMARY writer=%d reason=%d\n",
			polardb_route_ctx.writer_scope.hg, (int)plan.action_reason);
		return;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH && plan.target_hg >= 0) {
		current_hostgroup = plan.target_hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: PASSTHROUGH target_hg=%d\n",
			plan.target_hg);
	}
}

// ======================================================================
// Stage 4: process_result — response-path state update (LSN capture)
// ======================================================================

/**
 * @brief Process response-path RFQ state after a successfully completed query.
 *
 * RFQ-only LSN capture. After a successful query, read the
 * backend's WAL LSN from the extended ReadyForQuery (get_polardb_lsn() — native
 * PQgetLSN only, NO synchronous SQL fallback on the request path). If the
 * request writer group+epoch is missing or no longer matches the backend's
 * current writer group+epoch, the RFQ is old-timeline or cross-group data and
 * result processing skips all state/cache updates. Otherwise:
 *   - if the RFQ carried an LSN, advance this session's observed LSN
 *     (polardb_session_consistency.observed_lsn = max(prev, lsn)). SESSION_LSN reads wait
 *     on max(write_lsn, observed_lsn) for monotonic session reads.
 *   - if this was a WRITE query and the RFQ carried an LSN, advance the session
 *     own-write component (polardb_session_consistency.write_lsn = max(prev, lsn)). Later
 *     protected reads wait on max(write_lsn, observed_lsn). The write position is
 *     taken ONLY from the RFQ LSN: there is no global-LSN fallback and no
 *     monitored-LSN fallback. If a writer RFQ
 *     carries no LSN, ProxySQL cannot know the write position; the session is
 *     marked polardb_session_consistency.write_unknown and later automatic LSN-mode
 *     reads stay on the writer until another primary RFQ reports an LSN.
 *   - independent of write/read, if the RFQ carried an LSN, refresh the
 *     per-server LSN cache (replica lag tracking). Session LSN state and the
 *     server-RFQ counter move only after the group+epoch-aware cache update
 *     accepts the RFQ as current request data.
 *
 * The result-processing stage only updates the session LSN latches and
 * per-server LSN cache.
 *
 * @param myds              Backend data stream that produced the result.
 * @param query_digest_text Digest text of the query (write classification).
 */
void PgSQL_Session::polardb_process_result(PgSQL_Data_Stream* myds, const char* query_digest_text) {
	if (!polardb_config.is_polardb_enabled || !myds || !myds->myconn) {
		POLARDB_TRACE("PolarDB PROCESS_RESULT: skip (enabled=%d myconn=%p)\n",
			polardb_config.is_polardb_enabled ? 1 : 0, (void*)(myds ? myds->myconn : nullptr));
		return;
	}

	// RFQ-only LSN read (no extra round-trip); 0 if the RFQ carried no LSN.
	PgSQL_SrvC* backend_srv = myds->myconn->parent;
	uint64_t lsn = myds->myconn->get_polardb_lsn();
	bool has_lsn = (lsn > 0);
	int backend_hg = -1;
	bool backend_is_polar_hg = false;
	int backend_writer_hg = -1;
	int backend_consistency_mode = -1;
	uint64_t backend_writer_epoch = 0;
	PgSQL_HostGroups_Manager::PolarDB_HG_Config backend_config;
	if (backend_srv && backend_srv->myhgc) {
		backend_hg = (int)backend_srv->myhgc->hid;
		backend_config = PgHGM->get_polardb_hg_config((unsigned int)backend_hg);
		backend_is_polar_hg = backend_config.is_polardb_hostgroup;
		backend_writer_hg = backend_config.writer_hostgroup;
		backend_consistency_mode = backend_config.policy.consistency_mode;
		if (backend_config.writer_epoch) {
			backend_writer_epoch =
				backend_config.writer_epoch->load(std::memory_order_acquire);
		}
	}
	bool is_write = PolarDB_Protocol::is_write_query(query_digest_text);
	auto attach_request_epoch_to_session_lsn = [&]() {
		// Automatic routes were already scoped in collect(). Manual
		// destination-hostgroup routes skip collect but still capture a request
		// writer epoch, so result processing must attach that scope before it
		// mutates session LSN targets or missing-LSN latches.
		if (polardb_query.request_writer_scope.valid() &&
				!polardb_session_consistency.writer_scope.matches(
					polardb_query.request_writer_scope)) {
			polardb_scope_session_lsn(
				this,
				polardb_query.request_writer_scope,
				"PROCESS_RESULT");
		}
	};

	auto polardb_process_positioned_rfq = [&]() -> bool {
		bool rfq_accepted = false;
		if (backend_srv && backend_hg >= 0) {
			rfq_accepted = PgHGM->polardb_update_server_lsn(
				backend_srv,
				(unsigned int)backend_hg,
				backend_config,
				lsn,
				polardb_query.request_writer_scope);
		}
		if (!rfq_accepted) {
			POLARDB_TRACE(
				"PolarDB PROCESS_RESULT: RFQ LSN rejected before session update "
				"request_writer_hg=%d request_epoch=%lu request_valid=%d "
				"backend_hg=%d writer_hg=%d backend_epoch=%lu "
				"backend_epoch_valid=%d lsn=%lu is_write=%d parent=%p\n",
				polardb_query.request_writer_scope.hg,
				(unsigned long)polardb_query.request_writer_scope.epoch,
				polardb_query.request_writer_scope.valid() ? 1 : 0,
				backend_hg, backend_writer_hg,
				(unsigned long)backend_writer_epoch,
				backend_config.writer_epoch ? 1 : 0, (unsigned long)lsn,
				is_write ? 1 : 0, (void*)myds->myconn->parent);
			return false;
		}
		attach_request_epoch_to_session_lsn();
		POLARDB_THREAD_COUNT_ONE(thread, server_lsn_updates_from_rfq);
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: accepted %s RFQ LSN=%lu for per-server cache "
			"(%s:%d) digest='%.60s'\n",
			is_write ? "write" : "read", (unsigned long)lsn,
			backend_srv->address, backend_srv->port,
			query_digest_text ? query_digest_text : "(null)");

		const bool positioned_rfq_from_primary =
			polardb_positioned_rfq_from_primary(
				backend_is_polar_hg,
				backend_hg,
				backend_writer_hg);

		if (lsn > polardb_session_consistency.observed_lsn) {
			polardb_session_consistency.observed_lsn = lsn;
		}
		if (positioned_rfq_from_primary) {
			if (polardb_session_consistency.write_unknown || polardb_session_consistency.observed_unknown) {
				POLARDB_TRACE(
					"PolarDB PROCESS_RESULT: primary RFQ LSN restored missing-LSN latches "
					"(backend_hg=%d writer_hg=%d)\n",
					backend_hg, backend_writer_hg);
			}
			polardb_session_consistency.write_unknown = false;
			polardb_session_consistency.observed_unknown = false;
			polardb_rfq_degraded_route_warning_sent = false;
		}
		if (is_write) {
			// Advance the session's own-write component; the wait target is
			// max(write_lsn, observed_lsn).
			if (lsn > polardb_session_consistency.write_lsn) {
				polardb_session_consistency.write_lsn = lsn;
			}
			POLARDB_TRACE("PolarDB PROCESS_RESULT: write query digest='%.60s' session_write_lsn=%lu session_observed_lsn=%lu primary_source=%d\n",
				query_digest_text ? query_digest_text : "(null)",
				(unsigned long)polardb_session_consistency.write_lsn,
				(unsigned long)polardb_session_consistency.observed_lsn,
				positioned_rfq_from_primary ? 1 : 0);
		}
		return true;
	};

	auto polardb_process_missing_rfq = [&]() {
		if (!backend_config.writer_epoch ||
			!polardb_query.request_writer_scope.matches(
				PolarDB_WriterScope{backend_writer_hg, backend_writer_epoch})) {
			POLARDB_TRACE(
				"PolarDB PROCESS_RESULT: skip missing-LSN latch due to writer "
				"group/epoch request_hg=%d request_epoch=%lu request_valid=%d "
				"current_hg=%d current_epoch=%lu current_valid=%d "
				"backend_hg=%d is_write=%d\n",
				polardb_query.request_writer_scope.hg,
				(unsigned long)polardb_query.request_writer_scope.epoch,
				polardb_query.request_writer_scope.valid() ? 1 : 0,
				backend_writer_hg,
				(unsigned long)backend_writer_epoch,
				backend_config.writer_epoch ? 1 : 0,
				backend_hg, is_write ? 1 : 0);
			return;
		}
		attach_request_epoch_to_session_lsn();

		// RFQ carried no LSN. On a PolarDB backend with _polar_send_lsn this should
		// not happen for a writer query. If it does, preserve attribution: missing
		// write LSN latches write_unknown; missing tracked read LSN latches
		// observed_unknown. Do not poison mode=off sessions.
		if (is_write) {
			const bool first_unknown_write = !polardb_session_consistency.write_unknown;
			polardb_session_consistency.write_unknown = true;
			POLARDB_THREAD_COUNT_ONE(thread, write_missing_lsn);
			if (first_unknown_write) {
				proxy_warning(
					"PolarDB PROCESS_RESULT: writer query completed without RFQ LSN; "
					"automatic LSN-mode reads in this session will use writer until "
					"a later primary RFQ carries LSN (sess=%p digest='%.60s')\n",
					this, query_digest_text ? query_digest_text : "(null)");
			}
		} else {
			const int effective_mode = polardb_resolve_consistency_mode(
				polardb_config.session_consistency_mode,
				backend_consistency_mode,
				pgsql_thread___polardb_consistency_mode);
			if (backend_is_polar_hg &&
				effective_mode >= 0 &&
				polardb_consistency_from_int(effective_mode) == PolarDB_ConsistencyMode::SESSION_LSN) {
				const bool first_unknown_observed = !polardb_session_consistency.observed_unknown;
				polardb_session_consistency.observed_unknown = true;
				POLARDB_THREAD_COUNT_ONE(thread, read_missing_lsn);
				if (first_unknown_observed) {
					proxy_warning(
						"PolarDB PROCESS_RESULT: read query completed without RFQ LSN while SESSION_LSN "
						"tracking is active; automatic LSN-mode reads in this session will use "
						"writer while the observed-LSN gap remains latched (sess=%p digest='%.60s')\n",
						this, query_digest_text ? query_digest_text : "(null)");
				}
			}
		}
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: no LSN in RFQ (is_write=%d digest='%.60s') - session_write_lsn=%lu session_observed_lsn=%lu write_lsn_unknown=%d observed_lsn_unknown=%d\n",
			is_write ? 1 : 0, query_digest_text ? query_digest_text : "(null)",
			(unsigned long)polardb_session_consistency.write_lsn,
			(unsigned long)polardb_session_consistency.observed_lsn,
			polardb_session_consistency.write_unknown ? 1 : 0,
			polardb_session_consistency.observed_unknown ? 1 : 0);
	};

	if (has_lsn) {
		(void)polardb_process_positioned_rfq();
	} else {
		polardb_process_missing_rfq();
	}
}

#endif // POLARDB_PROXY
