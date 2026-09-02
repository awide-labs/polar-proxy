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
 * records the reader-selection policy and wait target, but does not activate a
 * wait or copy the query. After a concrete reader is acquired, a current cached
 * LSN at or beyond the target permits direct dispatch. Otherwise the wait becomes
 * active, and the wrapped query is built exactly once at ASYNC_IDLE after the
 * backend connection exists. The wrapper is three SET statements in front of the
 * user query: a consistency mode, a wait timeout, and the wait step itself
 * ("SET polar_xact_split_wait_lsn = '<lsn>'"). On that last statement the backend
 * blocks until its replay position
 * reaches the target LSN, which is what makes the replica read return the
 * session's own earlier writes.
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

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY

#if POLARDB_PROFILE
/**
 * @brief Out-of-line anchor for external consistency tracing.
 *
 * The body is deliberately empty. This symbol exists only so that a uprobe or a
 * perf probe can attach to a stable address and read the six arguments from their
 * registers. The noinline, noclone, used and default-visibility attributes keep
 * the symbol and its call sites from being optimized away, and the asm volatile
 * with a memory clobber keeps the arguments live at the probe point. Removing any
 * of them silently disables every external trace, so keep them as they are.
 *
 * @param event         PolarDB_ConsistencyTraceEvent value for this trace point.
 * @param session_id    Session the event belongs to.
 * @param target_lsn    Wait target, or the session target before the event.
 * @param event_lsn     LSN carried by the event itself.
 * @param auxiliary_lsn Second LSN; its meaning depends on @p event.
 * @param detail        Hostgroup and flag bits packed by
 *                      polardb_consistency_trace_detail().
 */
extern "C" __attribute__((noinline, noclone, used, visibility("default")))
void proxysql_polardb_consistency_trace(
		uint64_t event, uint64_t session_id, uint64_t target_lsn,
		uint64_t event_lsn, uint64_t auxiliary_lsn, uint64_t detail) {
	asm volatile("" : : "g"(event), "g"(session_id), "g"(target_lsn),
		"g"(event_lsn), "g"(auxiliary_lsn), "g"(detail) : "memory");
}

/**
 * @brief Reset and fill the per-query wait profile for a planned LSN wait.
 *
 * Clear polardb_query.wait_profile first; when @p plan carries no wait or a zero
 * target, return immediately and leave the profile inactive. Otherwise mark the
 * profile active, stamp the preparation time, and classify where the wait target
 * came from by comparing it against a base target: the transaction
 * split primary LSN for TXN_SPLIT, the session target for ORDINARY and
 * TXN_PREWRITE. A target above the base in GLOBAL_LSN mode is attributed to the
 * group LSN; a target that matches no known source sets target_mismatch.
 *
 * The caller keeps ownership of the profile on failure paths: a prepare step that
 * is declined after this ran must call polardb_query.wait_profile.reset() itself.
 *
 * @param plan       Route plan whose wait spec and reader plan are profiled.
 * @param route_ctx  Routing snapshot supplying the session and split positions.
 * @param context    Execute branch that prepared the wait; selects the base target.
 */
void PgSQL_Session::polardb_profile_prepare_wait(
		const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx,
		PolarDB_WaitProfileContext context) {
	PolarDB_WaitProfileState& profile = polardb_query.wait_profile;
	profile.reset();
	if (!plan.wait_spec.has_wait() || plan.wait_spec.target == 0) {
		return;
	}

	profile.active = true;
	profile.prepared_at_us = monotonic_time();
	profile.context = context;
	profile.observed_source_server_token =
		route_ctx.session.observed_lsn_source_server_token;
	POLARDB_PROFILE_THREAD_COUNT_ONE(thread, consistency_read_wait_planned);
	proxysql_polardb_consistency_trace(
		static_cast<uint64_t>(
			PolarDB_ConsistencyTraceEvent::READ_WAIT_PLANNED),
		thread_session_id, plan.wait_spec.target,
		route_ctx.session.write_lsn, route_ctx.session.observed_lsn,
		polardb_consistency_trace_detail(
			plan.target_hg, static_cast<uint32_t>(context)));

	uint64_t base_target = route_ctx.session.target();
	if (context == PolarDB_WaitProfileContext::TXN_SPLIT) {
		base_target = route_ctx.transaction_split.primary_lsn;
	}

	if (plan.wait_spec.target > base_target &&
			plan.reader.consistency_mode ==
				PolarDB_ConsistencyMode::GLOBAL_LSN) {
		profile.target_source = PolarDB_WaitProfileTargetSource::GLOBAL;
		return;
	}
	if (plan.wait_spec.target != base_target || base_target == 0) {
		profile.target_mismatch = true;
		return;
	}

	if (context == PolarDB_WaitProfileContext::TXN_SPLIT) {
		profile.target_source =
			PolarDB_WaitProfileTargetSource::TXN_PRIMARY;
	} else if (route_ctx.session.write_lsn == base_target &&
			route_ctx.session.observed_lsn == base_target) {
		profile.target_source =
			PolarDB_WaitProfileTargetSource::SESSION_EQUAL;
	} else if (route_ctx.session.write_lsn == base_target) {
		profile.target_source = PolarDB_WaitProfileTargetSource::WRITE;
	} else if (route_ctx.session.observed_lsn == base_target) {
		profile.target_source =
			PolarDB_WaitProfileTargetSource::OBSERVED;
	} else {
		profile.target_mismatch = true;
	}
}
#endif // POLARDB_PROFILE

static int polardb_effective_consistency_mode_for_session(
		const PgSQL_Session* sess,
		const PgSQL_HostGroups_Manager::PolarDB_HG_Policy& policy) {
	if (pgsql_thread___polardb_profile_off) {
		return static_cast<int>(PolarDB_ConsistencyMode::OFF);
	}
	return polardb_resolve_consistency_mode(
		sess ? sess->polardb_config.session_consistency_mode : -1,
		policy.consistency_mode,
		pgsql_thread___polardb_consistency_mode);
}

/**
 * @brief Keep the response policy paired with the request that selected it.
 *
 * Worker variables and hostgroup policy can change while a backend query is in
 * flight. Snapshot the effective consistency and split decisions before
 * dispatch, then use this state when processing its RFQ. In particular, a query
 * dispatched before profile=off still records its write LSN, while a later query
 * dispatched under profile=off cannot be re-enabled by session or hostgroup
 * overrides.
 */
static void polardb_snapshot_request_policy(
		PgSQL_Session* sess,
		const PgSQL_HostGroups_Manager::PolarDB_HG_Config& hg_config) {
	if (!sess || !hg_config.is_polardb_hostgroup) {
		return;
	}

	sess->polardb_query.profile_enabled =
		!pgsql_thread___polardb_profile_off;
	sess->polardb_query.effective_consistency_mode =
		polardb_effective_consistency_mode_for_session(
			sess, hg_config.policy);
	const PolarDB_ConsistencyMode mode =
		polardb_consistency_from_int(
			sess->polardb_query.effective_consistency_mode);
	sess->polardb_query.txn_split_enabled =
		sess->polardb_query.profile_enabled &&
		polardb_consistency_mode_uses_lsn_wait(mode) &&
		hg_config.policy.txn_split_enabled;
	if (!sess->polardb_query.profile_enabled &&
			(sess->polardb_txn_has_no_write_xids ||
			sess->polardb_transaction_split.active() ||
			sess->polardb_transaction_split.has_backend_evidence() ||
			sess->polardb_txn_reader.active() ||
			sess->polardb_txn_reader_failure.active() ||
			sess->polardb_txn_wait_safety.non_read_committed ||
			sess->polardb_txn_wait_safety.local_state_changed)) {
		sess->polardb_txn_has_no_write_xids = false;
		sess->polardb_teardown_transaction_reader_state(
			"profile_off", /*want_reuse=*/true);
	}
}

/**
 * @brief Read the current writer hostgroup and epoch for one hostgroup.
 *
 * Look up the PolarDB topology for @p hostgroup_id and, if it belongs to a
 * PolarDB replication group, fill @p writer_scope with that group's writer
 * hostgroup and its current epoch. The epoch identifies the live writer; it
 * changes on failover so that LSN state from an old timeline can be discarded.
 *
 * @return true if a valid writer scope was written; false for a non-PolarDB
 *         hostgroup or one with no writer hostgroup.
 */
static bool polardb_snapshot_writer_for_hg(
		int hostgroup_id, PolarDB_WriterScope* writer_scope,
		PgSQL_HostGroups_Manager::PolarDB_HG_Config* config = nullptr) {
	if (hostgroup_id < 0 || !writer_scope) {
		return false;
	}

	const auto hg_config =
		PgHGM->get_polardb_hg_config((unsigned int)hostgroup_id);
	if (!hg_config.is_polardb_hostgroup ||
			hg_config.writer_hostgroup < 0) {
		return false;
	}

	writer_scope->hg = hg_config.writer_hostgroup;
	writer_scope->epoch = hg_config.writer_epoch;
	if (config) {
		*config = hg_config;
	}
	return true;
}

/**
 * @brief Detect "manual routing" from the query rules and report its writer scope.
 *
 * A query rule is doing manual routing when it sets a destination hostgroup and
 * leaves replica_eligible unset (-1). In that case the operator is choosing the
 * backend, so the PolarDB pipeline must not override the route or add a wait
 * wrapper. When replica_eligible is set (0 or 1), the rule opted into automatic
 * routing even if it also names a default destination, so this is NOT manual mode.
 *
 * @return The rule values and the writer scope for a manual route.
 *
 * Rationale for the both-set case (destination + replica_eligible=1):
 * see doc/polardb-arch/06-ROUTING-PIPELINE.md (section E.5).
 */
PgSQL_Session::PolarDB_ManualRoute
PgSQL_Session::polardb_manual_route() const {
	PolarDB_ManualRoute route;
	route.replica_eligible = qpo ? qpo->replica_eligible : -1;
	route.destination_hg = qpo ? qpo->destination_hostgroup : -1;
	if (route.is_manual()) {
		// Inside a sticky (persistent-hostgroup) transaction the connection is
		// already fixed, so scope LSN state to where the session currently is;
		// otherwise scope it to the manual destination the rule selected.
		route.scope_hg = transaction_persistent_hostgroup >= 0
			? current_hostgroup : route.destination_hg;
	}
	return route;
}

/**
 * @brief Return whether the query cache must be bypassed for the current rule.
 *
 * Named profile=off takes precedence and leaves the ordinary ProxySQL cache
 * behavior untouched. Otherwise, two independent reasons disable it. First,
 * once the client has asked for RFQ LSNs the cache is off for the whole session,
 * because a cached result carries no RFQ LSN to hand back to the client. Second,
 * an automatic PolarDB read
 * (replica_eligible=1 on a PolarDB hostgroup) must reach the planner whenever the
 * planner could still choose the primary or a replica wait: read_target=primary and
 * GLOBAL_LSN always, SESSION_LSN once the session has an LSN target or one of the
 * missing-LSN sticky flags. Mode OFF leaves the cache enabled.
 *
 * This runs before collect() and plan(), so no reader plan exists yet; it reads
 * the same cheap inputs the planner will read again later.
 *
 * @return true when the cache lookup must be skipped for this query.
 */
bool PgSQL_Session::polardb_query_cache_is_disabled() const {
	if (pgsql_thread___polardb_profile_off) {
		return false;
	}
	if (polardb_route_state.client_rfq_lsn_requested) {
		return true;
	}

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

	const auto hg_config =
		PgHGM->get_polardb_hg_config((unsigned int)route_hg);
	if (!hg_config.is_polardb_hostgroup) {
		return false;
	}

	const int mode = polardb_resolve_consistency_mode(
		polardb_config.session_consistency_mode,
		hg_config.policy.consistency_mode,
		pgsql_thread___polardb_consistency_mode);
	const PolarDB_ConsistencyMode consistency_mode =
		polardb_consistency_from_int(mode);
	if (consistency_mode == PolarDB_ConsistencyMode::OFF) {
		return false;
	}
	const PolarDB_ReadTarget read_target =
		polardb_read_target_from_int(
			pgsql_thread___polardb_read_target);
	if (read_target == PolarDB_ReadTarget::PRIMARY) {
		return true;
	}
	switch (consistency_mode) {
	case PolarDB_ConsistencyMode::OFF:
		return false;
	case PolarDB_ConsistencyMode::EVENTUAL:
		return false;
	case PolarDB_ConsistencyMode::SESSION_LSN:
		break;
	case PolarDB_ConsistencyMode::GLOBAL_LSN:
		// GLOBAL_LSN always uses the group LSN, so routing cannot be
		// resolved from the ordinary backend cache before the PolarDB planner runs.
		return true;
	}

	// In SESSION_LSN mode, cache is safe until the session has an LSN target or a
	// missing-LSN sticky flag. A new session has no target; its first reader RFQ
	// establishes the observed LSN for later reads.
	return polardb_session_consistency.target() > 0 ||
			polardb_session_consistency.write_unknown ||
			polardb_session_consistency.observed_unknown;
}

static std::string polardb_normalize_query_words(
		const char* query, size_t len) {
	if (!query || len == 0) {
		return {};
	}

	// Normalize only letters and digits so the simple scan covers PostgreSQL
	// spellings with spaces, underscores, or punctuation, for example
	// REPEATABLE READ and repeatable_read.
	std::string normalized;
	normalized.reserve(len);
	for (size_t i = 0; i < len; ++i) {
		const unsigned char ch = (unsigned char)query[i];
		if (std::isalnum(ch)) {
			normalized.push_back((char)std::tolower(ch));
		}
	}
	return normalized;
}

static bool polardb_normalized_mentions_non_read_committed(
		const std::string& normalized) {
	return normalized.find("repeatableread") != std::string::npos ||
		normalized.find("serializable") != std::string::npos;
}

static bool polardb_normalized_mentions_read_committed(
		const std::string& normalized) {
	return normalized.find("readcommitted") != std::string::npos;
}

static bool polardb_normalized_sets_session_isolation_default(
		const std::string& normalized) {
	return normalized.find("sessioncharacteristics") != std::string::npos ||
		normalized.find("defaulttransactionisolation") != std::string::npos;
}

static bool polardb_normalized_sets_proxysql_polardb_variable(
		const std::string& normalized) {
	return normalized.rfind("setproxysqlpolardb", 0) == 0;
}

static bool polardb_query_mentions_non_read_committed(
		const char* query, size_t len) {
	return polardb_normalized_mentions_non_read_committed(
		polardb_normalize_query_words(query, len));
}

/**
 * @brief Record the writer scope this request runs under, for later LSN attribution.
 *
 * Snapshot the writer hostgroup+epoch for @p scope_hg into the per-query state.
 * The response path compares it against the backend's current writer scope, and
 * only an RFQ LSN from that same writer timeline may touch session LSN state. Automatic routes capture
 * this during collect(); this helper exists for the manual-routing path, which
 * skips collect() but still needs the scope attached.
 *
 * @return true if a valid scope was captured; false if @p scope_hg is not a
 *         PolarDB hostgroup (per-query scope is left reset).
 */
bool PgSQL_Session::polardb_capture_request_writer_scope(int scope_hg) {
	polardb_query.request_writer_scope.reset();

	PolarDB_WriterScope request_scope;
	PgSQL_HostGroups_Manager::PolarDB_HG_Config hg_config;
	if (!polardb_snapshot_writer_for_hg(
			scope_hg, &request_scope, &hg_config)) {
		return false;
	}

	polardb_query.request_writer_scope = request_scope;
	polardb_snapshot_request_policy(this, hg_config);
	return true;
}

/**
 * @brief Bind the session's LSN state to a writer scope, discarding stale state.
 *
 * The session's write_lsn / observed_lsn / missing-LSN sticky flags are only meaningful
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
 * The rebind covers more than LSN values. It also clears
 * polardb_txn_has_no_write_xids, resets the one-shot degraded-route warning,
 * counts a session-target epoch reset, and — when the session still holds
 * transaction-split state — calls polardb_teardown_transaction_reader_state() with
 * reuse. That tears down the transaction-split state and the reader-failure
 * writer route and releases any temporary split reader connection back to the
 * pool, so a caller must expect a pooled backend connection to be given up here.
 *
 * Invariant upheld: session LSN state is never compared against, or waited on,
 * across a writer-scope change.
 * Rationale: see doc/polardb-arch/06-ROUTING-PIPELINE.md (writer-epoch scoping).
 *
 * @param sess          Session to rebind. Null is a no-op.
 * @param writer_scope  Scope this request runs under. An invalid scope is a no-op.
 * @param stage         Trace-only label naming the pipeline stage.
 */
static void polardb_rebind_session_lsn_scope(
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
		sess->polardb_txn_has_no_write_xids = false;
		const bool has_split_scope_state =
			sess->polardb_transaction_split.active() ||
			sess->polardb_transaction_split.has_backend_evidence() ||
			sess->polardb_txn_reader_failure.active();
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
			// Drop write/observed LSNs and both missing-LSN sticky flags; they belong
			// to the old scope. Also clear the one-shot degraded-route warning so a
			// fresh degradation under the new scope is reported again. Count only
			// when state was actually discarded (not on the first bind).
			sess->polardb_session_consistency.reset_lsn_state();
			sess->polardb_route_state.rfq_degraded_route_warning_sent = false;
			PgHGM->status.polardb_session_target_epoch_reset.fetch_add(
				1, std::memory_order_relaxed);
		}
		if (has_split_scope_state) {
			sess->polardb_teardown_transaction_reader_state(
				scope_mismatch ? "writer_scope_change" : "writer_scope_bind",
				/*want_reuse=*/true);
		}
	}

	// Stamp the (possibly new) scope so later RFQ LSNs are attributed correctly.
	sess->polardb_session_consistency.writer_scope = writer_scope;
}

static void polardb_apply_request_scope_to_session_lsn(
		PgSQL_Session* sess, const char* stage) {
	if (!sess ||
			!sess->polardb_query.request_writer_scope.valid() ||
			sess->polardb_session_consistency.writer_scope.matches(
				sess->polardb_query.request_writer_scope)) {
		return;
	}

	polardb_rebind_session_lsn_scope(
		sess,
		sess->polardb_query.request_writer_scope,
		stage);
}

/**
 * @brief Check that this request's captured writer scope is still the live one.
 *
 * Return true only when the per-request writer scope was captured, is valid, and
 * equals {@p writer_hg, @p writer_epoch}. Epoch zero is valid; the writer
 * hostgroup identifies whether the scope exists.
 *
 * The zero-LSN and missing-LSN response paths use this check and skip every
 * session-state update when it is false. That fails open on purpose: no
 * write_unknown or observed_unknown is marked from data that may belong to
 * another replication group or to a writer timeline that has since changed.
 *
 * @param sess          Session holding the per-request writer scope.
 * @param backend_is_polar_hg true when the captured backend hostgroup belongs
 *                            to a PolarDB replication group.
 * @param writer_hg     Writer hostgroup the backend currently belongs to.
 * @param writer_epoch  Writer epoch read from that hostgroup.
 * @return true when the request scope matches the current writer scope.
 */
static bool polardb_request_scope_matches_current_writer(
		const PgSQL_Session* sess,
		bool backend_is_polar_hg,
		int writer_hg,
		uint64_t writer_epoch) {
	return sess && backend_is_polar_hg && writer_hg >= 0 &&
		sess->polardb_query.request_writer_scope.matches(
			PolarDB_WriterScope{writer_hg, writer_epoch});
}

static PolarDB_ConsistencyMode polardb_consistency_mode_or_off(
		int effective_consistency_mode) {
	return effective_consistency_mode >= 0
		? polardb_consistency_from_int(effective_consistency_mode)
		: PolarDB_ConsistencyMode::OFF;
}

// ======================================================================
// Stage 1: observe durable inputs, then collect routing snapshot
// ======================================================================

/**
 * @brief Record the durable session observations a route decision depends on.
 *
 * Run this before polardb_collect() on the request path. It returns without
 * touching anything when @p current_hg is not a PolarDB hostgroup, so
 * polardb_query.backend_isolation_status_needed keeps the value it already has
 * from the per-request reset.
 *
 * For a PolarDB hostgroup it captures the request writer scope, rebinds the
 * session LSN state to that scope through polardb_rebind_session_lsn_scope() — which can
 * clear transaction-split state and release the temporary split reader connection
 * — and sets polardb_query.backend_isolation_status_needed, which controls whether
 * the backend isolation parameter status is consulted later in the request. It
 * then inspects the current statement for the BEGIN and SET forms that make
 * pre-write reader waits unsafe and latches the corresponding wait-safety flags.
 *
 * @param current_hg  Hostgroup the query processor selected for this request.
 */
void PgSQL_Session::polardb_observe_route_inputs(int current_hg)
{
	const auto hg_config = PgHGM->get_polardb_hg_config(current_hg);
	if (!hg_config.is_polardb_hostgroup) {
		return;
	}

	PolarDB_WriterScope writer_scope;
	writer_scope.hg = hg_config.writer_hostgroup;
	if (writer_scope.hg < 0) {
		writer_scope.hg = current_hg;
	}
	writer_scope.epoch = hg_config.writer_epoch;
	const bool reader_hostgroup_configured =
		hg_config.reader_hostgroup >= 0;

	// Copy every policy value before scoping cleanup can release a split
	// backend. The local value remains valid even if that cleanup refreshes this
	// thread's topology snapshot.
	polardb_snapshot_request_policy(this, hg_config);
	if (writer_scope.valid()) {
		polardb_query.request_writer_scope = writer_scope;
	}

	if (polardb_query.request_writer_scope.valid()) {
		polardb_rebind_session_lsn_scope(this, writer_scope, "OBSERVE");
	}

	const bool track_txn_reader_wait_safety =
		polardb_query.txn_split_enabled &&
		reader_hostgroup_configured;
	polardb_query.backend_isolation_status_needed =
		track_txn_reader_wait_safety;
	if (!track_txn_reader_wait_safety || !CurrentQuery.QueryPointer) {
		return;
	}

	const char* query = (const char*)CurrentQuery.QueryPointer;
	const size_t query_len = CurrentQuery.QueryLength;
	if (CurrentQuery.PgQueryCmd == PGSQL_QUERY_BEGIN &&
			polardb_query_mentions_non_read_committed(query, query_len)) {
		polardb_note_txn_non_read_committed("begin_isolation");
	}
	if (CurrentQuery.PgQueryCmd != PGSQL_QUERY_SET) {
		return;
	}

	const std::string normalized =
		polardb_normalize_query_words(query, query_len);
	const bool proxysql_polardb_set =
		polardb_normalized_sets_proxysql_polardb_variable(normalized);
	const bool mentions_non_read_committed =
		polardb_normalized_mentions_non_read_committed(normalized);
	const bool mentions_read_committed =
		polardb_normalized_mentions_read_committed(normalized);
	if (polardb_normalized_sets_session_isolation_default(normalized)) {
		// PostgreSQL isolation is not part of PgSQL's tracked-variable table.
		// For PolarDB pre-write reader waits, keep a compact session-level
		// setting from the operator attribute and explicit session/default
		// isolation SET statements. DEFAULT or unknown isolation stays on the primary.
		const bool read_committed =
			mentions_read_committed && !mentions_non_read_committed;
		polardb_config.txn_reader_wait_default_read_committed =
			read_committed;
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: session isolation default "
			"read_committed=%d\n",
			read_committed ? 1 : 0);
	}
	if (mentions_non_read_committed) {
		polardb_note_txn_non_read_committed("set_transaction_isolation");
	}
	if (is_in_transaction() && !proxysql_polardb_set) {
		// SET LOCAL and transaction-scoped SET change backend-local state on
		// the primary transaction connection. Until replay of that state is
		// modeled, pre-write reader waits must stay on the primary.
		polardb_note_txn_local_state_change("in_transaction_set");
	}
}

/**
 * @brief Collect every routing input into a per-query snapshot.
 *
 * Fill PolarDB_Query_RouteCtx by reading session, HostGroups_Manager and thread
 * state. Durable session observations must happen before this function by calling
 * polardb_observe_route_inputs(); polardb_plan() then computes the route from
 * this snapshot.
 *
 * @param current_hg              Current hostgroup from the query processor.
 * @param qpo_replica_eligible    replica_eligible from the matched query rule (-1/0/1).
 * @param qpo_force_primary_hint  true if the query carried a leading SQL comment with route=primary.
 * @return Immutable routing inputs for the current query.
 */
PolarDB_Query_RouteCtx PgSQL_Session::polardb_collect(
		int current_hg, int qpo_replica_eligible,
		bool qpo_force_primary_hint) const
{
	PolarDB_Query_RouteCtx route_ctx{};

	const auto hg_config = PgHGM->get_polardb_hg_config(current_hg);
	route_ctx.is_polar_hg = hg_config.is_polardb_hostgroup;
	if (!route_ctx.is_polar_hg) {
		POLARDB_TRACE(
			"PolarDB COLLECT: hg=%d is_polar=false, skip\n", current_hg);
		return route_ctx;  // fast path — non-PolarDB HG
	}

	// Hostgroup topology
	const auto& policy = hg_config.policy;
	route_ctx.writer_scope.hg = hg_config.writer_hostgroup;
	if (route_ctx.writer_scope.hg < 0) route_ctx.writer_scope.hg = current_hg;  // already the writer HG
	route_ctx.reader_hg = hg_config.reader_hostgroup;
	route_ctx.writer_scope.epoch = hg_config.writer_epoch;

	// Copy the request policy captured by polardb_observe_route_inputs().
	route_ctx.effective_consistency_mode =
		polardb_query.effective_consistency_mode;
	route_ctx.read_target =
		pgsql_thread___polardb_read_target;
	route_ctx.read_fallback_action =
		pgsql_thread___polardb_action_read_fallback;
	const PolarDB_ConsistencyMode consistency_mode =
		polardb_consistency_mode_or_off(
			route_ctx.effective_consistency_mode);
	const bool lsn_mode =
		polardb_consistency_mode_uses_lsn_wait(consistency_mode);

	// Wait config + lag-cap input
	route_ctx.wait_timeout_ms = polardb_resolve_wait_timeout_ms(
		policy.lsn_wait_timeout_ms,
		pgsql_thread___polardb_lsn_wait_timeout_ms);
	route_ctx.lsn_wait_timeout_action = pgsql_thread___polardb_action_lsn_timeout;
	route_ctx.missing_lsn_action = pgsql_thread___polardb_action_missing_lsn;
	route_ctx.replica_loss_action =
		pgsql_thread___polardb_action_replica_loss;
	route_ctx.replica_error_action = pgsql_thread___polardb_action_replica_error;
	route_ctx.max_lag_bytes = policy.max_lag_bytes;

	// Session LSN positions. SESSION_LSN waits on max(write_lsn, observed_lsn).
	// The caller already reconciled this state with the current writer scope.
	route_ctx.session = polardb_session_consistency;
	route_ctx.transaction_split =
		polardb_transaction_split_snapshot(polardb_transaction_split);
	route_ctx.txn_split_enabled =
		polardb_query.txn_split_enabled;
	route_ctx.txn_writer_hg =
		polardb_txn_reader_failure.forced_writer_hg();
	route_ctx.txn_force_writer_after_reader_failure =
		route_ctx.txn_writer_hg >= 0;
	if (route_ctx.txn_force_writer_after_reader_failure) {
		POLARDB_TRACE("PolarDB COLLECT: reader-failure writer route active "
					  "writer_hg=%d "
					  "stage=%d blocked=%d\n",
			route_ctx.txn_writer_hg,
			(int)route_ctx.transaction_split.stage,
			route_ctx.transaction_split.blocked ? 1 : 0);
	}

	// Query eligibility (from query rules)
	route_ctx.replica_eligible = (qpo_replica_eligible == 1);

	// Protocol, transaction and placement state decide whether query-shape
	// classification can affect the route. `active_transactions` is only a
	// transient backend-query flag and is true even for autocommit SELECTs,
	// so use the explicit client transaction tracked from BEGIN/START
	// TRANSACTION through COMMIT or ROLLBACK. Read these inputs before
	// touching SQL text: a primary-targeted request and an explicit
	// transaction with split disabled already have a deterministic primary
	// route.
	route_ctx.is_extended_protocol =
		(extended_query_phase != EXTQ_PHASE_IDLE);
	route_ctx.in_transaction = is_in_transaction();
	// Leading query comment `/* route=primary */`.
	route_ctx.force_primary_hint = qpo_force_primary_hint;
	const bool may_use_reader =
		lsn_mode && route_ctx.replica_eligible &&
		route_ctx.read_target ==
			static_cast<int>(PolarDB_ReadTarget::REPLICA) &&
		!route_ctx.force_primary_hint &&
		(!route_ctx.in_transaction || route_ctx.txn_split_enabled);

	// Multi-statement check. A wrapped read must be a single statement: the
	// wrapper prepends a fixed number of SET statements, and the connection
	// layer counts exactly that many result sets before it forwards the
	// user result. A second user statement would shift that count, so a
	// multi-statement read is never offloaded to a replica. Done here as a
	// direct scan because regex-based multi-statement detection in query
	// rules is not reliable enough to trust. Only scan when a reader route
	// is still possible.
	route_ctx.is_multi_statement = false;
	if (may_use_reader && CurrentQuery.QueryPointer) {
		route_ctx.is_multi_statement = polardb_query_has_multiple_statements(
			(const char*)CurrentQuery.QueryPointer,
			CurrentQuery.QueryLength);
	}

	// Full split-safety classification is needed only inside an explicit
	// split-enabled transaction. Autocommit reads still need the narrower
	// locking-SELECT check because a broad replica_eligible SELECT rule can
	// also match SELECT ... FOR UPDATE/SHARE. Both scans are skipped when
	// placement already requires the primary.
	if (may_use_reader && CurrentQuery.QueryPointer) {
		const char* query = (const char*)CurrentQuery.QueryPointer;
		const bool is_top_level_select =
			(CurrentQuery.PgQueryCmd == PGSQL_QUERY_SELECT);
		if (route_ctx.in_transaction && route_ctx.txn_split_enabled) {
			route_ctx.is_txn_split_safe_read =
				PolarDB_Protocol::is_txn_split_safe_select(
					is_top_level_select, query);
			route_ctx.is_txn_split_locking_read =
				is_top_level_select &&
				!route_ctx.is_txn_split_safe_read;
		} else if (!route_ctx.in_transaction && is_top_level_select) {
			route_ctx.is_txn_split_locking_read =
				PolarDB_Protocol::is_locking_select_query(query);
		}
	}

	route_ctx.txn_reader_wait_isolation_read_committed =
		polardb_txn_wait_uses_read_committed();
	route_ctx.txn_reader_wait_local_state_clean =
		!polardb_txn_wait_safety.local_state_changed;

	{
		[[maybe_unused]] const char* query_text =
			CurrentQuery.QueryPointer ? (const char*)CurrentQuery.QueryPointer : "(null)";
		int query_len = CurrentQuery.QueryPointer ? (int)CurrentQuery.QueryLength : 0;
		if (query_len > 120) query_len = 120;
		POLARDB_TRACE(
			"PolarDB COLLECT: hg=%d writer=%d reader=%d mode=%d "
			"read_target=%d read_fallback_action=%d "
			"replica_eligible=%d multi_stmt=%d extended=%d in_txn=%d "
			"write_lsn=%lu observed_lsn=%lu write_lsn_unknown=%d "
			"observed_lsn_unknown=%d timeout_ms=%u timeout_action=%d "
			"missing_lsn_action=%d txn_split_enabled=%d "
			"txn_safe_read=%d txn_locking_read=%d txn_stage=%d "
			"txn_lsn=%lu txn_xids_len=%zu txn_wal_pending=%d "
			"txn_wait_rc=%d txn_wait_local_clean=%d "
			"writer_hg=%d writer_epoch=%lu Q='%.*s'\n",
			current_hg, route_ctx.writer_scope.hg, route_ctx.reader_hg,
			route_ctx.effective_consistency_mode, route_ctx.read_target,
			route_ctx.read_fallback_action, route_ctx.replica_eligible,
			route_ctx.is_multi_statement, route_ctx.is_extended_protocol,
			route_ctx.in_transaction,
			(unsigned long)route_ctx.session.write_lsn,
			(unsigned long)route_ctx.session.observed_lsn,
			route_ctx.session.write_unknown ? 1 : 0,
			route_ctx.session.observed_unknown ? 1 : 0,
			route_ctx.wait_timeout_ms, route_ctx.lsn_wait_timeout_action,
			route_ctx.missing_lsn_action, route_ctx.txn_split_enabled ? 1 : 0,
			route_ctx.is_txn_split_safe_read ? 1 : 0,
			route_ctx.is_txn_split_locking_read ? 1 : 0,
			(int)route_ctx.transaction_split.stage,
			(unsigned long)route_ctx.transaction_split.primary_lsn,
			route_ctx.transaction_split.xids.size(),
			route_ctx.transaction_split.wal_pending ? 1 : 0,
			route_ctx.txn_reader_wait_isolation_read_committed ? 1 : 0,
			route_ctx.txn_reader_wait_local_state_clean ? 1 : 0,
			route_ctx.writer_scope.hg,
			(unsigned long)route_ctx.writer_scope.epoch, query_len,
			query_text);
	}
	return route_ctx;
}

// ======================================================================
// Stage 2: plan — deterministic LSN routing decision
// ======================================================================

/**
 * @brief Attach the lag-cap byte limit and group LSN to the reader plan.
 *
 * The lag cap is a safety check, not the consistency wait: the
 * polar_xact_split_wait_lsn statement in the wrapped query is what actually waits
 * for the session target. The cap only avoids picking a replica so far behind
 * that the wait would likely time out.
 *
 * Rule (the cap is the hostgroup policy value when >= 0, otherwise the global
 * thread variable):
 *   - cap <= 0: no lag-cap condition — any online reader is fine and @p plan is
 *     left untouched.
 *   - cap  > 0: write the cap and current group LSN into plan.reader. Reader
 *     freshness and byte lag are checked later.
 *
 * @param plan      Output: only plan.reader.max_lag_bytes and
 *                  plan.reader.group_lsn are written.
 * @param route_ctx Routing context (provides the writer scope + max_lag_bytes).
 */
void PgSQL_Session::polardb_attach_reader_lag_cap(
		PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx) const
{
	// Resolve the byte cap: HG policy (>=0) overrides, else the global thread var.
	int max_lag = (route_ctx.max_lag_bytes >= 0) ? route_ctx.max_lag_bytes
	                                        : pgsql_thread___polardb_max_reader_lsn_gap_bytes;
	if (max_lag <= 0) {
		// No lag-cap condition — the wrapped query's wait statement
		// (SET polar_xact_split_wait_lsn) is what enforces consistency.
		return;
	}

	// Cap enabled: require a known group LSN. The selected reader is checked later.
	const uint64_t group_lsn =
		PgHGM->get_polardb_group_lsn(route_ctx.writer_scope.hg);
	plan.reader.group_lsn = group_lsn;
	plan.reader.max_lag_bytes = max_lag;
	POLARDB_TRACE(
		"PolarDB LAG-CAP: group_lsn=%lu max=%d\n",
		(unsigned long)group_lsn, max_lag);
}

/**
 * @brief Deterministic LSN routing decision.
 *
 * Decision tree:
 *   1. Fast paths: non-PolarDB hostgroup or a query that is not reader-eligible.
 *   2. Reader-failure route: once a transaction reader read failed, every
 *      remaining statement of that transaction is forced to the recorded writer
 *      hostgroup.
 *   3. Routing hint: a leading SQL comment carrying route=primary forces the
 *      writer regardless of consistency, session target or lag.
 *   4. Consistency and placement: OFF leaves routing to ProxySQL. Otherwise
 *      read_target chooses the primary or replica path. A replica route uses
 *      action_read_fallback when no reader hostgroup exists.
 *   5. Explicit transaction: before the first write, a READ COMMITTED safe SELECT
 *      with clean transaction-local state takes the pre-write reader wait path
 *      (plan.txn_wait_read set, reader target, primary backend stays open); once
 *      XIDs exist an eligible read becomes REPLICA_TXN_SPLIT. A rejected
 *      temporary-reader path keeps the retained primary when fallback is
 *      primary; fallback=error ends the request instead.
 *   6. Core rule: unsafe query shapes — multi-statement, locking read, remaining
 *      in-transaction cases — stay on the writer. Extended protocol normally
 *      keeps regular ProxySQL qpo routing and does not enter this planner; if it
 *      does, the defensive branch below fails closed to the writer instead of
 *      producing a wait wrapper.
 *   7. Unknown-LSN rules: missing write or observed RFQ sticky flags are turned
 *      into a route before any wait target is built, according to the
 *      missing-LSN action and read fallback.
 *   8. Consistency subdecision: build a query consistency snapshot and call
 *      PolarDB_Query_WaitPlan::build_consistency() for the wait payload. The final
 *      route still belongs to this function. Protected reads wait on
 *      max(write_lsn, observed_lsn), plus the group LSN in GLOBAL_LSN mode. A new
 *      session has no target, so its first read can use a reader without waiting;
 *      that reader's RFQ establishes observed_lsn.
 *   9. Lag-cap rule: set reader metadata; backend acquisition picks a reader
 *      within the cap or redirects this query to the writer.
 *
 * This function does not change session state or counters. HGM is read only for
 * GLOBAL_LSN and lag-cap snapshots. polardb_report_route_result() records the
 * selected route and transaction-split outcome.
 *
 * @param route_ctx Immutable routing context from polardb_collect().
 * @return Routing plan consumed by polardb_execute().
 */
PolarDB_Query_RoutePlan PgSQL_Session::polardb_plan(
		const PolarDB_Query_RouteCtx& route_ctx) const
{
	PolarDB_Query_RoutePlan plan;

	// --- Fast paths ---
	if (!route_ctx.is_polar_hg) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		POLARDB_TRACE(
			"PolarDB PLAN: not a PolarDB HG -> PASSTHROUGH\n");
		return plan;                                              // non-PolarDB HG
	}
	if (!route_ctx.replica_eligible) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.writer_scope.hg;                    // query rule did not opt in
		POLARDB_TRACE(
			"PolarDB PLAN: replica_eligible=false -> PASSTHROUGH writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}

	if (route_ctx.txn_force_writer_after_reader_failure &&
			route_ctx.txn_writer_hg >= 0) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.txn_writer_hg,
			PolarDB_Query_RoutePlan::RouteActionReason::READER_FAILURE_FORCE_WRITER);
		POLARDB_TRACE(
			"PolarDB PLAN: prior split-reader failure kept transaction "
			"to writer=%d\n",
			route_ctx.txn_writer_hg);
		return plan;
	}

	// --- Level 0: routing hint override (/* route=primary */) ---
	// Placed after the eligibility fast-paths (a non-eligible read is already
	// writer-bound) and before the mode decisions, so an explicit per-query hint keeps a
	// replica-eligible read to the writer regardless of consistency mode / RYW / lag.
	if (route_ctx.force_primary_hint) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::HINT_PRIMARY);
		POLARDB_TRACE(
			"PolarDB PLAN: route=primary hint -> FORCE_PRIMARY writer=%d\n", route_ctx.writer_scope.hg);
		return plan;
	}

	// Placement chooses writer or reader. Consistency is evaluated only when
	// placement chooses a reader.
	PolarDB_ConsistencyMode mode = PolarDB_ConsistencyMode::OFF;
	if (route_ctx.effective_consistency_mode >= 0) {
		mode = polardb_consistency_from_int(route_ctx.effective_consistency_mode);
	}
	if (mode == PolarDB_ConsistencyMode::OFF) {
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = -1;
		plan.reader.consistency_mode = mode;
		POLARDB_TRACE(
			"PolarDB PLAN: consistency=off -> ordinary ProxySQL routing\n");
		return plan;
	}
	const PolarDB_ReadTarget read_target =
		polardb_read_target_from_int(route_ctx.read_target);
	const PolarDB_ReadFallbackAction read_fallback =
		polardb_read_fallback_action_from_int(
			route_ctx.read_fallback_action);
	if (read_target == PolarDB_ReadTarget::PRIMARY) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg,
			PolarDB_Query_RoutePlan::RouteActionReason::READ_TARGET_PRIMARY);
		POLARDB_TRACE(
			"PolarDB PLAN: read_target=%s -> FORCE_PRIMARY writer=%d\n",
			polardb_read_target_name(read_target),
			route_ctx.writer_scope.hg);
		return plan;
	}
	if (route_ctx.reader_hg < 0) {
		if (read_fallback == PolarDB_ReadFallbackAction::ERROR) {
			plan.action = PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR;
			plan.action_reason =
				PolarDB_Query_RoutePlan::RouteActionReason::READ_FALLBACK_ERROR;
			POLARDB_TRACE(
				"PolarDB PLAN: no reader hostgroup; "
				"read fallback=error -> RETURN_ERROR\n");
		} else {
			plan.target_hg = route_ctx.writer_scope.hg;
			POLARDB_TRACE(
				"PolarDB PLAN: no reader hostgroup -> writer=%d\n",
				route_ctx.writer_scope.hg);
		}
		return plan;
	}

	// Keep the policy that selected this reader with the query until completion.
	// Factory helpers return a fresh plan, so reader-producing branches copy it
	// again after calling a helper.
	auto snapshot_reader_policy = [&](PolarDB_Query_RoutePlan& target) {
		target.reader.fallback_writer_hg = route_ctx.writer_scope.hg;
		target.reader.missing_lsn_action = route_ctx.missing_lsn_action;
		target.reader.lsn_wait_timeout_action =
			route_ctx.lsn_wait_timeout_action;
		target.reader.read_target = route_ctx.read_target;
		target.reader.read_fallback_action =
			route_ctx.read_fallback_action;
		target.reader.replica_loss_action =
			route_ctx.replica_loss_action;
		target.reader.replica_error_action = route_ctx.replica_error_action;
	};
	snapshot_reader_policy(plan);

	if (mode == PolarDB_ConsistencyMode::EVENTUAL) {
		if (route_ctx.in_transaction) {
			return PolarDB_Query_RoutePlan::force_primary(
				route_ctx.writer_scope.hg,
				PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION);
		}
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.reader_hg;
		plan.reader.consistency_mode = mode;
		POLARDB_TRACE(
			"PolarDB PLAN: consistency=eventual read_target=%s "
			"-> reader=%d\n",
			polardb_read_target_name(read_target),
			route_ctx.reader_hg);
		return plan;
	}

	const PolarDB_LsnWaitTimeoutAction timeout_action =
		polardb_lsn_wait_timeout_action_from_int(
			route_ctx.lsn_wait_timeout_action);
	if (polardb_consistency_policy_error(
			mode,
			polardb_missing_lsn_action_from_int(
				route_ctx.missing_lsn_action),
			timeout_action) != nullptr) {
		plan.action =
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR;
		plan.action_reason =
			PolarDB_Query_RoutePlan::RouteActionReason::INVALID_POLICY;
		POLARDB_TRACE("PolarDB PLAN: invalid consistency policy\n");
		return plan;
	}
	const PolarDB_WaitMode wait_mode =
		polardb_wait_mode_for_timeout_action(timeout_action);

	// Transaction split is LSN-only and simple-query only. This stage names a
	// split-readable route when primary RFQ evidence is complete; execute temporarily uses
	// a replica backend for one read and then restores the primary backend.
	bool allow_transaction_wait_read = false;
	if (route_ctx.in_transaction) {
		const uint64_t max_replica_replay_lsn =
			route_ctx.transaction_split.wal_pending &&
					route_ctx.transaction_split.primary_lsn != 0
			? PgHGM->get_polardb_max_replica_replay_lsn(
				route_ctx.writer_scope)
			: 0;
		PolarDB_Query_RoutePlan::RouteActionReason split_reason =
			polardb_txn_split_rejection_reason(
				route_ctx.txn_split_enabled,
				route_ctx.transaction_split,
				route_ctx.session.write_unknown,
				route_ctx.session.observed_unknown,
				route_ctx.is_multi_statement,
				route_ctx.is_extended_protocol,
				route_ctx.is_txn_split_safe_read,
				route_ctx.is_txn_split_locking_read,
				max_replica_replay_lsn);
		plan.split_checked = true;
		plan.split_reason = split_reason;
		if (route_ctx.transaction_split.wal_pending &&
				split_reason ==
					PolarDB_Query_RoutePlan::RouteActionReason::NONE) {
			POLARDB_TRACE(
				"PolarDB PLAN: replica replay supersedes RFQ WAL pending "
				"primary_lsn=%lu max_replica_replay_lsn=%lu reader_hg=%d "
				"primary_hg=%d\n",
				(unsigned long)route_ctx.transaction_split.primary_lsn,
				(unsigned long)max_replica_replay_lsn,
				route_ctx.reader_hg, route_ctx.writer_scope.hg);
		}
		if (split_reason == PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION &&
				route_ctx.txn_split_enabled &&
				route_ctx.transaction_split.stage ==
					PolarDB_TransactionSplitStage::TXN_ON_PRIMARY &&
				route_ctx.transaction_split.splittable &&
				route_ctx.transaction_split.xids.empty() &&
				route_ctx.is_txn_split_safe_read) {
			if (route_ctx.txn_reader_wait_isolation_read_committed &&
					route_ctx.txn_reader_wait_local_state_clean) {
				// An explicit 'x' marker with no XIDs identifies the pre-write phase.
				// This is not an XID-import split read: use the normal reader wait path
				// while execute keeps the primary transaction backend open.
				allow_transaction_wait_read = true;
				plan.split_reason =
					PolarDB_Query_RoutePlan::RouteActionReason::NONE;
				plan.target_hg = route_ctx.reader_hg;
				POLARDB_TRACE(
					"PolarDB PLAN: transaction pre-write read -> reader wait path "
					"reader=%d primary_hg=%d\n",
					route_ctx.reader_hg, route_ctx.writer_scope.hg);
			} else {
				POLARDB_TRACE(
					"PolarDB PLAN: transaction pre-write reader wait blocked "
					"read_committed=%d local_state_clean=%d -> FORCE_PRIMARY "
					"primary_hg=%d\n",
					route_ctx.txn_reader_wait_isolation_read_committed ? 1 : 0,
					route_ctx.txn_reader_wait_local_state_clean ? 1 : 0,
					route_ctx.writer_scope.hg);
				plan = PolarDB_Query_RoutePlan::force_primary(
					route_ctx.writer_scope.hg,
					PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION);
				plan.split_checked = true;
				return plan;
			}
		} else if (split_reason != PolarDB_Query_RoutePlan::RouteActionReason::NONE) {
			if (split_reason ==
					PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING) {
				POLARDB_TRACE(
					"PolarDB PLAN: RFQ WAL pending keeps transaction read on primary "
					"stage=%d primary_lsn=%lu xids_len=%zu safe_read=%d locking_read=%d "
					"reader_hg=%d primary_hg=%d\n",
					static_cast<int>(route_ctx.transaction_split.stage),
					(unsigned long)route_ctx.transaction_split.primary_lsn,
					route_ctx.transaction_split.xids.size(),
					route_ctx.is_txn_split_safe_read ? 1 : 0,
					route_ctx.is_txn_split_locking_read ? 1 : 0,
					route_ctx.reader_hg,
					route_ctx.writer_scope.hg);
			}
			plan = PolarDB_Query_RoutePlan::force_primary(
				route_ctx.writer_scope.hg, split_reason);
			plan.split_checked = true;
			plan.split_reason = split_reason;
			POLARDB_TRACE(
				"PolarDB PLAN: transaction read not split-readable reason=%s "
				"-> FORCE_PRIMARY primary_hg=%d\n",
				polardb_route_action_reason_name(split_reason),
				route_ctx.writer_scope.hg);
			return plan;
		}

		if (!allow_transaction_wait_read) {
			uint64_t split_target_lsn = route_ctx.transaction_split.primary_lsn;
			if (mode == PolarDB_ConsistencyMode::GLOBAL_LSN) {
				bool group_lsn_unknown = false;
				const uint64_t group_lsn =
					PgHGM->get_polardb_group_lsn(route_ctx.writer_scope.hg);
				split_target_lsn = polardb_target_with_global_lsn(
					split_target_lsn,
					group_lsn,
					&group_lsn_unknown);
				if (group_lsn_unknown) {
					plan = PolarDB_Query_RoutePlan::missing_lsn(
						PolarDB_Query_RoutePlan::RouteActionReason::GROUP_LSN_UNKNOWN,
						route_ctx.missing_lsn_action,
						route_ctx.read_fallback_action,
						route_ctx.writer_scope.hg,
						route_ctx.reader_hg,
						/*allow_reader_without_target=*/false);
					plan.split_checked = true;
					snapshot_reader_policy(plan);
					POLARDB_TRACE(
						"PolarDB PLAN: transaction split GLOBAL_LSN group LSN "
						"unknown -> action=%d target=%d\n",
						(int)plan.action, plan.target_hg);
					return plan;
				}
			}

			PolarDB_WaitSpec split_wait;
			split_wait.type = PolarDB_WaitType::LSN;
			split_wait.target = split_target_lsn;
			split_wait.timeout_ms = route_ctx.wait_timeout_ms;
			split_wait.mode = wait_mode;

			plan = PolarDB_Query_RoutePlan::replica_txn_split(
				route_ctx.reader_hg,
				route_ctx.writer_scope.hg,
				 split_wait,
				route_ctx.transaction_split.xids);
			plan.split_checked = true;
			snapshot_reader_policy(plan);
			plan.reader.consistency_mode = mode;
			plan.reader.lsn_wait_timeout_action =
				static_cast<int>(polardb_transaction_split_timeout_action(
					timeout_action,
					read_fallback ==
						PolarDB_ReadFallbackAction::PRIMARY));
			plan.reader.allow_reader_without_target = false;
			polardb_attach_reader_lag_cap(plan, route_ctx);
			POLARDB_TRACE(
				"PolarDB PLAN: REPLICA_TXN_SPLIT planned reader=%d wait_target=%lu "
				"xids_len=%zu timeout_ms=%u timeout_action=%s\n",
				route_ctx.reader_hg,
				(unsigned long)plan.wait_spec.target,
				plan.txn_xids.size(),
				plan.wait_spec.timeout_ms,
				polardb_lsn_wait_timeout_action_name(
					polardb_lsn_wait_timeout_action_from_int(
						plan.reader.lsn_wait_timeout_action)));
			return plan;
		}
	}

	// Unsafe query shapes stay on the primary.
	auto required_reason =
		polardb_writer_required_reason(
			route_ctx.in_transaction && !allow_transaction_wait_read,
			route_ctx.is_multi_statement,
			route_ctx.is_txn_split_locking_read);
	if (required_reason != PolarDB_Query_RoutePlan::RouteActionReason::NONE) {
		plan = PolarDB_Query_RoutePlan::force_primary(
			route_ctx.writer_scope.hg, required_reason);
		POLARDB_TRACE(
			"PolarDB PLAN: primary-required query shape reason=%d -> FORCE_PRIMARY writer=%d\n",
			(int)plan.action_reason, route_ctx.writer_scope.hg);
		return plan;
	}

	// A prior RFQ finished without an LSN while write or observed session state
	// needed to be marked unknown. In LSN mode, ProxySQL cannot build the exact wait
	// target for later automatic reads. Do not guess from monitor/global LSN:
	// apply the missing-LSN action only after primary-required query shapes had a
	// chance to force the writer.
	//
	// Extended protocol is different: it carries no wait wrapper, and local
	// Parse/Bind completions can bypass the normal result-wire notice flush.
	// Therefore warning degradation is disabled there; an unknown-target
	// extended read forces the writer rather than risk a silent stale result,
	// until a dedicated extended-protocol wait model exists.
	if (route_ctx.session.write_unknown) {
		plan = PolarDB_Query_RoutePlan::missing_lsn(
				PolarDB_Query_RoutePlan::RouteActionReason::WRITE_LSN_UNKNOWN,
				route_ctx.missing_lsn_action,
				route_ctx.read_fallback_action,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				!route_ctx.is_extended_protocol &&
					!polardb_consistency_mode_disallows_degraded_reader(
						mode));
		plan.split_checked = allow_transaction_wait_read;
		snapshot_reader_policy(plan);
		POLARDB_TRACE(
			"PolarDB PLAN: session write LSN unknown policy=%d -> action=%d target=%d\n",
			route_ctx.missing_lsn_action, (int)plan.action, plan.target_hg);
		return plan;
	}
	if (route_ctx.session.observed_unknown) {
		plan = PolarDB_Query_RoutePlan::missing_lsn(
				PolarDB_Query_RoutePlan::RouteActionReason::OBSERVED_LSN_UNKNOWN,
				route_ctx.missing_lsn_action,
				route_ctx.read_fallback_action,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				!route_ctx.is_extended_protocol &&
					!polardb_consistency_mode_disallows_degraded_reader(
						mode));
		plan.split_checked = allow_transaction_wait_read;
		snapshot_reader_policy(plan);
		POLARDB_TRACE(
			"PolarDB PLAN: session observed LSN unknown policy=%d -> action=%d target=%d\n",
			route_ctx.missing_lsn_action, (int)plan.action, plan.target_hg);
		return plan;
	}
	// Consistency subdecision. SESSION_LSN uses max(write, observed). GLOBAL_LSN
	// also includes the latest trusted group LSN observation and routes to the
	// primary when that observation is unknown.
	const bool global_lsn_mode = mode == PolarDB_ConsistencyMode::GLOBAL_LSN;
	bool group_lsn_unknown = false;
	uint64_t group_lsn = 0;
	if (global_lsn_mode) {
		group_lsn =
			PgHGM->get_polardb_group_lsn(route_ctx.writer_scope.hg);
	}
	uint64_t session_lsn = global_lsn_mode
		? route_ctx.session.target_with_global_lsn(
			group_lsn,
			&group_lsn_unknown)
		: route_ctx.session.target();
	if (group_lsn_unknown) {
		plan = PolarDB_Query_RoutePlan::missing_lsn(
				PolarDB_Query_RoutePlan::RouteActionReason::GROUP_LSN_UNKNOWN,
				route_ctx.missing_lsn_action,
				route_ctx.read_fallback_action,
				route_ctx.writer_scope.hg,
				route_ctx.reader_hg,
				/*allow_reader_without_target=*/false);
		plan.split_checked = allow_transaction_wait_read;
		snapshot_reader_policy(plan);
		POLARDB_TRACE(
			"PolarDB PLAN: GLOBAL_LSN group observation unknown policy=%d -> action=%d target=%d\n",
			route_ctx.missing_lsn_action, (int)plan.action, plan.target_hg);
		return plan;
	}
	PolarDB_Query_WaitPlan wait_plan = PolarDB_Query_WaitPlan::build_consistency(
		mode, session_lsn, route_ctx.wait_timeout_ms, wait_mode, /*prefer_replica=*/true);

	// REPLICA means the consistency policy allows a reader. It is not enough to
	// route by itself: this planner still applies query-shape, reader availability,
	// byte-lag, and cached-LSN freshness checks before choosing PASSTHROUGH reader
	// or REPLICA_WITH_WAIT.

	// If max(write, observed) yields no target, this session has no consistency
	// obligation yet and the first reader RFQ will establish one.
	if (!wait_plan.has_wait()) {
		// No session target -> reader is trivially consistent, no wait. This is
		// also safe for extended protocol: no wrapper is needed. Retain the mode
		// so an ordinary-reader failure still uses this query's captured policy.
		plan.action = PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH;
		plan.target_hg = route_ctx.reader_hg;
		plan.reader.consistency_mode = mode;
		plan.txn_wait_read = allow_transaction_wait_read;
		POLARDB_TRACE(
			"PolarDB PLAN: no consistency wait needed (mode=%d session_lsn=%lu group_lsn=%lu) -> "
			"reader=%d txn_wait_read=%d\n",
			(int)mode, (unsigned long)session_lsn, (unsigned long)group_lsn, route_ctx.reader_hg,
			allow_transaction_wait_read ? 1 : 0);
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
	polardb_attach_reader_lag_cap(plan, route_ctx);

	plan.action = PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT;
	plan.target_hg = route_ctx.reader_hg;
	plan.wait_spec = wait_plan.spec;
	plan.txn_wait_read = allow_transaction_wait_read;
	plan.reader.consistency_mode = mode;
	plan.reader.allow_reader_without_target =
		!route_ctx.is_extended_protocol &&
		!polardb_consistency_mode_disallows_degraded_reader(mode);
	POLARDB_TRACE(
		"PolarDB PLAN: REPLICA_WITH_WAIT reader=%d wait_target=%lu group_lsn=%lu mode=%d timeout_ms=%u wait_mode=%d\n",
		route_ctx.reader_hg, (unsigned long)plan.wait_spec.target,
		(unsigned long)group_lsn, (int)mode, plan.wait_spec.timeout_ms,
		(int)plan.wait_spec.mode);
	return plan;
}

// ======================================================================
// Stage 3: execute — apply side effects from the plan
// ======================================================================

void PgSQL_Session::polardb_return_consistency_error(
		PolarDB_Query_RoutePlan::RouteActionReason reason)
{
	const char* message =
		"PolarDB cannot enforce the required LSN because the LSN target is unavailable";
	if (reason == PolarDB_Query_RoutePlan::RouteActionReason::INVALID_POLICY) {
		message =
			"PolarDB global_lsn cannot use warning for a missing LSN "
			"or an LSN wait timeout";
	} else if (reason ==
			PolarDB_Query_RoutePlan::RouteActionReason::READ_FALLBACK_ERROR) {
		message =
			"PolarDB read routing requires a usable replica";
	}

	client_myds->setDSS_STATE_QUERY_SENT_NET();
	client_myds->myprot.generate_error_packet(
		true, is_extended_query_ready_for_query(), message,
		PGSQL_ERROR_CODES::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
		false, true);
	RequestEnd(NULL, true);
}

/**
 * @brief Count a produced route plan and report a best-effort-degraded RFQ route.
 *
 * Update the per-thread route counters for @p plan, then report degradation,
 * which is more than accounting. Every degraded route enqueues a per-query notice
 * to the client through polardb_enqueue_degraded_rfq_notice(), so this function
 * does produce protocol output the client receives.
 *
 * It also owns the one-shot polardb_route_state.rfq_degraded_route_warning_sent
 * flag: the first degraded route sets it and logs an operator warning, later
 * degraded routes only enqueue the client notice, and any non-degraded route
 * clears it so a fresh run of degradation is reported again.
 *
 * @param plan       Route plan produced by polardb_plan().
 * @param route_ctx  Routing snapshot the plan was built from.
 */
void PgSQL_Session::polardb_report_route_result(
	const PolarDB_Query_RoutePlan& plan,
	const PolarDB_Query_RouteCtx& route_ctx)
{
	POLARDB_THREAD_COUNT_ONE(thread, route_planner_total);
	if (plan.split_checked) {
		if (route_ctx.transaction_split.stage ==
				PolarDB_TransactionSplitStage::TXN_SPLITTABLE) {
			POLARDB_THREAD_COUNT_ONE(thread, queries_in_splittable_txn);
		}
		if (plan.action ==
				PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT) {
			POLARDB_THREAD_COUNT_ONE(thread, queries_split_eligible);
			if (route_ctx.transaction_split.wal_pending) {
				POLARDB_THREAD_COUNT_ONE(
					thread, split_wal_pending_replica_confirmed);
			}
		}
		switch (plan.split_reason) {
		case PolarDB_Query_RoutePlan::RouteActionReason::MULTI_STATEMENT:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_multistatement);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_NOT_SELECT:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_not_select);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_LOCKING_READ:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_for_update);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_WRITE_LSN_UNKNOWN:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_write_lsn_unknown);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_OBSERVED_LSN_UNKNOWN:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_observed_lsn_unknown);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION:
			POLARDB_THREAD_COUNT_ONE(thread, split_rejected_no_marker);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING:
			POLARDB_THREAD_COUNT_ONE(thread, split_wal_pending);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_BLOCKED:
			POLARDB_THREAD_COUNT_ONE(thread, split_blocked_reads);
			break;
		case PolarDB_Query_RoutePlan::RouteActionReason::INVARIANT_VIOLATION:
			POLARDB_THREAD_COUNT_ONE(thread, split_invariant_violations);
			break;
		default:
			break;
		}
	}
	if (!route_ctx.replica_eligible) {
		// This includes writes and control statements that reached the automatic
		// planner but were not opted into PolarDB reader routing by query rules.
		POLARDB_THREAD_COUNT_ONE(thread, route_replica_ineligible);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, route_replica_eligible);

		const bool planned_reader =
			plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT ||
			plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT ||
			(plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
				plan.target_hg == route_ctx.reader_hg);
		const bool planned_writer =
			plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY ||
			(plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
				plan.target_hg == route_ctx.writer_scope.hg);

		if (planned_reader) {
			POLARDB_THREAD_COUNT_ONE(thread, route_to_reader);
			if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
					plan.target_hg == route_ctx.reader_hg &&
					!plan.degraded_rfq_route) {
				POLARDB_THREAD_COUNT_ONE(thread, route_no_wait_target);
			}
			if (plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT) {
				POLARDB_THREAD_COUNT_ONE(thread, route_wait_required);
			}
			if (plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT) {
				POLARDB_THREAD_COUNT_ONE(thread, route_txn_split_planned);
			}
			if (plan.txn_wait_read) {
				POLARDB_THREAD_COUNT_ONE(thread, route_txn_wait_planned);
			}
		} else if (planned_writer) {
			POLARDB_THREAD_COUNT_ONE(thread, route_to_writer);
		} else if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
				plan.target_hg < 0) {
			// mode=off: the planner intentionally leaves the existing query-rule
			// destination unchanged instead of naming reader or writer.
			POLARDB_THREAD_COUNT_ONE(thread, route_passthrough_rule_owned);
		}
	}

	if (plan.action_reason == PolarDB_Query_RoutePlan::RouteActionReason::GROUP_LSN_UNKNOWN) {
		POLARDB_THREAD_COUNT_ONE(thread, group_lsn_unknown);
	}
	if (plan.degraded_rfq_route) {
		POLARDB_THREAD_COUNT_ONE(thread, rfq_best_effort_degraded_routes);
		// The per-query client notice is always enqueued, but the operator log
		// warning is fired at most once per run of degraded routes: the sticky flag
		// suppresses repeats until a non-degraded route clears it below. This keeps
		// a sustained degradation from flooding the log.
		polardb_enqueue_degraded_rfq_notice(plan, route_ctx);
		if (!polardb_route_state.rfq_degraded_route_warning_sent) {
			polardb_route_state.rfq_degraded_route_warning_sent = true;
			proxy_warning(
				"PolarDB PLAN: missing_lsn_action=warning "
				"(reason=%s reader_hg=%d writer_hg=%d sess=%p)\n",
				polardb_route_action_reason_name(plan.action_reason),
				plan.target_hg, route_ctx.writer_scope.hg, this);
		}
	} else {
		polardb_route_state.rfq_degraded_route_warning_sent = false;
	}
}

void PgSQL_Session::polardb_account_manual_route(int effective_hg, bool forced_writer)
{
	POLARDB_THREAD_COUNT_ONE(thread, route_manual_total);
	if (forced_writer) {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_forced_writer);
	}

	if (effective_hg < 0) {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_other);
		return;
	}

	const auto hg_config =
		PgHGM->get_polardb_hg_config(static_cast<unsigned int>(effective_hg));
	if (!hg_config.is_polardb_hostgroup) {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_other);
		return;
	}

	if (effective_hg == hg_config.reader_hostgroup) {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_to_reader);
	} else if (effective_hg == hg_config.writer_hostgroup) {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_to_writer);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, route_manual_other);
	}
}

bool PgSQL_Session::polardb_apply_reader_failure_writer_route(const char* stage)
{
	const int failure_writer_hg =
		polardb_txn_reader_failure.forced_writer_hg();
	if (failure_writer_hg < 0) {
		return false;
	}

	current_hostgroup = failure_writer_hg;
	POLARDB_TRACE(
		"PolarDB %s: reader-failure writer route writer_hg=%d\n",
		stage ? stage : "ROUTE", failure_writer_hg);
	return true;
}

/**
 * @brief Honor a session hostgroup lock instead of routing automatically.
 *
 * The two true cases are not symmetric. With @p require_current_match and a
 * current_hostgroup that differs from locked_on_hostgroup, nothing is touched at
 * all — no hostgroup assignment, no request writer scope capture, no manual-route
 * counting — because the mismatch is deliberately left to the core
 * locked-hostgroup check to report. Otherwise the lock is applied:
 * current_hostgroup becomes locked_on_hostgroup, the request writer scope is
 * captured for that hostgroup, and the route is counted as a manual route.
 *
 * @param stage                 Trace-only label naming the call site.
 * @param require_current_match true to leave a hostgroup mismatch to the core
 *                              check rather than overriding current_hostgroup.
 * @return true when automatic PolarDB routing must be skipped, which is not the
 *         same as "a route was applied" — see above. false when no lock is in
 *         effect and the caller proceeds with normal planning.
 */
bool PgSQL_Session::polardb_handle_locked_hostgroup_route(
		const char* stage, bool require_current_match)
{
	if (locked_on_hostgroup < 0) {
		return false;
	}
	if (require_current_match && current_hostgroup != locked_on_hostgroup) {
		// Extended protocol uses this path to stop automatic PolarDB routing
		// while leaving the core locked-hostgroup check to report the mismatch.
		return true;
	}

	current_hostgroup = locked_on_hostgroup;
	polardb_capture_request_writer_scope(locked_on_hostgroup);
	polardb_account_manual_route(current_hostgroup, false);
	POLARDB_THREAD_COUNT_ONE(thread, route_locked_hostgroup);
#if POLARDB_DEBUG
	if (is_in_transaction() && polardb_txn_wait_safety.local_state_changed &&
			CurrentQuery.QueryPointer &&
			CurrentQuery.PgQueryCmd == PGSQL_QUERY_SELECT &&
			PolarDB_Protocol::is_txn_split_safe_select(true,
				(const char*)CurrentQuery.QueryPointer)) {
		POLARDB_TRACE(
			"PolarDB PLAN: transaction pre-write reader wait blocked "
			"read_committed=%d local_state_clean=0 -> FORCE_PRIMARY "
			"primary_hg=%d\n",
			polardb_txn_wait_uses_read_committed() ? 1 : 0,
			locked_on_hostgroup);
	}
#endif // POLARDB_DEBUG
	POLARDB_TRACE(
		"PolarDB %s: locked hostgroup route hg=%d; automatic routing skipped\n",
		stage ? stage : "ROUTE", locked_on_hostgroup);
	return true;
}

/**
 * @brief Execute the routing decision — apply side effects.
 *
 * Branch order matters. plan.txn_wait_read is checked first: it is the pre-write
 * transaction reader wait, prepared by polardb_prepare_txn_wait_read() while the
 * primary transaction backend stays open for the rest of the transaction.
 * PASSTHROUGH and FORCE_PRIMARY just resolve the target HG.
 * REPLICA_WITH_WAIT records the wait target as reader-selection input. If the
 * selected reader has not reached it, backend acquisition activates the query
 * wait state and polardb_install_wait_wrapper() builds the wrapper once at
 * ASYNC_IDLE. A target-ready reader never activates wrapper state and dispatches
 * the original query directly. REPLICA_TXN_SPLIT builds the XID + LSN-wait wrapper immediately
 * on a temporary replica backend because the primary backend must keep the client
 * transaction open. The query wait state is reset on entry so stale state never leaks.
 *
 * Packet ownership differs per branch. REPLICA_WITH_WAIT leaves @p pkt with the
 * caller; if the selected reader really needs a wrapper, finalization copies the
 * SQL immediately before replacing the backend packet. Both transaction reader
 * paths take ownership: REPLICA_TXN_SPLIT saves the packet for split-read cleanup,
 * while the pre-write wait path moves it to the reader stream. Both clear @p pkt.
 *
 * @param plan Routing plan from polardb_plan().
 * @param route_ctx  Route context from polardb_collect().
 * @param pkt  Client packet ('Q' + len + query + NUL). See the ownership rules above.
 * @return Execution result carrying the final target hostgroup. A declined
 *         transaction-reader prepare, or a disabled transaction-reader branch,
 *         uses the writer when fallback is allowed and returns
 *         READ_FALLBACK_ERROR when replica fallback is disabled. The caller must use
 *         final_target_hg and not the plan's target.
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
	polardb_query.reader_wait_spec.reset();

	auto finish_declined_transaction_reader = [&](const char* operation) {
		const PolarDB_ReadFallbackAction fallback =
			polardb_read_fallback_action_from_int(
				plan.reader.read_fallback_action);
		if (fallback == PolarDB_ReadFallbackAction::ERROR) {
			result.final_target_hg = -1;
			result.return_error = true;
				polardb_return_consistency_error(
					PolarDB_Query_RoutePlan::RouteActionReason::
						READ_FALLBACK_ERROR);
			POLARDB_TRACE(
				"PolarDB EXECUTE: %s; read fallback=error -> RETURN_ERROR\n",
				operation);
			return;
		}
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: %s; using primary_hg=%d\n",
			operation, route_ctx.writer_scope.hg);
	};

	if (plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR) {
		result.final_target_hg = -1;
		result.return_error = true;
		polardb_return_consistency_error(plan.action_reason);
		POLARDB_TRACE(
			"PolarDB EXECUTE: RETURN_ERROR reason=%s\n",
			polardb_route_action_reason_name(plan.action_reason));
		return result;
	}

	if (plan.txn_wait_read) {
		if (polardb_route_state.wait_disabled) {
			finish_declined_transaction_reader(
				"transaction wait read disabled");
			return result;
		}
#if POLARDB_PROFILE
		polardb_profile_prepare_wait(
			plan, route_ctx, PolarDB_WaitProfileContext::TXN_PREWRITE);
#endif // POLARDB_PROFILE
		if (polardb_prepare_txn_wait_read(plan, route_ctx, pkt)) {
			result.final_target_hg = plan.target_hg;
			POLARDB_TRACE(
				"PolarDB EXECUTE: transaction wait read prepared reader_hg=%d "
				"primary_hg=%d target_lsn=%lu\n",
				plan.target_hg, route_ctx.writer_scope.hg,
				(unsigned long)plan.wait_spec.target);
			return result;
		}
#if POLARDB_PROFILE
		polardb_query.wait_profile.reset();
#endif // POLARDB_PROFILE
		finish_declined_transaction_reader(
			"transaction wait read prepare declined");
		return result;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH) {
		polardb_query.reader_plan = plan.reader;
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

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT) {
		if (polardb_route_state.wait_disabled) {
			finish_declined_transaction_reader(
				"transaction split wait disabled");
			return result;
		}
#if POLARDB_PROFILE
		polardb_profile_prepare_wait(
			plan, route_ctx, PolarDB_WaitProfileContext::TXN_SPLIT);
#endif // POLARDB_PROFILE
		if (polardb_prepare_txn_split_read(plan, route_ctx.writer_scope, pkt)) {
			result.final_target_hg = plan.target_hg;
			POLARDB_TRACE(
				"PolarDB EXECUTE: REPLICA_TXN_SPLIT prepared reader_hg=%d "
				"primary_hg=%d target_lsn=%lu\n",
				plan.target_hg, route_ctx.writer_scope.hg,
				(unsigned long)plan.wait_spec.target);
			return result;
		}
#if POLARDB_PROFILE
		polardb_query.wait_profile.reset();
#endif // POLARDB_PROFILE
		POLARDB_THREAD_COUNT_ONE(thread, split_reads_fallback);
		finish_declined_transaction_reader(
			"REPLICA_TXN_SPLIT prepare declined");
		return result;
	}

	// --- REPLICA_WITH_WAIT ---

	// check: a wait-wrapped read needs a query-bearing simple-query packet:
	// 'Q' (1) + length (4) + at least one query byte + NUL = 7 bytes.
	// Empty-query packets are valid PostgreSQL, but they have no SQL text to wrap
	// and remain on the primary. The check also prevents a size_t underflow.
	if (pkt.size < PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD + 1) {
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: simple-query packet has no SQL body "
			"(size=%u), fallback to primary\n",
			(unsigned)pkt.size);
		return result;
	}

	// Safety: if waits are disabled for this session, fall back to the writer.
	// Sending to a replica without the wait prefix would silently break RYW.
	if (polardb_route_state.wait_disabled) {
		result.final_target_hg = route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXECUTE: wait disabled, fallback to primary hg=%d\n",
			route_ctx.writer_scope.hg);
		return result;
	}

	// Record the per-query reader target for backend acquisition.
	polardb_query.reader_plan = plan.reader;
	if (plan.reader.consistency_mode == PolarDB_ConsistencyMode::GLOBAL_LSN) {
		POLARDB_THREAD_COUNT_ONE(thread, global_lsn_routing);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, session_lsn_routing);
	}

	// Keep the wait payload only as reader-selection input. Backend acquisition
	// activates wrapper state only when the selected reader is behind the target.
	// A target-ready reader therefore avoids the timer, wrapper counter, and
	// ASYNC_IDLE finalization path completely.
#if POLARDB_PROFILE
	polardb_profile_prepare_wait(
		plan, route_ctx, PolarDB_WaitProfileContext::ORDINARY);
#endif // POLARDB_PROFILE
	polardb_query.reader_wait_spec = plan.wait_spec;

	POLARDB_TRACE(
		"PolarDB EXECUTE: REPLICA_WITH_WAIT selection target=%lu timeout=%u reader_hg=%d query='%.80s'\n",
		(unsigned long)plan.wait_spec.target, plan.wait_spec.timeout_ms,
		route_ctx.reader_hg,
		(const char*)pkt.ptr + PGSQL_V3_MESSAGE_HEADER_SIZE);

	return result;
}

/**
 * @brief Apply PolarDB automatic routing for PostgreSQL extended protocol.
 *
 * This is intentionally routing-only: no PolarDB wait SQL is injected into
 * Parse/Bind/Execute streams. The per-request writer scope is reset on entry, and
 * for automatic routes the reader target and wait state are reset too, so nothing
 * from an earlier simple-query request leaks into this one.
 *
 * A session hostgroup lock short-circuits everything: the route stays on the
 * locked hostgroup and this returns. Manual destination_hostgroup rules keep their
 * destination as well, with one exception — when a transaction reader read already
 * failed, polardb_apply_reader_failure_writer_route() overrides current_hostgroup
 * with that failure writer hostgroup so the transaction stays where it must, and
 * the manual destination is not honored for that request.
 *
 * Automatic replica_eligible=1 reads may use a reader only when the session has no
 * wait target. If the session has a write/observed target, or a missing-LSN sticky
 * flag is set, the planner returns FORCE_PRIMARY and this helper keeps the
 * extended read on the writer.
 */
bool PgSQL_Session::polardb_apply_extended_route()
{
	polardb_query.request_writer_scope.reset();

	if (!PgHGM->status.polardb_active.load(std::memory_order_relaxed) || !qpo) {
		return false;
	}
	if (polardb_handle_locked_hostgroup_route(
			"EXTENDED", /*require_current_match=*/true)) {
		return false;
	}

	const PolarDB_ManualRoute manual_route = polardb_manual_route();
	if (manual_route.is_manual()) {
		const bool forced_writer =
			polardb_apply_reader_failure_writer_route("EXTENDED");
		polardb_capture_request_writer_scope(manual_route.scope_hg);
		polardb_account_manual_route(current_hostgroup, forced_writer);
		POLARDB_TRACE(
			"PolarDB EXTENDED: manual destination_hostgroup=%d scope_hg=%d, "
			"routing left unchanged\n",
			manual_route.destination_hg, manual_route.scope_hg);
		return false;
	}

	// Extended protocol never carries a wait wrapper. Clear any request-local
	// wait state before planning so an earlier simple-query wait cannot leak into
	// this request.
	polardb_query.reset_reader_plan();
	polardb_query.reset_wait();

	polardb_observe_route_inputs(current_hostgroup);
	if (pgsql_thread___polardb_profile_off) {
		// observe() has already reconciled writer scope and torn down any old
		// transaction-reader state. Clear every request-local reader artifact,
		// then leave placement to ordinary ProxySQL without planner counters.
		polardb_query.clear_reader_route();
		POLARDB_TRACE(
			"PolarDB EXTENDED: named profile off after request cleanup; "
			"routing left unchanged\n");
		return false;
	}
	PolarDB_Query_RouteCtx polardb_route_ctx = polardb_collect(
		current_hostgroup, manual_route.replica_eligible,
		qpo->force_primary_hint);
	if (!polardb_route_ctx.is_polar_hg) {
		return false;
	}

	PolarDB_Query_RoutePlan plan = polardb_plan(polardb_route_ctx);
	polardb_report_route_result(plan, polardb_route_ctx);
	polardb_query.reader_plan = plan.reader;
	if (plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR) {
		polardb_return_consistency_error(plan.action_reason);
		POLARDB_TRACE(
			"PolarDB EXTENDED: RETURN_ERROR reason=%s\n",
			polardb_route_action_reason_name(plan.action_reason));
		return true;
	}
	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT) {
		// Defensive only: polardb_plan() should return FORCE_PRIMARY for extended
		// protocol once a wait target exists. Never try to wrap extended protocol.
		current_hostgroup = polardb_route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: planner requested wait wrapper; force writer=%d\n",
			polardb_route_ctx.writer_scope.hg);
		return false;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY) {
		current_hostgroup = polardb_route_ctx.writer_scope.hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: FORCE_PRIMARY writer=%d reason=%d\n",
			polardb_route_ctx.writer_scope.hg, (int)plan.action_reason);
		return false;
	}

	if (plan.action == PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH && plan.target_hg >= 0) {
		current_hostgroup = plan.target_hg;
		POLARDB_TRACE(
			"PolarDB EXTENDED: PASSTHROUGH target_hg=%d\n",
			plan.target_hg);
	}
	return false;
}

/**
 * @brief Whether this command may carry a primary RFQ that must not move session LSN.
 *
 * A transaction-state-only primary RFQ — the "T + x + empty" hint, a transaction
 * marker with no write XIDs — reports where the transaction stands, not a position
 * the session wrote or read. When polardb_query.keep_session_lsn is set for such a
 * response, an accepted primary RFQ feeds only the transaction-split observer and
 * leaves observed_lsn and write_lsn where they are; that short-circuit is allowed
 * only for the commands listed here.
 *
 * Anything not listed always updates session LSN state, so adding or removing a
 * command silently changes session LSN attribution.
 *
 * @param command  Command classification captured when the query was dispatched.
 * @return true when the keep-session-LSN short-circuit may apply to this command.
 */
static bool polardb_query_command_can_preserve_session_lsn(
		PGSQL_QUERY_command command) {
	switch (command) {
	case PGSQL_QUERY_SELECT:
	case PGSQL_QUERY_SHOW:
	case PGSQL_QUERY_EXPLAIN:
	case PGSQL_QUERY_BEGIN:
	case PGSQL_QUERY_COMMIT:
	case PGSQL_QUERY_ROLLBACK:
	case PGSQL_QUERY_ABORT:
	case PGSQL_QUERY_SAVEPOINT:
	case PGSQL_QUERY_ROLLBACK_TO_SAVEPOINT:
	case PGSQL_QUERY_RELEASE_SAVEPOINT:
	case PGSQL_QUERY_SET:
	case PGSQL_QUERY_RESET:
		return true;
	default:
		return false;
	}
}

/**
 * @brief Apply an RFQ that carried a usable LSN to the server cache and the session.
 *
 * The epoch-aware per-server cache check runs first. An actual shared-cache
 * update is serialized with writer reset and rejects an old group/epoch.
 * Repeated equal/lower observations may be coalesced without a shared write; the
 * accepted session observation remains tagged with the request writer scope, so
 * a concurrent writer change clears it during the next request collection before
 * it can become a wait target. On rejection this returns false having changed no
 * session state at all.
 *
 * On acceptance it rebinds the session LSN state to the request writer scope, then
 * updates it. A transaction-state-only primary response (keep_session_lsn set and
 * a command accepted by polardb_query_command_can_preserve_session_lsn()) is a
 * short-circuit: it drives only polardb_observe_transaction_split() and leaves the
 * session positions alone. Otherwise observed_lsn is raised to the RFQ LSN, a
 * write statement also raises write_lsn, and a primary RFQ additionally clears the
 * write_unknown and observed_unknown sticky flags and the one-shot degraded-route
 * warning and drives polardb_observe_transaction_split().
 *
 * @param ctx  Response-path context: backend identity and writer scope, the RFQ
 *             LSN, the write classification and whether split observation is on.
 * @return true when the RFQ was accepted and session state was updated; false when
 *         the per-server cache rejected it and nothing in the session changed.
 */
bool PgSQL_Session::polardb_process_positioned_rfq_lsn(
		PolarDB_ResultProcessContext& ctx) {
#if POLARDB_PROFILE
	const uint64_t session_target_before =
		polardb_session_consistency.target();
#endif // POLARDB_PROFILE
	bool rfq_accepted = false;
	if (ctx.backend_srv && ctx.backend_hg >= 0 &&
			ctx.backend_is_polar_hg) {
		rfq_accepted = PgHGM->polardb_accept_rfq_server_lsn(
			ctx.backend_srv,
			(unsigned int)ctx.backend_hg,
			ctx.lsn,
			polardb_query.request_writer_scope,
			thread);
	}
	const bool backend_is_writer_hostgroup =
		polardb_backend_is_writer_hostgroup(
			ctx.backend_is_polar_hg,
			ctx.backend_hg,
			ctx.backend_writer_hg);
	if (!rfq_accepted) {
		if (ctx.is_write) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_lsn_write_rejected);
		} else {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_lsn_read_rejected);
		}
#if POLARDB_PROFILE
		uint32_t flags = ctx.is_write
			? POLARDB_CONSISTENCY_TRACE_WRITE : 0;
		if (backend_is_writer_hostgroup) {
			flags |= POLARDB_CONSISTENCY_TRACE_PRIMARY;
		}
		proxysql_polardb_consistency_trace(
			static_cast<uint64_t>(
				PolarDB_ConsistencyTraceEvent::RFQ_REJECTED),
			thread_session_id, session_target_before, ctx.lsn,
			polardb_session_consistency.target(),
			polardb_consistency_trace_detail(ctx.backend_hg, flags));
#endif // POLARDB_PROFILE
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: RFQ LSN rejected before session update "
			"request_writer_hg=%d request_epoch=%lu request_valid=%d "
			"backend_hg=%d writer_hg=%d backend_epoch=%lu "
			"backend_epoch_valid=%d lsn=%lu is_write=%d parent=%p\n",
			polardb_query.request_writer_scope.hg,
			(unsigned long)polardb_query.request_writer_scope.epoch,
			polardb_query.request_writer_scope.valid() ? 1 : 0,
			ctx.backend_hg, ctx.backend_writer_hg,
			(unsigned long)ctx.backend_writer_epoch,
			ctx.backend_is_polar_hg ? 1 : 0,
			(unsigned long)ctx.lsn,
			ctx.is_write ? 1 : 0,
			(void*)ctx.myds->myconn->parent);
		return false;
	}
	if (ctx.is_write) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_lsn_write_accepted);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_lsn_read_accepted);
	}
	POLARDB_THREAD_COUNT_ONE(thread, server_lsn_updates_from_rfq);
	POLARDB_TRACE(
		"PolarDB PROCESS_RESULT: accepted %s RFQ LSN=%lu for per-server cache "
		"(%s:%d) digest='%.60s'\n",
		ctx.is_write ? "write" : "read", (unsigned long)ctx.lsn,
		ctx.backend_srv->address, ctx.backend_srv->port,
		ctx.query_digest_text ? ctx.query_digest_text : "(null)");

	polardb_apply_request_scope_to_session_lsn(this, "PROCESS_RESULT");
	if (polardb_query.keep_session_lsn && backend_is_writer_hostgroup &&
			polardb_query_command_can_preserve_session_lsn(ctx.query_cmd)) {
		polardb_observe_transaction_split(
			ctx.myds->myconn, 0, ctx.split_observation_enabled,
			/*primary_source=*/true);
#if POLARDB_PROFILE
		uint32_t flags =
			POLARDB_CONSISTENCY_TRACE_PRIMARY |
			POLARDB_CONSISTENCY_TRACE_KEEP_SESSION_LSN;
		if (ctx.is_write) {
			flags |= POLARDB_CONSISTENCY_TRACE_WRITE;
		}
		proxysql_polardb_consistency_trace(
			static_cast<uint64_t>(
				PolarDB_ConsistencyTraceEvent::RFQ_ACCEPTED),
			thread_session_id, session_target_before, ctx.lsn,
			polardb_session_consistency.target(),
			polardb_consistency_trace_detail(ctx.backend_hg, flags));
#endif // POLARDB_PROFILE
		return true;
	}

	if (ctx.lsn > polardb_session_consistency.observed_lsn) {
		polardb_session_consistency.observed_lsn = ctx.lsn;
#if POLARDB_PROFILE
		polardb_session_consistency.observed_lsn_source_server_token =
			reinterpret_cast<uintptr_t>(ctx.backend_srv);
#endif // POLARDB_PROFILE
	}
	if (backend_is_writer_hostgroup) {
		if (polardb_session_consistency.write_unknown ||
				polardb_session_consistency.observed_unknown) {
			POLARDB_TRACE(
				"PolarDB PROCESS_RESULT: primary RFQ LSN restored missing-LSN sticky flags "
				"(backend_hg=%d writer_hg=%d)\n",
				ctx.backend_hg, ctx.backend_writer_hg);
		}
		polardb_session_consistency.write_unknown = false;
		polardb_session_consistency.observed_unknown = false;
		polardb_route_state.rfq_degraded_route_warning_sent = false;
		polardb_observe_transaction_split(
			ctx.myds->myconn,
			ctx.lsn,
			ctx.split_observation_enabled,
			backend_is_writer_hostgroup);
	}
	if (ctx.is_write) {
		// Advance the session's own-write component; the wait target is
		// max(write_lsn, observed_lsn).
		if (ctx.lsn > polardb_session_consistency.write_lsn) {
			polardb_session_consistency.write_lsn = ctx.lsn;
		}
		POLARDB_TRACE("PolarDB PROCESS_RESULT: write query digest='%.60s' session_write_lsn=%lu session_observed_lsn=%lu primary_source=%d\n",
			ctx.query_digest_text ? ctx.query_digest_text : "(null)",
			(unsigned long)polardb_session_consistency.write_lsn,
			(unsigned long)polardb_session_consistency.observed_lsn,
			backend_is_writer_hostgroup ? 1 : 0);
	}
#if POLARDB_PROFILE
	uint32_t flags = ctx.is_write
		? POLARDB_CONSISTENCY_TRACE_WRITE : 0;
	if (backend_is_writer_hostgroup) {
		flags |= POLARDB_CONSISTENCY_TRACE_PRIMARY;
	}
	proxysql_polardb_consistency_trace(
		static_cast<uint64_t>(
			PolarDB_ConsistencyTraceEvent::RFQ_ACCEPTED),
		thread_session_id, session_target_before, ctx.lsn,
		polardb_session_consistency.target(),
		polardb_consistency_trace_detail(ctx.backend_hg, flags));
#endif // POLARDB_PROFILE
	return true;
}

/**
 * @brief Update session state from a response whose RFQ LSN payload is present
 *        but zero.
 *
 * Precondition: the ReadyForQuery carried an RFQ LSN payload with value 0, so the
 * backend runs the PolarDB startup profile but reported no usable wait target.
 *
 * Returns immediately when the request writer scope does not match the backend's
 * current writer group and epoch, so nothing is attributed across a group change
 * or a failover.
 *
 * Exactly one case is treated as a problem: a write-class statement from the
 * primary, in an LSN consistency mode, that
 * polardb_zero_lsn_payload_can_skip_wait_target() does not clear. It sets the
 * sticky polardb_session_consistency.write_unknown, which pins every later
 * automatic LSN-mode read of this session to the writer until a positioned primary
 * RFQ clears it. Every other case is benign — reads, session-state statements,
 * non-LSN modes — and only feeds polardb_observe_transaction_split() without
 * setting any sticky flag.
 *
 * @param ctx  Response-path context for the completed query.
 */
void PgSQL_Session::polardb_process_zero_rfq_lsn(
		PolarDB_ResultProcessContext& ctx) {
	if (!polardb_request_scope_matches_current_writer(
			this, ctx.backend_is_polar_hg, ctx.backend_writer_hg,
			ctx.backend_writer_epoch)) {
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: skip zero-LSN RFQ payload due to writer "
			"group/epoch request_hg=%d request_epoch=%lu request_valid=%d "
			"current_hg=%d current_epoch=%lu current_valid=%d "
			"backend_hg=%d is_write=%d\n",
			polardb_query.request_writer_scope.hg,
			(unsigned long)polardb_query.request_writer_scope.epoch,
			polardb_query.request_writer_scope.valid() ? 1 : 0,
			ctx.backend_writer_hg,
			(unsigned long)ctx.backend_writer_epoch,
			ctx.backend_is_polar_hg ? 1 : 0,
			ctx.backend_hg, ctx.is_write ? 1 : 0);
		return;
	}
	const bool backend_is_writer_hostgroup =
		polardb_backend_is_writer_hostgroup(
			ctx.backend_is_polar_hg,
			ctx.backend_hg,
			ctx.backend_writer_hg);
	polardb_apply_request_scope_to_session_lsn(this, "PROCESS_RESULT");
	if (polardb_query.keep_session_lsn && backend_is_writer_hostgroup) {
		polardb_observe_transaction_split(
			ctx.myds->myconn, 0, ctx.split_observation_enabled,
			/*primary_source=*/true);
		return;
	}
	const bool safe_without_wait_target =
		!ctx.is_write ||
		polardb_zero_lsn_payload_can_skip_wait_target(ctx.query_digest_text);
	if (ctx.is_write && !safe_without_wait_target) {
		const bool mark_unknown =
			backend_is_writer_hostgroup && ctx.lsn_consistency_mode;
		if (mark_unknown) {
			if (ctx.myds && ctx.myds->myconn) {
				// Even a zero-LSN RFQ carries transaction/split state; keep
				// the split FSM in sync while the sticky unknown-LSN flag
				// remains the routing source of truth.
				polardb_observe_transaction_split(
					ctx.myds->myconn,
					0,
					ctx.split_observation_enabled,
					/*primary_source=*/true);
			}
			const bool first_unknown_write =
				!polardb_session_consistency.write_unknown;
			polardb_session_consistency.write_unknown = true;
			POLARDB_THREAD_COUNT_ONE(thread, write_missing_lsn);
			if (first_unknown_write) {
				proxy_warning(
					"PolarDB PROCESS_RESULT: writer query completed with zero RFQ LSN payload; "
					"automatic LSN-mode reads in this session will use writer until "
					"a later primary RFQ carries non-zero LSN (sess=%p digest='%.60s')\n",
					this,
					ctx.query_digest_text ? ctx.query_digest_text : "(null)");
			}
		}
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: RFQ LSN payload present with zero value "
			"for write-class statement (digest='%.60s') - marked_unknown=%d\n",
			ctx.query_digest_text ? ctx.query_digest_text : "(null)",
			mark_unknown ? 1 : 0);
		return;
	}
	if (backend_is_writer_hostgroup && ctx.myds && ctx.myds->myconn) {
		// RFQ payload presence shows the PolarDB startup profile is active,
		// but value 0 is not a wait target. This is normal for read-only or
		// session-state statements before the backend has a session WAL
		// position. Observe txn status/split flags, but do not poison the
		// session as missing-LSN.
		polardb_observe_transaction_split(
			ctx.myds->myconn,
			0,
			ctx.split_observation_enabled,
			/*primary_source=*/true);
	}
	POLARDB_TRACE(
		"PolarDB PROCESS_RESULT: RFQ LSN payload present with zero value "
		"(is_write=%d digest='%.60s') - no wait target recorded, no missing-LSN sticky flag set\n",
		ctx.is_write ? 1 : 0,
		ctx.query_digest_text ? ctx.query_digest_text : "(null)");
}

/**
 * @brief Update session state from a response whose RFQ carried no LSN payload
 *        at all.
 *
 * Precondition: the ReadyForQuery had no RFQ LSN payload, so no position can be
 * attributed to the session from this response.
 *
 * Returns immediately when the request writer scope does not match the backend's
 * current writer group and epoch. Otherwise it still feeds
 * polardb_observe_transaction_split(), so an idle transaction status closes the
 * transaction and clears the split state, and then records what is unknown: a
 * write on the primary in an LSN mode sets write_unknown; a read on any PolarDB
 * hostgroup in an LSN mode sets observed_unknown, and this is the only place
 * observed_unknown is set. Both flags are sticky until a positioned primary RFQ
 * arrives and clears them, and while either is set automatic LSN-mode reads are
 * routed to the writer.
 *
 * @param ctx  Response-path context for the completed query.
 */
void PgSQL_Session::polardb_process_missing_rfq_lsn(
		PolarDB_ResultProcessContext& ctx) {
	if (!polardb_request_scope_matches_current_writer(
			this, ctx.backend_is_polar_hg, ctx.backend_writer_hg,
			ctx.backend_writer_epoch)) {
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: skip missing-LSN sticky flag due to writer "
			"group/epoch request_hg=%d request_epoch=%lu request_valid=%d "
			"current_hg=%d current_epoch=%lu current_valid=%d "
			"backend_hg=%d is_write=%d\n",
			polardb_query.request_writer_scope.hg,
			(unsigned long)polardb_query.request_writer_scope.epoch,
			polardb_query.request_writer_scope.valid() ? 1 : 0,
			ctx.backend_writer_hg,
			(unsigned long)ctx.backend_writer_epoch,
			ctx.backend_is_polar_hg ? 1 : 0,
			ctx.backend_hg, ctx.is_write ? 1 : 0);
		return;
	}
	const bool backend_is_writer_hostgroup =
		polardb_backend_is_writer_hostgroup(
			ctx.backend_is_polar_hg,
			ctx.backend_hg,
			ctx.backend_writer_hg);
	polardb_apply_request_scope_to_session_lsn(this, "PROCESS_RESULT");
	if (polardb_query.keep_session_lsn && backend_is_writer_hostgroup) {
		polardb_observe_transaction_split(
			ctx.myds->myconn, 0, ctx.split_observation_enabled,
			/*primary_source=*/true);
		return;
	}
	if (backend_is_writer_hostgroup && ctx.myds && ctx.myds->myconn) {
		// Missing-LSN RFQ still carries transaction status and split flags.
		// Feed that through the same observer as positioned RFQ so idle
		// transaction close clears split state and reader-failure writer routing.
		// The session write_unknown/observed_unknown sticky flags below remain the
		// routing source of truth for the missing LSN itself.
		polardb_observe_transaction_split(
			ctx.myds->myconn,
			0,
			ctx.split_observation_enabled,
			/*primary_source=*/true);
	}

	// RFQ carried no LSN. On a PolarDB backend with _polar_send_lsn this should
	// not happen for a writer query. If it does, preserve attribution: missing
	// write LSN sets write_unknown; missing tracked read LSN sets
	// observed_unknown. Do not change mode=off sessions.
	if (ctx.is_write && backend_is_writer_hostgroup &&
			ctx.lsn_consistency_mode) {
		const bool first_unknown_write =
			!polardb_session_consistency.write_unknown;
		polardb_session_consistency.write_unknown = true;
		POLARDB_THREAD_COUNT_ONE(thread, write_missing_lsn);
		if (first_unknown_write) {
			proxy_warning(
				"PolarDB PROCESS_RESULT: writer query completed without RFQ LSN; "
				"automatic LSN-mode reads in this session will use writer until "
				"a later primary RFQ carries LSN (sess=%p digest='%.60s')\n",
				this, ctx.query_digest_text ? ctx.query_digest_text : "(null)");
		}
	} else if (!ctx.is_write) {
		if (ctx.backend_is_polar_hg &&
			ctx.lsn_consistency_mode) {
			const bool first_unknown_observed =
				!polardb_session_consistency.observed_unknown;
			polardb_session_consistency.observed_unknown = true;
			POLARDB_THREAD_COUNT_ONE(thread, read_missing_lsn);
			if (first_unknown_observed) {
				proxy_warning(
					"PolarDB PROCESS_RESULT: read query completed without RFQ LSN while SESSION_LSN "
					"tracking is active; automatic LSN-mode reads in this session will use "
					"writer while the observed-LSN gap remains marked unknown (sess=%p digest='%.60s')\n",
					this,
					ctx.query_digest_text ? ctx.query_digest_text : "(null)");
			}
		}
	}
	POLARDB_TRACE(
		"PolarDB PROCESS_RESULT: no LSN in RFQ (is_write=%d digest='%.60s') - session_write_lsn=%lu session_observed_lsn=%lu write_lsn_unknown=%d observed_lsn_unknown=%d\n",
		ctx.is_write ? 1 : 0,
		ctx.query_digest_text ? ctx.query_digest_text : "(null)",
		(unsigned long)polardb_session_consistency.write_lsn,
		(unsigned long)polardb_session_consistency.observed_lsn,
		polardb_session_consistency.write_unknown ? 1 : 0,
		polardb_session_consistency.observed_unknown ? 1 : 0);
}

/**
 * @brief Produce the LSN to append to the client ReadyForQuery, if any.
 *
 * polardb_query.keep_session_lsn is computed and stored whenever the response can
 * preserve the session LSN, including on the false return path where the client
 * requested no LSN at all. The following polardb_process_result() reads that flag
 * to determine whether the backend RFQ may advance the session write/observed
 * positions, so a false return does not mean "nothing happened".
 *
 * @param conn                    Backend connection that produced the response;
 *                                may be null.
 * @param backend_payload_present true when the backend RFQ carried an LSN payload.
 * @param backend_lsn             LSN from that payload, 0 when absent.
 * @param client_lsn              Output, must not be null. Zeroed on entry and
 *                                written only when this returns true.
 * @return true when *client_lsn holds the LSN to append to the client
 *         ReadyForQuery; false when nothing is appended and *client_lsn stays 0.
 */
bool PgSQL_Session::polardb_prepare_client_ready_lsn(PgSQL_Connection* conn,
		bool backend_payload_present, uint64_t backend_lsn,
		uint64_t* client_lsn) {
	if (!client_lsn) {
		return false;
	}
	*client_lsn = 0;
	if (!polardb_query.profile_enabled) {
		polardb_query.keep_session_lsn = false;
		return false;
	}

	const bool client_requested =
		polardb_route_state.client_rfq_lsn_requested;
	const bool result_has_error = conn && conn->query_result &&
		(conn->query_result->get_result_packet_type() & PGSQL_QUERY_RESULT_ERROR);
	const char txn_status = conn ? conn->get_transaction_status_char() : 0;
	const char* txn_xids = conn ? conn->get_polardb_txn_xids() : nullptr;
	// T + x + empty carries transaction state, not a frontend wait target.
	// Keep marker access ordered because each connection accessor checks PQstatus().
	bool txn_lsn_is_hint = false;
	if (conn && txn_status == 'T') {
		const bool splittable = conn->is_polardb_txn_splittable();
		if (splittable) {
			txn_lsn_is_hint = polardb_rfq_is_prewrite_split_candidate(
				txn_status, txn_xids, splittable,
				conn->is_polardb_txn_wal_pending());
		}
	}
	// A transaction with no write XIDs can close with an unrelated pooled LSN.
	const bool no_write_xids_txn_ended = polardb_txn_has_no_write_xids &&
		txn_status == 'I';
	const bool processing_client_query =
		status == PROCESSING_QUERY ||
		status == PROCESSING_STMT_EXECUTE;
	const bool keep_session_lsn = txn_lsn_is_hint ||
		(conn && !conn->processing_multi_statement && !result_has_error &&
			processing_client_query && no_write_xids_txn_ended);
	if (!client_requested && !keep_session_lsn) {
		return false;
	}

	bool response_from_writer = false;
	if (conn && conn->parent && conn->parent->myhgc && PgHGM) {
		const int backend_hg = (int)conn->parent->myhgc->hid;
		const auto backend_config =
			PgHGM->get_polardb_hg_config((unsigned int)backend_hg);
		response_from_writer = backend_config.is_polardb_hostgroup &&
			polardb_backend_is_writer_hostgroup(
				backend_config.is_polardb_hostgroup,
				backend_hg,
				backend_config.writer_hostgroup) &&
			polardb_query.request_writer_scope.matches(
				PolarDB_WriterScope{
					backend_config.writer_hostgroup,
					backend_config.writer_epoch});
	}

	uint64_t session_target = 0;
	const bool session_scope_matches_request =
		polardb_query.request_writer_scope.valid() &&
			polardb_session_consistency.writer_scope.matches(
				polardb_query.request_writer_scope);
	if (session_scope_matches_request) {
		session_target = polardb_session_consistency.target();
	}
	polardb_query.keep_session_lsn = keep_session_lsn &&
		response_from_writer && session_scope_matches_request;
	if (!client_requested) {
		return false;
	}

	uint64_t completed_wait_target = 0;
	const PolarDB_Query_WaitState& wait = polardb_query.wait;
	if (wait.wrapper_finalized &&
			!wait.timeout_error &&
			wait.spec.type == PolarDB_WaitType::LSN &&
			wait.spec.target > 0 &&
			wait.wait_started_at_us != 0 &&
			conn &&
			conn->polardb_query_wrap_state.wrapper_set_succeeded()) {
		completed_wait_target = wait.spec.target;
	}
	const uint64_t confirmed_read_target =
		std::max(completed_wait_target, polardb_query.wait_bypass_target);

	const PolarDB_ClientRfqDecision decision =
		polardb_client_rfq_decision(
			client_requested,
			backend_payload_present,
			backend_lsn,
			session_target,
			polardb_query.keep_session_lsn,
			response_from_writer,
			confirmed_read_target,
			completed_wait_target);
	if (!decision.include_lsn) {
		if (client_requested && !backend_payload_present &&
				(session_target > 0 || confirmed_read_target > 0)) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread,
				client_rfq_lsn_missing_with_target);
		}
		return false;
	}

	if (decision.raised_to_target) {
		POLARDB_THREAD_COUNT_ONE(thread, client_rfq_lsn_raised_to_target);
		if (decision.raised_by_writer) {
			POLARDB_THREAD_COUNT_ONE(thread,
				client_rfq_lsn_raised_by_writer);
		}
		if (decision.raised_by_wait) {
			POLARDB_THREAD_COUNT_ONE(thread,
				client_rfq_lsn_raised_by_wait);
		}
		POLARDB_TRACE(
			"PolarDB CLIENT_RFQ: raised backend_lsn=%lu to client_lsn=%lu "
			"session_target=%lu confirmed_read_target=%lu writer_response=%d\n",
			(unsigned long)backend_lsn,
			(unsigned long)decision.lsn,
			(unsigned long)session_target,
			(unsigned long)confirmed_read_target,
			response_from_writer ? 1 : 0);
	}

	*client_lsn = decision.lsn;
	return true;
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
 *   - if the RFQ carried an LSN wait target, advance this session's observed LSN
 *     (polardb_session_consistency.observed_lsn = max(prev, lsn)). SESSION_LSN reads wait
 *     on max(write_lsn, observed_lsn) for monotonic session reads. A writer
 *     T + x + empty transaction-state hint updates only the server cache.
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
 * A payload that is present but zero is dispatched separately
 * (PolarDB_RfqLsnPayloadState::ZERO): it records no wait target and, apart from a
 * write-class primary statement in an LSN mode that sets write_unknown, sets no
 * sticky flag. An absent payload on a read over a PolarDB hostgroup in an LSN mode
 * sets polardb_session_consistency.observed_unknown, the read-side counterpart of
 * write_unknown; it too keeps automatic LSN-mode reads on the writer until a
 * positioned primary RFQ clears it.
 *
 * All three paths also drive polardb_observe_transaction_split(), so this stage is
 * not confined to LSN bookkeeping: split observation advances the transaction-split
 * state machine, and a disabled split policy or an idle 'I' transaction status
 * clears the transaction-split state and the reader-failure writer route and
 * releases the temporary split reader connection back to the pool.
 *
 * @param myds              Backend data stream that produced the result.
 * @param query_digest_text Digest text or bounded raw query text.
 * @param query_cmd Stable command classification captured at dispatch time.
 */
void PgSQL_Session::polardb_process_result(
		PgSQL_Data_Stream* myds, const char* query_digest_text,
		PGSQL_QUERY_command query_cmd) {
	if (!polardb_config.is_polardb_enabled || !myds || !myds->myconn) {
		POLARDB_TRACE("PolarDB PROCESS_RESULT: skip (enabled=%d myconn=%p)\n",
			polardb_config.is_polardb_enabled ? 1 : 0, (void*)(myds ? myds->myconn : nullptr));
		return;
	}
	if (!polardb_query.profile_enabled) {
		POLARDB_TRACE(
			"PolarDB PROCESS_RESULT: skip (request profile=off)\n");
		return;
	}
	if (PgHGM && thread) {
		POLARDB_THREAD_COUNT_ONE(thread, result_process);
	}

	// RFQ-only LSN read (no extra round-trip). Value 0 can mean either "payload
	// absent" or "payload present but no usable wait target"; PQhasLSN() separates
	// those cases.
	PgSQL_SrvC* backend_srv = myds->myconn->parent;
	uint64_t lsn = myds->myconn->get_polardb_lsn();
	bool rfq_lsn_payload_present = myds->myconn->has_polardb_lsn_payload();
	const PolarDB_RfqLsnPayloadState rfq_lsn_state =
		polardb_rfq_lsn_payload_state(rfq_lsn_payload_present, lsn);
	bool has_lsn = rfq_lsn_state == PolarDB_RfqLsnPayloadState::POSITIONED;
	if (myds->myconn->polardb_startup_profile.requests_rfq_lsn()) {
		if (rfq_lsn_state == PolarDB_RfqLsnPayloadState::MISSING) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_requested_missing_payload);
		} else if (rfq_lsn_state == PolarDB_RfqLsnPayloadState::ZERO) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, rfq_requested_zero_payload);
		}
	}
	POLARDB_TRACE(
		"PolarDB PROCESS_RESULT: rfq probe requested_lsn=%d payload_present=%d "
		"cached_lsn=%lu pgsql_result=%p async_state=%d result_type=%u "
		"digest='%.60s'\n",
		myds->myconn->polardb_startup_profile.requests_rfq_lsn() ? 1 : 0,
		rfq_lsn_payload_present ? 1 : 0,
		(unsigned long)lsn,
		(void*)myds->myconn->pgsql_result,
		(int)myds->myconn->async_state_machine,
		(unsigned int)myds->myconn->result_type,
		query_digest_text ? query_digest_text : "(null)");
	int backend_hg = -1;
	bool backend_is_polar_hg = false;
	int backend_writer_hg = -1;
	uint64_t backend_writer_epoch = 0;
	PgSQL_HostGroups_Manager::PolarDB_HG_Config backend_config;
	if (backend_srv && backend_srv->myhgc) {
		backend_hg = (int)backend_srv->myhgc->hid;
		backend_config =
			PgHGM->get_polardb_hg_config((unsigned int)backend_hg);
		backend_is_polar_hg =
			backend_config.is_polardb_hostgroup;
		backend_writer_hg = backend_config.is_polardb_hostgroup ?
			backend_config.writer_hostgroup : -1;
		backend_writer_epoch = backend_config.writer_epoch;
	}
	if (PgHGM && thread) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, result_process_write_classify);
		if (query_digest_text) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread,
				result_process_write_classify_text);
		}
	}
	bool is_write =
		PolarDB_Protocol::should_advance_session_write_lsn(query_digest_text, query_cmd);
	const bool lsn_consistency_mode =
		polardb_query.effective_consistency_mode >= 0 &&
		polardb_consistency_mode_uses_lsn_wait(
			polardb_consistency_from_int(
				polardb_query.effective_consistency_mode));

	PolarDB_ResultProcessContext result_ctx;
	result_ctx.myds = myds;
	result_ctx.query_digest_text = query_digest_text;
	result_ctx.query_cmd = query_cmd;
	result_ctx.lsn = lsn;
	result_ctx.is_write = is_write;
	result_ctx.backend_srv = backend_srv;
	result_ctx.backend_hg = backend_hg;
	result_ctx.backend_is_polar_hg = backend_is_polar_hg;
	result_ctx.backend_writer_hg = backend_writer_hg;
	result_ctx.backend_writer_epoch = backend_writer_epoch;
	result_ctx.lsn_consistency_mode = lsn_consistency_mode;
	result_ctx.split_observation_enabled =
		polardb_query.txn_split_enabled;

	if (has_lsn) {
		(void)polardb_process_positioned_rfq_lsn(result_ctx);
	} else if (rfq_lsn_state == PolarDB_RfqLsnPayloadState::ZERO) {
		polardb_process_zero_rfq_lsn(result_ctx);
	} else {
		polardb_process_missing_rfq_lsn(result_ctx);
	}
}

#endif // POLARDB_PROXY
