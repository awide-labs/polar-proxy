/**
 * @file PgSQL_PolarDB_Wrap.cpp
 * @brief Builds the wait wrapper that gives a replica read read-your-writes consistency.
 *
 * When the planner routes a read to a replica that must first catch up to the
 * client's last write, this file rewrites the outgoing query.
 * It prepends three SET statements ahead of the user query:
 *   SET polar_consistency_mode = '<best_effort|strict>';     -- behavior on timeout
 *   SET polar_proxy_wait_timeout_ms = <ms>;                  -- how long to wait
 *   SET polar_xact_split_wait_lsn = '<session target LSN>';  -- the wait condition
 * The replica blocks on the last SET until it has replayed past the session's
 * last write, so the read returns that write instead of stale data.
 *
 * The whole thing is sent as one simple-query ('Q') packet. The rewrite is not
 * visible to the client: the connection layer drops the three leading SET results
 * and forwards only the user query's result (PgSQL_Connection.cpp). See
 * doc/polardb-arch/07-QUERY-WRAPPING.md for the end-to-end flow.
 *
 * These SET statements are per-query backend instructions, not ProxySQL session
 * variables. They are intentionally not tracked or replayed by the normal SET
 * machinery, so future injected settings must be safe on pooled connections:
 * either emit the value on every wrapped read or make the backend clear it when
 * the wrapped read finishes or aborts.
 *
 * Keep the registered underscore GUC names (`polar_*`). Dotted custom-option
 * names (`polardb.*`) can be accepted by PostgreSQL as inert placeholders, which
 * would make the query succeed without making the replica wait run.
 *
 * The pipeline in PgSQL_PolarDB_Flow.cpp drives the helpers here:
 *   - polardb_wait_mode_set_statement() — the consistency-mode SET
 *   - build_wrapped_wait_query()         — assemble the multi-statement string
 *   - polardb_install_wait_wrapper()  — the one place the wrap is installed
 *
 * This file also owns the wait-latency and wait-timeout accounting
 * (polardb_finish_wait, polardb_account_wait_timeout) and the wait-state
 * teardown on RESET/DISCARD (polardb_clear_staged_wait_state_for_reset),
 * because it owns the per-query wait state and the wrapped-query buffer.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"

#if POLARDB_PROXY
#include <cassert>
#endif // POLARDB_PROXY

#if POLARDB_PROXY && POLARDB_DEBUG
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#endif // POLARDB_PROXY && POLARDB_DEBUG

#if POLARDB_PROXY

/** @brief Truncate a query string for safe debug logging. */
[[maybe_unused]] static std::string log_snip(const char* s, size_t n) {
	if (!s) return "(null)";
	const size_t max_len = 200;
	size_t copy_len = n < max_len ? n : max_len;
	std::string out(s, copy_len);
	if (n > max_len) out += "...";
	return out;
}

#if POLARDB_PROFILE
/**
 * @brief Record the reader-selection sample of the per-query wait profile.
 *
 * At most one sample is kept per query: the first call is recorded and every
 * later call is dropped. The call is also a silent no-op unless the wait profile is active,
 * @p wait_spec stages a wait, and @p result reports an acquired reader — so a new
 * call site placed outside those conditions loses its data with no indication.
 *
 * Fills in the selected server token, the LSN gap against the wait target, and
 * the comparison against the best considered reader (only meaningful when both
 * LSN samples are fresh and non-zero), then emits the consistency trace event.
 *
 * @param wait_spec  Wait staged for this query; must report has_wait().
 * @param result     Outcome of reader selection; must report acquired().
 */
void PgSQL_Session::polardb_profile_note_reader_selection(
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_ReaderResult& result) {
	PolarDB_WaitProfileState& profile = polardb_query.wait_profile;
	if (!profile.active || profile.selection_recorded ||
			!wait_spec.has_wait() || !result.acquired()) {
		return;
	}

	profile.selection_recorded = true;
	profile.selected_server_token =
		reinterpret_cast<uintptr_t>(result.srv);
	profile.selected_lsn_known = result.selected_reader_lsn != 0;
	profile.selected_lsn_fresh = result.selected_reader_lsn_fresh;
	if (profile.selected_lsn_known && profile.selected_lsn_fresh) {
		profile.selected_gap_bytes = result.selected_reader_lsn < wait_spec.target
			? wait_spec.target - result.selected_reader_lsn : 0;
	}
	profile.selection_compared =
		result.selected_reader_lsn_fresh &&
		result.best_considered_reader_lsn_fresh &&
		result.selected_reader_lsn != 0 &&
		result.best_considered_reader_lsn != 0;
	profile.selected_behind_best =
		profile.selection_compared &&
		result.selected_reader_lsn < result.best_considered_reader_lsn;
	if (profile.selected_behind_best) {
		profile.selection_loss_bytes =
			result.best_considered_reader_lsn - result.selected_reader_lsn;
	}

	if (result.srv) {
		const uint64_t updated_at = result.srv->lsn_updated_at.load(
			std::memory_order_relaxed);
		const uint64_t now_us = monotonic_time();
		if (updated_at != 0 && now_us >= updated_at) {
			profile.selected_lsn_age_known = true;
			profile.selected_lsn_age_us = now_us - updated_at;
		}
	}

	uint32_t flags = 0;
	if (result.selected_reader_lsn_fresh) {
		flags |= POLARDB_CONSISTENCY_TRACE_SELECTED_LSN_FRESH;
	}
	if (result.best_considered_reader_lsn_fresh) {
		flags |= POLARDB_CONSISTENCY_TRACE_BEST_LSN_FRESH;
	}
	const int hostgroup_id =
		result.srv && result.srv->myhgc
			? static_cast<int>(result.srv->myhgc->hid) : -1;
	const PolarDB_ConsistencyTraceEvent event =
		result.wait_bypass_allowed
			? PolarDB_ConsistencyTraceEvent::READER_WAIT_BYPASSED
			: PolarDB_ConsistencyTraceEvent::READER_WAIT_REQUIRED;
	if (result.wait_bypass_allowed) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, consistency_reader_wait_bypassed);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, consistency_reader_wait_required);
	}
	proxysql_polardb_consistency_trace(
		static_cast<uint64_t>(event), thread_session_id,
		wait_spec.target, result.selected_reader_lsn,
		result.best_considered_reader_lsn,
		polardb_consistency_trace_detail(hostgroup_id, flags));
}

/**
 * @brief Record a reader-selection profile sample for a connection obtained
 *        without going through the reader selector.
 *
 * This is the fallback profiling entry. Because no selector result exists, the
 * server LSN is re-sampled here through srv->polardb_sample_lsn() using a
 * freshness window derived from the wait timeout and the reader plan lag cap, and
 * a synthetic PolarDB_ReaderResult with status ACQUIRED is handed to
 * polardb_profile_note_reader_selection(). The sample therefore reflects the LSN
 * at connection time rather than at selection time, and the best-considered
 * fields stay zero so the profile records no comparison.
 *
 * No-ops unless the wait profile is active, no selection has been recorded yet,
 * @p wait_spec stages a wait, and @p conn has a parent server.
 *
 * @param wait_spec    Wait staged for this query.
 * @param reader_plan  Reader plan whose max_lag_bytes bounds the freshness window.
 * @param conn         Reader connection in use. Ownership stays with the caller.
 */
void PgSQL_Session::polardb_profile_note_reader_connection(
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_Query_ReaderPlan& reader_plan,
		PgSQL_Connection* conn) {
	if (!polardb_query.wait_profile.active ||
			polardb_query.wait_profile.selection_recorded ||
			!wait_spec.has_wait() || !conn || !conn->parent) {
		return;
	}

	PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
	const uint64_t now_us = monotonic_time();
	const uint32_t fresh_ms = polardb_effective_lsn_freshness_ms(
		pgsql_thread___polardb_reader_lsn_max_age_ms,
		wait_spec.timeout_ms,
		reader_plan.max_lag_bytes,
		pgsql_thread___polardb_lag_cap_freshness_ms,
		nullptr);
	const PolarDB_ReaderLsnSample sample =
		srv->polardb_sample_lsn(now_us, fresh_ms);

	PolarDB_ReaderResult result;
	result.conn = conn;
	result.srv = srv;
	result.status = PolarDB_ReaderStatus::ACQUIRED;
	result.selected_reader_lsn = sample.lsn;
	result.selected_reader_lsn_fresh = sample.fresh;
	polardb_profile_note_reader_selection(wait_spec, result);
}

void PgSQL_Session::polardb_profile_note_wait_dispatched(
		PolarDB_Query_WrapperKind wrapper_kind) {
	PolarDB_WaitProfileState& profile = polardb_query.wait_profile;
	if (!profile.active || profile.dispatched_at_us != 0 ||
			(wrapper_kind != PolarDB_Query_WrapperKind::CONSISTENCY_WAIT &&
			 wrapper_kind != PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT)) {
		return;
	}

	const uint64_t now_us = monotonic_time();
	profile.dispatched_at_us = now_us;
	if (profile.prepared_at_us != 0 && now_us >= profile.prepared_at_us) {
		POLARDB_PROFILE_THREAD_COUNT(
			thread, wait_profile_plan_dispatch_sum_us,
			now_us - profile.prepared_at_us);
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, wait_profile_plan_dispatch_count);
	}
}

void PgSQL_Session::polardb_profile_note_wait_set_completed(
		PolarDB_Query_WrapperKind wrapper_kind) {
	PolarDB_WaitProfileState& profile = polardb_query.wait_profile;
	if (!profile.active || profile.wait_set_completed_at_us != 0 ||
			(wrapper_kind != PolarDB_Query_WrapperKind::CONSISTENCY_WAIT &&
			 wrapper_kind != PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT)) {
		return;
	}

	const uint64_t now_us = monotonic_time();
	profile.wait_set_completed_at_us = now_us;
	if (profile.dispatched_at_us != 0 && now_us >= profile.dispatched_at_us) {
		POLARDB_PROFILE_THREAD_COUNT(
			thread, wait_profile_dispatch_wait_set_sum_us,
			now_us - profile.dispatched_at_us);
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, wait_profile_dispatch_wait_set_count);
	}
}

/**
 * @brief Emit the wait-profile histograms for the finished query and clear the
 *        per-query profile.
 *
 * The reported wait interval is the correlated dispatch-to-wait-set interval when
 * both timestamps were captured; @p fallback_elapsed_us is used only when they
 * cannot produce one. The wait-set to query-end interval is counted separately.
 *
 * Call exactly once per query, and last: this ends with profile.reset(), so every
 * subsequent profiling call for the same query silently no-ops.
 *
 * @param fallback_elapsed_us  Wait duration in microseconds to report when the
 *                             dispatch and wait-set timestamps do not correlate.
 */
void PgSQL_Session::polardb_profile_record_wait_completion(
		unsigned long long fallback_elapsed_us) {
	PolarDB_WaitProfileState& profile = polardb_query.wait_profile;
	if (!profile.active) {
		return;
	}

	const uint64_t now_us = monotonic_time();
	unsigned long long correlated_elapsed_us = fallback_elapsed_us;
	if (profile.dispatched_at_us != 0 &&
			profile.wait_set_completed_at_us >= profile.dispatched_at_us) {
		correlated_elapsed_us =
			profile.wait_set_completed_at_us - profile.dispatched_at_us;
	}
	if (profile.wait_set_completed_at_us != 0 &&
			now_us >= profile.wait_set_completed_at_us) {
		POLARDB_PROFILE_THREAD_COUNT(
			thread, wait_profile_wait_set_query_end_sum_us,
			now_us - profile.wait_set_completed_at_us);
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, wait_profile_wait_set_query_end_count);
	}

	polardb_count_wait_profile_completion(
		thread, profile, correlated_elapsed_us);
	profile.reset();
}
#endif // POLARDB_PROFILE

/**
 * @brief Replace the outgoing simple-query packet on a backend data stream.
 *
 * Rebuilds the raw PostgreSQL 'Q' (simple query) packet in place so the backend
 * receives @p query instead of whatever the client sent. The old packet buffer
 * is freed and QueryPtr/QuerySize are repointed at the new body. After this the
 * client's original query text is gone from the wire.
 *
 * Packet layout: byte 0 = 'Q', bytes 1-4 = length in network byte order (length
 * counts the 4 length bytes plus the query plus its terminator), then the query
 * bytes, then a trailing '\0'.
 */
static void replace_simple_query_packet(PgSQL_Data_Stream* myds, const std::string& query) {
	PtrSize_t& pkt = myds->pgsql_real_query.pkt;
	size_t new_size =
		PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD + query.length();
	unsigned char* new_ptr = (unsigned char*)l_alloc(new_size);
	new_ptr[0] = 'Q';
	uint32_t packet_len = htonl(static_cast<uint32_t>(
		query.length() + PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD - 1));
	memcpy(new_ptr + 1, &packet_len, sizeof(packet_len));
	memcpy(
		new_ptr + PGSQL_V3_MESSAGE_HEADER_SIZE,
		query.c_str(), query.length());
	new_ptr[new_size - 1] = '\0';

	l_free(pkt.size, pkt.ptr);
	pkt = {};
	pkt.ptr = new_ptr;
	pkt.size = new_size;
	// QueryPtr/QuerySize address the query body only (after the 'Q' byte and the
	// 4-byte length header), matching how the rest of the data stream reads them.
	myds->pgsql_real_query.QueryPtr =
		(char*)new_ptr + PGSQL_V3_MESSAGE_HEADER_SIZE;
	myds->pgsql_real_query.QuerySize =
		new_size - PGSQL_V3_MESSAGE_HEADER_SIZE;
}

#if POLARDB_PROXY && POLARDB_DEBUG
/**
 * @brief Debug-only one-shot fault injector for the wrapper finalize path.
 *
 * Returns true exactly once per process when the environment variable
 * POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE is set to "1", and false thereafter.
 * Used by tests to force a single wrapper-build failure and exercise the
 * safety path that stops the query instead of sending it unwrapped to a
 * replica. Compiled only in debug builds.
 */
static bool polardb_debug_fail_wrap_finalize_once() {
	const char* env_value = std::getenv("POLARDB_DEBUG_FAIL_WRAP_FINALIZE_ONCE");
	if (!env_value || env_value[0] != '1' || env_value[1] != '\0') {
		return false;
	}

	static std::atomic<bool> consumed{false};
	bool expected = false;
	return consumed.compare_exchange_strong(expected, true);
}

/**
 * @brief Debug-only one-shot fault injector for a backend wrapper SET error.
 *
 * Tests use this to make the first injected SET fail on the backend without
 * simulating an LSN wait timeout. The fault file is consumed when it contains
 * "1", so the next wrapped read receives an ordinary wrapper SET ERROR while
 * later reads continue normally. Compiled only in debug builds.
 */
static bool polardb_debug_fail_wait_set_once() {
	char buf[16] = {0};
	bool enabled = false;
	if (polardb_debug_read_fault_file(
			"POLARDB_DEBUG_WRAP_SET_ERROR_FILE", buf, sizeof(buf))) {
		enabled = strcmp(buf, "1") == 0;
	}

	if (enabled) {
		polardb_debug_clear_fault_file("POLARDB_DEBUG_WRAP_SET_ERROR_FILE");
		POLARDB_TRACE("PolarDB WAIT WRAP: debug forced invalid wrapper SET\n");
	}
	return enabled;
}
#endif // POLARDB_PROXY && POLARDB_DEBUG

/**
 * @brief Return the `SET polar_consistency_mode = '...'` statement for a wait mode.
 *
 * This is the first SET in the wait wrapper. It configures what the replica does
 * when the wait times out: best_effort returns possibly-stale data with a
 * WARNING; strict raises an ERROR. The text never varies, so the two possible
 * statements are function-static literals and a reference to one is returned.
 *
 * @param wait_mode  The wait-timeout behavior (best_effort or strict). This is
 *                   the timeout-behavior knob, not the routing consistency mode.
 * @return Reference to a long-lived SET statement string (do not free or modify).
 *
 * Mode mapping (the numbers are the PolarDB_WaitMode enum values):
 *   1 = best_effort -> SET polar_consistency_mode = 'best_effort' (stale + WARNING on timeout)
 *   2 = strict      -> SET polar_consistency_mode = 'strict'      (ERROR on timeout)
 */
const std::string& PgSQL_Session::polardb_wait_mode_set_statement(
	PolarDB_WaitMode wait_mode) {
	int desired_mode = (int)wait_mode;
	POLARDB_TRACE("PolarDB WAIT: polardb_wait_mode_set_statement desired_mode=%d\n",
		desired_mode);

	static const std::string strict_mode_set =
		"SET polar_consistency_mode = 'strict'; ";
	static const std::string best_effort_mode_set =
		"SET polar_consistency_mode = 'best_effort'; ";

	if (desired_mode == (int)PolarDB_WaitMode::STRICT) {
		POLARDB_TRACE("PolarDB WAIT: injecting strict mode (will ERROR on timeout)\n");
		return strict_mode_set;
	}

	POLARDB_TRACE("PolarDB WAIT: injecting best_effort mode (will WARNING on timeout)\n");
	return best_effort_mode_set;
}

/**
 * @brief Append the wrapped multi-statement wait query to @p out.
 *
 * Appends four parts, in order, to whatever @p out already holds:
 *   SET polar_consistency_mode = '...';      (passed in via @p mode_set)
 *   SET polar_proxy_wait_timeout_ms = <ms>;
 *   SET polar_xact_split_wait_lsn = '<target>';
 *   <original user query>
 *
 * The backend runs all of them; the connection layer (PgSQL_Connection.cpp) drops
 * the SET results and forwards only the user query's result.
 *
 * On failure @p out is unchanged, including any transaction-split prefix already
 * present. A return of 0 means no wrapper was appended and the read must not be
 * sent to a replica unwrapped.
 *
 * @param orig_query  Original user query text.
 * @param orig_len    Length of @p orig_query in bytes.
 * @param wait_state  The query's wait state (wait type + LSN target + timeout).
 * @param mode_set    Consistency-mode SET from polardb_wait_mode_set_statement().
 * @param out         Buffer the wrapper is appended to.
 * @return Number of SET results prepended before the user query, or 0 when no
 *         wrapper was appended.
 */
uint32_t PgSQL_Session::append_wrapped_wait_query(const char* orig_query, size_t orig_len,
	const PolarDB_Query_WaitState& wait_state, const std::string& mode_set, std::string& out) {
	if (!orig_query || orig_len == 0) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — empty query\n");
		return 0;
	}
	if (wait_state.spec.type == PolarDB_WaitType::NONE) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — wait_type=NONE\n");
		return 0;
	}
	// A zero LSN is the "invalid / nothing to wait for" sentinel. Wrapping with
	// it would emit a wait request that blocks on nothing, so bail and let
	// the caller stop the read instead of risking stale replica data.
	if (wait_state.spec.type == PolarDB_WaitType::LSN &&
			XLogRecPtrIsInvalid(static_cast<XLogRecPtr>(wait_state.spec.target))) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — LSN target invalid\n");
		return 0;
	}

	const size_t original_size = out.size();
	// 128 bytes of slack covers the three SET statements.
	out.reserve(out.size() + mode_set.size() + orig_len + 128);
	uint32_t wrapper_stmts = 0;
#if POLARDB_PROXY && POLARDB_DEBUG
	if (polardb_debug_fail_wait_set_once()) {
		out.append("SET polar_xact_split_wait_lsn = 'not-a-lsn'; ");
	} else {
		out.append(mode_set);
	}
#else
	out.append(mode_set);
#endif
	wrapper_stmts++;
	PolarDB_Protocol::append_polar_timeout_set(wait_state.spec.timeout_ms, out);
	wrapper_stmts++;

	// Append the wait statement itself: SET polar_xact_split_wait_lsn = '<target>';
	uint64_t target = wait_state.spec.target;
	POLARDB_TRACE("PolarDB WAIT WRAP: type=%d target=%lu mode_set_len=%zu orig_len=%zu timeout_ms=%u\n",
		(int)wait_state.spec.type, (unsigned long)target, mode_set.size(), orig_len,
		wait_state.spec.timeout_ms);
	if (!PolarDB_Protocol::append_polar_wait_set(wait_state.spec.type, target, out)) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — wait statement unavailable\n");
		out.resize(original_size);
		return 0;
	}
	wrapper_stmts++;

	out.append(orig_query, orig_len);
	POLARDB_TRACE("PolarDB WAIT WRAP: built wrapper_stmts=%u wrapped_query_len=%zu query='%s'\n",
		wrapper_stmts, out.size(), log_snip(out.c_str(), out.size()).c_str());
	return wrapper_stmts;
}

/**
 * @brief Build the wrapped multi-statement wait query into a fresh @p out.
 *
 * Clears @p out and then delegates to append_wrapped_wait_query(), so any content
 * the caller left in the buffer is discarded. Use this variant when @p out carries
 * nothing worth keeping; use append_wrapped_wait_query() when the caller has
 * already placed statements of its own in front of the wrapper.
 *
 * Because the buffer starts empty, a return of 0 always leaves @p out empty — the
 * caller can treat an empty buffer as the build failure signal and must not send
 * the read to a replica unwrapped.
 *
 * @param orig_query  Original user query text.
 * @param orig_len    Length of @p orig_query in bytes.
 * @param wait_state  The query's wait state (wait type + LSN target + timeout).
 * @param mode_set    Consistency-mode SET from polardb_wait_mode_set_statement().
 * @param out         Output buffer; cleared on entry.
 * @return Number of SET results prepended before the user query, or 0 when no
 *         wrapper could be built.
 */
uint32_t PgSQL_Session::build_wrapped_wait_query(const char* orig_query, size_t orig_len,
	const PolarDB_Query_WaitState& wait_state, const std::string& mode_set, std::string& out) {
	out.clear();
	return append_wrapped_wait_query(orig_query, orig_len, wait_state, mode_set, out);
}

/**
 * @brief Stop safely when the wait wrapper cannot be built or installed.
 *
 * Read-your-writes must never be broken silently, so a read that needed a wait
 * wrapper but could not get one is aborted rather than sent to a replica
 * unwrapped. This helper counts the abort, logs the reason, records that later
 * reads in this session must use the writer until RESET, and clears the
 * half-built wrapper and wait state. The caller must stop before running the
 * query and return a clean error to the client.
 *
 * @param reason  Short human-readable cause, included in the error log.
 * @return Always PolarDB_WrapFinalizeResult::FAILED.
 */
PolarDB_WrapFinalizeResult
PgSQL_Session::polardb_fail_wrap_and_disable_session_waits(
		const char* reason) {
	PgHGM->status.polardb_wait_wrap_safety_abort.fetch_add(1, std::memory_order_relaxed);
	proxy_error("PolarDB WRAP: finalize failed: %s, sess=%p\n",
		reason ? reason : "unknown", this);

	// flag the session so later reads use the writer, and require the
	// current caller to stop before RunQuery() so this query's original text is
	// never sent unwrapped to a replica. The sticky flag clears on RESET.
	polardb_route_state.wait_disabled = true;
	polardb_query.wrapped_query_buf.clear();
	polardb_query.reset_wait();

	return PolarDB_WrapFinalizeResult::FAILED;
}

/**
 * @brief Build and install the wait wrapper just before backend dispatch.
 *
 * This is the single place the wrapper is applied. It runs once per query at the
 * ASYNC_IDLE state, after a backend connection and data stream exist. It returns
 * CONTINUE immediately unless reader acquisition activated a wait
 * (`wait_stage == WAITING`), so a direct-dispatch query is a safe no-op here.
 * The method copies the original SQL only when a wrapper is really needed,
 * assembles the wrapped multi-statement string exactly once, and swaps it into
 * the outgoing simple-query (`'Q'`) packet. It then records how many leading SET
 * results the connection must drop.
 *
 * Idempotent: a second call after success is a no-op (protected by
 * wrapper_finalized), so re-entering ASYNC_IDLE is safe.
 *
 * Safety path: any missing precondition routes through
 * polardb_fail_wrap_and_disable_session_waits()
 * and returns FAILED; the caller must then abort the query rather than send it
 * unwrapped. See doc/polardb-arch/07-QUERY-WRAPPING.md sections 4 and 7.
 *
 * The replica is assumed to support the PolarDB wait/timeout GUCs; there is
 * no per-connection capability probe. On a backend that does not, the first SET
 * errors and that error reaches the client (it is not a silent stale read).
 *
 * @param conn  Backend connection (must be non-null for a successful wrap).
 * @param myds  Backend data stream whose pgsql_real_query packet is replaced.
 * @return CONTINUE when the query is safe to run (no wrap needed, or wrap
 *         installed); FAILED when a needed wrapper could not be installed.
 */
PolarDB_WrapFinalizeResult
PgSQL_Session::polardb_install_wait_wrapper(
		PgSQL_Connection* conn, PgSQL_Data_Stream* myds) {
	if (!polardb_wait_active()) return PolarDB_WrapFinalizeResult::CONTINUE;
	if (polardb_query.wait.wrapper_finalized) {
		POLARDB_TRACE("PolarDB WRAP FINALIZE: already finalized, skip\n");
		return PolarDB_WrapFinalizeResult::CONTINUE;
	}

#if POLARDB_PROXY && POLARDB_DEBUG
	if (polardb_debug_fail_wrap_finalize_once()) {
		return polardb_fail_wrap_and_disable_session_waits(
			"debug fault injection");
	}
#endif

	if (!conn || !myds) {
		return polardb_fail_wrap_and_disable_session_waits(
			"missing backend connection or data stream");
	}

	// Most selected readers already satisfy the target, so the wait is bypassed
	// before this function. Copy the SQL only for the uncommon query that will
	// actually be wrapped. QuerySize includes the trailing NUL.
	if (polardb_query.original_query.empty() &&
			myds->pgsql_real_query.QueryPtr &&
			myds->pgsql_real_query.QuerySize > 0) {
		size_t query_size = myds->pgsql_real_query.QuerySize;
		if (myds->pgsql_real_query.QueryPtr[query_size - 1] == '\0') {
			--query_size;
		}
		polardb_query.original_query.assign(
			myds->pgsql_real_query.QueryPtr, query_size);
	}
	if (polardb_query.original_query.empty()) {
		return polardb_fail_wrap_and_disable_session_waits(
			"missing original query snapshot");
	}

	// Build the wrapped query once with all SETs.
	const std::string& mode_set =
		polardb_wait_mode_set_statement(polardb_query.wait.spec.mode);
	const std::string& original_query = polardb_query.original_query;
#if POLARDB_PROFILE
	const unsigned long long build_start_us = monotonic_time();
#endif // POLARDB_PROFILE
	const uint32_t wrapper_stmts = build_wrapped_wait_query(
		original_query.c_str(), original_query.size(), polardb_query.wait,
		mode_set, polardb_query.wrapped_query_buf);
#if POLARDB_PROFILE
	const unsigned long long build_end_us = monotonic_time();
	POLARDB_PROFILE_THREAD_COUNT(thread, wait_wrap_build_sum_us,
		build_end_us >= build_start_us ? build_end_us - build_start_us : 0);
	POLARDB_PROFILE_THREAD_COUNT_ONE(thread, wait_wrap_build_count);
#endif // POLARDB_PROFILE

	if (wrapper_stmts == 0) {
		return polardb_fail_wrap_and_disable_session_waits(
			"wrapper builder produced no statements");
	}
	polardb_query.wait.wrapper_stmts = wrapper_stmts;

	POLARDB_TRACE("PolarDB WRAP FINALIZE: wrapper_stmts=%u "
		"pkt_size=%zu query='%s'\n",
		polardb_query.wait.wrapper_stmts,
		polardb_query.wrapped_query_buf.size(),
		log_snip(polardb_query.wrapped_query_buf.c_str(), polardb_query.wrapped_query_buf.size()).c_str());

	if (!polardb_query.wrapped_query_buf.empty()) {
#if POLARDB_PROFILE
		const unsigned long long install_start_us = monotonic_time();
#endif // POLARDB_PROFILE
		replace_simple_query_packet(myds, polardb_query.wrapped_query_buf);
		// The backend stream now owns the wrapper packet, while CurrentQuery
		// must continue to describe the client's SQL through RequestEnd and
		// query logging. Point it at the request-owned stable copy before the
		// state machine resumes.
		CurrentQuery.QueryPointer = reinterpret_cast<unsigned char*>(
			polardb_query.original_query.data());
		CurrentQuery.QueryLength =
			static_cast<unsigned int>(polardb_query.original_query.size() + 1);
		polardb_query.dispatch_wrapper_stmts = polardb_query.wait.wrapper_stmts;
		polardb_query.dispatch_wrapper_kind = PolarDB_Query_WrapperKind::CONSISTENCY_WAIT;
		if (polardb_query.wait.spec.type == PolarDB_WaitType::LSN) {
			POLARDB_THREAD_COUNT_ONE(thread, wait_lsn_sent);
		}
		// Only the LSN wait type has a per-type sent counter today.
#if POLARDB_PROFILE
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, consistency_wait_wrapper_installed);
		proxysql_polardb_consistency_trace(
			static_cast<uint64_t>(
				PolarDB_ConsistencyTraceEvent::WAIT_WRAPPER_INSTALLED),
			thread_session_id, polardb_query.wait.spec.target,
			polardb_query.wait.wrapper_stmts, 0,
			polardb_consistency_trace_detail(
				myds->mybe ? myds->mybe->hostgroup_id : -1, 0));
		const unsigned long long install_end_us = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(thread, wait_wrap_install_sum_us,
			install_end_us >= install_start_us
				? install_end_us - install_start_us : 0);
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, wait_wrap_install_count);
#endif // POLARDB_PROFILE
	} else {
		return polardb_fail_wrap_and_disable_session_waits(
			"wrapped query is empty");
	}

	polardb_query.wait.wrapper_finalized = true;
	return PolarDB_WrapFinalizeResult::CONTINUE;
}

/**
 * @brief Turn a reader-selection result into direct dispatch or a real wait.
 *
 * The route planner records only reader_wait_spec. This is the first point at
 * which the concrete backend is known. A fresh cached LSN at or above the target
 * is a complete proof for this dispatch, so keep wait inactive and remember the
 * confirmed target for RFQ and failure handling. Only a behind reader activates
 * the wrapper state and its timer.
 */
bool PgSQL_Session::polardb_finish_reader_wait_selection(
		const PolarDB_WaitSpec& wait_spec, bool target_reached,
		int fallback_writer_hg) {
	if (!wait_spec.has_wait()) {
		polardb_query.reset_wait();
		return false;
	}

	if (target_reached) {
		polardb_query.wait_bypass_target = wait_spec.target;
		polardb_query.reset_wait();
		POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_bypassed);
		POLARDB_TRACE(
			"PolarDB DIRECT READ: selected reader reached target_lsn=%lu\n",
			(unsigned long)wait_spec.target);
		return false;
	}

	polardb_query.wait_bypass_target = 0;
	polardb_query.wait.prepare_from_spec(wait_spec);
	polardb_query.wait.wait_stage = PolarDB_WaitStage::WAITING;
	polardb_query.wait.wait_started_at_us = monotonic_time();
	polardb_query.wait.fallback_writer_hg = fallback_writer_hg;
	POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_prepared);
	POLARDB_TRACE(
		"PolarDB WAIT: selected reader is behind target_lsn=%lu; "
		"wrapper activated\n",
		(unsigned long)wait_spec.target);
	return true;
}


/**
 * @brief Finish one wait, update the reader LSN after success, and stop timing.
 */
void PgSQL_Session::polardb_finish_wait(PgSQL_Data_Stream* myds) {
	PolarDB_Query_WaitState& state = polardb_query.wait;
	if (state.wait_started_at_us == 0) return;

	const bool update_reader_lsn = polardb_config.is_polardb_enabled &&
		state.wrapper_finalized && !state.timeout_error &&
		state.spec.type == PolarDB_WaitType::LSN &&
		state.spec.target != 0 && myds && myds->myconn &&
		myds->myconn->polardb_query_wrap_state.wrapper_set_succeeded();
	if (update_reader_lsn) {
		PgSQL_SrvC* srv = myds->myconn->parent;
		if (srv && srv->myhgc && PgHGM) {
			const unsigned int backend_hg = srv->myhgc->hid;
			const bool accepted = PgHGM->polardb_accept_rfq_server_lsn(
				srv, backend_hg, state.spec.target,
				polardb_query.request_writer_scope, thread);
			if (accepted) {
				POLARDB_PROFILE_THREAD_COUNT_ONE(
					thread, wait_target_lsn_cache_advanced);
				POLARDB_TRACE(
					"PolarDB WAIT: advanced reader LSN cache "
					"hg=%u lsn=%lu\n",
					backend_hg, (unsigned long)state.spec.target);
			} else {
				POLARDB_PROFILE_THREAD_COUNT_ONE(
					thread, wait_target_lsn_cache_rejected);
			}
		} else {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, wait_target_lsn_cache_rejected);
		}
	}

	const uint64_t elapsed_us =
		monotonic_time() - state.wait_started_at_us;
	if (state.spec.type == PolarDB_WaitType::LSN) {
		POLARDB_THREAD_COUNT(thread, wait_lsn_sum_us,
			static_cast<unsigned long long>(elapsed_us));
		polardb_count_lsn_wait_elapsed_bucket(
			thread, static_cast<unsigned long long>(elapsed_us),
			/*transaction_split=*/false);
#if POLARDB_PROFILE
		polardb_profile_record_wait_completion(
			static_cast<unsigned long long>(elapsed_us));
#endif // POLARDB_PROFILE
	}
	state.wait_started_at_us = 0;
}

/**
 * @brief Account one confirmed PolarDB wait timeout.
 *
 * Call this only after the caller has confirmed the backend event is a PolarDB
 * proxy wait timeout, for example by checking PG_DIAG_MESSAGE_DETAIL against
 * POLARDB_LSN_WAIT_TIMEOUT_DETAIL. The helper owns counter updates and consumes
 * the wait timer, so repeated observations do not double-count.
 */
bool PgSQL_Session::polardb_account_wait_timeout(const char* source) {
	if (!polardb_wait_active()) {
		POLARDB_TRACE("PolarDB WAIT: skip timeout accounting source=%s reason=inactive\n",
			source ? source : "");
		return false;
	}
	if (polardb_query.wait.wait_started_at_us == 0) {
		POLARDB_TRACE("PolarDB WAIT: skip timeout accounting source=%s reason=already-accounted\n",
			source ? source : "");
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(thread, wait_error_timeout);
	if (polardb_query.wait.spec.type == PolarDB_WaitType::LSN) {
		POLARDB_THREAD_COUNT_ONE(thread, wait_error_lsn_wait_timeout);
	}
	polardb_finish_wait(nullptr);

	POLARDB_TRACE("PolarDB WAIT: timeout accounted source=%s wait_type=%d\n",
		source ? source : "", (int)polardb_query.wait.spec.type);
	return true;
}

/**
 * @brief Clear staged PolarDB wait/notice state on a RESET / RESET ALL /
 *        DISCARD ALL / RESET CONNECTION command.
 *
 * Tears down the per-query wait, reader, and wrapper state and any pending
 * notices, leaves and clears any reader-capacity wait this session is parked in,
 * and clears the per-session writer-fallback and degraded-route log flags.
 * Replica reads can therefore resume after a RESET.
 *
 * It does NOT return or destroy backend connections and does NOT clear the session
 * write/observed LSNs. Those LSNs record committed positions this client has
 * already observed; a RESET clears session configuration, not that history, so
 * read-your-writes still holds after a RESET. It also deliberately leaves the
 * transaction-split reader state and polardb_txn_has_no_write_xids alone — those
 * belong to the session-recycle path and are cleared only by
 * polardb_clear_session_state_for_recycle().
 *
 * @param reset_override When true, also clears the per-session PolarDB
 *                       consistency-mode override and the transaction-split
 *                       warmup mode. Callers set it for RESET ALL, DISCARD ALL,
 *                       and RESET CONNECTION. RESET proxysql.<name> clears only
 *                       the named override after this helper clears transient
 *                       state.
 */
void PgSQL_Session::polardb_clear_staged_wait_state_for_reset(bool reset_override) {
	polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::READER_UNAVAILABLE);
	polardb_reader_capacity_wait.reset();
	polardb_query.reset_for_new_query();
	discard_pending_notices();
	polardb_route_state.clear_resettable();
	if (reset_override) {
		polardb_set_session_consistency_mode(-1);
		polardb_set_txn_split_warmup_mode(-1);
	}
}

/**
 * @brief Tear down all PolarDB session state when the session itself is recycled.
 *
 * This is the full teardown, wider than the RESET / DISCARD handler
 * polardb_clear_staged_wait_state_for_reset(). On top of the per-query wait,
 * reader-capacity wait and pending notices, it clears
 * polardb_txn_has_no_write_xids and the transaction-split reader state, and it
 * uses polardb_route_state.clear_session() — which drops the negotiated client
 * RFQ-LSN capability and the cached ReaderPool identity hash, not just the
 * resettable flags. Nothing carries over to the next client on this session.
 *
 * The per-session PolarDB overrides are not touched here; those belong to the
 * RESET ALL / DISCARD ALL / RESET CONNECTION handler.
 */
void PgSQL_Session::polardb_clear_session_state_for_recycle() {
	polardb_txn_has_no_write_xids = false;
	polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::READER_UNAVAILABLE);
	polardb_reader_capacity_wait.reset();
	polardb_query.reset_for_new_query();
	polardb_route_state.clear_session();
	polardb_teardown_transaction_reader_state("session_reset", /*want_reuse=*/false);
	discard_pending_notices();
}

/**
 * @brief Run the single per-request PolarDB teardown at the end of a query.
 *
 * Call once per request, with the backend data stream that request ran on. The
 * steps are order-sensitive: any transaction-split reader that no longer belongs
 * to @p myds is reconciled first (debug builds assert that an active split reader
 * is the one for @p myds), then polardb_finish_wait() updates the reader LSN
 * after success and consumes the timer. Finally the query state and pending
 * notices are reset.
 *
 * Parse or Describe may finish while Bind/Execute messages from the same
 * extended-protocol Sync frame are still queued. On that successful
 * intermediate boundary, transient state is cleared while the captured route
 * and response policy are retained for Execute. Failure and final-Execute paths
 * always perform the full reset.
 *
 * @param myds  Backend data stream the request used. Ownership is unaffected.
 * @param called_on_failure  True when the request ended in failure: also clears
 *              polardb_txn_has_no_write_xids and suppresses the shared LSN-cache
 *              advance, because the reader proved nothing about the wait target.
 */
void PgSQL_Session::polardb_clear_request_state_for_query_end(
		PgSQL_Data_Stream* myds, bool called_on_failure) {
	if (called_on_failure) {
		polardb_txn_has_no_write_xids = false;
	}
	if (polardb_reader_capacity_wait.active) {
		polardb_leave_reader_capacity_wait(
			PolarDB_ReaderStatus::READER_UNAVAILABLE);
	}
	if (polardb_reader_capacity_wait.result_valid) {
		polardb_reader_capacity_wait.reset();
	}
	const bool current_txn_wait_reader =
		polardb_txn_reader.wait_read_active &&
		polardb_txn_reader.backend &&
		polardb_txn_reader.backend->server_myds == myds;
#if POLARDB_DEBUG
	assert(!polardb_txn_reader.wait_read_active || current_txn_wait_reader);
#endif // POLARDB_DEBUG
	if (polardb_txn_reader.wait_read_active && !current_txn_wait_reader) {
		POLARDB_THREAD_COUNT_ONE(thread, txn_wait_reader_reconciled);
		polardb_reconcile_txn_wait_read_end("request_end_stale", false);
	}
	polardb_finish_wait(called_on_failure ? nullptr : myds);
	const bool continue_extended_request =
		!called_on_failure &&
		!extended_query_frame.empty() &&
		(extended_query_phase &
			(EXTQ_PHASE_PROCESSING_PARSE |
			 EXTQ_PHASE_PROCESSING_DESCRIBE));
	if (continue_extended_request) {
		polardb_query.reset_between_extended_messages();
	} else {
		polardb_query.reset_for_new_query();
	}
	discard_pending_notices();
}

#endif // POLARDB_PROXY
