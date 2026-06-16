/**
 * @file PgSQL_PolarDB_Wrap.cpp
 * @brief Builds the wait wrapper that gives a replica read read-your-writes consistency.
 *
 * When the planner decides to send a read to a replica but wants it to first
 * catch up to the client's last write, this file rewrites the outgoing query.
 * It prepends three SET statements ahead of the user query:
 *   SET polar_consistency_mode = '<best_effort|strict>';     -- behavior on timeout
 *   SET polar_proxy_wait_timeout_ms = <ms>;                  -- how long to wait
 *   SET polar_xact_split_wait_lsn = '<session target LSN>';  -- the wait gate
 * The replica blocks on the last SET until it has replayed past the session's
 * last write, so the read sees that write instead of stale data.
 *
 * The whole thing is sent as one simple-query ('Q') packet. The client never
 * sees the change: the connection layer drops the three leading SET results and
 * forwards only the user query's result (PgSQL_Connection.cpp). See
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
 *   - build_polar_consistency_mode_set() — the consistency-mode SET
 *   - build_wrapped_wait_query()         — assemble the multi-statement string
 *   - finalize_wait_timeout_injection()  — the one place the wrap is installed
 *
 * This file also owns the wait-latency and wait-timeout accounting
 * (record_wait_latency, polardb_account_wait_timeout) and the wait-state
 * teardown on RESET/DISCARD (polardb_clear_staged_wait_state_for_reset),
 * because it owns the per-query wait state and the wrapped-query buffer.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"

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
	size_t new_size = 1 + 4 + query.length() + 1;
	unsigned char* new_ptr = (unsigned char*)l_alloc(new_size);
	new_ptr[0] = 'Q';
	uint32_t packet_len = htonl((uint32_t)(4 + query.length() + 1));
	memcpy(new_ptr + 1, &packet_len, 4);
	memcpy(new_ptr + 5, query.c_str(), query.length());
	new_ptr[new_size - 1] = '\0';

	l_free(pkt.size, pkt.ptr);
	pkt.ptr = new_ptr;
	pkt.size = new_size;
	// QueryPtr/QuerySize address the query body only (after the 'Q' byte and the
	// 4-byte length header), matching how the rest of the data stream reads them.
	myds->pgsql_real_query.QueryPtr = (char*)new_ptr + 5;
	myds->pgsql_real_query.QuerySize = new_size - 5;
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
 * "1", so the next wrapped read sees an ordinary wrapper SET ERROR while later
 * reads continue normally. Compiled only in debug builds.
 */
static bool polardb_debug_fail_wait_set_once() {
	char buf[16] = {0};
	bool enabled = false;
	if (polardb_debug_consume_fault_file(
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
 * This is the first SET in the wait wrapper. It tells the replica what to do
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
const std::string& PgSQL_Session::build_polar_consistency_mode_set(
	PolarDB_WaitMode wait_mode) {
	int desired_mode = (int)wait_mode;
	POLARDB_TRACE("PolarDB WAIT: build_polar_consistency_mode_set desired_mode=%d\n",
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
 * @brief Assemble the wrapped multi-statement wait query into @p out.
 *
 * Builds one SQL string from four parts:
 *   SET polar_consistency_mode = '...';      (passed in via @p mode_set)
 *   SET polar_proxy_wait_timeout_ms = <ms>;
 *   SET polar_xact_split_wait_lsn = '<target>';
 *   <original user query>
 *
 * The backend runs all four statements; the connection layer
 * (PgSQL_Connection.cpp) drops the three SET results and forwards only the user
 * query's result. The @p out buffer is reused across queries to avoid
 * reallocating, so it is cleared on entry.
 *
 * Safety contract: @p out is left empty (no wrapper produced) when there is
 * nothing safe to wrap — an empty query, no wait, or an LSN wait whose target is
 * zero. The caller treats an empty buffer as a build failure and must not send
 * the read to a replica unwrapped.
 *
 * @param orig_query  Original user query text.
 * @param orig_len    Length of @p orig_query in bytes.
 * @param wait_state  The query's wait state (wait type + LSN target + timeout).
 * @param mode_set    Consistency-mode SET from build_polar_consistency_mode_set().
 * @param out         Output buffer for the wrapped SQL; cleared on entry, empty on skip.
 */
void PgSQL_Session::build_wrapped_wait_query(const char* orig_query, size_t orig_len,
	const PolarDB_Query_WaitState& wait_state, const std::string& mode_set, std::string& out) {
	out.clear();
	if (!orig_query || orig_len == 0) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — empty query\n");
		return;
	}
	if (wait_state.spec.type == PolarDB_WaitType::NONE) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — wait_type=NONE\n");
		return;
	}
	// A zero LSN is the "invalid / nothing to wait for" sentinel. Wrapping with
	// it would emit a wait request that blocks on nothing, so bail and let
	// the caller stop the read instead of risking stale replica data.
	if (wait_state.spec.type == PolarDB_WaitType::LSN &&
			XLogRecPtrIsInvalid(static_cast<XLogRecPtr>(wait_state.spec.target))) {
		POLARDB_TRACE("PolarDB WAIT WRAP: skip — LSN target invalid\n");
		return;
	}

	// 128 bytes of slack covers the three SET statements; the buffer grows if needed.
	out.reserve(mode_set.size() + orig_len + 128);
#if POLARDB_PROXY && POLARDB_DEBUG
	if (polardb_debug_fail_wait_set_once()) {
		out.append("SET polar_xact_split_wait_lsn = 'not-a-lsn'; ");
	} else {
		out.append(mode_set);
	}
#else
	out.append(mode_set);
#endif
	PolarDB_Protocol::append_polar_timeout_set(wait_state.spec.timeout_ms, out);

	// Append the wait gate itself: SET polar_xact_split_wait_lsn = '<target>';
	uint64_t target = wait_state.spec.target;
	POLARDB_TRACE("PolarDB WAIT WRAP: type=%d target=%lu mode_set_len=%zu orig_len=%zu timeout_ms=%u\n",
		(int)wait_state.spec.type, (unsigned long)target, mode_set.size(), orig_len,
		wait_state.spec.timeout_ms);
	PolarDB_Protocol::append_polar_wait_set(wait_state.spec.type, target, out);

	out.append(orig_query, orig_len);
	POLARDB_TRACE("PolarDB WAIT WRAP: built wrapped_query_len=%zu query='%s'\n",
		out.size(), log_snip(out.c_str(), out.size()).c_str());
}

/**
 * @brief Stop safely when the wait wrapper cannot be built or installed.
 *
 * Read-your-writes must never be broken silently, so a read that needed a wait
 * wrapper but could not get one is aborted rather than sent to a replica
 * unwrapped. This helper counts the abort, logs the reason, latches
 * polardb_wait_disabled so later reads in this session go to the writer until
 * RESET, and clears the half-built wrapper and wait state. The caller must stop
 * before running the query and return a clean error to the client.
 *
 * @param reason  Short human-readable cause, included in the error log.
 * @return Always PolarDB_WrapFinalizeResult::FAILED.
 */
PolarDB_WrapFinalizeResult PgSQL_Session::fail_wait_wrap_finalize(const char* reason) {
	PgHGM->status.polardb_wait_wrap_safety_abort.fetch_add(1, std::memory_order_relaxed);
	proxy_error("PolarDB WRAP: finalize failed: %s, sess=%p\n",
		reason ? reason : "unknown", this);

	// Latch the session so later reads use the writer, and require the
	// current caller to stop before RunQuery() so this query's original text is
	// never sent unwrapped to a replica. The latch clears on RESET.
	polardb_wait_disabled = true;
	polardb_query.wrapped_query_buf.clear();
	polardb_query.reset_wait();

	return PolarDB_WrapFinalizeResult::FAILED;
}

/**
 * @brief Build and install the wait wrapper just before backend dispatch.
 *
 * This is the single place the wrapper is applied. It runs once per query at the
 * ASYNC_IDLE state, after a backend connection and data stream exist. It returns
 * CONTINUE immediately unless the plan/execute stage already staged a wait
 * (wait_stage == WAITING), so an un-staged query is a safe no-op here. When a
 * wait is staged, the plan stage already saved the intent (the prepared wait spec
 * and a snapshot of the original query text); this method assembles the wrapped
 * multi-statement string exactly once and swaps it into the outgoing simple-query
 * ('Q') packet, then records how many leading SET results the connection must drop.
 *
 * Idempotent: a second call after success is a no-op (guarded by
 * wrapper_finalized), so re-entering ASYNC_IDLE is safe.
 *
 * Safety path: any missing precondition routes through fail_wait_wrap_finalize()
 * and returns FAILED; the caller must then abort the query rather than send it
 * unwrapped. See doc/polardb-arch/07-QUERY-WRAPPING.md sections 4 and 7.
 *
 * The replica is assumed to understand the PolarDB wait/timeout GUCs; there is
 * no per-connection capability probe. On a backend that does not, the first SET
 * errors and that error reaches the client (it is not a silent stale read).
 *
 * @param conn  Backend connection (must be non-null for a successful wrap).
 * @param myds  Backend data stream whose pgsql_real_query packet is replaced.
 * @return CONTINUE when the query is safe to run (no wrap needed, or wrap
 *         installed); FAILED when a needed wrapper could not be installed.
 */
PolarDB_WrapFinalizeResult PgSQL_Session::finalize_wait_timeout_injection(PgSQL_Connection* conn, PgSQL_Data_Stream* myds) {
	if (!polardb_wait_active()) return PolarDB_WrapFinalizeResult::CONTINUE;
	if (polardb_query.wait.wrapper_finalized) {
		POLARDB_TRACE("PolarDB WRAP FINALIZE: already finalized, skip\n");
		return PolarDB_WrapFinalizeResult::CONTINUE;
	}

#if POLARDB_PROXY && POLARDB_DEBUG
	if (polardb_debug_fail_wrap_finalize_once()) {
		return fail_wait_wrap_finalize("debug fault injection");
	}
#endif

	if (!conn || !myds) {
		return fail_wait_wrap_finalize("missing backend connection or data stream");
	}

	if (polardb_query.wait.original_query.empty()) {
		return fail_wait_wrap_finalize("missing original query snapshot");
	}

	// Build the wrapped query once with all SETs.
	const std::string& mode_set =
		build_polar_consistency_mode_set(polardb_query.wait.spec.mode);
	const std::string& original_query = polardb_query.wait.original_query;
	build_wrapped_wait_query(
		original_query.c_str(), original_query.size(), polardb_query.wait,
		mode_set, polardb_query.wrapped_query_buf);

	// Record how many leading SET results the connection layer must skip before
	// the user query's result. build_wrapped_wait_query() prepends exactly the
	// three SETs named by POLARDB_WAIT_WRAPPER_SET_COUNT.
	polardb_query.wait.wrapper_stmts = POLARDB_WAIT_WRAPPER_SET_COUNT;

	POLARDB_TRACE("PolarDB WRAP FINALIZE: wrapper_stmts=%u "
		"pkt_size=%zu query='%s'\n",
		polardb_query.wait.wrapper_stmts,
		polardb_query.wrapped_query_buf.size(),
		log_snip(polardb_query.wrapped_query_buf.c_str(), polardb_query.wrapped_query_buf.size()).c_str());

	if (!polardb_query.wrapped_query_buf.empty()) {
		replace_simple_query_packet(myds, polardb_query.wrapped_query_buf);
		polardb_query.dispatch_wrapper_stmts = polardb_query.wait.wrapper_stmts;
		polardb_query.dispatch_wrapper_kind = PolarDB_Query_WrapperKind::CONSISTENCY_WAIT;
		if (polardb_query.wait.spec.type == PolarDB_WaitType::LSN) {
			POLARDB_THREAD_COUNT_ONE(thread, wait_lsn_sent);
		}
		// Only the LSN wait type has a per-type sent counter today.
	} else {
		return fail_wait_wrap_finalize("wrapped query is empty");
	}

	polardb_query.wait.wrapper_finalized = true;
	return PolarDB_WrapFinalizeResult::CONTINUE;
}


/**
 * @brief Record an LSN wait's elapsed latency and clear its start timer.
 *
 * Called when a wrapped wait read finishes, on every path: normal query end, a
 * proven wait timeout, and a failed wrapper SET. It adds the elapsed wait time
 * to the running total PolarDB_Wait_LSN_Sum_Us. Repeat calls are safe: once the
 * timer is zeroed the next call returns early, so the same wait is never counted
 * twice. The matching count PolarDB_Wait_LSN_Sent (the divisor for an average
 * wait) is bumped separately at install time in finalize_wait_timeout_injection(),
 * not here. Only LSN waits add to the sum; PolarDB_WaitType is NONE or LSN only.
 *
 * @param state The query wait state whose wait_started_at_us is consumed and zeroed.
 */
void PgSQL_Session::record_wait_latency(PolarDB_Query_WaitState& state) {
	if (state.wait_started_at_us == 0) {
		return;   // no wait was active (or already accounted)
	}
	uint64_t elapsed_us = monotonic_time() - state.wait_started_at_us;
	if (state.spec.type == PolarDB_WaitType::LSN) {
		POLARDB_THREAD_COUNT(thread, wait_lsn_sum_us,
			static_cast<unsigned long long>(elapsed_us));
	}
	state.wait_started_at_us = 0;
}

/**
 * @brief Account one proven PolarDB wait timeout.
 *
 * Call this only after the caller has proved the backend event is a PolarDB
 * proxy wait timeout, for example by checking PG_DIAG_MESSAGE_DETAIL against
 * POLARDB_LSN_WAIT_TIMEOUT_DETAIL. The helper owns counter updates and consumes
 * wait_started_at_us through record_wait_latency(), so repeated observations of
 * the same backend event do not double-count.
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
	record_wait_latency(polardb_query.wait);

	POLARDB_TRACE("PolarDB WAIT: timeout accounted source=%s wait_type=%d\n",
		source ? source : "", (int)polardb_query.wait.spec.type);
	return true;
}

/**
 * @brief Clear staged PolarDB wait/notice state on a RESET / RESET ALL /
 *        DISCARD ALL / RESET CONNECTION command.
 *
 * Tears down the per-query wait, reader, and wrapper state and any pending
 * notices, and re-arms two per-session latches: the writer-fallback safety
 * latch (polardb_wait_disabled) and the one-shot degraded-route log guard. Replica
 * reads can therefore resume after a RESET.
 *
 * It does NOT dispose backend connections and does NOT clear the session
 * write/observed LSNs. Those LSNs record committed positions this client has
 * already observed; a RESET clears session configuration, not that history, so
 * read-your-writes still holds after a RESET. The consistency-mode override is
 * cleared only when @p reset_override is true.
 *
 * @param reset_override When true, also clears the per-session consistency-mode
 *                       override. Callers set it for RESET ALL, DISCARD ALL,
 *                       RESET CONNECTION, and RESET proxysql.polardb_consistency_mode.
 */
void PgSQL_Session::polardb_clear_staged_wait_state_for_reset(bool reset_override) {
	polardb_query.reset_for_new_query();
	clear_pending_notices(/*free_buffers=*/true);  // FREE the queue, not just null it
	polardb_wait_disabled = false;          // RESET starts from normal wait behavior
	polardb_rfq_degraded_route_warning_sent = false;
	if (reset_override) {
		polardb_set_session_override(-1);
	}
}

#endif // POLARDB_PROXY
