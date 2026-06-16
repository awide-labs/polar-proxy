/**
 * @file PgSQL_PolarDB_Notices.cpp
 * @brief PolarDB NOTICE/WARNING capture and forwarding.
 *
 * A best_effort LSN wait surfaces a timeout as a backend WARNING/NOTICE while
 * the wrapped user query obtains its snapshot. The prepended SET result sets are
 * consumed and dropped while the wrapped query runs (PgSQL_Connection::handler()),
 * so the client never sees them, and that consumption can rotate query_result.
 * These helpers capture the timeout notice and store it on the session; the
 * captured notice is later prepended to the client output ahead of the user
 * result (PgSQL_Session::PgSQL_Result_to_PgSQL_wire()).
 *
 * The same capture-and-forward path also delivers a WARNING that ProxySQL
 * generates itself (PostgreSQL did not send it) when a read is routed to a
 * reader with no enforceable LSN wait target
 * (polardb_enqueue_degraded_rfq_notice).
 */

#include <arpa/inet.h>
#include <atomic>
#include <cstdio>
#include <cstring>

#include "PgSQL_Connection.h"
#include "PgSQL_Session.h"
#include "PgSQL_Data_Stream.h"
#if POLARDB_PROXY
#include "PgSQL_PolarDB.h"
#endif
#include "PgSQL_Protocol.h"
#include "proxysql.h"

#if POLARDB_PROXY

/**
 * @brief Clear any queued NoticeResponse packets for this session.
 *
 * Notices captured from skipped prepended-SET results are forwarded with the
 * SELECT result; we clear the queue on completion or error to avoid duplicates
 * and leaks.
 *
 * @note Always deletes and nullifies the pending_notices queue, even if
 *       free_buffers is false (the packet bytes may have been transferred to the
 *       client output array, which then owns them).
 * @param free_buffers If true, free the packet buffers before clearing.
 */
void PgSQL_Session::clear_pending_notices(bool free_buffers) {
	if (!pending_notices) {
		return;
	}
	while (pending_notices->len > 0) {
		if (free_buffers) {
			PtrSize_t ps;
			pending_notices->remove_index(pending_notices->len - 1, &ps);
			if (ps.ptr) {
				l_free(ps.size, ps.ptr);
			}
		} else {
			pending_notices->remove_index(pending_notices->len - 1, nullptr);
		}
	}
	delete pending_notices;
	pending_notices = nullptr;
}

/**
 * @brief Forward queued NoticeResponse packets to the client output array.
 *
 * Ownership of each packet buffer moves to PSarrayOUT. The queue object is then
 * cleared without freeing the packet bytes, matching the normal result-forward
 * ownership rule.
 */
void PgSQL_Session::polardb_flush_pending_notices_to_client() {
	if (!pending_notices || pending_notices->len == 0 ||
			!client_myds || !client_myds->PSarrayOUT) {
		return;
	}

	for (unsigned int i = 0; i < pending_notices->len; i++) {
		PtrSize_t* ps = pending_notices->index(i);
		if (ps && ps->ptr && ps->size > 0) {
			client_myds->PSarrayOUT->add(ps->ptr, ps->size);
		}
	}
	clear_pending_notices(/*free_buffers=*/false);
}

/**
 * @brief Enqueue a NoticeResponse packet to be forwarded with the next result.
 *
 * A best_effort LSN timeout can emit WARNING/NOTICE while the wrapped SELECT is
 * running. We capture the NoticeResponse packet here and forward it before the
 * SELECT result.
 *
 * @param pkt  Packet buffer (owned by the session after enqueue).
 * @param size Packet size in bytes.
 */
void PgSQL_Session::enqueue_pending_notice(unsigned char* pkt, unsigned int size) {
	if (!pkt || size == 0) {
		return;
	}
	if (!pending_notices) {
		// Lazily allocate on the first notice.
		pending_notices = new PtrSizeArray();
	}
	pending_notices->add(pkt, size);
}

/**
 * @brief Build a PostgreSQL NoticeResponse packet and queue it for the client.
 *
 * Allocates one packet from the local allocator, serializes the given fields
 * into wire form, and hands it to enqueue_pending_notice(), which takes
 * ownership. On a serialization failure the packet is freed here and nothing is
 * queued.
 *
 * @return true if the packet was built and queued; false on failure.
 */
bool PgSQL_Session::polardb_enqueue_notice_packet(
		const char* severity,
		const char* sqlstate,
		const char* primary,
		const char* detail,
		const char* severity_nonlocalized) {
	const unsigned int size = polardb_notice_response_packet_size(
		severity, sqlstate, primary, detail, severity_nonlocalized);
	unsigned char* pkt = (unsigned char*)l_alloc(size);
	const unsigned int written = polardb_write_notice_response_packet(
		pkt, size, severity, sqlstate, primary, detail, severity_nonlocalized);
	if (written == 0) {
		// Serialization returned 0. The buffer was sized from the same fields the
		// writer serializes, so it is never too small here; this only happens on a
		// null/short buffer. Free the packet so it is not leaked, since enqueue
		// never took ownership.
		l_free(size, pkt);
		return false;
	}
	// On success enqueue_pending_notice() owns pkt; the session frees it later.
	enqueue_pending_notice(pkt, written);
	return true;
}

/**
 * @brief Queue a WARNING that ProxySQL generates itself when a read is routed to
 *        a reader without a usable LSN wait target.
 *
 * The ReadyForQuery (RFQ) message normally carries the writer's LSN, which a
 * replica read waits for to guarantee read-your-writes. When the admin variable
 * pgsql-polardb_route_rfq_policy is best_effort, ProxySQL may still send the
 * read to a reader even though no enforceable wait target is available, so the
 * read can return stale data. This builds a client-visible WARNING (PostgreSQL
 * did not send it) so the application is told. The pending-notice flush sends
 * this warning ahead of the next backend result, the same ordering used for
 * wrapped read timeout notices.
 *
 * @param reason    short machine-readable cause (used in the Detail field).
 * @param reader_hg reader hostgroup the read was routed to.
 * @param writer_hg writer hostgroup this read should have tracked.
 */
void PgSQL_Session::polardb_enqueue_degraded_rfq_notice(const char* reason,
	int reader_hg, int writer_hg) {
	const char* severity = "WARNING";
	const char* sqlstate = "01000"; // SQL standard "warning" class (no subclass)
	const char* primary =
		"PolarDB best_effort RFQ route has no enforceable LSN wait target; read may be stale";
	char detail[192];
	snprintf(detail, sizeof(detail),
		"reason=%s reader_hg=%d writer_hg=%d",
		reason ? reason : "unknown",
		reader_hg,
		writer_hg);

	if (!polardb_enqueue_notice_packet(severity, sqlstate, primary, detail)) {
		return;
	}

	POLARDB_TRACE(
		"PolarDB PLAN: queued degraded RFQ NoticeResponse pending_len=%u\n",
		pending_notices ? pending_notices->len : 0);
}

/**
 * @brief Convenience overload that derives the notice fields from a route plan.
 *
 * Pulls the human-readable reason and the reader/writer hostgroup ids out of the
 * routing decision, then forwards to the explicit-argument overload above.
 */
void PgSQL_Session::polardb_enqueue_degraded_rfq_notice(
	const PolarDB_Query_RoutePlan& plan,
	const PolarDB_Query_RouteCtx& route_ctx) {
	polardb_enqueue_degraded_rfq_notice(
		polardb_route_action_reason_name(plan.action_reason),
		plan.target_hg,
		route_ctx.writer_scope.hg);
}

/**
 * @brief Handle PolarDB LSN wait-timeout notices emitted while a wrapped query runs.
 *
 * Called from notice_handler_cb() for every backend notice. This helper first
 * proves that the notice belongs to the current active LSN wait using PolarDB's
 * stable proxy wait-timeout detail marker plus wrapper state. It then performs
 * timeout accounting and applies the forwarding ownership rule below: generic
 * forwarding owns user-result notices; the PolarDB pending path only rescues
 * notices attached to hidden wrapper SET results. Other notices, including
 * user-generated text that looks like an LSN timeout, are ignored here; the
 * generic store in notice_handler_cb() already ran for them (or had no result
 * to store into).
 *
 * @note Runs in the libpq NoticeReceiver callback on the backend thread; keep it
 *       cheap and non-blocking. conn->myds and the session reached through it may
 *       be null during connection teardown.
 *
 * @param conn   Active backend connection supplied by the libpq notice receiver.
 * @param result libpq PGresult containing the notice fields.
 */
void polardb_handle_notice(PgSQL_Connection* conn, const PGresult* result) {
	if (!conn || !result) {
		// libpq should never pass NULL, but guard against it.
		return;
	}

	// Detect an LSN wait-timeout WARNING from PolarDB using the structured
	// backend marker. Do not match human-readable message text: user SQL can
	// raise the same text, while the errdetail_internal() marker is emitted only
	// by PolarDB's proxy LSN wait path.
	const char* detail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	const bool is_lsn_timeout = detail &&
		strcmp(detail, POLARDB_LSN_WAIT_TIMEOUT_DETAIL) == 0;

	if (!is_lsn_timeout) {
		return;
	}

	POLARDB_TRACE("PolarDB WAIT: LSN timeout detected in notice: %s\n",
		PQresultErrorMessage(result));

	// Prove that an in-flight consistency wrapper is active before accounting.
	// The best_effort timeout WARNING is emitted while the user's SELECT obtains
	// its snapshot, after the leading SET results may already be consumed. Do not
	// require stmt_pending > 0 here; wait_active plus wrapper_kind is the signal
	// that the notice belongs to the current PolarDB consistency wait.
	PgSQL_Session* sess = (conn->myds ? conn->myds->sess : nullptr);
	if (!sess) {
		return;
	}

	const bool wait_active =
		(sess->polardb_query.wait.wait_stage == PolarDB_WaitStage::WAITING);
	const bool consistency_wait =
		conn->polardb_query_wrap_state.is_consistency_wait();
	POLARDB_TRACE("PolarDB WAIT: notice handler wait_active=%d consistency_wait=%d "
		"pending=%u sess=%p conn=%p\n",
		wait_active ? 1 : 0,
		consistency_wait ? 1 : 0,
		conn->polardb_query_wrap_state.stmt_pending,
		(void*)sess, (void*)conn);
	if (!wait_active || !consistency_wait) {
		// Timeout-looking notice outside the current consistency wait: do NOT
		// count it and do NOT forward it ourselves. notice_handler_cb() already
		// ran its generic store before calling us (or had no result to store into).
		return;
	}

	// Current consistency wait confirmed. A best_effort wait surfaces a timeout
	// as WARNING/NOTICE rather than ERROR. Accounting is centralized so repeated
	// observations of the same backend event cannot double-count counters or
	// latency.
	sess->polardb_account_wait_timeout("notice");

	/*
	 * Notice forwarding ownership:
	 *
	 *  - Generic path owns notices attached to the user query result.
	 *    notice_handler_cb() first stores backend notices in conn->query_result.
	 *    When that result is the user SELECT result, ProxySQL forwards the notice
	 *    normally with the user result.
	 *
	 *  - PolarDB pending path owns only notices attached to hidden wrapper results.
	 *    A wrapped consistency read starts with prepended SET statements. ProxySQL
	 *    consumes those SET results and does not forward them to the client. If the
	 *    LSN timeout WARNING is attached to one of those hidden SET results, the
	 *    generic path would hide the WARNING too. In that case this helper copies
	 *    the notice into session->pending_notices so it is forwarded once with the
	 *    user result.
	 *
	 *  - Do not enqueue when conn->query_result is already the user result.
	 *    Generic forwarding is already correct there; enqueueing again would turn
	 *    one backend WARNING into two client-visible WARNINGs.
	 */
	const bool consuming_wrapper_set =
		conn->polardb_query_wrap_state.consuming_wrapper_set();
	if (conn->query_result && !consuming_wrapper_set) {
		POLARDB_TRACE(
			"PolarDB WAIT: timeout notice accounted; generic user-result path owns forwarding\n");
		return;
	}

	POLARDB_TRACE("PolarDB WAIT: saving timeout notice to session->pending_notices\n");

	const char* severity = PQresultErrorField(result, PG_DIAG_SEVERITY);
	const char* severity_nonlocalized =
		PQresultErrorField(result, PG_DIAG_SEVERITY_NONLOCALIZED);
	const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	const char* primary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);

	if (!sess->polardb_enqueue_notice_packet(
			severity, sqlstate, primary, detail, severity_nonlocalized)) {
		return;
	}

	POLARDB_TRACE("PolarDB WAIT: saved notice packet pending_len=%u\n",
		sess->pending_notices ? sess->pending_notices->len : 0);
}

#endif // POLARDB_PROXY
