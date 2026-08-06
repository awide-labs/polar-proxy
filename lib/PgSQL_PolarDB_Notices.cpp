/**
 * @file PgSQL_PolarDB_Notices.cpp
 * @brief PolarDB NOTICE/WARNING capture and forwarding.
 *
 * A best_effort LSN wait surfaces a timeout as a backend WARNING/NOTICE while
 * the wrapped user query obtains its snapshot. The prepended SET result sets are
 * consumed and dropped while the wrapped query runs (PgSQL_Connection::handler()),
 * so they are never forwarded to the client, and that consumption can rotate
 * query_result. These helpers capture the timeout notice and store it on the
 * session; the captured notice is later prepended to the client output ahead of
 * the user result (PgSQL_Session::PgSQL_Result_to_PgSQL_wire()).
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

bool polardb_is_lsn_wait_timeout_result(const pg_result* result) {
	if (!result) {
		return false;
	}
	const char* detail =
		PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	const char* source_function =
		PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION);
	return detail && source_function &&
		strcmp(detail, POLARDB_LSN_WAIT_TIMEOUT_DETAIL) == 0 &&
		strcmp(source_function,
			POLARDB_LSN_WAIT_TIMEOUT_SOURCE_FUNCTION) == 0;
}

/**
 * @brief Empty and destroy the pending-notice queue.
 *
 * The queue object is always deleted and the pointer nulled regardless of
 * @p free_buffers; that flag only controls who frees the queued packet bytes.
 *
 * @param free_buffers  True to free every queued packet buffer here. False is
 *                      legal only when ownership of all of them has already been
 *                      transferred to another owner — the client PSarrayOUT after
 *                      a flush, for instance. Passing false without that transfer
 *                      leaks every queued packet.
 */
void PgSQL_Session::PolarDB_NoticeQueueState::clear(bool free_buffers) {
	if (!pending) {
		return;
	}
	while (pending->len > 0) {
		if (free_buffers) {
			PtrSize_t ps;
			pending->remove_index(pending->len - 1, &ps);
			if (ps.ptr) {
				l_free(ps.size, ps.ptr);
			}
		} else {
			pending->remove_index(pending->len - 1, nullptr);
		}
	}
	delete pending;
	pending = nullptr;
}

/**
 * @brief Append a NoticeResponse packet to the pending queue.
 *
 * The backing PtrSizeArray is allocated on first use.
 *
 * Ownership transfer is conditional: the queue takes @p pkt only when it is
 * non-null and @p size is non-zero. A null or zero-sized packet is neither stored
 * nor freed, so it remains the caller's to release.
 *
 * @param pkt   Packet buffer from the local allocator. Ownership moves to the
 *              queue when it is accepted.
 * @param size  Packet size in bytes.
 */
void PgSQL_Session::PolarDB_NoticeQueueState::add(
		unsigned char* pkt, unsigned int size) {
	if (!pkt || size == 0) {
		return;
	}
	if (!pending) {
		pending = new PtrSizeArray();
	}
	pending->add(pkt, size);
}

void PgSQL_Session::discard_pending_notices() {
	polardb_notices.clear(true);
}

void PgSQL_Session::forget_transferred_notices() {
	polardb_notices.clear(false);
}

/**
 * @brief Forward queued NoticeResponse packets to the client output array.
 *
 * Ownership of each packet buffer moves to PSarrayOUT. The queue object is then
 * cleared without freeing the packet bytes, matching the normal result-forward
 * ownership rule.
 */
void PgSQL_Session::polardb_flush_pending_notices_to_client() {
	if (!polardb_notices.pending || polardb_notices.pending->len == 0 ||
			!client_myds || !client_myds->PSarrayOUT) {
		return;
	}

	for (unsigned int i = 0; i < polardb_notices.pending->len; i++) {
		PtrSize_t* ps = polardb_notices.pending->index(i);
		if (ps && ps->ptr && ps->size > 0) {
			client_myds->PSarrayOUT->add(ps->ptr, ps->size);
		}
	}
	forget_transferred_notices();
}

/**
 * @brief Enqueue a NoticeResponse packet to be forwarded with the next result.
 *
 * A best_effort LSN timeout can emit WARNING/NOTICE while the wrapped SELECT is
 * running. Capture the NoticeResponse packet here and it is forwarded before the
 * SELECT result.
 *
 * @param pkt  Packet buffer. The session takes ownership only when @p pkt is
 *             non-null and @p size is non-zero; a null or empty packet is
 *             ignored and stays the caller's to free.
 * @param size Packet size in bytes.
 */
void PgSQL_Session::enqueue_pending_notice(unsigned char* pkt, unsigned int size) {
	polardb_notices.add(pkt, size);
}

/**
 * @brief Build a PostgreSQL NoticeResponse packet and queue it for the client.
 *
 * Allocates one packet from the local allocator, serializes the given fields
 * into wire form, and hands it to enqueue_pending_notice(), which takes
 * ownership. On a serialization failure the packet is freed here and nothing is
 * queued.
 *
 * @return Serialized wire size when queued; zero on failure.
 */
unsigned int PgSQL_Session::polardb_enqueue_notice_packet(
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
		return 0;
	}
	// On success enqueue_pending_notice() owns pkt; the session frees it later.
	enqueue_pending_notice(pkt, written);
	return written;
}

unsigned int PgSQL_Session::polardb_enqueue_notice_packet(
		const PGresult* result) {
	if (!result) {
		return 0;
	}

	const PolarDB_NoticeField fields[] = {
		{'S', PQresultErrorField(result, PG_DIAG_SEVERITY)},
		{'V', PQresultErrorField(result, PG_DIAG_SEVERITY_NONLOCALIZED)},
		{'C', PQresultErrorField(result, PG_DIAG_SQLSTATE)},
		{'M', PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY)},
		{'D', PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL)},
		{'H', PQresultErrorField(result, PG_DIAG_MESSAGE_HINT)},
		{'P', PQresultErrorField(result, PG_DIAG_STATEMENT_POSITION)},
		{'p', PQresultErrorField(result, PG_DIAG_INTERNAL_POSITION)},
		{'q', PQresultErrorField(result, PG_DIAG_INTERNAL_QUERY)},
		{'W', PQresultErrorField(result, PG_DIAG_CONTEXT)},
		{'s', PQresultErrorField(result, PG_DIAG_SCHEMA_NAME)},
		{'t', PQresultErrorField(result, PG_DIAG_TABLE_NAME)},
		{'c', PQresultErrorField(result, PG_DIAG_COLUMN_NAME)},
		{'d', PQresultErrorField(result, PG_DIAG_DATATYPE_NAME)},
		{'n', PQresultErrorField(result, PG_DIAG_CONSTRAINT_NAME)},
		{'F', PQresultErrorField(result, PG_DIAG_SOURCE_FILE)},
		{'L', PQresultErrorField(result, PG_DIAG_SOURCE_LINE)},
		{'R', PQresultErrorField(result, PG_DIAG_SOURCE_FUNCTION)}
	};
	const size_t field_count = sizeof(fields) / sizeof(fields[0]);
	const unsigned int size =
		polardb_notice_response_packet_size(fields, field_count);
	unsigned char* pkt = (unsigned char*)l_alloc(size);
	const unsigned int written = polardb_write_notice_response_packet(
		pkt, size, fields, field_count);
	if (written == 0) {
		l_free(size, pkt);
		return 0;
	}
	enqueue_pending_notice(pkt, written);
	return written;
}

/**
 * @brief Queue a WARNING that ProxySQL generates itself when a read is routed to
 *        a reader without a usable LSN wait target.
 *
 * The ReadyForQuery (RFQ) message normally carries the writer's LSN, which a
 * replica read waits for to guarantee read-your-writes. When the admin variable
 * pgsql-polardb_action_missing_lsn is warning, ProxySQL may still send the
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
		"PolarDB reader route has no enforceable LSN wait target; read may be stale";
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
		polardb_notices.len());
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
 * @brief Account for a PolarDB LSN wait-timeout notice emitted while a wrapped
 *        query runs, and queue it for the client when the generic path would
 *        drop it.
 *
 * Called from notice_handler_cb() for every backend notice. This helper first
 * confirms that the notice belongs to the current active LSN wait using PolarDB's
 * stable proxy wait-timeout detail marker plus wrapper state. It then performs
 * timeout accounting and applies the forwarding ownership rule below: generic
 * forwarding owns user-result notices; the PolarDB pending path only rescues
 * notices attached to discarded wrapper SET results. Other notices, including
 * user-generated text that looks like an LSN timeout, are ignored here; the
 * generic store in notice_handler_cb() handles them after this returns false.
 *
 * @note Runs in the libpq NoticeReceiver callback on the backend thread; keep it
 *       cheap and non-blocking. conn->myds and the session reached through it may
 *       be null during connection teardown.
 *
 * @param conn   Active backend connection supplied by the libpq notice receiver.
 * @param result libpq PGresult containing the notice fields.
 * @return true when the session queue owns forwarding and the generic callback
 *         must not store a duplicate in the active result.
 */
bool polardb_handle_lsn_wait_timeout_notice(PgSQL_Connection* conn, const PGresult* result) {
	if (!conn || !result) {
		// libpq should never pass NULL, but check against it.
		return false;
	}

	// Require both the private DETAIL token and the backend source function.
	// Client-provided text or DETAIL alone does not identify this timeout.
	if (!polardb_is_lsn_wait_timeout_result(result)) {
		return false;
	}

	POLARDB_TRACE("PolarDB WAIT: LSN timeout detected in notice: %s\n",
		PQresultErrorMessage(result));

	// Confirm that an in-flight consistency wrapper is active before accounting.
	// The best_effort timeout WARNING is emitted while the user's SELECT obtains
	// its snapshot, after the leading SET results may already be consumed. Do not
	// require stmt_pending > 0 here; an active PolarDB wait plus wrapper_kind is
	// the signal that the notice belongs to the current wait operation. Transaction
	// split has its own wrapper kind but uses the same wait-timeout backend marker.
	PgSQL_Session* sess = (conn->myds ? conn->myds->sess : nullptr);
	if (!sess) {
		return false;
	}

	const bool wait_active =
		(sess->polardb_query.wait.wait_stage == PolarDB_WaitStage::WAITING) ||
		sess->polardb_txn_split_read_active();
	const bool polar_wait_wrapper =
		conn->polardb_query_wrap_state.is_polar_wait_wrapper();
	POLARDB_TRACE("PolarDB WAIT: notice handler wait_active=%d polar_wait_wrapper=%d "
		"pending=%u sess=%p conn=%p\n",
		wait_active ? 1 : 0,
		polar_wait_wrapper ? 1 : 0,
		conn->polardb_query_wrap_state.stmt_pending,
		(void*)sess, (void*)conn);
	if (!wait_active || !polar_wait_wrapper) {
		// Timeout-looking notice outside the current PolarDB wait wrapper: do NOT
		// count it and do NOT forward it ourselves. notice_handler_cb() will
		// continue through the generic result path after this returns false.
		return false;
	}

	// Current PolarDB wait confirmed. A best_effort wait surfaces a timeout as
	// WARNING/NOTICE rather than ERROR. Split reads use the same backend marker
	// but have their own counters, so account against the active wrapper family.
	if (sess->polardb_txn_split_read_active()) {
		sess->polardb_account_txn_split_wait_timeout("notice");
	} else {
		sess->polardb_account_wait_timeout("notice");
	}

	/*
	 * Notice forwarding ownership:
	 *
	 *  - Generic path owns notices attached to the user query result. Returning
	 *    false lets notice_handler_cb() store the notice there normally.
	 *
	 *  - PolarDB pending path owns notices attached to discarded wrapper results.
	 *    A wrapped consistency read starts with prepended SET statements. ProxySQL
	 *    consumes those SET results and does not forward them to the client. If the
	 *    LSN timeout WARNING is attached to one of those discarded SET results, the
	 *    generic path would hide the WARNING too. In that case this helper copies
	 *    the notice into the session notice queue so it is forwarded once with the
	 *    user result.
	 *
	 *  - An extended W before implicit backend Parse also uses the session queue:
	 *    ProxySQL discards ParseComplete before executing the client statement.
	 *    W before Bind/Execute uses the normal client-visible query result.
	 */
	const bool session_queue_owns =
		conn->polardb_query_wrap_state.wait_notice_uses_session_queue() ||
		!conn->query_result;
	if (!session_queue_owns) {
		POLARDB_TRACE(
			"PolarDB WAIT: timeout notice accounted; generic user-result path owns forwarding\n");
		return false;
	}

	POLARDB_TRACE("PolarDB WAIT: saving timeout notice to session notice queue\n");

	const unsigned int bytes_recv =
		sess->polardb_enqueue_notice_packet(result);
	if (bytes_recv == 0) {
		// Queue serialization failed, so leave the notice with the generic
		// query-result path instead of silently claiming and dropping it.
		return false;
	}
	conn->update_bytes_recv(bytes_recv);

	POLARDB_TRACE("PolarDB WAIT: saved notice packet pending_len=%u\n",
		sess->polardb_notices.len());
	return true;
}

#endif // POLARDB_PROXY
