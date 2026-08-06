/**
 * @file PgSQL_PolarDB_Split.cpp
 * @brief Execute one in-transaction PolarDB split read on a replica.
 *
 * A client transaction stays open on the primary backend. When primary RFQ
 * metadata reports the transaction as split-readable, the planner may choose a
 * replica for a single read. This file owns the temporary dispatch mechanics:
 * export transaction XIDs to the replica, add the same LSN wait used by normal
 * consistency reads, consume the hidden SET results, and restore the primary
 * backend after the read finishes.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_PolarDB_ReaderPool.h"
#include "proxysql.h"

#include <atomic>
#include <cassert>
#include <string_view>

#if POLARDB_PROXY

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

namespace {

/**
 * @brief Point at the SQL text inside a PostgreSQL simple-query packet.
 *
 * Nothing is copied: *query points just after the PostgreSQL v3 message header
 * and stays valid only while pkt lives and is unmodified. The packet must also
 * contain at least one query byte and its trailing NUL.
 *
 * @param pkt        Packet to inspect.
 * @param query      Receives a non-owning pointer to the SQL text. May be null.
 * @param query_len  Receives the text length, which excludes the trailing NUL of
 *                   the packet. May be null.
 * @return true when pkt is a simple-query packet and the out-parameters were set.
 *         false leaves both out-parameters cleared to nullptr / 0, which they are
 *         set to before any validation runs.
 */
static bool polardb_extract_simple_query_body(const PtrSize_t& pkt,
		const char** query, size_t* query_len) {
	if (query) {
		*query = nullptr;
	}
	if (query_len) {
		*query_len = 0;
	}
	if (!pkt.ptr ||
			pkt.size < PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD + 1 ||
			((const char*)pkt.ptr)[0] != 'Q') {
		return false;
	}
	if (query) {
		*query =
			(const char*)pkt.ptr + PGSQL_V3_MESSAGE_HEADER_SIZE;
	}
	if (query_len) {
		*query_len =
			pkt.size - PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD;
	}
	return true;
}

/**
 * @brief Append a value as the body of a single-quoted SQL string literal.
 *
 * Only the single quote is escaped, by doubling it. The enclosing quotes are not
 * written, so the caller must emit them around the appended text. Backslashes are
 * passed through unchanged, which is correct only while the backend keeps
 * standard_conforming_strings on. Embedded NUL bytes are not rejected.
 *
 * @param value  Text to escape and append.
 * @param out    Buffer to append to, already positioned after the opening quote.
 */
static void polardb_append_sql_literal(std::string_view value, std::string& out) {
	for (char c : value) {
		if (c == '\'') {
			out.append("''");
		} else {
			out.push_back(c);
		}
	}
}

/**
 * @brief Return whether a reader the session already holds may skip the LSN wait.
 *
 * Skipping the wait wrapper removes the server-side guarantee, so a wrong true
 * here returns stale data to the client with no other check behind it. The check
 * covers both the reader and the conditions it was picked under: the
 * connection is idle, reusable and connected; the session's cached
 * rfq_writer_scope still matches the writer scope of this request; the
 * connection carries an LSN payload that has already reached wait_spec.target;
 * the hostgroup still maps to the same writer hostgroup with the same
 * writer_epoch, loaded with acquire ordering so a concurrent failover is not
 * missed; the startup identity mode, startup profile generation and startup
 * config generation are unchanged since the connection was established; and the
 * server is still eligible for this request in the pool.
 *
 * Entry bumps txn_reader_reuse_bypass_checked; a true return also bumps
 * txn_reader_reuse_bypass_allowed.
 *
 * @param sess            Session that holds the reader.
 * @param reader_myds     Stream carrying the retained reader connection.
 * @param reader_hg       Reader hostgroup the request routes to.
 * @param reader_plan     Reader plan for this request, used for eligibility.
 * @param wait_spec       Wait target the reader must already have reached.
 * @param writer_scope    Writer hostgroup and epoch the request was planned under.
 * @param exclude_address Endpoint to treat as ineligible, normally a reader that
 *                        just failed. The default nullptr excludes nothing.
 * @param exclude_port    Port of that endpoint. The default -1 excludes nothing.
 * @return true when the LSN wait may be omitted for this reader, false when the
 *         request must keep the wait wrapper.
 */
static bool polardb_reused_txn_reader_can_bypass_wait(
		PgSQL_Session* sess,
		PgSQL_Data_Stream* reader_myds,
		unsigned int reader_hg,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_WriterScope& writer_scope,
		const char* exclude_address = nullptr,
		int exclude_port = -1) {
	POLARDB_THREAD_COUNT_ONE(
		sess ? sess->thread : nullptr, txn_reader_reuse_bypass_checked);
	PgSQL_Connection* conn = reader_myds ? reader_myds->myconn : nullptr;
	if (!sess || !PgHGM || !conn || !conn->parent || !conn->reusable ||
			!conn->is_connected() ||
			conn->async_state_machine != ASYNC_IDLE ||
			!sess->polardb_txn_reader.rfq_writer_scope.matches(writer_scope)) {
		return false;
	}
	if (wait_spec.target == 0 || !conn->has_polardb_lsn_payload()) {
		return false;
	}
	const uint64_t reader_lsn = conn->get_polardb_lsn();
	if (reader_lsn == 0 || reader_lsn < wait_spec.target) {
		return false;
	}
	const auto current_hg_config =
		PgHGM->get_polardb_hg_config(reader_hg);
	if (!current_hg_config.is_polardb_hostgroup ||
			current_hg_config.writer_hostgroup != writer_scope.hg ||
			current_hg_config.writer_epoch != writer_scope.epoch) {
		return false;
	}
	const int identity_mode = pgsql_thread___polardb_proxy_identity_mode;
	const int protocol = current_hg_config.policy.proxy_protocol >= 0
		? current_hg_config.policy.proxy_protocol
		: pgsql_thread___polardb_proxy_protocol;
	const PolarDB_StartupProfile current_profile =
		PolarDB_StartupProfile::from_protocol(
			polardb_proxy_protocol_from_int(protocol));
	if (conn->polardb_startup_identity_mode != identity_mode ||
			!polardb_startup_profile_matches_request_generation(
				conn->polardb_startup_profile,
				conn->polardb_startup_profile_generation,
				current_profile, identity_mode) ||
			(conn->polardb_startup_settings_set &&
				(!GloPTH ||
				 conn->polardb_startup_config_generation !=
					GloPTH->get_polardb_startup_config_generation()))) {
		return false;
	}
	if (!PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, static_cast<PgSQL_SrvC*>(conn->parent),
			reader_plan, wait_spec, exclude_address, exclude_port)) {
		return false;
	}
	POLARDB_THREAD_COUNT_ONE(
		sess->thread, txn_reader_reuse_bypass_allowed);
	return true;
}

static void polardb_count_split_fallback_status(
		PgSQL_Thread* thread, PolarDB_ReaderStatus status) {
	switch (status) {
	case PolarDB_ReaderStatus::READER_UNAVAILABLE:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_unavailable);
		break;
	case PolarDB_ReaderStatus::READER_BUSY:
	case PolarDB_ReaderStatus::READER_GROUP_BUSY:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_busy);
		break;
	case PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE:
		break;
	case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_rfq_unavailable);
		break;
	case PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_group_lsn_unknown);
		break;
	case PolarDB_ReaderStatus::READER_LSN_UNKNOWN:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_lsn_unknown);
		break;
	case PolarDB_ReaderStatus::READER_LSN_STALE:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_lsn_stale);
		break;
	case PolarDB_ReaderStatus::READER_LAG_EXCEEDED:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_lag_exceeded);
		break;
	case PolarDB_ReaderStatus::ACQUIRED:
		break;
	}
}

static void polardb_count_split_pool_acquire_failure(
		PgSQL_Thread* thread, PolarDB_ReaderStatus status) {
	switch (status) {
	case PolarDB_ReaderStatus::READER_UNAVAILABLE:
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		break;
	case PolarDB_ReaderStatus::READER_BUSY:
	case PolarDB_ReaderStatus::READER_GROUP_BUSY:
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_empty);
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_contention);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		break;
	case PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE:
		break;
	case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
		// The selected server has no connection for this exact RFQ key.
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_empty);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		break;
	default:
		break;
	}
}

/**
 * @brief Try to bring a busy split-reader connection back to a reusable state.
 *
 * Only ASYNC_QUERY_END is recoverable: the exchange is finished, so discarding
 * the staged result, the dispatch state and the wrapper state leaves a clean
 * connection. A timed-out connection is refused even though it looks quiet,
 * because the backend may still send the response later and those frames would
 * surface in whatever query runs next on that socket.
 *
 * @param sess  Session owning the connection, used for counters.
 * @param conn  Connection to normalize. May be null.
 * @return true when the connection reached ASYNC_IDLE and may be reused. false
 *         when the caller must not reuse it - a timeout state, any other
 *         non-terminal busy state, or a null argument. A connection that was
 *         already ASYNC_IDLE also returns false, meaning only that nothing was
 *         done, so the caller must apply its own reuse rule in that case.
 */
static bool polardb_try_normalize_split_cleanup_connection(
		PgSQL_Session* sess, PgSQL_Connection* conn) {
	if (!sess || !conn || conn->async_state_machine == ASYNC_IDLE) {
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(sess->thread, split_conn_cleanup_recovery_attempt);
	const PG_ASYNC_ST state = conn->async_state_machine;
	if (state == ASYNC_QUERY_TIMEOUT) {
		// A timeout state can still have a backend response arriving later. Reusing
		// that socket would mix stale frames into a future query, so destroy it.
		POLARDB_THREAD_COUNT_ONE(sess->thread,
			split_conn_cleanup_recovery_timeout_state);
		return false;
	}
	if (state != ASYNC_QUERY_END) {
		POLARDB_THREAD_COUNT_ONE(sess->thread,
			split_conn_cleanup_recovery_busy_state);
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(sess->thread,
		split_conn_cleanup_recovery_terminal);
	conn->async_free_result();
	conn->polardb_clear_request_state_for_release();
	POLARDB_THREAD_COUNT_ONE(sess->thread, split_conn_cleanup_normalized);
	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: normalized terminal split connection during cleanup "
		"conn=%p state=%d active_txn=%d\n",
		(void*)conn, (int)state, conn->IsActiveTransaction() ? 1 : 0);
	return true;
}

} // namespace

/**
 * @brief Queue one background split-reader warmup request for this session.
 *
 * The HGM layer deduplicates queued and in-flight keys and owns the actual
 * connection creation; this call only supplies the session auth profile and
 * startup identity needed for safe pool reuse. It returns without queueing
 * anything when the session already holds a connected split reader for
 * reader_hg, and when the client connection has no userinfo to warm up with.
 *
 * @param reader_hg      Reader hostgroup to warm up. A negative value is ignored.
 * @param reason         Trace text describing the trigger. May be null. Borrowed
 *                       for the duration of the call.
 * @param target_server  Server to pin the warmup to, or nullptr to let the HGM
 *                       pick any eligible server. Borrowed for the duration of
 *                       the call, like the userinfo strings.
 */
void PgSQL_Session::polardb_request_txn_split_warmup(
		int reader_hg, const char* reason,
		const PgSQL_SrvC* target_server) {
	if (reader_hg < 0) {
		return;
	}
	if (polardb_txn_reader.backend &&
			polardb_txn_reader.backend->hostgroup_id == reader_hg &&
			polardb_txn_reader.backend->server_myds &&
			polardb_txn_reader.backend->server_myds->myconn &&
			polardb_txn_reader.backend->server_myds->myconn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB WARMUP: skip split request reason=%s reader_hg=%d "
			"session already holds a split reader\n",
			reason ? reason : "unknown", reader_hg);
		return;
	}
	if (!client_myds || !client_myds->myconn || !client_myds->myconn->userinfo) {
		POLARDB_TRACE(
			"PolarDB WARMUP: skip split request reason=%s reader_hg=%d "
			"missing client userinfo\n",
			reason ? reason : "unknown", reader_hg);
		return;
	}

	PolarDB_StartupClientContext startup_client;
	const bool has_startup_client =
		polardb_startup_client_from_session(this, &startup_client);
	PgHGM->request_split_warmup(
		(unsigned int)reader_hg,
		client_myds->myconn->userinfo->username,
		client_myds->myconn->userinfo->password,
		client_myds->myconn->userinfo->dbname,
		has_startup_client ? startup_client : PolarDB_StartupClientContext{},
		client_myds->myconn, target_server);
	POLARDB_TRACE(
		"PolarDB WARMUP: requested split pool reason=%s reader_hg=%d "
		"target=%s:%u mode=%s\n",
		reason ? reason : "unknown",
		reader_hg,
		target_server && target_server->address
			? target_server->address : "",
		target_server ? target_server->port : 0,
		polardb_txn_split_warmup_mode_name(
			polardb_effective_txn_split_warmup_mode()));
}

void PgSQL_Session::polardb_begin_txn_reader_read(
		PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds,
		const PolarDB_WriterScope& writer_scope) {
	assert(reader_backend != nullptr);
	assert(reader_myds != nullptr);
	assert(reader_myds->myconn != nullptr);
	reader_myds->DSS = STATE_READY;
	reader_myds->fd = reader_myds->myconn->fd;
	polardb_txn_reader.backend = reader_backend;
	polardb_txn_reader.primary_backend = mybe;
	polardb_txn_reader.writer_scope = writer_scope;
	mybe = reader_backend;
	polardb_txn_reader.read_start_us = monotonic_time();
	polardb_txn_reader.wait_timeout_error = false;
}

/**
 * @brief Install the packet for one pre-write transaction wait read.
 *
 * The reader becomes the session's active backend and mybe is switched to it,
 * with the primary backend saved for restore.
 *
 * @param reader_backend  Replica backend to read from. Must have a connected
 *                        stream.
 * @param reader_myds     Stream of that backend.
 * @param pkt             Client simple-query packet. Ownership of the buffer
 *                        moves to reader_myds->pgsql_real_query, which frees it
 *                        in end(). The caller's PtrSize_t is cleared immediately
 *                        so ownership is explicit at this boundary.
 * @param writer_scope    Writer hostgroup and epoch this read was planned under.
 */
void PgSQL_Session::polardb_begin_txn_wait_read(
		PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds,
		PtrSize_t& pkt,
		const PolarDB_WriterScope& writer_scope) {
	polardb_begin_txn_reader_read(reader_backend, reader_myds, writer_scope);
	polardb_txn_reader.wait_read_active = true;
	reader_myds->free_pgsql_real_query();
	reader_myds->pgsql_real_query.take_packet(pkt);
}

/**
 * @brief Install the wrapper for one transaction split read on a replica.
 *
 * The reader becomes the session's active backend and mybe is switched to it,
 * with the primary backend saved for restore. The stream does not get a packet
 * of its own: QueryPtr and QuerySize are pointed straight at
 * polardb_txn_reader.wrapped_query with no copy, so that string must stay alive
 * and unmodified until the read ends, and the stream must be cleared with
 * pgsql_real_query.reset() rather than freed.
 *
 * @param reader_backend  Replica backend to read from.
 * @param reader_myds     Stream of that backend.
 * @param pkt             Original client packet. Ownership moves into
 *                        polardb_txn_reader.original_pkt and the caller's
 *                        complete PtrSize_t descriptor is cleared.
 * @param wrapped_query   Full wrapper text, moved into
 *                        polardb_txn_reader.wrapped_query. Must not be empty.
 * @param wait_spec       LSN wait the wrapper enforces.
 * @param reader_plan     Reader plan this read was routed with.
 * @param writer_scope    Writer hostgroup and epoch this read was planned under.
 * @param wrapper_stmts   Number of prepended wrapper statements. Stored as
 *                        polardb_query.dispatch_wrapper_stmts, the count of
 *                        hidden results the dispatcher must swallow before the
 *                        client's own result.
 * @param wait_bypassed   true when the wrapper carries no LSN wait, which sets
 *                        wait_start_us to 0 and so suppresses all split wait
 *                        accounting for this read.
 */
void PgSQL_Session::polardb_begin_txn_split_read(
		PgSQL_Backend* reader_backend,
		PgSQL_Data_Stream* reader_myds,
		PtrSize_t& pkt,
		std::string&& wrapped_query,
		const PolarDB_WaitSpec& wait_spec,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WriterScope& writer_scope,
		uint32_t wrapper_stmts, bool wait_bypassed) {
	polardb_begin_txn_reader_read(reader_backend, reader_myds, writer_scope);
	polardb_txn_reader.wrapped_query = std::move(wrapped_query);
	polardb_txn_reader.wait_spec = wait_spec;
	polardb_query.reader_plan = reader_plan;
	reader_myds->free_pgsql_real_query();
	assert(reader_myds->pgsql_real_query.pkt.ptr == nullptr);
	assert(reader_myds->pgsql_real_query.pkt.size == 0);
	assert(!polardb_txn_reader.wrapped_query.empty());
	reader_myds->pgsql_real_query.QueryPtr =
		const_cast<char*>(polardb_txn_reader.wrapped_query.c_str());
	reader_myds->pgsql_real_query.QuerySize =
		(unsigned int)polardb_txn_reader.wrapped_query.size();
	polardb_txn_reader.take_original_packet(pkt);
	polardb_txn_reader.split_active = true;
	polardb_txn_reader.wait_start_us =
		wait_bypassed ? 0 : polardb_txn_reader.read_start_us;
	polardb_transaction_split.begin_split_read();
	polardb_query.dispatch_wrapper_stmts = wrapper_stmts;
	polardb_query.dispatch_wrapper_kind =
		PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT;
	if (reader_myds->myconn) {
		reader_myds->myconn->polardb_txn_split_xids_dirty = true;
		reader_myds->myconn->polardb_txn_split_xids_reset_consumed = false;
	}
}

/**
 * @brief Prepare one pre-write transaction consistency read on a replica backend.
 *
 * Only connections that are already in the pool are taken
 * (PGSQL_POLARDB_TXN_READER_ONLY_POOLED); a pool miss declines the read and asks
 * for a demand warmup instead of blocking the request on a new connection. A
 * reader retained from an earlier read of a different hostgroup is released
 * first, and a pooled connection found disconnected is destroyed. When the
 * selected reader has provably reached the wait target the wrapper is skipped and
 * only wait_bypass_target is recorded.
 *
 * @param plan       Route plan; plan.target_hg selects the reader hostgroup.
 * @param route_ctx  Route context. Must report an open transaction.
 * @param pkt        Client simple-query packet.
 * @return true when the reader stream takes pkt and becomes active; false when
 *         pkt remains with the caller and the request stays on the writer.
 */
bool PgSQL_Session::polardb_prepare_txn_wait_read(
		const PolarDB_Query_RoutePlan& plan,
		const PolarDB_Query_RouteCtx& route_ctx,
		PtrSize_t& pkt) {
	const int reader_hg = plan.target_hg;
	auto request_warmup_after_miss = [&]() {
		const int warmup_mode = polardb_effective_txn_split_warmup_mode();
		if (warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND) ||
				warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH)) {
			polardb_request_txn_split_warmup(reader_hg, "txn_wait_demand");
		} else {
			POLARDB_TRACE(
				"PolarDB WARMUP: transaction wait demand request suppressed "
				"mode=%s reader_hg=%d\n",
				polardb_txn_split_warmup_mode_name(warmup_mode), reader_hg);
		}
	};
	if (reader_hg < 0 || !route_ctx.in_transaction) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined reader_hg=%d in_txn=%d\n",
			reader_hg, route_ctx.in_transaction ? 1 : 0);
		return false;
	}

	const bool needs_wait = plan.wait_spec.has_wait();
	const char* orig_query = nullptr;
	size_t orig_len = 0;
	if (needs_wait &&
			!polardb_extract_simple_query_body(pkt, &orig_query, &orig_len)) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined invalid simple-query packet "
			"size=%u\n",
			pkt.size);
		return false;
	}

	if (polardb_txn_reader.backend &&
			polardb_txn_reader.backend->hostgroup_id != reader_hg) {
		polardb_release_txn_reader_backend(/*want_reuse=*/true);
	}
	if (!polardb_txn_reader.backend) {
		polardb_txn_reader.backend = find_or_create_backend(reader_hg);
	}
	if (!polardb_txn_reader.backend || !polardb_txn_reader.backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined no backend reader_hg=%d\n",
			reader_hg);
		request_warmup_after_miss();
		return false;
	}

	PgSQL_Data_Stream* reader_myds = polardb_txn_reader.backend->server_myds;
	bool bypass_wait = false;
	if (!reader_myds->myconn) {
		PolarDB_ReaderResult reader_result =
			PgHGM->polardb_acquire_reader_connection(
				(unsigned int)reader_hg, this, plan.reader, plan.wait_spec,
				PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
		if (!reader_result.acquired()) {
			POLARDB_TRACE(
				"PolarDB TXN_WAIT: prepare declined reader acquisition status=%s "
				"reader_hg=%d target_lsn=%lu\n",
				polardb_reader_status_name(reader_result.status),
				reader_hg,
				(unsigned long)plan.wait_spec.target);
			request_warmup_after_miss();
			return false;
		}
		polardb_txn_reader.rfq_writer_scope.reset();
		reader_myds->attach_connection(reader_result.conn);
		if (needs_wait && reader_result.wait_bypass_allowed) {
			bypass_wait = true;
		}
	} else if (needs_wait) {
		bypass_wait = polardb_reused_txn_reader_can_bypass_wait(
			this, reader_myds, static_cast<unsigned int>(reader_hg),
			plan.reader, plan.wait_spec, route_ctx.writer_scope);
	}

	if (!reader_myds->myconn || !reader_myds->myconn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined disconnected reader "
			"reader_hg=%d\n",
			reader_hg);
		polardb_return_or_destroy_backend_stream(reader_myds, false);
		polardb_txn_reader.rfq_writer_scope.reset();
		request_warmup_after_miss();
		return false;
	}
#if POLARDB_PROFILE
	if (needs_wait) {
		polardb_profile_note_reader_connection(
			plan.wait_spec, plan.reader, reader_myds->myconn);
	}
#endif // POLARDB_PROFILE

	if (needs_wait && polardb_query.wait.wait_stage != PolarDB_WaitStage::IDLE) {
		// Defensive: execute resets the wait state before calling us. Keeping this
		// explicit prevents a stale wait from being combined with the new query.
		polardb_query.reset_wait();
	}
	if (needs_wait && bypass_wait) {
		POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_bypassed);
		polardb_query.mark_wait_satisfied(plan.wait_spec.target);
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: selected reader already reached "
			"target_lsn=%lu; wait wrapper bypassed\n",
			(unsigned long)plan.wait_spec.target);
	} else if (needs_wait) {
		if (plan.reader.consistency_mode == PolarDB_ConsistencyMode::GLOBAL_LSN) {
			POLARDB_THREAD_COUNT_ONE(thread, global_lsn_routing);
		} else {
			POLARDB_THREAD_COUNT_ONE(thread, session_lsn_routing);
		}
		POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_prepared);
		polardb_query.reader_plan = plan.reader;
		polardb_query.begin_wait(
			plan.wait_spec, monotonic_time(),
			plan.reader.fallback_writer_hg);
		polardb_query.original_query.assign(orig_query, orig_len);
	}

	polardb_begin_txn_wait_read(polardb_txn_reader.backend, reader_myds, pkt,
		route_ctx.writer_scope);

	POLARDB_TRACE(
		"PolarDB TXN_WAIT: prepared reader_hg=%d primary_hg=%d "
		"target_lsn=%lu wait=%d bypass_wait=%d\n",
		reader_hg, route_ctx.writer_scope.hg,
		(unsigned long)plan.wait_spec.target,
		needs_wait ? 1 : 0,
		bypass_wait ? 1 : 0);
	return true;
}

/**
 * @brief Prepare one transaction split read on a replica backend.
 *
 * Requires plan.target_hg >= 0, a non-empty plan.txn_xids and a real wait target;
 * without all three the open transaction cannot be made visible on the replica,
 * so the read is declined immediately. Only already-pooled connections are used
 * (PGSQL_POLARDB_TXN_READER_ONLY_POOLED) and a demand warmup is requested on a
 * pool miss. An endpoint recorded in polardb_txn_reader_failure is excluded from
 * selection and from the eligibility re-check of a retained reader.
 *
 * @param plan          Route plan carrying the reader hostgroup, the transaction
 *                      XIDs and the wait target.
 * @param writer_scope  Writer hostgroup and epoch this read is planned under.
 * @param pkt           Client simple-query packet.
 * @return true when the read was installed: pkt has been consumed by
 *          polardb_begin_txn_split_read() and the caller's PtrSize_t is cleared,
 *          and mybe points at the replica.
 *          false when the read was declined; pkt is untouched and the caller
 *          proceeds on the primary. A false return is not side-effect free: a
 *          split reader retained from an earlier read may already have been
 *          released, and a pooled connection found disconnected or no longer
 *          eligible may already have been destroyed.
 */
bool PgSQL_Session::polardb_prepare_txn_split_read(
		const PolarDB_Query_RoutePlan& plan,
		const PolarDB_WriterScope& writer_scope,
		PtrSize_t& pkt) {
#if POLARDB_PROFILE
	const unsigned long long prepare_start_us = monotonic_time();
	auto finish_prepare = [&](bool ok) {
		const unsigned long long now_us = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(thread, split_prepare_sum_us,
			now_us >= prepare_start_us ? now_us - prepare_start_us : 0);
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, split_prepare_count);
		return ok;
	};
#else
	auto finish_prepare = [](bool ok) { return ok; };
#endif // POLARDB_PROFILE

	const int reader_hg = plan.target_hg;
	if (reader_hg < 0 || plan.txn_xids.empty() || !plan.wait_spec.has_wait()) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: prepare declined reader_hg=%d xids_len=%zu wait=%d\n",
			reader_hg, plan.txn_xids.size(), plan.wait_spec.has_wait() ? 1 : 0);
		return finish_prepare(false);
	}

	if (polardb_txn_reader.backend &&
			polardb_txn_reader.backend->hostgroup_id != reader_hg) {
		polardb_release_txn_reader_backend(/*want_reuse=*/true);
	}
	if (!polardb_txn_reader.backend) {
		polardb_txn_reader.backend = find_or_create_backend(reader_hg);
	}
	if (!polardb_txn_reader.backend || !polardb_txn_reader.backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: prepare declined no backend reader_hg=%d\n",
			reader_hg);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		const int warmup_mode = polardb_effective_txn_split_warmup_mode();
		if (warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND) ||
				warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH)) {
			polardb_request_txn_split_warmup(reader_hg, "split_no_backend");
		}
		return finish_prepare(false);
	}

	PgSQL_Data_Stream* split_myds = polardb_txn_reader.backend->server_myds;
	bool bypass_wait = false;
	const char* skipped_reader_address = nullptr;
	int skipped_reader_port = -1;
	const bool exclude_reader =
		polardb_txn_reader_failure.matches_skipped_reader(
			reader_hg, &skipped_reader_address, &skipped_reader_port);
	if (!split_myds->myconn) {
		if (exclude_reader) {
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: excluding failed reader %s:%d "
				"for reader_hg=%d\n",
				skipped_reader_address, skipped_reader_port, reader_hg);
		}
#if POLARDB_PROFILE
		const unsigned long long reader_acquire_start_us = monotonic_time();
#endif // POLARDB_PROFILE
		PolarDB_ReaderResult reader_result =
			PgHGM->polardb_acquire_reader_connection(
				(unsigned int)reader_hg, this, plan.reader, plan.wait_spec,
				PGSQL_POLARDB_TXN_READER_ONLY_POOLED,
				exclude_reader ? skipped_reader_address : nullptr,
				exclude_reader ? skipped_reader_port : -1);
#if POLARDB_PROFILE
		const unsigned long long reader_acquire_end_us = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(thread, split_reader_acquire_sum_us,
			reader_acquire_end_us >= reader_acquire_start_us
				? reader_acquire_end_us - reader_acquire_start_us : 0);
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, split_reader_acquire_count);
#endif // POLARDB_PROFILE
		if (!reader_result.acquired()) {
#if POLARDB_PROFILE
			if (reader_result.exact_match_reserved) {
				POLARDB_PROFILE_THREAD_COUNT_ONE(
					thread, split_pool_miss_reserved_exact);
			}
#endif // POLARDB_PROFILE
			polardb_count_split_fallback_status(thread, reader_result.status);
			polardb_count_split_pool_acquire_failure(thread, reader_result.status);
			const bool warmup_can_help =
				polardb_reader_status_split_warmup_can_help(reader_result.status);
			bool warmup_requested = false;
			const int warmup_mode = polardb_effective_txn_split_warmup_mode();
			if (warmup_can_help &&
					(warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND) ||
					 warmup_mode == static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH))) {
				polardb_request_txn_split_warmup(reader_hg, "demand");
				warmup_requested = true;
			} else {
				POLARDB_TRACE(
					"PolarDB WARMUP: demand request suppressed mode=%s "
					"reader_hg=%d status=%s can_help=%d\n",
					polardb_txn_split_warmup_mode_name(warmup_mode), reader_hg,
					polardb_reader_status_name(reader_result.status),
					warmup_can_help ? 1 : 0);
			}
			(void)warmup_requested; // used by POLARDB_TRACE when tracing is enabled
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: prepare declined reader acquisition status=%s "
				"reader_hg=%d target_lsn=%lu warmup_requested=%d\n",
				polardb_reader_status_name(reader_result.status),
				reader_hg, (unsigned long)plan.wait_spec.target,
				warmup_requested ? 1 : 0);
			return finish_prepare(false);
		}
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_hit);
		bypass_wait = reader_result.wait_bypass_allowed;
		polardb_txn_reader.rfq_writer_scope.reset();
		split_myds->attach_connection(reader_result.conn);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, split_conn_reused);
		bypass_wait = polardb_reused_txn_reader_can_bypass_wait(
			this, split_myds, static_cast<unsigned int>(reader_hg),
			plan.reader, plan.wait_spec, writer_scope,
			exclude_reader ? skipped_reader_address : nullptr,
			exclude_reader ? skipped_reader_port : -1);
	}

	if (split_myds->myconn && plan.reader.require_replica &&
			!PgHGM->polardb_reader_server_can_serve_request(
				static_cast<unsigned int>(reader_hg),
				static_cast<PgSQL_SrvC*>(split_myds->myconn->parent),
				plan.reader, plan.wait_spec,
				exclude_reader ? skipped_reader_address : nullptr,
				exclude_reader ? skipped_reader_port : -1)) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: retained backend is no longer an eligible "
			"replica reader_hg=%d\n",
			reader_hg);
		polardb_release_txn_reader_backend(/*want_reuse=*/true);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		return finish_prepare(false);
	}

	if (!split_myds->myconn) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: prepare declined missing reader connection reader_hg=%d\n",
			reader_hg);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		return finish_prepare(false);
	}

	if (!split_myds->myconn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: prepare declined disconnected pooled reader "
			"reader_hg=%d\n",
			reader_hg);
		polardb_return_or_destroy_backend_stream(split_myds, false);
		polardb_txn_reader.rfq_writer_scope.reset();
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		return finish_prepare(false);
	}
#if POLARDB_PROFILE
	polardb_profile_note_reader_connection(
		plan.wait_spec, plan.reader, split_myds->myconn);
#endif // POLARDB_PROFILE

	// Build the wrapper only after a viable reader exists. This avoids doing
	// string work on the hot fallback path when no pooled reader can be used.
	std::string wrapped_query;
#if POLARDB_PROFILE
	const unsigned long long wrapper_build_start_us = monotonic_time();
#endif // POLARDB_PROFILE
	const uint32_t wrapper_stmts = polardb_build_txn_split_wrapped_query(
		pkt, plan.wait_spec, plan.txn_xids, bypass_wait, wrapped_query);
#if POLARDB_PROFILE
	const unsigned long long wrapper_build_end_us = monotonic_time();
	POLARDB_PROFILE_THREAD_COUNT(thread, split_wrapper_build_sum_us,
		wrapper_build_end_us >= wrapper_build_start_us
			? wrapper_build_end_us - wrapper_build_start_us : 0);
	POLARDB_PROFILE_THREAD_COUNT_ONE(thread, split_wrapper_build_count);
#endif // POLARDB_PROFILE
	if (wrapper_stmts == 0) {
		POLARDB_THREAD_COUNT_ONE(thread, split_send_failed);
		polardb_release_txn_reader_backend(/*want_reuse=*/true);
		return finish_prepare(false);
	}

	polardb_begin_txn_split_read(polardb_txn_reader.backend, split_myds, pkt,
		std::move(wrapped_query), plan.wait_spec, plan.reader,
		writer_scope, wrapper_stmts, bypass_wait);
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_total);
	if (bypass_wait) {
		POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_bypassed);
		polardb_query.mark_wait_satisfied(plan.wait_spec.target);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, split_lsn_wait_count);
	}

	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: prepared reader_hg=%d target_lsn=%lu xids_len=%zu "
		"wrapper_stmts=%u bypass_wait=%d\n",
		reader_hg, (unsigned long)plan.wait_spec.target, plan.txn_xids.size(),
		wrapper_stmts, bypass_wait ? 1 : 0);
	return finish_prepare(true);
}

/**
 * @brief Build the XID export plus LSN wait wrapper for a transaction split read.
 *
 * The wrapper always starts with the SET that exports the open transaction's XIDs
 * to the replica. The wait that follows is forced to STRICT mode regardless of
 * what wait_spec asks for: a split read that continues past a wait timeout would
 * run without the target LSN, where those XIDs may not yet be visible on the
 * replica, so the timeout has to stop the batch before the user query is
 * dispatched.
 *
 * @param pkt            Original client simple-query packet, read only.
 * @param wait_spec      Requested wait. Its mode is overridden to STRICT.
 * @param txn_xids       Transaction XID list to export. Must not be empty.
 * @param bypass_wait    true emits only the XID SET followed by the original
 *                       query, with no wait statements at all.
 * @param wrapped_query  Receives the wrapper text. Cleared on failure.
 * @return The number of prepended wrapper statements whose results the dispatcher
 *         must swallow before the client's own result: 1 when bypass_wait is set,
 *         more when the wait statements are included. 0 means the build failed
 *         (no XIDs, no wait target, or not a simple-query packet) and
 *         wrapped_query has been cleared.
 */
uint32_t PgSQL_Session::polardb_build_txn_split_wrapped_query(
		const PtrSize_t& pkt, const PolarDB_WaitSpec& wait_spec,
		std::string_view txn_xids, bool bypass_wait,
		std::string& wrapped_query) {
	wrapped_query.clear();
	if (txn_xids.empty() || !wait_spec.has_wait()) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: wrapper build declined xids_len=%zu wait=%d\n",
			txn_xids.size(), wait_spec.has_wait() ? 1 : 0);
		return 0;
	}

	const char* orig_query = nullptr;
	size_t orig_len = 0;
	if (!polardb_extract_simple_query_body(pkt, &orig_query, &orig_len)) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: wrapper build declined invalid simple-query "
			"packet size=%u\n",
			pkt.size);
		return 0;
	}

	PolarDB_Query_WaitState split_wait;
	split_wait.prepare_from_spec(wait_spec);
	// A transaction-split read must not continue after a wait timeout: without
	// the target LSN, the transaction's XIDs may not yet be visible on the
	// replica. Use strict mode internally so timeout stops before the user SELECT
	// is dispatched.
	split_wait.spec.mode = PolarDB_WaitMode::STRICT;

	wrapped_query.reserve(txn_xids.size() + orig_len + 192);
	wrapped_query.append("SET polar_xact_split_xids = '");
	polardb_append_sql_literal(txn_xids, wrapped_query);
	wrapped_query.append("'; ");
	uint32_t wrapper_stmts = 1;
	if (bypass_wait) {
		wrapped_query.append(orig_query, orig_len);
		return wrapper_stmts;
	}
	const uint32_t wait_wrapper_stmts = append_wrapped_wait_query(
			orig_query, orig_len, split_wait,
			polardb_wait_mode_set_statement(split_wait.spec.mode),
			wrapped_query);
	if (wait_wrapper_stmts == 0) {
		POLARDB_TRACE("PolarDB TXN_SPLIT: wrapper build declined empty wait query\n");
		wrapped_query.clear();
		return 0;
	}
	wrapper_stmts += wait_wrapper_stmts;
	return wrapper_stmts;
}

void PgSQL_Session::polardb_reset_txn_reader_request() {
	if (polardb_txn_reader.backend && polardb_txn_reader.backend->server_myds) {
		if (polardb_txn_reader.wait_read_active) {
			polardb_txn_reader.backend->server_myds->free_pgsql_real_query();
		} else {
			// For split reads, pgsql_real_query points to
			// polardb_txn_reader.wrapped_query. Clear the pointer before clearing
			// the string below.
			polardb_txn_reader.backend->server_myds->pgsql_real_query.reset();
		}
	}
	if (polardb_txn_reader.original_pkt.ptr) {
		l_free(polardb_txn_reader.original_pkt.size,
			polardb_txn_reader.original_pkt.ptr);
		polardb_txn_reader.original_pkt = {};
	}
	if (polardb_txn_reader.primary_backend) {
		mybe = polardb_txn_reader.primary_backend;
	}
	polardb_txn_reader.clear_request_state();
	polardb_query.reset_reader_plan();
	polardb_query.reset_dispatch_wrapper();
	discard_pending_notices();
}

/**
 * @brief Finish a transaction reader request and restore normal session state.
 *
 * Does nothing when neither a split read nor a wait read is active, so it is safe
 * to call on any terminal path. Latency is recorded, the split state is torn down
 * through polardb_reset_txn_reader_request() and the primary backend is restored.
 *
 * A clean finish caches polardb_txn_reader.rfq_writer_scope from the writer scope
 * the read ran under, which is what later authorizes skipping the LSN wait on the
 * reused reader; every other outcome resets it, so a doubtful read cannot grant
 * that permission.
 *
 * @param split_success             true for a split read that completed; moves the
 *                                  transaction stage back to TXN_SPLITTABLE.
 * @param split_error               true for a failed read; sets the stage to
 *                                  TXN_ON_PRIMARY and marks
 *                                  polardb_transaction_split.blocked, so no
 *                                  further split read happens in this transaction.
 * @param reason                    Trace text for the abort. May be null.
 * @param record_split_error_counter true to also count split_reads_error. Only
 *                                  meaningful together with split_error.
 * @param mark_reader_not_reusable  true to mark the reader connection not
 *                                  reusable. Only meaningful together with
 *                                  split_error.
 */
void PgSQL_Session::polardb_finish_txn_reader_read(
		bool split_success, bool split_error, const char* reason,
		bool record_split_error_counter, bool mark_reader_not_reusable) {
	const bool split_active = polardb_txn_reader.split_active;
	const bool wait_read_active = polardb_txn_reader.wait_read_active;
	if (!split_active && !wait_read_active) {
		return;
	}
	if (split_active || split_error) {
		polardb_record_txn_split_wait_latency();
		polardb_record_txn_split_latency();
	}
	if (split_success) {
		POLARDB_THREAD_COUNT_ONE(thread, split_reads_success);
	}
	if (split_error) {
		if (mark_reader_not_reusable &&
				polardb_txn_reader.backend &&
				polardb_txn_reader.backend->server_myds &&
				polardb_txn_reader.backend->server_myds->myconn) {
			polardb_txn_reader.backend->server_myds->myconn->reusable = false;
		}
		if (record_split_error_counter) {
			POLARDB_THREAD_COUNT_ONE(thread, split_reads_error);
		}
	}
	PgSQL_Connection* completed_reader =
		polardb_txn_reader.backend &&
			polardb_txn_reader.backend->server_myds
		? polardb_txn_reader.backend->server_myds->myconn : nullptr;
	if (!split_error &&
			polardb_txn_reader.writer_scope.valid() &&
			completed_reader && completed_reader->is_connected() &&
			completed_reader->has_polardb_lsn_payload() &&
			completed_reader->get_polardb_lsn() > 0) {
		polardb_txn_reader.rfq_writer_scope =
			polardb_txn_reader.writer_scope;
	} else {
		polardb_txn_reader.rfq_writer_scope.reset();
	}
	polardb_reset_txn_reader_request();
	if (split_success) {
		polardb_transaction_split.complete_split_read();
		POLARDB_TRACE("PolarDB TXN_SPLIT: completed split read; primary backend restored\n");
		return;
	}
	if (split_error) {
		polardb_transaction_split.fail_split_read();
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: aborted split read (%s); later reads use primary\n",
			reason ? reason : "unknown");
		return;
	}
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: completed pre-write transaction read; "
		"primary backend restored\n");
}

void PgSQL_Session::polardb_complete_txn_split_read() {
	if (!polardb_txn_reader.split_active) {
		return;
	}
	polardb_finish_txn_reader_read(
		/*split_success=*/true, /*split_error=*/false, nullptr,
		/*record_split_error_counter=*/false,
		/*mark_reader_not_reusable=*/false);
}

void PgSQL_Session::polardb_complete_txn_wait_read() {
	if (!polardb_txn_reader.wait_read_active) {
		return;
	}
	polardb_finish_txn_reader_read(
		/*split_success=*/false, /*split_error=*/false, nullptr,
		/*record_split_error_counter=*/false,
		/*mark_reader_not_reusable=*/false);
}

void PgSQL_Session::polardb_abort_txn_split_read(const char* reason) {
	polardb_finish_txn_reader_read(
		/*split_success=*/false, /*split_error=*/true, reason,
		/*record_split_error_counter=*/true,
		/*mark_reader_not_reusable=*/true);
}

void PgSQL_Session::polardb_release_txn_wait_read(bool want_reuse) {
	if (!polardb_txn_reader.wait_read_active &&
			!polardb_txn_reader.has_primary_backend()) {
		return;
	}
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: releasing reader after failure want_reuse=%d\n",
		want_reuse ? 1 : 0);
	polardb_release_txn_reader_backend(want_reuse);
}

void PgSQL_Session::polardb_reconcile_txn_wait_read_end(
		const char* reason, bool want_reuse) {
	if (!polardb_txn_reader.wait_read_active) {
		return;
	}
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: reconcile terminal path reason=%s want_reuse=%d\n",
		reason ? reason : "unknown", want_reuse ? 1 : 0);
	polardb_release_txn_wait_read(want_reuse);
}

void PgSQL_Session::polardb_reconcile_txn_wait_read_request_entry(
		const char* reason) {
	if (!polardb_txn_reader.wait_read_active) {
		return;
	}
	POLARDB_THREAD_COUNT_ONE(thread, txn_wait_reader_reconciled);
	proxy_warning(
		"PolarDB TXN_WAIT: stale transaction wait reader at request entry; "
		"restoring primary backend and dropping reader (sess=%p)\n",
		this);
	polardb_reconcile_txn_wait_read_end(reason, false);
}

/**
 * @brief Give up the temporary split replica connection and undo the split state.
 *
 * This reaches past the connection itself. While a reader request is active or a
 * primary backend was saved, the split wait latency is recorded and
 * polardb_reset_txn_reader_request() runs, which frees
 * polardb_txn_reader.original_pkt, clears the wrapper state and restores mybe to
 * the primary backend. The stream is always detached (fd 0) and
 * polardb_txn_reader.backend is cleared, or rfq_writer_scope is reset if the
 * tracked backend had already changed.
 *
 * @param want_reuse  Advisory. The connection goes back to the pool only when
 *                    this is true and the connection is also reusable, ASYNC_IDLE
 *                    and outside any transaction; a non-idle connection is first
 *                    given one chance through
 *                    polardb_try_normalize_split_cleanup_connection(). Anything
 *                    else is destroyed. The caller must not use the connection
 *                    after this returns.
 */
void PgSQL_Session::polardb_release_txn_reader_backend(bool want_reuse) {
	PgSQL_Backend* split_be = polardb_txn_reader.backend;
	if (!split_be) {
		return;
	}
	if (polardb_txn_reader.active() ||
			polardb_txn_reader.has_primary_backend()) {
		polardb_record_txn_split_wait_latency();
		polardb_reset_txn_reader_request();
	}
	if (split_be->server_myds) {
		PgSQL_Data_Stream* split_myds = split_be->server_myds;
		split_myds->max_connect_time = 0;
		split_myds->pgsql_real_query.reset();
		if (split_myds->myconn) {
			PgSQL_Connection* split_conn = split_myds->myconn;
			const bool was_non_idle =
				split_conn->async_state_machine != ASYNC_IDLE;
			if (want_reuse && was_non_idle) {
				polardb_try_normalize_split_cleanup_connection(this, split_conn);
			}
			const bool conn_reusable = split_conn->reusable;
			const bool conn_idle =
				split_conn->async_state_machine == ASYNC_IDLE;
			const bool conn_active_txn =
				split_conn->IsActiveTransaction();
			const bool reusable =
				want_reuse &&
				conn_reusable &&
				conn_idle &&
				!conn_active_txn;
			if (reusable) {
				polardb_return_or_destroy_backend_stream(split_myds, true);
				POLARDB_THREAD_COUNT_ONE(thread, split_conn_cleanup_success);
				if (was_non_idle) {
					POLARDB_THREAD_COUNT_ONE(thread,
						split_conn_cleanup_recovered);
				}
			} else {
				// Reason counters are flags, not a partition: one destroyed
				// backend can be non-reusable and non-idle at the same time.
				if (!want_reuse) {
					POLARDB_THREAD_COUNT_ONE(thread,
						split_conn_cleanup_no_reuse_requested);
				}
				if (!conn_reusable) {
					POLARDB_THREAD_COUNT_ONE(thread,
						split_conn_cleanup_not_reusable);
				}
				if (!conn_idle) {
					POLARDB_THREAD_COUNT_ONE(thread,
						split_conn_cleanup_not_idle);
				}
				if (conn_active_txn) {
					POLARDB_THREAD_COUNT_ONE(thread,
						split_conn_cleanup_active_txn);
				}
				polardb_return_or_destroy_backend_stream(split_myds, false);
				POLARDB_THREAD_COUNT_ONE(thread, split_conn_cleanup_failed);
			}
		}
	}
	if (polardb_txn_reader.backend == split_be) {
		polardb_txn_reader.clear_backend();
	} else {
		polardb_txn_reader.rfq_writer_scope.reset();
	}
}

void PgSQL_Session::polardb_record_txn_split_latency() {
	if (polardb_txn_reader.read_start_us == 0) {
		return;
	}
	const unsigned long long now_us = monotonic_time();
	if (now_us >= polardb_txn_reader.read_start_us) {
		POLARDB_THREAD_COUNT(thread, split_latency_sum_us,
			now_us - polardb_txn_reader.read_start_us);
		POLARDB_THREAD_COUNT_ONE(thread, split_latency_count);
	}
	polardb_txn_reader.read_start_us = 0;
}

void PgSQL_Session::polardb_record_txn_split_wait_latency() {
	if (polardb_txn_reader.wait_start_us == 0) {
		return;
	}
	const unsigned long long now_us = monotonic_time();
	if (now_us >= polardb_txn_reader.wait_start_us) {
		const unsigned long long elapsed_us =
			now_us - polardb_txn_reader.wait_start_us;
		POLARDB_THREAD_COUNT(thread, split_lsn_wait_sum_us, elapsed_us);
		polardb_count_lsn_wait_elapsed_bucket(
			thread, elapsed_us, /*transaction_split=*/true);
#if POLARDB_PROFILE
		polardb_profile_record_wait_completion(elapsed_us);
#endif // POLARDB_PROFILE
	}
	polardb_txn_reader.wait_start_us = 0;
}

/**
 * @brief Account an LSN wait timeout against the split read that is in flight.
 *
 * @param source  Trace text naming the detection site. May be null.
 * @return true when the timeout was charged to this split read. The wait latency
 *         is recorded at the same time, which zeroes wait_start_us, so the call
 *         is one-shot per split read and split_error_timeout /
 *         split_error_lsn_wait_timeout cannot be counted twice.
 *         false when no split read is active or its wait was already accounted;
 *         the caller must then use the non-split autocommit/session timeout
 *         accounting path instead.
 */
bool PgSQL_Session::polardb_account_txn_split_wait_timeout(const char* source) {
	if (!polardb_txn_reader.split_active || polardb_txn_reader.wait_start_us == 0) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: skip timeout accounting source=%s "
			"active=%d read_active=%d wait_start_us=%llu\n",
			source ? source : "",
			polardb_txn_reader.split_active ? 1 : 0,
			polardb_txn_split_read_active() ? 1 : 0,
			polardb_txn_reader.wait_start_us);
		return false;
	}
	const unsigned long long wait_start_us = polardb_txn_reader.wait_start_us;
	const unsigned long long now_us = monotonic_time();
	const unsigned long long elapsed_us =
		now_us >= wait_start_us ? now_us - wait_start_us : 0;
	(void)elapsed_us; // used only by POLARDB_TRACE in POLARDB_DEBUG builds
	// Record the timeout before latency accounting clears wait_start_us.
	polardb_txn_reader.wait_timeout_error = true;
	polardb_record_txn_split_wait_latency();
	POLARDB_THREAD_COUNT_ONE(thread, split_error_timeout);
	POLARDB_THREAD_COUNT_ONE(thread, split_error_lsn_wait_timeout);
	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: timeout accounted source=%s "
		"elapsed_us=%llu active=%d read_active=%d\n",
		source ? source : "",
		elapsed_us,
		polardb_txn_reader.split_active ? 1 : 0,
		polardb_txn_split_read_active() ? 1 : 0);
	return true;
}

#endif // POLARDB_PROXY
