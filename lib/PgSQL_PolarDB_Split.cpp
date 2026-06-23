/**
 * @file PgSQL_PolarDB_Split.cpp
 * @brief Execute one in-transaction PolarDB split read on a replica.
 *
 * A client transaction stays open on the primary backend. When primary RFQ
 * metadata says the transaction is split-readable, the planner may choose a
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
#include "proxysql.h"

#include <atomic>
#include <cassert>
#include <string_view>

#if POLARDB_PROXY

extern PgSQL_HostGroups_Manager* PgHGM;

namespace {

static bool polardb_extract_simple_query_body(const PtrSize_t& pkt,
		const char** query, size_t* query_len) {
	if (query) {
		*query = nullptr;
	}
	if (query_len) {
		*query_len = 0;
	}
	if (!pkt.ptr || pkt.size < 7 || ((const char*)pkt.ptr)[0] != 'Q') {
		return false;
	}
	if (query) {
		*query = (const char*)pkt.ptr + 5;
	}
	if (query_len) {
		*query_len = pkt.size - 6; // skip 'Q' + len and trailing NUL
	}
	return true;
}

static void polardb_append_sql_literal(std::string_view value, std::string& out) {
	for (char c : value) {
		if (c == '\'') {
			out.append("''");
		} else {
			out.push_back(c);
		}
	}
}

static bool polardb_startup_client_from_session(
			PgSQL_Session* sess, PolarDB_StartupClientContext* startup_client) {
	if (startup_client) {
		*startup_client = PolarDB_StartupClientContext{};
	}
	if (!sess || !sess->client_myds || !startup_client) {
		return false;
	}

	PgSQL_Data_Stream* client_myds = sess->client_myds;
	PolarDB_StartupIdentity identity{
		client_myds->addr.addr,
		client_myds->addr.port,
		PolarDB_StartupIdentitySource::CLIENT};
	if (identity.valid(false)) {
		startup_client->identity = std::move(identity);
		return true;
	}

	if (polardb_startup_identity_from_sockaddr(
			client_myds->client_addr,
			&identity,
			PolarDB_StartupIdentitySource::CLIENT)) {
		startup_client->identity = std::move(identity);
		return true;
	}

	return false;
}

static void polardb_count_split_fallback_status(
		PgSQL_Thread* thread, PolarDB_ReaderStatus status) {
	switch (status) {
	case PolarDB_ReaderStatus::READER_UNAVAILABLE:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_unavailable);
		break;
	case PolarDB_ReaderStatus::READER_BUSY:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_reader_busy);
		break;
	case PolarDB_ReaderStatus::RFQ_UNAVAILABLE:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_rfq_unavailable);
		break;
	case PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN:
		POLARDB_THREAD_COUNT_ONE(thread, split_fallback_primary_lsn_unknown);
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
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_empty);
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_contention);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		break;
	default:
		break;
	}
}

static bool polardb_split_cleanup_terminal_state(PG_ASYNC_ST state) {
	return state == ASYNC_QUERY_END;
}

static bool polardb_split_cleanup_timeout_state(PG_ASYNC_ST state) {
	return state == ASYNC_QUERY_TIMEOUT;
}

static bool polardb_try_normalize_split_cleanup_connection(
		PgSQL_Session* sess, PgSQL_Connection* conn) {
	if (!sess || !conn || conn->async_state_machine == ASYNC_IDLE) {
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(sess->thread, split_conn_cleanup_recovery_attempt);
	const PG_ASYNC_ST state = conn->async_state_machine;
	if (polardb_split_cleanup_timeout_state(state)) {
		// A timeout state can still have a backend response arriving later. Reusing
		// that socket would mix stale frames into a future query, so destroy it.
		POLARDB_THREAD_COUNT_ONE(sess->thread,
			split_conn_cleanup_recovery_timeout_state);
		return false;
	}
	if (!polardb_split_cleanup_terminal_state(state)) {
		POLARDB_THREAD_COUNT_ONE(sess->thread,
			split_conn_cleanup_recovery_busy_state);
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(sess->thread,
		split_conn_cleanup_recovery_terminal);
	conn->async_free_result();
	conn->dispatch_state.reset();
	conn->polardb_query_wrap_state.clear();
	POLARDB_THREAD_COUNT_ONE(sess->thread, split_conn_cleanup_normalized);
	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: normalized terminal split connection during cleanup "
		"conn=%p state=%d active_txn=%d\n",
		(void*)conn, (int)state, conn->IsActiveTransaction() ? 1 : 0);
	return true;
}

} // namespace

// Queues one background split-reader warmup request for this session. The HGM
// layer deduplicates queued/in-flight keys and owns the actual connection
// creation; this helper only supplies the session auth profile and startup
// identity needed for safe pool reuse.
void PgSQL_Session::polardb_request_txn_split_warmup(
		int reader_hg, const char* reason) {
	if (reader_hg < 0) {
		return;
	}
	if (polardb_txn_split_backend &&
			polardb_txn_split_backend->hostgroup_id == reader_hg &&
			polardb_txn_split_backend->server_myds &&
			polardb_txn_split_backend->server_myds->myconn &&
			polardb_txn_split_backend->server_myds->myconn->is_connected()) {
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
		has_startup_client ? startup_client : PolarDB_StartupClientContext{});
	POLARDB_TRACE(
		"PolarDB WARMUP: requested split pool reason=%s reader_hg=%d mode=%s\n",
		reason ? reason : "unknown",
		reader_hg,
		polardb_txn_split_warmup_mode_name(
			polardb_effective_txn_split_warmup_mode()));
}

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

	if (polardb_txn_split_backend &&
			polardb_txn_split_backend->hostgroup_id != reader_hg) {
		polardb_release_txn_split_backend(/*want_reuse=*/true);
	}
	if (!polardb_txn_split_backend) {
		polardb_txn_split_backend = find_or_create_backend(reader_hg);
	}
	if (!polardb_txn_split_backend || !polardb_txn_split_backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined no backend reader_hg=%d\n",
			reader_hg);
		request_warmup_after_miss();
		return false;
	}

	PgSQL_Data_Stream* reader_myds = polardb_txn_split_backend->server_myds;
	bool bypass_wait = false;
	if (!reader_myds->myconn) {
		PolarDB_ReaderResult reader_result =
			PgHGM->get_MyConn_polardb_reader(
				(unsigned int)reader_hg, this, plan.reader, false);
		if (!reader_result.acquired()) {
			POLARDB_TRACE(
				"PolarDB TXN_WAIT: prepare declined reader acquisition status=%s "
				"reader_hg=%d target_lsn=%lu\n",
				polardb_reader_status_name(reader_result.status),
				reader_hg,
				(unsigned long)plan.reader.consistency_target_lsn);
			request_warmup_after_miss();
			return false;
		}
		reader_myds->attach_connection(reader_result.conn);
		if (needs_wait && reader_result.wait_bypass_allowed) {
			bypass_wait = true;
			POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_bypassed);
			POLARDB_TRACE(
				"PolarDB TXN_WAIT: selected reader already reached "
				"target_lsn=%lu; wait wrapper bypassed\n",
				(unsigned long)plan.reader.consistency_target_lsn);
		}
	}

	if (!reader_myds->myconn || !reader_myds->myconn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB TXN_WAIT: prepare declined disconnected reader "
			"reader_hg=%d\n",
			reader_hg);
		if (reader_myds->myconn) {
			reader_myds->destroy_MySQL_Connection_From_Pool(false);
		}
		request_warmup_after_miss();
		return false;
	}

	if (needs_wait && polardb_query.wait.wait_stage != PolarDB_WaitStage::IDLE) {
		// Defensive: execute resets the wait state before calling us. Keeping this
		// explicit prevents a stale wait from being combined with the new query.
		polardb_query.reset_wait();
	}
	if (needs_wait) {
		if (plan.reader.consistency_mode == PolarDB_ConsistencyMode::GLOBAL_LSN) {
			POLARDB_THREAD_COUNT_ONE(thread, global_lsn_routing);
		} else {
			POLARDB_THREAD_COUNT_ONE(thread, session_lsn_routing);
		}
		POLARDB_THREAD_COUNT_ONE(thread, wait_wrap_prepared);
		if (!bypass_wait) {
			polardb_query.reader_plan = plan.reader;
			polardb_query.wait.prepare_from_spec(plan.wait_spec);
			polardb_query.wait.wait_stage = PolarDB_WaitStage::WAITING;
			polardb_query.wait.wait_started_at_us = monotonic_time();
			polardb_query.wait.fallback_writer_hg = plan.reader.fallback_writer_hg;
			polardb_query.wait.original_query.assign(orig_query, orig_len);
		}
	}

	reader_myds->DSS = STATE_READY;
	reader_myds->fd = reader_myds->myconn->fd;
	polardb_txn_split_saved_mybe = mybe;
	mybe = polardb_txn_split_backend;
	polardb_txn_wait_read_active = true;
	polardb_txn_split_read_start_us = monotonic_time();
	reader_myds->free_pgsql_real_query();
	reader_myds->pgsql_real_query.init(&pkt);

	POLARDB_TRACE(
		"PolarDB TXN_WAIT: prepared reader_hg=%d primary_hg=%d "
		"target_lsn=%lu wait=%d\n",
		reader_hg, route_ctx.writer_scope.hg,
		(unsigned long)plan.wait_spec.target,
		needs_wait ? 1 : 0);
	return true;
}

bool PgSQL_Session::polardb_prepare_txn_split_read(
		const PolarDB_Query_RoutePlan& plan, PtrSize_t& pkt) {
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

	if (polardb_txn_split_backend &&
			polardb_txn_split_backend->hostgroup_id != reader_hg) {
		polardb_release_txn_split_backend(/*want_reuse=*/true);
	}
	if (!polardb_txn_split_backend) {
		polardb_txn_split_backend = find_or_create_backend(reader_hg);
	}
	if (!polardb_txn_split_backend || !polardb_txn_split_backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: prepare declined no backend reader_hg=%d\n",
			reader_hg);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		return finish_prepare(false);
	}

	PgSQL_Data_Stream* split_myds = polardb_txn_split_backend->server_myds;
	if (!split_myds->myconn) {
		const bool exclude_shunned_reader =
			polardb_txn_reader_failure_pin == PolarDB_RoutePin::SHUN_READER &&
			polardb_txn_shunned_reader_hg == reader_hg &&
			!polardb_txn_shunned_reader_address.empty() &&
			polardb_txn_shunned_reader_port >= 0;
		if (exclude_shunned_reader) {
			POLARDB_TRACE(
				"PolarDB TXN_SPLIT: excluding shunned reader %s:%d "
				"for reader_hg=%d\n",
				polardb_txn_shunned_reader_address.c_str(),
				polardb_txn_shunned_reader_port, reader_hg);
		}
#if POLARDB_PROFILE
		const unsigned long long reader_acquire_start_us = monotonic_time();
#endif // POLARDB_PROFILE
		PolarDB_ReaderResult reader_result =
			PgHGM->get_MyConn_polardb_reader(
				(unsigned int)reader_hg, this, plan.reader, true,
				exclude_shunned_reader ?
					polardb_txn_shunned_reader_address.c_str() : nullptr,
				exclude_shunned_reader ?
					polardb_txn_shunned_reader_port : -1);
#if POLARDB_PROFILE
		const unsigned long long reader_acquire_end_us = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(thread, split_reader_acquire_sum_us,
			reader_acquire_end_us >= reader_acquire_start_us
				? reader_acquire_end_us - reader_acquire_start_us : 0);
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, split_reader_acquire_count);
#endif // POLARDB_PROFILE
		if (!reader_result.acquired()) {
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
				reader_hg, (unsigned long)plan.reader.consistency_target_lsn,
				warmup_requested ? 1 : 0);
			return finish_prepare(false);
		}
		POLARDB_THREAD_COUNT_ONE(thread, split_pool_hit);
		split_myds->attach_connection(reader_result.conn);
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, split_conn_reused);
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
		split_myds->destroy_MySQL_Connection_From_Pool(false);
		POLARDB_THREAD_COUNT_ONE(thread, split_no_backend);
		return finish_prepare(false);
	}

	// Build the wrapper only after a viable reader exists. This avoids doing
	// string work on the hot fallback path when no pooled reader can be used.
	polardb_txn_split_wrapped_query.clear();
#if POLARDB_PROFILE
	const unsigned long long wrapper_build_start_us = monotonic_time();
#endif // POLARDB_PROFILE
	const bool wrapper_built = polardb_build_txn_split_wrapped_query(
		pkt, plan.wait_spec, plan.txn_xids, polardb_txn_split_wrapped_query);
#if POLARDB_PROFILE
	const unsigned long long wrapper_build_end_us = monotonic_time();
	POLARDB_PROFILE_THREAD_COUNT(thread, split_wrapper_build_sum_us,
		wrapper_build_end_us >= wrapper_build_start_us
			? wrapper_build_end_us - wrapper_build_start_us : 0);
	POLARDB_PROFILE_THREAD_COUNT_ONE(thread, split_wrapper_build_count);
#endif // POLARDB_PROFILE
	if (!wrapper_built) {
		POLARDB_THREAD_COUNT_ONE(thread, split_send_failed);
		polardb_release_txn_split_backend(/*want_reuse=*/true);
		return finish_prepare(false);
	}

	split_myds->DSS = STATE_READY;
	split_myds->fd = split_myds->myconn->fd;
	polardb_txn_split_saved_mybe = mybe;
	mybe = polardb_txn_split_backend;
	polardb_txn_split_wait_spec = plan.wait_spec;
	polardb_query.reader_plan = plan.reader;
	split_myds->free_pgsql_real_query();
	assert(split_myds->pgsql_real_query.pkt.ptr == nullptr);
	assert(split_myds->pgsql_real_query.pkt.size == 0);
	assert(!polardb_txn_split_wrapped_query.empty());
	// QueryPtr borrows the session-owned wrapper string. Do not free it
	// through PgSQL_Data_Stream; reset clears this pointer before the string.
	split_myds->pgsql_real_query.QueryPtr =
		const_cast<char*>(polardb_txn_split_wrapped_query.c_str());
	split_myds->pgsql_real_query.QuerySize =
		(unsigned int)polardb_txn_split_wrapped_query.size();
	polardb_txn_split_original_pkt = pkt;
	pkt.ptr = nullptr;
	pkt.size = 0;
	polardb_txn_split_active = true;
	polardb_txn_split_read_start_us = monotonic_time();
	polardb_txn_split_wait_start_us = polardb_txn_split_read_start_us;
	polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLIT_READ_ACTIVE;
	polardb_transaction_split.was_splittable = true;
	polardb_query.dispatch_wrapper_stmts = POLARDB_TXN_SPLIT_WRAPPER_SET_COUNT;
	polardb_query.dispatch_wrapper_kind =
		PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT;
	if (split_myds->myconn) {
		// The backend has been or will be sent polar_xact_split_xids for this
		// split read. Mark the physical connection dirty so its next non-split
		// reuse first prepends SET polar_xact_split_xids = ''.
		split_myds->myconn->polardb_txn_split_xids_dirty = true;
		split_myds->myconn->polardb_txn_split_xids_reset_consumed = false;
	}
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_total);
	POLARDB_THREAD_COUNT_ONE(thread, split_lsn_wait_count);

	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: prepared reader_hg=%d target_lsn=%lu xids_len=%zu "
		"wrapper_stmts=%u\n",
		reader_hg, (unsigned long)plan.wait_spec.target, plan.txn_xids.size(),
		POLARDB_TXN_SPLIT_WRAPPER_SET_COUNT);
	return finish_prepare(true);
}

bool PgSQL_Session::polardb_build_txn_split_wrapped_query(
		const PtrSize_t& pkt, const PolarDB_WaitSpec& wait_spec,
		std::string_view txn_xids, std::string& wrapped_query) {
	wrapped_query.clear();
	if (txn_xids.empty() || !wait_spec.has_wait()) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: wrapper build declined xids_len=%zu wait=%d\n",
			txn_xids.size(), wait_spec.has_wait() ? 1 : 0);
		return false;
	}

	const char* orig_query = nullptr;
	size_t orig_len = 0;
	if (!polardb_extract_simple_query_body(pkt, &orig_query, &orig_len)) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: wrapper build declined invalid simple-query "
			"packet size=%u\n",
			pkt.size);
		return false;
	}

	PolarDB_Query_WaitState split_wait;
	split_wait.prepare_from_spec(wait_spec);
	// A transaction-split read must not continue after a wait timeout: without
	// the target LSN, the replica may not see the transaction's XIDs. Use strict
	// mode internally so timeout stops before the user SELECT is dispatched.
	split_wait.spec.mode = PolarDB_WaitMode::STRICT;

	wrapped_query.reserve(txn_xids.size() + orig_len + 192);
	wrapped_query.append("SET polar_xact_split_xids = '");
	polardb_append_sql_literal(txn_xids, wrapped_query);
	wrapped_query.append("'; ");
	if (!append_wrapped_wait_query(
			orig_query, orig_len, split_wait,
			build_polar_consistency_mode_set(split_wait.spec.mode),
			wrapped_query)) {
		POLARDB_TRACE("PolarDB TXN_SPLIT: wrapper build declined empty wait query\n");
		wrapped_query.clear();
		return false;
	}
	return true;
}

void PgSQL_Session::polardb_reset_txn_split_read() {
	if (polardb_txn_split_backend && polardb_txn_split_backend->server_myds) {
		if (polardb_txn_wait_read_active) {
			polardb_txn_split_backend->server_myds->free_pgsql_real_query();
		} else {
			// Split reads borrow QueryPtr from polardb_txn_split_wrapped_query.
			// Clear the pointer before the string is cleared below.
			polardb_txn_split_backend->server_myds->pgsql_real_query.reset();
		}
	}
	if (polardb_txn_split_original_pkt.ptr) {
		l_free(polardb_txn_split_original_pkt.size,
			polardb_txn_split_original_pkt.ptr);
		polardb_txn_split_original_pkt.ptr = nullptr;
		polardb_txn_split_original_pkt.size = 0;
	}
	polardb_txn_split_wrapped_query.clear();
	polardb_txn_split_wait_spec.reset();
	polardb_txn_split_read_start_us = 0;
	polardb_txn_split_wait_start_us = 0;
	if (polardb_txn_split_saved_mybe) {
		mybe = polardb_txn_split_saved_mybe;
		polardb_txn_split_saved_mybe = nullptr;
	}
	polardb_txn_split_active = false;
	polardb_txn_wait_read_active = false;
	polardb_query.reset_reader_target();
	polardb_query.reset_dispatch_wrapper();
	clear_pending_notices(/*free_buffers=*/true);
}

void PgSQL_Session::polardb_complete_txn_split_read() {
	if (!polardb_txn_split_active) {
		return;
	}
	polardb_record_txn_split_wait_latency();
	polardb_record_txn_split_latency();
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_success);
	if (polardb_txn_split_backend && polardb_txn_split_backend->server_myds &&
			polardb_txn_split_backend->server_myds->myconn &&
			polardb_txn_split_wait_spec.has_wait()) {
		polardb_note_reader_affinity(
			polardb_txn_split_backend->server_myds->myconn->parent,
			polardb_txn_split_wait_spec.target,
			polardb_query.request_writer_scope);
	}
	polardb_transaction_split.did_split = true;
	polardb_transaction_split.was_splittable = true;
	polardb_reset_txn_split_read();
	polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	POLARDB_TRACE("PolarDB TXN_SPLIT: completed split read; primary backend restored\n");
}

void PgSQL_Session::polardb_complete_txn_wait_read() {
	if (!polardb_txn_wait_read_active) {
		return;
	}
	polardb_reset_txn_split_read();
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: completed pre-write transaction read; "
		"primary backend restored\n");
}

void PgSQL_Session::polardb_abort_txn_split_read(const char* reason) {
	if (polardb_txn_split_backend && polardb_txn_split_backend->server_myds &&
			polardb_txn_split_backend->server_myds->myconn) {
		polardb_txn_split_backend->server_myds->myconn->reusable = false;
	}
	polardb_record_txn_split_wait_latency();
	polardb_record_txn_split_latency();
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_error);
	polardb_reset_txn_split_read();
	polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	polardb_transaction_split.blocked = true;
	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: aborted split read (%s); later reads use primary\n",
		reason ? reason : "unknown");
}

void PgSQL_Session::polardb_release_txn_wait_read(bool want_reuse) {
	if (!polardb_txn_wait_read_active && !polardb_txn_split_saved_mybe) {
		return;
	}
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: releasing reader after failure want_reuse=%d\n",
		want_reuse ? 1 : 0);
	polardb_release_txn_split_backend(want_reuse);
}

void PgSQL_Session::polardb_reconcile_txn_wait_read_end(
		const char* reason, bool want_reuse) {
	if (!polardb_txn_wait_read_active) {
		return;
	}
	POLARDB_TRACE(
		"PolarDB TXN_WAIT: reconcile terminal path reason=%s want_reuse=%d\n",
		reason ? reason : "unknown", want_reuse ? 1 : 0);
	polardb_release_txn_wait_read(want_reuse);
}

void PgSQL_Session::polardb_release_txn_split_backend(bool want_reuse) {
	PgSQL_Backend* split_be = polardb_txn_split_backend;
	if (!split_be) {
		return;
	}
	if (polardb_txn_split_active || polardb_txn_wait_read_active ||
			polardb_txn_split_saved_mybe) {
		polardb_record_txn_split_wait_latency();
		polardb_reset_txn_split_read();
	}
	if (split_be->server_myds) {
		PgSQL_Data_Stream* split_myds = split_be->server_myds;
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
			split_myds->fd = 0;
		}
	}
	if (polardb_txn_split_backend == split_be) {
		polardb_txn_split_backend = nullptr;
	}
}

void PgSQL_Session::polardb_record_txn_split_latency() {
	if (polardb_txn_split_read_start_us == 0) {
		return;
	}
	const unsigned long long now_us = monotonic_time();
	if (now_us >= polardb_txn_split_read_start_us) {
		POLARDB_THREAD_COUNT(thread, split_latency_sum_us,
			now_us - polardb_txn_split_read_start_us);
		POLARDB_THREAD_COUNT_ONE(thread, split_latency_count);
	}
	polardb_txn_split_read_start_us = 0;
}

void PgSQL_Session::polardb_record_txn_split_wait_latency() {
	if (polardb_txn_split_wait_start_us == 0) {
		return;
	}
	const unsigned long long now_us = monotonic_time();
	if (now_us >= polardb_txn_split_wait_start_us) {
		const unsigned long long elapsed_us =
			now_us - polardb_txn_split_wait_start_us;
		POLARDB_THREAD_COUNT(thread, split_lsn_wait_sum_us, elapsed_us);
		polardb_count_lsn_wait_elapsed_bucket(
			thread, elapsed_us, /*transaction_split=*/true);
	}
	polardb_txn_split_wait_start_us = 0;
}

bool PgSQL_Session::polardb_account_txn_split_wait_timeout(const char* source) {
	if (!polardb_txn_split_active || polardb_txn_split_wait_start_us == 0) {
		POLARDB_TRACE(
			"PolarDB TXN_SPLIT: skip timeout accounting source=%s "
			"active=%d read_active=%d wait_start_us=%llu\n",
			source ? source : "",
			polardb_txn_split_active ? 1 : 0,
			polardb_txn_split_read_active() ? 1 : 0,
			polardb_txn_split_wait_start_us);
		return false;
	}
	const unsigned long long wait_start_us = polardb_txn_split_wait_start_us;
	const unsigned long long now_us = monotonic_time();
	const unsigned long long elapsed_us =
		now_us >= wait_start_us ? now_us - wait_start_us : 0;
	(void)elapsed_us; // used only by POLARDB_TRACE in POLARDB_DEBUG builds
	polardb_record_txn_split_wait_latency();
	POLARDB_THREAD_COUNT_ONE(thread, split_error_timeout);
	POLARDB_THREAD_COUNT_ONE(thread, split_error_lsn_wait_timeout);
	POLARDB_TRACE(
		"PolarDB TXN_SPLIT: timeout accounted source=%s "
		"elapsed_us=%llu active=%d read_active=%d\n",
		source ? source : "",
		elapsed_us,
		polardb_txn_split_active ? 1 : 0,
		polardb_txn_split_read_active() ? 1 : 0);
	return true;
}

#endif // POLARDB_PROXY
