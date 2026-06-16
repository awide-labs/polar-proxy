/**
 * @file PgSQL_PolarDB_Failure.cpp
 * @brief Handle failed autocommit wait-wrapped reads.
 *
 * This file handles failures for autocommit reads that ProxySQL has wrapped
 * with a PolarDB LSN wait. One case can be retried: the replica failed before
 * any user result reached the client, and the primary can run the same SQL
 * because the failure was either a strict wait timeout or a lost replica
 * connection.
 *
 * If the query cannot be run again safely, this code still owns the cleanup:
 * the prepended SET wrapper is removed before the normal error path can run, so
 * internal wrapper results are never sent to the client.
 *
 * Transaction-scoped read-failure policy, primary pins, and split-read cleanup
 * are deliberately not implemented here. Future work can extend this file
 * without first moving code out of the main session state machine.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

#include <arpa/inet.h>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY
/**
 * @brief Consume one debug-only wait-retry fault instruction.
 *
 * Tests can either set a one-shot environment variable before ProxySQL starts,
 * or set POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE to a writable file and write one of
 * the supported fault names into it before the query. The file path lets a TAP
 * choose the exact retry attempt to perturb without restarting ProxySQL.
 */
static bool polardb_debug_once_enabled(const char* env_name) {
#if POLARDB_PROXY && POLARDB_DEBUG
	const char* env_value = std::getenv(env_name);
	static std::atomic<bool> fallback_unknown_used{false};
	static std::atomic<bool> result_started_used{false};
	static std::atomic<bool> writer_busy_used{false};
	std::atomic<bool>* used = nullptr;
	const char* fault_name = nullptr;
	if (strcmp(env_name, "POLARDB_DEBUG_WAIT_RETRY_FALLBACK_UNKNOWN_ONCE") == 0) {
		used = &fallback_unknown_used;
		fault_name = "fallback_unknown";
	} else if (strcmp(env_name, "POLARDB_DEBUG_WAIT_RETRY_RESULT_STARTED_ONCE") == 0) {
		used = &result_started_used;
		fault_name = "result_started";
	} else if (strcmp(env_name, "POLARDB_DEBUG_WAIT_RETRY_WRITER_BUSY_ONCE") == 0) {
		used = &writer_busy_used;
		fault_name = "writer_busy";
	}
	if (!used || !fault_name) {
		return false;
	}
	if (env_value && strcmp(env_value, "1") == 0 &&
			!used->exchange(true, std::memory_order_relaxed)) {
		return true;
	}

	// File path: a single shared fault file is probed once per candidate fault
	// name. Clear it only after a match, so a non-matching probe leaves the file
	// intact for the matching probe that follows.
	char buf[64] = {0};
	bool matched = false;
	if (polardb_debug_consume_fault_file(
			"POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE", buf, sizeof(buf))) {
		matched = (strcmp(buf, fault_name) == 0);
	}
	if (matched) {
		polardb_debug_clear_fault_file("POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE");
	}
	return matched;
#else
	(void)env_name;
	return false;
#endif
}

static bool polardb_wait_reader_can_return_to_pool(PgSQL_Connection* conn) {
	return conn &&
		conn->reusable == true &&
		conn->is_connected() &&
		conn->is_connection_in_reusable_state() &&
		conn->IsActiveTransaction() == false &&
		conn->MultiplexDisabled() == false &&
		conn->is_pipeline_active() == false;
}

/**
 * Release the replica stream after a wait-wrapped read is retried on the primary.
 * The caller already captured the retry decision and the pool-return decision,
 * so staged libpq results and the failed query packet must be discarded before
 * the stream is returned to the pool or destroyed.
 */
static void polardb_release_wait_reader(PgSQL_Data_Stream* failed_myds,
		bool can_return_to_pool) {
	if (!failed_myds) {
		return;
	}

	failed_myds->free_pgsql_real_query();
	PgSQL_Connection* failed_conn = failed_myds->myconn;
	if (!failed_conn) {
		failed_myds->DSS = STATE_NOT_INITIALIZED;
		return;
	}

	failed_conn->async_free_result();
	if (can_return_to_pool) {
		failed_conn->async_state_machine = ASYNC_IDLE;
		failed_myds->return_MySQL_Connection_To_Pool();
	} else {
		failed_myds->destroy_MySQL_Connection_From_Pool(false);
		failed_myds->fd = 0;
		failed_myds->DSS = STATE_NOT_INITIALIZED;
	}
}

/**
 * Build one PostgreSQL simple-query packet. Ownership of the allocated buffer
 * moves to the caller through @p out and must eventually be released by
 * PgSQL_MyDS_real_query::end().
 */
void PgSQL_Session::build_simple_query_packet(const std::string& sql,
		PtrSize_t& out) {
	out.ptr = NULL;
	out.size = 0;
	if (sql.empty() || sql.size() > static_cast<size_t>(UINT32_MAX - 6)) {
		return;
	}

	const unsigned int size = static_cast<unsigned int>(sql.size() + 6);
	out.ptr = l_alloc(size);
	if (!out.ptr) {
		return;
	}
	out.size = size;

	char* packet = static_cast<char*>(out.ptr);
	packet[0] = 'Q';
	const uint32_t packet_len = htonl(static_cast<uint32_t>(sql.size() + 5));
	memcpy(packet + 1, &packet_len, sizeof(packet_len));
	memcpy(packet + 5, sql.data(), sql.size());
	packet[size - 1] = '\0';
}

PgSQL_Session::PolarDB_WaitReadFailure
PgSQL_Session::polardb_capture_wait_read_failure(PgSQL_Data_Stream* failed_myds) {
	PolarDB_WaitReadFailure failure;
	failure.failed_myds = failed_myds;
	failure.timeout_error = polardb_query.wait.timeout_error;
	failure.fallback_writer_hg = polardb_query.wait.fallback_writer_hg;
	// Snapshot the original query before normal error handling can clear
	// per-query wait state; the primary retry rebuilds a fresh simple-query packet.
	failure.original_query = polardb_query.wait.original_query;

	if (polardb_debug_once_enabled("POLARDB_DEBUG_WAIT_RETRY_FALLBACK_UNKNOWN_ONCE")) {
		failure.fallback_writer_hg = -1;
	}

	PgSQL_Connection* failed_conn = failed_myds ? failed_myds->myconn : NULL;
	failure.wrapper_set_failure = failed_conn &&
		(failed_conn->polardb_query_wrap_state.wrapper_set_failed() ||
		 failed_conn->polardb_query_wrap_state.consuming_wrapper_set());
	failure.result_started = failed_conn && failed_conn->query_result &&
		failed_conn->query_result->is_transfer_started();
	if (polardb_debug_once_enabled("POLARDB_DEBUG_WAIT_RETRY_RESULT_STARTED_ONCE")) {
		failure.result_started = true;
	}
	failure.connection_lost = !failed_conn ||
		!failed_conn->is_connected() ||
		!failed_conn->is_connection_in_reusable_state();
	failure.can_return_to_pool = polardb_wait_reader_can_return_to_pool(failed_conn);
	// Dispatch wrapper fields move from the session to the connection when the
	// query starts. Capture must therefore use the connection-owned wrapper kind;
	// the session fields have already been cleared by this point.
	const bool consistency_wait = failed_conn &&
		failed_conn->polardb_query_wrap_state.is_consistency_wait();
	failure.wait_read =
		polardb_wait_active() &&
		consistency_wait &&
		polardb_query.wait.wrapper_finalized &&
		!failure.original_query.empty();

	if (failed_conn && failed_conn->parent) {
		PgSQL_SrvC* srv = failed_conn->parent;
		failure.reader_hg = srv->myhgc ? (int)srv->myhgc->hid : -1;
		failure.reader_address = srv->address ? srv->address : "";
		failure.reader_port = (int)srv->port;
	}

	return failure;
}

bool PgSQL_Session::polardb_handle_failed_wait_read(
		const PolarDB_WaitReadFailure& failure) {
	if (!failure.wait_read) {
		return false;
	}

	// A finalized wait-read has already replaced pgsql_real_query with the
	// prepended-SET wrapper and moved the SET-result countdown to the connection.
	// Any later retry path must see no wrapped packet; otherwise it could resend
	// internal SET statements after the connection's SET-result counter was
	// consumed. Remove the wrapper before any later error path can run.
	if (failure.failed_myds) {
		failure.failed_myds->query_retries_on_failure = 0;
		failure.failed_myds->free_pgsql_real_query();
	}
	if (polardb_query.wait.wait_started_at_us != 0) {
		record_wait_latency(polardb_query.wait);
	}
	polardb_query.reset_wait();
	polardb_query.reset_dispatch_wrapper();
	polardb_query.wrapped_query_buf.clear();
	clear_pending_notices(/*free_buffers=*/true);

	const bool recoverable_failure =
		failure.timeout_error || failure.connection_lost;
	if (!recoverable_failure || failure.result_started ||
		failure.fallback_writer_hg < 0 || failure.original_query.empty()) {
		POLARDB_TRACE(
			"PolarDB WAIT: failed wait-read cleaned wrapper; "
			"normal error path will not redispatch wrapped packet "
			"recoverable=%d result_started=%d writer_hg=%d original_query=%d\n",
			recoverable_failure ? 1 : 0, failure.result_started ? 1 : 0,
			failure.fallback_writer_hg, failure.original_query.empty() ? 0 : 1);
		return false;
	}

	PgSQL_Backend* writer_mybe = find_or_create_backend(failure.fallback_writer_hg);
	if (!writer_mybe || !writer_mybe->server_myds ||
			writer_mybe->server_myds == failure.failed_myds) {
		POLARDB_TRACE(
			"PolarDB WAIT: failed wait-read cleaned wrapper; "
			"primary retry stream unavailable writer_hg=%d\n",
			failure.fallback_writer_hg);
		return false;
	}
	PgSQL_Data_Stream* writer_myds = writer_mybe->server_myds;
	// Use a distinct writer stream. Reusing the failed reader stream would mix
	// packet and connection state from the failed dispatch with the retry.
	// Debug builds can force this branch after wrapper cleanup to prove that the
	// normal error path returns a clean client error without leaking wrapper SETs.
	const bool writer_retry_declined_by_debug =
		polardb_debug_once_enabled("POLARDB_DEBUG_WAIT_RETRY_WRITER_BUSY_ONCE");
	if ((writer_myds->myconn &&
				writer_myds->myconn->async_state_machine != ASYNC_IDLE) ||
			writer_retry_declined_by_debug) {
		POLARDB_TRACE(
			"PolarDB WAIT: failed wait-read cleaned wrapper; "
			"primary retry declined writer_hg=%d debug=%d\n",
			failure.fallback_writer_hg,
			writer_retry_declined_by_debug ? 1 : 0);
		return false;
	}

	PtrSize_t retry_pkt = {0, NULL};
	build_simple_query_packet(failure.original_query, retry_pkt);
	if (!retry_pkt.ptr || retry_pkt.size == 0) {
		return false;
	}

	POLARDB_TRACE(
		"PolarDB WAIT: %s before user result; redirecting original "
		"unwrapped query to writer_hg=%d reader_hg=%d\n",
		failure.timeout_error ? "strict wait timeout" : "reader connection lost",
		failure.fallback_writer_hg, failure.reader_hg);
	PgHGM->status.polardb_wait_reads_retried_on_writer.fetch_add(
		1, std::memory_order_relaxed);

	if (failure.reader_hg >= 0 && !failure.reader_address.empty()) {
		PgHGM->p_update_pgsql_error_counter(
			p_pgsql_error_type::pgsql,
			failure.reader_hg,
			const_cast<char*>(failure.reader_address.c_str()),
			failure.reader_port,
			9999);
	}

	polardb_release_wait_reader(
		failure.failed_myds, failure.can_return_to_pool);

	current_hostgroup = failure.fallback_writer_hg;
	mybe = writer_mybe;
	writer_myds->free_pgsql_real_query();
	writer_myds->pgsql_real_query.init(&retry_pkt);
	retry_pkt.ptr = NULL;
	retry_pkt.size = 0;

	CurrentQuery.query_parser_free();
	CurrentQuery.begin(
		reinterpret_cast<unsigned char*>(writer_myds->pgsql_real_query.pkt.ptr),
		writer_myds->pgsql_real_query.pkt.size,
		true);
	set_previous_status_mode3();
	return true;
}

// Send only this one query to the writer hostgroup. Used when a consistent
// reader cannot be obtained (no target-reaching reader can be acquired, or the
// requested guarantee cannot be enforced). The writer/primary already holds the latest
// WAL, so any required LSN is satisfied there and the LSN wait intent is
// dropped: reset_reader_target() and reset_wait() make sure no stale
// consistency-target LSN is carried onto the writer connection. Returns false when
// no writer hostgroup is known, leaving the caller to use the normal pool path.
bool PgSQL_Session::polardb_redirect_to_writer(int writer_hg, const char* reason) {
	if (writer_hg < 0) {
		return false;
	}

	PgSQL_Data_Stream* source_myds = mybe ? mybe->server_myds : NULL;

	POLARDB_TRACE(
		"PolarDB consistency: %s; redirecting this query to writer_hg=%d\n",
		reason ? reason : "reader acquisition fallback",
		writer_hg);
	POLARDB_THREAD_COUNT_ONE(thread, consistency_writer_fallback);
	polardb_query.reset_reader_target();
	polardb_query.reset_wait();
	current_hostgroup = writer_hg;
	mybe = find_or_create_backend(current_hostgroup);
	if (source_myds && mybe && mybe->server_myds != source_myds &&
			source_myds->pgsql_real_query.QueryPtr) {
		// The simple-query packet was attached to the reader backend before
		// acquisition. A one-query writer redirect changes backend streams, so
		// transfer that packet ownership to the writer stream before RunQuery().
		mybe->server_myds->free_pgsql_real_query();
		mybe->server_myds->pgsql_real_query.move_from(
			source_myds->pgsql_real_query);
	}
	return true;
}

#endif // POLARDB_PROXY
