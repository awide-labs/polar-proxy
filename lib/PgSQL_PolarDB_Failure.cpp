/**
 * @file PgSQL_PolarDB_Failure.cpp
 * @brief Choose the recovery action for a failed PolarDB reader read.
 *
 * This file owns every automatically routed reader-failure path through one
 * failure view: transaction-split, wait-wrapped, and ordinary replica reads,
 * including completed PostgreSQL ErrorResponse results and connection failures.
 * Classification runs before ProxySQL's generic rc==-1 retry/error logic.
 *
 * The failure stage answers with one of four actions. RETRY re-dispatches the
 * original client query, either on another reader of the same reader hostgroup
 * or on the live writer backend. FORWARD emits the captured reader error with
 * ReadyForQuery('T') and keeps the transaction on the writer. TERMINATE closes
 * the client session when no safe writer state remains. PASSTHROUGH means
 * PolarDB does not handle the failure and ProxySQL's generic rc==-1 path must run.
 */

#include "PgSQL_Session.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Error_Helper.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_Protocol.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <utility>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY
static constexpr uint8_t POLARDB_READER_RETRY_LIMIT = 1;

static std::string polardb_truncate_error_message(const std::string& message) {
	constexpr size_t kMaxSnapshotBytes = 512;
	if (message.size() <= kMaxSnapshotBytes) {
		return message;
	}
	return message.substr(0, kMaxSnapshotBytes);
}

static bool polardb_packet_contains_pointer(
		const PtrSize_t& packet, const void* pointer) {
	if (!packet.ptr || packet.size == 0 || !pointer) {
		return false;
	}
	const uintptr_t begin = reinterpret_cast<uintptr_t>(packet.ptr);
	const uintptr_t end = begin + packet.size;
	const uintptr_t value = reinterpret_cast<uintptr_t>(pointer);
	return end >= begin && value >= begin && value < end;
}

static void polardb_count_wait_retry_counter(
		std::atomic<unsigned long long>& counter,
		const char* name) {
	counter.fetch_add(1, std::memory_order_relaxed);
	POLARDB_TRACE("PolarDB WAIT: retry counter=%s\n", name ? name : "");
}

static bool polardb_failed_connection_is_poolable(PgSQL_Connection* conn);

static bool polardb_writer_scope_is_current(
		const PolarDB_WriterScope& scope) {
	if (!PgHGM || !scope.valid()) {
		return false;
	}
	const auto config =
		PgHGM->get_polardb_hg_config(static_cast<unsigned int>(scope.hg));
	return config.is_polardb_hostgroup &&
		config.writer_hostgroup == scope.hg &&
		config.writer_epoch == scope.epoch;
}

static bool polardb_reader_matches_writer_scope(
		int reader_hostgroup, const PolarDB_WriterScope& scope) {
	if (!PgHGM || reader_hostgroup < 0 || !scope.valid()) {
		return false;
	}
	const auto config = PgHGM->get_polardb_hg_config(
		static_cast<unsigned int>(reader_hostgroup));
	return config.is_polardb_hostgroup &&
		config.reader_hostgroup == reader_hostgroup &&
		config.writer_hostgroup == scope.hg &&
		config.writer_epoch == scope.epoch;
}

/**
 * @brief Tell whether a connection that just failed a query can still be used.
 *
 * This is not a general "is this connection healthy" test: it requires an error
 * to be present, so a live, error-free connection returns false. Call it only on
 * the rc==-1 failure path. Its result becomes PolarDB_RequestOutcome::reusable
 * and PolarDB_ReaderFailure::reusable, which is what separates CONNECTION_LOST
 * from REUSABLE_ERROR in polardb_reader_failure_kind_for(); using it anywhere
 * else classifies every working connection as lost.
 *
 * @param conn  Connection that reported the failure. May be null.
 * @return true when the connection is still connected, carries a backend error
 *         and is in a reusable protocol state, false otherwise.
 */
static bool polardb_failed_connection_is_reusable(PgSQL_Connection* conn) {
	return conn && conn->is_connected() && conn->is_error_present() &&
		conn->is_connection_in_reusable_state();
}

#if POLARDB_DEBUG
static const char* polardb_writer_state_name(PolarDB_WriterState state) {
	switch (state) {
	case PolarDB_WriterState::LIVE:
		return "live";
	case PolarDB_WriterState::NOT_STARTED:
		return "not_started";
	case PolarDB_WriterState::LOST:
		return "lost";
	}
	return "unknown";
}
#endif // POLARDB_DEBUG

/**
 * @brief Consume a transaction-split reader-failure fault for TAP coverage.
 *
 * These faults are DEBUG-only and file-driven so tests can enable one exact
 * split-failure branch after ProxySQL has started. The helper clears the file
 * only when the requested fault name matches, preserving the shared
 * match-limited semantics used by the other PolarDB debug fault files.
 */
static bool polardb_debug_consume_split_failure_fault(
		const char* fault_name) {
#if POLARDB_PROXY && POLARDB_DEBUG
	static std::atomic<int> death_twice_remaining{0};
	char buf[64] = {0};
	if (!fault_name ||
			!polardb_debug_read_fault_file(
				"POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE",
				buf, sizeof(buf))) {
		return false;
	}
	const bool matched = strcmp(buf, fault_name) == 0;
	if (matched) {
		polardb_debug_clear_fault_file(
			"POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE");
		POLARDB_TRACE(
			"PolarDB FAILURE DEBUG: consumed split failure fault=%s\n",
			fault_name);
		return true;
	}
	if (strcmp(fault_name, "death") == 0 &&
			strcmp(buf, "death_twice") == 0) {
		// Drive the reader-retry limit path: first death can retry another
		// reader, the second must fall back to the writer in the same statement.
		if (death_twice_remaining.load(std::memory_order_relaxed) <= 0) {
			death_twice_remaining.store(2, std::memory_order_relaxed);
		}
		const int before =
			death_twice_remaining.fetch_sub(1, std::memory_order_relaxed);
		if (before <= 1) {
			death_twice_remaining.store(0, std::memory_order_relaxed);
			polardb_debug_clear_fault_file(
				"POLARDB_DEBUG_SPLIT_FAILURE_FAULT_FILE");
		}
		POLARDB_TRACE(
			"PolarDB FAILURE DEBUG: consumed split failure fault=death_twice "
			"remaining=%d\n",
			before > 0 ? before - 1 : 0);
		return true;
	}
	return false;
#else
	(void)fault_name;
	return false;
#endif
}

static bool polardb_debug_writer_changed_after_reader_acquire() {
#if POLARDB_PROXY && POLARDB_DEBUG
	char buf[64] = {0};
	if (polardb_debug_read_fault_file(
			"POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE",
			buf, sizeof(buf)) &&
			strcmp(buf, "writer_changed_after_reader_acquire") == 0) {
		polardb_debug_clear_fault_file(
			"POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE");
		return true;
	}
#endif
	return false;
}

#if POLARDB_DEBUG
static bool polardb_debug_consume_connect_deadline_fault(
		const char* fault_name) {
	char buf[64] = {0};
	if (fault_name && polardb_debug_read_fault_file(
			"POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE",
			buf, sizeof(buf)) && strcmp(buf, fault_name) == 0) {
		polardb_debug_clear_fault_file(
			"POLARDB_DEBUG_CONNECT_DEADLINE_FAULT_FILE");
		return true;
	}
	return false;
}
#endif // POLARDB_DEBUG

PolarDB_RequestOutcome PgSQL_Session::polardb_capture_request_outcome(
		PgSQL_Backend* backend) {
	PolarDB_RequestOutcome outcome;
	outcome.backend = backend;
	if (!backend) {
		return outcome;
	}

	outcome.backend_hg = backend->hostgroup_id;
	outcome.backend_myds = backend->server_myds;
	if (!backend->server_myds || !backend->server_myds->myconn) {
		return outcome;
	}

	PgSQL_Connection* conn = backend->server_myds->myconn;
	outcome.connected = conn->is_connected();
	outcome.reusable = polardb_failed_connection_is_reusable(conn);
	outcome.can_return_to_pool =
		polardb_failed_connection_is_poolable(conn);
	outcome.result_started = conn->query_result &&
		conn->query_result->is_transfer_started();
	outcome.timeout_error = polardb_query.wait.timeout_error;
	outcome.wrapper_set_failure =
		conn->polardb_query_wrap_state.wrapper_set_failed() ||
		conn->polardb_query_wrap_state.consuming_wrapper_set();
	outcome.wrapper_is_consistency_wait =
		conn->polardb_query_wrap_state.is_consistency_wait();
	outcome.timeout_already_accounted =
		outcome.timeout_error && polardb_query.wait.wait_started_at_us == 0;
	if (outcome.wrapper_set_failure &&
			polardb_txn_reader.split_active &&
			backend == polardb_txn_reader.backend &&
			polardb_txn_reader.wait_timeout_error) {
		outcome.timeout_error = true;
		outcome.timeout_already_accounted = true;
	}
	outcome.backend_error_code = static_cast<int>(conn->get_error_code());
	outcome.error_message = polardb_truncate_error_message(conn->get_error_message());

	if (conn->parent) {
		outcome.backend_hg = conn->parent->myhgc ?
			(int)conn->parent->myhgc->hid : outcome.backend_hg;
		outcome.backend_address = conn->parent->address ?
			conn->parent->address : "";
		outcome.backend_port = (int)conn->parent->port;
	}

	return outcome;
}

/**
 * @brief Return the recovery action for a PolarDB reader request that failed.
 *
 * Call this on the rc==-1 path, before ProxySQL's generic retry and error paths,
 * with the outcome captured by polardb_capture_request_outcome(). Transaction-split
 * reads, wait-wrapped reads, and ordinary automatically routed replica reads
 * are all classified here so every reader failure uses one policy stage.
 *
 * @param outcome  Snapshot of the failed backend taken while its connection and
 *                 error text were still intact.
 * @return One of four actions, each of which obliges the caller differently:
 *         - RETRY: the query has already been re-dispatched. The replacement
 *           target is either another reader in the same reader hostgroup (at
 *           most POLARDB_READER_RETRY_LIMIT times per client statement) or the
 *           live writer backend. current_hostgroup, mybe and CurrentQuery are
 *           already pointed at that backend, so the caller must return to the
 *           dispatch loop and must not run ProxySQL's generic retry.
 *         - FORWARD: the reader error plus ReadyForQuery('T') has been written to
 *           the client and the transaction stays on the writer.
 *         - TERMINATE: no safe writer state remains; close the client session.
 *         - PASSTHROUGH: PolarDB did not handle it; run the generic rc==-1 path.
 */
PolarDB_FailureAction PgSQL_Session::polardb_handle_reader_failure(
		const PolarDB_RequestOutcome& outcome) {
	PolarDB_ReaderFailure failure = polardb_take_reader_failure(outcome);
	if (!failure.is_split()) {
		// Session-consistency wait reads are not transaction-split reads, but
		// they still belong to the same PolarDB reader-failure stage. Keep their
		// cleanup/retry here so PgSQL_Session.cpp reaches PolarDB failure
		// classification in one call, before falling through to ProxySQL's
		// generic rc==-1 path.
		failure = polardb_capture_wait_read_failure(std::move(failure));
		POLARDB_TRACE(
			"PolarDB WAIT: rc=-1 wait_active=%d wait_read=%d wrapper_set_failure=%d "
			"timeout_error=%d connection_lost=%d result_started=%d "
			"wrapper_finalized=%d original_query_saved=%d can_return_to_pool=%d "
			"retry_writer_hg=%d\n",
			polardb_wait_active() ? 1 : 0,
			failure.is_wait() ? 1 : 0,
			failure.wrapper_set_failure ? 1 : 0,
			failure.timeout ? 1 : 0,
			failure.reusable ? 0 : 1,
			failure.result_started ? 1 : 0,
			polardb_query.wait.wrapper_finalized ? 1 : 0,
			failure.retry_query.empty() ? 0 : 1,
			failure.can_return_to_pool ? 1 : 0,
			failure.fallback_writer_hg);
		if (failure.is_wait() && !failure.reusable) {
			POLARDB_THREAD_COUNT_ONE(thread, wait_error_connection_lost);
		}
		const bool txn_wait_reader_failed =
			polardb_txn_reader.wait_read_active &&
			polardb_txn_reader.backend &&
			polardb_txn_reader.backend->server_myds == outcome.backend_myds;
		if (txn_wait_reader_failed && !failure.is_wait()) {
			PgSQL_MyDS_real_query& failed_query =
				outcome.backend_myds->pgsql_real_query;
			if (failure.retry_query.empty() && failed_query.QueryPtr &&
					failed_query.QuerySize > 0) {
				size_t query_size = failed_query.QuerySize;
				if (failed_query.QueryPtr[query_size - 1] == '\0') {
					--query_size;
				}
				failure.retry_query.assign(failed_query.QueryPtr, query_size);
			}
			failure.request_kind = PolarDB_ReaderRequestKind::WAIT;
			if (failure.fallback_writer_hg < 0) {
				failure.fallback_writer_hg =
					polardb_txn_reader_failure.writer_hg >= 0 ?
					polardb_txn_reader_failure.writer_hg : polardb_writer_hostgroup_or_current();
			}
			POLARDB_TRACE(
				"PolarDB WAIT: txn-wait reader failure did not classify as "
				"wait_read; releasing temporary reader reader_hg=%d "
				"writer_hg=%d\n",
				failure.reader_hg, failure.fallback_writer_hg);
		}
		if (!failure.is_wait()) {
			failure = polardb_capture_ordinary_reader_failure(
				std::move(failure));
		}
		if (failure.is_wait() || failure.is_ordinary()) {
			// Same policy selector as transaction split. The wait-read adapter
			// below owns the different packet cleanup and retry mechanics.
			const PolarDB_ReaderFailureDecision decision =
				polardb_reader_failure_decision(failure);
			return polardb_handle_failed_reader_read(failure, decision);
		}

		// If this was not a retryable wait-read but still hit wrapper/timeout/
		// reader-loss state, clear request-local wait data before the generic
		// path runs. Ordinary SQL errors keep the normal ProxySQL path.
		if (polardb_wait_active() &&
				(failure.wrapper_set_failure ||
				 failure.timeout ||
				 !failure.reusable) &&
				polardb_query.wait.wait_started_at_us != 0) {
			polardb_finish_wait(nullptr);
			polardb_query.reset_wait();
			discard_pending_notices();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	if (polardb_debug_consume_split_failure_fault("death")) {
		failure.connected = false;
		failure.reusable = false;
		failure.timeout = false;
		if (failure.backend_error_message.empty()) {
			failure.has_backend_error = true;
			failure.backend_error_message =
				"PolarDB DEBUG split reader death";
			failure.backend_error_code =
				PGSQL_ERROR_CODES::ERRCODE_CONNECTION_FAILURE;
		}
	}
	if (polardb_debug_consume_split_failure_fault("sql_error")) {
		failure.connected = true;
		failure.reusable = true;
		failure.timeout = false;
		failure.has_backend_error = true;
		failure.backend_error_code =
			PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION;
		failure.backend_error_message =
			"PolarDB DEBUG split reader SQL error";
	}
	if (polardb_debug_consume_split_failure_fault("result_started")) {
		failure.result_started = true;
	}
	if (polardb_debug_consume_split_failure_fault("no_retry_packet")) {
		polardb_free_retry_pkt_if_owned(failure);
	}
	if (polardb_debug_consume_split_failure_fault("writer_epoch_changed") &&
			failure.writer_scope.valid()) {
		++failure.writer_scope.epoch;
		POLARDB_TRACE(
			"PolarDB FAILURE DEBUG: changed captured writer epoch before "
			"split retry\n");
	}

	POLARDB_TRACE(
		"PolarDB FAILURE: split reader_hg=%d connected=%d reusable=%d "
		"timeout=%d timeout_accounted=%d result_started=%d error='%s'\n",
		failure.reader_hg, failure.connected ? 1 : 0,
		failure.reusable ? 1 : 0, failure.timeout ? 1 : 0,
		failure.timeout_already_accounted ? 1 : 0,
		failure.result_started ? 1 : 0,
		failure.backend_error_message.c_str());

	POLARDB_THREAD_COUNT_ONE(thread, split_error_query_failed);
	if (!failure.connected) {
		POLARDB_THREAD_COUNT_ONE(thread, split_error_connection_lost);
	}
	if (failure.timeout && !failure.timeout_already_accounted) {
		POLARDB_THREAD_COUNT_ONE(thread, split_error_timeout);
		POLARDB_THREAD_COUNT_ONE(thread, split_error_lsn_wait_timeout);
		POLARDB_TRACE(
			"PolarDB FAILURE: split timeout accounted in failure handler "
			"reader_hg=%d\n",
			failure.reader_hg);
	}

	int writer_hg = -1;
	PgSQL_Backend* writer_backend = nullptr;
	PolarDB_WriterState writer_state =
		polardb_resolve_writer_state(failure, writer_hg, writer_backend);
	if (polardb_debug_consume_split_failure_fault("writer_lost")) {
		writer_hg = -1;
		writer_backend = nullptr;
		writer_state = PolarDB_WriterState::LOST;
	}
	if (polardb_debug_consume_split_failure_fault("writer_not_started")) {
		writer_hg = polardb_writer_hostgroup_or_current();
		writer_backend = nullptr;
		writer_state = PolarDB_WriterState::NOT_STARTED;
	}
	POLARDB_TRACE(
		"PolarDB FAILURE: writer_state=%s writer_hg=%d writer_backend=%p\n",
		polardb_writer_state_name(writer_state), writer_hg,
		static_cast<void*>(writer_backend));

	polardb_record_reader_failure(failure);
	polardb_end_split_after_reader_failure();

	if (writer_state == PolarDB_WriterState::LOST) {
		POLARDB_TRACE(
			"PolarDB FAILURE: terminating because writer transaction state is lost\n");
		return polardb_terminate_session_after_reader_failure(failure);
	}

	const PolarDB_ReaderFailureDecision decision =
		polardb_reader_failure_decision(failure);
	POLARDB_TRACE(
		"PolarDB FAILURE: policy kind=%s action=%s target=%s followup_route=%s "
		"timeout=%d reusable=%d result_started=%d\n",
		polardb_reader_failure_kind_name(decision.kind),
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		polardb_reader_failure_route_name(decision.reader_failure_route),
		failure.timeout ? 1 : 0,
		failure.reusable ? 1 : 0,
		failure.result_started ? 1 : 0);
	if (decision.action == PolarDB_ReaderAction::DISCONNECT_CLIENT) {
		return polardb_terminate_session_after_reader_failure(failure);
	}

	if (decision.action == PolarDB_ReaderAction::RETRY &&
			!failure.result_started) {
		if (decision.retry_target == PolarDB_RetryTarget::OTHER_READER &&
				polardb_try_redispatch_to_other_reader(failure)) {
			polardb_apply_reader_failure_route_state(
				PolarDB_ReaderFailureRoute::SKIP_READER, writer_hg, &failure);
			return PolarDB_FailureAction::RETRY;
		}
		if (decision.allow_writer_retry &&
				writer_state == PolarDB_WriterState::LIVE &&
				polardb_try_redispatch_to_writer(
					failure, writer_hg, writer_backend)) {
			polardb_apply_reader_failure_route_state(
				PolarDB_ReaderFailureRoute::FORCE_WRITER, writer_hg, &failure);
			return PolarDB_FailureAction::RETRY;
		}
	}

	if (decision.allow_writer_retry) {
		polardb_apply_reader_failure_route_state(
			PolarDB_ReaderFailureRoute::FORCE_WRITER, writer_hg, &failure);
	}
	POLARDB_TRACE(
		"PolarDB FAILURE: forwarding reader error after retry declined "
		"(action=%s target=%s writer_state=%s result_started=%d)\n",
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		polardb_writer_state_name(writer_state),
		failure.result_started ? 1 : 0);
	return polardb_forward_error_and_keep_writer(failure);
}

/**
 * @brief Convert one backend outcome into the common reader-failure record.
 *
 * Request-specific functions add the request kind and packet ownership.
 */
PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_failure_from_outcome(
		const PolarDB_RequestOutcome& outcome) {
	PolarDB_ReaderFailure failure;
	failure.reader_backend = outcome.backend;
	failure.failed_myds = outcome.backend_myds;
	failure.connected = outcome.connected;
	failure.reusable = outcome.reusable;
	failure.can_return_to_pool = outcome.can_return_to_pool;
	failure.result_started = outcome.result_started;
	failure.timeout = outcome.timeout_error;
	failure.timeout_already_accounted = outcome.timeout_already_accounted;
	failure.wrapper_set_failure = outcome.wrapper_set_failure;
	failure.wrapper_is_consistency_wait =
		outcome.wrapper_is_consistency_wait;
	failure.reader_hg = outcome.backend_hg;
	failure.reader_address = outcome.backend_address;
	failure.reader_port = outcome.backend_port;
	failure.has_backend_error = !outcome.error_message.empty();
	failure.backend_error_message = outcome.error_message;
	if (outcome.backend_error_code >= 0) {
		failure.backend_error_code =
			static_cast<PGSQL_ERROR_CODES>(outcome.backend_error_code);
	}
	return failure;
}

/**
 * @brief Add transaction-split state and take the retry packet when applicable.
 *
 * The original client packet is moved out of polardb_txn_reader before split
 * cleanup can free it. The caller must install failure.retry_pkt into another
 * stream or release it with polardb_free_retry_pkt_if_owned().
 */
PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_take_reader_failure(
		const PolarDB_RequestOutcome& outcome) {
	PolarDB_ReaderFailure failure = polardb_failure_from_outcome(outcome);

	if (polardb_txn_reader.split_active &&
			failure.reader_backend == polardb_txn_reader.backend) {
		failure.request_kind = PolarDB_ReaderRequestKind::SPLIT;
		// Move the original client packet before split cleanup can free it.
		// RETRY moves it to the next backend stream; FORWARD/TERMINATE free it.
		// Reader-retry also needs the split wait target and XIDs because the
		// active split wrapper is discarded during failure cleanup and rebuilt
		// for the replacement reader.
		failure.retry_pkt = polardb_txn_reader.release_original_packet();
		failure.reader_plan = polardb_query.reader_plan;
		failure.wait_spec = polardb_txn_reader.wait_spec;
		failure.writer_scope = polardb_txn_reader.writer_scope;
		failure.txn_xids = polardb_transaction_split.xids;
		POLARDB_TRACE(
			"PolarDB FAILURE: captured split retry packet size=%u "
			"reader_backend=%p target_lsn=%lu xids_len=%zu\n",
			failure.retry_pkt.size,
			static_cast<void*>(failure.reader_backend),
			(unsigned long)failure.wait_spec.target,
			failure.txn_xids.size());
	}
	return failure;
}

/**
 * @brief Give a best-effort writer hostgroup for the hostgroup being read from.
 *
 * The mapping comes from the reader-to-writer configuration. When no mapping is
 * configured the result falls back to current_hostgroup, so the returned value
 * can be the reader's own hostgroup and does not confirm that a writer was
 * found. A caller that routes a failed read on this value alone can send it back
 * to the same replica; check the result against the reader hostgroup first.
 *
 * @return The mapped writer hostgroup, current_hostgroup when no mapping exists,
 *         or -1 when current_hostgroup is not set.
 */
int PgSQL_Session::polardb_writer_hostgroup_or_current() {
	if (current_hostgroup < 0) {
		return -1;
	}
	const int mapped =
		PgHGM->get_writer_hostgroup_for_reader((unsigned int)current_hostgroup);
	return mapped >= 0 ? mapped : current_hostgroup;
}

PgSQL_Backend* PgSQL_Session::polardb_find_open_transaction_backend(
		int expected_writer_hg, PgSQL_Backend* exclude_reader) {
	if (expected_writer_hg < 0 || !mybes) {
		return nullptr;
	}
	PgSQL_Backend* backend = find_backend(expected_writer_hg);
	if (!backend || backend == exclude_reader || !backend->server_myds) {
		return nullptr;
	}
	PgSQL_Connection* conn = backend->server_myds->myconn;
	return conn && conn->is_connected() && conn->IsKnownActiveTransaction()
		? backend : nullptr;
}

/**
 * @brief Establish what is left of the writer side after a reader failed.
 *
 * A live writer backend is preferred. Failing that, evidence that a writer had
 * already started (exported transaction XIDs or a transaction persistent
 * hostgroup) without a live backend means the transaction cannot be continued.
 * A transaction that never touched a writer can still be moved to one, provided
 * a writer hostgroup exists that is not the failed reader's own hostgroup.
 *
 * @param failure         Failure record for the reader that just failed.
 * @param writer_hg       Out-parameter, see the return values below.
 * @param writer_backend  Out-parameter, see the return values below. The
 *                        backend stays owned by the session.
 * @return - LIVE: both out-parameters are set and the query may be retried on
 *           writer_backend.
 *         - NOT_STARTED: writer_hg is a usable hostgroup but writer_backend is
 *           null, so no retry is possible while the transaction can continue.
 *         - LOST: both out-parameters are cleared to -1/nullptr and the caller
 *           must terminate the session.
 */
PolarDB_WriterState PgSQL_Session::polardb_resolve_writer_state(
		const PolarDB_ReaderFailure& failure, int& writer_hg,
		PgSQL_Backend*& writer_backend) {
	writer_hg = -1;
	writer_backend = nullptr;
	if (!failure.writer_scope.valid()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: split retry has no writer scope\n");
		return PolarDB_WriterState::LOST;
	}

	writer_hg = failure.writer_scope.hg;
	const auto config = PgHGM
		? PgHGM->get_polardb_hg_config(
			static_cast<unsigned int>(writer_hg))
		: PgSQL_HostGroups_Manager::PolarDB_HG_Config{};
	const uint64_t current_epoch = config.writer_epoch;
	if (!config.is_polardb_hostgroup ||
			config.writer_hostgroup != writer_hg ||
			current_epoch != failure.writer_scope.epoch) {
		POLARDB_TRACE(
			"PolarDB FAILURE: writer scope changed before split retry "
			"expected_hg=%d expected_epoch=%lu current_epoch=%lu\n",
			writer_hg, (unsigned long)failure.writer_scope.epoch,
			(unsigned long)current_epoch);
		writer_hg = -1;
		return PolarDB_WriterState::LOST;
	}

	writer_backend = polardb_find_open_transaction_backend(
		writer_hg, failure.reader_backend);
	if (writer_backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: live writer found for split retry writer_hg=%d\n",
			writer_hg);
		return PolarDB_WriterState::LIVE;
	}

	const bool writer_existed =
		!polardb_transaction_split.xids.empty() ||
		transaction_persistent_hostgroup != -1;
	if (writer_existed) {
		POLARDB_TRACE(
			"PolarDB FAILURE: writer evidence exists but no live writer backend "
			"(xids_len=%zu trx_persistent_hg=%d)\n",
			polardb_transaction_split.xids.size(),
			transaction_persistent_hostgroup);
		writer_hg = -1;
		writer_backend = nullptr;
		return PolarDB_WriterState::LOST;
	}

	if (is_in_transaction()) {
		if (writer_hg < 0 || writer_hg == failure.reader_hg) {
			POLARDB_TRACE(
				"PolarDB FAILURE: no safe writer hostgroup for reader failure "
				"computed_writer_hg=%d reader_hg=%d\n",
				writer_hg, failure.reader_hg);
			writer_hg = -1;
			return PolarDB_WriterState::LOST;
		}
		POLARDB_TRACE(
			"PolarDB FAILURE: transaction has no started writer backend; "
			"computed writer_hg=%d\n",
			writer_hg);
		return PolarDB_WriterState::NOT_STARTED;
	}

	POLARDB_TRACE(
		"PolarDB FAILURE: no transaction context remains for reader failure\n");
	writer_hg = -1;
	writer_backend = nullptr;
	return PolarDB_WriterState::LOST;
}

PolarDB_ReaderFailureKind PgSQL_Session::polardb_reader_failure_kind_for(
		const PolarDB_ReaderFailure& failure) {
	if (failure.timeout) {
		return PolarDB_ReaderFailureKind::WAIT_TIMEOUT;
	}
	if (!failure.reusable) {
		return PolarDB_ReaderFailureKind::CONNECTION_LOST;
	}
	return PolarDB_ReaderFailureKind::REUSABLE_ERROR;
}

/**
 * @brief Turn a reader failure into the configured recovery action.
 *
 * The failure is first classified as CONNECTION_LOST, WAIT_TIMEOUT or
 * REUSABLE_ERROR. Each event has its own public action vocabulary, which is
 * converted here to the three operations shared by the failure executor:
 * retry, return an error, or disconnect the client.
 *
 * @param failure  Classified failure record.
 * @return The selected client outcome and, for a retry, its backend target.
 */
PgSQL_Session::PolarDB_ReaderFailureDecision
PgSQL_Session::polardb_reader_failure_decision(
		const PolarDB_ReaderFailure& failure) {
	PolarDB_ReaderFailureDecision decision;
	decision.kind = polardb_reader_failure_kind_for(failure);
	decision.allow_writer_retry = true;
	switch (decision.kind) {
	case PolarDB_ReaderFailureKind::CONNECTION_LOST: {
		const PolarDB_ReplicaLossAction action =
			polardb_replica_loss_action_from_int(
				failure.reader_plan.replica_loss_action);
		switch (action) {
		case PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY:
		case PolarDB_ReplicaLossAction::PRIMARY:
			decision.action = PolarDB_ReaderAction::RETRY;
			break;
		case PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR:
			decision.action = PolarDB_ReaderAction::RETRY;
			decision.allow_writer_retry = false;
			break;
		case PolarDB_ReplicaLossAction::ERROR:
			decision.action = PolarDB_ReaderAction::RETURN_ERROR;
			break;
		case PolarDB_ReplicaLossAction::DISCONNECT:
			decision.action = PolarDB_ReaderAction::DISCONNECT_CLIENT;
			break;
		}
		if ((action ==
					PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY ||
				action ==
					PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR) &&
				failure.request_kind != PolarDB_ReaderRequestKind::NONE &&
				!failure.wait_was_bypassed &&
				polardb_query.reader_retry_attempts <
					POLARDB_READER_RETRY_LIMIT) {
			decision.retry_target = PolarDB_RetryTarget::OTHER_READER;
			decision.reader_failure_route =
				PolarDB_ReaderFailureRoute::SKIP_READER;
		} else if (action ==
				PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR) {
			decision.action = PolarDB_ReaderAction::RETURN_ERROR;
		}
		break;
	}
	case PolarDB_ReaderFailureKind::WAIT_TIMEOUT: {
		const PolarDB_LsnWaitTimeoutAction action =
			polardb_lsn_wait_timeout_action_from_int(
				failure.reader_plan.lsn_wait_timeout_action);
		switch (action) {
		case PolarDB_LsnWaitTimeoutAction::PRIMARY:
			decision.action = PolarDB_ReaderAction::RETRY;
			break;
		case PolarDB_LsnWaitTimeoutAction::DISCONNECT:
			decision.action = PolarDB_ReaderAction::DISCONNECT_CLIENT;
			break;
		case PolarDB_LsnWaitTimeoutAction::WARNING:
			// A stale-with-warning wait normally completes successfully with a
			// notice. If it reaches this error path, returning the error is safer
			// than inventing a retry that the configured action did not request.
		case PolarDB_LsnWaitTimeoutAction::ERROR:
			decision.action = PolarDB_ReaderAction::RETURN_ERROR;
			break;
		}
		break;
	}
	case PolarDB_ReaderFailureKind::REUSABLE_ERROR: {
		const PolarDB_ReplicaErrorAction action =
			polardb_replica_error_action_from_int(
				failure.reader_plan.replica_error_action);
		switch (action) {
		case PolarDB_ReplicaErrorAction::PRIMARY:
			decision.action = PolarDB_ReaderAction::RETRY;
			break;
		case PolarDB_ReplicaErrorAction::ERROR:
			decision.action = PolarDB_ReaderAction::RETURN_ERROR;
			break;
		case PolarDB_ReplicaErrorAction::DISCONNECT:
			decision.action = PolarDB_ReaderAction::DISCONNECT_CLIENT;
			break;
		}
		break;
	}
	}

	// Reader-only placement turns any defensive writer retry into an error.
	if (!decision.allow_writer_retry &&
			decision.action == PolarDB_ReaderAction::RETRY &&
			decision.retry_target == PolarDB_RetryTarget::WRITER) {
		decision.action = PolarDB_ReaderAction::RETURN_ERROR;
	}

	if (decision.reader_failure_route == PolarDB_ReaderFailureRoute::NONE &&
			decision.action != PolarDB_ReaderAction::DISCONNECT_CLIENT &&
			decision.allow_writer_retry) {
		decision.retry_target = PolarDB_RetryTarget::WRITER;
		decision.reader_failure_route =
			PolarDB_ReaderFailureRoute::FORCE_WRITER;
	}
	return decision;
}

void PgSQL_Session::polardb_apply_reader_failure_route_state(
		PolarDB_ReaderFailureRoute route, int writer_hg,
		const PolarDB_ReaderFailure* failure) {
	if (route == PolarDB_ReaderFailureRoute::NONE) {
		return;
	}
	if (route == PolarDB_ReaderFailureRoute::FORCE_WRITER) {
		polardb_txn_reader_failure.set_force_writer(writer_hg);
		POLARDB_TRACE(
			"PolarDB FAILURE: transaction route=%s writer_hg=%d "
			"after reader failure\n",
			polardb_reader_failure_route_name(route), writer_hg);
		return;
	}

	if (failure && failure->reader_hg >= 0 &&
			!failure->reader_address.empty() &&
			failure->reader_port >= 0) {
		polardb_txn_reader_failure.set_reader_skip(
			failure->reader_hg, failure->reader_address,
			failure->reader_port);
	} else {
		polardb_txn_reader_failure.clear();
		polardb_txn_reader_failure.route = route;
	}
	POLARDB_TRACE(
		"PolarDB FAILURE: transaction route=%s skipped_reader=%s:%d "
		"reader_hg=%d after reader failure\n",
		polardb_reader_failure_route_name(route),
		polardb_txn_reader_failure.reader_address.c_str(),
		polardb_txn_reader_failure.reader_port,
		polardb_txn_reader_failure.reader_hg);
}

void PgSQL_Session::polardb_free_retry_pkt_if_owned(
		PolarDB_ReaderFailure& failure) {
	if (failure.retry_pkt.ptr) {
		if (polardb_packet_contains_pointer(
				failure.retry_pkt, CurrentQuery.QueryPointer)) {
			// CurrentQuery borrows the SQL text inside the client packet. A
			// terminal retry path has no stream left to own that packet, so
			// detach the parser before releasing the buffer.
			CurrentQuery.query_parser_free();
			CurrentQuery.PgQueryCmd = PGSQL_QUERY___NONE;
			CurrentQuery.QueryPointer = nullptr;
			CurrentQuery.QueryLength = 0;
		}
		l_free(failure.retry_pkt.size, failure.retry_pkt.ptr);
		failure.retry_pkt = {};
	}
}

/**
 * @brief Point CurrentQuery at the packet that is about to be dispatched.
 *
 * CurrentQuery does not copy or own the buffer: begin() aliases the query text
 * inside pkt past the 5-byte PostgreSQL header. The buffer must therefore stay
 * allocated and unmodified for the whole dispatch, which means pkt has to be one
 * that some stream already owns (a writer stream's pgsql_real_query.pkt, or
 * polardb_txn_reader.original_pkt), not a temporary. The call also frees the
 * previous query parser state and restores the mode3 previous status.
 *
 * @param pkt  Simple-query packet to alias. Must have a valid pointer and size.
 */
void PgSQL_Session::polardb_restart_query_dispatch_from_packet(
		const PtrSize_t& pkt) {
	CurrentQuery.query_parser_free();
	CurrentQuery.begin(
		reinterpret_cast<unsigned char*>(pkt.ptr),
		pkt.size,
		true);
	set_previous_status_mode3();
}

/**
 * @brief Prepare an extended Execute for dispatch on a different backend.
 *
 * stmt_backend_id names a prepared statement on one backend connection. It
 * cannot follow an Execute to another reader or the primary. Clearing it makes
 * the normal extended-query state machine reuse that backend's existing mapping
 * or allocate a new statement number before Execute.
 */
void PgSQL_Session::polardb_prepare_extended_retry() {
	CurrentQuery.extended_query_info.stmt_backend_id = 0;
	set_previous_status_mode3(/*allow_execute=*/true);
}

/**
 * Move one request from an abandoned backend stream to its retry target.
 *
 * Retry limits belong to the request, not to either stream. The source stops
 * carrying request state, while the target receives the remaining limits and
 * starts without timeout or cancellation markers from an earlier request. A
 * target that still needs a connection receives a fresh connection deadline.
 */
void PgSQL_Session::polardb_prepare_retry_backend(
		PgSQL_Data_Stream* source_myds, PgSQL_Data_Stream* target_myds) {
#if POLARDB_DEBUG
	const bool injected_expired_deadlines =
		polardb_debug_consume_connect_deadline_fault("retry_expired");
	if (injected_expired_deadlines) {
		const uint64_t expired =
			thread && thread->curtime > 0 ? thread->curtime - 1 : 1;
		if (source_myds) {
			source_myds->max_connect_time = expired;
		}
		if (target_myds) {
			target_myds->max_connect_time = expired;
		}
	}
#endif // POLARDB_DEBUG
	const int query_retries = source_myds
		? source_myds->query_retries_on_failure : 0;
	const int connect_retries = source_myds
		? source_myds->connect_retries_on_failure : 0;

	if (source_myds && source_myds != target_myds) {
		source_myds->query_retries_on_failure = 0;
		source_myds->connect_retries_on_failure = 0;
		source_myds->max_connect_time = 0;
		source_myds->wait_until = 0;
		source_myds->killed_at = 0;
		source_myds->kill_type = 0;
		source_myds->cancel_query = false;
	}
	if (!target_myds) {
		return;
	}

	target_myds->query_retries_on_failure = query_retries;
	target_myds->connect_retries_on_failure = connect_retries;
	target_myds->wait_until = 0;
	target_myds->killed_at = 0;
	target_myds->kill_type = 0;
	target_myds->cancel_query = false;
	const bool target_ready =
		target_myds->myconn &&
		target_myds->myconn->async_state_machine == ASYNC_IDLE;
	target_myds->max_connect_time =
		!target_ready && pgsql_thread___connect_timeout_server_max > 0
			? thread->curtime +
				static_cast<uint64_t>(
					pgsql_thread___connect_timeout_server_max) * 1000ULL
			: 0;
#if POLARDB_DEBUG
	if (injected_expired_deadlines) {
		POLARDB_TRACE(
			"PolarDB CONNECT_DEADLINE DEBUG: retry repaired "
			"source_cleared=%d target_ready=%d target_deadline=%s\n",
			!source_myds || source_myds == target_myds ||
				source_myds->max_connect_time == 0,
			target_ready ? 1 : 0,
			target_myds->max_connect_time == 0 ? "clear" : "set");
	}
#endif // POLARDB_DEBUG
}

/**
 * @brief Hand the retry packet to the writer stream and dispatch from there.
 *
 * @param source_myds     Stream that owned the failed request state. May be null.
 * @param writer_backend  Backend to dispatch on. Must have a server stream.
 * @param writer_hg       Hostgroup to record as the new current hostgroup.
 * @param retry_pkt       Client packet to retry. Consumed only on success.
 * @return true when the packet was installed. Ownership of the buffer then
 *         belongs to writer_myds->pgsql_real_query, which frees it in end(), and
 *         retry_pkt is zeroed; current_hostgroup and mybe now point at the
 *         writer and CurrentQuery aliases the installed packet.
 *         false when an argument was missing. Nothing is consumed and the caller
 *         still owns retry_pkt and must free or re-install it. current_hostgroup
 *         and mybe are left untouched in that case, because the guards run
 *         before anything is changed.
 */
bool PgSQL_Session::polardb_move_retry_packet_to_writer(
		PgSQL_Data_Stream* source_myds, PgSQL_Backend* writer_backend,
		int writer_hg, PtrSize_t& retry_pkt, bool extended_query) {
	if (!writer_backend || !writer_backend->server_myds || !retry_pkt.ptr) {
		return false;
	}

	PgSQL_Data_Stream* writer_myds = writer_backend->server_myds;
	polardb_prepare_retry_backend(source_myds, writer_myds);
	current_hostgroup = writer_hg;
	mybe = writer_backend;
	writer_myds->free_pgsql_real_query();
	writer_myds->pgsql_real_query.take_packet(retry_pkt);

	// A simple-query packet owns the SQL text used by CurrentQuery, so rebind
	// the parser to its new owner. Extended protocol keeps its prepared-statement
	// metadata in CurrentQuery and only needs the state-machine return point.
	if (extended_query) {
		polardb_prepare_extended_retry();
	} else {
		polardb_restart_query_dispatch_from_packet(
			writer_myds->pgsql_real_query.pkt);
	}
	return true;
}

/**
 * Clear state owned by a completed reader request before releasing its
 * connection. Transaction-split readers do not use this helper because their
 * query view is borrowed and must be reset rather than freed.
 */
static PgSQL_Connection* polardb_clear_reader_request(
		PgSQL_Data_Stream& reader_myds) {
	reader_myds.max_connect_time = 0;
	reader_myds.free_pgsql_real_query();
	PgSQL_Connection* conn = reader_myds.myconn;
	if (!conn) {
		reader_myds.DSS = STATE_NOT_INITIALIZED;
		return nullptr;
	}
	conn->async_free_result();
	return conn;
}

void PgSQL_Session::polardb_return_or_destroy_backend_stream(
		PgSQL_Data_Stream* myds, bool return_to_pool) {
	if (!myds || !myds->myconn) {
		return;
	}
#if POLARDB_PROXY
#if POLARDB_DEBUG
	PgSQL_Connection* dbg_conn = myds->myconn;
	PgSQL_SrvC* dbg_srv = dbg_conn ? static_cast<PgSQL_SrvC*>(dbg_conn->parent) : nullptr;
	POLARDB_TRACE(
		"PolarDB FAILURE: cleanup backend stream myds=%p return_to_pool=%d "
		"conn=%p hg=%d host=%s:%u state=%d connected=%d active_trx=%d "
		"multiplex_disabled=%d "
		"pipeline=%d dss=%d fd=%d\n",
		(void*)myds, return_to_pool ? 1 : 0, (void*)dbg_conn,
		dbg_srv && dbg_srv->myhgc ? dbg_srv->myhgc->hid : -1,
		dbg_srv ? dbg_srv->address : "(null)", dbg_srv ? dbg_srv->port : 0,
		dbg_conn ? (int)dbg_conn->async_state_machine : -1,
		dbg_conn && dbg_conn->is_connected() ? 1 : 0,
		dbg_conn && dbg_conn->IsActiveTransaction() ? 1 : 0,
		dbg_conn && dbg_conn->MultiplexDisabled() ? 1 : 0,
		dbg_conn && dbg_conn->is_pipeline_active() ? 1 : 0,
		myds->DSS, myds->fd);
#endif // POLARDB_DEBUG
#endif // POLARDB_PROXY
	if (return_to_pool) {
		myds->return_MySQL_Connection_To_Pool();
	} else {
		myds->destroy_MySQL_Connection_From_Pool(false);
	}
}

void PgSQL_Session::polardb_forward_reader_error(
		const PolarDB_ReaderFailure& failure, char rfq) {
	PG_pkt pgpkt{};
	pgpkt.set_multi_pkt_mode(true);
	const PGSQL_ERROR_CODES code = failure.has_backend_error
		? failure.backend_error_code
		: PGSQL_ERROR_CODES::ERRCODE_CONNECTION_FAILURE;
	const char* message = failure.has_backend_error
		? failure.backend_error_message.c_str()
		: "PolarDB reader replica connection failed";
	POLARDB_TRACE(
		"PolarDB FAILURE: forwarding reader error code=%s rfq=%c message='%s'\n",
		PgSQL_Error_Helper::get_error_code(code), rfq, message);
	pgpkt.write_generic('E', "cscscscsc",
		'S', "ERROR", 'V', "ERROR",
		'C', PgSQL_Error_Helper::get_error_code(code),
		'M', message,
		0);
	pgpkt.write_ReadyForQuery(rfq);
	pgpkt.set_multi_pkt_mode(false);
	auto buff = pgpkt.detach();
	client_myds->PSarrayOUT->add((void*)buff.first, buff.second);
	client_myds->DSS = STATE_SLEEP;
}

void PgSQL_Session::polardb_record_reader_failure(
		const PolarDB_ReaderFailure& failure) {
	if (failure.reader_hg >= 0 && !failure.reader_address.empty()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: recording reader failure status hg=%d endpoint=%s:%d\n",
			failure.reader_hg, failure.reader_address.c_str(),
			failure.reader_port);
		PgHGM->p_update_pgsql_error_counter(
			p_pgsql_error_type::pgsql,
			failure.reader_hg,
			const_cast<char*>(failure.reader_address.c_str()),
			failure.reader_port,
			POLARDB_REPLICA_FAILURE_ERROR_CODE);
	}
	if (!failure.reusable && failure.reader_backend &&
			failure.reader_backend->server_myds &&
			failure.reader_backend->server_myds->myconn) {
		failure.reader_backend->server_myds->myconn->reusable = false;
	}
}

void PgSQL_Session::polardb_end_split_after_reader_failure() {
	POLARDB_TRACE(
		"PolarDB FAILURE: finishing split read failure; transaction stage -> primary\n");
	polardb_finish_txn_reader_read(
		/*split_success=*/false, /*split_error=*/true, "reader_failure",
		/*record_split_error_counter=*/true,
		/*mark_reader_not_reusable=*/false);
}

/**
 * @brief Give up a failed reader backend, returning it to the pool or destroying it.
 *
 * The staged libpq result and the query packet are dropped here, so the caller
 * does not have to clear them first. When reader_backend is the session's active
 * transaction-split reader the work is delegated to
 * polardb_release_txn_reader_backend(), which additionally tears down the split
 * request state and restores mybe to the primary backend - a session-wide effect
 * that reaches beyond this one stream.
 *
 * @param reader_backend  Backend to give up. A null pointer is a no-op.
 * @param want_reuse      Advisory. Pool return happens only when this is true
 *                        and polardb_failed_connection_is_poolable() also
 *                        holds; otherwise the connection is destroyed and the
 *                        stream is reset to fd 0 / STATE_NOT_INITIALIZED. The
 *                        caller must not touch the connection afterwards either
 *                        way.
 */
void PgSQL_Session::polardb_release_reader_backend(
		PgSQL_Backend* reader_backend, bool want_reuse) {
	if (!reader_backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: no reader backend to release\n");
		return;
	}
	if (reader_backend == polardb_txn_reader.backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: releasing split reader backend want_reuse=%d\n",
			want_reuse ? 1 : 0);
		polardb_release_txn_reader_backend(want_reuse);
		return;
	}
	PgSQL_Data_Stream* reader_myds = reader_backend->server_myds;
	if (!reader_myds) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader backend has no data stream\n");
		return;
	}
	PgSQL_Connection* conn = polardb_clear_reader_request(*reader_myds);
	if (!conn) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader stream has no connection; reset stream state\n");
		return;
	}
	const bool reusable =
		want_reuse && polardb_failed_connection_is_poolable(conn);
	if (reusable) {
		POLARDB_TRACE(
			"PolarDB FAILURE: returning reader backend to pool\n");
		conn->async_state_machine = ASYNC_IDLE;
		polardb_return_or_destroy_backend_stream(reader_myds, true);
	} else {
		POLARDB_TRACE(
			"PolarDB FAILURE: destroying reader backend after failure want_reuse=%d "
			"conn_reusable=%d connected=%d state_reusable=%d active_trx=%d "
			"multiplex_disabled=%d pipeline=%d\n",
			want_reuse ? 1 : 0,
			conn->reusable ? 1 : 0,
			conn->is_connected() ? 1 : 0,
			conn->is_connection_in_reusable_state() ? 1 : 0,
			conn->IsActiveTransaction() ? 1 : 0,
			conn->MultiplexDisabled() ? 1 : 0,
			conn->is_pipeline_active() ? 1 : 0);
		polardb_return_or_destroy_backend_stream(reader_myds, false);
	}
}

/**
 * @brief Release a reader connection rejected before any query was sent.
 *
 * This path is deliberately separate from polardb_release_reader_backend().
 * The latter evaluates a connection that failed a query and therefore requires
 * a backend error to be present. A newly acquired replacement has no error; it
 * can return to the pool when it is still connected, idle and otherwise safe
 * for multiplexing.
 */
void PgSQL_Session::polardb_release_unused_reader_backend(
		PgSQL_Backend* reader_backend) {
	if (!reader_backend || !reader_backend->server_myds) {
		return;
	}
	PgSQL_Data_Stream* reader_myds = reader_backend->server_myds;
	PgSQL_Connection* conn = polardb_clear_reader_request(*reader_myds);
	if (!conn) {
		return;
	}
	const bool reusable =
		conn->reusable &&
		conn->is_connected() &&
		!conn->is_error_present() &&
		conn->async_state_machine == ASYNC_IDLE &&
		conn->is_connection_in_reusable_state() &&
		!conn->IsActiveTransaction() &&
		!conn->MultiplexDisabled() &&
		!conn->is_pipeline_active();
	POLARDB_TRACE(
		"PolarDB FAILURE: %s unused reader backend after pre-dispatch rejection\n",
		reusable ? "returning" : "destroying");
	polardb_return_or_destroy_backend_stream(reader_myds, reusable);
}

/**
 * @brief Re-run the original client query on the writer that holds the transaction.
 *
 * Every precondition is checked first: a connected writer stream that is idle and
 * in a known active transaction, a retry packet still owned by the failure record
 * and no client rows emitted yet. Only once all of them pass is the failed reader
 * released, and that happens before the packet is moved, so a late failure of the
 * move leaves the reader already gone.
 *
 * @param failure         Failure record. Mutated: reader_backend is cleared once
 *                        the reader is released and retry_pkt is consumed on
 *                        success. failed_myds keeps the request state available
 *                        if a previous peer-reader attempt already released the
 *                        backend.
 * @param writer_hg       Hostgroup of the writer backend.
 * @param writer_backend  Writer backend to dispatch on. May be null.
 * @return true when the query was re-dispatched: the reader backend has been
 *         released, failure.reader_backend is null, failure.retry_pkt has been
 *         consumed by the writer stream and current_hostgroup/mybe point at the
 *         writer.
 *         false when the retry was declined. If it was declined by one of the
 *         guards nothing changed, but a failure after the guards still leaves the
 *         reader released and failure.reader_backend null while the caller keeps
 *         ownership of failure.retry_pkt. Re-check failure.reader_backend instead
 *         of assuming it survived.
 */
bool PgSQL_Session::polardb_try_redispatch_to_writer(
		PolarDB_ReaderFailure& failure, int writer_hg,
		PgSQL_Backend* writer_backend) {
	if (!writer_backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=no_writer_backend\n");
		return false;
	}
	if (!writer_backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=no_writer_stream\n");
		return false;
	}
	if (!failure.retry_pkt.ptr) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=no_retry_packet\n");
		return false;
	}
	if (failure.result_started) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=result_already_started\n");
		return false;
	}
	PgSQL_Data_Stream* writer_myds = writer_backend->server_myds;
	PgSQL_Connection* writer_conn = writer_myds->myconn;
	if (!writer_conn) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=no_writer_connection\n");
		return false;
	}
	if (polardb_debug_consume_split_failure_fault("writer_busy")) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=writer_busy debug=1\n");
		return false;
	}
	if (!writer_conn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=writer_not_connected\n");
		return false;
	}
	if (!writer_conn->IsKnownActiveTransaction()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=writer_not_in_transaction\n");
		return false;
	}
	if (writer_conn->async_state_machine != ASYNC_IDLE) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=writer_busy async_state=%d\n",
			(int)writer_conn->async_state_machine);
		return false;
	}
	if (writer_hg != failure.writer_scope.hg ||
			writer_backend->hostgroup_id != writer_hg ||
			!polardb_writer_scope_is_current(failure.writer_scope)) {
		POLARDB_TRACE(
			"PolarDB FAILURE: retry declined reason=writer_scope_changed "
			"expected_hg=%d candidate_hg=%d expected_epoch=%lu\n",
			failure.writer_scope.hg, writer_backend->hostgroup_id,
			(unsigned long)failure.writer_scope.epoch);
		return false;
	}

	PgSQL_Data_Stream* reader_myds = failure.failed_myds;
	if (failure.reader_backend) {
		polardb_release_reader_backend(
			failure.reader_backend, failure.reusable);
		failure.reader_backend = nullptr;
	}
	if (!polardb_move_retry_packet_to_writer(
			reader_myds, writer_backend, writer_hg, failure.retry_pkt)) {
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(thread, split_reads_retried);
	POLARDB_TRACE(
		"PolarDB FAILURE: retry split read on writer_hg=%d\n",
		writer_hg);
	return true;
}

/**
 * @brief Re-run the split read on a different replica of the same reader hostgroup.
 *
 * A dead split reader is often one bad server rather than a bad hostgroup, so the
 * read is given one more chance on a sibling replica before it falls back to the
 * writer. The replacement is taken only from already-pooled connections of the
 * same reader hostgroup with the failed address and port excluded, and a fresh
 * XID plus LSN wrapper is built for it because the previous wrapper is discarded
 * during failure cleanup. polardb_query.reader_retry_attempts is incremented on
 * success, which is what caps this at POLARDB_READER_RETRY_LIMIT per statement.
 * After acquisition and immediately before dispatch, the current reader
 * hostgroup mapping and writer epoch must still match failure.writer_scope. If
 * not, the replacement connection is returned cleanly and the saved wrapper is
 * never sent.
 *
 * @param failure  Split-read failure record, mutated in place. On success
 *                 retry_pkt ownership moves into polardb_begin_txn_split_read().
 * @return true when the read was re-dispatched: mybe, current_hostgroup and
 *         CurrentQuery point at the replacement reader and the split state has
 *         been rebuilt.
 *         false when no replacement could be used. Note that the failed reader is
 *         released before several of those checks run, so false does not mean
 *         nothing happened: failure.reader_backend may already be null and the
 *         old reader gone, while failure.retry_pkt is still owned by the caller
 *         and must be installed elsewhere or freed.
 */
bool PgSQL_Session::polardb_try_redispatch_to_other_reader(
		PolarDB_ReaderFailure& failure) {
	if (!failure.is_split()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=not_split_read\n");
		return false;
	}
	if (!failure.retry_pkt.ptr) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=no_retry_packet\n");
		return false;
	}
	if (failure.result_started) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=result_already_started\n");
		return false;
	}
	if (failure.reader_hg < 0 || failure.reader_address.empty() ||
			failure.reader_port < 0) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=missing_failed_reader "
			"reader_hg=%d address='%s' port=%d\n",
			failure.reader_hg, failure.reader_address.c_str(),
			failure.reader_port);
		return false;
	}
	if (!failure.wait_spec.has_wait() || failure.txn_xids.empty()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=missing_split_payload "
			"target_lsn=%lu xids_len=%zu\n",
			(unsigned long)failure.wait_spec.target,
			failure.txn_xids.size());
		return false;
	}

	std::string wrapped_query;
	const uint32_t wrapper_stmts = polardb_build_txn_split_wrapped_query(
			failure.retry_pkt, failure.wait_spec,
			failure.txn_xids, false, wrapped_query);
	if (wrapper_stmts == 0) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=wrapper_build_failed\n");
		return false;
	}

	PgSQL_Backend* old_reader_backend = failure.reader_backend;
	PgSQL_Data_Stream* old_reader_myds =
		old_reader_backend ? old_reader_backend->server_myds : nullptr;
	polardb_release_reader_backend(old_reader_backend, failure.reusable);
	failure.reader_backend = nullptr;

	PgSQL_Backend* retry_backend = find_or_create_backend(failure.reader_hg);
	if (!retry_backend || !retry_backend->server_myds) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=no_retry_backend "
			"reader_hg=%d\n",
			failure.reader_hg);
		return false;
	}
	PgSQL_Data_Stream* retry_myds = retry_backend->server_myds;
	if (retry_myds->myconn) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=retry_stream_busy "
			"reader_hg=%d\n",
			failure.reader_hg);
		return false;
	}

	PolarDB_ReaderResult reader_result =
		PgHGM->polardb_acquire_reader_connection(
			(unsigned int)failure.reader_hg, this,
			failure.reader_plan, failure.wait_spec, true,
			failure.reader_address.c_str(), failure.reader_port);
	if (!reader_result.acquired()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined status=%s "
			"reader_hg=%d excluded=%s:%d\n",
			polardb_reader_status_name(reader_result.status),
			failure.reader_hg, failure.reader_address.c_str(),
			failure.reader_port);
		return false;
	}

	retry_myds->attach_connection(reader_result.conn);
	if (!retry_myds->myconn || !retry_myds->myconn->is_connected()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=acquired_not_connected "
			"reader_hg=%d\n",
			failure.reader_hg);
		polardb_return_or_destroy_backend_stream(retry_myds, false);
		return false;
	}

	if (polardb_debug_writer_changed_after_reader_acquire() &&
			failure.writer_scope.valid()) {
		++failure.writer_scope.epoch;
		POLARDB_TRACE(
			"PolarDB FAILURE DEBUG: changed captured writer epoch after "
			"replacement reader acquisition\n");
	}
	if (retry_backend->hostgroup_id != failure.reader_hg ||
			!polardb_reader_matches_writer_scope(
				failure.reader_hg, failure.writer_scope)) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=reader_scope_changed "
			"reader_hg=%d candidate_hg=%d writer_hg=%d writer_epoch=%lu\n",
			failure.reader_hg, retry_backend->hostgroup_id,
			failure.writer_scope.hg,
			(unsigned long)failure.writer_scope.epoch);
		polardb_release_unused_reader_backend(retry_backend);
		return false;
	}

	polardb_prepare_retry_backend(old_reader_myds, retry_myds);
	current_hostgroup = failure.reader_hg;
	polardb_begin_txn_split_read(retry_backend, retry_myds, failure.retry_pkt,
		std::move(wrapped_query), failure.wait_spec, failure.reader_plan,
		failure.writer_scope, wrapper_stmts, false);
	polardb_query.reader_retry_attempts++;

	POLARDB_THREAD_COUNT_ONE(thread, split_reads_retried_on_reader);
	POLARDB_TRACE(
		"PolarDB FAILURE: retry split read on other_reader_hg=%d "
		"excluded=%s:%d target_lsn=%lu\n",
		failure.reader_hg, failure.reader_address.c_str(),
		failure.reader_port, (unsigned long)failure.wait_spec.target);
	polardb_restart_query_dispatch_from_packet(
		polardb_txn_reader.original_pkt);
	return true;
}

PolarDB_FailureAction PgSQL_Session::polardb_forward_error_and_keep_writer(
		PolarDB_ReaderFailure& failure) {
	polardb_forward_reader_error(failure, 'T');
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_forwarded);
	POLARDB_TRACE(
		"PolarDB FAILURE: forwarded reader error; transaction remains on writer_hg=%d\n",
		polardb_txn_reader_failure.writer_hg);
	polardb_release_reader_backend(
		failure.reader_backend, failure.reusable);
	polardb_free_retry_pkt_if_owned(failure);
	return PolarDB_FailureAction::FORWARD;
}

PolarDB_FailureAction
PgSQL_Session::polardb_terminate_session_after_reader_failure(
		PolarDB_ReaderFailure& failure) {
	POLARDB_THREAD_COUNT_ONE(thread, reader_terminations);
	POLARDB_TRACE(
		"PolarDB FAILURE: terminating session after reader failure\n");
	polardb_release_reader_backend(failure.reader_backend, false);
	polardb_free_retry_pkt_if_owned(failure);
	return PolarDB_FailureAction::TERMINATE;
}

/**
 * @brief Consume one debug-only wait-retry fault instruction.
 *
 * Tests can either set a one-shot environment variable before ProxySQL starts,
 * or set POLARDB_DEBUG_WAIT_RETRY_FAULT_FILE to a writable file and write one of
 * the supported fault names into it before the query. The file path lets a TAP
 * choose the exact retry attempt to perturb without restarting ProxySQL.
 */
static bool polardb_debug_consume_wait_retry_fault(const char* env_name) {
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
	if (polardb_debug_read_fault_file(
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

static bool polardb_failed_connection_is_poolable(PgSQL_Connection* conn) {
	return conn &&
		conn->reusable == true &&
		polardb_failed_connection_is_reusable(conn) &&
		conn->IsActiveTransaction() == false &&
		conn->MultiplexDisabled() == false &&
		conn->is_pipeline_active() == false;
}

/**
 * @brief Release the stream of a failed plain wait-wrapped replica read.
 *
 * Every terminal outcome of a plain, non-transaction wait-wrapped read goes
 * through here: a retry on the primary, forwarding the reader error, and
 * terminating the session. An in-transaction wait read is released through
 * polardb_release_txn_wait_read() instead. The staged libpq result and the failed
 * query packet are discarded first so neither can follow the connection back into
 * the pool.
 *
 * @param failed_myds        Stream of the failed reader. A null pointer is a
 *                           no-op.
 * @param can_return_to_pool Chosen by the caller. true returns the connection to
 *                           the pool as idle; false destroys it and resets the
 *                           stream to fd 0 / STATE_NOT_INITIALIZED. Either way the
 *                           caller must not use the connection afterwards.
 */
void PgSQL_Session::polardb_release_reader_stream(PgSQL_Data_Stream* failed_myds,
		bool can_return_to_pool) {
	if (!failed_myds) {
		return;
	}

	PgSQL_Connection* failed_conn =
		polardb_clear_reader_request(*failed_myds);
	if (!failed_conn) {
		return;
	}

	if (can_return_to_pool) {
		failed_conn->async_state_machine = ASYNC_IDLE;
		polardb_return_or_destroy_backend_stream(failed_myds, true);
	} else {
		polardb_return_or_destroy_backend_stream(failed_myds, false);
	}
}

/**
 * @brief Build one PostgreSQL simple-query packet for the given SQL text.
 *
 * @param sql  Query text, without the wire header and without a trailing NUL.
 * @param out  Receives the packet. The buffer comes from l_alloc and belongs to
 *             the caller: release it with l_free(out.size, out.ptr), or transfer
 *             ownership by installing it into a PgSQL_MyDS_real_query, which then
 *             frees it in end(). out is left as {NULL, 0} when sql is empty, when
 *             it is larger than the maximum packet size, or when the allocation
 *             fails, so
 *             the caller must test out.ptr before using it.
 */
void PgSQL_Session::polardb_build_simple_query_packet(const std::string& sql,
		PtrSize_t& out) {
	out = {};
	if (sql.empty() ||
			sql.size() > static_cast<size_t>(
				UINT32_MAX - PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD)) {
		return;
	}

	const unsigned int size = static_cast<unsigned int>(
		sql.size() + PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD);
	out.ptr = l_alloc(size);
	if (!out.ptr) {
		return;
	}
	out.size = size;

	char* packet = static_cast<char*>(out.ptr);
	packet[0] = 'Q';
	const uint32_t packet_len = htonl(static_cast<uint32_t>(
		sql.size() + PGSQL_SIMPLE_QUERY_MESSAGE_OVERHEAD - 1));
	memcpy(packet + 1, &packet_len, sizeof(packet_len));
	memcpy(
		packet + PGSQL_V3_MESSAGE_HEADER_SIZE,
		sql.data(), sql.size());
	packet[size - 1] = '\0';
}

/**
 * @brief Take the original client packet needed to retry a reader read.
 *
 * Wait wrappers cannot be replayed, so their saved user SQL is rebuilt as one
 * simple-query packet. An ordinary read has no wrapper and its original packet
 * can be moved from the failed stream without copying.
 */
bool PgSQL_Session::polardb_prepare_reader_retry_packet(
		PolarDB_ReaderFailure& failure) {
	if (failure.retry_pkt.ptr && failure.retry_pkt.size > 0) {
		return true;
	}
	if (failure.is_wait()) {
		polardb_build_simple_query_packet(
			failure.retry_query, failure.retry_pkt);
		return failure.retry_pkt.ptr && failure.retry_pkt.size > 0;
	}
	if (!failure.is_ordinary() || !failure.failed_myds ||
			!failure.failed_myds->pgsql_real_query.pkt.ptr) {
		return false;
	}
	failure.retry_pkt =
		failure.failed_myds->pgsql_real_query.release_packet();
	return failure.retry_pkt.ptr && failure.retry_pkt.size > 0;
}

/**
 * Retry an ordinary or wait-wrapped read on another already-pooled reader.
 *
 * The failed endpoint is excluded. A wait read restores its captured wait so
 * ASYNC_IDLE finalization builds a fresh wrapper. An ordinary read reuses the
 * untouched client packet, including extended-protocol packets.
 */
bool PgSQL_Session::polardb_try_redispatch_reader_read_to_other_reader(
		PolarDB_ReaderFailure& failure) {
	if ((!failure.is_wait() && !failure.is_ordinary()) ||
			failure.is_split() ||
			polardb_txn_reader.wait_read_active ||
			failure.wait_was_bypassed || failure.result_started ||
			failure.reader_hg < 0 || failure.reader_address.empty() ||
			failure.reader_port < 0 ||
			(failure.is_wait() &&
				(failure.retry_query.empty() ||
				 !failure.wait_spec.has_wait()))) {
		return false;
	}

	if (!polardb_prepare_reader_retry_packet(failure)) {
		return false;
	}

	PgSQL_Data_Stream* failed_myds = failure.failed_myds;
	polardb_release_reader_stream(
		failed_myds, failure.can_return_to_pool);
	failure.failed_myds = nullptr;
	PgSQL_Backend* retry_backend = find_or_create_backend(failure.reader_hg);
	if (!retry_backend || !retry_backend->server_myds) {
		return false;
	}

	PgSQL_Data_Stream* retry_myds = retry_backend->server_myds;
	if (retry_myds->myconn) {
		POLARDB_TRACE(
			"PolarDB FAILURE: another-reader retry unavailable "
			"reason=retry_stream_busy reader_hg=%d\n",
			failure.reader_hg);
		return false;
	}
	PolarDB_WaitSpec no_wait;
	const PolarDB_WaitSpec& retry_wait =
		failure.is_wait() ? failure.wait_spec : no_wait;
	PolarDB_ReaderResult reader_result =
		PgHGM->polardb_acquire_reader_connection(
			static_cast<unsigned int>(failure.reader_hg), this,
			failure.reader_plan, retry_wait,
			/*only_pooled=*/true,
			failure.reader_address.c_str(), failure.reader_port);
	if (!reader_result.acquired()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: another-reader retry unavailable status=%s "
			"reader_hg=%d excluded=%s:%d\n",
			polardb_reader_status_name(reader_result.status),
			failure.reader_hg, failure.reader_address.c_str(),
			failure.reader_port);
		return false;
	}

	retry_myds->attach_connection(reader_result.conn);
	if (!retry_myds->myconn || !retry_myds->myconn->is_connected()) {
		polardb_return_or_destroy_backend_stream(retry_myds, false);
		return false;
	}
	retry_myds->assign_fd_from_pgsql_conn();
	retry_myds->myds_type = MYDS_BACKEND;
	retry_myds->DSS = STATE_READY;

	retry_myds->free_pgsql_real_query();
	retry_myds->pgsql_real_query.take_packet(failure.retry_pkt);
	polardb_prepare_retry_backend(failed_myds, retry_myds);
	current_hostgroup = failure.reader_hg;
	mybe = retry_backend;
	polardb_query.reader_plan = failure.reader_plan;
	if (failure.is_wait()) {
		const bool wait_activated =
			polardb_finish_reader_wait_selection(
				failure.wait_spec,
				reader_result.wait_bypass_allowed,
				failure.fallback_writer_hg);
		if (wait_activated &&
				polardb_query.original_query.empty()) {
			polardb_query.original_query = failure.retry_query;
		}
	} else {
		polardb_query.reset_wait();
	}
	polardb_query.reader_retry_attempts++;
	if (failure.extended_query) {
		polardb_prepare_extended_retry();
	} else {
		polardb_restart_query_dispatch_from_packet(
			retry_myds->pgsql_real_query.pkt);
	}
	if (failure.is_wait()) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_reads_retried_on_reader,
			"reader_succeeded");
	}
	POLARDB_TRACE(
		"PolarDB FAILURE: reader failed before user result; retrying "
		"original query on another reader_hg=%d excluded=%s:%d wait=%d\n",
		failure.reader_hg, failure.reader_address.c_str(),
		failure.reader_port, failure.is_wait() ? 1 : 0);
	return true;
}

/**
 * @brief Add wait-request state to a captured backend failure.
 *
 * WAIT requires an active consistency wrapper, a finalized wrapper, and the
 * saved original query. Query dispatch stores the wrapper type on the connection.
 *
 * This function never marks the request as SPLIT and never takes its packet. A
 * caller that recovers a transaction wait reader still adds its request-specific
 * retry text and fallback state.
 *
 * @param failure  Backend fields captured before cleanup starts.
 * @return The captured backend fields plus the request's wait and retry state.
 */
PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_capture_wait_read_failure(
		PolarDB_ReaderFailure failure) {
	failure.fallback_writer_hg = polardb_query.wait.fallback_writer_hg;
	failure.reader_plan = polardb_query.reader_plan;
	failure.wait_spec = polardb_query.wait.spec;
	// Preserve the original query for a possible writer retry.
	failure.retry_query = polardb_query.original_query;

	if (polardb_debug_consume_wait_retry_fault(
			"POLARDB_DEBUG_WAIT_RETRY_FALLBACK_UNKNOWN_ONCE")) {
		failure.fallback_writer_hg = -1;
	}

	if (polardb_debug_consume_wait_retry_fault(
			"POLARDB_DEBUG_WAIT_RETRY_RESULT_STARTED_ONCE")) {
		failure.result_started = true;
	}
	// The wrapper kind belongs to the backend after query dispatch.
	if (polardb_wait_active() &&
			failure.wrapper_is_consistency_wait &&
			polardb_query.wait.wrapper_finalized &&
			!failure.retry_query.empty()) {
		failure.request_kind = PolarDB_ReaderRequestKind::WAIT;
	}

	return failure;
}

/**
 * @brief Capture an automatically routed replica read with no active wrapper.
 *
 * A non-empty fallback writer in the per-query reader plan proves that the
 * planner selected this replica. Manual hostgroup routing and primary execution
 * do not carry that plan and therefore remain in ProxySQL's normal error path.
 */
PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_capture_ordinary_reader_failure(
		PolarDB_ReaderFailure failure) {
	const PolarDB_Query_ReaderPlan& plan = polardb_query.reader_plan;
	const bool query_status =
		status == PROCESSING_QUERY ||
		status == PROCESSING_STMT_PREPARE ||
		status == PROCESSING_STMT_DESCRIBE ||
		status == PROCESSING_STMT_EXECUTE;
	const bool automatic_replica =
		query_status &&
		!polardb_txn_reader.wait_read_active &&
		plan.fallback_writer_hg >= 0 &&
		plan.read_target == static_cast<int>(PolarDB_ReadTarget::REPLICA) &&
		plan.consistency_mode != PolarDB_ConsistencyMode::OFF &&
		failure.reader_hg >= 0 &&
		failure.reader_hg != plan.fallback_writer_hg;
	if (!automatic_replica) {
		return failure;
	}

	failure.request_kind = PolarDB_ReaderRequestKind::ORDINARY;
	failure.extended_query = status != PROCESSING_QUERY;
	failure.wait_was_bypassed =
		polardb_query.wait_bypass_target != 0;
	failure.reader_plan = plan;
	failure.fallback_writer_hg = plan.fallback_writer_hg;
	POLARDB_TRACE(
		"PolarDB FAILURE: captured ordinary replica read reader_hg=%d "
		"writer_hg=%d extended=%d wait_bypassed=%d reusable=%d\n",
		failure.reader_hg, failure.fallback_writer_hg,
		failure.extended_query ? 1 : 0,
		failure.wait_was_bypassed ? 1 : 0,
		failure.reusable ? 1 : 0);
	return failure;
}

/**
 * @brief Apply the failure policy to an ordinary or wait-wrapped replica read.
 *
 * A finalized wait read has already replaced pgsql_real_query with the
 * prepended-SET wrapper and moved the SET-result countdown onto the connection.
 * Any later error path that resent that packet would repeat internal SET
 * statements after their result counter was consumed, so the wrapper is removed
 * and the wait, dispatch-wrapper and pending-notice state is reset before any
 * policy branch runs. The mutable failure record also tracks packet ownership
 * while a retry moves the original request between backend streams.
 *
 * Where the error is delivered depends on which reader failed. For a plain
 * autocommit wait read, FORWARD and the declined-retry paths return PASSTHROUGH
 * so ProxySQL's generic error path emits the reader error. For a transaction
 * wait read the reader must be released while its identity is still known, so
 * those same paths finish the read here instead and return FORWARD, or TERMINATE
 * when client rows had already started.
 *
 * @param failure   Captured ordinary or wait-wrapped replica failure. Returns
 *                  PASSTHROUGH when neither request kind applies.
 * @param decision  Policy chosen by polardb_reader_failure_decision().
 * @return RETRY when the original unwrapped query was moved to the writer stream,
 *         FORWARD when the reader error was written to the client, TERMINATE when
 *         the session must be closed, PASSTHROUGH when the generic rc==-1 path
 *         must deliver the error.
 */
PolarDB_FailureAction PgSQL_Session::polardb_handle_failed_reader_read(
		PolarDB_ReaderFailure& failure,
		const PolarDB_ReaderFailureDecision& decision) {
	if (!failure.is_wait() && !failure.is_ordinary()) {
		return PolarDB_FailureAction::PASSTHROUGH;
	}
	auto count_wait_retry = [&](std::atomic<unsigned long long>& counter,
			const char* name) {
		if (failure.is_wait()) {
			polardb_count_wait_retry_counter(counter, name);
		}
	};
	count_wait_retry(
		PgHGM->status.polardb_wait_retry_evaluated, "evaluated");

	// A finalized wait-read has already replaced pgsql_real_query with the
	// prepended-SET wrapper and moved the SET-result countdown to the connection.
	// No wrapped packet may be left behind for a later retry path; otherwise that
	// path could resend internal SET statements after the connection's SET-result
	// counter was consumed. An ordinary read keeps its original client packet
	// until a retry actually needs to move it.
	if (failure.failed_myds) {
		failure.failed_myds->query_retries_on_failure = 0;
		if (failure.is_wait()) {
			failure.failed_myds->free_pgsql_real_query();
		}
	}
	if (failure.is_wait()) {
		if (polardb_query.wait.wait_started_at_us != 0) {
			polardb_finish_wait(nullptr);
		}
		polardb_query.reset_wait();
		polardb_query.reset_dispatch_wrapper();
		polardb_query.wrapped_query_buf.clear();
		discard_pending_notices();
	}

	const bool txn_wait_read =
		polardb_txn_reader.wait_read_active &&
		failure.failed_myds &&
		polardb_txn_reader.backend &&
		polardb_txn_reader.backend->server_myds == failure.failed_myds;
	PgSQL_Data_Stream* request_myds = failure.failed_myds;
	auto finish_retry_packet = [&](bool request_end_follows) {
		if (request_end_follows && request_myds &&
				polardb_packet_contains_pointer(
					failure.retry_pkt, CurrentQuery.QueryPointer)) {
			// RequestEnd logs CurrentQuery before it clears the backend packet.
			// Return ownership to that stream so any query alias inside a
			// simple-query or Parse packet stays valid until logging and query
			// accounting are complete.
			request_myds->free_pgsql_real_query();
			request_myds->pgsql_real_query.take_packet(failure.retry_pkt);
			return;
		}
		polardb_free_retry_pkt_if_owned(failure);
	};
	auto preserve_ordinary_request_packet = [&]() {
		if (!failure.is_ordinary() || failure.retry_pkt.ptr ||
				!failure.failed_myds ||
				!failure.failed_myds->pgsql_real_query.pkt.ptr) {
			return;
		}
		// CurrentQuery borrows the SQL text inside this packet. Keep the
		// packet alive across reader release even when policy returns an error
		// without attempting a retry.
		failure.retry_pkt =
			failure.failed_myds->pgsql_real_query.release_packet();
	};
	auto release_failed_reader = [&](bool want_reuse) {
		if (!failure.failed_myds) {
			return;
		}
		if (txn_wait_read) {
			polardb_release_txn_wait_read(want_reuse);
		} else {
			preserve_ordinary_request_packet();
			polardb_release_reader_stream(failure.failed_myds, want_reuse);
		}
		failure.failed_myds = nullptr;
	};
	auto finish_txn_wait_with_error = [&]() {
		release_failed_reader(false);
		finish_retry_packet(!failure.result_started);
		if (failure.result_started) {
			POLARDB_TRACE(
				"PolarDB WAIT: transaction wait-read failed after rows started; "
				"terminating session\n");
			return PolarDB_FailureAction::TERMINATE;
		}
		polardb_forward_reader_error(failure, 'T');
		return PolarDB_FailureAction::FORWARD;
	};
	auto finish_ordinary_with_error = [&]() {
		if (failure.result_started) {
			release_failed_reader(false);
			finish_retry_packet(false);
			return PolarDB_FailureAction::TERMINATE;
		}
		// A completed PostgreSQL ErrorResponse is already staged in query_result.
		// Leave it to the normal wire path while the failed stream still owns it.
		if (failure.reusable && failure.failed_myds) {
			return PolarDB_FailureAction::PASSTHROUGH;
		}
		release_failed_reader(false);
		finish_retry_packet(true);
		polardb_forward_reader_error(failure, 'I');
		return PolarDB_FailureAction::FORWARD;
	};
	auto finish_with_error = [&]() {
		return txn_wait_read
			? finish_txn_wait_with_error()
			: finish_ordinary_with_error();
	};

	POLARDB_TRACE(
		"PolarDB FAILURE: policy kind=%s action=%s target=%s wait=%d "
		"ordinary=%d timeout=%d reusable=%d result_started=%d\n",
		polardb_reader_failure_kind_name(decision.kind),
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		failure.is_wait() ? 1 : 0,
		failure.is_ordinary() ? 1 : 0,
		failure.timeout ? 1 : 0,
		failure.reusable ? 1 : 0,
		failure.result_started ? 1 : 0);
	if (decision.action == PolarDB_ReaderAction::DISCONNECT_CLIENT) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_policy_terminate,
			"policy_terminate");
		POLARDB_THREAD_COUNT_ONE(thread, reader_terminations);
		POLARDB_TRACE(
			"PolarDB FAILURE: terminating session after reader failure\n");
		release_failed_reader(false);
		polardb_free_retry_pkt_if_owned(failure);
		return PolarDB_FailureAction::TERMINATE;
	}
	if (decision.action == PolarDB_ReaderAction::RETURN_ERROR) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_policy_forward,
			"policy_forward");
		POLARDB_TRACE(
			"PolarDB FAILURE: policy action=error\n");
		return finish_with_error();
	}
	if (decision.retry_target != PolarDB_RetryTarget::WRITER) {
		if (!txn_wait_read &&
				polardb_try_redispatch_reader_read_to_other_reader(failure)) {
			return PolarDB_FailureAction::RETRY;
		}
		if (!decision.allow_writer_retry) {
			return finish_with_error();
		}
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_target_not_writer,
			"target_not_writer");
		POLARDB_TRACE(
			"PolarDB FAILURE: retry target=%s unavailable; "
			"falling back to primary\n",
			polardb_retry_target_name(decision.retry_target));
	}

	if (failure.result_started || failure.fallback_writer_hg < 0) {
		if (failure.result_started) {
			count_wait_retry(
				PgHGM->status.polardb_wait_retry_declined_result_started,
				"result_started");
		} else {
			count_wait_retry(
				PgHGM->status.polardb_wait_retry_declined_writer_hg_unknown,
				"writer_hg_unknown");
		}
		POLARDB_TRACE(
			"PolarDB FAILURE: primary retry unavailable "
			"result_started=%d writer_hg=%d\n",
			failure.result_started ? 1 : 0,
			failure.fallback_writer_hg);
		return finish_with_error();
	}

	PgSQL_Backend* writer_mybe = find_or_create_backend(failure.fallback_writer_hg);
	if (!writer_mybe || !writer_mybe->server_myds) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_writer_unavailable,
			"writer_unavailable");
		POLARDB_TRACE(
			"PolarDB FAILURE: "
			"primary retry stream unavailable writer_hg=%d\n",
			failure.fallback_writer_hg);
		return finish_with_error();
	}
	if (writer_mybe->server_myds == failure.failed_myds) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_same_stream,
			"same_stream");
		POLARDB_TRACE(
			"PolarDB FAILURE: "
			"primary retry stream unavailable writer_hg=%d\n",
			failure.fallback_writer_hg);
		return finish_with_error();
	}
	PgSQL_Data_Stream* writer_myds = writer_mybe->server_myds;
	// Use a distinct writer stream. Reusing the failed reader stream would mix
	// packet and connection state from the failed dispatch with the retry.
	// Debug builds can force this branch after wrapper cleanup to show that the
	// normal error path returns a clean client error without leaking wrapper SETs.
	const bool writer_retry_declined_by_debug =
		polardb_debug_consume_wait_retry_fault(
			"POLARDB_DEBUG_WAIT_RETRY_WRITER_BUSY_ONCE");
	if ((writer_myds->myconn &&
				writer_myds->myconn->async_state_machine != ASYNC_IDLE) ||
			writer_retry_declined_by_debug) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_writer_busy,
			"writer_busy");
		POLARDB_TRACE(
			"PolarDB FAILURE: "
			"primary retry declined writer_hg=%d debug=%d\n",
			failure.fallback_writer_hg,
			writer_retry_declined_by_debug ? 1 : 0);
		return finish_with_error();
	}

	if (!polardb_prepare_reader_retry_packet(failure)) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_packet_build_failed,
			"packet_build_failed");
		return finish_with_error();
	}

	POLARDB_TRACE(
		"PolarDB FAILURE: %s before user result; redirecting original "
		"query to primary_hg=%d reader_hg=%d\n",
		failure.timeout ? "LSN wait timeout" : "reader connection lost",
		failure.fallback_writer_hg, failure.reader_hg);
	count_wait_retry(
		PgHGM->status.polardb_wait_retry_attempted, "attempted");

	if (failure.reader_hg >= 0 && !failure.reader_address.empty()) {
		PgHGM->p_update_pgsql_error_counter(
			p_pgsql_error_type::pgsql,
			failure.reader_hg,
			const_cast<char*>(failure.reader_address.c_str()),
			failure.reader_port,
			POLARDB_REPLICA_FAILURE_ERROR_CODE);
	}

	release_failed_reader(failure.can_return_to_pool);
	polardb_query.clear_reader_route();

	if (!polardb_move_retry_packet_to_writer(
			request_myds, writer_mybe, failure.fallback_writer_hg,
			failure.retry_pkt,
			failure.extended_query)) {
		count_wait_retry(
			PgHGM->status.polardb_wait_retry_declined_move_failed,
			"move_failed");
		return finish_with_error();
	}
	if (failure.is_wait()) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_reads_retried_on_writer,
			"succeeded");
	} else {
		POLARDB_THREAD_COUNT_ONE(thread, consistency_writer_fallback);
	}
	return PolarDB_FailureAction::RETRY;
}

// Send only this one query to the writer hostgroup. Used when a consistent
// reader cannot be obtained (no target-reaching reader can be acquired, or the
// requested guarantee cannot be enforced). The writer/primary already holds the latest
// WAL, so any required LSN is satisfied there and the LSN wait intent is
// dropped: clear_reader_route() makes sure no stale consistency-target LSN or
// completed wait target is carried onto the writer connection. Return false when
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
	polardb_query.clear_reader_route();
	current_hostgroup = writer_hg;
	mybe = find_or_create_backend(current_hostgroup);
	PgSQL_Data_Stream* target_myds = mybe ? mybe->server_myds : nullptr;
	polardb_prepare_retry_backend(source_myds, target_myds);
	if (source_myds && target_myds && target_myds != source_myds &&
			source_myds->pgsql_real_query.QueryPtr) {
		// The simple-query packet was attached to the reader backend before
		// acquisition. A one-query writer redirect changes backend streams, so
		// transfer that packet ownership to the writer stream before RunQuery().
		target_myds->free_pgsql_real_query();
		target_myds->pgsql_real_query.move_from(
			source_myds->pgsql_real_query);
	}
	return true;
}

/**
 * @brief Downgrade a connection retry that followed a bypassed LSN wait.
 *
 * When the wait wrapper was skipped because the chosen reader had already reached
 * the target, a retry on a fresh connection has no wrapper left to guarantee that
 * target, so it must not go to an unchecked replica. A captured replica-loss
 * action ending in primary sends the retry there; an action ending in error or
 * disconnect declines the retry and leaves the caller's normal failure path in
 * control.
 *
 * @param retry_conn  Retry decision made by the caller. Returned unchanged when
 *                    it is false or when no wait was bypassed.
 * @param reason      Trace text for the redirect. May be null.
 * @return The retry decision after applying policy. false keeps the failure on
 *         the client error/close path. true means the primary redirect is done:
 *         the reader target and wait state are reset, current_hostgroup and mybe
 *         point at the primary, and the pending query packet has been moved.
 */
bool PgSQL_Session::polardb_redirect_wait_retry_to_writer(
		bool retry_conn, const char* reason) {
	if (!retry_conn || polardb_query.wait_bypass_target == 0) {
		return retry_conn;
	}
	const PolarDB_ReplicaLossAction action =
		polardb_replica_loss_action_from_int(
			polardb_query.reader_plan.replica_loss_action);
	if (action == PolarDB_ReplicaLossAction::PRIMARY ||
			action == PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY) {
		return polardb_redirect_to_writer(
			polardb_query.reader_plan.fallback_writer_hg, reason);
	}
	POLARDB_TRACE(
		"PolarDB consistency: %s after a bypassed reader wait; "
		"primary fallback is not permitted\n",
		reason ? reason : "reader retry failed");
	return false;
}

#endif // POLARDB_PROXY
