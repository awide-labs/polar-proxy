/**
 * @file PgSQL_PolarDB_Failure.cpp
 * @brief Handle PolarDB reader failures.
 *
 * This file owns two reader-failure paths through one failure view:
 *   - transaction-split replica failures, handled by polardb_on_failure() before
 *     ProxySQL's generic rc==-1 retry/error logic; and
 *   - autocommit wait-wrapped read cleanup/retry, whose wrapper-specific packet
 *     cleanup stays in a small adapter below.
 *
 * The split path has three terminal actions. RETRY re-dispatches the original
 * client query on the live writer backend, FORWARD emits the captured reader
 * error with ReadyForQuery('T') and pins the transaction to the writer, and
 * TERMINATE closes the client session if no safe writer state remains.
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

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY
static constexpr uint8_t POLARDB_READER_RETRY_BUDGET = 1;

static std::string polardb_snapshot_error_message(const std::string& message) {
	constexpr size_t kMaxSnapshotBytes = 512;
	if (message.size() <= kMaxSnapshotBytes) {
		return message;
	}
	return message.substr(0, kMaxSnapshotBytes);
}

static void polardb_count_wait_retry_counter(
		std::atomic<unsigned long long>& counter,
		const char* name) {
	counter.fetch_add(1, std::memory_order_relaxed);
	POLARDB_TRACE("PolarDB WAIT: retry counter=%s\n", name ? name : "");
}

static bool polardb_error_text_is_wait_timeout(const std::string& message) {
	return message.find("LSN wait timeout") != std::string::npos ||
		message.find("polar_wait_lsn timeout") != std::string::npos;
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
 * These faults are DEBUG-only and file-driven so tests can arm one exact
 * split-failure branch after ProxySQL has started. The helper clears the file
 * only when the requested fault name matches, preserving the shared
 * match-limited semantics used by the other PolarDB debug fault files.
 */
static bool polardb_debug_split_failure_fault_is(const char* fault_name) {
#if POLARDB_PROXY && POLARDB_DEBUG
	static std::atomic<int> death_twice_remaining{0};
	char buf[64] = {0};
	if (!fault_name ||
			!polardb_debug_consume_fault_file(
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
		// Drive the reader-retry budget path: first death can retry another
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

PolarDB_RequestOutcome PgSQL_Session::polardb_capture_outcome(
		PgSQL_Backend* backend, bool ok) {
	PolarDB_RequestOutcome outcome;
	outcome.ok = ok;
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
	outcome.reusable = outcome.connected &&
		conn->is_connection_in_reusable_state();
	outcome.result_started = conn->query_result &&
		conn->query_result->is_transfer_started();
	outcome.timeout_error = polardb_query.wait.timeout_error ||
		polardb_error_text_is_wait_timeout(conn->get_error_message());
	outcome.wrapper_set_failure =
		conn->polardb_query_wrap_state.wrapper_set_failed() ||
		conn->polardb_query_wrap_state.consuming_wrapper_set();
	outcome.backend_error_code = static_cast<int>(conn->get_error_code());
	outcome.error_message = polardb_snapshot_error_message(conn->get_error_message());

	if (conn->parent) {
		outcome.backend_hg = conn->parent->myhgc ?
			(int)conn->parent->myhgc->hid : outcome.backend_hg;
		outcome.backend_address = conn->parent->address ?
			conn->parent->address : "";
		outcome.backend_port = (int)conn->parent->port;
	}

	return outcome;
}

PolarDB_FailureAction PgSQL_Session::polardb_on_failure(
		const PolarDB_RequestOutcome& outcome) {
	PolarDB_ReaderFailure failure = polardb_reader_failure_view(outcome);
	if (!failure.split_read) {
		// Session-consistency wait reads are not transaction-split reads, but
		// they still belong to the same PolarDB reader-failure stage. Keep their
		// cleanup/retry here so PgSQL_Session.cpp has one PolarDB failure handler
		// before falling through to ProxySQL's generic rc==-1 handling.
		failure = polardb_capture_wait_read_failure(outcome.backend_myds);
		POLARDB_TRACE(
			"PolarDB WAIT: rc=-1 wait_active=%d wait_read=%d wrapper_set_failure=%d "
			"timeout_error=%d connection_lost=%d result_started=%d "
			"wrapper_finalized=%d original_query_saved=%d can_return_to_pool=%d "
			"retry_writer_hg=%d\n",
			polardb_wait_active() ? 1 : 0,
			failure.wait_read ? 1 : 0,
			failure.wrapper_set_failure ? 1 : 0,
			failure.timeout ? 1 : 0,
			failure.reusable ? 0 : 1,
			failure.result_started ? 1 : 0,
			polardb_query.wait.wrapper_finalized ? 1 : 0,
			failure.retry_query.empty() ? 0 : 1,
			failure.can_return_to_pool ? 1 : 0,
			failure.fallback_writer_hg);
		if (failure.wait_read && !failure.reusable) {
			POLARDB_THREAD_COUNT_ONE(thread, wait_error_connection_lost);
		}
		if (failure.wait_read) {
			// Same policy selector as transaction split. The wait-read adapter
			// below owns the different packet cleanup and retry mechanics.
			const PolarDB_ReaderFailureDecision decision =
				polardb_reader_decision_for(failure);
			return polardb_handle_failed_wait_read(failure, decision);
		}

		// If this was not a retryable wait-read but still hit wrapper/timeout/
		// reader-loss state, clear request-local wait data before generic
		// handling. Ordinary SQL errors keep the normal ProxySQL path.
		if (polardb_wait_active() &&
				(failure.wrapper_set_failure ||
				 failure.timeout ||
				 !failure.reusable) &&
				polardb_query.wait.wait_started_at_us != 0) {
			record_wait_latency(polardb_query.wait);
			polardb_query.reset_wait();
			clear_pending_notices(/*free_buffers=*/true);
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	if (polardb_debug_split_failure_fault_is("death")) {
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
	if (polardb_debug_split_failure_fault_is("sql_error")) {
		failure.connected = true;
		failure.reusable = true;
		failure.timeout = false;
		failure.has_backend_error = true;
		failure.backend_error_code =
			PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION;
		failure.backend_error_message =
			"PolarDB DEBUG split reader SQL error";
	}
	if (polardb_debug_split_failure_fault_is("result_started")) {
		failure.result_started = true;
	}
	if (polardb_debug_split_failure_fault_is("no_retry_packet")) {
		polardb_free_retry_pkt_if_owned(failure);
	}

	POLARDB_TRACE(
		"PolarDB FAILURE: split reader_hg=%d connected=%d reusable=%d "
		"timeout=%d timeout_accounted=%d result_started=%d error='%s'\n",
		failure.reader_hg, failure.connected ? 1 : 0,
		failure.reusable ? 1 : 0, failure.timeout ? 1 : 0,
		failure.timeout_already_accounted ? 1 : 0,
		failure.result_started ? 1 : 0,
		failure.backend_error_message.c_str());

	POLARDB_THREAD_COUNT_ONE(thread, split_reads_error);
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
	if (polardb_debug_split_failure_fault_is("writer_lost")) {
		writer_hg = -1;
		writer_backend = nullptr;
		writer_state = PolarDB_WriterState::LOST;
	}
	if (polardb_debug_split_failure_fault_is("writer_not_started")) {
		writer_hg = polardb_writer_hgid();
		writer_backend = nullptr;
		writer_state = PolarDB_WriterState::NOT_STARTED;
	}
	POLARDB_TRACE(
		"PolarDB FAILURE: writer_state=%s writer_hg=%d writer_backend=%p\n",
		polardb_writer_state_name(writer_state), writer_hg,
		static_cast<void*>(writer_backend));

	polardb_record_reader_failure_status(failure);
	polardb_finish_reader_failure_split_op(failure);

	if (writer_state == PolarDB_WriterState::LOST) {
		POLARDB_TRACE(
			"PolarDB FAILURE: terminating because writer transaction state is lost\n");
		return polardb_terminate_reader(failure);
	}

	const PolarDB_ReaderFailureDecision decision =
		polardb_reader_decision_for(failure);
	POLARDB_TRACE(
		"PolarDB FAILURE: policy kind=%s action=%s target=%s pin=%s "
		"timeout=%d reusable=%d result_started=%d\n",
		polardb_reader_failure_kind_name(decision.kind),
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		polardb_route_pin_name(decision.route_pin),
		failure.timeout ? 1 : 0,
		failure.reusable ? 1 : 0,
		failure.result_started ? 1 : 0);
	if (decision.action == PolarDB_ReaderAction::TERMINATE) {
		return polardb_terminate_reader(failure);
	}

	if (decision.action == PolarDB_ReaderAction::RETRY &&
			!failure.result_started) {
		if (decision.retry_target == PolarDB_RetryTarget::OTHER_READER &&
				polardb_try_redispatch_to_other_reader(failure)) {
			polardb_apply_reader_failure_route_pin(
				PolarDB_RoutePin::SHUN_READER, writer_hg, &failure);
			return PolarDB_FailureAction::RETRY;
		}
		if (writer_state == PolarDB_WriterState::LIVE &&
				polardb_try_redispatch_to_writer(
					failure, writer_hg, writer_backend)) {
			polardb_apply_reader_failure_route_pin(
				PolarDB_RoutePin::FORCE_WRITER, writer_hg, &failure);
			return PolarDB_FailureAction::RETRY;
		}
	}

	polardb_apply_reader_failure_route_pin(
		PolarDB_RoutePin::FORCE_WRITER, writer_hg, &failure);
	POLARDB_TRACE(
		"PolarDB FAILURE: forwarding reader error after retry declined "
		"(action=%s target=%s writer_state=%s result_started=%d)\n",
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		polardb_writer_state_name(writer_state),
		failure.result_started ? 1 : 0);
	return polardb_forward_and_continue(failure);
}

PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_reader_failure_view(
		const PolarDB_RequestOutcome& outcome) {
	PolarDB_ReaderFailure failure;
	failure.reader_backend = outcome.backend;
	failure.connected = outcome.connected;
	failure.reusable = outcome.reusable;
	failure.result_started = outcome.result_started;
	failure.timeout = outcome.timeout_error;
	failure.timeout_already_accounted = outcome.timeout_already_accounted;
	failure.reader_hg = outcome.backend_hg;
	failure.reader_address = outcome.backend_address;
	failure.reader_port = outcome.backend_port;
	failure.has_backend_error = !outcome.error_message.empty();
	failure.backend_error_message = outcome.error_message;
	if (outcome.backend_error_code >= 0) {
		failure.backend_error_code =
			static_cast<PGSQL_ERROR_CODES>(outcome.backend_error_code);
	}

	failure.split_read = polardb_txn_split_active &&
		failure.reader_backend == polardb_txn_split_backend;
	if (failure.split_read) {
		// Move the original client packet before split cleanup can free it.
		// RETRY moves it to the next backend stream; FORWARD/TERMINATE free it.
		// Reader-retry also needs the split wait target and XIDs because the
		// active split wrapper is discarded during failure cleanup and rebuilt
		// for the replacement reader.
		failure.retry_pkt = polardb_txn_split_original_pkt;
		polardb_txn_split_original_pkt.ptr = nullptr;
		polardb_txn_split_original_pkt.size = 0;
		failure.reader_plan = polardb_query.reader_plan;
		failure.wait_spec = polardb_txn_split_wait_spec;
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

int PgSQL_Session::polardb_writer_hgid() {
	if (current_hostgroup < 0) {
		return -1;
	}
	const int mapped =
		PgHGM->get_writer_hostgroup_for_reader((unsigned int)current_hostgroup);
	return mapped >= 0 ? mapped : current_hostgroup;
}

bool PgSQL_Session::polardb_find_live_writer(int& writer_hg,
		PgSQL_Backend*& writer_backend, PgSQL_Backend* exclude_reader) {
	auto accept = [&](PgSQL_Backend* candidate) -> bool {
		if (!candidate || candidate == exclude_reader ||
				!candidate->server_myds) {
			return false;
		}
		PgSQL_Connection* conn = candidate->server_myds->myconn;
		if (!conn || !conn->is_connected() ||
				!conn->IsKnownActiveTransaction()) {
			return false;
		}
		writer_hg = candidate->hostgroup_id;
		writer_backend = candidate;
		return true;
	};

	writer_hg = -1;
	writer_backend = nullptr;
	if (!mybes) {
		return false;
	}
	if (transaction_persistent_hostgroup != -1 &&
			accept(find_backend(transaction_persistent_hostgroup))) {
		return true;
	}
	const int active_hg = FindOneActiveTransaction(true);
	if (active_hg != -1 && accept(find_backend(active_hg))) {
		return true;
	}
	for (unsigned int i = 0; i < mybes->len; ++i) {
		if (accept(static_cast<PgSQL_Backend*>(mybes->index(i)))) {
			return true;
		}
	}
	return false;
}

PolarDB_WriterState PgSQL_Session::polardb_resolve_writer_state(
		const PolarDB_ReaderFailure& failure, int& writer_hg,
		PgSQL_Backend*& writer_backend) {
	if (polardb_find_live_writer(writer_hg, writer_backend,
			failure.reader_backend)) {
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
		writer_hg = polardb_writer_hgid();
		writer_backend = nullptr;
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
	if (!failure.reusable) {
		return PolarDB_ReaderFailureKind::CONNECTION_LOST;
	}
	if (failure.timeout) {
		return PolarDB_ReaderFailureKind::WAIT_TIMEOUT;
	}
	return PolarDB_ReaderFailureKind::REUSABLE_ERROR;
}

PgSQL_Session::PolarDB_ReaderFailureDecision
PgSQL_Session::polardb_reader_decision_for(
		const PolarDB_ReaderFailure& failure) {
	PolarDB_ReaderFailureDecision decision;
	decision.kind = polardb_reader_failure_kind_for(failure);
	switch (decision.kind) {
	case PolarDB_ReaderFailureKind::CONNECTION_LOST:
		decision.action = static_cast<PolarDB_ReaderAction>(
			pgsql_thread___polardb_reader_death_action);
		break;
	case PolarDB_ReaderFailureKind::WAIT_TIMEOUT:
		decision.action = static_cast<PolarDB_ReaderAction>(
			pgsql_thread___polardb_reader_timeout_action);
		break;
	case PolarDB_ReaderFailureKind::REUSABLE_ERROR:
		decision.action = static_cast<PolarDB_ReaderAction>(
			pgsql_thread___polardb_reader_error_action);
		break;
	}

	if (decision.action == PolarDB_ReaderAction::RETRY &&
			failure.split_read &&
			decision.kind == PolarDB_ReaderFailureKind::CONNECTION_LOST) {
		if (polardb_query.reader_retry_attempts < POLARDB_READER_RETRY_BUDGET) {
			// A dead split reader can be a local server failure. Try one other
			// reader with the same XID/LSN wrapper before falling back to the
			// writer. The one-attempt cap prevents a retry loop between readers when
			// multiple readers flap during one client statement.
			decision.retry_target = PolarDB_RetryTarget::OTHER_READER;
			decision.route_pin = PolarDB_RoutePin::SHUN_READER;
		} else {
			decision.retry_target = PolarDB_RetryTarget::WRITER;
			decision.route_pin = PolarDB_RoutePin::FORCE_WRITER;
		}
	} else {
		decision.retry_target = PolarDB_RetryTarget::WRITER;
		if (decision.action != PolarDB_ReaderAction::TERMINATE) {
			decision.route_pin = PolarDB_RoutePin::FORCE_WRITER;
		}
	}
	return decision;
}

void PgSQL_Session::polardb_apply_reader_failure_route_pin(
		PolarDB_RoutePin pin, int writer_hg,
		const PolarDB_ReaderFailure* failure) {
	if (pin == PolarDB_RoutePin::NONE) {
		return;
	}
	if (pin == PolarDB_RoutePin::FORCE_WRITER) {
		polardb_txn_reader_failure_pin = PolarDB_RoutePin::FORCE_WRITER;
		polardb_txn_writer_hg = writer_hg;
		polardb_txn_shunned_reader_hg = -1;
		polardb_txn_shunned_reader_address.clear();
		polardb_txn_shunned_reader_port = -1;
		POLARDB_TRACE(
			"PolarDB FAILURE: transaction route pin=%s writer_hg=%d "
			"after reader failure\n",
			polardb_route_pin_name(pin), writer_hg);
		return;
	}

	if (failure && failure->reader_hg >= 0 &&
			!failure->reader_address.empty() &&
			failure->reader_port >= 0) {
		polardb_txn_shunned_reader_hg = failure->reader_hg;
		polardb_txn_shunned_reader_address = failure->reader_address;
		polardb_txn_shunned_reader_port = failure->reader_port;
	}
	polardb_txn_reader_failure_pin = pin;
	polardb_txn_writer_hg = -1;
	POLARDB_TRACE(
		"PolarDB FAILURE: transaction route pin=%s shunned_reader=%s:%d "
		"reader_hg=%d after reader failure\n",
		polardb_route_pin_name(pin),
		polardb_txn_shunned_reader_address.c_str(),
		polardb_txn_shunned_reader_port,
		polardb_txn_shunned_reader_hg);
}

void PgSQL_Session::polardb_free_retry_pkt_if_owned(
		PolarDB_ReaderFailure& failure) {
	if (failure.retry_pkt.ptr) {
		l_free(failure.retry_pkt.size, failure.retry_pkt.ptr);
		failure.retry_pkt.ptr = nullptr;
		failure.retry_pkt.size = 0;
	}
}

void PgSQL_Session::polardb_reset_current_query_from_packet(
		const PtrSize_t& pkt) {
	CurrentQuery.query_parser_free();
	CurrentQuery.begin(
		reinterpret_cast<unsigned char*>(pkt.ptr),
		pkt.size,
		true);
	set_previous_status_mode3();
}

bool PgSQL_Session::polardb_move_retry_packet_to_writer(
		PgSQL_Backend* writer_backend, int writer_hg, PtrSize_t& retry_pkt) {
	if (!writer_backend || !writer_backend->server_myds || !retry_pkt.ptr) {
		return false;
	}

	PgSQL_Data_Stream* writer_myds = writer_backend->server_myds;
	current_hostgroup = writer_hg;
	mybe = writer_backend;
	writer_myds->free_pgsql_real_query();
	writer_myds->pgsql_real_query.init(&retry_pkt);
	retry_pkt.ptr = nullptr;
	retry_pkt.size = 0;

	// Reset from the packet now owned by the writer stream; retry_pkt is empty
	// after the move.
	polardb_reset_current_query_from_packet(writer_myds->pgsql_real_query.pkt);
	return true;
}

void PgSQL_Session::polardb_return_or_destroy_backend_stream(
		PgSQL_Data_Stream* myds, bool return_to_pool) {
	if (!myds) {
		return;
	}
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

void PgSQL_Session::polardb_record_reader_failure_status(
		const PolarDB_ReaderFailure& failure) {
	polardb_clear_reader_affinity(/*count_failure=*/true);
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

void PgSQL_Session::polardb_finish_reader_failure_split_op(
		PolarDB_ReaderFailure& /*failure*/) {
	POLARDB_TRACE(
		"PolarDB FAILURE: finishing split read failure; transaction stage -> primary\n");
	polardb_record_txn_split_wait_latency();
	polardb_record_txn_split_latency();
	polardb_reset_txn_split_read();
	polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	polardb_transaction_split.blocked = true;
}

void PgSQL_Session::polardb_release_reader_backend(
		PgSQL_Backend* reader_backend, bool want_reuse) {
	if (!reader_backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: no reader backend to release\n");
		return;
	}
	if (reader_backend == polardb_txn_split_backend) {
		POLARDB_TRACE(
			"PolarDB FAILURE: releasing split reader backend want_reuse=%d\n",
			want_reuse ? 1 : 0);
		polardb_release_txn_split_backend(want_reuse);
		return;
	}
	PgSQL_Data_Stream* reader_myds = reader_backend->server_myds;
	if (!reader_myds) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader backend has no data stream\n");
		return;
	}
	reader_myds->free_pgsql_real_query();
	PgSQL_Connection* conn = reader_myds->myconn;
	if (!conn) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader stream has no connection; reset stream state\n");
		reader_myds->DSS = STATE_NOT_INITIALIZED;
		return;
	}
	conn->async_free_result();
	const bool reusable =
		want_reuse &&
		conn->reusable &&
		conn->is_connected() &&
		conn->is_connection_in_reusable_state() &&
		!conn->IsActiveTransaction() &&
		!conn->MultiplexDisabled() &&
		!conn->is_pipeline_active();
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
		reader_myds->fd = 0;
		reader_myds->DSS = STATE_NOT_INITIALIZED;
	}
}

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
	if (polardb_debug_split_failure_fault_is("writer_busy")) {
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

	polardb_release_reader_backend(
		failure.reader_backend, failure.reusable);
	if (!polardb_move_retry_packet_to_writer(
			writer_backend, writer_hg, failure.retry_pkt)) {
		return false;
	}

	POLARDB_THREAD_COUNT_ONE(thread, split_reads_retried);
	POLARDB_TRACE(
		"PolarDB FAILURE: retry split read on writer_hg=%d\n",
		writer_hg);
	return true;
}

bool PgSQL_Session::polardb_try_redispatch_to_other_reader(
		PolarDB_ReaderFailure& failure) {
	if (!failure.split_read) {
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
	if (!failure.wait_spec.has_wait() || failure.txn_xids.empty() ||
			!failure.reader_plan.has_consistency_target_lsn()) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=missing_split_payload "
			"target_lsn=%lu xids_len=%zu reader_plan_target=%lu\n",
			(unsigned long)failure.wait_spec.target,
			failure.txn_xids.size(),
			(unsigned long)failure.reader_plan.consistency_target_lsn);
		return false;
	}

	std::string wrapped_query;
	if (!polardb_build_txn_split_wrapped_query(
			failure.retry_pkt, failure.wait_spec,
			failure.txn_xids, wrapped_query)) {
		POLARDB_TRACE(
			"PolarDB FAILURE: reader retry declined reason=wrapper_build_failed\n");
		return false;
	}

	PgSQL_Backend* old_reader_backend = failure.reader_backend;
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
		PgHGM->get_MyConn_polardb_reader(
			(unsigned int)failure.reader_hg, this,
			failure.reader_plan, true,
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
		retry_myds->destroy_MySQL_Connection_From_Pool(false);
		retry_myds->fd = 0;
		return false;
	}

	PgSQL_Backend* writer_backend = mybe;
	current_hostgroup = failure.reader_hg;
	mybe = retry_backend;
	polardb_txn_split_backend = retry_backend;
	polardb_txn_split_saved_mybe = writer_backend;
	polardb_txn_split_wrapped_query = std::move(wrapped_query);
	polardb_txn_split_wait_spec = failure.wait_spec;
	polardb_query.reader_plan = failure.reader_plan;
	retry_myds->DSS = STATE_READY;
	retry_myds->fd = retry_myds->myconn->fd;
	retry_myds->free_pgsql_real_query();
	assert(retry_myds->pgsql_real_query.pkt.ptr == nullptr);
	assert(retry_myds->pgsql_real_query.pkt.size == 0);
	assert(!polardb_txn_split_wrapped_query.empty());
	// QueryPtr borrows the session-owned retry wrapper string. The split reset
	// path clears this stream pointer before clearing the string storage.
	retry_myds->pgsql_real_query.QueryPtr =
		const_cast<char*>(polardb_txn_split_wrapped_query.c_str());
	retry_myds->pgsql_real_query.QuerySize =
		(unsigned int)polardb_txn_split_wrapped_query.size();
	polardb_txn_split_original_pkt = failure.retry_pkt;
	failure.retry_pkt.ptr = nullptr;
	failure.retry_pkt.size = 0;
	polardb_txn_split_active = true;
	polardb_txn_split_read_start_us = monotonic_time();
	polardb_txn_split_wait_start_us = polardb_txn_split_read_start_us;
	polardb_query.reader_retry_attempts++;
	polardb_transaction_split.blocked = false;
	polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLIT_READ_ACTIVE;
	polardb_transaction_split.was_splittable = true;
	polardb_query.dispatch_wrapper_stmts = POLARDB_TXN_SPLIT_WRAPPER_SET_COUNT;
	polardb_query.dispatch_wrapper_kind =
		PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT;
	if (retry_myds->myconn) {
		// Reader retry sends the same split-XID wrapper as the first split
		// attempt. The physical connection must clear it before later non-split
		// reuse, or the backend may keep stale XIDs.
		retry_myds->myconn->polardb_txn_split_xids_dirty = true;
		retry_myds->myconn->polardb_txn_split_xids_reset_consumed = false;
	}

	POLARDB_THREAD_COUNT_ONE(thread, split_reads_retried_on_reader);
	POLARDB_TRACE(
		"PolarDB FAILURE: retry split read on other_reader_hg=%d "
		"excluded=%s:%d target_lsn=%lu\n",
		failure.reader_hg, failure.reader_address.c_str(),
		failure.reader_port, (unsigned long)failure.wait_spec.target);
	polardb_reset_current_query_from_packet(polardb_txn_split_original_pkt);
	return true;
}

PolarDB_FailureAction PgSQL_Session::polardb_forward_and_continue(
		PolarDB_ReaderFailure& failure) {
	polardb_forward_reader_error(failure, 'T');
	POLARDB_THREAD_COUNT_ONE(thread, split_reads_forwarded);
	POLARDB_TRACE(
		"PolarDB FAILURE: forwarded reader error; transaction remains on writer_hg=%d\n",
		polardb_txn_writer_hg);
	polardb_release_reader_backend(
		failure.reader_backend, failure.reusable);
	polardb_free_retry_pkt_if_owned(failure);
	return PolarDB_FailureAction::FORWARD;
}

PolarDB_FailureAction PgSQL_Session::polardb_terminate_reader(
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
void PgSQL_Session::polardb_release_wait_reader(PgSQL_Data_Stream* failed_myds,
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
		polardb_return_or_destroy_backend_stream(failed_myds, true);
	} else {
		polardb_return_or_destroy_backend_stream(failed_myds, false);
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

PgSQL_Session::PolarDB_ReaderFailure
PgSQL_Session::polardb_capture_wait_read_failure(PgSQL_Data_Stream* failed_myds) {
	PolarDB_ReaderFailure failure;
	failure.failed_myds = failed_myds;
	failure.timeout = polardb_query.wait.timeout_error;
	failure.fallback_writer_hg = polardb_query.wait.fallback_writer_hg;
	// Snapshot the original query before normal error handling can clear
	// per-query wait state; the primary retry rebuilds a fresh simple-query packet.
	failure.retry_query = polardb_query.wait.original_query;

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
	failure.connected = failed_conn && failed_conn->is_connected();
	failure.reusable = failure.connected &&
		failed_conn->is_connection_in_reusable_state();
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
		!failure.retry_query.empty();

	if (failed_conn) {
		failure.has_backend_error = !failed_conn->get_error_message().empty();
		failure.backend_error_message =
			polardb_snapshot_error_message(failed_conn->get_error_message());
		failure.backend_error_code =
			static_cast<PGSQL_ERROR_CODES>(failed_conn->get_error_code());
	}

	if (failed_conn && failed_conn->parent) {
		PgSQL_SrvC* srv = failed_conn->parent;
		failure.reader_hg = srv->myhgc ? (int)srv->myhgc->hid : -1;
		failure.reader_address = srv->address ? srv->address : "";
		failure.reader_port = (int)srv->port;
	}

	return failure;
}

PolarDB_FailureAction PgSQL_Session::polardb_handle_failed_wait_read(
		const PolarDB_ReaderFailure& failure,
		const PolarDB_ReaderFailureDecision& decision) {
	if (!failure.wait_read) {
		return PolarDB_FailureAction::PASSTHROUGH;
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

	const bool txn_wait_read =
		polardb_txn_wait_read_active &&
		failure.failed_myds &&
		polardb_txn_split_backend &&
		polardb_txn_split_backend->server_myds == failure.failed_myds;
	auto release_failed_wait_reader = [&](bool want_reuse) {
		if (txn_wait_read) {
			polardb_release_txn_wait_read(want_reuse);
		} else {
			polardb_release_wait_reader(failure.failed_myds, want_reuse);
		}
	};
	auto finish_txn_wait_with_error = [&]() {
		release_failed_wait_reader(false);
		if (failure.result_started) {
			POLARDB_TRACE(
				"PolarDB WAIT: transaction wait-read failed after rows started; "
				"terminating session\n");
			return PolarDB_FailureAction::TERMINATE;
		}
		polardb_forward_reader_error(failure, 'T');
		return PolarDB_FailureAction::FORWARD;
	};

	POLARDB_TRACE(
		"PolarDB WAIT: policy kind=%s action=%s target=%s timeout=%d "
		"reusable=%d result_started=%d\n",
		polardb_reader_failure_kind_name(decision.kind),
		polardb_reader_action_name(decision.action),
		polardb_retry_target_name(decision.retry_target),
		failure.timeout ? 1 : 0,
		failure.reusable ? 1 : 0,
		failure.result_started ? 1 : 0);
	if (decision.action == PolarDB_ReaderAction::TERMINATE) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_retry_declined_policy_terminate,
			"policy_terminate");
		POLARDB_THREAD_COUNT_ONE(thread, reader_terminations);
		POLARDB_TRACE(
			"PolarDB WAIT: terminating session after reader failure\n");
		release_failed_wait_reader(false);
		return PolarDB_FailureAction::TERMINATE;
	}
	if (decision.action == PolarDB_ReaderAction::FORWARD) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_retry_declined_policy_forward,
			"policy_forward");
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		POLARDB_TRACE(
			"PolarDB WAIT: policy action=forward; normal error path will "
			"forward clean reader error after wrapper cleanup\n");
		return PolarDB_FailureAction::PASSTHROUGH;
	}
	if (decision.retry_target != PolarDB_RetryTarget::WRITER) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_retry_declined_target_not_writer,
			"target_not_writer");
		POLARDB_TRACE(
			"PolarDB WAIT: retry target=%s not implemented for wait-read yet\n",
			polardb_retry_target_name(decision.retry_target));
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	const bool recoverable_failure =
		failure.timeout || !failure.reusable;
	if (!recoverable_failure || failure.result_started ||
		failure.fallback_writer_hg < 0 || failure.retry_query.empty()) {
		if (!recoverable_failure) {
			polardb_count_wait_retry_counter(
				PgHGM->status.polardb_wait_retry_declined_not_recoverable,
				"not_recoverable");
		} else if (failure.result_started) {
			polardb_count_wait_retry_counter(
				PgHGM->status.polardb_wait_retry_declined_result_started,
				"result_started");
		} else if (failure.fallback_writer_hg < 0) {
			polardb_count_wait_retry_counter(
				PgHGM->status.polardb_wait_retry_declined_writer_hg_unknown,
				"writer_hg_unknown");
		} else {
			polardb_count_wait_retry_counter(
				PgHGM->status.polardb_wait_retry_declined_original_query_missing,
				"original_query_missing");
		}
		POLARDB_TRACE(
			"PolarDB WAIT: failed wait-read cleaned wrapper; "
			"normal error path will not redispatch wrapped packet "
			"recoverable=%d result_started=%d writer_hg=%d original_query=%d\n",
			recoverable_failure ? 1 : 0, failure.result_started ? 1 : 0,
			failure.fallback_writer_hg, failure.retry_query.empty() ? 0 : 1);
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	if (txn_wait_read) {
		release_failed_wait_reader(failure.can_return_to_pool);
	}

	PgSQL_Backend* writer_mybe = find_or_create_backend(failure.fallback_writer_hg);
	if (!writer_mybe || !writer_mybe->server_myds ||
			writer_mybe->server_myds == failure.failed_myds) {
		POLARDB_TRACE(
			"PolarDB WAIT: failed wait-read cleaned wrapper; "
			"primary retry stream unavailable writer_hg=%d\n",
			failure.fallback_writer_hg);
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
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
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	PtrSize_t retry_pkt = {0, NULL};
	build_simple_query_packet(failure.retry_query, retry_pkt);
	if (!retry_pkt.ptr || retry_pkt.size == 0) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_retry_declined_packet_build_failed,
			"packet_build_failed");
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}

	POLARDB_TRACE(
		"PolarDB WAIT: %s before user result; redirecting original "
		"unwrapped query to writer_hg=%d reader_hg=%d\n",
		failure.timeout ? "strict wait timeout" : "reader connection lost",
		failure.fallback_writer_hg, failure.reader_hg);
	PgHGM->status.polardb_wait_reads_retried_on_writer.fetch_add(
		1, std::memory_order_relaxed);

	if (failure.reader_hg >= 0 && !failure.reader_address.empty()) {
		PgHGM->p_update_pgsql_error_counter(
			p_pgsql_error_type::pgsql,
			failure.reader_hg,
			const_cast<char*>(failure.reader_address.c_str()),
			failure.reader_port,
			POLARDB_REPLICA_FAILURE_ERROR_CODE);
	}

	if (!txn_wait_read) {
		polardb_release_wait_reader(
			failure.failed_myds, failure.can_return_to_pool);
	}

	if (!polardb_move_retry_packet_to_writer(
			writer_mybe, failure.fallback_writer_hg, retry_pkt)) {
		polardb_count_wait_retry_counter(
			PgHGM->status.polardb_wait_retry_declined_move_failed,
			"move_failed");
		if (retry_pkt.ptr) {
			l_free(retry_pkt.size, retry_pkt.ptr);
		}
		if (txn_wait_read) {
			return finish_txn_wait_with_error();
		}
		return PolarDB_FailureAction::PASSTHROUGH;
	}
	polardb_count_wait_retry_counter(
		PgHGM->status.polardb_wait_reads_retried_on_writer, "succeeded");
	return PolarDB_FailureAction::RETRY;
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
