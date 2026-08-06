/**
 * @file polardb_session_unit_access.cpp
 * @brief Test-only access to PgSQL session routing state.
 *
 * Keep this translation unit free of libpq-private headers: PostgreSQL and
 * CityHash both define uint128, so combining those header domains is unsafe.
 */

#include "proxysql.h"
#include "cpp.h"
#include "PgSQL_Backend.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Extended_Query_Message.h"
#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Query_Processor.h"

#include "polardb_unit_support.h"

extern PgSQL_STMT_Manager* GloPgStmt;

static void init_test_frontend_protocol(PgSQL_Session* session) {
	session->client_myds->myprot.init(
		&session->client_myds, session->client_myds->myconn->userinfo, session);
}

struct FrontendMessageSummary {
	std::string types;
	char last_rfq{'\0'};
	bool last_rfq_has_lsn{false};
	uint64_t last_rfq_lsn{0};
	std::string sqlstate;
};

static FrontendMessageSummary drain_frontend_messages(
		PgSQL_Session* session) {
	FrontendMessageSummary summary;
	while (session->client_myds->PSarrayOUT->len != 0) {
		PtrSize_t packet{};
		session->client_myds->PSarrayOUT->remove_index(0, &packet);
		const auto* bytes = static_cast<const unsigned char*>(packet.ptr);
		size_t offset = 0;
		while (offset + 5 <= packet.size) {
			const uint32_t length =
				(static_cast<uint32_t>(bytes[offset + 1]) << 24) |
				(static_cast<uint32_t>(bytes[offset + 2]) << 16) |
				(static_cast<uint32_t>(bytes[offset + 3]) << 8) |
				static_cast<uint32_t>(bytes[offset + 4]);
			if (length < 4 || offset + 1 + length > packet.size) {
				summary.types = "invalid";
				break;
			}
			const char type = static_cast<char>(bytes[offset]);
			summary.types.push_back(type);
			if (type == 'Z' && (length == 5 || length == 13)) {
				summary.last_rfq = static_cast<char>(bytes[offset + 5]);
				if (length == 13) {
					summary.last_rfq_has_lsn = true;
					for (size_t i = 0; i < sizeof(uint64_t); ++i) {
						summary.last_rfq_lsn =
							(summary.last_rfq_lsn << 8) |
							static_cast<uint64_t>(bytes[offset + 6 + i]);
					}
				}
			} else if (type == 'E') {
				size_t field = offset + 5;
				const size_t end = offset + 1 + length;
				while (field < end && bytes[field] != 0) {
					const char tag = static_cast<char>(bytes[field++]);
					const auto* terminator = static_cast<const unsigned char*>(
						memchr(bytes + field, 0, end - field));
					if (!terminator) {
						summary.types = "invalid";
						break;
					}
					if (tag == 'C') {
						summary.sqlstate.assign(
							reinterpret_cast<const char*>(bytes + field),
							terminator - (bytes + field));
					}
					field = static_cast<size_t>(terminator - bytes) + 1;
				}
			}
			offset += 1 + length;
		}
		if (summary.types != "invalid" && offset != packet.size) {
			summary.types = "invalid";
		}
		l_free(packet.size, packet.ptr);
	}
	return summary;
}

static std::pair<std::string, char> drain_frontend_message_types(
		PgSQL_Session* session) {
	const auto summary = drain_frontend_messages(session);
	return {summary.types, summary.last_rfq};
}

static std::unique_ptr<PgSQL_Parse_Message> make_test_parse_message() {
	constexpr char query[] = "SELECT 1";
	constexpr unsigned int packet_size =
		1 + 4 + 1 + sizeof(query) + 2;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);
	packet[0] = 'P';
	packet[4] = packet_size - 1; // PostgreSQL length excludes the type byte.
	memcpy(packet + 6, query, sizeof(query));
	PtrSize_t pkt{packet_size, packet};
	auto parse = std::make_unique<PgSQL_Parse_Message>();
	assert(parse->parse(pkt));
	return parse;
}

static std::unique_ptr<PgSQL_Execute_Message> make_test_execute_message() {
	constexpr unsigned int packet_size = 10;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);
	packet[0] = 'E';
	packet[4] = packet_size - 1; // PostgreSQL length excludes the type byte.
	PtrSize_t pkt{packet_size, packet};
	auto execute = std::make_unique<PgSQL_Execute_Message>();
	assert(execute->parse(pkt));
	return execute;
}

static std::unique_ptr<PgSQL_Bind_Message> make_test_bind_message() {
	constexpr unsigned int packet_size = 13;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);
	packet[0] = 'B';
	packet[4] = packet_size - 1;
	PtrSize_t pkt{packet_size, packet};
	auto bind = std::make_unique<PgSQL_Bind_Message>();
	assert(bind->parse(pkt));
	return bind;
}

static std::unique_ptr<PgSQL_Describe_Message> make_test_describe_message(
		char stmt_type) {
	constexpr unsigned int packet_size = 7;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);
	packet[0] = 'D';
	packet[4] = packet_size - 1; // PostgreSQL length excludes the type byte.
	packet[5] = stmt_type;
	PtrSize_t pkt{packet_size, packet};
	auto describe = std::make_unique<PgSQL_Describe_Message>();
	assert(describe->parse(pkt));
	return describe;
}

void PolarDB_SessionUnitAccess::queue_test_parse(PgSQL_Session* session) {
	auto message = make_test_parse_message();
	const char* stmt_name = message->data().stmt_name;
	session->queue_extended_message(std::move(message),
		PgSQL_Session::ExtendedFrameMessageKind::PARSE, stmt_name);
}

void PolarDB_SessionUnitAccess::queue_test_bind(PgSQL_Session* session) {
	auto message = make_test_bind_message();
	const char* stmt_name = message->data().stmt_name;
	session->queue_extended_message(std::move(message),
		PgSQL_Session::ExtendedFrameMessageKind::BIND, stmt_name);
}

void PolarDB_SessionUnitAccess::queue_test_describe(
		PgSQL_Session* session, char stmt_type) {
	auto message = make_test_describe_message(stmt_type);
	const auto kind = stmt_type == 'P'
		? PgSQL_Session::ExtendedFrameMessageKind::DESCRIBE_PORTAL
		: PgSQL_Session::ExtendedFrameMessageKind::DESCRIBE_STATEMENT;
	session->queue_extended_message(std::move(message), kind);
}

void PolarDB_SessionUnitAccess::queue_test_execute(PgSQL_Session* session) {
	session->queue_extended_message(make_test_execute_message(),
		PgSQL_Session::ExtendedFrameMessageKind::EXECUTE);
}

void PolarDB_SessionUnitAccess::set_extended_route_state(
		PgSQL_Session* session, int replica_eligible,
		bool force_primary_hint, uint8_t phase) {
	session->qpo->replica_eligible = replica_eligible;
	session->qpo->force_primary_hint = force_primary_hint;
	session->extended_query_phase = phase;
}

void PolarDB_SessionUnitAccess::set_extended_request_boundary(
		PgSQL_Session* session, uint8_t phase, bool pending_message) {
	session->reset_extended_query_frame();
	session->extended_query_phase = phase;
	if (pending_message) {
		queue_test_parse(session);
		session->begin_extended_query_frame(true);
	}
}

bool PolarDB_SessionUnitAccess::extended_request_continues(
		const PgSQL_Session* session,
		bool called_on_failure, bool result_has_error) {
	return session->polardb_extended_request_continues(
		called_on_failure, result_has_error);
}

void PolarDB_SessionUnitAccess::clear_request_state_for_query_end(
		PgSQL_Session* session, PgSQL_Data_Stream* backend_myds,
		bool called_on_failure) {
	session->polardb_clear_request_state_for_query_end(
		backend_myds, called_on_failure);
}

PolarDB_SessionUnitAccess::ReaderFailureDecision
PolarDB_SessionUnitAccess::reader_failure_decision(
		PgSQL_Session* session, bool timeout, bool reusable,
		PGSQL_ERROR_CODES backend_error_code,
		PolarDB_LsnWaitTimeoutAction timeout_action,
		PolarDB_ReplicaErrorAction replica_error_action) {
	PgSQL_Session::PolarDB_ReaderFailure failure;
	failure.timeout = timeout;
	failure.reusable = reusable;
	failure.backend_error_code = backend_error_code;
	failure.reader_plan.lsn_wait_timeout_action =
		static_cast<int>(timeout_action);
	failure.reader_plan.replica_error_action =
		static_cast<int>(replica_error_action);

	const PgSQL_Session::PolarDB_ReaderFailureDecision decision =
		session->polardb_reader_failure_decision(failure);
	return ReaderFailureDecision{
		decision.kind,
		decision.action,
		decision.retry_target,
		decision.reader_failure_route,
		decision.allow_writer_retry};
}

bool PolarDB_SessionUnitAccess::request_has_wait_timeout_evidence(
		bool structured_timeout) {
	return PgSQL_Session::polardb_request_has_wait_timeout_evidence(
		structured_timeout);
}

PolarDB_SessionUnitAccess::ReaderFailurePacketResult
PolarDB_SessionUnitAccess::finish_reusable_wait_error(
		PgSQL_Session* session, bool query_aliases_packet) {
	auto retry_counter_total = []() {
		const auto& status = PgHGM->status;
		return
			status.polardb_wait_retry_evaluated.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_attempted.load(std::memory_order_relaxed) +
			status.polardb_wait_reads_retried_on_reader.load(std::memory_order_relaxed) +
			status.polardb_wait_reads_retried_on_writer.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_policy_forward.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_policy_terminate.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_target_not_writer.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_not_recoverable.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_result_started.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_writer_hg_unknown.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_original_query_missing.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_writer_unavailable.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_same_stream.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_writer_busy.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_packet_build_failed.load(std::memory_order_relaxed) +
			status.polardb_wait_retry_declined_move_failed.load(std::memory_order_relaxed);
	};
	const unsigned long long retry_counters_before = retry_counter_total();

	constexpr unsigned int packet_size = 16;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);

	PgSQL_Data_Stream request_myds;
	request_myds.init(MYDS_BACKEND, session, 0);
	PgSQL_Session::PolarDB_ReaderFailure failure;
	failure.request_kind = PgSQL_Session::PolarDB_ReaderRequestKind::WAIT;
	failure.failed_myds = &request_myds;
	failure.reusable = true;
	failure.connected = true;
	failure.retry_pkt = PtrSize_t{packet_size, packet};

	unsigned char non_alias_query[] = "SELECT 1";
	unsigned char* const saved_query_pointer = session->CurrentQuery.QueryPointer;
	const unsigned int saved_query_length = session->CurrentQuery.QueryLength;
	session->CurrentQuery.QueryPointer = query_aliases_packet
		? packet + 5 : non_alias_query;
	session->CurrentQuery.QueryLength = query_aliases_packet ? 4 : 8;

	PgSQL_Session::PolarDB_ReaderFailureDecision decision;
	decision.kind = PolarDB_ReaderFailureKind::QUERY_CANCELED;
	decision.action = PolarDB_ReaderAction::RETURN_ERROR;
	decision.allow_writer_retry = false;
	const PolarDB_FailureAction action =
		session->polardb_handle_failed_reader_read(failure, decision);
	const bool failure_owns_packet = failure.retry_pkt.ptr != nullptr;
	const bool request_stream_owns_packet =
		request_myds.pgsql_real_query.pkt.ptr == packet;

	session->CurrentQuery.QueryPointer = saved_query_pointer;
	session->CurrentQuery.QueryLength = saved_query_length;
	request_myds.free_pgsql_real_query();
	return ReaderFailurePacketResult{
		action, failure_owns_packet, request_stream_owns_packet,
		retry_counter_total() == retry_counters_before};
}

PolarDB_SessionUnitAccess::ReaderFailureEvidence
PolarDB_SessionUnitAccess::preserve_captured_wait_failure_evidence(
		PgSQL_Session* session) {
	PgSQL_Session::PolarDB_ReaderFailure failure;
	failure.timeout = true;
	failure.reusable = true;
	failure.connected = true;
	failure.has_backend_error = true;
	failure.backend_error_code = PGSQL_ERROR_CODES::ERRCODE_QUERY_CANCELED;
	failure.backend_error_message = "structured PolarDB wait timeout";

	// Deliberately leave the mutable session/connection wait state empty. The
	// rc==-1 outcome captured before cleanup must remain authoritative.
	session->polardb_query.reset_wait();
	failure = session->polardb_capture_wait_read_failure(std::move(failure));
	return ReaderFailureEvidence{
		failure.timeout,
		failure.reusable,
		failure.connected,
		failure.has_backend_error,
		failure.backend_error_code};
}

bool PolarDB_SessionUnitAccess::execute_qpo_error_is_terminal(
		PgSQL_Session* session) {
	init_test_frontend_protocol(session);
	constexpr unsigned int packet_size = 10;
	unsigned char* packet = static_cast<unsigned char*>(l_alloc(packet_size));
	memset(packet, 0, packet_size);
	PtrSize_t pkt{packet_size, packet};
	session->qpo->error_msg = strdup("blocked by unit query rule");
	bool lock_hostgroup = false;
	return session->
		handler___status_WAITING_CLIENT_DATA___STATE_SLEEP___PGSQL_QUERY_qpo(
			&pkt, &lock_hostgroup,
			PGSQL_EXTENDED_QUERY_TYPE_EXECUTE) ==
		PgSQL_Session::QpoHandlerResult::ERROR;
}

bool PolarDB_SessionUnitAccess::extended_dispatch_error_clears_frame(
		PgSQL_Session* session) {
	init_test_frontend_protocol(session);
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_PROCESSING_EXECUTE;
	queue_test_execute(session);
	// The valid Parse must be discarded after the first Execute reports its local
	// missing-portal error.
	queue_test_parse(session);
	session->extended_query_frame_state.begin(0, false, true, true);
	const int rc = session->handler___status_PROCESSING_EXTENDED_QUERY_SYNC();
	return rc == 0 && session->extended_query_frame.empty() &&
		session->extended_query_phase == EXTQ_PHASE_IDLE &&
		!session->extended_query_frame_state.active();
}

bool PolarDB_SessionUnitAccess::extended_local_flush_error_waits_for_sync(
		PgSQL_Session* session) {
	init_test_frontend_protocol(session);
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	queue_test_parse(session);

	// Local validation abandons the Flush batch before generate_error_packet().
	// reset_extended_query_frame() must preserve skip-until-Sync ownership.
	session->reset_extended_query_frame();
	session->client_myds->setDSS_STATE_QUERY_SENT_NET();
	session->client_myds->myprot.generate_error_packet(
		true, false, "unit local Flush error",
		PGSQL_ERROR_CODES::ERRCODE_INVALID_PARAMETER_VALUE, false, true);
	const bool waiting = session->extended_query_frame_state
		.waiting_for_client_sync_after_error;
	const bool candidates_cleared = session->extended_query_frame_state
		.backend_candidates_remaining == 0;
	const bool discards_bind = session->extended_query_frame_state
		.discards_until_sync('B');

	session->extended_query_frame_state.begin(0, false, true, false);
	session->complete_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_IDLE;
	return waiting && candidates_cleared && discards_bind;
}

bool PolarDB_SessionUnitAccess::ordinary_sync_frame_uses_lazy_state(
		PgSQL_Session* session) {
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	queue_test_parse(session);
	queue_test_bind(session);
	queue_test_execute(session);
	session->begin_extended_query_frame(true);
	const bool lazy = !session->extended_query_frame_state.active();
	const bool exact_candidates = session->extended_query_frame_state
		.backend_candidates_remaining == 2;
	PgSQL_Extended_Query_Info dispatch_info{};
	session->mark_extended_backend_dispatch(dispatch_info,
		/*snapshot_capable=*/true);
	const bool no_backend_publication = dispatch_info.flags ==
		PGSQL_EXTENDED_QUERY_FLAG_NONE;
	session->reset_extended_query_frame();
	return lazy && exact_candidates && no_backend_publication;
}

PolarDB_SessionUnitAccess::ExtendedFrameOwnerResult
PolarDB_SessionUnitAccess::exercise_extended_frame_owner(
		PgSQL_Session* session, int previous_hostgroup,
		int selected_hostgroup, int conflicting_hostgroup) {
	session->extended_query_frame_state.begin(
		previous_hostgroup, false, true, false);
	const bool selected =
		session->select_extended_frame_hostgroup(selected_hostgroup);
	session->previous_hostgroup = conflicting_hostgroup;
	const bool carried =
		session->extended_frame_hostgroup_or_previous() == selected_hostgroup;
	session->current_hostgroup = selected_hostgroup;
	const bool preflight = session->can_claim_extended_frame_backend();
	const bool deferred = preflight &&
		session->extended_query_frame_state.backend_hostgroup ==
			PgSQL_Session::ExtendedQueryFrameState::UNCLAIMED_BACKEND;
	session->note_extended_backend_command_sent(selected_hostgroup);
	const bool claimed =
		session->extended_query_frame_state.backend_hostgroup == selected_hostgroup;
	const bool rejected =
		!session->select_extended_frame_hostgroup(conflicting_hostgroup);
	const int final_selected = session->extended_query_frame_state.hostgroup;
	const int final_backend =
		session->extended_query_frame_state.backend_hostgroup;
	session->complete_extended_query_frame();
	const bool cleared = session->extended_query_frame_state.hostgroup < 0 &&
		session->extended_query_frame_state.backend_hostgroup < 0;
	return ExtendedFrameOwnerResult{
		selected, carried, deferred, claimed, rejected,
		final_selected, final_backend, cleared};
}

PolarDB_SessionUnitAccess::ExtendedFrameTransferResult
PolarDB_SessionUnitAccess::exercise_extended_frame_transfer(
		PgSQL_Session* session, int initial_hostgroup,
		int replacement_hostgroup) {
	session->extended_query_frame_state.begin(
		initial_hostgroup, false, true, false);
	session->current_hostgroup = initial_hostgroup;
	assert(session->can_claim_extended_frame_backend());
	session->note_extended_backend_command_sent(initial_hostgroup);
	const bool claimed =
		session->extended_query_frame_state.backend_hostgroup == initial_hostgroup;
	session->extended_query_frame_state.mark_backend_sync_sent(true);
	session->transfer_extended_frame_backend(replacement_hostgroup);
	const ExtendedFrameTransferResult result{
		claimed,
		!session->extended_query_frame_state.backend_sync_sent,
		session->extended_query_frame_state.hostgroup,
		session->extended_query_frame_state.backend_hostgroup};
	session->complete_extended_query_frame();
	return result;
}

PolarDB_SessionUnitAccess::ExtendedQpoHostgroupResult
PolarDB_SessionUnitAccess::exercise_extended_qpo_hostgroups(
		PgSQL_Session* session, int previous_hostgroup,
		int first_destination, int later_destination) {
	session->extended_query_frame_state.begin(
		previous_hostgroup, false, true, false);
	session->previous_hostgroup = previous_hostgroup;
	const int first_target =
		session->extended_qpo_target_hostgroup(first_destination);
	const bool first_selected =
		session->select_extended_frame_hostgroup(first_target);
	session->note_extended_backend_command_sent(first_target);
	const bool first_claimed =
		session->extended_query_frame_state.backend_hostgroup == first_target;
	const int later_target =
		session->extended_qpo_target_hostgroup(later_destination);
	const bool later_rejected =
		!session->select_extended_frame_hostgroup(later_target);
	const int final_selected = session->extended_query_frame_state.hostgroup;
	const int final_backend =
		session->extended_query_frame_state.backend_hostgroup;
	const int original_default = session->default_hostgroup;
	const int original_previous = session->previous_hostgroup;
	const int original_transaction = session->transaction_persistent_hostgroup;
	session->complete_extended_query_frame();
	session->default_hostgroup = first_destination;
	session->previous_hostgroup = -1;
	session->transaction_persistent_hostgroup = -1;
	const bool missing_previous_uses_default =
		session->extended_qpo_target_hostgroup(-1) == first_destination;
	session->default_hostgroup = original_default;
	session->previous_hostgroup = original_previous;
	session->transaction_persistent_hostgroup = original_transaction;
	const ExtendedQpoHostgroupResult result{
		first_selected, first_claimed, later_rejected,
		missing_previous_uses_default,
		final_selected, final_backend};
	return result;
}

bool PolarDB_SessionUnitAccess::implicit_prepare_retry_lifecycle(
		PgSQL_Session* session) {
	while (!session->previous_status.empty()) {
		session->previous_status.pop();
	}
	session->clear_implicit_prepare();
	const bool execute_started =
		session->begin_implicit_prepare(PROCESSING_STMT_EXECUTE);
	session->status = PROCESSING_STMT_PREPARE;
	session->polardb_prepare_extended_retry();
	const bool retry_kept_logical_continuation =
		execute_started &&
		session->implicit_prepare_continuation == PROCESSING_STMT_EXECUTE &&
		(session->CurrentQuery.extended_query_info.flags &
			PGSQL_EXTENDED_QUERY_FLAG_IMPLICIT_PREPARE) &&
		!session->previous_status.empty() &&
		session->previous_status.top() == PROCESSING_STMT_PREPARE;

	// CONNECTING_SERVER consumes only the transport continuation.
	session->previous_status.pop();
	enum session_status continuation = session_status___NONE;
	const bool resumed = session->resume_implicit_prepare(continuation);
	const bool success_balanced = resumed &&
		continuation == PROCESSING_STMT_EXECUTE &&
		session->implicit_prepare_continuation == session_status___NONE &&
		session->previous_status.empty() &&
		!session->resume_implicit_prepare(continuation);

	// Terminal forwarding uses the same RequestEnd cleanup operation.
	const bool describe_started =
		session->begin_implicit_prepare(PROCESSING_STMT_DESCRIBE);
	const bool conflicting_transition_rejected =
		!session->begin_implicit_prepare(PROCESSING_STMT_EXECUTE) &&
		session->implicit_prepare_continuation == PROCESSING_STMT_DESCRIBE;
	session->clear_implicit_prepare();
	const bool terminal_balanced =
		describe_started && conflicting_transition_rejected &&
		session->implicit_prepare_continuation == session_status___NONE &&
		!(session->CurrentQuery.extended_query_info.flags &
			PGSQL_EXTENDED_QUERY_FLAG_IMPLICIT_PREPARE);
	const bool unsupported_transition_rejected =
		!session->begin_implicit_prepare(PROCESSING_QUERY) &&
		session->implicit_prepare_continuation == session_status___NONE;
	return retry_kept_logical_continuation && success_balanced &&
		terminal_balanced && unsupported_transition_rejected;
}

bool PolarDB_SessionUnitAccess::activation_transition_is_conservative(
		PgSQL_Session* session) {
	if (!session || !session->thread) {
		return false;
	}
	const bool original_enabled =
		session->polardb_config.is_polardb_enabled;
	const uint64_t original_session_generation =
		session->polardb_config.activation_generation;
	const bool original_unknown =
		session->polardb_session_consistency.write_unknown;
	const uint64_t original_worker_generation =
		session->thread->polardb_activation_generation_cache;
	const uint64_t active_generation = original_worker_generation + 7;
	session->thread->polardb_activation_generation_cache = active_generation;

	// A session registered after activation has no unattributed pre-activation
	// window, so its first PolarDB request may route normally.
	session->polardb_config.is_polardb_enabled = false;
	session->polardb_config.activation_generation = active_generation;
	session->polardb_session_consistency.write_unknown = false;
	session->polardb_activate_session_for_request();
	const bool born_active_is_known =
		session->polardb_config.is_polardb_enabled &&
		!session->polardb_session_consistency.write_unknown;

	// A session that saw the previous generation could have completed an
	// ordinary writer request before its worker consumed the activation wake.
	session->polardb_config.is_polardb_enabled = false;
	session->polardb_config.activation_generation = active_generation - 1;
	session->polardb_session_consistency.write_unknown = false;
	session->polardb_activate_session_for_request();
	const bool crossed_activation_is_unknown =
		session->polardb_config.is_polardb_enabled &&
		session->polardb_session_consistency.write_unknown &&
		session->polardb_config.activation_generation == active_generation;

	session->polardb_session_consistency.write_unknown = false;
	session->polardb_activate_session_for_request();
	const bool repeated_activation_is_stable =
		!session->polardb_session_consistency.write_unknown;

	++session->thread->polardb_activation_generation_cache;
	session->polardb_activate_session_for_request();
	const bool reactivation_is_unknown =
		session->polardb_session_consistency.write_unknown &&
		session->polardb_config.activation_generation ==
			session->thread->polardb_activation_generation_cache;

	session->polardb_config.is_polardb_enabled = false;
	session->polardb_config.activation_generation =
		PgSQL_Session::PolarDB_SessionConfig::
			UNREGISTERED_ACTIVATION_GENERATION;
	session->polardb_session_consistency.write_unknown = false;
	session->polardb_activate_session_for_request();
	const bool unregistered_is_conservative =
		session->polardb_session_consistency.write_unknown;

	session->polardb_config.is_polardb_enabled = original_enabled;
	session->polardb_config.activation_generation =
		original_session_generation;
	session->polardb_session_consistency.write_unknown = original_unknown;
	session->thread->polardb_activation_generation_cache =
		original_worker_generation;
	return born_active_is_known && crossed_activation_is_unknown &&
		repeated_activation_is_stable && reactivation_is_unknown &&
		unregistered_is_conservative;
}

bool PolarDB_SessionUnitAccess::local_frame_requires_implicit_sync(
		PgSQL_Session* session) {
	session->complete_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->extended_query_frame_state.begin(10, false, false, false);
	const bool local_frame_detected =
		session->needs_implicit_sync_before_simple_query();

	constexpr char query[] = "SELECT 1";
	auto statement = std::make_shared<PgSQL_STMT_Global_info>(
		7003, "unit", "unit", query, strlen(query),
		Parse_Param_Types{}, nullptr, 0);
	auto bind = make_test_bind_message();
	session->bind_waiting_for_execute.capture(
		bind.release(), statement.get());
	session->complete_extended_query_frame();
	const bool completion_cleared_portal =
		!session->bind_waiting_for_execute &&
		session->extended_query_phase == EXTQ_PHASE_IDLE;

	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->extended_query_frame_state.begin(10, false, false, false);
	session->extended_query_frame_state.mark_error_waiting_for_sync();
	const bool error_still_discards =
		!session->needs_implicit_sync_before_simple_query();
	session->complete_extended_query_frame();
	return local_frame_detected && completion_cleared_portal &&
		error_still_discards;
}

PolarDB_SessionUnitAccess::ErrorPacketOwnershipResult
PolarDB_SessionUnitAccess::exercise_error_packet_ownership(
		PgSQL_Session* session) {
	init_test_frontend_protocol(session);
	session->complete_extended_query_frame();
	drain_frontend_message_types(session);
	auto generate = [&](bool ready, bool fatal) {
		session->client_myds->setDSS_STATE_QUERY_SENT_NET();
		session->client_myds->myprot.generate_error_packet(
			true, ready, "unit protocol error",
			PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION, fatal, true);
		return drain_frontend_message_types(session).first;
	};
	return ErrorPacketOwnershipResult{
		generate(false, false) == "E",
		generate(true, false) == "EZ",
		generate(false, true) == "E",
		generate(true, true) == "E"};
}

PolarDB_SessionUnitAccess::ForwardedErrorOwnershipResult
PolarDB_SessionUnitAccess::exercise_forwarded_error_ownership(
		PgSQL_Session* session, int hostgroup) {
	init_test_frontend_protocol(session);
	session->complete_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->extended_query_frame_state.begin(
		hostgroup, false, false, true);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	queue_test_parse(session);

	PgSQL_Session::PolarDB_ReaderFailure failure;
	failure.has_backend_error = true;
	failure.backend_error_code = PGSQL_ERROR_CODES::ERRCODE_QUERY_CANCELED;
	failure.backend_error_message = "unit forwarded reader error";
	session->polardb_forward_reader_error(failure, 'T');
	const auto flush_output = drain_frontend_message_types(session);
	const bool queued_messages_discarded =
		session->extended_query_frame.empty() &&
		!session->bind_waiting_for_execute;
	const bool waits_for_client_sync =
		session->extended_query_frame_state.waiting_for_client_sync_after_error;

	// Model the normal client Sync boundary after the forwarded Flush error.
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	if (session->emit_extended_ready_for_query(nullptr)) {
		session->complete_extended_query_frame();
	}
	const auto sync_output = drain_frontend_message_types(session);

	// When Sync was already consumed, forwarding emits the exact transaction
	// state immediately and completes the frame once.
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, true);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	session->polardb_forward_reader_error(failure, 'T');
	const auto immediate_output = drain_frontend_message_types(session);
	return ForwardedErrorOwnershipResult{
		flush_output.first == "E",
		queued_messages_discarded,
		waits_for_client_sync,
		sync_output.first == "Z",
		immediate_output.first == "EZ" && immediate_output.second == 'T',
		!session->extended_query_frame_state.active() &&
			session->extended_query_phase == EXTQ_PHASE_IDLE};
}

PolarDB_SessionUnitAccess::ManualRouteResult
PolarDB_SessionUnitAccess::exercise_manual_non_polardb_route(
		PgSQL_Session* session, int hostgroup, bool extended) {
	const int saved_destination = session->qpo->destination_hostgroup;
	const int saved_replica_eligible = session->qpo->replica_eligible;
	const int saved_current_hostgroup = session->current_hostgroup;
	const int saved_locked_hostgroup = session->locked_on_hostgroup;
	const uint8_t saved_phase = session->extended_query_phase;

	const auto& counters = session->thread->polardb_status_variables.stvar;
	const unsigned long long manual_total_before =
		counters[polardb_st_var_route_manual_total];
	const unsigned long long manual_other_before =
		counters[polardb_st_var_route_manual_other];
	const unsigned long long reconciled_before =
		counters[polardb_st_var_txn_wait_reader_reconciled];

	session->qpo->destination_hostgroup = hostgroup;
	session->qpo->replica_eligible = -1;
	session->current_hostgroup = hostgroup;
	session->locked_on_hostgroup = -1;
	session->extended_query_phase = extended
		? EXTQ_PHASE_PROCESSING_EXECUTE : EXTQ_PHASE_IDLE;
	session->polardb_query.request_writer_scope = PolarDB_WriterScope{777, 99};
	session->polardb_query.reader_plan.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	session->polardb_query.reader_wait_spec = PolarDB_WaitSpec::from_lsn(
		0x7710, POLARDB_DEFAULT_WAIT_TIMEOUT_MS, PolarDB_WaitMode::STRICT);

	// A backend shell is enough to exercise the real stale-reader terminal path:
	// no connection is returned, but ownership and active state are cleared.
	PgSQL_Backend stale_reader;
	session->polardb_txn_reader.backend = &stale_reader;
	session->polardb_txn_reader.wait_read_active = true;

	PtrSize_t pkt{0, nullptr};
	bool handled = false;
	if (extended) {
		session->extended_query_frame_state.complete();
		session->extended_query_frame_state.begin(
			hostgroup, false, true, true);
		handled = session->apply_extended_backend_route(&pkt) ==
			PgSQL_Session::QpoHandlerResult::ERROR;
	} else {
		handled = session->polardb_route_query(pkt, false);
	}
	const ManualRouteResult result{
		!handled && session->current_hostgroup == hostgroup &&
			(!extended ||
			 !session->extended_query_frame_state.route_required()),
		!session->polardb_query.request_writer_scope.valid(),
		session->polardb_query.reader_plan.read_target ==
			static_cast<int>(PolarDB_ReadTarget::PRIMARY) &&
			!session->polardb_query.reader_wait_spec.has_wait(),
		!session->polardb_txn_reader.wait_read_active &&
			session->polardb_txn_reader.backend == nullptr &&
			counters[polardb_st_var_txn_wait_reader_reconciled] ==
				reconciled_before + 1,
		counters[polardb_st_var_route_manual_total] - manual_total_before,
		counters[polardb_st_var_route_manual_other] - manual_other_before};

	// Never leave a test-owned stack address in session state, even on failure.
	session->polardb_txn_reader.clear_request_state();
	session->polardb_txn_reader.clear_backend();
	session->qpo->destination_hostgroup = saved_destination;
	session->qpo->replica_eligible = saved_replica_eligible;
	session->current_hostgroup = saved_current_hostgroup;
	session->locked_on_hostgroup = saved_locked_hostgroup;
	session->extended_query_phase = saved_phase;
	if (extended) {
		session->extended_query_frame_state.complete();
	}
	return result;
}

PolarDB_SessionUnitAccess::ExtendedFrameInvariantResult
PolarDB_SessionUnitAccess::exercise_extended_frame_invariants(
		PgSQL_Session* session, int hostgroup) {
	session->complete_extended_query_frame();
	const bool candidate_underflow_rejected =
		!session->note_extended_backend_candidate();

	auto& frame = session->extended_query_frame_state;
	frame.batch_classification_latched = true;
	frame.batch_classify_route = true;
	frame.batch_last_was_portal_describe = true;
	frame.note_execute(true);
	const bool queue_shape_failed_closed = frame.batch_requires_writer(false);
	const bool candidate_balance_repaired =
		frame.backend_candidates_remaining == 1;

	frame.complete();
	frame.begin(hostgroup, false, false, true);
	const bool initial_owner =
		session->note_extended_backend_command_sent(hostgroup);
	const bool command_owner_mismatch_rejected = initial_owner &&
		!session->note_extended_backend_command_sent(hostgroup + 1);
	const bool sync_owner_mismatch_rejected =
		!session->note_extended_backend_sync_sent(hostgroup + 1);
	session->complete_extended_query_frame();
	return ExtendedFrameInvariantResult{
		candidate_underflow_rejected,
		queue_shape_failed_closed,
		candidate_balance_repaired,
		command_owner_mismatch_rejected,
		sync_owner_mismatch_rejected};
}

PolarDB_SessionUnitAccess::PinnedFrameConflictResult
PolarDB_SessionUnitAccess::exercise_pinned_frame_conflict(
		PgSQL_Session* session, int hostgroup) {
	init_test_frontend_protocol(session);
	session->complete_extended_query_frame();
	drain_frontend_messages(session);
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->extended_query_frame_state.begin(
		hostgroup, false, false, true);
	session->current_hostgroup = hostgroup;
	const bool initial_owner =
		session->note_extended_backend_command_sent(hostgroup);
	const bool conflicting_selection_rejected =
		!session->select_extended_frame_hostgroup(hostgroup + 1);
	const int rc = session->reject_extended_frame_hostgroup(hostgroup + 1);
	session->reset_extended_query_frame();
	const auto flush = drain_frontend_messages(session);
	const bool waits_for_client_sync = session->extended_query_frame_state
		.waiting_for_client_sync_after_error;

	// Model the real dispatcher boundary: rc=2 discarded the remaining frame,
	// backend pipeline Sync completed, and only the later client Sync is allowed
	// to publish ReadyForQuery.
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->note_extended_backend_sync_sent(hostgroup);
	if (session->emit_extended_ready_for_query(nullptr)) {
		session->complete_extended_query_frame();
	}
	const auto sync = drain_frontend_messages(session);
	session->complete_extended_query_frame();
	return PinnedFrameConflictResult{
		initial_owner && conflicting_selection_rejected && rc == 2,
		flush.sqlstate == "P0001",
		flush.types == "E",
		waits_for_client_sync,
		sync.types == "Z"};
}

bool PolarDB_SessionUnitAccess::automatic_reader_route_preserves_writer_lock(
		PgSQL_Session* session, int writer_hostgroup, int reader_hostgroup) {
	const int saved_lock_setting = pgsql_thread___set_query_lock_on_hostgroup;
	const int saved_lock = session->locked_on_hostgroup;
	const int saved_current = session->current_hostgroup;
	pgsql_thread___set_query_lock_on_hostgroup = 1;
	session->locked_on_hostgroup = -1;
	session->current_hostgroup = writer_hostgroup;
	const bool accepted = session->enforce_extended_query_hostgroup_lock(
		true, "SELECT 1", 8);
	// PolarDB routing may change only the operation destination. The durable
	// lock remains the QPO-selected writer hostgroup.
	session->current_hostgroup = reader_hostgroup;
	const bool preserved = accepted &&
		session->locked_on_hostgroup == writer_hostgroup;
	pgsql_thread___set_query_lock_on_hostgroup = saved_lock_setting;
	session->locked_on_hostgroup = saved_lock;
	session->current_hostgroup = saved_current;
	return preserved;
}

PolarDB_SessionUnitAccess::BoundPortalIdentityResult
PolarDB_SessionUnitAccess::exercise_bound_portal_identity(
		PgSQL_Session* session) {
	constexpr char bound_query[] = "INSERT INTO t VALUES (1)";
	constexpr char replacement_query[] = "SELECT 1";
	PgSQL_STMT_Manager* saved_stmt_manager = GloPgStmt;
	std::unique_ptr<PgSQL_STMT_Manager> scoped_stmt_manager;
	if (!GloPgStmt) {
		scoped_stmt_manager = std::make_unique<PgSQL_STMT_Manager>();
		GloPgStmt = scoped_stmt_manager.get();
	}
	std::shared_ptr<const PgSQL_STMT_Global_info> bound_stmt =
		GloPgStmt->add_prepared_statement(
			"unit", "unit", bound_query, strlen(bound_query),
			Parse_Param_Types{}, nullptr, nullptr, 0,
			PGSQL_QUERY_INSERT);
	std::shared_ptr<const PgSQL_STMT_Global_info> replacement_stmt =
		GloPgStmt->add_prepared_statement(
			"unit", "unit", replacement_query, strlen(replacement_query),
			Parse_Param_Types{}, nullptr, nullptr, 0,
			PGSQL_QUERY_SELECT);
	PgSQL_STMT_Local local_stmts(true);
	local_stmts.set_is_client(session);
	local_stmts.client_insert(bound_stmt, "");
	const long owners_before_bind = bound_stmt.use_count();
	auto bind = make_test_bind_message();
	session->bind_waiting_for_execute.capture(
		bind.release(), bound_stmt.get());
	const bool bind_did_not_copy_shared_owner =
		bound_stmt.use_count() == owners_before_bind;

	// Replacing the unnamed statement changes future Bind operations only. The
	// map owner moves into the portal, without a shared_ptr increment/decrement.
	local_stmts.client_insert(replacement_stmt, "");
	const bool replacement_transferred_existing_owner =
		session->bind_waiting_for_execute.retained_statement.get() ==
			bound_stmt.get() &&
		bound_stmt.use_count() == owners_before_bind;
	const auto* execute_stmt =
		session->bind_waiting_for_execute.statement_info();
	const uint64_t execute_statement_id =
		execute_stmt ? execute_stmt->statement_id : 0;
	session->bind_waiting_for_execute.reset();

	// Close has the same portal-lifetime rule as replacement: the already-bound
	// Execute keeps the old statement alive without adding an owner on Bind.
	const long owners_before_close = replacement_stmt.use_count();
	auto close_bind = make_test_bind_message();
	session->bind_waiting_for_execute.capture(
		close_bind.release(), replacement_stmt.get());
	local_stmts.client_close("");
	const bool close_transferred_existing_owner =
		session->bind_waiting_for_execute.retained_statement.get() ==
			replacement_stmt.get() &&
		session->bind_waiting_for_execute.statement_info() ==
			replacement_stmt.get() &&
		replacement_stmt.use_count() == owners_before_close;
	const BoundPortalIdentityResult result{
		bound_stmt->statement_id,
		replacement_stmt->statement_id,
		execute_statement_id,
		bind_did_not_copy_shared_owner,
		replacement_transferred_existing_owner,
		close_transferred_existing_owner};
	session->bind_waiting_for_execute.reset();
	GloPgStmt->ref_count_server(bound_stmt.get(), -1);
	GloPgStmt->ref_count_server(replacement_stmt.get(), -1);
	bound_stmt.reset();
	replacement_stmt.reset();
	scoped_stmt_manager.reset();
	GloPgStmt = saved_stmt_manager;
	return result;
}

PolarDB_SessionUnitAccess::PoisonedExtendedFrameResult
PolarDB_SessionUnitAccess::exercise_poisoned_extended_frame(
		PgSQL_Session* session, int hostgroup) {
	init_test_frontend_protocol(session);
	const bool saved_tx_poisoned = session->tx_poisoned;
	session->tx_poisoned = true;
	session->complete_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->extended_query_frame_state.begin(
		hostgroup, false, false, false);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	session->emit_tx_poisoned_response("unit backend failure");
	const auto flush_output = drain_frontend_message_types(session);
	const bool flush_waits_for_client_sync =
		session->extended_query_frame_state.waiting_for_client_sync_after_error;

	// The failed backend has been released before the later client Sync.
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->extended_query_frame_state.mark_backend_sync_sent(true);
	const bool sync_ready = session->emit_extended_ready_for_query(nullptr);
	if (sync_ready) {
		session->complete_extended_query_frame();
	}
	const auto sync_output = drain_frontend_message_types(session);
	const bool sync_completed =
		!session->extended_query_frame_state.active() &&
		session->extended_query_phase == EXTQ_PHASE_IDLE;

	// If client Sync was already part of the frame, poisoning completes the
	// same boundary immediately and cannot leave a second RFQ owner behind.
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	session->emit_tx_poisoned_response("unit backend failure");
	const auto immediate_output = drain_frontend_message_types(session);

	const PoisonedExtendedFrameResult result{
		flush_output.first == "EN",
		flush_waits_for_client_sync,
		sync_ready && sync_output.first == "Z" && sync_output.second == 'E',
		sync_completed,
		immediate_output.first == "ENZ" && immediate_output.second == 'E' &&
			!session->extended_query_frame_state.active()};
	session->tx_poisoned = saved_tx_poisoned;
	return result;
}

bool PolarDB_SessionUnitAccess::locked_non_polardb_route_clears_stale_scope(
		PgSQL_Session* session, int hostgroup) {
	const PolarDB_WriterScope sentinel{777, 99};
	session->polardb_query.request_writer_scope = sentinel;
	session->current_hostgroup = hostgroup;
	session->locked_on_hostgroup = hostgroup;
	const uint64_t counter_before =
		session->thread->polardb_status_variables.stvar[
			polardb_st_var_route_locked_hostgroup];
	const PolarDB_HG_ConfigSnapshot non_polardb_config;
	const bool handled = session->polardb_handle_locked_hostgroup_route(
		"UNIT", false, nullptr, non_polardb_config);
	const bool reset_without_accounting =
		!session->polardb_query.request_writer_scope.valid() &&
		session->thread->polardb_status_variables.stvar[
			polardb_st_var_route_locked_hostgroup] == counter_before;
	session->locked_on_hostgroup = -1;
	return handled && reset_without_accounting;
}

void PolarDB_SessionUnitAccess::set_worker_polardb_active(
		PgSQL_Thread* worker, bool active) {
	worker->polardb_active_cache = active;
	if (PgHGM) {
		worker->polardb_activation_generation_cache =
			PgHGM->status.polardb_activation_generation.load(
				std::memory_order_relaxed);
	}
}

bool PolarDB_SessionUnitAccess::exercise_worker_polardb_publication(
		PgSQL_Thread* worker) {
	const bool original = PgHGM->status.polardb_active.load(
		std::memory_order_acquire);
	const bool original_cache = worker->polardb_active_cache;
	const uint64_t original_generation =
		PgHGM->status.polardb_activation_generation.load(
			std::memory_order_acquire);
	const uint64_t original_generation_cache =
		worker->polardb_activation_generation_cache;
	const unsigned int original_previous =
		worker->servers_table_version_previous;
	const unsigned int original_current =
		worker->servers_table_version_current;
	worker->servers_table_version_previous = 41;
	worker->servers_table_version_current = 42;
	const bool published = !original_cache;
	const uint64_t published_generation = original_generation +
		(published && !original_cache ? 1 : 0);
	PgHGM->status.polardb_activation_generation.store(
		published_generation, std::memory_order_relaxed);
	PgHGM->status.polardb_active.store(published, std::memory_order_release);
	__sync_add_and_fetch(&PgHGM->status.servers_table_version, 1);
	worker->on_pipe_wakeup(0);
	const bool observed_publication =
		worker->polardb_active_cache == published &&
		worker->polardb_activation_generation_cache == published_generation &&
		worker->servers_table_version_previous == 41 &&
		worker->servers_table_version_current == 42;

	PgHGM->status.polardb_activation_generation.store(
		original_generation, std::memory_order_relaxed);
	PgHGM->status.polardb_active.store(original, std::memory_order_release);
	__sync_add_and_fetch(&PgHGM->status.servers_table_version, 1);
	worker->on_pipe_wakeup(PgSQL_Thread::TOPOLOGY_PUBLICATION_WAKE);
	const bool restored_publication =
		worker->polardb_active_cache == original &&
		worker->polardb_activation_generation_cache == original_generation &&
		worker->servers_table_version_previous == 41 &&
		worker->servers_table_version_current == 42;
	const unsigned int published_version =
		PgHGM->get_servers_table_version();
	worker->refresh_hgm_publication(true);
	const bool maintenance_rotated_once =
		worker->servers_table_version_previous == 42 &&
		worker->servers_table_version_current == published_version;
	worker->servers_table_version_previous = original_previous;
	worker->servers_table_version_current = original_current;
	worker->polardb_activation_generation_cache = original_generation_cache;
	return observed_publication && restored_publication &&
		maintenance_rotated_once;
}

PolarDB_SessionUnitAccess::ExtendedFrameActivityTransitionResult
PolarDB_SessionUnitAccess::exercise_extended_frame_activity_transition(
		PgSQL_Session* session, bool initially_active,
		bool active_at_execute) {
	const bool original = session->thread->polardb_active_cache;
	session->reset_extended_query_frame();
	session->previous_hostgroup = 10;
	session->thread->polardb_active_cache = initially_active;
	queue_test_bind(session);
	queue_test_describe(session, 'P');
	session->thread->polardb_active_cache = active_at_execute;
	queue_test_execute(session);
	session->begin_extended_query_frame(true);
	const ExtendedFrameActivityTransitionResult result{
		session->extended_query_frame_state.backend_route_pending,
		session->extended_query_frame_state.backend_candidates_remaining,
		session->extended_query_frame_state.writer_required};
	session->reset_extended_query_frame();
	session->thread->polardb_active_cache = original;
	return result;
}

PolarDB_SessionUnitAccess::ExtendedFrameRouteResult
PolarDB_SessionUnitAccess::classify_bind_parse_execute(
		PgSQL_Session* session, int previous_hostgroup) {
	session->reset_extended_query_frame();
	session->previous_hostgroup = previous_hostgroup;
	queue_test_bind(session);
	queue_test_parse(session);
	queue_test_execute(session);
	session->begin_extended_query_frame(true);
	return ExtendedFrameRouteResult{
		session->extended_query_frame_state.backend_route_pending,
		session->extended_query_frame_state.backend_candidates_remaining,
		session->extended_query_frame_state.writer_required};
}

PolarDB_SessionUnitAccess::ExtendedFrameRouteResult
PolarDB_SessionUnitAccess::classify_extended_frame(
		PgSQL_Session* session, int previous_hostgroup,
		unsigned int parse_count, unsigned int execute_count,
		bool has_backend_metadata,
		bool has_elided_portal_describe) {
	session->reset_extended_query_frame();
	session->previous_hostgroup = previous_hostgroup;
	for (unsigned int i = 0; i < parse_count; ++i) {
		queue_test_parse(session);
	}
	if (parse_count == 1 && execute_count != 0) {
		queue_test_bind(session);
	}
	if (has_backend_metadata) {
		queue_test_describe(session, 'S');
	}
	unsigned int queued_executes = 0;
	if (has_elided_portal_describe) {
		queue_test_describe(session, 'P');
		if (execute_count != 0) {
			queue_test_execute(session);
			queued_executes = 1;
		}
	}
	for (unsigned int i = queued_executes; i < execute_count; ++i) {
		queue_test_execute(session);
	}
	session->begin_extended_query_frame(true);
	return ExtendedFrameRouteResult{
		session->extended_query_frame_state.backend_route_pending,
		session->extended_query_frame_state.backend_candidates_remaining,
		session->extended_query_frame_state.writer_required};
}

PolarDB_SessionUnitAccess::ExtendedBackendRouteResult
PolarDB_SessionUnitAccess::apply_extended_backend_route(
		PgSQL_Session* session) {
	PtrSize_t pkt{0, nullptr};
	const auto result = session->apply_extended_backend_route(&pkt);
	return ExtendedBackendRouteResult{
		result == PgSQL_Session::QpoHandlerResult::CONTINUE,
		!session->extended_query_frame_state.route_required()};
}

PolarDB_SessionUnitAccess::ExtendedFrameSyncResult
PolarDB_SessionUnitAccess::exercise_extended_frame_sync(
		PgSQL_Session* session, int hostgroup) {
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_PROCESSING_EXECUTE;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	const bool ready_before_backend =
		session->is_extended_query_ready_for_query();
	session->current_hostgroup = hostgroup;
	assert(session->can_claim_extended_frame_backend());
	session->note_extended_backend_command_sent(hostgroup);
	const bool ready_after_flush =
		session->is_extended_query_ready_for_query();
	const bool rfq_pending_after_flush =
		session->extended_query_frame_state.needs_backend_sync();
	session->note_extended_backend_sync_sent(hostgroup);
	const bool ready_after_sync =
		session->is_extended_query_ready_for_query();
	const bool rfq_pending_after_sync =
		session->extended_query_frame_state.needs_backend_sync();
	session->complete_extended_query_frame();
	return ExtendedFrameSyncResult{
		ready_before_backend, ready_after_flush, ready_after_sync,
		rfq_pending_after_flush, rfq_pending_after_sync};
}

PolarDB_SessionUnitAccess::DeferredRfqResult
PolarDB_SessionUnitAccess::exercise_deferred_extended_rfq(
		PgSQL_Session* session, int writer_hostgroup, uint64_t writer_epoch) {
	session->polardb_extended_rfq.reset();
	session->polardb_session_consistency.write_unknown = false;
	session->complete_extended_query_frame();
	session->extended_query_frame_state.begin(
		writer_hostgroup, false, false, true);
	session->extended_query_phase = EXTQ_PHASE_BUILDING;
	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, writer_epoch};
	session->polardb_query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	session->polardb_query.profile_enabled = true;
	session->polardb_query.wait_bypass_target = 0x5100;
	session->polardb_defer_extended_rfq_result(
		nullptr, "UPDATE t SET v = 1", PGSQL_QUERY_UPDATE);
	const bool pending_after_first_execute =
		session->polardb_extended_rfq.pending;
	const bool publication_pending_after_first_execute =
		session->polardb_extended_rfq.publication_pending;

	// A later successful Execute in the same backend cycle contributes its
	// target, but the final Sync RFQ still owns publication for both operations.
	session->polardb_query.wait_bypass_target = 0x5200;
	session->polardb_defer_extended_rfq_result(
		nullptr, "SELECT v FROM t", PGSQL_QUERY_SELECT);
	// A proxy-local RESET is one statement inside the still-open frame. It may
	// clear current-query staging, but the earlier Flush result belongs to Sync.
	session->polardb_clear_staged_wait_state_for_reset(
		/*reset_override=*/false);
	const bool staged_reset_preserved_frame_state =
		session->polardb_extended_rfq.pending &&
		session->polardb_extended_rfq.publication_pending &&
		session->polardb_extended_rfq.saw_write &&
		session->polardb_extended_rfq.confirmed_read_target == 0x5200;
	session->polardb_prepare_deferred_extended_rfq();
	const bool write_attribution_preserved =
		session->polardb_extended_rfq.saw_write &&
		!session->polardb_extended_rfq.can_preserve_session_lsn;
	const bool writer_scope_preserved =
		session->polardb_query.request_writer_scope.matches(
			PolarDB_WriterScope{writer_hostgroup, writer_epoch});
	const bool wait_target_preserved =
		session->polardb_query.wait_bypass_target == 0x5200;
	session->polardb_process_deferred_extended_rfq(nullptr);
	session->polardb_complete_deferred_extended_rfq_publication();
	const bool direct_publication_cleared_after_result =
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.publication_pending;
	session->complete_extended_query_frame();

	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, writer_epoch};
	session->polardb_query.profile_enabled = true;
	session->polardb_defer_extended_rfq_result(
		nullptr, "UPDATE t SET v = 2", PGSQL_QUERY_UPDATE);
	session->polardb_abandon_deferred_extended_rfq();
	const bool unknown_after_abandon =
		session->polardb_session_consistency.write_unknown;
	const bool state_reset_after_abandon =
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.publication_pending &&
		!session->polardb_extended_rfq.saw_write;

	session->polardb_session_consistency.write_unknown = false;
	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, writer_epoch};
	session->polardb_query.profile_enabled = true;
	session->polardb_defer_extended_rfq_result(
		nullptr, "UPDATE t SET v = 3", PGSQL_QUERY_UPDATE);
	session->polardb_clear_staged_wait_state_for_reset(
		/*reset_override=*/true);
	const bool inactive_reset_abandoned_orphan =
		session->polardb_session_consistency.write_unknown &&
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.publication_pending;

	// An error has no successful-result attribution, but its backend RFQ still
	// needs the request policy when Flush defers publication to client Sync.
	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, writer_epoch};
	session->polardb_query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	session->polardb_query.profile_enabled = true;
	session->polardb_query.wait_bypass_target = 0x5300;
	session->polardb_retain_extended_rfq_publication(nullptr);
	session->polardb_query.reset_for_new_query();
	const bool publication_was_pending =
		session->polardb_prepare_deferred_extended_rfq();
	const bool error_publication_policy_preserved =
		publication_was_pending &&
		!session->polardb_extended_rfq.pending &&
		session->polardb_query.profile_enabled &&
		session->polardb_query.request_writer_scope.matches(
			PolarDB_WriterScope{writer_hostgroup, writer_epoch}) &&
		session->polardb_query.wait_bypass_target == 0x5300;
	session->polardb_complete_deferred_extended_rfq_publication();
	const bool error_publication_cleared_after_emit =
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.publication_pending &&
		!session->polardb_extended_rfq.profile_enabled;
	session->polardb_query.profile_enabled = true;
	session->polardb_retain_extended_rfq_publication(nullptr);
	session->polardb_clear_request_state_for_query_end(
		nullptr, /*called_on_failure=*/true);
	const bool error_publication_abandoned_on_failure =
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.publication_pending;

	// A backend ErrorResponse in an Execute+Flush cycle differs from an orphaned
	// failure: client Sync still owns one RFQ. Preserve only its publication
	// policy, while abandoning prior successful write attribution conservatively.
	session->complete_extended_query_frame();
	session->polardb_session_consistency.write_unknown = false;
	session->extended_query_frame_state.begin(
		writer_hostgroup, false, false, true);
	session->extended_query_phase = EXTQ_PHASE_PROCESSING_EXECUTE |
		EXTQ_PHASE_EXECUTING_SYNC_IMPLICIT;
	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, writer_epoch};
	session->polardb_query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	session->polardb_query.profile_enabled = true;
	session->polardb_query.wait_bypass_target = 0x5400;
	session->polardb_defer_extended_rfq_result(
		nullptr, "UPDATE t SET v = 4", PGSQL_QUERY_UPDATE);
	session->note_extended_backend_error_response(nullptr);
	session->polardb_clear_request_state_for_query_end(
		nullptr, /*called_on_failure=*/true);
	const bool error_sync_result_attribution_discarded =
		!session->polardb_extended_rfq.pending &&
		!session->polardb_extended_rfq.saw_write &&
		session->polardb_session_consistency.write_unknown;
	const bool error_sync_policy_pending =
		session->polardb_extended_rfq.publication_pending &&
		session->extended_query_frame_state.waiting_for_client_sync_after_error;
	const bool error_sync_policy_prepared =
		session->polardb_prepare_deferred_extended_rfq();
	const bool error_sync_publication_preserved =
		error_sync_policy_pending && error_sync_policy_prepared &&
		session->polardb_query.profile_enabled &&
		session->polardb_query.request_writer_scope.matches(
			PolarDB_WriterScope{writer_hostgroup, writer_epoch}) &&
		session->polardb_query.wait_bypass_target == 0x5400;
	session->polardb_complete_deferred_extended_rfq_publication();
	session->complete_extended_query_frame();
	return DeferredRfqResult{
		pending_after_first_execute,
		publication_pending_after_first_execute,
		write_attribution_preserved,
		writer_scope_preserved,
		wait_target_preserved,
		staged_reset_preserved_frame_state,
		direct_publication_cleared_after_result,
		unknown_after_abandon,
		state_reset_after_abandon,
		inactive_reset_abandoned_orphan,
		error_publication_policy_preserved,
		error_publication_cleared_after_emit,
		error_publication_abandoned_on_failure,
		error_sync_publication_preserved,
		error_sync_result_attribution_discarded};
}

PolarDB_SessionUnitAccess::DeferredScopeMismatchResult
PolarDB_SessionUnitAccess::exercise_deferred_scope_mismatch(
		PgSQL_Session* session, PgSQL_Data_Stream* backend_myds,
		PgSQL_Connection* backend_conn, int writer_hostgroup,
		uint64_t current_epoch, uint64_t backend_lsn,
		uint64_t stale_target) {
	auto stage_mismatched_scopes = [&]() {
		session->polardb_extended_rfq.reset();
		session->polardb_query.reset_for_new_query();
		session->polardb_route_state.client_rfq_lsn_requested = true;
		session->status = PROCESSING_STMT_EXECUTE;
		session->polardb_query.profile_enabled = true;
		session->polardb_query.request_writer_scope =
			PolarDB_WriterScope{writer_hostgroup, current_epoch + 1};
		session->polardb_query.wait_bypass_target = stale_target;
		session->polardb_retain_extended_rfq_publication(backend_myds);

		session->polardb_query.reset_for_new_query();
		session->polardb_query.profile_enabled = true;
		session->polardb_query.request_writer_scope =
			PolarDB_WriterScope{writer_hostgroup, current_epoch};
		session->polardb_retain_extended_rfq_publication(backend_myds);
	};

	stage_mismatched_scopes();
	const bool aggregate_target_invalidated =
		!session->polardb_extended_rfq.scope_consistent &&
		!session->polardb_extended_rfq.writer_scope.valid() &&
		session->polardb_extended_rfq.confirmed_read_target == 0;
	uint64_t client_lsn = 0;
	const bool direct_rfq_uses_current_backend_lsn =
		session->polardb_prepare_client_ready_lsn(
			backend_conn, true, backend_lsn, &client_lsn,
			PolarDB_DeferredRfqPolicy::DETECT) &&
		client_lsn == backend_lsn;

	stage_mismatched_scopes();
	const bool deferred_policy_prepared =
		session->polardb_prepare_deferred_extended_rfq();
	client_lsn = 0;
	const bool standalone_sync_uses_current_backend_lsn =
		deferred_policy_prepared &&
		session->polardb_prepare_client_ready_lsn(
			backend_conn, true, backend_lsn, &client_lsn,
			PolarDB_DeferredRfqPolicy::PREPARED) &&
			client_lsn == backend_lsn;
	session->polardb_complete_deferred_extended_rfq_publication();

	session->polardb_extended_rfq.reset();
	session->polardb_query.reset_for_new_query();
	session->polardb_route_state.client_rfq_lsn_requested = true;
	session->status = PROCESSING_STMT_EXECUTE;
	session->polardb_query.profile_enabled = true;
	session->polardb_query.request_writer_scope =
		PolarDB_WriterScope{writer_hostgroup, current_epoch};
	session->polardb_retain_extended_rfq_publication(backend_myds);
	// ErrorResponse and its recovery RFQ are separate lifecycle boundaries.
	// Request cleanup may clear the live query state before internal Sync exposes
	// the exact RFQ values, so retained policy must bridge that interval.
	session->polardb_query.reset_for_new_query();
	session->polardb_retain_extended_rfq_evidence(backend_myds);
	const bool retained_policy_prepared =
		session->polardb_prepare_deferred_extended_rfq();
	client_lsn = 0;
	const bool released_backend_sync_uses_retained_lsn =
		retained_policy_prepared &&
		session->polardb_prepare_client_ready_lsn(
			nullptr, false, 0, &client_lsn,
			PolarDB_DeferredRfqPolicy::PREPARED) &&
		client_lsn == backend_lsn;
	unit_parse_rfq_lsn(backend_conn->pgsql_conn, backend_lsn + 1);
	client_lsn = 0;
	const bool attached_backend_sync_uses_retained_lsn =
		retained_policy_prepared &&
		session->polardb_prepare_client_ready_lsn(
			backend_conn, true, backend_lsn + 1, &client_lsn,
			PolarDB_DeferredRfqPolicy::PREPARED) &&
		client_lsn == backend_lsn;
	unit_parse_rfq_lsn(backend_conn->pgsql_conn, backend_lsn);
	session->polardb_complete_deferred_extended_rfq_publication();

	stage_mismatched_scopes();
	init_test_frontend_protocol(session);
	const int backend_hostgroup = backend_conn && backend_conn->parent &&
		backend_conn->parent->myhgc
		? static_cast<int>(backend_conn->parent->myhgc->hid)
		: -1;
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		backend_hostgroup, false, true, false);
	session->note_extended_backend_command_sent(backend_hostgroup);
	session->extended_query_frame_state.mark_backend_sync_sent(true);
	const bool emitted = session->emit_extended_ready_for_query(backend_conn);
	const auto emitted_rfq = drain_frontend_messages(session);
	const bool emitter_uses_current_backend_lsn = emitted &&
		emitted_rfq.types == "Z" && emitted_rfq.last_rfq_has_lsn &&
		emitted_rfq.last_rfq_lsn == backend_lsn;
	session->complete_extended_query_frame();
	session->polardb_extended_rfq.reset();
	session->polardb_query.reset_for_new_query();

	return DeferredScopeMismatchResult{
		aggregate_target_invalidated,
		direct_rfq_uses_current_backend_lsn,
		standalone_sync_uses_current_backend_lsn,
		released_backend_sync_uses_retained_lsn,
		attached_backend_sync_uses_retained_lsn,
		emitter_uses_current_backend_lsn};
}

bool PolarDB_SessionUnitAccess::sticky_frame_outranks_delayed_multiplex(
		PgSQL_Session* session) {
	if (!session || !session->thread) {
		return false;
	}
	PgSQL_Data_Stream backend_myds;
	backend_myds.init(MYDS_BACKEND, session, -1);
	PgSQL_Connection* backend_conn = new PgSQL_Connection(false);
	backend_myds.myconn = backend_conn;
	backend_conn->myds = &backend_myds;
	backend_conn->reusable = true;
	backend_conn->async_state_machine = ASYNC_IDLE;

	const bool saved_multiplexing = pgsql_thread___multiplexing;
	const int saved_delay = pgsql_thread___connection_delay_multiplex_ms;
	const int saved_auto_delay =
		pgsql_thread___auto_increment_delay_multiplex_timeout_ms;
	pgsql_thread___multiplexing = true;
	pgsql_thread___connection_delay_multiplex_ms = 10;
	pgsql_thread___auto_increment_delay_multiplex_timeout_ms = 0;
	session->finishQuery(&backend_myds, backend_conn,
		/*sticky_backend_connection=*/true);
	const bool retained = backend_myds.myconn == backend_conn &&
		backend_conn->myds == &backend_myds &&
		!backend_conn->multiplex_delayed &&
		backend_myds.wait_until == 0 &&
		backend_myds.DSS == STATE_MARIADB_GENERIC;

	pgsql_thread___multiplexing = saved_multiplexing;
	pgsql_thread___connection_delay_multiplex_ms = saved_delay;
	pgsql_thread___auto_increment_delay_multiplex_timeout_ms = saved_auto_delay;
	backend_myds.myconn = nullptr;
	backend_conn->myds = nullptr;
	delete backend_conn;
	return retained;
}

PolarDB_SessionUnitAccess::ExtendedFrameBatchPinResult
PolarDB_SessionUnitAccess::exercise_extended_frame_batch_pinning(
		PgSQL_Session* session, int hostgroup) {
	session->reset_extended_query_frame();
	session->extended_query_frame_state.note_parse("", false);
	session->extended_query_frame_state.begin(
		hostgroup, false, false, true);
	session->extended_query_frame_state.consume_backend_candidate();
	session->extended_query_frame_state.consume_backend_route();
	session->current_hostgroup = hostgroup;
	assert(session->can_claim_extended_frame_backend());
	session->note_extended_backend_command_sent(hostgroup);

	// A second Flush batch belongs to the already-open backend pipeline.
	session->extended_query_frame_state.note_parse("", false);
	session->extended_query_frame_state.begin(
		hostgroup, true, false, true);
	const bool reroute_required =
		session->extended_query_frame_state.route_required();
	const bool backend_pinned =
		session->extended_query_frame_state.backend_hostgroup == hostgroup;
	session->current_hostgroup = hostgroup + 1;
	const bool backend_switch_rejected =
		!session->can_claim_extended_frame_backend() &&
		!session->select_extended_frame_hostgroup(hostgroup + 1);
	session->complete_extended_query_frame();

	// If the first batch completed entirely in ProxySQL, no backend was pinned.
	// The next batch can classify its first actual backend operation normally.
	session->extended_query_frame_state.note_parse("", false);
	session->extended_query_frame_state.begin(
		hostgroup, true, false, true);
	session->extended_query_frame_state.consume_backend_candidate();
	session->extended_query_frame_state.note_parse("", false);
	session->extended_query_frame_state.begin(
		hostgroup, false, false, true);
	const bool local_reclassified =
		session->extended_query_frame_state.route_required() &&
		!session->extended_query_frame_state.writer_required;
	session->complete_extended_query_frame();

	return ExtendedFrameBatchPinResult{
		reroute_required, backend_pinned, backend_switch_rejected,
		local_reclassified};
}

PolarDB_SessionUnitAccess::ExtendedFrameErrorRecoveryResult
PolarDB_SessionUnitAccess::exercise_extended_frame_error_recovery(
		PgSQL_Session* session) {
	init_test_frontend_protocol(session);
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_PROCESSING_EXECUTE;
	session->extended_query_frame_state.note_parse("", false);
	session->extended_query_frame_state.begin(
		10, false, false, true);
	session->extended_query_frame_state.consume_backend_candidate();
	session->client_myds->setDSS_STATE_QUERY_SENT_NET();
	session->client_myds->myprot.generate_error_packet(
		true, false, "unit extended frame error",
		PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION, false, true);

	const bool waits = session->extended_query_frame_state
		.waiting_for_client_sync_after_error;
	const bool discards_parse =
		session->extended_query_frame_state.discards_until_sync('P');
	const bool discards_flush =
		session->extended_query_frame_state.discards_until_sync('H');
	const bool accepts_sync =
		!session->extended_query_frame_state.discards_until_sync('S');
	const bool accepts_terminate =
		!session->extended_query_frame_state.discards_until_sync('X');
	const bool ready_before_sync =
		session->extended_query_frame_state.ready_for_query();

	session->extended_query_frame_state.begin(
		10, false, true, false);
	const bool ready_after_sync =
		session->extended_query_frame_state.ready_for_query();
	session->complete_extended_query_frame();
	const bool cleared =
		!session->extended_query_frame_state.active() &&
		!session->extended_query_frame_state
			.waiting_for_client_sync_after_error;

	return ExtendedFrameErrorRecoveryResult{
		waits, discards_parse, discards_flush, accepts_sync,
		accepts_terminate, ready_before_sync, ready_after_sync, cleared};
}

PolarDB_SessionUnitAccess::ExtendedReadyOwnershipResult
PolarDB_SessionUnitAccess::exercise_extended_ready_ownership(
		PgSQL_Session* session, int hostgroup) {
	session->reset_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	const bool backend_error_deferred =
		!session->extended_backend_result_sends_ready(true);
	session->note_extended_backend_sync_sent(hostgroup);
	const bool backend_ready_once =
		session->extended_backend_result_sends_ready(true);
	const bool backend_ready_not_duplicated =
		!session->extended_backend_result_sends_ready(true);
	session->complete_extended_query_frame();

	// A standalone resync result is discarded internally. It must leave RFQ
	// ownership for the session boundary that observes completed backend Sync.
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->current_hostgroup = hostgroup;
	session->note_extended_backend_command_sent(hostgroup);
	session->note_extended_backend_sync_sent(hostgroup);
	const bool control_resync_deferred =
		!session->extended_query_frame_state.frontend_ready_sent();
	const bool control_resync_ready_once =
		session->emit_extended_ready_for_query(nullptr) &&
		!session->emit_extended_ready_for_query(nullptr);
	session->complete_extended_query_frame();

	// Ordinary non-PolarDB Sync frames deliberately do not activate durable
	// frame state, but local errors still own exactly one frontend RFQ.
	session->extended_query_phase = EXTQ_PHASE_EXECUTING_SYNC_CLIENT;
	const bool local_ready_once =
		session->extended_local_result_sends_ready(true);
	const bool local_ready_not_duplicated =
		!session->extended_local_result_sends_ready(true);
	session->complete_extended_query_frame();

	// Idle local results are Simple/local protocol responses. Each owns its RFQ
	// directly and must not latch state into the next request.
	const bool idle_local_ready_each_time =
		session->extended_local_result_sends_ready(true) &&
		session->extended_local_result_sends_ready(false);
	const bool idle_local_does_not_claim_frame =
		!session->extended_query_frame_state.active() &&
		!session->extended_query_frame_state.frontend_ready_sent() &&
		!session->extended_query_frame_state
			.waiting_for_client_sync_after_error;

	session->extended_query_frame_state.begin(
		hostgroup, false, true, false);
	session->polardb_query.reader_wait_spec =
		PolarDB_WaitSpec::from_lsn(123, 1000, PolarDB_WaitMode::STRICT);
	PgSQL_Extended_Query_Info describe_info{};
	session->mark_extended_backend_dispatch(describe_info,
		/*snapshot_capable=*/false);
	const bool describe_does_not_stage_wait =
		(describe_info.flags & PGSQL_EXTENDED_QUERY_FLAG_TRACK_FRAME_BACKEND) &&
		!(describe_info.flags & PGSQL_EXTENDED_QUERY_FLAG_PREPARE_POLAR_WAIT);
	session->polardb_query.reader_wait_spec.reset();
	session->complete_extended_query_frame();
	session->extended_query_phase = EXTQ_PHASE_IDLE;

	return ExtendedReadyOwnershipResult{
		backend_error_deferred, backend_ready_once,
		backend_ready_not_duplicated, control_resync_deferred,
		control_resync_ready_once, local_ready_once,
		local_ready_not_duplicated, idle_local_ready_each_time,
		idle_local_does_not_claim_frame, describe_does_not_stage_wait};
}

int PolarDB_SessionUnitAccess::extended_frame_hostgroup(
		const PgSQL_Session* session) {
	return session->extended_query_frame_state.hostgroup;
}

int PolarDB_SessionUnitAccess::extended_frame_hostgroup_or_previous(
		const PgSQL_Session* session) {
	return session->extended_frame_hostgroup_or_previous();
}

void PolarDB_SessionUnitAccess::apply_extended_hostgroup_lock(
		PgSQL_Session* session, int hostgroup) {
	session->locked_on_hostgroup = hostgroup;
}

void PolarDB_SessionUnitAccess::reset_extended_frame_state(
		PgSQL_Session* session) {
	session->complete_extended_query_frame();
}
