/**
 * @file polardb_session_unit_access.cpp
 * @brief Test-only access to PgSQL session routing state.
 *
 * Keep this translation unit free of libpq-private headers: PostgreSQL and
 * CityHash both define uint128, so combining those header domains is unsafe.
 */

#include "proxysql.h"
#include "cpp.h"
#include "PgSQL_Extended_Query_Message.h"
#include "PgSQL_Query_Processor.h"

#include "polardb_unit_support.h"

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
		session->extended_query_frame.emplace(
			std::unique_ptr<PgSQL_Parse_Message>{});
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

bool PolarDB_SessionUnitAccess::extended_execute_route_pending(
		const PgSQL_Session* session) {
	return session->polardb_extended_route.execute_pending;
}

void PolarDB_SessionUnitAccess::reset_extended_route_state(
		PgSQL_Session* session) {
	session->polardb_extended_route.reset();
}
