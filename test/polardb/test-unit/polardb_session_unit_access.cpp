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

bool PolarDB_SessionUnitAccess::extended_execute_route_pending(
		const PgSQL_Session* session) {
	return session->polardb_extended_route.execute_pending;
}

void PolarDB_SessionUnitAccess::reset_extended_route_state(
		PgSQL_Session* session) {
	session->polardb_extended_route.reset();
}
