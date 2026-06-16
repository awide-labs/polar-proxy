/**
 * @file PgSQL_PolarDB_Failure.cpp
 * @brief Handle PolarDB reader fallback helpers.
 */

#include "PgSQL_Session.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "proxysql.h"

#include <atomic>

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY
// Send only this one query to the writer hostgroup. Used when a consistent
// reader cannot be obtained (no replica is caught up, or the requested
// guarantee cannot be enforced). The writer/primary already holds the latest
// WAL, so any required LSN is satisfied there and the LSN wait intent is
// dropped: reset_reader_target() and reset_wait() make sure no stale
// required-LSN gate is carried onto the writer connection. Returns false when
// no writer hostgroup is known, leaving the caller to use the normal pool path.
bool PgSQL_Session::polardb_redirect_to_writer(int writer_hg, const char* reason) {
	if (writer_hg < 0) {
		return false;
	}

	POLARDB_TRACE(
		"PolarDB consistency: %s; redirecting this query to writer_hg=%d\n",
		reason ? reason : "reader acquisition fallback",
		writer_hg);
	PgHGM->status.polardb_consistency_writer_fallback.fetch_add(
		1, std::memory_order_relaxed);
	polardb_query.reset_reader_target();
	polardb_query.reset_wait();
	current_hostgroup = writer_hg;
	mybe = find_or_create_backend(current_hostgroup);
	return true;
}
#endif // POLARDB_PROXY
