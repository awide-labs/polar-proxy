/**
 * @file PgSQL_PolarDB_Consistency.cpp
 * @brief Small PolarDB consistency helpers used by the routing pipeline.
 *
 * Out-of-line definitions for two thin helpers consumed by
 * PgSQL_PolarDB_Flow.cpp:
 *   - polardb_resolve_wait_timeout_ms(int): the one-argument overload that
 *     supplies the global default and forwards to the header-inline resolver.
 *   - PgSQL_Session::polardb_set_session_override(): stores the per-session
 *     consistency-mode override (top tier of mode resolution).
 *
 * The pure, side-effect-free policy functions that share this domain
 * (the two-argument polardb_resolve_wait_timeout_ms(), the three-tier
 * polardb_resolve_consistency_mode() with priority session > hostgroup > global,
 * and PolarDB_Query_WaitPlan::build_consistency() for the LSN wait spec) are
 * defined inline in PgSQL_PolarDB.h so unit tests can exercise them without
 * linking the full PolarDB build.
 */

#include "PgSQL_Session.h"
#include "PgSQL_PolarDB.h"
#include "proxysql.h"
#include "cpp.h"

extern PgSQL_HostGroups_Manager* PgHGM;

#if POLARDB_PROXY

// Resolves the effective wait timeout from a hostgroup value, inheriting the
// global default (@brief on the declaration in PgSQL_PolarDB.h).
uint32_t polardb_resolve_wait_timeout_ms(int hg_timeout_ms) {
	// One-argument form: bind the global default to the current thread's
	// runtime value, then defer all tri-state logic to the header-inline
	// two-argument resolver so production and unit tests share one decision.
	return polardb_resolve_wait_timeout_ms(
		hg_timeout_ms, pgsql_thread___polardb_lag_wait_ms);
}

// Stores the per-session consistency-mode override (@brief on the declaration in
// PgSQL_Session.h). This value is the top tier of mode resolution
// (session override > hostgroup > global): a value >= 0 forces that mode for
// the session; -1 means "no override", so resolution falls through to the
// hostgroup and global settings. Set when the client runs
// `SET proxysql.polardb_consistency_mode = <value>`; `= default` (or -1 here)
// clears the override. It is also cleared by RESET proxysql.polardb_consistency_mode,
// RESET ALL, DISCARD ALL, and RESET CONNECTION.
void PgSQL_Session::polardb_set_session_override(int mode) {
	polardb_config.session_consistency_mode = mode;
	POLARDB_TRACE("PolarDB SET: session_consistency_mode=%d\n", mode);
}

#endif // POLARDB_PROXY
