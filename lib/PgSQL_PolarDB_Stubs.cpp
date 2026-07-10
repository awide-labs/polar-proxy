/**
 * @file PgSQL_PolarDB_Stubs.cpp
 * @brief Link-time placeholder for PolarDB symbols when POLARDB_PROXY is off.
 *
 * This translation unit is compiled in both build configurations (with and
 * without the PolarDB feature). Its purpose is to provide do-nothing
 * definitions for any PolarDB function that always-compiled core code might
 * call when the feature is disabled, so the binary still links.
 *
 * The PolarDB LSN session-consistency feature is enabled by `#if POLARDB_PROXY`
 * on BOTH sides: the declarations (include/PgSQL_PolarDB.h, plus the PolarDB
 * members added to PgSQL_Session.h / PgSQL_Connection.h /
 * PgSQL_HostGroups_Manager.h) AND every place in the core that calls into it.
 * Because the call sites are protected, a POLARDB_PROXY=0 build contains no
 * reference to any PolarDB symbol. So there is nothing to stub, and the active
 * body of this file is empty.
 *
 * Verified: a POLARDB_PROXY=0 build links clean and is byte-for-byte equivalent
 * to a build of ProxySQL without this feature at all. This equivalence is a
 * design contract; see doc/polardb-arch/02-BUILD-TOGGLE-AND-LIBPQ.md.
 *
 * Maintenance rule: if a future change adds an UNGUARDED core call to a PolarDB
 * symbol, put its no-op definition below. Such a stub must NOT name any PolarDB
 * type, because those types are not declared when POLARDB_PROXY=0.
 */

#include "proxysql.h"

#if !POLARDB_PROXY

// Intentionally empty. See the file header for why no stub is needed; add any
// future no-op definitions here.

#endif // !POLARDB_PROXY
