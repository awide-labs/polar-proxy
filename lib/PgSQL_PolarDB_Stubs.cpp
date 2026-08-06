/**
 * @file PgSQL_PolarDB_Stubs.cpp
 * @brief Link-time placeholder for PolarDB symbols when POLARDB_PROXY is off.
 *
 * This translation unit is compiled in both build configurations (with and
 * without the PolarDB feature). Its purpose is to provide do-nothing
 * definitions for any PolarDB function that always-compiled core code might
 * call when the feature is disabled, so the binary still links.
 *
 * PolarDB routing, configuration, counters, startup-profile handling, and wait
 * calls are protected by `#if POLARDB_PROXY`. Generic extended-protocol frame,
 * error-boundary, and RFQ-ownership fixes are intentionally shared by both
 * builds and do not call PolarDB symbols.
 *
 * A POLARDB_PROXY=0 build therefore links vanilla libpq and has no PolarDB
 * runtime surface. Its contract is behavioral compatibility for ordinary
 * PostgreSQL traffic, not byte identity with an upstream binary. See
 * doc/polardb-arch/02-BUILD-TOGGLE-AND-LIBPQ.md.
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
