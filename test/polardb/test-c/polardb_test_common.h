/**
 * @file polardb_test_common.h
 * @brief Shared helpers for the PolarDB libpq C test clients.
 *
 * Header-only. Included by both:
 *   - libpq_lsn_test.c                 (direct-to-PolarDB LSN protocol smoke)
 *   - proxysql_extended_protocol_test.c (ProxySQL extended-protocol helper)
 *
 * It centralizes three things that used to be duplicated (and divergent)
 * between those two files:
 *   1. getenv_default()  - one environment-lookup helper with a single,
 *                          documented "empty string counts as unset" policy.
 *   2. pg_conninfo()     - one libpq conninfo string builder, collapsing the
 *                          previously byte-identical base/replica builders and
 *                          the repeated "<base> _polar_send_lsn=true" snprintfs.
 *   3. PROXY_IDENTITY    - the proxy-identity suffix PolarDB requires before it
 *                          will accept the _polar_send_lsn connection option.
 *
 * No state is kept here; callers own their buffers.
 */

#ifndef POLARDB_TEST_COMMON_H
#define POLARDB_TEST_COMMON_H

#include <stdio.h>
#include <stdlib.h>

/*
 * New-style proxy identity, required by PolarDB to accept _polar_send_lsn.
 * Appended (as a suffix) to a conninfo when exercising the LSN protocol.
 */
#define PROXY_IDENTITY " _polar_proxy_client_host=10.0.0.1 _polar_proxy_client_port=54321"

/*
 * getenv_default() - return the value of environment variable `name`, or `def`
 * when the variable is unset OR set to the empty string.
 *
 * The two former copies disagreed on the empty-string case: libpq_lsn_test.c
 * treated "" as a real value, while proxysql_extended_protocol_test.c's
 * env_or_default() treated "" as unset (falling back to the default). We adopt
 * the empty-string-as-default semantics here: an exported-but-empty override
 * (e.g. `POLARDB_PORT=`) is almost always an accident, and falling back to the
 * default is the safer, more predictable behavior. This is a behavior change
 * only for libpq_lsn_test.c in the (degenerate) empty-string case, which the
 * test harness never sets.
 */
static inline const char* getenv_default(const char* name, const char* def) {
    const char* v = getenv(name);
    return (v && v[0]) ? v : def;
}

/*
 * pg_conninfo() - build a libpq conninfo string into `buf`.
 *
 * Produces:
 *   host=<host> port=<port> user=<user> password=<pass> dbname=<db> sslmode=disable<suffix>
 *
 * `suffix` is appended verbatim (pass "" for a plain base conninfo, or
 * " _polar_send_lsn=true" PROXY_IDENTITY to request the LSN protocol). This one
 * builder replaces the old build_conninfo_base()/build_conninfo_replica() pair
 * (which differed only in which env vars they read) and the repeated
 * "%s _polar_send_lsn=true" PROXY_IDENTITY snprintfs.
 *
 * Returns `buf` for convenient inline use.
 */
static inline char* pg_conninfo(char* buf, size_t sz,
                                const char* host, const char* port,
                                const char* user, const char* pass,
                                const char* db, const char* suffix) {
    snprintf(buf, sz,
             "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable%s",
             host, port, user, pass, db, suffix ? suffix : "");
    return buf;
}

#endif /* POLARDB_TEST_COMMON_H */
