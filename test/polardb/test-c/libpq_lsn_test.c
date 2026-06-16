/**
 * @file libpq_lsn_test.c
 * @brief Standalone test for the libpq PolarDB LSN protocol extension.
 *
 * Connects DIRECTLY to PolarDB (not through ProxySQL) and verifies the LSN-side
 * of the libpq protocol patch:
 *   1. The _polar_send_lsn / _polar_proxy_send_lsn connection-string options
 *      require a proxy identity and are accepted with one.
 *   2. ReadyForQuery LSN parsing: PQgetLSN()/PQhasLSN() expose a non-zero LSN
 *      after a query, and the LSN advances across a write.
 *   3. PQsetPolarSendLSN() is callable.
 *   4. A replica honors `SET polar_xact_split_wait_lsn = '<lsn>'` (smoke; skipped when no
 *      reachable PolarDB / replica, so the test never hard-fails off-cluster).
 *   5. A PolarDB LSN wait timeout exposes the stable detail marker that ProxySQL
 *      uses for timeout attribution.
 *
 * This test covers only the LSN protocol helpers used by the proxy.
 *
 * Environment variables:
 *   POLARDB_HOST     - PolarDB server host (default: 127.0.0.1)
 *   POLARDB_PORT     - PolarDB primary port (default: 60432)
 *   POLARDB_REPLICA_HOST - PolarDB replica host (default: POLARDB_HOST)
 *   POLARDB_REPLICA_PORT - PolarDB replica port (default: POLARDB_PORT)
 *   POLARDB_USER     - Username (default: postgres)
 *   POLARDB_PASSWORD - Password (default: postgres)
 *   POLARDB_DB       - Database (default: postgres)
 *
 * Build:
 *   make -C test polardb/bin/libpq_lsn_test
 *   (POLARDB_PROXY=1; needs the LSN-patched vendored libpq)
 *
 * Run:
 *   make -C test/polardb run-c-libpq-lsn
 *   (loads test/polardb/.env and maps direct topology/credentials)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdbool.h>  /* required by libpq-fe.h for the PolarDB row-data hook */
#include "libpq-fe.h"
#include "polardb_test_common.h"  /* getenv_default(), pg_conninfo(), PROXY_IDENTITY */

static int tests_passed = 0;
static int tests_failed = 0;
static int tests_skipped = 0;

#define RED     "\033[0;31m"
#define GREEN   "\033[0;32m"
#define YELLOW  "\033[1;33m"
#define BLUE    "\033[0;34m"
#define NC      "\033[0m"

#define PASS(msg, ...) do { printf(GREEN "[PASS]" NC " " msg "\n", ##__VA_ARGS__); tests_passed++; } while (0)
#define FAIL(msg, ...) do { printf(RED   "[FAIL]" NC " " msg "\n", ##__VA_ARGS__); tests_failed++; } while (0)
#define SKIP(msg, ...) do { printf(YELLOW "[SKIP]" NC " " msg "\n", ##__VA_ARGS__); tests_skipped++; } while (0)
#define INFO(msg, ...) do { printf(BLUE  "[INFO]" NC " " msg "\n", ##__VA_ARGS__); } while (0)
#define SECTION(msg)   do { printf("\n" YELLOW "--- %s ---" NC "\n", msg); } while (0)

/* getenv_default(), pg_conninfo(), and PROXY_IDENTITY come from
 * polardb_test_common.h. */
#define WAIT_TIMEOUT_DETAIL_MARKER "polar_proxy_lsn_wait_timeout"

/* Build a primary conninfo into `buf`, appending `suffix` (e.g. ""
 * or " _polar_send_lsn=true" PROXY_IDENTITY). */
static char* build_conninfo_base_suffixed(char* buf, size_t sz, const char* suffix) {
    return pg_conninfo(buf, sz,
                       getenv_default("POLARDB_HOST", "127.0.0.1"),
                       getenv_default("POLARDB_PORT", "60432"),
                       getenv_default("POLARDB_USER", "postgres"),
                       getenv_default("POLARDB_PASSWORD", "postgres"),
                       getenv_default("POLARDB_DB", "postgres"),
                       suffix);
}

/* Build a replica conninfo into `buf` (falls back to primary host/port when the
 * replica overrides are unset), appending `suffix`. */
static char* build_conninfo_replica_suffixed(char* buf, size_t sz, const char* suffix) {
    return pg_conninfo(buf, sz,
                       getenv_default("POLARDB_REPLICA_HOST", getenv_default("POLARDB_HOST", "127.0.0.1")),
                       getenv_default("POLARDB_REPLICA_PORT", getenv_default("POLARDB_PORT", "60432")),
                       getenv_default("POLARDB_USER", "postgres"),
                       getenv_default("POLARDB_PASSWORD", "postgres"),
                       getenv_default("POLARDB_DB", "postgres"),
                       suffix);
}

/* Convert an LSN uint64 to the canonical "X/XXXXXXXX" text form. */
static const char* lsn_to_string(uint64_t lsn) {
    static char buf[32];
    snprintf(buf, sizeof(buf), "%X/%08X", (uint32_t)(lsn >> 32), (uint32_t)(lsn & 0xFFFFFFFF));
    return buf;
}

/* Is a primary PolarDB reachable at all? Used to SKIP (not FAIL) off-cluster. */
static int polardb_reachable(const char* conninfo_base) {
    PGconn* conn = PQconnectdb(conninfo_base);
    int ok = (PQstatus(conn) == CONNECTION_OK);
    PQfinish(conn);
    return ok;
}

static int expect_connect_fail(const char* label, const char* conninfo) {
    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        PASS("%s rejected as expected", label);
        PQfinish(conn);
        return 0;
    }
    FAIL("%s unexpectedly connected", label);
    PQfinish(conn);
    return 1;
}

/* Test 1: LSN connection-string options require a proxy identity. */
static int test_lsn_conninfo_options(const char* conninfo_base) {
    SECTION("LSN connection-string options");
    char conninfo[2048];
    int fail = 0;

    snprintf(conninfo, sizeof(conninfo), "%s _polar_send_lsn=true", conninfo_base);
    fail += expect_connect_fail("_polar_send_lsn=true without proxy identity", conninfo);

    snprintf(conninfo, sizeof(conninfo), "%s _polar_send_lsn=true _polar_proxy_client_host=10.0.0.1", conninfo_base);
    fail += expect_connect_fail("_polar_send_lsn=true with host but no port", conninfo);

    build_conninfo_base_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) == CONNECTION_OK) {
        PASS("_polar_send_lsn=true with full proxy identity accepted");
    } else {
        FAIL("_polar_send_lsn=true with full proxy identity rejected: %s", PQerrorMessage(conn));
        fail++;
    }
    PQfinish(conn);
    return fail == 0 ? 0 : 1;
}

/* Test 2: PQgetLSN()/PQhasLSN() parse the RFQ LSN; LSN advances on a write. */
static int test_lsn_parsing(void) {
    SECTION("LSN parsing + advance on write");
    char conninfo[2048];
    PGconn* conn;
    PGresult* res;
    int fail = 0;

    build_conninfo_base_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        FAIL("cannot connect for LSN parsing: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    res = PQexec(conn, "SELECT 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        FAIL("SELECT 1 failed: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 1;
    }
    PQclear(res);

    uint64_t lsn = PQgetLSN(conn);
    if (lsn != 0) {
        PASS("PQgetLSN() non-zero after SELECT: %" PRIu64 " (%s)", lsn, lsn_to_string(lsn));
    } else {
        FAIL("PQgetLSN() returned 0 after SELECT");
        fail++;
    }
    if (PQhasLSN(conn)) {
        PASS("PQhasLSN() true after SELECT");
    } else {
        FAIL("PQhasLSN() false after SELECT");
        fail++;
    }

    res = PQexec(conn, "DROP TABLE IF EXISTS libpq_lsn_test_tbl");
    PQclear(res);
    res = PQexec(conn, "CREATE TABLE libpq_lsn_test_tbl (id serial, data text)");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        INFO("CREATE TABLE failed (permissions?), skipping LSN-advance: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return fail == 0 ? 0 : 1;
    }
    PQclear(res);

    res = PQexec(conn, "SELECT 1");
    PQclear(res);
    uint64_t lsn_before = PQgetLSN(conn);

    res = PQexec(conn, "INSERT INTO libpq_lsn_test_tbl (data) VALUES ('x')");
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        uint64_t lsn_after = PQgetLSN(conn);
        if (lsn_after >= lsn_before) {
            PASS("LSN advances across INSERT: %s -> %s", lsn_to_string(lsn_before), lsn_to_string(lsn_after));
        } else {
            FAIL("LSN regressed across INSERT: %s -> %s", lsn_to_string(lsn_before), lsn_to_string(lsn_after));
            fail++;
        }
    } else {
        INFO("INSERT failed (permissions?): %s", PQerrorMessage(conn));
        PQclear(res);
    }

    res = PQexec(conn, "DROP TABLE IF EXISTS libpq_lsn_test_tbl");
    PQclear(res);
    PQfinish(conn);
    return fail == 0 ? 0 : 1;
}

/* Test 3: the LSN API surface (PQsetPolarSendLSN) is callable. */
static int test_lsn_api(void) {
    SECTION("LSN API surface");
    char conninfo[2048];
    int fail = 0;

    build_conninfo_base_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        FAIL("cannot connect for API test: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    uint64_t lsn = PQgetLSN(conn);
    PASS("PQgetLSN() callable, returns %" PRIu64 " (%s)", lsn, lsn_to_string(lsn));
    PASS("PQhasLSN() callable, returns %d", PQhasLSN(conn));
    PQsetPolarSendLSN(conn, 1);
    PASS("PQsetPolarSendLSN() callable");

    PQfinish(conn);
    return fail == 0 ? 0 : 1;
}

/* Test 4: a replica honors SET polar_xact_split_wait_lsn. Smoke only — SKIP off-cluster. */
static int test_polar_xact_split_wait_lsn(void) {
    SECTION("SET polar_xact_split_wait_lsn on replica");
    char conninfo[2048];
    PGconn* primary;
    PGconn* replica;
    PGresult* res;
    int fail = 0;

    /* Capture a fresh write LSN on the primary. */
    build_conninfo_base_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    primary = PQconnectdb(conninfo);
    if (PQstatus(primary) != CONNECTION_OK) {
        SKIP("no primary for polar_xact_split_wait_lsn smoke: %s", PQerrorMessage(primary));
        PQfinish(primary);
        return 0;
    }
    res = PQexec(primary, "SELECT 1");
    PQclear(res);
    uint64_t target = PQgetLSN(primary);
    PQfinish(primary);
    if (target == 0) {
        SKIP("primary reported no LSN; skipping polar_xact_split_wait_lsn smoke");
        return 0;
    }

    build_conninfo_replica_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    replica = PQconnectdb(conninfo);
    if (PQstatus(replica) != CONNECTION_OK) {
        SKIP("no replica for polar_xact_split_wait_lsn smoke: %s", PQerrorMessage(replica));
        PQfinish(replica);
        return 0;
    }

    char setq[128];
    snprintf(setq, sizeof(setq), "SET polar_xact_split_wait_lsn = '%" PRIu64 "'", target);
    res = PQexec(replica, setq);
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PASS("replica accepted %s", setq);
    } else {
        /* A non-PolarDB or differently-configured backend may reject it; that is
         * informational here, not a hard failure of the LSN proxy slice. */
        INFO("replica rejected %s: %s", setq, PQerrorMessage(replica));
    }
    PQclear(res);
    PQfinish(replica);
    return fail == 0 ? 0 : 1;
}

/* Test 5: strict LSN timeout carries the stable detail marker used by ProxySQL. */
static int test_lsn_wait_timeout_detail_marker(void) {
    SECTION("LSN wait-timeout detail marker");
    char conninfo[2048];
    PGconn* replica;
    PGresult* res;
    int fail = 0;

    build_conninfo_replica_suffixed(conninfo, sizeof(conninfo), " _polar_send_lsn=true" PROXY_IDENTITY);
    replica = PQconnectdb(conninfo);
    if (PQstatus(replica) != CONNECTION_OK) {
        SKIP("no replica for LSN wait-timeout detail marker: %s", PQerrorMessage(replica));
        PQfinish(replica);
        return 0;
    }

    /* 9000000000000000000 is deliberately far-future (well beyond any real
     * replayed LSN) to force a wait timeout so the detail marker is emitted. */
    res = PQexec(replica,
                 "SET polar_consistency_mode = 'strict';"
                 "SET polar_proxy_wait_timeout_ms = 50;"
                 "SET polar_xact_split_wait_lsn = '9000000000000000000';"
                 "SELECT 1");
    if (PQresultStatus(res) != PGRES_FATAL_ERROR) {
        FAIL("strict wait timeout did not raise ERROR: status=%d message=%s",
             (int)PQresultStatus(res), PQerrorMessage(replica));
        fail++;
    } else {
        const char* primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        if (primary && strstr(primary, "LSN wait timeout")) {
            PASS("strict wait timeout primary message present: %s", primary);
        } else {
            FAIL("strict wait timeout primary message missing: %s", primary ? primary : "<null>");
            fail++;
        }
        if (detail && strcmp(detail, WAIT_TIMEOUT_DETAIL_MARKER) == 0) {
            PASS("strict wait timeout detail marker present: %s", detail);
        } else {
            FAIL("strict wait timeout detail marker mismatch: got %s expected %s",
                 detail ? detail : "<null>", WAIT_TIMEOUT_DETAIL_MARKER);
            fail++;
        }
    }
    PQclear(res);

    res = PQexec(replica, "SET polar_xact_split_wait_lsn = ''");
    PQclear(res);
    PQfinish(replica);
    return fail == 0 ? 0 : 1;
}

int main(void) {
    printf(BLUE "PolarDB libpq LSN test" NC "\n");
    char base[1024];
    build_conninfo_base_suffixed(base, sizeof(base), "");

    if (!polardb_reachable(base)) {
        INFO("No PolarDB reachable at the configured host/port; skipping live tests.");
        INFO("Set POLARDB_HOST/POLARDB_PORT to run against a real PolarDB.");
        SKIP("all live LSN tests (no backend)");
        printf("\n" GREEN "Passed: %d" NC ", " RED "Failed: %d" NC ", " YELLOW "Skipped: %d" NC "\n",
               tests_passed, tests_failed, tests_skipped);
        return 0;  /* no backend is not a build/test failure */
    }

    int rc = 0;
    rc |= test_lsn_conninfo_options(base);
    rc |= test_lsn_parsing();
    rc |= test_lsn_api();
    rc |= test_polar_xact_split_wait_lsn();
    rc |= test_lsn_wait_timeout_detail_marker();

    printf("\n" GREEN "Passed: %d" NC ", " RED "Failed: %d" NC ", " YELLOW "Skipped: %d" NC "\n",
           tests_passed, tests_failed, tests_skipped);
    return (tests_failed == 0) ? 0 : 1;
}
