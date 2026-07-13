/**
 * @file libpq_xact_test.c
 * @brief Standalone smoke test for the libpq PolarDB xact RFQ extension.
 *
 * This helper validates the client-side surface added for future transaction
 * split routing. It does not enable ProxySQL split routing by itself.
 *
 * The compile/link checks are always meaningful. Live RFQ-xact checks are
 * skipped when the configured backend is unavailable or does not accept the
 * xact startup parameter.
 */

#include <stdbool.h>  /* required by libpq-fe.h for the PolarDB row-data hook */
#include <ctype.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libpq-fe.h"
#include "polardb_test_common.h"

static int tests_passed = 0;
static int tests_failed = 0;
static int tests_skipped = 0;

enum {
    ISOLATION_REPORT_SUPPORTED = 0,
    ISOLATION_REPORT_UNSUPPORTED = 2,
    ISOLATION_REPORT_ERROR = 3
};

enum {
    W_MARKER_SUPPORTED = 0,
    W_MARKER_UNSUPPORTED = 2,
    W_MARKER_ERROR = 3
};

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

static char* build_conninfo_base_suffixed(char* buf, size_t sz, const char* suffix) {
    return pg_conninfo(buf, sz,
                       getenv_default("POLARDB_HOST", "127.0.0.1"),
                       getenv_default("POLARDB_PORT", "60432"),
                       getenv_default("POLARDB_USER", "postgres"),
                       getenv_default("POLARDB_PASSWORD", "postgres"),
                       getenv_default("POLARDB_DB", "postgres"),
                       suffix);
}

static int backend_reachable(const char* conninfo_base) {
    PGconn* conn = PQconnectdb(conninfo_base);
    int ok = (PQstatus(conn) == CONNECTION_OK);
    PQfinish(conn);
    return ok;
}

static const char* parameter_status_nonempty(PGconn* conn, const char* name) {
    const char* value = PQparameterStatus(conn, name);
    return (value && value[0]) ? value : NULL;
}

static void normalize_isolation_value(char* dst, size_t dst_sz, const char* raw) {
    size_t out = 0;
    if (dst_sz == 0) {
        return;
    }
    if (raw) {
        const unsigned char* p = (const unsigned char*)raw;
        while (*p && out + 1 < dst_sz) {
            if (isalnum(*p)) {
                dst[out++] = (char)tolower(*p);
            }
            p++;
        }
    }
    dst[out] = '\0';
}

static int isolation_equals(const char* raw, const char* normalized_expected) {
    char normalized[64];
    normalize_isolation_value(normalized, sizeof(normalized), raw);
    return strcmp(normalized, normalized_expected) == 0;
}

static int isolation_value_known(const char* raw) {
    char normalized[64];
    normalize_isolation_value(normalized, sizeof(normalized), raw);
    return strcmp(normalized, "readcommitted") == 0 ||
           strcmp(normalized, "readuncommitted") == 0 ||
           strcmp(normalized, "repeatableread") == 0 ||
           strcmp(normalized, "serializable") == 0;
}

static int exec_sql_ok(PGconn* conn, const char* sql) {
    PGresult* res = PQexec(conn, sql);
    ExecStatusType status = PQresultStatus(res);
    if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) {
        PQclear(res);
        return 1;
    }
    FAIL("%s failed: %s", sql, PQerrorMessage(conn));
    PQclear(res);
    return 0;
}

static int getenv_int_default(const char* name, int def, int min, int max) {
    const char* raw = getenv(name);
    char* end = NULL;
    long v;

    if (!raw || !raw[0]) {
        return def;
    }
    v = strtol(raw, &end, 10);
    if (end == raw || *end != '\0' || v < min || v > max) {
        INFO("ignoring invalid %s=%s; using %d", name, raw, def);
        return def;
    }
    return (int)v;
}

static int sql_value_is_on(const char* raw) {
    return raw &&
           (strcmp(raw, "on") == 0 ||
            strcmp(raw, "true") == 0 ||
            strcmp(raw, "1") == 0);
}

static int backend_xact_split_enabled(PGconn* conn) {
    PGresult* res = PQexec(conn, "SHOW polar_enable_xact_split");
    ExecStatusType status = PQresultStatus(res);
    int enabled = 0;

    if (status != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        INFO("backend does not expose polar_enable_xact_split: %s",
             PQerrorMessage(conn));
        PQclear(res);
        return 0;
    }

    enabled = sql_value_is_on(PQgetvalue(res, 0, 0));
    PQclear(res);
    return enabled;
}

static int exec_sql_quiet(PGconn* conn, const char* sql) {
    PGresult* res = PQexec(conn, sql);
    ExecStatusType status = PQresultStatus(res);
    int ok = (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

static int test_xact_w_marker_capability(int strict) {
    SECTION("Xact WAL-pending RFQ marker");

    char conninfo[2048];
    build_conninfo_base_suffixed(conninfo, sizeof(conninfo),
                                 " _polar_send_xact=true" PROXY_IDENTITY);

    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        SKIP("_polar_send_xact=true not accepted for w-marker probe: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        return strict ? W_MARKER_ERROR : W_MARKER_UNSUPPORTED;
    }

    if (!backend_xact_split_enabled(conn)) {
        SKIP("backend polar_enable_xact_split is off or unavailable");
        PQfinish(conn);
        return strict ? W_MARKER_ERROR : W_MARKER_UNSUPPORTED;
    }

    const int attempts = getenv_int_default("POLARDB_W_MARKER_ATTEMPTS", 40, 1, 500);
    const int payload_bytes =
        getenv_int_default("POLARDB_W_MARKER_PAYLOAD_BYTES", 131072, 1024, 1048576);
    const int pid = (int)getpid();
    char table[128];
    char sql[2048];
    int saw_xact_marker = 0;

    snprintf(table, sizeof(table), "polardb_xact_w_marker_probe_%d", pid);
    snprintf(sql, sizeof(sql),
             "DROP TABLE IF EXISTS %s; CREATE TABLE %s(id int PRIMARY KEY, payload text)",
             table, table);
    if (!exec_sql_ok(conn, sql)) {
        PQfinish(conn);
        return W_MARKER_ERROR;
    }
    (void)exec_sql_quiet(conn, "SET synchronous_commit = off");

    for (int i = 1; i <= attempts; i++) {
        if (!exec_sql_ok(conn, "BEGIN")) {
            snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", table);
            (void)exec_sql_quiet(conn, sql);
            PQfinish(conn);
            return W_MARKER_ERROR;
        }

        snprintf(sql, sizeof(sql),
                 "INSERT INTO %s VALUES (%d, repeat('x', %d))",
                 table, i, payload_bytes);
        if (!exec_sql_ok(conn, sql)) {
            (void)exec_sql_quiet(conn, "ROLLBACK");
            snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", table);
            (void)exec_sql_quiet(conn, sql);
            PQfinish(conn);
            return W_MARKER_ERROR;
        }

        const char* xids = PQgetXactSplitXids(conn);
        const int splittable = PQisXactSplittable(conn);
        const int wal_pending = PQisXactWalPending(conn);
        if ((xids && xids[0]) || splittable || wal_pending) {
            saw_xact_marker = 1;
        }
        INFO("w-marker attempt %d/%d: xids=%s splittable=%d wal_pending=%d",
             i, attempts, xids ? xids : "<null>", splittable, wal_pending);

        if (wal_pending) {
            PASS("backend emitted WAL-pending RFQ marker after in-transaction write");
            (void)exec_sql_quiet(conn, "ROLLBACK");
            snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", table);
            (void)exec_sql_quiet(conn, sql);
            PQfinish(conn);
            return W_MARKER_SUPPORTED;
        }

        if (!exec_sql_ok(conn, "ROLLBACK")) {
            snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", table);
            (void)exec_sql_quiet(conn, sql);
            PQfinish(conn);
            return W_MARKER_ERROR;
        }
    }

    snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", table);
    (void)exec_sql_quiet(conn, sql);
    PQfinish(conn);

    if (saw_xact_marker) {
        SKIP("backend emitted xact RFQ markers, but no WAL-pending marker was observed");
    } else {
        SKIP("backend did not emit xact RFQ markers during the w-marker probe");
    }
    return strict ? W_MARKER_ERROR : W_MARKER_UNSUPPORTED;
}

static int test_xact_api_defaults(const char* conninfo_base) {
    SECTION("Xact API defaults");

    PGconn* conn = PQconnectdb(conninfo_base);
    if (PQstatus(conn) != CONNECTION_OK) {
        SKIP("cannot connect for default API check: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 0;
    }

    if (PQgetXactSplitXids(conn) == NULL) {
        PASS("PQgetXactSplitXids() default is NULL");
    } else {
        FAIL("PQgetXactSplitXids() default unexpectedly returned: %s", PQgetXactSplitXids(conn));
    }

    if (PQisXactSplittable(conn) == 0 && PQisXactWalPending(conn) == 0) {
        PASS("xact status accessors default to false");
    } else {
        FAIL("xact status accessors default mismatch: splittable=%d wal_pending=%d",
             PQisXactSplittable(conn), PQisXactWalPending(conn));
    }

    PQsetPolarSendXact(conn, 1);
    PASS("PQsetPolarSendXact() callable");

    PQfinish(conn);
    return tests_failed == 0 ? 0 : 1;
}

static int test_xact_startup_and_rfq(void) {
    SECTION("Xact startup option and RFQ smoke");

    char conninfo[2048];
    build_conninfo_base_suffixed(conninfo, sizeof(conninfo),
                                 " _polar_send_xact=true" PROXY_IDENTITY);

    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        SKIP("_polar_send_xact=true not accepted by configured backend: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        return 0;
    }
    PASS("_polar_send_xact=true with proxy identity accepted");

    PGresult* res = PQexec(conn, "SELECT 1");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        FAIL("SELECT 1 failed on xact RFQ connection: %s", PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        return 1;
    }
    PQclear(res);

    const char* xids = PQgetXactSplitXids(conn);
    PASS("xact accessors callable after RFQ: xids=%s splittable=%d wal_pending=%d",
         xids ? xids : "<null>",
         PQisXactSplittable(conn),
         PQisXactWalPending(conn));

    PQfinish(conn);
    return 0;
}

static int test_isolation_parameter_status(const char* conninfo_base) {
    SECTION("Isolation ParameterStatus");

    PGconn* conn = PQconnectdb(conninfo_base);
    if (PQstatus(conn) != CONNECTION_OK) {
        SKIP("cannot connect for isolation ParameterStatus check: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        return 0;
    }

    const char* default_iso =
        parameter_status_nonempty(conn, "default_transaction_isolation");
    if (!default_iso) {
        SKIP("backend does not report default_transaction_isolation");
        PQfinish(conn);
        return 0;
    }

    if (isolation_value_known(default_iso)) {
        PASS("default_transaction_isolation reported at startup: %s", default_iso);
    } else {
        FAIL("default_transaction_isolation has unknown value: %s", default_iso);
    }

    if (exec_sql_ok(conn, "SET default_transaction_isolation = 'repeatable read'")) {
        default_iso =
            parameter_status_nonempty(conn, "default_transaction_isolation");
        if (isolation_equals(default_iso, "repeatableread")) {
            PASS("default_transaction_isolation report updates after SET");
        } else {
            FAIL("default_transaction_isolation after SET is %s, expected repeatable read",
                 default_iso ? default_iso : "<missing>");
        }
    }

    if (exec_sql_ok(conn, "RESET default_transaction_isolation")) {
        default_iso =
            parameter_status_nonempty(conn, "default_transaction_isolation");
        if (isolation_value_known(default_iso)) {
            PASS("default_transaction_isolation report remains valid after RESET: %s",
                 default_iso);
        } else {
            FAIL("default_transaction_isolation missing or unknown after RESET: %s",
                 default_iso ? default_iso : "<missing>");
        }
    }

    const char* current_iso =
        parameter_status_nonempty(conn, "transaction_isolation");
    if (!current_iso) {
        SKIP("backend does not report transaction_isolation");
        PQfinish(conn);
        return tests_failed == 0 ? 0 : 1;
    }

    if (isolation_value_known(current_iso)) {
        PASS("transaction_isolation reported: %s", current_iso);
    } else {
        FAIL("transaction_isolation has unknown value: %s", current_iso);
    }

    if (exec_sql_ok(conn, "BEGIN ISOLATION LEVEL REPEATABLE READ")) {
        current_iso = parameter_status_nonempty(conn, "transaction_isolation");
        if (isolation_equals(current_iso, "repeatableread")) {
            PASS("transaction_isolation report updates for BEGIN isolation level");
        } else {
            FAIL("transaction_isolation in transaction is %s, expected repeatable read",
                 current_iso ? current_iso : "<missing>");
        }
        (void)exec_sql_ok(conn, "COMMIT");
    }

    PQfinish(conn);
    return tests_failed == 0 ? 0 : 1;
}

static int probe_isolation_report(const char* conninfo_base) {
    PGconn* conn = PQconnectdb(conninfo_base);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "[ERROR] cannot connect for isolation report probe: %s\n",
                PQerrorMessage(conn));
        PQfinish(conn);
        return ISOLATION_REPORT_ERROR;
    }

    const char* default_iso =
        parameter_status_nonempty(conn, "default_transaction_isolation");
    if (!default_iso) {
        printf("[INFO] backend does not report default_transaction_isolation\n");
        PQfinish(conn);
        return ISOLATION_REPORT_UNSUPPORTED;
    }

    if (!isolation_value_known(default_iso)) {
        fprintf(stderr,
                "[ERROR] backend reported unknown default_transaction_isolation=%s\n",
                default_iso);
        PQfinish(conn);
        return ISOLATION_REPORT_ERROR;
    }

    printf("[INFO] backend reports default_transaction_isolation=%s\n",
           default_iso);
    PQfinish(conn);
    return ISOLATION_REPORT_SUPPORTED;
}

static void usage(const char* argv0) {
    fprintf(stderr, "Usage: %s [--probe-isolation-report|--probe-w-marker|--require-w-marker]\n", argv0);
}

int main(int argc, char** argv) {
    printf(BLUE "PolarDB libpq xact RFQ test" NC "\n");

    char base[1024];
    build_conninfo_base_suffixed(base, sizeof(base), "");

    if (argc > 2) {
        usage(argv[0]);
        return 2;
    }
    if (argc == 2) {
        if (strcmp(argv[1], "--probe-isolation-report") == 0) {
            return probe_isolation_report(base);
        }
        if (strcmp(argv[1], "--probe-w-marker") == 0) {
            return test_xact_w_marker_capability(0);
        }
        if (strcmp(argv[1], "--require-w-marker") == 0) {
            return test_xact_w_marker_capability(1);
        }
        usage(argv[0]);
        return 2;
    }

    if (!backend_reachable(base)) {
        INFO("No backend reachable at the configured host/port; skipping live xact tests.");
        INFO("Set POLARDB_HOST/POLARDB_PORT to run against a real PolarDB.");
        SKIP("all live xact tests (no backend)");
        printf("\n" GREEN "Passed: %d" NC ", " RED "Failed: %d" NC ", " YELLOW "Skipped: %d" NC "\n",
               tests_passed, tests_failed, tests_skipped);
        return 0;
    }

    int rc = 0;
    rc |= test_xact_api_defaults(base);
    rc |= test_xact_startup_and_rfq();
    rc |= (test_xact_w_marker_capability(0) == W_MARKER_ERROR);
    rc |= test_isolation_parameter_status(base);

    printf("\n" GREEN "Passed: %d" NC ", " RED "Failed: %d" NC ", " YELLOW "Skipped: %d" NC "\n",
           tests_passed, tests_failed, tests_skipped);
    return rc || tests_failed ? 1 : 0;
}
