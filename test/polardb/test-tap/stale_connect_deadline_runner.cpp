/**
 * Regression test for a stale connect deadline after a consistency error
 *
 *  A PolarDB consistency error ends the request through RequestEnd() while the
 *  connect deadline ('pgsql-connect_timeout_server_max') set at the beginning
 *  of PROCESSING_QUERY is still stored on the session's backend data stream.
 *  RequestEnd() did not clear it, and 'finishQuery()' leaves DSS
 *  STATE_NOT_INITIALIZED without resetting it either, so the deadline is
 *  inherited verbatim by the next query routed to the same hostgroup. An
 *  unrelated query issued more than 'pgsql-connect_timeout_server_max'
 *  milliseconds later then fails immediately in CONNECTING_SERVER with:
 *
 *      "Max connect timeout reached while reaching hostgroup N after 0ms"
 */

#include <unistd.h>

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include "libpq-fe.h"

using PGConnPtr = std::unique_ptr<PGconn, decltype(&PQfinish)>;

// --- Diagnostics and fatal asserts ------------------------------------------

static void check(bool pass, const char *fmt, ...) {
  if (pass) {
    return;
  }

  std::fprintf(stderr, "Bail out! ");
  va_list ap;
  va_start(ap, fmt);
  std::vfprintf(stderr, fmt, ap);
  va_end(ap);

  std::fprintf(stderr, "\n");
  std::exit(255);
}

// --- Environment ------------------------------------------------------------

std::string get_env(const char *name) {
  const char *value = std::getenv(name);
  check(value, "Missing required environment variable: %s\n", name);

  return std::string(value);
}

/* We get this from env.sh env exports */
std::string HOST;
std::string ADMIN_PORT, ADMIN_USER, ADMIN_PASSWORD, ADMIN_DATABASE,
    ADMIN_SSLMODE;
std::string PROXY_PORT, PROXY_USER, PROXY_PASSWORD, PROXY_DATABASE,
    PROXY_SSLMODE;

std::string PRIMARY_HOST, PRIMARY_PORT;
std::string REPLICA_HOST, REPLICA_PORT;
std::string REPLICA2_HOST, REPLICA2_PORT;

void load_config() {
  HOST = get_env("PROXYSQL_HOST");
  ADMIN_PORT = get_env("PROXYSQL_ADMIN_PORT");
  ADMIN_USER = get_env("PROXYSQL_ADMIN_USER");
  ADMIN_PASSWORD = get_env("PROXYSQL_ADMIN_PASSWORD");
  ADMIN_DATABASE = get_env("PROXYSQL_ADMIN_DATABASE");
  ADMIN_SSLMODE = get_env("PROXYSQL_ADMIN_PGSSLMODE");
  PROXY_PORT = get_env("PROXYSQL_PORT");
  PROXY_USER = get_env("PGUSER");
  PROXY_PASSWORD = get_env("PGPASSWORD");
  PROXY_DATABASE = get_env("PGDB");
  PROXY_SSLMODE = get_env("PGSSLMODE");

  PRIMARY_HOST = get_env("PRIMARY_HOST");
  PRIMARY_PORT = get_env("PRIMARY_PORT");
  REPLICA_HOST = get_env("REPLICA_HOST");
  REPLICA_PORT = get_env("REPLICA_PORT");
  REPLICA2_HOST = get_env("REPLICA2_HOST");
  REPLICA2_PORT = get_env("REPLICA2_PORT");
}

const int CONNECT_TIMEOUT_MS = 300;

PGConnPtr admin(nullptr, &PQfinish);
PGConnPtr session(nullptr, &PQfinish);

PGConnPtr create_new_connection(bool admin) {
  std::string connstr = 
       "host=" + HOST + " port=" + (admin ? ADMIN_PORT : PROXY_PORT)
     + " user=" + (admin ? ADMIN_USER : PROXY_USER)
     + " password=" + (admin ? ADMIN_PASSWORD : PROXY_PASSWORD)
     + " sslmode=" + (admin ? ADMIN_SSLMODE : PROXY_SSLMODE)
     + " dbname=" + (admin ? ADMIN_DATABASE : PROXY_DATABASE)
     + " connect_timeout=5";

  PGconn *conn = PQconnectdb(connstr.c_str());
  check(conn && PQstatus(conn) == CONNECTION_OK,
        "Failed to connect to '%s': %s", admin ? "Admin" : "Frontend",
        conn ? PQerrorMessage(conn) : "out of memory");

  return PGConnPtr(conn, &PQfinish);
}

void query(const PGConnPtr &conn, const char *sql, bool should_fail = false,
           const char *failure_keyword = nullptr) {
  PGresult *res = PQexec(conn.get(), sql);

  const ExecStatusType status = PQresultStatus(res);
  const bool ok = (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK);
  const std::string error = ok ? "" : PQerrorMessage(conn.get());
  PQclear(res);

  if (!should_fail) {
    check(ok, "Query failed   sql='%s' error='%s'", sql, error.c_str());
  } else {
    check(!ok, "Query unexpectedly succeeded   sql='%s'", sql);
    check(!failure_keyword || error.find(failure_keyword) != std::string::npos,
            "Query failed with the wrong error   sql='%s' expected='%s' error='%s'",
            sql, failure_keyword, error.c_str());
  }
}

static int reader_query_stat(const char *stat_name) {
  auto query = std::string("SELECT SUM(") + stat_name + ") FROM stats.stats_pgsql_connection_pool"
      " WHERE hostgroup=1";

  PGresult *res = PQexec(admin.get(), query.c_str());
  check(PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) != 0,
        "Failed to get reader hostgroup stat: query = %s, error = %s",
        query.c_str(),
        PQerrorMessage(admin.get()));

  const int count = atoi(PQgetvalue(res, 0, 0));
  PQclear(res);
  return count;
}

void setup_proxysql() {
  load_config();
  admin = create_new_connection(true);

  query(admin,
      ("INSERT INTO pgsql_users (username, password, active, default_hostgroup)"
       " VALUES ('" +
       PROXY_USER + "', '" + PROXY_PASSWORD + "', 1, 0)")
          .c_str());

  /* Disable monitoring so we can manipulate server status (no spirious ONLINE) */
  query(admin, "SET pgsql-monitor_enabled='false'");

  /* setup hostgroups and routing */
  query(admin, ("INSERT INTO pgsql_servers (hostgroup_id, hostname, port, "
                "status, weight, max_connections)"
                " VALUES (0, '" +
                PRIMARY_HOST + "', " + PRIMARY_PORT + ", 'ONLINE', 1000, 100)")
                   .c_str());
  query(admin, ("INSERT INTO pgsql_servers (hostgroup_id, hostname, port, "
                "status, weight, max_connections)"
                " VALUES (1, '" +
                REPLICA_HOST + "', " + REPLICA_PORT + ", 'ONLINE', 1000, 100)")
                   .c_str());
  query(admin,
        ("INSERT INTO pgsql_servers (hostgroup_id, hostname, port, status, "
         "weight, max_connections)"
         " VALUES (1, '" +
         REPLICA2_HOST + "', " + REPLICA2_PORT + ", 'ONLINE', 1000, 100)")
            .c_str());

  query(admin,
        "INSERT INTO pgsql_replication_hostgroups"
        " (writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled,"
        "  consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, "
        "proxy_protocol)"
        " VALUES (0, 1, 'polardb', 0, 'eventual', -1, -1, 'off')");

  query(admin, "INSERT INTO pgsql_query_rules"
               " (rule_id, active, match_pattern, replica_eligible, apply)"
               " VALUES (1, 1, '^SELECT', 1, 1)");

  /* Actual test params: set connect_timeout_server_max + disable fallback */
  query(admin, ("SET pgsql-connect_timeout_server_max=" +
                std::to_string(CONNECT_TIMEOUT_MS))
                   .c_str());
  query(admin, "SET pgsql-polardb_action_read_fallback='error'");

  query(admin, "LOAD PGSQL VARIABLES TO RUNTIME");
  query(admin, "LOAD PGSQL USERS TO RUNTIME");
  query(admin, "LOAD PGSQL SERVERS TO RUNTIME");
  query(admin, "LOAD PGSQL QUERY RULES TO RUNTIME");
}

int main() {
  setup_proxysql();

  session = create_new_connection(false);

  /* Verify replica offload */
  const int reader_queries_before = reader_query_stat("Queries");
  query(session, "SELECT 'reader check'");
  check(reader_query_stat("Queries") > reader_queries_before,
        "SELECT mis-routed");

  /* Shutdown readers and trigger bug via PolarDB routing error */
  query(admin, "UPDATE pgsql_servers SET status='OFFLINE_HARD' WHERE hostgroup_id=1");
  query(admin, "LOAD PGSQL SERVERS TO RUNTIME");
  query(session, "SELECT 'no readers'", true, "PolarDB read routing requires a usable replica");

  /* Regression: after the deadline, the same session must work */
  query(admin, "UPDATE pgsql_servers SET status='ONLINE' WHERE hostgroup_id=1");
  query(admin, "LOAD PGSQL SERVERS TO RUNTIME");

  std::this_thread::sleep_for(std::chrono::milliseconds(2 * CONNECT_TIMEOUT_MS));

  /* Optional: pool should be empty to hit this bug */
  check(reader_query_stat("ConnUsed + ConnFree") == 0, "flaky situation: more than one connection in the pool");

  /* Regression: re-using session should be OK */
  query(session, "SELECT 'reusing session is ok'");

  return 0;
}
