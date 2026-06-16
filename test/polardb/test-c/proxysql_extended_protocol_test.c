/**
 * @file proxysql_extended_protocol_test.c
 * @brief Tiny libpq client for ProxySQL PolarDB extended-protocol tests.
 *
 * psql sends simple-query protocol for the TAP cases. This helper sends the
 * final query through PQexecParams() so the ProxySQL test can verify the v1
 * extended-protocol scope separately from simple-query wait wrapping.
 *
 * Environment:
 *   PROXYSQL_HOST / PROXYSQL_PORT  ProxySQL frontend endpoint.
 *   PGDB / PGUSER / PGPASSWORD     PostgreSQL login through ProxySQL.
 *   PGSSLMODE                      Frontend SSL mode, default disable.
 *   POLARDB_EXTENDED_SETUP_SQL     Optional SQL executed first with PQexec().
 *   POLARDB_EXTENDED_SETUP_SQL2    Optional second setup SQL, also executed
 *                                  with PQexec() on the same connection. This
 *                                  is useful when the first setup query must
 *                                  finish and publish RFQ state before the
 *                                  second setup query runs.
 *   POLARDB_EXTENDED_REPORT_NOTICES=1
 *                                  Print setup/final notice counts.
 *   POLARDB_EXTENDED_PREPARED=1    Use PQprepare/PQexecPrepared instead of
 *                                  PQexecParams for the final query.
 *
 * argv[1] is the final SELECT. The first column of the first row is printed.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>  /* required by libpq-fe.h for the PolarDB row-data hook */
#include <libpq-fe.h>
#include "polardb_test_common.h"  /* getenv_default() (empty string => default) */

typedef struct NoticeState {
	int count;
} NoticeState;

static int env_is_one(const char* name) {
	const char* v = getenv(name);
	return v && strcmp(v, "1") == 0;
}

static void count_notice(void* arg, const PGresult* res) {
	(void)res;
	NoticeState* state = (NoticeState*)arg;
	if (state) {
		state->count++;
	}
}

static void print_result_error(PGconn* conn, PGresult* res, const char* context) {
	const char* msg = res ? PQresultErrorMessage(res) : PQerrorMessage(conn);
	fprintf(stderr, "%s failed: %s", context, msg ? msg : "(no error)");
	if (!msg || msg[0] == '\0' || msg[strlen(msg) - 1] != '\n') {
		fputc('\n', stderr);
	}
}

static int run_setup_sql(PGconn* conn, const char* sql) {
	if (!sql || sql[0] == '\0') {
		return 0;
	}

	PGresult* res = PQexec(conn, sql);
	ExecStatusType status = PQresultStatus(res);
	if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
		print_result_error(conn, res, "setup SQL");
		PQclear(res);
		return 1;
	}
	PQclear(res);
	return 0;
}

static PGresult* run_extended_query(PGconn* conn, const char* sql) {
	const char* prepared = getenv("POLARDB_EXTENDED_PREPARED");
	if (prepared && strcmp(prepared, "1") == 0) {
		PGresult* prep = PQprepare(conn, "polardb_extended_protocol_stmt", sql, 0, NULL);
		if (PQresultStatus(prep) != PGRES_COMMAND_OK) {
			print_result_error(conn, prep, "PQprepare");
			PQclear(prep);
			return NULL;
		}
		PQclear(prep);
		return PQexecPrepared(conn, "polardb_extended_protocol_stmt", 0, NULL, NULL, NULL, 0);
	}

	return PQexecParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0);
}

int main(int argc, char** argv) {
	const char* final_sql =
		(argc > 1) ? argv[1] : "SELECT host(inet_server_addr()) || ':' || inet_server_port()";
	const char* keywords[] = {
		"host",
		"port",
		"dbname",
		"user",
		"password",
		"sslmode",
		NULL
	};
	const char* values[] = {
		getenv_default("PROXYSQL_HOST", "127.0.0.1"),
		getenv_default("PROXYSQL_PORT", "16433"),
		getenv_default("PGDB", "postgres"),
		getenv_default("PGUSER", "postgres"),
		getenv_default("PGPASSWORD", "postgres"),
		getenv_default("PGSSLMODE", "disable"),
		NULL
	};

	NoticeState notices = {0};
	PGconn* conn = PQconnectdbParams(keywords, values, 0);
	if (PQstatus(conn) != CONNECTION_OK) {
		fprintf(stderr, "connect failed: %s", PQerrorMessage(conn));
		PQfinish(conn);
		return 2;
	}
	PQsetNoticeReceiver(conn, count_notice, &notices);

	if (run_setup_sql(conn, getenv("POLARDB_EXTENDED_SETUP_SQL")) != 0) {
		PQfinish(conn);
		return 3;
	}
	if (run_setup_sql(conn, getenv("POLARDB_EXTENDED_SETUP_SQL2")) != 0) {
		PQfinish(conn);
		return 3;
	}
	const int setup_notices = notices.count;
	notices.count = 0;

	PGresult* res = run_extended_query(conn, final_sql);
	if (!res) {
		PQfinish(conn);
		return 4;
	}
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		print_result_error(conn, res, "extended query");
		PQclear(res);
		PQfinish(conn);
		return 5;
	}
	const int final_notices = notices.count;
	if (env_is_one("POLARDB_EXTENDED_REPORT_NOTICES")) {
		printf("setup_notices=%d\n", setup_notices);
		printf("final_notices=%d\n", final_notices);
	}
	if (PQntuples(res) > 0 && PQnfields(res) > 0 && !PQgetisnull(res, 0, 0)) {
		printf("%s\n", PQgetvalue(res, 0, 0));
	}

	PQclear(res);
	PQfinish(conn);
	return 0;
}
