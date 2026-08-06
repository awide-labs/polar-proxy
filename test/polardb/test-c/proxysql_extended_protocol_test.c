/**
 * @file proxysql_extended_protocol_test.c
 * @brief Tiny libpq client for ProxySQL PolarDB extended-protocol tests.
 *
 * psql sends simple-query protocol for the TAP cases. This helper sends the
 * final query through PQexecParams() or PQprepare/PQexecPrepared() so the
 * ProxySQL test can verify v15_wait extended-protocol routing and reuse.
 *
 * Environment:
 *   PROXYSQL_HOST / PROXYSQL_PORT  ProxySQL frontend endpoint.
 *   PGDB / PGUSER / PGPASSWORD     PostgreSQL login through ProxySQL.
 *   PGSSLMODE                      Frontend SSL mode, default disable.
 *   POLARDB_EXTENDED_SETUP_SQL     Optional SQL executed first with PQexec().
 *   POLARDB_EXTENDED_SETUP_SQL2    Optional second setup SQL, also executed
 *                                  with PQexec() on the same connection. This
 *                                  is useful when the first setup query must
 *                                  finish and record RFQ state before the
 *                                  second setup query runs.
 *   POLARDB_EXTENDED_REPORT_NOTICES=1
 *                                  Print setup/final notice counts.
 *   POLARDB_EXTENDED_PREPARED=1    Use PQprepare/PQexecPrepared instead of
 *                                  PQexecParams for the final query.
 *   POLARDB_EXTENDED_PIPELINE_FLUSH=1
 *                                  Flush an expected-error query before Sync,
 *                                  then verify the same session recovers.
 *   POLARDB_EXTENDED_EXPECT_INERROR=1
 *                                  Require the Flush error's eventual RFQ to
 *                                  report failed-transaction state.
 *   POLARDB_EXTENDED_EXPECT_IDLE=1 Require a successful Flush's eventual RFQ
 *                                  to report idle transaction state.
 *   POLARDB_EXTENDED_REQUEST_LSN=1 Request an LSN in frontend RFQ messages.
 *   POLARDB_EXTENDED_PRE_SYNC_DELAY_MS
 *                                  Pause after the Flush result and before Sync
 *                                  to exercise delayed backend maintenance.
 *   POLARDB_EXTENDED_PIPELINE_FLUSH_SUCCESS=1
 *                                  Flush a successful query before Sync, then
 *                                  run the configured recovery query.
 *   POLARDB_EXTENDED_PIPELINE_FLUSH_RESET=1
 *                                  Flush a successful write, execute a
 *                                  proxy-local RESET in the same open frame,
 *                                  then Sync and run the recovery query.
 *   POLARDB_EXTENDED_PIPELINE_RESET_SQL
 *                                  Override that proxy-local RESET statement.
 *   POLARDB_EXTENDED_PIPELINE_MULTI=1
 *                                  Send argv[1] and the SQL in
 *                                  POLARDB_EXTENDED_PIPELINE_SQL2 in one Sync.
 *
 * argv[1] is the final SELECT. The first column of the first row is printed.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
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

static int sleep_before_pipeline_sync(void) {
	const char* value = getenv("POLARDB_EXTENDED_PRE_SYNC_DELAY_MS");
	if (!value || value[0] == '\0') {
		return 0;
	}
	char* end = NULL;
	errno = 0;
	const unsigned long delay_ms = strtoul(value, &end, 10);
	if (errno != 0 || !end || *end != '\0' || delay_ms > 60000) {
		fprintf(stderr, "invalid POLARDB_EXTENDED_PRE_SYNC_DELAY_MS: %s\n",
			value);
		return 1;
	}
	struct timespec delay = {
		(time_t)(delay_ms / 1000),
		(long)((delay_ms % 1000) * 1000000)
	};
	while (nanosleep(&delay, &delay) != 0) {
		if (errno != EINTR) {
			perror("nanosleep before pipeline Sync");
			return 1;
		}
	}
	return 0;
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

static int print_first_value(const PGresult* res, const char* label) {
	if (PQntuples(res) == 0 || PQnfields(res) == 0 || PQgetisnull(res, 0, 0)) {
		return 0;
	}
	printf("%s=%s\n", label, PQgetvalue(res, 0, 0));
	return 1;
}

static int drain_pipeline_to_sync(PGconn* conn, int expected_commands,
		int* command_results) {
	int sync_count = 0;
	int null_boundaries = 0;
	PGresult* res;

	while (sync_count == 0) {
		res = PQgetResult(conn);
		if (res == NULL) {
			// Pipeline mode reports one NULL boundary between query results.
			// Keep draining until the explicit PGRES_PIPELINE_SYNC marker, but
			// fail instead of spinning if the marker never arrives.
			if (PQstatus(conn) == CONNECTION_BAD ||
					++null_boundaries > expected_commands + 1) {
				fprintf(stderr,
					"pipeline ended before PGRES_PIPELINE_SYNC: %s",
					PQerrorMessage(conn));
				return 1;
			}
			continue;
		}
		const ExecStatusType status = PQresultStatus(res);
		if (status == PGRES_PIPELINE_SYNC) {
			++sync_count;
		} else if (command_results &&
				(status == PGRES_TUPLES_OK || status == PGRES_COMMAND_OK)) {
			char label[32];
			++*command_results;
			snprintf(label, sizeof(label), "result%d", *command_results);
			print_first_value(res, label);
		} else {
			print_result_error(conn, res, "pipeline command");
			PQclear(res);
			return 1;
		}
		PQclear(res);
	}
	res = PQgetResult(conn);
	if (res != NULL) {
		print_result_error(conn, res, "result after pipeline Sync");
		PQclear(res);
		return 1;
	}

	printf("pipeline_sync=%d\n", sync_count);
	if (sync_count != 1) {
		fprintf(stderr,
			"pipeline returned %d PGRES_PIPELINE_SYNC results, expected 1\n",
			sync_count);
		return 1;
	}
	return 0;
}

static int report_requested_lsn(PGconn* conn, const char* context) {
	if (!env_is_one("POLARDB_EXTENDED_REQUEST_LSN")) {
		return 0;
	}
	if (!PQhasLSN(conn)) {
		fprintf(stderr, "%s RFQ did not carry the requested frontend LSN\n",
			context);
		return 1;
	}
	printf("rfq_lsn=%llu\n", (unsigned long long)PQgetLSN(conn));
	return 0;
}

static int run_pipeline_flush_error(PGconn* conn, const char* sql) {
	if (!PQenterPipelineMode(conn) ||
			!PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQsendFlushRequest(conn)) {
		fprintf(stderr, "pipeline Flush setup failed: %s", PQerrorMessage(conn));
		return 1;
	}

	PGresult* res = PQgetResult(conn);
	if (!res || PQresultStatus(res) != PGRES_FATAL_ERROR) {
		print_result_error(conn, res, "pipeline Flush error query");
		PQclear(res);
		return 1;
	}
	printf("pipeline_error=1\n");
	PQclear(res);
	res = PQgetResult(conn);
	if (res != NULL) {
		fprintf(stderr,
			"pipeline error Flush returned a result before client Sync\n");
		PQclear(res);
		return 1;
	}
	printf("pre_sync_result=none\n");
	if (sleep_before_pipeline_sync() != 0) {
		return 1;
	}

	if (!PQpipelineSync(conn) || drain_pipeline_to_sync(conn, 0, NULL) != 0 ||
			!PQexitPipelineMode(conn)) {
		fprintf(stderr, "pipeline recovery failed: %s", PQerrorMessage(conn));
		return 1;
	}
	if (report_requested_lsn(conn, "pipeline error") != 0) {
		return 1;
	}
	const int expect_inerror = env_is_one("POLARDB_EXTENDED_EXPECT_INERROR");
	if (expect_inerror) {
		const PGTransactionStatusType txn_status = PQtransactionStatus(conn);
		if (txn_status != PQTRANS_INERROR) {
			fprintf(stderr,
				"pipeline RFQ transaction status is %d, expected PQTRANS_INERROR\n",
				(int)txn_status);
			return 1;
		}
		printf("transaction_status=E\n");
	}

	const char* recovery_sql = getenv_default(
		"POLARDB_EXTENDED_RECOVERY_SQL", "SELECT 1");
	res = PQexecParams(conn, recovery_sql, 0, NULL, NULL, NULL, NULL, 0);
	const ExecStatusType recovery_status = res
		? PQresultStatus(res) : PGRES_FATAL_ERROR;
	if (!res || (expect_inerror
			? recovery_status != PGRES_COMMAND_OK
			: (recovery_status != PGRES_TUPLES_OK &&
				recovery_status != PGRES_COMMAND_OK))) {
		print_result_error(conn, res, "post-pipeline recovery query");
		PQclear(res);
		return 1;
	}
	printf("recovery=1\n");
	print_first_value(res, "recovery_result");
	PQclear(res);
	if (expect_inerror) {
		const PGTransactionStatusType txn_status = PQtransactionStatus(conn);
		if (txn_status != PQTRANS_IDLE) {
			fprintf(stderr,
				"post-recovery transaction status is %d, expected PQTRANS_IDLE\n",
				(int)txn_status);
			return 1;
		}
		printf("transaction_status_after_recovery=I\n");
	}
	return 0;
}

static int run_pipeline_flush_success(PGconn* conn, const char* sql) {
	if (!PQenterPipelineMode(conn) ||
			!PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQsendFlushRequest(conn)) {
		fprintf(stderr, "pipeline successful Flush setup failed: %s",
			PQerrorMessage(conn));
		return 1;
	}

	PGresult* res = PQgetResult(conn);
	if (!res || (PQresultStatus(res) != PGRES_TUPLES_OK &&
			PQresultStatus(res) != PGRES_COMMAND_OK)) {
		print_result_error(conn, res, "pipeline successful Flush query");
		PQclear(res);
		return 1;
	}
	printf("flush_success=1\n");
	print_first_value(res, "flush_result");
	PQclear(res);
	res = PQgetResult(conn);
	if (res != NULL) {
		fprintf(stderr, "pipeline Flush returned an unexpected extra result\n");
		PQclear(res);
		return 1;
	}
	printf("pre_sync_result=none\n");
	if (sleep_before_pipeline_sync() != 0) {
		return 1;
	}

	if (!PQpipelineSync(conn) || drain_pipeline_to_sync(conn, 0, NULL) != 0 ||
			!PQexitPipelineMode(conn)) {
		fprintf(stderr, "pipeline successful Flush Sync failed: %s",
			PQerrorMessage(conn));
		return 1;
	}
	if (report_requested_lsn(conn, "pipeline success") != 0) {
		return 1;
	}
	if (env_is_one("POLARDB_EXTENDED_EXPECT_IDLE")) {
		const PGTransactionStatusType txn_status = PQtransactionStatus(conn);
		if (txn_status != PQTRANS_IDLE) {
			fprintf(stderr,
				"successful pipeline RFQ transaction status is %d, expected PQTRANS_IDLE\n",
				(int)txn_status);
			return 1;
		}
		printf("transaction_status=I\n");
	}

	const char* recovery_sql = getenv_default(
		"POLARDB_EXTENDED_RECOVERY_SQL", "SELECT 1");
	res = PQexecParams(conn, recovery_sql, 0, NULL, NULL, NULL, NULL, 0);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
		print_result_error(conn, res, "post-Flush consistency query");
		PQclear(res);
		return 1;
	}
	printf("recovery=1\n");
	print_first_value(res, "recovery_result");
	PQclear(res);
	return 0;
}

static int run_pipeline_flush_reset(PGconn* conn, const char* sql) {
	const char* reset_sql = getenv_default(
		"POLARDB_EXTENDED_PIPELINE_RESET_SQL",
		"RESET proxysql.polardb_txn_split_warmup");
	if (!PQenterPipelineMode(conn) ||
			!PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQsendFlushRequest(conn)) {
		fprintf(stderr, "pipeline write Flush setup failed: %s",
			PQerrorMessage(conn));
		return 1;
	}

	PGresult* res = PQgetResult(conn);
	if (!res || (PQresultStatus(res) != PGRES_TUPLES_OK &&
			PQresultStatus(res) != PGRES_COMMAND_OK)) {
		print_result_error(conn, res, "pipeline write Flush");
		PQclear(res);
		return 1;
	}
	printf("write_flush_success=1\n");
	PQclear(res);
	res = PQgetResult(conn);
	if (res != NULL) {
		fprintf(stderr,
			"pipeline write Flush returned an unexpected extra result\n");
		PQclear(res);
		return 1;
	}

	if (!PQsendQueryParams(conn, reset_sql, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQsendFlushRequest(conn)) {
		fprintf(stderr, "pipeline RESET Flush setup failed: %s",
			PQerrorMessage(conn));
		return 1;
	}
	res = PQgetResult(conn);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
		print_result_error(conn, res, "pipeline RESET Flush");
		PQclear(res);
		return 1;
	}
	printf("reset_flush_success=1\n");
	PQclear(res);
	res = PQgetResult(conn);
	if (res != NULL) {
		fprintf(stderr,
			"pipeline RESET Flush returned an unexpected extra result\n");
		PQclear(res);
		return 1;
	}
	printf("pre_sync_result=none\n");
	if (sleep_before_pipeline_sync() != 0) {
		return 1;
	}

	if (!PQpipelineSync(conn) || drain_pipeline_to_sync(conn, 0, NULL) != 0 ||
			!PQexitPipelineMode(conn)) {
		fprintf(stderr, "pipeline RESET Sync failed: %s",
			PQerrorMessage(conn));
		return 1;
	}
	if (report_requested_lsn(conn, "pipeline RESET") != 0) {
		return 1;
	}
	if (PQtransactionStatus(conn) != PQTRANS_IDLE) {
		fprintf(stderr,
			"pipeline RESET RFQ did not leave the autocommit session idle\n");
		return 1;
	}
	printf("transaction_status=I\n");

	const char* recovery_sql = getenv_default(
		"POLARDB_EXTENDED_RECOVERY_SQL", "SELECT 1");
	res = PQexecParams(conn, recovery_sql, 0, NULL, NULL, NULL, NULL, 0);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
		print_result_error(conn, res, "post-RESET consistency query");
		PQclear(res);
		return 1;
	}
	printf("recovery=1\n");
	print_first_value(res, "recovery_result");
	PQclear(res);
	return 0;
}

static int run_pipeline_multi(PGconn* conn, const char* sql) {
	const char* sql2 = getenv("POLARDB_EXTENDED_PIPELINE_SQL2");
	if (!sql2 || sql2[0] == '\0') {
		fprintf(stderr,
			"POLARDB_EXTENDED_PIPELINE_SQL2 is required for pipeline multi mode\n");
		return 1;
	}
	if (!PQenterPipelineMode(conn) ||
			!PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQsendQueryParams(conn, sql2, 0, NULL, NULL, NULL, NULL, 0) ||
			!PQpipelineSync(conn)) {
		fprintf(stderr, "pipeline multi setup failed: %s", PQerrorMessage(conn));
		return 1;
	}

	int command_results = 0;
	if (drain_pipeline_to_sync(conn, 2, &command_results) != 0 ||
			!PQexitPipelineMode(conn)) {
		return 1;
	}
	if (command_results != 2) {
		fprintf(stderr, "pipeline returned %d command results, expected 2\n",
			command_results);
		return 1;
	}
	return 0;
}

int main(int argc, char** argv) {
	const char* final_sql =
		(argc > 1) ? argv[1] : "SELECT host(inet_server_addr()) || ':' || inet_server_port()";
	const char* request_lsn = env_is_one("POLARDB_EXTENDED_REQUEST_LSN")
		? "true" : NULL;
	const char* keywords[] = {
		"host",
		"port",
		"dbname",
		"user",
		"password",
		"sslmode",
		"_polar_send_lsn",
		NULL
	};
	const char* values[] = {
		getenv_default("PROXYSQL_HOST", "127.0.0.1"),
		getenv_default("PROXYSQL_PORT", "16433"),
		getenv_default("PGDB", "postgres"),
		getenv_default("PGUSER", "postgres"),
		getenv_default("PGPASSWORD", "postgres"),
		getenv_default("PGSSLMODE", "disable"),
		request_lsn,
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
	if (env_is_one("POLARDB_EXTENDED_EXPECT_INERROR")) {
		const PGTransactionStatusType txn_status = PQtransactionStatus(conn);
		if (txn_status != PQTRANS_INTRANS) {
			fprintf(stderr,
				"setup transaction status is %d, expected PQTRANS_INTRANS\n",
				(int)txn_status);
			PQfinish(conn);
			return 3;
		}
		printf("transaction_status_after_setup=T\n");
	}
	if (env_is_one("POLARDB_EXTENDED_PIPELINE_FLUSH")) {
		const int rc = run_pipeline_flush_error(conn, final_sql);
		PQfinish(conn);
		return rc == 0 ? 0 : 6;
	}
	if (env_is_one("POLARDB_EXTENDED_PIPELINE_FLUSH_SUCCESS")) {
		const int rc = run_pipeline_flush_success(conn, final_sql);
		PQfinish(conn);
		return rc == 0 ? 0 : 8;
	}
	if (env_is_one("POLARDB_EXTENDED_PIPELINE_FLUSH_RESET")) {
		const int rc = run_pipeline_flush_reset(conn, final_sql);
		PQfinish(conn);
		return rc == 0 ? 0 : 9;
	}
	if (env_is_one("POLARDB_EXTENDED_PIPELINE_MULTI")) {
		const int rc = run_pipeline_multi(conn, final_sql);
		PQfinish(conn);
		return rc == 0 ? 0 : 7;
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
