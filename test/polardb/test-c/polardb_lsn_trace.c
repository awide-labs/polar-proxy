/**
 * @file polardb_lsn_trace.c
 * @brief LD_PRELOAD helper that prints client-visible RFQ LSN consistency.
 *
 * This is a diagnostic shim for programs already linked with the
 * PolarDB-patched libpq, such as the bundled pgbench, or sysbench when loaded
 * with this tree's vendored libpq via LD_LIBRARY_PATH.
 *
 * The shim does not change query behavior. It observes libpq calls, reads
 * PQhasLSN()/PQgetLSN() after libpq consumes ReadyForQuery, and prints one
 * parseable line per completed request:
 *
 *   POLARDB_LSN_TRACE ... target=... lsn=... verdict=CONSISTENT
 *
 * The verdict is intentionally client-observable only. Each PGconn has two
 * LSN contexts:
 *   - session context: survives until disconnect; tracks monotonic observed LSN
 *     and committed/autocommit write LSN.
 *   - transaction context: starts at BEGIN, freezes the session target at that
 *     point, tracks in-transaction RFQ/write LSN locally, and is discarded on
 *     ROLLBACK or merged into the session write target on COMMIT.
 *
 * Reads outside a transaction compare RFQ LSN against the session target:
 *   max(session_write_lsn, session_observed_lsn)
 *
 * Reads inside a transaction compare RFQ LSN against the local target:
 *   max(txn_start_target_lsn, txn_write_lsn, txn_observed_lsn)
 *
 * PostgreSQL/libpq uses one ordered protocol stream per PGconn. Normal sync or
 * async use has at most one active command. Pipeline mode can enqueue multiple
 * commands, but results and ReadyForQuery still arrive in order; when multiple
 * commands are pending at one RFQ boundary, this tracer reports a batch-level
 * result rather than pretending there was one RFQ per queued command.
 *
 * This shows the property available to a client through the RFQ-LSN extension.
 * It does not show whether ProxySQL used a replica wait, forced the writer, or
 * took a degraded best-effort path. That route/wait metadata is not currently
 * part of the client RFQ payload.
 */

#define _GNU_SOURCE

#include <ctype.h>
#include <dlfcn.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "libpq-fe.h"

typedef uint64_t (*pq_get_lsn_fn)(const PGconn *);
typedef int (*pq_has_lsn_fn)(const PGconn *);
typedef PGTransactionStatusType (*pq_transaction_status_fn)(const PGconn *);
typedef PGresult *(*pq_exec_fn)(PGconn *, const char *);
typedef PGresult *(*pq_exec_params_fn)(PGconn *, const char *, int, const Oid *,
									   const char *const *, const int *,
									   const int *, int);
typedef PGresult *(*pq_exec_prepared_fn)(PGconn *, const char *, int,
										 const char *const *, const int *,
										 const int *, int);
typedef int (*pq_send_query_fn)(PGconn *, const char *);
typedef int (*pq_send_query_params_fn)(PGconn *, const char *, int,
									   const Oid *, const char *const *,
									   const int *, const int *, int);
typedef int (*pq_send_query_prepared_fn)(PGconn *, const char *, int,
										 const char *const *, const int *,
										 const int *, int);
typedef PGresult *(*pq_get_result_fn)(PGconn *);
typedef void (*pq_finish_fn)(PGconn *);

typedef enum QueryKind {
	QUERY_KIND_UNKNOWN = 0,
	QUERY_KIND_READ,
	QUERY_KIND_WRITE,
	QUERY_KIND_TXN_CTL,
} QueryKind;

typedef enum TxnCtlKind {
	TXN_CTL_NONE = 0,
	TXN_CTL_BEGIN,
	TXN_CTL_COMMIT,
	TXN_CTL_ROLLBACK,
	TXN_CTL_OTHER,
} TxnCtlKind;

typedef struct PendingTrace {
	char *sql;
	QueryKind kind;
	TxnCtlKind txn_ctl;
	bool multi_statement;
	uint64_t session_target_lsn;
	uint64_t txn_target_lsn;
	unsigned long long seq;
	struct PendingTrace *next;
} PendingTrace;

typedef struct ConnTraceState {
	PGconn *conn;
	PendingTrace *pending_head;
	PendingTrace *pending_tail;
	unsigned int pending_count;
	unsigned long long next_seq;

	/* Connection-wide context: survives until PQfinish(). */
	uint64_t session_observed_lsn;
	uint64_t session_write_lsn;

	/* Transaction-local context: meaningful only while txn_active is true. */
	bool txn_active;
	bool txn_had_write;
	uint64_t txn_start_target_lsn;
	uint64_t txn_observed_lsn;
	uint64_t txn_write_lsn;

	struct ConnTraceState *next;
} ConnTraceState;

static pthread_mutex_t trace_lock = PTHREAD_MUTEX_INITIALIZER;
static ConnTraceState *trace_states;
static __thread int suppress_async_rfq_log;

static void *lookup_next_symbol(const char *name) {
	return dlsym(RTLD_NEXT, name);
}

static void *lookup_helper_symbol(const char *name) {
	void *sym = dlsym(RTLD_NEXT, name);
	return sym ? sym : dlsym(RTLD_DEFAULT, name);
}

static bool trace_enabled(void) {
	const char *env = getenv("POLARDB_LSN_TRACE");
	return !env || strcmp(env, "0") != 0;
}

static uint64_t max_u64(uint64_t a, uint64_t b) {
	return a > b ? a : b;
}

static uint64_t max3_u64(uint64_t a, uint64_t b, uint64_t c) {
	return max_u64(max_u64(a, b), c);
}

static void skip_ws_and_comments(const char **p) {
	for (;;) {
		while (**p && isspace((unsigned char)**p)) {
			(*p)++;
		}
		if ((*p)[0] == '-' && (*p)[1] == '-') {
			*p += 2;
			while (**p && **p != '\n') {
				(*p)++;
			}
			continue;
		}
		if ((*p)[0] == '/' && (*p)[1] == '*') {
			*p += 2;
			while ((*p)[0] && !((*p)[0] == '*' && (*p)[1] == '/')) {
				(*p)++;
			}
			if ((*p)[0]) {
				*p += 2;
			}
			continue;
		}
		break;
	}
}

static bool keyword_is(const char *p, const char *kw) {
	size_t n = strlen(kw);
	if (strncasecmp(p, kw, n) != 0) {
		return false;
	}
	return p[n] == '\0' || !isalnum((unsigned char)p[n]);
}

static QueryKind classify_sql(const char *sql) {
	if (!sql) {
		return QUERY_KIND_UNKNOWN;
	}
	const char *p = sql;
	skip_ws_and_comments(&p);
	if (keyword_is(p, "SELECT") || keyword_is(p, "WITH") ||
		keyword_is(p, "SHOW") || keyword_is(p, "EXPLAIN") ||
		keyword_is(p, "VALUES")) {
		return QUERY_KIND_READ;
	}
	if (keyword_is(p, "INSERT") || keyword_is(p, "UPDATE") ||
		keyword_is(p, "DELETE") || keyword_is(p, "MERGE") ||
		keyword_is(p, "CREATE") || keyword_is(p, "ALTER") ||
		keyword_is(p, "DROP") || keyword_is(p, "TRUNCATE") ||
		keyword_is(p, "COPY") || keyword_is(p, "VACUUM") ||
		keyword_is(p, "ANALYZE") || keyword_is(p, "REINDEX") ||
		keyword_is(p, "GRANT") || keyword_is(p, "REVOKE")) {
		return QUERY_KIND_WRITE;
	}
	if (keyword_is(p, "BEGIN") || keyword_is(p, "START") ||
		keyword_is(p, "COMMIT") || keyword_is(p, "END") ||
		keyword_is(p, "ROLLBACK") || keyword_is(p, "SAVEPOINT") ||
		keyword_is(p, "RELEASE")) {
		return QUERY_KIND_TXN_CTL;
	}
	return QUERY_KIND_UNKNOWN;
}

static TxnCtlKind classify_txn_ctl(const char *sql) {
	if (!sql) {
		return TXN_CTL_NONE;
	}
	const char *p = sql;
	skip_ws_and_comments(&p);
	if (keyword_is(p, "BEGIN") || keyword_is(p, "START")) {
		return TXN_CTL_BEGIN;
	}
	if (keyword_is(p, "COMMIT") || keyword_is(p, "END")) {
		return TXN_CTL_COMMIT;
	}
	if (keyword_is(p, "ROLLBACK")) {
		return TXN_CTL_ROLLBACK;
	}
	if (keyword_is(p, "SAVEPOINT") || keyword_is(p, "RELEASE")) {
		return TXN_CTL_OTHER;
	}
	return TXN_CTL_NONE;
}

static bool sql_has_multiple_statements(const char *sql) {
	if (!sql) {
		return false;
	}
	const char *p = sql;
	while (*p) {
		if (*p == '\'') {
			p++;
			while (*p) {
				if (*p == '\'' && p[1] == '\'') {
					p += 2;
					continue;
				}
				if (*p++ == '\'') {
					break;
				}
			}
			continue;
		}
		if (p[0] == '-' && p[1] == '-') {
			p += 2;
			while (*p && *p != '\n') {
				p++;
			}
			continue;
		}
		if (p[0] == '/' && p[1] == '*') {
			p += 2;
			while (p[0] && !(p[0] == '*' && p[1] == '/')) {
				p++;
			}
			if (p[0]) {
				p += 2;
			}
			continue;
		}
		if (*p == ';') {
			p++;
			const char *rest = p;
			skip_ws_and_comments(&rest);
			return *rest != '\0';
		}
		p++;
	}
	return false;
}

static const char *kind_name(QueryKind kind) {
	switch (kind) {
	case QUERY_KIND_READ:
		return "read";
	case QUERY_KIND_WRITE:
		return "write";
	case QUERY_KIND_TXN_CTL:
		return "txn";
	default:
		return "unknown";
	}
}

static const char *txn_ctl_name(TxnCtlKind ctl) {
	switch (ctl) {
	case TXN_CTL_BEGIN:
		return "begin";
	case TXN_CTL_COMMIT:
		return "commit";
	case TXN_CTL_ROLLBACK:
		return "rollback";
	case TXN_CTL_OTHER:
		return "other";
	default:
		return "none";
	}
}

static ConnTraceState *find_state_locked(PGconn *conn, bool create) {
	ConnTraceState *state = trace_states;
	while (state) {
		if (state->conn == conn) {
			return state;
		}
		state = state->next;
	}
	if (!create) {
		return NULL;
	}
	state = (ConnTraceState *)calloc(1, sizeof(*state));
	if (!state) {
		return NULL;
	}
	state->conn = conn;
	state->next = trace_states;
	trace_states = state;
	return state;
}

static void remove_state(PGconn *conn) {
	pthread_mutex_lock(&trace_lock);
	ConnTraceState **cur = &trace_states;
	while (*cur) {
		if ((*cur)->conn == conn) {
			ConnTraceState *dead = *cur;
			*cur = dead->next;
			PendingTrace *pending = dead->pending_head;
			while (pending) {
				PendingTrace *next = pending->next;
				free(pending->sql);
				free(pending);
				pending = next;
			}
			free(dead);
			break;
		}
		cur = &(*cur)->next;
	}
	pthread_mutex_unlock(&trace_lock);
}

static char *copy_sql_snip(const char *sql) {
	if (!sql) {
		return strdup("(null)");
	}
	size_t len = strlen(sql);
	const size_t max_len = 240;
	if (len > max_len) {
		len = max_len;
	}
	char *copy = (char *)malloc(len + 1);
	if (!copy) {
		return NULL;
	}
	memcpy(copy, sql, len);
	copy[len] = '\0';
	for (size_t i = 0; i < len; i++) {
		if (copy[i] == '\n' || copy[i] == '\r' || copy[i] == '\t') {
			copy[i] = ' ';
		}
	}
	return copy;
}

static void enqueue_pending_locked(ConnTraceState *state, PendingTrace *pending) {
	if (!state->pending_tail) {
		state->pending_head = state->pending_tail = pending;
	} else {
		state->pending_tail->next = pending;
		state->pending_tail = pending;
	}
	state->pending_count++;
}

static PendingTrace *pop_pending_locked(ConnTraceState *state) {
	PendingTrace *pending = state->pending_head;
	if (!pending) {
		return NULL;
	}
	state->pending_head = pending->next;
	if (!state->pending_head) {
		state->pending_tail = NULL;
	}
	pending->next = NULL;
	state->pending_count--;
	return pending;
}

static void clear_pending_locked(ConnTraceState *state) {
	PendingTrace *pending = state->pending_head;
	while (pending) {
		PendingTrace *next = pending->next;
		free(pending->sql);
		free(pending);
		pending = next;
	}
	state->pending_head = state->pending_tail = NULL;
	state->pending_count = 0;
}

static void record_query(PGconn *conn, const char *sql) {
	if (!trace_enabled() || !conn) {
		return;
	}
	pthread_mutex_lock(&trace_lock);
	ConnTraceState *state = find_state_locked(conn, true);
	if (state) {
		PendingTrace *pending = (PendingTrace *)calloc(1, sizeof(*pending));
		if (pending) {
			pending->sql = copy_sql_snip(sql);
			pending->kind = classify_sql(sql);
			pending->txn_ctl = classify_txn_ctl(sql);
			pending->multi_statement = sql_has_multiple_statements(sql);
			pending->session_target_lsn =
				max_u64(state->session_write_lsn, state->session_observed_lsn);
			pending->txn_target_lsn = state->txn_active ?
				max3_u64(state->txn_start_target_lsn,
						 state->txn_write_lsn,
						 state->txn_observed_lsn) :
				0;
			pending->seq = ++state->next_seq;
			enqueue_pending_locked(state, pending);
		}
	}
	pthread_mutex_unlock(&trace_lock);
}

static void lsn_to_pg(uint64_t lsn, char *buf, size_t len) {
	snprintf(buf, len, "%X/%X",
			 (unsigned int)(lsn >> 32),
			 (unsigned int)(lsn & 0xffffffffu));
}

static void emit_rfq(PGconn *conn, const char *api) {
	if (!trace_enabled() || !conn) {
		return;
	}

	pq_has_lsn_fn polar_has_lsn = (pq_has_lsn_fn)lookup_helper_symbol("PQhasLSN");
	pq_get_lsn_fn polar_get_lsn = (pq_get_lsn_fn)lookup_helper_symbol("PQgetLSN");
	pq_transaction_status_fn txn_status_fn =
		(pq_transaction_status_fn)lookup_helper_symbol("PQtransactionStatus");
	int has_lsn = 0;
	uint64_t lsn = 0;
	if (polar_has_lsn && polar_get_lsn) {
		has_lsn = polar_has_lsn(conn) ? 1 : 0;
		lsn = polar_get_lsn(conn);
	}
	PGTransactionStatusType pq_txn_status =
		txn_status_fn ? txn_status_fn(conn) : PQTRANS_UNKNOWN;

	pthread_mutex_lock(&trace_lock);
	ConnTraceState *state = find_state_locked(conn, true);
	if (!state || !state->pending_head) {
		pthread_mutex_unlock(&trace_lock);
		return;
	}

	const bool batch_rfq = state->pending_count > 1;
	PendingTrace *pending = pop_pending_locked(state);
	if (batch_rfq) {
		/*
		 * One ReadyForQuery closes an ordered batch (pipeline or a wrapper that
		 * queued more than one command before RFQ). The client can only show a
		 * boundary-level LSN for the whole batch, so collapse remaining pending
		 * commands and make the target conservative.
		 */
		PendingTrace *cur = state->pending_head;
		while (cur) {
			pending->session_target_lsn =
				max_u64(pending->session_target_lsn, cur->session_target_lsn);
			pending->txn_target_lsn =
				max_u64(pending->txn_target_lsn, cur->txn_target_lsn);
			if (cur->kind == QUERY_KIND_WRITE) {
				pending->kind = QUERY_KIND_WRITE;
			}
			cur = cur->next;
		}
		clear_pending_locked(state);
	}

	QueryKind kind = pending->kind;
	TxnCtlKind txn_ctl = pending->txn_ctl;
	uint64_t session_target = pending->session_target_lsn;
	uint64_t txn_target = pending->txn_target_lsn;
	uint64_t target = state->txn_active ? txn_target : session_target;
	const char *scope = state->txn_active ? "txn" : "session";
	if (txn_ctl == TXN_CTL_BEGIN && !state->txn_active) {
		target = session_target;
		scope = "begin";
	}
	const char *verdict = "UNKNOWN";
	if (kind == QUERY_KIND_WRITE) {
		verdict = (has_lsn && lsn > 0) ? "WRITE_LSN_OK" : "WRITE_LSN_UNKNOWN";
	} else if (kind == QUERY_KIND_READ) {
		if (target == 0) {
			verdict = has_lsn ? "NO_PRIOR_TARGET" : "NO_PRIOR_TARGET_NO_LSN";
		} else if (!has_lsn) {
			verdict = "UNKNOWN_NO_RFQ_LSN";
		} else if (lsn >= target) {
			verdict = "CONSISTENT";
		} else {
			verdict = "STALE_BY_LSN";
		}
	} else if (kind == QUERY_KIND_TXN_CTL) {
		verdict = has_lsn ? "TXN_CTL_LSN" : "TXN_CTL_NO_LSN";
	} else {
		verdict = has_lsn ? "OBSERVED_LSN" : "NO_RFQ_LSN";
	}
	if (batch_rfq) {
		verdict = has_lsn ? "BATCH_RFQ_LSN" : "BATCH_NO_RFQ_LSN";
	}

	const bool was_txn_active = state->txn_active;
	if (has_lsn && lsn > state->session_observed_lsn) {
		state->session_observed_lsn = lsn;
	}
	if (was_txn_active && has_lsn && lsn > state->txn_observed_lsn) {
		state->txn_observed_lsn = lsn;
	}
	if (kind == QUERY_KIND_WRITE && has_lsn && lsn > 0) {
		if (was_txn_active) {
			if (lsn > state->txn_write_lsn) {
				state->txn_write_lsn = lsn;
			}
			state->txn_had_write = true;
		} else if (lsn > state->session_write_lsn) {
			state->session_write_lsn = lsn;
		}
	}

	if (txn_ctl == TXN_CTL_BEGIN && !was_txn_active &&
		(pq_txn_status == PQTRANS_INTRANS || pq_txn_status == PQTRANS_INERROR)) {
		state->txn_active = true;
		state->txn_had_write = false;
		state->txn_start_target_lsn = session_target;
		state->txn_observed_lsn = has_lsn ? lsn : 0;
		state->txn_write_lsn = 0;
	} else if ((txn_ctl == TXN_CTL_COMMIT || txn_ctl == TXN_CTL_ROLLBACK) &&
			   was_txn_active &&
			   (pq_txn_status == PQTRANS_IDLE || pq_txn_status == PQTRANS_UNKNOWN)) {
		if (txn_ctl == TXN_CTL_COMMIT && state->txn_had_write &&
			has_lsn && lsn > state->session_write_lsn) {
			state->session_write_lsn = lsn;
		}
		state->txn_active = false;
		state->txn_had_write = false;
		state->txn_start_target_lsn = 0;
		state->txn_observed_lsn = 0;
		state->txn_write_lsn = 0;
	} else if (pq_txn_status == PQTRANS_IDLE && was_txn_active) {
		/* Defensive sync with libpq if a transaction ended via a compound SQL. */
		state->txn_active = false;
		state->txn_had_write = false;
		state->txn_start_target_lsn = 0;
		state->txn_observed_lsn = 0;
		state->txn_write_lsn = 0;
	} else if (!was_txn_active &&
			   (pq_txn_status == PQTRANS_INTRANS || pq_txn_status == PQTRANS_INERROR)) {
		/* Defensive sync when a multi-statement command opened a transaction. */
		state->txn_active = true;
		state->txn_had_write = false;
		state->txn_start_target_lsn = session_target;
		state->txn_observed_lsn = has_lsn ? lsn : 0;
		state->txn_write_lsn = 0;
	}

	char lsn_pg[32];
	char session_target_pg[32];
	char txn_target_pg[32];
	char target_pg[32];
	lsn_to_pg(lsn, lsn_pg, sizeof(lsn_pg));
	lsn_to_pg(session_target, session_target_pg, sizeof(session_target_pg));
	lsn_to_pg(txn_target, txn_target_pg, sizeof(txn_target_pg));
	lsn_to_pg(target, target_pg, sizeof(target_pg));

	fprintf(stderr,
			"POLARDB_LSN_TRACE conn=%p seq=%llu api=%s kind=%s "
			"txn_ctl=%s scope=%s batch=%u multi_stmt=%u has_lsn=%d lsn=%" PRIu64
			" lsn_pg=%s target=%" PRIu64 " target_pg=%s"
			" session_target=%" PRIu64 " session_target_pg=%s"
			" txn_target=%" PRIu64 " txn_target_pg=%s"
			" session_observed=%" PRIu64 " session_write=%" PRIu64
			" txn_active=%d txn_start=%" PRIu64 " txn_observed=%" PRIu64
			" txn_write=%" PRIu64 " verdict=%s sql=\"%s\"\n",
			(void *)conn,
			pending->seq,
			api ? api : "?",
			kind_name(kind),
			txn_ctl_name(txn_ctl),
			scope,
			batch_rfq ? 1u : 0u,
			pending->multi_statement ? 1u : 0u,
			has_lsn,
			lsn,
			lsn_pg,
			target,
			target_pg,
			session_target,
			session_target_pg,
			txn_target,
			txn_target_pg,
			state->session_observed_lsn,
			state->session_write_lsn,
			state->txn_active ? 1 : 0,
			state->txn_start_target_lsn,
			state->txn_observed_lsn,
			state->txn_write_lsn,
			verdict,
			pending->sql ? pending->sql : "(null)");

	free(pending->sql);
	free(pending);
	pthread_mutex_unlock(&trace_lock);
}

PGresult *PQexec(PGconn *conn, const char *query) {
	pq_exec_fn real_fn = (pq_exec_fn)lookup_next_symbol("PQexec");
	record_query(conn, query);
	suppress_async_rfq_log++;
	PGresult *res = real_fn ? real_fn(conn, query) : NULL;
	suppress_async_rfq_log--;
	emit_rfq(conn, "PQexec");
	return res;
}

PGresult *PQexecParams(PGconn *conn, const char *command, int nParams,
					   const Oid *paramTypes, const char *const *paramValues,
					   const int *paramLengths, const int *paramFormats,
					   int resultFormat) {
	pq_exec_params_fn real_fn = (pq_exec_params_fn)lookup_next_symbol("PQexecParams");
	record_query(conn, command);
	suppress_async_rfq_log++;
	PGresult *res = real_fn ?
		real_fn(conn, command, nParams, paramTypes, paramValues,
				paramLengths, paramFormats, resultFormat) :
		NULL;
	suppress_async_rfq_log--;
	emit_rfq(conn, "PQexecParams");
	return res;
}

PGresult *PQexecPrepared(PGconn *conn, const char *stmtName, int nParams,
						 const char *const *paramValues, const int *paramLengths,
						 const int *paramFormats, int resultFormat) {
	pq_exec_prepared_fn real_fn = (pq_exec_prepared_fn)lookup_next_symbol("PQexecPrepared");
	char buf[256];
	snprintf(buf, sizeof(buf), "EXECUTE_PREPARED %s", stmtName ? stmtName : "(null)");
	record_query(conn, buf);
	suppress_async_rfq_log++;
	PGresult *res = real_fn ?
		real_fn(conn, stmtName, nParams, paramValues, paramLengths,
				paramFormats, resultFormat) :
		NULL;
	suppress_async_rfq_log--;
	emit_rfq(conn, "PQexecPrepared");
	return res;
}

int PQsendQuery(PGconn *conn, const char *query) {
	pq_send_query_fn real_fn = (pq_send_query_fn)lookup_next_symbol("PQsendQuery");
	if (suppress_async_rfq_log == 0) {
		record_query(conn, query);
	}
	return real_fn ? real_fn(conn, query) : 0;
}

int PQsendQueryParams(PGconn *conn, const char *command, int nParams,
					  const Oid *paramTypes, const char *const *paramValues,
					  const int *paramLengths, const int *paramFormats,
					  int resultFormat) {
	pq_send_query_params_fn real_fn =
		(pq_send_query_params_fn)lookup_next_symbol("PQsendQueryParams");
	if (suppress_async_rfq_log == 0) {
		record_query(conn, command);
	}
	return real_fn ?
		real_fn(conn, command, nParams, paramTypes, paramValues,
				paramLengths, paramFormats, resultFormat) :
		0;
}

int PQsendQueryPrepared(PGconn *conn, const char *stmtName, int nParams,
						const char *const *paramValues, const int *paramLengths,
						const int *paramFormats, int resultFormat) {
	pq_send_query_prepared_fn real_fn =
		(pq_send_query_prepared_fn)lookup_next_symbol("PQsendQueryPrepared");
	char buf[256];
	snprintf(buf, sizeof(buf), "EXECUTE_PREPARED %s", stmtName ? stmtName : "(null)");
	if (suppress_async_rfq_log == 0) {
		record_query(conn, buf);
	}
	return real_fn ?
		real_fn(conn, stmtName, nParams, paramValues, paramLengths,
				paramFormats, resultFormat) :
		0;
}

PGresult *PQgetResult(PGconn *conn) {
	pq_get_result_fn real_fn = (pq_get_result_fn)lookup_next_symbol("PQgetResult");
	PGresult *res = real_fn ? real_fn(conn) : NULL;
	if (!res && suppress_async_rfq_log == 0) {
		emit_rfq(conn, "PQgetResult");
	}
	return res;
}

void PQfinish(PGconn *conn) {
	pq_finish_fn real_fn = (pq_finish_fn)lookup_next_symbol("PQfinish");
	remove_state(conn);
	if (real_fn) {
		real_fn(conn);
	}
}
