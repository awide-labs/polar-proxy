#ifndef PROXYSQL_PGSQL_POLARDB_READER_POOL_INTERNAL_H
#define PROXYSQL_PGSQL_POLARDB_READER_POOL_INTERNAL_H

#include "PgSQL_PolarDB.h"

#if POLARDB_PROXY

class PgSQL_Connection;
class PgSQL_Session;
class PgSQL_Thread;
class PtrArray;
struct PgSQL_PoolMatchKey;

enum class PolarDB_ReaderPoolRejectReason : uint8_t {
	NONE = 0,
	BAD_CONTEXT,
	PROFILE,
	AUTH,
	IDENTITY,
	SESSION_STATE
};

static constexpr unsigned int POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT = 16;

/*
 * Connection-compatibility helpers shared by reader acquisition, local return,
 * and background warmup. Routing eligibility remains in ReaderPool itself.
 */
uint64_t polardb_pool_auth_reuse_key_strings(
	const char* username, const char* dbname);
uint64_t polardb_pool_session_options_reuse_key(
	const PgSQL_Connection* conn);
void polardb_ensure_pool_key(PgSQL_Connection* conn);
PgSQL_PoolMatchKey polardb_core_pool_match_key_for_conn(
	const PgSQL_Connection* conn);
PgSQL_PoolMatchKey polardb_core_pool_match_key_for_request(
	const PolarDB_PoolRequest& request);
PolarDB_PoolRequest polardb_prepare_pool_request_for_session(
	const PolarDB_StartupProfile& startup_profile,
	bool only_pooled, bool require_rfq_profile, PgSQL_Session* sess);
PolarDB_PoolReuseClassification polardb_classify_pool_conn_for_reuse(
	PgSQL_Connection* conn, PgSQL_Session* sess,
	const PolarDB_PoolRequest& pool_request);
void polardb_count_reader_pool_reject(
	PgSQL_Thread* thread, PolarDB_ReaderPoolRejectReason reason);
PolarDB_ReaderPoolRejectReason
polardb_reader_pool_reject_reason_from_reuse_state(
	PolarDB_PoolReuseState state);
bool polardb_reader_pool_conn_usable(
	PgSQL_Connection* conn, PgSQL_Session* sess,
	const PolarDB_PoolRequest& pool_request,
	PolarDB_ReaderPoolRejectReason* reject_reason_out = nullptr);
unsigned int polardb_scan_cached_readers(
	PtrArray* cached_connections);
void polardb_repair_local_reader_count(
	const char* operation, unsigned int* recorded, unsigned int observed);

#endif // POLARDB_PROXY

#endif // PROXYSQL_PGSQL_POLARDB_READER_POOL_INTERNAL_H
