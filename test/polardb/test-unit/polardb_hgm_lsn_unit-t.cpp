/**
 * @file polardb_hgm_lsn_unit-t.cpp
 * @brief Unit tests for the PolarDB HostGroups Manager LSN state.
 *
 * Domain: counter metadata and thread-counter aggregation, writer-epoch
 * LSN-cache reset, and thread-local fresh-LSN reader targeting.
 */

#define POLARDB_UNIT_FULL_HARNESS 1

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_ExplicitTxnStateMgr.h"
#include "PgSQL_PolarDB_ReaderPool.h"
#include "postgres_fe.h"
#include "libpq-int.h"
#undef snprintf
#undef vsnprintf

#include <atomic>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

extern PgSQL_HostGroups_Manager *PgHGM;
extern PgSQL_Threads_Handler *GloPTH;

#if !POLARDB_PROXY
int main() {
	plan(1);
	ok(1, "PolarDB HGM LSN unit skipped when POLARDB_PROXY is disabled");
	return exit_status();
}
#else

// This is the full-harness unit: it links libproxysql.a and constructs real
// ProxySQL HGM/connection components. Opt in to the full-harness section of the
// shared helper header (HGM fixtures + stage_polardb_topology) by defining this
// before the include; the header-only tests never define it. Included inside the
// POLARDB_PROXY branch because those fixtures reference PolarDB-only fields.
#include "polardb_unit_common.h"

size_t pgsql_polardb_unit_collect_split_warmup_targets(
		PgSQL_PolarDB_ReaderPool* pool,
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free) {
	pool->polardb_collect_split_warmup_targets_locked(
		req, target_requests, found_hostgroup,
		saw_eligible_target, saw_compatible_free);
	return target_requests.size();
}

static void attach_test_frontend(PgSQL_Session& sess) {
	sess.connections_handler = true;
	sess.client_myds = new PgSQL_Data_Stream();
	sess.client_myds->init(MYDS_FRONTEND, &sess, 0);
	sess.client_myds->myconn = new PgSQL_Connection(true);
	set_test_userinfo(sess.client_myds->myconn);
	set_test_pgsql_defaults(sess.client_myds->myconn);
	sess.client_myds->addr.addr = strdup("127.0.0.1");
	sess.client_myds->addr.port = 5432;
	sess.client_myds->proxy_addr.addr = strdup("127.0.0.10");
	sess.client_myds->proxy_addr.port = 6033;
}

static PolarDB_StartupIdentity unit_proxy_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.10",
		6033,
		PolarDB_StartupIdentitySource::LISTENER_PROXY};
}

static PolarDB_StartupIdentity unit_other_proxy_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.11",
		6033,
		PolarDB_StartupIdentitySource::LISTENER_PROXY};
}

static PGconn *unit_connected_pgconn() {
	PGconn *conn = PQconnectStart(
		"host=127.0.0.1 port=1 connect_timeout=1");
	if (conn) {
		conn->status = CONNECTION_OK;
	}
	return conn;
}

static PgSQL_SplitWarmupRequest make_unit_warmup_request(
	unsigned int reader_hg, unsigned int max_connections_per_request);
static uint64_t unit_reader_pool_options_key(PgSQL_Connection *conn);

static void unit_warmup_request_use_connection_state(
		PgSQL_SplitWarmupRequest& req,
		PgSQL_Connection *conn) {
	if (!conn) {
		return;
	}
	req.startup_client = conn->polardb_startup_client;
	req.startup_identity_mode = conn->polardb_startup_identity_mode;
	req.startup_options_hash = unit_reader_pool_options_key(conn);
	req.has_startup_parameters = true;
	req.startup_parameters.clear();
	req.startup_parameter_hash.clear();
	req.startup_parameters.reserve(PGSQL_NAME_LAST_LOW_WM);
	req.startup_parameter_hash.reserve(PGSQL_NAME_LAST_LOW_WM);
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		req.startup_parameters.push_back(
			conn->variables[i].value ? conn->variables[i].value : "");
		req.startup_parameter_hash.push_back(conn->var_hash[i]);
	}
}

static unsigned int unit_reader_pool_shared_free_count(PgSQL_SrvC *reader) {
	return reader ? reader->pool_free_count_value() : 0;
}

static uint64_t unit_reader_pool_auth_key(PgSQL_Connection *conn) {
	if (!conn || !conn->userinfo || !conn->userinfo->username ||
			!conn->userinfo->dbname) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	hash = polardb_pool_hash_cstr(hash, conn->userinfo->username);
	hash = polardb_pool_hash_cstr(hash, conn->userinfo->dbname);
	return hash;
}

static uint64_t unit_reader_pool_options_key(PgSQL_Connection *conn) {
	if (!conn) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		hash = polardb_pool_hash_u64(hash, conn->var_hash[i]);
	}
	hash = polardb_pool_hash_u64(hash, conn->dynamic_variables_idx.size());
	for (uint32_t idx : conn->dynamic_variables_idx) {
		hash = polardb_pool_hash_u64(hash, idx);
		hash = polardb_pool_hash_u64(hash, conn->var_hash[idx]);
	}
	return hash ? hash : 1;
}

static void unit_reader_pool_refresh_key(PgSQL_Connection *conn) {
	if (!conn) {
		return;
	}
	conn->polardb_pool_key.auth_hash =
		unit_reader_pool_auth_key(conn);
	conn->polardb_pool_key.startup_identity_hash =
		polardb_startup_client_reuse_key(conn->polardb_startup_client);
	conn->polardb_pool_key.startup_options_hash =
		unit_reader_pool_options_key(conn);
}

static void unit_reader_pool_prepare_free_conn(
		PgSQL_SrvC *reader, PgSQL_Connection *conn) {
	conn->parent = reader;
	conn->pgsql_conn = PQconnectStart("polardb_unit_invalid_conninfo=1");
	unit_reader_pool_refresh_key(conn);
}

static PgSQL_PoolMatchKey unit_reader_pool_match_key(
		PgSQL_Connection *conn) {
	PgSQL_PoolMatchKey core_key;
	if (!conn) {
		return core_key;
	}
	unit_reader_pool_refresh_key(conn);
	core_key.words[0] = conn->polardb_startup_profile_generation;
	core_key.words[1] = conn->polardb_pool_key.auth_hash;
	core_key.words[2] = conn->polardb_pool_key.startup_identity_hash;
	core_key.words[3] = conn->polardb_pool_key.startup_options_hash;
	return core_key;
}

static void unit_reader_pool_add_matching(
		PgSQL_SrvC *reader, PgSQL_Connection *conn) {
	const PgSQL_PoolMatchKey core_key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, core_key));
}

static void unit_reader_pool_add_shared(
		PgSQL_SrvC *reader, PgSQL_Connection *conn) {
	unit_reader_pool_prepare_free_conn(reader, conn);
	unit_reader_pool_add_matching(reader, conn);
}

static bool unit_reader_pool_clear_shared(
		PgSQL_SrvC *reader, PgSQL_Connection *conn) {
	if (!reader->remove_free_connection(conn)) {
		return false;
	}
	conn->pgsql_conn = nullptr;
	return true;
}

static void test_polardb_counter_metadata() {
	int thread_count = 0;
	int global_count = 0;
	int gauge_count = 0;
	const char *wait_lsn_prom_name = nullptr;
	const char *wait_bypass_prom_name = nullptr;
	const char *split_fallback_reader_busy_prom_name = nullptr;
	const char *split_fallback_reader_lag_exceeded_prom_name = nullptr;
	const char *txn_wait_reader_reconciled_prom_name = nullptr;
	const char *warmup_bad_request_prom_name = nullptr;
	const char *warmup_target_attempts_prom_name = nullptr;
	const char *warmup_target_failed_prom_name = nullptr;
	const char *warmup_connect_failed_prom_name = nullptr;
	const char *warmup_add_failed_prom_name = nullptr;
	const char *warmup_pending_prom_name = nullptr;
	const char *drop_client_identity_prom_name = nullptr;
	const char *parent_bytes_flush_recv_atomic_prom_name = nullptr;
	const char *writev_attempts_prom_name = nullptr;
	const char *output_coalesce_hold_prom_name = nullptr;
	const char *result_row_run_attempts_prom_name = nullptr;
	const char *result_row_run_used_prom_name = nullptr;
	const char *result_row_run_not_candidate_prom_name = nullptr;

#define X(name, display_name, prom_name, help) \
	++thread_count; \
	if (strcmp(#name, "wait_lsn_sum_us") == 0) wait_lsn_prom_name = prom_name; \
	if (strcmp(#name, "wait_wrap_bypassed") == 0) wait_bypass_prom_name = prom_name; \
	if (strcmp(#name, "split_fallback_reader_busy") == 0) split_fallback_reader_busy_prom_name = prom_name; \
	if (strcmp(#name, "split_fallback_reader_lag_exceeded") == 0) split_fallback_reader_lag_exceeded_prom_name = prom_name; \
	if (strcmp(#name, "txn_wait_reader_reconciled") == 0) txn_wait_reader_reconciled_prom_name = prom_name; \
	if (strcmp(#name, "reader_pool_drop_client_identity") == 0) drop_client_identity_prom_name = prom_name; \
	if (strcmp(#name, "parent_bytes_flush_recv_atomic") == 0) parent_bytes_flush_recv_atomic_prom_name = prom_name; \
	if (strcmp(#name, "writev_attempts") == 0) writev_attempts_prom_name = prom_name; \
	if (strcmp(#name, "output_coalesce_hold") == 0) output_coalesce_hold_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_attempts") == 0) result_row_run_attempts_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_used") == 0) result_row_run_used_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_not_candidate") == 0) result_row_run_not_candidate_prom_name = prom_name;
	POLARDB_THREAD_COUNTER_LIST(X)
#undef X

#define X(name, display_name, prom_name, help) \
	++global_count; \
	if (strcmp(#name, "split_warmup_bad_request") == 0) warmup_bad_request_prom_name = prom_name; \
	if (strcmp(#name, "split_warmup_target_attempts") == 0) warmup_target_attempts_prom_name = prom_name; \
	if (strcmp(#name, "split_warmup_target_failed") == 0) warmup_target_failed_prom_name = prom_name; \
	if (strcmp(#name, "split_warmup_connect_failed") == 0) warmup_connect_failed_prom_name = prom_name; \
	if (strcmp(#name, "split_warmup_add_failed") == 0) warmup_add_failed_prom_name = prom_name;
	POLARDB_GLOBAL_COUNTER_LIST(X)
#undef X

#define X(name, display_name, prom_name, help) \
	++gauge_count; \
	if (strcmp(#name, "warmup_pending") == 0) warmup_pending_prom_name = prom_name;
	POLARDB_GAUGE_LIST(X)
#undef X

	ok(thread_count == POLARDB_THREAD_COUNTER_COUNT,
		"PolarDB counters: thread metadata list has expected size");
	ok(global_count == POLARDB_GLOBAL_COUNTER_COUNT,
		"PolarDB counters: global metadata list has expected size");
	ok(gauge_count == 1,
		"PolarDB counters: gauge metadata list has expected size");
	ok(thread_count + global_count == POLARDB_ALL_COUNTER_COUNT,
		"PolarDB counters: all metadata list covers thread plus global counters");
	ok(wait_lsn_prom_name != nullptr &&
			strcmp(wait_lsn_prom_name,
				"proxysql_polardb_wait_lsn_microseconds_total") == 0,
		"PolarDB counters: wait-latency Prometheus metric keeps microsecond units");
	ok(wait_bypass_prom_name != nullptr &&
			strcmp(wait_bypass_prom_name,
				"proxysql_polardb_wait_wrap_bypassed_total") == 0,
		"PolarDB counters: wait-bypass Prometheus metric is registered from metadata");
	ok(split_fallback_reader_busy_prom_name != nullptr &&
			strcmp(split_fallback_reader_busy_prom_name,
				"proxysql_polardb_split_fallback_reader_busy_total") == 0,
		"PolarDB counters: split reader-busy fallback metric is registered from metadata");
	ok(split_fallback_reader_lag_exceeded_prom_name != nullptr &&
			strcmp(split_fallback_reader_lag_exceeded_prom_name,
				"proxysql_polardb_split_fallback_reader_lag_exceeded_total") == 0,
		"PolarDB counters: split lag-cap fallback metric is registered from metadata");
	ok(txn_wait_reader_reconciled_prom_name != nullptr &&
			strcmp(txn_wait_reader_reconciled_prom_name,
				"proxysql_polardb_txn_wait_reader_reconciled_total") == 0,
		"PolarDB counters: txn-wait reader reconciliation metric is registered from metadata");
	ok(drop_client_identity_prom_name != nullptr &&
			strcmp(drop_client_identity_prom_name,
				"proxysql_polardb_reader_pool_drop_client_identity_total") == 0,
		"PolarDB counters: client-identity drop metric is registered from metadata");
	ok(parent_bytes_flush_recv_atomic_prom_name != nullptr &&
			strcmp(parent_bytes_flush_recv_atomic_prom_name,
				"proxysql_polardb_parent_bytes_flush_recv_atomic_total") == 0,
		"PolarDB counters: parent-byte flush atomic metric is registered from metadata");
	ok(writev_attempts_prom_name != nullptr &&
			strcmp(writev_attempts_prom_name,
				"proxysql_polardb_writev_attempts_total") == 0,
		"PolarDB counters: writev attempt metric is registered from metadata");
	ok(output_coalesce_hold_prom_name != nullptr &&
			strcmp(output_coalesce_hold_prom_name,
				"proxysql_polardb_output_coalesce_hold_total") == 0,
		"PolarDB counters: output coalesce hold metric is registered from metadata");
	ok(result_row_run_attempts_prom_name != nullptr &&
			strcmp(result_row_run_attempts_prom_name,
				"proxysql_polardb_result_row_run_attempts_total") == 0,
		"PolarDB counters: result row-run attempt metric is registered from metadata");
	ok(result_row_run_used_prom_name != nullptr &&
			strcmp(result_row_run_used_prom_name,
				"proxysql_polardb_result_row_run_used_total") == 0,
		"PolarDB counters: result row-run metric is registered from metadata");
	ok(result_row_run_not_candidate_prom_name != nullptr &&
			strcmp(result_row_run_not_candidate_prom_name,
				"proxysql_polardb_result_row_run_not_candidate_total") == 0,
		"PolarDB counters: result row-run not-candidate metric is registered from metadata");
	ok(warmup_bad_request_prom_name != nullptr &&
			strcmp(warmup_bad_request_prom_name,
				"proxysql_polardb_split_warmup_bad_request_total") == 0,
		"PolarDB counters: warmup bad-request metric is always registered");
	ok(warmup_target_attempts_prom_name != nullptr &&
			strcmp(warmup_target_attempts_prom_name,
				"proxysql_polardb_split_warmup_target_attempts_total") == 0,
		"PolarDB counters: warmup target-attempt metric is always registered");
	ok(warmup_target_failed_prom_name != nullptr &&
			strcmp(warmup_target_failed_prom_name,
				"proxysql_polardb_split_warmup_target_failed_total") == 0,
		"PolarDB counters: warmup target-failed metric is always registered");
	ok(warmup_connect_failed_prom_name != nullptr &&
			strcmp(warmup_connect_failed_prom_name,
				"proxysql_polardb_split_warmup_connect_failed_total") == 0,
		"PolarDB counters: warmup connect-failed metric is always registered");
	ok(warmup_add_failed_prom_name != nullptr &&
			strcmp(warmup_add_failed_prom_name,
				"proxysql_polardb_split_warmup_add_failed_total") == 0,
		"PolarDB counters: warmup add-failed metric is always registered");
	ok(warmup_pending_prom_name != nullptr &&
			strcmp(warmup_pending_prom_name,
				"proxysql_polardb_warmup_pending") == 0,
		"PolarDB counters: warmup pending is registered as a gauge metric");
}

static void test_polardb_parent_byte_flush_accounting() {
	PgSQL_SrvC srv(
		const_cast<char*>("polardb-byte-counter"), 15432, 100,
		MYSQL_SERVER_STATUS_ONLINE, 0, 100, 0, 0, 0,
		const_cast<char*>(""));
	PgSQL_Connection backend(false);
	backend.parent = &srv;

	const auto detach_before =
		PgHGM->status.polardb_parent_bytes_flush_detach.load(std::memory_order_relaxed);
	const auto recv_atomic_before =
		PgHGM->status.polardb_parent_bytes_flush_recv_atomic.load(std::memory_order_relaxed);
	const auto sent_atomic_before =
		PgHGM->status.polardb_parent_bytes_flush_sent_atomic.load(std::memory_order_relaxed);
	const auto recv_bytes_before =
		PgHGM->status.polardb_parent_bytes_flush_recv_bytes.load(std::memory_order_relaxed);
	const auto sent_bytes_before =
		PgHGM->status.polardb_parent_bytes_flush_sent_bytes.load(std::memory_order_relaxed);

	PgSQL_Data_Stream backend_stream;
	backend_stream.attach_connection(&backend);
	backend.polardb_parent_bytes_recv_pending = 123;
	backend.polardb_parent_bytes_sent_pending = 45;
	backend.polardb_parent_queries_sent_pending = 63;
	backend.polardb_parent_query_batch_count = 63;
	backend_stream.detach_connection();

	ok(srv.bytes_recv == 0 && srv.bytes_sent == 0 && srv.queries_sent == 63,
		"PolarDB parent bytes: detach keeps bytes local and publishes queries");
	ok(backend.polardb_parent_bytes_recv_pending == 123 &&
			backend.polardb_parent_bytes_sent_pending == 45 &&
			backend.polardb_parent_queries_sent_pending == 0 &&
			backend.polardb_parent_query_batch_count == 63,
		"PolarDB parent bytes: detach preserves only pending byte accounting");
	ok(PgHGM->status.polardb_parent_bytes_flush_detach.load(std::memory_order_relaxed) ==
			detach_before,
		"PolarDB parent bytes: detach does not force a shared counter update");

	backend.update_queries_sent();

	ok(srv.bytes_recv == 123,
		"PolarDB parent bytes: recv bytes flush into the server container");
	ok(srv.bytes_sent == 45,
		"PolarDB parent bytes: sent bytes flush into the server container");
	ok(srv.queries_sent == 64,
		"PolarDB parent bytes: query threshold flushes the accumulated query count");
	ok(backend.polardb_parent_bytes_recv_pending == 0 &&
			backend.polardb_parent_bytes_sent_pending == 0 &&
			backend.polardb_parent_queries_sent_pending == 0 &&
			backend.polardb_parent_query_batch_count == 0,
		"PolarDB parent bytes: flush clears pending bytes");
	ok(PgHGM->status.polardb_parent_bytes_flush_detach.load(std::memory_order_relaxed) ==
			detach_before,
		"PolarDB parent bytes: threshold flush does not report a detach");
	ok(PgHGM->status.polardb_parent_bytes_flush_recv_atomic.load(std::memory_order_relaxed) ==
			recv_atomic_before + 1,
		"PolarDB parent bytes: recv atomic flush is counted");
	ok(PgHGM->status.polardb_parent_bytes_flush_sent_atomic.load(std::memory_order_relaxed) ==
			sent_atomic_before + 1,
		"PolarDB parent bytes: sent atomic flush is counted");
	ok(PgHGM->status.polardb_parent_bytes_flush_recv_bytes.load(std::memory_order_relaxed) ==
			recv_bytes_before + 123,
		"PolarDB parent bytes: recv flushed-byte total is counted");
	ok(PgHGM->status.polardb_parent_bytes_flush_sent_bytes.load(std::memory_order_relaxed) ==
			sent_bytes_before + 45,
		"PolarDB parent bytes: sent flushed-byte total is counted");

	const auto recv_atomic_after =
		PgHGM->status.polardb_parent_bytes_flush_recv_atomic.load(std::memory_order_relaxed);
	backend.polardb_flush_parent_bytes(PolarDB_ParentBytesFlushReason::Manual);
	ok(PgHGM->status.polardb_parent_bytes_flush_recv_atomic.load(std::memory_order_relaxed) ==
			recv_atomic_after,
		"PolarDB parent bytes: empty flush does not count a flush reason");

	const auto destructor_before =
		PgHGM->status.polardb_parent_bytes_flush_destructor.load(std::memory_order_relaxed);
	{
		PgSQL_Connection closing_backend(false);
		closing_backend.parent = &srv;
		closing_backend.polardb_parent_bytes_recv_pending = 7;
		closing_backend.polardb_parent_bytes_sent_pending = 9;
		closing_backend.polardb_parent_queries_sent_pending = 1;
	}
	ok(srv.bytes_recv == 130 && srv.bytes_sent == 54 && srv.queries_sent == 65,
		"PolarDB parent bytes: connection destruction publishes pending accounting");
	ok(PgHGM->status.polardb_parent_bytes_flush_destructor.load(std::memory_order_relaxed) ==
			destructor_before + 1,
		"PolarDB parent bytes: destructor flush reason is counted");
}

static void test_polardb_writev_direct_send() {
	{
		int small_fds[2] = {-1, -1};
		ok(socketpair(AF_UNIX, SOCK_STREAM, 0, small_fds) == 0,
			"PolarDB writev: small-batch socketpair fixture is available");
		if (small_fds[0] >= 0 && small_fds[1] >= 0) {
			ProxySQL_Poll<PgSQL_Data_Stream> small_polls;
			std::unique_ptr<PgSQL_Thread> small_worker(new PgSQL_Thread());
			small_worker->curtime = monotonic_time();

			PgSQL_Session small_sess;
			small_sess.thread = small_worker.get();
			attach_test_frontend(small_sess);
			PgSQL_Data_Stream* small_myds = small_sess.client_myds;
			small_myds->fd = small_fds[0];
			small_myds->DSS = STATE_CLIENT_AUTH_OK;
			small_polls.add(POLLIN | POLLOUT, small_fds[0], small_myds, 0);
			pgsql_thread___polardb_writev_direct = true;

			char* small_first = static_cast<char*>(l_alloc(5));
			memcpy(small_first, "abcde", 5);
			small_myds->PSarrayOUT->add(small_first, 5);

			const auto small_before =
				small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_small_batch_fallback];
			const auto attempts_before =
				small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_attempts];
			const auto buffered_before =
				small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_buffered_fallback];
			small_sess.writeout();
			char small_received[8] = {};
			const ssize_t small_read_bytes =
				recv(small_fds[1], small_received, sizeof(small_received), MSG_DONTWAIT);

			ok(small_read_bytes == 5 &&
					memcmp(small_received, "abcde", 5) == 0,
				"PolarDB writev: session writeout sends a small buffered result");
			ok(small_myds->PSarrayOUT->len == 0 &&
					small_myds->queueOUT.head == small_myds->queueOUT.tail,
				"PolarDB writev: small buffered result leaves no queued output");
			ok(small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_small_batch_fallback] ==
					small_before + 1,
				"PolarDB writev: small-batch fallback is counted once per writeout");
			ok(small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_attempts] == attempts_before &&
				small_worker->polardb_status_variables.stvar[
					polardb_st_var_writev_buffered_fallback] == buffered_before,
				"PolarDB writev: small buffered output does not count a direct attempt");

			close(small_fds[1]);
		}
	}

	int fds[2] = {-1, -1};
	ok(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0,
		"PolarDB writev: socketpair fixture is available");
	if (fds[0] < 0 || fds[1] < 0) return;

	ProxySQL_Poll<PgSQL_Data_Stream> polls;
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	worker->curtime = monotonic_time();

	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PgSQL_Data_Stream* myds = sess.client_myds;
	myds->fd = fds[0];
	myds->DSS = STATE_CLIENT_AUTH_OK;
	myds->myconn->set_status(true, STATUS_PGSQL_CONNECTION_COMPRESSION);
	polls.add(POLLIN | POLLOUT, fds[0], myds, 0);
	pgsql_thread___polardb_writev_direct = true;

	const size_t second_size = QUEUE_T_DEFAULT_SIZE + 2;
	char* first = static_cast<char*>(l_alloc(3));
	char* second = static_cast<char*>(l_alloc(second_size));
	memcpy(first, "abc", 3);
	memset(second, 'x', second_size);
	second[0] = 'd';
	second[1] = 'e';
	myds->PSarrayOUT->add(first, 3);
	myds->PSarrayOUT->add(second, second_size);

	const auto attempts_before =
		worker->polardb_status_variables.stvar[polardb_st_var_writev_attempts];
	const auto bytes_before =
		worker->polardb_status_variables.stvar[polardb_st_var_writev_bytes];
	const auto packets_before =
		worker->polardb_status_variables.stvar[polardb_st_var_writev_packets];

	const int first_written = myds->polardb_writev_to_net_poll(4);
	char received[8] = {};
	const ssize_t first_read_bytes =
		recv(fds[1], received, sizeof(received), MSG_DONTWAIT);

	ok(first_written == 4, "PolarDB writev: direct send honors the byte budget");
	ok(first_read_bytes == 4 && memcmp(received, "abcd", 4) == 0,
		"PolarDB writev: direct send preserves packet order through a partial packet");
	ok(myds->DSS == STATE_SLEEP,
		"PolarDB writev: direct frontend send preserves auth-complete state transition");
	ok(!myds->myconn->get_status(STATUS_PGSQL_CONNECTION_COMPRESSION),
		"PolarDB writev: direct frontend send clears compression status like buffered send");
	ok(myds->PSarrayOUT->len == 1 && myds->polardb_write_head_partial == 1,
		"PolarDB writev: partial packet remains queued with a head offset");

	pgsql_thread___polardb_writev_direct = false;
	const int second_written = myds->polardb_writev_to_net_poll(second_size - 1);
	memset(received, 0, sizeof(received));
	const ssize_t second_read_bytes =
		recv(fds[1], received, sizeof(received), MSG_DONTWAIT);

	ok(static_cast<size_t>(second_written) == second_size - 1,
		"PolarDB writev: second direct send reports remaining bytes");
	ok(second_read_bytes > 0 && received[0] == 'e',
		"PolarDB writev: second direct send starts from the remaining partial byte");
	ok(myds->PSarrayOUT->len == 0 && myds->polardb_write_head_partial == 0,
		"PolarDB writev: fully sent packets are removed from PSarrayOUT");
	ok(worker->polardb_status_variables.stvar[polardb_st_var_writev_attempts] ==
			attempts_before + 2,
		"PolarDB writev: attempt counter increments");
	ok(worker->polardb_status_variables.stvar[polardb_st_var_writev_bytes] ==
			bytes_before + 3 + second_size,
		"PolarDB writev: byte counter increments by accepted bytes");
	ok(worker->polardb_status_variables.stvar[polardb_st_var_writev_packets] ==
			packets_before + 2,
		"PolarDB writev: packet counter counts fully sent packets");

	close(fds[1]);
}

static int polardb_ordered_counter_index(const char *needle) {
	const char *expected[] = {
#define X(name, display_name, prom_name, help) display_name,
		POLARDB_COUNTER_LIST(X, X)
#undef X
	};
	const size_t expected_count = sizeof(expected) / sizeof(expected[0]);

	for (size_t i = 0; i < expected_count; ++i) {
		if (strcmp(expected[i], needle) == 0) {
			return (int)i;
		}
	}
	return -1;
}

static void test_polardb_counter_order_metadata() {
	const int monitor_lsn =
		polardb_ordered_counter_index("PolarDB_LSN_Updates_From_Monitor");
	const int monitor_role =
		polardb_ordered_counter_index("PolarDB_Monitor_Health_Invalid_Role");
	const int monitor_values =
		polardb_ordered_counter_index("PolarDB_Monitor_Health_Invalid_Values");
	const int stale =
		polardb_ordered_counter_index("PolarDB_LSN_Stale_Count");
	const int writer_retry =
		polardb_ordered_counter_index("PolarDB_Wait_Reads_Retried_On_Writer");
	const int rfq_skipped =
		polardb_ordered_counter_index("PolarDB_RFQ_Profile_Skipped");
	const int split_for_update =
		polardb_ordered_counter_index("PolarDB_Split_Rejected_For_Update");
	const int split_write_unknown =
		polardb_ordered_counter_index("PolarDB_Split_Rejected_Write_LSN_Unknown");
	const int split_observed_unknown =
		polardb_ordered_counter_index("PolarDB_Split_Rejected_Observed_LSN_Unknown");
	const int split_wal_pending =
		polardb_ordered_counter_index("PolarDB_Split_WAL_Pending");

	ok(monitor_lsn >= 0 && monitor_lsn < monitor_role &&
			monitor_role < monitor_values && monitor_values < stale,
		"PolarDB counters: ordered metadata keeps monitor counters with monitor LSN");
	ok(writer_retry >= 0 && rfq_skipped >= 0 && writer_retry < rfq_skipped,
		"PolarDB counters: ordered metadata keeps retry before RFQ profile counters");
	ok(split_for_update >= 0 && split_for_update < split_write_unknown &&
			split_write_unknown < split_observed_unknown &&
			split_observed_unknown < split_wal_pending,
		"PolarDB counters: ordered metadata keeps split unknown-LSN vetoes with split rejections");
}

static void test_polardb_thread_counter_aggregation_and_fold() {
	ok(GloPTH != nullptr && PgHGM != nullptr,
		"PolarDB counters: test globals are initialized");
	if (!GloPTH || !PgHGM) {
		return;
	}

	if (!GloPTH->pgsql_threads) {
		GloPTH->init(2, 0);
	}
	GloPTH->status_variables.threads_initialized = 1;

	PgSQL_Thread *worker1 = new PgSQL_Thread();
	PgSQL_Thread *worker2 = new PgSQL_Thread();
	GloPTH->pgsql_threads[0].worker = worker1;
	GloPTH->pgsql_threads[1].worker = worker2;

	// Aggregation arithmetic the assertions below verify:
	//   target_lsn_preferred = 10 (global) + 2 (worker1) + 3 (worker2) = 15
	//   wait_lsn_sum_us       = 100 (global) + 7 (worker1) + 11 (worker2) = 118
	//   wait_wrap_bypassed    = 5 (global) + 13 (worker1) + 17 (worker2) = 35
	const uint64_t GLOBAL_TARGET_LSN_PREFERRED = 10;
	const uint64_t GLOBAL_WAIT_LSN_SUM_US = 100;
	const uint64_t GLOBAL_WAIT_WRAP_BYPASSED = 5;
	const uint64_t EXPECTED_TARGET_LSN_PREFERRED = 15;   // 10 + 2 + 3
	const uint64_t EXPECTED_WAIT_LSN_SUM_US = 118;       // 100 + 7 + 11
	const uint64_t EXPECTED_WAIT_WRAP_BYPASSED = 35;     // 5 + 13 + 17
	PgHGM->status.polardb_target_lsn_preferred.store(
		GLOBAL_TARGET_LSN_PREFERRED, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_lsn_sum_us.store(
		GLOBAL_WAIT_LSN_SUM_US, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_wrap_bypassed.store(
		GLOBAL_WAIT_WRAP_BYPASSED, std::memory_order_relaxed);
	worker1->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 2;
	worker2->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 3;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 7;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 11;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 13;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 17;

	// The enum and metadata list must stay tied together. Do not use a fixed
	// number here because POLARDB_PROFILE intentionally adds more counters.
	ok(POLARDB_st_var_END == POLARDB_THREAD_COUNTER_COUNT,
		"PolarDB counters: thread counter enum matches metadata count");
	ok(strcmp(
			polardb_thread_counter_display_name(polardb_st_var_wait_lsn_sum_us),
			"PolarDB_Wait_LSN_Sum_Us") == 0,
		"PolarDB counters: display name is derived from the thread counter list");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == EXPECTED_TARGET_LSN_PREFERRED,
		"PolarDB counters: aggregation sums global counter and live worker counters");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_wait_lsn_sum_us,
			PgHGM->status.polardb_wait_lsn_sum_us) == EXPECTED_WAIT_LSN_SUM_US,
		"PolarDB counters: aggregation preserves value-bearing wait sum");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_wait_wrap_bypassed,
			PgHGM->status.polardb_wait_wrap_bypassed) == EXPECTED_WAIT_WRAP_BYPASSED,
		"PolarDB counters: aggregation includes wait-wrapper bypasses");

	const uint64_t GLOBAL_LSN_UPDATES_FROM_MONITOR = 6;
	PgHGM->status.polardb_lsn_updates_from_monitor.store(
		GLOBAL_LSN_UPDATES_FROM_MONITOR, std::memory_order_relaxed);
	PgHGM->status.polardb_warmup_pending.store(3, std::memory_order_relaxed);
	PgHGM->p_update_metrics();
	// Each Prometheus export is checked as two invariants (family present, value
	// correct) so a wrong value reports the observed number instead of a bare 0/1.
	double metric_value = 0.0;
	ok(find_prometheus_counter_value(
			"proxysql_polardb_target_lsn_preferred_total", &metric_value),
		"PolarDB counters: Prometheus exports thread-backed totals");
	ok(metric_value == (double)EXPECTED_TARGET_LSN_PREFERRED,
		"PolarDB counters: thread-backed total value is %g (expected %g)",
		metric_value, (double)EXPECTED_TARGET_LSN_PREFERRED);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_wait_lsn_microseconds_total", &metric_value),
		"PolarDB counters: Prometheus exports wait sum in microseconds");
	ok(metric_value == (double)EXPECTED_WAIT_LSN_SUM_US,
		"PolarDB counters: wait-sum microsecond value is %g (expected %g)",
		metric_value, (double)EXPECTED_WAIT_LSN_SUM_US);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_wait_wrap_bypassed_total", &metric_value),
		"PolarDB counters: Prometheus exports wait-wrapper bypasses");
	ok(metric_value == (double)EXPECTED_WAIT_WRAP_BYPASSED,
		"PolarDB counters: wait-wrapper bypass value is %g (expected %g)",
		metric_value, (double)EXPECTED_WAIT_WRAP_BYPASSED);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_lsn_updates_from_monitor_total", &metric_value),
		"PolarDB counters: Prometheus exports global-only counters");
	ok(metric_value == (double)GLOBAL_LSN_UPDATES_FROM_MONITOR,
		"PolarDB counters: global-only counter value is %g (expected %g)",
		metric_value, (double)GLOBAL_LSN_UPDATES_FROM_MONITOR);
	ok(find_prometheus_gauge_value(
			"proxysql_polardb_warmup_pending", &metric_value),
		"PolarDB counters: Prometheus exports warmup pending as a gauge");
	ok(metric_value == 3.0,
		"PolarDB counters: warmup pending gauge value is %g (expected 3)",
		metric_value);

	// A null-thread counter increment lands directly on the global slot (worker
	// slots are untouched): 10 global + 4 = 14.
	POLARDB_THREAD_COUNT(nullptr, target_lsn_preferred, 4);
	ok(PgHGM->status.polardb_target_lsn_preferred.load(std::memory_order_relaxed) == 14,
		"PolarDB counters: null-thread fallback increments the global counter");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == 19,  // 14 global + 2 + 3 workers
		"PolarDB counters: aggregation includes null-thread global counter");

	delete worker1;
	GloPTH->pgsql_threads[0].worker = nullptr;
	delete worker2;
	GloPTH->pgsql_threads[1].worker = nullptr;

	ok(PgHGM->status.polardb_target_lsn_preferred.load(std::memory_order_relaxed) == 19,
		"PolarDB counters: worker teardown folds counters into global counter");
	ok(PgHGM->status.polardb_wait_lsn_sum_us.load(std::memory_order_relaxed) == 118,
		"PolarDB counters: worker teardown folds value-bearing wait sum");
	ok(PgHGM->status.polardb_wait_wrap_bypassed.load(std::memory_order_relaxed) == 35,
		"PolarDB counters: worker teardown folds wait-wrapper bypasses");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == 19,
		"PolarDB counters: total is preserved after worker teardown");

	PgHGM->status.polardb_target_lsn_preferred.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_lsn_sum_us.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_wrap_bypassed.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_lsn_updates_from_monitor.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_warmup_pending.store(0, std::memory_order_relaxed);
}

static void test_writer_epoch_change_resets_lsn_caches() {
	const int writer_hg = 910;
	const int reader_hg = 911;

	// Pre-epoch-change LSN cache seed values (arbitrary distinct nonzero LSNs).
	const uint64_t SEED_PRIMARY_MIRROR_LSN = 0x5000;
	const uint64_t SEED_OLD_WRITER_LSN = 0x5100;
	const uint64_t SEED_READER_LSN = 0x5200;
	const unsigned long long SEED_OLD_WRITER_LSN_TS = 101;
	const unsigned long long SEED_READER_LSN_TS = 202;

	stage_polardb_topology(PgHGM, "PolarDB HGM initial",
		writer_hg, "polardb-writer-old", 15432,
		reader_hg, "polardb-reader", 15433);

	const auto* writer_cfg = PgHGM->find_polardb_hg_config(writer_hg);
	const auto* reader_cfg = PgHGM->find_polardb_hg_config(reader_hg);
	ok(writer_cfg != nullptr && writer_cfg->is_polardb_hostgroup,
		"PolarDB HGM: writer snapshot pointer lookup finds PolarDB config");
	ok(writer_cfg != nullptr && writer_cfg->writer_hostgroup == writer_hg,
		"PolarDB HGM: writer snapshot pointer preserves writer hostgroup");
	ok(writer_cfg != nullptr && writer_cfg->reader_hostgroup == reader_hg,
		"PolarDB HGM: writer snapshot pointer preserves reader hostgroup");
	ok(writer_cfg != nullptr && reader_cfg != nullptr &&
			reader_cfg->writer_epoch == writer_cfg->writer_epoch,
		"PolarDB HGM: reader and writer snapshot entries share writer epoch cell");
	const auto writer_policy = PgHGM->get_polardb_hg_policy(writer_hg);
	ok(writer_cfg != nullptr &&
			writer_policy.txn_split_enabled == writer_cfg->policy.txn_split_enabled,
		"PolarDB HGM: policy accessor reads from snapshot config");

	PgSQL_HGC *writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	ok(writer_hgc != nullptr && writer_hgc->repl_config.configured,
		"PolarDB HGM: writer replication config loaded");
	ok(reader_hgc != nullptr && reader_hgc->mysrvs->cnt() == 1,
		"PolarDB HGM: reader hostgroup is present");

	PgSQL_SrvC *old_writer =
		find_pgsql_server(writer_hgc, "polardb-writer-old", 15432);
	PgSQL_SrvC *reader = find_pgsql_server(reader_hgc, "polardb-reader", 15433);
	ok(old_writer != nullptr && reader != nullptr,
		"PolarDB HGM: writer and reader server containers are available");

	writer_hgc->repl_config.polardb_primary_lsn->store(
		SEED_PRIMARY_MIRROR_LSN, std::memory_order_relaxed);
	old_writer->polardb_current_lsn.store(SEED_OLD_WRITER_LSN, std::memory_order_relaxed);
	old_writer->lsn_updated_at.store(SEED_OLD_WRITER_LSN_TS, std::memory_order_relaxed);
	reader->polardb_current_lsn.store(SEED_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(SEED_READER_LSN_TS, std::memory_order_relaxed);

	// Re-stage the same replication group with a NEW writer address: this is the
	// writer-identity change under test, so only servers_add + commit (no
	// replication-hostgroups reload) — not the full stage_polardb_topology block.
	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg, "polardb-writer-new", 15432,
			reader_hg, "polardb-reader", 15433)) == 0,
		"PolarDB HGM: changed writer identity staged for commit");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB HGM: changed writer topology commit succeeds");
	writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_SrvC *new_writer =
		find_pgsql_server(writer_hgc, "polardb-writer-new", 15432);
	ok(new_writer != nullptr,
		"PolarDB HGM: new writer server container is available");

	ok(writer_hgc->repl_config.polardb_primary_lsn->load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears primary LSN mirror");
	ok(writer_hgc->repl_config.polardb_writer_epoch->load(std::memory_order_relaxed) == 1,
		"PolarDB HGM: writer epoch increments after writer identity changes");
	// Old writer + paired reader LSN caches are cleared: assert the cached LSN and
	// its freshness timestamp as separate invariants for each server.
	ok(old_writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears old writer cached LSN");
	ok(old_writer->lsn_updated_at.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears old writer LSN timestamp");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears paired reader cached LSN");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears paired reader LSN timestamp");
}

static void test_hostgroup_config_cache_refreshes_after_reload() {
	const int writer_hg = 912;
	const int reader_hg = 913;

	stage_polardb_topology(PgHGM, "PolarDB hostgroup config cache",
		writer_hg, "polardb-config-cache-writer", 16432,
		reader_hg, "polardb-config-cache-reader", 16433);

	const auto* first = PgHGM->find_polardb_hg_config(writer_hg);
	const auto* repeated = PgHGM->find_polardb_hg_config(writer_hg);
	ok(first != nullptr && repeated == first,
		"PolarDB hostgroup config cache: repeated lookup returns the same config");
	ok(first != nullptr && !first->policy.txn_split_enabled,
		"PolarDB hostgroup config cache: initial policy disables transaction split");
	ok(PgHGM->find_polardb_hg_config(999999) == nullptr &&
			PgHGM->find_polardb_hg_config(999999) == nullptr,
		"PolarDB hostgroup config cache: repeated missing hostgroup stays missing");

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB hostgroup config cache reload",
		writer_hg, "polardb-config-cache-writer", 16432,
		reader_hg, "polardb-config-cache-reader", 16433);

	const auto* refreshed = PgHGM->find_polardb_hg_config(writer_hg);
	ok(refreshed != nullptr && refreshed->policy.txn_split_enabled,
		"PolarDB hostgroup config cache: topology reload refreshes the policy");
}

static void test_monitor_lsn_update_skips_non_online_servers() {
	const int writer_hg = 930;
	const int reader_hg = 931;
	const uint64_t BLOCKED_LSN = 0x9100;
	const uint64_t ACCEPTED_LSN = 0x9200;

	stage_polardb_topology(PgHGM, "PolarDB monitor LSN check",
		writer_hg, "polardb-monitor-writer", 17432,
		reader_hg, "polardb-monitor-reader", 17433);

	PgSQL_HGC *writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_SrvC *writer =
		find_pgsql_server(writer_hgc, "polardb-monitor-writer", 17432);
	ok(writer_hgc != nullptr && writer != nullptr,
		"PolarDB monitor LSN check: writer server container is available");
	if (!writer_hgc || !writer) {
		return;
	}

	writer->status = MYSQL_SERVER_STATUS_SHUNNED;
	ok(!PgHGM->polardb_update_server_lsn(
			"polardb-monitor-writer", 17432, BLOCKED_LSN),
		"PolarDB monitor LSN check: non-ONLINE server update is rejected");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN check: non-ONLINE server LSN cache stays empty");
	ok(writer_hgc->repl_config.polardb_primary_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN check: non-ONLINE server does not update primary mirror");

	writer->status = MYSQL_SERVER_STATUS_ONLINE;
	ok(PgHGM->polardb_update_server_lsn(
			"polardb-monitor-writer", 17432, ACCEPTED_LSN),
		"PolarDB monitor LSN check: ONLINE server update is accepted");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN check: ONLINE server updates its LSN cache");
	ok(writer_hgc->repl_config.polardb_primary_lsn->load(
			std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN check: ONLINE writer updates primary mirror");
}

static void test_lsn_observation_refreshes_freshness_timestamp() {
	const int writer_hg = 940;
	const int reader_hg = 941;
	const uint64_t FIRST_LSN = 0xA100;
	const uint64_t LOWER_LSN = 0xA000;
	const uint64_t FIRST_TS = 100;
	const uint64_t LOWER_TS = 200;
	const uint64_t EQUAL_TS = 300;

	stage_polardb_topology(PgHGM, "PolarDB LSN freshness",
		writer_hg, "polardb-freshness-writer", 18432,
		reader_hg, "polardb-freshness-reader", 18433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-freshness-reader", 18433);
	ok(reader != nullptr,
		"PolarDB LSN freshness: reader server container is available");
	if (!reader) {
		return;
	}

	ok(reader->polardb_advance_lsn(FIRST_LSN, FIRST_TS),
		"PolarDB LSN freshness: first valid sample advances cached LSN");
	ok(!reader->polardb_advance_lsn(LOWER_LSN, LOWER_TS),
		"PolarDB LSN freshness: lower sample is not counted as an advance");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == FIRST_LSN,
		"PolarDB LSN freshness: lower sample does not lower cached LSN");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == LOWER_TS,
		"PolarDB LSN freshness: lower sample refreshes observation timestamp");
	ok(!reader->polardb_advance_lsn(FIRST_LSN, EQUAL_TS),
		"PolarDB LSN freshness: equal sample is not counted as an advance");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == EQUAL_TS,
		"PolarDB LSN freshness: equal sample refreshes observation timestamp");
}

static void test_v2_target_reader_keeps_wait_until_lsn_is_reached() {
	const int writer_hg = 920;
	const int reader_hg = 921;

	stage_polardb_topology(PgHGM, "PolarDB local reader",
		writer_hg, "polardb-local-writer", 16432,
		reader_hg, "polardb-local-reader", 16433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-local-reader", 16433);
	ok(reader != nullptr,
		"PolarDB local reader: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());

	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	// The reader must have applied at least REQUIRED_READER_LSN to satisfy the
	// wait target. STALE_READER_LSN sits below it; the fresh value equals it.
	const uint64_t REQUIRED_READER_LSN = 0x1200;
	const uint64_t PRIMARY_LSN = 0x1300;
	const uint64_t MAX_LAG_BYTES = 0x400;
	const uint64_t STALE_READER_LSN = 0x1100;   // below REQUIRED_READER_LSN
	const uint64_t FRESH_READER_LSN = REQUIRED_READER_LSN;

	PolarDB_Query_ReaderPlan plan;
	plan.primary_lsn = PRIMARY_LSN;
	plan.max_lag_bytes = MAX_LAG_BYTES;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::lsn(
		REQUIRED_READER_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	PgSQL_Connection *cached = make_cached_reader_connection(reader);
	ok(cached != nullptr, "PolarDB local reader: cached connection fixture is available");
	if (!cached) {
		return;
	}
	cached->pgsql_conn = unit_connected_pgconn();
	ok(cached->pgsql_conn != nullptr,
		"PolarDB v2 reader: connected libpq fixture is available");
	reader->polardb_current_lsn.store(STALE_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	unit_reader_pool_add_matching(reader, cached);

	PolarDB_ReaderResult stale_result =
		PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(stale_result.acquired(),
		"PolarDB v2 reader: reader below the target remains usable with the backend wait");
	ok(!stale_result.wait_bypass_allowed,
		"PolarDB v2 reader: reader below the target cannot bypass wait wrapping");
	if (!stale_result.acquired()) {
		return;
	}
	PgHGM->push_MyConn_to_pool(stale_result.conn);

	reader->polardb_current_lsn.store(FRESH_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	PolarDB_ReaderResult hit_result =
		PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	PgSQL_Connection *hit = hit_result.conn;
	ok(hit_result.acquired(),
		"PolarDB v2 reader: fresh RFQ connection is acquired");
	ok(hit == cached,
		"PolarDB v2 reader: the selected server returns its exact matching connection");
	ok(hit_result.wait_bypass_allowed,
		"PolarDB v2 reader: fresh RFQ connection can bypass wait wrapping");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_target_lsn_preferred] == 1,
		"PolarDB v2 reader: hit increments target-LSN preferred counter");

	if (hit) {
		PgHGM->push_MyConn_to_pool(hit);
		reader->remove_free_connection(hit);
		delete hit;
	}
}

static bool unit_snapshot_contains_server(
		const std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>& snapshot,
		unsigned int hostgroup_id,
		PgSQL_SrvC* server) {
	if (!snapshot || !server) {
		return false;
	}
	auto it = snapshot->by_hostgroup.find(hostgroup_id);
	if (it == snapshot->by_hostgroup.end()) {
		return false;
	}
	for (const auto& candidate : it->second.servers) {
		if (candidate.srv == server) {
			return true;
		}
	}
	return false;
}

static SQLite3_result *make_pgsql_servers_result_single(
		int hostgroup_id, const char *addr, int port) {
	SQLite3_result *result = new SQLite3_result(11);
	char hostgroup_buf[16];
	char port_buf[16];
	snprintf(hostgroup_buf, sizeof(hostgroup_buf), "%d", hostgroup_id);
	snprintf(port_buf, sizeof(port_buf), "%d", port);

	char *row[] = {
		hostgroup_buf,
		(char*)addr,
		port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"0",
		(char*)"polardb server-list snapshot unit"
	};
	result->add_row(row);
	return result;
}

static SQLite3_result *make_pgsql_servers_result_with_reader_options(
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_addr, int reader_port,
		int64_t reader_weight, int64_t reader_max_connections,
		unsigned int reader_max_latency_ms) {
	SQLite3_result *result = new SQLite3_result(11);
	char writer_hg_buf[16];
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char reader_port_buf[16];
	char reader_weight_buf[32];
	char reader_max_connections_buf[32];
	char reader_max_latency_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", reader_port);
	snprintf(reader_weight_buf, sizeof(reader_weight_buf), "%ld",
		static_cast<long>(reader_weight));
	snprintf(reader_max_connections_buf, sizeof(reader_max_connections_buf),
		"%ld", static_cast<long>(reader_max_connections));
	snprintf(reader_max_latency_buf, sizeof(reader_max_latency_buf), "%u",
		reader_max_latency_ms);

	char *writer_row[] = {
		writer_hg_buf,
		(char*)writer_addr,
		writer_port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"0",
		(char*)"polardb selector snapshot writer"
	};
	result->add_row(writer_row);

	char *reader_row[] = {
		reader_hg_buf,
		(char*)reader_addr,
		reader_port_buf,
		(char*)"ONLINE",
		reader_weight_buf,
		(char*)"0",
		reader_max_connections_buf,
		(char*)"0",
		(char*)"0",
		reader_max_latency_buf,
		(char*)"polardb selector snapshot reader"
	};
	result->add_row(reader_row);
	return result;
}

static const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry*
unit_snapshot_server_entry(
		const std::shared_ptr<const PgSQL_HostGroups_Manager::
			PolarDB_ServerListSnapshot>& snapshot,
		unsigned int hostgroup_id,
		PgSQL_SrvC* server) {
	if (!snapshot || !server) {
		return nullptr;
	}
	auto hostgroup = snapshot->by_hostgroup.find(hostgroup_id);
	if (hostgroup == snapshot->by_hostgroup.end()) {
		return nullptr;
	}
	for (const auto& entry : hostgroup->second.servers) {
		if (entry.srv == server) {
			return &entry;
		}
	}
	return nullptr;
}

static bool unit_snapshot_server_options_equal(
		const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry* entry,
		int64_t weight, int64_t max_connections,
		unsigned int max_latency_us) {
	return entry && entry->weight == weight &&
		entry->max_connections == max_connections &&
		entry->max_latency_us == max_latency_us;
}

static void test_server_selection_snapshot_refresh_and_immutability() {
	const int writer_hg = 934;
	const int reader_hg = 935;
	const char *writer_addr = "polardb-selector-snapshot-writer";
	const char *reader_addr = "polardb-selector-snapshot-reader";
	const int writer_port = 17442;
	const int reader_port = 17443;

	stage_polardb_topology(PgHGM, "PolarDB selector snapshot",
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), reader_addr, reader_port);
	auto before = PgHGM->get_polardb_server_list_snapshot();
	const auto *before_entry =
		unit_snapshot_server_entry(before, reader_hg, reader);
	ok(unit_snapshot_server_options_equal(before_entry, 1, 50, 0),
		"PolarDB selector snapshot: initial server options are captured");

	ok(PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port,
			7, 160, 200)) == 0,
		"PolarDB selector snapshot: updated server options are staged");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB selector snapshot: updated server options are committed");
	auto after = PgHGM->get_polardb_server_list_snapshot();
	const auto *after_entry =
		unit_snapshot_server_entry(after, reader_hg, reader);
	ok(before && after && after->generation > before->generation &&
			unit_snapshot_server_options_equal(after_entry, 7, 160, 200000),
		"PolarDB selector snapshot: reload publishes a new complete option set");
	ok(unit_snapshot_server_options_equal(before_entry, 1, 50, 0),
		"PolarDB selector snapshot: retained snapshot keeps its original options");
}

static void test_reader_selection_reload_concurrency() {
	const int writer_hg = 936;
	const int reader_hg = 937;
	const char *writer_addr = "polardb-selector-race-writer";
	const char *reader_addr = "polardb-selector-race-reader";
	const int writer_port = 17452;
	const int reader_port = 17453;

	stage_polardb_topology(PgHGM, "PolarDB selector reload",
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	ok(PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port,
			3, 80, 100)) == 0,
		"PolarDB selector reload: initial options are staged");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB selector reload: initial options are committed");

	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), reader_addr, reader_port);
	ok(reader != nullptr,
		"PolarDB selector reload: reader server container is available");
	if (!reader) {
		return;
	}
	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	std::atomic<bool> stop{false};
	std::atomic<unsigned int> reload_failures{0};
	std::atomic<unsigned int> mixed_snapshots{0};
	std::atomic<unsigned long long> acquisitions{0};
	std::atomic<unsigned long long> latency_updates{0};

	std::thread selector([&]() {
		std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
		PgSQL_Session sess;
		sess.thread = worker.get();
		attach_test_frontend(sess);
		PolarDB_Query_ReaderPlan plan;
		plan.fallback_writer_hg = writer_hg;
		PolarDB_WaitSpec no_wait;
		while (!stop.load(std::memory_order_acquire)) {
			PolarDB_ReaderResult result = PgHGM->get_MyConn_polardb_reader(
				reader_hg, &sess, plan, no_wait,
				PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
			if (!result.acquired()) {
				std::this_thread::yield();
				continue;
			}
			auto selected_snapshot = std::static_pointer_cast<const
				PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>(
					result.conn->polardb_selected_server_snapshot);
			const auto *entry = unit_snapshot_server_entry(
				selected_snapshot, reader_hg, result.srv);
			const bool options_a = unit_snapshot_server_options_equal(
				entry, 3, 80, 100000);
			const bool options_b = unit_snapshot_server_options_equal(
				entry, 7, 160, 200000);
			if (!options_a && !options_b) {
				mixed_snapshots.fetch_add(1, std::memory_order_relaxed);
			}
			PgHGM->push_MyConn_to_pool(result.conn);
			acquisitions.fetch_add(1, std::memory_order_relaxed);
		}
	});

	std::thread monitor([&]() {
		unsigned int latency_us = 25;
		while (!stop.load(std::memory_order_acquire)) {
			PgHGM->set_server_current_latency_us(
				(char*)reader_addr, reader_port, latency_us);
			latency_us = latency_us == 25 ? 50 : 25;
			latency_updates.fetch_add(1, std::memory_order_relaxed);
		}
	});

	for (unsigned int iteration = 0; iteration < 40; iteration++) {
		const bool use_options_b = (iteration & 1U) != 0;
		if (PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
				writer_hg, writer_addr, writer_port,
				reader_hg, reader_addr, reader_port,
				use_options_b ? 7 : 3,
				use_options_b ? 160 : 80,
				use_options_b ? 200 : 100)) != 0 ||
				!PgHGM->commit({}, {}, false, false)) {
			reload_failures.fetch_add(1, std::memory_order_relaxed);
			break;
		}
	}
	stop.store(true, std::memory_order_release);
	selector.join();
	monitor.join();

	ok(reload_failures.load(std::memory_order_relaxed) == 0,
		"PolarDB selector reload: concurrent configuration reloads complete");
	ok(acquisitions.load(std::memory_order_relaxed) > 0 &&
			latency_updates.load(std::memory_order_relaxed) > 0,
		"PolarDB selector reload: selection and latency updates overlap reloads");
	ok(mixed_snapshots.load(std::memory_order_relaxed) == 0,
		"PolarDB selector reload: selection sees only complete option sets");

	reader->remove_free_connection(conn);
	delete conn;
}

static void test_server_list_snapshot_survives_topology_purge() {
	const int writer_hg = 930;
	const int reader_hg = 931;

	stage_polardb_topology(PgHGM, "PolarDB server-list snapshot",
		writer_hg, "polardb-snapshot-writer", 17432,
		reader_hg, "polardb-snapshot-reader", 17433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-snapshot-reader", 17433);
	auto before = PgHGM->get_polardb_server_list_snapshot();
	ok(reader != nullptr && unit_snapshot_contains_server(
			before, reader_hg, reader),
		"PolarDB server-list snapshot: current list contains staged reader");

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-snapshot-writer", 17432)) == 0,
		"PolarDB server-list snapshot: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB server-list snapshot: missing reader marked offline");
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-snapshot-writer", 17432)) == 0,
		"PolarDB server-list snapshot: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB server-list snapshot: offline reader purged");

	auto after = PgHGM->get_polardb_server_list_snapshot();
	ok(!unit_snapshot_contains_server(after, reader_hg, reader),
		"PolarDB server-list snapshot: new list excludes purged reader");
	ok(unit_snapshot_contains_server(before, reader_hg, reader),
		"PolarDB server-list snapshot: old list remains readable during scan");
}

static void test_reader_pool_request_limits() {
	ok(PGSQL_POLARDB_TXN_READER_ONLY_POOLED,
		"PolarDB reader request policy: transaction reader paths use pooled-only acquisition");
	ok(pgsql_split_warmup_batch_has_capacity(0),
		"PolarDB warmup batch limit: empty batch accepts work");
	ok(pgsql_split_warmup_batch_has_capacity(
			PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT - 1),
		"PolarDB warmup batch limit: last slot accepts work");
	ok(!pgsql_split_warmup_batch_has_capacity(
			PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT),
		"PolarDB warmup batch limit: full batch defers work");

	PolarDB_StartupProfile startup_profile;
	PolarDB_PoolRequest pooled_request =
		polardb_make_read_pool_request(startup_profile, true, 0, 0);
	ok(!pooled_request.allow_create,
		"PolarDB reader request policy: pooled-only requests cannot create");
	PolarDB_PoolRequest ordinary_request =
		polardb_make_read_pool_request(startup_profile, false, 0, 0);
	ok(ordinary_request.allow_create,
		"PolarDB reader request policy: ordinary requests can create");
}

static void test_core_match_pool_index_and_transfer() {
	const int writer_hg = 900;
	const int reader_hg = 901;
	stage_polardb_topology(PgHGM, "PostgreSQL core match pool",
		writer_hg, "polardb-core-exact-writer", 22322,
		reader_hg, "polardb-core-exact-reader", 22323);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), "polardb-core-exact-reader", 22323);
	ok(reader != nullptr,
		"PostgreSQL core match pool: selected server fixture is available");
	if (!reader) {
		return;
	}

	PgSQL_PoolMatchKey key_a;
	key_a.words[0] = 1;
	key_a.words[1] = 11;
	PgSQL_PoolMatchKey key_b;
	key_b.words[0] = 2;
	key_b.words[1] = 22;
	PgSQL_Connection* conn_a = make_cached_reader_connection(reader);
	PgSQL_Connection* conn_b = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(conn_a, key_a) &&
			reader->add_matching_connection(conn_b, key_b),
		"PostgreSQL core match pool: two opaque match keys are added to one FREE owner");
	ok(reader->pool_free_count_value() == 2 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: adding connections updates the shared counts");

	PgSQL_Connection* taken = reader->take_matching_connection(key_a);
	ok(taken == conn_a,
		"PostgreSQL core match pool: only the requested key is returned");
	ok(reader->pool_free_count_value() == 1 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core match pool: taking a connection moves FREE to USED together");
	ok(reader->take_matching_connection(key_a) == nullptr,
		"PostgreSQL core match pool: an empty key never returns a random connection");
	ok(reader->return_matching_connection(taken, key_a) &&
			reader->pool_free_count_value() == 2 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: returning restores the key lookup and counts");

	const PgSQL_PoolGetMode all_actions =
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
		PgSQL_PoolGetMode::ALLOW_RESET |
		PgSQL_PoolGetMode::ALLOW_CREATE;
	ok(pgsql_pool_get_mode_has(
			all_actions, PgSQL_PoolGetMode::ALLOW_EXACT_MATCH) &&
			pgsql_pool_get_mode_has(
				all_actions, PgSQL_PoolGetMode::ALLOW_RESET) &&
			pgsql_pool_get_mode_has(
				all_actions, PgSQL_PoolGetMode::ALLOW_CREATE),
		"PostgreSQL core match pool: all allowed actions are explicit and composable");
	PgSQL_PoolGetResult allowed = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, key_a, nullptr,
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH);
	ok(allowed.conn == conn_a &&
			allowed.source == PgSQL_PoolGetSource::EXACT_MATCH,
		"PostgreSQL core match pool: ALLOW_EXACT_MATCH takes only a matching connection");
	reader->return_matching_connection(allowed.conn, key_a);
	PgSQL_PoolGetResult denied = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, key_a, nullptr, PgSQL_PoolGetMode::NONE);
	ok(denied.conn == nullptr && denied.source == PgSQL_PoolGetSource::NONE,
		"PostgreSQL core match pool: NONE performs no pool action");

	PgSQL_Connection* used_a = reader->take_matching_connection(key_a);
	PgSQL_Connection* used_b = reader->take_matching_connection(key_b);
	ok(used_a == conn_a && used_b == conn_b,
		"PostgreSQL core match pool: saturation fixture moves all free connections to USED");
	PgSQL_PoolMatchKey missing_key;
	missing_key.words[0] = 3;
	missing_key.words[1] = 33;
	const int saved_max_connections = reader->max_connections;
	reader->max_connections = 3;
	PgSQL_PoolGetResult snapshot_full =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/2);
	ok(!snapshot_full.conn && snapshot_full.server_saturated,
		"PostgreSQL core match pool: snapshot capacity reports confirmed saturation under the pool lock");
	reader->max_connections = 1;
	PgSQL_PoolGetResult snapshot_has_room =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/3);
	ok(!snapshot_has_room.conn && !snapshot_has_room.server_saturated,
		"PostgreSQL core match pool: saturation uses the immutable selection limit instead of mutable server state");
	reader->max_connections = saved_max_connections;
	ok(reader->return_matching_connection(used_a, key_a),
		"PostgreSQL core match pool: one used connection returns after saturation check");
	PgSQL_PoolGetResult free_available =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/2);
	ok(!free_available.conn && !free_available.server_saturated,
		"PostgreSQL core match pool: any free connection prevents confirmed saturation");
	ok(reader->return_matching_connection(used_b, key_b),
		"PostgreSQL core match pool: second used connection returns after saturation check");

	reader->remove_free_connection(conn_a);
	reader->remove_free_connection(conn_b);
	delete conn_a;
	delete conn_b;

	PgSQL_Connection* offline_conn = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(offline_conn, key_a) &&
			reader->take_matching_connection(key_a) == offline_conn,
		"PostgreSQL core match pool: offline test connection moves to USED");
	reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	ok(!reader->return_matching_connection(offline_conn, key_a) &&
			reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: offline return removes USED without adding FREE");
	delete offline_conn;
	reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
}

static void test_core_match_pool_concurrent_transfer() {
	const int writer_hg = 904;
	const int reader_hg = 905;
	stage_polardb_topology(PgHGM, "PostgreSQL core match concurrency",
		writer_hg, "polardb-core-lock-writer", 22342,
		reader_hg, "polardb-core-lock-reader", 22343);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), "polardb-core-lock-reader", 22343);
	ok(reader != nullptr,
		"PostgreSQL core match concurrency: selected server fixture is available");
	if (!reader) {
		return;
	}

	PgSQL_PoolMatchKey key;
	key.words[0] = 7;
	key.words[1] = 77;
	static constexpr unsigned int connection_count = 16;
	static constexpr unsigned int thread_count = 16;
	static constexpr unsigned int transfers_per_thread = 500;
	std::vector<PgSQL_Connection*> connections;
	for (unsigned int i = 0; i < connection_count; i++) {
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		connections.push_back(conn);
		assert(reader->add_matching_connection(conn, key));
	}

	std::atomic<unsigned int> failures{0};
	std::vector<std::thread> threads;
	for (unsigned int i = 0; i < thread_count; i++) {
		threads.emplace_back([&]() {
			for (unsigned int n = 0; n < transfers_per_thread; n++) {
				PgSQL_Connection* conn = nullptr;
				while ((conn = reader->take_matching_connection(key)) == nullptr) {
					std::this_thread::yield();
				}
				if (!reader->return_matching_connection(conn, key)) {
					failures.fetch_add(1, std::memory_order_relaxed);
					delete conn;
					break;
				}
			}
		});
	}
	for (std::thread& thread : threads) {
		thread.join();
	}

	ok(failures.load(std::memory_order_relaxed) == 0 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == connection_count &&
			reader->matching_connection_count(key) == connection_count,
		"PostgreSQL core match concurrency: per-server lock preserves list, count, and key ownership");
	for (PgSQL_Connection* conn : connections) {
		reader->remove_free_connection(conn);
		delete conn;
	}
}

static void test_core_match_pool_concurrent_boundaries() {
	const int writer_hg = 906;
	const int reader_hg = 907;
	stage_polardb_topology(PgHGM, "PostgreSQL core boundary concurrency",
		writer_hg, "polardb-core-boundary-writer", 22352,
		reader_hg, "polardb-core-boundary-reader", 22353);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-core-boundary-reader", 22353);
	ok(reader != nullptr,
		"PostgreSQL core boundary concurrency: selected server fixture is available");
	if (!reader) {
		return;
	}
	reader->max_connections = 1000;
	const bool old_attributes_configured = reader->myhgc->attributes.configured;
	const int old_free_connections_pct =
		reader->myhgc->attributes.free_connections_pct;
	reader->myhgc->attributes.configured = true;
	reader->myhgc->attributes.free_connections_pct = 100;

	PgSQL_PoolMatchKey key;
	auto add_connections = [&]() {
		for (unsigned int i = 0; i < 8; i++) {
			PgSQL_Connection* conn = make_cached_reader_connection(reader);
			conn->pgsql_conn = unit_connected_pgconn();
			conn->creation_time = monotonic_time();
			conn->last_time_used = 1;
			const PgSQL_PoolMatchKey conn_key =
				unit_reader_pool_match_key(conn);
			if (key.empty()) {
				key = conn_key;
			} else {
				assert(key == conn_key);
			}
			assert(reader->add_matching_connection(conn, conn_key));
		}
	};
	add_connections();

	std::atomic<bool> stop{false};
	std::atomic<unsigned long long> transfers{0};
	std::atomic<unsigned long long> ping_extractions{0};
	std::atomic<unsigned long long> missing_server_snapshot_connections{0};
	std::atomic<unsigned int> sweep_count{0};
	std::vector<std::thread> takers;
	for (unsigned int i = 0; i < 8; i++) {
		takers.emplace_back([&]() {
			while (!stop.load(std::memory_order_acquire)) {
				PgSQL_Connection* conn = reader->take_matching_connection(key);
				if (!conn) {
					std::this_thread::yield();
					continue;
				}
				if (!reader->return_matching_connection(conn, key)) {
					delete conn;
				}
				transfers.fetch_add(1, std::memory_order_relaxed);
			}
		});
	}
	std::thread sweeper([&]() {
		while (!stop.load(std::memory_order_acquire)) {
			SQLite3_result* free_rows = PgHGM->SQL3_Free_Connections();
			delete free_rows;
			(void)PgHGM->Get_Memory_Stats();
			PgSQL_Connection* ping_connections[4] = {};
			const int ping_count = PgHGM->get_multiple_idle_connections(
				reader_hg, UINT64_MAX, ping_connections, 4);
			for (int i = 0; i < ping_count; i++) {
				PgSQL_Connection* conn = ping_connections[i];
				if (!conn->polardb_selected_server_snapshot) {
					missing_server_snapshot_connections.fetch_add(
						1, std::memory_order_relaxed);
				}
				PgHGM->push_MyConn_to_pool(conn);
			}
			ping_extractions.fetch_add(
				ping_count, std::memory_order_relaxed);
			sweep_count.fetch_add(1, std::memory_order_relaxed);
		}
	});

	for (unsigned int i = 0; i < 20; i++) {
		reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
		reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
		add_connections();
		std::this_thread::yield();
	}
	stop.store(true, std::memory_order_release);
	for (std::thread& thread : takers) {
		thread.join();
	}
	sweeper.join();
	reader->myhgc->attributes.configured = old_attributes_configured;
	reader->myhgc->attributes.free_connections_pct = old_free_connections_pct;

	ok(transfers.load(std::memory_order_relaxed) > 0 &&
			sweep_count.load(std::memory_order_relaxed) > 0,
		"PostgreSQL core boundary concurrency: transfers overlap pool sweeps and status changes");
	ok(ping_extractions.load(std::memory_order_relaxed) > 0 &&
			missing_server_snapshot_connections.load(std::memory_order_relaxed) == 0,
		"PostgreSQL core boundary concurrency: extracted ping connections retain their server snapshot");
	ok(reader->pool_used_count_value() == 0 &&
			reader->matching_connection_count(key) ==
				reader->pool_free_count_value(),
		"PostgreSQL core boundary concurrency: list, count, and exact index remain coherent");
	while (reader->pool_free_count_value() > 0) {
		delete reader->ConnectionsFree->remove(0);
	}
}

static void test_keyless_core_use_restores_exact_match() {
	const int writer_hg = 908;
	const int reader_hg = 909;
	stage_polardb_topology(PgHGM, "PostgreSQL core exact return",
		writer_hg, "polardb-core-return-writer", 22362,
		reader_hg, "polardb-core-return-reader", 22363);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-core-return-reader", 22363);
	ok(reader != nullptr,
		"PostgreSQL core exact return: selected server fixture is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, key));
	PgSQL_Connection* core_use = reader->ConnectionsFree->remove(0);
	reader->ConnectionsUsed->add(core_use);
	ok(reader->matching_connection_count(key) == 0 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core exact return: keyless core use temporarily removes the FREE index entry");
	PgHGM->push_MyConn_to_pool(core_use);
	ok(reader->matching_connection_count(key) == 1 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core exact return: one return path rebuilds the exact index from connection state");
	reader->remove_free_connection(conn);
	delete conn;
}

static void test_selected_server_survives_concurrent_purge() {
	const int writer_hg = 910;
	const int reader_hg = 911;
	stage_polardb_topology(PgHGM, "PolarDB selected-server lifetime",
		writer_hg, "polardb-lifetime-writer", 22372,
		reader_hg, "polardb-lifetime-reader", 22373);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-lifetime-reader", 22373);
	ok(reader != nullptr,
		"PolarDB selected-server lifetime: selected server fixture is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult selected = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(selected.acquired() &&
			selected.conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB selected-server lifetime: acquired connection retains its server snapshot");
	if (!selected.acquired()) {
		return;
	}

	std::atomic<bool> allow_return{false};
	std::atomic<bool> returned{false};
	std::thread returner([&]() {
		while (!allow_return.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
		PgHGM->push_MyConn_to_pool(selected.conn);
		returned.store(true, std::memory_order_release);
	});

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-lifetime-writer", 22372)) == 0,
		"PolarDB selected-server lifetime: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	(void)PgHGM->get_polardb_server_list_snapshot();
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-lifetime-writer", 22372)) == 0,
		"PolarDB selected-server lifetime: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	ok(selected.conn->parent == reader && reader->address &&
			strcmp(reader->address, "polardb-lifetime-reader") == 0,
		"PolarDB selected-server lifetime: retired server remains valid until return");
	allow_return.store(true, std::memory_order_release);
	returner.join();
	ok(returned.load(std::memory_order_acquire),
		"PolarDB selected-server lifetime: return completes after concurrent purge");
}

static void test_idle_ping_connection_survives_topology_purge() {
	const int writer_hg = 998;
	const int reader_hg = 999;
	stage_polardb_topology(PgHGM, "PolarDB idle-ping server lifetime",
		writer_hg, "polardb-ping-lifetime-writer", 22376,
		reader_hg, "polardb-ping-lifetime-reader", 22377);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-ping-lifetime-reader", 22377);
	ok(reader != nullptr,
		"PolarDB idle-ping lifetime: selected server fixture is available");
	if (!reader) {
		return;
	}
	reader->max_connections = 1000;
	const bool old_attributes_configured = reader->myhgc->attributes.configured;
	const int old_free_connections_pct =
		reader->myhgc->attributes.free_connections_pct;
	reader->myhgc->attributes.configured = true;
	reader->myhgc->attributes.free_connections_pct = 100;

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	conn->creation_time = monotonic_time();
	conn->last_time_used = 1;
	const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, key));
	PgSQL_Connection* extracted[1] = {};
	const int extracted_count = PgHGM->get_multiple_idle_connections(
		reader_hg, UINT64_MAX, extracted, 1);
	reader->myhgc->attributes.configured = old_attributes_configured;
	reader->myhgc->attributes.free_connections_pct = old_free_connections_pct;
	ok(extracted_count == 1 && extracted[0] == conn &&
			conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB idle-ping lifetime: extraction retains the current server snapshot");
	if (extracted_count != 1 || extracted[0] != conn) {
		reader->remove_free_connection(conn);
		delete conn;
		return;
	}

	PgSQL_PoolMatchKey used_key;
	ok(reader->used_connection_match_key(conn, &used_key) &&
			used_key == key,
		"PolarDB idle-ping lifetime: extraction preserves the exact match key");
	ok(reader->remove_used_connection(conn),
		"PolarDB idle-ping lifetime: external ping connection can leave core USED for purge test");

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-ping-lifetime-writer", 22376)) == 0,
		"PolarDB idle-ping lifetime: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	(void)PgHGM->get_polardb_server_list_snapshot();
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-ping-lifetime-writer", 22376)) == 0,
		"PolarDB idle-ping lifetime: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	ok(conn->parent == reader && reader->address &&
			strcmp(reader->address, "polardb-ping-lifetime-reader") == 0,
		"PolarDB idle-ping lifetime: retired server remains valid until ping connection destruction");
	delete conn;
}

static void test_worker_local_reader_selection_sequence() {
	std::atomic<uint64_t> selection_start{0};
	std::unique_ptr<PgSQL_Thread> first_worker(new PgSQL_Thread());
	std::unique_ptr<PgSQL_Thread> second_worker(new PgSQL_Thread());

	const uint64_t first = first_worker->next_polardb_reader_selection_sequence(
		910, 1, &selection_start);
	const uint64_t second = first_worker->next_polardb_reader_selection_sequence(
		910, 1, &selection_start);
	ok(first == 0 && second == 1 &&
			selection_start.load(std::memory_order_relaxed) == 1,
		"PolarDB local selection sequence: one worker advances without another shared write");

	const uint64_t other_worker =
		second_worker->next_polardb_reader_selection_sequence(
			910, 1, &selection_start);
	ok(other_worker == 1 &&
			selection_start.load(std::memory_order_relaxed) == 2,
		"PolarDB local selection sequence: another worker receives a different start");

	const uint64_t other_hostgroup =
		first_worker->next_polardb_reader_selection_sequence(
			911, 1, &selection_start);
	const uint64_t original_hostgroup =
		first_worker->next_polardb_reader_selection_sequence(
			910, 1, &selection_start);
	ok(other_hostgroup == 2 && original_hostgroup == 2,
		"PolarDB local selection sequence: each hostgroup advances independently");

	const uint64_t next_generation =
		first_worker->next_polardb_reader_selection_sequence(
			910, 2, &selection_start);
	ok(next_generation == 3,
		"PolarDB local selection sequence: topology refresh starts new worker state");
}

static void test_two_reader_degraded_uses_healthy_peer() {
	const int writer_hg = 912;
	const int reader_hg = 913;
	stage_polardb_topology_two_readers(PgHGM,
		"PolarDB v2 degraded two-reader selection",
		writer_hg, "polardb-v2-degraded-writer", 22382,
		reader_hg,
		"polardb-v2-degraded-reader-a", 22383,
		"polardb-v2-degraded-reader-b", 22384);
	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* entry = nullptr;
	if (snapshot) {
		auto found = snapshot->by_hostgroup.find(reader_hg);
		if (found != snapshot->by_hostgroup.end()) {
			entry = &found->second;
		}
	}
	const bool fixture_ok = entry && entry->servers.size() == 2 &&
		entry->selection_start;
	ok(fixture_ok,
		"PolarDB v2 degraded selection: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}
	PgSQL_SrvC* unavailable = entry->servers[0].srv;
	PgSQL_SrvC* healthy = entry->servers[1].srv;
	unavailable->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	PgSQL_Connection* conn = make_cached_reader_connection(healthy);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(healthy, conn);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult pooled = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled.acquired() && pooled.srv == healthy,
		"PolarDB v2 degraded selection: pooled-only read skips an unavailable first choice");
	if (pooled.conn) {
		PgHGM->push_MyConn_to_pool(pooled.conn);
	}
	PolarDB_ReaderResult ordinary = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait, /*only_pooled=*/false);
	ok(ordinary.acquired() && ordinary.srv == healthy,
		"PolarDB v2 degraded selection: ordinary read skips an unavailable first choice");
	if (ordinary.conn) {
		PgHGM->push_MyConn_to_pool(ordinary.conn);
	}
	unavailable->set_status(MYSQL_SERVER_STATUS_ONLINE);
	healthy->remove_free_connection(conn);
	delete conn;
}

static void test_two_reader_selection_ignores_inventory_and_alternates() {
	const int writer_hg = 902;
	const int reader_hg = 903;
	stage_polardb_topology_two_readers(PgHGM,
		"PolarDB v2 two-reader selection",
		writer_hg, "polardb-v2-select-writer", 22332,
		reader_hg,
		"polardb-v2-select-reader-a", 22333,
		"polardb-v2-select-reader-b", 22334);
	PgSQL_HGC* reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC* reader_a = find_pgsql_server(
		reader_hgc, "polardb-v2-select-reader-a", 22333);
	PgSQL_SrvC* reader_b = find_pgsql_server(
		reader_hgc, "polardb-v2-select-reader-b", 22334);
	ok(reader_hgc != nullptr && reader_a != nullptr && reader_b != nullptr,
		"PolarDB v2 selection: two-reader fixture is available");
	if (!reader_hgc || !reader_a || !reader_b) {
		return;
	}
	std::vector<PgSQL_Connection*> fixtures;
	for (unsigned int i = 0; i < 4; i++) {
		PgSQL_Connection* conn = make_cached_reader_connection(reader_a);
		conn->pgsql_conn = unit_connected_pgconn();
		unit_reader_pool_add_matching(reader_a, conn);
		fixtures.push_back(conn);
	}
	PgSQL_Connection* conn_b = make_cached_reader_connection(reader_b);
	conn_b->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader_b, conn_b);
	fixtures.push_back(conn_b);
	ok(reader_a->pool_free_count_value() == 4 &&
			reader_b->pool_free_count_value() == 1,
		"PolarDB v2 selection: fixture deliberately skews matching FREE connections 4:1");

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	unsigned int hits_a = 0;
	unsigned int hits_b = 0;
	PgSQL_SrvC* previous = nullptr;
	bool acquired_all = true;
	bool alternated = true;
	bool returned_all = true;
	for (unsigned int i = 0; i < 8; i++) {
		PolarDB_ReaderResult result = PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
		if (!result.acquired()) {
			acquired_all = false;
			break;
		}
		if (previous == result.srv) {
			alternated = false;
		}
		previous = result.srv;
		hits_a += result.srv == reader_a ? 1U : 0U;
		hits_b += result.srv == reader_b ? 1U : 0U;
		PgHGM->push_MyConn_to_pool(result.conn);
		returned_all = result.srv->pool_used_count_value() == 0 &&
			returned_all;
	}
	ok(acquired_all && returned_all,
		"PolarDB v2 selection: all matching connections are taken and returned");
	ok(alternated && hits_a == 4 && hits_b == 4,
		"PolarDB v2 selection: equal weights alternate 4/4 despite unequal FREE counts");

	reader_b->remove_free_connection(conn_b);
	fixtures.pop_back();
	delete conn_b;
	std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		snapshot = PgHGM->get_polardb_server_list_snapshot();
	auto snapshot_hg = snapshot->by_hostgroup.find(reader_hg);
	const bool reader_b_first = snapshot_hg != snapshot->by_hostgroup.end() &&
		!snapshot_hg->second.servers.empty() &&
		snapshot_hg->second.servers[0].srv == reader_b;
	if (!reader_b_first) {
		(void)worker->next_polardb_reader_selection_sequence(
			reader_hg, snapshot->generation,
			snapshot_hg->second.selection_start.get());
	}
	PolarDB_ReaderResult pooled_fallback =
		PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled_fallback.acquired() && pooled_fallback.srv == reader_a &&
			reader_b->pool_free_count_value() == 0,
		"PolarDB v2 selection: pooled-only miss uses an exact match on another eligible reader");
	if (pooled_fallback.conn) {
		PgHGM->push_MyConn_to_pool(pooled_fallback.conn);
	}

	for (PgSQL_Connection* conn : fixtures) {
		PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
		srv->remove_free_connection(conn);
		delete conn;
	}
}

static void test_two_reader_unequal_weights_keep_global_sequence() {
	const int writer_hg = 914;
	const int reader_hg = 915;
	ok(PgHGM->servers_add(make_pgsql_servers_result_two_readers(
			writer_hg, "polardb-weighted-writer", 22392,
			reader_hg,
			"polardb-weighted-reader-a", 22393,
			"polardb-weighted-reader-b", 22394,
			100, 1)) == 0,
		"PolarDB weighted selection: unequal reader weights are staged");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB weighted selection: unequal reader topology commits");

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* entry = nullptr;
	if (snapshot) {
		auto found = snapshot->by_hostgroup.find(reader_hg);
		if (found != snapshot->by_hostgroup.end()) {
			entry = &found->second;
		}
	}
	const bool fixture_ok = entry && entry->servers.size() == 2 &&
		entry->selection_start;
	ok(fixture_ok,
		"PolarDB weighted selection: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}
	const auto& servers = entry->servers;
	PgSQL_SrvC* high_weight = servers[0].weight == 100
		? servers[0].srv : servers[1].srv;
	PgSQL_SrvC* low_weight = servers[0].weight == 1
		? servers[0].srv : servers[1].srv;
	PgSQL_Connection* high_conn = make_cached_reader_connection(high_weight);
	PgSQL_Connection* low_conn = make_cached_reader_connection(low_weight);
	high_conn->pgsql_conn = unit_connected_pgconn();
	low_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(high_weight, high_conn);
	unit_reader_pool_add_matching(low_weight, low_conn);
	entry->selection_start->store(0, std::memory_order_relaxed);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	unsigned int high_hits = 0;
	unsigned int low_hits = 0;
	bool acquired_all = true;
	for (unsigned int i = 0; i < 101; i++) {
		PolarDB_ReaderResult result = PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
		if (!result.acquired()) {
			acquired_all = false;
			break;
		}
		high_hits += result.srv == high_weight ? 1U : 0U;
		low_hits += result.srv == low_weight ? 1U : 0U;
		PgHGM->push_MyConn_to_pool(result.conn);
	}
	ok(acquired_all && high_hits == 100 && low_hits == 1,
		"PolarDB weighted selection: 100:1 weights route exactly 100/1 over one cycle");
	ok(entry->selection_start->load(std::memory_order_relaxed) == 101,
		"PolarDB weighted selection: unequal weights retain the shared sequence");

	high_weight->remove_free_connection(high_conn);
	low_weight->remove_free_connection(low_conn);
	delete high_conn;
	delete low_conn;
}

static void test_multi_reader_selection_samples_two_servers() {
	const int writer_hg = 904;
	const int reader_hg = 905;
	SQLite3_result* servers = make_pgsql_servers_result_two_readers(
		writer_hg, "polardb-multi-select-writer", 22342,
		reader_hg,
		"polardb-multi-select-reader-a", 22343,
		"polardb-multi-select-reader-b", 22344);
	char reader_hg_buf[16];
	char reader_port_buf[16];
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", 22345);
	char* reader_row[] = {
		reader_hg_buf,
		(char*)"polardb-multi-select-reader-c",
		reader_port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb multi-reader unit reader"
	};
	servers->add_row(reader_row);
	ok(PgHGM->servers_add(servers) == 0,
		"PolarDB multi-reader selection: writer and three readers staged for commit");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB multi-reader selection: topology commit succeeds");

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* entry = nullptr;
	if (snapshot) {
		auto found = snapshot->by_hostgroup.find(reader_hg);
		if (found != snapshot->by_hostgroup.end()) {
			entry = &found->second;
		}
	}
	const bool fixture_ok = entry && entry->servers.size() == 3;
	ok(fixture_ok,
		"PolarDB multi-reader selection: three-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}

	std::vector<PgSQL_Connection*> fixtures;
	for (const auto& server_entry : entry->servers) {
		PgSQL_SrvC* reader = server_entry.srv;
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		conn->pgsql_conn = unit_connected_pgconn();
		unit_reader_pool_add_matching(reader, conn);
		fixtures.push_back(conn);
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	const unsigned long long considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	const unsigned long long selected_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_select];
	const unsigned long long decisions_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_active_load] +
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_random];
	bool acquired_all = true;
	for (unsigned int n = 0; n < 32; n++) {
		PolarDB_ReaderResult result = PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, no_wait, /*only_pooled=*/false);
		if (!result.acquired()) {
			acquired_all = false;
			break;
		}
		PgHGM->push_MyConn_to_pool(result.conn);
	}
	ok(acquired_all,
		"PolarDB multi-reader selection: sampled readers provide every connection");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered] ==
			considered_before + 64,
		"PolarDB multi-reader selection: each request examines two healthy readers");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_select] == selected_before + 32,
		"PolarDB multi-reader selection: each request compares two active loads");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_active_load] +
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_random] == decisions_before + 32,
		"PolarDB multi-reader selection: each sampled pair makes one decision");

	for (PgSQL_Connection* conn : fixtures) {
		PgSQL_SrvC* reader = static_cast<PgSQL_SrvC*>(conn->parent);
		reader->remove_free_connection(conn);
		delete conn;
	}
}

static void test_reader_pool_status_and_pooled_only_contract() {
	const int writer_hg = 972;
	const int reader_hg = 973;
	const uint64_t TARGET_LSN = 0xE100;

	stage_polardb_topology(PgHGM, "PolarDB reader pool status",
		writer_hg, "polardb-status-writer", 22432,
		reader_hg, "polardb-status-reader", 22433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-status-reader", 22433);
	ok(reader != nullptr,
		"PolarDB reader pool status: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PolarDB_Query_ReaderPlan plan;
	plan.primary_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	reader->polardb_current_lsn.store(TARGET_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);

	PolarDB_ReaderResult empty_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!empty_result.acquired(),
		"PolarDB reader pool status: pooled-only read does not create on empty pool");
	ok(reader->ConnectionsFree->conns_length() == 0 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: empty pooled-only read leaves inventory unchanged");

	PgSQL_Connection *cached = make_cached_reader_connection(reader);
	reader->ConnectionsFree->add(cached);
	reader->status = MYSQL_SERVER_STATUS_SHUNNED;

	PolarDB_ReaderResult offline_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!offline_result.acquired(),
		"PolarDB reader pool status: non-ONLINE reader is not selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: non-ONLINE reader keeps pooled backend idle");

	reader->status = MYSQL_SERVER_STATUS_ONLINE;
	PolarDB_ReaderResult online_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!online_result.acquired(),
		"PolarDB reader pool status: disconnected pooled backend is not selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: disconnected pooled backend remains idle");

	PolarDB_ReaderResult ordinary_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait,
			/*only_pooled=*/false);
	ok(ordinary_result.acquired() && ordinary_result.conn == cached,
		"PolarDB reader pool status: ordinary reader path can take disconnected backend");
	ok(reader->ConnectionsFree->conns_length() == 0 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB reader pool status: ordinary reader path moves backend to used list");

	if (ordinary_result.conn && ordinary_result.srv &&
			ordinary_result.srv->ConnectionsUsed) {
		ordinary_result.srv->ConnectionsUsed->remove(ordinary_result.conn);
	}
	if (online_result.conn && online_result.srv &&
			online_result.srv->ConnectionsUsed) {
		online_result.srv->ConnectionsUsed->remove(online_result.conn);
	}
	if (!online_result.conn && !ordinary_result.conn) {
		reader->ConnectionsFree->remove(cached);
	}
	PgSQL_Connection *conn_to_delete =
		ordinary_result.conn ? ordinary_result.conn :
			(online_result.conn ? online_result.conn : cached);
	delete conn_to_delete;
}

static void test_reader_pool_capacity_accounting_includes_reuse_pool() {
	const int writer_hg = 978;
	const int reader_hg = 979;

	stage_polardb_topology(PgHGM, "PolarDB reader pool capacity",
		writer_hg, "polardb-capacity-writer", 22632,
		reader_hg, "polardb-capacity-reader", 22633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-capacity-reader", 22633);
	ok(reader != nullptr,
		"PolarDB reader pool capacity: reader server container is available");
	if (!reader) {
		return;
	}

	const long saved_max_connections = reader->max_connections;
	reader->max_connections = 2;

	PgSQL_Connection *classic_free = make_cached_reader_connection(reader);
	reader->ConnectionsFree->add(classic_free);

	PgSQL_Connection *pool_free = make_cached_reader_connection(reader);
	unit_reader_pool_add_shared(reader, pool_free);

	const PolarDB_PoolConnStats stats = reader->polardb_pool_conn_stats();
	ok(stats.free == 2 && stats.used == 0 && stats.total() == 2,
		"PolarDB reader pool capacity: matching and ordinary connections share one core FREE list");
	ok(!reader->polardb_pool_can_open_socket(),
		"PolarDB reader pool capacity: max_connections blocks another socket");
	ok(reader->polardb_pool_can_run_active(),
		"PolarDB reader pool capacity: idle reuse-pool connection does not block active capacity");

	unit_reader_pool_clear_shared(reader, pool_free);
	delete pool_free;

	reader->ConnectionsFree->remove(classic_free);
	delete classic_free;
	reader->max_connections = saved_max_connections;
}

static void test_reader_pool_shared_inventory_rejects_unrelated_connection() {
	const int writer_hg = 988;
	const int reader_hg = 989;

	stage_polardb_topology(PgHGM, "PolarDB reader pool shared mismatch",
		writer_hg, "polardb-shared-mismatch-writer", 23632,
		reader_hg, "polardb-shared-mismatch-reader", 23633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-shared-mismatch-reader", 23633);
	ok(reader != nullptr,
		"PolarDB reader pool shared mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PolarDB_StartupClientContext other_startup_client;
	other_startup_client.identity = unit_other_proxy_identity();
	PgSQL_Connection *other = make_cached_reader_connection(reader);
	ok(other != nullptr,
		"PolarDB reader pool shared mismatch: connection fixture is available");
	if (!other) {
		return;
	}
	other->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	other->polardb_startup_client = other_startup_client;
	unit_reader_pool_add_shared(reader, other);

	const unsigned long long considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	const unsigned long long match_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_match_attempt];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool shared mismatch: unrelated shared entry is not acquired");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered] == considered_before + 1,
		"PolarDB reader pool mismatch: connection counts do not select the server");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_match_attempt] == match_before + 1,
		"PolarDB reader pool mismatch: one matching connection lookup is made");

	unit_reader_pool_clear_shared(reader, other);
	delete other;
}

static void test_reader_pool_shared_prefix_reaches_compatible_backend() {
	const int writer_hg = 912;
	const int reader_hg = 913;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool shared prefix",
		writer_hg, "polardb-prefix-writer", 24962,
		reader_hg, "polardb-prefix-reader", 24963);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-prefix-reader", 24963);
	ok(reader != nullptr,
		"PolarDB reader pool shared prefix: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	std::vector<PgSQL_Connection*> incompatible;
	for (int i = 0; i < 20; i++) {
		PgSQL_Connection *other = make_cached_reader_connection(reader);
		ok(other != nullptr,
			"PolarDB reader pool shared prefix: incompatible fixture is available");
		if (!other) {
			continue;
		}
		std::string username = "polardb_prefix_other_" + std::to_string(i);
		other->userinfo->set(
			const_cast<char*>(username.c_str()),
			(char*)"polardb_unit_pass",
			(char*)"polardb_unit_db",
			nullptr);
		unit_reader_pool_add_shared(reader, other);
		incompatible.push_back(other);
	}

	PgSQL_Connection *compatible = make_cached_reader_connection(reader);
	ok(compatible != nullptr,
		"PolarDB reader pool shared prefix: compatible fixture is available");
	if (compatible) {
		unit_reader_pool_add_shared(reader, compatible);
	}

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	const unsigned int remaining_shared =
		unit_reader_pool_shared_free_count(reader);
	ok(!result.acquired() && remaining_shared == incompatible.size(),
		"PolarDB reader pool shared prefix: compatible backend behind incompatible prefix is reached");

	// The unit fixture has no live PGconn, so the reached backend is rejected by
	// the normal connectedness check after it leaves shared inventory.
	if (remaining_shared > incompatible.size() && compatible &&
			unit_reader_pool_clear_shared(reader, compatible)) {
		delete compatible;
		compatible = nullptr;
	}
	for (PgSQL_Connection *other : incompatible) {
		if (unit_reader_pool_clear_shared(reader, other)) {
			delete other;
		}
	}
}

static void test_reader_pool_profile_mismatch_drops_backend() {
	const int writer_hg = 992;
	const int reader_hg = 993;

	stage_polardb_topology(PgHGM, "PolarDB reader pool profile mismatch",
		writer_hg, "polardb-profile-writer", 23832,
		reader_hg, "polardb-profile-reader", 23833);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-profile-reader", 23833);
	ok(reader != nullptr,
		"PolarDB reader pool profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB reader pool profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation();
	unit_reader_pool_add_shared(reader, stale);
	ok(stale->pgsql_conn != nullptr,
		"PolarDB reader pool profile mismatch: libpq fixture can be safely destroyed");

	const unsigned long long reject_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_profile];
	const unsigned long long drop_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool profile mismatch: stale backend is not acquired");
	ok(unit_reader_pool_shared_free_count(reader) == 1,
		"PolarDB reader pool profile mismatch: a missing match does not scan unrelated generations");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_profile] == reject_before,
		"PolarDB reader pool profile mismatch: unrelated exact key is not examined");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable] == drop_before,
		"PolarDB reader pool profile mismatch: unrelated exact key is not dropped on lookup");
	if (unit_reader_pool_clear_shared(reader, stale)) {
		delete stale;
	}
}

static void test_reader_pool_identity_mode_mismatch_drops_backend() {
	const int writer_hg = 946;
	const int reader_hg = 947;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool identity mode mismatch",
		writer_hg, "polardb-identity-mode-writer", 24932,
		reader_hg, "polardb-identity-mode-reader", 24933);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-identity-mode-reader", 24933);
	ok(reader != nullptr,
		"PolarDB reader pool identity mode mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB reader pool identity mode mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	unit_reader_pool_add_shared(reader, stale);

	const unsigned long long reject_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_identity];
	const unsigned long long drop_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool identity mode mismatch: stale identity-mode backend is not acquired");
	ok(unit_reader_pool_shared_free_count(reader) == 0,
		"PolarDB reader pool identity mode mismatch: stale identity-mode backend is removed");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_identity] == reject_before + 1,
		"PolarDB reader pool identity mode mismatch: identity reject is counted");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable] == drop_before + 1,
		"PolarDB reader pool identity mode mismatch: stale backend drop is counted");
}

static void test_reader_pool_client_identity_mode_not_released_to_shared() {
	const int writer_hg = 948;
	const int reader_hg = 949;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool client mode release",
		writer_hg, "polardb-client-mode-writer", 24942,
		reader_hg, "polardb-client-mode-reader", 24943);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-client-mode-reader", 24943);
	ok(reader != nullptr,
		"PolarDB reader pool client mode release: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	ok(conn != nullptr,
		"PolarDB reader pool client mode release: connection fixture is available");
	if (!conn) {
		return;
	}

	conn->polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB reader pool client mode release: core USED list owns the connection");

	const unsigned long long drop_before =
		PgHGM->status.polardb_reader_pool_drop_client_identity.load(
			std::memory_order_relaxed);
	PgSQL_Thread worker;
	worker.push_MyConn_local(conn);

	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0,
		"PolarDB reader pool client mode release: CLIENT-mode backend is closed, not shared");
	ok(PgHGM->status.polardb_reader_pool_drop_client_identity.load(
			std::memory_order_relaxed) == drop_before + 1,
		"PolarDB reader pool client mode release: client-identity drop counter increments");
}

#if POLARDB_PROFILE
static void test_reader_pool_shared_transfer_counters() {
	const int writer_hg = 948;
	const int reader_hg = 949;

	stage_polardb_topology(PgHGM, "PolarDB shared pool counters",
		writer_hg, "polardb-shared-counters-writer", 24942,
		reader_hg, "polardb-shared-counters-reader", 24943);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-shared-counters-reader", 24943);
	ok(reader != nullptr,
		"PolarDB shared pool counters: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Session sess;
	sess.thread = &worker;
	attach_test_frontend(sess);

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	const unsigned long long take_attempt_before =
		PgHGM->status.polardb_reader_pool_shared_take_attempt.load(
			std::memory_order_relaxed);
	const unsigned long long take_hit_before =
		PgHGM->status.polardb_reader_pool_shared_take_hit.load(
			std::memory_order_relaxed);
	const unsigned long long return_attempt_before =
		PgHGM->status.polardb_reader_pool_shared_return_attempt.load(
			std::memory_order_relaxed);
	const unsigned long long return_accepted_before =
		PgHGM->status.polardb_reader_pool_shared_return_accepted.load(
			std::memory_order_relaxed);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult acquired = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(acquired.acquired() && acquired.conn == conn,
		"PolarDB shared pool counters: selected reader connection is acquired");
	ok(PgHGM->status.polardb_reader_pool_shared_take_attempt.load(
			std::memory_order_relaxed) == take_attempt_before + 1 &&
		PgHGM->status.polardb_reader_pool_shared_take_hit.load(
			std::memory_order_relaxed) == take_hit_before + 1,
		"PolarDB shared pool counters: successful shared take is counted");

	ok(PgHGM->return_connection_with_match_key(acquired.conn),
		"PolarDB shared pool counters: acquired connection returns to shared pool");
	ok(PgHGM->status.polardb_reader_pool_shared_return_attempt.load(
			std::memory_order_relaxed) == return_attempt_before + 1 &&
		PgHGM->status.polardb_reader_pool_shared_return_accepted.load(
			std::memory_order_relaxed) == return_accepted_before + 1,
		"PolarDB shared pool counters: accepted shared return is counted");

	reader->remove_free_connection(conn);
	delete conn;
}
#endif // POLARDB_PROFILE

static void test_reader_pool_worker_local_reuse() {
	const int writer_hg = 946;
	const int reader_hg = 947;

	stage_polardb_topology(PgHGM, "PolarDB worker-local reuse",
		writer_hg, "polardb-worker-cache-writer", 24932,
		reader_hg, "polardb-worker-cache-reader", 24933);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader = find_pgsql_server(
		reader_hgc, "polardb-worker-cache-reader", 24933);
	ok(reader != nullptr,
		"PolarDB worker-local reuse: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);
	const int original_retention =
		pgsql_thread___polardb_reader_connection_retention;
	pgsql_thread___polardb_reader_connection_retention = 0;

#if POLARDB_PROFILE
	const unsigned long long local_take_hit_before =
		PgHGM->status.polardb_reader_pool_local_take_hit.load(
			std::memory_order_relaxed);
	const unsigned long long local_store_accepted_before =
		PgHGM->status.polardb_reader_pool_local_store_accepted.load(
			std::memory_order_relaxed);
	const unsigned long long local_return_before =
		PgHGM->status.polardb_reader_pool_local_return_to_shared.load(
			std::memory_order_relaxed);
#endif // POLARDB_PROFILE

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult first = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(first.acquired() && first.conn == conn,
		"PolarDB worker-local reuse: first read takes the selected reader connection");
	worker->push_MyConn_local(first.conn);
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local reuse: connection remains in the core USED list");
	ok(worker->get_MyConn_local(
			reader_hg, &sess, nullptr, 0, -1) == nullptr,
		"PolarDB worker-local reuse: normal local lookup does not take an exact-key reader connection");
	PolarDB_PoolKey different_key = conn->polardb_pool_key;
	different_key.auth_hash ^= 1;
	ok(worker->get_local_polardb_reader_connection(
			reader, conn->polardb_startup_profile_generation,
			different_key) == nullptr,
		"PolarDB worker-local reuse: a different exact key does not take the connection");

	PolarDB_ReaderResult second = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(second.acquired() && second.conn == conn,
		"PolarDB worker-local reuse: next read reuses the same selected-server connection");
	worker->push_MyConn_local(second.conn);
#if POLARDB_PROFILE
	ok(PgHGM->status.polardb_reader_pool_local_take_hit.load(
			std::memory_order_relaxed) == local_take_hit_before + 1,
		"PolarDB worker-local reuse: local reuse hit is counted");
	ok(PgHGM->status.polardb_reader_pool_local_store_accepted.load(
			std::memory_order_relaxed) == local_store_accepted_before + 2,
		"PolarDB worker-local reuse: both local stores are counted");
#endif // POLARDB_PROFILE

	std::atomic<bool> return_finished{false};
	PgHGM->wrlock();
	std::thread return_thread([&]() {
		worker->return_local_connections();
		return_finished.store(true, std::memory_order_release);
	});
	for (unsigned int attempt = 0;
			attempt < 500 &&
			!return_finished.load(std::memory_order_acquire);
			attempt++) {
		usleep(1000);
	}
	const bool returned_without_global_lock =
		return_finished.load(std::memory_order_acquire);
	PgHGM->wrunlock();
	return_thread.join();
	ok(returned_without_global_lock,
		"PolarDB worker-local reuse: exact return does not wait for the global hostgroup lock");
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker-local reuse: worker pass returns the connection to core FREE");
#if POLARDB_PROFILE
	ok(PgHGM->status.polardb_reader_pool_local_return_to_shared.load(
			std::memory_order_relaxed) == local_return_before + 1,
		"PolarDB worker-local reuse: return to shared pool is counted");
#endif // POLARDB_PROFILE

	pgsql_thread___polardb_reader_connection_retention = 1;
	PolarDB_ReaderResult retained = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(retained.acquired() && retained.conn == conn,
		"PolarDB worker-local retention: selected reader is acquired from shared storage");
	worker->push_MyConn_local(retained.conn);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local retention: a reader used in the pass remains local");
	PolarDB_ReaderResult reused = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(reused.acquired() && reused.conn == conn,
		"PolarDB worker-local retention: the next pass reuses the local reader");
	worker->push_MyConn_local(reused.conn);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local retention: reuse renews retention for one pass");
	pgsql_thread___polardb_reader_connection_retention = 0;
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker-local retention: disabling retention returns local readers immediately");
	pgsql_thread___polardb_reader_connection_retention = original_retention;
	reader->remove_free_connection(conn);
	delete conn;
}

static void test_reader_pool_worker_local_duplicates_last_one_pass() {
	const int writer_hg = 942;
	const int reader_hg = 943;

	stage_polardb_topology(PgHGM, "PolarDB worker-local duplicate sharing",
		writer_hg, "polardb-worker-share-writer", 24912,
		reader_hg, "polardb-worker-share-reader", 24913);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-worker-share-reader", 24913);
	ok(reader != nullptr,
		"PolarDB worker-local sharing: reader server is available");
	if (!reader) {
		return;
	}
	reader->max_connections = 2;
	const int original_retention =
		pgsql_thread___polardb_reader_connection_retention;
	pgsql_thread___polardb_reader_connection_retention = 1;

	PgSQL_Connection* first = make_cached_reader_connection(reader);
	PgSQL_Connection* duplicate = make_cached_reader_connection(reader);
	first->pgsql_conn = unit_connected_pgconn();
	duplicate->pgsql_conn = unit_connected_pgconn();
	ok(first->pgsql_conn != nullptr && duplicate->pgsql_conn != nullptr,
		"PolarDB worker-local sharing: connected backend fixtures are available");
	if (!first->pgsql_conn || !duplicate->pgsql_conn) {
		pgsql_thread___polardb_reader_connection_retention = original_retention;
		delete first;
		delete duplicate;
		return;
	}

	const PgSQL_PoolMatchKey first_match_key =
		unit_reader_pool_match_key(first);
	const PgSQL_PoolMatchKey duplicate_match_key =
		unit_reader_pool_match_key(duplicate);
	ok(first_match_key == duplicate_match_key,
		"PolarDB worker-local sharing: both connections have the same exact key");
	ok(reader->add_used_matching_connection(first, first_match_key) &&
			reader->add_used_matching_connection(duplicate, duplicate_match_key),
		"PolarDB worker-local sharing: the two-connection backend pool is in use");

	PgSQL_Thread first_worker;
	PgSQL_Thread second_worker;
	first_worker.push_MyConn_local(first);
	first_worker.push_MyConn_local(duplicate);
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local sharing: matching connections remain local until the worker pass ends");

	ok(second_worker.get_local_polardb_reader_connection(
			reader, duplicate->polardb_startup_profile_generation,
			duplicate->polardb_pool_key) == nullptr,
		"PolarDB worker-local sharing: another worker has no private copy");
	PgSQL_Connection* shared =
		reader->take_matching_connection(duplicate_match_key);
	ok(shared == nullptr,
		"PolarDB worker-local sharing: another worker waits until the current pass returns unused connections");
	PgSQL_Connection* local_first =
		first_worker.get_local_polardb_reader_connection(
			reader, first->polardb_startup_profile_generation,
			first->polardb_pool_key);
	PgSQL_Connection* local_duplicate =
		first_worker.get_local_polardb_reader_connection(
			reader, duplicate->polardb_startup_profile_generation,
			duplicate->polardb_pool_key);
	ok(local_first == first && local_duplicate == duplicate,
		"PolarDB worker-local sharing: the first worker can reuse both matching connections");

	first_worker.push_MyConn_local(local_first);
	first_worker.push_MyConn_local(local_duplicate);
	first_worker.return_local_connections();
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local sharing: readers used in the pass remain local");
	first_worker.return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 2,
		"PolarDB worker-local sharing: unused readers return after one pass");
	pgsql_thread___polardb_reader_connection_retention = original_retention;
	reader->remove_free_connection(first);
	reader->remove_free_connection(duplicate);
	delete first;
	delete duplicate;

	PgSQL_Connection* default_user = make_cached_reader_connection(reader);
	PgSQL_Connection* other_user = make_cached_reader_connection(reader);
	default_user->pgsql_conn = unit_connected_pgconn();
	other_user->pgsql_conn = unit_connected_pgconn();
	other_user->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	ok(default_user->pgsql_conn != nullptr && other_user->pgsql_conn != nullptr,
		"PolarDB worker-local sharing: separate-key backend fixtures are available");
	if (!default_user->pgsql_conn || !other_user->pgsql_conn) {
		delete default_user;
		delete other_user;
		return;
	}

	const PgSQL_PoolMatchKey default_user_match_key =
		unit_reader_pool_match_key(default_user);
	const PgSQL_PoolMatchKey other_user_match_key =
		unit_reader_pool_match_key(other_user);
	ok(!(default_user_match_key == other_user_match_key),
		"PolarDB worker-local sharing: different users have different exact keys");
	ok(reader->add_used_matching_connection(
			default_user, default_user_match_key) &&
			reader->add_used_matching_connection(
				other_user, other_user_match_key),
		"PolarDB worker-local sharing: separate-key connections are in use");

	first_worker.push_MyConn_local(default_user);
	first_worker.push_MyConn_local(other_user);
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local sharing: one worker keeps one connection for each exact key");
	PgSQL_Connection* local_default =
		first_worker.get_local_polardb_reader_connection(
			reader, default_user->polardb_startup_profile_generation,
			default_user->polardb_pool_key);
	PgSQL_Connection* local_other =
		first_worker.get_local_polardb_reader_connection(
			reader, other_user->polardb_startup_profile_generation,
			other_user->polardb_pool_key);
	ok(local_default == default_user && local_other == other_user,
		"PolarDB worker-local sharing: each exact key retrieves its own local connection");
	ok(PgHGM->return_connection_with_match_key(local_default) &&
			PgHGM->return_connection_with_match_key(local_other),
		"PolarDB worker-local sharing: separate-key connections return to the shared pool");
	reader->remove_free_connection(default_user);
	reader->remove_free_connection(other_user);
	delete default_user;
	delete other_user;
}

static void test_reader_pool_return_rebuilds_cleared_key() {
	const int writer_hg = 944;
	const int reader_hg = 945;

	stage_polardb_topology(PgHGM, "PolarDB cleared pool key return",
		writer_hg, "polardb-cleared-key-writer", 24922,
		reader_hg, "polardb-cleared-key-reader", 24923);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-cleared-key-reader", 24923);
	ok(reader != nullptr,
		"PolarDB cleared pool key return: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Session sess;
	sess.thread = &worker;
	attach_test_frontend(sess);

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult acquired = PgHGM->get_MyConn_polardb_reader(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(acquired.acquired() && acquired.conn == conn &&
			acquired.conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB cleared pool key return: selected connection retains the server list");
	if (!acquired.acquired()) {
		reader->remove_free_connection(conn);
		delete conn;
		return;
	}

	acquired.conn->polardb_pool_key = PolarDB_PoolKey{};
	ok(PgHGM->return_connection_with_match_key(acquired.conn) &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			acquired.conn->polardb_selected_server_snapshot == nullptr,
		"PolarDB cleared pool key return: return rebuilds the exact key after reset");

	reader->remove_free_connection(conn);
	delete conn;
}

static void test_reader_pool_destroy_used_connection_updates_accounting() {
	const int writer_hg = 934;
	const int reader_hg = 935;

	stage_polardb_topology(PgHGM, "PolarDB reader pool destroy used",
		writer_hg, "polardb-destroy-used-writer", 24632,
		reader_hg, "polardb-destroy-used-reader", 24633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-destroy-used-reader", 24633);
	ok(reader != nullptr,
		"PolarDB reader pool destroy used: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	ok(conn != nullptr,
		"PolarDB reader pool destroy used: connection fixture is available");
	if (!conn) {
		return;
	}

	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB reader pool destroy used: core USED list owns the connection");
	PgHGM->destroy_MyConn_from_pool(conn);

	ok(reader->pool_used_count_value() == 0,
		"PolarDB reader pool destroy used: core USED count is reduced");
}

static void test_reader_pool_rejected_return_defers_destruction() {
	const int writer_hg = 1102;
	const int reader_hg = 1103;

	stage_polardb_topology(PgHGM, "PolarDB deferred return destruction",
		writer_hg, "polardb-deferred-return-writer", 26042,
		reader_hg, "polardb-deferred-return-reader", 26043);

	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-deferred-return-reader", 26043);
	ok(reader != nullptr,
		"PolarDB deferred return destruction: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB deferred return destruction: core USED list owns the connection");
	conn->async_state_machine = ASYNC_QUERY_START;

	PgSQL_Connection *connection_to_delete = nullptr;
	const bool handled = PgHGM->return_connection_with_match_key(
		conn, nullptr, &connection_to_delete);
	ok(handled && connection_to_delete == conn,
		"PolarDB deferred return destruction: rejected connection is returned to the caller");
	ok(connection_to_delete &&
			connection_to_delete->polardb_selected_server_snapshot != nullptr,
		"PolarDB deferred return destruction: server ownership remains until deletion");
	ok(reader->pool_used_count_value() == 0,
		"PolarDB deferred return destruction: rejected connection leaves core USED accounting");
	delete connection_to_delete;
}

static void test_reader_pool_batch_rejected_return_defers_destruction() {
	const int writer_hg = 1104;
	const int reader_hg = 1105;

	stage_polardb_topology(PgHGM, "PolarDB batch deferred return destruction",
		writer_hg, "polardb-batch-deferred-return-writer", 26052,
		reader_hg, "polardb-batch-deferred-return-reader", 26053);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-batch-deferred-return-reader", 26053);
	ok(reader != nullptr,
		"PolarDB batch deferred return destruction: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB batch deferred return destruction: core USED list owns the connection");
	conn->async_state_machine = ASYNC_QUERY_START;

	std::vector<PgSQL_Connection*> connections{conn};
	std::vector<PgSQL_Connection*> connections_to_delete;
	PgHGM->return_polardb_reader_connections(
		connections, connections_to_delete);
	ok(connections_to_delete.size() == 1 &&
			connections_to_delete.front() == conn,
		"PolarDB batch deferred return destruction: rejected connection is returned to the caller");
	ok(conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB batch deferred return destruction: server ownership remains until deletion");
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0,
		"PolarDB batch deferred return destruction: rejected connection leaves core accounting");
	delete conn;
}

static void test_classic_worker_local_cache_policy() {
	const int writer_hg = 1106;
	const int reader_hg = 1107;

	stage_polardb_topology(PgHGM, "PostgreSQL worker-local cache policy",
		writer_hg, "pgsql-worker-cache-writer", 26062,
		reader_hg, "pgsql-worker-cache-reader", 26063);

	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(writer_hg),
		"pgsql-worker-cache-writer", 26062);
	ok(writer != nullptr,
		"PostgreSQL worker-local cache policy: writer server container is available");
	if (!writer) {
		return;
	}

	const unsigned int original_thread_count = GloPTH->num_threads;
	GloPTH->num_threads = 2;

	PgSQL_Connection* local_first = make_cached_reader_connection(writer);
	PgSQL_Connection* local_second = make_cached_reader_connection(writer);
	local_first->polardb_pool_key = PolarDB_PoolKey{};
	local_second->polardb_pool_key = PolarDB_PoolKey{};
	writer->ConnectionsUsed->add(local_first);
	writer->ConnectionsUsed->add(local_second);

	pgsql_thread___bounded_local_connection_cache = 0;
	PgSQL_Thread unbounded_worker;
	unbounded_worker.push_MyConn_local(local_first);
	unbounded_worker.push_MyConn_local(local_second);
	ok(writer->pool_used_count_value() == 2 &&
			writer->pool_free_count_value() == 0,
		"PostgreSQL worker-local cache policy: value 0 keeps 3.0.7 local reuse");
	unbounded_worker.return_local_connections();
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 2,
		"PostgreSQL worker-local cache policy: 3.0.7 local entries return after the worker pass");
	writer->remove_free_connection(local_first);
	writer->remove_free_connection(local_second);
	delete local_first;
	delete local_second;

	PgSQL_Connection* bounded_first = make_cached_reader_connection(writer);
	PgSQL_Connection* bounded_second = make_cached_reader_connection(writer);
	bounded_first->polardb_pool_key = PolarDB_PoolKey{};
	bounded_second->polardb_pool_key = PolarDB_PoolKey{};
	writer->ConnectionsUsed->add(bounded_first);
	writer->ConnectionsUsed->add(bounded_second);

	pgsql_thread___bounded_local_connection_cache = 1;
	PgSQL_Thread bounded_worker;
	bounded_worker.push_MyConn_local(bounded_first);
	bounded_worker.push_MyConn_local(bounded_second);
	ok(writer->pool_used_count_value() == 1 &&
			writer->pool_free_count_value() == 1,
		"PostgreSQL worker-local cache policy: value 1 uses the 3.0.9 bounded behavior");
	bounded_worker.return_local_connections();
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 2,
		"PostgreSQL worker-local cache policy: bounded local entry returns after the worker pass");
	writer->remove_free_connection(bounded_first);
	writer->remove_free_connection(bounded_second);
	delete bounded_first;
	delete bounded_second;

	GloPTH->num_threads = original_thread_count;
	pgsql_thread___bounded_local_connection_cache = 0;
}

static void test_reader_pool_key_invalidates_on_variable_change() {
	PgSQL_Session sess;
	attach_test_frontend(sess);
	PgSQL_Connection *client_conn = sess.client_myds->myconn;

	client_conn->polardb_pool_key.auth_hash = 11;
	client_conn->polardb_pool_key.startup_identity_hash = 22;
	client_conn->polardb_pool_key.startup_options_hash = 33;
	ok(!client_conn->polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: fixture starts with cached key");

	const bool changed = pgsql_variables.client_set_value(
		&sess, PGSQL_DATESTYLE, "ISO, MDY", false);
	ok(changed,
		"PolarDB reader pool key invalidation: client variable update succeeds");
	ok(client_conn->polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: client variable update clears cached key");

	PgSQL_Connection backend_conn(false);
	set_test_pgsql_defaults(&backend_conn);
	backend_conn.copy_pgsql_variables_to_startup_parameters(
		/*copy_only_critical_param=*/true);
	backend_conn.polardb_pool_key.auth_hash = 44;
	backend_conn.polardb_pool_key.startup_options_hash = 55;
	backend_conn.reset();
	ok(backend_conn.polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: backend reset clears cached key");
}

static void test_classic_free_profile_mismatch_drops_backend() {
	const int writer_hg = 994;
	const int reader_hg = 995;

	stage_polardb_topology(PgHGM, "PolarDB classic free profile mismatch",
		writer_hg, "polardb-classic-profile-writer", 23842,
		reader_hg, "polardb-classic-profile-reader", 23843);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-classic-profile-reader", 23843);
	ok(reader != nullptr,
		"PolarDB classic free profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB classic free profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation();
	stale->pgsql_conn = PQconnectStart("polardb_unit_invalid_conninfo=1");
	ok(stale->pgsql_conn != nullptr,
		"PolarDB classic free profile mismatch: libpq fixture can be safely destroyed");
	reader->ConnectionsFree->add(stale);

	const unsigned long long evicted_before =
		PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed);
	const unsigned long long skipped_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB classic free profile mismatch: stale backend is not acquired");
	ok(reader->ConnectionsFree->conns_length() == 1,
		"PolarDB classic free profile mismatch: exact-only lookup does not scan classic FREE entries");
	ok(PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed) ==
			evicted_before,
		"PolarDB classic free profile mismatch: exact-only lookup performs no eviction");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped] == skipped_before,
		"PolarDB classic free profile mismatch: exact-only lookup performs no profile scan");
	reader->ConnectionsFree->remove(stale);
	stale->pgsql_conn = nullptr;
	delete stale;
}

static void test_v2_profile_mismatch_does_not_scan_unrelated_key() {
	const int writer_hg = 996;
	const int reader_hg = 997;
	const uint64_t TARGET_LSN = 0xFEED00;

	stage_polardb_topology(PgHGM, "PolarDB local reader profile mismatch",
		writer_hg, "polardb-local-profile-writer", 23852,
		reader_hg, "polardb-local-profile-reader", 23853);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-local-profile-reader", 23853);
	ok(reader != nullptr,
		"PolarDB local reader profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB local reader profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation();
	reader->polardb_current_lsn.store(TARGET_LSN + 0x100, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	unit_reader_pool_add_shared(reader, stale);

	const unsigned long long evicted_before =
		PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed);
	const unsigned long long skipped_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::STRICT);
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB v2 profile mismatch: unrelated startup-profile key is not acquired");
	ok(PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed) ==
			evicted_before,
		"PolarDB v2 profile mismatch: exact lookup does not evict an unrelated key");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped] == skipped_before,
		"PolarDB v2 profile mismatch: exact lookup does not scan an unrelated key");
	reader->remove_free_connection(stale);
	delete stale;
}

static void test_split_warmup_counts_shared_reader_pool_inventory() {
	const int writer_hg = 990;
	const int reader_hg = 991;

	stage_polardb_topology(PgHGM, "PolarDB warmup shared inventory",
		writer_hg, "polardb-warmup-shared-writer", 23732,
		reader_hg, "polardb-warmup-shared-reader", 23733);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-warmup-shared-reader", 23733);
	ok(reader != nullptr,
		"PolarDB warmup shared inventory: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_SplitWarmupRequest req =
		make_unit_warmup_request(reader_hg, 1);
	PgSQL_Connection *shared = make_cached_reader_connection(reader);
	ok(shared != nullptr,
		"PolarDB warmup shared inventory: shared connection fixture is available");
	if (!shared) {
		return;
	}
	unit_warmup_request_use_connection_state(req, shared);
	unit_reader_pool_add_shared(reader, shared);

	std::vector<PgSQL_SplitWarmupRequest> target_requests;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_PolarDB_ReaderPool pool(PgHGM);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, req, target_requests,
		&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();
	ok(found_hostgroup && saw_eligible_target,
		"PolarDB warmup shared inventory: reader hostgroup is eligible");
	ok(saw_compatible_free,
		"PolarDB warmup shared inventory: shared reader-pool backend is counted");
	ok(target_requests.empty(),
		"PolarDB warmup shared inventory: compatible shared backend suppresses new target creation");

	unit_reader_pool_clear_shared(reader, shared);
	delete shared;
}

static void test_split_warmup_ignores_incompatible_shared_inventory() {
	const int writer_hg = 914;
	const int reader_hg = 915;

	stage_polardb_topology(PgHGM, "PolarDB warmup shared mismatch",
		writer_hg, "polardb-warmup-mismatch-writer", 24972,
		reader_hg, "polardb-warmup-mismatch-reader", 24973);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-warmup-mismatch-reader", 24973);
	ok(reader != nullptr,
		"PolarDB warmup shared mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_SplitWarmupRequest req =
		make_unit_warmup_request(reader_hg, 1);
	PgSQL_Connection *other = make_cached_reader_connection(reader);
	ok(other != nullptr,
		"PolarDB warmup shared mismatch: shared connection fixture is available");
	if (!other) {
		return;
	}
	unit_warmup_request_use_connection_state(req, other);
	other->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	unit_reader_pool_add_shared(reader, other);

	std::vector<PgSQL_SplitWarmupRequest> target_requests;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_PolarDB_ReaderPool pool(PgHGM);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, req, target_requests,
		&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();
	ok(found_hostgroup && saw_eligible_target,
		"PolarDB warmup shared mismatch: reader hostgroup is eligible");
	ok(!saw_compatible_free,
		"PolarDB warmup shared mismatch: incompatible shared backend is not counted");
	ok(!target_requests.empty(),
		"PolarDB warmup shared mismatch: incompatible shared backend does not suppress target creation");

	unit_reader_pool_clear_shared(reader, other);
	delete other;
}

static void test_no_wait_pooled_reader_requires_startup_identity() {
	const int writer_hg = 976;
	const int reader_hg = 977;

	stage_polardb_topology(PgHGM, "PolarDB no-wait pooled reader",
		writer_hg, "polardb-nowait-writer", 23432,
		reader_hg, "polardb-nowait-reader", 23433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-nowait-reader", 23433);
	ok(reader != nullptr,
		"PolarDB no-wait pooled reader: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *mismatched = make_cached_reader_connection(reader);
	PgSQL_Connection *compatible = make_cached_reader_connection(reader);
	ok(mismatched != nullptr && compatible != nullptr,
		"PolarDB no-wait pooled reader: cached connection fixtures are available");
	if (!mismatched || !compatible) {
		delete mismatched;
		delete compatible;
		return;
	}
	mismatched->pgsql_conn = unit_connected_pgconn();
	compatible->pgsql_conn = unit_connected_pgconn();
	ok(mismatched->pgsql_conn != nullptr && compatible->pgsql_conn != nullptr,
		"PolarDB no-wait pooled reader: libpq fixtures are available");
	mismatched->polardb_startup_client.identity =
		unit_other_proxy_identity();
	compatible->polardb_startup_client.identity =
		unit_proxy_identity();
	unit_reader_pool_add_matching(reader, mismatched);
	unit_reader_pool_add_matching(reader, compatible);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(result.acquired() && result.conn == compatible,
		"PolarDB no-wait pooled reader: compatible identity is selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB no-wait pooled reader: incompatible backend stays idle");

	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	if (result.conn != mismatched) {
		reader->ConnectionsFree->remove(mismatched);
	}
	if (result.conn != compatible) {
		reader->ConnectionsFree->remove(compatible);
	}
	delete mismatched;
	delete compatible;
}

static void test_no_wait_pooled_reader_requires_exact_session_state() {
	const int writer_hg = 980;
	const int reader_hg = 981;

	stage_polardb_topology(PgHGM, "PolarDB no-wait pooled exact state",
		writer_hg, "polardb-nowait-exact-writer", 23532,
		reader_hg, "polardb-nowait-exact-reader", 23533);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-nowait-exact-reader", 23533);
	ok(reader != nullptr,
		"PolarDB no-wait pooled exact state: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PgSQL_Connection *needs_reset = make_cached_reader_connection(reader);
	PgSQL_Connection *exact = make_cached_reader_connection(reader);
	ok(needs_reset != nullptr && exact != nullptr,
		"PolarDB no-wait pooled exact state: cached connection fixtures are available");
	if (!needs_reset || !exact) {
		delete needs_reset;
		delete exact;
		return;
	}

	needs_reset->pgsql_conn = unit_connected_pgconn();
	exact->pgsql_conn = unit_connected_pgconn();
	ok(needs_reset->pgsql_conn != nullptr && exact->pgsql_conn != nullptr,
		"PolarDB no-wait pooled exact state: libpq fixtures are available");
	const int extra_var_idx = PGSQL_NAME_LAST_LOW_WM + 1;
	needs_reset->var_hash[extra_var_idx] = 0x9a51;
	needs_reset->dynamic_variables_idx.push_back(extra_var_idx);
	unit_reader_pool_add_matching(reader, needs_reset);
	unit_reader_pool_add_matching(reader, exact);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(result.acquired() && result.conn == exact,
		"PolarDB no-wait pooled exact state: reset-needed backend is skipped");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB no-wait pooled exact state: reset-needed backend stays idle");

	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	if (result.conn != needs_reset) {
		reader->ConnectionsFree->remove(needs_reset);
	}
	if (result.conn != exact) {
		reader->ConnectionsFree->remove(exact);
	}
	delete needs_reset;
	delete exact;
}

static void test_successful_wait_cache_advance_requires_active_wait() {
	const int writer_hg = 950;
	const int reader_hg = 951;
	const uint64_t TARGET_LSN = 0xB100;

	stage_polardb_topology(PgHGM, "PolarDB wait cache advance",
		writer_hg, "polardb-wait-writer", 19432,
		reader_hg, "polardb-wait-reader", 19433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-wait-reader", 19433);
	const auto* reader_cfg = PgHGM->find_polardb_hg_config(reader_hg);
	ok(reader != nullptr,
		"PolarDB wait cache advance: reader server container is available");
	ok(reader_cfg != nullptr && reader_cfg->writer_epoch != nullptr,
		"PolarDB wait cache advance: reader has writer epoch config");
	if (!reader || !reader_cfg || !reader_cfg->writer_epoch) {
		return;
	}

	PgSQL_Session sess;
	sess.connections_handler = true;
	sess.polardb_config.is_polardb_enabled = true;
	sess.polardb_query.request_writer_scope = PolarDB_WriterScope{
		reader_cfg->writer_hostgroup,
		reader_cfg->writer_epoch->load(std::memory_order_relaxed)};
	sess.polardb_query.wait.wrapper_finalized = true;
	sess.polardb_query.wait.spec.type = PolarDB_WaitType::LSN;
	sess.polardb_query.wait.spec.target = TARGET_LSN;

	PgSQL_Data_Stream backend_myds;
	PgSQL_Connection* backend_conn = new PgSQL_Connection(false);
	backend_conn->parent = reader;
	backend_myds.myconn = backend_conn;

	sess.polardb_query.wait.wait_started_at_us = 0;
	sess.polardb_note_successful_wait_target(&backend_myds, false);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: timed-out/accounted wait does not advance reader LSN cache");

	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_note_successful_wait_target(&backend_myds, false);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: installed but unconsumed wrapper is not success confirmation");

	backend_conn->polardb_query_wrap_state.begin(
		1, PolarDB_Query_WrapperKind::CONSISTENCY_WAIT);
	backend_conn->polardb_query_wrap_state.consume_successful_wrapper_set();
	sess.polardb_note_successful_wait_target(&backend_myds, false);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == TARGET_LSN,
		"PolarDB wait cache advance: consumed successful wait advances reader LSN cache");

	reader->polardb_current_lsn.store(0, std::memory_order_relaxed);
	reader->lsn_updated_at.store(0, std::memory_order_relaxed);
	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_note_successful_wait_target(&backend_myds, true);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: failure cleanup does not advance reader LSN cache");

	backend_myds.myconn = nullptr;
	delete backend_conn;
}

static void test_reader_lsn_lag_range_predicate() {
	ok(polardb_reader_lsn_in_best_behind_range(0xC000, 0xC000, 0),
		"PolarDB best-behind range: exact mode includes the best reader LSN");
	ok(!polardb_reader_lsn_in_best_behind_range(0xBFF0, 0xC000, 0),
		"PolarDB best-behind range: exact mode excludes lower reader LSNs");
	ok(polardb_reader_lsn_in_best_behind_range(0xBFF0, 0xC000, 0x10),
		"PolarDB best-behind range: byte range includes close lower reader LSNs");
	ok(!polardb_reader_lsn_in_best_behind_range(0xBFE0, 0xC000, 0x10),
		"PolarDB best-behind range: byte range excludes distant lower reader LSNs");
	ok(!polardb_reader_lsn_in_best_behind_range(0, 0xC000, 0x10),
		"PolarDB best-behind range: missing reader LSN is not eligible");
}

static void test_txn_reader_state_clear_contract() {
	PgSQL_Session sess;
	PgSQL_Backend fake_backend;
	PgSQL_Backend fake_primary;

	sess.polardb_txn_reader.backend = &fake_backend;
	sess.polardb_txn_reader.primary_backend = &fake_primary;
	sess.polardb_txn_reader.split_active = true;
	sess.polardb_txn_reader.wait_read_active = true;
	sess.polardb_txn_reader.wait_spec =
		PolarDB_WaitSpec::lsn(100, 5000, PolarDB_WaitMode::STRICT);
	sess.polardb_txn_reader.writer_scope = PolarDB_WriterScope{10, 7};
	sess.polardb_txn_reader.read_start_us = 11;
	sess.polardb_txn_reader.wait_start_us = 12;

	sess.polardb_txn_reader.clear_request_state();
	ok(sess.polardb_txn_reader.backend == &fake_backend,
		"PolarDB txn reader state: clear preserves reusable reader backend");
	ok(sess.polardb_txn_reader.primary_backend == nullptr,
		"PolarDB txn reader state: clear drops saved primary backend");
	ok(!sess.polardb_txn_reader.active(),
		"PolarDB txn reader state: clear drops active markers");
	ok(!sess.polardb_txn_reader.wait_spec.has_wait(),
		"PolarDB txn reader state: clear resets wait spec");
	ok(!sess.polardb_txn_reader.writer_scope.valid(),
		"PolarDB txn reader state: clear resets writer scope snapshot");
	ok(sess.polardb_txn_reader.read_start_us == 0 &&
			sess.polardb_txn_reader.wait_start_us == 0,
		"PolarDB txn reader state: clear resets timing fields");
}

static void test_tied_freshest_behind_reader_can_acquire_second_candidate() {
	const int writer_hg = 960;
	const int reader_hg = 961;
	const uint64_t BEHIND_LSN = 0xC000;
	const uint64_t TARGET_LSN = 0xC100;

	stage_polardb_topology_two_readers(PgHGM, "PolarDB best-behind reader",
		writer_hg, "polardb-bestbehind-writer", 20432,
		reader_hg,
		"polardb-bestbehind-reader-a", 20433,
		"polardb-bestbehind-reader-b", 20434);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader_a =
		find_pgsql_server(reader_hgc, "polardb-bestbehind-reader-a", 20433);
	PgSQL_SrvC *reader_b =
		find_pgsql_server(reader_hgc, "polardb-bestbehind-reader-b", 20434);
	ok(reader_a != nullptr && reader_b != nullptr,
		"PolarDB best-behind reader: both reader server containers are available");
	if (!reader_a || !reader_b) {
		return;
	}

	const uint64_t now = monotonic_time();
	reader_a->polardb_current_lsn.store(BEHIND_LSN, std::memory_order_relaxed);
	reader_b->polardb_current_lsn.store(BEHIND_LSN, std::memory_order_relaxed);
	reader_a->lsn_updated_at.store(now, std::memory_order_relaxed);
	reader_b->lsn_updated_at.store(now, std::memory_order_relaxed);

	PgSQL_Connection *cached = make_cached_reader_connection(reader_b);
	cached->polardb_startup_client.identity =
		unit_proxy_identity();
	cached->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader_b, cached);
	ok(reader_b->ConnectionsFree->conns_length() == 1,
		"PolarDB best-behind reader: second tied reader has one pooled backend");

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PolarDB_Query_ReaderPlan plan;
	plan.primary_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const int saved_throttle = pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	PolarDB_ReaderResult first_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait, false);
	ok(!first_result.acquired() && first_result.srv == reader_a,
		"PolarDB best-behind reader: a miss stays on the selected reader");
	ok(reader_b->pool_free_count_value() == 1,
		"PolarDB best-behind reader: a miss does not reroute through another server's pool");
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait, false);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
	ok(result.acquired() && result.srv == reader_b,
		"PolarDB best-behind reader: tied freshest-behind set can acquire the second candidate");
	ok(!result.wait_bypass_allowed,
		"PolarDB best-behind reader: behind-target acquisition still requires wait wrapper");
	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	delete result.conn;
}

static void test_lag_range_best_behind_reader_can_acquire_lower_lsn_candidate() {
	const int writer_hg = 962;
	const int reader_hg = 963;
	const uint64_t BEST_LSN = 0xD000;
	const uint64_t LOWER_LSN_IN_RANGE = 0xCFF0;
	const uint64_t TARGET_LSN = 0xD100;

	stage_polardb_topology_two_readers(PgHGM, "PolarDB best-behind range",
		writer_hg, "polardb-range-writer", 21432,
		reader_hg,
		"polardb-range-reader-a", 21433,
		"polardb-range-reader-b", 21434);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader_a =
		find_pgsql_server(reader_hgc, "polardb-range-reader-a", 21433);
	PgSQL_SrvC *reader_b =
		find_pgsql_server(reader_hgc, "polardb-range-reader-b", 21434);
	ok(reader_a != nullptr && reader_b != nullptr,
		"PolarDB best-behind range: both reader server containers are available");
	if (!reader_a || !reader_b) {
		return;
	}

	const uint64_t now = monotonic_time();
	reader_a->polardb_current_lsn.store(BEST_LSN, std::memory_order_relaxed);
	reader_b->polardb_current_lsn.store(LOWER_LSN_IN_RANGE, std::memory_order_relaxed);
	reader_a->lsn_updated_at.store(now, std::memory_order_relaxed);
	reader_b->lsn_updated_at.store(now, std::memory_order_relaxed);

	PgSQL_Connection *cached = make_cached_reader_connection(reader_b);
	cached->polardb_startup_client.identity =
		unit_proxy_identity();
	cached->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader_b, cached);
	ok(reader_b->ConnectionsFree->conns_length() == 1,
		"PolarDB best-behind range: lower-LSN reader has one pooled backend");

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);

	PolarDB_Query_ReaderPlan plan;
	plan.primary_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const int saved_range = pgsql_thread___polardb_reader_lsn_lag_range_bytes;
	const int saved_throttle = pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = 0x20;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	PolarDB_ReaderResult first_result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait, false);
	ok(!first_result.acquired() && first_result.srv == reader_a,
		"PolarDB best-behind range: a miss stays on the selected reader");
	ok(reader_b->pool_free_count_value() == 1,
		"PolarDB best-behind range: a miss does not reroute through another server's pool");
	PolarDB_ReaderResult result =
		PgHGM->get_MyConn_polardb_reader(reader_hg, &sess, plan, wait, false);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = saved_range;
	ok(result.acquired() && result.srv == reader_b,
		"PolarDB best-behind range: byte range can acquire a lower-LSN candidate");
	ok(!result.wait_bypass_allowed,
		"PolarDB best-behind range: lower-LSN acquisition still requires wait wrapper");
	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	delete result.conn;
}

static void test_split_warmup_request_dedup() {
	const int reader_hg = 971;

	PolarDB_StartupClientContext startup_client;
	startup_client.identity = unit_proxy_identity();
	const bool saved_lazy_warmup = pgsql_thread___polardb_lazy_warmup_split;
	const bool saved_runtime_lazy_warmup =
		GloPTH ? GloPTH->variables.polardb_lazy_warmup_split : false;
	auto set_lazy_warmup = [](bool value) {
		pgsql_thread___polardb_lazy_warmup_split = value;
		if (GloPTH) {
			GloPTH->variables.polardb_lazy_warmup_split = value;
		}
	};

	// Use the public drain path to start from an empty queue. This keeps the
	// product class free of unit-only queue inspection helpers.
	set_lazy_warmup(false);
	PgHGM->warm_split_pools();

	set_lazy_warmup(true);
	pgsql_thread___polardb_lazy_warmup_split = false;

	const unsigned long long requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed);
	PgHGM->request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	PgHGM->request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	ok(PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed) ==
			requested_before + 1,
		"PolarDB warmup request: runtime setting owns request acceptance");
	ok(PgHGM->status.polardb_warmup_pending.load(std::memory_order_relaxed) == 1,
		"PolarDB warmup request: pending gauge stores the deduplicated queue depth");

	set_lazy_warmup(false);
	PgHGM->warm_split_pools();
	ok(PgHGM->status.polardb_warmup_pending.load(std::memory_order_relaxed) == 0,
		"PolarDB warmup request: disabled lazy warmup drains queued work");

	set_lazy_warmup(true);
	const unsigned long long hg0_requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed);
	const unsigned long long hg0_failed_before =
		PgHGM->status.polardb_split_warmup_failed.load(std::memory_order_relaxed);
	const unsigned long long hg0_no_target_before =
		PgHGM->status.polardb_split_warmup_no_target.load(std::memory_order_relaxed);
	PgHGM->request_split_warmup(
		0, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	ok(PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed) ==
			hg0_requested_before + 1,
		"PolarDB warmup request: reader hostgroup 0 is a valid request key");
	PgHGM->warm_split_pools();
	ok(PgHGM->status.polardb_split_warmup_no_target.load(std::memory_order_relaxed) ==
			hg0_no_target_before + 1,
		"PolarDB warmup request: missing reader hostgroup records no-target request");
	ok(PgHGM->status.polardb_split_warmup_failed.load(std::memory_order_relaxed) ==
			hg0_failed_before + 1,
		"PolarDB warmup request: missing reader hostgroup records request failure");

	set_lazy_warmup(false);
	PgHGM->warm_split_pools();

	pgsql_thread___polardb_lazy_warmup_split = saved_lazy_warmup;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = saved_runtime_lazy_warmup;
	}
}

static void test_split_warmup_runtime_variable_refresh() {
	if (!GloPTH) {
		ok(1, "PolarDB warmup variables: skipped without PgSQL thread handler");
		return;
	}

	const int saved_runtime_throttle =
		GloPTH->variables.throttle_connections_per_sec_to_hostgroup;
	const int saved_runtime_latency =
		GloPTH->variables.default_max_latency_ms;
	const int saved_runtime_retries =
		GloPTH->variables.connect_retries_on_failure;
	const int saved_runtime_shun =
		GloPTH->variables.shun_on_failures;
	const int saved_runtime_recovery =
		GloPTH->variables.shun_recovery_time_sec;
	const int saved_runtime_timeout =
		GloPTH->variables.connect_timeout_server;
	const int saved_runtime_timeout_max =
		GloPTH->variables.connect_timeout_server_max;
	const int saved_runtime_max_connections =
		GloPTH->variables.polardb_split_warmup_max_connections_per_request;
	char* saved_runtime_proxy_protocol =
		GloPTH->variables.polardb_proxy_protocol ?
			strdup(GloPTH->variables.polardb_proxy_protocol) : NULL;
	char* saved_runtime_identity_mode =
		GloPTH->variables.polardb_proxy_identity_mode ?
			strdup(GloPTH->variables.polardb_proxy_identity_mode) : NULL;
	char* saved_runtime_identity_host =
		GloPTH->variables.polardb_proxy_identity_host ?
			strdup(GloPTH->variables.polardb_proxy_identity_host) : NULL;
	const int saved_runtime_identity_port =
		GloPTH->variables.polardb_proxy_identity_port;

	const int saved_tls_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	const int saved_tls_latency = pgsql_thread___default_max_latency_ms;
	const int saved_tls_retries = pgsql_thread___connect_retries_on_failure;
	const int saved_tls_shun = pgsql_thread___shun_on_failures;
	const int saved_tls_recovery = pgsql_thread___shun_recovery_time_sec;
	const int saved_tls_timeout = pgsql_thread___connect_timeout_server;
	const int saved_tls_timeout_max = pgsql_thread___connect_timeout_server_max;
	const int saved_tls_max_connections =
		pgsql_thread___polardb_split_warmup_max_connections_per_request;
	const int saved_tls_proxy_protocol =
		pgsql_thread___polardb_proxy_protocol;
	const int saved_tls_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
	char* saved_tls_identity_host =
		pgsql_thread___polardb_proxy_identity_host ?
			strdup(pgsql_thread___polardb_proxy_identity_host) : NULL;
	const int saved_tls_identity_port =
		pgsql_thread___polardb_proxy_identity_port;

	GloPTH->variables.throttle_connections_per_sec_to_hostgroup = 0;
	GloPTH->variables.default_max_latency_ms = 17;
	GloPTH->variables.connect_retries_on_failure = 2;
	GloPTH->variables.shun_on_failures = 3;
	GloPTH->variables.shun_recovery_time_sec = 4;
	GloPTH->variables.connect_timeout_server = 1234;
	GloPTH->variables.connect_timeout_server_max = 4321;
	GloPTH->variables.polardb_split_warmup_max_connections_per_request = 7;
	free(GloPTH->variables.polardb_proxy_protocol);
	GloPTH->variables.polardb_proxy_protocol = strdup((char*)"legacy");
	free(GloPTH->variables.polardb_proxy_identity_mode);
	GloPTH->variables.polardb_proxy_identity_mode = strdup((char*)"proxy");
	free(GloPTH->variables.polardb_proxy_identity_host);
	GloPTH->variables.polardb_proxy_identity_host = strdup((char*)"127.0.0.99");
	GloPTH->variables.polardb_proxy_identity_port = 6603;

	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 99;
	pgsql_thread___default_max_latency_ms = 99;
	pgsql_thread___connect_retries_on_failure = 99;
	pgsql_thread___shun_on_failures = 99;
	pgsql_thread___shun_recovery_time_sec = 99;
	pgsql_thread___connect_timeout_server = 99;
	pgsql_thread___connect_timeout_server_max = 99;
	pgsql_thread___polardb_split_warmup_max_connections_per_request = 99;
	pgsql_thread___polardb_proxy_protocol = POLARDB_PROXY_PROTOCOL_OFF;
	pgsql_thread___polardb_proxy_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host = strdup((char*)"192.0.2.10");
	pgsql_thread___polardb_proxy_identity_port = 1234;

	PgHGM->refresh_split_warmup_variables();
	ok(pgsql_thread___throttle_connections_per_sec_to_hostgroup == 0,
		"PolarDB warmup variables: throttle value refreshes exactly");
	ok(pgsql_thread___default_max_latency_ms == 17 &&
			pgsql_thread___connect_retries_on_failure == 2 &&
			pgsql_thread___shun_on_failures == 3 &&
			pgsql_thread___shun_recovery_time_sec == 4,
		"PolarDB warmup variables: latency and shun policy refresh");
	ok(pgsql_thread___connect_timeout_server == 1234 &&
			pgsql_thread___connect_timeout_server_max == 4321,
		"PolarDB warmup variables: connect timeout policy refresh");
	ok(pgsql_thread___polardb_split_warmup_max_connections_per_request == 7,
		"PolarDB warmup variables: max connections per request refresh");
	ok(pgsql_thread___polardb_proxy_protocol == POLARDB_PROXY_PROTOCOL_LEGACY,
		"PolarDB warmup variables: proxy protocol refresh");
	ok(pgsql_thread___polardb_proxy_identity_mode ==
				static_cast<int>(PolarDB_ProxyIdentityMode::PROXY) &&
			pgsql_thread___polardb_proxy_identity_host &&
			strcmp(pgsql_thread___polardb_proxy_identity_host,
				"127.0.0.99") == 0 &&
			pgsql_thread___polardb_proxy_identity_port == 6603,
		"PolarDB warmup variables: proxy identity policy refresh");

	GloPTH->variables.throttle_connections_per_sec_to_hostgroup =
		saved_runtime_throttle;
	GloPTH->variables.default_max_latency_ms = saved_runtime_latency;
	GloPTH->variables.connect_retries_on_failure = saved_runtime_retries;
	GloPTH->variables.shun_on_failures = saved_runtime_shun;
	GloPTH->variables.shun_recovery_time_sec = saved_runtime_recovery;
	GloPTH->variables.connect_timeout_server = saved_runtime_timeout;
	GloPTH->variables.connect_timeout_server_max = saved_runtime_timeout_max;
	GloPTH->variables.polardb_split_warmup_max_connections_per_request =
		saved_runtime_max_connections;
	free(GloPTH->variables.polardb_proxy_protocol);
	GloPTH->variables.polardb_proxy_protocol =
		saved_runtime_proxy_protocol ? saved_runtime_proxy_protocol : strdup((char*)"");
	free(GloPTH->variables.polardb_proxy_identity_mode);
	GloPTH->variables.polardb_proxy_identity_mode =
		saved_runtime_identity_mode ? saved_runtime_identity_mode : strdup((char*)"");
	free(GloPTH->variables.polardb_proxy_identity_host);
	GloPTH->variables.polardb_proxy_identity_host =
		saved_runtime_identity_host ? saved_runtime_identity_host : strdup((char*)"");
	GloPTH->variables.polardb_proxy_identity_port =
		saved_runtime_identity_port;

	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		saved_tls_throttle;
	pgsql_thread___default_max_latency_ms = saved_tls_latency;
	pgsql_thread___connect_retries_on_failure = saved_tls_retries;
	pgsql_thread___shun_on_failures = saved_tls_shun;
	pgsql_thread___shun_recovery_time_sec = saved_tls_recovery;
	pgsql_thread___connect_timeout_server = saved_tls_timeout;
	pgsql_thread___connect_timeout_server_max = saved_tls_timeout_max;
	pgsql_thread___polardb_split_warmup_max_connections_per_request =
		saved_tls_max_connections;
	pgsql_thread___polardb_proxy_protocol = saved_tls_proxy_protocol;
	pgsql_thread___polardb_proxy_identity_mode = saved_tls_identity_mode;
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host = saved_tls_identity_host;
	pgsql_thread___polardb_proxy_identity_port = saved_tls_identity_port;
}

static void test_split_warmup_failure_accounting_policy() {
	ok(pgsql_split_warmup_count_connect_failure(false, false),
		"PolarDB warmup accounting: real connect failure is counted");
	ok(!pgsql_split_warmup_count_connect_failure(false, true),
		"PolarDB warmup accounting: shutdown stop is not a backend failure");
	ok(!pgsql_split_warmup_count_connect_failure(true, false),
		"PolarDB warmup accounting: successful connect is not a failure");
}

static void test_split_warmup_max_connections_per_request_policy() {
	ok(pgsql_split_warmup_max_connections_per_request_from_int(0) ==
			PGSQL_POLARDB_SPLIT_WARMUP_DEFAULT_MAX_CONNECTIONS_PER_REQUEST,
		"PolarDB warmup max connections per request: non-positive values use default");
	ok(pgsql_split_warmup_max_connections_per_request_from_int(4) == 4,
		"PolarDB warmup max connections per request: configured value is accepted");
	ok(pgsql_split_warmup_max_connections_per_request_from_int(1000) ==
			PGSQL_POLARDB_SPLIT_WARMUP_MAX_CONNECTIONS_PER_REQUEST_LIMIT,
		"PolarDB warmup max connections per request: configured value is capped");
}

static SQLite3_result *make_pgsql_servers_result_many_readers(
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_prefix, int reader_count,
		int reader_base_port, int reader_weight = 1) {
	SQLite3_result *result = new SQLite3_result(11);
	std::string writer_hg_text = std::to_string(writer_hg);
	std::string writer_port_text = std::to_string(writer_port);

	char *writer_row[] = {
		const_cast<char*>(writer_hg_text.c_str()),
		const_cast<char*>(writer_addr),
		const_cast<char*>(writer_port_text.c_str()),
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb warmup fanout unit writer"
	};
	result->add_row(writer_row);

	const std::string reader_hg_text = std::to_string(reader_hg);
	const std::string reader_weight_text = std::to_string(reader_weight);
	for (int i = 0; i < reader_count; ++i) {
		std::string address = std::string(reader_prefix) + std::to_string(i + 1);
		std::string port = std::to_string(reader_base_port + i);
		char *reader_row[] = {
			const_cast<char*>(reader_hg_text.c_str()),
			const_cast<char*>(address.c_str()),
			const_cast<char*>(port.c_str()),
			(char*)"ONLINE",
			const_cast<char*>(reader_weight_text.c_str()),
			(char*)"0",
			(char*)"50",
			(char*)"0",
			(char*)"0",
			(char*)"1000",
			(char*)"polardb warmup fanout unit reader"
		};
		result->add_row(reader_row);
	}
	return result;
}

static void stage_polardb_topology_many_readers(
		PgSQL_HostGroups_Manager *hgm,
		const char *label,
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_prefix, int reader_count,
		int reader_base_port, int reader_weight = 1) {
	ok(hgm->servers_add(make_pgsql_servers_result_many_readers(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_prefix, reader_count, reader_base_port,
			reader_weight)) == 0,
		"%s: writer and readers staged for commit", label);
	hgm->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(hgm->commit({}, {}, false, false),
		"%s: topology commit succeeds", label);
}

static void test_split_warmup_thread_refreshes_idle_server_list() {
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh first topology",
		1110, "polardb-warmup-refresh-writer-a", 26500,
		1111, "polardb-warmup-refresh-reader-a", 26501);

	std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		old_server_list = PgHGM->get_polardb_server_list_snapshot();
	std::weak_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		old_server_list_reference = old_server_list;
	ok(old_server_list != nullptr,
		"PolarDB idle warmup refresh: first server list is available");

	PgHGM->init();
	usleep(250000);
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh second topology",
		1112, "polardb-warmup-refresh-writer-b", 26502,
		1113, "polardb-warmup-refresh-reader-b", 26503);
	old_server_list.reset();
	PgHGM->refresh_polardb_thread_snapshots();

	usleep(250000);
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh third topology",
		1114, "polardb-warmup-refresh-writer-c", 26504,
		1115, "polardb-warmup-refresh-reader-c", 26505);
	PgHGM->shutdown_split_warmup_thread();

	ok(old_server_list_reference.expired(),
		"PolarDB idle warmup refresh: idle thread releases the old server list");
}

static void test_reader_selection_uses_all_configured_servers() {
	const int writer_hg = 1100;
	const int reader_hg = 1101;
	const int reader_count = 512;
	const int reader_base_port = 26000;
	const int reader_weight = 8388608;

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB reader selection scale",
		writer_hg, "polardb-reader-scale-writer", 25999,
		reader_hg, "polardb-reader-scale-", reader_count,
		reader_base_port, reader_weight);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *target = nullptr;
	for (int index = 0; index < reader_count; index++) {
		const std::string address =
			"polardb-reader-scale-" + std::to_string(index + 1);
		PgSQL_SrvC *reader = find_pgsql_server(
			reader_hgc, address.c_str(), reader_base_port + index);
		if (!reader) {
			continue;
		}
		if (index + 1 == reader_count) {
			target = reader;
		}
	}
	ok(target != nullptr,
		"PolarDB reader selection scale: last configured reader is available");
	if (!target) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(target);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(target, conn);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	bool selected_target_every_time = true;
	for (unsigned int attempt = 0; attempt < 16; attempt++) {
		PolarDB_ReaderResult result = PgHGM->get_MyConn_polardb_reader(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
		if (!result.acquired() || result.srv != target) {
			selected_target_every_time = false;
			if (result.conn) {
				PgHGM->push_MyConn_to_pool(result.conn);
			}
			break;
		}
		PgHGM->push_MyConn_to_pool(result.conn);
	}
	ok(selected_target_every_time,
		"PolarDB reader selection scale: large total weights and every configured reader remain usable");

	target->remove_free_connection(conn);
	delete conn;
}

static PgSQL_SplitWarmupRequest make_unit_warmup_request(
		unsigned int reader_hg,
		unsigned int max_connections_per_request) {
	PolarDB_StartupClientContext startup_client;
	startup_client.identity = unit_proxy_identity();
	return PgSQL_SplitWarmupRequest{
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client,
		monotonic_time(), max_connections_per_request};
}

static size_t polardb_unit_distinct_warmup_targets(
		const std::vector<PgSQL_SplitWarmupRequest>& target_requests) {
	std::vector<std::string> seen;
	for (const PgSQL_SplitWarmupRequest& req : target_requests) {
		std::string key = req.target_address + ":" +
			std::to_string(static_cast<unsigned int>(req.target_port));
		if (std::find(seen.begin(), seen.end(), key) == seen.end()) {
			seen.push_back(std::move(key));
		}
	}
	return seen.size();
}

static void test_split_warmup_target_expansion_policy() {
	const int writer_hg_one = 980;
	const int reader_hg_one = 981;
	const int writer_hg_many = 982;
	const int reader_hg_many = 983;
	const int writer_hg_full = 984;
	const int reader_hg_full = 985;
	const int writer_hg_no_rfq = 986;
	const int reader_hg_no_rfq = 987;

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup one-reader expansion",
		writer_hg_one, "polardb-warmup-one-writer", 24632,
		reader_hg_one, "polardb-warmup-one-reader-", 1, 24732);

	PgSQL_PolarDB_ReaderPool pool(PgHGM);

	std::vector<PgSQL_SplitWarmupRequest> one_reader_targets;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_SplitWarmupRequest one_reader_req =
		make_unit_warmup_request(reader_hg_one, 4);
	PgHGM->wrlock();
	const size_t one_reader_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, one_reader_req, one_reader_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	bool one_reader_sequence_ok = one_reader_count == 4 &&
		polardb_unit_distinct_warmup_targets(one_reader_targets) == 1;
	for (size_t i = 0; i < one_reader_targets.size(); ++i) {
		if (one_reader_targets[i].target_port != 24732 ||
				one_reader_targets[i].target_required_free_count != i + 1) {
			one_reader_sequence_ok = false;
		}
	}
	ok(found_hostgroup && saw_eligible_target && !saw_compatible_free &&
			one_reader_sequence_ok,
		"PolarDB warmup expansion: one reader can receive multiple target requests up to the request maximum");

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup many-reader expansion",
		writer_hg_many, "polardb-warmup-many-writer", 24642,
		reader_hg_many, "polardb-warmup-many-reader-", 8, 24742);

	std::vector<PgSQL_SplitWarmupRequest> many_reader_targets;
	found_hostgroup = false;
	saw_eligible_target = false;
	saw_compatible_free = false;
	PgSQL_SplitWarmupRequest many_reader_req =
		make_unit_warmup_request(reader_hg_many, 4);
	PgHGM->wrlock();
	const size_t many_reader_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, many_reader_req, many_reader_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	ok(found_hostgroup && saw_eligible_target && !saw_compatible_free &&
			many_reader_count == 4 &&
			polardb_unit_distinct_warmup_targets(many_reader_targets) == 4,
		"PolarDB warmup expansion: many readers receive at most one target each before repeats");

	std::vector<PgSQL_SplitWarmupRequest> first_single_target;
	std::vector<PgSQL_SplitWarmupRequest> second_single_target;
	PgSQL_SplitWarmupRequest single_req =
		make_unit_warmup_request(reader_hg_many, 1);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, single_req, first_single_target,
		nullptr, nullptr, nullptr);
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, single_req, second_single_target,
		nullptr, nullptr, nullptr);
	PgHGM->wrunlock();
	ok(first_single_target.size() == 1 &&
			second_single_target.size() == 1 &&
			first_single_target[0].target_port !=
				second_single_target[0].target_port,
		"PolarDB warmup expansion: repeated single-target requests rotate reader selection");

	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg_no_rfq, "polardb-warmup-no-rfq-writer", 24662,
			reader_hg_no_rfq, "polardb-warmup-no-rfq-reader", 24762)) == 0,
		"PolarDB warmup RFQ profile: writer and reader staged for commit");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row_with_protocol(
			writer_hg_no_rfq, reader_hg_no_rfq, "off"),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB warmup RFQ profile: topology commit succeeds");

	std::vector<PgSQL_SplitWarmupRequest> no_rfq_targets;
	found_hostgroup = false;
	saw_eligible_target = false;
	saw_compatible_free = false;
	PgSQL_SplitWarmupRequest no_rfq_req =
		make_unit_warmup_request(reader_hg_no_rfq, 4);
	PgHGM->wrlock();
	const size_t no_rfq_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, no_rfq_req, no_rfq_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	ok(found_hostgroup && !saw_eligible_target &&
			!saw_compatible_free && no_rfq_count == 0,
		"PolarDB warmup RFQ profile: protocol without RFQ fields produces no target requests");

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup full-reader expansion",
		writer_hg_full, "polardb-warmup-full-writer", 24652,
		reader_hg_full, "polardb-warmup-full-reader-", 1, 24752);

	PgSQL_HGC *full_reader_hgc = PgHGM->MyHGC_lookup(reader_hg_full);
	PgSQL_SrvC *full_reader =
		find_pgsql_server(full_reader_hgc, "polardb-warmup-full-reader-1", 24752);
	ok(full_reader != nullptr,
		"PolarDB warmup expansion: full-reader server container is available");
	if (full_reader) {
		const long saved_max_connections = full_reader->max_connections;
		PgSQL_Connection *incompatible = make_cached_reader_connection(full_reader);
		incompatible->pgsql_conn = unit_connected_pgconn();
		incompatible->polardb_startup_client.identity =
			unit_other_proxy_identity();
		full_reader->ConnectionsFree->add(incompatible);
		full_reader->max_connections = 1;

		std::vector<PgSQL_SplitWarmupRequest> full_reader_targets;
		found_hostgroup = false;
		saw_eligible_target = false;
		saw_compatible_free = false;
		PgSQL_SplitWarmupRequest full_reader_req =
			make_unit_warmup_request(reader_hg_full, 4);
		PgHGM->wrlock();
		const size_t full_reader_count =
			pgsql_polardb_unit_collect_split_warmup_targets(
				&pool, full_reader_req, full_reader_targets,
				&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
		PgHGM->wrunlock();

		ok(found_hostgroup && !saw_eligible_target &&
				!saw_compatible_free && full_reader_count == 0,
			"PolarDB warmup expansion: saturated reader receives no target requests");

		full_reader->ConnectionsFree->remove(incompatible);
		full_reader->max_connections = saved_max_connections;
		delete incompatible;
	}
}

static void test_txn_reader_failure_route_state_contract() {
	PgSQL_Session::PolarDB_TxnReaderFailureState state;
	int writer_hg = -1;

	ok(!state.active(),
		"PolarDB reader failure state: default state is inactive");
	ok(!state.force_writer(&writer_hg) && writer_hg == -1,
		"PolarDB reader failure state: default state has no writer override");

	state.set_force_writer(710);
	ok(state.active(),
		"PolarDB reader failure state: writer route is active");
	ok(state.force_writer(&writer_hg) && writer_hg == 710,
		"PolarDB reader failure state: writer hostgroup is returned");

	const char* skipped_address = nullptr;
	int skipped_port = -1;
	state.set_reader_skip(711, "reader-a", 15433);
	ok(state.active() && !state.force_writer(),
		"PolarDB reader failure state: reader skip does not force writer");
	ok(state.reader_skip(711, &skipped_address, &skipped_port) &&
			strcmp(skipped_address, "reader-a") == 0 &&
			skipped_port == 15433,
		"PolarDB reader failure state: matching failed reader is returned");
	ok(!state.reader_skip(712, &skipped_address, &skipped_port),
		"PolarDB reader failure state: another hostgroup is not skipped");

	state.clear();
	ok(!state.active(),
		"PolarDB reader failure state: clear returns to inactive state");
}

static void test_session_route_state_clear_tiers() {
	PgSQL_Session::PolarDB_SessionRouteState state;
	state.wait_disabled = true;
	state.client_rfq_lsn_requested = true;
	state.rfq_degraded_route_warning_sent = true;

	state.clear_resettable();
	ok(!state.wait_disabled &&
			!state.rfq_degraded_route_warning_sent &&
			state.client_rfq_lsn_requested,
		"PolarDB route state: resettable clear keeps client RFQ capability");

	state.wait_disabled = true;
	state.rfq_degraded_route_warning_sent = true;
	state.clear_session();
	ok(!state.wait_disabled &&
			!state.rfq_degraded_route_warning_sent &&
			!state.client_rfq_lsn_requested,
		"PolarDB route state: session clear removes client RFQ capability");
}

static void test_notice_queue_state_contract() {
	PgSQL_Session::PolarDB_NoticeQueueState notices;
	ok(notices.empty() && notices.len() == 0,
		"PolarDB notice queue: default state is empty");

	unsigned char* freed_pkt = (unsigned char*)l_alloc(4);
	memset(freed_pkt, 'n', 4);
	notices.add(freed_pkt, 4);
	ok(!notices.empty() && notices.len() == 1,
		"PolarDB notice queue: add allocates queue lazily");
	notices.clear(/*free_buffers=*/true);
	ok(notices.empty() && notices.len() == 0 && notices.pending == nullptr,
		"PolarDB notice queue: clear with free releases queue");

	unsigned char* moved_pkt = (unsigned char*)l_alloc(3);
	memset(moved_pkt, 'm', 3);
	notices.add(moved_pkt, 3);
	notices.clear(/*free_buffers=*/false);
	ok(notices.empty() && notices.pending == nullptr,
		"PolarDB notice queue: clear without free releases only queue owner");
	l_free(3, moved_pkt);
}

static void test_user_attributes_are_reapplied_after_reset() {
	PgSQL_Session sess;
	sess.user_attributes = strdup(
		"{\"default-transaction_isolation\":\"serializable\"}");

	sess.polardb_config.txn_reader_wait_default_read_committed = true;
	sess.polardb_config.txn_reader_wait_backend_default_seen = true;
	sess.polardb_reapply_user_attributes_after_reset();
	ok(!sess.polardb_txn_reader_wait_isolation_read_committed(),
		"PolarDB reset attributes: serializable user default disables pre-write reader wait");
	ok(!sess.polardb_config.txn_reader_wait_backend_default_seen,
		"PolarDB reset attributes: backend default observation is cleared");

	free(sess.user_attributes);
	sess.user_attributes = strdup(
		"{\"default-transaction_isolation\":\"read committed\"}");
	sess.polardb_config.txn_reader_wait_default_read_committed = false;
	sess.polardb_config.txn_reader_wait_backend_default_seen = true;
	sess.polardb_reapply_user_attributes_after_reset();
	ok(sess.polardb_txn_reader_wait_isolation_read_committed(),
		"PolarDB reset attributes: read committed user default enables pre-write reader wait");
	ok(!sess.polardb_config.txn_reader_wait_backend_default_seen,
		"PolarDB reset attributes: refreshed user default owns the reset state");
}

static void test_collect_is_const_stable_snapshot() {
	const int writer_hg = 978;
	const int reader_hg = 979;

	stage_polardb_topology_with_txn_split(PgHGM, "PolarDB collect snapshot",
		writer_hg, "polardb-collect-writer", 24432,
		reader_hg, "polardb-collect-reader", 24433);

	const auto* writer_cfg = PgHGM->find_polardb_hg_config(writer_hg);
	ok(writer_cfg != nullptr && writer_cfg->writer_epoch != nullptr,
		"PolarDB collect snapshot: writer config is available");
	if (!writer_cfg || !writer_cfg->writer_epoch) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	sess.thread = worker.get();
	attach_test_frontend(sess);
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	const char locking_query[] = "SELECT * FROM t FOR UPDATE";
	sess.CurrentQuery.begin(
		(unsigned char*)const_cast<char*>(locking_query),
		strlen(locking_query) + 1,
		false);
	sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SELECT;
	sess.polardb_session_consistency.writer_scope = PolarDB_WriterScope{
		writer_cfg->writer_hostgroup,
		writer_cfg->writer_epoch->load(std::memory_order_relaxed)};
	sess.polardb_session_consistency.write_lsn = 0x2110;
	sess.polardb_session_consistency.observed_lsn = 0x2220;
	sess.polardb_txn_wait_safety.local_state_changed = true;

	const PolarDB_SessionConsistency before_session =
		sess.polardb_session_consistency;
	const bool before_local_state =
		sess.polardb_txn_wait_safety.local_state_changed;
	const PgSQL_Session& const_sess = sess;
	PolarDB_Query_RouteCtx first;
	PolarDB_Query_RouteCtx second;
	const_sess.polardb_collect(first, writer_hg, /*qpo_replica_eligible=*/1,
		/*qpo_force_primary_hint=*/false);
	const_sess.polardb_collect(second, writer_hg, /*qpo_replica_eligible=*/1,
		/*qpo_force_primary_hint=*/false);

	ok(first.is_polar_hg && second.is_polar_hg,
		"PolarDB collect snapshot: repeated collect sees PolarDB topology");
	ok(!first.is_txn_split_safe_read && first.is_txn_split_locking_read,
		"PolarDB collect snapshot: autocommit locking read is marked writer-required");
	PolarDB_Query_RoutePlan locking_plan = sess.polardb_plan(first);
	ok(locking_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			locking_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::SPLIT_LOCKING_READ,
		"PolarDB collect snapshot: autocommit locking read plans writer route");
	ok(first.writer_scope.matches(second.writer_scope) &&
			first.reader_hg == second.reader_hg &&
			first.effective_consistency_mode ==
				second.effective_consistency_mode,
		"PolarDB collect snapshot: repeated collect returns stable route inputs");
	ok(first.session.write_lsn == before_session.write_lsn &&
			first.session.observed_lsn == before_session.observed_lsn,
		"PolarDB collect snapshot: collected session LSNs match source state");
	ok(sess.polardb_session_consistency.write_lsn == before_session.write_lsn &&
			sess.polardb_session_consistency.observed_lsn ==
				before_session.observed_lsn &&
			sess.polardb_txn_wait_safety.local_state_changed ==
				before_local_state,
		"PolarDB collect snapshot: collect does not mutate session state");

	sess.polardb_observe_route_inputs(writer_hg);
	ok(sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: LSN transaction split checks backend isolation status");
	sess.polardb_query.reset_for_new_query();
	ok(!sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: query reset clears backend isolation status check");
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::OFF);
	sess.polardb_observe_route_inputs(writer_hg);
	ok(!sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: consistency off skips backend isolation status check");
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);

	if (sess.transaction_state_manager) {
		sess.transaction_state_manager->handle_transaction("BEGIN");
		sess.polardb_txn_wait_safety.local_state_changed = false;
		const char proxysql_set[] =
			"SET proxysql.polardb_consistency_mode = lsn";
		sess.CurrentQuery.query_parser_free();
		sess.CurrentQuery.begin(
			(unsigned char*)const_cast<char*>(proxysql_set),
			strlen(proxysql_set) + 1,
			false);
		sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SET;
		sess.polardb_observe_route_inputs(writer_hg);
		ok(!sess.polardb_txn_wait_safety.local_state_changed,
			"PolarDB observe: ProxySQL-owned in-transaction SET does not dirty reader-wait state");

		const char backend_set[] = "SET TimeZone = 'UTC'";
		sess.CurrentQuery.query_parser_free();
		sess.CurrentQuery.begin(
			(unsigned char*)const_cast<char*>(backend_set),
			strlen(backend_set) + 1,
			false);
		sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SET;
		sess.polardb_observe_route_inputs(writer_hg);
		ok(sess.polardb_txn_wait_safety.local_state_changed,
			"PolarDB observe: backend-visible in-transaction SET dirties reader-wait state");
	} else {
		ok(1, "PolarDB observe: ProxySQL-owned SET skipped without transaction manager");
		ok(1, "PolarDB observe: backend-visible SET skipped without transaction manager");
	}
}

int main() {
	plan(NO_PLAN);

	int rc = test_init_minimal();
	ok(rc == 0, "test_init_minimal() succeeds");

	rc = test_init_query_processor();
	ok(rc == 0, "test_init_query_processor() succeeds");
	if (GloPTH) {
		GloPTH->variables.hostgroup_manager_verbose = 0;
	}

	rc = test_init_hostgroups();
	ok(rc == 0, "test_init_hostgroups() succeeds");

	test_polardb_counter_metadata();
	test_polardb_parent_byte_flush_accounting();
	test_polardb_writev_direct_send();
	test_polardb_counter_order_metadata();
	test_polardb_thread_counter_aggregation_and_fold();
	test_writer_epoch_change_resets_lsn_caches();
	test_hostgroup_config_cache_refreshes_after_reload();
	test_monitor_lsn_update_skips_non_online_servers();
	test_lsn_observation_refreshes_freshness_timestamp();
	test_v2_target_reader_keeps_wait_until_lsn_is_reached();
	test_server_selection_snapshot_refresh_and_immutability();
	test_reader_selection_reload_concurrency();
	test_server_list_snapshot_survives_topology_purge();
	test_reader_pool_request_limits();
	test_core_match_pool_index_and_transfer();
	test_core_match_pool_concurrent_transfer();
	test_core_match_pool_concurrent_boundaries();
	test_keyless_core_use_restores_exact_match();
	test_selected_server_survives_concurrent_purge();
	test_idle_ping_connection_survives_topology_purge();
	test_worker_local_reader_selection_sequence();
	test_two_reader_degraded_uses_healthy_peer();
	test_two_reader_selection_ignores_inventory_and_alternates();
	test_two_reader_unequal_weights_keep_global_sequence();
	test_multi_reader_selection_samples_two_servers();
	test_reader_pool_status_and_pooled_only_contract();
	test_reader_pool_capacity_accounting_includes_reuse_pool();
	test_reader_pool_shared_inventory_rejects_unrelated_connection();
	test_reader_pool_shared_prefix_reaches_compatible_backend();
	test_reader_pool_profile_mismatch_drops_backend();
	test_reader_pool_identity_mode_mismatch_drops_backend();
	test_reader_pool_client_identity_mode_not_released_to_shared();
#if POLARDB_PROFILE
	test_reader_pool_shared_transfer_counters();
#endif // POLARDB_PROFILE
	test_reader_pool_worker_local_reuse();
	test_reader_pool_worker_local_duplicates_last_one_pass();
	test_reader_pool_return_rebuilds_cleared_key();
	test_reader_pool_destroy_used_connection_updates_accounting();
	test_reader_pool_rejected_return_defers_destruction();
	test_reader_pool_batch_rejected_return_defers_destruction();
	test_classic_worker_local_cache_policy();
	test_reader_pool_key_invalidates_on_variable_change();
	test_classic_free_profile_mismatch_drops_backend();
	test_v2_profile_mismatch_does_not_scan_unrelated_key();
	test_split_warmup_counts_shared_reader_pool_inventory();
	test_split_warmup_ignores_incompatible_shared_inventory();
	test_no_wait_pooled_reader_requires_startup_identity();
	test_no_wait_pooled_reader_requires_exact_session_state();
	test_successful_wait_cache_advance_requires_active_wait();
	test_reader_lsn_lag_range_predicate();
	test_txn_reader_state_clear_contract();
	test_tied_freshest_behind_reader_can_acquire_second_candidate();
	test_lag_range_best_behind_reader_can_acquire_lower_lsn_candidate();
	test_split_warmup_request_dedup();
	test_split_warmup_runtime_variable_refresh();
	test_split_warmup_failure_accounting_policy();
	test_split_warmup_max_connections_per_request_policy();
	test_split_warmup_target_expansion_policy();
	test_txn_reader_failure_route_state_contract();
	test_session_route_state_clear_tiers();
	test_notice_queue_state_contract();
	test_user_attributes_are_reapplied_after_reset();
	test_collect_is_const_stable_snapshot();
	test_split_warmup_thread_refreshes_idle_server_list();
	test_reader_selection_uses_all_configured_servers();

	test_cleanup_hostgroups();
	test_cleanup_query_processor();
	test_cleanup_minimal();

	return exit_status();
}

#endif // POLARDB_PROXY
