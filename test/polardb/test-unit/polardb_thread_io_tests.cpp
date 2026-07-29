/**
 * @file polardb_thread_io_tests.cpp
 * @brief Worker counters, worker lifecycle, and frontend transport tests.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
extern "C" {
#include "postgres_fe.h"
#include "libpq-int.h"
}
#undef snprintf
#undef vsnprintf

#include "polardb_unit_common.h"
#include "polardb_unit_support.h"
#include "polardb_unit_domains.h"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

class PolarDB_TestSocketPair {
public:
	PolarDB_TestSocketPair() {
		if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds_) != 0) {
			fds_[0] = -1;
			fds_[1] = -1;
		}
	}

	~PolarDB_TestSocketPair() {
		close_fd(fds_[0]);
		close_fd(fds_[1]);
	}

	bool valid() const { return fds_[0] >= 0 && fds_[1] >= 0; }
	int first() const { return fds_[0]; }
	int second() const { return fds_[1]; }

	// The data stream closes an endpoint after a test transfers ownership to it.
	int release_first() {
		const int fd = fds_[0];
		fds_[0] = -1;
		return fd;
	}

	void close_second() {
		close_fd(fds_[1]);
		fds_[1] = -1;
	}

private:
	static void close_fd(int fd) {
		if (fd >= 0) {
			close(fd);
		}
	}

	int fds_[2]{-1, -1};
};

static void test_polardb_counter_metadata() {
	int thread_count = 0;
	int global_count = 0;
	int gauge_count = 0;
	const char *wait_lsn_prom_name = nullptr;
	const char *wait_bypass_prom_name = nullptr;
	const char *split_fallback_reader_busy_prom_name = nullptr;
	const char *split_fallback_reader_lag_exceeded_prom_name = nullptr;
	const char *split_pool_miss_reserved_exact_prom_name = nullptr;
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
	const char *busy_alternate_miss_display_name = nullptr;
	const char *busy_alternate_miss_prom_name = nullptr;

#define X(name, display_name, prom_name, help) \
	++thread_count; \
	if (strcmp(#name, "wait_lsn_sum_us") == 0) wait_lsn_prom_name = prom_name; \
	if (strcmp(#name, "wait_wrap_bypassed") == 0) wait_bypass_prom_name = prom_name; \
	if (strcmp(#name, "split_fallback_reader_busy") == 0) split_fallback_reader_busy_prom_name = prom_name; \
	if (strcmp(#name, "split_fallback_reader_lag_exceeded") == 0) split_fallback_reader_lag_exceeded_prom_name = prom_name; \
	if (strcmp(#name, "split_pool_miss_reserved_exact") == 0) split_pool_miss_reserved_exact_prom_name = prom_name; \
	if (strcmp(#name, "txn_wait_reader_reconciled") == 0) txn_wait_reader_reconciled_prom_name = prom_name; \
	if (strcmp(#name, "reader_pool_drop_client_identity") == 0) drop_client_identity_prom_name = prom_name; \
	if (strcmp(#name, "parent_bytes_flush_recv_atomic") == 0) parent_bytes_flush_recv_atomic_prom_name = prom_name; \
	if (strcmp(#name, "writev_attempts") == 0) writev_attempts_prom_name = prom_name; \
	if (strcmp(#name, "output_coalesce_hold") == 0) output_coalesce_hold_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_attempts") == 0) result_row_run_attempts_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_used") == 0) result_row_run_used_prom_name = prom_name; \
	if (strcmp(#name, "result_row_run_not_candidate") == 0) result_row_run_not_candidate_prom_name = prom_name; \
	if (strcmp(#name, "reader_pool_busy_alternate_miss") == 0) { \
		busy_alternate_miss_display_name = display_name; \
		busy_alternate_miss_prom_name = prom_name; \
	}
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
#if POLARDB_PROFILE
	ok(split_pool_miss_reserved_exact_prom_name != nullptr &&
			strcmp(split_pool_miss_reserved_exact_prom_name,
				"proxysql_polardb_split_pool_miss_reserved_exact_total") == 0,
		"PolarDB counters: reserved-exact split miss is available in profile builds");
#else
	ok(split_pool_miss_reserved_exact_prom_name == nullptr,
		"PolarDB counters: reserved-exact split miss is absent from release builds");
#endif // POLARDB_PROFILE
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
	ok(busy_alternate_miss_display_name != nullptr &&
			strcmp(busy_alternate_miss_display_name,
				"PolarDB_Reader_Pool_Busy_Alternate_Miss") == 0 &&
			busy_alternate_miss_prom_name != nullptr &&
			strcmp(busy_alternate_miss_prom_name,
				"proxysql_polardb_reader_pool_busy_alternate_miss_total") == 0,
		"PolarDB counters: alternate-miss accounting uses outcome-based exported names");
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
		"PolarDB parent bytes: detach keeps bytes local and adds query totals");
	ok(backend.polardb_parent_bytes_recv_pending == 123 &&
			backend.polardb_parent_bytes_sent_pending == 45 &&
			backend.polardb_parent_queries_sent_pending == 0 &&
			backend.polardb_parent_query_batch_count == 63,
		"PolarDB parent bytes: detach preserves only pending byte accounting");
	backend.polardb_note_query_sent();

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
	backend.polardb_flush_parent_counters(
		PolarDB_ParentCounterFlushReason::THRESHOLD_RECV);
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
		"PolarDB parent bytes: connection destruction flushes pending accounting");
	ok(PgHGM->status.polardb_parent_bytes_flush_destructor.load(std::memory_order_relaxed) ==
			destructor_before + 1,
		"PolarDB parent bytes: destructor flush reason is counted");
}

static bool polardb_plain_send_denied_by_policy(int* denied_errno) {
	PolarDB_TestSocketPair probe_sockets;
	if (!probe_sockets.valid()) {
		return false;
	}

	const char byte = 'x';
	errno = 0;
#ifdef __APPLE__
	const ssize_t sent = send(probe_sockets.first(), &byte, 1, 0);
#else
	const ssize_t sent = send(probe_sockets.first(), &byte, 1, MSG_NOSIGNAL);
#endif
	const int send_errno = sent < 0 ? errno : 0;

	if (denied_errno) {
		*denied_errno = send_errno;
	}
	return sent < 0 && (send_errno == EPERM || send_errno == EACCES);
}

static void test_polardb_writev_direct_send() {
	{
		PolarDB_TestSocketPair small_sockets;
		ok(small_sockets.valid(),
			"PolarDB writev: small-batch socketpair fixture is available");
		if (small_sockets.valid()) {
			int denied_errno = 0;
			const bool plain_send_denied =
				polardb_plain_send_denied_by_policy(&denied_errno);
			if (plain_send_denied) {
				const char* denied_name = denied_errno == EPERM ? "EPERM" : "EACCES";
				diag("Skipping buffered send checks: send() is blocked by execution policy (errno=%d %s)",
					denied_errno, denied_name);
			}

			ProxySQL_Poll<PgSQL_Data_Stream> small_polls;
			std::unique_ptr<PgSQL_Thread> small_worker(new PgSQL_Thread());
			small_worker->curtime = monotonic_time();

			PgSQL_Session small_sess;
			attach_test_frontend(small_sess, small_worker.get());
			PgSQL_Data_Stream* small_myds = small_sess.client_myds;
			small_myds->fd = small_sockets.release_first();
			small_myds->DSS = STATE_CLIENT_AUTH_OK;
			small_polls.add(POLLIN | POLLOUT, small_myds->fd, small_myds, 0);
			pgsql_thread___polardb_writev_direct = true;

			char* small_first = static_cast<char*>(l_alloc(5));
			memcpy(small_first, "abcde", 5);
			small_myds->PSarrayOUT->add(small_first, 5);
			const char* buffered_expected = "abcde";

			const PolarDB_ThreadCounterSnapshot small_fallback(
				small_worker.get(),
				polardb_st_var_writev_small_batch_fallback);
			const PolarDB_ThreadCounterSnapshot attempts(
				small_worker.get(), polardb_st_var_writev_attempts);
			const PolarDB_ThreadCounterSnapshot buffered_fallback(
				small_worker.get(),
				polardb_st_var_writev_buffered_fallback);
			small_sess.writeout();
			char small_received[8] = {};
			const ssize_t small_read_bytes =
				recv(small_sockets.second(), small_received,
					sizeof(small_received), MSG_DONTWAIT);

			if (plain_send_denied) {
				skip(2, "send() is unavailable under the current execution policy");
			} else {
				ok(small_read_bytes == 5 &&
						memcmp(small_received, buffered_expected, 5) == 0,
					"PolarDB writev: session writeout sends a small buffered result");
				ok(small_myds->PSarrayOUT->len == 0 &&
						small_myds->queueOUT.head == small_myds->queueOUT.tail,
					"PolarDB writev: small buffered result leaves no queued output");
			}
			ok(small_fallback.delta() == 0,
				"PolarDB writev: canonical buffered writeout skips the direct-write fallback probe");
			ok(attempts.delta() == 0 && buffered_fallback.delta() == 0,
				"PolarDB writev: small buffered output does not count a direct attempt");
		}
	}

	PolarDB_TestSocketPair sockets;
	ok(sockets.valid(),
		"PolarDB writev: socketpair fixture is available");
	if (!sockets.valid()) return;

	ProxySQL_Poll<PgSQL_Data_Stream> polls;
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	worker->curtime = monotonic_time();

	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PgSQL_Data_Stream* myds = sess.client_myds;
	myds->fd = sockets.release_first();
	myds->DSS = STATE_CLIENT_AUTH_OK;
	myds->myconn->set_status(true, STATUS_PGSQL_CONNECTION_COMPRESSION);
	polls.add(POLLIN | POLLOUT, myds->fd, myds, 0);
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

	const PolarDB_ThreadCounterSnapshot attempts(
		worker.get(), polardb_st_var_writev_attempts);
	const PolarDB_ThreadCounterSnapshot bytes(
		worker.get(), polardb_st_var_writev_bytes);
	const PolarDB_ThreadCounterSnapshot packets(
		worker.get(), polardb_st_var_writev_packets);

	const int first_written = myds->polardb_writev_to_net_poll(4);
	char received[8] = {};
	const ssize_t first_read_bytes =
		recv(sockets.second(), received, sizeof(received), MSG_DONTWAIT);

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
		recv(sockets.second(), received, sizeof(received), MSG_DONTWAIT);

	ok(static_cast<size_t>(second_written) == second_size - 1,
		"PolarDB writev: second direct send reports remaining bytes");
	ok(second_read_bytes > 0 && received[0] == 'e',
		"PolarDB writev: second direct send starts from the remaining partial byte");
	ok(myds->PSarrayOUT->len == 0 && myds->polardb_write_head_partial == 0,
		"PolarDB writev: fully sent packets are removed from PSarrayOUT");
	ok(attempts.delta() == 2,
		"PolarDB writev: attempt counter increments");
	ok(bytes.delta() == 3 + second_size,
		"PolarDB writev: byte counter increments by accepted bytes");
	ok(packets.delta() == 2,
		"PolarDB writev: packet counter counts fully sent packets");
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
	const int split_no_marker =
		polardb_ordered_counter_index("PolarDB_Split_Rejected_No_Marker");
	const int split_wal_pending =
		polardb_ordered_counter_index("PolarDB_Split_WAL_Pending");

	ok(monitor_lsn >= 0 && monitor_lsn < monitor_role &&
			monitor_role < monitor_values && monitor_values < stale,
		"PolarDB counters: ordered metadata keeps monitor counters with monitor LSN");
	ok(writer_retry >= 0 && rfq_skipped >= 0 && writer_retry < rfq_skipped,
		"PolarDB counters: ordered metadata keeps retry before RFQ profile counters");
	ok(split_for_update >= 0 && split_for_update < split_write_unknown &&
			split_write_unknown < split_observed_unknown &&
			split_observed_unknown < split_no_marker &&
			split_no_marker < split_wal_pending,
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
	ok(GloPTH->polardb_attach_worker(0, worker1) ==
			PolarDB_WorkerAttachResult::ATTACHED &&
			GloPTH->polardb_attach_worker(1, worker2) ==
			PolarDB_WorkerAttachResult::ATTACHED,
		"PolarDB counters: worker slots attach through the protected lifecycle API");

	// Aggregation arithmetic the assertions below verify:
	//   target_lsn_preferred = 10 (global) + 2 (worker1) + 3 (worker2) = 15
	//   wait_lsn_sum_us       = 100 (global) + 7 (worker1) + 11 (worker2) = 118
	//   wait_wrap_bypassed    = 5 (global) + 13 (worker1) + 17 (worker2) = 35
	const uint64_t GLOBAL_TARGET_LSN_PREFERRED = 10;
	const uint64_t GLOBAL_WAIT_LSN_SUM_US = 100;
	const uint64_t GLOBAL_WAIT_WRAP_BYPASSED = 5;
	const uint64_t GLOBAL_CAPACITY_WAIT_MAX_US = 100;
	const uint64_t EXPECTED_TARGET_LSN_PREFERRED = 15;   // 10 + 2 + 3
	const uint64_t EXPECTED_WAIT_LSN_SUM_US = 118;       // 100 + 7 + 11
	const uint64_t EXPECTED_WAIT_WRAP_BYPASSED = 35;     // 5 + 13 + 17
	const uint64_t EXPECTED_CAPACITY_WAIT_MAX_US = 250;
	PgHGM->status.polardb_target_lsn_preferred.store(
		GLOBAL_TARGET_LSN_PREFERRED, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_lsn_sum_us.store(
		GLOBAL_WAIT_LSN_SUM_US, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_wrap_bypassed.store(
		GLOBAL_WAIT_WRAP_BYPASSED, std::memory_order_relaxed);
	PgHGM->status.polardb_reader_capacity_wait_max_us.store(
		GLOBAL_CAPACITY_WAIT_MAX_US, std::memory_order_relaxed);
	worker1->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 2;
	worker2->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 3;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 7;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 11;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 13;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 17;
	worker1->polardb_status_variables.stvar[
		polardb_st_var_reader_capacity_wait_max_us] = 250;
	worker2->polardb_status_variables.stvar[
		polardb_st_var_reader_capacity_wait_max_us] = 180;
	worker1->status_variables.pgconnpoll_push = 2;
	worker2->status_variables.pgconnpoll_push = 3;
	const unsigned long pgconnpoll_push_before = __atomic_load_n(
		&PgHGM->status.pgconnpoll_push, __ATOMIC_RELAXED);

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
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_reader_capacity_wait_max_us,
			PgHGM->status.polardb_reader_capacity_wait_max_us) ==
			EXPECTED_CAPACITY_WAIT_MAX_US,
		"PolarDB counters: maximum aggregation selects the largest worker wait");
	ok(GloPTH->get_pgconnpoll_push() == 5,
		"PostgreSQL pool counters: aggregation includes worker-local pushes");

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

	ok(GloPTH->polardb_detach_worker(0, worker1) &&
			GloPTH->polardb_detach_worker(1, worker2),
		"PolarDB counters: worker slots detach before worker destruction");
	delete worker1;
	delete worker2;
	ok(__atomic_load_n(&PgHGM->status.pgconnpoll_push, __ATOMIC_RELAXED) ==
			pgconnpoll_push_before + 5,
		"PostgreSQL pool counters: worker teardown folds local pushes");

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

static void test_polardb_worker_attach_outcomes() {
	ok(GloPTH != nullptr && GloPTH->pgsql_threads != nullptr,
		"PolarDB worker lifecycle: handler storage is initialized");
	if (!GloPTH || !GloPTH->pgsql_threads) {
		return;
	}

	PgSQL_Thread* first_worker = new PgSQL_Thread();
	PgSQL_Thread* second_worker = new PgSQL_Thread();
	ok(GloPTH->polardb_attach_worker(0, first_worker) ==
			PolarDB_WorkerAttachResult::ATTACHED,
		"PolarDB worker lifecycle: an empty worker slot accepts its worker");
	ok(GloPTH->polardb_attach_worker(0, second_worker) ==
			PolarDB_WorkerAttachResult::INVALID_SLOT,
		"PolarDB worker lifecycle: an occupied worker slot is reported as invalid");
	ok(GloPTH->polardb_detach_worker(0, first_worker),
		"PolarDB worker lifecycle: the attached worker detaches before destruction");

	PolarDB_WorkerLifecycleUnitAccess::set_shutdown(GloPTH, true);
	ok(GloPTH->polardb_attach_worker(0, second_worker) ==
			PolarDB_WorkerAttachResult::SHUTDOWN_STARTED &&
			GloPTH->pgsql_threads[0].worker == nullptr,
		"PolarDB worker lifecycle: shutdown rejects a new worker without registering it");
	PolarDB_WorkerLifecycleUnitAccess::set_shutdown(GloPTH, false);

	ok(GloPTH->polardb_attach_worker(0, second_worker) ==
			PolarDB_WorkerAttachResult::ATTACHED &&
			GloPTH->polardb_detach_worker(0, second_worker),
		"PolarDB worker lifecycle: the slot remains usable after the shutdown-race check");
	delete first_worker;
	delete second_worker;
}

void run_polardb_thread_io_tests() {
	test_polardb_counter_metadata();
	test_polardb_parent_byte_flush_accounting();
	test_polardb_writev_direct_send();
	test_polardb_counter_order_metadata();
	test_polardb_thread_counter_aggregation_and_fold();
	test_polardb_worker_attach_outcomes();
}

#endif // POLARDB_PROXY
