#ifndef PROXYSQL_PGSQL_THREAD_H
#define PROXYSQL_PGSQL_THREAD_H
#define PROXYSQL_STANDARD_PGSQL_THREAD_H
#include <prometheus/counter.h>
#include <prometheus/gauge.h>

#include "proxysql.h"
#include "proxysql_admin.h"

#include "Base_Thread.h"
#include "ProxySQL_Poll.h"
#include "PgSQL_PolarDB_Counters.h"
#if POLARDB_PROXY
#include "PgSQL_PolarDB.h"
#endif // POLARDB_PROXY
#include "PgSQL_Variables.h"
#ifdef IDLE_THREADS
#include <sys/epoll.h>
#endif // IDLE_THREADS
#include <atomic>
#include <mutex>

#include "prometheus_helpers.h"

#include "PgSQL_Set_Stmt_Parser.h"

enum class AUTHENTICATION_METHOD {
	NO_PASSWORD,
	CLEAR_TEXT_PASSWORD,
	MD5_PASSWORD,
	SASL_SCRAM_SHA_256,
	SASL_SCRAM_SHA_256_PLUS
};

constexpr const char* AUTHENTICATION_METHOD_STR[] = {
	"NO_PASSWORD",
	"CLEAR_TEXT_PASSWORD",
	"MD5_PASSWORD",
	"SASL_SCRAM_SHA_256",
	"SASL_SCRAM_SHA_256_PLUS"
};

/*
#define MIN_POLL_LEN 8
#define MIN_POLL_DELETE_RATIO  8
#define MY_EPOLL_THREAD_MAXEVENTS 128
*/

#if POLARDB_PROXY
class PgSQL_HGC;
struct PolarDB_Query_ReaderPlan;
struct PolarDB_WaitSpec;
#if POLARDB_PROFILE
struct PolarDB_WaitProfileState;
#endif // POLARDB_PROFILE

// User-facing PolarDB proxy protocol dialect. Hostgroup config may use -1 to
// inherit the global pgsql-polardb_proxy_protocol value.
constexpr int POLARDB_PROXY_PROTOCOL_OFF    = 0;
constexpr int POLARDB_PROXY_PROTOCOL_LEGACY = 1;
constexpr int POLARDB_PROXY_PROTOCOL_V15    = 2;

enum PolarDB_ThreadStatusVariable {
#define X(name, display_name, prom_name, help) polardb_st_var_##name,
	POLARDB_THREAD_COUNTER_LIST(X)
#undef X
	POLARDB_st_var_END
};

static constexpr bool polardb_thread_counter_aggregates_max(
		PolarDB_ThreadStatusVariable idx) {
	switch (idx) {
#define X(name) case polardb_st_var_##name: return true;
	POLARDB_THREAD_MAX_COUNTER_LIST(X)
#undef X
	default:
		return false;
	}
}

static_assert(POLARDB_st_var_END == POLARDB_THREAD_COUNTER_COUNT,
	"Update PolarDB thread-counter aggregation/export coverage");

/**
 * @brief Per-worker PolarDB counter storage.
 *
 * Only the worker that owns this instance may write the array, and it writes
 * plain non-atomic increments. The stats path reads the values cross-thread
 * with relaxed atomic loads while holding the handler's
 * polardb_worker_lifecycle_mutex_, so scraped values are approximate. At worker
 * teardown the array is folded into the PgHGM global atomics exactly once.
 *
 * The 64-byte alignment is required: without it two workers' counter arrays can
 * share a cache line and every increment bounces that line between cores.
 */
struct alignas(64) PolarDB_ThreadStatusVariables {
	unsigned long long stvar[POLARDB_st_var_END] = {};
};

static inline const char* polardb_thread_counter_display_name(
	PolarDB_ThreadStatusVariable idx)
{
	switch (idx) {
#define X(name, display_name, prom_name, help) \
	case polardb_st_var_##name: \
		return display_name;
	POLARDB_THREAD_COUNTER_LIST(X)
#undef X
	default:
		return "PolarDB_Unknown_Thread_Counter";
	}
}
#endif // POLARDB_PROXY

#define ADMIN_HOSTGROUP	(-2)
#define STATS_HOSTGROUP	(-3)
#define SQLITE_HOSTGROUP (-4)


#define MYSQL_DEFAULT_COLLATION_CONNECTION	""
#define MYSQL_DEFAULT_NET_WRITE_TIMEOUT	"60"
#define MYSQL_DEFAULT_MAX_JOIN_SIZE	"18446744073709551615"

extern class PgSQL_Variables pgsql_variables;

#ifdef IDLE_THREADS
typedef struct __attribute__((aligned(64))) _pgsql_conn_exchange_t {
	pthread_mutex_t mutex_idles;
	PtrArray* idle_mysql_sessions;
	pthread_mutex_t mutex_resumes;
	PtrArray* resume_mysql_sessions;
} pgsql_conn_exchange_t;
#endif // IDLE_THREADS

typedef struct PgSQL_Session_Interrupt {
	uint32_t thread_id;    // Target session
	uint32_t secret_key;   // Auth via shared secret (authentication)
	std::unique_ptr<char, decltype(&free)> username;  // Auth via user identity (authorization)

	// Constructor for key
	PgSQL_Session_Interrupt(uint32_t tid, uint32_t key)
		: thread_id(tid), secret_key(key), username(nullptr, &free) {
	}

	// Constructor for username
	PgSQL_Session_Interrupt(uint32_t tid, const char* user)
		: thread_id(tid), secret_key(0), username((user ? strdup(user) : nullptr), &free) {
	}
} PgSQL_Session_Interrupt_t;

typedef struct PgSQL_Session_Interrupt_Queue_t {
	pthread_mutex_t m;
	std::vector<PgSQL_Session_Interrupt_t> conn_ids;
	std::vector<PgSQL_Session_Interrupt_t> query_ids;
} PgSQL_Session_Interrupt_Queue_t;

enum PgSQL_Thread_status_variable {
	/*st_var_backend_stmt_prepare,
	st_var_backend_stmt_execute,
	st_var_backend_stmt_close,
	st_var_frontend_stmt_prepare,
	st_var_frontend_stmt_describe,
	st_var_frontend_stmt_execute,
	st_var_frontend_stmt_close,
	st_var_queries,
	st_var_queries_slow,
	st_var_queries_gtid,
	st_var_queries_with_max_lag_ms,
	st_var_queries_with_max_lag_ms__delayed,
	st_var_queries_with_max_lag_ms__total_wait_time_us,
	st_var_queries_backends_bytes_sent,
	st_var_queries_backends_bytes_recv,
	st_var_queries_frontends_bytes_sent,
	st_var_queries_frontends_bytes_recv,
	st_var_query_processor_time,
	st_var_backend_query_time,
	st_var_mysql_backend_buffers_bytes,
	st_var_mysql_frontend_buffers_bytes,
	st_var_mysql_session_internal_bytes,
	st_var_ConnPool_get_conn_immediate,
	st_var_ConnPool_get_conn_success,
	st_var_ConnPool_get_conn_failure,
	st_var_ConnPool_get_conn_latency_awareness,
	st_var_gtid_binlog_collected,
	st_var_gtid_session_collected,
	st_var_generated_pkt_err,
	st_var_max_connect_timeout_err,
	st_var_backend_lagging_during_query,
	st_var_backend_offline_during_query,
	st_var_unexpected_com_quit,
	st_var_unexpected_packet,
	st_var_killed_connections,
	st_var_killed_queries,
	st_var_hostgroup_locked,
	st_var_hostgroup_locked_set_cmds,
	st_var_hostgroup_locked_queries,
	st_var_aws_aurora_replicas_skipped_during_query,
	st_var_automatic_detected_sqli,
	st_var_mysql_whitelisted_sqli_fingerprint,
	st_var_client_host_error_killed_connections,
	*/
	// PgSQL reuses the shared st_var_* indexes declared by MySQL_Thread.h.
	// Keep this sized to MY_st_var_END; PgSQL_Thread.cpp has a static_assert
	// to catch drift without introducing a PgSQL_Thread.h <-> MySQL_Thread.h
	// include cycle.
	PG_st_var_END = 45
};


struct CopyCmdMatcher {
	re2::RE2::Options options;
	re2::RE2 pattern;

	CopyCmdMatcher() : 
		options(RE2::Quiet), 
		pattern(
			R"(((?is)\bCOPY\b[^;]*?\bFROM\b[^;]*?\b(?:STDIN|STDOUT)\b))",
			options) {
	}

	inline
	bool match(const char* query, re2::StringPiece* matched = nullptr) const {
		return re2::RE2::PartialMatch(query, pattern, matched);
	}
};

class __attribute__((aligned(64))) PgSQL_Thread : public Base_Thread
{
private:
	unsigned int servers_table_version_previous;
	unsigned int servers_table_version_current;
	unsigned long long last_processing_idles;
	PgSQL_Connection** my_idle_conns;
	bool processing_idles;
	//bool maintenance_loop;

	PtrArray* cached_connections;
	unsigned int push_local_counter;	// round-robin counter for bounded local caching: cache 1-in-N where N = pgsql_threads
#if POLARDB_PROXY
	struct PolarDB_ReaderSelectionState {
		unsigned int hostgroup_id{0};
		uint64_t next_sequence{0};
	};
	uint64_t polardb_reader_selection_generation{0};
	bool polardb_reader_selection_generation_initialized{false};
	std::vector<PolarDB_ReaderSelectionState>
		polardb_reader_selection_sequences;
	uint64_t polardb_reader_cache_epoch{1};
	unsigned int polardb_reader_capacity_waiter_count{0};
	uint64_t polardb_reader_capacity_retry_at_us{0};
	unsigned int polardb_reader_local_connection_count{0};
	bool polardb_reader_local_added_this_pass{false};
	struct PolarDB_ReaderWaitScopeCount {
		uint64_t scope_hash{0};
		unsigned int count{0};
	};
	std::vector<PolarDB_ReaderWaitScopeCount>
		polardb_reader_capacity_wait_scopes;
	void polardb_reader_add_waiter_to_scope(uint64_t scope_hash);
	bool polardb_reader_remove_waiter_from_scope(uint64_t scope_hash);
	std::vector<uint64_t> polardb_reader_capacity_blocked_scopes;
	std::vector<PgSQL_Session*> polardb_reader_capacity_retry_sessions;
	static constexpr unsigned int POLARDB_LSN_PASS_CACHE_CAPACITY = 16;
	static constexpr uint64_t POLARDB_LSN_UPDATE_MAX_DELAY_US = 1000;
	struct PolarDB_PassLsnUpdate {
		uint64_t server_instance_id{0};
		PolarDB_WriterScope writer_scope;
		uint64_t max_lsn{0};
		uint64_t last_updated_at_us{0};
	};
	PolarDB_PassLsnUpdate
		polardb_pass_lsn_updates[POLARDB_LSN_PASS_CACHE_CAPACITY]{};
	unsigned int polardb_pass_lsn_update_count{0};
	bool polardb_worker_pass_active{false};
#if POLARDB_PROFILE
	static constexpr unsigned int POLARDB_PROFILE_PASS_SERVER_CAPACITY = 16;
	// Profile builds track the first 16 servers touched in one worker pass.
	// Further samples are counted as overflow rather than silently misclassified.
	PgSQL_SrvC* polardb_profile_used_count_servers[
		POLARDB_PROFILE_PASS_SERVER_CAPACITY]{};
	unsigned int polardb_profile_lsn_calls{0};
	unsigned int polardb_profile_lsn_advances{0};
	unsigned int polardb_profile_lsn_shared_updates{0};
	unsigned int polardb_profile_lsn_overflow{0};
	unsigned int polardb_profile_used_count_server_count{0};
	unsigned int polardb_profile_used_count_reads{0};
	unsigned int polardb_profile_used_count_overflow{0};
#endif // POLARDB_PROFILE
	/**
	 * @brief Worker-local view of the one reader pool reservation this worker
	 *        may hold.
	 *
	 * All fields are owned by the worker thread and carry no locking of their
	 * own. `hostgroup` needs no lifetime protection because HGCs are
	 * manager-owned and outlive all PostgreSQL workers, but `server` may only be
	 * dereferenced while `server_snapshot` is still held: the snapshot is what
	 * keeps the PgSQL_SrvC alive across a server list rebuild.
	 *
	 * Pool-side cancellation must complete before local state is cleared.
	 */
	struct PolarDB_ReaderPoolWorkerReservationState {
		uint64_t token{0};
		uint32_t session_id{0};
		uint64_t scope_hash{0};
		// HGCs are manager-owned and outlive all PostgreSQL workers.
		PgSQL_HGC* hostgroup{nullptr};
		PgSQL_SrvC* server{nullptr};
		std::shared_ptr<const void> server_snapshot;
		uint32_t profile_generation{0};
		PolarDB_PoolKey pool_key;

		bool active() const {
			return token != 0 && hostgroup != nullptr;
		}
		bool has_connection() const {
			return active() && server != nullptr;
		}
	private:
		friend class PgSQL_Thread;
		void clear_local() {
			token = 0;
			session_id = 0;
			scope_hash = 0;
			hostgroup = nullptr;
			server = nullptr;
			server_snapshot.reset();
			profile_generation = 0;
			pool_key = PolarDB_PoolKey{};
		}
	};
	PolarDB_ReaderPoolWorkerReservationState polardb_reader_pool_reservation;
	// After taking a reserved connection, the worker may retain one compatible
	// reader while the same local wait scope remains active. A matching remote
	// request causes the next completed reader to be reserved for that worker.
	struct PolarDB_ReaderRetentionState {
		// scope_hash tracks local wait-set lifetime only. Remote reservation
		// matching uses the exact request fields below.
		uint64_t scope_hash{0};
		// HGCs are manager-owned and outlive all PostgreSQL workers.
		PgSQL_HGC* hostgroup{nullptr};
		unsigned int hostgroup_id{0};
		uint32_t profile_generation{0};
		PolarDB_PoolKey pool_key;
		PolarDB_Query_ReaderPlan reader_plan;
		PolarDB_WaitSpec wait_spec;

		bool active() const {
			return scope_hash != 0 && hostgroup != nullptr &&
				profile_generation != 0 &&
				!pool_key.empty();
		}
		void reset() {
			scope_hash = 0;
			hostgroup = nullptr;
			hostgroup_id = 0;
			profile_generation = 0;
			pool_key = PolarDB_PoolKey{};
			reader_plan.reset();
			wait_spec.reset();
		}
	};
	PolarDB_ReaderRetentionState polardb_reader_retention;
	/**
	 * @brief One queued "your reservation now has a connection" notification.
	 *
	 * Entries are produced by a different (donor) worker thread through
	 * polardb_queue_reader_reservation_wake() and are drained only by the
	 * worker that owns this queue, in
	 * polardb_process_reader_reservation_wakes(). Keep server_snapshot
	 * attached until the entry is consumed: it is what keeps `server` alive
	 * between the push and the consume, across a server list rebuild.
	 */
	struct PolarDB_ReaderPoolReservationWakeNotification {
		uint64_t token{0};
		PgSQL_SrvC* server{nullptr};
		std::shared_ptr<const void> server_snapshot;
	};
	// Guards polardb_reader_pool_reservation_wakes. Producers are remote
	// workers; the single consumer is the worker that owns this thread object.
	std::mutex polardb_reader_pool_reservation_wake_mutex;
	std::vector<PolarDB_ReaderPoolReservationWakeNotification>
		polardb_reader_pool_reservation_wakes;
	// Set by a producer that also writes the wake byte to pipefd[1], cleared by
	// the owning worker before it drains the vector. Coalesces repeated wakes.
	std::atomic<bool> polardb_reader_pool_reservation_wake_pending{false};
	unsigned int polardb_worker_index{UINT_MAX};
	friend class PgSQL_Threads_Handler;
	friend struct PolarDB_ReaderRetentionUnitAccess;
	friend struct PolarDB_WorkerLifecycleUnitAccess;
#endif // POLARDB_PROXY

#ifdef IDLE_THREADS
	struct epoll_event events[MY_EPOLL_THREAD_MAXEVENTS];
	int efd;
	unsigned int mysess_idx;
	std::map<unsigned int, unsigned int> sessmap;
#endif // IDLE_THREADS

	//Session_Regex** match_regexes;

#ifdef IDLE_THREADS
	void worker_thread_assigns_sessions_to_idle_thread(PgSQL_Thread * thr);
	void worker_thread_gets_sessions_from_idle_thread();
	void idle_thread_gets_sessions_from_worker_thread();
	void idle_thread_assigns_sessions_to_worker_thread(PgSQL_Thread * thr);
	void idle_thread_check_if_worker_thread_has_unprocess_resumed_sessions_and_signal_it(PgSQL_Thread * thr);
	void idle_thread_prepares_session_to_send_to_worker_thread(int i);
	void idle_thread_to_kill_idle_sessions();
	//bool move_session_to_idle_mysql_sessions(PgSQL_Data_Stream * myds, unsigned int n);
#endif // IDLE_THREADS

	//unsigned int find_session_idx_in_mysql_sessions(PgSQL_Session * sess);
	//bool set_backend_to_be_skipped_if_frontend_is_slow(PgSQL_Data_Stream * myds, unsigned int n);
	void handle_mirror_queue_mysql_sessions();

	/**
	 * @brief Processes kill requests from the thread's kill queues.
	 *
	 * @details This function checks the thread's kill queues (`sess_intrpt_queue.conn_ids` and `sess_intrpt_queue.query_ids`)
	 * for any pending kill requests. If there are any requests, it calls `Scan_Sessions_to_Kill_All()`
	 * to iterate through all session arrays across all threads and identify sessions that match
	 * the kill requests. The `killed` flag is set to true for matching sessions. After processing
	 * all kill requests, the kill queues are cleared.
	 *
	 * @note This function is called within the `run()` function during a maintenance loop to
	 * process kill requests for connections and queries. It ensures that sessions matching
	 * kill requests are terminated.
	 *
	 */
	void handle_kill_queues();

	//void check_timing_out_session(unsigned int n);
	//void check_for_invalid_fd(unsigned int n);
	//void read_one_byte_from_pipe(unsigned int n);
	//void tune_timeout_for_myds_needs_pause(PgSQL_Data_Stream * myds);
	//void tune_timeout_for_session_needs_pause(PgSQL_Data_Stream * myds);
	//void configure_pollout(PgSQL_Data_Stream * myds, unsigned int n);

protected:
	int nfds;

public:

	void* gen_args;	// this is a generic pointer to create any sort of structure

	ProxySQL_Poll<PgSQL_Data_Stream> mypolls;
	pthread_t thread_id;
	unsigned long long pre_poll_time;
	unsigned long long last_maintenance_time;
	//unsigned long long last_move_to_idle_thread_time;
	std::atomic<unsigned long long> atomic_curtime;
	//PtrArray* mysql_sessions;
	PtrArray* mirror_queue_mysql_sessions;
	PtrArray* mirror_queue_mysql_sessions_cache;
#ifdef IDLE_THREADS
	PtrArray* idle_mysql_sessions;
	PtrArray* resume_mysql_sessions;
	pgsql_conn_exchange_t myexchange;
#endif // IDLE_THREADS
	CopyCmdMatcher *copy_cmd_matcher;

	int pipefd[2];
	PgSQL_Session_Interrupt_Queue_t sess_intrpt_queue;

	//bool epoll_thread;
	bool poll_timeout_bool;

	// status variables are per thread only
	// in this way, there is no need for atomic operation and there is no cache miss
	// when it is needed a total, all threads are checked
	struct {
		unsigned long long stvar[PG_st_var_END];
		unsigned int active_transactions;
		unsigned long long pgconnpoll_get;
		unsigned long long pgconnpoll_get_ok;
		unsigned long long pgconnpoll_push;
		// tx-poisoned feature counters. Each PgSQL thread maintains its own
		// (lock-free) and PgSQL_Threads_Handler aggregates across threads for
		// stats_pgsql_global exposure. See preserve_client_on_broken_backend_in_tx.
		unsigned long long tx_poisoned_total;
		unsigned long long tx_poisoned_recovered_total;
		unsigned long long tx_poisoned_rejected_statements_total;
	} status_variables;

#if POLARDB_PROXY
	// Worker-owned PolarDB counters. Query-path increments hit this per-thread
	// storage; non-worker paths fall back to the matching global counter atomic.
	// Aggregation/export is wired separately so the metadata, storage, and
	// operator-facing row order remain explicit.
	PolarDB_ThreadStatusVariables polardb_status_variables;
#endif // POLARDB_PROXY

	struct {
		int min_num_servers_lantency_awareness;
		int aurora_max_lag_ms_only_read_from_replicas;
		bool stats_time_backend_query;
		bool stats_time_query_processor;
		bool query_cache_stores_empty_result;
	} variables;

	pthread_mutex_t thread_mutex;

	// if set_parser_algorithm == 2 , a single thr_SetParser is used
	PgSQL_Set_Stmt_Parser *thr_SetParser;

	/**
	 * @brief Default constructor for the PgSQL_Thread class.
	 *
	 * @details This constructor initializes various members of the PgSQL_Thread object to their
	 * default values. It sets up mutexes, initializes status variables, and sets up the thread's
	 * variables. It also sets the thread's `shutdown` flag to `false`, indicating that the thread
	 * is not yet in a shutdown state.
	 *
	 * @note This constructor is called when a new PgSQL_Thread object is created.
	 */
	PgSQL_Thread();

	/**
	 * @brief Destructor for the PgSQL_Thread class.
	 *
	 * @details This destructor cleans up the PgSQL_Thread object, releasing resources and
	 * freeing allocated memory. It deletes session objects, frees cached connections, and
	 * destroys mutexes. It also ensures that the thread's `shutdown` flag is set to `true`
	 * to indicate that the thread is no longer active.
	 *
	 * @note This destructor is called automatically when the PgSQL_Thread object goes out of
	 * scope or is explicitly deleted.
	 */
	~PgSQL_Thread();

	//	PgSQL_Session* create_new_session_and_client_data_stream(int _fd);

	/**
	 * @brief Initializes the PgSQL_Thread object.
	 *
	 * @return `true` if initialization is successful, `false` otherwise.
	 *
	 * @details This function performs the initial setup for the PgSQL_Thread object. It allocates
	 * memory for various data structures, initializes mutexes, creates a pipe for communication,
	 * and configures the thread's variables. It also sets up regular expressions for parsing
	 * certain SQL statements, such as `SET` commands.
	 *
	 * @note This function is called once during the thread's lifetime to prepare it for
	 * handling connections and processing queries.
	 *
	 */
	bool init();

	/**
	 * @brief Retrieves multiple idle connections from the global connection pool.
	 *
	 * @param num_idles A reference to an integer variable that will hold the number of idle connections retrieved.
	 *
	 * @details This function requests multiple idle connections from the global connection pool (`PgHGM`)
	 * and stores them in the `my_idle_conns` array. It then creates new sessions for each retrieved
	 * connection, attaches the connection to the session's backend data stream, and registers the
	 * session as a connection handler. It also sets the connection's status to `PINGING_SERVER`
	 * and initiates the pinging process.
	 *
	 * @note This function is called within the `run()` function to acquire idle connections
	 * from the global pool and prepare them for use by the thread.
	 *
	 */
	void run___get_multiple_idle_connections(int& num_idles);

	/**
	 * @brief Cleans up the mirror queue to manage concurrency.
	 *
	 * @details This function ensures that the number of mirror sessions in the `mirror_queue_mysql_sessions_cache`
	 * array does not exceed the maximum concurrency limit (`pgsql_thread___mirror_max_concurrency`).
	 * It removes sessions from the cache if the limit is exceeded.
	 *
	 * @note This function is called within the `run()` function during a maintenance loop to
	 * control the concurrency of mirror sessions.
	 *
	 */
	void run___cleanup_mirror_queue();

	//void ProcessAllMyDS_BeforePoll();
	//void ProcessAllMyDS_AfterPoll();

	/**
	 * @brief The main loop for the PgSQL_Thread object.
	 *
	 * @details This function implements the main loop for the thread. It handles events, processes
	 * sessions, manages connections, and performs maintenance tasks. It continuously monitors
	 * the `shutdown` flag and exits the loop when it is set to true. The loop includes various
	 * steps such as:
	 *
	 *   - Acquiring idle connections from the global pool.
	 *   - Processing the mirror queue for completed mirror sessions.
	 *   - Calling `ProcessAllMyDS_BeforePoll()` and `ProcessAllMyDS_AfterPoll()` functions to
	 *     handle data stream events before and after the `poll()` call.
	 *   - Adding and removing listeners to the polling loop.
	 *   - Calling `poll()` to wait for events on sockets.
	 *   - Processing all sessions using the `process_all_sessions()` function.
	 *   - Returning unused connections to the global pool.
	 *   - Refreshing the thread's variables.
	 *   - Handling kill requests for connections or queries.
	 *
	 * @note This function is the entry point for the thread's execution. It is responsible for
	 * managing all aspects of the thread's lifecycle, including handling connections, processing
	 * queries, and performing maintenance tasks.
	 *
	 */
	void run();

	/**
	 * @brief Adds a new listener socket to the polling loop.
	 *
	 * @param sock The file descriptor of the listener socket.
	 *
	 * @details This function creates a new `PgSQL_Data_Stream` object for the listener socket,
	 * sets its type to `MYDS_LISTENER`, and adds it to the `mypolls` array for monitoring.
	 *
	 * @note This function is called by the `run()` function when a new listener socket
	 * is added to the thread's monitoring list.
	 *
	 */
	void poll_listener_add(int sock);

	/**
	 * @brief Removes a listener socket from the polling loop.
	 *
	 * @param sock The file descriptor of the listener socket to remove.
	 *
	 * @details This function finds the listener socket in the `mypolls` array based on its file
	 * descriptor and removes it from the array. It then deletes the associated `PgSQL_Data_Stream`
	 * object.
	 *
	 * @note This function is called by the `run()` function when a listener socket is
	 * removed from the thread's monitoring list.
	 *
	 */
	void poll_listener_del(int sock);

	//void register_session(PgSQL_Session*, bool up_start = true);

	/**
	 * @brief Unregisters a session from the thread's session array.
	 *
	 * @param idx The index of the session to unregister.
	 *
	 * @details This function removes a session from the `mysql_sessions` array at the specified index.
	 * It does not delete the session object itself; it is assumed that the caller will handle
	 * the deletion.
	 *
	 * @note This function is called by various parts of the code when a session is no longer
	 * active and needs to be removed from the thread's session list.
	 *
	 */
	void unregister_session(int);
#if POLARDB_PROXY
	/**
	 * @brief Unregister a session identified by pointer.
	 *
	 * Scan `mysql_sessions` linearly for the pointer and remove the first match.
	 * Like the index overload, this never deletes the session object; the caller
	 * remains responsible for that.
	 *
	 * @param sess Session to remove. A null pointer is accepted and reports false.
	 * @return true when the session was found and removed, false when the session
	 *         array is empty, the pointer is null, or the session is not
	 *         registered on this thread.
	 */
	bool unregister_session(PgSQL_Session*);
#endif // POLARDB_PROXY

	/**
	 * @brief Returns a pointer to the `pollfd` structure for a specific data stream.
	 *
	 * @param i The index of the data stream in the `mypolls` array.
	 *
	 * @return A pointer to the `pollfd` structure for the specified data stream.
	 *
	 * @details This function provides access to the `pollfd` structure for a particular data
	 * stream in the `mypolls` array. This structure is used by the `poll()` function to
	 * monitor events on the associated socket.
	 *
	 * @note This function is used internally by the thread to obtain references to the
	 * `pollfd` structures for data streams when interacting with the `poll()` function.
	 *
	 */
	struct pollfd* get_pollfd(unsigned int i);

	/**
	 * @brief Processes data on a specific data stream.
	 *                       
	 * @param myds A pointer to the `PgSQL_Data_Stream` object to process.
	 * @param n The index of the data stream in the `mypolls` array.
	 *
	 * @return `true` if processing is successful, `false` if the session should be removed.
	 *
	 * @details This function handles data events on a specific data stream. It checks for events
	 * such as `POLLIN`, `POLLOUT`, `POLLERR`, and `POLLHUP`. Based on the events, it reads data
	 * from the network, processes packets, and updates the session's status. It also handles
	 * timeout events and connection failures.
	 *
	 * @note This function is called by the `run()` function after the `poll()` call to process
	 * events on each data stream. It is responsible for managing data flow and updating
	 * session states.
	 *
	 */
	bool process_data_on_data_stream(PgSQL_Data_Stream * myds, unsigned int n);

	//void ProcessAllSessions_SortingSessions();

	/**
	 * @brief Processes a completed mirror session and manages its resources.
	 *
	 * @param n The index of the session in the `mysql_sessions` array.
	 * @param sess A pointer to the `PgSQL_Session` object representing the completed mirror session.
	 *
	 * @details This function handles the completion of a mirror session. It removes the completed
	 * session from the `mysql_sessions` array and decrements the `n` index to reflect the removal.
	 * It then checks if the `mirror_queue_mysql_sessions_cache` array is below a certain length
	 * (determined by `pgsql_thread___mirror_max_concurrency` and a scaling factor).
	 * If the cache is not full, the session is added to the cache, otherwise, it is deleted.
	 *
	 * @note This function is called within the `process_all_sessions()` function when a mirror
	 * session reaches the `WAITING_CLIENT_DATA` status, indicating completion.
	 *
	 */
	void ProcessAllSessions_CompletedMirrorSession(unsigned int& n, PgSQL_Session * sess);

	/**
	 * @brief Performs maintenance tasks on a session during a maintenance loop.
	 *
	 * @param sess A pointer to the `PgSQL_Session` object to be maintained.
	 * @param sess_time The idle time of the session in milliseconds.
	 * @param total_active_transactions_ A reference to the total number of active transactions across all threads.
	 *
	 * @details This function performs various maintenance checks on a session during a maintenance
	 * loop. It checks for idle transactions, inactive sessions, and expired connections. It also
	 * handles situations where the server's table version has changed and ensures that sessions
	 * using offline nodes are terminated.
	 *
	 * @note This function is called within the `process_all_sessions()` function during a
	 * maintenance loop. It is responsible for ensuring that sessions are properly managed and
	 * that resources are released when necessary.
	 *
	 */
	void ProcessAllSessions_MaintenanceLoop(PgSQL_Session * sess, unsigned long long sess_time, unsigned int& total_active_transactions_);

	/**
	 * @brief Processes all active sessions associated with the current thread.
	 *
	 * @details This function iterates through all sessions in the `mysql_sessions` array. For each
	 * session, it performs the following actions:
	 *
	 *   - Checks for completed mirror sessions and calls `ProcessAllSessions_CompletedMirrorSession()`
	 *     if necessary.
	 *   - If a maintenance loop is active, it calls `ProcessAllSessions_MaintenanceLoop()` to
	 *     perform maintenance tasks on the session.
	 *   - If the session is healthy and needs processing, it calls the session's `handler()`
	 *     function to handle session logic.
	 *   - If the session is unhealthy, it closes the connection and removes the session from the
	 *     `mysql_sessions` array.
	 *
	 * @note This function is called within the `run()` function of the `PgSQL_Thread` class. It
	 * is the core function responsible for managing and processing all active sessions associated
	 * with the thread.
	 *
	 */
	void process_all_sessions();
#if POLARDB_PROXY
	/**
	 * @brief Open a worker pass on the calling worker thread.
	 *
	 * Mark the pass active and clear the per-pass record of LSNs already stored
	 * into the shared LSN cache (and, in profile builds, the per-pass profile
	 * accumulators). The coalescing done by
	 * polardb_track_server_lsn_update() works only inside an active pass.
	 *
	 * Worker-thread-local; takes no locks. Every path that opens a pass must
	 * close it with polardb_finish_worker_pass().
	 */
	void polardb_begin_worker_pass();
	/**
	 * @brief Close the current worker pass on the calling worker thread.
	 *
	 * Flush the accumulated per-pass profile counters into this worker's counter
	 * array and clear the active flag. A no-op when no pass is open.
	 *
	 * Once this returns, the coalescing is disabled:
	 * polardb_track_server_lsn_update() returns true for every call until
	 * the next polardb_begin_worker_pass().
	 */
	void polardb_finish_worker_pass();
	/**
	 * @brief Return whether an observed server LSN must be stored in the shared
	 *        LSN cache, and record that store.
	 *
	 * This is not a pure predicate. Every call that returns true also inserts or
	 * updates the per-pass cache entry for (server, writer_scope), raising its
	 * max_lsn and stamping last_updated_at_us. Call it exactly once per
	 * intended store: a second call for the same observation returns false and
	 * silently suppresses a store the caller still needed.
	 *
	 * Within a pass the cache suppresses repeat stores of an LSN that is not
	 * higher than one already stored, except that a refresh is allowed once
	 * every POLARDB_LSN_UPDATE_MAX_DELAY_US so freshness timestamps do not
	 * go stale under a steady stream of equal observations.
	 *
	 * @param server        Server the LSN was observed on.
	 * @param lsn           Observed LSN.
	 * @param writer_scope  Writer scope the LSN belongs to.
	 * @param observed_at_us Monotonic time of the observation, in microseconds.
	 * @return true when the caller must store this observation in the shared LSN
	 *         cache. Fails open: true is also returned when no pass is active,
	 *         when server is null, when lsn is 0, when writer_scope is invalid,
	 *         and when the 16-entry pass cache is full.
	 */
	bool polardb_track_server_lsn_update(
		PgSQL_SrvC* server, uint64_t lsn,
		const PolarDB_WriterScope& writer_scope,
		uint64_t observed_at_us);
	#if POLARDB_PROFILE
	void polardb_profile_note_lsn_update(bool advanced);
	void polardb_profile_note_pool_used_count_read(PgSQL_SrvC* server);
	#endif // POLARDB_PROFILE
	/**
	 * @brief Retry the sessions on this worker that are waiting for reader
	 *        capacity.
	 *
	 * Collect every healthy, unkilled session with an active reader-capacity
	 * wait, order it so the session holding this worker's reservation runs
	 * first and the rest run oldest-wait-first, then re-enter each session's
	 * handler() to attempt acquisition again. A scope that reports
	 * READER_GROUP_BUSY blocks the remaining sessions in the same scope for this
	 * pass and registers a pool capacity request. A session whose handler fails
	 * or that becomes killed is unregistered and deleted here.
	 *
	 * Runs on the owning worker thread only.
	 *
	 * @param deadline_due      true when the reader-capacity retry deadline
	 *                          fired. Retries run even without a locally
	 *                          available connection, and the next deadline is set
	 *                          before returning.
	 * @param reservation_ready true when a reservation wake was just consumed, so
	 *                          the session owning the reservation is retried
	 *                          first. When both flags are false, retrying stops
	 *                          as soon as no locally cached reader remains.
	 */
	void polardb_process_reader_capacity_waiters(
		bool deadline_due, bool reservation_ready = false);
	/**
	 * @brief Schedule the reader-capacity retry and make the poll loop wake
	 *        for it.
	 *
	 * A no-op when this worker has no capacity waiters. Otherwise set
	 * polardb_reader_capacity_retry_at_us if no retry is pending, and shorten
	 * this->mypolls.poll_timeout so the worker's poll returns at that deadline.
	 * Call it after poll_timeout has been given its normal value, or the clamp is
	 * overwritten.
	 */
	void polardb_schedule_reader_capacity_retry();
	/**
	 * @brief Register a shared reader pool capacity request for a waiting
	 *        session and record the resulting reservation on this worker.
	 *
	 * A worker may hold at most one reservation at a time, so this rejects the
	 * request outright while another one is outstanding. It also classifies who
	 * already owns a matching reader (see polardb_reader_ownership()) and
	 * declines to queue when the session can be served locally, adopting reader
	 * retention instead. Every ownership check updates its corresponding
	 * diagnostic counter, whether or not a request is registered.
	 *
	 * @param sess Session with an active reader-capacity wait whose last status is
	 *             READER_GROUP_BUSY.
	 */
	void polardb_register_reader_capacity_request(PgSQL_Session* sess);
	/**
	 * @brief Report whether this worker already controls a reader that satisfies
	 *        a waiting session.
	 *
	 * Checked in order: this worker's reservation, the worker-local connection
	 * cache, then readers currently attached to other sessions on this worker.
	 * Meaningful only on the owning worker thread, because all three sources are
	 * worker-local.
	 *
	 * @param waiting_session Session whose reader plan and wait spec the
	 *                        candidates are matched against.
	 * @return RESERVATION when this worker's reservation already holds an
	 *         eligible connection for that session; LOCAL when an eligible reader
	 *         sits in the worker-local cache; ACTIVE when another session on this
	 *         worker is currently using an eligible reader; NONE when nothing
	 *         matches - which also covers a null session and a session with no
	 *         active reader-capacity wait.
	 */
	PolarDB_ReaderOwnership polardb_reader_ownership(
		PgSQL_Session* waiting_session);
	// Wait and retention callers supply their saved request requirements. The
	// common helper checks connection reuse state and current reader eligibility.
	// Retention also requires a worker-local waiter for the same scope.
	bool polardb_reader_connection_matches_wait(
		PgSQL_Connection* conn, PgSQL_Session* waiting_session,
		bool active_connection) const;
	bool polardb_reader_matches_retention(
		PgSQL_Connection* conn) const;
	bool polardb_reader_connection_matches_retention_requirements(
		PgSQL_Connection* conn, bool active_connection) const;
	bool polardb_reader_connection_matches_requirements(
		PgSQL_Connection* conn, bool active_connection,
		uint32_t profile_generation, const PolarDB_PoolKey& pool_key,
		unsigned int hostgroup_id,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec) const;
	void polardb_reader_start_retention(
		uint64_t scope_hash, PgSQL_HGC* hostgroup,
		unsigned int hostgroup_id, uint32_t profile_generation,
		const PolarDB_PoolKey& pool_key,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec);
	void polardb_reader_adopt_retention(PgSQL_Session* sess);
	void polardb_reader_clear_inactive_retention();
	/**
	 * @brief Offer a reader this worker was about to keep to a capacity request
	 *        raised by a different worker.
	 *
	 * The connection is matched against this worker's retention key first, and
	 * unrelated or self-only requests are discarded before the server pool lock
	 * is taken, so the common case costs no shared-state contention.
	 *
	 * @param conn   Connection being returned. Must be idle and reusable to be
	 *               eligible.
	 * @return Reservation disposition. CONNECTION_RESERVED transfers ownership;
	 *         NO_REMOTE_REQUEST keeps it local; other values use normal return.
	 */
	PolarDB_ReaderRemoteReservationResult
		polardb_reader_try_reserve_retained_connection(PgSQL_Connection* conn);
	/**
	 * @brief Select the destination for a reader connection this worker is
	 *        returning.
	 *
	 * Does not move the connection; the caller performs the disposition and each
	 * value obliges a different action.
	 *
	 * @param conn Connection being returned.
	 * @return Destination action plus rejection status. REMOVE_CONNECTION
	 *         carries the classified reason; other actions are not rejections.
	 */
	PolarDB_ReaderLocalReturnDecision polardb_reader_local_return_decision(
		PgSQL_Connection* conn) const;
	/**
	 * @brief Take ownership of a reader connection into the worker-local cache.
	 *
	 * Stamp the connection with the current polardb_reader_cache_epoch, append it
	 * to cached_connections, and update the local reader accounting
	 * (polardb_reader_local_connection_count and
	 * polardb_reader_local_added_this_pass). The caller must not touch the
	 * connection after this returns.
	 *
	 * @param conn Connection to cache. Must not be null.
	 */
	void polardb_store_reader_connection_locally(PgSQL_Connection* conn);
	bool polardb_reader_wait_scope_active(uint64_t scope_hash) const;
	/**
	 * @brief Take the connection held by this worker's reservation for a session.
	 *
	 * The reservation is revalidated against the session's current hostgroup,
	 * pool match key, reader plan and wait spec before it is taken, because the
	 * session's requirements may have moved on since the request was queued. A
	 * reservation that no longer matches is cancelled here.
	 *
	 * On success the reservation slot is cleared, reader retention is started for
	 * the same scope, and ownership of the connection passes to the caller, which
	 * must attach it to the session or return/destroy it.
	 *
	 * @param sess Session taking the reservation. Must be the session the
	 *             reservation was registered for.
	 * @return The reserved connection, now owned by the caller, or nullptr when
	 *         there is no reserved connection, the reservation belongs to a
	 *         different session, the reservation no longer matches the session's
	 *         requirements, or the pool reports the reservation as still pending,
	 *         retired, or missing.
	 */
	PgSQL_Connection* polardb_take_reader_reservation(PgSQL_Session* sess);
	/**
	 * @brief Give up this worker's reader pool reservation.
	 *
	 * Clears the worker-local reservation state, releases the retained server
	 * snapshot, and cancels the request on the pool side - on the server when a
	 * connection had already been assigned, otherwise on the hostgroup. If the
	 * cancellation frees a connection for a queued waiter, that waiter is woken
	 * through PgHGM->polardb_route_reader_reservation_wake().
	 *
	 * @param session_id 0 cancels whatever reservation is active, which is what
	 *                   worker teardown needs. A non-zero value cancels only when
	 *                   the reservation belongs to that session and otherwise
	 *                   does nothing.
	 */
	void polardb_cancel_reader_reservation(uint32_t session_id = 0);
	/**
	 * @brief Queue a "reservation is ready" wake on this worker.
	 *
	 * Called from a foreign thread through
	 * PgSQL_Threads_Handler::polardb_queue_reader_reservation_wake(), which
	 * holds polardb_worker_lifecycle_mutex_ across the call to keep this worker
	 * alive. This method then takes
	 * polardb_reader_pool_reservation_wake_mutex, so the lock order is
	 * lifecycle mutex first, wake mutex second; never take them the other way
	 * round. The first wake of a batch also writes a byte to pipefd[1] to break
	 * the target worker out of poll.
	 *
	 * @param token           Reservation token the wake refers to.
	 * @param server          Server holding the reserved connection.
	 * @param server_snapshot Snapshot keeping `server` alive until the wake is
	 *                        consumed. Must not be empty.
	 * @return true when the wake was queued and ownership of the connection and
	 *         snapshot passed to this worker. false when it was not queued -
	 *         this worker is shutting down, an argument is invalid, or the wake
	 *         pipe write failed - in which case the queued entry is removed again
	 *         and the caller still owns the connection and snapshot and must
	 *         dispose of them.
	 */
	bool polardb_queue_reader_reservation_wake(
		uint64_t token, PgSQL_SrvC* server,
		std::shared_ptr<const void> server_snapshot);
	/**
	 * @brief Drain the queued reservation wakes on the owning worker thread.
	 *
	 * Swaps the wake vector out under
	 * polardb_reader_pool_reservation_wake_mutex and processes it outside the
	 * lock. A wake whose token matches this worker's outstanding reservation
	 * fills in the reserved server and snapshot. Any other wake is not dropped:
	 * its reservation is cancelled and handed on to the next queued waiter.
	 *
	 * Worker destruction must call this before cancelling the reservation, so
	 * that cancellation targets the server the wake actually assigned.
	 *
	 * @return true when this worker's own reservation became ready, meaning a
	 *         connection can now be taken with
	 *         polardb_take_reader_reservation(). false when no wake was
	 *         pending or none matched this worker.
	 */
	bool polardb_process_reader_reservation_wakes();
#endif // POLARDB_PROXY

	/**
	 * @brief Refreshes the thread's variables from the global variables handler.
	 *
	 * @details This function updates the thread's variables with the latest values from the
	 * global variables handler (`GloPTH`) to ensure consistency. It retrieves all relevant
	 * variables from the global handler and updates the corresponding variables in the
	 * thread's local scope.
	 *
	 * @note This function is called periodically by `PgSQL_Thread::run()` to ensure that
	 * the thread's variables are synchronized with the global variables handler.
	 *
	 */
	void refresh_variables();

	/**
	 * @brief Registers a session as a connection handler.
	 *
	 * @param _sess A pointer to the `PgSQL_Session` object to register.
	 * @param _new A boolean flag indicating whether the session is newly created (true) or not (false).
	 *
	 * @details This function marks a session as a connection handler, adding it to the
	 * `mysql_sessions` array. It sets the session's `thread` pointer to the current thread
	 * and sets the `connections_handler` flag to true.
	 *
	 * @note This function is used to track sessions that are responsible for handling
	 * connections.
	 *
	 */
	void register_session_connection_handler(PgSQL_Session * _sess, bool _new = false);

	/**
	 * @brief Unregisters a session as a connection handler.
	 *
	 * @param idx The index of the session in the `mysql_sessions` array.
	 * @param _new A boolean flag indicating whether the session is newly created (true) or not (false).
	 *
	 * @details This function removes a session from the `mysql_sessions` array, effectively
	 * unregistering it as a connection handler.
	 *
	 * @note This function is typically called when a session is no longer active or needs to be
	 * removed from the connection handler list.
	 *
	 */
	void unregister_session_connection_handler(int idx, bool _new = false);

	/**
	 * @brief Handles a new connection accepted by a listener.
	 *
	 * @param myds A pointer to the `PgSQL_Data_Stream` object representing the new connection.
	 * @param n The index of the listener in the `mypolls` array.
	 *
	 * @details This function handles the acceptance of a new connection from a listener. It
	 * accepts the connection using `accept()`, performs some sanity checks, and then creates
	 * a new `PgSQL_Session` object to manage the connection. It configures the session's
	 * data stream, adds the connection to the `mypolls` array, and sets the connection's
	 * state to `CONNECTING_CLIENT`.
	 *
	 * @note This function is called within the `run()` function of the `PgSQL_Thread` class
	 * when a new connection is accepted by a listener. It is responsible for initializing
	 * the session and adding the connection to the polling loop.
	 *
	 */
	void listener_handle_new_connection(PgSQL_Data_Stream * myds, unsigned int n);

	/**
	 * @brief Calculates and updates the memory statistics for the current thread.
	 *
	 * @details This function iterates through all sessions associated with the current
	 * thread and gathers memory usage information from each session. It updates
	 * the `status_variables` structure with the calculated memory statistics,
	 * including the following:
	 *
	 *   - `st_var_mysql_backend_buffers_bytes`: Total bytes used for backend
	 *     connection buffers when fast forwarding is enabled.
	 *   - `st_var_mysql_frontend_buffers_bytes`: Total bytes used for frontend
	 *     connection buffers (read/write buffers and other queues).
	 *   - `st_var_mysql_session_internal_bytes`: Total bytes used for internal
	 *     session data structures.
	 *
	 * @note This function is called by `SQL3_GlobalStatus()` when the `_memory`
	 * flag is set to true.
	 *
	 */
	void Get_Memory_Stats();

	/**
	 * @brief Retrieves a local connection from the thread's cached connection pool.
	 *
	 * @param _hid The hostgroup ID to search for connections in.
	 * @param sess The current session requesting the connection.
	 * @param gtid_uuid The UUID of the GTID to consider (if applicable).
	 * @param gtid_trxid The transaction ID of the GTID to consider (if applicable).
	 * @param max_lag_ms The maximum replication lag allowed for the connection (if applicable).
	 *
	 * @return A pointer to a `PgSQL_Connection` object if a suitable connection is found,
	 * `NULL` otherwise.
	 *
	 * @details This function attempts to find a suitable connection in the thread's
	 * cached connection pool (`cached_connections`). It checks for matching hostgroup
	 * ID, connection options, GTID (if provided), and maximum replication lag (if
	 * provided). If a matching connection is found, it is removed from the cache and
	 * returned.
	 *
	 * @note This function is used by `PgSQL_Session::handler()` to obtain a
	 * connection from the local cache before resorting to the global connection pool.
	 *
	 */
	PgSQL_Connection* get_MyConn_local(unsigned int, PgSQL_Session * sess, char* gtid_uuid, uint64_t gtid_trxid, int max_lag_ms);
#if POLARDB_PROXY
	/**
	 * @brief Return the next round-robin sequence number this worker should use
	 *        when picking a reader in a hostgroup.
	 *
	 * The sequence is kept worker-locally so the common case costs no shared
	 * atomic traffic. Only the first call for a hostgroup takes a starting slot
	 * from the shared *selection_start with a relaxed fetch_add, spreading
	 * workers over different replicas instead of having them all start at 0;
	 * later calls are served purely from the local counter.
	 *
	 * @param hostgroup_id          Hostgroup the reader is being selected from.
	 * @param server_list_generation Current server list generation. When it
	 *                              differs from the cached generation the whole
	 *                              worker-local sequence cache is discarded and
	 *                              every hostgroup starts over.
	 * @param selection_start       Shared starting-slot counter for the
	 *                              hostgroup. May be null, in which case the
	 *                              local sequence starts at 0.
	 * @return A sequence value that increases by one per call for this
	 *         (worker, hostgroup) pair until server_list_generation changes.
	 */
	uint64_t polardb_next_reader_selection_sequence(
		unsigned int hostgroup_id, uint64_t server_list_generation,
		std::atomic<uint64_t>* selection_start);
	/**
	 * @brief Take a matching reader out of this worker's local connection cache.
	 *
	 * A connection matches only when all of these hold: its parent is exactly
	 * `server`, the server is currently ONLINE, its
	 * polardb_startup_profile_generation equals `profile_generation`, and its
	 * polardb_pool_key equals `pool_key`.
	 *
	 * @param server            Server the connection must belong to.
	 * @param profile_generation Startup profile generation the connection must
	 *                          have been created under.
	 * @param pool_key          Pool key the connection must carry.
	 * @return The matching connection, removed from cached_connections and now
	 *         owned by the caller, or nullptr when nothing in the local cache
	 *         satisfies all of the criteria above.
	 */
	PgSQL_Connection* polardb_take_local_reader_connection(
		PgSQL_SrvC* server, uint32_t profile_generation,
		const PolarDB_PoolKey& pool_key);
	/**
	 * @brief Record that one more session on this worker started waiting for
	 *        reader capacity.
	 *
	 * Increments the worker's waiter count and the per-scope waiter refcount, and
	 * sets polardb_reader_capacity_retry_at_us when no retry deadline is pending.
	 *
	 * Every call must be balanced by exactly one
	 * polardb_reader_capacity_wait_finished() for the same scope, or re-keyed
	 * with polardb_reader_capacity_wait_scope_changed(). An unbalanced call leaks
	 * the scope refcount, which keeps reader retention alive and pins a reader to
	 * this worker indefinitely.
	 *
	 * @param scope_hash Routing scope the session is waiting in.
	 */
	void polardb_reader_capacity_wait_started(uint64_t scope_hash);
	void polardb_reader_capacity_wait_scope_changed(
		uint64_t old_scope_hash, uint64_t new_scope_hash);
	void polardb_reader_capacity_wait_finished(uint64_t scope_hash);
	// Shared return paths exclude their donor worker from reservation selection.
	unsigned int get_polardb_worker_index() const {
		return polardb_worker_index;
	}
#endif // POLARDB_PROXY

	/**
	 * @brief Adds a connection to the thread's local connection cache.
	 *
	 * @param c The `PgSQL_Connection` object to hand back. Must not be null.
	 *
	 * @details Ownership of the connection always leaves the caller. Under
	 * POLARDB_PROXY the disposition is selected in this order:
	 *   - any in-flight idle ping on the connection is finished first;
	 *   - polardb_reader_try_reserve_retained_connection() may hand the
	 *     connection to another worker's outstanding capacity request, or mark it
	 *     for local retention;
	 *   - otherwise polardb_reader_local_return_decision() selects:
	 *     KEEP_WITH_WORKER stores it in `cached_connections`, REMOVE_CONNECTION
	 *     destroys it through `PgHGM->destroy_MyConn_from_pool()`, and the shared
	 *     pool values return it with `PgHGM->return_connection_with_match_key()`.
	 * Only a connection that none of those paths took reaches the generic
	 * tail, where a connection is cached locally when its server is online and it
	 * is idle - and when `pgsql_thread___bounded_local_connection_cache` is
	 * enabled only one in every `num_threads` such connections is kept, the rest
	 * going straight to the global pool with `PgHGM->push_MyConn_to_pool()`.
	 *
	 * @note The caller must not touch the connection after this returns: it may
	 * already be owned by another worker, by the global pool, or destroyed.
	 *
	 */
	void push_MyConn_local(PgSQL_Connection*);

	/**
	 * @brief Returns all connections in the thread's local cache to the global pool.
	 *
	 * @details This function returns generic connections after each worker pass.
	 * When PolarDB reader retention is enabled, reader connections used in the
	 * latest pass remain local and are returned after one pass without reuse.
	 *
	 * @note This function is called periodically by `PgSQL_Thread::run()` to
	 * ensure that unused connections are returned to the global pool.
	 *
	 */
	void return_local_connections();
	/**
	 * @brief Return every local connection during worker shutdown.
	 *
	 * PolarDB reader retention and remote handoff are skipped. All retained
	 * readers go back to the shared pool.
	 */
	void return_local_connections_for_shutdown();

	/**
     * Pseudocode (plan)
     * - For each pending connection-termination request (conn_ids):
     *   - If request.thread_id != sess.thread_session_id: continue.
     *   - If request.username is present AND session has a connected frontend user:
     *     - If request.username == session.frontend_user:
     *       - Log info, mark session as killed.
     *   - Erase the processed request from the queue.
     *   - Break (at most one matching request per session).
     * - Return sess->killed (true if this function marked it or it was already marked).
     */

	/**
	 * @brief Handle session termination requests targeting the given session.
	 *
	 * @details
	 * Scans the provided connection-termination queue for a request matching the
	 * specified session. If a matching request is found and the request carries a
	 * username that matches the authenticated frontend user of the session, the
	 * session is marked for termination (sess->killed = true) 
	 *
	 * @param sess The session potentially targeted for termination.
	 * @return true if the session is (now) marked as killed, false otherwise.
	 */
	bool Scan_Sessions_to_Kill___handle_session_termination(PgSQL_Session* sess);

	/**
	 * - Iterate over query_ids:
	 *   - If request.thread_id != sess->thread_session_id: continue
	 *   - Determine authorization method:
	 *     - If request.username is null: authenticate via secret_key == sess->cancel_secret_key
	 *     - Else: authorize via username == session frontend user
	 *   - If authorized and session has a backend server_myds:
	 *     - Log the request
	 *     - Set server_myds->wait_until = curtime to wake processing loop
	 *     - Set server_myds->kill_type = 1 to request backend cancellation
	 *     - Mark canceled_query = true
	 *   - Erase the processed request from query_ids
	 *   - Break (process at most one request per call)
	 * - Return canceled_query
	*/

	/**
	 * @brief Handle query cancellation requests for the given session.
	 *
	 * @param sess       Target session that may have a running query to cancel.
	 * @return true if a cancellation was scheduled for this session; false otherwise.
	 */
	bool Scan_Sessions_to_Kill___handle_query_cancellation(PgSQL_Session* sess);

	/**
	 * @brief Iterates through a session array to identify and kill sessions.
	 *
	 * @param mysess A pointer to the `PtrArray` containing the sessions to scan.
	 *
	 * @details This function iterates through the specified session array and checks
	 * each session against the thread's kill queues (`sess_intrpt_queue.conn_ids` and
	 * `sess_intrpt_queue.query_ids`). If a session matches a kill request, its `killed` flag is set
	 * to true. The kill queues are then updated to remove the processed kill
	 * requests.
	 *
	 * @note This function is called by `Scan_Sessions_to_Kill_All()` to kill
	 * sessions based on kill requests.
	 *
	 */
	void Scan_Sessions_to_Kill(PtrArray * mysess);

	/**
	 * @brief  Scans all session arrays across all threads to identify and kill sessions.
	 *
	 * @details This function iterates through all session arrays across different threads, including main worker threads and idle threads.
	 * It calls `Scan_Sessions_to_Kill()` for each session array to check for kill requests.
	 * The kill queues (`sess_intrpt_queue.conn_ids` and `sess_intrpt_queue.query_ids`) are cleared after processing all kill requests.
	 *
	 * @note This function is called by `PgSQL_Threads_Handler::kill_connection_or_query()` to kill sessions based on kill requests.
	 *
	 */
	void Scan_Sessions_to_Kill_All();

private:
	void return_local_connections_impl(bool shutdown);
};

#if POLARDB_PROXY
#ifndef POLARDB_THREAD_COUNTERS
#define POLARDB_THREAD_COUNTERS 1
#endif

/**
 * @brief Add to a PolarDB counter, preferring the calling worker's private slot.
 *
 * When `thread` is non-null the increment is a plain non-atomic += on that
 * thread's counter array, so `thread` must be the calling worker. Passing
 * another worker's PgSQL_Thread* is a data race; code that runs on a foreign
 * thread must pass nullptr instead, as
 * polardb_queue_reader_reservation_wake() does.
 *
 * @param thread         Calling worker, or nullptr to count on the global atomic.
 * @param idx            Counter to update.
 * @param global_counter Global atomic used when `thread` is nullptr; it also
 *                       receives the worker slot at thread teardown.
 * @param value          Amount to add.
 */
static inline void polardb_thread_count(
	PgSQL_Thread* thread,
	PolarDB_ThreadStatusVariable idx,
	std::atomic<unsigned long long>& global_counter,
	unsigned long long value = 1)
{
	if (thread) {
		thread->polardb_status_variables.stvar[idx] += value;
	} else {
		global_counter.fetch_add(value, std::memory_order_relaxed);
	}
}

/**
 * @brief Raise a PolarDB counter to a running maximum, preferring the calling
 *        worker's private slot.
 *
 * Same thread-affinity rule as polardb_thread_count(): with a non-null `thread`
 * this is a non-atomic read-modify-write of that worker's slot, so `thread` must
 * be the calling worker and any other thread must pass nullptr.
 *
 * The stored value is a maximum, not a sum. The counter must also be listed in
 * POLARDB_THREAD_MAX_COUNTER_LIST, otherwise the teardown fold and
 * PgSQL_Threads_Handler::get_polardb_counter() add the per-worker values instead
 * of taking their maximum. Only the POLARDB_THREAD_MAX macro checks that
 * registration; this function does not.
 *
 * @param thread         Calling worker, or nullptr to update the global atomic.
 * @param idx            Counter to update.
 * @param global_counter Global atomic used when `thread` is nullptr.
 * @param value          Candidate maximum; ignored when it is not larger than
 *                       the value already stored.
 */
static inline void polardb_thread_raise_max(
	PgSQL_Thread* thread,
	PolarDB_ThreadStatusVariable idx,
	std::atomic<unsigned long long>& global_counter,
	unsigned long long value)
{
	if (thread) {
		unsigned long long& current =
			thread->polardb_status_variables.stvar[idx];
		if (value > current) {
			current = value;
		}
		return;
	}
	unsigned long long current = global_counter.load(std::memory_order_relaxed);
	while (current < value && !global_counter.compare_exchange_weak(
			current, value, std::memory_order_relaxed,
			std::memory_order_relaxed)) {
	}
}

#if POLARDB_THREAD_COUNTERS
#define POLARDB_THREAD_COUNT(thread, name, value) \
	polardb_thread_count((thread), polardb_st_var_##name, \
		PgHGM->status.polardb_##name, (value))

#define POLARDB_THREAD_COUNT_ONE(thread, name) \
	POLARDB_THREAD_COUNT((thread), name, 1)

#define POLARDB_THREAD_MAX(thread, name, value) \
	do { \
		static_assert(polardb_thread_counter_aggregates_max( \
			polardb_st_var_##name), \
			"Register POLARDB_THREAD_MAX counters in the max-counter list"); \
		polardb_thread_raise_max((thread), polardb_st_var_##name, \
			PgHGM->status.polardb_##name, (value)); \
	} while (0)
#else
#define POLARDB_THREAD_COUNT(thread, name, value) do { } while (0)
#define POLARDB_THREAD_COUNT_ONE(thread, name) do { } while (0)
#define POLARDB_THREAD_MAX(thread, name, value) do { } while (0)
#endif // POLARDB_THREAD_COUNTERS

void polardb_count_lsn_wait_elapsed_bucket(
	PgSQL_Thread* thread,
	unsigned long long elapsed_us,
	bool transaction_split);
/**
 * @brief Bucket how far a selected reader's LSN is behind the target LSN.
 *
 * Exactly one counter is bumped per call. A zero target_lsn or reader_lsn is
 * classified as 'lsn_unknown' and a reader LSN that is not fresh as 'lsn_stale';
 * in both cases no gap bucket is recorded at all. A reader that is at or ahead
 * of the target is recorded in the zero-gap bucket, never as a negative gap.
 *
 * @param thread           Calling worker, or nullptr (see polardb_thread_count()).
 * @param target_lsn       LSN the request needs the reader to have reached.
 * @param reader_lsn       LSN observed on the selected reader.
 * @param reader_lsn_fresh Whether that observation is still considered current.
 */
void polardb_count_reader_target_lsn_gap_bucket(
	PgSQL_Thread* thread,
	uint64_t target_lsn,
	uint64_t reader_lsn,
	bool reader_lsn_fresh);
/**
 * @brief Record the quality of one LSN-targeted reader selection.
 *
 * Emits the target/gap buckets itself by calling
 * polardb_count_reader_target_lsn_gap_bucket(); a caller that also calls that
 * helper for the same selection double-counts the histogram. On top of that it
 * accumulates the selected gap, and compares the selected reader against the
 * best reader that was considered - those comparison counters are emitted only
 * when both LSNs are non-zero and fresh.
 *
 * A complete no-op when target_lsn is 0.
 *
 * @param thread                          Calling worker, or nullptr.
 * @param target_lsn                      LSN the request needs.
 * @param selected_reader_lsn             LSN of the reader that was chosen.
 * @param selected_reader_lsn_fresh       Whether that observation is current.
 * @param best_considered_reader_lsn      Highest LSN among the candidates seen.
 * @param best_considered_reader_lsn_fresh Whether that observation is current.
 */
void polardb_count_reader_target_selection(
	PgSQL_Thread* thread,
	uint64_t target_lsn,
	uint64_t selected_reader_lsn,
	bool selected_reader_lsn_fresh,
	uint64_t best_considered_reader_lsn,
	bool best_considered_reader_lsn_fresh);

#if POLARDB_PROFILE
void polardb_count_wait_profile_completion(
	PgSQL_Thread* thread,
	const PolarDB_WaitProfileState& state,
	unsigned long long elapsed_us);
#endif // POLARDB_PROFILE

#if POLARDB_PROFILE
#define POLARDB_PROFILE_THREAD_COUNT(thread, name, value) \
	POLARDB_THREAD_COUNT((thread), name, (value))
#define POLARDB_PROFILE_THREAD_COUNT_ONE(thread, name) \
	POLARDB_PROFILE_THREAD_COUNT((thread), name, 1)
#else
#define POLARDB_PROFILE_THREAD_COUNT(thread, name, value) do { } while (0)
#define POLARDB_PROFILE_THREAD_COUNT_ONE(thread, name) do { } while (0)
#endif // POLARDB_PROFILE

#if POLARDB_PERF_DEBUG
#define POLARDB_PERF_THREAD_COUNT(thread, name, value) \
	POLARDB_THREAD_COUNT((thread), name, (value))
#define POLARDB_PERF_THREAD_COUNT_ONE(thread, name) \
	POLARDB_PERF_THREAD_COUNT((thread), name, 1)
#else
#define POLARDB_PERF_THREAD_COUNT(thread, name, value) do { } while (0)
#define POLARDB_PERF_THREAD_COUNT_ONE(thread, name) do { } while (0)
#endif // POLARDB_PERF_DEBUG

#endif // POLARDB_PROXY


typedef PgSQL_Thread* create_PgSQL_Thread_t();
typedef void destroy_PgSQL_Thread_t(PgSQL_Thread*);

class PgSQL_Listeners_Manager {
private:
	PtrArray* ifaces;
public:
	PgSQL_Listeners_Manager();
	~PgSQL_Listeners_Manager();
	int add(const char* iface, unsigned int num_threads, int** perthrsocks);
	int find_idx(const char* iface);
	int find_idx(const char* address, int port);
	iface_info* find_iface_from_fd(int fd);
	int get_fd(unsigned int idx);
	void del(unsigned int idx);
};

/*struct p_th_counter {
	enum metric {
		queries_backends_bytes_sent = 0,
		queries_backends_bytes_recv,
		queries_frontends_bytes_sent,
		queries_frontends_bytes_recv,
		query_processor_time_nsec,
		backend_query_time_nsec,
		com_backend_stmt_prepare,
		com_backend_stmt_execute,
		com_backend_stmt_close,
		com_frontend_stmt_prepare,
		com_frontend_stmt_execute,
		com_frontend_stmt_close,
		questions,
		slow_queries,
		gtid_consistent_queries,
		gtid_session_collected,
		connpool_get_conn_latency_awareness,
		connpool_get_conn_immediate,
		connpool_get_conn_success,
		connpool_get_conn_failure,
		generated_error_packets,
		max_connect_timeouts,
		backend_lagging_during_query,
		backend_offline_during_query,
		queries_with_max_lag_ms,
		queries_with_max_lag_ms__delayed,
		queries_with_max_lag_ms__total_wait_time_us,
		mysql_unexpected_frontend_com_quit,
		hostgroup_locked_set_cmds,
		hostgroup_locked_queries,
		mysql_unexpected_frontend_packets,
		aws_aurora_replicas_skipped_during_query,
		automatic_detected_sql_injection,
		mysql_whitelisted_sqli_fingerprint,
		mysql_killed_backend_connections,
		mysql_killed_backend_queries,
		client_host_error_killed_connections,
		__size
	};
};

struct p_th_gauge {
	enum metric {
		active_transactions = 0,
		client_connections_non_idle,
		client_connections_hostgroup_locked,
		mysql_backend_buffers_bytes,
		mysql_frontend_buffers_bytes,
		mysql_session_internal_bytes,
		mirror_concurrency,
		mirror_queue_lengths,
		mysql_thread_workers,
		// global_variables
		mysql_wait_timeout,
		mysql_max_connections,
		mysql_monitor_enabled,
		mysql_monitor_ping_interval,
		mysql_monitor_ping_timeout,
		mysql_monitor_ping_max_failures,
		mysql_monitor_aws_rds_topology_discovery_interval,
		mysql_monitor_read_only_interval,
		mysql_monitor_read_only_timeout,
		mysql_monitor_writer_is_also_reader,
		mysql_monitor_replication_lag_group_by_host,
		mysql_monitor_replication_lag_interval,
		mysql_monitor_replication_lag_timeout,
		mysql_monitor_history,
		__size
	};
};

struct th_metrics_map_idx {
	enum index {
		counters = 0,
		gauges
	};
};
*/
/**
 * @brief Structure holding the data for a Client_Host_Cache entry.
 */
typedef struct _PgSQL_Client_Host_Cache_Entry {
	/**
	 * @brief Last time the entry was updated.
	 */
	uint64_t updated_at;
	/**
	 * @brief Error count associated with the entry.
	 */
	uint32_t error_count;
} PgSQL_Client_Host_Cache_Entry;

#if POLARDB_PROXY
/**
 * @brief Outcome of PgSQL_Threads_Handler::polardb_attach_worker().
 */
enum class PolarDB_WorkerAttachResult : uint8_t {
	/// The worker owns the slot and can receive reservation notifications. It
	/// must be released again with polardb_detach_worker().
	ATTACHED,
	/// The handler is shutting down. The worker must abandon startup rather than
	/// retry.
	SHUTDOWN_STARTED,
	/// The attach was rejected: the worker pointer is null, the thread array does
	/// not exist, the index is out of range, or - the case a retrying caller must
	/// detect - the slot is already occupied by a different worker.
	INVALID_SLOT
};
#endif // POLARDB_PROXY

class PgSQL_Threads_Handler
{
private:
	int shutdown_;
	size_t stacksize;
	pthread_attr_t attr;
	pthread_rwlock_t rwlock;
	PtrArray* bind_fds;
	PgSQL_Listeners_Manager* MLM;
	// VariablesPointers_int stores:
	// key: variable name
	// tuple:
	//   variable address
	//   min value
	//   max value
	//   special variable : if true, min and max values are ignored, and further input validation is required
	std::unordered_map<std::string, std::tuple<int*, int, int, bool>> VariablesPointers_int;
	// VariablesPointers_bool stores:
	// key: variable name
	// tuple:
	//   variable address
	//   special variable : if true, further input validation is required
	std::unordered_map<std::string, std::tuple<bool*, bool>> VariablesPointers_bool;
#if POLARDB_PROXY
	friend struct PolarDB_WorkerLifecycleUnitAccess;
	// Keeps reservation notification from outliving its target worker.
	std::mutex polardb_worker_lifecycle_mutex_;
	PolarDB_ParsedGlobalConfigValue polardb_global_config_;
	std::atomic<uint64_t> polardb_startup_config_generation_{1};
	/**
	 * @brief Parse the current `variables.polardb_*` strings into a config value.
	 *
	 * Reads the raw variable strings directly, so the caller must hold the
	 * handler rwlock for writing (commit() does; the constructor relies on being
	 * single-threaded).
	 *
	 * @param startup_generation Generation to stamp into config.startup. The
	 *        method only stores what it is given - it never bumps the generation.
	 *        Detecting a startup-input change with
	 *        polardb_same_startup_config_inputs() and incrementing the generation
	 *        is the caller's job; a caller that skips it stores a stale
	 *        generation.
	 * @return The parsed configuration value, not yet stored into
	 *         polardb_global_config_.
	 */
	PolarDB_ParsedGlobalConfigValue polardb_build_global_config_unlocked(
		uint64_t startup_generation) const;
#endif // POLARDB_PROXY
	/**
	 * @brief Holds the clients host cache. It keeps track of the number of
	 *   errors associated to a specific client:
	 *     - Key: client identifier, based on 'clientaddr'.
	 *     - Value: Structure of type 'PgSQL_Client_Host_Cache_Entry' holding
	 *       the last time the entry was updated and the error count associated
	 *       with the client.
	 */
	std::unordered_map<std::string, PgSQL_Client_Host_Cache_Entry> client_host_cache;
	/**
	 * @brief Holds the mutex for accessing 'client_host_cache', since every
	 *   access can potentially perform 'read/write' operations, a regular mutex
	 *   is enough.
	 */
	pthread_mutex_t mutex_client_host_cache;

public:
	struct {
		int authentication_method;
		int monitor_history;
		int monitor_connect_interval;
		int monitor_connect_interval_window;
		int monitor_connect_timeout;
		//! Monitor ping interval. Unit: 'ms'.
		int monitor_ping_interval;
		int monitor_ping_interval_window;
		int monitor_ping_max_failures;
		//! Monitor ping timeout. Unit: 'ms'.
		int monitor_ping_timeout;
		//! Monitor aws rds topology discovery interval. Unit: 'one discovery check per X monitor_read_only checks'.
		int monitor_aws_rds_topology_discovery_interval;
		//! Monitor read only timeout. Unit: 'ms'.
		int monitor_read_only_interval;
		int monitor_read_only_interval_window;
		//! Monitor read only timeout. Unit: 'ms'.
		int monitor_read_only_timeout;
		int monitor_read_only_max_timeout_count;
		bool monitor_enabled;
		//! ProxySQL session wait timeout. Unit: 'ms'.
		bool monitor_wait_timeout;
		bool monitor_writer_is_also_reader;
		bool monitor_replication_lag_group_by_host;
		//! How frequently a replication lag check is performed. Unit: 'ms'.
		int monitor_replication_lag_interval;
		int monitor_replication_lag_interval_window;
		//! Read only check timeout. Unit: 'ms'.
		int monitor_replication_lag_timeout;
		int monitor_replication_lag_count;
/* TODO: Remove
		int monitor_groupreplication_healthcheck_interval;
		int monitor_groupreplication_healthcheck_timeout;
		int monitor_groupreplication_healthcheck_max_timeout_count;
		int monitor_groupreplication_max_transactions_behind_count;
		int monitor_groupreplication_max_transactions_behind_for_read_only;
		int monitor_galera_healthcheck_interval;
		int monitor_galera_healthcheck_timeout;
		int monitor_galera_healthcheck_max_timeout_count;
		int monitor_query_interval;
		int monitor_query_timeout;
		int monitor_slave_lag_when_null;
*/
		int monitor_threads;
/* TODO: Remove
		int monitor_threads_min;
		int monitor_threads_max;
		int monitor_threads_queue_maxsize;
*/
		int monitor_local_dns_cache_ttl;
		int monitor_local_dns_cache_refresh_interval;
		int monitor_local_dns_resolver_queue_maxsize;
		char* monitor_username;
		char* monitor_password;
		char* monitor_dbname;
		char* monitor_replication_lag_use_percona_heartbeat;
		int ping_interval_server_msec;
		int ping_timeout_server;
		int shun_on_failures;
		int shun_recovery_time_sec;
		int unshun_algorithm;
		int query_retries_on_failure;
		bool connection_warming;
		// 0 keeps the ProxySQL 3.0.7 worker-local cache behavior.
		// 1 enables the ProxySQL 3.0.9 bounded 1-in-pgsql_threads behavior.
		int bounded_local_connection_cache;
		int client_host_cache_size;
		int client_host_error_counts;
		int connect_retries_on_failure;
		int connect_retries_delay;
		int connection_delay_multiplex_ms;
		int connection_max_age_ms;
		int connect_timeout_client;
		int connect_timeout_server;
		int connect_timeout_server_max;
		int free_connections_pct;
#ifdef IDLE_THREADS
		int session_idle_ms;
#endif // IDLE_THREADS
		bool sessions_sort;
		char* default_schema;
		char* interfaces;
		char* keep_multiplexing_variables;
		//unsigned int default_charset; // removed in 2.0.13 . Obsoleted previously using PgSQL_Variables instead
		int handle_unknown_charset;
		bool servers_stats;
		bool commands_stats;
		bool query_digests;
		bool query_digests_lowercase;
		bool query_digests_replace_null;
		bool query_digests_no_digits;
		bool query_digests_normalize_digest_text;
		bool query_digests_track_hostname;
		bool query_digests_keep_comment;
		int query_digests_grouping_limit;
		int query_digests_groups_grouping_limit;
		bool parse_failure_logs_digest;
		bool default_reconnect;
		bool have_compress;
		bool have_ssl;
		bool multiplexing;
		//		bool stmt_multiplexing;
		bool preserve_client_on_broken_backend_in_tx;
		bool log_unhealthy_connections;
		bool enforce_autocommit_on_reads;
		bool autocommit_false_not_reusable;
		bool autocommit_false_is_transaction;
		bool verbose_query_error;
		int max_allowed_packet;
#if POLARDB_PROXY
		// PolarDB LSN session-consistency knobs. Policy actions are word-valued
		// and name their final client-visible outcome.
		char* polardb_profile;                // named preset or custom
		char* polardb_consistency_mode;       // off | eventual | session_lsn | global_lsn
		char* polardb_read_target;            // primary | replica
		char* polardb_action_read_fallback;   // primary | error
		int polardb_max_reader_lsn_gap_bytes;    // reader lag cap in bytes; 0=off
		int polardb_max_reader_lag_ms;            // reserved; only 0 is accepted until a real lag-time producer exists
		int polardb_lsn_wait_timeout_ms;          // polar_xact_split_wait_lsn timeout; 0=wait indefinitely
		int polardb_reader_lsn_max_age_ms;        // maximum age of a cached per-server LSN
		int polardb_lag_cap_freshness_ms;     // max LSN-cache age under byte-lag cap + finite wait; 0=wait-fraction only
		int polardb_reader_lsn_lag_range_bytes; // 0 keeps exact best-behind reader choice
		bool polardb_reader_prefer_freshest_below_target; // experimental two-reader policy; balanced selection remains the default
		bool polardb_reader_prefer_less_loaded; // experimental two-reader strict freshness/load dominance
		int polardb_reader_connection_retention; // 0 returns after each pass; 1 retains active readers
		int polardb_output_coalesce_bytes;   // 0 disables incomplete streaming output coalescing by bytes
		int polardb_output_coalesce_packets; // 0 disables incomplete streaming output coalescing by packets
		bool polardb_monitor_lsn_updates;     // enable monitor LSN cache updates
		bool polardb_lazy_warmup_split;       // demand-warm connected split-reader pool entries
		bool polardb_writev_direct;           // plaintext frontend direct scatter/gather send path
		bool polardb_result_fast_forward;     // batch contiguous backend DataRow frames into one result packet
		int polardb_split_warmup_max_connections_per_request; // max backend connections per warmup request
		char* polardb_action_lsn_timeout;      // warning | primary | error | disconnect
		char* polardb_proxy_protocol;         // v15 | legacy | off
		char* polardb_action_missing_lsn;      // primary | warning | error
		char* polardb_action_replica_loss;     // replica_then_primary | replica_then_error | primary | error | disconnect
		char* polardb_action_replica_error;    // primary | error | disconnect
		char* polardb_proxy_identity_mode;    // client | proxy
		char* polardb_proxy_identity_host;    // empty or IP literal
		int polardb_proxy_identity_port;      // 0..65535
#endif // POLARDB_PROXY
		bool automatic_detect_sqli;
		bool firewall_whitelist_enabled;
		bool use_tcp_keepalive;
		int tcp_keepalive_time;
		int throttle_connections_per_sec_to_hostgroup;
		int max_transaction_idle_time;
		int max_transaction_time;
		int threshold_query_length;
		int threshold_resultset_size;
		int query_digests_max_digest_length;
		int query_digests_max_query_length;
		int query_rules_fast_routing_algorithm;
		int wait_timeout;
		int throttle_max_bytes_per_second_to_client;
		int throttle_ratio_server_to_client;
		int max_connections;
		int max_stmts_per_connection;
		int max_stmts_cache;
		int mirror_max_concurrency;
		int mirror_max_queue_length;
		int default_max_latency_ms;
		int default_query_delay;
		int default_query_timeout;
		int query_processor_iterations;
		/**
		 * @brief Defines when the first comment of a query needs to be processed.
		 * 0 : comment ignored
		 * 1 : comment processed before the query rules
		 * 2 : comment processed after the query rules (default behavior)
		 * 3 : comment processed before and after the query rules
		 */
		int query_processor_first_comment_parsing;
		int query_processor_regex;
		int query_processor_parser;
		int set_query_lock_on_hostgroup;
		int set_parser_algorithm;
		int auto_increment_delay_multiplex;
		int auto_increment_delay_multiplex_timeout_ms;
		int long_query_time;
		int hostgroup_manager_verbose;
		int binlog_reader_connect_retry_msec;
		char* init_connect;
		char* ldap_user_variable;
		char* add_ldap_user_comment;
		char* default_variables[PGSQL_NAME_LAST_LOW_WM];
		char* firewall_whitelist_errormsg;
#ifdef DEBUG
		bool session_debug;
#endif /* DEBUG */
		int poll_timeout;
		int poll_timeout_on_failure;
		char* eventslog_filename;
		int eventslog_filesize;
		/** @brief Circular buffer size for PostgreSQL advanced events logging. */
		int eventslog_buffer_history_size;
		/** @brief Maximum rows retained in stats_pgsql_query_events in-memory table. */
		int eventslog_table_memory_size;
		/** @brief Maximum query length copied into PostgreSQL eventslog circular buffer. */
		int eventslog_buffer_max_query_length;
		int eventslog_default_log;
		int eventslog_format;
		int eventslog_flush_timeout;
 		int eventslog_flush_size;
 		int eventslog_rate_limit;
		char* auditlog_filename;
		int auditlog_filesize;
		int auditlog_flush_timeout;
 		int auditlog_flush_size;
		// SSL related, proxy to server
		char* ssl_p2s_ca;
		char* ssl_p2s_capath;
		char* ssl_p2s_cert;
		char* ssl_p2s_key;
		char* ssl_p2s_cipher;
		char* ssl_p2s_crl;
		char* ssl_p2s_crlpath;
		int query_cache_size_MB;
		int query_cache_soft_ttl_pct;
		int query_cache_handle_warnings;
		int min_num_servers_lantency_awareness;
		int aurora_max_lag_ms_only_read_from_replicas;
		bool stats_time_backend_query;
		bool stats_time_query_processor;
		bool query_cache_stores_empty_result;
		bool kill_backend_connection_when_disconnect;
		int data_packets_history_size;
		char* server_version;
		char* server_encoding;
		/**
		 *   The processlist variables are logically group under "pgsql-" variables
		 *   and they are kept under PgSQL_Threads_Handler.
		 *
		 *   Other than configuration load/save or sync activities, these variables
		 *   are not utilized by PTH or PgSQL_Thread for any other purpose and hence
		 *   they are not associated with thread-local variables.
		 *
		 *   At runtime, ProxySQL_Admin keeps a copy of these variables and uses them
		 *   when collecting stats for stats_pgsql_processlist.
		 */
#ifdef IDLE_THREADS
		bool session_idle_show_processlist;
#endif
		int show_processlist_extended;
		int processlist_max_query_length;
#ifdef PROXYSQLFFTO
		bool ffto_enabled;
		int ffto_max_buffer_size;
#endif
	} variables;
	struct {
		unsigned int mirror_sessions_current;
		int threads_initialized = 0;
		/// Prometheus metrics arrays
		//std::array<prometheus::Counter*, p_th_counter::__size> p_counter_array{};
		//std::array<prometheus::Gauge*, p_th_gauge::__size> p_gauge_array{};
	} status_variables;

	std::atomic<bool> bootstrapping_listeners;

	/**
	 * @brief Update the client host cache with the supplied 'client_sockaddr',
	 *   and the supplied 'error' parameter specifying if there was a connection
	 *   error or not.
	 *
	 *   NOTE: This function is not safe, the supplied 'client_sockaddr' should
	 *   have been initialized by 'accept' or 'getpeername'. NULL checks are not
	 *   performed.
	 *
	 * @details The 'client_sockaddr' parameter is inspected, and the
	 *   'client_host_cache' map is only updated in case of:
	 *    - 'address_family' is either 'AF_INET' or 'AF_INET6'.
	 *    - The address obtained from it isn't '127.0.0.1'.
	 *
	 *   In case 'client_sockaddr' matches the previous description, the update
	 *   of the client host cache is performed in the following way:
	 *     1. If the cache is full, the oldest element in the cache is searched.
	 *     In case the oldest element address doesn't match the supplied
	 *     address, the oldest element is removed.
	 *     2. The cache is searched looking for the supplied address, in case of
	 *     being found, the entry is updated, otherwise the entry is inserted in
	 *     the cache.
	 *
	 * @param client_sockaddr A 'sockaddr' holding the required client information
	 *   to update the 'client_host_cache_map'.
	 * @param error 'true' if there was an error in the connection that should be
	 *   register, 'false' otherwise.
	 */
	void update_client_host_cache(struct sockaddr* client_sockaddr, bool error);
	/**
	 * @brief Retrieves the entry of the underlying 'client_host_cache' map for
	 *   the supplied 'client_sockaddr' in case of existing. In case it doesn't
	 *   exist or the supplied 'client_sockaddr' doesn't met the requirements
	 *   for being registered in the map, and zeroed 'PgSQL_Client_Host_Cache_Entry'
	 *   is returned.
	 *
	 *   NOTE: This function is not safe, the supplied 'client_sockaddr' should
	 *   have been initialized by 'accept' or 'getpeername'. NULL checks are not
	 *   performed.
	 *
	 * @details The 'client_sockaddr' parameter is inspected, and the
	 *   'client_host_cache' map is only searched in case of:
	 *    - 'address_family' is either 'AF_INET' or 'AF_INET6'.
	 *    - The address obtained from it isn't '127.0.0.1'.
	 *
	 * @param client_sockaddr A 'sockaddr' holding the required client information
	 *   to update the 'client_host_cache_map'.
	 * @return If found, the corresponding entry for the supplied 'client_sockaddr',
	 *   a zeroed 'PgSQL_Client_Host_Cache_Entry' otherwise.
	 */
	PgSQL_Client_Host_Cache_Entry find_client_host_cache(struct sockaddr* client_sockaddr);
	/**
	 * @brief Delete all the entries in the 'client_host_cache' internal map.
	 */
	void flush_client_host_cache();
	/**
	 * @brief Returns the current entries of 'client_host_cache' in a
	 *   'SQLite3_result'. In case the param 'reset' is specified, the structure
	 *   is cleaned after being queried.
	 *
	 * @param reset If 'true' the entries of the internal structure
	 *   'client_host_cache' will be cleaned after scrapping.
	 *
	 * @return SQLite3_result holding the current entries of the
	 *   'client_host_cache'. In the following format:
	 *
	 *    [ 'client_address', 'error_num', 'last_updated' ]
	 *
	 *    Where 'last_updated' is the last updated time expressed in 'ns'.
	 */
	SQLite3_result* get_client_host_cache(bool reset);
	/**
	 * @brief Callback to update the metrics.
	 */
	void p_update_metrics();
	unsigned int num_threads;
	proxysql_pgsql_thread_t* pgsql_threads;
#ifdef IDLE_THREADS
	proxysql_pgsql_thread_t* pgsql_threads_idles;
#endif // IDLE_THREADS
	/**
	 * @brief Returns the current global version number for thread variables.
	 *
	 * @return The current global version number.
	 *
	 * @details This function retrieves the current global version number for thread variables.
	 * This number is incremented whenever a thread variable is changed, allowing threads to
	 * detect changes and refresh their local variables accordingly.
	 *
	 * @note This function is used by threads to check for changes in global variables and
	 * to update their local copies if necessary.
	 *
	 */
	unsigned int get_global_version();

	/**
	 * @brief Acquires a write lock on the thread variables.
	 *
	 * @details This function acquires a write lock on the thread variables using a read-write lock.
	 * This lock prevents other threads from modifying the variables while the lock is held.
	 *
	 * @note This function should be called before modifying any thread variables to ensure
	 * data consistency.
	 *
	 */
	void wrlock();

	/** Acquire a shared lock on the thread variables. */
	void rdlock();

	/**
	 * @brief Releases a write lock on the thread variables.
	 *
	 * @details This function releases the write lock on the thread variables that was previously
	 * acquired using `wrlock()`. After calling this function, other threads can modify the
	 * variables.
	 *
	 * @note This function should be called after modifying thread variables to release the
	 * lock and allow other threads to access the variables.
	 *
	 */
	void wrunlock();

	/** Releases a shared lock acquired with `rdlock()`. */
	void rdunlock();

	/**
	 * @brief Commits changes to thread variables and increments the global version.
	 *
	 * @details This function increments the global version number for thread variables, signaling
	 * to other threads that changes have been made. It also updates the global variables
	 * handler (`GloPTH`) with the committed changes.
	 *
	 * @note This function should be called after modifying thread variables to ensure that
	 * other threads are notified of the changes and can update their local copies.
	 *
	 */
	void commit();

#if POLARDB_PROXY
	/** Copy the canonical parsed PolarDB values while the caller holds rwlock. */
	PolarDB_ParsedGlobalConfigValue get_polardb_global_config_unlocked() const;

	/**
	 * @brief Replace every profile-owned raw setting from one validated bundle.
	 *
	 * The caller must hold wrlock(). This changes the candidate variables only;
	 * call commit() afterwards to publish them to workers.
	 */
	void apply_polardb_global_config_unlocked(
		const PolarDB_ParsedGlobalConfigValue& config);

	/**
	 * @brief Copy the parsed PolarDB values for cold callers.
	 *
	 * Takes the handler rwlock in read mode around the copy. The caller must not
	 * already hold that lock in either mode - doing so self-deadlocks. A caller
	 * that is already inside rdlock()/wrlock() must use
	 * get_polardb_global_config_unlocked() instead.
	 *
	 * @return A snapshot of the configuration values stored by commit().
	 */
	PolarDB_ParsedGlobalConfigValue get_polardb_global_config();

	/**
	 * @brief Read the startup configuration generation.
	 *
	 * Lock-free acquire load. commit() increments the counter only when a
	 * startup-affecting input actually changes - proxy protocol dialect, proxy
	 * identity mode, identity host or identity port - not on every commit. That
	 * makes the value usable as a cache-invalidation stamp for anything derived
	 * from the startup identity.
	 *
	 * @return The current startup configuration generation.
	 */
	uint64_t get_polardb_startup_config_generation() const;
#endif // POLARDB_PROXY

	/**
	 * @brief Retrieves the value of a thread variable as a string.
	 *
	 * @param name The name of the variable to retrieve.
	 *
	 * @return A pointer to a string containing the value of the variable, or `NULL` if
	 * the variable is not found.
	 *
	 * @details This function retrieves the value of a thread variable as a string. It first
	 * checks for monitor-related variables, then for SSL variables, and finally for default
	 * variables. If the variable is found, its value is returned as a dynamically allocated
	 * string. Otherwise, `NULL` is returned.
	 *
	 * @note This function is used to access the values of thread variables from other parts
	 * of the code.
	 *
	 */
	char* get_variable(char* name);

	/**
	 * @brief Sets the value of a thread variable.
	 *
	 * @param name The name of the variable to set.
	 * @param value The new value to assign to the variable.
	 *
	 * @return `true` if the variable is set successfully, `false` otherwise.
	 *
	 * @details This function sets the value of a thread variable. It first checks for monitor,
	 * SSL, and default variables. If the variable is found, it updates the variable's value
	 * with the provided string. For integer variables, it performs range validation. For
	 * boolean variables, it checks for valid "true" or "false" values. For some variables,
	 * it performs additional input validation. If the variable is not found or the provided
	 * value is invalid, `false` is returned.
	 *
	 * @note This function is used to modify the values of thread variables from other parts
	 * of the code.
	 *
	 */
	bool set_variable(char* name, const char* value);

	/**
	 * @brief Returns a list of all available thread variables.
	 *
	 * @return A dynamically allocated array of strings containing the names of all thread
	 * variables, or `NULL` if there are no variables.
	 *
	 * @details This function retrieves a list of all available thread variables. It scans both
	 * the `pgsql_thread_variables_names` array and the `mysql_tracked_variables` array to
	 * include both PgSQL-specific and MySQL-related variables. The returned list is dynamically
	 * allocated and should be freed by the caller.
	 *
	 * @note This function is used to obtain a list of available thread variables for
	 * display or other purposes.
	 *
	 */
	char** get_variables_list();

	/**
	 * @brief Checks if a thread variable exists.
	 *
	 * @param name The name of the variable to check.
	 *
	 * @return `true` if the variable exists, `false` otherwise.
	 *
	 * @details This function checks if a thread variable exists. It scans both the
	 * `pgsql_thread_variables_names` array and the `mysql_tracked_variables` array to
	 * determine if the variable is defined.
	 *
	 * @note This function is used to check for the existence of thread variables before
	 * attempting to access or modify them.
	 *
	 */
	bool has_variable(const char* name);

	/**
	 * @brief Default constructor for the PgSQL_Threads_Handler class.
	 *
	 * @details This constructor initializes various members of the PgSQL_Threads_Handler object
	 * to their default values. It sets up mutexes, initializes variables, and creates a
	 * `PgSQL_Listeners_Manager` object. It also sets the `bootstrapping_listeners` flag to
	 * `true` to indicate that the listener bootstrapping process is ongoing.
	 *
	 * @note This constructor is called when a new PgSQL_Threads_Handler object is created.
	 *
	 */
	PgSQL_Threads_Handler();

	/**
	 * @brief Destructor for the PgSQL_Threads_Handler class.
	 *
	 * @details This destructor cleans up the PgSQL_Threads_Handler object, releasing resources
	 * and freeing allocated memory. It frees dynamically allocated strings, deletes the
	 * `PgSQL_Listeners_Manager` object, and destroys mutexes.
	 *
	 * @note This destructor is called automatically when the PgSQL_Threads_Handler object
	 * goes out of scope or is explicitly deleted.
	 *
	 */
	~PgSQL_Threads_Handler();

	/**
	 * @brief Retrieves the value of a thread variable as a string.
	 *
	 * @param name The name of the variable to retrieve.
	 *
	 * @return A pointer to a string containing the value of the variable, or `NULL` if
	 * the variable is not found.
	 *
	 * @details This function retrieves the value of a thread variable as a string. It checks
	 * if the variable exists and then returns its value as a dynamically allocated string.
	 * If the variable is not found, it returns `NULL`.
	 *
	 * @note This function is used internally by the `get_variable()` function to retrieve
	 * the value of a variable as a string.
	 */
	char* get_variable_string(char* name);

	/**
	 * @brief Retrieves the value of a thread variable as an integer.
	 *
	 * @param name The name of the variable to retrieve.
	 *
	 * @return The value of the variable as an integer, or 0 if the variable is not found
	 * or its value is not a valid integer.
	 *
	 * @details This function retrieves the value of a thread variable as an integer. It checks
	 * if the variable exists and then converts its value to an integer. If the variable is
	 * not found or its value is not a valid integer, it returns 0.
	 *
	 * @note This function is used internally by the `get_variable()` function to retrieve
	 * the value of a variable as an integer.
	 */
	int get_variable_int(const char* name);

	/**
	 * @brief Prints the current version of the PgSQL_Threads_Handler class.
	 *
	 * @details This function prints the current version of the PgSQL_Threads_Handler class
	 * to the standard error stream.
	 *
	 * @note This function is used for debugging and informational purposes.
	 */
	void print_version();

	/**
	 * @brief Initializes the PgSQL_Threads_Handler object.
	 *
	 * @param num The number of threads to create.
	 * @param stack The stack size for each thread.
	 *
	 * @details This function initializes the PgSQL_Threads_Handler object, creating the
	 * specified number of threads with the given stack size. It also initializes the
	 * global variables handler (`GloPTH`) and sets up the thread pool.
	 *
	 * @note This function is called once during the PgSQL_Threads_Handler object's
	 * lifetime to prepare it for managing threads.
	 */
	void init(unsigned int num = 0, size_t stack = 0);

	/**
	 * @brief Creates a new thread.
	 *
	 * @param tn The thread number.
	 * @param start_routine The start routine for the thread.
	 * @param epoll_thread A boolean flag indicating whether the thread is an epoll thread (true)
	 * or a worker thread (false).
	 *
	 * @return A pointer to the newly created thread object, or `NULL` if the thread creation
	 * failed.
	 *
	 * @details This function creates a new thread with the specified thread number, start routine,
	 * and thread type. It initializes the thread object, sets up the thread's variables, and
	 * starts the thread's execution.
	 *
	 * @note This function is used to create new threads for the PgSQL_Threads_Handler object.
	 *
	 */
	proxysql_pgsql_thread_t* create_thread(unsigned int tn, void* (*start_routine) (void*), bool);

	/**
	 * @brief Shuts down all threads in the thread pool.
	 *
	 * @details This function shuts down all threads in the thread pool, gracefully terminating
	 * their execution. It sets the `shutdown` flag to `true` for each thread, allowing them
	 * to exit their main loop. It then waits for all threads to terminate and frees any
	 * associated resources.
	 *
	 * @note This function is called when the PgSQL_Threads_Handler object is being shut down
	 * to gracefully terminate all managed threads.
	 *
	 */
	void shutdown_threads();
#if POLARDB_PROXY
	/**
	 * Registers a worker for reservation notifications while distinguishing normal
	 * shutdown from an invalid or already occupied worker slot.
	 *
	 * Acquires polardb_worker_lifecycle_mutex_ internally, so the caller must not
	 * already hold it - in particular this cannot be called from under
	 * shutdown_threads()'s lock. On ATTACHED it also stamps
	 * worker->polardb_worker_index, which every reservation and donor-exclusion
	 * path relies on, and the attach must later be paired with
	 * polardb_detach_worker().
	 *
	 * @param worker_index Slot to use.
	 * @param worker       Worker assigned to the slot. Must not be null.
	 * @return See PolarDB_WorkerAttachResult.
	 */
	PolarDB_WorkerAttachResult polardb_attach_worker(
		unsigned int worker_index, PgSQL_Thread* worker);
	/**
	 * @brief Release a worker's reservation notification slot.
	 *
	 * Acquires polardb_worker_lifecycle_mutex_, so the caller must not already
	 * hold it. Because the slot is cleared under that mutex, a true return also
	 * guarantees that no polardb_queue_reader_reservation_wake() call is still
	 * inside this worker - that guarantee is the reason the mutex exists.
	 * worker->polardb_worker_index is deliberately left set.
	 *
	 * @param worker_index Slot to release.
	 * @param worker       Worker that owns the slot.
	 * @return true when the slot still held exactly this worker and was cleared;
	 *         false when the worker is null, the thread array is missing, the
	 *         index is out of range, or the slot holds someone else.
	 */
	bool polardb_detach_worker(
		unsigned int worker_index, PgSQL_Thread* worker);
	/**
	 * @brief Deliver a reader pool reservation wake to another worker.
	 *
	 * Holds polardb_worker_lifecycle_mutex_ across the call into
	 * worker->polardb_queue_reader_reservation_wake(). That is what keeps the
	 * target worker alive for the duration of the notification, and it fixes the
	 * lock order as lifecycle mutex first, then the target's
	 * polardb_reader_pool_reservation_wake_mutex.
	 *
	 * @param worker_index    Worker to wake.
	 * @param token           Reservation token the wake refers to.
	 * @param server          Server holding the reserved connection.
	 * @param server_snapshot Snapshot keeping `server` alive until the target
	 *                        consumes the wake.
	 * @return true when the wake was queued on the target worker, which then owns
	 *         the connection and snapshot. false when it was not delivered -
	 *         shutdown has started, the index is out of range, the slot is empty,
	 *         or the target rejected the wake - and the caller still owns the
	 *         connection and the snapshot.
	 */
	bool polardb_queue_reader_reservation_wake(
		unsigned int worker_index, uint64_t token, PgSQL_SrvC* server,
		std::shared_ptr<const void> server_snapshot);
#endif // POLARDB_PROXY

	/**
	 * @brief Adds a new listener to the thread pool, based on an interface string.
	 *
	 * @param iface The interface string in the format "address:port" or "[ipv6_address]:port".
	 *
	 * @return 0 on success, -1 on failure.
	 *
	 * @details This function adds a new listener to the thread pool based on the provided
	 * interface string. It delegates the actual listener creation to the `PgSQL_Listeners_Manager`
	 * object (`MLM`). If the listener is successfully added, it signals all threads in the pool
	 * to update their polling lists.
	 *
	 * @note This function is used to configure listeners for the PgSQL_Threads_Handler object.
	 */
	int listener_add(const char* iface);

	/**
	 * @brief Adds a new listener to the thread pool, based on an address and port.
	 *
	 * @param address The address of the listener.
	 * @param port The port of the listener.
	 *
	 * @return 0 on success, -1 on failure.
	 *
	 * @details This function adds a new listener to the thread pool based on the provided
	 * address and port. It delegates the actual listener creation to the `PgSQL_Listeners_Manager`
	 * object (`MLM`). If the listener is successfully added, it signals all threads in the pool
	 * to update their polling lists.
	 *
	 * @note This function is used to configure listeners for the PgSQL_Threads_Handler object.
	 */
	int listener_add(const char* address, int port);

	/**
	 * @brief Removes a listener from the thread pool, based on an interface string.
	 *
	 * @param iface The interface string in the format "address:port" or "[ipv6_address]:port".
	 *
	 * @return 0 on success, -1 on failure.
	 *
	 * @details This function removes a listener from the thread pool based on the provided
	 * interface string. It delegates the actual listener removal to the `PgSQL_Listeners_Manager`
	 * object (`MLM`). If the listener is successfully removed, it signals all threads in the pool
	 * to update their polling lists.
	 *
	 * @note This function is used to remove listeners from the PgSQL_Threads_Handler object.
	 */
	int listener_del(const char* iface);

	/**
	 * @brief Removes a listener from the thread pool, based on an address and port.
	 *
	 * @param address The address of the listener to remove.
	 * @param port The port of the listener to remove.
	 *
	 * @return 0 on success, -1 on failure.
	 *
	 * @details This function removes a listener from the thread pool based on the provided
	 * address and port. It delegates the actual listener removal to the `PgSQL_Listeners_Manager`
	 * object (`MLM`). If the listener is successfully removed, it signals all threads in the pool
	 * to update their polling lists.
	 *
	 * @note This function is used to remove listeners from the PgSQL_Threads_Handler object.
	 */
	int listener_del(const char* address, int port);

	/**
	 * @brief Starts all configured listeners in the thread pool.
	 *
	 * @details This function starts all listeners that have been configured for the
	 * PgSQL_Threads_Handler object. It parses the `interfaces` variable, which contains
	 * a list of interface strings, and calls `listener_add()` to add each listener
	 * to the pool. After all listeners have been added, it sets the `bootstrapping_listeners`
	 * flag to `false` to indicate that the listener bootstrapping process is complete.
	 *
	 * @note This function is called to initiate the listening process for the
	 * PgSQL_Threads_Handler object.
	 */
	void start_listeners();

	/**
	 * @brief Stops all listeners in the thread pool.
	 *
	 * @details This function stops all listeners that have been configured for the
	 * PgSQL_Threads_Handler object. It parses the `interfaces` variable, which contains
	 * a list of interface strings, and calls `listener_del()` to remove each listener
	 * from the pool.
	 *
	 * @note This function is called to terminate the listening process for the
	 * PgSQL_Threads_Handler object.
	 */
	void stop_listeners();

	/**
	 * @brief Signals all threads in the thread pool.
	 *
	 * @param _c The signal value to send to each thread.
	 *
	 * @details This function sends a signal to all threads in the thread pool. It iterates
	 * through the thread pool and writes the signal value to the pipe associated with each
	 * thread.
	 *
	 * @note This function is used to send signals to threads for various purposes, such as
	 * notifying them of changes in global variables, requesting a thread to perform a specific
	 * task, or signaling a shutdown event.
	 *
	 */
	void signal_all_threads(unsigned char _c = 0);

	/**
	 * @brief Retrieves a process list for all threads in the thread pool.
	 *
	 * @param args
	 *   Processlist rendering options and optional typed query controls.
	 *   When `args.query_options.enabled=true`, filtering/sorting/pagination is
	 *   applied in memory after the live snapshot is collected.
	 *
	 * @return A `SQLite3_result` object containing the process list, or `NULL` if an error
	 * occurred.
	 *
	 * @details This function retrieves a process list for all threads in the thread pool. It
	 * iterates through the thread pool and gathers information about each active session.
	 * The information is then formatted into a `SQLite3_result` object, which can be used
	 * by the SQLite3 engine to return the process list to the client.
	 *
	 * @note This function is used to provide a process list view for the PgSQL_Threads_Handler
	 * object, allowing administrators to monitor active sessions and their status.
	 *
	 */
	SQLite3_result* SQL3_Processlist(processlist_config_t args);

	/**
	 * @brief Retrieves global status information for the thread pool.
	 *
	 * @param _memory A boolean flag indicating whether to include memory statistics in the
	 * global status information.
	 *
	 * @return A `SQLite3_result` object containing the global status information, or `NULL`
	 * if an error occurred.
	 *
	 * @details This function retrieves global status information for the thread pool, including
	 * metrics such as uptime, active transactions, connections, and queries. If the `_memory`
	 * flag is set to `true`, it also includes memory statistics for each thread.
	 *
	 * @note This function is used to provide a global status view for the PgSQL_Threads_Handler
	 * object, allowing administrators to monitor the overall health and performance of the
	 * thread pool.
	 *
	 */
	SQLite3_result* SQL3_GlobalStatus(bool _memory);

	/**
	 * @brief Kills a session based on its thread session ID.
	 *
	 * @param _thread_session_id The thread session ID of the session to kill.
	 *
	 * @return `true` if the session is found and killed, `false` otherwise.
	 *
	 * @details This function attempts to find and kill a session based on its thread session ID.
	 * It iterates through all threads in the thread pool and searches for a session with the
	 * matching ID. If the session is found, its `killed` flag is set to `true`, indicating that
	 * the session should be terminated.
	 *
	 * @note This function is used to terminate a specific session by its thread session ID.
	 *
	 */
	bool kill_session(uint32_t _thread_session_id);

	/**
	 * @brief Retrieves the total length of the mirror queue across all threads.
	 *
	 * @return The total length of the mirror queue.
	 *
	 * @details This function retrieves the total length of the mirror queue across all threads.
	 * It iterates through the thread pool and sums the length of the mirror queue for each
	 * thread.
	 *
	 * @note This function is used to monitor the size of the mirror queue, which is used
	 * to queue mirror sessions for processing.
	 *
	 */
	unsigned long long get_total_mirror_queue();

	//unsigned long long get_status_variable(enum PgSQL_Thread_status_variable v_idx, p_th_counter::metric m_idx, unsigned long long conv = 0);
	//unsigned long long get_status_variable(enum PgSQL_Thread_status_variable v_idx, p_th_gauge::metric m_idx, unsigned long long conv = 0);

	/**
	 * @brief Retrieves the total number of active transactions across all threads.
	 *
	 * @return The total number of active transactions.
	 *
	 * @details This function retrieves the total number of active transactions across all
	 * threads in the thread pool. It iterates through the thread pool and sums the number
	 * of active transactions for each thread.
	 *
	 * @note This function is used to monitor the number of active transactions, which is
	 * a key performance indicator for the PgSQL_Threads_Handler object.
	 *
	 */
	unsigned int get_active_transations();

	// Aggregated per-worker counters across all PgSQL threads.
	unsigned long long get_pgconnpoll_get();
	unsigned long long get_pgconnpoll_get_ok();
	unsigned long long get_pgconnpoll_push();
	unsigned long long get_tx_poisoned_total();
	unsigned long long get_tx_poisoned_recovered_total();
	unsigned long long get_tx_poisoned_rejected_statements_total();

#if POLARDB_PROXY
	/**
	 * @brief Aggregate one PolarDB counter across the global atomic and every
	 *        live worker.
	 *
	 * Acquires polardb_worker_lifecycle_mutex_ for the whole scan, which keeps
	 * the worker slots valid, so this must not be called from a context that
	 * already holds that mutex.
	 *
	 * The aggregation depends on the counter's registration: counters listed in
	 * POLARDB_THREAD_MAX_COUNTER_LIST are combined with max, all others with sum.
	 * A caller that assumes summation misreports max-style counters.
	 *
	 * The result is approximate: per-worker slots are read with relaxed loads
	 * while their owners keep writing them, and before threads are initialized or
	 * once shutdown has started only the global atomic is returned.
	 *
	 * @param idx            Counter to aggregate.
	 * @param global_counter Global atomic holding the folded contributions of
	 *                       already-destroyed workers.
	 * @return The aggregated value.
	 */
	unsigned long long get_polardb_counter(
		PolarDB_ThreadStatusVariable idx,
		std::atomic<unsigned long long>& global_counter);
#endif // POLARDB_PROXY

#ifdef IDLE_THREADS
	/**
	 * @brief Retrieves the number of non-idle client connections across all threads.
	 *
	 * @return The number of non-idle client connections.
	 *
	 * @details This function retrieves the number of non-idle client connections across all
	 * threads in the thread pool. It iterates through the thread pool and sums the number
	 * of non-idle client connections for each thread.
	 *
	 * @note This function is used to monitor the number of active client connections, which
	 * is a key performance indicator for the PgSQL_Threads_Handler object.
	 *
	 */
	unsigned int get_non_idle_client_connections();
#endif // IDLE_THREADS

	/**
	 * @brief Retrieves the total number of bytes used for backend connection buffers across
	 * all threads.
	 *
	 * @return The total number of bytes used for backend connection buffers.
	 *
	 * @details This function retrieves the total number of bytes used for backend connection
	 * buffers across all threads in the thread pool. It iterates through the thread pool and
	 * sums the number of bytes used for backend connection buffers for each thread.
	 *
	 * @note This function is used to monitor the memory usage of backend connection buffers,
	 * which is a key performance indicator for the PgSQL_Threads_Handler object.
	 *
	 */
	unsigned long long get_pgsql_backend_buffers_bytes();

	/**
	 * @brief Retrieves the total number of bytes used for frontend connection buffers across
	 * all threads.
	 *
	 * @return The total number of bytes used for frontend connection buffers.
	 *
	 * @details This function retrieves the total number of bytes used for frontend connection
	 * buffers across all threads in the thread pool. It iterates through the thread pool and
	 * sums the number of bytes used for frontend connection buffers for each thread.
	 *
	 * @note This function is used to monitor the memory usage of frontend connection buffers,
	 * which is a key performance indicator for the PgSQL_Threads_Handler object.
	 *
	 */
	unsigned long long get_pgsql_frontend_buffers_bytes();

	/**
	 * @brief Retrieves the total number of bytes used for internal session data structures
	 * across all threads.
	 *
	 * @return The total number of bytes used for internal session data structures.
	 *
	 * @details This function retrieves the total number of bytes used for internal session
	 * data structures across all threads in the thread pool. It iterates through the thread pool
	 * and sums the number of bytes used for internal session data structures for each thread.
	 *
	 * @note This function is used to monitor the memory usage of internal session data
	 * structures, which is a key performance indicator for the PgSQL_Threads_Handler object.
	 *
	 */
	unsigned long long get_pgsql_session_internal_bytes();

	iface_info* MLM_find_iface_from_fd(int fd) {
		return MLM->find_iface_from_fd(fd);
	}

	/**
	 * @brief Calculates and updates the memory statistics for all threads in the pool.
	 *
	 * @details This function iterates through all threads in the thread pool and calls
	 * the `Get_Memory_Stats()` function for each thread to calculate and update its
	 * memory statistics. 
	 *
	 * @note This function is used to gather memory statistics for all threads in the
	 * pool, providing a comprehensive view of memory usage.
	 *
	 */
	void Get_Memory_Stats();

	/**
	 * @brief Sends a kill request to all threads in the pool to either kill a connection
	 * or a query.
	 *
	 * @param _thread_session_id The thread session ID of the connection or query to kill.
	 * @param query A boolean flag indicating whether to kill a query (true) or a connection
	 * (false).
	 * @param username The username associated with the connection or query.
	 *
	 * @details This function sends a kill request to all threads in the pool to either kill
	 * a connection or a query. It adds the kill request to the kill queue (`sess_intrpt_queue.conn_ids` or
	 * `sess_intrpt_queue.query_ids`) for each thread and then signals all threads to process the kill queue.
	 *
	 * @note This function is used to terminate a specific connection or query by its thread
	 * session ID.
	 *
	 */
	void kill_connection_or_query(uint32_t sess_thd_id, uint32_t secret_key, const char* username, bool query);
};
	
	
#endif /* PROXYSQL_PGSQL_THREAD_H */
