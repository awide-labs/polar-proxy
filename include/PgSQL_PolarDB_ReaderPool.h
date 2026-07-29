#ifndef PROXYSQL_PGSQL_POLARDB_READER_POOL_H
#define PROXYSQL_PGSQL_POLARDB_READER_POOL_H

#include "PgSQL_PolarDB.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

class PgSQL_HostGroups_Manager;
class PgSQL_Connection;
class PgSQL_SrvC;
class PgSQL_Session;
class PgSQL_Thread;
struct PgSQL_PoolMatchKey;

#if POLARDB_PROXY
/**
 * @brief Demand signal for lazy transaction-split replica-pool warmup.
 *
 * Split reads require an already-pooled replica connection. When the pool is
 * empty, the session queues this key and the reader pool opens a connected
 * backend before adding it to the free list. The request carries the
 * startup client context captured from the session that requested warmup.
 */
struct PgSQL_SplitWarmupRequest {
	unsigned int hostgroup_id = 0;
	std::string username;
	std::string password;
	std::string dbname;
	PolarDB_StartupClientContext startup_client;
	std::vector<std::string> startup_parameters;
	std::vector<uint32_t> startup_parameter_hash;
	uint64_t startup_options_hash = 0;
	bool has_startup_parameters = false;
	int startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
	uint64_t startup_config_generation = 0;
	unsigned int max_connections_per_request = 1;
	unsigned long long requested_at_us = 0;
	std::string target_address;
	uint16_t target_port = 0;
	unsigned int target_required_free_count = 0;

	PgSQL_SplitWarmupRequest() = default;
	PgSQL_SplitWarmupRequest(
		unsigned int hg,
		const char* user,
		const char* pass,
		const char* db,
		const PolarDB_StartupClientContext& client_context,
		unsigned long long now_us,
		unsigned int max_connections)
		: hostgroup_id(hg)
		, username(user ? user : "")
		, password(pass ? pass : "")
		, dbname(db ? db : "")
		, startup_client(client_context)
		, max_connections_per_request(max_connections)
		, requested_at_us(now_us) {}

	bool has_target_server() const {
		return !target_address.empty() || target_port != 0;
	}
};

static inline bool pgsql_split_warmup_count_connect_failure(
			bool connected, bool stopped_by_shutdown) {
	return !connected && !stopped_by_shutdown;
}

static constexpr size_t PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT = 16;
static constexpr unsigned int PGSQL_POLARDB_SPLIT_WARMUP_DEFAULT_MAX_CONNECTIONS_PER_REQUEST = 1;
static constexpr unsigned int PGSQL_POLARDB_SPLIT_WARMUP_MAX_CONNECTIONS_PER_REQUEST_LIMIT = 64;
static constexpr bool PGSQL_POLARDB_TXN_READER_ONLY_POOLED = true;

static inline bool pgsql_split_warmup_batch_has_capacity(
		size_t pending_count) {
	return pending_count < PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT;
}

static inline unsigned int pgsql_split_warmup_max_connections_per_request_from_int(
		int configured) {
	if (configured <= 0) {
		return PGSQL_POLARDB_SPLIT_WARMUP_DEFAULT_MAX_CONNECTIONS_PER_REQUEST;
	}
	if (configured > static_cast<int>(PGSQL_POLARDB_SPLIT_WARMUP_MAX_CONNECTIONS_PER_REQUEST_LIMIT)) {
		return PGSQL_POLARDB_SPLIT_WARMUP_MAX_CONNECTIONS_PER_REQUEST_LIMIT;
	}
	return static_cast<unsigned int>(configured);
}

/**
 * PolarDB reader policy and warmup coordinator (ReaderPool v2).
 *
 * ReaderPool no longer owns a second set of connections. It selects a server
 * from the current topology plus global status, LSN, weight and active count.
 * Only after selection does the PostgreSQL pool move a matching connection
 * from FREE to USED. Ordinary reads stay on the selected server. Pooled-only
 * reads may use another otherwise eligible server when the selected server has
 * no exact shared-pool match, because those reads cannot reset or create.
 *
 * Taking or returning a matching connection uses one short per-server mutex.
 * The HGM write lock is used only for topology or status changes and creating
 * a connection. Creation checks the selected server again and cannot choose
 * another server.
 * Warmup adds matching connections to the same core FREE list.
 */
class PgSQL_PolarDB_ReaderPool {
public:
	explicit PgSQL_PolarDB_ReaderPool(PgSQL_HostGroups_Manager* hgm);
	~PgSQL_PolarDB_ReaderPool();

	void start();
	void shutdown();
	void request_split_warmup(unsigned int reader_hostgroup_id,
		const char* username, const char* password, const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn);
	void warm_split_pools();
	void refresh_split_warmup_variables();
	bool connection_match_key_for_return(PgSQL_Connection* conn,
		PgSQL_PoolMatchKey* match_key);
	PolarDB_ReaderLocalReturn local_return_decision(
		PgSQL_Connection* conn);
	PolarDB_ReaderResult get_MyConn_polardb_reader(unsigned int hid,
		PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address = nullptr, int exclude_port = -1);

private:
#ifdef POLARDB_UNIT_FULL_HARNESS
	friend size_t pgsql_polardb_unit_collect_split_warmup_targets(
		PgSQL_PolarDB_ReaderPool* pool,
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free);
#endif // POLARDB_UNIT_FULL_HARNESS
	void split_warmup_thread_run();
	bool drain_split_warmup_requests(
		std::vector<PgSQL_SplitWarmupRequest>& requests);
	void clear_split_warmup_inflight_key(const std::string& key);
	bool register_split_warmup_inflight_key(const std::string& key);
	bool requeue_split_warmup_request(const PgSQL_SplitWarmupRequest& req);
	void polardb_collect_split_warmup_targets_locked(
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free);

	PgSQL_HostGroups_Manager* hgm_ = nullptr;
	std::queue<PgSQL_SplitWarmupRequest> split_warmup_queue_;
	std::unordered_set<std::string> split_warmup_queued_;
	std::unordered_set<std::string> split_warmup_inflight_;
	std::mutex split_warmup_mutex_;
	std::condition_variable split_warmup_cv_;
	std::thread* split_warmup_thread_ = nullptr;
	std::atomic<bool> split_warmup_shutdown_{false};
	std::atomic<unsigned long long> split_warmup_next_candidate_{0};
};
#endif // POLARDB_PROXY

#endif // PROXYSQL_PGSQL_POLARDB_READER_POOL_H
