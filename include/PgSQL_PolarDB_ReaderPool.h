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

/**
 * Four-part connection match key used by the PostgreSQL pool.
 *
 * The caller fills the four values. The pool only compares them.
 */
struct PgSQL_PoolMatchKey {
	uint64_t words[4]{0, 0, 0, 0};

	bool empty() const {
		return words[0] == 0 && words[1] == 0 &&
			words[2] == 0 && words[3] == 0;
	}

	bool operator==(const PgSQL_PoolMatchKey& other) const {
		return words[0] == other.words[0] &&
			words[1] == other.words[1] &&
			words[2] == other.words[2] &&
			words[3] == other.words[3];
	}
};

#if POLARDB_PROXY
static inline PgSQL_PoolMatchKey pgsql_pool_match_key(
		uint32_t profile_generation, const PolarDB_PoolKey& pool_key) {
	PgSQL_PoolMatchKey key;
	key.words[0] = profile_generation;
	key.words[1] = pool_key.auth_hash;
	key.words[2] = pool_key.startup_identity_hash;
	key.words[3] = pool_key.startup_options_hash;
	return key;
}
#endif // POLARDB_PROXY

struct PgSQL_PoolMatchKeyHash {
	size_t operator()(const PgSQL_PoolMatchKey& key) const {
		uint64_t hash = 1469598103934665603ULL;
		for (uint64_t word : key.words) {
			hash ^= word;
			hash *= 1099511628211ULL;
		}
		return static_cast<size_t>(hash);
	}
};

#if POLARDB_PROXY
/**
 * @brief Demand signal for lazy transaction-split replica-pool warmup.
 *
 * Split reads require an already-pooled replica connection. When the session
 * cannot get one, it queues this key and the reader pool opens a connected
 * backend before adding it to the free list. Demand warmup fires both when no
 * reader has a compatible free connection and when the routing-selected reader
 * misses in the shared pool, which happens even while other readers still hold
 * free connections. The request carries the startup client context captured
 * from the session that requested warmup.
 *
 * A request may pin one endpoint: target_address, target_port and
 * target_required_free_count name the reader to warm and how many compatible
 * free connections it should end up with, and target_server_list_generation
 * records the server-list generation the target was chosen under. A pinned
 * request is dropped when that generation has moved on, because the endpoint
 * may no longer be part of the hostgroup.
 *
 * Requests are deduplicated on the identity fields together with
 * startup_identity_mode, startup_config_generation, the startup-parameter hash
 * and the pinned target, so a configuration change does not merge with requests
 * captured under the previous one.
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
	uint64_t target_server_list_generation = 0;

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

static inline bool pgsql_split_warmup_should_count_connect_failure(
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

static inline unsigned int polardb_split_warmup_connection_limit(
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
 * ReaderPool does not own a second set of connections. It selects a server
 * from the current topology plus global status, LSN, weight and active count.
 * Only after selection does the PostgreSQL pool move a matching connection
 * from FREE to USED.
 *
 * Selection prefers the chosen server but does not guarantee it. An ordinary
 * read is also served from an alternate reader in two cases: when the readers
 * are interchangeable for this request and the selected reader's pool mutex is
 * contended, so waiting on it would cost more than using an equivalent reader;
 * and when a group-capacity check is running and walks the remaining readers
 * before reporting the whole group busy. A pooled-only read may likewise use
 * another otherwise eligible server when the selected server has no exact
 * shared-pool match, because those reads cannot reset or create. Only creating
 * a connection is pinned to the selected server: it rechecks that server and
 * cannot fall back to another one.
 *
 * Taking or returning a matching connection uses one short per-server mutex.
 * The HGM write lock is used only for topology or status changes and creating
 * a connection.
 * Warmup adds matching connections to the same core FREE list.
 */
class PgSQL_PolarDB_ReaderPool {
public:
	explicit PgSQL_PolarDB_ReaderPool(PgSQL_HostGroups_Manager* hgm);
	~PgSQL_PolarDB_ReaderPool();

	void start();
	void shutdown();
	/**
	 * @brief Queue a warmup request for the transaction-split reader pool.
	 *
	 * The call is asynchronous and never blocks on a backend: it builds a dedup
	 * key, appends the request to the warmup queue and wakes the warmup thread,
	 * which does the connecting. It returns nothing, and an ineligible or
	 * duplicate request is dropped silently after bumping a counter. Requests
	 * are dropped when lazy warmup is disabled, when PolarDB is inactive, when
	 * the username or startup identity is unusable, when the hostgroup's startup
	 * profile cannot request an RFQ LSN, when an identical request is already
	 * queued or in flight, and when the queue is full.
	 *
	 * @param reader_hostgroup_id Reader hostgroup to warm.
	 * @param username Backend user the pooled connection must carry. A null or
	 *        empty user makes the request ineligible.
	 * @param password Password used when opening the backend socket. It is not
	 *        part of the dedup key.
	 * @param dbname Database the pooled connection must carry (may be null).
	 * @param startup_client Startup client context captured from the requesting
	 *        session; it must be valid for startup or the request is dropped.
	 * @param client_conn Connection sampled for the startup parameters to
	 *        replay on the warmed backend. It is only read, never retained, and
	 *        may be null, in which case no startup parameters are captured.
	 * @param target_server Optional reader to pin the warmup to. When given, its
	 *        address and port must be set, and the current server-list
	 *        generation is recorded so the request is dropped if the topology
	 *        changes before the warmup thread reaches it. When null, the warmup
	 *        thread picks the targets itself.
	 */
	void request_split_warmup(unsigned int reader_hostgroup_id,
		const char* username, const char* password, const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn,
		const PgSQL_SrvC* target_server = nullptr);
	/**
	 * @brief Run one warmup pass over the queued requests.
	 *
	 * Call this only from the dedicated warmup thread. It drains a batch of
	 * requests, expands each into per-reader targets, reserves a slot on each
	 * target and then blocks polling the backend sockets until they connect or
	 * time out, so it is not suitable for a worker's hot path.
	 *
	 * It takes the HGM write lock itself around target discovery and around
	 * each reservation, so the caller must not hold it. It returns immediately
	 * when nothing was drained.
	 */
	void warm_split_pools();
	/**
	 * @brief Reload the warmup thread's cached configuration variables.
	 *
	 * Call this only from the warmup thread. It writes the pgsql_thread___*
	 * cached variables of the calling thread, so calling it from a worker would
	 * overwrite that worker's cached configuration in the middle of a pass.
	 *
	 * It takes the GloPTH write lock for its whole body, so it must not be
	 * called from any context that already holds the threads-handler lock. It
	 * returns without doing anything when GloPTH is not yet initialised.
	 */
	void refresh_thread_variables();
	struct ConnectionReturnDecision {
		PolarDB_ReaderConnectionReturnStatus status{
			PolarDB_ReaderConnectionReturnStatus::NOT_MANAGED};
		PgSQL_PoolMatchKey match_key;

		bool returnable() const {
			return status ==
				PolarDB_ReaderConnectionReturnStatus::RETURNABLE;
		}
	};
	/** @brief Classify a connection and derive its exact shared-pool key. */
	ConnectionReturnDecision connection_return_decision(
		PgSQL_Connection* conn);
	/** @brief Count a classified drop reason. Other statuses are ignored. */
	void account_connection_return_rejection(
		PolarDB_ReaderConnectionReturnStatus status);
	/**
	 * Revalidate one reader against the current request and optionally retain
	 * the topology snapshot that keeps the returned server pointer valid.
	 *
	 * @param hostgroup_id Hostgroup the reader must currently belong to.
	 * @param server Reader to revalidate.
	 * @param reader_plan Routing plan; supplies the replica requirement and the
	 *        lag cap that the reader's LSN sample is checked against.
	 * @param wait_spec Consistency wait for this request; its timeout shapes the
	 *        freshness window applied to the LSN sample.
	 * @param exclude_address Address to refuse. Exclusion applies only when this
	 *        is non-empty and @p exclude_port is not negative; with the default
	 *        port of -1 the address is ignored.
	 * @param exclude_port Port that must accompany @p exclude_address.
	 * @param selected_server_snapshot Optional out-parameter. It is cleared on
	 *        entry and on a false return. On success it holds the server-list
	 *        snapshot that keeps @p server alive, and the caller must retain it
	 *        for as long as it uses that pointer. Passing null means the caller
	 *        gets no such guarantee and @p server may be freed by the next
	 *        topology change.
	 * @return true when this reader may serve the request.
	 */
	bool server_can_serve_request(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address = nullptr, int exclude_port = -1,
		std::shared_ptr<const void>* selected_server_snapshot = nullptr) const;
	/**
	 * @brief Derive the pool identity a reader-pool reservation is registered
	 *        and later re-validated under.
	 *
	 * A reservation is handed to a session only when the key computed now still
	 * equals the key it was registered with, so this must produce the same
	 * result for the same session and request. The key is deliberately built
	 * with only_pooled false: a pooled-only request and an ordinary read of the
	 * same session must share one reservation identity rather than compete for
	 * two.
	 *
	 * @param hostgroup_id Reader hostgroup the reservation belongs to.
	 * @param sess Session the reservation is for. Must not be null.
	 * @param wait_spec Consistency wait for the request.
	 * @param match_key Filled in only on success. Must not be null.
	 * @return true when a usable key was produced. false when the hostgroup is
	 *         not a PolarDB hostgroup, when the request carries a wait target
	 *         but the hostgroup's startup profile cannot request an RFQ LSN, or
	 *         when the derived key is empty. A false return means no reservation
	 *         identity exists for this request.
	 */
	bool reader_pool_reservation_match_key(
		unsigned int hostgroup_id, PgSQL_Session* sess,
		const PolarDB_WaitSpec& wait_spec,
		PgSQL_PoolMatchKey* match_key) const;
	/**
	 * Select whether a completed reader stays worker-local, returns to shared
	 * storage, or must be removed. The result also carries the connection check
	 * status so the final owner can count a removal once. Demand from
	 * excluded_worker_index does not force a return to the same worker.
	 */
	PolarDB_ReaderLocalReturnDecision local_return_decision(
		PgSQL_Connection* conn, unsigned int excluded_worker_index);
	/**
	 * @brief Acquire a reader connection for one query.
	 *
	 * This is the public acquisition entry point. It first tries the lock-free
	 * selection and pooled lookup; only if that finds nothing and the request
	 * allows it does it fall back to resetting or creating a connection on the
	 * selected server, which takes the HGM write lock.
	 *
	 * On ACQUIRED the caller owns result.conn. It has already been moved to the
	 * server's USED list, and the snapshot that keeps result.srv valid has been
	 * moved onto the connection, so the caller must not use
	 * result.selected_server_snapshot afterwards. result.wait_bypass_allowed
	 * false means the connection has not confirmed the requested LSN and the
	 * caller must still wrap the query for the wait target.
	 *
	 * @param hid Reader hostgroup to select from.
	 * @param sess Session the connection is for.
	 * @param reader_plan Replica requirement and lag cap for this read.
	 * @param wait_spec LSN wait target and type, empty when the read has none.
	 * @param only_pooled true to use only an already-pooled connection. Reset
	 *        and creation are then forbidden, so the HGM write lock is never
	 *        taken and RETRY_AFTER_CONFIG_CHANGE cannot be returned; a miss on
	 *        the selected server instead queues a warmup request for it.
	 * @param exclude_address Endpoint address to avoid, or null/empty for none.
	 *        It takes effect only together with a non-negative @p exclude_port.
	 * @param exclude_port Endpoint port to avoid, negative for none.
	 * @param confirm_reader_group_capacity true to probe every eligible reader
	 *        before reporting the group busy. Without it a saturated selected
	 *        server is reported as READER_BUSY and the other readers are not
	 *        examined.
	 * @return The acquisition result. result.acquired() reports success.
	 *         Otherwise result.status says what the caller must do:
	 *         - READER_BUSY: the selected reader is at capacity. Retry later;
	 *           result carries retry_scope_hash and the reservation identity for
	 *           pacing, but no capacity request may be registered.
	 *         - READER_GROUP_BUSY: every eligible reader was probed and none had
	 *           capacity. This is the only status that authorises registering a
	 *           reader-pool capacity request and waiting to be woken.
	 *         - RETRY_AFTER_CONFIG_CHANGE: topology or startup configuration
	 *           moved while acquiring. Re-plan and try again.
	 *         - GROUP_LSN_UNKNOWN, READER_LSN_UNKNOWN, READER_LSN_STALE,
	 *           READER_LAG_EXCEEDED: no reader can be trusted for this
	 *           consistency requirement, so the query goes to the writer.
	 *         - RFQ_UNAVAILABLE: no reader carries an RFQ-LSN startup profile.
	 *           The session applies pgsql-polardb_action_missing_lsn to route the
	 *           query, rather than performing a plain writer redirect.
	 *         - READER_UNAVAILABLE: no usable reader exists at all.
	 */
	PolarDB_ReaderResult polardb_acquire_reader_connection(unsigned int hid,
		PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address = nullptr, int exclude_port = -1,
		bool confirm_reader_group_capacity = false);

private:
	friend class PgSQL_HostGroups_Manager;
	friend struct PolarDB_WarmupUnitAccess;
	friend size_t pgsql_polardb_unit_collect_split_warmup_targets(
		PgSQL_PolarDB_ReaderPool* pool,
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free);
	/**
	 * Resolve request-independent topology and server checks once. This reads
	 * only the immutable server snapshot and server atomics; it takes no pool,
	 * topology, or demand mutex.
	 *
	 * @param hostgroup_id Hostgroup the server must currently be listed under.
	 * @param server Server to check.
	 * @param exclude_address Address to refuse, applied only together with a
	 *        non-negative @p exclude_port.
	 * @param exclude_port Port that must accompany @p exclude_address.
	 * @param server_snapshot Required out-parameter: passing null makes the call
	 *        return false even for an otherwise eligible server, because the
	 *        caller would have no way to keep @p server alive. It is cleared on
	 *        entry and left cleared on failure. On success it holds the
	 *        server-list snapshot that keeps @p server valid for as long as the
	 *        caller retains it.
	 * @return true when the server is still listed for the hostgroup, is not
	 *         excluded, and its snapshot entry is usable.
	 */
	bool resolve_reader_server_snapshot(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* server_snapshot) const;
	/**
	 * @brief Check one reader against the plan's replication-lag cap.
	 *
	 * The two failure directions are not symmetric. When the plan has no lag cap
	 * there is nothing to enforce and the reader is allowed without any LSN
	 * being looked at, so a true result does not mean the reader's position was
	 * verified. When a cap is configured the check is strict instead: a missing
	 * group LSN, a missing or stale reader sample, or a position outside the
	 * byte cap all reject the reader.
	 *
	 * @param server Reader to check. A null server is rejected.
	 * @param reader_plan Supplies the lag cap and the group LSN it is measured
	 *        against.
	 * @param wait_spec Consistency wait for this request; its timeout, together
	 *        with the lag-cap freshness setting, determines how old the reader's
	 *        LSN sample may be.
	 * @param now_us Current time. It must be a monotonic_time() sample whenever
	 *        the lag cap is enabled; callers pass 0 when it is not, since the
	 *        value is then unused.
	 * @return true when the reader may serve the read under the cap.
	 */
	bool server_meets_lag_policy(
		PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec, uint64_t now_us) const;
	void split_warmup_thread_run();
	/**
	 * @brief Take a batch of pending warmup requests off the queue.
	 *
	 * Takes split_warmup_mutex_ itself, so the caller must not hold it. At most
	 * SPLIT_WARMUP_DRAIN_LIMIT requests are moved per call; anything left stays
	 * queued for the next pass.
	 *
	 * @param requests Requests are appended to this vector; existing contents
	 *        are kept.
	 * @return true when at least one request was appended. false covers two
	 *         different situations: the queue had nothing to drain, or lazy
	 *         warmup has been disabled at runtime, in which case the whole queue
	 *         is purged and the queued, in-flight and rerun sets are cleared
	 *         before returning.
	 */
	bool drain_split_warmup_requests(
		std::vector<PgSQL_SplitWarmupRequest>& requests);
	void release_and_maybe_requeue_split_warmup_key(
		const std::string& key,
		const PgSQL_SplitWarmupRequest* rerun_request = nullptr);
	bool register_split_warmup_inflight_key(const std::string& key);
	bool requeue_split_warmup_request(const PgSQL_SplitWarmupRequest& req);
	/**
	 * @brief Expand one warmup request into per-reader target requests.
	 *
	 * The caller must hold the HGM write lock for the whole call, because this
	 * walks the hostgroup's server list directly. The _unlocked suffix means the
	 * lock is not taken here. Each appended request copies the endpoint it names,
	 * so the results stay usable after the lock is released.
	 *
	 * Readers already holding compatible free connections are counted but not
	 * targeted. Only the shortfall is distributed, starting from a rotating
	 * candidate so repeated requests do not always warm the same reader first.
	 *
	 * @param req Request to expand.
	 * @param target_requests Per-reader requests are appended here, each pinned
	 *        to one endpoint and asking for one connection. Existing contents
	 *        are kept.
	 * @param found_hostgroup Optional out-flag, true when the hostgroup still
	 *        exists. False means it is gone and the request cannot be served.
	 * @param saw_eligible_target Optional out-flag, true when at least one
	 *        reader of the hostgroup was usable.
	 * @param saw_compatible_free Optional out-flag, true when at least one
	 *        reader already holds a compatible free connection. Together with an
	 *        empty @p target_requests this means the pool is already warm, which
	 *        is what lets the caller tell "already warm" apart from "no target".
	 *        All three flags are cleared on entry.
	 */
	void polardb_collect_split_warmup_targets_unlocked(
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free);
	static bool apply_split_warmup_startup_parameters(
		PgSQL_Connection* conn, const PgSQL_SplitWarmupRequest& req);

	PgSQL_HostGroups_Manager* hgm_ = nullptr;
	std::queue<PgSQL_SplitWarmupRequest> split_warmup_queue_;
	std::unordered_set<std::string> split_warmup_queued_;
	std::unordered_set<std::string> split_warmup_inflight_;
	std::unordered_set<std::string> split_warmup_rerun_pending_;
	std::mutex split_warmup_mutex_;
	std::condition_variable split_warmup_cv_;
	std::thread* split_warmup_thread_ = nullptr;
	std::atomic<bool> split_warmup_shutdown_{false};
	std::atomic<unsigned long long> split_warmup_next_candidate_{0};
};
#endif // POLARDB_PROXY

#endif // PROXYSQL_PGSQL_POLARDB_READER_POOL_H
