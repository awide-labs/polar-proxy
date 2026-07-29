#ifndef PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H
#define PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H
#include "proxysql.h"
#include "cpp.h"
#include "proxysql_gtid.h"
#include "proxysql_admin.h"
#include "PgSQL_PolarDB_Counters.h"
#include "PgSQL_PolarDB_ReaderPool.h"
#include <atomic>
#include <deque>
#include <memory>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Headers for declaring Prometheus counters
#include "prometheus/counter.h"
#include "prometheus/gauge.h"

#include "thread.h"
#include "wqueue.h"

#include "ev.h"

#ifndef SPOOKYV2
#include "SpookyV2.h"
#define SPOOKYV2
#endif

#ifndef PROXYJSON
#define PROXYJSON
#include "../deps/json/json_fwd.hpp"
#endif // PROXYJSON

#ifdef DEBUG
/* */
//	Enabling STRESSTEST_POOL ProxySQL will do a lot of loops in the connection pool
//	This is for internal testing ONLY!!!!
//#define STRESSTEST_POOL
#endif // DEBUG


#include "Base_HostGroups_Manager.h"

// we have 2 versions of the same tables: with (debug) and without (no debug) checks
#ifdef DEBUG
#define MYHGM_PgSQL_SERVERS "CREATE TABLE pgsql_servers ( hostgroup_id INT NOT NULL DEFAULT 0 , hostname VARCHAR NOT NULL , port INT NOT NULL DEFAULT 5432 , weight INT CHECK (weight >= 0) NOT NULL DEFAULT 1 , status INT CHECK (status IN (0, 1, 2, 3, 4)) NOT NULL DEFAULT 0 , compression INT CHECK (compression >=0 AND compression <= 102400) NOT NULL DEFAULT 0 , max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 1000 , max_replication_lag INT CHECK (max_replication_lag >= 0 AND max_replication_lag <= 126144000) NOT NULL DEFAULT 0 , use_ssl INT CHECK (use_ssl IN(0,1)) NOT NULL DEFAULT 0 , max_latency_ms INT UNSIGNED CHECK (max_latency_ms>=0) NOT NULL DEFAULT 0 , comment VARCHAR NOT NULL DEFAULT '' , mem_pointer INT NOT NULL DEFAULT 0 , PRIMARY KEY (hostgroup_id, hostname, port) )"
#define MYHGM_PgSQL_SERVERS_INCOMING "CREATE TABLE pgsql_servers_incoming ( hostgroup_id INT NOT NULL DEFAULT 0 , hostname VARCHAR NOT NULL , port INT NOT NULL DEFAULT 5432 , weight INT CHECK (weight >= 0) NOT NULL DEFAULT 1 , status INT CHECK (status IN (0, 1, 2, 3, 4)) NOT NULL DEFAULT 0 , compression INT CHECK (compression >=0 AND compression <= 102400) NOT NULL DEFAULT 0 , max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 1000 , max_replication_lag INT CHECK (max_replication_lag >= 0 AND max_replication_lag <= 126144000) NOT NULL DEFAULT 0 , use_ssl INT CHECK (use_ssl IN(0,1)) NOT NULL DEFAULT 0 , max_latency_ms INT UNSIGNED CHECK (max_latency_ms>=0) NOT NULL DEFAULT 0 , comment VARCHAR NOT NULL DEFAULT '' , PRIMARY KEY (hostgroup_id, hostname, port))"
#else
#define MYHGM_PgSQL_SERVERS "CREATE TABLE pgsql_servers ( hostgroup_id INT NOT NULL DEFAULT 0 , hostname VARCHAR NOT NULL , port INT NOT NULL DEFAULT 5432 , weight INT NOT NULL DEFAULT 1 , status INT NOT NULL DEFAULT 0 , compression INT NOT NULL DEFAULT 0 , max_connections INT NOT NULL DEFAULT 1000 , max_replication_lag INT NOT NULL DEFAULT 0 , use_ssl INT NOT NULL DEFAULT 0 , max_latency_ms INT UNSIGNED NOT NULL DEFAULT 0 , comment VARCHAR NOT NULL DEFAULT '' , mem_pointer INT NOT NULL DEFAULT 0 , PRIMARY KEY (hostgroup_id, hostname, port) )"
#define MYHGM_PgSQL_SERVERS_INCOMING "CREATE TABLE pgsql_servers_incoming ( hostgroup_id INT NOT NULL DEFAULT 0 , hostname VARCHAR NOT NULL , port INT NOT NULL DEFAULT 5432 , weight INT NOT NULL DEFAULT 1 , status INT NOT NULL DEFAULT 0 , compression INT NOT NULL DEFAULT 0 , max_connections INT NOT NULL DEFAULT 1000 , max_replication_lag INT NOT NULL DEFAULT 0 , use_ssl INT NOT NULL DEFAULT 0 , max_latency_ms INT UNSIGNED NOT NULL DEFAULT 0 , comment VARCHAR NOT NULL DEFAULT '' , PRIMARY KEY (hostgroup_id, hostname, port))"
#endif /* DEBUG */
#define MYHGM_PgSQL_SERVERS_SSL_PARAMS "CREATE TABLE pgsql_servers_ssl_params (hostname VARCHAR NOT NULL , port INT CHECK (port >= 0 AND port <= 65535) NOT NULL DEFAULT 5432 , username VARCHAR NOT NULL DEFAULT '' , ssl_ca VARCHAR NOT NULL DEFAULT '' , ssl_cert VARCHAR NOT NULL DEFAULT '' , ssl_key VARCHAR NOT NULL DEFAULT '' , ssl_crl VARCHAR NOT NULL DEFAULT '' , ssl_crlpath VARCHAR NOT NULL DEFAULT '' , ssl_protocol_version_range VARCHAR NOT NULL DEFAULT '' , comment VARCHAR NOT NULL DEFAULT '' , PRIMARY KEY (hostname, port, username) )"
#if POLARDB_PROXY
// HGM-internal mirror of pgsql_replication_hostgroups for PolarDB topology and
// LSN consistency policy. Must stay aligned with
// ADMIN_SQLITE_TABLE_PGSQL_REPLICATION_HOSTGROUPS_V3_0_5.
#define MYHGM_PgSQL_REPLICATION_HOSTGROUPS "CREATE TABLE pgsql_replication_hostgroups (writer_hostgroup INT CHECK (writer_hostgroup>=0) NOT NULL PRIMARY KEY , reader_hostgroup INT NOT NULL CHECK (reader_hostgroup<>writer_hostgroup AND reader_hostgroup>=0) , check_type VARCHAR CHECK (LOWER(check_type) IN ('read_only', 'polardb')) NOT NULL DEFAULT 'read_only' , txn_split_enabled INT CHECK (txn_split_enabled IN (0, 1) AND (txn_split_enabled = 0 OR LOWER(check_type) = 'polardb')) NOT NULL DEFAULT 0 , consistency_mode VARCHAR CHECK (LOWER(consistency_mode) IN ('default', 'off', 'lsn', 'global_lsn', 'lsn_global', 'global', 'primary')) NOT NULL DEFAULT 'default' , max_lag_bytes INT NOT NULL DEFAULT -1 , lsn_wait_timeout_ms INT NOT NULL DEFAULT -1 , proxy_protocol VARCHAR CHECK (LOWER(proxy_protocol) IN ('default', 'v15', 'legacy', 'off')) NOT NULL DEFAULT 'default' , comment VARCHAR NOT NULL DEFAULT '' , UNIQUE (reader_hostgroup))"
#else
#define MYHGM_PgSQL_REPLICATION_HOSTGROUPS "CREATE TABLE pgsql_replication_hostgroups (writer_hostgroup INT CHECK (writer_hostgroup>=0) NOT NULL PRIMARY KEY , reader_hostgroup INT NOT NULL CHECK (reader_hostgroup<>writer_hostgroup AND reader_hostgroup>=0) , check_type VARCHAR CHECK (LOWER(check_type) IN ('read_only')) NOT NULL DEFAULT 'read_only' , comment VARCHAR NOT NULL DEFAULT '' , UNIQUE (reader_hostgroup))"
#endif // POLARDB_PROXY

#define PGHGM_GEN_ADMIN_RUNTIME_SERVERS "SELECT hostgroup_id, hostname, port, CASE status WHEN 0 THEN \"ONLINE\" WHEN 1 THEN \"SHUNNED\" WHEN 2 THEN \"OFFLINE_SOFT\" WHEN 3 THEN \"OFFLINE_HARD\" WHEN 4 THEN \"SHUNNED\" END status, weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment FROM pgsql_servers ORDER BY hostgroup_id, hostname, port"

#define MYHGM_PgSQL_HOSTGROUP_ATTRIBUTES "CREATE TABLE pgsql_hostgroup_attributes (hostgroup_id INT NOT NULL PRIMARY KEY , max_num_online_servers INT CHECK (max_num_online_servers>=0 AND max_num_online_servers <= 1000000) NOT NULL DEFAULT 1000000 , autocommit INT CHECK (autocommit IN (-1, 0, 1)) NOT NULL DEFAULT -1 , free_connections_pct INT CHECK (free_connections_pct >= 0 AND free_connections_pct <= 100) NOT NULL DEFAULT 10 , init_connect VARCHAR NOT NULL DEFAULT '' , multiplex INT CHECK (multiplex IN (0, 1)) NOT NULL DEFAULT 1 , connection_warming INT CHECK (connection_warming IN (0, 1)) NOT NULL DEFAULT 0 , throttle_connections_per_sec INT CHECK (throttle_connections_per_sec >= 1 AND throttle_connections_per_sec <= 1000000) NOT NULL DEFAULT 1000000 , ignore_session_variables VARCHAR CHECK (JSON_VALID(ignore_session_variables) OR ignore_session_variables = '') NOT NULL DEFAULT '' , hostgroup_settings VARCHAR CHECK (JSON_VALID(hostgroup_settings) OR hostgroup_settings = '') NOT NULL DEFAULT '' , servers_defaults VARCHAR CHECK (JSON_VALID(servers_defaults) OR servers_defaults = '') NOT NULL DEFAULT '' , comment VARCHAR NOT NULL DEFAULT '')"

/*
 * @brief Generates the 'runtime_pgsql_servers' resultset exposed to other ProxySQL cluster members.
 * @details Makes 'SHUNNED' and 'SHUNNED_REPLICATION_LAG' statuses equivalent to 'ONLINE'. 'SHUNNED' states
 *  are by definition local transitory states, this is why a 'pgsql_servers' table reconfiguration isn't
 *  normally performed when servers are internally imposed with these statuses. This means, that propagating
 *  this state to other cluster members is undesired behavior, and so it's generating a different checksum,
 *  due to a server having this particular state, that will result in extra unnecessary fetching operations.
 *  The query also filters out 'OFFLINE_HARD' servers, 'OFFLINE_HARD' is a local status which is equivalent to
 *  a server no longer being part of the table (DELETED state). And so, they shouldn't be propagated.
 *
 *  For placing the query into a single line for debugging purposes:
 *  ```
 *  sed 's/^\t\+"//g; s/"\s\\$//g; s/\\"/"/g' /tmp/select.sql | paste -sd ''
 *  ```
 */
#define PGHGM_GEN_CLUSTER_ADMIN_RUNTIME_SERVERS \
	"SELECT " \
		"hostgroup_id, hostname, port, " \
		"CASE status" \
		" WHEN 0 THEN \"ONLINE\"" \
		" WHEN 1 THEN \"ONLINE\"" \
		" WHEN 2 THEN \"OFFLINE_SOFT\"" \
		" WHEN 3 THEN \"OFFLINE_HARD\"" \
		" WHEN 4 THEN \"ONLINE\" " \
		"END status," \
		"weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment " \
	"FROM pgsql_servers " \
	"WHERE status != 3 " \
	"ORDER BY hostgroup_id, hostname, port" \

/**
 * @brief Generates the 'pgsql_servers_v2' resultset exposed to other ProxySQL cluster members.
 * @details The generated resultset is used for the checksum computation of the runtime ProxySQL config
 *  ('pgsql_servers_v2' checksum), and it's also forwarded to other cluster members when querying the Admin
 *  interface with 'CLUSTER_QUERY_PgSQL_SERVERS_V2'. It makes 'SHUNNED' state equivalent to 'ONLINE', and also
 *  filters out any 'OFFLINE_HARD' entries. This is done because none of the statuses are valid configuration
 *  statuses, they are local, transient status that ProxySQL uses during operation.
 */
#define PGHGM_GEN_CLUSTER_ADMIN_PGSQL_SERVERS \
	"SELECT " \
		"hostgroup_id, hostname, port, " \
		"CASE" \
		" WHEN status=\"SHUNNED\" THEN \"ONLINE\"" \
		" ELSE status " \
		"END AS status, " \
		"weight, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment " \
	"FROM main.pgsql_servers " \
	"WHERE status != \"OFFLINE_HARD\" " \
	"ORDER BY hostgroup_id, hostname, port"

class PgSQL_SrvConnList;
class PgSQL_SrvC;
class PgSQL_SrvList;
class PgSQL_HGC;
class PgSQL_Connection;

// Forward declaration for WebUI monitoring metrics collector
namespace ProxySQL {
namespace Monitoring {
class MetricsCollector;
}
}

class PgSQL_Errors_stats {
public:
	PgSQL_Errors_stats(int _hostgroup, const char* _hostname, int _port, const char* _username, const char* _address, const char* _dbname,
		const char* _sqlstate, const char* _errmsg, time_t tn);
	~PgSQL_Errors_stats();
	char** get_row();
	void add_time(unsigned long long n, const char* le);
	void free_row(char** pta);

private:
	int hostgroup;
	char* hostname;
	int port;
	char* username;
	char* client_address;
	char* dbname;
	char sqlstate[5 + 1];
	char* errmsg;
	time_t first_seen;
	time_t last_seen;
	unsigned long long count_star;
};

typedef std::unordered_map<std::uint64_t, std::unique_ptr<PgSQL_Errors_stats>> umap_pgsql_errors;

/**
 * Four-part connection match key used by the PostgreSQL pool.
 *
 * The caller fills the four values. The pool only compares them and does not
 * interpret protocol, LSN, identity, or session policy.
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

enum class PgSQL_PoolGetMode : uint8_t {
	NONE = 0,
	ALLOW_EXACT_MATCH = 1U << 0,
	ALLOW_RESET = 1U << 1,
	ALLOW_CREATE = 1U << 2
};

static inline PgSQL_PoolGetMode operator|(
		PgSQL_PoolGetMode lhs, PgSQL_PoolGetMode rhs) {
	return static_cast<PgSQL_PoolGetMode>(
		static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
}

static inline bool pgsql_pool_get_mode_has(
		PgSQL_PoolGetMode mode, PgSQL_PoolGetMode flag) {
	return (static_cast<uint8_t>(mode) & static_cast<uint8_t>(flag)) != 0;
}

enum class PgSQL_PoolGetSource : uint8_t {
	NONE = 0,
	EXACT_MATCH,
	RESET,
	CREATED
};

struct PgSQL_PoolGetResult {
	PgSQL_Connection* conn{nullptr};
	PgSQL_PoolGetSource source{PgSQL_PoolGetSource::NONE};
	bool server_saturated{false};
	bool retry_current_state{false};
	bool exact_match_claimed{false};
};

struct PolarDB_ReaderClaimWake {
	unsigned int worker_index{UINT_MAX};
	uint64_t token{0};
	PgSQL_SrvC* server{nullptr};
	std::shared_ptr<const void> server_snapshot;

	bool valid() const {
		return worker_index != UINT_MAX && token != 0 && server != nullptr;
	}
};

#if POLARDB_PROXY
struct PolarDB_ReaderClaimDemand {
	unsigned int worker_index{UINT_MAX};
	uint64_t token{0};
	uint64_t scope_hash{0};
	PgSQL_PoolMatchKey match_key;
	PolarDB_Query_ReaderPlan reader_plan;
	PolarDB_WaitSpec wait_spec;
};
#endif // POLARDB_PROXY

enum class PolarDB_ReaderClaimRetireReason : uint8_t {
	NONE = 0,
	ACQUIRED,
	RELEASED,
	EXPLICIT_REMOVE,
	CREATE_EVICT,
	IDLE_TRIM,
	MAX_AGE,
	SERVER_OFFLINE,
	POOL_DROP,
	INVALID_FREE_STATE
};

enum class PolarDB_ReaderClaimTakeStatus : uint8_t {
	MISSING = 0,
	PENDING,
	RETIRED,
	ACQUIRED
};

struct PolarDB_ReaderClaimTakeResult {
	PgSQL_Connection* conn{nullptr};
	PolarDB_ReaderClaimTakeStatus status{
		PolarDB_ReaderClaimTakeStatus::MISSING};
	PolarDB_ReaderClaimRetireReason retire_reason{
		PolarDB_ReaderClaimRetireReason::NONE};
};

class PgSQL_SrvConnList {
	private:
	PgSQL_SrvC *mysrvc;
	bool used_list;
#if POLARDB_PROXY
	std::unordered_map<PgSQL_PoolMatchKey,
		std::vector<PgSQL_Connection*>, PgSQL_PoolMatchKeyHash> matching_by_key;
	std::vector<PgSQL_PoolMatchKey> match_keys_by_position;
#endif // POLARDB_PROXY
	int find_idx(PgSQL_Connection* c) const;
	void add_unlocked(PgSQL_Connection*, const PgSQL_PoolMatchKey* key = nullptr);
	PgSQL_Connection* remove_position_unlocked(
		unsigned int index,
		PolarDB_ReaderClaimRetireReason reason =
			PolarDB_ReaderClaimRetireReason::EXPLICIT_REMOVE);
	PgSQL_Connection* remove_unlocked(
		unsigned int index,
		PolarDB_ReaderClaimRetireReason reason =
			PolarDB_ReaderClaimRetireReason::EXPLICIT_REMOVE);
#if POLARDB_PROXY
	PgSQL_Connection* remove_matching_unlocked(const PgSQL_PoolMatchKey& key);
	bool match_key_unlocked(PgSQL_Connection* conn,
		PgSQL_PoolMatchKey* key) const;
	void unindex_unlocked(PgSQL_Connection*);
#endif // POLARDB_PROXY
	void detach_all_unlocked(
		std::vector<PgSQL_Connection*>& connections,
		PolarDB_ReaderClaimRetireReason reason =
			PolarDB_ReaderClaimRetireReason::POOL_DROP);
	friend class PgSQL_SrvC;
	friend class PgSQL_HostGroups_Manager;
	public:
	PtrArray *conns;
	PgSQL_SrvConnList(PgSQL_SrvC *, bool used);
	~PgSQL_SrvConnList();
	void add(PgSQL_Connection *);
	void remove(PgSQL_Connection *c);
	PgSQL_Connection *remove(
		int index,
		PolarDB_ReaderClaimRetireReason reason =
			PolarDB_ReaderClaimRetireReason::EXPLICIT_REMOVE);
	PgSQL_Connection * get_random_MyConn(PgSQL_Session *sess, bool ff,
		bool only_pooled = false,
		std::vector<PgSQL_Connection*>* connections_to_delete = nullptr);
	void get_random_MyConn_inner_search(unsigned int start, unsigned int end, unsigned int& conn_found_idx, unsigned int& connection_quality_level, unsigned int& number_of_matching_session_variables, const PgSQL_Connection * client_conn);
	unsigned int conns_length();
	void drop_all_connections();
	PgSQL_Connection *index(unsigned int);
};

#if POLARDB_PROXY
struct PolarDB_PoolConnStats {
	unsigned int used{0};
	unsigned int free{0};

	unsigned int active() const {
		return used;
	}

	unsigned int idle() const {
		return free;
	}

	unsigned int total() const {
		return used + free;
	}
};

struct PolarDB_IdleTrimResult {
	unsigned int deferred{0};
	unsigned int cancelled{0};
	unsigned int destroyed{0};
};

enum class PolarDB_IdleTrimEndReason : uint8_t {
	TAKEN,
	RETAINED,
	OTHER_REMOVAL,
	DESTROYED
};
#endif // POLARDB_PROXY

class PgSQL_SrvC {	// MySQL Server Container
	public:
	PgSQL_HGC *myhgc;
	char *address;
	uint16_t port;
	uint16_t flags;
	int64_t weight;
	enum MySerStatus status;
	unsigned int compression;
	int64_t max_connections;
	unsigned int aws_aurora_current_lag_us;
	unsigned int max_replication_lag;
	unsigned int max_connections_used; // The maximum number of connections that has been opened
	unsigned int connect_OK;
	unsigned int connect_ERR;
	unsigned int cur_replication_lag_count;
	// note that these variables are in microsecond, while user defines max latency in millisecond
#if !POLARDB_PROXY
	unsigned int current_latency_us;
#endif // POLARDB_PROXY
	unsigned int current_latency_us_value() const {
#if POLARDB_PROXY
		return current_latency_us.load(std::memory_order_relaxed);
#else
		return current_latency_us;
#endif // POLARDB_PROXY
	}
	void set_current_latency_us_value(unsigned int value) {
#if POLARDB_PROXY
		current_latency_us.store(value, std::memory_order_relaxed);
#else
		current_latency_us = value;
#endif // POLARDB_PROXY
	}
	unsigned int max_latency_us;
	time_t time_last_detected_error;
	unsigned int connect_ERR_at_time_last_detected_error;
	unsigned long long queries_sent;
	unsigned long long bytes_sent;
	unsigned long long bytes_recv;
	bool shunned_automatic;
	bool shunned_and_kill_all_connections; // if a serious failure is detected, this will cause all connections to die even if the server is just shunned
	int32_t use_ssl;
	char *comment;
	PgSQL_SrvConnList *ConnectionsUsed;
	PgSQL_SrvConnList *ConnectionsFree;
	PgSQL_Connection* take_free_connection_for_ping(
		PgSQL_Connection* conn, unsigned long long max_last_time_used);
#if POLARDB_PROXY
	private:
	struct PolarDB_ReaderClaim {
		unsigned int worker_index;
		uint64_t token;
		PgSQL_Connection* conn;
		PgSQL_PoolMatchKey match_key;
	};
	struct PolarDB_ReaderClaimRetired {
		unsigned int worker_index;
		PolarDB_ReaderClaimRetireReason reason;
	};
	std::unordered_map<uint64_t, PolarDB_ReaderClaim>
		polardb_reader_claims;
	std::unordered_map<PgSQL_Connection*, uint64_t>
		polardb_reader_claim_token_by_connection;
	std::unordered_map<uint64_t, PolarDB_ReaderClaimRetired>
		polardb_reader_claim_retired;
	bool polardb_publish_matching_free_unlocked(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderClaimWake* wake,
		std::shared_ptr<const void>& released_server_snapshot);
	void polardb_forget_claimed_connection_unlocked(
		PgSQL_Connection* conn, PolarDB_ReaderClaimRetireReason reason);
	void polardb_retire_claims_unlocked(
		PolarDB_ReaderClaimRetireReason reason);
	bool polardb_connection_claimed_unlocked(PgSQL_Connection* conn) const;
	bool polardb_finish_idle_trim_unlocked(
		PgSQL_Connection* conn, PolarDB_IdleTrimEndReason reason);
	friend class PgSQL_SrvConnList;
	friend class PgSQL_HostGroups_Manager;

	public:
	// The FREE and USED lists have one owner and one lock per server. The global
	// HGM lock may take this lock; code must never take them in reverse order.
	mutable std::recursive_mutex pool_mutex;
	alignas(64) std::atomic<unsigned int> pool_free_count{0};
	std::atomic<unsigned int> pool_used_count{0};
	std::atomic<unsigned int> polardb_idle_ping_count{0};
	PgSQL_Connection* take_matching_connection(const PgSQL_PoolMatchKey& key);
	PgSQL_PoolGetResult take_existing_connection(PgSQL_Session* sess,
		const PgSQL_PoolMatchKey& key, PgSQL_PoolGetMode mode,
		unsigned int selected_max_connections = 0,
		unsigned long long* lock_wait_us = nullptr,
		unsigned long long* lock_hold_us = nullptr);
	bool add_matching_connection(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderClaimWake* wake = nullptr);
	bool add_used_matching_connection(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key);
	bool return_matching_connection(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key,
		unsigned long long* lock_wait_us = nullptr,
		unsigned long long* lock_hold_us = nullptr,
		PolarDB_ReaderClaimWake* wake = nullptr);
	PolarDB_ReaderYieldResult yield_matching_connection_to_claim(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		unsigned int donor_worker_index, PolarDB_ReaderClaimWake* wake);
	bool register_reader_claim_demand(
		unsigned int worker_index, uint64_t token,
		const PgSQL_PoolMatchKey& key);
	bool has_matching_reader_claim_demand(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index = UINT_MAX) const;
	PolarDB_ReaderClaimTakeResult take_reader_claim(
		unsigned int worker_index, uint64_t token);
	bool cancel_reader_claim(
		unsigned int worker_index, uint64_t token,
		PolarDB_ReaderClaimWake* next_wake = nullptr);
	bool evict_unclaimed_free_for_create(
		unsigned int preferred_count, unsigned int required_count,
		std::vector<PgSQL_Connection*>& connections_to_delete);
	unsigned int reader_claim_demand_count() const;
	unsigned int reader_claim_count() const;
	unsigned int matching_connection_count(
		const PgSQL_PoolMatchKey& key) const;
	bool used_connection_match_key(PgSQL_Connection* conn,
		PgSQL_PoolMatchKey* key) const;
	bool remove_used_connection(PgSQL_Connection* conn);
	bool remove_free_connection(PgSQL_Connection* conn);
	bool polardb_finish_idle_ping(PgSQL_Connection* conn, bool destroyed);
	PolarDB_IdleTrimResult polardb_trim_free_connections_pct_unlocked(
		unsigned int max_free,
		std::vector<PgSQL_Connection*>& connections_to_delete);
	unsigned int pool_free_count_value() const {
		return pool_free_count.load(std::memory_order_relaxed);
	}
	unsigned int pool_used_count_value() const {
		return pool_used_count.load(std::memory_order_relaxed);
	}
	alignas(64) std::atomic<int> polardb_fast_status{0};
	std::atomic<unsigned int> current_latency_us{0};

	// =========================================================================
	// PolarDB per-server LSN tracking (read-your-writes consistency)
	// =========================================================================
	// Updated by PgSQL_HostGroups_Manager::polardb_update_server_lsn() from two
	// sources: monitor health checks, and the LSN that a backend reports in the
	// extended ReadyForQuery (RFQ) message after running a query. Read without a
	// lock by reader selection and by the byte-lag and freshness checks.
	//
	// There is no per-reader millisecond replication lag value here on purpose:
	// the PostgreSQL/PolarDB path does not yet produce one, and the MySQL/Aurora
	// field aws_aurora_current_lag_us must not be reused for it.
	//
	// polardb_current_lsn : latest WAL LSN seen for this server. Advanced with a
	//                       compare-and-swap to the maximum, so a stale
	//                       observation can never lower it.
	// lsn_updated_at      : monotonic_time() microseconds when this server's LSN
	//                       was last observed; used by the lag-cap freshness check.
	// Keep the LSN pair isolated from unrelated PgSQL_SrvC state. Monitor/RFQ
	// writers update it while routing threads read it on reader selection.
	alignas(64) std::atomic<uint64_t> polardb_current_lsn{0};
	std::atomic<unsigned long long> lsn_updated_at{0};
#endif // POLARDB_PROXY
	/**
	 * @brief Constructs a new MySQL Server Container.
	 * @details For 'server_defaults' parameters, if '-1' is supplied, they try to be obtained from
	 *  'servers_defaults' entry from 'pgsql_hostgroup_attributes' when adding the server to it's target
	 *  hostgroup(via 'PgSQL_HostGroups_Manager::add'), if not found, value is set with 'pgsql_servers'
	 *  defaults.
	 * @param addr Address of the server, specified either by IP or hostname.
	 * @param port Server port.
	 * @param gitd_port If non-zero, enables GTID tracking for the server.
	 * @param _weight Server weight. 'server_defaults' param, check @details.
	 * @param _status Initial server status.
	 * @param _compression Enables compression for server connections.
	 * @param _max_connections Max server connections. 'server_defaults' param, check @details.
	 * @param _max_replication_lag If non-zero, enables replication lag checks.
	 * @param _use_ssl Enables SSL for server connections. 'servers_defaults' param, check @details.
	 * @param _max_latency_ms Max ping server latency. When exceeded, server gets excluded from conn-pool.
	 * @param _comment User defined comment.
	 */
	PgSQL_SrvC(
		char* addr, uint16_t port, int64_t _weight, enum MySerStatus _status, unsigned int _compression,
		int64_t _max_connections, unsigned int _max_replication_lag, int32_t _use_ssl, unsigned int	_max_latency_ms,
		char* _comment
	);
	~PgSQL_SrvC();
	void connect_error(int, bool get_mutex=true);
	void shun_and_killall();
	void set_status(enum MySerStatus new_status);
#if POLARDB_PROXY
	bool polardb_advance_lsn(uint64_t lsn, uint64_t observed_at_us);
	enum MySerStatus polardb_fast_status_value() const;
	PolarDB_PoolConnStats polardb_pool_conn_stats() const;
	unsigned int polardb_pool_active_count() const;
	unsigned int polardb_pool_total_count() const;
	bool polardb_pool_can_run_active() const;
	bool polardb_pool_can_open_socket() const;
#endif // POLARDB_PROXY
	/**
	 * @brief Update the maximum number of used connections
	 * @return The maximum number of used connections
	 */
	unsigned int update_max_connections_used()
	{
#if POLARDB_PROXY
		unsigned int connections_used = polardb_pool_active_count();
#else
		unsigned int connections_used = ConnectionsUsed->conns_length();
#endif // POLARDB_PROXY
		unsigned int current =
			__atomic_load_n(&max_connections_used, __ATOMIC_RELAXED);
		while (
			current < connections_used &&
			!__atomic_compare_exchange_n(
				&max_connections_used, &current, connections_used,
				false, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
		}
		return max_connections_used;
	}
};

class PgSQL_SrvList: public BaseSrvList<PgSQL_HGC> {
	public:
	PgSQL_SrvList(PgSQL_HGC* hgc) : BaseSrvList<PgSQL_HGC>(hgc) {}
	friend class PgSQL_HGC;
};


class PgSQL_HGC: public BaseHGC<PgSQL_HGC> {
	public:
	PgSQL_HGC(int _hid) : BaseHGC<PgSQL_HGC>(_hid) {}
	PgSQL_SrvC *get_random_MySrvC(char * gtid_uuid, uint64_t gtid_trxid, int max_lag_ms, PgSQL_Session *sess);
#if POLARDB_PROXY
	/**
	 * @brief Cached PolarDB writer/reader pairing and consistency policy for one
	 *        hostgroup (mutable runtime State).
	 *
	 * Loaded from the pgsql_replication_hostgroups admin table on each config
	 * commit, and kept on the writer hostgroup container so routing can look up
	 * the policy quickly. Supported consistency modes are off, lsn, global_lsn,
	 * and primary.
	 *
	 * The two shared_ptr<atomic> cells (primary LSN mirror and writer epoch) are
	 * the only fields a query thread reads at request time. They are copied by
	 * value into the current topology snapshot, so a query thread reads them
	 * through that snapshot without taking the HostGroups_Manager lock and without
	 * keeping a pointer to this container.
	 *
	 * Writer-epoch invariant: when the set of live (non-OFFLINE_HARD) writer
	 * servers changes, the epoch is bumped and the primary mirror plus the
	 * affected per-server LSN caches are reset, so stale positions from the old
	 * writer set are never carried forward.
	 *
	 * Rationale and full field semantics: see
	 * doc/polardb-arch/05-MONITOR-AND-HGM-LSN-STATE.md.
	 */
	struct {
		bool configured{false};            // true if this HG is in replication_hostgroups
		unsigned int reader_hostgroup{0};  // corresponding reader HG (if this is writer)
		unsigned int writer_hostgroup{0};  // corresponding writer HG (if this is reader)
		int max_lag_bytes{0};              // max LSN lag in bytes (lag-cap safety; 0 = off)
		std::string check_type;            // e.g. "polardb", "read_only"
		bool txn_split_enabled{false};     // enable RFQ XID evidence and split reads
		std::string consistency_mode;      // consistency mode string (off/lsn/global_lsn/primary)
		int consistency_mode_enum{-1};     // parsed enum value; -1 = use global default
		int lsn_wait_timeout_ms{0};        // polar_xact_split_wait_lsn timeout in ms
		std::string proxy_protocol;        // default/v15/legacy/off
		int proxy_protocol_enum{-1};       // parsed enum value; -1 = inherit global
		// Shared cells are copied into the current PolarDB topology snapshot so
		// query threads can read them without holding an HGM lock or retaining an
		// HGC pointer.
		std::shared_ptr<std::atomic<uint64_t>> polardb_primary_lsn{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // latest trusted group LSN observation (for global/lag targets)
		std::shared_ptr<std::atomic<uint64_t>> polardb_writer_epoch{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // bumps when the writer identity set changes
		std::string polardb_writer_identity; // sorted non-OFFLINE_HARD address:port set
		bool polardb_writer_identity_initialized{false};
	} repl_config;
	// Shared through the immutable server-list snapshot. Each worker uses this
	// once per server-list generation to start at a different weighted position,
	// then advances its own per-hostgroup sequence without shared writes.
	std::shared_ptr<std::atomic<uint64_t>> polardb_reader_selection_start{
		std::make_shared<std::atomic<uint64_t>>(0)
	};

	private:
	// FREE publication may hold PgSQL_SrvC::pool_mutex before taking this
	// mutex. HGC demand methods must never acquire a server pool mutex;
	// eligibility reads only the immutable topology snapshot and atomics.
	mutable std::mutex polardb_reader_claim_demand_mutex;
	std::deque<PolarDB_ReaderClaimDemand> polardb_reader_claim_demands;
	std::atomic<unsigned int> polardb_reader_claim_demand_count_fast{0};

	public:
	bool register_reader_claim_demand(
		unsigned int worker_index, uint64_t token, uint64_t scope_hash,
		const PgSQL_PoolMatchKey& key,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec);
	bool take_matching_reader_claim_demand(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderClaimDemand* selected,
		unsigned int excluded_worker_index = UINT_MAX,
		std::shared_ptr<const void>* selected_server_snapshot = nullptr);
	bool cancel_reader_claim_demand(unsigned int worker_index, uint64_t token);
	bool has_reader_claim_demand(
		unsigned int worker_index, uint64_t token) const;
	bool has_matching_reader_claim_demand(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index = UINT_MAX) const;
	bool has_reader_claim_demand_fast() const {
		return polardb_reader_claim_demand_count_fast.load(
			std::memory_order_acquire) != 0;
	}
	unsigned int reader_claim_demand_count() const;
#endif // POLARDB_PROXY
};

struct PgSQL_p_hg_counter {
	enum metric {
		servers_table_version = 0,
		server_connections_created,
		server_connections_delayed,
		server_connections_aborted,
		client_connections_created,
		client_connections_aborted,
		//com_autocommit,
		//com_autocommit_filtered,
		com_rollback,
		com_rollback_filtered,
		com_backend_reset_connection,
		//com_backend_init_db,
		// TODO: https://github.com/sysown/proxysql/issues/2690
		com_backend_set_client_encoding,
		//com_frontend_init_db,
		com_frontend_set_client_encoding,
		//com_frontend_use_db,
		com_commit_cnt,
		com_commit_cnt_filtered,
		selects_for_update__autocommit0,
		access_denied_wrong_password,
		access_denied_max_connections,
		access_denied_max_user_connections,
		pghgm_pgconnpool_get,
		pghgm_pgconnpool_get_ok,
		pghgm_pgconnpool_get_ping,
		pghgm_pgconnpool_push,
		pghgm_pgconnpool_reset,
		pghgm_pgconnpool_destroy,
		auto_increment_delay_multiplex,
#if POLARDB_PROXY
#define X(name, display_name, prom_name, help) polardb_##name,
		POLARDB_ALL_COUNTER_LIST(X)
#undef X
#endif // POLARDB_PROXY
		SIZE_
	};
};

struct PgSQL_p_hg_gauge {
	enum metric {
		server_connections_connected = 0,
		client_connections_connected,
#if POLARDB_PROXY
#define X(name, display_name, prom_name, help) polardb_##name,
		POLARDB_GAUGE_LIST(X)
#undef X
#endif // POLARDB_PROXY
		SIZE_
	};
};

struct PgSQL_p_hg_dyn_counter {
	enum metric : uint8_t {
		conn_pool_bytes_data_recv = 0,
		conn_pool_bytes_data_sent,
		connection_pool_conn_err,
		connection_pool_conn_ok,
		connection_pool_queries,
		gtid_executed,
		proxysql_pgsql_error,
		pgsql_error,
		SIZE_
	};
};

enum class p_pgsql_error_type : uint8_t {
	pgsql,
	proxysql
};

struct PgSQL_p_hg_dyn_gauge {
	enum metric : uint8_t {
		connection_pool_conn_free = 0,
		connection_pool_conn_used,
		connection_pool_latency_us,
		connection_pool_status,
		SIZE_
	};
};

struct PgSQL_hg_metrics_map_idx {
	enum index {
		counters = 0,
		gauges,
		dyn_counters,
		dyn_gauges,
	};
};

/**
 * @brief Required server info for the read_only Monitoring actions and replication_lag Monitoring actions.
 */
using hostgroupid_t = int;
using hostname_t = std::string;
using address_t = std::string;
using port_t = unsigned int;
using read_only_t = int;
using current_replication_lag = int;
using override_replication_lag = bool;

using read_only_server_t = std::tuple<hostname_t,port_t,read_only_t>;
using replication_lag_server_t = std::tuple<hostgroupid_t, address_t, port_t, current_replication_lag, override_replication_lag>;

enum PgSQL_READ_ONLY_SERVER_T {
	PG_ROS_HOSTNAME = 0,
	PG_ROS_PORT,
	PG_ROS_READONLY,
	PG_ROS_SIZE_
};

enum PgSQL_REPLICATION_LAG_SERVER_T {
	PG_RLS_HOSTGROUP_ID = 0,
	PG_RLS_ADDRESS,
	PG_RLS_PORT,
	PG_RLS_CURRENT_REPLICATION_LAG,
	PG_RLS_SIZE_
};

/**
 * @brief Contains the minimal info for server creation.
 */
struct PgSQL_srv_info_t {
	/* @brief Server address */
	string addr;
	/* @brief Server port */
	uint16_t port;
	/* @brief Server type identifier, used for logging, e.g: 'Aurora AWS', 'GR', etc... */
	string kind;
};

/**
 * @brief Contains options to be specified during server creation.
 */
struct PgSQL_srv_opts_t {
	int64_t weigth;
	int64_t max_conns;
	int32_t use_ssl;
};

class PgSQL_HostGroups_Manager : public Base_HostGroups_Manager<PgSQL_HGC> {
#if 0
	SQLite3DB	*admindb;
	SQLite3DB	*mydb;
	pthread_mutex_t readonly_mutex;
	std::set<std::string> read_only_set1;
	std::set<std::string> read_only_set2;
	pthread_mutex_t lock;
#endif // 0
	private:
	enum HGM_TABLES {
		PgSQL_SERVERS_V2 = 0,
		PgSQL_REPLICATION_HOSTGROUPS,
		PgSQL_GROUP_REPLICATION_HOSTGROUPS,
		PgSQL_GALERA_HOSTGROUPS,
		PgSQL_AWS_AURORA_HOSTGROUPS,
		PgSQL_HOSTGROUP_ATTRIBUTES,
		PgSQL_SERVERS_SSL_PARAMS,
		PgSQL_SERVERS,

		HGM_TABLES_SIZE_
	};

	std::array<uint64_t, HGM_TABLES_SIZE_> table_resultset_checksum { {0} };

	class HostGroup_Server_Mapping {
	public:
		enum Type {
			WRITER = 0,
			READER = 1,

			TYPE_SIZE_
		};

		struct Node {
			PgSQL_SrvC* srv = nullptr;
			unsigned int reader_hostgroup_id = -1;
			unsigned int writer_hostgroup_id = -1;
			//MySerStatus server_status = PgSQL_SERVER_STATUS_OFFLINE_HARD;
		};

		HostGroup_Server_Mapping(PgSQL_HostGroups_Manager* hgm) : readonly_flag(1), myHGM(hgm) { }
		~HostGroup_Server_Mapping() = default;

		/**
		  * @brief Copies all unique nodes from source vector to destination vector.
		  * @details Copies all unique nodes from source vector to destination vector. The source and destination 
		  *   vectors are identified by an input enumeration type, which can be either a reader or a writer. 
		  *	  During the copying process, the function also adds servers to the HostGroup connection container.
		  * @param dest_type Input  Can be reader or writer
		  * @param src_type Input  Can be reader or writer
		*/
		void copy_if_not_exists(Type dest_type, Type src_type);

		/**
		  * @brief Removes node located at the specified index.
		  * @details Node is removed from vector located at the specified index identified by an input enumeration type. 
		  *	  Node that was removed is marked as offline in the HostGroup connection container.
		  * @param dest_type Input  Can be reader or writer
		  * @param index Input  Index of node to be removed
		*/
		void remove(Type type, size_t index);

		/**
		  * @brief Removes all nodes.
		  * @details All nodes are removed from vector, identified by an input enumeration type.
		  *	  Nodes that are removed is marked as offline in the HostGroup connection container.
		  * @param type Input  Can be reader or writer
		*/
		void clear(Type type);

		inline
		const std::vector<Node>& get(Type type) const {
			return mapping[type];
		}

		inline
		void add(Type type, Node& node) {
			mapping[type].push_back(node);
		}

		inline
		void set_readonly_flag(int val) {
			readonly_flag = val;
		}

		inline
		int get_readonly_flag() const {
			return readonly_flag;
		}

	private:
		unsigned int get_hostgroup_id(Type type, const Node& node) const;
		PgSQL_SrvC* insert_HGM(unsigned int hostgroup_id, const PgSQL_SrvC* srv);
		void remove_HGM(PgSQL_SrvC* srv);

		std::array<std::vector<Node>, TYPE_SIZE_> mapping; // index 0 contains reader and 1 contains writer hostgroups
		int readonly_flag;
		PgSQL_HostGroups_Manager* myHGM;
	};

	/**
	 * @brief Used by 'MySQL_Monitor::read_only' to hold a mapping between servers and hostgroups.
	 * @details The hostgroup mapping holds the PgSQL_SrvC for each of the hostgroups in which the servers is
	 *  present, distinguishing between 'READER' and 'WRITER' hostgroups.
	 */
	std::unordered_map<std::string, std::unique_ptr<HostGroup_Server_Mapping>> hostgroup_server_mapping;
	/**
	 * @brief Holds the previous computed checksum for 'pgsql_servers'.
	 * @details Used to check if the servers checksums has changed during 'commit', if a change is detected,
	 *  the member 'hostgroup_server_mapping' is required to be regenerated.
	 *
	 *  This is only updated during 'read_only_action_v2', since the action itself modifies
	 *  'hostgroup_server_mapping' in case any actions needs to be performed against the servers.
	 */
	uint64_t hgsm_pgsql_servers_checksum = 0;
	/**
	 * @brief Holds the previous checksum for the 'PgSQL_REPLICATION_HOSTGROUPS'.
	 * @details Used during 'commit' to determine if config has changed for 'PgSQL_REPLICATION_HOSTGROUPS',
	 *   and 'hostgroup_server_mapping' should be rebuild.
	 */
	uint64_t hgsm_pgsql_replication_hostgroups_checksum = 0;

	std::mutex PgSQL_Servers_SSL_Params_map_mutex;
	std::unordered_map<std::string, PgSQLServers_SslParams> PgSQL_Servers_SSL_Params_map;
#if POLARDB_PROXY
	pthread_rwlock_t polardb_fast_topology_lock;
#endif // POLARDB_PROXY

#if 0
	PtrArray *MyHostGroups;
	std::unordered_map<unsigned int, PgSQL_HGC *>MyHostGroups_map;

	PgSQL_HGC * MyHGC_find(unsigned int);
	PgSQL_HGC * MyHGC_create(unsigned int);
#endif // 0

	void add(PgSQL_SrvC *, unsigned int);
	void purge_pgsql_servers_table();
	void generate_pgsql_servers_table(int *_onlyhg=NULL);
	void generate_pgsql_replication_hostgroups_table();

	/**
	 * @brief This resultset holds the current values for 'runtime_pgsql_servers' computed by either latest
	 *  'commit' or fetched from another Cluster node. It's also used by ProxySQL_Admin to respond to the
	 *  intercepted query 'CLUSTER_QUERY_RUNTIME_PgSQL_SERVERS'.
	 * @details This resultset can't right now just contain the value for 'incoming_pgsql_servers' as with the
	 *  rest of the intercepted resultset. This is due to 'runtime_pgsql_servers' reconfigurations that can be
	 *  triggered by monitoring actions like 'Galera' currently performs. These actions not only trigger status
	 *  changes in the servers, but also re-generate the servers table via 'commit', thus generating a new
	 *  checksum in the process. Because of this potential mismatch, the fetching server wouldn't be able to
	 *  compute the proper checksum for the fetched 'runtime_pgsql_servers' config.
	 *
	 *  As previously stated, these reconfigurations are monitoring actions, they can't be packed or performed
	 *  in a single action, since monitoring data is required, which may not be already present. This makes
	 *  this a convergent, but iterative process, that can't be compressed into a single action. Using other
	 *  nodes 'runtime_pgsql_servers' while fetching represents a best effort for avoiding these
	 *  reconfigurations in nodes that already holds the same monitoring conditions. If monitoring
	 *  conditions are not the same, circular fetching is still possible due to the previously described
	 *  scenario.
	 */
	SQLite3_result* runtime_pgsql_servers;
	/**
	 * @brief These resultset holds the latest values for 'incoming_*' tables used to promoted servers to runtime.
	 * @details All these resultsets are used by 'Cluster' to fetch and promote the same configuration used in the
	 *  node across the whole cluster. For these, the queries:
	 *   - 'CLUSTER_QUERY_PgSQL_REPLICATION_HOSTGROUPS'
	 *   - 'CLUSTER_QUERY_PgSQL_GROUP_REPLICATION_HOSTGROUPS'
	 *   - 'CLUSTER_QUERY_PgSQL_GALERA'
	 *   - 'CLUSTER_QUERY_PgSQL_AWS_AURORA'
	 *   - 'CLUSTER_QUERY_PgSQL_HOSTGROUP_ATTRIBUTES'
	 *  Issued by 'Cluster' are intercepted by 'ProxySQL_Admin' and return the content of these resultsets.
	 */
	SQLite3_result *incoming_replication_hostgroups;

	void generate_pgsql_hostgroup_attributes_table();
	void generate_pgsql_servers_ssl_params_table();
	SQLite3_result *incoming_hostgroup_attributes;
	SQLite3_result *incoming_pgsql_servers_ssl_params = nullptr;

	SQLite3_result* incoming_pgsql_servers_v2;

	char rand_del[8];
	pthread_mutex_t pgsql_errors_mutex;
	umap_pgsql_errors pgsql_errors_umap;

	/**
	 * @brief Update the prometheus "connection_pool" counters.
	 */
	void p_update_connection_pool();

	void p_update_connection_pool_update_counter(
		const std::string& endpoint_id, const std::map<std::string, std::string>& labels,
		std::map<std::string, prometheus::Counter*>& m_map, unsigned long long value, PgSQL_p_hg_dyn_counter::metric idx
	);
	void p_update_connection_pool_update_gauge(
		const std::string& endpoint_id, const std::map<std::string, std::string>& labels,
		std::map<std::string, prometheus::Gauge*>& m_map, unsigned long long value, PgSQL_p_hg_dyn_gauge::metric idx
	);

	public:
	// Friend declaration for WebUI monitoring metrics collector
	friend class ProxySQL::Monitoring::MetricsCollector;

	/**
	 * @brief Mutex used to guard 'pgsql_servers_to_monitor' resulset.
	 */
	std::mutex pgsql_servers_to_monitor_mutex {};
	/**
	 * @brief Resulset containing the latest 'pgsql_servers' present in 'mydb'.
	 * @details This resulset should be updated via 'update_table_pgsql_servers_for_monitor' each time actions
	 *   that modify the 'pgsql_servers' table are performed.
	 */
	SQLite3_result* pgsql_servers_to_monitor;

	struct {
		unsigned int servers_table_version;
		pthread_mutex_t servers_table_version_lock;
		pthread_cond_t servers_table_version_cond;
		unsigned long client_connections_aborted;
		unsigned long client_connections_created;
		int client_connections;
		unsigned long server_connections_aborted;
		unsigned long server_connections_created;
		unsigned long server_connections_delayed;
		unsigned long server_connections_connected;
		unsigned long pgconnpoll_get;
		unsigned long pgconnpoll_get_ok;
		unsigned long pgconnpoll_get_ping;
		unsigned long pgconnpoll_push;
		unsigned long pgconnpoll_reset;
		unsigned long pgconnpoll_destroy;
		unsigned long long autocommit_cnt;
		unsigned long long commit_cnt;
		unsigned long long rollback_cnt;
		unsigned long long autocommit_cnt_filtered;
		unsigned long long commit_cnt_filtered;
		unsigned long long rollback_cnt_filtered;
		unsigned long long backend_reset_connection;
		//unsigned long long backend_init_db;
		unsigned long long backend_set_client_encoding;
		//unsigned long long frontend_init_db;
		unsigned long long frontend_set_client_encoding;
		//unsigned long long frontend_use_db;
		unsigned long long access_denied_wrong_password;
		unsigned long long access_denied_max_connections;
		unsigned long long access_denied_max_user_connections;
		unsigned long long select_for_update_or_equivalent;
		unsigned long long auto_increment_delay_multiplex;

#if POLARDB_PROXY
		// =====================================================================
		// PolarDB LSN tracking stats
		// =====================================================================
		// Keep this storage block explicit. PgSQL_PolarDB_Counters.h owns
		// counter metadata (per-thread entries, SQL/Prometheus names, and
		// worker-total merge lists), but this struct owns storage, comments, and
		// cache-line layout.
		// Do not replace these declarations with POLARDB_ALL_COUNTER_LIST(X).
		std::atomic<unsigned long long> polardb_server_lsn_updates_from_rfq{0}; // accepted current-group/current-epoch per-server LSN cache updates from query RFQ
		std::atomic<unsigned long long> polardb_lsn_updates_from_monitor{0}; // LSN advances seen by the monitor
		std::atomic<unsigned long long> polardb_monitor_health_invalid_role{0}; // monitor role is not routable, including PolarDB "unknown" for POLAR_UNKNOWN/POLAR_STANDALONE_DATAMAX
		std::atomic<unsigned long long> polardb_monitor_health_invalid_values{0}; // PolarDB monitor health row has invalid availability or LSN text
		std::atomic<unsigned long long> polardb_lsn_stale_count{0};          // stale-LSN skips while selecting a reader
		std::atomic<unsigned long long> polardb_write_missing_lsn{0};        // writer RFQ carried no LSN; automatic RYW reads forced to writer
		std::atomic<unsigned long long> polardb_read_missing_lsn{0};         // read RFQ carried no LSN while SESSION_LSN tracked observations
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_to_target{0}; // client RFQ LSN raised to a confirmed session/wait target
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_by_writer{0}; // client RFQ LSN raised because response used the writer
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_by_wait{0}; // client RFQ LSN raised because an LSN wait completed
		std::atomic<unsigned long long> polardb_primary_lsn_unknown{0};      // GLOBAL_LSN had no shared group-LSN observation
		std::atomic<unsigned long long> polardb_rfq_best_effort_degraded_routes{0}; // RFQ-unavailable reads sent to reader without wait
		std::atomic<unsigned long long> polardb_consistency_writer_fallback{0}; // consistency reads redirected to writer after reader selection failed
		std::atomic<unsigned long long> polardb_lag_cap_freshness_clamped{0}; // byte-lag cap reduced the allowed age of a cached reader LSN
		std::atomic<unsigned long long> polardb_lag_cap_lsn_unknown{0}; // lag-cap reader candidate had no cached LSN
		std::atomic<unsigned long long> polardb_lag_cap_lsn_stale{0}; // lag-cap reader candidate had a stale cached LSN
		std::atomic<unsigned long long> polardb_lag_cap_rejected{0}; // lag-cap reader candidate exceeded max_lag_bytes
		std::atomic<unsigned long long> polardb_lag_cap_accepted{0}; // lag-cap reader candidate passed max_lag_bytes
		std::atomic<unsigned long long> polardb_wait_reads_retried_on_writer{0}; // wait-wrapped reads retried once on the writer after reader failure policy
		std::atomic<unsigned long long> polardb_wait_retry_evaluated{0}; // wait-read failures that entered retry-to-writer decision handling
		std::atomic<unsigned long long> polardb_wait_retry_attempted{0}; // wait-read retries with a rebuilt packet ready for writer dispatch
		std::atomic<unsigned long long> polardb_wait_retry_declined_policy_forward{0}; // retry skipped because policy forwards reader error
		std::atomic<unsigned long long> polardb_wait_retry_declined_policy_terminate{0}; // retry skipped because policy terminates the session
		std::atomic<unsigned long long> polardb_wait_retry_declined_target_not_writer{0}; // retry skipped because target is not WRITER
		std::atomic<unsigned long long> polardb_wait_retry_declined_not_recoverable{0}; // retry skipped because failure class is not retryable
		std::atomic<unsigned long long> polardb_wait_retry_declined_result_started{0}; // retry skipped because user result transfer started
		std::atomic<unsigned long long> polardb_wait_retry_declined_writer_hg_unknown{0}; // retry skipped because writer HG is unknown
		std::atomic<unsigned long long> polardb_wait_retry_declined_original_query_missing{0}; // retry skipped because original query was unavailable
		std::atomic<unsigned long long> polardb_wait_retry_declined_writer_unavailable{0}; // retry skipped because writer backend stream was unavailable
		std::atomic<unsigned long long> polardb_wait_retry_declined_same_stream{0}; // retry skipped because writer stream equals failed reader stream
		std::atomic<unsigned long long> polardb_wait_retry_declined_writer_busy{0}; // retry skipped because writer stream was not idle
		std::atomic<unsigned long long> polardb_wait_retry_declined_packet_build_failed{0}; // retry skipped because packet rebuild failed
		std::atomic<unsigned long long> polardb_wait_retry_declined_move_failed{0}; // retry skipped because retry packet move failed
		std::atomic<unsigned long long> polardb_rfq_profile_skipped{0};      // incompatible pooled-backend skip attempts for RFQ-LSN reads
		std::atomic<unsigned long long> polardb_rfq_profile_evicted{0};      // incompatible free pooled backends evicted to create RFQ-LSN-capable replacements
		std::atomic<unsigned long long> polardb_target_lsn_preferred{0};     // reader choice narrowed to fresh cached LSN >= target
		std::atomic<unsigned long long> polardb_target_lsn_fallback_wait{0}; // no target-reached reader acquired; wrapper remains correctness check
		std::atomic<unsigned long long> polardb_reader_pool_hit{0};          // reads that got a connection from the reader pool
		std::atomic<unsigned long long> polardb_reader_pool_miss_empty{0};   // reader-pool lookups with no usable pooled reader
		std::atomic<unsigned long long> polardb_reader_pool_return_to_core{0}; // reusable reader connections returned to the core FREE list
		std::atomic<unsigned long long> polardb_reader_pool_p2c_select{0}; // reader-pool selections using P2C
		std::atomic<unsigned long long> polardb_reader_pool_p2c_second{0}; // P2C selected the second sampled reader
		std::atomic<unsigned long long> polardb_reader_pool_p2c_active_load{0}; // P2C selected by lower global active load
		std::atomic<unsigned long long> polardb_reader_pool_p2c_random{0}; // P2C selected by random tie-break
		std::atomic<unsigned long long> polardb_reader_pool_drop_offline{0}; // reader-pool readers dropped because server is offline
		std::atomic<unsigned long long> polardb_reader_pool_drop_unusable{0}; // reader-pool readers dropped because no longer reusable
		std::atomic<unsigned long long> polardb_reader_pool_drop_client_identity{0}; // reader-pool readers closed because CLIENT startup identity cannot be shared
		std::atomic<unsigned long long> polardb_reader_pool_lookup{0};       // reader-pool lookup attempts
		std::atomic<unsigned long long> polardb_reader_pool_current_state_retry{0}; // cold reader creations retried after topology or startup configuration changed
		std::atomic<unsigned long long> polardb_reader_pool_create_decision{0};
		std::atomic<unsigned long long> polardb_reader_pool_create_issued{0};
		std::atomic<unsigned long long> polardb_reader_pool_create_connected{0};
		std::atomic<unsigned long long> polardb_reader_pool_create_failed{0};
		std::atomic<unsigned long long> polardb_reader_pool_create_timeout{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_ping_take{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_ping_return{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_ping_destroy{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_deferred{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_cancelled{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_cancelled_taken{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_cancelled_retained{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_cancelled_other{0};
		std::atomic<unsigned long long> polardb_reader_pool_idle_trim_destroyed{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_enter{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_exit{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_max_us{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_pass{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_pass_deadline{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_pass_local{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_attempt{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_acquired{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_selected_busy{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_group_busy{0};
		std::atomic<unsigned long long> polardb_reader_capacity_retry_scope_skipped{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_le_1ms{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_le_5ms{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_le_20ms{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_le_100ms{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_le_1s{0};
		std::atomic<unsigned long long> polardb_reader_capacity_wait_gt_1s{0};
		std::atomic<unsigned long long> polardb_reader_claim_ownership_local{0};
		std::atomic<unsigned long long> polardb_reader_claim_ownership_active{0};
		std::atomic<unsigned long long> polardb_reader_claim_ownership_claim{0};
		std::atomic<unsigned long long> polardb_reader_claim_ownership_zero{0};
		std::atomic<unsigned long long> polardb_reader_ownership_lease_started{0};
		std::atomic<unsigned long long> polardb_reader_ownership_lease_released{0};
		std::atomic<unsigned long long> polardb_reader_ownership_lease_yielded{0};
		std::atomic<unsigned long long> polardb_reader_yield_debt_demand_observed{0};
		std::atomic<unsigned long long> polardb_reader_yield_debt_set{0};
		std::atomic<unsigned long long> polardb_reader_yield_debt_fulfilled{0};
		std::atomic<unsigned long long> polardb_reader_yield_debt_cancelled{0};
		std::atomic<unsigned long long> polardb_reader_claim_demand_registered{0};
		std::atomic<unsigned long long> polardb_reader_claim_demand_cancelled{0};
		std::atomic<unsigned long long> polardb_reader_claim_published{0};
		std::atomic<unsigned long long> polardb_reader_claim_acquired{0};
		std::atomic<unsigned long long> polardb_reader_claim_released{0};
		std::atomic<unsigned long long> polardb_reader_claim_wake{0};
		std::atomic<unsigned long long> polardb_reader_claim_wake_coalesced{0};
		std::atomic<unsigned long long> polardb_reader_claim_missing{0};
		std::atomic<unsigned long long> polardb_reader_claim_missing_retired{0};
		std::atomic<unsigned long long> polardb_reader_claim_missing_unknown{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_create_evict{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_idle_trim{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_max_age{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_offline{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_pool_drop{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_explicit{0};
		std::atomic<unsigned long long> polardb_reader_claim_retired_invalid{0};
		std::atomic<unsigned long long> polardb_reader_claim_demand_retired{0};
		std::atomic<unsigned long long> polardb_reader_pool_server_considered{0}; // reader servers considered by reader pool
		std::atomic<unsigned long long> polardb_reader_pool_server_skip_unusable{0}; // reader-pool server skipped by status, weight, or latency
		std::atomic<unsigned long long> polardb_reader_pool_match_attempt{0}; // exact connection attempts on an eligible reader
		std::atomic<unsigned long long> polardb_reader_pool_match_miss{0}; // eligible-reader attempts with no matching connection
		std::atomic<unsigned long long> polardb_reader_pool_conn_examined{0}; // reader-pool connections popped and validated
		std::atomic<unsigned long long> polardb_reader_pool_reject_bad_context{0}; // reader-pool connection missing required context
		std::atomic<unsigned long long> polardb_reader_pool_reject_profile{0}; // reader-pool connection startup profile mismatch
		std::atomic<unsigned long long> polardb_reader_pool_reject_auth{0}; // reader-pool connection user or database mismatch
		std::atomic<unsigned long long> polardb_reader_pool_reject_identity{0}; // reader-pool connection startup identity mismatch
		std::atomic<unsigned long long> polardb_reader_pool_reject_session_state{0}; // reader-pool connection session state mismatch
		std::atomic<unsigned long long> polardb_reader_pool_key_full_check{0}; // reader-pool reuse needed full core compatibility checks
		std::atomic<unsigned long long> polardb_reader_target_selected_lsn_unknown{0}; // selected wait reader had no LSN sample
		std::atomic<unsigned long long> polardb_reader_target_selected_lsn_stale{0}; // selected wait reader had a stale LSN sample
		std::atomic<unsigned long long> polardb_reader_target_gap_zero{0};    // selected wait reader was already at target
		std::atomic<unsigned long long> polardb_reader_target_gap_le_4kb{0};  // selected wait reader was less than 4KB behind target
		std::atomic<unsigned long long> polardb_reader_target_gap_le_64kb{0}; // selected wait reader was less than 64KB behind target
		std::atomic<unsigned long long> polardb_reader_target_gap_le_1mb{0};  // selected wait reader was less than 1MB behind target
		std::atomic<unsigned long long> polardb_reader_target_gap_le_16mb{0}; // selected wait reader was less than 16MB behind target
		std::atomic<unsigned long long> polardb_reader_target_gap_gt_16mb{0}; // selected wait reader was more than 16MB behind target
		std::atomic<unsigned long long> polardb_reader_target_selected_gap_samples{0}; // fresh selected-reader LSN samples
		std::atomic<unsigned long long> polardb_reader_target_selected_gap_sum_bytes{0}; // selected-reader target gap total
		std::atomic<unsigned long long> polardb_reader_target_selection_compared{0}; // selected LSN compared with best considered
		std::atomic<unsigned long long> polardb_reader_target_selection_behind_best{0}; // selected reader behind best considered
		std::atomic<unsigned long long> polardb_reader_target_selection_loss_bytes{0}; // extra selected-reader gap total
		std::atomic<unsigned long long> polardb_session_target_epoch_reset{0}; // session LSN targets/sticky flags cleared after writer group/epoch change
		std::atomic<unsigned long long> polardb_query_parser_init{0}; // PgSQL queries submitted to parser/digest initializer
		std::atomic<unsigned long long> polardb_query_parser_init_bytes{0}; // query bytes submitted to parser/digest initializer
		std::atomic<unsigned long long> polardb_query_parser_init_digest_enabled{0}; // parser initializations with query digests enabled
		std::atomic<unsigned long long> polardb_query_parser_init_commands_enabled{0}; // parser initializations with command stats enabled
		std::atomic<unsigned long long> polardb_query_parser_command_type{0}; // parser command-type classifications
		std::atomic<unsigned long long> polardb_query_parser_update{0}; // query-parser stat updates attempted
		std::atomic<unsigned long long> polardb_query_parser_update_skipped_none{0}; // parser updates skipped with no parser state
		std::atomic<unsigned long long> polardb_query_parser_update_skipped_uninitialized{0}; // parser updates skipped with uninitialized command
		std::atomic<unsigned long long> polardb_query_parser_update_with_digest{0}; // parser updates carrying digest text
		std::atomic<unsigned long long> polardb_result_process{0}; // PolarDB result-processing observations
		std::atomic<unsigned long long> polardb_result_process_write_classify{0}; // result-processing read/write classifications
		std::atomic<unsigned long long> polardb_result_process_write_classify_text{0}; // result classifications with query text
		std::atomic<unsigned long long> polardb_parent_bytes_flush_threshold_recv{0}; // parent byte flushes caused by recv threshold
		std::atomic<unsigned long long> polardb_parent_bytes_flush_threshold_sent{0}; // parent byte flushes caused by sent threshold
		std::atomic<unsigned long long> polardb_parent_bytes_flush_detach{0}; // parent byte flushes on backend detach
		std::atomic<unsigned long long> polardb_parent_bytes_flush_destructor{0}; // parent byte flushes on connection destruction
		std::atomic<unsigned long long> polardb_parent_bytes_flush_no_parent{0}; // pending parent bytes dropped because no server parent was attached
		std::atomic<unsigned long long> polardb_parent_bytes_flush_recv_atomic{0}; // shared parent recv atomics after coalescing
		std::atomic<unsigned long long> polardb_parent_bytes_flush_sent_atomic{0}; // shared parent sent atomics after coalescing
		std::atomic<unsigned long long> polardb_parent_bytes_flush_recv_bytes{0}; // recv bytes flushed to the shared parent counter
		std::atomic<unsigned long long> polardb_parent_bytes_flush_sent_bytes{0}; // sent bytes flushed to the shared parent counter
		std::atomic<unsigned long long> polardb_writev_attempts{0}; // plaintext PgSQL frontend direct scatter/gather send attempts
		std::atomic<unsigned long long> polardb_writev_bytes{0}; // bytes sent through the direct scatter/gather path
		std::atomic<unsigned long long> polardb_writev_packets{0}; // fully-sent packets consumed by the direct scatter/gather path
		std::atomic<unsigned long long> polardb_writev_short_writes{0}; // direct scatter/gather sends that wrote less than the built view
		std::atomic<unsigned long long> polardb_writev_wouldblock{0}; // direct scatter/gather sends returning EAGAIN/EWOULDBLOCK/EINTR
		std::atomic<unsigned long long> polardb_writev_errors{0}; // direct scatter/gather sends returning a hard error
		std::atomic<unsigned long long> polardb_writev_buffered_fallback{0}; // PgSQL frontend writes forced back to queueOUT buffering
		std::atomic<unsigned long long> polardb_writev_small_batch_fallback{0}; // PgSQL frontend writes kept on the buffered path because they fit in one queue buffer
		std::atomic<unsigned long long> polardb_output_coalesce_hold{0}; // incomplete streaming frontend output flushes deferred
		std::atomic<unsigned long long> polardb_output_coalesce_flush_budget{0}; // incomplete streaming output flushed after budget
		std::atomic<unsigned long long> polardb_output_coalesce_flush_complete{0}; // held streaming output flushed at result completion
		std::atomic<unsigned long long> polardb_output_coalesce_flush_backpressure{0}; // coalesce skipped due to pending output/socket state
		std::atomic<unsigned long long> polardb_output_coalesce_disabled{0}; // coalesce disabled observations
#if POLARDB_PERF_DEBUG
		std::atomic<unsigned long long> polardb_perf_writev_skip_disabled{0}; // write-shape diagnostic: runtime switch disabled
		std::atomic<unsigned long long> polardb_perf_writev_skip_inactive{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_encrypted{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_not_frontend{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_state{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_session{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_mirror{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_poll{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_no_packets{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_queue_pending{0};
		std::atomic<unsigned long long> polardb_perf_writev_skip_queue_partial{0};
		std::atomic<unsigned long long> polardb_perf_writev_view_build_calls{0};
		std::atomic<unsigned long long> polardb_perf_writev_view_build_packets{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_512{0}; // write-shape diagnostic: frontend write size buckets
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_1k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_2k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_4k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_8k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_16k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_32k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_le_64k{0};
		std::atomic<unsigned long long> polardb_perf_write_bytes_gt_64k{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_1{0}; // write-shape diagnostic: direct sendmsg iov-count buckets
		std::atomic<unsigned long long> polardb_perf_write_iov_2{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_3_4{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_5_8{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_9_16{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_17_32{0};
		std::atomic<unsigned long long> polardb_perf_write_iov_33_64{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_1{0}; // write-shape diagnostic: packets per direct send
		std::atomic<unsigned long long> polardb_perf_packets_per_send_2{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_3_4{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_5_8{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_9_16{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_17_32{0};
		std::atomic<unsigned long long> polardb_perf_packets_per_send_33_64{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_512{0}; // write-shape diagnostic: protocol packet size buckets
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_1k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_2k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_4k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_8k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_16k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_32k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_le_64k{0};
		std::atomic<unsigned long long> polardb_perf_packet_bytes_gt_64k{0};
		std::atomic<unsigned long long> polardb_perf_plain_send_calls{0};
		std::atomic<unsigned long long> polardb_perf_plain_send_bytes{0};
#endif // POLARDB_PERF_DEBUG
		std::atomic<unsigned long long> polardb_result_row_run_attempts{0}; // attempts to detach a pending backend DataRow run
		std::atomic<unsigned long long> polardb_result_row_run_used{0}; // backend DataRow runs forwarded as one packet
		std::atomic<unsigned long long> polardb_result_row_run_frames{0}; // DataRow frames forwarded through row-run fast-forward
		std::atomic<unsigned long long> polardb_result_row_run_bytes{0}; // bytes forwarded through row-run fast-forward
		std::atomic<unsigned long long> polardb_result_row_run_unavailable{0}; // row-run probe fell back to normal result handling
		std::atomic<unsigned long long> polardb_result_row_run_partial{0}; // row-run probe saw an incomplete DataRow frame
		std::atomic<unsigned long long> polardb_result_row_run_not_candidate{0}; // row-run skipped before detach because current libpq input is not DataRow

		// Wait wrapping / RYW routing counters.
		//
		// Counter meaning:
		//  - PolarDB_Session_LSN_Routing / polardb_session_lsn_routing and
		//    PolarDB_Global_LSN_Routing / polardb_global_lsn_routing count the
		//    route-plan decision: a read was sent to a reader with an LSN wait
		//    requirement for the corresponding consistency mode.
		//  - PolarDB_Wait_Wrap_Prepared / polardb_wait_wrap_prepared counts the
		//    wrapper preparation for that decision. Today these normally move
		//    together because every LSN-routing decision prepares exactly one LSN
		//    wait wrapper. They are kept separate so that if other wait kinds are
		//    added later (such as CSN or transaction-split waits) they can
		//    distinguish route intent from wrapper construction.
		//
		// Timeout counters follow total/subset semantics:
		//  - PolarDB_Wait_Error_Timeout / polardb_wait_error_timeout is the total
		//    wait-timeout counter.
		//  - PolarDB_Wait_Error_LSN_Wait_Timeout /
		//    polardb_wait_error_lsn_wait_timeout is the LSN-wait subset. LSN is
		//    currently the only implemented wait type, so total and subset are
		//    expected to be equal. They can diverge if other wait-timeout kinds
		//    are added later.
		std::atomic<unsigned long long> polardb_session_lsn_routing{0};      // reads routed to a reader with a session-LSN wait
		std::atomic<unsigned long long> polardb_global_lsn_routing{0};       // reads routed to a reader with a global-LSN wait
		std::atomic<unsigned long long> polardb_route_planner_total{0};      // requests examined by automatic PolarDB route planner
		std::atomic<unsigned long long> polardb_route_replica_eligible{0};   // planner inputs marked replica eligible by query rules
		std::atomic<unsigned long long> polardb_route_replica_ineligible{0}; // planner inputs not replica eligible: writes/control/manual defaults
		std::atomic<unsigned long long> polardb_route_to_reader{0};          // eligible planner decisions targeting a reader HG
		std::atomic<unsigned long long> polardb_route_to_writer{0};          // eligible planner decisions targeting writer HG
		std::atomic<unsigned long long> polardb_route_passthrough_rule_owned{0}; // eligible decisions left to normal query-rule routing
		std::atomic<unsigned long long> polardb_route_no_wait_target{0};     // eligible reader decisions needing no session-LSN wait target
		std::atomic<unsigned long long> polardb_route_wait_required{0};      // eligible reader decisions requiring backend wait enforcement
		std::atomic<unsigned long long> polardb_route_txn_split_planned{0};  // eligible in-transaction reads planned for split
		std::atomic<unsigned long long> polardb_route_txn_wait_planned{0};   // pre-write txn reads planned for temporary reader wait
		std::atomic<unsigned long long> polardb_txn_wait_reader_reconciled{0}; // stale temporary reader ownership restored at request entry
		std::atomic<unsigned long long> polardb_route_manual_total{0};       // normal query-rule/sticky routes that bypass automatic planning
		std::atomic<unsigned long long> polardb_route_manual_to_reader{0};   // manual routes whose effective HG is a PolarDB reader
		std::atomic<unsigned long long> polardb_route_manual_to_writer{0};   // manual routes whose effective HG is a PolarDB writer
		std::atomic<unsigned long long> polardb_route_manual_other{0};       // manual routes outside known PolarDB reader/writer HGs
		std::atomic<unsigned long long> polardb_route_manual_forced_writer{0}; // manual route overridden by reader-failure safety handling
		std::atomic<unsigned long long> polardb_route_locked_hostgroup{0};   // explicit locked_on_hostgroup routes that bypass automatic planning
		std::atomic<unsigned long long> polardb_wait_wrap_prepared{0};        // wait wrapper intent prepared (REPLICA_WITH_WAIT)
		std::atomic<unsigned long long> polardb_wait_wrap_bypassed{0};       // selected reader reached consistency target -> wrapper skipped
		std::atomic<unsigned long long> polardb_wait_lsn_sent{0};             // LSN wait wrapper successfully installed/sent
		std::atomic<unsigned long long> polardb_wait_lsn_sum_us{0};          // total time spent in LSN waits (microseconds)
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_1ms{0};  // wait wrapper elapsed <= 1ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_5ms{0};  // wait wrapper elapsed <= 5ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_10ms{0}; // wait wrapper elapsed <= 10ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_50ms{0}; // wait wrapper elapsed <= 50ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_100ms{0}; // wait wrapper elapsed <= 100ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_500ms{0}; // wait wrapper elapsed <= 500ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_1s{0};   // wait wrapper elapsed <= 1s
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_gt_1s{0};   // wait wrapper elapsed > 1s
#if POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_wait_wrap_build_sum_us{0};   // wait wrapper SQL build latency total
		std::atomic<unsigned long long> polardb_wait_wrap_build_count{0};    // wait wrapper SQL build latency samples
		std::atomic<unsigned long long> polardb_wait_wrap_install_sum_us{0}; // wait wrapper packet install latency total
		std::atomic<unsigned long long> polardb_wait_wrap_install_count{0};  // wait wrapper packet install latency samples
		std::atomic<unsigned long long> polardb_wait_target_lsn_cache_advanced{0}; // successful waits that advanced selected-reader LSN cache
		std::atomic<unsigned long long> polardb_wait_target_lsn_cache_rejected{0}; // successful waits whose selected-reader LSN update was rejected
#endif // POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_wait_wrap_safety_abort{0};   // wrap build failed -> wait aborted
		std::atomic<unsigned long long> polardb_wait_error_timeout{0};       // wait-timeout notices accounted
		std::atomic<unsigned long long> polardb_wait_error_lsn_wait_timeout{0}; // LSN wait-timeout notices accounted
		std::atomic<unsigned long long> polardb_wait_error_connection_lost{0}; // wait-wrapped reader lost its backend connection
		std::atomic<unsigned long long> polardb_queries_in_splittable_txn{0}; // queries planned while txn split evidence is usable
		std::atomic<unsigned long long> polardb_queries_split_eligible{0};    // in-txn reads that passed split checks
		std::atomic<unsigned long long> polardb_xids_received{0};             // primary RFQs with transaction XIDs
		std::atomic<unsigned long long> polardb_txn_became_splittable{0};     // txn entered split-readable state
		std::atomic<unsigned long long> polardb_txn_lost_splittable{0};       // txn lost split-readable state
		std::atomic<unsigned long long> polardb_txn_committed_with_split{0};  // txn committed after a split read
		std::atomic<unsigned long long> polardb_txn_committed_no_split{0};    // split-readable txn committed without a split read
		std::atomic<unsigned long long> polardb_split_reads_total{0};         // split reads attempted
		std::atomic<unsigned long long> polardb_split_reads_success{0};       // split reads completed on replica
		std::atomic<unsigned long long> polardb_split_reads_fallback{0};      // split reads never dispatched to replica and ran on primary
		std::atomic<unsigned long long> polardb_split_fallback_reader_unavailable{0}; // split fallback: no usable reader server
		std::atomic<unsigned long long> polardb_split_fallback_reader_busy{0}; // split fallback: readers had no available pooled match
		std::atomic<unsigned long long> polardb_split_fallback_rfq_unavailable{0}; // split fallback: no RFQ-LSN-capable reader backend
		std::atomic<unsigned long long> polardb_split_fallback_primary_lsn_unknown{0}; // split fallback: lag cap had no primary LSN sample
		std::atomic<unsigned long long> polardb_split_fallback_reader_lsn_unknown{0}; // split fallback: lag cap had no reader LSN sample
		std::atomic<unsigned long long> polardb_split_fallback_reader_lsn_stale{0}; // split fallback: reader LSN sample was stale
		std::atomic<unsigned long long> polardb_split_fallback_reader_lag_exceeded{0}; // split fallback: byte lag exceeded max_lag_bytes
		std::atomic<unsigned long long> polardb_split_reads_retried{0};       // split reader failures redispatched on writer
		std::atomic<unsigned long long> polardb_split_reads_retried_on_reader{0}; // split reader failures redispatched on another replica
		std::atomic<unsigned long long> polardb_split_reads_forwarded{0};     // split reader failures forwarded while txn stays on writer
		std::atomic<unsigned long long> polardb_split_reads_error{0};         // split reads ended through an error path
		std::atomic<unsigned long long> polardb_reader_terminations{0};       // reader failures that closed the client session
		std::atomic<unsigned long long> polardb_split_rejected_multistatement{0}; // split candidates rejected as multi-statement
		std::atomic<unsigned long long> polardb_split_rejected_not_select{0}; // split candidates rejected by non-SELECT shape
		std::atomic<unsigned long long> polardb_split_rejected_for_update{0}; // split candidates rejected by locking SELECT shape
		std::atomic<unsigned long long> polardb_split_rejected_write_lsn_unknown{0}; // split candidates blocked by missing write LSN
		std::atomic<unsigned long long> polardb_split_rejected_observed_lsn_unknown{0}; // split candidates blocked by missing observed LSN
		std::atomic<unsigned long long> polardb_split_wal_pending{0};         // primary RFQ still has pending WAL
		std::atomic<unsigned long long> polardb_split_invariant_violations{0}; // unexpected transaction-split state
		std::atomic<unsigned long long> polardb_split_blocked_reads{0};       // transaction already blocked from further split reads
		std::atomic<unsigned long long> polardb_split_no_backend{0};          // no split replica backend available
		std::atomic<unsigned long long> polardb_split_send_failed{0};         // split wrapped query send failed
		std::atomic<unsigned long long> polardb_split_pool_hit{0};            // acquired an existing pooled split connection
		std::atomic<unsigned long long> polardb_split_pool_empty{0};          // no pooled split connection existed
		std::atomic<unsigned long long> polardb_split_pool_contention{0};     // no usable pooled split connection was available
		std::atomic<unsigned long long> polardb_split_pool_miss_claimed_exact{0}; // exact-compatible split capacity was reserved by a claim
		std::atomic<unsigned long long> polardb_split_conn_reused{0};         // reused already attached split backend
		std::atomic<unsigned long long> polardb_split_conn_cleanup_success{0}; // split backend returned to pool
		std::atomic<unsigned long long> polardb_split_conn_cleanup_failed{0}; // split backend destroyed instead of pooled
		std::atomic<unsigned long long> polardb_split_conn_cleanup_no_reuse_requested{0}; // caller requested split backend destruction
		std::atomic<unsigned long long> polardb_split_conn_cleanup_not_reusable{0}; // split backend was already marked non-reusable
		std::atomic<unsigned long long> polardb_split_conn_cleanup_not_idle{0}; // split backend async state was not idle
		std::atomic<unsigned long long> polardb_split_conn_cleanup_active_txn{0}; // split backend still had an active transaction
		std::atomic<unsigned long long> polardb_split_conn_cleanup_recovery_attempt{0}; // cleanup tried to recover a non-idle split backend
		std::atomic<unsigned long long> polardb_split_conn_cleanup_recovery_terminal{0}; // recovery found a completed backend result
		std::atomic<unsigned long long> polardb_split_conn_cleanup_recovery_timeout_state{0}; // recovery rejected a timed-out backend
		std::atomic<unsigned long long> polardb_split_conn_cleanup_recovery_busy_state{0}; // recovery rejected a still-busy backend
		std::atomic<unsigned long long> polardb_split_conn_cleanup_normalized{0}; // cleanup cleared a completed result
		std::atomic<unsigned long long> polardb_split_conn_cleanup_recovered{0}; // recovered split backend returned to the pool
#if POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_reader_acquire_sum_us{0};     // RFQ-aware reader acquisition latency total
		std::atomic<unsigned long long> polardb_reader_acquire_count{0};      // RFQ-aware reader acquisition latency samples
		std::atomic<unsigned long long> polardb_selected_server_pool_lock_wait_sum_us{0}; // selected-server pool-lock wait total
		std::atomic<unsigned long long> polardb_selected_server_pool_lock_wait_count{0}; // selected-server pool-lock wait samples
		std::atomic<unsigned long long> polardb_selected_server_pool_lock_hold_sum_us{0}; // selected-server pool-lock hold total
		std::atomic<unsigned long long> polardb_selected_server_pool_lock_hold_count{0}; // selected-server pool-lock hold samples
		std::atomic<unsigned long long> polardb_reader_target_ready_candidate{0}; // fresh reader candidates already at target
		std::atomic<unsigned long long> polardb_reader_target_no_ready_candidate{0}; // targeted acquisitions with no ready reader
		std::atomic<unsigned long long> polardb_reader_target_both_behind_compared{0}; // compared two fresh below-target readers
		std::atomic<unsigned long long> polardb_reader_target_both_behind_equal_lsn{0}; // both fresh below-target readers had the same LSN
		std::atomic<unsigned long long> polardb_reader_target_fresher_less_loaded{0}; // fresher reader had lower normalized load
		std::atomic<unsigned long long> polardb_reader_target_fresher_equal_loaded{0}; // fresher reader had equal normalized load
		std::atomic<unsigned long long> polardb_reader_target_fresher_more_loaded{0}; // fresher reader had higher normalized load
		std::atomic<unsigned long long> polardb_reader_target_fresher_exact_switch{0}; // exact-freshest changed weighted selection
		std::atomic<unsigned long long> polardb_reader_target_fresher_dominance_switch{0}; // strict dominance changed weighted selection
		std::atomic<unsigned long long> polardb_reader_target_lsn_unknown{0}; // targeted candidates with unknown reader LSN
		std::atomic<unsigned long long> polardb_reader_target_lsn_stale{0}; // targeted candidates with stale reader LSN
		std::atomic<unsigned long long> polardb_reader_target_lsn_behind{0}; // fresh targeted candidates behind target
		std::atomic<unsigned long long> polardb_reader_target_lag_cap_reject{0}; // targeted candidates rejected by byte-lag cap
		std::atomic<unsigned long long> polardb_reader_target_rfq_unavailable{0}; // targeted attempts rejected by RFQ profile policy
		std::atomic<unsigned long long> polardb_reader_target_rfq_no_protocol{0}; // effective protocol does not request RFQ LSN
		std::atomic<unsigned long long> polardb_reader_target_rfq_no_client_context{0}; // missing frontend user or startup identity
		std::atomic<unsigned long long> polardb_reader_target_rfq_candidate_profile_mismatch{0}; // pooled candidate lacks required RFQ startup bits
		std::atomic<unsigned long long> polardb_reader_target_rfq_candidate_identity_mismatch{0}; // pooled candidate startup identity differs
		std::atomic<unsigned long long> polardb_reader_target_rfq_candidate_auth_mismatch{0}; // pooled candidate user or database differs
		std::atomic<unsigned long long> polardb_reader_target_rfq_unavailable_profile_mismatch{0}; // failed acquisition saw only RFQ-profile-incompatible candidates
		std::atomic<unsigned long long> polardb_reader_target_rfq_unavailable_identity_mismatch{0}; // failed acquisition saw only identity-incompatible candidates
		std::atomic<unsigned long long> polardb_reader_target_rfq_unavailable_auth_mismatch{0}; // failed acquisition saw only auth-incompatible candidates
		std::atomic<unsigned long long> polardb_rfq_requested_missing_payload{0}; // RFQ-LSN startup connection returned RFQ with no LSN payload
		std::atomic<unsigned long long> polardb_rfq_requested_zero_payload{0}; // RFQ-LSN startup connection returned RFQ with zero LSN payload
		std::atomic<unsigned long long> polardb_client_rfq_lsn_missing_with_target{0}; // client requested RFQ LSN, but backend payload was absent with a target
		std::atomic<unsigned long long> polardb_split_prepare_sum_us{0};      // split prepare latency total
		std::atomic<unsigned long long> polardb_split_prepare_count{0};       // split prepare latency samples
		std::atomic<unsigned long long> polardb_split_reader_acquire_sum_us{0}; // split reader acquisition latency total
		std::atomic<unsigned long long> polardb_split_reader_acquire_count{0}; // split reader acquisition latency samples
		std::atomic<unsigned long long> polardb_split_wrapper_build_sum_us{0}; // split wrapper build latency total
		std::atomic<unsigned long long> polardb_split_wrapper_build_count{0}; // split wrapper build latency samples
#endif // POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_split_lsn_wait_count{0};      // split LSN wait wrappers prepared
		std::atomic<unsigned long long> polardb_split_lsn_wait_sum_us{0};     // split LSN wait latency total
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_1ms{0}; // split wait wrapper elapsed <= 1ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_5ms{0}; // split wait wrapper elapsed <= 5ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_10ms{0}; // split wait wrapper elapsed <= 10ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_50ms{0}; // split wait wrapper elapsed <= 50ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_100ms{0}; // split wait wrapper elapsed <= 100ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_500ms{0}; // split wait wrapper elapsed <= 500ms
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_le_1s{0}; // split wait wrapper elapsed <= 1s
		std::atomic<unsigned long long> polardb_split_lsn_wait_elapsed_gt_1s{0}; // split wait wrapper elapsed > 1s
		std::atomic<unsigned long long> polardb_split_error_connection_lost{0}; // split replica connection loss
		std::atomic<unsigned long long> polardb_split_error_query_failed{0};  // split user-query failure
		std::atomic<unsigned long long> polardb_split_error_timeout{0};       // split timeout total
		std::atomic<unsigned long long> polardb_split_error_lsn_wait_timeout{0}; // split LSN timeout subset
		std::atomic<unsigned long long> polardb_split_latency_sum_us{0};      // split read latency total
		std::atomic<unsigned long long> polardb_split_latency_count{0};       // split read latency samples
		std::atomic<unsigned long long> polardb_split_warmup_requested{0};    // lazy warmup base requests queued
		std::atomic<unsigned long long> polardb_split_warmup_target_attempts{0}; // target backend connect attempts produced by warmup
		std::atomic<unsigned long long> polardb_split_warmup_created{0};      // lazy warmup connections created
		std::atomic<unsigned long long> polardb_split_warmup_claimed_on_publish{0}; // warmup connection immediately assigned to a pending claim
		std::atomic<unsigned long long> polardb_split_warmup_failed{0};       // lazy warmup base requests rejected before target selection
		std::atomic<unsigned long long> polardb_split_warmup_target_failed{0}; // lazy warmup target backends failed
		std::atomic<unsigned long long> polardb_split_warmup_already_warm{0}; // base request skipped because compatible free backend exists
		std::atomic<unsigned long long> polardb_split_warmup_dedup_queued{0}; // warmup deduped against queued request
		std::atomic<unsigned long long> polardb_split_warmup_dedup_inflight{0}; // warmup deduped against in-flight request
		std::atomic<unsigned long long> polardb_split_warmup_queue_full{0};   // warmup request dropped by queue limit
		std::atomic<unsigned long long> polardb_split_warmup_no_target{0};    // base request found no eligible target reader
		std::atomic<unsigned long long> polardb_split_warmup_bad_request{0};  // warmup request lacked required identity/config
		std::atomic<unsigned long long> polardb_split_warmup_rfq_unavailable{0}; // warmup skipped because reader HG lacks RFQ LSN
		std::atomic<unsigned long long> polardb_split_warmup_connect_failed{0}; // warmup connection handshake failed
		std::atomic<unsigned long long> polardb_split_warmup_add_failed{0}; // warmup connected but could not be added to the pool
#if POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_confirmed_saturated{0};
		std::atomic<unsigned long long> polardb_reader_pool_hgm_create_lock_entry{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_accepted{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_rejected{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_lock_wait_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_lock_hold_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_store_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_store_accepted{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_store_rejected{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_return_to_shared{0};
		std::atomic<unsigned long long> polardb_split_warmup_queue_delay_sum_us{0}; // warmup queued-before-drain latency total
		std::atomic<unsigned long long> polardb_split_warmup_queue_delay_count{0}; // warmup queue-delay samples
		std::atomic<unsigned long long> polardb_split_warmup_connect_sum_us{0}; // warmup backend connect latency total
		std::atomic<unsigned long long> polardb_split_warmup_connect_count{0}; // warmup backend connect latency samples
		std::atomic<unsigned long long> polardb_split_warmup_add_sum_us{0}; // total time spent adding warm connections to the pool
		std::atomic<unsigned long long> polardb_split_warmup_add_count{0}; // warm connection add-time samples
		std::atomic<unsigned long long> polardb_idle_ping_pool_maintenance_sum_us{0}; // idle ping pool maintenance time
		std::atomic<unsigned long long> polardb_idle_ping_pool_maintenance_count{0}; // idle ping pool maintenance samples
#endif // POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_split_warmup_sum_us{0};       // request-to-pool warmup latency total
		std::atomic<unsigned long long> polardb_split_warmup_count{0};        // request-to-pool warmup samples
		std::atomic<unsigned long long> polardb_warmup_pending{0};            // queued lazy warmup requests

		// Fast condition read on every routing decision; true while any PolarDB
		// hostgroup is configured. Set from the current snapshot on each commit.
		// Own cache line so the hot read does not false-share with the counters above.
		alignas(64) std::atomic<bool> polardb_active{false};  // true when polardb_hostgroups_ is non-empty
#endif // POLARDB_PROXY

		//////////////////////////////////////////////////////
		///              Prometheus Metrics                ///
		//////////////////////////////////////////////////////

		/// Prometheus metrics arrays
		std::array<prometheus::Counter*, PgSQL_p_hg_counter::SIZE_> p_counter_array {};
		std::array<prometheus::Gauge*, PgSQL_p_hg_gauge::SIZE_> p_gauge_array {};

		// Prometheus dyn_metrics families arrays
		std::array<prometheus::Family<prometheus::Counter>*, PgSQL_p_hg_dyn_counter::SIZE_> p_dyn_counter_array {};
		std::array<prometheus::Family<prometheus::Gauge>*, PgSQL_p_hg_dyn_gauge::SIZE_> p_dyn_gauge_array {};

		/// Prometheus connection_pool metrics
		std::map<std::string, prometheus::Counter*> p_conn_pool_bytes_data_recv_map {};
		std::map<std::string, prometheus::Counter*> p_conn_pool_bytes_data_sent_map {};
		std::map<std::string, prometheus::Counter*> p_connection_pool_conn_err_map {};
		std::map<std::string, prometheus::Gauge*> p_connection_pool_conn_free_map {};
		std::map<std::string, prometheus::Counter*> p_connection_pool_conn_ok_map {};
		std::map<std::string, prometheus::Gauge*> p_connection_pool_conn_used_map {};
		std::map<std::string, prometheus::Gauge*> p_connection_pool_latency_us_map {};
		std::map<std::string, prometheus::Counter*> p_connection_pool_queries_map {};
		std::map<std::string, prometheus::Gauge*> p_connection_pool_status_map {};

		/// Prometheus gtid_executed metrics
		std::map<std::string, prometheus::Counter*> p_gtid_executed_map {};

		/// Prometheus pgsql_error metrics
		std::map<std::string, prometheus::Counter*> p_pgsql_errors_map {};

		//////////////////////////////////////////////////////
	} status;
	/**
	 * @brief Update the module prometheus metrics.
	 */
	void p_update_metrics();
	/**
	 * @brief Updates the 'pgsql_error' counter identified by the 'm_id' parameter,
	 * or creates a new one in case of not existing.
	 *
	 * @param hid The hostgroup identifier.
	 * @param address The connection address that triggered the error.
	 * @param port The port of the connection that triggered the error.
	 * @param errno The error code itself.
	 */
	void p_update_pgsql_error_counter(p_pgsql_error_type err_type, unsigned int hid, char* address, uint16_t port, unsigned int code);

	wqueue<PgSQL_Connection *> queue;

	PgSQL_HostGroups_Manager();
	~PgSQL_HostGroups_Manager();
	void init();
#if POLARDB_PROXY
	void polardb_fast_topology_wrlock();
	void polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
	//void wrlock();
	//void wrunlock();
	int servers_add(SQLite3_result *resultset);
	/**
	 * @brief Generates a new global checksum for module 'pgsql_servers_v2' using the provided hash.
	 * @param servers_v2_hash The 'raw_checksum' from 'PGHGM_GEN_CLUSTER_ADMIN_PGSQL_SERVERS' or peer node.
	 * @return Checksum computed using the provided hash, and 'pgsql_servers' config tables hashes.
	 */
	std::string gen_global_pgsql_servers_v2_checksum(uint64_t servers_v2_hash);
	bool commit(
		const peer_runtime_pgsql_servers_t& peer_runtime_pgsql_servers = {},
		const peer_pgsql_servers_v2_t& peer_pgsql_servers_v2 = {},
		bool only_commit_runtime_pgsql_servers = true,
		bool update_version = false
	);
	/**
	 * @brief Extracted from 'commit'. Performs the following actions:
	 *  1. Re-generates the 'myhgm.pgsql_servers' table.
	 *  2. If supplied 'runtime_pgsql_servers' is 'nullptr':
	 *  	1. Gets the contents of the table via 'PGHGM_GEN_CLUSTER_ADMIN_RUNTIME_SERVERS'.
	 *  	2. Save the resultset into 'this->runtime_pgsql_servers'.
	 *  3. If supplied 'runtime_pgsql_servers' isn't 'nullptr':
	 *  	1. Updates the 'this->runtime_pgsql_servers' with it.
	 *  4. Updates 'HGM_TABLES::PgSQL_SERVERS' with raw checksum from 'this->runtime_pgsql_servers'.
	 * @param runtime_pgsql_servers If not 'nullptr', used to update 'this->runtime_pgsql_servers'.
	 * @return The updated 'PgSQL_HostGroups_Manager::runtime_pgsql_servers'.
	 */
	uint64_t commit_update_checksum_from_pgsql_servers(SQLite3_result* runtime_pgsql_servers = nullptr);
	/**
	 * @brief Analogous to 'commit_generate_pgsql_servers_table' but for 'incoming_pgsql_servers_v2'.
	 */
	uint64_t commit_update_checksum_from_pgsql_servers_v2(SQLite3_result* incoming_pgsql_servers_v2 = nullptr);
	/**
	 * @brief Update all HGM_TABLES checksums and uses them to update the supplied SpookyHash.
	 * @details Checksums are the checksums for the following tables:
	 *  - pgsql_replication_hostgroups
	 *  - pgsql_hostgroup_attributes
	 *
	 *  These checksums are used to compute the global checksum for 'pgsql_servers_v2'.
	 * @param myhash SpookyHash to be updated with all the computed checksums.
	 * @param init Indicates if the SpookyHash checksum is initialized.
	 */
	void commit_update_checksums_from_tables(SpookyHash& myhash, bool& init);
	/**
	 * @brief Performs the following actions:
	 *  1. Gets the current contents of table 'myhgm.TableName', using 'ColumnName' ordering.
	 *  2. Computes the checksum for that resultset.
	 *  3. Updates the supplied 'raw_checksum' and the supplied 'SpookyHash' with it.
	 * @details Stands for 'commit_update_checksum_from_table_1'.
	 * @param myhash Hash to be updated with the resultset checksum from the selected table.
	 * @param init If the supplied 'SpookyHash' has already being initialized.
	 * @param TableName The tablename from which to obtain the resultset for the 'raw_checksum' computation.
	 * @param ColumnName A column name to use for ordering in the supplied 'TableName'.
	 * @param raw_checksum A 'raw_checksum' to be updated with the obtained resultset.
	 */
	void CUCFT1(
		SpookyHash& myhash, bool& init, const string& TableName, const string& ColumnName, uint64_t& raw_checksum
	);
	/**
	 * @brief Store the resultset for the 'runtime_pgsql_servers' table set that have been loaded to runtime.
	 *  The store configuration is later used by Cluster to propagate current config.
	 * @param The resulset to be stored replacing the current one.
	 */
	void save_runtime_pgsql_servers(SQLite3_result *);

	/**
	 * @brief Store the resultset for the 'pgsql_servers_v2' table.
	 *  The store configuration is later used by Cluster to propagate current config.
	 * @param The resulset to be stored replacing the current one.
	 */
	void save_pgsql_servers_v2(SQLite3_result* s);

	/**
	 * @brief These setters/getter functions store and retrieve the currently hold resultset for the
	 *  'incoming_*' table set that have been loaded to runtime. The store configuration is later used by
	 *  Cluster to propagate current config.
	 * @param The resulset to be stored replacing the current one.
	 */

	void save_incoming_pgsql_table(SQLite3_result *, const string&);
	SQLite3_result* get_current_pgsql_table(const string& name);

	//SQLite3_result * execute_query(char *query, char **error);
	/**
	 * @brief Creates a resultset with the current full content of the target table.
	 * @param string The target table. Valid values are:
	 *   - "pgsql_replication_hostgroups"
	 *   - "pgsql_hostgroup_attributes"
	 *   - "pgsql_servers"
	 *   - "cluster_pgsql_servers"
	 *   When targeting 'pgsql_servers' table is purged and regenerated.
	 * @return The generated resultset.
	 */
	SQLite3_result* dump_table_pgsql(const string&);
	PgSQLServers_SslParams * get_Server_SSL_Params(char *hostname, int port, char *username);

	/**
	 * @brief Update the public member resulset 'pgsql_servers_to_monitor'. This resulset should contain the latest
	 *   'pgsql_servers' present in 'PgSQL_HostGroups_Manager' db, which are not 'OFFLINE_HARD'. The resulset
	 *   fields match the definition of 'monitor_internal.pgsql_servers' table.
	 * @details Several details:
	 *   - Function assumes that 'pgsql_servers' table from 'PgSQL_HostGroups_Manager' db is ready
	 *     to be consumed, because of this it doesn't perform any of the following operations:
	 *       - Purging 'pgsql_servers' table.
	 *       - Regenerating 'pgsql_servers' table.
	 *   - Function locks on 'pgsql_servers_to_monitor_mutex'.
	 * @param lock When supplied the function calls 'wrlock()' and 'wrunlock()' functions for accessing the db.
	 */
	void update_table_pgsql_servers_for_monitor(bool lock=false);
	
	void MyConn_add_to_pool(PgSQL_Connection *);
	/**
	 * @brief Creates a new server in the target hostgroup if isn't already present.
	 * @details If the server is found already in the target hostgroup, no action is taken, unless its status
	 *   is 'OFFLINE_HARD'. In case of finding it as 'OFFLINE_HARD':
	 *     1. Server hostgroup attributes are reset to known values, so they can be updated.
	 *     2. Server attributes are updated to either table definition values, or hostgroup 'servers_defaults'.
	 *     3. Server is bring back as 'ONLINE'.
	 * @param hid The hostgroup in which the server is to be created (or to bring it back as 'ONLINE').
	 * @param srv_info Basic server info to be used during creation.
	 * @param srv_opts Server creation options.
	 * @return 0 in case of success, -1 in case of failure.
	 */
	int create_new_server_in_hg(uint32_t hid, const PgSQL_srv_info_t& srv_info, const PgSQL_srv_opts_t& srv_opts);
	/**
	 * @brief Completely removes server from the target hostgroup if found.
	 * @details Several actions are taken if server is found:
	 *   - Set the server as 'OFFLINE_HARD'.
	 *   - Drop all current FREE connections to the server.
	 *   - Delete the server from the 'myhgm.pgsql_servers' table.
	 *
	 *   This later step is not required if the caller is already going to perform a full deletion of the
	 *   servers in the target hostgroup. Which is a common operation during table regeneration.
	 * @param hid Target hostgroup id.
	 * @param addr Target server address.
	 * @param port Target server port.
	 * @return 0 in case of success, -1 in case of failure.
	 */
	int remove_server_in_hg(uint32_t hid, const string& addr, uint16_t port);

	PgSQL_Connection * get_MyConn_from_pool(unsigned int hid, PgSQL_Session *sess, bool ff, char * gtid_uuid, uint64_t gtid_trxid, int max_lag_ms, bool only_pooled = false);
#if POLARDB_PROXY
	PgSQL_PoolGetResult get_connection_from_selected_server(
		PgSQL_SrvC* srv, unsigned int expected_hostgroup_id,
		const PgSQL_PoolMatchKey& match_key,
		PgSQL_Session* sess, PgSQL_PoolGetMode mode,
		unsigned int selected_max_connections = 0,
		uint64_t expected_server_list_generation = 0,
		uint64_t expected_startup_config_generation = 0);
#endif // POLARDB_PROXY

	void drop_all_idle_connections();
	int get_multiple_idle_connections(int, unsigned long long, PgSQL_Connection **, int);
	SQLite3_result * SQL3_Connection_Pool(bool _reset, int *hid = nullptr);
	SQLite3_result * SQL3_Free_Connections();

	void push_MyConn_to_pool(PgSQL_Connection *, bool _lock=true);
	void push_MyConn_to_pool_array(PgSQL_Connection **, unsigned int);
	bool return_connection_with_match_key(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey* known_match_key = nullptr,
		PgSQL_Connection** detached_connection = nullptr);
#if POLARDB_PROXY
	PolarDB_ReaderYieldResult yield_connection_to_reader_claim(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& expected_match_key,
		unsigned int donor_worker_index);
	void return_polardb_reader_connections(
		PgSQL_Thread* thread,
		const std::vector<PgSQL_Connection*>& connections,
		std::vector<PgSQL_Connection*>& detached_connections);
#endif // POLARDB_PROXY
#if POLARDB_PROXY
	PolarDB_ReaderLocalReturn polardb_reader_local_return_decision(
		PgSQL_Connection* conn);
#endif // POLARDB_PROXY
	void destroy_MyConn_from_pool(PgSQL_Connection *, bool _lock=true);	

	void replication_lag_action_inner(PgSQL_HGC *, const char*, unsigned int, int);
	void replication_lag_action(const std::list<replication_lag_server_t>& pgsql_servers);
//	void read_only_action(char *hostname, int port, int read_only);
	void read_only_action_v2(const std::list<read_only_server_t>& pgsql_servers, bool writer_is_also_reader);
	unsigned int get_servers_table_version();
	void wait_servers_table_version(unsigned, unsigned);
	bool shun_and_killall(char *hostname, int port);
	void set_server_current_latency_us(char *hostname, int port, unsigned int _current_latency_us);
	unsigned long long Get_Memory_Stats();

	SQLite3_result *SQL3_Get_ConnPool_Stats();
	void increase_reset_counter();

	void add_pgsql_errors(int hostgroup, const char* hostname, int port, const char* username, const char* address,
		const char* dbname, const char* sqlstate, const char* errmsg);
	std::unique_ptr<SQLite3_result> get_pgsql_errors(bool);

	void shutdown();
	void unshun_server_all_hostgroups(const char * address, uint16_t port, time_t t, int max_wait_sec, unsigned int *skip_hid);
	PgSQL_SrvC* find_server_in_hg(unsigned int _hid, const std::string& addr, int port);

#if POLARDB_PROXY
	// =========================================================================
	// PolarDB LSN session-consistency support
	// =========================================================================
	// Topology helpers (writer/reader pairing) + the per-server LSN cache the
	// monitor feeds. The HG policy/topology cache itself is populated by the
	// admin loader. Until configuration is loaded these accessors fail safe
	// (polardb_active is false, so they return "not configured" / 0).
	//
	// Safe reload and query reads:
	// generate_pgsql_replication_hostgroups_table() builds a complete plain-value
	// topology/policy snapshot under the existing commit lock, then replaces the
	// current snapshot atomically and advances its generation. Query threads refresh a thread-local cached
	// shared_ptr only when the generation changes, so polardb_collect() and
	// attach-time PolarDB checks do not take the HGM global lock in steady state.
	// The snapshot deliberately contains no PgSQL_HGC/PgSQL_SrvC pointers.

	/**
	 * @brief Map a reader hostgroup to its writer hostgroup.
	 * @return writer hostgroup id, or -1 if not configured.
	 */
	int get_writer_hostgroup_for_reader(unsigned int reader_hostgroup_id);

	/**
	 * @brief Map a writer hostgroup to its reader hostgroup.
	 * @return reader hostgroup id, or -1 if not configured.
	 */
	int get_reader_hostgroup_for_writer(unsigned int writer_hostgroup_id);

	/**
	 * @brief Whether a hostgroup is part of a PolarDB replication configuration.
	 */
	bool is_polardb_hostgroup(unsigned int hostgroup_id);

	/**
	 * @brief Resolved PolarDB policy for a hostgroup (tri-state values).
	 * -1 = not configured (inherit global), 0 = explicitly disabled, >0 = explicit value.
	*/
	struct PolarDB_HG_Policy {
		bool txn_split_enabled{false};  // request/observe RFQ XID data and allow split reads
		int consistency_mode{-1};
		int lsn_wait_timeout_ms{-1};
		int max_lag_bytes{-1};
		int proxy_protocol{-1};
	};

	struct PolarDB_HG_Config {
		bool is_polardb_hostgroup{false};
		int writer_hostgroup{-1};
		int reader_hostgroup{-1};
		PolarDB_HG_Policy policy;
		std::shared_ptr<std::atomic<uint64_t>> primary_lsn;
		std::shared_ptr<std::atomic<uint64_t>> writer_epoch;
	};

	struct PolarDB_TopologySnapshot {
		uint64_t generation{0};
		std::unordered_map<unsigned int, PolarDB_HG_Config> by_hostgroup;
	};

	struct PolarDB_ServerSnapshotEntry {
		PgSQL_SrvC* srv{nullptr};
		int64_t weight{0};
		int64_t max_connections{0};
		unsigned int max_latency_us{0};
	};

	struct PolarDB_ServerListEntry {
		std::vector<PolarDB_ServerSnapshotEntry> servers;
		std::shared_ptr<std::atomic<uint64_t>> selection_start;
	};

	struct PolarDB_ServerListSnapshot {
		uint64_t generation{0};
		std::unordered_map<unsigned int, PolarDB_ServerListEntry> by_hostgroup;
	};

	/**
	 * @brief Read PolarDB topology and policy from the generation snapshot.
	 *
	 * The pointer form is for calls that only need to inspect the immutable
	 * snapshot entry during the current call. It avoids copying the shared atomic
	 * cells embedded in PolarDB_HG_Config. The pointer remains valid until this
	 * thread refreshes its cached topology snapshot.
	 */
	const PolarDB_HG_Config* find_polardb_hg_config(unsigned int hostgroup_id);
	PolarDB_HG_Config get_polardb_hg_config(unsigned int hostgroup_id);

	PolarDB_HG_Policy get_polardb_hg_policy(unsigned int hostgroup_id);
	std::shared_ptr<const PolarDB_ServerListSnapshot>
		get_polardb_server_list_snapshot() const;
	uint64_t polardb_server_list_snapshot_generation(
		const std::shared_ptr<const void>& snapshot) const;
	bool polardb_connected_reader_accepts_current_startup(
		const PgSQL_Connection* conn, unsigned int expected_hostgroup_id);
	PgSQL_HGC* polardb_find_hostgroup(unsigned int hostgroup_id);

	/**
	 * @brief Effective backend startup profile for this hostgroup's current
	 *        PolarDB proxy-protocol policy.
	 */
	PolarDB_StartupProfile polardb_startup_profile_for_hostgroup(
		unsigned int hostgroup_id, int fallback_proxy_protocol);

	/**
	 * @brief Whether the hostgroup's current effective proxy protocol requests
	 *        RFQ LSN feedback.
	 */
	bool polardb_hostgroup_requests_rfq_lsn(
		unsigned int hostgroup_id, int fallback_proxy_protocol);

	/**
	 * @brief Warn about loaded PolarDB policy combinations whose effective
	 * global/HG resolution disables expected RFQ LSN behavior.
	 */
	void polardb_warn_config_mismatches();

	/**
	 * @brief Update the per-server LSN cache.
	 * @param address Server address.
	 * @param port Server port.
	 * @param lsn Current LSN value observed by monitor or RFQ result update.
	 * @return true if this call advanced the cached server LSN.
	 */
	bool polardb_update_server_lsn(const char* address, uint16_t port, uint64_t lsn);
	/**
	 * @brief Process an RFQ LSN through a direct server pointer.
	 *
	 * The server pointer is used only for PgSQL_SrvC's atomic LSN cache.
	 * The caller resolves the backend hostgroup's generation-snapshot config once
	 * and passes it here, so RFQ result update stays off the global HGM lock and avoids
	 * rereading mutable PgSQL_HGC fields while handling a result.
	 *
	 * @return true when the RFQ LSN was accepted for the current
	 * replication-group writer epoch, even if the cached LSN was already equal
	 * or newer.
	 */
	bool polardb_update_server_lsn(PgSQL_SrvC* srv, unsigned int backend_hostgroup_id,
		const PolarDB_HG_Config& backend_config, uint64_t lsn,
		const PolarDB_WriterScope& request_scope);

	/**
	 * @brief Latest fresh primary LSN in the writer hostgroup.
	 * @return primary LSN, or 0 if no fresh data is available.
	 */
	uint64_t get_polardb_primary_lsn(unsigned int writer_hostgroup_id);

	/**
	 * @brief Select a reader connection from a reader hostgroup.
	 *
	 * The reader keeps ONLINE status, connection capacity, lag-cap safety, and
	 * RFQ startup-profile compatibility filters. When wait_spec has a target, it
	 * prefers fresh cached readers already at or beyond that LSN, but it MUST NOT
	 * reject the original replica set merely because cached polardb_current_lsn is
	 * below the session target or stale; polar_xact_split_wait_lsn in the wrapped
	 * query is what waits to the session target.
	 *
	 * Ordinary consistency reads pass only_pooled=false, so a cold eligible
	 * replica can create a new backend connection and still execute the current
	 * query on the replica. Transaction pre-write and split reads pass
	 * only_pooled=true because they can use only an already-free backend; after
	 * an exact miss they may try another otherwise eligible reader with an exact
	 * shared-pool match.
	 *
	 * @param hid           Reader hostgroup id.
	 * @param sess          Session requesting the connection.
	 * @param reader_plan   Per-reader lag-cap filters and fallback policy.
	 * @param wait_spec     Wait target and timeout for this query.
	 * @param only_pooled   If true, return only already-pooled connections.
	 * @param exclude_address Optional backend address to skip.
	 * @param exclude_port  Backend port to skip with @p exclude_address.
	 * @return Connection plus the exact result of the attempt.
	 */
	PolarDB_ReaderResult get_MyConn_polardb_reader(unsigned int hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address = nullptr, int exclude_port = -1,
		bool confirm_reader_group_capacity = false);
	bool polardb_reader_claim_server_eligible(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address = nullptr, int exclude_port = -1,
		std::shared_ptr<const void>* selected_server_snapshot = nullptr) const;
	bool polardb_reader_claim_match_key(
		unsigned int hostgroup_id, PgSQL_Session* sess,
		const PolarDB_WaitSpec& wait_spec,
		PgSQL_PoolMatchKey* match_key) const;
	void signal_polardb_reader_claim(
		PgSQL_SrvC* server, PolarDB_ReaderClaimWake wake);

	/**
	 * @brief Queue one lazy transaction-split pool warmup request.
	 *
	 * Pure producer: it only records the demand key (reader HG, user, database,
	 * proxy listener identity) and returns. It never opens a socket and never
	 * takes the HGM write lock.
	 */
	void request_split_warmup(unsigned int reader_hostgroup_id,
		const char* username, const char* password, const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn);

	/**
	 * @brief Drain queued split warmup requests into connected pool entries.
	 *
	 * Called from the HGM maintenance pass. It reserves capacity under the HGM
	 * write lock, opens the backend socket without holding that lock, then
	 * re-locks briefly to add the connected backend. Pooled-only split
	 * reads never run a connect handshake in the transaction path.
	 */
	void warm_split_pools();
	void refresh_split_warmup_variables();
	void refresh_polardb_thread_snapshots();
	void shutdown_split_warmup_thread();
#endif // POLARDB_PROXY

private:
	void update_hostgroup_manager_mappings();
	uint64_t get_pgsql_servers_checksum(SQLite3_result* runtime_pgsql_servers = nullptr);
	uint64_t get_pgsql_servers_v2_checksum(SQLite3_result* incoming_pgsql_servers_v2 = nullptr);
#if POLARDB_PROXY
	const std::shared_ptr<const PolarDB_TopologySnapshot>&
		get_polardb_topology_snapshot_cached() const;
	std::string polardb_writer_identity_locked(unsigned int writer_hostgroup_id);
	void polardb_reset_lsn_cache_for_hostgroup_locked(unsigned int hostgroup_id);
	void polardb_refresh_writer_epoch_locked(unsigned int writer_hostgroup_id, const char* reason);
	void polardb_refresh_all_writer_epochs_locked(const char* reason);
	void polardb_update_server_list_snapshot_locked();
	void polardb_retire_server_locked(PgSQL_SrvC* srv);
	void polardb_prune_retired_server_snapshots_locked();
	void polardb_detach_reclaimable_retired_servers_locked(
		std::vector<PgSQL_SrvC*>& servers_to_delete);
	void polardb_quiesce_snapshots_for_shutdown();

	// PolarDB HG topology cache populated from pgsql_replication_hostgroups;
	// empty until then, so accessors fail safe.
	std::unordered_map<unsigned int, unsigned int> polardb_writer_to_reader_;
	std::unordered_map<unsigned int, unsigned int> polardb_reader_to_writer_;
	std::unordered_set<unsigned int> polardb_hostgroups_;  // all HGs in the PolarDB config
	std::shared_ptr<const PolarDB_TopologySnapshot> polardb_topology_snapshot_;
	std::atomic<uint64_t> polardb_topology_generation_{0};
	std::shared_ptr<const PolarDB_ServerListSnapshot> polardb_server_list_snapshot_;
	std::atomic<uint64_t> polardb_server_list_generation_{0};
	std::deque<std::shared_ptr<const PolarDB_ServerListSnapshot>>
		polardb_retired_server_list_snapshots_;
	struct PolarDB_RetiredServer {
		PgSQL_SrvC* srv{nullptr};
		uint64_t generation{0};
	};
	std::vector<PolarDB_RetiredServer> polardb_retired_servers_;
	std::unique_ptr<PgSQL_PolarDB_ReaderPool> polardb_reader_pool_;
	bool polardb_snapshots_quiesced_{false};
#endif // POLARDB_PROXY
};


#endif /* PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H */
