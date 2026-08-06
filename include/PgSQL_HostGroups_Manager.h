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
#include <iterator>
#include <list>
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

class PgSQL_Thread;

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
// ADMIN_SQLITE_TABLE_PGSQL_REPLICATION_HOSTGROUPS_V3_0_9_V15_WAIT.
#define MYHGM_PgSQL_REPLICATION_HOSTGROUPS "CREATE TABLE pgsql_replication_hostgroups (writer_hostgroup INT CHECK (writer_hostgroup>=0) NOT NULL PRIMARY KEY , reader_hostgroup INT NOT NULL CHECK (reader_hostgroup<>writer_hostgroup AND reader_hostgroup>=0) , check_type VARCHAR CHECK (LOWER(check_type) IN ('read_only', 'polardb')) NOT NULL DEFAULT 'read_only' , txn_split_enabled INT CHECK (txn_split_enabled IN (0, 1) AND (txn_split_enabled = 0 OR LOWER(check_type) = 'polardb')) NOT NULL DEFAULT 0 , consistency_mode VARCHAR CHECK (LOWER(consistency_mode) IN ('default', 'off', 'eventual', 'session_lsn', 'global_lsn', 'primary')) NOT NULL DEFAULT 'default' , max_lag_bytes INT NOT NULL DEFAULT -1 , lsn_wait_timeout_ms INT NOT NULL DEFAULT -1 , proxy_protocol VARCHAR CHECK (LOWER(proxy_protocol) IN ('default', 'v15_wait', 'v15', 'legacy', 'off')) NOT NULL DEFAULT 'default' , comment VARCHAR NOT NULL DEFAULT '' , UNIQUE (reader_hostgroup))"
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

enum class PgSQL_PoolReturnCheck : uint8_t {
	CHECK_CONNECTION = 0,
	REUSE_LOCAL_RETURN_CHECK
};

enum class RejectedConnectionAction : uint8_t {
	DESTROY = 0,
	DETACH
};

enum class PgSQL_PoolGetMode : uint8_t {
	NONE = 0,
	ALLOW_EXACT_MATCH = 1U << 0,
	ALLOW_RESET = 1U << 1,
	ALLOW_CREATE = 1U << 2,
	SKIP_BUSY_POOL = 1U << 3
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
	bool retry_after_config_change{false};
#if POLARDB_PROFILE
	bool exact_match_reserved{false};
#endif // POLARDB_PROFILE
	bool pool_busy{false};
};

/**
 * @brief One-shot handoff that wakes the worker a reservation was created for.
 *
 * Filled in under PgSQL_SrvC::pool_mutex by the code that turns a returned
 * connection into a reservation. The producer must release pool_mutex and then
 * pass this record to
 * PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake(). Until
 * that happens the selected worker keeps waiting and only its own timeout ends
 * the wait, so deliver every populated wake exactly once.
 *
 * server_snapshot is the server-list snapshot reference that keeps `server`
 * alive across the handoff; hold it for as long as the wake is held.
 * has_reservation() is false for a default-constructed wake.
 */
struct PolarDB_ReaderPoolReservationWake {
	unsigned int worker_index{UINT_MAX};
	uint64_t token{0};
	PgSQL_SrvC* server{nullptr};
	std::shared_ptr<const void> server_snapshot;

	bool has_reservation() const {
		return worker_index != UINT_MAX && token != 0 && server != nullptr;
	}
};

enum class ServerReturnStatus : uint8_t {
	INVALID_CONNECTION = 0,
	NOT_IN_USED_LIST,
	SERVER_NOT_ONLINE,
	STORE_FAILED,
	STORED
};

struct ServerReturnResult {
	ServerReturnStatus status{ServerReturnStatus::INVALID_CONNECTION};
	PolarDB_ReaderPoolReservationWake wake;

	bool stored() const {
		return status == ServerReturnStatus::STORED;
	}
};

enum class PoolReturnStatus : uint8_t {
	NOT_HANDLED = 0,
	STORED,
	DETACHED,
	DESTROYED
};

struct PoolReturnResult {
	PoolReturnStatus status{PoolReturnStatus::NOT_HANDLED};
	PgSQL_Connection* detached_connection{nullptr};

	bool handled() const {
		return status != PoolReturnStatus::NOT_HANDLED;
	}
};

#if POLARDB_PROXY
/**
 * @brief One worker's unmet reader-pool capacity need, queued on the hostgroup.
 *
 * Once a compatible connection is returned, the request is removed and replaced
 * by one connection reservation. The reservation keeps that connection in core
 * FREE accounting but unavailable to ordinary acquisition until the selected
 * worker takes or cancels it.
 */
struct PolarDB_ReaderPoolCapacityRequest {
	unsigned int worker_index{UINT_MAX};
	uint64_t token{0};
	uint64_t scope_hash{0};
	PgSQL_PoolMatchKey match_key;
	PolarDB_Query_ReaderPlan reader_plan;
	PolarDB_WaitSpec wait_spec;
};
#endif // POLARDB_PROXY

enum class ReaderReservationEndReason : uint8_t {
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

enum class ReaderTakeStatus : uint8_t {
	MISSING = 0,
	PENDING,
	RETIRED,
	ACQUIRED
};

struct ReaderTakeResult {
	PgSQL_Connection* conn{nullptr};
	ReaderTakeStatus status{
		ReaderTakeStatus::MISSING};
	ReaderReservationEndReason retire_reason{
		ReaderReservationEndReason::NONE};
};

enum class ReaderCancelStatus : uint8_t {
	MISSING = 0,
	REQUEST_CANCELLED,
	RETIRED_RECORD_CLEARED,
	CONNECTION_RETURNED,
	CONNECTION_NOT_FREE
};

struct ReaderCancelResult {
	ReaderCancelStatus status{ReaderCancelStatus::MISSING};
	PolarDB_ReaderPoolReservationWake next_wake;

	bool completed() const {
		return status == ReaderCancelStatus::REQUEST_CANCELLED ||
			status == ReaderCancelStatus::RETIRED_RECORD_CLEARED ||
			status == ReaderCancelStatus::CONNECTION_RETURNED;
	}
};

class PgSQL_SrvConnList {
	private:
	PgSQL_SrvC *mysrvc;
	bool used_list;
#if POLARDB_PROXY
	std::unordered_map<PgSQL_PoolMatchKey,
		std::vector<PgSQL_Connection*>, PgSQL_PoolMatchKeyHash> matching_by_key;
	std::vector<PgSQL_PoolMatchKey> match_keys_by_position;
	size_t empty_match_bucket_count{0};
	void rebuild_free_match_index_unlocked();
	/**
	 * @brief Bring the positional key mirror back in line with the connection
	 *        array before a list mutation reads or writes it.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex. The common case is a size
	 * comparison that returns immediately. When mirror and array disagree, the
	 * repair pass reassigns every connection's polardb_core_pool_position, so
	 * any index or position the caller resolved before this call is stale and
	 * must be resolved again afterwards.
	 *
	 * On the USED list the exact-key reverse index is intentionally not
	 * maintained: the repair drops matching_by_key and keeps only the positional
	 * keys, because USED lookups never go through that index.
	 */
	void ensure_match_index_consistent_unlocked();
	/**
	 * @brief Enforce the cached empty-bucket limit after one bucket becomes empty.
	 *
	 * Keeps the bucket while the cache is below the server connection limit;
	 * otherwise erases it. The caller must hold PgSQL_SrvC::pool_mutex.
	 */
	void limit_empty_match_buckets_unlocked(
		decltype(matching_by_key)::iterator bucket);
	void prune_empty_match_buckets_unlocked();
#endif // POLARDB_PROXY
	int find_idx(PgSQL_Connection* c) const;
	/**
	 * @brief Append a connection to this list and record its pool match key.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex.
	 *
	 * @param key Exact pool match key to record. A null or empty key stores the
	 *            connection keyless, which makes it permanently unreachable by
	 *            exact-key lookup: remove_matching_unlocked() never returns it
	 *            and find_match_key_unlocked() /
	 *            PgSQL_SrvC::used_connection_match_key() report false for it.
	 *            Reserved connections are added keyless for exactly that reason.
	 *            The exact-key reverse index is built only on the FREE list, so
	 *            a key passed for the USED list is recorded positionally but
	 *            creates no lookup entry.
	 */
	void add_unlocked(PgSQL_Connection*, const PgSQL_PoolMatchKey* key = nullptr);
	/**
	 * @brief Remove a connection after its FREE-list index and reservation have
	 *        already been cleared.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex. On the FREE list this asserts
	 * that the connection is absent from the exact-key index and has no active
	 * reservation.
	 */
	PgSQL_Connection* remove_unindexed_unlocked(
		unsigned int index,
		ReaderReservationEndReason reason =
			ReaderReservationEndReason::EXPLICIT_REMOVE);
	/**
	 * @brief Remove the connection at @p index and hand it to the caller.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex. On the FREE list the exact
	 * key is unindexed as well and any reservation held on the connection is
	 * retired with @p reason, which is what a worker waiting on that reservation
	 * later reads back as
	 * ReaderTakeResult::retire_reason.
	 *
	 * @param index  Position in this list.
	 * @param reason Retirement reason recorded for a reservation on the removed
	 *               connection. Only meaningful on the FREE list.
	 * @return The removed connection, now owned by the caller, which must either
	 *         delete it or add it to another list; nullptr when @p index is out
	 *         of range, in which case nothing changed.
	 */
	PgSQL_Connection* remove_unlocked(
		unsigned int index,
		ReaderReservationEndReason reason =
			ReaderReservationEndReason::EXPLICIT_REMOVE);
#if POLARDB_PROXY
	/**
	 * @brief Remove one FREE connection indexed under the exact key @p key.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex. Valid on the FREE list only.
	 * Reserved connections are never present in the exact-key index, so a
	 * connection another worker is waiting for can never be handed out here.
	 *
	 * When the chosen candidate's current slot carries a different key, index
	 * and list have drifted apart; the index is rebuilt once and the lookup
	 * retried. That rebuild reassigns pool positions, so any index the caller
	 * cached before this call must be resolved again.
	 *
	 * @param key Exact pool match key to look up.
	 * @return The removed connection, now owned by the caller, or nullptr when
	 *         no FREE connection carries that key.
	 */
	PgSQL_Connection* remove_matching_unlocked(const PgSQL_PoolMatchKey& key);
	/**
	 * @brief Read a connection's match key without repairing the index.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex and call
	 * ensure_match_index_consistent_unlocked() before resolving positions.
	 */
	bool find_match_key_unlocked(PgSQL_Connection* conn,
		PgSQL_PoolMatchKey* key);
	void unindex_unlocked(PgSQL_Connection*);
	friend void pgsql_polardb_unit_corrupt_match_key_positions(
		PgSQL_SrvConnList*);
	friend void pgsql_polardb_unit_swap_match_key_positions(
		PgSQL_SrvConnList*, unsigned int, unsigned int);
	friend size_t pgsql_polardb_unit_empty_match_bucket_count(
		PgSQL_SrvConnList*);
#endif // POLARDB_PROXY
	/**
	 * @brief Empty this list, appending every connection to @p connections.
	 *
	 * The caller must hold PgSQL_SrvC::pool_mutex. Ownership of every appended
	 * connection passes to the caller, which must delete them only after
	 * releasing pool_mutex.
	 *
	 * @param connections Vector the connections are appended to. It is appended
	 *                    to, never cleared.
	 * @param reason      Retirement reason reported to workers waiting on
	 *                    reservations for this server; on the FREE list all
	 *                    outstanding reservations are retired with it.
	 */
	void detach_all_unlocked(
		std::vector<PgSQL_Connection*>& connections,
		ReaderReservationEndReason reason =
			ReaderReservationEndReason::POOL_DROP);
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
		ReaderReservationEndReason reason =
			ReaderReservationEndReason::EXPLICIT_REMOVE);
	/**
	 * @brief Pick the most reusable connection for @p sess, creating a new
	 *        backend when the pool cannot serve the request.
	 *
	 * In a PolarDB build the caller holds PgSQL_SrvC::pool_mutex across this
	 * scan and the following FREE-to-USED move.
	 *
	 * @param sess        Session whose user, database and session state
	 *                    determine how good a match each pooled connection is.
	 * @param ff          Fast-forward request: skip the compatibility search and
	 *                    go straight to creating a backend.
	 * @param only_pooled Restrict the result to an already-pooled connection.
	 *                    No backend is created; NULL is returned instead.
	 * @param connections_to_delete Receives connections evicted to make room for
	 *                    a new backend. Ownership passes to the caller, which
	 *                    must delete them after the pool lock is released. Must
	 *                    not be null in a PolarDB build: every create and evict
	 *                    branch asserts on it, so a null pointer aborts as soon
	 *                    as the pool needs eviction.
	 * @return The selected connection, already removed from the FREE list, or
	 *         NULL when nothing could be reused or created.
	 */
	PgSQL_Connection * get_random_MyConn_unlocked(PgSQL_Session *sess, bool ff,
		bool only_pooled = false,
		std::vector<PgSQL_Connection*>* connections_to_delete = nullptr,
		bool exact_only = false);
	void get_random_MyConn_inner_search(unsigned int start, unsigned int end,
		unsigned int& conn_found_idx, unsigned int& connection_quality_level,
		unsigned int& number_of_matching_session_variables,
		PgSQL_Session* sess
#if POLARDB_PROXY
		, const PolarDB_StartupProfile* startup_profile,
		const PolarDB_StartupClientContext* startup_client,
		bool has_reader_pool_reservations
#endif // POLARDB_PROXY
		);
	unsigned int conns_length();
	void drop_all_connections();
	PgSQL_Connection *index(unsigned int);
};

#if POLARDB_PROXY
/**
 * @brief USED and FREE connection counts for one server.
 *
 * The two counts are sampled from independent relaxed atomics, so the pair is a
 * consistent snapshot only when the sampling code already holds
 * PgSQL_SrvC::pool_mutex. Without that lock a connection can move between the
 * lists mid-sample, and total() is an estimate that may transiently over- or
 * under-count.
 */
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
	struct PolarDB_ReaderPoolConnectionReservation {
		unsigned int worker_index;
		uint64_t token;
		PgSQL_Connection* conn;
		PgSQL_PoolMatchKey match_key;
	};
	struct PolarDB_ReaderPoolReservationRetired {
		unsigned int worker_index;
		ReaderReservationEndReason reason;
	};
	std::unordered_map<uint64_t, PolarDB_ReaderPoolConnectionReservation>
		polardb_reader_pool_reservations;
	std::unordered_map<PgSQL_Connection*, uint64_t>
		polardb_reader_pool_reservation_token_by_connection;
	std::unordered_map<uint64_t, PolarDB_ReaderPoolReservationRetired>
		polardb_reader_pool_reservation_retired;
	/**
	 * @brief Move one selected capacity request and its concrete connection into
	 *        reserved FREE state.
	 *
	 * The caller holds pool_mutex, has removed the connection from USED when
	 * necessary, and supplies the snapshot that keeps the selected server alive
	 * until the target worker takes or cancels the reservation.
	 *
	 * The connection is inserted into the FREE list without an exact key. That
	 * is what makes a reservation invisible to ordinary acquisition, and this is
	 * the only place the invariant is established.
	 *
	 * @param conn Connection to reserve. Must not be null.
	 * @param selected Capacity request this reservation answers; supplies the
	 *        target worker index, token and match key.
	 * @param selected_server_snapshot Snapshot reference keeping this server
	 *        alive. Must not be empty; it is stored on the connection.
	 * @param wake Optional wake record. It is only populated here: after
	 *        releasing pool_mutex the caller must deliver it via
	 *        PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake(),
	 *        or the target worker waits until its own timeout.
	 */
	void reserve_reader_pool_connection_unlocked(
		PgSQL_Connection* conn,
		const PolarDB_ReaderPoolCapacityRequest& selected,
		std::shared_ptr<const void> selected_server_snapshot,
		PolarDB_ReaderPoolReservationWake* wake);
	/**
	 * @brief Store one reusable connection as ordinary FREE capacity, or reserve
	 *        it for a matching capacity request.
	 *
	 * The caller must hold pool_mutex and must have already removed the
	 * connection from the USED list.
	 *
	 * @param conn Connection to store. Must not be null.
	 * @param key  Exact pool match key the connection is indexed under.
	 * @param wake Optional wake record. It is cleared on entry and populated
	 *        only when the connection became a reservation; the caller must then
	 *        deliver it via
	 *        PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake()
	 *        after releasing pool_mutex.
	 * @param released_server_snapshot Receives the connection's previous server
	 *        snapshot reference. Declare it before taking pool_mutex so its last
	 *        reference is dropped outside the lock.
	 * @param excluded_worker_index Worker that must not be selected for a
	 *        reservation, so a returning worker cannot reserve its own
	 *        connection. UINT_MAX excludes nobody.
	 * @return true when the connection was stored, either as FREE capacity or as
	 *         a reservation. false when @p conn is null or the server is not
	 *         ONLINE; nothing was stored and the connection remains the caller's
	 *         to dispose of.
	 */
	bool store_matching_free_connection_unlocked(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolReservationWake* wake,
		std::shared_ptr<const void>& released_server_snapshot,
		unsigned int excluded_worker_index);
	void polardb_forget_reserved_connection_unlocked(
		PgSQL_Connection* conn, ReaderReservationEndReason reason);
	void polardb_retire_reservations_unlocked(
		ReaderReservationEndReason reason);
	bool polardb_connection_reserved_unlocked(PgSQL_Connection* conn) const;
	/**
	 * @brief Move one allowed FREE connection to USED while pool_mutex is held.
	 *
	 * Exact-key reuse is tried before the broader reset-compatible lookup, and
	 * the reset lookup drops the previous exact key. On both paths the
	 * connection is added to the USED list without an exact key, so the key must
	 * be recomputed from the connection when it is returned.
	 *
	 * Only result.conn and result.source are written, so diagnostic fields set
	 * earlier by the caller survive. Saturation and creation decisions remain
	 * with the caller.
	 *
	 * @param sess   Session the connection is being acquired for.
	 * @param key    Exact pool match key to try first.
	 * @param mode   Which reuse paths are permitted (exact match, reset).
	 * @param result Updated in place with the acquired connection and its
	 *               source; left untouched when nothing was reusable.
	 */
	void take_reusable_connection_unlocked(
		PgSQL_Session* sess, const PgSQL_PoolMatchKey& key,
		PgSQL_PoolGetMode mode, PgSQL_PoolGetResult& result);
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
	/**
	 * @brief Take an already-pooled connection from this server, without ever
	 *        creating one.
	 *
	 * Takes pool_mutex itself, so the caller must not hold it. A returned
	 * connection has already been moved into this server's USED list: the caller
	 * owns a checked-out connection, not a free one.
	 *
	 * @param sess Session the connection is acquired for.
	 * @param key  Exact pool match key to try first.
	 * @param mode Permitted reuse paths. With
	 *        PgSQL_PoolGetMode::SKIP_BUSY_POOL the pool lock is only tried, not
	 *        waited on: when another thread holds it the call returns
	 *        immediately with result.pool_busy set and nothing inspected, which
	 *        the caller must distinguish from a genuine pool miss.
	 * @param selected_max_connections Connection limit used to estimate
	 *        result.server_saturated. 0 disables the estimate entirely.
	 * @param lock_wait_us Optional; receives the time spent waiting for
	 *        pool_mutex. Zeroed on entry and only ever filled in profile builds,
	 *        so a release build always reports 0.
	 * @param lock_hold_us Optional; receives the time pool_mutex was held. Same
	 *        profile-build-only rule as @p lock_wait_us.
	 * @return The acquired connection plus the reason nothing was acquired.
	 */
	PgSQL_PoolGetResult take_existing_connection(PgSQL_Session* sess,
		const PgSQL_PoolMatchKey& key, PgSQL_PoolGetMode mode,
		unsigned int selected_max_connections = 0,
		unsigned long long* lock_wait_us = nullptr,
		unsigned long long* lock_hold_us = nullptr);
	/**
	 * @brief Add a connection to this server as FREE capacity under @p key.
	 *
	 * Takes pool_mutex itself, so the caller must not hold it.
	 *
	 * @param conn Connection to add. Must not be null.
	 * @param key  Exact pool match key to index it under.
	 * @param wake Optional wake record. A true return may mean the connection
	 *        became a reservation for a waiting worker rather than plain FREE
	 *        capacity; in that case the wake is populated and the caller must
	 *        deliver it through
	 *        PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake().
	 * @return true when the connection was accepted. false when @p conn is null
	 *         or the server is not ONLINE: nothing was stored and the connection
	 *         remains the caller's to dispose of.
	 */
	bool add_matching_connection(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolReservationWake* wake = nullptr);
	bool add_used_matching_connection(PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key);
	/**
	 * @brief Move a checked-out connection from USED back into FREE capacity.
	 *
	 * Takes pool_mutex itself, so the caller must not hold it.
	 *
	 * A false return has two shapes and the caller must respond to both the same
	 * way, by taking ownership: either the connection was not found in the USED
	 * list and nothing changed (an error is logged), or the server stopped being
	 * ONLINE after the connection had already been removed from USED. In both
	 * cases the connection is no longer registered with this server and the
	 * caller must delete it or detach it.
	 *
	 * @param conn Connection to return. Must not be null.
	 * @param key  Exact pool match key to index it under in FREE.
	 * @param lock_wait_us Optional; receives the time spent waiting for
	 *        pool_mutex. Zeroed on entry and only ever filled in profile builds.
	 * @param lock_hold_us Optional; receives the time pool_mutex was held. Same
	 *        profile-build-only rule.
	 * @param excluded_worker_index Worker that must not win the reservation,
	 *        normally the returning worker itself.
	 * @return Exact result. When status is STORED and result.wake is valid, the
	 *         caller must deliver that wake after this call.
	 */
	ServerReturnResult return_matching_connection(
		PgSQL_Connection* conn,
		const PgSQL_PoolMatchKey& key,
		unsigned long long* lock_wait_us = nullptr,
		unsigned long long* lock_hold_us = nullptr,
		unsigned int excluded_worker_index = UINT_MAX);
	/**
	 * @brief Hand a still-checked-out connection directly to another worker that
	 *        is waiting for capacity, instead of returning it to FREE.
	 *
	 * Takes pool_mutex itself and the hostgroup's capacity-request mutex beneath
	 * it; that is the required lock order, and request paths never take a server
	 * pool mutex while holding the request mutex.
	 *
	 * @param conn Connection currently in this server's USED list.
	 * @param key  Exact pool match key a waiting request must match.
	 * @param returning_worker_index Worker returning the connection; it is
	 *        excluded from selection so it cannot reserve its own connection.
	 * @param wake Cleared on entry; populated only on CONNECTION_RESERVED.
	 * @return CONNECTION_INVALID when the arguments are unusable or the
	 *         connection is not in USED on an ONLINE server, in which case
	 *         nothing changed. NO_REMOTE_REQUEST when no other worker wanted
	 *         it: the connection is still in USED and the caller must return it
	 *         by the normal path. CONNECTION_RESERVED when the connection left
	 *         USED and became a reservation; the caller must then deliver @p
	 *         wake through
	 *         PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake().
	 */
	PolarDB_ReaderRemoteReservationResult reserve_matching_used_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		unsigned int returning_worker_index,
		PolarDB_ReaderPoolReservationWake* wake);
	bool has_matching_reader_pool_capacity_request(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index = UINT_MAX) const;
	/**
	 * @brief Collect the outcome of one worker's reader-pool wait.
	 *
	 * Takes pool_mutex itself, so the caller must not hold it.
	 *
	 * @param worker_index Waiting worker.
	 * @param token        Token the worker registered its capacity request with.
	 * @return A status the caller must act on:
	 *         ACQUIRED - result.conn is non-null and has already been moved into
	 *         this server's USED list, so the worker owns a checked-out
	 *         connection;
	 *         PENDING - the capacity request is still queued and the worker
	 *         should keep waiting;
	 *         RETIRED - the reservation was destroyed and result.retire_reason
	 *         says why. This consumes the retirement record, so a repeat call
	 *         for the same token reports MISSING; a retry loop must read the
	 *         reason on this call;
	 *         MISSING - neither a reservation nor a queued request exists for
	 *         this worker and token.
	 */
	ReaderTakeResult take_reader_pool_reservation(
		unsigned int worker_index, uint64_t token);
	/**
	 * @brief Give up a reader-pool wait, whether it is still a queued capacity
	 *        request or already a concrete reservation.
	 *
	 * The queued request is cancelled first without pool_mutex; only the
	 * reservation stage takes pool_mutex, so the caller must not hold it.
	 *
	 * @param worker_index Worker abandoning the wait.
	 * @param token        Token the wait was registered with.
	 * @return Exact cancellation result. A returned connection may immediately
	 *         satisfy another waiting worker; in that case result.next_wake must
	 *         be delivered through
	 *         PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake().
	 */
	ReaderCancelResult cancel_reader_pool_reservation(
		unsigned int worker_index, uint64_t token);
	/**
	 * @brief Free room in the FREE list so a new backend can be created.
	 *
	 * Takes pool_mutex itself, so the caller must not hold it. Reserved
	 * connections are never evicted, because a worker is already waiting on
	 * each of them.
	 *
	 * @param preferred_count Best-effort upper bound on how many connections to
	 *        evict. Must be greater than or equal to @p required_count.
	 * @param required_count Minimum that must be evicted for the call to
	 *        succeed. The call is all-or-nothing with respect to it.
	 * @param connections_to_delete Evicted connections are appended here.
	 *        Ownership passes to the caller, which must delete them after
	 *        pool_mutex has been released.
	 * @return true when at least @p required_count connections were evicted.
	 *         false when that many were not evictable, in which case nothing was
	 *         evicted at all and the caller must not assume partial progress.
	 */
	bool evict_unreserved_free_for_create(
		unsigned int preferred_count, unsigned int required_count,
		std::vector<PgSQL_Connection*>& connections_to_delete);
	unsigned int reader_pool_capacity_request_count() const;
	unsigned int reader_pool_reservation_count() const;
	unsigned int matching_connection_count(
		const PgSQL_PoolMatchKey& key) const;
	bool used_connection_match_key(PgSQL_Connection* conn,
		PgSQL_PoolMatchKey* key) const;
	bool remove_used_connection(PgSQL_Connection* conn);
	bool remove_free_connection(PgSQL_Connection* conn);
	bool polardb_finish_idle_ping(PgSQL_Connection* conn, bool destroyed);
	/**
	 * @brief Shrink an over-full FREE list towards @p max_free, one maintenance
	 *        pass behind the decision.
	 *
	 * The caller must hold pool_mutex. Trimming is deliberately deferred: a
	 * connection is marked on one call and only destroyed on a later one, so a
	 * single call against a pool that has just grown destroys nothing. That
	 * grace period lets a burst of traffic reclaim its own idle connections
	 * instead of paying for a reconnect.
	 *
	 * Each call runs three passes and the result counters correspond to them:
	 * connections marked by an earlier call are destroyed (destroyed), the mark
	 * is cancelled on everything once the list is back at @p max_free
	 * (cancelled), and the current excess is marked for the next call
	 * (deferred).
	 *
	 * @param max_free Target size of the FREE list.
	 * @param connections_to_delete Destroyed connections are appended here.
	 *        Ownership passes to the caller, which must delete them after
	 *        releasing pool_mutex.
	 * @return Per-pass counts for this call.
	 */
	PolarDB_IdleTrimResult polardb_trim_free_connections_to_max_unlocked(
		unsigned int max_free,
		std::vector<PgSQL_Connection*>& connections_to_delete);
	unsigned int pool_free_count_value() const {
		return pool_free_count.load(std::memory_order_relaxed);
	}
	unsigned int pool_used_count_value() const {
		return pool_used_count.load(std::memory_order_relaxed);
	}
	// Lock-free mirror of `status`. set_status() stores it with release ordering
	// under pool_mutex, so other threads read a consistent value. Read it through
	// polardb_fast_status_value() from threads that do not hold the
	// HostGroups_Manager lock.
	alignas(64) std::atomic<int> polardb_fast_status{0};
	std::atomic<unsigned int> current_latency_us{0};

	// =========================================================================
	// PolarDB per-server LSN tracking (read-your-writes consistency)
	// =========================================================================
	// Immutable identity used by worker-local caches. Unlike the object address,
	// it cannot collide when a retired server container's storage is reused.
	uint64_t polardb_instance_id{0};

	// Updated through PgSQL_HostGroups_Manager from two sources: monitor health
	// checks, and the LSN that a backend reports in the
	// extended ReadyForQuery (RFQ) message after running a query. Read without a
	// lock by reader selection and by the byte-lag and freshness checks.
	//
	// There is no per-reader millisecond replication lag value here on purpose:
	// the PostgreSQL/PolarDB path does not yet produce one, and the MySQL/Aurora
	// field aws_aurora_current_lag_us must not be reused for it.
	//
	// polardb_current_lsn : latest WAL LSN observed for this server. Advanced with a
	//                       compare-and-swap to the maximum, so a stale
	//                       observation can never lower it.
	// lsn_updated_at      : monotonic_time() microseconds for a recent observation.
	//                       Repeated worker observations may be coalesced for at
	//                       most one millisecond.
	// Keep the LSN state isolated from unrelated PgSQL_SrvC state. Reader
	// selection only reads the two counters. The guard coordinates RFQ updates
	// with the rare reset after a writer change and has its own cache line, so
	// taking it does not invalidate the line read by reader selection.
	alignas(64) std::atomic<bool> polardb_lsn_cache_guard{false};
	alignas(64) std::atomic<uint64_t> polardb_current_lsn{0};
	std::atomic<unsigned long long> lsn_updated_at{0};
	/*
	 * polardb_advance_lsn stores the timestamp after updating the LSN.
	 * Acquiring that timestamp before reading the LSN pairs it with that LSN
	 * or a later one.
	 */
	PolarDB_ReaderLsnSample polardb_sample_lsn(
			uint64_t now_us, uint32_t freshness_ms) const {
		const uint64_t updated_at =
			lsn_updated_at.load(std::memory_order_acquire);
		const uint64_t lsn =
			polardb_current_lsn.load(std::memory_order_relaxed);
		return PolarDB_ReaderLsnSample{
			lsn, polardb_lsn_cache_is_fresh(updated_at, now_us, freshness_ms)};
	}
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
	void polardb_lock_lsn_cache();
	void polardb_unlock_lsn_cache();
	bool polardb_advance_lsn(uint64_t lsn, uint64_t observed_at_us);
	/**
	 * @brief Read the lock-free mirror of PgSQL_SrvC::status.
	 *
	 * set_status() stores the mirror with release ordering while holding
	 * pool_mutex, so query threads can test server status without taking the
	 * HostGroups_Manager lock that normally guards `status`. The mirror can
	 * therefore differ briefly from `status` as read by a thread that does hold
	 * that lock.
	 *
	 * @return The most recently stored server status.
	 */
	enum MySerStatus polardb_fast_status_value() const;
	/**
	 * @brief Sample this server's USED and FREE connection counts.
	 *
	 * The two counts are loaded as independent relaxed atomics without taking
	 * pool_mutex, so the pair is a consistent snapshot only when the caller
	 * already holds pool_mutex. Otherwise total() is an estimate that can over-
	 * or under-count while a connection moves between the lists.
	 *
	 * @return The sampled counts.
	 */
	PolarDB_PoolConnStats polardb_pool_conn_stats() const;
	unsigned int polardb_pool_total_count() const;
	bool polardb_pool_can_add_active_connection() const;
	bool polardb_pool_can_open_socket() const;
#endif // POLARDB_PROXY
	/**
	 * @brief Update the maximum number of used connections
	 * @return The maximum number of used connections
	 */
	unsigned int update_max_connections_used()
	{
#if POLARDB_PROXY
		unsigned int connections_used = pool_used_count_value();
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
	 * the policy quickly. Supported consistency modes are off, eventual,
	 * session_lsn, and global_lsn.
	 *
	 * Query threads read the group LSN and writer epoch through the current
	 * topology snapshot, without taking the HostGroups_Manager lock or keeping a
	 * pointer to this container.
	 *
	 * Writer-epoch rule: when the set of live (non-OFFLINE_HARD) writer
	 * servers changes, the group and replica-only LSN maxima plus the affected
	 * per-server LSN caches are reset before the epoch is increased, so stale
	 * positions from the old writer set are never carried forward.
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
		std::string consistency_mode;      // consistency mode string (off/eventual/session_lsn/global_lsn)
		int consistency_mode_enum{-1};     // parsed enum value; -1 = use global default
		int lsn_wait_timeout_ms{0};        // polar_xact_split_wait_lsn timeout in ms
		std::string proxy_protocol;        // default/v15_wait/v15/legacy/off
		int proxy_protocol_enum{-1};       // parsed enum value; -1 = inherit global
		// Shared values are copied into the topology snapshot so query threads
		// can read them without the HGM lock.
		std::shared_ptr<std::atomic<uint64_t>> polardb_group_lsn{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // highest LSN reported during the current writer epoch
		std::shared_ptr<std::atomic<uint64_t>> polardb_max_replica_replay_lsn{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // highest physical-replica replay LSN in this epoch
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
	// Returning FREE capacity may hold PgSQL_SrvC::pool_mutex before taking this
	// mutex. HGC request methods must never acquire a server pool mutex;
	// eligibility reads only the immutable topology snapshot and atomics.
	using PolarDB_ReaderPoolCapacityRequestList =
		std::list<PolarDB_ReaderPoolCapacityRequest>;
	using PolarDB_ReaderPoolCapacityRequestIterator =
		PolarDB_ReaderPoolCapacityRequestList::iterator;
	mutable std::mutex polardb_reader_pool_capacity_request_mutex;
	std::unordered_map<
		PgSQL_PoolMatchKey, PolarDB_ReaderPoolCapacityRequestList,
		PgSQL_PoolMatchKeyHash> polardb_reader_pool_capacity_requests_by_key;
	std::unordered_map<unsigned int, PolarDB_ReaderPoolCapacityRequestIterator>
		polardb_reader_pool_capacity_request_by_worker;
	std::unordered_map<uint64_t, PolarDB_ReaderPoolCapacityRequestIterator>
		polardb_reader_pool_capacity_request_by_token;
	std::atomic<unsigned int> polardb_reader_pool_capacity_request_count_fast{0};
	bool reader_pool_capacity_request_matches_server_unlocked(
		const PolarDB_ReaderPoolCapacityRequest& request, PgSQL_SrvC* server,
		unsigned int excluded_worker_index, uint64_t& now_us) const;
	bool has_reader_pool_capacity_request_candidate_unlocked(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index,
		bool& lag_time_needed) const;
	void erase_capacity_request_or_abort_unlocked(
		PolarDB_ReaderPoolCapacityRequestIterator request);

	public:
	/**
	 * @brief Queue one worker's request for reader-pool capacity on this
	 *        hostgroup.
	 *
	 * Takes polardb_reader_pool_capacity_request_mutex, which must never be held
	 * while acquiring a server pool_mutex.
	 *
	 * @param worker_index Worker that will wait. One request per worker.
	 * @param token        Non-zero identifier the worker later takes or cancels
	 *                     the request with. Must be unique while queued.
	 * @param scope_hash   Non-zero hash of the writer scope the request belongs
	 *                     to.
	 * @param key          Exact pool match key a returned connection must carry.
	 * @param reader_plan  Lag-cap filters and fallback policy for this request.
	 * @param wait_spec    Wait target and timeout for this query.
	 * @return true when the request was queued; the caller must then pair it
	 *         with cancel_reader_pool_capacity_request() or a take.
	 *         false when nothing was queued - an invalid worker index, token,
	 *         scope hash or key; a replica-only reader plan, which is rejected
	 *         as policy because those reads use pooled-only acquisition; or a
	 *         token or worker that is already queued. A caller that gets false
	 *         must not wait, or it blocks until its own timeout.
	 */
	bool register_reader_pool_capacity_request(
		unsigned int worker_index, uint64_t token, uint64_t scope_hash,
		const PgSQL_PoolMatchKey& key,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec);
	/**
	 * @brief Consume one queued capacity request that @p server can satisfy.
	 *
	 * The caller may already hold @p server's pool_mutex; this is the one HGC
	 * request method that runs under it, and the documented lock order allows
	 * only that direction. Resolving the server snapshot needs the HostGroups
	 * Manager, which must not be entered under the request mutex, so the request
	 * mutex is released for that step and re-taken afterwards. The request set
	 * can change across that gap, and the matching bucket is looked up again.
	 *
	 * @param server Server offering the capacity. Must not be null.
	 * @param key    Exact pool match key the request must match.
	 * @param selected On success, receives a copy of the request; the caller now
	 *        owns the obligation to turn it into a reservation or to release it.
	 * @param excluded_worker_index Worker that must not be selected, normally
	 *        the one offering the connection.
	 * @param selected_server_snapshot Reset on entry. On success it receives the
	 *        snapshot reference that keeps @p server alive; retain it until the
	 *        reservation is taken or cancelled.
	 * @return true when a request was found and erased from all request indexes.
	 *         false when none matched, in which case nothing was consumed and
	 *         @p selected_server_snapshot is left empty.
	 */
	bool take_matching_reader_pool_capacity_request(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolCapacityRequest* selected,
		unsigned int excluded_worker_index = UINT_MAX,
		std::shared_ptr<const void>* selected_server_snapshot = nullptr);
	bool cancel_reader_pool_capacity_request(unsigned int worker_index, uint64_t token);
	bool has_reader_pool_capacity_request(
		unsigned int worker_index, uint64_t token) const;
	bool has_matching_reader_pool_capacity_request(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index = UINT_MAX) const;
	bool has_reader_pool_capacity_request_for_another_worker(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index) const;
	bool has_reader_pool_capacity_request_fast() const {
		return polardb_reader_pool_capacity_request_count_fast.load(
			std::memory_order_acquire) != 0;
	}
	unsigned int reader_pool_capacity_request_count() const;
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
		std::atomic<unsigned long long> polardb_lsn_updates_from_monitor{0}; // LSN advances observed by the monitor
		std::atomic<unsigned long long> polardb_replica_replay_lsn_advanced{0}; // current-epoch monitor observations that advanced the physical-replica replay maximum
		std::atomic<unsigned long long> polardb_monitor_health_invalid_role{0}; // monitor role is not routable, including PolarDB "unknown" for POLAR_UNKNOWN/POLAR_STANDALONE_DATAMAX
		std::atomic<unsigned long long> polardb_monitor_health_invalid_values{0}; // PolarDB monitor health row has invalid availability or LSN text
		std::atomic<unsigned long long> polardb_lsn_stale_count{0};          // stale-LSN skips while selecting a reader
		std::atomic<unsigned long long> polardb_write_missing_lsn{0};        // writer RFQ carried no LSN; automatic RYW reads forced to writer
		std::atomic<unsigned long long> polardb_read_missing_lsn{0};         // read RFQ carried no LSN while SESSION_LSN tracked observations
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_to_target{0}; // client RFQ LSN raised to a confirmed session/wait target
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_by_writer{0}; // client RFQ LSN raised because response used the writer
		std::atomic<unsigned long long> polardb_client_rfq_lsn_raised_by_wait{0}; // client RFQ LSN raised because an LSN wait completed
		std::atomic<unsigned long long> polardb_group_lsn_unknown{0};        // GLOBAL_LSN had no group LSN
		std::atomic<unsigned long long> polardb_rfq_best_effort_degraded_routes{0}; // RFQ-unavailable reads sent to reader without wait
		std::atomic<unsigned long long> polardb_consistency_writer_fallback{0}; // consistency reads redirected to writer after reader selection failed
		std::atomic<unsigned long long> polardb_lag_cap_freshness_clamped{0}; // byte-lag cap reduced the allowed age of a cached reader LSN
		std::atomic<unsigned long long> polardb_lag_cap_lsn_unknown{0}; // lag-cap reader candidate had no cached LSN
		std::atomic<unsigned long long> polardb_lag_cap_lsn_stale{0}; // lag-cap reader candidate had a stale cached LSN
		std::atomic<unsigned long long> polardb_lag_cap_rejected{0}; // lag-cap reader candidate exceeded max_lag_bytes
		std::atomic<unsigned long long> polardb_lag_cap_accepted{0}; // lag-cap reader candidate passed max_lag_bytes
		std::atomic<unsigned long long> polardb_wait_reads_retried_on_reader{0}; // wait-wrapped reads retried once on another reader
		std::atomic<unsigned long long> polardb_wait_reads_retried_on_writer{0}; // wait-wrapped reads retried once on the writer after reader failure policy
		std::atomic<unsigned long long> polardb_wait_retry_evaluated{0}; // wait-read failures that entered the retry-to-writer decision
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
		std::atomic<unsigned long long> polardb_reader_pool_retry_after_config_change{0}; // cold reader creations retried after topology or startup configuration changed
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
		std::atomic<unsigned long long> polardb_writer_pool_acquire_attempt{0};
		std::atomic<unsigned long long> polardb_writer_pool_acquire_hit{0};
		std::atomic<unsigned long long> polardb_writer_pool_acquire_busy{0};
		std::atomic<unsigned long long> polardb_writer_pool_acquire_group_busy{0};
		std::atomic<unsigned long long> polardb_writer_capacity_wait_enter{0};
		std::atomic<unsigned long long> polardb_writer_capacity_wait_exit{0};
		std::atomic<unsigned long long> polardb_writer_capacity_wait_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_ownership_local{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_ownership_active{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_ownership_reservation{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_ownership_zero{0};
		std::atomic<unsigned long long> polardb_reader_pool_retention_started{0};
		std::atomic<unsigned long long> polardb_reader_pool_retention_cleared{0};
		std::atomic<unsigned long long> polardb_reader_pool_retained_connection_shared{0};
		std::atomic<unsigned long long> polardb_reader_pool_remote_request_seen{0};
		std::atomic<unsigned long long> polardb_reader_pool_remote_reservation_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_remote_reservation_created{0};
		std::atomic<unsigned long long> polardb_reader_pool_remote_reservation_not_created{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_request_registered{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_request_cancelled{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_request_duplicate_token{0};
		std::atomic<unsigned long long> polardb_reader_pool_capacity_request_duplicate_worker{0};
		std::atomic<unsigned long long> polardb_reader_pool_connection_reserved{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_acquired{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_released{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_wake{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_wake_coalesced{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_missing{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_missing_retired{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_missing_unknown{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_create_evict{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_idle_trim{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_max_age{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_offline{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_pool_drop{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_explicit{0};
		std::atomic<unsigned long long> polardb_reader_pool_reservation_retired_invalid{0};
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
		std::atomic<unsigned long long> polardb_output_coalesce_flush_backpressure{0}; // coalesce skipped due to pending output/socket state
#if POLARDB_PERF_DEBUG
		std::atomic<unsigned long long> polardb_perf_core_pool_exact_attempt{0};
		std::atomic<unsigned long long> polardb_perf_core_pool_exact_hit{0};
		std::atomic<unsigned long long> polardb_perf_core_pool_exact_miss{0};
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
		std::atomic<unsigned long long> polardb_result_row_run_unavailable{0}; // row-run probe fell back to the normal result path
		std::atomic<unsigned long long> polardb_result_row_run_partial{0}; // row-run probe saw an incomplete DataRow frame
		std::atomic<unsigned long long> polardb_result_row_run_not_candidate{0}; // row-run skipped before detach because current libpq input is not DataRow

		// Wait wrapping / RYW routing counters.
		//
		// Counter meaning:
		//  - PolarDB_Session_LSN_Routing / polardb_session_lsn_routing and
		//    PolarDB_Global_LSN_Routing / polardb_global_lsn_routing count the
		//    route-plan decision: a read was sent to a reader with an LSN wait
		//    requirement for the corresponding consistency mode.
		//  - PolarDB_Wait_Wrap_Prepared / polardb_wait_wrap_prepared counts only
		//    readers that were behind the target and therefore activated a real
		//    wrapper. PolarDB_Wait_Wrap_Bypassed counts target-ready reads that
		//    skipped the backend LSN wait.
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
		std::atomic<unsigned long long> polardb_route_manual_forced_writer{0}; // manual route forced to the writer after a reader failure
		std::atomic<unsigned long long> polardb_route_locked_hostgroup{0};   // explicit locked_on_hostgroup routes that bypass automatic planning
		std::atomic<unsigned long long> polardb_wait_wrap_prepared{0};        // selected reader needs a real wait wrapper
		std::atomic<unsigned long long> polardb_wait_wrap_bypassed{0};       // selected reader reached consistency target -> LSN wait skipped
		std::atomic<unsigned long long> polardb_txn_reader_reuse_bypass_checked{0}; // retained transaction-reader bypass checks
		std::atomic<unsigned long long> polardb_txn_reader_reuse_bypass_allowed{0}; // retained transaction-reader bypass checks accepted
		std::atomic<unsigned long long> polardb_wait_lsn_sent{0};             // LSN wait wrapper successfully installed/sent
		std::atomic<unsigned long long> polardb_wait_lsn_sum_us{0};          // total response time for wait-wrapped reads (microseconds)
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_1ms{0};  // wait wrapper elapsed <= 1ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_5ms{0};  // wait wrapper elapsed <= 5ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_10ms{0}; // wait wrapper elapsed <= 10ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_50ms{0}; // wait wrapper elapsed <= 50ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_100ms{0}; // wait wrapper elapsed <= 100ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_500ms{0}; // wait wrapper elapsed <= 500ms
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_le_1s{0};   // wait wrapper elapsed <= 1s
		std::atomic<unsigned long long> polardb_wait_lsn_elapsed_gt_1s{0};   // wait wrapper elapsed > 1s
#if POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_rfq_lsn_write_accepted{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_read_accepted{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_write_rejected{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_read_rejected{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_reject_inactive{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_reject_invalid_input{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_reject_missing_request_scope{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_reject_missing_backend_scope{0};
		std::atomic<unsigned long long> polardb_rfq_lsn_reject_scope_mismatch{0};
		std::atomic<unsigned long long> polardb_consistency_read_wait_planned{0};
		std::atomic<unsigned long long> polardb_consistency_reader_wait_bypassed{0};
		std::atomic<unsigned long long> polardb_consistency_reader_wait_required{0};
		std::atomic<unsigned long long> polardb_consistency_wait_wrapper_installed{0};
		std::atomic<unsigned long long> polardb_wait_wrap_build_sum_us{0};   // wait wrapper SQL build latency total
		std::atomic<unsigned long long> polardb_wait_wrap_build_count{0};    // wait wrapper SQL build latency samples
		std::atomic<unsigned long long> polardb_wait_wrap_install_sum_us{0}; // wait wrapper packet install latency total
		std::atomic<unsigned long long> polardb_wait_wrap_install_count{0};  // wait wrapper packet install latency samples
		std::atomic<unsigned long long> polardb_wait_target_lsn_cache_advanced{0}; // successful waits that advanced selected-reader LSN cache
		std::atomic<unsigned long long> polardb_wait_target_lsn_cache_rejected{0}; // successful waits whose selected-reader LSN update was rejected
		std::atomic<unsigned long long> polardb_lsn_update_call{0};
		std::atomic<unsigned long long> polardb_lsn_update_advance{0};
		std::atomic<unsigned long long> polardb_lsn_update_refresh_only{0};
		std::atomic<unsigned long long> polardb_lsn_update_shared_update{0};
		std::atomic<unsigned long long> polardb_lsn_update_coalesced{0};
		std::atomic<unsigned long long> polardb_lsn_update_pass_server{0};
		std::atomic<unsigned long long> polardb_lsn_update_pass_repeat{0};
		std::atomic<unsigned long long> polardb_reader_pool_used_count_read{0};
		std::atomic<unsigned long long> polardb_reader_pool_used_count_pass_server{0};
		std::atomic<unsigned long long> polardb_reader_pool_used_count_pass_repeat{0};
		std::atomic<unsigned long long> polardb_reader_pool_used_count_pass_overflow{0};
#endif // POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_lsn_update_pass_overflow{0}; // pass-cache capacity was exceeded
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
		std::atomic<unsigned long long> polardb_split_fallback_group_lsn_unknown{0}; // split fallback: lag cap had no group LSN sample
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
		std::atomic<unsigned long long> polardb_split_rejected_no_marker{0}; // transaction RFQ has no usable split or pre-write marker
		std::atomic<unsigned long long> polardb_split_wal_pending{0};         // primary RFQ still has pending WAL
		std::atomic<unsigned long long> polardb_split_wal_pending_replica_confirmed{0}; // WAL-pending read admitted after current-epoch replica replay confirmation
		std::atomic<unsigned long long> polardb_split_invariant_violations{0}; // unexpected transaction-split state
		std::atomic<unsigned long long> polardb_split_blocked_reads{0};       // transaction already blocked from further split reads
		std::atomic<unsigned long long> polardb_split_no_backend{0};          // no split replica backend available
		std::atomic<unsigned long long> polardb_split_send_failed{0};         // split wrapped query send failed
		std::atomic<unsigned long long> polardb_split_pool_hit{0};            // acquired an existing pooled split connection
		std::atomic<unsigned long long> polardb_split_pool_empty{0};          // no pooled split connection existed
		std::atomic<unsigned long long> polardb_split_pool_contention{0};     // no usable pooled split connection was available
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
		std::atomic<unsigned long long> polardb_reader_acquire_sum_us{0};     // ReaderPool acquisition latency total
		std::atomic<unsigned long long> polardb_reader_acquire_count{0};      // ReaderPool acquisition latency samples
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
		std::atomic<unsigned long long> polardb_rfq_requested_missing_payload{0}; // RFQ-LSN startup connection returned RFQ with no LSN payload
		std::atomic<unsigned long long> polardb_rfq_requested_zero_payload{0}; // RFQ-LSN startup connection returned RFQ with zero LSN payload
		std::atomic<unsigned long long> polardb_client_rfq_lsn_missing_with_target{0}; // client requested RFQ LSN, but backend payload was absent with a target
		std::atomic<unsigned long long> polardb_split_prepare_sum_us{0};      // split prepare latency total
		std::atomic<unsigned long long> polardb_split_prepare_count{0};       // split prepare latency samples
		std::atomic<unsigned long long> polardb_split_reader_acquire_sum_us{0}; // split reader acquisition latency total
		std::atomic<unsigned long long> polardb_split_reader_acquire_count{0}; // split reader acquisition latency samples
		std::atomic<unsigned long long> polardb_split_pool_miss_reserved_exact{0}; // exact-compatible split capacity was reserved by a reservation
		std::atomic<unsigned long long> polardb_split_wrapper_build_sum_us{0}; // split wrapper build latency total
		std::atomic<unsigned long long> polardb_split_wrapper_build_count{0}; // split wrapper build latency samples
		std::atomic<unsigned long long> polardb_wait_profile_plan_dispatch_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_plan_dispatch_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_dispatch_wait_set_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_dispatch_wait_set_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_wait_set_query_end_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_wait_set_query_end_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_mismatch{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_count{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_sum_us{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_1ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_5ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_10ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_50ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_100ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_500ms{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_le_1s{0};
		std::atomic<unsigned long long> polardb_txn_wait_lsn_elapsed_gt_1s{0};
		std::atomic<unsigned long long> polardb_wait_profile_ordinary_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_ordinary_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_unknown_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_unknown_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_write_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_write_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_observed_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_observed_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_session_equal_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_session_equal_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_global_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_global_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_txn_primary_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_target_txn_primary_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_selected_best_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_selected_best_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_selected_behind_best_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_selected_behind_best_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_selection_unknown_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_selection_unknown_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_same_reader_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_same_reader_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_cross_reader_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_cross_reader_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_reader_unknown_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_observed_reader_unknown_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_unknown_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_unknown_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_stale_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_stale_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_zero_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_zero_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_4kb_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_4kb_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_64kb_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_64kb_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_1mb_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_1mb_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_16mb_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_le_16mb_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_gt_16mb_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_gap_gt_16mb_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_unknown_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_unknown_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_100us_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_100us_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_1ms_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_1ms_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_5ms_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_le_5ms_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_gt_5ms_count{0};
		std::atomic<unsigned long long> polardb_wait_profile_lsn_age_gt_5ms_sum_us{0};
		std::atomic<unsigned long long> polardb_wait_profile_selected_gap_sum_bytes{0};
		std::atomic<unsigned long long> polardb_wait_profile_selection_loss_sum_bytes{0};
#endif // POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_split_lsn_wait_count{0};      // split LSN wait wrappers prepared
		std::atomic<unsigned long long> polardb_split_lsn_wait_sum_us{0};     // split wrapped-read response time total
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
		std::atomic<unsigned long long> polardb_split_warmup_connection_reserved{0}; // warmup connection immediately assigned to a pending reservation
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
		std::atomic<unsigned long long> polardb_reader_pool_busy_alternate_hit{0}; // busy selected pool served by another eligible reader
		std::atomic<unsigned long long> polardb_reader_pool_busy_alternate_miss{0}; // alternate returned no connection after selected pool was busy
#if POLARDB_PROFILE
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_take_busy{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_free_zero_before_lock{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_free_zero_became_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_selected_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_selected_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_selected_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_selected_busy{0};
		std::atomic<unsigned long long> polardb_reader_pool_additional_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_additional_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_additional_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_additional_busy{0};
		std::atomic<unsigned long long> polardb_reader_pool_confirmed_saturated{0};
		std::atomic<unsigned long long> polardb_reader_pool_hgm_create_lock_entry{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_accepted{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_rejected{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_1{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_2{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_3_4{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_5_8{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_9_16{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_group_17_plus{0};
		std::atomic<unsigned long long> polardb_reader_pool_exact_bucket_created{0};
		std::atomic<unsigned long long> polardb_reader_pool_exact_bucket_emptied{0};
		std::atomic<unsigned long long> polardb_reader_pool_exact_bucket_reused{0};
		std::atomic<unsigned long long> polardb_reader_pool_exact_bucket_pruned{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_lock_wait_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_pool_shared_return_lock_hold_sum_us{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_attempt{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_hit{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_take_miss{0};
		std::atomic<unsigned long long> polardb_reader_pool_local_scan_steps{0};
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
		// Incremented only on an inactive-to-active topology transition. Workers
		// cache it with polardb_active so sessions can detect an activation window
		// without loading shared state on the query path.
		std::atomic<uint64_t> polardb_activation_generation{0};
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
	/**
	 * @brief Get a connection to one already-chosen server, reusing a pooled one
	 *        or creating a backend when the mode allows it.
	 *
	 * Takes the HostGroups_Manager write lock itself whenever
	 * PgSQL_PoolGetMode::ALLOW_CREATE is set, and the server's pool_mutex
	 * beneath it, so the caller must hold neither. This call never selects a
	 * different server than @p srv.
	 *
	 * @param srv Server to serve the request from. Must not be null.
	 * @param expected_hostgroup_id Hostgroup @p srv must still belong to when
	 *        creation is attempted.
	 * @param match_key Exact pool match key for reuse and for registering a
	 *        newly created connection.
	 * @param sess Session the connection is acquired for.
	 * @param mode Permitted paths: exact match, reset reuse, create, and
	 *        skip-busy-pool.
	 * @param selected_max_connections Connection limit used for the saturation
	 *        estimate; 0 disables it.
	 * @param expected_server_list_generation Server-list generation the caller
	 *        made its decision on. 0 disables the guard. A mismatch returns with
	 *        result.retry_after_config_change set and no connection, which the
	 *        caller must distinguish from an ordinary miss.
	 * @param expected_startup_config_generation Startup-config generation guard,
	 *        with the same 0-disables and mismatch behaviour.
	 * @return The connection plus the reason nothing was acquired. A connection
	 *         with source==CREATED has already been registered in the server's
	 *         USED list with polardb_reader_pool_connect_pending set: its socket
	 *         is not connected yet and the caller owns finishing the connect or
	 *         returning it.
	 */
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

	void push_MyConn_to_pool(PgSQL_Connection *, bool _lock=true,
		PgSQL_Thread* counter_thread=nullptr);
	void push_MyConn_to_pool_array(
		PgSQL_Connection **, unsigned int,
		PgSQL_Thread* counter_thread = nullptr);
	/**
	 * @brief Return a PolarDB connection to its exact shared pool.
	 *
	 * REUSE_LOCAL_RETURN_CHECK is valid only immediately after the reader-local
	 * decision checked the same connection. A returning worker is excluded from
	 * reservation selection for the connection it is returning.
	 *
	 * @param conn Connection to return. Must not be null and must have a parent.
	 * @param return_check Whether to recheck the connection or to trust the
	 *        immediately preceding reader-local decision.
	 * @param unreturned_action Whether a connection rejected by the pool is
	 *        destroyed here or returned in result.detached_connection.
	 * @param returning_thread Worker performing the return, used for its
	 *        exclusion from reservation selection and for its counters.
	 * @return Exact ownership result. NOT_HANDLED leaves @p conn untouched;
	 *         STORED and DESTROYED consume it; DETACHED returns it to the caller.
	 */
	PoolReturnResult return_connection_with_match_key(
		PgSQL_Connection* conn, PgSQL_PoolReturnCheck return_check,
		RejectedConnectionAction unreturned_action,
		PgSQL_Thread* returning_thread = nullptr);
#if POLARDB_PROXY
	/**
	 * @brief Hand a connection a worker wanted to keep to another worker that is
	 *        waiting for capacity.
	 *
	 * @param conn Connection the returning worker still holds.
	 * @param expected_match_key Key the caller decided on. The connection's key
	 *        is recomputed and must still equal it.
	 * @param returning_worker_index Worker offering the connection; it is
	 *        excluded from selection.
	 * @return CONNECTION_INVALID when the arguments or the connection state are
	 *         unusable, including a recomputed match key that no longer equals
	 *         @p expected_match_key because the session state drifted; the
	 *         connection is untouched. NO_REMOTE_REQUEST when no other worker
	 *         wanted it and the returning worker keeps it. CONNECTION_RESERVED
	 *         when the connection left the returning worker's control and became
	 *         another worker's reservation. The resulting wake is signalled
	 *         inside this call, unlike the PgSQL_SrvC-level API where the caller
	 *         must signal it.
	 */
	PolarDB_ReaderRemoteReservationResult reserve_retained_reader_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& expected_match_key,
		unsigned int returning_worker_index);
	/**
	 * @brief Return a batch of reader connections to the shared pool in one pass
	 *        over the server's pool lock.
	 *
	 * The batch is optimised for connections that all share
	 * connections[0]->parent: those are returned under a single pool_mutex
	 * acquisition. A connection with a different parent is detached
	 * individually and appended to @p detached_connections; one whose parent
	 * does not own it is reported through proxy_error and still detached.
	 * Reservation wakes produced by the batch are signalled inside this call,
	 * unlike the per-connection PgSQL_SrvC API.
	 *
	 * @param thread Worker returning the batch; its index is excluded from
	 *        reservation selection.
	 * @param connections Connections to return.
	 * @param connection_count Number of entries in @p connections.
	 * @param detached_connections Every connection that could not be returned to
	 *        the pool is appended here and becomes the caller's to delete. The
	 *        vector is appended to, never cleared, so the caller must not assume
	 *        it starts empty.
	 */
	void polardb_return_reader_connections(
		PgSQL_Thread* thread,
		PgSQL_Connection* const* connections, size_t connection_count,
		std::vector<PgSQL_Connection*>& detached_connections);
	void polardb_return_reader_connections(
		PgSQL_Thread* thread,
		const std::vector<PgSQL_Connection*>& connections,
		std::vector<PgSQL_Connection*>& detached_connections) {
		polardb_return_reader_connections(
			thread, connections.data(), connections.size(),
			detached_connections);
	}
#endif // POLARDB_PROXY
#if POLARDB_PROXY
	PolarDB_ReaderLocalReturnDecision polardb_reader_local_return_decision(
		PgSQL_Connection* conn,
		unsigned int excluded_worker_index = UINT_MAX);
	void polardb_account_reader_return_rejection(
		PolarDB_ReaderConnectionReturnStatus status);
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
	// The topology snapshot deliberately contains no PgSQL_HGC/PgSQL_SrvC
	// pointers. That does not extend to the server-list snapshot below, which
	// does hold PgSQL_SrvC pointers; see PolarDB_ServerListSnapshot.

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
	using PolarDB_HG_Policy = PolarDB_HG_PolicySnapshot;
	using PolarDB_HG_Config = PolarDB_HG_ConfigSnapshot;
	static inline PolarDB_StartupProfile polardb_startup_profile_for_config(
			const PolarDB_HG_Config& config, int fallback_proxy_protocol,
			bool profile_off) {
		if (!config.is_polardb_hostgroup || profile_off) {
			return PolarDB_StartupProfile::from_protocol(
				PolarDB_ProxyProtocol::OFF);
		}
		const int protocol = config.policy.proxy_protocol >= 0
			? config.policy.proxy_protocol : fallback_proxy_protocol;
		return PolarDB_StartupProfile::from_protocol(
			polardb_proxy_protocol_from_int(protocol));
	}

	struct PolarDB_HG_SnapshotEntry {
		PolarDB_HG_Config config;
		std::shared_ptr<std::atomic<uint64_t>> group_lsn;
		std::shared_ptr<std::atomic<uint64_t>> max_replica_replay_lsn;
		std::shared_ptr<std::atomic<uint64_t>> writer_epoch;
	};

	struct PolarDB_TopologySnapshot {
		uint64_t generation{0};
		std::unordered_map<unsigned int, PolarDB_HG_SnapshotEntry> by_hostgroup;
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

	/**
	 * @brief Immutable per-hostgroup list of selectable servers for one
	 *        server-list generation.
	 *
	 * Unlike PolarDB_TopologySnapshot, this snapshot deliberately does hold
	 * PgSQL_SrvC pointers, through PolarDB_ServerSnapshotEntry, plus a
	 * shared_ptr into the hostgroup container. Reader selection needs the server
	 * objects themselves, and copying them per request would be far more
	 * expensive than keeping the snapshot alive.
	 *
	 * Those pointers are guaranteed valid only while a shared_ptr to this
	 * snapshot is held. A server removed from configuration is retired rather
	 * than deleted, and it is reclaimed once no snapshot still references it, so
	 * dropping the snapshot while still using a PgSQL_SrvC* taken from it leaves
	 * a dangling pointer.
	 */
	struct PolarDB_ServerListSnapshot {
		uint64_t generation{0};
		std::unordered_map<unsigned int, PolarDB_ServerListEntry> by_hostgroup;
	};

	/**
	 * @brief Copy one hostgroup config and its current writer epoch.
	 *
	 * The returned value contains no owning pointers. This keeps shared-pointer
	 * reference counts off the query path and remains valid after a topology
	 * reload.
	 */
	PolarDB_HG_Config get_polardb_hg_config(unsigned int hostgroup_id);
	/** Same lookup after the caller has already observed polardb_active=true. */
	PolarDB_HG_Config get_active_polardb_hg_config(unsigned int hostgroup_id);
	/**
	 * Query-worker lookup from the snapshot refreshed by the publication wake.
	 * Immutable policy comes directly from that snapshot; the returned value
	 * samples the live writer epoch so a failover cannot leave request routing on
	 * an old timeline. It performs no topology-generation or shared_ptr operation
	 * and fails closed before the first refresh.
	 */
	PolarDB_HG_Config get_thread_cached_polardb_hg_config(
		unsigned int hostgroup_id) const;
	/** Immutable-policy lookup; callers must not use writer_epoch from this view. */
	const PolarDB_HG_Config* find_thread_cached_polardb_hg_config(
		unsigned int hostgroup_id) const;

	PolarDB_HG_Policy get_polardb_hg_policy(unsigned int hostgroup_id);
	std::shared_ptr<const PolarDB_ServerListSnapshot>
		get_polardb_server_list_snapshot() const;
	bool polardb_hostgroup_has_usable_server(unsigned int hostgroup_id) const;
	uint64_t polardb_server_list_snapshot_generation(
		const std::shared_ptr<const void>& snapshot) const;
	bool polardb_reader_connection_is_current(
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
	 * @brief Validate a candidate global policy against loaded hostgroup
	 *        overrides.
	 *
	 * @return Empty on success; otherwise a human-readable reason. This is an
	 *         admin-path check and is never called for a query.
	 */
	std::string polardb_loaded_hostgroup_policy_error(
		const PolarDB_ParsedGlobalConfigValue& global_config) const;
	/**
	 * @brief Whether any loaded PolarDB group resolves to GLOBAL_LSN.
	 *
	 * Used when validating a global timeout action. Hostgroup overrides are
	 * included, so a setting change cannot weaken an already-loaded group even
	 * when the global consistency mode itself is not GLOBAL_LSN.
	 */
	bool polardb_has_effective_global_lsn(int global_consistency_mode) const;

	/**
	 * @brief Update the per-server LSN cache from a monitor observation.
	 *
	 * This is the monitor path, addressed by address and port. RFQ observations
	 * use the PgSQL_SrvC* overload below.
	 *
	 * Acquires the HostGroups_Manager write lock itself and walks every
	 * hostgroup and server, so the caller must not already hold it. Every
	 * hostgroup that carries the address and port is updated; servers that are
	 * not MYSQL_SERVER_STATUS_ONLINE are skipped. Besides the per-server cache
	 * it also advances the replication group's shared group LSN. A physical
	 * replica observation may additionally advance the current-epoch
	 * replica-only replay maximum; primary observations never do.
	 *
	 * @param address Server address.
	 * @param port Server port.
	 * @param lsn Observed LSN value.
	 * @param node_type Physical role reported with the monitor observation.
	 * @return true if a matching ONLINE server was found and this call advanced
	 *         its cached LSN.
	 */
	bool polardb_update_server_lsn_from_monitor(
		const char* address, uint16_t port, uint64_t lsn,
		PolarDB_NodeType node_type);
	/**
	 * @brief Process an RFQ LSN through a direct server pointer.
	 *
	 * The server pointer is used only for PgSQL_SrvC's atomic LSN cache. The
	 * backend hostgroup is resolved through the thread-local topology snapshot,
	 * without taking the global HGM lock or copying reference-counted state.
	 * When the observation is stored, the group's shared LSN is advanced with the
	 * per-server cache. Direct RFQ carries no authoritative physical role and
	 * therefore never advances the replica-only replay maximum.
	 *
	 * @param srv Server the RFQ came from. Must not be null.
	 * @param backend_hostgroup_id Hostgroup the backend belongs to.
	 * @param lsn Observed LSN. Zero is rejected.
	 * @param request_scope Writer hostgroup and epoch the request was planned
	 *        under; an observation from a stale or foreign scope is rejected.
	 * @param worker Optional worker performing the update. When it is given, the
	 *        worker may coalesce repeated observations and skip the store, in
	 *        which case neither the per-server cache nor the group mirror is
	 *        written even though the call still succeeds.
	 * @return true when the RFQ LSN was accepted for the current
	 *         replication-group writer epoch. Acceptance does not imply the
	 *         value was stored or that it advanced anything: the cached LSN
	 *         may already be equal or newer, or the store may have been
	 *         coalesced away.
	 */
	bool polardb_accept_rfq_server_lsn(
		PgSQL_SrvC* srv, unsigned int backend_hostgroup_id,
		uint64_t lsn, const PolarDB_WriterScope& request_scope,
		PgSQL_Thread* worker = nullptr);

	/**
	 * @brief Read the highest LSN reported for a PolarDB group.
	 *
	 * For the current writer epoch, the value is the highest accepted monitor
	 * or RFQ sample from any server in the group. It is not the writer's current
	 * WAL position and has no freshness timestamp.
	 *
	 * @param writer_hostgroup_id Writer hostgroup to read.
	 * @return The group LSN, or 0 when PolarDB is inactive, no topology
	 *         snapshot has been stored yet, the hostgroup is not in the snapshot, it
	 *         has no group LSN, or nothing has ever been observed for it.
	 */
	uint64_t get_polardb_group_lsn(unsigned int writer_hostgroup_id);

	/**
	 * @brief Read the highest replay LSN confirmed by a physical replica.
	 *
	 * Primary observations, including a primary copied into the reader hostgroup,
	 * never update this value. It is reset before the writer epoch advances.
	 *
	 * @param writer_scope Writer hostgroup and epoch that produced the `w` marker.
	 * @return Replica replay maximum for the current epoch, or 0 when unavailable.
	 */
	uint64_t get_polardb_max_replica_replay_lsn(
		const PolarDB_WriterScope& writer_scope);

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
	 * @param confirm_reader_group_capacity When the selected reader is
	 *        saturated, keep scanning the remaining readers before reporting the
	 *        group busy, and do not skip a reader whose pool lock is contended.
	 *        Pass true from a worker that is already waiting for capacity and
	 *        needs a definite answer about the whole reader group rather than a
	 *        cheap first-choice attempt.
	 * @return Connection plus the exact result of the attempt. When the reader
	 *         pool is not constructed, a default-constructed
	 *         PolarDB_ReaderResult is returned without any selection being
	 *         attempted.
	 */
	PolarDB_ReaderResult polardb_acquire_reader_connection(
		unsigned int hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address = nullptr, int exclude_port = -1,
		bool confirm_reader_group_capacity = false);
	/**
	 * @brief Check whether a reader still satisfies the current topology,
	 *        status, latency, exclusion and lag requirements for one request.
	 *
	 * @param hostgroup_id Reader hostgroup @p server must still belong to.
	 * @param server Reader to validate. Must not be null.
	 * @param reader_plan Per-reader lag-cap filters and fallback policy.
	 * @param wait_spec Wait target and timeout for this query.
	 * @param exclude_address Optional backend address that is not eligible.
	 * @param exclude_port Backend port to exclude with @p exclude_address.
	 * @param selected_server_snapshot Optional. On success it receives the
	 *        server-list snapshot reference that keeps @p server alive; the
	 *        caller must retain it for as long as it uses that pointer.
	 * @return true when the reader may serve the request. false when it may not,
	 *         and also whenever the reader pool is not constructed.
	 */
	bool polardb_reader_server_can_serve_request(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address = nullptr, int exclude_port = -1,
		std::shared_ptr<const void>* selected_server_snapshot = nullptr) const;
	bool polardb_reader_pool_reservation_match_key(
		unsigned int hostgroup_id, PgSQL_Session* sess,
		const PolarDB_WaitSpec& wait_spec,
		PgSQL_PoolMatchKey* match_key) const;
	/**
	 * @brief Deliver a reservation wake to the worker it was created for.
	 *
	 * This is the required counterpart to every PolarDB_ReaderPoolReservationWake
	 * produced under a server pool_mutex. The caller must hold no server
	 * pool_mutex when calling: an undeliverable wake re-enters
	 * PgSQL_SrvC::cancel_reader_pool_reservation() on that server.
	 *
	 * The wake is taken by value and consumed. When the target worker can no
	 * longer be woken, its reservation is cancelled; returning that connection
	 * to the pool can immediately satisfy another waiting worker, and this call
	 * follows that chain until a wake is delivered or the chain ends. The caller
	 * does not have to drive the chain, but should know it can happen here.
	 *
	 * @param wake Wake record to deliver. An invalid wake is a no-op, so an
	 *        unconditional call after a return path is safe.
	 */
	void polardb_route_reader_reservation_wake(PolarDB_ReaderPoolReservationWake wake);

	/**
	 * @brief Queue one lazy transaction-split pool warmup request.
	 *
	 * Pure producer: it only records the request key (reader HG, user, database,
	 * proxy listener identity) and returns. It never opens a socket and never
	 * takes the HGM write lock.
	 */
	void request_split_warmup(unsigned int reader_hostgroup_id,
		const char* username, const char* password, const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn,
		const PgSQL_SrvC* target_server = nullptr);

	/**
	 * @brief Drain queued split warmup requests into connected pool entries.
	 *
	 * Called from the HGM maintenance pass. It reserves capacity under the HGM
	 * write lock, opens the backend socket without holding that lock, then
	 * re-locks briefly to add the connected backend. Pooled-only split
	 * reads never run a connect handshake in the transaction path.
	 */
	void warm_split_pools();
	void polardb_refresh_split_warmup_variables();
	void polardb_refresh_thread_snapshots();
	void shutdown_split_warmup_thread();
#endif // POLARDB_PROXY

private:
	void update_hostgroup_manager_mappings();
	uint64_t get_pgsql_servers_checksum(SQLite3_result* runtime_pgsql_servers = nullptr);
	uint64_t get_pgsql_servers_v2_checksum(SQLite3_result* incoming_pgsql_servers_v2 = nullptr);
#if POLARDB_PROXY
	friend class PgSQL_HGC;
	bool polardb_resolve_reader_server_snapshot(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* server_snapshot) const;
	bool polardb_reader_server_meets_lag_policy(
		PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec, uint64_t now_us) const;
	const PolarDB_TopologySnapshot* get_polardb_topology_snapshot_cached() const;
	std::string polardb_writer_identity_under_hgm_write_lock(unsigned int writer_hostgroup_id);
	void polardb_reset_lsn_cache_for_hostgroup_under_hgm_write_lock(unsigned int hostgroup_id);
	void polardb_refresh_writer_epoch_under_hgm_write_lock(
		unsigned int writer_hostgroup_id, const char* reason);
	void polardb_refresh_all_writer_epochs_under_hgm_write_lock(const char* reason);
	void polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
	void polardb_retire_server_under_fast_topology_lock(PgSQL_SrvC* srv);
	void polardb_prune_retired_server_snapshots_under_fast_topology_lock();
	void polardb_detach_reclaimable_retired_servers_under_fast_topology_lock(
		std::vector<PgSQL_SrvC*>& servers_to_delete);
	void polardb_clear_snapshots_for_shutdown();

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
	bool polardb_snapshots_cleared_for_shutdown_{false};
#endif // POLARDB_PROXY
};


#endif /* PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H */
