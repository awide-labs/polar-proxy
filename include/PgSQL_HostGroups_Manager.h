#ifndef PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H
#define PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H
#include "proxysql.h"
#include "cpp.h"
#include "proxysql_gtid.h"
#include "proxysql_admin.h"
#include "PgSQL_PolarDB_Counters.h"
#include <atomic>
#include <memory>
#include <thread>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>

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
// LSN consistency policy. Must stay in sync with
// ADMIN_SQLITE_TABLE_PGSQL_REPLICATION_HOSTGROUPS_V3_0_5.
#define MYHGM_PgSQL_REPLICATION_HOSTGROUPS "CREATE TABLE pgsql_replication_hostgroups (writer_hostgroup INT CHECK (writer_hostgroup>=0) NOT NULL PRIMARY KEY , reader_hostgroup INT NOT NULL CHECK (reader_hostgroup<>writer_hostgroup AND reader_hostgroup>=0) , check_type VARCHAR CHECK (LOWER(check_type) IN ('read_only', 'polardb')) NOT NULL DEFAULT 'read_only' , txn_split_enabled INT CHECK (txn_split_enabled IN (0, 1) AND (txn_split_enabled = 0 OR LOWER(check_type) = 'polardb')) NOT NULL DEFAULT 0 , consistency_mode VARCHAR CHECK (LOWER(consistency_mode) IN ('default', 'off', 'lsn', 'primary')) NOT NULL DEFAULT 'default' , max_lag_bytes INT NOT NULL DEFAULT -1 , lsn_wait_timeout_ms INT NOT NULL DEFAULT -1 , proxy_protocol VARCHAR CHECK (LOWER(proxy_protocol) IN ('default', 'v15', 'legacy', 'off')) NOT NULL DEFAULT 'default' , comment VARCHAR NOT NULL DEFAULT '' , UNIQUE (reader_hostgroup))"
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

#if POLARDB_PROXY
/**
 * @brief Demand signal for lazy transaction-split replica-pool warmup.
 *
 * Split reads require an already-pooled replica connection. When the pool is
 * empty, the session queues this key and the HGM maintenance pass opens a
 * connected backend before publishing it into the free list. The request carries
 * the real startup client context captured from the session that requested
 * warmup. SSL and proxy session/cancel fields are reserved for future startup
 * keys; identity matching is active now.
 */
struct PgSQL_SplitWarmupRequest {
	unsigned int hostgroup_id = 0;
	std::string username;
	std::string password;
	std::string dbname;
	PolarDB_StartupClientContext startup_client;
	int identity_match = static_cast<int>(PolarDB_SplitWarmupIdentity::STRICT);
	unsigned long long requested_at_us = 0;

	PgSQL_SplitWarmupRequest() = default;
	PgSQL_SplitWarmupRequest(
		unsigned int hg,
		const char* user,
		const char* pass,
		const char* db,
		const PolarDB_StartupClientContext& client_context,
		int match_mode,
		unsigned long long now_us)
		: hostgroup_id(hg)
		, username(user ? user : "")
		, password(pass ? pass : "")
		, dbname(db ? db : "")
		, startup_client(client_context)
		, identity_match(match_mode)
		, requested_at_us(now_us) {}
};
#endif // POLARDB_PROXY

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

class PgSQL_SrvConnList {
	private:
	PgSQL_SrvC *mysrvc;
	int find_idx(PgSQL_Connection *c) {
		//for (unsigned int i=0; i<conns_length(); i++) {
		for (unsigned int i=0; i<conns->len; i++) {
			PgSQL_Connection *conn = nullptr;
			conn = (PgSQL_Connection *)conns->index(i);
			if (conn==c) {
				return (unsigned int)i;
			}
		}
		return -1;
	}
	public:
	PtrArray *conns;
	PgSQL_SrvConnList(PgSQL_SrvC *);
	~PgSQL_SrvConnList();
	void add(PgSQL_Connection *);
	void remove(PgSQL_Connection *c) {
		int i = -1;
		i = find_idx(c);
		assert(i>=0);
		conns->remove_index_fast((unsigned int)i);
	}
	PgSQL_Connection *remove(int);
	PgSQL_Connection * get_random_MyConn(PgSQL_Session *sess, bool ff, bool only_pooled = false);
	void get_random_MyConn_inner_search(unsigned int start, unsigned int end, unsigned int& conn_found_idx, unsigned int& connection_quality_level, unsigned int& number_of_matching_session_variables, const PgSQL_Connection * client_conn);
	unsigned int conns_length() { return conns->len; }
	void drop_all_connections();
	PgSQL_Connection *index(unsigned int);
};

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
	unsigned int current_latency_us;
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
#if POLARDB_PROXY
	// =========================================================================
	// PolarDB per-server LSN tracking (read-your-writes consistency)
	// =========================================================================
	// Updated by PgSQL_HostGroups_Manager::polardb_update_server_lsn() from two
	// sources: monitor health checks, and the LSN that a backend reports in the
	// extended ReadyForQuery (RFQ) message after running a query. Read without a
	// lock by reader acquisition and by the byte-lag / cache-freshness helpers.
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
	void connect_error(int, bool get_mutex);
	void shun_and_killall();
#if POLARDB_PROXY
	bool polardb_advance_lsn(uint64_t lsn, uint64_t observed_at_us);
#endif // POLARDB_PROXY
	/**
	 * @brief Update the maximum number of used connections
	 * @return The maximum number of used connections
	 */
	unsigned int update_max_connections_used()
	{
		unsigned int connections_used = ConnectionsUsed->conns_length();
		if (max_connections_used < connections_used)
			max_connections_used = connections_used;
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
	 * the policy quickly. Supported consistency modes are off, lsn, and primary.
	 *
	 * The two shared_ptr<atomic> cells (primary LSN mirror and writer epoch) are
	 * the only fields a query thread reads at request time. They are copied by
	 * value into the published topology snapshot, so a query thread reads them
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
		std::string consistency_mode;      // consistency mode string (off/lsn/primary)
		int consistency_mode_enum{-1};     // parsed enum value; -1 = use global default
		int lsn_wait_timeout_ms{0};        // polar_xact_split_wait_lsn timeout in ms
		std::string proxy_protocol;        // default/v15/legacy/off
		int proxy_protocol_enum{-1};       // parsed enum value; -1 = inherit global
		// Shared cells are copied into the published PolarDB topology snapshot so
		// query threads can read them without holding an HGM lock or retaining an
		// HGC pointer.
		std::shared_ptr<std::atomic<uint64_t>> polardb_primary_lsn{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // latest LSN from primary (for lag calc)
		std::shared_ptr<std::atomic<uint64_t>> polardb_writer_epoch{
			std::make_shared<std::atomic<uint64_t>>(0)
		};                                  // bumps when the writer identity set changes
		std::string polardb_writer_identity; // sorted non-OFFLINE_HARD address:port set
		bool polardb_writer_identity_initialized{false};
	} repl_config;
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
		std::atomic<unsigned long long> polardb_lsn_stale_count{0};          // stale-LSN skips in reader acquisition
		std::atomic<unsigned long long> polardb_write_missing_lsn{0};        // writer RFQ carried no LSN; automatic RYW reads forced to writer
		std::atomic<unsigned long long> polardb_read_missing_lsn{0};         // read RFQ carried no LSN while SESSION_LSN tracked observations
		std::atomic<unsigned long long> polardb_primary_lsn_unknown{0};      // PRIMARY baseline requested but writer mirror had no LSN
		std::atomic<unsigned long long> polardb_rfq_best_effort_degraded_routes{0}; // RFQ-unavailable reads sent to reader without wait
		std::atomic<unsigned long long> polardb_consistency_writer_fallback{0}; // consistency reads redirected to writer after reader acquisition status
		std::atomic<unsigned long long> polardb_wait_reads_retried_on_writer{0}; // wait-wrapped reads retried once on the writer after timeout or reader loss
		std::atomic<unsigned long long> polardb_rfq_profile_skipped{0};      // incompatible pooled-backend skip attempts for RFQ-LSN reads
		std::atomic<unsigned long long> polardb_rfq_profile_evicted{0};      // incompatible free pooled backends evicted to create RFQ-LSN-capable replacements
		std::atomic<unsigned long long> polardb_tl_cache_bypassed_for_target{0}; // thread-local cache bypasses for consistency-target RFQ-LSN reads
		std::atomic<unsigned long long> polardb_target_lsn_preferred{0};     // reader choice narrowed to fresh cached LSN >= target
		std::atomic<unsigned long long> polardb_target_lsn_fallback_wait{0}; // no target-reached reader acquired; wrapper remains correctness gate
		std::atomic<unsigned long long> polardb_session_target_epoch_reset{0}; // session LSN targets/latches cleared after writer group/epoch change

		// Wait wrapping / RYW routing counters.
		//
		// Counter meaning:
		//  - PolarDB_Session_LSN_Routing / polardb_session_lsn_routing counts the
		//    route-plan decision: a read was sent to a reader with an LSN wait
		//    requirement.
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
		std::atomic<unsigned long long> polardb_wait_wrap_prepared{0};        // wait wrapper intent prepared (REPLICA_WITH_WAIT)
		std::atomic<unsigned long long> polardb_wait_wrap_bypassed{0};       // selected reader reached consistency target -> wrapper skipped
		std::atomic<unsigned long long> polardb_wait_lsn_sent{0};             // LSN wait wrapper successfully installed/sent
		std::atomic<unsigned long long> polardb_wait_lsn_sum_us{0};          // total time spent in LSN waits (microseconds)
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
		std::atomic<unsigned long long> polardb_split_conn_reused{0};         // reused already attached split backend
		std::atomic<unsigned long long> polardb_split_conn_cleanup_success{0}; // split backend returned to pool
		std::atomic<unsigned long long> polardb_split_conn_cleanup_failed{0}; // split backend destroyed instead of pooled
		std::atomic<unsigned long long> polardb_split_lsn_wait_count{0};      // split LSN wait wrappers prepared
		std::atomic<unsigned long long> polardb_split_lsn_wait_sum_us{0};     // split LSN wait latency total
		std::atomic<unsigned long long> polardb_split_error_connection_lost{0}; // split replica connection loss
		std::atomic<unsigned long long> polardb_split_error_query_failed{0};  // split user-query failure
		std::atomic<unsigned long long> polardb_split_error_timeout{0};       // split timeout total
		std::atomic<unsigned long long> polardb_split_error_lsn_wait_timeout{0}; // split LSN timeout subset
		std::atomic<unsigned long long> polardb_split_latency_sum_us{0};      // split read latency total
		std::atomic<unsigned long long> polardb_split_latency_count{0};       // split read latency samples
		std::atomic<unsigned long long> polardb_split_warmup_requested{0};    // lazy warmup requests queued
		std::atomic<unsigned long long> polardb_split_warmup_created{0};      // lazy warmup connections created
		std::atomic<unsigned long long> polardb_split_warmup_failed{0};       // lazy warmup requests failed
		std::atomic<unsigned long long> polardb_split_warmup_sum_us{0};       // request-to-pool warmup latency total
		std::atomic<unsigned long long> polardb_split_warmup_count{0};        // request-to-pool warmup samples
		std::atomic<unsigned long long> polardb_warmup_pending{0};            // queued lazy warmup requests

		// Fast gate read on every routing decision; true while any PolarDB
		// hostgroup is configured. Set from the published snapshot on each commit.
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

	void drop_all_idle_connections();
	int get_multiple_idle_connections(int, unsigned long long, PgSQL_Connection **, int);
	SQLite3_result * SQL3_Connection_Pool(bool _reset, int *hid = nullptr);
	SQLite3_result * SQL3_Free_Connections();

	void push_MyConn_to_pool(PgSQL_Connection *, bool _lock=true);
	void push_MyConn_to_pool_array(PgSQL_Connection **, unsigned int);
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
	// Reload safety / hot path:
	// generate_pgsql_replication_hostgroups_table() builds a complete plain-value
	// topology/policy snapshot under the existing commit lock, then publishes it
	// atomically with a generation. Query threads refresh a thread-local cached
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

	/**
	 * @brief Read PolarDB topology and policy from the generation snapshot.
	 */
	PolarDB_HG_Config get_polardb_hg_config(unsigned int hostgroup_id);

	PolarDB_HG_Policy get_polardb_hg_policy(unsigned int hostgroup_id);

	/**
	 * @brief Whether the hostgroup's current effective proxy protocol requests
	 *        RFQ LSN feedback.
	 */
	bool polardb_hostgroup_requests_rfq_lsn(unsigned int hostgroup_id);

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
	 * rereading mutable PgSQL_HGC fields in the result hot path.
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
	 * RFQ startup-profile compatibility filters. When consistency_target_lsn is present it
	 * prefers fresh cached readers already at or beyond that LSN, but it MUST NOT
	 * reject the original replica set merely because cached polardb_current_lsn is
	 * below the session target or stale; polar_xact_split_wait_lsn in the wrapped
	 * query is what waits to the session target.
	 *
	 * Consistency reads pass only_pooled=false, so a cold eligible replica can
	 * create a new backend connection and still execute the current query on the
	 * replica. Future pooled-only callers can pass only_pooled=true to require an
	 * already-free backend.
	 *
	 * @param hid           Reader hostgroup id.
	 * @param sess          Session requesting the connection.
	 * @param reader_plan  Required target and per-reader lag-cap filters.
	 * @param only_pooled   If true, return only already-pooled connections.
	 * @param exclude_address Optional backend address to skip.
	 * @param exclude_port  Backend port to skip with @p exclude_address.
	 * @return Connection plus the precise acquisition outcome.
	 */
	PolarDB_ReaderResult get_MyConn_polardb_reader(unsigned int hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan, bool only_pooled,
		const char* exclude_address = nullptr, int exclude_port = -1);

	/**
	 * @brief Queue one lazy transaction-split pool warmup request.
	 *
	 * Pure producer: it only records the demand key (reader HG, user, database,
	 * proxy listener identity) and returns. It never opens a socket and never
	 * takes the HGM write lock.
	 */
	void request_split_warmup(unsigned int reader_hostgroup_id,
		const char* username, const char* password, const char* dbname,
		const PolarDB_StartupClientContext& startup_client);

	/**
	 * @brief Drain queued split warmup requests into connected pool entries.
	 *
	 * Called from the HGM maintenance pass. It reserves capacity under the HGM
	 * write lock, opens the backend socket without holding that lock, then
	 * re-locks briefly to publish the fully connected backend. Pooled-only split
	 * reads never run a connect handshake in the transaction path.
	 */
	void warm_split_pools();
#endif // POLARDB_PROXY

private:
	void update_hostgroup_manager_mappings();
	uint64_t get_pgsql_servers_checksum(SQLite3_result* runtime_pgsql_servers = nullptr);
	uint64_t get_pgsql_servers_v2_checksum(SQLite3_result* incoming_pgsql_servers_v2 = nullptr);
#if POLARDB_PROXY
	std::shared_ptr<const PolarDB_TopologySnapshot> get_polardb_topology_snapshot_cached() const;
	std::string polardb_writer_identity_locked(unsigned int writer_hostgroup_id);
	void polardb_reset_lsn_cache_for_hostgroup_locked(unsigned int hostgroup_id);
	void polardb_refresh_writer_epoch_locked(unsigned int writer_hostgroup_id, const char* reason);
	void polardb_refresh_all_writer_epochs_locked(const char* reason);

	// PolarDB HG topology cache populated from pgsql_replication_hostgroups;
	// empty until then, so accessors fail safe.
	std::unordered_map<unsigned int, unsigned int> polardb_writer_to_reader_;
	std::unordered_map<unsigned int, unsigned int> polardb_reader_to_writer_;
	std::unordered_set<unsigned int> polardb_hostgroups_;  // all HGs in the PolarDB config
	std::shared_ptr<const PolarDB_TopologySnapshot> polardb_topology_snapshot_;
	std::atomic<uint64_t> polardb_topology_generation_{0};
	std::queue<PgSQL_SplitWarmupRequest> split_warmup_queue_;
	std::unordered_set<std::string> split_warmup_queued_;
	std::unordered_set<std::string> split_warmup_inflight_;
	std::mutex split_warmup_mutex_;
#endif // POLARDB_PROXY
};


#endif /* PROXYSQL_PGSQL_HOSTGROUPS_MANAGER_H */
