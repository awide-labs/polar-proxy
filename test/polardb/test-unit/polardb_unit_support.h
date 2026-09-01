/**
 * @file polardb_unit_support.h
 * @brief Shared construction and inspection helpers for PolarDB component tests.
 *
 * These helpers create real ProxySQL hostgroup, session, worker, and connection
 * objects. Focused value and parser tests should use polardb_unit_common.h
 * instead. See README.md for helper choice, ownership, and test structure.
 */

#ifndef POLARDB_UNIT_SUPPORT_H
#define POLARDB_UNIT_SUPPORT_H

#include "PgSQL_PolarDB.h"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

class PgSQL_Connection;
class PgSQL_HGC;
class PgSQL_HostGroups_Manager;
class PgSQL_PolarDB_ReaderPool;
class PgSQL_Session;
class PgSQL_SrvC;
class PgSQL_SrvConnList;
class PgSQL_Thread;
class PgSQL_Threads_Handler;
class SQLite3_result;
struct PgSQL_PoolMatchKey;
struct PgSQL_SplitWarmupRequest;
struct pg_conn;
typedef struct pg_conn PGconn;

// Build explicit configuration rows or locate objects already owned by HGM.
// Row builders are intended for immediate handoff to HGM staging methods.
SQLite3_result* make_polardb_replication_row_with_protocol(
	int writer_hg, int reader_hg, const char* proxy_protocol,
	const char* txn_split = "0");
SQLite3_result* make_polardb_replication_row(
	int writer_hg, int reader_hg);
SQLite3_result* make_pgsql_servers_result(
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port,
	int writer_max_connections = 50);
SQLite3_result* make_pgsql_servers_result_two_readers(
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1,
	int first_reader_max_connections = 50,
	int second_reader_max_connections = 50);
PgSQL_SrvC* find_pgsql_server(
	PgSQL_HGC* hostgroup, const char* address, int port);
bool find_prometheus_counter_value(const char* name, double* value);
bool find_prometheus_gauge_value(const char* name, double* value);
void set_test_userinfo(PgSQL_Connection* conn);
void set_test_pgsql_defaults(PgSQL_Connection* conn);
PgSQL_Connection* make_cached_reader_connection(PgSQL_SrvC* reader);
void stage_polardb_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);
void stage_polardb_topology_with_txn_split(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);
void stage_polardb_topology_two_readers(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1);

// Narrow test-only access to component state that is not public production API.
struct PolarDB_ReaderRetentionUnitAccess {
	static void set_worker_index(
		PgSQL_Thread* worker, unsigned int worker_index);
	static void set_local_connection_count(
		PgSQL_Thread* worker, unsigned int count);
	static unsigned int local_connection_count(
		const PgSQL_Thread* worker);
};

struct PolarDB_WorkerLifecycleUnitAccess {
	static std::mutex& mutex(PgSQL_Threads_Handler* handler);
	static std::mutex& wake_mutex(PgSQL_Thread* worker);
	static void set_shutdown(
		PgSQL_Threads_Handler* handler, bool shutdown_started);
};

struct PolarDB_WarmupUnitAccess {
	static std::string queued_key(
		const PgSQL_PolarDB_ReaderPool& pool);
	static bool drain(
		PgSQL_PolarDB_ReaderPool& pool,
		std::vector<PgSQL_SplitWarmupRequest>& requests);
	static bool register_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key);
	static bool rerun_pending(
		const PgSQL_PolarDB_ReaderPool& pool, const std::string& key);
	static void finish_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key,
		const PgSQL_SplitWarmupRequest& request);
	static bool apply_startup_parameters(
		PgSQL_Connection* conn,
		const PgSQL_SplitWarmupRequest& request);
};

struct PolarDB_OneReaderTestTopology {
	PgSQL_HGC* writer_hostgroup{nullptr};
	PgSQL_HGC* reader_hostgroup{nullptr};
	PgSQL_SrvC* writer{nullptr};
	PgSQL_SrvC* reader{nullptr};

	bool valid() const {
		return writer_hostgroup && reader_hostgroup && writer && reader;
	}
};

struct PolarDB_TwoReaderTestTopology {
	PgSQL_HGC* writer_hostgroup{nullptr};
	PgSQL_HGC* reader_hostgroup{nullptr};
	PgSQL_SrvC* writer{nullptr};
	PgSQL_SrvC* first_reader{nullptr};
	PgSQL_SrvC* second_reader{nullptr};

	bool valid() const {
		return writer_hostgroup && reader_hostgroup && writer &&
			first_reader && second_reader;
	}
};

// Captures one worker counter so a test can assert only its own delta.
class PolarDB_ThreadCounterSnapshot {
public:
	PolarDB_ThreadCounterSnapshot(
		const PgSQL_Thread* worker, unsigned int counter_index);

	unsigned long long delta() const;

private:
	const PgSQL_Thread* worker_;
	unsigned int counter_index_;
	unsigned long long initial_value_;
};

// Stage ordinary topologies and return borrowed HGM-owned objects. The pointers
// remain valid only until the next topology replacement or HGM cleanup.
PolarDB_OneReaderTestTopology stage_polardb_one_reader_test_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);

PolarDB_TwoReaderTestTopology stage_polardb_two_reader_test_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1);

void stage_polardb_topology_many_readers(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_prefix, int reader_count,
	int reader_base_port, int reader_weight = 1);

// Attach the standard test frontend objects to a session. Normal session
// reset/destruction releases the allocated stream and connection.
void attach_test_frontend(
	PgSQL_Session& session, PgSQL_Thread* worker = nullptr);

// Construct protocol identities, packets, and connections for component tests.
// make_cached_reader_connection() returns a caller-owned connection until it is
// inserted into a ReaderPool list.
PolarDB_StartupIdentity unit_proxy_identity();
PolarDB_StartupIdentity unit_other_proxy_identity();
PGconn* unit_connected_pgconn();
PtrSize_t unit_simple_query_packet(const char* query);

// ReaderPool helpers preserve the production matching index while tests add,
// inspect, or remove synthetic connections.
unsigned int unit_reader_pool_shared_free_count(PgSQL_SrvC* reader);
uint64_t unit_reader_pool_options_key(PgSQL_Connection* conn);
void unit_reader_pool_refresh_key(PgSQL_Connection* conn);
void unit_reader_pool_prepare_free_conn(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
PgSQL_PoolMatchKey unit_reader_pool_match_key(PgSQL_Connection* conn);
bool unit_register_reader_pool_capacity_request(
	PgSQL_SrvC* reader, unsigned int worker_index, uint64_t token,
	const PgSQL_PoolMatchKey& key);
void unit_reader_pool_add_matching(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
void unit_reader_pool_add_shared(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
bool unit_reader_pool_clear_shared(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);

// Focused accessors used to exercise private warmup and match-index recovery.
size_t pgsql_polardb_unit_collect_split_warmup_targets(
	PgSQL_PolarDB_ReaderPool* pool,
	const PgSQL_SplitWarmupRequest& request,
	std::vector<PgSQL_SplitWarmupRequest>& target_requests,
	bool* found_hostgroup,
	bool* saw_eligible_target,
	bool* saw_compatible_free);
void pgsql_polardb_unit_corrupt_match_key_positions(
	PgSQL_SrvConnList* connections);
void pgsql_polardb_unit_swap_match_key_positions(
	PgSQL_SrvConnList* connections, unsigned int first,
	unsigned int second);
size_t pgsql_polardb_unit_empty_match_bucket_count(
	PgSQL_SrvConnList* connections);

#endif // POLARDB_UNIT_SUPPORT_H
