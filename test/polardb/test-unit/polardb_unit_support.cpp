/**
 * @file polardb_unit_support.cpp
 * @brief Shared support for PolarDB component tests.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_PolarDB_ReaderPool.h"
extern "C" {
#include "postgres_fe.h"
#include "libpq-int.h"
}
#undef snprintf
#undef vsnprintf

#include "polardb_unit_common.h"
#include "polardb_unit_support.h"

#include <cassert>
#include <cstring>

SQLite3_result* make_polardb_replication_row_with_protocol(
		int writer_hg, int reader_hg, const char* proxy_protocol,
		const char* txn_split) {
	SQLite3_result* result = new SQLite3_result(9);
	char writer_buf[16];
	char reader_buf[16];
	snprintf(writer_buf, sizeof(writer_buf), "%d", writer_hg);
	snprintf(reader_buf, sizeof(reader_buf), "%d", reader_hg);

	char* row[] = {
		writer_buf,
		reader_buf,
		(char*)"polardb",
		(char*)(txn_split ? txn_split : "0"),
		(char*)"session_lsn",
		(char*)"1000",
		(char*)"0",
		(char*)(proxy_protocol ? proxy_protocol : "v15"),
		(char*)"writer epoch unit"
	};
	result->add_row(row);
	return result;
}

SQLite3_result* make_polardb_replication_row(
		int writer_hg, int reader_hg) {
	return make_polardb_replication_row_with_protocol(
		writer_hg, reader_hg, "v15");
}

SQLite3_result* make_pgsql_servers_result(
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_addr, int reader_port) {
	SQLite3_result* result = new SQLite3_result(11);
	char writer_hg_buf[16];
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char reader_port_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", reader_port);

	char* writer_row[] = {
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
		(char*)"polardb writer epoch unit"
	};
	result->add_row(writer_row);

	char* reader_row[] = {
		reader_hg_buf,
		(char*)reader_addr,
		reader_port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"0",
		(char*)"polardb writer epoch unit"
	};
	result->add_row(reader_row);
	return result;
}

SQLite3_result* make_pgsql_servers_result_two_readers(
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg,
		const char* first_reader_addr, int first_reader_port,
		const char* second_reader_addr, int second_reader_port,
		int first_reader_weight, int second_reader_weight,
		int first_reader_max_connections,
		int second_reader_max_connections) {
	SQLite3_result* result = new SQLite3_result(11);
	char writer_hg_buf[16];
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char first_reader_port_buf[16];
	char second_reader_port_buf[16];
	char first_reader_weight_buf[16];
	char second_reader_weight_buf[16];
	char first_reader_max_connections_buf[16];
	char second_reader_max_connections_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(first_reader_port_buf, sizeof(first_reader_port_buf),
		"%d", first_reader_port);
	snprintf(second_reader_port_buf, sizeof(second_reader_port_buf),
		"%d", second_reader_port);
	snprintf(first_reader_weight_buf, sizeof(first_reader_weight_buf),
		"%d", first_reader_weight);
	snprintf(second_reader_weight_buf, sizeof(second_reader_weight_buf),
		"%d", second_reader_weight);
	snprintf(first_reader_max_connections_buf,
		sizeof(first_reader_max_connections_buf),
		"%d", first_reader_max_connections);
	snprintf(second_reader_max_connections_buf,
		sizeof(second_reader_max_connections_buf),
		"%d", second_reader_max_connections);

	char* writer_row[] = {
		writer_hg_buf,
		(char*)writer_addr,
		writer_port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb two-reader unit writer"
	};
	result->add_row(writer_row);

	char* first_reader_row[] = {
		reader_hg_buf,
		(char*)first_reader_addr,
		first_reader_port_buf,
		(char*)"ONLINE",
		first_reader_weight_buf,
		(char*)"0",
		first_reader_max_connections_buf,
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb two-reader unit reader1"
	};
	result->add_row(first_reader_row);

	char* second_reader_row[] = {
		reader_hg_buf,
		(char*)second_reader_addr,
		second_reader_port_buf,
		(char*)"ONLINE",
		second_reader_weight_buf,
		(char*)"0",
		second_reader_max_connections_buf,
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb two-reader unit reader2"
	};
	result->add_row(second_reader_row);
	return result;
}

PgSQL_SrvC* find_pgsql_server(
		PgSQL_HGC* hostgroup, const char* address, int port) {
	if (!hostgroup || !hostgroup->mysrvs || !address) {
		return nullptr;
	}
	for (unsigned int index = 0; index < hostgroup->mysrvs->cnt(); ++index) {
		PgSQL_SrvC* server = hostgroup->mysrvs->idx(index);
		if (server && server->address &&
				strcmp(server->address, address) == 0 &&
				server->port == port) {
			return server;
		}
	}
	return nullptr;
}

bool find_prometheus_counter_value(const char* name, double* value) {
	if (!GloVars.prometheus_registry || !name || !value) {
		return false;
	}
	auto families = GloVars.prometheus_registry->Collect();
	for (const auto& family : families) {
		if (family.name != name) {
			continue;
		}
		if (family.metric.empty()) {
			return false;
		}
		*value = family.metric[0].counter.value;
		return true;
	}
	return false;
}

bool find_prometheus_gauge_value(const char* name, double* value) {
	if (!GloVars.prometheus_registry || !name || !value) {
		return false;
	}
	auto families = GloVars.prometheus_registry->Collect();
	for (const auto& family : families) {
		if (family.name != name) {
			continue;
		}
		if (family.metric.empty()) {
			return false;
		}
		*value = family.metric[0].gauge.value;
		return true;
	}
	return false;
}

void set_test_userinfo(PgSQL_Connection* conn) {
	conn->userinfo->set(
		(char*)"polardb_unit_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
}

void set_test_pgsql_defaults(PgSQL_Connection* conn) {
	for (int index = 0; index < PGSQL_NAME_LAST_LOW_WM; ++index) {
		const char* value = pgsql_tracked_variables[index].default_value;
		conn->var_hash[index] =
			SpookyHash::Hash32(value, strlen(value), 10);
		if (conn->variables[index].value) {
			free(conn->variables[index].value);
		}
		conn->variables[index].value = strdup(value);
	}
}

PgSQL_Connection* make_cached_reader_connection(PgSQL_SrvC* reader) {
	PgSQL_Connection* conn = new PgSQL_Connection(false);
	set_test_userinfo(conn);
	set_test_pgsql_defaults(conn);
	conn->parent = reader;
	conn->async_state_machine = ASYNC_IDLE;
	conn->reusable = true;
	const PolarDB_StartupProfile startup_profile = make_v15_profile();
	const int identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
	PolarDB_StartupClientContext startup_client;
	startup_client.identity = PolarDB_StartupIdentity{
		"127.0.0.10",
		6033,
		PolarDB_StartupIdentitySource::LISTENER_PROXY};
	conn->set_polardb_startup_settings(
		startup_profile, identity_mode,
		pgsql_thread___polardb_startup_config_generation,
		startup_client);
	return conn;
}

void stage_polardb_topology(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_addr, int reader_port) {
	ok(hgm->servers_add(make_pgsql_servers_result(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port)) == 0,
		"%s: writer and reader staged for commit", label);
	hgm->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(hgm->commit({}, {}, false, false),
		"%s: topology commit succeeds", label);
}

void stage_polardb_topology_with_txn_split(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_addr, int reader_port) {
	ok(hgm->servers_add(make_pgsql_servers_result(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port)) == 0,
		"%s: writer and reader staged for commit", label);
	hgm->save_incoming_pgsql_table(
		make_polardb_replication_row_with_protocol(
			writer_hg, reader_hg, "v15", "1"),
		"pgsql_replication_hostgroups");
	ok(hgm->commit({}, {}, false, false),
		"%s: topology commit succeeds", label);
}

void stage_polardb_topology_two_readers(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg,
		const char* first_reader_addr, int first_reader_port,
		const char* second_reader_addr, int second_reader_port,
		int first_reader_weight, int second_reader_weight) {
	ok(hgm->servers_add(make_pgsql_servers_result_two_readers(
			writer_hg, writer_addr, writer_port,
			reader_hg, first_reader_addr, first_reader_port,
			second_reader_addr, second_reader_port,
			first_reader_weight, second_reader_weight)) == 0,
		"%s: writer and two readers staged for commit", label);
	hgm->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(hgm->commit({}, {}, false, false),
		"%s: two-reader topology commit succeeds", label);
}

void PolarDB_ReaderRetentionUnitAccess::set_worker_index(
		PgSQL_Thread* worker, unsigned int worker_index) {
	worker->polardb_worker_index = worker_index;
}

void PolarDB_ReaderRetentionUnitAccess::set_local_connection_count(
		PgSQL_Thread* worker, unsigned int count) {
	worker->polardb_reader_local_connection_count = count;
}

unsigned int PolarDB_ReaderRetentionUnitAccess::local_connection_count(
		const PgSQL_Thread* worker) {
	return worker->polardb_reader_local_connection_count;
}

std::mutex& PolarDB_WorkerLifecycleUnitAccess::mutex(
		PgSQL_Threads_Handler* handler) {
	return handler->polardb_worker_lifecycle_mutex_;
}

std::mutex& PolarDB_WorkerLifecycleUnitAccess::wake_mutex(
		PgSQL_Thread* worker) {
	return worker->polardb_reader_pool_reservation_wake_mutex;
}

void PolarDB_WorkerLifecycleUnitAccess::set_shutdown(
		PgSQL_Threads_Handler* handler, bool shutdown_started) {
	std::lock_guard<std::mutex> lifecycle_lock(
		handler->polardb_worker_lifecycle_mutex_);
	handler->shutdown_ = shutdown_started;
}

std::string PolarDB_WarmupUnitAccess::queued_key(
		const PgSQL_PolarDB_ReaderPool& pool) {
	return pool.split_warmup_queued_.empty()
		? std::string() : *pool.split_warmup_queued_.begin();
}

bool PolarDB_WarmupUnitAccess::drain(
		PgSQL_PolarDB_ReaderPool& pool,
		std::vector<PgSQL_SplitWarmupRequest>& requests) {
	return pool.drain_split_warmup_requests(requests);
}

bool PolarDB_WarmupUnitAccess::register_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key) {
	return pool.register_split_warmup_inflight_key(key);
}

bool PolarDB_WarmupUnitAccess::rerun_pending(
		const PgSQL_PolarDB_ReaderPool& pool, const std::string& key) {
	return pool.split_warmup_rerun_pending_.find(key) !=
		pool.split_warmup_rerun_pending_.end();
}

void PolarDB_WarmupUnitAccess::finish_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key,
		const PgSQL_SplitWarmupRequest& request) {
	pool.release_and_maybe_requeue_split_warmup_key(key, &request);
}

bool PolarDB_WarmupUnitAccess::apply_startup_parameters(
		PgSQL_Connection* conn,
		const PgSQL_SplitWarmupRequest& request) {
	return PgSQL_PolarDB_ReaderPool::
		apply_split_warmup_startup_parameters(conn, request);
}

void pgsql_polardb_unit_corrupt_match_key_positions(
		PgSQL_SrvConnList* connections) {
	if (connections && !connections->match_keys_by_position.empty()) {
		connections->match_keys_by_position.pop_back();
	}
}

void pgsql_polardb_unit_swap_match_key_positions(
		PgSQL_SrvConnList* connections, unsigned int first,
		unsigned int second) {
	if (connections &&
			first < connections->match_keys_by_position.size() &&
			second < connections->match_keys_by_position.size()) {
		std::swap(connections->match_keys_by_position[first],
			connections->match_keys_by_position[second]);
	}
}

size_t pgsql_polardb_unit_empty_match_bucket_count(
		PgSQL_SrvConnList* connections) {
	return connections ? connections->empty_match_bucket_count : 0;
}

PolarDB_ThreadCounterSnapshot::PolarDB_ThreadCounterSnapshot(
		const PgSQL_Thread* worker, unsigned int counter_index)
	: worker_(worker),
	  counter_index_(counter_index),
	  initial_value_(worker
		? worker->polardb_status_variables.stvar[counter_index] : 0) {}

unsigned long long PolarDB_ThreadCounterSnapshot::delta() const {
	return worker_
		? worker_->polardb_status_variables.stvar[counter_index_] -
			initial_value_
		: 0;
}

PolarDB_OneReaderTestTopology stage_polardb_one_reader_test_topology(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_addr, int reader_port) {
	stage_polardb_topology(
		hgm, label,
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	PolarDB_OneReaderTestTopology topology;
	topology.writer_hostgroup = hgm->MyHGC_lookup(writer_hg);
	topology.reader_hostgroup = hgm->MyHGC_lookup(reader_hg);
	topology.writer = find_pgsql_server(
		topology.writer_hostgroup, writer_addr, writer_port);
	topology.reader = find_pgsql_server(
		topology.reader_hostgroup, reader_addr, reader_port);
	return topology;
}

PolarDB_TwoReaderTestTopology stage_polardb_two_reader_test_topology(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg,
		const char* first_reader_addr, int first_reader_port,
		const char* second_reader_addr, int second_reader_port,
		int first_reader_weight, int second_reader_weight) {
	stage_polardb_topology_two_readers(
		hgm, label,
		writer_hg, writer_addr, writer_port,
		reader_hg,
		first_reader_addr, first_reader_port,
		second_reader_addr, second_reader_port,
		first_reader_weight, second_reader_weight);
	PolarDB_TwoReaderTestTopology topology;
	topology.writer_hostgroup = hgm->MyHGC_lookup(writer_hg);
	topology.reader_hostgroup = hgm->MyHGC_lookup(reader_hg);
	topology.writer = find_pgsql_server(
		topology.writer_hostgroup, writer_addr, writer_port);
	topology.first_reader = find_pgsql_server(
		topology.reader_hostgroup, first_reader_addr, first_reader_port);
	topology.second_reader = find_pgsql_server(
		topology.reader_hostgroup, second_reader_addr, second_reader_port);
	return topology;
}

static SQLite3_result* make_pgsql_servers_result_many_readers(
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_prefix, int reader_count,
		int reader_base_port, int reader_weight) {
	SQLite3_result* result = new SQLite3_result(11);
	std::string writer_hg_text = std::to_string(writer_hg);
	std::string writer_port_text = std::to_string(writer_port);
	char* writer_row[] = {
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
		(char*)"polardb many-reader test writer"
	};
	result->add_row(writer_row);

	const std::string reader_hg_text = std::to_string(reader_hg);
	const std::string reader_weight_text = std::to_string(reader_weight);
	for (int index = 0; index < reader_count; ++index) {
		std::string address =
			std::string(reader_prefix) + std::to_string(index + 1);
		std::string port = std::to_string(reader_base_port + index);
		char* reader_row[] = {
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
			(char*)"polardb many-reader test reader"
		};
		result->add_row(reader_row);
	}
	return result;
}

void stage_polardb_topology_many_readers(
		PgSQL_HostGroups_Manager* hgm, const char* label,
		int writer_hg, const char* writer_addr, int writer_port,
		int reader_hg, const char* reader_prefix, int reader_count,
		int reader_base_port, int reader_weight) {
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

void attach_test_frontend(
		PgSQL_Session& session, PgSQL_Thread* worker) {
	if (worker) {
		session.thread = worker;
	}
	session.connections_handler = true;
	session.client_myds = new PgSQL_Data_Stream();
	session.client_myds->init(MYDS_FRONTEND, &session, 0);
	session.client_myds->myconn = new PgSQL_Connection(true);
	set_test_userinfo(session.client_myds->myconn);
	set_test_pgsql_defaults(session.client_myds->myconn);
	session.client_myds->addr.addr = strdup("127.0.0.1");
	session.client_myds->addr.port = 5432;
	session.client_myds->proxy_addr.addr = strdup("127.0.0.10");
	session.client_myds->proxy_addr.port = 6033;
}

PolarDB_StartupIdentity unit_proxy_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.10",
		6033,
		PolarDB_StartupIdentitySource::LISTENER_PROXY};
}

PolarDB_StartupIdentity unit_other_proxy_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.11",
		6033,
		PolarDB_StartupIdentitySource::LISTENER_PROXY};
}

PGconn* unit_connected_pgconn() {
	PGconn* conn = PQconnectStart(
		"host=127.0.0.1 port=1 connect_timeout=1");
	if (conn) {
		conn->status = CONNECTION_OK;
	}
	return conn;
}

PtrSize_t unit_simple_query_packet(const char* query) {
	const size_t query_len = strlen(query);
	const unsigned int packet_size =
		static_cast<unsigned int>(query_len + 6);
	unsigned char* packet =
		static_cast<unsigned char*>(l_alloc(packet_size));
	packet[0] = 'Q';
	const uint32_t wire_len = static_cast<uint32_t>(query_len + 5);
	for (int shift = 24, offset = 1; shift >= 0; shift -= 8, ++offset) {
		packet[offset] = static_cast<unsigned char>(
			(wire_len >> shift) & 0xff);
	}
	memcpy(packet + 5, query, query_len + 1);
	return PtrSize_t{packet_size, packet, 0, nullptr};
}

unsigned int unit_reader_pool_shared_free_count(PgSQL_SrvC* reader) {
	return reader ? reader->pool_free_count_value() : 0;
}

static uint64_t unit_reader_pool_auth_key(PgSQL_Connection* conn) {
	if (!conn || !conn->userinfo || !conn->userinfo->username ||
			!conn->userinfo->dbname) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	hash = polardb_pool_hash_cstr(hash, conn->userinfo->username);
	hash = polardb_pool_hash_cstr(hash, conn->userinfo->dbname);
	return hash;
}

uint64_t unit_reader_pool_options_key(PgSQL_Connection* conn) {
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

void unit_reader_pool_refresh_key(PgSQL_Connection* conn) {
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

void unit_reader_pool_prepare_free_conn(
		PgSQL_SrvC* reader, PgSQL_Connection* conn) {
	conn->parent = reader;
	conn->pgsql_conn = PQconnectStart("polardb_unit_invalid_conninfo=1");
	unit_reader_pool_refresh_key(conn);
}

PgSQL_PoolMatchKey unit_reader_pool_match_key(PgSQL_Connection* conn) {
	PgSQL_PoolMatchKey core_key;
	if (!conn) {
		return core_key;
	}
	unit_reader_pool_refresh_key(conn);
	return pgsql_pool_match_key(
		conn->polardb_startup_profile_generation,
		conn->polardb_pool_key);
}

bool unit_register_reader_pool_capacity_request(
		PgSQL_SrvC* reader, unsigned int worker_index, uint64_t token,
		const PgSQL_PoolMatchKey& key) {
	return reader && reader->myhgc &&
		reader->myhgc->register_reader_pool_capacity_request(
			worker_index, token, 1, key,
			PolarDB_Query_ReaderPlan{}, PolarDB_WaitSpec{});
}

void unit_reader_pool_add_matching(
		PgSQL_SrvC* reader, PgSQL_Connection* conn) {
	const PgSQL_PoolMatchKey core_key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, core_key));
}

void unit_reader_pool_add_shared(
		PgSQL_SrvC* reader, PgSQL_Connection* conn) {
	unit_reader_pool_prepare_free_conn(reader, conn);
	unit_reader_pool_add_matching(reader, conn);
}

bool unit_reader_pool_clear_shared(
		PgSQL_SrvC* reader, PgSQL_Connection* conn) {
	if (!reader->remove_free_connection(conn)) {
		return false;
	}
	conn->pgsql_conn = nullptr;
	return true;
}
