/**
 * @file polardb_reader_selection_tests.cpp
 * @brief Reader routing and selection tests for the full PolarDB harness.
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
#include "polardb_unit_domains.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

static void test_worker_local_reader_selection_sequence() {
	std::atomic<uint64_t> selection_start{0};
	std::unique_ptr<PgSQL_Thread> first_worker(new PgSQL_Thread());
	std::unique_ptr<PgSQL_Thread> second_worker(new PgSQL_Thread());

	const uint64_t first = first_worker->polardb_next_reader_selection_sequence(
		910, 1, &selection_start);
	const uint64_t second = first_worker->polardb_next_reader_selection_sequence(
		910, 1, &selection_start);
	ok(first == 0 && second == 1 &&
			selection_start.load(std::memory_order_relaxed) == 1,
		"PolarDB local selection sequence: one worker advances without another shared write");

	const uint64_t other_worker =
		second_worker->polardb_next_reader_selection_sequence(
			910, 1, &selection_start);
	ok(other_worker == 1 &&
			selection_start.load(std::memory_order_relaxed) == 2,
		"PolarDB local selection sequence: another worker receives a different start");

	const uint64_t other_hostgroup =
		first_worker->polardb_next_reader_selection_sequence(
			911, 1, &selection_start);
	const uint64_t original_hostgroup =
		first_worker->polardb_next_reader_selection_sequence(
			910, 1, &selection_start);
	ok(other_hostgroup == 2 && original_hostgroup == 2,
		"PolarDB local selection sequence: each hostgroup advances independently");

	const uint64_t next_generation =
		first_worker->polardb_next_reader_selection_sequence(
			910, 2, &selection_start);
	ok(next_generation == 3,
		"PolarDB local selection sequence: topology refresh starts new worker state");
}

static void test_two_reader_degraded_uses_healthy_peer() {
	const int writer_hg = 912;
	const int reader_hg = 913;
	const PolarDB_TwoReaderTestTopology topology =
		stage_polardb_two_reader_test_topology(PgHGM,
		"PolarDB v2 degraded two-reader selection",
		writer_hg, "polardb-v2-degraded-writer", 22382,
		reader_hg,
		"polardb-v2-degraded-reader-a", 22383,
		"polardb-v2-degraded-reader-b", 22384);
	ok(topology.valid(),
		"PolarDB v2 degraded selection: two-reader snapshot is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* unavailable = topology.first_reader;
	PgSQL_SrvC* healthy = topology.second_reader;
	unavailable->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	PgSQL_Connection* conn = make_cached_reader_connection(healthy);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(healthy, conn);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult pooled = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled.acquired() && pooled.srv == healthy,
		"PolarDB v2 degraded selection: pooled-only read skips an unavailable first choice");
	if (pooled.conn) {
		PgHGM->push_MyConn_to_pool(pooled.conn);
	}
	unavailable->set_status(MYSQL_SERVER_STATUS_ONLINE);
	PgSQL_Connection* recovered_conn =
		make_cached_reader_connection(unavailable);
	recovered_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(unavailable, recovered_conn);
	PolarDB_ReaderResult ordinary = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait, /*only_pooled=*/false);
	ok(ordinary.acquired() && ordinary.srv == healthy,
		"PolarDB v2 degraded selection: selection sequence advances while its first reader is unavailable");
	if (ordinary.conn) {
		PgHGM->push_MyConn_to_pool(ordinary.conn);
	}
	unavailable->remove_free_connection(recovered_conn);
	healthy->remove_free_connection(conn);
	delete recovered_conn;
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
	const bool saved_lazy_warmup = pgsql_thread___polardb_lazy_warmup_split;
	const bool saved_runtime_lazy_warmup =
		GloPTH ? GloPTH->variables.polardb_lazy_warmup_split : false;
	auto set_lazy_warmup = [](bool value) {
		pgsql_thread___polardb_lazy_warmup_split = value;
		if (GloPTH) {
			GloPTH->variables.polardb_lazy_warmup_split = value;
		}
	};
	set_lazy_warmup(false);
	PgHGM->warm_split_pools();
	set_lazy_warmup(true);

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
	attach_test_frontend(sess, worker.get());
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
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
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
		(void)worker->polardb_next_reader_selection_sequence(
			reader_hg, snapshot->generation,
				snapshot_hg->second.selection_start.get());
	}
#if POLARDB_PROFILE
	const unsigned long long selected_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt];
	const unsigned long long selected_miss_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss];
	const unsigned long long additional_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt];
	const unsigned long long additional_hit_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit];
#endif // POLARDB_PROFILE
	const unsigned long long warmup_requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(
			std::memory_order_relaxed);
	PolarDB_ReaderResult pooled_fallback =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled_fallback.acquired() && pooled_fallback.srv == reader_a &&
			pooled_fallback.selected_pool_miss_server == reader_b &&
			reader_b->pool_free_count_value() == 0,
		"PolarDB v2 selection: pooled-only fallback records the selected reader that needs warmup");
	ok(PgHGM->status.polardb_split_warmup_requested.load(
			std::memory_order_relaxed) == warmup_requested_before + 1,
		"PolarDB v2 selection: a locked selected-reader miss requests targeted warmup after another reader serves the query");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt] ==
			selected_attempt_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss] ==
			selected_miss_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt] ==
			additional_attempt_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit] ==
			additional_hit_before + 1,
		"PolarDB v2 selection: counters separate the selected-reader miss from the successful additional lookup");
#endif // POLARDB_PROFILE
	if (pooled_fallback.conn) {
		PgHGM->push_MyConn_to_pool(pooled_fallback.conn);
	}

	for (PgSQL_Connection* conn : fixtures) {
		PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
		srv->remove_free_connection(conn);
		delete conn;
	}
	set_lazy_warmup(false);
	PgHGM->warm_split_pools();
	pgsql_thread___polardb_lazy_warmup_split = saved_lazy_warmup;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split =
			saved_runtime_lazy_warmup;
	}
}

static void test_two_reader_busy_selected_uses_peer() {
	const int writer_hg = 906;
	const int reader_hg = 907;
	stage_polardb_topology_two_readers(PgHGM,
		"PolarDB busy selected reader",
		writer_hg, "polardb-busy-selected-writer", 22352,
		reader_hg,
		"polardb-busy-selected-reader-a", 22353,
		"polardb-busy-selected-reader-b", 22354);

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
		"PolarDB busy selected reader: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}

	PgSQL_SrvC* selected = entry->servers[0].srv;
	PgSQL_SrvC* peer = entry->servers[1].srv;
	PgSQL_Connection* selected_conn =
		make_cached_reader_connection(selected);
	PgSQL_Connection* peer_conn = make_cached_reader_connection(peer);
	selected_conn->pgsql_conn = unit_connected_pgconn();
	peer_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(selected, selected_conn);
	unit_reader_pool_add_matching(peer, peer_conn);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	entry->selection_start->store(0, std::memory_order_relaxed);

	std::atomic<bool> selected_locked{false};
	std::atomic<bool> release_selected{false};
	std::thread selected_holder([&]() {
		std::lock_guard<std::recursive_mutex> lock(selected->pool_mutex);
		selected_locked.store(true, std::memory_order_release);
		while (!release_selected.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
	});
	while (!selected_locked.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	const unsigned long long alternate_hits_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit];
#if POLARDB_PROFILE
	const unsigned long long selected_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt];
	const unsigned long long selected_busy_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_busy];
	const unsigned long long additional_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt];
	const unsigned long long additional_hit_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit];
#endif // POLARDB_PROFILE
	PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait, /*only_pooled=*/false);
	release_selected.store(true, std::memory_order_release);
	selected_holder.join();

	ok(result.acquired() && result.conn == peer_conn && result.srv == peer &&
			selected->pool_free_count_value() == 1 &&
			peer->pool_used_count_value() == 1,
		"PolarDB busy selected reader: ordinary read takes the equal-weight peer without waiting for the selected pool");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit] ==
			alternate_hits_before + 1,
		"PolarDB busy selected reader: release counters record the alternate acquisition");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt] ==
			selected_attempt_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_busy] ==
			selected_busy_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt] ==
			additional_attempt_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit] ==
			additional_hit_before + 1,
		"PolarDB busy selected reader: counters record the busy first lookup and successful additional lookup");
#endif // POLARDB_PROFILE

	if (result.conn) {
		PgHGM->push_MyConn_to_pool(result.conn);
	}

	auto acquire_after_brief_pool_lock = [&](PgSQL_SrvC* locked_server,
			const PolarDB_Query_ReaderPlan& request_plan,
			const PolarDB_WaitSpec& wait, bool only_pooled) {
		std::atomic<bool> locked{false};
		std::thread holder([&]() {
			std::lock_guard<std::recursive_mutex> lock(
				locked_server->pool_mutex);
			locked.store(true, std::memory_order_release);
			usleep(20000);
		});
		while (!locked.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
		PolarDB_ReaderResult acquired = PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, request_plan, wait, only_pooled);
		holder.join();
		return acquired;
	};

	entry->selection_start->store(0, std::memory_order_relaxed);
	const unsigned long long pooled_alternate_hits_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit];
	PolarDB_ReaderResult pooled_result =
		acquire_after_brief_pool_lock(
			peer, plan, no_wait, PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled_result.acquired() && pooled_result.conn == peer_conn &&
			pooled_result.srv == peer,
		"PolarDB busy selected reader: pooled-only acquisition waits for the reader chosen by the sequence");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit] ==
			pooled_alternate_hits_before,
		"PolarDB busy selected reader: pooled-only acquisition does not switch readers because the selected pool mutex is busy");
	if (pooled_result.conn) {
		PgHGM->push_MyConn_to_pool(pooled_result.conn);
	}

	const uint64_t target_lsn = 0xE900;
	const uint64_t now_us = monotonic_time();
	selected->polardb_current_lsn.store(
		target_lsn, std::memory_order_relaxed);
	peer->polardb_current_lsn.store(
		target_lsn - 0x100, std::memory_order_relaxed);
	selected->lsn_updated_at.store(now_us, std::memory_order_relaxed);
	peer->lsn_updated_at.store(now_us, std::memory_order_relaxed);
	plan.group_lsn = target_lsn;
	plan.max_lag_bytes = 0x1000;
	const PolarDB_WaitSpec target_wait = PolarDB_WaitSpec::from_lsn(
		target_lsn, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	entry->selection_start->store(0, std::memory_order_relaxed);
	const unsigned long long target_alternate_hits_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit];
	PolarDB_ReaderResult target_result =
		acquire_after_brief_pool_lock(
			selected, plan, target_wait, /*only_pooled=*/false);
	ok(target_result.acquired() && target_result.conn == selected_conn &&
			target_result.srv == selected &&
			target_result.wait_bypass_allowed,
		"PolarDB busy selected reader: target-bearing acquisition keeps the target-ready reader");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_busy_alternate_hit] ==
			target_alternate_hits_before,
		"PolarDB busy selected reader: target-bearing acquisition does not switch to a behind-target peer");
	if (target_result.conn) {
		PgHGM->push_MyConn_to_pool(target_result.conn);
	}

	selected->remove_free_connection(selected_conn);
	peer->remove_free_connection(peer_conn);
	delete selected_conn;
	delete peer_conn;
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
	attach_test_frontend(sess, worker.get());
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	unsigned int high_hits = 0;
	unsigned int low_hits = 0;
	bool acquired_all = true;
	for (unsigned int i = 0; i < 101; i++) {
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
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
	ok(entry->selection_start->load(std::memory_order_relaxed) == 1,
		"PolarDB weighted selection: unequal weights seed one worker-local sequence");

	high_weight->remove_free_connection(high_conn);
	low_weight->remove_free_connection(low_conn);
	delete high_conn;
	delete low_conn;
}

static void test_unusable_third_keeps_multi_reader_policy() {
	const int writer_hg = 1904;
	const int reader_hg = 1905;
	SQLite3_result* servers = make_pgsql_servers_result_two_readers(
		writer_hg, "polardb-parity-writer", 25340,
		reader_hg,
		"polardb-parity-reader-a", 25341,
		"polardb-parity-reader-b", 25342,
		/*reader_weight1=*/100, /*reader_weight2=*/1,
		/*reader_max_connections1=*/1,
		/*reader_max_connections2=*/1);
	char reader_hg_buf[16];
	char reader_port_buf[16];
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", 25343);
	char* unusable_row[] = {
		reader_hg_buf,
		(char*)"polardb-parity-reader-c",
		reader_port_buf,
		(char*)"ONLINE",
		(char*)"0",
		(char*)"0",
		(char*)"1",
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb parity unusable reader"
	};
	servers->add_row(unusable_row);
	ok(PgHGM->servers_add(servers) == 0,
		"PolarDB reader parity: two usable readers and one unusable reader are staged");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB reader parity: three-reader topology commits");

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* entry = nullptr;
	if (snapshot) {
		auto found = snapshot->by_hostgroup.find(reader_hg);
		if (found != snapshot->by_hostgroup.end()) {
			entry = &found->second;
		}
	}
	const bool fixture_ok = entry && entry->servers.size() == 3 &&
		entry->selection_start;
	ok(fixture_ok,
		"PolarDB reader parity: three-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}

	PgSQL_SrvC* high_weight = nullptr;
	PgSQL_SrvC* low_weight = nullptr;
	PgSQL_SrvC* unusable = nullptr;
	for (const auto& server : entry->servers) {
		if (server.weight == 100) {
			high_weight = server.srv;
		} else if (server.weight == 1) {
			low_weight = server.srv;
		} else if (server.weight == 0) {
			unusable = server.srv;
		}
	}
	ok(high_weight && low_weight && unusable,
		"PolarDB reader parity: usable and unusable readers are identified");
	if (!high_weight || !low_weight || !unusable) {
		return;
	}

	PgSQL_Connection* high_conn =
		make_cached_reader_connection(high_weight);
	PgSQL_Connection* low_conn =
		make_cached_reader_connection(low_weight);
	high_conn->pgsql_conn = unit_connected_pgconn();
	low_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(high_weight, high_conn);
	unit_reader_pool_add_matching(low_weight, low_conn);
	const PgSQL_PoolMatchKey high_key =
		unit_reader_pool_match_key(high_conn);
	const PgSQL_PoolMatchKey low_key =
		unit_reader_pool_match_key(low_conn);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;

	entry->selection_start->store(0, std::memory_order_relaxed);
	unsigned int high_hits = 0;
	unsigned int low_hits = 0;
	bool acquired_all = true;
	for (unsigned int n = 0; n < 101; n++) {
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
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
	ok(acquired_all && high_hits + low_hits == 101,
		"PolarDB reader parity: general selection uses only the two usable readers");
	ok(entry->selection_start->load(std::memory_order_relaxed) == 0,
		"PolarDB reader parity: a three-reader snapshot retains the general P2C policy");

	const uint64_t TARGET_LSN = 0xE500;
	const uint64_t now = monotonic_time();
	high_weight->polardb_current_lsn.store(
		TARGET_LSN - 0x100, std::memory_order_relaxed);
	low_weight->polardb_current_lsn.store(
		TARGET_LSN, std::memory_order_relaxed);
	high_weight->lsn_updated_at.store(now, std::memory_order_relaxed);
	low_weight->lsn_updated_at.store(now, std::memory_order_relaxed);
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	entry->selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult ready = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(ready.acquired() && ready.srv == low_weight &&
			ready.wait_bypass_allowed,
		"PolarDB reader parity: target-ready peer wins with an unusable third reader");
	if (ready.conn) {
		PgHGM->push_MyConn_to_pool(ready.conn);
	}

	plan.group_lsn = 0;
	plan.max_lag_bytes = -1;
	entry->selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult excluded = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED,
		high_weight->address, high_weight->port);
	ok(excluded.acquired() && excluded.srv == low_weight,
		"PolarDB reader parity: exclusion selects the usable peer");
	if (excluded.conn) {
		PgHGM->push_MyConn_to_pool(excluded.conn);
	}

	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x100;
	high_weight->polardb_current_lsn.store(
		TARGET_LSN - 0x100, std::memory_order_relaxed);
	low_weight->polardb_current_lsn.store(
		TARGET_LSN + 0x80, std::memory_order_relaxed);
	entry->selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult lag_allowed = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(lag_allowed.acquired() && lag_allowed.srv == low_weight,
		"PolarDB reader parity: lag rejection selects the usable peer");
	if (lag_allowed.conn) {
		PgHGM->push_MyConn_to_pool(lag_allowed.conn);
	}

	plan.group_lsn = 0;
	plan.max_lag_bytes = -1;
	ok(high_weight->remove_free_connection(high_conn),
		"PolarDB reader parity: selected reader fixture is removed for pooled fallback");
	entry->selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult pooled_fallback =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(pooled_fallback.acquired() &&
			pooled_fallback.srv == low_weight,
		"PolarDB reader parity: pooled-only miss uses the usable peer");
	if (pooled_fallback.conn) {
		PgHGM->push_MyConn_to_pool(pooled_fallback.conn);
	}
	unit_reader_pool_add_matching(high_weight, high_conn);

	PgSQL_Connection* high_used =
		high_weight->take_matching_connection(high_key);
	PgSQL_Connection* low_used =
		low_weight->take_matching_connection(low_key);
	entry->selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult full = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait, /*only_pooled=*/false,
		nullptr, -1, /*confirm_reader_group_capacity=*/true);
	ok(high_used == high_conn && low_used == low_conn &&
			!full.acquired() &&
			full.status == PolarDB_ReaderStatus::READER_GROUP_BUSY,
		"PolarDB reader parity: unusable third reader does not hide full usable group");

	ok(high_weight->return_matching_connection(
				high_used, high_key).stored() &&
			low_weight->return_matching_connection(
				low_used, low_key).stored(),
		"PolarDB reader parity: bounded reader fixtures return to FREE");
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
	attach_test_frontend(sess, worker.get());
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
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
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

	const uint64_t TARGET_LSN = 0xC100;
	const uint64_t now = monotonic_time();
	PgSQL_SrvC* target_ready = entry->servers[2].srv;
	for (const auto& server_entry : entry->servers) {
		server_entry.srv->polardb_current_lsn.store(
			TARGET_LSN, std::memory_order_relaxed);
		server_entry.srv->lsn_updated_at.store(now, std::memory_order_relaxed);
	}
	PolarDB_WaitSpec target_wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	const unsigned long long ready_considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	const unsigned long long ready_p2c_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_select];
	bool acquired_ready_sample = true;
	for (unsigned int n = 0; n < 32; n++) {
		PolarDB_ReaderResult result =
			PgHGM->polardb_acquire_reader_connection(
				reader_hg, &sess, plan, target_wait,
				/*only_pooled=*/false);
		if (!result.acquired() || !result.wait_bypass_allowed) {
			acquired_ready_sample = false;
			break;
		}
		PgHGM->push_MyConn_to_pool(result.conn);
	}
	ok(acquired_ready_sample,
		"PolarDB multi-reader selection: target-ready sampled pairs serve every read without a wait");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered] ==
			ready_considered_before + 64 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_p2c_select] ==
			ready_p2c_before + 32,
		"PolarDB multi-reader selection: a target-ready request examines and compares only two readers");

	constexpr useconds_t TARGET_READY_POOL_LOCK_HOLD_US = 2000;
	constexpr unsigned int TARGET_READY_ALTERNATE_ATTEMPTS = 64;
	PgSQL_SrvC* contended_reader = entry->servers[0].srv;
	bool alternate_used = false;
	bool contention_acquisitions_succeeded = true;
	for (unsigned int n = 0;
			n < TARGET_READY_ALTERNATE_ATTEMPTS && !alternate_used; n++) {
		std::atomic<bool> pool_locked{false};
		std::thread holder([&]() {
			std::lock_guard<std::recursive_mutex> lock(
				contended_reader->pool_mutex);
			pool_locked.store(true, std::memory_order_release);
			usleep(TARGET_READY_POOL_LOCK_HOLD_US);
		});
		while (!pool_locked.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
		const unsigned long long alternate_hits_before =
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_busy_alternate_hit];
		PolarDB_ReaderResult result =
			PgHGM->polardb_acquire_reader_connection(
				reader_hg, &sess, plan, target_wait,
				/*only_pooled=*/false);
		holder.join();
		if (!result.acquired() || !result.wait_bypass_allowed) {
			contention_acquisitions_succeeded = false;
			break;
		}
		PgHGM->push_MyConn_to_pool(result.conn);
		alternate_used =
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_busy_alternate_hit] >
			alternate_hits_before;
	}
	ok(contention_acquisitions_succeeded,
		"PolarDB multi-reader selection: target-ready acquisition survives a sampled reader's busy pool");
	ok(alternate_used,
		"PolarDB multi-reader selection: a target-ready sampled peer avoids the busy selected pool");

	for (const auto& server_entry : entry->servers) {
		server_entry.srv->polardb_current_lsn.store(
			TARGET_LSN - 0x100, std::memory_order_relaxed);
		server_entry.srv->lsn_updated_at.store(now, std::memory_order_relaxed);
	}
	const unsigned long long expanded_considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	PolarDB_ReaderResult expanded_behind =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, target_wait,
			/*only_pooled=*/false);
	ok(expanded_behind.acquired() && !expanded_behind.wait_bypass_allowed,
		"PolarDB multi-reader selection: an all-behind sampled pair expands to a wait-capable reader");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered] ==
			expanded_considered_before + entry->servers.size(),
		"PolarDB multi-reader selection: full expansion evaluates each sampled reader only once");
	if (expanded_behind.conn) {
		PgHGM->push_MyConn_to_pool(expanded_behind.conn);
	}

	// Reject every sampled reader. The first sampled evaluation therefore
	// fails, but the second must still run; full expansion must then reuse both
	// rejected outcomes and evaluate only the remaining reader. Exactly three
	// considered servers proves there was neither short-circuiting nor a
	// duplicate evaluation.
	for (const auto& server_entry : entry->servers) {
		server_entry.srv->set_current_latency_us_value(
			std::numeric_limits<unsigned int>::max());
	}
	const unsigned long long rejected_considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	const unsigned long long rejected_unusable_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_skip_unusable];
	PolarDB_ReaderResult rejected_sample =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, target_wait,
			/*only_pooled=*/false);
	if (rejected_sample.conn) {
		PgHGM->push_MyConn_to_pool(rejected_sample.conn);
	}
	ok(!rejected_sample.acquired() &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_server_considered] ==
				rejected_considered_before + entry->servers.size() &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_server_skip_unusable] ==
				rejected_unusable_before + entry->servers.size(),
		"PolarDB multi-reader selection: rejected sampled readers are each evaluated and reused exactly once");
	for (const auto& server_entry : entry->servers) {
		server_entry.srv->set_current_latency_us_value(0);
	}

	for (const auto& server_entry : entry->servers) {
		server_entry.srv->polardb_current_lsn.store(
			TARGET_LSN - 0x100, std::memory_order_relaxed);
		server_entry.srv->lsn_updated_at.store(now, std::memory_order_relaxed);
	}
	target_ready->polardb_current_lsn.store(
		TARGET_LSN, std::memory_order_relaxed);
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
#if POLARDB_PROFILE
	const unsigned long long used_reads_before_ready =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_used_count_read];
#endif // POLARDB_PROFILE
	worker->polardb_begin_worker_pass();
	PolarDB_ReaderResult target_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, target_wait, /*only_pooled=*/false);
	worker->polardb_finish_worker_pass();
	ok(target_result.acquired() && target_result.srv == target_ready &&
			target_result.wait_bypass_allowed,
		"PolarDB multi-reader selection: target-ready reader wins across "
		"more than two readers");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables
				.stvar[polardb_st_var_reader_pool_used_count_read] ==
			used_reads_before_ready,
		"PolarDB multi-reader selection: a single ready reader needs no "
		"shared load read");
#endif // POLARDB_PROFILE
	if (target_result.conn) {
		PgHGM->push_MyConn_to_pool(target_result.conn);
	}

	PgSQL_SrvC* second_ready = entry->servers[1].srv;
	PgSQL_SrvC* behind_reader = entry->servers[0].srv;
	PgSQL_Connection* ready_conn = target_result.conn;
	PgSQL_Connection* second_ready_conn = fixtures[1];
	second_ready->polardb_current_lsn.store(
		TARGET_LSN, std::memory_order_relaxed);
	if (ready_conn) {
		target_ready->remove_free_connection(ready_conn);
	}
	second_ready->remove_free_connection(second_ready_conn);
#if POLARDB_PROFILE
	const unsigned long long selected_miss_before_later_search =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss];
	const unsigned long long additional_attempt_before_later_search =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt];
	const unsigned long long additional_hit_before_later_search =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit];
	const unsigned long long additional_miss_before_later_search =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_miss];
	const unsigned long long wait_before_later_search =
		worker->polardb_status_variables.stvar[
			polardb_st_var_target_lsn_fallback_wait];
#endif // POLARDB_PROFILE
	PolarDB_ReaderResult later_search = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, target_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(later_search.acquired() && later_search.srv == behind_reader &&
			(later_search.selected_pool_miss_server == target_ready ||
			 later_search.selected_pool_miss_server == second_ready) &&
			!later_search.wait_bypass_allowed,
		"PolarDB multi-reader selection: ready-pool misses record one selected reader before using a behind-target reader");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss] ==
			selected_miss_before_later_search + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt] ==
			additional_attempt_before_later_search + 2 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_hit] ==
			additional_hit_before_later_search + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_miss] ==
			additional_miss_before_later_search + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_target_lsn_fallback_wait] ==
			wait_before_later_search + 1,
		"PolarDB multi-reader selection: counters continue across ready misses, the later pool hit, and the required backend wait");
#endif // POLARDB_PROFILE
	if (later_search.conn) {
		PgHGM->push_MyConn_to_pool(later_search.conn);
	}
	if (ready_conn) {
		unit_reader_pool_add_matching(target_ready, ready_conn);
	}
	unit_reader_pool_add_matching(second_ready, second_ready_conn);

	target_ready->polardb_current_lsn.store(
		TARGET_LSN - 0x100, std::memory_order_relaxed);
	second_ready->polardb_current_lsn.store(
		TARGET_LSN - 0x100, std::memory_order_relaxed);
#if POLARDB_PROFILE
	const unsigned long long used_reads_before_behind =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_used_count_read];
#endif // POLARDB_PROFILE
	worker->polardb_begin_worker_pass();
	PolarDB_ReaderResult behind_result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, target_wait, /*only_pooled=*/false);
	worker->polardb_finish_worker_pass();
	ok(behind_result.acquired() && !behind_result.wait_bypass_allowed,
		"PolarDB multi-reader selection: all-behind readers still use P2C fallback");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_used_count_read] ==
			used_reads_before_behind + 2,
		"PolarDB multi-reader selection: P2C reads load for only its two candidates");
#endif // POLARDB_PROFILE
	if (behind_result.conn) {
		PgHGM->push_MyConn_to_pool(behind_result.conn);
	}

	for (PgSQL_Connection* conn : fixtures) {
		PgSQL_SrvC* reader = static_cast<PgSQL_SrvC*>(conn->parent);
		reader->remove_free_connection(conn);
		delete conn;
	}

#if POLARDB_PROFILE
	const unsigned long long selected_attempt_before_empty =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt];
	const unsigned long long selected_miss_before_empty =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss];
	const unsigned long long additional_attempt_before_empty =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt];
	const unsigned long long additional_miss_before_empty =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_miss];
#endif // POLARDB_PROFILE
	PolarDB_ReaderResult empty_result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, target_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!empty_result.acquired(),
		"PolarDB multi-reader selection: an empty three-reader pool reports no connection");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_attempt] ==
			selected_attempt_before_empty + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_selected_miss] ==
			selected_miss_before_empty + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_attempt] ==
			additional_attempt_before_empty + 2 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_additional_miss] ==
			additional_miss_before_empty + 2,
		"PolarDB multi-reader selection: counters identify one selected-reader miss and two additional misses");
#endif // POLARDB_PROFILE
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
	attach_test_frontend(sess, worker.get());

	PolarDB_Query_ReaderPlan plan;
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const int saved_throttle = pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	PolarDB_ReaderResult first_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait, false);
	ok(!first_result.acquired() && first_result.srv == reader_a,
		"PolarDB best-behind reader: a miss stays on the selected reader");
	ok(reader_b->pool_free_count_value() == 1,
		"PolarDB best-behind reader: a miss does not reroute through another server's pool");
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait, false);
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

static void test_two_reader_freshest_behind_reader_is_preferred() {
	const int writer_hg = 964;
	const int reader_hg = 965;
	const uint64_t LOWER_LSN = 0xD800;
	const uint64_t HIGHER_LSN = 0xD900;
	const uint64_t TARGET_LSN = 0xDA00;

	stage_polardb_topology_two_readers(PgHGM,
		"PolarDB exact best-behind reader",
		writer_hg, "polardb-exact-writer", 22432,
		reader_hg,
		"polardb-exact-reader-a", 22433,
		"polardb-exact-reader-b", 22434);

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	if (!snapshot) {
		ok(false,
			"PolarDB exact best-behind reader: two-reader snapshot is available");
		return;
	}
	auto hostgroup = snapshot->by_hostgroup.find(reader_hg);
	const bool fixture_ok = hostgroup != snapshot->by_hostgroup.end() &&
		hostgroup->second.servers.size() == 2 &&
		hostgroup->second.selection_start;
	ok(fixture_ok,
		"PolarDB exact best-behind reader: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}

	PgSQL_SrvC *first = hostgroup->second.servers[0].srv;
	PgSQL_SrvC *second = hostgroup->second.servers[1].srv;
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	const uint64_t now = monotonic_time();
	first->polardb_current_lsn.store(LOWER_LSN, std::memory_order_relaxed);
	second->polardb_current_lsn.store(HIGHER_LSN, std::memory_order_relaxed);
	first->lsn_updated_at.store(now, std::memory_order_relaxed);
	second->lsn_updated_at.store(now, std::memory_order_relaxed);

	PgSQL_Connection *cached = make_cached_reader_connection(second);
	cached->polardb_startup_client.identity = unit_proxy_identity();
	cached->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(second, cached);

	PolarDB_Query_ReaderPlan plan;
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const bool saved_preference =
		pgsql_thread___polardb_reader_prefer_freshest_below_target;
	const int saved_range = pgsql_thread___polardb_reader_lsn_lag_range_bytes;
	const int saved_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = false;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = 0;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	std::unique_ptr<PgSQL_Thread> balanced_worker(new PgSQL_Thread());
	PgSQL_Session balanced_sess;
	attach_test_frontend(balanced_sess, balanced_worker.get());
	PolarDB_ReaderResult balanced_result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &balanced_sess, plan, wait, /*only_pooled=*/false);
	ok(!balanced_result.acquired() && balanced_result.srv == first,
		"PolarDB exact best-behind reader: disabled policy preserves balanced first choice");
	ok(second->pool_free_count_value() == 1,
		"PolarDB exact best-behind reader: disabled policy does not consume the fresher peer");

	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	second->polardb_current_lsn.store(TARGET_LSN, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> reached_worker(new PgSQL_Thread());
	PgSQL_Session reached_sess;
	attach_test_frontend(reached_sess, reached_worker.get());
	PolarDB_ReaderResult reached_result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &reached_sess, plan, wait, /*only_pooled=*/false);
	ok(reached_result.acquired() && reached_result.srv == second &&
			reached_result.wait_bypass_allowed,
		"PolarDB exact best-behind reader: disabled policy still prefers a target-reached peer");
	if (reached_result.conn && reached_result.srv &&
			reached_result.srv->ConnectionsUsed) {
		reached_result.srv->ConnectionsUsed->remove(reached_result.conn);
	}
	delete reached_result.conn;

	first->polardb_current_lsn.store(TARGET_LSN, std::memory_order_relaxed);
	first->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	second->polardb_current_lsn.store(
		TARGET_LSN + 0x100, std::memory_order_relaxed);
	second->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	cached = make_cached_reader_connection(first);
	cached->polardb_startup_client.identity = unit_proxy_identity();
	cached->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(first, cached);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> both_ready_worker(new PgSQL_Thread());
	PgSQL_Session both_ready_sess;
	attach_test_frontend(both_ready_sess, both_ready_worker.get());
	const PolarDB_ThreadCounterSnapshot considered_ready(
		both_ready_worker.get(),
		polardb_st_var_reader_pool_server_considered);
	PolarDB_ReaderResult both_ready_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &both_ready_sess, plan, wait,
			/*only_pooled=*/false);
	ok(both_ready_result.acquired() && both_ready_result.srv == first &&
			both_ready_result.wait_bypass_allowed,
		"PolarDB exact best-behind reader: two target-ready peers preserve weighted selection");
	ok(considered_ready.delta() == 1 &&
			both_ready_result.selected_reader_lsn == TARGET_LSN &&
			both_ready_result.best_considered_reader_lsn == TARGET_LSN,
		"PolarDB exact best-behind reader: a target-ready selected reader does not sample its peer");
	if (both_ready_result.conn && both_ready_result.srv) {
		(void)both_ready_result.srv->remove_used_connection(
			both_ready_result.conn);
	}
	delete both_ready_result.conn;

	first->polardb_current_lsn.store(LOWER_LSN, std::memory_order_relaxed);
	first->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	second->polardb_current_lsn.store(HIGHER_LSN, std::memory_order_relaxed);
	cached = make_cached_reader_connection(second);
	cached->polardb_startup_client.identity = unit_proxy_identity();
	cached->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(second, cached);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	pgsql_thread___polardb_reader_prefer_freshest_below_target = true;
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, wait, /*only_pooled=*/false);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = saved_range;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = saved_preference;

	ok(result.acquired() && result.srv == second,
		"PolarDB exact best-behind reader: higher fresh LSN wins before pool acquisition");
	ok(result.acquired() && !result.wait_bypass_allowed &&
			result.selected_reader_lsn == HIGHER_LSN &&
			result.best_considered_reader_lsn == HIGHER_LSN,
		"PolarDB exact best-behind reader: selected observation is the best considered LSN");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_behind_best] == 0 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_loss_bytes] == 0,
		"PolarDB exact best-behind reader: selection records no avoidable LSN loss");

	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	delete result.conn;
}

static void test_two_reader_fresher_less_loaded_policy() {
	const int writer_hg = 966;
	const int reader_hg = 967;
	const uint64_t LOWER_LSN = 0xDB00;
	const uint64_t HIGHER_LSN = 0xDC00;
	const uint64_t TARGET_LSN = 0xDD00;

	stage_polardb_topology_two_readers(PgHGM,
		"PolarDB fresher less-loaded reader",
		writer_hg, "polardb-dominance-writer", 22532,
		reader_hg,
		"polardb-dominance-reader-a", 22533,
		"polardb-dominance-reader-b", 22534);

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	if (!snapshot) {
		ok(false,
			"PolarDB fresher less-loaded reader: two-reader snapshot is available");
		return;
	}
	auto hostgroup = snapshot->by_hostgroup.find(reader_hg);
	const bool fixture_ok = hostgroup != snapshot->by_hostgroup.end() &&
		hostgroup->second.servers.size() == 2 &&
		hostgroup->second.selection_start;
	ok(fixture_ok,
		"PolarDB fresher less-loaded reader: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}

	PgSQL_SrvC* first = hostgroup->second.servers[0].srv;
	PgSQL_SrvC* second = hostgroup->second.servers[1].srv;
	const uint64_t now = monotonic_time();
	first->polardb_current_lsn.store(LOWER_LSN, std::memory_order_relaxed);
	second->polardb_current_lsn.store(HIGHER_LSN, std::memory_order_relaxed);
	first->lsn_updated_at.store(now, std::memory_order_relaxed);
	second->lsn_updated_at.store(now, std::memory_order_relaxed);

	PolarDB_Query_ReaderPlan plan;
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const bool saved_exact =
		pgsql_thread___polardb_reader_prefer_freshest_below_target;
	const bool saved_dominance =
		pgsql_thread___polardb_reader_prefer_less_loaded;
	const int saved_range = pgsql_thread___polardb_reader_lsn_lag_range_bytes;
	const int saved_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = false;
	pgsql_thread___polardb_reader_prefer_less_loaded = true;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = 0;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	auto add_free = [&](PgSQL_SrvC* reader) {
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		conn->polardb_startup_client.identity = unit_proxy_identity();
		conn->pgsql_conn = unit_connected_pgconn();
		unit_reader_pool_add_matching(reader, conn);
		return conn;
	};
	auto add_used = [&](PgSQL_SrvC* reader) {
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
		return reader->add_used_matching_connection(conn, key) ? conn : nullptr;
	};
	auto remove_used = [&](PgSQL_SrvC* reader, PgSQL_Connection* conn) {
		if (conn) {
			(void)reader->remove_used_connection(conn);
			delete conn;
		}
	};
	auto remove_free = [&](PgSQL_SrvC* reader, PgSQL_Connection* conn) {
		if (conn) {
			(void)reader->remove_free_connection(conn);
			delete conn;
		}
	};

	PgSQL_Connection* first_used = add_used(first);
	PgSQL_Connection* second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	PolarDB_ReaderResult less_loaded = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, wait, /*only_pooled=*/false);
	ok(first_used && less_loaded.acquired() && less_loaded.srv == second,
		"PolarDB fresher less-loaded reader: strict dominance selects a fresher peer with lower normalized load");
	if (less_loaded.conn && less_loaded.srv) {
		(void)less_loaded.srv->remove_used_connection(less_loaded.conn);
	}
	delete less_loaded.conn;
	remove_used(first, first_used);

	second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> equal_worker(new PgSQL_Thread());
	PgSQL_Session equal_sess;
	attach_test_frontend(equal_sess, equal_worker.get());
	PolarDB_ReaderResult equal_loaded = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &equal_sess, plan, wait, /*only_pooled=*/false);
	ok(!equal_loaded.acquired() && equal_loaded.srv == first &&
			second->pool_free_count_value() == 1,
		"PolarDB fresher less-loaded reader: equal normalized load preserves weighted selection");
	remove_free(second, second_free);

	PgSQL_Connection* second_used = add_used(second);
	second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> more_worker(new PgSQL_Thread());
	PgSQL_Session more_sess;
	attach_test_frontend(more_sess, more_worker.get());
	PolarDB_ReaderResult more_loaded = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &more_sess, plan, wait, /*only_pooled=*/false);
	ok(second_used && !more_loaded.acquired() && more_loaded.srv == first &&
			second->pool_free_count_value() == 1,
		"PolarDB fresher less-loaded reader: higher normalized load preserves weighted selection");
	remove_free(second, second_free);
	remove_used(second, second_used);

	second->polardb_current_lsn.store(
		TARGET_LSN, std::memory_order_relaxed);
	second->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	second_used = add_used(second);
	second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> ready_worker(new PgSQL_Thread());
	PgSQL_Session ready_sess;
	attach_test_frontend(ready_sess, ready_worker.get());
	PolarDB_ReaderResult target_ready = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &ready_sess, plan, wait, /*only_pooled=*/false);
	ok(second_used && target_ready.acquired() &&
			target_ready.srv == second &&
			target_ready.wait_bypass_allowed,
		"PolarDB fresher less-loaded reader: a target-ready peer wins even when it has higher normalized load");
	if (target_ready.conn && target_ready.srv) {
		(void)target_ready.srv->remove_used_connection(target_ready.conn);
	}
	delete target_ready.conn;
	remove_used(second, second_used);

	second->polardb_current_lsn.store(HIGHER_LSN, std::memory_order_relaxed);
	second->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	second_used = add_used(second);
	second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	pgsql_thread___polardb_reader_prefer_freshest_below_target = true;
	std::unique_ptr<PgSQL_Thread> exact_worker(new PgSQL_Thread());
	PgSQL_Session exact_sess;
	attach_test_frontend(exact_sess, exact_worker.get());
	PolarDB_ReaderResult exact_precedence = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &exact_sess, plan, wait, /*only_pooled=*/false);
	ok(second_used && exact_precedence.acquired() &&
			exact_precedence.srv == second,
		"PolarDB fresher less-loaded reader: exact-freshest policy takes precedence when both switches are enabled");
	if (exact_precedence.conn && exact_precedence.srv) {
		(void)exact_precedence.srv->remove_used_connection(
			exact_precedence.conn);
	}
	delete exact_precedence.conn;
	remove_used(second, second_used);

	second->polardb_current_lsn.store(LOWER_LSN, std::memory_order_relaxed);
	second->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	second_free = add_free(second);
	hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
	pgsql_thread___polardb_reader_prefer_freshest_below_target = false;
	std::unique_ptr<PgSQL_Thread> equal_lsn_worker(new PgSQL_Thread());
	PgSQL_Session equal_lsn_sess;
	attach_test_frontend(equal_lsn_sess, equal_lsn_worker.get());
	PolarDB_ReaderResult equal_lsn = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &equal_lsn_sess, plan, wait, /*only_pooled=*/false);
	ok(!equal_lsn.acquired() && equal_lsn.srv == first &&
			second->pool_free_count_value() == 1,
		"PolarDB fresher less-loaded reader: equal reader LSN preserves weighted selection");
	remove_free(second, second_free);

#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_both_behind_compared] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_less_loaded] == 1 &&
		equal_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_equal_loaded] == 1 &&
		more_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_more_loaded] == 1 &&
		exact_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_more_loaded] == 1 &&
		equal_lsn_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_both_behind_compared] == 1 &&
		equal_lsn_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_both_behind_equal_lsn] == 1 &&
		equal_lsn_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_less_loaded] == 0 &&
		equal_lsn_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_equal_loaded] == 0 &&
		equal_lsn_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_more_loaded] == 0,
		"PolarDB fresher less-loaded reader: profile counters classify normalized load relations");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_dominance_switch] == 1 &&
		exact_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_fresher_exact_switch] == 1,
		"PolarDB fresher less-loaded reader: profile counters classify policy switches");
#endif // POLARDB_PROFILE

	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = saved_range;
	pgsql_thread___polardb_reader_prefer_less_loaded =
		saved_dominance;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = saved_exact;
}

static void test_two_reader_less_loaded_weight_scale_invariance() {
	const uint64_t LOWER_LSN = 0xDE00;
	const uint64_t HIGHER_LSN = 0xDF00;
	const uint64_t TARGET_LSN = 0xE000;
	const int saved_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	const bool saved_exact =
		pgsql_thread___polardb_reader_prefer_freshest_below_target;
	const bool saved_dominance =
		pgsql_thread___polardb_reader_prefer_less_loaded;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = false;
	pgsql_thread___polardb_reader_prefer_less_loaded = true;

	struct WeightCase {
		int writer_hg;
		int reader_hg;
		int first_weight;
		int second_weight;
		const char* label;
	};
	const WeightCase cases[] = {
		{1966, 1967, 2, 1, "unequal weights"},
		{1968, 1969, 200, 100, "equivalent scaled weights"},
	};
	bool selected_fresher[2] = {false, false};

	for (size_t case_idx = 0; case_idx < 2; ++case_idx) {
		const WeightCase& weight_case = cases[case_idx];
		const int port_base = 22600 + static_cast<int>(case_idx) * 10;
		stage_polardb_topology_two_readers(PgHGM,
			"PolarDB less-loaded weight scaling",
			weight_case.writer_hg, "polardb-weight-writer", port_base,
			weight_case.reader_hg,
			"polardb-weight-reader-a", port_base + 1,
			"polardb-weight-reader-b", port_base + 2,
			weight_case.first_weight, weight_case.second_weight);

		auto snapshot = PgHGM->get_polardb_server_list_snapshot();
		if (!snapshot) {
			ok(false,
				"PolarDB less-loaded weight scaling: %s topology is available",
				weight_case.label);
			continue;
		}
		auto hostgroup = snapshot->by_hostgroup.find(weight_case.reader_hg);
		const bool fixture_ok =
			hostgroup != snapshot->by_hostgroup.end() &&
			hostgroup->second.servers.size() == 2 &&
			hostgroup->second.selection_start;
		ok(fixture_ok,
			"PolarDB less-loaded weight scaling: %s topology is available",
			weight_case.label);
		if (!fixture_ok) {
			continue;
		}

		PgSQL_SrvC* first = hostgroup->second.servers[0].srv;
		PgSQL_SrvC* second = hostgroup->second.servers[1].srv;
		const uint64_t now = monotonic_time();
		first->polardb_current_lsn.store(LOWER_LSN, std::memory_order_relaxed);
		second->polardb_current_lsn.store(HIGHER_LSN, std::memory_order_relaxed);
		first->lsn_updated_at.store(now, std::memory_order_relaxed);
		second->lsn_updated_at.store(now, std::memory_order_relaxed);

		std::vector<PgSQL_Connection*> first_used;
		for (int i = 0; i < 4; ++i) {
			PgSQL_Connection* conn = make_cached_reader_connection(first);
			const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
			if (first->add_used_matching_connection(conn, key)) {
				first_used.push_back(conn);
			} else {
				delete conn;
			}
		}
		PgSQL_Connection* second_used = make_cached_reader_connection(second);
		const PgSQL_PoolMatchKey second_used_key =
			unit_reader_pool_match_key(second_used);
		if (!second->add_used_matching_connection(
				second_used, second_used_key)) {
			delete second_used;
			second_used = nullptr;
		}
		PgSQL_Connection* second_free = make_cached_reader_connection(second);
		second_free->polardb_startup_client.identity = unit_proxy_identity();
		second_free->pgsql_conn = unit_connected_pgconn();
		unit_reader_pool_add_matching(second, second_free);

		PolarDB_Query_ReaderPlan plan;
		plan.group_lsn = TARGET_LSN + 0x100;
		plan.max_lag_bytes = 0x1000;
		plan.fallback_writer_hg = weight_case.writer_hg;
		PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
			TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
			PolarDB_WaitMode::BEST_EFFORT);
		std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
		PgSQL_Session sess;
		attach_test_frontend(sess, worker.get());
		hostgroup->second.selection_start->store(0, std::memory_order_relaxed);
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
			weight_case.reader_hg, &sess, plan, wait,
			/*only_pooled=*/false);
		selected_fresher[case_idx] = result.acquired() && result.srv == second;
		ok(first_used.size() == 4 && second_used &&
				selected_fresher[case_idx],
			"PolarDB less-loaded weight scaling: %s preserves normalized-load dominance",
			weight_case.label);

		if (result.conn && result.srv) {
			(void)result.srv->remove_used_connection(result.conn);
			delete result.conn;
		} else {
			(void)second->remove_free_connection(second_free);
			delete second_free;
		}
		for (PgSQL_Connection* conn : first_used) {
			(void)first->remove_used_connection(conn);
			delete conn;
		}
		if (second_used) {
			(void)second->remove_used_connection(second_used);
			delete second_used;
		}
	}

	ok(selected_fresher[0] && selected_fresher[1],
		"PolarDB less-loaded weight scaling: multiplying all weights by the same factor preserves the decision");
	pgsql_thread___polardb_reader_prefer_less_loaded = saved_dominance;
	pgsql_thread___polardb_reader_prefer_freshest_below_target = saved_exact;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
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
	attach_test_frontend(sess, worker.get());

	PolarDB_Query_ReaderPlan plan;
	plan.group_lsn = TARGET_LSN + 0x100;
	plan.max_lag_bytes = 0x1000;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	const int saved_range = pgsql_thread___polardb_reader_lsn_lag_range_bytes;
	const int saved_throttle = pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	const uint64_t compared_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_compared];
	const uint64_t behind_best_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_behind_best];
	const uint64_t loss_bytes_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_loss_bytes];
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = 0x20;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 0;
	PolarDB_ReaderResult first_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait, false);
	ok(!first_result.acquired() && first_result.srv == reader_a,
		"PolarDB best-behind range: a miss stays on the selected reader");
	ok(reader_b->pool_free_count_value() == 1,
		"PolarDB best-behind range: a miss does not reroute through another server's pool");
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait, false);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup = saved_throttle;
	pgsql_thread___polardb_reader_lsn_lag_range_bytes = saved_range;
	ok(result.acquired() && result.srv == reader_b,
		"PolarDB best-behind range: byte range can acquire a lower-LSN candidate");
	ok(!result.wait_bypass_allowed,
		"PolarDB best-behind range: lower-LSN acquisition still requires wait wrapper");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_compared] ==
			compared_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_behind_best] ==
			behind_best_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_loss_bytes] ==
			loss_bytes_before + BEST_LSN - LOWER_LSN_IN_RANGE,
		"PolarDB best-behind range: acquisition records avoidable selected LSN loss");
	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	delete result.conn;
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
	attach_test_frontend(sess, worker.get());
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	bool selected_target_every_time = true;
	for (unsigned int attempt = 0; attempt < 16; attempt++) {
		PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
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

void run_polardb_reader_load_policy_tests() {
	test_two_reader_fresher_less_loaded_policy();
	test_two_reader_less_loaded_weight_scale_invariance();
}

void run_polardb_reader_basic_selection_tests() {
	test_worker_local_reader_selection_sequence();
	test_two_reader_degraded_uses_healthy_peer();
	test_two_reader_selection_ignores_inventory_and_alternates();
	test_two_reader_busy_selected_uses_peer();
}

void run_polardb_reader_multi_selection_tests() {
	test_two_reader_unequal_weights_keep_global_sequence();
	test_unusable_third_keeps_multi_reader_policy();
	test_multi_reader_selection_samples_two_servers();
}

void run_polardb_reader_lag_predicate_tests() {
	test_reader_lsn_lag_range_predicate();
}

void run_polardb_reader_target_policy_tests() {
	test_tied_freshest_behind_reader_can_acquire_second_candidate();
	test_two_reader_freshest_behind_reader_is_preferred();
	test_lag_range_best_behind_reader_can_acquire_lower_lsn_candidate();
}

void run_polardb_reader_scale_tests() {
	test_reader_selection_uses_all_configured_servers();
}

#endif // POLARDB_PROXY
