/**
 * @file polardb_reader_pool_tests.cpp
 * @brief ReaderPool storage, matching, reuse, and return tests.
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
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

static void test_reader_pool_request_limits() {
	ok(PGSQL_POLARDB_TXN_READER_ONLY_POOLED,
		"PolarDB reader request policy: transaction reader paths use pooled-only acquisition");
	ok(pgsql_split_warmup_batch_has_capacity(0),
		"PolarDB warmup batch limit: empty batch accepts work");
	ok(pgsql_split_warmup_batch_has_capacity(
			PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT - 1),
		"PolarDB warmup batch limit: last slot accepts work");
	ok(!pgsql_split_warmup_batch_has_capacity(
			PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT),
		"PolarDB warmup batch limit: full batch defers work");

	PolarDB_StartupProfile startup_profile;
	PolarDB_PoolKey key;
	key.auth_hash = 1;
	PolarDB_PoolRequest pooled_request(
		key, startup_profile, 1, PolarDB_PoolProfile::BASE, true);
	ok(pooled_request.only_pooled,
		"PolarDB reader request policy: pooled-only requests cannot create");
	PolarDB_PoolRequest ordinary_request(
		key, startup_profile, 1, PolarDB_PoolProfile::BASE, false);
	PolarDB_PoolRequest invalid_request(
		PolarDB_PoolKey{}, startup_profile, -1,
		PolarDB_PoolProfile::BASE, false);
	ok(!ordinary_request.only_pooled,
		"PolarDB reader request policy: ordinary requests can create");
	ok(ordinary_request.ready_for_matching() &&
			!invalid_request.ready_for_matching(),
		"PolarDB reader request policy: only complete requests can be matched");
	static_assert(!std::is_default_constructible<
		PolarDB_PoolRequest>::value,
		"pool requests must include their matching fields");
	ok(sizeof(PolarDB_PoolRequest) <= 40,
		"PolarDB reader request policy: hot pool request stays within 40 bytes");
}

static void test_core_match_pool_index_and_transfer() {
	const int writer_hg = 900;
	const int reader_hg = 901;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PostgreSQL core match pool",
		writer_hg, "polardb-core-exact-writer", 22322,
		reader_hg, "polardb-core-exact-reader", 22323);
	ok(topology.valid(),
		"PostgreSQL core match pool: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;

	PgSQL_PoolMatchKey key_a;
	key_a.words[0] = 1;
	key_a.words[1] = 11;
	PgSQL_PoolMatchKey key_b;
	key_b.words[0] = 2;
	key_b.words[1] = 22;
	ok(reader->return_matching_connection(nullptr, key_a).status ==
				ServerReturnStatus::INVALID_CONNECTION &&
			reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: null return is rejected without changing pool counts");
	PgSQL_Connection* untracked = make_cached_reader_connection(reader);
	ok(reader->return_matching_connection(untracked, key_a).status ==
				ServerReturnStatus::NOT_IN_USED_LIST &&
			reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: untracked return is rejected without changing pool counts");
	delete untracked;
	PgSQL_Connection* conn_a = make_cached_reader_connection(reader);
	PgSQL_Connection* conn_b = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(conn_a, key_a) &&
			reader->add_matching_connection(conn_b, key_b),
		"PostgreSQL core match pool: two opaque match keys are added to one FREE owner");
	ok(reader->pool_free_count_value() == 2 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: adding connections updates the shared counts");
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		pgsql_polardb_unit_corrupt_match_key_positions(
			reader->ConnectionsFree);
	}
	ok(reader->matching_connection_count(key_a) == 1 &&
			reader->matching_connection_count(key_b) == 1,
		"PostgreSQL core match pool: a damaged key mirror rebuilds from authoritative exact entries");
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		pgsql_polardb_unit_swap_match_key_positions(
			reader->ConnectionsFree, 0, 1);
	}

	PgSQL_Connection* taken = reader->take_matching_connection(key_a);
	ok(taken == conn_a && reader->matching_connection_count(key_b) == 1,
		"PostgreSQL core match pool: equal-length key corruption repairs before exact acquisition");
	ok(reader->pool_free_count_value() == 1 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core match pool: taking a connection moves FREE to USED together");
	const uint32_t used_position = taken->polardb_core_pool_position;
	ok(!reader->remove_free_connection(taken) &&
			taken->polardb_core_pool_position == used_position,
		"PostgreSQL core match pool: a FREE-list miss preserves the valid USED-list position hint");
	ok(reader->take_matching_connection(key_a) == nullptr,
		"PostgreSQL core match pool: an empty key never returns a random connection");
	ok(pgsql_polardb_unit_empty_match_bucket_count(
			reader->ConnectionsFree) == 1,
		"PostgreSQL core match pool: taking the last exact connection retains its empty bucket");
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		pgsql_polardb_unit_corrupt_match_key_positions(
			reader->ConnectionsUsed);
	}
	ok(reader->return_matching_connection(taken, key_a).stored() &&
			reader->pool_free_count_value() == 2 &&
			reader->pool_used_count_value() == 0 &&
			pgsql_polardb_unit_empty_match_bucket_count(
				reader->ConnectionsFree) == 0,
		"PostgreSQL core match pool: returning repairs the USED mirror and restores counts");

	const int64_t original_max_connections = reader->max_connections;
	reader->max_connections = 1;
	PgSQL_PoolMatchKey bounded_key_a;
	bounded_key_a.words[0] = 0x401;
	PgSQL_PoolMatchKey bounded_key_b;
	bounded_key_b.words[0] = 0x402;
	PgSQL_Connection* bounded_a = make_cached_reader_connection(reader);
	PgSQL_Connection* bounded_b = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(bounded_a, bounded_key_a) &&
			reader->add_matching_connection(bounded_b, bounded_key_b) &&
			reader->take_matching_connection(bounded_key_a) == bounded_a &&
			reader->take_matching_connection(bounded_key_b) == bounded_b &&
			pgsql_polardb_unit_empty_match_bucket_count(
				reader->ConnectionsFree) == 1,
		"PostgreSQL core match pool: retained empty buckets are bounded by the server connection limit");
	ok(reader->return_matching_connection(bounded_a, bounded_key_a).stored() &&
			reader->return_matching_connection(
				bounded_b, bounded_key_b).stored() &&
			pgsql_polardb_unit_empty_match_bucket_count(
				reader->ConnectionsFree) == 0,
		"PostgreSQL core match pool: bounded exact buckets remain reusable after return");
	reader->max_connections = 0;
	reader->remove_free_connection(bounded_a);
	reader->remove_free_connection(bounded_b);
	delete bounded_a;
	delete bounded_b;
	reader->max_connections = original_max_connections;

	const PgSQL_PoolGetMode all_actions =
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
		PgSQL_PoolGetMode::ALLOW_RESET |
		PgSQL_PoolGetMode::ALLOW_CREATE;
	ok(pgsql_pool_get_mode_has(
			all_actions, PgSQL_PoolGetMode::ALLOW_EXACT_MATCH) &&
			pgsql_pool_get_mode_has(
				all_actions, PgSQL_PoolGetMode::ALLOW_RESET) &&
			pgsql_pool_get_mode_has(
				all_actions, PgSQL_PoolGetMode::ALLOW_CREATE),
		"PostgreSQL core match pool: all allowed actions are explicit and composable");
	PgSQL_PoolGetResult allowed = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, key_a, nullptr,
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH);
	ok(allowed.conn == conn_a &&
			allowed.source == PgSQL_PoolGetSource::EXACT_MATCH,
		"PostgreSQL core match pool: ALLOW_EXACT_MATCH takes only a matching connection");
	reader->return_matching_connection(allowed.conn, key_a);
	PgSQL_PoolGetResult denied = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, key_a, nullptr, PgSQL_PoolGetMode::NONE);
	ok(denied.conn == nullptr && denied.source == PgSQL_PoolGetSource::NONE,
		"PostgreSQL core match pool: NONE performs no pool action");

	PgSQL_Session reset_sess;
	attach_test_frontend(reset_sess);
	PgSQL_PoolMatchKey reset_request_key;
	reset_request_key.words[0] = 0x303;
	conn_a->polardb_startup_config_generation++;
	PgSQL_Connection* held_compatible =
		reader->take_matching_connection(key_b);
	PgSQL_PoolGetResult stale_only =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, reset_request_key, &reset_sess,
			PgSQL_PoolGetMode::ALLOW_RESET);
	ok(!stale_only.conn && held_compatible == conn_b &&
			reader->pool_free_count_value() == 1 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core match pool: reset lookup rejects the only backend opened with stale startup settings");
	ok(reader->return_matching_connection(held_compatible, key_b).stored(),
		"PostgreSQL core match pool: compatible backend returns after the stale-only lookup");
	PgSQL_PoolGetResult reset_allowed =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, reset_request_key, &reset_sess,
			PgSQL_PoolGetMode::ALLOW_RESET);
	ok(reset_allowed.conn == conn_b &&
			reset_allowed.source == PgSQL_PoolGetSource::RESET &&
			reader->pool_free_count_value() == 1 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core match pool: reset lookup skips stale startup settings and takes the compatible backend");
	// Reset-compatible acquisition deliberately drops the previous exact key.
	// The caller stores the connection under its final state on return.
	ok(reset_allowed.conn &&
			reader->return_matching_connection(
				reset_allowed.conn, key_b).stored(),
		"PostgreSQL core match pool: reset-compatible connection returns under its final exact key");
	conn_a->polardb_startup_config_generation--;

	PgSQL_PoolGetResult exact_before_reset =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, key_a, &reset_sess,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_RESET);
	ok(exact_before_reset.conn == conn_a &&
			exact_before_reset.source == PgSQL_PoolGetSource::EXACT_MATCH,
		"PostgreSQL core match pool: combined reuse prefers the exact key before reset-compatible lookup");
	reader->return_matching_connection(exact_before_reset.conn, key_a);

	std::atomic<bool> pool_locked{false};
	std::atomic<bool> release_pool_lock{false};
	std::thread pool_holder([&]() {
		std::lock_guard<std::recursive_mutex> lock(reader->pool_mutex);
		pool_locked.store(true, std::memory_order_release);
		while (!release_pool_lock.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
	});
	while (!pool_locked.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	PgSQL_PoolGetResult busy = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, key_a, nullptr,
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
			PgSQL_PoolGetMode::SKIP_BUSY_POOL);
	release_pool_lock.store(true, std::memory_order_release);
	pool_holder.join();
	ok(!busy.conn && busy.pool_busy &&
			busy.source == PgSQL_PoolGetSource::NONE,
		"PostgreSQL core match pool: nonblocking lookup reports a busy selected server without changing pool ownership");

	PgSQL_PoolMatchKey missing_key;
	missing_key.words[0] = 3;
	missing_key.words[1] = 33;
	const auto current_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	const uint64_t current_server_generation = current_server_snapshot
		? current_server_snapshot->generation : 0;
	const uint64_t current_startup_generation =
		GloPTH->get_polardb_startup_config_generation();
	PgSQL_PoolGetResult stale_startup =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_CREATE,
			/*selected_max_connections=*/0,
			current_server_generation,
			current_startup_generation + 1);
	ok(!stale_startup.conn && stale_startup.retry_after_config_change &&
			!stale_startup.server_saturated,
		"PostgreSQL core match pool: stale startup state requests a cold-path retry without reporting saturation");
	PgSQL_PoolGetResult stale_server_list =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_CREATE,
			/*selected_max_connections=*/0,
			current_server_generation + 1,
			current_startup_generation);
	ok(!stale_server_list.conn && stale_server_list.retry_after_config_change &&
			!stale_server_list.server_saturated,
		"PostgreSQL core match pool: stale selected-server state requests a cold-path retry without reporting saturation");

	PgSQL_Connection* used_a = reader->take_matching_connection(key_a);
	PgSQL_Connection* used_b = reader->take_matching_connection(key_b);
	ok(used_a == conn_a && used_b == conn_b,
		"PostgreSQL core match pool: saturation fixture moves all free connections to USED");
	const int saved_max_connections = reader->max_connections;
	reader->max_connections = 3;
	PgSQL_PoolGetResult snapshot_full =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/2);
	ok(!snapshot_full.conn && snapshot_full.server_saturated,
		"PostgreSQL core match pool: snapshot capacity reports confirmed saturation under the pool lock");
	const int saved_creation_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		std::numeric_limits<int>::max();
	PgSQL_PoolGetResult create_after_snapshot_full =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_CREATE,
			/*selected_max_connections=*/2,
			current_server_generation,
			current_startup_generation);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		saved_creation_throttle;
	ok(create_after_snapshot_full.conn &&
			create_after_snapshot_full.source ==
				PgSQL_PoolGetSource::CREATED &&
			!create_after_snapshot_full.server_saturated,
		"PostgreSQL core match pool: creation uses the current higher limit after a stale saturation probe");
	if (create_after_snapshot_full.conn) {
		reader->remove_used_connection(create_after_snapshot_full.conn);
		delete create_after_snapshot_full.conn;
	}
	PgSQL_PoolGetResult create_revalidates =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_CREATE,
			/*selected_max_connections=*/2,
			current_server_generation + 1,
			current_startup_generation);
	ok(!create_revalidates.conn &&
			create_revalidates.retry_after_config_change &&
			!create_revalidates.server_saturated,
		"PostgreSQL core match pool: snapshot saturation does not bypass creation-state revalidation");
	reader->max_connections = 1;
	PgSQL_PoolGetResult snapshot_has_room =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/3);
	ok(!snapshot_has_room.conn && !snapshot_has_room.server_saturated,
		"PostgreSQL core match pool: saturation uses the immutable selection limit instead of mutable server state");
	reader->max_connections = saved_max_connections;
	ok(reader->return_matching_connection(used_a, key_a).stored(),
		"PostgreSQL core match pool: one used connection returns after saturation check");
	PgSQL_PoolGetResult free_available =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, missing_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/2);
	ok(!free_available.conn && !free_available.server_saturated,
		"PostgreSQL core match pool: an unreserved free connection preserves the creation path");
	ok(reader->return_matching_connection(used_b, key_b).stored(),
		"PostgreSQL core match pool: second used connection returns after saturation check");

	ok(reader->remove_free_connection(conn_a),
		"PostgreSQL core match pool: removing the first FREE connection compacts the owner list");
	delete conn_a;
	conn_b->last_time_used = 1;
	PgSQL_Connection* ping_conn = reader->take_free_connection_for_ping(
		conn_b, std::numeric_limits<unsigned long long>::max());
	PgSQL_PoolMatchKey compacted_key;
	ok(ping_conn == conn_b &&
			reader->used_connection_match_key(conn_b, &compacted_key) &&
			compacted_key == key_b,
		"PostgreSQL core match pool: fast compaction preserves the moved connection key");
	if (ping_conn) {
		reader->polardb_finish_idle_ping(ping_conn, false);
		reader->return_matching_connection(ping_conn, key_b);
	}
	reader->remove_free_connection(conn_b);
	delete conn_b;

	PgSQL_Connection* offline_conn = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(offline_conn, key_a) &&
			reader->take_matching_connection(key_a) == offline_conn,
		"PostgreSQL core match pool: offline test connection moves to USED");
	reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	ok(reader->return_matching_connection(offline_conn, key_a).status ==
				ServerReturnStatus::SERVER_NOT_ONLINE &&
			reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: offline return removes USED without adding FREE");
	delete offline_conn;
	reader->set_status(MYSQL_SERVER_STATUS_ONLINE);

	PgSQL_Connection* unstorable = make_cached_reader_connection(reader);
	ok(reader->add_used_matching_connection(unstorable, key_a),
		"PostgreSQL core match pool: storage-failure fixture starts in USED");
	PgSQL_SrvConnList* saved_free = reader->ConnectionsFree;
	reader->ConnectionsFree = nullptr;
	const ServerReturnResult storage_failure =
		reader->return_matching_connection(unstorable, key_a);
	reader->ConnectionsFree = saved_free;
	ok(storage_failure.status == ServerReturnStatus::STORE_FAILED &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core match pool: missing FREE owner rejects the return after removing USED");
	delete unstorable;
}


static void test_core_match_pool_concurrent_transfer() {
	const int writer_hg = 904;
	const int reader_hg = 905;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PostgreSQL core match concurrency",
		writer_hg, "polardb-core-lock-writer", 22342,
		reader_hg, "polardb-core-lock-reader", 22343);
	ok(topology.valid(),
		"PostgreSQL core match concurrency: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;

	PgSQL_PoolMatchKey key;
	key.words[0] = 7;
	key.words[1] = 77;
	static constexpr unsigned int connection_count = 16;
	static constexpr unsigned int thread_count = 16;
	static constexpr unsigned int transfers_per_thread = 500;
	std::vector<PgSQL_Connection*> connections;
	for (unsigned int i = 0; i < connection_count; i++) {
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		connections.push_back(conn);
		assert(reader->add_matching_connection(conn, key));
	}

	std::atomic<unsigned int> failures{0};
	std::vector<std::thread> threads;
	for (unsigned int i = 0; i < thread_count; i++) {
		threads.emplace_back([&]() {
			for (unsigned int n = 0; n < transfers_per_thread; n++) {
				PgSQL_Connection* conn = nullptr;
				while ((conn = reader->take_matching_connection(key)) == nullptr) {
					std::this_thread::yield();
				}
				if (!reader->return_matching_connection(conn, key).stored()) {
					failures.fetch_add(1, std::memory_order_relaxed);
					delete conn;
					break;
				}
			}
		});
	}
	for (std::thread& thread : threads) {
		thread.join();
	}

	ok(failures.load(std::memory_order_relaxed) == 0 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == connection_count &&
			reader->matching_connection_count(key) == connection_count,
		"PostgreSQL core match concurrency: per-server lock preserves list, count, and key ownership");
	for (PgSQL_Connection* conn : connections) {
		reader->remove_free_connection(conn);
		delete conn;
	}
}

static void test_core_match_pool_concurrent_boundaries() {
	const int writer_hg = 906;
	const int reader_hg = 907;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PostgreSQL core boundary concurrency",
		writer_hg, "polardb-core-boundary-writer", 22352,
		reader_hg, "polardb-core-boundary-reader", 22353);
	ok(topology.valid(),
		"PostgreSQL core boundary concurrency: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;
	reader->max_connections = 1000;
	const bool old_attributes_configured = reader->myhgc->attributes.configured;
	const int old_free_connections_pct =
		reader->myhgc->attributes.free_connections_pct;
	reader->myhgc->attributes.configured = true;
	reader->myhgc->attributes.free_connections_pct = 100;

	PgSQL_PoolMatchKey key;
	auto add_connections = [&]() {
		for (unsigned int i = 0; i < 8; i++) {
			PgSQL_Connection* conn = make_cached_reader_connection(reader);
			conn->pgsql_conn = unit_connected_pgconn();
			conn->creation_time = monotonic_time();
			conn->last_time_used = 1;
			const PgSQL_PoolMatchKey conn_key =
				unit_reader_pool_match_key(conn);
			if (key.empty()) {
				key = conn_key;
			} else {
				assert(key == conn_key);
			}
			assert(reader->add_matching_connection(conn, conn_key));
		}
	};
	add_connections();

	std::atomic<bool> stop{false};
	std::atomic<unsigned long long> transfers{0};
	std::atomic<unsigned long long> ping_extractions{0};
	std::atomic<unsigned long long> missing_server_snapshot_connections{0};
	std::atomic<unsigned int> sweep_count{0};
	std::vector<std::thread> takers;
	for (unsigned int i = 0; i < 8; i++) {
		takers.emplace_back([&]() {
			while (!stop.load(std::memory_order_acquire)) {
				PgSQL_Connection* conn = reader->take_matching_connection(key);
				if (!conn) {
					std::this_thread::yield();
					continue;
				}
				if (!reader->return_matching_connection(conn, key).stored()) {
					delete conn;
				}
				transfers.fetch_add(1, std::memory_order_relaxed);
			}
		});
	}
	std::thread sweeper([&]() {
		while (!stop.load(std::memory_order_acquire)) {
			SQLite3_result* free_rows = PgHGM->SQL3_Free_Connections();
			delete free_rows;
			(void)PgHGM->Get_Memory_Stats();
			PgSQL_Connection* ping_connections[4] = {};
			const int ping_count = PgHGM->get_multiple_idle_connections(
				reader_hg, UINT64_MAX, ping_connections, 4);
			for (int i = 0; i < ping_count; i++) {
				PgSQL_Connection* conn = ping_connections[i];
				if (!conn->polardb_selected_server_snapshot) {
					missing_server_snapshot_connections.fetch_add(
						1, std::memory_order_relaxed);
				}
				PgHGM->push_MyConn_to_pool(conn);
			}
			ping_extractions.fetch_add(
				ping_count, std::memory_order_relaxed);
			sweep_count.fetch_add(1, std::memory_order_relaxed);
		}
	});

	for (unsigned int i = 0; i < 20; i++) {
		reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
		reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
		add_connections();
		std::this_thread::yield();
	}
	stop.store(true, std::memory_order_release);
	for (std::thread& thread : takers) {
		thread.join();
	}
	sweeper.join();
	reader->myhgc->attributes.configured = old_attributes_configured;
	reader->myhgc->attributes.free_connections_pct = old_free_connections_pct;

	ok(transfers.load(std::memory_order_relaxed) > 0 &&
			sweep_count.load(std::memory_order_relaxed) > 0,
		"PostgreSQL core boundary concurrency: transfers overlap pool sweeps and status changes");
	ok(ping_extractions.load(std::memory_order_relaxed) > 0 &&
			missing_server_snapshot_connections.load(std::memory_order_relaxed) == 0,
		"PostgreSQL core boundary concurrency: extracted ping connections retain their server snapshot");
	ok(reader->pool_used_count_value() == 0 &&
			reader->matching_connection_count(key) ==
				reader->pool_free_count_value(),
		"PostgreSQL core boundary concurrency: list, count, and exact index remain coherent");
	while (reader->pool_free_count_value() > 0) {
		delete reader->ConnectionsFree->remove(0);
	}
}

static void test_keyless_core_use_restores_exact_match() {
	const int writer_hg = 908;
	const int reader_hg = 909;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PostgreSQL core exact return",
		writer_hg, "polardb-core-return-writer", 22362,
		reader_hg, "polardb-core-return-reader", 22363);
	ok(topology.valid(),
		"PostgreSQL core exact return: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, key));
	PgSQL_Connection* core_use = reader->ConnectionsFree->remove(0);
	reader->ConnectionsUsed->add(core_use);
	ok(reader->matching_connection_count(key) == 0 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL core exact return: keyless core use temporarily removes the FREE index entry");
	PgHGM->push_MyConn_to_pool(core_use);
	ok(reader->matching_connection_count(key) == 1 &&
			reader->pool_used_count_value() == 0,
		"PostgreSQL core exact return: one return path rebuilds the exact index from connection state");
	reader->remove_free_connection(conn);
	delete conn;
}


static void test_idle_ping_reader_pool_accounting() {
	const int writer_hg = 996;
	const int reader_hg = 997;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PolarDB idle-ping accounting",
		writer_hg, "polardb-ping-accounting-writer", 22372,
		reader_hg, "polardb-ping-accounting-reader", 22373);
	ok(topology.valid(),
		"PolarDB idle-ping accounting: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;

	const unsigned long long take_before =
		PgHGM->status.polardb_reader_pool_idle_ping_take.load(
			std::memory_order_relaxed);
	const unsigned long long return_before =
		PgHGM->status.polardb_reader_pool_idle_ping_return.load(
			std::memory_order_relaxed);
	const unsigned long long destroy_before =
		PgHGM->status.polardb_reader_pool_idle_ping_destroy.load(
			std::memory_order_relaxed);

	PgSQL_Connection* returned = make_cached_reader_connection(reader);
	returned->last_time_used = 1;
	unit_reader_pool_add_matching(reader, returned);
	ok(reader->take_free_connection_for_ping(returned, 2) == returned &&
			returned->polardb_idle_ping_inflight.load(
				std::memory_order_acquire) &&
			reader->polardb_idle_ping_count.load(
				std::memory_order_relaxed) == 1,
		"PolarDB idle-ping accounting: FREE extraction records in-flight ownership");
	ok(reader->polardb_finish_idle_ping(returned, false) &&
			!returned->polardb_idle_ping_inflight.load(
				std::memory_order_acquire) &&
			reader->polardb_idle_ping_count.load(
				std::memory_order_relaxed) == 0,
		"PolarDB idle-ping accounting: successful completion clears ownership");
	ok(PgHGM->status.polardb_reader_pool_idle_ping_take.load(
			std::memory_order_relaxed) == take_before + 1 &&
		PgHGM->status.polardb_reader_pool_idle_ping_return.load(
			std::memory_order_relaxed) == return_before + 1,
		"PolarDB idle-ping accounting: extraction and return counters advance once");
	reader->remove_used_connection(returned);
	delete returned;

	PgSQL_Connection* destroyed = make_cached_reader_connection(reader);
	destroyed->last_time_used = 1;
	unit_reader_pool_add_matching(reader, destroyed);
	ok(reader->take_free_connection_for_ping(destroyed, 2) == destroyed,
		"PolarDB idle-ping accounting: destruction fixture enters maintenance ownership");
	PgHGM->destroy_MyConn_from_pool(destroyed);
	ok(reader->polardb_idle_ping_count.load(
			std::memory_order_relaxed) == 0 &&
		PgHGM->status.polardb_reader_pool_idle_ping_destroy.load(
			std::memory_order_relaxed) == destroy_before + 1,
		"PolarDB idle-ping accounting: destruction clears ownership and advances its counter");
}

static void test_reader_pool_idle_trim_grace() {
	const int writer_hg = 974;
	const int reader_hg = 975;
	const PolarDB_OneReaderTestTopology topology =
		stage_polardb_one_reader_test_topology(
			PgHGM, "PolarDB idle-trim grace",
		writer_hg, "polardb-trim-writer", 22374,
		reader_hg, "polardb-trim-reader", 22375);
	ok(topology.valid(),
		"PolarDB idle-trim grace: topology is available");
	if (!topology.valid()) {
		return;
	}
	PgSQL_SrvC* reader = topology.reader;

	PgSQL_PoolMatchKey key;
	for (unsigned int index = 0; index < 5; index++) {
		PgSQL_Connection* connection = make_cached_reader_connection(reader);
		const PgSQL_PoolMatchKey connection_key =
			unit_reader_pool_match_key(connection);
		if (key.empty()) {
			key = connection_key;
		}
		assert(connection_key == key);
		assert(reader->add_matching_connection(connection, key));
	}

	const unsigned long long deferred_before =
		PgHGM->status.polardb_reader_pool_idle_trim_deferred.load(
			std::memory_order_relaxed);
	const unsigned long long destroyed_before =
		PgHGM->status.polardb_reader_pool_idle_trim_destroyed.load(
			std::memory_order_relaxed);
	const unsigned long long cancelled_before =
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled.load(
			std::memory_order_relaxed);
	const unsigned long long cancelled_taken_before =
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_taken.load(
			std::memory_order_relaxed);
	const unsigned long long cancelled_retained_before =
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_retained.load(
			std::memory_order_relaxed);
	const unsigned long long cancelled_other_before =
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_other.load(
			std::memory_order_relaxed);
	std::vector<PgSQL_Connection*> connections_to_delete;
	PolarDB_IdleTrimResult first;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		first = reader->polardb_trim_free_connections_to_max_unlocked(
			2, connections_to_delete);
	}
	ok(first.deferred == 3 && first.cancelled == 0 && first.destroyed == 0 &&
			connections_to_delete.empty() &&
			reader->pool_free_count_value() == 5,
		"PolarDB idle-trim grace: first over-limit observation defers exactly the excess inventory");

	PgSQL_Connection* reused = nullptr;
	for (unsigned int index = 0;
			index < reader->ConnectionsFree->conns_length(); index++) {
		PgSQL_Connection* connection = reader->ConnectionsFree->index(index);
		if (connection && connection->polardb_idle_trim_pending) {
			reused = connection;
			break;
		}
	}
	assert(reused != nullptr);
	reused->last_time_used = 1;
	PgSQL_Connection* taken = reader->take_free_connection_for_ping(reused, 2);
	ok(taken == reused &&
			reader->polardb_finish_idle_ping(reused, false) &&
			!reused->polardb_idle_trim_pending &&
			reader->remove_used_connection(reused) &&
			reader->add_matching_connection(reused, key),
		"PolarDB idle-trim grace: reuse cancels stale trim eligibility before adding it back to FREE");

	PolarDB_IdleTrimResult second;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		second = reader->polardb_trim_free_connections_to_max_unlocked(
			2, connections_to_delete);
	}
	ok(second.deferred == 1 && second.cancelled == 0 &&
			second.destroyed == 2 &&
			connections_to_delete.size() == 2 &&
			reader->pool_free_count_value() == 3,
		"PolarDB idle-trim grace: second observation removes only continuously idle excess inventory");
	for (PgSQL_Connection* connection : connections_to_delete) {
		delete connection;
	}
	connections_to_delete.clear();

	PgSQL_Connection* retained = reader->take_matching_connection(key);
	assert(retained != nullptr);
	ok(reader->remove_used_connection(retained),
		"PolarDB idle-trim grace: target-reduction fixture removes one unmarked connection");

	PolarDB_IdleTrimResult third;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		third = reader->polardb_trim_free_connections_to_max_unlocked(
			2, connections_to_delete);
	}
	ok(third.deferred == 0 && third.cancelled == 1 &&
			third.destroyed == 0 && connections_to_delete.empty() &&
			reader->pool_free_count_value() == 2,
		"PolarDB idle-trim grace: falling to the target retains and clears pending inventory");
	delete retained;
	bool retained_trim_marker = false;
	for (unsigned int index = 0;
			index < reader->ConnectionsFree->conns_length(); index++) {
		PgSQL_Connection* connection = reader->ConnectionsFree->index(index);
		retained_trim_marker = retained_trim_marker ||
			(connection && connection->polardb_idle_trim_pending);
	}
	ok(!retained_trim_marker,
		"PolarDB idle-trim grace: retained target inventory has no stale trim eligibility");
	ok(PgHGM->status.polardb_reader_pool_idle_trim_deferred.load(
			std::memory_order_relaxed) == deferred_before + 4 &&
		PgHGM->status.polardb_reader_pool_idle_trim_destroyed.load(
			std::memory_order_relaxed) == destroyed_before + 2 &&
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled.load(
			std::memory_order_relaxed) == cancelled_before + 2 &&
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_taken.load(
			std::memory_order_relaxed) == cancelled_taken_before + 1 &&
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_retained.load(
			std::memory_order_relaxed) == cancelled_retained_before + 1 &&
		PgHGM->status.polardb_reader_pool_idle_trim_cancelled_other.load(
			std::memory_order_relaxed) == cancelled_other_before,
		"PolarDB idle-trim grace: deferred outcomes and cancellation reasons reconcile exactly");

	while (PgSQL_Connection* connection = reader->take_matching_connection(key)) {
		assert(reader->remove_used_connection(connection));
		delete connection;
	}
	ok(reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 0,
		"PolarDB idle-trim grace: fixture cleanup leaves no pooled connection ownership");
}


static void test_reader_pool_status_and_pooled_only_contract() {
	const int writer_hg = 972;
	const int reader_hg = 973;
	const uint64_t TARGET_LSN = 0xE100;

	stage_polardb_topology(PgHGM, "PolarDB reader pool status",
		writer_hg, "polardb-status-writer", 22432,
		reader_hg, "polardb-status-reader", 22433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-status-reader", 22433);
	ok(reader != nullptr,
		"PolarDB reader pool status: reader server container is available");
	if (!reader) {
		return;
	}

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

	reader->polardb_current_lsn.store(TARGET_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);

	PolarDB_ReaderResult empty_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!empty_result.acquired(),
		"PolarDB reader pool status: pooled-only read does not create on empty pool");
	ok(reader->ConnectionsFree->conns_length() == 0 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: empty pooled-only read leaves inventory unchanged");

	PgSQL_Connection *cached = make_cached_reader_connection(reader);
	reader->ConnectionsFree->add(cached);
	reader->status = MYSQL_SERVER_STATUS_SHUNNED;

	PolarDB_ReaderResult offline_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!offline_result.acquired(),
		"PolarDB reader pool status: non-ONLINE reader is not selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: non-ONLINE reader keeps pooled backend idle");

	reader->status = MYSQL_SERVER_STATUS_ONLINE;
	PolarDB_ReaderResult online_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!online_result.acquired(),
		"PolarDB reader pool status: disconnected pooled backend is not selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 0,
		"PolarDB reader pool status: disconnected pooled backend remains idle");

	PolarDB_ReaderResult ordinary_result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, wait,
			/*only_pooled=*/false);
	ok(ordinary_result.acquired() && ordinary_result.conn == cached,
		"PolarDB reader pool status: ordinary reader path can take disconnected backend");
	ok(reader->ConnectionsFree->conns_length() == 0 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB reader pool status: ordinary reader path moves backend to used list");

	if (ordinary_result.conn && ordinary_result.srv &&
			ordinary_result.srv->ConnectionsUsed) {
		ordinary_result.srv->ConnectionsUsed->remove(ordinary_result.conn);
	}
	if (online_result.conn && online_result.srv &&
			online_result.srv->ConnectionsUsed) {
		online_result.srv->ConnectionsUsed->remove(online_result.conn);
	}
	if (!online_result.conn && !ordinary_result.conn) {
		reader->ConnectionsFree->remove(cached);
	}
	PgSQL_Connection *conn_to_delete =
		ordinary_result.conn ? ordinary_result.conn :
			(online_result.conn ? online_result.conn : cached);
	delete conn_to_delete;
}

static void test_reader_pool_capacity_accounting_includes_reuse_pool() {
	const int writer_hg = 978;
	const int reader_hg = 979;

	stage_polardb_topology(PgHGM, "PolarDB reader pool capacity",
		writer_hg, "polardb-capacity-writer", 22632,
		reader_hg, "polardb-capacity-reader", 22633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-capacity-reader", 22633);
	ok(reader != nullptr,
		"PolarDB reader pool capacity: reader server container is available");
	if (!reader) {
		return;
	}

	const long saved_max_connections = reader->max_connections;
	reader->max_connections = 2;

	PgSQL_Connection *classic_free = make_cached_reader_connection(reader);
	reader->ConnectionsFree->add(classic_free);

	PgSQL_Connection *pool_free = make_cached_reader_connection(reader);
	unit_reader_pool_add_shared(reader, pool_free);

	const PolarDB_PoolConnStats stats = reader->polardb_pool_conn_stats();
	ok(stats.free == 2 && stats.used == 0 && stats.total() == 2,
		"PolarDB reader pool capacity: matching and ordinary connections share one core FREE list");
	ok(!reader->polardb_pool_can_open_socket(),
		"PolarDB reader pool capacity: max_connections blocks another socket");
	ok(reader->polardb_pool_can_add_active_connection(),
		"PolarDB reader pool capacity: idle reuse-pool connection does not block active capacity");

	unit_reader_pool_clear_shared(reader, pool_free);
	delete pool_free;

	reader->ConnectionsFree->remove(classic_free);
	delete classic_free;
	reader->max_connections = saved_max_connections;
}

static void test_reader_pool_shared_inventory_rejects_unrelated_connection() {
	const int writer_hg = 988;
	const int reader_hg = 989;

	stage_polardb_topology(PgHGM, "PolarDB reader pool shared mismatch",
		writer_hg, "polardb-shared-mismatch-writer", 23632,
		reader_hg, "polardb-shared-mismatch-reader", 23633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-shared-mismatch-reader", 23633);
	ok(reader != nullptr,
		"PolarDB reader pool shared mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PolarDB_StartupClientContext other_startup_client;
	other_startup_client.identity = unit_other_proxy_identity();
	PgSQL_Connection *other = make_cached_reader_connection(reader);
	ok(other != nullptr,
		"PolarDB reader pool shared mismatch: connection fixture is available");
	if (!other) {
		return;
	}
	other->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	other->polardb_startup_client = other_startup_client;
	unit_reader_pool_add_shared(reader, other);

	const unsigned long long considered_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered];
	const unsigned long long match_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_match_attempt];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool shared mismatch: unrelated shared entry is not acquired");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_server_considered] == considered_before + 1,
		"PolarDB reader pool mismatch: connection counts do not select the server");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_match_attempt] == match_before + 1,
		"PolarDB reader pool mismatch: one matching connection lookup is made");

	unit_reader_pool_clear_shared(reader, other);
	delete other;
}

static void test_reader_pool_shared_prefix_reaches_compatible_backend() {
	const int writer_hg = 912;
	const int reader_hg = 913;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool shared prefix",
		writer_hg, "polardb-prefix-writer", 24962,
		reader_hg, "polardb-prefix-reader", 24963);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-prefix-reader", 24963);
	ok(reader != nullptr,
		"PolarDB reader pool shared prefix: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	std::vector<PgSQL_Connection*> incompatible;
	for (int i = 0; i < 20; i++) {
		PgSQL_Connection *other = make_cached_reader_connection(reader);
		ok(other != nullptr,
			"PolarDB reader pool shared prefix: incompatible fixture is available");
		if (!other) {
			continue;
		}
		std::string username = "polardb_prefix_other_" + std::to_string(i);
		other->userinfo->set(
			const_cast<char*>(username.c_str()),
			(char*)"polardb_unit_pass",
			(char*)"polardb_unit_db",
			nullptr);
		unit_reader_pool_add_shared(reader, other);
		incompatible.push_back(other);
	}

	PgSQL_Connection *compatible = make_cached_reader_connection(reader);
	ok(compatible != nullptr,
		"PolarDB reader pool shared prefix: compatible fixture is available");
	if (compatible) {
		unit_reader_pool_add_shared(reader, compatible);
	}

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	const unsigned int remaining_shared =
		unit_reader_pool_shared_free_count(reader);
	ok(!result.acquired() && remaining_shared == incompatible.size(),
		"PolarDB reader pool shared prefix: compatible backend behind incompatible prefix is reached");

	// The unit fixture has no live PGconn, so the reached backend is rejected by
	// the normal connectedness check after it leaves shared inventory.
	if (remaining_shared > incompatible.size() && compatible &&
			unit_reader_pool_clear_shared(reader, compatible)) {
		delete compatible;
		compatible = nullptr;
	}
	for (PgSQL_Connection *other : incompatible) {
		if (unit_reader_pool_clear_shared(reader, other)) {
			delete other;
		}
	}
}

static void test_reader_pool_profile_mismatch_drops_backend() {
	const int writer_hg = 992;
	const int reader_hg = 993;

	stage_polardb_topology(PgHGM, "PolarDB reader pool profile mismatch",
		writer_hg, "polardb-profile-writer", 23832,
		reader_hg, "polardb-profile-reader", 23833);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-profile-reader", 23833);
	ok(reader != nullptr,
		"PolarDB reader pool profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB reader pool profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation(
			static_cast<int>(PolarDB_ProxyIdentityMode::PROXY));
	unit_reader_pool_add_shared(reader, stale);
	ok(stale->pgsql_conn != nullptr,
		"PolarDB reader pool profile mismatch: libpq fixture can be safely destroyed");

	const unsigned long long reject_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_profile];
	const unsigned long long drop_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool profile mismatch: stale backend is not acquired");
	ok(unit_reader_pool_shared_free_count(reader) == 1,
		"PolarDB reader pool profile mismatch: a missing match does not scan unrelated generations");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_profile] == reject_before,
		"PolarDB reader pool profile mismatch: unrelated exact key is not examined");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable] == drop_before,
		"PolarDB reader pool profile mismatch: unrelated exact key is not dropped on lookup");
	if (unit_reader_pool_clear_shared(reader, stale)) {
		delete stale;
	}
}

static void test_reader_pool_identity_mode_mismatch_drops_backend() {
	const int writer_hg = 946;
	const int reader_hg = 947;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool identity mode mismatch",
		writer_hg, "polardb-identity-mode-writer", 24932,
		reader_hg, "polardb-identity-mode-reader", 24933);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-identity-mode-reader", 24933);
	ok(reader != nullptr,
		"PolarDB reader pool identity mode mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB reader pool identity mode mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	unit_reader_pool_add_shared(reader, stale);

	const unsigned long long reject_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_identity];
	const unsigned long long drop_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB reader pool identity mode mismatch: stale identity-mode backend is not acquired");
	ok(unit_reader_pool_shared_free_count(reader) == 0,
		"PolarDB reader pool identity mode mismatch: stale identity-mode backend is removed");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reject_identity] == reject_before + 1,
		"PolarDB reader pool identity mode mismatch: identity reject is counted");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_drop_unusable] == drop_before + 1,
		"PolarDB reader pool identity mode mismatch: stale backend drop is counted");
}

static void test_reader_pool_client_identity_mode_not_released_to_shared() {
	const int writer_hg = 948;
	const int reader_hg = 949;

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB reader pool client mode release",
		writer_hg, "polardb-client-mode-writer", 24942,
		reader_hg, "polardb-client-mode-reader", 24943);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-client-mode-reader", 24943);
	ok(reader != nullptr,
		"PolarDB reader pool client mode release: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	ok(conn != nullptr,
		"PolarDB reader pool client mode release: connection fixture is available");
	if (!conn) {
		return;
	}

	conn->polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB reader pool client mode release: core USED list owns the connection");

	const unsigned long long drop_before =
		PgHGM->status.polardb_reader_pool_drop_client_identity.load(
			std::memory_order_relaxed);
	PgSQL_Thread worker;
	worker.push_MyConn_local(conn);

	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0,
		"PolarDB reader pool client mode release: CLIENT-mode backend is closed, not shared");
	ok(PgHGM->status.polardb_reader_pool_drop_client_identity.load(
			std::memory_order_relaxed) == drop_before + 1,
		"PolarDB reader pool client mode release: client-identity drop counter increments");
}

#if POLARDB_PROFILE
static void test_reader_pool_shared_transfer_counters() {
	const int writer_hg = 948;
	const int reader_hg = 949;

	stage_polardb_topology(PgHGM, "PolarDB shared pool counters",
		writer_hg, "polardb-shared-counters-writer", 24942,
		reader_hg, "polardb-shared-counters-reader", 24943);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-shared-counters-reader", 24943);
	ok(reader != nullptr,
		"PolarDB shared pool counters: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	const unsigned long long take_attempt_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_take_attempt];
	const unsigned long long take_hit_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_take_hit];
	const unsigned long long return_attempt_before =
		PgHGM->status.polardb_reader_pool_shared_return_attempt.load(
			std::memory_order_relaxed);
	const unsigned long long return_accepted_before =
		PgHGM->status.polardb_reader_pool_shared_return_accepted.load(
			std::memory_order_relaxed);
	const unsigned long long worker_return_attempt_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_attempt];
	const unsigned long long worker_return_accepted_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_accepted];
	const unsigned long long global_return_to_core_before =
		PgHGM->status.polardb_reader_pool_return_to_core.load(
			std::memory_order_relaxed);
	const unsigned long long worker_return_to_core_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_return_to_core];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult acquired = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(acquired.acquired() && acquired.conn == conn,
		"PolarDB shared pool counters: selected reader connection is acquired");
	ok(worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_take_attempt] ==
			take_attempt_before + 1 &&
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_take_hit] ==
			take_hit_before + 1,
		"PolarDB shared pool counters: successful shared take is counted");

	ok(PgHGM->return_connection_with_match_key(
			acquired.conn, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
			RejectedConnectionAction::DESTROY, &worker).status ==
				PoolReturnStatus::STORED,
		"PolarDB shared pool counters: acquired connection returns to shared pool");
	ok(PgHGM->status.polardb_reader_pool_shared_return_attempt.load(
			std::memory_order_relaxed) == return_attempt_before &&
		PgHGM->status.polardb_reader_pool_shared_return_accepted.load(
			std::memory_order_relaxed) == return_accepted_before &&
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_attempt] ==
			worker_return_attempt_before + 1 &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_reader_pool_shared_return_accepted] ==
				worker_return_accepted_before + 1 &&
			PgHGM->status.polardb_reader_pool_return_to_core.load(
				std::memory_order_relaxed) == global_return_to_core_before &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_reader_pool_return_to_core] ==
				worker_return_to_core_before + 1,
		"PolarDB shared pool counters: worker return uses only worker-local counters");

	reader->remove_free_connection(conn);
	delete conn;

	const unsigned long long free_zero_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_free_zero_before_lock];
	const unsigned long long zero_became_hit_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_free_zero_became_hit];
	PolarDB_ReaderResult empty = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!empty.acquired(),
		"PolarDB shared pool counters: pooled request misses an observed-empty shared pool");
	ok(worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_free_zero_before_lock] ==
			free_zero_before + 1 &&
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_free_zero_became_hit] ==
			zero_became_hit_before,
		"PolarDB shared pool counters: observed-empty miss is recorded without a zero-to-hit race");
}
#endif // POLARDB_PROFILE

static void test_reader_pool_worker_local_reuse() {
	const int writer_hg = 946;
	const int reader_hg = 947;

	stage_polardb_topology(PgHGM, "PolarDB worker-local reuse",
		writer_hg, "polardb-worker-cache-writer", 24932,
		reader_hg, "polardb-worker-cache-reader", 24933);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader = find_pgsql_server(
		reader_hgc, "polardb-worker-cache-reader", 24933);
	ok(reader != nullptr,
		"PolarDB worker-local reuse: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);
	const int original_retention =
		pgsql_thread___polardb_reader_connection_retention;
	pgsql_thread___polardb_reader_connection_retention = 0;

#if POLARDB_PROFILE
	const unsigned long long local_take_hit_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_take_hit];
	const unsigned long long local_scan_steps_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_scan_steps];
	const unsigned long long local_store_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_attempt];
	const unsigned long long local_store_accepted_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_accepted];
	const unsigned long long local_return_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_return_to_shared];
	const unsigned long long return_group_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_group];
	const unsigned long long return_group_1_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_group_1];
	const unsigned long long bucket_emptied_before =
		PgHGM->status.polardb_reader_pool_exact_bucket_emptied.load(
			std::memory_order_relaxed);
	const unsigned long long bucket_reused_before =
		PgHGM->status.polardb_reader_pool_exact_bucket_reused.load(
			std::memory_order_relaxed);
#endif // POLARDB_PROFILE

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult first = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(first.acquired() && first.conn == conn,
		"PolarDB worker-local reuse: first read takes the selected reader connection");
	worker->push_MyConn_local(first.conn);
	PolarDB_ReaderRetentionUnitAccess::set_local_connection_count(
		worker.get(), 0);
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local reuse: connection remains in the core USED list");

	std::unique_ptr<PgSQL_Thread> waiting_worker(new PgSQL_Thread());
	PgSQL_Session waiting_sess;
	attach_test_frontend(waiting_sess, waiting_worker.get());
	const int original_max_connections = reader->max_connections;
	const int original_creation_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		std::numeric_limits<int>::max();
	reader->max_connections = 2;
	PgSQL_PoolMatchKey different_match_key = unit_reader_pool_match_key(conn);
	different_match_key.words[1] ^= 1;
	PgSQL_PoolGetResult different_created =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, different_match_key, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
				PgSQL_PoolGetMode::ALLOW_CREATE);
	ok(different_created.conn &&
			different_created.source == PgSQL_PoolGetSource::CREATED,
		"PolarDB worker-local reuse: incompatible local capacity does not block required creation");
	if (different_created.conn) {
		reader->remove_used_connection(different_created.conn);
		delete different_created.conn;
	}
	PolarDB_ReaderResult peer_created = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &waiting_sess, plan, no_wait,
		/*only_pooled=*/false);
	ok(peer_created.acquired() && peer_created.conn != conn &&
			reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local reuse: a cold peer creates below the server limit because another worker's local connection is unavailable");
	if (peer_created.conn) {
		reader->remove_used_connection(peer_created.conn);
		delete peer_created.conn;
	}
	reader->max_connections = original_max_connections;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		original_creation_throttle;
	ok(worker->get_MyConn_local(
			reader_hg, &sess, nullptr, 0, -1) == nullptr,
		"PolarDB worker-local reuse: normal local lookup does not take an exact-key reader connection");
	PolarDB_PoolKey different_key = conn->polardb_pool_key;
	different_key.auth_hash ^= 1;
	ok(worker->polardb_take_local_reader_connection(
			reader, conn->polardb_startup_profile_generation,
			different_key) == nullptr,
		"PolarDB worker-local reuse: a different exact key does not take the connection");

	PolarDB_ReaderResult second = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(second.acquired() && second.conn == conn &&
			PolarDB_ReaderRetentionUnitAccess::local_connection_count(
				worker.get()) == 0,
		"PolarDB worker-local reuse: next read reuses the same selected-server connection");
	worker->push_MyConn_local(second.conn);
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_take_hit] ==
			local_take_hit_before + 1,
		"PolarDB worker-local reuse: local reuse hit is counted");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_scan_steps] ==
			local_scan_steps_before + 2,
		"PolarDB worker-local reuse: examined cache slots are counted");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_attempt] ==
			local_store_attempt_before + 2,
		"PolarDB worker-local reuse: both local store attempts are counted once");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_accepted] ==
			local_store_accepted_before + 2,
		"PolarDB worker-local reuse: both local stores are counted");
#endif // POLARDB_PROFILE

	std::atomic<bool> return_finished{false};
	PgHGM->wrlock();
	std::thread return_thread([&]() {
		worker->return_local_connections();
		return_finished.store(true, std::memory_order_release);
	});
	for (unsigned int attempt = 0;
			attempt < 500 &&
			!return_finished.load(std::memory_order_acquire);
			attempt++) {
		usleep(1000);
	}
	const bool returned_without_global_lock =
		return_finished.load(std::memory_order_acquire);
	PgHGM->wrunlock();
	return_thread.join();
	ok(returned_without_global_lock,
		"PolarDB worker-local reuse: exact return does not wait for the global hostgroup lock");
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker-local reuse: worker pass returns the connection to core FREE");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_return_to_shared] ==
			local_return_before + 1,
		"PolarDB worker-local reuse: return to shared pool is counted");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_group] ==
			return_group_before + 1 &&
			worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_shared_return_group_1] ==
			return_group_1_before + 1,
		"PolarDB worker-local reuse: one grouped lock acquisition with one connection is counted");
	ok(PgHGM->status.polardb_reader_pool_exact_bucket_emptied.load(
			std::memory_order_relaxed) ==
			bucket_emptied_before + 1 &&
			PgHGM->status.polardb_reader_pool_exact_bucket_reused.load(
				std::memory_order_relaxed) ==
			bucket_reused_before + 1,
		"PolarDB worker-local reuse: exact bucket retention and reuse are counted");
#endif // POLARDB_PROFILE

	pgsql_thread___polardb_reader_connection_retention = 1;
	PolarDB_ReaderResult retained = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(retained.acquired() && retained.conn == conn,
		"PolarDB worker-local retention: selected reader is acquired from shared storage");
	worker->push_MyConn_local(retained.conn);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local retention: a reader used in the pass remains local");
	PolarDB_ReaderResult reused = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(reused.acquired() && reused.conn == conn,
		"PolarDB worker-local retention: the next pass reuses the local reader");
	worker->push_MyConn_local(reused.conn);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local retention: reuse renews retention for one pass");
	pgsql_thread___polardb_reader_connection_retention = 0;
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker-local retention: disabling retention returns local readers immediately");
	PolarDB_ReaderRetentionUnitAccess::set_local_connection_count(
		worker.get(), 1);
	worker->return_local_connections();
	ok(PolarDB_ReaderRetentionUnitAccess::local_connection_count(
			worker.get()) == 0,
		"PolarDB worker-local reuse: an empty cache repairs stale reader accounting");
	pgsql_thread___polardb_reader_connection_retention = original_retention;
	reader->remove_free_connection(conn);
	delete conn;
}


static void test_reader_pool_worker_local_duplicates_last_one_pass() {
	const int writer_hg = 942;
	const int reader_hg = 943;

	stage_polardb_topology(PgHGM, "PolarDB worker-local duplicate sharing",
		writer_hg, "polardb-worker-share-writer", 24912,
		reader_hg, "polardb-worker-share-reader", 24913);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-worker-share-reader", 24913);
	ok(reader != nullptr,
		"PolarDB worker-local sharing: reader server is available");
	if (!reader) {
		return;
	}
	reader->max_connections = 2;
	const int original_retention =
		pgsql_thread___polardb_reader_connection_retention;
	pgsql_thread___polardb_reader_connection_retention = 1;

	PgSQL_Connection* first = make_cached_reader_connection(reader);
	PgSQL_Connection* duplicate = make_cached_reader_connection(reader);
	first->pgsql_conn = unit_connected_pgconn();
	duplicate->pgsql_conn = unit_connected_pgconn();
	ok(first->pgsql_conn != nullptr && duplicate->pgsql_conn != nullptr,
		"PolarDB worker-local sharing: connected backend fixtures are available");
	if (!first->pgsql_conn || !duplicate->pgsql_conn) {
		pgsql_thread___polardb_reader_connection_retention = original_retention;
		delete first;
		delete duplicate;
		return;
	}

	const PgSQL_PoolMatchKey first_match_key =
		unit_reader_pool_match_key(first);
	const PgSQL_PoolMatchKey duplicate_match_key =
		unit_reader_pool_match_key(duplicate);
	ok(first_match_key == duplicate_match_key,
		"PolarDB worker-local sharing: both connections have the same exact key");
	ok(reader->add_used_matching_connection(first, first_match_key) &&
			reader->add_used_matching_connection(duplicate, duplicate_match_key),
		"PolarDB worker-local sharing: the two-connection backend pool is in use");

	PgSQL_Thread first_worker;
	PgSQL_Thread second_worker;
	first_worker.push_MyConn_local(first);
	first_worker.push_MyConn_local(duplicate);
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local sharing: matching connections remain local until the worker pass ends");

	ok(second_worker.polardb_take_local_reader_connection(
			reader, duplicate->polardb_startup_profile_generation,
			duplicate->polardb_pool_key) == nullptr,
		"PolarDB worker-local sharing: another worker has no private copy");
	PgSQL_Connection* shared =
		reader->take_matching_connection(duplicate_match_key);
	ok(shared == nullptr,
		"PolarDB worker-local sharing: another worker waits until the current pass returns unused connections");
	PgSQL_Connection* local_first =
		first_worker.polardb_take_local_reader_connection(
			reader, first->polardb_startup_profile_generation,
			first->polardb_pool_key);
	PgSQL_Connection* local_duplicate =
		first_worker.polardb_take_local_reader_connection(
			reader, duplicate->polardb_startup_profile_generation,
			duplicate->polardb_pool_key);
	ok(local_first == first && local_duplicate == duplicate,
		"PolarDB worker-local sharing: the first worker can reuse both matching connections");

	first_worker.push_MyConn_local(local_first);
	first_worker.push_MyConn_local(local_duplicate);
	PolarDB_ReaderRetentionUnitAccess::set_local_connection_count(
		&first_worker, 3);
	first_worker.return_local_connections();
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0 &&
			PolarDB_ReaderRetentionUnitAccess::local_connection_count(
				&first_worker) == 2,
		"PolarDB worker-local sharing: readers used in the pass remain local");
	PolarDB_ReaderRetentionUnitAccess::set_local_connection_count(
		&first_worker, 1);
	first_worker.return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 2 &&
			PolarDB_ReaderRetentionUnitAccess::local_connection_count(
				&first_worker) == 0,
		"PolarDB worker-local sharing: unused readers return after one pass");
	pgsql_thread___polardb_reader_connection_retention = original_retention;
	reader->remove_free_connection(first);
	reader->remove_free_connection(duplicate);
	delete first;
	delete duplicate;

	PgSQL_Connection* default_user = make_cached_reader_connection(reader);
	PgSQL_Connection* other_user = make_cached_reader_connection(reader);
	default_user->pgsql_conn = unit_connected_pgconn();
	other_user->pgsql_conn = unit_connected_pgconn();
	other_user->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	ok(default_user->pgsql_conn != nullptr && other_user->pgsql_conn != nullptr,
		"PolarDB worker-local sharing: separate-key backend fixtures are available");
	if (!default_user->pgsql_conn || !other_user->pgsql_conn) {
		delete default_user;
		delete other_user;
		return;
	}

	const PgSQL_PoolMatchKey default_user_match_key =
		unit_reader_pool_match_key(default_user);
	const PgSQL_PoolMatchKey other_user_match_key =
		unit_reader_pool_match_key(other_user);
	ok(!(default_user_match_key == other_user_match_key),
		"PolarDB worker-local sharing: different users have different exact keys");
	ok(reader->add_used_matching_connection(
			default_user, default_user_match_key) &&
			reader->add_used_matching_connection(
				other_user, other_user_match_key),
		"PolarDB worker-local sharing: separate-key connections are in use");

	first_worker.push_MyConn_local(default_user);
	first_worker.push_MyConn_local(other_user);
	ok(reader->pool_used_count_value() == 2 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker-local sharing: one worker keeps one connection for each exact key");
	PgSQL_Connection* local_default =
		first_worker.polardb_take_local_reader_connection(
			reader, default_user->polardb_startup_profile_generation,
			default_user->polardb_pool_key);
	PgSQL_Connection* local_other =
		first_worker.polardb_take_local_reader_connection(
			reader, other_user->polardb_startup_profile_generation,
			other_user->polardb_pool_key);
	ok(local_default == default_user && local_other == other_user,
		"PolarDB worker-local sharing: each exact key retrieves its own local connection");
	ok(PgHGM->return_connection_with_match_key(
				local_default, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
				RejectedConnectionAction::DESTROY).status ==
				PoolReturnStatus::STORED &&
			PgHGM->return_connection_with_match_key(
				local_other, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
				RejectedConnectionAction::DESTROY).status ==
				PoolReturnStatus::STORED,
		"PolarDB worker-local sharing: separate-key connections return to the shared pool");
	reader->remove_free_connection(default_user);
	reader->remove_free_connection(other_user);
	delete default_user;
	delete other_user;
}

static void test_reader_pool_return_rebuilds_cleared_key() {
	const int writer_hg = 944;
	const int reader_hg = 945;

	stage_polardb_topology(PgHGM, "PolarDB cleared pool key return",
		writer_hg, "polardb-cleared-key-writer", 24922,
		reader_hg, "polardb-cleared-key-reader", 24923);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-cleared-key-reader", 24923);
	ok(reader != nullptr,
		"PolarDB cleared pool key return: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult acquired = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(acquired.acquired() && acquired.conn == conn &&
			acquired.conn->polardb_selected_server_snapshot != nullptr &&
			acquired.conn->polardb_exact_pool_connection,
		"PolarDB cleared pool key return: selected connection retains exact-pool ownership and the server list");
	if (!acquired.acquired()) {
		reader->remove_free_connection(conn);
		delete conn;
		return;
	}

	acquired.conn->polardb_pool_key = PolarDB_PoolKey{};
	ok(PgHGM->return_connection_with_match_key(
				acquired.conn, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
				RejectedConnectionAction::DESTROY).status ==
				PoolReturnStatus::STORED &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			acquired.conn->polardb_selected_server_snapshot == nullptr,
		"PolarDB cleared pool key return: return rebuilds the exact key after reset");

	reader->remove_free_connection(conn);
	delete conn;
}

static void test_reader_pool_destroy_used_connection_updates_accounting() {
	const int writer_hg = 934;
	const int reader_hg = 935;

	stage_polardb_topology(PgHGM, "PolarDB reader pool destroy used",
		writer_hg, "polardb-destroy-used-writer", 24632,
		reader_hg, "polardb-destroy-used-reader", 24633);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-destroy-used-reader", 24633);
	ok(reader != nullptr,
		"PolarDB reader pool destroy used: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	ok(conn != nullptr,
		"PolarDB reader pool destroy used: connection fixture is available");
	if (!conn) {
		return;
	}

	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB reader pool destroy used: core USED list owns the connection");
	PgHGM->destroy_MyConn_from_pool(conn);

	ok(reader->pool_used_count_value() == 0,
		"PolarDB reader pool destroy used: core USED count is reduced");
}

static void test_reader_pool_rejected_return_defers_destruction() {
	const int writer_hg = 1102;
	const int reader_hg = 1103;

	stage_polardb_topology(PgHGM, "PolarDB deferred return destruction",
		writer_hg, "polardb-deferred-return-writer", 26042,
		reader_hg, "polardb-deferred-return-reader", 26043);

	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-deferred-return-reader", 26043);
	ok(reader != nullptr,
		"PolarDB deferred return destruction: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB deferred return destruction: core USED list owns the connection");
	conn->async_state_machine = ASYNC_QUERY_START;

	const PoolReturnResult returned = PgHGM->return_connection_with_match_key(
		conn, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
		RejectedConnectionAction::DETACH);
	ok(returned.status == PoolReturnStatus::DETACHED &&
			returned.detached_connection == conn,
		"PolarDB deferred return destruction: rejected connection is returned to the caller");
	ok(returned.detached_connection &&
			returned.detached_connection->polardb_selected_server_snapshot != nullptr,
		"PolarDB deferred return destruction: server ownership remains until deletion");
	ok(reader->pool_used_count_value() == 0,
		"PolarDB deferred return destruction: rejected connection leaves core USED accounting");
	delete returned.detached_connection;
}

static void test_reader_pool_batch_rejected_return_defers_destruction() {
	const int writer_hg = 1104;
	const int reader_hg = 1105;

	stage_polardb_topology(PgHGM, "PolarDB batch deferred return destruction",
		writer_hg, "polardb-batch-deferred-return-writer", 26052,
		reader_hg, "polardb-batch-deferred-return-reader", 26053);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-batch-deferred-return-reader", 26053);
	ok(reader != nullptr,
		"PolarDB batch deferred return destruction: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB batch deferred return destruction: core USED list owns the connection");
	conn->async_state_machine = ASYNC_QUERY_START;

	std::vector<PgSQL_Connection*> connections{conn};
	std::vector<PgSQL_Connection*> connections_to_delete;
	PgHGM->polardb_return_reader_connections(
		nullptr,
		connections, connections_to_delete);
	ok(connections_to_delete.size() == 1 &&
			connections_to_delete.front() == conn,
		"PolarDB batch deferred return destruction: rejected connection is returned to the caller");
	ok(conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB batch deferred return destruction: server ownership remains until deletion");
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0,
		"PolarDB batch deferred return destruction: rejected connection leaves core accounting");
	delete conn;
}

static void test_reader_pool_batch_mixed_parent_detaches_safely() {
	const int writer_hg = 1120;
	const int reader_hg = 1121;

	stage_polardb_topology_two_readers(
		PgHGM, "PolarDB mixed-parent batch return",
		writer_hg, "polardb-mixed-return-writer", 26610,
		reader_hg,
		"polardb-mixed-return-reader-a", 26611,
		"polardb-mixed-return-reader-b", 26612);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-mixed-return-reader-a", 26611);
	PgSQL_SrvC* peer = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-mixed-return-reader-b", 26612);
	ok(reader != nullptr && peer != nullptr,
		"PolarDB mixed-parent batch return: both reader containers are available");
	if (!reader || !peer) {
		return;
	}

	PgSQL_Connection* reader_conn = make_cached_reader_connection(reader);
	PgSQL_Connection* peer_conn = make_cached_reader_connection(peer);
	reader_conn->pgsql_conn = unit_connected_pgconn();
	peer_conn->pgsql_conn = unit_connected_pgconn();
	const PgSQL_PoolMatchKey reader_key =
		unit_reader_pool_match_key(reader_conn);
	const PgSQL_PoolMatchKey peer_key =
		unit_reader_pool_match_key(peer_conn);
	ok(reader->add_used_matching_connection(reader_conn, reader_key) &&
			peer->add_used_matching_connection(peer_conn, peer_key),
		"PolarDB mixed-parent batch return: each server owns one USED connection");

	std::vector<PgSQL_Connection*> connections{reader_conn, peer_conn};
	std::vector<PgSQL_Connection*> connections_to_delete;
	PgHGM->polardb_return_reader_connections(
		nullptr, connections, connections_to_delete);

	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			peer->pool_used_count_value() == 0 &&
			peer->pool_free_count_value() == 0 &&
			connections_to_delete.size() == 1 &&
			connections_to_delete.front() == peer_conn,
		"PolarDB mixed-parent batch return: foreign connection leaves its real USED list before deferred deletion");

	reader->remove_free_connection(reader_conn);
	delete reader_conn;
	delete peer_conn;
}

static void test_reader_pool_batch_return_uses_worker_counters() {
	const int writer_hg = 1116;
	const int reader_hg = 1117;

	stage_polardb_topology(PgHGM, "PolarDB batch return worker counters",
		writer_hg, "polardb-batch-counter-writer", 26600,
		reader_hg, "polardb-batch-counter-reader", 26601);

	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-batch-counter-reader", 26601);
	ok(reader != nullptr,
		"PolarDB batch return counters: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB batch return counters: core USED list owns the connection");

	const unsigned long global_push_before = __atomic_load_n(
		&PgHGM->status.pgconnpoll_push, __ATOMIC_RELAXED);
	const unsigned long long global_return_before =
		PgHGM->status.polardb_reader_pool_return_to_core.load(
			std::memory_order_relaxed);
	const unsigned long long local_push_before =
		worker->status_variables.pgconnpoll_push;
	const unsigned long long local_return_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_return_to_core];

	std::vector<PgSQL_Connection*> connections{conn};
	std::vector<PgSQL_Connection*> connections_to_delete;
	PgHGM->polardb_return_reader_connections(
		worker.get(), connections, connections_to_delete);

	ok(connections_to_delete.empty() &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB batch return counters: reusable connection reaches core FREE");
	ok(worker->status_variables.pgconnpoll_push == local_push_before + 1 &&
			__atomic_load_n(&PgHGM->status.pgconnpoll_push,
				__ATOMIC_RELAXED) == global_push_before,
		"PolarDB batch return counters: push is recorded only on the worker");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_return_to_core] ==
			local_return_before + 1 &&
			PgHGM->status.polardb_reader_pool_return_to_core.load(
				std::memory_order_relaxed) == global_return_before,
		"PolarDB batch return counters: accepted return is recorded only on the worker");

	reader->remove_free_connection(conn);
	delete conn;
}

static void test_classic_worker_local_cache_policy() {
	const int writer_hg = 1106;
	const int reader_hg = 1107;

	stage_polardb_topology(PgHGM, "PostgreSQL worker-local cache policy",
		writer_hg, "pgsql-worker-cache-writer", 26062,
		reader_hg, "pgsql-worker-cache-reader", 26063);

	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(writer_hg),
		"pgsql-worker-cache-writer", 26062);
	ok(writer != nullptr,
		"PostgreSQL worker-local cache policy: writer server container is available");
	if (!writer) {
		return;
	}

	const unsigned int original_thread_count = GloPTH->num_threads;
	GloPTH->num_threads = 2;

	PgSQL_Connection* local_first = make_cached_reader_connection(writer);
	PgSQL_Connection* local_second = make_cached_reader_connection(writer);
	local_first->polardb_pool_key = PolarDB_PoolKey{};
	local_second->polardb_pool_key = PolarDB_PoolKey{};
	writer->ConnectionsUsed->add(local_first);
	writer->ConnectionsUsed->add(local_second);
	ok(!local_first->polardb_exact_pool_connection &&
			!local_second->polardb_exact_pool_connection &&
			!PgHGM->return_connection_with_match_key(
				local_first, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
				RejectedConnectionAction::DESTROY).handled(),
		"PostgreSQL worker-local cache policy: classic keyless return bypasses exact-pool handling");

	pgsql_thread___bounded_local_connection_cache = 0;
	PgSQL_Thread unbounded_worker;
	unbounded_worker.push_MyConn_local(local_first);
	unbounded_worker.push_MyConn_local(local_second);
	ok(writer->pool_used_count_value() == 2 &&
			writer->pool_free_count_value() == 0,
		"PostgreSQL worker-local cache policy: value 0 keeps 3.0.7 local reuse");
	std::atomic<bool> classic_return_finished{false};
	PgHGM->wrlock();
	std::thread classic_return_thread([&]() {
		unbounded_worker.return_local_connections();
		classic_return_finished.store(true, std::memory_order_release);
	});
	for (unsigned int attempt = 0;
			attempt < 500 &&
			!classic_return_finished.load(std::memory_order_acquire);
			attempt++) {
		usleep(1000);
	}
	const bool classic_return_avoided_global_lock =
		classic_return_finished.load(std::memory_order_acquire);
	PgHGM->wrunlock();
	classic_return_thread.join();
	ok(classic_return_avoided_global_lock,
		"PostgreSQL worker-local cache policy: classic batch return does not wait for the global hostgroup lock");
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 2,
		"PostgreSQL worker-local cache policy: 3.0.7 local entries return after the worker pass");
	writer->remove_free_connection(local_first);
	writer->remove_free_connection(local_second);
	delete local_first;
	delete local_second;

	PgSQL_Connection* bounded_first = make_cached_reader_connection(writer);
	PgSQL_Connection* bounded_second = make_cached_reader_connection(writer);
	bounded_first->polardb_pool_key = PolarDB_PoolKey{};
	bounded_second->polardb_pool_key = PolarDB_PoolKey{};
	writer->ConnectionsUsed->add(bounded_first);
	writer->ConnectionsUsed->add(bounded_second);

	pgsql_thread___bounded_local_connection_cache = 1;
	PgSQL_Thread bounded_worker;
	bounded_worker.push_MyConn_local(bounded_first);
	bounded_worker.push_MyConn_local(bounded_second);
	ok(writer->pool_used_count_value() == 1 &&
			writer->pool_free_count_value() == 1,
		"PostgreSQL worker-local cache policy: value 1 uses the 3.0.9 bounded behavior");
	bounded_worker.return_local_connections();
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 2,
		"PostgreSQL worker-local cache policy: bounded local entry returns after the worker pass");
	writer->remove_free_connection(bounded_first);
	writer->remove_free_connection(bounded_second);
	delete bounded_first;
	delete bounded_second;

	PgSQL_Connection* removed_mapping = make_cached_reader_connection(writer);
	removed_mapping->polardb_pool_key = PolarDB_PoolKey{};
	writer->ConnectionsUsed->add(removed_mapping);
	PgSQL_Thread removed_mapping_worker;
	PgSQL_Session removed_mapping_session;
	attach_test_frontend(
		removed_mapping_session, &removed_mapping_worker);
	removed_mapping_worker.push_MyConn_local(removed_mapping);
	const bool polardb_active_before =
		PgHGM->status.polardb_active.load(std::memory_order_relaxed);
	PgHGM->status.polardb_active.store(false, std::memory_order_relaxed);
	ok(removed_mapping_worker.get_MyConn_local(
			writer_hg, &removed_mapping_session, nullptr, 0, -1) == nullptr,
		"PostgreSQL worker-local cache policy: removed PolarDB mapping does not expose an RFQ-started backend");
	PgHGM->status.polardb_active.store(
		polardb_active_before, std::memory_order_relaxed);
	removed_mapping_worker.return_local_connections();
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 1,
		"PostgreSQL worker-local cache policy: rejected RFQ backend returns to the shared pool");
	writer->remove_free_connection(removed_mapping);
	delete removed_mapping;

	GloPTH->num_threads = original_thread_count;
	pgsql_thread___bounded_local_connection_cache = 0;
}

static void test_reader_pool_key_invalidates_on_variable_change() {
	PgSQL_Session sess;
	attach_test_frontend(sess);
	PgSQL_Connection *client_conn = sess.client_myds->myconn;

	client_conn->polardb_pool_key.auth_hash = 11;
	client_conn->polardb_pool_key.startup_identity_hash = 22;
	client_conn->polardb_pool_key.startup_options_hash = 33;
	ok(!client_conn->polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: fixture starts with cached key");

	const bool changed = pgsql_variables.client_set_value(
		&sess, PGSQL_DATESTYLE, "ISO, MDY", false);
	ok(changed,
		"PolarDB reader pool key invalidation: client variable update succeeds");
	ok(client_conn->polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: client variable update clears cached key");

	PgSQL_Connection backend_conn(false);
	set_test_pgsql_defaults(&backend_conn);
	backend_conn.copy_pgsql_variables_to_startup_parameters(
		/*copy_only_critical_param=*/true);
	backend_conn.polardb_pool_key.auth_hash = 44;
	backend_conn.polardb_pool_key.startup_options_hash = 55;
	backend_conn.reset();
	ok(backend_conn.polardb_pool_key.empty(),
		"PolarDB reader pool key invalidation: backend reset clears cached key");
}

static void test_classic_free_profile_mismatch_drops_backend() {
	const int writer_hg = 994;
	const int reader_hg = 995;

	stage_polardb_topology(PgHGM, "PolarDB classic free profile mismatch",
		writer_hg, "polardb-classic-profile-writer", 23842,
		reader_hg, "polardb-classic-profile-reader", 23843);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-classic-profile-reader", 23843);
	ok(reader != nullptr,
		"PolarDB classic free profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB classic free profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation(
			static_cast<int>(PolarDB_ProxyIdentityMode::PROXY));
	stale->pgsql_conn = PQconnectStart("polardb_unit_invalid_conninfo=1");
	ok(stale->pgsql_conn != nullptr,
		"PolarDB classic free profile mismatch: libpq fixture can be safely destroyed");
	reader->ConnectionsFree->add(stale);

	const unsigned long long evicted_before =
		PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed);
	const unsigned long long skipped_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB classic free profile mismatch: stale backend is not acquired");
	ok(reader->ConnectionsFree->conns_length() == 1,
		"PolarDB classic free profile mismatch: exact-only lookup does not scan classic FREE entries");
	ok(PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed) ==
			evicted_before,
		"PolarDB classic free profile mismatch: exact-only lookup performs no eviction");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped] == skipped_before,
		"PolarDB classic free profile mismatch: exact-only lookup performs no profile scan");
	reader->ConnectionsFree->remove(stale);
	stale->pgsql_conn = nullptr;
	delete stale;
}

static void test_v2_profile_mismatch_does_not_scan_unrelated_key() {
	const int writer_hg = 996;
	const int reader_hg = 997;
	const uint64_t TARGET_LSN = 0xFEED00;

	stage_polardb_topology(PgHGM, "PolarDB local reader profile mismatch",
		writer_hg, "polardb-local-profile-writer", 23852,
		reader_hg, "polardb-local-profile-reader", 23853);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-local-profile-reader", 23853);
	ok(reader != nullptr,
		"PolarDB local reader profile mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *stale = make_cached_reader_connection(reader);
	ok(stale != nullptr,
		"PolarDB local reader profile mismatch: backend fixture is available");
	if (!stale) {
		return;
	}
	stale->polardb_startup_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::LEGACY);
	stale->polardb_startup_profile_generation =
		stale->polardb_startup_profile.generation(
			static_cast<int>(PolarDB_ProxyIdentityMode::PROXY));
	reader->polardb_current_lsn.store(TARGET_LSN + 0x100, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	unit_reader_pool_add_shared(reader, stale);

	const unsigned long long evicted_before =
		PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed);
	const unsigned long long skipped_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped];

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::STRICT);
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!result.acquired(),
		"PolarDB v2 profile mismatch: unrelated startup-profile key is not acquired");
	ok(PgHGM->status.polardb_rfq_profile_evicted.load(std::memory_order_relaxed) ==
			evicted_before,
		"PolarDB v2 profile mismatch: exact lookup does not evict an unrelated key");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_rfq_profile_skipped] == skipped_before,
		"PolarDB v2 profile mismatch: exact lookup does not scan an unrelated key");
	reader->remove_free_connection(stale);
	delete stale;
}


static void test_no_wait_pooled_reader_requires_startup_identity() {
	const int writer_hg = 976;
	const int reader_hg = 977;

	stage_polardb_topology(PgHGM, "PolarDB no-wait pooled reader",
		writer_hg, "polardb-nowait-writer", 23432,
		reader_hg, "polardb-nowait-reader", 23433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-nowait-reader", 23433);
	ok(reader != nullptr,
		"PolarDB no-wait pooled reader: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *mismatched = make_cached_reader_connection(reader);
	PgSQL_Connection *compatible = make_cached_reader_connection(reader);
	ok(mismatched != nullptr && compatible != nullptr,
		"PolarDB no-wait pooled reader: cached connection fixtures are available");
	if (!mismatched || !compatible) {
		delete mismatched;
		delete compatible;
		return;
	}
	mismatched->pgsql_conn = unit_connected_pgconn();
	compatible->pgsql_conn = unit_connected_pgconn();
	ok(mismatched->pgsql_conn != nullptr && compatible->pgsql_conn != nullptr,
		"PolarDB no-wait pooled reader: libpq fixtures are available");
	mismatched->polardb_startup_client.identity =
		unit_other_proxy_identity();
	compatible->polardb_startup_client.identity =
		unit_proxy_identity();
	unit_reader_pool_add_matching(reader, mismatched);
	unit_reader_pool_add_matching(reader, compatible);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(result.acquired() && result.conn == compatible,
		"PolarDB no-wait pooled reader: compatible identity is selected");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB no-wait pooled reader: incompatible backend stays idle");

	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	if (result.conn != mismatched) {
		reader->ConnectionsFree->remove(mismatched);
	}
	if (result.conn != compatible) {
		reader->ConnectionsFree->remove(compatible);
	}
	delete mismatched;
	delete compatible;
}

static void test_no_wait_pooled_reader_requires_exact_session_state() {
	const int writer_hg = 980;
	const int reader_hg = 981;

	stage_polardb_topology(PgHGM, "PolarDB no-wait pooled exact state",
		writer_hg, "polardb-nowait-exact-writer", 23532,
		reader_hg, "polardb-nowait-exact-reader", 23533);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-nowait-exact-reader", 23533);
	ok(reader != nullptr,
		"PolarDB no-wait pooled exact state: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	PgSQL_Connection *needs_reset = make_cached_reader_connection(reader);
	PgSQL_Connection *exact = make_cached_reader_connection(reader);
	ok(needs_reset != nullptr && exact != nullptr,
		"PolarDB no-wait pooled exact state: cached connection fixtures are available");
	if (!needs_reset || !exact) {
		delete needs_reset;
		delete exact;
		return;
	}

	needs_reset->pgsql_conn = unit_connected_pgconn();
	exact->pgsql_conn = unit_connected_pgconn();
	ok(needs_reset->pgsql_conn != nullptr && exact->pgsql_conn != nullptr,
		"PolarDB no-wait pooled exact state: libpq fixtures are available");
	const int extra_var_idx = PGSQL_NAME_LAST_LOW_WM + 1;
	needs_reset->var_hash[extra_var_idx] = 0x9a51;
	needs_reset->dynamic_variables_idx.push_back(extra_var_idx);
	unit_reader_pool_add_matching(reader, needs_reset);
	unit_reader_pool_add_matching(reader, exact);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult result =
		PgHGM->polardb_acquire_reader_connection(reader_hg, &sess, plan, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(result.acquired() && result.conn == exact,
		"PolarDB no-wait pooled exact state: reset-needed backend is skipped");
	ok(reader->ConnectionsFree->conns_length() == 1 &&
			reader->ConnectionsUsed->conns_length() == 1,
		"PolarDB no-wait pooled exact state: reset-needed backend stays idle");

	if (result.conn && result.srv && result.srv->ConnectionsUsed) {
		result.srv->ConnectionsUsed->remove(result.conn);
	}
	if (result.conn != needs_reset) {
		reader->ConnectionsFree->remove(needs_reset);
	}
	if (result.conn != exact) {
		reader->ConnectionsFree->remove(exact);
	}
	delete needs_reset;
	delete exact;
}

void run_polardb_reader_pool_index_tests() {
	test_reader_pool_request_limits();
	test_core_match_pool_index_and_transfer();
}

void run_polardb_reader_pool_concurrency_tests() {
	test_core_match_pool_concurrent_transfer();
	test_core_match_pool_concurrent_boundaries();
	test_keyless_core_use_restores_exact_match();
}

void run_polardb_reader_pool_maintenance_tests() {
	test_idle_ping_reader_pool_accounting();
	test_reader_pool_idle_trim_grace();
}

void run_polardb_reader_pool_reuse_tests() {
	test_reader_pool_status_and_pooled_only_contract();
	test_reader_pool_capacity_accounting_includes_reuse_pool();
	test_reader_pool_shared_inventory_rejects_unrelated_connection();
	test_reader_pool_shared_prefix_reaches_compatible_backend();
	test_reader_pool_profile_mismatch_drops_backend();
	test_reader_pool_identity_mode_mismatch_drops_backend();
	test_reader_pool_client_identity_mode_not_released_to_shared();
#if POLARDB_PROFILE
	test_reader_pool_shared_transfer_counters();
#endif // POLARDB_PROFILE
	test_reader_pool_worker_local_reuse();
}

void run_polardb_reader_pool_return_tests() {
	test_reader_pool_worker_local_duplicates_last_one_pass();
	test_reader_pool_return_rebuilds_cleared_key();
	test_reader_pool_destroy_used_connection_updates_accounting();
	test_reader_pool_rejected_return_defers_destruction();
	test_reader_pool_batch_rejected_return_defers_destruction();
	test_reader_pool_batch_mixed_parent_detaches_safely();
	test_reader_pool_batch_return_uses_worker_counters();
	test_classic_worker_local_cache_policy();
	test_reader_pool_key_invalidates_on_variable_change();
	test_classic_free_profile_mismatch_drops_backend();
	test_v2_profile_mismatch_does_not_scan_unrelated_key();
}

void run_polardb_reader_pool_pooled_only_tests() {
	test_no_wait_pooled_reader_requires_startup_identity();
	test_no_wait_pooled_reader_requires_exact_session_state();
}

#endif // POLARDB_PROXY
