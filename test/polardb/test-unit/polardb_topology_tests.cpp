/**
 * @file polardb_topology_tests.cpp
 * @brief Topology state, snapshots, and server lifetime tests.
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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

static void test_writer_epoch_change_resets_lsn_caches() {
	const int writer_hg = 910;
	const int reader_hg = 911;

	// Pre-epoch-change LSN cache seed values (arbitrary distinct nonzero LSNs).
	const uint64_t SEED_GROUP_LSN = 0x5000;
	const uint64_t SEED_REPLICA_REPLAY_LSN = 0x4F00;
	const uint64_t SEED_OLD_WRITER_LSN = 0x5100;
	const uint64_t SEED_READER_LSN = 0x5200;
	const unsigned long long SEED_OLD_WRITER_LSN_TS = 101;
	const unsigned long long SEED_READER_LSN_TS = 202;

	stage_polardb_topology(PgHGM, "PolarDB HGM initial",
		writer_hg, "polardb-writer-old", 15432,
		reader_hg, "polardb-reader", 15433);

	const auto writer_cfg = PgHGM->get_polardb_hg_config(writer_hg);
	const auto reader_cfg = PgHGM->get_polardb_hg_config(reader_hg);
	PgHGM->polardb_refresh_thread_snapshots();
	const auto worker_cached_writer_cfg =
		PgHGM->get_thread_cached_polardb_hg_config(writer_hg);
	const PolarDB_WriterScope old_scope{
		writer_cfg.writer_hostgroup, writer_cfg.writer_epoch};
	ok(writer_cfg.is_polardb_hostgroup,
		"PolarDB HGM: writer snapshot lookup finds PolarDB config");
	ok(writer_cfg.writer_hostgroup == writer_hg,
		"PolarDB HGM: writer config preserves writer hostgroup");
	ok(writer_cfg.reader_hostgroup == reader_hg,
		"PolarDB HGM: writer config preserves reader hostgroup");
	ok(reader_cfg.is_polardb_hostgroup &&
			reader_cfg.writer_epoch == writer_cfg.writer_epoch,
		"PolarDB HGM: reader and writer configs use the same writer epoch");
	ok(worker_cached_writer_cfg.writer_epoch == writer_cfg.writer_epoch,
		"PolarDB HGM: publication refresh materializes the writer epoch in the worker cache");
	const auto writer_policy = PgHGM->get_polardb_hg_policy(writer_hg);
	ok(writer_policy.txn_split_enabled == writer_cfg.policy.txn_split_enabled,
		"PolarDB HGM: policy accessor reads from snapshot config");

	PgSQL_HGC *writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	ok(writer_hgc != nullptr && writer_hgc->repl_config.configured,
		"PolarDB HGM: writer replication config loaded");
	ok(reader_hgc != nullptr && reader_hgc->mysrvs->cnt() == 1,
		"PolarDB HGM: reader hostgroup is present");

	PgSQL_SrvC *old_writer =
		find_pgsql_server(writer_hgc, "polardb-writer-old", 15432);
	PgSQL_SrvC *reader = find_pgsql_server(reader_hgc, "polardb-reader", 15433);
	ok(old_writer != nullptr && reader != nullptr,
		"PolarDB HGM: writer and reader server containers are available");

	writer_hgc->repl_config.polardb_group_lsn->store(
		SEED_GROUP_LSN, std::memory_order_relaxed);
	writer_hgc->repl_config.polardb_max_replica_replay_lsn->store(
		SEED_REPLICA_REPLAY_LSN, std::memory_order_relaxed);
	old_writer->polardb_current_lsn.store(SEED_OLD_WRITER_LSN, std::memory_order_relaxed);
	old_writer->lsn_updated_at.store(SEED_OLD_WRITER_LSN_TS, std::memory_order_relaxed);
	reader->polardb_current_lsn.store(SEED_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(SEED_READER_LSN_TS, std::memory_order_relaxed);

	// Re-stage the same replication group with a NEW writer address: this is the
	// writer-identity change under test, so only servers_add + commit (no
	// replication-hostgroups reload) — not the full stage_polardb_topology block.
	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg, "polardb-writer-new", 15432,
			reader_hg, "polardb-reader", 15433)) == 0,
		"PolarDB HGM: changed writer identity staged for commit");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB HGM: changed writer topology commit succeeds");
	writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_SrvC *new_writer =
		find_pgsql_server(writer_hgc, "polardb-writer-new", 15432);
	ok(new_writer != nullptr,
		"PolarDB HGM: new writer server container is available");

	ok(writer_hgc->repl_config.polardb_group_lsn->load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer change clears the group LSN");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer change clears the replica replay maximum");
	ok(writer_hgc->repl_config.polardb_writer_epoch->load(std::memory_order_relaxed) == 1,
		"PolarDB HGM: writer epoch increments after writer identity changes");
	ok(PgHGM->get_thread_cached_polardb_hg_config(writer_hg).writer_epoch == 1,
		"PolarDB HGM: request lookup observes an epoch-only failover before the publication wake");
	PgHGM->polardb_refresh_thread_snapshots();
	ok(PgHGM->get_thread_cached_polardb_hg_config(writer_hg).writer_epoch == 1,
		"PolarDB HGM: publication refresh preserves the live epoch without rebuilding policy configs");
	// Old writer + paired reader LSN caches are cleared: assert the cached LSN and
	// its freshness timestamp as separate invariants for each server.
	ok(old_writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears old writer cached LSN");
	ok(old_writer->lsn_updated_at.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears old writer LSN timestamp");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears paired reader cached LSN");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears paired reader LSN timestamp");

	ok(!PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, 0x5300, old_scope, nullptr),
		"PolarDB HGM: RFQ update from the previous writer epoch is rejected");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0 &&
			reader->lsn_updated_at.load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: rejected RFQ update leaves the reader LSN cache empty");
	ok(writer_hgc->repl_config.polardb_group_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB HGM: rejected RFQ update leaves the group LSN empty");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB HGM: rejected old-epoch RFQ leaves the replica replay maximum empty");

	const auto new_config = PgHGM->get_polardb_hg_config(reader_hg);
	const PolarDB_WriterScope new_scope{
		new_config.writer_hostgroup, new_config.writer_epoch};
	ok(PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, 0x5400, new_scope, nullptr),
		"PolarDB HGM: current-epoch reader RFQ is accepted before role refresh");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB HGM: direct RFQ cannot advance the replica replay maximum");
	ok(PgHGM->polardb_update_server_lsn_from_monitor(
			"polardb-reader", 15433, 0x5500, PolarDB_NodeType::REPLICA),
		"PolarDB HGM: current-epoch replica monitor observation is accepted");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == 0x5500,
		"PolarDB HGM: current-epoch monitor observation restores replica replay evidence");
	ok(PgHGM->get_polardb_max_replica_replay_lsn(new_scope) == 0x5500 &&
			PgHGM->get_polardb_max_replica_replay_lsn(old_scope) == 0,
		"PolarDB HGM: replica replay lookup is bound to the marker's writer epoch");
}

static void test_writer_epoch_change_serializes_rfq_publication() {
	const int writer_hg = 914;
	const int reader_hg = 915;
	const uint64_t old_lsn = 0x5400;

	stage_polardb_topology(PgHGM, "PolarDB RFQ/reset serialization",
		writer_hg, "polardb-race-writer-old", 15532,
		reader_hg, "polardb-race-reader", 15533);

	const auto old_config = PgHGM->get_polardb_hg_config(reader_hg);
	PgSQL_HGC* writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-race-reader", 15533);
	ok(old_config.is_polardb_hostgroup &&
			writer_hgc != nullptr && reader != nullptr,
		"PolarDB RFQ/reset serialization: topology fixture is available");
	if (!old_config.is_polardb_hostgroup || !writer_hgc || !reader) {
		return;
	}

	const PolarDB_WriterScope old_scope{
		old_config.writer_hostgroup,
		old_config.writer_epoch};
	auto server_snapshot = PgHGM->get_polardb_server_list_snapshot();
	ok(server_snapshot != nullptr && old_scope.valid(),
		"PolarDB RFQ/reset serialization: old writer scope and server lifetime are pinned");
	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg, "polardb-race-writer-new", 15532,
			reader_hg, "polardb-race-reader", 15533)) == 0,
		"PolarDB RFQ/reset serialization: changed writer identity is staged");

	std::atomic<bool> update_started{false};
	std::atomic<bool> update_finished{false};
	std::atomic<bool> update_accepted{false};
	std::atomic<bool> commit_started{false};
	std::atomic<bool> commit_finished{false};
	std::atomic<bool> commit_succeeded{false};

	// Hold the reader guard so both operations reach the exact boundary under
	// test. Whichever waiter proceeds first is safe: an old RFQ published first
	// is cleared by the reset; a reset first makes the RFQ fail its epoch check.
	reader->polardb_lock_lsn_cache();
	std::thread updater([&]() {
		update_started.store(true, std::memory_order_release);
		update_accepted.store(
			PgHGM->polardb_accept_rfq_server_lsn(
				reader, reader_hg, old_lsn, old_scope, nullptr),
			std::memory_order_release);
		update_finished.store(true, std::memory_order_release);
	});
	while (!update_started.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	for (unsigned int i = 0; i < 1000; ++i) {
		std::this_thread::yield();
	}
	ok(!update_finished.load(std::memory_order_acquire),
		"PolarDB RFQ/reset serialization: RFQ publication waits while reset owns the cache");

	std::thread committer([&]() {
		commit_started.store(true, std::memory_order_release);
		commit_succeeded.store(
			PgHGM->commit({}, {}, false, false),
			std::memory_order_release);
		commit_finished.store(true, std::memory_order_release);
	});
	while (!commit_started.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	for (unsigned int i = 0; i < 1000; ++i) {
		std::this_thread::yield();
	}
	reader->polardb_unlock_lsn_cache();

	updater.join();
	committer.join();
	ok(update_finished.load(std::memory_order_acquire) &&
			commit_finished.load(std::memory_order_acquire) &&
			commit_succeeded.load(std::memory_order_acquire),
		"PolarDB RFQ/reset serialization: concurrent update and writer commit finish");

	writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	const uint64_t current_epoch =
		writer_hgc && writer_hgc->repl_config.polardb_writer_epoch
			? writer_hgc->repl_config.polardb_writer_epoch->load(
				std::memory_order_acquire)
			: 0;
	ok(current_epoch == old_scope.epoch + 1,
		"PolarDB RFQ/reset serialization: writer commit advances the epoch once");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0 &&
			reader->lsn_updated_at.load(std::memory_order_relaxed) == 0 &&
			writer_hgc &&
			writer_hgc->repl_config.polardb_group_lsn->load(
				std::memory_order_relaxed) == 0,
		"PolarDB RFQ/reset serialization: old RFQ cannot repopulate cleared LSN state");
	diag(
		"PolarDB RFQ/reset serialization: old RFQ %s the reset boundary",
		update_accepted.load(std::memory_order_acquire)
			? "completed before" : "was rejected after");
}

static void test_hostgroup_config_cache_refreshes_after_reload() {
	const int writer_hg = 912;
	const int reader_hg = 913;

	stage_polardb_topology(PgHGM, "PolarDB hostgroup config cache",
		writer_hg, "polardb-config-cache-writer", 16432,
		reader_hg, "polardb-config-cache-reader", 16433);

	const auto first = PgHGM->get_polardb_hg_config(writer_hg);
	const auto repeated = PgHGM->get_polardb_hg_config(writer_hg);
	ok(first.is_polardb_hostgroup && repeated.is_polardb_hostgroup &&
			repeated.writer_epoch == first.writer_epoch,
		"PolarDB hostgroup config cache: repeated reads use the same writer epoch");
	ok(!first.policy.txn_split_enabled,
		"PolarDB hostgroup config cache: initial policy disables transaction split");
	ok(!PgHGM->get_polardb_hg_config(999999).is_polardb_hostgroup,
		"PolarDB hostgroup config cache: repeated missing hostgroup stays missing");

	stage_polardb_topology_with_txn_split(PgHGM,
		"PolarDB hostgroup config cache reload",
		writer_hg, "polardb-config-cache-writer", 16432,
		reader_hg, "polardb-config-cache-reader", 16433);

	const auto refreshed = PgHGM->get_polardb_hg_config(writer_hg);
	ok(refreshed.policy.txn_split_enabled && !first.policy.txn_split_enabled,
		"PolarDB hostgroup config cache: reload refreshes without mutating old copies");
}

static void test_monitor_lsn_update_skips_non_online_servers() {
	const int writer_hg = 930;
	const int reader_hg = 931;
	const uint64_t BLOCKED_LSN = 0x9100;
	const uint64_t ACCEPTED_LSN = 0x9200;
	const uint64_t READER_LSN = 0x9300;
	const uint64_t DIRECT_READER_LSN = 0x9400;
	const uint64_t WRITER_AS_READER_LSN = 0x9500;
	const uint64_t WRITER_AS_READER_RFQ_LSN = 0x9600;

	stage_polardb_topology(PgHGM, "PolarDB monitor LSN check",
		writer_hg, "polardb-monitor-writer", 17432,
		reader_hg, "polardb-monitor-reader", 17433);

	PgSQL_HGC *writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *writer =
		find_pgsql_server(writer_hgc, "polardb-monitor-writer", 17432);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-monitor-reader", 17433);
	ok(writer_hgc != nullptr && writer != nullptr &&
			reader_hgc != nullptr && reader != nullptr,
		"PolarDB monitor LSN check: writer and reader containers are available");
	if (!writer_hgc || !writer || !reader_hgc || !reader) {
		return;
	}

	writer->status = MYSQL_SERVER_STATUS_SHUNNED;
	ok(!PgHGM->polardb_update_server_lsn_from_monitor(
			"polardb-monitor-writer", 17432, BLOCKED_LSN,
			PolarDB_NodeType::PRIMARY),
		"PolarDB monitor LSN check: non-ONLINE server update is rejected");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN check: non-ONLINE server LSN cache stays empty");
	ok(writer_hgc->repl_config.polardb_group_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN check: non-ONLINE server does not update group LSN");

	writer->status = MYSQL_SERVER_STATUS_ONLINE;
	ok(PgHGM->polardb_update_server_lsn_from_monitor(
			"polardb-monitor-writer", 17432, ACCEPTED_LSN,
			PolarDB_NodeType::PRIMARY),
		"PolarDB monitor LSN check: ONLINE server update is accepted");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN check: ONLINE server updates its LSN cache");
	ok(writer_hgc->repl_config.polardb_group_lsn->load(
			std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN check: ONLINE writer updates group LSN");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN check: physical writer does not update replica replay maximum");

	ok(PgHGM->polardb_update_server_lsn_from_monitor(
			"polardb-monitor-reader", 17433, READER_LSN,
			PolarDB_NodeType::REPLICA),
		"PolarDB monitor LSN check: ONLINE reader update is accepted");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: ONLINE reader updates its LSN cache");
	ok(writer_hgc->repl_config.polardb_group_lsn->load(
			std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: ONLINE reader updates shared group LSN");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: physical replica updates replica replay maximum");

	const auto reader_config = PgHGM->get_polardb_hg_config(reader_hg);
	const PolarDB_WriterScope reader_scope{
		reader_config.writer_hostgroup, reader_config.writer_epoch};
	ok(PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, DIRECT_READER_LSN, reader_scope, nullptr),
		"PolarDB monitor LSN check: replica RFQ remains a valid group observation");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: direct RFQ cannot reuse historical monitor role as replay proof");

	ok(PgHGM->polardb_update_server_lsn_from_monitor(
			"polardb-monitor-reader", 17433, WRITER_AS_READER_LSN,
			PolarDB_NodeType::PRIMARY),
		"PolarDB monitor LSN check: primary role on a reader-hostgroup endpoint is accepted");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: writer-as-reader observation cannot advance replica replay maximum");
	ok(PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, WRITER_AS_READER_RFQ_LSN, reader_scope, nullptr),
		"PolarDB monitor LSN check: writer-as-reader RFQ remains a valid group observation");
	ok(writer_hgc->repl_config.polardb_max_replica_replay_lsn->load(
			std::memory_order_relaxed) == READER_LSN,
		"PolarDB monitor LSN check: writer-as-reader RFQ cannot advance replica replay maximum");
}

static void test_lsn_observation_refreshes_freshness_timestamp() {
	const int writer_hg = 940;
	const int reader_hg = 941;
	const uint64_t FIRST_LSN = 0xA100;
	const uint64_t LOWER_LSN = 0xA000;
	const uint64_t FIRST_TS = 100;
	const uint64_t LOWER_TS = 200;
	const uint64_t EQUAL_TS = 300;

	stage_polardb_topology(PgHGM, "PolarDB LSN freshness",
		writer_hg, "polardb-freshness-writer", 18432,
		reader_hg, "polardb-freshness-reader", 18433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-freshness-reader", 18433);
	ok(reader != nullptr,
		"PolarDB LSN freshness: reader server container is available");
	if (!reader) {
		return;
	}

	ok(reader->polardb_advance_lsn(FIRST_LSN, FIRST_TS),
		"PolarDB LSN freshness: first valid sample advances cached LSN");
	ok(!reader->polardb_advance_lsn(LOWER_LSN, LOWER_TS),
		"PolarDB LSN freshness: lower sample is not counted as an advance");
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == FIRST_LSN,
		"PolarDB LSN freshness: lower sample does not lower cached LSN");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == LOWER_TS,
		"PolarDB LSN freshness: lower sample refreshes observation timestamp");
	ok(!reader->polardb_advance_lsn(FIRST_LSN, EQUAL_TS),
		"PolarDB LSN freshness: equal sample is not counted as an advance");
	ok(reader->lsn_updated_at.load(std::memory_order_relaxed) == EQUAL_TS,
		"PolarDB LSN freshness: equal sample refreshes observation timestamp");
	const PolarDB_ReaderLsnSample fresh_sample =
		reader->polardb_sample_lsn(EQUAL_TS + 1000, 1);
	ok(fresh_sample.lsn == FIRST_LSN && fresh_sample.fresh,
		"PolarDB LSN freshness: server sampling returns the cached LSN within the freshness limit");
	const PolarDB_ReaderLsnSample stale_sample =
		reader->polardb_sample_lsn(EQUAL_TS + 1001, 1);
	ok(stale_sample.lsn == FIRST_LSN && !stale_sample.fresh,
		"PolarDB LSN freshness: server sampling marks an older observation stale");
}

static void test_worker_pass_lsn_update() {
	const int writer_hg = 942;
	const int reader_hg = 943;

	stage_polardb_topology(PgHGM, "PolarDB worker pass LSN coalescing",
		writer_hg, "polardb-pass-profile-writer", 19432,
		reader_hg, "polardb-pass-profile-reader", 19433);
	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(writer_hg),
		"polardb-pass-profile-writer", 19432);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-pass-profile-reader", 19433);
	const auto reader_cfg = PgHGM->get_polardb_hg_config(reader_hg);
	ok(writer != nullptr && reader != nullptr &&
			reader_cfg.is_polardb_hostgroup &&
			reader_cfg.writer_hostgroup == writer_hg,
		"PolarDB worker pass profile: servers and reader config are available");
	if (!writer || !reader || !reader_cfg.is_polardb_hostgroup ||
			reader_cfg.writer_hostgroup != writer_hg) {
		return;
	}
	const PolarDB_WriterScope scope{
		reader_cfg.writer_hostgroup,
		reader_cfg.writer_epoch};
	const PolarDB_WriterScope next_scope{scope.hg, scope.epoch + 1};

	PgSQL_Thread worker;
	ok(worker.polardb_track_server_lsn_update(
			reader, 0xB100, scope, 10000),
		"PolarDB worker pass LSN update: outside a pass updates immediately");
#if POLARDB_PROFILE
	worker.polardb_profile_note_pool_used_count_read(writer);
#endif
	worker.polardb_begin_worker_pass();
	ok(worker.polardb_track_server_lsn_update(
			reader, 0xB100, scope, 10000),
		"PolarDB worker pass LSN update: first observation updates the cache");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(true);
#endif
	ok(!worker.polardb_track_server_lsn_update(
			reader, 0xB100, scope, 10001),
		"PolarDB worker pass LSN update: immediate equal repeat is coalesced");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(false);
#endif
	ok(worker.polardb_track_server_lsn_update(
			reader, 0xB200, scope, 10002),
		"PolarDB worker pass LSN update: higher repeat updates immediately");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(true);
#endif
	ok(!worker.polardb_track_server_lsn_update(
			reader, 0xB180, scope, 10003),
		"PolarDB worker pass LSN update: immediate lower repeat is coalesced");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(false);
#endif
	ok(worker.polardb_track_server_lsn_update(
			reader, 0xB180, scope, 11002),
		"PolarDB worker pass LSN update: timestamp refresh is delayed by at most one millisecond");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(false);
#endif
	ok(worker.polardb_track_server_lsn_update(
			reader, 0x100, next_scope, 11003),
		"PolarDB worker pass LSN update: a new writer scope is never hidden by the old scope");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(false);
	worker.polardb_profile_note_pool_used_count_read(writer);
	worker.polardb_profile_note_pool_used_count_read(writer);
	worker.polardb_profile_note_pool_used_count_read(reader);
#endif
	worker.polardb_finish_worker_pass();

#if POLARDB_PROFILE
	const auto& counters = worker.polardb_status_variables.stvar;
	ok(counters[polardb_st_var_lsn_update_call] == 6 &&
			counters[polardb_st_var_lsn_update_advance] == 2 &&
			counters[polardb_st_var_lsn_update_refresh_only] == 2 &&
			counters[polardb_st_var_lsn_update_shared_update] == 4 &&
			counters[polardb_st_var_lsn_update_coalesced] == 2,
		"PolarDB worker pass profile: LSN calls split into shared and coalesced updates");
	ok(counters[polardb_st_var_lsn_update_pass_server] == 2 &&
			counters[polardb_st_var_lsn_update_pass_repeat] == 4 &&
			counters[polardb_st_var_lsn_update_pass_overflow] == 0,
		"PolarDB worker pass profile: repeated server and scope observations are counted");
	ok(counters[polardb_st_var_reader_pool_used_count_read] == 3 &&
			counters[polardb_st_var_reader_pool_used_count_pass_server] == 2 &&
			counters[polardb_st_var_reader_pool_used_count_pass_repeat] == 1 &&
			counters[polardb_st_var_reader_pool_used_count_pass_overflow] == 0,
		"PolarDB worker pass profile: repeated used-count reads are counted");

	worker.polardb_finish_worker_pass();
	worker.polardb_profile_note_pool_used_count_read(writer);
	ok(counters[polardb_st_var_lsn_update_call] == 6 &&
			counters[polardb_st_var_reader_pool_used_count_read] == 3,
		"PolarDB worker pass profile: inactive and repeated finish calls add nothing");
#endif // POLARDB_PROFILE

	worker.polardb_begin_worker_pass();
	ok(worker.polardb_track_server_lsn_update(
			reader, 0x100, next_scope, 12000),
		"PolarDB worker pass LSN update: a new pass applies its first observation");
#if POLARDB_PROFILE
	worker.polardb_profile_note_lsn_update(false);
	worker.polardb_profile_note_pool_used_count_read(writer);
#endif // POLARDB_PROFILE
	worker.polardb_finish_worker_pass();
#if POLARDB_PROFILE
	ok(counters[polardb_st_var_lsn_update_call] == 7 &&
			counters[polardb_st_var_lsn_update_advance] == 2 &&
			counters[polardb_st_var_lsn_update_refresh_only] == 3 &&
			counters[polardb_st_var_lsn_update_shared_update] == 5 &&
			counters[polardb_st_var_lsn_update_coalesced] == 2 &&
			counters[polardb_st_var_lsn_update_pass_server] == 3 &&
			counters[polardb_st_var_lsn_update_pass_repeat] == 4 &&
			counters[polardb_st_var_reader_pool_used_count_read] == 4 &&
			counters[polardb_st_var_reader_pool_used_count_pass_server] == 3 &&
			counters[polardb_st_var_reader_pool_used_count_pass_repeat] == 1,
		"PolarDB worker pass profile: a new pass resets server tracking");
#endif // POLARDB_PROFILE

	reader->polardb_current_lsn.store(0, std::memory_order_relaxed);
	reader->lsn_updated_at.store(0, std::memory_order_relaxed);
	PgSQL_Thread integration_worker;
	integration_worker.polardb_begin_worker_pass();
	ok(PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, 0xB100, scope,
			&integration_worker) &&
			reader->polardb_current_lsn.load(std::memory_order_relaxed) ==
				0xB100,
		"PolarDB worker pass LSN update: hostgroup update applies the first observation");
	ok(PgHGM->polardb_accept_rfq_server_lsn(
			reader, reader_hg, 0xB200, scope,
			&integration_worker) &&
			reader->polardb_current_lsn.load(std::memory_order_relaxed) ==
				0xB200,
		"PolarDB worker pass LSN update: hostgroup update applies a higher repeat");
	integration_worker.polardb_finish_worker_pass();

	PgSQL_Thread overflow_worker;
	std::vector<std::unique_ptr<PgSQL_SrvC>> overflow_servers;
	overflow_worker.polardb_begin_worker_pass();
	for (unsigned int i = 0; i < 17; i++) {
		const std::string address =
			"polardb-pass-overflow-" + std::to_string(i);
		overflow_servers.emplace_back(new PgSQL_SrvC(
			const_cast<char*>(address.c_str()), 20000 + i, 1,
			MYSQL_SERVER_STATUS_ONLINE, 0, 10, 0, 0, 0,
			const_cast<char*>("")));
		ok(overflow_worker.polardb_track_server_lsn_update(
				overflow_servers.back().get(), 0x100 + i, scope,
				20000 + i),
			"PolarDB worker pass LSN update: identity %u is updated",
			i + 1);
#if POLARDB_PROFILE
		overflow_worker.polardb_profile_note_lsn_update(false);
		overflow_worker.polardb_profile_note_pool_used_count_read(
			overflow_servers.back().get());
#endif // POLARDB_PROFILE
	}
	overflow_worker.polardb_finish_worker_pass();
	const auto& overflow_counters =
		overflow_worker.polardb_status_variables.stvar;
	ok(overflow_counters[polardb_st_var_lsn_update_pass_overflow] == 1,
		"PolarDB worker pass LSN update: overflow is visible in release and profile builds");
#if POLARDB_PROFILE
	ok(overflow_counters[polardb_st_var_lsn_update_call] == 17 &&
			overflow_counters[polardb_st_var_lsn_update_shared_update] == 17 &&
			overflow_counters[polardb_st_var_lsn_update_refresh_only] == 17 &&
			overflow_counters[polardb_st_var_lsn_update_coalesced] == 0 &&
			overflow_counters[polardb_st_var_lsn_update_pass_server] == 16 &&
			overflow_counters[polardb_st_var_lsn_update_pass_repeat] == 0 &&
			overflow_counters[polardb_st_var_lsn_update_pass_overflow] == 1 &&
			overflow_counters[polardb_st_var_reader_pool_used_count_read] == 17 &&
			overflow_counters[polardb_st_var_reader_pool_used_count_pass_server] == 16 &&
			overflow_counters[polardb_st_var_reader_pool_used_count_pass_repeat] == 0 &&
			overflow_counters[polardb_st_var_reader_pool_used_count_pass_overflow] == 1,
		"PolarDB worker pass profile: both pass trackers preserve overflow arithmetic");
#endif // POLARDB_PROFILE
}


static bool unit_snapshot_contains_server(
		const std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>& snapshot,
		unsigned int hostgroup_id,
		PgSQL_SrvC* server) {
	if (!snapshot || !server) {
		return false;
	}
	auto it = snapshot->by_hostgroup.find(hostgroup_id);
	if (it == snapshot->by_hostgroup.end()) {
		return false;
	}
	for (const auto& candidate : it->second.servers) {
		if (candidate.srv == server) {
			return true;
		}
	}
	return false;
}

static SQLite3_result *make_pgsql_servers_result_single(
		int hostgroup_id, const char *addr, int port) {
	SQLite3_result *result = new SQLite3_result(11);
	char hostgroup_buf[16];
	char port_buf[16];
	snprintf(hostgroup_buf, sizeof(hostgroup_buf), "%d", hostgroup_id);
	snprintf(port_buf, sizeof(port_buf), "%d", port);

	char *row[] = {
		hostgroup_buf,
		(char*)addr,
		port_buf,
		(char*)"ONLINE",
		(char*)"1",
		(char*)"0",
		(char*)"50",
		(char*)"0",
		(char*)"0",
		(char*)"0",
		(char*)"polardb server-list snapshot unit"
	};
	result->add_row(row);
	return result;
}

static SQLite3_result *make_pgsql_servers_result_with_reader_options(
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_addr, int reader_port,
		int64_t reader_weight, int64_t reader_max_connections,
		unsigned int reader_max_latency_ms) {
	SQLite3_result *result = new SQLite3_result(11);
	char writer_hg_buf[16];
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char reader_port_buf[16];
	char reader_weight_buf[32];
	char reader_max_connections_buf[32];
	char reader_max_latency_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", reader_port);
	snprintf(reader_weight_buf, sizeof(reader_weight_buf), "%ld",
		static_cast<long>(reader_weight));
	snprintf(reader_max_connections_buf, sizeof(reader_max_connections_buf),
		"%ld", static_cast<long>(reader_max_connections));
	snprintf(reader_max_latency_buf, sizeof(reader_max_latency_buf), "%u",
		reader_max_latency_ms);

	char *writer_row[] = {
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
		(char*)"polardb selector snapshot writer"
	};
	result->add_row(writer_row);

	char *reader_row[] = {
		reader_hg_buf,
		(char*)reader_addr,
		reader_port_buf,
		(char*)"ONLINE",
		reader_weight_buf,
		(char*)"0",
		reader_max_connections_buf,
		(char*)"0",
		(char*)"0",
		reader_max_latency_buf,
		(char*)"polardb selector snapshot reader"
	};
	result->add_row(reader_row);
	return result;
}

static const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry*
unit_snapshot_server_entry(
		const std::shared_ptr<const PgSQL_HostGroups_Manager::
			PolarDB_ServerListSnapshot>& snapshot,
		unsigned int hostgroup_id,
		PgSQL_SrvC* server) {
	if (!snapshot || !server) {
		return nullptr;
	}
	auto hostgroup = snapshot->by_hostgroup.find(hostgroup_id);
	if (hostgroup == snapshot->by_hostgroup.end()) {
		return nullptr;
	}
	for (const auto& entry : hostgroup->second.servers) {
		if (entry.srv == server) {
			return &entry;
		}
	}
	return nullptr;
}

static bool unit_snapshot_server_options_equal(
		const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry* entry,
		int64_t weight, int64_t max_connections,
		unsigned int max_latency_us) {
	return entry && entry->weight == weight &&
		entry->max_connections == max_connections &&
		entry->max_latency_us == max_latency_us;
}

static void test_server_selection_snapshot_refresh_and_immutability() {
	const int writer_hg = 934;
	const int reader_hg = 935;
	const char *writer_addr = "polardb-selector-snapshot-writer";
	const char *reader_addr = "polardb-selector-snapshot-reader";
	const int writer_port = 17442;
	const int reader_port = 17443;

	stage_polardb_topology(PgHGM, "PolarDB selector snapshot",
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), reader_addr, reader_port);
	auto before = PgHGM->get_polardb_server_list_snapshot();
	const auto *before_entry =
		unit_snapshot_server_entry(before, reader_hg, reader);
	ok(unit_snapshot_server_options_equal(before_entry, 1, 50, 0),
		"PolarDB selector snapshot: initial server options are captured");

	ok(PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port,
			7, 160, 200)) == 0,
		"PolarDB selector snapshot: updated server options are staged");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB selector snapshot: updated server options are committed");
	auto after = PgHGM->get_polardb_server_list_snapshot();
	const auto *after_entry =
		unit_snapshot_server_entry(after, reader_hg, reader);
	ok(before && after && after->generation > before->generation &&
			unit_snapshot_server_options_equal(after_entry, 7, 160, 200000),
		"PolarDB selector snapshot: reload installs a new complete option set");
	ok(unit_snapshot_server_options_equal(before_entry, 1, 50, 0),
		"PolarDB selector snapshot: retained snapshot keeps its original options");

	PolarDB_Query_ReaderPlan plan;
	PolarDB_WaitSpec no_wait;
	reader->set_current_latency_us_value(150000);
	ok(PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, reader, plan, no_wait),
		"PolarDB reader eligibility: current latency within the snapshot limit remains eligible");
	reader->set_current_latency_us_value(250000);
	ok(!PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, reader, plan, no_wait),
		"PolarDB reader eligibility: current latency above the snapshot limit is rejected");
	reader->set_current_latency_us_value(0);
}

static void test_replica_requirement_rejects_unknown_writer() {
	const int writer_hg = 938;
	const int reader_hg = 939;
	const char* writer_addr = "polardb-replica-check-writer";
	const char* reader_addr = "polardb-replica-check-reader";
	const int writer_port = 17462;
	const int reader_port = 17463;

	stage_polardb_topology(PgHGM, "PolarDB replica requirement",
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), reader_addr, reader_port);
	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(writer_hg), writer_addr, writer_port);
	PolarDB_Query_ReaderPlan replica_only;
	replica_only.require_replica = true;
	PolarDB_WaitSpec no_wait;
	ok(reader && writer &&
			PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, reader, replica_only, no_wait),
		"PolarDB replica requirement: known reader is eligible");
	if (!reader || !writer) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	PolarDB_ReaderResult known_writer =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, replica_only, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(known_writer.acquired() && known_writer.conn == conn,
		"PolarDB replica requirement: acquisition accepts a known reader");
	if (known_writer.conn) {
		PgHGM->push_MyConn_to_pool(known_writer.conn);
	}

	writer->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	PolarDB_ReaderResult offline_writer =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, replica_only, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!offline_writer.acquired() &&
			offline_writer.status == PolarDB_ReaderStatus::READER_UNAVAILABLE,
		"PolarDB replica requirement: acquisition rejects an all-offline writer list");
	writer->set_status(MYSQL_SERVER_STATUS_ONLINE);

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			reader_hg, reader_addr, reader_port)) == 0,
		"PolarDB replica requirement: reader-only server table is staged");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB replica requirement: unavailable writer is committed");
	ok(!PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, reader, replica_only, no_wait),
		"PolarDB replica requirement: unknown writer blocks replica-only routing");
	PolarDB_ReaderResult missing_writer =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, replica_only, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!missing_writer.acquired() &&
			missing_writer.status == PolarDB_ReaderStatus::READER_UNAVAILABLE,
		"PolarDB replica requirement: acquisition rejects a missing writer");

	PolarDB_Query_ReaderPlan ordinary;
	ok(PgHGM->polardb_reader_server_can_serve_request(
			reader_hg, reader, ordinary, no_wait),
		"PolarDB replica requirement: ordinary reader routing is unchanged");
	PolarDB_ReaderResult ordinary_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, ordinary, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(ordinary_result.acquired() && ordinary_result.conn == conn,
		"PolarDB replica requirement: rejected requests leave the reader connection available");
	if (ordinary_result.conn) {
		PgHGM->push_MyConn_to_pool(ordinary_result.conn);
	}
	reader->remove_free_connection(conn);
	delete conn;
}

static void test_reader_selection_reload_concurrency() {
	const int writer_hg = 936;
	const int reader_hg = 937;
	const char *writer_addr = "polardb-selector-race-writer";
	const char *reader_addr = "polardb-selector-race-reader";
	const int writer_port = 17452;
	const int reader_port = 17453;

	stage_polardb_topology(PgHGM, "PolarDB selector reload",
		writer_hg, writer_addr, writer_port,
		reader_hg, reader_addr, reader_port);
	ok(PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr, reader_port,
			3, 80, 100)) == 0,
		"PolarDB selector reload: initial options are staged");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB selector reload: initial options are committed");

	PgSQL_SrvC *reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), reader_addr, reader_port);
	ok(reader != nullptr,
		"PolarDB selector reload: reader server container is available");
	if (!reader) {
		return;
	}
	PgSQL_Connection *conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);

	std::atomic<bool> stop{false};
	std::atomic<unsigned int> reload_failures{0};
	std::atomic<unsigned int> mixed_snapshots{0};
	std::atomic<unsigned long long> acquisitions{0};
	std::atomic<unsigned long long> latency_updates{0};

	std::thread selector([&]() {
		std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
		PgSQL_Session sess;
		attach_test_frontend(sess, worker.get());
		PolarDB_Query_ReaderPlan plan;
		plan.fallback_writer_hg = writer_hg;
		PolarDB_WaitSpec no_wait;
		while (!stop.load(std::memory_order_acquire)) {
			PolarDB_ReaderResult result = PgHGM->polardb_acquire_reader_connection(
				reader_hg, &sess, plan, no_wait,
				PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
			if (!result.acquired()) {
				std::this_thread::yield();
				continue;
			}
			auto selected_snapshot = std::static_pointer_cast<const
				PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>(
					result.conn->polardb_selected_server_snapshot);
			const auto *entry = unit_snapshot_server_entry(
				selected_snapshot, reader_hg, result.srv);
			const bool options_a = unit_snapshot_server_options_equal(
				entry, 3, 80, 100000);
			const bool options_b = unit_snapshot_server_options_equal(
				entry, 7, 160, 200000);
			if (!options_a && !options_b) {
				mixed_snapshots.fetch_add(1, std::memory_order_relaxed);
			}
			PgHGM->push_MyConn_to_pool(result.conn);
			acquisitions.fetch_add(1, std::memory_order_relaxed);
		}
	});

	std::thread monitor([&]() {
		unsigned int latency_us = 25;
		while (!stop.load(std::memory_order_acquire)) {
			PgHGM->set_server_current_latency_us(
				(char*)reader_addr, reader_port, latency_us);
			latency_us = latency_us == 25 ? 50 : 25;
			latency_updates.fetch_add(1, std::memory_order_relaxed);
		}
	});

	for (unsigned int iteration = 0; iteration < 40; iteration++) {
		const bool use_options_b = (iteration & 1U) != 0;
		if (PgHGM->servers_add(make_pgsql_servers_result_with_reader_options(
				writer_hg, writer_addr, writer_port,
				reader_hg, reader_addr, reader_port,
				use_options_b ? 7 : 3,
				use_options_b ? 160 : 80,
				use_options_b ? 200 : 100)) != 0 ||
				!PgHGM->commit({}, {}, false, false)) {
			reload_failures.fetch_add(1, std::memory_order_relaxed);
			break;
		}
	}
	stop.store(true, std::memory_order_release);
	selector.join();
	monitor.join();

	ok(reload_failures.load(std::memory_order_relaxed) == 0,
		"PolarDB selector reload: concurrent configuration reloads complete");
	ok(acquisitions.load(std::memory_order_relaxed) > 0 &&
			latency_updates.load(std::memory_order_relaxed) > 0,
		"PolarDB selector reload: selection and latency updates overlap reloads");
	ok(mixed_snapshots.load(std::memory_order_relaxed) == 0,
		"PolarDB selector reload: selection sees only complete option sets");

	reader->remove_free_connection(conn);
	delete conn;
}

static void test_server_list_snapshot_survives_topology_purge() {
	const int writer_hg = 930;
	const int reader_hg = 931;

	stage_polardb_topology(PgHGM, "PolarDB server-list snapshot",
		writer_hg, "polardb-snapshot-writer", 17432,
		reader_hg, "polardb-snapshot-reader", 17433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-snapshot-reader", 17433);
	auto before = PgHGM->get_polardb_server_list_snapshot();
	ok(reader != nullptr && unit_snapshot_contains_server(
			before, reader_hg, reader),
		"PolarDB server-list snapshot: current list contains staged reader");

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-snapshot-writer", 17432)) == 0,
		"PolarDB server-list snapshot: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB server-list snapshot: missing reader marked offline");
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-snapshot-writer", 17432)) == 0,
		"PolarDB server-list snapshot: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB server-list snapshot: offline reader purged");

	auto after = PgHGM->get_polardb_server_list_snapshot();
	ok(!unit_snapshot_contains_server(after, reader_hg, reader),
		"PolarDB server-list snapshot: new list excludes purged reader");
	ok(unit_snapshot_contains_server(before, reader_hg, reader),
		"PolarDB server-list snapshot: old list remains readable during scan");
}

static void test_selected_server_survives_concurrent_purge() {
	const int writer_hg = 910;
	const int reader_hg = 911;
	stage_polardb_topology(PgHGM, "PolarDB selected-server lifetime",
		writer_hg, "polardb-lifetime-writer", 22372,
		reader_hg, "polardb-lifetime-reader", 22373);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-lifetime-reader", 22373);
	ok(reader != nullptr,
		"PolarDB selected-server lifetime: selected server fixture is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, conn);
	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult selected = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &sess, plan, no_wait,
		PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(selected.acquired() &&
			selected.conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB selected-server lifetime: acquired connection retains its server snapshot");
	if (!selected.acquired()) {
		return;
	}

	std::atomic<bool> allow_return{false};
	std::atomic<bool> returned{false};
	std::thread returner([&]() {
		while (!allow_return.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
		PgHGM->push_MyConn_to_pool(selected.conn);
		returned.store(true, std::memory_order_release);
	});

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-lifetime-writer", 22372)) == 0,
		"PolarDB selected-server lifetime: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	(void)PgHGM->get_polardb_server_list_snapshot();
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-lifetime-writer", 22372)) == 0,
		"PolarDB selected-server lifetime: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	ok(selected.conn->parent == reader && reader->address &&
			strcmp(reader->address, "polardb-lifetime-reader") == 0,
		"PolarDB selected-server lifetime: retired server remains valid until return");
	allow_return.store(true, std::memory_order_release);
	returner.join();
	ok(returned.load(std::memory_order_acquire),
		"PolarDB selected-server lifetime: return completes after concurrent purge");
}

static void test_idle_ping_connection_survives_topology_purge() {
	const int writer_hg = 998;
	const int reader_hg = 999;
	stage_polardb_topology(PgHGM, "PolarDB idle-ping server lifetime",
		writer_hg, "polardb-ping-lifetime-writer", 22376,
		reader_hg, "polardb-ping-lifetime-reader", 22377);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-ping-lifetime-reader", 22377);
	ok(reader != nullptr,
		"PolarDB idle-ping lifetime: selected server fixture is available");
	if (!reader) {
		return;
	}
	reader->max_connections = 1000;
	const bool old_attributes_configured = reader->myhgc->attributes.configured;
	const int old_free_connections_pct =
		reader->myhgc->attributes.free_connections_pct;
	reader->myhgc->attributes.configured = true;
	reader->myhgc->attributes.free_connections_pct = 100;

	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	conn->creation_time = monotonic_time();
	conn->last_time_used = 1;
	const PgSQL_PoolMatchKey key = unit_reader_pool_match_key(conn);
	assert(reader->add_matching_connection(conn, key));
	PgSQL_Connection* extracted[1] = {};
	const int extracted_count = PgHGM->get_multiple_idle_connections(
		reader_hg, UINT64_MAX, extracted, 1);
	reader->myhgc->attributes.configured = old_attributes_configured;
	reader->myhgc->attributes.free_connections_pct = old_free_connections_pct;
	ok(extracted_count == 1 && extracted[0] == conn &&
			conn->polardb_selected_server_snapshot != nullptr,
		"PolarDB idle-ping lifetime: extraction retains the current server snapshot");
	if (extracted_count != 1 || extracted[0] != conn) {
		reader->remove_free_connection(conn);
		delete conn;
		return;
	}

	PgSQL_PoolMatchKey used_key;
	ok(reader->used_connection_match_key(conn, &used_key) &&
			used_key == key,
		"PolarDB idle-ping lifetime: extraction preserves the exact match key");
	ok(reader->remove_used_connection(conn),
		"PolarDB idle-ping lifetime: external ping connection can leave core USED for purge test");

	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-ping-lifetime-writer", 22376)) == 0,
		"PolarDB idle-ping lifetime: writer-only table staged for removal");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	(void)PgHGM->get_polardb_server_list_snapshot();
	ok(PgHGM->servers_add(make_pgsql_servers_result_single(
			writer_hg, "polardb-ping-lifetime-writer", 22376)) == 0,
		"PolarDB idle-ping lifetime: writer-only table staged for purge");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	assert(PgHGM->commit({}, {}, false, false));
	ok(conn->parent == reader && reader->address &&
			strcmp(reader->address, "polardb-ping-lifetime-reader") == 0,
		"PolarDB idle-ping lifetime: retired server remains valid until ping connection destruction");
	delete conn;
}


static void test_hgm_shutdown_releases_server_snapshots() {
	auto current_snapshot = PgHGM->get_polardb_server_list_snapshot();
	std::weak_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		current_snapshot_reference = current_snapshot;
	ok(current_snapshot != nullptr,
		"PolarDB shutdown: current server snapshot exists before cleanup");
	current_snapshot.reset();

	PgHGM->shutdown();
	ok(PgHGM->get_polardb_server_list_snapshot() == nullptr,
		"PolarDB shutdown: current server snapshot is empty after cleanup");
	ok(current_snapshot_reference.expired(),
		"PolarDB shutdown: calling-thread snapshot cache releases the final generation");
	ok(!PgHGM->is_polardb_hostgroup(1115),
		"PolarDB shutdown: topology access returns no server after cleanup");

	PgHGM->shutdown();
	ok(PgHGM->get_polardb_server_list_snapshot() == nullptr,
		"PolarDB shutdown: repeated snapshot cleanup is idempotent");
}

void run_polardb_topology_state_tests() {
	test_writer_epoch_change_resets_lsn_caches();
	test_writer_epoch_change_serializes_rfq_publication();
	test_hostgroup_config_cache_refreshes_after_reload();
}

void run_polardb_topology_lsn_tests() {
	test_monitor_lsn_update_skips_non_online_servers();
	test_lsn_observation_refreshes_freshness_timestamp();
	test_worker_pass_lsn_update();
}

void run_polardb_topology_snapshot_tests() {
	test_server_selection_snapshot_refresh_and_immutability();
	test_replica_requirement_rejects_unknown_writer();
	test_reader_selection_reload_concurrency();
	test_server_list_snapshot_survives_topology_purge();
}

void run_polardb_topology_connection_lifetime_tests() {
	test_selected_server_survives_concurrent_purge();
	test_idle_ping_connection_survives_topology_purge();
}

void run_polardb_topology_shutdown_tests() {
	test_hgm_shutdown_releases_server_snapshots();
}

#endif // POLARDB_PROXY
