/**
 * @file polardb_query_digest_unit-t.cpp
 * @brief Unit tests for worker-local PostgreSQL query digest aggregation.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Query_Processor.h"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

extern PgSQL_Query_Processor* GloPgQPro;

static const char* single_digest_field(SQLite3_result* result, int column) {
	if (!result || result->rows_count != 1 || result->rows.empty() ||
			column >= result->rows[0]->cnt) {
		return nullptr;
	}
	return result->rows[0]->fields[column];
}

static uint64_t digest_count(SQLite3_result* result) {
	uint64_t count = 0;
	if (!result) {
		return count;
	}
	for (const auto* row : result->rows) {
		if (row->cnt > 5 && row->fields[5]) {
			count += strtoull(row->fields[5], nullptr, 10);
		}
	}
	return count;
}

static void test_digest_merge() {
	QP_query_digest_stats total(
		"user", "db", 1, "SELECT 1", 20, "127.0.0.1", 2048);
	QP_query_digest_stats batch(
		"user", "db", 1, "SELECT 1", 20, "127.0.0.1", 2048);

	total.add_time(30, 200, 2, 3);
	total.add_time(10, 300, 4, 5);
	batch.add_time(20, 100, 6, 7);
	batch.add_time(50, 400, 8, 9);
	total.merge(batch);

	ok(total.count_star == 4, "digest merge preserves count");
	ok(total.sum_time == 110, "digest merge preserves total time");
	ok(total.min_time == 10, "digest merge preserves minimum time");
	ok(total.max_time == 50, "digest merge preserves maximum time");
	ok(total.first_seen == 100, "digest merge preserves earliest timestamp");
	ok(total.last_seen == 400, "digest merge preserves latest timestamp");
	ok(total.rows_affected == 20, "digest merge preserves affected rows");
	ok(total.rows_sent == 24, "digest merge preserves sent rows");
}

static void test_thread_digests() {
	pgsql_thread___query_digests_normalize_digest_text = false;
	pgsql_thread___query_digests_max_digest_length = 2048;
	GloPgQPro->purge_query_digests(false, false, nullptr);
	GloPgQPro->init_thread();

	PgSQL_Connection_userinfo ui;
	ui.set((char*)"worker", (char*)"password", (char*)"db", nullptr);
	char first_text[] = "SELECT first";
	GloPgQPro->update_query_digest(
		0x1001, 0x2001, first_text, 20, &ui, 40, 1000,
		"127.0.0.1", 2, 3);
	GloPgQPro->update_query_digest(
		0x1001, 0x2001, first_text, 20, &ui, 10, 1100,
		"127.0.0.1", 4, 5);

	ok(GloPgQPro->get_query_digests_total_size() > 0,
		"digest memory accounting includes worker-local samples");
	SQLite3_result* result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 1,
		"admin snapshot merges pending worker digests");
	ok(single_digest_field(result, 4) &&
			strcmp(single_digest_field(result, 4), first_text) == 0,
		"admin snapshot preserves digest text");
	ok(single_digest_field(result, 5) &&
			strcmp(single_digest_field(result, 5), "2") == 0,
		"admin snapshot preserves count");
	ok(single_digest_field(result, 8) &&
			strcmp(single_digest_field(result, 8), "50") == 0,
		"admin snapshot preserves total time");
	ok(single_digest_field(result, 9) &&
			strcmp(single_digest_field(result, 9), "10") == 0,
		"admin snapshot preserves minimum time");
	ok(single_digest_field(result, 10) &&
			strcmp(single_digest_field(result, 10), "40") == 0,
		"admin snapshot preserves maximum time");
	ok(single_digest_field(result, 12) &&
			strcmp(single_digest_field(result, 12), "6") == 0,
		"admin snapshot preserves affected rows");
	ok(single_digest_field(result, 13) &&
			strcmp(single_digest_field(result, 13), "8") == 0,
		"admin snapshot preserves sent rows");
	delete result;

	ok(GloPgQPro->purge_query_digests(false, false, nullptr) == 1,
		"digest reset removes the stored digest");

	pgsql_thread___query_digests_normalize_digest_text = true;
	char normalized_text[] = "SELECT ?";
	GloPgQPro->update_query_digest(
		0x1002, 0x2002, normalized_text, 20, &ui, 25, 1200,
		"127.0.0.1", 0, 1);
	auto snapshot = GloPgQPro->get_query_digests_v2(true);
	result = snapshot.first;
	ok(result && result->rows_count == 1,
		"unlocked admin snapshot merges a pending normalized digest");
	ok(single_digest_field(result, 4) &&
			strcmp(single_digest_field(result, 4), normalized_text) == 0,
		"unlocked admin snapshot preserves normalized digest text");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);

	char topk_text[] = "SELECT topk";
	GloPgQPro->update_query_digest(
		0x1006, 0x2006, topk_text, 20, &ui, 20, 1250,
		"127.0.0.1", 0, 1);
	query_digest_filter_opts_t no_filters;
	auto topk = GloPgQPro->get_query_digests_topk(
		no_filters, query_digest_sort_by_t::count_star, 1, 0, 1);
	ok(topk.matched_count == 1 && topk.rows.size() == 1 &&
			topk.rows[0].digest_text == topk_text,
		"top-k snapshot merges pending worker digests");
	GloPgQPro->purge_query_digests(false, false, nullptr);

	char stale_text[] = "SELECT stale";
	char current_text[] = "SELECT current";
	GloPgQPro->update_query_digest(
		0x1003, 0x2003, stale_text, 20, &ui, 30, 1300,
		"127.0.0.1", 0, 1);
	auto reset = GloPgQPro->get_query_digests_reset_v2(false);
	ok(reset.first && reset.first->rows_count == 1,
		"digest reset includes samples still pending in a worker");
	ok(single_digest_field(reset.first, 4) &&
			strcmp(single_digest_field(reset.first, 4), stale_text) == 0,
		"digest reset preserves pending worker samples");
	delete reset.first;
	GloPgQPro->update_query_digest(
		0x1004, 0x2004, current_text, 20, &ui, 35, 1400,
		"127.0.0.1", 0, 1);
	GloPgQPro->update_query_processor_stats();
	result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 1,
		"digest reset prevents pending old samples from reappearing");
	ok(single_digest_field(result, 4) &&
			strcmp(single_digest_field(result, 4), current_text) == 0,
		"digest reset keeps only post-reset samples");
	ok(single_digest_field(result, 5) &&
			strcmp(single_digest_field(result, 5), "1") == 0,
		"digest reset keeps the post-reset count exact");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);

	pgsql_thread___query_digests_normalize_digest_text = false;
	char bounded_text[] = "SELECT bounded";
	for (uint64_t i = 0; i < 255; ++i) {
		GloPgQPro->update_query_digest(
			0x2000 + i, 0x3000 + i, bounded_text, 20, &ui, 1, 1500 + i,
			"127.0.0.1", 0, 1);
	}
	result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 255,
		"admin snapshot merges a worker map below its entry limit");
	delete result;
	GloPgQPro->update_query_digest(
		0x20ff, 0x30ff, bounded_text, 20, &ui, 1, 1755,
		"127.0.0.1", 0, 1);
	result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 256,
		"later snapshots merge new worker digests with stored digests");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);

	char shutdown_text[] = "SELECT shutdown";
	GloPgQPro->update_query_digest(
		0x1005, 0x2005, shutdown_text, 20, &ui, 45, 1500,
		"127.0.0.1", 0, 1);
	GloPgQPro->end_thread();
	result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 1,
		"worker shutdown merges pending digest samples");
	ok(single_digest_field(result, 4) &&
			strcmp(single_digest_field(result, 4), shutdown_text) == 0,
		"worker shutdown preserves the pending digest");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);
}

static void test_concurrent_thread_merge() {
	constexpr int worker_count = 4;
	constexpr int updates_per_worker = 1000;
	std::vector<std::thread> workers;
	workers.reserve(worker_count);

	for (int worker = 0; worker < worker_count; ++worker) {
		workers.emplace_back([]() {
			pgsql_thread___query_digests_normalize_digest_text = false;
			pgsql_thread___query_digests_max_digest_length = 2048;
			GloPgQPro->init_thread();
			PgSQL_Connection_userinfo ui;
			ui.set((char*)"worker", (char*)"password", (char*)"db", nullptr);
			char digest_text[] = "SELECT concurrent";
			for (int i = 0; i < updates_per_worker; ++i) {
				GloPgQPro->update_query_digest(
					0x4001, 0x5001, digest_text, 20, &ui, 2, 2000 + i,
					"127.0.0.1", 0, 1);
			}
			GloPgQPro->end_thread();
		});
	}
	for (auto& worker : workers) {
		worker.join();
	}

	SQLite3_result* result = GloPgQPro->get_query_digests();
	ok(result && result->rows_count == 1,
		"concurrent worker merges produce one digest");
	ok(single_digest_field(result, 4) &&
			strcmp(single_digest_field(result, 4), "SELECT concurrent") == 0,
		"concurrent worker merges preserve digest text");
	ok(single_digest_field(result, 5) &&
			strcmp(single_digest_field(result, 5), "4000") == 0,
		"concurrent worker merges preserve count");
	ok(single_digest_field(result, 8) &&
			strcmp(single_digest_field(result, 8), "8000") == 0,
		"concurrent worker merges preserve total time");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);
}

static void test_concurrent_snapshot() {
	constexpr int worker_count = 4;
	constexpr int updates_per_worker = 10000;
	std::atomic<int> ready{0};
	std::atomic<int> finished{0};
	std::atomic<bool> start{false};
	std::vector<std::thread> workers;
	workers.reserve(worker_count);

	for (int worker = 0; worker < worker_count; ++worker) {
		workers.emplace_back([&ready, &finished, &start]() {
			pgsql_thread___query_digests_normalize_digest_text = false;
			pgsql_thread___query_digests_max_digest_length = 2048;
			GloPgQPro->init_thread();
			PgSQL_Connection_userinfo ui;
			ui.set((char*)"worker", (char*)"password", (char*)"db", nullptr);
			char digest_text[] = "SELECT snapshot";
			ready.fetch_add(1, std::memory_order_release);
			while (!start.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
			for (int i = 0; i < updates_per_worker; ++i) {
				GloPgQPro->update_query_digest(
					0x5001, 0x6001, digest_text, 20, &ui, 1, 2500 + i,
					"127.0.0.1", 0, 1);
			}
			finished.fetch_add(1, std::memory_order_release);
			GloPgQPro->end_thread();
		});
	}
	while (ready.load(std::memory_order_acquire) != worker_count) {
		std::this_thread::yield();
	}

	bool monotonic = true;
	bool topk_valid = true;
	uint64_t previous_count = 0;
	start.store(true, std::memory_order_release);
	do {
		SQLite3_result* snapshot = GloPgQPro->get_query_digests();
		const uint64_t count = digest_count(snapshot);
		monotonic = monotonic && count >= previous_count;
		previous_count = count;
		delete snapshot;

		query_digest_filter_opts_t no_filters;
		auto topk = GloPgQPro->get_query_digests_topk(
			no_filters, query_digest_sort_by_t::count_star, 1, 0, 1);
		topk_valid = topk_valid &&
			(topk.matched_count == 0 ||
				(topk.matched_count == 1 && topk.rows.size() == 1));
	} while (finished.load(std::memory_order_acquire) != worker_count);

	for (auto& worker : workers) {
		worker.join();
	}
	SQLite3_result* result = GloPgQPro->get_query_digests();
	ok(monotonic, "concurrent admin snapshots never lose stored updates");
	ok(topk_valid, "concurrent top-k snapshots remain structurally valid");
	ok(digest_count(result) ==
			static_cast<uint64_t>(worker_count * updates_per_worker),
		"concurrent snapshots preserve the final digest count");
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);
}

static void test_concurrent_reset() {
	constexpr int worker_count = 4;
	constexpr int updates_before_reset = 100;
	constexpr int updates_during_reset = 10000;
	std::atomic<int> ready{0};
	std::atomic<bool> continue_updates{false};
	std::vector<std::thread> workers;
	workers.reserve(worker_count);

	for (int worker = 0; worker < worker_count; ++worker) {
		workers.emplace_back([&ready, &continue_updates]() {
			pgsql_thread___query_digests_normalize_digest_text = false;
			pgsql_thread___query_digests_max_digest_length = 2048;
			GloPgQPro->init_thread();
			PgSQL_Connection_userinfo ui;
			ui.set((char*)"worker", (char*)"password", (char*)"db", nullptr);
			char digest_text[] = "SELECT reset";
			for (int i = 0; i < updates_before_reset; ++i) {
				GloPgQPro->update_query_digest(
					0x6001, 0x7001, digest_text, 20, &ui, 3, 3000 + i,
					"127.0.0.1", 0, 1);
			}
			ready.fetch_add(1, std::memory_order_release);
			while (!continue_updates.load(std::memory_order_acquire)) {
				std::this_thread::yield();
			}
			for (int i = 0; i < updates_during_reset; ++i) {
				GloPgQPro->update_query_digest(
					0x6001, 0x7001, digest_text, 20, &ui, 3, 4000 + i,
					"127.0.0.1", 0, 1);
				if ((i & 31) == 0) {
					std::this_thread::yield();
				}
			}
			GloPgQPro->end_thread();
		});
	}
	while (ready.load(std::memory_order_acquire) != worker_count) {
		std::this_thread::yield();
	}

	continue_updates.store(true, std::memory_order_release);
	auto reset = GloPgQPro->get_query_digests_reset_v2(false);
	for (auto& worker : workers) {
		worker.join();
	}
	SQLite3_result* result = GloPgQPro->get_query_digests();
	const uint64_t reset_updates = digest_count(reset.first);
	const uint64_t later_updates = digest_count(result);
	const uint64_t total_updates =
		worker_count * (updates_before_reset + updates_during_reset);
	ok(reset_updates >= worker_count * updates_before_reset &&
			reset_updates <= total_updates,
		"concurrent reset includes every update completed before its boundary");
	ok(reset_updates + later_updates == total_updates,
		"concurrent reset neither loses nor duplicates worker updates");
	ok((reset.first && reset.first->rows_count == 1) &&
			(later_updates == 0 || (result && result->rows_count == 1)),
		"concurrent reset keeps one digest on each side of the boundary");
	delete reset.first;
	delete result;
	GloPgQPro->purge_query_digests(false, false, nullptr);
}

int main() {
	plan(40);

	test_init_minimal();
	test_init_query_processor();
	test_digest_merge();
	test_thread_digests();
	test_concurrent_thread_merge();
	test_concurrent_snapshot();
	test_concurrent_reset();
	test_cleanup_query_processor();
	test_cleanup_minimal();

	return exit_status();
}
