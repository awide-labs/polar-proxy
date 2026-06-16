/**
 * @file polardb_hgm_lsn_unit-t.cpp
 * @brief Unit tests for the PolarDB HostGroups Manager LSN state.
 *
 * Domain: counter metadata and thread-counter aggregation, writer-epoch
 * LSN-cache reset, and thread-local fresh-LSN reader targeting.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"

#include <atomic>
#include <cstring>
#include <memory>

extern PgSQL_HostGroups_Manager *PgHGM;
extern PgSQL_Threads_Handler *GloPTH;

#if !POLARDB_PROXY
int main() {
	plan(1);
	ok(1, "PolarDB HGM LSN unit skipped when POLARDB_PROXY is disabled");
	return exit_status();
}
#else

// This is the full-harness unit: it links libproxysql.a and constructs real
// ProxySQL HGM/connection components. Opt in to the full-harness section of the
// shared helper header (HGM fixtures + stage_polardb_topology) by defining this
// before the include; the header-only tests never define it. Included inside the
// POLARDB_PROXY branch because those fixtures reference PolarDB-only fields.
#define POLARDB_UNIT_FULL_HARNESS 1
#include "polardb_unit_common.h"

static void test_polardb_counter_metadata() {
	int thread_count = 0;
	int global_count = 0;
	const char *wait_lsn_prom_name = nullptr;
	const char *wait_bypass_prom_name = nullptr;

#define X(name, display_name, prom_name, help) \
	++thread_count; \
	if (strcmp(#name, "wait_lsn_sum_us") == 0) wait_lsn_prom_name = prom_name; \
	if (strcmp(#name, "wait_wrap_bypassed") == 0) wait_bypass_prom_name = prom_name;
	POLARDB_THREAD_COUNTER_LIST(X)
#undef X

#define X(name, display_name, prom_name, help) ++global_count;
	POLARDB_GLOBAL_COUNTER_LIST(X)
#undef X

	ok(thread_count == POLARDB_THREAD_COUNTER_COUNT,
		"PolarDB counters: thread metadata list has expected size");
	ok(global_count == POLARDB_GLOBAL_COUNTER_COUNT,
		"PolarDB counters: global metadata list has expected size");
	ok(thread_count + global_count == POLARDB_ALL_COUNTER_COUNT,
		"PolarDB counters: all metadata list covers thread plus global counters");
	ok(wait_lsn_prom_name != nullptr &&
			strcmp(wait_lsn_prom_name,
				"proxysql_polardb_wait_lsn_microseconds_total") == 0,
		"PolarDB counters: wait-latency Prometheus metric keeps microsecond units");
	ok(wait_bypass_prom_name != nullptr &&
			strcmp(wait_bypass_prom_name,
				"proxysql_polardb_wait_wrap_bypassed_total") == 0,
		"PolarDB counters: wait-bypass Prometheus metric is registered from metadata");
}

static int polardb_ordered_counter_index(const char *needle) {
	const char *expected[] = {
#define X(name, display_name, prom_name, help) display_name,
		POLARDB_COUNTER_LIST(X, X)
#undef X
	};
	const size_t expected_count = sizeof(expected) / sizeof(expected[0]);

	for (size_t i = 0; i < expected_count; ++i) {
		if (strcmp(expected[i], needle) == 0) {
			return (int)i;
		}
	}
	return -1;
}

static void test_polardb_counter_order_metadata() {
	const int monitor_lsn =
		polardb_ordered_counter_index("PolarDB_LSN_Updates_From_Monitor");
	const int monitor_role =
		polardb_ordered_counter_index("PolarDB_Monitor_Health_Invalid_Role");
	const int monitor_values =
		polardb_ordered_counter_index("PolarDB_Monitor_Health_Invalid_Values");
	const int stale =
		polardb_ordered_counter_index("PolarDB_LSN_Stale_Count");
	const int writer_retry =
		polardb_ordered_counter_index("PolarDB_Wait_Reads_Retried_On_Writer");
	const int rfq_skipped =
		polardb_ordered_counter_index("PolarDB_RFQ_Profile_Skipped");

	ok(monitor_lsn >= 0 && monitor_lsn < monitor_role &&
			monitor_role < monitor_values && monitor_values < stale,
		"PolarDB counters: ordered metadata keeps monitor counters with monitor LSN");
	ok(writer_retry >= 0 && rfq_skipped >= 0 && writer_retry < rfq_skipped,
		"PolarDB counters: ordered metadata keeps retry before RFQ profile counters");
}

static void test_polardb_thread_counter_aggregation_and_fold() {
	ok(GloPTH != nullptr && PgHGM != nullptr,
		"PolarDB counters: test globals are initialized");
	if (!GloPTH || !PgHGM) {
		return;
	}

	if (!GloPTH->pgsql_threads) {
		GloPTH->init(2, 0);
	}
	GloPTH->status_variables.threads_initialized = 1;

	PgSQL_Thread *worker1 = new PgSQL_Thread();
	PgSQL_Thread *worker2 = new PgSQL_Thread();
	GloPTH->pgsql_threads[0].worker = worker1;
	GloPTH->pgsql_threads[1].worker = worker2;

	// Aggregation arithmetic the assertions below verify:
	//   target_lsn_preferred = 10 (global) + 2 (worker1) + 3 (worker2) = 15
	//   wait_lsn_sum_us       = 100 (global) + 7 (worker1) + 11 (worker2) = 118
	//   wait_wrap_bypassed    = 5 (global) + 13 (worker1) + 17 (worker2) = 35
	const uint64_t GLOBAL_TARGET_LSN_PREFERRED = 10;
	const uint64_t GLOBAL_WAIT_LSN_SUM_US = 100;
	const uint64_t GLOBAL_WAIT_WRAP_BYPASSED = 5;
	const uint64_t EXPECTED_TARGET_LSN_PREFERRED = 15;   // 10 + 2 + 3
	const uint64_t EXPECTED_WAIT_LSN_SUM_US = 118;       // 100 + 7 + 11
	const uint64_t EXPECTED_WAIT_WRAP_BYPASSED = 35;     // 5 + 13 + 17
	PgHGM->status.polardb_target_lsn_preferred.store(
		GLOBAL_TARGET_LSN_PREFERRED, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_lsn_sum_us.store(
		GLOBAL_WAIT_LSN_SUM_US, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_wrap_bypassed.store(
		GLOBAL_WAIT_WRAP_BYPASSED, std::memory_order_relaxed);
	worker1->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 2;
	worker2->polardb_status_variables.stvar[polardb_st_var_target_lsn_preferred] = 3;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 7;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_lsn_sum_us] = 11;
	worker1->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 13;
	worker2->polardb_status_variables.stvar[polardb_st_var_wait_wrap_bypassed] = 17;

	// Number of PolarDB thread-counter slots. The X-macro list and several other
	// metadata lists are keyed off this; bump it deliberately when adding a slot.
	const int POLARDB_EXPECTED_THREAD_COUNTER_SLOTS = 19;
	ok(POLARDB_st_var_END == POLARDB_EXPECTED_THREAD_COUNTER_SLOTS,
		"PolarDB counters: thread counter list has the expected slot count");
	ok(strcmp(
			polardb_thread_counter_display_name(polardb_st_var_wait_lsn_sum_us),
			"PolarDB_Wait_LSN_Sum_Us") == 0,
		"PolarDB counters: display name is derived from the thread counter list");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == EXPECTED_TARGET_LSN_PREFERRED,
		"PolarDB counters: aggregation sums global counter and live worker counters");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_wait_lsn_sum_us,
			PgHGM->status.polardb_wait_lsn_sum_us) == EXPECTED_WAIT_LSN_SUM_US,
		"PolarDB counters: aggregation preserves value-bearing wait sum");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_wait_wrap_bypassed,
			PgHGM->status.polardb_wait_wrap_bypassed) == EXPECTED_WAIT_WRAP_BYPASSED,
		"PolarDB counters: aggregation includes wait-wrapper bypasses");

	const uint64_t GLOBAL_LSN_UPDATES_FROM_MONITOR = 6;
	PgHGM->status.polardb_lsn_updates_from_monitor.store(
		GLOBAL_LSN_UPDATES_FROM_MONITOR, std::memory_order_relaxed);
	PgHGM->p_update_metrics();
	// Each Prometheus export is checked as two invariants (family present, value
	// correct) so a wrong value reports the observed number instead of a bare 0/1.
	double metric_value = 0.0;
	ok(find_prometheus_counter_value(
			"proxysql_polardb_target_lsn_preferred_total", &metric_value),
		"PolarDB counters: Prometheus exports thread-backed totals");
	ok(metric_value == (double)EXPECTED_TARGET_LSN_PREFERRED,
		"PolarDB counters: thread-backed total value is %g (expected %g)",
		metric_value, (double)EXPECTED_TARGET_LSN_PREFERRED);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_wait_lsn_microseconds_total", &metric_value),
		"PolarDB counters: Prometheus exports wait sum in microseconds");
	ok(metric_value == (double)EXPECTED_WAIT_LSN_SUM_US,
		"PolarDB counters: wait-sum microsecond value is %g (expected %g)",
		metric_value, (double)EXPECTED_WAIT_LSN_SUM_US);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_wait_wrap_bypassed_total", &metric_value),
		"PolarDB counters: Prometheus exports wait-wrapper bypasses");
	ok(metric_value == (double)EXPECTED_WAIT_WRAP_BYPASSED,
		"PolarDB counters: wait-wrapper bypass value is %g (expected %g)",
		metric_value, (double)EXPECTED_WAIT_WRAP_BYPASSED);
	ok(find_prometheus_counter_value(
			"proxysql_polardb_lsn_updates_from_monitor_total", &metric_value),
		"PolarDB counters: Prometheus exports global-only counters");
	ok(metric_value == (double)GLOBAL_LSN_UPDATES_FROM_MONITOR,
		"PolarDB counters: global-only counter value is %g (expected %g)",
		metric_value, (double)GLOBAL_LSN_UPDATES_FROM_MONITOR);

	// A null-thread counter increment lands directly on the global slot (worker
	// slots are untouched): 10 global + 4 = 14.
	POLARDB_THREAD_COUNT(nullptr, target_lsn_preferred, 4);
	ok(PgHGM->status.polardb_target_lsn_preferred.load(std::memory_order_relaxed) == 14,
		"PolarDB counters: null-thread fallback increments the global counter");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == 19,  // 14 global + 2 + 3 workers
		"PolarDB counters: aggregation includes null-thread global counter");

	delete worker1;
	GloPTH->pgsql_threads[0].worker = nullptr;
	delete worker2;
	GloPTH->pgsql_threads[1].worker = nullptr;

	ok(PgHGM->status.polardb_target_lsn_preferred.load(std::memory_order_relaxed) == 19,
		"PolarDB counters: worker teardown folds counters into global counter");
	ok(PgHGM->status.polardb_wait_lsn_sum_us.load(std::memory_order_relaxed) == 118,
		"PolarDB counters: worker teardown folds value-bearing wait sum");
	ok(PgHGM->status.polardb_wait_wrap_bypassed.load(std::memory_order_relaxed) == 35,
		"PolarDB counters: worker teardown folds wait-wrapper bypasses");
	ok(GloPTH->get_polardb_counter(
			polardb_st_var_target_lsn_preferred,
			PgHGM->status.polardb_target_lsn_preferred) == 19,
		"PolarDB counters: total is preserved after worker teardown");

	PgHGM->status.polardb_target_lsn_preferred.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_lsn_sum_us.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_wait_wrap_bypassed.store(0, std::memory_order_relaxed);
	PgHGM->status.polardb_lsn_updates_from_monitor.store(0, std::memory_order_relaxed);
}

static void test_writer_epoch_change_resets_lsn_caches() {
	const int writer_hg = 910;
	const int reader_hg = 911;

	// Pre-epoch-change LSN cache seed values (arbitrary distinct nonzero LSNs).
	const uint64_t SEED_PRIMARY_MIRROR_LSN = 0x5000;
	const uint64_t SEED_OLD_WRITER_LSN = 0x5100;
	const uint64_t SEED_READER_LSN = 0x5200;
	const unsigned long long SEED_OLD_WRITER_LSN_TS = 101;
	const unsigned long long SEED_READER_LSN_TS = 202;

	stage_polardb_topology(PgHGM, "PolarDB HGM initial",
		writer_hg, "polardb-writer-old", 15432,
		reader_hg, "polardb-reader", 15433);

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

	writer_hgc->repl_config.polardb_primary_lsn->store(
		SEED_PRIMARY_MIRROR_LSN, std::memory_order_relaxed);
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

	ok(writer_hgc->repl_config.polardb_primary_lsn->load(std::memory_order_relaxed) == 0,
		"PolarDB HGM: writer epoch change clears primary LSN mirror");
	ok(writer_hgc->repl_config.polardb_writer_epoch->load(std::memory_order_relaxed) == 1,
		"PolarDB HGM: writer epoch increments after writer identity changes");
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
}

static void test_monitor_lsn_update_skips_non_online_servers() {
	const int writer_hg = 930;
	const int reader_hg = 931;
	const uint64_t BLOCKED_LSN = 0x9100;
	const uint64_t ACCEPTED_LSN = 0x9200;

	stage_polardb_topology(PgHGM, "PolarDB monitor LSN guard",
		writer_hg, "polardb-monitor-writer", 17432,
		reader_hg, "polardb-monitor-reader", 17433);

	PgSQL_HGC *writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	PgSQL_SrvC *writer =
		find_pgsql_server(writer_hgc, "polardb-monitor-writer", 17432);
	ok(writer_hgc != nullptr && writer != nullptr,
		"PolarDB monitor LSN guard: writer server container is available");
	if (!writer_hgc || !writer) {
		return;
	}

	writer->status = MYSQL_SERVER_STATUS_SHUNNED;
	ok(!PgHGM->polardb_update_server_lsn(
			"polardb-monitor-writer", 17432, BLOCKED_LSN),
		"PolarDB monitor LSN guard: non-ONLINE server update is rejected");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN guard: non-ONLINE server LSN cache stays empty");
	ok(writer_hgc->repl_config.polardb_primary_lsn->load(
			std::memory_order_relaxed) == 0,
		"PolarDB monitor LSN guard: non-ONLINE server does not update primary mirror");

	writer->status = MYSQL_SERVER_STATUS_ONLINE;
	ok(PgHGM->polardb_update_server_lsn(
			"polardb-monitor-writer", 17432, ACCEPTED_LSN),
		"PolarDB monitor LSN guard: ONLINE server update is accepted");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN guard: ONLINE server updates its LSN cache");
	ok(writer_hgc->repl_config.polardb_primary_lsn->load(
			std::memory_order_relaxed) == ACCEPTED_LSN,
		"PolarDB monitor LSN guard: ONLINE writer updates primary mirror");
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
}

static void test_thread_local_target_reader_requires_fresh_lsn() {
	const int writer_hg = 920;
	const int reader_hg = 921;

	stage_polardb_topology(PgHGM, "PolarDB local reader",
		writer_hg, "polardb-local-writer", 16432,
		reader_hg, "polardb-local-reader", 16433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-local-reader", 16433);
	ok(reader != nullptr,
		"PolarDB local reader: reader server container is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());

	PgSQL_Session sess;
	sess.thread = worker.get();
	sess.connections_handler = true;
	sess.client_myds = new PgSQL_Data_Stream();
	sess.client_myds->init(MYDS_FRONTEND, &sess, 0);
	sess.client_myds->myconn = new PgSQL_Connection(true);
	set_test_userinfo(sess.client_myds->myconn);

	// The reader must have applied at least REQUIRED_READER_LSN to satisfy the
	// plan. STALE_READER_LSN sits below it (rejected); the fresh value equals it.
	const uint64_t REQUIRED_READER_LSN = 0x1200;
	const uint64_t PRIMARY_LSN = 0x1300;
	const uint64_t MAX_LAG_BYTES = 0x400;
	const uint64_t STALE_READER_LSN = 0x1100;   // below REQUIRED_READER_LSN
	const uint64_t FRESH_READER_LSN = REQUIRED_READER_LSN;

	PolarDB_Query_ReaderPlan plan;
	plan.consistency_target_lsn = REQUIRED_READER_LSN;
	plan.primary_lsn = PRIMARY_LSN;
	plan.max_lag_bytes = MAX_LAG_BYTES;
	plan.fallback_writer_hg = writer_hg;

	PgSQL_Connection *cached = make_cached_reader_connection(reader);
	ok(cached != nullptr, "PolarDB local reader: cached connection fixture is available");
	if (!cached) {
		return;
	}
	reader->polardb_current_lsn.store(STALE_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	worker->push_MyConn_local(cached);

	ok(worker->get_MyConn_local_polardb_reader(reader_hg, &sess, plan) == nullptr,
		"PolarDB local reader: stale cached reader is rejected");

	reader->polardb_current_lsn.store(FRESH_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	PgSQL_Connection *hit =
		worker->get_MyConn_local_polardb_reader(reader_hg, &sess, plan);
	ok(hit == cached,
		"PolarDB local reader: fresh RFQ cached reader is returned");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_target_lsn_preferred] == 1,
		"PolarDB local reader: hit increments target-LSN preferred counter");

	delete hit;
}

int main() {
	// 61 ok() in this (POLARDB_PROXY) branch + 4 calls to stage_polardb_topology()
	// (2 assertions each, defined in polardb_unit_common.h) = 69.
	plan(69);

	int rc = test_init_minimal();
	ok(rc == 0, "test_init_minimal() succeeds");

	rc = test_init_query_processor();
	ok(rc == 0, "test_init_query_processor() succeeds");
	if (GloPTH) {
		GloPTH->variables.hostgroup_manager_verbose = 0;
	}

	rc = test_init_hostgroups();
	ok(rc == 0, "test_init_hostgroups() succeeds");

	test_polardb_counter_metadata();
	test_polardb_counter_order_metadata();
	test_polardb_thread_counter_aggregation_and_fold();
	test_writer_epoch_change_resets_lsn_caches();
	test_monitor_lsn_update_skips_non_online_servers();
	test_lsn_observation_refreshes_freshness_timestamp();
	test_thread_local_target_reader_requires_fresh_lsn();

	test_cleanup_hostgroups();
	test_cleanup_query_processor();
	test_cleanup_minimal();

	return exit_status();
}

#endif // POLARDB_PROXY
