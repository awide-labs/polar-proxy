/**
 * @file polardb_consistency_tests.cpp
 * @brief Consistency wait accounting and session-state tests.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_ExplicitTxnStateMgr.h"
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
#include <cstring>
#include <memory>
#include <string>
#include <vector>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

static void test_reader_target_selection_counter_contract() {
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	const uint64_t TARGET = 0x20000;
	const uint64_t SELECTED = 0x1F000;
	const uint64_t BEST = 0x1F800;

	polardb_count_reader_target_selection(
		worker.get(), TARGET, SELECTED, true, BEST, true);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_gap_le_4kb] == 1,
		"PolarDB target counters: selected gap enters exactly one histogram bucket");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selected_gap_samples] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selected_gap_sum_bytes] == TARGET - SELECTED,
		"PolarDB target counters: selected gap sample and byte total agree");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_compared] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_behind_best] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_loss_bytes] == BEST - SELECTED,
		"PolarDB target counters: selected-versus-best loss is classified and measured");

	polardb_count_reader_target_selection(
		worker.get(), TARGET, 0, false, 0, false);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selected_lsn_unknown] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selected_gap_samples] == 1,
		"PolarDB target counters: unknown selected LSN does not create a gap sample");

	polardb_count_reader_target_selection(
		worker.get(), TARGET, TARGET, false, TARGET, true);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selected_lsn_stale] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_selection_compared] == 1,
		"PolarDB target counters: stale selected LSN is not compared with the best sample");
}

static void test_wait_histogram_boundary_contract() {
	struct WaitBucketCase {
		unsigned long long first_sample;
		unsigned long long second_sample;
		PolarDB_ThreadStatusVariable ordinary_counter;
		PolarDB_ThreadStatusVariable split_counter;
#if POLARDB_PROFILE
		PolarDB_ThreadStatusVariable txn_counter;
#endif // POLARDB_PROFILE
		const char* label;
	};
#if POLARDB_PROFILE
#define POLARDB_WAIT_BUCKET_CASE(first, second, suffix) { \
	(first), (second), polardb_st_var_wait_lsn_elapsed_##suffix, \
	polardb_st_var_split_lsn_wait_elapsed_##suffix, \
	polardb_st_var_txn_wait_lsn_elapsed_##suffix, #suffix }
#else
#define POLARDB_WAIT_BUCKET_CASE(first, second, suffix) { \
	(first), (second), polardb_st_var_wait_lsn_elapsed_##suffix, \
	polardb_st_var_split_lsn_wait_elapsed_##suffix, #suffix }
#endif // POLARDB_PROFILE
	const WaitBucketCase wait_cases[] = {
		POLARDB_WAIT_BUCKET_CASE(0, 1000, le_1ms),
		POLARDB_WAIT_BUCKET_CASE(1001, 5000, le_5ms),
		POLARDB_WAIT_BUCKET_CASE(5001, 10000, le_10ms),
		POLARDB_WAIT_BUCKET_CASE(10001, 50000, le_50ms),
		POLARDB_WAIT_BUCKET_CASE(50001, 100000, le_100ms),
		POLARDB_WAIT_BUCKET_CASE(100001, 500000, le_500ms),
		POLARDB_WAIT_BUCKET_CASE(500001, 1000000, le_1s),
		POLARDB_WAIT_BUCKET_CASE(1000001, 2000000, gt_1s),
	};
#undef POLARDB_WAIT_BUCKET_CASE

	std::unique_ptr<PgSQL_Thread> wait_worker(new PgSQL_Thread());
	uint64_t ordinary_wait_total = 0;
	uint64_t split_wait_total = 0;
#if POLARDB_PROFILE
	uint64_t txn_wait_total = 0;
#endif // POLARDB_PROFILE
	for (const WaitBucketCase& bucket : wait_cases) {
		polardb_count_lsn_wait_elapsed_bucket(
			wait_worker.get(), bucket.first_sample, false);
		polardb_count_lsn_wait_elapsed_bucket(
			wait_worker.get(), bucket.second_sample, false);
		polardb_count_lsn_wait_elapsed_bucket(
			wait_worker.get(), bucket.first_sample, true);
		polardb_count_lsn_wait_elapsed_bucket(
			wait_worker.get(), bucket.second_sample, true);
#if POLARDB_PROFILE
		PolarDB_WaitProfileState state;
		state.active = true;
		state.context = PolarDB_WaitProfileContext::TXN_PREWRITE;
		polardb_count_wait_profile_completion(
			wait_worker.get(), state, bucket.first_sample);
		polardb_count_wait_profile_completion(
			wait_worker.get(), state, bucket.second_sample);
#endif // POLARDB_PROFILE
		bool mapped =
			wait_worker->polardb_status_variables.stvar[
				bucket.ordinary_counter] == 2 &&
			wait_worker->polardb_status_variables.stvar[
				bucket.split_counter] == 2;
#if POLARDB_PROFILE
		mapped = mapped &&
			wait_worker->polardb_status_variables.stvar[
				bucket.txn_counter] == 2;
#endif // POLARDB_PROFILE
		ok(mapped, "PolarDB wait histogram: %s boundaries map consistently",
			bucket.label);
		ordinary_wait_total +=
			wait_worker->polardb_status_variables.stvar[
				bucket.ordinary_counter];
		split_wait_total +=
			wait_worker->polardb_status_variables.stvar[
				bucket.split_counter];
#if POLARDB_PROFILE
		txn_wait_total +=
			wait_worker->polardb_status_variables.stvar[
				bucket.txn_counter];
#endif // POLARDB_PROFILE
	}
	bool wait_totals_match =
		ordinary_wait_total == 2 * sizeof(wait_cases) / sizeof(wait_cases[0]) &&
		split_wait_total == 2 * sizeof(wait_cases) / sizeof(wait_cases[0]);
#if POLARDB_PROFILE
	wait_totals_match = wait_totals_match &&
		txn_wait_total == 2 * sizeof(wait_cases) / sizeof(wait_cases[0]);
#endif // POLARDB_PROFILE
	ok(wait_totals_match,
		"PolarDB wait histogram: each sample increments exactly one bucket");

	struct GapBucketCase {
		uint64_t first_gap;
		uint64_t second_gap;
		PolarDB_ThreadStatusVariable target_counter;
#if POLARDB_PROFILE
		PolarDB_ThreadStatusVariable profile_counter;
#endif // POLARDB_PROFILE
		const char* label;
	};
#if POLARDB_PROFILE
#define POLARDB_GAP_BUCKET_CASE(first, second, suffix) { \
	(first), (second), polardb_st_var_reader_target_gap_##suffix, \
	polardb_st_var_wait_profile_gap_##suffix##_count, #suffix }
#else
#define POLARDB_GAP_BUCKET_CASE(first, second, suffix) { \
	(first), (second), polardb_st_var_reader_target_gap_##suffix, #suffix }
#endif // POLARDB_PROFILE
	const GapBucketCase gap_cases[] = {
		POLARDB_GAP_BUCKET_CASE(0, 0, zero),
		POLARDB_GAP_BUCKET_CASE(1, 4ULL * 1024ULL, le_4kb),
		POLARDB_GAP_BUCKET_CASE(
			4ULL * 1024ULL + 1, 64ULL * 1024ULL, le_64kb),
		POLARDB_GAP_BUCKET_CASE(
			64ULL * 1024ULL + 1, 1024ULL * 1024ULL, le_1mb),
		POLARDB_GAP_BUCKET_CASE(
			1024ULL * 1024ULL + 1, 16ULL * 1024ULL * 1024ULL,
			le_16mb),
		POLARDB_GAP_BUCKET_CASE(
			16ULL * 1024ULL * 1024ULL + 1,
			32ULL * 1024ULL * 1024ULL, gt_16mb),
	};
#undef POLARDB_GAP_BUCKET_CASE

	std::unique_ptr<PgSQL_Thread> gap_worker(new PgSQL_Thread());
	const uint64_t target_lsn = 64ULL * 1024ULL * 1024ULL + 1;
	for (const GapBucketCase& bucket : gap_cases) {
		const uint64_t gaps[] = {bucket.first_gap, bucket.second_gap};
		for (uint64_t gap : gaps) {
			polardb_count_reader_target_lsn_gap_bucket(
				gap_worker.get(), target_lsn, target_lsn - gap, true);
#if POLARDB_PROFILE
			PolarDB_WaitProfileState state;
			state.active = true;
			state.context = PolarDB_WaitProfileContext::ORDINARY;
			state.selection_recorded = true;
			state.selected_lsn_known = true;
			state.selected_lsn_fresh = true;
			state.selected_gap_bytes = gap;
			polardb_count_wait_profile_completion(
				gap_worker.get(), state, 1);
#endif // POLARDB_PROFILE
		}
		bool mapped =
			gap_worker->polardb_status_variables.stvar[
				bucket.target_counter] == 2;
#if POLARDB_PROFILE
		mapped = mapped &&
			gap_worker->polardb_status_variables.stvar[
				bucket.profile_counter] == 2;
#endif // POLARDB_PROFILE
		ok(mapped, "PolarDB LSN-gap histogram: %s boundaries map consistently",
			bucket.label);
	}
	polardb_count_reader_target_lsn_gap_bucket(
		gap_worker.get(), target_lsn, target_lsn + 1, true);
	ok(gap_worker->polardb_status_variables.stvar[
			polardb_st_var_reader_target_gap_zero] == 3,
		"PolarDB LSN-gap histogram: reader ahead of target maps to zero gap");
	uint64_t target_gap_total = 0;
#if POLARDB_PROFILE
	uint64_t profile_gap_total = 0;
#endif // POLARDB_PROFILE
	for (const GapBucketCase& bucket : gap_cases) {
		target_gap_total +=
			gap_worker->polardb_status_variables.stvar[
				bucket.target_counter];
#if POLARDB_PROFILE
		profile_gap_total +=
			gap_worker->polardb_status_variables.stvar[
				bucket.profile_counter];
#endif // POLARDB_PROFILE
	}
	bool gap_totals_match =
		target_gap_total == 2 * sizeof(gap_cases) / sizeof(gap_cases[0]) + 1;
#if POLARDB_PROFILE
	gap_totals_match = gap_totals_match &&
		profile_gap_total == 2 * sizeof(gap_cases) / sizeof(gap_cases[0]);
#endif // POLARDB_PROFILE
	ok(gap_totals_match,
		"PolarDB LSN-gap histogram: each sample increments exactly one bucket");
}

static void test_v2_target_reader_keeps_wait_until_lsn_is_reached() {
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
	attach_test_frontend(sess, worker.get());

	// The reader must have applied at least REQUIRED_READER_LSN to satisfy the
	// wait target. STALE_READER_LSN sits below it; the fresh value equals it.
	const uint64_t REQUIRED_READER_LSN = 0x1200;
	const uint64_t GROUP_LSN = 0x1300;
	const uint64_t MAX_LAG_BYTES = 0x400;
	const uint64_t STALE_READER_LSN = 0x1100;   // below REQUIRED_READER_LSN
	const uint64_t FRESH_READER_LSN = REQUIRED_READER_LSN;

	PolarDB_Query_ReaderPlan plan;
	plan.group_lsn = GROUP_LSN;
	plan.max_lag_bytes = MAX_LAG_BYTES;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		REQUIRED_READER_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);

	PgSQL_Connection *cached = make_cached_reader_connection(reader);
	ok(cached != nullptr, "PolarDB local reader: cached connection fixture is available");
	if (!cached) {
		return;
	}
	cached->pgsql_conn = unit_connected_pgconn();
	ok(cached->pgsql_conn != nullptr,
		"PolarDB v2 reader: connected libpq fixture is available");
	reader->polardb_current_lsn.store(STALE_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	unit_reader_pool_add_matching(reader, cached);
	const PolarDB_ThreadCounterSnapshot stale_gap_bucket(
		worker.get(), polardb_st_var_reader_target_gap_le_4kb);
	const PolarDB_ThreadCounterSnapshot stale_gap_sum(
		worker.get(),
		polardb_st_var_reader_target_selected_gap_sum_bytes);

	PolarDB_ReaderResult stale_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(stale_result.acquired(),
		"PolarDB v2 reader: reader below the target remains usable with the backend wait");
	ok(!stale_result.wait_bypass_allowed,
		"PolarDB v2 reader: reader below the target cannot bypass wait wrapping");
	ok(stale_gap_bucket.delta() == 1 &&
			stale_gap_sum.delta() ==
				REQUIRED_READER_LSN - STALE_READER_LSN,
		"PolarDB v2 reader: acquisition records the selected reader target gap once");
	if (!stale_result.acquired()) {
		return;
	}
	PgHGM->push_MyConn_to_pool(stale_result.conn);

	reader->polardb_current_lsn.store(FRESH_READER_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	const PolarDB_ThreadCounterSnapshot zero_gap(
		worker.get(), polardb_st_var_reader_target_gap_zero);
	PolarDB_ReaderResult hit_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	PgSQL_Connection *hit = hit_result.conn;
	ok(hit_result.acquired(),
		"PolarDB v2 reader: fresh RFQ connection is acquired");
	ok(hit == cached,
		"PolarDB v2 reader: the selected server returns its exact matching connection");
	ok(hit_result.wait_bypass_allowed,
		"PolarDB v2 reader: fresh RFQ connection can bypass wait wrapping");
	ok(zero_gap.delta() == 1,
		"PolarDB v2 reader: target-reached acquisition records a zero selected gap");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_target_lsn_preferred] == 1,
		"PolarDB v2 reader: hit increments target-LSN preferred counter");

	if (hit) {
		PgHGM->push_MyConn_to_pool(hit);
		reader->remove_free_connection(hit);
		delete hit;
	}

	const PolarDB_ThreadCounterSnapshot fallback_wait(
		worker.get(), polardb_st_var_target_lsn_fallback_wait);
	const int saved_creation_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		std::numeric_limits<int>::max();
	PolarDB_ReaderResult created_result =
		PgHGM->polardb_acquire_reader_connection(
			reader_hg, &sess, plan, wait, /*only_pooled=*/false);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		saved_creation_throttle;
	ok(created_result.acquired(),
		"PolarDB v2 reader: cold target read creates a reader backend");
	ok(created_result.acquired() && !created_result.wait_bypass_allowed &&
			fallback_wait.delta() == 1,
		"PolarDB v2 reader: cold backend keeps wait and counts fallback selection");
	if (created_result.conn) {
		reader->remove_used_connection(created_result.conn);
		delete created_result.conn;
	}
}

static void test_reader_wait_selection_activates_only_when_needed() {
	const uint64_t TARGET_LSN = 0xB050;
	const int WRITER_HG = 949;
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	const PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		TARGET_LSN, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	const PolarDB_ThreadCounterSnapshot prepared(
		worker.get(), polardb_st_var_wait_wrap_prepared);
	const PolarDB_ThreadCounterSnapshot bypassed(
		worker.get(), polardb_st_var_wait_wrap_bypassed);

	const bool ready_activated =
		sess.polardb_finish_reader_wait_selection(
			wait, /*target_reached=*/true, WRITER_HG);
	ok(!ready_activated && !sess.polardb_wait_active() &&
			!sess.polardb_query.wait.spec.has_wait() &&
			sess.polardb_query.wait_bypass_target == TARGET_LSN,
		"PolarDB direct reader: target-ready selection leaves wrapper state inactive");
	ok(prepared.delta() == 0 && bypassed.delta() == 1,
		"PolarDB direct reader: target-ready selection counts only wait bypass");

	sess.polardb_query.clear_reader_route();
	const bool behind_activated =
		sess.polardb_finish_reader_wait_selection(
			wait, /*target_reached=*/false, WRITER_HG);
	ok(behind_activated && sess.polardb_wait_active() &&
			sess.polardb_query.wait.spec.target == TARGET_LSN &&
			sess.polardb_query.wait.fallback_writer_hg == WRITER_HG,
		"PolarDB wait reader: behind-target selection activates the requested wait");
	ok(prepared.delta() == 1 && bypassed.delta() == 1,
		"PolarDB wait reader: only the behind-target selection counts wrapper preparation");
}

static void test_successful_wait_cache_advance_requires_active_wait() {
	const int writer_hg = 950;
	const int reader_hg = 951;
	const uint64_t TARGET_LSN = 0xB100;

	stage_polardb_topology(PgHGM, "PolarDB wait cache advance",
		writer_hg, "polardb-wait-writer", 19432,
		reader_hg, "polardb-wait-reader", 19433);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-wait-reader", 19433);
	const auto reader_cfg = PgHGM->get_polardb_hg_config(reader_hg);
	ok(reader != nullptr,
		"PolarDB wait cache advance: reader server container is available");
	ok(reader_cfg.is_polardb_hostgroup &&
			reader_cfg.writer_hostgroup == writer_hg,
		"PolarDB wait cache advance: reader has writer scope config");
	if (!reader || !reader_cfg.is_polardb_hostgroup ||
			reader_cfg.writer_hostgroup != writer_hg) {
		return;
	}

	PgSQL_Session sess;
	sess.connections_handler = true;
	sess.polardb_config.is_polardb_enabled = true;
	sess.polardb_query.request_writer_scope = PolarDB_WriterScope{
		reader_cfg.writer_hostgroup,
		reader_cfg.writer_epoch};
	sess.polardb_query.wait.wrapper_finalized = true;
	sess.polardb_query.wait.spec.type = PolarDB_WaitType::LSN;
	sess.polardb_query.wait.spec.target = TARGET_LSN;

	PgSQL_Data_Stream backend_myds;
	PgSQL_Connection* backend_conn = new PgSQL_Connection(false);
	backend_conn->parent = reader;
	backend_myds.myconn = backend_conn;

	sess.polardb_query.wait.wait_started_at_us = 0;
	sess.polardb_finish_wait(&backend_myds);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: timed-out/accounted wait does not advance reader LSN cache");

	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_finish_wait(&backend_myds);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: installed but unconsumed wrapper is not success confirmation");

	backend_conn->polardb_query_wrap_state.begin(
		1, PolarDB_Query_WrapperKind::CONSISTENCY_WAIT);
	backend_conn->polardb_query_wrap_state.consume_successful_wrapper_set();
	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_finish_wait(&backend_myds);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == TARGET_LSN,
		"PolarDB wait cache advance: consumed successful wait advances reader LSN cache");

	reader->polardb_current_lsn.store(0, std::memory_order_relaxed);
	reader->lsn_updated_at.store(0, std::memory_order_relaxed);
	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_finish_wait(nullptr);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: failure cleanup does not advance reader LSN cache");

	backend_myds.myconn = nullptr;
	delete backend_conn;
}

static void test_wait_wrapper_failure_preserves_prefix() {
	PgSQL_Session sess;
	PolarDB_Query_WaitState wait;
	wait.spec.type = static_cast<PolarDB_WaitType>(255);
	wait.spec.target = 1;
	wait.spec.timeout_ms = 1000;
	std::string wrapped = "prefix; ";
	const std::string original = wrapped;

	const uint32_t wrapper_stmts = sess.append_wrapped_wait_query(
		"SELECT 1", strlen("SELECT 1"), wait,
		sess.polardb_wait_mode_set_statement(PolarDB_WaitMode::STRICT),
		wrapped);
	ok(wrapper_stmts == 0 && wrapped == original,
		"PolarDB wait wrapper: failed assembly preserves caller prefix");
}

static void test_wait_wrapper_keeps_original_query_alive() {
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());

	const char* original_sql = "SELECT 42";
	PtrSize_t packet = unit_simple_query_packet(original_sql);
	sess.CurrentQuery.begin(
		static_cast<unsigned char*>(packet.ptr), packet.size, true);

	PgSQL_Data_Stream backend_myds;
	backend_myds.pgsql_real_query.init(&packet);
	PgSQL_Connection backend_conn(false);

	sess.polardb_query.wait.prepare_from_spec(PolarDB_WaitSpec::from_lsn(
		0x1200, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::STRICT));
	sess.polardb_query.wait.wait_stage = PolarDB_WaitStage::WAITING;

	const PolarDB_WrapFinalizeResult result =
		sess.polardb_install_wait_wrapper(
			&backend_conn, &backend_myds);

	ok(result == PolarDB_WrapFinalizeResult::CONTINUE &&
			sess.polardb_query.wait.wrapper_finalized &&
			sess.polardb_query.original_query == original_sql,
		"PolarDB wait wrapper: finalization copies the original SQL and installs the wrapper");
	ok(sess.CurrentQuery.QueryPointer ==
			reinterpret_cast<unsigned char*>(
				sess.polardb_query.original_query.data()) &&
			sess.CurrentQuery.QueryLength ==
				sess.polardb_query.original_query.size() + 1,
		"PolarDB wait wrapper: CurrentQuery uses request-owned original SQL");
	ok(std::strcmp(
			reinterpret_cast<const char*>(sess.CurrentQuery.QueryPointer),
			original_sql) == 0 &&
			backend_myds.pgsql_real_query.QueryPtr !=
				reinterpret_cast<char*>(sess.CurrentQuery.QueryPointer),
		"PolarDB wait wrapper: logging sees original SQL while backend sees wrapper");

	backend_myds.free_pgsql_real_query();
}

#if POLARDB_PROFILE
static void test_wait_profile_target_and_counter_contract() {
	PgSQL_Session sess;
	PolarDB_Query_RouteCtx route_ctx;
	route_ctx.session.write_lsn = 0x1000;
	route_ctx.session.observed_lsn = 0x2000;
	route_ctx.session.observed_lsn_source_server_token = 0xA1;
	PolarDB_Query_RoutePlan plan;
	plan.wait_spec = PolarDB_WaitSpec::from_lsn(
		0x2000, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	plan.reader.consistency_mode = PolarDB_ConsistencyMode::SESSION_LSN;

	sess.polardb_profile_prepare_wait(
		plan, route_ctx, PolarDB_WaitProfileContext::TXN_PREWRITE);
	ok(sess.polardb_query.wait_profile.active &&
			sess.polardb_query.wait_profile.target_source ==
				PolarDB_WaitProfileTargetSource::OBSERVED &&
			sess.polardb_query.wait_profile.observed_source_server_token == 0xA1,
		"PolarDB wait profile: pre-write wait attributes an observed-session target");

	plan.reader.consistency_mode = PolarDB_ConsistencyMode::GLOBAL_LSN;
	plan.wait_spec.target = 0x3000;
	sess.polardb_profile_prepare_wait(
		plan, route_ctx, PolarDB_WaitProfileContext::ORDINARY);
	ok(sess.polardb_query.wait_profile.target_source ==
			PolarDB_WaitProfileTargetSource::GLOBAL &&
			!sess.polardb_query.wait_profile.target_mismatch,
		"PolarDB wait profile: global LSN raise is attributed separately");

	route_ctx.transaction_split.primary_lsn = 0x4000;
	plan.wait_spec.target = 0x4000;
	sess.polardb_profile_prepare_wait(
		plan, route_ctx, PolarDB_WaitProfileContext::TXN_SPLIT);
	ok(sess.polardb_query.wait_profile.target_source ==
			PolarDB_WaitProfileTargetSource::TXN_PRIMARY &&
			!sess.polardb_query.wait_profile.target_mismatch,
		"PolarDB wait profile: transaction split attributes its primary LSN target");

	plan.reader.consistency_mode = PolarDB_ConsistencyMode::SESSION_LSN;
	plan.wait_spec.target = 0x4001;
	sess.polardb_profile_prepare_wait(
		plan, route_ctx, PolarDB_WaitProfileContext::TXN_SPLIT);
	ok(sess.polardb_query.wait_profile.target_mismatch &&
			sess.polardb_query.wait_profile.target_source ==
				PolarDB_WaitProfileTargetSource::UNKNOWN,
		"PolarDB wait profile: inconsistent captured target is counted as a mismatch");

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PolarDB_WaitProfileState state;
	state.active = true;
	state.context = PolarDB_WaitProfileContext::TXN_PREWRITE;
	state.target_source = PolarDB_WaitProfileTargetSource::OBSERVED;
	state.selection_recorded = true;
	state.selected_lsn_known = true;
	state.selected_lsn_fresh = true;
	state.selection_compared = true;
	state.selected_behind_best = true;
	state.selected_gap_bytes = 4096;
	state.selection_loss_bytes = 512;
	state.selected_lsn_age_known = true;
	state.selected_lsn_age_us = 750;
	state.observed_source_server_token = 0xA1;
	state.selected_server_token = 0xB2;
	polardb_count_wait_profile_completion(worker.get(), state, 3000);

	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_txn_wait_lsn_count] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_txn_wait_lsn_sum_us] == 3000 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_txn_wait_lsn_elapsed_le_5ms] == 1,
		"PolarDB wait profile: pre-write count, sum, and histogram share one sample");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_target_observed_count] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_target_observed_sum_us] == 3000 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_observed_cross_reader_count] == 1,
		"PolarDB wait profile: observed target correlates cross-reader elapsed time");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_selected_behind_best_count] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_selection_loss_sum_bytes] == 512,
		"PolarDB wait profile: selected-behind-best count keeps its LSN loss");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_gap_le_4kb_count] == 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_selected_gap_sum_bytes] == 4096 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_wait_profile_lsn_age_le_1ms_count] == 1,
		"PolarDB wait profile: target-gap and LSN-age buckets correlate the sample");
}
#endif // POLARDB_PROFILE

static void test_session_route_state_clear_tiers() {
	PgSQL_Session::PolarDB_SessionRouteState state;
	state.wait_disabled = true;
	state.client_rfq_lsn_requested = true;
	state.rfq_degraded_route_warning_sent = true;

	state.clear_resettable();
	ok(!state.wait_disabled &&
			!state.rfq_degraded_route_warning_sent &&
			state.client_rfq_lsn_requested,
		"PolarDB route state: resettable clear keeps client RFQ capability");

	state.wait_disabled = true;
	state.rfq_degraded_route_warning_sent = true;
	state.clear_session();
	ok(!state.wait_disabled &&
			!state.rfq_degraded_route_warning_sent &&
			!state.client_rfq_lsn_requested,
		"PolarDB route state: session clear removes client RFQ capability");
}

static void test_notice_queue_state_contract() {
	PgSQL_Session sess;
	PgSQL_Session::PolarDB_NoticeQueueState& notices = sess.polardb_notices;
	ok(notices.empty() && notices.len() == 0,
		"PolarDB notice queue: default state is empty");

	unsigned char* freed_pkt = (unsigned char*)l_alloc(4);
	memset(freed_pkt, 'n', 4);
	notices.add(freed_pkt, 4);
	ok(!notices.empty() && notices.len() == 1,
		"PolarDB notice queue: add allocates queue lazily");
	sess.discard_pending_notices();
	ok(notices.empty() && notices.len() == 0 && notices.pending == nullptr,
		"PolarDB notice queue: discard releases queued packets");

	sess.discard_pending_notices();
	ok(notices.empty() && notices.pending == nullptr,
		"PolarDB notice queue: repeated discard is safe");
}

static void test_user_attributes_are_reapplied_after_reset() {
	PgSQL_Session sess;
	sess.user_attributes = strdup(
		"{\"default-transaction_isolation\":\"serializable\"}");

	sess.polardb_config.txn_reader_wait_default_read_committed = true;
	sess.polardb_config.txn_reader_wait_backend_default_seen = true;
	sess.polardb_reapply_user_attributes_after_reset();
	ok(!sess.polardb_txn_wait_uses_read_committed(),
		"PolarDB reset attributes: serializable user default disables pre-write reader wait");
	ok(!sess.polardb_config.txn_reader_wait_backend_default_seen,
		"PolarDB reset attributes: backend default observation is cleared");

	free(sess.user_attributes);
	sess.user_attributes = strdup(
		"{\"default-transaction_isolation\":\"read committed\"}");
	sess.polardb_config.txn_reader_wait_default_read_committed = false;
	sess.polardb_config.txn_reader_wait_backend_default_seen = true;
	sess.polardb_reapply_user_attributes_after_reset();
	ok(sess.polardb_txn_wait_uses_read_committed(),
		"PolarDB reset attributes: read committed user default enables pre-write reader wait");
	ok(!sess.polardb_config.txn_reader_wait_backend_default_seen,
		"PolarDB reset attributes: refreshed user default owns the reset state");
}

static void test_collect_is_const_stable_snapshot() {
	const int writer_hg = 978;
	const int reader_hg = 979;

	stage_polardb_topology_with_txn_split(PgHGM, "PolarDB collect snapshot",
		writer_hg, "polardb-collect-writer", 24432,
		reader_hg, "polardb-collect-reader", 24433);

	const auto writer_cfg = PgHGM->get_polardb_hg_config(writer_hg);
	ok(writer_cfg.is_polardb_hostgroup &&
			writer_cfg.writer_hostgroup == writer_hg,
		"PolarDB collect snapshot: writer config is available");
	if (!writer_cfg.is_polardb_hostgroup ||
			writer_cfg.writer_hostgroup != writer_hg) {
		return;
	}
	PgSQL_HGC* writer_hgc = PgHGM->MyHGC_lookup(writer_hg);
	ok(writer_hgc != nullptr &&
			writer_hgc->repl_config.polardb_max_replica_replay_lsn != nullptr,
		"PolarDB collect snapshot: replica replay state is available");
	if (!writer_hgc ||
			!writer_hgc->repl_config.polardb_max_replica_replay_lsn) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	pgsql_thread___polardb_read_target = static_cast<int>(
		PolarDB_ReadTarget::REPLICA);
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	const char locking_query[] = "SELECT * FROM t FOR UPDATE";
	sess.CurrentQuery.begin(
		(unsigned char*)const_cast<char*>(locking_query),
		strlen(locking_query) + 1,
		false);
	sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SELECT;
	sess.polardb_session_consistency.writer_scope = PolarDB_WriterScope{
		writer_cfg.writer_hostgroup,
		writer_cfg.writer_epoch};
	sess.polardb_session_consistency.write_lsn = 0x2110;
	sess.polardb_session_consistency.observed_lsn = 0x2220;
	sess.polardb_txn_wait_safety.local_state_changed = true;
	pgsql_thread___polardb_profile_off = false;
	sess.polardb_observe_route_inputs(writer_hg);

	const PolarDB_SessionConsistency before_session =
		sess.polardb_session_consistency;
	const bool before_local_state =
		sess.polardb_txn_wait_safety.local_state_changed;
	const PgSQL_Session& const_sess = sess;
	const PolarDB_Query_RouteCtx first = const_sess.polardb_collect(
		writer_hg, /*qpo_replica_eligible=*/1,
		/*qpo_force_primary_hint=*/false);
	const PolarDB_Query_RouteCtx second = const_sess.polardb_collect(
		writer_hg, /*qpo_replica_eligible=*/1,
		/*qpo_force_primary_hint=*/false);

	ok(first.is_polar_hg && second.is_polar_hg,
		"PolarDB collect snapshot: repeated collect sees PolarDB topology");
	ok(!first.is_txn_split_safe_read && first.is_txn_split_locking_read,
		"PolarDB collect snapshot: autocommit locking read is marked writer-required");
	PolarDB_Query_RoutePlan locking_plan = sess.polardb_plan(first);
	ok(locking_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			locking_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::
					SPLIT_LOCKING_READ,
		"PolarDB collect snapshot: autocommit locking read plans writer route");

	pgsql_thread___polardb_read_target = static_cast<int>(
		PolarDB_ReadTarget::PRIMARY);
	const PolarDB_Query_RouteCtx primary_target = const_sess.polardb_collect(
		writer_hg, /*qpo_replica_eligible=*/1,
		/*qpo_force_primary_hint=*/false);
	ok(!primary_target.is_multi_statement &&
			!primary_target.is_txn_split_safe_read &&
			!primary_target.is_txn_split_locking_read,
		"PolarDB collect shape: primary-targeted reads skip SQL classification");
	const PolarDB_Query_RoutePlan primary_target_plan =
		sess.polardb_plan(primary_target);
	ok(primary_target_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			primary_target_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::
					READ_TARGET_PRIMARY,
		"PolarDB collect shape: skipped classification preserves the primary route");
	pgsql_thread___polardb_read_target = static_cast<int>(
		PolarDB_ReadTarget::REPLICA);

	if (sess.transaction_state_manager) {
		const bool split_enabled = sess.polardb_query.txn_split_enabled;
		sess.transaction_state_manager->handle_transaction("BEGIN");
		sess.polardb_query.txn_split_enabled = false;
		const PolarDB_Query_RouteCtx split_disabled = const_sess.polardb_collect(
			writer_hg, /*qpo_replica_eligible=*/1,
			/*qpo_force_primary_hint=*/false);
		ok(split_disabled.in_transaction &&
				!split_disabled.is_multi_statement &&
				!split_disabled.is_txn_split_safe_read &&
				!split_disabled.is_txn_split_locking_read,
			"PolarDB collect shape: a split-disabled transaction skips SQL classification");
		const PolarDB_Query_RoutePlan split_disabled_plan =
			sess.polardb_plan(split_disabled);
		ok(split_disabled_plan.action ==
				PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
				split_disabled_plan.action_reason ==
					PolarDB_Query_RoutePlan::RouteActionReason::
						HG_SPLIT_DISABLED,
			"PolarDB collect shape: split-disabled transaction remains on the primary");
		// Unit sessions do not attach a backend connection, so clean the
		// synthetic transaction state directly instead of executing ROLLBACK's
		// client/server variable reconciliation.
		sess.transaction_state_manager->reset_state();
		sess.polardb_query.txn_split_enabled = split_enabled;
	} else {
		ok(0, "PolarDB collect shape: transaction manager fixture exists");
		ok(0, "PolarDB collect shape: split-disabled route fixture exists");
	}

	PolarDB_Query_RouteCtx no_reader_ctx;
	no_reader_ctx.is_polar_hg = true;
	no_reader_ctx.replica_eligible = true;
	no_reader_ctx.reader_hg = -1;
	no_reader_ctx.writer_scope = first.writer_scope;
	no_reader_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL);
	no_reader_ctx.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	no_reader_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR);
	PolarDB_Query_RoutePlan no_reader_plan =
		sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
				PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR &&
			no_reader_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::
					READ_FALLBACK_ERROR,
		"PolarDB placement: error fallback rejects a missing reader "
		"hostgroup");

	no_reader_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY);
	no_reader_plan = sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
			no_reader_plan.target_hg == first.writer_scope.hg,
		"PolarDB placement: primary fallback handles a missing reader hostgroup");

	no_reader_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR);
	no_reader_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::OFF);
	no_reader_plan = sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
			no_reader_plan.target_hg == -1,
		"PolarDB placement: consistency off leaves routing to ordinary ProxySQL");

	no_reader_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL);
	no_reader_ctx.force_primary_hint = true;
	no_reader_plan = sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			no_reader_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::HINT_PRIMARY,
		"PolarDB placement: an explicit primary hint overrides the replica target");

	PolarDB_Query_RouteCtx policy_snapshot_ctx;
	policy_snapshot_ctx.is_polar_hg = true;
	policy_snapshot_ctx.replica_eligible = true;
	policy_snapshot_ctx.reader_hg = reader_hg;
	policy_snapshot_ctx.writer_scope = first.writer_scope;
	policy_snapshot_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::EVENTUAL);
	policy_snapshot_ctx.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	policy_snapshot_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR);
	policy_snapshot_ctx.missing_lsn_action =
		static_cast<int>(PolarDB_MissingLsnAction::ERROR);
	policy_snapshot_ctx.lsn_wait_timeout_action =
		static_cast<int>(PolarDB_LsnWaitTimeoutAction::DISCONNECT);
	policy_snapshot_ctx.replica_loss_action = static_cast<int>(
		PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR);
	policy_snapshot_ctx.replica_error_action =
		static_cast<int>(PolarDB_ReplicaErrorAction::DISCONNECT);
	PolarDB_Query_RoutePlan policy_snapshot_plan =
		sess.polardb_plan(policy_snapshot_ctx);
	ok(policy_snapshot_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
			policy_snapshot_plan.target_hg == reader_hg,
		"PolarDB policy snapshot: reader-producing plan targets the reader");
	ok(policy_snapshot_plan.reader.read_target ==
			policy_snapshot_ctx.read_target &&
			policy_snapshot_plan.reader.read_fallback_action ==
				policy_snapshot_ctx.read_fallback_action &&
			policy_snapshot_plan.reader.missing_lsn_action ==
				policy_snapshot_ctx.missing_lsn_action &&
			policy_snapshot_plan.reader.lsn_wait_timeout_action ==
				policy_snapshot_ctx.lsn_wait_timeout_action &&
			policy_snapshot_plan.reader.replica_loss_action ==
				policy_snapshot_ctx.replica_loss_action &&
			policy_snapshot_plan.reader.replica_error_action ==
				policy_snapshot_ctx.replica_error_action,
		"PolarDB policy snapshot: plan captures every reader failure decision");
	PolarDB_Query_RouteCtx first_session_read_ctx = policy_snapshot_ctx;
	first_session_read_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	first_session_read_ctx.session.writer_scope =
		first_session_read_ctx.writer_scope;
	first_session_read_ctx.session.write_lsn = 0;
	first_session_read_ctx.session.observed_lsn = 0;
	const PolarDB_Query_RoutePlan first_session_read_plan =
		sess.polardb_plan(first_session_read_ctx);
	ok(first_session_read_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::PASSTHROUGH &&
			first_session_read_plan.target_hg == reader_hg &&
			first_session_read_plan.reader.consistency_mode ==
				PolarDB_ConsistencyMode::SESSION_LSN,
		"PolarDB first session read: reader plan retains SESSION_LSN for failure policy");
	PolarDB_Query_RouteCtx missing_primary_ctx = policy_snapshot_ctx;
	missing_primary_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	missing_primary_ctx.session.writer_scope =
		missing_primary_ctx.writer_scope;
	missing_primary_ctx.session.write_unknown = true;
	missing_primary_ctx.missing_lsn_action =
		static_cast<int>(PolarDB_MissingLsnAction::PRIMARY);
	missing_primary_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::ERROR);
	const PolarDB_Query_RoutePlan missing_primary_plan =
		sess.polardb_plan(missing_primary_ctx);
	ok(missing_primary_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			missing_primary_plan.target_hg ==
				missing_primary_ctx.writer_scope.hg,
		"PolarDB policy axes: missing-LSN primary is independent of reader availability fallback");
	PolarDB_Query_RouteCtx invalid_global_ctx = missing_primary_ctx;
	invalid_global_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN);
	invalid_global_ctx.missing_lsn_action =
		static_cast<int>(PolarDB_MissingLsnAction::WARNING);
	invalid_global_ctx.lsn_wait_timeout_action =
		static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY);
	const PolarDB_Query_RoutePlan invalid_global_plan =
		sess.polardb_plan(invalid_global_ctx);
	ok(invalid_global_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::RETURN_ERROR &&
			invalid_global_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::INVALID_POLICY,
		"PolarDB global consistency: planner rejects missing-LSN warning defensively");
	policy_snapshot_ctx.read_target =
		static_cast<int>(PolarDB_ReadTarget::PRIMARY);
	policy_snapshot_ctx.read_fallback_action =
		static_cast<int>(PolarDB_ReadFallbackAction::PRIMARY);
	policy_snapshot_ctx.missing_lsn_action =
		static_cast<int>(PolarDB_MissingLsnAction::PRIMARY);
	policy_snapshot_ctx.lsn_wait_timeout_action =
		static_cast<int>(PolarDB_LsnWaitTimeoutAction::PRIMARY);
	policy_snapshot_ctx.replica_loss_action = static_cast<int>(
		PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY);
	policy_snapshot_ctx.replica_error_action =
		static_cast<int>(PolarDB_ReplicaErrorAction::PRIMARY);
	ok(policy_snapshot_plan.reader.read_target ==
			static_cast<int>(PolarDB_ReadTarget::REPLICA) &&
			policy_snapshot_plan.reader.read_fallback_action ==
				static_cast<int>(PolarDB_ReadFallbackAction::ERROR) &&
			policy_snapshot_plan.reader.missing_lsn_action ==
				static_cast<int>(PolarDB_MissingLsnAction::ERROR) &&
			policy_snapshot_plan.reader.lsn_wait_timeout_action ==
				static_cast<int>(
					PolarDB_LsnWaitTimeoutAction::DISCONNECT) &&
			policy_snapshot_plan.reader.replica_loss_action ==
				static_cast<int>(
					PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR) &&
			policy_snapshot_plan.reader.replica_error_action ==
				static_cast<int>(
					PolarDB_ReplicaErrorAction::DISCONNECT),
		"PolarDB policy snapshot: later source changes do not alter the query plan");
	ok(first.writer_scope.matches(second.writer_scope) &&
			first.reader_hg == second.reader_hg &&
			first.effective_consistency_mode ==
				second.effective_consistency_mode,
		"PolarDB collect snapshot: repeated collect returns stable route inputs");
	ok(first.session.write_lsn == before_session.write_lsn &&
			first.session.observed_lsn == before_session.observed_lsn,
		"PolarDB collect snapshot: collected session LSNs match source state");
	ok(sess.polardb_session_consistency.write_lsn == before_session.write_lsn &&
			sess.polardb_session_consistency.observed_lsn ==
				before_session.observed_lsn &&
			sess.polardb_txn_wait_safety.local_state_changed ==
				before_local_state,
		"PolarDB collect snapshot: collect does not mutate session state");

	PolarDB_Query_RouteCtx read_only_ctx;
	read_only_ctx.is_polar_hg = true;
	read_only_ctx.replica_eligible = true;
	read_only_ctx.writer_scope = PolarDB_WriterScope{
		writer_cfg.writer_hostgroup,
		writer_cfg.writer_epoch};
	read_only_ctx.session.writer_scope = read_only_ctx.writer_scope;
	read_only_ctx.session.observed_lsn = 0x2220;
	read_only_ctx.reader_hg = reader_hg;
	read_only_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	const PolarDB_Query_RoutePlan read_only_plan =
		sess.polardb_plan(read_only_ctx);
	ok(read_only_ctx.session.write_lsn == 0 &&
			read_only_plan.action ==
				PolarDB_Query_RoutePlan::RouteAction::REPLICA_WITH_WAIT &&
			read_only_plan.wait_spec.target ==
				read_only_ctx.session.observed_lsn,
		"PolarDB session consistency: later autocommit read preserves the last observed LSN");

	PolarDB_Query_RouteCtx txn_ctx;
	txn_ctx.is_polar_hg = true;
	txn_ctx.replica_eligible = true;
	txn_ctx.in_transaction = true;
	txn_ctx.txn_split_enabled = true;
	txn_ctx.is_txn_split_safe_read = true;
	txn_ctx.txn_reader_wait_isolation_read_committed = true;
	txn_ctx.txn_reader_wait_local_state_clean = true;
	txn_ctx.writer_scope = PolarDB_WriterScope{
		writer_cfg.writer_hostgroup,
		writer_cfg.writer_epoch};
	txn_ctx.session.writer_scope = txn_ctx.writer_scope;
	txn_ctx.session.write_lsn = 0x2110;
	txn_ctx.reader_hg = reader_hg;
	txn_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	txn_ctx.transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	txn_ctx.transaction_split.primary_lsn = 0x2110;
	txn_ctx.transaction_split.splittable = true;
	const PolarDB_ThreadCounterSnapshot no_marker_rejection(
		worker.get(), polardb_st_var_split_rejected_no_marker);
	PolarDB_Query_RoutePlan txn_plan = sess.polardb_plan(txn_ctx);
	ok(txn_plan.txn_wait_read && txn_plan.target_hg == reader_hg &&
			no_marker_rejection.delta() == 0,
		"PolarDB transaction wait safety: explicit empty-XID split marker permits a pre-write reader without a rejection count");

	txn_ctx.transaction_split.splittable = false;
	txn_plan = sess.polardb_plan(txn_ctx);
	const PolarDB_Query_RoutePlan repeated_txn_plan =
		sess.polardb_plan(txn_ctx);
	ok(txn_plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			txn_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION &&
			repeated_txn_plan.action == txn_plan.action &&
			repeated_txn_plan.action_reason == txn_plan.action_reason &&
			txn_plan.target_hg == writer_cfg.writer_hostgroup &&
			no_marker_rejection.delta() == 0,
		"PolarDB route planning: repeated planning is stable and does not change counters");
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(no_marker_rejection.delta() == 1,
		"PolarDB route accounting: an empty-XID rejection is counted once");

	txn_ctx.transaction_split.splittable = true;
	txn_ctx.txn_reader_wait_local_state_clean = false;
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			no_marker_rejection.delta() == 1,
		"PolarDB transaction wait safety: a valid pre-write marker blocked by local state does not count as a missing marker");

	txn_ctx.txn_reader_wait_local_state_clean = true;
	txn_ctx.transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	txn_ctx.transaction_split.xids = "10,11";
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action == PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT &&
			no_marker_rejection.delta() == 1,
		"PolarDB transaction wait safety: complete backend split metadata permits later replica reads without a rejection count");

	const uint64_t wal_pending_lsn = 0x2330;
	txn_ctx.transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	txn_ctx.transaction_split.primary_lsn = wal_pending_lsn;
	txn_ctx.transaction_split.splittable = false;
	txn_ctx.transaction_split.wal_pending = true;
	writer_hgc->repl_config.polardb_max_replica_replay_lsn->store(
		wal_pending_lsn - 1, std::memory_order_relaxed);
	const PolarDB_ThreadCounterSnapshot wal_pending_rejection(
		worker.get(), polardb_st_var_split_wal_pending);
	const PolarDB_ThreadCounterSnapshot wal_pending_confirmed(
		worker.get(), polardb_st_var_split_wal_pending_replica_confirmed);
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			txn_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::WAL_PENDING &&
			wal_pending_rejection.delta() == 1 &&
			wal_pending_confirmed.delta() == 0,
		"PolarDB WAL-pending route: replay below the transaction LSN stays on primary");

	writer_hgc->repl_config.polardb_max_replica_replay_lsn->store(
		wal_pending_lsn, std::memory_order_relaxed);
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT &&
			txn_plan.wait_spec.type == PolarDB_WaitType::LSN &&
			txn_plan.wait_spec.target == wal_pending_lsn &&
			txn_plan.txn_xids == txn_ctx.transaction_split.xids &&
			txn_plan.reader.require_replica &&
			wal_pending_rejection.delta() == 1 &&
			wal_pending_confirmed.delta() == 1,
		"PolarDB WAL-pending route: replica replay confirmation keeps XIDs and the strict selected-replica LSN target");
	txn_ctx.transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::REPLICA_TXN_SPLIT &&
			wal_pending_rejection.delta() == 1 &&
			wal_pending_confirmed.delta() == 2,
		"PolarDB WAL-pending route: replica replay confirmation permits consecutive split reads");

	txn_ctx.transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_ON_PRIMARY;
	txn_ctx.transaction_split.failed = true;
	txn_plan = sess.polardb_plan(txn_ctx);
	sess.polardb_report_route_result(txn_plan, txn_ctx);
	ok(txn_plan.action == PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			txn_plan.action_reason ==
				PolarDB_Query_RoutePlan::RouteActionReason::IN_TRANSACTION &&
			wal_pending_confirmed.delta() == 2,
		"PolarDB WAL-pending route: failed transaction cannot use replica replay confirmation");
	writer_hgc->repl_config.polardb_max_replica_replay_lsn->store(
		0, std::memory_order_relaxed);
	txn_ctx.transaction_split.failed = false;
	txn_ctx.transaction_split.wal_pending = false;

	sess.polardb_observe_route_inputs(writer_hg);
	ok(sess.polardb_query.profile_enabled &&
			sess.polardb_query.data_path_enabled() &&
			sess.polardb_query.effective_consistency_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN) &&
			sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: request captures active LSN and transaction-split policy");
	sess.polardb_query.reset_for_new_query();
	ok(!sess.polardb_query.profile_enabled &&
			!sess.polardb_query.data_path_enabled() &&
			sess.polardb_query.effective_consistency_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::OFF) &&
			!sess.polardb_query.txn_split_enabled &&
			!sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: query reset clears the captured request policy");
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::OFF);
	sess.polardb_observe_route_inputs(writer_hg);
	ok(sess.polardb_query.profile_enabled &&
			!sess.polardb_query.data_path_enabled() &&
			sess.polardb_query.effective_consistency_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::OFF) &&
			!sess.polardb_query.backend_isolation_status_needed,
		"PolarDB observe: custom consistency off remains an active, overridable policy");
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	sess.polardb_txn_has_no_write_xids = true;
	sess.polardb_transaction_split.stage =
		PolarDB_TransactionSplitStage::TXN_SPLITTABLE;
	sess.polardb_txn_reader_failure.set_force_writer(writer_hg);
	pgsql_thread___polardb_profile_off = true;
	sess.polardb_observe_route_inputs(writer_hg);
	ok(!sess.polardb_query.profile_enabled &&
			!sess.polardb_query.data_path_enabled() &&
			sess.polardb_query.effective_consistency_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::OFF) &&
			!sess.polardb_query.txn_split_enabled &&
			!sess.polardb_txn_has_no_write_xids &&
			!sess.polardb_transaction_split.active() &&
			!sess.polardb_txn_reader_failure.active(),
		"PolarDB observe: named profile off overrides the session and clears split state");
	sess.current_hostgroup = writer_hg;
	const PolarDB_WaitSpec stale_extended_wait =
		PolarDB_WaitSpec::from_lsn(
			0x2230, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
			PolarDB_WaitMode::STRICT);
	sess.polardb_query.reader_plan.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	sess.polardb_query.reader_wait_spec = stale_extended_wait;
	sess.polardb_query.wait.prepare_from_spec(stale_extended_wait);
	sess.polardb_query.wait.wait_stage = PolarDB_WaitStage::WAITING;
	sess.polardb_query.wait_bypass_target = stale_extended_wait.target;
	const unsigned long long planner_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_route_planner_total];
	const bool extended_handled = sess.polardb_apply_extended_route();
	ok(!extended_handled &&
			sess.current_hostgroup == writer_hg &&
			sess.polardb_query.reader_plan.read_target ==
				static_cast<int>(PolarDB_ReadTarget::PRIMARY) &&
			!sess.polardb_query.reader_wait_spec.has_wait() &&
			!sess.polardb_query.wait.spec.has_wait() &&
			sess.polardb_query.wait.wait_stage ==
				PolarDB_WaitStage::IDLE &&
			sess.polardb_query.wait_bypass_target == 0 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_route_planner_total] == planner_before,
		"PolarDB extended route: named profile off clears every reader artifact and skips the planner");
	sess.polardb_route_state.client_rfq_lsn_requested = true;
	ok(!sess.polardb_query_cache_is_disabled(),
		"PolarDB profile off leaves ordinary query-cache behavior enabled");
	sess.polardb_route_state.client_rfq_lsn_requested = false;
	pgsql_thread___polardb_profile_off = false;

	if (sess.transaction_state_manager) {
		sess.transaction_state_manager->handle_transaction("BEGIN");
		sess.polardb_txn_wait_safety.local_state_changed = false;
		const char proxysql_set[] =
			"SET proxysql.polardb_consistency_mode = session_lsn";
		sess.CurrentQuery.query_parser_free();
		sess.CurrentQuery.begin(
			(unsigned char*)const_cast<char*>(proxysql_set),
			strlen(proxysql_set) + 1,
			false);
		sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SET;
		sess.polardb_observe_route_inputs(writer_hg);
		ok(!sess.polardb_txn_wait_safety.local_state_changed,
			"PolarDB observe: ProxySQL-owned in-transaction SET does not dirty reader-wait state");

		const char backend_set[] = "SET TimeZone = 'UTC'";
		sess.CurrentQuery.query_parser_free();
		sess.CurrentQuery.begin(
			(unsigned char*)const_cast<char*>(backend_set),
			strlen(backend_set) + 1,
			false);
		sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SET;
		sess.polardb_observe_route_inputs(writer_hg);
		ok(sess.polardb_txn_wait_safety.local_state_changed,
			"PolarDB observe: backend-visible in-transaction SET dirties reader-wait state");
	} else {
		ok(1, "PolarDB observe: ProxySQL-owned SET skipped without transaction manager");
		ok(1, "PolarDB observe: backend-visible SET skipped without transaction manager");
	}
}

void run_polardb_consistency_counter_tests() {
	test_reader_target_selection_counter_contract();
	test_wait_histogram_boundary_contract();
}

void run_polardb_consistency_profile_tests() {
#if POLARDB_PROFILE
	test_wait_profile_target_and_counter_contract();
#endif // POLARDB_PROFILE
}

void run_polardb_consistency_target_tests() {
	test_v2_target_reader_keeps_wait_until_lsn_is_reached();
	test_reader_wait_selection_activates_only_when_needed();
}

void run_polardb_consistency_wait_cache_tests() {
	test_successful_wait_cache_advance_requires_active_wait();
	test_wait_wrapper_failure_preserves_prefix();
	test_wait_wrapper_keeps_original_query_alive();
}

void run_polardb_session_state_tests() {
	test_session_route_state_clear_tiers();
	test_notice_queue_state_contract();
	test_user_attributes_are_reapplied_after_reset();
	test_collect_is_const_stable_snapshot();
}

#endif // POLARDB_PROXY
