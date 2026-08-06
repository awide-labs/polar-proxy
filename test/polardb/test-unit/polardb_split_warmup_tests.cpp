/**
 * @file polardb_split_warmup_tests.cpp
 * @brief Transaction-split, retained-reader, and warmup tests.
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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

size_t pgsql_polardb_unit_collect_split_warmup_targets(
		PgSQL_PolarDB_ReaderPool* pool,
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free) {
	pool->polardb_collect_split_warmup_targets_unlocked(
		req, target_requests, found_hostgroup,
		saw_eligible_target, saw_compatible_free);
	return target_requests.size();
}

static PgSQL_SplitWarmupRequest make_unit_warmup_request(
		unsigned int reader_hg,
		unsigned int max_connections_per_request) {
	PolarDB_StartupClientContext startup_client;
	startup_client.identity = unit_proxy_identity();
	return PgSQL_SplitWarmupRequest{
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client,
		monotonic_time(), max_connections_per_request};
}

static void unit_warmup_request_use_connection_state(
		PgSQL_SplitWarmupRequest& req,
		PgSQL_Connection *conn) {
	if (!conn) {
		return;
	}
	req.startup_client = conn->polardb_startup_client;
	req.startup_identity_mode = conn->polardb_startup_identity_mode;
	req.startup_options_hash = unit_reader_pool_options_key(conn);
	req.has_startup_parameters = true;
	req.startup_parameters.assign(PGSQL_NAME_LAST_HIGH_WM, std::string());
	req.startup_parameter_hash.assign(PGSQL_NAME_LAST_HIGH_WM, 0);
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		req.startup_parameters[i] =
			conn->variables[i].value ? conn->variables[i].value : "";
		req.startup_parameter_hash[i] = conn->var_hash[i];
	}
	for (uint32_t idx : conn->dynamic_variables_idx) {
		if (idx <= PGSQL_NAME_LAST_LOW_WM ||
				idx >= PGSQL_NAME_LAST_HIGH_WM ||
				!conn->variables[idx].value ||
				conn->var_hash[idx] == 0) {
			continue;
		}
		req.startup_parameters[idx] = conn->variables[idx].value;
		req.startup_parameter_hash[idx] = conn->var_hash[idx];
	}
}


static void test_txn_split_excludes_writer_as_reader() {
	static constexpr int WRITER_HG = 1284;
	static constexpr int READER_HG = 1285;
	static constexpr int PORT = 19552;
	static constexpr uint64_t TARGET_LSN = 0xF100;
	const char* address = "polardb-dual-role";

	stage_polardb_topology_with_txn_split(
		PgHGM, "PolarDB dual-role transaction split",
		WRITER_HG, address, PORT, READER_HG, address, PORT);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(READER_HG), address, PORT);
	ok(reader != nullptr,
		"PolarDB dual-role transaction split: reader hostgroup entry is available");
	if (!reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);
	PgSQL_Connection* cached = make_cached_reader_connection(reader);
	cached->pgsql_conn = unit_connected_pgconn();
	reader->polardb_current_lsn.store(TARGET_LSN, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	unit_reader_pool_add_matching(reader, cached);

	PolarDB_Query_ReaderPlan ordinary;
	ordinary.fallback_writer_hg = WRITER_HG;
	PolarDB_WaitSpec no_wait;
	ok(PgHGM->polardb_reader_server_can_serve_request(
			READER_HG, reader, ordinary, no_wait),
		"PolarDB dual-role transaction split: ordinary reads allow writer-as-reader");

	PolarDB_Query_RoutePlan split =
		PolarDB_Query_RoutePlan::replica_txn_split(
			READER_HG, WRITER_HG,
			PolarDB_WaitSpec::from_lsn(
				TARGET_LSN, 1000, PolarDB_WaitMode::STRICT),
			"7,100");
	ok(!PgHGM->polardb_reader_server_can_serve_request(
			READER_HG, reader, split.reader, split.wait_spec),
		"PolarDB dual-role transaction split: retained writer endpoint is rejected");
#if POLARDB_PROFILE
	const uint64_t reader_acquire_count_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_reader_acquire_count];
#endif // POLARDB_PROFILE
	PolarDB_ReaderResult split_result =
		PgHGM->polardb_acquire_reader_connection(
			READER_HG, &sess, split.reader, split.wait_spec,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(!split_result.acquired(),
		"PolarDB dual-role transaction split: acquisition cannot return the writer endpoint");

	PolarDB_ReaderResult ordinary_result =
		PgHGM->polardb_acquire_reader_connection(
			READER_HG, &sess, ordinary, no_wait,
			PGSQL_POLARDB_TXN_READER_ONLY_POOLED);
	ok(ordinary_result.acquired() && ordinary_result.conn == cached,
		"PolarDB dual-role transaction split: ordinary acquisition remains unchanged");
#if POLARDB_PROFILE
	ok(worker.polardb_status_variables.stvar[
			polardb_st_var_reader_acquire_count] ==
			reader_acquire_count_before + 2,
		"PolarDB reader acquisition profile: every public call is counted once");
#endif // POLARDB_PROFILE
	if (ordinary_result.conn) {
		const auto writer_cfg =
			PgHGM->get_polardb_hg_config(WRITER_HG);
		PgSQL_Backend* primary_backend =
			sess.find_or_create_backend(WRITER_HG);
		PgSQL_Backend* retained_backend =
			sess.find_or_create_backend(READER_HG);
		sess.mybe = primary_backend;
		retained_backend->server_myds->attach_connection(
			ordinary_result.conn);
		sess.polardb_txn_reader.backend = retained_backend;
		if (writer_cfg.is_polardb_hostgroup) {
			sess.polardb_txn_reader.rfq_writer_scope =
				PolarDB_WriterScope{
					WRITER_HG,
					writer_cfg.writer_epoch};
		}
		PtrSize_t pkt = unit_simple_query_packet("SELECT 1");
		PolarDB_Query_RouteCtx route_ctx;
		route_ctx.in_transaction = true;
		route_ctx.writer_scope =
			sess.polardb_txn_reader.rfq_writer_scope;
		const PolarDB_Query_ExecuteResult execute_result =
			sess.polardb_execute(split, route_ctx, pkt);
		ok(execute_result.final_target_hg == WRITER_HG &&
				pkt.ptr != nullptr &&
				sess.polardb_txn_reader.backend == nullptr,
			"PolarDB dual-role transaction split: retained writer falls back before wrapper dispatch");
		if (pkt.ptr) {
			l_free(pkt.size, pkt.ptr);
		}
		worker.return_local_connections();
		reader->remove_free_connection(ordinary_result.conn);
		delete ordinary_result.conn;
	}
}


static void test_split_warmup_counts_shared_reader_pool_inventory() {
	const int writer_hg = 990;
	const int reader_hg = 991;

	stage_polardb_topology(PgHGM, "PolarDB warmup shared inventory",
		writer_hg, "polardb-warmup-shared-writer", 23732,
		reader_hg, "polardb-warmup-shared-reader", 23733);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-warmup-shared-reader", 23733);
	ok(reader != nullptr,
		"PolarDB warmup shared inventory: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_SplitWarmupRequest req =
		make_unit_warmup_request(reader_hg, 1);
	PgSQL_Connection *shared = make_cached_reader_connection(reader);
	ok(shared != nullptr,
		"PolarDB warmup shared inventory: shared connection fixture is available");
	if (!shared) {
		return;
	}
	unit_warmup_request_use_connection_state(req, shared);
	unit_reader_pool_add_shared(reader, shared);

	std::vector<PgSQL_SplitWarmupRequest> target_requests;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_PolarDB_ReaderPool pool(PgHGM);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, req, target_requests,
		&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();
	ok(found_hostgroup && saw_eligible_target,
		"PolarDB warmup shared inventory: reader hostgroup is eligible");
	ok(saw_compatible_free,
		"PolarDB warmup shared inventory: shared reader-pool backend is counted");
	ok(target_requests.empty(),
		"PolarDB warmup shared inventory: compatible shared backend suppresses new target creation");

	unit_reader_pool_clear_shared(reader, shared);
	delete shared;
}

static void test_split_warmup_ignores_incompatible_shared_inventory() {
	const int writer_hg = 914;
	const int reader_hg = 915;

	stage_polardb_topology(PgHGM, "PolarDB warmup shared mismatch",
		writer_hg, "polardb-warmup-mismatch-writer", 24972,
		reader_hg, "polardb-warmup-mismatch-reader", 24973);

	PgSQL_HGC *reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC *reader =
		find_pgsql_server(reader_hgc, "polardb-warmup-mismatch-reader", 24973);
	ok(reader != nullptr,
		"PolarDB warmup shared mismatch: reader server container is available");
	if (!reader) {
		return;
	}

	PgSQL_SplitWarmupRequest req =
		make_unit_warmup_request(reader_hg, 1);
	PgSQL_Connection *other = make_cached_reader_connection(reader);
	ok(other != nullptr,
		"PolarDB warmup shared mismatch: shared connection fixture is available");
	if (!other) {
		return;
	}
	unit_warmup_request_use_connection_state(req, other);
	other->userinfo->set(
		(char*)"polardb_other_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	unit_reader_pool_add_shared(reader, other);

	std::vector<PgSQL_SplitWarmupRequest> target_requests;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_PolarDB_ReaderPool pool(PgHGM);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, req, target_requests,
		&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();
	ok(found_hostgroup && saw_eligible_target,
		"PolarDB warmup shared mismatch: reader hostgroup is eligible");
	ok(!saw_compatible_free,
		"PolarDB warmup shared mismatch: incompatible shared backend is not counted");
	ok(!target_requests.empty(),
		"PolarDB warmup shared mismatch: incompatible shared backend does not suppress target creation");

	unit_reader_pool_clear_shared(reader, other);
	delete other;
}


static void test_txn_split_xids_cleanup_boundary() {
	auto finish = [](PolarDB_Query_WrapperKind kind, bool consume_wrapper,
			bool query_error) {
		PgSQL_Connection conn(false);
		conn.polardb_txn_split_xids_dirty = true;
		conn.polardb_query_wrap_state.begin(1, kind);
		if (consume_wrapper) {
			conn.polardb_query_wrap_state.consume_successful_wrapper_set();
		}
		if (query_error) {
			conn.set_error(
				PGSQL_ERROR_CODES::ERRCODE_RAISE_EXCEPTION,
				"unit split query error", false);
		}
		conn.async_free_result();
		return conn.polardb_txn_split_xids_dirty;
	};

	ok(!finish(PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT, true, false),
		"PolarDB split cleanup: successful complete wrapper clears split-XID state");
	ok(finish(PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT, false, false),
		"PolarDB split cleanup: incomplete wrapper keeps split-XID state dirty");
	ok(finish(PolarDB_Query_WrapperKind::CONSISTENCY_WAIT, true, false),
		"PolarDB split cleanup: unrelated wrapper cannot clear split-XID state");
	ok(finish(PolarDB_Query_WrapperKind::TXN_SPLIT_WAIT, true, true),
		"PolarDB split cleanup: query error keeps split-XID state dirty");
}

static void unit_parse_xact_rfq(PGconn* conn, char marker,
		const char* xids, uint64_t lsn, char transaction_status = 'T') {
	assert(!marker || xids != nullptr);
	const size_t xids_len = xids ? strlen(xids) : 0;
	const size_t xact_len = marker ? 1 + xids_len + 1 : 0;
	const size_t payload_len = 1 + sizeof(uint64_t) + xact_len;
	const size_t frame_len = 1 + sizeof(uint32_t) + payload_len;
	assert(conn != nullptr && frame_len <= static_cast<size_t>(conn->inBufSize));

	char* p = conn->inBuffer;
	*p++ = 'Z';
	const uint32_t wire_len = static_cast<uint32_t>(
		sizeof(uint32_t) + payload_len);
	for (int shift = 24; shift >= 0; shift -= 8) {
		*p++ = static_cast<char>((wire_len >> shift) & 0xff);
	}
	*p++ = transaction_status;
	for (int shift = 56; shift >= 0; shift -= 8) {
		*p++ = static_cast<char>((lsn >> shift) & 0xff);
	}
	if (marker) {
		*p++ = marker;
		memcpy(p, xids, xids_len + 1);
	}

	conn->inStart = 0;
	conn->inCursor = 0;
	conn->inEnd = static_cast<int>(frame_len);
	conn->asyncStatus = PGASYNC_BUSY;
	conn->pipelineStatus = PQ_PIPELINE_OFF;
	conn->polar_proxy_send_lsn = true;
	conn->polar_proxy_send_xact = true;
	pqParseInput3(conn);
}

static void test_libpq_empty_xact_markers() {
	PGconn* conn = unit_connected_pgconn();
	ok(conn != nullptr,
		"PolarDB libpq RFQ parser: connection-state fixture is available");
	if (!conn) {
		return;
	}

	unit_parse_xact_rfq(conn, 'x', "", 0xE100);
	ok(PQgetLSN(conn) == 0xE100 && PQhasLSN(conn) == 1 &&
			PQtransactionStatus(conn) == PQTRANS_INTRANS,
		"PolarDB libpq RFQ parser: positioned ReadyForQuery state is decoded");
	ok(PQisXactSplittable(conn) == 1 && PQisXactWalPending(conn) == 0 &&
			PQgetXactSplitXids(conn) != nullptr &&
			PQgetXactSplitXids(conn)[0] == '\0',
		"PolarDB libpq RFQ parser: x+empty is explicit splittable pre-write state");

	unit_parse_xact_rfq(conn, 'w', "", 0xE200);
	ok(PQgetLSN(conn) == 0xE200 && PQisXactSplittable(conn) == 0 &&
			PQisXactWalPending(conn) == 1 &&
			PQgetXactSplitXids(conn) != nullptr &&
			PQgetXactSplitXids(conn)[0] == '\0',
		"PolarDB libpq RFQ parser: w+empty preserves WAL-pending state");

	unit_parse_xact_rfq(conn, 'x', "7,100,101", 0xE300);
	ok(PQisXactSplittable(conn) == 1 && PQisXactWalPending(conn) == 0 &&
			strcmp(PQgetXactSplitXids(conn), "7,100,101") == 0,
		"PolarDB libpq RFQ parser: nonempty split XIDs remain intact");

	unit_parse_xact_rfq(conn, 0, nullptr, 0xE400);
	ok(PQgetLSN(conn) == 0xE400 && PQisXactSplittable(conn) == 0 &&
			PQisXactWalPending(conn) == 0 &&
			PQgetXactSplitXids(conn) == nullptr,
		"PolarDB libpq RFQ parser: absent marker clears prior split state and means hard-unsplittable");

	PQfinish(conn);
}

static void unit_set_current_query(PgSQL_Session& sess,
		const char* query, PGSQL_QUERY_command command) {
	sess.polardb_query.keep_session_lsn = false;
	sess.status = PROCESSING_QUERY;
	sess.CurrentQuery.QueryPointer = reinterpret_cast<unsigned char*>(
		const_cast<char*>(query));
	sess.CurrentQuery.QueryLength = static_cast<int>(strlen(query));
	sess.CurrentQuery.PgQueryCmd = command;
}

static void test_read_only_txn_end_keeps_session_lsn() {
	static constexpr int WRITER_HG = 1280;
	static constexpr int READER_HG = 1281;
	stage_polardb_topology_with_txn_split(
		PgHGM, "PolarDB read-only transaction RFQ",
		WRITER_HG, "polardb-read-only-writer", 19532,
		READER_HG, "polardb-read-only-reader", 19533);
	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(WRITER_HG),
		"polardb-read-only-writer", 19532);
	const auto writer_cfg = PgHGM->get_polardb_hg_config(WRITER_HG);
	ok(writer && writer_cfg.is_polardb_hostgroup &&
			writer_cfg.writer_hostgroup == WRITER_HG,
		"PolarDB read-only transaction RFQ: writer fixture is available");
	if (!writer || !writer_cfg.is_polardb_hostgroup ||
			writer_cfg.writer_hostgroup != WRITER_HG) {
		return;
	}

	PgSQL_Thread worker;
	worker.curtime = monotonic_time();
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);
	sess.connections_handler = true;
	sess.polardb_config.is_polardb_enabled = true;
	sess.polardb_route_state.client_rfq_lsn_requested = true;
	sess.polardb_query.profile_enabled = true;
	sess.polardb_query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	sess.polardb_query.txn_split_enabled = true;
	pgsql_thread___polardb_profile_off = false;
	const PolarDB_WriterScope scope{
		writer_cfg.writer_hostgroup,
		writer_cfg.writer_epoch};
	sess.polardb_query.request_writer_scope = scope;
	sess.polardb_session_consistency.writer_scope = scope;
	sess.polardb_session_consistency.write_lsn = 0x100;

	PgSQL_Data_Stream backend_myds;
	backend_myds.sess = &sess;
	PgSQL_Connection* backend_conn = make_cached_reader_connection(writer);
	backend_conn->pgsql_conn = unit_connected_pgconn();
	backend_conn->myds = &backend_myds;
	backend_myds.myconn = backend_conn;

	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0x900);
	uint64_t client_lsn = 0;
	bool include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0x900, &client_lsn);
	ok(include_client_lsn && client_lsn == 0x100 &&
			sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: BEGIN keeps the frontend LSN");
	sess.polardb_process_result(&backend_myds, "BEGIN", PGSQL_QUERY_BEGIN);
	ok(sess.polardb_txn_has_no_write_xids &&
			sess.polardb_session_consistency.target() == 0x100 &&
			sess.polardb_transaction_split.primary_lsn == 0,
		"PolarDB read-only transaction RFQ: BEGIN hint does not advance the session LSN");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0x900,
		"PolarDB read-only transaction RFQ: BEGIN hint still advances the server cache");

	unit_set_current_query(sess, "SELECT 1", PGSQL_QUERY_SELECT);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xA00);
	client_lsn = 0;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xA00, &client_lsn);
	ok(include_client_lsn && client_lsn == 0x100,
		"PolarDB read-only transaction RFQ: read hint keeps the client LSN");
	sess.polardb_process_result(&backend_myds, "SELECT ?", PGSQL_QUERY_SELECT);
	ok(sess.polardb_txn_has_no_write_xids &&
			sess.polardb_session_consistency.target() == 0x100 &&
			sess.polardb_transaction_split.primary_lsn == 0 &&
			writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0xA00,
		"PolarDB read-only transaction RFQ: read hint updates only the server cache");

	unit_set_current_query(sess, "COMMIT", PGSQL_QUERY_COMMIT);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 0, nullptr, 0xB00, 'I');
	client_lsn = 0;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xB00, &client_lsn);
	ok(include_client_lsn && client_lsn == 0x100 &&
			sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: client keeps its session LSN at COMMIT");

	sess.polardb_process_result(&backend_myds, "COMMIT", PGSQL_QUERY_COMMIT);
	ok(sess.polardb_session_consistency.target() == 0x100 &&
			!sess.polardb_txn_has_no_write_xids,
		"PolarDB read-only transaction RFQ: COMMIT ignores the unrelated LSN and ends transaction state");
	ok(writer->polardb_current_lsn.load(std::memory_order_relaxed) == 0xB00,
		"PolarDB read-only transaction RFQ: server cache still records the backend LSN");

	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xB00);
	client_lsn = 0;
	(void)sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xB00, &client_lsn);
	sess.polardb_process_result(&backend_myds, "BEGIN", PGSQL_QUERY_BEGIN);
	unit_set_current_query(sess, "COMMIT", PGSQL_QUERY_COMMIT);
	sess.status = PROCESSING_STMT_EXECUTE;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 0, nullptr, 0xB80, 'I');
	client_lsn = 0;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xB80, &client_lsn);
	ok(include_client_lsn && client_lsn == 0x100 &&
			sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: extended COMMIT keeps the frontend LSN");
	sess.polardb_process_result(&backend_myds, "COMMIT", PGSQL_QUERY_COMMIT);
	ok(sess.polardb_session_consistency.target() == 0x100 &&
			!sess.polardb_txn_has_no_write_xids,
		"PolarDB read-only transaction RFQ: extended COMMIT preserves session consistency");

	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xB00);
	client_lsn = 0;
	(void)sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xB00, &client_lsn);
	sess.polardb_process_result(&backend_myds, "BEGIN", PGSQL_QUERY_BEGIN);
	unit_set_current_query(sess, "INSERT INTO t VALUES (1)", PGSQL_QUERY_INSERT);
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "7,100", 0xC00);
	sess.polardb_process_result(
		&backend_myds, "INSERT INTO t VALUES (?)", PGSQL_QUERY_INSERT);
	ok(!sess.polardb_txn_has_no_write_xids &&
			sess.polardb_session_consistency.write_lsn == 0xC00 &&
			sess.polardb_transaction_split.primary_lsn == 0xC00,
		"PolarDB read-only transaction RFQ: a write marks the transaction as not read-only");

	unit_set_current_query(sess, "INSERT INTO t VALUES (2)", PGSQL_QUERY_INSERT);
	sess.polardb_query.keep_session_lsn = true;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xD00);
	sess.polardb_process_result(
		&backend_myds, "INSERT INTO t VALUES (?)", PGSQL_QUERY_INSERT);
	ok(sess.polardb_session_consistency.write_lsn == 0xD00 &&
			sess.polardb_transaction_split.primary_lsn == 0xD00,
		"PolarDB read-only transaction RFQ: a write never suppresses its own LSN");

	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	sess.polardb_query.request_writer_scope = scope;
	sess.polardb_session_consistency.writer_scope = scope;
	sess.polardb_session_consistency.write_lsn = 0;
	sess.polardb_session_consistency.observed_lsn = 0;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xD80);
	client_lsn = UINT64_MAX;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xD80, &client_lsn);
	ok(include_client_lsn && client_lsn == 0xD80 &&
			sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: empty session scope preserves the backend LSN");

	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	sess.polardb_query.request_writer_scope = scope;
	sess.polardb_session_consistency.writer_scope =
		PolarDB_WriterScope{scope.hg, scope.epoch + 1};
	sess.polardb_session_consistency.write_lsn = 0;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xE00);
	client_lsn = 0;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xE00, &client_lsn);
	ok(include_client_lsn && client_lsn == 0xE00 &&
			!sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: a stale session scope preserves the current backend LSN");

	const unsigned long long raised_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_client_rfq_lsn_raised_to_target];
	const unsigned long long writer_raised_before =
		worker.polardb_status_variables.stvar[
			polardb_st_var_client_rfq_lsn_raised_by_writer];
	unit_set_current_query(sess, "BEGIN", PGSQL_QUERY_BEGIN);
	sess.polardb_query.request_writer_scope = scope;
	sess.polardb_session_consistency.writer_scope = scope;
	sess.polardb_session_consistency.write_lsn = 0xF00;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 'x', "", 0xE80);
	client_lsn = 0;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0xE80, &client_lsn);
	ok(include_client_lsn && client_lsn == 0xF00 &&
			sess.polardb_query.keep_session_lsn,
		"PolarDB read-only transaction RFQ: saved position remains the client baseline");
	ok(worker.polardb_status_variables.stvar[
				polardb_st_var_client_rfq_lsn_raised_to_target] ==
			raised_before + 1 &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_client_rfq_lsn_raised_by_writer] ==
			writer_raised_before + 1,
		"PolarDB read-only transaction RFQ: raise counters compare against the raw backend LSN");

	unit_set_current_query(
		sess, "INSERT INTO t VALUES (3)", PGSQL_QUERY_INSERT);
	sess.polardb_query.request_writer_scope = scope;
	sess.polardb_session_consistency.writer_scope = scope;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 0, nullptr, 0x1000, 'I');
	pgsql_thread___polardb_profile_off = true;
	sess.polardb_process_result(
		&backend_myds, "INSERT INTO t VALUES (?)", PGSQL_QUERY_INSERT);
	ok(sess.polardb_session_consistency.write_lsn == 0x1000,
		"PolarDB result policy: an in-flight LSN request finishes after profile switches off");

	sess.polardb_query.reset_for_new_query();
	unit_set_current_query(
		sess, "INSERT INTO t VALUES (4)", PGSQL_QUERY_INSERT);
	sess.polardb_query.request_writer_scope = scope;
	unit_parse_xact_rfq(backend_conn->pgsql_conn, 0, nullptr, 0x1100, 'I');
	pgsql_thread___polardb_profile_off = false;
	sess.polardb_process_result(
		&backend_myds, "INSERT INTO t VALUES (?)", PGSQL_QUERY_INSERT);
	ok(sess.polardb_session_consistency.write_lsn == 0x1000,
		"PolarDB result policy: a request started under profile off stays disabled after re-enable");
	client_lsn = UINT64_MAX;
	include_client_lsn = sess.polardb_prepare_client_ready_lsn(
		backend_conn, true, 0x1100, &client_lsn);
	ok(!include_client_lsn && client_lsn == 0 &&
			!sess.polardb_query.keep_session_lsn,
		"PolarDB profile off suppresses client ReadyForQuery LSN output");

	backend_myds.myconn = nullptr;
	delete backend_conn;
}

static void test_wait_bypass_target_reaches_client_rfq() {
	static constexpr int WRITER_HG = 1286;
	static constexpr int READER_HG = 1287;
	static constexpr uint64_t BACKEND_LSN = 0x100;
	static constexpr uint64_t TARGET_LSN = 0x200;

	stage_polardb_topology_with_txn_split(
		PgHGM, "PolarDB wait bypass client RFQ",
		WRITER_HG, "polardb-bypass-writer", 19562,
		READER_HG, "polardb-bypass-reader", 19563);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(READER_HG),
		"polardb-bypass-reader", 19563);
	const auto writer_cfg = PgHGM->get_polardb_hg_config(WRITER_HG);
	ok(reader != nullptr && writer_cfg.is_polardb_hostgroup,
		"PolarDB wait bypass client RFQ: reader fixture is available");
	if (!reader || !writer_cfg.is_polardb_hostgroup) {
		return;
	}
	const PolarDB_WriterScope writer_scope{
		writer_cfg.writer_hostgroup, writer_cfg.writer_epoch};

	PgSQL_Thread worker;
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);
	sess.connections_handler = true;
	sess.status = PROCESSING_QUERY;
	sess.polardb_route_state.client_rfq_lsn_requested = true;
	sess.polardb_query.profile_enabled = true;
	sess.polardb_query.request_writer_scope = writer_scope;
	sess.polardb_query.wait_bypass_target = TARGET_LSN;

	PgSQL_Data_Stream backend_myds;
	backend_myds.sess = &sess;
	PgSQL_Connection* backend_conn = make_cached_reader_connection(reader);
	backend_conn->pgsql_conn = unit_connected_pgconn();
	backend_conn->myds = &backend_myds;
	backend_myds.myconn = backend_conn;

	uint64_t client_lsn = 0;
	ok(sess.polardb_prepare_client_ready_lsn(
			backend_conn, true, BACKEND_LSN, &client_lsn) &&
			client_lsn == TARGET_LSN,
		"PolarDB wait bypass client RFQ: confirmed target is returned without a wrapper");

	sess.polardb_query.reset_for_new_query();
	sess.polardb_query.profile_enabled = true;
	sess.polardb_query.request_writer_scope = writer_scope;
	client_lsn = 0;
	ok(sess.polardb_prepare_client_ready_lsn(
			backend_conn, true, BACKEND_LSN, &client_lsn) &&
			client_lsn == BACKEND_LSN,
		"PolarDB wait bypass client RFQ: next query does not reuse the prior target");

	unit_parse_rfq_lsn(backend_conn->pgsql_conn, BACKEND_LSN);
	const auto scope_mismatch =
		PolarDB_SessionUnitAccess::exercise_deferred_scope_mismatch(
			&sess, &backend_myds, backend_conn, WRITER_HG,
			writer_scope.epoch, BACKEND_LSN, TARGET_LSN);
	ok(scope_mismatch.aggregate_target_invalidated &&
			scope_mismatch.direct_rfq_uses_current_backend_lsn &&
			scope_mismatch.standalone_sync_uses_current_backend_lsn &&
			scope_mismatch.released_backend_sync_uses_retained_lsn &&
			scope_mismatch.attached_backend_sync_uses_retained_lsn &&
			scope_mismatch.emitter_uses_current_backend_lsn,
		"PolarDB deferred RFQ: retained values survive backend release or reuse while writer-epoch mismatch cannot publish an old target");

	backend_myds.myconn = nullptr;
	delete backend_conn;
}

static void test_wait_bypass_reset_retry_uses_writer() {
	static constexpr int WRITER_HG = 1288;
	static constexpr int READER_HG = 1289;
	static constexpr uint64_t TARGET_LSN = 0x300;

	stage_polardb_topology_with_txn_split(
		PgHGM, "PolarDB wait bypass reset retry",
		WRITER_HG, "polardb-reset-retry-writer", 19564,
		READER_HG, "polardb-reset-retry-reader", 19565);

	PgSQL_Thread worker;
	worker.curtime = 5000000;
	PgSQL_Session sess;
	attach_test_frontend(sess, &worker);
	PgSQL_Backend* reader_backend =
		sess.find_or_create_backend(READER_HG);
	PgSQL_Backend* writer_backend =
		sess.find_or_create_backend(WRITER_HG);
	ok(reader_backend && writer_backend,
		"PolarDB wait bypass reset retry: backend fixtures are available");
	if (!reader_backend || !writer_backend) {
		return;
	}
	PgSQL_Data_Stream* reader_myds = reader_backend->server_myds;
	PgSQL_Data_Stream* writer_myds = writer_backend->server_myds;
	ok(reader_myds->killed_at == 0 && writer_myds->killed_at == 0,
		"PolarDB backend retry: fresh streams have clear cancellation state");
	const int saved_connect_timeout =
		pgsql_thread___connect_timeout_server_max;
	pgsql_thread___connect_timeout_server_max = 10000;
	const uint64_t expected_writer_deadline =
		worker.curtime + 10000ULL * 1000ULL;

	auto verify_redirect = [&](const char* reason, const char* label) {
		sess.current_hostgroup = READER_HG;
		sess.mybe = reader_backend;
		reader_myds->query_retries_on_failure = 3;
		reader_myds->connect_retries_on_failure = 4;
		reader_myds->max_connect_time = worker.curtime - 1;
		reader_myds->wait_until = worker.curtime - 1;
		reader_myds->killed_at = worker.curtime - 1;
		reader_myds->kill_type = 1;
		reader_myds->cancel_query = true;
		writer_myds->query_retries_on_failure = 91;
		writer_myds->connect_retries_on_failure = 92;
		writer_myds->max_connect_time = worker.curtime - 1;
		writer_myds->wait_until = worker.curtime - 1;
		writer_myds->killed_at = worker.curtime - 1;
		writer_myds->kill_type = 1;
		writer_myds->cancel_query = true;
		sess.polardb_query.reader_plan.fallback_writer_hg = WRITER_HG;
		sess.polardb_query.reader_plan.replica_loss_action =
			static_cast<int>(
				PolarDB_ReplicaLossAction::REPLICA_THEN_PRIMARY);
		sess.polardb_query.wait.prepare_from_spec(
			PolarDB_WaitSpec::from_lsn(
				TARGET_LSN, 1000, PolarDB_WaitMode::STRICT));
		sess.polardb_query.wait_bypass_target = TARGET_LSN;

		const bool retry =
			sess.polardb_redirect_wait_retry_to_writer(true, reason);
		ok(retry && sess.current_hostgroup == WRITER_HG &&
				sess.mybe == writer_backend &&
				reader_myds->query_retries_on_failure == 0 &&
				reader_myds->connect_retries_on_failure == 0 &&
				reader_myds->max_connect_time == 0 &&
				reader_myds->wait_until == 0 &&
				reader_myds->killed_at == 0 &&
				reader_myds->kill_type == 0 &&
				!reader_myds->cancel_query &&
				writer_myds->query_retries_on_failure == 3 &&
				writer_myds->connect_retries_on_failure == 4 &&
				writer_myds->max_connect_time == expected_writer_deadline &&
				writer_myds->wait_until == 0 &&
				writer_myds->killed_at == 0 &&
				writer_myds->kill_type == 0 &&
				!writer_myds->cancel_query &&
				sess.polardb_query.reader_plan.fallback_writer_hg == -1 &&
				!sess.polardb_query.reader_plan.require_replica &&
				!sess.polardb_query.wait.spec.has_wait(),
			"%s", label);
		sess.polardb_query.reset_for_new_query();
	};
	auto verify_error = [&](PolarDB_ReplicaLossAction action,
			const char* label) {
		sess.current_hostgroup = READER_HG;
		sess.mybe = reader_backend;
		sess.polardb_query.reader_plan.fallback_writer_hg = WRITER_HG;
		sess.polardb_query.reader_plan.replica_loss_action =
			static_cast<int>(action);
		sess.polardb_query.wait_bypass_target = TARGET_LSN;

		const bool retry =
			sess.polardb_redirect_wait_retry_to_writer(
				true, "reset-compatible reader failure");
		ok(!retry && sess.current_hostgroup == READER_HG &&
				sess.mybe == reader_backend,
			"%s", label);
		sess.polardb_query.reset_for_new_query();
	};

	verify_redirect(
		"reset-compatible reader failure",
		"PolarDB wait bypass reset retry: reset failure redirects to writer");
	verify_redirect(
		"reset-compatible reader timeout",
		"PolarDB wait bypass reset retry: reset timeout redirects to writer");
	verify_error(
		PolarDB_ReplicaLossAction::REPLICA_THEN_ERROR,
		"PolarDB wait bypass reset retry: replica_then_error does not use primary");
	verify_error(
		PolarDB_ReplicaLossAction::ERROR,
		"PolarDB wait bypass reset retry: error does not use primary");
	pgsql_thread___connect_timeout_server_max = saved_connect_timeout;
}

static void test_retained_txn_reader_wait_bypass_contract() {
	static constexpr int WRITER_HG = 1282;
	static constexpr int READER_HG = 1283;
	static constexpr uint64_t TARGET_LSN = 0xD100;
	enum class Case {
		MATCH,
		BELOW_TARGET,
		MISSING_RFQ,
		WRITER_EPOCH,
		LIVE_WRITER_EPOCH,
		STARTUP_SETTINGS,
		STARTUP_GENERATION,
		STARTUP_PROFILE,
		STARTUP_IDENTITY,
		UNINSTALLED_STARTUP_PROFILE,
		OFFLINE,
		LATENCY,
		LAG_CAP,
		NO_RETAINED_PROOF,
		RETRY_EXCLUDED,
		SPLIT_MATCH,
	};

	stage_polardb_topology_with_txn_split(
		PgHGM, "PolarDB retained txn reader bypass",
		WRITER_HG, "polardb-retained-writer", 19542,
		READER_HG, "polardb-retained-reader", 19543);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(READER_HG),
		"polardb-retained-reader", 19543);
	PgSQL_HGC* writer_hgc = PgHGM->MyHGC_lookup(WRITER_HG);
	const auto writer_cfg = PgHGM->get_polardb_hg_config(WRITER_HG);
	ok(reader && writer_hgc && writer_cfg.is_polardb_hostgroup &&
			writer_cfg.writer_hostgroup == WRITER_HG && GloPTH,
		"PolarDB retained txn reader bypass: topology fixture is available");
	if (!reader || !writer_hgc || !writer_cfg.is_polardb_hostgroup ||
			writer_cfg.writer_hostgroup != WRITER_HG || !GloPTH) {
		return;
	}

	const PolarDB_WriterScope writer_scope{
		writer_cfg.writer_hostgroup,
		writer_cfg.writer_epoch};
	auto run_case = [&](Case test_case, bool expect_bypass,
			const char* label) {
		PgSQL_Thread worker;
		PgSQL_Session sess;
		attach_test_frontend(sess, &worker);
		PgSQL_Backend* writer_backend =
			sess.find_or_create_backend(WRITER_HG);
		PgSQL_Backend* reader_backend =
			sess.find_or_create_backend(READER_HG);
		PgSQL_Connection* conn = make_cached_reader_connection(reader);
		conn->pgsql_conn = unit_connected_pgconn();
		if (!writer_backend || !reader_backend ||
				!reader_backend->server_myds || !conn->pgsql_conn) {
			ok(false, "%s", label);
			delete conn;
			return;
		}
		sess.mybe = writer_backend;
		conn->pgsql_conn->polar_has_lsn = true;
		conn->pgsql_conn->polar_last_lsn = TARGET_LSN;
		const PgSQL_PoolMatchKey match_key =
			unit_reader_pool_match_key(conn);
		if (!reader->add_used_matching_connection(conn, match_key)) {
			ok(false, "%s", label);
			delete conn;
			return;
		}
		reader_backend->server_myds->attach_connection(conn);
		sess.polardb_txn_reader.backend = reader_backend;
		sess.polardb_txn_reader.rfq_writer_scope = writer_scope;

		PolarDB_Query_RoutePlan plan;
		plan.target_hg = READER_HG;
		plan.txn_wait_read = true;
		plan.wait_spec = PolarDB_WaitSpec::from_lsn(
			TARGET_LSN, 1000, PolarDB_WaitMode::STRICT);
		plan.reader.fallback_writer_hg = WRITER_HG;
		const bool split =
			test_case == Case::RETRY_EXCLUDED ||
			test_case == Case::SPLIT_MATCH;
		PgSQL_Connection* replacement_conn = nullptr;
		if (split) {
			plan = PolarDB_Query_RoutePlan::replica_txn_split(
				READER_HG, WRITER_HG, plan.wait_spec, "10,11");
		}
		PolarDB_Query_RouteCtx route_ctx;
		route_ctx.in_transaction = true;
		route_ctx.writer_scope = writer_scope;
		const int saved_default_max_latency_ms =
			pgsql_thread___default_max_latency_ms;
		auto set_current_startup_settings = [&]() {
			const PolarDB_ParsedGlobalConfigValue global_config =
				GloPTH->get_polardb_global_config();
			const int identity_mode =
				static_cast<int>(
					global_config.startup.identity_mode);
			const PolarDB_StartupProfile profile =
				PgHGM->polardb_startup_profile_for_hostgroup(
					READER_HG,
					static_cast<int>(
						global_config.startup.proxy_protocol));
			const PolarDB_StartupClientContext startup_client =
				conn->polardb_startup_client;
			conn->polardb_selected_server_snapshot =
				PgHGM->get_polardb_server_list_snapshot();
			conn->set_polardb_startup_settings(
				profile, identity_mode,
				global_config.startup.generation,
				startup_client);
		};
		// Begin with a connection opened under the current startup settings.
		// Each negative case below then changes only the condition it tests.
		set_current_startup_settings();

		switch (test_case) {
		case Case::BELOW_TARGET:
			conn->pgsql_conn->polar_last_lsn = TARGET_LSN - 1;
			break;
		case Case::MISSING_RFQ:
			conn->pgsql_conn->polar_has_lsn = false;
			break;
		case Case::WRITER_EPOCH:
			sess.polardb_txn_reader.rfq_writer_scope =
				PolarDB_WriterScope{WRITER_HG, writer_scope.epoch + 1};
			break;
		case Case::LIVE_WRITER_EPOCH:
			writer_hgc->repl_config.polardb_writer_epoch->store(
				writer_scope.epoch + 1, std::memory_order_release);
			break;
		case Case::STARTUP_SETTINGS:
			break;
		case Case::STARTUP_GENERATION:
			conn->polardb_startup_config_generation++;
			break;
		case Case::STARTUP_PROFILE:
			conn->polardb_startup_profile =
				PolarDB_StartupProfile::from_protocol(
					PolarDB_ProxyProtocol::OFF);
			conn->polardb_startup_profile_generation =
				conn->polardb_startup_profile.generation(
					conn->polardb_startup_identity_mode);
			break;
		case Case::STARTUP_IDENTITY:
			conn->polardb_startup_identity_mode =
				conn->polardb_startup_identity_mode ==
					static_cast<int>(
						PolarDB_ProxyIdentityMode::CLIENT)
				? static_cast<int>(
					PolarDB_ProxyIdentityMode::PROXY)
				: static_cast<int>(
					PolarDB_ProxyIdentityMode::CLIENT);
			conn->polardb_startup_profile_generation =
				conn->polardb_startup_profile.generation(
					conn->polardb_startup_identity_mode);
			break;
		case Case::UNINSTALLED_STARTUP_PROFILE:
			conn->polardb_startup_settings_set = false;
			conn->polardb_startup_profile =
				PolarDB_StartupProfile::from_protocol(
					PolarDB_ProxyProtocol::OFF);
			conn->polardb_startup_profile_generation =
				conn->polardb_startup_profile.generation(
					conn->polardb_startup_identity_mode);
			break;
		case Case::OFFLINE:
			reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
			break;
		case Case::LATENCY:
			pgsql_thread___default_max_latency_ms = 1;
			reader->set_current_latency_us_value(
				1001);
			break;
		case Case::LAG_CAP:
			plan.reader.group_lsn = TARGET_LSN + 0x100;
			plan.reader.max_lag_bytes = 0x10;
			reader->polardb_current_lsn.store(
				TARGET_LSN, std::memory_order_relaxed);
			reader->lsn_updated_at.store(
				monotonic_time(), std::memory_order_relaxed);
			break;
		case Case::NO_RETAINED_PROOF: {
			reader_backend->server_myds->
				destroy_MySQL_Connection_From_Pool(false);
			conn = make_cached_reader_connection(reader);
			conn->pgsql_conn = unit_connected_pgconn();
			conn->pgsql_conn->polar_has_lsn = true;
			conn->pgsql_conn->polar_last_lsn = TARGET_LSN;
			reader->polardb_current_lsn.store(
				0, std::memory_order_relaxed);
			reader->lsn_updated_at.store(
				0, std::memory_order_relaxed);
			replacement_conn = conn;
			const PgSQL_PoolMatchKey replacement_key =
				unit_reader_pool_match_key(conn);
			if (!reader->add_matching_connection(
					conn, replacement_key)) {
				ok(false, "%s", label);
				delete conn;
				return;
			}
			break;
		}
		case Case::RETRY_EXCLUDED:
			sess.polardb_txn_reader_failure.set_reader_skip(
				READER_HG, reader->address, reader->port);
			break;
		default:
			break;
		}

		const unsigned long long checked_before =
			worker.polardb_status_variables.stvar[
				polardb_st_var_txn_reader_reuse_bypass_checked];
		const unsigned long long allowed_before =
			worker.polardb_status_variables.stvar[
				polardb_st_var_txn_reader_reuse_bypass_allowed];
		const unsigned long long bypassed_before =
			worker.polardb_status_variables.stvar[
				polardb_st_var_wait_wrap_bypassed];
		PtrSize_t pkt = unit_simple_query_packet("SELECT 1");
		const PolarDB_Query_ExecuteResult result =
			sess.polardb_execute(plan, route_ctx, pkt);
		const bool prepared =
			result.final_target_hg == READER_HG &&
			sess.polardb_txn_reader.active();
		const bool bypassed = split
			? sess.polardb_txn_reader.wrapped_query.find(
				"polar_xact_split_wait_lsn") == std::string::npos
			: sess.polardb_query.wait.wait_stage ==
				PolarDB_WaitStage::IDLE;
		const bool wait_present = split
			? sess.polardb_txn_reader.wrapped_query.find(
				"polar_xact_split_wait_lsn") != std::string::npos
			: sess.polardb_query.wait.wait_stage ==
				PolarDB_WaitStage::WAITING;
		const bool replacement_verified =
			test_case != Case::NO_RETAINED_PROOF ||
			(reader_backend->server_myds->myconn == replacement_conn &&
			 !sess.polardb_txn_reader.rfq_writer_scope.valid());
			const bool expect_reuse_check =
				test_case != Case::NO_RETAINED_PROOF;
			const bool expect_fallback =
				test_case == Case::RETRY_EXCLUDED;
			const bool counters_verified =
			worker.polardb_status_variables.stvar[
				polardb_st_var_txn_reader_reuse_bypass_checked] ==
					checked_before + (expect_reuse_check ? 1 : 0) &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_txn_reader_reuse_bypass_allowed] ==
					allowed_before + (expect_bypass ? 1 : 0) &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_wait_wrap_bypassed] ==
					bypassed_before + (expect_bypass ? 1 : 0);
			ok((expect_fallback
					? (!prepared &&
						result.final_target_hg == WRITER_HG &&
						sess.polardb_txn_reader.backend == nullptr)
					: (prepared &&
						(expect_bypass ? bypassed : wait_present))) &&
					replacement_verified &&
					counters_verified &&
					(expect_fallback || !split ||
					 sess.polardb_txn_reader.wrapped_query.find(
						"polar_xact_split_xids") != std::string::npos),
				"%s", label);
		if (!prepared && pkt.ptr) {
			l_free(pkt.size, pkt.ptr);
		}

		reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
		reader->set_current_latency_us_value(0);
		pgsql_thread___default_max_latency_ms =
			saved_default_max_latency_ms;
		if (test_case == Case::LIVE_WRITER_EPOCH) {
			writer_hgc->repl_config.polardb_writer_epoch->store(
				writer_scope.epoch, std::memory_order_release);
		}
		sess.polardb_txn_reader_failure.clear();
	};

	run_case(Case::MATCH, true,
		"PolarDB retained txn reader bypass: matching connection RFQ skips the pre-write wait");
	run_case(Case::BELOW_TARGET, false,
		"PolarDB retained txn reader bypass: below-target RFQ keeps the pre-write wait");
	run_case(Case::MISSING_RFQ, false,
		"PolarDB retained txn reader bypass: missing RFQ payload keeps the pre-write wait");
	run_case(Case::WRITER_EPOCH, false,
		"PolarDB retained txn reader bypass: mismatched RFQ writer epoch keeps the pre-write wait");
	run_case(Case::LIVE_WRITER_EPOCH, false,
		"PolarDB retained txn reader bypass: live writer epoch change keeps the pre-write wait");
	run_case(Case::STARTUP_SETTINGS, true,
		"PolarDB retained txn reader bypass: current startup settings permit bypass");
	run_case(Case::STARTUP_GENERATION, false,
		"PolarDB retained txn reader bypass: stale startup generation keeps the pre-write wait");
	run_case(Case::STARTUP_PROFILE, false,
		"PolarDB retained txn reader bypass: changed startup profile keeps the pre-write wait");
	run_case(Case::STARTUP_IDENTITY, false,
		"PolarDB retained txn reader bypass: changed startup identity mode keeps the pre-write wait");
	run_case(Case::UNINSTALLED_STARTUP_PROFILE, false,
		"PolarDB retained txn reader bypass: changed uninstalled startup profile keeps the pre-write wait");
	run_case(Case::OFFLINE, false,
		"PolarDB retained txn reader bypass: offline reader keeps the pre-write wait");
	run_case(Case::LATENCY, false,
		"PolarDB retained txn reader bypass: over-limit reader latency keeps the pre-write wait");
	run_case(Case::LAG_CAP, false,
		"PolarDB retained txn reader bypass: byte-lag cap failure keeps the pre-write wait");
	run_case(Case::NO_RETAINED_PROOF, false,
		"PolarDB retained txn reader bypass: backend replacement without RFQ confirmation keeps the pre-write wait");
	run_case(Case::RETRY_EXCLUDED, false,
		"PolarDB retained txn reader bypass: retry-excluded reader falls back to primary");
	run_case(Case::SPLIT_MATCH, true,
		"PolarDB retained txn reader bypass: post-write split reuse skips only the LSN wait");
}

static void test_txn_reader_state_clear_contract() {
	PgSQL_Session sess;
	PgSQL_Backend fake_backend;
	PgSQL_Backend fake_primary;

	sess.polardb_txn_reader.backend = &fake_backend;
	sess.polardb_txn_reader.primary_backend = &fake_primary;
	sess.polardb_txn_reader.split_active = true;
	sess.polardb_txn_reader.wait_read_active = true;
	sess.polardb_txn_reader.wait_spec =
		PolarDB_WaitSpec::from_lsn(100, 5000, PolarDB_WaitMode::STRICT);
	sess.polardb_txn_reader.writer_scope = PolarDB_WriterScope{10, 7};
	sess.polardb_txn_reader.rfq_writer_scope =
		PolarDB_WriterScope{10, 7};
	sess.polardb_txn_reader.read_start_us = 11;
	sess.polardb_txn_reader.wait_start_us = 12;

	sess.polardb_txn_reader.clear_request_state();
	ok(sess.polardb_txn_reader.backend == &fake_backend,
		"PolarDB txn reader state: clear preserves reusable reader backend");
	ok(sess.polardb_txn_reader.primary_backend == nullptr,
		"PolarDB txn reader state: clear drops saved primary backend");
	ok(!sess.polardb_txn_reader.active(),
		"PolarDB txn reader state: clear drops active markers");
	ok(!sess.polardb_txn_reader.wait_spec.has_wait(),
		"PolarDB txn reader state: clear resets wait spec");
	ok(!sess.polardb_txn_reader.writer_scope.valid(),
		"PolarDB txn reader state: clear resets writer scope snapshot");
	ok(sess.polardb_txn_reader.rfq_writer_scope.matches(
			PolarDB_WriterScope{10, 7}),
		"PolarDB txn reader state: request clear preserves retained-reader RFQ scope");
	ok(sess.polardb_txn_reader.read_start_us == 0 &&
			sess.polardb_txn_reader.wait_start_us == 0,
		"PolarDB txn reader state: clear resets timing fields");

	sess.polardb_txn_reader.clear_backend();
	ok(sess.polardb_txn_reader.backend == nullptr &&
			!sess.polardb_txn_reader.rfq_writer_scope.valid(),
		"PolarDB txn reader state: backend release clears retained-reader RFQ scope");
}


static void test_split_warmup_request_dedup() {
	const int reader_hg = 971;

	PolarDB_StartupClientContext startup_client;
	startup_client.identity = unit_proxy_identity();
	const bool saved_lazy_warmup = pgsql_thread___polardb_lazy_warmup_split;
	const bool saved_runtime_lazy_warmup =
		GloPTH ? GloPTH->variables.polardb_lazy_warmup_split : false;
	const uint64_t saved_startup_generation =
		pgsql_thread___polardb_startup_config_generation;
	auto set_lazy_warmup = [](bool value) {
		pgsql_thread___polardb_lazy_warmup_split = value;
		if (GloPTH) {
			GloPTH->variables.polardb_lazy_warmup_split = value;
		}
	};

	// Use the public drain path to start from an empty queue. This keeps the
	// product class free of unit-only queue inspection helpers.
	set_lazy_warmup(false);
	PgHGM->warm_split_pools();

	set_lazy_warmup(true);

	const unsigned long long requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed);
	PgHGM->request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	PgHGM->request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	ok(PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed) ==
			requested_before + 1,
		"PolarDB warmup request: refreshed thread-local setting owns request acceptance");
	ok(PgHGM->status.polardb_warmup_pending.load(std::memory_order_relaxed) == 1,
		"PolarDB warmup request: pending gauge stores the deduplicated queue depth");

	set_lazy_warmup(false);
	PgHGM->warm_split_pools();
	ok(PgHGM->status.polardb_warmup_pending.load(std::memory_order_relaxed) == 0,
		"PolarDB warmup request: disabled lazy warmup drains queued work");

	set_lazy_warmup(true);
	const unsigned long long config_change_retry_before =
		PgHGM->status.polardb_reader_pool_retry_after_config_change.load(
			std::memory_order_relaxed);
	const unsigned long long stale_failed_before =
		PgHGM->status.polardb_split_warmup_failed.load(
			std::memory_order_relaxed);
	const uint64_t queued_startup_generation =
		saved_startup_generation != 0 ? saved_startup_generation : 1;
	pgsql_thread___polardb_startup_config_generation =
		queued_startup_generation;
	PgHGM->request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	pgsql_thread___polardb_startup_config_generation =
		queued_startup_generation + 1;
	PgHGM->warm_split_pools();
	ok(PgHGM->status.polardb_reader_pool_retry_after_config_change.load(
			std::memory_order_relaxed) == config_change_retry_before + 1,
		"PolarDB warmup request: stale startup generation is discarded before connect");
	ok(PgHGM->status.polardb_split_warmup_failed.load(
			std::memory_order_relaxed) == stale_failed_before,
		"PolarDB warmup request: configuration churn is not counted as a backend failure");
	pgsql_thread___polardb_startup_config_generation =
		saved_startup_generation;

	set_lazy_warmup(true);
	const unsigned long long hg0_requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed);
	const unsigned long long hg0_failed_before =
		PgHGM->status.polardb_split_warmup_failed.load(std::memory_order_relaxed);
	const unsigned long long hg0_no_target_before =
		PgHGM->status.polardb_split_warmup_no_target.load(std::memory_order_relaxed);
	PgHGM->request_split_warmup(
		0, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr);
	ok(PgHGM->status.polardb_split_warmup_requested.load(std::memory_order_relaxed) ==
			hg0_requested_before + 1,
		"PolarDB warmup request: reader hostgroup 0 is a valid request key");
	PgHGM->warm_split_pools();
	ok(PgHGM->status.polardb_split_warmup_no_target.load(std::memory_order_relaxed) ==
			hg0_no_target_before + 1,
		"PolarDB warmup request: missing reader hostgroup records no-target request");
	ok(PgHGM->status.polardb_split_warmup_failed.load(std::memory_order_relaxed) ==
			hg0_failed_before + 1,
		"PolarDB warmup request: missing reader hostgroup records request failure");

	set_lazy_warmup(false);
	PgHGM->warm_split_pools();

	pgsql_thread___polardb_lazy_warmup_split = saved_lazy_warmup;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = saved_runtime_lazy_warmup;
	}
}

static void test_split_warmup_targeted_rerun() {
	const int writer_hg = 988;
	const int reader_hg = 989;
	stage_polardb_topology(PgHGM, "PolarDB targeted warmup rerun",
		writer_hg, "polardb-warmup-rerun-writer", 24832,
		reader_hg, "polardb-warmup-rerun-reader", 24932);
	PgSQL_HGC* reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC* target = find_pgsql_server(
		reader_hgc, "polardb-warmup-rerun-reader", 24932);
	ok(target != nullptr,
		"PolarDB targeted warmup rerun: target reader is available");
	if (!target) {
		return;
	}

	const bool saved_lazy_warmup = pgsql_thread___polardb_lazy_warmup_split;
	const bool saved_runtime_lazy_warmup =
		GloPTH ? GloPTH->variables.polardb_lazy_warmup_split : false;
	pgsql_thread___polardb_lazy_warmup_split = true;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = true;
	}
	PgSQL_PolarDB_ReaderPool pool(PgHGM);
	PolarDB_StartupClientContext startup_client;
	startup_client.identity = unit_proxy_identity();
	const unsigned long long requested_before =
		PgHGM->status.polardb_split_warmup_requested.load(
			std::memory_order_relaxed);

	pool.request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr, target);
	const std::string request_key = PolarDB_WarmupUnitAccess::queued_key(pool);
	std::vector<PgSQL_SplitWarmupRequest> requests;
	const bool drained = PolarDB_WarmupUnitAccess::drain(pool, requests);
	const bool registered = !request_key.empty() && requests.size() == 1 &&
		requests[0].target_server_list_generation != 0 &&
		PolarDB_WarmupUnitAccess::register_inflight(pool, request_key);
	ok(drained && registered,
		"PolarDB targeted warmup rerun: first request records the server generation and enters in-flight state");

	pool.request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr, target);
	pool.request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr, target);
	ok(PolarDB_WarmupUnitAccess::rerun_pending(pool, request_key),
		"PolarDB targeted warmup rerun: repeated misses during creation coalesce into one follow-up");

	if (registered) {
		PolarDB_WarmupUnitAccess::finish_inflight(
			pool, request_key, requests[0]);
	}
	ok(!PolarDB_WarmupUnitAccess::rerun_pending(pool, request_key) &&
			PolarDB_WarmupUnitAccess::queued_key(pool) == request_key &&
			PgHGM->status.polardb_split_warmup_requested.load(
				std::memory_order_relaxed) == requested_before + 2,
		"PolarDB targeted warmup rerun: completion queues exactly one coalesced follow-up");

	pgsql_thread___polardb_lazy_warmup_split = false;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = false;
	}
	requests.clear();
	(void)PolarDB_WarmupUnitAccess::drain(pool, requests);
	pgsql_thread___polardb_lazy_warmup_split = true;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = true;
	}
	pool.request_split_warmup(
		reader_hg, "polardb_unit_user", "polardb_unit_pass",
		"polardb_unit_db", startup_client, nullptr, target);
	stage_polardb_topology(PgHGM, "PolarDB targeted warmup generation change",
		writer_hg, "polardb-warmup-rerun-writer-new", 24833,
		reader_hg, "polardb-warmup-rerun-reader-new", 24933);
	const unsigned long long retry_before =
		PgHGM->status.polardb_reader_pool_retry_after_config_change.load(
			std::memory_order_relaxed);
	const unsigned long long target_attempts_before =
		PgHGM->status.polardb_split_warmup_target_attempts.load(
			std::memory_order_relaxed);
	pool.warm_split_pools();
	ok(PgHGM->status.polardb_reader_pool_retry_after_config_change.load(
			std::memory_order_relaxed) == retry_before + 1 &&
		PgHGM->status.polardb_split_warmup_target_attempts.load(
			std::memory_order_relaxed) == target_attempts_before,
		"PolarDB targeted warmup rerun: a server-list change discards stale work before connecting");

	pgsql_thread___polardb_lazy_warmup_split = false;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split = false;
	}
	requests.clear();
	(void)PolarDB_WarmupUnitAccess::drain(pool, requests);
	pgsql_thread___polardb_lazy_warmup_split = saved_lazy_warmup;
	if (GloPTH) {
		GloPTH->variables.polardb_lazy_warmup_split =
			saved_runtime_lazy_warmup;
	}
}

static void test_split_warmup_preserves_dynamic_session_state() {
	PgSQL_Connection client(true);
	PgSQL_Connection warmed(false);
	set_test_userinfo(&client);
	set_test_userinfo(&warmed);
	set_test_pgsql_defaults(&client);
	set_test_pgsql_defaults(&warmed);

	const char* search_path = "public";
	free(client.variables[PGSQL_SEARCH_PATH].value);
	client.variables[PGSQL_SEARCH_PATH].value = strdup(search_path);
	client.var_hash[PGSQL_SEARCH_PATH] =
		SpookyHash::Hash32(search_path, strlen(search_path), 10);
	client.reorder_dynamic_variables_idx();

	PgSQL_SplitWarmupRequest request =
		make_unit_warmup_request(/*reader_hg=*/989, 1);
	unit_warmup_request_use_connection_state(request, &client);
	const bool applied =
		PolarDB_WarmupUnitAccess::apply_startup_parameters(
			&warmed, request);
	const bool has_search_path =
		std::find(warmed.dynamic_variables_idx.begin(),
			warmed.dynamic_variables_idx.end(),
			static_cast<uint32_t>(PGSQL_SEARCH_PATH)) !=
			warmed.dynamic_variables_idx.end();

	ok(applied && request.startup_parameters.size() ==
				PGSQL_NAME_LAST_HIGH_WM &&
			request.startup_parameter_hash[PGSQL_SEARCH_PATH] != 0 &&
			has_search_path &&
			warmed.variables[PGSQL_SEARCH_PATH].value &&
			strcmp(warmed.variables[PGSQL_SEARCH_PATH].value,
				search_path) == 0 &&
			unit_reader_pool_options_key(&warmed) ==
				request.startup_options_hash,
		"PolarDB targeted warmup: dynamic session state keeps the exact reuse key");
}

static void test_split_warmup_runtime_variable_refresh() {
	if (!GloPTH) {
		ok(1, "PolarDB warmup variables: skipped without PgSQL thread handler");
		return;
	}

	const int saved_runtime_throttle =
		GloPTH->variables.throttle_connections_per_sec_to_hostgroup;
	const int saved_runtime_latency =
		GloPTH->variables.default_max_latency_ms;
	const int saved_runtime_retries =
		GloPTH->variables.connect_retries_on_failure;
	const int saved_runtime_shun =
		GloPTH->variables.shun_on_failures;
	const int saved_runtime_recovery =
		GloPTH->variables.shun_recovery_time_sec;
	const int saved_runtime_timeout =
		GloPTH->variables.connect_timeout_server;
	const int saved_runtime_timeout_max =
		GloPTH->variables.connect_timeout_server_max;
	const int saved_runtime_max_connections =
		GloPTH->variables.polardb_split_warmup_max_connections_per_request;
	const bool saved_runtime_lazy_warmup =
		GloPTH->variables.polardb_lazy_warmup_split;
	char* saved_runtime_proxy_protocol =
		GloPTH->variables.polardb_proxy_protocol ?
			strdup(GloPTH->variables.polardb_proxy_protocol) : NULL;
	char* saved_runtime_identity_mode =
		GloPTH->variables.polardb_proxy_identity_mode ?
			strdup(GloPTH->variables.polardb_proxy_identity_mode) : NULL;
	char* saved_runtime_identity_host =
		GloPTH->variables.polardb_proxy_identity_host ?
			strdup(GloPTH->variables.polardb_proxy_identity_host) : NULL;
	const int saved_runtime_identity_port =
		GloPTH->variables.polardb_proxy_identity_port;

	const int saved_tls_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	const int saved_tls_latency = pgsql_thread___default_max_latency_ms;
	const int saved_tls_retries = pgsql_thread___connect_retries_on_failure;
	const int saved_tls_shun = pgsql_thread___shun_on_failures;
	const int saved_tls_recovery = pgsql_thread___shun_recovery_time_sec;
	const int saved_tls_timeout = pgsql_thread___connect_timeout_server;
	const int saved_tls_timeout_max = pgsql_thread___connect_timeout_server_max;
	const int saved_tls_max_connections =
		pgsql_thread___polardb_split_warmup_max_connections_per_request;
	const bool saved_tls_lazy_warmup =
		pgsql_thread___polardb_lazy_warmup_split;
	const int saved_tls_proxy_protocol =
		pgsql_thread___polardb_proxy_protocol;
	const int saved_tls_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
	char* saved_tls_identity_host =
		pgsql_thread___polardb_proxy_identity_host ?
			strdup(pgsql_thread___polardb_proxy_identity_host) : NULL;
	const int saved_tls_identity_port =
		pgsql_thread___polardb_proxy_identity_port;
	GloPTH->wrlock();
	GloPTH->variables.throttle_connections_per_sec_to_hostgroup = 0;
	GloPTH->variables.default_max_latency_ms = 17;
	GloPTH->variables.connect_retries_on_failure = 2;
	GloPTH->variables.shun_on_failures = 3;
	GloPTH->variables.shun_recovery_time_sec = 4;
	GloPTH->variables.connect_timeout_server = 1234;
	GloPTH->variables.connect_timeout_server_max = 4321;
	GloPTH->variables.polardb_split_warmup_max_connections_per_request = 7;
	GloPTH->variables.polardb_lazy_warmup_split = !saved_tls_lazy_warmup;
	free(GloPTH->variables.polardb_proxy_protocol);
	GloPTH->variables.polardb_proxy_protocol = strdup((char*)"legacy");
	free(GloPTH->variables.polardb_proxy_identity_mode);
	GloPTH->variables.polardb_proxy_identity_mode = strdup((char*)"proxy");
	free(GloPTH->variables.polardb_proxy_identity_host);
	GloPTH->variables.polardb_proxy_identity_host = strdup((char*)"127.0.0.99");
	GloPTH->variables.polardb_proxy_identity_port = 6603;
	GloPTH->commit();
	GloPTH->wrunlock();

	pgsql_thread___throttle_connections_per_sec_to_hostgroup = 99;
	pgsql_thread___default_max_latency_ms = 99;
	pgsql_thread___connect_retries_on_failure = 99;
	pgsql_thread___shun_on_failures = 99;
	pgsql_thread___shun_recovery_time_sec = 99;
	pgsql_thread___connect_timeout_server = 99;
	pgsql_thread___connect_timeout_server_max = 99;
	pgsql_thread___polardb_split_warmup_max_connections_per_request = 99;
	pgsql_thread___polardb_lazy_warmup_split = saved_tls_lazy_warmup;
	pgsql_thread___polardb_proxy_protocol = POLARDB_PROXY_PROTOCOL_OFF;
	pgsql_thread___polardb_proxy_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::CLIENT);
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host = strdup((char*)"192.0.2.10");
	pgsql_thread___polardb_proxy_identity_port = 1234;

	PgHGM->polardb_refresh_split_warmup_variables();
	ok(pgsql_thread___throttle_connections_per_sec_to_hostgroup == 0,
		"PolarDB warmup variables: throttle value refreshes exactly");
	ok(pgsql_thread___default_max_latency_ms == 17 &&
			pgsql_thread___connect_retries_on_failure == 2 &&
			pgsql_thread___shun_on_failures == 3 &&
			pgsql_thread___shun_recovery_time_sec == 4,
		"PolarDB warmup variables: latency and shun policy refresh");
	ok(pgsql_thread___connect_timeout_server == 1234 &&
			pgsql_thread___connect_timeout_server_max == 4321,
		"PolarDB warmup variables: connect timeout policy refresh");
	ok(pgsql_thread___polardb_split_warmup_max_connections_per_request == 7,
		"PolarDB warmup variables: max connections per request refresh");
	ok(pgsql_thread___polardb_lazy_warmup_split == !saved_tls_lazy_warmup,
		"PolarDB warmup variables: lazy-warmup enablement refreshes safely");
	ok(pgsql_thread___polardb_proxy_protocol == POLARDB_PROXY_PROTOCOL_LEGACY,
		"PolarDB warmup variables: proxy protocol refresh");
	ok(pgsql_thread___polardb_proxy_identity_mode ==
				static_cast<int>(PolarDB_ProxyIdentityMode::PROXY) &&
			pgsql_thread___polardb_proxy_identity_host &&
			strcmp(pgsql_thread___polardb_proxy_identity_host,
				"127.0.0.99") == 0 &&
			pgsql_thread___polardb_proxy_identity_port == 6603,
		"PolarDB warmup variables: proxy identity policy refresh");
	ok(pgsql_thread___polardb_startup_config_generation ==
			GloPTH->get_polardb_startup_config_generation(),
		"PolarDB warmup variables: startup generation refreshes with parsed fields");

	GloPTH->wrlock();
	GloPTH->variables.throttle_connections_per_sec_to_hostgroup =
		saved_runtime_throttle;
	GloPTH->variables.default_max_latency_ms = saved_runtime_latency;
	GloPTH->variables.connect_retries_on_failure = saved_runtime_retries;
	GloPTH->variables.shun_on_failures = saved_runtime_shun;
	GloPTH->variables.shun_recovery_time_sec = saved_runtime_recovery;
	GloPTH->variables.connect_timeout_server = saved_runtime_timeout;
	GloPTH->variables.connect_timeout_server_max = saved_runtime_timeout_max;
	GloPTH->variables.polardb_split_warmup_max_connections_per_request =
		saved_runtime_max_connections;
	GloPTH->variables.polardb_lazy_warmup_split =
		saved_runtime_lazy_warmup;
	free(GloPTH->variables.polardb_proxy_protocol);
	GloPTH->variables.polardb_proxy_protocol =
		saved_runtime_proxy_protocol ? saved_runtime_proxy_protocol : strdup((char*)"");
	free(GloPTH->variables.polardb_proxy_identity_mode);
	GloPTH->variables.polardb_proxy_identity_mode =
		saved_runtime_identity_mode ? saved_runtime_identity_mode : strdup((char*)"");
	free(GloPTH->variables.polardb_proxy_identity_host);
	GloPTH->variables.polardb_proxy_identity_host =
		saved_runtime_identity_host ? saved_runtime_identity_host : strdup((char*)"");
	GloPTH->variables.polardb_proxy_identity_port =
		saved_runtime_identity_port;
	GloPTH->commit();
	GloPTH->wrunlock();

	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		saved_tls_throttle;
	pgsql_thread___default_max_latency_ms = saved_tls_latency;
	pgsql_thread___connect_retries_on_failure = saved_tls_retries;
	pgsql_thread___shun_on_failures = saved_tls_shun;
	pgsql_thread___shun_recovery_time_sec = saved_tls_recovery;
	pgsql_thread___connect_timeout_server = saved_tls_timeout;
	pgsql_thread___connect_timeout_server_max = saved_tls_timeout_max;
	pgsql_thread___polardb_split_warmup_max_connections_per_request =
		saved_tls_max_connections;
	pgsql_thread___polardb_lazy_warmup_split = saved_tls_lazy_warmup;
	pgsql_thread___polardb_proxy_protocol = saved_tls_proxy_protocol;
	pgsql_thread___polardb_proxy_identity_mode = saved_tls_identity_mode;
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host = saved_tls_identity_host;
	pgsql_thread___polardb_proxy_identity_port = saved_tls_identity_port;
	pgsql_thread___polardb_startup_config_generation =
		GloPTH->get_polardb_startup_config_generation();
}

static void test_startup_config_update() {
	if (!GloPTH) {
		ok(1, "PolarDB startup config: skipped without PgSQL thread handler");
		ok(1, "PolarDB startup config: consistency-only generation skipped");
		ok(1, "PolarDB startup config: concurrent shared readers skipped");
		return;
	}

	GloPTH->wrlock();
	char* saved_protocol = strdup(GloPTH->variables.polardb_proxy_protocol);
	char* saved_consistency = strdup(GloPTH->variables.polardb_consistency_mode);
	const PolarDB_ParsedGlobalConfigValue before =
		GloPTH->get_polardb_global_config_unlocked();
	const char* alternate_protocol =
		before.startup.proxy_protocol == PolarDB_ProxyProtocol::LEGACY ?
			"v15" : "legacy";
	const bool protocol_set = GloPTH->set_variable(
		(char*)"polardb_proxy_protocol", alternate_protocol);
	GloPTH->commit();
	const PolarDB_ParsedGlobalConfigValue after_startup =
		GloPTH->get_polardb_global_config_unlocked();
	GloPTH->wrunlock();

	ok(protocol_set &&
			after_startup.startup.generation ==
				before.startup.generation + 1 &&
			after_startup.startup.proxy_protocol !=
				before.startup.proxy_protocol,
		"PolarDB startup config: one committed startup change advances one generation");

	GloPTH->wrlock();
	const char* alternate_consistency =
		after_startup.consistency_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN) ?
			"off" : "session_lsn";
	const bool consistency_set = GloPTH->set_variable(
		(char*)"polardb_consistency_mode", alternate_consistency);
	GloPTH->commit();
	const PolarDB_ParsedGlobalConfigValue after_consistency =
		GloPTH->get_polardb_global_config_unlocked();
	GloPTH->wrunlock();

	ok(consistency_set &&
			after_consistency.consistency_mode !=
				after_startup.consistency_mode &&
			after_consistency.startup.generation ==
				after_startup.startup.generation,
		"PolarDB startup config: consistency-only commit preserves startup generation");

	std::atomic<bool> reader_started{false};
	std::atomic<bool> reader_acquired{false};
	GloPTH->rdlock();
	std::thread concurrent_reader([&]() {
		reader_started.store(true, std::memory_order_release);
		GloPTH->rdlock();
		reader_acquired.store(true, std::memory_order_release);
		GloPTH->rdunlock();
	});
	while (!reader_started.load(std::memory_order_acquire)) {
		std::this_thread::yield();
	}
	for (unsigned int attempt = 0; attempt < 1000; ++attempt) {
		if (reader_acquired.load(std::memory_order_acquire)) {
			break;
		}
		usleep(100);
	}
	const bool shared_reader_acquired =
		reader_acquired.load(std::memory_order_acquire);
	GloPTH->rdunlock();
	concurrent_reader.join();
	ok(shared_reader_acquired,
		"PolarDB startup config: cold readers share the config update lock");

	GloPTH->wrlock();
	GloPTH->set_variable((char*)"polardb_proxy_protocol", saved_protocol);
	GloPTH->set_variable((char*)"polardb_consistency_mode", saved_consistency);
	GloPTH->commit();
	GloPTH->wrunlock();
	free(saved_protocol);
	free(saved_consistency);
}

static void test_connected_reader_startup_settings_acceptance() {
	const int writer_hg = 988;
	const int reader_hg = 989;
	stage_polardb_topology(PgHGM, "PolarDB connected reader startup settings",
		writer_hg, "polardb-settings-writer", 22982,
		reader_hg, "polardb-settings-reader", 22983);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg),
		"polardb-settings-reader", 22983);
	ok(reader != nullptr,
		"PolarDB connected reader startup settings: reader fixture is available");
	if (!reader || !GloPTH) {
		return;
	}

	const PolarDB_ParsedGlobalConfigValue global_config =
		GloPTH->get_polardb_global_config();
	const int identity_mode =
		static_cast<int>(global_config.startup.identity_mode);
	const PolarDB_StartupProfile profile =
		PgHGM->polardb_startup_profile_for_hostgroup(
			reader_hg,
			static_cast<int>(global_config.startup.proxy_protocol));
	PolarDB_StartupClientContext startup_client;
	startup_client.identity = PolarDB_StartupIdentity{
		"127.0.0.41", 6041,
		polardb_proxy_identity_mode_uses_client_identity(identity_mode)
			? PolarDB_StartupIdentitySource::CLIENT
			: PolarDB_StartupIdentitySource::LISTENER_PROXY};

	PgSQL_Connection conn(false);
	conn.parent = reader;
	conn.polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	conn.set_polardb_startup_settings(
		profile, identity_mode, global_config.startup.generation,
		startup_client);
	ok(conn.polardb_startup_settings_set &&
			conn.polardb_startup_config_generation ==
				global_config.startup.generation &&
			conn.polardb_startup_profile_generation ==
				profile.generation(identity_mode) &&
			conn.polardb_startup_client.identity.host == "127.0.0.41",
		"PolarDB connected reader startup settings: stored values preserve the selected startup state");

	ok(polardb_connection_startup_settings_match(
			&conn, profile, &startup_client,
			global_config.startup.generation, identity_mode),
		"PolarDB connected reader startup settings: matching settings allow classic-pool reuse");

	const PolarDB_StartupProfile off_profile =
		PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	ok(!polardb_connection_startup_settings_match(
			&conn, off_profile, nullptr,
			global_config.startup.generation, identity_mode),
		"PolarDB connected reader startup settings: a protocol change rejects classic-pool reuse");

	PolarDB_StartupClientContext other_client = startup_client;
	other_client.identity.host = "127.0.0.42";
	ok(!polardb_connection_startup_settings_match(
			&conn, profile, &other_client,
			global_config.startup.generation, identity_mode),
		"PolarDB connected reader startup settings: a different startup identity rejects classic-pool reuse");

	ok(!polardb_connection_startup_settings_match(
			&conn, profile, &startup_client,
			global_config.startup.generation + 1, identity_mode),
		"PolarDB connected reader startup settings: a newer startup generation rejects classic-pool reuse");

	ok(PgHGM->polardb_reader_connection_is_current(
			&conn, reader_hg),
		"PolarDB connected reader startup settings: unchanged current state accepts the connected reader");

	conn.polardb_selected_server_snapshot.reset();
	ok(PgHGM->polardb_reader_connection_is_current(
			&conn, reader_hg),
		"PolarDB connected reader startup settings: a core-created connection does not require a ReaderPool snapshot");

	conn.polardb_startup_config_generation++;
	ok(!PgHGM->polardb_reader_connection_is_current(
			&conn, reader_hg),
		"PolarDB connected reader startup settings: stale startup generation rejects the connected reader");
	conn.polardb_startup_config_generation =
		global_config.startup.generation;
	reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	ok(!PgHGM->polardb_reader_connection_is_current(
			&conn, reader_hg),
		"PolarDB connected reader startup settings: offline selected server rejects dispatch");
	reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
}

static void test_split_warmup_failure_accounting_policy() {
	ok(pgsql_split_warmup_should_count_connect_failure(false, false),
		"PolarDB warmup accounting: real connect failure is counted");
	ok(!pgsql_split_warmup_should_count_connect_failure(false, true),
		"PolarDB warmup accounting: shutdown stop is not a backend failure");
	ok(!pgsql_split_warmup_should_count_connect_failure(true, false),
		"PolarDB warmup accounting: successful connect is not a failure");
}

static void test_split_warmup_max_connections_per_request_policy() {
	ok(polardb_split_warmup_connection_limit(0) ==
			PGSQL_POLARDB_SPLIT_WARMUP_DEFAULT_MAX_CONNECTIONS_PER_REQUEST,
		"PolarDB warmup max connections per request: non-positive values use default");
	ok(polardb_split_warmup_connection_limit(4) == 4,
		"PolarDB warmup max connections per request: configured value is accepted");
	ok(polardb_split_warmup_connection_limit(1000) ==
			PGSQL_POLARDB_SPLIT_WARMUP_MAX_CONNECTIONS_PER_REQUEST_LIMIT,
		"PolarDB warmup max connections per request: configured value is capped");
}

static void test_split_warmup_thread_refreshes_idle_server_list() {
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh first topology",
		1110, "polardb-warmup-refresh-writer-a", 26500,
		1111, "polardb-warmup-refresh-reader-a", 26501);

	std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		old_server_list = PgHGM->get_polardb_server_list_snapshot();
	std::weak_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		old_server_list_reference = old_server_list;
	ok(old_server_list != nullptr,
		"PolarDB idle warmup refresh: first server list is available");

	PgHGM->init();
	usleep(250000);
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh second topology",
		1112, "polardb-warmup-refresh-writer-b", 26502,
		1113, "polardb-warmup-refresh-reader-b", 26503);
	old_server_list.reset();
	PgHGM->polardb_refresh_thread_snapshots();

	usleep(250000);
	stage_polardb_topology(PgHGM, "PolarDB idle warmup refresh third topology",
		1114, "polardb-warmup-refresh-writer-c", 26504,
		1115, "polardb-warmup-refresh-reader-c", 26505);
	PgHGM->shutdown_split_warmup_thread();

	ok(old_server_list_reference.expired(),
		"PolarDB idle warmup refresh: idle thread releases the old server list");
}


static size_t polardb_unit_distinct_warmup_targets(
		const std::vector<PgSQL_SplitWarmupRequest>& target_requests) {
	std::vector<std::string> seen;
	for (const PgSQL_SplitWarmupRequest& req : target_requests) {
		std::string key = req.target_address + ":" +
			std::to_string(static_cast<unsigned int>(req.target_port));
		if (std::find(seen.begin(), seen.end(), key) == seen.end()) {
			seen.push_back(std::move(key));
		}
	}
	return seen.size();
}

static void test_split_warmup_target_expansion_policy() {
	const int writer_hg_one = 980;
	const int reader_hg_one = 981;
	const int writer_hg_many = 982;
	const int reader_hg_many = 983;
	const int writer_hg_full = 984;
	const int reader_hg_full = 985;
	const int writer_hg_no_rfq = 986;
	const int reader_hg_no_rfq = 987;

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup one-reader expansion",
		writer_hg_one, "polardb-warmup-one-writer", 24632,
		reader_hg_one, "polardb-warmup-one-reader-", 1, 24732);

	PgSQL_PolarDB_ReaderPool pool(PgHGM);

	std::vector<PgSQL_SplitWarmupRequest> one_reader_targets;
	bool found_hostgroup = false;
	bool saw_eligible_target = false;
	bool saw_compatible_free = false;
	PgSQL_SplitWarmupRequest one_reader_req =
		make_unit_warmup_request(reader_hg_one, 4);
	PgHGM->wrlock();
	const size_t one_reader_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, one_reader_req, one_reader_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	bool one_reader_sequence_ok = one_reader_count == 4 &&
		polardb_unit_distinct_warmup_targets(one_reader_targets) == 1;
	for (size_t i = 0; i < one_reader_targets.size(); ++i) {
		if (one_reader_targets[i].target_port != 24732 ||
				one_reader_targets[i].target_required_free_count != i + 1) {
			one_reader_sequence_ok = false;
		}
	}
	ok(found_hostgroup && saw_eligible_target && !saw_compatible_free &&
			one_reader_sequence_ok,
		"PolarDB warmup expansion: one reader can receive multiple target requests up to the request maximum");

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup many-reader expansion",
		writer_hg_many, "polardb-warmup-many-writer", 24642,
		reader_hg_many, "polardb-warmup-many-reader-", 8, 24742);

	std::vector<PgSQL_SplitWarmupRequest> many_reader_targets;
	found_hostgroup = false;
	saw_eligible_target = false;
	saw_compatible_free = false;
	PgSQL_SplitWarmupRequest many_reader_req =
		make_unit_warmup_request(reader_hg_many, 4);
	PgHGM->wrlock();
	const size_t many_reader_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, many_reader_req, many_reader_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	ok(found_hostgroup && saw_eligible_target && !saw_compatible_free &&
			many_reader_count == 4 &&
			polardb_unit_distinct_warmup_targets(many_reader_targets) == 4,
		"PolarDB warmup expansion: many readers receive at most one target each before repeats");

	std::vector<PgSQL_SplitWarmupRequest> first_single_target;
	std::vector<PgSQL_SplitWarmupRequest> second_single_target;
	PgSQL_SplitWarmupRequest single_req =
		make_unit_warmup_request(reader_hg_many, 1);
	PgHGM->wrlock();
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, single_req, first_single_target,
		nullptr, nullptr, nullptr);
	pgsql_polardb_unit_collect_split_warmup_targets(
		&pool, single_req, second_single_target,
		nullptr, nullptr, nullptr);
	PgHGM->wrunlock();
	ok(first_single_target.size() == 1 &&
			second_single_target.size() == 1 &&
			first_single_target[0].target_port !=
				second_single_target[0].target_port,
		"PolarDB warmup expansion: repeated single-target requests rotate reader selection");

	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg_no_rfq, "polardb-warmup-no-rfq-writer", 24662,
			reader_hg_no_rfq, "polardb-warmup-no-rfq-reader", 24762)) == 0,
		"PolarDB warmup RFQ profile: writer and reader staged for commit");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row_with_protocol(
			writer_hg_no_rfq, reader_hg_no_rfq, "off"),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB warmup RFQ profile: topology commit succeeds");

	std::vector<PgSQL_SplitWarmupRequest> no_rfq_targets;
	found_hostgroup = false;
	saw_eligible_target = false;
	saw_compatible_free = false;
	PgSQL_SplitWarmupRequest no_rfq_req =
		make_unit_warmup_request(reader_hg_no_rfq, 4);
	PgHGM->wrlock();
	const size_t no_rfq_count =
		pgsql_polardb_unit_collect_split_warmup_targets(
			&pool, no_rfq_req, no_rfq_targets,
			&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
	PgHGM->wrunlock();

	ok(found_hostgroup && !saw_eligible_target &&
			!saw_compatible_free && no_rfq_count == 0,
		"PolarDB warmup RFQ profile: protocol without RFQ fields produces no target requests");

	stage_polardb_topology_many_readers(PgHGM,
		"PolarDB warmup full-reader expansion",
		writer_hg_full, "polardb-warmup-full-writer", 24652,
		reader_hg_full, "polardb-warmup-full-reader-", 1, 24752);

	PgSQL_HGC *full_reader_hgc = PgHGM->MyHGC_lookup(reader_hg_full);
	PgSQL_SrvC *full_reader =
		find_pgsql_server(full_reader_hgc, "polardb-warmup-full-reader-1", 24752);
	ok(full_reader != nullptr,
		"PolarDB warmup expansion: full-reader server container is available");
	if (full_reader) {
		const long saved_max_connections = full_reader->max_connections;
		PgSQL_Connection *incompatible = make_cached_reader_connection(full_reader);
		incompatible->pgsql_conn = unit_connected_pgconn();
		incompatible->polardb_startup_client.identity =
			unit_other_proxy_identity();
		full_reader->ConnectionsFree->add(incompatible);
		full_reader->max_connections = 1;

		std::vector<PgSQL_SplitWarmupRequest> full_reader_targets;
		found_hostgroup = false;
		saw_eligible_target = false;
		saw_compatible_free = false;
		PgSQL_SplitWarmupRequest full_reader_req =
			make_unit_warmup_request(reader_hg_full, 4);
		PgHGM->wrlock();
		const size_t full_reader_count =
			pgsql_polardb_unit_collect_split_warmup_targets(
				&pool, full_reader_req, full_reader_targets,
				&found_hostgroup, &saw_eligible_target, &saw_compatible_free);
		PgHGM->wrunlock();

		ok(found_hostgroup && !saw_eligible_target &&
				!saw_compatible_free && full_reader_count == 0,
			"PolarDB warmup expansion: saturated reader receives no target requests");

		full_reader->ConnectionsFree->remove(incompatible);
		full_reader->max_connections = saved_max_connections;
		delete incompatible;
	}
}

static void test_txn_reader_failure_route_state_contract() {
	PgSQL_Session::PolarDB_TxnReaderFailureState state;
	ok(!state.active(),
		"PolarDB reader failure state: default state is inactive");
	ok(state.forced_writer_hg() == -1,
		"PolarDB reader failure state: default state has no writer override");

	state.set_force_writer(710);
	ok(state.active(),
		"PolarDB reader failure state: writer route is active");
	ok(state.forced_writer_hg() == 710,
		"PolarDB reader failure state: writer hostgroup is returned");

	const char* skipped_address = nullptr;
	int skipped_port = -1;
	state.set_reader_skip(711, "reader-a", 15433);
	ok(state.active() && state.forced_writer_hg() == -1,
		"PolarDB reader failure state: reader skip does not force writer");
	ok(state.matches_skipped_reader(711, &skipped_address, &skipped_port) &&
			strcmp(skipped_address, "reader-a") == 0 &&
			skipped_port == 15433,
		"PolarDB reader failure state: matching failed reader is returned");
	ok(!state.matches_skipped_reader(712, &skipped_address, &skipped_port),
		"PolarDB reader failure state: another hostgroup is not skipped");

	state.clear();
	ok(!state.active(),
		"PolarDB reader failure state: clear returns to inactive state");
}

void run_polardb_split_routing_tests() {
	test_txn_split_excludes_writer_as_reader();
}

void run_polardb_split_warmup_inventory_tests() {
	test_split_warmup_counts_shared_reader_pool_inventory();
	test_split_warmup_ignores_incompatible_shared_inventory();
}

void run_polardb_split_protocol_tests() {
	test_txn_split_xids_cleanup_boundary();
	test_libpq_empty_xact_markers();
	test_read_only_txn_end_keeps_session_lsn();
}

void run_polardb_retained_reader_retry_tests() {
	test_wait_bypass_target_reaches_client_rfq();
	test_wait_bypass_reset_retry_uses_writer();
}

void run_polardb_retained_reader_lifecycle_tests() {
	test_retained_txn_reader_wait_bypass_contract();
	test_txn_reader_state_clear_contract();
}

void run_polardb_split_warmup_policy_tests() {
	test_split_warmup_request_dedup();
	test_split_warmup_targeted_rerun();
	test_split_warmup_preserves_dynamic_session_state();
	test_startup_config_update();
	test_connected_reader_startup_settings_acceptance();
	test_split_warmup_runtime_variable_refresh();
	test_split_warmup_failure_accounting_policy();
	test_split_warmup_max_connections_per_request_policy();
	test_split_warmup_target_expansion_policy();
	test_txn_reader_failure_route_state_contract();
}

void run_polardb_split_warmup_thread_tests() {
	test_split_warmup_thread_refreshes_idle_server_list();
}

#endif // POLARDB_PROXY
