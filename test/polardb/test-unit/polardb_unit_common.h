/**
 * @file polardb_unit_common.h
 * @brief Shared helpers for the PolarDB unit tests.
 *
 * Two flavours of PolarDB unit test live in this directory:
 *
 *   - "header-only" tests (polardb_routing_lsn_unit-t, polardb_startup_profile_unit-t)
 *     compile with only -DPOLARDB_PROXY=1, include just <tap.h> + <PgSQL_PolarDB.h>,
 *     and link against tap.o alone. They must NOT pull in HostGroups_Manager /
 *     full-harness types.
 *
 *   - the "full-harness" test (polardb_hgm_lsn_unit-t) links the full libproxysql.a
 *     and includes the TAP test harness (test_globals.h / test_init.h).
 *
 * Accordingly this header has two sections:
 *
 *   1. Lightweight helpers (notice walker, value factories, named constants, the
 *      writer-scope match-matrix helper) — ALWAYS available; depend only on
 *      <tap.h> + <PgSQL_PolarDB.h>.
 *
 *   2. Full-harness HGM fixtures — protected by POLARDB_UNIT_FULL_HARNESS, which only
 *      the hgm test defines (after including the harness headers it needs) before
 *      including this file. These reference real ProxySQL HGM/connection types.
 */

#ifndef POLARDB_UNIT_COMMON_H
#define POLARDB_UNIT_COMMON_H

#include "tap.h"
#include "PgSQL_PolarDB.h"

#include <cstring>

// ============================================================================
// Section 1: lightweight helpers (header-only friendly)
// ============================================================================

/**
 * @brief Walk a PostgreSQL NoticeResponse ('N') packet looking for a field.
 *
 * Returns true when a field with the given single-byte @p code carries exactly
 * @p expected as its NUL-terminated value. Bounds-checked against @p size so a
 * malformed packet can never read past the buffer.
 */
static inline bool notice_packet_has_field(
	const unsigned char* pkt,
	unsigned int size,
	unsigned char code,
	const char* expected) {
	if (!pkt || size < 6 || !expected) {
		return false;
	}
	const unsigned char* field_ptr = pkt + 5;
	const unsigned char* packet_end = pkt + size;
	while (field_ptr < packet_end && *field_ptr != '\0') {
		const unsigned char field_code = *field_ptr++;
		const char* value = reinterpret_cast<const char*>(field_ptr);
		const size_t value_len = strlen(value);
		if (field_ptr + value_len >= packet_end) {
			return false;
		}
		if (field_code == code && strcmp(value, expected) == 0) {
			return true;
		}
		field_ptr += value_len + 1;
	}
	return false;
}

/// @brief Default V15 startup profile (requests RFQ LSN, emits startup params).
static inline PolarDB_StartupProfile make_v15_profile() {
	return PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::V15);
}

/// @brief A valid non-wildcard IPv4 loopback startup identity on the default port.
static inline PolarDB_StartupIdentity make_loopback_identity() {
	return PolarDB_StartupIdentity{
		"127.0.0.1",
		5432,
		PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK
	};
}

// Named writer hostgroup / epoch constants for the PolarDB_WriterScope match
// matrix. The matrix probes that a scope only matches when BOTH the writer
// hostgroup and the (hostgroup-scoped) epoch are identical and valid.
static const int POLARDB_SCOPE_WRITER_HG = 10;          // canonical writer hostgroup
static const int POLARDB_SCOPE_OTHER_WRITER_HG = 11;    // a different writer hostgroup
static const int POLARDB_SCOPE_MISSING_HG = -1;         // unset/invalid writer hostgroup
static const uint64_t POLARDB_SCOPE_EPOCH = 7;          // canonical writer epoch
static const uint64_t POLARDB_SCOPE_CHANGED_EPOCH = 8;  // epoch after a writer change

/**
 * @brief Assert the PolarDB_WriterScope::matches() invariant matrix.
 *
 * Both the RFQ-result-update check and the session-LSN scope check accept new
 * state only when the request scope matches the current writer scope. This
 * helper runs the shared 5-invariant matrix; @p label names the consuming domain
 * (e.g. "RFQ result update", "session LSN scope") so failures stay diagnosable.
 *
 * Emits exactly 5 ok() assertions.
 */
static inline void check_writer_scope_match_matrix(const char* label) {
	ok(PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}.matches(
			PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}),
		"matching writer hostgroup and epoch accepts %s", label);
	ok(!PolarDB_WriterScope{POLARDB_SCOPE_MISSING_HG, POLARDB_SCOPE_EPOCH}.matches(
			PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}),
		"missing request writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}.matches(
			PolarDB_WriterScope{POLARDB_SCOPE_MISSING_HG, POLARDB_SCOPE_EPOCH}),
		"missing current writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}.matches(
			PolarDB_WriterScope{POLARDB_SCOPE_OTHER_WRITER_HG, POLARDB_SCOPE_EPOCH}),
		"same scalar epoch from another writer hostgroup rejects %s", label);
	ok(!PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_EPOCH}.matches(
			PolarDB_WriterScope{POLARDB_SCOPE_WRITER_HG, POLARDB_SCOPE_CHANGED_EPOCH}),
		"same writer hostgroup with changed epoch rejects %s", label);
}

// ============================================================================
// Section 2: full-harness HGM fixtures (POLARDB_UNIT_FULL_HARNESS only)
// ============================================================================
//
// These reference real ProxySQL types (SQLite3_result, PgSQL_HGC, PgSQL_SrvC,
// PgSQL_Connection, PgSQL_HostGroups_Manager, the Prometheus registry). They are
// only compiled into the full-harness HGM test, which defines
// POLARDB_UNIT_FULL_HARNESS after including the harness headers it needs.

#ifdef POLARDB_UNIT_FULL_HARNESS

/**
 * @brief Build a single-row pgsql_replication_hostgroups SQLite3_result.
 *
 * Column legend (9 columns):
 *   [0] writer_hostgroup  [1] reader_hostgroup  [2] check_type ("polardb")
 *   [3] txn_split ("0")  [4] consistency ("lsn")  [5] max_lag_bytes ("1000")
 *   [6] lsn_wait_timeout_ms ("0")  [7] proxy_protocol ("v15")  [8] comment
 */
static SQLite3_result *make_polardb_replication_row_with_protocol(
		int writer_hg, int reader_hg, const char* proxy_protocol,
		const char* txn_split = "0") {
	SQLite3_result *result = new SQLite3_result(9);
	char writer_buf[16];   // holds an int formatted as decimal text
	char reader_buf[16];
	snprintf(writer_buf, sizeof(writer_buf), "%d", writer_hg);
	snprintf(reader_buf, sizeof(reader_buf), "%d", reader_hg);

	char *row[] = {
		writer_buf,
		reader_buf,
		(char*)"polardb",
		(char*)(txn_split ? txn_split : "0"),
		(char*)"lsn",
		(char*)"1000",
		(char*)"0",
		(char*)(proxy_protocol ? proxy_protocol : "v15"),
		(char*)"writer epoch unit"
	};
	result->add_row(row);
	return result;
}

static SQLite3_result *make_polardb_replication_row(int writer_hg, int reader_hg) {
	return make_polardb_replication_row_with_protocol(writer_hg, reader_hg, "v15");
}

/**
 * @brief Build a two-row (writer + reader) pgsql_servers SQLite3_result.
 *
 * Column legend (11 columns):
 *   [0] hostgroup  [1] address  [2] port  [3] status ("ONLINE")
 *   [4] weight ("1")  [5] compression ("0")  [6] max_connections ("50")
 *   [7] max_replication_lag ("0")  [8] use_ssl ("0")  [9] max_latency_ms ("0")
 *   [10] comment
 */
static SQLite3_result *make_pgsql_servers_result(
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_addr, int reader_port) {
	SQLite3_result *result = new SQLite3_result(11);
	char writer_hg_buf[16];     // each holds an int formatted as decimal text
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char reader_port_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port_buf, sizeof(reader_port_buf), "%d", reader_port);

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
		(char*)"polardb writer epoch unit"
	};
	result->add_row(writer_row);

	char *reader_row[] = {
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

static SQLite3_result *make_pgsql_servers_result_two_readers(
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg,
		const char *reader_addr1, int reader_port1,
		const char *reader_addr2, int reader_port2,
		int reader_weight1 = 1, int reader_weight2 = 1,
		int reader_max_connections1 = 50,
		int reader_max_connections2 = 50) {
	SQLite3_result *result = new SQLite3_result(11);
	char writer_hg_buf[16];
	char writer_port_buf[16];
	char reader_hg_buf[16];
	char reader_port1_buf[16];
	char reader_port2_buf[16];
	char reader_weight1_buf[16];
	char reader_weight2_buf[16];
	char reader_max_connections1_buf[16];
	char reader_max_connections2_buf[16];
	snprintf(writer_hg_buf, sizeof(writer_hg_buf), "%d", writer_hg);
	snprintf(writer_port_buf, sizeof(writer_port_buf), "%d", writer_port);
	snprintf(reader_hg_buf, sizeof(reader_hg_buf), "%d", reader_hg);
	snprintf(reader_port1_buf, sizeof(reader_port1_buf), "%d", reader_port1);
	snprintf(reader_port2_buf, sizeof(reader_port2_buf), "%d", reader_port2);
	snprintf(reader_weight1_buf, sizeof(reader_weight1_buf), "%d", reader_weight1);
	snprintf(reader_weight2_buf, sizeof(reader_weight2_buf), "%d", reader_weight2);
	snprintf(reader_max_connections1_buf,
		sizeof(reader_max_connections1_buf), "%d", reader_max_connections1);
	snprintf(reader_max_connections2_buf,
		sizeof(reader_max_connections2_buf), "%d", reader_max_connections2);

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
		(char*)"1000",
		(char*)"polardb two-reader unit writer"
	};
	result->add_row(writer_row);

	char *reader1_row[] = {
		reader_hg_buf,
		(char*)reader_addr1,
		reader_port1_buf,
		(char*)"ONLINE",
		reader_weight1_buf,
		(char*)"0",
		reader_max_connections1_buf,
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb two-reader unit reader1"
	};
	result->add_row(reader1_row);

	char *reader2_row[] = {
		reader_hg_buf,
		(char*)reader_addr2,
		reader_port2_buf,
		(char*)"ONLINE",
		reader_weight2_buf,
		(char*)"0",
		reader_max_connections2_buf,
		(char*)"0",
		(char*)"0",
		(char*)"1000",
		(char*)"polardb two-reader unit reader2"
	};
	result->add_row(reader2_row);
	return result;
}

/// @brief Find a server in a hostgroup by literal address + port, or nullptr.
static PgSQL_SrvC *find_pgsql_server(PgSQL_HGC *hgc, const char *addr, int port) {
	if (!hgc || !hgc->mysrvs || !addr) return nullptr;

	for (unsigned int i = 0; i < hgc->mysrvs->cnt(); ++i) {
		PgSQL_SrvC *srv = hgc->mysrvs->idx(i);
		if (srv && srv->address && strcmp(srv->address, addr) == 0 &&
				srv->port == port) {
			return srv;
		}
	}
	return nullptr;
}

/// @brief Read the first sample of a Prometheus counter family by name.
/// @return true and writes *value when the named family has at least one metric.
static bool find_prometheus_counter_value(const char *name, double *value) {
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

/// @brief Read the first sample of a Prometheus gauge family by name.
/// @return true and writes *value when the named family has at least one metric.
static bool find_prometheus_gauge_value(const char *name, double *value) {
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

/// @brief Fill a connection with the shared PolarDB unit-test user/db identity.
static void set_test_userinfo(PgSQL_Connection *conn) {
	conn->userinfo->set(
		(char*)"polardb_unit_user",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
}

/// @brief Fill a connection with ProxySQL's default critical PostgreSQL variables.
static void set_test_pgsql_defaults(PgSQL_Connection *conn) {
	for (int idx = 0; idx < PGSQL_NAME_LAST_LOW_WM; idx++) {
		const char *value = pgsql_tracked_variables[idx].default_value;
		conn->var_hash[idx] = SpookyHash::Hash32(value, strlen(value), 10);
		if (conn->variables[idx].value) {
			free(conn->variables[idx].value);
		}
		conn->variables[idx].value = strdup(value);
	}
}

/// @brief Build a reusable idle reader connection (V15 RFQ profile) on @p reader.
static PgSQL_Connection *make_cached_reader_connection(PgSQL_SrvC *reader) {
	PgSQL_Connection *conn = new PgSQL_Connection(false);
	set_test_userinfo(conn);
	set_test_pgsql_defaults(conn);
	conn->parent = reader;
	conn->async_state_machine = ASYNC_IDLE;
	conn->reusable = true;
	conn->polardb_startup_profile = make_v15_profile();
	conn->polardb_startup_identity_mode =
		static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
	conn->polardb_startup_profile_generation =
		conn->polardb_startup_profile.generation(
			conn->polardb_startup_identity_mode);
	conn->polardb_startup_client.identity =
		PolarDB_StartupIdentity{
			"127.0.0.10",
			6033,
			PolarDB_StartupIdentitySource::LISTENER_PROXY};
	return conn;
}

/**
 * @brief Stage a writer+reader topology and commit it through the HGM.
 *
 * Runs the repeated servers_add(...) + save_incoming_pgsql_table(...) +
 * commit({}, {}, false, false) sequence and asserts each step, using @p label as
 * the message prefix so the two call sites stay distinguishable.
 *
 * Emits exactly 2 ok() assertions (the staging add and the commit).
 */
static inline void stage_polardb_topology(
		PgSQL_HostGroups_Manager *hgm,
		const char *label,
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_addr, int reader_port) {
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

static inline void stage_polardb_topology_with_txn_split(
		PgSQL_HostGroups_Manager *hgm,
		const char *label,
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg, const char *reader_addr, int reader_port) {
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

static inline void stage_polardb_topology_two_readers(
		PgSQL_HostGroups_Manager *hgm,
		const char *label,
		int writer_hg, const char *writer_addr, int writer_port,
		int reader_hg,
		const char *reader_addr1, int reader_port1,
		const char *reader_addr2, int reader_port2,
		int reader_weight1 = 1, int reader_weight2 = 1) {
	ok(hgm->servers_add(make_pgsql_servers_result_two_readers(
			writer_hg, writer_addr, writer_port,
			reader_hg, reader_addr1, reader_port1,
			reader_addr2, reader_port2,
			reader_weight1, reader_weight2)) == 0,
		"%s: writer and two readers staged for commit", label);
	hgm->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(hgm->commit({}, {}, false, false),
		"%s: two-reader topology commit succeeds", label);
}

#endif // POLARDB_UNIT_FULL_HARNESS

#endif // POLARDB_UNIT_COMMON_H
