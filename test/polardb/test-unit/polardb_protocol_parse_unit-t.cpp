/**
 * @file polardb_protocol_parse_unit-t.cpp
 * @brief Unit tests for the PolarDB protocol/string parsing helpers.
 *
 * Domain: node-type name mapping and writer/reader classification, monitor-health
 * parse helpers (availability, LSN text, update gate), and simple-query
 * multi-statement detection.
 */

#include "tap.h"
#include "PgSQL_PolarDB.h"
#include "polardb_unit_common.h"

#include <cstring>

// ---- node-type & monitor-health string parsers ----

static void test_parse_node_type_names() {
	ok(PolarDB_Protocol::parse_node_type(nullptr) == PolarDB_NodeType::UNKNOWN,
		"node type parser maps null to unknown");
	ok(PolarDB_Protocol::parse_node_type("primary") == PolarDB_NodeType::PRIMARY,
		"node type parser maps primary");
	ok(PolarDB_Protocol::parse_node_type("master") == PolarDB_NodeType::PRIMARY,
		"node type parser maps master alias to primary");
	ok(PolarDB_Protocol::parse_node_type("replica") == PolarDB_NodeType::REPLICA,
		"node type parser maps replica");
	ok(PolarDB_Protocol::parse_node_type("standby") == PolarDB_NodeType::STANDBY,
		"node type parser maps standby");
	ok(PolarDB_Protocol::parse_node_type("other") == PolarDB_NodeType::UNKNOWN,
		"node type parser maps unknown string to unknown");
	ok(PolarDB_Protocol::is_writer(PolarDB_NodeType::PRIMARY),
		"node type helper treats primary as writer");
	ok(!PolarDB_Protocol::is_writer(PolarDB_NodeType::REPLICA),
		"node type helper treats replica as non-writer");
	ok(PolarDB_Protocol::is_reader(PolarDB_NodeType::REPLICA),
		"node type helper treats replica as reader");
	ok(PolarDB_Protocol::is_reader(PolarDB_NodeType::STANDBY),
		"node type helper treats standby as reader");
	ok(!PolarDB_Protocol::is_reader(PolarDB_NodeType::UNKNOWN),
		"node type helper treats unknown as non-reader");
	ok(PolarDB_Protocol::node_type_to_read_only(PolarDB_NodeType::PRIMARY) == 0,
		"read-only conversion maps primary to writer");
	ok(PolarDB_Protocol::node_type_to_read_only(PolarDB_NodeType::REPLICA) == 1,
		"read-only conversion maps replica to reader");
	ok(PolarDB_Protocol::node_type_to_read_only(PolarDB_NodeType::UNKNOWN) == 1,
		"read-only conversion maps unknown to non-writer");
}

static void test_monitor_health_parse_helpers() {
	ok(PolarDB_Protocol::parse_is_available(nullptr),
		"availability parser defaults missing value to available");
	ok(PolarDB_Protocol::parse_is_available("t"),
		"availability parser accepts lowercase true");
	ok(PolarDB_Protocol::parse_is_available("T"),
		"availability parser accepts uppercase true");
	ok(!PolarDB_Protocol::parse_is_available("f"),
		"availability parser rejects lowercase false");
	ok(!PolarDB_Protocol::parse_is_available("unexpected"),
		"availability parser treats non-true values as unavailable");

	ok(PolarDB_Protocol::parse_lsn_string(nullptr) == 0,
		"LSN parser maps missing value to zero");
	ok(PolarDB_Protocol::parse_lsn_string("0/1234ABCD") == 0x1234ABCDULL,
		"LSN parser accepts ordinary PostgreSQL LSN text");
	ok(PolarDB_Protocol::parse_lsn_string("1/5") ==
			((1ULL << 32) | 5ULL),
		"LSN parser combines high and low WAL halves");
	ok(PolarDB_Protocol::parse_lsn_string("not-an-lsn") == 0,
		"LSN parser maps malformed text to zero");
	ok(PolarDB_Protocol::parse_lsn_string("1/5junk") == 0,
		"LSN parser rejects partially parsed LSN text");

	ok(!polardb_should_update_monitor_lsn(false, 500),
		"monitor LSN update gate respects disabled monitor updates");
	ok(!polardb_should_update_monitor_lsn(true, 0),
		"monitor LSN update gate rejects zero LSN");
	ok(polardb_should_update_monitor_lsn(true, 500),
		"monitor LSN update gate accepts enabled positive LSN");
}

// ---- multi-statement detection ----

static void test_simple_query_multi_statement_detection() {
	ok(!polardb_query_has_multiple_statements("SELECT 1", strlen("SELECT 1")),
		"single statement without semicolon is not multi-statement");
	ok(!polardb_query_has_multiple_statements("SELECT 1;", strlen("SELECT 1;")),
		"single statement with one trailing semicolon is not multi-statement");
	ok(!polardb_query_has_multiple_statements(" \tSELECT 1; \r\n",
			strlen(" \tSELECT 1; \r\n")),
		"single statement allows surrounding whitespace and one trailing semicolon");

	// This is intentionally conservative, not SQL-literal-aware: an apparent
	// separator keeps the query on the writer rather than risking wrapper result
	// count mismatch.
	ok(polardb_query_has_multiple_statements("SELECT ';'", strlen("SELECT ';'")),
		"semicolon inside SQL literal is conservatively treated as multi-statement");

	const char wire_query[] = {'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', ';', '\0'};
	ok(!polardb_query_has_multiple_statements(wire_query, sizeof(wire_query)),
		"PostgreSQL simple-query wire terminator is ignored after trailing semicolon");

	const char wire_spaced_query[] = {
		' ', 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', ';', ' ', '\0'};
	ok(!polardb_query_has_multiple_statements(
			wire_spaced_query, sizeof(wire_spaced_query)),
		"PostgreSQL simple-query wire terminator is ignored after whitespace");

	ok(polardb_query_has_multiple_statements(
			"SELECT 1; SELECT 2", strlen("SELECT 1; SELECT 2")),
		"two statements separated by semicolon are multi-statement");
	ok(polardb_query_has_multiple_statements(
			"SELECT 1; SELECT 2;", strlen("SELECT 1; SELECT 2;")),
		"two statements remain multi-statement with one final semicolon");

	const char wire_multi_query[] = {
		'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', ';', ' ',
		'S', 'E', 'L', 'E', 'C', 'T', ' ', '2', ';', '\0'};
	ok(polardb_query_has_multiple_statements(
			wire_multi_query, sizeof(wire_multi_query)),
		"PostgreSQL wire terminator does not hide a real second statement");
}

int main() {
	// 36 ok() in this file = 36.
	plan(36);
	test_parse_node_type_names();
	test_monitor_health_parse_helpers();
	test_simple_query_multi_statement_detection();
	return exit_status();
}
