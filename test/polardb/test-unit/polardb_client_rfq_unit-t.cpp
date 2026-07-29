/**
 * @file polardb_client_rfq_unit-t.cpp
 * @brief Unit tests for client-facing PolarDB ReadyForQuery LSN packets.
 *
 * Domain: backend-backed ReadyForQuery forwarding. The standard PostgreSQL RFQ
 * packet must stay byte-for-byte unchanged unless the client opted in and the
 * backend RFQ carried a PolarDB LSN. The opt-in form appends one uint64 LSN
 * after the transaction-status byte.
 */

#include "tap.h"

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <utility>

#include "PgSQL_Protocol.h"
#include "PgSQL_Connection.h"
#include "PgSQL_PolarDB.h"

#if !POLARDB_PROXY
int main() {
	plan(1);
	ok(1, "PolarDB client RFQ unit skipped when POLARDB_PROXY is disabled");
	return exit_status();
}
#else

static uint32_t read_be32(const unsigned char *p) {
	return ((uint32_t)p[0] << 24) |
		((uint32_t)p[1] << 16) |
		((uint32_t)p[2] << 8) |
		(uint32_t)p[3];
}

static uint64_t read_be64(const unsigned char *p) {
	return ((uint64_t)read_be32(p) << 32) | read_be32(p + 4);
}

static const unsigned char *single_result_packet(PtrSizeArray& out) {
	if (out.len != 1) {
		return nullptr;
	}
	return reinterpret_cast<const unsigned char*>(out.index(0)->ptr);
}

static unsigned int single_result_packet_size(PtrSizeArray& out) {
	if (out.len != 1) {
		return 0;
	}
	return out.index(0)->size;
}

static void release_packets(PtrSizeArray& out) {
	while (out.len) {
		PtrSize_t pkt {};
		out.remove_index_fast(0, &pkt);
		l_free(pkt.size, pkt.ptr);
	}
}

static void test_standard_ready_for_query_shape() {
	PgSQL_Protocol proto;
	PgSQL_Connection conn(true);
	PgSQL_Query_Result result;
	result.init(&proto, nullptr, &conn);

	const unsigned int bytes =
		result.add_ready_status(PQTRANS_INTRANS, false, 0x0102030405060708ULL);
	PtrSizeArray out;
	const bool complete = result.get_resultset(&out);
	const unsigned char *pkt = single_result_packet(out);

	ok(complete, "client RFQ: non-LSN result is complete after RFQ");
	ok(bytes == 6, "client RFQ: non-LSN path reports standard 6-byte packet");
	ok(single_result_packet_size(out) == 6,
		"client RFQ: non-LSN path stores standard 6-byte packet");
	ok(pkt != nullptr && pkt[0] == 'Z',
		"client RFQ: non-LSN path emits ReadyForQuery message type");
	ok(pkt != nullptr && read_be32(pkt + 1) == 5,
		"client RFQ: non-LSN path uses PostgreSQL RFQ length 5");
	ok(pkt != nullptr && pkt[5] == 'T',
		"client RFQ: non-LSN path preserves transaction status");
	release_packets(out);
}

static void test_polardb_lsn_ready_for_query_shape() {
	static const uint64_t LSN = 0x0102030405060708ULL;

	PgSQL_Protocol proto;
	PgSQL_Connection conn(true);
	PgSQL_Query_Result result;
	result.init(&proto, nullptr, &conn);

	const unsigned int bytes =
		result.add_ready_status(PQTRANS_INERROR, true, LSN);
	PtrSizeArray out;
	const bool complete = result.get_resultset(&out);
	const unsigned char *pkt = single_result_packet(out);

	ok(complete, "client RFQ: LSN result is complete after RFQ");
	ok(bytes == 14, "client RFQ: LSN path reports 14-byte packet");
	ok(single_result_packet_size(out) == 14,
		"client RFQ: LSN path stores 14-byte packet");
	ok(pkt != nullptr && pkt[0] == 'Z',
		"client RFQ: LSN path emits ReadyForQuery message type");
	ok(pkt != nullptr && read_be32(pkt + 1) == 13,
		"client RFQ: LSN path includes status byte plus uint64 in length");
	ok(pkt != nullptr && pkt[5] == 'E',
		"client RFQ: LSN path preserves transaction error status");
	ok(pkt != nullptr && read_be64(pkt + 6) == LSN,
		"client RFQ: LSN path appends backend LSN in network byte order");
	release_packets(out);

	PgSQL_Query_Result zero_result;
	zero_result.init(&proto, nullptr, &conn);
	const unsigned int zero_bytes =
		zero_result.add_ready_status(PQTRANS_IDLE, true, 0);
	PtrSizeArray zero_out;
	const bool zero_complete = zero_result.get_resultset(&zero_out);
	const unsigned char *zero_pkt = single_result_packet(zero_out);
	ok(zero_complete && zero_bytes == 14 &&
			single_result_packet_size(zero_out) == 14,
		"client RFQ: present-zero LSN keeps the PolarDB packet shape");
	ok(zero_pkt != nullptr && read_be64(zero_pkt + 6) == 0,
		"client RFQ: present-zero LSN remains distinct from a missing payload");
	release_packets(zero_out);
}

static void test_polardb_client_rfq_decision() {
	PolarDB_ClientRfqDecision d = polardb_client_rfq_decision(
		true, true, 100, 200, false, false, 0);
	ok(d.include_lsn && d.lsn == 100 && !d.raised_to_target,
		"client RFQ decision: no-wait replica preserves backend LSN");

	d = polardb_client_rfq_decision(
		true, true, 100, 200, false, true, 0);
	ok(d.include_lsn && d.lsn == 200 && d.raised_to_target &&
			d.raised_by_writer && !d.raised_by_wait,
		"client RFQ decision: writer response raises to session target");

	d = polardb_client_rfq_decision(
		true, true, 300, 200, false, true, 0);
	ok(d.include_lsn && d.lsn == 300 && !d.raised_to_target,
		"client RFQ decision: writer response preserves newer backend LSN");

	d = polardb_client_rfq_decision(
		true, true, 0, 0, false, true, 0);
	ok(d.include_lsn && d.lsn == 0 && !d.raised_to_target,
		"client RFQ decision: present-zero remains an explicit LSN payload");

	d = polardb_client_rfq_decision(
		true, true, 0, 0, false, false, 300, 300);
	ok(d.include_lsn && d.lsn == 300 && d.raised_to_target &&
			!d.raised_by_writer && d.raised_by_wait,
		"client RFQ decision: successful wait raises zero backend LSN");

	d = polardb_client_rfq_decision(
		true, true, 100, 0, false, false, 300);
	ok(d.include_lsn && d.lsn == 300 && d.raised_to_target &&
			!d.raised_by_writer && !d.raised_by_wait,
		"client RFQ decision: cached-reader bypass is not counted as a completed wait");

	d = polardb_client_rfq_decision(
		true, true, 100, 300, false, true, 300, 300);
	ok(d.include_lsn && d.lsn == 300 && d.raised_to_target &&
			d.raised_by_writer && d.raised_by_wait,
		"client RFQ decision: equal writer and wait checks record both reasons");

	d = polardb_client_rfq_decision(
		true, false, 0, 200, false, true, 0);
	ok(!d.include_lsn && d.lsn == 0,
		"client RFQ decision: missing backend payload does not emit LSN");

	d = polardb_client_rfq_decision(
		false, true, 100, 200, false, true, 0);
	ok(!d.include_lsn,
		"client RFQ decision: client opt-in is required");

	d = polardb_client_rfq_decision(
		true, true, 100, 200, true, true, 0);
	ok(d.include_lsn && d.lsn == 200 && d.raised_to_target &&
			d.raised_by_writer && !d.raised_by_wait,
		"client RFQ decision: saved session baseline records a writer raise");

	d = polardb_client_rfq_decision(
		true, true, 100, 200, true, true, 300, 300);
	ok(d.include_lsn && d.lsn == 300 && d.raised_to_target &&
			!d.raised_by_writer && d.raised_by_wait,
		"client RFQ decision: higher wait target supersedes saved session baseline");

	d = polardb_client_rfq_decision(
		true, true, 300, 200, true, true, 0);
	ok(d.include_lsn && d.lsn == 200 && !d.raised_to_target,
		"client RFQ decision: saved session baseline can suppress unrelated backend LSN");

	d = polardb_client_rfq_decision(
		true, true, 300, 200, true, true, 250, 250);
	ok(d.include_lsn && d.lsn == 250 && !d.raised_to_target,
		"client RFQ decision: raise accounting remains relative to raw backend LSN");

	d = polardb_client_rfq_decision(
		true, true, 300, 200, true, false, 0);
	ok(d.include_lsn && d.lsn == 300 && !d.raised_to_target,
		"client RFQ decision: saved session baseline requires a writer response");
}

int main() {
	plan(29);
	test_standard_ready_for_query_shape();
	test_polardb_lsn_ready_for_query_shape();
	test_polardb_client_rfq_decision();
	return exit_status();
}

#endif // POLARDB_PROXY
