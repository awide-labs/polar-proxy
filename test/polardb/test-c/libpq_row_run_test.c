/**
 * @file libpq_row_run_test.c
 * @brief Offline unit test for the PolarDB libpq DataRow row-run API.
 *
 * This test constructs a minimal PGconn receive buffer and validates
 * PSpeekRowRun(), PSadvanceInput(), and PSdetachRowRun() before ProxySQL
 * consumes the API.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"

static int tests_passed = 0;
static int tests_failed = 0;

#define PASS(msg, ...) do { printf("[PASS] " msg "\n", ##__VA_ARGS__); tests_passed++; } while (0)
#define FAIL(msg, ...) do { printf("[FAIL] " msg "\n", ##__VA_ARGS__); tests_failed++; } while (0)
#define CHECK(cond, msg, ...) do { if (cond) { PASS(msg, ##__VA_ARGS__); } else { FAIL(msg, ##__VA_ARGS__); } } while (0)

static void put_u16(char *p, uint16_t v) {
	p[0] = (char)((v >> 8) & 0xff);
	p[1] = (char)(v & 0xff);
}

static void put_u32(char *p, uint32_t v) {
	p[0] = (char)((v >> 24) & 0xff);
	p[1] = (char)((v >> 16) & 0xff);
	p[2] = (char)((v >> 8) & 0xff);
	p[3] = (char)(v & 0xff);
}

static size_t append_datarow(char *buf, size_t off, const char *value) {
	const size_t vlen = strlen(value);
	const uint32_t msg_len = (uint32_t)(4 + 2 + 4 + vlen);
	buf[off++] = 'D';
	put_u32(buf + off, msg_len); off += 4;
	put_u16(buf + off, 1); off += 2;
	put_u32(buf + off, (uint32_t)vlen); off += 4;
	memcpy(buf + off, value, vlen); off += vlen;
	return off;
}

static size_t append_bad_field_count_datarow(char *buf, size_t off) {
	const uint32_t msg_len = 4 + 2;
	buf[off++] = 'D';
	put_u32(buf + off, msg_len); off += 4;
	put_u16(buf + off, 2); off += 2;
	return off;
}

static size_t append_bad_length_datarow(char *buf, size_t off) {
	buf[off++] = 'D';
	put_u32(buf + off, 3); off += 4;
	return off;
}

static size_t append_command_complete(char *buf, size_t off) {
	const char tag[] = "SELECT 2";
	const uint32_t msg_len = (uint32_t)(4 + sizeof(tag));
	buf[off++] = 'C';
	put_u32(buf + off, msg_len); off += 4;
	memcpy(buf + off, tag, sizeof(tag)); off += sizeof(tag);
	return off;
}

static PGresult *make_one_column_result(void) {
	PGresult *res = PQmakeEmptyPGresult(NULL, PGRES_TUPLES_OK);
	PGresAttDesc att;
	memset(&att, 0, sizeof(att));
	att.name = (char *)"c1";
	att.format = 0;
	att.typid = 25; /* text */
	att.typlen = -1;
	if (!res || !PQsetResultAttrs(res, 1, &att)) {
		fprintf(stderr, "failed to create one-column PGresult\n");
		exit(2);
	}
	return res;
}

static void setup_conn(PGconn *conn, PGresult *res, char *buf, size_t len, size_t cap) {
	memset(conn, 0, sizeof(*conn));
	conn->asyncStatus = PGASYNC_BUSY;
	conn->result = res;
	conn->inBuffer = buf;
	conn->inBufSize = (int)cap;
	conn->inStart = 0;
	conn->inCursor = 0;
	conn->inEnd = (int)len;
}

static void test_multi_row_run_stops_before_control(void) {
	char buf[256];
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	size_t row1_end;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_datarow(buf, off, "alpha");
	row1_end = off;
	off = append_datarow(buf, off, "beta");
	off = append_command_complete(buf, off);

	setup_conn(&conn, res, buf, off, sizeof(buf));
	CHECK(PSrowRunPending(&conn) == 1, "row-run pending detects DataRow at input start");
	rc = PSpeekRowRun(&conn, &data, &len, &frames, 0);
	CHECK(rc == 0, "multi-row run is available rc=%d", rc);
	CHECK(data == buf, "row-run data points at inStart");
	CHECK(frames == 2, "row-run contains exactly two DataRow frames (%d)", frames);
	CHECK(len == row1_end + (strlen("beta") + 11), "row-run stops before CommandComplete len=%zu", len);
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "peek does not advance cursor");

	PSadvanceInput(&conn, len);
	CHECK(conn.inStart == (int)len && conn.inCursor == (int)len, "advance consumes exactly row-run bytes");
	CHECK(conn.inBuffer[conn.inStart] == 'C', "advance leaves control frame for libpq path");
	CHECK(PSrowRunPending(&conn) == 0, "row-run pending rejects control frame at input start");

	data = NULL; len = 0; frames = 0;
	rc = PSpeekRowRun(&conn, &data, &len, &frames, 0);
	CHECK(rc == 1 && frames == 0 && len == 0, "control frame falls through rc=%d frames=%d len=%zu", rc, frames, len);
	PQclear(res);
}

static void test_budget_limits_run_after_first_frame(void) {
	char buf[256];
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	size_t row1_len;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_datarow(buf, off, "one");
	row1_len = off;
	off = append_datarow(buf, off, "two");

	setup_conn(&conn, res, buf, off, sizeof(buf));
	rc = PSpeekRowRun(&conn, &data, &len, &frames, row1_len);
	CHECK(rc == 0, "budgeted row-run is available rc=%d", rc);
	CHECK(frames == 1, "budget keeps one frame (%d)", frames);
	CHECK(len == row1_len, "budgeted run length equals first frame (%zu/%zu)", len, row1_len);
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "budgeted peek does not advance cursor");
	PQclear(res);
}

static void test_incomplete_first_frame_returns_eof(void) {
	char buf[256];
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_datarow(buf, off, "partial");
	setup_conn(&conn, res, buf, off - 2, sizeof(buf));
	CHECK(PSrowRunPending(&conn) == 1, "row-run pending allows incomplete DataRow to reach detach/peek");
	rc = PSpeekRowRun(&conn, &data, &len, &frames, 0);
	CHECK(rc == EOF, "incomplete first DataRow returns EOF rc=%d", rc);
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "incomplete peek does not advance cursor");
	PQclear(res);
}

static void test_bad_length_falls_back(void) {
	char buf[256];
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_bad_length_datarow(buf, off);
	setup_conn(&conn, res, buf, off, sizeof(buf));
	rc = PSpeekRowRun(&conn, &data, &len, &frames, 0);
	CHECK(rc == 1, "bad frame length falls back rc=%d", rc);
	CHECK(frames == 0 && len == 0 && data == NULL, "bad frame length exposes no row-run");
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "bad frame length does not advance cursor");
	PQclear(res);
}

static void test_bad_field_count_is_header_only_in_release(void) {
	char buf[256];
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_bad_field_count_datarow(buf, off);
	setup_conn(&conn, res, buf, off, sizeof(buf));
	rc = PSpeekRowRun(&conn, &data, &len, &frames, 0);
	CHECK(rc == 0, "release row-run trusts complete DataRow envelope rc=%d", rc);
	CHECK(frames == 1 && len == off && data == buf, "release row-run exposes complete DataRow envelope");
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "release row-run peek does not advance cursor");
	PQclear(res);
}

static void test_detach_row_run_rotates_buffer_and_preserves_tail(void) {
	char *buf = (char *)malloc(256);
	PGconn conn;
	PGresult *res = make_one_column_result();
	size_t off = 0;
	size_t row1_end;
	char *owned = NULL;
	const char *data = NULL;
	size_t len = 0;
	int frames = 0;
	int rc;

	off = append_datarow(buf, off, "alpha");
	row1_end = off;
	off = append_datarow(buf, off, "beta");
	off = append_command_complete(buf, off);

	setup_conn(&conn, res, buf, off, 256);
	rc = PSdetachRowRun(&conn, &owned, &data, &len, &frames, 0);
	CHECK(rc == 0, "detach row-run is available rc=%d", rc);
	CHECK(owned == buf, "detach transfers the original inBuffer");
	CHECK(data == owned, "detached data points inside the owned buffer");
	CHECK(frames == 2, "detached run contains two frames (%d)", frames);
	CHECK(len == row1_end + (strlen("beta") + 11), "detached run length stops before CommandComplete len=%zu", len);
	CHECK(conn.inBuffer != owned, "libpq receives a fresh inBuffer after detach");
	CHECK(conn.inStart == 0 && conn.inCursor == 0, "detach resets parser cursor to fresh buffer start");
	CHECK(conn.inEnd > 0 && conn.inBuffer[0] == 'C', "detach preserves control-frame tail for libpq");

	free(owned);
	free(conn.inBuffer);
	conn.inBuffer = NULL;
	PQclear(res);
}

int main(void) {
	test_multi_row_run_stops_before_control();
	test_budget_limits_run_after_first_frame();
	test_incomplete_first_frame_returns_eof();
	test_bad_length_falls_back();
	test_bad_field_count_is_header_only_in_release();
	test_detach_row_run_rotates_buffer_and_preserves_tail();

	printf("\nlibpq row-run tests: passed=%d failed=%d\n", tests_passed, tests_failed);
	return tests_failed == 0 ? 0 : 1;
}
