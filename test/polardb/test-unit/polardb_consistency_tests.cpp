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
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;
extern bool polardb_is_lsn_wait_timeout_result(const PGresult* result);

#if POLARDB_PROXY

static void test_wait_timeout_fields() {
	PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_FATAL_ERROR);
	ok(result != nullptr,
		"PolarDB timeout fields: result fixture created");
	if (!result) {
		return;
	}
	pqSaveMessageField(result, PG_DIAG_MESSAGE_DETAIL,
		POLARDB_LSN_WAIT_TIMEOUT_DETAIL);
	ok(!polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout fields: DETAIL alone is rejected");
	pqSaveMessageField(result, PG_DIAG_SOURCE_FUNCTION,
		"client_controlled_function");
	ok(!polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout fields: another source function is rejected");
	pqSaveMessageField(result, PG_DIAG_SOURCE_FUNCTION,
		POLARDB_LSN_WAIT_TIMEOUT_SOURCE_FUNCTION);
	ok(polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout fields: exact DETAIL and source function are accepted");
	PQclear(result);
}

struct PolarDB_WireCapture {
	bool fixture_ready{false};
	int send_rc{0};
	int flush_rc{-1};
	std::vector<unsigned char> bytes;
	std::string trace;
};

template <typename Sender>
static PolarDB_WireCapture capture_libpq_wire(Sender sender) {
	PolarDB_WireCapture capture;
	int sockets[2] = {-1, -1};
	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
		return capture;
	}
	for (int socket_fd : sockets) {
		const int flags = fcntl(socket_fd, F_GETFL, 0);
		if (flags < 0 ||
				fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) != 0) {
			close(sockets[0]);
			close(sockets[1]);
			return capture;
		}
	}

	PGconn* conn = PQconnectStart("polardb_unit_invalid_conninfo=1");
	if (!conn) {
		close(sockets[0]);
		close(sockets[1]);
		return capture;
	}
	pqDropConnection(conn, true);
	conn->sock = sockets[0];
	// The fixture models a network backend. AF_UNIX makes libpq coalesce writes
	// into 8 KiB chunks and can retain a valid short protocol frame after PQflush.
	conn->raddr.addr.ss_family = AF_INET;
	conn->write_failed = false;
	free(conn->write_err_msg);
	conn->write_err_msg = nullptr;
	conn->status = CONNECTION_OK;
	conn->asyncStatus = PGASYNC_IDLE;
	conn->pipelineStatus = PQ_PIPELINE_OFF;
	if (PQsetnonblocking(conn, 1) != 0) {
		PQfinish(conn);
		close(sockets[1]);
		return capture;
	}
	FILE* trace_file = tmpfile();
	if (trace_file) {
		PQtrace(conn, trace_file);
		PQsetTraceFlags(conn, PQTRACE_SUPPRESS_TIMESTAMPS);
	}
	capture.fixture_ready = true;
	if constexpr (std::is_invocable_v<Sender, PGconn*, int>) {
		capture.send_rc = sender(conn, sockets[1]);
	} else {
		capture.send_rc = sender(conn);
	}
	if (capture.send_rc == 1) {
		capture.flush_rc = PQflush(conn);
	}
	if (capture.flush_rc == 0) {
		struct pollfd peer = {sockets[1], POLLIN, 0};
		int poll_rc;
		do {
			poll_rc = poll(&peer, 1, 1000);
		} while (poll_rc < 0 && errno == EINTR);
	}

	unsigned char buf[512];
	for (;;) {
		const ssize_t received = recv(
			sockets[1], buf, sizeof(buf), MSG_DONTWAIT);
		if (received > 0) {
			capture.bytes.insert(
				capture.bytes.end(), buf, buf + received);
			continue;
		}
		if (received < 0 && errno == EINTR) {
			continue;
		}
		break;
	}
	if (trace_file) {
		fflush(trace_file);
		rewind(trace_file);
		char trace_buf[512];
		while (fgets(trace_buf, sizeof(trace_buf), trace_file)) {
			capture.trace.append(trace_buf);
		}
		PQuntrace(conn);
		fclose(trace_file);
	}

	PQfinish(conn);
	close(sockets[1]);
	return capture;
}

static uint32_t polardb_wire_u32(
		const std::vector<unsigned char>& wire, size_t offset) {
	return (static_cast<uint32_t>(wire[offset]) << 24) |
		(static_cast<uint32_t>(wire[offset + 1]) << 16) |
		(static_cast<uint32_t>(wire[offset + 2]) << 8) |
		static_cast<uint32_t>(wire[offset + 3]);
}

static uint64_t polardb_wire_u64(
		const std::vector<unsigned char>& wire, size_t offset) {
	return (static_cast<uint64_t>(polardb_wire_u32(wire, offset)) << 32) |
		polardb_wire_u32(wire, offset + 4);
}

static std::string polardb_wire_message_types(
		const std::vector<unsigned char>& wire) {
	std::string types;
	size_t offset = 0;
	while (offset + 5 <= wire.size()) {
		const uint32_t length = polardb_wire_u32(wire, offset + 1);
		if (length < 4 || offset + 1 + length > wire.size()) {
			return "invalid";
		}
		types.push_back(static_cast<char>(wire[offset]));
		offset += 1 + length;
	}
	return offset == wire.size() ? types : "invalid";
}

static bool polardb_wait_frame_matches(
		const std::vector<unsigned char>& wire,
		uint64_t target_lsn, uint32_t timeout_ms, int mode) {
	return wire.size() >= 21 && wire[0] == 'W' &&
		polardb_wire_u32(wire, 1) == 20 &&
		wire[5] == 1 && wire[6] == static_cast<unsigned char>(mode) &&
		wire[7] == 0 && wire[8] == 0 &&
		polardb_wire_u32(wire, 9) == timeout_ms &&
		polardb_wire_u64(wire, 13) == target_lsn;
}

static bool polardb_wait_wire_matches(
		const char* label, const PolarDB_WireCapture& capture,
		const char* expected_types, uint64_t target_lsn,
		uint32_t timeout_ms, int mode) {
	const std::string types = polardb_wire_message_types(capture.bytes);
	const bool matches = polardb_wait_frame_matches(
		capture.bytes, target_lsn, timeout_ms, mode) &&
		types == expected_types;
	if (!matches) {
		if (capture.bytes.size() >= 21) {
			diag("%s: bytes=%zu types=%s first=%u length=%u version=%u "
				"mode=%u flags=%u/%u timeout=%u target=%llu",
				label, capture.bytes.size(), types.c_str(), capture.bytes[0],
				polardb_wire_u32(capture.bytes, 1), capture.bytes[5],
				capture.bytes[6], capture.bytes[7], capture.bytes[8],
				polardb_wire_u32(capture.bytes, 9),
				(unsigned long long)polardb_wire_u64(capture.bytes, 13));
		} else {
			diag("%s: bytes=%zu types=%s send_rc=%d flush_rc=%d",
				label, capture.bytes.size(), types.c_str(),
				capture.send_rc, capture.flush_rc);
		}
	}
	return matches;
}

static rlim_t polardb_current_virtual_bytes() {
	FILE* statm = fopen("/proc/self/statm", "r");
	unsigned long pages = 0;
	if (!statm || fscanf(statm, "%lu", &pages) != 1) {
		if (statm) {
			fclose(statm);
		}
		return 0;
	}
	fclose(statm);
	const long page_size = sysconf(_SC_PAGESIZE);
	return page_size > 0
		? static_cast<rlim_t>(pages) * static_cast<rlim_t>(page_size)
		: 0;
}

struct PolarDB_PrepareOomChildResult {
	bool launched{false};
	bool checkpoint_restored{false};
	bool clean_resend{false};
};

enum PolarDB_PrepareOomChildFailure : int {
	POLARDB_PREPARE_OOM_CHILD_OK = 0,
	POLARDB_PREPARE_OOM_CHILD_CHECKPOINT_FAILED = 1 << 0,
	POLARDB_PREPARE_OOM_CHILD_CLEAN_RESEND_FAILED = 1 << 1,
};

static PolarDB_PrepareOomChildResult run_prepare_oom_child_process() {
	char executable[4096];
	const ssize_t executable_len = readlink(
		"/proc/self/exe", executable, sizeof(executable) - 1);
	if (executable_len <= 0) {
		return {};
	}
	executable[executable_len] = '\0';

	std::vector<std::string> environment_storage;
	for (char** entry = environ; entry && *entry; ++entry) {
		if (strncmp(*entry, "MALLOC_CONF=", 12) != 0 &&
				strncmp(*entry, "POLARDB_LIBPQ_PREPARE_OOM_CHILD=",
					sizeof("POLARDB_LIBPQ_PREPARE_OOM_CHILD=") - 1) != 0) {
			environment_storage.emplace_back(*entry);
		}
	}
	environment_storage.emplace_back("MALLOC_CONF=xmalloc:false");
	environment_storage.emplace_back("POLARDB_LIBPQ_PREPARE_OOM_CHILD=1");

	std::vector<char*> child_environment;
	child_environment.reserve(environment_storage.size() + 1);
	for (std::string& entry : environment_storage) {
		child_environment.push_back(entry.data());
	}
	child_environment.push_back(nullptr);
	char* const child_argv[] = {executable, nullptr};

	pid_t child_pid = -1;
	if (posix_spawn(
			&child_pid, executable, nullptr, nullptr, child_argv,
			child_environment.data()) != 0) {
		return {};
	}
	int child_status = 0;
	while (waitpid(child_pid, &child_status, 0) < 0) {
		if (errno != EINTR) {
			return {};
		}
	}
	if (!WIFEXITED(child_status)) {
		return {};
	}
	const int failures = WEXITSTATUS(child_status);
	return {
		true,
		(failures & POLARDB_PREPARE_OOM_CHILD_CHECKPOINT_FAILED) == 0,
		(failures & POLARDB_PREPARE_OOM_CHILD_CLEAN_RESEND_FAILED) == 0};
}

int run_polardb_libpq_prepare_oom_child() {
	const uint64_t failed_target = UINT64_C(0x1111111122222222);
	const uint64_t valid_target = UINT64_C(0x3333333344444444);
	int failed_send_rc = -1;
	bool fault_ready = false;
	bool checkpoint_restored = false;
	const PolarDB_WireCapture recovered = capture_libpq_wire(
		[&](PGconn* conn) {
			// The child starts with xmalloc:false, so this deliberate realloc OOM
			// follows libpq's recoverable error path instead of aborting ProxySQL.
			std::string large_query(64U * 1024U * 1024U, 'x');
			struct rlimit saved_limit;
			const rlim_t virtual_bytes = polardb_current_virtual_bytes();
			if (virtual_bytes == 0 ||
					getrlimit(RLIMIT_AS, &saved_limit) != 0) {
				return 0;
			}
			struct rlimit fault_limit = saved_limit;
			const rlim_t headroom = 8U * 1024U * 1024U;
			if (virtual_bytes > RLIM_INFINITY - headroom) {
				return 0;
			}
			fault_limit.rlim_cur = virtual_bytes + headroom;
			if (saved_limit.rlim_max != RLIM_INFINITY &&
					fault_limit.rlim_cur > saved_limit.rlim_max) {
				return 0;
			}
			if (setrlimit(RLIMIT_AS, &fault_limit) != 0) {
				return 0;
			}
			fault_ready = true;
			const int saved_count = conn->outCount;
			const int saved_msg_start = conn->outMsgStart;
			const int saved_msg_end = conn->outMsgEnd;
			failed_send_rc = PQsendPreparePolarWait(
				conn, "large_stmt", large_query.c_str(), 0, nullptr,
				failed_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
			const int restore_rc = setrlimit(RLIMIT_AS, &saved_limit);
			checkpoint_restored = restore_rc == 0 &&
				conn->outCount == saved_count &&
				conn->outMsgStart == saved_msg_start &&
				conn->outMsgEnd == saved_msg_end;
			if (failed_send_rc != 0 || restore_rc != 0) {
				return 0;
			}
			return PQsendPreparePolarWait(
				conn, "stmt", "SELECT 1", 0, nullptr,
				valid_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
		});

	int failures = POLARDB_PREPARE_OOM_CHILD_OK;
	if (!recovered.fixture_ready || !fault_ready || failed_send_rc != 0 ||
			!checkpoint_restored || recovered.send_rc != 1 ||
			recovered.flush_rc != 0) {
		failures |= POLARDB_PREPARE_OOM_CHILD_CHECKPOINT_FAILED;
	}
	if (!polardb_wait_frame_matches(
			recovered.bytes, valid_target, 1000,
			PQ_POLAR_CONSISTENCY_STRICT) ||
			polardb_wire_message_types(recovered.bytes) != "WPS") {
		failures |= POLARDB_PREPARE_OOM_CHILD_CLEAN_RESEND_FAILED;
	}
	return failures;
}

static void test_extended_wait_libpq_wire_order() {
	const uint64_t target_lsn = UINT64_C(0x0102030405060708);
	const uint32_t timeout_ms = 0x00010203;
	const int mode = PQ_POLAR_CONSISTENCY_STRICT;

	const PolarDB_WireCapture prepare = capture_libpq_wire(
		[&](PGconn* conn) {
			return PQsendPreparePolarWait(
				conn, "stmt", "SELECT $1", 0, nullptr,
				target_lsn, timeout_ms, mode);
		});
	ok(prepare.fixture_ready && prepare.send_rc == 1 &&
			prepare.flush_rc == 0,
		"v15_wait libpq wire: prepare fixture sends successfully");
	ok(polardb_wait_wire_matches(
			"prepare", prepare, "WPS", target_lsn, timeout_ms, mode),
		"v15_wait libpq wire: W precedes Parse and carries exact fields");
	ok(prepare.trace.find("PolarWait") != std::string::npos &&
			prepare.trace.find("CopyBothResponse") == std::string::npos,
		"v15_wait libpq trace: frontend W is decoded as PolarWait");

	const PolarDB_WireCapture execute = capture_libpq_wire(
		[&](PGconn* conn) {
			return PQsendQueryPreparedPolarWait(
				conn, "stmt", 0, nullptr, nullptr, nullptr, 0,
				target_lsn, timeout_ms, mode);
		});
	ok(execute.fixture_ready && execute.send_rc == 1 &&
			execute.flush_rc == 0,
		"v15_wait libpq wire: prepared Execute fixture sends successfully");
	ok(polardb_wait_wire_matches(
			"execute", execute, "WBDES", target_lsn, timeout_ms, mode),
		"v15_wait libpq wire: W precedes Bind/Describe/Execute in one flush");

	const PolarDB_WireCapture params = capture_libpq_wire(
		[&](PGconn* conn) {
			return PQsendQueryParamsPolarWait(
				conn, "SELECT 1", 0, nullptr, nullptr, nullptr, nullptr, 0,
				target_lsn, timeout_ms, mode);
		});
	ok(params.fixture_ready && params.send_rc == 1 &&
			params.flush_rc == 0,
		"v15_wait libpq wire: parameterized query fixture sends successfully");
	ok(polardb_wait_wire_matches(
			"params", params, "WPBDES", target_lsn, timeout_ms, mode),
		"v15_wait libpq wire: W precedes Parse for a one-shot extended query");

	const PolarDB_WireCapture max_timeout = capture_libpq_wire(
		[&](PGconn* conn) {
			return PQsendQueryPreparedPolarWait(
				conn, "stmt", 0, nullptr, nullptr, nullptr, 0,
				target_lsn, UINT32_MAX, mode);
		});
	ok(max_timeout.fixture_ready && max_timeout.send_rc == 1 &&
			max_timeout.flush_rc == 0,
		"v15_wait libpq wire: full uint32 timeout sends successfully");
	ok(polardb_wait_wire_matches(
			"max_timeout", max_timeout, "WBDES", target_lsn,
			UINT32_MAX, mode),
		"v15_wait libpq wire: full uint32 timeout preserves all wire bits");
}

static void test_extended_wait_libpq_send_rollback() {
	const uint64_t failed_target = UINT64_C(0x1111111122222222);
	const uint64_t valid_target = UINT64_C(0x3333333344444444);
	const char* values[] = {"value"};
	const int binary_formats[] = {1};
	const int binary_lengths[] = {5};
	int failed_send_rc = -1;
	int invalid_target_rc = -1;
	int invalid_mode_rc = -1;

	const PolarDB_WireCapture rejected_spec = capture_libpq_wire(
		[&](PGconn* conn) {
			invalid_target_rc = PQsendQueryPreparedPolarWait(
				conn, "stmt", 0, nullptr, nullptr, nullptr, 0,
				0, 1000, PQ_POLAR_CONSISTENCY_STRICT);
			invalid_mode_rc = PQsendQueryPreparedPolarWait(
				conn, "stmt", 0, nullptr, nullptr, nullptr, 0,
				failed_target, 1000, 99);
			if (invalid_target_rc != 0 || invalid_mode_rc != 0) {
				return 0;
			}
			return PQsendQueryPreparedPolarWait(
				conn, "stmt", 0, nullptr, nullptr, nullptr, 0,
				valid_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
		});
	ok(rejected_spec.fixture_ready && invalid_target_rc == 0 &&
			invalid_mode_rc == 0 && rejected_spec.send_rc == 1 &&
			rejected_spec.flush_rc == 0,
		"v15_wait libpq validation: malformed wait specs leave the connection reusable");
	ok(polardb_wait_wire_matches(
			"rejected-spec", rejected_spec, "WBDES", valid_target, 1000,
			PQ_POLAR_CONSISTENCY_STRICT) &&
			rejected_spec.trace.find("PolarWait") != std::string::npos &&
			rejected_spec.trace.find("PolarWait",
				rejected_spec.trace.find("PolarWait") + 1) == std::string::npos,
		"v15_wait libpq validation: rejected specs stage no W before a valid send");

	const PolarDB_WireCapture recovered = capture_libpq_wire(
		[&](PGconn* conn) {
			failed_send_rc = PQsendQueryPreparedPolarWait(
				conn, "stmt", 1, values, nullptr, binary_formats, 0,
				failed_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
			if (failed_send_rc != 0) {
				return 0;
			}
			return PQsendQueryPreparedPolarWait(
				conn, "stmt", 1, values, binary_lengths, binary_formats, 0,
				valid_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
		});
	ok(recovered.fixture_ready && failed_send_rc == 0 &&
			recovered.send_rc == 1 && recovered.flush_rc == 0,
		"v15_wait libpq rollback: failed Bind leaves connection reusable");
	ok(polardb_wait_wire_matches(
			"rollback", recovered, "WBDES", valid_target, 1000,
			PQ_POLAR_CONSISTENCY_STRICT),
		"v15_wait libpq rollback: next send contains only its own W and Execute");
	const size_t trace_wait = recovered.trace.find("PolarWait");
	ok(trace_wait != std::string::npos &&
			recovered.trace.find("PolarWait", trace_wait + 1) == std::string::npos &&
			recovered.trace.find(std::to_string(failed_target)) == std::string::npos &&
			recovered.trace.find(std::to_string(valid_target)) != std::string::npos,
		"v15_wait libpq rollback: trace contains only the committed wait");

	constexpr int prefix_size = 8192 - 21;
	ssize_t premature_bytes = -1;
	bool checkpoint_restored = false;
	failed_send_rc = -1;
	const PolarDB_WireCapture threshold = capture_libpq_wire(
		[&](PGconn* conn, int peer_fd) {
			if (pqCheckOutBufferSpace(prefix_size, conn) != 0) {
				return 0;
			}
			memset(conn->outBuffer, 'x', prefix_size);
			conn->outCount = prefix_size;
			const int saved_msg_start = conn->outMsgStart;
			const int saved_msg_end = conn->outMsgEnd;

			failed_send_rc = PQsendQueryPreparedPolarWait(
				conn, "stmt", 1, values, nullptr, binary_formats, 0,
				failed_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
			unsigned char probe[64];
			premature_bytes = recv(
				peer_fd, probe, sizeof(probe), MSG_DONTWAIT);
			if (premature_bytes < 0 &&
					(errno == EAGAIN || errno == EWOULDBLOCK)) {
				premature_bytes = 0;
			}
			checkpoint_restored = conn->outCount == prefix_size &&
				conn->outMsgStart == saved_msg_start &&
				conn->outMsgEnd == saved_msg_end;

			// The synthetic prefix exists only to exercise the threshold. Remove it
			// before proving that the same connection accepts a clean valid send.
			conn->outCount = 0;
			conn->outMsgStart = 0;
			conn->outMsgEnd = 0;
			if (failed_send_rc != 0) {
				return 0;
			}
			return PQsendQueryPreparedPolarWait(
				conn, "stmt", 1, values, binary_lengths, binary_formats, 0,
				valid_target, 1000, PQ_POLAR_CONSISTENCY_STRICT);
		});
	ok(threshold.fixture_ready && failed_send_rc == 0 &&
			premature_bytes == 0 && checkpoint_restored,
		"v15_wait libpq rollback: failed compound send cannot auto-flush W at 8K");
	ok(threshold.send_rc == 1 && threshold.flush_rc == 0 &&
		polardb_wait_wire_matches(
			"rollback-threshold", threshold, "WBDES", valid_target, 1000,
			PQ_POLAR_CONSISTENCY_STRICT),
		"v15_wait libpq rollback: threshold failure is followed by a clean valid send");

	const PolarDB_PrepareOomChildResult prepare_oom =
		run_prepare_oom_child_process();
	ok(prepare_oom.launched && prepare_oom.checkpoint_restored,
		"v15_wait libpq rollback: failed Parse restores the staged W checkpoint");
	ok(prepare_oom.launched && prepare_oom.clean_resend,
		"v15_wait libpq rollback: Prepare failure is followed by a clean W and Parse");
}

static void test_extended_wait_notice_owner_contract() {
	PgSQL_Connection::PolarDB_Query_WrapState state;

	state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	ok(state.wait_notice_uses_session_queue(),
		"v15_wait notice owner: implicit backend Parse keeps W notices in session queue");
	state.observe_extended_wait_completion(true);
	ok(!state.wait_notice_uses_session_queue(),
		"v15_wait notice owner: owner returns to query result after Parse result arrives");
	ok(state.wrapper_set_succeeded(),
		"v15_wait result: semantic Parse success confirms the preceding W");

	state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	state.observe_extended_wait_completion(false);
	ok(!state.wrapper_set_succeeded(),
		"v15_wait result: ErrorResponse followed by pipeline sync cannot confirm W");
	ok(!state.wait_notice_uses_session_queue(),
		"v15_wait notice owner: semantic error ends temporary Parse-result ownership");

	state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	ok(!state.wrapper_set_succeeded() && state.wait_notice_uses_session_queue(),
		"v15_wait result: no semantic completion changes neither success nor notice ownership");

	state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT);
	ok(!state.wait_notice_uses_session_queue(),
		"v15_wait notice owner: prepared Execute keeps W notices in client result");

	state.begin(1, PolarDB_Query_WrapperKind::CONSISTENCY_WAIT);
	ok(state.wait_notice_uses_session_queue(),
		"PolarDB notice owner: discarded SQL-wrapper results still use session queue");
	state.clear();
	ok(!state.wait_notice_uses_session_queue(),
		"PolarDB notice owner: clear restores normal query-result ownership");
}

static void test_extended_wait_cleanup_boundaries() {
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PgSQL_Data_Stream backend_myds;
	backend_myds.init(MYDS_BACKEND, &sess, 0);
	PgSQL_Connection backend_conn(false);
	backend_myds.attach_connection(&backend_conn);

	backend_conn.polardb_query_wrap_state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	PolarDB_SessionUnitAccess::set_extended_request_boundary(
		&sess, EXTQ_PHASE_PROCESSING_PARSE, true);
	PolarDB_SessionUnitAccess::clear_request_state_for_query_end(
		&sess, &backend_myds, false);
	ok(backend_conn.polardb_query_wrap_state.is_extended_wait(),
		"v15_wait cleanup: successful intermediate Parse retains state for Execute");

	PolarDB_SessionUnitAccess::set_extended_request_boundary(
		&sess, EXTQ_PHASE_IDLE, false);
	PolarDB_SessionUnitAccess::clear_request_state_for_query_end(
		&sess, &backend_myds, false);
	ok(!backend_conn.polardb_query_wrap_state.is_extended_wait(),
		"v15_wait cleanup: final Execute boundary clears connection state");

	backend_conn.polardb_query_wrap_state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	PolarDB_SessionUnitAccess::set_extended_request_boundary(
		&sess, EXTQ_PHASE_PROCESSING_PARSE, true);
	ok(!PolarDB_SessionUnitAccess::extended_request_continues(
			&sess, false, true),
		"v15_wait cleanup: Parse ErrorResponse is final despite pending messages");
	PolarDB_SessionUnitAccess::clear_request_state_for_query_end(
		&sess, &backend_myds, true);
	ok(!backend_conn.polardb_query_wrap_state.is_extended_wait(),
		"v15_wait cleanup: failure boundary clears connection state");

	PolarDB_SessionUnitAccess::set_extended_request_boundary(
		&sess, EXTQ_PHASE_IDLE, false);
	backend_myds.detach_connection();
}

static void test_query_cancellation_is_not_retried() {
	PgSQL_Session sess;
	ok(!PolarDB_SessionUnitAccess::request_has_wait_timeout_evidence(false),
		"PolarDB reader failure: marker-less 57014 ignores legacy timeout text");
	ok(PolarDB_SessionUnitAccess::request_has_wait_timeout_evidence(true),
		"PolarDB reader failure: structured timeout marker owns 57014 classification");
	ok(!PolarDB_SessionUnitAccess::request_has_wait_timeout_evidence(false),
		"PolarDB reader failure: unstructured timeout text cannot authorize fallback");

	const auto canceled = PolarDB_SessionUnitAccess::reader_failure_decision(
		&sess, /*timeout=*/false, /*reusable=*/true,
		PGSQL_ERROR_CODES::ERRCODE_QUERY_CANCELED,
		PolarDB_LsnWaitTimeoutAction::PRIMARY,
		PolarDB_ReplicaErrorAction::PRIMARY);
	ok(canceled.kind == PolarDB_ReaderFailureKind::QUERY_CANCELED &&
			canceled.action == PolarDB_ReaderAction::RETURN_ERROR,
		"PolarDB reader failure: non-marker 57014 is returned as cancellation");
	ok(!canceled.allow_writer_retry &&
			canceled.reader_failure_route == PolarDB_ReaderFailureRoute::NONE,
		"PolarDB reader failure: cancellation cannot install a writer retry route");

	const auto wait_timeout =
		PolarDB_SessionUnitAccess::reader_failure_decision(
			&sess, /*timeout=*/true, /*reusable=*/true,
			PGSQL_ERROR_CODES::ERRCODE_QUERY_CANCELED,
			PolarDB_LsnWaitTimeoutAction::PRIMARY,
			PolarDB_ReplicaErrorAction::ERROR);
	ok(wait_timeout.kind == PolarDB_ReaderFailureKind::WAIT_TIMEOUT &&
			wait_timeout.action == PolarDB_ReaderAction::RETRY,
		"PolarDB reader failure: structured 57014 remains a wait timeout");
	ok(wait_timeout.allow_writer_retry &&
			wait_timeout.reader_failure_route ==
				PolarDB_ReaderFailureRoute::FORCE_WRITER,
		"PolarDB reader failure: structured timeout retains configured primary fallback");

	const auto preserved =
		PolarDB_SessionUnitAccess::preserve_captured_wait_failure_evidence(&sess);
	ok(preserved.timeout && preserved.reusable && preserved.connected &&
			preserved.has_backend_error &&
			preserved.backend_error_code ==
				PGSQL_ERROR_CODES::ERRCODE_QUERY_CANCELED,
		"PolarDB reader failure: cleanup enrichment preserves the immutable timeout outcome");

	const auto released_packet =
		PolarDB_SessionUnitAccess::finish_reusable_wait_error(
			&sess, /*query_aliases_packet=*/false);
	ok(released_packet.action == PolarDB_FailureAction::PASSTHROUGH &&
			!released_packet.failure_owns_packet &&
			!released_packet.request_stream_owns_packet &&
			released_packet.retry_counters_unchanged,
		"PolarDB reader failure: reusable cancellation releases a detached packet without retry accounting");

	const auto restored_packet =
		PolarDB_SessionUnitAccess::finish_reusable_wait_error(
			&sess, /*query_aliases_packet=*/true);
	ok(restored_packet.action == PolarDB_FailureAction::PASSTHROUGH &&
			!restored_packet.failure_owns_packet &&
			restored_packet.request_stream_owns_packet &&
			restored_packet.retry_counters_unchanged,
		"PolarDB reader failure: cancellation restores a borrowed packet without retry accounting");
}

static void test_extended_frame_local_error_contract() {
	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	ok(PolarDB_SessionUnitAccess::exercise_worker_polardb_publication(
			worker.get()),
		"PolarDB publication: worker wake refreshes PolarDB state without consuming the generic server-version delta");
	PgSQL_Session qpo_sess;
	attach_test_frontend(qpo_sess, worker.get());
	ok(PolarDB_SessionUnitAccess::execute_qpo_error_is_terminal(&qpo_sess),
		"Extended Execute QPO: a local query-rule error is terminal for the Sync frame");

	PgSQL_Session dispatch_sess;
	attach_test_frontend(dispatch_sess, worker.get());
	ok(PolarDB_SessionUnitAccess::extended_dispatch_error_clears_frame(
			&dispatch_sess),
		"Extended dispatcher: a local Execute error discards later messages through Sync");
	ok(PolarDB_SessionUnitAccess::extended_local_flush_error_waits_for_sync(
			&dispatch_sess),
		"Extended ErrorResponse: local Flush errors retain discard-until-Sync state");

	PolarDB_SessionUnitAccess::set_worker_polardb_active(worker.get(), false);
	ok(PolarDB_SessionUnitAccess::ordinary_sync_frame_uses_lazy_state(
			&dispatch_sess),
		"Extended inactive path: ordinary Sync frames use only O(1) candidate state");
	const auto activated =
		PolarDB_SessionUnitAccess::exercise_extended_frame_activity_transition(
			&dispatch_sess, false, true);
	ok(activated.backend_route_pending && activated.backend_candidates == 1 &&
			activated.writer_required,
		"Extended classifier: inactive-to-active batch transition stays balanced and requires writer");
	const auto deactivated =
		PolarDB_SessionUnitAccess::exercise_extended_frame_activity_transition(
			&dispatch_sess, true, false);
	ok(deactivated.backend_route_pending &&
			deactivated.backend_candidates == 1 &&
			deactivated.writer_required,
		"Extended classifier: active-to-inactive batch transition stays balanced and pinned conservatively");
	PolarDB_SessionUnitAccess::set_worker_polardb_active(worker.get(), true);
	const auto reordered =
		PolarDB_SessionUnitAccess::classify_bind_parse_execute(
			&dispatch_sess, 10);
	ok(reordered.backend_route_pending && reordered.backend_candidates == 2 &&
			reordered.writer_required,
		"Extended classifier: Bind followed by replacement Parse cannot be reader-eligible");

	PgSQL_Session owner_sess;
	const auto owner =
		PolarDB_SessionUnitAccess::exercise_extended_frame_owner(
			&owner_sess, 10, 20, 30);
	ok(owner.selected && owner.carried_across_previous_hostgroup_change &&
			owner.ownership_deferred_until_send,
		"Extended frame owner: selected hostgroup survives until dispatch");
	ok(owner.backend_claimed && owner.backend_switch_rejected &&
			owner.selected_hostgroup == 20 && owner.backend_hostgroup == 20,
		"Extended frame owner: a dispatched Sync frame cannot switch backend hostgroups");
	ok(owner.cleared,
		"Extended frame owner: Sync completion clears route and backend ownership");
	const auto transfer =
		PolarDB_SessionUnitAccess::exercise_extended_frame_transfer(
			&owner_sess, 20, 30);
	ok(transfer.backend_claimed && transfer.backend_sync_reset &&
			transfer.selected_hostgroup == 30 &&
			transfer.backend_hostgroup == 30,
		"Extended frame owner: controlled retry transfers ownership and clears prior backend Sync evidence");
	const auto qpo_hostgroups =
		PolarDB_SessionUnitAccess::exercise_extended_qpo_hostgroups(
			&owner_sess, 10, 20, 30);
	ok(qpo_hostgroups.first_destination_selected &&
			qpo_hostgroups.first_destination_claimed &&
			qpo_hostgroups.later_destination_rejected &&
			qpo_hostgroups.selected_hostgroup == 20 &&
			qpo_hostgroups.backend_hostgroup == 20,
		"Extended QPO routing: the first destination owns the frame and a later conflicting destination is rejected");
	ok(qpo_hostgroups.missing_previous_uses_default,
		"Extended QPO routing: a query-cache hit without previous placement falls back to the configured default hostgroup");
	const auto frame_invariants =
		PolarDB_SessionUnitAccess::exercise_extended_frame_invariants(
			&owner_sess, 20);
	ok(frame_invariants.candidate_underflow_rejected &&
			frame_invariants.queue_shape_failed_closed &&
			frame_invariants.candidate_balance_repaired,
		"Extended frame invariants: accounting drift is checked and ambiguous queue shape fails closed to writer");
	ok(frame_invariants.command_owner_mismatch_rejected &&
			frame_invariants.sync_owner_mismatch_rejected,
		"Extended frame invariants: command and Sync ownership mismatches are rejected without aborting the proxy");
	const auto pinned_conflict =
		PolarDB_SessionUnitAccess::exercise_pinned_frame_conflict(
			&dispatch_sess, 20);
	ok(pinned_conflict.operation_rejected &&
			pinned_conflict.sqlstate_is_p0001 &&
			pinned_conflict.flush_emits_error_only,
		"Extended pinned frame: a conflicting later Flush operation returns one P0001 ErrorResponse and is not dispatched");
	ok(pinned_conflict.waits_for_client_sync &&
			pinned_conflict.sync_emits_only_rfq,
		"Extended pinned frame: recovery discards through client Sync and emits ReadyForQuery only at that boundary");
	ok(PolarDB_SessionUnitAccess::implicit_prepare_retry_lifecycle(&owner_sess),
		"Extended implicit prepare: retry preserves the logical continuation and terminal cleanup consumes it once");
	ok(PolarDB_SessionUnitAccess::activation_transition_is_conservative(
			&dispatch_sess),
		"PolarDB activation: only sessions crossing a topology activation start with unknown write state");
	ok(PolarDB_SessionUnitAccess::local_frame_requires_implicit_sync(
			&owner_sess),
		"Extended local frame: Simple Query requires the central implicit-Sync boundary and completion clears its portal");
	const auto error_packets =
		PolarDB_SessionUnitAccess::exercise_error_packet_ownership(
			&dispatch_sess);
	ok(error_packets.nonfatal_without_ready_is_error_only &&
			error_packets.nonfatal_with_ready_has_one_rfq &&
			error_packets.fatal_without_ready_is_error_only &&
			error_packets.fatal_with_ready_is_error_only,
		"PostgreSQL errors: nonfatal callers own RFQ placement and fatal errors never emit RFQ");
	const auto forwarded_error =
		PolarDB_SessionUnitAccess::exercise_forwarded_error_ownership(
			&dispatch_sess, 20);
	ok(forwarded_error.flush_error_is_error_only &&
			forwarded_error.queued_messages_discarded &&
			forwarded_error.waits_for_client_sync &&
			forwarded_error.sync_adds_one_rfq,
		"PolarDB forwarded Flush error: queued operations are discarded and client Sync owns the only RFQ");
	ok(forwarded_error.transaction_rfq_preserved &&
			forwarded_error.frame_completed,
		"PolarDB forwarded Sync error: the exact transaction byte is preserved and the frame completes once");
	ok(PolarDB_SessionUnitAccess::automatic_reader_route_preserves_writer_lock(
			&dispatch_sess, 10, 20),
		"Extended hostgroup lock: automatic reader placement cannot replace the QPO-selected writer lock");
	const auto portal_identity =
		PolarDB_SessionUnitAccess::exercise_bound_portal_identity(&owner_sess);
	ok(portal_identity.bound_statement_id !=
			portal_identity.replacement_statement_id &&
			portal_identity.execute_statement_id ==
				portal_identity.bound_statement_id &&
			portal_identity.bind_did_not_copy_shared_owner &&
			portal_identity.replacement_transferred_existing_owner &&
			portal_identity.close_transferred_existing_owner,
		"Extended Bind ownership: the hot path avoids shared-owner RMWs and replacement/Close preserve portal identity");
	const auto poisoned =
		PolarDB_SessionUnitAccess::exercise_poisoned_extended_frame(
			&dispatch_sess, 20);
	ok(poisoned.flush_emits_error_without_ready &&
			poisoned.flush_waits_for_client_sync &&
			poisoned.sync_emits_ready_in_error &&
			poisoned.sync_completes_frame &&
			poisoned.consumed_sync_emits_one_ready_in_error,
		"Extended backend death: transaction poison preserves Sync-owned Z(E) and completes once");
	const auto sync =
		PolarDB_SessionUnitAccess::exercise_extended_frame_sync(
			&owner_sess, 20);
	ok(sync.ready_before_backend && !sync.ready_after_flush &&
			sync.ready_after_sync && sync.rfq_pending_after_flush &&
			!sync.rfq_pending_after_sync,
		"Extended frame owner: ReadyForQuery waits for an actual backend Sync");
	const auto deferred_rfq =
		PolarDB_SessionUnitAccess::exercise_deferred_extended_rfq(
			&dispatch_sess, 10, 7);
	ok(deferred_rfq.pending_after_first_execute &&
			deferred_rfq.publication_pending_after_first_execute &&
			deferred_rfq.write_attribution_preserved &&
			deferred_rfq.writer_scope_preserved &&
			deferred_rfq.wait_target_preserved &&
			deferred_rfq.staged_reset_preserved_frame_state &&
			deferred_rfq.direct_publication_cleared_after_result &&
			deferred_rfq.unknown_after_abandon &&
			deferred_rfq.state_reset_after_abandon &&
			deferred_rfq.inactive_reset_abandoned_orphan &&
				deferred_rfq.error_publication_policy_preserved &&
				deferred_rfq.error_publication_cleared_after_emit &&
				deferred_rfq.error_publication_abandoned_on_failure &&
				deferred_rfq.error_sync_publication_preserved &&
				deferred_rfq.error_sync_result_attribution_discarded,
		"Extended Flush RFQ: result attribution and frontend publication retain their independent policy lifetimes");
	PgSQL_Session multiplex_sess;
	attach_test_frontend(multiplex_sess, worker.get());
	ok(PolarDB_SessionUnitAccess::sticky_frame_outranks_delayed_multiplex(
			&multiplex_sess),
		"Extended frame owner: delayed multiplexing cannot release a sticky Execute+Flush backend");
	const auto batches =
		PolarDB_SessionUnitAccess::exercise_extended_frame_batch_pinning(
			&owner_sess, 20);
	ok(batches.reroute_required_after_backend_claim &&
			batches.backend_stays_pinned &&
			batches.backend_switch_rejected,
		"Extended frame owner: later Flush batches reroute but cannot switch an open backend pipeline");
	ok(batches.local_batch_can_be_reclassified,
		"Extended frame owner: a local-only Flush batch does not pin later backend work");
	PgSQL_Session error_sess;
	attach_test_frontend(error_sess, worker.get());
	const auto error =
		PolarDB_SessionUnitAccess::exercise_extended_frame_error_recovery(
			&error_sess);
	ok(error.waits_for_client_sync && error.discards_parse &&
			error.discards_flush && error.accepts_sync &&
			error.accepts_terminate,
		"Extended frame error: messages are discarded until Sync while Terminate remains valid");
	ok(!error.ready_before_sync && error.ready_after_sync &&
			error.state_cleared,
		"Extended frame error: ReadyForQuery is emitted only at Sync and recovery clears frame state");
	const auto ready =
		PolarDB_SessionUnitAccess::exercise_extended_ready_ownership(
			&error_sess, 20);
	ok(ready.backend_error_deferred && ready.backend_ready_once &&
			ready.backend_ready_not_duplicated,
		"Extended backend error: RFQ waits for resync and is emitted exactly once");
	ok(ready.control_resync_deferred && ready.control_resync_ready_once,
		"Extended control resync: discarded backend result leaves one RFQ for the session boundary");
	ok(ready.local_ready_once && ready.local_ready_not_duplicated,
		"Extended local error: lazy Sync frame emits exactly one RFQ");
	ok(ready.idle_local_ready_each_time &&
			ready.idle_local_does_not_claim_frame,
		"Idle local result: each request emits RFQ without latching extended ownership");
	ok(ready.describe_does_not_stage_wait,
		"Extended Describe: backend ownership is tracked without staging W");
}

static void test_wait_timeout_provenance() {
	PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_FATAL_ERROR);
	ok(result != nullptr, "PolarDB timeout provenance: result fixture created");
	if (!result) {
		return;
	}
	pqSaveMessageField(result, PG_DIAG_MESSAGE_DETAIL,
		POLARDB_LSN_WAIT_TIMEOUT_DETAIL);
	ok(!polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout provenance: DETAIL alone is not trusted");
	pqSaveMessageField(result, PG_DIAG_SOURCE_FUNCTION,
		"client_controlled_function");
	ok(!polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout provenance: forged source function is rejected");
	pqSaveMessageField(result, PG_DIAG_SOURCE_FUNCTION,
		POLARDB_LSN_WAIT_TIMEOUT_SOURCE_FUNCTION);
	ok(polardb_is_lsn_wait_timeout_result(result),
		"PolarDB timeout provenance: exact DETAIL and source function are accepted");
	PQclear(result);
}

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

static void test_extended_wait_binds_before_first_backend_send() {
	const int writer_hg = 952;
	const int reader_hg = 953;
	const uint64_t target_lsn = 0xB090;
	stage_polardb_topology(PgHGM, "PolarDB extended pre-send wait",
		writer_hg, "polardb-pre-send-writer", 19434,
		reader_hg, "polardb-pre-send-reader", 19435);

	PgSQL_HGC* reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC* reader = find_pgsql_server(
		reader_hgc, "polardb-pre-send-reader", 19435);
	ok(reader != nullptr,
		"PolarDB extended pre-send: concrete reader is available");
	if (!reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	PgSQL_Connection conn(false);
	conn.parent = reader;
	const PolarDB_WaitSpec wait = PolarDB_WaitSpec::from_lsn(
		target_lsn, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	auto prepare_frame = [&]() {
		sess.current_hostgroup = reader_hg;
		sess.previous_hostgroup = writer_hg;
		sess.polardb_query.reader_plan.fallback_writer_hg = writer_hg;
		sess.polardb_query.reader_wait_spec = wait;
	};

	reader->polardb_current_lsn.store(target_lsn, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	prepare_frame();
	const PolarDB_ThreadCounterSnapshot bypassed(
		worker.get(), polardb_st_var_wait_wrap_bypassed);
	const bool ready_first = sess.polardb_prepare_extended_wait(&conn);
	const bool ready_second = sess.polardb_prepare_extended_wait(&conn);
	ok(ready_first && ready_second && !sess.polardb_wait_active() &&
			sess.polardb_query.wait_bypass_target == target_lsn &&
			bypassed.delta() == 1,
		"PolarDB extended pre-send: attached target-ready reader is sampled once");

	sess.polardb_query.clear_reader_route();
	reader->polardb_current_lsn.store(target_lsn - 1,
		std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	prepare_frame();
	const PolarDB_ThreadCounterSnapshot prepared(
		worker.get(), polardb_st_var_wait_wrap_prepared);
	const bool behind_first = sess.polardb_prepare_extended_wait(&conn);
	const bool behind_second = sess.polardb_prepare_extended_wait(&conn);
	ok(behind_first && behind_second && sess.polardb_wait_active() &&
			sess.polardb_query.wait.spec.target == target_lsn &&
			prepared.delta() == 1,
		"PolarDB extended pre-send: newly selected behind reader activates one W wait");

	// A failed request may return this backend before a semantic result clears
	// its connection-local W marker. The pool boundary must remove that marker
	// so a later session cannot mistake the old W for its own prepared wait.
	conn.dispatch_state.wrapper_stmts = 1;
	conn.dispatch_state.wrapper_kind = PolarDB_Query_WrapperKind::EXTENDED_WAIT;
	conn.polardb_query_wrap_state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::SESSION_QUEUE);
	conn.polardb_clear_request_state_for_release();
	ok(conn.dispatch_state.wrapper_stmts == 0 &&
			conn.dispatch_state.wrapper_kind == PolarDB_Query_WrapperKind::NONE &&
			!conn.polardb_query_wrap_state.was_wrapped &&
			!conn.polardb_query_wrap_state.is_extended_wait(),
		"PolarDB extended pool release: request-owned W and dispatch state are cleared");

	PgSQL_Session next_sess;
	attach_test_frontend(next_sess, worker.get());
	next_sess.current_hostgroup = reader_hg;
	next_sess.previous_hostgroup = writer_hg;
	next_sess.polardb_query.reader_plan.fallback_writer_hg = writer_hg;
	next_sess.polardb_query.reader_wait_spec = wait;
	const PolarDB_ThreadCounterSnapshot next_prepared(
		worker.get(), polardb_st_var_wait_wrap_prepared);
	const bool next_owner_prepared =
		next_sess.polardb_prepare_extended_wait(&conn);
	ok(next_owner_prepared && next_sess.polardb_wait_active() &&
			next_sess.polardb_query.wait.spec.target == target_lsn &&
			next_prepared.delta() == 1,
		"PolarDB extended pool release: next owner prepares a fresh W on the reused backend");
	sess.polardb_query.clear_reader_route();
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

	backend_conn->polardb_query_wrap_state.begin_extended_wait(
		PolarDB_ExtendedWaitNoticeOwner::QUERY_RESULT);
	backend_conn->polardb_query_wrap_state.observe_extended_wait_completion(
		false);
	sess.polardb_query.wait.wait_started_at_us = monotonic_time();
	sess.polardb_finish_wait(&backend_myds);
	ok(reader->polardb_current_lsn.load(std::memory_order_relaxed) == 0,
		"PolarDB wait cache advance: failed W plus pipeline sync leaves reader LSN unchanged");

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

	PtrSize_t txn_packet = unit_simple_query_packet("SELECT txn");
	txn_packet.flags = 2;
	txn_packet.owner = &txn_packet;
	sess.polardb_txn_reader.take_original_packet(txn_packet);
	ok(!txn_packet.ptr && txn_packet.size == 0 && txn_packet.flags == 0 &&
			!txn_packet.owner &&
			sess.polardb_txn_reader.original_pkt.flags == 2 &&
			sess.polardb_txn_reader.original_pkt.owner == &txn_packet,
		"transaction reader: taking a packet moves the complete descriptor");
	PtrSize_t released_txn_packet =
		sess.polardb_txn_reader.release_original_packet();
	ok(released_txn_packet.ptr && released_txn_packet.flags == 2 &&
			released_txn_packet.owner == &txn_packet &&
			!sess.polardb_txn_reader.original_pkt.ptr &&
			sess.polardb_txn_reader.original_pkt.size == 0 &&
			sess.polardb_txn_reader.original_pkt.flags == 0 &&
			!sess.polardb_txn_reader.original_pkt.owner,
		"transaction reader: releasing a packet clears its stored descriptor");
	l_free(released_txn_packet.size, released_txn_packet.ptr);

	const char* original_sql = "SELECT 42";
	PtrSize_t packet = unit_simple_query_packet(original_sql);
	packet.flags = 2;
	packet.owner = &packet;
	sess.CurrentQuery.begin(
		static_cast<unsigned char*>(packet.ptr), packet.size, true);

	PgSQL_Data_Stream backend_myds;
	ok(!backend_myds.pgsql_real_query.pkt.ptr &&
			backend_myds.pgsql_real_query.pkt.size == 0 &&
			backend_myds.pgsql_real_query.pkt.flags == 0 &&
			!backend_myds.pgsql_real_query.pkt.owner &&
			!backend_myds.pgsql_real_query.QueryPtr &&
			backend_myds.pgsql_real_query.QuerySize == 0,
		"real query: construction initializes the complete owned descriptor");
	backend_myds.pgsql_real_query.take_packet(packet);
	ok(!packet.ptr && packet.size == 0 && packet.flags == 0 && !packet.owner,
		"real query: taking a packet clears the complete source descriptor");
	ok(backend_myds.pgsql_real_query.pkt.flags == 2 &&
			backend_myds.pgsql_real_query.pkt.owner == &packet,
		"real query: taking a packet preserves its packet metadata");
	PtrSize_t released = backend_myds.pgsql_real_query.release_packet();
	ok(released.ptr && !backend_myds.pgsql_real_query.pkt.ptr &&
			!backend_myds.pgsql_real_query.QueryPtr,
		"real query: releasing a packet clears the stream descriptor");
	backend_myds.pgsql_real_query.take_packet(released);
	ok(!released.ptr && released.size == 0 && released.flags == 0 &&
			!released.owner,
		"real query: taking a released packet clears its temporary owner");
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
	ok(backend_myds.pgsql_real_query.pkt.flags == 0 &&
			!backend_myds.pgsql_real_query.pkt.owner,
		"PolarDB wait wrapper: replacement owns fresh packet metadata");
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

	PGresult* result = PQmakeEmptyPGresult(nullptr, PGRES_NONFATAL_ERROR);
	pqSaveMessageField(result, PG_DIAG_SEVERITY, "WARNING");
	pqSaveMessageField(result, PG_DIAG_SQLSTATE, "01000");
	pqSaveMessageField(result, PG_DIAG_MESSAGE_PRIMARY, "wait warning");
	pqSaveMessageField(result, PG_DIAG_MESSAGE_HINT, "retry later");
	pqSaveMessageField(result, PG_DIAG_CONTEXT, "while waiting for replay");
	pqSaveMessageField(result, PG_DIAG_SCHEMA_NAME, "public");
	const unsigned int notice_size = sess.polardb_enqueue_notice_packet(result);
	const PtrSize_t queued = notices.pending && notices.pending->len == 1
		? notices.pending->pdata[0] : PtrSize_t{0, nullptr};
	ok(notice_size == queued.size && notice_size != 0,
		"PolarDB notice queue: backend result serializes into one packet");
	ok(notice_packet_has_field(
			(const unsigned char*)queued.ptr, queued.size, 'H', "retry later") &&
			notice_packet_has_field(
				(const unsigned char*)queued.ptr, queued.size, 'W',
				"while waiting for replay") &&
			notice_packet_has_field(
				(const unsigned char*)queued.ptr, queued.size, 's', "public"),
		"PolarDB notice queue: queued warning preserves optional backend fields");
	PQclear(result);
	sess.discard_pending_notices();
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
	PolarDB_SessionUnitAccess::set_worker_polardb_active(worker.get(), true);
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

	no_reader_ctx.read_target =
		static_cast<int>(PolarDB_ReadTarget::PRIMARY);
	no_reader_plan = sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			no_reader_plan.target_hg == first.writer_scope.hg,
		"PolarDB placement: primary read target remains effective when consistency is off");

	no_reader_ctx.read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	no_reader_ctx.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::PRIMARY_ONLY);
	no_reader_plan = sess.polardb_plan(no_reader_ctx);
	ok(no_reader_plan.action ==
			PolarDB_Query_RoutePlan::RouteAction::FORCE_PRIMARY &&
			no_reader_plan.target_hg == first.writer_scope.hg,
		"PolarDB placement: legacy per-hostgroup primary mode remains on the writer");

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
		"PolarDB collect snapshot: collect leaves session state unchanged");

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
	PolarDB_SessionUnitAccess::set_extended_route_state(
		&sess, 1, false, EXTQ_PHASE_PROCESSING_EXECUTE);
	PtrSize_t extended_pkt{0, nullptr};
	const bool extended_handled = sess.polardb_route_query(extended_pkt);
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

static void test_extended_wait_routes_only_execute_to_reader() {
	const int writer_hg = 982;
	const int reader_hg = 983;
	const uint64_t target_lsn = 0x4510;

	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg, "polardb-v15-wait-writer", 26432,
			reader_hg, "polardb-v15-wait-reader", 26433)) == 0,
		"PolarDB v15_wait route: writer and reader staged for commit");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row_with_protocol(
			writer_hg, reader_hg, "v15_wait"),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB v15_wait route: topology commit succeeds");

	const auto writer_cfg = PgHGM->get_polardb_hg_config(writer_hg);
	ok(writer_cfg.is_polardb_hostgroup &&
			writer_cfg.writer_hostgroup == writer_hg &&
			writer_cfg.reader_hostgroup == reader_hg,
		"PolarDB v15_wait route: writer scope and reader hostgroup are available");
	if (!writer_cfg.is_polardb_hostgroup ||
			writer_cfg.writer_hostgroup != writer_hg ||
			writer_cfg.reader_hostgroup != reader_hg) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PolarDB_SessionUnitAccess::set_worker_polardb_active(worker.get(), true);
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	sess.polardb_config.session_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::SESSION_LSN);
	sess.polardb_session_consistency.writer_scope = PolarDB_WriterScope{
		writer_cfg.writer_hostgroup, writer_cfg.writer_epoch};
	sess.polardb_session_consistency.write_lsn = target_lsn;
	const char select_query[] = "SELECT $1";
	sess.CurrentQuery.begin(
		(unsigned char*)const_cast<char*>(select_query),
		strlen(select_query) + 1, false);
	sess.CurrentQuery.PgQueryCmd = PGSQL_QUERY_SELECT;

	const int saved_read_target = pgsql_thread___polardb_read_target;
	const bool saved_profile_off = pgsql_thread___polardb_profile_off;
	pgsql_thread___polardb_read_target =
		static_cast<int>(PolarDB_ReadTarget::REPLICA);
	pgsql_thread___polardb_profile_off = false;

	auto prepare_route = [&](int hostgroup, uint8_t phase) {
		sess.polardb_query.reset_for_new_query();
		sess.current_hostgroup = hostgroup;
		PolarDB_SessionUnitAccess::set_extended_route_state(
			&sess, 1, false, phase);
	};
	auto route_extended = [&](bool writer_required) {
		PtrSize_t pkt{0, nullptr};
		return sess.polardb_route_query(pkt, writer_required);
	};

	prepare_route(writer_hg, EXTQ_PHASE_PROCESSING_EXECUTE);
	const bool activation_error = route_extended(false);
	ok(!activation_error && sess.polardb_config.is_polardb_enabled &&
			!sess.polardb_session_consistency.write_unknown &&
			sess.current_hostgroup == reader_hg &&
			sess.polardb_query.reader_wait_spec.has_wait() &&
			sess.polardb_query.reader_wait_spec.target == target_lsn,
		"PolarDB activation: a session born under the active topology routes its first request normally");

	const auto one_shot =
		PolarDB_SessionUnitAccess::classify_extended_frame(
			&sess, writer_hg, 1, 1, false);
	ok(one_shot.backend_route_pending &&
			one_shot.backend_candidates == 2 &&
			!one_shot.writer_required,
		"PolarDB extended frame: one-shot Parse/Bind/Execute routes at its first actual backend operation");
	prepare_route(writer_hg, EXTQ_PHASE_PROCESSING_PARSE);
	const bool one_shot_error = route_extended(one_shot.writer_required);
	ok(!one_shot_error && sess.current_hostgroup == reader_hg &&
			sess.polardb_query.reader_wait_spec.has_wait() &&
			sess.polardb_query.reader_wait_spec.target == target_lsn &&
			sess.polardb_query.request_writer_scope.matches(
				PolarDB_WriterScope{writer_hg, writer_cfg.writer_epoch}),
		"PolarDB extended route: one-shot Parse selects the reader and W target");

	const auto reusable =
		PolarDB_SessionUnitAccess::classify_extended_frame(
			&sess, writer_hg, 0, 1, false, true);
	ok(reusable.backend_route_pending &&
			reusable.backend_candidates == 1 &&
			!reusable.writer_required,
		"PolarDB extended frame: reusable Bind/Execute routes at Execute");
	prepare_route(writer_hg, EXTQ_PHASE_PROCESSING_EXECUTE);
	const bool reusable_error = route_extended(reusable.writer_required);
	ok(!reusable_error && sess.current_hostgroup == reader_hg &&
			sess.polardb_query.reader_wait_spec.target == target_lsn,
		"PolarDB extended route: reusable Execute uses the shared reader plan");

	const auto metadata =
		PolarDB_SessionUnitAccess::classify_extended_frame(
			&sess, writer_hg, 1, 1, true);
	ok(metadata.backend_route_pending &&
			metadata.backend_candidates == 3 &&
			metadata.writer_required,
		"PolarDB extended frame: backend metadata pins the frame to writer");
	prepare_route(writer_hg, EXTQ_PHASE_PROCESSING_PARSE);
	const bool metadata_error = route_extended(metadata.writer_required);
	ok(!metadata_error && sess.current_hostgroup == writer_hg &&
			!sess.polardb_query.reader_wait_spec.has_wait(),
		"PolarDB extended route: metadata frame cannot open a reader pipeline");

	const auto multi_execute =
		PolarDB_SessionUnitAccess::classify_extended_frame(
			&sess, writer_hg, 0, 2, false);
	ok(multi_execute.backend_route_pending &&
			multi_execute.backend_candidates == 2 &&
			multi_execute.writer_required,
		"PolarDB extended frame: multiple Execute operations require writer");
	prepare_route(writer_hg, EXTQ_PHASE_PROCESSING_EXECUTE);
	const bool multi_error = route_extended(multi_execute.writer_required);
	ok(!multi_error && sess.current_hostgroup == writer_hg &&
			!sess.polardb_query.reader_wait_spec.has_wait(),
		"PolarDB extended route: one Sync frame never splits across readers");

	const auto parse_only =
		PolarDB_SessionUnitAccess::classify_extended_frame(
			&sess, writer_hg, 1, 0, false);
	ok(parse_only.backend_route_pending &&
			parse_only.backend_candidates == 1 &&
			parse_only.writer_required,
		"PolarDB extended frame: Parse-only metadata stays on writer");

	constexpr int non_polardb_hg = 1999;
	sess.polardb_query.reader_wait_spec = PolarDB_WaitSpec::from_lsn(
		target_lsn, POLARDB_DEFAULT_WAIT_TIMEOUT_MS,
		PolarDB_WaitMode::BEST_EFFORT);
	const auto mixed = PolarDB_SessionUnitAccess::classify_extended_frame(
		&sess, non_polardb_hg, 0, 1, false, true);
	prepare_route(non_polardb_hg, EXTQ_PHASE_PROCESSING_EXECUTE);
	const auto mixed_route =
		PolarDB_SessionUnitAccess::apply_extended_backend_route(&sess);
	ok(mixed.backend_route_pending && mixed_route.continued &&
			mixed_route.route_consumed &&
			sess.current_hostgroup == non_polardb_hg &&
			!sess.polardb_query.reader_wait_spec.has_wait(),
		"PolarDB extended route: ordinary hostgroup skips routing and clears the prior W target");
	ok(PolarDB_SessionUnitAccess::locked_non_polardb_route_clears_stale_scope(
			&sess, non_polardb_hg),
		"PolarDB extended route: ordinary locked hostgroup clears stale scope without accounting");
	const auto simple_manual =
		PolarDB_SessionUnitAccess::exercise_manual_non_polardb_route(
			&sess, non_polardb_hg, false);
	ok(simple_manual.route_left_unchanged && simple_manual.scope_reset &&
			simple_manual.reader_plan_reset &&
			simple_manual.txn_reader_reconciled &&
			simple_manual.manual_total_delta == 1 &&
			simple_manual.manual_other_delta == 1,
		"PolarDB simple manual route: ordinary hostgroup resets stale request state and accounts manual-other once");
	const auto extended_manual =
		PolarDB_SessionUnitAccess::exercise_manual_non_polardb_route(
			&sess, non_polardb_hg, true);
	ok(extended_manual.route_left_unchanged && extended_manual.scope_reset &&
			extended_manual.reader_plan_reset &&
			extended_manual.txn_reader_reconciled &&
			extended_manual.manual_total_delta == 1 &&
			extended_manual.manual_other_delta == 1,
		"PolarDB extended manual route: ordinary hostgroup resets stale request state and accounts manual-other once");

	const int v15_writer_hg = 984;
	const int v15_reader_hg = 985;
	ok(PgHGM->servers_add(make_pgsql_servers_result(
			v15_writer_hg, "polardb-v15-writer", 26434,
			v15_reader_hg, "polardb-v15-reader", 26435)) == 0,
		"PolarDB v15 route: writer and reader staged for commit");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row_with_protocol(
			v15_writer_hg, v15_reader_hg, "v15"),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB v15 route: topology commit succeeds");
	const auto v15_writer_cfg = PgHGM->get_polardb_hg_config(v15_writer_hg);
	sess.polardb_session_consistency.writer_scope = PolarDB_WriterScope{
		v15_writer_cfg.writer_hostgroup, v15_writer_cfg.writer_epoch};
	sess.polardb_session_consistency.write_lsn = target_lsn;
	prepare_route(v15_writer_hg, EXTQ_PHASE_PROCESSING_EXECUTE);
	const bool v15_error = route_extended(false);
	ok(!v15_error && sess.current_hostgroup == v15_writer_hg &&
			!sess.polardb_query.reader_wait_spec.has_wait(),
		"PolarDB v15 route: extended wait requirement falls back to writer");

	PolarDB_SessionUnitAccess::reset_extended_frame_state(&sess);
	pgsql_thread___polardb_read_target = saved_read_target;
	pgsql_thread___polardb_profile_off = saved_profile_off;
}

void run_polardb_consistency_counter_tests() {
	test_wait_timeout_fields();
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
	test_extended_wait_binds_before_first_backend_send();
}

void run_polardb_consistency_wait_cache_tests() {
	test_extended_wait_libpq_wire_order();
	test_extended_wait_libpq_send_rollback();
	test_successful_wait_cache_advance_requires_active_wait();
	test_wait_wrapper_failure_preserves_prefix();
	test_wait_wrapper_keeps_original_query_alive();
}

void run_polardb_session_state_tests() {
	test_wait_timeout_provenance();
	test_extended_wait_notice_owner_contract();
	test_extended_wait_cleanup_boundaries();
	test_query_cancellation_is_not_retried();
	test_extended_frame_local_error_contract();
	test_session_route_state_clear_tiers();
	test_notice_queue_state_contract();
	test_user_attributes_are_reapplied_after_reset();
	test_collect_is_const_stable_snapshot();
	test_extended_wait_routes_only_execute_to_reader();
}

#endif // POLARDB_PROXY
