/**
 * @file base_session_unit-t.cpp
 * @brief Regression tests for shared Base_Session helpers.
 */

#include "tap.h"
#include "PgSQL_Session.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "ProxySQL_Poll.h"
#include "Base_Thread.h"

#include <functional>
#include <poll.h>
#include <vector>

static void test_update_expired_conns_after_reset() {
	PgSQL_Session* sess = new PgSQL_Session();
	ok(sess->mybes != nullptr, "pgsql session starts with backend array");

	delete sess->mybes;
	sess->mybes = nullptr;
	ok(sess->mybes == nullptr, "pgsql session can reach no-backend-array state");

	const std::vector<std::function<bool(PgSQL_Connection*)>> checks;
	ok(sess->find_backend(0) == nullptr,
		"find_backend tolerates reset session with no backend array");
	ok(sess->has_any_backend() == false,
		"has_any_backend tolerates reset session with no backend array");
	sess->reset_all_backends();
	ok(true, "reset_all_backends tolerates reset session with no backend array");
	sess->update_expired_conns(checks);
	ok(true, "update_expired_conns tolerates reset session with no backend array");
	ok(sess->NumActiveTransactions() == 0,
		"NumActiveTransactions tolerates reset session with no backend array");
	ok(sess->HasOfflineBackends() == false,
		"HasOfflineBackends tolerates reset session with no backend array");
	ok(sess->SetEventInOfflineBackends() == false,
		"SetEventInOfflineBackends tolerates reset session with no backend array");
	ok(sess->FindOneActiveTransaction() == -1,
		"FindOneActiveTransaction tolerates reset session with no backend array");

	// Avoid the global client-connection counter path; this narrow unit does not
	// initialize PgHGM.
	sess->connections_handler = true;
	delete sess;
}

static void test_backend_only_handler_without_backend() {
	PgSQL_Session* sess = new PgSQL_Session();
	sess->connections_handler = true;
	sess->client_myds = nullptr;
	sess->mybe = nullptr;
	sess->to_process = 1;

	ok(sess->handler() == -1,
		"backend-only handler terminates instead of asserting when backend is missing");

	delete sess;
}

static void test_pgsql_poll_remove_data_stream_with_stale_index() {
	ProxySQL_Poll<PgSQL_Data_Stream> poll;
	PgSQL_Data_Stream* first = new PgSQL_Data_Stream();
	PgSQL_Data_Stream* second = new PgSQL_Data_Stream();
	first->myds_type = MYDS_BACKEND_NOT_CONNECTED;
	second->myds_type = MYDS_BACKEND_NOT_CONNECTED;

	poll.add(POLLIN, 101, first, 0);
	poll.add(POLLIN, 102, second, 0);
	second->poll_fds_idx = 99;

	poll.remove_data_stream(second);
	ok(poll.len == 1, "poll removal falls back to pointer lookup for stale data-stream index");
	ok(poll.myds[0] == first, "stale-index removal leaves the unrelated stream registered");
	ok(second->mypolls == nullptr && second->poll_fds_idx == -1,
		"removed data stream is detached from poll state");

	delete second;
	delete first;
	ok(poll.len == 0, "remaining data stream unregisters from poll during destruction");
}

static void test_session_pause_poll_timeout() {
	ok(Base_Thread::session_pause_poll_timeout(1000, 1400, 0) == 400,
		"future session pause supplies the poll timeout when none is set");
	ok(Base_Thread::session_pause_poll_timeout(1000, 1400, 200) == 200,
		"future session pause does not lengthen an existing poll timeout");
	ok(Base_Thread::session_pause_poll_timeout(1000, 1400, 800) == 400,
		"future session pause shortens a longer poll timeout");
	ok(Base_Thread::session_pause_poll_timeout(1000, 1000, 0) == 1,
		"due session pause becomes runnable without unsigned underflow");
	ok(Base_Thread::session_pause_poll_timeout(1000, 900, 500) == 1,
		"expired session pause becomes runnable without unsigned underflow");
}

int main() {
	plan(20);
	test_update_expired_conns_after_reset();
	test_backend_only_handler_without_backend();
	test_pgsql_poll_remove_data_stream_with_stale_index();
	test_session_pause_poll_timeout();
	return exit_status();
}
