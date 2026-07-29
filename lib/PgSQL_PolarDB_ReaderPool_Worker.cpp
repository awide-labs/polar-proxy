/**
 * @file PgSQL_PolarDB_ReaderPool_Worker.cpp
 * @brief Worker-local ReaderPool admission, reservations, and connection reuse.
 */

#include "proxysql.h"
#include "cpp.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Logger.hpp"
#include "PgSQL_PolarDB.h"
#include "PgSQL_PolarDB_ReaderPool_Internal.h"
#include "PgSQL_Session.h"
#include "PgSQL_Thread.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <unistd.h>
#include <vector>

extern PgSQL_Logger* GloPgSQL_Logger;

#if POLARDB_PROXY
bool PgSQL_Thread::polardb_reader_wait_scope_active(
		uint64_t scope_hash) const {
	if (scope_hash == 0) {
		return false;
	}
	return std::any_of(
		polardb_reader_capacity_wait_scopes.begin(),
		polardb_reader_capacity_wait_scopes.end(),
		[scope_hash](const PolarDB_ReaderWaitScopeCount& scope) {
			return scope.scope_hash == scope_hash && scope.count > 0;
		});
}

/**
 * @brief Test whether one connection could satisfy a session's pending reader
 *        wait.
 *
 * The session must have an active capacity wait that already carries a
 * reservation identity (profile generation and pool key); a wait without one
 * never matches. The connection is then checked against that identity and
 * against the session's reader plan and wait spec.
 *
 * Call on the owning worker thread. Takes no locks.
 *
 * @param conn             Candidate connection. May be null, which never matches.
 * @param waiting_session  Session whose pending wait is being matched.
 * @param active_connection true when the connection is currently attached to a
 *        running session, which permits its async state machine to be something
 *        other than ASYNC_IDLE. Pass false for an idle cached connection.
 * @return true when the connection satisfies the waiting session's wait.
 */
bool PgSQL_Thread::polardb_reader_connection_matches_wait(
		PgSQL_Connection* conn, PgSQL_Session* waiting_session,
		bool active_connection) const {
	if (!waiting_session || waiting_session->current_hostgroup < 0) {
		return false;
	}
	const auto& wait = waiting_session->polardb_reader_capacity_wait;
	if (!wait.active || wait.reservation_profile_generation == 0 ||
			wait.reservation_pool_key.empty()) {
		return false;
	}
	return polardb_reader_connection_matches_requirements(
		conn, active_connection, wait.reservation_profile_generation,
		wait.reservation_pool_key,
			static_cast<unsigned int>(waiting_session->current_hostgroup),
			waiting_session->polardb_query.reader_plan,
			waiting_session->polardb_query.wait.spec);
}

/**
 * @brief Test whether an idle connection may be held back for a retention scope
 *        that still has waiters on this worker.
 *
 * "Active" refers to the retention scope, not to the connection: the retained
 * scope must still count at least one local waiter, and the connection is
 * checked as an idle one, so it must be in ASYNC_IDLE.
 *
 * Call on the owning worker thread. Takes no locks.
 *
 * @param conn  Candidate idle connection. May be null, which never matches.
 * @return true when the retention scope still has local waiters and the
 *         connection matches the retained requirements.
 */
bool PgSQL_Thread::polardb_reader_matches_retention(
		PgSQL_Connection* conn) const {
	return polardb_reader_wait_scope_active(
			polardb_reader_retention.scope_hash) &&
		polardb_reader_connection_matches_retention_requirements(conn, false);
}

bool PgSQL_Thread::polardb_reader_connection_matches_retention_requirements(
		PgSQL_Connection* conn, bool active_connection) const {
	if (!polardb_reader_retention.active()) {
		return false;
	}
	return polardb_reader_connection_matches_requirements(
		conn, active_connection,
		polardb_reader_retention.profile_generation,
		polardb_reader_retention.pool_key,
		polardb_reader_retention.hostgroup_id,
		polardb_reader_retention.reader_plan,
		polardb_reader_retention.wait_spec);
}

/**
 * @brief Report whether one connection may be reused for a given reader
 *        identity, plan and wait spec.
 *
 * This is the common accept test behind both retention and wait matching. The
 * connection is accepted only when all of the following hold: it is reusable,
 * not inside a transaction, not multiplex-disabled, has no dirty transaction
 * split xids, and carries exactly the given startup profile generation and pool
 * key. The check then re-validates the *server* the connection is attached to
 * against the caller's reader plan and wait spec, because a connection that is
 * still fine on its own may sit on a replica that is no longer eligible.
 *
 * Call on the owning worker thread. Takes no locks; it reads connection state
 * that only that thread may touch.
 *
 * @param conn               Candidate connection. Null is rejected.
 * @param active_connection  true when the connection is attached to a running
 *        session. This relaxes only the async-idle requirement; every other
 *        condition still applies. Pass false for an idle connection, which must
 *        then be in ASYNC_IDLE.
 * @param profile_generation Startup profile generation the connection must
 *        carry. Zero is rejected.
 * @param pool_key           Pool key the connection must carry. Empty is
 *        rejected.
 * @param hostgroup_id       Hostgroup the request routes to, used for the
 *        server eligibility check.
 * @param reader_plan        Reader plan the server must still satisfy.
 * @param wait_spec          Wait spec the server must still satisfy.
 * @return true when the connection may serve a request with this identity.
 */
bool PgSQL_Thread::polardb_reader_connection_matches_requirements(
		PgSQL_Connection* conn, bool active_connection,
		uint32_t profile_generation, const PolarDB_PoolKey& pool_key,
		unsigned int hostgroup_id,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec) const {
	if (!conn || !PgHGM || !conn->parent || profile_generation == 0 ||
			pool_key.empty() || !conn->reusable ||
			conn->IsActiveTransaction() || conn->MultiplexDisabled() ||
			conn->polardb_txn_split_xids_dirty ||
			(!active_connection &&
				conn->async_state_machine != ASYNC_IDLE) ||
			conn->polardb_startup_profile_generation != profile_generation ||
			conn->polardb_pool_key != pool_key) {
		return false;
	}
	return PgHGM->polardb_reader_server_can_serve_request(
		hostgroup_id, static_cast<PgSQL_SrvC*>(conn->parent),
		reader_plan, wait_spec);
}

/**
 * Record the exact wait scope that one worker-local reader may continue to
 * serve. Callers check whether an earlier retention scope is still useful
 * before entering this common initializer.
 */
void PgSQL_Thread::polardb_reader_start_retention(
		uint64_t scope_hash, PgSQL_HGC* hostgroup,
		unsigned int hostgroup_id, uint32_t profile_generation,
		const PolarDB_PoolKey& pool_key,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec) {
	polardb_reader_retention.scope_hash = scope_hash;
	polardb_reader_retention.hostgroup = hostgroup;
	polardb_reader_retention.hostgroup_id = hostgroup_id;
	polardb_reader_retention.profile_generation = profile_generation;
	polardb_reader_retention.pool_key = pool_key;
	polardb_reader_retention.reader_plan = reader_plan;
	polardb_reader_retention.wait_spec = wait_spec;
	POLARDB_THREAD_COUNT_ONE(this, reader_pool_retention_started);
}

/**
 * @brief Seed worker-local retention from a session's pending capacity wait.
 *
 * Silently does nothing when retention is already active on this worker, or
 * when the session has no active wait or the wait carries no reservation
 * identity (reservation server with a hostgroup, non-zero profile generation
 * and non-empty pool key). Callers cannot observe whether retention was
 * adopted, so they must not depend on it.
 *
 * Call on the owning worker thread. Takes no locks.
 *
 * @param sess  Session whose pending wait supplies the retention scope. May be
 *              null, which is a no-op.
 */
void PgSQL_Thread::polardb_reader_adopt_retention(
		PgSQL_Session* sess) {
	if (!sess || polardb_reader_retention.active()) {
		return;
	}
	const auto& wait = sess->polardb_reader_capacity_wait;
	if (!wait.active || sess->current_hostgroup < 0 ||
			!wait.reservation_server || !wait.reservation_server->myhgc ||
			wait.reservation_profile_generation == 0 || wait.reservation_pool_key.empty()) {
		return;
	}
	polardb_reader_start_retention(
		wait.scope_hash, wait.reservation_server->myhgc,
		static_cast<unsigned int>(sess->current_hostgroup),
		wait.reservation_profile_generation, wait.reservation_pool_key,
		sess->polardb_query.reader_plan, sess->polardb_query.wait.spec);
}

void PgSQL_Thread::polardb_reader_clear_inactive_retention() {
	if (!polardb_reader_retention.active() ||
			polardb_reader_wait_scope_active(
				polardb_reader_retention.scope_hash)) {
		return;
	}
	polardb_reader_retention.reset();
	POLARDB_THREAD_COUNT_ONE(this, reader_pool_retention_cleared);
}

/**
 * @brief Offer a returning retained connection to a capacity request raised by
 *        another worker.
 *
 * Call this on the connection return path before the ordinary return logic.
 * The connection must match this worker's active retention; otherwise nothing
 * happens. When another worker has a matching pending request, the connection
 * is stored into that worker's reservation under the server pool lock, and
 * inactive retention is cleared as a side effect.
 *
 * CONNECTION_RESERVED transfers the connection to another worker. This thread
 * must not use the pointer again. Every other result leaves the connection with
 * this worker for the matching local or normal return path.
 *
 * Call on the owning worker thread. Enters the server pool lock internally.
 *
 * @param conn Connection being returned. Ownership may transfer away.
 * @return The connection disposition.
 */
PolarDB_ReaderRemoteReservationResult
PgSQL_Thread::polardb_reader_try_reserve_retained_connection(
		PgSQL_Connection* conn) {
	if (!polardb_reader_retention.active() ||
			polardb_worker_index == UINT_MAX || !PgHGM ||
			!polardb_reader_connection_matches_retention_requirements(
				conn, false)) {
		return PolarDB_ReaderRemoteReservationResult::NOT_ATTEMPTED;
	}
	const PgSQL_PoolMatchKey match_key = pgsql_pool_match_key(
		polardb_reader_retention.profile_generation,
		polardb_reader_retention.pool_key);
	// Ignore unrelated and self-only request before entering the server pool
	// lock. Such a request cannot receive this connection.
	if (!polardb_reader_retention.hostgroup->
			has_reader_pool_capacity_request_for_another_worker(
				match_key, polardb_worker_index)) {
		return PolarDB_ReaderRemoteReservationResult::NOT_ATTEMPTED;
	}
	// Request detection and reservation stay in one worker-local call, so no
	// intermediate state survives between worker passes.
	POLARDB_THREAD_COUNT_ONE(this, reader_pool_remote_request_seen);
	POLARDB_THREAD_COUNT_ONE(this, reader_pool_remote_reservation_attempt);
	const PolarDB_ReaderRemoteReservationResult reservation_result =
		PgHGM->reserve_retained_reader_connection(
			conn, match_key, polardb_worker_index);
	if (reservation_result ==
			PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED) {
		POLARDB_THREAD_COUNT_ONE(
			this, reader_pool_remote_reservation_created);
		POLARDB_THREAD_COUNT_ONE(
			this, reader_pool_retained_connection_shared);
	} else {
		POLARDB_THREAD_COUNT_ONE(
			this, reader_pool_remote_reservation_not_created);
	}
	polardb_reader_clear_inactive_retention();
	return reservation_result;
}

/**
 * @brief Report what this worker already holds that could satisfy a waiting
 *        session, so the caller can skip registering a capacity request when
 *        this worker can already serve the session.
 *
 * Answers are ordered by strength and the first that applies wins: RESERVATION
 * when this worker already holds a reservation carrying a connection for the
 * session's scope and identity on a still-eligible server, LOCAL when a cached
 * worker connection matches, ACTIVE when a connection matching the wait is
 * currently in flight on another session of this worker, and NONE otherwise.
 * NONE is also returned when the session has no active wait at all.
 *
 * Finding LOCAL or ACTIVE walks the cached connection array and then every
 * session on this worker, so this is O(sessions) and belongs on the retry path
 * rather than on a per-query hot path.
 *
 * Call on the owning worker thread. Takes no locks.
 *
 * @param waiting_session  Session that is waiting for reader capacity.
 * @return The strongest ownership this worker holds for that session's wait.
 */
PolarDB_ReaderOwnership PgSQL_Thread::polardb_reader_ownership(
		PgSQL_Session* waiting_session) {
	if (!waiting_session ||
			!waiting_session->polardb_reader_capacity_wait.active) {
		return PolarDB_ReaderOwnership::NONE;
	}
	const auto& wait = waiting_session->polardb_reader_capacity_wait;
	if (polardb_reader_pool_reservation.has_connection() &&
			polardb_reader_pool_reservation.scope_hash == wait.scope_hash &&
			polardb_reader_pool_reservation.profile_generation ==
				wait.reservation_profile_generation &&
			polardb_reader_pool_reservation.pool_key == wait.reservation_pool_key &&
			polardb_reader_pool_reservation.server && PgHGM &&
			waiting_session->current_hostgroup >= 0 &&
			PgHGM->polardb_reader_server_can_serve_request(
				static_cast<unsigned int>(waiting_session->current_hostgroup),
				polardb_reader_pool_reservation.server,
				waiting_session->polardb_query.reader_plan,
				waiting_session->polardb_query.wait.spec)) {
		return PolarDB_ReaderOwnership::RESERVATION;
	}
	if (cached_connections) {
		for (unsigned int index = 0; index < cached_connections->len; index++) {
			PgSQL_Connection* conn = static_cast<PgSQL_Connection*>(
				cached_connections->index(index));
			if (polardb_reader_connection_matches_wait(
					conn, waiting_session, false)) {
				return PolarDB_ReaderOwnership::LOCAL;
			}
		}
	}
	for (unsigned int index = 0; index < mysql_sessions->len; index++) {
		PgSQL_Session* owner = static_cast<PgSQL_Session*>(
			mysql_sessions->index(index));
		PgSQL_Connection* conn = owner && owner->mybe &&
			owner->mybe->server_myds
			? owner->mybe->server_myds->myconn : nullptr;
		if (polardb_reader_connection_matches_wait(
				conn, waiting_session, true)) {
			return PolarDB_ReaderOwnership::ACTIVE;
		}
	}
	return PolarDB_ReaderOwnership::NONE;
}

/**
 * @brief Register this worker's request for reader capacity on behalf of a
 *        waiting session, and record the resulting reservation.
 *
 * Does nothing unless the initial preconditions hold: this worker has no
 * reservation already active, its worker index is set, the session's wait is
 * active with last_status READER_GROUP_BUSY, and the wait carries a full
 * reservation identity (server with hostgroup, server snapshot, non-zero
 * profile generation, non-empty pool key).
 *
 * Once those checks pass, the ownership result is counted. LOCAL and ACTIVE
 * also adopt reader retention without registering another worker. Other
 * refused registrations may likewise leave only their ownership counter
 * updated.
 *
 * On success the session's reservation server snapshot is moved into the
 * worker reservation, so the session no longer holds it. The reservation is
 * hostgroup-scoped at this point: its server stays null until a wake notifies
 * this worker that a connection is ready, which is why active() and
 * has_connection() are separate tests.
 *
 * Call on the owning worker thread.
 *
 * @param sess  Waiting session to register capacity for.
 */
void PgSQL_Thread::polardb_register_reader_capacity_request(PgSQL_Session* sess) {
	if (!sess || polardb_reader_pool_reservation.active() ||
			polardb_worker_index == UINT_MAX) {
		return;
	}
	auto& wait = sess->polardb_reader_capacity_wait;
	if (!wait.active ||
			wait.last_status != PolarDB_ReaderStatus::READER_GROUP_BUSY ||
			!wait.reservation_server || !wait.reservation_server_snapshot ||
			wait.reservation_profile_generation == 0 || wait.reservation_pool_key.empty()) {
		return;
	}
	const PolarDB_ReaderOwnership ownership =
		polardb_reader_ownership(sess);
	switch (ownership) {
	case PolarDB_ReaderOwnership::LOCAL:
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_capacity_ownership_local);
		polardb_reader_adopt_retention(sess);
		break;
	case PolarDB_ReaderOwnership::ACTIVE:
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_capacity_ownership_active);
		polardb_reader_adopt_retention(sess);
		break;
	case PolarDB_ReaderOwnership::RESERVATION:
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_capacity_ownership_reservation);
		break;
	case PolarDB_ReaderOwnership::NONE:
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_capacity_ownership_zero);
		break;
	}
	if (!polardb_reader_pool_capacity_request_should_register(wait.last_status, ownership)) {
		return;
	}
	static std::atomic<uint64_t> next_token{1};
	uint64_t token = next_token.fetch_add(1, std::memory_order_relaxed);
	if (token == 0) {
		token = next_token.fetch_add(1, std::memory_order_relaxed);
	}
	const PgSQL_PoolMatchKey match_key = pgsql_pool_match_key(
		wait.reservation_profile_generation, wait.reservation_pool_key);
	PgSQL_HGC* hostgroup = wait.reservation_server->myhgc;
	// Capacity waiting is entered only by ordinary capacity acquisition, whose
	// ReaderPool call has no excluded endpoint. Pooled-only split/failure retries
	// may use exclusions but do not enter this worker wait/reservation path.
	if (!hostgroup || !hostgroup->register_reader_pool_capacity_request(
			polardb_worker_index, token, wait.scope_hash, match_key,
			sess->polardb_query.reader_plan,
			sess->polardb_query.wait.spec)) {
		return;
	}
	polardb_reader_pool_reservation.token = token;
	polardb_reader_pool_reservation.session_id = sess->thread_session_id;
	polardb_reader_pool_reservation.scope_hash = wait.scope_hash;
	polardb_reader_pool_reservation.hostgroup = hostgroup;
	polardb_reader_pool_reservation.server = nullptr;
	polardb_reader_pool_reservation.server_snapshot =
		std::move(wait.reservation_server_snapshot);
	polardb_reader_pool_reservation.profile_generation =
		wait.reservation_profile_generation;
	polardb_reader_pool_reservation.pool_key = wait.reservation_pool_key;
}

/**
 * @brief Drop this worker's reader pool reservation and hand any reserved
 *        connection on to the next waiter.
 *
 * The shared reservation is cancelled while the worker still holds its server
 * snapshot. Local state is then cleared. If cancellation hands a connection to
 * another waiter, that waiter is signalled.
 *
 * Call on the owning worker thread.
 *
 * @param session_id  Session the reservation must belong to. Pass 0 to cancel
 *        this worker's reservation regardless of which session owns it; a
 *        non-zero value cancels only when it matches.
 */
void PgSQL_Thread::polardb_cancel_reader_reservation(uint32_t session_id) {
	if (!polardb_reader_pool_reservation.active() ||
			(session_id != 0 && polardb_reader_pool_reservation.session_id != session_id)) {
		return;
	}
	PgSQL_HGC* hostgroup = polardb_reader_pool_reservation.hostgroup;
	PgSQL_SrvC* server = polardb_reader_pool_reservation.server;
	const unsigned int worker_index = polardb_worker_index;
	const uint64_t token = polardb_reader_pool_reservation.token;
	ReaderCancelResult cancel;
	if (server) {
		cancel = server->cancel_reader_pool_reservation(worker_index, token);
	} else if (hostgroup &&
			hostgroup->cancel_reader_pool_capacity_request(
				worker_index, token)) {
		cancel.status =
			ReaderCancelStatus::REQUEST_CANCELLED;
	}
	polardb_reader_pool_reservation.clear_local();
	if (PgHGM && cancel.next_wake.has_reservation()) {
		PgHGM->polardb_route_reader_reservation_wake(cancel.next_wake);
	}
}

/**
 * @brief Take the connection reserved for a session, if one is ready.
 *
 * Before taking it, the reservation is re-validated against what the session
 * needs now: same hostgroup, same pool match key, and a server that is still
 * eligible for the session's reader plan and wait spec. A session that has
 * moved on cannot be served by a stale reservation, so the reservation is
 * cancelled instead of being handed over.
 *
 * A non-null return transfers ownership of the connection to the caller and
 * starts retention for the reservation's scope, so a further returning
 * connection can stay with this worker; the reservation itself is cleared.
 *
 * A null return means one of three different things, and the caller must not
 * collapse them: the reservation is still pending and the session should keep
 * waiting; the reservation was retired or is unknown to the server, in which
 * case local reservation state has been reset and the session must request
 * capacity again; or the reservation was no longer current for this session and
 * has been cancelled here.
 *
 * Call on the owning worker thread.
 *
 * @param sess  Session taking its reservation.
 * @return The reserved connection, now owned by the caller, or null.
 */
PgSQL_Connection* PgSQL_Thread::polardb_take_reader_reservation(
		PgSQL_Session* sess) {
	if (!sess || !polardb_reader_pool_reservation.has_connection() ||
			polardb_reader_pool_reservation.session_id != sess->thread_session_id) {
		return nullptr;
	}
	PgSQL_SrvC* server = polardb_reader_pool_reservation.server;
	const unsigned int hostgroup_id = server && server->myhgc
		? server->myhgc->hid : UINT_MAX;
	PgSQL_PoolMatchKey current_key;
	const PgSQL_PoolMatchKey registered_key = pgsql_pool_match_key(
		polardb_reader_pool_reservation.profile_generation,
		polardb_reader_pool_reservation.pool_key);
	const bool current = PgHGM && hostgroup_id != UINT_MAX &&
		sess->current_hostgroup == static_cast<int>(hostgroup_id) &&
		PgHGM->polardb_reader_pool_reservation_match_key(
			hostgroup_id, sess, sess->polardb_query.wait.spec, &current_key) &&
		current_key == registered_key &&
		PgHGM->polardb_reader_server_can_serve_request(
			hostgroup_id, server, sess->polardb_query.reader_plan,
			sess->polardb_query.wait.spec);
	if (!current) {
		polardb_cancel_reader_reservation(sess->thread_session_id);
		return nullptr;
	}
	const ReaderTakeResult result = server->take_reader_pool_reservation(
		polardb_worker_index, polardb_reader_pool_reservation.token);
	if (result.status == ReaderTakeStatus::PENDING) {
		return nullptr;
	}
	if (result.status == ReaderTakeStatus::RETIRED) {
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_reservation_missing);
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_reservation_missing_retired);
		polardb_reader_pool_reservation.clear_local();
		return nullptr;
	}
	if (result.status == ReaderTakeStatus::MISSING) {
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_reservation_missing);
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_reservation_missing_unknown);
		polardb_reader_pool_reservation.clear_local();
		return nullptr;
	}
	if (polardb_reader_retention.active()) {
		POLARDB_THREAD_COUNT_ONE(this, reader_pool_retention_cleared);
	}
	polardb_reader_start_retention(
		polardb_reader_pool_reservation.scope_hash, polardb_reader_pool_reservation.hostgroup,
		hostgroup_id, polardb_reader_pool_reservation.profile_generation,
		polardb_reader_pool_reservation.pool_key, sess->polardb_query.reader_plan,
		sess->polardb_query.wait.spec);
	polardb_reader_pool_reservation.clear_local();
	return result.conn;
}

/**
 * @brief Queue a wake telling this worker that its reservation now has a
 *        connection, and poke its pipe.
 *
 * This runs on a foreign thread, not on the worker that owns this object.
 * PgSQL_Threads_Handler::polardb_queue_reader_reservation_wake calls it while
 * holding the worker lifecycle mutex, and that is what keeps this thread object
 * alive for the duration of the call. The queue is protected by the reservation
 * wake mutex; the pipe write is coalesced, so a wake that arrives while one is
 * already pending only appends to the queue.
 *
 * When the pipe write fails the queued entry is rolled back, so a false return
 * means nothing was handed over: the caller still owns the connection and must
 * retire the reservation itself.
 *
 * @param token            Reservation token the wake refers to. Zero is
 *                         rejected.
 * @param server           Server holding the reserved connection. Must not be
 *                         null.
 * @param server_snapshot  Server list snapshot the reservation was made
 *                         against. Must not be null.
 * @return true when the wake was queued and the worker will pick it up, false
 *         when it was not queued and ownership stays with the caller.
 */
bool PgSQL_Thread::polardb_queue_reader_reservation_wake(
		uint64_t token, PgSQL_SrvC* server,
		std::shared_ptr<const void> server_snapshot) {
	if (shutdown || token == 0 || !server || !server_snapshot) {
		return false;
	}
	std::lock_guard<std::mutex> lock(polardb_reader_pool_reservation_wake_mutex);
	polardb_reader_pool_reservation_wakes.push_back(
		PolarDB_ReaderPoolReservationWakeNotification{
			token, server, std::move(server_snapshot)});
	if (polardb_reader_pool_reservation_wake_pending.exchange(
			true, std::memory_order_acq_rel)) {
		// The returning connection may belong to another worker. Keep this
		// cross-thread counter in the shared atomic instead of writing the
		// target worker's non-atomic counter array.
		POLARDB_THREAD_COUNT_ONE(nullptr, reader_pool_reservation_wake_coalesced);
		return true;
	}
	const unsigned char byte = 0;
	if (write(pipefd[1], &byte, 1) == 1 || errno == EAGAIN) {
		return true;
	}
	polardb_reader_pool_reservation_wakes.pop_back();
	polardb_reader_pool_reservation_wake_pending.store(false, std::memory_order_release);
	return false;
}

/**
 * @brief Drain the queued reservation wakes and apply the one that belongs to
 *        this worker's current reservation.
 *
 * Run this on the owning worker thread. The queue is swapped out under the
 * reservation wake mutex and processed outside it. A wake whose token no longer
 * matches the current reservation refers to a connection reserved for a request
 * this worker has already abandoned, so that reservation is cancelled and any
 * next waiter it yields is signalled onward rather than left stranded.
 *
 * @return true only when a wake filled in this worker's reservation with a
 *         server and connection, which is not the same as "wakes were
 *         consumed".
 */
bool PgSQL_Thread::polardb_process_reader_reservation_wakes() {
	if (!polardb_reader_pool_reservation_wake_pending.exchange(
			false, std::memory_order_acq_rel)) {
		return false;
	}
	std::vector<PolarDB_ReaderPoolReservationWakeNotification> wakes;
	{
		std::lock_guard<std::mutex> lock(polardb_reader_pool_reservation_wake_mutex);
		wakes.swap(polardb_reader_pool_reservation_wakes);
	}
	bool reservation_ready = false;
	for (PolarDB_ReaderPoolReservationWakeNotification& wake : wakes) {
		if (polardb_reader_pool_reservation.active() &&
				polardb_reader_pool_reservation.token == wake.token) {
			polardb_reader_pool_reservation.server = wake.server;
			polardb_reader_pool_reservation.server_snapshot =
				std::move(wake.server_snapshot);
			reservation_ready = true;
			continue;
		}
		const auto cancel = wake.server->cancel_reader_pool_reservation(
			polardb_worker_index, wake.token);
		if (PgHGM && cancel.next_wake.has_reservation()) {
			PgHGM->polardb_route_reader_reservation_wake(
				cancel.next_wake);
		}
	}
	return reservation_ready;
}

/**
 * @brief Re-enter the session handler for every session waiting on reader
 *        capacity, in reservation-first then oldest-first order.
 *
 * Waiters are ordered so the session whose reservation just became ready runs
 * first, then the remaining ones by wait start time. Once a scope reports
 * READER_GROUP_BUSY in this pass, the other sessions in that same scope are
 * skipped rather than made to repeat a lookup that is already known to fail.
 *
 * This may destroy sessions: a session whose handler returns -1, or that is
 * killed while running, is unregistered from this worker and deleted here. The
 * caller must therefore hold no session pointer or iterator across this call.
 *
 * Run on the owning worker thread.
 *
 * @param deadline_due       true when the retry deadline expired. Only then is
 *        the next retry deadline set, and only then does the pass run when
 *        nothing else changed.
 * @param reservation_ready  true when a reservation wake just landed, which
 *        promotes the reserving session to the front of the pass.
 */
void PgSQL_Thread::polardb_process_reader_capacity_waiters(
		bool deadline_due, bool reservation_ready) {
	POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_pass);
	if (deadline_due) {
		POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_pass_deadline);
	} else {
		POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_pass_local);
	}
	polardb_reader_capacity_retry_sessions.clear();
	polardb_reader_capacity_blocked_scopes.clear();
	polardb_reader_capacity_retry_sessions.reserve(
		polardb_reader_capacity_waiter_count);
	for (unsigned int n = 0; n < mysql_sessions->len; n++) {
		PgSQL_Session* sess =
			static_cast<PgSQL_Session*>(mysql_sessions->index(n));
		if (sess && sess->healthy && !sess->killed &&
				sess->polardb_reader_capacity_wait.active) {
			polardb_reader_capacity_retry_sessions.push_back(sess);
		}
	}
	const uint32_t reservation_session_id = reservation_ready && polardb_reader_pool_reservation.active()
		? polardb_reader_pool_reservation.session_id : 0;
	std::sort(polardb_reader_capacity_retry_sessions.begin(),
		polardb_reader_capacity_retry_sessions.end(),
		[reservation_session_id](const PgSQL_Session* lhs, const PgSQL_Session* rhs) {
			const bool lhs_has_reservation = reservation_session_id != 0 &&
				lhs->thread_session_id == reservation_session_id;
			const bool rhs_has_reservation = reservation_session_id != 0 &&
				rhs->thread_session_id == reservation_session_id;
			if (lhs_has_reservation != rhs_has_reservation) {
				return lhs_has_reservation;
			}
			const auto& left = lhs->polardb_reader_capacity_wait;
			const auto& right = rhs->polardb_reader_capacity_wait;
			return left.started_at_us != right.started_at_us
				? left.started_at_us < right.started_at_us
				: lhs->thread_session_id < rhs->thread_session_id;
		});

	for (PgSQL_Session* sess : polardb_reader_capacity_retry_sessions) {
		if (!deadline_due && !reservation_ready &&
				polardb_reader_local_connection_count == 0) {
			break;
		}
		auto& wait = sess->polardb_reader_capacity_wait;
		if (!wait.active || !sess->healthy || sess->killed) {
			continue;
		}
		if (polardb_reader_capacity_scope_is_blocked(
				polardb_reader_capacity_blocked_scopes, wait.scope_hash)) {
			POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_scope_skipped);
			continue;
		}

		wait.retry_admitted = true;
		wait.result_valid = false;
		sess->to_process = 1;
		POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_attempt);
		const int rc = sess->handler();
		wait.retry_admitted = false;
		if (rc == -1 || sess->killed) {
			char buf[1024];
			snprintf(buf, sizeof(buf), "%s:%d:%s()", __FILE__, __LINE__, __func__);
			GloPgSQL_Logger->log_audit_entry(
				PGSQL_LOG_EVENT_TYPE::AUTH_CLOSE, sess, NULL, buf);
			(void)unregister_session(sess);
			delete sess;
			continue;
		}
		if (!wait.result_valid) {
			continue;
		}
		if (wait.last_status == PolarDB_ReaderStatus::ACQUIRED) {
			POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_acquired);
		} else if (wait.last_status ==
				PolarDB_ReaderStatus::READER_GROUP_BUSY) {
			POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_group_busy);
			polardb_record_reader_group_busy_scope(
				polardb_reader_capacity_blocked_scopes,
				wait.scope_hash, wait.last_status);
			polardb_register_reader_capacity_request(sess);
		} else if (wait.last_status == PolarDB_ReaderStatus::READER_BUSY) {
			POLARDB_THREAD_COUNT_ONE(this, reader_capacity_retry_selected_busy);
		}
	}
	polardb_reader_capacity_retry_sessions.clear();
	if (deadline_due && polardb_reader_capacity_waiter_count > 0) {
		polardb_reader_capacity_retry_at_us =
			monotonic_time() + polardb_reader_capacity_retry_delay_us(
				pgsql_thread___connect_retries_delay);
	}
}

void PgSQL_Thread::polardb_schedule_reader_capacity_retry() {
	if (polardb_reader_capacity_waiter_count == 0) {
		return;
	}
	if (polardb_reader_capacity_retry_at_us == 0) {
		polardb_reader_capacity_retry_at_us =
			curtime + polardb_reader_capacity_retry_delay_us(
				pgsql_thread___connect_retries_delay);
	}
	mypolls.poll_timeout = polardb_reader_capacity_retry_timeout_us(
		polardb_reader_capacity_retry_at_us, curtime,
		mypolls.poll_timeout);
}

/**
 * @brief Return the next selection sequence number this worker uses to
 *        distribute reads between two readers of a hostgroup.
 *
 * The sequence is worker-local and monotonic per hostgroup. Only its first
 * value is taken from the shared selection_start atomic, which spreads the
 * starting points of different workers; every later value advances a local
 * counter, so the sequence is globally fair at seeding time rather than on
 * every call. Equal weights alternate; unequal weights map local sequence slots
 * to the configured ratio. That keeps the shared atomic off the per-query path.
 *
 * A change of server list generation invalidates the alternation, so all
 * per-hostgroup sequences are dropped and the next call for a hostgroup seeds
 * again.
 *
 * Run on the owning worker thread.
 *
 * @param hostgroup_id           Hostgroup the sequence belongs to.
 * @param server_list_generation Generation of the server list snapshot in use.
 *        Any difference from the cached generation resets every sequence.
 * @param selection_start        Shared seed counter for the hostgroup. May be
 *        null, in which case a new sequence starts at 0.
 * @return The sequence value to use for this selection.
 */
uint64_t PgSQL_Thread::polardb_next_reader_selection_sequence(
		unsigned int hostgroup_id, uint64_t server_list_generation,
		std::atomic<uint64_t>* selection_start) {
	if (!polardb_reader_selection_generation_initialized ||
			polardb_reader_selection_generation != server_list_generation) {
		polardb_reader_selection_sequences.clear();
		polardb_reader_selection_generation = server_list_generation;
		polardb_reader_selection_generation_initialized = true;
	}
	for (PolarDB_ReaderSelectionState& state :
			polardb_reader_selection_sequences) {
		if (state.hostgroup_id == hostgroup_id) {
			return state.next_sequence++;
		}
	}
	const uint64_t first_sequence = selection_start
		? selection_start->fetch_add(1, std::memory_order_relaxed) : 0;
	polardb_reader_selection_sequences.push_back(
		PolarDB_ReaderSelectionState{hostgroup_id, first_sequence + 1});
	return first_sequence;
}

void PgSQL_Thread::polardb_reader_capacity_wait_started(
		uint64_t scope_hash) {
	++polardb_reader_capacity_waiter_count;
	polardb_reader_add_waiter_to_scope(scope_hash);
	if (polardb_reader_capacity_retry_at_us == 0) {
		const uint64_t now = curtime ? curtime : monotonic_time();
		polardb_reader_capacity_retry_at_us =
			now + polardb_reader_capacity_retry_delay_us(
				pgsql_thread___connect_retries_delay);
	}
}

/**
 * Add one worker-local waiter to a routing scope. The vector stays small: it
 * contains one entry per distinct scope currently waiting on this worker.
 */
void PgSQL_Thread::polardb_reader_add_waiter_to_scope(
		uint64_t scope_hash) {
	auto scope = std::find_if(
		polardb_reader_capacity_wait_scopes.begin(),
		polardb_reader_capacity_wait_scopes.end(),
		[scope_hash](const PolarDB_ReaderWaitScopeCount& candidate) {
			return candidate.scope_hash == scope_hash;
		});
	if (scope == polardb_reader_capacity_wait_scopes.end()) {
		polardb_reader_capacity_wait_scopes.push_back(
			PolarDB_ReaderWaitScopeCount{scope_hash, 1});
	} else {
		++scope->count;
	}
}

/**
 * @brief Remove one worker-local waiter from a routing scope.
 *
 * The scope must already be registered by a matching
 * polardb_reader_add_waiter_to_scope call with a count above zero; both are
 * asserted. A session whose scope hash drifts without going through
 * polardb_reader_capacity_wait_scope_changed breaks that pairing.
 *
 * When the count reaches zero the entry is erased, which invalidates any
 * iterator or pointer into polardb_reader_capacity_wait_scopes.
 *
 * Run on the owning worker thread.
 *
 * @param scope_hash  Scope the waiter is leaving.
 * @return true when the last waiter left the scope and its entry was erased.
 */
bool PgSQL_Thread::polardb_reader_remove_waiter_from_scope(
		uint64_t scope_hash) {
	auto scope = std::find_if(
		polardb_reader_capacity_wait_scopes.begin(),
		polardb_reader_capacity_wait_scopes.end(),
		[scope_hash](const PolarDB_ReaderWaitScopeCount& candidate) {
			return candidate.scope_hash == scope_hash;
		});
	assert(scope != polardb_reader_capacity_wait_scopes.end());
	assert(scope->count > 0);
	if (--scope->count != 0) {
		return false;
	}
	polardb_reader_capacity_wait_scopes.erase(scope);
	return true;
}

/**
 * @brief Move a still-waiting session from one routing scope to another.
 *
 * Call this whenever a waiting session's scope hash changes. It is the only
 * legal way to do so: letting the hash drift leaves the old scope counted and
 * the new one unregistered, and the later
 * polardb_reader_capacity_wait_finished then asserts on a scope that was never
 * added.
 *
 * The session stays a waiter throughout, so polardb_reader_capacity_waiter_count
 * is unchanged. Emptying the old scope clears retention that no longer has a
 * waiter behind it.
 *
 * Run on the owning worker thread.
 *
 * @param old_scope_hash  Scope the session is leaving.
 * @param new_scope_hash  Scope the session is entering. Equal hashes are a
 *                        no-op.
 */
void PgSQL_Thread::polardb_reader_capacity_wait_scope_changed(
		uint64_t old_scope_hash, uint64_t new_scope_hash) {
	if (old_scope_hash == new_scope_hash) {
		return;
	}
	if (polardb_reader_remove_waiter_from_scope(old_scope_hash)) {
		polardb_reader_clear_inactive_retention();
	}
	polardb_reader_add_waiter_to_scope(new_scope_hash);
}

/**
 * @brief Record that one session stopped waiting for reader capacity.
 *
 * Pair this exactly once with each polardb_reader_capacity_wait_started, and
 * pass the session's *current* scope hash — the one it holds now, after any
 * polardb_reader_capacity_wait_scope_changed. Both the waiter count and the
 * scope entry are asserted.
 *
 * Draining the last waiter on this worker resets the whole retry state: the
 * retry deadline is cleared, the scope and blocked-scope bookkeeping is
 * emptied, and retention is dropped.
 *
 * Run on the owning worker thread.
 *
 * @param scope_hash  Scope the finishing session was waiting in.
 */
void PgSQL_Thread::polardb_reader_capacity_wait_finished(
		uint64_t scope_hash) {
	assert(polardb_reader_capacity_waiter_count > 0);
	--polardb_reader_capacity_waiter_count;
	if (polardb_reader_remove_waiter_from_scope(scope_hash)) {
		polardb_reader_clear_inactive_retention();
	}
	if (polardb_reader_capacity_waiter_count == 0) {
		polardb_reader_capacity_retry_at_us = 0;
		polardb_reader_capacity_wait_scopes.clear();
		polardb_reader_capacity_blocked_scopes.clear();
		polardb_reader_clear_inactive_retention();
	}
}

static bool polardb_local_reader_connection_matches(
		const PgSQL_Connection* conn, const PgSQL_SrvC* server,
		uint32_t profile_generation, const PolarDB_PoolKey& pool_key) {
	return conn && conn->parent == server &&
		conn->polardb_startup_profile_generation == profile_generation &&
		conn->polardb_pool_key == pool_key;
}

static void polardb_log_local_reader_count_mismatch(
		const char* operation, unsigned int recorded,
		unsigned int observed) {
	static std::atomic<bool> logged{false};
	if (!logged.exchange(true, std::memory_order_relaxed)) {
		proxy_error(
			"PolarDB local reader connection count mismatch during %s: recorded=%u observed=%u\n",
			operation, recorded, observed);
	}
}

unsigned int polardb_scan_cached_readers(
		PtrArray* cached_connections) {
	unsigned int count = 0;
	if (!cached_connections) {
		return count;
	}
	for (unsigned int index = 0; index < cached_connections->len; ++index) {
		const PgSQL_Connection* conn =
			static_cast<const PgSQL_Connection*>(
				cached_connections->index(index));
		if (conn && !conn->polardb_pool_key.empty()) {
			++count;
		}
	}
	return count;
}

void polardb_repair_local_reader_count(
		const char* operation, unsigned int* recorded,
		unsigned int observed) {
	if (*recorded == observed) {
		return;
	}
	polardb_log_local_reader_count_mismatch(
		operation, *recorded, observed);
	*recorded = observed;
	assert(*recorded == observed);
}

/**
 * @brief Take a matching reader connection out of this worker's local cache.
 *
 * On a hit the connection is detached from the cached connection array and
 * ownership passes to the caller, and the worker-local reader count is
 * decremented. The caller must therefore either use the connection or return or
 * destroy it; leaving it on the floor leaks it. A hit found while the count
 * still reads zero repairs the count by recounting the array.
 *
 * A miss is returned for a null server, an empty pool key, and for any server
 * whose fast status is not ONLINE.
 *
 * Run on the owning worker thread. Takes no locks.
 *
 * @param server             Server the connection must be attached to.
 * @param profile_generation Startup profile generation the connection must
 *                           carry.
 * @param pool_key           Pool key the connection must carry.
 * @return The connection, now owned by the caller, or null on a miss.
 */
PgSQL_Connection* PgSQL_Thread::polardb_take_local_reader_connection(
		PgSQL_SrvC* server, uint32_t profile_generation,
		const PolarDB_PoolKey& pool_key) {
	if (!server || pool_key.empty() || !cached_connections) {
		return nullptr;
	}
	if (server->polardb_fast_status_value() != MYSQL_SERVER_STATUS_ONLINE) {
		return nullptr;
	}
	unsigned int scan_steps = 0;
	for (unsigned int index = 0; index < cached_connections->len; index++) {
		++scan_steps;
		PgSQL_Connection* conn = static_cast<PgSQL_Connection*>(
			cached_connections->index(index));
		if (polardb_local_reader_connection_matches(
				conn, server, profile_generation, pool_key)) {
#if POLARDB_PROFILE
			POLARDB_PROFILE_THREAD_COUNT(
				this, reader_pool_local_scan_steps, scan_steps);
#endif // POLARDB_PROFILE
			if (polardb_reader_local_connection_count == 0) {
				polardb_repair_local_reader_count(
					"local take",
					&polardb_reader_local_connection_count,
					polardb_scan_cached_readers(
						cached_connections));
			}
			assert(polardb_reader_local_connection_count > 0);
			if (polardb_reader_local_connection_count > 0) {
				--polardb_reader_local_connection_count;
			}
			return static_cast<PgSQL_Connection*>(
				cached_connections->remove_index_fast(index));
		}
	}
#if POLARDB_PROFILE
	POLARDB_PROFILE_THREAD_COUNT(
		this, reader_pool_local_scan_steps, scan_steps);
#endif // POLARDB_PROFILE
	return nullptr;
}

PolarDB_ReaderLocalReturnDecision
PgSQL_Thread::polardb_reader_local_return_decision(
		PgSQL_Connection* conn) const {
	return PgHGM
		? PgHGM->polardb_reader_local_return_decision(
			conn, polardb_worker_index)
		: PolarDB_ReaderLocalReturnDecision{};
}

/**
 * @brief Keep a returning reader connection in this worker's local cache.
 *
 * The worker takes ownership of the connection. It is stamped with the current
 * worker cache epoch so a later epoch bump can evict it, the worker-local
 * reader count is incremented, and this pass is marked as having added one.
 *
 * The connection is accepted unconditionally and is not null-checked: whether
 * it may be kept locally is settled by the caller, which reaches this
 * through polardb_reader_local_return_decision or
 * polardb_reader_try_reserve_retained_connection.
 *
 * Run on the owning worker thread.
 *
 * @param conn  Already-vetted connection to keep. Must not be null.
 */
void PgSQL_Thread::polardb_store_reader_connection_locally(
		PgSQL_Connection* conn) {
	if (!cached_connections) {
		cached_connections = new PtrArray();
	}
	conn->polardb_worker_cache_epoch = polardb_reader_cache_epoch;
	POLARDB_PROFILE_THREAD_COUNT_ONE(
		this, reader_pool_local_store_accepted);
	cached_connections->add(conn);
	++polardb_reader_local_connection_count;
	polardb_reader_local_added_this_pass = true;
}
#endif // POLARDB_PROXY
