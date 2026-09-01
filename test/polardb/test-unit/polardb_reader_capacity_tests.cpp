/**
 * @file polardb_reader_capacity_tests.cpp
 * @brief ReaderPool capacity request, reservation, and retention tests.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
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
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

extern PgSQL_HostGroups_Manager* PgHGM;
extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY

struct PoolUnlockCheckingDeleter {
	PgSQL_SrvC* server;
	std::atomic<bool>* released_outside_lock;

	void operator()(const void* value) const {
		std::atomic<bool> acquired{false};
		std::thread lock_probe([&]() {
			if (server->pool_mutex.try_lock()) {
				acquired.store(true, std::memory_order_release);
				server->pool_mutex.unlock();
			}
		});
		lock_probe.join();
		released_outside_lock->store(
			acquired.load(std::memory_order_acquire),
			std::memory_order_release);
		delete static_cast<const int*>(value);
	}
};

static void test_reader_pool_reservation_lifecycle() {
	const int writer_hg = 902;
	const int reader_hg = 903;
	stage_polardb_topology_two_readers(PgHGM, "PostgreSQL reader reservation",
		writer_hg, "polardb-reservation-writer", 22332,
		reader_hg,
		"polardb-reservation-reader-a", 22333,
		"polardb-reservation-reader-b", 22334);
	PgSQL_SrvC* reader = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), "polardb-reservation-reader-a", 22333);
	PgSQL_SrvC* peer = find_pgsql_server(
		PgHGM->MyHGC_lookup(reader_hg), "polardb-reservation-reader-b", 22334);
	ok(reader != nullptr && peer != nullptr,
		"PostgreSQL reader reservation: both reader servers are available");
	if (!reader || !peer) {
		return;
	}

	PgSQL_PoolMatchKey key_a;
	key_a.words[0] = 1;
	key_a.words[1] = 101;
	PgSQL_PoolMatchKey key_b;
	key_b.words[0] = 2;
	key_b.words[1] = 202;
	PolarDB_Query_ReaderPlan replica_only_plan;
	replica_only_plan.require_replica = true;
	ok(!reader->myhgc->register_reader_pool_capacity_request(
			16, 116, 116, key_a, replica_only_plan, PolarDB_WaitSpec{}) &&
			reader->reader_pool_capacity_request_count() == 0,
		"PostgreSQL reader reservation: replica-only pooled reads cannot enter unsupported reservation matching");
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->polardb_selected_server_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	ok(reader->add_matching_connection(conn, key_a) &&
			!conn->polardb_selected_server_snapshot,
		"PostgreSQL reader reservation: returning an unreserved connection releases its routing snapshot");
	ok(reader->take_matching_connection(key_a) == conn,
		"PostgreSQL reader reservation: connection moves to USED without retaining its routing snapshot");

	ok(unit_register_reader_pool_capacity_request(reader, 1, 101, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 3, 103, key_b) &&
			unit_register_reader_pool_capacity_request(reader, 2, 102, key_a) &&
			reader->reader_pool_capacity_request_count() == 3,
		"PostgreSQL reader reservation: keyed request queues preserve per-key FIFO across interleaved keys");
	const unsigned long long duplicate_token_before =
		PgHGM->status.polardb_reader_pool_capacity_request_duplicate_token.load(
			std::memory_order_relaxed);
	ok(!unit_register_reader_pool_capacity_request(reader, 4, 101, key_b) &&
			reader->reader_pool_capacity_request_count() == 3 &&
			PgHGM->status.polardb_reader_pool_capacity_request_duplicate_token.load(
				std::memory_order_relaxed) == duplicate_token_before + 1,
		"PostgreSQL reader reservation: another worker cannot reuse an active token");
	const unsigned long long duplicate_worker_before =
		PgHGM->status.polardb_reader_pool_capacity_request_duplicate_worker.load(
			std::memory_order_relaxed);
	ok(!unit_register_reader_pool_capacity_request(reader, 1, 104, key_b) &&
			reader->reader_pool_capacity_request_count() == 3 &&
			PgHGM->status.polardb_reader_pool_capacity_request_duplicate_worker.load(
				std::memory_order_relaxed) == duplicate_worker_before + 1,
		"PostgreSQL reader reservation: repeated worker registration is rejected and counted");
	ok(reader->has_matching_reader_pool_capacity_request(key_a) &&
			reader->has_matching_reader_pool_capacity_request(key_b),
		"PostgreSQL reader reservation: local returns can detect matching cold-worker request");
	const ServerReturnResult first_return =
		reader->return_matching_connection(conn, key_a);
	ok(first_return.stored() &&
			first_return.wake.worker_index == 1 &&
			first_return.wake.token == 101 &&
			first_return.wake.server_snapshot != nullptr &&
			reader->reader_pool_reservation_count() == 1 &&
			reader->reader_pool_capacity_request_count() == 2,
		"PostgreSQL reader reservation: first matching request receives the returned FREE connection");
	ok(reader->pool_free_count_value() == 1 &&
			reader->pool_used_count_value() == 0 &&
			reader->take_matching_connection(key_a) == nullptr,
		"PostgreSQL reader reservation: reserved connection stays FREE but ordinary exact lookup skips it");
	{
		std::lock_guard<std::recursive_mutex> pool_lock(reader->pool_mutex);
		pgsql_polardb_unit_corrupt_match_key_positions(
			reader->ConnectionsFree);
	}
	ok(reader->matching_connection_count(key_a) == 0 &&
			reader->reader_pool_reservation_count() == 1,
		"PostgreSQL reader reservation: exact-index repair keeps a reserved FREE connection hidden");
#if POLARDB_PROFILE
	PgSQL_PoolGetResult reserved_exact =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, key_a, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/0);
	ok(!reserved_exact.conn && reserved_exact.exact_match_reserved,
		"PostgreSQL reader reservation: pooled-only exact miss identifies compatible capacity hidden by a reservation");
#endif // POLARDB_PROFILE
	PgSQL_PoolGetResult reserved_capacity =
		PgHGM->get_connection_from_selected_server(
			reader, reader_hg, key_a, nullptr,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH,
			/*selected_max_connections=*/1);
	ok(!reserved_capacity.conn && reserved_capacity.server_saturated,
		"PostgreSQL reader reservation: non-evictable reserved FREE capacity reports saturation without entering creation");
	ReaderTakeResult wrong_worker =
		reader->take_reader_pool_reservation(9, 101);
	ok(wrong_worker.status == ReaderTakeStatus::MISSING &&
			reader->reader_pool_reservation_count() == 1,
		"PostgreSQL reader reservation: a reserved connection is available only to its assigned worker");
	ReaderTakeResult first_take =
		reader->take_reader_pool_reservation(1, 101);
	ok(first_take.status == ReaderTakeStatus::ACQUIRED &&
			first_take.conn == conn &&
			reader->pool_free_count_value() == 0 &&
			reader->pool_used_count_value() == 1,
		"PostgreSQL reader reservation: target worker transfers the reserved connection FREE to USED");

	const ServerReturnResult second_return =
		reader->return_matching_connection(conn, key_a);
	ok(second_return.stored() &&
			second_return.wake.worker_index == 2 &&
			second_return.wake.token == 102,
		"PostgreSQL reader reservation: the next compatible worker receives the next return");
	ok(unit_register_reader_pool_capacity_request(reader, 4, 104, key_a),
		"PostgreSQL reader reservation: a later compatible request can wait behind an active reservation");
	const auto reassigned_cancel =
		reader->cancel_reader_pool_reservation(2, 102);
	ok(reassigned_cancel.status ==
				ReaderCancelStatus::CONNECTION_RETURNED &&
			reassigned_cancel.next_wake.worker_index == 4 &&
			reassigned_cancel.next_wake.token == 104 &&
			reader->reader_pool_reservation_count() == 1,
		"PostgreSQL reader reservation: cancellation reassigns the same FREE connection without a race");
	ReaderTakeResult reassigned_take =
		reader->take_reader_pool_reservation(4, 104);
	ok(reassigned_take.status == ReaderTakeStatus::ACQUIRED &&
			reassigned_take.conn == conn,
		"PostgreSQL reader reservation: reassigned worker takes the connection before normal selection");
	const ServerReturnResult ordinary_return =
		reader->return_matching_connection(conn, key_a);
	ok(ordinary_return.stored() &&
			!ordinary_return.wake.has_reservation() &&
			reader->take_matching_connection(key_a) == conn,
		"PostgreSQL reader reservation: unmatched return restores ordinary exact availability");
	ok(reader->cancel_reader_pool_reservation(3, 103).completed() &&
			reader->reader_pool_capacity_request_count() == 0 &&
			!reader->has_matching_reader_pool_capacity_request(key_b),
		"PostgreSQL reader reservation: pending request cancels without connection ownership");

	PgSQL_Connection* self_donor = make_cached_reader_connection(reader);
	ok(reader->add_used_matching_connection(self_donor, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 20, 120, key_a),
		"PostgreSQL reader remote reservation: donor connection starts in USED with a request from the same worker");
	PolarDB_ReaderPoolReservationWake self_wake;
	const PolarDB_ReaderRemoteReservationResult self_result =
		reader->reserve_matching_used_connection(
			self_donor, key_a, 20, &self_wake);
	PgSQL_PoolMatchKey self_used_key;
	ok(self_result ==
			PolarDB_ReaderRemoteReservationResult::NO_REMOTE_REQUEST &&
			!self_wake.has_reservation() &&
			reader->used_connection_match_key(self_donor, &self_used_key) &&
			self_used_key == key_a &&
			reader->reader_pool_capacity_request_count() == 1,
		"PostgreSQL reader remote reservation: donor cannot satisfy its own request and remains in USED");
	ok(reader->cancel_reader_pool_reservation(20, 120).completed() &&
			reader->remove_used_connection(self_donor),
		"PostgreSQL reader remote reservation: self-request cancellation leaves the donor removable from USED");
	delete self_donor;

	PgSQL_Connection* snapshot_donor = make_cached_reader_connection(reader);
	std::atomic<bool> snapshot_released_outside_lock{false};
	snapshot_donor->polardb_selected_server_snapshot =
		std::shared_ptr<const void>(
			new int(1),
			PoolUnlockCheckingDeleter{reader, &snapshot_released_outside_lock});
	ok(reader->add_used_matching_connection(snapshot_donor, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 23, 123, key_a),
		"PostgreSQL reader reservation snapshot: donor starts with an older routing snapshot");
	const unsigned int snapshot_free_before =
		reader->pool_free_count_value();
	const unsigned int snapshot_used_before =
		reader->pool_used_count_value();
	PolarDB_ReaderPoolReservationWake snapshot_wake;
	const PolarDB_ReaderRemoteReservationResult snapshot_result =
		reader->reserve_matching_used_connection(
			snapshot_donor, key_a, 24, &snapshot_wake);
	ok(snapshot_result ==
			PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED &&
			reader->pool_free_count_value() == snapshot_free_before + 1 &&
			reader->pool_used_count_value() + 1 == snapshot_used_before &&
			reader->take_matching_connection(key_a) == nullptr &&
			snapshot_wake.server_snapshot != nullptr &&
			snapshot_released_outside_lock.load(std::memory_order_acquire),
		"PostgreSQL reader reservation snapshot: reserved connection is excluded from normal matching and the old routing snapshot is released after unlocking");
	const ReaderTakeResult snapshot_take =
		reader->take_reader_pool_reservation(23, 123);
	ok(snapshot_take.status == ReaderTakeStatus::ACQUIRED &&
			snapshot_take.conn == snapshot_donor &&
			reader->remove_used_connection(snapshot_donor),
		"PostgreSQL reader reservation snapshot: assigned worker takes the reserved connection");
	delete snapshot_donor;

	PgSQL_Connection* competing_donor_a =
		make_cached_reader_connection(reader);
	PgSQL_Connection* competing_donor_b =
		make_cached_reader_connection(peer);
	ok(reader->add_used_matching_connection(competing_donor_a, key_a) &&
			peer->add_used_matching_connection(competing_donor_b, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 21, 121, key_a),
		"PostgreSQL reader remote reservation: two returning workers observe one remote request");
	PolarDB_ReaderPoolReservationWake competing_wake_a;
	PolarDB_ReaderPoolReservationWake competing_wake_b;
	PolarDB_ReaderRemoteReservationResult competing_result_a =
		PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	PolarDB_ReaderRemoteReservationResult competing_result_b =
		PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	std::thread competing_return_a([&]() {
		competing_result_a = reader->reserve_matching_used_connection(
			competing_donor_a, key_a, 30, &competing_wake_a);
	});
	std::thread competing_return_b([&]() {
		competing_result_b = peer->reserve_matching_used_connection(
			competing_donor_b, key_a, 31, &competing_wake_b);
	});
	competing_return_a.join();
	competing_return_b.join();
	const bool donor_a_reserved = competing_result_a ==
		PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED;
	const bool donor_b_reserved = competing_result_b ==
		PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED;
	const bool donor_a_retained = competing_result_a ==
		PolarDB_ReaderRemoteReservationResult::NO_REMOTE_REQUEST;
	const bool donor_b_retained = competing_result_b ==
		PolarDB_ReaderRemoteReservationResult::NO_REMOTE_REQUEST;
	PolarDB_ReaderPoolReservationWake& competing_wake =
		competing_wake_a.has_reservation()
		? competing_wake_a : competing_wake_b;
	PgSQL_Connection* retained_donor = donor_a_retained
		? competing_donor_a : competing_donor_b;
	PgSQL_SrvC* retained_server = donor_a_retained ? reader : peer;
	PgSQL_PoolMatchKey retained_key;
	ok(donor_a_reserved != donor_b_reserved &&
			donor_a_retained != donor_b_retained &&
			competing_wake.has_reservation() && competing_wake.worker_index == 21 &&
			competing_wake.token == 121 &&
			reader->reader_pool_capacity_request_count() == 0 &&
			retained_server->used_connection_match_key(
				retained_donor, &retained_key) && retained_key == key_a,
		"PostgreSQL reader remote reservation: concurrent returns reserve one connection and leave the other with its worker");
	const ReaderTakeResult competing_take =
		competing_wake.server->take_reader_pool_reservation(21, 121);
	ok(competing_take.status == ReaderTakeStatus::ACQUIRED &&
			competing_take.conn != nullptr,
		"PostgreSQL reader remote reservation: waiting worker takes the reserved connection");
	reader->remove_used_connection(competing_donor_a);
	peer->remove_used_connection(competing_donor_b);
	delete competing_donor_a;
	delete competing_donor_b;

	PgSQL_Connection* cancelled_donor = make_cached_reader_connection(reader);
	ok(reader->add_used_matching_connection(cancelled_donor, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 22, 122, key_a) &&
			reader->cancel_reader_pool_reservation(22, 122).completed(),
		"PostgreSQL reader remote reservation: remote request can cancel before the connection returns");
	PolarDB_ReaderPoolReservationWake cancelled_wake;
	const PolarDB_ReaderRemoteReservationResult cancelled_result =
		reader->reserve_matching_used_connection(
			cancelled_donor, key_a, 30, &cancelled_wake);
	PgSQL_PoolMatchKey cancelled_used_key;
	ok(cancelled_result ==
			PolarDB_ReaderRemoteReservationResult::NO_REMOTE_REQUEST &&
			!cancelled_wake.has_reservation() &&
			reader->used_connection_match_key(
				cancelled_donor, &cancelled_used_key) &&
			cancelled_used_key == key_a,
		"PostgreSQL reader remote reservation: cancelling the request leaves the returning connection in USED");
	reader->remove_used_connection(cancelled_donor);
	delete cancelled_donor;

	PgSQL_Connection* invalid_donor = make_cached_reader_connection(reader);
	const PgSQL_PoolMatchKey invalid_key =
		unit_reader_pool_match_key(invalid_donor);
	ok(reader->add_used_matching_connection(invalid_donor, invalid_key) &&
			unit_register_reader_pool_capacity_request(reader, 23, 123, invalid_key),
		"PostgreSQL reader remote reservation: non-reusable connection starts in USED with a matching remote request");
	invalid_donor->reusable = false;
	const PolarDB_ReaderRemoteReservationResult invalid_result =
		PgHGM->reserve_retained_reader_connection(
			invalid_donor, invalid_key, 30);
	PgSQL_PoolMatchKey invalid_used_key;
	ok(invalid_result ==
			PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID &&
			reader->used_connection_match_key(
				invalid_donor, &invalid_used_key) &&
			reader->reader_pool_capacity_request_count() == 1,
		"PostgreSQL reader remote reservation: non-reusable connection cannot satisfy the request");
	invalid_donor->reusable = true;
	reader->polardb_fast_status.store(
		MYSQL_SERVER_STATUS_OFFLINE_HARD, std::memory_order_relaxed);
	const PolarDB_ReaderRemoteReservationResult offline_result =
		PgHGM->reserve_retained_reader_connection(
			invalid_donor, invalid_key, 30);
	reader->polardb_fast_status.store(
		MYSQL_SERVER_STATUS_ONLINE, std::memory_order_relaxed);
	ok(offline_result ==
			PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID &&
			reader->used_connection_match_key(
				invalid_donor, &invalid_used_key) &&
			reader->cancel_reader_pool_reservation(23, 123).completed(),
		"PostgreSQL reader remote reservation: offline connection remains in USED and leaves the request pending");
	reader->remove_used_connection(invalid_donor);
	delete invalid_donor;

	PolarDB_Query_ReaderPlan lag_plan;
	lag_plan.group_lsn = 1000;
	lag_plan.max_lag_bytes = 10;
	PolarDB_WaitSpec no_wait;
	reader->polardb_current_lsn.store(1000, std::memory_order_relaxed);
	reader->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	peer->polardb_current_lsn.store(900, std::memory_order_relaxed);
	peer->lsn_updated_at.store(monotonic_time(), std::memory_order_relaxed);
	PgSQL_Connection* lagged_peer_conn = make_cached_reader_connection(peer);
	ok(peer->add_used_matching_connection(lagged_peer_conn, key_b) &&
			reader->myhgc->register_reader_pool_capacity_request(
				11, 111, 111, key_b, lag_plan, no_wait),
		"PostgreSQL reader reservation: lag-qualified hostgroup request is registered independently of its original reader");
	const ServerReturnResult lagged_return =
		peer->return_matching_connection(lagged_peer_conn, key_b);
	ok(lagged_return.stored() && !lagged_return.wake.has_reservation() &&
			peer->take_matching_connection(key_b) == lagged_peer_conn &&
			reader->reader_pool_capacity_request_count() == 1,
		"PostgreSQL reader reservation: a lag-ineligible peer leaves the hostgroup request pending");
	peer->remove_used_connection(lagged_peer_conn);
	delete lagged_peer_conn;
	PgSQL_Connection* eligible_conn = make_cached_reader_connection(reader);
	ok(reader->add_used_matching_connection(eligible_conn, key_b),
		"PostgreSQL reader reservation: eligible donor starts in USED");
	const ServerReturnResult eligible_return =
		reader->return_matching_connection(eligible_conn, key_b);
	ok(eligible_return.stored() &&
			eligible_return.wake.worker_index == 11 &&
			eligible_return.wake.token == 111 &&
			eligible_return.wake.server == reader,
		"PostgreSQL reader reservation: current lag rules select the eligible donor");
	const ReaderTakeResult eligible_take =
		reader->take_reader_pool_reservation(11, 111);
	ok(eligible_take.status == ReaderTakeStatus::ACQUIRED &&
			eligible_take.conn == eligible_conn,
		"PostgreSQL reader reservation: waiting worker takes the connection reserved by the lag-eligible reader");
	reader->remove_used_connection(eligible_conn);
	delete eligible_conn;

	PolarDB_Query_ReaderPlan unrestricted_plan;
	PgSQL_Connection* mixed_policy_conn =
		make_cached_reader_connection(peer);
	ok(peer->add_used_matching_connection(mixed_policy_conn, key_b) &&
			reader->myhgc->register_reader_pool_capacity_request(
				14, 114, 114, key_b, lag_plan, no_wait) &&
			reader->myhgc->register_reader_pool_capacity_request(
				15, 115, 115, key_b, unrestricted_plan, no_wait),
		"PostgreSQL reader reservation: same-key requests may carry different lag policies");
	const ServerReturnResult mixed_policy_return =
		peer->return_matching_connection(mixed_policy_conn, key_b);
	ok(mixed_policy_return.stored() &&
			mixed_policy_return.wake.worker_index == 15 &&
			mixed_policy_return.wake.token == 115 &&
			reader->reader_pool_capacity_request_count() == 1,
		"PostgreSQL reader reservation: one server check still skips a lag-ineligible first request");
	const ReaderTakeResult mixed_policy_take =
		peer->take_reader_pool_reservation(15, 115);
	ok(mixed_policy_take.status ==
			ReaderTakeStatus::ACQUIRED &&
			mixed_policy_take.conn == mixed_policy_conn &&
			reader->cancel_reader_pool_reservation(14, 114).completed(),
		"PostgreSQL reader reservation: later eligible request receives the connection while the first request remains pending");
	peer->remove_used_connection(mixed_policy_conn);
	delete mixed_policy_conn;
	reader->polardb_current_lsn.store(0, std::memory_order_relaxed);
	peer->polardb_current_lsn.store(0, std::memory_order_relaxed);

	PgSQL_Connection* concurrent_a = make_cached_reader_connection(reader);
	PgSQL_Connection* concurrent_b = make_cached_reader_connection(peer);
	ok(reader->add_used_matching_connection(concurrent_a, key_a) &&
			peer->add_used_matching_connection(concurrent_b, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 12, 112, key_a) &&
			unit_register_reader_pool_capacity_request(peer, 13, 113, key_a),
		"PostgreSQL reader reservation: two simultaneous returns have two matching capacity requests");
	ServerReturnResult concurrent_return_a;
	ServerReturnResult concurrent_return_b;
	std::thread return_a([&]() {
		concurrent_return_a = reader->return_matching_connection(
			concurrent_a, key_a);
	});
	std::thread return_b([&]() {
		concurrent_return_b = peer->return_matching_connection(
			concurrent_b, key_a);
	});
	return_a.join();
	return_b.join();
	const bool concurrent_tokens_match =
		(concurrent_return_a.wake.token == 112 &&
			concurrent_return_b.wake.token == 113) ||
		(concurrent_return_a.wake.token == 113 &&
			concurrent_return_b.wake.token == 112);
	ok(concurrent_return_a.stored() && concurrent_return_b.stored() &&
			concurrent_return_a.wake.has_reservation() &&
			concurrent_return_b.wake.has_reservation() &&
			concurrent_tokens_match &&
			reader->reader_pool_capacity_request_count() == 0,
		"PostgreSQL reader reservation: simultaneous returns reserve one connection for each waiting worker");
	const ReaderTakeResult concurrent_take_a =
		concurrent_return_a.wake.server->take_reader_pool_reservation(
			concurrent_return_a.wake.worker_index,
			concurrent_return_a.wake.token);
	const ReaderTakeResult concurrent_take_b =
		concurrent_return_b.wake.server->take_reader_pool_reservation(
			concurrent_return_b.wake.worker_index,
			concurrent_return_b.wake.token);
	ok(concurrent_take_a.status == ReaderTakeStatus::ACQUIRED &&
			concurrent_take_b.status == ReaderTakeStatus::ACQUIRED,
		"PostgreSQL reader reservation: both waiting workers take their reserved connections");
	reader->remove_used_connection(concurrent_a);
	peer->remove_used_connection(concurrent_b);
	delete concurrent_a;
	delete concurrent_b;

	const unsigned long long explicit_before =
		PgHGM->status.polardb_reader_pool_reservation_retired_explicit.load(
			std::memory_order_relaxed);
	ok(reader->return_matching_connection(conn, key_a).stored() &&
			reader->take_matching_connection(key_a) == conn &&
			unit_register_reader_pool_capacity_request(reader, 7, 107, key_a),
		"PostgreSQL reader reservation: explicit removal starts with a matching capacity request");
	const ServerReturnResult explicit_return =
		reader->return_matching_connection(conn, key_a);
	ok(explicit_return.stored() &&
			explicit_return.wake.worker_index == 7 &&
			explicit_return.wake.token == 107 &&
			reader->remove_free_connection(conn),
		"PostgreSQL reader reservation: removing the reserved FREE connection records an explicit retirement");
	const ReaderTakeResult explicit_retired =
		reader->take_reader_pool_reservation(7, 107);
	ok(explicit_retired.status == ReaderTakeStatus::RETIRED &&
			explicit_retired.retire_reason ==
				ReaderReservationEndReason::EXPLICIT_REMOVE &&
			PgHGM->status.polardb_reader_pool_reservation_retired_explicit.load(
				std::memory_order_relaxed) == explicit_before + 1,
		"PostgreSQL reader reservation: worker receives the explicit-removal reason");
	delete conn;

	PgSQL_Connection* reserved_for_create =
		make_cached_reader_connection(reader);
	PgSQL_Connection* ordinary_for_create =
		make_cached_reader_connection(reader);
	const int saved_max_connections = reader->max_connections;
	const unsigned long long create_evict_before =
		PgHGM->status.polardb_reader_pool_reservation_retired_create_evict.load(
			std::memory_order_relaxed);
	ok(reader->add_matching_connection(reserved_for_create, key_a) &&
			reader->take_matching_connection(key_a) == reserved_for_create &&
			unit_register_reader_pool_capacity_request(reader, 10, 110, key_a),
		"PostgreSQL reader reservation: creation eviction starts with one USED connection and a matching capacity request");
	const ServerReturnResult create_return =
		reader->return_matching_connection(reserved_for_create, key_a);
	ok(create_return.stored() &&
			create_return.wake.worker_index == 10 &&
			create_return.wake.token == 110 &&
			reader->add_matching_connection(ordinary_for_create, key_b),
		"PostgreSQL reader reservation: reserved and normally available connections both count toward the server limit");
	reader->max_connections = 2;
	PgSQL_PoolMatchKey create_key;
	create_key.words[0] = 3;
	create_key.words[1] = 303;
	const int saved_creation_throttle =
		pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		std::numeric_limits<int>::max();
	PgSQL_PoolGetResult created = PgHGM->get_connection_from_selected_server(
		reader, reader_hg, create_key, nullptr,
		PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
			PgSQL_PoolGetMode::ALLOW_CREATE);
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		saved_creation_throttle;
	ok(created.conn && created.source == PgSQL_PoolGetSource::CREATED &&
			reader->reader_pool_reservation_count() == 1 &&
			PgHGM->status.polardb_reader_pool_reservation_retired_create_evict.load(
				std::memory_order_relaxed) == create_evict_before,
		"PostgreSQL reader reservation: connection creation removes only normally available FREE capacity");
	const ReaderTakeResult reservation_after_create =
		reader->take_reader_pool_reservation(10, 110);
	ok(reservation_after_create.status == ReaderTakeStatus::ACQUIRED &&
			reservation_after_create.conn == reserved_for_create,
		"PostgreSQL reader reservation: creation preserves the promised connection for its worker");
	reader->max_connections = saved_max_connections;
	if (created.conn) {
		reader->remove_used_connection(created.conn);
		delete created.conn;
	}
	if (reservation_after_create.conn) {
		reader->remove_used_connection(reservation_after_create.conn);
		delete reservation_after_create.conn;
	}

	const unsigned long long offline_before =
		PgHGM->status.polardb_reader_pool_reservation_retired_offline.load(
			std::memory_order_relaxed);
	PgSQL_Connection* offline_conn = make_cached_reader_connection(reader);
	ok(reader->add_matching_connection(offline_conn, key_a) &&
			reader->take_matching_connection(key_a) == offline_conn &&
			unit_register_reader_pool_capacity_request(reader, 5, 105, key_a) &&
			unit_register_reader_pool_capacity_request(reader, 6, 106, key_b) &&
			unit_register_reader_pool_capacity_request(reader, 8, 108, key_b),
		"PostgreSQL reader reservation: offline cleanup starts with one USED connection and three capacity requests");
	const ServerReturnResult offline_return =
		reader->return_matching_connection(offline_conn, key_a);
	ok(offline_return.stored() &&
			reader->reader_pool_reservation_count() == 1 &&
			reader->reader_pool_capacity_request_count() == 2,
		"PostgreSQL reader reservation: returning the connection reserves it and leaves two requests pending");
	reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	ok(reader->pool_free_count_value() == 0 &&
			reader->reader_pool_reservation_count() == 0 &&
			reader->reader_pool_capacity_request_count() == 2 &&
			peer->has_matching_reader_pool_capacity_request(key_b) &&
			PgHGM->status.polardb_reader_pool_reservation_retired_offline.load(
				std::memory_order_relaxed) == offline_before + 1,
		"PostgreSQL reader reservation: OFFLINE_HARD retires only the reservation assigned through that server");
	const ReaderTakeResult offline_reservation =
		reader->take_reader_pool_reservation(5, 105);
	const ReaderTakeResult offline_demand =
		reader->take_reader_pool_reservation(6, 106);
	ok(offline_reservation.status == ReaderTakeStatus::RETIRED &&
			offline_reservation.retire_reason ==
				ReaderReservationEndReason::SERVER_OFFLINE &&
			offline_demand.status == ReaderTakeStatus::PENDING,
		"PostgreSQL reader reservation: pending hostgroup request survives one reader going offline");

	PgSQL_Connection* peer_conn = make_cached_reader_connection(peer);
	ok(peer->add_used_matching_connection(peer_conn, key_b),
		"PostgreSQL reader reservation: eligible peer connection starts in USED after the original reader goes offline");
	const ServerReturnResult peer_return =
		peer->return_matching_connection(peer_conn, key_b);
	ok(peer_return.stored() &&
			peer_return.wake.worker_index == 6 &&
			peer_return.wake.token == 106 &&
			peer_return.wake.server == peer &&
			peer->reader_pool_reservation_count() == 1 &&
			peer->reader_pool_capacity_request_count() == 1,
		"PostgreSQL reader reservation: another eligible reader reserves its returned connection for the pending request");
	const ReaderTakeResult peer_take =
		peer->take_reader_pool_reservation(6, 106);
	ok(peer_take.status == ReaderTakeStatus::ACQUIRED &&
			peer_take.conn == peer_conn,
		"PostgreSQL reader reservation: target worker takes the cross-reader reserved connection without rerunning selection");
	peer->remove_used_connection(peer_conn);
	delete peer_conn;
	ok(reader->cancel_reader_pool_reservation(8, 108).completed() &&
			reader->take_reader_pool_reservation(8, 108).status ==
				ReaderTakeStatus::MISSING,
		"PostgreSQL reader reservation: remaining hostgroup request cancels through either reader");
	ok(reader->take_reader_pool_reservation(9, 999).status ==
			ReaderTakeStatus::MISSING,
		"PostgreSQL reader reservation: an unknown token remains distinguishable from retirement");
	reader->set_status(MYSQL_SERVER_STATUS_ONLINE);

	auto verify_non_online_cancellation =
		[&](enum MySerStatus offline_status, unsigned int worker_index,
				uint64_t token, const char* status_name) {
			PgSQL_PoolMatchKey cancellation_key = key_a;
			cancellation_key.words[3] = token;
			const unsigned int free_before =
				reader->pool_free_count_value();
			PgSQL_Connection* offline_reservation =
				make_cached_reader_connection(reader);
			ok(reader->add_used_matching_connection(
						offline_reservation, cancellation_key) &&
					unit_register_reader_pool_capacity_request(
						reader, worker_index, token, cancellation_key),
				"PostgreSQL reader reservation: %s cancellation starts with one USED connection and a matching request",
				status_name);
			const ServerReturnResult returned =
				reader->return_matching_connection(
					offline_reservation, cancellation_key);
			ok(returned.stored() &&
					returned.wake.worker_index == worker_index &&
					returned.wake.token == token &&
					reader->reader_pool_reservation_count() == 1,
				"PostgreSQL reader reservation: returning the connection creates a reservation before %s cancellation",
				status_name);
			std::atomic<bool> snapshot_released_outside_lock{false};
			offline_reservation->polardb_selected_server_snapshot =
				std::shared_ptr<const void>(
					new int(1),
					PoolUnlockCheckingDeleter{
						reader, &snapshot_released_outside_lock});
			reader->set_status(offline_status);
			const unsigned long long released_before =
				PgHGM->status.polardb_reader_pool_reservation_released.load(
					std::memory_order_relaxed);
			const auto cancel =
				reader->cancel_reader_pool_reservation(worker_index, token);
			ok(cancel.status ==
						ReaderCancelStatus::CONNECTION_RETURNED &&
					!cancel.next_wake.has_reservation() &&
					snapshot_released_outside_lock.load(
						std::memory_order_acquire) &&
					reader->reader_pool_reservation_count() == 0 &&
					reader->pool_free_count_value() == free_before + 1 &&
					reader->matching_connection_count(cancellation_key) == 1 &&
					PgHGM->status.polardb_reader_pool_reservation_released.load(
						std::memory_order_relaxed) == released_before + 1,
				"PostgreSQL reader reservation: %s cancellation restores exact-match FREE capacity and releases the routing snapshot after unlocking",
				status_name);
			ok(reader->take_matching_connection(cancellation_key) == nullptr,
				"PostgreSQL reader reservation: %s server remains ineligible while the released connection stays FREE",
				status_name);
			reader->set_status(MYSQL_SERVER_STATUS_ONLINE);
			ok(reader->take_matching_connection(cancellation_key) == offline_reservation &&
					reader->remove_used_connection(offline_reservation),
				"PostgreSQL reader reservation: %s cancellation preserves the exact connection for reuse after re-enable",
				status_name);
			delete offline_reservation;
		};
	verify_non_online_cancellation(
		MYSQL_SERVER_STATUS_SHUNNED, 30, 130, "SHUNNED");
	verify_non_online_cancellation(
		MYSQL_SERVER_STATUS_OFFLINE_SOFT, 31, 131, "OFFLINE_SOFT");

	PgSQL_Thread* wake_worker = new PgSQL_Thread();
	const bool pipe_ready = pipe(wake_worker->pipefd) == 0;
	const bool worker_attached = pipe_ready &&
		GloPTH->polardb_attach_worker(0, wake_worker) ==
			PolarDB_WorkerAttachResult::ATTACHED;
	ok(worker_attached,
		"PostgreSQL reader reservation wake: target worker attaches before notification");
	if (worker_attached) {
		const std::shared_ptr<const void> wake_snapshot =
			PgHGM->get_polardb_server_list_snapshot();
		std::unique_lock<std::mutex> wake_lock(
			PolarDB_WorkerLifecycleUnitAccess::wake_mutex(wake_worker));
		std::atomic<bool> signal_started{false};
		bool signalled = false;
		std::thread signal_thread([&]() {
			signal_started.store(true, std::memory_order_release);
			signalled =
				GloPTH->polardb_queue_reader_reservation_wake(
				0, 132, reader, wake_snapshot);
		});
		while (!signal_started.load(std::memory_order_acquire)) {
			std::this_thread::yield();
		}
		std::mutex& lifecycle_mutex =
			PolarDB_WorkerLifecycleUnitAccess::mutex(GloPTH);
		bool signal_holds_lifecycle = false;
		for (unsigned int attempt = 0; attempt < 100000; attempt++) {
			if (!lifecycle_mutex.try_lock()) {
				signal_holds_lifecycle = true;
				break;
			}
			lifecycle_mutex.unlock();
			std::this_thread::yield();
		}
		ok(signal_holds_lifecycle,
			"PostgreSQL reader reservation wake: notification pins its target lifecycle while enqueueing");
		bool detached = false;
		std::thread detach_thread;
		if (signal_holds_lifecycle) {
			detach_thread = std::thread([&]() {
				detached = GloPTH->polardb_detach_worker(0, wake_worker);
			});
		}
		wake_lock.unlock();
		signal_thread.join();
		if (detach_thread.joinable()) {
			detach_thread.join();
		}
		if (!detached) {
			detached = GloPTH->polardb_detach_worker(0, wake_worker);
		}
		unsigned char byte = 0;
		const ssize_t bytes_read = signalled
			? read(wake_worker->pipefd[0], &byte, 1) : -1;
		ok(signalled && detached && bytes_read == 1,
			"PostgreSQL reader reservation wake: in-flight notification completes before target detachment");
		ok(!wake_worker->polardb_process_reader_reservation_wakes(),
			"PostgreSQL reader reservation wake: unmatched test wake drains without becoming runnable work");
		const bool signalled_after_detach =
			GloPTH->polardb_queue_reader_reservation_wake(
				0, 133, reader, wake_snapshot);
		ok(!signalled_after_detach,
			"PostgreSQL reader reservation wake: detached worker rejects later notifications");
	}
	else if (pipe_ready) {
		close(wake_worker->pipefd[0]);
		close(wake_worker->pipefd[1]);
	}
	if (worker_attached) {
		close(wake_worker->pipefd[0]);
		close(wake_worker->pipefd[1]);
	}
	delete wake_worker;

	PgSQL_Thread coalesced_worker;
	const bool coalesced_pipe_ready = pipe(coalesced_worker.pipefd) == 0;
	const std::shared_ptr<const void> coalesced_snapshot =
		PgHGM->get_polardb_server_list_snapshot();
	const unsigned long long coalesced_global_before =
		PgHGM->status.polardb_reader_pool_reservation_wake_coalesced.load(
			std::memory_order_relaxed);
	const unsigned long long coalesced_local_before =
		coalesced_worker.polardb_status_variables.stvar[
			polardb_st_var_reader_pool_reservation_wake_coalesced];
	const bool first_notification = coalesced_pipe_ready &&
		coalesced_worker.polardb_queue_reader_reservation_wake(
			140, reader, coalesced_snapshot);
	const bool second_notification = first_notification &&
		coalesced_worker.polardb_queue_reader_reservation_wake(
			141, reader, coalesced_snapshot);
	ok(second_notification &&
			PgHGM->status.polardb_reader_pool_reservation_wake_coalesced.load(
				std::memory_order_relaxed) == coalesced_global_before + 1 &&
			coalesced_worker.polardb_status_variables.stvar[
				polardb_st_var_reader_pool_reservation_wake_coalesced] ==
				coalesced_local_before,
		"PostgreSQL reader reservation wake: cross-thread coalescing updates only the shared counter");
	if (coalesced_pipe_ready) {
		unsigned char byte = 0;
		(void)read(coalesced_worker.pipefd[0], &byte, 1);
		(void)coalesced_worker.polardb_process_reader_reservation_wakes();
		close(coalesced_worker.pipefd[0]);
		close(coalesced_worker.pipefd[1]);
	}

}


static void test_reader_capacity_group_confirmation() {
	const int writer_hg = 1110;
	const int reader_hg = 1111;
	ok(PgHGM->servers_add(make_pgsql_servers_result_two_readers(
			writer_hg, "polardb-capacity-scope-writer", 25110,
			reader_hg,
			"polardb-capacity-scope-reader-a", 25111,
			"polardb-capacity-scope-reader-b", 25112,
			/*reader_weight1=*/1, /*reader_weight2=*/1,
			/*reader_max_connections1=*/1,
			/*reader_max_connections2=*/1)) == 0,
		"PolarDB capacity scope: writer and bounded readers are staged");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB capacity scope: bounded reader topology commits");

	auto snapshot = PgHGM->get_polardb_server_list_snapshot();
	const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* entry = nullptr;
	if (snapshot) {
		auto found = snapshot->by_hostgroup.find(reader_hg);
		if (found != snapshot->by_hostgroup.end()) {
			entry = &found->second;
		}
	}
	const bool fixture_ok = entry && entry->servers.size() == 2 &&
		entry->selection_start;
	ok(fixture_ok,
		"PolarDB capacity scope: two-reader snapshot is available");
	if (!fixture_ok) {
		return;
	}
	PgSQL_SrvC* selected = entry->servers[0].srv;
	PgSQL_SrvC* peer = entry->servers[1].srv;

	PgSQL_Connection* selected_conn = make_cached_reader_connection(selected);
	selected_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(selected, selected_conn);
	const PgSQL_PoolMatchKey selected_key =
		unit_reader_pool_match_key(selected_conn);
	PgSQL_Connection* selected_used =
		selected->take_matching_connection(selected_key);

	PgSQL_Connection* peer_conn = make_cached_reader_connection(peer);
	peer_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(peer, peer_conn);
	const PgSQL_PoolMatchKey peer_key = unit_reader_pool_match_key(peer_conn);
	ok(selected_used == selected_conn &&
			selected->pool_used_count_value() == 1 &&
			peer->pool_free_count_value() == 1,
		"PolarDB capacity scope: selected reader is full while its peer is FREE");

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	entry->selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> ordinary_worker(new PgSQL_Thread());
	PgSQL_Session ordinary_sess;
	attach_test_frontend(ordinary_sess, ordinary_worker.get());
	PolarDB_ReaderResult ordinary = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &ordinary_sess, plan, no_wait, /*only_pooled=*/false);
	ok(!ordinary.acquired() &&
			ordinary.status == PolarDB_ReaderStatus::READER_BUSY &&
			ordinary.retry_scope_hash != 0 &&
			ordinary.reservation_profile_generation != 0 &&
			!ordinary.reservation_pool_key.empty() &&
			peer->pool_free_count_value() == 1,
		"PolarDB capacity scope: ordinary busy result preserves its reader choice and retry identity");

	entry->selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> admitted_worker(new PgSQL_Thread());
	PgSQL_Session admitted_sess;
	attach_test_frontend(admitted_sess, admitted_worker.get());
	PolarDB_ReaderResult admitted = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &admitted_sess, plan, no_wait, /*only_pooled=*/false,
		nullptr, -1, /*confirm_reader_group_capacity=*/true);
	ok(admitted.acquired() && admitted.srv == peer,
		"PolarDB capacity scope: admitted retry acquires from a FREE peer");
	if (admitted.conn) {
		ok(peer->return_matching_connection(
				admitted.conn, peer_key).stored(),
			"PolarDB capacity scope: acquired peer returns to core FREE accounting");
	} else {
		ok(false,
			"PolarDB capacity scope: acquired peer returns to core FREE accounting");
	}

	PgSQL_Connection* peer_used = peer->take_matching_connection(peer_key);
	entry->selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> full_worker(new PgSQL_Thread());
	PgSQL_Session full_sess;
	attach_test_frontend(full_sess, full_worker.get());
	PolarDB_ReaderResult full = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &full_sess, plan, no_wait, /*only_pooled=*/false,
		nullptr, -1, /*confirm_reader_group_capacity=*/true);
	ok(!full.acquired() &&
			full.status == PolarDB_ReaderStatus::READER_GROUP_BUSY &&
			full.retry_scope_hash == ordinary.retry_scope_hash &&
			full.reservation_profile_generation ==
				ordinary.reservation_profile_generation &&
			full.reservation_pool_key == ordinary.reservation_pool_key,
		"PolarDB capacity scope: group busy preserves the same complete retry identity");
	full_sess.current_hostgroup = reader_hg;
	full_sess.polardb_query.reader_plan = plan;
	full_sess.polardb_enter_reader_capacity_wait(
		full.retry_scope_hash, full.status, full.srv,
		std::move(full.selected_server_snapshot),
		full.reservation_profile_generation, &full.reservation_pool_key);
	full_worker->polardb_reader_adopt_retention(&full_sess);
	PgSQL_Connection* owned_connection =
		full.srv == selected ? selected_used : peer_used;
	const bool clean_wait_match =
		full_worker->polardb_reader_connection_matches_wait(
			owned_connection, &full_sess, true);
	const bool clean_retention_match =
		full_worker->polardb_reader_connection_matches_retention_requirements(
			owned_connection, true);
	owned_connection->polardb_txn_split_xids_dirty = true;
	ok(clean_wait_match && clean_retention_match &&
			!full_worker->polardb_reader_connection_matches_wait(
				owned_connection, &full_sess, true) &&
			!full_worker->polardb_reader_connection_matches_retention_requirements(
				owned_connection, true),
		"PolarDB capacity ownership: dirty transaction-split state cannot satisfy ownership or local retention");
	owned_connection->polardb_txn_split_xids_dirty = false;
	full_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);

	entry->selection_start->store(0, std::memory_order_relaxed);
	std::unique_ptr<PgSQL_Thread> other_worker(new PgSQL_Thread());
	PgSQL_Session other_sess;
	attach_test_frontend(other_sess, other_worker.get());
	other_sess.client_myds->myconn->userinfo->set(
		(char*)"polardb_capacity_other",
		(char*)"polardb_unit_pass",
		(char*)"polardb_unit_db",
		nullptr);
	PolarDB_ReaderResult other = PgHGM->polardb_acquire_reader_connection(
		reader_hg, &other_sess, plan, no_wait, /*only_pooled=*/false,
		nullptr, -1, /*confirm_reader_group_capacity=*/true);
	ok(other.status == PolarDB_ReaderStatus::READER_GROUP_BUSY &&
			other.retry_scope_hash != full.retry_scope_hash,
		"PolarDB capacity scope: different compatibility keys do not block each other");

	ok(peer_used == peer_conn &&
			peer->return_matching_connection(
				peer_used, peer_key).stored() &&
			selected->return_matching_connection(
				selected_used, selected_key).stored(),
		"PolarDB capacity scope: saturated fixtures return to FREE accounting");
	selected->remove_free_connection(selected_conn);
	peer->remove_free_connection(peer_conn);
	delete selected_conn;
	delete peer_conn;
}

static void test_writer_capacity_admission_shape() {
	const int writer_hg = 1120;
	const int reader_hg = 1121;
	ok(PgHGM->servers_add(make_pgsql_servers_result(
			writer_hg, "polardb-writer-capacity-writer", 25120,
			reader_hg, "polardb-writer-capacity-reader", 25121,
			/*writer_max_connections=*/1)) == 0,
		"PolarDB writer capacity: bounded writer and reader are staged");
	PgHGM->save_incoming_pgsql_table(
		make_polardb_replication_row(writer_hg, reader_hg),
		"pgsql_replication_hostgroups");
	ok(PgHGM->commit({}, {}, false, false),
		"PolarDB writer capacity: bounded topology commits");
	PgSQL_SrvC* writer = find_pgsql_server(
		PgHGM->MyHGC_lookup(writer_hg),
		"polardb-writer-capacity-writer", 25120);
	ok(writer != nullptr,
		"PolarDB writer capacity: writer server is available");
	if (!writer) {
		return;
	}

	PgSQL_Connection* writer_conn = make_cached_reader_connection(writer);
	writer_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(writer, writer_conn);
	const PgSQL_PoolMatchKey writer_key =
		unit_reader_pool_match_key(writer_conn);
	PgSQL_Connection* writer_used =
		writer->take_matching_connection(writer_key);

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PgSQL_Session sess;
	attach_test_frontend(sess, worker.get());
	sess.current_hostgroup = writer_hg;
	PolarDB_Query_ReaderPlan writer_plan;
	writer_plan.fallback_writer_hg = writer_hg;
	writer_plan.read_target =
		static_cast<int>(PolarDB_ReadTarget::PRIMARY);
	PolarDB_WaitSpec no_wait;
	PolarDB_ReaderResult full = PgHGM->polardb_acquire_reader_connection(
		writer_hg, &sess, writer_plan, no_wait, /*only_pooled=*/false,
		nullptr, -1, /*confirm_reader_group_capacity=*/true);
	ok(writer_used == writer_conn && !full.acquired() &&
			full.status == PolarDB_ReaderStatus::READER_GROUP_BUSY &&
			full.srv == writer && full.retry_scope_hash != 0 &&
			full.reservation_profile_generation != 0 &&
			!full.reservation_pool_key.empty(),
		"PolarDB writer capacity: saturated writer returns a complete group-busy retry identity");

	const uint64_t writer_wait_enter_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_writer_capacity_wait_enter];
	sess.polardb_enter_reader_capacity_wait(
		full.retry_scope_hash, full.status, full.srv,
		std::move(full.selected_server_snapshot),
		full.reservation_profile_generation, &full.reservation_pool_key,
		PolarDB_PoolCapacityTarget::WRITER, &writer_plan, &no_wait);
	writer_plan.fallback_writer_hg = -1;
	ok(sess.polardb_reader_capacity_wait.active &&
			sess.polardb_reader_capacity_wait.target ==
				PolarDB_PoolCapacityTarget::WRITER &&
			sess.polardb_reader_capacity_wait.reservation_plan.
				fallback_writer_hg == writer_hg &&
			!sess.polardb_reader_capacity_wait.reservation_wait_spec.has_wait(),
		"PolarDB writer capacity: wait state owns an immutable writer reservation plan");
	const uint64_t writer_wait_exit_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_writer_capacity_wait_exit];
	sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	ok(!sess.polardb_reader_capacity_wait.active &&
			sess.polardb_reader_capacity_wait.target ==
				PolarDB_PoolCapacityTarget::READER &&
			sess.polardb_reader_capacity_wait.reservation_plan.
				fallback_writer_hg < 0,
		"PolarDB writer capacity: leaving the wait clears writer-specific state");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_writer_capacity_wait_enter] ==
				writer_wait_enter_before + 1 &&
		worker->polardb_status_variables.stvar[
			polardb_st_var_writer_capacity_wait_exit] ==
				writer_wait_exit_before + 1,
		"PolarDB writer capacity: writer waits have explicit enter and exit observability");

	ok(worker->polardb_reader_local_return_decision(writer_used).action ==
			PolarDB_ReaderLocalReturn::KEEP_WITH_WORKER,
		"PolarDB writer capacity: an exact writer connection can remain worker-local");
	worker->push_MyConn_local(writer_used);
	const auto writer_cfg = PgHGM->get_polardb_hg_config(writer_hg);
	sess.polardb_query.profile_enabled = true;
	sess.polardb_query.effective_consistency_mode =
		static_cast<int>(PolarDB_ConsistencyMode::OFF);
	sess.polardb_query.request_writer_scope = PolarDB_WriterScope{
		writer_hg, writer_cfg.writer_epoch};
	PgSQL_Connection* local_writer = worker->get_MyConn_local(
		writer_hg, &sess, nullptr, 0, -1);
	ok(local_writer == writer_conn &&
			PolarDB_ReaderRetentionUnitAccess::local_connection_count(
				worker.get()) == 0,
		"PolarDB writer capacity: an active profile reuses an exact local writer even with consistency off");
	worker->push_MyConn_local(local_writer);
	sess.polardb_query.profile_enabled = false;
	ok(worker->get_MyConn_local(
			writer_hg, &sess, nullptr, 0, -1) == nullptr,
		"PolarDB writer capacity: profile-off requests cannot enter keyed writer reuse");
	worker->return_local_connections();
	ok(writer->pool_used_count_value() == 0 &&
			writer->pool_free_count_value() == 1,
		"PolarDB writer capacity: local writer fixture returns to FREE accounting");
	writer->remove_free_connection(writer_conn);
	delete writer_conn;
}

static void test_reader_capacity_pass_admission_shape() {
	const uint64_t first_scope = 0x1111;
	const uint64_t second_scope = 0x2222;
	PgSQL_Thread worker;
	worker.polardb_reader_capacity_wait_started(first_scope);
	worker.polardb_reader_capacity_wait_started(first_scope);
	ok(worker.polardb_reader_wait_scope_active(first_scope) &&
			!worker.polardb_reader_wait_scope_active(second_scope),
		"PolarDB capacity ownership: repeated waiters share one active scope");
	worker.polardb_reader_capacity_wait_scope_changed(
		first_scope, second_scope);
	ok(worker.polardb_reader_wait_scope_active(first_scope) &&
			worker.polardb_reader_wait_scope_active(second_scope),
		"PolarDB capacity ownership: rerouting moves only one waiter between scopes");
	worker.polardb_reader_capacity_wait_finished(first_scope);
	worker.polardb_reader_capacity_wait_finished(second_scope);
	ok(!worker.polardb_reader_wait_scope_active(first_scope) &&
			!worker.polardb_reader_wait_scope_active(second_scope),
		"PolarDB capacity ownership: the last waiter releases each scope");

	PgSQL_Session wait_sess;
	wait_sess.thread = &worker;
	worker.curtime = 1000000;
	wait_sess.polardb_enter_reader_capacity_wait(
		first_scope, PolarDB_ReaderStatus::READER_BUSY);
	worker.curtime += 4200;
	wait_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	ok(worker.polardb_status_variables.stvar[
			polardb_st_var_reader_capacity_wait_sum_us] == 4200 &&
			worker.polardb_status_variables.stvar[
				polardb_st_var_reader_capacity_wait_max_us] == 4200,
		"PolarDB capacity accounting: completed wait records total and maximum latency");
	ok(worker.polardb_status_variables.stvar[
			polardb_st_var_reader_capacity_wait_le_5ms] == 1,
		"PolarDB capacity accounting: completed wait records its latency bucket");

	std::vector<uint64_t> blocked;
	unsigned int attempts = 0;
	for (unsigned int n = 0; n < 1100; n++) {
		if (polardb_reader_capacity_scope_is_blocked(blocked, first_scope)) {
			continue;
		}
		attempts++;
		polardb_record_reader_group_busy_scope(
			blocked, first_scope, PolarDB_ReaderStatus::READER_GROUP_BUSY);
	}
	ok(attempts == 1 && blocked.size() == 1,
		"PolarDB capacity admission: mass expiry permits one failed group probe per scope");

	blocked.clear();
	attempts = 0;
	for (unsigned int n = 0; n < 1100; n++) {
		if (polardb_reader_capacity_scope_is_blocked(blocked, first_scope)) {
			continue;
		}
		attempts++;
		const PolarDB_ReaderStatus status = n < 10
			? PolarDB_ReaderStatus::ACQUIRED
			: PolarDB_ReaderStatus::READER_GROUP_BUSY;
		polardb_record_reader_group_busy_scope(blocked, first_scope, status);
	}
	ok(attempts == 11,
		"PolarDB capacity admission: successful acquisitions continue until the first group-full result");
	ok(polardb_reader_capacity_scope_is_blocked(blocked, first_scope) &&
			!polardb_reader_capacity_scope_is_blocked(blocked, second_scope),
		"PolarDB capacity admission: a full scope does not block an unrelated scope");
	polardb_record_reader_group_busy_scope(
		blocked, second_scope, PolarDB_ReaderStatus::READER_BUSY);
	ok(!polardb_reader_capacity_scope_is_blocked(blocked, second_scope),
		"PolarDB capacity admission: one busy selected server does not prove the reader group is full");
	ok(polardb_reader_capacity_retry_delay_us(0) == 1000,
		"PolarDB capacity admission: zero retry delay advances on a one-millisecond worker tick");
	ok(polardb_reader_capacity_retry_delay_us(10) == 10000,
		"PolarDB capacity admission: positive retry delay keeps its configured pacing");
	ok(polardb_reader_capacity_retry_timeout_us(1000, 1000, 0) == 1000 &&
			polardb_reader_capacity_retry_timeout_us(1500, 1000, 0) == 1000,
		"PolarDB capacity admission: due and sub-millisecond retries preserve the one-millisecond poll floor");
	ok(polardb_reader_capacity_retry_timeout_us(6000, 1000, 2000) == 2000,
		"PolarDB capacity admission: an earlier dynamic timeout remains authoritative");
	ok(polardb_reader_pool_capacity_request_should_register(
			PolarDB_ReaderStatus::READER_GROUP_BUSY,
			PolarDB_ReaderOwnership::NONE) &&
			!polardb_reader_pool_capacity_request_should_register(
				PolarDB_ReaderStatus::READER_GROUP_BUSY,
				PolarDB_ReaderOwnership::LOCAL) &&
			!polardb_reader_pool_capacity_request_should_register(
				PolarDB_ReaderStatus::READER_GROUP_BUSY,
				PolarDB_ReaderOwnership::ACTIVE) &&
			!polardb_reader_pool_capacity_request_should_register(
				PolarDB_ReaderStatus::READER_GROUP_BUSY,
				PolarDB_ReaderOwnership::RESERVATION) &&
			!polardb_reader_pool_capacity_request_should_register(
				PolarDB_ReaderStatus::READER_BUSY,
				PolarDB_ReaderOwnership::NONE),
		"PolarDB reader reservation: only a group-busy zero-owner worker enters the remote reservation path");
}

static void test_reader_pool_local_return_request_scope() {
	const int writer_hg = 948;
	const int reader_hg = 949;

	stage_polardb_topology(PgHGM, "PolarDB local return request",
		writer_hg, "polardb-return-key-writer", 24942,
		reader_hg, "polardb-return-key-reader", 24943);

	PgSQL_HGC* reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC* reader = find_pgsql_server(
		reader_hgc, "polardb-return-key-reader", 24943);
	ok(reader_hgc != nullptr && reader != nullptr,
		"PolarDB local return request: reader hostgroup and server are available");
	if (!reader_hgc || !reader) {
		return;
	}

	PgSQL_Thread worker;
	PgSQL_Connection* conn = make_cached_reader_connection(reader);
	conn->pgsql_conn = unit_connected_pgconn();
	const PgSQL_PoolMatchKey match_key = unit_reader_pool_match_key(conn);
	ok(reader->add_used_matching_connection(conn, match_key),
		"PolarDB local return request: core USED list owns the connection");
	const PolarDB_ReaderLocalReturnDecision unmanaged =
		worker.polardb_reader_local_return_decision(nullptr);
	ok(unmanaged.action == PolarDB_ReaderLocalReturn::USE_SHARED_POOL &&
			unmanaged.connection_status ==
				PolarDB_ReaderConnectionReturnStatus::NOT_MANAGED,
		"PolarDB local return request: unmanaged connection uses normal shared return checks");
	PgSQL_HGC* attached_hgc = reader->myhgc;
	reader->myhgc = nullptr;
	const PolarDB_ReaderLocalReturnDecision detached =
		worker.polardb_reader_local_return_decision(conn);
	reader->myhgc = attached_hgc;
	ok(detached.action == PolarDB_ReaderLocalReturn::USE_SHARED_POOL &&
			detached.connection_status ==
				PolarDB_ReaderConnectionReturnStatus::NOT_MANAGED,
		"PolarDB local return request: detached server uses normal shared return checks");
	const PolarDB_ReaderLocalReturnDecision reusable =
		worker.polardb_reader_local_return_decision(conn);
	ok(reusable.action == PolarDB_ReaderLocalReturn::KEEP_WITH_WORKER &&
			reusable.connection_status ==
				PolarDB_ReaderConnectionReturnStatus::RETURNABLE,
		"PolarDB local return request: reusable connection stays local without a remote waiter");
	reader->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	const PolarDB_ReaderLocalReturnDecision offline =
		worker.polardb_reader_local_return_decision(conn);
	ok(offline.action == PolarDB_ReaderLocalReturn::REMOVE_CONNECTION &&
			offline.connection_status ==
				PolarDB_ReaderConnectionReturnStatus::OFFLINE,
		"PolarDB local return request: offline connection must be removed");
	reader->set_status(MYSQL_SERVER_STATUS_ONLINE);

	PolarDB_Query_ReaderPlan plan;
	plan.fallback_writer_hg = writer_hg;
	PolarDB_WaitSpec no_wait;
	const unsigned int local_worker =
		std::max(1U, GloPTH ? GloPTH->num_threads : 0U);
	const unsigned int remote_worker = local_worker + 1;
	PolarDB_ReaderRetentionUnitAccess::set_worker_index(
		&worker, local_worker);
	ok(reader_hgc->register_reader_pool_capacity_request(
			local_worker, 0x9490, 0x9491, match_key, plan, no_wait),
		"PolarDB local return request: same-worker request is registered");
	ok(worker.polardb_reader_local_return_decision(conn).action ==
			PolarDB_ReaderLocalReturn::KEEP_WITH_WORKER &&
			reader_hgc->reader_pool_capacity_request_count() == 1,
		"PolarDB local return request: same-worker request keeps the connection local");
	ok(reader_hgc->register_reader_pool_capacity_request(
			remote_worker, 0x9492, 0x9493, match_key, plan, no_wait),
		"PolarDB local return request: compatible remote request joins the same-worker request");

	const PolarDB_ReaderLocalReturnDecision decision =
		worker.polardb_reader_local_return_decision(conn);
	ok(decision.action ==
			PolarDB_ReaderLocalReturn::USE_SHARED_POOL_REUSE_CHECKED &&
			decision.connection_status ==
				PolarDB_ReaderConnectionReturnStatus::RETURNABLE,
		"PolarDB local return request: remote request selects checked shared return");

	const ServerReturnResult server_return = reader->return_matching_connection(
		conn, match_key, nullptr, nullptr, local_worker);
	ok(server_return.stored() &&
			server_return.wake.worker_index == remote_worker &&
			server_return.wake.token == 0x9492 &&
			reader_hgc->reader_pool_capacity_request_count() == 1 &&
			reader->reader_pool_reservation_count() == 1 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			reader->matching_connection_count(match_key) == 0,
		"PolarDB local return request: returned connection is reserved for the remote worker, not the returning worker");
	ok(reader_hgc->cancel_reader_pool_capacity_request(local_worker, 0x9490),
		"PolarDB local return request: remaining same-worker request is removed");
	const auto cancel =
		reader->cancel_reader_pool_reservation(remote_worker, 0x9492);
	ok(cancel.status ==
				ReaderCancelStatus::CONNECTION_RETURNED &&
			!cancel.next_wake.has_reservation() &&
			reader_hgc->reader_pool_capacity_request_count() == 0 &&
			reader->reader_pool_reservation_count() == 0 &&
			reader->matching_connection_count(match_key) == 1,
		"PolarDB local return request: reservation cancellation restores ordinary exact availability");

	ok(reader->take_matching_connection(match_key) == conn &&
			reader_hgc->register_reader_pool_capacity_request(
				local_worker, 0x9494, 0x9495,
				match_key, plan, no_wait),
		"PolarDB local return request: grouped return starts with a USED connection and a request from the same worker");
	std::vector<PgSQL_Connection*> grouped_connections{conn};
	std::vector<PgSQL_Connection*> grouped_connections_to_delete;
	PgHGM->polardb_return_reader_connections(
		&worker, grouped_connections, grouped_connections_to_delete);
	ok(grouped_connections_to_delete.empty() &&
			reader_hgc->reader_pool_capacity_request_count() == 1 &&
			reader->reader_pool_reservation_count() == 0 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			reader->matching_connection_count(match_key) == 1,
		"PolarDB local return request: grouped return leaves the same-worker request pending and restores normal FREE matching");
	ok(reader_hgc->cancel_reader_pool_capacity_request(local_worker, 0x9494),
		"PolarDB local return request: grouped-return same-worker request is removed");

	ok(reader->take_matching_connection(match_key) == conn &&
			reader_hgc->register_reader_pool_capacity_request(
				remote_worker, 0x9496, 0x9497,
				match_key, plan, no_wait),
		"PolarDB local return request: checked return starts with a USED connection and a matching remote request");
	const PoolReturnResult pool_return =
		PgHGM->return_connection_with_match_key(
			conn, PgSQL_PoolReturnCheck::REUSE_LOCAL_RETURN_CHECK,
			RejectedConnectionAction::DETACH, &worker);
	ok(pool_return.status == PoolReturnStatus::STORED &&
			pool_return.detached_connection == nullptr &&
			reader_hgc->reader_pool_capacity_request_count() == 0 &&
			reader->matching_connection_count(match_key) == 1,
		"PolarDB local return request: checked shared return indexes the connection under its rebuilt pool key");
	reader->remove_free_connection(conn);
	delete conn;
}

static void test_reader_pool_retention_and_remote_reservation() {
	const int writer_hg = 950;
	const int reader_hg = 951;

	stage_polardb_topology(PgHGM, "PolarDB worker reader retention",
		writer_hg, "polardb-worker-retention-writer", 24950,
		reader_hg, "polardb-worker-retention-reader", 24951);

	PgSQL_HGC* reader_hgc = PgHGM->MyHGC_lookup(reader_hg);
	PgSQL_SrvC* reader = find_pgsql_server(
		reader_hgc, "polardb-worker-retention-reader", 24951);
	ok(reader_hgc != nullptr && reader != nullptr,
		"PolarDB worker reader retention: reader hostgroup and server are available");
	if (!reader_hgc || !reader) {
		return;
	}

	std::unique_ptr<PgSQL_Thread> worker(new PgSQL_Thread());
	PolarDB_ReaderRetentionUnitAccess::set_worker_index(worker.get(), 0);
	ok(true,
		"PolarDB worker reader retention: isolated worker index is configured");
	PgSQL_Session retention_sess;
	retention_sess.thread = worker.get();
	retention_sess.current_hostgroup = reader_hg;
	retention_sess.polardb_query.reader_plan.fallback_writer_hg = writer_hg;
	retention_sess.polardb_query.wait.spec = PolarDB_WaitSpec{};
	const int original_retention =
		pgsql_thread___polardb_reader_connection_retention;
	pgsql_thread___polardb_reader_connection_retention = 0;
	const unsigned int unavailable_worker =
		std::max(1U, GloPTH ? GloPTH->num_threads : 0U);

	auto install_retention = [&](PgSQL_Connection* conn, uint64_t scope_hash) {
		std::shared_ptr<const void> server_snapshot =
			PgHGM->get_polardb_server_list_snapshot();
		retention_sess.polardb_enter_reader_capacity_wait(
			scope_hash, PolarDB_ReaderStatus::READER_GROUP_BUSY,
			reader, std::move(server_snapshot),
			conn->polardb_startup_profile_generation,
			&conn->polardb_pool_key);
		worker->polardb_reader_adopt_retention(&retention_sess);
	};

	PgSQL_Connection* self_conn = make_cached_reader_connection(reader);
	self_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, self_conn);
	const PgSQL_PoolMatchKey self_key =
		unit_reader_pool_match_key(self_conn);
	ok(reader->take_matching_connection(self_key) == self_conn &&
			unit_register_reader_pool_capacity_request(
				reader, 0, 120, self_key),
		"PolarDB worker reader retention: worker starts with one USED connection and a matching request of its own");
	install_retention(self_conn, 0x9501);
	const uint64_t self_request_seen_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen];
	const uint64_t self_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_attempt];
	const uint64_t self_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_created];
	const uint64_t self_not_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_not_created];
#if POLARDB_PROFILE
	const uint64_t self_store_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_attempt];
	const uint64_t self_store_accepted_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_accepted];
#endif // POLARDB_PROFILE
	worker->push_MyConn_local(self_conn);
	ok(reader->reader_pool_capacity_request_count() == 1 &&
			reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker reader retention: self-only request remains pending and the connection remains local USED");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen] ==
				self_request_seen_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_attempt] ==
				self_attempt_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_created] ==
				self_created_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_not_created] ==
				self_not_created_before,
		"PolarDB worker reader retention: self-only request does not attempt a remote reservation");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_attempt] ==
				self_store_attempt_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_local_store_accepted] ==
				self_store_accepted_before + 1,
		"PolarDB worker reader retention: self-only retention counts one local store");
#endif // POLARDB_PROFILE
	ok(reader->cancel_reader_pool_reservation(0, 120).completed(),
		"PolarDB worker reader retention: self-only request cancels cleanly");
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker reader retention: retained self-request connection returns to shared FREE");
	reader->remove_free_connection(self_conn);
	delete self_conn;

	PgSQL_Connection* unrelated_conn = make_cached_reader_connection(reader);
	unrelated_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, unrelated_conn);
	const PgSQL_PoolMatchKey unrelated_conn_key =
		unit_reader_pool_match_key(unrelated_conn);
	PgSQL_PoolMatchKey unrelated_request_key = unrelated_conn_key;
	unrelated_request_key.words[1] ^= 1;
	ok(reader->take_matching_connection(unrelated_conn_key) ==
				unrelated_conn &&
			unit_register_reader_pool_capacity_request(
				reader, unavailable_worker, 126,
				unrelated_request_key),
		"PolarDB worker reader retention: worker starts with one USED connection and an unrelated remote request");
	install_retention(unrelated_conn, 0x9506);
	const uint64_t unrelated_request_seen_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen];
	const uint64_t unrelated_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_attempt];
	const uint64_t unrelated_not_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_not_created];
	worker->push_MyConn_local(unrelated_conn);
	ok(reader->reader_pool_capacity_request_count() == 1 &&
			reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker reader retention: unrelated request leaves the connection local");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen] ==
				unrelated_request_seen_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_attempt] ==
				unrelated_attempt_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_not_created] ==
				unrelated_not_created_before,
		"PolarDB worker reader retention: unrelated request does not attempt a remote reservation");
	ok(reader->cancel_reader_pool_reservation(
			unavailable_worker, 126).completed(),
		"PolarDB worker reader retention: unrelated request cancels cleanly");
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	worker->return_local_connections();
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker reader retention: connection returns to shared FREE after the unrelated request");
	reader->remove_free_connection(unrelated_conn);
	delete unrelated_conn;

	PgSQL_Connection* rekeyed_conn = make_cached_reader_connection(reader);
	rekeyed_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, rekeyed_conn);
	const PgSQL_PoolMatchKey original_key =
		unit_reader_pool_match_key(rekeyed_conn);
	ok(reader->take_matching_connection(original_key) == rekeyed_conn &&
			unit_register_reader_pool_capacity_request(
				reader, unavailable_worker, 123, original_key),
		"PolarDB worker reader retention: worker starts with one USED connection and a request matching its original pool key");
	install_retention(rekeyed_conn, 0x9502);
	rekeyed_conn->var_hash[PGSQL_DATESTYLE] ^= 1U;
	unit_reader_pool_refresh_key(rekeyed_conn);
	const PgSQL_PoolMatchKey current_key =
		unit_reader_pool_match_key(rekeyed_conn);
	ok(!(current_key == original_key),
		"PolarDB worker reader retention: backend state change produces a new reusable key");
	worker->push_MyConn_local(rekeyed_conn);
	ok(reader->reader_pool_capacity_request_count() == 1 &&
			reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker reader retention: a retention-key mismatch keeps the reusable connection local");
	worker->return_local_connections();
	ok(reader->reader_pool_capacity_request_count() == 1 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1 &&
			reader->matching_connection_count(current_key) == 1,
		"PolarDB worker reader retention: grouped return indexes the connection under its updated pool key");
	ok(reader->cancel_reader_pool_reservation(
			unavailable_worker, 123).completed(),
		"PolarDB worker reader retention: unmatched original-key request cancels cleanly");
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	reader->remove_free_connection(rekeyed_conn);
	delete rekeyed_conn;

	PgSQL_Connection* oversized_conn = make_cached_reader_connection(reader);
	oversized_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, oversized_conn);
	const PgSQL_PoolMatchKey oversized_key =
		unit_reader_pool_match_key(oversized_conn);
	ok(reader->take_matching_connection(oversized_key) == oversized_conn,
		"PolarDB worker reader retention: oversized connection starts in USED");
	install_retention(oversized_conn, 0x9505);
	worker->push_MyConn_local(oversized_conn);
	ok(unit_register_reader_pool_capacity_request(
			reader, unavailable_worker, 124, oversized_key),
		"PolarDB worker reader retention: oversized connection has a matching remote request");
	oversized_conn->largest_query_length =
		static_cast<unsigned int>(
			GloPTH->variables.threshold_query_length) + 1U;
	pgsql_thread___polardb_reader_connection_retention = 1;
	worker->return_local_connections();
	pgsql_thread___polardb_reader_connection_retention = 0;
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0 &&
			PolarDB_ReaderRetentionUnitAccess::local_connection_count(
				worker.get()) == 0,
		"PolarDB worker reader retention: oversized connection is removed instead of retained or reserved");
	ok(reader->cancel_reader_pool_reservation(
			unavailable_worker, 124).completed(),
		"PolarDB worker reader retention: oversized connection request cancels cleanly");
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);

	PgSQL_Connection* invalid_conn = make_cached_reader_connection(reader);
	invalid_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, invalid_conn);
	const PgSQL_PoolMatchKey invalid_key =
		unit_reader_pool_match_key(invalid_conn);
	ok(reader->take_matching_connection(invalid_key) == invalid_conn &&
			unit_register_reader_pool_capacity_request(
				reader, unavailable_worker, 125, invalid_key),
		"PolarDB worker reader retention: non-reusable completion starts in USED with a matching request");
	install_retention(invalid_conn, 0x9502);
	invalid_conn->largest_query_length =
		static_cast<unsigned int>(
			GloPTH->variables.threshold_query_length) + 1U;
#if POLARDB_PROFILE
	const uint64_t invalid_rejected_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_rejected];
#endif // POLARDB_PROFILE
	worker->push_MyConn_local(invalid_conn);
	ok(reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 0 &&
			reader->reader_pool_capacity_request_count() == 1,
		"PolarDB worker reader retention: non-reusable completion is destroyed instead of retained");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_store_rejected] ==
				invalid_rejected_before + 1,
		"PolarDB worker reader retention: non-reusable completion records one rejected local store");
#endif // POLARDB_PROFILE
	const uint64_t invalid_clear_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared];
	ok(reader->cancel_reader_pool_reservation(
			unavailable_worker, 125).completed(),
		"PolarDB worker reader retention: non-reusable completion request cancels cleanly");
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared] ==
				invalid_clear_before + 1,
		"PolarDB worker reader retention: inactive wait scope clears reader retention");

	PgSQL_Connection* remote_conn = make_cached_reader_connection(reader);
	remote_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, remote_conn);
	const PgSQL_PoolMatchKey remote_key =
		unit_reader_pool_match_key(remote_conn);
	ok(reader->take_matching_connection(remote_key) == remote_conn &&
			unit_register_reader_pool_capacity_request(
				reader, unavailable_worker, 121, remote_key),
		"PolarDB worker reader retention: worker starts with one USED connection and a matching remote request");
	install_retention(remote_conn, 0x9503);
	const uint64_t remote_request_seen_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen];
	const uint64_t remote_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_attempt];
	const uint64_t remote_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_created];
	const uint64_t remote_not_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_not_created];
	worker->push_MyConn_local(remote_conn);
	ok(reader->reader_pool_capacity_request_count() == 0 &&
			reader->reader_pool_reservation_count() == 0 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker reader retention: failed worker wake returns the reserved connection to shared FREE");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen] ==
				remote_request_seen_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_attempt] ==
				remote_attempt_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_created] ==
				remote_created_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_not_created] ==
				remote_not_created_before,
		"PolarDB worker reader retention: remote reservation increments each success counter once");
	const uint64_t remote_clear_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared];
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared] ==
				remote_clear_before + 1,
		"PolarDB worker reader retention: completed remote reservation clears inactive retention");
	reader->remove_free_connection(remote_conn);
	delete remote_conn;

	PgSQL_Connection* grouped_conn = make_cached_reader_connection(reader);
	grouped_conn->pgsql_conn = unit_connected_pgconn();
	unit_reader_pool_add_matching(reader, grouped_conn);
	const PgSQL_PoolMatchKey grouped_key =
		unit_reader_pool_match_key(grouped_conn);
	ok(reader->take_matching_connection(grouped_key) == grouped_conn,
		"PolarDB worker reader retention: grouped return starts with one USED connection");
	install_retention(grouped_conn, 0x9504);
	worker->push_MyConn_local(grouped_conn);
	ok(reader->reader_pool_capacity_request_count() == 0 &&
			reader->pool_used_count_value() == 1 &&
			reader->pool_free_count_value() == 0,
		"PolarDB worker reader retention: connection remains local until a remote request appears");
	ok(unit_register_reader_pool_capacity_request(
			reader, unavailable_worker, 122, grouped_key),
		"PolarDB worker reader retention: remote request is registered after local retention");
	const uint64_t grouped_request_seen_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen];
	const uint64_t grouped_attempt_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_attempt];
	const uint64_t grouped_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_created];
	const uint64_t grouped_not_created_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_reservation_not_created];
	const uint64_t grouped_shared_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retained_connection_shared];
#if POLARDB_PROFILE
	const uint64_t grouped_return_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_return_to_shared];
#endif // POLARDB_PROFILE
	worker->return_local_connections();
	ok(reader->reader_pool_capacity_request_count() == 0 &&
			reader->reader_pool_reservation_count() == 0 &&
			reader->pool_used_count_value() == 0 &&
			reader->pool_free_count_value() == 1,
		"PolarDB worker reader retention: failed worker wake returns the grouped connection to shared FREE");
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_remote_request_seen] ==
				grouped_request_seen_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_attempt] ==
				grouped_attempt_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_created] ==
				grouped_created_before + 1 &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_remote_reservation_not_created] ==
				grouped_not_created_before &&
			worker->polardb_status_variables.stvar[
				polardb_st_var_reader_pool_retained_connection_shared] ==
				grouped_shared_before + 1,
		"PolarDB worker reader retention: grouped return increments each successful reservation counter once");
#if POLARDB_PROFILE
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_local_return_to_shared] ==
				grouped_return_before + 1,
		"PolarDB worker reader retention: remote reservation counts one local return to shared");
#endif // POLARDB_PROFILE
	const uint64_t grouped_clear_before =
		worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared];
	retention_sess.polardb_leave_reader_capacity_wait(
		PolarDB_ReaderStatus::ACQUIRED);
	ok(worker->polardb_status_variables.stvar[
			polardb_st_var_reader_pool_retention_cleared] ==
				grouped_clear_before + 1,
		"PolarDB worker reader retention: grouped return clears inactive retention");
	reader->remove_free_connection(grouped_conn);
	delete grouped_conn;

	pgsql_thread___polardb_reader_connection_retention = original_retention;
}

void run_polardb_reader_reservation_lifecycle_tests() {
	test_reader_pool_reservation_lifecycle();
}

void run_polardb_reader_capacity_admission_tests() {
	test_reader_capacity_group_confirmation();
	test_writer_capacity_admission_shape();
	test_reader_capacity_pass_admission_shape();
}

void run_polardb_reader_capacity_retention_tests() {
	test_reader_pool_local_return_request_scope();
	test_reader_pool_retention_and_remote_reservation();
}

#endif // POLARDB_PROXY
