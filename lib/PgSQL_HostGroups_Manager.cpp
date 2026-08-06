#include "../deps/json/json.hpp"
using json = nlohmann::json;
#define PROXYJSON

#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_PolarDB_HGM_Internal.h"
#include "PgSQL_PolarDB_ReaderPool_Internal.h"
#include "ConnectionPoolDecision.h"
#include "proxysql.h"
#include "cpp.h"

#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_Session.h"
#include "PgSQL_Thread.h"

#include <memory>
#include <algorithm>
#include <chrono>
#include <pthread.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <poll.h>
#include <string>
#include <thread>
#include <vector>

#include "prometheus/counter.h"
#include "prometheus/detail/builder.h"
#include "prometheus/family.h"
#include "prometheus/gauge.h"

#include "prometheus_helpers.h"
#include "proxysql_utils.h"

#define char_malloc (char *)malloc

#if POLARDB_PROXY
#endif // POLARDB_PROXY

#include "thread.h"
#include "wqueue.h"

#include "ev.h"

#include <functional>
#include <mutex>
#include <type_traits>

using std::function;

#if POLARDB_PROXY && POLARDB_PROFILE
#define POLARDB_PROFILE_STATUS_COUNT(name, value) \
	POLARDB_HGM_PROFILE_STATUS_COUNT(status, name, value)
#define POLARDB_PROFILE_STATUS_COUNT_ONE(name) \
	POLARDB_PROFILE_STATUS_COUNT(name, 1)
#else
#define POLARDB_PROFILE_STATUS_COUNT(name, value) do { } while (0)
#define POLARDB_PROFILE_STATUS_COUNT_ONE(name) do { } while (0)
#endif // POLARDB_PROXY && POLARDB_PROFILE

#if POLARDB_PROXY
#define POLARDB_STATUS_COUNT(name, value) \
	POLARDB_HGM_STATUS_COUNT(status, name, value)
#define POLARDB_STATUS_COUNT_ONE(name) \
	POLARDB_STATUS_COUNT(name, 1)
#endif // POLARDB_PROXY

#ifdef TEST_AURORA
static unsigned long long array_mysrvc_total = 0;
static unsigned long long array_mysrvc_cands = 0;
#endif // TEST_AURORA

#define SAFE_SQLITE3_STEP(_stmt) do {\
  do {\
    rc=(*proxy_sqlite3_step)(_stmt);\
    if (rc!=SQLITE_DONE) {\
      assert(rc==SQLITE_LOCKED);\
      usleep(100);\
    }\
  } while (rc!=SQLITE_DONE);\
} while (0)

extern ProxySQL_Admin *GloAdmin;
extern PgSQL_Threads_Handler *GloPTH;
extern MySQL_Monitor *GloMyMon;

class PgSQL_SrvConnList;
class PgSQL_SrvC;
class PgSQL_SrvList;
class PgSQL_HGC;

const int PgSQL_ERRORS_STATS_FIELD_NUM = 11;

#if POLARDB_PROXY
static bool polardb_atomic_max_u64(std::atomic<uint64_t>& target, uint64_t value) {
	uint64_t cur = target.load(std::memory_order_relaxed);
	while (cur < value) {
		if (target.compare_exchange_weak(cur, value,
				std::memory_order_relaxed, std::memory_order_relaxed)) {
			return true;
		}
	}
	return false;
}

static bool polardb_atomic_max_u64(
		const std::shared_ptr<std::atomic<uint64_t>>& target, uint64_t value) {
	return target ? polardb_atomic_max_u64(*target, value) : false;
}

#endif // POLARDB_PROXY

bool pgsql_connection_creation_throttled_unlocked(PgSQL_SrvC* mysrvc) {
	unsigned long long curtime = monotonic_time();
	curtime = curtime / 1000 / 1000; // convert to second
	PgSQL_HGC* myhgc = mysrvc->myhgc;
	if (curtime > myhgc->current_time_now) {
		myhgc->current_time_now = curtime;
		myhgc->new_connections_now = 0;
	}
	myhgc->new_connections_now++;
	unsigned int throttle_connections_per_sec_to_hostgroup =
		(unsigned int)pgsql_thread___throttle_connections_per_sec_to_hostgroup;
	if (myhgc->attributes.configured == true) {
		// pgsql_hostgroup_attributes takes priority
		throttle_connections_per_sec_to_hostgroup = myhgc->attributes.throttle_connections_per_sec;
	}
	if (should_throttle_connection_creation(
			myhgc->new_connections_now, throttle_connections_per_sec_to_hostgroup)) {
		__sync_fetch_and_add(&PgHGM->status.server_connections_delayed, 1);
		return true;
	}
	return false;
}

PgSQL_Connection* pgsql_create_backend_connection_unlocked(PgSQL_SrvC* mysrvc) {
	PgSQL_Connection* conn = new PgSQL_Connection(false);
	conn->parent = mysrvc;
	// if attributes.multiplex == true , STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG is set to false. And vice-versa
	conn->set_status(!conn->parent->myhgc->attributes.multiplex,
		STATUS_PGSQL_CONNECTION_NO_MULTIPLEX_HG);
	__sync_fetch_and_add(&PgHGM->status.server_connections_created, 1);
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7,
		"Returning PostgreSQL Connection %p, server %s:%d\n",
		conn, conn->parent->address, conn->parent->port);
	return conn;
}

unsigned int pgsql_srv_latency_limit_us(unsigned int configured_max_latency_us) {
	return configured_max_latency_us
		? configured_max_latency_us
		: pgsql_thread___default_max_latency_ms * 1000;
}

unsigned int pgsql_srv_latency_limit_us(const PgSQL_SrvC* mysrvc) {
	return pgsql_srv_latency_limit_us(mysrvc->max_latency_us);
}

bool pgsql_srv_latency_allowed(
		unsigned int current_latency_us,
		unsigned int configured_max_latency_us) {
	unsigned int max_latency_us =
		pgsql_srv_latency_limit_us(configured_max_latency_us);
	// ProxySQL uses 0 for max_latency_ms/default_max_latency_ms as no limit.
	if (max_latency_us == 0) {
		return true;
	}
	return current_latency_us < max_latency_us;
}

bool pgsql_srv_latency_allowed(const PgSQL_SrvC* mysrvc) {
	return pgsql_srv_latency_allowed(
		mysrvc->current_latency_us_value(), mysrvc->max_latency_us);
}

/**
 * @brief Helper function used to try to extract a value from the JSON field 'servers_defaults'.
 *
 * @param j JSON object constructed from 'servers_defaults' field.
 * @param hid Hostgroup for which the 'servers_defaults' is defined in 'pgsql_hostgroup_attributes'. Used for
 *  error logging.
 * @param key The key for the value to be extracted.
 * @param val_check A validation function, checks if the value is within a expected range.
 *
 * @return The value extracted from the supplied JSON. In case of error '-1', and error cause is logged.
 */
template <typename T, typename std::enable_if<std::is_integral<T>::value, bool>::type = true>
T PgSQL_j_get_srv_default_int_val(
	const json& j, uint32_t hid, const string& key, const function<bool(T)>& val_check
) {
	if (j.find(key) != j.end()) {
		const json::value_t val_type = j[key].type();
		const char* type_name = j[key].type_name();

		if (val_type == json::value_t::number_integer || val_type == json::value_t::number_unsigned) {
			T val = j[key].get<T>();

			if (val_check(val)) {
				return val;
			} else {
				proxy_error(
					"Invalid value %ld supplied for 'pgsql_hostgroup_attributes.servers_defaults.%s' for hostgroup %d."
						" Value NOT UPDATED.\n",
					static_cast<int64_t>(val), key.c_str(), hid
				);
			}
		} else {
			proxy_error(
				"Invalid type '%s'(%hhu) supplied for 'pgsql_hostgroup_attributes.servers_defaults.%s' for hostgroup %d."
					" Value NOT UPDATED.\n",
				type_name, static_cast<std::uint8_t>(val_type), key.c_str(), hid
			);
		}
	}

	return static_cast<T>(-1);
}

PgSQL_Errors_stats::PgSQL_Errors_stats(int _hostgroup, const char* _hostname, int _port, const char* _username, const char* _address, const char* _dbname,
	const char* _sqlstate, const char* _errmsg, time_t tn) {
	hostgroup = _hostgroup;
	if (_hostname) {
		hostname = strdup(_hostname);
	} else {
		hostname = strdup((char*)"");
	}
	port = _port;
	if (_username) {
		username = strdup(_username);
	} else {
		username = strdup((char*)"");
	}
	if (_address) {
		client_address = strdup(_address);
	} else {
		client_address = strdup((char*)"");
	}
	if (_dbname) {
		dbname = strdup(_dbname);
	} else {
		dbname = strdup((char*)"");
	}
	if (_sqlstate) {
		strncpy(sqlstate, _sqlstate, 5);
		sqlstate[5] = '\0';
	} else {
		sqlstate[0] = '\0';
	}
	if (_errmsg) {
		errmsg = strdup(_errmsg);
	} else {
		errmsg = strdup((char*)"");
	}
	last_seen = tn;
	first_seen = tn;
	count_star = 1;
}

PgSQL_Errors_stats::~PgSQL_Errors_stats() {
	if (hostname) {
		free(hostname);
		hostname = NULL;
	}
	if (username) {
		free(username);
		username = NULL;
	}
	if (client_address) {
		free(client_address);
		client_address = NULL;
	}
	if (dbname) {
		free(dbname);
		dbname = NULL;
	}
	if (errmsg) {
		free(errmsg);
		errmsg = NULL;
	}
}

char** PgSQL_Errors_stats::get_row() {
	char buf[128];
	char** pta = (char**)malloc(sizeof(char*) * PgSQL_ERRORS_STATS_FIELD_NUM);
	sprintf(buf, "%d", hostgroup);
	pta[0] = strdup(buf);
	assert(hostname);
	pta[1] = strdup(hostname);
	sprintf(buf, "%d", port);
	pta[2] = strdup(buf);
	assert(username);
	pta[3] = strdup(username);
	assert(client_address);
	pta[4] = strdup(client_address);
	assert(dbname);
	pta[5] = strdup(dbname);
	pta[6] = strdup(sqlstate);
	sprintf(buf, "%llu", count_star);
	pta[7] = strdup(buf);
	sprintf(buf, "%ld", first_seen);
	pta[8] = strdup(buf);
	sprintf(buf, "%ld", last_seen);
	pta[9] = strdup(buf);
	assert(errmsg);
	pta[10] = strdup(errmsg);
	return pta;
}

void PgSQL_Errors_stats::add_time(unsigned long long n, const char* le) {
	count_star++;
	if (first_seen == 0) {
		first_seen = n;
	}
	last_seen = n;
	if (strcmp(errmsg, le)) {
		free(errmsg);
		errmsg = strdup(le);
	}
}

void PgSQL_Errors_stats::free_row(char** pta) {
	int i;
	for (i = 0; i < PgSQL_ERRORS_STATS_FIELD_NUM; i++) {
		assert(pta[i]);
		free(pta[i]);
	}
	free(pta);
}

PgSQL_Connection *PgSQL_SrvConnList::index(unsigned int _k) {
#if POLARDB_PROXY
	std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
	return _k < conns->len
		? static_cast<PgSQL_Connection*>(conns->index(_k))
		: nullptr;
}

#if POLARDB_PROXY
static bool polardb_make_room_for_classic_connection_unlocked(
		PgSQL_SrvC* mysrvc, unsigned int preferred_classic_evict,
		std::vector<PgSQL_Connection*>& connections_to_delete) {
	if (!mysrvc || mysrvc->max_connections <= 0 || !mysrvc->ConnectionsFree) {
		return false;
	}
	const unsigned int max_connections =
		static_cast<unsigned int>(mysrvc->max_connections);
	PolarDB_PoolConnStats capacity = mysrvc->polardb_pool_conn_stats();
	if (capacity.active() >= max_connections) {
		return false;
	}

	unsigned int required_free = 0;
	if (capacity.total() >= max_connections) {
		required_free = capacity.total() - max_connections + 1;
	}
	unsigned int classic_to_evict = preferred_classic_evict;
	if (classic_to_evict < required_free) {
		classic_to_evict = required_free;
	}
	if (!mysrvc->evict_unreserved_free_for_create(
			classic_to_evict, required_free, connections_to_delete)) {
		return false;
	}

	capacity = mysrvc->polardb_pool_conn_stats();

	return capacity.active() < max_connections &&
		capacity.total() < max_connections;
}
#endif // POLARDB_PROXY

int PgSQL_SrvConnList::find_idx(PgSQL_Connection* conn) const {
	if (!conn) {
		return -1;
	}
#if POLARDB_PROXY
	const unsigned int position = conn->polardb_core_pool_position;
	if (position < conns->len && conns->index(position) == conn) {
		return static_cast<int>(position);
	}
#endif // POLARDB_PROXY
	for (unsigned int index = 0; index < conns->len; index++) {
		if (conns->index(index) == conn) {
#if POLARDB_PROXY
			conn->polardb_core_pool_position = index;
#endif // POLARDB_PROXY
			return static_cast<int>(index);
		}
	}
	// FREE and USED share this hint. A miss in one list must not discard a
	// valid position in the other; pointer validation keeps stale hints safe.
	return -1;
}

#if POLARDB_PROXY
/**
 * @brief Rebuild the FREE-list positional key mirror from its exact-key
 *        buckets.
 *
 * The caller must hold mysrvc->pool_mutex, since this rewrites conns,
 * match_keys_by_position and matching_by_key. Valid on the FREE list only.
 *
 * This runs only after an index inconsistency. `matching_by_key` remains the
 * authority because opaque keys cannot always be reconstructed from a
 * connection. Reserved, stale, and multiply indexed connections are left
 * keyless, so exact lookup skips them instead of returning a connection for
 * the wrong key.
 *
 * Every connection's polardb_core_pool_position is reassigned, so any index the
 * caller obtained from find_idx() before this call is stale and must be
 * resolved again afterwards.
 */
void PgSQL_SrvConnList::rebuild_free_match_index_unlocked() {
	assert(!used_list);
	if (empty_match_bucket_count != 0) {
		POLARDB_HGM_PROFILE_STATUS_COUNT(
			PgHGM->status, reader_pool_exact_bucket_pruned,
			empty_match_bucket_count);
		empty_match_bucket_count = 0;
	}
	std::unordered_map<PgSQL_Connection*, unsigned int> positions;
	positions.reserve(conns->len);
	for (unsigned int index = 0; index < conns->len; ++index) {
		PgSQL_Connection* conn =
			static_cast<PgSQL_Connection*>(conns->index(index));
		if (conn) {
			conn->polardb_core_pool_position = index;
			positions.emplace(conn, index);
		}
	}

	std::vector<PgSQL_PoolMatchKey> repaired_keys(conns->len);
	std::vector<unsigned int> key_counts(conns->len, 0);
	for (const auto& indexed : matching_by_key) {
		if (indexed.first.empty()) {
			continue;
		}
		for (PgSQL_Connection* conn : indexed.second) {
			const auto position = positions.find(conn);
			if (position == positions.end() ||
					mysrvc->polardb_connection_reserved_unlocked(conn)) {
				continue;
			}
			const unsigned int index = position->second;
			if (key_counts[index]++ == 0) {
				repaired_keys[index] = indexed.first;
			}
		}
	}

	decltype(matching_by_key) repaired_index;
	for (unsigned int index = 0; index < conns->len; ++index) {
		if (key_counts[index] == 1) {
			repaired_index[repaired_keys[index]].push_back(
				static_cast<PgSQL_Connection*>(conns->index(index)));
		} else {
			repaired_keys[index] = PgSQL_PoolMatchKey{};
		}
	}
	match_keys_by_position.swap(repaired_keys);
	matching_by_key.swap(repaired_index);
}

void PgSQL_SrvConnList::limit_empty_match_buckets_unlocked(
		decltype(matching_by_key)::iterator bucket) {
	assert(!used_list);
	assert(bucket != matching_by_key.end());
	assert(bucket->second.empty());
	POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
		PgHGM->status, reader_pool_exact_bucket_emptied);
	const size_t empty_bucket_limit =
		mysrvc && mysrvc->max_connections > 0
			? static_cast<size_t>(mysrvc->max_connections) : 0;
	if (empty_match_bucket_count < empty_bucket_limit) {
		++empty_match_bucket_count;
		return;
	}
	matching_by_key.erase(bucket);
	POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
		PgHGM->status, reader_pool_exact_bucket_pruned);
}

void PgSQL_SrvConnList::prune_empty_match_buckets_unlocked() {
	if (used_list || empty_match_bucket_count == 0) {
		return;
	}
	size_t pruned = 0;
	for (auto bucket = matching_by_key.begin();
			bucket != matching_by_key.end();) {
		if (!bucket->second.empty()) {
			++bucket;
			continue;
		}
		bucket = matching_by_key.erase(bucket);
		++pruned;
	}
	assert(pruned == empty_match_bucket_count);
	empty_match_bucket_count = 0;
	POLARDB_HGM_PROFILE_STATUS_COUNT(
		PgHGM->status, reader_pool_exact_bucket_pruned, pruned);
}

void PgSQL_SrvConnList::ensure_match_index_consistent_unlocked() {
	if (match_keys_by_position.size() == conns->len) {
		return;
	}
	static std::atomic<bool> logged{false};
	if (!logged.exchange(true, std::memory_order_relaxed)) {
		proxy_error(
			"PostgreSQL core pool key index mismatch on %s:%u list=%s keys=%zu connections=%u; rebuilding\n",
			mysrvc->address ? mysrvc->address : "(null)", mysrvc->port,
			used_list ? "USED" : "FREE", match_keys_by_position.size(),
			conns->len);
	}
	if (!used_list) {
		rebuild_free_match_index_unlocked();
		assert(match_keys_by_position.size() == conns->len);
		return;
	}
	// USED has no exact-key reverse index. Preserve its known positional prefix
	// and leave any missing positions keyless.
	matching_by_key.clear();
	match_keys_by_position.resize(conns->len);
	for (unsigned int index = 0; index < conns->len; ++index) {
		PgSQL_Connection* conn =
			static_cast<PgSQL_Connection*>(conns->index(index));
		if (conn) {
			conn->polardb_core_pool_position = index;
		}
		const PgSQL_PoolMatchKey& key = match_keys_by_position[index];
		if (!used_list && conn && !key.empty() &&
				!mysrvc->polardb_connection_reserved_unlocked(conn)) {
			matching_by_key[key].push_back(conn);
		}
	}
	assert(match_keys_by_position.size() == conns->len);
}

void PgSQL_SrvConnList::unindex_unlocked(PgSQL_Connection* conn) {
	ensure_match_index_consistent_unlocked();
	const int position = find_idx(conn);
	if (position < 0 ||
			static_cast<size_t>(position) >= match_keys_by_position.size()) {
		return;
	}
	const PgSQL_PoolMatchKey& key =
		match_keys_by_position[static_cast<size_t>(position)];
	if (key.empty()) {
		return;
	}
	auto indexed = matching_by_key.find(key);
	if (indexed != matching_by_key.end()) {
		auto& entries = indexed->second;
		bool removed = false;
		for (size_t i = 0; i < entries.size(); i++) {
			if (entries[i] == conn) {
				entries[i] = entries.back();
				entries.pop_back();
				removed = true;
				break;
			}
		}
		if (removed && entries.empty()) {
			limit_empty_match_buckets_unlocked(indexed);
		}
	}
}
#endif // POLARDB_PROXY

void PgSQL_SrvConnList::add_unlocked(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey* key) {
#if POLARDB_PROXY
	ensure_match_index_consistent_unlocked();
	if (conn->polardb_idle_trim_pending) {
		mysrvc->polardb_finish_idle_trim_unlocked(
			conn, PolarDB_IdleTrimEndReason::OTHER_REMOVAL);
	}
	conn->polardb_core_pool_position = conns->len;
#endif // POLARDB_PROXY
	conns->add(conn);
#if POLARDB_PROXY
	match_keys_by_position.push_back(
		key ? *key : PgSQL_PoolMatchKey{});
	if (key && !key->empty()) {
		conn->polardb_exact_pool_connection = true;
	}
	std::atomic<unsigned int>& count = used_list
		? mysrvc->pool_used_count : mysrvc->pool_free_count;
	count.store(conns->len, std::memory_order_relaxed);
	if (key && !key->empty()) {
		if (!used_list) {
			auto inserted = matching_by_key.try_emplace(*key);
			if (inserted.second) {
				POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
					PgHGM->status, reader_pool_exact_bucket_created);
			} else if (inserted.first->second.empty()) {
				assert(empty_match_bucket_count > 0);
				--empty_match_bucket_count;
				POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
					PgHGM->status, reader_pool_exact_bucket_reused);
			}
			inserted.first->second.push_back(conn);
		}
	}
#endif // POLARDB_PROXY
}

#if POLARDB_PROXY
bool PgSQL_SrvConnList::find_match_key_unlocked(
		PgSQL_Connection* conn, PgSQL_PoolMatchKey* key) {
	if (!conn || !key) {
		return false;
	}
	assert(match_keys_by_position.size() == conns->len);
	const int position = find_idx(conn);
	if (position < 0 ||
			static_cast<size_t>(position) >= match_keys_by_position.size()) {
		return false;
	}
	*key = match_keys_by_position[static_cast<size_t>(position)];
	return !key->empty();
}
#endif // POLARDB_PROXY

PgSQL_Connection* PgSQL_SrvConnList::remove_unindexed_unlocked(
		unsigned int index, ReaderReservationEndReason reason) {
	if (index >= conns->len) {
		return nullptr;
	}
	PgSQL_Connection* conn =
		static_cast<PgSQL_Connection*>(conns->index(index));
#if POLARDB_PROXY
	assert(match_keys_by_position.size() == conns->len);
#ifdef DEBUG
	if (!used_list) {
		assert(!mysrvc->polardb_connection_reserved_unlocked(conn));
		for (const auto& bucket : matching_by_key) {
			assert(std::find(
				bucket.second.begin(), bucket.second.end(), conn) ==
				bucket.second.end());
		}
	}
#endif // DEBUG
	PgSQL_Connection* last = static_cast<PgSQL_Connection*>(
		conns->index(conns->len - 1));
#endif // POLARDB_PROXY
	conn = static_cast<PgSQL_Connection*>(
		conns->remove_index_fast(index));
#if POLARDB_PROXY
	if (!used_list) {
		const PolarDB_IdleTrimEndReason trim_reason =
			reason == ReaderReservationEndReason::IDLE_TRIM
				? PolarDB_IdleTrimEndReason::DESTROYED
				: reason == ReaderReservationEndReason::ACQUIRED
					? PolarDB_IdleTrimEndReason::TAKEN
					: PolarDB_IdleTrimEndReason::OTHER_REMOVAL;
		mysrvc->polardb_finish_idle_trim_unlocked(conn, trim_reason);
	}
	conn->polardb_core_pool_position = UINT32_MAX;
	if (index < conns->len) {
		last->polardb_core_pool_position = index;
		match_keys_by_position[index] = match_keys_by_position.back();
	}
	match_keys_by_position.pop_back();
	assert(match_keys_by_position.size() == conns->len);
#endif // POLARDB_PROXY
#if POLARDB_PROXY
	std::atomic<unsigned int>& count = used_list
		? mysrvc->pool_used_count : mysrvc->pool_free_count;
	count.store(conns->len, std::memory_order_relaxed);
#endif // POLARDB_PROXY
	return conn;
}

PgSQL_Connection* PgSQL_SrvConnList::remove_unlocked(
		unsigned int index, ReaderReservationEndReason reason) {
	if (index >= conns->len) {
		return nullptr;
	}
#if POLARDB_PROXY
	PgSQL_Connection* conn =
		static_cast<PgSQL_Connection*>(conns->index(index));
	if (!used_list) {
		unindex_unlocked(conn);
		mysrvc->polardb_forget_reserved_connection_unlocked(conn, reason);
	} else {
		ensure_match_index_consistent_unlocked();
	}
#endif // POLARDB_PROXY
	return remove_unindexed_unlocked(index, reason);
}

#if POLARDB_PROXY
PgSQL_Connection* PgSQL_SrvConnList::remove_matching_unlocked(
		const PgSQL_PoolMatchKey& key) {
	for (unsigned int attempt = 0; attempt < 2; ++attempt) {
		ensure_match_index_consistent_unlocked();
		auto indexed = matching_by_key.find(key);
		if (indexed == matching_by_key.end() || indexed->second.empty()) {
			return nullptr;
		}
		PgSQL_Connection* conn = indexed->second.back();
		const int index = find_idx(conn);
		if (index >= 0 &&
				static_cast<size_t>(index) < match_keys_by_position.size() &&
			match_keys_by_position[static_cast<size_t>(index)] == key) {
			indexed->second.pop_back();
			if (indexed->second.empty()) {
				limit_empty_match_buckets_unlocked(indexed);
			}
			return remove_unindexed_unlocked(
				static_cast<unsigned int>(index),
				ReaderReservationEndReason::ACQUIRED);
		}
		// Never return a map candidate whose current FREE slot carries another
		// key. Rebuild once; the healthy lookup remains O(1).
		rebuild_free_match_index_unlocked();
	}
	return nullptr;
}
#endif // POLARDB_PROXY

void PgSQL_SrvConnList::detach_all_unlocked(
		std::vector<PgSQL_Connection*>& connections,
		ReaderReservationEndReason reason) {
	connections.reserve(connections.size() + conns->len);
	while (conns->len) {
		connections.push_back(remove_unlocked(0, reason));
	}
#if POLARDB_PROXY
	if (!used_list) {
		prune_empty_match_buckets_unlocked();
		mysrvc->polardb_retire_reservations_unlocked(reason);
	}
#endif // POLARDB_PROXY
}

PgSQL_Connection * PgSQL_SrvConnList::remove(
		int index, ReaderReservationEndReason reason) {
#if POLARDB_PROXY
	std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
	return index >= 0
		? remove_unlocked(static_cast<unsigned int>(index), reason) : nullptr;
}

PgSQL_SrvConnList::PgSQL_SrvConnList(PgSQL_SrvC *_mysrvc, bool used) {
	mysrvc=_mysrvc;
	used_list=used;
	conns=new PtrArray();
}

void PgSQL_SrvConnList::add(PgSQL_Connection *c) {
#if POLARDB_PROXY
	std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
	add_unlocked(c);
}

void PgSQL_SrvConnList::remove(PgSQL_Connection* conn) {
#if POLARDB_PROXY
	std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
	const int index = find_idx(conn);
	if (index < 0) {
		proxy_error(
			"PostgreSQL core pool remove missed connection %p on %s:%u list=%s\n",
			(void*)conn, mysrvc->address ? mysrvc->address : "(null)",
			mysrvc->port, used_list ? "USED" : "FREE");
		return;
	}
	(void)remove_unlocked(static_cast<unsigned int>(index));
}

unsigned int PgSQL_SrvConnList::conns_length() {
#if POLARDB_PROXY
	return used_list
		? mysrvc->pool_used_count.load(std::memory_order_relaxed)
		: mysrvc->pool_free_count.load(std::memory_order_relaxed);
#else
	return conns->len;
#endif // POLARDB_PROXY
}

PgSQL_SrvConnList::~PgSQL_SrvConnList() {
	std::vector<PgSQL_Connection*> connections_to_delete;
#if POLARDB_PROXY
	{
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
		detach_all_unlocked(connections_to_delete);
		delete conns;
		conns = nullptr;
#if POLARDB_PROXY
	}
#endif // POLARDB_PROXY
	for (PgSQL_Connection* conn : connections_to_delete) {
		delete conn;
	}
	mysrvc=NULL;
}

void PgSQL_SrvConnList::drop_all_connections() {
	std::vector<PgSQL_Connection*> connections_to_delete;
#if POLARDB_PROXY
	{
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Dropping all connections (%u total) on PgSQL_SrvConnList %p for server %s:%d , hostgroup=%d , status=%d\n", conns_length(), this, mysrvc->address, mysrvc->port, mysrvc->myhgc->hid, mysrvc->status);
		detach_all_unlocked(connections_to_delete);
#if POLARDB_PROXY
	}
#endif // POLARDB_PROXY
	for (PgSQL_Connection* conn : connections_to_delete) {
		delete conn;
	}
}


#if POLARDB_PROXY
static std::atomic<uint64_t> polardb_next_server_instance_id{1};
#endif // POLARDB_PROXY

PgSQL_SrvC::PgSQL_SrvC(
	char* add, uint16_t p, int64_t _weight, enum MySerStatus _status, unsigned int _compression,
	int64_t _max_connections, unsigned int _max_replication_lag, int32_t _use_ssl, unsigned int _max_latency_ms,
	char* _comment
) {
	address=strdup(add);
	port=p;
	weight=_weight;
	status=_status;
	compression=_compression;
	max_connections=_max_connections;
	max_replication_lag=_max_replication_lag;
	use_ssl=_use_ssl;
	cur_replication_lag_count=0;
	max_latency_us=_max_latency_ms*1000;
	set_current_latency_us_value(0);
	aws_aurora_current_lag_us = 0;
	connect_OK=0;
	connect_ERR=0;
	queries_sent=0;
	bytes_sent=0;
	bytes_recv=0;
	max_connections_used=0;
	time_last_detected_error=0;
	connect_ERR_at_time_last_detected_error=0;
	shunned_automatic=false;
	shunned_and_kill_all_connections=false;	// false to default
	//charset=_charset;
	myhgc=NULL;
	comment=strdup(_comment);
	ConnectionsUsed=new PgSQL_SrvConnList(this, /*used=*/true);
	ConnectionsFree=new PgSQL_SrvConnList(this, /*used=*/false);
#if POLARDB_PROXY
	polardb_fast_status.store((int)_status, std::memory_order_relaxed);
	polardb_instance_id = polardb_next_server_instance_id.fetch_add(
		1, std::memory_order_relaxed);
#endif // POLARDB_PROXY
}

#if POLARDB_PROXY
static void polardb_count_reader_pool_reservation_retirement(
		ReaderReservationEndReason reason) {
	if (!PgHGM) {
		return;
	}
	switch (reason) {
	case ReaderReservationEndReason::CREATE_EVICT:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_create_evict);
		break;
	case ReaderReservationEndReason::IDLE_TRIM:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_idle_trim);
		break;
	case ReaderReservationEndReason::MAX_AGE:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_max_age);
		break;
	case ReaderReservationEndReason::SERVER_OFFLINE:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_offline);
		break;
	case ReaderReservationEndReason::POOL_DROP:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_pool_drop);
		break;
	case ReaderReservationEndReason::EXPLICIT_REMOVE:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_explicit);
		break;
	case ReaderReservationEndReason::INVALID_FREE_STATE:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_reservation_retired_invalid);
		break;
	case ReaderReservationEndReason::NONE:
	case ReaderReservationEndReason::ACQUIRED:
	case ReaderReservationEndReason::RELEASED:
		break;
	}
}

static bool polardb_reader_pool_reservation_reason_is_retirement(
		ReaderReservationEndReason reason) {
	return reason != ReaderReservationEndReason::NONE &&
		reason != ReaderReservationEndReason::ACQUIRED &&
		reason != ReaderReservationEndReason::RELEASED;
}

// The caller holds polardb_reader_pool_capacity_request_mutex and has already resolved
// the server's current topology state. This predicate therefore checks only
// worker exclusion and the request-specific lag policy.
bool PgSQL_HGC::reader_pool_capacity_request_matches_server_unlocked(
		const PolarDB_ReaderPoolCapacityRequest& request, PgSQL_SrvC* server,
		unsigned int excluded_worker_index, uint64_t& now_us) const {
	if (request.worker_index == excluded_worker_index || !PgHGM) {
		return false;
	}
	if (request.reader_plan.lag_cap_enabled() && now_us == 0) {
		// Normally sampled before this lock. This covers a lag-limited request
		// inserted or exposed between the preliminary and final request scans.
		now_us = monotonic_time();
	}
	return PgHGM->polardb_reader_server_meets_lag_policy(
		server, request.reader_plan, request.wait_spec, now_us);
}

// The caller holds polardb_reader_pool_capacity_request_mutex. This first pass checks
// only the keyed request state, allowing topology resolution to run without the
// request mutex and avoiding it entirely for unrelated or self-only request.
bool PgSQL_HGC::has_reader_pool_capacity_request_candidate_unlocked(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index,
		bool& lag_time_needed) const {
	lag_time_needed = false;
	auto bucket = polardb_reader_pool_capacity_requests_by_key.find(key);
	if (bucket == polardb_reader_pool_capacity_requests_by_key.end()) {
		return false;
	}
	for (const PolarDB_ReaderPoolCapacityRequest& request : bucket->second) {
		if (request.worker_index == excluded_worker_index) {
			continue;
		}
		lag_time_needed = request.reader_plan.lag_cap_enabled();
		return true;
	}
	return false;
}

void PgSQL_HGC::erase_capacity_request_or_abort_unlocked(
		PolarDB_ReaderPoolCapacityRequestIterator request) {
	// Keyed lists own request nodes. The worker and token maps are secondary
	// indexes; verify that all three still identify this node before removal.
	const PgSQL_PoolMatchKey key = request->match_key;
	auto bucket = polardb_reader_pool_capacity_requests_by_key.find(key);
	auto worker = polardb_reader_pool_capacity_request_by_worker.find(
		request->worker_index);
	auto token = polardb_reader_pool_capacity_request_by_token.find(request->token);
	const unsigned int request_count =
		polardb_reader_pool_capacity_request_count_fast.load(
			std::memory_order_relaxed);
	const PolarDB_ReaderPoolCapacityRequest* selected =
		std::addressof(*request);
	const bool indexes_match =
		bucket != polardb_reader_pool_capacity_requests_by_key.end() &&
		worker != polardb_reader_pool_capacity_request_by_worker.end() &&
		token != polardb_reader_pool_capacity_request_by_token.end() &&
		std::addressof(*worker->second) == selected &&
		std::addressof(*token->second) == selected &&
		request_count != 0 &&
		polardb_reader_pool_capacity_request_by_worker.size() == request_count &&
		polardb_reader_pool_capacity_request_by_token.size() == request_count;
	if (!indexes_match) {
		proxy_error(
			"PolarDB reader capacity request indexes are inconsistent for hostgroup=%d worker=%u token=%llu count=%u worker_index_size=%zu token_index_size=%zu\n",
			hid, request->worker_index,
			static_cast<unsigned long long>(request->token),
			request_count,
			polardb_reader_pool_capacity_request_by_worker.size(),
			polardb_reader_pool_capacity_request_by_token.size());
		abort();
	}
	polardb_reader_pool_capacity_request_by_worker.erase(worker);
	polardb_reader_pool_capacity_request_by_token.erase(token);
	bucket->second.erase(request);
	if (bucket->second.empty()) {
		polardb_reader_pool_capacity_requests_by_key.erase(bucket);
	}
	polardb_reader_pool_capacity_request_count_fast.fetch_sub(
		1, std::memory_order_release);
}

bool PgSQL_HGC::register_reader_pool_capacity_request(
		unsigned int worker_index, uint64_t token, uint64_t scope_hash,
		const PgSQL_PoolMatchKey& key,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec) {
	if (worker_index == UINT_MAX || token == 0 || scope_hash == 0 ||
			key.empty()) {
		return false;
	}
	// Replica-only split reads use pooled-only acquisition and do not enter
	// reservation waiting. Reject such registrations until reservation matching explicitly
	// excludes the active writer endpoint for every request.
	if (reader_plan.require_replica) {
		return false;
	}
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	if (polardb_reader_pool_capacity_request_by_token.find(token) !=
			polardb_reader_pool_capacity_request_by_token.end()) {
		if (PgHGM) {
			PgHGM->status.polardb_reader_pool_capacity_request_duplicate_token.fetch_add(
				1, std::memory_order_relaxed);
		}
		return false;
	}
	if (polardb_reader_pool_capacity_request_by_worker.find(worker_index) !=
			polardb_reader_pool_capacity_request_by_worker.end()) {
		if (PgHGM) {
			PgHGM->status.polardb_reader_pool_capacity_request_duplicate_worker.fetch_add(
				1, std::memory_order_relaxed);
		}
		return false;
	}
	auto& requests = polardb_reader_pool_capacity_requests_by_key[key];
	requests.push_back(
		PolarDB_ReaderPoolCapacityRequest{
			worker_index, token, scope_hash, key, reader_plan, wait_spec});
	auto request = std::prev(requests.end());
	polardb_reader_pool_capacity_request_by_worker.emplace(worker_index, request);
	polardb_reader_pool_capacity_request_by_token.emplace(token, request);
	polardb_reader_pool_capacity_request_count_fast.fetch_add(
		1, std::memory_order_release);
	if (PgHGM) {
		PgHGM->status.polardb_reader_pool_capacity_request_registered.fetch_add(
			1, std::memory_order_relaxed);
	}
	return true;
}

bool PgSQL_HGC::take_matching_reader_pool_capacity_request(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolCapacityRequest* selected,
		unsigned int excluded_worker_index,
		std::shared_ptr<const void>* selected_server_snapshot) {
	if (selected_server_snapshot) {
		selected_server_snapshot->reset();
	}
	if (!server || !selected || key.empty() ||
			polardb_reader_pool_capacity_request_count_fast.load(
				std::memory_order_acquire) == 0) {
		return false;
	}
	bool lag_time_needed = false;
	{
		std::lock_guard<std::mutex> lock(
			polardb_reader_pool_capacity_request_mutex);
		if (!has_reader_pool_capacity_request_candidate_unlocked(
				key, excluded_worker_index, lag_time_needed)) {
			return false;
		}
	}
	std::shared_ptr<const void> server_snapshot;
	if (!PgHGM || !PgHGM->polardb_resolve_reader_server_snapshot(
			hid, server, nullptr, -1, &server_snapshot)) {
		return false;
	}
	uint64_t now_us = lag_time_needed ? monotonic_time() : 0;
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	auto bucket = polardb_reader_pool_capacity_requests_by_key.find(key);
	if (bucket == polardb_reader_pool_capacity_requests_by_key.end()) {
		return false;
	}
	auto request = std::find_if(
		bucket->second.begin(), bucket->second.end(),
		[&](const PolarDB_ReaderPoolCapacityRequest& entry) {
			return reader_pool_capacity_request_matches_server_unlocked(
				entry, server, excluded_worker_index, now_us);
		});
	if (request == bucket->second.end()) {
		return false;
	}
	*selected = *request;
	if (selected_server_snapshot) {
		*selected_server_snapshot = std::move(server_snapshot);
	}
	erase_capacity_request_or_abort_unlocked(request);
	return true;
}

bool PgSQL_HGC::cancel_reader_pool_capacity_request(
		unsigned int worker_index, uint64_t token) {
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	auto worker = polardb_reader_pool_capacity_request_by_worker.find(worker_index);
	if (worker == polardb_reader_pool_capacity_request_by_worker.end() ||
			worker->second->token != token) {
		return false;
	}
	erase_capacity_request_or_abort_unlocked(worker->second);
	if (PgHGM) {
		PgHGM->status.polardb_reader_pool_capacity_request_cancelled.fetch_add(
			1, std::memory_order_relaxed);
	}
	return true;
}

bool PgSQL_HGC::has_reader_pool_capacity_request(
		unsigned int worker_index, uint64_t token) const {
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	auto worker = polardb_reader_pool_capacity_request_by_worker.find(worker_index);
	return worker != polardb_reader_pool_capacity_request_by_worker.end() &&
		worker->second->token == token;
}

bool PgSQL_HGC::has_matching_reader_pool_capacity_request(
		PgSQL_SrvC* server, const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index) const {
	if (!server || key.empty() ||
			polardb_reader_pool_capacity_request_count_fast.load(
				std::memory_order_acquire) == 0) {
		return false;
	}
	bool lag_time_needed = false;
	{
		std::lock_guard<std::mutex> lock(
			polardb_reader_pool_capacity_request_mutex);
		if (!has_reader_pool_capacity_request_candidate_unlocked(
				key, excluded_worker_index, lag_time_needed)) {
			return false;
		}
	}
	std::shared_ptr<const void> server_snapshot;
	if (!PgHGM || !PgHGM->polardb_resolve_reader_server_snapshot(
			hid, server, nullptr, -1, &server_snapshot)) {
		return false;
	}
	uint64_t now_us = lag_time_needed ? monotonic_time() : 0;
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	auto bucket = polardb_reader_pool_capacity_requests_by_key.find(key);
	if (bucket == polardb_reader_pool_capacity_requests_by_key.end()) {
		return false;
	}
	return std::any_of(
		bucket->second.begin(), bucket->second.end(),
		[&](const PolarDB_ReaderPoolCapacityRequest& request) {
			return reader_pool_capacity_request_matches_server_unlocked(
				request, server, excluded_worker_index, now_us);
		});
}

bool PgSQL_HGC::has_reader_pool_capacity_request_for_another_worker(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index) const {
	if (key.empty() ||
			polardb_reader_pool_capacity_request_count_fast.load(
				std::memory_order_acquire) == 0) {
		return false;
	}
	bool lag_time_needed = false;
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	return has_reader_pool_capacity_request_candidate_unlocked(
		key, excluded_worker_index, lag_time_needed);
}

unsigned int PgSQL_HGC::reader_pool_capacity_request_count() const {
	std::lock_guard<std::mutex> lock(polardb_reader_pool_capacity_request_mutex);
	return static_cast<unsigned int>(
		polardb_reader_pool_capacity_request_by_worker.size());
}

bool PgSQL_SrvC::polardb_connection_reserved_unlocked(
		PgSQL_Connection* conn) const {
	return conn && polardb_reader_pool_reservation_token_by_connection.find(conn) !=
		polardb_reader_pool_reservation_token_by_connection.end();
}

void PgSQL_SrvC::polardb_forget_reserved_connection_unlocked(
		PgSQL_Connection* conn, ReaderReservationEndReason reason) {
	auto reverse = polardb_reader_pool_reservation_token_by_connection.find(conn);
	if (reverse == polardb_reader_pool_reservation_token_by_connection.end()) {
		return;
	}
	const uint64_t token = reverse->second;
	auto reservation = polardb_reader_pool_reservations.find(token);
	if (reservation != polardb_reader_pool_reservations.end() &&
			polardb_reader_pool_reservation_reason_is_retirement(reason)) {
		const bool inserted = polardb_reader_pool_reservation_retired.emplace(
			token,
			PolarDB_ReaderPoolReservationRetired{
				reservation->second.worker_index, reason}).second;
		if (inserted) {
			polardb_count_reader_pool_reservation_retirement(reason);
		}
	}
	polardb_reader_pool_reservations.erase(token);
	polardb_reader_pool_reservation_token_by_connection.erase(reverse);
}

void PgSQL_SrvC::polardb_retire_reservations_unlocked(
		ReaderReservationEndReason reason) {
	if (!polardb_reader_pool_reservation_reason_is_retirement(reason)) {
		polardb_reader_pool_reservations.clear();
		polardb_reader_pool_reservation_token_by_connection.clear();
		return;
	}
	for (const auto& entry : polardb_reader_pool_reservations) {
		const bool inserted = polardb_reader_pool_reservation_retired.emplace(
			entry.first,
			PolarDB_ReaderPoolReservationRetired{
				entry.second.worker_index, reason}).second;
		if (inserted) {
			polardb_count_reader_pool_reservation_retirement(reason);
		}
	}
	polardb_reader_pool_reservations.clear();
	polardb_reader_pool_reservation_token_by_connection.clear();
}

void PgSQL_SrvC::reserve_reader_pool_connection_unlocked(
		PgSQL_Connection* conn,
		const PolarDB_ReaderPoolCapacityRequest& selected,
		std::shared_ptr<const void> selected_server_snapshot,
		PolarDB_ReaderPoolReservationWake* wake) {
	assert(conn);
	assert(ConnectionsFree);
	assert(selected_server_snapshot);

	conn->polardb_selected_server_snapshot =
		std::move(selected_server_snapshot);
	ConnectionsFree->add_unlocked(conn);
	polardb_reader_pool_reservations.emplace(
		selected.token,
		PolarDB_ReaderPoolConnectionReservation{
			selected.worker_index, selected.token, conn, selected.match_key});
	polardb_reader_pool_reservation_token_by_connection.emplace(conn, selected.token);
	if (wake) {
		wake->worker_index = selected.worker_index;
		wake->token = selected.token;
		wake->server = this;
		wake->server_snapshot = conn->polardb_selected_server_snapshot;
	}
	if (PgHGM) {
		PgHGM->status.polardb_reader_pool_connection_reserved.fetch_add(
			1, std::memory_order_relaxed);
	}
}

bool PgSQL_SrvC::store_matching_free_connection_unlocked(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolReservationWake* wake,
		std::shared_ptr<const void>& released_server_snapshot,
		unsigned int excluded_worker_index) {
	if (wake) {
		*wake = PolarDB_ReaderPoolReservationWake{};
	}
	if (!conn || !ConnectionsFree || status != MYSQL_SERVER_STATUS_ONLINE) {
		return false;
	}
	released_server_snapshot =
		std::move(conn->polardb_selected_server_snapshot);
	PolarDB_ReaderPoolCapacityRequest selected;
	std::shared_ptr<const void> selected_server_snapshot;
	if (!myhgc || !myhgc->take_matching_reader_pool_capacity_request(
			this, key, &selected, excluded_worker_index,
			&selected_server_snapshot)) {
		ConnectionsFree->add_unlocked(conn, &key);
		return true;
	}
	reserve_reader_pool_connection_unlocked(
		conn, selected, std::move(selected_server_snapshot), wake);
	return true;
}

PgSQL_Connection* PgSQL_SrvC::take_matching_connection(
		const PgSQL_PoolMatchKey& key) {
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (status != MYSQL_SERVER_STATUS_ONLINE || !ConnectionsFree ||
			!ConnectionsUsed) {
		return nullptr;
	}
	PgSQL_Connection* conn =
		ConnectionsFree->remove_matching_unlocked(key);
	if (!conn) {
		return nullptr;
	}
	ConnectionsUsed->add_unlocked(conn, &key);
	return conn;
}

// Both callers run for each acquisition, so keep these checks inlined.
__attribute__((always_inline)) inline
void PgSQL_SrvC::take_reusable_connection_unlocked(
		PgSQL_Session* sess, const PgSQL_PoolMatchKey& key,
		PgSQL_PoolGetMode mode, PgSQL_PoolGetResult& result) {
	if (pgsql_pool_get_mode_has(mode,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH)) {
		result.conn = ConnectionsFree->remove_matching_unlocked(key);
		if (result.conn) {
			ConnectionsUsed->add_unlocked(result.conn);
			result.source = PgSQL_PoolGetSource::EXACT_MATCH;
		}
	}
	if (!result.conn && pgsql_pool_get_mode_has(
			mode, PgSQL_PoolGetMode::ALLOW_RESET)) {
		// Reset-compatible reuse is broader than an exact-key lookup but still
		// requires the same user and database.
		result.conn = ConnectionsFree->get_random_MyConn_unlocked(
			sess, false, true);
		if (result.conn) {
			ConnectionsUsed->add_unlocked(result.conn);
			result.source = PgSQL_PoolGetSource::RESET;
		}
	}
}

PgSQL_PoolGetResult PgSQL_SrvC::take_existing_connection(
		PgSQL_Session* sess, const PgSQL_PoolMatchKey& key,
		PgSQL_PoolGetMode mode, unsigned int selected_max_connections,
		unsigned long long* lock_wait_us,
		unsigned long long* lock_hold_us) {
	PgSQL_PoolGetResult result;
	if (lock_wait_us) {
		*lock_wait_us = 0;
	}
	if (lock_hold_us) {
		*lock_hold_us = 0;
	}
#if POLARDB_PROXY && POLARDB_PROFILE
	const unsigned long long wait_started_at = monotonic_time();
#endif // POLARDB_PROXY && POLARDB_PROFILE
	std::unique_lock<std::recursive_mutex> pool_lock(
		pool_mutex, std::defer_lock);
	if (pgsql_pool_get_mode_has(mode, PgSQL_PoolGetMode::SKIP_BUSY_POOL)) {
		if (!pool_lock.try_lock()) {
			result.pool_busy = true;
			return result;
		}
	} else {
		pool_lock.lock();
	}
#if POLARDB_PROXY && POLARDB_PROFILE
	const unsigned long long lock_acquired_at = monotonic_time();
	if (lock_wait_us) {
		*lock_wait_us = lock_acquired_at - wait_started_at;
	}
#endif // POLARDB_PROXY && POLARDB_PROFILE

	if (status == MYSQL_SERVER_STATUS_ONLINE && ConnectionsFree &&
			ConnectionsUsed) {
		take_reusable_connection_unlocked(sess, key, mode, result);
#if POLARDB_PROFILE
		if (!result.conn && selected_max_connections == 0 &&
				pgsql_pool_get_mode_has(
					mode, PgSQL_PoolGetMode::ALLOW_EXACT_MATCH) &&
				!key.empty()) {
			result.exact_match_reserved = std::any_of(
				polardb_reader_pool_reservations.begin(), polardb_reader_pool_reservations.end(),
				[&key](const auto& entry) {
					return entry.second.match_key == key;
				});
		}
#endif // POLARDB_PROFILE
		if (!result.conn && selected_max_connections > 0) {
			const unsigned int used = pool_used_count_value();
			const unsigned int free = pool_free_count_value();
			const unsigned int reserved_free =
				static_cast<unsigned int>(polardb_reader_pool_reservations.size());
			const bool all_free_reserved =
				free > 0 && reserved_free == free;
			result.server_saturated =
				used >= selected_max_connections ||
				(all_free_reserved &&
				 static_cast<uint64_t>(used) + free >=
					selected_max_connections);
		}
	}

#if POLARDB_PROXY && POLARDB_PROFILE
	if (lock_hold_us) {
		*lock_hold_us = monotonic_time() - lock_acquired_at;
	}
#endif // POLARDB_PROXY && POLARDB_PROFILE
	return result;
}
#endif // POLARDB_PROXY

PgSQL_Connection* PgSQL_SrvC::take_free_connection_for_ping(
		PgSQL_Connection* conn, unsigned long long max_last_time_used) {
	if (!conn) {
		return nullptr;
	}
#if POLARDB_PROXY
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
#endif // POLARDB_PROXY
	if (status != MYSQL_SERVER_STATUS_ONLINE || !ConnectionsFree ||
			!ConnectionsUsed) {
		return nullptr;
	}
#if POLARDB_PROXY
	ConnectionsFree->ensure_match_index_consistent_unlocked();
#endif // POLARDB_PROXY
	const int index = ConnectionsFree->find_idx(conn);
	bool reserved = false;
#if POLARDB_PROXY
	reserved = polardb_connection_reserved_unlocked(conn);
#endif // POLARDB_PROXY
	if (index < 0 || !conn->last_time_used ||
			conn->last_time_used >= max_last_time_used ||
			reserved) {
		return nullptr;
	}
#if POLARDB_PROXY
	PgSQL_PoolMatchKey key;
	const bool has_key =
		ConnectionsFree->find_match_key_unlocked(conn, &key);
#endif // POLARDB_PROXY
	PgSQL_Connection* selected = ConnectionsFree->remove_unlocked(
		static_cast<unsigned int>(index),
		ReaderReservationEndReason::ACQUIRED);
	if (!selected) {
		return nullptr;
	}
#if POLARDB_PROXY
	ConnectionsUsed->add_unlocked(selected, has_key ? &key : nullptr);
	if (!selected->polardb_idle_ping_inflight.exchange(
			true, std::memory_order_acq_rel)) {
		polardb_idle_ping_count.fetch_add(1, std::memory_order_relaxed);
		if (PgHGM) {
			POLARDB_HGM_STATUS_COUNT_ONE(
				PgHGM->status, reader_pool_idle_ping_take);
		}
	}
#else
	ConnectionsUsed->add_unlocked(selected);
#endif // POLARDB_PROXY
	return selected;
}

#if POLARDB_PROXY
bool PgSQL_SrvC::polardb_finish_idle_ping(
		PgSQL_Connection* conn, bool destroyed) {
	if (!conn || !conn->polardb_idle_ping_inflight.load(
			std::memory_order_relaxed)) {
		return false;
	}
	if (!conn->polardb_idle_ping_inflight.exchange(
			false, std::memory_order_acq_rel)) {
		return false;
	}
	unsigned int count = polardb_idle_ping_count.load(
		std::memory_order_acquire);
	while (count != 0 && !polardb_idle_ping_count.compare_exchange_weak(
			count, count - 1, std::memory_order_acq_rel,
			std::memory_order_acquire)) {
	}
	assert(count != 0);
	if (PgHGM) {
		if (destroyed) {
			POLARDB_HGM_STATUS_COUNT_ONE(
				PgHGM->status, reader_pool_idle_ping_destroy);
		} else {
			POLARDB_HGM_STATUS_COUNT_ONE(
				PgHGM->status, reader_pool_idle_ping_return);
		}
	}
	return true;
}

bool PgSQL_SrvC::polardb_finish_idle_trim_unlocked(
		PgSQL_Connection* conn, PolarDB_IdleTrimEndReason reason) {
	if (!conn || !conn->polardb_idle_trim_pending) {
		return false;
	}
	conn->polardb_idle_trim_pending = false;
	if (!PgHGM) {
		return true;
	}
	if (reason == PolarDB_IdleTrimEndReason::DESTROYED) {
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_idle_trim_destroyed);
		return true;
	}
	POLARDB_HGM_STATUS_COUNT_ONE(
		PgHGM->status, reader_pool_idle_trim_cancelled);
	switch (reason) {
	case PolarDB_IdleTrimEndReason::TAKEN:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_idle_trim_cancelled_taken);
		break;
	case PolarDB_IdleTrimEndReason::RETAINED:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_idle_trim_cancelled_retained);
		break;
	case PolarDB_IdleTrimEndReason::OTHER_REMOVAL:
		POLARDB_HGM_STATUS_COUNT_ONE(
			PgHGM->status, reader_pool_idle_trim_cancelled_other);
		break;
	case PolarDB_IdleTrimEndReason::DESTROYED:
		break;
	}
	return true;
}

PolarDB_IdleTrimResult
PgSQL_SrvC::polardb_trim_free_connections_to_max_unlocked(
		unsigned int max_free,
		std::vector<PgSQL_Connection*>& connections_to_delete) {
	PolarDB_IdleTrimResult result;
	if (!ConnectionsFree) {
		return result;
	}

	unsigned int index = 0;
	while (ConnectionsFree->conns->len > max_free &&
			index < ConnectionsFree->conns->len) {
		PgSQL_Connection* connection = static_cast<PgSQL_Connection*>(
			ConnectionsFree->conns->index(index));
		if (!connection || !connection->polardb_idle_trim_pending) {
			index++;
			continue;
		}
		connections_to_delete.push_back(
			ConnectionsFree->remove_unlocked(
				index, ReaderReservationEndReason::IDLE_TRIM));
		result.destroyed++;
	}
	if (ConnectionsFree->conns->len <= max_free) {
		for (index = 0; index < ConnectionsFree->conns->len; index++) {
			PgSQL_Connection* connection = static_cast<PgSQL_Connection*>(
				ConnectionsFree->conns->index(index));
			if (polardb_finish_idle_trim_unlocked(
					connection, PolarDB_IdleTrimEndReason::RETAINED)) {
				result.cancelled++;
			}
		}
	}

	unsigned int excess = ConnectionsFree->conns->len > max_free
		? ConnectionsFree->conns->len - max_free : 0;
	for (index = 0;
			index < ConnectionsFree->conns->len && excess != 0;
			index++) {
		PgSQL_Connection* connection = static_cast<PgSQL_Connection*>(
			ConnectionsFree->conns->index(index));
		if (!connection || connection->polardb_idle_trim_pending) {
			continue;
		}
		connection->polardb_idle_trim_pending = true;
		result.deferred++;
		excess--;
	}

	if (PgHGM) {
		POLARDB_HGM_STATUS_COUNT(
			PgHGM->status, reader_pool_idle_trim_deferred,
			result.deferred);
	}
	return result;
}

bool PgSQL_SrvC::add_matching_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		PolarDB_ReaderPoolReservationWake* wake) {
	if (!conn) {
		return false;
	}
	std::shared_ptr<const void> released_server_snapshot;
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (status != MYSQL_SERVER_STATUS_ONLINE || !ConnectionsFree) {
		return false;
	}
	return store_matching_free_connection_unlocked(
		conn, key, wake, released_server_snapshot, UINT_MAX);
}

bool PgSQL_SrvC::add_used_matching_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key) {
	if (!conn) {
		return false;
	}
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (status != MYSQL_SERVER_STATUS_ONLINE || !ConnectionsUsed) {
		return false;
	}
	ConnectionsUsed->add_unlocked(conn, &key);
	return true;
}

ServerReturnResult PgSQL_SrvC::return_matching_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		unsigned long long* lock_wait_us,
		unsigned long long* lock_hold_us,
		unsigned int excluded_worker_index) {
	ServerReturnResult result;
	if (!conn) {
		return result;
	}
	if (lock_wait_us) {
		*lock_wait_us = 0;
	}
	if (lock_hold_us) {
		*lock_hold_us = 0;
	}
	std::shared_ptr<const void> released_server_snapshot;
#if POLARDB_PROXY && POLARDB_PROFILE
	const unsigned long long wait_started_at = monotonic_time();
#endif // POLARDB_PROXY && POLARDB_PROFILE
	std::unique_lock<std::recursive_mutex> pool_lock(pool_mutex);
#if POLARDB_PROXY && POLARDB_PROFILE
	const unsigned long long lock_acquired_at = monotonic_time();
	if (lock_wait_us) {
		*lock_wait_us = lock_acquired_at - wait_started_at;
	}
#endif // POLARDB_PROXY && POLARDB_PROFILE
	const int index = ConnectionsUsed ? ConnectionsUsed->find_idx(conn) : -1;
	if (index < 0) {
#if POLARDB_PROXY && POLARDB_PROFILE
		if (lock_hold_us) {
			*lock_hold_us = monotonic_time() - lock_acquired_at;
		}
#endif // POLARDB_PROXY && POLARDB_PROFILE
		proxy_error(
			"PostgreSQL core could not return matching USED connection %p on %s:%u\n",
			(void*)conn, address ? address : "(null)", port);
		result.status = ServerReturnStatus::NOT_IN_USED_LIST;
		return result;
	}
	(void)ConnectionsUsed->remove_unlocked(static_cast<unsigned int>(index));
	if (status != MYSQL_SERVER_STATUS_ONLINE) {
#if POLARDB_PROXY && POLARDB_PROFILE
		if (lock_hold_us) {
			*lock_hold_us = monotonic_time() - lock_acquired_at;
		}
#endif // POLARDB_PROXY && POLARDB_PROFILE
		result.status = ServerReturnStatus::SERVER_NOT_ONLINE;
		return result;
	}
	const bool stored =
		store_matching_free_connection_unlocked(
			conn, key, &result.wake, released_server_snapshot,
			excluded_worker_index);
#if POLARDB_PROXY && POLARDB_PROFILE
	if (lock_hold_us) {
		*lock_hold_us = monotonic_time() - lock_acquired_at;
	}
#endif // POLARDB_PROXY && POLARDB_PROFILE
	result.status = stored
		? ServerReturnStatus::STORED
		: ServerReturnStatus::STORE_FAILED;
	return result;
}

PolarDB_ReaderRemoteReservationResult
PgSQL_SrvC::reserve_matching_used_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& key,
		unsigned int returning_worker_index,
		PolarDB_ReaderPoolReservationWake* wake) {
	if (wake) {
		*wake = PolarDB_ReaderPoolReservationWake{};
	}
	if (!conn || key.empty() || returning_worker_index == UINT_MAX) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	std::shared_ptr<const void> previous_server_snapshot;
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	const int used_index = ConnectionsUsed
		? ConnectionsUsed->find_idx(conn) : -1;
	if (used_index < 0 || !ConnectionsFree ||
			status != MYSQL_SERVER_STATUS_ONLINE || !myhgc) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	PolarDB_ReaderPoolCapacityRequest selected;
	std::shared_ptr<const void> selected_server_snapshot;
	// Lock order is server pool_mutex, then HGC request mutex. Request paths never
	// hold the HGC mutex while acquiring a server pool mutex.
	if (!myhgc->take_matching_reader_pool_capacity_request(
			this, key, &selected, returning_worker_index,
			&selected_server_snapshot)) {
		return PolarDB_ReaderRemoteReservationResult::NO_REMOTE_REQUEST;
	}
	assert(selected_server_snapshot);
	previous_server_snapshot =
		std::move(conn->polardb_selected_server_snapshot);
	(void)ConnectionsUsed->remove_unlocked(
		static_cast<unsigned int>(used_index));
	reserve_reader_pool_connection_unlocked(
		conn, selected, std::move(selected_server_snapshot), wake);
	return PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED;
}

bool PgSQL_SrvC::has_matching_reader_pool_capacity_request(
		const PgSQL_PoolMatchKey& key,
		unsigned int excluded_worker_index) const {
	return myhgc && myhgc->has_matching_reader_pool_capacity_request(
		const_cast<PgSQL_SrvC*>(this), key, excluded_worker_index);
}

ReaderTakeResult PgSQL_SrvC::take_reader_pool_reservation(
		unsigned int worker_index, uint64_t token) {
	ReaderTakeResult result;
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	auto reservation = polardb_reader_pool_reservations.find(token);
	if (reservation != polardb_reader_pool_reservations.end() &&
			reservation->second.worker_index == worker_index) {
		const PolarDB_ReaderPoolConnectionReservation selected = reservation->second;
		const int index = ConnectionsFree
			? ConnectionsFree->find_idx(selected.conn) : -1;
		if (index < 0 || status != MYSQL_SERVER_STATUS_ONLINE ||
				!ConnectionsUsed) {
			const ReaderReservationEndReason reason =
				status != MYSQL_SERVER_STATUS_ONLINE
					? ReaderReservationEndReason::SERVER_OFFLINE
					: ReaderReservationEndReason::INVALID_FREE_STATE;
			polardb_forget_reserved_connection_unlocked(
				selected.conn, reason);
		}
		else {
			PgSQL_Connection* conn = ConnectionsFree->remove_unlocked(
				static_cast<unsigned int>(index),
				ReaderReservationEndReason::ACQUIRED);
			ConnectionsUsed->add_unlocked(conn, &selected.match_key);
			result.conn = conn;
			result.status = ReaderTakeStatus::ACQUIRED;
			if (PgHGM) {
				PgHGM->status.polardb_reader_pool_reservation_acquired.fetch_add(
					1, std::memory_order_relaxed);
			}
			return result;
		}
	}
	if (myhgc && myhgc->has_reader_pool_capacity_request(worker_index, token)) {
		result.status = ReaderTakeStatus::PENDING;
	}
	if (result.status == ReaderTakeStatus::PENDING) {
		return result;
	}
	auto retired = polardb_reader_pool_reservation_retired.find(token);
	if (retired != polardb_reader_pool_reservation_retired.end() &&
			retired->second.worker_index == worker_index) {
		result.status = ReaderTakeStatus::RETIRED;
		result.retire_reason = retired->second.reason;
		polardb_reader_pool_reservation_retired.erase(retired);
	}
	return result;
}

ReaderCancelResult
PgSQL_SrvC::cancel_reader_pool_reservation(
		unsigned int worker_index, uint64_t token) {
	ReaderCancelResult result;
	if (myhgc && myhgc->cancel_reader_pool_capacity_request(worker_index, token)) {
		result.status =
			ReaderCancelStatus::REQUEST_CANCELLED;
		return result;
	}
	std::shared_ptr<const void> released_server_snapshot;
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	auto reservation = polardb_reader_pool_reservations.find(token);
	if (reservation == polardb_reader_pool_reservations.end() ||
			reservation->second.worker_index != worker_index) {
		auto retired = polardb_reader_pool_reservation_retired.find(token);
		if (retired == polardb_reader_pool_reservation_retired.end() ||
				retired->second.worker_index != worker_index) {
			return result;
		}
		polardb_reader_pool_reservation_retired.erase(retired);
		result.status =
			ReaderCancelStatus::RETIRED_RECORD_CLEARED;
		return result;
	}
	const PolarDB_ReaderPoolConnectionReservation selected = reservation->second;
	const int index = ConnectionsFree
		? ConnectionsFree->find_idx(selected.conn) : -1;
	if (index < 0) {
		polardb_forget_reserved_connection_unlocked(
			selected.conn,
			ReaderReservationEndReason::RELEASED);
		result.status =
			ReaderCancelStatus::CONNECTION_NOT_FREE;
		return result;
	}
	PgSQL_Connection* conn = ConnectionsFree->remove_unlocked(
		static_cast<unsigned int>(index),
		ReaderReservationEndReason::RELEASED);
	if (status == MYSQL_SERVER_STATUS_ONLINE) {
		const bool stored = store_matching_free_connection_unlocked(
			conn, selected.match_key, &result.next_wake,
			released_server_snapshot,
			UINT_MAX);
		assert(stored);
		(void)stored;
	}
	else {
		released_server_snapshot =
			std::move(conn->polardb_selected_server_snapshot);
		ConnectionsFree->add_unlocked(conn, &selected.match_key);
	}
	if (PgHGM) {
		PgHGM->status.polardb_reader_pool_reservation_released.fetch_add(
			1, std::memory_order_relaxed);
	}
	result.status = ReaderCancelStatus::CONNECTION_RETURNED;
	return result;
}

bool PgSQL_SrvC::evict_unreserved_free_for_create(
		unsigned int preferred_count, unsigned int required_count,
		std::vector<PgSQL_Connection*>& connections_to_delete) {
	if (preferred_count == 0 && required_count == 0) {
		return true;
	}
	if (preferred_count < required_count) {
		return false;
	}
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (!ConnectionsFree) {
		return required_count == 0;
	}

	unsigned int evictable = 0;
	for (unsigned int index = 0; index < ConnectionsFree->conns->len; index++) {
		PgSQL_Connection* conn = static_cast<PgSQL_Connection*>(
			ConnectionsFree->conns->index(index));
		if (!polardb_connection_reserved_unlocked(conn)) {
			evictable++;
		}
	}
	if (evictable < required_count) {
		return false;
	}

	unsigned int remaining = std::min(preferred_count, evictable);
	for (unsigned int index = 0;
			index < ConnectionsFree->conns->len && remaining > 0;) {
		PgSQL_Connection* conn = static_cast<PgSQL_Connection*>(
			ConnectionsFree->conns->index(index));
		if (polardb_connection_reserved_unlocked(conn)) {
			index++;
			continue;
		}
		connections_to_delete.push_back(
			ConnectionsFree->remove_unlocked(
				index, ReaderReservationEndReason::CREATE_EVICT));
		remaining--;
	}
	return remaining == 0;
}

unsigned int PgSQL_SrvC::reader_pool_capacity_request_count() const {
	return myhgc ? myhgc->reader_pool_capacity_request_count() : 0;
}

unsigned int PgSQL_SrvC::reader_pool_reservation_count() const {
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	return static_cast<unsigned int>(polardb_reader_pool_reservations.size());
}

bool PgSQL_SrvC::used_connection_match_key(
		PgSQL_Connection* conn, PgSQL_PoolMatchKey* key) const {
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (!ConnectionsUsed) {
		return false;
	}
	ConnectionsUsed->ensure_match_index_consistent_unlocked();
	return ConnectionsUsed->find_match_key_unlocked(conn, key);
}

unsigned int PgSQL_SrvC::matching_connection_count(
		const PgSQL_PoolMatchKey& key) const {
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	if (!ConnectionsFree) {
		return 0;
	}
	ConnectionsFree->ensure_match_index_consistent_unlocked();
	auto found = ConnectionsFree->matching_by_key.find(key);
	return found == ConnectionsFree->matching_by_key.end()
		? 0U : static_cast<unsigned int>(found->second.size());
}

bool PgSQL_SrvC::remove_used_connection(PgSQL_Connection* conn) {
	if (!conn) {
		return false;
	}
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	const int index = ConnectionsUsed ? ConnectionsUsed->find_idx(conn) : -1;
	if (index < 0) {
		return false;
	}
	(void)ConnectionsUsed->remove_unlocked(static_cast<unsigned int>(index));
	return true;
}

bool PgSQL_SrvC::remove_free_connection(PgSQL_Connection* conn) {
	if (!conn) {
		return false;
	}
	std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
	const int index = ConnectionsFree ? ConnectionsFree->find_idx(conn) : -1;
	if (index < 0) {
		return false;
	}
	(void)ConnectionsFree->remove_unlocked(static_cast<unsigned int>(index));
	return true;
}
#endif // POLARDB_PROXY

#if POLARDB_PROXY
void PgSQL_SrvC::set_status(enum MySerStatus new_status) {
	std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		server_list = new_status == MYSQL_SERVER_STATUS_OFFLINE_HARD && PgHGM
			? PgHGM->get_polardb_server_list_snapshot() : nullptr;
	(void)server_list;
	std::vector<PgSQL_Connection*> connections_to_delete;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(pool_mutex);
		status = new_status;
		polardb_fast_status.store((int)new_status, std::memory_order_release);
		if (new_status == MYSQL_SERVER_STATUS_OFFLINE_HARD && ConnectionsFree) {
			ConnectionsFree->detach_all_unlocked(
				connections_to_delete,
				ReaderReservationEndReason::SERVER_OFFLINE);
		}
	}
	for (PgSQL_Connection* conn : connections_to_delete) {
		delete conn;
	}
}

enum MySerStatus PgSQL_SrvC::polardb_fast_status_value() const {
	return (enum MySerStatus)polardb_fast_status.load(std::memory_order_acquire);
}

void PgSQL_SrvC::polardb_lock_lsn_cache() {
	// TODO: Compare the fixed 64-spin yield with a short CPU-pause backoff.
	// Keep a rare yield under sustained contention, and require matched
	// throughput, CPU, scheduler-wait, and cache-line measurements.
	unsigned int spins = 0;
	bool expected = false;
	while (!polardb_lsn_cache_guard.compare_exchange_weak(
			expected, true,
			std::memory_order_acquire, std::memory_order_relaxed)) {
		expected = false;
		while (polardb_lsn_cache_guard.load(std::memory_order_relaxed)) {
			if (++spins % 64 == 0) {
				std::this_thread::yield();
			}
		}
	}
}

void PgSQL_SrvC::polardb_unlock_lsn_cache() {
	polardb_lsn_cache_guard.store(false, std::memory_order_release);
}

bool PgSQL_SrvC::polardb_advance_lsn(uint64_t lsn, uint64_t observed_at_us) {
	if (lsn == 0) return false;

	bool advanced = polardb_atomic_max_u64(polardb_current_lsn, lsn);
	// Store the observation time after its LSN, with release ordering. Readers
	// acquire this timestamp before loading the LSN, so a fresh timestamp cannot
	// expose an older value.
	lsn_updated_at.store(observed_at_us, std::memory_order_release);
	return advanced;
}
#endif // POLARDB_PROXY

#if !POLARDB_PROXY
void PgSQL_SrvC::set_status(enum MySerStatus new_status) {
	status = new_status;
}
#endif // !POLARDB_PROXY

static unsigned int pgsql_pool_report_conn_used(PgSQL_SrvC* mysrvc) {
	if (!mysrvc) {
		return 0;
	}
#if POLARDB_PROXY
	return mysrvc->polardb_pool_conn_stats().active();
#else
	return mysrvc->ConnectionsUsed ? mysrvc->ConnectionsUsed->conns_length() : 0;
#endif // POLARDB_PROXY
}

static unsigned int pgsql_pool_report_conn_free(PgSQL_SrvC* mysrvc) {
	if (!mysrvc) {
		return 0;
	}
#if POLARDB_PROXY
	return mysrvc->polardb_pool_conn_stats().idle();
#else
	return mysrvc->ConnectionsFree ? mysrvc->ConnectionsFree->conns_length() : 0;
#endif // POLARDB_PROXY
}

static bool pgsql_pool_no_live_connections(PgSQL_SrvC* mysrvc) {
	if (!mysrvc) {
		return true;
	}
#if POLARDB_PROXY
	// FREE and USED must be observed under the same server lock. A connection
	// transfer changes the two counts separately while holding this lock; two
	// unlocked reads could otherwise mistake that brief transfer for an empty
	// server and allow topology cleanup to retire it.
	std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
	const bool core_empty =
		(!mysrvc->ConnectionsUsed ||
			mysrvc->ConnectionsUsed->conns_length() == 0) &&
		(!mysrvc->ConnectionsFree ||
			mysrvc->ConnectionsFree->conns_length() == 0);
	return core_empty;
}

void pgsql_pool_status_count(
	unsigned long* counter, unsigned long value
) {
	__sync_fetch_and_add(counter, value);
}

void pgsql_pool_status_count_get(
	PgSQL_Thread* thread, unsigned long* fallback_counter, unsigned long value
) {
	if (thread) {
		thread->status_variables.pgconnpoll_get += value;
	} else {
		pgsql_pool_status_count(fallback_counter, value);
	}
}

void pgsql_pool_status_count_get_ok(
	PgSQL_Thread* thread, unsigned long* fallback_counter, unsigned long value
) {
	if (thread) {
		thread->status_variables.pgconnpoll_get_ok += value;
	} else {
		pgsql_pool_status_count(fallback_counter, value);
	}
}

void pgsql_pool_status_count_push(
	PgSQL_Thread* thread, unsigned long* fallback_counter, unsigned long value
) {
	if (thread) {
		thread->status_variables.pgconnpoll_push += value;
	} else {
		pgsql_pool_status_count(fallback_counter, value);
	}
}

unsigned long pgsql_pool_status_read(unsigned long* counter) {
	return __atomic_load_n(counter, __ATOMIC_RELAXED);
}

unsigned long pgsql_pool_status_read_get(unsigned long* fallback_counter) {
	unsigned long long total =
		__atomic_load_n(fallback_counter, __ATOMIC_RELAXED);
	if (GloPTH) {
		total += GloPTH->get_pgconnpoll_get();
	}
	return static_cast<unsigned long>(total);
}

unsigned long pgsql_pool_status_read_get_ok(unsigned long* fallback_counter) {
	unsigned long long total =
		__atomic_load_n(fallback_counter, __ATOMIC_RELAXED);
	if (GloPTH) {
		total += GloPTH->get_pgconnpoll_get_ok();
	}
	return static_cast<unsigned long>(total);
}

unsigned long pgsql_pool_status_read_push(unsigned long* fallback_counter) {
	unsigned long long total =
		__atomic_load_n(fallback_counter, __ATOMIC_RELAXED);
	if (GloPTH) {
		total += GloPTH->get_pgconnpoll_push();
	}
	return static_cast<unsigned long>(total);
}

static void pgsql_pool_trim_idle_connections_to_max(PgSQL_SrvC* mysrvc) {
	if (!mysrvc || !mysrvc->ConnectionsFree) {
		return;
	}
#if POLARDB_PROXY
	std::vector<PgSQL_Connection*> connections_to_delete;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
		const unsigned int max_connections =
			mysrvc->max_connections > 0
				? static_cast<unsigned int>(mysrvc->max_connections)
				: 0;
		while (mysrvc->ConnectionsFree->conns_length() &&
				mysrvc->ConnectionsUsed->conns_length() +
					mysrvc->ConnectionsFree->conns_length() > max_connections) {
			connections_to_delete.push_back(
				mysrvc->ConnectionsFree->remove(
					0, ReaderReservationEndReason::IDLE_TRIM));
		}
	}
	for (PgSQL_Connection* conn : connections_to_delete) {
		delete conn;
	}
#else
	const unsigned int max_connections =
		mysrvc->max_connections > 0
			? static_cast<unsigned int>(mysrvc->max_connections)
			: 0;
	while (mysrvc->ConnectionsFree->conns_length() &&
			mysrvc->ConnectionsUsed->conns_length() +
				mysrvc->ConnectionsFree->conns_length() > max_connections) {
		delete mysrvc->ConnectionsFree->remove(0);
	}
#endif // POLARDB_PROXY
}

void PgSQL_SrvC::connect_error(int err_num, bool get_mutex) {
	// NOTE: this function operates without any mutex
	// although, it is not extremely important if any counter is lost
	// as a single connection failure won't make a significant difference
	proxy_debug(PROXY_DEBUG_MYSQL_CONNECTION, 5, "Connect failed with code '%d'\n", err_num);
	__sync_fetch_and_add(&connect_ERR,1);
	__sync_fetch_and_add(&PgHGM->status.server_connections_aborted,1);
	if (err_num >= 1048 && err_num <= 1052)
		return;
	if (err_num >= 1054 && err_num <= 1075)
		return;
	if (err_num >= 1099 && err_num <= 1104)
		return;
	if (err_num >= 1106 && err_num <= 1113)
		return;
	if (err_num >= 1116 && err_num <= 1118)
		return;
	if (err_num == 1136 || (err_num >= 1138 && err_num <= 1149))
		return;
	switch (err_num) {
		case 1007: // Can't create database
		case 1008: // Can't drop database
		case 1044: // access denied
		case 1045: // access denied
/*
		case 1048: // Column cannot be null
		case 1049: // Unknown database
		case 1050: // Table already exists
		case 1051: // Unknown table
		case 1052: // Column is ambiguous
*/
		case 1120:
		case 1203: // User %s already has more than 'max_user_connections' active connections
		case 1226: // User '%s' has exceeded the '%s' resource (current value: %ld)
		case 3118: // Access denied for user '%s'. Account is locked..
			return;
			break;
		default:
			break;
	}
	time_t t=time(NULL);
	if (t > time_last_detected_error) {
		time_last_detected_error=t;
		connect_ERR_at_time_last_detected_error=1;
	} else {
		if (t < time_last_detected_error) {
			// time_last_detected_error is in the future
			// this means that monitor has a ping interval too big and tuned that in the future
			return;
		}
		// same time
		/**
		 * @brief The expected configured retries set by 'pgsql-connect_retries_on_failure' + '2' extra expected
		 *   connection errors.
		 * @details This two extra connections errors are expected:
		 *   1. An initial connection error generated by the datastream and the connection when being created,
		 *     this is, right after the session has requested a connection to the connection pool. This error takes
		 *     places directly in the state machine from 'PgSQL_Connection'. Because of this, we consider this
		 *     additional error to be a consequence of the two states machines, and it's not considered for
		 *     'connect_retries'.
		 *   2. A second connection connection error, which is the initial connection error generated by 'PgSQL_Session'
		 *     when already in the 'CONNECTING_SERVER' state. This error is an 'extra error' to always consider, since
		 *     it's not part of the retries specified by 'pgsql_thread___connect_retries_on_failure', thus, we set the
		 *     'connect_retries' to be 'pgsql_thread___connect_retries_on_failure + 1'.
		 */
		int connect_retries = pgsql_thread___connect_retries_on_failure + 1;
		int max_failures = pgsql_thread___shun_on_failures > connect_retries ? connect_retries : pgsql_thread___shun_on_failures;

		if (__sync_add_and_fetch(&connect_ERR_at_time_last_detected_error,1) >= (unsigned int)max_failures) {
			bool _shu=false;
			if (get_mutex==true)
				PgHGM->wrlock(); // to prevent race conditions, lock here. See #627
			if (status==MYSQL_SERVER_STATUS_ONLINE) {
				set_status(MYSQL_SERVER_STATUS_SHUNNED);
				shunned_automatic=true;
				_shu=true;
			} else {
				_shu=false;
			}
			if (get_mutex==true)
				PgHGM->wrunlock();
			if (_shu) {
			proxy_error("Shunning server %s:%d with %u errors/sec. Shunning for %u seconds\n", address, port, connect_ERR_at_time_last_detected_error , pgsql_thread___shun_recovery_time_sec);
			}
		}
	}
}

void PgSQL_SrvC::shun_and_killall() {
	set_status(MYSQL_SERVER_STATUS_SHUNNED);
	shunned_automatic=true;
	shunned_and_kill_all_connections=true;
}

PgSQL_SrvC::~PgSQL_SrvC() {
	if (address) free(address);
	if (comment) free(comment);
	delete ConnectionsUsed;
	delete ConnectionsFree;
}

using metric_name = std::string;
using metric_help = std::string;
using metric_tags = std::map<std::string, std::string>;

using hg_counter_tuple =
	std::tuple<
		PgSQL_p_hg_counter::metric,
		metric_name,
		metric_help,
		metric_tags
	>;

using hg_gauge_tuple =
	std::tuple<
		PgSQL_p_hg_gauge::metric,
		metric_name,
		metric_help,
		metric_tags
	>;

using hg_dyn_counter_tuple =
	std::tuple<
		PgSQL_p_hg_dyn_counter::metric,
		metric_name,
		metric_help,
		metric_tags
	>;

using hg_dyn_gauge_tuple =
	std::tuple<
		PgSQL_p_hg_dyn_gauge::metric,
		metric_name,
		metric_help,
		metric_tags
	>;

using hg_counter_vector = std::vector<hg_counter_tuple>;
using hg_gauge_vector = std::vector<hg_gauge_tuple>;
using hg_dyn_counter_vector = std::vector<hg_dyn_counter_tuple>;
using hg_dyn_gauge_vector = std::vector<hg_dyn_gauge_tuple>;

/**
 * @brief Metrics map holding the metrics for the 'PgSQL_HostGroups_Manager' module.
 *
 * @note Many metrics in this map, share a common "id name", because
 *  they differ only by label, because of this, HELP is shared between
 *  them. For better visual identification of this groups they are
 *  sepparated using a line separator comment.
 */
const std::tuple<
	hg_counter_vector,
	hg_gauge_vector,
	hg_dyn_counter_vector,
	hg_dyn_gauge_vector
>
hg_metrics_map = std::make_tuple(
	hg_counter_vector {
		std::make_tuple (
			PgSQL_p_hg_counter::servers_table_version,
			"proxysql_servers_table_version_total",
			"Number of times the \"servers_table\" have been modified.",
			metric_tags {}
		),

		// ====================================================================
		std::make_tuple (
			PgSQL_p_hg_counter::server_connections_created,
			"proxysql_server_connections_total",
			"Total number of server connections (created|delayed|aborted).",
			metric_tags {
				{ "status", "created" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::server_connections_delayed,
			"proxysql_server_connections_total",
			"Total number of server connections (created|delayed|aborted).",
			metric_tags {
				{ "status", "delayed" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::server_connections_aborted,
			"proxysql_server_connections_total",
			"Total number of server connections (created|delayed|aborted).",
			metric_tags {
				{ "status", "aborted" },
				{ "protocol", "pgsql" }
			}
		),
		// ====================================================================

		// ====================================================================
		std::make_tuple (
			PgSQL_p_hg_counter::client_connections_created,
			"proxysql_client_connections_total",
			"Total number of client connections created.",
			metric_tags {
				{ "status", "created" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::client_connections_aborted,
			"proxysql_client_connections_total",
			"Total number of client failed connections (or closed improperly).",
			metric_tags {
				{ "status", "aborted" },
				{ "protocol", "pgsql" }
			}
		),
		// ====================================================================

		/*std::make_tuple(
			PgSQL_p_hg_counter::com_autocommit,
			"proxysql_com_autocommit_total",
			"Total queries autocommited.",
			metric_tags {}
		),
		std::make_tuple(
			PgSQL_p_hg_counter::com_autocommit_filtered,
			"proxysql_com_autocommit_filtered_total",
			"Total queries filtered autocommit.",
			metric_tags {}
		),*/
		std::make_tuple (
			PgSQL_p_hg_counter::com_rollback,
			"proxysql_com_rollback_total",
			"Total queries rollbacked.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::com_rollback_filtered,
			"proxysql_com_rollback_filtered_total",
			"Total queries filtered rollbacked.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::com_backend_reset_connection,
			"proxysql_com_backend_reset_connection_total",
			"Total backend_reset_connection queries backend.",
			metric_tags {}
		),
		/*std::make_tuple(
			PgSQL_p_hg_counter::com_backend_init_db,
			"proxysql_com_backend_init_db_total",
			"Total queries backend INIT DB.",
			metric_tags {}
		),*/
		std::make_tuple (
			PgSQL_p_hg_counter::com_backend_set_client_encoding,
			"proxysql_com_backend_set_client_encoding_total",
			"Total queries backend SET client_encoding.",
			metric_tags {}
		),
		/*std::make_tuple(
			PgSQL_p_hg_counter::com_frontend_init_db,
			"proxysql_com_frontend_init_db_total",
			"Total INIT DB queries frontend.",
			metric_tags {}
		),*/
		std::make_tuple (
			PgSQL_p_hg_counter::com_frontend_set_client_encoding,
			"proxysql_com_frontend_set_client_encoding_total",
			"Total SET client_encoding frontend queries.",
			metric_tags {}
		),
		/*std::make_tuple(
			PgSQL_p_hg_counter::com_frontend_use_db,
			"proxysql_com_frontend_use_db_total",
			"Total USE DB queries frontend.",
			metric_tags {}
		),*/
		std::make_tuple (
			PgSQL_p_hg_counter::com_commit_cnt,
			"proxysql_com_commit_cnt_total",
			"Total queries commit.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::com_commit_cnt_filtered,
			"proxysql_com_commit_cnt_filtered_total",
			"Total queries commit filtered.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::selects_for_update__autocommit0,
			"proxysql_selects_for_update__autocommit0_total",
			"Total queries that are SELECT for update or equivalent.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::access_denied_wrong_password,
			"proxysql_access_denied_wrong_password_total",
			"Total access denied \"wrong password\".",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::access_denied_max_connections,
			"proxysql_access_denied_max_connections_total",
			"Total access denied \"max connections\".",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::access_denied_max_user_connections,
			"proxysql_access_denied_max_user_connections_total",
			"Total access denied \"max user connections\".",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),

		// ====================================================================
		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_get,
			"proxysql_pghgm_pgconnpool_get_total",
			"The number of requests made to the connection pool.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_get_ok,
			"proxysql_pghgm_pgconnpool_get_ok_total",
			"The number of successful requests to the connection pool (i.e. where a connection was available).",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_get_ping,
			"proxysql_pghgm_myconnpool_get_ping_total",
			"The number of connections that were taken from the pool to run a ping to keep them alive.",
			metric_tags {}
		),
		// ====================================================================

		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_push,
			"proxysql_pghgm_pgconnpool_push_total",
			"The number of connections returned to the connection pool.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_reset,
			"proxysql_pghgm_pgconnpool_reset_total",
			"The number of connections that have been reset / re-initialized using \"COM_CHANGE_USER\"",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_counter::pghgm_pgconnpool_destroy,
			"proxysql_pghgm_pgconnpool_destroy_total",
			"The number of connections considered unhealthy and therefore closed.",
			metric_tags {}
		),

		// ====================================================================

		std::make_tuple (
			PgSQL_p_hg_counter::auto_increment_delay_multiplex,
			"proxysql_myhgm_auto_increment_multiplex_total",
			"The number of times that 'auto_increment_delay_multiplex' has been triggered.",
			metric_tags {}
		),
#if POLARDB_PROXY
		// PolarDB counters. These mirror stats_pgsql_global PolarDB_* rows and
		// are updated only during Prometheus/TSDB metric collection.
#define X(name, display_name, prom_name, help) \
		std::make_tuple ( \
			PgSQL_p_hg_counter::polardb_##name, \
			prom_name, \
			help, \
			metric_tags {} \
		),
		POLARDB_ALL_COUNTER_LIST(X)
#undef X
#endif // POLARDB_PROXY
	},
	// prometheus gauges
	hg_gauge_vector {
		std::make_tuple (
			PgSQL_p_hg_gauge::server_connections_connected,
			"proxysql_server_connections_connected",
			"Backend connections that are currently connected.",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_gauge::client_connections_connected,
			"proxysql_client_connections_connected",
			"Client connections that are currently connected.",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		)
#if POLARDB_PROXY
#define X(name, display_name, prom_name, help) \
		, std::make_tuple ( \
			PgSQL_p_hg_gauge::polardb_##name, \
			prom_name, \
			help, \
			metric_tags {} \
		)
		POLARDB_GAUGE_LIST(X)
#undef X
#endif // POLARDB_PROXY
	},
	// prometheus dynamic counters
	hg_dyn_counter_vector {
		// connection_pool
		// ====================================================================

		// ====================================================================
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::conn_pool_bytes_data_recv,
			"proxysql_connpool_data_bytes_total",
			"Amount of data (sent|recv) from the backend, excluding metadata.",
			metric_tags {
				{ "traffic_flow", "recv" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::conn_pool_bytes_data_sent,
			"proxysql_connpool_data_bytes_total",
			"Amount of data (sent|recv) from the backend, excluding metadata.",
			metric_tags {
				{ "traffic_flow", "sent" },
				{ "protocol", "pgsql" }
			}
		),
		// ====================================================================

		// ====================================================================
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::connection_pool_conn_err,
			"proxysql_connpool_conns_total",
			"How many connections have been tried to be established.",
			metric_tags {
				{ "status", "err" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::connection_pool_conn_ok,
			"proxysql_connpool_conns_total",
			"How many connections have been tried to be established.",
			metric_tags {
				{ "status", "ok" },
				{ "protocol", "pgsql" }
			}
		),
		// ====================================================================

		std::make_tuple (
			PgSQL_p_hg_dyn_counter::connection_pool_queries,
			"proxysql_connpool_conns_queries_total",
			"The number of queries routed towards this particular backend server.",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),
		// gtid
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::gtid_executed,
			"proxysql_gtid_executed_total",
			"Tracks the number of executed gtid per host and port.",
			metric_tags {}
		),
		// pgsql_error
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::proxysql_pgsql_error,
			"proxysql_pgsql_error_total",
			"Tracks the pgsql errors generated by proxysql.",
			metric_tags {}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_counter::pgsql_error,
			"pgsql_error_total",
			"Tracks the pgsql errors encountered.",
			metric_tags {}
		)
	},
	// prometheus dynamic gauges
	hg_dyn_gauge_vector {
		std::make_tuple (
			PgSQL_p_hg_dyn_gauge::connection_pool_conn_free,
			"proxysql_connpool_conns",
			"How many backend connections are currently (free|used).",
			metric_tags {
				{ "status", "free" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_gauge::connection_pool_conn_used,
			"proxysql_connpool_conns",
			"How many backend connections are currently (free|used).",
			metric_tags {
				{ "status", "used" },
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_gauge::connection_pool_latency_us,
			"proxysql_connpool_conns_latency_us",
			"The currently ping time in microseconds, as reported from Monitor.",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		),
		std::make_tuple (
			PgSQL_p_hg_dyn_gauge::connection_pool_status,
			"proxysql_connpool_conns_status",
			"The status of the backend server (1 - ONLINE, 2 - SHUNNED, 3 - OFFLINE_SOFT, 4 - OFFLINE_HARD).",
			metric_tags {
				{ "protocol", "pgsql" }
			}
		)
	}
);

PgSQL_HostGroups_Manager::PgSQL_HostGroups_Manager() {
	status.client_connections=0;
	status.client_connections_aborted=0;
	status.client_connections_created=0;
	status.server_connections_connected=0;
	status.server_connections_aborted=0;
	status.server_connections_created=0;
	status.server_connections_delayed=0;
	status.servers_table_version=0;
	pthread_mutex_init(&status.servers_table_version_lock, NULL);
	pthread_cond_init(&status.servers_table_version_cond, NULL);
	status.pgconnpoll_get=0;
	status.pgconnpoll_get_ok=0;
	status.pgconnpoll_get_ping=0;
	status.pgconnpoll_push=0;
	status.pgconnpoll_destroy=0;
	status.pgconnpoll_reset=0;
	status.autocommit_cnt=0;
	status.commit_cnt=0;
	status.rollback_cnt=0;
	status.autocommit_cnt_filtered=0;
	status.commit_cnt_filtered=0;
	status.rollback_cnt_filtered=0;
	status.backend_reset_connection=0;
	//status.backend_init_db=0;
	status.backend_set_client_encoding=0;
	//status.frontend_init_db=0;
	status.frontend_set_client_encoding=0;
	//status.frontend_use_db=0;
	status.access_denied_wrong_password=0;
	status.access_denied_max_connections=0;
	status.access_denied_max_user_connections=0;
	status.select_for_update_or_equivalent=0;
	status.auto_increment_delay_multiplex=0;
#if 0
	pthread_mutex_init(&readonly_mutex, NULL);
	pthread_mutex_init(&lock, NULL);
	admindb=NULL;	// initialized only if needed
	mydb=new SQLite3DB();
#endif // 0
#ifdef DEBUG
	mydb->open((char *)"file:mem_mydb?mode=memory&cache=shared", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX);
#else
	mydb->open((char *)"file:mem_mydb?mode=memory", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX);
#endif /* DEBUG */
	mydb->execute(MYHGM_PgSQL_SERVERS);
	mydb->execute(MYHGM_PgSQL_SERVERS_INCOMING);
	mydb->execute(MYHGM_PgSQL_SERVERS_SSL_PARAMS);
	mydb->execute(MYHGM_PgSQL_REPLICATION_HOSTGROUPS);
	mydb->execute(MYHGM_PgSQL_HOSTGROUP_ATTRIBUTES);
	mydb->execute("CREATE INDEX IF NOT EXISTS idx_pgsql_servers_hostname_port ON pgsql_servers (hostname,port)");
	MyHostGroups=new PtrArray();
	runtime_pgsql_servers=NULL;
	incoming_replication_hostgroups=NULL;
	incoming_hostgroup_attributes = NULL;
	incoming_pgsql_servers_v2 = NULL;
	pgsql_servers_to_monitor = NULL;

	{
		static const char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		rand_del[0] = '-';
		for (int i = 1; i < 6; i++) {
			rand_del[i] = alphanum[rand_fast() % (sizeof(alphanum) - 1)];
		}
		rand_del[6] = '-';
		rand_del[7] = 0;
	}
	pthread_mutex_init(&pgsql_errors_mutex, NULL);

	// Initialize prometheus metrics
	init_prometheus_counter_array<PgSQL_hg_metrics_map_idx, PgSQL_p_hg_counter>(hg_metrics_map, this->status.p_counter_array);
	init_prometheus_gauge_array<PgSQL_hg_metrics_map_idx, PgSQL_p_hg_gauge>(hg_metrics_map, this->status.p_gauge_array);
	init_prometheus_dyn_counter_array<PgSQL_hg_metrics_map_idx, PgSQL_p_hg_dyn_counter>(hg_metrics_map, this->status.p_dyn_counter_array);
	init_prometheus_dyn_gauge_array<PgSQL_hg_metrics_map_idx, PgSQL_p_hg_dyn_gauge>(hg_metrics_map, this->status.p_dyn_gauge_array);

	pthread_mutex_init(&pgsql_errors_mutex, NULL);
#if POLARDB_PROXY
	pthread_rwlock_init(&polardb_fast_topology_lock, NULL);
	polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
	polardb_reader_pool_.reset(new PgSQL_PolarDB_ReaderPool(this));
#endif // POLARDB_PROXY
}

void PgSQL_HostGroups_Manager::init() {
#if POLARDB_PROXY
	if (polardb_reader_pool_) {
		polardb_reader_pool_->start();
	}
#endif // POLARDB_PROXY
}

void PgSQL_HostGroups_Manager::shutdown() {
#if POLARDB_PROXY
	shutdown_split_warmup_thread();
	polardb_clear_snapshots_for_shutdown();
#endif // POLARDB_PROXY
	pthread_mutex_lock(&pgsql_errors_mutex);
	pgsql_errors_umap.clear();
	pthread_mutex_unlock(&pgsql_errors_mutex);
}

PgSQL_HostGroups_Manager::~PgSQL_HostGroups_Manager() {
#if POLARDB_PROXY
	shutdown();
#endif // POLARDB_PROXY
	while (MyHostGroups->len) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->remove_index_fast(0);
		delete myhgc;
	}
	delete MyHostGroups;
	delete mydb;
	if (admindb) {
		delete admindb;
	}
#if POLARDB_PROXY
	pthread_rwlock_destroy(&polardb_fast_topology_lock);
#endif // POLARDB_PROXY
	pthread_mutex_destroy(&lock);
}

#if POLARDB_PROXY

void PgSQL_HostGroups_Manager::shutdown_split_warmup_thread() {
	if (polardb_reader_pool_) {
		polardb_reader_pool_->shutdown();
	}
}

#endif // POLARDB_PROXY

void PgSQL_HostGroups_Manager::p_update_pgsql_error_counter(p_pgsql_error_type err_type, unsigned int hid, char* address, uint16_t port, unsigned int code) {
	PgSQL_p_hg_dyn_counter::metric metric = PgSQL_p_hg_dyn_counter::pgsql_error;
	if (err_type == p_pgsql_error_type::proxysql) {
		metric = PgSQL_p_hg_dyn_counter::proxysql_pgsql_error;
	}

	std::string s_hostgroup = std::to_string(hid);
	std::string s_address = std::string(address);
	std::string s_port = std::to_string(port);
	// TODO: Create switch here to classify error codes
	std::string s_code = std::to_string(code);
	std::string metric_id = s_hostgroup + ":" + address + ":" + s_port + ":" + s_code;
	std::map<string, string> metric_labels {
		{ "hostgroup", s_hostgroup },
		{ "address", address },
		{ "port", s_port },
		{ "code", s_code }
	};

	pthread_mutex_lock(&pgsql_errors_mutex);

	p_inc_map_counter(
		status.p_pgsql_errors_map,
		status.p_dyn_counter_array[metric],
		metric_id,
		metric_labels
	);



	pthread_mutex_unlock(&pgsql_errors_mutex);
}

void PgSQL_HostGroups_Manager::wait_servers_table_version(unsigned v, unsigned w) {
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	//ts.tv_sec += w;
	unsigned int i = 0;
	int rc = 0;
	pthread_mutex_lock(&status.servers_table_version_lock);
	while ((rc == 0 || rc == ETIMEDOUT) && (i < w) && (__sync_fetch_and_add(&glovars.shutdown,0)==0) && (__sync_fetch_and_add(&status.servers_table_version,0) < v)) {
		i++;
		ts.tv_sec += 1;
		rc = pthread_cond_timedwait( &status.servers_table_version_cond, &status.servers_table_version_lock, &ts);
	}
	pthread_mutex_unlock(&status.servers_table_version_lock);
}

unsigned int PgSQL_HostGroups_Manager::get_servers_table_version() {
	return __sync_fetch_and_add(&status.servers_table_version,0);
}

// we always assume that the calling thread has acquired a rdlock()
int PgSQL_HostGroups_Manager::servers_add(SQLite3_result *resultset) {
	if (resultset==NULL) {
		return 0;
	}
	int rc;
	mydb->execute("DELETE FROM pgsql_servers_incoming");
	char *query1=(char *)"INSERT INTO pgsql_servers_incoming VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)";
	std::string query32s = "INSERT INTO pgsql_servers_incoming VALUES " + generate_multi_rows_query(32,11);
	char *query32 = (char *)query32s.c_str();
	auto [rc1, statement1_unique] = mydb->prepare_v2(query1);
	ASSERT_SQLITE_OK(rc1, mydb);
	auto [rc2, statement32_unique] = mydb->prepare_v2(query32);
	ASSERT_SQLITE_OK(rc2, mydb);
	sqlite3_stmt *statement1 = statement1_unique.get();
	sqlite3_stmt *statement32 = statement32_unique.get();
	MySerStatus status1=MYSQL_SERVER_STATUS_ONLINE;
	int row_idx=0;
	int max_bulk_row_idx=resultset->rows_count/32;
	max_bulk_row_idx=max_bulk_row_idx*32;
	for (std::vector<SQLite3_row *>::iterator it = resultset->rows.begin() ; it != resultset->rows.end(); ++it) {
		SQLite3_row *r1=*it;
		status1=MYSQL_SERVER_STATUS_ONLINE;
		if (strcasecmp(r1->fields[3],"ONLINE")) {
			if (!strcasecmp(r1->fields[3],"SHUNNED")) {
				status1=MYSQL_SERVER_STATUS_SHUNNED;
			} else {
				if (!strcasecmp(r1->fields[3],"OFFLINE_SOFT")) {
					status1=MYSQL_SERVER_STATUS_OFFLINE_SOFT;
				} else {
					if (!strcasecmp(r1->fields[3],"OFFLINE_HARD")) {
						status1=MYSQL_SERVER_STATUS_OFFLINE_HARD;
					}
				}
			}
		}
		int idx=row_idx%32;
		if (row_idx<max_bulk_row_idx) { // bulk
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+1, atoi(r1->fields[0])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_text)(statement32,  (idx*11)+2, r1->fields[1], -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+3, atoi(r1->fields[2])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+4, atoi(r1->fields[4])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+5, status1); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+6, atoi(r1->fields[5])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+7, atoi(r1->fields[6])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+8, atoi(r1->fields[7])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+9, atoi(r1->fields[8])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement32, (idx*11)+10, atoi(r1->fields[9])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_text)(statement32,  (idx*11)+11, r1->fields[10], -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
			if (idx==31) {
				SAFE_SQLITE3_STEP2(statement32);
				rc=(*proxy_sqlite3_clear_bindings)(statement32); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_reset)(statement32); ASSERT_SQLITE_OK(rc, mydb);
			}
		} else { // single row
			rc=(*proxy_sqlite3_bind_int64)(statement1, 1, atoi(r1->fields[0])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_text)(statement1,  2, r1->fields[1], -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 3, atoi(r1->fields[2])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 4, atoi(r1->fields[4])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 5, status1); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 6, atoi(r1->fields[5])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 7, atoi(r1->fields[6])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 8, atoi(r1->fields[7])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 9, atoi(r1->fields[8])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_int64)(statement1, 10, atoi(r1->fields[9])); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_bind_text)(statement1,  11, r1->fields[10], -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
			SAFE_SQLITE3_STEP2(statement1);
			rc=(*proxy_sqlite3_clear_bindings)(statement1); ASSERT_SQLITE_OK(rc, mydb);
			rc=(*proxy_sqlite3_reset)(statement1); ASSERT_SQLITE_OK(rc, mydb);
		}
		row_idx++;
	}
	return 0;
}

void PgSQL_HostGroups_Manager::CUCFT1(
	SpookyHash& myhash, bool& init, const string& TableName, const string& ColumnName, uint64_t& raw_checksum
) {
	char *error=NULL;
	int cols=0;
	int affected_rows=0;
	SQLite3_result *resultset=NULL;
	string query = "SELECT * FROM " + TableName + " ORDER BY " + ColumnName;
	mydb->execute_statement(query.c_str(), &error , &cols , &affected_rows , &resultset);
	if (resultset) {
		if (resultset->rows_count) {
			if (init == false) {
				init = true;
				myhash.Init(19,3);
			}
			uint64_t hash1_ = resultset->raw_checksum();
			raw_checksum = hash1_;
			myhash.Update(&hash1_, sizeof(hash1_));
			proxy_info("Checksum for table %s is 0x%lX\n", TableName.c_str(), hash1_);
		}
		delete resultset;
	} else {
		proxy_info("Checksum for table %s is 0x%lX\n", TableName.c_str(), (long unsigned int)0);
	}
}

void PgSQL_HostGroups_Manager::commit_update_checksums_from_tables(SpookyHash& myhash, bool& init) {
	// Always reset the current table values before recomputing
	for (size_t i = 0; i < table_resultset_checksum.size(); i++) {
		if (i != HGM_TABLES::PgSQL_SERVERS && i != HGM_TABLES::PgSQL_SERVERS_V2) {
			table_resultset_checksum[i] = 0;
		}
	}

	CUCFT1(myhash,init,"pgsql_replication_hostgroups","writer_hostgroup", table_resultset_checksum[HGM_TABLES::PgSQL_REPLICATION_HOSTGROUPS]);
	CUCFT1(myhash,init,"pgsql_hostgroup_attributes","hostgroup_id", table_resultset_checksum[HGM_TABLES::PgSQL_HOSTGROUP_ATTRIBUTES]);
	CUCFT1(myhash,init,"pgsql_servers_ssl_params","hostname,port,username", table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS_SSL_PARAMS]);
}

/**
 * @brief This code updates the 'hostgroup_server_mapping' table with the most recent pgsql_servers and pgsql_replication_hostgroups 
 *	  records while utilizing checksums to prevent unnecessary updates.
 * 
 * IMPORTANT: Make sure wrlock() is called before calling this method.
 * 
*/
void PgSQL_HostGroups_Manager::update_hostgroup_manager_mappings() {

	if (hgsm_pgsql_servers_checksum != table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS] ||
		hgsm_pgsql_replication_hostgroups_checksum != table_resultset_checksum[HGM_TABLES::PgSQL_REPLICATION_HOSTGROUPS])
	{
		proxy_info("Rebuilding 'Hostgroup_Manager_Mapping' due to checksums change - pgsql_servers { old: 0x%lX, new: 0x%lX }, pgsql_replication_hostgroups { old:0x%lX, new:0x%lX }\n",
			hgsm_pgsql_servers_checksum, table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS],
			hgsm_pgsql_replication_hostgroups_checksum, table_resultset_checksum[HGM_TABLES::PgSQL_REPLICATION_HOSTGROUPS]);

		char* error = NULL;
		int cols = 0;
		int affected_rows = 0;
		SQLite3_result* resultset = NULL;

		hostgroup_server_mapping.clear();

		const char* query = "SELECT DISTINCT hostname, port, '1' is_writer, status, reader_hostgroup, writer_hostgroup, mem_pointer FROM pgsql_replication_hostgroups JOIN pgsql_servers ON hostgroup_id=writer_hostgroup WHERE status<>3 \
							 UNION \
							 SELECT DISTINCT hostname, port, '0' is_writer, status, reader_hostgroup, writer_hostgroup, mem_pointer FROM pgsql_replication_hostgroups JOIN pgsql_servers ON hostgroup_id=reader_hostgroup WHERE status<>3 \
							 ORDER BY hostname, port";

		mydb->execute_statement(query, &error, &cols, &affected_rows, &resultset);

		if (resultset && resultset->rows_count) {
			std::string fetched_server_id;
			HostGroup_Server_Mapping* fetched_server_mapping = NULL;

			for (std::vector<SQLite3_row*>::iterator it = resultset->rows.begin(); it != resultset->rows.end(); ++it) {
				SQLite3_row* r = *it;

				const std::string& server_id = std::string(r->fields[0]) + ":::" + r->fields[1];

				if (fetched_server_mapping == NULL || server_id != fetched_server_id) {

					auto itr = hostgroup_server_mapping.find(server_id);

					if (itr == hostgroup_server_mapping.end()) {
						std::unique_ptr<HostGroup_Server_Mapping> server_mapping(new HostGroup_Server_Mapping(this));
						fetched_server_mapping = server_mapping.get();
						hostgroup_server_mapping.insert( std::pair<std::string,std::unique_ptr<PgSQL_HostGroups_Manager::HostGroup_Server_Mapping>> {
															server_id, std::move(server_mapping)
															} );
					} else {
						fetched_server_mapping = itr->second.get();
					}

					fetched_server_id = server_id;
				}

				HostGroup_Server_Mapping::Node node;
				//node.server_status = static_cast<MySerStatus>(atoi(r->fields[3]));
				node.reader_hostgroup_id = atoi(r->fields[4]);
				node.writer_hostgroup_id = atoi(r->fields[5]);
				node.srv = reinterpret_cast<PgSQL_SrvC*>(atoll(r->fields[6]));

				HostGroup_Server_Mapping::Type type = (r->fields[2] && r->fields[2][0] == '1') ? HostGroup_Server_Mapping::Type::WRITER : HostGroup_Server_Mapping::Type::READER;
				fetched_server_mapping->add(type, node);
			}
		}
		delete resultset;

		hgsm_pgsql_servers_checksum = table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS];
		hgsm_pgsql_replication_hostgroups_checksum = table_resultset_checksum[HGM_TABLES::PgSQL_REPLICATION_HOSTGROUPS];
	}
}

/**
 * @brief Generates a resultset holding the current Admin 'runtime_pgsql_servers' as reported by Admin.
 * @details Requires caller to hold the mutex 'PgSQL_HostGroups_Manager::wrlock'.
 * @param mydb The db in which to perform the query, typically 'PgSQL_HostGroups_Manager::mydb'.
 * @return An SQLite3 resultset for the query 'MYHGM_GEN_ADMIN_RUNTIME_SERVERS'.
 */
unique_ptr<SQLite3_result> get_admin_runtime_pgsql_servers(SQLite3DB* mydb) {
	char* error = nullptr;
	int cols = 0;
	int affected_rows = 0;
	SQLite3_result* resultset = nullptr;

	mydb->execute_statement(PGHGM_GEN_CLUSTER_ADMIN_RUNTIME_SERVERS, &error, &cols, &affected_rows, &resultset);

	if (error) {
		proxy_error("SQLite3 query generating 'runtime_pgsql_servers' resultset failed with error '%s'\n", error);
		assert(0);
	}

	return unique_ptr<SQLite3_result>(resultset);
}

/**
 * @brief Generates a resultset with holding the current 'pgsql_servers_v2' table.
 * @details Requires caller to hold the mutex 'ProxySQL_Admin::mysql_servers_wrlock'.
 * @return A resulset holding 'pgsql_servers_v2'.
 */
unique_ptr<SQLite3_result> get_pgsql_servers_v2() {
	char* error = nullptr;
	int cols = 0;
	int affected_rows = 0;
	SQLite3_result* resultset = nullptr;

	if (GloAdmin && GloAdmin->admindb) {
		GloAdmin->admindb->execute_statement(
			PGHGM_GEN_CLUSTER_ADMIN_PGSQL_SERVERS, &error, &cols, &affected_rows, &resultset
		);
	}

	return unique_ptr<SQLite3_result>(resultset);
}

static void update_glovars_checksum_with_peers(
	ProxySQL_Checksum_Value& module_checksum,
	const string& new_checksum,
	const string& peer_checksum_value,
	time_t new_epoch,
	time_t peer_checksum_epoch,
	bool update_version
) {
	module_checksum.set_checksum(const_cast<char*>(new_checksum.c_str()));

	if (update_version)
		module_checksum.version++;

	bool computed_checksum_matches =
		peer_checksum_value != "" && module_checksum.checksum == peer_checksum_value;

	if (peer_checksum_epoch != 0 && computed_checksum_matches) {
		module_checksum.epoch = peer_checksum_epoch;
	} else {
		module_checksum.epoch = new_epoch;
	}
}

/**
 * @brief Updates the global 'pgsql_servers' module checksum.
 * @details If the new computed checksum matches the supplied 'cluster_checksum', the epoch used for the
 *  checksum is the supplied epoch instead of current time. This way we ensure the preservation of the
 *  checksum and epoch fetched from the ProxySQL cluster peer node.
 *
 *  IMPORTANT: This function also generates a new 'global_checksum'. This is because everytime
 *  'runtime_pgsql_servers' change, updating the global checksum is unconditional.
 * @param new_checksum The new computed checksum for 'runtime_pgsql_servers'.
 * @param peer_checksum A checksum fetched from another ProxySQL cluster node, holds the checksum value
 *  and its epoch. Should be empty if no remote checksum is being considered.
 * @param epoch The epoch to be preserved in case the supplied 'peer_checksum' matches the new computed
 *  checksum.
 */
static void update_glovars_pgsql_servers_checksum(
	const string& new_checksum,
	const runtime_pgsql_servers_checksum_t& peer_checksum = {},
	bool update_version = false
) {
	time_t new_epoch = time(NULL);

	update_glovars_checksum_with_peers(
		GloVars.checksums_values.pgsql_servers,
		new_checksum,
		peer_checksum.value,
		new_epoch,
		peer_checksum.epoch,
		update_version
	);

	GloVars.checksums_values.updates_cnt++;
	GloVars.generate_global_checksum();
	GloVars.epoch_version = new_epoch;
}

/**
 * @brief Updates the global 'pgsql_servers_v2' module checksum.
 * @details Unlike 'update_glovars_pgsql_servers_checksum' this function doesn't generate a new
 *  'global_checksum'. It's caller responsibility to ensure that 'global_checksum' is updated. 
 * @param new_checksum The new computed checksum for 'pgsql_servers_v2'.
 * @param peer_checksum A checksum fetched from another ProxySQL cluster node, holds the checksum value
 *  and its epoch. Should be empty if no remote checksum is being considered.
 * @param epoch The epoch to be preserved in case the supplied 'peer_checksum' matches the new computed
 *  checksum.
 */
static void update_glovars_pgsql_servers_v2_checksum(
	const string& new_checksum,
	const pgsql_servers_v2_checksum_t& peer_checksum = {},
	bool update_version = false
) {
	time_t new_epoch = time(NULL);

	update_glovars_checksum_with_peers(
		GloVars.checksums_values.pgsql_servers_v2,
		new_checksum,
		peer_checksum.value,
		new_epoch,
		peer_checksum.epoch,
		update_version
	);
}

uint64_t PgSQL_HostGroups_Manager::commit_update_checksum_from_pgsql_servers(SQLite3_result* runtime_pgsql_servers) {
	mydb->execute("DELETE FROM pgsql_servers");
	generate_pgsql_servers_table();

	if (runtime_pgsql_servers == nullptr) {
		unique_ptr<SQLite3_result> resultset { get_admin_runtime_pgsql_servers(mydb) };
		save_runtime_pgsql_servers(resultset.release());
	} else {
		save_runtime_pgsql_servers(runtime_pgsql_servers);
	}

	uint64_t raw_checksum = this->runtime_pgsql_servers ? this->runtime_pgsql_servers->raw_checksum() : 0;
	table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS] = raw_checksum;

	return raw_checksum;
}

uint64_t PgSQL_HostGroups_Manager::commit_update_checksum_from_pgsql_servers_v2(SQLite3_result* pgsql_servers_v2) {
	if (pgsql_servers_v2 == nullptr) {
		unique_ptr<SQLite3_result> resultset { get_pgsql_servers_v2() };
		save_pgsql_servers_v2(resultset.release());
	} else {
		save_pgsql_servers_v2(pgsql_servers_v2);
	}

	uint64_t raw_checksum = this->incoming_pgsql_servers_v2 ? this->incoming_pgsql_servers_v2->raw_checksum() : 0;
	table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS_V2] = raw_checksum;

	return raw_checksum;
}

std::string PgSQL_HostGroups_Manager::gen_global_pgsql_servers_v2_checksum(uint64_t servers_v2_hash) {
	bool init = false;
	SpookyHash global_hash {};

	if (servers_v2_hash != 0) {
		if (init == false) {
			init = true;
			global_hash.Init(19, 3);
		}

		global_hash.Update(&servers_v2_hash, sizeof(servers_v2_hash));
	}

	commit_update_checksums_from_tables(global_hash, init);

	uint64_t hash_1 = 0, hash_2 = 0;
	if (init) {
		global_hash.Final(&hash_1,&hash_2);
	}

	string mysrvs_checksum { get_checksum_from_hash(hash_1) };
	return mysrvs_checksum;
}

bool PgSQL_HostGroups_Manager::commit(
	const peer_runtime_pgsql_servers_t& peer_runtime_pgsql_servers,
	const peer_pgsql_servers_v2_t& peer_pgsql_servers_v2,
	bool only_commit_runtime_pgsql_servers,
	bool update_version
) {
	// if only_commit_runtime_pgsql_servers is true, pgsql_servers_v2 resultset will not be entertained and will cause memory leak.
	if (only_commit_runtime_pgsql_servers) {
		proxy_info("Generating runtime pgsql servers records only.\n");
	} else {
		proxy_info("Generating runtime pgsql servers and pgsql servers v2 records.\n");
	}

	unsigned long long curtime1=monotonic_time();
	wrlock();
	bool pgsql_servers_mutated = false;
	// purge table
	purge_pgsql_servers_table();

	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_servers\n");
	mydb->execute("DELETE FROM pgsql_servers");
	generate_pgsql_servers_table();

	char *error=NULL;
	int cols=0;
	int affected_rows=0;
	SQLite3_result *resultset=NULL;
	if (GloPTH->variables.hostgroup_manager_verbose) {
		mydb->execute_statement((char *)"SELECT * FROM pgsql_servers_incoming", &error , &cols , &affected_rows , &resultset);
		if (error) {
			proxy_error("Error on read from pgsql_servers_incoming : %s\n", error);
		} else {
			if (resultset) {
				proxy_info("Dumping pgsql_servers_incoming\n");
				resultset->dump_to_stderr();
			}
		}
		if (resultset) { delete resultset; resultset=NULL; }
	}
	char *query=NULL;
	query=(char *)"SELECT mem_pointer, t1.hostgroup_id, t1.hostname, t1.port FROM pgsql_servers t1 LEFT OUTER JOIN pgsql_servers_incoming t2 ON (t1.hostgroup_id=t2.hostgroup_id AND t1.hostname=t2.hostname AND t1.port=t2.port) WHERE t2.hostgroup_id IS NULL";
	mydb->execute_statement(query, &error , &cols , &affected_rows , &resultset);
	if (error) {
		proxy_error("Error on %s : %s\n", query, error);
	} else {
		if (GloPTH->variables.hostgroup_manager_verbose) {
			proxy_info("Dumping pgsql_servers LEFT JOIN pgsql_servers_incoming\n");
			resultset->dump_to_stderr();
		}
		for (std::vector<SQLite3_row *>::iterator it = resultset->rows.begin() ; it != resultset->rows.end(); ++it) {
			SQLite3_row *r=*it;
			long long ptr=atoll(r->fields[0]);
			proxy_warning("Removed server at address %lld, hostgroup %s, address %s port %s. Setting status OFFLINE HARD and immediately dropping all free connections. Used connections will be dropped when trying to use them\n", ptr, r->fields[1], r->fields[2], r->fields[3]);
			pgsql_servers_mutated = true;
			PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)ptr;
			mysrvc->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
			mysrvc->ConnectionsFree->drop_all_connections();
			char *q1=(char *)"DELETE FROM pgsql_servers WHERE mem_pointer=%lld";
			char *q2=(char *)malloc(strlen(q1)+32);
			sprintf(q2,q1,ptr);
			mydb->execute(q2);
			free(q2);
		}
	}
	if (resultset) { delete resultset; resultset=NULL; }

	// This seems unnecessary. Removed as part of issue #829
	//mydb->execute("DELETE FROM pgsql_servers");
	//generate_pgsql_servers_table();

	mydb->execute("INSERT OR IGNORE INTO pgsql_servers(hostgroup_id, hostname, port, weight, status, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment) SELECT hostgroup_id, hostname, port, weight, status, compression, max_connections, max_replication_lag, use_ssl, max_latency_ms, comment FROM pgsql_servers_incoming");

	// SELECT FROM pgsql_servers whatever is not identical in pgsql_servers_incoming, or where mem_pointer=0 (where there is no pointer yet)
	query=(char *)"SELECT t1.*, t2.weight, t2.status, t2.compression, t2.max_connections, t2.max_replication_lag, t2.use_ssl, t2.max_latency_ms, t2.comment FROM pgsql_servers t1 JOIN pgsql_servers_incoming t2 ON (t1.hostgroup_id=t2.hostgroup_id AND t1.hostname=t2.hostname AND t1.port=t2.port) WHERE mem_pointer=0 OR t1.weight<>t2.weight OR t1.status<>t2.status OR t1.compression<>t2.compression OR t1.max_connections<>t2.max_connections OR t1.max_replication_lag<>t2.max_replication_lag OR t1.use_ssl<>t2.use_ssl OR t1.max_latency_ms<>t2.max_latency_ms or t1.comment<>t2.comment";
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "%s\n", query);
	mydb->execute_statement(query, &error , &cols , &affected_rows , &resultset);
	if (error) {
		proxy_error("Error on %s : %s\n", query, error);
	} else {

		if (GloPTH->variables.hostgroup_manager_verbose) {
			proxy_info("Dumping pgsql_servers JOIN pgsql_servers_incoming\n");
			resultset->dump_to_stderr();
		}
		// optimization #829
		int rc;
		char *query1=(char *)"UPDATE pgsql_servers SET mem_pointer = ?1 WHERE hostgroup_id = ?2 AND hostname = ?3 AND port = ?4";
		auto [rc1, statement1_unique] = mydb->prepare_v2(query1);
		ASSERT_SQLITE_OK(rc1, mydb);
		char *query2=(char *)"UPDATE pgsql_servers SET weight = ?1 , status = ?2 , compression = ?3 , max_connections = ?4 , max_replication_lag = ?5 , use_ssl = ?6 , max_latency_ms = ?7 , comment = ?8 WHERE hostgroup_id = ?9 AND hostname = ?10 AND port = ?11";
		auto [rc2, statement2_unique] = mydb->prepare_v2(query2);
		ASSERT_SQLITE_OK(rc2, mydb);
		sqlite3_stmt *statement1 = statement1_unique.get();
		sqlite3_stmt *statement2 = statement2_unique.get();

		for (std::vector<SQLite3_row *>::iterator it = resultset->rows.begin() ; it != resultset->rows.end(); ++it) {
			SQLite3_row *r=*it;
			long long ptr=atoll(r->fields[11]); // increase this index every time a new column is added
			proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Server %s:%d , weight=%d, status=%d, mem_pointer=%llu, hostgroup=%d, compression=%d\n", r->fields[1], atoi(r->fields[2]), atoi(r->fields[3]), (MySerStatus) atoi(r->fields[4]), ptr, atoi(r->fields[0]), atoi(r->fields[5]));
			//fprintf(stderr,"%lld\n", ptr);
			if (ptr==0) {
				pgsql_servers_mutated = true;
				if (GloPTH->variables.hostgroup_manager_verbose) {
					proxy_info("Creating new server in HG %d : %s:%d , weight=%d, status=%d\n", atoi(r->fields[0]), r->fields[1], atoi(r->fields[2]), atoi(r->fields[3]), (MySerStatus) atoi(r->fields[4]));
				}
				PgSQL_SrvC *mysrvc=new PgSQL_SrvC(r->fields[1], atoi(r->fields[2]), atoi(r->fields[3]), (MySerStatus) atoi(r->fields[4]), atoi(r->fields[5]), atoi(r->fields[6]), atoi(r->fields[7]), atoi(r->fields[8]), atoi(r->fields[9]), r->fields[10]); // add new fields here if adding more columns in pgsql_servers
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Adding new server %s:%d , weight=%d, status=%d, mem_ptr=%p into hostgroup=%d\n", r->fields[1], atoi(r->fields[2]), atoi(r->fields[3]), (MySerStatus) atoi(r->fields[4]), mysrvc, atoi(r->fields[0]));
				add(mysrvc,atoi(r->fields[0]));
				ptr=(uintptr_t)mysrvc;
				rc=(*proxy_sqlite3_bind_int64)(statement1, 1, ptr); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_bind_int64)(statement1, 2, atoi(r->fields[0])); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_bind_text)(statement1, 3,  r->fields[1], -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_bind_int64)(statement1, 4, atoi(r->fields[2])); ASSERT_SQLITE_OK(rc, mydb);
				SAFE_SQLITE3_STEP2(statement1);
				rc=(*proxy_sqlite3_clear_bindings)(statement1); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_reset)(statement1); ASSERT_SQLITE_OK(rc, mydb);
			} else {
				bool run_update=false;
				bool server_mutated=false;
				PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)ptr;
				// carefully increase the 2nd index by 1 for every new column added

				if (atoi(r->fields[3])!=atoi(r->fields[12])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Changing weight for server %d:%s:%d (%s:%d) from %d (%ld) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[3]) , mysrvc->weight , atoi(r->fields[12]));
					mysrvc->weight=atoi(r->fields[12]);
				}
				if (atoi(r->fields[4])!=atoi(r->fields[13])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing status for server %d:%s:%d (%s:%d) from %d (%d) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[4]) , mysrvc->status , atoi(r->fields[13]));
					mysrvc->set_status((MySerStatus)atoi(r->fields[13]));
					if (mysrvc->status==MYSQL_SERVER_STATUS_SHUNNED) {
						mysrvc->shunned_automatic=false;
					}
				}
				if (atoi(r->fields[5])!=atoi(r->fields[14])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing compression for server %d:%s:%d (%s:%d) from %d (%d) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[5]) , mysrvc->compression , atoi(r->fields[14]));
					mysrvc->compression=atoi(r->fields[14]);
				}
				if (atoi(r->fields[6])!=atoi(r->fields[15])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
					proxy_info("Changing max_connections for server %d:%s:%d (%s:%d) from %d (%ld) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[6]) , mysrvc->max_connections , atoi(r->fields[15]));
					mysrvc->max_connections=atoi(r->fields[15]);
				}
				if (atoi(r->fields[7])!=atoi(r->fields[16])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing max_replication_lag for server %u:%s:%d (%s:%d) from %d (%d) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[7]) , mysrvc->max_replication_lag , atoi(r->fields[16]));
					mysrvc->max_replication_lag=atoi(r->fields[16]);
					if (mysrvc->max_replication_lag == 0) { // we just changed it to 0
						if (mysrvc->status == MYSQL_SERVER_STATUS_SHUNNED_REPLICATION_LAG) {
							// the server is currently shunned due to replication lag
							// but we reset max_replication_lag to 0
							// therefore we immediately reset the status too
							mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);
						}
					}
				}
				if (atoi(r->fields[8])!=atoi(r->fields[17])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing use_ssl for server %d:%s:%d (%s:%d) from %d (%d) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[8]) , mysrvc->use_ssl , atoi(r->fields[17]));
					mysrvc->use_ssl=atoi(r->fields[17]);
				}
				if (atoi(r->fields[9])!=atoi(r->fields[18])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing max_latency_ms for server %d:%s:%d (%s:%d) from %d (%d) to %d\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), atoi(r->fields[9]) , mysrvc->max_latency_us/1000 , atoi(r->fields[18]));
					mysrvc->max_latency_us=1000*atoi(r->fields[18]);
				}
				if (strcmp(r->fields[10],r->fields[19])) {
					server_mutated=true;
					if (GloPTH->variables.hostgroup_manager_verbose)
						proxy_info("Changing comment for server %d:%s:%d (%s:%d) from '%s' to '%s'\n" , mysrvc->myhgc->hid , mysrvc->address, mysrvc->port, r->fields[1], atoi(r->fields[2]), r->fields[10], r->fields[19]);
					free(mysrvc->comment);
					mysrvc->comment=strdup(r->fields[19]);
				}
				if (server_mutated) {
					pgsql_servers_mutated=true;
				}
				if (run_update) {
					rc=(*proxy_sqlite3_bind_int64)(statement2, 1, mysrvc->weight); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 2, mysrvc->status); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 3, mysrvc->compression); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 4, mysrvc->max_connections); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 5, mysrvc->max_replication_lag); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 6, mysrvc->use_ssl); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 7, mysrvc->max_latency_us/1000); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_text)(statement2,  8,  mysrvc->comment, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 9, mysrvc->myhgc->hid); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_text)(statement2,  10,  mysrvc->address, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement2, 11, mysrvc->port); ASSERT_SQLITE_OK(rc, mydb);
					SAFE_SQLITE3_STEP2(statement2);
					rc=(*proxy_sqlite3_clear_bindings)(statement2); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_reset)(statement2); ASSERT_SQLITE_OK(rc, mydb);
				}
			}
		}
		// RAII auto-finalizes statement1 and statement2
	}
	if (resultset) { delete resultset; resultset=NULL; }
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_servers_incoming\n");
	mydb->execute("DELETE FROM pgsql_servers_incoming");

	string global_checksum_v2 {};
	if (only_commit_runtime_pgsql_servers == false) {
		// replication
		if (incoming_replication_hostgroups) { // this IF is extremely important, otherwise replication hostgroups may disappear
			generate_pgsql_replication_hostgroups_table();
		}

		// hostgroup attributes
		if (incoming_hostgroup_attributes) {
			proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_hostgroup_attributes\n");
			mydb->execute("DELETE FROM pgsql_hostgroup_attributes");
			generate_pgsql_hostgroup_attributes_table();
		}

		// SSL params
		if (incoming_pgsql_servers_ssl_params) {
			proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_servers_ssl_params\n");
			mydb->execute("DELETE FROM pgsql_servers_ssl_params");
			generate_pgsql_servers_ssl_params_table();
		}

		uint64_t new_hash = commit_update_checksum_from_pgsql_servers_v2(peer_pgsql_servers_v2.resultset);

		{
			const string new_checksum { get_checksum_from_hash(new_hash) };
			proxy_info("Checksum for table %s is %s\n", "pgsql_servers_v2", new_checksum.c_str());
		}

		global_checksum_v2 = gen_global_pgsql_servers_v2_checksum(new_hash);
		proxy_info("New computed global checksum for 'pgsql_servers_v2' is '%s'\n", global_checksum_v2.c_str());
	}

	// pgsql_servers mutations affect writer identity even for runtime-only
	// commits. Keep this under the HGM write lock; the helper bumps epochs only
	// when the active writer address:port set really changed.
	if (pgsql_servers_mutated) {
#if POLARDB_PROXY
		polardb_refresh_all_writer_epochs_under_hgm_write_lock("pgsql_servers reload");
		polardb_fast_topology_wrlock();
		polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
		polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
	}

	// Update 'pgsql_servers' and global checksums
	{
		uint64_t new_hash = commit_update_checksum_from_pgsql_servers(peer_runtime_pgsql_servers.resultset);
		const string new_checksum { get_checksum_from_hash(new_hash) };
		proxy_info("Checksum for table %s is %s\n", "pgsql_servers", new_checksum.c_str());

		pthread_mutex_lock(&GloVars.checksum_mutex);
		if (only_commit_runtime_pgsql_servers == false) {
			update_glovars_pgsql_servers_v2_checksum(global_checksum_v2, peer_pgsql_servers_v2.checksum, true);
		}
		update_glovars_pgsql_servers_checksum(new_checksum, peer_runtime_pgsql_servers.checksum, update_version);
		pthread_mutex_unlock(&GloVars.checksum_mutex);
	}

	// fill Hostgroup_Manager_Mapping with latest records
	update_hostgroup_manager_mappings();

	__sync_fetch_and_add(&status.servers_table_version,1);

	// We completely reset read_only_set1. It will generated (completely) again in read_only_action()
	// Note: read_only_set1 will be regenerated all at once
	read_only_set1.erase(read_only_set1.begin(), read_only_set1.end());
	// We completely reset read_only_set2. It will be again written in read_only_action()
	// Note: read_only_set2 will be regenerated one server at the time
	read_only_set2.erase(read_only_set2.begin(), read_only_set2.end());

	this->status.p_counter_array[PgSQL_p_hg_counter::servers_table_version]->Increment();
	pthread_cond_broadcast(&status.servers_table_version_cond);
	pthread_mutex_unlock(&status.servers_table_version_lock);

	// NOTE: In order to guarantee the latest generated version, this should be kept after all the
	// calls to 'generate_pgsql_servers'.
	update_table_pgsql_servers_for_monitor(false);

	wrunlock();
	unsigned long long curtime2=monotonic_time();
	curtime1 = curtime1/1000;
	curtime2 = curtime2/1000;
	proxy_info("PgSQL_HostGroups_Manager::commit() locked for %llums\n", curtime2-curtime1);

	if (GloPTH) {
		GloPTH->signal_all_threads(
			PgSQL_Thread::TOPOLOGY_PUBLICATION_WAKE);
	}

	return true;
}

/** 
 * @brief Calculate the checksum for the runtime pgsql_servers record, after excluding all the rows
 *    with the status OFFLINE_HARD from the result set
 * 
 * @details The runtime pgsql_servers is now considered as a distinct module and have a separate checksum calculation.
 *    This is because the records in the runtime module may differ from those in the admin pgsql_servers module, which
 *	  can leave cluster members with inconsistent state.
 * 
 * @param runtime_pgsql_servers resultset of runtime pgsql_servers or can be a nullptr.
*/
uint64_t PgSQL_HostGroups_Manager::get_pgsql_servers_checksum(SQLite3_result* runtime_pgsql_servers) {

	//Note: GloVars.checksum_mutex needs to be locked
	SQLite3_result* resultset = nullptr;

	if (runtime_pgsql_servers == nullptr) {
		char* error = NULL;
		int cols = 0;
		int affected_rows = 0;

		mydb->execute_statement(PGHGM_GEN_CLUSTER_ADMIN_RUNTIME_SERVERS, &error, &cols, &affected_rows, &resultset);

		if (resultset) {
			save_runtime_pgsql_servers(resultset);
		} else {
			proxy_info("Checksum for table %s is 0x%lX\n", "pgsql_servers", (long unsigned int)0);
		}
	} else {
		resultset = runtime_pgsql_servers;
		save_runtime_pgsql_servers(runtime_pgsql_servers);
	}

	table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS] = resultset != nullptr ? resultset->raw_checksum() : 0;
	proxy_info("Checksum for table %s is 0x%lX\n", "pgsql_servers", table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS]);

	return table_resultset_checksum[HGM_TABLES::PgSQL_SERVERS];
}

void PgSQL_HostGroups_Manager::purge_pgsql_servers_table() {
#if POLARDB_PROXY
	polardb_fast_topology_wrlock();
	bool polardb_server_list_changed = false;
#endif // POLARDB_PROXY
	for (unsigned int i=0; i<MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		PgSQL_SrvC *mysrvc=NULL;
		for (unsigned int j=0; j<myhgc->mysrvs->servers->len; j++) {
			mysrvc=myhgc->mysrvs->idx(j);
			if (mysrvc->status==MYSQL_SERVER_STATUS_OFFLINE_HARD) {
				if (pgsql_pool_no_live_connections(mysrvc)) {
					// no more connections for OFFLINE_HARD server, removing it
					mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->remove_index_fast(j);
					j--;
#if POLARDB_PROXY
					polardb_retire_server_under_fast_topology_lock(mysrvc);
					polardb_server_list_changed = true;
#else
					delete mysrvc;
#endif // POLARDB_PROXY
				}
			}
		}
	}
#if POLARDB_PROXY
	if (polardb_server_list_changed) {
		polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
	}
	polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
}



void PgSQL_HostGroups_Manager::generate_pgsql_servers_table(int *_onlyhg) {
	int rc;
	PtrArray *lst=new PtrArray();
	//sqlite3 *mydb3=mydb->get_db();
	char *query1=(char *)"INSERT INTO pgsql_servers VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";
	auto [rc1, statement1_unique] = mydb->prepare_v2(query1);
	ASSERT_SQLITE_OK(rc1, mydb);
	std::string query32s = "INSERT INTO pgsql_servers VALUES " + generate_multi_rows_query(32,12);
	char *query32 = (char *)query32s.c_str();
	auto [rc2, statement32_unique] = mydb->prepare_v2(query32);
	ASSERT_SQLITE_OK(rc2, mydb);
	sqlite3_stmt *statement1 = statement1_unique.get();
	sqlite3_stmt *statement32 = statement32_unique.get();

	if (pgsql_thread___hostgroup_manager_verbose) {
		if (_onlyhg==NULL) {
			proxy_info("Dumping current PgSQL Servers structures for hostgroup ALL\n");
		} else {
			int hidonly=*_onlyhg;
			proxy_info("Dumping current PgSQL Servers structures for hostgroup %d\n", hidonly);
		}
	}
	for (unsigned int i=0; i<MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		if (_onlyhg) {
			int hidonly=*_onlyhg;
			if (myhgc->hid!=(unsigned int)hidonly) {
				// skipping this HG
				continue;
			}
		}
		PgSQL_SrvC *mysrvc=NULL;
		for (unsigned int j=0; j<myhgc->mysrvs->servers->len; j++) {
			mysrvc=myhgc->mysrvs->idx(j);
			if (pgsql_thread___hostgroup_manager_verbose) {
				char *st;
				switch (mysrvc->status) {
					case 0:
						st=(char *)"ONLINE";
						break;
					case 2:
						st=(char *)"OFFLINE_SOFT";
						break;
					case 3:
						st=(char *)"OFFLINE_HARD";
						break;
					default:
					case 1:
					case 4:
						st=(char *)"SHUNNED";
						break;
				}
				fprintf(stderr,"HID: %d , address: %s , port: %d , weight: %ld , status: %s , max_connections: %ld , max_replication_lag: %u , use_ssl: %u , max_latency_ms: %u , comment: %s\n", mysrvc->myhgc->hid, mysrvc->address, mysrvc->port, mysrvc->weight, st, mysrvc->max_connections, mysrvc->max_replication_lag, mysrvc->use_ssl, mysrvc->max_latency_us*1000, mysrvc->comment);
			}
			lst->add(mysrvc);
			if (lst->len==32) {
				while (lst->len) {
					int i=lst->len;
					i--;
					PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)lst->remove_index_fast(0);
					uintptr_t ptr=(uintptr_t)mysrvc;
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+1, mysrvc->myhgc->hid); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_text)(statement32,  (i*12)+2, mysrvc->address, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+3, mysrvc->port); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+4, mysrvc->weight); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+5, mysrvc->status); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+6, mysrvc->compression); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+7, mysrvc->max_connections); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+8, mysrvc->max_replication_lag); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+9, mysrvc->use_ssl); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+10, mysrvc->max_latency_us/1000); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_text)(statement32,  (i*12)+11, mysrvc->comment, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
					rc=(*proxy_sqlite3_bind_int64)(statement32, (i*12)+12, ptr); ASSERT_SQLITE_OK(rc, mydb);
				}
				SAFE_SQLITE3_STEP2(statement32);
				rc=(*proxy_sqlite3_clear_bindings)(statement32); ASSERT_SQLITE_OK(rc, mydb);
				rc=(*proxy_sqlite3_reset)(statement32); ASSERT_SQLITE_OK(rc, mydb);
			}
		}
	}
	while (lst->len) {
		PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)lst->remove_index_fast(0);
		uintptr_t ptr=(uintptr_t)mysrvc;
		rc=(*proxy_sqlite3_bind_int64)(statement1, 1, mysrvc->myhgc->hid); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement1,  2, mysrvc->address, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 3, mysrvc->port); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 4, mysrvc->weight); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 5, mysrvc->status); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 6, mysrvc->compression); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 7, mysrvc->max_connections); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 8, mysrvc->max_replication_lag); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 9, mysrvc->use_ssl); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 10, mysrvc->max_latency_us/1000); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement1,  11, mysrvc->comment, -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement1, 12, ptr); ASSERT_SQLITE_OK(rc, mydb);

		SAFE_SQLITE3_STEP2(statement1);
		rc=(*proxy_sqlite3_clear_bindings)(statement1); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_reset)(statement1); ASSERT_SQLITE_OK(rc, mydb);
	}
	if (pgsql_thread___hostgroup_manager_verbose) {
		char *error=NULL;
		int cols=0;
		int affected_rows=0;
		SQLite3_result *resultset=NULL;
		if (_onlyhg==NULL) {
			mydb->execute_statement((char *)"SELECT hostgroup_id hid, hostname, port, weight, status, compression cmp, max_connections max_conns, max_replication_lag max_lag, use_ssl ssl, max_latency_ms max_lat, comment, mem_pointer FROM pgsql_servers", &error , &cols , &affected_rows , &resultset);
		} else {
			int hidonly=*_onlyhg;
			char *q1 = (char *)malloc(256);
			sprintf(q1,"SELECT hostgroup_id hid, hostname, port, weight, status, compression cmp, max_connections max_conns, max_replication_lag max_lag, use_ssl ssl, max_latency_ms max_lat, comment, mem_pointer FROM pgsql_servers WHERE hostgroup_id=%d" , hidonly);
			mydb->execute_statement(q1, &error , &cols , &affected_rows , &resultset);
			free(q1);
		}
		if (error) {
			proxy_error("Error on read from pgsql_servers : %s\n", error);
		} else {
			if (resultset) {
				if (_onlyhg==NULL) {
					proxy_info("Dumping pgsql_servers: ALL\n");
				} else {
					int hidonly=*_onlyhg;
					proxy_info("Dumping pgsql_servers: HG %d\n", hidonly);
				}
				resultset->dump_to_stderr();
			}
		}
		if (resultset) { delete resultset; resultset=NULL; }
	}
	delete lst;
}

#if POLARDB_PROXY
static PolarDB_ParsedGlobalConfigValue polardb_current_global_config() {
	if (GloPTH) {
		return GloPTH->get_polardb_global_config();
	}
	return PolarDB_ParsedGlobalConfigValue{};
}

static void polardb_warn_effective_config_mismatches(
		const PgSQL_HostGroups_Manager::PolarDB_TopologySnapshot& snapshot,
		int global_consistency_mode, int global_proxy_protocol, bool profile_off,
		const char* global_identity_host, int global_identity_port) {
	bool saw_rfq_capable_polardb_row = false;

	for (const auto& entry : snapshot.by_hostgroup) {
		const PgSQL_HostGroups_Manager::PolarDB_HG_Config& hg_config =
			entry.second.config;
		if (!hg_config.is_polardb_hostgroup ||
				entry.first != static_cast<unsigned int>(hg_config.writer_hostgroup)) {
			continue;
		}

		const int effective_consistency_mode =
			hg_config.policy.consistency_mode >= 0 ?
				hg_config.policy.consistency_mode : global_consistency_mode;
		const int effective_proxy_protocol =
			hg_config.policy.proxy_protocol >= 0 ?
				hg_config.policy.proxy_protocol : global_proxy_protocol;
		if (effective_proxy_protocol != POLARDB_PROXY_PROTOCOL_OFF) {
			saw_rfq_capable_polardb_row = true;
		}
		const PolarDB_ConsistencyMode resolved_mode =
			polardb_consistency_from_int(effective_consistency_mode);
		if (!profile_off &&
				polardb_consistency_mode_uses_lsn_wait(resolved_mode) &&
				effective_proxy_protocol == POLARDB_PROXY_PROTOCOL_OFF) {
			proxy_warning("PolarDB replication hostgroup writer=%d reader=%d resolves consistency_mode=%s but effective proxy_protocol=off; RFQ LSN startup requests are disabled for this group\n",
				hg_config.writer_hostgroup, hg_config.reader_hostgroup,
				polardb_consistency_mode_name(resolved_mode));
		}
	}

	const bool fallback_identity_partially_set =
		(global_identity_host && global_identity_host[0] != '\0') ||
		global_identity_port != 0;
	if (saw_rfq_capable_polardb_row &&
			fallback_identity_partially_set &&
			!PolarDB_StartupIdentity{
				global_identity_host ? global_identity_host : "",
				global_identity_port}.valid(true)) {
		proxy_warning("PolarDB replication hostgroups include an RFQ-capable proxy_protocol, but configured fallback startup identity '%s:%d' is incomplete or invalid; client/listener identity may still satisfy client-backed connections\n",
			global_identity_host ? global_identity_host : "", global_identity_port);
	}
}

#endif // POLARDB_PROXY

void PgSQL_HostGroups_Manager::generate_pgsql_replication_hostgroups_table() {
	if (incoming_replication_hostgroups==NULL)
		return;
	if (pgsql_thread___hostgroup_manager_verbose) {
		proxy_info("New pgsql_replication_hostgroups table\n");
	}
#if POLARDB_PROXY
	// Besides mirroring the table into mydb, populate the PolarDB topology cache
	// (writer<->reader pairing) and the per-writer-HGC repl_config consumed by
	// the routing pipeline. The incoming resultset carries the PolarDB columns:
	//   0=writer_hostgroup, 1=reader_hostgroup, 2=check_type,
	//   3=txn_split_enabled, 4=consistency_mode, 5=max_lag_bytes,
	//   6=lsn_wait_timeout_ms, 7=proxy_protocol, 8=comment

	const PolarDB_ParsedGlobalConfigValue global_config =
		polardb_current_global_config();
	const PolarDB_LsnWaitTimeoutAction timeout_action =
		polardb_lsn_wait_timeout_action_from_int(
			global_config.lsn_wait_timeout_action);
	const PolarDB_MissingLsnAction missing_lsn_action =
		polardb_missing_lsn_action_from_int(
			global_config.missing_lsn_action);
	const bool profile_off =
		polardb_profile_from_int(global_config.profile) ==
			PolarDB_Profile::OFF;
	for (SQLite3_row* row : incoming_replication_hostgroups->rows) {
		if (!row || strcasecmp(row->fields[2], "polardb") != 0) {
			continue;
		}
		const int configured_mode =
			polardb_hostgroup_consistency_mode_from_string(
				row->fields[4], -1);
		const int effective_mode = configured_mode >= 0
			? configured_mode : global_config.consistency_mode;
		const int configured_protocol =
			polardb_proxy_protocol_from_string(row->fields[7], -1);
		const int effective_protocol = configured_protocol >= 0
			? configured_protocol
			: static_cast<int>(global_config.startup.proxy_protocol);
		const char* policy_error = polardb_consistency_policy_error(
			polardb_consistency_from_int(effective_mode),
			missing_lsn_action, timeout_action);
		if (policy_error) {
			proxy_error(
				"Cannot load PostgreSQL replication hostgroups: "
				"writer_hostgroup=%s resolves consistency_mode=global_lsn "
				"with an invalid warning action: %s. "
				"The previous runtime topology is kept.\n",
				row->fields[0], policy_error);
			incoming_replication_hostgroups = nullptr;
			return;
		}
		const char* lsn_source_error = profile_off ? nullptr :
			polardb_hostgroup_lsn_source_error(
				polardb_consistency_from_int(effective_mode),
				polardb_proxy_protocol_from_int(effective_protocol));
		if (lsn_source_error) {
			proxy_error(
				"Cannot load PostgreSQL replication hostgroups: "
				"writer_hostgroup=%s reader_hostgroup=%s: %s. "
				"The previous runtime topology is kept.\n",
				row->fields[0], row->fields[1], lsn_source_error);
			incoming_replication_hostgroups = nullptr;
			return;
		}
	}
#endif // POLARDB_PROXY

	// Validate before replacing the runtime mirror. A rejected PolarDB policy
	// must leave both the routing snapshot and the admin-visible runtime table
	// unchanged.
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4,
		"DELETE FROM pgsql_replication_hostgroups\n");
	mydb->execute("DELETE FROM pgsql_replication_hostgroups");

#if POLARDB_PROXY
	// Mark all HGCs as not configured (same pattern as hostgroup_attributes).
	for (unsigned int i = 0; i < MyHostGroups->len; i++) {
		PgSQL_HGC* myhgc = (PgSQL_HGC*)MyHostGroups->index(i);
		myhgc->repl_config.configured = false;
	}

	// Clear PolarDB caches before repopulating (O(1) lookups, not SQLite queries).
	polardb_hostgroups_.clear();
	polardb_writer_to_reader_.clear();
	polardb_reader_to_writer_.clear();
	auto next_polardb_snapshot = std::make_shared<PolarDB_TopologySnapshot>();

	const int global_consistency_mode = global_config.consistency_mode;
	const int global_proxy_protocol = static_cast<int>(
		global_config.startup.proxy_protocol);
	const char* global_identity_host =
		global_config.startup.configured_identity.host.c_str();
	const int global_identity_port =
		global_config.startup.configured_identity.port;

	for (std::vector<SQLite3_row *>::iterator it = incoming_replication_hostgroups->rows.begin() ; it != incoming_replication_hostgroups->rows.end(); ++it) {
		SQLite3_row *r=*it;
		int writer_hg = atoi(r->fields[0]);
		int reader_hg = atoi(r->fields[1]);
		const char* check_type = r->fields[2];
		const bool txn_split_enabled = atoi(r->fields[3]) != 0;
		const char* consistency_mode = r->fields[4];
		int max_lag_bytes = atoi(r->fields[5]);
		int lsn_wait_timeout_ms = atoi(r->fields[6]);
		const char* proxy_protocol = r->fields[7];
		const bool is_polardb_check = (strcasecmp(check_type, "polardb") == 0);
		const int parsed_consistency_mode =
			polardb_hostgroup_consistency_mode_from_string(
				consistency_mode, -1);
		const int parsed_proxy_protocol =
			polardb_proxy_protocol_from_string(proxy_protocol, -1);

		// Populate PolarDB caches for hot-path lookups.
		if (is_polardb_check) {
			polardb_hostgroups_.insert(writer_hg);
			polardb_hostgroups_.insert(reader_hg);
			polardb_reader_to_writer_[reader_hg] = writer_hg;
			polardb_writer_to_reader_[writer_hg] = reader_hg;
			POLARDB_TRACE("PolarDB CACHE: added writer_hg %d <-> reader_hg %d mapping\n",
				writer_hg, reader_hg);
		}

		// Cache the replication config on the writer HGC.
		PgSQL_HGC* writer_hgc = MyHGC_lookup(writer_hg);
		if (writer_hgc) {
			writer_hgc->repl_config.configured = true;
			writer_hgc->repl_config.reader_hostgroup = reader_hg;
			writer_hgc->repl_config.writer_hostgroup = writer_hg;
			writer_hgc->repl_config.check_type = check_type;
			writer_hgc->repl_config.txn_split_enabled = txn_split_enabled;
			writer_hgc->repl_config.consistency_mode = consistency_mode;
			writer_hgc->repl_config.consistency_mode_enum = parsed_consistency_mode;
			writer_hgc->repl_config.max_lag_bytes = max_lag_bytes;
			writer_hgc->repl_config.lsn_wait_timeout_ms = lsn_wait_timeout_ms;
			writer_hgc->repl_config.proxy_protocol = proxy_protocol;
			writer_hgc->repl_config.proxy_protocol_enum = parsed_proxy_protocol;
			// Runtime group LSN and writer epoch values survive a config reload.
			// The helper below resets them only when the writer server set
			// actually changes.
		}

		if (is_polardb_check) {
			PolarDB_HG_SnapshotEntry snapshot_entry;
			PolarDB_HG_Config& hg_config = snapshot_entry.config;
			hg_config.is_polardb_hostgroup = true;
			hg_config.writer_hostgroup = writer_hg;
			hg_config.reader_hostgroup = reader_hg;
			hg_config.policy.txn_split_enabled = txn_split_enabled;
			hg_config.policy.consistency_mode = parsed_consistency_mode;
			hg_config.policy.max_lag_bytes = max_lag_bytes;
			hg_config.policy.lsn_wait_timeout_ms = lsn_wait_timeout_ms;
			hg_config.policy.proxy_protocol = parsed_proxy_protocol;
			if (writer_hgc) {
				snapshot_entry.group_lsn =
					writer_hgc->repl_config.polardb_group_lsn;
				snapshot_entry.max_replica_replay_lsn =
					writer_hgc->repl_config.polardb_max_replica_replay_lsn;
				snapshot_entry.writer_epoch =
					writer_hgc->repl_config.polardb_writer_epoch;
			}
			next_polardb_snapshot->by_hostgroup[(unsigned int)writer_hg] =
				snapshot_entry;
			next_polardb_snapshot->by_hostgroup[(unsigned int)reader_hg] =
				std::move(snapshot_entry);
		}

		char *comment_escaped=escape_string_single_quotes(r->fields[8],false);
		int comment_length=strlen(comment_escaped);
		char *query=(char *)malloc(512+comment_length);
		sprintf(query,"INSERT INTO pgsql_replication_hostgroups VALUES(%s,%s,'%s',%s,'%s',%s,%s,'%s','%s')",
			r->fields[0], r->fields[1], r->fields[2], r->fields[3],
			r->fields[4], r->fields[5], r->fields[6], r->fields[7],
			comment_escaped);
		if (comment_escaped!=r->fields[8]) {
			free(comment_escaped);
		}
		mydb->execute(query);
		if (pgsql_thread___hostgroup_manager_verbose) {
			fprintf(stderr,"writer_hostgroup: %s , reader_hostgroup: %s, check_type: %s, "
				"txn_split_enabled: %s, consistency_mode: %s, max_lag_bytes: %s, lsn_wait_timeout_ms: %s, proxy_protocol: %s, comment: %s\n",
				r->fields[0], r->fields[1], r->fields[2], r->fields[3],
				r->fields[4], r->fields[5], r->fields[6], r->fields[7],
				r->fields[8]);
		}
		free(query);
	}
	polardb_warn_effective_config_mismatches(*next_polardb_snapshot,
		global_consistency_mode, global_proxy_protocol, profile_off,
		global_identity_host, global_identity_port);
	polardb_refresh_all_writer_epochs_under_hgm_write_lock("replication-hostgroups reload");

	next_polardb_snapshot->generation =
		polardb_topology_generation_.load(std::memory_order_relaxed) + 1;
	std::shared_ptr<const PolarDB_TopologySnapshot> current_snapshot =
		next_polardb_snapshot;
	std::atomic_store_explicit(&polardb_topology_snapshot_, current_snapshot,
		std::memory_order_release);
	polardb_topology_generation_.store(next_polardb_snapshot->generation,
		std::memory_order_release);
	const bool next_polardb_active =
		!next_polardb_snapshot->by_hostgroup.empty();
	const bool was_polardb_active = status.polardb_active.load(
		std::memory_order_relaxed);
	if (next_polardb_active && !was_polardb_active) {
		status.polardb_activation_generation.fetch_add(
			1, std::memory_order_relaxed);
	}
	// The release publishes the generation increment before workers observe the
	// active topology and copy both values into their local caches.
	status.polardb_active.store(next_polardb_active, std::memory_order_release);
#else
	for (std::vector<SQLite3_row *>::iterator it = incoming_replication_hostgroups->rows.begin() ; it != incoming_replication_hostgroups->rows.end(); ++it) {
		SQLite3_row *r=*it;
		char *o=NULL;
		int comment_length=0;	// #issue #643
		//if (r->fields[3]) { // comment is not null
			o=escape_string_single_quotes(r->fields[3],false);
			comment_length=strlen(o);
		//}
		char *query=(char *)malloc(256+comment_length);
		//if (r->fields[3]) { // comment is not null
			sprintf(query,"INSERT INTO pgsql_replication_hostgroups VALUES(%s,%s,'%s','%s')",r->fields[0], r->fields[1], r->fields[2], o);
			if (o!=r->fields[3]) { // there was a copy
				free(o);
			}
		//} else {
			//sprintf(query,"INSERT INTO pgsql_replication_hostgroups VALUES(%s,%s,NULL)",r->fields[0],r->fields[1]);
		//}
		mydb->execute(query);
		if (pgsql_thread___hostgroup_manager_verbose) {
			fprintf(stderr,"writer_hostgroup: %s , reader_hostgroup: %s, check_type %s, comment: %s\n", r->fields[0],r->fields[1], r->fields[2], r->fields[3]);
		}
		free(query);
	}
#endif // POLARDB_PROXY
	incoming_replication_hostgroups=NULL;
}

void PgSQL_HostGroups_Manager::update_table_pgsql_servers_for_monitor(bool lock) {
	if (lock) {
		wrlock();
	}

	std::lock_guard<std::mutex> pgsql_servers_lock(this->pgsql_servers_to_monitor_mutex);

	char* error = NULL;
	int cols = 0;
	int affected_rows = 0;
	SQLite3_result* resultset = NULL;
	char* query = const_cast<char*>("SELECT hostname, port, status, use_ssl FROM pgsql_servers WHERE status != 3 GROUP BY hostname, port");

	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "%s\n", query);
	mydb->execute_statement(query, &error , &cols , &affected_rows , &resultset);

	if (error != nullptr) {
		proxy_error("Error on read from pgsql_servers : %s\n", error);
	} else {
		if (resultset != nullptr) {
			delete this->pgsql_servers_to_monitor;
			this->pgsql_servers_to_monitor = resultset;
		}
	}

	if (lock) {
		wrunlock();
	}

	// Wake the PgSQL resolver loop, not MySQL's. Waking MySQL's would refresh
	// the wrong DNS cache, and pgsql hostnames would stay unresolvable until the
	// next refresh_interval (default 60 s).
	PgSQL_Monitor::trigger_dns_cache_update();
}

SQLite3_result * PgSQL_HostGroups_Manager::dump_table_pgsql(const string& name) {
	char * query = (char *)"";
	if (name == "pgsql_replication_hostgroups") {
#if POLARDB_PROXY
		// Dump the PolarDB routing columns alongside the upstream
		// writer/reader/check_type/comment.
		query=(char *)"SELECT writer_hostgroup, reader_hostgroup, check_type, txn_split_enabled, consistency_mode, max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment FROM pgsql_replication_hostgroups";
#else
		query=(char *)"SELECT writer_hostgroup, reader_hostgroup, check_type, comment FROM pgsql_replication_hostgroups";
#endif
	} else if (name == "pgsql_hostgroup_attributes") {
		query=(char *)"SELECT hostgroup_id, max_num_online_servers, autocommit, free_connections_pct, init_connect, multiplex, connection_warming, throttle_connections_per_sec, ignore_session_variables, hostgroup_settings, servers_defaults, comment FROM pgsql_hostgroup_attributes ORDER BY hostgroup_id";
	} else if (name == "pgsql_servers") {
		query = (char *)PGHGM_GEN_ADMIN_RUNTIME_SERVERS;
	} else if (name == "cluster_pgsql_servers") {
		query = (char *)PGHGM_GEN_CLUSTER_ADMIN_RUNTIME_SERVERS;
	} else if (name == "pgsql_servers_ssl_params") {
		query=(char *)"SELECT hostname, port, username, ssl_ca, ssl_cert, ssl_key, ssl_crl, ssl_crlpath, ssl_protocol_version_range, comment FROM pgsql_servers_ssl_params ORDER BY hostname, port, username";
	} else {
		assert(0);
	}
	wrlock();
	if (name == "pgsql_servers") {
		purge_pgsql_servers_table();
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_servers\n");
		mydb->execute("DELETE FROM pgsql_servers");
		generate_pgsql_servers_table();
	}
	char *error=NULL;
	int cols=0;
	int affected_rows=0;
	SQLite3_result *resultset=NULL;
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "%s\n", query);
	mydb->execute_statement(query, &error , &cols , &affected_rows , &resultset);
	wrunlock();
	return resultset;
}

void PgSQL_HostGroups_Manager::increase_reset_counter() {
	wrlock();
	status.pgconnpoll_reset++;
	wrunlock();
}
#if POLARDB_PROXY
void PgSQL_HostGroups_Manager::polardb_route_reader_reservation_wake(
		PolarDB_ReaderPoolReservationWake wake) {
	while (wake.has_reservation()) {
		PgSQL_SrvC* server = wake.server;
		if (GloPTH && GloPTH->polardb_queue_reader_reservation_wake(
				wake.worker_index, wake.token, wake.server,
				wake.server_snapshot)) {
			status.polardb_reader_pool_reservation_wake.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}
		const auto cancel = server->cancel_reader_pool_reservation(
			wake.worker_index, wake.token);
		wake = cancel.next_wake;
	}
}
#endif // POLARDB_PROXY

PoolReturnResult PgSQL_HostGroups_Manager::return_connection_with_match_key(
		PgSQL_Connection* conn,
		PgSQL_PoolReturnCheck return_check,
		RejectedConnectionAction unreturned_action,
		PgSQL_Thread* returning_thread) {
#if !POLARDB_PROXY
	(void)conn;
	(void)return_check;
	(void)unreturned_action;
	(void)returning_thread;
	return {};
#else
	if (!conn || !conn->parent) {
		return {};
	}
	PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
	const bool exact_pool_connection =
		conn->polardb_exact_pool_connection ||
		!conn->polardb_pool_key.empty() ||
		conn->polardb_selected_server_snapshot != nullptr;
	// This is the common classic return. Its connection never entered the
	// exact-key pool, so there is no exact ownership to recover from USED and
	// no reason to enter the exact-key return operation.
	if (!exact_pool_connection) {
		return {};
	}
	conn->auto_increment_delay_token = 0;
	PgSQL_PoolMatchKey current_match_key;
	bool can_return = GloPTH != nullptr &&
		conn->async_state_machine == ASYNC_IDLE &&
		conn->largest_query_length <=
			static_cast<unsigned int>(GloPTH->variables.threshold_query_length) &&
		conn->local_stmts->get_num_backend_stmts() <=
			static_cast<unsigned int>(GloPTH->variables.max_stmts_per_connection);
	bool has_current_match_key = false;
	if (can_return && polardb_reader_pool_) {
		if (return_check ==
				PgSQL_PoolReturnCheck::REUSE_LOCAL_RETURN_CHECK) {
			// The immediately preceding local-return decision checked the same
			// connection. Rebuild its four-word key without repeating profile,
			// identity, connection-state, and session-state checks.
			current_match_key = pgsql_pool_match_key(
				conn->polardb_startup_profile_generation,
				conn->polardb_pool_key);
			has_current_match_key = !current_match_key.empty();
		} else {
			const auto decision =
				polardb_reader_pool_->connection_return_decision(conn);
			has_current_match_key = decision.returnable();
			current_match_key = decision.match_key;
			if (!has_current_match_key) {
				polardb_reader_pool_->
					account_connection_return_rejection(decision.status);
			}
		}
	}
	const auto reject_connection = [&]() {
		PoolReturnResult result;
		(void)srv->remove_used_connection(conn);
		if (unreturned_action == RejectedConnectionAction::DETACH) {
			result.status = PoolReturnStatus::DETACHED;
			result.detached_connection = conn;
		} else {
			delete conn;
			result.status = PoolReturnStatus::DESTROYED;
		}
		return result;
	};
	pgsql_pool_status_count_push(
		returning_thread, &status.pgconnpoll_push, 1);
	if (can_return && has_current_match_key) {
		const unsigned int excluded_worker_index = returning_thread
			? returning_thread->get_polardb_worker_index() : UINT_MAX;
		unsigned long long* lock_wait_out = nullptr;
		unsigned long long* lock_hold_out = nullptr;
#if POLARDB_PROFILE
		unsigned long long lock_wait_us = 0;
		unsigned long long lock_hold_us = 0;
		lock_wait_out = &lock_wait_us;
		lock_hold_out = &lock_hold_us;
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			returning_thread, reader_pool_shared_return_attempt);
#endif // POLARDB_PROFILE
		const ServerReturnResult returned = srv->return_matching_connection(
			conn, current_match_key, lock_wait_out, lock_hold_out,
			excluded_worker_index);
#if POLARDB_PROFILE
		POLARDB_PROFILE_THREAD_COUNT(
			returning_thread, reader_pool_shared_return_lock_wait_sum_us,
			lock_wait_us);
		POLARDB_PROFILE_THREAD_COUNT(
			returning_thread, reader_pool_shared_return_lock_hold_sum_us,
			lock_hold_us);
		if (returned.stored()) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				returning_thread, reader_pool_shared_return_accepted);
		} else {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				returning_thread, reader_pool_shared_return_rejected);
		}
#endif // POLARDB_PROFILE
		if (!returned.stored()) {
			return reject_connection();
		}
		status.polardb_reader_pool_return_to_core.fetch_add(
			1, std::memory_order_relaxed);
		polardb_route_reader_reservation_wake(returned.wake);
		return PoolReturnResult{PoolReturnStatus::STORED, nullptr};
	}
	return reject_connection();
#endif // !POLARDB_PROXY
}

#if POLARDB_PROXY
PolarDB_ReaderRemoteReservationResult
PgSQL_HostGroups_Manager::reserve_retained_reader_connection(
		PgSQL_Connection* conn, const PgSQL_PoolMatchKey& expected_match_key,
		unsigned int returning_worker_index) {
	if (!conn || !conn->parent || expected_match_key.empty() ||
			returning_worker_index == UINT_MAX || !GloPTH ||
			conn->async_state_machine != ASYNC_IDLE ||
			conn->largest_query_length > static_cast<unsigned int>(
				GloPTH->variables.threshold_query_length) ||
			conn->local_stmts->get_num_backend_stmts() >
				static_cast<unsigned int>(
				GloPTH->variables.max_stmts_per_connection)) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	if (!polardb_reader_pool_) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	const auto decision =
		polardb_reader_pool_->connection_return_decision(conn);
	if (!decision.returnable()) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	if (!(decision.match_key == expected_match_key)) {
		return PolarDB_ReaderRemoteReservationResult::CONNECTION_INVALID;
	}
	PgSQL_SrvC* server = static_cast<PgSQL_SrvC*>(conn->parent);
	PolarDB_ReaderPoolReservationWake wake;
	const PolarDB_ReaderRemoteReservationResult result =
		server->reserve_matching_used_connection(
			conn, decision.match_key, returning_worker_index, &wake);
	if (result ==
			PolarDB_ReaderRemoteReservationResult::CONNECTION_RESERVED) {
		status.polardb_reader_pool_return_to_core.fetch_add(
			1, std::memory_order_relaxed);
		polardb_route_reader_reservation_wake(std::move(wake));
	}
	return result;
}
#endif // POLARDB_PROXY

#if POLARDB_PROXY
void PgSQL_HostGroups_Manager::polardb_return_reader_connections(
		PgSQL_Thread* thread,
		PgSQL_Connection* const* connections, size_t connection_count,
		std::vector<PgSQL_Connection*>& detached_connections) {
	if (!connections || connection_count == 0) {
		return;
	}
	PgSQL_SrvC* server = connections[0] && connections[0]->parent
		? static_cast<PgSQL_SrvC*>(connections[0]->parent) : nullptr;
	if (!server) {
		for (size_t index = 0; index < connection_count; ++index) {
			PgSQL_Connection* connection = connections[index];
			if (!connection) {
				continue;
			}
			PgSQL_SrvC* actual_server = connection->parent
				? static_cast<PgSQL_SrvC*>(connection->parent) : nullptr;
			if (!actual_server ||
					actual_server->remove_used_connection(connection)) {
				detached_connections.push_back(connection);
			} else {
				proxy_error(
					"PostgreSQL core refused to detach mixed batch connection %p because its parent does not own it\n",
					(void*)connection);
			}
		}
		return;
	}

	struct PreparedReturn {
		PgSQL_Connection* connection;
		PgSQL_PoolMatchKey match_key;
		bool reusable;
		std::shared_ptr<const void> released_server_snapshot;
	};
	PreparedReturn singleton{};
	std::vector<PreparedReturn> prepared;
	if (connection_count > 1) {
		prepared.reserve(connection_count);
	}
	size_t prepared_count = 0;
	std::vector<PolarDB_ReaderPoolReservationWake> reservation_wakes;
	// Every connection in this batch came from the same worker pass.
	const unsigned int excluded_worker_index = thread
		? thread->get_polardb_worker_index() : UINT_MAX;
	unsigned int return_attempts = 0;
	for (size_t index = 0; index < connection_count; ++index) {
		PgSQL_Connection* connection = connections[index];
		if (!connection || connection->parent != server) {
			if (connection) {
				PgSQL_SrvC* actual_server = connection->parent
					? static_cast<PgSQL_SrvC*>(connection->parent) : nullptr;
				if (!actual_server ||
						actual_server->remove_used_connection(connection)) {
					detached_connections.push_back(connection);
				} else {
					proxy_error(
						"PostgreSQL core refused to detach mixed batch connection %p because its parent does not own it\n",
						(void*)connection);
				}
			}
			continue;
		}
		connection->auto_increment_delay_token = 0;
		const bool pool_state_allows_return = GloPTH != nullptr &&
			connection->async_state_machine == ASYNC_IDLE &&
			connection->largest_query_length <= static_cast<unsigned int>(
				GloPTH->variables.threshold_query_length) &&
			connection->local_stmts->get_num_backend_stmts() <=
				static_cast<unsigned int>(
					GloPTH->variables.max_stmts_per_connection);
		PgSQL_PoolMatchKey match_key;
		bool reusable = false;
		if (pool_state_allows_return && polardb_reader_pool_) {
			const auto decision =
				polardb_reader_pool_->connection_return_decision(connection);
			reusable = decision.returnable();
			match_key = decision.match_key;
			if (!reusable) {
				polardb_reader_pool_->
					account_connection_return_rejection(decision.status);
			}
		}
		if (reusable) {
			return_attempts++;
		}
		pgsql_pool_status_count_push(
			thread, &status.pgconnpoll_push, 1);
		PreparedReturn entry{
			connection, match_key, reusable, nullptr};
		if (connection_count == 1) {
			singleton = std::move(entry);
			prepared_count = 1;
		} else {
			prepared.push_back(std::move(entry));
		}
	}
	if (connection_count > 1) {
		prepared_count = prepared.size();
	}
	PreparedReturn* prepared_data = connection_count == 1
		? &singleton : prepared.data();
	if (server->myhgc && server->myhgc->has_reader_pool_capacity_request_fast()) {
		reservation_wakes.reserve(prepared_count);
	}

#if POLARDB_PROFILE
	POLARDB_PROFILE_THREAD_COUNT_ONE(
		thread, reader_pool_shared_return_group);
	const size_t group_size = prepared_count;
	if (group_size == 1) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_1);
	} else if (group_size == 2) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_2);
	} else if (group_size <= 4) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_3_4);
	} else if (group_size <= 8) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_5_8);
	} else if (group_size <= 16) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_9_16);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_return_group_17_plus);
	}
	const unsigned long long wait_started_at = monotonic_time();
#endif // POLARDB_PROFILE
	unsigned int returned = 0;
	{
		std::unique_lock<std::recursive_mutex> pool_lock(server->pool_mutex);
#if POLARDB_PROFILE
		const unsigned long long lock_acquired_at = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(
			thread, reader_pool_shared_return_lock_wait_sum_us,
			lock_acquired_at - wait_started_at);
#endif // POLARDB_PROFILE
		for (size_t index = 0; index < prepared_count; ++index) {
			PreparedReturn& entry = prepared_data[index];
			const int used_index = server->ConnectionsUsed
				? server->ConnectionsUsed->find_idx(entry.connection) : -1;
			if (used_index < 0) {
				proxy_error(
					"PostgreSQL core could not return matching USED connection %p on %s:%u\n",
					(void*)entry.connection,
					server->address ? server->address : "(null)", server->port);
				detached_connections.push_back(entry.connection);
				continue;
			}
			(void)server->ConnectionsUsed->remove_unlocked(
				static_cast<unsigned int>(used_index));
			if (!entry.reusable ||
					server->status != MYSQL_SERVER_STATUS_ONLINE) {
				detached_connections.push_back(entry.connection);
				continue;
			}
			PolarDB_ReaderPoolReservationWake reservation_wake;
			if (server->store_matching_free_connection_unlocked(
					entry.connection, entry.match_key, &reservation_wake,
					entry.released_server_snapshot,
					excluded_worker_index)) {
				returned++;
				if (reservation_wake.has_reservation()) {
					reservation_wakes.push_back(std::move(reservation_wake));
				}
			} else {
				detached_connections.push_back(entry.connection);
			}
		}
#if POLARDB_PROFILE
		POLARDB_PROFILE_THREAD_COUNT(
			thread, reader_pool_shared_return_lock_hold_sum_us,
			monotonic_time() - lock_acquired_at);
#endif // POLARDB_PROFILE
	}
#if POLARDB_PROFILE
	POLARDB_PROFILE_THREAD_COUNT(
		thread, reader_pool_shared_return_attempt, return_attempts);
	POLARDB_PROFILE_THREAD_COUNT(
		thread, reader_pool_shared_return_accepted, returned);
	POLARDB_PROFILE_THREAD_COUNT(
		thread, reader_pool_shared_return_rejected,
		return_attempts - returned);
#endif // POLARDB_PROFILE
	POLARDB_THREAD_COUNT(thread, reader_pool_return_to_core, returned);
	for (PolarDB_ReaderPoolReservationWake& reservation_wake : reservation_wakes) {
		polardb_route_reader_reservation_wake(
			std::move(reservation_wake));
	}
}

PolarDB_ReaderLocalReturnDecision
PgSQL_HostGroups_Manager::polardb_reader_local_return_decision(
		PgSQL_Connection* conn, unsigned int excluded_worker_index) {
	return polardb_reader_pool_
		? polardb_reader_pool_->local_return_decision(
			conn, excluded_worker_index)
		: PolarDB_ReaderLocalReturnDecision{};
}

void PgSQL_HostGroups_Manager::
polardb_account_reader_return_rejection(
		PolarDB_ReaderConnectionReturnStatus status) {
	if (polardb_reader_pool_) {
		polardb_reader_pool_->account_connection_return_rejection(status);
	}
}
#endif // POLARDB_PROXY

void PgSQL_HostGroups_Manager::push_MyConn_to_pool(
		PgSQL_Connection *c, bool _lock, PgSQL_Thread* counter_thread) {
	assert(c->parent);
	PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)c->parent;
	if (return_connection_with_match_key(
			c, PgSQL_PoolReturnCheck::CHECK_CONNECTION,
			RejectedConnectionAction::DESTROY, counter_thread).handled()) {
		return;
	}
#if POLARDB_PROXY
	std::shared_ptr<const void> selected_server_snapshot =
		std::move(c->polardb_selected_server_snapshot);
	(void)selected_server_snapshot;
	(void)_lock;
#else
	if (_lock) {
		wrlock();
	}
#endif // POLARDB_PROXY
	c->auto_increment_delay_token = 0;
	pgsql_pool_status_count_push(
		counter_thread, &status.pgconnpoll_push, 1);
	PgSQL_Connection* connection_to_delete = nullptr;
#if POLARDB_PROXY
	// USED membership keeps the parent server alive until this lock is held.
	// The server lock then owns the complete USED -> FREE or USED -> destroyed
	// transition. Generic returns therefore do not need the global HGM lock.
	{
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PgSQL_Connection %p, server %s:%d with status %d\n", c, mysrvc->address, mysrvc->port, mysrvc->status);
		const int used_index = mysrvc->ConnectionsUsed
			? mysrvc->ConnectionsUsed->find_idx(c) : -1;
		if (used_index < 0) {
			proxy_error(
				"PostgreSQL core pool return missed USED connection %p on %s:%u\n",
				(void*)c, mysrvc->address ? mysrvc->address : "(null)",
				mysrvc->port);
			connection_to_delete = c;
		} else {
			(void)mysrvc->ConnectionsUsed->remove_unlocked(
				static_cast<unsigned int>(used_index));
		}
		if (!connection_to_delete && GloPTH != NULL) {
			if (c->largest_query_length >
					(unsigned int)GloPTH->variables.threshold_query_length) {
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Destroying PgSQL_Connection %p, server %s:%d with status %d . largest_query_length = %lu\n", c, mysrvc->address, mysrvc->port, mysrvc->status, c->largest_query_length);
				connection_to_delete = c;
			} else if (
#if POLARDB_PROXY
					mysrvc->polardb_fast_status_value() ==
						MYSQL_SERVER_STATUS_ONLINE &&
#else
					mysrvc->status == MYSQL_SERVER_STATUS_ONLINE &&
#endif // POLARDB_PROXY
					c->async_state_machine == ASYNC_IDLE) {
				if (c->local_stmts->get_num_backend_stmts() >
						(unsigned int)GloPTH->variables.max_stmts_per_connection) {
					proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Destroying PgSQL_Connection %p, server %s:%d with status %d because has too many prepared statements\n", c, mysrvc->address, mysrvc->port, (int)mysrvc->status);
					pgsql_pool_status_count(
						&status.pgconnpoll_destroy, 1);
					connection_to_delete = c;
				} else {
					mysrvc->ConnectionsFree->add_unlocked(c);
				}
			} else {
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Destroying PgSQL_Connection %p, server %s:%d with status %d\n", c, mysrvc->address, mysrvc->port, mysrvc->status);
				connection_to_delete = c;
			}
		}
#if POLARDB_PROXY
		// A destroyed connection may be the server's final live connection.
		// Delete it before releasing the server lock so its destructor can still
		// read its parent safely.
		delete connection_to_delete;
		connection_to_delete = nullptr;
	}
#endif // POLARDB_PROXY
	delete connection_to_delete;
#if !POLARDB_PROXY
	if (_lock) {
		wrunlock();
	}
#endif // !POLARDB_PROXY
}

void PgSQL_HostGroups_Manager::push_MyConn_to_pool_array(
		PgSQL_Connection **ca, unsigned int cnt,
		PgSQL_Thread* counter_thread) {
	if (!ca || cnt == 0) {
		return;
	}
	unsigned int i=0;
	PgSQL_Connection *c=NULL;
	c=ca[i];
#if POLARDB_PROXY
	// Do not serialize the whole worker batch behind HGM. Each connection
	// returns under only its owning server lock.
	while (i<cnt) {
		push_MyConn_to_pool(c, true, counter_thread);
		i++;
		if (i<cnt) {
			c=ca[i];
		}
	}
#else
	wrlock();
	while (i<cnt) {
		push_MyConn_to_pool(c, false, counter_thread);
		i++;
		if (i<cnt) {
			c=ca[i];
		}
	}
	wrunlock();
#endif // POLARDB_PROXY
}

PgSQL_SrvC *PgSQL_HGC::get_random_MySrvC(char * gtid_uuid, uint64_t gtid_trxid, int max_lag_ms, PgSQL_Session *sess) {
	PgSQL_SrvC *mysrvc=NULL;
	unsigned int j;
	unsigned int sum=0;
	unsigned int TotalUsedConn=0;
	unsigned int l=mysrvs->cnt();
	static time_t last_hg_log = 0;
#ifdef TEST_AURORA
	unsigned long long a1 = array_mysrvc_total/10000;
	array_mysrvc_total += l;
	unsigned long long a2 = array_mysrvc_total/10000;
	if (a2 > a1) {
		fprintf(stderr, "Total: %llu, Candidates: %llu\n", array_mysrvc_total-l, array_mysrvc_cands);
	}
#endif // TEST_AURORA
	PgSQL_SrvC *mysrvcCandidates_static[32];
	PgSQL_SrvC **mysrvcCandidates = mysrvcCandidates_static;
	unsigned int num_candidates = 0;
	bool max_connections_reached = false;
	auto server_active_count = [](PgSQL_SrvC* srv) -> unsigned int {
		if (!srv || !srv->ConnectionsUsed) {
			return 0;
		}
#if POLARDB_PROXY
		return srv->pool_used_count_value();
#else
		return srv->ConnectionsUsed->conns_length();
#endif // POLARDB_PROXY
	};
	auto server_can_add_active_connection = [](PgSQL_SrvC* srv) -> bool {
		if (!srv || srv->max_connections <= 0 || !srv->ConnectionsUsed) {
			return false;
		}
#if POLARDB_PROXY
		return srv->polardb_pool_can_add_active_connection();
#else
		return srv->ConnectionsUsed->conns_length() <
			static_cast<unsigned int>(srv->max_connections);
#endif // POLARDB_PROXY
	};
	if (l>32) {
		mysrvcCandidates = (PgSQL_SrvC **)malloc(sizeof(PgSQL_SrvC *)*l);
	}
	if (l) {
		//int j=0;
		for (j=0; j<l; j++) {
			mysrvc=mysrvs->idx(j);
			if (mysrvc->status==MYSQL_SERVER_STATUS_ONLINE) { // consider this server only if ONLINE
				if (server_can_add_active_connection(mysrvc)) { // consider this server only if active capacity remains
					if ( mysrvc->current_latency_us_value() < ( mysrvc->max_latency_us ? mysrvc->max_latency_us : pgsql_thread___default_max_latency_ms *1000 ) ) { // consider the host only if not too far
						if (gtid_trxid) {
#if 0
							if (PgHGM->gtid_exists(mysrvc, gtid_uuid, gtid_trxid)) {
								sum+=mysrvc->weight;
								TotalUsedConn+=server_active_count(mysrvc);
								mysrvcCandidates[num_candidates]=mysrvc;
								num_candidates++;
							}
#endif // 0
						} else {
							if (max_lag_ms >= 0) {
								if ((unsigned int)max_lag_ms >= mysrvc->aws_aurora_current_lag_us/1000) {
									sum+=mysrvc->weight;
									TotalUsedConn+=server_active_count(mysrvc);
									mysrvcCandidates[num_candidates]=mysrvc;
									num_candidates++;
								} else {
									sess->thread->status_variables.stvar[st_var_aws_aurora_replicas_skipped_during_query]++;
								}
							} else {
								sum+=mysrvc->weight;
								TotalUsedConn+=server_active_count(mysrvc);
								mysrvcCandidates[num_candidates]=mysrvc;
								num_candidates++;
							}
						}
					}
				} else {
					max_connections_reached = true;
				}
			} else {
				if (mysrvc->status==MYSQL_SERVER_STATUS_SHUNNED) {
					// try to recover shunned servers
					if (mysrvc->shunned_automatic && pgsql_thread___shun_recovery_time_sec) {
						time_t t;
						t=time(NULL);
						// we do all these changes without locking . We assume the server is not used from long
						// even if the server is still in used and any of the follow command fails it is not critical
						// because this is only an attempt to recover a server that is probably dead anyway

						// the next few lines of code try to solve issue #530
						int max_wait_sec = (pgsql_thread___shun_recovery_time_sec * 1000 >= pgsql_thread___connect_timeout_server_max ? pgsql_thread___connect_timeout_server_max /1000 - 1 : pgsql_thread___shun_recovery_time_sec);
						if (max_wait_sec < 1) { // min wait time should be at least 1 second
							max_wait_sec = 1;
						}
						if (t > mysrvc->time_last_detected_error && (t - mysrvc->time_last_detected_error) > max_wait_sec) {
							if (
								(mysrvc->shunned_and_kill_all_connections==false) // it is safe to bring it back online
								||
								(mysrvc->shunned_and_kill_all_connections==true && pgsql_pool_no_live_connections(mysrvc)) // if shunned_and_kill_all_connections is set, ensure all connections are already dropped
							) {
#ifdef DEBUG
								if (GloPTH->variables.hostgroup_manager_verbose >= 3) {
									proxy_info("Unshunning server %s:%d.\n", mysrvc->address, mysrvc->port);
								}
#endif
								mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);
								mysrvc->shunned_automatic=false;
								mysrvc->shunned_and_kill_all_connections=false;
								mysrvc->connect_ERR_at_time_last_detected_error=0;
								mysrvc->time_last_detected_error=0;
								// note: the following function scans all the hostgroups.
								// This is ok for now because we only have a global mutex.
								// If one day we implement a mutex per hostgroup (unlikely,
								// but possible), this must be taken into consideration
								if (pgsql_thread___unshun_algorithm == 1) {
									PgHGM->unshun_server_all_hostgroups(mysrvc->address, mysrvc->port, t, max_wait_sec, &mysrvc->myhgc->hid);
								}
								// if a server is taken back online, consider it immediately
								if ( mysrvc->current_latency_us_value() < ( mysrvc->max_latency_us ? mysrvc->max_latency_us : pgsql_thread___default_max_latency_ms *1000 ) ) { // consider the host only if not too far
									if (gtid_trxid) {
#if 0
										if (PgHGM->gtid_exists(mysrvc, gtid_uuid, gtid_trxid)) {
											sum+=mysrvc->weight;
											TotalUsedConn+=server_active_count(mysrvc);
											mysrvcCandidates[num_candidates]=mysrvc;
											num_candidates++;
										}
#endif // 0
									} else {
										if (max_lag_ms >= 0) {
											if ((unsigned int)max_lag_ms >= mysrvc->aws_aurora_current_lag_us/1000) {
												sum+=mysrvc->weight;
												TotalUsedConn+=server_active_count(mysrvc);
												mysrvcCandidates[num_candidates]=mysrvc;
												num_candidates++;
											}
										} else {
											sum+=mysrvc->weight;
											TotalUsedConn+=server_active_count(mysrvc);
											mysrvcCandidates[num_candidates]=mysrvc;
											num_candidates++;
										}
									}
								}
							}
						}
					}
				}
			}
		}
		if (max_lag_ms > 0) { // we are using AWS Aurora, as this logic is implemented only here
			unsigned int min_num_replicas = sess->thread->variables.aurora_max_lag_ms_only_read_from_replicas;
			if (min_num_replicas) {
				if (num_candidates >= min_num_replicas) { // there are at least N replicas
					// we try to remove the writer
					unsigned int total_aws_aurora_current_lag_us=0;
					for (j=0; j<num_candidates; j++) {
						mysrvc = mysrvcCandidates[j];
						total_aws_aurora_current_lag_us += mysrvc->aws_aurora_current_lag_us;
					}
					if (total_aws_aurora_current_lag_us) { // we are just double checking that we don't have all servers with aws_aurora_current_lag_us==0
						for (j=0; j<num_candidates; j++) {
							mysrvc = mysrvcCandidates[j];
							if (mysrvc->aws_aurora_current_lag_us==0) {
								sum-=mysrvc->weight;
								TotalUsedConn-=server_active_count(mysrvc);
								if (j < num_candidates-1) {
									mysrvcCandidates[j]=mysrvcCandidates[num_candidates-1];
								}
								num_candidates--;
							}
						}
					}
				}
			}
		}
		if (sum==0) {
			// per issue #531 , we try a desperate attempt to bring back online any shunned server
			// we do this lowering the maximum wait time to 10%
			// most of the follow code is copied from few lines above
			time_t t;
			t=time(NULL);
			int max_wait_sec = (pgsql_thread___shun_recovery_time_sec * 1000 >= pgsql_thread___connect_timeout_server_max ? pgsql_thread___connect_timeout_server_max /10000 - 1 : pgsql_thread___shun_recovery_time_sec /10 );
			if (max_wait_sec < 1) { // min wait time should be at least 1 second
				max_wait_sec = 1;
			}
			if (t - last_hg_log > 1) { // log this at most once per second to avoid spamming the logs
				last_hg_log = time(NULL);

				if (gtid_trxid) {
					proxy_error("Hostgroup %u has no servers ready for GTID '%s:%ld'. Waiting for replication...\n", hid, gtid_uuid, gtid_trxid);
				} else {
					proxy_error("Hostgroup %u has no servers available%s! Checking servers shunned for more than %u second%s\n", hid,
						(max_connections_reached ? " or max_connections reached for all servers" : ""), max_wait_sec, max_wait_sec == 1 ? "" : "s");
				}
			}
			for (j=0; j<l; j++) {
				mysrvc=mysrvs->idx(j);
				if (mysrvc->status==MYSQL_SERVER_STATUS_SHUNNED && mysrvc->shunned_automatic==true) {
					if ((t - mysrvc->time_last_detected_error) > max_wait_sec) {
						mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);
						mysrvc->shunned_automatic=false;
						mysrvc->connect_ERR_at_time_last_detected_error=0;
						mysrvc->time_last_detected_error=0;
						// if a server is taken back online, consider it immediately
						if ( mysrvc->current_latency_us_value() < ( mysrvc->max_latency_us ? mysrvc->max_latency_us : pgsql_thread___default_max_latency_ms *1000 ) ) { // consider the host only if not too far
							if (gtid_trxid) {
#if 0
								if (PgHGM->gtid_exists(mysrvc, gtid_uuid, gtid_trxid)) {
									sum+=mysrvc->weight;
									TotalUsedConn+=server_active_count(mysrvc);
									mysrvcCandidates[num_candidates]=mysrvc;
									num_candidates++;
								}
#endif // 0
							} else {
								if (max_lag_ms >= 0) {
									if ((unsigned int)max_lag_ms >= mysrvc->aws_aurora_current_lag_us/1000) {
										sum+=mysrvc->weight;
										TotalUsedConn+=server_active_count(mysrvc);
										mysrvcCandidates[num_candidates]=mysrvc;
										num_candidates++;
									}
								} else {
									sum+=mysrvc->weight;
									TotalUsedConn+=server_active_count(mysrvc);
									mysrvcCandidates[num_candidates]=mysrvc;
									num_candidates++;
								}
							}
						}
					}
				}
			}
		}
		if (sum==0) {
			proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PgSQL_SrvC NULL because no backend ONLINE or with weight\n");
			if (l>32) {
				free(mysrvcCandidates);
			}
#ifdef TEST_AURORA
			array_mysrvc_cands += num_candidates;
#endif // TEST_AURORA
			return NULL; // if we reach here, we couldn't find any target
		}

/*
		unsigned int New_sum=0;
		unsigned int New_TotalUsedConn=0;
		// we will now scan again to ignore overloaded servers
		for (j=0; j<num_candidates; j++) {
			mysrvc = mysrvcCandidates[j];
			unsigned int len=mysrvc->ConnectionsUsed->conns_length();
			if ((len * sum) <= (TotalUsedConn * mysrvc->weight * 1.5 + 1)) {

				New_sum+=mysrvc->weight;
				New_TotalUsedConn+=len;
			} else {
				// remove the candidate
				if (j+1 < num_candidates) {
					mysrvcCandidates[j] = mysrvcCandidates[num_candidates-1];
				}
				j--;
				num_candidates--;
			}
		}
*/

		unsigned int New_sum=sum;

		if (New_sum==0) {
			proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PgSQL_SrvC NULL because no backend ONLINE or with weight\n");
			if (l>32) {
				free(mysrvcCandidates);
			}
#ifdef TEST_AURORA
			array_mysrvc_cands += num_candidates;
#endif // TEST_AURORA
			return NULL; // if we reach here, we couldn't find any target
		}

		// latency awareness algorithm is enabled only when compiled with USE_MYSRVC_ARRAY
		if (sess && sess->thread->variables.min_num_servers_lantency_awareness) {
			if ((int) num_candidates >= sess->thread->variables.min_num_servers_lantency_awareness) {
				unsigned int servers_with_latency = 0;
				unsigned int total_latency_us = 0;
				// scan and verify that all servers have some latency
				for (j=0; j<num_candidates; j++) {
					mysrvc = mysrvcCandidates[j];
					const unsigned int current_latency_us =
						mysrvc->current_latency_us_value();
					if (current_latency_us) {
						servers_with_latency++;
						total_latency_us += current_latency_us;
					}
				}
				if (servers_with_latency == num_candidates) {
					// all servers have some latency.
					// That is good. If any server have no latency, something is wrong
					// and we will skip this algorithm
					sess->thread->status_variables.stvar[st_var_ConnPool_get_conn_latency_awareness]++;
					unsigned int avg_latency_us = 0;
					avg_latency_us = total_latency_us/num_candidates;
					for (j=0; j<num_candidates; j++) {
						mysrvc = mysrvcCandidates[j];
						if (mysrvc->current_latency_us_value() > avg_latency_us) {
							// remove the candidate
							if (j+1 < num_candidates) {
								mysrvcCandidates[j] = mysrvcCandidates[num_candidates-1];
							}
							j--;
							num_candidates--;
						}
					}
					// we scan again to adjust weight
					New_sum = 0;
					for (j=0; j<num_candidates; j++) {
						mysrvc = mysrvcCandidates[j];
						New_sum+=mysrvc->weight;
					}
				}
			}
		}


		unsigned int k;
		//if (New_sum > 32768) {
		//	k=rand()%New_sum;
		//} else {
		//	k=fastrand()%New_sum;
		//}
		k = rand_fast() % New_sum;
		k++;
		New_sum=0;

		for (j=0; j<num_candidates; j++) {
			mysrvc = mysrvcCandidates[j];
			New_sum+=mysrvc->weight;
			if (k<=New_sum) {
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PgSQL_SrvC %p, server %s:%d\n", mysrvc, mysrvc->address, mysrvc->port);
				if (l>32) {
					free(mysrvcCandidates);
				}
#ifdef TEST_AURORA
				array_mysrvc_cands += num_candidates;
#endif // TEST_AURORA
				return mysrvc;
			}
		}
	} else {
		time_t t = time(NULL);

		if (t - last_hg_log > 1) {
			last_hg_log = time(NULL);
			proxy_error("Hostgroup %u has no servers available!\n", hid);
		}
	}
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PgSQL_SrvC NULL\n");
	if (l>32) {
		free(mysrvcCandidates);
	}
#ifdef TEST_AURORA
	array_mysrvc_cands += num_candidates;
#endif // TEST_AURORA
	return NULL; // if we reach here, we couldn't find any target
}

//unsigned int PgSQL_SrvList::cnt() {
//	return servers->len;
//}

//PgSQL_SrvC * PgSQL_SrvList::idx(unsigned int i) { return (PgSQL_SrvC *)servers->index(i); }

void PgSQL_SrvConnList::get_random_MyConn_inner_search(
		unsigned int start, unsigned int end, unsigned int& conn_found_idx,
		unsigned int& connection_quality_level,
		unsigned int& number_of_matching_session_variables,
		PgSQL_Session* sess
#if POLARDB_PROXY
		, const PolarDB_StartupProfile* startup_profile,
		const PolarDB_StartupClientContext* startup_client,
		bool has_reader_pool_reservations
#endif // POLARDB_PROXY
		) {
	PgSQL_Connection * conn=NULL;
	unsigned int k;
	const PgSQL_Connection* client_conn =
		sess && sess->client_myds ? sess->client_myds->myconn : nullptr;
	for (k = start;  k < end; k++) {
		conn = (PgSQL_Connection *)conns->index(k);
#if POLARDB_PROXY
		if (has_reader_pool_reservations &&
				mysrvc->polardb_connection_reserved_unlocked(conn)) {
			continue;
		}
		if (startup_profile &&
				!polardb_connection_startup_settings_match(
					conn, *startup_profile, startup_client,
					pgsql_thread___polardb_startup_config_generation,
					pgsql_thread___polardb_proxy_identity_mode)) {
			continue;
		}
		// A removed PolarDB hostgroup mapping must not turn an RFQ-started
		// connection into an unrestricted classic-pool entry. Unmarked core
		// connections and connections opened without RFQ fields remain usable.
		if (!startup_profile && conn &&
				conn->polardb_startup_settings_set &&
				conn->polardb_startup_profile.emits_startup_params()) {
			continue;
		}
#endif // POLARDB_PROXY
		if (conn->has_same_connection_options(client_conn)) {
			if (connection_quality_level == 0) {
				// this is our best candidate so far
				connection_quality_level = 1;
				conn_found_idx = k;
			}
			if (conn->requires_RESETTING_CONNECTION(client_conn)==false) {
				if (connection_quality_level == 1) {
					// this is our best candidate so far
					connection_quality_level = 2;
					conn_found_idx = k;
				}
				unsigned int cnt_match = 0; // number of matching session variables
				unsigned int not_match = 0; // number of not matching session variables
				cnt_match = conn->number_of_matching_session_variables(client_conn, not_match);

				if (not_match==0) {
					// it seems we found the perfect connection
					number_of_matching_session_variables = cnt_match;
					connection_quality_level = 3;
					conn_found_idx = k;
					return; // exit immediately, we found the perfect connection
				} else {
					// we didn't find the perfect connection
					// but maybe is better than what we have so far?
					if (cnt_match > number_of_matching_session_variables) {
						// this is our best candidate so far
						number_of_matching_session_variables = cnt_match;
						conn_found_idx = k;
					}
				}
			} else {
				/*if (connection_quality_level == 1) {
					int rca = pgsql_thread___reset_connection_algorithm;
					if (rca==1) {
						int ql = GloPTH->variables.connpoll_reset_queue_length;
						if (ql==0) {
							// if:
							// pgsql-reset_connection_algorithm=1 and
							// pgsql-connpoll_reset_queue_length=0
							// we will not return a connection with connection_quality_level == 1
							// because we want to run COM_CHANGE_USER
							// This change was introduced to work around Galera bug
							// https://github.com/codership/galera/issues/613
							connection_quality_level = 0;
						}
					}
				}
				*/
			}
		}
	}
}

PgSQL_Connection * PgSQL_SrvConnList::get_random_MyConn_unlocked(
		PgSQL_Session *sess, bool ff, bool only_pooled,
		std::vector<PgSQL_Connection*>* connections_to_delete,
		bool exact_only) {
	PgSQL_Connection * conn=NULL;
	unsigned int i;
	unsigned int conn_found_idx = 0;
	unsigned int l=conns_length();

	if (only_pooled && l == 0) {
		return NULL;
	}

	unsigned int connection_quality_level = 0;
	bool needs_warming = false;
	// connection_quality_level:
	// 0 : not found any good connection, tracked options are not OK
	// 1 : tracked options are OK , but RESETTING SESSION is required
	// 2 : tracked options are OK , RESETTING SESSION is not required, but some SET statement or INIT_DB needs to be executed
	// 3 : tracked options are OK , RESETTING SESSION is not required, and it seems that SET statements or INIT_DB ARE not required
	unsigned int number_of_matching_session_variables = 0; // this includes session variables AND schema
	bool connection_warming = pgsql_thread___connection_warming;
	int free_connections_pct = pgsql_thread___free_connections_pct;
	if (mysrvc->myhgc->attributes.configured == true) {
		// pgsql_hostgroup_attributes takes priority
		connection_warming = mysrvc->myhgc->attributes.connection_warming;
		free_connections_pct = mysrvc->myhgc->attributes.free_connections_pct;
	}
	unsigned int conns_free = mysrvc->ConnectionsFree->conns_length();
	unsigned int conns_used = mysrvc->ConnectionsUsed->conns_length();
	if (connection_warming == true) {
		unsigned int total_connections = conns_free + conns_used;
#if POLARDB_PROXY
		total_connections = mysrvc->polardb_pool_total_count();
#endif // POLARDB_PROXY
		unsigned int expected_warm_connections = (unsigned int)free_connections_pct * mysrvc->max_connections / 100;
		if (total_connections < expected_warm_connections) {
			needs_warming = true;
		}
	}
	if (l && ff==false && needs_warming==false) {
		i = rand_fast() % l;
		if (sess && sess->client_myds && sess->client_myds->myconn && sess->client_myds->myconn->userinfo) {
#if POLARDB_PROXY
			const bool has_reader_pool_reservations =
				!used_list &&
				!mysrvc->polardb_reader_pool_reservation_token_by_connection.empty();
			const unsigned int hostgroup_id =
				mysrvc && mysrvc->myhgc ? mysrvc->myhgc->hid : 0;
			PolarDB_StartupProfile startup_profile;
			const PolarDB_StartupProfile* startup_profile_ptr = nullptr;
			PolarDB_StartupClientContext startup_client;
			const PolarDB_StartupClientContext* startup_client_ptr = nullptr;
			const auto* polardb_config =
				sess && sess->thread && sess->thread->polardb_is_active() && PgHGM
					? PgHGM->find_thread_cached_polardb_hg_config(hostgroup_id)
					: nullptr;
			if (polardb_config) {
				startup_profile =
					PgSQL_HostGroups_Manager::polardb_startup_profile_for_config(
					*polardb_config, pgsql_thread___polardb_proxy_protocol,
					pgsql_thread___polardb_profile_off);
				startup_profile_ptr = &startup_profile;
				if (startup_profile.emits_startup_params() &&
						polardb_startup_client_from_session(
							sess, &startup_client)) {
					startup_client_ptr = &startup_client;
				}
			}
#endif // POLARDB_PROXY
			get_random_MyConn_inner_search(
				i, l, conn_found_idx, connection_quality_level,
				number_of_matching_session_variables, sess
#if POLARDB_PROXY
				, startup_profile_ptr, startup_client_ptr,
				has_reader_pool_reservations
#endif // POLARDB_PROXY
				);
			if (connection_quality_level !=3 ) { // we didn't find the perfect connection
				get_random_MyConn_inner_search(
					0, i, conn_found_idx, connection_quality_level,
					number_of_matching_session_variables, sess
#if POLARDB_PROXY
					, startup_profile_ptr, startup_client_ptr,
					has_reader_pool_reservations
#endif // POLARDB_PROXY
					);
			}
			if (exact_only) {
				if (connection_quality_level != 3) {
					return NULL;
				}
				conn = remove_unlocked(conn_found_idx);
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7,
					"Returning exact PostgreSQL Connection %p, server %s:%d\n",
					conn, conn->parent->address, conn->parent->port);
				return conn;
			}
			// Evaluate pool state to determine create-vs-reuse and eviction (warming was already checked above)
			ConnectionPoolDecision decision = evaluate_pool_state(
				conns_free, conns_used, (unsigned int)mysrvc->max_connections,
				connection_quality_level, false, 0
			);
			// connection_quality_level:
			// 1 : tracked options are OK , but RESETTING SESSION is required
			// 2 : tracked options are OK , RESETTING SESSION is not required, but some SET statement or INIT_DB needs to be executed
			switch (connection_quality_level) {
			case 0: // not found any good connection, tracked options are not OK
					if (only_pooled) {
						// Quality zero means the hard user/database compatibility
						// wall failed. PostgreSQL cannot repair that connection with
						// DISCARD ALL or setting the requested session variables.
						return NULL;
					}
					// we must check if connections need to be freed before
					// creating a new connection
					{
						bool can_create = true;
						if (decision.evict_connections) {
#if POLARDB_PROXY
							assert(connections_to_delete);
							can_create =
								polardb_make_room_for_classic_connection_unlocked(
								mysrvc, decision.num_to_evict,
								*connections_to_delete);
#else
							unsigned int cur_free = conns_free;
							unsigned int connections_to_free = decision.num_to_evict;
							while (cur_free && connections_to_free) {
								PgSQL_Connection* c = mysrvc->ConnectionsFree->remove(0);
								delete c;

								cur_free = mysrvc->ConnectionsFree->conns_length();
								connections_to_free -= 1;
							}
#endif // POLARDB_PROXY
						} else {
#if POLARDB_PROXY
							assert(connections_to_delete);
							can_create =
								polardb_make_room_for_classic_connection_unlocked(
								mysrvc, 0, *connections_to_delete);
#endif // POLARDB_PROXY
						}

						// we must create a new connection
						if (!can_create) {
							return NULL;
						}
						conn = pgsql_create_backend_connection_unlocked(mysrvc);
					}
					break;
				case 1: //tracked options are OK , but RESETTING SESSION is required
					// we may consider creating a new connection
					{
						if (only_pooled) {
							conn=remove_unlocked(conn_found_idx);
							break;
						}
						if (decision.create_new_connection) {
#if POLARDB_PROXY
							assert(connections_to_delete);
							if (!polardb_make_room_for_classic_connection_unlocked(
									mysrvc, 0, *connections_to_delete)) {
								return NULL;
							}
#endif // POLARDB_PROXY
							conn = pgsql_create_backend_connection_unlocked(mysrvc);
						} else {
							conn=remove_unlocked(conn_found_idx);
						}
					}
					break;
				case 2: // tracked options are OK , RESETTING SESSION is not required, but some SET statement or INIT_DB needs to be executed
				case 3: // tracked options are OK , RESETTING SESSION is not required, and it seems that SET statements or INIT_DB ARE not required
					// here we return the best connection we have, no matter if connection_quality_level is 2 or 3
					conn=remove_unlocked(conn_found_idx);
					break;
				default: // this should never happen
					// LCOV_EXCL_START
					assert(0);
					break;
					// LCOV_EXCL_STOP
			}
		} else {
			conn=remove_unlocked(i);
		}
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PostgreSQL Connection %p, server %s:%d\n", conn, conn->parent->address, conn->parent->port);
		return conn;
	} else {
		if (only_pooled) {
			// No compatibility search ran on this path (fast-forward,
			// warming, or missing client context), so an arbitrary pooled
			// connection cannot satisfy a pooled-only match.
			return NULL;
		}
		if (pgsql_connection_creation_throttled_unlocked(mysrvc)) {
			return NULL;
		} else {
#if POLARDB_PROXY
			assert(connections_to_delete);
			if (!polardb_make_room_for_classic_connection_unlocked(
					mysrvc, 0, *connections_to_delete)) {
				return NULL;
			}
#endif // POLARDB_PROXY
			conn = pgsql_create_backend_connection_unlocked(mysrvc);
			return  conn;
		}
	}
	return NULL; // never reach here
}

void PgSQL_HostGroups_Manager::unshun_server_all_hostgroups(const char * address, uint16_t port, time_t t, int max_wait_sec, unsigned int *skip_hid) {
	// we scan all hostgroups looking for a specific server to unshun
	// if skip_hid is not NULL , the specific hostgroup is skipped
	if (GloPTH->variables.hostgroup_manager_verbose >= 3) {
		char buf[64];
		if (skip_hid == NULL) {
			sprintf(buf,"NULL");
		} else {
			sprintf(buf,"%u", *skip_hid);
		}
		proxy_info("Calling unshun_server_all_hostgroups() for server %s:%d . Arguments: %lu , %d , %s\n" , address, port, t, max_wait_sec, buf);
	}
	int i, j;
	for (i=0; i<(int)MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		if (skip_hid != NULL && myhgc->hid == *skip_hid) {
			// if skip_hid is not NULL, we skip that specific hostgroup
			continue;
		}
		bool found = false; // was this server already found in this hostgroup?
		for (j=0; found==false && j<(int)myhgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->index(j);
			if (mysrvc->status==MYSQL_SERVER_STATUS_SHUNNED) {
				// we only care for SHUNNED nodes
				// Note that we check for address and port only for status==MYSQL_SERVER_STATUS_SHUNNED ,
				// that means that potentially we will pass by the matching node and still looping .
				// This is potentially an optimization because we only check status and do not perform any strcmp()
				if (strcmp(mysrvc->address,address)==0 && mysrvc->port==port) {
					// we found the server in this hostgroup
					// no need to process more servers in the same hostgroup
					found = true;
					if (t > mysrvc->time_last_detected_error && (t - mysrvc->time_last_detected_error) > max_wait_sec) {
						if (
							(mysrvc->shunned_and_kill_all_connections==false) // it is safe to bring it back online
							||
							(mysrvc->shunned_and_kill_all_connections==true && pgsql_pool_no_live_connections(mysrvc)) // if shunned_and_kill_all_connections is set, ensure all connections are already dropped
						) {
							if (GloPTH->variables.hostgroup_manager_verbose >= 3) {
								proxy_info("Unshunning server %d:%s:%d . time_last_detected_error=%lu\n", mysrvc->myhgc->hid, address, port, mysrvc->time_last_detected_error);
							}
							mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);
							mysrvc->shunned_automatic=false;
							mysrvc->shunned_and_kill_all_connections=false;
							mysrvc->connect_ERR_at_time_last_detected_error=0;
							mysrvc->time_last_detected_error=0;
						}
					}
				}
			}
		}
	}
}

PgSQL_Connection * PgSQL_HostGroups_Manager::get_MyConn_from_pool(unsigned int _hid, PgSQL_Session *sess, bool ff, char * gtid_uuid, uint64_t gtid_trxid, int max_lag_ms, bool only_pooled) {
	PgSQL_Connection * conn=NULL;
	std::vector<PgSQL_Connection*> connections_to_delete;
	pgsql_pool_status_count_get(sess ? sess->thread : NULL,
		&status.pgconnpoll_get);
#if POLARDB_PROXY
	PgSQL_SrvC *mysrvc = NULL;
	std::shared_ptr<const PolarDB_ServerListSnapshot> selected_server_snapshot;

	// Server choice remains under HGM. Hold the current server-list snapshot
	// after releasing HGM so the selected server cannot be retired while its
	// own pool lock is acquired.
	wrlock();
	PgSQL_HGC *myhgc=MyHGC_lookup(_hid);
#ifdef TEST_AURORA
	for (int i=0; i<10; i++)
#endif // TEST_AURORA
	if (myhgc) {
		mysrvc = myhgc->get_random_MySrvC(
			gtid_uuid, gtid_trxid, max_lag_ms, sess);
	}
	if (mysrvc) {
		selected_server_snapshot = get_polardb_server_list_snapshot();
	}
	wrunlock();

	// The common classic hit is exact and already pooled. Move only that case
	// under the selected server lock, outside HGM. Reset, creation, warming and
	// unusual compatibility cases retain the established slow path below.
	if (mysrvc && selected_server_snapshot) {
		POLARDB_PERF_THREAD_COUNT_ONE(
			sess ? sess->thread : nullptr, perf_core_pool_exact_attempt);
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
		if (mysrvc->status == MYSQL_SERVER_STATUS_ONLINE &&
				mysrvc->ConnectionsFree && mysrvc->ConnectionsUsed) {
			conn = mysrvc->ConnectionsFree->get_random_MyConn_unlocked(
				sess, ff, true, &connections_to_delete, true);
		}
		if (conn) {
			mysrvc->ConnectionsUsed->add_unlocked(conn);
			pgsql_pool_status_count_get_ok(sess ? sess->thread : NULL,
				&status.pgconnpoll_get_ok);
			mysrvc->update_max_connections_used();
		}
		if (conn) {
			POLARDB_PERF_THREAD_COUNT_ONE(
				sess ? sess->thread : nullptr,
				perf_core_pool_exact_hit);
		} else {
			POLARDB_PERF_THREAD_COUNT_ONE(
				sess ? sess->thread : nullptr,
				perf_core_pool_exact_miss);
		}
	}
	for (PgSQL_Connection* connection : connections_to_delete) {
		delete connection;
	}
	connections_to_delete.clear();
	if (conn) {
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7,
			"Returning PostgreSQL Connection %p, server %s:%d\n",
			conn, conn->parent->address, conn->parent->port);
		return conn;
	}

	// A miss may require reset or creation and therefore still needs HGM.
	// Select again because topology or load may have changed after the fast
	// attempt.
	wrlock();
	myhgc = MyHGC_lookup(_hid);
	mysrvc = NULL;
#ifdef TEST_AURORA
	for (int i=0; i<10; i++)
#endif // TEST_AURORA
	if (myhgc) {
		mysrvc = myhgc->get_random_MySrvC(
			gtid_uuid, gtid_trxid, max_lag_ms, sess);
	}
	if (mysrvc) {
		std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
		conn = mysrvc->ConnectionsFree->get_random_MyConn_unlocked(
			sess, ff, only_pooled, &connections_to_delete);
		if (conn) {
			mysrvc->ConnectionsUsed->add_unlocked(conn);
			pgsql_pool_status_count_get_ok(sess ? sess->thread : NULL,
				&status.pgconnpoll_get_ok);
			mysrvc->update_max_connections_used();
		}
	}
	wrunlock();
	for (PgSQL_Connection* connection : connections_to_delete) {
		delete connection;
	}
#else
	wrlock();
	PgSQL_HGC *myhgc=MyHGC_lookup(_hid);
	PgSQL_SrvC *mysrvc = NULL;
#ifdef TEST_AURORA
	for (int i=0; i<10; i++)
#endif // TEST_AURORA
	if (myhgc) {
		mysrvc = myhgc->get_random_MySrvC(
			gtid_uuid, gtid_trxid, max_lag_ms, sess);
	}
	if (mysrvc) {
		conn=mysrvc->ConnectionsFree->get_random_MyConn_unlocked(
			sess, ff, only_pooled, &connections_to_delete);
		if (conn) {
			mysrvc->ConnectionsUsed->add(conn);
			pgsql_pool_status_count_get_ok(sess ? sess->thread : NULL,
				&status.pgconnpoll_get_ok);
			mysrvc->update_max_connections_used();
		}
	}
	wrunlock();
	for (PgSQL_Connection* connection : connections_to_delete) {
		delete connection;
	}
#endif // POLARDB_PROXY
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning PostgreSQL Connection %p, server %s:%d\n", conn, (conn ? conn->parent->address : "") , (conn ? conn->parent->port : 0 ));
	return conn;
}

#if POLARDB_PROXY
PgSQL_PoolGetResult
PgSQL_HostGroups_Manager::get_connection_from_selected_server(
		PgSQL_SrvC* srv, unsigned int expected_hostgroup_id,
		const PgSQL_PoolMatchKey& match_key,
		PgSQL_Session* sess, PgSQL_PoolGetMode mode,
		unsigned int selected_max_connections,
		uint64_t expected_server_list_generation,
		uint64_t expected_startup_config_generation) {
	PgSQL_PoolGetResult result;
	if (!srv) {
		return result;
	}

	if (pgsql_pool_get_mode_has(mode,
			PgSQL_PoolGetMode::ALLOW_EXACT_MATCH) ||
			pgsql_pool_get_mode_has(mode, PgSQL_PoolGetMode::ALLOW_RESET)) {
#if POLARDB_PROFILE
		PgSQL_Thread* thread = sess ? sess->thread : nullptr;
#endif // POLARDB_PROFILE
		unsigned long long lock_wait_us = 0;
		unsigned long long lock_hold_us = 0;
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_take_attempt);
		result = srv->take_existing_connection(
			sess, match_key, mode, selected_max_connections,
			&lock_wait_us, &lock_hold_us);
		if (result.conn) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread,
				reader_pool_shared_take_hit);
		} else if (result.pool_busy) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread,
				reader_pool_shared_take_busy);
		} else {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread,
				reader_pool_shared_take_miss);
		}
#if POLARDB_PROXY
		if (!result.pool_busy) {
			POLARDB_PROFILE_THREAD_COUNT(
				thread,
				selected_server_pool_lock_wait_sum_us, lock_wait_us);
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread,
				selected_server_pool_lock_wait_count);
			POLARDB_PROFILE_THREAD_COUNT(
				thread,
				selected_server_pool_lock_hold_sum_us, lock_hold_us);
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread,
				selected_server_pool_lock_hold_count);
		}
#endif // POLARDB_PROXY
		if (result.conn) {
			return result;
		}
		if (result.pool_busy) {
			return result;
		}
	}
	if (!pgsql_pool_get_mode_has(
			mode, PgSQL_PoolGetMode::ALLOW_CREATE)) {
		// Snapshot saturation is sufficient for capacity probes. Creation
		// checks current limits again while holding HGM below.
		if (result.server_saturated) {
			POLARDB_PROFILE_STATUS_COUNT_ONE(reader_pool_confirmed_saturated);
		}
		return result;
	}
	result.server_saturated = false;

	// Creating changes global limits, so check the selected server again while
	// holding HGM. This function must not choose a different server.
	POLARDB_PROFILE_STATUS_COUNT_ONE(reader_pool_hgm_create_lock_entry);
	wrlock();
#if POLARDB_PROXY
	if ((expected_server_list_generation != 0 &&
			polardb_server_list_generation_.load(std::memory_order_acquire) !=
				expected_server_list_generation) ||
		(expected_startup_config_generation != 0 &&
			(!GloPTH ||
			 GloPTH->get_polardb_startup_config_generation() !=
				expected_startup_config_generation))) {
		result.retry_after_config_change = true;
		wrunlock();
		return result;
	}
#endif // POLARDB_PROXY
	if (srv->status != MYSQL_SERVER_STATUS_ONLINE || !srv->myhgc ||
			srv->myhgc->hid != expected_hostgroup_id ||
			srv->weight <= 0 || !pgsql_srv_latency_allowed(srv) ||
			srv->max_connections <= 0 ||
			pgsql_connection_creation_throttled_unlocked(srv)) {
		wrunlock();
		return result;
	}
	std::vector<PgSQL_Connection*> connections_to_delete;
	{
		std::lock_guard<std::recursive_mutex> pool_lock(srv->pool_mutex);
		// A compatible connection may have returned while this request waited
		// for HGM. Recheck the complete permitted reuse set before creating.
		srv->take_reusable_connection_unlocked(
			sess, match_key, mode, result);
		if (result.conn) {
			srv->update_max_connections_used();
		} else {
			POLARDB_STATUS_COUNT_ONE(reader_pool_create_decision);
			const unsigned int max_connections =
				static_cast<unsigned int>(srv->max_connections);
			const unsigned int used = srv->pool_used_count_value();
			const unsigned int free = srv->pool_free_count_value();
			bool creation_capacity_blocked = used >= max_connections;
			if (!creation_capacity_blocked) {
				const unsigned int total = used + free;
				const unsigned int evict_count = total >= max_connections
					? total - max_connections + 1 : 0;
				if (srv->evict_unreserved_free_for_create(
						evict_count, evict_count, connections_to_delete)) {
					result.conn = pgsql_create_backend_connection_unlocked(srv);
					if (result.conn) {
						if (srv->add_used_matching_connection(
								result.conn, match_key)) {
							result.conn->polardb_reader_pool_created = true;
							result.conn->polardb_reader_pool_connect_pending.store(
								true, std::memory_order_release);
							POLARDB_STATUS_COUNT_ONE(
								reader_pool_create_issued);
							result.source = PgSQL_PoolGetSource::CREATED;
						} else {
							connections_to_delete.push_back(result.conn);
							result.conn = nullptr;
						}
					}
				} else {
					creation_capacity_blocked = true;
				}
			}
			result.server_saturated =
				!result.conn && (creation_capacity_blocked ||
					(free == 0 && used >= max_connections));
		}
	}
	for (PgSQL_Connection* conn : connections_to_delete) {
		delete conn;
	}
	wrunlock();
	return result;
}
#endif // POLARDB_PROXY

void PgSQL_HostGroups_Manager::destroy_MyConn_from_pool(PgSQL_Connection *c, bool _lock) {
	bool to_del=true; // the default, legacy behavior
	PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)c->parent;
#if POLARDB_PROXY
	mysrvc->polardb_finish_idle_ping(c, true);
#endif // POLARDB_PROXY
	if (mysrvc->status==MYSQL_SERVER_STATUS_ONLINE && c->send_quit) {
		if (c->async_state_machine!=ASYNC_IDLE) {
			// the connection seems health, but we are trying to destroy it
			// probably because there is a long running query
			// therefore we will try to kill the connection

			if (pgsql_thread___kill_backend_connection_when_disconnect) {
				if (c->is_connected()) {
					const PgSQL_Connection_userinfo* ui = c->userinfo;

					std::unique_ptr<PgSQL_Backend_Kill_Args> backend_kill_args = std::make_unique<PgSQL_Backend_Kill_Args>(
						(PGconn*)c->get_pg_connection(), ui->username, ui->password, ui->dbname, c->parent->address,
						c->parent->port, c->parent->myhgc->hid, c->parent->use_ssl,
						PgSQL_Backend_Kill_Args::TYPE::TERMINATE_CONNECTION, nullptr
					);

					pthread_attr_t attr;
					pthread_attr_init(&attr);
					pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
					pthread_attr_setstacksize(&attr, 256 * 1024);
					pthread_t pt;
					if (pthread_create(&pt, &attr, &PgSQL_backend_kill_thread, backend_kill_args.release()) != 0) {
						// LCOV_EXCL_START
						proxy_error("Thread creation\n");
						assert(0);
						// LCOV_EXCL_STOP
					}
				}
			}
		}
	}
	if (to_del) {
		// we lock only this part of the code because we need to remove the connection from ConnectionsUsed
		if (_lock) {
			wrlock();
		}
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Destroying PgSQL_Connection %p, server %s:%d Error %s\n", c, mysrvc->address, mysrvc->port,
			c->get_error_code_with_message().c_str());
#if POLARDB_PROXY
		{
			std::lock_guard<std::recursive_mutex> pool_lock(mysrvc->pool_mutex);
#endif // POLARDB_PROXY
			mysrvc->ConnectionsUsed->remove(c);
#if POLARDB_PROXY
		}
#endif // POLARDB_PROXY
		status.pgconnpoll_destroy++;
		delete c;
		if (_lock) {
			wrunlock();
		}
	}
}

inline double get_prometheus_counter_val(
	std::map<std::string, prometheus::Counter*>& counter_map, const std::string& endpoint_id
) {
	const auto& counter_entry = counter_map.find(endpoint_id);
	double current_val = 0;

	if (counter_entry != counter_map.end()) {
		current_val = counter_entry->second->Value();
	}

	return current_val;
}

void reset_hg_attrs_server_defaults(PgSQL_SrvC* mysrvc) {
	mysrvc->weight = -1;
	mysrvc->max_connections = -1;
	mysrvc->use_ssl = -1;
}

void update_hg_attrs_server_defaults(PgSQL_SrvC* mysrvc, PgSQL_HGC* myhgc) {
	if (mysrvc->weight == -1) {
		if (myhgc->servers_defaults.weight != -1) {
			mysrvc->weight = myhgc->servers_defaults.weight;
		} else {
			// Same harcoded default as in 'CREATE TABLE pgsql_servers ...'
			mysrvc->weight = 1;
		}
	}
	if (mysrvc->max_connections == -1) {
		if (myhgc->servers_defaults.max_connections != -1) {
			mysrvc->max_connections = myhgc->servers_defaults.max_connections;
		} else {
			// Same harcoded default as in 'CREATE TABLE pgsql_servers ...'
			mysrvc->max_connections = 1000;
		}
	}
	if (mysrvc->use_ssl == -1) {
		if (myhgc->servers_defaults.use_ssl != -1) {
			mysrvc->use_ssl = myhgc->servers_defaults.use_ssl;
		} else {
			// Same harcoded default as in 'CREATE TABLE pgsql_servers ...'
			mysrvc->use_ssl = 0;
		}
	}
}

void PgSQL_HostGroups_Manager::add(PgSQL_SrvC *mysrvc, unsigned int _hid) {
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Adding PgSQL_SrvC %p (%s:%d) for hostgroup %d\n", mysrvc, mysrvc->address, mysrvc->port, _hid);

	// Since metrics for servers are stored per-endpoint; the metrics for a particular endpoint can live longer than the
	// 'PgSQL_SrvC' itself. For example, a failover or a server config change could remove the server from a particular
	// hostgroup, and a subsequent one bring it back to the original hostgroup. For this reason, everytime a 'mysrvc' is
	// created and added to a particular hostgroup, we update the endpoint metrics for it.
	std::string endpoint_id { std::to_string(_hid) + ":" + string { mysrvc->address } + ":" + std::to_string(mysrvc->port) };

	mysrvc->bytes_recv = get_prometheus_counter_val(this->status.p_conn_pool_bytes_data_recv_map, endpoint_id);
	mysrvc->bytes_sent = get_prometheus_counter_val(this->status.p_conn_pool_bytes_data_sent_map, endpoint_id);
	mysrvc->connect_ERR = get_prometheus_counter_val(this->status.p_connection_pool_conn_err_map, endpoint_id);
	mysrvc->connect_OK = get_prometheus_counter_val(this->status.p_connection_pool_conn_ok_map, endpoint_id);
	mysrvc->queries_sent = get_prometheus_counter_val(this->status.p_connection_pool_queries_map, endpoint_id);

	PgSQL_HGC *myhgc=MyHGC_lookup(_hid);
	update_hg_attrs_server_defaults(mysrvc, myhgc);
#if POLARDB_PROXY
	polardb_fast_topology_wrlock();
#endif // POLARDB_PROXY
	myhgc->mysrvs->add(mysrvc);
#if POLARDB_PROXY
	polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
	polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
}

void PgSQL_HostGroups_Manager::replication_lag_action_inner(PgSQL_HGC *myhgc, const char *address, unsigned int port, int current_replication_lag) {
	int j;
	for (j=0; j<(int)myhgc->mysrvs->cnt(); j++) {
		PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->index(j);
		if (strcmp(mysrvc->address,address)==0 && mysrvc->port==port) {
			if (mysrvc->status==MYSQL_SERVER_STATUS_ONLINE) {
				if (
//					(current_replication_lag==-1 )
//					||
					(
						current_replication_lag>=0 &&
						mysrvc->max_replication_lag > 0 && // see issue #4018
						((unsigned int)current_replication_lag > mysrvc->max_replication_lag)
					)
				) {
					// always increase the counter
					mysrvc->cur_replication_lag_count += 1;
					if (mysrvc->cur_replication_lag_count >= (unsigned int)pgsql_thread___monitor_replication_lag_count) {
						proxy_warning("Shunning server %s:%d from HG %u with replication lag of %d second, count number: '%d'\n", address, port, myhgc->hid, current_replication_lag, mysrvc->cur_replication_lag_count);
						mysrvc->set_status(MYSQL_SERVER_STATUS_SHUNNED_REPLICATION_LAG);
					} else {
						proxy_info(
							"Not shunning server %s:%d from HG %u with replication lag of %d second, count number: '%d' < replication_lag_count: '%d'\n",
							address,
							port,
							myhgc->hid,
							current_replication_lag,
							mysrvc->cur_replication_lag_count,
							pgsql_thread___monitor_replication_lag_count
						);
					}
				} else {
					mysrvc->cur_replication_lag_count = 0;
				}
			} else {
				if (mysrvc->status==MYSQL_SERVER_STATUS_SHUNNED_REPLICATION_LAG) {
					if (
						(current_replication_lag>=0 && ((unsigned int)current_replication_lag <= mysrvc->max_replication_lag))
						||
						(current_replication_lag==-2) // see issue 959
					) {
						mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);
						proxy_warning("Re-enabling server %s:%d from HG %u with replication lag of %d second\n", address, port, myhgc->hid, current_replication_lag);
						mysrvc->cur_replication_lag_count = 0;
					}
				}
			}
			return;
		}
	}
}

void PgSQL_HostGroups_Manager::replication_lag_action(const std::list<replication_lag_server_t>& pgsql_servers) {

	//this method does not use admin table, so this lock is not needed. 
	//GloAdmin->mysql_servers_wrlock();
	unsigned long long curtime1 = monotonic_time();
	wrlock();

	for (const auto& server : pgsql_servers) {

		const int hid = std::get<PgSQL_REPLICATION_LAG_SERVER_T::PG_RLS_HOSTGROUP_ID>(server);
		const std::string& address = std::get<PgSQL_REPLICATION_LAG_SERVER_T::PG_RLS_ADDRESS>(server);
		const unsigned int port = std::get<PgSQL_REPLICATION_LAG_SERVER_T::PG_RLS_PORT>(server);
		const int current_replication_lag = std::get<PgSQL_REPLICATION_LAG_SERVER_T::PG_RLS_CURRENT_REPLICATION_LAG>(server);

		if (/* pgsql_thread___monitor_replication_lag_group_by_host == */ false) { // feature currently not enabled
			// legacy check. 1 check per server per hostgroup
			PgSQL_HGC *myhgc = MyHGC_find(hid);
			replication_lag_action_inner(myhgc,address.c_str(),port,current_replication_lag);
		}
		else {
			// only 1 check per server, no matter the hostgroup
			// all hostgroups must be searched
			for (unsigned int i=0; i<MyHostGroups->len; i++) {
				PgSQL_HGC*myhgc=(PgSQL_HGC*)MyHostGroups->index(i);
				replication_lag_action_inner(myhgc,address.c_str(),port,current_replication_lag);
			}
		}
	}

	wrunlock();
	//GloAdmin->mysql_servers_wrunlock();

	unsigned long long curtime2 = monotonic_time();
	curtime1 = curtime1 / 1000;
	curtime2 = curtime2 / 1000;
	proxy_debug(PROXY_DEBUG_MONITOR, 7, "PgSQL_HostGroups_Manager::replication_lag_action() locked for %llums (server count:%ld)\n", curtime2 - curtime1, pgsql_servers.size());
}

void PgSQL_HostGroups_Manager::drop_all_idle_connections() {
	// NOTE: the caller should hold wrlock
	int i, j;
	for (i=0; i<(int)MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		for (j=0; j<(int)myhgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->index(j);
#if POLARDB_PROXY
			std::vector<PgSQL_Connection*> connections_to_delete;
			{
				std::lock_guard<std::recursive_mutex> pool_lock(
					mysrvc->pool_mutex);
				PgSQL_SrvConnList *free_connections = mysrvc->ConnectionsFree;
				if (mysrvc->status != MYSQL_SERVER_STATUS_ONLINE) {
					proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Server %s:%d is not online\n", mysrvc->address, mysrvc->port);
					free_connections->detach_all_unlocked(
						connections_to_delete,
						ReaderReservationEndReason::SERVER_OFFLINE);
				}

				const unsigned int max_connections =
					mysrvc->max_connections > 0
						? static_cast<unsigned int>(mysrvc->max_connections)
						: 0;
				while (free_connections->conns->len &&
						mysrvc->ConnectionsUsed->conns->len +
							free_connections->conns->len > max_connections) {
					connections_to_delete.push_back(
						free_connections->remove_unlocked(
							0, ReaderReservationEndReason::IDLE_TRIM));
				}

				int free_connections_pct = pgsql_thread___free_connections_pct;
				if (mysrvc->myhgc->attributes.configured == true) {
					free_connections_pct =
						mysrvc->myhgc->attributes.free_connections_pct;
				}
				const unsigned int max_free_connections =
					static_cast<unsigned int>(free_connections_pct) *
						max_connections / 100;
				mysrvc->polardb_trim_free_connections_to_max_unlocked(
					max_free_connections, connections_to_delete);

				if (pgsql_thread___connection_max_age_ms) {
					const unsigned long long curtime = monotonic_time();
					const unsigned long long max_age_us =
						static_cast<unsigned long long>(
							pgsql_thread___connection_max_age_ms) * 1000ULL;
					unsigned int index = 0;
					while (index < free_connections->conns->len) {
						PgSQL_Connection *connection =
							static_cast<PgSQL_Connection*>(
								free_connections->conns->index(index));
						if (curtime > connection->creation_time + max_age_us) {
							connections_to_delete.push_back(
								free_connections->remove_unlocked(
									index,
									ReaderReservationEndReason::MAX_AGE));
						} else {
							index++;
						}
					}
				}
				free_connections->prune_empty_match_buckets_unlocked();
			}
			for (PgSQL_Connection* connection : connections_to_delete) {
				delete connection;
			}
#else
			if (mysrvc->status!=MYSQL_SERVER_STATUS_ONLINE) {
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Server %s:%d is not online\n", mysrvc->address, mysrvc->port);
				mysrvc->ConnectionsFree->drop_all_connections();
			}

			pgsql_pool_trim_idle_connections_to_max(mysrvc);
			PgSQL_SrvConnList *free_connections = mysrvc->ConnectionsFree;
			int free_connections_pct = pgsql_thread___free_connections_pct;
			if (mysrvc->myhgc->attributes.configured == true) {
				free_connections_pct =
					mysrvc->myhgc->attributes.free_connections_pct;
			}
			while (free_connections->conns_length() >
					free_connections_pct * mysrvc->max_connections / 100) {
				delete free_connections->remove(0);
			}

			if (pgsql_thread___connection_max_age_ms) {
				const unsigned long long curtime = monotonic_time();
				const unsigned long long max_age_us =
					static_cast<unsigned long long>(
						pgsql_thread___connection_max_age_ms) * 1000ULL;
				unsigned int index = 0;
				while (index < free_connections->conns_length()) {
					PgSQL_Connection* connection = free_connections->index(index);
					if (curtime > connection->creation_time + max_age_us) {
						delete free_connections->remove(index);
					} else {
						index++;
					}
				}
			}
#endif // POLARDB_PROXY
		}
	}
}

/*
 * Prepares at most num_conn idle connections in the given hostgroup for
 * pinging. When -1 is passed as a hostgroup, all hostgroups are examined.
 *
 * The resulting idle connections are returned in conn_list. Note that not all
 * currently idle connections will be returned (some might be purged).
 *
 * Connections are purged according to 2 criteria:
 * - whenever the maximal number of connections for a server is hit, free
 *   connections will be purged
 * - also, idle connections that cause the number of free connections to rise
 *   above a certain percentage of the maximal number of connections will be
 *   dropped as well
 */
int PgSQL_HostGroups_Manager::get_multiple_idle_connections(int _hid, unsigned long long _max_last_time_used, PgSQL_Connection **conn_list, int num_conn) {
#if POLARDB_PROXY && POLARDB_PROFILE
	const unsigned long long maintenance_started_at = monotonic_time();
#endif // POLARDB_PROXY && POLARDB_PROFILE
#if POLARDB_PROXY
	std::vector<PgSQL_SrvC*> retired_servers_to_delete;
#endif // POLARDB_PROXY
	wrlock();
#if POLARDB_PROXY
	// Every worker reaches this maintenance path periodically. Refreshing its
	// thread-local snapshot here prevents an idle worker from retaining an old
	// topology generation indefinitely. The same snapshot also keeps a server
	// valid while an extracted connection is being pinged outside HGM.
	const std::shared_ptr<const PolarDB_ServerListSnapshot> server_snapshot =
		get_polardb_server_list_snapshot();
	polardb_fast_topology_wrlock();
	polardb_detach_reclaimable_retired_servers_under_fast_topology_lock(
		retired_servers_to_delete);
	polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
	drop_all_idle_connections();
	int num_conn_current=0;
	// Keep pointers rather than list indexes. Query threads can change a FREE
	// list after its scan; each pointer is therefore looked up again under only
	// its own server lock before it is moved. A missing or recently used
	// connection is skipped.
	std::multimap<uint64_t,
		std::pair<PgSQL_SrvC*, PgSQL_Connection*>> oldest_idle_connections;
	if (conn_list && num_conn > 0) {
		for (unsigned int i = 0; i < MyHostGroups->len; i++) {
			PgSQL_HGC* myhgc =
				static_cast<PgSQL_HGC*>(MyHostGroups->index(i));
			if (_hid >= 0 && _hid != static_cast<int>(myhgc->hid)) {
				continue;
			}
			for (unsigned int j = 0; j < myhgc->mysrvs->cnt(); j++) {
				PgSQL_SrvC* mysrvc = myhgc->mysrvs->idx(j);
#if POLARDB_PROXY
				std::lock_guard<std::recursive_mutex> pool_lock(
					mysrvc->pool_mutex);
#endif // POLARDB_PROXY
				PgSQL_SrvConnList* free_connections =
					mysrvc->ConnectionsFree;
				for (unsigned int k = 0;
						k < free_connections->conns_length(); k++) {
					PgSQL_Connection* conn = free_connections->index(k);
					if (!conn || !conn->last_time_used ||
							conn->last_time_used >= _max_last_time_used) {
						continue;
					}
					if (static_cast<int>(oldest_idle_connections.size()) <
							num_conn) {
						oldest_idle_connections.insert({conn->last_time_used,
							{mysrvc, conn}});
						continue;
					}
					auto newest = std::prev(oldest_idle_connections.end());
					if (conn->last_time_used < newest->first) {
						oldest_idle_connections.erase(newest);
						oldest_idle_connections.insert({conn->last_time_used,
							{mysrvc, conn}});
					}
				}
			}
		}

		for (const auto& candidate : oldest_idle_connections) {
			PgSQL_SrvC* mysrvc = candidate.second.first;
			PgSQL_Connection* conn = mysrvc->take_free_connection_for_ping(
				candidate.second.second, _max_last_time_used);
			if (!conn) {
				continue;
			}
#if POLARDB_PROXY
			conn->polardb_selected_server_snapshot = server_snapshot;
#endif // POLARDB_PROXY
			conn_list[num_conn_current++] = conn;
			if (num_conn_current >= num_conn) {
				break;
			}
		}
	}

	pgsql_pool_status_count(&status.pgconnpoll_get_ping, num_conn_current);
	wrunlock();
#if POLARDB_PROXY
	for (PgSQL_SrvC* retired_server : retired_servers_to_delete) {
		delete retired_server;
	}
#endif // POLARDB_PROXY
#if POLARDB_PROXY && POLARDB_PROFILE
	POLARDB_PROFILE_STATUS_COUNT(idle_ping_pool_maintenance_sum_us,
		monotonic_time() - maintenance_started_at);
	POLARDB_PROFILE_STATUS_COUNT_ONE(idle_ping_pool_maintenance_count);
#endif // POLARDB_PROXY && POLARDB_PROFILE
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 7, "Returning %d idle connections\n", num_conn_current);
	return num_conn_current;
}

void PgSQL_HostGroups_Manager::save_incoming_pgsql_table(SQLite3_result *s, const string& name) {
	SQLite3_result ** inc = NULL;
	if (name == "pgsql_replication_hostgroups") {
		inc = &incoming_replication_hostgroups;
	} else if (name == "pgsql_hostgroup_attributes") {
		inc = &incoming_hostgroup_attributes;
	} else if (name == "pgsql_servers_ssl_params") {
		inc = &incoming_pgsql_servers_ssl_params;
	} else {
		assert(0);
	}
	if (*inc != nullptr) {
		delete *inc;
		*inc = nullptr;
	}
	*inc = s;
}

void PgSQL_HostGroups_Manager::save_runtime_pgsql_servers(SQLite3_result *s) {
	if (runtime_pgsql_servers) {
		delete runtime_pgsql_servers;
		runtime_pgsql_servers = nullptr;
	}
	runtime_pgsql_servers=s;
}

void PgSQL_HostGroups_Manager::save_pgsql_servers_v2(SQLite3_result* s) {
	if (incoming_pgsql_servers_v2) {
		delete incoming_pgsql_servers_v2;
		incoming_pgsql_servers_v2 = nullptr;
	}
	incoming_pgsql_servers_v2 = s;
}

SQLite3_result* PgSQL_HostGroups_Manager::get_current_pgsql_table(const string& name) {
	if (name == "pgsql_replication_hostgroups") {
		return this->incoming_replication_hostgroups;
	} else if (name == "pgsql_hostgroup_attributes") {
		return this->incoming_hostgroup_attributes;
	} else if (name == "cluster_pgsql_servers") {
		return this->runtime_pgsql_servers;
	} else if (name == "pgsql_servers_v2") {
		return this->incoming_pgsql_servers_v2;
	} else if (name == "pgsql_servers_ssl_params") {
		return this->incoming_pgsql_servers_ssl_params;
	} else {
		assert(0);
	}
	return NULL;
}



SQLite3_result * PgSQL_HostGroups_Manager::SQL3_Free_Connections() {
	const int colnum=12;
	proxy_debug(PROXY_DEBUG_MYSQL_CONNECTION, 4, "Dumping Free Connections in Pool\n");
	SQLite3_result *result=new SQLite3_result(colnum);
	result->add_column_definition(SQLITE_TEXT,"fd");
	result->add_column_definition(SQLITE_TEXT,"hostgroup");
	result->add_column_definition(SQLITE_TEXT,"srv_host");
	result->add_column_definition(SQLITE_TEXT,"srv_port");
	result->add_column_definition(SQLITE_TEXT,"user");
	result->add_column_definition(SQLITE_TEXT,"dbname");
	result->add_column_definition(SQLITE_TEXT,"init_connect");
	result->add_column_definition(SQLITE_TEXT,"time_zone");
	result->add_column_definition(SQLITE_TEXT,"sql_mode");
	//result->add_column_definition(SQLITE_TEXT,"autocommit");
	result->add_column_definition(SQLITE_TEXT,"idle_ms");
	result->add_column_definition(SQLITE_TEXT,"statistics");
	result->add_column_definition(SQLITE_TEXT,"pgsql_info");
	unsigned long long curtime = monotonic_time();
	wrlock();
	int i,j, k, l;
	for (i=0; i<(int)MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		for (j=0; j<(int)myhgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->index(j);
			if (mysrvc->status!=MYSQL_SERVER_STATUS_ONLINE) {
				proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Server %s:%d is not online\n", mysrvc->address, mysrvc->port);
				mysrvc->ConnectionsFree->drop_all_connections();
			}
			pgsql_pool_trim_idle_connections_to_max(mysrvc);
#if POLARDB_PROXY
			std::lock_guard<std::recursive_mutex> pool_lock(
				mysrvc->pool_mutex);
#endif // POLARDB_PROXY
			char buf[1024];
			for (l=0; l < (int) mysrvc->ConnectionsFree->conns_length(); l++) {
				char **pta=(char **)malloc(sizeof(char *)*colnum);
				PgSQL_Connection *conn = mysrvc->ConnectionsFree->index(l);
				sprintf(buf,"%d", conn->fd);
				pta[0]=strdup(buf);
				sprintf(buf,"%d", (int)myhgc->hid);
				pta[1]=strdup(buf);
				pta[2]=strdup(mysrvc->address);
				sprintf(buf,"%d", mysrvc->port);
				pta[3]=strdup(buf);
				pta[4] = strdup(conn->userinfo->username);
				pta[5] = strdup(conn->userinfo->dbname);
				pta[6] = NULL;
				if (conn->options.init_connect) {
					pta[6] = strdup(conn->options.init_connect);
				}
				pta[7] = NULL;
				/*if (conn->variables[SQL_TIME_ZONE].value) {
					pta[7] = strdup(conn->variables[SQL_TIME_ZONE].value);
				}*/
				pta[8] = NULL;
				/*if (conn->variables[SQL_SQL_MODE].value) {
					pta[8] = strdup(conn->variables[SQL_SQL_MODE].value);
				}*/
				//sprintf(buf,"%d", conn->options.autocommit);
				//pta[9]=strdup(buf);
				sprintf(buf,"%llu", (curtime-conn->last_time_used)/1000);
				pta[9]=strdup(buf);
				{
					json j;
					char buff[32];
					sprintf(buff,"%p",conn);
					j["address"] = buff;
					uint64_t age_ms = (curtime - conn->creation_time)/1000;
					j["age_ms"] = age_ms;
					j["bytes_recv"] = conn->bytes_info.bytes_recv;
					j["bytes_sent"] = conn->bytes_info.bytes_sent;
					j["pgconnpoll_get"] = conn->statuses.pgconnpoll_get;
					j["pgconnpoll_put"] = conn->statuses.pgconnpoll_put;
					j["questions"] = conn->statuses.questions;
					const string s = j.dump();
					pta[10] = strdup(s.c_str());
				}
				{
					json j;
					char buff[32];
					sprintf(buff, "%p", conn->get_pg_connection());
					j["address"] = buff;
					j["host"] = conn->get_pg_host();
					j["host_addr"] = conn->get_pg_hostaddr();
					j["port"] = conn->get_pg_port();
					j["user"] = conn->get_pg_user();
					j["database"] = conn->get_pg_dbname();
					j["backend_pid"] = conn->get_pg_backend_pid();
					j["using_ssl"] = conn->get_pg_ssl_in_use() ? "YES" : "NO";
					j["error_msg"] = conn->get_pg_error_message();
					j["options"] = conn->get_pg_options();
					j["fd"] = conn->get_pg_socket_fd();
					j["protocol_version"] = conn->get_pg_protocol_version();
					j["server_version"] = conn->get_pg_server_version_str(buff, sizeof(buff));
					j["transaction_status"] = conn->get_pg_transaction_status_str();
					j["connection_status"] = conn->get_pg_connection_status_str();
					j["client_encoding"] = conn->get_pg_client_encoding();
					j["is_nonblocking"] = conn->get_pg_is_nonblocking() ? "YES" : "NO";
					const string s = j.dump();
					pta[11] = strdup(s.c_str());
				}
				result->add_row(pta);
				for (k=0; k<colnum; k++) {
					if (pta[k])
						free(pta[k]);
				}
				free(pta);
			}
		}
	}
	wrunlock();
	return result;
}

void PgSQL_HostGroups_Manager::p_update_connection_pool_update_counter(
	const std::string& endpoint_id, const std::map<std::string, std::string>& labels, std::map<std::string,
	prometheus::Counter*>& m_map, unsigned long long value, PgSQL_p_hg_dyn_counter::metric idx
) {
	const auto& counter_id = m_map.find(endpoint_id);
	if (counter_id != m_map.end()) {
		const auto& cur_val = counter_id->second->Value();
		counter_id->second->Increment(value - cur_val);
	} else {
		auto& new_counter = status.p_dyn_counter_array[idx];
		m_map.insert(
			{
				endpoint_id,
				std::addressof(new_counter->Add(labels))
			}
		);
	}
}

void PgSQL_HostGroups_Manager::p_update_connection_pool_update_gauge(
	const std::string& endpoint_id, const std::map<std::string, std::string>& labels,
	std::map<std::string, prometheus::Gauge*>& m_map, unsigned long long value, PgSQL_p_hg_dyn_gauge::metric idx
) {
	const auto& counter_id = m_map.find(endpoint_id);
	if (counter_id != m_map.end()) {
		counter_id->second->Set(value);
	} else {
		auto& new_counter = status.p_dyn_gauge_array[idx];
		m_map.insert(
			{
				endpoint_id,
				std::addressof(new_counter->Add(labels))
			}
		);
	}
}

void PgSQL_HostGroups_Manager::p_update_connection_pool() {
	std::vector<string> cur_servers_ids {};
	wrlock();
	for (int i = 0; i < static_cast<int>(MyHostGroups->len); i++) {
		PgSQL_HGC *myhgc = static_cast<PgSQL_HGC*>(MyHostGroups->index(i));
		for (int j = 0; j < static_cast<int>(myhgc->mysrvs->cnt()); j++) {
			PgSQL_SrvC *mysrvc = static_cast<PgSQL_SrvC*>(myhgc->mysrvs->servers->index(j));
			std::string endpoint_addr = mysrvc->address;
			std::string endpoint_port = std::to_string(mysrvc->port);
			std::string hostgroup_id = std::to_string(myhgc->hid);
			std::string endpoint_id = hostgroup_id + ":" + endpoint_addr + ":" + endpoint_port;
			const std::map<std::string, std::string> common_labels {
				{"endpoint", endpoint_addr + ":" + endpoint_port},
				{"hostgroup", hostgroup_id },
				{"protocol", "pgsql" }
			};
			cur_servers_ids.push_back(endpoint_id);

			// proxysql_connection_pool_bytes_data_recv metric
			std::map<std::string, std::string> recv_pool_bytes_labels = common_labels;
			recv_pool_bytes_labels.insert({"traffic_flow", "recv"});
			p_update_connection_pool_update_counter(endpoint_id, recv_pool_bytes_labels,
				status.p_conn_pool_bytes_data_recv_map, mysrvc->bytes_recv, PgSQL_p_hg_dyn_counter::conn_pool_bytes_data_recv);

			// proxysql_connection_pool_bytes_data_sent metric
			std::map<std::string, std::string> sent_pool_bytes_labels = common_labels;
			sent_pool_bytes_labels.insert({"traffic_flow", "sent"});
			p_update_connection_pool_update_counter(endpoint_id, sent_pool_bytes_labels,
				status.p_conn_pool_bytes_data_sent_map, mysrvc->bytes_sent, PgSQL_p_hg_dyn_counter::conn_pool_bytes_data_sent);

			// proxysql_connection_pool_conn_err metric
			std::map<std::string, std::string> pool_conn_err_labels = common_labels;
			pool_conn_err_labels.insert({"status", "err"});
			p_update_connection_pool_update_counter(endpoint_id, pool_conn_err_labels,
				status.p_connection_pool_conn_err_map, mysrvc->connect_ERR, PgSQL_p_hg_dyn_counter::connection_pool_conn_err);

			// proxysql_connection_pool_conn_ok metric
			std::map<std::string, std::string> pool_conn_ok_labels = common_labels;
			pool_conn_ok_labels.insert({"status", "ok"});
			p_update_connection_pool_update_counter(endpoint_id, pool_conn_ok_labels,
				status.p_connection_pool_conn_ok_map, mysrvc->connect_OK, PgSQL_p_hg_dyn_counter::connection_pool_conn_ok);

			// proxysql_connection_pool_conn_free metric
			std::map<std::string, std::string> pool_conn_free_labels = common_labels;
			pool_conn_free_labels.insert({"status", "free"});
			p_update_connection_pool_update_gauge(endpoint_id, pool_conn_free_labels,
				status.p_connection_pool_conn_free_map, pgsql_pool_report_conn_free(mysrvc), PgSQL_p_hg_dyn_gauge::connection_pool_conn_free);

			// proxysql_connection_pool_conn_used metric
			std::map<std::string, std::string> pool_conn_used_labels = common_labels;
			pool_conn_used_labels.insert({"status", "used"});
			p_update_connection_pool_update_gauge(endpoint_id, pool_conn_used_labels,
				status.p_connection_pool_conn_used_map, pgsql_pool_report_conn_used(mysrvc), PgSQL_p_hg_dyn_gauge::connection_pool_conn_used);

			// proxysql_connection_pool_latency_us metric
			p_update_connection_pool_update_gauge(endpoint_id, common_labels,
				status.p_connection_pool_latency_us_map, mysrvc->current_latency_us_value(), PgSQL_p_hg_dyn_gauge::connection_pool_latency_us);

			// proxysql_connection_pool_queries metric
			p_update_connection_pool_update_counter(endpoint_id, common_labels,
				status.p_connection_pool_queries_map, mysrvc->queries_sent, PgSQL_p_hg_dyn_counter::connection_pool_queries);

			// proxysql_connection_pool_status metric
			p_update_connection_pool_update_gauge(endpoint_id, common_labels,
				status.p_connection_pool_status_map, mysrvc->status + 1, PgSQL_p_hg_dyn_gauge::connection_pool_status);
		}
	}

	// Remove the non-present servers for the gauge metrics
	vector<string> missing_server_keys {};

	for (const auto& key : status.p_connection_pool_status_map) {
		if (std::find(cur_servers_ids.begin(), cur_servers_ids.end(), key.first) == cur_servers_ids.end()) {
			missing_server_keys.push_back(key.first);
		}
	}

	for (const auto& key : missing_server_keys) {
		auto gauge = status.p_connection_pool_status_map[key];
		status.p_dyn_gauge_array[PgSQL_p_hg_dyn_gauge::connection_pool_status]->Remove(gauge);
		status.p_connection_pool_status_map.erase(key);

		gauge = status.p_connection_pool_conn_used_map[key];
		status.p_dyn_gauge_array[PgSQL_p_hg_dyn_gauge::connection_pool_conn_free]->Remove(gauge);
		status.p_connection_pool_conn_used_map.erase(key);

		gauge = status.p_connection_pool_conn_free_map[key];
		status.p_dyn_gauge_array[PgSQL_p_hg_dyn_gauge::connection_pool_conn_used]->Remove(gauge);
		status.p_connection_pool_conn_free_map.erase(key);

		gauge = status.p_connection_pool_latency_us_map[key];
		status.p_dyn_gauge_array[PgSQL_p_hg_dyn_gauge::connection_pool_latency_us]->Remove(gauge);
		status.p_connection_pool_latency_us_map.erase(key);
	}

	wrunlock();
}

SQLite3_result * PgSQL_HostGroups_Manager::SQL3_Connection_Pool(bool _reset, int *hid) {
  const int colnum=13;
  proxy_debug(PROXY_DEBUG_MYSQL_CONNECTION, 4, "Dumping Connection Pool\n");
  SQLite3_result *result=new SQLite3_result(colnum);
  result->add_column_definition(SQLITE_TEXT,"hostgroup");
  result->add_column_definition(SQLITE_TEXT,"srv_host");
  result->add_column_definition(SQLITE_TEXT,"srv_port");
  result->add_column_definition(SQLITE_TEXT,"status");
  result->add_column_definition(SQLITE_TEXT,"ConnUsed");
  result->add_column_definition(SQLITE_TEXT,"ConnFree");
  result->add_column_definition(SQLITE_TEXT,"ConnOK");
  result->add_column_definition(SQLITE_TEXT,"ConnERR");
  result->add_column_definition(SQLITE_TEXT,"MaxConnUsed");
  result->add_column_definition(SQLITE_TEXT,"Queries");
  result->add_column_definition(SQLITE_TEXT,"Bytes_sent");
  result->add_column_definition(SQLITE_TEXT,"Bytes_recv");
  result->add_column_definition(SQLITE_TEXT,"Latency_us");
	wrlock();
	int i,j, k;
	for (i=0; i<(int)MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		for (j=0; j<(int)myhgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC *mysrvc=(PgSQL_SrvC *)myhgc->mysrvs->servers->index(j);
			if (hid == NULL) {
				if (mysrvc->status!=MYSQL_SERVER_STATUS_ONLINE) {
					proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Server %s:%d is not online\n", mysrvc->address, mysrvc->port);
					//__sync_fetch_and_sub(&status.server_connections_connected, mysrvc->ConnectionsFree->conns->len);
					mysrvc->ConnectionsFree->drop_all_connections();
				}
				pgsql_pool_trim_idle_connections_to_max(mysrvc);
			} else {
				if (*hid != (int)myhgc->hid) {
					continue;
				}
			}
			char buf[1024];
			char **pta=(char **)malloc(sizeof(char *)*colnum);
			sprintf(buf,"%d", (int)myhgc->hid);
			pta[0]=strdup(buf);
			pta[1]=strdup(mysrvc->address);
			sprintf(buf,"%d", mysrvc->port);
			pta[2]=strdup(buf);
			switch (mysrvc->status) {
				case 0:
					pta[3]=strdup("ONLINE");
					break;
				case 1:
					pta[3]=strdup("SHUNNED");
					break;
				case 2:
					pta[3]=strdup("OFFLINE_SOFT");
					break;
				case 3:
					pta[3]=strdup("OFFLINE_HARD");
					break;
				case 4:
					pta[3]=strdup("SHUNNED_REPLICATION_LAG");
					break;
				default:
					// LCOV_EXCL_START
					assert(0);
					break;
					// LCOV_EXCL_STOP
			}
			sprintf(buf,"%u", pgsql_pool_report_conn_used(mysrvc));
			pta[4]=strdup(buf);
			sprintf(buf,"%u", pgsql_pool_report_conn_free(mysrvc));
			pta[5]=strdup(buf);
			sprintf(buf,"%u", mysrvc->connect_OK);
			pta[6]=strdup(buf);
			if (_reset) {
				mysrvc->connect_OK=0;
			}
			sprintf(buf,"%u", mysrvc->connect_ERR);
			pta[7]=strdup(buf);
			if (_reset) {
				mysrvc->connect_ERR=0;
			}
			sprintf(buf,"%u", mysrvc->max_connections_used);
			pta[8]=strdup(buf);
			if (_reset) {
				mysrvc->max_connections_used=0;
			}
			sprintf(buf,"%llu", mysrvc->queries_sent);
			pta[9]=strdup(buf);
			if (_reset) {
				mysrvc->queries_sent=0;
			}
			sprintf(buf,"%llu", mysrvc->bytes_sent);
			pta[10]=strdup(buf);
			if (_reset) {
				mysrvc->bytes_sent=0;
			}
			sprintf(buf,"%llu", mysrvc->bytes_recv);
			pta[11]=strdup(buf);
			if (_reset) {
				mysrvc->bytes_recv=0;
			}
			sprintf(buf,"%u", mysrvc->current_latency_us_value());
			pta[12]=strdup(buf);
			result->add_row(pta);
			for (k=0; k<colnum; k++) {
				if (pta[k])
					free(pta[k]);
			}
			free(pta);
		}
	}
	wrunlock();
	return result;
}

/**
 * @brief New implementation of the read_only_action method that does not depend on the admin table.
 *   The method checks each server in the provided list and adjusts the servers according to their corresponding read_only value.
 *   If any change has occured, checksum is calculated.
 *
 * @param pgsql_servers List of servers having hostname, port and read only value.
 * 
 */
void PgSQL_HostGroups_Manager::read_only_action_v2(
	const std::list<read_only_server_t>& pgsql_servers, bool writer_is_also_reader
) {

	bool update_pgsql_servers_table = false;

	unsigned long long curtime1 = monotonic_time();
	wrlock();
	for (const auto& server : pgsql_servers) {
		bool is_writer = false;
		const std::string& hostname = std::get<PgSQL_READ_ONLY_SERVER_T::PG_ROS_HOSTNAME>(server);
		const int port = std::get<PgSQL_READ_ONLY_SERVER_T::PG_ROS_PORT>(server);
		const int read_only = std::get<PgSQL_READ_ONLY_SERVER_T::PG_ROS_READONLY>(server);
		const std::string& srv_id = hostname + ":::" + std::to_string(port);
		
		auto itr = hostgroup_server_mapping.find(srv_id);

		if (itr == hostgroup_server_mapping.end()) {
			proxy_warning("Server %s:%d not found\n", hostname.c_str(), port);
			continue;
		}

		HostGroup_Server_Mapping* host_server_mapping = itr->second.get();

		if (!host_server_mapping)
			assert(0);

		const std::vector<HostGroup_Server_Mapping::Node>& writer_map = host_server_mapping->get(HostGroup_Server_Mapping::Type::WRITER);

		is_writer = !writer_map.empty();

		if (read_only == 0) {
			if (is_writer == false) {
				// the server has read_only=0 (writer), but we can't find any writer, 
				// so we copy all reader nodes to writer
				proxy_info("Server '%s:%d' found with 'read_only=0', but not found as writer\n", hostname.c_str(), port);
				proxy_debug(PROXY_DEBUG_MONITOR, 5, "Server '%s:%d' found with 'read_only=0', but not found as writer\n", hostname.c_str(), port);
				host_server_mapping->copy_if_not_exists(HostGroup_Server_Mapping::Type::WRITER, HostGroup_Server_Mapping::Type::READER);

				if (writer_is_also_reader == false) {
					// remove node from reader
					host_server_mapping->clear(HostGroup_Server_Mapping::Type::READER);
				}

				update_pgsql_servers_table = true;
				proxy_info("Regenerating table 'pgsql_servers' due to actions on server '%s:%d'\n", hostname.c_str(), port);
			} else {
				bool act = false;

				// if the server was RO=0 on the previous check then no action is needed
				if (host_server_mapping->get_readonly_flag() != 0) {
					// it is the first time that we detect RO on this server
					const std::vector<HostGroup_Server_Mapping::Node>& reader_map = host_server_mapping->get(HostGroup_Server_Mapping::Type::READER);

					for (const auto& reader_node : reader_map) {
						for (const auto& writer_node : writer_map) {

							if (reader_node.writer_hostgroup_id == writer_node.writer_hostgroup_id) {
								goto __writer_found;
							}
						}
						act = true;
						break;
					__writer_found:
						continue;
					}

					if (act == false) {
						// no action required, therefore we set readonly_flag to 0
						proxy_info("read_only_action_v2() detected RO=0 on server %s:%d for the first time after commit(), but no need to reconfigure\n", hostname.c_str(), port);
						host_server_mapping->set_readonly_flag(0);
					}
				} else {
					// the server was already detected as RO=0
					// no action required
				}

				if (act == true) {	// there are servers either missing, or with stats=OFFLINE_HARD

					proxy_info("Server '%s:%d' with 'read_only=0' found missing at some 'writer_hostgroup'\n", hostname.c_str(), port);
					proxy_debug(PROXY_DEBUG_MONITOR, 5, "Server '%s:%d' with 'read_only=0' found missing at some 'writer_hostgroup'\n", hostname.c_str(), port);

					// copy all reader nodes to writer
					host_server_mapping->copy_if_not_exists(HostGroup_Server_Mapping::Type::WRITER, HostGroup_Server_Mapping::Type::READER);

					if (writer_is_also_reader == false) {
						// remove node from reader
						host_server_mapping->clear(HostGroup_Server_Mapping::Type::READER);
					}

					update_pgsql_servers_table = true;
					proxy_info("Regenerating table 'pgsql_servers' due to actions on server '%s:%d'\n", hostname.c_str(), port);
				}
			}
		} else if (read_only == 1) {
			if (is_writer) {
				// the server has read_only=1 (reader), but we find it as writer, so we copy all writer nodes to reader (previous reader nodes will be reused)
				proxy_info("Server '%s:%d' found with 'read_only=1', but not found as reader\n", hostname.c_str(), port);
				proxy_debug(PROXY_DEBUG_MONITOR, 5, "Server '%s:%d' found with 'read_only=1', but not found as reader\n", hostname.c_str(), port);
				host_server_mapping->copy_if_not_exists(HostGroup_Server_Mapping::Type::READER, HostGroup_Server_Mapping::Type::WRITER);

				// clearing all writer nodes
				host_server_mapping->clear(HostGroup_Server_Mapping::Type::WRITER);

				update_pgsql_servers_table = true;
				proxy_info("Regenerating table 'pgsql_servers' due to actions on server '%s:%d'\n", hostname.c_str(), port);
			}
		} else {
			// LCOV_EXCL_START
			assert(0);
			break;
			// LCOV_EXCL_STOP
		}
	}

	if (update_pgsql_servers_table) {
		purge_pgsql_servers_table();
		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 4, "DELETE FROM pgsql_servers\n");
		mydb->execute("DELETE FROM pgsql_servers");
		generate_pgsql_servers_table();

		// Update the global checksums after 'pgsql_servers' regeneration
		{
			unique_ptr<SQLite3_result> resultset { get_admin_runtime_pgsql_servers(mydb) };
			uint64_t raw_checksum = resultset ? resultset->raw_checksum() : 0;

			// This is required to be updated to avoid extra rebuilding member 'hostgroup_server_mapping'
			// during 'commit'. For extra details see 'hgsm_pgsql_servers_checksum' @details.
			hgsm_pgsql_servers_checksum = raw_checksum;

			string mysrvs_checksum { get_checksum_from_hash(raw_checksum) };
			save_runtime_pgsql_servers(resultset.release());
			proxy_info("Checksum for table %s is %s\n", "pgsql_servers", mysrvs_checksum.c_str());

			pthread_mutex_lock(&GloVars.checksum_mutex);
			update_glovars_pgsql_servers_checksum(mysrvs_checksum);
			pthread_mutex_unlock(&GloVars.checksum_mutex);
		}
#if POLARDB_PROXY
		polardb_refresh_all_writer_epochs_under_hgm_write_lock("read_only_action_v2");
#endif // POLARDB_PROXY
	}
	wrunlock();
	unsigned long long curtime2 = monotonic_time();
	curtime1 = curtime1 / 1000;
	curtime2 = curtime2 / 1000;
	proxy_debug(PROXY_DEBUG_MONITOR, 7, "PgSQL_HostGroups_Manager::read_only_action_v2() locked for %llums (server count:%ld)\n", curtime2 - curtime1, pgsql_servers.size());
}

// shun_and_killall
// this function is called only from MySQL_Monitor::monitor_ping()
// it temporary disables a host that is not responding to pings, and mark the host in a way that when used the connection will be dropped
// return true if the status was changed
bool PgSQL_HostGroups_Manager::shun_and_killall(char *hostname, int port) {
	time_t t = time(NULL);
	bool ret = false;
	wrlock();
	PgSQL_SrvC *mysrvc=NULL;
	for (unsigned int i=0; i<MyHostGroups->len; i++) {
	PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		unsigned int j;
		unsigned int l=myhgc->mysrvs->cnt();
		if (l) {
			for (j=0; j<l; j++) {
				mysrvc=myhgc->mysrvs->idx(j);
				if (mysrvc->port==port && strcmp(mysrvc->address,hostname)==0) {
					switch (mysrvc->status) {
						case MYSQL_SERVER_STATUS_SHUNNED:
							if (mysrvc->shunned_automatic==false) {
								break;
							}
						case MYSQL_SERVER_STATUS_ONLINE:
							if (mysrvc->status == MYSQL_SERVER_STATUS_ONLINE) {
								ret = true;
							}
							mysrvc->set_status(MYSQL_SERVER_STATUS_SHUNNED);
						case MYSQL_SERVER_STATUS_OFFLINE_SOFT:
							mysrvc->shunned_automatic=true;
							mysrvc->shunned_and_kill_all_connections=true;
							mysrvc->ConnectionsFree->drop_all_connections();
							break;
						default:
							break;
					}
					// if Monitor is enabled and pgsql-monitor_ping_interval is
					// set too high, ProxySQL will unshun hosts that are not
					// available. For this reason time_last_detected_error will
					// be tuned in the future
					if (mysql_thread___monitor_enabled) {
						int a = pgsql_thread___shun_recovery_time_sec;
						int b = mysql_thread___monitor_ping_interval;
						b = b/1000;
						if (b > a) {
							t = t + (b - a);
						}
					}
					mysrvc->time_last_detected_error = t;
				}
			}
		}
	}
	wrunlock();
	return ret;
}

// set_server_current_latency_us
// this function is called only from MySQL_Monitor::monitor_ping()
// it set the average latency for a host in the last 3 pings
// the connection pool will use this information to evaluate or exclude a specific hosts
// note that this variable is in microsecond, while user defines it in millisecond
void PgSQL_HostGroups_Manager::set_server_current_latency_us(char *hostname, int port, unsigned int _current_latency_us) {
	wrlock();
	PgSQL_SrvC *mysrvc=NULL;
  for (unsigned int i=0; i<MyHostGroups->len; i++) {
    PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		unsigned int j;
		unsigned int l=myhgc->mysrvs->cnt();
		if (l) {
			for (j=0; j<l; j++) {
				mysrvc=myhgc->mysrvs->idx(j);
				if (mysrvc->port==port && strcmp(mysrvc->address,hostname)==0) {
					mysrvc->set_current_latency_us_value(_current_latency_us);
				}
			}
		}
	}
	wrunlock();
}

void PgSQL_HostGroups_Manager::p_update_metrics() {
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::servers_table_version], status.servers_table_version);
	// Update *server_connections* related metrics
	status.p_gauge_array[PgSQL_p_hg_gauge::server_connections_connected]->Set(status.server_connections_connected);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::server_connections_aborted], status.server_connections_aborted);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::server_connections_created], status.server_connections_created);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::server_connections_delayed], status.server_connections_delayed);

	// Update *client_connections* related metrics
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::client_connections_created], status.client_connections_created);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::client_connections_aborted], status.client_connections_aborted);
	status.p_gauge_array[PgSQL_p_hg_gauge::client_connections_connected]->Set(status.client_connections);

	// Update *acess_denied* related metrics
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::access_denied_wrong_password], status.access_denied_wrong_password);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::access_denied_max_connections], status.access_denied_max_connections);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::access_denied_max_user_connections], status.access_denied_max_user_connections);

	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::selects_for_update__autocommit0], status.select_for_update_or_equivalent);

	// Update *com_* related metrics
	//p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_autocommit], status.autocommit_cnt);
	//p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_autocommit_filtered], status.autocommit_cnt_filtered);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_commit_cnt], status.commit_cnt);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_commit_cnt_filtered], status.commit_cnt_filtered);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_rollback], status.rollback_cnt);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_rollback_filtered], status.rollback_cnt_filtered);
	//p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_backend_init_db], status.backend_init_db);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_backend_reset_connection], status.backend_reset_connection);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_backend_set_client_encoding], status.backend_set_client_encoding);
	//p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_frontend_init_db], status.frontend_init_db);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_frontend_set_client_encoding], status.frontend_set_client_encoding);
	//p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::com_frontend_use_db], status.frontend_use_db);

	// Update *myconnpoll* related metrics
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_get], pgsql_pool_status_read_get(&status.pgconnpoll_get));
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_get_ok], pgsql_pool_status_read_get_ok(&status.pgconnpoll_get_ok));
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_get_ping], pgsql_pool_status_read(&status.pgconnpoll_get_ping));
	p_update_counter(
		status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_push],
		pgsql_pool_status_read_push(&status.pgconnpoll_push));
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_reset], status.pgconnpoll_reset);
	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::pghgm_pgconnpool_destroy], status.pgconnpoll_destroy);

	p_update_counter(status.p_counter_array[PgSQL_p_hg_counter::auto_increment_delay_multiplex], status.auto_increment_delay_multiplex);

#if POLARDB_PROXY
	// PolarDB thread counters are stored per worker thread to avoid query-path
	// global-atomic contention. PolarDB global counters stay as PgHGM->status
	// atomics. Prometheus consumes the same absolute totals as stats_pgsql_global;
	// p_update_counter() turns them into scrape deltas.
	if (GloPTH) {
#define X(name, display_name, prom_name, help) \
		p_update_counter( \
			status.p_counter_array[PgSQL_p_hg_counter::polardb_##name], \
			GloPTH->get_polardb_counter(polardb_st_var_##name, status.polardb_##name));
		POLARDB_THREAD_COUNTER_LIST(X)
#undef X
	}
#define X(name, display_name, prom_name, help) \
		p_update_counter( \
			status.p_counter_array[PgSQL_p_hg_counter::polardb_##name], \
			status.polardb_##name.load(std::memory_order_relaxed));
		POLARDB_GLOBAL_COUNTER_LIST(X)
#undef X
#define X(name, display_name, prom_name, help) \
		status.p_gauge_array[PgSQL_p_hg_gauge::polardb_##name]->Set( \
			status.polardb_##name.load(std::memory_order_relaxed));
		POLARDB_GAUGE_LIST(X)
#undef X
#endif // POLARDB_PROXY

	// Update the *connection_pool* metrics
	this->p_update_connection_pool();
}

SQLite3_result * PgSQL_HostGroups_Manager::SQL3_Get_ConnPool_Stats() {
	const int colnum=2;
	char buf[256];
	char **pta=(char **)malloc(sizeof(char *)*colnum);
	proxy_debug(PROXY_DEBUG_MYSQL_CONNECTION, 4, "Dumping PgSQL Global Status\n");
	SQLite3_result *result=new SQLite3_result(colnum);
	result->add_column_definition(SQLITE_TEXT,"Variable_Name");
	result->add_column_definition(SQLITE_TEXT,"Variable_Value");
	wrlock();
	// NOTE: as there is no string copy, we do NOT free pta[0] and pta[1]
    {
		pta[0]=(char *)"PgHGM_pgconnpoll_get";
		sprintf(buf,"%lu",pgsql_pool_status_read_get(&status.pgconnpoll_get));
		pta[1]=buf;
		result->add_row(pta);
	}
    {
		pta[0]=(char *)"PgHGM_pgconnpoll_get_ok";
		sprintf(buf,"%lu",pgsql_pool_status_read_get_ok(&status.pgconnpoll_get_ok));
		pta[1]=buf;
		result->add_row(pta);
	}
    {
		pta[0]=(char *)"PgHGM_pgconnpoll_push";
		sprintf(buf,"%lu",pgsql_pool_status_read_push(&status.pgconnpoll_push));
		pta[1]=buf;
		result->add_row(pta);
	}
    {
		pta[0]=(char *)"PgHGM_pgconnpoll_destroy";
		sprintf(buf,"%lu",status.pgconnpoll_destroy);
		pta[1]=buf;
		result->add_row(pta);
	}
    {
		pta[0]=(char *)"PgHGM_pgconnpoll_reset";
		sprintf(buf,"%lu",status.pgconnpoll_reset);
		pta[1]=buf;
		result->add_row(pta);
	}
	wrunlock();
	free(pta);
	return result;
}


unsigned long long PgSQL_HostGroups_Manager::Get_Memory_Stats() {
	unsigned long long intsize=0;
	wrlock();
	PgSQL_SrvC *mysrvc=NULL;
  for (unsigned int i=0; i<MyHostGroups->len; i++) {
		intsize+=sizeof(PgSQL_HGC);
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		unsigned int j,k;
		unsigned int l=myhgc->mysrvs->cnt();
		if (l) {
			for (j=0; j<l; j++) {
				intsize+=sizeof(PgSQL_SrvC);
				mysrvc=myhgc->mysrvs->idx(j);
#if POLARDB_PROXY
				std::lock_guard<std::recursive_mutex> pool_lock(
					mysrvc->pool_mutex);
#endif // POLARDB_PROXY
				intsize+=((mysrvc->ConnectionsUsed->conns_length())*sizeof(PgSQL_Connection *));
				for (k=0; k<mysrvc->ConnectionsFree->conns_length(); k++) {
					//PgSQL_Connection *myconn=(PgSQL_Connection *)mysrvc->ConnectionsFree->conns->index(k);
					PgSQL_Connection *myconn=mysrvc->ConnectionsFree->index(k);
					intsize+= sizeof(PgSQL_Connection);
					intsize+=myconn->get_memory_usage();
					//intsize+=(4096*15); // ASYNC_CONTEXT_DEFAULT_STACK_SIZE
					if (myconn->query_result) {
						intsize+=myconn->query_result->current_size();
					}
				}
				intsize+=((mysrvc->ConnectionsUsed->conns_length())*sizeof(PgSQL_Connection *));
			}
		}
	}
	wrunlock();
	return intsize;
}

void PgSQL_HostGroups_Manager::add_pgsql_errors(int hostgroup, const char *hostname, int port, const char *username, const char *address, 
	const char *dbname, const char* sqlstate, const char *errmsg) {
	SpookyHash myhash;
	uint64_t hash1;
	uint64_t hash2;
	size_t rand_del_len=strlen(rand_del);
	time_t tn = time(NULL);
	myhash.Init(11,4);
	myhash.Update(&hostgroup,sizeof(hostgroup));
	myhash.Update(rand_del,rand_del_len);
	if (hostname) {
		myhash.Update(hostname,strlen(hostname));
	}
	myhash.Update(rand_del,rand_del_len);
	myhash.Update(&port,sizeof(port));
	if (username) {
		myhash.Update(username,strlen(username));
	}
	myhash.Update(rand_del,rand_del_len);
	if (address) {
		myhash.Update(address,strlen(address));
	}
	myhash.Update(rand_del,rand_del_len);
	if (dbname) {
		myhash.Update(dbname,strlen(dbname));
	}
	myhash.Update(rand_del,rand_del_len);
	if (sqlstate) {
		myhash.Update(sqlstate, strlen(sqlstate));
	}
	myhash.Final(&hash1,&hash2);

	pthread_mutex_lock(&pgsql_errors_mutex);
	if (auto it = pgsql_errors_umap.find(hash1); it != pgsql_errors_umap.end()) {
		// found
		auto& err_stats = it->second;
		err_stats->add_time(tn, errmsg);
	} else {
		PgSQL_Errors_stats* err_stats = new PgSQL_Errors_stats(hostgroup, hostname, port, username, address, dbname, sqlstate, errmsg, tn);
		pgsql_errors_umap.insert(std::make_pair(hash1, err_stats));
	}
	pthread_mutex_unlock(&pgsql_errors_mutex);
}

std::unique_ptr<SQLite3_result> PgSQL_HostGroups_Manager::get_pgsql_errors(bool reset) {
	std::unique_ptr<SQLite3_result> result = std::make_unique<SQLite3_result>(PgSQL_ERRORS_STATS_FIELD_NUM);
	result->add_column_definition(SQLITE_TEXT,"hid");
	result->add_column_definition(SQLITE_TEXT,"hostname");
	result->add_column_definition(SQLITE_TEXT,"port");
	result->add_column_definition(SQLITE_TEXT,"username");
	result->add_column_definition(SQLITE_TEXT,"client_address");
	result->add_column_definition(SQLITE_TEXT,"database");
	result->add_column_definition(SQLITE_TEXT,"sqlstate");
	result->add_column_definition(SQLITE_TEXT,"count_star");
	result->add_column_definition(SQLITE_TEXT,"first_seen");
	result->add_column_definition(SQLITE_TEXT,"last_seen");
	result->add_column_definition(SQLITE_TEXT,"last_error");
	pthread_mutex_lock(&pgsql_errors_mutex);
	for (auto it=pgsql_errors_umap.begin(); it!=pgsql_errors_umap.end(); ++it) {
		auto& err_stats = it->second;
		char **pta= err_stats->get_row();
		result->add_row(pta);
		err_stats->free_row(pta);
		if (reset) {
			err_stats.reset();
		}
	}
	if (reset) {
		pgsql_errors_umap.clear();
	}
	pthread_mutex_unlock(&pgsql_errors_mutex);
	return result;
}

/**
 * @brief Initializes the supplied 'PgSQL_HGC' with the specified 'hostgroup_settings'.
 * @details Input verification is performed in the supplied 'hostgroup_settings'. It's expected to be a valid
 *  JSON that may contain the following fields:
 *   - handle_warnings: Value must be >= 0.
 *
 *  In case input verification fails for a field, supplied 'PgSQL_HGC' is NOT updated for that field. An error
 *  message is logged specifying the source of the error.
 *
 * @param hostgroup_settings String containing a JSON defined in 'pgsql_hostgroup_attributes'.
 * @param myhgc The 'PgSQL_HGC' of the target hostgroup of the supplied 'hostgroup_settings'.
 */
void init_myhgc_hostgroup_settings(const char* hostgroup_settings, PgSQL_HGC* myhgc) {
	const uint32_t hid = myhgc->hid;

	if (hostgroup_settings[0] != '\0') {
		try {
			nlohmann::json j = nlohmann::json::parse(hostgroup_settings);

			const auto handle_warnings_check = [](int8_t handle_warnings) -> bool { return handle_warnings == 0 || handle_warnings == 1; };
			int8_t handle_warnings = PgSQL_j_get_srv_default_int_val<int8_t>(j, hid, "handle_warnings", handle_warnings_check);
			myhgc->attributes.handle_warnings = handle_warnings;
		}
		catch (const json::exception& e) {
			proxy_error(
				"JSON parsing for 'pgsql_hostgroup_attributes.hostgroup_settings' for hostgroup %d failed with exception `%s`.\n",
				hid, e.what()
			);
		}
	}
}

/**
 * @brief Initializes the supplied 'PgSQL_HGC' with the specified 'servers_defaults'.
 * @details Input verification is performed in the supplied 'server_defaults'. It's expected to be a valid
 *  JSON that may contain the following fields:
 *   - weight: Must be an unsigned integer >= 0.
 *   - max_connections: Must be an unsigned integer >= 0.
 *   - use_ssl: Must be a integer with either value 0 or 1.
 *
 *  In case input verification fails for a field, supplied 'PgSQL_HGC' is NOT updated for that field. An error
 *  message is logged specifying the source of the error.
 *
 * @param servers_defaults String containing a JSON defined in 'pgsql_hostgroup_attributes'.
 * @param myhgc The 'PgSQL_HGC' of the target hostgroup of the supplied 'servers_defaults'.
 */
void init_myhgc_servers_defaults(char* servers_defaults, PgSQL_HGC* myhgc) {
	uint32_t hid = myhgc->hid;

	if (strcmp(servers_defaults, "") != 0) {
		try {
		    nlohmann::json j = nlohmann::json::parse(servers_defaults);

			const auto weight_check = [] (int64_t weight) -> bool { return weight >= 0; };
			int64_t weight = PgSQL_j_get_srv_default_int_val<int64_t>(j, hid, "weight", weight_check);

			myhgc->servers_defaults.weight = weight;

			const auto max_conns_check = [] (int64_t max_conns) -> bool { return max_conns >= 0; };
			int64_t max_conns = PgSQL_j_get_srv_default_int_val<int64_t>(j, hid, "max_connections", max_conns_check);

			myhgc->servers_defaults.max_connections = max_conns;

			const auto use_ssl_check = [] (int32_t use_ssl) -> bool { return use_ssl == 0 || use_ssl == 1; };
			int32_t use_ssl = PgSQL_j_get_srv_default_int_val<int32_t>(j, hid, "use_ssl", use_ssl_check);

			myhgc->servers_defaults.use_ssl = use_ssl;
		} catch (const json::exception& e) {
			proxy_error(
				"JSON parsing for 'pgsql_hostgroup_attributes.servers_defaults' for hostgroup %d failed with exception `%s`.\n",
				hid, e.what()
			);
		}
	}
}

void PgSQL_HostGroups_Manager::generate_pgsql_hostgroup_attributes_table() {
	if (incoming_hostgroup_attributes==NULL) {
		return;
	}
	int rc;
	const char * query=(const char *)"INSERT INTO pgsql_hostgroup_attributes ( "
		"hostgroup_id, max_num_online_servers, autocommit, free_connections_pct, "
		"init_connect, multiplex, connection_warming, throttle_connections_per_sec, "
		"ignore_session_variables, hostgroup_settings, servers_defaults, comment) VALUES "
		"(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";

	auto [rc1, statement_unique] = mydb->prepare_v2(query);
	ASSERT_SQLITE_OK(rc1, mydb);
	sqlite3_stmt *statement = statement_unique.get();
	proxy_info("New pgsql_hostgroup_attributes table\n");
	bool current_configured[MyHostGroups->len];
	// set configured = false to all
	// in this way later we can known which HG were updated
	for (unsigned int i=0; i<MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		current_configured[i] = myhgc->attributes.configured;
		myhgc->attributes.configured = false;
	}

	/**
	 * @brief We iterate the whole resultset incoming_hostgroup_attributes and configure
	 * both the hostgroup in memory, but also pupulate table pgsql_hostgroup_attributes
	 *   connection errors.
	 * @details for each row in incoming_hostgroup_attributes:
	 *   1. it finds (or create) the hostgroup
	 *   2. it writes the in pgsql_hostgroup_attributes
	 *   3. it finds (or create) the attributes of the hostgroup
	*/
	for (std::vector<SQLite3_row *>::iterator it = incoming_hostgroup_attributes->rows.begin() ; it != incoming_hostgroup_attributes->rows.end(); ++it) {
		SQLite3_row *r=*it;
		unsigned int hid = (unsigned int)atoi(r->fields[0]);
		PgSQL_HGC *myhgc = MyHGC_lookup(hid); // note: MyHGC_lookup() will create the HG if doesn't exist!
		int max_num_online_servers       = atoi(r->fields[1]);
		int autocommit                   = atoi(r->fields[2]);
		int free_connections_pct         = atoi(r->fields[3]);
		char * init_connect              = r->fields[4];
		int multiplex                    = atoi(r->fields[5]);
		int connection_warming           = atoi(r->fields[6]);
		int throttle_connections_per_sec = atoi(r->fields[7]);
		char * ignore_session_variables  = r->fields[8];
		char * hostgroup_settings		 = r->fields[9];
		char * servers_defaults          = r->fields[10];
		char * comment                   = r->fields[11];
		proxy_info("Loading MySQL Hostgroup Attributes info for (%d,%d,%d,%d,\"%s\",%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\")\n",
			hid, max_num_online_servers, autocommit, free_connections_pct,
			init_connect, multiplex, connection_warming, throttle_connections_per_sec,
			ignore_session_variables, hostgroup_settings, servers_defaults, comment
		);
		rc=(*proxy_sqlite3_bind_int64)(statement, 1, hid);                          ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 2, max_num_online_servers);       ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 3, autocommit);                   ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 4, free_connections_pct);         ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement,  5, init_connect,              -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 6, multiplex);                    ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 7, connection_warming);           ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_int64)(statement, 8, throttle_connections_per_sec); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement,  9, ignore_session_variables,  -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement, 10, hostgroup_settings,		-1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement, 11, servers_defaults,          -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_bind_text)(statement, 12, comment,                   -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb);
		SAFE_SQLITE3_STEP2(statement);
		rc=(*proxy_sqlite3_clear_bindings)(statement); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_reset)(statement); ASSERT_SQLITE_OK(rc, mydb);
		myhgc->attributes.configured                   = true;
		myhgc->attributes.max_num_online_servers       = max_num_online_servers;
		myhgc->attributes.autocommit                   = autocommit;
		myhgc->attributes.free_connections_pct         = free_connections_pct;
		myhgc->attributes.multiplex                    = multiplex;
		myhgc->attributes.connection_warming           = connection_warming;
		myhgc->attributes.throttle_connections_per_sec = throttle_connections_per_sec;
		if (myhgc->attributes.init_connect != NULL)
			free(myhgc->attributes.init_connect);
		myhgc->attributes.init_connect = strdup(init_connect);
		if (myhgc->attributes.comment != NULL)
			free(myhgc->attributes.comment);
		myhgc->attributes.comment = strdup(comment);
		// for ignore_session_variables we store 2 versions:
		// 1. the text
		// 2. the JSON
		// Because calling JSON functions is expensive, we first verify if it changes
		if (myhgc->attributes.ignore_session_variables_text == NULL) {
			myhgc->attributes.ignore_session_variables_text = strdup(ignore_session_variables);
			if (strlen(ignore_session_variables) != 0) { // only if there is a valid JSON
				if (myhgc->attributes.ignore_session_variables_json != nullptr) { delete myhgc->attributes.ignore_session_variables_json; }
				myhgc->attributes.ignore_session_variables_json = new json(json::parse(ignore_session_variables));
			}
		} else {
			if (strcmp(myhgc->attributes.ignore_session_variables_text, ignore_session_variables) != 0) {
				free(myhgc->attributes.ignore_session_variables_text);
				myhgc->attributes.ignore_session_variables_text = strdup(ignore_session_variables);
				if (strlen(ignore_session_variables) != 0) { // only if there is a valid JSON
					if (myhgc->attributes.ignore_session_variables_json != nullptr) { delete myhgc->attributes.ignore_session_variables_json; }
					myhgc->attributes.ignore_session_variables_json = new json(json::parse(ignore_session_variables));
				}
				// TODO: assign the variables
			}
		}
		init_myhgc_hostgroup_settings(hostgroup_settings, myhgc);
		init_myhgc_servers_defaults(servers_defaults, myhgc);
	}
	for (unsigned int i=0; i<MyHostGroups->len; i++) {
		PgSQL_HGC *myhgc=(PgSQL_HGC *)MyHostGroups->index(i);
		if (myhgc->attributes.configured == false) {
			if (current_configured[i] == true) {
				// if configured == false and previously it was configured == true , reset to defaults
				proxy_info("Resetting hostgroup attributes for hostgroup %u\n", myhgc->hid);
				myhgc->reset_attributes();
			}
		}
	}

	delete incoming_hostgroup_attributes;
	incoming_hostgroup_attributes=NULL;
}

void PgSQL_HostGroups_Manager::generate_pgsql_servers_ssl_params_table() {
	if (incoming_pgsql_servers_ssl_params==NULL) {
		return;
	}
	int rc;

	const char * query = (const char *)"INSERT INTO pgsql_servers_ssl_params ("
		"hostname, port, username, ssl_ca, ssl_cert, ssl_key, "
		"ssl_crl, ssl_crlpath, ssl_protocol_version_range, comment) VALUES "
		"(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)";

	auto [rc1, statement_unique] = mydb->prepare_v2(query);
	ASSERT_SQLITE_OK(rc1, mydb);
	sqlite3_stmt *statement = statement_unique.get();
	proxy_info("New pgsql_servers_ssl_params table\n");
	std::lock_guard<std::mutex> lock(PgSQL_Servers_SSL_Params_map_mutex);
	PgSQL_Servers_SSL_Params_map.clear();

	for (std::vector<SQLite3_row *>::iterator it = incoming_pgsql_servers_ssl_params->rows.begin() ; it != incoming_pgsql_servers_ssl_params->rows.end(); ++it) {
		SQLite3_row *r=*it;
		proxy_info("Loading PgSQL Server SSL Params for (%s,%s,%s)\n",
			r->fields[0], r->fields[1], r->fields[2]
		);

		rc=(*proxy_sqlite3_bind_text)(statement,  1,  r->fields[0]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // hostname
		rc=(*proxy_sqlite3_bind_int64)(statement, 2,  atoi(r->fields[1]));                   ASSERT_SQLITE_OK(rc, mydb); // port
		rc=(*proxy_sqlite3_bind_text)(statement,  3,  r->fields[2]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // username
		rc=(*proxy_sqlite3_bind_text)(statement,  4,  r->fields[3]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_ca
		rc=(*proxy_sqlite3_bind_text)(statement,  5,  r->fields[4]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_cert
		rc=(*proxy_sqlite3_bind_text)(statement,  6,  r->fields[5]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_key
		rc=(*proxy_sqlite3_bind_text)(statement,  7,  r->fields[6]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_crl
		rc=(*proxy_sqlite3_bind_text)(statement,  8,  r->fields[7]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_crlpath
		rc=(*proxy_sqlite3_bind_text)(statement,  9,  r->fields[8]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // ssl_protocol_version_range
		rc=(*proxy_sqlite3_bind_text)(statement,  10, r->fields[9]  , -1, SQLITE_TRANSIENT); ASSERT_SQLITE_OK(rc, mydb); // comment

		SAFE_SQLITE3_STEP2(statement);
		rc=(*proxy_sqlite3_clear_bindings)(statement); ASSERT_SQLITE_OK(rc, mydb);
		rc=(*proxy_sqlite3_reset)(statement); ASSERT_SQLITE_OK(rc, mydb);

		PgSQLServers_SslParams PSSP(
			r->fields[0], atoi(r->fields[1]), r->fields[2],
			r->fields[3], r->fields[4], r->fields[5],
			r->fields[6], r->fields[7],
			r->fields[8], r->fields[9]
		);
		string MapKey = PSSP.getMapKey(rand_del);
		PgSQL_Servers_SSL_Params_map.emplace(MapKey, PSSP);
	}
	delete incoming_pgsql_servers_ssl_params;
	incoming_pgsql_servers_ssl_params=NULL;
}

PgSQLServers_SslParams * PgSQL_HostGroups_Manager::get_Server_SSL_Params(char *hostname, int port, char *username) {
	string MapKey = string(hostname) + string(rand_del) + to_string(port) + string(rand_del) + string(username);
	std::lock_guard<std::mutex> lock(PgSQL_Servers_SSL_Params_map_mutex);
	auto it = PgSQL_Servers_SSL_Params_map.find(MapKey);
	if (it != PgSQL_Servers_SSL_Params_map.end()) {
		PgSQLServers_SslParams * PSSP = new PgSQLServers_SslParams(it->second);
		return PSSP;
	} else {
		MapKey = string(hostname) + string(rand_del) + to_string(port) + string(rand_del) + "";
		it = PgSQL_Servers_SSL_Params_map.find(MapKey);
		if (it != PgSQL_Servers_SSL_Params_map.end()) {
			PgSQLServers_SslParams * PSSP = new PgSQLServers_SslParams(it->second);
			return PSSP;
		}
	}
	return NULL;
}

int PgSQL_HostGroups_Manager::create_new_server_in_hg(
	uint32_t hid, const PgSQL_srv_info_t& srv_info, const PgSQL_srv_opts_t& srv_opts
) {
	int32_t res = -1;
	PgSQL_SrvC* mysrvc = find_server_in_hg(hid, srv_info.addr, srv_info.port);

	if (mysrvc == nullptr) {
		char* c_hostname { const_cast<char*>(srv_info.addr.c_str()) };
		PgSQL_SrvC* mysrvc = new PgSQL_SrvC(
			c_hostname, srv_info.port, srv_opts.weigth, MYSQL_SERVER_STATUS_ONLINE, 0, srv_opts.max_conns, 0,
			srv_opts.use_ssl, 0, const_cast<char*>("")
		);
		add(mysrvc,hid);
		proxy_info(
			"Adding new discovered %s node %s:%d with: hostgroup=%d, weight=%ld, max_connections=%ld, use_ssl=%d\n",
			srv_info.kind.c_str(), c_hostname, srv_info.port, hid, mysrvc->weight, mysrvc->max_connections,
			mysrvc->use_ssl
		);

		res = 0;
	} else {
		// If the server is found as 'OFFLINE_HARD' we reset the 'PgSQL_SrvC' values corresponding with the
		// 'servers_defaults' (as in a new 'PgSQL_SrvC' creation). We then later update these values with the
		// 'servers_defaults' attributes from its corresponding 'PgSQL_HGC'. This way we ensure uniform behavior
		// of new servers, and 'OFFLINE_HARD' ones when a user update 'servers_defaults' values, and reloads
		// the servers to runtime.
		if (mysrvc && mysrvc->status == MYSQL_SERVER_STATUS_OFFLINE_HARD) {
			reset_hg_attrs_server_defaults(mysrvc);
			update_hg_attrs_server_defaults(mysrvc, mysrvc->myhgc);
#if POLARDB_PROXY
			polardb_fast_topology_wrlock();
			polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
			polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
			mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);

			proxy_info(
				"Found healthy previously discovered %s node %s:%d as 'OFFLINE_HARD', setting back as 'ONLINE' with:"
					" hostgroup=%d, weight=%ld, max_connections=%ld, use_ssl=%d\n",
				srv_info.kind.c_str(), srv_info.addr.c_str(), srv_info.port, hid, mysrvc->weight,
				mysrvc->max_connections, mysrvc->use_ssl
			);

			res = 0;
		}
	}

	return res;
}

int PgSQL_HostGroups_Manager::remove_server_in_hg(uint32_t hid, const string& addr, uint16_t port) {
	PgSQL_SrvC* mysrvc = find_server_in_hg(hid, addr, port);
	if (mysrvc == nullptr) {
		return -1;
	}

	uint64_t mysrvc_addr = reinterpret_cast<uint64_t>(mysrvc);

	proxy_warning(
		"Removed server at address %ld, hostgroup %d, address %s port %d."
		" Setting status OFFLINE HARD and immediately dropping all free connections."
		" Used connections will be dropped when trying to use them\n",
		mysrvc_addr, hid, mysrvc->address, mysrvc->port
	);

	// Set the server status
	mysrvc->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	mysrvc->ConnectionsFree->drop_all_connections();

	// TODO-NOTE: This is only required in case the caller isn't going to perform:
	//   - Full deletion of servers in the target 'hid'.
	//   - Table regeneration for the servers in the target 'hid'.
	// This is a very common pattern when further operations have been performed over the
	// servers, e.g. a set of servers additions and deletions over the target hostgroups.
	// ////////////////////////////////////////////////////////////////////////

	// Remove the server from the table
	const string del_srv_query { "DELETE FROM pgsql_servers WHERE mem_pointer=" + std::to_string(mysrvc_addr) };
	mydb->execute(del_srv_query.c_str());

	// ////////////////////////////////////////////////////////////////////////

	return 0;
}

PgSQL_SrvC* PgSQL_HostGroups_Manager::find_server_in_hg(unsigned int _hid, const std::string& addr, int port) {
	PgSQL_SrvC* f_server = nullptr;

	PgSQL_HGC* myhgc = nullptr;
	for (uint32_t i = 0; i < MyHostGroups->len; i++) {
		myhgc = static_cast<PgSQL_HGC*>(MyHostGroups->index(i));

		if (myhgc->hid == _hid) {
			break;
		}
	}

	if (myhgc != nullptr) {
		for (uint32_t j = 0; j < myhgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC* mysrvc = static_cast<PgSQL_SrvC*>(myhgc->mysrvs->servers->index(j));

			if (strcmp(mysrvc->address, addr.c_str()) == 0 && mysrvc->port == port) {
				f_server = mysrvc;
			}
		}
	}

	return f_server;
}

void PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::copy_if_not_exists(Type dest_type, Type src_type) {

	assert(dest_type != src_type);

	const std::vector<Node>& src_nodes = mapping[src_type];

	if (src_nodes.empty()) return;

	std::vector<Node>& dest_nodes = mapping[dest_type];
	std::list<Node> append;

	for (const auto& src_node : src_nodes) {

		for (const auto& dest_node : dest_nodes) {

			if (src_node.reader_hostgroup_id == dest_node.reader_hostgroup_id &&
				src_node.writer_hostgroup_id == dest_node.writer_hostgroup_id) {
				goto __skip;
			}
		}

		append.push_back(src_node);

	__skip:
		continue;
	}

	if (append.empty()) {
		return;
	}

	if (dest_nodes.capacity() < (dest_nodes.size() + append.size()))
		dest_nodes.reserve(dest_nodes.size() + append.size());

	for (auto& node : append) {

		if (node.srv->status == MYSQL_SERVER_STATUS_SHUNNED ||
			node.srv->status == MYSQL_SERVER_STATUS_SHUNNED_REPLICATION_LAG) {
			// Status updated from "*SHUNNED" to "ONLINE" as "read_only" value was successfully 
			// retrieved from the backend server, indicating server is now online.
			node.srv->set_status(MYSQL_SERVER_STATUS_ONLINE);
		}

		PgSQL_SrvC* new_srv = insert_HGM(get_hostgroup_id(dest_type, node), node.srv);
			
		if (!new_srv) assert(0);
			
		node.srv = new_srv;
		dest_nodes.push_back(node);
	}
}

void PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::remove(Type type, size_t index) {

	std::vector<Node>& nodes = mapping[type];

	// ensure that we're not attempting to access out of the bounds of the container.
	assert(index < nodes.size());

	remove_HGM(nodes[index].srv);

	//Swap the element with the back element, except in the case when we're the last element.
	if (index + 1 != nodes.size())
		std::swap(nodes[index], nodes.back());

	//Pop the back of the container, deleting our old element.
	nodes.pop_back();
}

void PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::clear(Type type) {

	for (const auto& node : mapping[type]) {
		remove_HGM(node.srv);
	}

	mapping[type].clear();
}

unsigned int PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::get_hostgroup_id(Type type, const Node& node) const {

	if (type == Type::WRITER)
		return node.writer_hostgroup_id;
	else if (type == Type::READER)
		return node.reader_hostgroup_id;
	else
		assert(0);
}

PgSQL_SrvC* PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::insert_HGM(unsigned int hostgroup_id, const PgSQL_SrvC* srv) {

	PgSQL_HGC* myhgc = myHGM->MyHGC_lookup(hostgroup_id);

	if (!myhgc)
		return NULL;

	PgSQL_SrvC* ret_srv = NULL;
	
	for (uint32_t j = 0; j < myhgc->mysrvs->cnt(); j++) {
		PgSQL_SrvC* mysrvc = static_cast<PgSQL_SrvC*>(myhgc->mysrvs->servers->index(j));
		if (strcmp(mysrvc->address, srv->address) == 0 && mysrvc->port == srv->port) {
			if (mysrvc->status == MYSQL_SERVER_STATUS_OFFLINE_HARD) {
				
				mysrvc->weight = srv->weight;
				mysrvc->compression = srv->compression;
				mysrvc->max_connections = srv->max_connections;
				mysrvc->max_replication_lag = srv->max_replication_lag;
				mysrvc->use_ssl = srv->use_ssl;
				mysrvc->max_latency_us = srv->max_latency_us;
				mysrvc->comment = strdup(srv->comment);
#if POLARDB_PROXY
				myHGM->polardb_fast_topology_wrlock();
				myHGM->polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
				myHGM->polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
				mysrvc->set_status(MYSQL_SERVER_STATUS_ONLINE);

				if (GloPTH->variables.hostgroup_manager_verbose) {
					proxy_info(
						"Found server node in Host Group Container %s:%d as 'OFFLINE_HARD', setting back as 'ONLINE' with:"
						" hostgroup_id=%d, weight=%ld, compression=%d, max_connections=%ld, use_ssl=%d,"
						" max_replication_lag=%d, max_latency_ms=%d, comment=%s\n",
						mysrvc->address, mysrvc->port, hostgroup_id, mysrvc->weight, mysrvc->compression,
						mysrvc->max_connections, mysrvc->use_ssl, mysrvc->max_replication_lag, (mysrvc->max_latency_us / 1000),
						mysrvc->comment
					);
				}
				ret_srv = mysrvc;
				break;
			}
		}
	}
	
	if (!ret_srv) {
		if (GloPTH->variables.hostgroup_manager_verbose) {
			proxy_info("Creating new server in HG %d : %s:%d , weight=%ld, status=%d\n", hostgroup_id, srv->address, srv->port, srv->weight, srv->status);
		}

		proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5, "Adding new server %s:%d , weight=%ld, status=%d, mem_ptr=%p into hostgroup=%d\n", srv->address, srv->port, srv->weight, srv->status, srv, hostgroup_id);

		ret_srv = new PgSQL_SrvC(srv->address, srv->port, srv->weight, srv->status, srv->compression,
			srv->max_connections, srv->max_replication_lag, srv->use_ssl, (srv->max_latency_us / 1000), srv->comment);

#if POLARDB_PROXY
		myHGM->polardb_fast_topology_wrlock();
#endif // POLARDB_PROXY
		myhgc->mysrvs->add(ret_srv);
#if POLARDB_PROXY
		myHGM->polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks();
		myHGM->polardb_fast_topology_unlock();
#endif // POLARDB_PROXY
	}

	return ret_srv;
}

void PgSQL_HostGroups_Manager::HostGroup_Server_Mapping::remove_HGM(PgSQL_SrvC* srv) {
	proxy_warning("Removed server at address %p, hostgroup %d, address %s port %d. Setting status OFFLINE HARD and immediately dropping all free connections. Used connections will be dropped when trying to use them\n", (void*)srv, srv->myhgc->hid, srv->address, srv->port);
	srv->set_status(MYSQL_SERVER_STATUS_OFFLINE_HARD);
	srv->ConnectionsFree->drop_all_connections();
}

#if POLARDB_PROXY
// ===========================================================================
// PolarDB LSN session-consistency support
// ===========================================================================
//
// Topology lookups (writer/reader pairing), the per-server LSN cache fed by the
// monitor, and a byte-lag/cache-freshness reader-acquisition plan. The HG topology cache is
// populated by the admin loader; until then polardb_active stays false and
// every accessor below fails safe (returns "not configured" / 0 / no reader).
//
// Fallback default for pgsql-polardb_reader_lsn_max_age_ms (max age of a cached
// per-server LSN to trust) used when the knob is unset or non-positive.
//
// TODO: pgsql-polardb_max_reader_lag_ms is deferred. The PgSQL path does not currently
// produce a real millisecond replica-lag value; use LSN byte lag and cache
// freshness as the supported LSN-only safety controls.


void PgSQL_HostGroups_Manager::polardb_warn_config_mismatches() {
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot || snapshot->by_hostgroup.empty()) {
		return;
	}

	const PolarDB_ParsedGlobalConfigValue global_config =
		polardb_current_global_config();
	const int global_consistency_mode = global_config.consistency_mode;
	const int global_proxy_protocol = static_cast<int>(
		global_config.startup.proxy_protocol);
	const bool profile_off =
		polardb_profile_from_int(global_config.profile) ==
			PolarDB_Profile::OFF;
	const char* global_identity_host =
		global_config.startup.configured_identity.host.c_str();
	const int global_identity_port =
		global_config.startup.configured_identity.port;

	polardb_warn_effective_config_mismatches(*snapshot,
		global_consistency_mode, global_proxy_protocol, profile_off,
		global_identity_host, global_identity_port);
}

std::string PgSQL_HostGroups_Manager::polardb_loaded_hostgroup_policy_error(
		const PolarDB_ParsedGlobalConfigValue& global_config) const {
	if (polardb_profile_from_int(global_config.profile) ==
			PolarDB_Profile::OFF) {
		return {};
	}
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) {
		return {};
	}
	for (const auto& entry : snapshot->by_hostgroup) {
		const PolarDB_HG_Config& config = entry.second.config;
		if (!config.is_polardb_hostgroup ||
				entry.first !=
					static_cast<unsigned int>(config.writer_hostgroup)) {
			continue;
		}
		const int effective_mode = config.policy.consistency_mode >= 0
			? config.policy.consistency_mode
			: global_config.consistency_mode;
		const int effective_protocol = config.policy.proxy_protocol >= 0
			? config.policy.proxy_protocol
			: static_cast<int>(global_config.startup.proxy_protocol);
		const char* error = polardb_hostgroup_lsn_source_error(
			polardb_consistency_from_int(effective_mode),
			polardb_proxy_protocol_from_int(effective_protocol));
		if (!error) {
			continue;
		}
		char message[384];
		snprintf(message, sizeof(message),
			"writer_hostgroup=%d reader_hostgroup=%d: %s",
			config.writer_hostgroup, config.reader_hostgroup, error);
		return message;
	}
	return {};
}

bool PgSQL_HostGroups_Manager::polardb_has_effective_global_lsn(
		int global_consistency_mode) const {
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) {
		return false;
	}
	for (const auto& entry : snapshot->by_hostgroup) {
		const PolarDB_HG_Config& config = entry.second.config;
		if (!config.is_polardb_hostgroup ||
				entry.first !=
					static_cast<unsigned int>(config.writer_hostgroup)) {
			continue;
		}
		const int effective_mode = config.policy.consistency_mode >= 0
			? config.policy.consistency_mode : global_consistency_mode;
		if (effective_mode ==
				static_cast<int>(PolarDB_ConsistencyMode::GLOBAL_LSN)) {
			return true;
		}
	}
	return false;
}

bool PgSQL_HostGroups_Manager::polardb_update_server_lsn_from_monitor(
		const char* hostname, uint16_t port, uint64_t lsn,
		PolarDB_NodeType node_type) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return false;
	if (hostname == nullptr) return false;
	const uint64_t now_us = monotonic_time();
	bool any_advanced = false;
	bool matched = false;
	POLARDB_TRACE("PolarDB LSN CACHE: monitor update enter host=%s port=%u lsn=%lu\n",
		hostname, port, (unsigned long)lsn);

	wrlock();

	for (unsigned int i = 0; i < MyHostGroups->len; i++) {
		PgSQL_HGC* hgc = (PgSQL_HGC*)MyHostGroups->index(i);
		if (!hgc) continue;

		std::shared_ptr<std::atomic<uint64_t>> group_lsn;
		std::shared_ptr<std::atomic<uint64_t>> max_replica_replay_lsn;
		auto reader_it = polardb_reader_to_writer_.find(hgc->hid);
		if (reader_it != polardb_reader_to_writer_.end()) {
			PgSQL_HGC* writer_hgc = MyHGC_find(reader_it->second);
			if (writer_hgc) {
				group_lsn = writer_hgc->repl_config.polardb_group_lsn;
				max_replica_replay_lsn =
					writer_hgc->repl_config.polardb_max_replica_replay_lsn;
			}
		} else if (polardb_writer_to_reader_.find(hgc->hid) !=
				polardb_writer_to_reader_.end()) {
			group_lsn = hgc->repl_config.polardb_group_lsn;
			max_replica_replay_lsn =
				hgc->repl_config.polardb_max_replica_replay_lsn;
		}

		for (unsigned int j = 0; j < hgc->mysrvs->cnt(); j++) {
			PgSQL_SrvC* srv = hgc->mysrvs->idx(j);
			if (srv && strcmp(srv->address, hostname) == 0 && srv->port == port) {
				matched = true;
				if (srv->status != MYSQL_SERVER_STATUS_ONLINE) {
					POLARDB_TRACE(
						"PolarDB LSN CACHE: skip non-ONLINE host=%s port=%u "
						"hg=%u status=%d lsn=%lu\n",
						hostname, port, hgc->hid, srv->status, (unsigned long)lsn);
					continue;
				}

				// The HGM write lock serializes this monitor observation with writer
				// epoch reset. Role and replay LSN come from this same health result.
				srv->polardb_lock_lsn_cache();
				bool advanced = srv->polardb_advance_lsn(lsn, now_us);

				// group_lsn intentionally accepts primary and replica observations.
				// The replica-only maximum proves that a physical replica replayed a
				// target and therefore never accepts a writer-as-reader observation.
				if (group_lsn) {
					polardb_atomic_max_u64(group_lsn, lsn);
				}
				if (PolarDB_Protocol::is_reader(node_type) &&
						polardb_atomic_max_u64(max_replica_replay_lsn, lsn)) {
					POLARDB_HGM_STATUS_COUNT_ONE(
						status, replica_replay_lsn_advanced);
				}
				srv->polardb_unlock_lsn_cache();

				POLARDB_TRACE("PolarDB LSN CACHE: monitor update matched host=%s port=%u lsn=%lu advanced=%d\n",
					hostname, port, (unsigned long)lsn, advanced ? 1 : 0);
				any_advanced = any_advanced || advanced;
			}
		}
	}

	wrunlock();
	return matched && any_advanced;
}

bool PgSQL_HostGroups_Manager::polardb_accept_rfq_server_lsn(
		PgSQL_SrvC* srv, unsigned int backend_hostgroup_id,
		uint64_t lsn, const PolarDB_WriterScope& request_scope,
		PgSQL_Thread* worker) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(worker, rfq_lsn_reject_inactive);
		return false;
	}
	if (!srv || lsn == 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(worker, rfq_lsn_reject_invalid_input);
		return false;
	}

	if (!request_scope.valid()) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			worker, rfq_lsn_reject_missing_request_scope);
		POLARDB_TRACE(
			"PolarDB LSN CACHE: skip direct RFQ update without request "
			"writer group/epoch hg=%u request_hg=%d lsn=%lu\n",
			backend_hostgroup_id, request_scope.hg, (unsigned long)lsn);
		return false;
	}

	const auto snapshot = get_polardb_topology_snapshot_cached();
	const PolarDB_HG_SnapshotEntry* backend = nullptr;
	if (snapshot) {
		const auto found =
			snapshot->by_hostgroup.find(backend_hostgroup_id);
		if (found != snapshot->by_hostgroup.end()) {
			backend = &found->second;
		}
	}
	if (!backend || !backend->writer_epoch) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			worker, rfq_lsn_reject_missing_backend_scope);
		POLARDB_TRACE(
			"PolarDB LSN CACHE: skip direct RFQ update without current "
			"writer epoch hg=%u lsn=%lu request_epoch=%lu\n",
			backend_hostgroup_id, (unsigned long)lsn,
			(unsigned long)request_scope.epoch);
		return false;
	}

	const PolarDB_HG_Config& backend_config = backend->config;
	uint64_t current_writer_epoch =
		backend->writer_epoch->load(std::memory_order_acquire);
	if (!request_scope.matches(
			PolarDB_WriterScope{
				backend_config.writer_hostgroup,
				current_writer_epoch})) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			worker, rfq_lsn_reject_scope_mismatch);
		POLARDB_TRACE(
			"PolarDB LSN CACHE: skip stale/cross-group direct RFQ update "
			"hg=%u request_hg=%d current_hg=%d request_epoch=%lu "
			"current_epoch=%lu lsn=%lu\n",
			backend_hostgroup_id,
			request_scope.hg, backend_config.writer_hostgroup,
			(unsigned long)request_scope.epoch,
			(unsigned long)current_writer_epoch,
			(unsigned long)lsn);
		return false;
	}

	// The connection using this parent is still in ConnectionsUsed while result processing
	// runs, and OFFLINE_HARD deletion waits for both used and free lists to drain.
	// Therefore the PgSQL_SrvC object is alive here. The server guard prevents a
	// writer change from clearing the caches between the final epoch check and
	// this update.
	//
	// Direct RFQ carries no authoritative physical role. It remains a valid
	// server/group observation for this writer epoch, but only a monitor result
	// containing role and replay LSN together may advance replica replay proof.
	const uint64_t now_us = monotonic_time();
	const bool update_cache = !worker ||
		worker->polardb_track_server_lsn_update(
			srv, lsn, request_scope, now_us);
	bool advanced = false;
	if (update_cache) {
		// Writer reset takes every affected server guard before clearing the
		// server/group cells and then advancing the epoch. This check and update
		// therefore happen wholly before that reset, or see its new epoch and
		// reject the old RFQ.
		srv->polardb_lock_lsn_cache();
		current_writer_epoch =
			backend->writer_epoch->load(std::memory_order_acquire);
		if (!request_scope.matches(
				PolarDB_WriterScope{
					backend_config.writer_hostgroup,
					current_writer_epoch})) {
			srv->polardb_unlock_lsn_cache();
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				worker, rfq_lsn_reject_scope_mismatch);
			POLARDB_TRACE(
				"PolarDB LSN CACHE: writer changed before direct RFQ "
				"cache update hg=%u request_hg=%d current_hg=%d "
				"request_epoch=%lu current_epoch=%lu lsn=%lu\n",
				backend_hostgroup_id,
				request_scope.hg, backend_config.writer_hostgroup,
				(unsigned long)request_scope.epoch,
				(unsigned long)current_writer_epoch,
				(unsigned long)lsn);
			return false;
		}
		advanced = srv->polardb_advance_lsn(lsn, now_us);
		polardb_atomic_max_u64(backend->group_lsn, lsn);
		srv->polardb_unlock_lsn_cache();
	}
	// A coalesced observation writes no shared state and deliberately takes no
	// guard. Result processing may record it only in session state tagged with
	// request_scope. If a writer change overlaps this return, collect clears that
	// old scoped state before the next query can use it.
#if POLARDB_PROFILE
	if (worker) {
		worker->polardb_profile_note_lsn_update(advanced);
	}
#else
	(void)advanced;
#endif // POLARDB_PROFILE

	POLARDB_TRACE(
		"PolarDB LSN CACHE: accepted direct RFQ update hg=%u lsn=%lu "
			"cache_updated=%d advanced=%d request_hg=%d current_hg=%d "
			"request_epoch=%lu current_epoch=%lu\n",
			backend_hostgroup_id, (unsigned long)lsn,
			update_cache ? 1 : 0,
			advanced ? 1 : 0,
			request_scope.hg, backend_config.writer_hostgroup,
			(unsigned long)request_scope.epoch,
			(unsigned long)current_writer_epoch);
	return true;
}

uint64_t PgSQL_HostGroups_Manager::get_polardb_group_lsn(
		unsigned int writer_hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return 0;

	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return 0;
	auto it = snapshot->by_hostgroup.find(writer_hostgroup_id);
	if (it == snapshot->by_hostgroup.end() || !it->second.group_lsn) {
		return 0;
	}
	return it->second.group_lsn->load(std::memory_order_relaxed);
}

uint64_t PgSQL_HostGroups_Manager::get_polardb_max_replica_replay_lsn(
		const PolarDB_WriterScope& writer_scope) {
	if (!status.polardb_active.load(std::memory_order_relaxed) ||
			!writer_scope.valid()) {
		return 0;
	}

	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return 0;
	const auto it = snapshot->by_hostgroup.find(
		static_cast<unsigned int>(writer_scope.hg));
	if (it == snapshot->by_hostgroup.end() ||
			!it->second.max_replica_replay_lsn || !it->second.writer_epoch) {
		return 0;
	}
	if (it->second.writer_epoch->load(std::memory_order_acquire) !=
			writer_scope.epoch) {
		return 0;
	}
	const uint64_t replica_lsn =
		it->second.max_replica_replay_lsn->load(std::memory_order_relaxed);
	return it->second.writer_epoch->load(std::memory_order_acquire) ==
		writer_scope.epoch ? replica_lsn : 0;
}

#if POLARDB_PROXY
void PgSQL_HostGroups_Manager::request_split_warmup(
		unsigned int reader_hostgroup_id,
		const char* username,
		const char* password,
		const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn,
		const PgSQL_SrvC* target_server) {
	if (polardb_reader_pool_) {
		polardb_reader_pool_->request_split_warmup(
			reader_hostgroup_id, username, password, dbname, startup_client,
			client_conn, target_server);
	}
}

void PgSQL_HostGroups_Manager::warm_split_pools() {
	if (polardb_reader_pool_) {
		polardb_reader_pool_->warm_split_pools();
	}
}

void PgSQL_HostGroups_Manager::polardb_refresh_split_warmup_variables() {
	if (polardb_reader_pool_) {
		polardb_reader_pool_->refresh_thread_variables();
	}
}
#endif // POLARDB_PROXY

PolarDB_ReaderResult PgSQL_HostGroups_Manager::polardb_acquire_reader_connection(
		unsigned int _hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address, int exclude_port,
		bool confirm_reader_group_capacity) {
	if (!polardb_reader_pool_) {
		return PolarDB_ReaderResult{};
	}
	return polardb_reader_pool_->polardb_acquire_reader_connection(
		_hid, sess, reader_plan, wait_spec, only_pooled,
		exclude_address, exclude_port, confirm_reader_group_capacity);
}

bool PgSQL_HostGroups_Manager::polardb_reader_server_can_serve_request(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* selected_server_snapshot) const {
	return polardb_reader_pool_ &&
		polardb_reader_pool_->server_can_serve_request(
			hostgroup_id, server, reader_plan, wait_spec,
			exclude_address, exclude_port, selected_server_snapshot);
}

bool PgSQL_HostGroups_Manager::polardb_resolve_reader_server_snapshot(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* server_snapshot) const {
	return polardb_reader_pool_ &&
		polardb_reader_pool_->resolve_reader_server_snapshot(
			hostgroup_id, server, exclude_address, exclude_port,
			server_snapshot);
}

bool PgSQL_HostGroups_Manager::polardb_reader_server_meets_lag_policy(
		PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec, uint64_t now_us) const {
	return polardb_reader_pool_ &&
		polardb_reader_pool_->server_meets_lag_policy(
			server, reader_plan, wait_spec, now_us);
}

bool PgSQL_HostGroups_Manager::polardb_reader_pool_reservation_match_key(
		unsigned int hostgroup_id, PgSQL_Session* sess,
		const PolarDB_WaitSpec& wait_spec,
		PgSQL_PoolMatchKey* match_key) const {
	return polardb_reader_pool_ &&
		polardb_reader_pool_->reader_pool_reservation_match_key(
			hostgroup_id, sess, wait_spec, match_key);
}

#endif // POLARDB_PROXY
