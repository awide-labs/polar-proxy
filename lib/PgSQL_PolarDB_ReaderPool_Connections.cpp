/**
 * @file PgSQL_PolarDB_ReaderPool_Connections.cpp
 * @brief Reader connection compatibility, reuse, and return policy.
 */

#include "PgSQL_PolarDB_ReaderPool.h"

#include "PgSQL_PolarDB_ReaderPool_Internal.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Session.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY
#define POLARDB_STATUS_COUNT(name, value) \
	POLARDB_HGM_STATUS_COUNT(hgm_->status, name, value)
#define POLARDB_STATUS_COUNT_ONE(name) \
	POLARDB_STATUS_COUNT(name, 1)

/**
 * @brief Read the used and free pooled connection counts for this server.
 *
 * The two counters are read lock-free and independently of each other, without
 * the server pool mutex, so the pair is an estimate rather than a consistent
 * snapshot: total() can transiently exceed max_connections, or miss a connection
 * that is in flight between the used and free lists.
 *
 * @return Used and free counts. Use them for capacity heuristics only, never as
 *         an exact accounting of the two lists.
 */
PolarDB_PoolConnStats PgSQL_SrvC::polardb_pool_conn_stats() const {
	PolarDB_PoolConnStats snapshot;
	snapshot.used = pool_used_count_value();
	snapshot.free = pool_free_count_value();
	return snapshot;
}

/**
 * @brief Read the number of sockets this server currently holds open.
 *
 * @return Used plus free connection count, read lock-free and therefore an
 *         estimate.
 */
unsigned int PgSQL_SrvC::polardb_pool_total_count() const {
	return pool_used_count_value() + pool_free_count_value();
}

/**
 * @brief Report whether one more connection to this server may be put to work.
 *
 * This bounds only the connections that are currently in use, so it stays true
 * when the server already holds max_connections sockets because some of them are
 * free and pooled: a free connection is expected to be reused rather than a new
 * socket opened. Use polardb_pool_can_open_socket() instead before creating a
 * new backend socket; taking the wrong one of the pair either over-admits
 * sockets or needlessly refuses a valid reuse.
 *
 * @return true when the used count is below max_connections. false when
 *         max_connections is unset (zero or negative) or already reached by
 *         connections in use.
 */
bool PgSQL_SrvC::polardb_pool_can_add_active_connection() const {
	return max_connections > 0 &&
		pool_used_count_value() < static_cast<unsigned int>(max_connections);
}

/**
 * @brief Report whether a new socket may be opened to this server.
 *
 * This bounds the total number of open sockets, used plus free, against
 * max_connections, and is the check to run before creating a backend. Use
 * polardb_pool_can_add_active_connection() instead to check whether an already
 * pooled connection may be handed out.
 *
 * The counts behind it are read lock-free, so the answer is advisory: capacity
 * can change between this check and the add_matching_connection() that follows,
 * and that add must still be allowed to fail.
 *
 * @return true when used plus free is below max_connections. false when
 *         max_connections is unset (zero or negative) or already reached.
 */
bool PgSQL_SrvC::polardb_pool_can_open_socket() const {
	return max_connections > 0 &&
		polardb_pool_total_count() < static_cast<unsigned int>(max_connections);
}

static uint64_t polardb_pool_auth_reuse_key(
		const PgSQL_Connection_userinfo* userinfo) {
	return userinfo
		? polardb_pool_auth_reuse_key_strings(
			userinfo->username, userinfo->dbname)
		: 0;
}

/**
 * @brief Hash a user name and database name into the pool auth reuse key.
 *
 * The key draws the boundary that stops a backend authenticated for one
 * user/database pair from being handed to another.
 *
 * @param username  Backend user name.
 * @param dbname    Backend database name.
 * @return FNV hash of the two names, or 0 for the reserved 'no key' value.
 *         Comparison sites skip the auth check entirely when the hash they hold
 *         is 0, so 0 means 'do not compare', not 'no match'. A null username or
 *         dbname yields 0, and a computed hash that happens to be 0 is not folded
 *         away, so it is indistinguishable from 'no key'.
 */
uint64_t polardb_pool_auth_reuse_key_strings(
		const char* username, const char* dbname) {
	if (!username || !dbname) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	hash = polardb_pool_hash_cstr(hash, username);
	hash = polardb_pool_hash_cstr(hash, dbname);
	return hash;
}

/**
 * @brief Compute the session options reuse key for a connection.
 *
 * The key covers every low water mark variable hash plus the set of dynamic
 * variable indexes and their hashes, so it describes the connection's session
 * state only for as long as those hashes are current. Callers cache the result on
 * the connection, so it must be recomputed whenever the session variables change;
 * PgSQL_Variables clears the cached key for exactly that reason.
 *
 * @param conn  Connection whose variable hashes are read.
 * @return Hash of the session option state. 0 is reserved for 'unknown' and is
 *         returned only for a null connection; a computed 0 is folded to 1 so a
 *         real key is never mistaken for 'unknown'. Comparison sites skip the
 *         session option check when the hash they hold is 0.
 */
uint64_t polardb_pool_session_options_reuse_key(
		const PgSQL_Connection* conn) {
	if (!conn) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		hash = polardb_pool_hash_u64(hash, conn->var_hash[i]);
	}
	hash = polardb_pool_hash_u64(hash, conn->dynamic_variables_idx.size());
	for (uint32_t idx : conn->dynamic_variables_idx) {
		hash = polardb_pool_hash_u64(hash, idx);
		hash = polardb_pool_hash_u64(hash, conn->var_hash[idx]);
	}
	return hash ? hash : 1;
}

/**
 * @brief Build the pool key describing a connection under a given pool profile.
 *
 * The contents of the key depend on the profile: startup_identity_hash is filled
 * in only for PolarDB_PoolProfile::RFQ and left 0 for every other profile. Keys
 * built under different profiles are therefore not comparable, and comparing an
 * RFQ request against a key built under another profile silently skips the
 * identity boundary.
 *
 * @param conn     Connection to describe. A null connection yields an empty key.
 * @param profile  Pool profile the key is built for.
 * @return The pool key for this connection under this profile.
 */
static PolarDB_PoolKey polardb_pool_key_for_connection_profile(
		const PgSQL_Connection* conn,
		PolarDB_PoolProfile profile) {
	PolarDB_PoolKey key;
	if (!conn) {
		return key;
	}
	key.auth_hash = polardb_pool_auth_reuse_key(conn->userinfo);
	if (profile == PolarDB_PoolProfile::RFQ) {
		key.startup_identity_hash = polardb_startup_client_reuse_key(
			conn->polardb_startup_client);
	}
	key.startup_options_hash = polardb_pool_session_options_reuse_key(conn);
	return key;
}

/**
 * @brief Fill the connection's pool key when it does not have one yet.
 *
 * This only ever fills an empty key: an existing key is left as it is and is
 * never recomputed here. Invalidation belongs to the code that changes session
 * state - PgSQL_Variables clears conn->polardb_pool_key - and every cached-key
 * comparison depends on that.
 *
 * @param conn     Connection to key. A null connection is ignored.
 * @param profile  Pool profile the key is built for.
 */
static void polardb_ensure_pool_key(
		PgSQL_Connection* conn, PolarDB_PoolProfile profile) {
	if (!conn || !conn->polardb_pool_key.empty()) {
		return;
	}
	conn->polardb_pool_key =
		polardb_pool_key_for_connection_profile(conn, profile);
}

/**
 * @brief Fill the connection's pool key from its installed startup profile.
 *
 * The pool profile is derived from conn->polardb_startup_profile, so call this
 * after the startup settings is installed on the connection and before deriving a
 * match key from it with polardb_core_pool_match_key_for_conn(): without a key
 * the match key is empty and the connection is silently dropped from the exact
 * pool. An existing key is kept as it is and never refreshed; invalidating a
 * stale key belongs to the code that changes the session state.
 *
 * @param conn  Connection to key. A null connection is ignored.
 */
void polardb_ensure_pool_key(PgSQL_Connection* conn) {
	if (!conn) {
		return;
	}
	polardb_ensure_pool_key(
		conn,
		polardb_pool_profile_from_startup_profile(
			conn->polardb_startup_profile));
}

/**
 * @brief Key a pool request for the connection this session is asking for.
 *
 * Besides returning the keyed request, this caches what it derives: the auth and
 * session option hashes are written back onto the pool key of the session's
 * client connection, and for an RFQ request the startup identity hash and the
 * identity mode it was derived under are cached on sess->polardb_route_state.
 * The cached identity hash is re-derived only when the identity mode differs, so
 * any other change to the session's startup identity is not observed here.
 *
 * @param startup_profile     Startup profile the connection must use.
 * @param only_pooled         Whether opening a new connection is forbidden.
 * @param require_rfq_profile Whether the request requires RFQ fields.
 * @param sess     Session the request is keyed for. May be null, and so may its
 *                 client connection, in which case nothing is cached and the
 *                 hashes are computed from what is available.
 * @return A complete request keyed for this session.
 */
PolarDB_PoolRequest polardb_prepare_pool_request_for_session(
		const PolarDB_StartupProfile& startup_profile,
		bool only_pooled,
		bool require_rfq_profile,
		PgSQL_Session* sess) {
	PolarDB_PoolKey key;
	const int startup_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
	const PolarDB_PoolProfile expected_profile = require_rfq_profile
		? PolarDB_PoolProfile::RFQ
		: polardb_pool_profile_from_startup_profile(startup_profile);
	PgSQL_Connection* client_conn =
		(sess && sess->client_myds && sess->client_myds->myconn)
			? sess->client_myds->myconn : nullptr;
	if (client_conn && client_conn->polardb_pool_key.auth_hash != 0) {
		key.auth_hash = client_conn->polardb_pool_key.auth_hash;
	} else {
		key.auth_hash =
			polardb_pool_auth_reuse_key(
				client_conn ? client_conn->userinfo : nullptr);
		if (client_conn) {
			client_conn->polardb_pool_key.auth_hash =
				key.auth_hash;
		}
	}
	if (client_conn &&
			client_conn->polardb_pool_key.startup_options_hash != 0) {
		key.startup_options_hash =
			client_conn->polardb_pool_key.startup_options_hash;
	} else {
		key.startup_options_hash =
			polardb_pool_session_options_reuse_key(client_conn);
		if (client_conn) {
			client_conn->polardb_pool_key.startup_options_hash =
				key.startup_options_hash;
		}
	}
	if (expected_profile == PolarDB_PoolProfile::RFQ) {
		if (sess &&
				sess->polardb_route_state.reader_pool_startup_identity_mode ==
					startup_identity_mode &&
				sess->polardb_route_state.reader_pool_startup_identity_hash != 0) {
			key.startup_identity_hash =
				sess->polardb_route_state.reader_pool_startup_identity_hash;
		} else {
			PolarDB_StartupClientContext startup_client;
			if (polardb_startup_client_from_session(sess, &startup_client)) {
				key.startup_identity_hash =
					polardb_startup_client_reuse_key(startup_client);
				if (sess) {
					sess->polardb_route_state.reader_pool_startup_identity_hash =
						key.startup_identity_hash;
					sess->polardb_route_state.reader_pool_startup_identity_mode =
						startup_identity_mode;
				}
			}
		}
	}
	return PolarDB_PoolRequest(
		key, startup_profile, startup_identity_mode,
		expected_profile, only_pooled);
}

/**
 * @brief Report whether a connection's startup identity mode satisfies a request.
 *
 * True means 'no identity boundary applies, or it is met'. The mode is compared
 * only for an RFQ request, because no other profile carries a client-derived
 * identity, and a null connection is accepted for the same reason. The
 * neighbouring polardb_startup_profile_matches_request_generation() rejects a
 * null connection instead, so the two predicates do not treat null alike.
 *
 * @param conn          Connection to check. Null is accepted as matching.
 * @param pool_request  Request being served.
 * @return true when the identity mode does not stand in the way of reusing this
 *         connection for this request.
 */
static bool polardb_identity_mode_matches(
		const PgSQL_Connection* conn,
		const PolarDB_PoolRequest& pool_request) {
	if (!conn || pool_request.expected_profile != PolarDB_PoolProfile::RFQ) {
		return true;
	}
	return conn->polardb_startup_identity_mode ==
		pool_request.startup_identity_mode;
}

bool polardb_connection_startup_settings_match(
		const PgSQL_Connection* conn,
		const PolarDB_StartupProfile& startup_profile,
		const PolarDB_StartupClientContext* startup_client,
		uint64_t current_startup_generation,
		int current_identity_mode) {
	if (!conn || !conn->polardb_startup_settings_set ||
			!polardb_startup_profile_matches_request_generation(
				conn->polardb_startup_profile,
				conn->polardb_startup_profile_generation,
				startup_profile,
				current_identity_mode)) {
		return false;
	}
	if (!startup_profile.emits_startup_params()) {
		return true;
	}
	return conn->polardb_startup_config_generation ==
			current_startup_generation &&
		startup_client &&
		conn->polardb_startup_client.compatible_for_reuse(
			*startup_client);
}

static bool polardb_startup_profile_matches_request_generation(
		const PgSQL_Connection* conn,
		const PolarDB_PoolRequest& pool_request) {
	if (!conn) {
		return false;
	}
	return polardb_startup_profile_matches_request_generation(
		conn->polardb_startup_profile,
		conn->polardb_startup_profile_generation,
		pool_request.startup_profile,
		pool_request.startup_identity_mode);
}

/**
 * @brief Classify whether a pooled connection can serve this session's request.
 *
 * The caller must already own conn, having popped it from a pool or taken it from
 * the worker local cache. No pool mutex and no hostgroup manager lock is held or
 * taken here.
 *
 * When the connection's cached pool key equals the request key, the answer
 * short-circuits to EXACT without running requires_RESETTING_CONNECTION() or
 * comparing session variables one by one. That is sound only because the client
 * connection's cached key is cleared whenever its session state changes
 * (PgSQL_Variables), so an equal key implies equal session state; the connection
 * options are still compared on that path.
 *
 * As a side effect, the slow path caches the pool key on conn once the connection
 * is accepted.
 *
 * @param conn          Candidate connection, owned by the caller.
 * @param sess          Session asking for the connection. Its client connection
 *                      supplies the session state to compare against.
 * @param pool_request  Request describing the required profile, identity and key.
 * @return The reuse state, plus the number of matching session variables. EXACT
 *         means the connection can be used as is; NEEDS_RESET and
 *         NEEDS_VARIABLE_UPDATE mean it is recoverable at a cost; every other
 *         value means it cannot serve this request. matching_session_variables is
 *         filled in only on the slow path and stays 0 for a fast path EXACT, so
 *         do not use it to rank candidates.
 */
PolarDB_PoolReuseClassification polardb_classify_pool_conn_for_reuse(
		PgSQL_Connection* conn, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request) {
	PolarDB_PoolReuseClassification classification;
	if (!conn || !conn->parent || !sess ||
			conn->async_state_machine != ASYNC_IDLE ||
			!sess->client_myds || !sess->client_myds->myconn ||
			!sess->client_myds->myconn->userinfo ||
			!pool_request.ready_for_matching()) {
		classification.state = PolarDB_PoolReuseState::BAD_CONTEXT;
		return classification;
	}

	PgSQL_SrvC* srv = conn->parent;
	if (srv->polardb_fast_status_value() != MYSQL_SERVER_STATUS_ONLINE ||
			!polardb_startup_profile_matches_request_generation(
				conn, pool_request)) {
		classification.state = PolarDB_PoolReuseState::PROFILE_MISMATCH;
		return classification;
	}
	if (!polardb_identity_mode_matches(conn, pool_request)) {
		classification.state = PolarDB_PoolReuseState::IDENTITY_MISMATCH;
		return classification;
	}
	if (!conn->is_connected()) {
		classification.state = PolarDB_PoolReuseState::BAD_CONTEXT;
		return classification;
	}

	PgSQL_Connection* client_conn = sess->client_myds->myconn;
	const PolarDB_PoolKey& conn_key = conn->polardb_pool_key;
	if (!conn_key.empty()) {
		if (pool_request.key.auth_hash != 0 &&
				conn_key.auth_hash != pool_request.key.auth_hash) {
			classification.state = PolarDB_PoolReuseState::AUTH_MISMATCH;
			return classification;
		}
		if (pool_request.expected_profile == PolarDB_PoolProfile::RFQ &&
				pool_request.key.startup_identity_hash != 0 &&
				conn_key.startup_identity_hash !=
					pool_request.key.startup_identity_hash) {
			classification.state = PolarDB_PoolReuseState::IDENTITY_MISMATCH;
			return classification;
		}
		if (pool_request.key.startup_options_hash != 0 &&
				conn_key.startup_options_hash !=
					pool_request.key.startup_options_hash) {
			classification.state = PolarDB_PoolReuseState::SESSION_STATE_MISMATCH;
			return classification;
		}
	}

	if (conn->polardb_txn_split_xids_dirty) {
		classification.state = PolarDB_PoolReuseState::SESSION_STATE_MISMATCH;
		return classification;
	}
	const bool exact_cached_key =
		!conn_key.empty() && !pool_request.key.empty() &&
		conn_key == pool_request.key;
	if (exact_cached_key) {
		if (!conn->has_same_connection_options(client_conn)) {
			classification.state = PolarDB_PoolReuseState::AUTH_MISMATCH;
			return classification;
		}
		classification.state = PolarDB_PoolReuseState::EXACT;
		return classification;
	}
	if (sess->thread) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(sess->thread,
			reader_pool_key_full_check);
	}

	if (!conn->has_same_connection_options(client_conn)) {
		classification.state = PolarDB_PoolReuseState::AUTH_MISMATCH;
		return classification;
	}

	const bool cached_identity_match =
		pool_request.expected_profile == PolarDB_PoolProfile::RFQ &&
		!conn_key.empty() &&
		pool_request.key.startup_identity_hash != 0 &&
		conn_key.startup_identity_hash ==
			pool_request.key.startup_identity_hash;
	if (pool_request.expected_profile == PolarDB_PoolProfile::RFQ &&
			!cached_identity_match) {
		PolarDB_StartupClientContext required_startup_client;
		if (!polardb_startup_client_from_session(sess, &required_startup_client) ||
				!polardb_startup_client_compatible_for_warmup(
					conn->polardb_startup_client,
					required_startup_client)) {
			classification.state = PolarDB_PoolReuseState::IDENTITY_MISMATCH;
			return classification;
		}
	}

	if (conn->requires_RESETTING_CONNECTION(client_conn)) {
		classification.state = PolarDB_PoolReuseState::NEEDS_RESET;
		return classification;
	}

	unsigned int not_matching = 0;
	classification.matching_session_variables =
		conn->number_of_matching_session_variables(client_conn, not_matching);
	if (not_matching != 0) {
		classification.state = PolarDB_PoolReuseState::NEEDS_VARIABLE_UPDATE;
		return classification;
	}

	polardb_ensure_pool_key(conn, pool_request.expected_profile);
	classification.state = PolarDB_PoolReuseState::EXACT;
	return classification;
}

static bool polardb_reader_pool_conn_can_remain_pooled(
	const PgSQL_Connection* conn);

/**
 * @brief Report whether a connection may go back into the shared pool at all.
 *
 * This is the session-independent counterpart of
 * polardb_classify_pool_conn_for_reuse(), used on the return path. It checks the
 * startup profile generation, connection state, and return-side limits (dirty
 * split XIDs, largest query length, backend statement count) without comparing
 * against a requesting client.
 *
 * @param hgm   Hostgroup manager used to resolve the hostgroup's startup profile.
 *              Null yields BAD_CONTEXT.
 * @param srv   Server the connection must belong to. A null server, a server with
 *              no hostgroup, or a connection whose parent is a different server
 *              yields BAD_CONTEXT, as does a connection that is not idle.
 * @param conn  Connection being returned, owned by the caller.
 * @return EXACT when the connection may be pooled; it is the only reusable
 *         outcome. Every other value means the connection must not be pooled.
 *         SESSION_STATE_MISMATCH also covers the case where the thread handler
 *         globals are unavailable and the limits cannot be evaluated.
 */
static PolarDB_PoolReuseState polardb_pool_return_state(
		PgSQL_HostGroups_Manager* hgm, PgSQL_SrvC* srv,
		PgSQL_Connection* conn) {
	if (!conn || !srv || conn->parent != srv ||
			conn->async_state_machine != ASYNC_IDLE) {
		return PolarDB_PoolReuseState::BAD_CONTEXT;
	}

	if (!hgm || !srv->myhgc) {
		return PolarDB_PoolReuseState::BAD_CONTEXT;
	}
	const PolarDB_StartupProfile startup_profile =
		hgm->polardb_startup_profile_for_hostgroup(
			srv->myhgc->hid, pgsql_thread___polardb_proxy_protocol);
	// Check the current startup profile using the identity mode recorded when
	// this connection was opened.
	if (!polardb_startup_profile_matches_request_generation(
			conn->polardb_startup_profile,
			conn->polardb_startup_profile_generation,
			startup_profile,
			conn->polardb_startup_identity_mode)) {
		return PolarDB_PoolReuseState::PROFILE_MISMATCH;
	}
	if (!conn->is_connected()) {
		return PolarDB_PoolReuseState::BAD_CONTEXT;
	}

	if (conn->polardb_txn_split_xids_dirty || GloPTH == NULL ||
			conn->largest_query_length >
				(unsigned int)GloPTH->variables.threshold_query_length ||
			conn->local_stmts->get_num_backend_stmts() >
				(unsigned int)GloPTH->variables.max_stmts_per_connection) {
		return PolarDB_PoolReuseState::SESSION_STATE_MISMATCH;
	}

	return PolarDB_PoolReuseState::EXACT;
}

PgSQL_PolarDB_ReaderPool::ConnectionReturnDecision
PgSQL_PolarDB_ReaderPool::connection_return_decision(
		PgSQL_Connection* conn) {
	ConnectionReturnDecision decision;
	if (!conn || !conn->parent) {
		return decision;
	}
	PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
	if (!srv->myhgc || !hgm_->is_polardb_hostgroup(srv->myhgc->hid)) {
		return decision;
	}
	if (srv->polardb_fast_status_value() != MYSQL_SERVER_STATUS_ONLINE) {
		decision.status = PolarDB_ReaderConnectionReturnStatus::OFFLINE;
		return decision;
	}
	const bool pool_safe = polardb_reader_pool_conn_can_remain_pooled(conn);
	if (!pool_safe) {
		decision.status =
			PolarDB_ReaderConnectionReturnStatus::CLIENT_IDENTITY;
		return decision;
	}
	if (polardb_pool_return_state(hgm_, srv, conn) !=
			PolarDB_PoolReuseState::EXACT) {
		decision.status = PolarDB_ReaderConnectionReturnStatus::UNUSABLE;
		return decision;
	}

	polardb_ensure_pool_key(conn);
	decision.match_key = polardb_core_pool_match_key_for_conn(conn);
	decision.status = decision.match_key.empty()
		? PolarDB_ReaderConnectionReturnStatus::EMPTY_KEY
		: PolarDB_ReaderConnectionReturnStatus::RETURNABLE;
	return decision;
}

void PgSQL_PolarDB_ReaderPool::account_connection_return_rejection(
		PolarDB_ReaderConnectionReturnStatus status) {
	switch (status) {
	case PolarDB_ReaderConnectionReturnStatus::OFFLINE:
		POLARDB_STATUS_COUNT_ONE(reader_pool_drop_offline);
		break;
	case PolarDB_ReaderConnectionReturnStatus::CLIENT_IDENTITY:
		POLARDB_STATUS_COUNT_ONE(reader_pool_drop_client_identity);
		break;
	case PolarDB_ReaderConnectionReturnStatus::UNUSABLE:
		POLARDB_STATUS_COUNT_ONE(reader_pool_drop_unusable);
		break;
	default:
		break;
	}
}

/**
 * @brief Return where a finished reader connection goes when a worker releases it.
 *
 * The caller must own conn. No hostgroup manager or pool lock is taken.
 *
 * @param conn                   Connection being released, owned by the caller.
 * @param excluded_worker_index  Worker to ignore when looking for a pending
 *                               capacity request, normally the releasing worker.
 * @return One of:
 *         - REMOVE_CONNECTION: the connection failed the return checks and must be
 *           destroyed rather than pooled.
 *         - KEEP_WITH_WORKER: no other worker is waiting for a connection with
 *           this match key, so keep it in the worker local cache.
 *         - USE_SHARED_POOL_REUSE_CHECKED: another worker is waiting for this match
 *           key, so hand it to the shared pool. The return checks have already
 *           run, so the caller may skip them, but this verdict is valid only for
 *           an immediate return of this same connection with no intervening use.
 *         - USE_SHARED_POOL: hand it to the shared pool without asserting its
 *           state. This is also the fall-through for a connection this pool does
 *           not manage - one with no pool key, or on a server that is not a
 *           configured PolarDB reader hostgroup - in which case no reuse check has
 *           run at all and the caller must still have the connection checked.
 */
PolarDB_ReaderLocalReturnDecision
PgSQL_PolarDB_ReaderPool::local_return_decision(
		PgSQL_Connection* conn, unsigned int excluded_worker_index) {
	PolarDB_ReaderLocalReturnDecision decision;
	if (!conn || !conn->parent || conn->polardb_pool_key.empty()) {
		return decision;
	}
	PgSQL_SrvC* server = static_cast<PgSQL_SrvC*>(conn->parent);
	const auto hostgroup_config = server->myhgc
		? hgm_->get_polardb_hg_config(server->myhgc->hid)
		: PgSQL_HostGroups_Manager::PolarDB_HG_Config{};
	if (!hostgroup_config.is_polardb_hostgroup ||
			hostgroup_config.reader_hostgroup !=
				static_cast<int>(server->myhgc->hid)) {
		return decision;
	}
	const ConnectionReturnDecision return_decision =
		connection_return_decision(conn);
	decision.connection_status = return_decision.status;
	if (!return_decision.returnable()) {
		decision.action = PolarDB_ReaderLocalReturn::REMOVE_CONNECTION;
		return decision;
	}
	if (!server->has_matching_reader_pool_capacity_request(
			return_decision.match_key, excluded_worker_index)) {
		decision.action = PolarDB_ReaderLocalReturn::KEEP_WITH_WORKER;
		return decision;
	}
	decision.action =
		PolarDB_ReaderLocalReturn::USE_SHARED_POOL_REUSE_CHECKED;
	return decision;
}

void polardb_count_reader_pool_reject(
		PgSQL_Thread* thread, PolarDB_ReaderPoolRejectReason reason) {
	switch (reason) {
	case PolarDB_ReaderPoolRejectReason::BAD_CONTEXT:
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_reject_bad_context);
		break;
	case PolarDB_ReaderPoolRejectReason::PROFILE:
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_reject_profile);
		break;
	case PolarDB_ReaderPoolRejectReason::AUTH:
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_reject_auth);
		break;
	case PolarDB_ReaderPoolRejectReason::IDENTITY:
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_reject_identity);
		break;
	case PolarDB_ReaderPoolRejectReason::SESSION_STATE:
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_reject_session_state);
		break;
	case PolarDB_ReaderPoolRejectReason::NONE:
	default:
		break;
	}
}

PolarDB_ReaderPoolRejectReason
polardb_reader_pool_reject_reason_from_reuse_state(
		PolarDB_PoolReuseState state) {
	switch (state) {
	case PolarDB_PoolReuseState::BAD_CONTEXT:
		return PolarDB_ReaderPoolRejectReason::BAD_CONTEXT;
	case PolarDB_PoolReuseState::PROFILE_MISMATCH:
		return PolarDB_ReaderPoolRejectReason::PROFILE;
	case PolarDB_PoolReuseState::AUTH_MISMATCH:
		return PolarDB_ReaderPoolRejectReason::AUTH;
	case PolarDB_PoolReuseState::IDENTITY_MISMATCH:
		return PolarDB_ReaderPoolRejectReason::IDENTITY;
	case PolarDB_PoolReuseState::NEEDS_RESET:
	case PolarDB_PoolReuseState::NEEDS_VARIABLE_UPDATE:
	case PolarDB_PoolReuseState::SESSION_STATE_MISMATCH:
		return PolarDB_ReaderPoolRejectReason::SESSION_STATE;
	case PolarDB_PoolReuseState::EXACT:
	default:
		return PolarDB_ReaderPoolRejectReason::NONE;
	}
}

/**
 * @brief Report whether a pooled connection can serve this request as it stands.
 *
 * Usable is stricter than reusable: only PolarDB_PoolReuseState::EXACT passes, so
 * NEEDS_RESET and NEEDS_VARIABLE_UPDATE - which a caller that pays for a reset or
 * a variable synchronisation can still recover - are reported here as not usable,
 * and the callers respond by removing and deleting the connection.
 *
 * @param conn               Candidate connection, owned by the caller.
 * @param sess               Session asking for the connection.
 * @param pool_request       Request describing the required profile, identity and
 *                           key.
 * @param reject_reason_out  Optional. Set to NONE on success, and to the reason
 *                           the connection was rejected on failure.
 * @return true when the connection can be handed to the session with no reset and
 *         no variable synchronisation.
 */
bool polardb_reader_pool_conn_usable(
		PgSQL_Connection* conn, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		PolarDB_ReaderPoolRejectReason* reject_reason_out) {
	if (reject_reason_out) {
		*reject_reason_out = PolarDB_ReaderPoolRejectReason::NONE;
	}
	const PolarDB_PoolReuseClassification classification =
		polardb_classify_pool_conn_for_reuse(conn, sess, pool_request);
	if (classification.state != PolarDB_PoolReuseState::EXACT) {
		if (reject_reason_out) {
			*reject_reason_out =
				polardb_reader_pool_reject_reason_from_reuse_state(
					classification.state);
		}
		return false;
	}
	return true;
}

/**
 * @brief Report whether this backend may be shared with other clients.
 *
 * This is a cross-client safety boundary, not a performance heuristic. A startup
 * profile that emits startup parameters puts the requesting client's identity
 * into the backend's startup packet, so such a connection may stay in a shared
 * pool only when the identity mode is PROXY and the identity it carries is the
 * proxy's own. Answering true wrongly hands one client's identity-bearing backend
 * to a different client.
 *
 * @param conn  Connection to check. A null connection yields false.
 * @return true when the connection carries no client-derived identity, or the
 *         identity mode is PROXY.
 */
static bool polardb_reader_pool_conn_can_remain_pooled(
		const PgSQL_Connection* conn) {
	if (!conn) {
		return false;
	}
	if (!conn->polardb_startup_profile.emits_startup_params()) {
		return true;
	}
	return conn->polardb_startup_identity_mode ==
		static_cast<int>(PolarDB_ProxyIdentityMode::PROXY);
}


#endif // POLARDB_PROXY
