/**
 * @file PgSQL_PolarDB_ReaderPool_Warmup.cpp
 * @brief Background creation of transaction-split reader connections.
 */

#include "PgSQL_PolarDB_ReaderPool.h"

#include "PgSQL_PolarDB_HGM_Internal.h"
#include "PgSQL_PolarDB_ReaderPool_Internal.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Connection.h"
#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <poll.h>
#include <vector>

extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY && POLARDB_PROFILE
#define POLARDB_PROFILE_STATUS_COUNT(name, value) \
	POLARDB_HGM_PROFILE_STATUS_COUNT(hgm_->status, name, value)
#define POLARDB_PROFILE_STATUS_COUNT_ONE(name) \
	POLARDB_PROFILE_STATUS_COUNT(name, 1)
#else
#define POLARDB_PROFILE_STATUS_COUNT(name, value) do { } while (0)
#define POLARDB_PROFILE_STATUS_COUNT_ONE(name) do { } while (0)
#endif // POLARDB_PROXY && POLARDB_PROFILE

#if POLARDB_PROXY
#define POLARDB_STATUS_COUNT(name, value) \
	POLARDB_HGM_STATUS_COUNT(hgm_->status, name, value)
#define POLARDB_STATUS_COUNT_ONE(name) \
	POLARDB_STATUS_COUNT(name, 1)

PgSQL_PolarDB_ReaderPool::PgSQL_PolarDB_ReaderPool(PgSQL_HostGroups_Manager* hgm)
	: hgm_(hgm) {}

PgSQL_PolarDB_ReaderPool::~PgSQL_PolarDB_ReaderPool() {
	shutdown();
}

void PgSQL_PolarDB_ReaderPool::start() {
	if (split_warmup_thread_) {
		return;
	}
	split_warmup_shutdown_.store(false, std::memory_order_relaxed);
	split_warmup_thread_ =
		new std::thread(&PgSQL_PolarDB_ReaderPool::split_warmup_thread_run,
			this);
}

void PgSQL_PolarDB_ReaderPool::shutdown() {
	split_warmup_shutdown_.store(true, std::memory_order_relaxed);
	split_warmup_cv_.notify_all();
	if (split_warmup_thread_) {
		split_warmup_thread_->join();
		delete split_warmup_thread_;
		split_warmup_thread_ = nullptr;
	}
}

static std::string polardb_split_warmup_key(
		const PgSQL_SplitWarmupRequest& req) {
	// The dedup key must mirror the configured reuse boundary. Password is not
	// part of the boundary: PostgreSQL backends are reused only within the same
	// database auth profile, and the warmup request still carries the password
	// separately when opening the socket.
	std::string key = std::to_string(req.hostgroup_id);
	key.push_back('\x1f');
	key.append(req.username);
	key.push_back('\x1f');
	key.append(req.dbname);
	key.push_back('\x1f');
	key.append(req.startup_client.identity.host);
	key.push_back('\x1f');
	key.append(std::to_string(
		static_cast<int>(req.startup_client.identity.source)));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_client.identity.port));
	key.push_back('\x1f');
	key.append(req.startup_client.frontend_ssl ? "ssl" : "nossl");
	key.push_back('\x1f');
	key.append(req.startup_client.ssl_version);
	key.push_back('\x1f');
	key.append(req.startup_client.ssl_cipher);
	key.push_back('\x1f');
	key.append(req.startup_client.has_proxy_session ? "sid" : "nosid");
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_client.proxy_session_id));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_client.proxy_cancel_key));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_identity_mode));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_config_generation));
	key.push_back('\x1f');
	key.append(std::to_string(req.has_startup_parameters ? 1 : 0));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_options_hash));
	if (req.has_target_server()) {
		key.push_back('\x1f');
		key.append(req.target_address);
		key.push_back('\x1f');
		key.append(std::to_string(req.target_port));
		key.push_back('\x1f');
		key.append(std::to_string(req.target_server_list_generation));
		// Generic warmup expands one request into numbered creation steps. A
		// direct selected-server repair is always one step and is deduplicated
		// only by server, generation, and exact pool identity.
		if (req.target_server_list_generation == 0) {
			key.push_back('\x1f');
			key.append(std::to_string(req.target_required_free_count));
		}
	}
	return key;
}

PgSQL_PoolMatchKey polardb_core_pool_match_key_for_conn(
		const PgSQL_Connection* conn) {
	return conn
		? pgsql_pool_match_key(
			conn->polardb_startup_profile_generation,
			conn->polardb_pool_key)
		: PgSQL_PoolMatchKey{};
}

PgSQL_PoolMatchKey polardb_core_pool_match_key_for_request(
		const PolarDB_PoolRequest& request) {
	if (!request.ready_for_matching()) {
		return {};
	}
	return pgsql_pool_match_key(
		request.startup_profile.generation(request.startup_identity_mode),
		request.key);
}

static bool polardb_split_warmup_server_matches_target(
		PgSQL_SrvC* mysrvc,
		const PgSQL_SplitWarmupRequest& req) {
	if (!req.has_target_server()) {
		return true;
	}
	return mysrvc && mysrvc->address &&
		req.target_port == mysrvc->port &&
		req.target_address == mysrvc->address;
}

static PolarDB_PoolRequest polardb_pool_request_for_warmup_request(
		const PgSQL_SplitWarmupRequest& req,
		const PolarDB_StartupProfile& startup_profile) {
	PolarDB_PoolKey key;
	key.auth_hash =
		polardb_pool_auth_reuse_key_strings(
			req.username.c_str(), req.dbname.c_str());
	key.startup_identity_hash =
		polardb_startup_client_reuse_key(req.startup_client);
	key.startup_options_hash = req.startup_options_hash;
	return PolarDB_PoolRequest(
		key, startup_profile, req.startup_identity_mode,
		PolarDB_PoolProfile::RFQ, /*only_pooled=*/true);
}

/**
 * @brief Count the free pooled connections on a server that match a warmup request.
 *
 * The caller must hold the hostgroup manager write lock; the per-server pool mutex
 * is taken beneath it, which is the only permitted order.
 *
 * @param mysrvc           Server to inspect. A null server yields 0.
 * @param req              Warmup request supplying the auth, identity and session
 *                         option key the pooled connections must match.
 * @param startup_profile  Startup profile of the request's hostgroup.
 * @return Number of matching free connections. 0 means either that the server has
 *         no matching free connection or that this startup profile cannot request
 *         RFQ LSN and XID at all, so 0 must never be read as 'this server is
 *         already warm'.
 */
static unsigned int polardb_split_warmup_server_compatible_free_count(
		PgSQL_SrvC* mysrvc,
		const PgSQL_SplitWarmupRequest& req,
		const PolarDB_StartupProfile& startup_profile) {
	if (!mysrvc) {
		return 0;
	}
	if (!startup_profile.requests_rfq_lsn() ||
			!startup_profile.requests_rfq_xid()) {
		return 0;
	}
	const PolarDB_PoolRequest warmup_request =
		polardb_pool_request_for_warmup_request(req, startup_profile);
	return mysrvc->matching_connection_count(
		polardb_core_pool_match_key_for_request(warmup_request));
}

static unsigned int polardb_split_warmup_available_slots(PgSQL_SrvC* mysrvc) {
	if (!mysrvc || mysrvc->max_connections <= 0 ||
			!mysrvc->ConnectionsUsed || !mysrvc->ConnectionsFree) {
		return 0;
	}
	const PolarDB_PoolConnStats pool_stats = mysrvc->polardb_pool_conn_stats();
	const unsigned int total = pool_stats.total();
	const unsigned int max_connections =
		static_cast<unsigned int>(mysrvc->max_connections);
	return total < max_connections ? max_connections - total : 0;
}

static constexpr const char* polardb_split_warmup_reason_throttle = "throttle";

/**
 * @brief Report why a server cannot take a warmup connection right now.
 *
 * @param mysrvc          Server to evaluate.
 * @param check_throttle  Also consult the per-server connection creation
 *                        throttle. That path requires the hostgroup manager write
 *                        lock; pass false where the lock is not held.
 * @return nullptr when the server is eligible. Otherwise a pointer to a static
 *         string literal naming the reason. Callers may compare the result by
 *         pointer identity - the throttle reason is matched against
 *         polardb_split_warmup_reason_throttle to abandon the whole request - so
 *         every reason must stay a distinct static literal and must never be an
 *         equal but separately allocated string.
 */
static const char* polardb_split_warmup_server_unavailable_reason(
		PgSQL_SrvC* mysrvc,
		bool check_throttle) {
	// Targeted warmup requests are keyed and later resolved by server
	// address:port. A malformed runtime server row without an address cannot
	// be queued safely because the drain side would not be able to find the
	// same target after the off-lock connect window.
	if (!mysrvc) {
		return "null_server";
	}
	if (mysrvc->status != MYSQL_SERVER_STATUS_ONLINE) {
		return "status";
	}
	if (!mysrvc->address) {
		return "address";
	}
	if (mysrvc->weight <= 0) {
		return "weight";
	}
	if (!pgsql_srv_latency_allowed(mysrvc)) {
		return "latency";
	}
	if (mysrvc->max_connections <= 0) {
		return "max_connections";
	}
	if (!mysrvc->ConnectionsUsed) {
		return "used_list";
	}
	if (!mysrvc->ConnectionsFree) {
		return "free_list";
	}
	if (polardb_split_warmup_available_slots(mysrvc) == 0) {
		return "capacity";
	}
	if (check_throttle && pgsql_connection_creation_throttled_unlocked(mysrvc)) {
		return polardb_split_warmup_reason_throttle;
	}
	return nullptr;
}

static void polardb_trace_split_warmup_target_declined(
		const char* phase,
		const PgSQL_SplitWarmupRequest& req,
		PgSQL_SrvC* mysrvc,
		const char* reason) {
	POLARDB_TRACE(
		"PolarDB WARMUP: target declined phase=%s reason=%s "
		"reader_hg=%u server=%s:%u status=%d weight=%ld "
		"latency_us=%u limit_us=%u used=%u free=%u max=%ld "
		"user=%s db=%s startup_identity=%s:%d source=%d\n",
		phase ? phase : "unknown", reason ? reason : "unknown",
		req.hostgroup_id,
		(mysrvc && mysrvc->address) ? mysrvc->address : "",
		mysrvc ? mysrvc->port : 0,
		mysrvc ? static_cast<int>(mysrvc->status) : -1,
		mysrvc ? mysrvc->weight : 0,
		mysrvc ? mysrvc->current_latency_us_value() : 0,
		mysrvc ? pgsql_srv_latency_limit_us(mysrvc) : 0,
		(mysrvc && mysrvc->ConnectionsUsed)
			? mysrvc->ConnectionsUsed->conns_length() : 0,
		(mysrvc && mysrvc->ConnectionsFree)
			? mysrvc->ConnectionsFree->conns_length() : 0,
		mysrvc ? mysrvc->max_connections : 0,
		req.username.c_str(), req.dbname.c_str(),
		req.startup_client.identity.host.c_str(),
		req.startup_client.identity.port,
		static_cast<int>(req.startup_client.identity.source));
}

static constexpr size_t SPLIT_WARMUP_QUEUE_LIMIT = 1024;
static constexpr size_t SPLIT_WARMUP_DRAIN_LIMIT = 16;

static bool polardb_split_warmup_is_enabled() {
	return pgsql_thread___polardb_lazy_warmup_split;
}

/**
 * @brief Copy a client connection's startup variables into a warmup request.
 *
 * The low water mark variables are captured all or nothing: if any one of them is
 * unset on the client connection, the request's has_startup_parameters,
 * startup_options_hash and both vectors are reset to empty and the capture stops,
 * so a partially populated request can never be observed. Dynamic (high water
 * mark) variables are best effort and are skipped one by one when they are out of
 * range or unset.
 *
 * @param req          Request to fill in.
 * @param client_conn  Client connection whose variables are copied. A null
 *                     connection leaves the request untouched rather than
 *                     clearing it.
 */
static void polardb_split_warmup_capture_startup_parameters(
		PgSQL_SplitWarmupRequest& req,
		const PgSQL_Connection* client_conn) {
	if (!client_conn) {
		return;
	}
	req.has_startup_parameters = true;
	req.startup_options_hash = polardb_pool_session_options_reuse_key(
		client_conn);
	req.startup_parameters.assign(PGSQL_NAME_LAST_HIGH_WM, std::string());
	req.startup_parameter_hash.assign(PGSQL_NAME_LAST_HIGH_WM, 0);
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		if (!client_conn->variables[i].value ||
				client_conn->var_hash[i] == 0) {
			req.has_startup_parameters = false;
			req.startup_options_hash = 0;
			req.startup_parameters.clear();
			req.startup_parameter_hash.clear();
			return;
		}
		req.startup_parameters[i] = client_conn->variables[i].value;
		req.startup_parameter_hash[i] = client_conn->var_hash[i];
	}
	for (uint32_t idx : client_conn->dynamic_variables_idx) {
		if (idx <= PGSQL_NAME_LAST_LOW_WM ||
				idx >= PGSQL_NAME_LAST_HIGH_WM ||
				!client_conn->variables[idx].value ||
				client_conn->var_hash[idx] == 0) {
			continue;
		}
		req.startup_parameters[idx] = client_conn->variables[idx].value;
		req.startup_parameter_hash[idx] = client_conn->var_hash[idx];
	}
}

/**
 * @brief Force a warmup backend to carry the captured client startup parameters.
 *
 * @param conn  Backend connection to configure, before its socket is opened. On
 *              success it takes ownership of freshly allocated copies of the
 *              captured values and the strings it held in those slots are
 *              released. Index PGSQL_NAME_LAST_LOW_WM is deliberately left alone,
 *              dynamic slots with no captured hash are skipped, and the dynamic
 *              index list is reordered afterwards.
 * @param req   Warmup request holding the captured parameters.
 * @return true when the parameters were applied and
 *         conn->polardb_forced_startup_parameters is set. false when nothing was
 *         applied, because the request carries no complete capture; the backend
 *         then keeps its own options and will be pooled under a session options
 *         key that cannot match the requesting session, so the warmup does not
 *         serve the request it was raised for.
 */
bool PgSQL_PolarDB_ReaderPool::apply_split_warmup_startup_parameters(
		PgSQL_Connection* conn, const PgSQL_SplitWarmupRequest& req) {
	if (!conn || !req.has_startup_parameters ||
			req.startup_parameters.size() != PGSQL_NAME_LAST_HIGH_WM ||
			req.startup_parameter_hash.size() != PGSQL_NAME_LAST_HIGH_WM) {
		return false;
	}
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		if (req.startup_parameter_hash[i] == 0) {
			return false;
		}
	}

	conn->polardb_forced_startup_parameters = true;
	for (int i = 0; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		if (i == PGSQL_NAME_LAST_LOW_WM ||
				(i > PGSQL_NAME_LAST_LOW_WM &&
				 req.startup_parameter_hash[i] == 0)) {
			continue;
		}
		free(conn->startup_parameters[i]);
		conn->startup_parameters[i] =
			strdup(req.startup_parameters[i].c_str());
		conn->startup_parameters_hash[i] =
			req.startup_parameter_hash[i];
		free(conn->variables[i].value);
		conn->variables[i].value =
			strdup(req.startup_parameters[i].c_str());
		conn->var_hash[i] = req.startup_parameter_hash[i];
	}
	conn->reorder_dynamic_variables_idx();
	return true;
}

/**
 * @brief Queue a background request to warm the split reader pool for one identity.
 *
 * Call with no hostgroup manager lock and without split_warmup_mutex_ held; this
 * takes split_warmup_mutex_ itself. Everything it needs is copied out of
 * client_conn and target_server during the call, so both are borrowed for the
 * duration of the call only.
 *
 * The enqueue is best effort and never blocks: it bumps the matching counter and
 * returns silently when warmup is disabled at runtime, when the startup identity
 * is not valid, when an identical request is already queued or in flight, and
 * when the queue is full.
 *
 * @param reader_hostgroup_id  Reader hostgroup to warm.
 * @param username             Backend user name. Must be non-null and non-empty.
 * @param password             Backend password to use when opening the socket.
 * @param dbname               Backend database name.
 * @param startup_client       Client startup context the warmed backend must
 *                             reproduce. Must be valid for startup.
 * @param client_conn          Client connection to copy the session startup
 *                             variables from. May be null, in which case no
 *                             startup parameters are captured.
 * @param target_server        When non-null, ask for a single connection to this
 *                             one server as a targeted repair, checked against the
 *                             current server list generation so a stale target is
 *                             dropped. When null, the warmup thread selects the
 *                             readers itself.
 */
void PgSQL_PolarDB_ReaderPool::request_split_warmup(
		unsigned int reader_hostgroup_id,
		const char* username,
		const char* password,
		const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn,
		const PgSQL_SrvC* target_server) {
	if (!polardb_split_warmup_is_enabled()) {
		POLARDB_TRACE(
			"PolarDB WARMUP: split lazy warmup disabled; skip request "
			"reader_hg=%u user=%s db=%s\n",
			reader_hostgroup_id, username ? username : "",
			dbname ? dbname : "");
		return;
	}
	if (!hgm_->status.polardb_active.load(std::memory_order_relaxed) ||
			!username || username[0] == '\0' ||
			!startup_client.identity_valid_for_startup(false)) {
		POLARDB_STATUS_COUNT_ONE(split_warmup_bad_request);
		hgm_->status.polardb_split_warmup_failed.fetch_add(1, std::memory_order_relaxed);
		return;
	}

	const auto hg_config =
		hgm_->get_polardb_hg_config(reader_hostgroup_id);
	if (hg_config.is_polardb_hostgroup &&
			!hgm_->polardb_hostgroup_requests_rfq_lsn(
				reader_hostgroup_id,
				pgsql_thread___polardb_proxy_protocol)) {
		POLARDB_STATUS_COUNT_ONE(split_warmup_rfq_unavailable);
		POLARDB_TRACE(
			"PolarDB WARMUP: split lazy warmup skipped; reader_hg=%u "
			"reason=rfq_profile_unavailable user=%s db=%s\n",
			reader_hostgroup_id, username ? username : "",
			dbname ? dbname : "");
		return;
	}

	const unsigned long long now_us = monotonic_time();
	const unsigned int max_connections_per_request =
		polardb_split_warmup_connection_limit(
			pgsql_thread___polardb_split_warmup_max_connections_per_request);
	PgSQL_SplitWarmupRequest base_request{
		reader_hostgroup_id, username, password, dbname,
		startup_client, now_us, max_connections_per_request};
	if (target_server) {
		if (!target_server->address || target_server->address[0] == '\0' ||
				target_server->port == 0) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_bad_request);
			hgm_->status.polardb_split_warmup_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}
		base_request.target_address = target_server->address;
		base_request.target_port = target_server->port;
		base_request.target_required_free_count = 1;
		const auto server_snapshot = hgm_->get_polardb_server_list_snapshot();
		if (!server_snapshot) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_bad_request);
			hgm_->status.polardb_split_warmup_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}
		base_request.target_server_list_generation =
			server_snapshot->generation;
	}
	base_request.startup_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
	base_request.startup_config_generation =
		pgsql_thread___polardb_startup_config_generation;
	polardb_split_warmup_capture_startup_parameters(
		base_request, client_conn);

	bool notify_executor = false;
	bool queued = false;
	{
		std::unique_lock<std::mutex> lk(split_warmup_mutex_);
		const std::string request_key = polardb_split_warmup_key(base_request);
		// Keep the request path cheap: it only removes duplicate requests for
		// one identity. The worker expands the request to specific readers and
		// checks whether each reader already has a matching free connection.
		if (split_warmup_queued_.find(request_key) != split_warmup_queued_.end()) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_queued);
		} else if (split_warmup_inflight_.find(request_key) != split_warmup_inflight_.end()) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_inflight);
			if (base_request.has_target_server()) {
				split_warmup_rerun_pending_.insert(request_key);
			}
		} else if (split_warmup_queue_.size() >= SPLIT_WARMUP_QUEUE_LIMIT) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_queue_full);
			hgm_->status.polardb_split_warmup_failed.fetch_add(
				1, std::memory_order_relaxed);
		} else {
			notify_executor = split_warmup_queue_.empty();
			split_warmup_queued_.insert(request_key);
			split_warmup_queue_.push(std::move(base_request));
			queued = true;
		}
		hgm_->status.polardb_warmup_pending.store(
			split_warmup_queue_.size(), std::memory_order_relaxed);
	}
	if (!queued) {
		return;
	}
	hgm_->status.polardb_split_warmup_requested.fetch_add(
		1, std::memory_order_relaxed);
	POLARDB_TRACE(
		"PolarDB WARMUP: queued split pool request reader_hg=%u "
		"user=%s db=%s identity_mode=%s startup_identity=%s:%d "
		"source=%d max_connections_per_request=%u target=%s:%u\n",
		reader_hostgroup_id, username, dbname ? dbname : "",
		polardb_proxy_identity_mode_name(
			pgsql_thread___polardb_proxy_identity_mode),
		startup_client.identity.host.c_str(), startup_client.identity.port,
		static_cast<int>(startup_client.identity.source),
		max_connections_per_request,
		target_server && target_server->address ? target_server->address : "",
		target_server ? target_server->port : 0);
	if (notify_executor) {
		split_warmup_cv_.notify_one();
	}
}

void PgSQL_PolarDB_ReaderPool::split_warmup_thread_run() {
	set_thread_name("PgHGWarmup", GloVars.set_thread_name);
	while (!split_warmup_shutdown_.load(std::memory_order_relaxed) &&
			__sync_fetch_and_add(&glovars.shutdown, 0) == 0) {
		std::unique_lock<std::mutex> lk(split_warmup_mutex_);
		split_warmup_cv_.wait_for(lk, std::chrono::milliseconds(100), [&]() {
			return split_warmup_shutdown_.load(std::memory_order_relaxed) ||
				!split_warmup_queue_.empty();
		});
		if (split_warmup_shutdown_.load(std::memory_order_relaxed) ||
				__sync_fetch_and_add(&glovars.shutdown, 0) != 0) {
			break;
		}
		lk.unlock();
		hgm_->polardb_refresh_thread_snapshots();
		refresh_thread_variables();
		warm_split_pools();
	}
}

/**
 * @brief Refresh the process-global configuration cache the warmup thread reads.
 *
 * The warmup thread is not a PgSQL_Thread, so it refreshes the cache itself
 * instead of going through PgSQL_Thread::refresh_variables(). Call it from the
 * warmup thread with no lock held: it takes the thread handler write lock, which
 * get_polardb_global_config_unlocked() requires, and rewrites the process-global
 * pgsql_thread___* values that worker threads also read, including freeing and
 * replacing the pgsql_thread___polardb_proxy_identity_host string.
 *
 * Readers of those globals do not take the thread handler lock, so they may read
 * values from either side of a refresh.
 */
void PgSQL_PolarDB_ReaderPool::refresh_thread_variables() {
	if (!GloPTH) {
		return;
	}
	GloPTH->wrlock();
	const PolarDB_ParsedGlobalConfigValue polardb_global_config =
		GloPTH->get_polardb_global_config_unlocked();
	pgsql_thread___throttle_connections_per_sec_to_hostgroup =
		GloPTH->variables.throttle_connections_per_sec_to_hostgroup;
	pgsql_thread___default_max_latency_ms =
		GloPTH->variables.default_max_latency_ms;
	pgsql_thread___connect_retries_on_failure =
		GloPTH->variables.connect_retries_on_failure;
	pgsql_thread___shun_on_failures =
		GloPTH->variables.shun_on_failures;
	pgsql_thread___shun_recovery_time_sec =
		GloPTH->variables.shun_recovery_time_sec;
	pgsql_thread___connect_timeout_server =
		GloPTH->variables.connect_timeout_server;
	pgsql_thread___connect_timeout_server_max =
		GloPTH->variables.connect_timeout_server_max;
	pgsql_thread___polardb_split_warmup_max_connections_per_request =
		GloPTH->variables.polardb_split_warmup_max_connections_per_request;
	pgsql_thread___polardb_lazy_warmup_split =
		GloPTH->variables.polardb_lazy_warmup_split;
	pgsql_thread___polardb_profile_off =
		polardb_profile_from_int(polardb_global_config.profile) ==
			PolarDB_Profile::OFF;
	pgsql_thread___polardb_proxy_protocol = static_cast<int>(
		polardb_global_config.startup.proxy_protocol);
	pgsql_thread___polardb_proxy_identity_mode = static_cast<int>(
		polardb_global_config.startup.identity_mode);
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host = strdup(
		polardb_global_config.startup.configured_identity.host.c_str());
	pgsql_thread___polardb_proxy_identity_port =
		polardb_global_config.startup.configured_identity.port;
	pgsql_thread___polardb_startup_config_generation =
		polardb_global_config.startup.generation;
	GloPTH->wrunlock();
}

/**
 * @brief Expand one warmup request into per-server targeted warmup steps.
 *
 * The caller must already hold the hostgroup manager write lock - that is what the
 * _unlocked suffix means here - and must not hold split_warmup_mutex_.
 *
 * Only the shortfall is planned: the free connections that already match the
 * request are subtracted from the per-request limit, and nothing is emitted once
 * they cover it. Servers are visited round robin from a rotating start index and
 * readers with no matching free connection are served first, so repeated requests
 * do not pile every new socket onto the same replica.
 *
 * @param req                  Request to expand.
 * @param target_requests      Appended to, never cleared. Each appended request
 *                             names one server, carries max_connections_per_request
 *                             forced to 1, and uses target_required_free_count as
 *                             the step number so the reserve side can tell whether
 *                             that step is still needed when it runs.
 * @param found_hostgroup      Optional. Set to true when the request's hostgroup
 *                             exists.
 * @param saw_eligible_target  Optional. Set to true when at least one server in
 *                             that hostgroup passed the availability checks.
 * @param saw_compatible_free  Optional. Set to true when at least one matching
 *                             free connection already exists. The caller uses the
 *                             three flags to tell 'hostgroup missing' from 'no
 *                             eligible reader' from 'already warm' and to pick the
 *                             failure counter accordingly.
 */
void PgSQL_PolarDB_ReaderPool::polardb_collect_split_warmup_targets_unlocked(
		const PgSQL_SplitWarmupRequest& req,
		std::vector<PgSQL_SplitWarmupRequest>& target_requests,
		bool* found_hostgroup,
		bool* saw_eligible_target,
		bool* saw_compatible_free) {
	struct WarmupCandidate {
		PgSQL_SrvC* srv = nullptr;
		unsigned int compatible_free = 0;
		unsigned int capacity_available = 0;
		unsigned int planned = 0;
	};

	if (found_hostgroup) {
		*found_hostgroup = false;
	}
	if (saw_eligible_target) {
		*saw_eligible_target = false;
	}
	if (saw_compatible_free) {
		*saw_compatible_free = false;
	}

	PgSQL_HGC* myhgc = hgm_->MyHGC_lookup(req.hostgroup_id);
	if (!myhgc) {
		return;
	}
	if (found_hostgroup) {
		*found_hostgroup = true;
	}
	const PgSQL_HostGroups_Manager::PolarDB_HG_Policy policy =
		hgm_->get_polardb_hg_policy(req.hostgroup_id);
	const int protocol =
		policy.proxy_protocol >= 0 ?
			policy.proxy_protocol : pgsql_thread___polardb_proxy_protocol;
	const PolarDB_StartupProfile warmup_profile =
		PolarDB_StartupProfile::from_protocol(
			polardb_proxy_protocol_from_int(protocol));
	if (!warmup_profile.requests_rfq_lsn() || !warmup_profile.requests_rfq_xid()) {
		POLARDB_TRACE(
			"PolarDB WARMUP: target discovery skipped reader_hg=%u "
			"reason=rfq_profile_unavailable protocol=%d\n",
			req.hostgroup_id, protocol);
		return;
	}

	std::vector<WarmupCandidate> candidates;
	unsigned int compatible_free_total = 0;
	for (unsigned int i = 0; i < myhgc->mysrvs->cnt(); i++) {
		PgSQL_SrvC* mysrvc = myhgc->mysrvs->idx(i);
		const char* unavailable_reason =
			polardb_split_warmup_server_unavailable_reason(
				mysrvc, /*check_throttle=*/false);
		if (unavailable_reason) {
			polardb_trace_split_warmup_target_declined(
				"expand", req, mysrvc, unavailable_reason);
			continue;
		}
		if (saw_eligible_target) {
			*saw_eligible_target = true;
		}
		const unsigned int compatible_free =
			polardb_split_warmup_server_compatible_free_count(
				mysrvc, req, warmup_profile);
		compatible_free_total += compatible_free;
		if (compatible_free > 0 && saw_compatible_free) {
			*saw_compatible_free = true;
		}

		const unsigned int capacity_available =
			polardb_split_warmup_available_slots(mysrvc);
		if (capacity_available == 0) {
			continue;
		}
		candidates.push_back({mysrvc, compatible_free, capacity_available, 0});
	}

	const unsigned int max_connections_per_request =
		polardb_split_warmup_connection_limit(
			static_cast<int>(req.max_connections_per_request));
	if (compatible_free_total >= max_connections_per_request) {
		if (saw_compatible_free) {
			*saw_compatible_free = true;
		}
		return;
	}

	unsigned int remaining = max_connections_per_request - compatible_free_total;
	const size_t candidate_count = candidates.size();
	const size_t start_candidate =
		candidate_count == 0 ? 0 :
		static_cast<size_t>(
			split_warmup_next_candidate_.fetch_add(
				1, std::memory_order_relaxed) % candidate_count);
	auto candidate_by_order = [&](size_t order) -> WarmupCandidate& {
		return candidates[(start_candidate + order) % candidate_count];
	};
	auto add_target = [&](WarmupCandidate& candidate) {
		if (!candidate.srv || !candidate.srv->address ||
				candidate.planned >= candidate.capacity_available ||
				remaining == 0) {
			return;
		}
		PgSQL_SplitWarmupRequest target_request = req;
		target_request.max_connections_per_request = 1;
		target_request.target_address =
			candidate.srv->address ? candidate.srv->address : "";
		target_request.target_port = candidate.srv->port;
		target_request.target_required_free_count =
			candidate.compatible_free + candidate.planned + 1;
		target_requests.push_back(std::move(target_request));
		candidate.planned++;
		remaining--;
	};

	for (size_t order = 0; order < candidate_count; ++order) {
		if (remaining == 0) {
			break;
		}
		WarmupCandidate& candidate = candidate_by_order(order);
		if (candidate.compatible_free == 0) {
			add_target(candidate);
		}
	}
	while (remaining > 0) {
		bool added = false;
		for (size_t order = 0; order < candidate_count; ++order) {
			if (remaining == 0) {
				break;
			}
			WarmupCandidate& candidate = candidate_by_order(order);
			const unsigned int expected_free =
				candidate.compatible_free + candidate.planned;
			if (expected_free >= max_connections_per_request) {
				continue;
			}
			const size_t before = target_requests.size();
			add_target(candidate);
			if (target_requests.size() != before) {
				added = true;
			}
		}
		if (!added) {
			break;
		}
	}
}

static int polardb_split_warmup_connect_timeout_ms() {
	const int configured_timeout_ms =
		pgsql_thread___connect_timeout_server_max > 0
			? pgsql_thread___connect_timeout_server_max
			: pgsql_thread___connect_timeout_server;
	return configured_timeout_ms > 0 ? configured_timeout_ms : 10000;
}

/**
 * @brief One warmup connection attempt in a batch.
 *
 * The item owns conn. The connection is already linked into the target server's
 * ConnectionsUsed list and stays parked there for the whole off-lock connect
 * window, so the server keeps accounting for it while it is being established.
 * target_address and target_port are copies rather than a PgSQL_SrvC pointer,
 * because the server the connection was reserved on may be gone by the time the
 * connect finishes and has to be resolved again by address and port.
 *
 * The flags form one tri-state. complete false means the connect is still in
 * flight and the poll loop owns the item. complete true with connected false
 * means the attempt failed and an error is set on conn. complete true with
 * connected true means conn is a finished idle backend ready to be pooled.
 * stopped_by_shutdown marks an attempt abandoned because shutdown was requested,
 * which must not be accounted as a connect failure.
 */
struct PgSQL_SplitWarmupConnectAttempt {
	PgSQL_SplitWarmupRequest req;
	std::string request_key;
	PgSQL_Connection* conn = nullptr;
	std::string target_address;
	uint16_t target_port = 0;
	unsigned long long connect_start_us = 0;
	unsigned long long deadline_us = 0;
	bool connected = false;
	bool complete = false;
	bool stopped_by_shutdown = false;
};

static short polardb_split_warmup_connect_events(PgSQL_Connection* conn) {
	short events = 0;
	if (!conn) {
		return events;
	}
	if (conn->async_exit_status & PG_EVENT_READ) {
		events |= POLLIN;
	}
	if (conn->async_exit_status & PG_EVENT_WRITE) {
		events |= POLLOUT;
	}
	return events;
}

/**
 * @brief Promote a completed libpq handle into a poolable idle backend.
 *
 * Call with no hostgroup manager lock held.
 *
 * @param conn  Connection whose connect has finished. A null connection yields
 *              false.
 * @return true when the connection is fully initialised as an idle reusable
 *         backend - async state, socket, creation time, startup parameters read
 *         back from the server and PolarDB tracking - and the global
 *         server_connections_connected and per-server connect_OK counters have
 *         been bumped. false when the handle did not reach CONNECTION_OK: an
 *         error is then set on the connection and the caller must destroy it.
 */
static bool polardb_finish_split_warmup_connection(PgSQL_Connection* conn) {
	if (!conn) {
		return false;
	}
	if (!conn->is_connected()) {
		conn->set_error(
			PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
			"PolarDB split warmup connection did not reach CONNECTION_OK",
			true);
		return false;
	}

	conn->async_state_machine = ASYNC_IDLE;
	conn->fd = conn->get_pg_socket_fd();
	conn->creation_time = monotonic_time();
	conn->last_time_used = 0;
	conn->reusable = true;
	conn->init_startup_parameters_from_server();
	conn->polardb_enable_requested_rfq_parsing();
	__sync_fetch_and_add(&PgHGM->status.server_connections_connected, 1);
	__sync_fetch_and_add(&conn->parent->connect_OK, 1);
	return true;
}

/**
 * @brief Start the connect for one warmup item.
 *
 * @param item  Item to start. On return item.complete is true only when the
 *              connect finished or failed synchronously; while it is still false
 *              the socket is in progress and ownership of the item passes to the
 *              caller's poll loop, which must drive it to completion.
 * @param hgm   Hostgroup manager used to count the attempt. May be null, in which
 *              case the counter is skipped.
 */
static void polardb_start_split_warmup_connection(
		PgSQL_SplitWarmupConnectAttempt& item,
		PgSQL_HostGroups_Manager* hgm) {
	PgSQL_Connection* conn = item.conn;
	if (!conn) {
		item.complete = true;
		item.connected = false;
		return;
	}

	if (hgm) {
		hgm->status.polardb_split_warmup_target_attempts.fetch_add(
			1, std::memory_order_relaxed);
	}
	conn->connect_start();
	if (conn->is_error_present() || conn->get_pg_connection() == nullptr) {
		item.complete = true;
		item.connected = false;
		return;
	}

	if (conn->async_exit_status == PG_EVENT_NONE) {
		item.connected = polardb_finish_split_warmup_connection(conn);
		item.complete = true;
	}
}

static bool polardb_split_warmup_is_stopping(
		const std::atomic<bool>* shutdown_flag) {
	return (shutdown_flag &&
			shutdown_flag->load(std::memory_order_relaxed)) ||
		__sync_fetch_and_add(&glovars.shutdown, 0) != 0;
}

static void polardb_fail_pending_split_warmup_connections(
		std::vector<PgSQL_SplitWarmupConnectAttempt>& pending,
		const char* message,
		bool stopped_by_shutdown) {
	for (PgSQL_SplitWarmupConnectAttempt& item : pending) {
		if (item.complete || !item.conn) {
			continue;
		}
		item.conn->set_error(
			PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
			message,
			true);
		item.complete = true;
		item.connected = false;
		item.stopped_by_shutdown = stopped_by_shutdown;
	}
}

/**
 * @brief Drive a batch of warmup connects to completion.
 *
 * The caller must hold no hostgroup manager or pool lock: this blocks in poll()
 * for up to the configured maximum server connect timeout, while every pending
 * connection is still linked into its server's ConnectionsUsed list. Connecting
 * the batch in one poll loop is what keeps a slow replica from serialising the
 * rest of the batch behind it.
 *
 * @param pending        Items to connect. On return every item has complete set,
 *                       and is either connected or has an error set on its
 *                       connection.
 * @param hgm            Hostgroup manager used to count attempts. May be null.
 * @param shutdown_flag  Optional. When it is set, or a global shutdown is
 *                       requested, the items still in flight are failed with
 *                       stopped_by_shutdown set, so the caller can tell shutdown
 *                       abandonment from a real connect failure and neither count
 *                       it as one nor shun the replica.
 */
static void polardb_connect_split_warmup_batch(
		std::vector<PgSQL_SplitWarmupConnectAttempt>& pending,
		PgSQL_HostGroups_Manager* hgm,
		const std::atomic<bool>* shutdown_flag) {
	const int timeout_ms = polardb_split_warmup_connect_timeout_ms();
	const unsigned long long now_us = monotonic_time();
	for (PgSQL_SplitWarmupConnectAttempt& item : pending) {
		item.connect_start_us = now_us;
		item.deadline_us = now_us + (unsigned long long)timeout_ms * 1000ULL;
	}
	if (polardb_split_warmup_is_stopping(shutdown_flag)) {
		polardb_fail_pending_split_warmup_connections(
			pending, "PolarDB split warmup interrupted by shutdown",
			true);
		return;
	}
	for (PgSQL_SplitWarmupConnectAttempt& item : pending) {
		if (polardb_split_warmup_is_stopping(shutdown_flag)) {
			polardb_fail_pending_split_warmup_connections(
				pending, "PolarDB split warmup interrupted by shutdown",
				true);
			return;
		}
		polardb_start_split_warmup_connection(item, hgm);
	}

	while (true) {
		if (polardb_split_warmup_is_stopping(shutdown_flag)) {
			polardb_fail_pending_split_warmup_connections(
				pending, "PolarDB split warmup interrupted by shutdown",
				true);
			break;
		}

		std::vector<struct pollfd> fds;
		std::vector<size_t> indexes;
		unsigned long long nearest_deadline_us = 0;
		unsigned int active = 0;

		const unsigned long long loop_now_us = monotonic_time();
		for (size_t i = 0; i < pending.size(); i++) {
			PgSQL_SplitWarmupConnectAttempt& item = pending[i];
			if (item.complete) {
				continue;
			}
			active++;
			PgSQL_Connection* conn = item.conn;
			if (loop_now_us >= item.deadline_us) {
				conn->set_error(
					PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
					"PolarDB split warmup connection timed out",
					true);
				item.complete = true;
				item.connected = false;
				continue;
			}

			const short events = polardb_split_warmup_connect_events(conn);
			const int fd = conn ? conn->fd : -1;
			if (fd < 0 || events == 0) {
				if (conn) {
					conn->set_error(
						PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
						"PolarDB split warmup connection has no pollable backend socket",
						true);
				}
				item.complete = true;
				item.connected = false;
				continue;
			}

			struct pollfd pfd;
			pfd.fd = fd;
			pfd.events = events;
			pfd.revents = 0;
			fds.push_back(pfd);
			indexes.push_back(i);
			if (nearest_deadline_us == 0 ||
					item.deadline_us < nearest_deadline_us) {
				nearest_deadline_us = item.deadline_us;
			}
		}

		if (active == 0 || fds.empty()) {
			break;
		}

		const unsigned long long poll_now_us = monotonic_time();
		const int poll_timeout_ms =
			nearest_deadline_us > poll_now_us
				? (int)((nearest_deadline_us - poll_now_us + 999ULL) / 1000ULL)
				: 0;
		const int bounded_poll_timeout_ms =
			poll_timeout_ms > 100 ? 100 : poll_timeout_ms;

		int rc = poll(fds.data(), fds.size(), bounded_poll_timeout_ms);
		if (rc < 0) {
			if (errno == EINTR) {
				continue;
			}
			for (PgSQL_SplitWarmupConnectAttempt& item : pending) {
				if (item.complete || !item.conn) {
					continue;
				}
				item.conn->set_error(
					PGSQL_ERROR_CODES::ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
					strerror(errno),
					true);
				item.complete = true;
				item.connected = false;
				item.stopped_by_shutdown = false;
			}
			break;
		}
		if (rc == 0) {
			continue;
		}

		for (size_t i = 0; i < fds.size(); i++) {
			if (fds[i].revents == 0) {
				continue;
			}
			PgSQL_SplitWarmupConnectAttempt& item = pending[indexes[i]];
			PgSQL_Connection* conn = item.conn;
			conn->connect_cont(fds[i].revents);
			if (conn->is_error_present()) {
				item.complete = true;
				item.connected = false;
				continue;
			}
			if (conn->async_exit_status == PG_EVENT_NONE) {
				item.connected = polardb_finish_split_warmup_connection(conn);
				item.complete = true;
			}
		}
	}
}

/**
 * @brief Take a batch of queued warmup requests off the queue.
 *
 * Call with no hostgroup manager lock and without split_warmup_mutex_ held; this
 * takes split_warmup_mutex_.
 *
 * When warmup has been disabled at runtime, this instead discards the whole queue
 * along with the queued, in-flight and rerun bookkeeping and returns false, so a
 * disabled warmup leaves nothing behind to be picked up later.
 *
 * @param requests  Appended with at most SPLIT_WARMUP_DRAIN_LIMIT requests. The
 *                  vector is never cleared.
 * @return true when requests is non-empty on return, false when nothing was
 *         drained. Drained requests have had their queued key erased but are not
 *         registered as in flight; the caller must register each one with
 *         register_split_warmup_inflight_key() before acting on it.
 */
bool PgSQL_PolarDB_ReaderPool::drain_split_warmup_requests(
		std::vector<PgSQL_SplitWarmupRequest>& requests) {
	std::lock_guard<std::mutex> warmup_lock(split_warmup_mutex_);
	if (!polardb_split_warmup_is_enabled()) {
		const size_t dropped = split_warmup_queue_.size();
		while (!split_warmup_queue_.empty()) {
			split_warmup_queue_.pop();
		}
		split_warmup_queued_.clear();
		split_warmup_inflight_.clear();
		split_warmup_rerun_pending_.clear();
		hgm_->status.polardb_warmup_pending.store(0, std::memory_order_relaxed);
		if (dropped > 0) {
			POLARDB_TRACE(
				"PolarDB WARMUP: split lazy warmup disabled; "
				"dropped %zu queued requests\n",
				dropped);
		}
		return false;
	}

	size_t drained = 0;
	while (!split_warmup_queue_.empty() &&
			drained < SPLIT_WARMUP_DRAIN_LIMIT) {
		PgSQL_SplitWarmupRequest request =
			std::move(split_warmup_queue_.front());
		split_warmup_queue_.pop();
		split_warmup_queued_.erase(polardb_split_warmup_key(request));
		requests.push_back(std::move(request));
		++drained;
	}
	hgm_->status.polardb_warmup_pending.store(
		split_warmup_queue_.size(), std::memory_order_relaxed);
	if (!split_warmup_queue_.empty()) {
		POLARDB_TRACE(
			"PolarDB WARMUP: deferred %zu queued split warmup requests "
			"after draining %zu this pass\n",
			split_warmup_queue_.size(), drained);
	}
	return !requests.empty();
}

/**
 * @brief Release a warmup key registered with register_split_warmup_inflight_key().
 *
 * Call with no hostgroup manager lock held: this takes split_warmup_mutex_, and
 * split_warmup_mutex_ is only ever taken outside the hostgroup manager lock,
 * never under it.
 *
 * @param key            Key to release.
 * @param rerun_request  Optional. When it is given and a duplicate request for
 *                       this key arrived while the key was in flight, the request
 *                       is queued again so a targeted repair that raced with the
 *                       in-flight attempt is not lost. Pass null to let such a
 *                       duplicate be dropped.
 */
void PgSQL_PolarDB_ReaderPool::release_and_maybe_requeue_split_warmup_key(
		const std::string& key,
		const PgSQL_SplitWarmupRequest* rerun_request) {
	bool queued = false;
	bool queue_full = false;
	{
		std::unique_lock<std::mutex> lk(split_warmup_mutex_);
		split_warmup_inflight_.erase(key);
		const bool rerun_pending =
			split_warmup_rerun_pending_.erase(key) != 0;
		if (rerun_request && rerun_pending &&
				split_warmup_queued_.find(key) == split_warmup_queued_.end() &&
				polardb_split_warmup_is_enabled()) {
			if (split_warmup_queue_.size() >= SPLIT_WARMUP_QUEUE_LIMIT) {
				queue_full = true;
			} else {
				PgSQL_SplitWarmupRequest rerun = *rerun_request;
				rerun.requested_at_us = monotonic_time();
				split_warmup_queued_.insert(key);
				split_warmup_queue_.push(std::move(rerun));
				queued = true;
			}
		}
		hgm_->status.polardb_warmup_pending.store(
			split_warmup_queue_.size(), std::memory_order_relaxed);
	}
	if (queue_full) {
		POLARDB_STATUS_COUNT_ONE(split_warmup_queue_full);
		hgm_->status.polardb_split_warmup_target_failed.fetch_add(
			1, std::memory_order_relaxed);
	}
	if (queued) {
		hgm_->status.polardb_split_warmup_requested.fetch_add(
			1, std::memory_order_relaxed);
		split_warmup_cv_.notify_one();
	}
}

/**
 * @brief Register a warmup key so that no duplicate request runs concurrently.
 *
 * Call with no hostgroup manager lock held; this takes split_warmup_mutex_. It is
 * paired with
 * release_and_maybe_requeue_split_warmup_key().
 *
 * @param key  Key to register.
 * @return true when the caller now owns the key and must release it with
 *         release_and_maybe_requeue_split_warmup_key() exactly once on every exit path,
 *         including the error paths. A key that is never released stays registered
 *         for good and every later request for that identity is deduplicated
 *         away, so that pool can never warm again. false when another
 *         registration for the key is already in flight: the caller must do
 *         nothing, and in particular must not release a key it did not register.
 */
bool PgSQL_PolarDB_ReaderPool::register_split_warmup_inflight_key(
		const std::string& key) {
	std::unique_lock<std::mutex> lk(split_warmup_mutex_);
	if (!split_warmup_inflight_.insert(key).second) {
		POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_inflight);
		return false;
	}
	return true;
}

/**
 * @brief Put a warmup request back on the queue.
 *
 * Call with no hostgroup manager lock held; this takes split_warmup_mutex_.
 *
 * @param req  Request to queue again. It is copied.
 * @return true when the request was queued and the warmup thread was notified.
 *         false means the request was dropped, not deferred: an identical request
 *         is already queued, an identical request is in flight (for a targeted
 *         request a rerun is recorded so the in-flight owner can requeue it), or
 *         the queue is full. The caller gets no further attempt out of this call.
 */
bool PgSQL_PolarDB_ReaderPool::requeue_split_warmup_request(
		const PgSQL_SplitWarmupRequest& req) {
	bool queued = false;
	{
		std::unique_lock<std::mutex> lk(split_warmup_mutex_);
		const std::string req_key = polardb_split_warmup_key(req);
		if (split_warmup_queued_.find(req_key) != split_warmup_queued_.end()) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_queued);
		} else if (split_warmup_inflight_.find(req_key) != split_warmup_inflight_.end()) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_inflight);
			if (req.has_target_server()) {
				split_warmup_rerun_pending_.insert(req_key);
			}
		} else if (split_warmup_queue_.size() >= SPLIT_WARMUP_QUEUE_LIMIT) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_queue_full);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
		} else {
			split_warmup_queued_.insert(req_key);
			split_warmup_queue_.push(req);
			queued = true;
		}
		hgm_->status.polardb_warmup_pending.store(
			split_warmup_queue_.size(), std::memory_order_relaxed);
	}
	if (queued) {
		split_warmup_cv_.notify_one();
	}
	return queued;
}

/**
 * @brief Run one warmup pass over the queued requests.
 *
 * Runs on the warmup thread only, and must be called with no lock held.
 *
 * The pass drains a batch of requests, expands each generic request into targeted
 * per-server steps, and reserves one backend per planned socket while holding the
 * hostgroup manager write lock. It then drops that lock, connects the whole batch
 * off-lock, and retakes the write lock once per completed item to pool or discard
 * it. Connecting never happens under the write lock because it blocks for as long
 * as the server connect timeout, which would stall every worker thread.
 *
 * A reserved connection stays parked in its target server's ConnectionsUsed list
 * across the off-lock window, so the server keeps accounting for the socket it is
 * about to hold. Because the configuration can change while the lock is dropped,
 * the target server is resolved again by address and port once the connect
 * finishes; a connection whose server is gone, replaced, offline or out of
 * capacity is destroyed rather than pooled.
 *
 * Every in-flight key registered during the pass is released before returning, on
 * every path.
 */
void PgSQL_PolarDB_ReaderPool::warm_split_pools() {
	std::vector<PgSQL_SplitWarmupRequest> requests;
	if (!drain_split_warmup_requests(requests)) {
		return;
	}

	std::vector<PgSQL_SplitWarmupConnectAttempt> pending;
	std::vector<std::string> base_inflight_keys;
	pending.reserve(PGSQL_POLARDB_SPLIT_WARMUP_CONNECT_BATCH_LIMIT);

	auto reserve_target_request = [&](const PgSQL_SplitWarmupRequest& req) {
		if (!pgsql_split_warmup_batch_has_capacity(pending.size())) {
			requeue_split_warmup_request(req);
			return;
		}
#if POLARDB_PROFILE
		const unsigned long long drain_start_us = monotonic_time();
		if (req.requested_at_us > 0) {
			POLARDB_PROFILE_STATUS_COUNT(split_warmup_queue_delay_sum_us,
				drain_start_us >= req.requested_at_us
					? drain_start_us - req.requested_at_us : 0);
			POLARDB_PROFILE_STATUS_COUNT_ONE(split_warmup_queue_delay_count);
		}
#endif // POLARDB_PROFILE
		if (req.username.empty() ||
				!req.startup_client.identity_valid_for_startup(false)) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_bad_request);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}

		const std::string req_key = polardb_split_warmup_key(req);
		if (!register_split_warmup_inflight_key(req_key)) {
			return;
		}

		PgSQL_Connection* conn = nullptr;
		PgSQL_SrvC* target = nullptr;
		bool skip_request = false;
		const PolarDB_StartupProfile warmup_profile =
			hgm_->polardb_startup_profile_for_hostgroup(
				req.hostgroup_id, pgsql_thread___polardb_proxy_protocol);

		hgm_->wrlock();
		PgSQL_HGC* myhgc = hgm_->MyHGC_lookup(req.hostgroup_id);
		if (!myhgc) {
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(req_key, &req);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}

		for (unsigned int i = 0; i < myhgc->mysrvs->cnt(); i++) {
			PgSQL_SrvC* mysrvc = myhgc->mysrvs->idx(i);
			if (!polardb_split_warmup_server_matches_target(mysrvc, req)) {
				if (req.has_target_server()) {
					polardb_trace_split_warmup_target_declined(
						"reserve", req, mysrvc, "target_mismatch");
				}
				continue;
			}
			const unsigned int compatible_free =
				polardb_split_warmup_server_compatible_free_count(
					mysrvc, req, warmup_profile);
			if (compatible_free > 0) {
				if (req.has_target_server() &&
						compatible_free >= req.target_required_free_count) {
					skip_request = true;
					break;
				}
				if (!req.has_target_server()) {
					continue;
				}
			}
			const char* unavailable_reason =
				polardb_split_warmup_server_unavailable_reason(
					mysrvc, /*check_throttle=*/true);
			if (unavailable_reason) {
				polardb_trace_split_warmup_target_declined(
					"reserve", req, mysrvc, unavailable_reason);
				if (unavailable_reason == polardb_split_warmup_reason_throttle) {
					break;
				}
				continue;
			}
			target = mysrvc;
			break;
		}

		if (skip_request) {
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(req_key, &req);
			POLARDB_STATUS_COUNT_ONE(split_warmup_already_warm);
			return;
		}

		if (!target) {
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(req_key, &req);
			POLARDB_STATUS_COUNT_ONE(split_warmup_no_target);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}

		conn = pgsql_create_backend_connection_unlocked(target);
		if (!conn || !conn->userinfo) {
			delete conn;
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(req_key, &req);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}
		conn->userinfo->set(
			const_cast<char*>(req.username.c_str()),
			const_cast<char*>(req.password.c_str()),
			const_cast<char*>(req.dbname.c_str()),
			nullptr);
#if POLARDB_PROXY
		conn->set_polardb_startup_settings(
			warmup_profile, req.startup_identity_mode,
			req.startup_config_generation != 0
				? req.startup_config_generation
				: pgsql_thread___polardb_startup_config_generation,
			req.startup_client);
		// Split warmup is opened outside client dispatch, but it is still owned by
		// the startup identity selected for the requesting session. Force that
		// identity through the normal conninfo builder.
		apply_split_warmup_startup_parameters(conn, req);
#endif // POLARDB_PROXY
		PgSQL_SplitWarmupConnectAttempt item;
		item.req = req;
		item.request_key = req_key;
		item.conn = conn;
		item.target_address = target->address ? target->address : "";
		item.target_port = target->port;
		conn->polardb_selected_server_snapshot =
			hgm_->get_polardb_server_list_snapshot();
		target->ConnectionsUsed->add(conn);
		target->update_max_connections_used();
		POLARDB_TRACE(
			"PolarDB WARMUP: reserved split pool connection "
			"reader_hg=%u server=%s:%u user=%s db=%s startup_client=%s:%d\n",
			req.hostgroup_id, target->address, target->port,
			req.username.c_str(), req.dbname.c_str(),
			req.startup_client.identity.host.c_str(),
			req.startup_client.identity.port);
		hgm_->wrunlock();
		pending.push_back(std::move(item));
	};

	for (const PgSQL_SplitWarmupRequest& req : requests) {
		if (req.startup_config_generation != 0 &&
				req.startup_config_generation !=
					pgsql_thread___polardb_startup_config_generation) {
			POLARDB_STATUS_COUNT_ONE(reader_pool_retry_after_config_change);
			continue;
		}
		const std::string req_key = polardb_split_warmup_key(req);
		if (req.has_target_server()) {
			if (req.target_server_list_generation != 0) {
				const auto server_snapshot =
					hgm_->get_polardb_server_list_snapshot();
				if (!server_snapshot ||
						server_snapshot->generation !=
							req.target_server_list_generation) {
					POLARDB_STATUS_COUNT_ONE(reader_pool_retry_after_config_change);
					continue;
				}
			}
			reserve_target_request(req);
			continue;
		}

		if (!register_split_warmup_inflight_key(req_key)) {
			continue;
		}
		base_inflight_keys.push_back(req_key);

		std::vector<PgSQL_SplitWarmupRequest> target_requests;
		bool found_hostgroup = false;
		bool saw_eligible_target = false;
		bool saw_compatible_free = false;

		hgm_->wrlock();
		polardb_collect_split_warmup_targets_unlocked(
			req, target_requests, &found_hostgroup,
			&saw_eligible_target, &saw_compatible_free);
		hgm_->wrunlock();

		if (!found_hostgroup) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_no_target);
			hgm_->status.polardb_split_warmup_failed.fetch_add(
				1, std::memory_order_relaxed);
			release_and_maybe_requeue_split_warmup_key(req_key);
			base_inflight_keys.pop_back();
			continue;
		}
		if (target_requests.empty()) {
			if (saw_compatible_free) {
				POLARDB_STATUS_COUNT_ONE(split_warmup_already_warm);
			} else if (!saw_eligible_target) {
				POLARDB_STATUS_COUNT_ONE(split_warmup_no_target);
				hgm_->status.polardb_split_warmup_failed.fetch_add(
					1, std::memory_order_relaxed);
			}
			POLARDB_TRACE(
				"PolarDB WARMUP: split pool already has compatible reader or no "
				"eligible target reader_hg=%u user=%s db=%s startup_identity=%s:%d source=%d\n",
				req.hostgroup_id, req.username.c_str(), req.dbname.c_str(),
				req.startup_client.identity.host.c_str(),
				req.startup_client.identity.port,
				static_cast<int>(req.startup_client.identity.source));
			release_and_maybe_requeue_split_warmup_key(req_key);
			base_inflight_keys.pop_back();
			continue;
		}

		for (const PgSQL_SplitWarmupRequest& target_request : target_requests) {
			reserve_target_request(target_request);
		}
	}

	if (!pending.empty()) {
		polardb_connect_split_warmup_batch(pending, hgm_, &split_warmup_shutdown_);
	}

	for (PgSQL_SplitWarmupConnectAttempt& item : pending) {
		PgSQL_Connection* conn = item.conn;
		std::shared_ptr<const void> selected_server_snapshot;
		PgSQL_SrvC* target = nullptr;
#if POLARDB_PROFILE
		const unsigned long long connect_end_us = monotonic_time();
		POLARDB_PROFILE_STATUS_COUNT(split_warmup_connect_sum_us,
			connect_end_us >= item.connect_start_us
				? connect_end_us - item.connect_start_us : 0);
		POLARDB_PROFILE_STATUS_COUNT_ONE(split_warmup_connect_count);
		const unsigned long long add_start_us = monotonic_time();
#endif // POLARDB_PROFILE
		hgm_->wrlock();
		target = static_cast<PgSQL_SrvC*>(conn->parent);
		if (target && target->ConnectionsUsed) {
			target->ConnectionsUsed->remove(conn);
		}

		if (!item.connected) {
			if (pgsql_split_warmup_should_count_connect_failure(
					item.connected, item.stopped_by_shutdown)) {
				proxy_error("PolarDB split warmup: connection failed to %s:%u "
					"for HG %u user=%s db=%s: %s\n",
					item.target_address.c_str(), item.target_port,
					item.req.hostgroup_id, item.req.username.c_str(),
					item.req.dbname.c_str(), conn->get_error_message().c_str());
				POLARDB_STATUS_COUNT_ONE(split_warmup_connect_failed);
				if (target) {
					hgm_->p_update_pgsql_error_counter(
						p_pgsql_error_type::pgsql, target->myhgc->hid,
						target->address, target->port,
						POLARDB_REPLICA_FAILURE_ERROR_CODE);
					target->connect_error(POLARDB_REPLICA_FAILURE_ERROR_CODE);
				}
				hgm_->status.polardb_split_warmup_target_failed.fetch_add(
					1, std::memory_order_relaxed);
			} else {
				POLARDB_TRACE(
					"PolarDB WARMUP: split connect stopped during shutdown "
					"target=%s:%u hg=%u\n",
					item.target_address.c_str(), item.target_port,
					item.req.hostgroup_id);
			}
			delete conn;
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
			continue;
		}

		if (!hgm_->polardb_reader_connection_is_current(
				conn, item.req.hostgroup_id)) {
			POLARDB_TRACE(
				"PolarDB WARMUP: discarding connected backend because "
				"startup or server state changed reader_hg=%u server=%s:%u\n",
				item.req.hostgroup_id, item.target_address.c_str(),
				item.target_port);
			POLARDB_STATUS_COUNT_ONE(reader_pool_retry_after_config_change);
			delete conn;
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
			continue;
		}
		selected_server_snapshot =
			std::move(conn->polardb_selected_server_snapshot);

		PgSQL_SrvC* reserved_parent = target;
		PgSQL_SrvC* resolved_target = nullptr;
		PgSQL_HGC* current_hgc = hgm_->MyHGC_lookup(item.req.hostgroup_id);
		if (current_hgc) {
			for (unsigned int i = 0; i < current_hgc->mysrvs->cnt(); i++) {
				PgSQL_SrvC* mysrvc = current_hgc->mysrvs->idx(i);
				if (mysrvc &&
						mysrvc->port == item.target_port &&
						mysrvc->address &&
						item.target_address == mysrvc->address) {
					resolved_target = mysrvc;
					break;
				}
			}
		}
		target = resolved_target;

		if (!target || target != reserved_parent ||
				target->status != MYSQL_SERVER_STATUS_ONLINE ||
				!target->ConnectionsFree || target->max_connections <= 0) {
			proxy_warning(
				"PolarDB split warmup: discarding connected backend for HG %u "
				"user=%s db=%s because target server changed while connecting\n",
				item.req.hostgroup_id, item.req.username.c_str(),
				item.req.dbname.c_str());
			POLARDB_STATUS_COUNT_ONE(split_warmup_add_failed);
			delete conn;
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			continue;
		}

		if (!target->polardb_pool_can_open_socket()) {
			proxy_warning(
				"PolarDB split warmup: discarding connected backend for HG %u "
				"server=%s:%u user=%s db=%s because capacity changed while connecting\n",
				item.req.hostgroup_id, target->address, target->port,
				item.req.username.c_str(), item.req.dbname.c_str());
			POLARDB_STATUS_COUNT_ONE(split_warmup_add_failed);
			delete conn;
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			continue;
		}

		polardb_ensure_pool_key(conn);
		const PgSQL_PoolMatchKey match_key =
			polardb_core_pool_match_key_for_conn(conn);
		conn->polardb_selected_server_snapshot =
			std::move(selected_server_snapshot);
		assert(conn->polardb_selected_server_snapshot);
		PolarDB_ReaderPoolReservationWake reservation_wake;
		if (!target->add_matching_connection(conn, match_key, &reservation_wake)) {
			delete conn;
			POLARDB_STATUS_COUNT_ONE(split_warmup_add_failed);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			hgm_->wrunlock();
			release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
			continue;
		}
#if POLARDB_PROFILE
		const unsigned long long add_end_us = monotonic_time();
		POLARDB_PROFILE_STATUS_COUNT(split_warmup_add_sum_us,
			add_end_us >= add_start_us
				? add_end_us - add_start_us : 0);
		POLARDB_PROFILE_STATUS_COUNT_ONE(split_warmup_add_count);
#endif // POLARDB_PROFILE
		hgm_->status.polardb_split_warmup_created.fetch_add(1, std::memory_order_relaxed);
		if (reservation_wake.has_reservation()) {
			hgm_->status.polardb_split_warmup_connection_reserved.fetch_add(
				1, std::memory_order_relaxed);
		}
		const unsigned long long elapsed_us =
			item.req.requested_at_us > 0 ? monotonic_time() - item.req.requested_at_us : 0;
		hgm_->status.polardb_split_warmup_sum_us.fetch_add(
			elapsed_us, std::memory_order_relaxed);
		hgm_->status.polardb_split_warmup_count.fetch_add(
			1, std::memory_order_relaxed);
		POLARDB_TRACE(
			"PolarDB WARMUP: added connected split pool connection "
			"reader_hg=%u server=%s:%u user=%s db=%s startup_client=%s:%d "
			"elapsed_us=%llu\n",
			item.req.hostgroup_id, target->address, target->port,
			item.req.username.c_str(), item.req.dbname.c_str(),
			item.req.startup_client.identity.host.c_str(),
			item.req.startup_client.identity.port, elapsed_us);
		hgm_->wrunlock();
		hgm_->polardb_route_reader_reservation_wake(reservation_wake);
		release_and_maybe_requeue_split_warmup_key(item.request_key, &item.req);
	}

	for (const std::string& key : base_inflight_keys) {
		release_and_maybe_requeue_split_warmup_key(key);
	}
}


#endif // POLARDB_PROXY
