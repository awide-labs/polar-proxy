#include "PgSQL_PolarDB_ReaderPool.h"

#include "PgSQL_PolarDB_HGM_Internal.h"
#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Session.h"
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

extern PgSQL_Threads_Handler *GloPTH;

#if POLARDB_PROXY && POLARDB_DEBUG
static bool polardb_debug_reader_acquire_fault(const char* fault_name) {
	char buf[64] = {0};
	bool matched = false;
	if (polardb_debug_consume_fault_file(
			"POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE", buf, sizeof(buf))) {
		matched = (strcmp(buf, fault_name) == 0);
	}

	if (matched) {
		polardb_debug_clear_fault_file("POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE");
	}
	return matched;
}
#endif // POLARDB_PROXY && POLARDB_DEBUG

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

static uint64_t polardb_pool_auth_reuse_key_strings(
	const char* username, const char* dbname);
static uint64_t polardb_pool_session_options_reuse_key(
	const PgSQL_Connection* conn);
static void polardb_ensure_pool_key(PgSQL_Connection* conn);
static PgSQL_PoolMatchKey polardb_core_pool_match_key_for_conn(
	const PgSQL_Connection* conn);
static bool polardb_startup_profile_matches_request_generation(
	const PgSQL_Connection* conn,
	const PolarDB_PoolRequest& pool_request);
static bool polardb_startup_identity_mode_matches_request(
	const PgSQL_Connection* conn,
	const PolarDB_PoolRequest& pool_request);

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
	key.append(std::to_string(req.has_startup_parameters ? 1 : 0));
	key.push_back('\x1f');
	key.append(std::to_string(req.startup_options_hash));
	if (req.has_target_server()) {
		key.push_back('\x1f');
		key.append(req.target_address);
		key.push_back('\x1f');
		key.append(std::to_string(req.target_port));
		key.push_back('\x1f');
		key.append(std::to_string(req.target_required_free_count));
	}
	return key;
}

static PgSQL_PoolMatchKey polardb_core_pool_match_key(
		uint32_t profile_generation, const PolarDB_PoolKey& pool_key) {
	PgSQL_PoolMatchKey core_key;
	core_key.words[0] = profile_generation;
	core_key.words[1] = pool_key.auth_hash;
	core_key.words[2] = pool_key.startup_identity_hash;
	core_key.words[3] = pool_key.startup_options_hash;
	return core_key;
}

static PgSQL_PoolMatchKey polardb_core_pool_match_key_for_conn(
		const PgSQL_Connection* conn) {
	return conn
		? polardb_core_pool_match_key(
			conn->polardb_startup_profile_generation,
			conn->polardb_pool_key)
		: PgSQL_PoolMatchKey{};
}

static PgSQL_PoolMatchKey polardb_core_pool_match_key_for_request(
		const PolarDB_PoolRequest& request) {
	return polardb_core_pool_match_key(
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
	PolarDB_PoolRequest request =
		polardb_make_read_pool_request(
			startup_profile, true, 0, 0);
	request.expected_profile = PolarDB_PoolProfile::RFQ;
	request.startup_identity_mode = req.startup_identity_mode;
	request.key.auth_hash =
		polardb_pool_auth_reuse_key_strings(
			req.username.c_str(), req.dbname.c_str());
	request.key.startup_identity_hash =
		polardb_startup_client_reuse_key(req.startup_client);
	request.key.startup_options_hash = req.startup_options_hash;
	return request;
}

static unsigned int polardb_split_warmup_server_compatible_free_count(
		PgSQL_SrvC* mysrvc,
		const PgSQL_SplitWarmupRequest& req,
		const PolarDB_StartupProfile& startup_profile) {
	if (!mysrvc) {
		return 0;
	}
	if (!startup_profile.has_rfq_lsn() ||
			!startup_profile.has_rfq_xid()) {
		return 0;
	}
	const PolarDB_PoolRequest warmup_request =
		polardb_pool_request_for_warmup_request(req, startup_profile);
	return mysrvc->matching_connection_count(
		polardb_core_pool_match_key_for_request(warmup_request));
}

static bool polardb_split_warmup_server_capacity_available(
		PgSQL_SrvC* mysrvc,
		unsigned int* capacity_available) {
	if (capacity_available) {
		*capacity_available = 0;
	}
	if (!mysrvc || mysrvc->max_connections <= 0 ||
			!mysrvc->ConnectionsUsed || !mysrvc->ConnectionsFree) {
		return false;
	}
	const PolarDB_PoolConnStats pool_stats = mysrvc->polardb_pool_conn_stats();
	const unsigned int total = pool_stats.total();
	const unsigned int max_connections =
		static_cast<unsigned int>(mysrvc->max_connections);
	if (total >= max_connections) {
		return false;
	}
	if (capacity_available) {
		*capacity_available = max_connections - total;
	}
	return true;
}

static constexpr const char* polardb_split_warmup_reason_throttle = "throttle";

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
	if (!polardb_split_warmup_server_capacity_available(mysrvc, nullptr)) {
		return "capacity";
	}
	if (check_throttle && pgsql_connection_creation_throttled_locked(mysrvc)) {
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
		mysrvc ? mysrvc->current_latency_us : 0,
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

static bool polardb_split_warmup_enabled_runtime() {
	return GloPTH && GloPTH->variables.polardb_lazy_warmup_split;
}

static void polardb_split_warmup_capture_startup_parameters(
		PgSQL_SplitWarmupRequest& req,
		const PgSQL_Connection* client_conn) {
	if (!client_conn) {
		return;
	}
	req.has_startup_parameters = true;
	req.startup_options_hash = polardb_pool_session_options_reuse_key(
		client_conn);
	req.startup_parameters.clear();
	req.startup_parameter_hash.clear();
	req.startup_parameters.reserve(PGSQL_NAME_LAST_LOW_WM);
	req.startup_parameter_hash.reserve(PGSQL_NAME_LAST_LOW_WM);
	for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
		if (!client_conn->variables[i].value ||
				client_conn->var_hash[i] == 0) {
			req.has_startup_parameters = false;
			req.startup_options_hash = 0;
			req.startup_parameters.clear();
			req.startup_parameter_hash.clear();
			return;
		}
		req.startup_parameters.push_back(client_conn->variables[i].value);
		req.startup_parameter_hash.push_back(client_conn->var_hash[i]);
	}
}

void PgSQL_PolarDB_ReaderPool::request_split_warmup(
		unsigned int reader_hostgroup_id,
		const char* username,
		const char* password,
		const char* dbname,
		const PolarDB_StartupClientContext& startup_client,
		const PgSQL_Connection* client_conn) {
	if (!polardb_split_warmup_enabled_runtime()) {
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

	const PgSQL_HostGroups_Manager::PolarDB_HG_Config* hg_config =
		hgm_->find_polardb_hg_config(reader_hostgroup_id);
	if (hg_config && hg_config->is_polardb_hostgroup &&
			!hgm_->polardb_hostgroup_requests_rfq_lsn(reader_hostgroup_id)) {
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
		pgsql_split_warmup_max_connections_per_request_from_int(
			pgsql_thread___polardb_split_warmup_max_connections_per_request);
	PgSQL_SplitWarmupRequest base_request{
		reader_hostgroup_id, username, password, dbname,
		startup_client, now_us, max_connections_per_request};
	base_request.startup_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
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
		"source=%d max_connections_per_request=%u\n",
		reader_hostgroup_id, username, dbname ? dbname : "",
		polardb_proxy_identity_mode_name(
			pgsql_thread___polardb_proxy_identity_mode),
		startup_client.identity.host.c_str(), startup_client.identity.port,
		static_cast<int>(startup_client.identity.source),
		max_connections_per_request);
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
		refresh_split_warmup_variables();
		warm_split_pools();
	}
}

void PgSQL_PolarDB_ReaderPool::refresh_split_warmup_variables() {
	if (!GloPTH) {
		return;
	}
	GloPTH->wrlock();
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
	pgsql_thread___polardb_proxy_protocol =
		polardb_proxy_protocol_from_string(
			GloPTH->variables.polardb_proxy_protocol,
			POLARDB_PROXY_PROTOCOL_V15);
	pgsql_thread___polardb_proxy_identity_mode =
		polardb_proxy_identity_mode_from_string(
			GloPTH->variables.polardb_proxy_identity_mode,
			static_cast<int>(PolarDB_ProxyIdentityMode::PROXY));
	if (pgsql_thread___polardb_proxy_identity_host) {
		free(pgsql_thread___polardb_proxy_identity_host);
	}
	pgsql_thread___polardb_proxy_identity_host =
		GloPTH->variables.polardb_proxy_identity_host ?
			strdup(GloPTH->variables.polardb_proxy_identity_host) :
			strdup((char*)"");
	pgsql_thread___polardb_proxy_identity_port =
		GloPTH->variables.polardb_proxy_identity_port;
	GloPTH->wrunlock();
}

void PgSQL_PolarDB_ReaderPool::polardb_collect_split_warmup_targets_locked(
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
			policy.proxy_protocol : current_global_polardb_proxy_protocol();
	const PolarDB_StartupProfile warmup_profile =
		PolarDB_StartupProfile::from_protocol(
			polardb_proxy_protocol_from_int(protocol));
	if (!warmup_profile.has_rfq_lsn() || !warmup_profile.has_rfq_xid()) {
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

		unsigned int capacity_available = 0;
		if (!polardb_split_warmup_server_capacity_available(
				mysrvc, &capacity_available)) {
			continue;
		}
		candidates.push_back({mysrvc, compatible_free, capacity_available, 0});
	}

	const unsigned int max_connections_per_request =
		pgsql_split_warmup_max_connections_per_request_from_int(
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

struct PgSQL_SplitWarmupConnect {
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
	conn->polardb_init_connection_tracking();
	__sync_fetch_and_add(&PgHGM->status.server_connections_connected, 1);
	__sync_fetch_and_add(&conn->parent->connect_OK, 1);
	return true;
}

static void polardb_start_split_warmup_connection(
		PgSQL_SplitWarmupConnect& item,
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

static bool polardb_split_warmup_shutdown_requested(
		const std::atomic<bool>* shutdown_flag) {
	return (shutdown_flag &&
			shutdown_flag->load(std::memory_order_relaxed)) ||
		__sync_fetch_and_add(&glovars.shutdown, 0) != 0;
}

static void polardb_fail_pending_split_warmup_connections(
		std::vector<PgSQL_SplitWarmupConnect>& pending,
		const char* message,
		bool stopped_by_shutdown) {
	for (PgSQL_SplitWarmupConnect& item : pending) {
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

static void polardb_connect_split_warmup_batch(
		std::vector<PgSQL_SplitWarmupConnect>& pending,
		PgSQL_HostGroups_Manager* hgm,
		const std::atomic<bool>* shutdown_flag) {
	const int timeout_ms = polardb_split_warmup_connect_timeout_ms();
	const unsigned long long now_us = monotonic_time();
	for (PgSQL_SplitWarmupConnect& item : pending) {
		item.connect_start_us = now_us;
		item.deadline_us = now_us + (unsigned long long)timeout_ms * 1000ULL;
	}
	if (polardb_split_warmup_shutdown_requested(shutdown_flag)) {
		polardb_fail_pending_split_warmup_connections(
			pending, "PolarDB split warmup interrupted by shutdown",
			true);
		return;
	}
	for (PgSQL_SplitWarmupConnect& item : pending) {
		if (polardb_split_warmup_shutdown_requested(shutdown_flag)) {
			polardb_fail_pending_split_warmup_connections(
				pending, "PolarDB split warmup interrupted by shutdown",
				true);
			return;
		}
		polardb_start_split_warmup_connection(item, hgm);
	}

	while (true) {
		if (polardb_split_warmup_shutdown_requested(shutdown_flag)) {
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
			PgSQL_SplitWarmupConnect& item = pending[i];
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
			for (PgSQL_SplitWarmupConnect& item : pending) {
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
			PgSQL_SplitWarmupConnect& item = pending[indexes[i]];
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

bool PgSQL_PolarDB_ReaderPool::drain_split_warmup_requests(
		std::vector<PgSQL_SplitWarmupRequest>& requests) {
	std::lock_guard<std::mutex> warmup_lock(split_warmup_mutex_);
	if (!polardb_split_warmup_enabled_runtime()) {
		const size_t dropped = split_warmup_queue_.size();
		while (!split_warmup_queue_.empty()) {
			split_warmup_queue_.pop();
		}
		split_warmup_queued_.clear();
		split_warmup_inflight_.clear();
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

void PgSQL_PolarDB_ReaderPool::clear_split_warmup_inflight_key(
		const std::string& key) {
	std::unique_lock<std::mutex> lk(split_warmup_mutex_);
	split_warmup_inflight_.erase(key);
}

bool PgSQL_PolarDB_ReaderPool::register_split_warmup_inflight_key(
		const std::string& key) {
	std::unique_lock<std::mutex> lk(split_warmup_mutex_);
	if (!split_warmup_inflight_.insert(key).second) {
		POLARDB_STATUS_COUNT_ONE(split_warmup_dedup_inflight);
		return false;
	}
	return true;
}

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

void PgSQL_PolarDB_ReaderPool::warm_split_pools() {
	std::vector<PgSQL_SplitWarmupRequest> requests;
	if (!drain_split_warmup_requests(requests)) {
		return;
	}

	std::vector<PgSQL_SplitWarmupConnect> pending;
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
			hgm_->polardb_startup_profile_for_hostgroup(req.hostgroup_id);

		hgm_->wrlock();
		PgSQL_HGC* myhgc = hgm_->MyHGC_lookup(req.hostgroup_id);
		if (!myhgc) {
			hgm_->wrunlock();
			clear_split_warmup_inflight_key(req_key);
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
			clear_split_warmup_inflight_key(req_key);
			POLARDB_STATUS_COUNT_ONE(split_warmup_already_warm);
			return;
		}

		if (!target) {
			hgm_->wrunlock();
			clear_split_warmup_inflight_key(req_key);
			POLARDB_STATUS_COUNT_ONE(split_warmup_no_target);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			return;
		}

		conn = pgsql_create_backend_connection_locked(target);
		if (!conn || !conn->userinfo) {
			delete conn;
			hgm_->wrunlock();
			clear_split_warmup_inflight_key(req_key);
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
		// Split warmup is opened outside client dispatch, but it is still owned by
		// the startup identity selected for the requesting session. Force that
		// identity through the normal conninfo builder.
		conn->polardb_forced_startup_identity = req.startup_client.identity;
		if (req.has_startup_parameters &&
				req.startup_parameters.size() == PGSQL_NAME_LAST_LOW_WM &&
				req.startup_parameter_hash.size() == PGSQL_NAME_LAST_LOW_WM) {
			conn->polardb_forced_startup_parameters = true;
			for (int i = 0; i < PGSQL_NAME_LAST_LOW_WM; i++) {
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
		}
#endif // POLARDB_PROXY
		PgSQL_SplitWarmupConnect item;
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
		const std::string req_key = polardb_split_warmup_key(req);
		if (req.has_target_server()) {
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
		polardb_collect_split_warmup_targets_locked(
			req, target_requests, &found_hostgroup,
			&saw_eligible_target, &saw_compatible_free);
		hgm_->wrunlock();

		if (!found_hostgroup) {
			POLARDB_STATUS_COUNT_ONE(split_warmup_no_target);
			hgm_->status.polardb_split_warmup_failed.fetch_add(
				1, std::memory_order_relaxed);
			clear_split_warmup_inflight_key(req_key);
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
			clear_split_warmup_inflight_key(req_key);
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

	for (PgSQL_SplitWarmupConnect& item : pending) {
		PgSQL_Connection* conn = item.conn;
		std::shared_ptr<const void> selected_server_snapshot =
			conn->polardb_selected_server_snapshot;
		(void)selected_server_snapshot;
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
			if (pgsql_split_warmup_count_connect_failure(
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
			clear_split_warmup_inflight_key(item.request_key);
			continue;
		}

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
			clear_split_warmup_inflight_key(item.request_key);
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
			clear_split_warmup_inflight_key(item.request_key);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			continue;
		}

		polardb_ensure_pool_key(conn);
		const PgSQL_PoolMatchKey match_key =
			polardb_core_pool_match_key_for_conn(conn);
		// The local copy above keeps the server alive. Clear the connection's
		// old snapshot before another worker can take it from the FREE list.
		conn->polardb_selected_server_snapshot.reset();
		if (!target->add_matching_connection(conn, match_key)) {
			delete conn;
			POLARDB_STATUS_COUNT_ONE(split_warmup_add_failed);
			hgm_->status.polardb_split_warmup_target_failed.fetch_add(
				1, std::memory_order_relaxed);
			hgm_->wrunlock();
			clear_split_warmup_inflight_key(item.request_key);
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
		clear_split_warmup_inflight_key(item.request_key);
	}

	for (const std::string& key : base_inflight_keys) {
		clear_split_warmup_inflight_key(key);
	}
}

PolarDB_PoolConnStats PgSQL_SrvC::polardb_pool_conn_stats() const {
	PolarDB_PoolConnStats snapshot;
	snapshot.used = pool_used_count_value();
	snapshot.free = pool_free_count_value();
	return snapshot;
}

unsigned int PgSQL_SrvC::polardb_pool_active_count() const {
	return pool_used_count_value();
}

unsigned int PgSQL_SrvC::polardb_pool_total_count() const {
	return pool_used_count_value() + pool_free_count_value();
}

bool PgSQL_SrvC::polardb_pool_can_run_active() const {
	return max_connections > 0 &&
		polardb_pool_active_count() < static_cast<unsigned int>(max_connections);
}

bool PgSQL_SrvC::polardb_pool_can_open_socket() const {
	return max_connections > 0 &&
		polardb_pool_total_count() < static_cast<unsigned int>(max_connections);
}

static PolarDB_PoolRequest polardb_pool_request_for_server(
		const PolarDB_PoolRequest& request, PgSQL_SrvC* srv) {
	PolarDB_PoolRequest scoped_request = request;
	scoped_request.scope.server = srv;
	if (srv && srv->myhgc) {
		scoped_request.scope.hostgroup_id = srv->myhgc->hid;
	}
	return scoped_request;
}

static uint64_t polardb_pool_auth_reuse_key(
		const PgSQL_Connection_userinfo* userinfo) {
	return userinfo
		? polardb_pool_auth_reuse_key_strings(
			userinfo->username, userinfo->dbname)
		: 0;
}

static uint64_t polardb_pool_auth_reuse_key_strings(
		const char* username, const char* dbname) {
	if (!username || !dbname) {
		return 0;
	}
	uint64_t hash = 1469598103934665603ULL;
	hash = polardb_pool_hash_cstr(hash, username);
	hash = polardb_pool_hash_cstr(hash, dbname);
	return hash;
}

static uint64_t polardb_pool_session_options_reuse_key(
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

static void polardb_ensure_pool_key(
		PgSQL_Connection* conn, PolarDB_PoolProfile profile) {
	if (!conn || !conn->polardb_pool_key.empty()) {
		return;
	}
	conn->polardb_pool_key =
		polardb_pool_key_for_connection_profile(conn, profile);
}

static void polardb_ensure_pool_key(PgSQL_Connection* conn) {
	if (!conn) {
		return;
	}
	polardb_ensure_pool_key(
		conn,
		polardb_pool_profile_from_startup_profile(
			conn->polardb_startup_profile));
}

static PolarDB_PoolRequest polardb_pool_request_for_session(
		const PolarDB_PoolRequest& request,
		PgSQL_Session* sess) {
	PolarDB_PoolRequest keyed_request = request;
	keyed_request.startup_identity_mode =
		pgsql_thread___polardb_proxy_identity_mode;
	PgSQL_Connection* client_conn =
		(sess && sess->client_myds && sess->client_myds->myconn)
			? sess->client_myds->myconn : nullptr;
	if (client_conn && client_conn->polardb_pool_key.auth_hash != 0) {
		keyed_request.key.auth_hash =
			client_conn->polardb_pool_key.auth_hash;
	} else {
		keyed_request.key.auth_hash =
			polardb_pool_auth_reuse_key(
				client_conn ? client_conn->userinfo : nullptr);
		if (client_conn) {
			client_conn->polardb_pool_key.auth_hash =
				keyed_request.key.auth_hash;
		}
	}
	if (client_conn &&
			client_conn->polardb_pool_key.startup_options_hash != 0) {
		keyed_request.key.startup_options_hash =
			client_conn->polardb_pool_key.startup_options_hash;
	} else {
		keyed_request.key.startup_options_hash =
			polardb_pool_session_options_reuse_key(client_conn);
		if (client_conn) {
			client_conn->polardb_pool_key.startup_options_hash =
				keyed_request.key.startup_options_hash;
		}
	}
	if (keyed_request.expected_profile == PolarDB_PoolProfile::RFQ) {
		if (sess &&
				sess->polardb_route_state.reader_pool_startup_identity_mode ==
					keyed_request.startup_identity_mode &&
				sess->polardb_route_state.reader_pool_startup_identity_hash != 0) {
			keyed_request.key.startup_identity_hash =
				sess->polardb_route_state.reader_pool_startup_identity_hash;
		} else {
			PolarDB_StartupClientContext startup_client;
			if (polardb_startup_client_from_session(sess, &startup_client)) {
				keyed_request.key.startup_identity_hash =
					polardb_startup_client_reuse_key(startup_client);
				if (sess) {
					sess->polardb_route_state.reader_pool_startup_identity_hash =
						keyed_request.key.startup_identity_hash;
					sess->polardb_route_state.reader_pool_startup_identity_mode =
						keyed_request.startup_identity_mode;
				}
			}
		}
	}
	return keyed_request;
}

static bool polardb_startup_identity_mode_matches_request(
		const PgSQL_Connection* conn,
		const PolarDB_PoolRequest& pool_request) {
	if (!conn || pool_request.expected_profile != PolarDB_PoolProfile::RFQ) {
		return true;
	}
	return conn->polardb_startup_identity_mode ==
		pool_request.startup_identity_mode;
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

static PolarDB_PoolReuseClassification polardb_classify_pool_conn_for_reuse(
		PgSQL_Connection* conn, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request) {
	PolarDB_PoolReuseClassification classification;
	if (!conn || !conn->parent || !sess ||
			conn->async_state_machine != ASYNC_IDLE ||
			!sess->client_myds || !sess->client_myds->myconn ||
			!sess->client_myds->myconn->userinfo) {
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
	if (!polardb_startup_identity_mode_matches_request(conn, pool_request)) {
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

enum class PolarDB_ReaderPoolRejectReason : uint8_t {
	NONE = 0,
	BAD_CONTEXT,
	PROFILE,
	AUTH,
	IDENTITY,
	SESSION_STATE
};

static void polardb_count_reader_pool_reject(
	PgSQL_Thread* thread, PolarDB_ReaderPoolRejectReason reason);
static bool polardb_reader_pool_conn_usable(
	PgSQL_Connection* conn, PgSQL_Session* sess,
	const PolarDB_PoolRequest& pool_request,
	PolarDB_ReaderPoolRejectReason* reject_reason_out);
static bool polardb_reader_pool_conn_can_remain_pooled(
	const PgSQL_Connection* conn);
static constexpr unsigned int POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT = 16;

static PolarDB_PoolReuseState polardb_pool_reuse_state_for_conn(
		PgSQL_HostGroups_Manager* hgm, PgSQL_SrvC* srv,
		PgSQL_Connection* conn) {
	if (!conn || !srv || conn->parent != srv ||
			conn->async_state_machine != ASYNC_IDLE) {
		return PolarDB_PoolReuseState::BAD_CONTEXT;
	}

	PolarDB_PoolRequest request;
	if (!hgm || !srv->myhgc) {
		return PolarDB_PoolReuseState::BAD_CONTEXT;
	}
	const PolarDB_StartupProfile startup_profile =
		hgm->polardb_startup_profile_for_hostgroup(srv->myhgc->hid);
	request = polardb_make_read_pool_request(
		startup_profile, /*only_pooled=*/true,
		/*consistency_target_lsn=*/0, /*max_lag_bytes=*/0);
	request.allow_create = false;
	request = polardb_pool_request_for_server(request, srv);
	if (!polardb_startup_profile_matches_request_generation(conn, request)) {
		return PolarDB_PoolReuseState::PROFILE_MISMATCH;
	}
	if (!polardb_startup_identity_mode_matches_request(conn, request)) {
		return PolarDB_PoolReuseState::IDENTITY_MISMATCH;
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

static bool polardb_reader_pool_conn_reusable(
		PgSQL_HostGroups_Manager* hgm, PgSQL_SrvC* srv,
		PgSQL_Connection* conn) {
	return polardb_pool_reuse_state_for_conn(hgm, srv, conn) ==
		PolarDB_PoolReuseState::EXACT;
}

bool PgSQL_PolarDB_ReaderPool::connection_match_key_for_return(
		PgSQL_Connection* conn, PgSQL_PoolMatchKey* match_key) {
	if (!conn || !match_key || !conn->parent) {
		return false;
	}
	PgSQL_SrvC* srv = static_cast<PgSQL_SrvC*>(conn->parent);
	if (!srv->myhgc || !hgm_->is_polardb_hostgroup(srv->myhgc->hid)) {
		return false;
	}
	const bool server_online =
		srv->polardb_fast_status_value() == MYSQL_SERVER_STATUS_ONLINE;
	const bool pool_safe = polardb_reader_pool_conn_can_remain_pooled(conn);
	const bool reusable =
		polardb_reader_pool_conn_reusable(hgm_, srv, conn);
	if (!server_online || !pool_safe || !reusable) {
		if (!server_online) {
			POLARDB_STATUS_COUNT_ONE(reader_pool_drop_offline);
		} else if (!pool_safe) {
			POLARDB_STATUS_COUNT_ONE(reader_pool_drop_client_identity);
		} else {
			POLARDB_STATUS_COUNT_ONE(reader_pool_drop_unusable);
		}
		return false;
	}
	polardb_ensure_pool_key(conn);
	*match_key = polardb_core_pool_match_key_for_conn(conn);
	return !match_key->empty();
}

PolarDB_ReaderLocalReturn
PgSQL_PolarDB_ReaderPool::local_return_decision(
		PgSQL_Connection* conn) {
	if (!conn || !conn->parent || conn->polardb_pool_key.empty()) {
		return PolarDB_ReaderLocalReturn::USE_SHARED_POOL;
	}
	PgSQL_SrvC* server = static_cast<PgSQL_SrvC*>(conn->parent);
	const auto* hostgroup_config = server->myhgc
		? hgm_->find_polardb_hg_config(server->myhgc->hid) : nullptr;
	if (!hostgroup_config ||
			hostgroup_config->reader_hostgroup !=
				static_cast<int>(server->myhgc->hid)) {
		return PolarDB_ReaderLocalReturn::USE_SHARED_POOL;
	}
	PgSQL_PoolMatchKey match_key;
	return connection_match_key_for_return(conn, &match_key)
		? PolarDB_ReaderLocalReturn::KEEP_WITH_WORKER
		: PolarDB_ReaderLocalReturn::REMOVE_CONNECTION;
}

#if POLARDB_DEBUG
static const char* polardb_reader_pool_reject_reason_name(
		PolarDB_ReaderPoolRejectReason reason) {
	switch (reason) {
	case PolarDB_ReaderPoolRejectReason::NONE:
		return "none";
	case PolarDB_ReaderPoolRejectReason::BAD_CONTEXT:
		return "bad_context";
	case PolarDB_ReaderPoolRejectReason::PROFILE:
		return "profile";
	case PolarDB_ReaderPoolRejectReason::AUTH:
		return "auth";
	case PolarDB_ReaderPoolRejectReason::IDENTITY:
		return "identity";
	case PolarDB_ReaderPoolRejectReason::SESSION_STATE:
		return "session_state";
	}
	return "unknown";
}
#endif // POLARDB_DEBUG

static void polardb_count_reader_pool_reject(
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

static PolarDB_ReaderPoolRejectReason
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

static bool polardb_reader_pool_conn_usable(
		PgSQL_Connection* conn, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		PolarDB_ReaderPoolRejectReason* reject_reason_out = nullptr) {
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

struct PolarDB_ReaderNode {
	PgSQL_SrvC* srv;
	uint64_t lsn;
	unsigned int weight;
	unsigned int active_count;
	bool target_reached;
};

static bool polardb_reader_pool_use_p2c(unsigned int num_nodes) {
	return num_nodes > 1;
}

static bool polardb_reader_pool_server_ok(PgSQL_SrvC* srv) {
	return srv &&
		srv->polardb_fast_status_value() == MYSQL_SERVER_STATUS_ONLINE &&
		srv->weight > 0 &&
		pgsql_srv_latency_allowed(srv) &&
		srv->max_connections > 0;
}

static PgSQL_Connection* polardb_reader_pool_get_from_server(
		PgSQL_HostGroups_Manager* hgm, PgSQL_Thread* thread,
		PgSQL_SrvC* srv, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request) {
	if (!hgm || !srv || !srv->myhgc) {
		return nullptr;
	}
	const PolarDB_PoolRequest server_request =
		polardb_pool_request_for_server(pool_request, srv);
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_match_attempt);
	const PgSQL_PoolMatchKey match_key =
		polardb_core_pool_match_key_for_request(server_request);
	for (unsigned int attempt = 0;
			attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
		POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
			hgm->status, reader_pool_local_take_attempt);
		PgSQL_Connection* local_conn = thread
			? thread->get_local_polardb_reader_connection(
				srv,
				server_request.startup_profile.generation(
					server_request.startup_identity_mode),
				server_request.key)
			: nullptr;
		if (!local_conn) {
			POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
				hgm->status, reader_pool_local_take_miss);
			break;
		}
		POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(
			hgm->status, reader_pool_local_take_hit);
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_conn_examined);
		PolarDB_ReaderPoolRejectReason reject_reason =
			PolarDB_ReaderPoolRejectReason::NONE;
		if (polardb_reader_pool_conn_usable(
				local_conn, sess, server_request, &reject_reason)) {
			return local_conn;
		}
		polardb_count_reader_pool_reject(thread, reject_reason);
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_drop_unusable);
		(void)srv->remove_used_connection(local_conn);
		delete local_conn;
	}
	PgSQL_PoolGetMode mode = PgSQL_PoolGetMode::ALLOW_EXACT_MATCH;
	if (server_request.allow_create) {
		mode = mode | PgSQL_PoolGetMode::ALLOW_RESET;
	}
	for (unsigned int attempt = 0;
			attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
		PgSQL_PoolGetResult got =
			hgm->get_connection_from_selected_server(
				srv, srv->myhgc->hid, match_key, sess, mode);
		PgSQL_Connection* conn = got.conn;
		if (!conn) {
			break;
		}
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_conn_examined);
		if (got.source == PgSQL_PoolGetSource::EXACT_MATCH) {
			PolarDB_ReaderPoolRejectReason reject_reason =
				PolarDB_ReaderPoolRejectReason::NONE;
			if (polardb_reader_pool_conn_usable(
					conn, sess, server_request, &reject_reason)) {
#if POLARDB_DEBUG
				POLARDB_TRACE(
					"PolarDB READER_POOL: take matching core connection srv=%p "
					"host=%s:%u conn=%p\n",
					(void*)srv, srv->address ? srv->address : "(null)",
					srv->port, (void*)conn);
#endif // POLARDB_DEBUG
				return conn;
			}
			polardb_count_reader_pool_reject(thread, reject_reason);
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_drop_unusable);
		} else {
			bool acceptable = !conn->is_connected();
			PolarDB_PoolReuseClassification classification;
			if (!acceptable) {
				classification = polardb_classify_pool_conn_for_reuse(
					conn, sess, server_request);
				acceptable =
					classification.state == PolarDB_PoolReuseState::EXACT ||
					classification.state == PolarDB_PoolReuseState::NEEDS_RESET ||
					classification.state ==
						PolarDB_PoolReuseState::NEEDS_VARIABLE_UPDATE;
			}
			if (acceptable) {
				return conn;
			}
			polardb_count_reader_pool_reject(
				thread,
				polardb_reader_pool_reject_reason_from_reuse_state(
					classification.state));
		}
		(void)srv->remove_used_connection(conn);
		delete conn;
	}
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_match_miss);
	return nullptr;
}

static int polardb_reader_pool_pick_weighted_node(
		PolarDB_ReaderNode* nodes, unsigned int num_nodes,
		unsigned int weight_sum) {
	if (!nodes || num_nodes == 0 || weight_sum == 0) {
		return -1;
	}
	unsigned int k = rand_fast() % weight_sum;
	k++;
	unsigned int running_sum = 0;
	for (unsigned int j = 0; j < num_nodes; j++) {
		running_sum += nodes[j].weight;
		if (k <= running_sum) {
			return (int)j;
		}
	}
	return (int)(num_nodes - 1);
}

static bool polardb_reader_pool_node_better(
		PgSQL_Thread* thread, const PolarDB_ReaderNode& lhs,
		const PolarDB_ReaderNode& rhs) {
	// Selection compares only global server load.  Neither worker-local cache
	// history nor matching connection counts are allowed to choose the server.
	const uint64_t lhs_load =
		static_cast<uint64_t>(lhs.active_count) * rhs.weight;
	const uint64_t rhs_load =
		static_cast<uint64_t>(rhs.active_count) * lhs.weight;
	if (lhs_load != rhs_load) {
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_active_load);
		return lhs_load < rhs_load;
	}
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_random);
	return (rand_fast() & 1) == 0;
}

static PolarDB_ReaderResult polardb_get_conn_from_reader_nodes(
		PgSQL_HostGroups_Manager* hgm,
		PolarDB_ReaderNode* nodes, unsigned int num_nodes,
		unsigned int weight_sum, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request) {
	PolarDB_ReaderResult result;
	if (num_nodes == 0 || weight_sum == 0) {
		return result;
	}

	PgSQL_Thread* thread = sess ? sess->thread : NULL;
	int first_idx = polardb_reader_pool_pick_weighted_node(
		nodes, num_nodes, weight_sum);
	int second_idx = -1;
	int selected_idx = first_idx;
	const bool use_p2c = polardb_reader_pool_use_p2c(num_nodes);
	if (use_p2c && num_nodes > 1 && first_idx >= 0) {
		second_idx = polardb_reader_pool_pick_weighted_node(
			nodes, num_nodes, weight_sum);
		if (second_idx == first_idx) {
			second_idx = (first_idx + 1 +
				(int)(rand_fast() % (num_nodes - 1))) % (int)num_nodes;
		}
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_select);
		if (polardb_reader_pool_node_better(
				thread, nodes[second_idx], nodes[first_idx])) {
			selected_idx = second_idx;
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_second);
		}
	}
	if (selected_idx >= 0) {
		// Ordinary reads keep the policy choice even when that server has no
		// match. Pooled-only reads cannot create or reset, so another otherwise
		// eligible server with an exact shared-pool match remains usable.
		result.srv = nodes[selected_idx].srv;
		auto try_node = [&](int idx) {
			PgSQL_Connection* conn = polardb_reader_pool_get_from_server(
				hgm, thread, nodes[idx].srv, sess, pool_request);
			if (!conn) {
				return false;
			}
			result.conn = conn;
			result.srv = nodes[idx].srv;
			result.status = PolarDB_ReaderStatus::ACQUIRED;
			return true;
		};
		(void)try_node(selected_idx);
		if (!result.acquired() && pool_request.only_pooled) {
			for (unsigned int offset = 1; offset < num_nodes; offset++) {
				const int fallback_idx =
					(selected_idx + static_cast<int>(offset)) %
					static_cast<int>(num_nodes);
				if (try_node(fallback_idx)) {
					break;
				}
			}
		}
	}
	return result;
}

static PolarDB_ReaderResult polardb_try_reader_pool_fast(
		PgSQL_HostGroups_Manager* hgm, unsigned int hostgroup_id,
		PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address, int exclude_port) {
	PolarDB_ReaderResult result;
	PgSQL_Thread* thread = sess ? sess->thread : NULL;
	if (!hgm || !sess) {
		return result;
	}
	const PgSQL_HostGroups_Manager::PolarDB_HG_Config* hg_config =
		hgm->find_polardb_hg_config(hostgroup_id);
	if (!hg_config || !hg_config->is_polardb_hostgroup) {
		return result;
	}
	const bool has_wait_target = wait_spec.has_wait();
	const bool lag_cap_enabled = reader_plan.lag_cap_enabled();
	const PolarDB_StartupProfile startup_profile =
		hgm->polardb_startup_profile_for_hostgroup(hostgroup_id);
	if (has_wait_target && !startup_profile.has_rfq_lsn()) {
		result.status = PolarDB_ReaderStatus::RFQ_UNAVAILABLE;
		return result;
	}
	if (lag_cap_enabled && reader_plan.primary_lsn == 0) {
		result.status = PolarDB_ReaderStatus::PRIMARY_LSN_UNKNOWN;
		return result;
	}

	PolarDB_PoolRequest pool_request =
		polardb_pool_request_for_session(
			polardb_make_read_pool_request(
				startup_profile, only_pooled, wait_spec.target,
				reader_plan.max_lag_bytes),
			sess);
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_lookup);

	const uint64_t now_us =
		(has_wait_target || lag_cap_enabled) ? monotonic_time() : 0;
	bool freshness_clamped = false;
	const uint32_t fresh_ms = polardb_effective_lsn_freshness_ms(
		pgsql_thread___polardb_lsn_freshness_ms,
		wait_spec.timeout_ms,
		reader_plan.max_lag_bytes,
		pgsql_thread___polardb_lag_cap_freshness_ms,
		&freshness_clamped);
	if (freshness_clamped) {
		POLARDB_THREAD_COUNT_ONE(thread, lag_cap_freshness_clamped);
	}
	PolarDB_ReaderStatus filter_status =
		PolarDB_ReaderStatus::READER_UNAVAILABLE;
	auto lag_allows_reader = [&](uint64_t reader_lsn, bool reader_lsn_fresh) {
		if (!lag_cap_enabled) {
			return true;
		}
		if (reader_lsn == 0) {
			POLARDB_THREAD_COUNT_ONE(thread, lsn_stale_count);
			POLARDB_THREAD_COUNT_ONE(thread, lag_cap_lsn_unknown);
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_lsn_unknown);
			filter_status = polardb_reader_status_prefer(
				filter_status, PolarDB_ReaderStatus::READER_LSN_UNKNOWN);
			return false;
		}
		if (!reader_lsn_fresh) {
			POLARDB_THREAD_COUNT_ONE(thread, lsn_stale_count);
			POLARDB_THREAD_COUNT_ONE(thread, lag_cap_lsn_stale);
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_lsn_stale);
			filter_status = polardb_reader_status_prefer(
				filter_status, PolarDB_ReaderStatus::READER_LSN_STALE);
			return false;
		}
		if (!reader_plan.within_byte_cap(reader_lsn)) {
			POLARDB_THREAD_COUNT_ONE(thread, lag_cap_rejected);
			POLARDB_PROFILE_THREAD_COUNT_ONE(thread,
				reader_target_lag_cap_reject);
			filter_status = polardb_reader_status_prefer(
				filter_status, PolarDB_ReaderStatus::READER_LAG_EXCEEDED);
			return false;
		}
		POLARDB_THREAD_COUNT_ONE(thread, lag_cap_accepted);
		return true;
	};

	/*
	 * Selection reads the current topology plus global status, LSN, weight and
	 * active count. Ordinary reads never let pool contents change that choice.
	 * A pooled-only read may try another otherwise eligible server after the
	 * first server has no exact shared-pool match, because it cannot create or
	 * reset a connection.
	 */
	std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
		server_snapshot = hgm->get_polardb_server_list_snapshot();
	if (!server_snapshot) {
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_miss_empty);
		return result;
	}
	auto hg_it = server_snapshot->by_hostgroup.find(hostgroup_id);
	if (hg_it == server_snapshot->by_hostgroup.end() ||
			hg_it->second.servers.empty()) {
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_miss_empty);
		return result;
	}
	result.selected_server_snapshot = server_snapshot;

	static constexpr unsigned int POLARDB_READER_NODE_STACK_CAP = 32;
	PolarDB_ReaderNode nodes_static[POLARDB_READER_NODE_STACK_CAP];
	PolarDB_ReaderNode* nodes = nodes_static;
	unsigned int num_nodes = 0;
	unsigned int num_ready_nodes = 0;
	unsigned int weight_sum = 0;
	unsigned int ready_weight_sum = 0;
	const std::vector<PgSQL_SrvC*>& servers = hg_it->second.servers;
	std::atomic<uint64_t>* selection_sequence =
		hg_it->second.selection_sequence.get();
	const unsigned int num_servers = servers.size();

	// The common two-replica topology uses global weighted alternation.
	// A healthy match normally examines one server. The peer is examined only
	// for LSN preference, an unusable first choice, or a pooled-only exact miss.
	if (num_servers == 2 && selection_sequence) {
		const uint64_t weight0 = servers[0] && servers[0]->weight > 0
			? static_cast<uint64_t>(servers[0]->weight) : 0;
		const uint64_t weight1 = servers[1] && servers[1]->weight > 0
			? static_cast<uint64_t>(servers[1]->weight) : 0;
		const uint64_t weight_sum64 = weight0 + weight1;
		const uint64_t sequence = selection_sequence->fetch_add(
			1, std::memory_order_relaxed);
		unsigned int first_idx = sequence & 1U;
		if (weight_sum64 != 0) {
			const uint64_t slot = sequence % weight_sum64;
			const __uint128_t before =
				static_cast<__uint128_t>(slot) * weight1 / weight_sum64;
			const __uint128_t after =
				static_cast<__uint128_t>(slot + 1) * weight1 / weight_sum64;
			first_idx = after > before ? 1U : 0U;
		}
		const unsigned int second_idx = 1 - first_idx;

		auto evaluate = [&](PgSQL_SrvC* srv, PolarDB_ReaderNode* node) {
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_server_considered);
			if (!polardb_reader_pool_server_ok(srv) ||
					(exclude_address && exclude_address[0] && exclude_port >= 0 &&
					 srv->address && strcmp(srv->address, exclude_address) == 0 &&
					 (int)srv->port == exclude_port)) {
				POLARDB_THREAD_COUNT_ONE(thread,
					reader_pool_server_skip_unusable);
				return false;
			}
			const unsigned int active = srv->pool_used_count_value();
			uint64_t reader_lsn = 0;
			bool reader_lsn_fresh = false;
			if (has_wait_target || lag_cap_enabled) {
				reader_lsn = srv->polardb_current_lsn.load(
					std::memory_order_relaxed);
				reader_lsn_fresh = polardb_lsn_cache_fresh(
					srv->lsn_updated_at.load(std::memory_order_relaxed),
					now_us, fresh_ms);
			}
			if (!lag_allows_reader(reader_lsn, reader_lsn_fresh)) {
				return false;
			}
			*node = PolarDB_ReaderNode{
				srv, reader_lsn, static_cast<unsigned int>(srv->weight),
				active,
				reader_lsn_fresh && has_wait_target &&
					reader_lsn >= wait_spec.target};
			return true;
		};

		PolarDB_ReaderNode first_node{};
		PolarDB_ReaderNode second_node{};
		const bool first_ok = evaluate(servers[first_idx], &first_node);
		bool second_ok = false;
		if (!first_ok || (has_wait_target && !first_node.target_reached)) {
			second_ok = evaluate(servers[second_idx], &second_node);
		}
		PolarDB_ReaderNode* preferred = first_ok ? &first_node : nullptr;
		if (!first_ok && second_ok) {
			preferred = &second_node;
		} else if (second_ok && second_node.target_reached &&
				(!first_ok || !first_node.target_reached)) {
			preferred = &second_node;
		}
		auto try_node = [&](PolarDB_ReaderNode* node) {
			if (!node) {
				return false;
			}
			PgSQL_Connection* conn = polardb_reader_pool_get_from_server(
				hgm, thread, node->srv, sess, pool_request);
			if (!conn) {
				return false;
			}
			result.conn = conn;
			result.srv = node->srv;
			result.status = PolarDB_ReaderStatus::ACQUIRED;
			result.wait_bypass_allowed = node->target_reached;
			return true;
		};
		(void)try_node(preferred);
		if (!result.acquired() && only_pooled) {
			PolarDB_ReaderNode* alternate = nullptr;
			if (preferred == &first_node) {
				if (!second_ok) {
					second_ok = evaluate(servers[second_idx], &second_node);
				}
				alternate = second_ok ? &second_node : nullptr;
			} else if (preferred == &second_node) {
				alternate = first_ok ? &first_node : nullptr;
			}
			(void)try_node(alternate);
		}
		if (result.acquired()) {
			pgsql_pool_status_count_get(thread, &hgm->status.pgconnpoll_get);
			pgsql_pool_status_count_get_ok(thread, &hgm->status.pgconnpoll_get_ok);
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_hit);
			if (result.wait_bypass_allowed) {
				POLARDB_THREAD_COUNT_ONE(thread, target_lsn_preferred);
			} else if (has_wait_target) {
				POLARDB_THREAD_COUNT_ONE(thread, target_lsn_fallback_wait);
			}
			return result;
		}
		result.srv = preferred ? preferred->srv : nullptr;
		result.status = result.srv
			? (only_pooled ? PolarDB_ReaderStatus::RFQ_UNAVAILABLE
				: PolarDB_ReaderStatus::READER_UNAVAILABLE)
			: filter_status;
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_miss_empty);
		return result;
	}

	bool sampled_pair = false;
	// P2C compares two readers. Equal-weight ordinary reads can sample that
	// pair directly; any unusable sample returns to the complete scan below.
	if (num_servers > 2 && num_servers <= POLARDB_READER_NODE_STACK_CAP &&
			!has_wait_target && !lag_cap_enabled &&
			!only_pooled &&
			(!exclude_address || !exclude_address[0] || exclude_port < 0)) {
		const unsigned int common_weight = servers[0]
			? static_cast<unsigned int>(servers[0]->weight) : 0;
		bool equal_positive_weights = common_weight > 0;
		for (unsigned int n = 1; equal_positive_weights && n < num_servers; n++) {
			equal_positive_weights = servers[n] &&
				static_cast<unsigned int>(servers[n]->weight) == common_weight;
		}
		if (equal_positive_weights) {
			const unsigned int first_idx = rand_fast() % num_servers;
			const unsigned int second_idx =
				(first_idx + 1 + rand_fast() % (num_servers - 1)) % num_servers;
			PgSQL_SrvC* first = servers[first_idx];
			PgSQL_SrvC* second = servers[second_idx];
			if (polardb_reader_pool_server_ok(first) &&
					polardb_reader_pool_server_ok(second)) {
				POLARDB_THREAD_COUNT(thread, reader_pool_server_considered, 2);
				nodes[0] = PolarDB_ReaderNode{
					first, 0, common_weight, first->pool_used_count_value(), false};
				nodes[1] = PolarDB_ReaderNode{
					second, 0, common_weight, second->pool_used_count_value(), false};
				num_nodes = 2;
				weight_sum = common_weight * 2;
				sampled_pair = true;
			}
		}
	}

	const unsigned int start = num_servers > 1 ? rand_fast() % num_servers : 0;
	for (unsigned int n = 0; !sampled_pair && n < num_servers; n++) {
		PgSQL_SrvC* srv = servers[(start + n) % num_servers];
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_server_considered);
		if (!polardb_reader_pool_server_ok(srv)) {
			POLARDB_THREAD_COUNT_ONE(thread,
				reader_pool_server_skip_unusable);
			continue;
		}
		const unsigned int active = srv->pool_used_count_value();
		if (exclude_address && exclude_address[0] && exclude_port >= 0 &&
				srv->address &&
				strcmp(srv->address, exclude_address) == 0 &&
				(int)srv->port == exclude_port) {
			POLARDB_THREAD_COUNT_ONE(thread,
				reader_pool_server_skip_unusable);
			continue;
		}
		bool reader_lsn_fresh = false;
		uint64_t reader_lsn = 0;
		if (has_wait_target || lag_cap_enabled) {
			const uint64_t updated_us =
				srv->lsn_updated_at.load(std::memory_order_relaxed);
			reader_lsn =
				srv->polardb_current_lsn.load(std::memory_order_relaxed);
			reader_lsn_fresh =
				polardb_lsn_cache_fresh(updated_us, now_us, fresh_ms);
		}
		if (!lag_allows_reader(reader_lsn, reader_lsn_fresh)) {
			continue;
		}
		const bool reader_lsn_reaches_target =
			reader_lsn_fresh && has_wait_target &&
			reader_lsn >= wait_spec.target;
		if (num_nodes >= POLARDB_READER_NODE_STACK_CAP) {
			POLARDB_THREAD_COUNT_ONE(thread, reader_node_limit);
			continue;
		}
			nodes[num_nodes] = PolarDB_ReaderNode{
			srv,
			reader_lsn,
			static_cast<unsigned int>(srv->weight),
			active,
			reader_lsn_reaches_target};
		num_nodes++;
		weight_sum += static_cast<unsigned int>(srv->weight);
		if (reader_lsn_reaches_target) {
			std::swap(nodes[num_ready_nodes], nodes[num_nodes - 1]);
			num_ready_nodes++;
			ready_weight_sum += static_cast<unsigned int>(srv->weight);
		}
	}
	result.status = filter_status;
	bool fallback_to_wait_set = false;
	if (has_wait_target && num_ready_nodes > 0 && ready_weight_sum > 0) {
		PolarDB_ReaderResult preferred_result = polardb_get_conn_from_reader_nodes(
			hgm, nodes, num_ready_nodes, ready_weight_sum, sess, pool_request);
		if (preferred_result.acquired()) {
			result = preferred_result;
			result.selected_server_snapshot = server_snapshot;
			result.wait_bypass_allowed = true;
			POLARDB_THREAD_COUNT_ONE(thread, target_lsn_preferred);
		} else if (!result.srv) {
			result.srv = preferred_result.srv;
		}
	}
	if (!result.acquired()) {
		const bool preferred_attempted =
			has_wait_target && num_ready_nodes > 0;
		unsigned int fallback_nodes =
			preferred_attempted ? num_nodes - num_ready_nodes : num_nodes;
		unsigned int fallback_weight_sum =
			preferred_attempted ? weight_sum - ready_weight_sum : weight_sum;
		PolarDB_ReaderNode* fallback_set =
			preferred_attempted ? nodes + num_ready_nodes : nodes;
		fallback_to_wait_set =
			has_wait_target && fallback_nodes > 0 && fallback_weight_sum > 0;
		PolarDB_ReaderResult fallback_result =
			polardb_get_conn_from_reader_nodes(
				hgm, fallback_set, fallback_nodes, fallback_weight_sum, sess,
				pool_request);
		if (fallback_result.acquired()) {
			result = fallback_result;
			result.selected_server_snapshot = server_snapshot;
			result.wait_bypass_allowed = false;
			if (fallback_to_wait_set) {
				POLARDB_THREAD_COUNT_ONE(thread, target_lsn_fallback_wait);
			}
		} else if (!result.srv) {
			result.srv = fallback_result.srv;
		}
	}

	if (result.acquired()) {
		pgsql_pool_status_count_get(thread, &hgm->status.pgconnpoll_get);
		pgsql_pool_status_count_get_ok(thread, &hgm->status.pgconnpoll_get_ok);
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_hit);
		return result;
	}

	if (num_nodes > 0) {
		result.status = only_pooled
			? PolarDB_ReaderStatus::RFQ_UNAVAILABLE
			: PolarDB_ReaderStatus::READER_UNAVAILABLE;
	}
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_miss_empty);
	return result;
}

PolarDB_ReaderResult PgSQL_PolarDB_ReaderPool::get_MyConn_polardb_reader(
		unsigned int _hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address, int exclude_port) {
	PolarDB_ReaderResult result = polardb_try_reader_pool_fast(
		hgm_, _hid, sess,
		reader_plan, wait_spec, only_pooled,
		exclude_address, exclude_port);
	if (result.acquired()) {
		result.conn->polardb_selected_server_snapshot =
			result.selected_server_snapshot;
	}
	if (result.acquired() ||
			result.status == PolarDB_ReaderStatus::RFQ_UNAVAILABLE ||
			polardb_reader_status_redirects_to_writer(result.status) ||
			only_pooled) {
		return result;
	}

	// Selection above used the current hostgroup snapshot and global server
	// values. If no connection matched, creation is allowed only on that server.
	PgSQL_SrvC* selected = result.srv;
	if (!selected) {
		return result;
	}

	pgsql_pool_status_count_get(sess ? sess->thread : nullptr,
		&hgm_->status.pgconnpoll_get);
	const PolarDB_StartupProfile startup_profile =
		hgm_->polardb_startup_profile_for_hostgroup(_hid);
	PolarDB_PoolRequest request = polardb_pool_request_for_session(
		polardb_make_read_pool_request(
			startup_profile, /*only_pooled=*/false, wait_spec.target,
			reader_plan.max_lag_bytes),
		sess);
	request = polardb_pool_request_for_server(request, selected);
	const PgSQL_PoolMatchKey match_key =
		polardb_core_pool_match_key_for_request(request);

	PgSQL_Connection* conn = nullptr;
	for (unsigned int attempt = 0;
			attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
		PgSQL_PoolGetResult got =
			hgm_->get_connection_from_selected_server(
				selected, _hid, match_key, sess,
				PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
					PgSQL_PoolGetMode::ALLOW_CREATE);
		if (!got.conn) {
			break;
		}
		if (got.source == PgSQL_PoolGetSource::CREATED) {
			conn = got.conn;
			break;
		}
		PolarDB_ReaderPoolRejectReason reject_reason =
			PolarDB_ReaderPoolRejectReason::NONE;
		if (polardb_reader_pool_conn_usable(
				got.conn, sess, request, &reject_reason)) {
			conn = got.conn;
			break;
		}
		polardb_count_reader_pool_reject(sess ? sess->thread : nullptr,
			reject_reason);
		(void)selected->remove_used_connection(got.conn);
		delete got.conn;
	}
	if (!conn) {
		result.status = PolarDB_ReaderStatus::READER_BUSY;
		return result;
	}
	selected->update_max_connections_used();
	pgsql_pool_status_count_get_ok(sess ? sess->thread : nullptr,
		&hgm_->status.pgconnpoll_get_ok);
	result.conn = conn;
	conn->polardb_selected_server_snapshot = result.selected_server_snapshot;
	result.srv = selected;
	result.status = PolarDB_ReaderStatus::ACQUIRED;
	// A newly created backend has not confirmed the target LSN.  The query wrapper
	// remains mandatory when wait_spec carries a target.
	result.wait_bypass_allowed = false;
	return result;
}


#endif // POLARDB_PROXY
