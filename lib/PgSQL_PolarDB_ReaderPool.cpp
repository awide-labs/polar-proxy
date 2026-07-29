#include "PgSQL_PolarDB_ReaderPool.h"

#include "PgSQL_PolarDB_HGM_Internal.h"
#include "PgSQL_PolarDB_ReaderPool_Internal.h"
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
	bool keep_until_cleared = false;
	if (polardb_debug_read_fault_file(
			"POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE", buf, sizeof(buf))) {
		matched = (strcmp(buf, fault_name) == 0);
		keep_until_cleared =
			strcmp(fault_name, "reader_busy") == 0 &&
			strcmp(buf, "reader_busy_until_deadline") == 0;
		matched = matched || keep_until_cleared;
	}

	if (matched && !keep_until_cleared) {
		polardb_debug_clear_fault_file("POLARDB_DEBUG_READER_ACQUIRE_FAULT_FILE");
	}
	return matched;
}
#endif // POLARDB_PROXY && POLARDB_DEBUG

#if POLARDB_PROXY
static inline unsigned int polardb_reader_selection_used_count(
		PgSQL_Thread* thread, PgSQL_SrvC* server) {
#if POLARDB_PROFILE
	if (thread) {
		thread->polardb_profile_note_pool_used_count_read(server);
	}
#else
	(void)thread;
#endif // POLARDB_PROFILE
	return server->pool_used_count_value();
}

/**
 * @brief One candidate reader, captured for a single acquisition request.
 *
 * The values are a snapshot taken while evaluating the request: the server
 * pointer plus its configured weight and connection limit, and the LSN sample
 * with the two flags derived from it (whether the sample is fresh enough to
 * trust, and whether it already reached the request's wait target).
 *
 * active_count is the exception. It is filled with 0 when the node is built and
 * is loaded from the shared used count only for the two candidates that a
 * power-of-two-choices comparison actually compares. Reading it anywhere else
 * compares 0 against 0 and silently falls through to the random tie-break, so
 * treat it as meaningful only inside such a comparison.
 */
struct PolarDB_ReaderNode {
	PgSQL_SrvC* srv;
	uint64_t lsn;
	unsigned int weight;
	unsigned int max_connections;
	unsigned int active_count;
	bool lsn_fresh;
	bool target_reached;
};

/**
 * @brief Compare two candidates by weight-normalised active load.
 *
 * Load is compared by cross-multiplication rather than by division, so a
 * heavier-weighted reader is allowed proportionally more active connections
 * before it counts as the busier one.
 *
 * Both nodes must have had active_count loaded; nodes still holding the initial
 * 0 always compare equal.
 *
 * @param lhs  First candidate.
 * @param rhs  Second candidate.
 * @return A value below zero when lhs is the less loaded and therefore the
 *         preferred node, 0 on a tie, above zero when rhs is preferred.
 */
static int polardb_reader_normalized_load_compare(
		const PolarDB_ReaderNode& lhs,
		const PolarDB_ReaderNode& rhs) {
	const uint64_t lhs_load =
		static_cast<uint64_t>(lhs.active_count) * rhs.weight;
	const uint64_t rhs_load =
		static_cast<uint64_t>(rhs.active_count) * lhs.weight;
	return lhs_load < rhs_load ? -1 : (lhs_load > rhs_load ? 1 : 0);
}

static void polardb_count_reader_target_candidate(
		PgSQL_Thread* thread, uint64_t target_lsn,
		uint64_t reader_lsn, bool reader_lsn_fresh) {
	if (target_lsn == 0) {
		return;
	}
	if (reader_lsn == 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_lsn_unknown);
	} else if (!reader_lsn_fresh) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_lsn_stale);
	} else if (reader_lsn >= target_lsn) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_ready_candidate);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_target_lsn_behind);
	}
}

static void polardb_merge_reader_lsn_observation(
		PolarDB_ReaderResult* result, const PolarDB_ReaderNode* node) {
	if (!result || !node) {
		return;
	}
	if (node->srv == result->srv) {
		result->selected_reader_lsn = node->lsn;
		result->selected_reader_lsn_fresh = node->lsn_fresh;
	}
	if (node->lsn_fresh &&
			(!result->best_considered_reader_lsn_fresh ||
			 node->lsn > result->best_considered_reader_lsn)) {
		result->best_considered_reader_lsn = node->lsn;
		result->best_considered_reader_lsn_fresh = true;
	}
}

static void polardb_count_reader_target_result(
		PgSQL_Session* sess, const PolarDB_WaitSpec& wait_spec,
		const PolarDB_ReaderResult& result) {
	if (!result.acquired() || !wait_spec.has_wait()) {
		return;
	}
	polardb_count_reader_target_selection(
		sess ? sess->thread : nullptr, wait_spec.target,
		result.selected_reader_lsn, result.selected_reader_lsn_fresh,
		result.best_considered_reader_lsn,
		result.best_considered_reader_lsn_fresh);
#if POLARDB_PROFILE
	if (sess) {
		sess->polardb_profile_note_reader_selection(wait_spec, result);
	}
#endif // POLARDB_PROFILE
}

static void polardb_count_reader_both_behind_load(
		PgSQL_Thread* thread,
		const PolarDB_ReaderNode& first,
		const PolarDB_ReaderNode& second) {
#if POLARDB_PROFILE
	POLARDB_PROFILE_THREAD_COUNT_ONE(
		thread, reader_target_both_behind_compared);
	if (first.lsn == second.lsn) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_target_both_behind_equal_lsn);
		return;
	}
	const PolarDB_ReaderNode& fresher =
		first.lsn > second.lsn ? first : second;
	const PolarDB_ReaderNode& other =
		first.lsn > second.lsn ? second : first;
	const int load_order =
		polardb_reader_normalized_load_compare(fresher, other);
	if (load_order < 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_target_fresher_less_loaded);
	} else if (load_order == 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_target_fresher_equal_loaded);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_target_fresher_more_loaded);
	}
#else
	(void)thread;
	(void)first;
	(void)second;
#endif // POLARDB_PROFILE
}

static int polardb_reader_select_weighted_index(
		PgSQL_Thread* thread, unsigned int hostgroup_id,
		uint64_t server_snapshot_generation,
		std::atomic<uint64_t>* selection_start,
		uint64_t weight0, uint64_t weight1) {
	// With exactly two configured readers, use the worker-local sequence instead
	// of choosing two random candidates. Equal weights alternate between readers;
	// unequal weights assign sequence slots in the configured ratio.
	const uint64_t weight_sum = weight0 + weight1;
	const uint64_t sequence = thread
		? thread->polardb_next_reader_selection_sequence(
			hostgroup_id, server_snapshot_generation, selection_start)
		: selection_start->fetch_add(1, std::memory_order_relaxed);
	int selected_idx = sequence & 1U;
	if (weight0 != weight1 && weight_sum != 0) {
		const uint64_t slot = sequence % weight_sum;
		const __uint128_t before =
			static_cast<__uint128_t>(slot) * weight1 / weight_sum;
		const __uint128_t after =
			static_cast<__uint128_t>(slot + 1) * weight1 / weight_sum;
		selected_idx = after > before ? 1 : 0;
	}
	return selected_idx;
}

static int polardb_reader_pool_refine_two_by_lsn_target(
		PgSQL_Thread* thread, PolarDB_ReaderNode* nodes,
		bool has_wait_target) {
	int selected_idx = 0;
	if (!has_wait_target) {
		return selected_idx;
	}

	// For a read that requires an LSN, prefer a reader already at that LSN. If
	// both are ready, or either LSN is too old to trust, keep the weighted choice.
	// If both have current LSN samples but are behind, prefer the better reader
	// by the freshness and normalized-load comparison.
	const int other_idx = 1 - selected_idx;
	PolarDB_ReaderNode& selected = nodes[selected_idx];
	PolarDB_ReaderNode& other = nodes[other_idx];
	if (other.target_reached != selected.target_reached) {
		return other.target_reached ? other_idx : selected_idx;
	}
	if (selected.target_reached ||
			!selected.lsn_fresh || !other.lsn_fresh) {
		return selected_idx;
	}

	if (POLARDB_PROFILE ||
			pgsql_thread___polardb_reader_prefer_less_loaded) {
		selected.active_count =
			polardb_reader_selection_used_count(thread, selected.srv);
		other.active_count =
			polardb_reader_selection_used_count(thread, other.srv);
	}
	polardb_count_reader_both_behind_load(thread, selected, other);
	if (pgsql_thread___polardb_reader_prefer_freshest_below_target) {
		if (other.lsn == selected.lsn) {
			return selected_idx;
		}
		const uint64_t best_lsn = std::max(other.lsn, selected.lsn);
		const int range_bytes =
			pgsql_thread___polardb_reader_lsn_lag_range_bytes;
		const bool prefer_other = polardb_reader_lsn_in_best_behind_range(
			other.lsn, best_lsn, range_bytes) &&
			!polardb_reader_lsn_in_best_behind_range(
				selected.lsn, best_lsn, range_bytes);
		if (prefer_other) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_target_fresher_exact_switch);
			return other_idx;
		}
		return selected_idx;
	}
	if (!pgsql_thread___polardb_reader_prefer_less_loaded ||
			other.lsn <= selected.lsn) {
		return selected_idx;
	}
	if (polardb_reader_normalized_load_compare(other, selected) < 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_target_fresher_dominance_switch);
		return other_idx;
	}
	return selected_idx;
}

static bool polardb_reader_is_candidate(
		const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry& entry) {
	PgSQL_SrvC* srv = entry.srv;
	return srv &&
		srv->polardb_fast_status_value() == MYSQL_SERVER_STATUS_ONLINE &&
		entry.weight > 0 &&
		pgsql_srv_latency_allowed(
			srv->current_latency_us_value(), entry.max_latency_us) &&
		entry.max_connections > 0;
}

static const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry*
polardb_find_writer_server_list(
		const PgSQL_HostGroups_Manager::PolarDB_HG_Config& hg_config,
		const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot& snapshot) {
	if (hg_config.writer_hostgroup < 0) {
		return nullptr;
	}
	const auto writer_hg = snapshot.by_hostgroup.find(
		static_cast<unsigned int>(hg_config.writer_hostgroup));
	return writer_hg == snapshot.by_hostgroup.end()
		? nullptr : &writer_hg->second;
}

/**
 * @brief Test whether a reader endpoint is known not to be the current writer.
 *
 * This is the enforcement of reader_plan.require_replica. A reader server is
 * matched by address and port against the configured writer entries; entries
 * in OFFLINE_HARD are ignored.
 *
 * A replica-only request is rejected when the writer hostgroup is missing or
 * has no usable endpoint. In that case the endpoint cannot be treated as a
 * replica.
 *
 * @param writers  Writer-hostgroup server list, or null when it is unavailable.
 * @param server   Reader endpoint to classify.
 * @return true when a usable writer endpoint is known and differs from the
 *         reader endpoint.
 */
static bool polardb_reader_endpoint_is_known_replica(
		const PgSQL_HostGroups_Manager::PolarDB_ServerListEntry* writers,
		const PgSQL_SrvC* server) {
	if (!writers || !server || !server->address) {
		return false;
	}
	bool writer_known = false;
	for (const auto& entry : writers->servers) {
		const PgSQL_SrvC* writer = entry.srv;
		if (!writer ||
				writer->polardb_fast_status_value() ==
					MYSQL_SERVER_STATUS_OFFLINE_HARD ||
				!writer->address) {
			continue;
		}
		writer_known = true;
		if (writer->port == server->port &&
				strcmp(writer->address, server->address) == 0) {
			return false;
		}
	}
	return writer_known;
}

/**
 * @brief Test whether two sampled readers may answer this request.
 *
 * Pair sampling is safe when an ordinary acquisition can create on its selected
 * server and no request-wide check needs every server. An LSN target does not
 * by itself require a full scan: when both sampled readers already reached the
 * target, they form a complete P2C choice for a no-wait answer. When either one
 * is behind, the caller expands to the complete server list before choosing so
 * another ready reader is not hidden by the sample.
 *
 * @param lag_cap_enabled             true when a replication lag cap applies.
 * @param only_pooled                 true when only an existing pooled
 *                                    connection may be used.
 * @param confirm_reader_group_capacity true when the caller needs every reader
 *                                    probed before declaring the group full.
 * @param exclude_address             Endpoint address to avoid, or null/empty
 *                                    for none.
 * @param exclude_port                Endpoint port to avoid, negative for none.
 * @return true when the caller may first evaluate two readers.
 */
static bool polardb_request_can_sample_pair(
		bool lag_cap_enabled, bool only_pooled,
		bool confirm_reader_group_capacity,
		const char* exclude_address, int exclude_port) {
	return !lag_cap_enabled && !only_pooled &&
		!confirm_reader_group_capacity &&
		(!exclude_address || !exclude_address[0] || exclude_port < 0);
}

/**
 * @brief Test whether any two eligible readers are interchangeable.
 *
 * With no LSN target, the pair-sampling conditions are also sufficient to use
 * the other sampled reader when the selected server's pool mutex is busy.
 * Target-bearing reads keep the selected reader in the current release; trying
 * a target-ready peer is a separate benchmark experiment.
 */
static bool polardb_request_can_skip_busy_pool(
		bool has_wait_target, bool lag_cap_enabled, bool only_pooled,
		bool confirm_reader_group_capacity,
		const char* exclude_address, int exclude_port) {
	return !has_wait_target &&
		polardb_request_can_sample_pair(
			lag_cap_enabled, only_pooled, confirm_reader_group_capacity,
			exclude_address, exclude_port);
}

bool PgSQL_PolarDB_ReaderPool::reader_pool_reservation_match_key(
		unsigned int hostgroup_id, PgSQL_Session* sess,
		const PolarDB_WaitSpec& wait_spec,
		PgSQL_PoolMatchKey* match_key) const {
	if (!hgm_ || !sess || !match_key) {
		return false;
	}
	const auto hg_config =
		hgm_->get_polardb_hg_config(hostgroup_id);
	if (!hg_config.is_polardb_hostgroup) {
		return false;
	}
	const PolarDB_StartupProfile startup_profile =
		hgm_->polardb_startup_profile_for_hostgroup(
			hostgroup_id, pgsql_thread___polardb_proxy_protocol);
	if (wait_spec.has_wait() && !startup_profile.requests_rfq_lsn()) {
		return false;
	}
	const PolarDB_PoolRequest request = polardb_prepare_pool_request_for_session(
		startup_profile, /*only_pooled=*/false,
		/*require_rfq_profile=*/wait_spec.target != 0, sess);
	*match_key = polardb_core_pool_match_key_for_request(request);
	return !match_key->empty();
}

bool PgSQL_PolarDB_ReaderPool::server_can_serve_request(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* selected_server_snapshot) const {
	if (selected_server_snapshot) {
		selected_server_snapshot->reset();
	}
	std::shared_ptr<const void> server_snapshot;
	if (!resolve_reader_server_snapshot(
			hostgroup_id, server, exclude_address, exclude_port,
			&server_snapshot)) {
		return false;
	}
	if (reader_plan.require_replica) {
		const auto hg_config =
			hgm_->get_polardb_hg_config(hostgroup_id);
		const auto snapshot = std::static_pointer_cast<
			const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>(
				server_snapshot);
		const auto* writers = snapshot
			? polardb_find_writer_server_list(hg_config, *snapshot)
			: nullptr;
		if (!hg_config.is_polardb_hostgroup ||
				!polardb_reader_endpoint_is_known_replica(
					writers, server)) {
			return false;
		}
	}
	const uint64_t now_us =
		reader_plan.lag_cap_enabled() ? monotonic_time() : 0;
	if (!server_meets_lag_policy(
			server, reader_plan, wait_spec, now_us)) {
		return false;
	}
	if (selected_server_snapshot) {
		*selected_server_snapshot = std::move(server_snapshot);
	}
	return true;
}

bool PgSQL_PolarDB_ReaderPool::resolve_reader_server_snapshot(
		unsigned int hostgroup_id, PgSQL_SrvC* server,
		const char* exclude_address, int exclude_port,
		std::shared_ptr<const void>* server_snapshot) const {
	if (server_snapshot) {
		server_snapshot->reset();
	}
	if (!hgm_ || !server ||
			!server_snapshot ||
			(exclude_address && exclude_address[0] && exclude_port >= 0 &&
			 server->address && strcmp(server->address, exclude_address) == 0 &&
			 static_cast<int>(server->port) == exclude_port)) {
		return false;
	}
	const auto snapshot = hgm_->get_polardb_server_list_snapshot();
	if (!snapshot) {
		return false;
	}
	auto hostgroup = snapshot->by_hostgroup.find(hostgroup_id);
	if (hostgroup == snapshot->by_hostgroup.end()) {
		return false;
	}
	const PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry* current =
		nullptr;
	for (const auto& entry : hostgroup->second.servers) {
		if (entry.srv == server) {
			current = &entry;
			break;
		}
	}
	if (!current || !polardb_reader_is_candidate(*current)) {
		return false;
	}
	*server_snapshot = snapshot;
	return true;
}

bool PgSQL_PolarDB_ReaderPool::server_meets_lag_policy(
		PgSQL_SrvC* server,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec, uint64_t now_us) const {
	if (!server) {
		return false;
	}
	if (!reader_plan.lag_cap_enabled()) {
		return true;
	}
	if (reader_plan.group_lsn == 0) {
		return false;
	}
	bool freshness_clamped = false;
	const uint32_t fresh_ms = polardb_effective_lsn_freshness_ms(
		pgsql_thread___polardb_reader_lsn_max_age_ms,
		wait_spec.timeout_ms, reader_plan.max_lag_bytes,
		pgsql_thread___polardb_lag_cap_freshness_ms,
		&freshness_clamped);
	(void)freshness_clamped;
	const PolarDB_ReaderLsnSample sample =
		server->polardb_sample_lsn(now_us, fresh_ms);
	return sample.lsn != 0 && sample.fresh &&
		reader_plan.within_byte_cap(sample.lsn);
}

/**
 * @brief Acquire a reusable connection from one specific reader, trying the
 *        worker-local cache first and then that server's shared pool.
 *
 * Every connection this pops is owned by this function until it either returns
 * that connection or destroys it. A connection that turns out not to be usable
 * for the request is removed from the server and destroyed here rather than
 * being put back, so the caller never receives it; both scans are bounded by
 * POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT. A returned connection is owned by
 * the caller.
 *
 * @param hgm       Hostgroups manager owning the shared pool.
 * @param thread    Worker thread, used for the local cache and counters. May be
 *                  null, which skips local reuse.
 * @param srv       Reader to acquire from. Must have a hostgroup.
 * @param sess      Session the connection is for, used to judge reusability.
 * @param pool_request  Pool identity and mode of the request.
 * @param selected_max_connections  Connection limit to enforce for this server.
 *                  Ignored for pooled-only requests, which never create.
 * @param skip_busy_pool   true to give up instead of blocking on this server's
 *                  pool mutex when it is contended. The caller must supply an
 *                  alternate reader already proved equivalent for this
 *                  request.
 * @param skip_local_reuse true to bypass the worker-local cache and go straight
 *                  to the shared pool.
 * @return The acquisition result. result.conn is null on failure; note that
 *         result.pool_busy means the server pool lock was skipped, which is not
 *         the same as the pool being empty, and it can only be set when
 *         skip_busy_pool was requested. result.server_saturated is meaningful
 *         only for requests that are not pooled-only.
 */
static PgSQL_PoolGetResult polardb_reader_pool_get_from_server(
		PgSQL_HostGroups_Manager* hgm, PgSQL_Thread* thread,
		PgSQL_SrvC* srv, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		unsigned int selected_max_connections,
		bool skip_busy_pool = false,
		bool skip_local_reuse = false) {
	PgSQL_PoolGetResult result;
	if (!hgm || !srv || !srv->myhgc ||
			!pool_request.ready_for_matching()) {
		return result;
	}
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_match_attempt);
	const PgSQL_PoolMatchKey match_key =
		polardb_core_pool_match_key_for_request(pool_request);
	if (!skip_local_reuse) {
		for (unsigned int attempt = 0; attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_pool_local_take_attempt);
			PgSQL_Connection* local_conn = thread
				? thread->polardb_take_local_reader_connection(
					srv,
					pool_request.startup_profile.generation(
						pool_request.startup_identity_mode),
					pool_request.key)
				: nullptr;
			if (!local_conn) {
				POLARDB_PROFILE_THREAD_COUNT_ONE(
					thread, reader_pool_local_take_miss);
				break;
			}
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_pool_local_take_hit);
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_conn_examined);
			PolarDB_ReaderPoolRejectReason reject_reason =
				PolarDB_ReaderPoolRejectReason::NONE;
			if (polardb_reader_pool_conn_usable(
					local_conn, sess, pool_request, &reject_reason)) {
				result.conn = local_conn;
				result.source = PgSQL_PoolGetSource::EXACT_MATCH;
				return result;
			}
			polardb_count_reader_pool_reject(thread, reject_reason);
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_drop_unusable);
			(void)srv->remove_used_connection(local_conn);
			delete local_conn;
		}
	}
	PgSQL_PoolGetMode mode = PgSQL_PoolGetMode::ALLOW_EXACT_MATCH;
	if (!pool_request.only_pooled) {
		mode = mode | PgSQL_PoolGetMode::ALLOW_RESET;
	}
	if (skip_busy_pool) {
		mode = mode | PgSQL_PoolGetMode::SKIP_BUSY_POOL;
	}
#if POLARDB_PROFILE
	const bool shared_free_observed_zero =
		pool_request.only_pooled && srv->pool_free_count_value() == 0;
	if (shared_free_observed_zero) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_shared_free_zero_before_lock);
	}
#endif // POLARDB_PROFILE
	for (unsigned int attempt = 0;
			attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
		PgSQL_PoolGetResult got =
			hgm->get_connection_from_selected_server(
				srv, srv->myhgc->hid, match_key, sess, mode,
				pool_request.only_pooled ? 0 : selected_max_connections);
		PgSQL_Connection* conn = got.conn;
		if (!conn) {
			if (got.pool_busy) {
				result.pool_busy = true;
				return result;
			}
			result.server_saturated = got.server_saturated;
#if POLARDB_PROFILE
			result.exact_match_reserved = got.exact_match_reserved;
#endif // POLARDB_PROFILE
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_match_miss);
			return result;
		}
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_conn_examined);
		if (got.source == PgSQL_PoolGetSource::EXACT_MATCH) {
			PolarDB_ReaderPoolRejectReason reject_reason =
				PolarDB_ReaderPoolRejectReason::NONE;
			if (polardb_reader_pool_conn_usable(
					conn, sess, pool_request, &reject_reason)) {
#if POLARDB_PROFILE
				if (shared_free_observed_zero) {
					POLARDB_PROFILE_THREAD_COUNT_ONE(
						thread,
						reader_pool_shared_free_zero_became_hit);
				}
#endif // POLARDB_PROFILE
#if POLARDB_DEBUG
				POLARDB_TRACE(
					"PolarDB READER_POOL: take matching core connection srv=%p "
					"host=%s:%u conn=%p\n",
					(void*)srv, srv->address ? srv->address : "(null)",
					srv->port, (void*)conn);
#endif // POLARDB_DEBUG
				return got;
			}
			polardb_count_reader_pool_reject(thread, reject_reason);
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_drop_unusable);
		} else {
			bool acceptable = !conn->is_connected();
			PolarDB_PoolReuseClassification classification;
			if (!acceptable) {
				classification = polardb_classify_pool_conn_for_reuse(
					conn, sess, pool_request);
				acceptable =
					classification.state == PolarDB_PoolReuseState::EXACT ||
					classification.state == PolarDB_PoolReuseState::NEEDS_RESET ||
					classification.state ==
						PolarDB_PoolReuseState::NEEDS_VARIABLE_UPDATE;
			}
			if (acceptable) {
				return got;
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
	return result;
}

static void polardb_count_reader_pool_attempt(
		PgSQL_Thread* thread, bool additional,
		const PgSQL_PoolGetResult& result) {
	if (additional) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(
			thread, reader_pool_additional_attempt);
		if (result.pool_busy) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_pool_additional_busy);
		} else if (result.conn) {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_pool_additional_hit);
		} else {
			POLARDB_PROFILE_THREAD_COUNT_ONE(
				thread, reader_pool_additional_miss);
		}
		return;
	}

	POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_pool_selected_attempt);
	if (result.pool_busy) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_pool_selected_busy);
	} else if (result.conn) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_pool_selected_hit);
	} else {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread, reader_pool_selected_miss);
	}
}

/**
 * @brief Acquire from the selected reader, trying an alternate before waiting
 *        on the selected reader's busy pool mutex.
 *
 * Waiting on one reader's pool mutex costs more than using the other reader
 * when nothing in the request distinguishes them, so the attempt escalates in
 * three steps: the selected reader without blocking on its pool mutex; then, if
 * that mutex was contended, the alternate reader, also without blocking; and
 * finally the selected reader again, this time blocking on the mutex and
 * bypassing the worker-local cache, which was already scanned.
 *
 * With no alternate, or an alternate that is the same server, only a single
 * ordinary blocking attempt on the selected reader is made.
 *
 * @param hgm        Hostgroups manager owning the shared pool.
 * @param thread     Worker thread, for local reuse and counters. May be null.
 * @param selected   Reader chosen for this request.
 * @param alternate  Interchangeable second reader, or null when there is none.
 * @param sess       Session the connection is for.
 * @param pool_request  Pool identity and mode of the request.
 * @param acquired_server  Receives the server that actually served the request.
 *        It is pre-set to selected.srv and is rewritten to the alternate when
 *        the alternate served it, so on success the caller must record this
 *        value rather than the node it started from. May be null.
 * @param first_attempt_is_additional  true when the first attempt is already a
 *        fallback, which only reclassifies the profiling counters.
 * @return The acquisition result; result.conn is owned by the caller on success.
 */
static PgSQL_PoolGetResult polardb_reader_acquire_with_alternate(
		PgSQL_HostGroups_Manager* hgm, PgSQL_Thread* thread,
		const PolarDB_ReaderNode& selected,
		const PolarDB_ReaderNode* alternate,
		PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		PgSQL_SrvC** acquired_server,
		bool first_attempt_is_additional) {
	if (acquired_server) {
		*acquired_server = selected.srv;
	}
	if (!alternate || alternate->srv == selected.srv) {
		PgSQL_PoolGetResult result = polardb_reader_pool_get_from_server(
			hgm, thread, selected.srv, sess, pool_request,
			selected.max_connections);
		polardb_count_reader_pool_attempt(
			thread, first_attempt_is_additional, result);
		return result;
	}

	PgSQL_PoolGetResult result = polardb_reader_pool_get_from_server(
		hgm, thread, selected.srv, sess, pool_request,
		selected.max_connections, /*skip_busy_pool=*/true);
	polardb_count_reader_pool_attempt(
		thread, first_attempt_is_additional, result);
	if (!result.pool_busy) {
		return result;
	}

	PgSQL_PoolGetResult alternate_result =
		polardb_reader_pool_get_from_server(
			hgm, thread, alternate->srv, sess, pool_request,
			alternate->max_connections, /*skip_busy_pool=*/true);
	polardb_count_reader_pool_attempt(
		thread, /*additional=*/true, alternate_result);
	if (alternate_result.conn) {
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_busy_alternate_hit);
		if (acquired_server) {
			*acquired_server = alternate->srv;
		}
		return alternate_result;
	}

	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_busy_alternate_miss);
	result = polardb_reader_pool_get_from_server(
		hgm, thread, selected.srv, sess, pool_request,
		selected.max_connections, /*skip_busy_pool=*/false,
		/*skip_local_reuse=*/true);
	polardb_count_reader_pool_attempt(
		thread, /*additional=*/true, result);
	return result;
}

static int polardb_reader_pool_pick_weighted_node(
		PolarDB_ReaderNode* nodes, unsigned int num_nodes,
		uint64_t weight_sum) {
	if (!nodes || num_nodes == 0 || weight_sum == 0) {
		return -1;
	}
	const uint64_t random_value =
		(static_cast<uint64_t>(rand_fast()) << 32) |
		static_cast<uint64_t>(rand_fast());
	uint64_t k = random_value % weight_sum;
	k++;
	uint64_t running_sum = 0;
	for (unsigned int j = 0; j < num_nodes; j++) {
		running_sum += nodes[j].weight;
		if (k <= running_sum) {
			return (int)j;
		}
	}
	return (int)(num_nodes - 1);
}

/**
 * @brief Build the stable identity of one reader retry scope.
 *
 * Two waiting requests share a scope when a connection that satisfies one would
 * satisfy the other, so the identity covers everything that can change that
 * answer: the hostgroup, the pool match key of the request, the reader plan
 * (group LSN, byte lag cap, replica requirement), the wait target and type,
 * and the excluded endpoint. The value must stay the same across retries of the
 * same waiting session, which is why it is derived from the request rather than
 * assigned.
 *
 * 0 is reserved to mean "no scope", so a computed 0 is mapped to 1.
 *
 * @param hostgroup_id     Hostgroup the request routes to.
 * @param pool_request     Request whose pool match key identifies the pool.
 * @param reader_plan      Reader plan constraints of the request.
 * @param wait_spec        Wait target and type of the request.
 * @param exclude_address  Endpoint address to avoid, or null for none.
 * @param exclude_port     Endpoint port to avoid, negative for none.
 * @return A non-zero scope identity.
 */
static uint64_t polardb_reader_retry_scope_hash(
		unsigned int hostgroup_id,
		const PolarDB_PoolRequest& pool_request,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address, int exclude_port) {
	uint64_t hash = 1469598103934665603ULL;
	const PgSQL_PoolMatchKey match_key =
		polardb_core_pool_match_key_for_request(pool_request);
	hash = polardb_pool_hash_u64(hash, hostgroup_id);
	for (uint64_t word : match_key.words) {
		hash = polardb_pool_hash_u64(hash, word);
	}
	hash = polardb_pool_hash_u64(hash, reader_plan.group_lsn);
	hash = polardb_pool_hash_i32(hash, reader_plan.max_lag_bytes);
	hash = polardb_pool_hash_u64(hash, reader_plan.require_replica ? 1 : 0);
	hash = polardb_pool_hash_u64(hash, wait_spec.target);
	hash = polardb_pool_hash_i32(hash, static_cast<int>(wait_spec.type));
	hash = polardb_pool_hash_i32(hash, exclude_port);
	hash = polardb_pool_hash_cstr(hash, exclude_address);
	return hash == 0 ? 1 : hash;
}

/*
 * Busy results carry both the worker retry scope and the exact pool identity
 * needed if the wait later requests a concrete connection.
 */
static void polardb_set_reader_retry_identity(
		PolarDB_ReaderResult& result, unsigned int hostgroup_id,
		const PolarDB_PoolRequest& pool_request,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		const char* exclude_address, int exclude_port) {
	if (result.status != PolarDB_ReaderStatus::READER_BUSY &&
			result.status != PolarDB_ReaderStatus::READER_GROUP_BUSY) {
		return;
	}
	result.retry_scope_hash = polardb_reader_retry_scope_hash(
		hostgroup_id, pool_request, reader_plan, wait_spec,
		exclude_address, exclude_port);
	result.reservation_profile_generation =
		pool_request.startup_profile.generation(
			pool_request.startup_identity_mode);
	result.reservation_pool_key = pool_request.key;
}

/**
 * @brief Turn a set of evaluated candidates into an acquisition result by
 *        trying the selected node and, where allowed, the remaining ones.
 *
 * The selected node is attempted first, with the busy alternate passed down so
 * a contended pool mutex can be side-stepped. What happens after a failure
 * depends on the request:
 *  - a pooled-only request cannot create or reset a connection, so it walks the
 *    remaining nodes round-robin from the selected index until one yields a
 *    connection;
 *  - an ordinary request stops at the selected node, because the choice of
 *    server is the routing decision;
 *  - a request with confirm_reader_group_capacity walks the remaining nodes
 *    only after the selected one reported saturation. That walk stops as soon
 *    as some node is found not saturated, and keeps that node in result.srv
 *    rather than declaring the group full, since the caller can still create a
 *    connection there.
 *
 * @param hgm  Hostgroups manager owning the shared pool.
 * @param nodes  Candidate array.
 * @param num_nodes  Number of candidates.
 * @param selected_idx  Index of the chosen candidate. Out-of-range yields an
 *        empty result.
 * @param busy_alternate_idx  Index of the interchangeable second candidate, or
 *        negative when the request forbids serving from another reader.
 * @param sess  Session the connection is for.
 * @param pool_request  Pool identity and mode of the request.
 * @param confirm_reader_group_capacity  true when READER_GROUP_BUSY may be
 *        produced, which happens only when every node reported saturation.
 * @param first_attempt_is_additional  true when the first attempt is already a
 *        fallback, which affects counters and pool-miss reporting.
 * @return The result. On success result.conn is owned by the caller and
 *         result.srv names the server that served it. On failure result.srv is
 *         left naming the node the caller may create a connection on, and the
 *         caller's creation path depends on that.
 */
static PolarDB_ReaderResult polardb_take_conn_from_reader_nodes(
		PgSQL_HostGroups_Manager* hgm,
		PolarDB_ReaderNode* nodes, unsigned int num_nodes,
		int selected_idx, int busy_alternate_idx, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		bool confirm_reader_group_capacity,
		bool first_attempt_is_additional = false) {
	PolarDB_ReaderResult result;
	if (num_nodes == 0 || selected_idx < 0 ||
			selected_idx >= static_cast<int>(num_nodes)) {
		return result;
	}

	PgSQL_Thread* thread = sess ? sess->thread : NULL;
	result.srv = nodes[selected_idx].srv;
	const int alternate_idx =
		busy_alternate_idx >= 0 &&
			busy_alternate_idx < static_cast<int>(num_nodes) &&
			busy_alternate_idx != selected_idx
			? busy_alternate_idx : -1;
	auto try_node = [&](int idx) {
		const bool additional_attempt =
			first_attempt_is_additional || idx != selected_idx;
		PgSQL_SrvC* acquired_server = nodes[idx].srv;
		const PolarDB_ReaderNode* busy_alternate =
			alternate_idx >= 0 && alternate_idx != idx
				? &nodes[alternate_idx] : nullptr;
		PgSQL_PoolGetResult acquired =
			polardb_reader_acquire_with_alternate(
				hgm, thread, nodes[idx], busy_alternate, sess,
				pool_request, &acquired_server, additional_attempt);
		if (pool_request.only_pooled && !additional_attempt &&
				!acquired.conn && !acquired.pool_busy) {
			result.selected_pool_miss_server = nodes[idx].srv;
		}
#if POLARDB_DEBUG
		const PgSQL_PoolMatchKey trace_key =
			polardb_core_pool_match_key_for_request(pool_request);
		POLARDB_TRACE(
			"PolarDB READER_POOL: attempt=%s requested=%s:%u actual=%s:%u "
			"conn=%p source=%u pool_busy=%d saturated=%d target_ready=%d "
			"reader_lsn=%lu key=%lu/%lu/%lu/%lu\n",
			additional_attempt ? "additional" : "selected",
			nodes[idx].srv && nodes[idx].srv->address
				? nodes[idx].srv->address : "(null)",
			nodes[idx].srv ? nodes[idx].srv->port : 0,
			acquired_server && acquired_server->address
				? acquired_server->address : "(null)",
			acquired_server ? acquired_server->port : 0,
			(void*)acquired.conn, static_cast<unsigned int>(acquired.source),
			acquired.pool_busy, acquired.server_saturated,
			nodes[idx].target_reached, (unsigned long)nodes[idx].lsn,
			(unsigned long)trace_key.words[0],
			(unsigned long)trace_key.words[1],
			(unsigned long)trace_key.words[2],
			(unsigned long)trace_key.words[3]);
#endif // POLARDB_DEBUG
		if (!acquired.conn) {
#if POLARDB_PROFILE
			result.exact_match_reserved =
				result.exact_match_reserved ||
				acquired.exact_match_reserved;
#endif // POLARDB_PROFILE
			if (!pool_request.only_pooled) {
				result.server_saturated = acquired.server_saturated;
			}
			return false;
		}
		result.conn = acquired.conn;
		result.srv = acquired_server;
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
	} else if (!result.acquired() && confirm_reader_group_capacity &&
			result.server_saturated) {
		bool group_saturated = true;
		for (unsigned int offset = 1; offset < num_nodes; offset++) {
			const int fallback_idx =
				(selected_idx + static_cast<int>(offset)) %
				static_cast<int>(num_nodes);
			if (try_node(fallback_idx)) {
				group_saturated = false;
				break;
			}
			if (!result.server_saturated) {
				// This reader can create a connection. Preserve it for the
				// locked creation path instead of declaring the group full.
				result.srv = nodes[fallback_idx].srv;
				group_saturated = false;
				break;
			}
		}
		if (!result.acquired() && group_saturated) {
			result.status = PolarDB_ReaderStatus::READER_GROUP_BUSY;
		}
	}
	return result;
}

/**
 * @brief Choose a reader from a candidate set by power-of-two choices, then
 *        acquire a connection from it.
 *
 * Two candidates are drawn by configured weight, their shared active counts are
 * loaded, and the less loaded relative to its weight wins. Loading the counts
 * for only those two keeps the shared counters off the path for every other
 * configured reader.
 *
 * @param hgm  Hostgroups manager owning the shared pool.
 * @param nodes  Candidate array.
 * @param num_nodes  Number of candidates.
 * @param weight_sum  Sum of the candidate weights. Zero yields an empty result.
 * @param sess  Session the connection is for.
 * @param pool_request  Pool identity and mode of the request.
 * @param pair_preselected  true when the caller already drew exactly two
 *        equal-weight candidates, so this only compares their load instead of
 *        sampling again.
 * @param confirm_reader_group_capacity  true when READER_GROUP_BUSY may be
 *        produced after every node reports saturation.
 * @param avoid_busy_pool  true to make the losing candidate of the comparison
 *        the busy-pool alternate, which lets the request be served by that
 *        reader when the winner's pool mutex is contended.
 * @param first_attempt_is_additional  true when this whole attempt is a
 *        fallback: it reclassifies the profiling counters and suppresses the
 *        selected-pool-miss signal.
 * @return The acquisition result, as produced by
 *         polardb_take_conn_from_reader_nodes.
 */
static PolarDB_ReaderResult polardb_get_conn_from_reader_nodes(
		PgSQL_HostGroups_Manager* hgm,
		PolarDB_ReaderNode* nodes, unsigned int num_nodes,
		uint64_t weight_sum, PgSQL_Session* sess,
		const PolarDB_PoolRequest& pool_request,
		bool pair_preselected,
		bool confirm_reader_group_capacity,
		bool avoid_busy_pool,
		bool first_attempt_is_additional = false) {
	PolarDB_ReaderResult result;
	if (num_nodes == 0 || weight_sum == 0) {
		return result;
	}

	PgSQL_Thread* thread = sess ? sess->thread : NULL;
	// "Power of two choices" first chooses two candidates using their configured
	// weights, then compares active connections relative to those weights. Pool
	// inventory and socket limits are enforced later, during acquisition and
	// warmup. When pair_preselected is true, the caller has already chosen two
	// equal-weight candidates, so only compare their load.
	const bool compare_preselected_pair = pair_preselected && num_nodes == 2;
	int first_idx = 0;
	if (num_nodes > 1 && !compare_preselected_pair) {
		first_idx = polardb_reader_pool_pick_weighted_node(
			nodes, num_nodes, weight_sum);
	}
	int second_idx = compare_preselected_pair ? 1 : -1;
	int selected_idx = first_idx;
	const bool use_p2c = compare_preselected_pair ||
		num_nodes > 1;
	if (use_p2c && num_nodes > 1 && first_idx >= 0) {
		if (!compare_preselected_pair) {
			second_idx = polardb_reader_pool_pick_weighted_node(
				nodes, num_nodes, weight_sum);
			if (second_idx == first_idx) {
				second_idx = (first_idx + 1 +
					(int)(rand_fast() % (num_nodes - 1))) % (int)num_nodes;
			}
		}
		// Load shared USED counts only for the pair that P2C compares. Reader
		// eligibility and weighted sampling do not consume this value.
		nodes[first_idx].active_count =
			polardb_reader_selection_used_count(
				thread, nodes[first_idx].srv);
		nodes[second_idx].active_count =
			polardb_reader_selection_used_count(
				thread, nodes[second_idx].srv);
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_select);
		// Selection compares only global server load. Neither worker-local cache
		// history nor matching connection counts are allowed to choose the server.
		const int load_order = polardb_reader_normalized_load_compare(
			nodes[second_idx], nodes[first_idx]);
		bool take_second = false;
		if (load_order != 0) {
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_active_load);
			take_second = load_order < 0;
		} else {
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_random);
			take_second = (rand_fast() & 1) == 0;
		}
		if (take_second) {
			selected_idx = second_idx;
			POLARDB_THREAD_COUNT_ONE(thread, reader_pool_p2c_second);
		}
	}
	const int busy_alternate_idx =
		avoid_busy_pool && second_idx >= 0
			? (selected_idx == first_idx ? second_idx : first_idx)
			: -1;
	return polardb_take_conn_from_reader_nodes(
		hgm, nodes, num_nodes, selected_idx, busy_alternate_idx, sess,
		pool_request, confirm_reader_group_capacity,
		first_attempt_is_additional);
}

/**
 * @brief Select an eligible reader of a hostgroup and acquire a pooled
 *        connection from it, without creating one.
 *
 * Candidates are evaluated from the current server list snapshot: status,
 * weight, latency and connection limit, the replica requirement, the excluded
 * endpoint, and the LSN sample when the request has a wait target or a lag cap.
 * A request with a wait target prefers the readers that already reached it, so
 * the query can skip the server-side wait, and only falls back to the remaining
 * readers when none has.
 *
 * The server that ends up serving the request is not always the one selected.
 * When the request makes its readers interchangeable, a contended per-server
 * pool mutex hands the read to the alternate reader instead of blocking on it,
 * and a group-capacity check probes the remaining readers once the selected one
 * reports saturation. result.srv always names the server that actually served
 * the request, or, on failure, the server the caller may create a connection
 * on.
 *
 * On success result.selected_server_snapshot holds the snapshot the selection
 * was made against; the caller must move it onto the acquired connection, since
 * that snapshot is what keeps result.srv valid.
 *
 * @param hostgroup_id  Reader hostgroup to select from.
 * @param sess          Session the connection is for. Must not be null.
 * @param reader_plan   Replica requirement and lag cap for this read.
 * @param wait_spec     LSN wait target and type, empty when the read has none.
 * @param only_pooled   true to use only an existing pooled connection, never a
 *                      reset or a creation.
 * @param exclude_address  Endpoint address to avoid, or null/empty for none.
 * @param exclude_port     Endpoint port to avoid, negative for none.
 * @param confirm_reader_group_capacity  true to probe every eligible reader
 *        before reporting READER_GROUP_BUSY.
 * @return The result. result.acquired() reports success and result.conn is then
 *         owned by the caller. Otherwise result.status carries the reason,
 *         where READER_BUSY and READER_GROUP_BUSY additionally carry the retry
 *         scope and reservation identity for a waiting session.
 */
static PolarDB_ReaderResult polardb_try_acquire_reader_connection(
		PgSQL_HostGroups_Manager* hgm, unsigned int hostgroup_id,
		PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address, int exclude_port,
		bool confirm_reader_group_capacity) {
	PolarDB_ReaderResult result;
	PgSQL_Thread* thread = sess ? sess->thread : NULL;
	if (!hgm || !sess) {
		return result;
	}
	const auto hg_config =
		hgm->get_polardb_hg_config(hostgroup_id);
	if (!hg_config.is_polardb_hostgroup) {
		return result;
	}
	const bool has_wait_target = wait_spec.has_wait();
	const bool lag_cap_enabled = reader_plan.lag_cap_enabled();
	const PolarDB_StartupProfile startup_profile =
		hgm->polardb_startup_profile_for_hostgroup(
			hostgroup_id, pgsql_thread___polardb_proxy_protocol);
	if (has_wait_target && !startup_profile.requests_rfq_lsn()) {
		result.status = PolarDB_ReaderStatus::RFQ_UNAVAILABLE;
		return result;
	}
	if (lag_cap_enabled && reader_plan.group_lsn == 0) {
		result.status = PolarDB_ReaderStatus::GROUP_LSN_UNKNOWN;
		return result;
	}

	PolarDB_PoolRequest pool_request =
		polardb_prepare_pool_request_for_session(
			startup_profile, only_pooled,
			/*require_rfq_profile=*/wait_spec.target != 0, sess);
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_lookup);

	const uint64_t now_us =
		(has_wait_target || lag_cap_enabled) ? monotonic_time() : 0;
	bool freshness_clamped = false;
	const uint32_t fresh_ms = polardb_effective_lsn_freshness_ms(
		pgsql_thread___polardb_reader_lsn_max_age_ms,
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
			filter_status = polardb_reader_status_prefer(
				filter_status, PolarDB_ReaderStatus::READER_LSN_UNKNOWN);
			return false;
		}
		if (!reader_lsn_fresh) {
			POLARDB_THREAD_COUNT_ONE(thread, lsn_stale_count);
			POLARDB_THREAD_COUNT_ONE(thread, lag_cap_lsn_stale);
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
	 * Selection reads the current topology plus global status, LSN and weight.
	 * Multi-reader load comparison also reads the active count. Pool contents
	 * do not enter that choice.
	 * Acquisition may still end up on a different server than the one selected:
	 * an ordinary read whose readers are interchangeable is handed to the
	 * alternate reader when the selected reader's pool mutex is contended, a
	 * group-capacity check probes the remaining readers once the selected one
	 * reports saturation, and a pooled-only read may try another otherwise
	 * eligible server after the first has no exact shared-pool match, because it
	 * cannot create or reset a connection.
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
	const uint64_t server_snapshot_generation = server_snapshot->generation;
	const auto* selected_server_snapshot = server_snapshot.get();
	const auto* writer_servers = reader_plan.require_replica
		? polardb_find_writer_server_list(
			hg_config, *selected_server_snapshot)
		: nullptr;
	auto server_allowed = [&](PgSQL_SrvC* srv) {
		return !reader_plan.require_replica ||
			polardb_reader_endpoint_is_known_replica(
				writer_servers, srv);
	};
	result.selected_server_snapshot = std::move(server_snapshot);

#if POLARDB_PROXY && POLARDB_DEBUG
	PolarDB_ReaderStatus debug_status = PolarDB_ReaderStatus::ACQUIRED;
	if (wait_spec.has_wait()) {
		if (polardb_debug_reader_acquire_fault("reader_busy")) {
			debug_status = PolarDB_ReaderStatus::READER_BUSY;
		} else if (polardb_debug_reader_acquire_fault(
				"reader_lsn_unknown")) {
			debug_status = PolarDB_ReaderStatus::READER_LSN_UNKNOWN;
		}
	}
	if (debug_status != PolarDB_ReaderStatus::ACQUIRED) {
		result.status = debug_status;
		polardb_set_reader_retry_identity(
			result, hostgroup_id, pool_request, reader_plan, wait_spec,
			exclude_address, exclude_port);
		POLARDB_TRACE(
			"PolarDB route smart: debug forced reader acquisition status=%s "
			"(reader_hg=%u consistency_target_lsn=%lu)\n",
			polardb_reader_status_name(result.status), hostgroup_id,
			(unsigned long)wait_spec.target);
		return result;
	}
#endif // POLARDB_PROXY && POLARDB_DEBUG

	static constexpr unsigned int POLARDB_READER_NODE_STACK_CAP = 32;
	PolarDB_ReaderNode nodes_static[POLARDB_READER_NODE_STACK_CAP];
	PolarDB_ReaderNode* nodes = nodes_static;
	unsigned int num_nodes = 0;
	unsigned int num_ready_nodes = 0;
	uint64_t weight_sum = 0;
	uint64_t ready_weight_sum = 0;
	const std::vector<
		PgSQL_HostGroups_Manager::PolarDB_ServerSnapshotEntry>& servers =
		hg_it->second.servers;
	std::atomic<uint64_t>* selection_start =
		hg_it->second.selection_start.get();
	const unsigned int num_servers = servers.size();
	static thread_local std::vector<PolarDB_ReaderNode> nodes_dynamic;
	if (num_servers > POLARDB_READER_NODE_STACK_CAP) {
		nodes_dynamic.resize(num_servers);
		nodes = nodes_dynamic.data();
	}
	auto evaluate_reader = [&](const PgSQL_HostGroups_Manager::
			PolarDB_ServerSnapshotEntry& entry,
			PolarDB_ReaderNode* node) {
		PgSQL_SrvC* srv = entry.srv;
		POLARDB_THREAD_COUNT_ONE(thread, reader_pool_server_considered);
		if (!polardb_reader_is_candidate(entry) ||
				!server_allowed(srv) ||
				(exclude_address && exclude_address[0] && exclude_port >= 0 &&
				 srv->address && strcmp(srv->address, exclude_address) == 0 &&
				 static_cast<int>(srv->port) == exclude_port)) {
			POLARDB_THREAD_COUNT_ONE(thread,
				reader_pool_server_skip_unusable);
			return false;
		}
		PolarDB_ReaderLsnSample sample;
		if (has_wait_target || lag_cap_enabled) {
			sample = srv->polardb_sample_lsn(now_us, fresh_ms);
		}
		polardb_count_reader_target_candidate(
			thread, wait_spec.target, sample.lsn, sample.fresh);
		if (!lag_allows_reader(sample.lsn, sample.fresh)) {
			return false;
		}
		*node = PolarDB_ReaderNode{
			srv, sample.lsn, static_cast<unsigned int>(entry.weight),
			static_cast<unsigned int>(entry.max_connections), 0,
			sample.fresh,
			sample.fresh && has_wait_target &&
				sample.lsn >= wait_spec.target};
		return true;
	};

	const bool use_two_reader_policy = num_servers == 2 && selection_start;
	bool two_first_ok = false;
	bool two_peer_evaluated = false;
	int two_peer_server_idx = -1;
	bool two_avoid_busy_pool = false;
	if (use_two_reader_policy) {
		const uint64_t weight0 = servers[0].weight > 0
			? static_cast<uint64_t>(servers[0].weight) : 0;
		const uint64_t weight1 = servers[1].weight > 0
			? static_cast<uint64_t>(servers[1].weight) : 0;
		const int first_server_idx = polardb_reader_select_weighted_index(
			thread, hostgroup_id, server_snapshot_generation,
			selection_start, weight0, weight1);
		two_peer_server_idx = 1 - first_server_idx;
		two_avoid_busy_pool =
			weight0 == weight1 &&
			polardb_request_can_skip_busy_pool(
				has_wait_target, lag_cap_enabled, only_pooled,
				confirm_reader_group_capacity,
				exclude_address, exclude_port);

		PolarDB_ReaderNode first{};
		two_first_ok = evaluate_reader(servers[first_server_idx], &first);
		if (two_first_ok) {
			nodes[num_nodes++] = first;
		}
		if (two_avoid_busy_pool || confirm_reader_group_capacity ||
				!two_first_ok ||
				(has_wait_target && !first.target_reached)) {
			PolarDB_ReaderNode peer{};
			two_peer_evaluated = true;
			if (evaluate_reader(servers[two_peer_server_idx], &peer)) {
				nodes[num_nodes++] = peer;
			}
		}
		for (unsigned int n = 0; n < num_nodes; n++) {
			weight_sum += nodes[n].weight;
			if (nodes[n].target_reached) {
				num_ready_nodes++;
				ready_weight_sum += nodes[n].weight;
			}
		}
	}

	bool sampled_pair = false;
	bool sampled_results_available = false;
	unsigned int sampled_idx[2] = {0, 0};
	bool sampled_ok[2] = {false, false};
	PolarDB_ReaderNode sampled_node[2]{};
	// With three or more equal-weight readers, first inspect a random pair.
	// A target-bearing request can stop at that pair when both readers
	// already reached the target. If either reader is behind, or either
	// sampled server cannot be used, inspect the complete list so a usable
	// or ready server elsewhere is not missed. Keep both evaluations so
	// full expansion observes and counts each sampled reader only once.
	if (!use_two_reader_policy && num_servers > 2 &&
		polardb_request_can_sample_pair(lag_cap_enabled, only_pooled,
			confirm_reader_group_capacity,
			exclude_address, exclude_port)) {
		const unsigned int common_weight =
			servers[0].weight > 0
				? static_cast<unsigned int>(servers[0].weight)
				: 0;
		bool equal_positive_weights = common_weight > 0;
		for (unsigned int n = 1; equal_positive_weights && n < num_servers;
			n++) {
			equal_positive_weights =
				servers[n].weight > 0 &&
				static_cast<unsigned int>(servers[n].weight) == common_weight;
		}
		if (equal_positive_weights) {
			sampled_idx[0] = rand_fast() % num_servers;
			sampled_idx[1] =
				(sampled_idx[0] + 1 + rand_fast() % (num_servers - 1)) %
				num_servers;
			sampled_results_available = true;
			sampled_ok[0] =
				evaluate_reader(servers[sampled_idx[0]], &sampled_node[0]);
			sampled_ok[1] =
				evaluate_reader(servers[sampled_idx[1]], &sampled_node[1]);
			if (sampled_ok[0] && sampled_ok[1] &&
					(!has_wait_target ||
					 (sampled_node[0].target_reached &&
					  sampled_node[1].target_reached))) {
				nodes[0] = sampled_node[0];
				nodes[1] = sampled_node[1];
				num_nodes = 2;
				weight_sum = static_cast<uint64_t>(common_weight) * 2;
				num_ready_nodes =
					(sampled_node[0].target_reached ? 1U : 0U) +
					(sampled_node[1].target_reached ? 1U : 0U);
				ready_weight_sum =
					static_cast<uint64_t>(common_weight) * num_ready_nodes;
				sampled_pair = true;
			}
		}
	}

	// When every configured server must be inspected, begin at a random
	// position. Later fallback searches follow this array order, so a fixed
	// starting point would repeatedly favor the same neighboring readers.
	const unsigned int scan_start =
		!sampled_pair && num_servers > 2 ? rand_fast() % num_servers : 0;
	for (unsigned int n = 0;
			!use_two_reader_policy && !sampled_pair && n < num_servers; n++) {
		const unsigned int server_idx = (scan_start + n) % num_servers;
		PolarDB_ReaderNode node{};
		bool node_ok = false;
		if (sampled_results_available && server_idx == sampled_idx[0]) {
			node = sampled_node[0];
			node_ok = sampled_ok[0];
		} else if (sampled_results_available &&
				server_idx == sampled_idx[1]) {
			node = sampled_node[1];
			node_ok = sampled_ok[1];
		} else {
			node_ok = evaluate_reader(servers[server_idx], &node);
		}
		if (!node_ok) {
			continue;
		}
		nodes[num_nodes] = node;
		num_nodes++;
		weight_sum += node.weight;
		if (node.target_reached) {
			num_ready_nodes++;
			ready_weight_sum += node.weight;
		}
	}
	if (has_wait_target && num_ready_nodes == 0) {
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread,
			reader_target_no_ready_candidate);
	}
	result.status = filter_status;
	if (use_two_reader_policy) {
		const int selected_idx = num_nodes == 2
			? polardb_reader_pool_refine_two_by_lsn_target(
				thread, nodes, has_wait_target)
			: (num_nodes == 1 ? 0 : -1);
		// Try the other reader without waiting for the selected reader's pool
		// mutex only when both readers are interchangeable for this request:
		// equal weights, no LSN target, no lag limit, no exclusion, not
		// pooled-only, and no full-group capacity check. Otherwise wait for
		// the selected reader.
		const int busy_alternate_idx =
			two_avoid_busy_pool && num_nodes == 2 ? 1 - selected_idx : -1;
		PolarDB_ReaderResult selected_result =
			polardb_take_conn_from_reader_nodes(
				hgm, nodes, num_nodes, selected_idx, busy_alternate_idx, sess,
				pool_request, confirm_reader_group_capacity);

		if (!selected_result.acquired() && only_pooled && two_first_ok &&
				!two_peer_evaluated) {
			PolarDB_ReaderNode peer{};
			two_peer_evaluated = true;
			if (evaluate_reader(servers[two_peer_server_idx], &peer)) {
				nodes[num_nodes++] = peer;
				PolarDB_ReaderResult fallback_result =
					polardb_take_conn_from_reader_nodes(
						hgm, &nodes[num_nodes - 1], 1,
						/*selected_idx=*/0, /*busy_alternate_idx=*/-1, sess,
						pool_request,
						/*confirm_reader_group_capacity=*/false,
						/*first_attempt_is_additional=*/true);
				fallback_result.selected_pool_miss_server =
					selected_result.selected_pool_miss_server;
#if POLARDB_PROFILE
				selected_result.exact_match_reserved =
					selected_result.exact_match_reserved ||
					fallback_result.exact_match_reserved;
#endif // POLARDB_PROFILE
				if (fallback_result.acquired()) {
					selected_result = std::move(fallback_result);
				}
			}
		}
		selected_result.selected_server_snapshot =
			std::move(result.selected_server_snapshot);
		result = std::move(selected_result);
		if (result.acquired()) {
			for (unsigned int n = 0; n < num_nodes; n++) {
				if (nodes[n].srv == result.srv) {
					result.wait_bypass_allowed = nodes[n].target_reached;
					break;
				}
			}
		}
	} else {
		if (has_wait_target && num_ready_nodes > 0) {
			unsigned int ready_idx = 0;
			for (unsigned int n = 0; n < num_nodes; n++) {
				if (nodes[n].target_reached) {
					std::swap(nodes[ready_idx], nodes[n]);
					ready_idx++;
				}
			}
		}
		if (has_wait_target && num_ready_nodes > 0 &&
				ready_weight_sum > 0) {
			PolarDB_ReaderResult preferred_result =
				polardb_get_conn_from_reader_nodes(
					hgm, nodes, num_ready_nodes, ready_weight_sum, sess,
					pool_request, false, confirm_reader_group_capacity,
					/*avoid_busy_pool=*/
					sampled_pair && num_ready_nodes == 2);
			if (preferred_result.acquired()) {
				preferred_result.selected_server_snapshot =
					std::move(result.selected_server_snapshot);
				result = std::move(preferred_result);
				result.wait_bypass_allowed = true;
			} else {
				result.srv = preferred_result.srv;
				result.selected_pool_miss_server =
					preferred_result.selected_pool_miss_server;
				result.server_saturated =
					preferred_result.server_saturated;
#if POLARDB_PROFILE
				result.exact_match_reserved =
					preferred_result.exact_match_reserved;
#endif // POLARDB_PROFILE
				result.status = preferred_result.status;
			}
		}
		const bool preferred_can_create =
			confirm_reader_group_capacity &&
			result.srv && !result.server_saturated &&
			result.status != PolarDB_ReaderStatus::READER_GROUP_BUSY;
		if (!result.acquired() && !preferred_can_create) {
			const bool preferred_attempted =
				has_wait_target && num_ready_nodes > 0;
			const unsigned int fallback_nodes = preferred_attempted
				? num_nodes - num_ready_nodes : num_nodes;
			const uint64_t fallback_weight_sum = preferred_attempted
				? weight_sum - ready_weight_sum : weight_sum;
			PolarDB_ReaderNode* fallback_set = preferred_attempted
				? nodes + num_ready_nodes : nodes;
			PolarDB_ReaderResult fallback_result =
				polardb_get_conn_from_reader_nodes(
					hgm, fallback_set, fallback_nodes,
					fallback_weight_sum, sess, pool_request,
					sampled_pair, confirm_reader_group_capacity,
					/*avoid_busy_pool=*/sampled_pair,
					/*first_attempt_is_additional=*/preferred_attempted);
			if (!fallback_result.selected_pool_miss_server) {
				fallback_result.selected_pool_miss_server =
					result.selected_pool_miss_server;
			}
#if POLARDB_PROFILE
			result.exact_match_reserved =
				result.exact_match_reserved ||
				fallback_result.exact_match_reserved;
#endif // POLARDB_PROFILE
			if (fallback_result.acquired()) {
				fallback_result.selected_server_snapshot =
					std::move(result.selected_server_snapshot);
				result = std::move(fallback_result);
				result.wait_bypass_allowed = false;
			} else if (!fallback_result.srv) {
				// Keep the ready subset's result when no fallback exists.
			} else if (!confirm_reader_group_capacity ||
					fallback_result.status !=
						PolarDB_ReaderStatus::READER_GROUP_BUSY) {
				result.srv = fallback_result.srv;
				result.server_saturated =
					fallback_result.server_saturated;
				result.status = fallback_result.status;
			} else if (result.status !=
					PolarDB_ReaderStatus::READER_GROUP_BUSY) {
				result.srv = fallback_result.srv;
				result.server_saturated =
					fallback_result.server_saturated;
				result.status = fallback_result.status;
			}
		}
	}

	if (has_wait_target) {
		result.selected_reader_lsn = 0;
		result.best_considered_reader_lsn = 0;
		result.selected_reader_lsn_fresh = false;
		result.best_considered_reader_lsn_fresh = false;
		for (unsigned int n = 0; n < num_nodes; n++) {
			polardb_merge_reader_lsn_observation(&result, &nodes[n]);
		}
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

	if (result.status != PolarDB_ReaderStatus::READER_GROUP_BUSY) {
		result.status = result.srv
			? (only_pooled ? PolarDB_ReaderStatus::RFQ_UNAVAILABLE
				: (result.server_saturated
					? PolarDB_ReaderStatus::READER_BUSY
					: PolarDB_ReaderStatus::READER_UNAVAILABLE))
			: filter_status;
	}
	polardb_set_reader_retry_identity(
		result, hostgroup_id, pool_request, reader_plan, wait_spec,
		exclude_address, exclude_port);
	POLARDB_THREAD_COUNT_ONE(thread, reader_pool_miss_empty);
	return result;
}

#if POLARDB_PROFILE
namespace {

class PolarDB_ReaderAcquireTimer {
public:
	explicit PolarDB_ReaderAcquireTimer(PgSQL_Thread* thread)
		: thread_(thread), started_at_us_(monotonic_time()) {}

	~PolarDB_ReaderAcquireTimer() {
		const unsigned long long finished_at_us = monotonic_time();
		POLARDB_PROFILE_THREAD_COUNT(
			thread_, reader_acquire_sum_us,
			finished_at_us >= started_at_us_
				? finished_at_us - started_at_us_ : 0);
		POLARDB_PROFILE_THREAD_COUNT_ONE(thread_, reader_acquire_count);
	}

private:
	PgSQL_Thread* thread_;
	unsigned long long started_at_us_;
};

} // namespace
#endif // POLARDB_PROFILE

PolarDB_ReaderResult PgSQL_PolarDB_ReaderPool::polardb_acquire_reader_connection(
		unsigned int _hid, PgSQL_Session* sess,
		const PolarDB_Query_ReaderPlan& reader_plan,
		const PolarDB_WaitSpec& wait_spec,
		bool only_pooled,
		const char* exclude_address, int exclude_port,
		bool confirm_reader_group_capacity) {
#if POLARDB_PROFILE
	// Count the full public acquisition operation, including every early result.
	PolarDB_ReaderAcquireTimer acquire_timer(sess ? sess->thread : nullptr);
#endif // POLARDB_PROFILE
	PolarDB_ReaderResult result = polardb_try_acquire_reader_connection(
		hgm_, _hid, sess,
		reader_plan, wait_spec, only_pooled,
		exclude_address, exclude_port, confirm_reader_group_capacity);
	if (result.acquired()) {
		if (only_pooled && result.selected_pool_miss_server && sess) {
			const int warmup_mode =
				sess->polardb_effective_txn_split_warmup_mode();
			if (warmup_mode ==
					static_cast<int>(PolarDB_TxnSplitWarmupMode::DEMAND) ||
					warmup_mode ==
					static_cast<int>(PolarDB_TxnSplitWarmupMode::BOTH)) {
				sess->polardb_request_txn_split_warmup(
					static_cast<int>(_hid), "selected_pool_miss",
					result.selected_pool_miss_server);
			} else {
				POLARDB_TRACE(
					"PolarDB WARMUP: selected reader pool miss request "
					"suppressed mode=%s reader_hg=%u server=%s:%u\n",
					polardb_txn_split_warmup_mode_name(warmup_mode), _hid,
					result.selected_pool_miss_server->address
						? result.selected_pool_miss_server->address : "",
					result.selected_pool_miss_server->port);
			}
		}
		result.conn->polardb_selected_server_snapshot =
			std::move(result.selected_server_snapshot);
		polardb_count_reader_target_result(sess, wait_spec, result);
	}
	if (result.status == PolarDB_ReaderStatus::READER_BUSY ||
			result.status == PolarDB_ReaderStatus::READER_GROUP_BUSY) {
		pgsql_pool_status_count_get(sess ? sess->thread : nullptr,
			&hgm_->status.pgconnpoll_get);
		return result;
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
		hgm_->polardb_startup_profile_for_hostgroup(
			_hid, pgsql_thread___polardb_proxy_protocol);
	PolarDB_PoolRequest request = polardb_prepare_pool_request_for_session(
		startup_profile, /*only_pooled=*/false,
		/*require_rfq_profile=*/wait_spec.target != 0, sess);
	const PgSQL_PoolMatchKey match_key =
		polardb_core_pool_match_key_for_request(request);
	const uint64_t selected_server_list_generation =
		hgm_->polardb_server_list_snapshot_generation(
			result.selected_server_snapshot);

	PgSQL_Connection* conn = nullptr;
	bool created = false;
	for (unsigned int attempt = 0;
			attempt < POLARDB_READER_POOL_SERVER_POP_SCAN_LIMIT; attempt++) {
		PgSQL_PoolGetResult got =
			hgm_->get_connection_from_selected_server(
				selected, _hid, match_key, sess,
				PgSQL_PoolGetMode::ALLOW_EXACT_MATCH |
					PgSQL_PoolGetMode::ALLOW_RESET |
					PgSQL_PoolGetMode::ALLOW_CREATE,
				/*selected_max_connections=*/0,
				selected_server_list_generation,
				pgsql_thread___polardb_startup_config_generation);
		if (got.retry_after_config_change) {
			POLARDB_THREAD_COUNT_ONE(
				sess ? sess->thread : nullptr,
				reader_pool_retry_after_config_change);
			result.status =
				PolarDB_ReaderStatus::RETRY_AFTER_CONFIG_CHANGE;
			return result;
		}
		if (!got.conn) {
			break;
		}
		if (got.source == PgSQL_PoolGetSource::CREATED) {
			conn = got.conn;
			created = true;
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
		polardb_set_reader_retry_identity(
			result, _hid, request, reader_plan, wait_spec,
			exclude_address, exclude_port);
		return result;
	}
	selected->update_max_connections_used();
	pgsql_pool_status_count_get_ok(sess ? sess->thread : nullptr,
		&hgm_->status.pgconnpoll_get_ok);
	result.conn = conn;
	conn->polardb_selected_server_snapshot =
		std::move(result.selected_server_snapshot);
	if (created) {
		PolarDB_StartupClientContext startup_client;
		(void)polardb_startup_client_from_session(sess, &startup_client);
		conn->set_polardb_startup_settings(
			startup_profile, request.startup_identity_mode,
			pgsql_thread___polardb_startup_config_generation,
			startup_client);
	}
	result.srv = selected;
	result.status = PolarDB_ReaderStatus::ACQUIRED;
	// A newly created backend has not confirmed the target LSN.  The query wrapper
	// is mandatory when wait_spec carries a target.
	result.wait_bypass_allowed = false;
	if (wait_spec.has_wait()) {
		POLARDB_THREAD_COUNT_ONE(
			sess ? sess->thread : nullptr, target_lsn_fallback_wait);
	}
	polardb_count_reader_target_result(sess, wait_spec, result);
	return result;
}


#endif // POLARDB_PROXY
