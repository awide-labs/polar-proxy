/**
 * @file PgSQL_PolarDB_Topology.cpp
 * @brief PolarDB topology snapshots, server lifetime, and writer epochs.
 */

#include "PgSQL_HostGroups_Manager.h"
#include "PgSQL_PolarDB.h"
#include "PgSQL_Connection.h"
#include "PgSQL_Thread.h"
#include "proxysql.h"
#include "cpp.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

extern PgSQL_Threads_Handler* GloPTH;

#if POLARDB_PROXY
/**
 * @brief Take the PolarDB fast-topology lock for writing.
 *
 * This lock guards the shared server-list snapshot, its generation counter,
 * the retired server-list snapshots and the retired PgSQL_SrvC list. It is a
 * second lock independent of the HostGroups_Manager lock: the caller must
 * already hold the HGM write lock before taking it, and must never take the two
 * in the opposite order.
 *
 * There is no read-side companion. Readers never take this lock; they load the
 * current snapshot from the atomic shared_ptr instead, so the rwlock is
 * used write-only.
 *
 * The lock is a raw pthread rwlock with no RAII wrapper and is not recursive.
 * Every acquisition must be released with polardb_fast_topology_unlock().
 */
void PgSQL_HostGroups_Manager::polardb_fast_topology_wrlock() {
	pthread_rwlock_wrlock(&polardb_fast_topology_lock);
}

void PgSQL_HostGroups_Manager::polardb_fast_topology_unlock() {
	pthread_rwlock_unlock(&polardb_fast_topology_lock);
}

/**
 * @brief Stop making new PolarDB snapshots visible to other threads and release
 *        every retired server.
 *
 * Unlike the rest of this file, this call takes both the HGM write lock and the
 * fast-topology write lock itself, so the caller must hold neither. It clears
 * the shared server-list and topology snapshots, bumps both generations so
 * every thread-local cache is invalidated, and marks PolarDB inactive.
 *
 * All worker, idle, monitor and ping threads must already be joined. Any
 * snapshot that still has an owner outside the HGM at this point is a live
 * reference into objects about to be freed, so it aborts the process rather
 * than continuing. After the checks pass, every retired PgSQL_SrvC is deleted
 * once both locks are dropped.
 *
 * The call is idempotent: a second invocation returns without doing anything.
 */
void PgSQL_HostGroups_Manager::polardb_clear_snapshots_for_shutdown() {
	std::shared_ptr<const PolarDB_ServerListSnapshot> current_server_snapshot;
	std::shared_ptr<const PolarDB_TopologySnapshot> current_topology_snapshot;
	std::vector<PgSQL_SrvC*> retired_servers;

	wrlock();
	if (polardb_snapshots_cleared_for_shutdown_) {
		wrunlock();
		return;
	}
	polardb_snapshots_cleared_for_shutdown_ = true;
	polardb_fast_topology_wrlock();
	current_server_snapshot = std::atomic_load_explicit(
		&polardb_server_list_snapshot_, std::memory_order_acquire);
	current_topology_snapshot = std::atomic_load_explicit(
		&polardb_topology_snapshot_, std::memory_order_acquire);
	std::atomic_store_explicit(&polardb_server_list_snapshot_,
		std::shared_ptr<const PolarDB_ServerListSnapshot>{},
		std::memory_order_release);
	std::atomic_store_explicit(&polardb_topology_snapshot_,
		std::shared_ptr<const PolarDB_TopologySnapshot>{},
		std::memory_order_release);
	polardb_server_list_generation_.fetch_add(1, std::memory_order_release);
	polardb_topology_generation_.fetch_add(1, std::memory_order_release);
	status.polardb_active.store(false, std::memory_order_release);
	polardb_fast_topology_unlock();
	wrunlock();

	// HGM shutdown is reached only after worker, idle, monitor and ping threads
	// are joined. Refresh the calling thread as well so its TLS cache cannot
	// keep the final generation alive during teardown.
	polardb_refresh_thread_snapshots();

	wrlock();
	polardb_fast_topology_wrlock();
	if (current_server_snapshot && current_server_snapshot.use_count() != 1) {
		proxy_error(
			"PolarDB server-list snapshot still has external owners during HGM shutdown\n");
		abort();
	}
	if (current_topology_snapshot && current_topology_snapshot.use_count() != 1) {
		proxy_error(
			"PolarDB topology snapshot still has external owners during HGM shutdown\n");
		abort();
	}
	for (const auto& snapshot : polardb_retired_server_list_snapshots_) {
		if (snapshot && snapshot.use_count() != 1) {
			proxy_error(
				"PolarDB retired server-list snapshot still has external owners during HGM shutdown\n");
			abort();
		}
	}
	polardb_retired_server_list_snapshots_.clear();
	retired_servers.reserve(polardb_retired_servers_.size());
	for (PolarDB_RetiredServer& retired : polardb_retired_servers_) {
		if (retired.srv) {
			retired_servers.push_back(retired.srv);
			retired.srv = nullptr;
		}
	}
	polardb_retired_servers_.clear();
	polardb_fast_topology_unlock();
	wrunlock();

	for (PgSQL_SrvC* retired_server : retired_servers) {
		delete retired_server;
	}
}

/**
 * @brief Drop the calling thread's references to superseded PolarDB snapshots.
 *
 * Reclamation keys off use_count(): a retired server-list snapshot is erased
 * only when the HGM is its last owner, and a retired PgSQL_SrvC is freed only
 * once no retired snapshot can still reference it. A thread that keeps an old
 * snapshot in its thread-local caches therefore pins that generation and every
 * server retired under it for as long as it runs.
 *
 * This re-reads the current topology and server-list snapshots into the calling
 * thread's caches, which releases the previous ones. Every thread that reads
 * PolarDB snapshots must call it periodically, even when it has no work to do.
 */
void PgSQL_HostGroups_Manager::polardb_refresh_thread_snapshots() {
	(void)get_polardb_server_list_snapshot();
	(void)get_polardb_topology_snapshot_cached();
}

/**
 * @brief Return this thread's copy of the current PolarDB server-list snapshot.
 *
 * The call takes no lock. It compares the shared generation counter against the
 * calling thread's cached copy and reloads only when they differ, so a thread
 * that polls it repeatedly pays one atomic load.
 *
 * Each PolarDB_ServerSnapshotEntry holds a raw PgSQL_SrvC*, and the returned
 * shared_ptr is the only thing keeping those server objects alive. Hold it for
 * as long as any entry or server pointer taken from it is dereferenced. Do not
 * hold it longer than that: retired snapshots and retired servers are reclaimed
 * only once the HGM is their last owner, so a retained copy delays reclamation
 * and, at shutdown, aborts the process.
 *
 * @return The current snapshot, or an empty shared_ptr once
 *         polardb_clear_snapshots_for_shutdown() has run.
 */
std::shared_ptr<const PgSQL_HostGroups_Manager::PolarDB_ServerListSnapshot>
PgSQL_HostGroups_Manager::get_polardb_server_list_snapshot() const {
	const uint64_t generation =
		polardb_server_list_generation_.load(std::memory_order_acquire);
	static thread_local const PgSQL_HostGroups_Manager* cached_owner = nullptr;
	static thread_local uint64_t cached_generation = UINT64_MAX;
	static thread_local std::shared_ptr<const PolarDB_ServerListSnapshot>
		cached_snapshot;

	if (cached_owner != this) {
		cached_owner = this;
		cached_generation = UINT64_MAX;
		cached_snapshot.reset();
	}
	if (cached_generation != generation) {
		cached_snapshot = std::atomic_load_explicit(
			&polardb_server_list_snapshot_, std::memory_order_acquire);
		cached_generation = cached_snapshot
			? cached_snapshot->generation : generation;
	}
	return cached_snapshot;
}

bool PgSQL_HostGroups_Manager::polardb_hostgroup_has_usable_server(
		unsigned int hostgroup_id) const {
	const auto snapshot = get_polardb_server_list_snapshot();
	if (!snapshot) {
		return false;
	}
	const auto found = snapshot->by_hostgroup.find(hostgroup_id);
	if (found == snapshot->by_hostgroup.end()) {
		return false;
	}
	for (const PolarDB_ServerSnapshotEntry& entry : found->second.servers) {
		if (entry.srv && entry.weight > 0 && entry.max_connections > 0 &&
				entry.srv->polardb_fast_status_value() ==
					MYSQL_SERVER_STATUS_ONLINE &&
				entry.srv->polardb_pool_can_add_active_connection()) {
			return true;
		}
	}
	return false;
}

/**
 * @brief Read the generation stamped on a server-list snapshot.
 *
 * @param snapshot Snapshot previously obtained from
 *        get_polardb_server_list_snapshot(). The cast back to the snapshot type
 *        is unchecked, so passing any other shared_ptr is undefined behaviour
 *        and produces no diagnostic.
 * @return The snapshot generation, or 0 for a null pointer. A 0 result does not
 *         by itself identify a null argument, because it is indistinguishable
 *         from a snapshot whose generation is 0.
 */
uint64_t PgSQL_HostGroups_Manager::polardb_server_list_snapshot_generation(
		const std::shared_ptr<const void>& snapshot) const {
	const auto typed = std::static_pointer_cast<
		const PolarDB_ServerListSnapshot>(snapshot);
	return typed ? typed->generation : 0;
}

/**
 * @brief Return whether a pooled backend still matches the current PolarDB
 *        startup configuration and topology.
 *
 * This runs after asynchronous connect and on connection reuse, so it takes no
 * HGM lock. It reads only the calling thread's topology and server-list
 * snapshots, the global startup configuration and per-server atomics, and it
 * refreshes the calling thread's cached server-list snapshot as a side effect.
 *
 * A connection is accepted only when its installed startup settings still
 * matches the current startup generation, identity mode, profile and configured
 * fallback identity, and when its server is still listed for the expected
 * hostgroup with a positive weight, a positive connection limit and ONLINE
 * status.
 *
 * @param conn Pooled connection to check. A null connection is rejected.
 * @param expected_hostgroup_id Hostgroup the connection is about to serve.
 * @return true when the connection may still be used, which includes the case
 *         of a connection with no PolarDB startup settings installed: nothing
 *         was negotiated for it, so nothing can be stale. false in every other
 *         case, including an uninitialised GloPTH.
 */
bool PgSQL_HostGroups_Manager::polardb_reader_connection_is_current(
		const PgSQL_Connection* conn, unsigned int expected_hostgroup_id) {
	if (!conn) {
		return false;
	}
	if (!conn->polardb_startup_settings_set) {
		return true;
	}
	if (!GloPTH) {
		return false;
	}

	const PolarDB_ParsedGlobalConfigValue global_config =
		GloPTH->get_polardb_global_config();
	if (conn->polardb_startup_config_generation !=
			global_config.startup.generation) {
		return false;
	}
	const int identity_mode =
		static_cast<int>(global_config.startup.identity_mode);
	if (conn->polardb_startup_identity_mode != identity_mode ||
			conn->polardb_startup_profile_generation !=
				conn->polardb_startup_profile.generation(identity_mode)) {
		return false;
	}

	const auto hg_config =
		get_polardb_hg_config(expected_hostgroup_id);
	if (!hg_config.is_polardb_hostgroup) {
		return false;
	}
	const bool profile_off =
		polardb_profile_from_int(global_config.profile) ==
			PolarDB_Profile::OFF;
	const int protocol = profile_off
		? static_cast<int>(PolarDB_ProxyProtocol::OFF)
		: (hg_config.policy.proxy_protocol >= 0
			? hg_config.policy.proxy_protocol
			: static_cast<int>(global_config.startup.proxy_protocol));
	const PolarDB_StartupProfile current_profile =
		PolarDB_StartupProfile::from_protocol(
			polardb_proxy_protocol_from_int(protocol));
	if (!polardb_startup_profile_compatible_for_reuse(
			conn->polardb_startup_profile, current_profile)) {
		return false;
	}

	const PolarDB_StartupIdentity& installed_identity =
		conn->polardb_startup_client.identity;
	if (conn->polardb_startup_profile.emits_startup_params()) {
		const bool client_identity = installed_identity.source ==
			PolarDB_StartupIdentitySource::CLIENT;
		const bool expect_client_identity =
			polardb_proxy_identity_mode_uses_client_identity(identity_mode);
		if (installed_identity.source == PolarDB_StartupIdentitySource::NONE ||
				client_identity != expect_client_identity) {
			return false;
		}
	}
	if (installed_identity.source ==
			PolarDB_StartupIdentitySource::CONFIGURED_FALLBACK &&
			(installed_identity.host !=
				global_config.startup.configured_identity.host ||
			 installed_identity.port !=
				global_config.startup.configured_identity.port)) {
		return false;
	}

	const auto current_servers = get_polardb_server_list_snapshot();
	// ReaderPool connections carry their own snapshot while they are outside
	// the core pool. A connection created through the classic core path does
	// not: its USED-list membership already prevents its parent server from
	// being retired. The current snapshot is sufficient for the membership and
	// ONLINE check in both cases.
	if (!current_servers || !conn->parent) {
		return false;
	}
	const auto hostgroup =
		current_servers->by_hostgroup.find(expected_hostgroup_id);
	if (hostgroup == current_servers->by_hostgroup.end()) {
		return false;
	}
	for (const PolarDB_ServerSnapshotEntry& entry : hostgroup->second.servers) {
		if (entry.srv == conn->parent) {
			return entry.weight > 0 && entry.max_connections > 0 &&
				conn->parent->polardb_fast_status_value() ==
					MYSQL_SERVER_STATUS_ONLINE;
		}
	}
	return false;
}

/**
 * @brief Erase retired server-list snapshots that no thread still references.
 *
 * The caller must hold polardb_fast_topology_lock write-side.
 *
 * A retired snapshot is dropped when it is null or when its use_count() is 1,
 * meaning the HGM's own entry is the last remaining owner and no thread can
 * still be reading it. Every other snapshot is kept, so a thread that never
 * refreshes its thread-local caches keeps its generation alive indefinitely.
 */
void PgSQL_HostGroups_Manager::
polardb_prune_retired_server_snapshots_under_fast_topology_lock() {
	for (auto it = polardb_retired_server_list_snapshots_.begin();
			it != polardb_retired_server_list_snapshots_.end();) {
		if (!*it || it->use_count() == 1) {
			it = polardb_retired_server_list_snapshots_.erase(it);
			continue;
		}
		++it;
	}
}

/**
 * @brief Hand back every retired server that no live snapshot can still name.
 *
 * The caller must hold polardb_fast_topology_lock write-side.
 *
 * Retired snapshots are pruned first, and the oldest generation still held by a
 * surviving snapshot becomes the cut-off. A retired server is released only
 * when it was retired before that generation, because any snapshot at or after
 * it may still contain the raw PgSQL_SrvC pointer.
 *
 * Ownership of every server appended to @p servers_to_delete passes to the
 * caller: the HGM clears its own reference and will never free them again. Do
 * not delete them while holding a lock: release both polardb_fast_topology_lock
 * and the HGM write lock, then delete.
 *
 * @param servers_to_delete Vector the reclaimed servers are appended to.
 *        Existing contents are left untouched.
 */
void PgSQL_HostGroups_Manager::
polardb_detach_reclaimable_retired_servers_under_fast_topology_lock(
		std::vector<PgSQL_SrvC*>& servers_to_delete) {
	polardb_prune_retired_server_snapshots_under_fast_topology_lock();
	uint64_t oldest_active_generation = UINT64_MAX;
	for (const auto& snapshot : polardb_retired_server_list_snapshots_) {
		oldest_active_generation =
			std::min(oldest_active_generation, snapshot->generation);
	}

	const uint64_t delete_before_generation =
		oldest_active_generation == UINT64_MAX
			? polardb_server_list_generation_.load(std::memory_order_relaxed) + 1
			: oldest_active_generation;
	auto out = polardb_retired_servers_.begin();
	for (auto it = polardb_retired_servers_.begin();
			it != polardb_retired_servers_.end(); ++it) {
		if (it->generation < delete_before_generation) {
			servers_to_delete.push_back(it->srv);
			it->srv = nullptr;
			continue;
		}
		if (out != it) {
			*out = *it;
		}
		++out;
	}
	polardb_retired_servers_.erase(out, polardb_retired_servers_.end());
}

/**
 * @brief Take ownership of a server removed from its hostgroup and hold it until
 *        no snapshot can still reference it.
 *
 * Shared server-list snapshots hold raw PgSQL_SrvC pointers, so a server
 * unlinked from its hostgroup cannot be deleted straight away. Record it here
 * together with the current server-list generation instead.
 *
 * The caller must hold polardb_fast_topology_lock write-side. The caller must
 * have already removed @p srv from its hostgroup, and must not delete it:
 * ownership passes to the HGM, which frees it from
 * polardb_detach_reclaimable_retired_servers_under_fast_topology_lock() once no
 * live snapshot generation can still name it.
 *
 * @param srv Server to retire. A null pointer is ignored.
 */
void PgSQL_HostGroups_Manager::
polardb_retire_server_under_fast_topology_lock(PgSQL_SrvC* srv) {
	if (!srv) {
		return;
	}
	polardb_retired_servers_.push_back(PolarDB_RetiredServer{
		srv,
		polardb_server_list_generation_.load(std::memory_order_acquire)});
}

/**
 * @brief Build a new PolarDB server-list snapshot and store it where every
 *        thread reads it.
 *
 * The caller must hold the HGM write lock, because this walks MyHostGroups and
 * every PgSQL_SrvC, and polardb_fast_topology_lock write-side, because it
 * replaces the shared snapshot and its generation. The only call that runs
 * without them is the one in the HGM constructor, before any other thread can
 * observe the snapshot.
 *
 * The previous snapshot is not freed. It moves onto the retired list so threads
 * still reading it stay valid, and is reclaimed later once they let go of it.
 *
 * The generation store is what makes the new snapshot visible: it is a release
 * store that every thread's cached-snapshot check acquire-loads, so it both
 * exposes the new snapshot and invalidates every thread-local cache. This call
 * stores the new snapshot unconditionally, with no shutdown check, so do not
 * call it once polardb_clear_snapshots_for_shutdown() has run.
 */
void PgSQL_HostGroups_Manager::
polardb_update_server_list_snapshot_under_hgm_and_fast_topology_locks() {
	auto next = std::make_shared<PolarDB_ServerListSnapshot>();
	next->generation =
		polardb_server_list_generation_.load(std::memory_order_relaxed) + 1;

	for (unsigned int i = 0; MyHostGroups && i < MyHostGroups->len; i++) {
		PgSQL_HGC* myhgc = static_cast<PgSQL_HGC*>(MyHostGroups->index(i));
		if (!myhgc || !myhgc->mysrvs) {
			continue;
		}
		PolarDB_ServerListEntry& entry = next->by_hostgroup[myhgc->hid];
		entry.selection_start = myhgc->polardb_reader_selection_start;
		std::vector<PolarDB_ServerSnapshotEntry>& servers = entry.servers;
		const unsigned int count = myhgc->mysrvs->cnt();
		servers.reserve(count);
		for (unsigned int j = 0; j < count; j++) {
			PgSQL_SrvC* srv = myhgc->mysrvs->idx(j);
			if (srv) {
				servers.push_back(PolarDB_ServerSnapshotEntry{
					srv, srv->weight, srv->max_connections, srv->max_latency_us});
			}
		}
	}

	std::shared_ptr<const PolarDB_ServerListSnapshot> old_snapshot =
		std::atomic_load_explicit(&polardb_server_list_snapshot_,
			std::memory_order_acquire);
	if (old_snapshot) {
		polardb_retired_server_list_snapshots_.push_back(old_snapshot);
	}
	std::shared_ptr<const PolarDB_ServerListSnapshot> current = next;
	std::atomic_store_explicit(&polardb_server_list_snapshot_, current,
		std::memory_order_release);
	polardb_server_list_generation_.store(next->generation,
		std::memory_order_release);
	polardb_prune_retired_server_snapshots_under_fast_topology_lock();
}

/**
 * @brief Encode the backend set of a writer hostgroup as a comparable string.
 *
 * The caller must hold the HGM write lock.
 *
 * Callers depend on the exact encoding: polardb_refresh_writer_epoch_under_hgm_write_lock()
 * compares two results verbatim to determine whether the writer changed. Servers
 * in MYSQL_SERVER_STATUS_OFFLINE_HARD are excluded; each remaining server
 * contributes one line of "address<TAB>port"; the lines are sorted and each is
 * terminated with a newline.
 *
 * @param writer_hostgroup_id Writer hostgroup to encode.
 * @return The encoded identity. An empty string means either that the hostgroup
 *         does not exist or that it has no non-OFFLINE_HARD server; the two
 *         cases are not distinguishable from the result.
 */
std::string PgSQL_HostGroups_Manager::polardb_writer_identity_under_hgm_write_lock(
		unsigned int writer_hostgroup_id) {
	PgSQL_HGC* hgc = MyHGC_find(writer_hostgroup_id);
	if (!hgc) {
		return {};
	}

	std::vector<std::string> identities;
	identities.reserve(hgc->mysrvs->cnt());
	for (unsigned int i = 0; i < hgc->mysrvs->cnt(); i++) {
		PgSQL_SrvC* srv = hgc->mysrvs->idx(i);
		if (!srv || srv->status == MYSQL_SERVER_STATUS_OFFLINE_HARD) {
			continue;
		}
		std::string identity = srv->address ? srv->address : "";
		identity.push_back('\t');
		identity += std::to_string(srv->port);
		identities.push_back(identity);
	}

	std::sort(identities.begin(), identities.end());
	std::string result;
	for (const std::string& identity : identities) {
		result += identity;
		result.push_back('\n');
	}
	return result;
}

/**
 * @brief Clear the cached LSN and freshness timestamp of every server in one
 *        hostgroup.
 *
 * The caller must hold the HGM write lock, because this walks the hostgroup's
 * server list.
 *
 * Per-server LSN state normally only advances: it is stored with a
 * compare-and-swap to the maximum so a stale observation can never lower it.
 * This call deliberately breaks that rule and zeroes both halves outright,
 * which is safe only because the cached positions belong to a WAL timeline that
 * is about to be abandoned. Use it only immediately before advancing the writer
 * epoch, never as a general-purpose reset.
 *
 * @param hostgroup_id Hostgroup to clear. An unknown hostgroup is ignored.
 */
void PgSQL_HostGroups_Manager::polardb_reset_lsn_cache_for_hostgroup_under_hgm_write_lock(
		unsigned int hostgroup_id) {
	PgSQL_HGC* hgc = MyHGC_find(hostgroup_id);
	if (!hgc) {
		return;
	}

	for (unsigned int i = 0; i < hgc->mysrvs->cnt(); i++) {
		PgSQL_SrvC* srv = hgc->mysrvs->idx(i);
		if (!srv) {
			continue;
		}
		polardb_reset_server_lsn_cache(
			srv->polardb_current_lsn, srv->lsn_updated_at);
	}
}

/**
 * @brief Detect a writer change for one pair and, if so, bump its writer epoch
 *        and drop stale LSN state.
 *
 * The "writer epoch" is a per-pair counter that marks the WAL timeline currently
 * in effect. It increments every time the writer's backend set changes (a
 * failover or a config edit). Sessions tag their saved LSN target with the epoch
 * they observed and discard the target when the epoch has moved on, so a read
 * never waits for an LSN that belongs to a previous writer.
 *
 * The first call after a writer becomes configured only records the current
 * identity; it does not count as a change. A later call that finds a different
 * identity clears the group LSN and the per-server LSN caches of both
 * the writer and reader hostgroups, then increments the epoch last.
 *
 * Precondition: caller must hold the HostGroups_Manager write lock.
 *
 * @param writer_hostgroup_id Writer hostgroup of the pair to examine. The call
 *        is a silent no-op when this hostgroup does not exist or has no
 *        configured replication pairing. Only the writer hostgroup of a pair
 *        carries that pairing, so passing a reader hostgroup id does nothing.
 * @param reason Short description recorded in the log line (may be null).
 */
void PgSQL_HostGroups_Manager::polardb_refresh_writer_epoch_under_hgm_write_lock(
		unsigned int writer_hostgroup_id, const char* reason) {
	PgSQL_HGC* writer_hgc = MyHGC_find(writer_hostgroup_id);
	if (!writer_hgc || !writer_hgc->repl_config.configured) {
		return;
	}

	std::string current_identity =
		polardb_writer_identity_under_hgm_write_lock(writer_hostgroup_id);
	if (!writer_hgc->repl_config.polardb_writer_identity_initialized) {
		writer_hgc->repl_config.polardb_writer_identity = current_identity;
		writer_hgc->repl_config.polardb_writer_identity_initialized = true;
		return;
	}

	if (current_identity == writer_hgc->repl_config.polardb_writer_identity) {
		return;
	}

	PgSQL_HGC* reader_hgc =
		MyHGC_find(writer_hgc->repl_config.reader_hostgroup);
	std::vector<PgSQL_SrvC*> cache_servers;
	cache_servers.reserve(
		writer_hgc->mysrvs->cnt() +
		(reader_hgc && reader_hgc != writer_hgc
			? reader_hgc->mysrvs->cnt() : 0));
	auto add_cache_servers = [&](PgSQL_HGC* hgc) {
		if (!hgc) return;
		for (unsigned int i = 0; i < hgc->mysrvs->cnt(); i++) {
			PgSQL_SrvC* srv = hgc->mysrvs->idx(i);
			if (srv && std::find(
					cache_servers.begin(), cache_servers.end(), srv) ==
					cache_servers.end()) {
				cache_servers.push_back(srv);
			}
		}
	};
	add_cache_servers(writer_hgc);
	if (reader_hgc != writer_hgc) {
		add_cache_servers(reader_hgc);
	}
	for (PgSQL_SrvC* srv : cache_servers) {
		srv->polardb_lock_lsn_cache();
	}

	writer_hgc->repl_config.polardb_writer_identity = current_identity;
	if (writer_hgc->repl_config.polardb_group_lsn) {
		writer_hgc->repl_config.polardb_group_lsn->store(0, std::memory_order_relaxed);
	}

	polardb_reset_lsn_cache_for_hostgroup_under_hgm_write_lock(writer_hostgroup_id);
	if (writer_hgc->repl_config.reader_hostgroup != writer_hostgroup_id) {
		polardb_reset_lsn_cache_for_hostgroup_under_hgm_write_lock(
			writer_hgc->repl_config.reader_hostgroup);
	}

	// Make the cache reset visible as the epoch boundary. Query threads
	// acquire-load this epoch before trusting any session target or group LSN.
	const uint64_t new_epoch = writer_hgc->repl_config.polardb_writer_epoch
		? writer_hgc->repl_config.polardb_writer_epoch->fetch_add(
			1, std::memory_order_acq_rel) + 1
		: 0;

	for (auto it = cache_servers.rbegin(); it != cache_servers.rend(); ++it) {
		(*it)->polardb_unlock_lsn_cache();
	}

	proxy_info(
		"PolarDB writer epoch advanced for writer HG %u to %lu after %s; "
		"cleared group and per-server LSN cache for the replication group\n",
		writer_hostgroup_id, (unsigned long)new_epoch,
		reason ? reason : "writer identity change");
}

/**
 * @brief Run the writer-epoch check for every configured PolarDB pair.
 *
 * The caller must hold the HGM write lock, as required by
 * polardb_refresh_writer_epoch_under_hgm_write_lock().
 *
 * Only hostgroups registered as a PolarDB writer are visited, that is, pairs
 * whose replication row has check_type 'polardb'. Other replication hostgroups
 * are not examined.
 *
 * @param reason Short description recorded in the log line of each pair whose
 *        epoch advances (may be null).
 */
void PgSQL_HostGroups_Manager::polardb_refresh_all_writer_epochs_under_hgm_write_lock(
		const char* reason) {
	for (const auto& writer_reader : polardb_writer_to_reader_) {
		polardb_refresh_writer_epoch_under_hgm_write_lock(writer_reader.first, reason);
	}
}

/**
 * @brief Return this thread's cached PolarDB topology snapshot.
 *
 * The call takes no lock. It compares the shared generation counter against the
 * calling thread's cached copy and reloads only when they differ, so the common
 * case is one generation load with no shared_ptr reference-count update.
 *
 * The returned pointer is valid until this thread performs another topology
 * snapshot lookup. Callers must use it only inside the current function and
 * must not store it.
 *
 * @return Pointer to the cached snapshot. It is null once
 *         polardb_clear_snapshots_for_shutdown() has run, so callers
 *         must check it before dereferencing.
 */
const PgSQL_HostGroups_Manager::PolarDB_TopologySnapshot*
PgSQL_HostGroups_Manager::get_polardb_topology_snapshot_cached() const {
	const uint64_t generation = polardb_topology_generation_.load(std::memory_order_acquire);
	static thread_local const PgSQL_HostGroups_Manager* cached_owner = nullptr;
	static thread_local uint64_t cached_generation = UINT64_MAX;
	static thread_local std::shared_ptr<const PolarDB_TopologySnapshot> cached_snapshot;

	if (cached_owner != this) {
		cached_owner = this;
		cached_generation = UINT64_MAX;
		cached_snapshot.reset();
	}

	if (cached_generation != generation) {
		auto snapshot = std::atomic_load_explicit(&polardb_topology_snapshot_,
			std::memory_order_acquire);
		cached_snapshot = snapshot;
		cached_generation = snapshot ? snapshot->generation : generation;
	}

	return cached_snapshot.get();
}

bool PgSQL_HostGroups_Manager::is_polardb_hostgroup(unsigned int hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return false;
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return false;
	bool result = snapshot->by_hostgroup.count(hostgroup_id) > 0;
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5,
		"PolarDB SNAPSHOT: is_polardb_hostgroup(%u) = %d (generation=%lu cache size=%zu)\n",
		hostgroup_id, result ? 1 : 0, (unsigned long)snapshot->generation,
		snapshot->by_hostgroup.size());
	return result;
}

int PgSQL_HostGroups_Manager::get_writer_hostgroup_for_reader(unsigned int reader_hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return -1;
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return -1;
	auto it = snapshot->by_hostgroup.find(reader_hostgroup_id);
	return (it != snapshot->by_hostgroup.end())
		? it->second.config.writer_hostgroup : -1;
}

int PgSQL_HostGroups_Manager::get_reader_hostgroup_for_writer(unsigned int writer_hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return -1;
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return -1;
	auto it = snapshot->by_hostgroup.find(writer_hostgroup_id);
	int result = (it != snapshot->by_hostgroup.end())
		? it->second.config.reader_hostgroup : -1;
	proxy_debug(PROXY_DEBUG_MYSQL_CONNPOOL, 5,
		"PolarDB SNAPSHOT: get_reader_hostgroup_for_writer(%u) = %d (generation=%lu cache size=%zu)\n",
		writer_hostgroup_id, result, (unsigned long)snapshot->generation,
		snapshot->by_hostgroup.size());
	return result;
}

PgSQL_HostGroups_Manager::PolarDB_HG_Config
PgSQL_HostGroups_Manager::get_polardb_hg_config(unsigned int hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return {};

	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return {};

	auto it = snapshot->by_hostgroup.find(hostgroup_id);
	if (it == snapshot->by_hostgroup.end()) return {};
	PolarDB_HG_Config config = it->second.config;
	if (it->second.writer_epoch) {
		config.writer_epoch =
			it->second.writer_epoch->load(std::memory_order_acquire);
	}
	return config;
}

PgSQL_HostGroups_Manager::PolarDB_HG_Policy PgSQL_HostGroups_Manager::get_polardb_hg_policy(unsigned int hostgroup_id) {
	if (!status.polardb_active.load(std::memory_order_relaxed)) return {};
	const auto snapshot = get_polardb_topology_snapshot_cached();
	if (!snapshot) return {};
	auto it = snapshot->by_hostgroup.find(hostgroup_id);
	return it != snapshot->by_hostgroup.end()
		? it->second.config.policy : PolarDB_HG_Policy{};
}

PgSQL_HGC* PgSQL_HostGroups_Manager::polardb_find_hostgroup(
		unsigned int hostgroup_id) {
	return MyHGC_find(hostgroup_id);
}

/**
 * @brief Resolve the startup profile a backend of this hostgroup must be opened
 *        with.
 *
 * @param hostgroup_id Hostgroup to resolve.
 * @param fallback_proxy_protocol Proxy protocol to use when the hostgroup
 *        policy leaves proxy_protocol unset (a negative value). It is ignored
 *        whenever the hostgroup policy sets one.
 * @return The profile derived from the effective proxy protocol. A hostgroup
 *         that is unknown, or that is not marked as a PolarDB hostgroup, yields
 *         the profile for PolarDB_ProxyProtocol::OFF and @p
 *         fallback_proxy_protocol is not consulted at all.
 */
PolarDB_StartupProfile PgSQL_HostGroups_Manager::polardb_startup_profile_for_hostgroup(
		unsigned int hostgroup_id, int fallback_proxy_protocol) {
	const auto config = get_polardb_hg_config(hostgroup_id);
	if (!config.is_polardb_hostgroup ||
			pgsql_thread___polardb_profile_off) {
		return PolarDB_StartupProfile::from_protocol(PolarDB_ProxyProtocol::OFF);
	}
	const int protocol = config.policy.proxy_protocol >= 0 ?
		config.policy.proxy_protocol : fallback_proxy_protocol;
	return PolarDB_StartupProfile::from_protocol(
		polardb_proxy_protocol_from_int(protocol));
}

bool PgSQL_HostGroups_Manager::polardb_hostgroup_requests_rfq_lsn(
		unsigned int hostgroup_id, int fallback_proxy_protocol) {
	return polardb_startup_profile_for_hostgroup(
		hostgroup_id, fallback_proxy_protocol).requests_rfq_lsn();
}
#endif // POLARDB_PROXY
