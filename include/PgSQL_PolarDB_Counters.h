#ifndef PROXYSQL_PGSQL_POLARDB_COUNTERS_H
#define PROXYSQL_PGSQL_POLARDB_COUNTERS_H

#if POLARDB_PROXY

// PolarDB counter metadata is intentionally shared by the SQL stats export,
// per-thread counter entries, worker-total merges, and Prometheus registration.
//
// THREAD counters have per-thread slots and are summed on read. GLOBAL counters
// stay as PgHGM->status atomics only. Prometheus uses ALL counters, but only
// THREAD counters may feed PolarDB_ThreadStatusVariable/stvar[].
//
// Important boundary: these lists describe counter metadata and generated
// external counter interfaces. They are not storage-layout definitions. Keep the
// PgHGM->status.polardb_* atomic fields explicit in PgSQL_HostGroups_Manager.h
// so reviewers can see comments, grouping, and cache-line/layout decisions next
// to the actual storage.
// Ordered operator-visible counter list. The first callback is used for
// thread-backed counters; the second is used for global-only counters. Keep this
// order aligned with stats_pgsql_global / docs so the operator-facing list is
// stable while storage remains explicit in PgSQL_HostGroups_Manager.h.
#define POLARDB_COUNTER_LIST(T, G) \
	T(server_lsn_updates_from_rfq, "PolarDB_Server_LSN_Updates_From_RFQ", \
		"proxysql_polardb_server_lsn_updates_from_rfq_total", \
		"RFQ-carried per-server LSN cache updates accepted") \
	G(lsn_updates_from_monitor, "PolarDB_LSN_Updates_From_Monitor", \
		"proxysql_polardb_lsn_updates_from_monitor_total", \
		"LSN advances observed by the monitor") \
	G(monitor_health_invalid_role, "PolarDB_Monitor_Health_Invalid_Role", \
		"proxysql_polardb_monitor_health_invalid_role_total", \
		"Monitor health rows reporting a role ProxySQL cannot route to, including POLAR_UNKNOWN/POLAR_STANDALONE_DATAMAX") \
	G(monitor_health_invalid_values, "PolarDB_Monitor_Health_Invalid_Values", \
		"proxysql_polardb_monitor_health_invalid_values_total", \
		"Monitor health rows with invalid availability or LSN text") \
	T(lsn_stale_count, "PolarDB_LSN_Stale_Count", \
		"proxysql_polardb_lsn_stale_count_total", \
		"Reader candidates skipped because their cached LSN is below the session target") \
	T(write_missing_lsn, "PolarDB_Write_Missing_LSN", \
		"proxysql_polardb_write_missing_lsn_total", \
		"Writer queries whose RFQ did not include an LSN") \
	T(read_missing_lsn, "PolarDB_Read_Missing_LSN", \
		"proxysql_polardb_read_missing_lsn_total", \
		"Reader queries whose RFQ did not include an LSN") \
	T(primary_lsn_unknown, "PolarDB_Primary_LSN_Unknown", \
		"proxysql_polardb_primary_lsn_unknown_total", \
		"Primary-baseline reads that could not use a known primary LSN") \
	T(rfq_best_effort_degraded_routes, "PolarDB_RFQ_Best_Effort_Degraded_Routes", \
		"proxysql_polardb_rfq_best_effort_degraded_routes_total", \
		"Best-effort RFQ-unavailable reads routed without an LSN wait") \
	T(consistency_writer_fallback, "PolarDB_Consistency_Writer_Fallback", \
		"proxysql_polardb_consistency_writer_fallback_total", \
		"Consistency reads redirected to the writer after reader acquisition") \
	G(wait_reads_retried_on_writer, "PolarDB_Wait_Reads_Retried_On_Writer", \
		"proxysql_polardb_wait_reads_retried_on_writer_total", \
		"Wait reads retried once on the writer") \
	T(rfq_profile_skipped, "PolarDB_RFQ_Profile_Skipped", \
		"proxysql_polardb_rfq_profile_skipped_total", \
		"Pooled backends skipped because their startup profile cannot return RFQ LSN") \
	G(rfq_profile_evicted, "PolarDB_RFQ_Profile_Evicted", \
		"proxysql_polardb_rfq_profile_evicted_total", \
		"Incompatible pooled backends evicted for RFQ-LSN-capable replacements") \
	T(tl_cache_bypassed_for_target, "PolarDB_TL_Cache_Bypassed_For_Target", \
		"proxysql_polardb_tl_cache_bypassed_for_target_total", \
		"Thread-local cache bypasses for consistency-target RFQ-LSN reads") \
	T(target_lsn_preferred, "PolarDB_Target_LSN_Preferred", \
		"proxysql_polardb_target_lsn_preferred_total", \
		"Reader choices that preferred a cached LSN already at or above target") \
	T(target_lsn_fallback_wait, "PolarDB_Target_LSN_Fallback_Wait", \
		"proxysql_polardb_target_lsn_fallback_wait_total", \
		"Reader choices that kept the backend wait as the correctness gate") \
	G(session_target_epoch_reset, "PolarDB_Session_Target_Epoch_Reset", \
		"proxysql_polardb_session_target_epoch_reset_total", \
		"Session LSN targets cleared after writer epoch changes") \
	T(session_lsn_routing, "PolarDB_Session_LSN_Routing", \
		"proxysql_polardb_session_lsn_routing_total", \
		"Reads routed to a reader with a session-LSN wait requirement") \
	T(wait_wrap_prepared, "PolarDB_Wait_Wrap_Prepared", \
		"proxysql_polardb_wait_wrap_prepared_total", \
		"Wait wrappers prepared for replica reads") \
	T(wait_wrap_bypassed, "PolarDB_Wait_Wrap_Bypassed", \
		"proxysql_polardb_wait_wrap_bypassed_total", \
		"Wait wrappers skipped because the selected reader already reached the consistency target LSN") \
	G(wait_wrap_safety_abort, "PolarDB_Wait_Wrap_Safety_Abort", \
		"proxysql_polardb_wait_wrap_safety_abort_total", \
		"Wrap build failures that aborted the wait") \
	T(wait_lsn_sent, "PolarDB_Wait_LSN_Sent", \
		"proxysql_polardb_wait_lsn_sent_total", \
		"LSN wait wrappers successfully sent") \
	T(wait_lsn_sum_us, "PolarDB_Wait_LSN_Sum_Us", \
		"proxysql_polardb_wait_lsn_microseconds_total", \
		"Total time spent in LSN waits, in microseconds") \
	T(wait_error_timeout, "PolarDB_Wait_Error_Timeout", \
		"proxysql_polardb_wait_error_timeout_total", \
		"Total wait-timeout events accounted") \
	T(wait_error_lsn_wait_timeout, "PolarDB_Wait_Error_LSN_Wait_Timeout", \
		"proxysql_polardb_wait_error_lsn_wait_timeout_total", \
		"LSN wait-timeout events accounted") \
	T(wait_error_connection_lost, "PolarDB_Wait_Error_Connection_Lost", \
		"proxysql_polardb_wait_error_connection_lost_total", \
		"Wait-wrapped reads whose reader lost its backend connection")

#define POLARDB_COUNTER_LIST_SKIP(name, display_name, prom_name, help)

#define POLARDB_THREAD_COUNTER_LIST(X) \
	POLARDB_COUNTER_LIST(X, POLARDB_COUNTER_LIST_SKIP)

#define POLARDB_GLOBAL_COUNTER_LIST(X) \
	POLARDB_COUNTER_LIST(POLARDB_COUNTER_LIST_SKIP, X)

#define POLARDB_ALL_COUNTER_LIST(X) \
	POLARDB_COUNTER_LIST(X, X)

// Keep the counts derived from the lists so a counter add/remove has one
// source of truth. The unit tests still assert the current expected shape.
#define POLARDB_COUNT_ONE(name, display_name, prom_name, help) + 1
static constexpr int POLARDB_THREAD_COUNTER_COUNT =
	0 POLARDB_THREAD_COUNTER_LIST(POLARDB_COUNT_ONE);
static constexpr int POLARDB_GLOBAL_COUNTER_COUNT =
	0 POLARDB_GLOBAL_COUNTER_LIST(POLARDB_COUNT_ONE);
#undef POLARDB_COUNT_ONE

static constexpr int POLARDB_ALL_COUNTER_COUNT =
	POLARDB_THREAD_COUNTER_COUNT + POLARDB_GLOBAL_COUNTER_COUNT;

#endif // POLARDB_PROXY

#endif // PROXYSQL_PGSQL_POLARDB_COUNTERS_H
