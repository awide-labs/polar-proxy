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
		"Wait-wrapped reads whose reader lost its backend connection") \
	T(queries_in_splittable_txn, "PolarDB_Queries_In_Splittable_Txn", \
		"proxysql_polardb_queries_in_splittable_txn_total", \
		"Queries planned while the current transaction had split-readable primary RFQ evidence") \
	T(queries_split_eligible, "PolarDB_Queries_Split_Eligible", \
		"proxysql_polardb_queries_split_eligible_total", \
		"In-transaction reads that passed the transaction-split planner checks") \
	T(xids_received, "PolarDB_XIDs_Received", \
		"proxysql_polardb_xids_received_total", \
		"Primary RFQs that reported transaction XIDs") \
	T(txn_became_splittable, "PolarDB_Txn_Became_Splittable", \
		"proxysql_polardb_txn_became_splittable_total", \
		"Transactions that became eligible for split reads") \
	T(txn_lost_splittable, "PolarDB_Txn_Lost_Splittable", \
		"proxysql_polardb_txn_lost_splittable_total", \
		"Transactions that lost split-readable state before commit") \
	T(txn_committed_with_split, "PolarDB_Txn_Committed_With_Split", \
		"proxysql_polardb_txn_committed_with_split_total", \
		"Transactions that committed after at least one split read") \
	T(txn_committed_no_split, "PolarDB_Txn_Committed_No_Split", \
		"proxysql_polardb_txn_committed_no_split_total", \
		"Split-readable transactions that committed without a split read") \
	T(split_reads_total, "PolarDB_Split_Reads_Total", \
		"proxysql_polardb_split_reads_total", \
		"Transaction-split read attempts") \
	T(split_reads_success, "PolarDB_Split_Reads_Success", \
		"proxysql_polardb_split_reads_success_total", \
		"Transaction-split reads completed on a replica") \
	T(split_reads_fallback, "PolarDB_Split_Reads_Fallback", \
		"proxysql_polardb_split_reads_fallback_total", \
		"Transaction-split reads never dispatched to a replica and run on the primary") \
	T(split_fallback_reader_unavailable, "PolarDB_Split_Fallback_Reader_Unavailable", \
		"proxysql_polardb_split_fallback_reader_unavailable_total", \
		"Transaction-split reader acquisition fell back because no reader was online or usable") \
	T(split_fallback_reader_busy, "PolarDB_Split_Fallback_Reader_Busy", \
		"proxysql_polardb_split_fallback_reader_busy_total", \
		"Transaction-split reader acquisition fell back because readers were at capacity or had no pooled match") \
	T(split_fallback_rfq_unavailable, "PolarDB_Split_Fallback_RFQ_Unavailable", \
		"proxysql_polardb_split_fallback_rfq_unavailable_total", \
		"Transaction-split reader acquisition fell back because no RFQ-LSN-capable reader backend was available") \
	T(split_fallback_primary_lsn_unknown, "PolarDB_Split_Fallback_Primary_LSN_Unknown", \
		"proxysql_polardb_split_fallback_primary_lsn_unknown_total", \
		"Transaction-split reader acquisition fell back because lag-cap policy had no primary LSN sample") \
	T(split_fallback_reader_lsn_unknown, "PolarDB_Split_Fallback_Reader_LSN_Unknown", \
		"proxysql_polardb_split_fallback_reader_lsn_unknown_total", \
		"Transaction-split reader acquisition fell back because lag-cap policy had no reader LSN sample") \
	T(split_fallback_reader_lsn_stale, "PolarDB_Split_Fallback_Reader_LSN_Stale", \
		"proxysql_polardb_split_fallback_reader_lsn_stale_total", \
		"Transaction-split reader acquisition fell back because the reader LSN sample was stale") \
	T(split_fallback_reader_lag_exceeded, "PolarDB_Split_Fallback_Reader_Lag_Exceeded", \
		"proxysql_polardb_split_fallback_reader_lag_exceeded_total", \
		"Transaction-split reader acquisition fell back because byte lag exceeded max_lag_bytes") \
	T(split_reads_retried, "PolarDB_Split_Reads_Retried", \
		"proxysql_polardb_split_reads_retried_total", \
		"Transaction-split reader failures redispatched on the writer") \
	T(split_reads_retried_on_reader, "PolarDB_Split_Reads_Retried_On_Reader", \
		"proxysql_polardb_split_reads_retried_on_reader_total", \
		"Transaction-split reader failures redispatched on another replica") \
	T(split_reads_forwarded, "PolarDB_Split_Reads_Forwarded", \
		"proxysql_polardb_split_reads_forwarded_total", \
		"Transaction-split reader failures forwarded to the client while keeping the transaction on the writer") \
	T(split_reads_error, "PolarDB_Split_Reads_Error", \
		"proxysql_polardb_split_reads_error_total", \
		"Transaction-split reads that ended in an error path") \
	T(reader_terminations, "PolarDB_Reader_Terminations", \
		"proxysql_polardb_reader_terminations_total", \
		"Replica-reader failures that closed the client session") \
	T(split_rejected_multistatement, "PolarDB_Split_Rejected_Multistatement", \
		"proxysql_polardb_split_rejected_multistatement_total", \
		"Transaction-split candidates rejected because the query has multiple statements") \
	T(split_rejected_not_select, "PolarDB_Split_Rejected_Not_Select", \
		"proxysql_polardb_split_rejected_not_select_total", \
		"Transaction-split candidates rejected because the statement shape is not a split-safe SELECT") \
	T(split_rejected_for_update, "PolarDB_Split_Rejected_For_Update", \
		"proxysql_polardb_split_rejected_for_update_total", \
		"Transaction-split candidates rejected because the SELECT takes write locks") \
	T(split_rejected_write_lsn_unknown, "PolarDB_Split_Rejected_Write_LSN_Unknown", \
		"proxysql_polardb_split_rejected_write_lsn_unknown_total", \
		"Transaction-split candidates rejected because a prior write RFQ had no LSN") \
	T(split_rejected_observed_lsn_unknown, "PolarDB_Split_Rejected_Observed_LSN_Unknown", \
		"proxysql_polardb_split_rejected_observed_lsn_unknown_total", \
		"Transaction-split candidates rejected because a prior tracked read RFQ had no LSN") \
	T(split_wal_pending, "PolarDB_Split_WAL_Pending", \
		"proxysql_polardb_split_wal_pending_total", \
		"Transaction-split candidates rejected because primary RFQ reported WAL pending") \
	T(split_invariant_violations, "PolarDB_Split_Invariant_Violations", \
		"proxysql_polardb_split_invariant_violations_total", \
		"Unexpected transaction-split state-machine violations") \
	T(split_blocked_reads, "PolarDB_Split_Blocked_Reads", \
		"proxysql_polardb_split_blocked_reads_total", \
		"Transaction-split reads rejected because this transaction was blocked after a split fault") \
	T(split_no_backend, "PolarDB_Split_No_Backend", \
		"proxysql_polardb_split_no_backend_total", \
		"Transaction-split reads that could not get a replica backend") \
	T(split_send_failed, "PolarDB_Split_Send_Failed", \
		"proxysql_polardb_split_send_failed_total", \
		"Transaction-split reads whose wrapped query could not be sent") \
	T(split_pool_hit, "PolarDB_Split_Pool_Hit", \
		"proxysql_polardb_split_pool_hit_total", \
		"Transaction-split reads that acquired an existing pooled replica connection") \
	T(split_pool_empty, "PolarDB_Split_Pool_Empty", \
		"proxysql_polardb_split_pool_empty_total", \
		"Transaction-split reads that found no pooled replica connection") \
	T(split_pool_contention, "PolarDB_Split_Pool_Contention", \
		"proxysql_polardb_split_pool_contention_total", \
		"Transaction-split reads that could not use a pooled replica connection because the pool had no available match") \
	T(split_conn_reused, "PolarDB_Split_Conn_Reused", \
		"proxysql_polardb_split_conn_reused_total", \
		"Transaction-split reads that reused an already attached split backend connection") \
	T(split_conn_cleanup_success, "PolarDB_Split_Conn_Cleanup_Success", \
		"proxysql_polardb_split_conn_cleanup_success_total", \
		"Transaction-split replica connections returned cleanly to the pool") \
	T(split_conn_cleanup_failed, "PolarDB_Split_Conn_Cleanup_Failed", \
		"proxysql_polardb_split_conn_cleanup_failed_total", \
		"Transaction-split replica connections destroyed instead of returned to the pool") \
	T(split_lsn_wait_count, "PolarDB_Split_LSN_Wait_Count", \
		"proxysql_polardb_split_lsn_wait_count_total", \
		"LSN wait wrappers prepared for transaction-split reads") \
	T(split_lsn_wait_sum_us, "PolarDB_Split_LSN_Wait_Sum_Us", \
		"proxysql_polardb_split_lsn_wait_microseconds_total", \
		"Total transaction-split LSN wait time, in microseconds") \
	T(split_error_connection_lost, "PolarDB_Split_Error_Connection_Lost", \
		"proxysql_polardb_split_error_connection_lost_total", \
		"Transaction-split reads whose replica connection was lost") \
	T(split_error_query_failed, "PolarDB_Split_Error_Query_Failed", \
		"proxysql_polardb_split_error_query_failed_total", \
		"Transaction-split reads whose user query failed on the replica") \
	T(split_error_timeout, "PolarDB_Split_Error_Timeout", \
		"proxysql_polardb_split_error_timeout_total", \
		"Transaction-split wait timeout events accounted") \
	T(split_error_lsn_wait_timeout, "PolarDB_Split_Error_LSN_Wait_Timeout", \
		"proxysql_polardb_split_error_lsn_wait_timeout_total", \
		"Transaction-split LSN wait-timeout events accounted") \
	T(split_latency_sum_us, "PolarDB_Split_Latency_Sum_Us", \
		"proxysql_polardb_split_latency_microseconds_total", \
		"Total transaction-split read latency, in microseconds") \
	T(split_latency_count, "PolarDB_Split_Latency_Count", \
		"proxysql_polardb_split_latency_count_total", \
		"Transaction-split read latency samples") \
	G(split_warmup_requested, "PolarDB_Split_Warmup_Requested", \
		"proxysql_polardb_split_warmup_requested_total", \
		"Lazy split pool warmup requests queued after a pool-empty split attempt") \
	G(split_warmup_created, "PolarDB_Split_Warmup_Created", \
		"proxysql_polardb_split_warmup_created_total", \
		"Lazy split pool warmup connections added to replica pools") \
	G(split_warmup_failed, "PolarDB_Split_Warmup_Failed", \
		"proxysql_polardb_split_warmup_failed_total", \
		"Lazy split pool warmup requests that could not create a connection") \
	G(split_warmup_sum_us, "PolarDB_Split_Warmup_Sum_Us", \
		"proxysql_polardb_split_warmup_microseconds_total", \
		"Total time from lazy split warmup request to pooled connection, in microseconds") \
	G(split_warmup_count, "PolarDB_Split_Warmup_Count", \
		"proxysql_polardb_split_warmup_count_total", \
		"Lazy split warmup latency samples") \
	G(warmup_pending, "PolarDB_Warmup_Pending", \
		"proxysql_polardb_warmup_pending", \
		"Current number of queued lazy split pool warmup requests")

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
