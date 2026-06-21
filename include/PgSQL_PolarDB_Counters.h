#ifndef PROXYSQL_PGSQL_POLARDB_COUNTERS_H
#define PROXYSQL_PGSQL_POLARDB_COUNTERS_H

#ifndef POLARDB_PROFILE
#define POLARDB_PROFILE 0
#endif

#if POLARDB_PROXY

#if POLARDB_PROFILE
#define POLARDB_PROFILE_THREAD_COUNTER_LIST(T) \
	T(wait_wrap_build_sum_us, "PolarDB_Wait_Wrap_Build_Sum_Us", \
		"proxysql_polardb_wait_wrap_build_microseconds_total", \
		"Total time spent building wait-wrapper SQL, in microseconds") \
	T(wait_wrap_build_count, "PolarDB_Wait_Wrap_Build_Count", \
		"proxysql_polardb_wait_wrap_build_count_total", \
		"Wait-wrapper SQL build latency samples") \
	T(wait_wrap_install_sum_us, "PolarDB_Wait_Wrap_Install_Sum_Us", \
		"proxysql_polardb_wait_wrap_install_microseconds_total", \
		"Total time spent installing wait-wrapper packets, in microseconds") \
	T(wait_wrap_install_count, "PolarDB_Wait_Wrap_Install_Count", \
		"proxysql_polardb_wait_wrap_install_count_total", \
		"Wait-wrapper packet install latency samples") \
	T(reader_acquire_sum_us, "PolarDB_Reader_Acquire_Sum_Us", \
		"proxysql_polardb_reader_acquire_microseconds_total", \
		"Total time spent in RFQ-aware reader acquisition, in microseconds") \
	T(reader_acquire_count, "PolarDB_Reader_Acquire_Count", \
		"proxysql_polardb_reader_acquire_count_total", \
		"RFQ-aware reader acquisition latency samples") \
	T(hgm_reader_lock_wait_sum_us, "PolarDB_HGM_Reader_Lock_Wait_Sum_Us", \
		"proxysql_polardb_hgm_reader_lock_wait_microseconds_total", \
		"Total time spent waiting for the HostGroups_Manager reader-acquire lock") \
	T(hgm_reader_lock_wait_count, "PolarDB_HGM_Reader_Lock_Wait_Count", \
		"proxysql_polardb_hgm_reader_lock_wait_count_total", \
		"HostGroups_Manager reader-acquire lock wait samples") \
	T(hgm_reader_lock_hold_sum_us, "PolarDB_HGM_Reader_Lock_Hold_Sum_Us", \
		"proxysql_polardb_hgm_reader_lock_hold_microseconds_total", \
		"Total time the HostGroups_Manager reader-acquire lock was held") \
	T(hgm_reader_lock_hold_count, "PolarDB_HGM_Reader_Lock_Hold_Count", \
		"proxysql_polardb_hgm_reader_lock_hold_count_total", \
		"HostGroups_Manager reader-acquire lock hold samples") \
	T(reader_target_ready_candidate, "PolarDB_Reader_Target_Ready_Candidate", \
		"proxysql_polardb_reader_target_ready_candidate_total", \
		"Reader candidates whose fresh LSN cache already reached the wait target") \
	T(reader_target_no_ready_candidate, "PolarDB_Reader_Target_No_Ready_Candidate", \
		"proxysql_polardb_reader_target_no_ready_candidate_total", \
		"Targeted reader acquisitions with no fresh target-reached candidate") \
	T(reader_target_lsn_unknown, "PolarDB_Reader_Target_LSN_Unknown", \
		"proxysql_polardb_reader_target_lsn_unknown_total", \
		"Reader candidates with no cached LSN while a wait target was present") \
	T(reader_target_lsn_stale, "PolarDB_Reader_Target_LSN_Stale", \
		"proxysql_polardb_reader_target_lsn_stale_total", \
		"Reader candidates whose cached LSN was too stale for target bypass") \
	T(reader_target_lsn_behind, "PolarDB_Reader_Target_LSN_Behind", \
		"proxysql_polardb_reader_target_lsn_behind_total", \
		"Fresh reader candidates whose cached LSN was behind the wait target") \
	T(reader_target_lag_cap_reject, "PolarDB_Reader_Target_Lag_Cap_Reject", \
		"proxysql_polardb_reader_target_lag_cap_reject_total", \
		"Reader candidates rejected by the byte-lag cap") \
	T(reader_target_rfq_unavailable, "PolarDB_Reader_Target_RFQ_Unavailable", \
		"proxysql_polardb_reader_target_rfq_unavailable_total", \
		"Targeted reader attempts rejected because RFQ-LSN feedback is unavailable") \
	T(reader_target_rfq_no_protocol, "PolarDB_Reader_Target_RFQ_No_Protocol", \
		"proxysql_polardb_reader_target_rfq_no_protocol_total", \
		"Targeted reader attempts where effective proxy protocol does not request RFQ LSN") \
	T(reader_target_rfq_no_client_context, "PolarDB_Reader_Target_RFQ_No_Client_Context", \
		"proxysql_polardb_reader_target_rfq_no_client_context_total", \
		"Targeted reader attempts missing frontend user or startup identity context") \
	T(reader_target_rfq_candidate_profile_mismatch, "PolarDB_Reader_Target_RFQ_Candidate_Profile_Mismatch", \
		"proxysql_polardb_reader_target_rfq_candidate_profile_mismatch_total", \
		"Pooled reader candidates skipped because their startup profile lacks required RFQ bits") \
	T(reader_target_rfq_candidate_identity_mismatch, "PolarDB_Reader_Target_RFQ_Candidate_Identity_Mismatch", \
		"proxysql_polardb_reader_target_rfq_candidate_identity_mismatch_total", \
		"Pooled reader candidates skipped because their PolarDB startup identity differs") \
	T(reader_target_rfq_candidate_auth_mismatch, "PolarDB_Reader_Target_RFQ_Candidate_Auth_Mismatch", \
		"proxysql_polardb_reader_target_rfq_candidate_auth_mismatch_total", \
		"Pooled reader candidates skipped because their user or database differs") \
	T(reader_target_rfq_unavailable_profile_mismatch, "PolarDB_Reader_Target_RFQ_Unavailable_Profile_Mismatch", \
		"proxysql_polardb_reader_target_rfq_unavailable_profile_mismatch_total", \
		"Targeted reader acquisitions that failed because only RFQ-profile-incompatible pooled readers were available") \
	T(reader_target_rfq_unavailable_identity_mismatch, "PolarDB_Reader_Target_RFQ_Unavailable_Identity_Mismatch", \
		"proxysql_polardb_reader_target_rfq_unavailable_identity_mismatch_total", \
		"Targeted reader acquisitions that failed because only startup-identity-incompatible pooled readers were available") \
	T(reader_target_rfq_unavailable_auth_mismatch, "PolarDB_Reader_Target_RFQ_Unavailable_Auth_Mismatch", \
		"proxysql_polardb_reader_target_rfq_unavailable_auth_mismatch_total", \
		"Targeted reader acquisitions that failed because only auth-incompatible pooled readers were available") \
	T(rfq_requested_missing_payload, "PolarDB_RFQ_Requested_Missing_Payload", \
		"proxysql_polardb_rfq_requested_missing_payload_total", \
		"Results on RFQ-LSN startup-profile connections whose ReadyForQuery carried no LSN payload") \
	T(rfq_requested_zero_payload, "PolarDB_RFQ_Requested_Zero_Payload", \
		"proxysql_polardb_rfq_requested_zero_payload_total", \
		"Results on RFQ-LSN startup-profile connections whose ReadyForQuery carried a zero LSN payload") \
	T(reader_target_pool_busy, "PolarDB_Reader_Target_Pool_Busy", \
		"proxysql_polardb_reader_target_pool_busy_total", \
		"Targeted reader attempts rejected by capacity, free-list, or creation throttles") \
	T(reader_target_best_behind_attempt, "PolarDB_Reader_Target_Best_Behind_Attempt", \
		"proxysql_polardb_reader_target_best_behind_attempt_total", \
		"Targeted reader acquisitions that first tried the freshest behind reader") \
	T(reader_target_best_behind_acquired, "PolarDB_Reader_Target_Best_Behind_Acquired", \
		"proxysql_polardb_reader_target_best_behind_acquired_total", \
		"Targeted reader acquisitions served by the freshest behind reader") \
	T(reader_target_fallback_acquired, "PolarDB_Reader_Target_Fallback_Acquired", \
		"proxysql_polardb_reader_target_fallback_acquired_total", \
		"Targeted reader acquisitions that require the backend wait wrapper") \
	T(wait_target_lsn_cache_advanced, "PolarDB_Wait_Target_LSN_Cache_Advanced", \
		"proxysql_polardb_wait_target_lsn_cache_advanced_total", \
		"Successful backend waits that advanced the selected reader LSN cache") \
	T(wait_target_lsn_cache_rejected, "PolarDB_Wait_Target_LSN_Cache_Rejected", \
		"proxysql_polardb_wait_target_lsn_cache_rejected_total", \
		"Successful backend waits whose selected-reader LSN cache update was rejected") \
	T(split_prepare_sum_us, "PolarDB_Split_Prepare_Sum_Us", \
		"proxysql_polardb_split_prepare_microseconds_total", \
		"Total time spent preparing transaction-split reads") \
	T(split_prepare_count, "PolarDB_Split_Prepare_Count", \
		"proxysql_polardb_split_prepare_count_total", \
		"Transaction-split prepare latency samples") \
	T(split_reader_acquire_sum_us, "PolarDB_Split_Reader_Acquire_Sum_Us", \
		"proxysql_polardb_split_reader_acquire_microseconds_total", \
		"Total time spent acquiring readers for transaction-split reads") \
	T(split_reader_acquire_count, "PolarDB_Split_Reader_Acquire_Count", \
		"proxysql_polardb_split_reader_acquire_count_total", \
		"Transaction-split reader acquisition latency samples") \
	T(split_wrapper_build_sum_us, "PolarDB_Split_Wrapper_Build_Sum_Us", \
		"proxysql_polardb_split_wrapper_build_microseconds_total", \
		"Total time spent building transaction-split wrapper SQL") \
	T(split_wrapper_build_count, "PolarDB_Split_Wrapper_Build_Count", \
		"proxysql_polardb_split_wrapper_build_count_total", \
		"Transaction-split wrapper build latency samples")

#define POLARDB_PROFILE_GLOBAL_COUNTER_LIST(G) \
	G(split_warmup_queue_delay_sum_us, "PolarDB_Split_Warmup_Queue_Delay_Sum_Us", \
		"proxysql_polardb_split_warmup_queue_delay_microseconds_total", \
		"Total time lazy split warmup requests spent queued before drain") \
	G(split_warmup_queue_delay_count, "PolarDB_Split_Warmup_Queue_Delay_Count", \
		"proxysql_polardb_split_warmup_queue_delay_count_total", \
		"Lazy split warmup queue-delay samples") \
	G(split_warmup_connect_sum_us, "PolarDB_Split_Warmup_Connect_Sum_Us", \
		"proxysql_polardb_split_warmup_connect_microseconds_total", \
		"Total time spent connecting lazy split warmup backends") \
	G(split_warmup_connect_count, "PolarDB_Split_Warmup_Connect_Count", \
		"proxysql_polardb_split_warmup_connect_count_total", \
		"Lazy split warmup connect latency samples") \
	G(split_warmup_publish_sum_us, "PolarDB_Split_Warmup_Publish_Sum_Us", \
		"proxysql_polardb_split_warmup_publish_microseconds_total", \
		"Total time spent publishing connected lazy split warmup backends into the pool") \
	G(split_warmup_publish_count, "PolarDB_Split_Warmup_Publish_Count", \
		"proxysql_polardb_split_warmup_publish_count_total", \
		"Lazy split warmup publish latency samples")
#else
#define POLARDB_PROFILE_THREAD_COUNTER_LIST(T)
#define POLARDB_PROFILE_GLOBAL_COUNTER_LIST(G)
#endif // POLARDB_PROFILE

// PolarDB counter metadata is intentionally shared by the SQL stats export,
// per-thread counter entries, worker-total merges, and Prometheus registration.
//
// THREAD counters have per-thread slots and are summed on read. GLOBAL counters
// stay as PgHGM->status atomics only. Prometheus uses ALL counters, but only
// THREAD counters may feed PolarDB_ThreadStatusVariable/stvar[].
//
// Gauges are listed separately because they can decrease. They still appear in
// stats_pgsql_global, but Prometheus must export them through its gauge path.
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
	T(reader_affinity_set, "PolarDB_Reader_Affinity_Set", \
		"proxysql_polardb_reader_affinity_set_total", \
		"Session-local reader affinity hints refreshed after a proven wait") \
	T(reader_affinity_hit, "PolarDB_Reader_Affinity_Hit", \
		"proxysql_polardb_reader_affinity_hit_total", \
		"Reader acquisitions served by a session-local reader affinity hint") \
	T(reader_affinity_miss_expired, "PolarDB_Reader_Affinity_Miss_Expired", \
		"proxysql_polardb_reader_affinity_miss_expired_total", \
		"Reader affinity hints skipped because their TTL or use count expired") \
	T(reader_affinity_miss_scope, "PolarDB_Reader_Affinity_Miss_Scope", \
		"proxysql_polardb_reader_affinity_miss_scope_total", \
		"Reader affinity hints skipped because the writer epoch changed") \
	T(reader_affinity_miss_not_ready, "PolarDB_Reader_Affinity_Miss_Not_Ready", \
		"proxysql_polardb_reader_affinity_miss_not_ready_total", \
		"Reader affinity hints skipped because the proven LSN was below the new target") \
	T(reader_affinity_miss_no_free, "PolarDB_Reader_Affinity_Miss_No_Free", \
		"proxysql_polardb_reader_affinity_miss_no_free_total", \
		"Reader affinity hints skipped because no compatible pooled connection was free") \
	T(reader_affinity_miss_profile, "PolarDB_Reader_Affinity_Miss_Profile", \
		"proxysql_polardb_reader_affinity_miss_profile_total", \
		"Reader affinity hints skipped because pooled connections did not match RFQ or startup identity") \
	T(reader_affinity_clear_failure, "PolarDB_Reader_Affinity_Clear_Failure", \
		"proxysql_polardb_reader_affinity_clear_failure_total", \
		"Reader affinity hints cleared after a reader failure") \
	T(reader_affinity_bypassed_wait, "PolarDB_Reader_Affinity_Bypassed_Wait", \
		"proxysql_polardb_reader_affinity_bypassed_wait_total", \
		"Reader affinity hits that avoided sending an LSN wait wrapper") \
	T(reader_target_selected_lsn_unknown, "PolarDB_Reader_Target_Selected_LSN_Unknown", \
		"proxysql_polardb_reader_target_selected_lsn_unknown_total", \
		"Reader acquisitions that still needed a wait and had no selected-reader LSN sample") \
	T(reader_target_selected_lsn_stale, "PolarDB_Reader_Target_Selected_LSN_Stale", \
		"proxysql_polardb_reader_target_selected_lsn_stale_total", \
		"Reader acquisitions that still needed a wait because the selected-reader LSN sample was stale") \
	T(reader_target_gap_zero, "PolarDB_Reader_Target_Gap_Zero", \
		"proxysql_polardb_reader_target_gap_zero_total", \
		"Wait-required reader acquisitions whose selected-reader LSN was already at the target") \
	T(reader_target_gap_le_4kb, "PolarDB_Reader_Target_Gap_Le_4KB", \
		"proxysql_polardb_reader_target_gap_le_4kb_total", \
		"Wait-required reader acquisitions with selected-reader LSN less than 4KB behind target") \
	T(reader_target_gap_le_64kb, "PolarDB_Reader_Target_Gap_Le_64KB", \
		"proxysql_polardb_reader_target_gap_le_64kb_total", \
		"Wait-required reader acquisitions with selected-reader LSN less than 64KB behind target") \
	T(reader_target_gap_le_1mb, "PolarDB_Reader_Target_Gap_Le_1MB", \
		"proxysql_polardb_reader_target_gap_le_1mb_total", \
		"Wait-required reader acquisitions with selected-reader LSN less than 1MB behind target") \
	T(reader_target_gap_le_16mb, "PolarDB_Reader_Target_Gap_Le_16MB", \
		"proxysql_polardb_reader_target_gap_le_16mb_total", \
		"Wait-required reader acquisitions with selected-reader LSN less than 16MB behind target") \
	T(reader_target_gap_gt_16mb, "PolarDB_Reader_Target_Gap_Gt_16MB", \
		"proxysql_polardb_reader_target_gap_gt_16mb_total", \
		"Wait-required reader acquisitions with selected-reader LSN more than 16MB behind target") \
	G(session_target_epoch_reset, "PolarDB_Session_Target_Epoch_Reset", \
		"proxysql_polardb_session_target_epoch_reset_total", \
		"Session LSN targets cleared after writer epoch changes") \
	T(session_lsn_routing, "PolarDB_Session_LSN_Routing", \
		"proxysql_polardb_session_lsn_routing_total", \
		"Reads routed to a reader with a session-LSN wait requirement") \
	T(route_planner_total, "PolarDB_Route_Planner_Total", \
		"proxysql_polardb_route_planner_total", \
		"Queries examined by the automatic PolarDB route planner") \
	T(route_replica_eligible, "PolarDB_Route_Replica_Eligible", \
		"proxysql_polardb_route_replica_eligible_total", \
		"Planner inputs whose query rule marked the request as replica eligible") \
	T(route_replica_ineligible, "PolarDB_Route_Replica_Ineligible", \
		"proxysql_polardb_route_replica_ineligible_total", \
		"Planner inputs not marked replica eligible, such as writes or control statements") \
	T(route_to_reader, "PolarDB_Route_To_Reader", \
		"proxysql_polardb_route_to_reader_total", \
		"Replica-eligible planner decisions targeting a reader hostgroup") \
	T(route_to_writer, "PolarDB_Route_To_Writer", \
		"proxysql_polardb_route_to_writer_total", \
		"Replica-eligible planner decisions targeting the writer hostgroup") \
	T(route_passthrough_rule_owned, "PolarDB_Route_Passthrough_Rule_Owned", \
		"proxysql_polardb_route_passthrough_rule_owned_total", \
		"Replica-eligible planner decisions left to normal query-rule routing") \
	T(route_no_wait_target, "PolarDB_Route_No_Wait_Target", \
		"proxysql_polardb_route_no_wait_target_total", \
		"Replica-eligible reader decisions that needed no session-LSN wait target") \
	T(route_wait_required, "PolarDB_Route_Wait_Required", \
		"proxysql_polardb_route_wait_required_total", \
		"Replica-eligible reader decisions that required a backend LSN wait") \
	T(route_txn_split_planned, "PolarDB_Route_Txn_Split_Planned", \
		"proxysql_polardb_route_txn_split_planned_total", \
		"Replica-eligible in-transaction reads planned for transaction split") \
	T(route_txn_wait_planned, "PolarDB_Route_Txn_Wait_Planned", \
		"proxysql_polardb_route_txn_wait_planned_total", \
		"Pre-write in-transaction reads planned for a temporary reader wait path") \
	T(route_manual_total, "PolarDB_Route_Manual_Total", \
		"proxysql_polardb_route_manual_total", \
		"Queries where a normal query rule or sticky hostgroup selected the route") \
	T(route_manual_to_reader, "PolarDB_Route_Manual_To_Reader", \
		"proxysql_polardb_route_manual_to_reader_total", \
		"Manual-route queries whose effective hostgroup was a PolarDB reader") \
	T(route_manual_to_writer, "PolarDB_Route_Manual_To_Writer", \
		"proxysql_polardb_route_manual_to_writer_total", \
		"Manual-route queries whose effective hostgroup was a PolarDB writer") \
	T(route_manual_other, "PolarDB_Route_Manual_Other", \
		"proxysql_polardb_route_manual_other_total", \
		"Manual-route queries whose effective hostgroup was not a known PolarDB reader or writer") \
	T(route_manual_forced_writer, "PolarDB_Route_Manual_Forced_Writer", \
		"proxysql_polardb_route_manual_forced_writer_total", \
		"Manual-route queries overridden to the writer by a reader-failure safety pin") \
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
		"Total elapsed time for wait-wrapped LSN reads, in microseconds") \
	T(wait_lsn_elapsed_le_1ms, "PolarDB_Wait_LSN_Elapsed_Le_1ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_1ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 1ms") \
	T(wait_lsn_elapsed_le_5ms, "PolarDB_Wait_LSN_Elapsed_Le_5ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_5ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 5ms") \
	T(wait_lsn_elapsed_le_10ms, "PolarDB_Wait_LSN_Elapsed_Le_10ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_10ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 10ms") \
	T(wait_lsn_elapsed_le_50ms, "PolarDB_Wait_LSN_Elapsed_Le_50ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_50ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 50ms") \
	T(wait_lsn_elapsed_le_100ms, "PolarDB_Wait_LSN_Elapsed_Le_100ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_100ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 100ms") \
	T(wait_lsn_elapsed_le_500ms, "PolarDB_Wait_LSN_Elapsed_Le_500ms", \
		"proxysql_polardb_wait_lsn_elapsed_le_500ms_total", \
		"Wait-wrapped reads whose elapsed time was at most 500ms") \
	T(wait_lsn_elapsed_le_1s, "PolarDB_Wait_LSN_Elapsed_Le_1s", \
		"proxysql_polardb_wait_lsn_elapsed_le_1s_total", \
		"Wait-wrapped reads whose elapsed time was at most 1s") \
	T(wait_lsn_elapsed_gt_1s, "PolarDB_Wait_LSN_Elapsed_Gt_1s", \
		"proxysql_polardb_wait_lsn_elapsed_gt_1s_total", \
		"Wait-wrapped reads whose elapsed time was more than 1s") \
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
	T(split_conn_cleanup_no_reuse_requested, "PolarDB_Split_Conn_Cleanup_No_Reuse_Requested", \
		"proxysql_polardb_split_conn_cleanup_no_reuse_requested_total", \
		"Destroyed split connections where the caller requested no reuse") \
	T(split_conn_cleanup_not_reusable, "PolarDB_Split_Conn_Cleanup_Not_Reusable", \
		"proxysql_polardb_split_conn_cleanup_not_reusable_total", \
		"Destroyed split connections already marked non-reusable") \
	T(split_conn_cleanup_not_idle, "PolarDB_Split_Conn_Cleanup_Not_Idle", \
		"proxysql_polardb_split_conn_cleanup_not_idle_total", \
		"Destroyed split connections whose async state was not idle") \
	T(split_conn_cleanup_active_txn, "PolarDB_Split_Conn_Cleanup_Active_Txn", \
		"proxysql_polardb_split_conn_cleanup_active_txn_total", \
		"Destroyed split connections that still had an active transaction") \
	T(split_conn_cleanup_recovery_attempt, "PolarDB_Split_Conn_Cleanup_Recovery_Attempt", \
		"proxysql_polardb_split_conn_cleanup_recovery_attempt_total", \
		"Split connection cleanup attempts that tried to recover a non-idle backend") \
	T(split_conn_cleanup_recovery_terminal, "PolarDB_Split_Conn_Cleanup_Recovery_Terminal", \
		"proxysql_polardb_split_conn_cleanup_recovery_terminal_total", \
		"Recovery attempts where the backend was already at a completed query terminal state") \
	T(split_conn_cleanup_recovery_timeout_state, "PolarDB_Split_Conn_Cleanup_Recovery_Timeout_State", \
		"proxysql_polardb_split_conn_cleanup_recovery_timeout_state_total", \
		"Recovery attempts rejected because the backend was in a timeout state") \
	T(split_conn_cleanup_recovery_busy_state, "PolarDB_Split_Conn_Cleanup_Recovery_Busy_State", \
		"proxysql_polardb_split_conn_cleanup_recovery_busy_state_total", \
		"Recovery attempts rejected because the backend was still in-flight") \
	T(split_conn_cleanup_normalized, "PolarDB_Split_Conn_Cleanup_Normalized", \
		"proxysql_polardb_split_conn_cleanup_normalized_total", \
		"Split connections whose completed error result was cleared during cleanup") \
	T(split_conn_cleanup_recovered, "PolarDB_Split_Conn_Cleanup_Recovered", \
		"proxysql_polardb_split_conn_cleanup_recovered_total", \
		"Previously non-idle split connections returned to the pool after cleanup") \
	POLARDB_PROFILE_THREAD_COUNTER_LIST(T) \
	T(split_lsn_wait_count, "PolarDB_Split_LSN_Wait_Count", \
		"proxysql_polardb_split_lsn_wait_count_total", \
		"LSN wait wrappers prepared for transaction-split reads") \
	T(split_lsn_wait_sum_us, "PolarDB_Split_LSN_Wait_Sum_Us", \
		"proxysql_polardb_split_lsn_wait_microseconds_total", \
		"Total transaction-split LSN wait time, in microseconds") \
	T(split_lsn_wait_elapsed_le_1ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_1ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_1ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 1ms") \
	T(split_lsn_wait_elapsed_le_5ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_5ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_5ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 5ms") \
	T(split_lsn_wait_elapsed_le_10ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_10ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_10ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 10ms") \
	T(split_lsn_wait_elapsed_le_50ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_50ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_50ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 50ms") \
	T(split_lsn_wait_elapsed_le_100ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_100ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_100ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 100ms") \
	T(split_lsn_wait_elapsed_le_500ms, "PolarDB_Split_LSN_Wait_Elapsed_Le_500ms", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_500ms_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 500ms") \
	T(split_lsn_wait_elapsed_le_1s, "PolarDB_Split_LSN_Wait_Elapsed_Le_1s", \
		"proxysql_polardb_split_lsn_wait_elapsed_le_1s_total", \
		"Transaction-split wait wrappers whose elapsed time was at most 1s") \
	T(split_lsn_wait_elapsed_gt_1s, "PolarDB_Split_LSN_Wait_Elapsed_Gt_1s", \
		"proxysql_polardb_split_lsn_wait_elapsed_gt_1s_total", \
		"Transaction-split wait wrappers whose elapsed time was more than 1s") \
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
	G(split_warmup_already_warm, "PolarDB_Split_Warmup_Already_Warm", \
		"proxysql_polardb_split_warmup_already_warm_total", \
		"Lazy split pool warmup requests skipped because a compatible free backend already existed") \
	G(split_warmup_dedup_queued, "PolarDB_Split_Warmup_Dedup_Queued", \
		"proxysql_polardb_split_warmup_dedup_queued_total", \
		"Lazy split pool warmup requests deduplicated against queued work") \
	G(split_warmup_dedup_inflight, "PolarDB_Split_Warmup_Dedup_Inflight", \
		"proxysql_polardb_split_warmup_dedup_inflight_total", \
		"Lazy split pool warmup requests deduplicated against in-flight work") \
	G(split_warmup_queue_full, "PolarDB_Split_Warmup_Queue_Full", \
		"proxysql_polardb_split_warmup_queue_full_total", \
		"Lazy split pool warmup requests dropped because the queue was full") \
	G(split_warmup_no_target, "PolarDB_Split_Warmup_No_Target", \
		"proxysql_polardb_split_warmup_no_target_total", \
		"Lazy split pool warmup drains that could not find an eligible target reader") \
	G(split_warmup_bad_request, "PolarDB_Split_Warmup_Bad_Request", \
		"proxysql_polardb_split_warmup_bad_request_total", \
		"Lazy split pool warmup requests rejected before queueing because required session identity was missing") \
	G(split_warmup_connect_failed, "PolarDB_Split_Warmup_Connect_Failed", \
		"proxysql_polardb_split_warmup_connect_failed_total", \
		"Lazy split pool warmup backends whose connection handshake failed") \
	G(split_warmup_publish_failed, "PolarDB_Split_Warmup_Publish_Failed", \
		"proxysql_polardb_split_warmup_publish_failed_total", \
		"Lazy split pool warmup backends discarded after connecting because the target changed or reached capacity") \
	POLARDB_PROFILE_GLOBAL_COUNTER_LIST(G) \
	G(split_warmup_sum_us, "PolarDB_Split_Warmup_Sum_Us", \
		"proxysql_polardb_split_warmup_microseconds_total", \
		"Total time from lazy split warmup request to pooled connection, in microseconds") \
	G(split_warmup_count, "PolarDB_Split_Warmup_Count", \
		"proxysql_polardb_split_warmup_count_total", \
		"Lazy split warmup latency samples")

#define POLARDB_GAUGE_LIST(G) \
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
