#ifndef PROXYSQL_PGSQL_POLARDB_COUNTERS_H
#define PROXYSQL_PGSQL_POLARDB_COUNTERS_H

#ifndef POLARDB_PROFILE
#define POLARDB_PROFILE 0
#endif

#ifndef POLARDB_PERF_DEBUG
#define POLARDB_PERF_DEBUG 0
#endif

#if POLARDB_PROXY

#if POLARDB_PROFILE
#define POLARDB_HGM_PROFILE_STATUS_COUNT(status_obj, name, value) \
	(status_obj).polardb_##name.fetch_add((value), std::memory_order_relaxed)
#define POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(status_obj, name) \
	POLARDB_HGM_PROFILE_STATUS_COUNT(status_obj, name, 1)
#else
#define POLARDB_HGM_PROFILE_STATUS_COUNT(status_obj, name, value) do { } while (0)
#define POLARDB_HGM_PROFILE_STATUS_COUNT_ONE(status_obj, name) do { } while (0)
#endif // POLARDB_PROFILE

#define POLARDB_HGM_STATUS_COUNT(status_obj, name, value) \
	(status_obj).polardb_##name.fetch_add((value), std::memory_order_relaxed)
#define POLARDB_HGM_STATUS_COUNT_ONE(status_obj, name) \
	POLARDB_HGM_STATUS_COUNT(status_obj, name, 1)

#if POLARDB_PROFILE
#define POLARDB_PROFILE_THREAD_COUNTER_LIST(T) \
	T(rfq_lsn_write_accepted, "PolarDB_RFQ_LSN_Write_Accepted", \
		"proxysql_polardb_rfq_lsn_write_accepted_total", \
		"Write RFQ LSN values accepted into the session consistency state") \
	T(rfq_lsn_read_accepted, "PolarDB_RFQ_LSN_Read_Accepted", \
		"proxysql_polardb_rfq_lsn_read_accepted_total", \
		"Read RFQ LSN values accepted into the session consistency state") \
	T(rfq_lsn_write_rejected, "PolarDB_RFQ_LSN_Write_Rejected", \
		"proxysql_polardb_rfq_lsn_write_rejected_total", \
		"Write RFQ LSN values rejected before updating the session consistency state") \
	T(rfq_lsn_read_rejected, "PolarDB_RFQ_LSN_Read_Rejected", \
		"proxysql_polardb_rfq_lsn_read_rejected_total", \
		"Read RFQ LSN values rejected before updating the session consistency state") \
	T(rfq_lsn_reject_inactive, "PolarDB_RFQ_LSN_Reject_Inactive", \
		"proxysql_polardb_rfq_lsn_reject_inactive_total", \
		"RFQ LSN cache updates rejected because PolarDB routing was inactive") \
	T(rfq_lsn_reject_invalid_input, "PolarDB_RFQ_LSN_Reject_Invalid_Input", \
		"proxysql_polardb_rfq_lsn_reject_invalid_input_total", \
		"RFQ LSN cache updates rejected because the server or LSN was invalid") \
	T(rfq_lsn_reject_missing_request_scope, "PolarDB_RFQ_LSN_Reject_Missing_Request_Scope", \
		"proxysql_polardb_rfq_lsn_reject_missing_request_scope_total", \
		"RFQ LSN cache updates rejected because the request had no writer group and epoch") \
	T(rfq_lsn_reject_missing_backend_scope, "PolarDB_RFQ_LSN_Reject_Missing_Backend_Scope", \
		"proxysql_polardb_rfq_lsn_reject_missing_backend_scope_total", \
		"RFQ LSN cache updates rejected because the backend had no current writer group and epoch") \
	T(rfq_lsn_reject_scope_mismatch, "PolarDB_RFQ_LSN_Reject_Scope_Mismatch", \
		"proxysql_polardb_rfq_lsn_reject_scope_mismatch_total", \
		"RFQ LSN cache updates rejected because request and current writer group or epoch differed") \
	T(consistency_read_wait_planned, "PolarDB_Consistency_Read_Wait_Planned", \
		"proxysql_polardb_consistency_read_wait_planned_total", \
		"Replica reads planned with a nonzero LSN wait target") \
	T(consistency_reader_wait_bypassed, "PolarDB_Consistency_Reader_Wait_Bypassed", \
		"proxysql_polardb_consistency_reader_wait_bypassed_total", \
		"Planned waits bypassed because the selected reader cache already reached the target") \
	T(consistency_reader_wait_required, "PolarDB_Consistency_Reader_Wait_Required", \
		"proxysql_polardb_consistency_reader_wait_required_total", \
		"Planned waits retained because the selected reader cache had not reached the target") \
	T(consistency_wait_wrapper_installed, "PolarDB_Consistency_Wait_Wrapper_Installed", \
		"proxysql_polardb_consistency_wait_wrapper_installed_total", \
		"Planned consistency waits installed into the outgoing reader query") \
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
		"Total time spent in ReaderPool acquisition, in microseconds") \
	T(reader_acquire_count, "PolarDB_Reader_Acquire_Count", \
		"proxysql_polardb_reader_acquire_count_total", \
		"ReaderPool acquisition latency samples") \
	T(selected_server_pool_lock_wait_sum_us, "PolarDB_Selected_Server_Pool_Lock_Wait_Sum_Us", \
		"proxysql_polardb_selected_server_pool_lock_wait_microseconds_total", \
		"Total time spent waiting for the selected server pool lock") \
	T(selected_server_pool_lock_wait_count, "PolarDB_Selected_Server_Pool_Lock_Wait_Count", \
		"proxysql_polardb_selected_server_pool_lock_wait_count_total", \
		"Selected server pool lock wait samples") \
	T(selected_server_pool_lock_hold_sum_us, "PolarDB_Selected_Server_Pool_Lock_Hold_Sum_Us", \
		"proxysql_polardb_selected_server_pool_lock_hold_microseconds_total", \
		"Total time the selected server pool lock was held") \
	T(selected_server_pool_lock_hold_count, "PolarDB_Selected_Server_Pool_Lock_Hold_Count", \
		"proxysql_polardb_selected_server_pool_lock_hold_count_total", \
		"Selected server pool lock hold samples") \
	T(reader_pool_shared_take_attempt, "PolarDB_Reader_Pool_Shared_Take_Attempt", \
		"proxysql_polardb_reader_pool_shared_take_attempt_total", \
		"Attempts to take a reader connection from a shared server pool") \
	T(reader_pool_shared_take_hit, "PolarDB_Reader_Pool_Shared_Take_Hit", \
		"proxysql_polardb_reader_pool_shared_take_hit_total", \
		"Shared reader-pool attempts that returned a connection") \
	T(reader_pool_shared_take_miss, "PolarDB_Reader_Pool_Shared_Take_Miss", \
		"proxysql_polardb_reader_pool_shared_take_miss_total", \
		"Shared reader-pool attempts that found no usable connection") \
	T(reader_pool_shared_take_busy, "PolarDB_Reader_Pool_Shared_Take_Busy", \
		"proxysql_polardb_reader_pool_shared_take_busy_total", \
		"Shared reader-pool attempts skipped because another worker held its mutex") \
	T(reader_pool_shared_free_zero_before_lock, "PolarDB_Reader_Pool_Shared_Free_Zero_Before_Lock", \
		"proxysql_polardb_reader_pool_shared_free_zero_before_lock_total", \
		"Pooled exact-key lookups that observed no shared FREE connection before taking the server pool lock") \
	T(reader_pool_shared_free_zero_became_hit, "PolarDB_Reader_Pool_Shared_Free_Zero_Became_Hit", \
		"proxysql_polardb_reader_pool_shared_free_zero_became_hit_total", \
		"Pooled exact-key lookups that observed no shared FREE connection but acquired one after taking the server pool lock") \
	T(reader_pool_selected_attempt, "PolarDB_Reader_Pool_Selected_Attempt", \
		"proxysql_polardb_reader_pool_selected_attempt_total", \
		"Attempts to get a connection from the reader chosen by routing policy") \
	T(reader_pool_selected_hit, "PolarDB_Reader_Pool_Selected_Hit", \
		"proxysql_polardb_reader_pool_selected_hit_total", \
		"Selected-reader attempts that returned a connection from that reader") \
	T(reader_pool_selected_miss, "PolarDB_Reader_Pool_Selected_Miss", \
		"proxysql_polardb_reader_pool_selected_miss_total", \
		"Selected-reader attempts that found no matching connection") \
	T(reader_pool_selected_busy, "PolarDB_Reader_Pool_Selected_Busy", \
		"proxysql_polardb_reader_pool_selected_busy_total", \
		"Selected-reader attempts skipped because its pool mutex was busy") \
	T(reader_pool_additional_attempt, "PolarDB_Reader_Pool_Additional_Attempt", \
		"proxysql_polardb_reader_pool_additional_attempt_total", \
		"Pool lookups after the first reader lookup for the request") \
	T(reader_pool_additional_hit, "PolarDB_Reader_Pool_Additional_Hit", \
		"proxysql_polardb_reader_pool_additional_hit_total", \
		"Additional reader-pool lookups that returned a connection") \
	T(reader_pool_additional_miss, "PolarDB_Reader_Pool_Additional_Miss", \
		"proxysql_polardb_reader_pool_additional_miss_total", \
		"Additional reader-pool lookups that found no matching connection") \
	T(reader_pool_additional_busy, "PolarDB_Reader_Pool_Additional_Busy", \
		"proxysql_polardb_reader_pool_additional_busy_total", \
		"Additional reader-pool lookups skipped because its pool mutex was busy") \
	T(reader_pool_local_take_attempt, "PolarDB_Reader_Pool_Local_Take_Attempt", \
		"proxysql_polardb_reader_pool_local_take_attempt_total", \
		"Attempts to reuse a reader connection held by the current worker") \
	T(reader_pool_local_take_hit, "PolarDB_Reader_Pool_Local_Take_Hit", \
		"proxysql_polardb_reader_pool_local_take_hit_total", \
		"Worker-local reader reuse attempts that returned a connection") \
	T(reader_pool_local_take_miss, "PolarDB_Reader_Pool_Local_Take_Miss", \
		"proxysql_polardb_reader_pool_local_take_miss_total", \
		"Worker-local reader reuse attempts that found no connection") \
	T(reader_pool_local_scan_steps, "PolarDB_Reader_Pool_Local_Scan_Steps", \
		"proxysql_polardb_reader_pool_local_scan_steps_total", \
		"Worker-local cached connection slots examined by reader lookups") \
	T(reader_pool_local_store_attempt, "PolarDB_Reader_Pool_Local_Store_Attempt", \
		"proxysql_polardb_reader_pool_local_store_attempt_total", \
		"Attempts to keep a released reader connection with the current worker") \
	T(reader_pool_local_store_accepted, "PolarDB_Reader_Pool_Local_Store_Accepted", \
		"proxysql_polardb_reader_pool_local_store_accepted_total", \
		"Released reader connections kept by the current worker") \
	T(reader_pool_local_store_rejected, "PolarDB_Reader_Pool_Local_Store_Rejected", \
		"proxysql_polardb_reader_pool_local_store_rejected_total", \
		"Released reader connections not kept by the current worker") \
	T(reader_pool_local_return_to_shared, "PolarDB_Reader_Pool_Local_Return_To_Shared", \
		"proxysql_polardb_reader_pool_local_return_to_shared_total", \
		"Worker-local reader connections returned to shared server pools") \
	T(reader_pool_shared_return_attempt, "PolarDB_Reader_Pool_Shared_Return_Attempt", \
		"proxysql_polardb_reader_pool_shared_return_attempt_total", \
		"Attempts to return a reader connection to its shared server pool") \
	T(reader_pool_shared_return_accepted, "PolarDB_Reader_Pool_Shared_Return_Accepted", \
		"proxysql_polardb_reader_pool_shared_return_accepted_total", \
		"Reader connections accepted by their shared server pool") \
	T(reader_pool_shared_return_rejected, "PolarDB_Reader_Pool_Shared_Return_Rejected", \
		"proxysql_polardb_reader_pool_shared_return_rejected_total", \
		"Reader connections rejected by their shared server pool") \
	T(reader_pool_shared_return_group, "PolarDB_Reader_Pool_Shared_Return_Group", \
		"proxysql_polardb_reader_pool_shared_return_group_total", \
		"Shared reader-pool mutex acquisitions for grouped connection returns") \
	T(reader_pool_shared_return_group_1, "PolarDB_Reader_Pool_Shared_Return_Group_1", \
		"proxysql_polardb_reader_pool_shared_return_group_1_total", \
		"Grouped shared reader-pool returns containing one connection") \
	T(reader_pool_shared_return_group_2, "PolarDB_Reader_Pool_Shared_Return_Group_2", \
		"proxysql_polardb_reader_pool_shared_return_group_2_total", \
		"Grouped shared reader-pool returns containing two connections") \
	T(reader_pool_shared_return_group_3_4, "PolarDB_Reader_Pool_Shared_Return_Group_3_4", \
		"proxysql_polardb_reader_pool_shared_return_group_3_4_total", \
		"Grouped shared reader-pool returns containing three or four connections") \
	T(reader_pool_shared_return_group_5_8, "PolarDB_Reader_Pool_Shared_Return_Group_5_8", \
		"proxysql_polardb_reader_pool_shared_return_group_5_8_total", \
		"Grouped shared reader-pool returns containing five through eight connections") \
	T(reader_pool_shared_return_group_9_16, "PolarDB_Reader_Pool_Shared_Return_Group_9_16", \
		"proxysql_polardb_reader_pool_shared_return_group_9_16_total", \
		"Grouped shared reader-pool returns containing nine through sixteen connections") \
	T(reader_pool_shared_return_group_17_plus, "PolarDB_Reader_Pool_Shared_Return_Group_17_Plus", \
		"proxysql_polardb_reader_pool_shared_return_group_17_plus_total", \
		"Grouped shared reader-pool returns containing at least seventeen connections") \
	T(reader_pool_shared_return_lock_wait_sum_us, "PolarDB_Reader_Pool_Shared_Return_Lock_Wait_Sum_Us", \
		"proxysql_polardb_reader_pool_shared_return_lock_wait_microseconds_total", \
		"Total time spent waiting to return reader connections to shared server pools") \
	T(reader_pool_shared_return_lock_hold_sum_us, "PolarDB_Reader_Pool_Shared_Return_Lock_Hold_Sum_Us", \
		"proxysql_polardb_reader_pool_shared_return_lock_hold_microseconds_total", \
		"Total time shared server pool locks were held while returning reader connections") \
	T(reader_target_ready_candidate, "PolarDB_Reader_Target_Ready_Candidate", \
		"proxysql_polardb_reader_target_ready_candidate_total", \
		"Reader candidates whose fresh LSN cache already reached the wait target") \
	T(reader_target_no_ready_candidate, "PolarDB_Reader_Target_No_Ready_Candidate", \
		"proxysql_polardb_reader_target_no_ready_candidate_total", \
		"Targeted reader acquisitions with no fresh target-reached candidate") \
	T(reader_target_both_behind_compared, "PolarDB_Reader_Target_Both_Behind_Compared", \
		"proxysql_polardb_reader_target_both_behind_compared_total", \
		"Two-reader target selections comparing two fresh readers below target") \
	T(reader_target_both_behind_equal_lsn, "PolarDB_Reader_Target_Both_Behind_Equal_LSN", \
		"proxysql_polardb_reader_target_both_behind_equal_lsn_total", \
		"Both-behind comparisons where the two readers had the same LSN") \
	T(reader_target_fresher_less_loaded, "PolarDB_Reader_Target_Fresher_Less_Loaded", \
		"proxysql_polardb_reader_target_fresher_less_loaded_total", \
		"Both-behind comparisons where the fresher reader had lower weight-normalized load") \
	T(reader_target_fresher_equal_loaded, "PolarDB_Reader_Target_Fresher_Equal_Loaded", \
		"proxysql_polardb_reader_target_fresher_equal_loaded_total", \
		"Both-behind comparisons where the fresher reader had equal weight-normalized load") \
	T(reader_target_fresher_more_loaded, "PolarDB_Reader_Target_Fresher_More_Loaded", \
		"proxysql_polardb_reader_target_fresher_more_loaded_total", \
		"Both-behind comparisons where the fresher reader had higher weight-normalized load") \
	T(reader_target_fresher_exact_switch, "PolarDB_Reader_Target_Fresher_Exact_Switch", \
		"proxysql_polardb_reader_target_fresher_exact_switch_total", \
		"Both-behind selections switched from the weighted first reader by exact-freshest policy") \
	T(reader_target_fresher_dominance_switch, "PolarDB_Reader_Target_Fresher_Dominance_Switch", \
		"proxysql_polardb_reader_target_fresher_dominance_switch_total", \
		"Both-behind selections switched from the weighted first reader by strict dominance") \
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
	T(rfq_requested_missing_payload, "PolarDB_RFQ_Requested_Missing_Payload", \
		"proxysql_polardb_rfq_requested_missing_payload_total", \
		"Results on RFQ-LSN startup-profile connections whose ReadyForQuery carried no LSN payload") \
	T(rfq_requested_zero_payload, "PolarDB_RFQ_Requested_Zero_Payload", \
		"proxysql_polardb_rfq_requested_zero_payload_total", \
		"Results on RFQ-LSN startup-profile connections whose ReadyForQuery carried a zero LSN payload") \
	T(client_rfq_lsn_missing_with_target, "PolarDB_Client_RFQ_LSN_Missing_With_Target", \
		"proxysql_polardb_client_rfq_lsn_missing_with_target_total", \
		"Client RFQ-LSN requests where the backend RFQ had no LSN payload while the session or wait target was non-zero") \
	T(wait_target_lsn_cache_advanced, "PolarDB_Wait_Target_LSN_Cache_Advanced", \
		"proxysql_polardb_wait_target_lsn_cache_advanced_total", \
		"Successful backend waits that advanced the selected reader LSN cache") \
	T(wait_target_lsn_cache_rejected, "PolarDB_Wait_Target_LSN_Cache_Rejected", \
		"proxysql_polardb_wait_target_lsn_cache_rejected_total", \
		"Successful backend waits whose selected-reader LSN cache update was rejected") \
	T(lsn_update_call, "PolarDB_LSN_Update_Call", \
		"proxysql_polardb_lsn_update_call_total", \
		"Accepted worker observations handled by the server LSN cache path") \
	T(lsn_update_advance, "PolarDB_LSN_Update_Advance", \
		"proxysql_polardb_lsn_update_advance_total", \
		"Worker observations that advanced a server LSN cache") \
	T(lsn_update_refresh_only, "PolarDB_LSN_Update_Refresh_Only", \
		"proxysql_polardb_lsn_update_refresh_only_total", \
		"Worker observations that refreshed only the server LSN timestamp") \
	T(lsn_update_shared_update, "PolarDB_LSN_Update_Shared_Update", \
		"proxysql_polardb_lsn_update_shared_update_total", \
		"Worker observations that updated the shared server LSN cache") \
	T(lsn_update_coalesced, "PolarDB_LSN_Update_Coalesced", \
		"proxysql_polardb_lsn_update_coalesced_total", \
		"Same-server and writer-scope observations skipped within the bounded refresh interval") \
	T(lsn_update_pass_server, "PolarDB_LSN_Update_Pass_Server", \
		"proxysql_polardb_lsn_update_pass_server_total", \
		"Distinct server and writer-scope identities observed by worker passes") \
	T(lsn_update_pass_repeat, "PolarDB_LSN_Update_Pass_Repeat", \
		"proxysql_polardb_lsn_update_pass_repeat_total", \
		"Repeated worker LSN observations for a tracked server in the same pass") \
	T(reader_pool_used_count_read, "PolarDB_Reader_Pool_Used_Count_Read", \
		"proxysql_polardb_reader_pool_used_count_read_total", \
		"Shared used-connection count reads made for reader selection") \
	T(reader_pool_used_count_pass_server, "PolarDB_Reader_Pool_Used_Count_Pass_Server", \
		"proxysql_polardb_reader_pool_used_count_pass_server_total", \
		"Distinct server used counts read by worker passes for reader selection") \
	T(reader_pool_used_count_pass_repeat, "PolarDB_Reader_Pool_Used_Count_Pass_Repeat", \
		"proxysql_polardb_reader_pool_used_count_pass_repeat_total", \
		"Repeated reader-selection used-count reads for a tracked server in the same worker pass") \
	T(reader_pool_used_count_pass_overflow, "PolarDB_Reader_Pool_Used_Count_Pass_Overflow", \
		"proxysql_polardb_reader_pool_used_count_pass_overflow_total", \
		"Reader-selection used-count reads not classified after 16 distinct servers were tracked in a pass") \
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
	T(split_pool_miss_reserved_exact, "PolarDB_Split_Pool_Miss_Reserved_Exact", \
		"proxysql_polardb_split_pool_miss_reserved_exact_total", \
		"Transaction-split pooled misses where exact-compatible FREE capacity was reserved by a ReaderPool reservation") \
	T(split_wrapper_build_sum_us, "PolarDB_Split_Wrapper_Build_Sum_Us", \
		"proxysql_polardb_split_wrapper_build_microseconds_total", \
		"Total time spent building transaction-split wrapper SQL") \
	T(split_wrapper_build_count, "PolarDB_Split_Wrapper_Build_Count", \
		"proxysql_polardb_split_wrapper_build_count_total", \
		"Transaction-split wrapper build latency samples") \
	T(wait_profile_plan_dispatch_sum_us, "PolarDB_Wait_Profile_Plan_Dispatch_Sum_Us", \
		"proxysql_polardb_wait_profile_plan_dispatch_microseconds_total", \
		"Profiled time from wait planning through backend dispatch") \
	T(wait_profile_plan_dispatch_count, "PolarDB_Wait_Profile_Plan_Dispatch_Count", \
		"proxysql_polardb_wait_profile_plan_dispatch_count_total", \
		"Profiled wait plan-to-dispatch latency samples") \
	T(wait_profile_dispatch_wait_set_sum_us, "PolarDB_Wait_Profile_Dispatch_Wait_Set_Sum_Us", \
		"proxysql_polardb_wait_profile_dispatch_wait_set_microseconds_total", \
		"Profiled time from backend dispatch through the final wait SET result") \
	T(wait_profile_dispatch_wait_set_count, "PolarDB_Wait_Profile_Dispatch_Wait_Set_Count", \
		"proxysql_polardb_wait_profile_dispatch_wait_set_count_total", \
		"Profiled dispatch-to-wait-SET latency samples") \
	T(wait_profile_wait_set_query_end_sum_us, "PolarDB_Wait_Profile_Wait_Set_Query_End_Sum_Us", \
		"proxysql_polardb_wait_profile_wait_set_query_end_microseconds_total", \
		"Profiled time from the final wait SET result through user-query completion") \
	T(wait_profile_wait_set_query_end_count, "PolarDB_Wait_Profile_Wait_Set_Query_End_Count", \
		"proxysql_polardb_wait_profile_wait_set_query_end_count_total", \
		"Profiled wait-SET-to-query-end latency samples") \
	T(wait_profile_target_mismatch, "PolarDB_Wait_Profile_Target_Mismatch", \
		"proxysql_polardb_wait_profile_target_mismatch_total", \
		"Profiled waits whose route target could not be derived from captured consistency state") \
	T(txn_wait_lsn_count, "PolarDB_Txn_Wait_LSN_Count", \
		"proxysql_polardb_txn_wait_lsn_count_total", \
		"Pre-write transaction reader LSN waits completed or failed") \
	T(txn_wait_lsn_sum_us, "PolarDB_Txn_Wait_LSN_Sum_Us", \
		"proxysql_polardb_txn_wait_lsn_microseconds_total", \
		"Correlated wrapper-prefix time for pre-write transaction reader LSN waits") \
	T(txn_wait_lsn_elapsed_le_1ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_1ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_1ms_total", \
		"Pre-write transaction reader waits completed within 1ms") \
	T(txn_wait_lsn_elapsed_le_5ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_5ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_5ms_total", \
		"Pre-write transaction reader waits completed within 5ms") \
	T(txn_wait_lsn_elapsed_le_10ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_10ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_10ms_total", \
		"Pre-write transaction reader waits completed within 10ms") \
	T(txn_wait_lsn_elapsed_le_50ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_50ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_50ms_total", \
		"Pre-write transaction reader waits completed within 50ms") \
	T(txn_wait_lsn_elapsed_le_100ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_100ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_100ms_total", \
		"Pre-write transaction reader waits completed within 100ms") \
	T(txn_wait_lsn_elapsed_le_500ms, "PolarDB_Txn_Wait_LSN_Elapsed_Le_500ms", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_500ms_total", \
		"Pre-write transaction reader waits completed within 500ms") \
	T(txn_wait_lsn_elapsed_le_1s, "PolarDB_Txn_Wait_LSN_Elapsed_Le_1s", \
		"proxysql_polardb_txn_wait_lsn_elapsed_le_1s_total", \
		"Pre-write transaction reader waits completed within 1s") \
	T(txn_wait_lsn_elapsed_gt_1s, "PolarDB_Txn_Wait_LSN_Elapsed_Gt_1s", \
		"proxysql_polardb_txn_wait_lsn_elapsed_gt_1s_total", \
		"Pre-write transaction reader waits taking more than 1s") \
	T(wait_profile_ordinary_count, "PolarDB_Wait_Profile_Ordinary_Count", \
		"proxysql_polardb_wait_profile_ordinary_count_total", \
		"Profiled ordinary autocommit reader waits") \
	T(wait_profile_ordinary_sum_us, "PolarDB_Wait_Profile_Ordinary_Sum_Us", \
		"proxysql_polardb_wait_profile_ordinary_microseconds_total", \
		"Correlated wrapper-prefix time for ordinary autocommit reader waits") \
	T(wait_profile_target_unknown_count, "PolarDB_Wait_Profile_Target_Unknown_Count", \
		"proxysql_polardb_wait_profile_target_unknown_count_total", \
		"Profiled waits with an unclassified target source") \
	T(wait_profile_target_unknown_sum_us, "PolarDB_Wait_Profile_Target_Unknown_Sum_Us", \
		"proxysql_polardb_wait_profile_target_unknown_microseconds_total", \
		"Correlated wrapper-prefix time for waits with an unclassified target source") \
	T(wait_profile_target_write_count, "PolarDB_Wait_Profile_Target_Write_Count", \
		"proxysql_polardb_wait_profile_target_write_count_total", \
		"Profiled waits whose target came from the session write LSN") \
	T(wait_profile_target_write_sum_us, "PolarDB_Wait_Profile_Target_Write_Sum_Us", \
		"proxysql_polardb_wait_profile_target_write_microseconds_total", \
		"Correlated wrapper-prefix time for session-write targets") \
	T(wait_profile_target_observed_count, "PolarDB_Wait_Profile_Target_Observed_Count", \
		"proxysql_polardb_wait_profile_target_observed_count_total", \
		"Profiled waits whose target came from the session observed LSN") \
	T(wait_profile_target_observed_sum_us, "PolarDB_Wait_Profile_Target_Observed_Sum_Us", \
		"proxysql_polardb_wait_profile_target_observed_microseconds_total", \
		"Correlated wrapper-prefix time for session-observed targets") \
	T(wait_profile_target_session_equal_count, "PolarDB_Wait_Profile_Target_Session_Equal_Count", \
		"proxysql_polardb_wait_profile_target_session_equal_count_total", \
		"Profiled waits where write and observed LSNs equally supplied the session target") \
	T(wait_profile_target_session_equal_sum_us, "PolarDB_Wait_Profile_Target_Session_Equal_Sum_Us", \
		"proxysql_polardb_wait_profile_target_session_equal_microseconds_total", \
		"Correlated wrapper-prefix time for equal write and observed session targets") \
	T(wait_profile_target_global_count, "PolarDB_Wait_Profile_Target_Global_Count", \
		"proxysql_polardb_wait_profile_target_global_count_total", \
		"Profiled waits whose target was raised by the global writer-group LSN") \
	T(wait_profile_target_global_sum_us, "PolarDB_Wait_Profile_Target_Global_Sum_Us", \
		"proxysql_polardb_wait_profile_target_global_microseconds_total", \
		"Correlated wrapper-prefix time for global writer-group targets") \
	T(wait_profile_target_txn_primary_count, "PolarDB_Wait_Profile_Target_Txn_Primary_Count", \
		"proxysql_polardb_wait_profile_target_txn_primary_count_total", \
		"Profiled transaction-split waits whose target came from the primary transaction LSN") \
	T(wait_profile_target_txn_primary_sum_us, "PolarDB_Wait_Profile_Target_Txn_Primary_Sum_Us", \
		"proxysql_polardb_wait_profile_target_txn_primary_microseconds_total", \
		"Correlated wrapper-prefix time for primary transaction targets") \
	T(wait_profile_selected_best_count, "PolarDB_Wait_Profile_Selected_Best_Count", \
		"proxysql_polardb_wait_profile_selected_best_count_total", \
		"Profiled waits whose selected reader was not behind the best reader LSN sampled for that acquisition") \
	T(wait_profile_selected_best_sum_us, "PolarDB_Wait_Profile_Selected_Best_Sum_Us", \
		"proxysql_polardb_wait_profile_selected_best_microseconds_total", \
		"Correlated wrapper-prefix time after selecting the best reader LSN sampled for that acquisition") \
	T(wait_profile_selected_behind_best_count, "PolarDB_Wait_Profile_Selected_Behind_Best_Count", \
		"proxysql_polardb_wait_profile_selected_behind_best_count_total", \
		"Profiled waits whose selected reader was behind the best reader LSN sampled for that acquisition") \
	T(wait_profile_selected_behind_best_sum_us, "PolarDB_Wait_Profile_Selected_Behind_Best_Sum_Us", \
		"proxysql_polardb_wait_profile_selected_behind_best_microseconds_total", \
		"Correlated wrapper-prefix time after selecting behind the best reader LSN sampled for that acquisition") \
	T(wait_profile_selection_unknown_count, "PolarDB_Wait_Profile_Selection_Unknown_Count", \
		"proxysql_polardb_wait_profile_selection_unknown_count_total", \
		"Profiled waits lacking a comparable selected and best reader LSN") \
	T(wait_profile_selection_unknown_sum_us, "PolarDB_Wait_Profile_Selection_Unknown_Sum_Us", \
		"proxysql_polardb_wait_profile_selection_unknown_microseconds_total", \
		"Correlated wrapper-prefix time for waits lacking a comparable reader selection") \
	T(wait_profile_observed_same_reader_count, "PolarDB_Wait_Profile_Observed_Same_Reader_Count", \
		"proxysql_polardb_wait_profile_observed_same_reader_count_total", \
		"Observed-target waits routed back to the reader that supplied the target") \
	T(wait_profile_observed_same_reader_sum_us, "PolarDB_Wait_Profile_Observed_Same_Reader_Sum_Us", \
		"proxysql_polardb_wait_profile_observed_same_reader_microseconds_total", \
		"Correlated wrapper-prefix time for same-reader observed targets") \
	T(wait_profile_observed_cross_reader_count, "PolarDB_Wait_Profile_Observed_Cross_Reader_Count", \
		"proxysql_polardb_wait_profile_observed_cross_reader_count_total", \
		"Observed-target waits routed to a different reader from the target source") \
	T(wait_profile_observed_cross_reader_sum_us, "PolarDB_Wait_Profile_Observed_Cross_Reader_Sum_Us", \
		"proxysql_polardb_wait_profile_observed_cross_reader_microseconds_total", \
		"Correlated wrapper-prefix time for cross-reader observed targets") \
	T(wait_profile_observed_reader_unknown_count, "PolarDB_Wait_Profile_Observed_Reader_Unknown_Count", \
		"proxysql_polardb_wait_profile_observed_reader_unknown_count_total", \
		"Observed-target waits lacking source or selected-reader identity") \
	T(wait_profile_observed_reader_unknown_sum_us, "PolarDB_Wait_Profile_Observed_Reader_Unknown_Sum_Us", \
		"proxysql_polardb_wait_profile_observed_reader_unknown_microseconds_total", \
		"Correlated wrapper-prefix time for observed targets with unknown reader relation") \
	T(wait_profile_gap_unknown_count, "PolarDB_Wait_Profile_Gap_Unknown_Count", \
		"proxysql_polardb_wait_profile_gap_unknown_count_total", \
		"Profiled waits with no selected-reader LSN sample") \
	T(wait_profile_gap_unknown_sum_us, "PolarDB_Wait_Profile_Gap_Unknown_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_unknown_microseconds_total", \
		"Correlated wrapper-prefix time for waits with no selected-reader LSN sample") \
	T(wait_profile_gap_stale_count, "PolarDB_Wait_Profile_Gap_Stale_Count", \
		"proxysql_polardb_wait_profile_gap_stale_count_total", \
		"Profiled waits whose selected-reader LSN sample was stale") \
	T(wait_profile_gap_stale_sum_us, "PolarDB_Wait_Profile_Gap_Stale_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_stale_microseconds_total", \
		"Correlated wrapper-prefix time for stale selected-reader LSN samples") \
	T(wait_profile_gap_zero_count, "PolarDB_Wait_Profile_Gap_Zero_Count", \
		"proxysql_polardb_wait_profile_gap_zero_count_total", \
		"Profiled waits whose selected reader was already at or beyond target") \
	T(wait_profile_gap_zero_sum_us, "PolarDB_Wait_Profile_Gap_Zero_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_zero_microseconds_total", \
		"Correlated wrapper-prefix time for zero selected-reader target gaps") \
	T(wait_profile_gap_le_4kb_count, "PolarDB_Wait_Profile_Gap_Le_4KB_Count", \
		"proxysql_polardb_wait_profile_gap_le_4kb_count_total", \
		"Profiled waits with selected-reader target gap at most 4KB") \
	T(wait_profile_gap_le_4kb_sum_us, "PolarDB_Wait_Profile_Gap_Le_4KB_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_le_4kb_microseconds_total", \
		"Correlated wrapper-prefix time for target gaps at most 4KB") \
	T(wait_profile_gap_le_64kb_count, "PolarDB_Wait_Profile_Gap_Le_64KB_Count", \
		"proxysql_polardb_wait_profile_gap_le_64kb_count_total", \
		"Profiled waits with selected-reader target gap at most 64KB") \
	T(wait_profile_gap_le_64kb_sum_us, "PolarDB_Wait_Profile_Gap_Le_64KB_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_le_64kb_microseconds_total", \
		"Correlated wrapper-prefix time for target gaps at most 64KB") \
	T(wait_profile_gap_le_1mb_count, "PolarDB_Wait_Profile_Gap_Le_1MB_Count", \
		"proxysql_polardb_wait_profile_gap_le_1mb_count_total", \
		"Profiled waits with selected-reader target gap at most 1MB") \
	T(wait_profile_gap_le_1mb_sum_us, "PolarDB_Wait_Profile_Gap_Le_1MB_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_le_1mb_microseconds_total", \
		"Correlated wrapper-prefix time for target gaps at most 1MB") \
	T(wait_profile_gap_le_16mb_count, "PolarDB_Wait_Profile_Gap_Le_16MB_Count", \
		"proxysql_polardb_wait_profile_gap_le_16mb_count_total", \
		"Profiled waits with selected-reader target gap at most 16MB") \
	T(wait_profile_gap_le_16mb_sum_us, "PolarDB_Wait_Profile_Gap_Le_16MB_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_le_16mb_microseconds_total", \
		"Correlated wrapper-prefix time for target gaps at most 16MB") \
	T(wait_profile_gap_gt_16mb_count, "PolarDB_Wait_Profile_Gap_Gt_16MB_Count", \
		"proxysql_polardb_wait_profile_gap_gt_16mb_count_total", \
		"Profiled waits with selected-reader target gap above 16MB") \
	T(wait_profile_gap_gt_16mb_sum_us, "PolarDB_Wait_Profile_Gap_Gt_16MB_Sum_Us", \
		"proxysql_polardb_wait_profile_gap_gt_16mb_microseconds_total", \
		"Correlated wrapper-prefix time for target gaps above 16MB") \
	T(wait_profile_lsn_age_unknown_count, "PolarDB_Wait_Profile_LSN_Age_Unknown_Count", \
		"proxysql_polardb_wait_profile_lsn_age_unknown_count_total", \
		"Profiled waits lacking a selected-reader LSN sample timestamp") \
	T(wait_profile_lsn_age_unknown_sum_us, "PolarDB_Wait_Profile_LSN_Age_Unknown_Sum_Us", \
		"proxysql_polardb_wait_profile_lsn_age_unknown_microseconds_total", \
		"Correlated wrapper-prefix time for waits lacking an LSN sample timestamp") \
	T(wait_profile_lsn_age_le_100us_count, "PolarDB_Wait_Profile_LSN_Age_Le_100us_Count", \
		"proxysql_polardb_wait_profile_lsn_age_le_100us_count_total", \
		"Profiled waits with selected-reader LSN sample age at most 100us") \
	T(wait_profile_lsn_age_le_100us_sum_us, "PolarDB_Wait_Profile_LSN_Age_Le_100us_Sum_Us", \
		"proxysql_polardb_wait_profile_lsn_age_le_100us_microseconds_total", \
		"Correlated wrapper-prefix time for selected-reader LSN age at most 100us") \
	T(wait_profile_lsn_age_le_1ms_count, "PolarDB_Wait_Profile_LSN_Age_Le_1ms_Count", \
		"proxysql_polardb_wait_profile_lsn_age_le_1ms_count_total", \
		"Profiled waits with selected-reader LSN sample age at most 1ms") \
	T(wait_profile_lsn_age_le_1ms_sum_us, "PolarDB_Wait_Profile_LSN_Age_Le_1ms_Sum_Us", \
		"proxysql_polardb_wait_profile_lsn_age_le_1ms_microseconds_total", \
		"Correlated wrapper-prefix time for selected-reader LSN age at most 1ms") \
	T(wait_profile_lsn_age_le_5ms_count, "PolarDB_Wait_Profile_LSN_Age_Le_5ms_Count", \
		"proxysql_polardb_wait_profile_lsn_age_le_5ms_count_total", \
		"Profiled waits with selected-reader LSN sample age at most 5ms") \
	T(wait_profile_lsn_age_le_5ms_sum_us, "PolarDB_Wait_Profile_LSN_Age_Le_5ms_Sum_Us", \
		"proxysql_polardb_wait_profile_lsn_age_le_5ms_microseconds_total", \
		"Correlated wrapper-prefix time for selected-reader LSN age at most 5ms") \
	T(wait_profile_lsn_age_gt_5ms_count, "PolarDB_Wait_Profile_LSN_Age_Gt_5ms_Count", \
		"proxysql_polardb_wait_profile_lsn_age_gt_5ms_count_total", \
		"Profiled waits with selected-reader LSN sample age above 5ms") \
	T(wait_profile_lsn_age_gt_5ms_sum_us, "PolarDB_Wait_Profile_LSN_Age_Gt_5ms_Sum_Us", \
		"proxysql_polardb_wait_profile_lsn_age_gt_5ms_microseconds_total", \
		"Correlated wrapper-prefix time for selected-reader LSN age above 5ms") \
	T(wait_profile_selected_gap_sum_bytes, "PolarDB_Wait_Profile_Selected_Gap_Sum_Bytes", \
		"proxysql_polardb_wait_profile_selected_gap_bytes_total", \
		"Selected-reader target-gap bytes correlated with completed profiled waits") \
	T(wait_profile_selection_loss_sum_bytes, "PolarDB_Wait_Profile_Selection_Loss_Sum_Bytes", \
		"proxysql_polardb_wait_profile_selection_loss_bytes_total", \
		"Extra target-gap bytes from selecting behind the best considered reader")

#define POLARDB_PROFILE_GLOBAL_COUNTER_LIST(G) \
	G(reader_pool_confirmed_saturated, "PolarDB_Reader_Pool_Confirmed_Saturated", \
		"proxysql_polardb_reader_pool_confirmed_saturated_total", \
		"Selected-server capacity probes with no reusable connection and snapshot capacity fully used") \
	G(reader_pool_hgm_create_lock_entry, "PolarDB_Reader_Pool_HGM_Create_Lock_Entry", \
		"proxysql_polardb_reader_pool_hgm_create_lock_entry_total", \
		"Selected-server acquisition attempts that entered the HGM creation lock") \
	G(reader_pool_exact_bucket_created, "PolarDB_Reader_Pool_Exact_Bucket_Created", \
		"proxysql_polardb_reader_pool_exact_bucket_created_total", \
		"Exact-key FREE buckets created for reader connections") \
	G(reader_pool_exact_bucket_emptied, "PolarDB_Reader_Pool_Exact_Bucket_Emptied", \
		"proxysql_polardb_reader_pool_exact_bucket_emptied_total", \
		"Exact-key FREE buckets emptied after their last connection was taken") \
	G(reader_pool_exact_bucket_reused, "PolarDB_Reader_Pool_Exact_Bucket_Reused", \
		"proxysql_polardb_reader_pool_exact_bucket_reused_total", \
		"Retained empty exact-key FREE buckets reused by a returning connection") \
	G(reader_pool_exact_bucket_pruned, "PolarDB_Reader_Pool_Exact_Bucket_Pruned", \
		"proxysql_polardb_reader_pool_exact_bucket_pruned_total", \
		"Empty exact-key FREE buckets erased at the retention bound or maintenance") \
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
	G(split_warmup_add_sum_us, "PolarDB_Split_Warmup_Add_Sum_Us", \
		"proxysql_polardb_split_warmup_add_microseconds_total", \
		"Total time spent adding connected lazy split warmup backends to the pool") \
	G(split_warmup_add_count, "PolarDB_Split_Warmup_Add_Count", \
		"proxysql_polardb_split_warmup_add_count_total", \
		"Lazy split warmup add-time samples") \
	G(idle_ping_pool_maintenance_sum_us, "PolarDB_Idle_Ping_Pool_Maintenance_Sum_Us", \
		"proxysql_polardb_idle_ping_pool_maintenance_microseconds_total", \
		"Total time spent finding and preparing idle connections for ping") \
	G(idle_ping_pool_maintenance_count, "PolarDB_Idle_Ping_Pool_Maintenance_Count", \
		"proxysql_polardb_idle_ping_pool_maintenance_count_total", \
		"Idle connection ping-pool maintenance samples")
#else
#define POLARDB_PROFILE_THREAD_COUNTER_LIST(T)
#define POLARDB_PROFILE_GLOBAL_COUNTER_LIST(G)
#endif // POLARDB_PROFILE

#if POLARDB_PERF_DEBUG
#define POLARDB_PERF_DEBUG_THREAD_COUNTER_LIST(T) \
	T(perf_core_pool_exact_attempt, "PolarDB_Perf_Core_Pool_Exact_Attempt", \
		"proxysql_polardb_perf_core_pool_exact_attempt_total", \
		"Diagnostic-build generic core-pool exact-match attempts made before the HGM slow path") \
	T(perf_core_pool_exact_hit, "PolarDB_Perf_Core_Pool_Exact_Hit", \
		"proxysql_polardb_perf_core_pool_exact_hit_total", \
		"Diagnostic-build generic core-pool exact-match attempts that returned a connection") \
	T(perf_core_pool_exact_miss, "PolarDB_Perf_Core_Pool_Exact_Miss", \
		"proxysql_polardb_perf_core_pool_exact_miss_total", \
		"Diagnostic-build generic core-pool exact-match attempts that continued to the HGM slow path") \
	T(perf_writev_skip_disabled, "PolarDB_Perf_WriteV_Skip_Disabled", \
		"proxysql_polardb_perf_writev_skip_disabled_total", \
		"Direct frontend write skipped because the runtime switch was disabled") \
	T(perf_writev_skip_inactive, "PolarDB_Perf_WriteV_Skip_Inactive", \
		"proxysql_polardb_perf_writev_skip_inactive_total", \
		"Direct frontend write skipped because the stream, fd, or network state was unusable") \
	T(perf_writev_skip_encrypted, "PolarDB_Perf_WriteV_Skip_Encrypted", \
		"proxysql_polardb_perf_writev_skip_encrypted_total", \
		"Direct plaintext frontend write skipped because the stream was encrypted") \
	T(perf_writev_skip_not_frontend, "PolarDB_Perf_WriteV_Skip_Not_Frontend", \
		"proxysql_polardb_perf_writev_skip_not_frontend_total", \
		"Direct frontend write skipped because the stream was not a frontend stream") \
	T(perf_writev_skip_state, "PolarDB_Perf_WriteV_Skip_State", \
		"proxysql_polardb_perf_writev_skip_state_total", \
		"Direct frontend write skipped because the data-stream state was not ready") \
	T(perf_writev_skip_session, "PolarDB_Perf_WriteV_Skip_Session", \
		"proxysql_polardb_perf_writev_skip_session_total", \
		"Direct frontend write skipped because the session was missing or not PostgreSQL") \
	T(perf_writev_skip_mirror, "PolarDB_Perf_WriteV_Skip_Mirror", \
		"proxysql_polardb_perf_writev_skip_mirror_total", \
		"Direct frontend write skipped for a mirrored session") \
	T(perf_writev_skip_poll, "PolarDB_Perf_WriteV_Skip_Poll", \
		"proxysql_polardb_perf_writev_skip_poll_total", \
		"Direct frontend write skipped because poll state was missing") \
	T(perf_writev_skip_no_packets, "PolarDB_Perf_WriteV_Skip_No_Packets", \
		"proxysql_polardb_perf_writev_skip_no_packets_total", \
		"Direct frontend write skipped because PSarrayOUT had no packets") \
	T(perf_writev_skip_queue_pending, "PolarDB_Perf_WriteV_Skip_Queue_Pending", \
		"proxysql_polardb_perf_writev_skip_queue_pending_total", \
		"Direct frontend write skipped because queueOUT already had bytes") \
	T(perf_writev_skip_queue_partial, "PolarDB_Perf_WriteV_Skip_Queue_Partial", \
		"proxysql_polardb_perf_writev_skip_queue_partial_total", \
		"Direct frontend write skipped because queueOUT had a partial packet") \
	T(perf_writev_view_build_calls, "PolarDB_Perf_WriteV_View_Build_Calls", \
		"proxysql_polardb_perf_writev_view_build_calls_total", \
		"Direct frontend write view build attempts") \
	T(perf_writev_view_build_packets, "PolarDB_Perf_WriteV_View_Build_Packets", \
		"proxysql_polardb_perf_writev_view_build_packets_total", \
		"Packets included in direct frontend write views") \
	T(perf_write_bytes_le_512, "PolarDB_Perf_Write_Bytes_Le_512", \
		"proxysql_polardb_perf_write_bytes_le_512_total", \
		"Frontend socket writes of at most 512 bytes") \
	T(perf_write_bytes_le_1k, "PolarDB_Perf_Write_Bytes_Le_1KB", \
		"proxysql_polardb_perf_write_bytes_le_1kb_total", \
		"Frontend socket writes of 513 bytes to 1KB") \
	T(perf_write_bytes_le_2k, "PolarDB_Perf_Write_Bytes_Le_2KB", \
		"proxysql_polardb_perf_write_bytes_le_2kb_total", \
		"Frontend socket writes of more than 1KB to 2KB") \
	T(perf_write_bytes_le_4k, "PolarDB_Perf_Write_Bytes_Le_4KB", \
		"proxysql_polardb_perf_write_bytes_le_4kb_total", \
		"Frontend socket writes of more than 2KB to 4KB") \
	T(perf_write_bytes_le_8k, "PolarDB_Perf_Write_Bytes_Le_8KB", \
		"proxysql_polardb_perf_write_bytes_le_8kb_total", \
		"Frontend socket writes of more than 4KB to 8KB") \
	T(perf_write_bytes_le_16k, "PolarDB_Perf_Write_Bytes_Le_16KB", \
		"proxysql_polardb_perf_write_bytes_le_16kb_total", \
		"Frontend socket writes of more than 8KB to 16KB") \
	T(perf_write_bytes_le_32k, "PolarDB_Perf_Write_Bytes_Le_32KB", \
		"proxysql_polardb_perf_write_bytes_le_32kb_total", \
		"Frontend socket writes of more than 16KB to 32KB") \
	T(perf_write_bytes_le_64k, "PolarDB_Perf_Write_Bytes_Le_64KB", \
		"proxysql_polardb_perf_write_bytes_le_64kb_total", \
		"Frontend socket writes of more than 32KB to 64KB") \
	T(perf_write_bytes_gt_64k, "PolarDB_Perf_Write_Bytes_Gt_64KB", \
		"proxysql_polardb_perf_write_bytes_gt_64kb_total", \
		"Frontend socket writes larger than 64KB") \
	T(perf_write_iov_1, "PolarDB_Perf_Write_Iov_1", \
		"proxysql_polardb_perf_write_iov_1_total", \
		"Direct frontend writes with one iovec") \
	T(perf_write_iov_2, "PolarDB_Perf_Write_Iov_2", \
		"proxysql_polardb_perf_write_iov_2_total", \
		"Direct frontend writes with two iovecs") \
	T(perf_write_iov_3_4, "PolarDB_Perf_Write_Iov_3_4", \
		"proxysql_polardb_perf_write_iov_3_4_total", \
		"Direct frontend writes with three or four iovecs") \
	T(perf_write_iov_5_8, "PolarDB_Perf_Write_Iov_5_8", \
		"proxysql_polardb_perf_write_iov_5_8_total", \
		"Direct frontend writes with five to eight iovecs") \
	T(perf_write_iov_9_16, "PolarDB_Perf_Write_Iov_9_16", \
		"proxysql_polardb_perf_write_iov_9_16_total", \
		"Direct frontend writes with nine to sixteen iovecs") \
	T(perf_write_iov_17_32, "PolarDB_Perf_Write_Iov_17_32", \
		"proxysql_polardb_perf_write_iov_17_32_total", \
		"Direct frontend writes with seventeen to thirty-two iovecs") \
	T(perf_write_iov_33_64, "PolarDB_Perf_Write_Iov_33_64", \
		"proxysql_polardb_perf_write_iov_33_64_total", \
		"Direct frontend writes with thirty-three to sixty-four iovecs") \
	T(perf_packets_per_send_1, "PolarDB_Perf_Packets_Per_Send_1", \
		"proxysql_polardb_perf_packets_per_send_1_total", \
		"Direct frontend writes containing one protocol packet") \
	T(perf_packets_per_send_2, "PolarDB_Perf_Packets_Per_Send_2", \
		"proxysql_polardb_perf_packets_per_send_2_total", \
		"Direct frontend writes containing two protocol packets") \
	T(perf_packets_per_send_3_4, "PolarDB_Perf_Packets_Per_Send_3_4", \
		"proxysql_polardb_perf_packets_per_send_3_4_total", \
		"Direct frontend writes containing three or four protocol packets") \
	T(perf_packets_per_send_5_8, "PolarDB_Perf_Packets_Per_Send_5_8", \
		"proxysql_polardb_perf_packets_per_send_5_8_total", \
		"Direct frontend writes containing five to eight protocol packets") \
	T(perf_packets_per_send_9_16, "PolarDB_Perf_Packets_Per_Send_9_16", \
		"proxysql_polardb_perf_packets_per_send_9_16_total", \
		"Direct frontend writes containing nine to sixteen protocol packets") \
	T(perf_packets_per_send_17_32, "PolarDB_Perf_Packets_Per_Send_17_32", \
		"proxysql_polardb_perf_packets_per_send_17_32_total", \
		"Direct frontend writes containing seventeen to thirty-two protocol packets") \
	T(perf_packets_per_send_33_64, "PolarDB_Perf_Packets_Per_Send_33_64", \
		"proxysql_polardb_perf_packets_per_send_33_64_total", \
		"Direct frontend writes containing thirty-three to sixty-four protocol packets") \
	T(perf_packet_bytes_le_512, "PolarDB_Perf_Packet_Bytes_Le_512", \
		"proxysql_polardb_perf_packet_bytes_le_512_total", \
		"Protocol packets in direct frontend writes of at most 512 bytes") \
	T(perf_packet_bytes_le_1k, "PolarDB_Perf_Packet_Bytes_Le_1KB", \
		"proxysql_polardb_perf_packet_bytes_le_1kb_total", \
		"Protocol packets in direct frontend writes of 513 bytes to 1KB") \
	T(perf_packet_bytes_le_2k, "PolarDB_Perf_Packet_Bytes_Le_2KB", \
		"proxysql_polardb_perf_packet_bytes_le_2kb_total", \
		"Protocol packets in direct frontend writes of more than 1KB to 2KB") \
	T(perf_packet_bytes_le_4k, "PolarDB_Perf_Packet_Bytes_Le_4KB", \
		"proxysql_polardb_perf_packet_bytes_le_4kb_total", \
		"Protocol packets in direct frontend writes of more than 2KB to 4KB") \
	T(perf_packet_bytes_le_8k, "PolarDB_Perf_Packet_Bytes_Le_8KB", \
		"proxysql_polardb_perf_packet_bytes_le_8kb_total", \
		"Protocol packets in direct frontend writes of more than 4KB to 8KB") \
	T(perf_packet_bytes_le_16k, "PolarDB_Perf_Packet_Bytes_Le_16KB", \
		"proxysql_polardb_perf_packet_bytes_le_16kb_total", \
		"Protocol packets in direct frontend writes of more than 8KB to 16KB") \
	T(perf_packet_bytes_le_32k, "PolarDB_Perf_Packet_Bytes_Le_32KB", \
		"proxysql_polardb_perf_packet_bytes_le_32kb_total", \
		"Protocol packets in direct frontend writes of more than 16KB to 32KB") \
	T(perf_packet_bytes_le_64k, "PolarDB_Perf_Packet_Bytes_Le_64KB", \
		"proxysql_polardb_perf_packet_bytes_le_64kb_total", \
		"Protocol packets in direct frontend writes of more than 32KB to 64KB") \
	T(perf_packet_bytes_gt_64k, "PolarDB_Perf_Packet_Bytes_Gt_64KB", \
		"proxysql_polardb_perf_packet_bytes_gt_64kb_total", \
		"Protocol packets in direct frontend writes larger than 64KB") \
	T(perf_plain_send_calls, "PolarDB_Perf_Plain_Send_Calls", \
		"proxysql_polardb_perf_plain_send_calls_total", \
		"Plaintext frontend sends through the buffered path") \
	T(perf_plain_send_bytes, "PolarDB_Perf_Plain_Send_Bytes", \
		"proxysql_polardb_perf_plain_send_bytes_total", \
		"Plaintext frontend bytes sent through the buffered path")
#else
#define POLARDB_PERF_DEBUG_THREAD_COUNTER_LIST(T)
#endif // POLARDB_PERF_DEBUG

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
	T(lsn_update_pass_overflow, "PolarDB_LSN_Update_Pass_Overflow", \
		"proxysql_polardb_lsn_update_pass_overflow_total", \
		"Worker LSN observations applied immediately after the pass cache filled") \
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
	T(client_rfq_lsn_raised_to_target, "PolarDB_Client_RFQ_LSN_Raised_To_Target", \
		"proxysql_polardb_client_rfq_lsn_raised_to_target_total", \
		"Client ReadyForQuery LSN payloads raised from backend RFQ value to a confirmed session or wait target") \
	T(client_rfq_lsn_raised_by_writer, "PolarDB_Client_RFQ_LSN_Raised_By_Writer", \
		"proxysql_polardb_client_rfq_lsn_raised_by_writer_total", \
		"Client ReadyForQuery LSN payloads raised because the response came from the current writer hostgroup") \
	T(client_rfq_lsn_raised_by_wait, "PolarDB_Client_RFQ_LSN_Raised_By_Wait", \
		"proxysql_polardb_client_rfq_lsn_raised_by_wait_total", \
		"Client ReadyForQuery LSN payloads raised because the query completed a successful LSN wait") \
	T(group_lsn_unknown, "PolarDB_Group_LSN_Unknown", \
		"proxysql_polardb_group_lsn_unknown_total", \
		"GLOBAL_LSN reads that could not use a known group LSN observation") \
	T(rfq_best_effort_degraded_routes, "PolarDB_RFQ_Best_Effort_Degraded_Routes", \
		"proxysql_polardb_rfq_best_effort_degraded_routes_total", \
		"Best-effort RFQ-unavailable reads routed without an LSN wait") \
	T(consistency_writer_fallback, "PolarDB_Consistency_Writer_Fallback", \
		"proxysql_polardb_consistency_writer_fallback_total", \
		"Consistency reads redirected to the writer after reader acquisition") \
	T(lag_cap_freshness_clamped, "PolarDB_Lag_Cap_Freshness_Clamped", \
		"proxysql_polardb_lag_cap_freshness_clamped_total", \
		"Reader acquisitions where byte-lag cap reduced the effective LSN-cache freshness window") \
	T(lag_cap_lsn_unknown, "PolarDB_Lag_Cap_LSN_Unknown", \
		"proxysql_polardb_lag_cap_lsn_unknown_total", \
		"Reader candidates rejected by byte-lag cap because their cached LSN was unknown") \
	T(lag_cap_lsn_stale, "PolarDB_Lag_Cap_LSN_Stale", \
		"proxysql_polardb_lag_cap_lsn_stale_total", \
		"Reader candidates rejected by byte-lag cap because their cached LSN sample was stale") \
	T(lag_cap_rejected, "PolarDB_Lag_Cap_Rejected", \
		"proxysql_polardb_lag_cap_rejected_total", \
		"Reader candidates rejected because cached byte lag exceeded max_lag_bytes") \
	T(lag_cap_accepted, "PolarDB_Lag_Cap_Accepted", \
		"proxysql_polardb_lag_cap_accepted_total", \
		"Reader candidates accepted by the byte-lag cap") \
	G(wait_reads_retried_on_reader, "PolarDB_Wait_Reads_Retried_On_Reader", \
		"proxysql_polardb_wait_reads_retried_on_reader_total", \
		"Wait reads retried once on another reader") \
	G(wait_reads_retried_on_writer, "PolarDB_Wait_Reads_Retried_On_Writer", \
		"proxysql_polardb_wait_reads_retried_on_writer_total", \
		"Wait reads retried once on the writer") \
	G(wait_retry_evaluated, "PolarDB_Wait_Retry_Evaluated", \
		"proxysql_polardb_wait_retry_evaluated_total", \
		"Wait-read failures evaluated by the retry-to-writer path") \
	G(wait_retry_attempted, "PolarDB_Wait_Retry_Attempted", \
		"proxysql_polardb_wait_retry_attempted_total", \
		"Wait-read failures with a rebuilt packet ready to move to the writer") \
	G(wait_retry_declined_policy_forward, "PolarDB_Wait_Retry_Declined_Policy_Forward", \
		"proxysql_polardb_wait_retry_declined_policy_forward_total", \
		"Wait-read retries declined because policy forwards the reader error") \
	G(wait_retry_declined_policy_terminate, "PolarDB_Wait_Retry_Declined_Policy_Terminate", \
		"proxysql_polardb_wait_retry_declined_policy_terminate_total", \
		"Wait-read retries declined because policy terminates the client session") \
	G(wait_retry_declined_target_not_writer, "PolarDB_Wait_Retry_Declined_Target_Not_Writer", \
		"proxysql_polardb_wait_retry_declined_target_not_writer_total", \
		"Wait-read retries declined because the configured retry target is not the writer") \
	G(wait_retry_declined_not_recoverable, "PolarDB_Wait_Retry_Declined_Not_Recoverable", \
		"proxysql_polardb_wait_retry_declined_not_recoverable_total", \
		"Wait-read retries declined because the failure class is not retryable") \
	G(wait_retry_declined_result_started, "PolarDB_Wait_Retry_Declined_Result_Started", \
		"proxysql_polardb_wait_retry_declined_result_started_total", \
		"Wait-read retries declined because user-result transfer had already started") \
	G(wait_retry_declined_writer_hg_unknown, "PolarDB_Wait_Retry_Declined_Writer_HG_Unknown", \
		"proxysql_polardb_wait_retry_declined_writer_hg_unknown_total", \
		"Wait-read retries declined because the writer hostgroup was unknown") \
	G(wait_retry_declined_original_query_missing, "PolarDB_Wait_Retry_Declined_Original_Query_Missing", \
		"proxysql_polardb_wait_retry_declined_original_query_missing_total", \
		"Wait-read retries declined because the original unwrapped query was not saved") \
	G(wait_retry_declined_writer_unavailable, "PolarDB_Wait_Retry_Declined_Writer_Unavailable", \
		"proxysql_polardb_wait_retry_declined_writer_unavailable_total", \
		"Wait-read retries declined because no writer backend stream was available") \
	G(wait_retry_declined_same_stream, "PolarDB_Wait_Retry_Declined_Same_Stream", \
		"proxysql_polardb_wait_retry_declined_same_stream_total", \
		"Wait-read retries declined because the writer stream was the failed reader stream") \
	G(wait_retry_declined_writer_busy, "PolarDB_Wait_Retry_Declined_Writer_Busy", \
		"proxysql_polardb_wait_retry_declined_writer_busy_total", \
		"Wait-read retries declined because the writer stream was not idle") \
	G(wait_retry_declined_packet_build_failed, "PolarDB_Wait_Retry_Declined_Packet_Build_Failed", \
		"proxysql_polardb_wait_retry_declined_packet_build_failed_total", \
		"Wait-read retries declined because rebuilding the simple-query packet failed") \
	G(wait_retry_declined_move_failed, "PolarDB_Wait_Retry_Declined_Move_Failed", \
		"proxysql_polardb_wait_retry_declined_move_failed_total", \
		"Wait-read retries declined because moving the retry packet to the writer failed") \
	T(rfq_profile_skipped, "PolarDB_RFQ_Profile_Skipped", \
		"proxysql_polardb_rfq_profile_skipped_total", \
		"Pooled backends skipped because their startup profile cannot return RFQ LSN") \
	G(rfq_profile_evicted, "PolarDB_RFQ_Profile_Evicted", \
		"proxysql_polardb_rfq_profile_evicted_total", \
		"Incompatible pooled backends evicted for RFQ-LSN-capable replacements") \
	T(target_lsn_preferred, "PolarDB_Target_LSN_Preferred", \
		"proxysql_polardb_target_lsn_preferred_total", \
		"Reader choices that preferred a cached LSN already at or above target") \
	T(target_lsn_fallback_wait, "PolarDB_Target_LSN_Fallback_Wait", \
		"proxysql_polardb_target_lsn_fallback_wait_total", \
		"Reader choices that kept the backend wait as the correctness check") \
	T(reader_pool_hit, "PolarDB_Reader_Pool_Hit", \
		"proxysql_polardb_reader_pool_hit_total", \
		"Reader acquisitions served by the PolarDB reader pool") \
	T(reader_pool_miss_empty, "PolarDB_Reader_Pool_Miss_Empty", \
		"proxysql_polardb_reader_pool_miss_empty_total", \
		"Reader pool lookups that found no usable pooled reader") \
	T(reader_pool_return_to_core, "PolarDB_Reader_Pool_Return_To_Core", \
		"proxysql_polardb_reader_pool_return_to_core_total", \
		"Reusable reader connections returned to the selected server core FREE list") \
	T(reader_pool_p2c_select, "PolarDB_Reader_Pool_P2C_Select", \
		"proxysql_polardb_reader_pool_p2c_select_total", \
		"Reader pool selections made with P2C") \
	T(reader_pool_p2c_second, "PolarDB_Reader_Pool_P2C_Second", \
		"proxysql_polardb_reader_pool_p2c_second_total", \
		"P2C selected the second sampled reader") \
	T(reader_pool_p2c_active_load, "PolarDB_Reader_Pool_P2C_Decide_Active_Load", \
		"proxysql_polardb_reader_pool_p2c_active_load_total", \
		"P2C decided by lower globally visible weight-normalized active load") \
	T(reader_pool_p2c_random, "PolarDB_Reader_Pool_P2C_Decide_Random", \
		"proxysql_polardb_reader_pool_p2c_random_total", \
		"P2C where global load/free counts tied and random tie-break selected the reader") \
	T(reader_pool_drop_offline, "PolarDB_Reader_Pool_Drop_Offline", \
		"proxysql_polardb_reader_pool_drop_offline_total", \
		"Reader pool readers dropped because their server was no longer ONLINE") \
	T(reader_pool_drop_unusable, "PolarDB_Reader_Pool_Drop_Ineligible", \
		"proxysql_polardb_reader_pool_drop_unusable_total", \
		"Reader pool readers dropped because they were no longer reusable") \
	T(reader_pool_drop_client_identity, "PolarDB_Reader_Pool_Drop_Client_Identity", \
		"proxysql_polardb_reader_pool_drop_client_identity_total", \
		"Reader pool readers closed instead of pooled because backend startup identity is client-specific") \
	T(reader_pool_lookup, "PolarDB_Reader_Pool_Lookup", \
		"proxysql_polardb_reader_pool_lookup_total", \
		"PolarDB reader-pool lookup attempts") \
	T(reader_pool_retry_after_config_change, "PolarDB_Reader_Pool_Retry_After_Config_Change", \
		"proxysql_polardb_reader_pool_retry_after_config_change_total", \
		"Cold reader creations retried after topology or startup configuration changed") \
	T(reader_pool_create_decision, "PolarDB_Reader_Pool_Create_Decision", \
		"proxysql_polardb_reader_pool_create_decision_total", \
		"ReaderPool cold-path decisions that reached exact-compatible backend creation admission") \
	T(reader_pool_create_issued, "PolarDB_Reader_Pool_Create_Issued", \
		"proxysql_polardb_reader_pool_create_issued_total", \
		"ReaderPool backend connection objects issued by cold creation") \
	T(reader_pool_create_connected, "PolarDB_Reader_Pool_Create_Connected", \
		"proxysql_polardb_reader_pool_create_connected_total", \
		"ReaderPool-created backend connections that completed connection establishment") \
	T(reader_pool_create_failed, "PolarDB_Reader_Pool_Create_Failed", \
		"proxysql_polardb_reader_pool_create_failed_total", \
		"ReaderPool-created backend connections that failed before establishment") \
	T(reader_pool_create_timeout, "PolarDB_Reader_Pool_Create_Timeout", \
		"proxysql_polardb_reader_pool_create_timeout_total", \
		"ReaderPool-created backend connections that exceeded the backend connect timeout") \
	T(reader_pool_idle_ping_take, "PolarDB_Reader_Pool_Idle_Ping_Take", \
		"proxysql_polardb_reader_pool_idle_ping_take_total", \
		"Exact pool connections removed from shared FREE for idle-ping maintenance") \
	T(reader_pool_idle_ping_return, "PolarDB_Reader_Pool_Idle_Ping_Return", \
		"proxysql_polardb_reader_pool_idle_ping_return_total", \
		"Idle-ping maintenance connections returned to normal ReaderPool ownership") \
	T(reader_pool_idle_ping_destroy, "PolarDB_Reader_Pool_Idle_Ping_Destroy", \
		"proxysql_polardb_reader_pool_idle_ping_destroy_total", \
		"Idle-ping maintenance connections destroyed before normal ReaderPool return") \
	T(reader_pool_idle_trim_deferred, "PolarDB_Reader_Pool_Idle_Trim_Deferred", \
		"proxysql_polardb_reader_pool_idle_trim_deferred_total", \
		"Excess shared FREE connections deferred for one maintenance pass before percentage trimming") \
	T(reader_pool_idle_trim_cancelled, "PolarDB_Reader_Pool_Idle_Trim_Cancelled", \
		"proxysql_polardb_reader_pool_idle_trim_cancelled_total", \
		"Pending percentage trims cancelled because the connection was taken, retained, or removed for another reason") \
	T(reader_pool_idle_trim_cancelled_taken, "PolarDB_Reader_Pool_Idle_Trim_Cancelled_Taken", \
		"proxysql_polardb_reader_pool_idle_trim_cancelled_taken_total", \
		"Pending percentage trims cancelled because the connection left shared FREE for use") \
	T(reader_pool_idle_trim_cancelled_retained, "PolarDB_Reader_Pool_Idle_Trim_Cancelled_Retained", \
		"proxysql_polardb_reader_pool_idle_trim_cancelled_retained_total", \
		"Pending percentage trims cancelled because shared FREE no longer exceeded its configured target") \
	T(reader_pool_idle_trim_cancelled_other, "PolarDB_Reader_Pool_Idle_Trim_Cancelled_Other", \
		"proxysql_polardb_reader_pool_idle_trim_cancelled_other_total", \
		"Pending percentage trims cancelled because another removal policy handled the connection") \
	T(reader_pool_idle_trim_destroyed, "PolarDB_Reader_Pool_Idle_Trim_Destroyed", \
		"proxysql_polardb_reader_pool_idle_trim_destroyed_total", \
		"Excess shared FREE connections destroyed after remaining idle across a maintenance pass") \
	T(reader_capacity_wait_enter, "PolarDB_Reader_Capacity_Wait_Enter", \
		"proxysql_polardb_reader_capacity_wait_enter_total", \
		"Sessions entering worker-local ReaderPool capacity waiting") \
	T(reader_capacity_wait_exit, "PolarDB_Reader_Capacity_Wait_Exit", \
		"proxysql_polardb_reader_capacity_wait_exit_total", \
		"Sessions leaving worker-local ReaderPool capacity waiting") \
	T(reader_capacity_wait_sum_us, "PolarDB_Reader_Capacity_Wait_Sum_Us", \
		"proxysql_polardb_reader_capacity_wait_microseconds_total", \
		"Total completed ReaderPool capacity wait time, in microseconds") \
	T(reader_capacity_wait_max_us, "PolarDB_Reader_Capacity_Wait_Max_Us", \
		"proxysql_polardb_reader_capacity_wait_max_microseconds_total", \
		"Largest completed ReaderPool capacity wait observed, in microseconds") \
	T(reader_capacity_retry_pass, "PolarDB_Reader_Capacity_Retry_Pass", \
		"proxysql_polardb_reader_capacity_retry_pass_total", \
		"Worker passes admitted to retry ReaderPool capacity waiters") \
	T(reader_capacity_retry_pass_deadline, "PolarDB_Reader_Capacity_Retry_Pass_Deadline", \
		"proxysql_polardb_reader_capacity_retry_pass_deadline_total", \
		"ReaderPool capacity retry passes started by the worker deadline") \
	T(reader_capacity_retry_pass_local, "PolarDB_Reader_Capacity_Retry_Pass_Local", \
		"proxysql_polardb_reader_capacity_retry_pass_local_total", \
		"ReaderPool capacity retry passes started by a same-pass local return") \
	T(reader_capacity_retry_attempt, "PolarDB_Reader_Capacity_Retry_Attempt", \
		"proxysql_polardb_reader_capacity_retry_attempt_total", \
		"Worker-admitted ReaderPool capacity retry attempts") \
	T(reader_capacity_retry_acquired, "PolarDB_Reader_Capacity_Retry_Acquired", \
		"proxysql_polardb_reader_capacity_retry_acquired_total", \
		"Worker-admitted capacity retries that acquired a reader") \
	T(reader_capacity_retry_selected_busy, "PolarDB_Reader_Capacity_Retry_Selected_Busy", \
		"proxysql_polardb_reader_capacity_retry_selected_busy_total", \
		"Worker-admitted retries that could not establish complete reader-group saturation") \
	T(reader_capacity_retry_group_busy, "PolarDB_Reader_Capacity_Retry_Group_Busy", \
		"proxysql_polardb_reader_capacity_retry_group_busy_total", \
		"Worker-admitted retries that found every currently eligible reader full") \
	T(reader_capacity_retry_scope_skipped, "PolarDB_Reader_Capacity_Retry_Scope_Skipped", \
		"proxysql_polardb_reader_capacity_retry_scope_skipped_total", \
		"Capacity waiters deferred after the same scope was confirmed full in a worker pass") \
	T(reader_capacity_wait_le_1ms, "PolarDB_Reader_Capacity_Wait_Le_1ms", \
		"proxysql_polardb_reader_capacity_wait_le_1ms_total", \
		"Completed ReaderPool capacity waits no longer than one millisecond") \
	T(reader_capacity_wait_le_5ms, "PolarDB_Reader_Capacity_Wait_Le_5ms", \
		"proxysql_polardb_reader_capacity_wait_le_5ms_total", \
		"Completed ReaderPool capacity waits over one and no longer than five milliseconds") \
	T(reader_capacity_wait_le_20ms, "PolarDB_Reader_Capacity_Wait_Le_20ms", \
		"proxysql_polardb_reader_capacity_wait_le_20ms_total", \
		"Completed ReaderPool capacity waits over five and no longer than twenty milliseconds") \
	T(reader_capacity_wait_le_100ms, "PolarDB_Reader_Capacity_Wait_Le_100ms", \
		"proxysql_polardb_reader_capacity_wait_le_100ms_total", \
		"Completed ReaderPool capacity waits over twenty and no longer than one hundred milliseconds") \
	T(reader_capacity_wait_le_1s, "PolarDB_Reader_Capacity_Wait_Le_1s", \
		"proxysql_polardb_reader_capacity_wait_le_1s_total", \
		"Completed ReaderPool capacity waits over one hundred milliseconds and no longer than one second") \
	T(reader_capacity_wait_gt_1s, "PolarDB_Reader_Capacity_Wait_Gt_1s", \
		"proxysql_polardb_reader_capacity_wait_gt_1s_total", \
		"Completed ReaderPool capacity waits longer than one second") \
	T(reader_pool_capacity_ownership_local, "PolarDB_ReaderPool_Capacity_Ownership_Local", \
		"proxysql_polardb_reader_pool_capacity_ownership_local_total", \
		"Capacity requests avoided because the worker held compatible local capacity") \
	T(reader_pool_capacity_ownership_active, "PolarDB_ReaderPool_Capacity_Ownership_Active", \
		"proxysql_polardb_reader_pool_capacity_ownership_active_total", \
		"Capacity requests avoided because the worker had compatible reusable active capacity") \
	T(reader_pool_capacity_ownership_reservation, "PolarDB_ReaderPool_Capacity_Ownership_Reservation", \
		"proxysql_polardb_reader_pool_capacity_ownership_reservation_total", \
		"Capacity requests avoided because the worker already had a compatible reservation") \
	T(reader_pool_capacity_ownership_zero, "PolarDB_ReaderPool_Capacity_Ownership_Zero", \
		"proxysql_polardb_reader_pool_capacity_ownership_zero_total", \
		"Complete reader-group busy results for workers with no compatible owned capacity") \
	T(reader_pool_retention_started, "PolarDB_ReaderPool_Retention_Started", \
		"proxysql_polardb_reader_pool_retention_started_total", \
		"Workers allowed to retain a compatible reader while local work remains") \
	T(reader_pool_retention_cleared, "PolarDB_ReaderPool_Retention_Cleared", \
		"proxysql_polardb_reader_pool_retention_cleared_total", \
		"Reader retention scopes cleared after compatible local work ended") \
	T(reader_pool_retained_connection_shared, "PolarDB_ReaderPool_Retained_Connection_Shared", \
		"proxysql_polardb_reader_pool_retained_connection_shared_total", \
		"Retained reader connections returned to shared matching or reserved for remote requests") \
	T(reader_pool_remote_request_seen, "PolarDB_ReaderPool_Remote_Request_Seen", \
		"proxysql_polardb_reader_pool_remote_request_seen_total", \
		"Compatible remote requests found while returning retained readers") \
	T(reader_pool_remote_reservation_attempt, "PolarDB_ReaderPool_Remote_Reservation_Attempt", \
		"proxysql_polardb_reader_pool_remote_reservation_attempt_total", \
		"Attempts to reserve retained readers for compatible remote requests") \
	T(reader_pool_remote_reservation_created, "PolarDB_ReaderPool_Remote_Reservation_Created", \
		"proxysql_polardb_reader_pool_remote_reservation_created_total", \
		"Retained readers successfully reserved for remote workers") \
	T(reader_pool_remote_reservation_not_created, "PolarDB_ReaderPool_Remote_Reservation_Not_Created", \
		"proxysql_polardb_reader_pool_remote_reservation_not_created_total", \
		"Remote reservation attempts that found no eligible request or connection") \
	T(reader_pool_capacity_request_registered, "PolarDB_ReaderPool_Capacity_Request_Registered", \
		"proxysql_polardb_reader_pool_capacity_request_registered_total", \
		"Cold-worker ReaderPool capacity requests registered") \
	T(reader_pool_capacity_request_cancelled, "PolarDB_ReaderPool_Capacity_Request_Cancelled", \
		"proxysql_polardb_reader_pool_capacity_request_cancelled_total", \
		"Pending ReaderPool capacity requests cancelled before a connection was reserved") \
	T(reader_pool_capacity_request_duplicate_token, "PolarDB_ReaderPool_Capacity_Request_Duplicate_Token", \
		"proxysql_polardb_reader_pool_capacity_request_duplicate_token_total", \
		"Capacity request registrations that repeated an active token") \
	T(reader_pool_capacity_request_duplicate_worker, "PolarDB_ReaderPool_Capacity_Request_Duplicate_Worker", \
		"proxysql_polardb_reader_pool_capacity_request_duplicate_worker_total", \
		"Capacity request registrations rejected because the worker already had an active request") \
	T(reader_pool_connection_reserved, "PolarDB_ReaderPool_Connection_Reserved", \
		"proxysql_polardb_reader_pool_connection_reserved_total", \
		"Connections reserved for waiting workers instead of normal ReaderPool matching") \
	T(reader_pool_reservation_acquired, "PolarDB_ReaderPool_Reservation_Acquired", \
		"proxysql_polardb_reader_pool_reservation_acquired_total", \
		"Worker-assigned FREE reader connections acquired") \
	T(reader_pool_reservation_released, "PolarDB_ReaderPool_Reservation_Released", \
		"proxysql_polardb_reader_pool_reservation_released_total", \
		"Connections from cancelled reservations returned to normal ReaderPool matching") \
	T(reader_pool_reservation_wake, "PolarDB_ReaderPool_Reservation_Wake", \
		"proxysql_polardb_reader_pool_reservation_wake_total", \
		"Worker wakeups sent after a connection was reserved") \
	T(reader_pool_reservation_wake_coalesced, "PolarDB_ReaderPool_Reservation_Wake_Coalesced", \
		"proxysql_polardb_reader_pool_reservation_wake_coalesced_total", \
		"Reader reservation wakes coalesced with an already pending worker wake") \
	T(reader_pool_reservation_missing, "PolarDB_ReaderPool_Reservation_Missing", \
		"proxysql_polardb_reader_pool_reservation_missing_total", \
		"Worker reservation states that found no active server-side reservation") \
	T(reader_pool_reservation_missing_retired, "PolarDB_ReaderPool_Reservation_Missing_Retired", \
		"proxysql_polardb_reader_pool_reservation_missing_retired_total", \
		"Worker reservation states resolved to a classified server-side retirement") \
	T(reader_pool_reservation_missing_unknown, "PolarDB_ReaderPool_Reservation_Missing_Unknown", \
		"proxysql_polardb_reader_pool_reservation_missing_unknown_total", \
		"Worker reservation states with no active, pending, or retired server-side token") \
	G(reader_pool_reservation_retired_create_evict, "PolarDB_ReaderPool_Reservation_Retired_Create_Evict", \
		"proxysql_polardb_reader_pool_reservation_retired_create_evict_total", \
		"Reader reservation tokens retired when FREE capacity was evicted for creation") \
	G(reader_pool_reservation_retired_idle_trim, "PolarDB_ReaderPool_Reservation_Retired_Idle_Trim", \
		"proxysql_polardb_reader_pool_reservation_retired_idle_trim_total", \
		"Reader reservation tokens retired by idle FREE connection trimming") \
	G(reader_pool_reservation_retired_max_age, "PolarDB_ReaderPool_Reservation_Retired_Max_Age", \
		"proxysql_polardb_reader_pool_reservation_retired_max_age_total", \
		"Reader reservation tokens retired by connection maximum-age cleanup") \
	G(reader_pool_reservation_retired_offline, "PolarDB_ReaderPool_Reservation_Retired_Offline", \
		"proxysql_polardb_reader_pool_reservation_retired_offline_total", \
		"Reader reservation tokens retired when their server became offline") \
	G(reader_pool_reservation_retired_pool_drop, "PolarDB_ReaderPool_Reservation_Retired_Pool_Drop", \
		"proxysql_polardb_reader_pool_reservation_retired_pool_drop_total", \
		"Reader reservation tokens retired while dropping a FREE pool") \
	G(reader_pool_reservation_retired_explicit, "PolarDB_ReaderPool_Reservation_Retired_Explicit", \
		"proxysql_polardb_reader_pool_reservation_retired_explicit_total", \
		"Reader reservation tokens retired by an explicit FREE connection removal") \
	G(reader_pool_reservation_retired_invalid, "PolarDB_ReaderPool_Reservation_Retired_Invalid", \
		"proxysql_polardb_reader_pool_reservation_retired_invalid_total", \
		"Reader reservation tokens retired after inconsistent FREE-list state was detected") \
	T(reader_pool_server_considered, "PolarDB_Reader_Pool_Server_Considered", \
		"proxysql_polardb_reader_pool_server_considered_total", \
		"Reader servers examined by PolarDB policy selection") \
	T(reader_pool_server_skip_unusable, "PolarDB_Reader_Pool_Server_Skip_Unusable", \
		"proxysql_polardb_reader_pool_server_skip_unusable_total", \
		"Reader pool reader servers skipped because status, weight, or latency made them unusable") \
	T(reader_pool_busy_alternate_hit, "PolarDB_Reader_Pool_Busy_Alternate_Hit", \
		"proxysql_polardb_reader_pool_busy_alternate_hit_total", \
		"Reader acquisitions served by an eligible alternate while the selected pool was busy") \
	T(reader_pool_busy_alternate_miss, "PolarDB_Reader_Pool_Busy_Alternate_Miss", \
		"proxysql_polardb_reader_pool_busy_alternate_miss_total", \
		"Selected pool was busy; the alternate returned no connection, so acquisition retried the selected reader normally") \
	T(reader_pool_match_attempt, "PolarDB_Reader_Pool_Match_Attempt", \
		"proxysql_polardb_reader_pool_match_attempt_total", \
		"Attempts to get a matching connection from an eligible reader") \
	T(reader_pool_match_miss, "PolarDB_Reader_Pool_Match_Miss", \
		"proxysql_polardb_reader_pool_match_miss_total", \
		"Selected-server attempts that found no matching connection") \
	T(reader_pool_conn_examined, "PolarDB_Reader_Pool_Conn_Examined", \
		"proxysql_polardb_reader_pool_conn_examined_total", \
		"Reader pool connections taken and checked") \
	T(reader_pool_reject_bad_context, "PolarDB_Reader_Pool_Reject_Bad_Context", \
		"proxysql_polardb_reader_pool_reject_bad_context_total", \
		"Reader pool connections rejected because required session or connection context was missing") \
	T(reader_pool_reject_profile, "PolarDB_Reader_Pool_Reject_Profile", \
		"proxysql_polardb_reader_pool_reject_profile_total", \
		"Reader pool connections rejected because their startup profile is incompatible with the hostgroup profile") \
	T(reader_pool_reject_auth, "PolarDB_Reader_Pool_Reject_Auth", \
		"proxysql_polardb_reader_pool_reject_auth_total", \
		"Reader pool connections rejected because user or database differed") \
	T(reader_pool_reject_identity, "PolarDB_Reader_Pool_Reject_Identity", \
		"proxysql_polardb_reader_pool_reject_identity_total", \
		"Reader pool connections rejected because PolarDB startup identity differed") \
	T(reader_pool_reject_session_state, "PolarDB_Reader_Pool_Reject_Session_State", \
		"proxysql_polardb_reader_pool_reject_session_state_total", \
		"Reader pool connections rejected because session state did not match") \
	T(reader_pool_key_full_check, "PolarDB_Reader_Pool_Key_Full_Check", \
		"proxysql_polardb_reader_pool_key_full_check_total", \
		"Reader pool reuse validations that needed core option/reset/session-variable checks") \
	T(reader_target_selected_lsn_unknown, "PolarDB_Reader_Target_Selected_LSN_Unknown", \
		"proxysql_polardb_reader_target_selected_lsn_unknown_total", \
		"Targeted reader acquisitions with no selected-reader LSN sample") \
	T(reader_target_selected_lsn_stale, "PolarDB_Reader_Target_Selected_LSN_Stale", \
		"proxysql_polardb_reader_target_selected_lsn_stale_total", \
		"Targeted reader acquisitions whose selected-reader LSN sample was stale") \
	T(reader_target_gap_zero, "PolarDB_Reader_Target_Gap_Zero", \
		"proxysql_polardb_reader_target_gap_zero_total", \
		"Targeted reader acquisitions whose selected-reader LSN was already at the target") \
	T(reader_target_gap_le_4kb, "PolarDB_Reader_Target_Gap_Le_4KB", \
		"proxysql_polardb_reader_target_gap_le_4kb_total", \
		"Targeted reader acquisitions with selected-reader LSN less than 4KB behind target") \
	T(reader_target_gap_le_64kb, "PolarDB_Reader_Target_Gap_Le_64KB", \
		"proxysql_polardb_reader_target_gap_le_64kb_total", \
		"Targeted reader acquisitions with selected-reader LSN less than 64KB behind target") \
	T(reader_target_gap_le_1mb, "PolarDB_Reader_Target_Gap_Le_1MB", \
		"proxysql_polardb_reader_target_gap_le_1mb_total", \
		"Targeted reader acquisitions with selected-reader LSN less than 1MB behind target") \
	T(reader_target_gap_le_16mb, "PolarDB_Reader_Target_Gap_Le_16MB", \
		"proxysql_polardb_reader_target_gap_le_16mb_total", \
		"Targeted reader acquisitions with selected-reader LSN less than 16MB behind target") \
	T(reader_target_gap_gt_16mb, "PolarDB_Reader_Target_Gap_Gt_16MB", \
		"proxysql_polardb_reader_target_gap_gt_16mb_total", \
		"Targeted reader acquisitions with selected-reader LSN more than 16MB behind target") \
	T(reader_target_selected_gap_samples, "PolarDB_Reader_Target_Selected_Gap_Samples", \
		"proxysql_polardb_reader_target_selected_gap_samples_total", \
		"Targeted reader acquisitions with a fresh selected-reader LSN sample") \
	T(reader_target_selected_gap_sum_bytes, "PolarDB_Reader_Target_Selected_Gap_Sum_Bytes", \
		"proxysql_polardb_reader_target_selected_gap_bytes_total", \
		"Total target gap in bytes observed on selected readers with fresh LSN samples") \
	T(reader_target_selection_compared, "PolarDB_Reader_Target_Selection_Compared", \
		"proxysql_polardb_reader_target_selection_compared_total", \
		"Targeted acquisitions comparing a fresh selected-reader LSN with the best fresh reader LSN sampled for that acquisition") \
	T(reader_target_selection_behind_best, "PolarDB_Reader_Target_Selection_Behind_Best", \
		"proxysql_polardb_reader_target_selection_behind_best_total", \
		"Targeted acquisitions that selected a reader behind the best fresh reader LSN sampled for that acquisition") \
	T(reader_target_selection_loss_bytes, "PolarDB_Reader_Target_Selection_Loss_Bytes", \
		"proxysql_polardb_reader_target_selection_loss_bytes_total", \
		"Total extra target gap relative to the best fresh reader LSN sampled for each acquisition") \
	G(session_target_epoch_reset, "PolarDB_Session_Target_Epoch_Reset", \
		"proxysql_polardb_session_target_epoch_reset_total", \
		"Session LSN targets cleared after writer epoch changes") \
	T(query_parser_init, "PolarDB_Query_Parser_Init", \
		"proxysql_polardb_query_parser_init_total", \
		"PgSQL queries submitted to the query parser/digest initializer") \
	T(query_parser_init_bytes, "PolarDB_Query_Parser_Init_Bytes", \
		"proxysql_polardb_query_parser_init_bytes_total", \
		"Query bytes submitted to the PgSQL parser/digest initializer") \
	T(query_parser_init_digest_enabled, "PolarDB_Query_Parser_Init_Digest_Enabled", \
		"proxysql_polardb_query_parser_init_digest_enabled_total", \
		"PgSQL parser initializations made while query digest collection was enabled") \
	T(query_parser_init_commands_enabled, "PolarDB_Query_Parser_Init_Commands_Enabled", \
		"proxysql_polardb_query_parser_init_commands_enabled_total", \
		"PgSQL parser initializations made while command statistics were enabled") \
	T(query_parser_command_type, "PolarDB_Query_Parser_Command_Type", \
		"proxysql_polardb_query_parser_command_type_total", \
		"PgSQL command-type classifications requested from the parser") \
	T(query_parser_update, "PolarDB_Query_Parser_Update", \
		"proxysql_polardb_query_parser_update_total", \
		"PgSQL query-parser statistic updates attempted at query end") \
	T(query_parser_update_skipped_none, "PolarDB_Query_Parser_Update_Skipped_None", \
		"proxysql_polardb_query_parser_update_skipped_none_total", \
		"PgSQL query-parser statistic updates skipped because the query had no parser state") \
	T(query_parser_update_skipped_uninitialized, "PolarDB_Query_Parser_Update_Skipped_Uninitialized", \
		"proxysql_polardb_query_parser_update_skipped_uninitialized_total", \
		"PgSQL query-parser statistic updates skipped because the parser command was still uninitialized") \
	T(query_parser_update_with_digest, "PolarDB_Query_Parser_Update_With_Digest", \
		"proxysql_polardb_query_parser_update_with_digest_total", \
		"PgSQL query-parser statistic updates that carried a digest text") \
	T(result_process, "PolarDB_Result_Process", \
		"proxysql_polardb_result_process_total", \
		"PolarDB result-processing observations run at query completion") \
	T(result_process_write_classify, "PolarDB_Result_Process_Write_Classify", \
		"proxysql_polardb_result_process_write_classify_total", \
		"PolarDB result-processing calls that classified the completed query as read or write") \
	T(result_process_write_classify_text, "PolarDB_Result_Process_Write_Classify_Text", \
		"proxysql_polardb_result_process_write_classify_text_total", \
		"PolarDB result-processing classifications that had query text available") \
	T(parent_bytes_flush_threshold_recv, "PolarDB_Parent_Bytes_Flush_Threshold_Recv", \
		"proxysql_polardb_parent_bytes_flush_threshold_recv_total", \
		"Parent byte flushes caused by the backend recv-byte threshold") \
	T(parent_bytes_flush_threshold_sent, "PolarDB_Parent_Bytes_Flush_Threshold_Sent", \
		"proxysql_polardb_parent_bytes_flush_threshold_sent_total", \
		"Parent byte flushes caused by the backend sent-byte threshold") \
	T(parent_bytes_flush_destructor, "PolarDB_Parent_Bytes_Flush_Destructor", \
		"proxysql_polardb_parent_bytes_flush_destructor_total", \
		"Parent byte flushes made while destroying a backend connection") \
	T(parent_bytes_flush_no_parent, "PolarDB_Parent_Bytes_Flush_No_Parent", \
		"proxysql_polardb_parent_bytes_flush_no_parent_total", \
		"Pending parent bytes dropped because no server container was attached") \
	T(parent_bytes_flush_recv_atomic, "PolarDB_Parent_Bytes_Flush_Recv_Atomic", \
		"proxysql_polardb_parent_bytes_flush_recv_atomic_total", \
		"Shared parent recv-byte atomic updates after coalescing") \
	T(parent_bytes_flush_sent_atomic, "PolarDB_Parent_Bytes_Flush_Sent_Atomic", \
		"proxysql_polardb_parent_bytes_flush_sent_atomic_total", \
		"Shared parent sent-byte atomic updates after coalescing") \
	T(parent_bytes_flush_recv_bytes, "PolarDB_Parent_Bytes_Flush_Recv_Bytes", \
		"proxysql_polardb_parent_bytes_flush_recv_bytes_total", \
		"Recv bytes flushed to shared parent counters after coalescing") \
	T(parent_bytes_flush_sent_bytes, "PolarDB_Parent_Bytes_Flush_Sent_Bytes", \
		"proxysql_polardb_parent_bytes_flush_sent_bytes_total", \
		"Sent bytes flushed to shared parent counters after coalescing") \
	T(writev_attempts, "PolarDB_WriteV_Attempts", \
		"proxysql_polardb_writev_attempts_total", \
		"Plaintext PgSQL frontend direct scatter/gather send attempts") \
	T(writev_bytes, "PolarDB_WriteV_Bytes", \
		"proxysql_polardb_writev_bytes_total", \
		"Bytes sent through the direct scatter/gather path") \
	T(writev_packets, "PolarDB_WriteV_Packets", \
		"proxysql_polardb_writev_packets_total", \
		"Fully-sent packets consumed by the direct scatter/gather path") \
	T(writev_short_writes, "PolarDB_WriteV_Short_Writes", \
		"proxysql_polardb_writev_short_writes_total", \
		"Direct scatter/gather sends that wrote less than the built view") \
	T(writev_wouldblock, "PolarDB_WriteV_WouldBlock", \
		"proxysql_polardb_writev_wouldblock_total", \
		"Direct scatter/gather sends returning EAGAIN, EWOULDBLOCK, or EINTR") \
	T(writev_errors, "PolarDB_WriteV_Errors", \
		"proxysql_polardb_writev_errors_total", \
		"Direct scatter/gather sends returning a hard error") \
	T(writev_buffered_fallback, "PolarDB_WriteV_Buffered_Fallback", \
		"proxysql_polardb_writev_buffered_fallback_total", \
		"PgSQL frontend writes forced back to queueOUT buffering") \
	T(writev_small_batch_fallback, "PolarDB_WriteV_Small_Batch_Fallback", \
		"proxysql_polardb_writev_small_batch_fallback_total", \
		"PgSQL frontend writes kept on the buffered path because they fit in one queue buffer") \
	T(output_coalesce_hold, "PolarDB_Output_Coalesce_Hold", \
		"proxysql_polardb_output_coalesce_hold_total", \
		"Incomplete streaming frontend output flushes deferred") \
	T(output_coalesce_flush_budget, "PolarDB_Output_Coalesce_Flush_Budget", \
		"proxysql_polardb_output_coalesce_flush_budget_total", \
		"Incomplete streaming output flushed after the coalesce budget") \
	T(output_coalesce_flush_backpressure, "PolarDB_Output_Coalesce_Flush_Backpressure", \
		"proxysql_polardb_output_coalesce_flush_backpressure_total", \
		"Coalesce skipped because output or socket state already had pending bytes") \
	T(result_row_run_attempts, "PolarDB_Result_Row_Run_Attempts", \
		"proxysql_polardb_result_row_run_attempts_total", \
		"Attempts to detach a pending backend DataRow run") \
	T(result_row_run_used, "PolarDB_Result_Row_Run_Used", \
		"proxysql_polardb_result_row_run_used_total", \
		"Backend DataRow runs forwarded as one result packet") \
	T(result_row_run_frames, "PolarDB_Result_Row_Run_Frames", \
		"proxysql_polardb_result_row_run_frames_total", \
		"DataRow frames forwarded through row-run fast-forward") \
	T(result_row_run_bytes, "PolarDB_Result_Row_Run_Bytes", \
		"proxysql_polardb_result_row_run_bytes_total", \
		"Bytes forwarded through row-run fast-forward") \
	T(result_row_run_unavailable, "PolarDB_Result_Row_Run_Unavailable", \
		"proxysql_polardb_result_row_run_unavailable_total", \
		"Row-run probes that fell back to normal result handling") \
	T(result_row_run_partial, "PolarDB_Result_Row_Run_Partial", \
		"proxysql_polardb_result_row_run_partial_total", \
		"Row-run probes that saw an incomplete DataRow frame") \
	T(result_row_run_not_candidate, "PolarDB_Result_Row_Run_Not_Candidate", \
		"proxysql_polardb_result_row_run_not_candidate_total", \
		"Row-run checks skipped because libpq was not positioned at a DataRow frame") \
	T(session_lsn_routing, "PolarDB_Session_LSN_Routing", \
		"proxysql_polardb_session_lsn_routing_total", \
		"Reads routed to a reader with a session-LSN wait requirement") \
	T(global_lsn_routing, "PolarDB_Global_LSN_Routing", \
		"proxysql_polardb_global_lsn_routing_total", \
		"Reads routed to a reader with a global-LSN wait requirement") \
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
	T(txn_wait_reader_reconciled, "PolarDB_Txn_Wait_Reader_Reconciled", \
		"proxysql_polardb_txn_wait_reader_reconciled_total", \
		"Temporary transaction wait reader ownership restored at request entry") \
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
		"Manual-route queries overridden to the writer by reader-failure safety handling") \
	T(route_locked_hostgroup, "PolarDB_Route_Locked_Hostgroup", \
		"proxysql_polardb_route_locked_hostgroup_total", \
		"Queries where a session hostgroup lock skipped automatic PolarDB routing") \
	T(wait_wrap_prepared, "PolarDB_Wait_Wrap_Prepared", \
		"proxysql_polardb_wait_wrap_prepared_total", \
		"Wait wrappers activated after the selected reader was found behind the consistency target") \
	T(wait_wrap_bypassed, "PolarDB_Wait_Wrap_Bypassed", \
		"proxysql_polardb_wait_wrap_bypassed_total", \
		"Backend LSN waits skipped because the selected reader already reached the consistency target LSN") \
	T(txn_reader_reuse_bypass_checked, "PolarDB_Txn_Reader_Reuse_Bypass_Checked", \
		"proxysql_polardb_txn_reader_reuse_bypass_checked_total", \
		"Retained transaction-reader connections checked for a confirmed target LSN") \
	T(txn_reader_reuse_bypass_allowed, "PolarDB_Txn_Reader_Reuse_Bypass_Allowed", \
		"proxysql_polardb_txn_reader_reuse_bypass_allowed_total", \
		"Retained transaction-reader connections whose exact RFQ LSN allowed wait-wrapper bypass") \
	G(wait_wrap_safety_abort, "PolarDB_Wait_Wrap_Safety_Abort", \
		"proxysql_polardb_wait_wrap_safety_abort_total", \
		"Wrap build failures that aborted the wait") \
	T(wait_lsn_sent, "PolarDB_Wait_LSN_Sent", \
		"proxysql_polardb_wait_lsn_sent_total", \
		"LSN wait wrappers successfully sent") \
	T(wait_lsn_sum_us, "PolarDB_Wait_LSN_Sum_Us", \
		"proxysql_polardb_wait_lsn_microseconds_total", \
		"Total ProxySQL-observed response time for wait-wrapped LSN reads, including query execution and transport") \
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
	T(split_fallback_group_lsn_unknown, "PolarDB_Split_Fallback_Group_LSN_Unknown", \
		"proxysql_polardb_split_fallback_group_lsn_unknown_total", \
		"Transaction-split reader acquisition fell back because lag-cap policy had no group LSN sample") \
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
	T(split_rejected_no_marker, "PolarDB_Split_Rejected_No_Marker", \
		"proxysql_polardb_split_rejected_no_marker_total", \
		"Transaction reads kept on the primary because RFQ state had no usable split or pre-write marker") \
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
	POLARDB_PERF_DEBUG_THREAD_COUNTER_LIST(T) \
	T(split_lsn_wait_count, "PolarDB_Split_LSN_Wait_Count", \
		"proxysql_polardb_split_lsn_wait_count_total", \
		"LSN wait wrappers prepared for transaction-split reads") \
	T(split_lsn_wait_sum_us, "PolarDB_Split_LSN_Wait_Sum_Us", \
		"proxysql_polardb_split_lsn_wait_microseconds_total", \
		"Total ProxySQL-observed response time for transaction-split reads carrying an LSN wait wrapper") \
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
	G(split_warmup_target_attempts, "PolarDB_Split_Warmup_Target_Attempts", \
		"proxysql_polardb_split_warmup_target_attempts_total", \
		"Backend connect attempts produced by split pool warmup requests") \
	G(split_warmup_created, "PolarDB_Split_Warmup_Created", \
		"proxysql_polardb_split_warmup_created_total", \
		"Lazy split pool warmup connections added to replica pools") \
	G(split_warmup_connection_reserved, "PolarDB_Split_Warmup_Connection_Reserved", \
		"proxysql_polardb_split_warmup_connection_reserved_total", \
		"Warmup-created split connections reserved immediately for waiting workers") \
	G(split_warmup_failed, "PolarDB_Split_Warmup_Failed", \
		"proxysql_polardb_split_warmup_failed_total", \
		"Lazy split pool warmup base requests rejected or completed without a target") \
	G(split_warmup_target_failed, "PolarDB_Split_Warmup_Target_Failed", \
		"proxysql_polardb_split_warmup_target_failed_total", \
		"Lazy split pool warmup target backends that failed before entering the pool") \
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
	G(split_warmup_rfq_unavailable, "PolarDB_Split_Warmup_RFQ_Unavailable", \
		"proxysql_polardb_split_warmup_rfq_unavailable_total", \
		"Lazy split pool warmup requests skipped because the reader hostgroup does not request RFQ LSN") \
	G(split_warmup_connect_failed, "PolarDB_Split_Warmup_Connect_Failed", \
		"proxysql_polardb_split_warmup_connect_failed_total", \
		"Lazy split pool warmup backends whose connection handshake failed") \
	G(split_warmup_add_failed, "PolarDB_Split_Warmup_Add_Failed", \
		"proxysql_polardb_split_warmup_add_failed_total", \
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

#define POLARDB_THREAD_MAX_COUNTER_LIST(X) \
	X(reader_capacity_wait_max_us)

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
