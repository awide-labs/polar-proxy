/**
 * @file polardb_unit_domains.h
 * @brief Test-domain runners linked into polardb_hgm_lsn_unit-t.
 *
 * Domain files define runners only: they do not call plan(), initialize global
 * ProxySQL state, or clean it up. The HGM test main owns initialization, runner
 * order, and cleanup; the topology shutdown runner must remain last.
 */

#ifndef POLARDB_UNIT_DOMAINS_H
#define POLARDB_UNIT_DOMAINS_H

// Reader selection policy.
void run_polardb_reader_load_policy_tests();
void run_polardb_reader_basic_selection_tests();
void run_polardb_reader_multi_selection_tests();
void run_polardb_reader_lag_predicate_tests();
void run_polardb_reader_target_policy_tests();
void run_polardb_reader_scale_tests();

// ReaderPool storage, reuse, return, and maintenance.
void run_polardb_reader_pool_index_tests();
void run_polardb_reader_pool_concurrency_tests();
void run_polardb_reader_pool_maintenance_tests();
void run_polardb_reader_pool_reuse_tests();
void run_polardb_reader_pool_return_tests();
void run_polardb_reader_pool_pooled_only_tests();

// ReaderPool capacity waiting and reservations.
void run_polardb_reader_reservation_lifecycle_tests();
void run_polardb_reader_capacity_admission_tests();
void run_polardb_reader_capacity_retention_tests();

// Transaction split, retained readers, and warmup.
void run_polardb_split_routing_tests();
void run_polardb_split_warmup_inventory_tests();
void run_polardb_split_protocol_tests();
void run_polardb_retained_reader_retry_tests();
void run_polardb_retained_reader_lifecycle_tests();
void run_polardb_split_warmup_policy_tests();
void run_polardb_split_warmup_thread_tests();

// Thread counters and frontend I/O.
void run_polardb_thread_io_tests();

// Topology state and lifetime.
void run_polardb_topology_state_tests();
void run_polardb_topology_lsn_tests();
void run_polardb_topology_snapshot_tests();
void run_polardb_topology_connection_lifetime_tests();
void run_polardb_topology_shutdown_tests();

// Consistency targets, wait accounting, and session state.
void run_polardb_consistency_counter_tests();
void run_polardb_consistency_profile_tests();
void run_polardb_consistency_target_tests();
void run_polardb_consistency_wait_cache_tests();
void run_polardb_session_state_tests();

// Re-executed before ProxySQL initialization with recoverable jemalloc OOM.
int run_polardb_libpq_prepare_oom_child();

#endif // POLARDB_UNIT_DOMAINS_H
