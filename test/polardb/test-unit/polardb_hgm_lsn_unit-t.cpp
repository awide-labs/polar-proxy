/**
 * @file polardb_hgm_lsn_unit-t.cpp
 * @brief Unit tests for the PolarDB HostGroups Manager LSN state.
 *
 * Domain: counter metadata and thread-counter aggregation, writer-epoch
 * LSN-cache reset, and thread-local fresh-LSN reader targeting.
 */

#include "tap.h"
#include "test_globals.h"
#include "test_init.h"

#include "proxysql.h"
#include "proxysql_glovars.hpp"
#include "cpp.h"
#include "PgSQL_Data_Stream.h"
#include "PgSQL_ExplicitTxnStateMgr.h"
#include "PgSQL_PolarDB_ReaderPool.h"
extern "C" {
#include "postgres_fe.h"
#include "libpq-int.h"
}
#undef snprintf
#undef vsnprintf

#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <unistd.h>

extern PgSQL_HostGroups_Manager *PgHGM;
extern PgSQL_Threads_Handler *GloPTH;

#if !POLARDB_PROXY
int main() {
	plan(1);
	ok(1, "PolarDB HGM LSN unit skipped when POLARDB_PROXY is disabled");
	return exit_status();
}
#else

// This binary links libproxysql.a and runs the domain files against shared
// ProxySQL HGM, worker, session, and connection components.
#include "polardb_unit_common.h"
#include "polardb_unit_support.h"
#include "polardb_unit_domains.h"

int main() {
	if (getenv("POLARDB_LIBPQ_PREPARE_OOM_CHILD")) {
		return run_polardb_libpq_prepare_oom_child();
	}

	plan(NO_PLAN);

	int rc = test_init_minimal();
	ok(rc == 0, "test_init_minimal() succeeds");

	rc = test_init_query_processor();
	ok(rc == 0, "test_init_query_processor() succeeds");
	if (GloPTH) {
		GloPTH->variables.hostgroup_manager_verbose = 0;
	}

	rc = test_init_hostgroups();
	ok(rc == 0, "test_init_hostgroups() succeeds");

	// Thread counters and frontend I/O.
	run_polardb_thread_io_tests();

	// Topology state, LSN updates, snapshots, and connection lifetime.
	run_polardb_topology_state_tests();
	run_polardb_topology_lsn_tests();
	run_polardb_topology_snapshot_tests();
	run_polardb_topology_connection_lifetime_tests();

	// Consistency targets, wait accounting, and session state.
	run_polardb_consistency_counter_tests();
	run_polardb_consistency_profile_tests();
	run_polardb_consistency_target_tests();
	run_polardb_consistency_wait_cache_tests();
	run_polardb_session_state_tests();

	// Reader selection policy.
	run_polardb_reader_load_policy_tests();
	run_polardb_reader_basic_selection_tests();
	run_polardb_reader_multi_selection_tests();
	run_polardb_reader_lag_predicate_tests();
	run_polardb_reader_target_policy_tests();
	run_polardb_reader_scale_tests();

	// ReaderPool storage, reuse, return, and maintenance.
	run_polardb_reader_pool_index_tests();
	run_polardb_reader_pool_concurrency_tests();
	run_polardb_reader_pool_maintenance_tests();
	run_polardb_reader_pool_reuse_tests();
	run_polardb_reader_pool_return_tests();
	run_polardb_reader_pool_pooled_only_tests();

	// ReaderPool capacity waiting and reservations.
	run_polardb_reader_reservation_lifecycle_tests();
	run_polardb_reader_capacity_admission_tests();
	run_polardb_reader_capacity_retention_tests();

	// Transaction split, retained readers, and warmup.
	run_polardb_split_routing_tests();
	run_polardb_split_warmup_inventory_tests();
	run_polardb_split_protocol_tests();
	run_polardb_retained_reader_retry_tests();
	run_polardb_retained_reader_lifecycle_tests();
	run_polardb_split_warmup_policy_tests();
	run_polardb_split_warmup_thread_tests();

	// This test clears HGM snapshots and must remain last.
	run_polardb_topology_shutdown_tests();

	test_cleanup_hostgroups();
	test_cleanup_query_processor();
	test_cleanup_minimal();

	return exit_status();
}

#endif // POLARDB_PROXY
