/**
 * @file polardb_unit_support.h
 * @brief Shared construction and inspection helpers for PolarDB component tests.
 *
 * These helpers create real ProxySQL hostgroup, session, worker, and connection
 * objects. Focused value and parser tests should use polardb_unit_common.h
 * instead. See README.md for helper choice, ownership, and test structure.
 */

#ifndef POLARDB_UNIT_SUPPORT_H
#define POLARDB_UNIT_SUPPORT_H

#include "PgSQL_PolarDB.h"
#include "PgSQL_Error_Helper.h"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

class PgSQL_Connection;
class PgSQL_HGC;
class PgSQL_HostGroups_Manager;
class PgSQL_PolarDB_ReaderPool;
class PgSQL_Session;
class PgSQL_SrvC;
class PgSQL_SrvConnList;
class PgSQL_Thread;
class PgSQL_Threads_Handler;
class SQLite3_result;
struct PgSQL_PoolMatchKey;
struct PgSQL_SplitWarmupRequest;
struct pg_conn;
typedef struct pg_conn PGconn;

// Build explicit configuration rows or locate objects already owned by HGM.
// Row builders are intended for immediate handoff to HGM staging methods.
SQLite3_result* make_polardb_replication_row_with_protocol(
	int writer_hg, int reader_hg, const char* proxy_protocol,
	const char* txn_split = "0");
SQLite3_result* make_polardb_replication_row(
	int writer_hg, int reader_hg);
SQLite3_result* make_pgsql_servers_result(
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port,
	int writer_max_connections = 50);
SQLite3_result* make_pgsql_servers_result_two_readers(
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1,
	int first_reader_max_connections = 50,
	int second_reader_max_connections = 50);
PgSQL_SrvC* find_pgsql_server(
	PgSQL_HGC* hostgroup, const char* address, int port);
bool find_prometheus_counter_value(const char* name, double* value);
bool find_prometheus_gauge_value(const char* name, double* value);
void set_test_userinfo(PgSQL_Connection* conn);
void set_test_pgsql_defaults(PgSQL_Connection* conn);
PgSQL_Connection* make_cached_reader_connection(PgSQL_SrvC* reader);
void stage_polardb_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);
void stage_polardb_topology_with_txn_split(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);
void stage_polardb_topology_two_readers(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1);

// Narrow test-only access to component state that is not public production API.
struct PolarDB_ReaderRetentionUnitAccess {
	static void set_worker_index(
		PgSQL_Thread* worker, unsigned int worker_index);
	static void set_local_connection_count(
		PgSQL_Thread* worker, unsigned int count);
	static unsigned int local_connection_count(
		const PgSQL_Thread* worker);
};

struct PolarDB_WorkerLifecycleUnitAccess {
	static std::mutex& mutex(PgSQL_Threads_Handler* handler);
	static std::mutex& wake_mutex(PgSQL_Thread* worker);
	static void set_shutdown(
		PgSQL_Threads_Handler* handler, bool shutdown_started);
};

struct PolarDB_SessionUnitAccess {
	static void queue_test_parse(PgSQL_Session* session);
	static void queue_test_bind(PgSQL_Session* session);
	static void queue_test_describe(PgSQL_Session* session, char stmt_type);
	static void queue_test_execute(PgSQL_Session* session);
	struct ReaderFailureDecision {
		PolarDB_ReaderFailureKind kind;
		PolarDB_ReaderAction action;
		PolarDB_RetryTarget retry_target;
		PolarDB_ReaderFailureRoute reader_failure_route;
		bool allow_writer_retry;
	};
	struct ReaderFailurePacketResult {
		PolarDB_FailureAction action;
		bool failure_owns_packet;
		bool request_stream_owns_packet;
		bool retry_counters_unchanged;
	};
	struct ReaderFailureEvidence {
		bool timeout;
		bool reusable;
		bool connected;
		bool has_backend_error;
		PGSQL_ERROR_CODES backend_error_code;
	};
	struct ExtendedFrameOwnerResult {
		bool selected;
		bool carried_across_previous_hostgroup_change;
		bool ownership_deferred_until_send;
		bool backend_claimed;
		bool backend_switch_rejected;
		int selected_hostgroup;
		int backend_hostgroup;
		bool cleared;
	};
	struct ExtendedFrameTransferResult {
		bool backend_claimed;
		bool backend_sync_reset;
		int selected_hostgroup;
		int backend_hostgroup;
	};
	struct ExtendedQpoHostgroupResult {
		bool first_destination_selected;
		bool first_destination_claimed;
		bool later_destination_rejected;
		bool missing_previous_uses_default;
		int selected_hostgroup;
		int backend_hostgroup;
	};
	struct BoundPortalIdentityResult {
		uint64_t bound_statement_id;
		uint64_t replacement_statement_id;
		uint64_t execute_statement_id;
		bool bind_did_not_copy_shared_owner;
		bool replacement_transferred_existing_owner;
		bool close_transferred_existing_owner;
	};
	struct PoisonedExtendedFrameResult {
		bool flush_emits_error_without_ready;
		bool flush_waits_for_client_sync;
		bool sync_emits_ready_in_error;
		bool sync_completes_frame;
		bool consumed_sync_emits_one_ready_in_error;
	};
	struct ExtendedFrameRouteResult {
		bool backend_route_pending;
		unsigned int backend_candidates;
		bool writer_required;
	};
	struct ExtendedFrameActivityTransitionResult {
		bool backend_route_pending;
		unsigned int backend_candidates;
		bool writer_required;
	};
	struct ExtendedBackendRouteResult {
		bool continued;
		bool route_consumed;
	};
	struct ExtendedFrameSyncResult {
		bool ready_before_backend;
		bool ready_after_flush;
		bool ready_after_sync;
		bool rfq_pending_after_flush;
		bool rfq_pending_after_sync;
	};
	struct DeferredRfqResult {
		bool pending_after_first_execute;
		bool publication_pending_after_first_execute;
		bool write_attribution_preserved;
		bool writer_scope_preserved;
		bool wait_target_preserved;
		bool staged_reset_preserved_frame_state;
		bool direct_publication_cleared_after_result;
		bool unknown_after_abandon;
		bool state_reset_after_abandon;
		bool inactive_reset_abandoned_orphan;
		bool error_publication_policy_preserved;
		bool error_publication_cleared_after_emit;
		bool error_publication_abandoned_on_failure;
		bool error_sync_publication_preserved;
		bool error_sync_result_attribution_discarded;
	};
	struct DeferredScopeMismatchResult {
		bool aggregate_target_invalidated;
		bool direct_rfq_uses_current_backend_lsn;
		bool standalone_sync_uses_current_backend_lsn;
		bool released_backend_sync_uses_retained_lsn;
		bool attached_backend_sync_uses_retained_lsn;
		bool emitter_uses_current_backend_lsn;
	};
	struct ExtendedFrameBatchPinResult {
		bool reroute_required_after_backend_claim;
		bool backend_stays_pinned;
		bool backend_switch_rejected;
		bool local_batch_can_be_reclassified;
	};
	struct ExtendedFrameErrorRecoveryResult {
		bool waits_for_client_sync;
		bool discards_parse;
		bool discards_flush;
		bool accepts_sync;
		bool accepts_terminate;
		bool ready_before_sync;
		bool ready_after_sync;
		bool state_cleared;
	};
	struct ExtendedReadyOwnershipResult {
		bool backend_error_deferred;
		bool backend_ready_once;
		bool backend_ready_not_duplicated;
		bool control_resync_deferred;
		bool control_resync_ready_once;
		bool local_ready_once;
		bool local_ready_not_duplicated;
		bool idle_local_ready_each_time;
		bool idle_local_does_not_claim_frame;
		bool describe_does_not_stage_wait;
	};
	struct ErrorPacketOwnershipResult {
		bool nonfatal_without_ready_is_error_only;
		bool nonfatal_with_ready_has_one_rfq;
		bool fatal_without_ready_is_error_only;
		bool fatal_with_ready_is_error_only;
	};
	struct ForwardedErrorOwnershipResult {
		bool flush_error_is_error_only;
		bool queued_messages_discarded;
		bool waits_for_client_sync;
		bool sync_adds_one_rfq;
		bool transaction_rfq_preserved;
		bool frame_completed;
	};
	struct ManualRouteResult {
		bool route_left_unchanged;
		bool scope_reset;
		bool reader_plan_reset;
		bool txn_reader_reconciled;
		unsigned long long manual_total_delta;
		unsigned long long manual_other_delta;
	};
	struct ExtendedFrameInvariantResult {
		bool candidate_underflow_rejected;
		bool queue_shape_failed_closed;
		bool candidate_balance_repaired;
		bool command_owner_mismatch_rejected;
		bool sync_owner_mismatch_rejected;
	};
	struct PinnedFrameConflictResult {
		bool operation_rejected;
		bool sqlstate_is_p0001;
		bool flush_emits_error_only;
		bool waits_for_client_sync;
		bool sync_emits_only_rfq;
	};

	static void set_extended_route_state(
		PgSQL_Session* session, int replica_eligible,
		bool force_primary_hint, uint8_t phase);
	static void set_extended_request_boundary(
		PgSQL_Session* session, uint8_t phase, bool pending_message);
	static bool extended_request_continues(
		const PgSQL_Session* session,
		bool called_on_failure, bool result_has_error);
	static void clear_request_state_for_query_end(
		PgSQL_Session* session, PgSQL_Data_Stream* backend_myds,
		bool called_on_failure);
	static ReaderFailureDecision reader_failure_decision(
		PgSQL_Session* session, bool timeout, bool reusable,
		PGSQL_ERROR_CODES backend_error_code,
		PolarDB_LsnWaitTimeoutAction timeout_action,
		PolarDB_ReplicaErrorAction replica_error_action);
	static bool request_has_wait_timeout_evidence(bool structured_timeout);
	static ReaderFailurePacketResult finish_reusable_wait_error(
		PgSQL_Session* session, bool query_aliases_packet);
	static ReaderFailureEvidence preserve_captured_wait_failure_evidence(
		PgSQL_Session* session);
	static bool execute_qpo_error_is_terminal(PgSQL_Session* session);
	static bool extended_dispatch_error_clears_frame(PgSQL_Session* session);
	static bool extended_local_flush_error_waits_for_sync(
		PgSQL_Session* session);
	static bool ordinary_sync_frame_uses_lazy_state(PgSQL_Session* session);
	static ExtendedFrameOwnerResult exercise_extended_frame_owner(
		PgSQL_Session* session, int previous_hostgroup,
		int selected_hostgroup, int conflicting_hostgroup);
	static ExtendedFrameTransferResult exercise_extended_frame_transfer(
		PgSQL_Session* session, int initial_hostgroup,
		int replacement_hostgroup);
	static ExtendedQpoHostgroupResult exercise_extended_qpo_hostgroups(
		PgSQL_Session* session, int previous_hostgroup,
		int first_destination, int later_destination);
	static bool implicit_prepare_retry_lifecycle(PgSQL_Session* session);
	static bool activation_transition_is_conservative(PgSQL_Session* session);
	static bool local_frame_requires_implicit_sync(PgSQL_Session* session);
	static ErrorPacketOwnershipResult exercise_error_packet_ownership(
		PgSQL_Session* session);
	static ForwardedErrorOwnershipResult exercise_forwarded_error_ownership(
		PgSQL_Session* session, int hostgroup);
	static ManualRouteResult exercise_manual_non_polardb_route(
		PgSQL_Session* session, int hostgroup, bool extended);
	static ExtendedFrameInvariantResult exercise_extended_frame_invariants(
		PgSQL_Session* session, int hostgroup);
	static PinnedFrameConflictResult exercise_pinned_frame_conflict(
		PgSQL_Session* session, int hostgroup);
	static bool automatic_reader_route_preserves_writer_lock(
		PgSQL_Session* session, int writer_hostgroup, int reader_hostgroup);
	static BoundPortalIdentityResult exercise_bound_portal_identity(
		PgSQL_Session* session);
	static PoisonedExtendedFrameResult exercise_poisoned_extended_frame(
		PgSQL_Session* session, int hostgroup);
	static bool locked_non_polardb_route_clears_stale_scope(
		PgSQL_Session* session, int hostgroup);
	static void set_worker_polardb_active(
		PgSQL_Thread* worker, bool active);
	static bool exercise_worker_polardb_publication(
		PgSQL_Thread* worker);
	static ExtendedFrameActivityTransitionResult
		exercise_extended_frame_activity_transition(
			PgSQL_Session* session, bool initially_active,
			bool active_at_execute);
	static ExtendedFrameRouteResult classify_bind_parse_execute(
		PgSQL_Session* session, int previous_hostgroup);
	static ExtendedFrameRouteResult classify_extended_frame(
		PgSQL_Session* session, int previous_hostgroup,
		unsigned int parse_count, unsigned int execute_count,
		bool has_backend_metadata,
		bool has_elided_portal_describe = false);
	static ExtendedBackendRouteResult apply_extended_backend_route(
		PgSQL_Session* session);
	static ExtendedFrameSyncResult exercise_extended_frame_sync(
		PgSQL_Session* session, int hostgroup);
	static DeferredRfqResult exercise_deferred_extended_rfq(
		PgSQL_Session* session, int writer_hostgroup, uint64_t writer_epoch);
	static DeferredScopeMismatchResult exercise_deferred_scope_mismatch(
		PgSQL_Session* session, PgSQL_Data_Stream* backend_myds,
		PgSQL_Connection* backend_conn, int writer_hostgroup,
		uint64_t current_epoch, uint64_t backend_lsn,
		uint64_t stale_target);
	static bool sticky_frame_outranks_delayed_multiplex(
		PgSQL_Session* session);
	static ExtendedFrameBatchPinResult exercise_extended_frame_batch_pinning(
		PgSQL_Session* session, int hostgroup);
	static ExtendedFrameErrorRecoveryResult
		exercise_extended_frame_error_recovery(PgSQL_Session* session);
	static ExtendedReadyOwnershipResult
		exercise_extended_ready_ownership(PgSQL_Session* session,
			int hostgroup);
	static int extended_frame_hostgroup(const PgSQL_Session* session);
	static int extended_frame_hostgroup_or_previous(
		const PgSQL_Session* session);
	static void apply_extended_hostgroup_lock(
		PgSQL_Session* session, int hostgroup);
	static void reset_extended_frame_state(PgSQL_Session* session);
};

struct PolarDB_WarmupUnitAccess {
	static std::string queued_key(
		const PgSQL_PolarDB_ReaderPool& pool);
	static bool drain(
		PgSQL_PolarDB_ReaderPool& pool,
		std::vector<PgSQL_SplitWarmupRequest>& requests);
	static bool register_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key);
	static bool rerun_pending(
		const PgSQL_PolarDB_ReaderPool& pool, const std::string& key);
	static void finish_inflight(
		PgSQL_PolarDB_ReaderPool& pool, const std::string& key,
		const PgSQL_SplitWarmupRequest& request);
	static bool apply_startup_parameters(
		PgSQL_Connection* conn,
		const PgSQL_SplitWarmupRequest& request);
};

struct PolarDB_OneReaderTestTopology {
	PgSQL_HGC* writer_hostgroup{nullptr};
	PgSQL_HGC* reader_hostgroup{nullptr};
	PgSQL_SrvC* writer{nullptr};
	PgSQL_SrvC* reader{nullptr};

	bool valid() const {
		return writer_hostgroup && reader_hostgroup && writer && reader;
	}
};

struct PolarDB_TwoReaderTestTopology {
	PgSQL_HGC* writer_hostgroup{nullptr};
	PgSQL_HGC* reader_hostgroup{nullptr};
	PgSQL_SrvC* writer{nullptr};
	PgSQL_SrvC* first_reader{nullptr};
	PgSQL_SrvC* second_reader{nullptr};

	bool valid() const {
		return writer_hostgroup && reader_hostgroup && writer &&
			first_reader && second_reader;
	}
};

// Captures one worker counter so a test can assert only its own delta.
class PolarDB_ThreadCounterSnapshot {
public:
	PolarDB_ThreadCounterSnapshot(
		const PgSQL_Thread* worker, unsigned int counter_index);

	unsigned long long delta() const;

private:
	const PgSQL_Thread* worker_;
	unsigned int counter_index_;
	unsigned long long initial_value_;
};

// Stage ordinary topologies and return borrowed HGM-owned objects. The pointers
// remain valid only until the next topology replacement or HGM cleanup.
PolarDB_OneReaderTestTopology stage_polardb_one_reader_test_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_addr, int reader_port);

PolarDB_TwoReaderTestTopology stage_polardb_two_reader_test_topology(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg,
	const char* first_reader_addr, int first_reader_port,
	const char* second_reader_addr, int second_reader_port,
	int first_reader_weight = 1, int second_reader_weight = 1);

void stage_polardb_topology_many_readers(
	PgSQL_HostGroups_Manager* hgm, const char* label,
	int writer_hg, const char* writer_addr, int writer_port,
	int reader_hg, const char* reader_prefix, int reader_count,
	int reader_base_port, int reader_weight = 1);

// Attach the standard test frontend objects to a session. Normal session
// reset/destruction releases the allocated stream and connection.
void attach_test_frontend(
	PgSQL_Session& session, PgSQL_Thread* worker = nullptr);

// Construct protocol identities, packets, and connections for component tests.
// make_cached_reader_connection() returns a caller-owned connection until it is
// inserted into a ReaderPool list.
PolarDB_StartupIdentity unit_proxy_identity();
PolarDB_StartupIdentity unit_other_proxy_identity();
PGconn* unit_connected_pgconn();
void unit_parse_rfq_lsn(PGconn* conn, uint64_t lsn,
	char transaction_status = 'I');
PtrSize_t unit_simple_query_packet(const char* query);

// ReaderPool helpers preserve the production matching index while tests add,
// inspect, or remove synthetic connections.
unsigned int unit_reader_pool_shared_free_count(PgSQL_SrvC* reader);
uint64_t unit_reader_pool_options_key(PgSQL_Connection* conn);
void unit_reader_pool_refresh_key(PgSQL_Connection* conn);
void unit_reader_pool_prepare_free_conn(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
PgSQL_PoolMatchKey unit_reader_pool_match_key(PgSQL_Connection* conn);
bool unit_register_reader_pool_capacity_request(
	PgSQL_SrvC* reader, unsigned int worker_index, uint64_t token,
	const PgSQL_PoolMatchKey& key);
void unit_reader_pool_add_matching(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
void unit_reader_pool_add_shared(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);
bool unit_reader_pool_clear_shared(
	PgSQL_SrvC* reader, PgSQL_Connection* conn);

// Focused accessors used to exercise private warmup and match-index recovery.
size_t pgsql_polardb_unit_collect_split_warmup_targets(
	PgSQL_PolarDB_ReaderPool* pool,
	const PgSQL_SplitWarmupRequest& request,
	std::vector<PgSQL_SplitWarmupRequest>& target_requests,
	bool* found_hostgroup,
	bool* saw_eligible_target,
	bool* saw_compatible_free);
void pgsql_polardb_unit_corrupt_match_key_positions(
	PgSQL_SrvConnList* connections);
void pgsql_polardb_unit_swap_match_key_positions(
	PgSQL_SrvConnList* connections, unsigned int first,
	unsigned int second);
size_t pgsql_polardb_unit_empty_match_bucket_count(
	PgSQL_SrvConnList* connections);

#endif // POLARDB_UNIT_SUPPORT_H
