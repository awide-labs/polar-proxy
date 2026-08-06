# 12 - Threadvars and Observability

> Scope: per-thread PolarDB configuration mirrors and every exported PolarDB stat counter, including this feature's RFQ routing counters and inactive knobs. | Audience: O/M | Status: stable | Prereqs: [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md), [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) | Verified against: this branch

## 1. Thread-Local Knobs

The admin variables live on `PgSQL_Threads_Handler::variables`. At runtime, `refresh_variables()` copies them into `pgsql_thread___*` mirrors so the hot path can read stable thread-local values.

| Admin variable | Runtime mirror | Behavior |
|---|---|---|
| `pgsql-polardb_profile` | committed profile bundle | Named coherent policy or `custom`; not read directly per query. |
| `pgsql-polardb_consistency_mode` | `pgsql_thread___polardb_consistency_mode` | `off`, `eventual`, `session_lsn`, or `global_lsn`; resolved after session and per-HG tiers. |
| `pgsql-polardb_read_target` | `pgsql_thread___polardb_read_target` | `primary` or `replica`; independent placement axis. |
| `pgsql-polardb_action_read_fallback` | `pgsql_thread___polardb_action_read_fallback` | `primary` or `error` when replica placement cannot be served. |
| `pgsql-polardb_action_lsn_timeout` | `pgsql_thread___polardb_action_lsn_timeout` | `warning`, `primary`, `error`, or `disconnect`; backend wire mode is derived from this complete action. |
| `pgsql-polardb_max_reader_lsn_gap_bytes` | `pgsql_thread___polardb_max_reader_lsn_gap_bytes` | Byte lag cap. `0` disables, positive values force writer when cached byte lag exceeds cap. |
| `pgsql-polardb_max_reader_lag_ms` | `pgsql_thread___polardb_max_reader_lag_ms` | Reserved. Runtime accepts only `0`; no millisecond-lag producer exists. |
| `pgsql-polardb_lsn_wait_timeout_ms` | `pgsql_thread___polardb_lsn_wait_timeout_ms` | Global default wait timeout. `1000` by default; `0` means wait indefinitely. |
| `pgsql-polardb_reader_lsn_max_age_ms` | `pgsql_thread___polardb_reader_lsn_max_age_ms` | Cached-LSN freshness window for lag checks. |
| `pgsql-polardb_monitor_lsn_updates` | `pgsql_thread___polardb_monitor_lsn_updates` | Enables/disables monitor-side LSN cache updates. |
| `pgsql-polardb_proxy_protocol` | `pgsql_thread___polardb_proxy_protocol` | `v15_wait`, `v15`, `legacy`, or `off`; per-HG `default` inherits it. |
| `pgsql-polardb_action_missing_lsn` | `pgsql_thread___polardb_action_missing_lsn` | `primary`, `warning`, or `error` when required LSN evidence is unavailable. |
| `pgsql-polardb_action_replica_loss` | `pgsql_thread___polardb_action_replica_loss` | Retry/fallback/terminal action after reader connection loss. |
| `pgsql-polardb_action_replica_error` | `pgsql_thread___polardb_action_replica_error` | `primary`, `error`, or `disconnect` for reusable reader errors. |
| `pgsql-polardb_proxy_identity_host` | `pgsql_thread___polardb_proxy_identity_host` | Fallback identity host for RFQ-requesting startup profiles; empty or non-wildcard IP literal. |
| `pgsql-polardb_proxy_identity_port` | `pgsql_thread___polardb_proxy_identity_port` | Fallback identity port for RFQ-requesting startup profiles; `0` means unset/staging. |

### Reserved millisecond lag cap

`pgsql-polardb_max_reader_lag_ms` is intentionally reserved and accepts only zero. Until a trustworthy time-lag producer exists, use `max_reader_lsn_gap_bytes`, `reader_lsn_max_age_ms`, and the backend wait timeout.

## 2. Counter Family

This implementation exports **299 stat counters** (252 thread-backed + 47 global-only) through `stats_pgsql_global`, plus one `PolarDB_Warmup_Pending` gauge and one internal `polardb_active` boolean condition. The condition is not exported as a stat counter and should not be summed with the counters.

The external names and values are stable, but the storage is split for hot-path
performance:

- 252 thread-backed counters use per-thread
  `PgSQL_Thread::polardb_status_variables.stvar[]` slots. This is a
  PolarDB-local mirror of ProxySQL's per-thread `stvar[]` pattern, but remains
  separate from the generic PgSQL `status_variables.stvar[]` path.
- The ordered `POLARDB_COUNTER_LIST(thread_cb, global_cb)` macro is the single
  source of truth for the external counter order, SQL names, Prometheus names,
  and help text. `POLARDB_THREAD_COUNTER_LIST` and
  `POLARDB_GLOBAL_COUNTER_LIST` are filtered views of that ordered list.
  Increment sites use
  `POLARDB_THREAD_COUNT(thread, name, value)` or
  `POLARDB_THREAD_COUNT_ONE(thread, name)`, so the per-thread slot
  `polardb_st_var_<name>` and global counter `PgHGM->status.polardb_<name>` are
  derived from the same token.
- The old `PgHGM->status.polardb_*` atomics remain as global counters for rare
  calls without a worker thread. `PgSQL_Threads_Handler` exports the value as
  `global counter + sum(live per-thread slots)`.
- `PgSQL_Thread::~PgSQL_Thread()` folds a worker's 252 thread-backed counter slots
  into the matching global counters before the worker object is freed. Runtime
  PgSQL thread resize is not supported today, so this mainly preserves
  monotonic shutdown-time scrapes and supports future use any later worker lifecycle
  changes.
- 47 global-only counters remain `PgHGM->status` atomics because they are monitor-side,
  configuration/failover-side, or rare failure-path events. The seven below are a
  representative subset; the full set is the `G()`-tagged entries in
  `POLARDB_COUNTER_LIST` (for example the `wait_retry_declined_*` and
  `split_warmup_*` families):
  `PolarDB_LSN_Updates_From_Monitor`,
  `PolarDB_Monitor_Health_Invalid_Role`,
  `PolarDB_Monitor_Health_Invalid_Values`,
  `PolarDB_Wait_Reads_Retried_On_Writer`,
  `PolarDB_RFQ_Profile_Evicted`,
  `PolarDB_Session_Target_Epoch_Reset`, and
  `PolarDB_Wait_Wrap_Safety_Abort`.
- The shared counter metadata macros are not storage-layout definitions.
  The counter lists drive generated enum entries, counter-array entries,
  SQL export, and Prometheus metrics. The
  actual `PgHGM->status.polardb_*` atomics stay explicit in
  `PgSQL_HostGroups_Manager.h` so comments, grouping, and cache-line decisions
  remain visible next to the storage.

This is intentionally a dedicated PolarDB aggregation path. The generic PgSQL
thread-variable / Prometheus export loop is commented out in this branch, so
PolarDB uses `PgSQL_Threads_Handler::get_polardb_counter()` rather than reviving
that generic path.

The same counters are exposed in two places:

- SQL: `stats_pgsql_global` rows named `PolarDB_*`.
- Prometheus: `proxysql_polardb_*_total` counters registered through the active
  PgSQL HGM Prometheus path. The wait-latency sum is exported as
  `proxysql_polardb_wait_lsn_microseconds_total` so it exactly matches the SQL
  microsecond counter.

Prometheus collection is scrape/TSDB-time work, not query-path work. Each
collection aggregates 252 thread-backed counters across PgSQL worker threads and
reads 47 global-only atomics. Disabling the web endpoint or TSDB sampling
disables that runtime collection cost; there is no PolarDB-specific Prometheus
toggle.

The table below enumerates the core LSN and consistency counters as a curated
starting point. It is one family out of the complete always-on surface. Section 3
is the comprehensive, family-organized catalog of all **299 always-on counters**;
the authoritative export order is defined by `POLARDB_COUNTER_LIST` in
`PgSQL_PolarDB_Counters.h`. Section 4 documents the additional diagnostic counter
families that only exist in `POLARDB_PROFILE=1` / `POLARDB_PERF_DEBUG=1` builds and
are absent from production builds.

| # | Exported name | Field | What it counts | Operator reading |
|---:|---|---|---|---|
| 1 | `PolarDB_Server_LSN_Updates_From_RFQ` | `polardb_server_lsn_updates_from_rfq` | Query RFQ carried an LSN and the direct HGM update condition accepted it for the current writer group+epoch. Counts reads and writes, including accepted RFQs that did not strictly advance the cached LSN. | Should grow under PolarDB traffic. Flat zero under traffic points at missing RFQ-LSN support, disabled LSN parsing, or stale/missing writer group/epoch rejection. |
| 2 | `PolarDB_LSN_Updates_From_Monitor` | `polardb_lsn_updates_from_monitor` | Monitor observed an LSN advance. | Confirms monitor-side LSN collection is active. |
| 3 | `PolarDB_Monitor_Health_Invalid_Role` | `polardb_monitor_health_invalid_role` | Monitor health row reported a role ProxySQL cannot route as primary or reader. | Backend role output is not usable for routing; the row is sanitized before it can affect reader state. |
| 4 | `PolarDB_Monitor_Health_Invalid_Values` | `polardb_monitor_health_invalid_values` | Monitor health row had invalid availability text or invalid LSN text. | Backend health values are not trustworthy. Invalid availability is not allowed to shun a server, and invalid LSN text is not fed into the cache. |
| 5 | `PolarDB_LSN_Stale_Count` | `polardb_lsn_stale_count` | Reader candidates skipped during selection because their cached LSN was below the session target. | Normally low; a rising value means readers are lagging behind the session's consistency target. Byte-lag rejections are counted separately by the `PolarDB_Lag_Cap_*` counters. |
| 6 | `PolarDB_Write_Missing_LSN` | `polardb_write_missing_lsn` | Writer query completed without RFQ LSN. | Deployment problem. Automatic LSN-mode reads in that session follow `action_missing_lsn` until a primary-sourced RFQ clears the flag. |
| 7 | `PolarDB_Read_Missing_LSN` | `polardb_read_missing_lsn` | Tracked SESSION_LSN read completed without RFQ LSN. | Deployment/profile problem or backend mismatch; later reads follow `action_missing_lsn` until a primary RFQ clears the flag. |
| 8 | `PolarDB_Group_LSN_Unknown` | `polardb_group_lsn_unknown` | GLOBAL_LSN required a current group observation but none was available. | Global consistency failed closed through the configured missing-LSN action. |
| 9 | `PolarDB_RFQ_Best_Effort_Degraded_Routes` | `polardb_rfq_warning_degraded_routes` | `action_missing_lsn=warning` allowed an eligible simple-query reader route without an RFQ-derived wait target. | Degraded consistency path; expected only if deliberately configured. Clients also receive a WARNING `NoticeResponse` before the result; the proxy log is edge-limited per session. |
| 10 | `PolarDB_Consistency_Writer_Fallback` | `polardb_consistency_writer_fallback` | Reader acquisition returned a consistency-safety status or strict RFQ-unavailable status, and this read was redirected to the writer. | Offload loss from consistency-safe writer fallback. Compare with reader status traces and lag/RFQ counters to identify the root cause. |
| 11 | `PolarDB_Wait_Reads_Retried_On_Writer` | `polardb_wait_reads_retried_on_writer` | A wait-wrapped reader query was retried once on the writer after strict wait timeout or reader connection loss, before any user result reached the client. | Reader-side failure was safely recovered on the writer. Check timeout and connection-loss counters for the reason. |
| 12 | `PolarDB_RFQ_Profile_Skipped` | `polardb_rfq_profile_skipped` | Pool acquisition skipped a pooled connection whose startup profile did not request RFQ LSN. | Old/off-profile pooled connections exist during RFQ-required reads. |
| 13 | `PolarDB_RFQ_Profile_Evicted` | `polardb_rfq_profile_evicted` | Pool acquisition evicted incompatible free pooled connections to create one RFQ-LSN-capable replacement. | Old/off-profile free pool capacity is being replaced under RFQ demand; one acquisition can evict more than one connection if the free pool is already over cap. |
| 14 | `PolarDB_Reader_Pool_Miss_Empty` | `polardb_reader_pool_miss_empty` | Reader pool lookup found no usable pooled reader for the acquisition. | Targeted reads are reaching new-backend creation or shared selection because no pooled reader already satisfied RFQ profile, state, freshness, and target-LSN checks. |
| 15 | `PolarDB_Target_LSN_Preferred` | `polardb_target_lsn_preferred` | Reader acquisition acquired a fresh cached reader already at or beyond the target. | Consistency-target preference is avoiding waits where possible. |
| 16 | `PolarDB_Target_LSN_Fallback_Wait` | `polardb_target_lsn_fallback_wait` | No preferred target-reaching connection was acquired; acquisition fell back to the full candidate set and relies on the wait wrapper. | Normal under lag or sparse pools; correctness still comes from the backend wait. |
| 17 | `PolarDB_Session_Target_Epoch_Reset` | `polardb_session_target_epoch_reset` | Collect or accepted result processing saw a writer group/epoch scope change and discarded at least one old write/observed LSN target or missing-LSN flag. | Should move only around writer failover/topology movement or cross-group session movement. A rising value means old-scope session state was invalidated safely. |
| 18 | `PolarDB_Session_LSN_Routing` | `polardb_session_lsn_routing` | Planner chose a reader route with an LSN wait. | RYW read offload is happening. |
| 19 | `PolarDB_Wait_Wrap_Prepared` | `polardb_wait_wrap_prepared` | Wrapper preparation for a reader-with-wait decision. | Normally moves with `PolarDB_Session_LSN_Routing`. |
| 20 | `PolarDB_Wait_Wrap_Bypassed` | `polardb_wait_wrap_bypassed` | Backend acquisition selected a reader whose fresh cached LSN already reached the consistency target, so the staged wait wrapper was cleared. | Normal healthy fast path. Compare with `PolarDB_Target_LSN_Preferred`; a high value means many protected reads avoided the backend wait. |
| 21 | `PolarDB_Wait_Wrap_Safety_Abort` | `polardb_wait_wrap_safety_abort` | Wrapper construction failed before backend dispatch. | Should be 0. Non-zero means the wrapper safety path stopped an unprotected replica read. |
| 22 | `PolarDB_Wait_LSN_Sent` | `polardb_wait_lsn_sent` | LSN wait wrapper was successfully installed/sent. | Compare with prepared and bypassed counts; gaps beyond bypasses mean wrapper safety aborts. |
| 23 | `PolarDB_Wait_LSN_Sum_Us` | `polardb_wait_lsn_sum_us` | Total microseconds spent in LSN waits. | Divide by sent count for average wait latency. Bypassed reads do not add wait time. |
| 24 | `PolarDB_Wait_Error_Timeout` | `polardb_wait_error_timeout` | Total wait timeout events accounted once per wait. | Replicas are not catching up within configured timeout. |
| 25 | `PolarDB_Wait_Error_LSN_Wait_Timeout` | `polardb_wait_error_lsn_wait_timeout` | LSN-wait subset of timeout events. | Equals total in this LSN-only implementation; can diverge when future wait families exist. |
| 26 | `PolarDB_Wait_Error_Connection_Lost` | `polardb_wait_error_connection_lost` | Wait-wrapped reader lost its backend connection before any user result reached the client. | Reader/backend instability during protected reads; matching writer-retry counter means the read was recovered on the writer. |

## 3. Complete Counter Catalog by Family

Section 2 covers the core LSN/consistency counters. This section catalogs the
remaining always-on families so the whole 299-counter surface is documented. Every
name below is byte-exact to `POLARDB_COUNTER_LIST` in `PgSQL_PolarDB_Counters.h`.
`T`-tagged names are thread-backed; `G`-tagged names are global-only atomics. Where
a family is large its full member list is given so no name is omitted silently.

### 3.1 Consistency-wait latency: `PolarDB_Wait_LSN_Elapsed_*`

Eight histogram buckets (plus the `PolarDB_Wait_LSN_Sum_Us` running total from
Section 2) that record the elapsed time of every wait-wrapped LSN read. All eight
are incremented by `polardb_count_lsn_wait_elapsed_bucket()`
(`lib/PgSQL_Thread.cpp:77`), which selects exactly one bucket per completed wait.
SLO-relevant: the tail buckets show how often protected reads stall.

| Exported name | Bucket |
|---|---|
| `PolarDB_Wait_LSN_Elapsed_Le_1ms` | elapsed ≤ 1ms |
| `PolarDB_Wait_LSN_Elapsed_Le_5ms` | elapsed ≤ 5ms |
| `PolarDB_Wait_LSN_Elapsed_Le_10ms` | elapsed ≤ 10ms |
| `PolarDB_Wait_LSN_Elapsed_Le_50ms` | elapsed ≤ 50ms |
| `PolarDB_Wait_LSN_Elapsed_Le_100ms` | elapsed ≤ 100ms |
| `PolarDB_Wait_LSN_Elapsed_Le_500ms` | elapsed ≤ 500ms |
| `PolarDB_Wait_LSN_Elapsed_Le_1s` | elapsed ≤ 1s |
| `PolarDB_Wait_LSN_Elapsed_Gt_1s` | elapsed > 1s |

The transaction-split path has a parallel eight-bucket histogram
`PolarDB_Split_LSN_Wait_Elapsed_*` (same thresholds), driven by the same helper;
it is listed with the split family in §3.10.

### 3.2 Client-visible RFQ LSN raise: `PolarDB_Client_RFQ_LSN_Raised_*`

Three counters for cases where the proxy raised the LSN payload in the
`ReadyForQuery` sent to the client above the raw backend RFQ value. These are the
catalog rows for the client-visible raise behavior already described in
[09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) §6 and §9.

| Exported name | What it counts |
|---|---|
| `PolarDB_Client_RFQ_LSN_Raised_To_Target` | Client RFQ LSN raised from the backend value to a confirmed session or wait target. |
| `PolarDB_Client_RFQ_LSN_Raised_By_Writer` | Client RFQ LSN raised because the response came from the current writer hostgroup. |
| `PolarDB_Client_RFQ_LSN_Raised_By_Wait` | Client RFQ LSN raised because the query completed a successful LSN wait. |

### 3.3 Route planner: `PolarDB_Route_*`

The automatic route planner's decision breakdown. `Route_Planner_Total` is the
denominator; `Manual_*` counts routes the planner left to normal query-rule or
sticky-hostgroup selection.

| Exported name | What it counts |
|---|---|
| `PolarDB_Route_Planner_Total` | Queries examined by the automatic route planner. |
| `PolarDB_Route_Replica_Eligible` | Planner inputs whose query rule marked the request replica eligible. |
| `PolarDB_Route_Replica_Ineligible` | Planner inputs not marked replica eligible (writes, control statements). |
| `PolarDB_Route_To_Reader` | Replica-eligible decisions targeting a reader hostgroup. |
| `PolarDB_Route_To_Writer` | Replica-eligible decisions targeting the writer hostgroup. |
| `PolarDB_Route_Passthrough_Rule_Owned` | Replica-eligible decisions left to normal query-rule routing. |
| `PolarDB_Route_No_Wait_Target` | Replica-eligible reader decisions needing no session-LSN wait target. |
| `PolarDB_Route_Wait_Required` | Replica-eligible reader decisions that required a backend LSN wait. |
| `PolarDB_Route_Txn_Split_Planned` | Replica-eligible in-transaction reads planned for transaction split. |
| `PolarDB_Route_Txn_Wait_Planned` | Pre-write in-transaction reads planned for a temporary reader wait path. |
| `PolarDB_Route_Manual_Total` | Queries where a query rule or sticky hostgroup selected the route. |
| `PolarDB_Route_Manual_To_Reader` | Manual-route queries whose effective hostgroup was a PolarDB reader. |
| `PolarDB_Route_Manual_To_Writer` | Manual-route queries whose effective hostgroup was a PolarDB writer. |
| `PolarDB_Route_Manual_Other` | Manual-route queries whose hostgroup was not a known PolarDB reader/writer. |
| `PolarDB_Route_Manual_Forced_Writer` | Manual-route queries overridden to the writer by reader-failure safety handling. |
| `PolarDB_Route_Locked_Hostgroup` | Queries where a session hostgroup lock skipped automatic routing. |

The related routing counters `PolarDB_Session_LSN_Routing` (§2) and
`PolarDB_Global_LSN_Routing` record reads routed to a reader with a session-LSN or
global-LSN wait requirement respectively.

### 3.4 Pooled-reader reuse rejects: `PolarDB_Reader_Pool_Reject_*`

Five counters naming why a pooled reader connection could not be reused for an
acquisition. They partition the reject reasons checked during pool reuse.

| Exported name | Reject reason |
|---|---|
| `PolarDB_Reader_Pool_Reject_Bad_Context` | Required session or connection context was missing. |
| `PolarDB_Reader_Pool_Reject_Profile` | Startup profile incompatible with the hostgroup profile. |
| `PolarDB_Reader_Pool_Reject_Auth` | User or database differed. |
| `PolarDB_Reader_Pool_Reject_Identity` | PolarDB startup identity differed. |
| `PolarDB_Reader_Pool_Reject_Session_State` | Session state did not match. |

### 3.5 Two-reader sample counters: `PolarDB_Reader_Pool_P2C_*`

These names are retained for compatibility. They count cases where the selector
compares two reader candidates. Two-reader hostgroups use deterministic weighted
alternation; eligible equal-weight ordinary reads with three or more readers
sample two directly; special requests use the complete candidate path.

| Exported name | What it counts |
|---|---|
| `PolarDB_Reader_Pool_P2C_Select` | Reader-pool selections that compared two candidates. |
| `PolarDB_Reader_Pool_P2C_Second` | The second sampled reader was selected. |
| `PolarDB_Reader_Pool_P2C_Decide_Active_Load` | Lower globally visible active load selected the reader. |
| `PolarDB_Reader_Pool_P2C_Decide_Random` | Global load/free counts tied; random tie-break selected the reader. |

The broader reader-pool mechanics families (`PolarDB_Reader_Pool_Hit`,
`_Miss_Empty`, `_Lookup`, `_Server_Considered`, `_Match_Attempt`, `_Conn_Examined`,
`_Drop_Offline`, `_Drop_Ineligible`, `_Return_To_Core`, and the lag-cap
`PolarDB_Lag_Cap_*` and target-gap `PolarDB_Reader_Target_Gap_*` histograms) are
all always-on `T` counters in `POLARDB_COUNTER_LIST`; see the header for the full
per-counter help text.

### 3.6 Query parser: `PolarDB_Query_Parser_*`

Nine counters covering the PgSQL query parser / digest initializer and its
end-of-query statistic update.

| Exported name | What it counts |
|---|---|
| `PolarDB_Query_Parser_Init` | Queries submitted to the parser/digest initializer. |
| `PolarDB_Query_Parser_Init_Bytes` | Query bytes submitted to the initializer. |
| `PolarDB_Query_Parser_Init_Digest_Enabled` | Initializations run while query-digest collection was enabled. |
| `PolarDB_Query_Parser_Init_Commands_Enabled` | Initializations run while command statistics were enabled. |
| `PolarDB_Query_Parser_Command_Type` | Command-type classifications requested from the parser. |
| `PolarDB_Query_Parser_Update` | Parser statistic updates attempted at query end. |
| `PolarDB_Query_Parser_Update_Skipped_None` | Updates skipped because the query had no parser state. |
| `PolarDB_Query_Parser_Update_Skipped_Uninitialized` | Updates skipped because the parser command was still uninitialized. |
| `PolarDB_Query_Parser_Update_With_Digest` | Updates that carried a digest text. |

### 3.7 Coalesced backend byte accounting: `PolarDB_Parent_Bytes_Flush_*`

Nine counters describing how per-connection recv/sent byte counts are coalesced and
flushed to the shared parent (server) atomics, so the hot path avoids per-packet
atomic updates.

| Exported name | What it counts |
|---|---|
| `PolarDB_Parent_Bytes_Flush_Threshold_Recv` | Flushes triggered by the backend recv-byte threshold. |
| `PolarDB_Parent_Bytes_Flush_Threshold_Sent` | Flushes triggered by the backend sent-byte threshold. |
| `PolarDB_Parent_Bytes_Flush_Detach` | Flushes made before detaching a backend connection. |
| `PolarDB_Parent_Bytes_Flush_Destructor` | Flushes made while destroying a backend connection. |
| `PolarDB_Parent_Bytes_Flush_No_Parent` | Pending bytes dropped because no server container was attached. |
| `PolarDB_Parent_Bytes_Flush_Recv_Atomic` | Shared parent recv-byte atomic updates after coalescing. |
| `PolarDB_Parent_Bytes_Flush_Sent_Atomic` | Shared parent sent-byte atomic updates after coalescing. |
| `PolarDB_Parent_Bytes_Flush_Recv_Bytes` | Recv bytes flushed to shared parent counters. |
| `PolarDB_Parent_Bytes_Flush_Sent_Bytes` | Sent bytes flushed to shared parent counters. |

### 3.8 Result processing and row-run fast-forward

Result-processing observations (`PolarDB_Result_Process*`, 3 counters) and the
DataRow row-run fast-forward path (`PolarDB_Result_Row_Run_*`, 7 counters).

| Exported name | What it counts |
|---|---|
| `PolarDB_Result_Process` | Result-processing observations run at query completion. |
| `PolarDB_Result_Process_Write_Classify` | Result-processing calls that classified the query as read or write. |
| `PolarDB_Result_Process_Write_Classify_Text` | Classifications that had query text available. |
| `PolarDB_Result_Row_Run_Attempts` | Attempts to detach a pending backend DataRow run. |
| `PolarDB_Result_Row_Run_Used` | Backend DataRow runs forwarded as one result packet. |
| `PolarDB_Result_Row_Run_Frames` | DataRow frames forwarded through row-run fast-forward. |
| `PolarDB_Result_Row_Run_Bytes` | Bytes forwarded through row-run fast-forward. |
| `PolarDB_Result_Row_Run_Unavailable` | Row-run probes that fell back to normal result handling. |
| `PolarDB_Result_Row_Run_Partial` | Row-run probes that saw an incomplete DataRow frame. |
| `PolarDB_Result_Row_Run_Not_Candidate` | Row-run checks skipped because libpq was not at a DataRow frame. |

Note: `PolarDB_Reader_Acquire_Count` / `PolarDB_Reader_Acquire_Sum_Us` are **not**
always-on — they are compiled in only under `POLARDB_PROFILE=1` (see §4).

### 3.9 Data-plane writes: `PolarDB_WriteV_*` and `PolarDB_Output_Coalesce_*`

The plaintext direct scatter/gather (`writev`) frontend send path and the
streaming-output coalesce path.

`PolarDB_WriteV_*` — eight always-on counters:

| Exported name | What it counts |
|---|---|
| `PolarDB_WriteV_Attempts` | Plaintext frontend direct scatter/gather send attempts. |
| `PolarDB_WriteV_Bytes` | Bytes sent through the direct scatter/gather path. |
| `PolarDB_WriteV_Packets` | Fully-sent packets consumed by the direct path. |
| `PolarDB_WriteV_Short_Writes` | Sends that wrote less than the built view. |
| `PolarDB_WriteV_WouldBlock` | Sends returning EAGAIN, EWOULDBLOCK, or EINTR. |
| `PolarDB_WriteV_Errors` | Sends returning a hard error. |
| `PolarDB_WriteV_Buffered_Fallback` | Frontend writes forced back to queueOUT buffering. |
| `PolarDB_WriteV_Small_Batch_Fallback` | Frontend writes kept on the buffered path because they fit one queue buffer. |

`PolarDB_Output_Coalesce_*` — five counters:

| Exported name | What it counts |
|---|---|
| `PolarDB_Output_Coalesce_Hold` | Incomplete streaming frontend output flushes deferred. |
| `PolarDB_Output_Coalesce_Flush_Budget` | Incomplete streaming output flushed after the coalesce budget. |
| `PolarDB_Output_Coalesce_Flush_Complete` | Held streaming output flushed at result completion. |
| `PolarDB_Output_Coalesce_Flush_Backpressure` | Coalesce skipped because output/socket already had pending bytes. |
| `PolarDB_Output_Coalesce_Disabled` | **(inert — defined, not emitted)** The counter exists (`PgSQL_PolarDB_Counters.h:649`) but no code path increments it in this branch. |

### 3.10 Transaction split and XID family (`PolarDB_Split_*`, `PolarDB_Txn_*`, `PolarDB_XIDs_Received`)

This is the largest family: about **85 always-on counters** covering the
transaction-split machinery. They are always compiled and always exported, but they
only advance for hostgroup pairs where transaction split is enabled. Transaction
split is enabled **per hostgroup pair by `txn_split_enabled` (default `0`)**; with the
default configuration these counters stay at zero. See
[06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) for the split routing decision and
[04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md) for the schema knob.

The members are grouped by sub-family below. The complete ordered list with help
text is in `POLARDB_COUNTER_LIST` (`PgSQL_PolarDB_Counters.h`, roughly lines
336–1020).

**Transaction lifecycle** (`PolarDB_Txn_*`, `PolarDB_Queries_*`, `PolarDB_XIDs_Received`):

| Exported name | What it counts |
|---|---|
| `PolarDB_Queries_In_Splittable_Txn` | Queries planned while the transaction had split-readable primary RFQ evidence. |
| `PolarDB_Queries_Split_Eligible` | In-transaction reads that passed the split planner checks. |
| `PolarDB_XIDs_Received` | Primary RFQs that reported transaction XIDs. |
| `PolarDB_Txn_Became_Splittable` | Transactions that became eligible for split reads. |
| `PolarDB_Txn_Lost_Splittable` | Transactions that lost split-readable state before commit. |
| `PolarDB_Txn_Committed_With_Split` | Transactions that committed after at least one split read. |
| `PolarDB_Txn_Committed_No_Split` | Split-readable transactions that committed without a split read. |
| `PolarDB_Txn_Wait_Reader_Reconciled` | Temporary transaction-wait reader ownership restored at request entry. |

**Split read outcomes** (`PolarDB_Split_Reads_*`, plus terminations):

| Exported name | What it counts |
|---|---|
| `PolarDB_Split_Reads_Total` | Transaction-split read attempts. |
| `PolarDB_Split_Reads_Success` | Split reads completed on a replica. |
| `PolarDB_Split_Reads_Fallback` | Split reads never dispatched to a replica, run on the primary. |
| `PolarDB_Split_Reads_Retried` | Split reader failures redispatched on the writer. |
| `PolarDB_Split_Reads_Retried_On_Reader` | Split reader failures redispatched on another replica. |
| `PolarDB_Split_Reads_Forwarded` | Split reader failures forwarded to the client, transaction kept on the writer. |
| `PolarDB_Split_Reads_Error` | Split reads that ended in an error path. |
| `PolarDB_Reader_Terminations` | Replica-reader failures that closed the client session. |

**Split candidate rejects** (`PolarDB_Split_Rejected_*`, plus WAL-pending and invariant checks):

| Exported name | Reject / check reason |
|---|---|
| `PolarDB_Split_Rejected_Multistatement` | Query has multiple statements. |
| `PolarDB_Split_Rejected_Not_Select` | Statement shape is not a split-safe SELECT. |
| `PolarDB_Split_Rejected_For_Update` | SELECT takes write locks. |
| `PolarDB_Split_Rejected_Write_LSN_Unknown` | A prior write RFQ had no LSN. |
| `PolarDB_Split_Rejected_Observed_LSN_Unknown` | A prior tracked read RFQ had no LSN. |
| `PolarDB_Split_WAL_Pending` | Primary RFQ reported WAL pending. |
| `PolarDB_Split_Invariant_Violations` | Unexpected split state-machine violations (should stay 0). |
| `PolarDB_Split_Blocked_Reads` | Reads rejected because the transaction was blocked after a split fault. |
| `PolarDB_Split_No_Backend` | Split reads that could not get a replica backend. |
| `PolarDB_Split_Send_Failed` | Split reads whose wrapped query could not be sent. |

**Split reader-acquisition fallbacks** (`PolarDB_Split_Fallback_*`):

| Exported name | Fallback cause |
|---|---|
| `PolarDB_Split_Fallback_Reader_Unavailable` | No reader online or usable. |
| `PolarDB_Split_Fallback_Reader_Busy` | Readers at capacity or no pooled match. |
| `PolarDB_Split_Fallback_RFQ_Unavailable` | No RFQ-LSN-capable reader backend available. |
| `PolarDB_Split_Fallback_Group_LSN_Unknown` | Lag-cap policy had no group LSN sample. |
| `PolarDB_Split_Fallback_Reader_LSN_Unknown` | Lag-cap policy had no reader LSN sample. |
| `PolarDB_Split_Fallback_Reader_LSN_Stale` | Reader LSN sample was stale. |
| `PolarDB_Split_Fallback_Reader_Lag_Exceeded` | Byte lag exceeded `max_lag_bytes`. |

**Split pool reuse** (`PolarDB_Split_Pool_*`, `PolarDB_Split_Conn_Reused`):

| Exported name | What it counts |
|---|---|
| `PolarDB_Split_Pool_Hit` | Split reads that acquired an existing pooled replica connection. |
| `PolarDB_Split_Pool_Empty` | Split reads that found no pooled replica connection. |
| `PolarDB_Split_Pool_Contention` | Split reads that could not use a pooled connection because none matched. |
| `PolarDB_Split_Conn_Reused` | Split reads that reused an already attached split backend connection. |

**Split connection cleanup** (`PolarDB_Split_Conn_Cleanup_*`, 12 counters):

| Exported name | What it counts |
|---|---|
| `PolarDB_Split_Conn_Cleanup_Success` | Replica connections returned cleanly to the pool. |
| `PolarDB_Split_Conn_Cleanup_Failed` | Replica connections destroyed instead of returned. |
| `PolarDB_Split_Conn_Cleanup_No_Reuse_Requested` | Destroyed because the caller requested no reuse. |
| `PolarDB_Split_Conn_Cleanup_Not_Reusable` | Destroyed because already marked non-reusable. |
| `PolarDB_Split_Conn_Cleanup_Not_Idle` | Destroyed because async state was not idle. |
| `PolarDB_Split_Conn_Cleanup_Active_Txn` | Destroyed because a transaction was still active. |
| `PolarDB_Split_Conn_Cleanup_Recovery_Attempt` | Cleanups that tried to recover a non-idle backend. |
| `PolarDB_Split_Conn_Cleanup_Recovery_Terminal` | Recovery where the backend was already at a completed-query terminal state. |
| `PolarDB_Split_Conn_Cleanup_Recovery_Timeout_State` | Recovery rejected because the backend was in a timeout state. |
| `PolarDB_Split_Conn_Cleanup_Recovery_Busy_State` | Recovery rejected because the backend was still in-flight. |
| `PolarDB_Split_Conn_Cleanup_Normalized` | Completed error result cleared during cleanup. |
| `PolarDB_Split_Conn_Cleanup_Recovered` | Previously non-idle connections returned to the pool after cleanup. |

**Split LSN wait latency** (`PolarDB_Split_LSN_Wait_*`, 10 counters): the count
`PolarDB_Split_LSN_Wait_Count`, running total `PolarDB_Split_LSN_Wait_Sum_Us`, and
the eight-bucket histogram `PolarDB_Split_LSN_Wait_Elapsed_{Le_1ms, Le_5ms, Le_10ms,
Le_50ms, Le_100ms, Le_500ms, Le_1s, Gt_1s}` (same thresholds and helper as §3.1).

**Split read errors and latency** (`PolarDB_Split_Error_*`, `PolarDB_Split_Latency_*`):

| Exported name | What it counts |
|---|---|
| `PolarDB_Split_Error_Connection_Lost` | Split reads whose replica connection was lost. |
| `PolarDB_Split_Error_Query_Failed` | Split reads whose user query failed on the replica. |
| `PolarDB_Split_Error_Timeout` | Split wait-timeout events accounted. |
| `PolarDB_Split_Error_LSN_Wait_Timeout` | Split LSN wait-timeout events accounted. |
| `PolarDB_Split_Latency_Sum_Us` | Total transaction-split read latency, in microseconds. |
| `PolarDB_Split_Latency_Count` | Transaction-split read latency samples. |

**Lazy split-pool warmup** (`PolarDB_Split_Warmup_*`, 16 counters — all `G`
global-only): request/dedup/queue accounting plus connect/add outcomes and the
`PolarDB_Split_Warmup_Sum_Us` / `PolarDB_Split_Warmup_Count` latency pair.

| Exported name | What it counts |
|---|---|
| `PolarDB_Split_Warmup_Requested` | Warmup requests queued after a pool-empty split attempt. |
| `PolarDB_Split_Warmup_Target_Attempts` | Backend connect attempts produced by warmup requests. |
| `PolarDB_Split_Warmup_Created` | Warmup connections added to replica pools. |
| `PolarDB_Split_Warmup_Failed` | Warmup base requests rejected or completed without a target. |
| `PolarDB_Split_Warmup_Target_Failed` | Warmup target backends that failed before publication. |
| `PolarDB_Split_Warmup_Already_Warm` | Warmup skipped because a compatible free backend already existed. |
| `PolarDB_Split_Warmup_Dedup_Queued` | Warmup requests deduplicated against queued work. |
| `PolarDB_Split_Warmup_Dedup_Inflight` | Warmup requests deduplicated against in-flight work. |
| `PolarDB_Split_Warmup_Queue_Full` | Warmup requests dropped because the queue was full. |
| `PolarDB_Split_Warmup_No_Target` | Warmup drains that could not find an eligible target reader. |
| `PolarDB_Split_Warmup_Bad_Request` | Warmup requests rejected before queueing (missing session identity). |
| `PolarDB_Split_Warmup_RFQ_Unavailable` | Warmup skipped because the reader hostgroup does not request RFQ LSN. |
| `PolarDB_Split_Warmup_Connect_Failed` | Warmup backends whose connection handshake failed. |
| `PolarDB_Split_Warmup_Add_Failed` | Warmup backends discarded after connecting (target changed or at capacity). |
| `PolarDB_Split_Warmup_Sum_Us` | Total time from warmup request to pooled connection, in microseconds. |
| `PolarDB_Split_Warmup_Count` | Lazy split warmup latency samples. |

The related planner counters `PolarDB_Route_Txn_Split_Planned` and
`PolarDB_Route_Txn_Wait_Planned` live in the route family (§3.3); the current
`PolarDB_Warmup_Pending` gauge (queue depth) is documented in §2 as the one
non-counter gauge.

## 4. Profile / perf-debug build tiers (not in production builds)

The families in §2–§3 are always compiled. Two build flags add **extra diagnostic
counter families that are absent from default and production builds**. Both default
OFF (`lib/Makefile:63-68`):

- `POLARDB_PROFILE=1` — compiles in `POLARDB_PROFILE_THREAD_COUNTER_LIST` (36 `T`
  counters) and `POLARDB_PROFILE_GLOBAL_COUNTER_LIST` (23 `G` counters), 59 total.
- `POLARDB_PERF_DEBUG=1` — compiles in `POLARDB_PERF_DEBUG_THREAD_COUNTER_LIST` (47
  `T` counters).

These change the exported counter count and are for latency/throughput profiling
only. Do not assume they exist when reading a production build's
`stats_pgsql_global`; the always-on surface is 299 (§2). All names below are
byte-exact to `PgSQL_PolarDB_Counters.h`.

### 4.1 `POLARDB_PROFILE=1` diagnostic families

Latency-sample pairs (`_Count` + `_Sum_Us`) for hot stages, plus pool-lock and
idle-ping timing:

- `PolarDB_Wait_Wrap_Build_{Count, Sum_Us}` — wait-wrapper SQL build time.
- `PolarDB_Wait_Wrap_Install_{Count, Sum_Us}` — wait-wrapper packet install time.
- `PolarDB_Reader_Acquire_{Count, Sum_Us}` — RFQ-aware reader acquisition time
  (this is why `Reader_Acquire_*` is **not** in the always-on catalog, §3.8).
- `PolarDB_Split_Prepare_{Count, Sum_Us}`, `PolarDB_Split_Reader_Acquire_{Count,
  Sum_Us}`, `PolarDB_Split_Wrapper_Build_{Count, Sum_Us}` — transaction-split stage timing.
- `PolarDB_Selected_Server_Pool_Lock_Wait_{Count, Sum_Us}` and
  `PolarDB_Selected_Server_Pool_Lock_Hold_{Count, Sum_Us}` — contention and hold
  time on the selected-server pool lock.
- `PolarDB_Idle_Ping_Pool_Maintenance_{Count, Sum_Us}` (`G`) — time finding and
  preparing idle connections for ping.

It also adds a detailed reader-target diagnostic breakdown
(`PolarDB_Reader_Target_Ready_Candidate`, `_No_Ready_Candidate`, `_LSN_Unknown`,
`_LSN_Stale`, `_LSN_Behind`, `_Lag_Cap_Reject`, `_RFQ_Unavailable`,
`_RFQ_No_Protocol`, `_RFQ_No_Client_Context`, and the
`PolarDB_Reader_Target_RFQ_Candidate_{Profile,Identity,Auth}_Mismatch` /
`PolarDB_Reader_Target_RFQ_Unavailable_{Profile,Identity,Auth}_Mismatch` reason
counters), the RFQ-payload observability counters
(`PolarDB_RFQ_Requested_Missing_Payload`, `_Zero_Payload`,
`PolarDB_Client_RFQ_LSN_Missing_With_Target`), the wait-target cache counters
(`PolarDB_Wait_Target_LSN_Cache_Advanced`, `_Rejected`), and the profile-only
warmup timing globals (`PolarDB_Split_Warmup_Queue_Delay_{Count, Sum_Us}`,
`PolarDB_Split_Warmup_Connect_{Count, Sum_Us}`, `PolarDB_Split_Warmup_Add_{Count,
Sum_Us}`).

The remaining 15 profile-only globals describe worker-local and shared
selected-server transfers:

| Counter | Meaning |
|---|---|
| `PolarDB_Reader_Pool_Local_Take_Attempt` | Attempts to reuse an exact connection held by the current worker after server selection. |
| `PolarDB_Reader_Pool_Local_Take_Hit` | Local attempts that returned a connection. |
| `PolarDB_Reader_Pool_Local_Take_Miss` | Local attempts with no matching connection. |
| `PolarDB_Reader_Pool_Local_Store_Attempt` | Released exact reader connections considered for current-pass worker reuse. |
| `PolarDB_Reader_Pool_Local_Store_Accepted` | Connections retained by the worker for the current pass. |
| `PolarDB_Reader_Pool_Local_Store_Rejected` | Duplicate or unusable connections not retained by the worker. |
| `PolarDB_Reader_Pool_Local_Return_To_Shared` | Retained connections returned to shared server pools at pass end. |
| `PolarDB_Reader_Pool_Shared_Take_Attempt` | Attempts to take from a selected server's shared FREE list. |
| `PolarDB_Reader_Pool_Shared_Take_Hit` | Shared takes that returned a connection. |
| `PolarDB_Reader_Pool_Shared_Take_Miss` | Shared takes with no usable connection. |
| `PolarDB_Reader_Pool_Shared_Return_Attempt` | Direct or batched returns to shared server pools. |
| `PolarDB_Reader_Pool_Shared_Return_Accepted` | Shared returns accepted into FREE. |
| `PolarDB_Reader_Pool_Shared_Return_Rejected` | Shared returns rejected because the server or connection was no longer reusable. |
| `PolarDB_Reader_Pool_Shared_Return_Lock_Wait_Sum_Us` | Total time waiting for server pool mutexes during shared return. |
| `PolarDB_Reader_Pool_Shared_Return_Lock_Hold_Sum_Us` | Total time holding server pool mutexes during shared return. |

### 4.2 `POLARDB_PERF_DEBUG=1` histograms

47 thread counters for frontend write-path shape analysis:

- `PolarDB_Perf_Write_Bytes_*` — per-send byte-size histogram (9 buckets: `Le_512`,
  `Le_1KB`, `Le_2KB`, `Le_4KB`, `Le_8KB`, `Le_16KB`, `Le_32KB`, `Le_64KB`, `Gt_64KB`).
- `PolarDB_Perf_Write_Iov_*` — iovec-count histogram (7 buckets: `1`, `2`, `3_4`,
  `5_8`, `9_16`, `17_32`, `33_64`).
- `PolarDB_Perf_Packets_Per_Send_*` — protocol-packets-per-send histogram (7
  buckets: `1`, `2`, `3_4`, `5_8`, `9_16`, `17_32`, `33_64`).
- `PolarDB_Perf_Packet_Bytes_*` — per-packet byte-size histogram (9 buckets, same
  thresholds as `Write_Bytes`).
- `PolarDB_Perf_Plain_Send_{Calls, Bytes}` — plaintext buffered-path send volume.
- `PolarDB_Perf_WriteV_Skip_*` — direct-write skip reasons (`Disabled`, `Inactive`,
  `Encrypted`, `Not_Frontend`, `State`, `Session`, `Mirror`, `Poll`, `No_Packets`,
  `Queue_Pending`, `Queue_Partial`) plus `PolarDB_Perf_WriteV_View_Build_Calls` /
  `_Packets`.

## 5. Total and Subset Counter Pairs

Two pairs are intentionally equal in this implementation:

- `PolarDB_Session_LSN_Routing` and `PolarDB_Wait_Wrap_Prepared` normally move together because every LSN reader route prepares one wrapper.
- `PolarDB_Wait_Wrap_Prepared` splits into `PolarDB_Wait_LSN_Sent` plus
  `PolarDB_Wait_Wrap_Bypassed` in the healthy fast path; any remaining gap should
  be explained by `PolarDB_Wait_Wrap_Safety_Abort`.
- `PolarDB_Wait_Error_Timeout` and `PolarDB_Wait_Error_LSN_Wait_Timeout` are equal because LSN is the only implemented wait type.

They are kept separate so future CSN or transaction-split wait families can distinguish total behavior from the LSN subset without renaming the stats surface.

## 6. `polardb_active`

`polardb_active` is an internal `atomic<bool>` on `PgHGM->status`. It is true when at least one PolarDB hostgroup pair is configured. HGM accessors check it directly; workers copy it during publication refresh and the query route uses the worker-local `polardb_is_active()` gate, avoiding a shared load per query.

It is a condition, not a counter:

- It is not exported through `stats_pgsql_global`.
- It is not part of the 299 exported counters.
- It should be described as a boolean state flag.

## 7. Signals to Watch

| Symptom | Likely meaning |
|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ == 0` under traffic | libpq RFQ-LSN parsing is not active, startup profile did not request RFQ LSN, RFQ LSN is not being returned, traffic is not reaching PolarDB backends, or RFQs are being rejected as stale/missing writer-epoch data. |
| `PolarDB_Monitor_Health_Invalid_Role > 0` | The monitor saw a role outside `primary`/`master`, `replica`, and `standby`. ProxySQL `UNKNOWN` commonly means PolarDB returned the literal role `unknown` for `POLAR_UNKNOWN` while a node has no established role, or for `POLAR_STANDALONE_DATAMAX` (DataMax); ProxySQL treats it as non-reader/non-writer. |
| `PolarDB_Monitor_Health_Invalid_Values > 0` | The monitor saw invalid availability or LSN text. Availability must be one of `t`/`T`/`f`/`F`; LSN text must parse as WAL LSN text. Invalid availability text does not shun, and invalid LSN text is ignored. |
| `PolarDB_Write_Missing_LSN > 0` | Writer RFQ LSN was missing. Check backend support, libpq patch, startup profile, and pool reuse. |
| `PolarDB_Read_Missing_LSN > 0` | Tracked read RFQ LSN was missing while SESSION_LSN tracking was active. |
| `PolarDB_RFQ_Best_Effort_Degraded_Routes > 0` | The system served best-effort degraded simple-query reader routes without an RFQ wait target. Clients should also see WARNING notices on those reads. |
| `PolarDB_Consistency_Writer_Fallback > 0` | Consistency reads are failing closed to the writer during reader acquisition. This is safe, but it means offload is being lost because readers are missing/stale/over-lagged, group LSN is unknown under a cap, or strict RFQ requirements are not met. |
| `PolarDB_RFQ_Profile_Skipped > 0` | Pooled connections with non-RFQ startup profiles are present and being skipped for RFQ-required reads. |
| `PolarDB_RFQ_Profile_Evicted > 0` | Incompatible free pooled connections are being pruned under RFQ-required read pressure. |
| `PolarDB_Reader_Pool_Miss_Empty > 0` | Reader-pool lookups found no usable pooled reader; new reader backends are being created under read pressure. |
| `PolarDB_Session_Target_Epoch_Reset > 0` | Sessions observed a writer group/epoch scope change and had old LSN target or missing-LSN flag state invalidated. Correlate with monitor/admin failover movement and cross-group routing. |
| `PolarDB_Wait_Wrap_Safety_Abort > 0` | Wrapper construction failed and the request failed closed. |
| `PolarDB_Wait_Error_Timeout > 0` | Replica did not reach the requested LSN before the finite timeout. |
| `PolarDB_LSN_Stale_Count > 0` | Reader candidates were skipped during selection because their cached LSN was below the session's consistency target. |

Verified against this branch.
