# 12 - Threadvars and Observability

> Scope: per-thread PolarDB configuration mirrors and every exported PolarDB stat counter, including this feature's RFQ routing counters and inactive knobs. | Audience: O/M | Status: stable | Prereqs: [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md), [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) | Verified against: this branch

## 1. Thread-Local Knobs

The admin variables live on `PgSQL_Threads_Handler::variables`. At runtime, `refresh_variables()` copies them into `pgsql_thread___*` mirrors so the hot path can read stable thread-local values.

| Admin variable | Runtime mirror | Behavior |
|---|---|---|
| `pgsql-polardb_consistency_mode` | `pgsql_thread___polardb_consistency_mode` | `off`, `lsn`, or `primary`; used after session override and per-HG policy resolution. |
| `pgsql-polardb_wait_timeout_mode` | `pgsql_thread___polardb_wait_timeout_mode` | `best_effort` or `strict`; always emitted into the wrapper as `polar_consistency_mode`. |
| `pgsql-polardb_lag_bytes` | `pgsql_thread___polardb_lag_bytes` | Byte lag cap. `0` disables, positive values force writer when cached byte lag exceeds cap. |
| `pgsql-polardb_lag_ms` | `pgsql_thread___polardb_lag_ms` | Reserved. Runtime accepts only `0` in this implementation; no routing effect. |
| `pgsql-polardb_lag_wait_ms` | `pgsql_thread___polardb_lag_wait_ms` | Global default wait timeout. `1000` by default; `0` means wait indefinitely. |
| `pgsql-polardb_lsn_freshness_ms` | `pgsql_thread___polardb_lsn_freshness_ms` | Cached-LSN freshness window for lag checks. |
| `pgsql-polardb_monitor_lsn_updates` | `pgsql_thread___polardb_monitor_lsn_updates` | Enables/disables monitor-side LSN cache updates. |
| `pgsql-polardb_proxy_protocol` | `pgsql_thread___polardb_proxy_protocol` | Global startup protocol: `v15`, `legacy`, or `off`; per-HG `proxy_protocol='default'` inherits it. |
| `pgsql-polardb_route_rfq_policy` | `pgsql_thread___polardb_route_rfq_policy` | Missing RFQ target policy: `strict` forces writer, `best_effort` allows degraded reader routing. |
| `pgsql-polardb_session_lsn_baseline` | `pgsql_thread___polardb_session_lsn_baseline` | Empty-session baseline: `observed` uses ordinary reader routing; `primary` tries the primary LSN mirror. |
| `pgsql-polardb_proxy_identity_host` | `pgsql_thread___polardb_proxy_identity_host` | Fallback identity host for RFQ-requesting startup profiles; empty or non-wildcard IP literal. |
| `pgsql-polardb_proxy_identity_port` | `pgsql_thread___polardb_proxy_identity_port` | Fallback identity port for RFQ-requesting startup profiles; `0` means unset/staging. |

### Reserved `polardb_lag_ms`

`pgsql-polardb_lag_ms` is intentionally reserved. The runtime registration accepts only `0`, and the normal build has no PgSQL millisecond-lag producer. The source keeps a producer sketch: estimate catch-up time from byte lag divided by recent replay bytes per millisecond, then use that estimate as a time-based reader filter. Until that producer exists, use `polardb_lag_bytes` and `polardb_lsn_freshness_ms`.

## 2. Counter Family

This implementation exports **26 stat counters** through `stats_pgsql_global`, plus one internal `polardb_active` boolean gate. The gate is not exported as a stat counter and should not be summed with the counters.

The external names and values are stable, but the storage is split for hot-path
performance:

- 19 thread-backed counters use per-thread
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
- `PgSQL_Thread::~PgSQL_Thread()` folds a worker's 19 thread-backed counter slots
  into the matching global counters before the worker object is freed. Runtime
  PgSQL thread resize is not supported today, so this mainly preserves
  monotonic shutdown-time scrapes and future-proofs any later worker lifecycle
  changes.
- 7 global-only counters remain `PgHGM->status` atomics because they are monitor-side,
  configuration/failover-side, or rare failure-path events:
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

The same 26 counters are exposed in two places:

- SQL: `stats_pgsql_global` rows named `PolarDB_*`.
- Prometheus: `proxysql_polardb_*_total` counters registered through the active
  PgSQL HGM Prometheus path. The wait-latency sum is exported as
  `proxysql_polardb_wait_lsn_microseconds_total` so it exactly matches the SQL
  microsecond counter.

Prometheus collection is scrape/TSDB-time work, not query-path work. Each
collection aggregates 19 thread-backed counters across PgSQL worker threads and
reads 7 global-only atomics. Disabling the web endpoint or TSDB sampling
disables that runtime collection cost; there is no PolarDB-specific Prometheus
toggle.

| # | Exported name | Field | What it counts | Operator reading |
|---:|---|---|---|---|
| 1 | `PolarDB_Server_LSN_Updates_From_RFQ` | `polardb_server_lsn_updates_from_rfq` | Query RFQ carried an LSN and the direct HGM update gate accepted it for the current writer group+epoch. Counts reads and writes, including accepted RFQs that did not strictly advance the cached LSN. | Should grow under PolarDB traffic. Flat zero under traffic points at missing RFQ-LSN support, disabled LSN parsing, or stale/missing writer group/epoch rejection. |
| 2 | `PolarDB_LSN_Updates_From_Monitor` | `polardb_lsn_updates_from_monitor` | Monitor observed an LSN advance. | Confirms monitor-side LSN collection is active. |
| 3 | `PolarDB_Monitor_Health_Invalid_Role` | `polardb_monitor_health_invalid_role` | Monitor health row reported a role ProxySQL cannot route as primary or reader. | Backend role output is not usable for routing; the row is sanitized before it can affect reader state. |
| 4 | `PolarDB_Monitor_Health_Invalid_Values` | `polardb_monitor_health_invalid_values` | Monitor health row had invalid availability text or invalid LSN text. | Backend health values are not trustworthy. Invalid availability is not allowed to shun a server, and invalid LSN text is not fed into the cache. |
| 5 | `PolarDB_LSN_Stale_Count` | `polardb_lsn_stale_count` | Active byte-lag stale-sample counter. Increments when an enabled `max_lag_bytes` check finds missing or stale primary/reader LSN state. | Normally flat unless `max_lag_bytes` is enabled and LSN samples are missing or stale. |
| 6 | `PolarDB_Write_Missing_LSN` | `polardb_write_missing_lsn` | Writer query completed without RFQ LSN. | Deployment problem. Automatic LSN-mode reads in that session follow `route_rfq_policy` until a primary-sourced RFQ clears the latch. |
| 7 | `PolarDB_Read_Missing_LSN` | `polardb_read_missing_lsn` | Tracked SESSION_LSN read completed without RFQ LSN. | Deployment/profile problem or backend mismatch; later reads follow `route_rfq_policy` until a primary RFQ clears the latch. |
| 8 | `PolarDB_Primary_LSN_Unknown` | `polardb_primary_lsn_unknown` | `session_lsn_baseline=primary` needed the primary mirror but it was empty. | First-read baseline could not be enforced from primary cache. |
| 9 | `PolarDB_RFQ_Best_Effort_Degraded_Routes` | `polardb_rfq_best_effort_degraded_routes` | `route_rfq_policy=best_effort` allowed an eligible simple-query reader route without an RFQ-derived wait target. | Degraded consistency path; expected only if deliberately configured. Clients also receive a WARNING `NoticeResponse` before the result; the proxy log is edge-limited per session. |
| 10 | `PolarDB_Consistency_Writer_Fallback` | `polardb_consistency_writer_fallback` | Reader acquisition returned a consistency-safety status or strict RFQ-unavailable status, and this read was redirected to the writer. | Offload loss from consistency-safe writer fallback. Compare with reader status traces and lag/RFQ counters to identify the root cause. |
| 11 | `PolarDB_Wait_Reads_Retried_On_Writer` | `polardb_wait_reads_retried_on_writer` | A wait-wrapped reader query was retried once on the writer after strict wait timeout or reader connection loss, before any user result reached the client. | Reader-side failure was safely recovered on the writer. Check timeout and connection-loss counters for the reason. |
| 12 | `PolarDB_RFQ_Profile_Skipped` | `polardb_rfq_profile_skipped` | Pool acquisition skipped a pooled connection whose startup profile did not request RFQ LSN. | Old/off-profile pooled connections exist during RFQ-required reads. |
| 13 | `PolarDB_RFQ_Profile_Evicted` | `polardb_rfq_profile_evicted` | Pool acquisition evicted incompatible free pooled connections to create one RFQ-LSN-capable replacement. | Old/off-profile free pool capacity is being replaced under RFQ demand; one acquisition can evict more than one connection if the free pool is already over cap. |
| 14 | `PolarDB_TL_Cache_Bypassed_For_Target` | `polardb_tl_cache_bypassed_for_target` | Conservative thread-local RFQ reader fast path missed for a consistency-target read. | Targeted reads are reaching shared route-smart selection because no local cached backend already satisfied RFQ profile, state, freshness, target LSN, and byte-cap checks. |
| 15 | `PolarDB_Target_LSN_Preferred` | `polardb_target_lsn_preferred` | Reader acquisition acquired a fresh cached reader already at or beyond the target. | Consistency-target preference is avoiding waits where possible. |
| 16 | `PolarDB_Target_LSN_Fallback_Wait` | `polardb_target_lsn_fallback_wait` | No preferred target-reaching connection was acquired; acquisition fell back to the full candidate set and relies on the wait wrapper. | Normal under lag or sparse pools; correctness still comes from the backend wait. |
| 17 | `PolarDB_Session_Target_Epoch_Reset` | `polardb_session_target_epoch_reset` | Collect or accepted result processing saw a writer group/epoch scope change and discarded at least one old write/observed LSN target or missing-LSN latch. | Should move only around writer failover/topology movement or cross-group session movement. A rising value means old-scope session state was invalidated safely. |
| 18 | `PolarDB_Session_LSN_Routing` | `polardb_session_lsn_routing` | Planner chose a reader route with an LSN wait. | RYW read offload is happening. |
| 19 | `PolarDB_Wait_Wrap_Prepared` | `polardb_wait_wrap_prepared` | Wrapper preparation for a reader-with-wait decision. | Normally moves with `PolarDB_Session_LSN_Routing`. |
| 20 | `PolarDB_Wait_Wrap_Bypassed` | `polardb_wait_wrap_bypassed` | Backend acquisition selected a reader whose fresh cached LSN already reached the consistency target, so the staged wait wrapper was cleared. | Normal healthy fast path. Compare with `PolarDB_Target_LSN_Preferred`; a high value means many protected reads avoided the backend wait. |
| 21 | `PolarDB_Wait_Wrap_Safety_Abort` | `polardb_wait_wrap_safety_abort` | Wrapper construction failed before backend dispatch. | Should be 0. Non-zero means the wrapper safety path stopped an unprotected replica read. |
| 22 | `PolarDB_Wait_LSN_Sent` | `polardb_wait_lsn_sent` | LSN wait wrapper was successfully installed/sent. | Compare with prepared and bypassed counts; gaps beyond bypasses mean wrapper safety aborts. |
| 23 | `PolarDB_Wait_LSN_Sum_Us` | `polardb_wait_lsn_sum_us` | Total microseconds spent in LSN waits. | Divide by sent count for average wait latency. Bypassed reads do not add wait time. |
| 24 | `PolarDB_Wait_Error_Timeout` | `polardb_wait_error_timeout` | Total wait timeout events accounted once per wait. | Replicas are not catching up within configured timeout. |
| 25 | `PolarDB_Wait_Error_LSN_Wait_Timeout` | `polardb_wait_error_lsn_wait_timeout` | LSN-wait subset of timeout events. | Equals total in this LSN-only implementation; can diverge when future wait families exist. |
| 26 | `PolarDB_Wait_Error_Connection_Lost` | `polardb_wait_error_connection_lost` | Wait-wrapped reader lost its backend connection before any user result reached the client. | Reader/backend instability during protected reads; matching writer-retry counter means the read was recovered on the writer. |

## 3. Total and Subset Counter Pairs

Two pairs are intentionally equal in this implementation:

- `PolarDB_Session_LSN_Routing` and `PolarDB_Wait_Wrap_Prepared` normally move together because every LSN reader route prepares one wrapper.
- `PolarDB_Wait_Wrap_Prepared` splits into `PolarDB_Wait_LSN_Sent` plus
  `PolarDB_Wait_Wrap_Bypassed` in the healthy fast path; any remaining gap should
  be explained by `PolarDB_Wait_Wrap_Safety_Abort`.
- `PolarDB_Wait_Error_Timeout` and `PolarDB_Wait_Error_LSN_Wait_Timeout` are equal because LSN is the only implemented wait type.

They are kept separate so future CSN or transaction-split wait families can distinguish total behavior from the LSN subset without renaming the stats surface.

## 4. `polardb_active`

`polardb_active` is an internal `atomic<bool>` on `PgHGM->status`. It is true when at least one PolarDB hostgroup pair is configured. Accessors and the routing path check it first so non-PolarDB deployments bypass the feature cheaply.

It is a gate, not a counter:

- It is not exported through `stats_pgsql_global`.
- It is not part of the 26 exported counters.
- It should be described as a boolean state flag.

## 5. Signals to Watch

| Symptom | Likely meaning |
|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ == 0` under traffic | libpq RFQ-LSN parsing is not active, startup profile did not request RFQ LSN, RFQ LSN is not being returned, traffic is not reaching PolarDB backends, or RFQs are being rejected as stale/missing writer-epoch data. |
| `PolarDB_Monitor_Health_Invalid_Role > 0` | The monitor saw a role outside `primary`/`master`, `replica`, and `standby`. ProxySQL `UNKNOWN` commonly means PolarDB returned the literal role `unknown` for `POLAR_UNKNOWN` while a node has no established role, or for `POLAR_STANDALONE_DATAMAX` (DataMax); ProxySQL treats it as non-reader/non-writer. |
| `PolarDB_Monitor_Health_Invalid_Values > 0` | The monitor saw invalid availability or LSN text. Availability must be one of `t`/`T`/`f`/`F`; LSN text must parse as WAL LSN text. Invalid availability text does not shun, and invalid LSN text is ignored. |
| `PolarDB_Write_Missing_LSN > 0` | Writer RFQ LSN was missing. Check backend support, libpq patch, startup profile, and pool reuse. |
| `PolarDB_Read_Missing_LSN > 0` | Tracked read RFQ LSN was missing while SESSION_LSN tracking was active. |
| `PolarDB_RFQ_Best_Effort_Degraded_Routes > 0` | The system served best-effort degraded simple-query reader routes without an RFQ wait target. Clients should also see WARNING notices on those reads. |
| `PolarDB_Consistency_Writer_Fallback > 0` | Consistency reads are failing closed to the writer during reader acquisition. This is safe, but it means offload is being lost because readers are missing/stale/over-lagged, primary LSN is unknown under a cap, or strict RFQ requirements are not met. |
| `PolarDB_RFQ_Profile_Skipped > 0` | Pooled connections with non-RFQ startup profiles are present and being skipped for RFQ-required reads. |
| `PolarDB_RFQ_Profile_Evicted > 0` | Incompatible free pooled connections are being pruned under RFQ-required read pressure. |
| `PolarDB_TL_Cache_Bypassed_For_Target > 0` | Consistency-target reads missed the conservative per-thread cache fast path and used shared route-smart selection. |
| `PolarDB_Session_Target_Epoch_Reset > 0` | Sessions observed a writer group/epoch scope change and had old LSN target or missing-LSN latch state invalidated. Correlate with monitor/admin failover movement and cross-group routing. |
| `PolarDB_Wait_Wrap_Safety_Abort > 0` | Wrapper construction failed and the request failed closed. |
| `PolarDB_Wait_Error_Timeout > 0` | Replica did not reach the requested LSN before the finite timeout. |
| `PolarDB_LSN_Stale_Count > 0` | An enabled byte-lag cap saw missing or stale primary/reader LSN samples. |

Verified against this branch.
