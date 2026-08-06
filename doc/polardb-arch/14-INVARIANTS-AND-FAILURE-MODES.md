# 14 - Invariants and Failure Modes

> Scope: this LSN-only feature's LSN read-your-writes guarantee, the invariants it depends on, and the failure-mode catalogue for current behavior. | Audience: R/M/O/C | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) | Verified against: this branch

## 1. Guarantee

For autocommit simple-query and extended-protocol reads routed automatically by the LSN policy, this LSN-only feature provides monotonic session-LSN routing for one client session: after result processing records a positioned write or observed LSN, later automatic replica-eligible reads are sent to a reader with an explicit wait target for `polardb_session_consistency.target()` (`max(write_lsn, observed_lsn)`), use a confirmed target-ready bypass, or are forced to the writer. Only eligible simple-query reads may use the explicitly accounted `best_effort` degraded path when no exact target exists.

The guarantee does not cover manual reader routes, extended-protocol transaction split, or CSN commit-counter consistency. Extended autocommit waits require `v15_wait`; profiles without it use the writer. Transaction-split reads (doc 19) and reader-failure recovery (doc 20) are separate subsystems, not absent. Reader-failure recovery is active for autocommit wait-protected reader queries and for simple-query transaction-split reads.

## 2. Preconditions

| Precondition | Reason |
|---|---|
| `POLARDB_PROXY=1` build with patched libpq | ProxySQL needs `PQhasLSN()` / `PQgetLSN()` and RFQ LSN parsing. |
| PolarDB backend supports RFQ LSN and wait GUCs | `polar_xact_split_wait_lsn`, `polar_consistency_mode`, and `polar_proxy_wait_timeout_ms` are required for wrapped reads. |
| RFQ-requesting startup profile and backend RFQ LSN support | Startup parameters request RFQ LSN, but only result RFQs carrying LSN confirm usable capability. Missing RFQ LSN is handled by flags and route policy. |
| Query is automatic, autocommit, single-statement simple query | The wrapper is SQL text prepended to a simple-query packet. |
| Manual routes are not used when RYW is required | Manual destination-hostgroup routes are authoritative and bypass PolarDB wrapping. |

## 3. Core Invariants

| ID | Invariant | Current behavior |
|---|---|---|
| I1 | Session write and observed LSNs are per client session and monotonic. | `polardb_session_consistency.write_lsn` advances on positioned writes; `polardb_session_consistency.observed_lsn` advances on any positioned RFQ; neither regresses. |
| I2 | Missing RFQ LSN does not invent a target. | Missing write/read RFQ LSN sets the corresponding flag, increments `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN`, and later automatic LSN-mode reads follow `pgsql-polardb_action_missing_lsn`. |
| I3 | Wait-protected reads always emit explicit policy. | Simple query emits mode, timeout, and target LSN SETs; extended `v15_wait` encodes all three fields in `W`. No backend policy inheritance is relied on. A reader already confirmed at the target emits neither mechanism; see I9. |
| I4 | Wrapper result filtering is leading-only and counted. | The connection skips the prepended SET results and forwards the user query result. |
| I5 | Timeout accounting is exactly-once per wait. | `polardb_account_wait_timeout()` checks the active wait and `wait_started_at_us`, then zeroes the timer through latency accounting. |
| I6 | Timeout recognition uses exact backend provenance. | ProxySQL requires `PG_DIAG_MESSAGE_DETAIL == polar_proxy_lsn_wait_timeout` and `PG_DIAG_SOURCE_FUNCTION == polar_proxy_wait_for_lsn`. Marker-less 57014 cancellation is not retried. |
| I7 | Per-query wait state resets; session LSN state survives. | RESET-style cleanup clears transient wait/wrapper/notice state but keeps write/observed LSN state. |
| I8 | Non-PolarDB and disabled builds bypass safely. | `polardb_active` and `#if POLARDB_PROXY` controls keep the feature out of normal routing when not configured/compiled. |
| I9 | The backend wait may be skipped only for a reader confirmed caught up. | The SQL wrapper or extended `W` is the read-your-writes condition for every consistency read **except** one whose selected reader is confirmed (by a fresh cached LSN) to have already reached the target. In that case `reset_wait()` clears the shared staged intent before dispatch, neither wire mechanism is emitted, and `PolarDB_Wait_Wrap_Bypassed` increments. For any other reader the protocol-appropriate wait remains mandatory, so the bypass only removes a wait that would have been a no-op. |
| I10 | The query-result cache never bypasses the wait. | The `cache_ttl` query-result cache lookup runs before `collect()`/`plan()`, so `polardb_query_cache_disabled_for_current_rule()` re-derives the planner's decision from the cheap pre-plan inputs and disables **both** the cache GET and the cache store whenever the session has a consistency obligation (`polardb_session_consistency.target() > 0`, a `write_unknown`/`observed_unknown` flag, or `GLOBAL_LSN`; target-free `SESSION_LSN` is handled according to its current route policy). Evaluated per query and fail-closed, so a result cached before a write can never be served to a read issued after it; reads with no obligation (`mode=off`, or no prior write) cache normally. Mechanism: 06 §E.6. |

## 4. Failure-Mode Catalogue

| ID | Trigger | Behavior | Safety / observability |
|---|---|---|---|
| F1 | Writer query completes but RFQ carries no LSN. | Session is marked `polardb_session_consistency.write_unknown`; automatic LSN-mode reads use `action_missing_lsn`. | `primary` preserves RYW by writer routing; `warning` permits degraded simple-query reader routing. Extended unknown-target reads force the writer. `PolarDB_Write_Missing_LSN` increments; first transition emits `proxy_warning`. Every degraded simple-query route also emits a client WARNING notice. |
| F2 | Tracked SESSION_LSN read completes but RFQ carries no LSN. | Session is marked `polardb_session_consistency.observed_unknown`; automatic LSN-mode reads use `action_missing_lsn`. | `PolarDB_Read_Missing_LSN` increments; first transition emits `proxy_warning`. Every degraded simple-query route also emits a client WARNING notice; extended unknown-target reads force the writer. |
| F3 | `GLOBAL_LSN` requires a group target but no current group LSN exists. | `GROUP_LSN_UNKNOWN` goes through `action_missing_lsn`. | `PolarDB_Group_LSN_Unknown` increments. If `warning` degrades an eligible simple-query route, the client receives the same WARNING notice. Extended unknown-target reads force the writer. |
| F4 | Backend does not recognize wrapper GUCs. | The wrapper SET fails and the error reaches the client. | Not a silent stale read. Capability probing is future work. |
| F5 | Manual destination-hostgroup route points to reader. | Manual route bypasses PolarDB planning/wrapping. | By design; no RYW guarantee for manual reader routes. Operator must not manually route reads to readers while expecting RYW. |
| F6 | Wrapper construction fails before dispatch. | Request returns an internal error and `polardb_wait_disabled` keeps later reads on the writer until reset. | Safe writer fallback for later reads. `PolarDB_Wait_Wrap_Safety_Abort` increments. |
| F7 | Byte lag cap exceeded or lag data missing/stale. | Automatic read is forced to writer. | Safe writer fallback. |
| F8 | Explicit transaction or multi-statement query. | Forced to writer. | Safe writer fallback; the feature is autocommit/single-statement only. |
| F9 | Extended protocol, no protected target. | Automatic read may use reader without wrapper. | Safe for RYW because the session has no protected target. |
| F10 | Extended autocommit protocol after a known session target. | A negotiated `v15_wait` backend receives in-band `W` immediately before Parse or Bind/Execute; unsupported profiles and unsafe frame shapes use the writer. | The wait and command share one libpq flush. Once a backend command is sent, the Sync frame remains pinned to that backend. |
| F11 | Pooled connection startup profile did not request RFQ LSN for an RFQ-required read. | Pool acquisition skips/counts it and tries another candidate or a fresh connection. If old-profile free connections fill `max_connections` capacity and creation is otherwise allowed, enough incompatible free connections are evicted to leave room for one replacement. | `PolarDB_RFQ_Profile_Skipped` increments; `PolarDB_RFQ_Profile_Evicted` increments once per evicted connection. Compatible pooled connections are not closed for this reason. |
| F12 | `best_effort` finite timeout. | Backend returns rows plus one PolarDB timeout warning for the wait event. | Best-effort contract allows stale rows with warning; timeout counters increment. |
| F13 | `strict` finite timeout. | Backend raises ERROR and does not serve stale rows. | Strict contract preserved; timeout counters increment. |
| F14 | User emits timeout-looking text. | Not counted as PolarDB timeout without structured detail marker. | Safe. Generic notice/error path handles it normally. |
| F15 | `POLARDB_PROXY=0` or no PolarDB hostgroups. | PolarDB routing does not run. | Ordinary PostgreSQL behavior remains compatible; generic extended-protocol correctness is shared and PolarDB counters stay absent/flat. |
| F16 | `pgsql-polardb_max_reader_lag_ms` / `PolarDB_LSN_Stale_Count`. | Runtime lag-ms accepts only `0`, but `PolarDB_LSN_Stale_Count` is active for byte-lag safety. | Do not use `polardb_max_reader_lag_ms`; use `max_lag_bytes`/`polardb_max_reader_lsn_gap_bytes` and monitor stale-count growth as a real byte-lag signal. |
| F17 | Internal extended-frame candidate or backend-owner invariant is violated. | Pre-send candidate drift returns a protocol error and discards through Sync. Post-send owner drift marks the backend non-reusable and surfaces a fatal connection error. | The affected frame/backend is contained; release builds do not abort the ProxySQL process. |

## 5. Open Limitations

| Limitation | Status |
|---|---|
| Capability probe for wrapper GUC support | Future improvement. Today unsupported backends error on wrapper SET. |
| Prometheus exposure for PolarDB counters | Implemented through the active PgSQL HGM metrics path; scrape/TSDB collection aggregates thread-backed counters and reads global-only counters. |
| Dedicated counters for each writer-forcing action reason | Partially implemented. `PolarDB_Route_To_Writer`, `PolarDB_Route_Manual_To_Writer`, `PolarDB_Route_Manual_Forced_Writer`, `PolarDB_Route_Locked_Hostgroup`, `PolarDB_Route_No_Wait_Target`, and `PolarDB_Consistency_Writer_Fallback` exist; a generic per-reason `FORCE_PRIMARY` breakdown is still trace-only. |
| Extended-protocol wait | Implemented for autocommit through negotiated `v15_wait`; extended transaction split remains unsupported. Current policy is described in F9-F10. |
| Millisecond lag cap | Reserved until a real PgSQL producer exists. |

Verified against this branch.
