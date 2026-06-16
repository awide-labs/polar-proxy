# 14 - Invariants and Failure Modes

> Scope: this LSN-only feature's LSN read-your-writes guarantee, the invariants it depends on, and the failure-mode catalogue for current behavior. | Audience: R/M/O/C | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md), [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md), [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) | Verified against: this branch

## 1. Guarantee

For autocommit simple-query reads routed automatically by the LSN policy, this LSN-only feature provides monotonic session-LSN routing for one client session: after result processing records a positioned write or observed LSN, later automatic replica-eligible simple-query reads are sent to a reader with an explicit wait target for `polardb_session_consistency.target()` (`max(write_lsn, observed_lsn)`), are forced to the writer, or, only when `pgsql-polardb_route_rfq_policy=best_effort`, are explicitly accounted as degraded routes without a wait target.

The guarantee does not cover manual reader routes, extended-protocol wait wrapping, transaction-split reads, CSN/global consistency, or general reader-failure policy. The reader-failure recovery in this branch is limited to autocommit wait-wrapped reader queries: a strict LSN wait timeout or a lost reader connection can be retried once on the writer before any user result has started.

## 2. Preconditions

| Precondition | Reason |
|---|---|
| `POLARDB_PROXY=1` build with patched libpq | ProxySQL needs `PQhasLSN()` / `PQgetLSN()` and RFQ LSN parsing. |
| PolarDB backend supports RFQ LSN and wait GUCs | `polar_xact_split_wait_lsn`, `polar_consistency_mode`, and `polar_proxy_wait_timeout_ms` are required for wrapped reads. |
| RFQ-requesting startup profile and backend RFQ LSN support | Startup parameters request RFQ LSN, but only result RFQs carrying LSN confirm usable capability. Missing RFQ LSN is handled by latches and route policy. |
| Query is automatic, autocommit, single-statement simple query | The wrapper is SQL text prepended to a simple-query packet. |
| Manual routes are not used when RYW is required | Manual destination-hostgroup routes are authoritative and bypass PolarDB wrapping. |

## 3. Core Invariants

| ID | Invariant | Current behavior |
|---|---|---|
| I1 | Session write and observed LSNs are per client session and monotonic. | `polardb_session_consistency.write_lsn` advances on positioned writes; `polardb_session_consistency.observed_lsn` advances on any positioned RFQ; neither regresses. |
| I2 | Missing RFQ LSN does not invent a target. | Missing write/read RFQ LSN sets the corresponding latch, increments `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN`, and later automatic LSN-mode reads follow `pgsql-polardb_route_rfq_policy`. |
| I3 | Wrapped reads always emit explicit policy. | Mode, timeout, and target LSN SETs are emitted for every consistency wait. No backend policy inheritance is relied on. (Applies to a consistency read that actually wraps; a read whose selected reader is already at the target is not wrapped — see I9.) |
| I4 | Wrapper result filtering is leading-only and counted. | The connection skips the prepended SET results and forwards the user query result. |
| I5 | Timeout accounting is exactly-once per wait. | `polardb_account_wait_timeout()` checks the active wait and `wait_started_at_us`, then zeroes the timer through latency accounting. |
| I6 | Timeout recognition uses structured backend marker. | ProxySQL checks `PG_DIAG_MESSAGE_DETAIL == polar_proxy_lsn_wait_timeout`, not human-readable text. |
| I7 | Per-query wait state resets; session LSN state survives. | RESET-style cleanup clears transient wait/wrapper/notice state but keeps write/observed LSN state. |
| I8 | Non-PolarDB and disabled builds bypass safely. | `polardb_active` and `#if POLARDB_PROXY` gates keep the feature out of normal routing when not configured/compiled. |
| I9 | The wait wrapper may be skipped only for a reader proven caught up. | The wait wrapper is the read-your-writes gate for every consistency read **except** one whose selected reader is proven (by a fresh cached LSN) to have already reached the consistency target — a thread-local cached backend at the target, or a reader acquired from the fresh target-reached prefix with `PolarDB_ReaderResult::wait_bypass_allowed`. In that one case the session clears the staged wait before wrap finalize (`reset_wait()`), the wrapper emits nothing, and `PolarDB_Wait_Wrap_Bypassed` increments. For any other reader the wait `SET` stays the correctness gate, so RYW is never weakened — the bypass only skips a wait that would have been a no-op. |
| I10 | The query-result cache never bypasses the wait. | The `cache_ttl` query-result cache lookup runs before `collect()`/`plan()`, so `polardb_query_cache_disabled_for_current_rule()` re-derives the planner's decision from the cheap pre-plan inputs and disables **both** the cache GET and the cache store whenever the session has a consistency obligation (`polardb_session_consistency.target() > 0`, a `write_unknown`/`observed_unknown` latch, or a `PRIMARY` first-read baseline; `mode=primary` also disables it). Evaluated per query and fail-closed, so a result cached before a write can never be served to a read issued after it; reads with no obligation (`mode=off`, or no prior write) cache normally. Mechanism: 06 §E.6. |

## 4. Failure-Mode Catalogue

| ID | Trigger | Behavior | Safety / observability |
|---|---|---|---|
| F1 | Writer query completes but RFQ carries no LSN. | Session is marked `polardb_session_consistency.write_unknown`; automatic LSN-mode reads use `route_rfq_policy`. | `strict` preserves RYW by writer routing; `best_effort` permits degraded simple-query reader routing. Extended unknown-target reads force the writer. `PolarDB_Write_Missing_LSN` increments; first transition emits `proxy_warning`. Every degraded simple-query route also emits a client WARNING notice. |
| F2 | Tracked SESSION_LSN read completes but RFQ carries no LSN. | Session is marked `polardb_session_consistency.observed_unknown`; automatic LSN-mode reads use `route_rfq_policy`. | `PolarDB_Read_Missing_LSN` increments; first transition emits `proxy_warning`. Every degraded simple-query route also emits a client WARNING notice; extended unknown-target reads force the writer. |
| F3 | `session_lsn_baseline=primary` but the primary mirror is empty. | `PRIMARY_LSN_UNKNOWN` goes through `route_rfq_policy`. | `PolarDB_Primary_LSN_Unknown` increments. If `best_effort` degrades a simple-query route, the client receives the same WARNING notice. Extended unknown-target reads force the writer. |
| F4 | Backend does not recognize wrapper GUCs. | The wrapper SET fails and the error reaches the client. | Not a silent stale read. Capability probing is future work. |
| F5 | Manual destination-hostgroup route points to reader. | Manual route bypasses PolarDB planning/wrapping. | By design; no RYW guarantee for manual reader routes. Operator must not manually route reads to readers while expecting RYW. |
| F6 | Wrapper construction fails before dispatch. | Request returns an internal error and `polardb_wait_disabled` pins later reads to writer until reset. | Safe writer fallback for later reads. `PolarDB_Wait_Wrap_Safety_Abort` increments. |
| F7 | Byte lag cap exceeded or lag data missing/stale. | Automatic read is forced to writer. | Safe writer fallback. |
| F8 | Explicit transaction or multi-statement query. | Forced to writer. | Safe writer fallback; the feature is autocommit/single-statement only. |
| F9 | Extended protocol, no protected target. | Automatic read may use reader without wrapper. | Safe for RYW because the session has no protected target. |
| F10 | Extended protocol after known session target. | Forced to writer because this implementation cannot wait-wrap Parse/Bind/Execute. | Safe writer fallback with action reason `EXTENDED_PROTOCOL`. |
| F11 | Pooled connection startup profile did not request RFQ LSN for an RFQ-required read. | Pool acquisition skips/counts it and tries another candidate or a fresh connection. If old-profile free connections fill `max_connections` capacity and creation is otherwise allowed, enough incompatible free connections are evicted to leave room for one replacement. | `PolarDB_RFQ_Profile_Skipped` increments; `PolarDB_RFQ_Profile_Evicted` increments once per evicted connection. Compatible pooled connections are not closed for this reason. |
| F12 | `best_effort` finite timeout. | Backend returns rows plus one PolarDB timeout warning for the wait event. | Best-effort contract allows stale rows with warning; timeout counters increment. |
| F13 | `strict` finite timeout. | Backend raises ERROR and does not serve stale rows. | Strict contract preserved; timeout counters increment. |
| F14 | User emits timeout-looking text. | Not counted as PolarDB timeout without structured detail marker. | Safe. Generic notice/error path handles it normally. |
| F15 | `POLARDB_PROXY=0` or no PolarDB hostgroups. | Feature does not run. | Upstream behavior. PolarDB counters stay flat. |
| F16 | `pgsql-polardb_lag_ms` / `PolarDB_LSN_Stale_Count`. | Runtime lag-ms accepts only `0`, but `PolarDB_LSN_Stale_Count` is active for byte-lag safety. | Do not use `polardb_lag_ms`; use `max_lag_bytes`/`polardb_lag_bytes` and monitor stale-count growth as a real byte-lag signal. |

## 5. Open Limitations

| Limitation | Status |
|---|---|
| Capability probe for wrapper GUC support | Future improvement. Today unsupported backends error on wrapper SET. |
| Prometheus exposure for PolarDB counters | Implemented through the active PgSQL HGM metrics path; scrape/TSDB collection aggregates thread-backed counters and reads global-only counters. |
| Dedicated counters for each writer-forcing action reason | Not in this implementation. Routing reason is visible only in debug traces. |
| Extended-protocol wait wrapper | Not in this implementation. Current policy is described in F9-F10. |
| Millisecond lag cap | Reserved until a real PgSQL producer exists. |

Verified against this branch.
