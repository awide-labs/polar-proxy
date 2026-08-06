# 09 - Result Processing: RFQ-LSN Capture and Write Tracking

> Scope: the `polardb_process_result()` response path: read backend RFQ payloads, advance session write/observed LSNs, refresh per-server LSN cache, and maintain missing-LSN flags. | Audience: R/M/O/C | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md), [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md) | Verified against: this branch

## 1. Purpose

Result processing is the response-side half of read-your-writes. After a query succeeds, ProxySQL reads the backend WAL LSN that libpq parsed from ReadyForQuery (RFQ). It uses that LSN in three ways:

1. For any positioned RFQ, advance this client session's observed LSN (`polardb_session_consistency.observed_lsn`).
2. If the query was a writer query, advance this client session's write LSN (`polardb_session_consistency.write_lsn`).
3. For any accepted current-group/current-epoch LSN-bearing RFQ, refresh the per-server LSN cache and increment `PolarDB_Server_LSN_Updates_From_RFQ`.

Result processing never chooses routing, never wraps SQL, and never issues a query. It reads already-parsed libpq state.

## 2. Call Site and checks

`PgSQL_Session::polardb_process_result()` is defined in
`lib/PgSQL_PolarDB_Flow.cpp`. A normal request calls it from the successful
`PgSQL_Session::RequestEnd()` path when `polardb_config.is_polardb_enabled` is
true. Extended Execute+Flush is different: the semantic result arrives before
the final backend RFQ, so `RequestEnd()` records its attribution in
`polardb_extended_rfq`. Sync later applies the one positioning RFQ to all
successful deferred Execute results through
`polardb_process_deferred_extended_rfq()`. Result attribution (`pending`) and
frontend RFQ publication (`publication_pending`) have separate lifetimes. A
normal backend-owned Sync generates its RFQ before `RequestEnd()`, so that RFQ
uses the deferred policy and `RequestEnd()` closes publication only after it
has consumed the final result attribution.

The function returns immediately when PolarDB is disabled, the data stream is missing, or the backend connection is missing. That keeps non-PolarDB and failed-query paths out of the result-processing logic.

Before dispatch, the routing path captures the replication group's current writer hostgroup and writer epoch in `polardb_query.request_writer_scope`. LSN-bearing RFQs are accepted only by the direct HGM update path, which repeats the group+epoch check through the topology snapshot and updates only atomic per-server/primary-LSN cells. If that update path rejects the RFQ, `polardb_process_result()` skips all session LSN side effects. Missing-LSN RFQs repeat a current group+epoch check immediately before setting missing-LSN flags. Any accepted positioned or missing-LSN result first scopes the session LSN state to the request writer group+epoch, clearing old write/observed LSNs and unknown flags if the session was unscoped, belonged to another replication group, or belonged to an older writer epoch. These skips are trace-observable and deliberately do not add a new counter.

The immutable worker snapshot provides the hostgroup policy and shared-state
pointers, but `get_thread_cached_polardb_hg_config()` reads `writer_epoch` from
the shared atomic with acquire ordering on every active PolarDB lookup. A
failover epoch change therefore becomes visible immediately; correctness does
not depend on a publication wake or the periodic worker snapshot refresh.

## 3. RFQ LSN Read

Result processing calls:

```cpp
uint64_t lsn = myds->myconn->get_polardb_lsn();
bool has_lsn = (lsn > 0);
```

`PgSQL_Connection::get_polardb_lsn()` returns `PQgetLSN()` only when the libpq connection is healthy and `PQhasLSN()` is true. Otherwise it returns `0`.

This is RFQ-only. SQL-based LSN probing belongs to the monitor, not the request/response hot path.

## 4. Write Classification

Result processing classifies the completed query with `PolarDB_Protocol::is_write_query(query_digest_text)`. The classifier is conservative:

- Plain `SELECT`, `SHOW`, and `EXPLAIN` are reads.
- `EXPLAIN ANALYZE <DML>` is still classified as a read by this coarse
  result-path classifier. RYW remains safe because the RFQ LSN still advances
  `observed_lsn`; only the diagnostic own-write component (`write_lsn`) is less
  precise for that shape.
- `SELECT ... FOR ...` is treated as a write because it takes locks.
- `WITH` and anything else not recognized as a read are treated as writes.

The planner uses `replica_eligible`; result processing uses the digest classifier only to decide whether a successful RFQ LSN should advance the session write target.

## 5. Four Result-Processing Outcomes

| Query class | RFQ LSN? | Session LSN state | Per-server cache | Counter / signal |
|---|---:|---|---|---|
| missing/stale writer group or epoch | any | unchanged | no update | trace only |
| write | yes | tag session with request writer group+epoch; after the direct HGM update accepts: `observed_lsn=max(old, lsn)` and `write_lsn=max(old, lsn)`; primary-sourced RFQ clears write/observed unknown flags | accepted update | `PolarDB_Server_LSN_Updates_From_RFQ++` |
| write | no | tag session with request writer group+epoch; leave old LSNs; set `polardb_session_consistency.write_unknown=true` | no update | `PolarDB_Write_Missing_LSN++`; one `proxy_warning` on first transition |
| read | yes | tag session with request writer group+epoch; after the direct HGM update accepts: `observed_lsn=max(old, lsn)`; replica RFQ does not clear unknown flags | accepted update | `PolarDB_Server_LSN_Updates_From_RFQ++` |
| tracked SESSION_LSN read | no | tag session with request writer group+epoch; leave old LSNs; set `polardb_session_consistency.observed_unknown=true` | no update | `PolarDB_Read_Missing_LSN++`; one `proxy_warning` on first transition |
| untracked read | no | unchanged | no update | trace only |

Writes are observations, but `polardb_session_consistency.write_lsn` is kept separately for diagnostics and future policy. The wait target for SESSION_LSN reads is `max(polardb_session_consistency.write_lsn, polardb_session_consistency.observed_lsn)`.

ProxySQL does not guess an LSN from monitor/global state. If a query that should preserve SESSION_LSN monotonicity completes without RFQ LSN, the matching unknown flag is set and later automatic LSN-mode reads route through `pgsql-polardb_action_missing_lsn`.

## 6. Client-Visible RFQ LSN — Raise to a Confirmed Target

Result processing also decides the LSN ProxySQL forwards to an **LSN-aware client**. A client that sent `_polar_send_lsn=true` (or `_polar_proxy_send_lsn=true`) at startup expects every ReadyForQuery to carry an LSN it can read with `PQhasLSN()` / `PQgetLSN()`. Because ProxySQL may serve consecutive queries of one session from different backend connections, the raw backend RFQ LSN alone could move **backwards** across reuse. `PgSQL_Session::polardb_client_ready_lsn()` (`lib/PgSQL_PolarDB_Flow.cpp`) prevents that: the client-visible LSN is the maximum of the backend RFQ LSN, a writer-confirmed session target, and a successful wait target.

The decision (`polardb_client_rfq_decision()`, `include/PgSQL_PolarDB.h`) is:

1. **Eligibility check** — if the client did not request an LSN, or the backend RFQ carried no LSN payload, emit a standard RFQ with no LSN (`PQhasLSN()==0`). ProxySQL never adds an LSN to a payload-less RFQ, even when the session has a target.
2. **Start** from the backend's reported LSN.
3. **Raise to a confirmed target** — take the maximum of the backend LSN and:
   - the **session target** (`polardb_session_consistency.target()`), but only on a response **from the writer** for the session's current writer group+epoch (`polardb_positioned_rfq_from_primary()` + scope match). A writer response must never report a position below what the session has already committed.
   - the **successful wait target** — the LSN of a finalized, non-timed-out `SET polar_xact_split_wait_lsn` whose wrapper SET succeeded (`wait.wrapper_finalized && !wait.timeout_error && wrapper_set_succeeded()`). The successful wait confirms the reader reached that LSN.
4. If the chosen target exceeds the backend LSN, the client-visible LSN is **raised** and the raise is attributed to the writer or the wait.

| Situation | Client-visible LSN | Counter |
|---|---|---|
| client didn't request, or backend RFQ carried no LSN | none (standard RFQ) | — |
| backend LSN ≥ any confirmed target | backend LSN, unchanged | — |
| writer response below the session's committed target | raised to the session target | `PolarDB_Client_RFQ_LSN_Raised_To_Target++`, `PolarDB_Client_RFQ_LSN_Raised_By_Writer++` |
| reader response after a successful LSN wait | raised to the wait target | `PolarDB_Client_RFQ_LSN_Raised_To_Target++`, `PolarDB_Client_RFQ_LSN_Raised_By_Wait++` |

The client-visible stream is monotonic and bounded by confirmed positions: a writer response or a successful reader wait. Query cache is disabled for LSN-aware clients (see [06-ROUTING-PIPELINE.md §E.6](06-ROUTING-PIPELINE.md)) so a cached RFQ cannot replay a stale or lower LSN. The client-side accessors that read this value are covered in [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md).

## 7. Missing LSN Flow

A missing RFQ LSN is abnormal for a correctly configured RFQ-requesting PolarDB deployment. It can indicate a plain PostgreSQL backend, an unpatched libpq, a backend that was not asked to send RFQ LSN, a stale pooled connection whose startup profile did not request RFQ LSN, or another deployment mismatch.

Current code handles it as follows:

1. In result processing, if `is_write && !has_lsn`, set `polardb_session_consistency.write_unknown=true`.
2. In result processing, if a tracked SESSION_LSN read has no RFQ LSN, set `polardb_session_consistency.observed_unknown=true`.
3. Increment `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN` respectively.
4. Emit a `proxy_warning` only on the first transition from known to unknown, so logs are useful but not spammed per query.
5. In planning, if either flag is true, return the matching action reason (`WRITE_LSN_UNKNOWN` or `OBSERVED_LSN_UNKNOWN`) through `pgsql-polardb_action_missing_lsn`.
6. A later primary-sourced positioned RFQ clears both unknown flags. A replica-sourced RFQ advances observed/cache state but does not clear them.

Under `strict`, the policy preserves RYW by avoiding automatic replica reads when the exact wait target is unknown. Under `best_effort`, eligible simple-query reads can proceed without a wait, are accounted as degraded routes, and receive a WARNING `NoticeResponse` before the result. The proxy log for degraded routing is edge-limited per session while the degradation remains active, but the degraded-route counter and simple-query client notice are per route. Extended-protocol unknown-target reads conservatively use the writer because there is no target for `W`; this is separate from timeout notices on a real extended wait, which are preserved through implicit Parse ownership.

## 8. Per-Server Cache Update

For every RFQ with an LSN, read or write, `polardb_process_result()` first calls the direct server-pointer overload:

```cpp
PgHGM->polardb_update_server_lsn(backend_srv, backend_hg, backend_config, lsn, polardb_query.request_writer_scope)
```

The function is `polardb_update_server_lsn` (not `update_server_lsn`), the first argument is the `PgSQL_SrvC*` `backend_srv` (not `parent`), and the request identity is a single `const PolarDB_WriterScope& request_scope` argument (`polardb_query.request_writer_scope`) — not two separate `request_writer_hg`/`request_writer_epoch` scalars. The signature has five parameters, not six.

Result processing resolves `backend_config` once from the topology snapshot and passes it to that overload with the backend hostgroup plus the request writer scope (hostgroup+epoch). The overload performs the current group+epoch check, advances `PgSQL_SrvC::polardb_current_lsn`, and updates the snapshot's shared writer-scope group-LSN cell without taking the global HGM lock or rereading HGC state. If the request identity is missing/stale/cross-group, the current epoch is missing, or the backend server pointer/hostgroup is unusable, the RFQ is rejected and result processing does not update session LSN state or the RFQ counter.

`PolarDB_Server_LSN_Updates_From_RFQ` increments only after this direct HGM update path accepts the RFQ as current-group/current-epoch data. Acceptance does not require the cached LSN to strictly increase; equal/older LSNs can still be accepted for session monotonicity while leaving the cache unchanged.

This is intentionally broader than write tracking. A reader RFQ LSN tells ProxySQL how far that reader has replayed, so it is useful for lag and freshness decisions even though it does not advance the client session's write target. The shared writer-scope cell is monotonic and should be read as "latest trusted group LSN observed by ProxySQL"; it is not a synchronous query of the primary's current WAL tip.

## 9. Counters Owned by This Stage

| Counter | Meaning |
|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ` | An RFQ carried an LSN and the direct HGM update path accepted it for the current writer group+epoch. Counts reads and writes, including accepted RFQs whose LSN did not strictly advance the cache. |
| `PolarDB_Write_Missing_LSN` | A writer query completed but RFQ carried no LSN. Later automatic LSN-mode reads in that session follow `action_missing_lsn` until a primary-sourced RFQ clears the flag. |
| `PolarDB_Read_Missing_LSN` | A tracked SESSION_LSN read completed but RFQ carried no LSN. Later automatic LSN-mode reads follow `action_missing_lsn` until a primary-sourced RFQ clears the flag. |
| `PolarDB_Client_RFQ_LSN_Raised_To_Target` | The client-visible RFQ LSN was raised above the backend's reported LSN to a confirmed session or wait target so the client's LSN stream does not move backwards across backend reuse (see §6). |
| `PolarDB_Client_RFQ_LSN_Raised_By_Writer` | A raise (above) used the session's committed target on a writer response. |
| `PolarDB_Client_RFQ_LSN_Raised_By_Wait` | A raise (above) used a finalized, non-timed-out LSN wait target on a reader response. |

`PolarDB_Server_LSN_Updates_From_RFQ` is not the same as the monitor counter. RFQ updates count accepted current-group/current-epoch query completions carrying an LSN; monitor updates count monitor-observed LSN advances.

## 10. Reviewer Notes

- Result processing is success-only. A failed or aborted write must not advance the session RYW target.
- Successful Flush-delimited Execute results are not positioned until Sync
  supplies the backend RFQ. If that RFQ can no longer arrive, a deferred write
  marks the session write position unknown rather than reusing an older RFQ.
- A proxy-local single-variable RESET inside that open frame clears only the
  RESET statement's staged wait state. The deferred result/RFQ record remains
  frame-owned until Sync processes it or terminal failure abandons it.
- RESET cleanup preserves deferred RFQ state only while an active extended
  frame owns it. If no frame is active, deferred state is orphaned and is
  abandoned; an orphaned successful write makes the session write position
  unknown rather than allowing attribution to cross a later RESET boundary.
- Error resynchronization while the session remains in a
  `PROCESSING_STMT_*` state keeps semantic ErrorResponse ownership in the
  statement path. Standalone `RESYNCHRONIZING_CONNECTION` consumes only the
  backend synchronization result and leaves frontend RFQ publication to the
  session frame boundary. Mixing those roles can publish an early or duplicate
  RFQ.
- The session write and observed LSNs advance with `max()`, so they never regress.
- The session write and observed LSNs intentionally survive per-query cleanup and RESET-style operations; monotonic session reads are a client-session property, not a single-query property.
- A writer group or epoch mismatch makes the whole RFQ result cross-group or old-timeline data. Result processing must skip both session state and HGM cache publication from that result.
- Deferred confirmed-read targets are owned by the same writer group and epoch
  as their result attribution. If one Flush cycle spans different scopes, the
  aggregate target is discarded and the client RFQ uses only the current
  backend payload; numeric LSN maxima are never compared across timelines.
- Accepted result processing scopes the session LSN state to the request writer group+epoch before attaching new state. If the session already carried unscoped, cross-group, or old-epoch targets/flags, `polardb_process_result()` clears them immediately and increments `PolarDB_Session_Target_Epoch_Reset` only when it discarded real state.
- Missing RFQ LSN is handled at query/result-processing/policy level. Startup parameter acceptance alone does not show whether the backend will return an RFQ LSN.
- `SESSION_LSN` derives its target only from this session's positioned write and observed RFQs; an empty session has no target and its first positioned reader RFQ establishes one. `GLOBAL_LSN` independently raises that target to the current group LSN. Missing request-attributed RFQ evidence sets the corresponding unknown flag; result processing does not invent a replacement target.

Verified against this branch.
