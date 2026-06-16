# 09 - Result Processing: RFQ-LSN Capture and Write Tracking

> Scope: the `polardb_process_result()` response path: read backend RFQ payloads, advance session write/observed LSNs, refresh per-server LSN cache, and maintain missing-LSN latches. | Audience: R/M/O/C | Status: stable | Prereqs: [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md), [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md), [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md), [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md) | Verified against: this branch

## 1. Purpose

Result processing is the response-side half of read-your-writes. After a query succeeds, ProxySQL reads the backend WAL LSN that libpq parsed from ReadyForQuery (RFQ). It uses that LSN in three ways:

1. For any positioned RFQ, advance this client session's observed LSN (`polardb_session_consistency.observed_lsn`).
2. If the query was a writer query, advance this client session's write LSN (`polardb_session_consistency.write_lsn`).
3. For any accepted current-group/current-epoch LSN-bearing RFQ, refresh the per-server LSN cache and increment `PolarDB_Server_LSN_Updates_From_RFQ`.

Result processing never chooses routing, never wraps SQL, and never issues a query. It reads already-parsed libpq state.

## 2. Call Site and Guards

`PgSQL_Session::polardb_process_result()` is defined in `lib/PgSQL_PolarDB_Flow.cpp`. It is called from `PgSQL_Session::RequestEnd()` on the success path only, and only when `polardb_config.is_polardb_enabled` is true.

The function returns immediately when PolarDB is disabled, the data stream is missing, or the backend connection is missing. That keeps non-PolarDB and failed-query paths out of the result-processing logic.

Before dispatch, the routing path captures the replication group's current writer hostgroup and writer epoch in `polardb_query.request_writer_scope`. LSN-bearing RFQs are accepted only by the direct HGM update path, which repeats the group+epoch check through the topology snapshot and updates only atomic per-server/primary-LSN cells. If that update path rejects the RFQ, `polardb_process_result()` skips all session LSN side effects. Missing-LSN RFQs repeat a current group+epoch check immediately before setting missing-LSN latches. Any accepted positioned or missing-LSN result first scopes the session LSN state to the request writer group+epoch, clearing old write/observed LSNs and unknown latches if the session was unscoped, belonged to another replication group, or belonged to an older writer epoch. These skips are trace-observable and deliberately do not add a new counter.

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
| write | yes | tag session with request writer group+epoch; after the direct HGM update accepts: `observed_lsn=max(old, lsn)` and `write_lsn=max(old, lsn)`; primary-sourced RFQ clears write/observed unknown latches | accepted update | `PolarDB_Server_LSN_Updates_From_RFQ++` |
| write | no | tag session with request writer group+epoch; leave old LSNs; set `polardb_session_consistency.write_unknown=true` | no update | `PolarDB_Write_Missing_LSN++`; one `proxy_warning` on first transition |
| read | yes | tag session with request writer group+epoch; after the direct HGM update accepts: `observed_lsn=max(old, lsn)`; replica RFQ does not clear unknown latches | accepted update | `PolarDB_Server_LSN_Updates_From_RFQ++` |
| tracked SESSION_LSN read | no | tag session with request writer group+epoch; leave old LSNs; set `polardb_session_consistency.observed_unknown=true` | no update | `PolarDB_Read_Missing_LSN++`; one `proxy_warning` on first transition |
| untracked read | no | unchanged | no update | trace only |

Writes are observations, but `polardb_session_consistency.write_lsn` is kept separately for diagnostics and future policy. The wait target for SESSION_LSN reads is `max(polardb_session_consistency.write_lsn, polardb_session_consistency.observed_lsn)`.

ProxySQL does not guess an LSN from monitor/global state. If a query that should preserve SESSION_LSN monotonicity completes without RFQ LSN, the matching unknown latch is set and later automatic LSN-mode reads route through `pgsql-polardb_route_rfq_policy`.

## 6. Missing LSN Flow

A missing RFQ LSN is abnormal for a correctly configured RFQ-requesting PolarDB deployment. It can indicate a plain PostgreSQL backend, an unpatched libpq, a backend that was not asked to send RFQ LSN, a stale pooled connection whose startup profile did not request RFQ LSN, or another deployment mismatch.

Current code handles it as follows:

1. In result processing, if `is_write && !has_lsn`, set `polardb_session_consistency.write_unknown=true`.
2. In result processing, if a tracked SESSION_LSN read has no RFQ LSN, set `polardb_session_consistency.observed_unknown=true`.
3. Increment `PolarDB_Write_Missing_LSN` or `PolarDB_Read_Missing_LSN` respectively.
4. Emit a `proxy_warning` only on the first transition from known to unknown, so logs are useful but not spammed per query.
5. In planning, if either latch is true, return the matching action reason (`WRITE_LSN_UNKNOWN` or `OBSERVED_LSN_UNKNOWN`) through `pgsql-polardb_route_rfq_policy`.
6. A later primary-sourced positioned RFQ clears both unknown latches. A replica-sourced RFQ advances observed/cache state but does not clear them.

Under `strict`, the policy preserves RYW by avoiding automatic replica reads when the exact wait target is unknown. Under `best_effort`, eligible simple-query reads can proceed without a wait, are accounted as degraded routes, and receive a WARNING `NoticeResponse` before the result. The proxy log for degraded routing is edge-limited per session while the degradation remains active, but the degraded-route counter and simple-query client notice are per route. Extended-protocol unknown-target reads force the writer in this implementation because there is no safe wait/notice wrapper for local Parse/Bind completions.

## 7. Per-Server Cache Update

For every RFQ with an LSN, read or write, `polardb_process_result()` first calls the direct server-pointer overload:

```cpp
PgHGM->polardb_update_server_lsn(backend_srv, backend_hg, backend_config, lsn, polardb_query.request_writer_scope)
```

The function is `polardb_update_server_lsn` (not `update_server_lsn`), the first argument is the `PgSQL_SrvC*` `backend_srv` (not `parent`), and the request identity is a single `const PolarDB_WriterScope& request_scope` argument (`polardb_query.request_writer_scope`) — not two separate `request_writer_hg`/`request_writer_epoch` scalars. The signature has five parameters, not six.

Result processing resolves `backend_config` once from the topology snapshot and passes it to that overload with the backend hostgroup plus the request writer scope (hostgroup+epoch). The overload performs the current group+epoch check, advances `PgSQL_SrvC::polardb_current_lsn`, and updates the snapshot's shared primary mirror cell without taking the global HGM lock or rereading HGC state. If the request identity is missing/stale/cross-group, the current epoch is missing, or the backend server pointer/hostgroup is unusable, the RFQ is rejected and result processing does not update session LSN state or the RFQ counter.

`PolarDB_Server_LSN_Updates_From_RFQ` increments only after this direct HGM update path accepts the RFQ as current-group/current-epoch data. Acceptance does not require the cached LSN to strictly increase; equal/older LSNs can still be accepted for session monotonicity while leaving the cache unchanged.

This is intentionally broader than write tracking. A reader RFQ LSN tells ProxySQL how far that reader has replayed, so it is useful for lag and freshness decisions even though it does not advance the client session's write target.

## 8. Counters Owned by This Stage

| Counter | Meaning |
|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ` | An RFQ carried an LSN and the direct HGM update path accepted it for the current writer group+epoch. Counts reads and writes, including accepted RFQs whose LSN did not strictly advance the cache. |
| `PolarDB_Write_Missing_LSN` | A writer query completed but RFQ carried no LSN. Later automatic LSN-mode reads in that session follow `route_rfq_policy` until a primary-sourced RFQ clears the latch. |
| `PolarDB_Read_Missing_LSN` | A tracked SESSION_LSN read completed but RFQ carried no LSN. Later automatic LSN-mode reads follow `route_rfq_policy` until a primary-sourced RFQ clears the latch. |

`PolarDB_Server_LSN_Updates_From_RFQ` is not the same as the monitor counter. RFQ updates count accepted current-group/current-epoch query completions carrying an LSN; monitor updates count monitor-observed LSN advances.

## 9. Reviewer Notes

- Result processing is success-only. A failed or aborted write must not advance the session RYW target.
- The session write and observed LSNs advance with `max()`, so they never regress.
- The session write and observed LSNs intentionally survive per-query cleanup and RESET-style operations; monotonic session reads are a client-session property, not a single-query property.
- A writer group or epoch mismatch makes the whole RFQ result cross-group or old-timeline data. Result processing must skip both session state and HGM cache publication from that result.
- Accepted result processing scopes the session LSN state to the request writer group+epoch before attaching new state. If the session already carried unscoped, cross-group, or old-epoch targets/latches, `polardb_process_result()` clears them immediately and increments `PolarDB_Session_Target_Epoch_Reset` only when it discarded real state.
- Missing RFQ LSN is handled at query/result-processing/policy level. Startup parameter acceptance alone is not proof the backend will return RFQ LSN.
- The wait target is derived from this session's write/observed RFQs, or from the primary mirror only when `pgsql-polardb_session_lsn_baseline=primary` is configured for an empty session. The result-processing path does not substitute monitor LSN or global LSN after a missing RFQ.

Verified against this branch.
