# PolarDB LSN Session-Consistency - Documentation Set

> Scope: the index for the PolarDB read-your-writes (LSN-only) documentation set, document map, conventions, glossary, and authority rules. | Audience: R (PR reviewer) / M (maintainer) / O (operator) / C (contributor) | Status: stable | Prereqs: none | Verified against: this branch

This directory documents the PolarDB LSN-only read-your-writes (RYW) feature in this checkout, including the RFQ routing model. The code in the current branch is the authority for implemented behavior. Future-feature docs refer to the full implementation only for design comparison; those references are directional and are not line-compatible with this branch.

## 1. Feature Summary

ProxySQL can split PostgreSQL traffic between a writer and one or more readers. A simple split can break read-your-writes consistency: a client writes on the writer, then reads from a reader that has not replayed the write yet. PolarDB exposes the backend WAL LSN on ReadyForQuery (RFQ). ProxySQL requests RFQ LSN payloads through a startup profile, confirms usable LSNs only when result RFQs actually carry them, tracks the session's write and observed LSNs monotonically, and prefixes protected replica reads with a wait target so the reader waits until it reaches that LSN before answering.

This implementation is intentionally LSN-only. It covers autocommit simple-query reads and retries two safe reader-side failure cases before any user result is sent: a strict LSN wait-timeout ERROR and a lost reader connection. It does not implement CSN/global consistency, in-transaction split reads, general reader-failure policy, or extended-protocol wait wrapping. Those topics are documented as future work in docs 18-21.

The runtime shape is:

```
collect -> plan -> execute -> process_result
```

- `collect`: read query, session, hostgroup, and thread-local policy inputs.
- `plan`: decide passthrough, reader with wait, or writer.
- `execute`: select the target hostgroup and prepare the wait wrapper if needed.
- `process_result`: read the backend RFQ LSN after success, update session write/observed LSN state, clear or set missing-LSN latches, and refresh the per-server LSN cache.

## 2. Document Map

| Doc | Purpose | Scope |
|---|---|---|
| [POLARDB_ARCHITECTURE.md](POLARDB_ARCHITECTURE.md) | Whole-feature overview: components, hooks, pipeline, diagrams, config, observability, build, and status matrix. | this feature |
| [POLARDB_STRUCTURES.md](POLARDB_STRUCTURES.md) | Every PolarDB struct, enum, field, and counter with ownership and lifecycle. | this feature |
| [POLARDB_STRUCTURE_DOMAIN_MAP.md](POLARDB_STRUCTURE_DOMAIN_MAP.md) | Relationship map for PolarDB structs: ownership roots, shared value types, per-query pipeline, suffix vocabulary, and normalization audit. | this feature |
| [01-BACKGROUND-AND-DESIGN.md](01-BACKGROUND-AND-DESIGN.md) | RYW problem statement, LSN mechanism, design principles, and glossary. | this feature |
| [02-BUILD-TOGGLE-AND-LIBPQ.md](02-BUILD-TOGGLE-AND-LIBPQ.md) | `POLARDB_PROXY`, no-op stubs, and the libpq RFQ-LSN patch. | this feature |
| [03-TYPES-AND-ENUMS.md](03-TYPES-AND-ENUMS.md) | Types, enums, constants, values, and wire mapping. | this feature |
| [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md) | Admin schema, thread-local knobs, RFQ startup policy, policy resolution, and load/persist path. | this feature |
| [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md) | Monitor LSN feed, HGM topology maps, per-server LSN cache, lag gates, and locking model. | this feature |
| [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) | Request-side routing pipeline, decision matrix, RFQ policy, consistency-target reader preference, and `consistency_target_lsn` design. | this feature |
| [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md) | Simple-query wait wrapper, `dispatch_state` handoff, leading-result skip, and safety fences. | this feature |
| [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md) | Timeout modes, structured marker detection, exactly-once timeout accounting, and notice forwarding ownership. | this feature |
| [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) | RFQ-LSN result-processing path, session write/observed LSNs, per-server cache refresh, and missing-LSN latches. | this feature |
| [10-SESSION-INTEGRATION.md](10-SESSION-INTEGRATION.md) | `PgSQL_Session` fields, hook points, per-query reset, RESET CONNECTION, CHANGE_USER, and cleanup ordering. | this feature |
| [11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md) | `PgSQL_Connection` startup profiles, RFQ LSN accessor, notice receiver, `dispatch_state`, and wrap-state filter. | this feature |
| [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md) | Thread-local knobs and all PolarDB stats. | this feature |
| [13-QUERY-LIFECYCLE-AND-TRACES.md](13-QUERY-LIFECYCLE-AND-TRACES.md) | End-to-end lifecycle and worked traces. | this feature |
| [14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md) | RYW invariants, failure-mode catalogue, and known gaps. | this feature |
| [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md) | Deferred items, inactive knobs/counters, status matrix, and v2 path. | this feature plus roadmap |
| [16-TESTING-AND-VALIDATION.md](16-TESTING-AND-VALIDATION.md) | Committed test coverage and recommended external integration coverage. | this feature |
| [17-OPERATOR-GUIDE.md](17-OPERATOR-GUIDE.md) | Deployment, configuration, counters, and troubleshooting. | this feature |
| [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md) | CSN/global-consistency design sketch. | future |
| [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md) | In-transaction read offload design sketch. | future |
| [20-FUTURE-READER-FAILURE-RETRY-DESIGN.md](20-FUTURE-READER-FAILURE-RETRY-DESIGN.md) | Reader-failure recovery and retry design sketch. | future |
| [21-FUTURE-OTHER-CAPABILITIES.md](21-FUTURE-OTHER-CAPABILITIES.md) | Additional future capabilities and where they would plug into this feature. | future |

## 3. Audience Paths

| Audience | Recommended path |
|---|---|
| PR reviewer | `POLARDB_ARCHITECTURE.md`, `01`, `06`, `07`, `08`, `09`, `14`, `02`, `15` |
| Maintainer | `POLARDB_ARCHITECTURE.md`, `POLARDB_STRUCTURES.md`, `POLARDB_STRUCTURE_DOMAIN_MAP.md`, `10`, `11`, `05`, `03`, `12`, `13`, `16` |
| Operator | `17`, `04`, `12`, `POLARDB_ARCHITECTURE.md` sections 9-12, `15` |
| Contributor | `01`, `06`, `POLARDB_STRUCTURES.md`, `POLARDB_STRUCTURE_DOMAIN_MAP.md`, `15`, then future docs `18`-`21` |

## 4. Source Map

The feature is gated by `POLARDB_PROXY`. With the flag off, PolarDB declarations and call sites are compiled out and `lib/PgSQL_PolarDB_Stubs.cpp` remains an empty no-op translation unit.

| File | Role |
|---|---|
| `include/PgSQL_PolarDB.h` | PolarDB enums, constants, inline helpers, and per-query route/wait structs. |
| `include/PgSQL_PolarDB_Counters.h` | Ordered PolarDB counter metadata used by SQL stats export, thread counters, global counters, teardown folding, and Prometheus registration. |
| `lib/PgSQL_PolarDB.cpp` | Query classification and small protocol helpers. |
| `lib/PgSQL_PolarDB_Flow.cpp` | `collect`, `plan`, `execute`, and `process_result`. |
| `lib/PgSQL_PolarDB_Consistency.cpp` | Session consistency override and mode resolution helpers. |
| `lib/PgSQL_PolarDB_Wrap.cpp` | Wait-wrapper construction, timeout resolution, and timeout accounting helpers. |
| `lib/PgSQL_PolarDB_Notices.cpp` | Structured timeout notice handling and pending notice rescue. |
| `lib/PgSQL_PolarDB_Failure.cpp` | Narrow autocommit wait-read retry foundation. |
| `lib/PgSQL_PolarDB_Stubs.cpp` | Empty no-op stub file for `POLARDB_PROXY=0`. |
| `deps/postgresql/polardb_libpq.patch` | libpq patch that parses the RFQ LSN. Mandatory for RYW. |
| `scripts/verify-polardb-libpq-lsn-patch.sh` | Patch-apply/scope check for the libpq patch. |
| `test/polardb/test-c/libpq_lsn_test.c` | Direct-to-PolarDB libpq smoke test. |

## 5. Conventions

- Older headers may still record `Verified against: this branch`. For RFQ routing, treat the current branch as authoritative.
- Code citations refer to this branch unless explicitly tagged `(full implementation)`.
- Future docs are design references. They are not claims about shipped behavior in this feature.
- `polardb_active` is a boolean gate, not a stat counter.
- The current stat surface is 26 exported counters plus the non-exported `polardb_active` gate.
- `pgsql-polardb_lag_ms` is reserved in this feature. Runtime accepts only `0`; it has no routing effect until a real PgSQL millisecond-lag producer is added.
- `PolarDB_LSN_Stale_Count` is active for byte-lag enforcement: it increments when an enabled `max_lag_bytes` check cannot trust a primary or reader LSN sample. The millisecond-lag knob remains deferred.

## 6. Glossary

| Term | Definition |
|---|---|
| RYW | Read-your-writes consistency: after a client commits a write, later reads by that client see that write or newer data. |
| Writer | The PolarDB primary backend that accepts writes. |
| Reader | A PolarDB replica backend used for reads. |
| WAL LSN | PostgreSQL write-ahead-log position. Larger values represent later database progress. |
| RFQ | ReadyForQuery, the PostgreSQL server message sent when a command is complete. The PolarDB libpq patch reads an appended LSN from this message. |
| Wait wrapper | Three prepended `SET` statements before a simple-query read: mode, timeout, and target LSN. |
| Process result | Response-path step that reads RFQ LSN, updates session write/observed state, maintains missing-LSN latches, and refreshes per-server LSN state. |
| Startup profile | Connection-local record of which PolarDB proxy protocol and RFQ payload bits ProxySQL requested. It is not proof that the backend will return RFQ LSNs. |
| Session write LSN | The highest primary-sourced write RFQ LSN captured for this client session. It remains separate for diagnostics and future policy. |
| Session observed LSN | The highest RFQ LSN observed by this client session from any positioned RFQ. Protected reads wait on `max(write_lsn, observed_lsn)`. |
| Writer epoch | Per-writer-HG epoch that bumps when the sorted active writer identity set changes. Session LSN targets and missing-LSN latches are cleared when a session sees a different writer group or epoch. |
| Missing write LSN | A write completed but RFQ carried no LSN. The session marks `polardb_session_consistency.write_unknown`, increments `PolarDB_Write_Missing_LSN`, and routes later automatic LSN-mode reads according to `pgsql-polardb_route_rfq_policy`. |
| Missing observed LSN | A tracked SESSION_LSN read completed without RFQ LSN. The session marks `polardb_session_consistency.observed_unknown`, increments `PolarDB_Read_Missing_LSN`, and routes later automatic LSN-mode reads according to `pgsql-polardb_route_rfq_policy`. |
| best_effort | On finite wait timeout, return possibly stale rows and send a WARNING. |
| strict | On finite wait timeout, raise ERROR instead of serving stale data. |
| Manual route | A query rule or hint that directly chooses a hostgroup. Manual routes are authoritative and bypass RYW wrapping. |
| Extended protocol | PostgreSQL Parse/Bind/Execute flow. This feature does not wait-wrap it. Manual reader routes are honored; automatic extended reads without prior write LSN may use a reader; automatic extended reads after a known write/observed LSN target or unknown RFQ target use the writer. |

## 7. Authority and Caveats

- Current behavior is the code in this branch.
- Docs 18-21 intentionally describe future designs. They should not be used as evidence that those features ship in the current LSN-only branch.
- The integration TAP suites discussed in project review are not committed in this branch. Doc 16 distinguishes committed tests from recommended external integration coverage.
- No runtime test was executed while writing this doc set. The claims here are source-verified unless explicitly marked as external/manual coverage.

Verified against this branch.
