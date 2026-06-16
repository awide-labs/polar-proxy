# PolarDB LSN Session-Consistency — Architecture

> Scope: the whole PolarDB read-your-writes (LSN-only) feature — components, the four integration hooks, the collect/plan/execute/process_result pipeline, query-flow diagrams, config, observability, build, and the implemented-vs-deferred status matrix. | Audience: R (PR reviewer) / M (maintainer) / O (operator) / C (contributor) | Status: stable | Prereqs: read this first; it links down to the numbered deep-dive docs (`01-BACKGROUND-AND-DESIGN.md` … `17-OPERATOR-GUIDE.md`) and to `POLARDB_STRUCTURES.md`. | Verified against: this branch

This is the main entry point for the PolarDB documentation set. It gives the complete picture of the feature in one place and links **down** into the numbered deep-dive docs for full detail. It does not repeat their depth.

---

## Table of contents

1. [What this is / scope / non-goals](#1-what-this-is--scope--non-goals)
2. [Background in one page](#2-background-in-one-page)
3. [High-level architecture](#3-high-level-architecture)
4. [Domain map and component responsibilities](#4-domain-map-and-component-responsibilities)
5. [The four integration hooks](#5-the-four-integration-hooks)
6. [The collect → plan → execute → process_result pipeline](#6-the-collect--plan--execute--process_result-pipeline)
7. [Query-flow diagrams](#7-query-flow-diagrams)
8. [Configuration at a glance](#8-configuration-at-a-glance)
9. [Observability at a glance](#9-observability-at-a-glance)
10. [Build and compatibility](#10-build-and-compatibility)
11. [Correctness summary](#11-correctness-summary)
12. [Status: implemented vs deferred](#12-status-implemented-vs-deferred)
13. [Source map and line counts](#13-source-map-and-line-counts)
14. [Glossary (quick reference)](#14-glossary-quick-reference)
15. [Document cross-reference index](#15-document-cross-reference-index)
16. [Appendix: Mermaid diagrams](#appendix-mermaid-diagrams)

---

## 1. What this is / scope / non-goals

### What this feature does

ProxySQL routes client queries to backend servers. For PostgreSQL, ProxySQL can split traffic: send writes to a **primary** (the writer node) and reads to **replicas** (reader nodes). Replicas can lag behind the primary, so a naive split breaks **read-your-writes (RYW)**: a client that just wrote data might read a replica that has not yet caught up and see stale data.

This feature adds RYW consistency for **PolarDB** (an Alibaba PostgreSQL-compatible database with one primary and read replicas). It works like this:

1. After a write, ProxySQL records the write's position in the write-ahead log — the **LSN (Log Sequence Number)**, a 64-bit number that grows with every write.
2. On a later read that is allowed to go to a replica, ProxySQL tells the replica: "do not answer until you have replayed past LSN X." It does this by prefixing the read with `SET` statements (the **wait wrapper**). The replica blocks until it has caught up, then runs the read.
3. ProxySQL learns the LSN with no extra query: a patched libpq reads it from the **ReadyForQuery (RFQ)** message the backend already sends after every command.

The result: the client's own reads always see its own writes, even when those reads run on a replica.

### Scope (what this feature ships)

| In scope | Notes |
|---|---|
| LSN-based session RYW | The session's own writes are visible to its own later reads. |
| Autocommit reads only | Only reads outside an explicit transaction are eligible for a replica. |
| Simple-query protocol only | Reads sent with the PostgreSQL extended protocol (Parse/Bind/Execute) are not wrapped. |
| `best_effort` and `strict` timeout modes | On a wait timeout, `best_effort` serves stale data with a WARNING; `strict` raises an ERROR. |
| Byte-lag safety cap | An optional cap that keeps reads off a replica that is too far behind. |
| Compile-time isolation | The whole feature is behind one build flag, `POLARDB_PROXY`. |

### Non-goals (explicitly NOT in this feature)

These are described, framed as future work, in `15-LIMITATIONS-AND-ROADMAP.md` and the future-design docs `18-FUTURE-CSN-DESIGN.md` … `21-FUTURE-OTHER-CAPABILITIES.md`. They describe the **full implementation**, not this branch.

| Non-goal | Why out of scope here |
|---|---|
| **CSN (Commit Sequence Number) / global consistency** | A different consistency unit (commit counter, not WAL byte position) for cross-session consistency. **Experimental and incomplete** even in the full implementation: it requires PolarDB backend support, applies only in global-consistency mode, and its wait behavior is not reliably verified. Not present in this branch at all. |
| **Transaction-split read offload** | Offloading reads issued *inside* an open transaction to a replica. Needs a state machine and a second backend connection. Not present (`PgSQL_PolarDB_Split.cpp` exists only in the full implementation). |
| **General reader-failure recovery / retry** | RETRY / FORWARD / writer-loss handling when a replica fails mid-read inside a transaction. Not present. This branch has only the narrow autocommit wait-read retry foundation in `PgSQL_PolarDB_Failure.cpp`. |
| **Extended-protocol RYW** | Wrapping Parse/Bind/Execute reads. Such reads are routed to the writer instead of running as unprotected replica reads. |
| **Millisecond lag cap** | A time-based (vs byte-based) lag gate. The knob exists but is inert — see [§12](#12-status-implemented-vs-deferred). |

---

## 2. Background in one page

Full detail is in `01-BACKGROUND-AND-DESIGN.md`. The short version:

**The RYW problem.** Read/write splitting sends writes to the primary and reads to replicas to spread load. Replicas apply the primary's changes asynchronously, so a replica is usually slightly behind. If a client writes a row and then immediately reads it from a replica, the replica may not have that row yet. The client "loses its own write." That is the bug this feature prevents.

**PolarDB's mechanism.** PolarDB exposes two things ProxySQL uses:

- It can **append the current WAL LSN to every ReadyForQuery (RFQ) message**. With a patched libpq, ProxySQL reads that LSN for free after each query (`lib/PgSQL_Connection.cpp:1349-1359`). No extra round-trip.
- It honors a server setting, `polar_xact_split_wait_lsn`. When a read on a replica is preceded by `SET polar_xact_split_wait_lsn = '<target>'`, the replica blocks until it has replayed up to that LSN before running the read.

**The wait-on-replica gate.** ProxySQL records each session's highest write LSN (`polardb_session_consistency.write_lsn`, `include/PgSQL_Session.h:505`). When that session next issues a replica-eligible read, ProxySQL prefixes the read with the `SET` that makes the replica wait for that LSN. The wait — not any LSN comparison inside ProxySQL — is the actual consistency guarantee. Any LSN check ProxySQL does on its own is only a **safety** filter (see [§11](#11-correctness-summary)).

**Two timeout modes.** The replica's wait can time out (it never catches up in time). `polar_consistency_mode` chooses the outcome:

- `best_effort` — the replica returns possibly-stale rows and emits a WARNING. The read still succeeds.
- `strict` — the replica raises an ERROR and the read fails.

---

## 3. High-level architecture

The feature sits inside the normal PostgreSQL session path. The PolarDB code only runs when a PolarDB hostgroup is configured; otherwise a single atomic check (`PgHGM->status.polardb_active`) skips all of it.

A **hostgroup (HG)** is a numbered group of backend servers. A PolarDB pair links one writer HG and one reader HG.

```
                          ┌───────────────────────── PgSQL_Session (one per client) ──────────────────────────┐
                          │                                                                                   │
   client ──('Q' query)──►│  get_pkts_from_client()                                                           │
                          │     │                                                                             │
                          │     │  if (PgHGM->status.polardb_active)   ◄── fast bypass when no PolarDB HG     │
                          │     ▼                                          (Session.cpp:2543)                 │
                          │  ┌─────────── PolarDB route pipeline (HOOK 2) ───────────┐                        │
                          │  │ collect  → plan      → execute                        │   Flow.cpp:233/410/658  │
                          │  │ (snapshot) (decide)    (set current_hostgroup,        │                        │
                          │  │                         prepare wait state)           │                        │
                          │  └───────────────────────────────────────────────────────┘                        │
                          │     │                                                                             │
                          │     ▼  find_or_create_backend(current_hostgroup)                                  │
                          │  ┌──────────── backend attach (HOOK 1) ────────────┐                              │
                          │  │ fresh connect: profile-driven proxy startup +   │  Connection.cpp              │
                          │  │   PQsetPolarSendLSN(conn,1)                     │  Connection.cpp:1335         │
                          │  │ pooled/fresh: is_polardb_enabled = true         │  Session.cpp:5696/5710       │
                          │  └─────────────────────────────────────────────────┘                              │
                          │     │                                                                             │
                          │     ▼  ASYNC_IDLE: finalize_wait_timeout_injection()  (build wrapper)             │
                          │        (single wrap point, Session.cpp:3607 → Wrap.cpp:303)                       │
                          │     │                                                                             │
                          └─────┼─────────────────────────────────────────────────────────────────────────────┘
                                │
                                ▼  backend runs:  SET mode ; SET timeout ; SET wait_lsn ; <user read>
                          ┌──────────── PgSQL_Connection (backend, HOOK 3) ─────────────┐
                          │ WIRE filter: drop the 3 SET results, forward only the user  │  Connection.cpp:564-592
                          │ result. notice hook captures best_effort timeout WARNING.   │  Connection.cpp:2558-2563
                          └─────────────────────────────────────────────────────────────┘
                                │
                                ▼  RequestEnd() success (HOOK 4)
                          ┌────────── process_result ───────┐
                          │ read RFQ LSN, advance           │  Session.cpp:6098-6105 →
                          │ polardb_session_consistency.write_lsn       │
                          └─────────────────────────────────┘
                                │
                                ▼  result (+ any captured notice) returned to client
```

Cross-thread state (per-server LSN cache, topology maps, counters) lives on the **HostGroups Manager (HGM)**, the single object `PgHGM` that owns backend topology. Everything else (session fields, connection fields) is driven by one thread at a time and needs no locking. `POLARDB_STRUCTURES.md` is the full ownership and thread-safety reference; `POLARDB_STRUCTURE_DOMAIN_MAP.md` is the expanded map of how the structs compose and flow through the request path.

---

## 4. Domain map and component responsibilities

This section maps the feature two ways. First as **domains** — subsystems grouped by what they are responsible for. Then as the **files** that implement them. The domain view is the better way to understand the design; the file view is the better way to find code. They describe the same feature at two zoom levels.

### The domains (subsystems)

The LSN-only feature is nine domains. Six run inside one client session and are driven by a single thread at a time, so they need no locking. One domain (Monitor & HGM LSN state) is shared across threads and is the only place that uses atomics and a lock. Two domains (admin schema, build) are set up once and then only read.

```
┌──────────── PgSQL_Session  (one client; one driving thread; no locks) ─────────────┐
│                                                                                    │
│  REQUEST PATH                                              RESPONSE PATH           │
│  ┌─────────────────────┐   ┌──────────────────┐    ┌──────────────────┐            │
│  │ 1 Routing pipeline  │──►│ 2 Query wrapping │──► │ 3 Wait & notices │            │
│  │   (consistency      │   │   single wrap pt │    │   timeout mode,  │            │
│  │    planner)         │   │   build SETs +   │    │   capture/forward│            │
│  │   collect→plan→     │   │   inject; latency│    │   the NOTICE     │            │
│  │   execute; matrix;  │   │   doc 07         │    │   doc 08         │            │
│  │   lag cap; doc 06   │   └──────────────────┘    └──────────────────┘            │
│  └─────────┬───────────┘                                     │                     │
│            │                                       ┌──────────▼───────────┐        │
│            │                                       │ 4 Process result &   │        │
│            │                                       │   write tracking:    │        │
│            │                                       │   read RFQ           │        │
│            │                                       │   LSN, advance       │        │
│            │                                       │   session consistency│        │
│            │                                       │   LSN state          │        │
│            │                                       │   doc 09             │        │
│            │                                       └──────────────────────┘        │
│  ┌─────────▼──────────────────────────────────────────────────────────────────┐    │
│  │ 5 Session orchestration — the four hooks, the per-query reset, all call    │    │
│  │   sites that drive domains 1–4.  doc 10                                    │    │
│  └────────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────┬──────────────────────────────────────────┘
                                          │
   ┌─────────────── 6 Connection & libpq  (backend side, per session) ───────────────┐
   │ startup profile • PQsetPolarSendLSN • inline SET filter • get RFQ LSN           │
   │ doc 11                                                                          │
   └─────────────────────────────────────────┬───────────────────────────────────────┘
                                             │
 ══════════════════ shared across threads (PgHGM): atomics + one rwlock ══════════════════
   ┌─────────────────────────── 7 Monitor & HGM LSN state ───────────────────────────────┐
   │ polardb_active condition • per-server LSN cache • topology maps • lag inputs • counters  │
   │ doc 05                                                                              │
   └─────────────────────────────────────────────────────────────────────────────────────┘
                                              │
 ─────────────────────────── set up once, then read ───────────────────────────
   ┌────────── 8 Admin schema & policy (doc 04) ──────────┐  ┌──── 9 Build & compatibility (doc 02) ────┐
   │ pgsql_replication_hostgroups columns • three-tier    │  │ POLARDB_PROXY toggle • empty stub TU •   │
   │ config resolution • thread-vars • load/save/sync     │  │ libpq RFQ-LSN patch • byte-equivalence   │
   └──────────────────────────────────────────────────────┘  └──────────────────────────────────────────┘
```

| # | Domain (subsystem) | What it owns | Main files | Thread model | Deep-dive |
|---|---|---|---|---|---|
| 1 | Routing pipeline (consistency planner) | The per-query decision: gather inputs, plan the action, apply it. The decision matrix, the safe writer-fallback rules, the lag cap. | `lib/PgSQL_PolarDB_Flow.cpp`, `lib/PgSQL_PolarDB_Consistency.cpp`, inline helpers in `include/PgSQL_PolarDB.h` | per-session (no locks) | `06-ROUTING-PIPELINE.md` |
| 2 | Query wrapping | Build and inject the wait-wrapper SQL once (the single wrap point). Wait-latency accounting. RESET cleanup. | `lib/PgSQL_PolarDB_Wrap.cpp` | per-session | `07-QUERY-WRAPPING.md` |
| 3 | Wait-timeout & notices | best_effort vs strict timeout behavior. Structured timeout-marker detection. Capture the timeout NOTICE and forward it ahead of the result. | `lib/PgSQL_PolarDB_Notices.cpp` | per-session | `08-WAIT-TIMEOUT-AND-NOTICES.md` |
| 4 | Result processing & write tracking | Read the backend RFQ LSN once. Advance `polardb_session_consistency.write_lsn` on a write. Refresh the per-server LSN cache. The write/read heuristic. | `lib/PgSQL_PolarDB_Flow.cpp` (polardb_process_result), `lib/PgSQL_PolarDB.cpp` | per-session | `09-PUBLISH-AND-WRITE-TRACKING.md` |
| 5 | Session orchestration | The four hook points, the unconditional per-query reset, and every call site into domains 1–4. | `lib/PgSQL_Session.cpp`, `include/PgSQL_Session.h` | per-session | `10-SESSION-INTEGRATION.md` |
| 6 | Connection & libpq | Ask the backend to report its LSN (conninfo + `PQsetPolarSendLSN`). The inline SET-result filter. Read the RFQ LSN. | `lib/PgSQL_Connection.cpp`, `include/PgSQL_Connection.h`, the libpq patch | per-session (backend side) | `11-CONNECTION-AND-LIBPQ.md` |
| 7 | Monitor & HGM LSN state | The `polardb_active` gate. The per-server LSN cache. Topology maps. Lag inputs. The stat counters. | `lib/PgSQL_HostGroups_Manager.cpp`, `include/PgSQL_HostGroups_Manager.h`, `lib/PgSQL_Monitor.cpp` | **shared** (atomics + one rwlock) | `05-MONITOR-AND-HGM-LSN-STATE.md` |
| 8 | Admin schema & policy | The `pgsql_replication_hostgroups` columns. Three-tier config resolution. The thread-vars. Load / save / sync / disk-upgrade. | `lib/ProxySQL_Admin.cpp`, `include/ProxySQL_Admin_Tables_Definitions.h`, `include/proxysql_structs.h`, `lib/PgSQL_Thread.cpp`, `ProxySQL_Cluster/Config/Disk_Upgrade` | process-wide (set once) | `04-ADMIN-SCHEMA-AND-CONFIG.md` |
| 9 | Build & compatibility | The `POLARDB_PROXY` toggle. The empty stub TU. The libpq RFQ-LSN patch. Both-tier byte-equivalence. | `Makefile`s, `lib/PgSQL_PolarDB_Stubs.cpp`, `deps/postgresql/polardb_libpq.patch`, guards in `include/PgSQL_PolarDB.h` | build / link time | `02-BUILD-TOGGLE-AND-LIBPQ.md` |

### How the domains connect

- **A read flows through the per-session domains in order.** Session orchestration (5) calls the routing pipeline (1). For a replica-with-wait read, the pipeline hands wait intent to query wrapping (2); the connection (6) sends the wrapped query and filters out the prepended SET results; wait & notices (3) handle a best_effort timeout; result processing (4) records the result LSN. Orchestration (5) ties them together at exactly four hooks (see section 5).
- **Only one domain is shared across threads.** Monitor & HGM LSN state (7) lives on the single `PgHGM` object and is read and written by many threads, so it is the only domain that uses atomics and a lock. Every per-session domain (1–6) is driven by one thread at a time and needs no locking.
- **The boundary back into shared state is narrow.** The per-session domains *read* topology and cached LSNs from HGM (7); the only per-session domain that *writes* back into shared state is result processing (4), refreshing the per-server LSN cache. Keeping that boundary small is what makes the feature safe to reason about.
- **Two domains are foundations.** Admin schema & policy (8) and Build & compatibility (9) are set up once — at config load and at compile time — and then only read by the domains above.
- **The full implementation has more domains.** The complete PolarDB implementation adds a transaction-split engine and a connection-warmup path, and a heavier notices/health surface. None of those are in the LSN-only tree; they are described as future work in [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md) and docs 18–21.

### The 9 dedicated files

The feature is **9 dedicated files** plus `#if POLARDB_PROXY` blocks in a set of core files. Line counts should be treated as current-branch review aids, not stable API.

| File | Lines | One-line responsibility |
|---|---:|---|
| `include/PgSQL_PolarDB.h` | 1926 | All PolarDB types, enums, structs, the `PolarDB_Protocol` static-helper class, the header-inline deterministic helpers (`PolarDB_Query_WaitPlan::build_consistency`, `polardb_resolve_*`, lag-cap predicates), and the `POLARDB_TRACE` macro. |
| `include/PgSQL_PolarDB_Counters.h` | 127 | The shared counter-metadata X-macro (`POLARDB_COUNTER_LIST`): the single source of truth for the 26 exported counters (19 thread-backed `T(...)` + 7 global-only `G(...)`), driving the SQL stats export, the per-thread counter slots, teardown folding, and Prometheus registration. |
| `lib/PgSQL_PolarDB.cpp` | 111 | PolarDB health-check parse (`parse_polardb_full_health_check`, delegating to `PolarDB_Protocol::parse_node_type` / `parse_is_available` / `parse_lsn_string`) and the write-vs-read query heuristic `PolarDB_Protocol::is_write_query`. |
| `lib/PgSQL_PolarDB_Flow.cpp` | 1030 | The four pipeline stages `polardb_collect` / `polardb_plan` / `polardb_execute` / `polardb_process_result`, plus `polardb_reader_lag_plan()`, which attaches byte-lag inputs to the per-query reader plan. |
| `lib/PgSQL_PolarDB_Consistency.cpp` | 62 | Thin helpers: `polardb_resolve_wait_timeout_ms`, `polardb_set_session_override`, `is_polardb_hostgroup` (delegates to HGM). |
| `lib/PgSQL_PolarDB_Wrap.cpp` | 449 | Builds and installs the wait wrapper SQL (`finalize_wait_timeout_injection`, the single wrap point), plus wait-latency/timeout accounting and RESET cleanup. |
| `lib/PgSQL_PolarDB_Notices.cpp` | 293 | Captures the `best_effort` wait-timeout WARNING/NOTICE and queues it to be forwarded ahead of the user's result. |
| `lib/PgSQL_PolarDB_Failure.cpp` | 322 | The narrow autocommit wait-read retry foundation: capture a wait-wrapped read failure (`polardb_capture_wait_read_failure`), retry the original read on the writer after a strict LSN-wait timeout or reader connection loss (`polardb_retry_wait_read_on_writer`), and redirect to the writer HG (`polardb_redirect_to_writer`). |
| `lib/PgSQL_PolarDB_Stubs.cpp` | 34 | Intentionally empty under `#if !POLARDB_PROXY`. Every PolarDB declaration and call site is guarded, so a `POLARDB_PROXY=0` build needs no link-time stub today (`PgSQL_PolarDB_Stubs.cpp:10-23`). |

All seven `.cpp` files are registered in `lib/Makefile`. Deep-dive: `03-TYPES-AND-ENUMS.md` (types), `06-ROUTING-PIPELINE.md` (Flow), `07-QUERY-WRAPPING.md` (Wrap), `08-WAIT-TIMEOUT-AND-NOTICES.md` (Notices), `02-BUILD-TOGGLE-AND-LIBPQ.md` (Stubs).

### Core files carrying `#if POLARDB_PROXY` blocks (integration points)

| File | Carries |
|---|---|
| `include/PgSQL_Session.h` | PolarDB session members + method declarations (`:476-534`). |
| `include/PgSQL_Connection.h` | Per-connection wrap-state + LSN members and the documented wire-sequence comment. |
| `include/PgSQL_HostGroups_Manager.h` | `status.polardb_active` gate, per-HG and per-server LSN cache, the 26 stat counters. |
| `include/PgSQL_Query_Processor.h` | `replica_eligible` and `force_primary_hint` query-rule/QPO fields. |
| `include/ProxySQL_Admin_Tables_Definitions.h` | Schema for `pgsql_replication_hostgroups` (the `polardb` `check_type` and LSN columns) at `:308`. |
| `include/proxysql_structs.h` | The twelve `pgsql_thread___polardb_*` thread-local config variables. |
| `lib/PgSQL_Connection.cpp` | **Hooks 1 + 3** — conninfo, `is_polardb_enabled`, LSN parsing enable/read, WIRE filter, notice hook. |
| `lib/PgSQL_Session.cpp` | **Hooks 2 + 4** — route pipeline call site, pooled/fresh enable, wrap finalize, result processing. |
| `lib/PgSQL_HostGroups_Manager.cpp` | Topology maps, per-server LSN cache, `polardb_update_server_lsn`, `polardb_active` set at `:1864`. |
| `lib/PgSQL_Monitor.cpp` | `polardb` `check_type` health probe + monitor-driven per-server LSN cache updates. |
| `lib/PgSQL_Query_Processor.cpp` | Propagates `replica_eligible` through query-rule match/apply/log. |
| `lib/PgSQL_Thread.cpp` | Declares / initializes / get / set the `pgsql-polardb_*` config variables. |
| `lib/ProxySQL_Admin.cpp`, `ProxySQL_Admin_Disk_Upgrade.cpp`, `ProxySQL_Cluster.cpp`, `ProxySQL_Config.cpp` | Load/save/sync/disk-upgrade of the PolarDB columns. |
| `src/Makefile`, `lib/Makefile`, `Makefile` | The `-DPOLARDB_PROXY` flag and helper targets. |

Deep-dives: `10-SESSION-INTEGRATION.md`, `11-CONNECTION-AND-LIBPQ.md`, `05-MONITOR-AND-HGM-LSN-STATE.md`, `04-ADMIN-SCHEMA-AND-CONFIG.md`, `12-THREADVARS-AND-OBSERVABILITY.md`.

---

## 5. The four integration hooks

The whole feature attaches to the normal session path at exactly four points. This is the most useful map for a reviewer to start from.

```
client write ──► [HOOK 1: connect / enable + request LSN reporting]
                       │
client read  ──► [HOOK 2: route pipeline]  collect → plan → execute   (in get_pkts_from_client)
                       │  (REPLICA_WITH_WAIT saves intent only; wrapper built later)
                       ▼
              ASYNC_IDLE: finalize_wait_timeout_injection (build wrapper packet, single point)
                       ▼  backend runs:  SET mode ; SET timeout ; SET wait_lsn ; <user read>
              [HOOK 3: WIRE filter] drop the 3 SET results, keep the user result
                                    + notice hook forwards the best_effort timeout WARNING
                       ▼
              [HOOK 4: RequestEnd process_result] read RFQ LSN, advance session write/observed LSNs
```

### HOOK 1 — connect / enable

Marks the session as PolarDB-enabled and asks the backend to report its LSN. Three sites, because a connection can be fresh or reused from the pool:

| Site | What it does | file:line |
|---|---|---|
| Fresh-connect startup profile | When the backend HG is a PolarDB HG, resolve per-HG/global `proxy_protocol`, emit v15, legacy, or no PolarDB proxy startup params, record the startup profile, and set `is_polardb_enabled = true`. | `lib/PgSQL_Connection.cpp` |
| After a successful connect | `polardb_init_connection_tracking()` calls `PQsetPolarSendLSN(conn, 1)` so the patched libpq parses the LSN PolarDB appends to RFQ. | `lib/PgSQL_Connection.cpp:1325` (function definition); `:1335` (the `PQsetPolarSendLSN` call). |
| Pooled or fresh, in the session | A reused pooled connection never runs `connect`, so the session enable flag is also set when getting/creating a backend for a PolarDB HG. | `lib/PgSQL_Session.cpp:5696-5697` (pooled), `:5710-5711` (fresh). |

Detail: `11-CONNECTION-AND-LIBPQ.md`, `02-BUILD-TOGGLE-AND-LIBPQ.md`.

### HOOK 2 — route pipeline

In `PgSQL_Session::get_pkts_from_client()`, gated by `PgHGM->status.polardb_active`. It unconditionally resets the per-query LSN target, skips the pipeline for manual routing, then runs `collect → plan → execute` and overwrites `current_hostgroup`.

| Step | file:line |
|---|---|
| Per-query reset of `polardb_query.reader_plan.consistency_target_lsn` | `lib/PgSQL_Session.cpp:2532` |
| Gate on `polardb_active` | `lib/PgSQL_Session.cpp:2543` |
| Manual-mode skip (`replica_eligible == -1` and a rule set a destination HG) | `lib/PgSQL_Session.cpp:2546-2551` |
| `polardb_collect(...)` | `lib/PgSQL_Session.cpp:2554` |
| `polardb_plan(...)` | `lib/PgSQL_Session.cpp:2556` |
| `polardb_execute(...)` → `current_hostgroup` | `lib/PgSQL_Session.cpp:2558-2559` |
| PASSTHROUGH with an explicit target HG | `lib/PgSQL_Session.cpp:2560-2564` |

Detail: `06-ROUTING-PIPELINE.md`.

### HOOK 3 — WIRE filter + wait/notice

Three coordinated sites on the backend connection:

| Site | What it does | file:line |
|---|---|---|
| Wrapper build/install | At `ASYNC_IDLE`, `finalize_wait_timeout_injection(myconn, myds)` builds the multi-statement wait wrapper exactly once and replaces the `'Q'` packet; on FAILED it sends a clean error and ends the request instead of sending an unwrapped replica read. | `lib/PgSQL_Session.cpp:3590-3604` (call at `:3595`). |
| WIRE result filter | In `PgSQL_Connection::handler()`, each prepended `SET` result (`PGRES_COMMAND_OK`/`PGRES_EMPTY_QUERY`) is silently consumed and its buffer recycled; only the (N+1)th result — the user's read — reaches the client. A wrapper `SET` ERROR (e.g. a strict-mode timeout) is accounted, then flows through normally. | `lib/PgSQL_Connection.cpp:564-592`. |
| Notice forwarding | `notice_handler_cb` always calls `polardb_handle_notice(conn, result)` so a `best_effort` wait-timeout WARNING is accounted and re-queued to be sent ahead of the user result — even when the generic result was already rotated out. | `lib/PgSQL_Connection.cpp:2558-2563` → `lib/PgSQL_PolarDB_Notices.cpp:97`. |

Detail: `07-QUERY-WRAPPING.md`, `08-WAIT-TIMEOUT-AND-NOTICES.md`.

### HOOK 4 — RequestEnd / process_result

In `PgSQL_Session::RequestEnd()` on the success path (`called_on_failure == false`), when `is_polardb_enabled`, call `polardb_process_result(myds, query_digest_text)`. It reads the RFQ LSN (no extra round-trip) and, on a write that carried an LSN, advances `polardb_session_consistency.write_lsn`.

| Step | file:line |
|---|---|
| Guard + call | `lib/PgSQL_Session.cpp` (call at `:6103`). |

Detail: `09-PUBLISH-AND-WRITE-TRACKING.md`.

---

## 6. The collect → plan → execute → process_result pipeline

This is the core of the feature. There are four stages, all methods of `PgSQL_Session`, all in `lib/PgSQL_PolarDB_Flow.cpp`. The first three run on the request path before the backend is chosen; the fourth runs on the response path.

| Stage | Entry point | Side effects? | One-line job |
|---|---|---|---|
| collect | `polardb_collect()` `Flow.cpp:233` | May clear stale session LSN state after writer-epoch change | Snapshot every routing input into `PolarDB_Query_RouteCtx`. |
| plan | `polardb_plan()` `Flow.cpp:410` | None (reads HGM only for the lag cap) | Decide PASSTHROUGH / FORCE_PRIMARY / REPLICA_WITH_WAIT into `PolarDB_Query_RoutePlan`. |
| execute | `polardb_execute()` `Flow.cpp:658` | Yes — sets the target HG and prepares the wait state | Apply the decision. For REPLICA_WITH_WAIT, snapshot the query and prepare the wait. |
| process_result | `polardb_process_result()` | Yes — advances session write/observed LSN state and latches | On a positioned RFQ, advance observed LSN; on a positioned write, advance write LSN; maintain missing-LSN latches; refresh the per-server LSN cache. |

```
 REQUEST PATH                                                      RESPONSE PATH
 ───────────────────────────────────────────────                  ──────────────────────────────
 client 'Q'
   │
   ▼  collect()            ── PolarDB_Query_RouteCtx (all inputs) ──►
   │  Flow.cpp:233
   ▼  plan()               ── PolarDB_Query_RoutePlan (action + reason) ─►          polardb_process_result()
   │  Flow.cpp:410                                                              reads RFQ LSN
   ▼  execute()            ── current_hostgroup, prepared wait state ──►       if is_write && has_lsn:
   │  Flow.cpp:658                                                               session.write_lsn = max(..)
   ▼  acquire reader → if reader already at target: reset_wait() (Wait_Wrap_Bypassed)
   ▼  (ASYNC_IDLE) wrap   (skipped when bypassed)                              refresh per-server LSN cache
   ▼  backend                                                       ◄─── RFQ (carries LSN) ───
```

### The decision matrix (what `plan()` decides)

`plan()` is evaluated strictly top-down; the **first** matching rule returns. Assume the pipeline is entered: a PolarDB HG is configured, it is not manual mode, `is_polar_hg` is true, and a reader HG exists. Columns show the resulting **action** and **action reason**. A dash means "that input does not matter at the row that decides."

| consistency_mode | in txn | multi-stmt | extended | session has LSN target? | lag over cap | Action | Action reason | Decided at |
|---|---|---|---|---|---|---|---|---|
| `replica_eligible = 0` (rule did not opt in) | — | — | — | — | — | PASSTHROUGH (writer) | — | `Flow.cpp:428` |
| any, with `/* route=primary */` hint | — | — | — | — | — | FORCE_PRIMARY | HINT_PRIMARY | `Flow.cpp:440` |
| PRIMARY_ONLY | — | — | — | — | — | FORCE_PRIMARY | MODE_PRIMARY | `Flow.cpp:455` |
| OFF | — | — | — | — | — | PASSTHROUGH (rules own, target = -1) | — | `Flow.cpp:466` |
| SESSION_LSN | yes | — | — | — | — | FORCE_PRIMARY | IN_TRANSACTION | `Flow.cpp:474-483` |
| SESSION_LSN | no | yes | — | — | — | FORCE_PRIMARY | MULTI_STATEMENT | `Flow.cpp:474-483` |
| SESSION_LSN | no | no | yes | — | — | FORCE_PRIMARY | EXTENDED_PROTOCOL | `Flow.cpp:580-592` |
| SESSION_LSN | no | no | no | no (`polardb_session_consistency.target() == 0`) | — | PASSTHROUGH (reader, no wait) | — | `Flow.cpp:567-577` |
| SESSION_LSN | no | no | no | yes (`> 0`) | cap attached to `plan.reader`; reader safety checked during backend acquisition | REPLICA_WITH_WAIT, or one-query writer fallback if acquisition returns a safety status | NONE at plan time; `PolarDB_ReaderStatus` at acquisition time | `Flow.cpp`, `HGM.cpp` |
| SESSION_LSN | no | no | no | yes (`> 0`) | no cap failure and compatible reader acquired | REPLICA_WITH_WAIT (reader + LSN wait) | NONE | `Flow.cpp`, `HGM.cpp` |

**A common mistake to avoid:** the "session has LSN target?" column does **not** mean "the current query is a write." The current query is already known to be a replica-safe read (`replica_eligible == 1`). The column means "does this session already have a monotonic target" — i.e. `polardb_session_consistency.target() > 0`, derived from prior positioned writes and prior positioned observations. There is no per-query is-write flag in `plan()`. `is_write_query()` is used only on the result-processing path, never in routing (`lib/PgSQL_PolarDB.cpp:102`).

Other things you can read off the matrix:

- `/* route=primary */` and `consistency_mode = PRIMARY_ONLY` both force the writer regardless of everything else.
- `consistency_mode = OFF` returns PASSTHROUGH with target `-1`, which means "leave routing to the query rules" — it is **not** a force-to-reader.
- Extended protocol is not wait-wrapped in this implementation. Manual destination-hostgroup routes remain authoritative; automatic extended reads without a prior write LSN can use a reader; automatic extended reads after a known write/observed LSN target or unknown RFQ target are forced to the writer.

Full per-stage detail, every action reason, and the lag-cap internals: `06-ROUTING-PIPELINE.md`.

---

## 7. Query-flow diagrams

Four common flows. Full step-by-step traces with the exact GUCs emitted and the session state before and after each stage are in `13-QUERY-LIFECYCLE-AND-TRACES.md` (scenarios T1–T6). These diagrams are the summary.

### Flow A — no-PolarDB passthrough

No PolarDB HG is configured, so the single atomic gate is false and the entire pipeline is skipped.

```
client 'Q' ──► get_pkts_from_client()
                  │  PgHGM->status.polardb_active == false   (Session.cpp:2543)
                  ▼  (PolarDB block skipped entirely)
               find_or_create_backend(current_hostgroup)   ── normal ProxySQL routing
                  ▼
               backend runs the user query unchanged
                  ▼
               RequestEnd(): is_polardb_enabled == false → no process_result
                  ▼
               result returned to client
```

### Flow B — RYW write→read (the core case)

A write sets the session LSN; the next replica-eligible read is wrapped to wait for it.

```
WRITE (e.g. INSERT/UPDATE), routed to the writer:
   ... runs on primary ...
   RequestEnd success → polardb_process_result()
       is_write = true, RFQ LSN = L
       polardb_session_consistency.write_lsn = max(0, L) = L         Flow.cpp:944-949

READ (replica_eligible, autocommit, simple query):
   collect()  → polardb_session_consistency.write_lsn = L
   plan()     → SESSION_LSN, session has written, lag OK
              → REPLICA_WITH_WAIT (target = L)            Flow.cpp:599-603
   execute()  → current_hostgroup = reader_hg
              → prepare wait state, snapshot query        Flow.cpp:714-728
   backend acquisition:
     ├─ reader already at L (thread-local cache hit, or acquired from the
     │    target-reached prefix → wait_bypass_allowed):
     │      reset_wait(); Wait_Wrap_Bypassed++
     │      → wrapper SKIPPED, the bare <user SELECT> goes to the reader
     │        (the reader is already caught up, so the wait would be a no-op)
     └─ reader behind L (fell back to the full set): keep the staged wait
   ASYNC_IDLE → finalize_wait_timeout_injection()  (emits nothing when bypassed)  Wrap.cpp:303
       backend receives ONE packet (non-bypass path):
         SET polar_consistency_mode = 'best_effort';
         SET polar_proxy_wait_timeout_ms = <ms>;
         SET polar_xact_split_wait_lsn = '<L>';
         <user SELECT>
   WIRE filter → drop 3 SET results, forward SELECT      Connection.cpp:564
   result returned to client  (guaranteed to include the write)
```

### Flow C — best_effort timeout

The replica does not catch up in time; in `best_effort` it serves stale data with a WARNING.

```
wrapped read on reader, polar_consistency_mode='best_effort'
   replica waits up to <ms>, does NOT reach target L
   backend emits a WARNING/NOTICE (structured detail = "polar_proxy_lsn_wait_timeout")
   backend runs the SELECT anyway and returns (possibly stale) rows
        │
        ▼
   notice_handler_cb → polardb_handle_notice()           Notices.cpp:97
        marker matches → account timeout                 Notices.cpp:149
        build a NoticeResponse, enqueue pending_notices  Notices.cpp:198
        │
        ▼
   flush pending notice, THEN normal or streamed result rows
        │
        ▼
   client sees: WARNING (once), then rows
   counters: Wait_Error_Timeout++ , Wait_Error_LSN_Wait_Timeout++
```

### Flow D — strict timeout

Same wait, but `polar_consistency_mode = 'strict'`: the replica raises an ERROR instead of serving stale data.

```
wrapped read on reader, polar_consistency_mode='strict'
   replica waits up to <ms>, does NOT reach target L
   backend raises an ERROR (structured detail = "polar_proxy_lsn_wait_timeout")
        │
        ▼
   WIRE filter sees an error status on a wrapper SET             Connection.cpp:579-590
        polardb_account_wrapper_set_error() → account timeout    (via Connection.cpp:70)
        stop consuming; let the ERROR flow to the client
        │
        ▼
   client sees: ERROR (read fails)
   counters: Wait_Error_Timeout++ , Wait_Error_LSN_Wait_Timeout++
```

---

## 8. Configuration at a glance

Full detail in `04-ADMIN-SCHEMA-AND-CONFIG.md`. Two configuration surfaces feed one resolver.

### Per-hostgroup-pair: `pgsql_replication_hostgroups`

The PolarDB build (`POLARDB_PROXY=1`) uses the LSN-only eight-column `V3_0_4` schema; it is wider than the upstream/non-PolarDB `V3_0_2` schema only because it adds the PolarDB LSN policy columns plus `proxy_protocol`.

| Column | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `writer_hostgroup` | INT `>= 0`, PRIMARY KEY | — | Writer (primary) HG id. |
| `reader_hostgroup` | INT `<> writer`, `>= 0`, UNIQUE | — | Reader (replica) HG id. |
| `check_type` | VARCHAR in `('read_only','polardb')` | `'read_only'` | `'polardb'` turns the pair into a PolarDB pair; only then do the LSN columns apply. |
| `consistency_mode` | VARCHAR in `('default','off','lsn','primary')` | `'default'` | Per-HG policy; `'default'` defers to the global knob. |
| `max_lag_bytes` | INT | `-1` | Per-HG reader byte-lag cap; `-1` = inherit global, `0` = off, `> 0` = cap. |
| `lsn_wait_timeout_ms` | INT | `-1` | Per-HG wait timeout (ms); `-1` = inherit global, `0` = wait indefinitely, `> 0` = explicit. |
| `proxy_protocol` | VARCHAR in `('default','v15','legacy','off')` | `'default'` | Per-HG startup protocol; `'default'` inherits the global knob. |
| `comment` | VARCHAR | `''` | Free text. |

The full implementation's CSN/split schema items (`txn_split_enabled`, and the `csn`/`session`/`global` consistency values) are **not part of this feature's V3_0_4**. Reintroducing them requires a later coordinated schema version and migration, not a reinterpretation of V3_0_4 — see `04-ADMIN-SCHEMA-AND-CONFIG.md`.

### Global: the `pgsql-polardb_*` admin variables

| Admin variable | Type | Range | Default | Meaning |
|---|---|---|---|---|
| `polardb_consistency_mode` | word | `off` \| `lsn` \| `primary` | `off` | Global consistency policy (lowest tier). |
| `polardb_wait_timeout_mode` | word | `best_effort` \| `strict` | `best_effort` | Timeout outcome: serve stale (WARNING) vs error. |
| `polardb_proxy_protocol` | word | `v15` \| `legacy` \| `off` | `v15` | Startup parameter dialect for RFQ payload requests. |
| `polardb_route_rfq_policy` | word | `strict` \| `best_effort` | `strict` | Missing RFQ target route policy. |
| `polardb_session_lsn_baseline` | word | `observed` \| `primary` | `observed` | Empty-session SESSION_LSN baseline. |
| `polardb_proxy_identity_host` | string | empty or non-wildcard IP literal | empty | Fallback startup identity host; non-empty host may be staged while port is `0`. |
| `polardb_proxy_identity_port` | int | `0 .. 65535` | `0` | Fallback startup identity port; completed fallback pairs require port `1..65535`. |
| `polardb_lag_bytes` | int | `0 .. INT_MAX` | `0` (off) | Global reader byte-lag cap. |
| `polardb_lag_ms` | int | `0` only | `0` | **Reserved in this implementation** — runtime accepts only `0`; no millisecond-lag producer exists (see [§12](#12-status-implemented-vs-deferred)). |
| `polardb_lag_wait_ms` | int | `0 .. 60000` | `1000` | Global wait timeout (ms); `0` = wait indefinitely. |
| `polardb_lsn_freshness_ms` | int | `100 .. 60000` | `5000` | Max age of a cached per-server LSN that routing will trust. |
| `polardb_monitor_lsn_updates` | bool | `0 \| 1` | `1` | Allow the monitor to refresh the per-server LSN cache. |

The word knobs are stored as strings but exposed to the hot path as parsed ints in thread-local copies.

`LOAD PGSQL SERVERS TO RUNTIME` and `LOAD PGSQL VARIABLES TO RUNTIME` log a warning for any PolarDB pair whose effective policy is `consistency_mode='lsn'` and effective startup protocol is `off` (directly on the row or inherited from the global protocol). The load still succeeds so operators can stage changes without blocking unrelated rows.

### Three-tier resolution

The consistency mode resolves through one pure function, `polardb_resolve_consistency_mode()` (`include/PgSQL_PolarDB.h:1277`), wired through `polardb_collect()`:

```
session override (set via SET on the client)   tier 1   (-1 = unset)
        │ if >= 0 use it
per-HG  consistency_mode (from the schema)      tier 2   (-1 = "default"/unset)
        │ else if >= 0 use it
global  pgsql-polardb_consistency_mode          tier 3   (always defined)
        ▼
effective_consistency_mode
```

The wait timeout resolves in two tiers (per-HG, then global; no session tier) via `polardb_resolve_wait_timeout_ms()`. The byte-lag cap also resolves in two tiers (per-HG, then global).

---

## 9. Observability at a glance

There are **26 exported stat counters + 1 internal `polardb_active` gate**. The
gate is a boolean, not a counter. The exported counter names are stable SQL
rows. Internally, 19 thread-backed counters use per-thread storage plus a global
counter: a PolarDB-local `stvar[]` slot on `PgSQL_Thread` plus the matching
`PgHGM->status.polardb_*` global counter. The 7 global-only counters remain global
atomics. Worker teardown folds the per-thread slots into the global counters so
shutdown-time scrapes and any future worker lifecycle changes keep monotonic
totals.

**Exposure:** SQL and Prometheus. The counters surface in the admin table `stats_pgsql_global` (filter `WHERE Variable_Name LIKE 'PolarDB_%'`) and as `proxysql_polardb_*_total` Prometheus counters. Prometheus uses the active PgSQL HGM metrics path; it does not revive the commented generic PgSQL thread-variable export loop.

Full per-counter semantics, lockstep partners, and operator interpretation are in `12-THREADVARS-AND-OBSERVABILITY.md`. The main counters:

| Counter (display name) | Increment site | What a non-zero value tells an operator |
|---|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ` | `Flow.cpp:916` | LSN learning over the query path is being accepted for the current writer group+epoch. Should grow under traffic; flat-zero means the LSN-on-RFQ patch is not active, traffic is not returning RFQ LSN, or result processing is rejecting stale/missing writer group/epoch data. |
| `PolarDB_LSN_Updates_From_Monitor` | `Monitor.cpp:1947` | The monitor is refreshing LSN data between queries. |
| `PolarDB_Session_LSN_Routing` | `Flow.cpp:716` | Reads are being routed to a replica with an LSN wait (RYW routing is happening). |
| `PolarDB_Wait_Wrap_Prepared` | `Flow.cpp:728` | A wait-wrapper intent was prepared. Tracks `Session_LSN_Routing` 1:1 in this implementation. |
| `PolarDB_Wait_Wrap_Bypassed` | backend acquisition | The selected reader already reached the consistency target, so the staged wait wrapper was cleared before dispatch. |
| `PolarDB_Wait_LSN_Sent` | `Wrap.cpp:244` | The wait actually went on the wire. `Prepared - Sent` is expected to include bypassed waits plus any safety aborts. |
| `PolarDB_Wait_LSN_Sum_Us` | `Wrap.cpp:272` | Total microseconds spent waiting. Divide by `Wait_LSN_Sent` for the average wait. |
| `PolarDB_Wait_Wrap_Safety_Abort` | `Wrap.cpp:185` | Wrapper construction failed before the read could be protected. Should be 0; each one is also logged, and the read is not sent unwrapped to a replica. |
| `PolarDB_Wait_Error_Timeout` | `Wrap.cpp:298` | RYW waits are timing out (replicas can't catch up in time). |
| `PolarDB_Wait_Error_LSN_Wait_Timeout` | `Wrap.cpp:300` | The LSN subset of the timeout total. Equals `Wait_Error_Timeout` in this implementation (LSN is the only wait type). |
| `PolarDB_Wait_Error_Connection_Lost` | dispatch error handling | A wait-wrapped reader lost its backend connection before any user result reached the client. |
| `PolarDB_LSN_Stale_Count` | reader acquisition | Active for byte-lag enforcement. It increments when an enabled `max_lag_bytes` check sees missing or stale primary/reader LSN state. The deferred millisecond-lag branch is still guarded by `POLARDB_PROXY_TODO`. |
| `PolarDB_Write_Missing_LSN` | process_result | Writer query completed without RFQ LSN; later automatic LSN-mode reads in that session follow `route_rfq_policy` until a primary-sourced RFQ clears the latch. |
| `PolarDB_Read_Missing_LSN` | process_result | Tracked SESSION_LSN read completed without RFQ LSN. |
| `PolarDB_Primary_LSN_Unknown` | plan | Primary baseline requested but the primary mirror had no LSN. |
| `PolarDB_RFQ_Best_Effort_Degraded_Routes` | plan | Best-effort policy allowed a degraded reader route without a wait target. |
| `PolarDB_Consistency_Writer_Fallback` | dispatch | Reader acquisition used the writer because RFQ was unavailable under strict policy, primary LSN was unknown under a cap, or every viable reader was missing/stale/over-lagged. |
| `PolarDB_Wait_Reads_Retried_On_Writer` | dispatch error handling | A wait-wrapped reader query was retried once on the writer after strict wait timeout or reader connection loss, before any user result reached the client. |
| `PolarDB_RFQ_Profile_Skipped` | pool selection | Pooled connection skipped because its startup profile did not request RFQ LSN. |
| `PolarDB_RFQ_Profile_Evicted` | pool selection | Incompatible free pooled connections evicted to make room for RFQ-LSN-capable replacements. |
| `PolarDB_TL_Cache_Bypassed_For_Target` | pool selection | Targeted read bypassed the thread-local backend cache so route-smart selection could enforce RFQ/profile requirements. |
| `PolarDB_Target_LSN_Preferred` | reader acquisition | Fresh cached reader at or beyond the target was preferred and acquired. |
| `PolarDB_Target_LSN_Fallback_Wait` | reader acquisition | Acquisition fell back to the full weighted candidate set and relied on the wait wrapper. |
| `PolarDB_Session_Target_Epoch_Reset` | collect/process_result | At least one session LSN target or missing-LSN latch was discarded after the session writer group or epoch changed. |
| `PolarDB_Monitor_Health_Invalid_Role` | `Monitor.cpp:838` | Monitor health rows reported a role ProxySQL cannot route to (including POLAR_UNKNOWN / POLAR_STANDALONE_DATAMAX); a topology or backend-config problem the monitor is rejecting. |
| `PolarDB_Monitor_Health_Invalid_Values` | `Monitor.cpp:842` | Monitor health rows had an invalid availability or LSN text field; the monitor could not parse the health probe result. |

Known observability gaps: no wait-**success** counter, no generic FORCE_PRIMARY reason breakdown outside the consistency reader-acquisition fallback counter, no per-server LSN/lag gauge in stats, and only a sum (not max/p99) for wait latency. These are catalogued in `12-THREADVARS-AND-OBSERVABILITY.md` and `15-LIMITATIONS-AND-ROADMAP.md`.

---

## 10. Build and compatibility

Full detail in `02-BUILD-TOGGLE-AND-LIBPQ.md`.

### The build toggle

The whole feature is gated by one make variable, default ON:

```
POLARDB_PROXY ?= 1      # Makefile:149
```

Build with `make POLARDB_PROXY=0` to compile the feature out. The flag becomes the `-DPOLARDB_PROXY` C++ define in each compile stage (`lib/Makefile`, `src/Makefile`); the source tests it with `#if POLARDB_PROXY`. The helper target `make polardb-check` clean-builds both tiers.

### Both tiers stay equivalent

- Every PolarDB declaration and every core call site is wrapped in `#if POLARDB_PROXY`. With `POLARDB_PROXY=0` the five feature translation units compile to empty objects.
- `lib/PgSQL_PolarDB_Stubs.cpp` is the place for any link-time no-op stub, but it is intentionally empty today: because both declarations and call sites are guarded, the `POLARDB_PROXY=0` build has no unguarded symbol to stub (`lib/PgSQL_PolarDB_Stubs.cpp:10-23`).
- The schema and the table-load SELECT also branch on `POLARDB_PROXY`, so the non-PolarDB build keeps the exact upstream column shape.

> Note: byte-equivalence of the `POLARDB_PROXY=0` build to upstream is the documented in-code contract (`PgSQL_PolarDB_Stubs.cpp:10-23`), grounded in source reading, not a build+diff performed for this doc.

### The libpq patch requirement (mandatory for RYW)

RYW depends on a patched libpq that reads the LSN PolarDB appends to ReadyForQuery. The patch (`deps/postgresql/polardb_libpq.patch`) is applied **only** when `POLARDB_PROXY=1`; a `POLARDB_PROXY=0` build links vanilla libpq.

What the patch adds:

- Three new public functions, exported as ordinals 188–190: `PQgetLSN()` (the LSN from the last RFQ, 0 if none), `PQhasLSN()` (was an LSN present?), and `PQsetPolarSendLSN()` (turn on the per-connection LSN parse).
- New `struct pg_conn` runtime fields and a set of connection-string options, including `_polar_send_lsn`.
- A change to `getReadyForQuery()` that, after the normal RFQ parse, reads a network-order 64-bit LSN if `polar_proxy_send_lsn` is set and at least 8 bytes remain, then skips any unparsed trailing bytes (`conn->inCursor = msg_end`). That boundary check is the documented fix for the extended-protocol length mismatch.

**Deployment consequence:** RYW requires both a genuine PolarDB backend (that appends the LSN) and the patched libpq (that reads it). Without them the feature degrades safely: result processing cannot advance `polardb_session_consistency.write_lsn` / `.observed_lsn`, and missing-LSN events are handled by latches plus `route_rfq_policy`. See `17-OPERATOR-GUIDE.md`.

---

## 11. Correctness summary

Full proofs and the failure-mode catalogue are in `14-INVARIANTS-AND-FAILURE-MODES.md`. The key points:

**The RYW guarantee.** After a session writes (and the write's RFQ carried an LSN), every later autocommit, simple-query, replica-eligible read from that same session sees that write. It holds because the read is sent to the replica with `SET polar_xact_split_wait_lsn = '<polardb_session_consistency.target()>'`, and the replica blocks until it has replayed past that LSN before answering. **Preconditions:** a patched libpq and a genuine PolarDB backend.

**The single most important point.** The wait `SET` — run on the replica — is the default consistency gate. ProxySQL may skip that `SET` only after backend acquisition binds the query to a specific reader whose fresh cached LSN is already at or beyond the same consistency target. Fallback readers still use the backend wait. The byte-lag cap remains a safety-only bound: it keeps reads off a replica that is so far behind the wait would likely time out.

**Safe writer fallback.** When anything is uncertain, the read goes to the writer, which is always consistent, instead of risking a stale replica read. Every safe fallback path:

| # | Trigger | Where |
|---|---|---|
| 1 | `/* route=primary */` hint | `Flow.cpp:440` |
| 2 | `consistency_mode = PRIMARY_ONLY` | `Flow.cpp:455` |
| 3 | Inside an explicit transaction | `Flow.cpp:474-483` |
| 4 | Multi-statement read | `Flow.cpp:474-483` |
| 5 | Extended protocol reached the planner | `Flow.cpp:580-592` |
| 6 | Consistency helper returned PRIMARY (defensive) | `Flow.cpp:553-560` |
| 7 | Reader lag over the cap | `Flow.cpp:597` plus HGM reader acquisition |
| 8 | Cap enabled but a primary/reader LSN is stale or missing | `Flow.cpp:358-384` plus HGM reader acquisition |
| 9 | Malformed simple-query packet (`size < 7`) | `Flow.cpp:691-697` |
| 10 | `polardb_wait_disabled` (a prior wrapper build failed this session) | `Flow.cpp:703-711` |
| 11 | Wrapper build fails for the current query | `Wrap.cpp:263` → caller sends an ERROR (`Session.cpp:3595`); sets `polardb_wait_disabled` for future reads (#10) |

**Other invariants:** `polardb_process_result()` reads the LSN from RFQ only (never an extra SQL probe on the hot path); `polardb_session_consistency.write_lsn` advances monotonically via `max()` and survives RESET but is zero per new client session; timeout accounting is idempotent (the `wait_started_at_us == 0` de-dup) so the same backend timeout cannot be double-counted.

---

## 12. Status: implemented vs deferred

| Capability | Status | Notes / next-step doc |
|---|---|---|
| LSN session RYW (autocommit, simple query) | **Implemented** | The core feature. `06`, `07`, `08`, `09`. |
| `best_effort` / `strict` timeout modes | **Implemented** | Structured-marker detection. `08-WAIT-TIMEOUT-AND-NOTICES.md`. |
| Byte-lag safety cap (`polardb_lag_bytes` / `max_lag_bytes`) | **Implemented** | Safety only, not the gate. `05`, `06`. |
| Per-server LSN cache (RFQ + monitor) | **Implemented** | `05-MONITOR-AND-HGM-LSN-STATE.md`. |
| Consistency-target reader preference | **Implemented** | Readers with fresh cached LSN `>= consistency_target_lsn` are preferred first; fallback keeps the full weighted reader set and the wait wrapper remains the correctness gate. `06`, `12`. |
| 26 stat counters + `polardb_active` gate | **Implemented** | SQL and Prometheus exposure; `polardb_active` is internal only. `12-THREADVARS-AND-OBSERVABILITY.md`. |
| Compile toggle + libpq RFQ-LSN patch | **Implemented** | `02-BUILD-TOGGLE-AND-LIBPQ.md`. |
| **Millisecond lag cap** (`polardb_lag_ms`) | **DEFERRED / inert** | Runtime accepts only `0`; no producer and no routing effect. The helper `polardb_lag_ms_within_cap()` is explicitly "wire only after a real producer exists" (`include/PgSQL_PolarDB.h:533-547`; header note `:504-507`; knob `include/PgSQL_Thread.h:1008`; per-server note `include/PgSQL_HostGroups_Manager.h:220-222`). |
| **`PolarDB_LSN_Stale_Count`** | **ACTIVE for byte-lag; ms-lag deferred** | Increments when an enabled `max_lag_bytes` check cannot trust a primary or reader LSN sample. The millisecond-lag producer remains deferred. |
| Hard caught-up reader-acquisition gate | **Not implemented by design** | ProxySQL does not reject the original reader set solely because cached LSN is below target; the wait SET is the gate. |
| CSN / global consistency | **Future (not in branch)** | **Experimental, incomplete**; needs PolarDB backend support; global-mode only; wait behavior not reliably verified. `18-FUTURE-CSN-DESIGN.md`. |
| Transaction-split read offload | **Future (not in branch)** | `19-FUTURE-TXN-SPLIT-DESIGN.md`. |
| Reader-failure recovery / retry | **Future (not in branch)** | `20-FUTURE-READER-FAILURE-RETRY-DESIGN.md`. |
| Extended-protocol RYW | **Future (not in branch)** | No wait wrapper in this implementation; manual reader routes are honored, automatic reads without a prior write LSN may use reader, and automatic reads after a known write/observed LSN target or unknown RFQ target use writer. |
| Cancel-session metadata | **Not present** | Future PolarDB15 extension; this implementation keeps no inert session-id/cancel-key state. |

The roadmap hub with the extension path to v2 is `15-LIMITATIONS-AND-ROADMAP.md`.

---

## 13. Source map and line counts

Dedicated files:

| File | Lines |
|---|---:|
| `include/PgSQL_PolarDB.h` | 1926 |
| `include/PgSQL_PolarDB_Counters.h` | 127 |
| `lib/PgSQL_PolarDB.cpp` | 111 |
| `lib/PgSQL_PolarDB_Flow.cpp` | 1030 |
| `lib/PgSQL_PolarDB_Consistency.cpp` | 62 |
| `lib/PgSQL_PolarDB_Wrap.cpp` | 449 |
| `lib/PgSQL_PolarDB_Failure.cpp` | 322 |
| `lib/PgSQL_PolarDB_Notices.cpp` | 293 |
| `lib/PgSQL_PolarDB_Stubs.cpp` | 34 |
| **Total (dedicated)** | **4350** |

Key entry points to jump to:

| What | Where |
|---|---|
| Route pipeline call site (HOOK 2) | `lib/PgSQL_Session.cpp:2543-2568` |
| collect / plan / execute / process_result | `lib/PgSQL_PolarDB_Flow.cpp:233 / :410 / :658 / :846` |
| Single wrap point | `lib/PgSQL_PolarDB_Wrap.cpp:303` (called from `lib/PgSQL_Session.cpp:3607`) |
| WIRE filter (HOOK 3) | `lib/PgSQL_Connection.cpp:564-592` |
| Notice hook | `lib/PgSQL_Connection.cpp:2558-2563` → `lib/PgSQL_PolarDB_Notices.cpp:97` |
| Process-result hook (HOOK 4) | `lib/PgSQL_Session.cpp` |
| Connect/enable (HOOK 1) | `lib/PgSQL_Connection.cpp:1204-1221`, `:1325/:1335`; `lib/PgSQL_Session.cpp:5696/5710` |
| `polardb_active` gate set | `lib/PgSQL_HostGroups_Manager.cpp:1864` |
| Counters | `include/PgSQL_HostGroups_Manager.h:677-714` |
| Schema | `include/ProxySQL_Admin_Tables_Definitions.h:308` |

---

## 14. Glossary (quick reference)

The authoritative glossary is in `01-BACKGROUND-AND-DESIGN.md`. One term per concept, used the same way everywhere.

| Term | Meaning |
|---|---|
| **PolarDB** | An Alibaba PostgreSQL-compatible database with one primary (writer) and read replicas. |
| **writer / reader** | Writer = primary node (takes writes). Reader = replica (read-only, may lag). Used throughout instead of "primary/replica" for the durable field names (`writer_hg` / `reader_hg`). |
| **hostgroup (HG)** | A numbered group of backend servers in ProxySQL. A PolarDB pair links a writer HG and a reader HG. |
| **LSN (Log Sequence Number)** | A 64-bit position in the write-ahead log (WAL); larger = more recent. Carried as `XLogRecPtr` (`uint64_t`, `InvalidXLogRecPtr = 0`). |
| **WAL (Write-Ahead Log)** | PostgreSQL/PolarDB's append-only log of changes; replicas replay it to catch up. |
| **RYW (read-your-writes)** | After a session writes, its own later reads see that write, even on a replica. |
| **RFQ (ReadyForQuery)** | The PostgreSQL message a backend sends after each command. Patched PolarDB libpq appends the WAL LSN to it. |
| **wait wrapper** | The `SET` statements ProxySQL prepends to a replica-eligible read so the replica waits for the target LSN. The same concept; not called "wrap" elsewhere. |
| **process_result** | The response-path step that reads the RFQ LSN, advances session write/observed LSNs, maintains missing-LSN latches, and refreshes per-server cache state. |
| **CSN (Commit Sequence Number)** | A future, experimental commit counter for cross-session consistency. Not in this branch. |
| **GUC** | A PostgreSQL server setting changed with `SET name = value`. |

---

## 15. Document cross-reference index

This document is the entry point. The set is organized as: three self-contained reference docs (this doc, structures, and the structure-domain map), numbered per-concern deep-dives (01–12), cross-cutting docs (13–17), and future-design docs (18–21).

| Doc | What it covers |
|---|---|
| `POLARDB_STRUCTURES.md` | Every struct, enum, field, and counter: ownership, lifecycle, thread-safety. The data reference. |
| `POLARDB_STRUCTURE_DOMAIN_MAP.md` | Structure relationships: ownership roots, shared leaf values, suffix vocabulary, per-query flow, and normalization audit. |
| `01-BACKGROUND-AND-DESIGN.md` | The RYW problem, PolarDB's LSN mechanism, why LSN-only first, design principles, glossary. |
| `02-BUILD-TOGGLE-AND-LIBPQ.md` | `POLARDB_PROXY` gating, the empty stub TU, the libpq RFQ-LSN patch surface. |
| `03-TYPES-AND-ENUMS.md` | All `PgSQL_PolarDB.h` enums, structs, and constants. |
| `04-ADMIN-SCHEMA-AND-CONFIG.md` | The `pgsql_replication_hostgroups` schema, the knobs, the three-tier resolution. |
| `05-MONITOR-AND-HGM-LSN-STATE.md` | Per-server LSN cache, topology maps, lag cap, freshness gate. |
| `06-ROUTING-PIPELINE.md` | The collect/plan/execute pipeline and the full decision matrix (the most important part). |
| `07-QUERY-WRAPPING.md` | The wait-wrapper SQL, the single wrap point, the consume loop. |
| `08-WAIT-TIMEOUT-AND-NOTICES.md` | best_effort WARNING vs strict ERROR, structured-marker detection, accounting de-dup. |
| `09-PUBLISH-AND-WRITE-TRACKING.md` | RFQ LSN capture → `polardb_session_consistency.write_lsn` and per-server cache. |
| `10-SESSION-INTEGRATION.md` | Session fields, the hook points, per-query and reset lifecycle. |
| `11-CONNECTION-AND-LIBPQ.md` | conninfo, RFQ accessors, `dispatch_state`, the wrap-state filter, the notice receiver. |
| `12-THREADVARS-AND-OBSERVABILITY.md` | Thread-local knobs + every counter with full semantics + stats exposure. |
| `13-QUERY-LIFECYCLE-AND-TRACES.md` | Stage-by-stage trace + worked scenarios T1–T6 with state snapshots. |
| `14-INVARIANTS-AND-FAILURE-MODES.md` | The RYW guarantee, safe fallback proofs, the failure catalogue. |
| `15-LIMITATIONS-AND-ROADMAP.md` | Out-of-scope items, deferred knobs, the status matrix, the path to v2. |
| `16-TESTING-AND-VALIDATION.md` | The libpq smoke test, the integration suite, the RYW invariants to assert, CI gaps. |
| `17-OPERATOR-GUIDE.md` | Deployment requirements, config recipes, troubleshooting by counter/log. |
| `18-FUTURE-CSN-DESIGN.md` | **Future, experimental:** CSN global consistency, as a delta from this feature. |
| `19-FUTURE-TXN-SPLIT-DESIGN.md` | **Future:** in-transaction read offload (split FSM), as a delta from this feature. |
| `20-FUTURE-READER-FAILURE-RETRY-DESIGN.md` | **Future:** reader-failure recovery / retry, as a delta from this feature. |
| `21-FUTURE-OTHER-CAPABILITIES.md` | **Future:** remaining full-implementation parts (warmup, version detection, deeper health checks). |

---

## Appendix: Mermaid diagrams

These render the same diagrams from the body, for viewers (e.g. GitHub) that show Mermaid.

### A1. High-level architecture (from §3)

```mermaid
flowchart TD
    C[Client] -->|'Q' query| GPC[get_pkts_from_client]
    GPC --> GATE{polardb_active?<br/>Session.cpp:2563}
    GATE -- no --> FB[find_or_create_backend<br/>normal routing]
    GATE -- yes --> PIPE[Route pipeline HOOK 2<br/>collect -> plan -> execute<br/>Flow.cpp:233/410/658]
    PIPE --> FB
    FB --> H1[Backend attach HOOK 1<br/>profile-driven startup params<br/>+ conditional PQsetPolarSendLSN<br/>when REQUEST_RFQ_LSN requested]
    H1 --> WRAP[ASYNC_IDLE: finalize_wait_timeout_injection<br/>single wrap point<br/>skipped when reader already at target<br/>Wait_Wrap_Bypassed<br/>Session.cpp:3607 -> Wrap.cpp:303]
    WRAP --> BE[Backend runs:<br/>SET mode; SET timeout; SET wait_lsn; user read<br/>SETs omitted when bypassed]
    BE --> H3[WIRE filter HOOK 3<br/>drop 3 SET results, keep user result<br/>+ notice hook<br/>Connection.cpp:564 / 2558]
    H3 --> H4[RequestEnd process_result HOOK 4<br/>read RFQ LSN, advance observed/write LSNs<br/>]
    H4 --> R[Result + notice to client]
```

### A2. The four hooks on the timeline (from §5)

```mermaid
sequenceDiagram
    participant Cl as Client
    participant Se as PgSQL_Session
    participant Co as PgSQL_Connection (backend)
    participant Be as PolarDB backend
    Note over Se,Co: HOOK 1 connect/enable (Connection.cpp:1204/1335, Session.cpp:5696/5710)
    Cl->>Se: read 'Q'
    Note over Se: HOOK 2 route pipeline collect->plan->execute (Session.cpp:2543)
    Se->>Co: dispatch (ASYNC_IDLE wrap, Wrap.cpp:303)
    Co->>Be: SET mode; SET timeout; SET wait_lsn; user read
    Be-->>Co: 3x SET result, then user result (+ best_effort WARNING)
    Note over Co: HOOK 3 WIRE filter drops 3 SET results + notice hook (Connection.cpp:564/2558)
    Co-->>Se: user result only
    Note over Se: HOOK 4 process_result read RFQ LSN, advance observed/write LSNs
    Se-->>Cl: result (+ notice)
```

### A3. The pipeline (from §6)

```mermaid
flowchart LR
    Q['Q' query] --> CO[collect<br/>Flow.cpp:233<br/>RouteCtx]
    CO --> PL[plan<br/>Flow.cpp:410<br/>RoutePlan: action + reason]
    PL --> EX[execute<br/>Flow.cpp:658<br/>set HG, prepare wait]
    EX --> ACQ{acquired reader<br/>already at target?}
    ACQ -- yes --> BYP[reset_wait<br/>Wait_Wrap_Bypassed<br/>wrapper skipped]
    ACQ -- no --> WR[ASYNC_IDLE wrap]
    BYP --> BK[backend]
    WR --> BK[backend]
    BK -->|RFQ carries LSN| PUB[process_result<br/>advance observed/write LSNs<br/>refresh per-server cache]
```

### A4. The decision matrix as a flow (from §6)

```mermaid
flowchart TD
    S[plan: enter<br/>polar HG, reader exists] --> E{replica_eligible?}
    E -- no --> PW1[PASSTHROUGH writer<br/>Flow.cpp:222]
    E -- yes --> H{route=primary hint?}
    H -- yes --> FP1[FORCE_PRIMARY HINT_PRIMARY<br/>Flow.cpp:234]
    H -- no --> M{consistency_mode}
    M -- PRIMARY_ONLY --> FP2[FORCE_PRIMARY MODE_PRIMARY<br/>Flow.cpp:249]
    M -- OFF --> PT[PASSTHROUGH target=-1<br/>rules own routing<br/>Flow.cpp:260]
    M -- SESSION_LSN --> T{in transaction?}
    T -- yes --> FP3[FORCE_PRIMARY IN_TRANSACTION<br/>Flow.cpp:269]
    T -- no --> MS{multi-statement?}
    MS -- yes --> FP4[FORCE_PRIMARY MULTI_STATEMENT<br/>Flow.cpp:277]
    MS -- no --> XP{extended protocol?}
    XP -- yes --> FP5[FORCE_PRIMARY EXTENDED_PROTOCOL<br/>Flow.cpp:285]
    XP -- no --> W{session has target LSN?}
    W -- no --> PR[PASSTHROUGH reader, no wait]
    W -- yes --> RW[REPLICA_WITH_WAIT plan<br/>reader acquisition enforces cap/status]
    RW --> ACQ{reader acquired?}
    ACQ -- yes --> AT{from target-reached<br/>prefix or tl-cache hit?}
    AT -- yes --> BYP[reader, wrapper bypassed<br/>Wait_Wrap_Bypassed]
    AT -- no --> SEND[reader + LSN wait<br/>Target_LSN_Fallback_Wait]
    ACQ -- safety status --> FP6[one-query writer fallback]
```

### A5. Flow B — RYW write→read (from §7)

```mermaid
sequenceDiagram
    participant Cl as Client
    participant Se as PgSQL_Session
    participant Wr as Writer
    participant Rd as Reader
    Cl->>Se: WRITE
    Se->>Wr: run write
    Wr-->>Se: RFQ LSN = L
    Note over Se: process_result: polardb_session_consistency.write_lsn = max(0,L) = L (Flow.cpp:944-949)
    Cl->>Se: READ (replica_eligible, autocommit)
    Note over Se: plan -> REPLICA_WITH_WAIT target=L (Flow.cpp:599-603)
    alt acquired reader already at L (tl-cache hit or wait_bypass_allowed)
        Note over Se: reset_wait(); Wait_Wrap_Bypassed++ — wrapper skipped
        Se->>Rd: bare user SELECT (no SETs)
        Rd-->>Se: rows (reader already at L)
    else reader behind L (fell back to full set)
        Se->>Rd: SET wait_lsn='L'; user SELECT
        Rd-->>Se: (waits to L) rows
        Note over Se: WIRE filter drops SET results (Connection.cpp:564)
    end
    Se-->>Cl: rows (include the write)
```

### A6. Flow C / D — timeout (from §7)

```mermaid
flowchart TD
    WT[wrapped read on reader<br/>wait times out at target L] --> MODE{polar_consistency_mode}
    MODE -- best_effort --> N[backend WARNING + stale rows]
    N --> NH[notice hook account timeout<br/>Notices.cpp:149]
    NH --> FW[flush notice, then rows]
    FW --> CB[client: WARNING once, then rows]
    MODE -- strict --> ER[backend ERROR]
    ER --> WF[WIRE sees error on wrapper SET<br/>account timeout<br/>Connection.cpp:666-678]
    WF --> CE[client: ERROR]
    NH --> CNT[Wait_Error_Timeout++ / Wait_Error_LSN_Wait_Timeout++]
    WF --> CNT
```

---

Verified against this branch.
