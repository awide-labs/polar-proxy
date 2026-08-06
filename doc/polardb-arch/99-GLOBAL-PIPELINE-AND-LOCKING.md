# 99 — Global Pipeline & Locking Model (end-to-end layering)

> Scope: the single end-to-end view of one PolarDB request — the event-driven execution model,
> the three enable conditions, per-query routing (Simple + Extended), reader selection → acquisition →
> backend preparation → dispatch, the backend response loop, `RequestEnd`/`process_result`, the
> connection lifecycle, the failure state machine, and the fast/slow-path locking model. Companion
> to [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) (routing stages + traces C.1–C.16).
> | Audience: maintainers, operators, and contributors | Status: current implementation reference.

Notation matches doc 06: an actor (`CLIENT ──▶`, `BACKEND ──▶`) begins a flow, `├─`/`└─` are
steps or branches, `│` continues the same flow, and `→` *inside* a step means "leads to".
Right-aligned `file:line` anchors are repo-relative navigation aids; function names are the stable
reference when later edits move a line.

## Contents

- [Execution model: the event loop](#execution-model-the-event-loop)
- [TL;DR: one request end to end](#tldr-one-request-end-to-end)
- [Backend connection async lifecycle](#backend-connection-async-lifecycle)
- [The three PolarDB enable conditions](#the-three-polardb-enable-conditions)
- [0. The one invariant](#0-the-one-invariant)
- [1. Connection startup](#1-connection-startup)
- [2. Per-query request and route action](#2-per-query-request-and-route-action)
    - [2.1 Extended Protocol lane](#21-extended-protocol-lane)
- [3. Reader selection, acquisition, and backend preparation](#3-reader-selection-acquisition-and-backend-preparation)
- [4. Wrapper construction and dispatch](#4-wrapper-construction-and-dispatch)
- [5. Backend response loop, RequestEnd, finishQuery](#5-backend-response-loop-requestend-finishquery)
- [6. Failure state machine](#6-failure-state-machine)
    - [6.1 Autocommit consistency-read failure](#61-autocommit-consistency-read-failure)
    - [6.2 Transaction-reader failure](#62-transaction-reader-failure)
    - [6.3 Generic ProxySQL fallback](#63-generic-proxysql-fallback)
    - [6.4 Timeout and RFQ-unavailable types](#64-timeout-and-rfq-unavailable-types)
- [7. Connection lifecycle: keep, return, reset, destroy](#7-connection-lifecycle-keep-return-reset-destroy)
- [8. Locking model: fast vs slow paths](#8-locking-model-fast-vs-slow-paths)
    - [8.1 Fast path: SELECTION](#81-fast-path-selection)
    - [8.2 Warm path: GET](#82-warm-path-get)
    - [8.3 Warm path: RETURN](#83-warm-path-return)
    - [8.4 Cold path: CREATE](#84-cold-path-create)
    - [8.5 Topology mutation and retirement](#85-topology-mutation-and-retirement)
    - [8.6 Warmup and maintenance locking](#86-warmup-and-maintenance-locking)
    - [8.7 Fast and slow lock summary](#87-fast-and-slow-lock-summary)
    - [8.8 Open issues and refinement points](#88-open-issues-and-refinement-points)

---

## Execution model: the event loop

The stages §1–§7 below read as a straight line, but that is their *logical* order, not how they
run in time. ProxySQL is **event-driven**, and understanding that is what makes the rest of this
document connect.

Each worker thread runs ProxySQL's own `poll()` / `epoll_wait()` loop over frontend and
backend data streams. I/O readiness drives the work: when a socket becomes readable or writable,
the worker advances the owning session's `handler()`. The handler is a state machine, not a
straight-through function. One client query therefore crosses `handler()` many times while the
backend connection advances. The session carries a status state machine
(`PROCESSING_QUERY`, `CONNECTING_SERVER`, `RESETTING_CONNECTION`, …), and each backend
connection carries its own asynchronous state (`ASYNC_IDLE` plus connect, query, and result
states; `proxysql_structs.h:132`).

```
WORKER ──▶ one worker runs ProxySQL's poll/epoll loop over client + backend fds
           │
           ├─ a socket becomes ready → call that session's handler()  — a STATE MACHINE
           │    session status : PROCESSING_QUERY · CONNECTING_SERVER · RESETTING · …
           │    backend async  : ASYNC_IDLE · connect / query / result   proxysql_structs.h:132
           │
           └─ ONE query crosses handler() MANY times, yielding to the loop between ticks:
                ├─ tick 1      client 'Q' → route (§2) → acquire a backend (§3)
                │                warm pooled backend already ASYNC_IDLE ───────────────┐
                │                cold backend → begin connect + auth, then YIELD        │
                ├─ ticks 2..k  backend fd ready on later ticks → connect / auth → ASYNC_IDLE
                ├─ tick k      backend idle → finalize staged wait if needed + dispatch (§4) ◀─┘
                │                (the replica may block on SET wait_lsn; meanwhile the
                │                 worker serves OTHER sessions whose fds are ready)
                ├─ ticks k+1…  backend result readable → response loop + RequestEnd (§5)
                └─ last tick   keep / return / reset / destroy the connection (§7)
```

Two consequences define the execution model:

- **Backend I/O waits are asynchronous.** Connecting, authentication, reset, query execution, and
  `SET polar_xact_split_wait_lsn` do not busy-wait for backend I/O. The session yields and is
  advanced again when its fd is ready, while the worker can serve other ready sessions. Short
  mutex waits and synchronous CPU work can still occur inside one handler invocation.
- **Warm collapses, cold spreads.** On the warm path a pooled, already-idle backend lets §2 → §4
  happen on a single tick; a cold connect, a reset, or a real LSN wait spreads the *same* logical
  stages across many ticks. The stage *order* is invariant — only the number of ticks changes.

The ReaderPool/HGM locks described in §8 are taken and released inside one handler invocation;
none is intentionally held while waiting for backend I/O. Section 8 covers connection selection,
pool ownership, topology, and warmup locks. It does not claim that the parser, query processor,
logger, allocator, or other ProxySQL subsystems are lock-free.

---

## TL;DR: one request end to end

The whole request in two arrows, shown for the **cold path** — a freshly connected client with an
empty pool, so a backend must be created and connected. Assumes client startup (§1) already ran;
the example is an autocommit consistency read, so the wait wrapper appears. On a *warm* second
query the `SELECT → GET(hit) → ASYNC_IDLE → dispatch` steps collapse into a single event-loop tick
— no CREATE, no connect.

**Forward — `query ──▶ … ──▶ backend`:**

```
CLIENT query 'Q'
 → decode frame + parser / digest / query rules
 → init query lifecycle  (reset reader target + writer scope)                    §2
 → routing check (polardb_active) → automatic PolarDB route                      §2
 → observe → collect → plan → account → execute   (one route action)            §2
 → plan = REPLICA_WITH_WAIT → STAGE wait intent (save query + target LSN)        §2 → §4
 → SELECT preferred reader   (lock-free snapshot, no mutex)                       §3 / §8.1
 → POOL GET on that server   (pool_mutex) → MISS (0 backends)                     §3 / §8.2
 → CREATE conn object        (HGM wrlock + pool_mutex; object only, no I/O)       §3 / §8.4
 → BACKEND PREPARE (async, UNLOCKED, many event-loop ticks):
        TCP connect → TLS → auth → PolarDB startup profile / RFQ negotiation      §3
 → backend reaches ASYNC_IDLE                                                     §4
 → FINALIZE wrapper once: SET mode; SET timeout; SET wait_lsn; <query>            §4
 → DISPATCH (PQsendQuery / async_query) → BACKEND runs (may block on wait_lsn)    §4
```

**Return — `backend reply ──▶ … ──▶ client`:**

```
BACKEND result LOOP  (messages over many ticks)                                  §5
 → hide the internal SET results
 → preserve any NoticeResponse (pending-notice queue)
 → stream visible DataRows to the client  (may begin before the query ends)
 → terminal ReadyForQuery  (the ONLY message carrying RFQ LSN / XID)
 → build client RFQ  (polardb_client_ready_lsn; may raise to a proven target)     §5 / 09§6
 → drain remaining result → client output queue → update txn-state manager        §5
 → RequestEnd(success) → polardb_process_result (cache / session LSN, XID state)   §5
 → finishQuery → keep one exact conn for this worker pass, or return to shared     §7 / §8.3
 → socket writeout → CLIENT sees the result
```

The per-stage detail behind every line above is §1–§8; this box is only the shape.

---

## Backend connection async lifecycle

One backend object participates in three state machines at the same time. They answer different
questions and must not be treated as aliases:

```
SESSION ──▶ PgSQL_Session::status
            └─ what logical work owns the request?
               CONNECTING_SERVER · PROCESSING_QUERY · PROCESSING_STMT_* · RESETTING_CONNECTION · …

STREAM  ──▶ PgSQL_Data_Stream::DSS
            └─ how is the backend socket attached and being driven?
               STATE_NOT_INITIALIZED · STATE_MARIADB_CONNECTING · STATE_READY ·
               STATE_MARIADB_QUERY · STATE_MARIADB_GENERIC · …

CONNECTION ──▶ PgSQL_Connection::async_state_machine
               └─ which nonblocking libpq operation is in progress?
                  ASYNC_CONNECT_* · ASYNC_QUERY_* · ASYNC_USE_RESULT_* ·
                  ASYNC_STMT_* · ASYNC_RESYNC_* · ASYNC_RESET_SESSION_* · ASYNC_IDLE
```

`PG_ASYNC_ST` is an alias of ProxySQL's shared `ASYNC_ST` enum
(`include/proxysql_structs.h:51-136`). The enum also contains names such as
`ASYNC_CHANGE_USER_*`, `ASYNC_SET_NAMES_*`, and `ASYNC_INITDB_*`; the PostgreSQL connection
handler does not implement those branches. Their presence in the shared enum does not make them
PostgreSQL backend lifecycle stages. The flows below list the states the PostgreSQL handler and its
session callers actually drive.

`ASYNC_IDLE` is the connection state machine's reusable rendezvous point: the backend is connected
and no connect, query, result fetch, resynchronization, or reset operation is active. It does **not**
mean "FREE in the shared pool." A USED connection can be `ASYNC_IDLE` while it is attached and
waiting for dispatch, retained by a transaction, or held for delayed multiplexing. Conversely, a
FREE connection is expected to be connected, reusable, and idle before another worker takes it.

### New connection: creation and connect

Creation registers an unconnected object as USED. TCP connect, TLS/authentication, startup-profile
negotiation, and PolarDB RFQ tracking then advance asynchronously outside the HGM/pool locks.

```
CREATE ──▶ new PgSQL_Connection                              Connection.cpp:391
           │    async state = ASYNC_CONNECT_START             Connection.cpp:415
           │
           ├─ ASYNC_CONNECT_START
           │    ├─ operation needs socket readiness → ASYNC_CONNECT_CONT → YIELD
           │    └─ operation completed immediately → ASYNC_CONNECT_END
           │
           ├─ ASYNC_CONNECT_CONT ↺                            Connection.cpp:588
           │    ├─ still pending → record required event → YIELD
           │    ├─ deadline reached → ASYNC_CONNECT_TIMEOUT
           │    └─ completed → ASYNC_CONNECT_END
           │
           └─ ASYNC_CONNECT_END                               Connection.cpp:601
                ├─ connected + startup accepted → ASYNC_CONNECT_SUCCESSFUL → ASYNC_IDLE
                ├─ connect/startup error       → ASYNC_CONNECT_FAILED     → failure handling
                └─ deadline exceeded           → ASYNC_CONNECT_TIMEOUT    → failure handling
```

Only the successful branch reaches `ASYNC_IDLE` (`async_connect()`, `Connection.cpp:2197`). A
failed or timed-out new object is not a reusable pooled connection; the session retry/error path
ultimately destroys it.

### Simple Query: dispatch, streaming result, and completion

The PolarDB consistency wrapper, when required, is already finalized before the connection enters
`ASYNC_QUERY_START`. Wrapper `SET` results and the user result share the same result-fetch loop, but
only the user-visible part is forwarded.

```
ASYNC_IDLE ──▶ install query bytes / finalized wrapper        Connection.cpp:2403
               └─ ASYNC_QUERY_START
                    ├─ send pending → ASYNC_QUERY_CONT → YIELD
                    ├─ send error   → ASYNC_QUERY_END
                    └─ sent         → ASYNC_USE_RESULT_START

ASYNC_QUERY_CONT ↺
    ├─ libpq still needs socket readiness → YIELD
    ├─ send/protocol error                → ASYNC_QUERY_END
    └─ query accepted                     → ASYNC_USE_RESULT_START

ASYNC_USE_RESULT_START ──▶ start one PGresult
                           └─ ASYNC_USE_RESULT_CONT ↺
                                ├─ wrapper SET result → consume; hide; next USE_RESULT_START
                                ├─ notice             → preserve/forward
                                ├─ row/result         → append or stream to client
                                ├─ more input needed  → YIELD
                                └─ terminal RFQ       → ASYNC_QUERY_END

ASYNC_QUERY_END ──▶ classify success/error → RequestEnd → async_free_result
                    ├─ clean/reusable → ASYNC_IDLE → KEEP or RETURN (§7)
                    ├─ repair allowed → ASYNC_IDLE → RESET_SESSION path
                    └─ broken/unsafe  → DESTROY
```

`ASYNC_QUERY_END` means that the connection-level query operation has ended; it is not itself the
reusable state. `RequestEnd()` calls `async_free_result()`, which clears per-query result ownership
and moves the connection back to `ASYNC_IDLE` (`Session.cpp:6754`, `Connection.cpp:2314`). A normal
PostgreSQL SQL error may still end on a synchronized, connected backend and be forwarded safely;
transport loss, unknown transaction state, or an incomplete protocol exchange can require retry,
reset, or destruction instead.

### Extended Protocol: Parse, Describe, Execute, and resynchronization

Extended messages use their own start/continue/end states. Result collection is shared with the
Simple Query lane. If an error leaves libpq pipeline state unsynchronized, ProxySQL sends Sync and
does not make the connection reusable until resynchronization completes.

```
ASYNC_IDLE ──▶ one Extended Protocol operation
               ├─ Parse    → ASYNC_STMT_PREPARE_START  → *_CONT ↺ → USE_RESULT → *_END
               ├─ Describe → ASYNC_STMT_DESCRIBE_START → *_CONT ↺ → USE_RESULT → *_END
               └─ Execute  → ASYNC_STMT_EXECUTE_START  → *_CONT ↺ → USE_RESULT → *_END
                                                                  Connection.cpp:1187

extended result error before backend ReadyForQuery
    └─ ASYNC_RESYNC_START                                         Connection.cpp:1083
         ├─ send Sync / flush pending → ASYNC_RESYNC_CONT ↺ → YIELD
         └─ completion
              ├─ synchronized → ASYNC_RESYNC_END → RequestEnd/cleanup → ASYNC_IDLE
              └─ failed       → ASYNC_RESYNC_END with error → DESTROY
```

Parse, Describe, and Execute can each end without ending the whole frontend Extended-Protocol
frame. Session ownership, pending frontend frames, and Sync boundaries therefore decide whether
the backend remains KEEP-attached even after the individual connection operation returns to idle.

### Reset and maintenance ping

Reset repairs an already-connected object in a helper session. It may first leave pipeline mode,
roll back an active transaction, and then issue `DISCARD ALL`; those phases can repeat the reset
start/continue sequence before the final result is known.

```
RESET ──▶ ASYNC_IDLE → ASYNC_RESET_SESSION_START
          │              ├─ pipeline active → Sync/exit pipeline
          │              ├─ transaction active → ROLLBACK
          │              └─ otherwise → DISCARD ALL
          │
          └─ ASYNC_RESET_SESSION_CONT ↺
               ├─ needs socket readiness → YIELD
               ├─ another cleanup phase  → ASYNC_RESET_SESSION_START
               └─ ASYNC_RESET_SESSION_END
                    ├─ clean    → ASYNC_RESET_SESSION_SUCCESSFUL → ASYNC_IDLE → RETURN
                    ├─ error    → ASYNC_RESET_SESSION_FAILED     → DESTROY
                    └─ deadline → ASYNC_RESET_SESSION_TIMEOUT    → DESTROY

PING ──▶ generic PostgreSQL core, when scheduled:
        ASYNC_IDLE → ASYNC_PING_START → ASYNC_PING_SUCCESSFUL → ASYNC_IDLE → RETURN
        the helper performs no backend I/O (`Connection.cpp:2548`); active PolarDB mode does not
        schedule these connection checks and keeps only the core pool-cleanup part
```

### Pool ownership is a separate lifecycle axis

The FREE/USED index says who owns the object; the async state says what protocol work it is doing.
The normal combined lifecycle is:

```
UNALLOCATED
    └─ CREATE → USED + ASYNC_CONNECT_* → ASYNC_IDLE
                  │
                  ├─ dispatch → QUERY/STMT/RESULT states → ASYNC_IDLE
                  │    ├─ KEEP   → remains USED and attached to the session
                  │    ├─ RETURN → USED → FREE under this server's pool_mutex
                  │    └─ RESET  → remains USED while helper runs → FREE only after success
                  │
FREE + ASYNC_IDLE ──▶ exact/reset acquisition → FREE → USED
                  │
                  └─ OFFLINE · disconnect · timeout · failed reset · unsafe protocol state
                       → remove from its current owner → DESTROY → UNALLOCATED
```

Destruction is not an async state and can terminate the object from any branch once ownership has
been removed safely. The destructor clears remaining results, closes libpq with `PQfinish()`, and
releases the connection object (`Connection.cpp:451`).

---

## The three PolarDB enable conditions

"PolarDB on" is not one switch — three **independent** facts enable different parts of the pipeline.
Keeping them separate explains the two different enable checks in the flows below.

| Condition | Meaning | What it enables |
|---|---|---|
| **global `polardb_active`** (`PgHGM->status.polardb_active`) | at least one PolarDB replication hostgroup is configured | whether the routing/policy machinery runs at all (§2) |
| **session `polardb_config.is_polardb_enabled`** | the request/session is operating on a PolarDB hostgroup/backend path; for a new backend this is set before the connect attempt completes | whether PolarDB result processing runs inside successful `RequestEnd` (§5) |
| **`client_rfq_lsn_requested`** (`polardb_route_state`, `Session.h:680`) | the client asked for LSN-aware replies at startup (§1) | whether the client may receive an extended RFQ LSN — and it also **disables the query cache** for that session (§2) |

---

## 0. The one invariant

```
routing ──▶ chooses a PREFERRED server
            │
            ├─ acquisition gets a connection from that server
            │    ordinary read      → that selected server only
            │    pooled-only txn read → may fall back to ANOTHER eligible server that
            │                           already has an exact match (never an ineligible
            │                           one, never by reset or create)
            ├─ the response is attributed to the final bound server; RFQ metadata
            │    supplies the LSN/XID evidence used for consistency accounting
            └─ one lifecycle path keeps / returns / resets / destroys it
```

Everything else is policy branching around this straight ownership pipeline. There is no second
ReaderPool inventory and no separate return manager: a connection lives in one server's shared
pool and is returned there by an exact key. The one deliberate softening of "selected server only"
is the **pooled-only transaction read** — when it cannot create or reset, it may take an exact
match from another *eligible* server, so pool availability can decide *which* eligible reader
serves it (`ReaderPool.cpp:1999`). Routing eligibility still bounds the set; the pool can never
promote an ineligible reader.

---

## 1. Connection startup

Once per frontend connection, before any query flows, ProxySQL authenticates the client and
records whether it asked for **LSN-aware replies**. The `client_rfq_lsn_requested` flag permits
the response path (§5) to append an LSN when the backend supplied trustworthy RFQ metadata. When
the backend supplied no RFQ LSN payload, ProxySQL sends a standard ReadyForQuery. The same flag
also disables query-cache use for the session (§2).

```
CLIENT ──▶ TCP / TLS accept
           │
           ├─ PostgreSQL StartupMessage → authenticate client
           ├─ consume the client PolarDB RFQ request      (_polar_send_lsn / _polar_proxy_send_lsn)
           ├─ remember client_rfq_lsn_requested           (client-visible RFQ LSN later — doc 09 §6)
           ├─ strip the proxy-only startup keys before normal startup handling
           └─ session ready
```

The backend-side RFQ negotiation (startup profile, `proxy_protocol` dialect, patched-libpq
LSN/XID tracking) happens later, on the connection that actually serves a query — see §3 and
[11-CONNECTION-AND-LIBPQ.md](11-CONNECTION-AND-LIBPQ.md).

---

## 2. Per-query request and route action

Every executable Simple Query request receives one routing decision for the whole request.
**Simple Query** and **Extended Protocol** are separate lanes (§2.1); this section diagrams the
Simple Query lane. Ordinary ProxySQL routing (PolarDB disabled, a locked hostgroup, or a manual
query-rule destination) is handled first. An automatic PolarDB read then runs
`observe → collect → plan → account → execute` to decide *where* the request goes and *whether*
it needs a consistency wait.

```
CLIENT ──▶ client 'Q' packet (Simple Query)
           │
           ├─ decode frame → query parser / digest / query rules
           ├─ initialize query lifecycle                              Session.cpp:2621
           │    · reset reader target · reset writer scope
           │    · reconcile any stale borrowed transaction reader
           │
           ├─ routing check                                           Session.cpp:2635 (polardb_active)
           │    ├─ PolarDB disabled            → normal ProxySQL hostgroup route
           │    ├─ locked hostgroup            → locked route
           │    ├─ explicit / manual dest      → manual route (+ txn reader-failure writer route)
           │    └─ automatic PolarDB route
           │         ├─ observe route inputs   polardb_observe_route_inputs  Flow.cpp:377
           │         ├─ collect route context  polardb_collect               Flow.cpp:468
           │         ├─ plan ONE route action  polardb_plan                  Flow.cpp:694
           │         ├─ account the decision   polardb_account_route_plan    Flow.cpp:1092
           │         └─ execute the action     polardb_execute               Flow.cpp:1259
           │
           └─ route action
                ├─ PASSTHROUGH           → query-rule / current hostgroup
                ├─ FORCE_PRIMARY         → bind writer backend, clear reader wait intent
                ├─ REPLICA (no wait)     → select eligible reader
                ├─ REPLICA_WITH_WAIT     → save query + target LSN/timeout/mode/fallback, select reader
                ├─ PRE-WRITE TXN READER  → retain txn writer, borrow reader, wait on session LSN   (06 C.12)
                └─ POST-WRITE TXN SPLIT  → retain txn writer, require XIDs + txn LSN, borrow reader,
                                           prepare XID + strict-LSN wrapper                         (06 C.11)
```

**Query cache is disabled for RFQ clients.** A client that requested RFQ LSN also disables the
query cache for its reads (`polardb_query_cache_disabled_for_current_rule`, `Flow.cpp:120`):
cached wire bytes could replay a stale or absent RFQ LSN and break the client's monotonic LSN
stream. This is a second effect of the `client_rfq_lsn_requested` condition, beyond LSN stamping.

> Note: `Flow.cpp:377` is the **observe/collect** entry (`polardb_observe_route_inputs`), not the
> planner. The planner is `polardb_plan()` at `Flow.cpp:694`. Route-action semantics and full
> scenario traces are in [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) §C.

### 2.1 Extended Protocol lane

Parse / Bind / Execute / Sync is routed on a **separate path** and never receives the text-mode
`SET` wrappers. A wrapped read must be one Simple Query request so the connection layer can count
and hide the prepended SET results. Extended-Protocol consistency that requires a wait is enforced
by routing to the writer, and the client-visible RFQ LSN appears only at **Sync**.

```
CLIENT ──▶ Parse / Bind / Execute / Sync
           │
           ├─ polardb_apply_extended_route()                          Flow.cpp:1399
           │    never injects a Simple-Query SET wrapper
           │    ├─ locked route                  → keep the locked hostgroup
           │    ├─ manual destination            → honor it unless a prior transaction-reader
           │    │                                  failure keeps the transaction on the writer
           │    ├─ eligible automatic read with no wait target
           │    │                                → planner may select a reader, without a wrapper
           │    └─ wait target or any other primary-only condition
           │                                     → FORCE_PRIMARY (writer)
           │
           └─ the client-visible RFQ LSN is emitted only at Sync, not per Execute
```

---

## 3. Reader selection, acquisition, and backend preparation

Three steps have different responsibilities and lock costs. **Selection** picks a preferred
server without an HGM or pool mutex (§8.1). **Acquisition** moves or creates one connection object
for a server (§8.2 and §8.4). **Backend preparation** then connects or repairs that object
asynchronously, outside the HGM and pool locks, until it reaches `ASYNC_IDLE`.

```
SELECT ──▶ pick a PREFERRED reader from the membership snapshot       ReaderPool.cpp:2104
           │    reject OFFLINE / excluded / lag-ineligible readers
           │    ├─ 2 readers  → deterministic weighted alternation
           │    └─ 3+ readers → direct two-reader sample for eligible equal-weight
           │                    ordinary reads; complete scan for special cases
           │
POOL   ──▶ build the exact PgSQL_PoolMatchKey
           │
           ├─ try an exact connection retained by this worker for the selected server
           ├─ shared existing-object phase, under selected server pool_mutex
           │    ├─ ordinary read   → ALLOW_EXACT_MATCH | ALLOW_RESET
           │    └─ pooled-only read → ALLOW_EXACT_MATCH only
           │
           ├─ validate every object after taking it
           │    unusable object → remove from USED and destroy; continue bounded scan
           │
           ├─ pooled-only exact miss
           │    → try exact matches on other ELIGIBLE readers          ReaderPool.cpp:1999
           │    → never reset and never create
           │
           └─ ordinary miss on the preferred server
                → ALLOW_EXACT_MATCH | ALLOW_CREATE on that server only ReaderPool.cpp:2400
                → HGM wrlock + selected server pool_mutex (§8.4)
                → create and register an unconnected object
           │
PREPARE ──▶ prepare the acquired object outside HGM/pool locks
           │    ├─ exact object → already connected and compatible
           │    ├─ reset object → asynchronous session-state repair
           │    └─ new object   → asynchronous TCP connect + authentication +
           │                     PolarDB startup profile + patched-libpq RFQ tracking
           │    → advances the backend async state machine until ASYNC_IDLE → §4
           │
           └─ acquisition outcome
                ├─ acquired and target already reached → use backend; bypass LSN wrapper
                ├─ acquired but target not proven      → use backend; retain wait wrapper
                ├─ RFQ unavailable, strict policy      → one-query writer redirect
                ├─ RFQ unavailable, best-effort policy → reader without wait + degradation notice
                ├─ reader temporarily busy             → normal ProxySQL retry/wait
                └─ pooled-only miss on all eligible    → do not create; optionally request bounded
                                                         warmup; use writer for this request
```

The worker-local exact phase uses no mutex and runs only after server selection. The shared phase
performs only list/index work under `pool_mutex`; connection repair occurs later. Cold creation
may evict and destroy an old FREE connection while holding HGM and `pool_mutex`, so that branch
can perform close-related cleanup under the locks. The new connection's TCP connect,
authentication, and startup negotiation still occur later and unlocked.

---

## 4. Wrapper construction and dispatch

`ASYNC_IDLE` means the acquired backend is connected, prepared, and ready for another query.
Ordinary consistency waits and pre-write transaction waits defer wrapper construction until this
state. Post-write transaction split is different: it uses a connected pooled-only reader and
builds its XID/LSN wrapper during route execution, immediately after acquiring that reader.

```
WRAP ──▶ ordinary consistency wait / pre-write transaction wait
         │
         ├─ route execute
         │    └─ save original query · target LSN · timeout · writer fallback
         ├─ acquire and prepare the backend
         └─ at ASYNC_IDLE: finalize_wait_timeout_injection() exactly once
              ├─ wait bypassed → original query
              ├─ wait required
              │    ├─ SET polar_consistency_mode
              │    ├─ SET polar_proxy_wait_timeout_ms
              │    ├─ SET polar_xact_split_wait_lsn
              │    └─ original query
              └─ build failure
                   ├─ send NO partial wrapper
                   ├─ mark waits disabled for this session
                   ├─ restore the retained writer when a reader was borrowed
                   └─ return a clean internal error

SPLIT ──▶ post-write transaction split
          │
          ├─ require retained live writer · transaction XIDs · transaction LSN
          ├─ acquire a connected pooled-only reader
          ├─ build the wrapper immediately during route execute             Split.cpp:575
          │    ├─ SET polar_xact_split_xids
          │    ├─ SET strict consistency mode / wait timeout / target LSN
          │    └─ original SELECT
          ├─ target already reached
          │    └─ omit the LSN-wait SETs, but still SET transaction XIDs
          └─ acquisition / wrapper-build failure
               ├─ release the temporary reader
               └─ route this request to the retained writer before dispatch

IDLE ──▶ prepared backend reaches ASYNC_IDLE
         │
         ├─ ordinary/pre-write wait → finalize the staged wrapper above
         ├─ transaction split       → wrapper is already built
         └─ install ordinary query timeout → async_query / PQsendQuery
```

---

## 5. Backend response loop, RequestEnd, finishQuery

The response is a **loop**, not one block. Backend messages arrive over many event-loop ticks;
visible rows may be **streamed to the client while the query is still running**, and only the
**terminal ReadyForQuery** carries the RFQ LSN/XID metadata. This is also why a retry is impossible
once any result byte has left: `result_started` is a hard barrier — you cannot un-send streamed
rows (§6).

```
BACKEND ──▶ result LOOP — messages arrive over many ticks (§ execution model)
            │
            ├─ internal wrapper SET result   → consume and hide                     Connection.cpp:775
            ├─ NoticeResponse                → preserve / forward (pending-notice queue)
            ├─ strict wait timeout / wrapper SET failure
            │                                → stop before visible user output → failure policy (§6)
            ├─ best-effort wait timeout      → queue warning/notice; user query may continue
            ├─ visible DataRows              → may STREAM to the client NOW (before the query ends)
            └─ terminal ReadyForQuery        → the ONLY message with RFQ LSN/XID metadata
                 ├─ build the client RFQ     polardb_client_ready_lsn  (doc 09 §6)
                 │    (standard RFQ if the client didn't request LSN or the backend gave none;
                 │     may RAISE to a proven target after a wait; may expose writer/session LSN)
                 ├─ drain any remaining result → client output queue                 Session.cpp:3940
                 ├─ update the ProxySQL transaction-state manager                    Session.cpp:3942
                 │
                 ├─ RequestEnd(success)                                              Session.cpp:3993 (body :6663)
                 │    └─ [success branch only, :6672] polardb_process_result         Session.cpp:6721
                 │         ├─ validate writer hostgroup + topology epoch
                 │         ├─ update selected-server cached LSN; session observed/write LSN
                 │         ├─ consume writer txn XID/RFQ state; update the split stage
                 │         └─ mark unknown state when required RFQ data is missing
                 │       · clear per-query PolarDB state · release buffers + saved packet
                 │
                 ├─ finishQuery                                                      Session.cpp:3994 (body :7134)
                 │    (KEEP / RETURN / RESET / DESTROY — see §7)
                 │
                 ├─ temporary transaction-reader completion (split / pre-write reader)
                 │    restore the retained writer as active backend  polardb_reset_txn_split_read  Split.cpp:682
                 └─ frontend output queue → nonblocking socket writeout
                      visible rows may have left before RequestEnd; final RFQ may leave later
```

`polardb_process_result()` runs **inside** the success branch of `RequestEnd()` — not a separate
step after it — and the result has already entered the client output queue by then.

---

## 6. Failure state machine

Failure handling is not one universal "error" branch. It first **captures immutable facts**, then
**classifies**, then **chooses an action** after checking the required safety conditions. The configured action per failure
kind (`retry` / `forward` / `terminate`) is a knob — the "defaults" below are defaults, not fixed.

```
FAILURE ──▶ backend failure or wrapper failure
            │
            ├─ CAPTURE backend outcome               polardb_capture_outcome        Failure.cpp:137
            │    connected? · reusable? · error? · timeout? · wrapper-SET failure?
            │    result already started? · backend endpoint?
            ├─ BUILD wait/split failure view
            │    preserve original query/packet · identify temporary reader
            ├─ transaction reader only
            │    └─ resolve writer state             polardb_resolve_writer_state   Failure.cpp:486
            │
            ├─ CLASSIFY                             polardb_reader_failure_kind_for  Failure.cpp:536
            │    CONNECTION_LOST · WAIT_TIMEOUT · REUSABLE_ERROR      (enum PgSQL_PolarDB.h:2836)
            │
            ├─ POLICY (per configured action)       polardb_reader_decision_for      Failure.cpp:548
            │    RETRY · FORWARD · TERMINATE          (enum PgSQL_PolarDB.h:2829; per-kind knob)
            │    dispatch: polardb_on_failure                                         Failure.cpp:184
            │
            └─ SAFETY CHECKS                        Failure.cpp:567
                 ├─ common       → result not started · original query/packet available
                 ├─ writer retry → writer hostgroup known · writer stream available and idle
                 └─ split retry  → LIVE writer or one eligible alternate reader · retry limit
```

Default action per kind (`lib/PgSQL_Thread.cpp:1284-1286`; each is configurable):

| Failure | Default | Override knob |
|---|---|---|
| Reader connection lost | `retry` | `pgsql-polardb_reader_death_action` |
| Reader wait timeout | `retry` | `pgsql-polardb_reader_timeout_action` |
| Reusable reader SQL error | `forward` | `pgsql-polardb_reader_error_action` |

### 6.1 Autocommit consistency-read failure

```
READER ──▶ reader failure before any visible result
           │
           ├─ remove the wrapped packet; clear wrapper / wait / notices
           ├─ return the reader only if connected, idle and clean
           ├─ policy = RETRY and all required conditions pass
           │    └─ rebuild ORIGINAL unwrapped packet → move to writer stream → dispatch once
           ├─ policy = FORWARD, or safe retry cannot be completed
           │    └─ forward the clean reader error through the normal path
           ├─ policy = TERMINATE
           │    └─ close the client session
           └─ result already started
                └─ never retry; a streamed row cannot be un-sent (§5)
```

### 6.2 Transaction-reader failure

The retry target depends on the **writer state** resolved at failure time
(`polardb_resolve_writer_state`, `Failure.cpp:486`):

```
READER ──▶ borrowed transaction reader fails
           │
           ├─ preserve the original client packet; record failed endpoint; split stage → primary  Split.cpp:682
           │
           ├─ writer state = LIVE       → retry may run on the existing writer transaction         Failure.cpp:494
           ├─ writer state = NOT_STARTED → writer HG known but no live writer txn backend yet       Failure.cpp:526
           │                              → RETRY cannot use an existing writer; FORWARD keeps
           │                                later reads on the writer; TERMINATE closes the session
           ├─ writer state = LOST       → transaction evidence exists but cannot be recovered       Failure.cpp:508
           │                              → TERMINATE the client session
           │
           ├─ PRE-WRITE transaction wait reader
           │    └─ RETRY targets the writer; it does not try another reader
           └─ POST-WRITE transaction-split reader
                ├─ connection lost + RETRY
                │    └─ try one OTHER reader once (exclude endpoint, rebuild wrapper)
                │         → if unavailable/fails again, try the LIVE writer          Failure.cpp:567
                ├─ wait timeout + RETRY
                │    └─ try the LIVE writer
                └─ reusable SQL error
                     ├─ default FORWARD; configured policy may RETRY or TERMINATE
                     └─ FORWARD keeps later transaction reads on the writer
```

A retry is forbidden after any result was exposed. Forwarded transaction errors carry
ReadyForQuery('T') so the client's transaction stays alive on the writer (06 C.15/C.16).

### 6.3 Generic ProxySQL fallback

```
EVENT ──▶ not a recognized PolarDB reader failure → existing ProxySQL logic
          │
          ├─ safe pre-result connection failure → reconnect / retry if allowed
          ├─ backend lost inside a transaction  → synthesize 25P02 + ReadyForQuery('E'),
          │                                        preserve client for ROLLBACK, or terminate
          ├─ ordinary reusable SQL error        → forward the backend error; retain / return if safe
          └─ OFFLINE / disconnected / corrupt   → destroy the backend connection
```

### 6.4 Timeout and RFQ-unavailable types

"Timeout" and "best-effort" are not one thing. Six distinct events have different owners, and
timeout and missing-target handling are separate policies:
`action_lsn_timeout` decides the client-visible outcome when a wait reaches its
deadline, while `action_missing_lsn` decides what routing does when a target
cannot be enforced at all.

| Event | Owner and action |
|---|---|
| Backend connect timeout | core async-connect retry / failure |
| Ordinary query timeout | core cancel / kill machinery |
| PolarDB LSN wait timeout, `action_lsn_timeout=warning` | backend best-effort mode; WARNING/notice + possibly stale result |
| PolarDB LSN wait timeout, `action_lsn_timeout=primary/error/disconnect` | backend strict mode; ProxySQL applies the configured fallback, error, or disconnect before any visible query result (§6.1) |
| RFQ unavailable, `action_missing_lsn = primary` | writer redirect *before* dispatch |
| RFQ unavailable, `action_missing_lsn = warning` | reader without a wait + a degradation notice |

---

## 7. Connection lifecycle: keep, return, reset, destroy

Across normal completion and exceptional cleanup, every backend connection takes one of four
outcomes. Normal successful requests usually enter this decision through `finishQuery`; failure,
disconnect, and session-reset paths can return, reset, or destroy a backend directly.

| Outcome | Conditions |
|---|---|
| **KEEP** (stay attached) | active transaction · sticky session state · multiplex disabled · pending Extended-Protocol frame · delayed multiplex |
| **RETURN** (worker pass or shared pool) | connected · reusable · idle · no transaction / pipeline; exact readers also require a rebuildable exact key |
| **RESET** (repair, then reuse) | lifecycle limit such as max age / max statements, or an explicit cleanup path that permits reset |
| **DESTROY** | server OFFLINE · disconnected · non-idle unsafe state · failed reset · invalid / unrebuildable exact key · explicit no-reuse |

For a keyed PolarDB reader RETURN, the selected server is already fixed. The worker may keep one
exact connection for the same server, startup generation, and key until the current event-loop
pass ends. That short-lived entry remains in core USED. A duplicate goes directly to the same
server's shared FREE list. Neither case can change routing, and neither takes the HGM lock:

```
RETURN ──▶ finishQuery: reusable AND idle                             Session.cpp:7185
           │
           ├─ PgSQL_Data_Stream::return_MySQL_Connection_To_Pool      Data_Stream.cpp:1621
           │    over a lifecycle limit? → RESET via helper instead    Data_Stream.cpp:1630
           │    detach from the session / backend stream
           │
           ├─ PgSQL_Thread::push_MyConn_local                         Thread.cpp:6423
           │    ├─ first exact tuple this pass → keep in worker array; stays core USED
           │    ├─ duplicate exact tuple → direct shared return
           │    └─ invalid or offline → remove and DESTROY
           │
           └─ end of worker pass: PgSQL_Thread::return_local_connections
                ├─ group retained exact connections by server
                ├─ LOCK one server pool_mutex
                ├─ return_matching_connection(): USED → FREE and exact index update
                └─ UNLOCK; then continue with the next server
```

Client disconnect and full session reset first restore or drop any borrowed transaction reader.
Because the frontend is leaving, no backend remains KEEP-attached: every attached backend becomes
RETURN, RESET, or DESTROY. Classic keyless connections use ProxySQL's classic return path; the
exact funnel above applies to keyed v2 reader connections. There are no fixed worker slots,
depth, refill, or idle-expiry settings.

---

## 8. Locking model: fast vs slow paths

The locking design keeps steady reader selection off the global HGM lock, confines warm pool
movement to one server, and reserves global locking for topology, creation, and maintenance
operations. It also includes shared atomic traffic that can affect cache coherence even when no
mutex is taken. The shorter ownership reference is
[51-READERPOOL-TRANSFER-AND-LOCKING.md](51-READERPOOL-TRANSFER-AND-LOCKING.md).

### 8.1 Fast path: SELECTION

**Purpose: pick a preferred replica from a stable membership/lifetime view without taking an HGM
or pool mutex.** Snapshot retrieval still reads the shared generation atomic and returns the
thread-local cached `shared_ptr` by value, which updates its reference count. With two readers,
every selection also updates the shared `selection_sequence`. Selection is mutex-free on the
steady path, not free of shared-memory traffic.

The snapshot keeps `PgSQL_SrvC*` objects alive and freezes hostgroup membership. Selection then reads
live server fields. Fast status, LSN, LSN timestamp, and used/free counts are atomic. `weight`,
`max_connections`, `current_latency_us`, and `max_latency_us` are plain fields with concurrent
update sites. This is a C++ data-race surface. Section 8.8 states the required ownership refinement.

**What a worker sees** — servers plus per-server *counters/fields*, never the connection objects:

```
ProxySQL ──▶ ONE immutable snapshot, versioned by a generation counter          HGM.cpp:1555
             │    snapshot[reader_hg] = { servers: [S1,S2,S3], selection_sequence }   HGM.h:1547
             │    (re)published under the HGM wrlock only when config changes — §8.5
             │
             ├─ worker 1 · worker 2 · … · worker N   (each caches it thread-locally; re-reads
             │    the shared_ptr ONLY when the generation moves — but by-value = refcount atomics)
             │
             └─ per server, the selector reads:
                  ├─ ATOMIC      : status · LSN · LSN age · pool_used_count · pool_free_count   HGM.h:319-320
                  └─ NON-ATOMIC  : weight · max_connections · current/max latency   (data race)  HGM.h:292-304
                  ── it does NOT see individual FREE/USED connection OBJECTS: those stay in each
                     server's private pool behind pool_mutex, touched only at GET (§8.2) ──
```

**Balancing differs by replica count:**

| Topology | Actual balancing |
|---|---|
| **2 readers** | **deterministic weighted alternation** via `selection_sequence.fetch_add(1)` (`ReaderPool.cpp:2139`). `active_count` is collected but does **not** decide between two healthy readers; availability and target-LSN preference may still choose the peer. |
| **3+ readers, equal-weight ordinary request** | sample two distinct healthy readers directly and keep the lower global active count; exact ties are random. |
| **3+ readers, special or unequal-weight request** | complete candidate scan with status, LSN, lag, exclusion, pooled-only, and weighted-load checks. |
| **pooled-only** | after an exact miss, may search other *eligible* readers (`ReaderPool.cpp:1999`). |

So for the common two-replica topology, active-count feedback does not choose between two healthy
readers; alternation is deterministic. The direct two-reader sample applies only to the eligible
three-or-more equal-weight ordinary path. Other requests use the complete candidate scan.

### 8.2 Warm path: GET

**Purpose: take one already-open connection object from the chosen server.** Once selection has
named a server, GET first checks the current worker for the same server, startup generation, and
exact key. A local hit needs no mutex because the connection remains in core USED. A miss checks
that server's shared FREE list under `pool_mutex`. Queries using different replicas do not contend
on the same mutex. Connect, reset, and authentication work happen later, outside this lock.

```
GET ──▶ get_local_polardb_reader_connection
        │    selected server + generation + exact key must match
        │    usable → return the existing USED connection without a mutex
        │
        └─ local miss → PgSQL_SrvC::take_existing_connection
             ├─ lock ONLY this server's pool_mutex
             ├─ ALLOW_EXACT_MATCH → move an exact-key conn FREE → USED
             ├─ ALLOW_RESET       → move a same user/db FREE conn to USED
             └─ return before any HGM write lock when an object was found
```

### 8.3 Warm path: RETURN

**Purpose: keep one reusable exact connection close for the remainder of the current worker pass,
then make it shared again.** Local retention happens only after routing selected the server. It
cannot create long-lived reader affinity because every retained entry is returned at pass end.
Duplicates go directly to the same server's shared pool.

```
RETURN ──▶ local_return_decision
           ├─ unusable / offline → REMOVE and close
           ├─ first exact tuple this pass → KEEP in worker array, still USED
           └─ duplicate or pass-end flush → return_connection_with_match_key
                ├─ rebuild and validate exact key
                └─ return_matching_connection under selected server pool_mutex
                     USED → FREE + exact index update
```

At pass end, same-server returns share one outer recursive mutex acquisition. Existing helpers
enter the same mutex while moving each connection, which is why changing to a plain mutex requires
an unlocked-helper refactor rather than only a type change.

### 8.4 Cold path: CREATE

**Purpose: build a new backend object when the selected server has no usable object.** Creation
changes global connection accounting and must revalidate topology, limits, and throttling under
the HGM write lock. The `PgSQL_Connection` object is created and registered under HGM and the
selected server mutex. TCP connect, authentication, and startup negotiation happen later through
the asynchronous state machine, outside those locks.

```
CREATE ──▶ get_connection_from_selected_server — ALLOW_CREATE branch  HGM.cpp:3717
           │
           ├─ wrlock()                      (creation changes global connection limits)
           ├─ re-validate status · hg · weight · latency · max_connections · throttle
           ├─ lock the server's pool_mutex                                          HGM.cpp:3727
           ├─ if capacity requires eviction: remove + destroy old FREE objects
           │    (close-related cleanup can therefore occur under both locks)
           ├─ create + register the connection OBJECT (unconnected)                 HGM.cpp:3740
           └─ wrunlock()   → then connect / auth / startup happen asynchronously → §3 prepare
```

### 8.5 Topology mutation and retirement

**Purpose: publish hostgroup membership changes while preserving server lifetime for active
selectors and active connections.** A topology change runs under the HGM write lock, builds a
new snapshot, swaps it atomically, and bumps the generation. Selection does not take HGM. Status
transitions and FREE-list draining also take the affected server's `pool_mutex`, so GET/RETURN can
briefly contend with topology work on that server.

```
COMMIT ──▶ polardb_update_server_list_snapshot_locked                HGM.cpp:1619
           │
           ├─ build the new membership snapshot
           ├─ retain the old snapshot until its shared references drain             HGM.cpp:1642
           ├─ tag removed servers with their retirement generation                  HGM.cpp:1610
           ├─ atomic_store(snapshot, release) → generation.store(release)            HGM.cpp:1649
           ├─ delete retired servers only before the oldest active generation       HGM.cpp:1577
           └─ status transition / FREE drain under affected server pool_mutex
                → same-server GET/RETURN may wait briefly
```

### 8.6 Warmup and maintenance locking

Warmup and maintenance run outside the steady routing decision, but they share HGM and per-server
pool state with query workers.

```
WARMUP REQUEST ──▶ query worker
                   │
                   ├─ build exact identity/profile request
                   ├─ deduplicate under warmup queue mutex
                   ├─ enforce queue bound
                   └─ enqueue + notify PgHGWarmup                              ReaderPool.cpp:343

PgHGWarmup ──▶ dedicated warmup executor thread                       ReaderPool.cpp:436
               │
               ├─ cap compatible FREE inventory per request
               │    default 1 · configurable 1..64 · existing compatible FREE counts
               ├─ distribute missing targets across eligible readers in rotating order
               │    (does not blindly create one connection on every reader)         ReaderPool.cpp:573
               ├─ reserve target/object under HGM wrlock + target pool_mutex
               ├─ release locks → connect/authenticate asynchronously in a batch
               └─ reacquire HGM wrlock + target pool_mutex
                    → revalidate membership/status/capacity → publish exact FREE object

MAINT  ──▶ idle-ping / purge / admin scans
           ├─ get_multiple_idle_connections holds HGM during its scan
           ├─ servers are inspected ONE server pool_mutex at a time
           ├─ the snapshot keeps each candidate server alive
           ├─ candidate connection pointer is searched again in that server's FREE list
           │    under pool_mutex; vanished/recently-used candidates are skipped
           └─ idle purge may destroy/close FREE connections while holding that server mutex

LOCK ORDER ──▶ allowed:   HGM wrlock → server pool_mutex
               allowed:   server pool_mutex only
               forbidden: server pool_mutex → HGM wrlock
```

### 8.7 Fast and slow lock summary

| Operation | Lock taken | Notes |
|---|---|---|
| **SELECTION** | **no HGM/pool mutex** on the steady path | generation atomic + `shared_ptr` reference count; two-reader path also updates the shared selection sequence; live plain selection fields require the ownership refinement in §8.8. |
| **GET local hit** | no mutex | selected server, generation, and exact key already match; connection remains USED. |
| **GET shared hit** | one server `pool_mutex` | FREE→USED of an object; connect/reset happen later, unlocked. |
| **RETURN keep local** | no mutex | at most one exact tuple for the current worker pass; connection remains USED. |
| **RETURN shared** | one server `pool_mutex`; pass-end returns are grouped per server | USED→FREE and exact index update; helper calls currently re-enter the recursive mutex. |
| **CREATE** | HGM `wrlock` then server `pool_mutex` | may evict/close old FREE objects under the locks; creates a new object; its connect/auth are asynchronous and unlocked. |
| **TOPOLOGY MUTATE** | HGM `wrlock` (+ per-server `pool_mutex` for status/drain/retire) | publish is lock-free for readers, but GET/RETURN may briefly contend (§8.5). |
| **WARMUP** | queue mutex on request; HGM + target server mutex while reserving/publishing | dedicated executor connects outside HGM; request size is bounded (§8.6). |
| **MAINTENANCE** | HGM + one server `pool_mutex` at a time | can briefly stall same-server GET/RETURN; idle destruction may close under the server lock (§8.6). |

### 8.8 Open issues and refinement points

```
SELECTION FIELD OWNERSHIP
─────────────────────────
current:
    snapshot owns membership + PgSQL_SrvC lifetime
    selector reads plain weight / max_connections / current_latency_us / max_latency_us
    configuration and monitor paths can update those fields concurrently

required refinement:
    snapshot immutable configuration fields: weight · max_connections · max_latency_us
    make dynamic current_latency_us atomic
    keep status · LSN · LSN age · used/free counts atomic

TWO-READER COHERENCE
────────────────────
current:
    one shared selection_sequence.fetch_add per routed read
    deterministic weighted alternation; active_count does not choose between healthy peers

before changing it:
    attribute the cache-line cost with perf-c2c before changing the algorithm
    any replacement must preserve deterministic weighted balance and must not use local connection inventory

SNAPSHOT REFERENCE COUNT
────────────────────────
current:
    each active reader connection owns a shared_ptr to the selected server-list snapshot
    acquire, return, batch return, and destroy can update the same shared reference count

possible cleanup:
    use move operations where ownership transfers and the source is no longer needed
    consider a raw pointer plus generation only if a worker-owned snapshot clearly keeps it alive
    measure shared cache-line traffic before changing the lifetime model

CAPACITY
────────
current:
    selection checks max_connections > 0
    new-object capacity is enforced during creation
    existing-object acquisition does not reject solely because used >= max_connections
    PolarDB_Pool_Capacity_Active_Block is declared but has no increment site

required observability:
    wire the counter only together with a defined v2 capacity-admission rule
    do not interpret the present zero value as evidence that capacity never binds

CANDIDATE SET
─────────────
current:
    the general reader selector keeps at most 32 eligible nodes in its stack array
    additional eligible nodes increment Reader_Node_Limit and are skipped

RETURN
──────
current:
    pass-end batching holds one server mutex while existing return helpers enter it again
    std::recursive_mutex makes that nesting valid

refinement:
    if profiling shows a material cost, add already-locked return helpers
    change to std::mutex only after every nested helper call is removed

SERVICE-RATE SIGNAL
───────────────────
current:
    eligible equal-weight 3+ ordinary reads compare two sampled active counts
    special and unequal-weight 3+ requests scan candidates and compare normalized load
    2-reader weighted alternation does not use active count or completion latency

implementation requirement for any latency-aware policy:
    preserve server-only selection inputs
    use sampling or per-worker shards before aggregation
    avoid one contended global atomic update on every completion

POSTGRESQL MAINTENANCE PING
───────────────────────────
current:
    async_ping changes ASYNC_IDLE → ASYNC_PING_START → ASYNC_PING_SUCCESSFUL → ASYNC_IDLE
    it performs no backend I/O, so the success result does not validate socket/backend liveness
    PolarDB and classic PostgreSQL currently run the inherited extraction/helper path

core boundary:
    a real nonblocking liveness check belongs in generic PostgreSQL core, not ReaderPool policy
```

#### 8.8.1 Heterogeneous reader capacity

The general 3+ reader path ranks candidates by weight-normalized active connection count. The
equal-weight ordinary fast path compares two sampled active counts directly. Neither uses
`max_connections` as a utilization denominator or rejects a server at selection solely because
`used >= max_connections`; new-object capacity is enforced later during creation.

For readers with materially different backend capacity, current configuration should express
their relative capacity through `weight`. A reader with approximately five times the sustainable
capacity should normally have approximately five times the weight. Weighted selection and
weight-normalized active count then direct more work to that reader. This is an operator-supplied
capacity model; ProxySQL does not derive weight from `max_connections`.

`max_connections` remains a creation limit, not a complete reader-utilization signal. Existing
object acquisition can still proceed without a selection-time `used/max_connections` rejection.
The declared `PolarDB_Pool_Capacity_Active_Block` counter has no increment site, so its
zero value does not show that capacity pressure is absent.

```
OPEN CAPACITY QUESTION
──────────────────────
selection today:
    2 readers  → weighted alternation; no active-capacity feedback
    equal-weight 3+ ordinary reads → compare two sampled active counts
    special or unequal-weight 3+ requests → complete scan + normalized active load

creation today:
    enforce max_connections while creating a new object

missing contract:
    define whether v2 selection should reject or penalize used >= max_connections
    define the exact status returned when capacity binds
    increment PolarDB_Pool_Capacity_Active_Block only at that defined boundary
    prove behavior with unequal-capacity and lowered-runtime-cap tests
```

#### 8.8.2 Heterogeneous query cost and service rate

`active_count` is a concurrency signal, not a direct service-rate measurement. If work completes
more slowly on one reader, connections remain active longer and its active count rises. The 3+
reader comparisons then steer some new work away, which provides delayed feedback, but they cannot
distinguish many short requests from fewer long requests and does not estimate the reader's
available throughput. The two-reader weighted-alternation path does not use this feedback at all.

A latency-aware policy requires a precise definition of the measured interval: backend execution,
consistency-wait time, queue/acquisition time, or complete proxy-observed request time. Mixing
those intervals would make the signal difficult to interpret. Updating one shared atomic EWMA on
every completion would also introduce a new coherence hotspot.

```
OPEN SERVICE-RATE QUESTION
──────────────────────────
define:
    which interval represents reader service time
    whether consistency-wait time is part of reader load or a separate signal
    how weight and active_count combine with the latency signal

preserve:
    selection uses server-wide state only
    worker-local connection inventory never selects a server
    two-reader balance remains deterministic unless a measured load signal overrides it

implementation boundary:
    collect completion data with sampling or per-worker shards
    aggregate without one global atomic write per query
    add the signal only after mixed-load and heterogeneous-reader tests show a benefit
```

#### 8.8.3 PostgreSQL pooled-connection maintenance ping

This is inherited PostgreSQL-core scaffolding: `async_ping()` reports success without backend I/O,
so it cannot validate a pooled socket. It is not part of ReaderPool selection, RFQ handling, LSN
waits, or transaction split.

- PolarDB and classic PostgreSQL currently keep the inherited idle-connection extraction and
  helper-session flow. The helper succeeds without sending a backend packet.
- The separate PostgreSQL server monitor remains active; a dead pooled connection is detected by a
  real request and handled by the normal failure path.
- Feature-off and non-PolarDB PostgreSQL behavior remain unchanged by ReaderPool.
- Any real asynchronous pooled-socket check should be completed as a generic ProxySQL PostgreSQL
  feature, not added to PolarDB policy.
