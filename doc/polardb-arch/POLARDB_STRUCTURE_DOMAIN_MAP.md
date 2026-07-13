# PolarDB Structure Domain: Map, Relations, and Normalization Audit

**Scope.** Every PolarDB type currently in the tree, how they group, how they
compose (parent ◆ child), how they flow through the per-query call chain, and a
normalization audit explaining where a repeated value is intentional and where a
future change should avoid adding duplicate state.

**Legend**
```
A ◆──▶ B     A embeds a B BY VALUE (composition / has-a)
A ┄┄▶ B      A holds a SNAPSHOT COPY of B (value copy across a boundary)
A ══▶ B      pipeline stage A PRODUCES B
A ··▶ B      A READS B (no ownership)
[enum]       enum class
{f,g}        plain data fields
() methods   behavior bound to the type
```

**Suffix vocabulary.** PolarDB type names use suffixes as a lifecycle contract:

| Suffix | Meaning | Current types |
|---|---|---|
| `Ctx` | Immutable input context collected for one stage. It bundles facts a stage reads; it is not mutable runtime state and not a stored decision. | `PolarDB_Query_RouteCtx` |
| `Plan` | Planner/helper output describing how to perform an action. It may be copied into runtime state if a later stage must execute the decision. | `PolarDB_Query_RoutePlan`, `PolarDB_Query_ReaderPlan`, `PolarDB_Query_WaitPlan` |
| `Spec` | Immutable shape/payload, with no runtime lifecycle and no routing decision. | `PolarDB_WaitSpec` |
| `Scope` | Identity/validity boundary for state derived under a topology epoch. | `PolarDB_WriterScope` |
| `State` | Mutable runtime workspace reset by named methods at a query/operation boundary. | `PolarDB_QueryState`, `PolarDB_Query_WaitState`, `PolarDB_Query_DispatchState`, `PolarDB_Query_WrapState` |
| `Result` | One-shot outcome returned by an operation. It is not retained as mutable runtime state. | `PolarDB_ReaderResult`, `PolarDB_Query_ExecuteResult`, `PolarDB_WrapFinalizeResult` |
| `Type` | Protocol/domain category with external semantic meaning. | `PolarDB_NodeType`, `PolarDB_WaitType` |
| `Kind` | Internal variant discriminator for a local state machine. | `PolarDB_Query_WrapperKind` |

`PolarDB_SessionConsistency` intentionally does not use the `State` suffix:
it is durable per-client session truth that survives query cleanup. `Ctx` is
reserved for immutable collected input; none of the current `Plan`, `State`,
`Spec`, or `Result` objects should use `Ctx`. `Type` stays reserved
for categories such as node/wait type; wrapper dispatch uses `Kind` because it
only selects an internal wrapper-consumption path.

`PolarDB_Query_` marks query-stage owners: route context, route plan, reader
plan, wait plan, wait state, query state, wrapper kind, and execute result.
Shared leaf values omit the infix even when query-stage owners embed them:
`PolarDB_WriterScope` and `PolarDB_WaitSpec` are deliberately not named
`PolarDB_Query_*`.

The header carries the same compact contract as the review guideline; this
document is the expanded map.

---

## 1. The domain at a glance: 4 ownership roots + 1 transient pipeline

Everything hangs off **three long-lived owners** (Session, Connection, HostGroups
Manager) plus **one request-stack-scoped pipeline**. Two leaf value types
(`WriterScope`, `WaitSpec`) are the shared atoms embedded across all of them.

```
┌──────────────────────────── SHARED LEAF VALUE TYPES (the atoms) ────────────────────────────┐
│  PolarDB_WriterScope {hg,epoch} valid()/matches()/reset()                                    │
│  PolarDB_WaitSpec    {type,target,timeout_ms,mode} has_wait()/reset()/lsn()                  │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
        ▲embedded            ▲embedded                         ▲embedded
        │                    │                                 │
╔═══════╪════════════════════╪═════════════╗   ╔═══════════════╪══════════════╗   ╔═══════════════════════════╗
║ PgSQL_Session (per client) │             ║   ║ PgSQL_Connection (per backend)║   ║ PgSQL_HostGroups_Manager  ║
║                            │             ║   ║                               ║   ║   (global, lock-free)     ║
║ polardb_config {…}         │             ║   ║ StartupProfile ◆─[ProxyProto] ║   ║ TopologySnapshot          ║
║                            │             ║   ║ StartupIdentity ◆─[IdSource]  ║   ║   ◆─ map<hg,HG_Config      ║
║ SessionConsistency ◆─WriterScope(writer) ║   ║ DispatchState ◆─[WrapperKind] ║   ║         ◆─ HG_Policy>     ║
║   {write_lsn,observed_lsn,               ║   ║ WrapState     ◆─[WrapperKind] ║   ║ PgSQL_SrvC.polardb_*_lsn  ║
║    write_unknown,observed_unknown}       ║   ╚═══════════════════════════════╝   ║ status.* (227 counters)  ║
║                                          ║                                       ╚═══════════════════════════╝
║ QueryState ◆──WriterScope(request_scope) ║
║   ◆──ReaderPlan  ◆──WaitState◆WaitSpec   ║          PER-QUERY PIPELINE (transient, request-stack only)
║   {wrapped_query_buf,dispatch_wrapper_*} ║          ───────────────────────────────────────────────
╚══════════════════════════════════════════╝          RouteCtx ══plan()══▶ RoutePlan ══execute()══▶ ExecuteResult
                                                       ◆WriterScope         ◆WaitSpec                {final_hg,
                                                       ◆SessionConsistency  ◆ReaderPlan               executed_action}
                                                       (snapshot ┄┄from Session)  +[RouteAction]/[Reason]
                                                                            │
                                              get_MyConn_polardb_reader(◆ReaderPlan) ══▶ ReaderResult ◆[ReaderStatus]
```

Two things to notice immediately:
- **`SessionConsistency` and the two leaf atoms are the only types that cross the
  Session↔pipeline boundary** — `RouteCtx.session` is a *value snapshot* of the session's
  `SessionConsistency` (taken once in `collect`), so the planner never touches live session state.
- **`QueryState` is the busiest hub** (6 fields, including 3 structured state objects). It's the per-query workspace;
  the pipeline's `RoutePlan` outputs are copied *into* it at `execute`.

---

## 2. Per-group charts

### 2.1 Leaf value atoms (embedded everywhere)
```
PolarDB_WriterScope                         PolarDB_WaitSpec
 {int hg; uint64 epoch}                      {WaitType type; uint64 target;
 valid() matches(other) reset()               uint32 timeout_ms; WaitMode mode}
                                              has_wait()  reset()  static lsn(lsn,timeout,mode)
 embedded in (3):                            embedded in (3):
   SessionConsistency.writer_scope             WaitPlan.spec
   QueryState.request_writer_scope             WaitState.spec
   RouteCtx.writer_scope                       RoutePlan.wait_spec
```
These are shared definitions: one definition each, reused at every site.
**Correctly normalized — do not split or duplicate.**

### 2.2 Per-session state (owned by PgSQL_Session)
```
PgSQL_Session
 ├─ polardb_config            {hg_min_* tri-states; session_consistency_mode}      (config override)
 ├─ PolarDB_SessionConsistency        ◆──▶ WriterScope (writer_scope)              (LIVES for the session)
 │     {write_lsn, observed_lsn, write_unknown, observed_unknown}
 │     target()=max(write,observed)   target_with_baseline(baseline,primary,&unk)
 │     reset_lsn_state()   has_lsn_state()
 └─ PolarDB_QueryState                                                            (RESET each query)
       ├─ WriterScope request_writer_scope        ◆──▶ WriterScope
       ├─ ReaderPlan reader_plan                  ◆──▶ ReaderPlan
       ├─ WaitState wait                          ◆──▶ WaitState ◆──▶ WaitSpec
       ├─ std::string wrapped_query_buf
       └─ {dispatch_wrapper_stmts, dispatch_wrapper_kind:[WrapperKind]}
       reset_reader_target() | reset_wait() | reset_dispatch_wrapper() | reset_for_new_query()
```
The session/query split (per-session truth vs per-query workspace) is the central
normalization of the whole domain and is correct.

### 2.3 Per-query pipeline (transient input -> decision -> effect)
```
        collect()                       plan()                         execute()
Session ─────────▶ RouteCtx ═══════════════════▶ RoutePlan ═══════════════════▶ ExecuteResult
HostGroups Manager    │                              │                              {int final_target_hg;
   snapshot ┄┄┄┄┄┄┄┄┘ │                              │                               RouteAction executed_action}
   RouteCtx {                          RoutePlan {
     ◆ WriterScope writer_scope          ◆ WaitSpec  wait_spec       ─┐ same LSN as
     ┄ SessionConsistency session        ◆ ReaderPlan reader  ───────┘ reader.consistency_target_lsn (§5.3)
     7×int (timeouts/modes/baseline/      int target_hg
            reader_hg/lag/mode)           [RouteAction] action          PASSTHROUGH|REPLICA_WITH_WAIT|FORCE_PRIMARY
     6×bool (hg/eligible/txn/             [RouteActionReason] action_reason
            multistmt/extended/hint)      bool degraded_rfq_route
   }                                      force_primary() rfq_unavailable()   (factories)
                                        }
                                                  │ backend acquisition
                                                  ▼
                          get_MyConn_polardb_reader(◆ ReaderPlan) ═══▶ ReaderResult
                                                                       {PgSQL_Connection* conn;
                                                                        PgSQL_SrvC* srv;
                                                                        [ReaderStatus] status;
                                                                        bool wait_bypass_allowed}  acquired()
                                                                       (bypass set only when acquired from
                                                                        the target-reached prefix → wrapper skipped)
```
Clean input→decision→effect triple. `RouteCtx` is pure input (immutable after collect),
`RoutePlan` is the sealed decision, `ExecuteResult`/`ReaderResult` are the effects.

### 2.4 Wait sub-domain (the SET polar_xact_split_wait_lsn machinery)
```
                 PolarDB_WaitSpec  {type,target,timeout_ms,mode}        ← THE shape (leaf)
                   ▲                 ▲                  ▲
        ┌──────────┘                 │                  └────────────────┐
   WaitPlan ◆ spec               RoutePlan ◆ wait_spec             WaitState ◆ spec
   + route_hint:[RouteHint]      (decision carrier)                + wait_stage:[WaitStage]
   build_consistency(mode,lsn,                                     + wrapper_stmts
     timeout,mode,prefer)                                          + wait_started_at_us
   = the consistency-helper OUTPUT                                 + wrapper_finalized
                                                                   + timeout_error
                                                                   + fallback_writer_hg
   (transient, used in plan())                                     + std::string original_query
                                                                   prepare_from_spec(WaitSpec)
   spec ──────────────────────────────────────────────────────────▶ spec   (execute copies)
```
**WaitSpec = immutable shape; WaitState = shape + runtime.** Runtime fields stay
out of the spec. `WaitPlan` = `WaitSpec` + `route_hint` is the
consistency-helper's return contract.

### 2.5 Reader sub-domain (freshness/lag selection)
```
PolarDB_Query_ReaderPlan  {consistency_target_lsn, primary_lsn, max_lag_bytes,
                           fallback_writer_hg, route_rfq_policy, allow_best_effort_degrade}
   has_consistency_target_lsn()  lag_cap_enabled()
   reader_lsn_reaches_consistency_target(reader_lsn)  within_byte_cap(reader_lsn)
   │ embedded in (2): QueryState.reader_plan  (persisted)  +  RoutePlan.reader  (decision)
   ▼ consumed by
PgSQL_HostGroups_Manager::get_MyConn_polardb_reader(hid, sess, ReaderPlan, WaitSpec, only_pooled)
   ▼ produces
PolarDB_ReaderResult {conn, srv, selected_server_snapshot, status:[ReaderStatus(8 vals)], wait_bypass_allowed:bool}
   acquired()  →  redirects_to_writer?   wait_bypass_allowed → wrapper skipped?
```
`ReaderPlan` owns its selection predicates (the lag/freshness methods) — selection policy
lives with the selection inputs. Appearing in both `RoutePlan.reader` (output) and
`QueryState.reader_plan` (persisted copy) is a lifecycle copy, **not** redundancy.
Selection keeps the readers whose fresh cached LSN already reaches the target as a
contiguous prefix; when a reader is acquired from that prefix, `ReaderResult.wait_bypass_allowed`
is set so the session can clear the staged wait and skip the wrapper
(`PolarDB_Wait_Wrap_Bypassed`). For any other reader the flag stays false and the wait
wrapper remains the correctness enforcement.

### 2.6 Connection sub-domain (startup identity + wrapper consumption)
```
PgSQL_Connection
 ├─ PolarDB_StartupProfile   {protocol:[ProxyProtocol], request_bits}
 │      has_rfq_lsn()  requests(bit)  static from_protocol()
 ├─ (resolve_polardb_startup_identity →) PolarDB_StartupIdentity
 │      {std::string host; int port; source:[StartupIdentitySource]}
 │      host_is_wildcard()/host_is_ip()/valid(reject_wildcard)   + null-safe ctors
 ├─ PolarDB_Query_DispatchState {wrapper_stmts, wrapper_kind:[WrapperKind]}
 │      reset()    // session-to-connection handoff, consumed by query_start()
 └─ PolarDB_Query_WrapState  {was_wrapped, stmt_failed, stmt_total, stmt_pending,
                                  wrapper_kind:[WrapperKind]}
        begin(n,kind) mark_wrapper_set_failed() clear() has_pending() is_consistency_wait()
```
Self-contained on the connection. `PolarDB_Query_DispatchState` is the short-lived
session-to-connection handoff. `WrapState` is the connection-side mutable
consumer for wrapper result sets; the Session prepares the wrapper, dispatch
state transfers the count/kind, and the Connection consumes the prepended SET
results before exposing the user result.

### 2.7 HostGroups Manager sub-domain (topology + per-server LSN cache)
```
PgSQL_HostGroups_Manager
 ├─ std::shared_ptr<const PolarDB_TopologySnapshot>   (generation-versioned, lock-free read)
 │     PolarDB_TopologySnapshot {uint64 generation; unordered_map<hg, HG_Config>}
 │       PolarDB_HG_Config {is_polardb_hostgroup, writer_hg, reader_hg,
 │                          policy:◆HG_Policy, primary_lsn:shared_ptr<atomic<u64>>,
 │                          writer_epoch:shared_ptr<atomic<u64>>}
 │         PolarDB_HG_Policy {consistency_mode, lsn_wait_timeout_ms, max_lag_bytes, proxy_protocol}
 ├─ PgSQL_SrvC.polardb_current_lsn : atomic<u64>          (per-server replica LSN)
 │   PgSQL_HGC.repl_config {polardb_primary_lsn, polardb_writer_epoch, polardb_writer_identity}  (writer-HGC policy snapshot)
 └─ status.* : 227 exported counters (PolarDB_* — the external metrics contract)
```
`HG_Policy` (raw tri-state config) ◆ inside `HG_Config` (resolved topology) ◆ inside
`TopologySnapshot` (the versioned bundle) is a clean 3-level nesting matching
config→resolution→snapshot. Correctly normalized.

### 2.8 Enum inventory (13 top-level + 2 nested, all `enum class`)
```
Routing/consistency : ConsistencyMode · RouteAction* · RouteActionReason* · ConsistencyRouteHint
Wait                : WaitType · WaitMode · WaitStage
Reader              : ReaderStatus(8) · RfqRoutePolicy · SessionLsnBaseline
Connection/startup  : ProxyProtocol · StartupIdentitySource · WrapperKind
Health/wrap         : NodeType · WrapFinalizeResult
                      (* = nested in RoutePlan)
```

---

## 3. Composition matrix (who embeds whom, by value)

| Container ↓  embeds →     | Writer&shy;Scope | Wait&shy;Spec | Reader&shy;Plan | Wait&shy;Plan | Wait&shy;State | Session&shy;Consist | HG&shy;Policy |
|---------------------------|:----:|:----:|:----:|:----:|:----:|:----:|:----:|
| SessionConsistency        | ◆(writer) |   |   |   |   | — |   |
| QueryState                | ◆(req) |   | ◆ |   | ◆ |   |   |
| RouteCtx                  | ◆ |   |   |   |   | ┄(snap) |   |
| RoutePlan                 |   | ◆ | ◆ |   |   |   |   |
| WaitPlan                  |   | ◆ |   | — |   |   |   |
| WaitState                 |   | ◆ |   |   | — |   |   |
| HG_Config                 |   |   |   |   |   |   | ◆ |
| TopologySnapshot          |   |   |   |   |   |   | ◆(via HG_Config map) |

`◆` value member · `┄` snapshot copy. Note every column with ≥2 `◆`
(`WriterScope`, `WaitSpec`, `ReaderPlan`) is a *deliberately* shared type — that's good
normalization, not duplication.

---

## 4. Flow / call-chain (which structs are touched at each stage)

```
 STAGE            FUNCTION                         READS                         WRITES / PRODUCES
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 1 collect        polardb_collect()                Session.polardb_config        RouteCtx (incl. ┄session,
                                                    Session.SessionConsistency      ◆writer_scope, resolved
                                                    HostGroups Manager snapshot    ints/bools)
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 2 plan           polardb_plan(RouteCtx)           RouteCtx.session.target*()    RoutePlan (◆WaitSpec ◆ReaderPlan
                  ├ WaitPlan::build_consistency()  HostGroups Manager primary LSN action/reason/target_hg)
                  ├ ReaderPlan lag methods         RouteCtx.* policy
                  └ force_primary()/rfq_unavailable()
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 3 execute        polardb_execute(RoutePlan)       RoutePlan.wait_spec/.reader        ExecuteResult; and COPIES into
                                                                                  QueryState: reader_plan,
                                                                                  wait(prepare_from_spec)
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 4 acquire        get_MyConn_polardb_reader()      QueryState.reader_plan        ReaderResult{conn,srv,status,
   (HostGroups    ↳ per-status → reader | writer-   PgSQL_SrvC.polardb_current_lsn   wait_bypass_allowed}
    Manager)        redirect | degrade | retry                                      (status drives caller:
                  ↳ reader from target-reached prefix                               use / redirect_to_writer;
                    (wait_bypass_allowed) → reset_wait();                           wait_bypass_allowed → skip wrap)
                    Wait_Wrap_Bypassed++
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 5 wrap           finalize_wait_timeout_injection() QueryState.wait (WaitState)  wrapped_query_buf;
   (skipped when  ├ build_polar_consistency_mode_set(spec.mode)                    QueryState.dispatch_wrapper_
    stage 4       └ build_wrapped_wait_query()                                     {stmts,kind}; later copied to
    bypassed →                                                                     PolarDB_Query_DispatchState, then
    emits nothing)                                                                 WrapState.begin()
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 6 process_result polardb_process_result()         RFQ LSN; QueryState.request_  Session.SessionConsistency
                                                    writer_scope; HostGroups        .{write_lsn|observed_lsn|
                                                    Manager scope
                                                                                    *_unknown};
                                                                                  HostGroups Manager
                                                                                  polardb_update_server_lsn()
 ───────────────  ───────────────────────────────  ────────────────────────────  ───────────────────────────
 7 cleanup        __cleanup / reset()              —                             QueryState.reset_for_new_query()
```
The loop closes at stage 6: positioned RFQ results feed `SessionConsistency` (the per-session
truth), which stage 1 snapshots into the next query's `RouteCtx`. **`SessionConsistency` is the
only state that survives a query; everything in `QueryState` and the pipeline is per-query.**

---

## 5. Normalization audit: redundant relations

The domain is, overall, **correctly normalized**:
the leaf atoms are shared, the session/query split is clean, and the pipeline is
a non-redundant input→decision→effect chain.

### 5.1 The session-target LSN exists in two fields: acceptable
At `REPLICA_WITH_WAIT`, the same LSN is `RoutePlan.wait_spec.target` **and** `RoutePlan.reader.consistency_target_lsn`
(set equal in Flow.cpp). Execute copies `RoutePlan.wait_spec` into `QueryState.wait.spec`
and `RoutePlan.reader` into `QueryState.reader_plan`. That leaves two consumers:
the **wait spec** drives the `SET ... wait_lsn` wrapper, while the **reader plan**
drives backend freshness/lag filtering. Those are different consumers at different
stages, so one shared value with two views is the right shape. The invariant is:
`RoutePlan.wait_spec.target == RoutePlan.reader.consistency_target_lsn`.

### 5.2 Correctly normalized: keep these shapes
| Looks like duplication | Why it's correct |
|---|---|
| `ReaderPlan` in both `RoutePlan.reader` and `QueryState.reader_plan` | output vs persisted copy across the plan→state boundary (different lifecycles) |
| `WaitSpec` in WaitPlan / WaitState / RoutePlan | one shared shape at three stages; merging would re-scatter the wait fields |
| Two `WriterScope` in `RouteCtx` (`session.writer_scope` + `writer_scope`) | the failover signal: "what the LSN is bound to" vs "current group" — their mismatch is load-bearing |
| `route_rfq_policy`/`max_lag_bytes` in `RouteCtx` and `ReaderPlan` | resolved config flowing input→plan; the plan must be self-contained at acquisition time |
| `DispatchState` (Connection) vs `WrapState` (Connection) | handoff vs consumer: dispatch state transfers count/kind from Session; WrapState counts down backend results |
| `WrapState` (Connection) vs `WaitState` (Session) | two sides of the wire: Session *prepares* the wrap, Connection *consumes* the result sets |
| `wrapper_stmts` in WaitState and `dispatch_wrapper_stmts` in QueryState | Session→Connection hand-off copy at send time (layer boundary) |

### Verdict
**Not over-normalized.** The structure count matches the number of real lifecycle
stages and ownership boundaries; the value-type sharing is deliberate and
load-bearing. `QueryState` has six fields:
```
QueryState:
    request_writer_scope
    reader_plan
    wait
    wrapped_query_buf
    dispatch_wrapper_stmts
    dispatch_wrapper_kind
```
