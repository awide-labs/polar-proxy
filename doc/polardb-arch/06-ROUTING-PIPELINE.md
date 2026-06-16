# PolarDB Routing Pipeline — Flow Reference (v2)

> Flow-reference view of the LSN-only routing pipeline.
> This document mirrors the section layout and diagram style of the full implementation
> `20-PIPELINE-FLOW.md`, rewritten for the LSN-only feature with verified
> final-tree line numbers. It is a companion to the prose-style
> [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) — same facts, different format
> (compact stage IDs, boxed per-stage detail, and end-to-end ASCII traces).
>
> Source of truth: `lib/PgSQL_PolarDB_Flow.cpp` (the four stage functions),
> `lib/PgSQL_Session.cpp` (orchestration and the response path),
> `lib/PgSQL_PolarDB_Wrap.cpp` (the single wrap point),
> `lib/PgSQL_Connection.cpp` (inline SET filtering),
> `lib/PgSQL_PolarDB_Notices.cpp` (timeout-notice capture).
>
> Verified against this branch.

---

## 0. Compact Stage IDs

```
Request path  (client → backend):
  [consistency_target_lsn reset] → [polardb_active?] → [manual-mode?] → collect → plan → execute
                       → backend_bind → wait_finalize → dispatch

Response path (backend → client):
  set_filter → wire (forward notice + result) → request_end → [is_polardb_enabled?] → process_result → writeout
```

**Fast-bypass gates (so a non-PolarDB query pays almost nothing):**
- `polardb_active` (`std::atomic<bool>` on `PgHGM->status`): skips the entire
  collect / plan / execute pipeline when no PolarDB hostgroup is configured.
  A non-PolarDB query pays one relaxed atomic-bool load
  (`lib/PgSQL_Session.cpp:2543`; the flag is set at `lib/PgSQL_HostGroups_Manager.cpp:1864`).
- `is_polardb_enabled` (session bool): skips the `polardb_process_result()` call on the
  response path for any session that never connected to a PolarDB hostgroup
  (`lib/PgSQL_Session.cpp:6102-6103`).

**Always-on safety, even before the gate:**
- The per-query reader plan and writer scope are reset unconditionally at the very
  top of the handler, before the `polardb_active` check, so a stale LSN can never
  leak into a passthrough read (`polardb_query.reset_reader_target(); polardb_query.request_writer_scope.reset();`,
  `lib/PgSQL_Session.cpp:2534-2535`). `reset_reader_target()` resets the whole reader
  plan (consistency_target_lsn, primary_lsn, max_lag_bytes, fallback_writer_hg,
  route_rfq_policy, allow_best_effort_degrade), not just consistency_target_lsn.

> Difference from the full implementation: that tree's pipeline also has a
> `split_finalize` step on the response path and a `REPLICA_TXN_SPLIT` action on
> the request path. Neither exists in the LSN-only tree — an in-transaction read
> routes to the writer instead (see scenario C.5). Transaction split is
> future work; see [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md).

---

## A. Request Path — Detailed Stages

```
Stage 0  FAST BYPASS + PER-QUERY RESET                    Session.cpp:2534,2546,2550
  Domain:   Session orchestration
  Reset:    polardb_query.reset_reader_target() + request_writer_scope.reset()  (always, before the gate)   :2534-2535
  Guard:    if (PgHGM->status.polardb_active.load(relaxed))                    :2546
            If no PolarDB hostgroup is configured, skip Stages 1-3 entirely.
  Manual:   bool manual_mode = polardb_manual_route_scope(...)  (== replica_eligible < 0 && dest >= 0)   :2550-2552
            If a query rule pinned a destination and left replica_eligible
            unset, the user is doing manual routing — skip the pipeline and
            leave current_hostgroup as the rule set it (see E.5, scenario C.9).

Stage 1  COLLECT                                          Flow.cpp:233
  Domain:   Session + HGM snapshot; may repair session epoch state
  Function: polardb_collect(route_ctx, current_hg, qpo_replica_eligible, force_primary_hint)
  What:     Zero-init PolarDB_Query_RouteCtx, then snapshot every routing input:
            is_polar_hg, writer_hg/reader_hg, effective_consistency_mode,
            wait_timeout_ms/mode, route_rfq_policy, session_lsn_baseline,
            max_lag_bytes, writer_scope, session.write_lsn, session.observed_lsn,
            write/observed missing-LSN latches, replica_eligible,
            is_multi_statement (own semicolon scan, eligible reads only),
            is_extended_protocol, in_transaction, force_primary_hint.
            If the writer_epoch differs from the session's stored epoch, collect
            reseeds the writer scope and clears session write/observed LSNs
            and both missing-LSN latches before copying them into route_ctx. It
            increments PolarDB_Session_Target_Epoch_Reset only if at least one
            target or latch was actually discarded.
  Fast exit: if the current HG is not a PolarDB HG, return immediately          :51-57
  Output:   PolarDB_Query_RouteCtx (request stack, immutable after collect)

Stage 2  PLAN                                             Flow.cpp:410
  Domain:   Pure decision (no mutations; one read of cached HGM LSNs for lag)
  Function: polardb_plan(route_ctx) → PolarDB_Query_RoutePlan
  What:     A flat, strictly top-down sequence of if-checks. First match returns.
              L-1  Fast paths       !is_polar_hg / reader_hg<0 / !replica_eligible
              L0   Hint override    force_primary_hint → FORCE_PRIMARY
              L1   Mode decisions       PRIMARY_ONLY → FORCE_PRIMARY ; OFF → PASSTHROUGH
              L2   Query-shape      in_txn / multi-stmt → FORCE_PRIMARY
              L3   Missing target   write/observed/primary LSN unknown → route_rfq_policy
              L4   Consistency      target=max(write_lsn, observed_lsn), or primary baseline
              L5   No-target path   target==0 → PASSTHROUGH to reader, no wait        (Flow.cpp:569-577)
              L6   Extended proto   is_extended_protocol → FORCE_PRIMARY (EXTENDED_PROTOCOL)   (Flow.cpp:580-592)
              L7   Lag-cap inputs   polardb_reader_lag_plan() attaches primary_lsn/max_lag_bytes
                                    to plan.reader (NOT a plan FORCE_PRIMARY); the per-reader cap is
                                    enforced at backend acquisition, where over-cap → writer fallback
                                    counted as PolarDB_Consistency_Writer_Fallback (see C.8)   (Flow.cpp:595-597)
              L8   Otherwise        REPLICA_WITH_WAIT (target = reader)                (Flow.cpp:599-609)
  Output:   PolarDB_Query_RoutePlan { action, target_hg, wait, action_reason, ... }

Stage 3  EXECUTE                                          Flow.cpp:658
  Domain:   Session side effects (intent only — no packet rewrite here)
  Function: polardb_execute(plan, route_ctx, pkt) → PolarDB_Query_ExecuteResult
  What:     Always reset the per-query wait state first (:385-386). Then per action:
              PASSTHROUGH       → return plan.target_hg unchanged
              FORCE_PRIMARY     → final_target_hg = route_ctx.writer_hg
              REPLICA_WITH_WAIT → validate, then STAGE the wait:
                                  set polardb_query.reader_plan.consistency_target_lsn = plan.wait_spec.target,
                                  bump polardb_session_lsn_routing,
                                  fill PolarDB_Query_WaitPlan, prepare_from_spec(plan.wait_spec),
                                  wait_stage = WAITING, wait_started_at_us = now,
                                  snapshot the user query text,
                                  bump polardb_wait_wrap_prepared.
                                  (The wrapper SQL is NOT built here.)
  Runtime writer fallback: malformed packet (pkt.size<7, :408) or polardb_wait_disabled
                       latch set (:422) → override to FORCE_PRIMARY (writer).
  Output:   PolarDB_Query_ExecuteResult { final_target_hg }

Stage 4  BACKEND BIND  (+ reader acquire / wrap-bypass branch)   Session.cpp:2570, 5726-5805
  Domain:   Session orchestration
  Function: find_or_create_backend(current_hostgroup)
  What:     Get or create the backend (mybe) for the final target HG. For a
            pooled or fresh PolarDB connection, set is_polardb_enabled = true
            (Session.cpp:5696 pooled, :5710 fresh). Init the query on the data
            stream. The original (unwrapped) packet still sits on the stream.
  Wrap bypass branch (only when reader_plan.has_consistency_target_lsn()):
            If the acquired reader is ALREADY at the consistency target, the
            staged wait is a no-op, so it is cleared here and Stage 5 wraps
            NOTHING. Two paths:
              (a) thread-local cache hit: get_MyConn_local_polardb_reader()
                  returns a cached RFQ-capable backend at target →
                  reset_wait() + reset_reader_target();
                  wait_wrap_bypassed++ , tl_cache_bypassed_for_target++ ;
                  trace "PolarDB WRAP BYPASS: thread-local reader reached
                  consistency_target_lsn=...".
              (b) route-smart: get_MyConn_polardb_reader() returns acquired with
                  wait_bypass_allowed (from the fresh target-reached prefix) →
                  reset_wait(); wait_wrap_bypassed++ ; PolarDB_Target_LSN_Preferred++ ;
                  trace "PolarDB WRAP BYPASS: route-smart reader reached
                  consistency_target_lsn=...".
            Any OTHER acquired reader keeps the staged wait — the wrapper is the
            correctness gate (PolarDB_Target_LSN_Fallback_Wait). See E.3.

Stage 5  WAIT FINALIZE (single wrapping point)            Wrap.cpp:303  (called Session.cpp:3607)
  Domain:   Session + packet rewrite
  Function: finalize_wait_timeout_injection(conn, myds)
  What:     After the connection is established (ASYNC_IDLE), build the wrapped
            query ONCE from the saved snapshot:
              build_polar_consistency_mode_set()  → "SET polar_consistency_mode = '...';"   :222
              build_wrapped_wait_query()          → mode SET + timeout SET + wait SET + query :224
            Replace the packet on the data stream; set the expected SET-result
            count. If the build cannot run safely, call fail_wait_wrap_finalize()
            (see B/Stage and section 8), which fails the query closed.
  Note:     PolarDB backends always support the wait/timeout GUCs, so there is no
            per-connection probe (see E.1).

Stage 6  DISPATCH                                         Connection.cpp (async_query / query_start)
  Domain:   Connection / libpq
  Function: PgSQL_Connection::async_query() → query_start() → PQsendQuery()
  What:     Begin the per-connection wrap-state countdown
            (polardb_query_wrap_state, Connection.h:776) with the wrapper
            statement count, so the connection layer can consume the prepended
            SET results inline. Send the query to the backend via the libpq async
            API.

Connection setup (cold path, once per backend connection):
  build_polardb_startup_profile() + append_polardb_startup_params()
    Resolve per-HG `proxy_protocol` over global `pgsql-polardb_proxy_protocol`.
    v15 emits `_polar_proxy_client_host`, `_polar_proxy_client_port`,
    `_polar_proxy_send_lsn=true`; legacy emits `_polar_origin_client_ip`,
    `_polar_origin_client_port`, `_polar_send_lsn=true`; off emits no PolarDB
    proxy startup params. RFQ-requesting profiles require a usable identity.
  polardb_init_connection_tracking()
    Called after successful connect. Calls PQsetPolarSendLSN(conn,1) only when
    the recorded startup profile requested REQUEST_RFQ_LSN. This is requested,
    not confirmed: backend RFQ LSN capability is confirmed only by later RFQs
    that actually carry LSN.
```

---

## B. Response Path — Detailed Stages

```
Stage 1  SET FILTER (inline, in the connection handler)   Connection.cpp:556
  Domain:   Connection handler NEXT_IMMEDIATE loop
  Function: PgSQL_Connection::handler() inline block
  What:     "Deterministic inline consumption of wrapped prepended SET results."
            For a wrapped read, the first N completed results are the SET
            statements. The connection counts them down (stmt_total) and loops
            back without forwarding them. When the count reaches zero, the next
            result is the user's result and is forwarded normally.
            The connection is the sole owner of SET-result consumption; the
            session/WIRE never sees the prepended SET results.

Stage 2  WIRE (forward notice + result)                  Session.cpp:5778
  Domain:   Session result routing
  Function: PgSQL_Result_to_PgSQL_wire(conn, myds)
  What:     If a best_effort wait timed out, the backend emitted a WARNING/NOTICE
            while the wrapped SELECT ran; it was captured to pending_notices.
            WIRE prepends those captured notices to the client output buffer so
            the client sees them ahead of the SELECT result (:5820-5828), then
            forwards the user result. Ownership of the notice bytes transfers to
            the output array (cleared without freeing).

Stage 3  REQUEST END                                     Session.cpp:6063
  Domain:   Session cleanup + tracking
  Function: RequestEnd(myds, called_on_failure)
  What:     On the success branch, caller-side gate:
              if (!called_on_failure && polardb_config.is_polardb_enabled)
                  polardb_process_result(myds, query_digest_text);
            Then account the wait time once and clear per-query state:
              record_wait_latency(polardb_query.wait);                           :6317
              polardb_query.reset_for_new_query();                               :6318
              clear_pending_notices(/*free_buffers=*/true);                      :6319
  Note:     reset_for_new_query() (include/PgSQL_PolarDB.h:1651) clears ALL per-query
            PolarDB state — reader plan (reader_plan.reset()), request_writer_scope,
            wait, wrapped_query_buf, and the dispatch-wrapper fields — not just
            consistency_target_lsn. clear_pending_notices(true) then frees any notice
            buffers not forwarded on error paths.
  Note:     The caller-side gate avoids a function call and a log line on the
            skip path for non-PolarDB sessions.

Stage 4  PROCESS_RESULT
  Domain:   Consistency / write tracking
  Function: polardb_process_result(myds, query_digest_text)
  What:     Read the backend WAL LSN from ReadyForQuery ONCE
            (myds->myconn->get_polardb_lsn() — no extra round-trip; 0 if
            the RFQ carried no LSN).
            Classify the query with is_write_query() (:498) — the only use of the
            write/read heuristic in the pipeline.
            Advance polardb_session_consistency.observed_lsn on any positioned RFQ LSN.
            Advance polardb_session_consistency.write_lsn only for positioned writes. Protected
            reads wait on max(write_lsn, observed_lsn). A primary-sourced
            positioned RFQ clears write/observed missing-LSN latches; a
            replica-sourced RFQ updates observed/cache state but does not clear
            those latches. Refresh the per-server LSN cache and bump
            polardb_server_lsn_updates_from_rfq only after the direct HGM
            update gate accepts an LSN-bearing RFQ for the current writer group+epoch.

Stage 5  WRITEOUT                                         (Session writeout path)
  Domain:   Protocol output
  What:     Flush the client output buffer to the network. The session returns to
            waiting for the next request.
```

> Difference from the full implementation: between Stage 3 and Stage 4 that tree has a
> SPLIT FINALIZE stage (`polardb_complete_txn_split_read` / `_abort_txn_split_read`).
> The LSN-only tree has no split path, so there is no such stage.

---

## C. End-to-End Scenario Traces

The traces use the same notation as the full implementation document: `CLIENT ──▶` is a
client packet, `├─` is a step, `│` is the same request continuing, `BACKEND ──▶`
/ `REPLICA ──▶` is a result arriving.

### C.1 Regular Query (non-PolarDB hostgroup)

```
CLIENT ──▶ handler() receives 'Q' packet
           │
           ├─ polardb_query.reset_reader_target() + request_writer_scope.reset()   ← always reset (Session.cpp:2534-2535)
           ├─ polardb_active.load() == false                        ← fast bypass (Session.cpp:2543)
           │    skip collect / plan / execute entirely
           │    cost: one relaxed atomic-bool load
           │
           ├─ find_or_create_backend(current_hostgroup)             Session.cpp:2570
           ├─ query runs (no wrapping, wrap-state not started)
           │
     BACKEND ──▶ result arrives
           │
           ├─ Connection::handler()  — no wrapped read → no inline filter
           ├─ PgSQL_Result_to_PgSQL_wire() — forward result as-is
           ├─ RequestEnd()  — is_polardb_enabled == false → process_result NOT called
           └─ writeout() → client
```

### C.2 Session Consistency (LSN wait, autocommit SELECT) — the core case

```
CLIENT ──▶ handler() receives 'Q' packet: SELECT ...
           │
           ├─ polardb_collect(route_ctx, hg=100, replica_eligible=1, hint=false)   Flow.cpp:233
           │    route_ctx.is_polar_hg = true
           │    route_ctx.writer_hg = 100, route_ctx.reader_hg = 101
           │    route_ctx.effective_consistency_mode = SESSION_LSN
           │    route_ctx.session.write_lsn = 0/1A3B400
           │    route_ctx.session.observed_lsn = 0/1A3B200
           │    route_ctx.in_transaction = false, route_ctx.is_multi_statement = false,
           │    route_ctx.is_extended_protocol = false
           │
           ├─ polardb_plan(route_ctx)                                              Flow.cpp:410
           │    L-1 eligible, reader present
           │    L1  mode = SESSION_LSN (not OFF, not PRIMARY_ONLY)
           │    L2  not in txn / not multi / not extended
           │    L4  target=max(write_lsn, observed_lsn)=0/1A3B400 → has_wait
           │    L5  reader lag within cap → allowed
           │    → action = REPLICA_WITH_WAIT, target_hg = 101, plan.wait_spec.target = 0/1A3B400
           │
           ├─ polardb_execute(plan, route_ctx, pkt)                               Flow.cpp:658
           │    reset per-query wait state                                   :385-386
           │    polardb_query.reader_plan.consistency_target_lsn = 0/1A3B400                 :432
           │    bump polardb_session_lsn_routing                            :433
           │    stage wait: wait_stage=WAITING, snapshot "SELECT ..."       :436-448
           │    bump polardb_wait_wrap_prepared                            :451
           │    → final_target_hg = 101    (wrapper deferred to finalize)
           │
           ├─ find_or_create_backend(101)                                  ← replica
           │    backend acquisition: if the acquired reader is ALREADY at
           │    0/1A3B400 (thread-local cache hit, or acquired from the
           │    target-reached prefix → wait_bypass_allowed):
           │      reset_wait(); polardb_wait_wrap_bypassed++
           │      → wrapper SKIPPED, the bare "SELECT ..." is dispatched (no SETs).
           │    Otherwise (reader behind target) the staged wait stands and the
           │    trace continues on the wrap path below (Target_LSN_Fallback_Wait).
           │
           ├─ finalize_wait_timeout_injection(conn, myds)                   Wrap.cpp:303 (Session.cpp:3607)
           │    build_polar_consistency_mode_set() → "SET polar_consistency_mode = 'best_effort'; "
           │    build_wrapped_wait_query():
           │      "SET polar_consistency_mode = 'best_effort'; "
           │      "SET polar_proxy_wait_timeout_ms = 1000; "
           │      "SET polar_xact_split_wait_lsn = '0/1A3B400'; "
           │      "SELECT ..."
           │    replace packet on data stream; expected SET results = 3
           │    bump polardb_wait_lsn_sent
           │
           ├─ async_query() → query_start() → PQsendQuery(wrapped query)
           │    begin polardb_query_wrap_state (stmt_total = 3)
           │
     REPLICA ──▶ results arrive (3 SET results, then the SELECT)
           │
           ├─ Connection::handler() inline filter                          Connection.cpp:556
           │    consume 3 SET results (countdown 3→0), then forward the SELECT
           │
           ├─ PgSQL_Result_to_PgSQL_wire()                                 Session.cpp:5778
           │    (best_effort: if a timeout NOTICE was captured, prepend it)  :5820-5828
           │    forward SELECT result to client
           │
           ├─ RequestEnd()                                                 Session.cpp:6063
           │    polardb_process_result(myds, "SELECT")                            :6103 →
           │      read RFQ LSN from the replica connection
           │      not a write → polardb_session_consistency.write_lsn unchanged
           │      refresh per-server LSN cache (replica), bump *_from_rfq
           │    record_wait_latency(polardb_query.wait)                    :6317
           │    polardb_query.reset_for_new_query()                        :6318  ← clears reader plan, writer scope, wait, wrapped buffer, dispatch fields
           │    clear_pending_notices(true)                                :6319
           │
           └─ writeout() → client gets the SELECT result only
```

### C.3 First read of a session / no session target

```
CLIENT ──▶ SELECT ...   (autocommit, SESSION_LSN, but the session has no write or observed target)
           │
           ├─ polardb_collect(route_ctx, ...)
           │    route_ctx.session.write_lsn = 0
           │    route_ctx.session.observed_lsn = 0
           │
           ├─ polardb_plan(route_ctx)
           │    If pgsql-polardb_session_lsn_baseline='observed':
           │        target==0 → PASSTHROUGH, target_hg = reader_hg (101), no wait.
           │        If the RFQ carries LSN, result processing records it as observed for later reads.
           │    If pgsql-polardb_session_lsn_baseline='primary':
           │        use the replication group's primary LSN mirror as the first target.
           │        If the mirror is unknown, PRIMARY_LSN_UNKNOWN goes through
           │        pgsql-polardb_route_rfq_policy.
           │
           ├─ execute: PASSTHROUGH → final_target_hg = 101 (no wrapper), or
           │    REPLICA_WITH_WAIT if a primary baseline target was available
           ├─ find_or_create_backend(101) ← replica, query runs unwrapped
           └─ normal response path; result processing refreshes the per-server LSN cache
              and records the observed LSN when RFQ carries one
```

### C.4 Consistency Mode = PRIMARY_ONLY (force primary)

```
CLIENT ──▶ SELECT ...
           │
           ├─ polardb_collect(route_ctx, ...) → route_ctx.effective_consistency_mode = PRIMARY_ONLY
           │
           ├─ polardb_plan(route_ctx)
           │    L1  mode == PRIMARY_ONLY → FORCE_PRIMARY, action reason = MODE_PRIMARY     Flow.cpp:249-256
           │
           ├─ execute: FORCE_PRIMARY → final_target_hg = writer_hg (100)
           ├─ find_or_create_backend(100) ← primary, no wrapping, no wait
           └─ normal response path (process_result still runs for LSN tracking)
```

### C.5 In an explicit transaction (route to writer)

```
CLIENT ──▶ BEGIN ; ... ; SELECT ...   (the SELECT is inside an explicit transaction)
           │
           ├─ polardb_collect(route_ctx, ...) → route_ctx.in_transaction = true
           │
           ├─ polardb_plan(route_ctx)
           │    L2  in_transaction → FORCE_PRIMARY, action reason = IN_TRANSACTION         Flow.cpp:269-276
           │
           ├─ execute: FORCE_PRIMARY → writer_hg (100)
           └─ the read runs on the primary (always consistent)

  NOTE: the full implementation offloads in-transaction reads to a replica via transaction
  split. The LSN-only tree does not — it routes to the writer. Split is
  future work; see 19-FUTURE-TXN-SPLIT-DESIGN.md.
```

### C.6 Multi-statement read (route to writer)

```
CLIENT ──▶ SELECT a; SELECT b;   (replica_eligible, but more than one statement)
           │
           ├─ polardb_collect(route_ctx, ...)
           │    own semicolon scan: trim, drop one trailing ';', any ';' left?   Flow.cpp:85-96
           │    → route_ctx.is_multi_statement = true
           │
           ├─ polardb_plan(route_ctx)
           │    L2  is_multi_statement → FORCE_PRIMARY, action reason = MULTI_STATEMENT    Flow.cpp:277-284
           │
           └─ the read runs on the primary
              (a wrapped read must be a single statement so the connection layer
               can count SET results correctly)
```

### C.7 Extended protocol in this implementation

```
CLIENT ──▶ Parse / Bind / Execute   (extended query protocol)
           │
           ├─ This implementation never injects a wait wrapper into Parse/Bind/Execute.
           │
           ├─ Manual destination_hostgroup reader route:
           │    honored as manual policy, no wrapper.
           │
           ├─ Automatic replica_eligible read with no prior write LSN:
           │    may use reader, no wrapper needed because there is no RYW target.
           │
           ├─ Automatic replica_eligible read after a known write LSN:
           │    FORCE_PRIMARY, action reason = EXTENDED_PROTOCOL.
           │
           └─ Automatic replica_eligible read after an unknown RFQ LSN:
                FORCE_PRIMARY. This implementation does not degrade extended-protocol unknown-target
                reads because local Parse/Bind completions bypass the simple-result
                notice flush path.

  NOTE: the text-only wait wrapper is simple-query only. Extended-protocol RYW is
  future work; this implementation either honors explicit manual routing or keeps automatic reads
  on the writer when a known or unknown session RYW target must be protected.
```

### C.8 Reader acquisition fails the lag-cap safety check

```
CLIENT ──▶ SELECT ...   (autocommit, SESSION_LSN, session has written)
           │
           ├─ polardb_plan(route_ctx)
           │    L4  target=max(write_lsn, observed_lsn) > 0 → has_wait
           │    L5  polardb_reader_lag_plan(): attach primary_lsn + max_lag_bytes
           │    → REPLICA_WITH_WAIT plan with plan.reader requirements
           │
           ├─ get_MyConn_polardb_reader(reader_hg, plan.reader)
           │    cap enabled; candidate readers are missing/stale/over-lagged
           │    → PRIMARY_LSN_UNKNOWN / READER_LSN_UNKNOWN /
           │      READER_LSN_STALE / READER_LAG_EXCEEDED
           │
           └─ session dispatch redirects this one read to the writer and bumps
              PolarDB_Consistency_Writer_Fallback

  NOTE: the lag cap is a SAFETY check, not the consistency gate. The RYW guarantee
  comes from the wait SET, not from the cap (see E.3). The cap only avoids picking
  a replica so far behind that the wait would probably time out. When lag and
  contention collide, safety statuses outrank READER_BUSY, so the query may use
  the writer instead of waiting for a busy but in-cap reader.
```

### C.9 Manual mode (pipeline bypass)

```
Setup: a query rule sets destination_hostgroup=105 and leaves replica_eligible unset.

CLIENT ──▶ SELECT /* analytics */ count(*) FROM big_table
           │
           ├─ qpo: destination_hostgroup = 105, replica_eligible = -1 (unset)
           ├─ current_hostgroup = 105 (from the rule)
           │
           ├─ manual_mode = polardb_manual_route_scope(...) = true               Session.cpp:2550-2552
           │    (manual ≡ replica_eligible < 0 && dest >= 0)
           │    → SKIP the pipeline entirely (collect/plan/execute not run)
           │
           ├─ find_or_create_backend(105)
           ├─ query runs on HG 105 as-is (no wrapping, no wait, no consistency)
           └─ normal response path

  NOTE: a manually-routed read does NOT get the RYW wait wrapper. Consistency for
  this path is the operator's responsibility. This is the one bypass that is NOT
  protected by automatic writer fallback (see section 8 / E.5). Auto-installed rules set replica_eligible=1
  (auto mode), not a destination, so they do not trigger manual mode.
```

### C.10 Wrapper build fails -> writer-fallback latch

```
CLIENT ──▶ SELECT ...   (plan said REPLICA_WITH_WAIT, execute staged the wait)
           │
           ├─ finalize_wait_timeout_injection(conn, myds)                       Wrap.cpp:303
           │    a precondition is missing:
           │      missing backend connection or data stream                      :214
           │      missing original-query snapshot                                :218
           │      wrapped query came out empty                                   :248
           │    → fail_wait_wrap_finalize(reason)                                :260
           │        set polardb_wait_disabled = true   (latch for this session)  :268
           │        bump polardb_wait_wrap_safety_abort                          :267
           │
           ├─ the CURRENT query is NOT silently sent to the replica:
           │    return a clean ERROR packet and end the request                  Session.cpp:3607
           │
           └─ EVERY later read in this session now uses the writer
              via the execute-time wait_disabled check                          Flow.cpp:705
              (until a RESET clears the latch, Wrap.cpp:330)
```

### Future scenarios (not in this implementation)

These appear in the full implementation `20-PIPELINE-FLOW.md` but are not part of the
LSN-only feature. They are listed here so the section map matches; the designs
are in the future-design documents.

| Scenario in the full implementation | LSN-only behavior today | Future design |
|---|---|---|
| Transaction with split read | in-txn read routes to writer (C.5) | [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md) |
| Split read failure / abort | n/a (no split) | [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md) |
| Lazy pool warmup | n/a (no split-reader pool) | [21-FUTURE-OTHER-CAPABILITIES.md](21-FUTURE-OTHER-CAPABILITIES.md) |
| CSN / global consistency wait | n/a (enum slot reserved) | [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md) (experimental) |

---

## D. Function Reference

### Pipeline stage functions (`lib/PgSQL_PolarDB_Flow.cpp`)

| Function | Line | Stage | Side effects |
|----------|------|-------|--------------|
| `polardb_collect` | 233 | collect | repairs stale session LSN targets/latches when writer group or epoch changes |
| `polardb_plan` | 410 | plan | none (one read of cached HGM LSNs for the lag cap) |
| `polardb_execute` | 658 | execute | sets target HG; stages per-query wait state; bumps counters |
| `polardb_process_result` | 846 | result processing | advances session write/observed LSN state; refreshes per-server LSN cache |

### Helper functions called by the pipeline

| Function | File:line | Called by |
|----------|-----------|-----------|
| `polardb_resolve_consistency_mode` | `include/PgSQL_PolarDB.h:1277` | collect (three-tier mode resolution) |
| `polardb_resolve_wait_timeout_ms` | `lib/PgSQL_PolarDB_Consistency.cpp:33` | collect (timeout resolution) |
| `PolarDB_Query_WaitPlan::build_consistency` | `include/PgSQL_PolarDB.h:1102` | plan (build the wait payload) |
| `polardb_reader_lag_plan` | `lib/PgSQL_PolarDB_Flow.cpp:358` | plan (attach byte-lag inputs to `plan.reader`) |
| `PolarDB_Query_ReaderPlan::within_byte_cap` | `include/PgSQL_PolarDB.h:1608` | acquisition-time byte-lag-cap predicate (called by reader acquisition in HGM and Thread) |
| `polardb_lag_ms_within_cap` | `include/PgSQL_PolarDB.h:1338` | registered but inert ms-lag helper, not the byte-lag gate (see F.2) |
| `is_polardb_hostgroup` | `lib/PgSQL_PolarDB_Consistency.cpp:58` (delegates to HGM) | collect |
| `build_polar_consistency_mode_set` | `lib/PgSQL_PolarDB_Wrap.cpp:157` | wait finalize |
| `build_wrapped_wait_query` | `lib/PgSQL_PolarDB_Wrap.cpp:202` | wait finalize |
| `finalize_wait_timeout_injection` | `lib/PgSQL_PolarDB_Wrap.cpp:303` | session (single wrap point) |
| `fail_wait_wrap_finalize` | `lib/PgSQL_PolarDB_Wrap.cpp:260` | wait finalize (on failure) |
| `record_wait_latency` | `lib/PgSQL_PolarDB_Wrap.cpp:376` | WIRE / RequestEnd / failed-wait accounting |
| `PolarDB_Protocol::is_write_query` | `lib/PgSQL_PolarDB.cpp:77` | result processing (the only write/read classification) |
| `get_polardb_lsn` | `include/PgSQL_Connection.h:830` | result processing (read RFQ LSN) |

### Session orchestration call sites (`lib/PgSQL_Session.cpp`)

| What | Line | Context |
|------|------|---------|
| Per-query reader-plan + writer-scope reset | 2534-2535 | top of the 'Q' handler, before the gate |
| `polardb_active` master gate | 2543 | enter the pipeline only if true |
| Manual-mode check | 2550-2552 | `polardb_manual_route_scope()` (≡ `replica_eligible < 0 && dest >= 0`) → skip the pipeline |
| `polardb_collect()` | 2554 | gather inputs |
| `if (route_ctx.is_polar_hg)` guard | 2555 | only plan for a PolarDB HG |
| `polardb_plan()` | 2556 | the decision |
| `polardb_execute()` | 2558 | apply, set `current_hostgroup` |
| Backend bind | 2570 | `find_or_create_backend(current_hostgroup)` |
| Wait finalize | 3595 | `finalize_wait_timeout_injection()` at ASYNC_IDLE |
| Backend-acquisition consume | — | clear `polardb_query.reader_plan.consistency_target_lsn` and `polardb_query_reader_plan` once acquisition is handled |
| WIRE | 5778 | `PgSQL_Result_to_PgSQL_wire()` (notice + result) |
| RequestEnd | 6063 | response-path cleanup + process_result gate |
| Process result | — | `polardb_process_result()` (caller-side gated) |
| Wait-latency accounting | 6317 | `record_wait_latency()` |

### Connection-level SET filtering (`lib/PgSQL_Connection.cpp`)

| What | Line | Context |
|------|------|---------|
| Init connection tracking | 456 | `polardb_init_connection_tracking()` on connect success |
| Inline SET consume | 556 | wrapped prepended-SET consumption in `handler()` |
| `PQsetPolarSendLSN(conn,1)` | 1335 | enable RFQ-LSN parsing (def at 1325) |
| Fresh-connect startup profile | connection setup | emit v15/legacy/off PolarDB proxy startup params from the resolved profile |
| Wrap-state runtime | `include/PgSQL_Connection.h:776` | `polardb_query_wrap_state` (`was_wrapped` :747, `stmt_total` :749) |

---

## E. Design Decisions

### E.1 No GUC probe

PolarDB backends always support the wait and timeout GUCs (`polar_consistency_mode`,
`polar_proxy_wait_timeout_ms`, `polar_xact_split_wait_lsn`). The LSN-only tree
therefore has **no** per-connection probe — no extra round-trip, no probe state
machine, no "GUC supported?" flag to check before wrapping. The wait finalize
step always injects the SETs for a PolarDB connection. This is a deliberate
simplification over earlier designs that probed `polar_proxy_wait_timeout_ms`
on first use.

### E.2 Deferred wrapping (single wrap point)

The pipeline uses intent-then-wrap:

1. `polardb_execute()` saves the wait intent — type, target, timeout, and a
   snapshot of the original query text — and bumps `polardb_wait_wrap_prepared`.
   It does **not** build any SQL (`lib/PgSQL_PolarDB_Flow.cpp:719-725`).
2. `finalize_wait_timeout_injection()` builds the wrapped query exactly once, at
   `ASYNC_IDLE`, after the backend connection exists
   (`lib/PgSQL_PolarDB_Wrap.cpp:303`).

There is one place that builds the wrapper, which removes any chance of building
it twice (an earlier two-phase design built once without the timeout, then rebuilt
with it). The original unwrapped packet sits on the data stream until finalize
replaces it.

**Build failure stops the read; it is not best-effort.** If the wrapper cannot be built
safely, `fail_wait_wrap_finalize()` sets the `polardb_wait_disabled` latch and the
caller returns a clean error for the current query rather than sending an
unwrapped read to a replica (see scenario C.10 and section 8).

### E.3 The `consistency_target_lsn` and wait-bypass rule

Once plan picks a wait target, reader acquisition uses the target as a
preference. It is not a hard proxy-side rejection gate for the whole reader set:
fallback readers may still be selected and protected by the backend wait.

- `polardb_query.reader_plan.consistency_target_lsn` is a per-query field set by execute from
  `plan.wait_spec.target`. It is an **advisory hint** to reader acquisition, not a hard
  filter.
- Consistency-target reader acquisition first prefers ONLINE readers with a fresh cached
  LSN greater than or equal to the target and balances among that subset. If it
  acquires one of those readers, `PolarDB_ReaderResult.wait_bypass_allowed` lets
  the session clear the staged wait and increment `PolarDB_Wait_Wrap_Bypassed`.
  If it cannot acquire a preferred connection, it falls back to the full original
  candidate set and relies on the wait wrapper. `PolarDB_Target_LSN_Preferred`,
  `PolarDB_Target_LSN_Fallback_Wait`, and `PolarDB_Wait_Wrap_Bypassed` account
  these paths.
- For reads that require RFQ LSN, pool acquisition skips free pooled connections
  whose startup profile did not request `REQUEST_RFQ_LSN`. It increments
  `PolarDB_RFQ_Profile_Skipped`. Among RFQ-capable free connections on the same
  server, the pool still uses the regular PostgreSQL reuse preference: same
  connection options, no reset required, then more matching session
  variables/schema. If fresh connection creation is allowed and max-connection
  capacity is blocked by old-profile free connections, it evicts enough
  incompatible free connections to leave room for one RFQ-LSN-capable
  replacement, incrementing `PolarDB_RFQ_Profile_Evicted` for each evicted
  connection.
- The default RYW gate is the `SET polar_xact_split_wait_lsn = '<target>'`
  statement on the wire. The backend itself blocks until its replay LSN passes
  the target (or the timeout fires). The only bypass is after binding a specific
  reader whose fresh cached LSN already reaches the same consistency target.
- A mandatory proxy-side "caught-up" check would not add correctness unless the
  feature grows an explicit lag cap for this purpose. Cached LSN freshness is
  used only to prefer and optionally bypass the wait for a specific selected
  reader; fallback readers still use the backend wait as the correctness gate.

So the contract is: **plan and execute choose a reader and set up the wait; the
wait is skipped only when backend acquisition proves the selected reader already
reaches the same consistency target.**

### E.4 Safe writer fallback is the safety rule

When any precondition for a safe replica read is missing or uncertain, the
pipeline routes to the writer (always consistent) rather than risk a stale read.
It never silently sends an unwrapped read to a replica. The complete list of
safe fallback paths is in section 8.

### E.5 `replica_eligible` 3-way semantics and manual mode

The `replica_eligible` query-rule field controls how the pipeline interacts with
user routing. Three values, three behaviors:

| Value | Meaning | Pipeline behavior |
|-------|---------|-------------------|
| `1` | Auto mode | The pipeline runs and decides routing + wrapping. |
| `0` | Force primary | The L-1 fast path returns PASSTHROUGH to the writer. No wrapping. |
| `-1` | Unset | If a destination HG is also set (`dest >= 0`) → **manual mode**, the pipeline is skipped. If no destination is set → the pipeline runs as auto mode. |

Manual mode is decided by the helper `PgSQL_Session::polardb_manual_route_scope()`
(defined in `lib/PgSQL_PolarDB_Flow.cpp:89`), invoked from `lib/PgSQL_Session.cpp:2550-2552`:

```cpp
int replica_eligible = -1;
int dest_hg = -1;
int manual_scope_hg = -1;
bool manual_mode =
    polardb_manual_route_scope(&manual_scope_hg, &dest_hg, &replica_eligible);
```

The helper returns `false` when `rule_replica_eligible >= 0 || rule_dest_hg < 0` —
i.e. manual mode is equivalent to `replica_eligible < 0 && destination_hostgroup >= 0` —
and additionally resolves the writer scope hostgroup (`current_hostgroup` inside a
sticky transaction, otherwise the rule's destination HG) used for later LSN
attribution. The boolean `(replica_eligible < 0 && dest >= 0)` is an explanation of
the helper's return value, not the literal call-site code. Note that
`lib/PgSQL_Session.cpp:2546` is the `PgHGM->status.polardb_active` guard, not the
manual-mode computation; the manual-mode decision lives in the helper call near :2550.

Why not just check the destination HG: a rule may set both a destination and
`replica_eligible=1` (auto mode with a preferred default HG); in that case the
pipeline should still run. Only when `replica_eligible` is explicitly unset does
the operator signal "I am handling routing." Existing `destination_hostgroup`-only
rules therefore keep their exact behavior.

### E.6 Writer-to-reader HG mapping is always populated

The HostGroups Manager keeps a writer→reader map so collect can fill
`route_ctx.reader_hg`. This map is populated for **every** PolarDB hostgroup pair, not
only when some optional feature is enabled. If the map returned `-1`, plan's L-1
fast path would send every SELECT to the writer regardless of consistency
configuration — so a fully-populated map is what lets consistency-only setups
route eligible reads to the reader at all.

### E.7 Routing hint `/* route=primary */` (implemented); others are future

The query processor parses a `/* route=primary */` first-comment hint into
`qpo->force_primary_hint`. collect copies it into the context
(`force_primary_hint`), and plan's L0 turns it into `FORCE_PRIMARY` with action reason
`HINT_PRIMARY` (`lib/PgSQL_PolarDB_Flow.cpp:234-241`). This is the only routing
hint wired in this implementation.

The full implementation also sketches `/* route=replica */` (force replica, skip the wait —
expert use) and `/* split=off */` (disable split for a query/transaction). These
are not in the LSN-only tree; `split=off` has no meaning without split. See
[15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

### E.6 Query-result cache interaction (`cache_ttl`)

ProxySQL's generic query-result cache (the `cache_ttl` query-rule field, served from
`GloPgQC`) is consulted in the query-rules phase — **before** `polardb_collect()` /
`polardb_plan()` run — so at cache-lookup time there is no route plan yet. Left
unguarded, a cached replica result could be returned to a later read **without** the
LSN wait, defeating read-your-writes.

`PgSQL_Session::polardb_query_cache_disabled_for_current_rule()`
(`lib/PgSQL_PolarDB_Flow.cpp:117`) closes this. It is called as
`&& !polardb_query_cache_disabled_for_current_rule()` at **both** the cache GET
(`lib/PgSQL_Session.cpp:5526`) and the cache SET (`:6053`), and re-derives the
planner's decision from the inputs that exist before planning (its own comment:
*"Cache lookup happens before collect()/plan(), so no reader plan exists yet. Use the
same cheap inputs the planner will later read…"*):

1. **Gate** — only an automatic, `replica_eligible == 1` read with `polardb_active`
   set is a candidate; anything else caches normally.
2. **Route hostgroup** — compute the HG the planner will use (`current_hostgroup`, or
   the rule's `destination_hostgroup` outside a sticky transaction); no HG → cache
   normally.
3. **PolarDB hostgroup?** — if the route HG is not a PolarDB hostgroup → cache
   normally.
4. **Resolve consistency mode** with the same 3-tier resolution the planner uses
   (session override → HG policy → global default): `off` → cache allowed; `primary`
   → cache disabled (the read must reach the primary); `lsn` → fall through to the
   obligation check.
5. **`lsn` obligation** — disable the cache the moment the session owes a wait:
   `polardb_session_consistency.target() > 0`, a missing-LSN latch (`write_unknown` /
   `observed_unknown`), or a `PRIMARY` first-read baseline (which can synthesize a
   first-read target from the writer mirror). Otherwise the cache is allowed.

Why this closes the hole correctly:

- **No route plan needed** — it uses only the session-level and rule-level signals
  available before planning.
- **Re-evaluated per query against current session state** — a read issued before the
  session has written may be cached, but the moment the session writes
  (`target() > 0`) the next read's GET sees the obligation, returns `true`, and is
  forced to miss → it takes the normal wait path. A stale pre-write result can never
  be served after the write.
- **Symmetric (GET and SET)** — a result is stored only when there is no obligation
  and served only when there is no obligation, so neither side can leak a stale
  result into a consistency read.
- **Fail-closed** — any uncertainty (a missing-LSN latch, or a `PRIMARY` baseline)
  disables the cache.

See invariant I10 in
[14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md).

---

## F. Deferred Cleanup / Known Gaps (this implementation)

Items that are real but low-risk, deferred to keep the LSN-only feature focused.
The full list, with operator impact, is in
[15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md).

### F.1 Missing RFQ LSN does not invent a target

`polardb_session_consistency.write_lsn` advances only on positioned writes, and
`polardb_session_consistency.observed_lsn` advances only on positioned RFQs. If a write RFQ carries no
LSN, ProxySQL sets `polardb_session_consistency.write_unknown` and increments
`PolarDB_Write_Missing_LSN`. If a tracked SESSION_LSN read RFQ carries no LSN,
ProxySQL sets `polardb_session_consistency.observed_unknown` and increments
`PolarDB_Read_Missing_LSN`. Later automatic reads route through
`pgsql-polardb_route_rfq_policy`: `strict` forces the writer; `best_effort`
allows a degraded reader route and increments
`PolarDB_RFQ_Best_Effort_Degraded_Routes`. ProxySQL also queues a PostgreSQL
`NoticeResponse` ahead of the result so the client sees that the read was routed
without an enforceable RFQ LSN wait target. ProxySQL does not guess a
replacement target from monitor or global state.

The always-on proxy log for degraded RFQ routing is edge-limited per client
session while the degradation remains active; the counter and client notice are
per degraded simple-query route. Extended-protocol unknown-target reads force
the writer instead of degrading in this implementation.

### F.2 Millisecond lag cap is inert

`pgsql-polardb_lag_ms` and `polardb_lag_ms_within_cap()` are registered but have
no routing effect, because PgSQL/PolarDB has no millisecond-lag producer yet
(`include/PgSQL_PolarDB.h:504-507`, `:536`). `PolarDB_LSN_Stale_Count` is still
active for byte-lag stale/missing samples when `max_lag_bytes` is enabled. Do
not treat `polardb_lag_ms` or that counter as a supported millisecond-lag gate
until a producer exists.

### F.3 The L3 consistency-helper PRIMARY branch is defensive

In plan, the helper's PRIMARY branch can only be reached by `SESSION_LSN`, but the
helper returns PRIMARY only for `PRIMARY_ONLY` — which already returned at L1. So
the L3 PRIMARY branch is effectively unreachable in normal operation; it is a
safety net (`lib/PgSQL_PolarDB_Flow.cpp:188-190`, `include/PgSQL_PolarDB.h:844`).

### F.4 The `pkt.size < 7` guard is in practice unreachable

A simple-query packet is `'Q'` + 4-byte length + query + NUL, so the query
processor has already parsed it before execute runs. The `pkt.size < 7` check
(`lib/PgSQL_PolarDB_Flow.cpp:408`) guards against a `size_t` underflow on
`orig_len = pkt.size - 5 - 1` rather than a real input.

---

## G. Roadmap (future, not in this implementation)

These are designed but not implemented in the LSN-only tree. Each has a
future-design document; this section gives the one-paragraph shape so the section
map matches the full implementation `20-PIPELINE-FLOW.md`.

- **Transaction-split read offload** — offload an in-transaction read to a replica
  while a write transaction is open, using PolarDB's transaction-split GUCs and a
  small FSM (NONE → ON_PRIMARY → SPLITTABLE → SPLIT_READ_ACTIVE). Today the
  pipeline routes to the writer for any in-transaction read (action reason
  `IN_TRANSACTION`). Full design:
  [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md).
- **CSN / global consistency (experimental)** — a commit-sequence-number wait for
  cross-session consistency. The enum slot (value `2`) is reserved. It depends on
  PolarDB backend support, applies mainly in global-consistency mode, and its wait
  behavior is not reliably verified, so it is described as experimental. Full
  design: [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md).
- **Extended-protocol RYW** — this implementation has no wait wrapper for Parse/Bind/Execute. Manual reader routes are honored; automatic extended reads without a prior write LSN may use reader; automatic extended reads with a known write/observed LSN target or unknown RFQ target use writer. A binary-aware wrapper would lift this limitation.
- **Multi-reader-group routing (1:N)** — let one writer map to several reader
  groups (for example OLTP vs analytics, or tiered consistency), selected in plan
  by policy and lag. The current schema is a strict 1:1 writer↔reader mapping; the
  manual-mode bypass (C.9) is the current workaround. Sketch in
  [21-FUTURE-OTHER-CAPABILITIES.md](21-FUTURE-OTHER-CAPABILITIES.md).
- **Graduated wait timeout with retry** — inject a small initial timeout, then
  retry with a larger one or fall back to the writer, so a lagging replica does not
  block for the full configured timeout. Would live in the wait-finalize step plus
  a retry handler after dispatch.

---

## H. Recent Refactoring Changes

These are already in the LSN-only tree (they shaped the current structure).

### H.1 `polardb_active` fast-bypass

A single relaxed atomic-bool load (`PgHGM->status.polardb_active`,
`lib/PgSQL_Session.cpp:2543`) gates the whole pipeline. A non-PolarDB query no
longer allocates a `RouteCtx`, calls `is_polardb_hostgroup()`, or logs anything.

### H.2 Caller-side `is_polardb_enabled` gate on process_result

`polardb_process_result()` is called only when the session is PolarDB-enabled
(`lib/PgSQL_Session.cpp:6102-6103`), instead of being called unconditionally and
self-skipping. This avoids the call and a log line for non-PolarDB sessions.

### H.3 `polardb_init_connection_tracking()` extraction

The per-connection LSN-tracking setup (enable RFQ-LSN parsing) is a single method
(`lib/PgSQL_Connection.cpp:456` call site, `1325` definition) instead of inline
code in the connect-success path. Cold path, once per connection.

### H.4 `polardb_export_stats()` extraction

The PolarDB stat-counter exports are a single file-static function
(`lib/PgSQL_Thread.cpp:4518`, called at `:4946`) instead of inline code in the
admin status handler. Admin-query path only — no hot-path impact.

---

## I. TODO Items (production readiness)

### I.1 Downgrade `proxy_info` to `proxy_debug` in hot paths

The pipeline currently logs at `proxy_info` in the collect / plan / execute /
result-processing stages, which means string formatting and I/O on every PolarDB query.
Before production these should move to `proxy_debug` (or behind a verbosity flag),
keeping `proxy_error` / `proxy_warning` for real errors. (No logging is removed
until the feature is stable — this is a downgrade-when-stable note, not an action
to take blindly.)

### I.2 `polardb_active` early-out in HGM helpers

Several HostGroups Manager helpers (reader lookup, policy lookup, per-server LSN
update, global-LSN read) could skip work when `polardb_active == false`. Most
valuable for the lock-guarded scan/update functions in non-PolarDB deployments.

### I.3 Connection inline sites — assessed, mostly intentional

The inline PolarDB sites in the connection handler are hot-path one-liners or the
wrap-state consume loop (which uses `NEXT_IMMEDIATE`/goto and cannot be
extracted cleanly). Only the connection-setup block was extracted (H.3). The rest
stay inline on purpose — wrapping them in methods would add call overhead for no
clarity gain.

---

## Appendix: Mermaid diagrams

The diagrams above are the normative, in-text ASCII. These Mermaid versions are an
extra convenience for renderers that prefer them; they carry no information beyond
the ASCII.

### A.1 Request path (stage IDs)

```mermaid
flowchart TD
    A["'Q' handler"] --> R["reset_reader_target() + request_writer_scope.reset()\nSession.cpp:2534-2535"]
    R --> G{"polardb_active?\nSession.cpp:2543"}
    G -- no --> BIND["find_or_create_backend\nSession.cpp:2570"]
    G -- yes --> M{"manual_mode?\npolardb_manual_route_scope\nSession.cpp:2550"}
    M -- yes --> BIND
    M -- no --> C["collect\nFlow.cpp:233"]
    C --> P{"is_polar_hg?"}
    P -- no --> BIND
    P -- yes --> PL["plan\nFlow.cpp:410"]
    PL --> EX["execute\nFlow.cpp:658\ncurrent_hostgroup = final_target_hg"]
    EX --> BIND
    BIND --> BP{"reader acquired & already at target?\n(tl-cache hit or wait_bypass_allowed)"}
    BP -- yes --> BYP["reset_wait(); Wait_Wrap_Bypassed++\nWRAP BYPASS — no SETs"]
    BP -- no --> WF["wait finalize (single wrap)\nWrap.cpp:303"]
    BYP --> D["dispatch\nasync_query / PQsendQuery"]
    WF --> D
```

### A.2 Response path (stage IDs)

```mermaid
flowchart TD
    S["result arrives"] --> F["inline SET filter\nConnection.cpp:556"]
    F --> W["WIRE: prepend notice + forward result\nSession.cpp:5778"]
    W --> RE["RequestEnd\nSession.cpp:6063"]
    RE --> GP{"is_polardb_enabled?"}
    GP -- yes --> PUB["polardb_process_result"]
    GP -- no --> WO["writeout → client"]
    PUB --> RL["record_wait_latency\nSession.cpp:6317"]
    RL --> WO
```

### A.3 The plan decision (top-down, first match wins)

```mermaid
flowchart TD
    S["plan — Flow.cpp:410"] --> P1{"is_polar_hg?"}
    P1 -- no --> O1["PASSTHROUGH (target -1)\nFlow.cpp:209"]
    P1 -- yes --> P2{"reader_hg < 0?"}
    P2 -- yes --> O2["PASSTHROUGH (writer)\nFlow.cpp:215"]
    P2 -- no --> P3{"replica_eligible?"}
    P3 -- no --> O3["PASSTHROUGH (writer)\nFlow.cpp:222"]
    P3 -- yes --> P4{"force_primary_hint?"}
    P4 -- yes --> O4["FORCE_PRIMARY (HINT_PRIMARY)\nFlow.cpp:234"]
    P4 -- no --> P5{"mode?"}
    P5 -- PRIMARY_ONLY --> O5["FORCE_PRIMARY (MODE_PRIMARY)\nFlow.cpp:249"]
    P5 -- OFF --> O6["PASSTHROUGH (rules own)\nFlow.cpp:260"]
    P5 -- SESSION_LSN --> P6{"hard query shape?\nin_txn or multi_stmt"}
    P6 -- yes --> O7["FORCE_PRIMARY\nIN_TRANSACTION / MULTI_STATEMENT"]
    P6 -- no --> P7{"write/observed\nLSN unknown latch?"}
    P7 -- yes --> R1{"route_rfq_policy"}
    R1 -- strict --> O8["FORCE_PRIMARY\nWRITE_LSN_UNKNOWN / OBSERVED_LSN_UNKNOWN"]
    R1 -- best_effort --> O9["PASSTHROUGH reader\n(degraded_rfq_route)"]
    P7 -- no --> P8{"target=max(write_lsn,\nobserved_lsn)"}
    P8 -- "target==0 and\nbaseline=primary" --> P9{"primary mirror\nhas LSN?"}
    P9 -- no --> R2{"route_rfq_policy"}
    R2 -- strict --> O10["FORCE_PRIMARY\nPRIMARY_LSN_UNKNOWN"]
    R2 -- best_effort --> O11["PASSTHROUGH reader\n(degraded_rfq_route)"]
    P9 -- yes --> P10["target=primary mirror"]
    P8 -- "target==0 and\nbaseline=observed" --> O12["PASSTHROUGH reader\nfirst read, no wait"]
    P8 -- "target>0" --> P11{"extended_protocol?"}
    P10 --> P11
    P11 -- yes --> O13["FORCE_PRIMARY\nEXTENDED_PROTOCOL"]
    P11 -- no --> O14["REPLICA_WITH_WAIT plan\nplan.reader carries cap/status inputs"]
    O14 --> A1{"reader acquired?"}
    A1 -- yes --> O15["reader, wait target"]
    A1 -- safety status --> O16["writer fallback\nPolarDB_Consistency_Writer_Fallback"]
```

### A.4 The single RYW gate is the SET

```mermaid
flowchart LR
    A["execute: consistency_target_lsn = plan.wait_spec.target\nFlow.cpp:432"] --> B["reader acquisition\n(advisory, EXCEPT it can prove the\nreader is already at the target)"]
    B -- "reader behind target" --> C["wrap: SET polar_xact_split_wait_lsn = '<target>'\nWrap.cpp:303"]
    C --> D["backend blocks until replay LSN >= target\n(THE single RYW gate)"]
    B -- "reader already at target (WrapBypass)" --> BY["no wrap, no wait\nreader already satisfies the target\nWait_Wrap_Bypassed"]
    D --> E["process_result: read RFQ LSN\nadvance observed_lsn on any positioned RFQ\nadvance write_lsn on positioned writes\nclear/set missing-LSN latches"]
    BY --> E
```

For a read that wraps, the SET is the single RYW gate. The one exception is
WrapBypass: when reader acquisition proves the selected reader has already reached
the target (a fresh thread-local cache hit, or a reader from the target-reached
prefix with `wait_bypass_allowed`), there is nothing to wait for — the wrap and the
backend gate are skipped and read-your-writes is carried by that freshness proof
instead of the SET (`PolarDB_Wait_Wrap_Bypassed`). The proof is required: any reader
not proven at the target keeps the SET as the gate.

---

## Cross-references

- [06-ROUTING-PIPELINE.md](06-ROUTING-PIPELINE.md) — the prose companion (decision matrix, safe fallback proofs)
- [03-TYPES-AND-ENUMS.md](03-TYPES-AND-ENUMS.md) — `RouteCtx`, `RoutePlan`, `RouteAction`, `RouteActionReason`, `ConsistencyMode`
- [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md) — schema, knobs, three-tier resolution
- [05-MONITOR-AND-HGM-LSN-STATE.md](05-MONITOR-AND-HGM-LSN-STATE.md) — the per-server LSN cache the lag cap reads
- [07-QUERY-WRAPPING.md](07-QUERY-WRAPPING.md) — how the wait wrapper is built and injected (the deferred step)
- [08-WAIT-TIMEOUT-AND-NOTICES.md](08-WAIT-TIMEOUT-AND-NOTICES.md) — best_effort vs strict and notice forwarding
- [09-PUBLISH-AND-WRITE-TRACKING.md](09-PUBLISH-AND-WRITE-TRACKING.md) — the full result-processing stage and `polardb_session_consistency.write_lsn`
- [12-THREADVARS-AND-OBSERVABILITY.md](12-THREADVARS-AND-OBSERVABILITY.md) — the counters this pipeline bumps
- [13-QUERY-LIFECYCLE-AND-TRACES.md](13-QUERY-LIFECYCLE-AND-TRACES.md) — more worked end-to-end traces
- [14-INVARIANTS-AND-FAILURE-MODES.md](14-INVARIANTS-AND-FAILURE-MODES.md) — the RYW invariant and safe fallback proofs
- [15-LIMITATIONS-AND-ROADMAP.md](15-LIMITATIONS-AND-ROADMAP.md) — deferred items and the path to CSN/split
- [18-FUTURE-CSN-DESIGN.md](18-FUTURE-CSN-DESIGN.md), [19-FUTURE-TXN-SPLIT-DESIGN.md](19-FUTURE-TXN-SPLIT-DESIGN.md), [21-FUTURE-OTHER-CAPABILITIES.md](21-FUTURE-OTHER-CAPABILITIES.md) — future designs

---

Verified against this branch.
