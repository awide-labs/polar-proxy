# PolarDB `_polar_proxy_session_id` / `_polar_proxy_cancel_key` support in ProxySQL

| | |
|---|---|
| **Status** | DESIGN (first-class). Default-off feature; no code change in this document. |
| **Scope** | Backend-visible PolarDB *session identity* (SID) and cancel-key plumbing for ProxySQL→PolarDB backend connections, and how it interacts with ProxySQL's own frontend cancel, with connection pooling, with the transaction-split feature, and with ProxySQL cluster mode. |
| **Audience** | ProxySQL PgSQL-module engineers implementing PolarDB proxy support; reviewers of the PolarDB fold. |
| **Verified against** | ProxySQL branch `polardb-dev` and PolarDB backend branch `POLARDB_15_PROXY`. All `file:line` references below were checked in those revisions. |
| **Sibling docs** | The SSL-propagation design (`_polar_proxy_use_ssl` / `_polar_proxy_ssl_version` / `_polar_proxy_ssl_cipher_name` + pool-key extension) is a separate document; this doc references it where the two features interact but does not specify it. |

---

## Table of contents

1. [Background and motivation](#1-background-and-motivation)
2. [Vanilla PostgreSQL cancel mechanism](#2-vanilla-postgresql-cancel-mechanism)
3. [PolarDB proxy-mode extension (verified backend contract)](#3-polardb-proxy-mode-extension-verified-backend-contract)
4. [Current ProxySQL state and the precise gap](#4-current-proxysql-state-and-the-precise-gap)
5. [General design (invariants and model)](#5-general-design-invariants-and-model)
6. [ProxySQL-branch implementation design](#6-proxysql-branch-implementation-design)
7. [Interaction with split / warmup and identity-aware pooling](#7-interaction-with-split--warmup-and-identity-aware-pooling)
8. [Testing and verification plan](#8-testing-and-verification-plan)
9. [Phasing and open questions](#9-phasing-and-open-questions)

---

## 1. Background and motivation

### 1.1 What the capability is

Every PostgreSQL backend hands the frontend, at the end of authentication, a
`BackendKeyData` (`'K'`) message carrying a `(backendPID, secretKey)` pair. To
cancel a running query the frontend opens a *new* TCP connection and sends a
`CancelRequest` packet quoting that pair; the postmaster matches the pair to a
live backend process and signals it with `SIGINT`. The cancel pair is therefore
**the addressing identity of one specific backend process**.

PolarDB's proxy mode breaks the 1:1 "one frontend connection == one backend PID"
assumption that native cancel relies on, because a proxy pools and multiplexes
backends behind it. PolarDB adds two startup-packet keys —
`_polar_proxy_session_id` (a virtual **session id**, "SID") and
`_polar_proxy_cancel_key` — which become the proxy-visible *replacement* for the
real backend PID and key both in the `'K'` message and in the `CancelRequest`
matching path. A `CancelRequest` whose `backendPID` field falls in the proxy-SID
range (strictly `> 10_000_000`) is resolved through a SID→PID lookup table inside
the backend node rather than by raw PID match. This lets a cancel target a
pooled/proxied backend by a stable virtual SID instead of by the hidden real PID,
and it makes `pg_stat_activity` / logs carry the proxy-assigned SID instead of an
OS PID the client could never have learned.

### 1.2 Who needs it

- Operators running ProxySQL in front of PolarDB who want
  `pg_stat_activity` / audit logs to carry a stable, proxy-meaningful session
  identity rather than a transient pooled PID, and who want client-issued cancels
  (libpq `PQcancel`, `pg_cancel_backend`, psql Ctrl-C) to reach the backend doing
  the work.
- The PolarDB **transaction-split** feature inside ProxySQL, where *one frontend
  session is served by more than one backend at once* (primary + replica). Each
  of those backends is a distinct OS process and, in proxy mode, needs a
  **distinct** SID — they must never collide on a node.

### 1.3 The failure if absent (current state)

ProxySQL today does **not** send `_polar_proxy_session_id` /
`_polar_proxy_cancel_key`. Its backend startup-param builder
`PgSQL_Connection::append_polardb_startup_params`
(`lib/PgSQL_Connection.cpp:1500`) emits only the client host/port and the
send-lsn/send-xact flags (`lib/PgSQL_Connection.cpp:1538-1561`). The consequences
are *not* "cancel is broken" — ProxySQL's own `PQcancel`-based path works today
(see §4.3) — but rather a loss of control over the backend-visible identity:

- On a **primary** PolarDB in proxy mode with no SID supplied, the backend
  auto-assigns `polar_proxy_session_id = MyProcPid + POLAR_BASE_PROXY_SID` and
  `cancel_key = MyCancelKey`
  (`src/backend/utils/activity/backend_status.c:405-409`). Cancel still works (the
  backend echoes that auto SID in its `'K'`), but the SID is non-deterministic and
  ProxySQL can neither pre-allocate it nor reason about it.
- On a **non-primary** node (standby/RO) in proxy mode with no SID supplied, the
  backend sets `polar_proxy_session_id = 0` and `cancel_key = 0`
  (`backend_status.c:410-414`; the branch tested is `polar_is_primary()` at
  `:405`, not a literal "replica" role). The advertised cancel identity on such a
  session has **no proxy SID at all**. For the split feature, where the read leg
  lives on a replica, this means there is no proxy-stable session identity for
  that leg unless ProxySQL supplies one.

So the failure mode is: *ProxySQL has no control over, and no stable knowledge of,
the backend-visible session identity, and on non-primary nodes there may be none
at all* — which is exactly what the split feature and proper `pg_stat_activity`
attribution require.

---

## 2. Vanilla PostgreSQL cancel mechanism

The wire-protocol baseline that PolarDB's proxy mode extends:

1. After auth, the backend sends `BackendKeyData` (`'K'`): `Int32 PID`,
   `Int32 secretKey`. In the vanilla server these are `MyProcPid` and
   `MyCancelKey`.
2. libpq stores them in `conn->be_pid` / `conn->be_key` on receipt of `'K'`
   (`src/interfaces/libpq/fe-protocol3.c`, the `'K'` case storing
   `conn->be_pid` / `conn->be_key`).
3. To cancel, the client builds a `PGcancel` by copying those fields
   (`PQgetCancel`: `cancel->be_pid = conn->be_pid; cancel->be_key = conn->be_key;`)
   and `PQcancel` sends a `CancelRequest` with `backendPID = be_pid`,
   `cancelAuthCode = be_key`.
4. The postmaster's `processCancelRequest` walks the backend list, matches
   `pid == backendPID && cancel_key == cancelAuthCode`, and signals that process
   with `SIGINT`.

The cancel pair is a *direct* addressing identity of one backend process. There
is no indirection and no proxy concept. PolarDB inserts an indirection layer
(SID → real PID) at step 4 and changes what the backend puts in `'K'` at step 1.

---

## 3. PolarDB proxy-mode extension (verified backend contract)

References in this section use the PolarDB backend branch
`POLARDB_15_PROXY`.

### 3.1 Startup parsing (`postmaster.c` `ProcessStartupPacket`)

PolarDB parses extra top-level conninfo keys, all under the `_polar_` prefix
(`postmaster.c:2410-2438`):

| Key | Field | Validation | Flag set |
|---|---|---|---|
| `_polar_proxy_session_id` | `port->polar_proxy_session_id` (`int`) | `parse_int(...)` succeeds **and** value `!= 0`, else `FATAL` "invalid value … Valid values is not-0-integer" | `got_session_id = true` (`:2427`) |
| `_polar_proxy_cancel_key` | `port->polar_proxy_cancel_key` (`int32`) | `parse_int(...)` succeeds; **zero is allowed** | `got_cancel_key = true` (`:2438`) |

The sibling proxy keys parsed in the same block are
`_polar_proxy_client_host`/`_port` (legacy `_polar_origin_client_ip`/`_port`,
`:2414-2417`), `_polar_proxy_send_lsn` (legacy `_polar_send_lsn`),
`_polar_proxy_send_xact`, and the SSL trio `_polar_proxy_use_ssl` /
`_polar_proxy_ssl_version` / `_polar_proxy_ssl_cipher_name`. SID/cancel and SSL
are independent feature families that share the same controlling (§3.2).

The SID/cancel field types are confirmed in `src/include/libpq/libpq-be.h:225-226`
(`int polar_proxy_session_id;`, `int32 polar_proxy_cancel_key;`). `parse_int`
(guc.c) with `flags=0` fills a signed int, so the usable SID span is bounded by
`INT32_MAX`.

### 3.2 Rules for sending these parameters together

- **Proxy mode requires client host AND port.** `polar_proxy = true` only when
  both `client_host` and `client_port` are present; if exactly one is present the
  backend raises `FATAL` "[Proxy] proxy info is incomplete." When proxy mode is
  on, the backend rewrites `remote_host`/`remote_port` to the advertised client
  address and encodes `polar_proxy_client_raddr` (`postmaster.c:2506-2540`).
- **SID and cancel-key must come as a pair.**
  `if (got_session_id != got_cancel_key) ereport(FATAL, "[Proxy] Should set both
  session id and cancel key.")` (`postmaster.c:2551-2552`). You cannot send one
  without the other.
- **SID/cancel are only meaningful inside proxy mode.** The pairing check above
  lives in the `else` branch that runs *only when `polar_proxy` was turned on*.
  The broader condition `if (!polar_proxy && (send_lsn || ssl_in_use)) FATAL "[Proxy]
  Proxy is disabled, unable to use lsn or ssl"` (`postmaster.c:2555-2559`)
  confirms proxy mode is a prerequisite for the whole `_polar_proxy_*` family.
  **Operational consequence: to send a SID you must also advertise a client
  host/port, i.e. you must already be running in proxy mode.**

### 3.3 SID range and uniqueness (`backend_status.h`, `backend_status.c`)

- `POLAR_BASE_PROXY_SID = 10000000`;
  `POLAR_IS_PID(pid)      = (pid > 0 && pid < 10^7)`;
  `POLAR_IS_PROXY_SID(pid) = (pid > 10^7)`;
  `POLAR_IS_VALID_SID     = IS_PID || IS_PROXY_SID`
  (`src/include/utils/backend_status.h:352-355`). **The boundary is strict `>`,
  so `10000000` itself is neither a valid PID nor a valid proxy SID.** The
  backend's own error text confirms the range: "should between (10^7, INT_MAX]"
  (`backend_status.c:1485`).
- **Auto-assign** (no SID supplied, proxy mode on; `backend_status.c:400-414`):

  | Node | Result |
  |---|---|
  | `polar_is_primary()` true | `session_id = MyProcPid + POLAR_BASE_PROXY_SID`, `cancel_key = MyCancelKey` (`:406-409`) |
  | otherwise (standby/RO) | `session_id = 0`, `cancel_key = 0` (`:411-414`) |

- **Explicit SID set** — `polar_proxy_set_sid` (`backend_status.c:1480` onward):
  - Rejects non-proxy-mode: `elog(ERROR, "POLAR: Unable to set proxy sid when not
    under proxy mode")` (`:1483`).
  - Rejects `!POLAR_IS_PROXY_SID(proxy_sid)`: `elog(ERROR, "POLAR: Invalid proxy
    sid: %d, should between (10^7, INT_MAX]")` (`:1484-1485`).
  - **Scans the entire `BackendStatusArray` and raises an ERROR if the SID is
    already in use by another live backend**: when `save_procpid > 0 &&
    save_polar_proxy_sid == proxy_sid`, it does `elog(ERROR, "POLAR: The proxy sid
    %d is already in use")` (`:1528-1531`). **This uniqueness check is per-node
    only — it scans *this node's* shared-memory `BackendStatusArray`. It is NOT
    replicated across primary↔replica or across nodes.**
- `polar_proxy_set_cancel_key` simply stores the value.

> **Severity precision (corrected from draft).** The duplicate-SID failure is
> `elog(ERROR)`, **not** `FATAL`. It is raised from `pgstat_bestart()` during
> `InitPostgres`, i.e. after authentication, inside the bootstrap transaction;
> an ERROR there aborts the bootstrap and the connection attempt fails.
> Operationally the connect still fails (so a retry strategy is reachable), but
> two things follow that the implementation must respect:
> 1. It is an `elog(ERROR)`, so the SQLSTATE is the generic `XX000` — there is
>    **no dedicated error code**. The only discriminator is the English message
>    substring "is already in use", which is locale- and version-fragile (see
>    §6.6, G3).
> 2. Because it is an ERROR inside bootstrap rather than a protocol-level FATAL,
>    ProxySQL must surface it as a *connect failure*, not silently treat the
>    backend as usable.

### 3.4 The `'K'` message in proxy mode (`postgres.c`) — the linchpin

When the backend reports cancellation info to the frontend, it calls
`polar_proxy_get_sid(MyProcPid, &proxy_cancel_key)` and sends *that* as the `'K'`
`session_id`; if the result is a proxy SID it sends `proxy_cancel_key`, otherwise
it sends `MyCancelKey` (`postgres.c:4626-4640`):

```c
int32 session_id = polar_proxy_get_sid(MyProcPid, &proxy_cancel_key);
pq_beginmessage(&buf, 'K');
pq_sendint32(&buf, (int32) session_id);
if (POLAR_IS_PROXY_SID(session_id))
    pq_sendint32(&buf, (int32) proxy_cancel_key);
else
    pq_sendint32(&buf, (int32) MyCancelKey);
```

`polar_proxy_get_sid` returns `my_beentry->polar_proxy_session_id`
(`backend_status.c:1658-1662`), which `polar_proxy_set_sid` populated from the
SID ProxySQL supplied.

> **In proxy mode, the `BackendKeyData` the backend sends to ProxySQL's libpq is
> the SID + cancel_key, not the real PID. When ProxySQL supplies a SID, the
> backend echoes *that exact SID* back in `'K'`.**

This is the load-bearing fact for §4.3: libpq stores it into `be_pid`/`be_key`
and ProxySQL's existing `PQcancel` path uses it verbatim — so a SID ProxySQL
supplies is self-consistent end-to-end without any extra bookkeeping for
ProxySQL's *own* cancels.

### 3.5 Cancel routing in proxy mode (`postmaster.c`)

`processCancelRequest`: if `POLAR_IS_PROXY_SID(backendPID)`, it resolves the real
PID via `polar_proxy_get_pid(backendPID, cancelAuthCode, true)` and signals that
process with `SIGINT`. `polar_proxy_get_pid` (`backend_status.c:1573` onward)
scans `BackendStatusArray` for `save_polar_proxy_sid == proxy_sid`, then:

| Outcome | Return |
|---|---|
| No matching SID | `0` |
| SID matches, `auth_cancel_key` true, cancel-key mismatch | `InvalidPid` (`-1`) |
| SID matches, cancel-key OK (or `auth_cancel_key` false) | the real `st_procpid` (`> 0`) |

The return-code contract is documented at `backend_status.c:1565-1571`.

### 3.6 libpq exposure (env vs conninfo)

In *upstream* PolarDB libpq these keys are exposed as `PQEnvironmentOption`
entries (env-var → startup-option mappings); that is why the PolarDB TAP test
`008_proxy.pl` injects them via environment. **This is a libpq convenience, not a
backend env read** — the backend only ever sees startup-packet options (§3.1).
(The upstream env-table cite is not in the tree under design and the ProxySQL
design does not depend on it; ProxySQL uses the conninfo path below.)

The **ProxySQL-side patched libpq** (`deps/postgresql/polardb_libpq.patch`)
registers these as *real conninfo options* in `PQconninfoOptions`
(`patch:57-63` for `_polar_proxy_session_id` / `_polar_proxy_cancel_key`;
`patch:65-75` for the SSL trio), declares the backing `PGconn` fields
(`patch:398-402`), frees them in `freePGconn` (`patch:122-126`), and **emits them
in the startup packet** (`patch:324-327`):

```c
if (conn->_polar_proxy_session_id && conn->_polar_proxy_session_id[0])
    ADD_STARTUP_OPTION("_polar_proxy_session_id", conn->_polar_proxy_session_id);
if (conn->_polar_proxy_cancel_key && conn->_polar_proxy_cancel_key[0])
    ADD_STARTUP_OPTION("_polar_proxy_cancel_key", conn->_polar_proxy_cancel_key);
```

> **Important: libpq emits the two keys *independently*** — each is protected only by
> its own non-empty-string test, with no pairing enforced in libpq. The backend
> FATALs on `got_session_id != got_cancel_key` (§3.2). **Therefore the "both or
> neither" invariant must be enforced entirely in the ProxySQL C++ builder; libpq
> does not protect us.** The libpq plumbing already exists; the only missing piece
> is the ProxySQL builder populating these two conninfo keys.

---

## 4. Current ProxySQL state and the precise gap

References in this section use the ProxySQL branch `polardb-dev`.

### 4.1 The backend startup builder

`PgSQL_Connection::append_polardb_startup_params`
(`lib/PgSQL_Connection.cpp:1500`) emits, for the V15 dialect,
`_polar_proxy_client_host`/`_port` + optional `_polar_proxy_send_lsn`/`_send_xact`
(`:1538-1549`); for the LEGACY dialect, the `_polar_origin_client_ip`/`_port` +
`_polar_send_lsn`/`_send_xact` variants (`:1551-1561`). It does **not** emit
`_polar_proxy_session_id` or `_polar_proxy_cancel_key` (nor the SSL trio).
Identity is chosen by `resolve_polardb_startup_identity` (CLIENT →
LISTENER_PROXY → CONFIGURED_FALLBACK → NONE, enum at
`include/PgSQL_PolarDB.h:464`), and a profile that requests RFQ payloads but
resolves to `NONE` is refused (`:1513-1525`). Both call sites are protected by
`#if POLARDB_PROXY` (`lib/PgSQL_Connection.cpp:1317-1334` client-backed, calls at
`:1326` / `:1342`).

The connection records what it requested in `polardb_startup_profile` (type
`PolarDB_StartupProfile`, `include/PgSQL_PolarDB.h:481` onward). **There is no
field on `PgSQL_Connection` for an allocated SID or cancel-key.**

### 4.2 ProxySQL's frontend cancel is its own, and authoritative

ProxySQL hands the *client* its own synthetic `BackendKeyData`, not the
backend's: `backend_pid = (*myds)->sess->thread_session_id`, with a fresh random
`cancel_key` stored into `sess->cancel_secret_key`
(`lib/PgSQL_Protocol.cpp:1406-1412`). The real backend key is hidden from the
client. The two fields are `PgSQL_Session::thread_session_id` and
`PgSQL_Session::cancel_secret_key`.

A client `CancelRequest` is parsed in `process_pkt_handshake_response`, which
extracts `pid`,`key` and calls
`GloPTH->kill_connection_or_query(pid, key, nullptr, true)`
(`lib/PgSQL_Protocol.cpp:694`). That posts a `query_ids{pid,key}` entry into every
worker thread's `sess_intrpt_queue` (`lib/PgSQL_Thread.cpp:5694-5723`). Each
worker matches it in `Scan_Sessions_to_Kill___handle_query_cancellation`: only
when **both** `thread_id == sess->thread_session_id` **and** `secret_key ==
sess->cancel_secret_key` match does it set the cancel flag on the attached backend
data stream. Because *both* the id and the secret must match, a stale client
cancel cannot match a different session — this substantiates INV-4 below.

### 4.3 How ProxySQL actually cancels the backend (the load-bearing path)

The per-backend cancel flag drives
`PgSQL_Session::handler_again___new_thread_to_cancel_query`
(`lib/PgSQL_Session.cpp:1274-1302`), which builds a `PgSQL_Backend_Kill_Args`
from the *live backend connection* — `(PGconn*)myds->myconn->get_pg_connection()`
— and spawns a detached `PgSQL_backend_kill_thread`. That thread calls
`PQcancel(cancel_conn, …)` (`lib/PgSQL_Connection.cpp:3676`). The `cancel_conn`
is a `PGcancel*` captured at connect time by `PQgetCancel(conn)`
(`lib/PgSQL_Connection.cpp:3598`); the success log is at `:3682`
("Canceled query on … with backend PID %d successfully").

> **Consequence.** ProxySQL's *own* backend cancel rides on libpq's
> `be_pid`/`be_key`, which in PolarDB proxy mode are *already the SID/cancel_key
> the backend put in `'K'`* (§3.4). So ProxySQL's existing cancel works regardless
> of whether ProxySQL supplied the SID or the backend auto-assigned it. **The SID
> feature is therefore NOT primarily about making ProxySQL's own cancel work — it
> works today. It is about (a) making the backend-visible session identity
> deterministic, controllable, and present on non-primary nodes, and (b)
> supporting the split topology where multiple backends serve one frontend and
> each needs a distinct, ProxySQL-known SID.** This reframing is the most
> important factual correction this design carries.

### 4.4 The pool-compatibility key ignores PolarDB identity

`PgSQL_Connection::has_same_connection_options` compares username + dbname only
(it short-circuits true when the userinfo hash matches;
`lib/PgSQL_Connection.cpp:2638`). The RFQ reader-acquisition scan
(`lib/PgSQL_HostGroups_Manager.cpp:5395-5432`) checks `has_rfq_lsn()` capability
(`:5407`) + DB options (`has_same_connection_options` at `:5417`) + a session-var
match, but keys on neither the advertised PolarDB startup identity nor any SID.
Generic reuse (`get_random_MyConn_inner_search`) likewise only uses
`has_same_connection_options`.

A subtlety that *strengthens* the design: pool reuse does **not** reconnect. The
reset path is SQL-level `DISCARD ALL` / `ROLLBACK` on the existing connection
(`lib/PgSQL_Connection.cpp` reset path), with no `PQreset` or re-handshake. **The
startup-packet SID therefore survives reuse unchanged** — which is exactly what
INV-3 below requires.

> **The gap, precisely.** A PolarDB SID set at startup is *fixed for the life of
> the pooled backend* (it is a startup-packet property; libpq sends it once). The
> pool reuses one backend across many frontend sessions without reconnecting. So a
> SID, once chosen, is bound to a *backend*, not to a *frontend session*. Any
> design that tries to make the SID track the frontend session is fighting the
> pool. This is the same structural gap the SSL feature has (frontend SSL state
> vs. pooled backend), but the two resolve *differently*: SSL state must enter the
> pool key, the SID must not (§6.4).

---

## 5. General design (invariants and model)

### 5.1 Core invariants

- **INV-1 (per-node SID uniqueness).** No two simultaneously-live backends on a
  PolarDB *node* may carry the same SID. The backend enforces this at
  `polar_proxy_set_sid` (`backend_status.c:1528-1531`) with an
  `elog(ERROR)` — but only the backend that loses the race learns about it, as a
  **connection-aborting ERROR** (not a FATAL; §3.3). ProxySQL must therefore never
  knowingly issue a duplicate SID to two backends on the same node, or it converts
  a benign reuse into a failed connection.
- **INV-2 (split distinctness, by construction).** When one frontend session holds
  two backends (primary + replica), each gets a *distinct* SID. This distinctness
  comes from **per-connection allocation** — each backend connect draws a fresh
  SID — **not** from the legs being on different nodes. Stating it this way covers
  the degraded same-node topology (e.g. a reader hostgroup pointing at the
  primary): even if both legs land on one physical node, per-connection allocation
  keeps their SIDs distinct, so the per-node uniqueness check (INV-1) is satisfied
  by construction.
- **INV-3 (SID is a backend property, not a frontend property).** Because the SID
  is a startup-packet field and backends are pooled and reused without reconnect
  (§4.4), the SID belongs to the `PgSQL_Connection` for that connection's life. A
  frontend session temporarily uses a backend (and its SID) for the duration of use.
- **INV-4 (ProxySQL frontend cancel stays authoritative).** The
  `thread_session_id`/`cancel_secret_key` handle ProxySQL gives the client (§4.2)
  is never replaced by the backend SID. The backend SID is an *internal routing
  detail* for backend-directed cancels; clients keep talking to ProxySQL's
  synthetic key, and a stale client cancel cannot match a reused session because
  both the id and the secret must match (§4.2).
- **INV-5 (proxy-mode prerequisite, not RFQ-coupled).** A SID may be sent only
  when client host/port are also sent — i.e. proxy mode is on, which in ProxySQL
  terms means a non-NONE identity resolves (§3.2). This is **independent of
  whether RFQ LSN/XID is requested**; see §6.2 / G4.

### 5.2 The two disjoint ID spaces (no cross-collision)

ProxySQL maintains *two unrelated* identity spaces, and a reader must not fear
them colliding:

| Space | Value | Where it lives | Sent to PolarDB? |
|---|---|---|---|
| ProxySQL frontend session id | `thread_session_id`, a monotonic global counter (`__sync_fetch_and_add(&glovars.thread_id,1)`, `lib/PgSQL_Session.cpp:1031`) advertised to the *client* as ProxySQL's frontend `backend_pid` (`PgSQL_Protocol.cpp:1406`) | frontend (client ↔ ProxySQL) | **No** |
| PolarDB backend SID | `(10^7, INT32_MAX]`, ProxySQL-chosen per backend connection | backend (ProxySQL ↔ PolarDB) | **Yes** (`_polar_proxy_session_id`) |

`thread_session_id` is monotonic and over a long-lived process **can exceed
10,000,000**, which is the PolarDB proxy-SID base. This is harmless: the client's
`CancelRequest` is matched *inside ProxySQL* against `thread_session_id` and is
**never relayed verbatim to the backend**. When ProxySQL cancels the backend it
builds a fresh `PQcancel` from the backend's own `be_pid`/`be_key` (§4.3), which
in proxy mode is the SID the backend put in `'K'`. The two spaces never cross, so
a `thread_session_id` that happens to land above `10^7` cannot be mistaken for a
PolarDB SID.

### 5.3 The right model

- ProxySQL allocates a SID *per backend connection* at connect time and writes it
  into the conninfo (`_polar_proxy_session_id` + `_polar_proxy_cancel_key`). The
  cancel-key is ProxySQL-chosen random and need not relate to anything the client
  sees.
- The SID lives in a new `PgSQL_Connection` field, released when the connection is
  destroyed; on pool reuse the SID is *kept* (INV-3) — it is the backend's
  identity.
- For frontend→backend cancel, ProxySQL keeps using its existing
  `PQcancel`-on-live-`PGconn` path (§4.3). Because the backend echoes our SID in
  `'K'` and libpq captured it, the `PGcancel` already carries the right SID —
  **no change to the cancel path is needed for the common case**. The SID's added
  value is that the identity is now *deterministic and ProxySQL-chosen*, which
  matters for cluster uniqueness (§6.6) and for the split legs (INV-2).
- For the split topology, the two backends are acquired independently and each
  gets its own SID through the same per-connection allocation — INV-2 falls out
  for free.

### 5.4 Why the SID is proxy-connection identity, never client identity

The established position for *client-host* identity is that advertising
ProxySQL's listener identity on a reusable backend collapses distinct clients
into one backend-visible identity, breaking HBA/auditing. The SID has the same
shape: it is per-backend and is what shows up in `pg_stat_activity` /
`pg_terminate_backend` / logs. A pooled backend keeps one SID across many client
sessions, so a DBA reading `pg_stat_activity` sees a single long-lived proxy SID,
not the actual client behind the current query. That is acceptable and even
honest for a *pooled* connection (it is "the proxy's connection #N"), but it means
**the SID must be understood as proxy-connection identity, never client
identity.** Do not try to rotate the SID per client — a SID is a startup-only
property and cannot change without reconnecting; let it be the pooled
connection's stable id.

---

## 6. ProxySQL-branch implementation design

> **Build-tier check requirement (applies to everything below).** Off mode must
> expose no PolarDB identity/cancel surface and must retain ordinary PostgreSQL
> behavioral compatibility. **Every new PolarDB field, allocator,
> counter, and code path in this section MUST sit inside `#if POLARDB_PROXY`** (or
> be added only to the PolarDB-specific files), or that invariant breaks.

### 6.1 Data structures

New fields on `PgSQL_Connection` (alongside `polardb_startup_profile`), all under
`#if POLARDB_PROXY`:

- `int32_t polardb_proxy_sid {0};` — `0` = none/unset; otherwise a value in
  `(10_000_000, INT32_MAX]`. **Invariant: every non-zero value must satisfy
  `POLAR_IS_PROXY_SID` (strict `>`, §3.3).**
- `int32_t polardb_proxy_cancel_key {0};` — ProxySQL-chosen random `int32`.
- (optional) `bool polardb_sid_proxy_assigned {false};` — distinguishes a
  ProxySQL-chosen SID from a backend-auto-assigned one; only needed if split
  consumes the SID (§6.4, deferred).

These are *backend-connection* lifetime (INV-3): set once at connect, kept across
pool reuse, cleared on destroy.

**SID allocator.** Two candidate homes:

- a per-`PgSQL_Thread` counter (thread-local, lock-free) feeding a global
  reconciliation, or
- a single global `std::atomic<uint32_t>` cursor in `PgSQL_HostGroups_Manager`.

Given the int32 budget (§6.6) and the low allocation rate (one per backend
connect), a **single global atomic cursor is simplest** and adequate; contention
is not the concern, the encoding and per-node-uniqueness handling are.

### 6.2 Hook points

- **Emit.** Extend `append_polardb_startup_params`
  (`lib/PgSQL_Connection.cpp:1500`) so that, for a connection in proxy mode (a
  resolved non-NONE identity) **and** with `pgsql-polardb_proxy_send_session_id`
  true, it allocates `polardb_proxy_sid`/`polardb_proxy_cancel_key` (if not
  already set) and appends `_polar_proxy_session_id=<sid>` and
  `_polar_proxy_cancel_key=<key>`. **It must emit both or neither** — libpq does
  not enforce the pairing (§3.6) and the backend FATALs on
  `got_session_id != got_cancel_key` (`postmaster.c:2551-2552`).
- **condition (corrected from draft).** The emission condition is **proxy-mode + knob**, not
  RFQ. `emits_startup_params()` returns `protocol != OFF && request_bits != 0`
  (`include/PgSQL_PolarDB.h:519`), i.e. it is true only when RFQ LSN/XID is
  requested. Today every non-OFF profile sets both RFQ bits, so
  `emits_startup_params() == (protocol != OFF)` and the coupling is *currently*
  harmless — but SID/cancel is logically independent of RFQ. If a future profile
  turns RFQ off while keeping proxy mode on, SID emission must not silently stop.
  **condition SID on: a non-NONE identity resolves (proxy mode) AND
  `pgsql-polardb_proxy_send_session_id` is true — not on `request_bits`.**
- **Dialect (corrected from draft).** v1 emits SID only for the V15 dialect, as a
  deliberate **ProxySQL simplicity choice**, *not* because the backend lacks a
  legacy parser. The backend parses `_polar_proxy_session_id` /
  `_polar_proxy_cancel_key` regardless of which client-host dialect
  (`_polar_proxy_client_host` *or* legacy `_polar_origin_client_ip`,
  `postmaster.c:2414-2417`) established proxy mode — a LEGACY connection that also
  sent the SID keys *would* be accepted. We restrict to V15 to keep one code path;
  the restriction can be lifted later with no backend change.
- **No change** to `lib/PgSQL_Protocol.cpp:1406` (frontend `'K'` to the client) —
  the client still gets ProxySQL's synthetic key (INV-4).
- **No change** required to `handler_again___new_thread_to_cancel_query` /
  `PQcancel` (§4.3) for the common case — libpq already carries the SID.
- **Observability.** Record the chosen SID at connect (counter + optional trace)
  and surface it where backend connections are listed (§6.10).

### 6.3 libpq patch additions

**None required.** `deps/postgresql/polardb_libpq.patch` already registers
`_polar_proxy_session_id` / `_polar_proxy_cancel_key` as conninfo options
(`patch:57-63`), declares the `PGconn` fields (`patch:398-399`), frees them
(`patch:122-123`), and emits them in the startup packet (`patch:324-327`). (The
SSL trio is likewise already present, `patch:65-75` / `:328-333`.) The patch must
continue to be applied during the deps build; the only standing risk is a future
libpq bump silently dropping it. As noted in §3.6, libpq emits the two keys
independently, so the "both or neither" pairing is solely the C++ builder's
responsibility.

### 6.4 Pool-key changes

The SID itself does **not** enter the pool-compatibility key, *because it is
intentionally stable across reuse* (INV-3) — a pooled backend keeps its SID and
that is correct. What changes:

- Acquisition must not assume "fresh SID for each backend use." The reader-acquisition
  scan (`HGM.cpp:5395-5432`) and generic reuse are unaffected; they reuse the
  backend (and its SID) as-is.
- The only pool interaction to police (deferred): a backend created *without* a
  ProxySQL-chosen SID (backend-auto-assigned, or built before this feature was
  enabled) must not be treated as interchangeable with a proxy-assigned-SID
  backend *if split logic depends on knowing the SID*. If so, record
  `polardb_sid_proxy_assigned` (§6.1) — a *capability* flag parallel to
  `has_rfq_lsn()`, not a per-client key — and let split prefer proxy-assigned
  connections. Defer until split actually consumes the SID (§7, §9).

> **Implementation trap (the existing struct invites it).**
> `PolarDB_StartupClientContext` already declares `has_proxy_session` /
> `proxy_session_id` / `proxy_cancel_key` placeholder fields, and
> `compatible_for_reuse` (`include/PgSQL_PolarDB.h:656-658`) compares **all three**
> in its equality. The SID and cancel-key must therefore live on the **separate**
> `PgSQL_Connection` fields of §6.1 (`polardb_proxy_sid` /
> `polardb_proxy_cancel_key`), and these three `StartupClientContext` placeholders
> must stay **unpopulated** (`has_proxy_session=false`, ids `0`). Populating them
> with a per-connection SID would make `compatible_for_reuse` return false for
> every distinct-SID backend pair — i.e. **shatter pool reuse entirely**, since no
> two backends ever share a SID (INV-2). This is the inverse of the SSL sibling,
> which deliberately *does* populate its `frontend_ssl`/`ssl_version`/`ssl_cipher`
> placeholders so the pool key buckets on them.
>
> **Explicit contrast with the SSL sibling feature.** **SSL state MUST enter the
> pool key** — a backend opened under client-TLS=on cannot serve a client-TLS=off
> frontend, because PolarDB HBA `hostssl`/`hostnossl` matching used the
> proxy-advertised SSL flag, so the backend semantics differ by frontend SSL
> state. **SID MUST NOT enter the pool key** — it is deliberately backend-stable
> and carries no per-frontend semantics that pooling must isolate. An implementer
> must not over-apply the SSL rule to the SID.

### 6.5 Config knobs (string-form, per ProxySQL conventions)

Following the existing `polardb_proxy_*` variable family (declared in
`lib/PgSQL_Thread.cpp:413-417`, defaults at `:1166-1170`):

| Variable | Type / values | Default | Meaning |
|---|---|---|---|
| `pgsql-polardb_proxy_send_session_id` | bool `"true"`/`"false"` | `"false"` | When false: exactly today's behavior (backend auto-assigns / 0). When true: ProxySQL allocates and sends SID+cancel_key for V15 proxy-mode connections. Default-off keeps vanilla-backend and PolarDB-without-need deployments untouched. |
| `pgsql-polardb_proxy_instance_id` (cluster) | int `0..N` | `0` | Per-ProxySQL-instance component baked into the SID encoding (§6.6). Must be unique across ProxySQL instances that share a PolarDB node. |

These follow the existing variable registration / get-set pattern used for the
`polardb_proxy_*` family. **Note (corrected from draft):** there is no standalone
`polardb_proxy_send_lsn` config variable to mirror — `send_lsn` is derived from
the startup *profile* (`has_rfq_lsn()`), not a knob. The registration pattern,
not a specific RFQ knob, is what `send_session_id` should imitate.

### 6.6 SID encoding and range budget (cluster uniqueness)

The usable SID space is `(10_000_000, 2_147_483_647]` (`backend_status.h:352-355`;
`INT32_MAX` because the field is a signed int parsed by `parse_int`). **The
backend's uniqueness check is per-node only and is NOT cluster-replicated (§3.3).**
A ProxySQL *cluster* whose members share a PolarDB node will collide if every
member allocates from the same low range.

Proposed encoding (an `int32` strictly above the base):

```
offset = (instance_id << SEQ_BITS) | (seq & SEQ_MASK)     // 0 .. 2^30-1
sid    = POLAR_BASE_PROXY_SID + 1 + offset                // >= 10_000_001
```

> **Off-by-one fix (corrected from draft, G1).** The naive formula
> `BASE + (instance_id<<SEQ_BITS) + (seq & SEQ_MASK)` yields exactly `10_000_000`
> when `instance_id=0, seq=0`, which `POLAR_IS_PROXY_SID` **rejects** (strict `>`,
> §3.3 → `set_sid` raises "Invalid proxy sid"). The `+ 1` above forces the minimum
> emitted SID to `10_000_001`. **Invariant: `POLAR_IS_PROXY_SID(sid)` must hold
> for every emitted SID; assert it in the unit test (§8.1).**

Budget within the ~31-bit usable span above `10^7`
(`2_147_483_647 - 10_000_000 = 2_137_483_647 ≈ 2^30.99`):

- `INSTANCE_BITS = 6` (64 ProxySQL instances) and `SEQ_BITS = 24`
  (16,777,216 live+recent SIDs per instance) → `6 + 24 = 30` bits, max offset
  `2^30 - 1 = 1_073_741_823`, comfortably under the span even with the `+ 1`.
  `SEQ_BITS` could widen to 25 (~33M/instance) and still fit.
- `seq` is a per-instance monotonic counter that **wraps** within `SEQ_MASK`. Wrap
  is safe because a SID need only be unique among *currently live* backends on a
  node; 16M outstanding backend connections from one ProxySQL instance to one node
  is far beyond any real pool size, so by the time `seq` wraps the old SID's
  backend is long gone.
- `instance_id` comes from `pgsql-polardb_proxy_instance_id` (§6.5). For v1,
  operator-configured is sufficient; a central allocator is deferred (§9).

**Collision handling — and its honest limit (G2, G3).**

- *Transient/intra-instance race* (two near-simultaneous connects from one
  instance briefly pick colliding SIDs against the same node): treat the backend
  ERROR as a **retryable connect failure** — re-allocate the next `seq` and retry
  once or twice. This resolves the race because the retry draws a different `seq`.
- *Sustained duplicate `instance_id`* (two instances misconfigured with the same
  `instance_id`): **retry does NOT cure this.** Both instances allocate from the
  same band, so retrying picks the next `seq` in the *same* band and can keep
  colliding with the peer's live allocations. `instance_id` uniqueness is an
  **operator invariant** that the connect-retry cannot repair. The design must
  therefore *detect and alert* on sustained collisions (a dedicated counter +
  log, §6.10), not present retry as a fix.
- *Detection fragility:* the duplicate-SID error is `elog(ERROR)` → SQLSTATE
  `XX000`, with no dedicated error code (§3.3). The only discriminator is the
  message substring **"is already in use"**, which is locale- and
  version-dependent. v1 accepts message-substring matching **with an explicit
  fragility caveat and a dependency on the backend version**; a dedicated backend errcode
  is listed as a G0 backend ask (§8.3, §9). The design must not overstate
  detectability.

### 6.7 Mapping frontend session → active backend SID(s)

For ProxySQL's own cancels the mapping is implicit: the active backend is
`sess->mybe->server_myds->myconn`, and its `PQgetCancel` already carries the SID
(§4.3). For split, ProxySQL holds more than one backend; the cancel path operates
on `mybe->server_myds` (the currently-attached backend). The split backend is a
distinct `PgSQL_Backend*` (`polardb_txn_split_backend`,
`lib/PgSQL_PolarDB_Split.cpp:136-204`) with its own `server_myds`, and `mybe` is
repointed to it during a split read (`:204`); the saved primary is
`polardb_txn_split_saved_mybe` (`:203`). Therefore:

- **v1:** cancel the *currently attached* backend (existing behavior). During a
  split read the attached backend is the reader leg, so a cancel reaches it
  correctly.
- **Future (split-aware cancel):** keep a list of active `(role,
  PgSQL_Connection*)` on the session and, on cancel, `PQcancel` *each* live leg.
  Each leg's `PGcancel` carries its own distinct SID (INV-2), so the two
  `CancelRequest`s target two different backends correctly. This depends on the
  split work's leg-tracking structure; defer to §7.

### 6.8 Send-SID vs. let-backend-auto-assign; pooled-reuse semantics

- **On a primary**, not sending a SID still yields a working cancel
  (auto-assign, §3.3) but a non-deterministic SID ProxySQL cannot predict or keep
  cluster-unique. **On a non-primary node**, not sending a SID yields `SID=0` / no
  proxy cancel identity (`backend_status.c:411-414`). So *if the deployment uses
  replicas in proxy mode* (which the split/RFQ feature does), ProxySQL should send
  the SID to obtain a stable session identity on the read leg.
- **Pooled-reuse semantics.** A SID is fixed for the backend's life. After reuse
  by a different frontend, the SID is unchanged; a cancel issued against that
  backend cancels *whatever query that backend is currently running*, which is the
  new frontend's query. That is correct and is exactly how PostgreSQL cancel
  behaves under any pool — the cancel targets the backend, which is doing the
  current owner's work. ProxySQL's frontend-cancel layer (INV-4) already ensures a
  stale *client* cancel (old `thread_session_id`/secret) cannot match the new
  session, so a leaked client cancel cannot accidentally reach an unrelated
  session through ProxySQL.

### 6.9 Dual-tier (`POLARDB_PROXY=1`/`=0`) + vanilla-backend safety

- `POLARDB_PROXY=0`: all of the above compiles out via the stub path
  (`lib/PgSQL_PolarDB_Stubs.cpp`); `append_polardb_startup_params` emits nothing.
  All new PolarDB fields/logic must be `#if POLARDB_PROXY`-protected to preserve
  the off-build runtime boundary; generic protocol fixes may remain shared.
- `POLARDB_PROXY=1` but **vanilla PostgreSQL backend**: the new keys are only
  emitted when proxy mode resolves (a non-OFF profile with an identity) **and**
  the default-off knob is on. A vanilla backend is configured with
  `proxy_protocol=off`, so no `_polar_proxy_*` key is sent — identical to today.
  **Honest framing (corrected from draft, G7):** the SID emission introduces **no
  new vanilla-PG failure surface beyond what `_polar_proxy_client_host` already
  has.** The patched libpq emits any `_polar_proxy_*` key whenever its conninfo
  string is non-empty (§3.6); if an operator mis-points a V15/proxy-mode hostgroup
  at a *vanilla* PostgreSQL, vanilla PG already rejects the unrecognized startup
  option (the client-host key alone already does this). SID does not add a new
  path; the default-off knob is the real safeguard, and proxy-protocol-off for
  vanilla hostgroups is the operator-facing one.
- **Legacy dialect:** no SID keys in v1 — a deliberate ProxySQL simplicity choice,
  not a backend constraint (§6.2).

### 6.10 Observability / counters

Counters are added to the PolarDB counter X-macro list
(`include/PgSQL_PolarDB_Counters.h`; existing entries use `T(...)` for per-thread
and `G(...)` for global counters, e.g. `T(rfq_profile_skipped, …)` at `:56`) and
incremented via `POLARDB_THREAD_COUNT_ONE(thread, name)`
(`include/PgSQL_Thread.h:826`):

| Counter | When | Purpose |
|---|---|---|
| `polardb_proxy_sid_assigned` | on each SID allocation | volume / sanity |
| `polardb_proxy_sid_collision_retry` | on a retried duplicate-SID connect | distinguishes transient races from a sustained `instance_id` clash; a non-trivial sustained rate is the alert signal for misconfigured `instance_id` (§6.6, G2) |

- Surface `polardb_proxy_sid` on the backend-connection listing / stats so a DBA
  can correlate a ProxySQL backend connection with its `pg_stat_activity` row by
  SID.
- **Confidentiality of the trace (G9).** `POLARDB_TRACE` expands to `proxy_info`
  when PolarDB tracing is enabled (`include/PgSQL_PolarDB.h:90`), and the builder
  already traces conninfo-related strings. **The `cancel_key` must NOT be logged
  at info level** — emit it only behind the trace condition (or masked), and prefer
  logging the SID without the cancel_key in any always-on path. Treat the
  cancel_key like a secret in all logging.

> The exact final symbol names must be checked against the current
> `include/PgSQL_PolarDB_Counters.h` at implementation time (the fold may rename);
> the *pattern* (X-macro entry + `POLARDB_THREAD_COUNT_ONE`) is the stable
> contract.

### 6.11 conninfo value escaping for the new keys

The builder appends values raw (e.g. `conninfo << " _polar_proxy_client_host=" <<
identity.host;`, `lib/PgSQL_Connection.cpp:1538`). For SID/cancel the values are
**integers ProxySQL itself generates** — no user-controlled bytes — so no quoting
or escaping is required for these two keys, and conninfo injection is not a
concern for them (unlike the host value, which derives from client input). State
this explicitly so the integer-only nature is clear.

---

## 7. Interaction with split / warmup and identity-aware pooling

- **Split** (`lib/PgSQL_PolarDB_Split.cpp`) holds primary + replica backends per
  frontend. SID makes each leg independently cancellable (INV-2, by per-connection
  allocation). The split-aware multi-leg cancel (§6.7 future) depends on split's
  leg-tracking structure, so **SID support should land first** (each leg gets a
  distinct, ProxySQL-known SID), then split can consume it.
- **Identity-aware pooling prerequisite.** The SSL sibling feature *requires* the
  pool key to gain frontend-SSL state. The SID feature *does not* require a
  pool-key change (§6.4), but both share the underlying refactor need: the pool
  must be able to record per-connection PolarDB startup properties (identity
  source, SSL state, SID, proxy-assigned-vs-auto). Clean sequencing:
  1. Land the per-connection PolarDB property record + SID emission (this doc).
  2. Land SSL with its pool-key extension (sibling doc).
  3. Wire split's multi-leg cancel onto the SIDs.
- **Warmup** (lazy pool warming) opens backends ahead of demand; those backends
  get SIDs at warm-open time. Nothing special is needed beyond a thread-safe
  allocator across warmers and request threads (§6.1 global atomic handles this).

---

## 8. Testing and verification plan

### 8.1 Unit tests (`test/.../unit`, linked against `libproxysql.a`)

- **SID encoding.** `(instance_id, seq) → sid` always lies in `(10^7, INT32_MAX]`,
  is distinct for distinct `(instance, seq)`, wraps within `SEQ_MASK`, and
  **never yields `<= 10_000_000`** (assert `POLAR_IS_PROXY_SID(sid)` for the
  whole range including `instance_id=0, seq=0`).
- **Builder pairing.** `append_polardb_startup_params` with
  `send_session_id=true`, V15, identity present → conninfo contains **both**
  `_polar_proxy_session_id` and `_polar_proxy_cancel_key`; with
  `send_session_id=false` → neither; LEGACY → neither (v1 choice); identity NONE →
  connection refused (existing behavior preserved).

### 8.2 TAP tests (pgsql infra group)

- **Cancel reaches backend.** Run a long `SELECT pg_sleep(30)` through
  ProxySQL→PolarDB; issue `PQcancel` from the client; assert the query is canceled
  and ProxySQL logs "Canceled query … successfully" (`PgSQL_Connection.cpp:3682`).
- **`pg_stat_activity` shows the ProxySQL-chosen SID.** Connect with
  `send_session_id=true`, read the visible session id (via stats or a PolarDB proxy
  view) and assert it equals the SID ProxySQL allocated (requires surfacing the
  SID via stats, §6.10).
- **Replica cancel identity.** Force a read onto a non-primary node with
  `send_session_id=true`; assert the session has a non-zero proxy SID (vs. `SID=0`
  with the feature off, `backend_status.c:411-414`).
- **Cluster uniqueness.** Two ProxySQL instances with distinct
  `polardb_proxy_instance_id` against one PolarDB node; assert no duplicate-SID
  failure across N connects. **Negative test:** two instances with the *same*
  `instance_id` → assert the `polardb_proxy_sid_collision_retry` counter climbs
  and the operator-alert path fires (confirming retry does NOT silently mask the
  misconfiguration, §6.6/G2).

### 8.3 Backend G0-style invariants

- **Pairing FATAL.** Send only one of session_id/cancel_key → backend FATAL
  "Should set both session id and cancel key." (`postmaster.c:2551-2552`).
- **Duplicate-SID ERROR.** Force two backends with the same SID on one node →
  second connect fails with `elog(ERROR)` "The proxy sid %d is already in use"
  (`backend_status.c:1528-1531`). Confirms the retry path (§6.6) is reachable, and
  confirms the only discriminator is the message substring (no dedicated SQLSTATE
  — motivates the dedicated-errcode backend ask).
- **SID echoed in `'K'`.** Connect with a known SID, read libpq `be_pid` → equals
  the SID (confirms §3.4 / `postgres.c:4626-4640`).
- **Backend ask (G0 / future):** propose a dedicated error code for the
  duplicate-SID condition so ProxySQL's retry detection does not depend on English
  message matching (§6.6/G3).

---

## 9. Phasing and open questions

### Phase 1 (this doc — minimal, default-off)

- Add `polardb_proxy_sid` / `polardb_proxy_cancel_key` connection fields +
  global allocator + `pgsql-polardb_proxy_send_session_id` (default off) +
  `pgsql-polardb_proxy_instance_id`, all under `#if POLARDB_PROXY`.
- Emit both keys in `append_polardb_startup_params` (V15, proxy mode resolved,
  knob on; condition independent of RFQ — §6.2). Rely on the *existing* libpq patch and
  the *existing* `PQcancel` path; no cancel-path code change.
- Counters (`sid_assigned`, `sid_collision_retry`) + stats surfacing of the SID +
  collision-retry-on-connect with the honest sustained-clash detection/alert.

### Phase 2 (after split leg-tracking lands)

- Split-aware multi-leg cancel: `PQcancel` each live backend leg (each carries its
  own distinct SID).
- Optional `polardb_sid_proxy_assigned` capability flag to let split prefer
  proxy-assigned-SID connections — only if split actually consumes the SID.

### Deferred

- Central / cluster-coordinated SID allocator (Phase 1 uses operator-set
  `instance_id`); revisit only if 64 instances × ~16M is ever tight.
- A dedicated backend error code for duplicate-SID (replaces message-substring
  matching).
- Lifting the V15-only restriction to also emit SID under the legacy dialect (the
  backend already accepts it — §6.2).
- Any change to the client-facing key (never — INV-4).

### Open questions

1. **SID read-back for TAP.** Does any PolarDB proxy view expose the SID to a
   client query in a form we can assert in TAP, or must we read it via libpq
   `be_pid` only? Confirm the `pg_stat_activity`/proxy-view column name on this
   branch.
2. **Audit fidelity vs. pooling.** On pooled reuse across frontends, the audit/log
   SID remains the pooled backend's stable SID (current design). Rotation is
   impossible without reconnecting (SID is startup-only), so the real question is
   "do we ever force-reconnect for audit fidelity?" — assumed *no* for v1.
3. **Cancel-key strength vs. confidentiality.** ProxySQL chooses a random `int32`
   cancel_key; the backend stores and checks it (`backend_status.c` get_pid
   cancel-key compare). The 32-bit *strength* matches native PG's `MyCancelKey`
   (acceptable). Separately, *confidentiality in transit*: the SID/cancel pair
   travels in cleartext in the startup packet on a non-TLS ProxySQL↔PolarDB hop —
   the same exposure native `BackendKeyData` already has on a non-TLS hop. A
   sniffer on that segment could cancel arbitrary backends by SID. This is an
   additional argument for the ProxySQL↔PolarDB transport TLS covered by the SSL
   sibling doc, and a reason to keep the cancel_key out of always-on logs (§6.10).
4. **Failover.** If a non-primary node is promoted, does an in-flight SID stay
   valid? The SID is node-local; a promoted node keeps its own
   `BackendStatusArray`, so the SID stays valid for that process — but worth a
   failover test.
