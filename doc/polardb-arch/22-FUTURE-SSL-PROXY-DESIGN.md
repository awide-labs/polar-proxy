# SSL Support for PolarDB in ProxySQL — Design

| | |
|---|---|
| **Status** | DESIGN (authoritative; design/research only — no code changes) |
| **Scope** | Propagating the **frontend (client↔ProxySQL) TLS state** to a PolarDB proxy-mode backend via the `_polar_proxy_use_ssl` / `_polar_proxy_ssl_version` / `_polar_proxy_ssl_cipher_name` startup contract, and making that state safe under connection pooling. |
| **Audience** | ProxySQL engineers working on the PolarDB integration (startup-param builder, pooling, warmup) and reviewers familiar with the PostgreSQL wire protocol. |
| **Verified against** | ProxySQL branch `polardb-dev` and PolarDB backend branch `POLARDB_15_PROXY`. All `file:line` references below were checked in those revisions. |
| **Companion docs** | Reader-failure recovery (doc 20); the sibling **session-id / cancel** proxy design (doc 23) — shares the startup-packet controlling and the same pool-compatibility machinery. |

---

## Table of contents

1. [Background & motivation](#1-background--motivation)
2. [Vanilla PostgreSQL SSL mechanism (baseline)](#2-vanilla-postgresql-ssl-mechanism-baseline)
3. [PolarDB proxy-mode SSL extension (verified backend contract)](#3-polardb-proxy-mode-ssl-extension-verified-backend-contract)
4. [Current ProxySQL state & the precise gap](#4-current-proxysql-state--the-precise-gap)
5. [General design (correct invariants, code-independent)](#5-general-design-correct-invariants-code-independent)
6. [ProxySQL-branch implementation design](#6-proxysql-branch-implementation-design)
7. [Interaction with split / warmup and identity-aware pooling](#7-interaction-with-split--warmup-and-identity-aware-pooling)
8. [Testing & verification plan](#8-testing--verification-plan)
9. [Phasing, deferrals, and open questions](#9-phasing-deferrals-and-open-questions)

---

## The three SSL surfaces (orientation)

A PolarDB-fronted ProxySQL deployment has **three distinct SSL surfaces**. The doc keeps them strictly separate; the central design error to avoid is conflating them.

| Tag | Surface | Hop | Already wired in ProxySQL? |
|-----|---------|-----|----------------------------|
| **(A)** | **Frontend SSL** | client ↔ ProxySQL TLS termination | Yes — ProxySQL terminates client TLS. |
| **(B)** | **Backend transport SSL** | ProxySQL ↔ PolarDB libpq `sslmode` TLS | Yes — `pgsql_servers.use_ssl` + `pgsql-ssl_p2s_*`. |
| **(C)** | **Proxy SSL-metadata propagation** | the `_polar_proxy_use_ssl/_ssl_version/_ssl_cipher_name` startup keys that tell the backend the **frontend** (client) SSL state | **No** — this is the feature this doc designs. |

**Load-bearing thesis:** **(C) must mirror hop (A), not hop (B).** PolarDB consumes the proxy-advertised SSL state for HBA authentication of the *client*; ProxySQL must therefore advertise the client's TLS, never the proxy↔backend transport's TLS.

**Critical correction over the first analysis draft (verified in source):** advertised SSL state (C) does **not** universally fix observability. It feeds **HBA unconditionally**, but it feeds `pg_stat_ssl` and the "connection authorized" log line **only when hop (B) is also TLS** (see §3.5). The advertised version/cipher are surfaced in those views *only* when the ProxySQL↔backend transport itself negotiated TLS. This is because the backend controls those two surfaces on `port->ssl_in_use` (its own TLS handshake), not on the proxy SSL flag.

---

## 1. Background & motivation

PolarDB proxy mode lets a connection-pooling proxy sit between clients and the database while preserving per-client properties at the backend: the real client host/port (for HBA matching and `pg_stat_activity`), the real client SSL state (for HBA `hostssl`/`hostnossl` and `ssl_version()`/`ssl_cipher()`), and a per-backend session id / cancel key.

ProxySQL today advertises only the client host/port plus the RFQ payload request bits (`_polar_proxy_send_lsn` / `_polar_proxy_send_xact`). It does **not** advertise the client's SSL state, so `port->polar_proxy_ssl_in_use` stays `false` at the backend for every connection.

What breaks without (C):

- **HBA correctness (the load-bearing failure).** A PolarDB cluster with `hostssl` rules for some client networks (e.g. require TLS from outside the VPC) cannot enforce them through ProxySQL: the backend's *advertised-client* HBA pass evaluates against `polar_proxy_ssl_in_use`, which defaults to `false`. Every client looks plaintext to the `hostssl` matcher and is implicitly rejected; conversely a `hostnossl` rule wrongly admits a TLS client. This is a correctness/security gap, not cosmetic. HBA is the one consumer that uses the advertised flag *independently of hop (B)*.
- **Audit/observability — but only when hop (B) is also TLS.** `pg_stat_ssl` and the connection-authorized log read `be_tls_get_version()/cipher()`, which prefer the proxy-advertised strings — *but only after the SSL status row / log line is controlled open by `port->ssl_in_use`* (hop B). So propagation (C) makes those surfaces report the **client's** TLS *when the proxy↔backend link is also encrypted*; when the proxy↔backend link is plaintext, those surfaces report no SSL regardless of (C). See §3.5.

Who needs it: any PolarDB deployment that fronts the cluster with ProxySQL and enforces SSL policy at the database via `hostssl`/`hostnossl`, while still pooling backend connections for throughput. For audit-grade `pg_stat_ssl`, the same deployment must additionally run TLS on the proxy↔backend hop (B).

---

## 2. Vanilla PostgreSQL SSL mechanism (baseline)

Plain PostgreSQL wire SSL is strictly point-to-point. A client sends an `SSLRequest` (a special startup packet with code `80877103`); the server replies `S` (proceed with TLS) or `N` (plaintext). After the TLS handshake the server records the negotiated state on its `Port`: `port->ssl_in_use` and `port->ssl` (the OpenSSL object). `be_tls_get_version()` / `be_tls_get_cipher()` derive version/cipher from `port->ssl`. HBA matching keys on `port->ssl_in_use` for `hostssl`/`hostnossl`.

The wire protocol has **no** way for a TLS-terminating middlebox to tell the server "the client's TLS was version X, cipher Y." In vanilla PostgreSQL, a proxy that terminates client TLS and re-originates a separate backend connection makes the client's TLS invisible to the backend. That is exactly the gap PolarDB proxy mode closes with out-of-band startup metadata.

On the ProxySQL frontend the same `SSLRequest` handshake is already implemented: `PgSQL_Protocol::process_startup_packet` sets `ssl_request`; the session then allocates a server-side SSL object and accepts the TLS handshake (`PgSQL_Session.cpp:4124-4134` — `client_myds->ssl = GloVars.get_SSL_new()` at 4128, `SSL_set_accept_state` at 4130). After the handshake, `client_myds->encrypted` is true (`PgSQL_Data_Stream.h:183`) and `client_myds->ssl` (`PgSQL_Data_Stream.h:138`) holds the negotiated OpenSSL session — exactly the object PolarDB needs us to describe.

---

## 3. PolarDB proxy-mode SSL extension (verified backend contract)

References in this section use the PolarDB backend branch `POLARDB_15_PROXY`.

### 3.1 Startup-packet keys (top-level conninfo, parsed in `ProcessStartupPacket`)

`postmaster.c` parses `_polar_*` keys directly out of the startup packet (not inside the `options` GUC block):

| Key | Backend field | Type | Parse site |
|-----|---------------|------|------------|
| `_polar_proxy_client_host` / `_polar_proxy_client_port` (legacy `_polar_origin_client_ip` / `_polar_origin_client_port`) | rewrites `remote_host`/`remote_port`, sets `polar_proxy_client_raddr` | string | `postmaster.c:~2410` |
| `_polar_proxy_use_ssl` | `port->polar_proxy_ssl_in_use` (bool; invalid bool → FATAL) | bool | `postmaster.c:2460-2468` |
| `_polar_proxy_ssl_version` | `port->polar_proxy_ssl_version` (`pstrdup`) | string | `postmaster.c:2470-2471` |
| `_polar_proxy_ssl_cipher_name` | `port->polar_proxy_ssl_cipher_name` (`pstrdup`) | string | `postmaster.c:2472-2473` |
| `_polar_proxy_send_lsn` / `_polar_proxy_send_xact` | RFQ LSN/XID request | bool | — (out of scope) |
| `_polar_proxy_session_id` / `_polar_proxy_cancel_key` | proxy SID / cancel key | int | — (sibling doc) |

`Port` field declarations: `libpq-be.h:223-231` — `polar_proxy` (bool), `polar_proxy_client_raddr` (SockAddr), `polar_proxy_ssl_in_use` (bool), `polar_proxy_ssl_cipher_name` (char\*), `polar_proxy_ssl_version` (char\*); access macros `POLAR_PROXY_GET_RADDR*` at `libpq-be.h:234-236`. Defaults are set at `postmaster.c:4827-4834`: `polar_proxy=false`, `polar_proxy_ssl_in_use=false`, cipher/version `NULL`.

### 3.2 When SSL metadata may be forwarded

Three backend FATALs define the contract envelope:

1. **Proxy mode requires both host and port.** `polar_proxy` is set true **only if both** `client_host` and `client_port` are present (`postmaster.c:2507-2536`); incomplete (one without the other) → FATAL. When set, the backend rewrites `remote_host`/`remote_port` to the advertised client and encodes `polar_proxy_client_raddr` (`polar_encode_client_conn`, ~2527).
2. **SSL keys are all-or-nothing.** If `polar_proxy_ssl_in_use` is true, **both** `_polar_proxy_ssl_version` and `_polar_proxy_ssl_cipher_name` must be present, or FATAL `"[Proxy] ssl info is incomplete."` (`postmaster.c:2547-2549`).
3. **SSL (and RFQ LSN) require proxy mode.** If `!polar_proxy` and (`send_lsn` or `ssl_in_use`) → FATAL `"[Proxy] Proxy is disabled, unable to use lsn or ssl"` (`postmaster.c:2555-2559`).

Consequence: **advertising any SSL state is strictly a superset of the existing identity-advertising path.** You cannot turn on (C) without already advertising a client host/port — which ProxySQL already does for RFQ. SSL propagation reuses that identity path and inherits its "no identity → refuse" rule.

### 3.3 HBA: dual check, the proxy pass uses the advertised SSL flag

`check_hba` runs the matcher **twice** (`hba.c:2202-2220`):

```
hba = get_matched_hba(port, false);          // hop B: proxy↔backend TCP, uses port->ssl_in_use   (line 2207)
if (port->polar_proxy)
    hba = get_matched_hba(port, true);        // hop A: advertised client, uses polar_proxy_ssl_in_use (line 2209)
if either is NULL → uaImplicitReject          // line 2218
```

Inside `get_matched_hba` the SSL source is selected by the `proxy` flag (`hba.c:2126`):

```c
if (proxy ? port->polar_proxy_ssl_in_use : port->ssl_in_use) { ... match host/hostssl ... }
```

The IP check likewise uses `POLAR_PROXY_GET_RADDR(port, proxy)` (`hba.c:2117`, and the proxy-pass IP/network checks downstream), i.e. the advertised client address for the proxy pass. Hostname verification is **disabled** for proxy connections (`hba.c:698-703`: when `port->polar_proxy`, `check_hostname` short-circuits to `return false`).

Consequences:

- **Both HBA passes must succeed.** The proxy↔backend hop (B) must satisfy the backend's HBA rule for the proxy's own source address + transport TLS, AND the advertised-client hop (A) must satisfy the HBA rule for the client's address + the SSL flag we advertise. Advertise `use_ssl=false` for a TLS client → a `hostssl` client rule rejects. Advertise `use_ssl=true` for a plaintext client → a `hostnossl` rule rejects.
- **Hostname-based HBA rules are unmatchable through proxy mode** (`hba.c:698-703`). A `host ... <hostname>` line never matches the advertised-client pass. This is a hard limitation parallel to the `clientcert`/`clientname` limitation (§9), and must be documented for operators.

### 3.4 `ssl_version()` / `ssl_cipher()` precedence (no validation)

`be-secure-openssl.c:1303-1322`:

```c
const char *be_tls_get_version(Port *port) {       // 1303
    if (port->polar_proxy_ssl_in_use) return port->polar_proxy_ssl_version;  // advertised, verbatim
    else if (port->ssl)               return SSL_get_version(port->ssl);     // backend-hop TLS
    else                              return NULL;
}
const char *be_tls_get_cipher(Port *port) {        // 1314
    if (port->polar_proxy_ssl_in_use) return port->polar_proxy_ssl_cipher_name;
    else if (port->ssl)               return SSL_get_cipher(port->ssl);
    else                              return NULL;
}
```

When proxy SSL is advertised, the backend returns **our advertised strings verbatim** — there is no validation that they name a real TLS version or OpenSSL cipher (§trust model, §6.9). ProxySQL must therefore send exactly the strings OpenSSL reports on the frontend (e.g. `TLSv1.3`, `TLS_AES_256_GCM_SHA384`).

**Asymmetry — `cipher_bits` is not proxy-aware.** `be_tls_get_cipher_bits` (`be-secure-openssl.c:1289-1300`) reads only `port->ssl` (`SSL_get_cipher_bits(port->ssl, &bits)` at 1295) and returns `0` when there is no backend-side `port->ssl`. So a proxy-SSL connection that has no backend-hop TLS reports `bits = 0` even when version/cipher are populated. This is a backend limitation we inherit, not a ProxySQL bug.

### 3.5 What actually surfaces `pg_stat_ssl` and the auth log (the corrected contract)

This is the claim the first analysis draft got wrong. Both observability surfaces are enabled when `port->ssl_in_use` (hop **B**, the backend's own TLS handshake), **not** on `polar_proxy_ssl_in_use` (hop **C**):

- **`pg_stat_ssl` status row** — `backend_status.c:426`: `if (MyProcPort && MyProcPort->ssl_in_use) { st_ssl=true; ssl_version=be_tls_get_version(...); ssl_cipher=be_tls_get_cipher(...); ... }` else `st_ssl=false` (line 438). The view reads `beentry->st_ssl` then `ssl_version`/`ssl_cipher` (`pgstatfuncs.c:843-848`).
- **Connection-authorized log** — `postinit.c:283`: `#ifdef USE_SSL if (port->ssl_in_use) appendStringInfo(... be_tls_get_version(port), be_tls_get_cipher(port), be_tls_get_cipher_bits(port));`.
- **`port->ssl_in_use` is set only on a real backend TLS handshake** — the only setters in the whole backend are `be-secure-openssl.c:459` (`= true`, after the proxy↔backend handshake succeeds) and `:691` (`= false`, on shutdown). It is never derived from the proxy flag.

| Hop (B): ProxySQL↔backend transport | Hop (C): advertised client SSL | HBA *client* pass evaluates against | `pg_stat_ssl` / auth-log for client TLS |
|---|---|---|---|
| plaintext (`use_ssl=0`) | `use_ssl=false` (no keys) | plaintext | not-SSL |
| plaintext (`use_ssl=0`) | `use_ssl=true` (keys advertised) | **TLS** (correct for HBA) | **not-SSL** (controlled shut by `ssl_in_use=false`) — advertised version/cipher are dead here |
| TLS (`use_ssl=1`) | `use_ssl=false` (no keys) | plaintext | reports the proxy↔backend cipher (not the client's) |
| TLS (`use_ssl=1`) | `use_ssl=true` (keys advertised) | **TLS** (correct) | **reports the client's** advertised version/cipher (correct); `cipher_bits` may be wrong per §3.4 |

**Practical rule for operators:** to get a correct `hostssl`/`hostnossl` decision, enable (C) alone. To *also* get correct `pg_stat_ssl`/audit reflecting the client's TLS, enable (C) **and** run hop (B) over TLS (`pgsql_servers.use_ssl=1`).

### 3.6 libpq client-side options (how the keys travel)

PolarDB's own libpq registers all the keys as conninfo options (`fe-connect.c:373-388`: `_polar_proxy_use_ssl` at 382, `_polar_proxy_ssl_version` at 385, `_polar_proxy_ssl_cipher_name` at 388, alongside the session/cancel keys). PolarDB's TAP test `008_proxy.pl` injects them via env, which is libpq's `PG*` env convenience — **the backend never reads env**; it reads the startup packet. ProxySQL builds the conninfo string itself and relies on its own patched libpq to serialize the keys into the startup packet.

---

## 4. Current ProxySQL state & the precise gap

References in this section use the ProxySQL branch `polardb-dev`.

### 4.1 What already works

- **Frontend SSL (A) is fully terminated.** `client_myds->encrypted` (`PgSQL_Data_Stream.h:183`) and `client_myds->ssl` (`PgSQL_Data_Stream.h:138`) hold the negotiated client TLS; handshake at `PgSQL_Session.cpp:4124-4134`. The cipher is already read on the frontend: `SSL_get_current_cipher(client_myds->ssl)` + `SSL_CIPHER_get_name(...)` in `fill_internal_session` (`PgSQL_Session.cpp:735-744`). **Note:** there is no existing *PgSQL* caller of `SSL_get_version`; the confirmed PgSQL accessor covers cipher only. `SSL_get_version(client_myds->ssl)` is confirmed in the MySQL sibling (`MySQL_Session.cpp:1445`) — version reuse is cross-protocol, not a PgSQL precedent.
- **Backend transport SSL (B) is fully wired.** `connect_start` emits `sslmode='require'` + `sslkey/sslcert/sslrootcert/sslcrl/sslcrldir` (+ `ssl_min/max_protocol_version`) when `parent->use_ssl` is set, falling back to `pgsql_thread___ssl_p2s_*` globals; else `sslmode='disable'` (`PgSQL_Connection.cpp:1239-1273`). `use_ssl` is the per-server `pgsql_servers.use_ssl` column (assigned at `PgSQL_HostGroups_Manager.cpp:358`). This block uses the conninfo-escaping helper `append_conninfo_param` (defined `PgSQL_Connection.cpp:1178`).
- **libpq plumbing for (C) is ALREADY PRESENT.** The ProxySQL libpq patch (`deps/postgresql/polardb_libpq.patch`) already: registers the conninfo options (`_polar_proxy_use_ssl`/`_ssl_version`/`_ssl_cipher_name`, patch lines 65-75); declares the struct fields (patch lines 400-402); serializes them into the startup packet via `ADD_STARTUP_OPTION` (patch lines 328-333); and frees them on teardown (patch lines 124-126). **The libpq side needs no new patch work for SSL.** The startup packet will carry these keys the moment ProxySQL sets the corresponding conninfo keys.
  - **Important — the patch does NOT scope SSL to V15.** The `/* PG15-only metadata */` text at patch line 323 is a **comment, not a check.** The SSL `ADD_STARTUP_OPTION` calls at 328-333 fire whenever the corresponding `conn->_polar_proxy_ssl_*` field is non-empty, independent of whether legacy (`_polar_origin_client_ip`) or V15 (`_polar_proxy_client_host`) identity keys are set. The V15-only restriction must therefore be **enforced by the ProxySQL builder**, not assumed from libpq (see §6.3).
- **The pool-compatibility machinery for (C) ALREADY EXISTS — but is inert.** This is the key fact the first draft missed. `PolarDB_StartupClientContext` (`PgSQL_PolarDB.h:636-660`) already declares the SSL/session/cancel fields as documented *reserved placeholders*:
  ```cpp
  struct PolarDB_StartupClientContext {
      PolarDB_StartupIdentity identity;
      bool        frontend_ssl  = false;   // line 638
      std::string ssl_version;             // line 639
      std::string ssl_cipher;              // line 640
      bool        has_proxy_session = false;
      uint64_t    proxy_session_id  = 0;
      uint32_t    proxy_cancel_key  = 0;
      bool compatible_for_reuse(const PolarDB_StartupClientContext& other) const {  // line 649
          return identity.source == other.identity.source &&
                 identity.host   == other.identity.host   &&
                 identity.port   == other.identity.port   &&
                 frontend_ssl    == other.frontend_ssl    &&   // line 653
                 ssl_version     == other.ssl_version     &&   // line 654
                 ssl_cipher      == other.ssl_cipher      &&   // line 655
                 has_proxy_session == other.has_proxy_session &&
                 proxy_session_id  == other.proxy_session_id  &&
                 proxy_cancel_key  == other.proxy_cancel_key;
      }
  };
  ```
  Both the **reader-acquisition path** and the **warmup dedup key** already consult these fields:
  - Reader acquisition `polardb_get_rfq_profile_compatible_conn` skips a pooled candidate when `!candidate->polardb_startup_client.compatible_for_reuse(required_startup_client)` (`PgSQL_HostGroups_Manager.cpp:5445-5447`), with a debug log "startup client identity differs".
  - The split warmup dedup key already hashes `frontend_ssl`/`ssl_version`/`ssl_cipher` (and the session/cancel fields) into its bucket string (`PgSQL_HostGroups_Manager.cpp:5211-5222`).
  - The split split reader path stamps `conn->polardb_forced_startup_identity = req.startup_client.identity` (`PgSQL_HostGroups_Manager.cpp:5316`), and the builder honours a forced identity at `PgSQL_Connection.cpp:1426-1438`.

### 4.2 The precise gap (narrower than the first draft assumed)

Because the pool-key machinery already buckets on the SSL fields, the remaining work is just to **populate** those fields from the frontend and **emit** the corresponding startup keys. Concretely:

1. **The SSL fields are never populated.** `polardb_session_startup_client_context` (`PgSQL_HostGroups_Manager.cpp:5026-5054`) — the one helper that builds a `PolarDB_StartupClientContext` from a session — sets only `identity` from `client_myds->addr`; it never reads `client_myds->encrypted`/`ssl`. So `frontend_ssl` is `false` and `ssl_version`/`ssl_cipher` are empty for every session. The pool/warmup buckets therefore all collapse into the single "nossl" bucket. (A whole-tree grep finds no assignment to `frontend_ssl`, `ssl_version`, or `ssl_cipher` anywhere outside the placeholder defaults and the comparison/key code.)
2. **The builder only sets `identity`, never the SSL fields, and never emits the SSL keys.** `append_polardb_startup_params` (`PgSQL_Connection.cpp:1500-1560`) resets `polardb_startup_client` (line 1502), sets `polardb_startup_client.identity = identity` (line 1526), and in the V15 branch emits `_polar_proxy_client_host/_port` + `_polar_proxy_send_lsn/_send_xact` (V15 branch at ~1535-1543; LEGACY branch at ~1545-1554). It has **no** branch reading the frontend SSL state and emitting `_polar_proxy_use_ssl/_ssl_version/_ssl_cipher_name`. So `polar_proxy_ssl_in_use` stays false at the backend for every connection.
3. **PolarDB key emission is raw, not escaped.** The V15/legacy branchs write `conninfo << " _polar_proxy_client_host=" << identity.host;` — **raw `<<`, unquoted**, *not* `append_conninfo_param`. (The transport-SSL block at 1239-1273 *does* use `append_conninfo_param`; the PolarDB key block does not.) The full conninfo string is later parsed by `PQconnectStart` (`PgSQL_Connection.cpp:~1359`); that parse — not `ADD_STARTUP_OPTION` — is where conninfo quoting matters. `ADD_STARTUP_OPTION` builds the wire packet from already-parsed `conn->` fields and performs no conninfo-string quoting. So new SSL key values must be emitted via `append_conninfo_param` **or** validated against a safe charset before raw emission (§6.3).
4. **No config knob** to enable/disable SSL propagation, and **no observability counters** for the SSL path.

This is one instance of a general invariant, which the same code already half-honors for identity:

> *Any startup-time property that affects backend semantics and can be reused across frontends must be either (a) stable across reuse, or (b) part of the pool-compatibility key.*

DB auth is in the generic key (`has_same_connection_options`, §4.3); advertised identity and advertised SSL are in the PolarDB key (`compatible_for_reuse`) *structurally* — but the SSL dimension is dead until the fields are populated.

### 4.3 Generic pool key is the wrong layer

`has_same_connection_options` (`PgSQL_Connection.cpp:2632-2642`) compares `userinfo->username` + `userinfo->dbname` only and is shared with non-PolarDB pooling. **Do not** add PolarDB SSL there. The correct layer is the PolarDB `compatible_for_reuse` key that the reader-acquisition path already consults.

---

## 5. General design (correct invariants, code-independent)

Let a backend connection `B` be created on behalf of some frontend `F`. Define the **advertised SSL descriptor** `S(F) = (use_ssl, version, cipher)` derived from `F`'s client TLS:

- `use_ssl(F) = F.encrypted` — true iff the client negotiated TLS with ProxySQL.
- `version(F) = SSL_get_version(F.ssl)` when encrypted, else undefined.
- `cipher(F) = SSL_CIPHER_get_name(SSL_get_current_cipher(F.ssl))` when encrypted, else undefined.

**Invariants:**

1. **Mirror the client hop (A), not the backend hop (B).** `S(B)` advertised to PolarDB MUST equal `S(F)` for the frontend that triggered `B`'s creation. It MUST NOT be derived from the ProxySQL↔backend transport `sslmode`. (Backend §3.3/§3.4: PolarDB uses the advertised flag for the *client* HBA pass.)
2. **All-or-nothing SSL keys.** Advertise either zero SSL keys, or all three with non-empty `version` and `cipher` (backend FATAL otherwise, §3.2). Never advertise `use_ssl=true` with an empty/missing version or cipher.
3. **SSL implies identity.** Advertising any SSL metadata requires advertising `client_host`/`client_port` (proxy mode), else backend FATAL (§3.2). The SSL path reuses the existing identity resolution and inherits its "no identity → refuse" rule (`PgSQL_Connection.cpp:1508-1520`).
4. **SSL descriptor is part of the pool key for reusable PolarDB backends.** Two frontends may share a pooled backend only if their advertised SSL descriptors are equal. The minimum correct discriminator is the boolean `use_ssl` (that is what flips `hostssl`↔`hostnossl`). The existing `compatible_for_reuse` (§4.1) over-keys on exact version+cipher too; see §6.4 for the granularity discussion.
5. **Fixed for backend lifetime.** `S(B)` is decided at startup and cannot change on a live backend. A reused backend keeps the `S` it was born with; the pool must not hand it to a frontend with a different `S`.
6. **Vanilla-backend safety.** If the backend is not a PolarDB proxy build, none of these keys exist; sending them to a vanilla PostgreSQL backend would make it treat them as GUC options and error. Emission must be enabled when a PolarDB hostgroup + non-OFF proxy protocol (the same condition as the existing identity keys).
7. **Observability is a two-hop conjunction (corrected).** Advertised SSL (C) feeds **HBA unconditionally**, but feeds `pg_stat_ssl`/auth-log **only when hop (B) is also TLS** (§3.5). The two hops are *otherwise* independent decisions — a plaintext client over a TLS backend transport advertises `use_ssl=false`; a TLS client over a plaintext backend transport advertises `use_ssl=true` — but for the audit surfaces specifically, correctness is the *conjunction* of (B) and (C). The doc must not claim (C) alone fixes `pg_stat_ssl` for a plaintext-backend deployment.

---

## 6. ProxySQL-branch implementation design

### 6.1 Capture the frontend SSL descriptor

Populate `frontend_ssl` / `ssl_version` / `ssl_cipher` on `PolarDB_StartupClientContext` (`PgSQL_PolarDB.h:638-640`) — the placeholders already exist; no new struct is needed. The single source of truth is `client_myds`. The natural place is **`polardb_session_startup_client_context`** (`PgSQL_HostGroups_Manager.cpp:5026-5054`), which already snapshots the session's identity and is the only producer of a startup-client context for both pooling and warmup. Extend it to also read SSL:

```cpp
startup_client->frontend_ssl = client_myds->encrypted;          // PgSQL_Data_Stream.h:183
if (client_myds->encrypted && client_myds->ssl) {
    const char* v = SSL_get_version(client_myds->ssl);          // confirmed in MySQL_Session.cpp:1445
    const SSL_CIPHER* c = SSL_get_current_cipher(client_myds->ssl);
    const char* cn = c ? SSL_CIPHER_get_name(c) : nullptr;       // confirmed in PgSQL_Session.cpp:737-741
    if (v)  startup_client->ssl_version = v;
    if (cn) startup_client->ssl_cipher  = cn;
}
```

The same population must be applied wherever a `PolarDB_StartupClientContext` is built for a real client connection — today that is exactly this helper (the warmup request and the split reader path both flow through `startup_client` values produced here). On the connection's own builder path (§6.2/§6.3), the descriptor is taken from the same frontend.

This must run **after** the frontend TLS handshake completes. For client-backed sessions the handshake (`STATE_SERVER_HANDSHAKE`) precedes query dispatch, so by the time a backend is acquired the descriptor is stable. For non-client connections (monitor/internal/warmup with no client) `client_myds` is absent, so `frontend_ssl=false` and no SSL keys are emitted — the same null-client check that already returns `false` from the helper at line 5031.

### 6.2 Set the descriptor on the connection at build time

`append_polardb_startup_params` resets `polardb_startup_client` (line 1502) and sets `.identity` (line 1526). Extend it to also stamp the SSL fields, enabled when the propagation knob (§6.5), from the session's frontend descriptor (the same source as §6.1). Keeping the advertised descriptor on `polardb_startup_client` means the pool's `compatible_for_reuse` check (§4.1) sees the real bucket with zero further wiring — the bucketing code is already in place.

For the split split reader path, the forced identity is already carried via `conn->polardb_forced_startup_identity` (`PgSQL_HostGroups_Manager.cpp:5316`, honoured at `PgSQL_Connection.cpp:1426-1438`). The SSL fields travel through `req.startup_client` and are stamped onto `polardb_startup_client` the same way; no separate "forced SSL" field is required because all of a frontend's split backends share one `S(F)` (§7).

### 6.3 Emit the SSL keys (the actual hook point)

In `append_polardb_startup_params`, inside the **V15 branch only** (~1535-1543), after the existing `_polar_proxy_client_host/_port` + `send_lsn/send_xact` emission, add:

```cpp
if (polardb_startup_client.frontend_ssl) {
    // V15 branch only. The LEGACY branch (~1545-1554) MUST NOT emit SSL keys: the libpq
    // patch serializes whenever the conninfo key is set regardless of dialect
    // (patch 328-333), so the V15-only restriction is enforced HERE, not in libpq.
    append_conninfo_param(conninfo, "_polar_proxy_use_ssl", (char*)"true");
    append_conninfo_param(conninfo, "_polar_proxy_ssl_version",     (char*)polardb_startup_client.ssl_version.c_str());
    append_conninfo_param(conninfo, "_polar_proxy_ssl_cipher_name", (char*)polardb_startup_client.ssl_cipher.c_str());
}
```

Rules:

- **Use `append_conninfo_param` (or hard-validate), not raw `<<`.** The PolarDB key block currently emits raw (§4.2 item 3); cipher names and `TLSvX.Y` are in practice `[A-Za-z0-9_.:+-]` with no spaces, so raw emission *happens* to be safe today — but a future OpenSSL build or custom cipher alias containing a space or quote would corrupt the conninfo parse or inject a key. Route SSL values through `append_conninfo_param` (the helper the transport block already uses) **or** validate against an explicit safe charset before emission. This design picks `append_conninfo_param`.
- **Emit nothing when `frontend_ssl=false`** (do not emit `_polar_proxy_use_ssl=false`; the backend default is already false, and omitting keeps the packet minimal and the legacy/vanilla path byte-untouched).
- **All-or-nothing check (Invariant 2).** Before emitting, require non-empty `ssl_version` AND `ssl_cipher`. If `frontend_ssl` is true but either is empty (OpenSSL returned NULL — §6.8 failure modes), do **not** advertise `use_ssl=true`. Behavior is governed by the propagation knob: `strict` → refuse the backend connection (mirror the existing identity "refuse on NONE" pattern at `PgSQL_Connection.cpp:1508-1520`); `best_effort` → advertise plaintext (visible via counter), accepting that this may flip the HBA outcome.

### 6.4 Pool-key granularity (the dimension is already wired)

The reader-acquisition condition (`PgSQL_HostGroups_Manager.cpp:5445-5447`) and warmup dedup key (`5211-5222`) already compare the full SSL descriptor via `compatible_for_reuse` / the key string. Once §6.1 populates the fields, **bucketing turns on with no further pooling code change.**

Granularity choice: the only field the HBA `hostssl`/`hostnossl` decision depends on is the boolean `frontend_ssl` (§3.3). `compatible_for_reuse` *also* compares exact `ssl_version` and `ssl_cipher`, so a TLS1.3 client and a TLS1.2 client will not share a backend even though both satisfy the same `hostssl` rule. That over-bucketing is **harmless for correctness** (it never reuses an incompatible backend) but can fragment the pool. Two positions:

- **Keep exact version+cipher in the key (status quo of `compatible_for_reuse`).** Simplest — the code already does it — and it keeps `pg_stat_ssl` exactly accurate per reused backend (when hop B is TLS, §3.5). Cost: more pool fragmentation under heterogeneous cipher negotiation.
- **Relax to the boolean only.** Maximizes reuse and is sufficient for HBA. Cost: a reused backend's `pg_stat_ssl` shows the *first* client's cipher for later same-tier clients — a reporting nuance, not an auth bug. Requires changing `compatible_for_reuse` to drop the version/cipher equality terms (and the warmup key to match).

**Recommendation:** ship Phase-1 with the existing exact-match (no code change to the key), measure fragmentation via the `ssl_pool_bucket_skipped` counter (§6.7), and only relax to boolean-only if fragmentation is material (deferred, §9). The HBA correctness is identical either way.

Disconnected pooled entries carry no live advertised state (`disconnected = candidate->get_pg_connection() == nullptr`, `PgSQL_HostGroups_Manager.cpp:~5429`); they are admitted into the RFQ branch and reconnected with the acquiring session's descriptor, so they are SSL-bucket-agnostic — correct as-is (the `compatible_for_reuse` check is skipped for the disconnected case in the loop).

### 6.5 Config knobs (string-form, CT conventions)

Follow the existing PolarDB thread-variable pattern (decls `PgSQL_Thread.h:1094-1105`, e.g. `polardb_proxy_protocol` at 1101; registration array `PgSQL_Thread.cpp:413-417`; `_from_string`/`_from_int` mappers in `PgSQL_PolarDB.h` — `polardb_proxy_protocol_from_string` at ~837, `_from_int` at ~445).

- `pgsql-polardb_proxy_ssl_propagation` — string enum `off | best_effort | strict` (default `best_effort`):
  - `off`: never emit SSL keys (current behavior; backend sees the client hop as plaintext — only safe if the cluster has no `hostssl` client rules).
  - `best_effort`: emit when the frontend is encrypted and version+cipher are available; if unavailable, advertise plaintext (HBA may then reject — visible via counter).
  - `strict`: emit when encrypted; if version/cipher unavailable, **refuse** the backend connection rather than misreport.
- Per-hostgroup override mirrors `proxy_protocol` (the HG policy consulted in `build_polardb_startup_profile`): add `ssl_propagation` to the HG policy with the same `-1 = inherit global` rule.
- Backend transport SSL (B) stays controlled by `pgsql_servers.use_ssl` + `pgsql-ssl_p2s_*`; **no new knob.** The doc explicitly notes (B) and (C) are decoupled and that turning on (C) does **not** require (B) — except that audit-grade `pg_stat_ssl` does require both (§3.5).

### 6.6 Dual-tier (`POLARDB_PROXY=1`/`=0`) + vanilla-backend safety

- All new code lives under `#if POLARDB_PROXY` like the existing emission block (`PgSQL_Connection.cpp:1317-1334` client-backed, `1337-1346` non-client). With `POLARDB_PROXY=0` the SSL population, knob, and emission compile out; the stub layer (`lib/PgSQL_PolarDB_Stubs.cpp`) keeps signatures.
- **`POLARDB_PROXY=0` byte-shape guarantee:** under `=0` no `_polar_*` keys are emitted for *any* hostgroup; the conninfo is byte-identical to the pre-PolarDB builder. SSL propagation adds nothing on `=0`.
- **Vanilla PostgreSQL backend safety:** emission is enabled when `is_polardb_hostgroup(hid)` (the `is_polardb_hg` condition at `PgSQL_Connection.cpp:1231/1321/1341`) AND a non-OFF proxy protocol (`profile.emits_startup_params()`). A vanilla backend placed in a PolarDB hostgroup would already receive the identity/RFQ keys and reject them as unknown GUCs — SSL adds no new failure mode; it is the same pre-existing operator misconfiguration.

### 6.7 Observability / counters

Add `T(...)` entries to the PolarDB counter X-macro list `POLARDB_COUNTER_LIST(T, G)` (`include/PgSQL_PolarDB_Counters.h:22`; entry form `T(name, "Display", "prom_name", "help")`, e.g. `rfq_profile_skipped` at line 56). Proposed:

| Counter | Meaning |
|---|---|
| `ssl_propagated` | backends started advertising client SSL = on |
| `ssl_propagation_skipped_no_metadata` | encrypted frontend but version/cipher unavailable (drives `strict`-refuse / `best_effort`-plaintext) |
| `ssl_pool_bucket_skipped` | pooled backend passed over because its advertised SSL bucket differed from the session's |
| `ssl_propagation_refused` | `strict`-mode connection refusals |

Aggregate via the same per-thread → residual-atomic path as `rfq_profile_skipped` (`POLARDB_THREAD_COUNT_ONE`, used at `PgSQL_HostGroups_Manager.cpp:~5492`; macro at `PgSQL_Thread.h:823`).

Also extend `fill_internal_session` JSON: the frontend block already emits `client.encrypted` + `client.ssl_cipher` (`PgSQL_Session.cpp:735-744`); add the backend's advertised descriptor under the per-backend block so the two hops are visible side by side for debugging.

### 6.8 Edge / failure cases

1. **Non-SSL client.** `client_myds->encrypted == false` → no SSL keys → backend `polar_proxy_ssl_in_use=false` → client HBA pass matches `host` / `hostnossl`. Correct.
2. **`SSL_get_version`/`SSL_get_current_cipher` returns NULL.** Treat as "metadata unavailable": `best_effort` → advertise plaintext (counter); `strict` → refuse. Never advertise `use_ssl=true` with an empty string (backend FATAL §3.2).
3. **HBA reject on the proxy pass.** If the cluster's `hostssl` client rule doesn't match (wrong client subnet, or `clientcert` required and we pass none), the backend implicitly rejects (`hba.c:2218`). ProxySQL sees an auth-failed backend connect; surface it via the existing backend-connect error path. Enabling propagation **can change HBA outcomes** — operators must be warned.
4. **`cipher_bits = 0` in `pg_stat_ssl`.** Known backend asymmetry (§3.4); version/cipher correct, bits 0 for the proxy-SSL-only path. Documented limitation, not actionable in ProxySQL.
5. **Mixed SSL/plaintext clients on one hostgroup.** Expected; the pool maintains separate buckets. Operators should size pools accordingly; `ssl_pool_bucket_skipped` exposes fragmentation.
6. **Legacy protocol (`PolarDB_ProxyProtocol::LEGACY`).** The builder must **not** emit SSL keys in the legacy branch (§6.3): the backend treats SSL metadata as PG15-only, and the libpq patch's serialization is not dialect-scoped. With `ssl_propagation != off` on a legacy hostgroup, log/skip rather than emit.
7. **`use_ssl` (B) toggled at runtime** does not change (C). Toggling `ssl_propagation` (C) affects only **new** backend connections; pooled ones keep their advertised `S`, and the pool condition keeps that safe.

### 6.9 Security / trust model

The backend accepts the advertised SSL strings with **zero validation** — `be_tls_get_version/cipher` return them verbatim (§3.4), and `postmaster.c` performs only bool/completeness checks (§3.1/§3.2). This is acceptable because **ProxySQL is the trusted proxy**: the design assumes a fully trusted proxy that asserts the client's SSL identity. There is **no cryptographic binding** between an advertised cipher and any real handshake on the client hop. Anyone who can open a proxy-mode startup packet to the backend can claim any SSL state; the backend's only defense is the network ACL on its listener (proxy mode requires reaching the backend listener). Operators must restrict the PolarDB backend listener to trusted proxies. This is a property of the PolarDB proxy contract, not a ProxySQL choice — but it must be stated explicitly because (C) extends what ProxySQL asserts on the client's behalf.

---

## 7. Interaction with split / warmup and identity-aware pooling

- **Identity-aware pooling is already implemented.** The PolarDB pool groups connections by advertised identity *and* by the currently unused SSL/session/cancel fields through `compatible_for_reuse` (`PgSQL_PolarDB.h:649-660`) and the warmup key (`PgSQL_HostGroups_Manager.cpp:5211-5222`). The SSL work simply **populates the SSL dimension of an existing key**; it does not need to invent the key.
  - However, the identity dimension is only *meaningfully* populated for client-backed sessions; `polardb_session_startup_client_context` derives identity from the real client address. The SSL fields are derived from the same client at the same point. So the two dimensions become live together, as they should.
- **Split holds multiple backends per frontend** (primary + replica). Each split backend is created via the same builder/profile path, so each inherits the **same** frontend `S(F)` — one client → one SSL state. Capture the descriptor **once** from the frontend (§6.1) and reuse it for every backend acquired for that frontend, so any (rare/absent in PostgreSQL) mid-session re-handshake cannot desync siblings. Unlike session-id/cancel, which **must differ per backend** (sibling doc), SSL is per-frontend and shared across that frontend's backends.
- **Lazy split warmup** creates pool entries before a client owns them. A warmed entry built without a client advertises `frontend_ssl=false`; when later acquired by an SSL client, `compatible_for_reuse` skips it (bucket mismatch) and forces a fresh connect — correct, but a warmup-efficiency loss. The warmup dedup key already carries the SSL fields (`PgSQL_HostGroups_Manager.cpp:5211-5222`), so warmup can be made SSL-bucket-aware by populating `req.startup_client` SSL fields from the requesting frontend. Options: (a) warm separate SSL/plaintext buckets if the deployment is SSL-dominated; (b) accept the reconnect. Flag for the warmup design.

---

## 8. Testing & verification plan

### 8.1 Unit (link against `libproxysql.a`, `test/.../unit/`)

- `append_polardb_startup_params` emits all three SSL keys for an encrypted descriptor, none for plaintext, and never `use_ssl=true` with empty version/cipher (asserts Invariant 2 / §6.3 check).
- SSL keys emitted **only** in the V15 branch, never the LEGACY branch (§6.3 / §6.8.6).
- SSL key values pass through `append_conninfo_param` (or the chosen validator) — assert no raw injection for a crafted cipher string with a space/quote (§6.3 / §4.2 item 3).
- `PolarDB_StartupClientContext::compatible_for_reuse` truth table over the SSL fields (boolean + version + cipher), confirming bucketing once populated.
- Knob mapping `off|best_effort|strict` ↔ enum, per-HG inherit (`-1`).

### 8.2 TAP (`test/tap/tests`, registered in `groups.json`, PolarDB/`pgsql16` group)

- **HBA check (the load-bearing test).** Configure backend `hostssl` for the client subnet only: SSL client succeeds, plaintext client is rejected; flip to `hostnossl` and invert. This shows (C) drives the *client* HBA pass independently of (B).
- **Observability (corrected expectations).** With hop (B) = TLS (`pgsql_servers.use_ssl=1`): SSL client → assert `pg_stat_ssl` for the backend reports the **client's** TLS version+cipher, and `ssl_version()`/`ssl_cipher()` SQL match. With hop (B) = plaintext: assert `pg_stat_ssl` shows **not-SSL** for the client hop even though the client used TLS and (C) is advertised (shows the §3.5 two-hop conjunction; do **not** assert client cipher visibility here).
- **Pool bucket.** Interleave SSL and plaintext clients on one hostgroup; assert no cross-bucket reuse via `ssl_pool_bucket_skipped` movement and, if feasible, backend PID/`pg_stat_ssl` correlation.
- **Negative.** `strict` mode + forced NULL cipher → connection refused with a clear error; `best_effort` → plaintext advertised + counter increment.

### 8.3 Backend G0-style (PolarDB tree)

- Re-confirm against the exact `POLARDB_15_PROXY` CI revision: the all-or-nothing FATAL (`postmaster.c:2547-2549`) and the no-proxy-no-ssl FATAL (`2555-2559`).
- Confirm `be_tls_get_version/cipher` precedence (`be-secure-openssl.c:1303-1322`) and the `cipher_bits=0` asymmetry (`1289-1300`).
- Confirm the `pg_stat_ssl` / auth-log controlling on `port->ssl_in_use` (`backend_status.c:426`, `postinit.c:283`, `be-secure-openssl.c:459`, `pgstatfuncs.c:843`).

---

## 9. Phasing, deferrals, and open questions

### Phasing

1. **Phase 1 — populate + emit.** Populate the existing SSL fields in `polardb_session_startup_client_context`, set them on the connection, emit the V15-only SSL keys via `append_conninfo_param`, add the `ssl_propagation` knob and counters. Because the pool/warmup keys already bucket on these fields, mixed-client pools become safe **as soon as the fields are populated** — there is no separate "Phase 2 pool condition" to land for SSL.
   - **Safety envelope (corrected & widened):** the pool already buckets on advertised **identity** as well as SSL. Both dimensions become live together, so a homogeneous-client restriction is *not* required — the key correctly separates clients by (identity, SSL). The pre-existing risk the first draft flagged (identity not bucketed) does **not** apply to this arch; the SSL dimension is the only inert part, and Phase 1 lights it up.
2. **Phase 2 — pool-key granularity tuning (optional).** If `ssl_pool_bucket_skipped` shows material fragmentation, relax `compatible_for_reuse` (and the warmup key) to bucket on the boolean `frontend_ssl` only, dropping exact version/cipher equality (§6.4). Document the `pg_stat_ssl` cipher-staleness caveat.
3. **Phase 3 — warmup bucket awareness (optional).** Populate `req.startup_client` SSL fields from the requesting frontend so warmed entries land in the right bucket (§7).

### Hard limitations (document, do not attempt)

- **Client certificate identity is unsupportable through proxy mode.** There is no `_polar_proxy_*` key for the peer cert subject/serial/issuer; the backend cannot see the client cert through proxy mode, so `hostssl ... clientcert=verify-*` / `clientname` client rules cannot be satisfied via ProxySQL.
- **Hostname-based HBA rules are unmatchable through proxy mode.** `check_hostname` short-circuits to `false` for proxy connections (`hba.c:698-703`); a `host ... <hostname>` line never matches the advertised-client pass.
- **`pg_stat_ssl.bits`** is `0` for the proxy-SSL-only path (backend-side, §3.4) — out of ProxySQL scope.

### Defer

- Per-version/per-cipher relaxation (boolean is sufficient for HBA; §6.4) — only if fragmentation warrants.
- Backend `cipher_bits` correctness (backend-side change).

### Open questions

- **OQ1.** Does any consumer in the deployed PolarDB build read `polar_proxy_ssl_cipher_name`/`version` beyond `pg_stat_ssl` + the auth log (e.g. a policy GUC keyed on cipher strength)? If so, exact-cipher pool keying (the current `compatible_for_reuse` behavior) must be retained, and the boolean-only relaxation (§6.4 / Phase 2) is off the table.
- **OQ2 (open — UNVERIFIED).** Is the frontend SSL handshake guaranteed complete before the first backend connection is built in **all** session paths (fast-forward, prepared-statement-first, multiplexed)? The handshake state machine (`PgSQL_Session.cpp:4124-4134`) precedes dispatch, but the fast-forward path (`session_fast_forward`) was **not** traced to confirm `client_myds->ssl` is populated at descriptor-capture time. Verify before implementation.
- **OQ3 (partly answered by source).** When `pgsql_servers.use_ssl=0` (plaintext backend transport) but the client is SSL and `ssl_propagation` wants `use_ssl=true`: the backend **accepts** this (independent hops) and the HBA decision is correct, **but** `pg_stat_ssl`/audit will show not-SSL for the client hop (§3.5). The remaining policy question: is sending client TLS metadata over a plaintext proxy↔backend link an acceptable posture for the target deployments, or should `strict` additionally require `use_ssl=1` on the backend transport (B) to keep audit honest? Recommend documenting the §3.5 conjunction and letting operators decide; consider a future `strict+require_backend_tls` variant.
- **OQ4 (open — UNVERIFIED).** Is `pgsql-polardb_proxy_ssl_propagation` cluster-replicated? It is a `pgsql-*` thread variable, so it *should* follow the existing cluster-sync of pgsql variables, but the cluster-variable sync list was **not** opened during verification. Verify inclusion before relying on cluster sync, or scope the knob node-local.
