# 51 - Reader connection transfer and locking

This document describes the current ReaderPool connection flow. It
focuses on who owns each connection, when a mutex is used, and which lock order
is allowed.

## 1. Design summary

ReaderPool is a routing and compatibility policy over the core PostgreSQL
connection pool. It does not own a second set of backend connections.

The order is:

1. Read the current topology snapshot.
2. Choose a reader from server status, weight, LSN/lag state, and global active
   connection counts.
3. After the server is known, try an exact connection retained by the current
   worker.
4. If there is no local match, try the selected server's shared FREE list.
5. If the request allows it, reset or create a connection on that same server.

Worker-local connections cannot choose the server. This keeps routing fair
across workers and prevents a warm local connection from attracting traffic
that should go to another reader.

## 2. Connection ownership

Every backend connection belongs to one PgSQL_SrvC.

| State | Core list | Can another worker take it? | Mutex |
|---|---|---|---|
| Shared idle connection | FREE | Yes | Selected server mutex protects take |
| Active query connection | USED | No | Transfer into USED is protected |
| Retained by one worker for the current pass | USED | No | No mutex while retained |
| Returning to shared idle | USED to FREE | After transfer completes | Selected server mutex protects return |
| Closed or rejected | Removed | No | Removed under the owning list rules |

A connection retained by a worker remains in the core USED list. There is no
second ownership state and no duplicate ReaderPool counter used for capacity.

## 3. Worker-local reuse

The worker uses its existing cached_connections array. PolarDB reader entries
are separated from the classic PostgreSQL local-cache lookup.

The worker keeps no more than one connection for the same:

- selected server;
- startup profile generation;
- exact pool key.

The entry lives only until PgSQL_Thread::return_local_connections() at the end
of the current worker pass. There are no fixed slot counts, depth settings,
refill settings, or idle expiry timers.

When a later query in the same pass selects the same server, the worker checks
for the same generation and key. polardb_reader_pool_conn_usable() then checks
the connection against the current session, including login and connection
options, before reuse.

If a second connection with the same tuple is released, it is not kept locally.
It returns immediately to that server's shared pool so another worker can use
it.

## 4. Shared pool transfer

Each PgSQL_SrvC owns:

- ConnectionsFree;
- ConnectionsUsed;
- the exact-key FREE index and reverse lookup;
- atomic FREE and USED counts;
- one std::recursive_mutex named pool_mutex.

The mutex keeps the list, key index, reverse lookup, connection position, and
counts consistent during a transfer.

An exact shared take performs:

    lock selected server
      find exact key in FREE index
      remove connection from FREE
      add connection to USED
    unlock selected server

A shared return performs:

    lock selected server
      find connection in USED
      remove connection from USED
      if server is online:
        rebuild or confirm exact key
        add connection to FREE and key index
      else:
        leave it removed so the caller closes it
    unlock selected server

At worker-pass end, retained connections are grouped by server. The worker
holds one server mutex while returning that server's group. The existing return
helpers enter the same mutex again, which is why it is currently recursive.

Changing pool_mutex to std::mutex requires a separate code cleanup: the outer
batch must call only helpers that assume the mutex is already held. It is not
safe to change the mutex type alone.

## 5. Lock order

There are two lock levels:

1. HGM read/write lock for topology, status changes, capacity checks, and
   topology-owned reports.
2. One server pool_mutex for that server's FREE/USED connection transfer.

The rules are:

| Rule | Allowed |
|---|---|
| HGM lock, then one server mutex | Yes |
| One server mutex without HGM | Yes |
| Server mutex, then HGM lock | No |
| Two server mutexes at the same time | No in normal query and maintenance paths |
| Network connect, query, reset, or ping while either lock is held | No |

Reader selection uses immutable topology and atomic server values. It does not
take pool_mutex. A shared connection take or return does not take the HGM lock.
Creation is different because it changes global capacity: it takes HGM first,
rechecks the selected server, then takes that server's mutex to register the new
USED connection.

## 6. Topology and server removal

A selected connection keeps the server-list snapshot alive while it is active
or retained by the worker. This prevents its conn->parent server object from
being destroyed during a topology update.

An OFFLINE_HARD change takes HGM and then the server mutex. It changes status
and drains FREE connections while the same mutex excludes concurrent transfers.
A connection returning after the status change is removed from USED and closed
instead of being added back to FREE.

Maintenance code holds HGM while it walks topology. When PolarDB is enabled it
also takes one server mutex at a time before scanning or moving that server's
connections. It does not keep several server mutexes at once.

## 7. Disabled build

With POLARDB_PROXY=0, the classic PostgreSQL pool does not contain the PolarDB
exact-key maps, atomic FREE/USED counts, or per-server pool mutex. Classic list
operations and maintenance remain under the original HGM ownership model.

The disabled build still contains compile-time key type declarations used by
shared headers, but they add no per-server or per-connection runtime storage.

## 8. Performance counters

POLARDB_PROFILE=1 adds counters for:

- local take attempt, hit, and miss;
- local store attempt, accepted, and rejected;
- local entries returned to shared pools;
- shared take attempt, hit, and miss;
- shared return attempt, accepted, and rejected;
- total shared-return mutex wait and hold time.

These counters are absent from production builds. They are intended to answer:

- how often same-pass local reuse avoids a mutex;
- whether duplicate releases are reaching shared pools;
- whether shared server mutex wait time is material;
- whether connections are being rejected during return.

## 9. Deferred cleanup

The following are separate changes and are not part of this design:

- converting the recursive server mutex to a plain mutex;
- replacing per-connection snapshot ownership with a different lifetime model;
- adding a long-lived or fixed-depth worker reader cache;
- adding automatic cache depth or expiry tuning;
- implementing real PostgreSQL idle-socket ping I/O.

Any future change must preserve server-first routing, one core connection owner,
the HGM-then-server lock order, and the disabled-build separation.
