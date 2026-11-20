# Polar Proxy Changelog

All notable changes Polar Proxy makes on top of
[ProxySQL](https://github.com/sysown/proxysql) will be documented in this
file. Changes in ProxySQL itself are listed in `CHANGELOG.md`.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions are `X.Y.Z.N`: `X.Y.Z` is the ProxySQL release Polar Proxy is based
on, and `N` counts Polar Proxy releases on that base, starting from 0.

## [Unreleased]

### Added

- Support for PolarDB clusters. Set `check_type` to `'polardb'` for a
  writer/reader pair in `pgsql_replication_hostgroups` to enable it. The
  monitor then takes each node's role and replay position from PolarDB
  itself, shuns nodes that PolarDB reports as unavailable (for example,
  during maintenance) and brings them back once they become available again
  (XCOM-247)
- Read-your-writes routing for PolarDB. Reads matched by a query rule with
  the new `pgsql_query_rules.replica_eligible` column set to 1 go to a
  reader. If the session has written data the reader has not replayed yet,
  the reader first waits until it catches up, so the session always sees
  its own changes. `replica_eligible = 0` keeps matching reads on the
  writer. Rules that only set `destination_hostgroup` route as before and
  get no such protection (XCOM-247)
- Consistency modes for PolarDB reads: `session_lsn` (the default) makes a
  session see its own writes, `global_lsn` makes it see all writes committed
  on the writer before the read started, `eventual` sends reads to readers
  without waiting, and `off` leaves routing to query rules alone. The mode is
  set with `pgsql-polardb_consistency_mode`, per writer/reader pair with
  `pgsql_replication_hostgroups.consistency_mode`, and per session with
  `SET proxysql.polardb_consistency_mode` (`RESET` returns to the configured
  mode) (XCOM-247)
- A single read can be sent to the writer by starting it with the
  `/* route=primary */` comment (XCOM-247)
- Settings choosing what happens when a PolarDB read cannot be served safely
  by a reader: the reader does not catch up within
  `pgsql-polardb_lsn_wait_timeout_ms` (1 second by default, also settable
  per pair with `lsn_wait_timeout_ms`), the session's position is unknown,
  no suitable reader is available, or the reader fails or disconnects. The
  read can run on the writer instead (the default, done only before any
  part of the result has reached the client), return the reader's possibly
  stale result with a warning, fail with an error, or close the session.
  `pgsql-polardb_profile` sets all of them at once to one of the predefined
  profiles: `session_fallback` (the default), `session_warning`,
  `session_error`, `global_fallback`, `global_error`, `eventual` or `off`;
  changing an individual `pgsql-polardb_action_*` setting switches the
  profile to `custom` (XCOM-247)
- Readers too far behind the writer can be excluded from routing with
  `pgsql-polardb_max_reader_lsn_gap_bytes`, or per pair with
  `pgsql_replication_hostgroups.max_lag_bytes`. The limit is off by default
  (XCOM-247)
- Reads sent with the extended query protocol, which most drivers use for
  parameterized queries and prepared statements, can use readers with the
  same read-your-writes guarantee. This requires a PolarDB server that
  supports it and `proxy_protocol = 'v15_wait'` for the pair in
  `pgsql_replication_hostgroups` or in `pgsql-polardb_proxy_protocol`. With
  other protocol settings such reads go to the writer whenever they would
  have to wait (XCOM-247)
- Transaction split: with `pgsql_replication_hostgroups.txn_split_enabled`
  set to 1, plain `SELECT` statements inside `READ COMMITTED` transactions
  can run on a reader while the transaction itself stays on the writer.
  This works only for queries sent with the simple query protocol (XCOM-247)
- With `pgsql-polardb_proxy_identity_mode` set to `'client'`, PolarDB sees
  the address of the application client instead of the address of the
  proxy (XCOM-247)
- PolarDB routing statistics in `stats_pgsql_global` (`PolarDB_*`
  variables), also exported to Prometheus as `proxysql_polardb_*_total`:
  how many reads went to readers or fell back to the writer, how long reads
  waited for readers to catch up, how many waits timed out, and more
  (XCOM-247)
- PolarDB support is built by default. Build with `make POLARDB_PROXY=0` to
  get a binary without it (XCOM-247)

### Changed

- Polar Proxy reports its own version in the `X.Y.Z.N` form, for example
  `3.0.10.0`, where `X.Y.Z` is the ProxySQL release it is based on (XCOM-247)

### Fixed

- Queries on long-lived client sessions could fail with `Max connect timeout
  reached while reaching hostgroup ... after 0ms` once the session had been
  connected to the backend longer than `pgsql-connect_timeout_server_max`
  (10 seconds by default), although no connection attempt took that long
  (XCOM-247)
- Running `\d+` in psql connected to the admin interface crashed the proxy.
  It now returns an error suggesting `\d` instead (XCOM-247)

### Performance

- Under heavy load, requests to a writer or reader that has reached its
  connection limit now wait for a connection to become free instead of
  repeatedly retrying, which reduces CPU usage and lock contention
  (XCOM-247)
- Collecting query digest statistics no longer makes all PostgreSQL worker
  threads contend for one lock on every query, which improves throughput
  with many clients (XCOM-247)
- PostgreSQL worker threads no longer take logger locks when the event and
  audit logs are disabled (XCOM-247)
- Query results are sent to clients without TLS without copying them into
  an intermediate buffer first; `pgsql-polardb_writev_direct = false`
  restores the old behavior (XCOM-247)

[unreleased]: https://github.com/awide-labs/polar-proxy/compare/2deb0acc3e77...v3.0
