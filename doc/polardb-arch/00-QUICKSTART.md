# 00 - Quick Start: ProxySQL with PolarDB Support

> Scope: first operator path from build/install to a running ProxySQL instance configured for PolarDB read-your-writes (RYW). | Audience: O/M | Status: stable | Prereqs: a reachable PolarDB writer and reader, PostgreSQL client tools for validation | Verified against: this branch

This quick start is intentionally operational. It shows the minimum sequence to build or install the PolarDB-enabled ProxySQL binary, initialize a data directory, start ProxySQL, register a PolarDB writer/reader pair, enable session consistency, and verify that the path is alive.

For detailed behavior and troubleshooting, read [17-OPERATOR-GUIDE.md](17-OPERATOR-GUIDE.md). For every admin variable and schema column, read [04-ADMIN-SCHEMA-AND-CONFIG.md](04-ADMIN-SCHEMA-AND-CONFIG.md).

## Table of Contents

1. [Assumptions and Ports](#1-assumptions-and-ports)
2. [Build or Install ProxySQL](#2-build-or-install-proxysql)
3. [Initialize and Start ProxySQL](#3-initialize-and-start-proxysql)
4. [Prepare the PolarDB Cluster](#4-prepare-the-polardb-cluster)
5. [Configure ProxySQL for the PolarDB Cluster](#5-configure-proxysql-for-the-polardb-cluster)
6. [Enable Session Consistency](#6-enable-session-consistency)
7. [Application Usage](#7-application-usage)
8. [Validate the Setup](#8-validate-the-setup)
9. [Basic Troubleshooting](#9-basic-troubleshooting)

## 1. Assumptions and Ports

The examples use these names and ports. Replace them with your environment values.

| Item | Example value |
|---|---|
| PolarDB writer | `polardb-writer.example.com:5432` |
| PolarDB reader | `polardb-reader.example.com:5432` |
| ProxySQL admin over MySQL protocol | `127.0.0.1:6032`, user `admin`, password `admin` |
| ProxySQL admin over PostgreSQL protocol | `127.0.0.1:6132`, user `admin`, password `admin` |
| ProxySQL PostgreSQL listener for applications | `0.0.0.0:6033` |
| Writer hostgroup | `10` |
| Reader hostgroup | `11` |
| Application user | `app` |
| Application database | `appdb` |

Use the MySQL-protocol admin listener for the SQL examples below unless your tooling is already using the PostgreSQL admin listener. The admin SQL is the same configuration surface.

## 2. Build or Install ProxySQL

### Option A - Build from this source tree

Build with PolarDB support enabled:

```bash
make -j$(nproc) POLARDB_PROXY=1
```

`POLARDB_PROXY=1` is the required feature switch. It compiles the PolarDB code, applies the PolarDB libpq RFQ-LSN patch, exposes the `pgsql-polardb_*` variables, adds PolarDB columns to `pgsql_replication_hostgroups`, and adds `replica_eligible` to `pgsql_query_rules`.

The resulting binary is:

```bash
./src/proxysql
```

### Option B - Install a packaged binary

Install the package using your normal OS package flow. The package must be built with `POLARDB_PROXY=1`. Verify after start with:

```sql
SELECT variable_name, variable_value
FROM global_variables
WHERE variable_name LIKE 'pgsql-polardb%'
ORDER BY variable_name;
```

If this returns no rows, the binary does not include PolarDB support.

## 3. Initialize and Start ProxySQL

ProxySQL needs a config file and a data directory. This minimal config enables both admin listeners and the PostgreSQL client listener.

Create `/etc/proxysql-polardb.cnf`:

```ini
datadir="/var/lib/proxysql-polardb"

admin_variables=
{
    admin_credentials="admin:admin"
    mysql_ifaces="127.0.0.1:6032"
    pgsql_ifaces="127.0.0.1:6132"
}

pgsql_variables=
{
    interfaces="0.0.0.0:6033"
}
```

Initialize the data directory and start ProxySQL in the foreground for the first run:

```bash
sudo install -d -m 0750 -o proxysql -g proxysql /var/lib/proxysql-polardb
sudo ./src/proxysql -f -c /etc/proxysql-polardb.cnf
```

If you run from a package, use the installed binary path instead of `./src/proxysql`.

For service-manager deployments, start the packaged service after placing the equivalent config. The important point is that the first start creates the internal SQLite database in `datadir`; later `SAVE ... TO DISK` commands persist runtime configuration there.

Confirm the admin and PostgreSQL listeners:

```bash
mysql -h127.0.0.1 -P6032 -uadmin -padmin -e "SELECT 1"
psql "host=127.0.0.1 port=6033 user=app dbname=appdb" -c "SELECT 1"
```

The second command will succeed only after the backend user is configured in Section 5.

## 4. Prepare the PolarDB Cluster

You need one writer endpoint and at least one reader endpoint. Both must be real PolarDB nodes, not plain PostgreSQL.

Create or confirm the application user on the PolarDB writer and make sure the same credentials work on readers:

```sql
CREATE USER app WITH PASSWORD 'secret';
CREATE DATABASE appdb OWNER app;
GRANT CONNECT ON DATABASE appdb TO app;
```

Confirm direct connectivity before putting ProxySQL in the path:

```bash
psql "host=polardb-writer.example.com port=5432 user=app password=secret dbname=appdb" -c "SELECT 1"
psql "host=polardb-reader.example.com port=5432 user=app password=secret dbname=appdb" -c "SELECT 1"
```

The PolarDB backend must support the underscore core GUCs used by the proxy:

```sql
SET polar_consistency_mode = 'best_effort';
SET polar_proxy_wait_timeout_ms = 1000;
SET polar_xact_split_wait_lsn = '0';
```

Do not use dotted `polardb.*` GUC names for the wait path. PostgreSQL can accept unknown dotted custom options as inert placeholders, while the PolarDB wait machinery reads the underscore core GUCs above.

## 5. Configure ProxySQL for the PolarDB Cluster

Connect to the ProxySQL admin interface:

```bash
mysql -h127.0.0.1 -P6032 -uadmin -padmin
```

Register the writer and reader backends:

```sql
INSERT INTO pgsql_servers (hostgroup_id, hostname, port)
VALUES
  (10, 'polardb-writer.example.com', 5432),
  (11, 'polardb-reader.example.com', 5432);
```

Register the application user. The default hostgroup should be the writer hostgroup:

```sql
INSERT INTO pgsql_users (username, password, active, default_hostgroup)
VALUES ('app', 'secret', 1, 10);
```

Define the PolarDB writer/reader pair:

```sql
INSERT INTO pgsql_replication_hostgroups
  (writer_hostgroup, reader_hostgroup, check_type,
   txn_split_enabled, consistency_mode,
   max_lag_bytes, lsn_wait_timeout_ms, proxy_protocol, comment)
VALUES
  (10, 11, 'polardb',
   0, 'lsn',
   -1, 1000, 'v15', 'main PolarDB cluster');
```

Mark ordinary `SELECT` traffic as replica-eligible:

```sql
INSERT INTO pgsql_query_rules
  (rule_id, active, match_digest, replica_eligible, apply)
VALUES
  (100, 1, '^SELECT', 1, 1);
```

Load the configuration to runtime and persist it:

```sql
LOAD PGSQL SERVERS TO RUNTIME;
LOAD PGSQL USERS TO RUNTIME;
LOAD PGSQL QUERY RULES TO RUNTIME;

SAVE PGSQL SERVERS TO DISK;
SAVE PGSQL USERS TO DISK;
SAVE PGSQL QUERY RULES TO DISK;
```

Why these values matter:

- `check_type='polardb'` enables the PolarDB LSN/RFQ path for this hostgroup pair.
- `consistency_mode='lsn'` enables read-your-writes routing for this pair.
- `proxy_protocol='v15'` requests RFQ LSN startup parameters using the current protocol form.
- `replica_eligible=1` opts matching reads into automatic PolarDB reader routing.
- `lsn_wait_timeout_ms=1000` gives a reader up to one second to catch up for a protected read.

## 6. Enable Session Consistency

Set the global defaults. The per-hostgroup `consistency_mode='lsn'` above already enables the main pair, but setting the global default makes behavior explicit for pairs that use `consistency_mode='default'`.

```sql
SET pgsql-polardb_consistency_mode = 'lsn';
SET pgsql-polardb_wait_timeout_mode = 'best_effort';
SET pgsql-polardb_proxy_protocol = 'v15';
SET pgsql-polardb_route_rfq_policy = 'strict';
SET pgsql-polardb_session_lsn_baseline = 'observed';

LOAD PGSQL VARIABLES TO RUNTIME;
SAVE PGSQL VARIABLES TO DISK;
```

Meaning:

- `pgsql-polardb_consistency_mode='lsn'`: protect eligible reads using the session LSN.
- `pgsql-polardb_wait_timeout_mode='best_effort'`: if a reader cannot catch up before timeout, return the result with a warning instead of failing the client query.
- `pgsql-polardb_proxy_protocol='v15'`: request the v15 PolarDB startup parameters for RFQ LSN.
- `pgsql-polardb_route_rfq_policy='strict'`: if an RFQ-derived target is unknown, route to writer instead of silently trusting the reader.
- `pgsql-polardb_session_lsn_baseline='observed'`: a read-only session can start on a reader and then use the observed RFQ LSN as future evidence.

For correctness-first deployments, switch timeout mode to strict:

```sql
SET pgsql-polardb_wait_timeout_mode = 'strict';
LOAD PGSQL VARIABLES TO RUNTIME;
SAVE PGSQL VARIABLES TO DISK;
```

In strict mode, a reader wait timeout does not return stale reader data. ProxySQL retries the original read on the writer when it is safe to do so.

## 7. Application Usage

Applications connect to the ProxySQL PostgreSQL listener, not directly to the PolarDB endpoints:

```bash
psql "host=proxysql.example.com port=6033 user=app password=secret dbname=appdb"
```

The default behavior is automatic:

- writes go to the writer hostgroup
- matching `SELECT` queries enter the `replica_eligible=1` rule
- ProxySQL tracks write and observed RFQ LSNs for the session
- protected reads use a caught-up reader or fall back safely to the writer

A session can override the consistency mode when needed:

```sql
SET proxysql.polardb_consistency_mode TO 'off';
SET proxysql.polardb_consistency_mode TO 'lsn';
SET proxysql.polardb_consistency_mode TO 'primary';
RESET proxysql.polardb_consistency_mode;
```

Use cases:

- `off`: disable PolarDB wait behavior for this session.
- `lsn`: use normal read-your-writes mode.
- `primary`: force eligible reads to the writer.
- `RESET`: return to the per-hostgroup/global policy.

To force one read to the writer without changing the session:

```sql
/* route=primary */ SELECT * FROM important_table WHERE id = 1;
```

Avoid manual rules that directly route protected reads to the reader hostgroup. Manual reader routes are authoritative and bypass the automatic wait wrapper.

## 8. Validate the Setup

Run a write followed by a read through ProxySQL:

```sql
CREATE TABLE IF NOT EXISTS quickstart_ryw(id int primary key, v text);

INSERT INTO quickstart_ryw VALUES (1, 'seen')
ON CONFLICT (id) DO UPDATE SET v='seen';

SELECT v FROM quickstart_ryw WHERE id = 1;
```

Expected result:

```text
seen
```

Check PolarDB counters:

```sql
SELECT Variable_Name, Variable_Value
FROM stats_pgsql_global
WHERE Variable_Name LIKE 'PolarDB_%'
ORDER BY Variable_Name;
```

Useful first counters:

| Counter | What it tells you |
|---|---|
| `PolarDB_Server_LSN_Updates_From_RFQ` | ProxySQL is receiving LSNs from backend RFQ messages. This should grow under normal traffic. |
| `PolarDB_Session_LSN_Routing` | Queries are entering the session-LSN routing path. |
| `PolarDB_Wait_Wrap_Prepared` | ProxySQL prepared a wait wrapper for a protected read. |
| `PolarDB_Wait_LSN_Sent` | ProxySQL sent the LSN wait wrapper to a reader. |
| `PolarDB_Wait_Wrap_Bypassed` | ProxySQL selected a reader already known to be caught up and skipped the wrapper. |
| `PolarDB_Wait_Error_Timeout` | A reader timed out waiting for the target LSN. |
| `PolarDB_Wait_Reads_Retried_On_Writer` | ProxySQL recovered a strict timeout or reader loss by retrying on the writer. |

At least one of `PolarDB_Wait_LSN_Sent` or `PolarDB_Wait_Wrap_Bypassed` should move when a write is followed by a protected read.

## 9. Basic Troubleshooting

### No `pgsql-polardb_*` variables

The binary is not built with PolarDB support. Rebuild or install a package built with `POLARDB_PROXY=1`.

### `PolarDB_Server_LSN_Updates_From_RFQ` stays at zero

Common causes:

- ProxySQL is linked with vanilla libpq instead of the patched libpq.
- The backend is not PolarDB.
- `proxy_protocol` resolves to `off` for the hostgroup pair.
- No traffic has completed through PolarDB backends yet.

Check:

```sql
SELECT writer_hostgroup, reader_hostgroup, check_type, consistency_mode, proxy_protocol
FROM runtime_pgsql_replication_hostgroups;
```

### Reads never go to readers

Common causes:

- no matching query rule with `replica_eligible=1`
- effective `consistency_mode='primary'`
- RFQ LSN is missing and `pgsql-polardb_route_rfq_policy='strict'` forces writer
- byte lag cap rejects the reader
- reader health is not `ONLINE`

Check runtime rules and hostgroups:

```sql
SELECT rule_id, active, match_digest, destination_hostgroup, replica_eligible, apply
FROM runtime_pgsql_query_rules
ORDER BY rule_id;

SELECT hostgroup_id, hostname, port, status
FROM runtime_pgsql_servers
ORDER BY hostgroup_id, hostname, port;
```

### Reads can be stale under lag

This is expected only when `pgsql-polardb_wait_timeout_mode='best_effort'` and the reader times out waiting for the target LSN. Use `strict` mode if a stale reader answer must never be returned.

### A wrapped read errors on a `polar_*` setting

The backend is probably not a compatible PolarDB server, or the PolarDB version does not expose the required underscore core GUCs. Do not point `check_type='polardb'` pairs at plain PostgreSQL nodes.

Verified against this branch.
