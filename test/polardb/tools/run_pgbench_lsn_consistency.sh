#!/usr/bin/env bash
# Run a client-visible RFQ LSN consistency check with pgbench.
#
# Prefer test/polardb/bin/pgbench-polar-rfq when it is built. That binary has
# patched libpq linked statically and exposes the \polar_lsn meta-command.
# The startup options are passed through the dbname/conninfo argument.
#
# The result is client-observable:
#   CONSISTENT means the read response RFQ LSN reached the session target LSN
#   captured before the read. It does not expose ProxySQL's internal route or
#   wait branch.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLARDB_TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CALLER_PGBENCH_BIN="${PGBENCH_BIN:-}"
# shellcheck source=../common/env.sh
. "$POLARDB_TEST_DIR/common/env.sh"

CLIENTS="${CLIENTS:-1}"
TXNS="${TXNS:-3}"
CLIENT_HOST="$POLARDB_RFQ_CLIENT_HOST"
CLIENT_PORT="$POLARDB_RFQ_CLIENT_PORT"
DEFAULT_PGBENCH_POLAR_RFQ="$PROXYSQL_ROOT/test/polardb/bin/pgbench-polar-rfq"

if [ -z "$CALLER_PGBENCH_BIN" ] && [ -x "$DEFAULT_PGBENCH_POLAR_RFQ" ]; then
    PGBENCH_BIN="$DEFAULT_PGBENCH_POLAR_RFQ"
fi
polardb_require_pgbench

if ! "$PGBENCH_BIN" --version 2>/dev/null | grep -q 'polar-rfq-lsn'; then
    echo "PGBENCH_BIN does not expose native RFQ-LSN support: $PGBENCH_BIN" >&2
    echo "Run: make -C $PROXYSQL_ROOT/test/polardb build-pgbench-polar-rfq" >&2
    exit 1
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/polardb-pgbench-lsn.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

setup_sql="$tmpdir/setup.sql"
workload_sql="$tmpdir/session_consistency.sql"

cat >"$setup_sql" <<'SQL'
CREATE TABLE IF NOT EXISTS polardb_lsn_check (
  id bigint PRIMARY KEY,
  val bigint NOT NULL DEFAULT 0
);
SQL

cat >"$workload_sql" <<'SQL'
\set id random(1, 1000000)
BEGIN;
INSERT INTO polardb_lsn_check(id, val)
  VALUES (:id, 1)
  ON CONFLICT (id) DO UPDATE SET val = polardb_lsn_check.val + 1;
\polar_lsn write
SELECT val FROM polardb_lsn_check WHERE id = :id;
\polar_lsn read
\if :read_has_lsn
  \if :read_lsn >= :write_lsn
    \shell echo PGBENCH_POLAR_LSN verdict CONSISTENT write_lsn :write_lsn read_lsn :read_lsn
  \else
    \shell echo PGBENCH_POLAR_LSN verdict STALE_BY_LSN write_lsn :write_lsn read_lsn :read_lsn
  \endif
\else
  \shell echo PGBENCH_POLAR_LSN verdict UNKNOWN_NO_RFQ_LSN write_lsn :write_lsn read_lsn :read_lsn
\endif
COMMIT;
SQL

base_conninfo="host=$PROXYSQL_HOST port=$PROXYSQL_PORT user=$PGUSER password=$PGPASSWORD dbname=$PGDB sslmode=$PGSSLMODE"
lsn_conninfo="$base_conninfo _polar_proxy_send_lsn=true _polar_proxy_client_host=$CLIENT_HOST _polar_proxy_client_port=$CLIENT_PORT"

echo "== setup table through pgbench-polar-rfq =="
"$PGBENCH_BIN" -n -M simple -c 1 -t 1 -f "$setup_sql" "$base_conninfo"

echo
echo "== pgbench RFQ-LSN consistency check =="
echo "pgbench=$PGBENCH_BIN"
echo "clients=$CLIENTS txns=$TXNS"
echo "Native verdicts:"
echo "  CONSISTENT           read RFQ LSN >= session target captured before read"
echo "  STALE_BY_LSN         read RFQ LSN < target"
echo "  UNKNOWN_NO_RFQ_LSN   client did not receive RFQ LSN"
echo

"$PGBENCH_BIN" -n -M simple -c "$CLIENTS" -t "$TXNS" -f "$workload_sql" "$lsn_conninfo"
