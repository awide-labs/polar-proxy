#!/usr/bin/env bash
# TAP integration test for frontend SSL propagation into backend HBA.
#
# Covers:
#   - an SSL client passes backend hostssl, but a plaintext client does not;
#   - flipping to hostnossl inverts both outcomes.
# The advertised identity is the real frontend TCP peer; the proxy reaches
# the backend via a Unix socket, so its own HBA pass uses a local rule.
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$SCRIPT_DIR/../common/env.sh"
# shellcheck source=../lib/tap_core.sh
source "$SCRIPT_DIR/../lib/tap_core.sh"
# shellcheck source=../lib/tap_polardb.sh
source "$SCRIPT_DIR/../lib/tap_polardb.sh"

PROXYSQL_BINARY="${PROXYSQL_BINARY:-$PROXYSQL_ROOT/src/proxysql}"
PROXYSQL_WRAPPER="${PROXYSQL_WRAPPER:-$SCRIPT_DIR/../common/proxysql_lifecycle.sh}"
PROXYSQL_DATA_DIR="${PROXYSQL_DATA_DIR:-$(polardb_proxy_sharded_data_dir "$POLARDB_RUNTIME_DIR/proxysql_test_polardb_ssl_hba")}"
PROXYSQL_START_LOG="${PROXYSQL_START_LOG:-${PROXYSQL_DATA_DIR}.start.log}"

SSL_HBA_WRITER_HG=45
SSL_HBA_READER_HG=46
SSL_HBA_FLIP_WRITER_HG=47
SSL_HBA_FLIP_READER_HG=48

# Dedicated backend objects so the HBA rewrite and its explicit reject never
# touch the direct-SQL or monitor credentials.
SSL_HBA_USER="polardb_ssl_hba_u"
SSL_HBA_DB="polardb_ssl_hba_d"
SSL_HBA_PASSWORD="polardb_ssl_hba_p"

PLAN=13
FAIL=0
STARTED_PROXY=0
HBA_BACKED_UP=0
HBA_RESTORED=0
BACKEND_ROLE_CREATED=0
BACKEND_DB_CREATED=0
BACKEND_SSL_ENABLED=0
BACKEND_SSL_CRT=""
BACKEND_SSL_KEY=""
HBA_FILE=""
HBA_BAK=""

cleanup() {
	if [ "$HBA_BACKED_UP" -eq 1 ] && [ "$HBA_RESTORED" -eq 0 ]; then
		cp "$HBA_BAK" "$HBA_FILE" >/dev/null 2>&1 || true
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"SELECT pg_reload_conf();" >/dev/null 2>&1 || true
		rm -f "$HBA_BAK" >/dev/null 2>&1 || true
		HBA_RESTORED=1
	fi
	if [ "$BACKEND_SSL_ENABLED" -eq 1 ]; then
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"ALTER SYSTEM RESET ssl_cert_file;" >/dev/null 2>&1 || true
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"ALTER SYSTEM RESET ssl_key_file;" >/dev/null 2>&1 || true
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"ALTER SYSTEM RESET ssl;" >/dev/null 2>&1 || true
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"SELECT pg_reload_conf();" >/dev/null 2>&1 || true
		rm -f "$BACKEND_SSL_CRT" "$BACKEND_SSL_KEY" >/dev/null 2>&1 || true
		BACKEND_SSL_ENABLED=0
	fi
	if [ "$STARTED_PROXY" -eq 1 ]; then
		PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" stop --data-dir "$PROXYSQL_DATA_DIR" >/dev/null 2>&1 || true
		STARTED_PROXY=0
	fi
	if [ "$BACKEND_DB_CREATED" -eq 1 ]; then
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"DROP DATABASE IF EXISTS $SSL_HBA_DB WITH (FORCE);" >/dev/null 2>&1 || true
	fi
	if [ "$BACKEND_ROLE_CREATED" -eq 1 ]; then
		polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
			"DROP ROLE IF EXISTS $SSL_HBA_USER;" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT

plan "$PLAN"

polardb_require_proxysql_or_skip_all polardb
polardb_require_command_or_skip_all psql psql
polardb_require_command_or_skip_all openssl openssl

# The HBA rewrite needs the primary's pg_hba.conf on the local filesystem.
if [ "${POLARDB_DCS_MODE:-}" != "direct" ]; then
	diag "hostssl scenarios require POLARDB_DCS_MODE=direct in the environment"
	polardb_skip_remaining "direct HBA control not configured"
	exit 0
fi

if ! polardb_detect_topology >/dev/null 2>&1; then
	diag "PolarDB topology is not configured/detectable"
	polardb_skip_remaining "PolarDB topology unavailable"
	exit 0
fi

hba_write_stdin() {
	echo "$1" > "$HBA_FILE"
}

# Rebuild pg_hba.conf from the backup with the given first-rule keyword
# (hostssl or hostnossl), reload, and assert the result: the scenario rule
# leads the file as a $first_keyword line and nothing fails to parse.
hba_apply_rules() {
	local first_keyword="$1"
	local original
	local rules
	local applied

	if ! original="$(cat "$HBA_BAK")"; then
		return 1
	fi

	rules="$(
		printf '%s %s %s %s/%s trust\n' "$first_keyword" "$SSL_HBA_DB" "$SSL_HBA_USER" "$CLIENT_SRC" "$CLIENT_SRC_PLEN"
		printf 'local %s %s trust\n' "$SSL_HBA_DB" "$SSL_HBA_USER"
		printf 'host %s %s %s/%s reject\n' "$SSL_HBA_DB" "$SSL_HBA_USER" "$CLIENT_SRC" "$CLIENT_SRC_PLEN"
		printf '%s' "$original"
	)"

	hba_write_stdin "$rules"
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "SELECT pg_reload_conf();" >/dev/null

	applied="$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"SELECT (count(*) FILTER (WHERE error IS NOT NULL) = 0 AND count(*) FILTER (WHERE line_number = 1 AND type = '$first_keyword') = 1)::int FROM pg_hba_file_rules;" |
		tr -d '[:space:]')"
	if [ "$applied" = "1" ]; then
		ok 0 "$first_keyword rules applied and parsed without error"
	else
		diag "$first_keyword rules not applied cleanly: parse errors, or the first pg_hba.conf line is not a $first_keyword rule"
		ok 1 "$first_keyword rules applied and parsed without error"
	fi
}

ssl_hba_client_query() {
	local sslmode="$1"
	PGPASSWORD="$SSL_HBA_PASSWORD" PGSSLMODE="$sslmode" psql \
		-h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" \
		-U "$SSL_HBA_USER" -d "$SSL_HBA_DB" \
		-A -t -q -v ON_ERROR_STOP=1 -c "SELECT 1;" 2>&1
}

_writer_server_online() {
	[ "$(admin_sql "SELECT status FROM stats_pgsql_connection_pool WHERE hostgroup=$1;" 2>/dev/null | tr -d '[:space:]')" = "ONLINE" ]
}

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

if PROXYSQL_BINARY="$PROXYSQL_BINARY" "$PROXYSQL_WRAPPER" start \
	--data-dir "$PROXYSQL_DATA_DIR" \
	--admin-port "$PROXYSQL_ADMIN_PORT" \
	--proxy-port "$PROXYSQL_PORT" \
	--mysql-admin-port "$PROXYSQL_MYSQL_ADMIN_PORT" >"$PROXYSQL_START_LOG" 2>&1; then
	STARTED_PROXY=1
	ok 0 "start ProxySQL for the SSL HBA scenario"
else
	diag "ProxySQL start failed; log follows"
	sed 's/^/# /' "$PROXYSQL_START_LOG" 2>/dev/null || true
	ok 1 "start ProxySQL for the SSL HBA scenario"
	for _ in $(seq 2 "$PLAN"); do ok 1 "dependent check skipped after start failure"; done
	exit 1
fi

set_global_var_runtime "pgsql-polardb_proxy_identity_mode" "client"
set_global_var_runtime "pgsql-polardb_read_target" "primary"

BACKEND_SOCKET_DIR="$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
	"SELECT btrim(split_part(current_setting('unix_socket_directories'), ',', 1));" 2>/dev/null | tr -d '[:space:]')"
if [ -z "$BACKEND_SOCKET_DIR" ] || [ ! -S "$BACKEND_SOCKET_DIR/.s.PGSQL.$PRIMARY_PORT" ]; then
	diag "primary Unix socket unavailable: '$BACKEND_SOCKET_DIR/.s.PGSQL.$PRIMARY_PORT'"
	polardb_skip_remaining "primary Unix socket not accessible in direct mode"
	exit 0
fi

admin_sql "DELETE FROM pgsql_servers WHERE hostgroup_id IN ($SSL_HBA_WRITER_HG,$SSL_HBA_READER_HG,$SSL_HBA_FLIP_WRITER_HG,$SSL_HBA_FLIP_READER_HG);" >/dev/null
admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES ($SSL_HBA_WRITER_HG, '$BACKEND_SOCKET_DIR', $PRIMARY_PORT);" >/dev/null
admin_sql "INSERT INTO pgsql_servers (hostgroup_id, hostname, port) VALUES ($SSL_HBA_FLIP_WRITER_HG, '$BACKEND_SOCKET_DIR', $PRIMARY_PORT);" >/dev/null
admin_sql "DELETE FROM pgsql_replication_hostgroups WHERE writer_hostgroup IN ($SSL_HBA_WRITER_HG,$SSL_HBA_FLIP_WRITER_HG);" >/dev/null
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, proxy_protocol) VALUES ($SSL_HBA_WRITER_HG, $SSL_HBA_READER_HG, 'polardb', 'v15');" >/dev/null
admin_sql "INSERT INTO pgsql_replication_hostgroups (writer_hostgroup, reader_hostgroup, check_type, proxy_protocol) VALUES ($SSL_HBA_FLIP_WRITER_HG, $SSL_HBA_FLIP_READER_HG, 'polardb', 'v15');" >/dev/null
admin_sql "DELETE FROM pgsql_users WHERE username='$SSL_HBA_USER';" >/dev/null
admin_sql "INSERT INTO pgsql_users (username, password, default_hostgroup) VALUES ('$SSL_HBA_USER', '$SSL_HBA_PASSWORD', $SSL_HBA_WRITER_HG);" >/dev/null
admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
admin_sql "LOAD PGSQL SERVERS TO RUNTIME;" >/dev/null
admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null
admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null

if wait_until _writer_server_online "$SSL_HBA_WRITER_HG" -- 20 1; then
	ok 0 "writer server reported ONLINE by the monitor"
else
	diag "writer server in hostgroup $SSL_HBA_WRITER_HG never reached ONLINE"
	polardb_skip_remaining "writer server not ONLINE"
	exit 0
fi

polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP DATABASE IF EXISTS $SSL_HBA_DB WITH (FORCE);" >/dev/null 2>&1 || true
polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" "DROP ROLE IF EXISTS $SSL_HBA_USER;" >/dev/null 2>&1 || true
if polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"CREATE ROLE $SSL_HBA_USER LOGIN PASSWORD '$SSL_HBA_PASSWORD';" >/dev/null 2>&1; then
	BACKEND_ROLE_CREATED=1
	ok 0 "backend scenario role created"
else
	diag "CREATE ROLE failed for $SSL_HBA_USER (direct user lacks CREATEROLE?)"
	polardb_skip_remaining "direct user cannot create the scenario role"
	exit 0
fi
if polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"CREATE DATABASE $SSL_HBA_DB OWNER $SSL_HBA_USER;" >/dev/null 2>&1; then
	BACKEND_DB_CREATED=1
	ok 0 "backend scenario database created"
else
	diag "CREATE DATABASE failed for $SSL_HBA_DB (direct user lacks CREATEDB?)"
	polardb_skip_remaining "direct user cannot create the scenario database"
	exit 0
fi

# The test's TCP peer address at the primary is also its frontend TCP
# peer address at ProxySQL.
CLIENT_SRC="$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
	"SELECT inet_client_addr();" 2>/dev/null | tr -d '[:space:]')"
if [ -n "$CLIENT_SRC" ] &&
		printf '%s' "$CLIENT_SRC" | grep -Eq '^[0-9A-Fa-f:.]+$'; then
	ok 0 "frontend client source address discovered"
else
	diag "unexpected frontend source address: '$CLIENT_SRC'"
	polardb_skip_remaining "frontend source address not resolvable"
	exit 0
fi
CLIENT_SRC_PLEN=32
case "$CLIENT_SRC" in
*:*) CLIENT_SRC_PLEN=128 ;;
esac

HBA_FILE="$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
	"SHOW hba_file;" 2>/dev/null | tr -d '[:space:]')"
HBA_BAK="${HBA_FILE}.polardb_ssl_hba_tap.bak"
if [ -n "$HBA_FILE" ] && [ -f "$HBA_FILE" ] &&
		cp "$HBA_FILE" "$HBA_BAK" 2>/dev/null; then
	HBA_BACKED_UP=1
	ok 0 "primary pg_hba.conf located and backed up"
else
	diag "cannot read or back up '$HBA_FILE' (path from SHOW hba_file)"
	polardb_skip_remaining "primary pg_hba.conf not accessible in direct mode"
	exit 0
fi

# ---------------------------------------------------------------------------
# Backend SSL
# ---------------------------------------------------------------------------

# A hostssl line is rejected at HBA load time while the primary runs with
# ssl=off, so enable SSL for the scenario with a throwaway self-signed
# certificate.
backend_ssl_on() {
	[ "$(polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"SHOW ssl;" 2>/dev/null | tr -d '[:space:]')" = "on" ]
}

backend_ssl_disable() {
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"ALTER SYSTEM RESET ssl_cert_file;" >/dev/null 2>&1 || true
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"ALTER SYSTEM RESET ssl_key_file;" >/dev/null 2>&1 || true
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"ALTER SYSTEM RESET ssl;" >/dev/null 2>&1 || true
	polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
		"SELECT pg_reload_conf();" >/dev/null 2>&1 || true
}

if backend_ssl_on; then
	ok 0 "primary already runs with SSL enabled"
else
	backend_dir="$(dirname "$HBA_FILE")"

	backend_owner="$(stat -c '%U:%G' "$HBA_FILE" 2>/dev/null)"
	BACKEND_SSL_CRT="$backend_dir/polardb_ssl_hba_tap.crt"
	BACKEND_SSL_KEY="$backend_dir/polardb_ssl_hba_tap.key"
	if [ -n "$backend_owner" ] &&
			openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
				-subj /CN=polardb-ssl-hba-tap \
				-keyout "$BACKEND_SSL_KEY" -out "$BACKEND_SSL_CRT" >/dev/null 2>&1 &&
			chown "$backend_owner" "$BACKEND_SSL_CRT" "$BACKEND_SSL_KEY" 2>/dev/null &&
			chmod 600 "$BACKEND_SSL_KEY" &&
			chmod 644 "$BACKEND_SSL_CRT" &&
			polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
				"ALTER SYSTEM SET ssl_cert_file = '$BACKEND_SSL_CRT';" >/dev/null 2>&1 &&
			polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
				"ALTER SYSTEM SET ssl_key_file = '$BACKEND_SSL_KEY';" >/dev/null 2>&1 &&
			polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
				"ALTER SYSTEM SET ssl = on;" >/dev/null 2>&1 &&
			polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
				"SELECT pg_reload_conf();" >/dev/null 2>&1 &&
			wait_until backend_ssl_on -- 10 1; then
		BACKEND_SSL_ENABLED=1
		ok 0 "primary SSL enabled with a throwaway certificate for hostssl rules"
	else
		diag "could not enable SSL on the primary (datadir not writable, or direct user cannot ALTER SYSTEM?)"
		backend_ssl_disable
		rm -f "$BACKEND_SSL_CRT" "$BACKEND_SSL_KEY" >/dev/null 2>&1 || true
		polardb_skip_remaining "cannot enable SSL on the primary"
		exit 0
	fi
fi

# ---------------------------------------------------------------------------
# Scenario: hostssl
# ---------------------------------------------------------------------------

hba_apply_rules hostssl

ssl_out="$(ssl_hba_client_query require)"; ssl_rc=$?
if [ "$ssl_rc" -eq 0 ] && [ "$(printf '%s' "$ssl_out" | tr -d '[:space:]')" = "1" ]; then
	ok 0 "SSL client passes backend hostssl with client identity"
else
	diag "SSL client output: $ssl_out"
	diag "SSL client rc: $ssl_rc"
	if printf '%s' "$ssl_out" |
			grep -q 'unrecognized configuration parameter "_polar_proxy'; then
		polardb_skip_remaining "backend rejects _polar_proxy_* startup keys (PolarDB proxy build required)"
		exit 0
	fi
	ok 1 "SSL client passes backend hostssl with client identity"
fi

plain_out="$(ssl_hba_client_query disable)"; plain_rc=$?
if [ "$plain_rc" -ne 0 ] &&
		printf '%s' "$plain_out" | grep -Eqi 'pg_hba|SSL (off|on)|no entry'; then
	ok 0 "plaintext client is rejected by the backend hostssl rule through the proxy"
else
	diag "plaintext client output: $plain_out"
	diag "plaintext client rc: $plain_rc"
	ok 1 "plaintext client is rejected by the backend hostssl rule through the proxy"
fi

# ---------------------------------------------------------------------------
# Scenario: hostnossl
# ---------------------------------------------------------------------------

# A pooled backend from the successful hostssl phase would bypass the new
# HBA check, so route this phase through a second, cold writer hostgroup.
admin_sql "UPDATE pgsql_users SET default_hostgroup=$SSL_HBA_FLIP_WRITER_HG WHERE username='$SSL_HBA_USER';" >/dev/null
admin_sql "LOAD PGSQL USERS TO RUNTIME;" >/dev/null

hba_apply_rules hostnossl

ssl_out="$(ssl_hba_client_query require)"; ssl_rc=$?
if [ "$ssl_rc" -ne 0 ] &&
		printf '%s' "$ssl_out" | grep -Eqi 'pg_hba|SSL (off|on)|no entry'; then
	ok 0 "SSL client is rejected by backend hostnossl with client identity"
else
	diag "SSL client output: $ssl_out"
	diag "SSL client rc: $ssl_rc"
	ok 1 "SSL client is rejected by backend hostnossl with client identity"
fi

plain_out="$(ssl_hba_client_query disable)"; plain_rc=$?
if [ "$plain_rc" -eq 0 ] &&
		[ "$(printf '%s' "$plain_out" | tr -d '[:space:]')" = "1" ]; then
	ok 0 "plaintext client passes the backend hostnossl rule through the proxy"
else
	diag "plaintext client output: $plain_out"
	diag "plaintext client rc: $plain_rc"
	ok 1 "plaintext client passes the backend hostnossl rule through the proxy"
fi

# ---------------------------------------------------------------------------
# Restore
# ---------------------------------------------------------------------------

cp "$HBA_BAK" "$HBA_FILE" >/dev/null 2>&1
polardb_direct_sql "$PRIMARY_HOST" "$PRIMARY_PORT" \
	"SELECT pg_reload_conf();" >/dev/null 2>&1
rm -f "$HBA_BAK" >/dev/null 2>&1
HBA_RESTORED=1

if [ "$FAIL" -eq 0 ]; then
	exit 0
fi
exit 1
