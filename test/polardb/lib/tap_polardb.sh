#!/usr/bin/env bash
# Shared PolarDB TAP primitives.
#
# This layer collects the small building blocks that the PolarDB TAP scripts
# (config_roundtrip / lsn_session_consistency / wait_timeout_cleanup) used to
# re-define one-by-one: admin/proxy SQL wrappers, stats-counter readers,
# server-endpoint extraction, the auto/manual SELECT query-rule helpers, the
# debug-fault availability probe, a generic poller, and two higher-level
# write+read+assert helpers.
#
# It does NOT emit TAP itself or change any assertion; lib/tap_core.sh still owns
# plan()/ok()/skip_ok(). These helpers are deliberately thin so each test keeps
# full control over its own plan count and diagnostics.
#
# Contract for callers:
#   - source lib/tap_core.sh first (this file loads common/env.sh itself and
#     relies on ok()/diag());
#   - export the connection variables this file reads (documented per function);
#   - the assert_* helpers call ok() exactly once each.
#
# ProxySQL configuration layers:
#
#   MEMORY (pgsql_servers) -- LOAD ... TO RUNTIME --> RUNTIME
#   RUNTIME                -- SAVE ... FROM RUNTIME -> MEMORY
#   MEMORY                 -- SAVE ... TO DISK ----> DISK
#
# A temporary TAP change normally uses three separate admin requests:
#
#   1. UPDATE the MEMORY table.
#   2. LOAD that module TO RUNTIME.
#   3. Read the runtime_* table when the runtime state is part of the test.
#
# Do not combine UPDATE and LOAD in one admin_sql string. ProxySQL's admin
# SQLite path prepares one statement from that request, so "UPDATE ...; LOAD
# ..." can update MEMORY without changing RUNTIME. SAVE ... FROM RUNTIME is the
# reverse operation: it replaces MEMORY with live state. Use it only before an
# edit when the live state must become the edit base, never after an UPDATE.
# SAVE ... TO DISK is persistence and is not needed by isolated TAP scenarios.
# An OFFLINE_HARD server may be absent from runtime_pgsql_servers; verify that
# it is not ONLINE rather than requiring a literal OFFLINE_HARD runtime row.

if [ -n "${POLARDB_TAP_POLARDB_LOADED:-}" ]; then
    return 0
fi
POLARDB_TAP_POLARDB_LOADED=1

POLARDB_TAP_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common/env.sh
source "$POLARDB_TAP_LIB_DIR/../common/env.sh"

# -----------------------------------------------------------------------------
# Admin / proxy SQL wrappers
# -----------------------------------------------------------------------------

# Run exactly one statement against the ProxySQL PostgreSQL admin interface and
# print the bare result. Uses ON_ERROR_STOP so callers can rely on the exit
# status. Configuration sequences must call admin_sql once per operation.
# Reads: PROXYSQL_HOST, PROXYSQL_ADMIN_PORT, and PROXYSQL_ADMIN_*.
admin_sql() {
    PGPASSWORD="$PROXYSQL_ADMIN_PASSWORD" PGSSLMODE="$PROXYSQL_ADMIN_PGSSLMODE" \
        psql -h "$PROXYSQL_HOST" -p "$PROXYSQL_ADMIN_PORT" \
        -U "$PROXYSQL_ADMIN_USER" -d "$PROXYSQL_ADMIN_DATABASE" \
        -A -t -q -v ON_ERROR_STOP=1 -c "$1"
}

# Run one statement against the ProxySQL PostgreSQL proxy port (the data path).
# Reads: PROXYSQL_HOST, PROXYSQL_PORT, PGUSER, PGDB, PGPASSWORD,
#        PROXYSQL_PGSSLMODE (falls back to PGSSLMODE).
proxy_sql() {
    PGPASSWORD="$PGPASSWORD" PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}" \
        psql -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" -U "$PGUSER" -d "$PGDB" \
        -A -t -q -v ON_ERROR_STOP=1 -c "$1"
}

# Feed a multi-statement script to the proxy port over stdin. When called with a
# single argument the script is taken from "$1"; with no arguments it reads
# stdin (so callers can use a here-doc). Stderr is folded into stdout (2>&1) so
# wrapper warnings/notices are captured alongside results.
# Reads: same variables as proxy_sql().
proxy_script() {
    if [ "$#" -ge 1 ]; then
        printf '%s\n' "$1" | PGPASSWORD="$PGPASSWORD" PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}" \
            psql -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" -U "$PGUSER" -d "$PGDB" \
            -A -t -q 2>&1
    else
        PGPASSWORD="$PGPASSWORD" PGSSLMODE="${PROXYSQL_PGSSLMODE:-$PGSSLMODE}" \
            psql -h "$PROXYSQL_HOST" -p "$PROXYSQL_PORT" -U "$PGUSER" -d "$PGDB" \
            -A -t -q -v ON_ERROR_STOP=1
    fi
}

# -----------------------------------------------------------------------------
# Stats counters (stats_pgsql_global)
# -----------------------------------------------------------------------------

# Print the numeric value of a stats_pgsql_global counter, defaulting to 0 when
# the row is absent or unreadable. Always emits a single trimmed token.
counter() {
    local name="$1"
    local v
    v=$(admin_sql "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='$name';" 2>/dev/null | tr -d '[:space:]')
    printf '%s\n' "${v:-0}"
}

# Print the delta of a counter relative to a previously captured value:
#   before=$(counter NAME); ...; counter_delta NAME "$before"
counter_delta() {
    local name="$1"
    local before="$2"
    local after
    after=$(counter "$name")
    echo $((after - before))
}

# Snapshot the counters most LSN TAP scenarios compare around a protected read.
# The values are stored as <prefix>_prepared, <prefix>_wait,
# <prefix>_bypass, and <prefix>_query_lsn.
snapshot_consistency_counters() {
    local prefix="$1"
    local value

    value=$(counter PolarDB_Wait_Wrap_Prepared)
    printf -v "${prefix}_prepared" '%s' "$value"
    value=$(counter PolarDB_Wait_LSN_Sent)
    printf -v "${prefix}_wait" '%s' "$value"
    value=$(counter PolarDB_Wait_Wrap_Bypassed)
    printf -v "${prefix}_bypass" '%s' "$value"
    value=$(counter PolarDB_Server_LSN_Updates_From_RFQ)
    printf -v "${prefix}_query_lsn" '%s' "$value"
}

snapshot_counter_delta() {
    local before_prefix="$1"
    local after_prefix="$2"
    local field="$3"
    local before_var="${before_prefix}_${field}"
    local after_var="${after_prefix}_${field}"

    echo $((${!after_var} - ${!before_var}))
}

snapshot_protect_delta() {
    local before_prefix="$1"
    local after_prefix="$2"

    echo $(($(snapshot_counter_delta "$before_prefix" "$after_prefix" wait) + $(snapshot_counter_delta "$before_prefix" "$after_prefix" bypass)))
}

snapshot_wrap_or_bypass_delta() {
    local before_prefix="$1"
    local after_prefix="$2"

    echo $(($(snapshot_counter_delta "$before_prefix" "$after_prefix" prepared) + $(snapshot_counter_delta "$before_prefix" "$after_prefix" bypass)))
}

# -----------------------------------------------------------------------------
# Global variable helpers
# -----------------------------------------------------------------------------

global_var() {
    admin_sql "SELECT variable_value FROM global_variables WHERE variable_name='$1';" | tr -d '\r[:space:]'
}

runtime_var() {
    admin_sql "SELECT variable_value FROM runtime_global_variables WHERE variable_name='$1';" | tr -d '\r[:space:]'
}

set_global_var() {
    admin_sql "UPDATE global_variables SET variable_value='$2' WHERE variable_name='$1';" >/dev/null
}

set_global_var_runtime() {
    set_global_var "$1" "$2"
    admin_sql "LOAD PGSQL VARIABLES TO RUNTIME;" >/dev/null
}

# -----------------------------------------------------------------------------
# Server-endpoint extraction
# -----------------------------------------------------------------------------

# Extract a backend "host:port" endpoint from psql output. The PolarDB tests
# print it via `SELECT host(inet_server_addr()) || ':' || inet_server_port()`,
# optionally suffixed with `|data`.
#
# Usage: extract_endpoint <text> [regex] [head|tail]
#   regex  defaults to '^[0-9.]+:[0-9]+$' (bare endpoint line)
#   which  defaults to tail (last matching line); pass head for the first
extract_endpoint() {
    local text="$1"
    local regex="${2:-^[0-9.]+:[0-9]+$}"
    local which="${3:-tail}"
    printf '%s\n' "$text" | grep -E "$regex" | "$which" -1 | tr -d '[:space:]'
}

first_endpoint_from_output() {
    extract_endpoint "$1" '^[0-9.]+:[0-9]+$' head
}

last_endpoint_from_output() {
    extract_endpoint "$1" '^[0-9.]+:[0-9]+$' tail
}

endpoint_list_from_output() {
    printf '%s\n' "$1" | grep -E '^[0-9.]+:[0-9]+$' | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

first_endpoint_payload_from_output() {
    extract_endpoint "$1" '^[0-9.]+:[0-9]+\|' head
}

last_endpoint_payload_from_output() {
    extract_endpoint "$1" '^[0-9.]+:[0-9]+\|' tail
}

# -----------------------------------------------------------------------------
# SELECT query-rule helpers
# -----------------------------------------------------------------------------
# Both helpers replace the whole pgsql_query_rules table with a single ^SELECT
# rule and load it to runtime. The comment string lets each test tag its rule.
# Reads: POLARDB_READER_HG, POLARDB_SELECT_RULE_ID.

# Auto route: replica_eligible=1 lets the PolarDB planner decide writer/reader.
set_select_rule_auto() {
    local comment="${1:-lsn_session_consistency_select}"
    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, replica_eligible, apply, comment) VALUES ($POLARDB_SELECT_RULE_ID, 1, '^SELECT', 1, 0, '$comment');" >/dev/null
    admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

# Manual reader route: send ^SELECT to the reader hostgroup, bypassing the planner.
set_select_rule_manual_reader() {
    local comment="${1:-manual_reader_route}"
    admin_sql "DELETE FROM pgsql_query_rules;" >/dev/null
    admin_sql "INSERT INTO pgsql_query_rules (rule_id, active, match_pattern, destination_hostgroup, apply, comment) VALUES ($POLARDB_SELECT_RULE_ID, 1, '^SELECT', $POLARDB_READER_HG, 1, '$comment');" >/dev/null
    admin_sql "LOAD PGSQL QUERY RULES TO RUNTIME;" >/dev/null
}

# -----------------------------------------------------------------------------
# Debug-fault availability probe
# -----------------------------------------------------------------------------

# Return success when the ProxySQL binary was built with the named debug-fault
# token compiled in (a POLARDB_DEBUG build). Caches the `strings` dump in
# <strings_file> so repeated probes do not re-scan the binary.
#
# Usage: debug_fault_available <token> <strings_file>
# Reads: PROXYSQL_BINARY.
debug_fault_available() {
    local token="$1"
    local strings_file="$2"

    command -v strings >/dev/null 2>&1 || return 1
    if [ ! -s "$strings_file" ]; then
        strings "$PROXYSQL_BINARY" >"$strings_file" 2>/dev/null || true
    fi
    grep -q "$token" "$strings_file"
}

debug_fault_file_available() {
    local file_var="$1"
    local strings_file="$2"
    local path="${!file_var:-}"

    [ -n "$path" ] || return 1
    debug_fault_available "$file_var" "$strings_file"
}

debug_fault_file_ready() {
    local file_var="$1"
    local strings_file="$2"
    local path="${!file_var:-}"

    debug_fault_file_available "$file_var" "$strings_file" || return 1
    : >"$path" || return 1
    [ -f "$path" ] && [ -r "$path" ] && [ -w "$path" ]
}

set_debug_fault_file() {
    local file_var="$1"
    local value="$2"
    local path="${!file_var:-}"

    [ -n "$path" ] || return 1
    printf '%s\n' "$value" >"$path"
}

clear_debug_fault_file() {
    local file_var="$1"
    local path="${!file_var:-}"

    [ -n "$path" ] || return 1
    : >"$path"
}

# -----------------------------------------------------------------------------
# Generic poller
# -----------------------------------------------------------------------------

# Poll a command until it succeeds or a timeout elapses.
#   wait_until <command...> -- <timeout_s> <interval_s>
# The command runs in a subshell each iteration; returns 0 on first success,
# 1 on timeout. timeout_s/interval_s are the last two positional args after the
# literal `--` separator so the command itself may contain arbitrary words.
wait_until() {
    local cmd=()
    while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
        cmd+=("$1")
        shift
    done
    [ "$1" = "--" ] && shift
    local timeout_s="${1:-10}"
    local interval_s="${2:-0.2}"
    local start
    start=$(date +%s)
    while true; do
        if "${cmd[@]}"; then
            return 0
        fi
        [ $(($(date +%s) - start)) -ge "$timeout_s" ] && return 1
        sleep "$interval_s"
    done
}

# Generate small write traffic on a direct writer connection. Tests use this to
# move WAL forward while a replica is intentionally held behind. The caller owns
# the table lifecycle and must call tap_stop_wal_pulse during cleanup.
tap_start_wal_pulse() {
    local host="$1"
    local port="$2"
    local table="$3"
    local max_iters="${4:-200}" # 0 = run until tap_stop_wal_pulse

    (
        local i=0
        while true; do
            polardb_direct_sql "$host" "$port" \
                "INSERT INTO $table VALUES (0, repeat('x', 200)) ON CONFLICT (id) DO UPDATE SET data=repeat('x', 200);" >/dev/null 2>&1
            i=$((i + 1))
            [ "$max_iters" -gt 0 ] && [ "$i" -ge "$max_iters" ] && break
            sleep 0.05
        done
    ) &
    TAP_WAL_PULSE_PID=$!
}

tap_stop_wal_pulse() {
    if [ -n "${TAP_WAL_PULSE_PID:-}" ]; then
        kill "$TAP_WAL_PULSE_PID" 2>/dev/null || true
        wait "$TAP_WAL_PULSE_PID" 2>/dev/null || true
        TAP_WAL_PULSE_PID=""
    fi
}

# -----------------------------------------------------------------------------
# Trace helpers
# -----------------------------------------------------------------------------

trace_file() {
    if [ -n "${POLARDB_TRACE_FILE:-}" ]; then
        printf '%s\n' "$POLARDB_TRACE_FILE"
    elif [ -n "${PROXYSQL_DATA_DIR:-}" ]; then
        printf '%s/proxysql.log\n' "$PROXYSQL_DATA_DIR"
    else
        return 1
    fi
}

_trace_present() {
    local file
    file=$(trace_file)
    [ -n "$file" ] && grep -Eq "$1" "$file" 2>/dev/null
}

wait_for_trace() {
    local pattern="$1"
    local timeout_sec="${2:-10}"
    wait_until _trace_present "$pattern" -- "$timeout_sec" 0.2
}

trace_count() {
    local pattern="$1"
    local file
    file=$(trace_file)
    if [ -z "$file" ]; then
        echo 0
        return 0
    fi

    grep -Ec "$pattern" "$file" 2>/dev/null || true
}

_trace_count_gt() {
    [ "$(trace_count "$1")" -gt "$2" ]
}

wait_for_trace_increment() {
    local pattern="$1"
    local before="$2"
    local timeout_sec="${3:-10}"
    wait_until _trace_count_gt "$pattern" "$before" -- "$timeout_sec" 0.2
}

# -----------------------------------------------------------------------------
# Higher-level write+read+assert helpers
# -----------------------------------------------------------------------------
# These collapse the recurring "run SQL via proxy_script, pull the endpoint,
# check it against the expected backend and the Wait_LSN_Sent delta" pattern.
# Each calls ok() exactly once and emits diagnostics on failure.
#
# WAIT_COUNTER may be overridden by a caller; it defaults to the v1 LSN wait
# counter. Reads: REPLICA_SERVER_ENDPOINT / PRIMARY_SERVER_ENDPOINT through the
# <expected_endpoint> argument.
: "${WAIT_COUNTER:=PolarDB_Wait_LSN_Sent}"

# Assert that running <sql...> routes to <expected_endpoint> and sends at least
# one LSN wait (delta >= 1) -- i.e. a consistency-protected reader read.
#   assert_waited_read <label> <expected_endpoint> <sql>
assert_waited_read() {
    local label="$1"
    local expected="$2"
    local sql="$3"
    local wait_before wait_after out endpoint
    wait_before=$(counter "$WAIT_COUNTER")
    out=$(proxy_script "$sql" 2>&1)
    wait_after=$(counter "$WAIT_COUNTER")
    endpoint=$(extract_endpoint "$out")
    if [ "$endpoint" = "$expected" ] && [ $((wait_after - wait_before)) -ge 1 ]; then
        ok 0 "$label"
    else
        diag "$label output: $out"
        diag "endpoint=$endpoint expected=$expected wait_delta=$((wait_after - wait_before))"
        ok 1 "$label"
    fi
}

# Assert that running <sql...> routes to <expected_endpoint> with NO LSN wait
# (delta == 0) -- i.e. a route that bypasses the wait wrapper.
#   assert_routed_read <label> <expected_endpoint> <sql>
assert_routed_read() {
    local label="$1"
    local expected="$2"
    local sql="$3"
    local wait_before wait_after out endpoint
    wait_before=$(counter "$WAIT_COUNTER")
    out=$(proxy_script "$sql" 2>&1)
    wait_after=$(counter "$WAIT_COUNTER")
    endpoint=$(extract_endpoint "$out")
    if [ "$endpoint" = "$expected" ] && [ "$wait_after" -eq "$wait_before" ]; then
        ok 0 "$label"
    else
        diag "$label output: $out"
        diag "endpoint=$endpoint expected=$expected wait_delta=$((wait_after - wait_before))"
        ok 1 "$label"
    fi
}
