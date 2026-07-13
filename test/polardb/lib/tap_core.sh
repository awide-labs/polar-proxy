#!/usr/bin/env bash
# Common TAP helpers for PolarDB integration tests.
#
# The test scripts keep scenario-specific logic local, but share the basic TAP
# writer and prerequisite handling here so missing local builds produce clean
# SKIP output instead of noisy shell errors.

if [ -n "${POLARDB_TAP_HELPER_LOADED:-}" ]; then
    return 0
fi
POLARDB_TAP_HELPER_LOADED=1

: "${TEST_NUM:=0}"
: "${FAIL:=0}"

plan() { echo "1..$1"; }
diag() { printf '# %s\n' "$*"; }

ok() {
    local rc="$1"
    local msg="$2"
    TEST_NUM=$((TEST_NUM + 1))
    if [ "$rc" -eq 0 ]; then
        printf 'ok %d - %s\n' "$TEST_NUM" "$msg"
    else
        printf 'not ok %d - %s\n' "$TEST_NUM" "$msg"
        FAIL=$((FAIL + 1))
    fi
}

skip_ok() {
    local msg="$1"
    local reason="$2"
    TEST_NUM=$((TEST_NUM + 1))
    printf 'ok %d - %s # SKIP %s\n' "$TEST_NUM" "$msg" "$reason"
}

polardb_skip_remaining() {
    local reason="$1"
    local total="${2:-${PLAN:-0}}"
    while [ "$TEST_NUM" -lt "$total" ]; do
        skip_ok "prerequisite-dependent check" "$reason"
    done
}

polardb_build_suggestion() {
    local tier="${1:-polardb}"
    printf 'make -C %s %s' "${PROXYSQL_ROOT:-.}" "$tier"
}

polardb_require_command_or_skip_all() {
    local cmd="$1"
    local label="${2:-$cmd}"
    if command -v "$cmd" >/dev/null 2>&1; then
        return 0
    fi
    diag "$label is required for this TAP test but was not found in PATH"
    polardb_skip_remaining "$label missing"
    exit 0
}

polardb_require_proxysql_or_skip_all() {
    local tier="${1:-polardb}"
    local binary="${PROXYSQL_BINARY:-${PROXYSQL_ROOT:-.}/src/proxysql}"
    if [ -x "$binary" ]; then
        return 0
    fi

    diag "ProxySQL binary is not built or not executable: $binary"
    diag "Build it first with: $(polardb_build_suggestion "$tier")"
    diag "Use 'polardb-debug' instead when running tests that assert debug-only traces or fault injection."
    polardb_skip_remaining "ProxySQL binary not built"
    exit 0
}

tap_trace_count() {
    local log_file="$1"
    local pattern="$2"
    local count

    count=$(grep -F -- "$pattern" "$log_file" 2>/dev/null | wc -l | tr -d '[:space:]')
    printf '%s\n' "${count:-0}"
}

tap_trace_has() {
    local log_file="$1"
    local pattern="$2"
    [ "$(tap_trace_count "$log_file" "$pattern")" -gt 0 ]
}

tap_trace_checks_enabled() {
    local log_file="$1"
    [ -f "$log_file" ] && tap_trace_has "$log_file" "PolarDB "
}

tap_trace_expect_count() {
    local log_file="$1"
    local pattern="$2"
    local op="$3"
    local expected="$4"
    local count

    count=$(tap_trace_count "$log_file" "$pattern")
    case "$op" in
    ge)
        if [ "$count" -ge "$expected" ]; then
            return 0
        fi
        diag "trace pattern count too low in $log_file: $pattern (count=$count expected>=$expected)"
        ;;
    eq)
        if [ "$count" -eq "$expected" ]; then
            return 0
        fi
        diag "trace pattern count mismatch in $log_file: $pattern (count=$count expected=$expected)"
        ;;
    *)
        diag "invalid trace count expectation op=$op pattern=$pattern"
        ;;
    esac
    return 1
}

tap_trace_must_have() {
    local log_file="$1"
    local pattern="$2"

    tap_trace_expect_count "$log_file" "$pattern" ge 1
}

tap_trace_must_not_have() {
    local log_file="$1"
    local pattern="$2"

    tap_trace_expect_count "$log_file" "$pattern" eq 0
}

tap_trace_must_count_ge() {
    local log_file="$1"
    local pattern="$2"
    local min_count="$3"

    tap_trace_expect_count "$log_file" "$pattern" ge "$min_count"
}

tap_trace_must_count_eq() {
    local log_file="$1"
    local pattern="$2"
    local expected_count="$3"

    tap_trace_expect_count "$log_file" "$pattern" eq "$expected_count"
}
