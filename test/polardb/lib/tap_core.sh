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
