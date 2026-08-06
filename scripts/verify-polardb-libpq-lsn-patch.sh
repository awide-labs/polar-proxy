#!/usr/bin/env bash
#
# verify-polardb-libpq-lsn-patch.sh
#
# Verifies that the installed PolarDB LSN/Xact libpq patch
# (deps/postgresql/polardb_libpq.patch) applies cleanly as the LAST patch
# on top of the vendored PostgreSQL source plus ALL upstream libpq patches.
#
# CONVENTION: our PolarDB patch is always applied LAST, AFTER sslkeylogfile.
# Therefore the baseline = postgres + the 5 standard patches + sslkeylogfile,
# and our PolarDB patch is dry-run-applied on top of that.
#
# Usage:
#   verify-polardb-libpq-lsn-patch.sh [PROXYSQL_TREE_ROOT]
#
#   PROXYSQL_TREE_ROOT defaults to the tree this script lives in (parent of scripts/).
#
# Prints PASS or FAIL and exits non-zero on FAIL.

set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

PG_DIR="$ROOT/deps/postgresql"
PATCH="$PG_DIR/polardb_libpq.patch"

# Upstream libpq patches, in the order they must be applied.
# Our PolarDB patch goes LAST, after sslkeylogfile.
UPSTREAM_PATCHES=(
	get_result_from_pgconn
	handle_row_data
	fmt_err_msg
	bind_fmt_text
	pqsendpipelinesync
	sslkeylogfile
)

fail() { echo "FAIL: $*" >&2; exit 1; }

# --- sanity checks on inputs ---------------------------------------------
[ -f "$PATCH" ] || fail "PolarDB libpq patch not found: $PATCH"

TARBALL="$(ls "$PG_DIR"/postgresql-*.tar.gz 2>/dev/null | head -n 1 || true)"
[ -n "$TARBALL" ] || fail "postgres tarball not found under $PG_DIR"

for p in "${UPSTREAM_PATCHES[@]}"; do
	[ -f "$PG_DIR/$p.patch" ] || fail "upstream patch missing: $PG_DIR/$p.patch"
done

# --- scratch dir ----------------------------------------------------------
TMP="$(mktemp -d "${TMPDIR:-/tmp}/polardb-lsn-verify.XXXXXX")"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo "Extracting $(basename "$TARBALL") ..."
tar xzf "$TARBALL" -C "$TMP"

# locate extracted source root (contains src/interfaces/libpq)
PG_SRC="$(find "$TMP" -maxdepth 5 -type d -path '*/src/interfaces/libpq' -printf '%p\n' \
	| sed 's:/src/interfaces/libpq$::' | head -n 1)"
[ -n "$PG_SRC" ] && [ -d "$PG_SRC/src/interfaces/libpq" ] \
	|| fail "could not locate extracted postgres source root"

# --- apply the 6 upstream patches in order --------------------------------
for p in "${UPSTREAM_PATCHES[@]}"; do
	echo "Applying upstream patch: $p"
	if ! patch -p0 -d "$PG_SRC" < "$PG_DIR/$p.patch" >/dev/null 2>&1; then
		fail "upstream patch did not apply: $p"
	fi
done

# Remove .orig residue left by upstream patches (e.g. sslkeylogfile applies
# with fuzz/offset against the reordered libpq-fe.h). This isolates whether
# OUR patch applies cleanly.
find "$PG_SRC" -name '*.orig' -delete 2>/dev/null || true

# --- dry-run apply OUR PolarDB patch LAST ---------------------------------
echo "Dry-run applying PolarDB libpq patch (must be clean, no fuzz/offset) ..."
DRY_OUT="$(patch -p0 --dry-run -d "$PG_SRC" < "$PATCH" 2>&1)" || {
	echo "$DRY_OUT" >&2
	fail "PolarDB libpq patch dry-run failed"
}

# Reject any fuzz or offset in the dry-run output.
if echo "$DRY_OUT" | grep -qiE 'fuzz|offset|FAILED|hunk'; then
	echo "$DRY_OUT" >&2
	fail "PolarDB libpq patch applied with fuzz/offset/failure"
fi

# --- real apply to confirm and ensure no .orig from our patch -------------
echo "Real-apply PolarDB libpq patch to confirm (no .orig expected) ..."
patch -p0 -d "$PG_SRC" < "$PATCH" >/dev/null 2>&1 \
	|| fail "PolarDB libpq patch real-apply failed"

ORIG="$(find "$PG_SRC" -name '*.orig' 2>/dev/null || true)"
[ -z "$ORIG" ] || fail "PolarDB libpq patch produced .orig (fuzz): $ORIG"

# --- public ABI and W wire-layout contract --------------------------------
EXPECTED_EXPORTS=(
	"PQgetLSN 188"
	"PQhasLSN 189"
	"PQsetPolarSendLSN 190"
	"PQgetXactSplitXids 191"
	"PQisXactSplittable 192"
	"PQisXactWalPending 193"
	"PQsetPolarSendXact 194"
	"PSpeekRowRun 195"
	"PSrowRunPending 196"
	"PSadvanceInput 197"
	"PSdetachRowRun 198"
	"PQsendQueryParamsPolarWait 199"
	"PQsendPreparePolarWait 200"
	"PQsendQueryPreparedPolarWait 201"
)

for entry in "${EXPECTED_EXPORTS[@]}"; do
	read -r symbol ordinal <<< "$entry"
	awk -v symbol="$symbol" -v ordinal="$ordinal" \
		'$1 == symbol && $2 == ordinal { found = 1 } END { exit !found }' \
		"$PG_SRC/src/interfaces/libpq/exports.txt" \
		|| fail "libpq export mismatch: expected $symbol at ordinal $ordinal"
	grep -Fq "$symbol" "$PG_SRC/src/interfaces/libpq/libpq-fe.h" \
		|| fail "libpq public header missing expected symbol: $symbol"
done

FE_EXEC="$PG_SRC/src/interfaces/libpq/fe-exec.c"
grep -Fq '#define POLAR_WAIT_PROTOCOL_VERSION 1' "$FE_EXEC" \
	|| fail "W protocol version is not fixed at 1"
grep -Fq '#define POLAR_WAIT_PROTOCOL_FLAGS 0' "$FE_EXEC" \
	|| fail "W protocol flags are not fixed at 0"

# Payload order is part of the client/server contract:
# version:u8, mode:u8, flags:u16, timeout_ms:u32, target_lsn:u64.
WIRE_FIELDS=(
	"pqPutc(POLAR_WAIT_PROTOCOL_VERSION, conn)"
	"pqPutc(wait_spec->consistency_mode, conn)"
	"pqPutInt(POLAR_WAIT_PROTOCOL_FLAGS, 2, conn)"
	"pqPutInt((int) wait_spec->timeout_ms, 4, conn)"
	"pqPutInt((int) (wait_spec->target_lsn >> 32), 4, conn)"
	"pqPutInt((int) wait_spec->target_lsn, 4, conn)"
)

previous_line=0
for field in "${WIRE_FIELDS[@]}"; do
	line="$(grep -nF "$field" "$FE_EXEC" | head -n 1 | cut -d: -f1)"
	[ -n "$line" ] || fail "W encoder missing field: $field"
	[ "$line" -gt "$previous_line" ] \
		|| fail "W encoder field is out of order: $field"
	previous_line="$line"
done

grep -Fq "pqPutMsgStart('W', conn)" "$FE_EXEC" \
	|| fail "libpq does not encode the W frontend message"
grep -Fq 'pqPutMsgEndNoFlush(conn)' "$FE_EXEC" \
	|| fail "W is not staged for the compound command flush"

# --- belt-and-suspenders: xact is expected, CSN is still deferred ----------
if grep -qiE 'csn|PQgetCSN|PQhasCSN|PQsetPolarSendCSN|_polar_send_csn|_polar_proxy_send_csn|polar_proxy_send_csn|polar_last_csn|polar_has_csn' "$PATCH"; then
	fail "PolarDB libpq patch references deferred CSN tokens"
fi

REQUIRED_PATCH_TOKENS=(
	PQgetLSN
	PQhasLSN
	PQsetPolarSendLSN
	PQPolarConsistencyMode
	PQsendQueryParamsPolarWait
	PQsendPreparePolarWait
	PQsendQueryPreparedPolarWait
	pqPutPolarWait
	_pq_.polar_proxy_wait_v1
	PQgetXactSplitXids
	PQisXactSplittable
	PQisXactWalPending
	PQsetPolarSendXact
	_polar_send_xact
	_polar_proxy_send_xact
)

for token in "${REQUIRED_PATCH_TOKENS[@]}"; do
	grep -Fq "$token" "$PATCH" || fail "PolarDB libpq patch missing expected token: $token"
done

grep -Fq "pqPutMsgStart('W', conn)" "$PATCH" \
	|| fail "PolarDB libpq patch does not encode the W frontend message"

echo "PASS: PolarDB LSN/Xact/Wait libpq patch applies cleanly as the LAST patch"
exit 0
