#!/usr/bin/env bash
#
# verify-polardb-libpq-lsn-patch.sh
#
# Verifies that the installed LSN-only PolarDB libpq patch
# (deps/postgresql/polardb_libpq.patch) applies cleanly as the LAST patch
# on top of the vendored PostgreSQL source plus ALL upstream libpq patches.
#
# CONVENTION: our PolarDB patch is always applied LAST, AFTER sslkeylogfile.
# Therefore the baseline = postgres + the 5 standard patches + sslkeylogfile,
# and our LSN-only patch is dry-run-applied on top of that.
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
# Our LSN-only patch goes LAST, after sslkeylogfile.
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
[ -f "$PATCH" ] || fail "LSN-only patch not found: $PATCH"

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

# --- dry-run apply OUR LSN-only patch LAST --------------------------------
echo "Dry-run applying LSN-only patch (must be clean, no fuzz/offset) ..."
DRY_OUT="$(patch -p0 --dry-run -d "$PG_SRC" < "$PATCH" 2>&1)" || {
	echo "$DRY_OUT" >&2
	fail "LSN-only patch dry-run failed"
}

# Reject any fuzz or offset in the dry-run output.
if echo "$DRY_OUT" | grep -qiE 'fuzz|offset|FAILED|hunk'; then
	echo "$DRY_OUT" >&2
	fail "LSN-only patch applied with fuzz/offset/failure"
fi

# --- real apply to confirm and ensure no .orig from our patch -------------
echo "Real-apply LSN-only patch to confirm (no .orig expected) ..."
patch -p0 -d "$PG_SRC" < "$PATCH" >/dev/null 2>&1 \
	|| fail "LSN-only patch real-apply failed"

ORIG="$(find "$PG_SRC" -name '*.orig' 2>/dev/null || true)"
[ -z "$ORIG" ] || fail "LSN-only patch produced .orig (fuzz): $ORIG"

# --- belt-and-suspenders: the patch must be CSN/Xact free -----------------
if grep -qiE 'csn|xact|xid|splittable|wal_pending' "$PATCH"; then
	# allow upstream 'xactStatus'/'PQTRANS' context lines and substrings
	# like 'exactly'; flag only real CSN/Xact tokens
	if grep -iE 'csn|xact|xid|splittable|wal_pending' "$PATCH" \
		| grep -vqiE 'xactStatus|PQTRANS|exactly|next message'; then
		fail "LSN-only patch still references CSN/Xact tokens"
	fi
fi

echo "PASS: LSN-only PolarDB libpq patch applies cleanly as the LAST patch"
exit 0
