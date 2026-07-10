#!/usr/bin/env bash
#
# Regenerate deps/postgresql/polardb_libpq.patch from the current expanded
# vendored PostgreSQL source tree.
#
# Usage:
#   scripts/regenerate-polardb-libpq-patch.sh [--verify] [PROXYSQL_TREE_ROOT]
#
# The script builds the baseline from postgresql-*.tar.gz plus the standard
# upstream libpq patches, then diffs that baseline against
# deps/postgresql/postgresql for the libpq files owned by the PolarDB patch.

set -euo pipefail

VERIFY=0
if [ "${1:-}" = "--verify" ]; then
	VERIFY=1
	shift
fi

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PG_DIR="$ROOT/deps/postgresql"
GENERATED_SRC="$PG_DIR/postgresql"
PATCH="$PG_DIR/polardb_libpq.patch"

UPSTREAM_PATCHES=(
	get_result_from_pgconn
	handle_row_data
	fmt_err_msg
	bind_fmt_text
	pqsendpipelinesync
	sslkeylogfile
)

POLARDB_LIBPQ_FILES=(
	src/interfaces/libpq/exports.txt
	src/interfaces/libpq/fe-connect.c
	src/interfaces/libpq/fe-exec.c
	src/interfaces/libpq/fe-protocol3.c
	src/interfaces/libpq/libpq-fe.h
	src/interfaces/libpq/libpq-int.h
)

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

[ -d "$GENERATED_SRC/src/interfaces/libpq" ] ||
	fail "expanded PostgreSQL source not found: $GENERATED_SRC"

TARBALL="$(ls "$PG_DIR"/postgresql-*.tar.gz 2>/dev/null | head -n 1 || true)"
[ -n "$TARBALL" ] || fail "postgres tarball not found under $PG_DIR"

for p in "${UPSTREAM_PATCHES[@]}"; do
	[ -f "$PG_DIR/$p.patch" ] || fail "upstream patch missing: $PG_DIR/$p.patch"
done

for f in "${POLARDB_LIBPQ_FILES[@]}"; do
	[ -f "$GENERATED_SRC/$f" ] || fail "generated source file missing: $GENERATED_SRC/$f"
done

TMP="$(mktemp -d "${TMPDIR:-/tmp}/polardb-libpq-regenerate.XXXXXX")"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

tar xzf "$TARBALL" -C "$TMP"
BASE_SRC="$(find "$TMP" -maxdepth 5 -type d -path '*/src/interfaces/libpq' -printf '%p\n' |
	sed 's:/src/interfaces/libpq$::' | head -n 1)"
[ -n "$BASE_SRC" ] && [ -d "$BASE_SRC/src/interfaces/libpq" ] ||
	fail "could not locate extracted postgres source root"

for p in "${UPSTREAM_PATCHES[@]}"; do
	patch -p0 -d "$BASE_SRC" <"$PG_DIR/$p.patch" >/dev/null
done

find "$BASE_SRC" -name '*.orig' -delete 2>/dev/null || true

TMP_PATCH="$TMP/polardb_libpq.patch"
: >"$TMP_PATCH"

for f in "${POLARDB_LIBPQ_FILES[@]}"; do
	set +e
	diff -u --label "$f" --label "$f" "$BASE_SRC/$f" "$GENERATED_SRC/$f" >>"$TMP_PATCH"
	rc=$?
	set -e
	[ "$rc" -le 1 ] || fail "diff failed for $f"
done

mv "$TMP_PATCH" "$PATCH"
echo "Regenerated $PATCH"

if [ "$VERIFY" = "1" ]; then
	"$ROOT/scripts/verify-polardb-libpq-lsn-patch.sh" "$ROOT"
fi
