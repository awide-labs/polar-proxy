#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROXYSQL_ROOT=$(cd "$SCRIPT_DIR/../../.." && pwd)

PATCH_FILE="$PROXYSQL_ROOT/test/polardb/tools/patches/pgbench-polar-lsn-native.patch"
PG_SRC_LINK="$PROXYSQL_ROOT/deps/postgresql/postgresql"
PG_SRC_REAL=$(readlink -f "$PG_SRC_LINK")

OUT=${1:-"$PROXYSQL_ROOT/test/polardb/bin/pgbench-polar-rfq"}
BUILD_ROOT="$PROXYSQL_ROOT/test/polardb/bin/pgbench-polar-rfq-build"
BUILD_PG_ROOT="$BUILD_ROOT/deps/postgresql/postgresql"
BUILD_PGBENCH_DIR="$BUILD_PG_ROOT/src/bin/pgbench"
BUILD_LIBPQ_DIR="$BUILD_PG_ROOT/src/interfaces/libpq"
BUILD_PGCOMMON_A="$BUILD_PG_ROOT/src/common/libpgcommon.a"
BUILD_PGPORT_A="$BUILD_PG_ROOT/src/port/libpgport.a"
BUILD_LIBPQ_A="$BUILD_LIBPQ_DIR/libpq.a"

if [[ ! -f "$PATCH_FILE" ]]; then
  echo "missing patch: $PATCH_FILE" >&2
  exit 1
fi

if [[ ! -f "$PG_SRC_REAL/src/Makefile.global" ]]; then
  echo "vendored PostgreSQL tree is not configured: $PG_SRC_REAL" >&2
  echo "run: make -C $PROXYSQL_ROOT polardb-libpq" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"
rm -rf "$BUILD_ROOT"
mkdir -p "$BUILD_ROOT/deps/postgresql"

cp -a "$PG_SRC_REAL" "$BUILD_PG_ROOT"
patch -d "$BUILD_ROOT" -p1 < "$PATCH_FILE"

make -C "$BUILD_PGBENCH_DIR" clean MAKELEVEL=0
make -C "$BUILD_PGBENCH_DIR" \
  CC="${CC:-cc}" \
  CXX="${CXX:-c++}" \
  MAKELEVEL=0 \
  enable_rpath=no \
  libpq_pgport="$BUILD_PGCOMMON_A $BUILD_PGPORT_A $BUILD_LIBPQ_A" \
  LDFLAGS_EX="${LDFLAGS_EX:-}"

cp "$BUILD_PGBENCH_DIR/pgbench" "$OUT"
chmod +x "$OUT"

"$OUT" --version
