#!/bin/make -f


PROXYSQL_PATH := $(shell while [ ! -f ./src/proxysql_global.cpp ]; do cd ..; done; pwd)

include $(PROXYSQL_PATH)/include/makefiles_vars.mk
include $(PROXYSQL_PATH)/include/makefiles_paths.mk


IDIRS :=	-I$(PROXYSQL_IDIR) \
			-I$(POSTGRESQL_IDIR) \
			-I$(JEMALLOC_IDIR) \
			-I$(MARIADB_IDIR) \
			-I$(LIBCONFIG_IDIR) \
			-I$(RE2_IDIR) \
			-I$(SQLITE3_IDIR) \
			-I$(PCRE_IDIR) \
			-I$(SYS_LOC_IDIR) \
			-I$(CLICKHOUSE_CPP_IDIR) \
			-I$(MICROHTTPD_IDIR) \
			-I$(LIBHTTPSERVER_IDIR) \
			-I$(LIBINJECTION_IDIR) \
			-I$(CURL_IDIR) \
			-I$(EV_IDIR) \
			-I$(JSON_IDIR) \
			-I$(SSL_IDIR)


POLARDB_POSTGRESQL_SRC := $(PROXYSQL_PATH)/deps/postgresql/postgresql/src
POLARDB_LIBPQ_DIR := $(POLARDB_POSTGRESQL_SRC)/interfaces/libpq
POLARDB_PGCOMMON_DIR := $(POLARDB_POSTGRESQL_SRC)/common
POLARDB_PGPORT_DIR := $(POLARDB_POSTGRESQL_SRC)/port
POLARDB_PGBENCH_DIR := $(POLARDB_POSTGRESQL_SRC)/bin/pgbench
POLARDB_PGBENCH := $(POLARDB_PGBENCH_DIR)/pgbench
POLARDB_HELPER_BIN_DIR := polardb/bin
POLARDB_PGBENCH_POLAR_RFQ := $(POLARDB_HELPER_BIN_DIR)/pgbench-polar-rfq
POLARDB_SYSBENCH_POLAR_RFQ := $(POLARDB_HELPER_BIN_DIR)/sysbench-polar-rfq
POLARDB_SYSBENCH_POLAR_RFQ_DIR := $(POLARDB_HELPER_BIN_DIR)/sysbench-polar-rfq.d
POLARDB_SYSBENCH_POLAR_RFQ_SCRIPT := $(POLARDB_SYSBENCH_POLAR_RFQ_DIR)/sysbench_lsn_consistency.lua
POLARDB_SYSBENCH_POLAR_RFQ_LUA_DIR := $(POLARDB_SYSBENCH_POLAR_RFQ_DIR)/lua
POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR := ../deps/sysbench-polar-rfq
DEPS_SYSBENCH_POLAR_RFQ := $(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)/src/sysbench
POLARDB_C_HELPERS := \
	$(POLARDB_HELPER_BIN_DIR)/libpq_lsn_test \
	$(POLARDB_HELPER_BIN_DIR)/libpq_xact_test \
	$(POLARDB_HELPER_BIN_DIR)/libpq_row_run_test \
	$(POLARDB_HELPER_BIN_DIR)/proxysql_extended_protocol_test \
	$(POLARDB_HELPER_BIN_DIR)/polardb_lsn_trace.so

LDIRS :=	-L$(POLARDB_LIBPQ_DIR) -L$(POLARDB_PGCOMMON_DIR) -L$(POLARDB_PGPORT_DIR) -L$(SSL_LDIR)
RPATHS :=	-Wl,-rpath,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_PGCOMMON_DIR) -Wl,-rpath-link,$(POLARDB_PGPORT_DIR)
POLARDB_PGBENCH_LDFLAGS := -Wl,-rpath,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_LIBPQ_DIR)
POLARDB_LIBPQ_A := $(POLARDB_LIBPQ_DIR)/libpq.a
POLARDB_PGCOMMON_A := $(POLARDB_PGCOMMON_DIR)/libpgcommon.a
POLARDB_PGPORT_A := $(POLARDB_PGPORT_DIR)/libpgport.a
POLARDB_HELPER_LIBS := -lpq -lpgcommon -lpgport -lssl -lcrypto -lpthread -lm -ldl
POLARDB_SYSBENCH_PGSQL_LIBS := $(POLARDB_LIBPQ_A) $(POLARDB_PGCOMMON_A) $(POLARDB_PGPORT_A) -lssl -lcrypto -lz -lm

# PolarDB LSN test helpers. They need the LSN-patched libpq built by the
# POLARDB_PROXY=1 tier. Integration TAP scripts call these helpers explicitly.
POLARDB_PROXY ?= 1

ifeq ($(POLARDB_PROXY),1)
POLARDB_TESTS := $(POLARDB_C_HELPERS) $(POLARDB_PGBENCH)
else
POLARDB_TESTS :=
endif

POLARDB_LIBPQ_HEADER := $(POLARDB_LIBPQ_DIR)/libpq-fe.h
POLARDB_LIBPQ_SO := $(POLARDB_LIBPQ_DIR)/libpq.so

.PHONY: all polardb clean-polardb check-polardb-libpq \
	polardb-run-c-libpq-lsn polardb-run-c-libpq-xact polardb-run-c-libpq-row-run polardb-run-c-extended-protocol \
	polardb-tap-config polardb-tap-lsn polardb-tap-split polardb-tap-wait-timeout polardb-tap \
	polardb-bench polardb-analyze-traces polardb-pgbench-polar-rfq polardb-sysbench-polar-rfq \
	polardb-rfq-bench-tools-check

all: sqlite_history_convert $(POLARDB_TESTS)

polardb: $(POLARDB_TESTS)
ifeq ($(POLARDB_PROXY),0)
	@echo "PolarDB test helpers disabled because POLARDB_PROXY=0"
endif

check-polardb-libpq:
ifeq ($(POLARDB_PROXY),1)
	@test -f "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored PostgreSQL source is not extracted/built. Run: make -C .. polardb-libpq"; exit 1; }
	@grep -q 'PQgetLSN' "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored libpq headers are not PolarDB-patched. Run: make -C .. polardb-libpq"; exit 1; }
	@grep -q 'PQgetXactSplitXids' "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored libpq headers are missing PolarDB xact RFQ support. Run: make -C .. polardb-libpq"; exit 1; }
	@grep -q 'PSpeekRowRun' "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored libpq headers are missing PolarDB row-run support. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_LIBPQ_SO)" || { echo "Missing vendored libpq shared library. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_POSTGRESQL_SRC)/Makefile.global" || { echo "Vendored PostgreSQL tree is not configured. Run: make -C .. polardb-libpq"; exit 1; }
	@nm -D "$(POLARDB_LIBPQ_SO)" 2>/dev/null | grep -q 'PQgetLSN' || { echo "Vendored libpq library is not PolarDB-patched. Run: make -C .. polardb-libpq"; exit 1; }
	@nm -D "$(POLARDB_LIBPQ_SO)" 2>/dev/null | grep -q 'PQgetXactSplitXids' || { echo "Vendored libpq library is missing PolarDB xact RFQ support. Run: make -C .. polardb-libpq"; exit 1; }
	@nm -D "$(POLARDB_LIBPQ_SO)" 2>/dev/null | grep -q 'PSpeekRowRun' || { echo "Vendored libpq shared library is missing PolarDB row-run support. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_LIBPQ_A)" || { echo "Missing vendored libpq static archive. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_PGCOMMON_A)" || { echo "Missing PostgreSQL common static archive. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_PGPORT_A)" || { echo "Missing PostgreSQL port static archive. Run: make -C .. polardb-libpq"; exit 1; }
	@nm "$(POLARDB_LIBPQ_A)" 2>/dev/null | grep -q 'PQgetLSN' || { echo "Vendored libpq static archive is not PolarDB-patched. Run: make -C .. polardb-libpq"; exit 1; }
	@nm "$(POLARDB_LIBPQ_A)" 2>/dev/null | grep -q 'PSpeekRowRun' || { echo "Vendored libpq static archive is missing PolarDB row-run support. Run: make -C .. polardb-libpq"; exit 1; }
endif

clean-polardb:
	rm -f $(POLARDB_C_HELPERS)
	rm -f $(POLARDB_PGBENCH_POLAR_RFQ)
	rm -f $(POLARDB_SYSBENCH_POLAR_RFQ)
	rm -rf $(POLARDB_SYSBENCH_POLAR_RFQ_DIR)
	rm -rf $(POLARDB_PGBENCH_POLAR_RFQ)-build
	+@test ! -d "$(POLARDB_PGBENCH_DIR)" || $(MAKE) -C "$(POLARDB_PGBENCH_DIR)" clean MAKELEVEL=0

sqlite_history_convert: sqlite_history_convert.cpp
	g++ -ggdb ../lib/SpookyV2.cpp ../lib/debug.cpp ../deps/sqlite3/sqlite3/sqlite3.o sqlite_history_convert.cpp ../lib/sqlite3db.cpp -o sqlite_history_convert $(IDIRS) -pthread -ldl

$(POLARDB_PGBENCH): check-polardb-libpq
	+$(MAKE) -C "$(POLARDB_PGBENCH_DIR)" CC=${CC} CXX=${CXX} MAKELEVEL=0 LDFLAGS_EX="$(POLARDB_PGBENCH_LDFLAGS) $${LDFLAGS_EX:-}"

$(POLARDB_PGBENCH_POLAR_RFQ): check-polardb-libpq polardb/tools/build_pgbench_polar_rfq.sh polardb/tools/patches/pgbench-polar-lsn-native.patch | $(POLARDB_HELPER_BIN_DIR)
	+polardb/tools/build_pgbench_polar_rfq.sh "$@"

polardb-pgbench-polar-rfq: $(POLARDB_PGBENCH_POLAR_RFQ)
	@"$(POLARDB_PGBENCH_POLAR_RFQ)" --version | grep -q 'polar-rfq-lsn' || { echo "pgbench RFQ marker is missing"; exit 1; }
	@nm "$(POLARDB_PGBENCH_POLAR_RFQ)" 2>/dev/null | grep -q ' PQgetLSN$$' || { echo "pgbench does not contain PQgetLSN"; exit 1; }

$(DEPS_SYSBENCH_POLAR_RFQ): check-polardb-libpq
	@test -d "$(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)" || { echo "Missing sysbench RFQ-LSN submodule: $(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)"; exit 1; }
	cd "$(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)" && ./autogen.sh
	cd "$(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)" && \
		CPPFLAGS="-I$(POLARDB_LIBPQ_DIR) -I$(POLARDB_POSTGRESQL_SRC)/include $${CPPFLAGS:-}" \
		LDFLAGS="-L$(POLARDB_LIBPQ_DIR) $${LDFLAGS:-}" \
		./configure --without-mysql --with-pgsql \
			--with-pgsql-includes="$(POLARDB_LIBPQ_DIR)" \
			--with-pgsql-libs="$(POLARDB_LIBPQ_DIR)"
	+cd "$(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)" && CC=${CC} CXX=${CXX} $(MAKE) \
		PGSQL_LIBS="$(POLARDB_SYSBENCH_PGSQL_LIBS)"

$(POLARDB_SYSBENCH_POLAR_RFQ): $(DEPS_SYSBENCH_POLAR_RFQ) | $(POLARDB_HELPER_BIN_DIR)
	cp "$<" "$@"

$(POLARDB_SYSBENCH_POLAR_RFQ_SCRIPT): polardb/tools/sysbench_lsn_consistency.lua | $(POLARDB_SYSBENCH_POLAR_RFQ_DIR)
	cp "$<" "$@"

$(POLARDB_SYSBENCH_POLAR_RFQ_LUA_DIR): $(DEPS_SYSBENCH_POLAR_RFQ) | $(POLARDB_SYSBENCH_POLAR_RFQ_DIR)
	rm -rf "$@"
	mkdir -p "$@"
	cp $(POLARDB_SYSBENCH_POLAR_RFQ_SRC_DIR)/src/lua/*.lua "$@"/

$(POLARDB_SYSBENCH_POLAR_RFQ_DIR):
	mkdir -p "$@"

polardb-sysbench-polar-rfq: $(POLARDB_SYSBENCH_POLAR_RFQ) $(POLARDB_SYSBENCH_POLAR_RFQ_SCRIPT) $(POLARDB_SYSBENCH_POLAR_RFQ_LUA_DIR)
	@nm -D "$(POLARDB_SYSBENCH_POLAR_RFQ)" 2>/dev/null | grep -q ' db_polar_has_lsn$$' || { echo "sysbench does not export db_polar_has_lsn"; exit 1; }
	@strings "$(POLARDB_SYSBENCH_POLAR_RFQ)" | grep -q 'primary_lsn_verdict' || { echo "sysbench does not contain primary-LSN reporting"; exit 1; }
	@test -r "$(POLARDB_SYSBENCH_POLAR_RFQ_SCRIPT)" || { echo "sysbench RFQ workload is missing"; exit 1; }
	@test -r "$(POLARDB_SYSBENCH_POLAR_RFQ_LUA_DIR)/oltp_read_write.lua" || { echo "sysbench Lua files are missing"; exit 1; }

polardb-rfq-bench-tools-check: check-polardb-libpq polardb-pgbench-polar-rfq polardb-sysbench-polar-rfq
	@echo "PolarDB RFQ benchmark tools passed"

$(POLARDB_HELPER_BIN_DIR):
	mkdir -p $(POLARDB_HELPER_BIN_DIR)

$(POLARDB_HELPER_BIN_DIR)/libpq_lsn_test: check-polardb-libpq polardb/test-c/libpq_lsn_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/libpq_lsn_test.c -o $@ $(IDIRS) $(LDIRS) $(RPATHS) $(POLARDB_HELPER_LIBS)

$(POLARDB_HELPER_BIN_DIR)/libpq_xact_test: check-polardb-libpq polardb/test-c/libpq_xact_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/libpq_xact_test.c -o $@ $(IDIRS) $(LDIRS) $(RPATHS) $(POLARDB_HELPER_LIBS)

$(POLARDB_HELPER_BIN_DIR)/libpq_row_run_test: check-polardb-libpq polardb/test-c/libpq_row_run_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/libpq_row_run_test.c -o $@ $(IDIRS) -I$(POLARDB_LIBPQ_DIR) $(LDIRS) $(RPATHS) $(POLARDB_HELPER_LIBS)

$(POLARDB_HELPER_BIN_DIR)/proxysql_extended_protocol_test: check-polardb-libpq polardb/test-c/proxysql_extended_protocol_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/proxysql_extended_protocol_test.c -o $@ $(IDIRS) $(LDIRS) $(RPATHS) $(POLARDB_HELPER_LIBS)

$(POLARDB_HELPER_BIN_DIR)/polardb_lsn_trace.so: check-polardb-libpq polardb/test-c/polardb_lsn_trace.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb -fPIC -shared polardb/test-c/polardb_lsn_trace.c -o $@ $(IDIRS) -lpthread -ldl

polardb-run-c-libpq-lsn:
	+$(MAKE) -C polardb run-c-libpq-lsn

polardb-run-c-libpq-xact:
	+$(MAKE) -C polardb run-c-libpq-xact

polardb-run-c-libpq-row-run:
	+$(MAKE) -C polardb run-c-libpq-row-run

polardb-run-c-extended-protocol:
	+$(MAKE) -C polardb run-c-extended-protocol QUERY="$(QUERY)"

polardb-tap-config:
	+$(MAKE) -C polardb tap-config

polardb-tap-lsn:
	+$(MAKE) -C polardb tap-lsn

polardb-tap-split:
	+$(MAKE) -C polardb tap-split

polardb-tap-wait-timeout:
	+$(MAKE) -C polardb tap-wait-timeout

polardb-tap:
	+$(MAKE) -C polardb tap

polardb-bench:
	+$(MAKE) -C polardb bench

polardb-analyze-traces:
	+$(MAKE) -C polardb trace-analyze ARGS="$(ARGS)"
