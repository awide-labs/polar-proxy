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
POLARDB_C_HELPERS := \
	$(POLARDB_HELPER_BIN_DIR)/libpq_lsn_test \
	$(POLARDB_HELPER_BIN_DIR)/proxysql_extended_protocol_test

LDIRS :=	-L$(POLARDB_LIBPQ_DIR) -L$(POLARDB_PGCOMMON_DIR) -L$(POLARDB_PGPORT_DIR)
RPATHS :=	-Wl,-rpath,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_PGCOMMON_DIR) -Wl,-rpath-link,$(POLARDB_PGPORT_DIR)
POLARDB_PGBENCH_LDFLAGS := -Wl,-rpath,$(POLARDB_LIBPQ_DIR) -Wl,-rpath-link,$(POLARDB_LIBPQ_DIR)

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
	polardb-run-c-libpq-lsn polardb-run-c-extended-protocol \
	polardb-tap-config polardb-tap-lsn polardb-tap-wait-timeout polardb-tap \
	polardb-bench polardb-analyze-traces

all: sqlite_history_convert $(POLARDB_TESTS)

polardb: $(POLARDB_TESTS)
ifeq ($(POLARDB_PROXY),0)
	@echo "PolarDB test helpers disabled because POLARDB_PROXY=0"
endif

check-polardb-libpq:
ifeq ($(POLARDB_PROXY),1)
	@test -f "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored PostgreSQL source is not extracted/built. Run: make -C .. polardb-libpq"; exit 1; }
	@grep -q 'PQgetLSN' "$(POLARDB_LIBPQ_HEADER)" || { echo "Vendored libpq headers are not PolarDB-patched. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_LIBPQ_SO)" || { echo "Missing vendored libpq shared library. Run: make -C .. polardb-libpq"; exit 1; }
	@test -f "$(POLARDB_POSTGRESQL_SRC)/Makefile.global" || { echo "Vendored PostgreSQL tree is not configured. Run: make -C .. polardb-libpq"; exit 1; }
	@nm -D "$(POLARDB_LIBPQ_SO)" 2>/dev/null | grep -q 'PQgetLSN' || { echo "Vendored libpq library is not PolarDB-patched. Run: make -C .. polardb-libpq"; exit 1; }
endif

clean-polardb:
	rm -f $(POLARDB_C_HELPERS)
	+@test ! -d "$(POLARDB_PGBENCH_DIR)" || $(MAKE) -C "$(POLARDB_PGBENCH_DIR)" clean MAKELEVEL=0

sqlite_history_convert: sqlite_history_convert.cpp
	g++ -ggdb ../lib/SpookyV2.cpp ../lib/debug.cpp ../deps/sqlite3/sqlite3/sqlite3.o sqlite_history_convert.cpp ../lib/sqlite3db.cpp -o sqlite_history_convert $(IDIRS) -pthread -ldl

$(POLARDB_PGBENCH): check-polardb-libpq
	+$(MAKE) -C "$(POLARDB_PGBENCH_DIR)" CC=${CC} CXX=${CXX} MAKELEVEL=0 LDFLAGS_EX="$(POLARDB_PGBENCH_LDFLAGS) $${LDFLAGS_EX:-}"

$(POLARDB_HELPER_BIN_DIR):
	mkdir -p $(POLARDB_HELPER_BIN_DIR)

$(POLARDB_HELPER_BIN_DIR)/libpq_lsn_test: check-polardb-libpq polardb/test-c/libpq_lsn_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/libpq_lsn_test.c -o $@ $(IDIRS) $(LDIRS) $(RPATHS) -lpq -lpgcommon -lpgport -lpthread -ldl

$(POLARDB_HELPER_BIN_DIR)/proxysql_extended_protocol_test: check-polardb-libpq polardb/test-c/proxysql_extended_protocol_test.c | $(POLARDB_HELPER_BIN_DIR)
	cc -ggdb polardb/test-c/proxysql_extended_protocol_test.c -o $@ $(IDIRS) $(LDIRS) $(RPATHS) -lpq -lpgcommon -lpgport -lpthread -ldl

polardb-run-c-libpq-lsn:
	+$(MAKE) -C polardb run-c-libpq-lsn

polardb-run-c-extended-protocol:
	+$(MAKE) -C polardb run-c-extended-protocol QUERY="$(QUERY)"

polardb-tap-config:
	+$(MAKE) -C polardb tap-config

polardb-tap-lsn:
	+$(MAKE) -C polardb tap-lsn

polardb-tap-wait-timeout:
	+$(MAKE) -C polardb tap-wait-timeout

polardb-tap:
	+$(MAKE) -C polardb tap

polardb-bench:
	+$(MAKE) -C polardb bench

polardb-analyze-traces:
	+$(MAKE) -C polardb trace-analyze ARGS="$(ARGS)"
