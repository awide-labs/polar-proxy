#!/bin/make -f
#
# PolarDB-owned unit tests.
#
# These tests intentionally live outside the generic test/tap/tests/unit
# registry so the PolarDB test target can evolve its own coverage condition without
# adding product-specific cases to the shared upstream unit suite.

PROXYSQL_PATH := $(shell while [ ! -f ./src/proxysql_global.cpp ]; do cd ..; done; pwd)

include $(PROXYSQL_PATH)/include/makefiles_vars.mk
include $(PROXYSQL_PATH)/include/makefiles_paths.mk

POLARDB_PROXY ?= 1
POLARDB_PROFILE ?= 0
POLARDB_PERF_DEBUG ?= 0
PYTHON ?= python3

# Build products are kept out of this source directory:
#   BINDIR (../bin)   compiled unit-test binaries
#   BUILDDIR (../build) object files + gcov intermediates (.gcno/.gcda/.gcov)
# ODIR is the object dir for the shared test-helper objects and tap.o; it lives
# under BUILDDIR so a single `clean` of bin/ + build/ wipes everything.
BINDIR := ../bin
BUILDDIR := ../build
# These options change public counter layouts. Keep their unit objects
# separate so mode switches cannot link stale objects.
ODIR := $(BUILDDIR)/profile-$(POLARDB_PROFILE)-perf-$(POLARDB_PERF_DEBUG)
TEST_HELPERS_DIR := $(PROXYSQL_PATH)/test/tap/test_helpers
PROXYSQL_TEST_HEADERS := $(wildcard $(PROXYSQL_PATH)/include/*.h) $(wildcard $(PROXYSQL_PATH)/include/*.hpp)

IDIRS :=	-I$(TAP_IDIR) \
			-I$(RE2_IDIR) \
			-I$(PROXYSQL_IDIR) \
			-I$(JEMALLOC_IDIR) \
			-I$(LIBCONFIG_IDIR) \
			-I$(MARIADB_IDIR) \
			-I$(LIBDAEMON_IDIR) \
			-I$(MICROHTTPD_IDIR) \
			-I$(LIBHTTPSERVER_IDIR) \
			-I$(CURL_IDIR) -I$(EV_IDIR) \
			-I$(PROMETHEUS_IDIR) \
			-I$(DOTENV_DYN_IDIR) \
			-I$(SQLITE3_IDIR) \
			-I$(JSON_IDIR) \
			-I$(POSTGRESQL_IDIR) \
			-I$(LIBSCRAM_IDIR) \
			-I$(LIBUSUAL_IDIR) \
			-I$(SSL_IDIR) \
			-I$(ZSTD_IDIR) \
			-I$(PARSERSQL_IDIR) \
			-I$(PROXYSQL_PATH)/deps/lz4/lz4/lib \
			-I$(PROXYSQL_PATH)/include \
			-I$(PROXYSQL_PATH)/test/tap/test_helpers

LDIRS :=	-L$(TAP_LDIR) \
			-L$(RE2_LDIR) \
			-L$(PROXYSQL_LDIR) \
			-L$(JEMALLOC_LDIR) \
			-L$(LIBCONFIG_LDIR) \
			-L$(MARIADB_LDIR) \
			-L$(LIBDAEMON_LDIR) \
			-L$(MICROHTTPD_LDIR) \
			-L$(LIBHTTPSERVER_LDIR) \
			-L$(CURL_LDIR) -L$(EV_LDIR) \
			-L$(PROMETHEUS_LDIR) \
			-L$(DOTENV_DYN_LDIR) \
			-L$(PCRE_LDIR) \
			-L$(LIBINJECTION_LDIR) \
			-L$(POSTGRESQL_LDIR) \
			-L$(LIBSCRAM_LDIR) \
			-L$(LIBUSUAL_LDIR) \
			-L$(SSL_LDIR) \
			-L$(PARSERSQL_LDIR)

ifeq ($(UNAME_S),Linux)
	LDIRS += -L$(COREDUMPER_LDIR)
endif

CLICKHOUSE_CPP_PATH := $(DEPS_PATH)/clickhouse-cpp/clickhouse-cpp
CLICKHOUSE_CPP_IDIR := $(CLICKHOUSE_CPP_PATH) -I$(CLICKHOUSE_CPP_PATH)/contrib/absl
CLICKHOUSE_CPP_LDIR := $(CLICKHOUSE_CPP_PATH)/clickhouse
LZ4_LDIR := $(DEPS_PATH)/lz4/lz4/lib

IDIRS += -I$(CLICKHOUSE_CPP_IDIR)

LIBPROXYSQLAR := $(PROXYSQL_LDIR)/libproxysql.a
STATIC_LIBS := $(CITYHASH_LDIR)/libcityhash.a \
			   $(LZ4_LDIR)/liblz4.a \
			   $(ZSTD_LDIR)/libzstd.a

ifeq ($(PROXYSQLCLICKHOUSE),1)
ifneq ($(wildcard $(CLICKHOUSE_CPP_LDIR)/libclickhouse-cpp-lib.a),)
	STATIC_LIBS += $(CLICKHOUSE_CPP_LDIR)/libclickhouse-cpp-lib.a \
				   $(LZ4_LDIR)/liblz4.a $(ZSTD_LDIR)/libzstd.a
endif
endif

ifeq ($(UNAME_S),Linux)
	STATIC_LIBS += $(COREDUMPER_LDIR)/libcoredumper.a
endif

ifeq ($(UNAME_S),Darwin)
	MYLIBS := -lssl -lcrypto -lpthread -lm -lz -liconv -lgnutls \
		  -lprometheus-cpp-pull -lprometheus-cpp-core -luuid -lzstd $(LWGCOV)
	POLARDB_UNIT_LIBPROXYSQL := $(LIBPROXYSQLAR)
else
	MYLIBS := -Wl,--export-dynamic -Wl,-Bdynamic -lgnutls -lcurl -lssl -lcrypto -luuid \
		  -Wl,-Bstatic -lconfig -lsqlparser -ldaemon -lconfig++ -lre2 -lpcrecpp -lpcre \
		  -lmariadbclient -lhttpserver -lmicrohttpd -linjection -lev \
		  -lprometheus-cpp-pull -lprometheus-cpp-core \
		  -Wl,-Bstatic -lpq -lpgcommon -lpgport \
		  -Wl,-Bdynamic -lpthread -lm -lz -lzstd -lrt -ldl \
		  -lscram -lusual -Wl,--allow-multiple-definition \
		  $(LWGCOV)
	POLARDB_UNIT_LIBPROXYSQL := $(LIBPROXYSQLAR)
endif

ifneq ($(NOJEMALLOC),1)
ifeq ($(UNAME_S),Linux)
	MYLIBS += -Wl,-Bstatic -ljemalloc -Wl,-Bdynamic
endif
endif

# ProxySQL component tests link libproxysql.a and construct real components.
# Keep the test support DEBUG flag aligned with the archive, otherwise component
# constructors reject the mixed debug/non-debug process.
POLARDB_UNIT_DEBUG_FLAG :=
ifneq ($(shell nm $(LIBPROXYSQLAR) 2>/dev/null | grep -cw 'init_debug_struct'),0)
	POLARDB_UNIT_DEBUG_FLAG := -DDEBUG
endif
POLARDB_PROFILE ?= 0
POLARDB_UNIT_PROFILE_FLAG :=
ifeq ($(POLARDB_PROFILE),1)
	POLARDB_UNIT_PROFILE_FLAG := -DPOLARDB_PROFILE=1
endif
POLARDB_UNIT_PERF_DEBUG_FLAG :=
ifeq ($(POLARDB_PERF_DEBUG),1)
	POLARDB_UNIT_PERF_DEBUG_FLAG := -DPOLARDB_PERF_DEBUG=1
endif

# Unit objects are compiled into BUILDDIR, so gcov runtime data follows the
# matching .gcno files there without an explicit -fprofile-dir override.

# Match the product PolarDB tier: POLARDB_PROXY=1 builds without ClickHouse.
OPT := $(STDCPP) -O0 -ggdb -DPOLARDB_PROXY=1 $(POLARDB_UNIT_PROFILE_FLAG) $(POLARDB_UNIT_PERF_DEBUG_FLAG) $(POLARDB_UNIT_DEBUG_FLAG) \
	   -DGITVERSION=\"$(GIT_VERSION)\" -DMYSQLX_TEST_BUILD $(NOJEM) \
	   $(WGCOV) $(WASAN) -Wl,--no-as-needed -Wl,-rpath,$(TAP_LDIR)

TEST_HELPERS_OBJ := $(ODIR)/test_globals.o $(ODIR)/test_init.o $(ODIR)/tap.o

# Focused tests link tap.o without libproxysql.a. Each binary covers one
# self-contained PolarDB domain.
POLARDB_FOCUSED_TESTS := \
	polardb_protocol_parse_unit-t \
	polardb_status_policy_unit-t \
	polardb_query_state_unit-t \
	polardb_startup_profile_unit-t \
	pgsql_status_variables_unit-t

# Component tests link libproxysql.a and the shared TAP initialization helpers.
POLARDB_COMPONENT_TESTS := \
	polardb_routing_lsn_unit-t \
	polardb_client_rfq_unit-t \
	polardb_query_digest_unit-t \
	polardb_hgm_lsn_unit-t

POLARDB_HGM_TEST_SOURCES := \
	polardb_hgm_lsn_unit-t.cpp \
	polardb_unit_support.cpp \
	polardb_session_unit_access.cpp \
	polardb_reader_selection_tests.cpp \
	polardb_reader_pool_tests.cpp \
	polardb_reader_capacity_tests.cpp \
	polardb_split_warmup_tests.cpp \
	polardb_thread_io_tests.cpp \
	polardb_topology_tests.cpp \
	polardb_consistency_tests.cpp
POLARDB_HGM_TEST_OBJECTS := \
	$(addprefix $(ODIR)/,$(POLARDB_HGM_TEST_SOURCES:.cpp=.o))
POLARDB_SINGLE_SOURCE_COMPONENT_TESTS := \
	$(filter-out polardb_hgm_lsn_unit-t,$(POLARDB_COMPONENT_TESTS))

POLARDB_UNIT_TESTS := \
	$(POLARDB_FOCUSED_TESTS) \
	$(POLARDB_COMPONENT_TESTS)

# Binaries land in BINDIR; the bare names above are the run-loop handles.
POLARDB_UNIT_BINARIES := $(addprefix $(BINDIR)/,$(POLARDB_UNIT_TESTS))
POLARDB_PYTHON_TESTS := ../tools/tests/coverage_summary_unit.py

# The local tree may hold gcov-instrumented objects after
# `make -C test/polardb coverage`. Link libgcov into unit binaries instead of
# requiring a clean rebuild before switching back to a normal unit-test build.
POLARDB_UNIT_GCOV_LINK ?= -lgcov --coverage

.PHONY: all check clean $(POLARDB_UNIT_TESTS)
all: $(POLARDB_UNIT_BINARIES)

check: all
	@set -e; \
	for test_binary in $(POLARDB_UNIT_BINARIES); do \
		echo "==> $$test_binary"; \
		$$test_binary; \
	done
	$(PYTHON) $(POLARDB_PYTHON_TESTS)

# Convenience aliases: `make polardb_routing_lsn_unit-t` builds the bin/ target.
$(POLARDB_UNIT_TESTS): %: $(BINDIR)/%

clean:
	rm -rf $(BUILDDIR) $(POLARDB_UNIT_BINARIES)

$(BINDIR):
	mkdir -p $(BINDIR)

$(ODIR):
	mkdir -p $(ODIR)

$(ODIR)/tap.o: $(TAP_PATH)/tap.cpp | $(ODIR)
	$(CXX) -c -o $@ $< $(OPT) $(IDIRS) -w

$(ODIR)/tap_noise_stubs.o: | $(ODIR)
	@printf '#include <vector>\n#include <string>\n#include <mutex>\nstd::vector<std::string> noise_failures;\nstd::mutex noise_failure_mutex;\nextern "C" { void stop_noise_tools(){} int get_noise_tools_count(){return 0;} }\n' | \
		$(CXX) -x c++ -c -o $@ - $(STDCPP)

$(ODIR)/test_globals.o: $(TEST_HELPERS_DIR)/test_globals.cpp $(PROXYSQL_TEST_HEADERS) | $(ODIR)
	$(CXX) -c -o $@ $< $(OPT) $(IDIRS) -I$(PROXYSQL_PATH)/test -Wall

$(ODIR)/test_init.o: $(TEST_HELPERS_DIR)/test_init.cpp $(PROXYSQL_TEST_HEADERS) | $(ODIR)
	$(CXX) -c -o $@ $< $(OPT) $(IDIRS) -Wall

.PHONY: FORCE
FORCE:

$(LIBPROXYSQLAR): FORCE
	$(MAKE) -B -C $(PROXYSQL_PATH)/lib libproxysql.a \
			PROXYSQLCLICKHOUSE=0 \
			POLARDB_PROXY=1 \
			POLARDB_PROFILE=$(POLARDB_PROFILE) \
			DEBUG="$(POLARDB_UNIT_DEBUG_FLAG)" \
			PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) \
			PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) CC=$(CC) CXX=$(CXX)

# Focused test object (compiled into BUILDDIR so its .gcno/.gcda intermediates
# stay out of the source tree) and its link step (binary into BINDIR).
$(ODIR)/%.o: %.cpp polardb_unit_common.h | $(ODIR)
	$(CXX) -c $(abspath $<) -I$(TAP_IDIR) -I$(PROXYSQL_PATH)/include \
		-DPOLARDB_PROXY=1 $(POLARDB_UNIT_PROFILE_FLAG) $(POLARDB_UNIT_PERF_DEBUG_FLAG) $(STDCPP) -O0 -ggdb $(WGCOV) $(WASAN) -o $@

$(ODIR)/pgsql_status_variables_unit-t.o: pgsql_status_variables_unit-t.cpp $(PROXYSQL_TEST_HEADERS) | $(ODIR)
	$(CXX) -c $(abspath $<) $(IDIRS) \
		-DPOLARDB_PROXY=1 $(POLARDB_UNIT_PROFILE_FLAG) $(POLARDB_UNIT_PERF_DEBUG_FLAG) $(STDCPP) -O0 -ggdb $(WGCOV) $(WASAN) -o $@

$(addprefix $(BINDIR)/,$(POLARDB_FOCUSED_TESTS)): $(BINDIR)/%: $(ODIR)/%.o $(ODIR)/tap.o $(ODIR)/tap_noise_stubs.o FORCE | $(BINDIR)
	$(CXX) $(filter-out FORCE,$^) \
		-DPOLARDB_PROXY=1 $(POLARDB_UNIT_PROFILE_FLAG) $(POLARDB_UNIT_PERF_DEBUG_FLAG) $(STDCPP) -O0 -ggdb $(WGCOV) $(LWGCOV) $(WASAN) \
		-lpthread $(POLARDB_UNIT_GCOV_LINK) -o $@

# Component tests link libproxysql.a and the TAP initialization helpers and are
# built in two steps so their build products stay out of the source dir and BINDIR:
#   1. compile into BUILDDIR so .o/.gcno/.gcda land in build.
#   2. link the object into BINDIR.
$(addprefix $(ODIR)/,$(addsuffix .o,$(POLARDB_SINGLE_SOURCE_COMPONENT_TESTS))): $(ODIR)/%.o: %.cpp polardb_unit_common.h $(PROXYSQL_TEST_HEADERS) | $(ODIR)
	$(CXX) -c $(abspath $<) $(IDIRS) $(OPT) -o $@

$(addprefix $(BINDIR)/,$(POLARDB_SINGLE_SOURCE_COMPONENT_TESTS)): $(BINDIR)/%: $(ODIR)/%.o $(TEST_HELPERS_OBJ) $(LIBPROXYSQLAR) FORCE | $(BINDIR)
	$(CXX) $(ODIR)/$*.o $(TEST_HELPERS_OBJ) $(LDIRS) $(OPT) \
		$(POLARDB_UNIT_LIBPROXYSQL) $(STATIC_LIBS) $(MYLIBS) $(POLARDB_UNIT_GCOV_LINK) \
		-Wl,--allow-multiple-definition -o $@

$(POLARDB_HGM_TEST_OBJECTS): $(ODIR)/%.o: %.cpp polardb_unit_common.h polardb_unit_support.h polardb_unit_domains.h $(PROXYSQL_TEST_HEADERS) | $(ODIR)
	$(CXX) -c $(abspath $<) $(IDIRS) $(OPT) -o $@

$(BINDIR)/polardb_hgm_lsn_unit-t: $(POLARDB_HGM_TEST_OBJECTS) $(TEST_HELPERS_OBJ) $(LIBPROXYSQLAR) FORCE | $(BINDIR)
	$(CXX) $(POLARDB_HGM_TEST_OBJECTS) $(TEST_HELPERS_OBJ) $(LDIRS) $(OPT) \
		$(POLARDB_UNIT_LIBPROXYSQL) $(STATIC_LIBS) $(MYLIBS) $(POLARDB_UNIT_GCOV_LINK) \
		-Wl,--allow-multiple-definition -o $@
