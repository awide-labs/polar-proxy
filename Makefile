#!/bin/make -f


### NOTES:
### version string is fetched from git history
### when not available, specify GIT_VERSION_BASE during make:
###
### ```
### make GIT_VERSION_BASE="v3.x.y"
### ```

GIT_VERSION_BASE := $(shell git describe --long --abbrev=7 2>/dev/null || git describe --long --abbrev=7 --always)
ifndef GIT_VERSION_BASE
    $(error GIT_VERSION_BASE is not set)
endif

.PHONY: lint lint-generate-cdb lint-run lint-tests

lint-generate-cdb:
	@echo "Generating compile_commands.json (requires bear)"
	./scripts/lint/generate-compile-commands.sh

lint-run:
	@echo "Running local linters"
	./scripts/lint/run-local.sh

lint: lint-generate-cdb lint-run
	@echo "Done lint"

.PHONY: lint-tests
lint-tests:
	@echo "Running TAP test static analysis"
	./scripts/lint/run_tap_tests.py $(FILES)


### RELEASE TIERS & FEATURE FLAGS:
### ProxySQL supports three distinct release tiers built from the same codebase.
### The tier is controlled by environment variables which enable feature guards
### and dynamically adjust the version number to maintain a clear upgrade path.
###
### 1. ProxySQL v3.0.x (Stable Tier)
###    - The default build.
###    - Includes bug fixes and core enhancements.
###    - No extra flags required.
###
### 2. ProxySQL v3.1.x (Innovative Tier)
###    - Enabled by setting `PROXYSQL31=1`.
###    - Includes v3.0 features plus:
###      * FFTO (Fast Forward Traffic Observer)
###      * TSDB (Time Series Database subsystem)
###    - Automatically increments the minor version (e.g., 3.0.6 -> 3.1.6).
###
### 3. ProxySQL v4.0.x (Plugin Chassis Tier)
###    - Enabled by setting `PROXYSQL40=1`.
###    - Includes v3.1 features plus:
###      * Four-phase plugin lifecycle (register_schemas + init split)
###      * Pre-execution query-hook plugin ABI
###      * Shared Prometheus registry access for plugins
###      * Generic admin-command alias dispatch
###      * GenAI plugin (built alongside core)
###    - Automatically increments the major version (e.g., 3.0.6 -> 4.0.6).
###
### HIERARCHY: `PROXYSQL40=1` implies `PROXYSQL31=1` implies `PROXYSQLFFTO=1` + `PROXYSQLTSDB=1`.

# If PROXYSQL40 is enabled, it automatically enables PROXYSQL31
ifeq ($(PROXYSQL40),1)
    PROXYSQL31 := 1
endif

# If PROXYSQL31 is enabled, it automatically enables FFTO and TSDB
ifeq ($(PROXYSQL31),1)
    PROXYSQLFFTO := 1
    PROXYSQLTSDB := 1
endif

# Only increment version at the top-level make to avoid double-incrementing in recursive makes
GIT_VERSION ?= $(GIT_VERSION_BASE)
ifeq ($(MAKELEVEL),0)
# Normalize GIT_VERSION by stripping leading 'v' for arithmetic
GIT_VERSION_NORM := $(shell echo "$(GIT_VERSION_BASE)" | sed 's/^v//')
# If PROXYSQL40 is enabled, increment the major version number by 1
ifeq ($(PROXYSQL40),1)
	GIT_VERSION := $(shell echo "$(GIT_VERSION_NORM)" | awk -F. '{printf "%d.%s", $$1+1, substr($$0, length($$1)+2)}')
else
# If PROXYSQL31 is enabled, increment the minor version number by 1
ifeq ($(PROXYSQL31),1)
	GIT_VERSION := $(shell echo "$(GIT_VERSION_NORM)" | awk -F. '{printf "%s.%d.%s", $$1, $$2+1, substr($$0, length($$1)+length($$2)+3)}')
endif
endif
endif

export GIT_VERSION

# Extract CURVER from GIT_VERSION (first 3 numbers, e.g., 3.0.6 from 3.0.6-388-ga94b7d6)
CURVER := $(shell echo "$(GIT_VERSION)" | sed -nE 's/^v?([0-9]+\.[0-9]+\.[0-9]+).*/\1/p' | head -1)

# Validate CURVER has 3 numbers separated by dots
CURVER_CHECK := $(shell echo "$(CURVER)" | grep -cE '^[0-9]+\.[0-9]+\.[0-9]+$$')

ifeq ($(CURVER_CHECK),0)
    $(error CURVER "$(CURVER)" derived from GIT_VERSION "$(GIT_VERSION)" does not have 3 numbers separated by dots. Expected format: X.Y.Z)
endif

export CURVER
export PROXYSQL40
export PROXYSQL31
export PROXYSQLFFTO
export PROXYSQLTSDB

### NOTES:
### SOURCE_DATE_EPOCH is used for reproducible builds
### for details consult https://reproducible-builds.org/docs/source-date-epoch/

SOURCE_DATE_EPOCH ?= $(shell git show -s --format=%ct HEAD || date +%s)
export SOURCE_DATE_EPOCH

### NOTES:
### to compile without jemalloc, set environment variable NOJEMALLOC=1
### to compile with gcov code coverage, set environment variable WITHGCOV=1
### to compile with ASAN, set environment variables NOJEMALLOC=1, WITHASAN=1:
###   * To perform a full ProxySQL build with ASAN then execute:
###
###     ```
###     make build_deps_debug -j$(nproc) && make debug -j$(nproc) && make build_tap_test_debug -j$(nproc)
###     ```
###
### ** to use on-demand coredump generation feature, compile code without ASAN option (WITHASAN=0).
###
### NOTES for Valgrind:
### When running Valgrind, SQLite's internal memory allocator may cause false
### positives in pcache*, memjrnl*, and sqlite3Btree* functions. To avoid this,
### rebuild SQLite with -USQLITE_ENABLE_MEMORY_MANAGEMENT in deps/Makefile

O0 := -O0
O2 := -O2 -fno-omit-frame-pointer
O1 := -O1
O3 := -O3 -mtune=native

#EXTRALINK := #-pg
ALL_DEBUG := $(O0) -ggdb -DDEBUG
NO_DEBUG := $(O2) -ggdb
DEBUG := $(ALL_DEBUG)
#export DEBUG
#export EXTRALINK
export MAKE

# PolarDB proxy support. Default ON; build with POLARDB_PROXY=0 to compile the
# PolarDB compilation units to no-op stubs (no behavior change vs upstream).
POLARDB_PROXY ?= 1
# Optional PolarDB profiling counters. Default OFF so production builds avoid
# per-query timing probes; build with POLARDB_PROFILE=1 when collecting latency
# phase breakdowns.
POLARDB_PROFILE ?= 0
# Optional PolarDB diagnostic counters for write/output shape and other
# benchmark-only details. Default OFF; build with POLARDB_PERF_DEBUG=1 when
# measuring where output batching can help.
POLARDB_PERF_DEBUG ?= 0
# PolarDB builds do not use ClickHouse. Force it off here so recursive make
# calls cannot accidentally mix ClickHouse-enabled and disabled objects.
ifeq ($(POLARDB_PROXY),1)
override PROXYSQLCLICKHOUSE := 0
else
PROXYSQLCLICKHOUSE ?= 1
endif

### detect compiler support for c++17 (required)
CPLUSPLUS := $(shell ${CC} -std=c++17 -dM -E -x c++ /dev/null 2>/dev/null | grep -F __cplusplus | egrep -o '[0-9]{6}L')
ifneq ($(CPLUSPLUS),201703L)
    $(error Compiler must support at least c++17)
endif
STDCPP := -std=c++17 -DCXX17

### detect distro
DISTRO := Unknown
ifneq (,$(wildcard /etc/os-release))
	DISTRO := $(shell awk -F= '/^NAME/{print $$2}' /etc/os-release)
endif

### multiprocessing
NPROCS := 1
OS := $(shell uname -s)
UNAME_S := $(OS)
ifeq ($(OS),Linux)
	NPROCS := $(shell nproc)
endif
ifneq (,$(findstring $(OS),Darwin FreeBSD))
	NPROCS := $(shell sysctl -n hw.ncpu)
	LEGACY_BUILD := 1
    CC ?= cc
    CXX ?= c++
    export CC
    export CXX
endif
export MAKEOPT := -j${NPROCS}

### systemd
SYSTEMD := 0
ifeq ($(wildcard /usr/lib/systemd/system), /usr/lib/systemd/system)
	SYSTEMD := 1
endif

### check user/group
USERCHECK :=
GROUPCHECK :=
ifeq ($(OS),Linux)
USERCHECK := $(shell getent passwd proxysql)
GROUPCHECK := $(shell getent group proxysql)
endif


### main targets

.DEFAULT_GOAL := default
.PHONY: default
default: build_src

.PHONY: debug
debug: build_src_debug

.PHONY: testaurora_random
testaurora_random: build_src_testaurora_random

.PHONY: testaurora
testaurora: build_src_testaurora build_cluster_simulator
	# cd test/tap && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA" CC=${CC} CXX=${CXX} ${MAKE}
	# cd test/tap/tests && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA" CC=${CC} CXX=${CXX} ${MAKE} $(MAKECMDGOALS)

.PHONY: testgalera
testgalera: build_src_testgalera build_cluster_simulator
	cd test/tap && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GALERA" CC=${CC} CXX=${CXX} ${MAKE}
	cd test/tap/tests && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GALERA" CC=${CC} CXX=${CXX} ${MAKE} $(MAKECMDGOALS)

.PHONY: testgrouprep
testgrouprep: build_src_testgrouprep build_cluster_simulator

.PHONY: testreadonly
testreadonly: build_src_testreadonly build_cluster_simulator

.PHONY: testreplicationlag
testreplicationlag: build_src_testreplicationlag build_cluster_simulator

.PHONY: testall
testall: build_src_testall

.PHONY: clickhouse
clickhouse: build_src_clickhouse

.PHONY: debug_clickhouse
debug_clickhouse: build_src_debug_clickhouse


### helper targets

.PHONY: build_deps
build_deps: $(if $(LEGACY_BUILD),build_deps_legacy,build_deps_default)

.PHONY: build_lib
build_lib: $(if $(LEGACY_BUILD),build_lib_legacy,build_lib_default)

.PHONY: build_src
build_src: $(if $(LEGACY_BUILD),build_src_legacy,build_src_default)

.PHONY: build_deps_debug
build_deps_debug: $(if $(LEGACY_BUILD),build_deps_debug_legacy,build_deps_debug_default)

.PHONY: build_lib_debug
build_lib_debug: $(if $(LEGACY_BUILD),build_lib_debug_legacy,build_lib_debug_default)

.PHONY: build_src_debug
build_src_debug: $(if $(LEGACY_BUILD),build_src_debug_legacy,build_src_debug_default)

# RAG ingester (PoC)
.PHONY: rag_ingest
rag_ingest: build_deps
	cd RAG_POC && ${MAKE} CC=${CC} CXX=${CXX} CXXFLAGS="${CXXFLAGS}"

.PHONY: rag_ingest_clean
rag_ingest_clean:
	cd RAG_POC && ${MAKE} clean

# legacy build targets (pre c++17)
.PHONY: build_deps_legacy
build_deps_legacy:
	cd deps && OPTZ="${O2} -ggdb" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_legacy
build_lib_legacy: build_deps_legacy
	cd lib && OPTZ="${O2} -ggdb" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_legacy
build_src_legacy: build_lib_legacy
	cd src && OPTZ="${O2} -ggdb" CC=${CC} CXX=${CXX} ${MAKE}
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/mysqlx && OPTZ="${O2} -ggdb" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] mysqlx plugin (PROXYSQL40 not set)")
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/genai && OPTZ="${O2} -ggdb" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] genai plugin (PROXYSQL40 not set)")

.PHONY: build_deps_debug_legacy
build_deps_debug_legacy:
	cd deps && OPTZ="${O0} -ggdb -DDEBUG" PROXYDEBUG=1 CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_debug_legacy
build_lib_debug_legacy: build_deps_debug_legacy
	cd lib && OPTZ="${O0} -ggdb -DDEBUG" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_debug_legacy
build_src_debug_legacy: build_lib_debug_legacy
	cd src && OPTZ="${O0} -ggdb -DDEBUG" CC=${CC} CXX=${CXX} ${MAKE}
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/mysqlx && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] mysqlx plugin (PROXYSQL40 not set)")
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/genai && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] genai plugin (PROXYSQL40 not set)")
#--

.PHONY: build_src_testaurora
build_src_testaurora: build_lib_testaurora
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testaurora_random
build_src_testaurora_random: build_lib_testaurora_random
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA -DTEST_AURORA_RANDOM" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testaurora
build_lib_testaurora: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testaurora_random
build_lib_testaurora_random: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA -DTEST_AURORA_RANDOM" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testgalera
build_src_testgalera: build_lib_testgalera
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GALERA" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testgalera
build_lib_testgalera: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GALERA" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testgrouprep
build_src_testgrouprep: build_lib_testgrouprep
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GROUPREP" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testgrouprep
build_lib_testgrouprep: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_GROUPREP" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testreadonly
build_src_testreadonly: build_lib_testreadonly
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_READONLY" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testreadonly
build_lib_testreadonly: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_READONLY" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testreplicationlag
build_src_testreplicationlag: build_lib_testreplicationlag
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_REPLICATIONLAG" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testreplicationlag
build_lib_testreplicationlag: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_REPLICATIONLAG" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_testall
build_src_testall: build_lib_testall
	cd src && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA -DTEST_GALERA -DTEST_GROUPREP -DTEST_READONLY -DTEST_REPLICATIONLAG" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_testall
build_lib_testall: build_deps_debug
	cd lib && OPTZ="${O0} -ggdb -DDEBUG -DTEST_AURORA -DTEST_GALERA -DTEST_GROUPREP -DTEST_READONLY -DTEST_REPLICATIONLAG" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_tap_test
build_tap_test: build_tap_tests
.PHONY: build_tap_tests
build_tap_tests: build_src
	cd test/tap && OPTZ="${O2} -ggdb" CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_tap_test_debug
build_tap_test_debug: build_tap_tests_debug
.PHONY: build_tap_tests_debug
build_tap_tests_debug: build_src_debug
	cd test/tap && OPTZ="${O0} -ggdb -DDEBUG" CC=${CC} CXX=${CXX} ${MAKE} debug

# The simulator links against libproxysql.a from the PREVIOUS lib build in this
# invocation (release for `make build_cluster_simulator`, debug for the _debug
# variant, or a TEST_<FAMILY>-flavored debug build when pulled in as a prereq
# of the `test<family>` targets). Keep this rule dependency-free so it does not
# clobber the caller's lib/src flavor by recursing into a conflicting build.
.PHONY: build_cluster_simulator
build_cluster_simulator:
	cd test/deps/cluster_simulator && CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_cluster_simulator_debug
build_cluster_simulator_debug:
	cd test/deps/cluster_simulator && CC=${CC} CXX=${CXX} ${MAKE} debug

# ClickHouse build targets are now default build targets. 
# To maintain backward compatibility, ClickHouse targets are still available.
.PHONY: build_deps_clickhouse
build_deps_clickhouse: build_deps_default

.PHONY: build_deps_debug_clickhouse
build_deps_debug_clickhouse: build_deps_debug_default

.PHONY: build_lib_clickhouse
build_lib_clickhouse: build_lib_default

.PHONY: build_lib_debug_clickhouse
build_lib_debug_clickhouse: build_lib_debug_default

.PHONY: build_src_clickhouse
build_src_clickhouse: build_src_default

.PHONY: build_src_debug_clickhouse
build_src_debug_clickhouse: build_src_debug_default
#--

.PHONY: build_deps_default
build_deps_default:
	cd deps && OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_deps_debug_default
build_deps_debug_default:
	cd deps && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) PROXYDEBUG=1 CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_default
build_lib_default: build_deps_default
	cd lib && OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_lib_debug_default
build_lib_debug_default: build_deps_debug_default
	cd lib && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE}

.PHONY: build_src_default
build_src_default: build_lib_default
	cd src && OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE}
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/mysqlx && OPTZ="${O2} -ggdb" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] mysqlx plugin (PROXYSQL40 not set)")
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/genai && OPTZ="${O2} -ggdb" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] genai plugin (PROXYSQL40 not set)")

.PHONY: build_src_debug_default
build_src_debug_default: build_lib_debug_default
	cd src && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQLCLICKHOUSE=$(PROXYSQLCLICKHOUSE) POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) POLARDB_PROFILE=$(POLARDB_PROFILE) POLARDB_PERF_DEBUG=$(POLARDB_PERF_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE}
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/mysqlx && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] mysqlx plugin (PROXYSQL40 not set)")
	$(if $(filter 1,$(PROXYSQL40)),cd plugins/genai && OPTZ="${O0} -ggdb -DDEBUG" PROXYSQL40=$(PROXYSQL40) PROXYSQL31=$(PROXYSQL31) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX} ${MAKE},@echo "[skip] genai plugin (PROXYSQL40 not set)")

# Core + PolarDB build chain used by compile/link optimization targets.
# It forces PROXYSQLCLICKHOUSE=0 and does not build plugins.
# Keep this path narrow: deps/lib/src only, with PolarDB libpq support enabled.
.PHONY: build_deps_polardb_core
build_deps_polardb_core:
	cd deps && ${MAKE} OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=0 POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX}

.PHONY: build_lib_polardb_core
build_lib_polardb_core: build_deps_polardb_core
	cd lib && ${MAKE} OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=0 POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX}

.PHONY: build_src_polardb_core
build_src_polardb_core: build_lib_polardb_core
	cd src && ${MAKE} OPTZ="${O2} -ggdb" PROXYSQLCLICKHOUSE=0 POLARDB_PROXY=$(POLARDB_PROXY) POLARDB_DEBUG=$(POLARDB_DEBUG) PROXYSQLFFTO=$(PROXYSQLFFTO) PROXYSQLTSDB=$(PROXYSQLTSDB) CC=${CC} CXX=${CXX}

.PHONY: clean_polardb_core_build
clean_polardb_core_build:
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean

# Rebuild only libpq (e.g. after editing the PolarDB libpq patch) without a full
# postgres re-extract. Mirrors deps/ libpq-rebuild.
.PHONY: libpq-rebuild
libpq-rebuild:
	+$(MAKE) -C deps libpq-rebuild

# Build the PolarDB binary with the verbose POLARDB_DEBUG trace facility enabled
# (per-call Handler/RunQuery/RC/WIRE FSM traces, off by default). Clean build so the
# -DPOLARDB_DEBUG=1 flag is applied to every object. Usage: make polardb-debug
.PHONY: polardb-debug
polardb-debug:
	+$(MAKE) clean
	+$(MAKE) POLARDB_PROXY=1 POLARDB_DEBUG=1 build_src
	@echo "=== Built POLARDB_PROXY=1 POLARDB_DEBUG=1 (verbose PolarDB trace enabled) ==="

# Build the optimized PolarDB binary with profiling counters enabled. This is
# meant for benchmark/profiling runs where query-path timing probes are useful.
.PHONY: polardb-profile
polardb-profile:
	+$(MAKE) clean
	+$(MAKE) POLARDB_PROXY=1 POLARDB_PROFILE=1 build_src
	@echo "=== Built POLARDB_PROXY=1 POLARDB_PROFILE=1 (PolarDB profiling counters enabled) ==="

.PHONY: polardb-perf-debug
polardb-perf-debug:
	+$(MAKE) clean
	+$(MAKE) POLARDB_PROXY=1 POLARDB_PERF_DEBUG=1 build_src
	@echo "=== Built POLARDB_PROXY=1 POLARDB_PERF_DEBUG=1 (PolarDB diagnostic counters enabled) ==="

# Build the PolarDB debug binary for coverage attribution. This keeps the same
# POLARDB_DEBUG traces as polardb-debug, but uses the debug build recipes so
# gcov does not lose small functions to optimized inlining.
.PHONY: polardb-coverage-debug
polardb-coverage-debug:
	+$(MAKE) clean
	+$(MAKE) POLARDB_PROXY=1 POLARDB_DEBUG=1 build_src_debug
	@echo "=== Built POLARDB_PROXY=1 POLARDB_DEBUG=1 -O0 (coverage attribution) ==="

# Build the PolarDB release binary (POLARDB_PROXY=1, optimized, no trace facility).
# When switching between POLARDB_PROXY tiers, run a clean build first:
#   make clean && make polardb
.PHONY: polardb
polardb:
	+$(MAKE) POLARDB_PROXY=1 build_src
	@echo "=== Built POLARDB_PROXY=1 (PolarDB release tier) ==="


# PolarDB-only optimization builds. These targets are explicit so normal
# ProxySQL, ClickHouse, deps, and packaging builds remain unchanged.
POLARDB_OPT_PGO_DIR ?= $(CURDIR)/build/polardb-pgo
POLARDB_OPT_PGO_CS_DIR ?= $(CURDIR)/build/polardb-pgo-cs
POLARDB_OPT_PGO_CS_PROFILE ?= $(POLARDB_OPT_PGO_CS_DIR)/clang-cs.profdata
POLARDB_OPT_BOLT_DIR ?= $(CURDIR)/build/polardb-bolt
POLARDB_OPT_BOLT_BINARY ?= $(CURDIR)/src/proxysql
POLARDB_OPT_BOLT_INSTRUMENTED ?= $(CURDIR)/src/proxysql.bolt.inst
POLARDB_OPT_BOLT_RAW_FDATA ?= $(POLARDB_OPT_BOLT_DIR)/proxysql.fdata
POLARDB_OPT_BOLT_FDATA ?= $(POLARDB_OPT_BOLT_DIR)/proxysql.merged.fdata
POLARDB_OPT_BOLT_OUT ?= $(CURDIR)/src/proxysql.bolt
POLARDB_OPT_REMARKS_DIR ?= $(CURDIR)/build/polardb-remarks

# BOLT optimization policy for the PolarDB/core binary:
#   -reorder-blocks=ext-tsp    profile-guided basic-block layout for I-cache locality.
#   -reorder-functions=cdsort  cache-directed function clustering. LLVM 22 treats
#                              hfsort+ as a deprecated spelling for this mode.
#   -split-functions           split hot/cold fragments inside profiled functions.
#   -split-all-cold            move all cold blocks BOLT can prove cold out of hot code.
#   -split-eh                  keep C++ exception-handling paths in cold fragments.
#   -icf                       fold identical functions; keep a non-BOLT/non-ICF binary
#                              when debugging exact symbol identity matters.
#   -use-gnu-stack             compatibility workaround for generated segment metadata.
#   -dyno-stats                print dynamic instruction/branch deltas from the profile.
POLARDB_OPT_BOLT_FLAGS ?= -reorder-blocks=ext-tsp -reorder-functions=cdsort -split-functions -split-all-cold -split-eh -icf -use-gnu-stack -dyno-stats

# Report-only flags: keep them separate so production BOLT generation can run
# quietly while the report target captures profile quality, cache, and pass timing.
POLARDB_OPT_BOLT_REPORT_FLAGS ?= -print-profile-stats -print-function-statistics=100 -print-cache-metrics -print-large-functions -time-opts

.PHONY: polardb-lto-gcc polardb-pgo-gcc-generate polardb-pgo-gcc-use polardb-pgo-lto-gcc
.PHONY: polardb-lto-clang polardb-pgo-clang-generate polardb-pgo-clang-merge polardb-pgo-clang-use
.PHONY: polardb-pgo-clang-cs-generate polardb-pgo-clang-cs-merge polardb-pgo-clang-cs-use
.PHONY: polardb-bolt-build polardb-bolt-instrument polardb-bolt-merge polardb-bolt-optimize
.PHONY: polardb-pgo-gcc-use-remarks polardb-pgo-lto-gcc-remarks
.PHONY: polardb-pgo-clang-use-remarks polardb-pgo-clang-cs-use-remarks
.PHONY: polardb-bolt-optimize-report polardb-opt-report

polardb-lto-gcc:
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=auto build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with GCC LTO ==="

polardb-pgo-gcc-generate:
	@mkdir -p "$(POLARDB_OPT_PGO_DIR)"
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=generate POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" build_src_polardb_core
	@echo "=== Built GCC PGO-instrumented PolarDB binary ==="
	@echo "    Train with: make -C test/polardb pgo-train-core"

polardb-pgo-gcc-use:
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with GCC PGO ==="

polardb-pgo-gcc-use-remarks:
	@mkdir -p "$(POLARDB_OPT_REMARKS_DIR)"
	+$(MAKE) clean_polardb_core_build
	+bash -o pipefail -c 'CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_REMARKS=1 POLARDB_OPT_REMARKS_DIR="$(POLARDB_OPT_REMARKS_DIR)" build_src_polardb_core 2>&1 | tee "$(POLARDB_OPT_REMARKS_DIR)/gcc-pgo-use-build.log"'
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

polardb-pgo-lto-gcc:
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=auto build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with GCC PGO + LTO ==="

polardb-pgo-lto-gcc-remarks:
	@mkdir -p "$(POLARDB_OPT_REMARKS_DIR)"
	+$(MAKE) clean_polardb_core_build
	+bash -o pipefail -c 'CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=auto POLARDB_OPT_REMARKS=1 POLARDB_OPT_REMARKS_DIR="$(POLARDB_OPT_REMARKS_DIR)" build_src_polardb_core 2>&1 | tee "$(POLARDB_OPT_REMARKS_DIR)/gcc-pgo-lto-build.log"'
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

polardb-lto-clang:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang LTO on this tree" >&2; exit 1; }
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with clang ThinLTO ==="

polardb-pgo-clang-generate:
	@mkdir -p "$(POLARDB_OPT_PGO_DIR)"
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=generate POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" build_src_polardb_core
	@echo "=== Built clang IR PGO-instrumented PolarDB binary ==="
	@echo "    Train with: make -C test/polardb pgo-train-core"
	@echo "    Then merge: make polardb-pgo-clang-merge"

polardb-pgo-clang-merge:
	@command -v llvm-profdata >/dev/null 2>&1 || { echo "llvm-profdata is required to merge clang PGO profiles" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_PGO_DIR)"
	@set -- "$(POLARDB_OPT_PGO_DIR)"/*.profraw; [ -e "$$1" ] || { echo "No clang .profraw files found in $(POLARDB_OPT_PGO_DIR)" >&2; exit 1; }; llvm-profdata merge -output="$(POLARDB_OPT_PGO_DIR)/clang.profdata" "$(POLARDB_OPT_PGO_DIR)"/*.profraw
	@echo "=== Merged clang PGO profile: $(POLARDB_OPT_PGO_DIR)/clang.profdata ==="

polardb-pgo-clang-use:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang builds on this tree" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_PGO_DIR)/clang.profdata" || { echo "Missing $(POLARDB_OPT_PGO_DIR)/clang.profdata; run polardb-pgo-clang-merge first" >&2; exit 1; }
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_PGO_PROFILE="$(POLARDB_OPT_PGO_DIR)/clang.profdata" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with clang PGO + ThinLTO ==="

polardb-pgo-clang-use-remarks:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang builds on this tree" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_PGO_DIR)/clang.profdata" || { echo "Missing $(POLARDB_OPT_PGO_DIR)/clang.profdata; run polardb-pgo-clang-merge first" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_REMARKS_DIR)"
	+$(MAKE) clean_polardb_core_build
	+bash -o pipefail -c 'CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_PGO_PROFILE="$(POLARDB_OPT_PGO_DIR)/clang.profdata" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin POLARDB_OPT_REMARKS=1 POLARDB_OPT_REMARKS_DIR="$(POLARDB_OPT_REMARKS_DIR)" build_src_polardb_core 2>&1 | tee "$(POLARDB_OPT_REMARKS_DIR)/clang-pgo-use-build.log"'
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

polardb-pgo-clang-cs-generate:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang builds on this tree" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_PGO_DIR)/clang.profdata" || { echo "Missing $(POLARDB_OPT_PGO_DIR)/clang.profdata; run polardb-pgo-clang-merge first" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_PGO_CS_DIR)"
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=cs-generate POLARDB_OPT_PGO_DIR="$(POLARDB_OPT_PGO_DIR)" POLARDB_OPT_PGO_PROFILE="$(POLARDB_OPT_PGO_DIR)/clang.profdata" POLARDB_OPT_PGO_CS_DIR="$(POLARDB_OPT_PGO_CS_DIR)" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin build_src_polardb_core
	@echo "=== Built clang context-sensitive PGO-instrumented PolarDB binary ==="
	@echo "    Train with: make -C test/polardb pgo-train-core"
	@echo "    Then merge: make polardb-pgo-clang-cs-merge"

polardb-pgo-clang-cs-merge:
	@command -v llvm-profdata >/dev/null 2>&1 || { echo "llvm-profdata is required to merge clang CS PGO profiles" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_PGO_CS_DIR)"
	@set -- "$(POLARDB_OPT_PGO_CS_DIR)"/*.profraw; [ -e "$$1" ] || { echo "No clang CS .profraw files found in $(POLARDB_OPT_PGO_CS_DIR)" >&2; exit 1; }; llvm-profdata merge -output="$(POLARDB_OPT_PGO_CS_PROFILE)" "$(POLARDB_OPT_PGO_CS_DIR)"/*.profraw
	@echo "=== Merged clang CS PGO profile: $(POLARDB_OPT_PGO_CS_PROFILE) ==="

polardb-pgo-clang-cs-use:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang builds on this tree" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_PGO_CS_PROFILE)" || { echo "Missing $(POLARDB_OPT_PGO_CS_PROFILE); run polardb-pgo-clang-cs-merge first" >&2; exit 1; }
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_PROFILE="$(POLARDB_OPT_PGO_CS_PROFILE)" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin build_src_polardb_core
	@echo "=== Built POLARDB_PROXY=1 with clang context-sensitive PGO + ThinLTO ==="

polardb-pgo-clang-cs-use-remarks:
	@command -v ld.lld >/dev/null 2>&1 || { echo "ld.lld is required for clang builds on this tree" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_PGO_CS_PROFILE)" || { echo "Missing $(POLARDB_OPT_PGO_CS_PROFILE); run polardb-pgo-clang-cs-merge first" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_REMARKS_DIR)"
	+$(MAKE) clean_polardb_core_build
	+bash -o pipefail -c 'CCACHE_DISABLE=1 $(MAKE) CC=clang CXX=clang++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_PGO=use POLARDB_OPT_PGO_PROFILE="$(POLARDB_OPT_PGO_CS_PROFILE)" POLARDB_OPT_LTO=1 POLARDB_OPT_LTO_MODE=thin POLARDB_OPT_REMARKS=1 POLARDB_OPT_REMARKS_DIR="$(POLARDB_OPT_REMARKS_DIR)" build_src_polardb_core 2>&1 | tee "$(POLARDB_OPT_REMARKS_DIR)/clang-cs-pgo-use-build.log"'
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

polardb-bolt-build:
	@mkdir -p "$(POLARDB_OPT_BOLT_DIR)"
	+$(MAKE) clean_polardb_core_build
	+CCACHE_DISABLE=1 $(MAKE) CC=gcc CXX=g++ POLARDB_PROXY=1 PROXYSQLCLICKHOUSE=0 POLARDB_OPT_BUILD=1 POLARDB_OPT_BOLT_READY=1 build_src_polardb_core
	@echo "=== Built BOLT-ready PolarDB binary with --emit-relocs ==="
	@echo "    Instrument next: make polardb-bolt-instrument"

polardb-bolt-instrument:
	@command -v llvm-bolt >/dev/null 2>&1 || { echo "llvm-bolt is required for BOLT instrumentation" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_BOLT_BINARY)" || { echo "Missing binary: $(POLARDB_OPT_BOLT_BINARY)" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_BOLT_DIR)"
	rm -f "$(POLARDB_OPT_BOLT_RAW_FDATA)" "$(POLARDB_OPT_BOLT_RAW_FDATA)".*
	llvm-bolt "$(POLARDB_OPT_BOLT_BINARY)" -instrument -instrumentation-file="$(POLARDB_OPT_BOLT_RAW_FDATA)" -instrumentation-file-append-pid -o "$(POLARDB_OPT_BOLT_INSTRUMENTED)"
	@echo "=== Wrote BOLT-instrumented binary: $(POLARDB_OPT_BOLT_INSTRUMENTED) ==="
	@echo "    Train with: make -C test/polardb bolt-train-core"

polardb-bolt-merge:
	@command -v merge-fdata >/dev/null 2>&1 || { echo "merge-fdata is required to merge BOLT instrumentation profiles" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_BOLT_DIR)"
	@files=$$(find "$(POLARDB_OPT_BOLT_DIR)" -maxdepth 1 -type f \( -name '$(notdir $(POLARDB_OPT_BOLT_RAW_FDATA))' -o -name '$(notdir $(POLARDB_OPT_BOLT_RAW_FDATA)).*' \) | sort); \
		[ -n "$$files" ] || { echo "No BOLT instrumentation profiles found under $(POLARDB_OPT_BOLT_DIR)" >&2; exit 1; }; \
		merge-fdata $$files > "$(POLARDB_OPT_BOLT_FDATA)"
	@echo "=== Merged BOLT profile: $(POLARDB_OPT_BOLT_FDATA) ==="

polardb-bolt-optimize: polardb-bolt-merge
	@command -v llvm-bolt >/dev/null 2>&1 || { echo "llvm-bolt is required for BOLT optimization" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_BOLT_BINARY)" || { echo "Missing binary: $(POLARDB_OPT_BOLT_BINARY)" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_BOLT_FDATA)" || { echo "Missing BOLT profile: $(POLARDB_OPT_BOLT_FDATA)" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_BOLT_DIR)"
	llvm-bolt "$(POLARDB_OPT_BOLT_BINARY)" -o "$(POLARDB_OPT_BOLT_OUT)" -data="$(POLARDB_OPT_BOLT_FDATA)" $(POLARDB_OPT_BOLT_FLAGS)
	@echo "=== Wrote BOLT-optimized binary: $(POLARDB_OPT_BOLT_OUT) ==="

polardb-bolt-optimize-report: polardb-bolt-merge
	@command -v llvm-bolt >/dev/null 2>&1 || { echo "llvm-bolt is required for BOLT optimization" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_BOLT_BINARY)" || { echo "Missing binary: $(POLARDB_OPT_BOLT_BINARY)" >&2; exit 1; }
	@test -f "$(POLARDB_OPT_BOLT_FDATA)" || { echo "Missing BOLT profile: $(POLARDB_OPT_BOLT_FDATA)" >&2; exit 1; }
	@mkdir -p "$(POLARDB_OPT_BOLT_DIR)" "$(POLARDB_OPT_REMARKS_DIR)"
	bash -o pipefail -c 'llvm-bolt "$(POLARDB_OPT_BOLT_BINARY)" -o "$(POLARDB_OPT_BOLT_OUT)" -data="$(POLARDB_OPT_BOLT_FDATA)" $(POLARDB_OPT_BOLT_FLAGS) $(POLARDB_OPT_BOLT_REPORT_FLAGS) 2>&1 | tee "$(POLARDB_OPT_BOLT_DIR)/bolt-optimize.log"'
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

polardb-opt-report:
	@mkdir -p "$(POLARDB_OPT_REMARKS_DIR)"
	@$(CURDIR)/test/polardb/tools/opt_remarks_report.py --root "$(CURDIR)" --remarks-dir "$(POLARDB_OPT_REMARKS_DIR)" --pgo-dir "$(POLARDB_OPT_PGO_DIR)" --cs-dir "$(POLARDB_OPT_PGO_CS_DIR)" --bolt-dir "$(POLARDB_OPT_BOLT_DIR)" --output "$(POLARDB_OPT_REMARKS_DIR)/summary.md"
	@echo "=== Wrote optimization remarks report: $(POLARDB_OPT_REMARKS_DIR)/summary.md ==="

# Verify both tiers with explicit clean builds: feature off first, then on.
# The final build is the PolarDB binary used by development and live tests.
# Usage: make polardb-check
.PHONY: polardb-check
polardb-check:
	@echo "=== polardb-check [1/2]: POLARDB_PROXY=0 (stubs) ==="
	+$(MAKE) clean
	+$(MAKE) -C deps postgresql-reset
	+$(MAKE) POLARDB_PROXY=0 build_src
	@echo "=== polardb-check [2/2]: POLARDB_PROXY=1 ==="
	+$(MAKE) clean
	+$(MAKE) -C deps postgresql-reset
	+$(MAKE) POLARDB_PROXY=1 build_src
	@echo "=== polardb-check OK: both tiers build; tree left at POLARDB_PROXY=1 ==="

# Re-extract PostgreSQL, apply the full libpq patch stack, rebuild libpq and
# bundled pgbench, then build the standalone PolarDB test helpers. Configure
# test/polardb/.env before running the helpers against a live cluster. Usage:
#   make polardb-libpq
.PHONY: polardb-libpq
polardb-libpq:
	cd deps/postgresql && rm -rf postgres-*/ || true
	+$(MAKE) -C deps POLARDB_PROXY=1 postgresql
	+$(MAKE) -C test polardb
	@echo "=== Rebuilt libpq (PolarDB patch), bundled pgbench, and PolarDB test helpers ==="
	@echo "    Configure test/polardb/.env, then run:"
	@echo "      make -C test/polardb help"
	@echo "      make -C test/polardb run-c-libpq-lsn"
	@echo "      make -C test/polardb run-c-libpq-xact"
	@echo "      make -C test/polardb run-c-extended-protocol QUERY='SELECT 1'"

# Refresh deps/postgresql/polardb_libpq.patch from the expanded vendored
# PostgreSQL tree and verify that the regenerated patch applies cleanly.
# Usage:
#   make polardb-libpq-patch
.PHONY: polardb-libpq-patch
polardb-libpq-patch:
	./scripts/regenerate-polardb-libpq-patch.sh --verify


### packaging targets

SYS_KERN := $(shell uname -s)
#SYS_DIST := $(shell source /etc/os-release &>/dev/null; if [ -z ${NAME} ]; then head -1 /etc/redhat-release; else echo ${NAME}; fi | awk '{ print $1 })
SYS_ARCH := $(shell uname -m)
REL_ARCH = $(subst x86_64,amd64,$(subst aarch64,arm64,$(SYS_ARCH)))
RPM_ARCH = .$(SYS_ARCH)
DEB_ARCH = _$(REL_ARCH)
REL_VERS := $(shell echo ${GIT_VERSION} | sed -E 's/^v//' | grep -Eo '^[0-9\.]+')
RPM_VERS := -$(REL_VERS)-1
DEB_VERS := _$(REL_VERS)

packages: $(REL_ARCH)-packages ;
almalinux: $(REL_ARCH)-almalinux ;
centos: $(REL_ARCH)-centos ;
debian: $(REL_ARCH)-debian ;
fedora: $(REL_ARCH)-fedora ;
opensuse: $(REL_ARCH)-opensuse ;
ubuntu: $(REL_ARCH)-ubuntu ;
pkglist: $(REL_ARCH)-pkglist

amd64-%: SYS_ARCH := x86_64
amd64-packages: amd64-centos amd64-ubuntu amd64-debian amd64-fedora amd64-opensuse amd64-almalinux
amd64-almalinux: almalinux8 almalinux8-clang almalinux8-dbg almalinux9 almalinux9-clang almalinux9-dbg almalinux10 almalinux10-clang almalinux10-dbg
amd64-centos: centos9 centos9-clang centos9-dbg centos10 centos10-clang centos10-dbg
amd64-debian: debian12 debian12-clang debian12-dbg debian13 debian13-clang debian13-dbg
amd64-fedora: fedora42 fedora42-clang fedora42-dbg fedora43 fedora43-clang fedora43-dbg fedora44 fedora44-clang fedora44-dbg
amd64-opensuse: opensuse15 opensuse15-clang opensuse15-dbg opensuse16 opensuse16-clang opensuse16-dbg
amd64-ubuntu: ubuntu22 ubuntu22-clang ubuntu22-dbg ubuntu24 ubuntu24-clang ubuntu24-dbg
amd64-pkglist:
	@${MAKE} -nk amd64-packages 2>/dev/null | grep -Eo 'binaries/proxysql[^ ]*' | sed 's,^binaries/,,'

arm64-%: SYS_ARCH := aarch64
arm64-packages: arm64-centos arm64-debian arm64-ubuntu arm64-fedora arm64-opensuse arm64-almalinux
arm64-almalinux: almalinux8 almalinux9 almalinux10
arm64-centos: centos9 centos10
arm64-debian: debian12 debian13
arm64-fedora: fedora42 fedora43 fedora44
arm64-opensuse: opensuse15 opensuse16
arm64-ubuntu: ubuntu22 ubuntu24
arm64-pkglist:
	@${MAKE} -nk arm64-packages 2>/dev/null | grep -Eo 'binaries/proxysql[^ ]*' | sed 's,^binaries/,,'

almalinux%: build-almalinux% ;
centos%: build-centos% ;
debian%: build-debian% ;
fedora%: build-fedora% ;
opensuse%: build-opensuse% ;
ubuntu%: build-ubuntu% ;


.PHONY: build-%
.NOTPARALLEL: build-%
build-%: BLD_NAME=$(patsubst build-%,%,$@)
build-%: PKG_VERS=$(if $(filter $(shell echo ${BLD_NAME} | grep -Eo '[a-z]+'),debian ubuntu),$(DEB_VERS),$(RPM_VERS))
build-%: PKG_TYPE=$(if $(filter $(shell echo $(BLD_NAME) | grep -Eo '\-de?bu?g|\-test|\-tap'),-dbg -debug -test -tap),-dbg,)
build-%: PKG_NAME=$(firstword $(subst -, ,$(BLD_NAME)))
build-%: PKG_COMP=$(if $(filter $(shell echo $(BLD_NAME) | grep -Eo '\-clang'),-clang),-clang,)
build-%: PKG_ARCH=$(if $(filter $(shell echo ${BLD_NAME} | grep -Eo '[a-z]+'),debian ubuntu),$(DEB_ARCH),$(RPM_ARCH))
build-%: PKG_KIND=$(if $(filter $(shell echo ${BLD_NAME} | grep -Eo '[a-z]+'),debian ubuntu),deb,rpm)
build-%: PKG_FILE=binaries/proxysql$(PKG_VERS)$(PKG_TYPE)-$(PKG_NAME)$(PKG_COMP)$(PKG_ARCH).$(PKG_KIND)
build-%:
	@echo 'building $@'
	@IMG_NAME=$(PKG_NAME) IMG_TYPE=$(subst -,_,$(PKG_TYPE)) IMG_COMP=$(subst -,_,$(PKG_COMP)) BLD_NAME=$(BLD_NAME) $(MAKE) $(PKG_FILE)

.NOTPARALLEL: binaries/proxysql%
binaries/proxysql%:
	${MAKE} cleanbuild
	${MAKE} cleantest
	find . -not -path "./binaries/*" -not -path "./.git/*" | xargs touch -h --date=@${SOURCE_DATE_EPOCH}
	@docker compose -p "${GIT_VERSION/./}" down -v --remove-orphans
	@docker compose -p "${GIT_VERSION/./}" up $(IMG_NAME)$(IMG_TYPE)$(IMG_COMP)_build
	@docker compose -p "${GIT_VERSION/./}" down -v --remove-orphans


### clean targets

.PHONY: clean
clean:
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean
	cd plugins/mysqlx && ${MAKE} clean
	cd plugins/genai && ${MAKE} clean
	cd test/tap && ${MAKE} clean
	rm -f pkgroot || true

.PHONY: cleandeps
cleandeps:
	cd deps && ${MAKE} cleanall
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean
	cd plugins/mysqlx && ${MAKE} clean
	cd plugins/genai && ${MAKE} clean

.PHONY: cleandev
cleandev:
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean
	cd plugins/mysqlx && ${MAKE} clean
	cd plugins/genai && ${MAKE} clean

.PHONY: cleantest
cleantest:
	cd test/tap && ${MAKE} clean
	cd test/deps && ${MAKE} cleanall

.PHONY: cleanall
cleanall:
	cd deps && ${MAKE} cleanall
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean
	cd plugins/mysqlx && ${MAKE} clean
	cd plugins/genai && ${MAKE} clean
	cd test/tap && ${MAKE} clean
	cd test/deps && ${MAKE} cleanall
	rm -f binaries/* || true
	rm -rf pkgroot || true

.PHONY: cleanbuild
cleanbuild:
	cd deps && ${MAKE} cleanall
	cd lib && ${MAKE} clean
	cd src && ${MAKE} clean
	cd plugins/mysqlx && ${MAKE} clean
	cd plugins/genai && ${MAKE} clean
	rm -rf pkgroot || true


### install targets

.PHONY: install
install: src/proxysql
	install -m 0755 src/proxysql /usr/bin
	install -m 0600 etc/proxysql.cnf /etc
	if [ ! -d /var/lib/proxysql ]; then mkdir /var/lib/proxysql ; fi
	if [ -f plugins/mysqlx/ProxySQL_MySQLX_Plugin.so ]; then \
		install -d /usr/lib/proxysql/plugins ; \
		install -m 0755 plugins/mysqlx/ProxySQL_MySQLX_Plugin.so /usr/lib/proxysql/plugins/ ; \
	fi
	if [ -f plugins/genai/ProxySQL_GenAI_Plugin.so ]; then \
		install -d /usr/lib/proxysql/plugins ; \
		install -m 0755 plugins/genai/ProxySQL_GenAI_Plugin.so /usr/lib/proxysql/plugins/ ; \
	fi
ifeq ($(findstring proxysql,$(USERCHECK)),)
	@echo "Creating proxysql user and group"
	useradd -r -U -s /bin/false proxysql
endif
ifeq ($(SYSTEMD), 1)
	install -m 0644 systemd/system/proxysql.service /usr/lib/systemd/system/
	systemctl enable proxysql.service
else
	install -m 0755 etc/init.d/proxysql /etc/init.d
ifeq ($(DISTRO),"CentOS Linux")
		chkconfig --level 0123456 proxysql on
else
ifeq ($(DISTRO),"Rocky Linux")
		chkconfig --level 0123456 proxysql on
else
ifeq ($(DISTRO),"Red Hat Enterprise Linux Server")
		chkconfig --level 0123456 proxysql on
else
ifeq ($(DISTRO),"Ubuntu")
		update-rc.d proxysql defaults
else
ifeq ($(DISTRO),"Debian GNU/Linux")
		update-rc.d proxysql defaults
else
ifeq ($(DISTRO),"Unknown")
	$(warning Not sure how to install proxysql service on this OS)
endif
endif
endif
endif
endif
endif
endif

.PHONY: uninstall
uninstall:
	if [ -f /etc/proxysql.cnf ]; then rm /etc/proxysql.cnf ; fi
	if [ -f /usr/bin/proxysql ]; then rm /usr/bin/proxysql ; fi
	if [ -f /usr/lib/proxysql/plugins/ProxySQL_MySQLX_Plugin.so ]; then rm /usr/lib/proxysql/plugins/ProxySQL_MySQLX_Plugin.so ; fi
	if [ -f /usr/lib/proxysql/plugins/ProxySQL_GenAI_Plugin.so ]; then rm /usr/lib/proxysql/plugins/ProxySQL_GenAI_Plugin.so ; fi
	if [ -d /usr/lib/proxysql/plugins ]; then rmdir /usr/lib/proxysql/plugins 2>/dev/null || true ; fi
	if [ -d /usr/lib/proxysql ]; then rmdir /usr/lib/proxysql 2>/dev/null || true ; fi
	if [ -d /var/lib/proxysql ]; then rmdir /var/lib/proxysql 2>/dev/null || true ; fi
ifeq ($(SYSTEMD), 1)
		systemctl stop proxysql.service
		if [ -f /usr/lib/systemd/system/proxysql.service ]; then rm /usr/lib/systemd/system/proxysql.service ; fi
		find /etc/systemd -name "proxysql.service" -exec rm {} \;
		systemctl daemon-reload
else
ifeq ($(DISTRO),"CentOS Linux")
		chkconfig --level 0123456 proxysql off
		if [ -f /etc/init.d/proxysql ]; then rm /etc/init.d/proxysql ; fi
else
ifeq ($(DISTRO),"Red Hat Enterprise Linux Server")
		chkconfig --level 0123456 proxysql off
		if [ -f /etc/init.d/proxysql ]; then rm /etc/init.d/proxysql ; fi
else
ifeq ($(DISTRO),"Ubuntu")
		if [ -f /etc/init.d/proxysql ]; then rm /etc/init.d/proxysql ; fi
		update-rc.d proxysql remove
else
ifeq ($(DISTRO),"Debian GNU/Linux")
		if [ -f /etc/init.d/proxysql ]; then rm /etc/init.d/proxysql ; fi
		update-rc.d proxysql remove
else
ifeq ($(DISTRO),"Unknown")
$(warning Not sure how to uninstall proxysql service on this OS)
endif
endif
endif
endif
endif
endif
ifneq ($(findstring proxysql,$(USERCHECK)),)
	@echo "Deleting proxysql user"
	userdel proxysql
endif
ifneq ($(findstring proxysql,$(GROUPCHECK)),)
	@echo "Deleting proxysql group"
	groupdel proxysql
endif
