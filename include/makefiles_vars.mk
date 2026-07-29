#!/bin/make -f


GIT_VERSION ?= $(shell git describe --long --abbrev=7)
ifndef GIT_VERSION
    $(error GIT_VERSION is not set)
endif

# Build type describes compiler mode; build variant describes optional product
# code and instrumentation. Both affect object compatibility. An unclassified
# direct lib/src build is custom, never assumed compatible with release.
PROXYSQL_BUILD_TYPE := custom
ifneq (,$(filter -O1 -O2 -O3 -Os -Oz,$(OPTZ)))
	PROXYSQL_BUILD_TYPE := release
endif
ifneq (,$(filter -O0,$(OPTZ)))
	PROXYSQL_BUILD_TYPE := debug
endif
ifneq (,$(findstring -DDEBUG,$(OPTZ) $(DEBUG)))
	PROXYSQL_BUILD_TYPE := debug
endif

POLARDB_BUILD_VARIANT := core
ifeq ($(POLARDB_PROXY),1)
	POLARDB_BUILD_VARIANT := polardb
endif
ifeq ($(POLARDB_PROFILE),1)
	POLARDB_BUILD_VARIANT := $(POLARDB_BUILD_VARIANT)-profile
endif
ifeq ($(POLARDB_PERF_DEBUG),1)
	POLARDB_BUILD_VARIANT := $(POLARDB_BUILD_VARIANT)-perf
endif
ifeq ($(POLARDB_DEBUG),1)
	POLARDB_BUILD_VARIANT := $(POLARDB_BUILD_VARIANT)-debug
endif
POLARDB_BUILD_VARIANT := $(POLARDB_BUILD_VARIANT)-$(PROXYSQL_BUILD_TYPE)

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

DISTRO := $(shell if [ -f /etc/os-release ]; then grep '^ID=' /etc/os-release | cut -d= -f2 | tr -d '"'; else echo "unknown"; fi)

CENTOSVER := Unknown
ifneq (,$(wildcard /etc/system-release))
	CENTOSVER := $(shell rpm --eval %rhel)
endif

# NOTE: CENTOSVER is still used in deps/Makefile for CentOS 6 workarounds.
# IS_ARM and IS_CENTOS were removed when jemalloc page-size detection
# switched from arch-specific to auto-detection (see deps/Makefile).


### detect compiler support for c++11/17
CPLUSPLUS := $(shell ${CC} -std=c++17 -dM -E -x c++ /dev/null 2>/dev/null | grep -F __cplusplus | egrep -o '[0-9]{6}L')
ifneq ($(CPLUSPLUS),201703L)
	CPLUSPLUS := $(shell ${CC} -std=c++11 -dM -E -x c++ /dev/null 2>/dev/null| grep -F __cplusplus | egrep -o '[0-9]{6}L')
ifneq ($(CPLUSPLUS),201103L)
    $(error Compiler must support at least c++11)
endif
endif
STDCPP := -std=c++$(shell echo $(CPLUSPLUS) | cut -c3-4) -DCXX$(shell echo $(CPLUSPLUS) | cut -c3-4)


WGCOV :=
ifeq ($(WITHGCOV),1)
	WGCOV := -DWITHGCOV -lgcov --coverage
endif

WASAN :=
ifeq ($(WITHASAN),1)
	WASAN := -fsanitize=address
	# Force the disable of JEMALLOC, since ASAN isn't compatible.
	export NOJEMALLOC=1
	# workaroud ASAN limitation ASLR > 28bits
	# https://github.com/google/sanitizers/issues/1716
	# sudo sysctl vm.mmap_rnd_bits=28
    $(warning ASAN needs ASLR =< 28bits, make sure 'sysctl vm.mmap_rnd_bits=28' is set.)
endif
ifeq ($(TEST_WITHASAN),1)
	WASAN += -DTEST_WITHASAN
endif

# ThreadSanitizer support. Mutually exclusive with WITHASAN — both
# sanitizers reroute the same memory-management hooks and the linker
# rejects the combination outright. Like ASAN, TSAN is incompatible
# with jemalloc, so NOJEMALLOC is forced. The flag is added to both
# CXX_FLAGS and LD_FLAGS via $(WASAN) — TSAN piggybacks on the same
# variable name to keep the propagation paths unchanged across deps/
# lib/ src/ test/ Makefiles. Reuse means a build is *either* ASAN
# *or* TSAN, never both. TSAN inherits ASAN's ASLR width constraint
# (Linux 5.18+ defaults vm.mmap_rnd_bits=32; TSAN expects 28).
ifeq ($(WITHTSAN),1)
ifeq ($(WITHASAN),1)
    $(error WITHASAN=1 and WITHTSAN=1 are mutually exclusive — pick one)
endif
	WASAN := -fsanitize=thread
	export NOJEMALLOC=1
    $(warning TSAN needs ASLR =< 28bits, make sure 'sysctl vm.mmap_rnd_bits=28' is set.)
endif

NOJEM :=
ifeq ($(NOJEMALLOC),1)
	NOJEM := -DNOJEM
endif


# PolarDB-only compile/link optimization controls.
#
# These variables are intentionally inert for normal ProxySQL builds. They are
# enabled only by explicit polardb-* optimization targets in the top-level
# Makefile. The goal is to tune the core + PolarDB path without changing the
# generic build, ClickHouse targets, or third-party dependency recipes.
POLARDB_OPT_BUILD ?= 0
POLARDB_OPT_LTO ?= 0
POLARDB_OPT_LTO_MODE ?= auto
POLARDB_OPT_PGO ?= off
POLARDB_OPT_PGO_DIR ?= $(PROXYSQL_PATH)/build/polardb-pgo
POLARDB_OPT_PGO_RAW ?= $(POLARDB_OPT_PGO_DIR)/default_%m.profraw
POLARDB_OPT_PGO_PROFILE ?= $(POLARDB_OPT_PGO_DIR)/clang.profdata
POLARDB_OPT_PGO_CS_DIR ?= $(PROXYSQL_PATH)/build/polardb-pgo-cs
POLARDB_OPT_BOLT_READY ?= 0
POLARDB_OPT_REMARKS ?= 0
POLARDB_OPT_REMARKS_DIR ?= $(PROXYSQL_PATH)/build/polardb-remarks
POLARDB_OPT_REMARKS_HOTNESS ?= 1000
# Escaped pipes are intentional: these regexes are expanded directly into shell
# compile/link commands. Keep the backslashes so the shell does not treat the
# regex alternation as a pipeline.
POLARDB_OPT_REMARKS_PASSES ?= inline\|pgo\|pgo-icall-prom\|loop-vectorize\|slp-vectorizer\|hotcoldsplit\|function-specialization\|ipsccp

POLARDB_OPT_CFLAGS :=
POLARDB_OPT_CXXFLAGS :=
POLARDB_OPT_LDFLAGS :=
POLARDB_OPT_AR := ar

ifeq ($(POLARDB_OPT_BUILD),1)
ifneq ($(POLARDB_PROXY),1)
    $(error POLARDB_OPT_BUILD=1 requires POLARDB_PROXY=1)
endif
ifeq ($(PROXYSQLCLICKHOUSE),1)
    $(error POLARDB_OPT_BUILD=1 is scoped to core/PolarDB; do not set PROXYSQLCLICKHOUSE=1)
endif
ifneq ($(filter $(POLARDB_OPT_PGO),off generate cs-generate use),$(POLARDB_OPT_PGO))
    $(error POLARDB_OPT_PGO must be off, generate, cs-generate, or use)
endif
ifneq ($(filter $(POLARDB_OPT_LTO_MODE),auto full thin),$(POLARDB_OPT_LTO_MODE))
    $(error POLARDB_OPT_LTO_MODE must be auto, full, or thin)
endif

POLARDB_OPT_IS_CLANG := $(findstring clang,$(notdir $(CXX)))

ifeq ($(POLARDB_OPT_LTO),1)
ifneq ($(POLARDB_OPT_IS_CLANG),)
ifeq ($(POLARDB_OPT_LTO_MODE),thin)
    POLARDB_OPT_CFLAGS += -flto=thin
    POLARDB_OPT_CXXFLAGS += -flto=thin
else
    POLARDB_OPT_CFLAGS += -flto
    POLARDB_OPT_CXXFLAGS += -flto
endif
    POLARDB_OPT_AR := $(shell command -v llvm-ar 2>/dev/null || echo ar)
else
ifeq ($(POLARDB_OPT_LTO_MODE),thin)
    $(error POLARDB_OPT_LTO_MODE=thin requires clang/clang++)
endif
ifeq ($(POLARDB_OPT_LTO_MODE),full)
    POLARDB_OPT_CFLAGS += -flto
    POLARDB_OPT_CXXFLAGS += -flto
else
    POLARDB_OPT_CFLAGS += -flto=auto
    POLARDB_OPT_CXXFLAGS += -flto=auto
endif
    POLARDB_OPT_AR := $(shell command -v gcc-ar 2>/dev/null || echo ar)
endif
endif

ifeq ($(POLARDB_OPT_PGO),generate)
ifneq ($(POLARDB_OPT_IS_CLANG),)
    POLARDB_OPT_CFLAGS += -fprofile-generate=$(POLARDB_OPT_PGO_DIR)
    POLARDB_OPT_CXXFLAGS += -fprofile-generate=$(POLARDB_OPT_PGO_DIR)
else
    POLARDB_OPT_CFLAGS += -fprofile-generate=$(POLARDB_OPT_PGO_DIR)
    POLARDB_OPT_CXXFLAGS += -fprofile-generate=$(POLARDB_OPT_PGO_DIR)
endif
endif

ifeq ($(POLARDB_OPT_PGO),cs-generate)
ifneq ($(POLARDB_OPT_IS_CLANG),)
    POLARDB_OPT_CFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_PROFILE) -fcs-profile-generate=$(POLARDB_OPT_PGO_CS_DIR)
    POLARDB_OPT_CXXFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_PROFILE) -fcs-profile-generate=$(POLARDB_OPT_PGO_CS_DIR)
else
    $(error POLARDB_OPT_PGO=cs-generate requires clang/clang++)
endif
endif

ifeq ($(POLARDB_OPT_PGO),use)
ifneq ($(POLARDB_OPT_IS_CLANG),)
    POLARDB_OPT_CFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_PROFILE)
    POLARDB_OPT_CXXFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_PROFILE)
else
    POLARDB_OPT_CFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_DIR) -fprofile-correction -Wno-error=coverage-mismatch
    POLARDB_OPT_CXXFLAGS += -fprofile-use=$(POLARDB_OPT_PGO_DIR) -fprofile-correction -Wno-error=coverage-mismatch
endif
endif

ifeq ($(POLARDB_OPT_BOLT_READY),1)
    POLARDB_OPT_CFLAGS += -g
    POLARDB_OPT_CXXFLAGS += -g
    POLARDB_OPT_LDFLAGS += -Wl,--emit-relocs
endif

ifeq ($(POLARDB_OPT_REMARKS),1)
ifneq ($(POLARDB_OPT_IS_CLANG),)
    POLARDB_OPT_CFLAGS += -Rpass=$(POLARDB_OPT_REMARKS_PASSES) -Rpass-missed=$(POLARDB_OPT_REMARKS_PASSES) -Rpass-analysis=$(POLARDB_OPT_REMARKS_PASSES)
    POLARDB_OPT_CFLAGS += -fdiagnostics-show-hotness -fdiagnostics-hotness-threshold=$(POLARDB_OPT_REMARKS_HOTNESS)
    POLARDB_OPT_CFLAGS += -fsave-optimization-record=yaml -foptimization-record-passes=$(POLARDB_OPT_REMARKS_PASSES) -ftime-trace
    POLARDB_OPT_CXXFLAGS += -Rpass=$(POLARDB_OPT_REMARKS_PASSES) -Rpass-missed=$(POLARDB_OPT_REMARKS_PASSES) -Rpass-analysis=$(POLARDB_OPT_REMARKS_PASSES)
    POLARDB_OPT_CXXFLAGS += -fdiagnostics-show-hotness -fdiagnostics-hotness-threshold=$(POLARDB_OPT_REMARKS_HOTNESS)
    POLARDB_OPT_CXXFLAGS += -fsave-optimization-record=yaml -foptimization-record-passes=$(POLARDB_OPT_REMARKS_PASSES) -ftime-trace
    POLARDB_OPT_LDFLAGS += -Wl,--opt-remarks-passes=$(POLARDB_OPT_REMARKS_PASSES)
    POLARDB_OPT_LDFLAGS += -Wl,--opt-remarks-with-hotness -Wl,--opt-remarks-hotness-threshold=$(POLARDB_OPT_REMARKS_HOTNESS)
    POLARDB_OPT_LDFLAGS += -Wl,--opt-remarks-format=yaml -Wl,--opt-remarks-filename=$(POLARDB_OPT_REMARKS_DIR)/lto.opt.yaml
    POLARDB_OPT_LDFLAGS += -Wl,--plugin-opt=stats-file=$(POLARDB_OPT_REMARKS_DIR)/lto.stats -Wl,--plugin-opt=time-trace=$(POLARDB_OPT_REMARKS_DIR)/lto-time-trace.json
else
    POLARDB_OPT_CFLAGS += -fopt-info -fprofile-report -ftime-report
    POLARDB_OPT_CXXFLAGS += -fopt-info -fprofile-report -ftime-report
endif
endif
endif
