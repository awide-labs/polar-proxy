#!/bin/make -f

PROXYSQL_ROOT := $(shell while [ ! -f ./src/proxysql_global.cpp ]; do cd ..; done; pwd)
POLARDB_TEST_DIR := $(PROXYSQL_ROOT)/test/polardb
TEST_MAKE := $(MAKE) -C $(PROXYSQL_ROOT)/test
TRACE_ANALYZE_ARGS ?= --latest 1
COVERAGE_DIR ?= /tmp/proxysql-polardb-coverage
GCOVR ?= $(shell if command -v gcovr >/dev/null 2>&1; then \
		echo gcovr; \
	else \
		echo "python3 -m gcovr"; \
	fi)
GCOVR_AVAILABLE ?= $(shell $(GCOVR) --version >/dev/null 2>&1 && echo 1 || echo 0)
JOBS ?= $(shell nproc 2>/dev/null || echo 4)
POLARDB_COVERAGE_BUILD_TARGET ?= polardb-coverage-debug
POLARDB_TAKEN_MIN ?= 0
POLARDB_DEDICATED_TAKEN_MIN ?= $(POLARDB_TAKEN_MIN)
POLARDB_FUNCTION_TAKEN_MIN ?= 0
POLARDB_FUNCTION_MIN ?= 0
POLARDB_FUNCTION_TAKEN_EXCLUDES ?= \
	handle_async_check_cont.*lambda
POLARDB_UNIT_DIR := $(POLARDB_TEST_DIR)/test-unit
# Build products live outside the source dir: binaries in bin/, objects + gcov
# intermediates (.gcno/.gcda/.gcov) in build/.
POLARDB_UNIT_BIN_DIR := $(POLARDB_TEST_DIR)/bin
POLARDB_UNIT_BUILD_DIR := $(POLARDB_TEST_DIR)/build
POLARDB_LEGACY_UNIT_DIR := $(PROXYSQL_ROOT)/test/tap/tests/unit
POLARDB_UNIT_TESTS := \
	$(POLARDB_UNIT_BIN_DIR)/polardb_routing_lsn_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_protocol_parse_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_status_policy_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_query_state_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_startup_profile_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_hgm_lsn_unit-t
POLARDB_DEDICATED_IMPL_FILES := \
	PgSQL_PolarDB_Wrap.cpp \
	PgSQL_PolarDB_Flow.cpp \
	PgSQL_PolarDB_Consistency.cpp \
	PgSQL_PolarDB_Notices.cpp \
	PgSQL_PolarDB_Failure.cpp \
	PgSQL_PolarDB.cpp
POLARDB_DEDICATED_THRESHOLD_FILES := \
	PgSQL_PolarDB_Wrap.cpp \
	PgSQL_PolarDB_Flow.cpp \
	PgSQL_PolarDB_Notices.cpp \
	PgSQL_PolarDB_Failure.cpp \
	PgSQL_PolarDB.cpp
POLARDB_SHARED_IMPL_FILES := \
	PgSQL_Session.cpp \
	PgSQL_Connection.cpp \
	PgSQL_HostGroups_Manager.cpp \
	PgSQL_Monitor.cpp \
	PgSQL_Thread.cpp
POLARDB_IMPL_FILES := \
	$(POLARDB_DEDICATED_IMPL_FILES) \
	$(POLARDB_SHARED_IMPL_FILES)
POLARDB_GCOVR_FILTERS := \
	--filter '$(PROXYSQL_ROOT)/include/PgSQL_PolarDB.h' \
	--filter '$(PROXYSQL_ROOT)/include/PgSQL_Connection.h' \
	--filter '$(PROXYSQL_ROOT)/include/PgSQL_Data_Stream.h' \
	--filter '$(PROXYSQL_ROOT)/include/PgSQL_HostGroups_Manager.h' \
	--filter '$(PROXYSQL_ROOT)/include/PgSQL_Session.h' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_PolarDB.*\.cpp' \
	--exclude '$(PROXYSQL_ROOT)/lib/PgSQL_PolarDB_Stubs.cpp' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_Session.cpp' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_Connection.cpp' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_HostGroups_Manager.cpp' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_Monitor.cpp' \
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_Thread.cpp'

# Remove PolarDB-owned unit binaries and their gcov artifacts from a unit
# directory. Used by the clean/coverage targets so the "stale artifact" sweep
# lives in one place instead of being copy-pasted. $(1) is the directory.
define POLARDB_CLEAN_GCOV
	@find $(1) -maxdepth 1 \
		\( -name 'polardb_*_unit-t' -o -name '*polardb*.gcda' -o -name '*polardb*.gcno' \) -delete
endef

# Heavy benchmark targets: target name -> bench script basename (without .sh).
# The recipe is generated once below so bench1..bench4 stay in sync.
POLARDB_HEAVY_BENCHES := \
	bench1:bench1_lsn_stress \
	bench2:bench2_lsn_offload \
	bench3:bench3_replica_lag \
	bench4:bench4_loaded_primary

define POLARDB_HEAVY_BENCH_RULE
$(word 1,$(subst :, ,$(1))):
	$$(POLARDB_TEST_DIR)/test-bench/$(word 2,$(subst :, ,$(1))).sh $$(ARGS)
endef

.PHONY: help build clean \
	run-c-libpq-lsn run-c-extended-protocol \
	tap-config tap-lsn tap-wait-timeout tap tap-debug-trace \
	unit \
	test-case1 test-case2 test-case2-best-effort-success test-case2-best-effort-failure \
	test-case2-strict-success test-case2-strict-failure test-case5 test-cases \
	bench-case1 bench-case2 bench-case5 bench \
	bench1 bench2 bench3 bench4 bench-heavy \
	trace-analyze analyze-traces coverage-tools-unit coverage-parser-selftest coverage

.NOTPARALLEL: tap

help:
	@echo "PolarDB test targets:"
	@echo "  make -C test/polardb build                  build C helpers and bundled pgbench"
	@echo "  make -C test/polardb run-c-libpq-lsn        run direct libpq LSN helper"
	@echo "  make -C test/polardb run-c-extended-protocol QUERY='SELECT 1'  # requires configured ProxySQL"
	@echo "  make -C test/polardb tap-config             run config round-trip TAP"
	@echo "  make -C test/polardb tap-lsn                run main LSN TAP"
	@echo "  make -C test/polardb tap-wait-timeout       run wait-timeout TAP"
	@echo "  make -C test/polardb tap                    run all TAP scripts"
	@echo "  make -C test/polardb tap-debug-trace        run TAP scripts and debug trace analysis"
	@echo "  make -C test/polardb unit                   build and run PolarDB-owned unit tests"
	@echo "  make -C test/polardb coverage               run gcov build, TAP/unit tests, trace analysis, and reports"
	@echo "                                               coverage uses POLARDB_COVERAGE_BUILD_TARGET=polardb-coverage-debug (-O0)"
	@echo "                                               coverage percentage gates default to report-only"
	@echo "                                               set POLARDB_DEDICATED_TAKEN_MIN=N to gate dedicated files"
	@echo "                                               set POLARDB_FUNCTION_TAKEN_MIN=N to gate filtered PolarDB functions"
	@echo "                                               set POLARDB_FUNCTION_TAKEN_EXCLUDES='regex ...' for known staged gaps"
	@echo "                                               set POLARDB_FUNCTION_MIN=N to guard parser/filter sanity"
	@echo "  make -C test/polardb test-case1             run short eventual-consistency scenario"
	@echo "  make -C test/polardb test-case2 ARGS='-a -success'"
	@echo "  make -C test/polardb test-case5             run short primary-only scenario"
	@echo "  make -C test/polardb test-cases             run all short scenario variants"
	@echo "  make -C test/polardb bench                  compatibility alias for test-cases"
	@echo "  make -C test/polardb bench1                 run heavy LSN stress benchmark"
	@echo "  make -C test/polardb bench2                 run heavy LSN offload benchmark"
	@echo "  make -C test/polardb bench3                 run heavy replica-lag benchmark"
	@echo "  make -C test/polardb bench4                 run heavy loaded-primary benchmark"
	@echo "  make -C test/polardb bench-heavy            run all heavy benchmark scripts"
	@echo "  make -C test/polardb trace-analyze ARGS='--latest 6'"
	@echo ""
	@echo "Configure test/polardb/.env first, or set POLARDB_ENV_FILE=/path/to/env."

build:
	+$(TEST_MAKE) polardb

clean:
	+$(TEST_MAKE) clean-polardb
	$(call POLARDB_CLEAN_GCOV,$(POLARDB_LEGACY_UNIT_DIR))
	rm -rf $(POLARDB_UNIT_BIN_DIR) $(POLARDB_UNIT_BUILD_DIR) $(POLARDB_TEST_DIR)/test_output

run-c-libpq-lsn:
	$(POLARDB_TEST_DIR)/test-c/run_helper.sh libpq_lsn $(ARGS)

run-c-extended-protocol:
	$(POLARDB_TEST_DIR)/test-c/run_helper.sh extended_protocol "$(or $(QUERY),SELECT 1)"

tap-config:
	$(POLARDB_TEST_DIR)/test-tap/config_roundtrip_tap.sh

tap-lsn:
	$(POLARDB_TEST_DIR)/test-tap/lsn_session_consistency_tap.sh

tap-wait-timeout:
	$(POLARDB_TEST_DIR)/test-tap/wait_timeout_cleanup_tap.sh

tap: tap-config tap-lsn tap-wait-timeout

tap-debug-trace: tap-lsn tap-wait-timeout
	+$(MAKE) -C $(POLARDB_TEST_DIR) trace-analyze ARGS="$(or $(ARGS),$(TRACE_ANALYZE_ARGS))"

# Unit objects share one build directory across normal and WITHGCOV builds.
# Clean before the normal unit run so a previous coverage run cannot leave
# gcov-enabled objects that later fail to link without libgcov.
unit:
	+$(MAKE) -C $(POLARDB_UNIT_DIR) clean
	+$(MAKE) -C $(POLARDB_UNIT_DIR) -j$(JOBS)
	@for t in $(POLARDB_UNIT_TESTS); do "$$t"; done

test-case1:
	$(POLARDB_TEST_DIR)/test-bench/test-case1.sh $(ARGS)

test-case2:
	$(POLARDB_TEST_DIR)/test-bench/test-case2.sh $(ARGS)

test-case2-best-effort-success:
	$(POLARDB_TEST_DIR)/test-bench/test-case2.sh -a -success

test-case2-best-effort-failure:
	$(POLARDB_TEST_DIR)/test-bench/test-case2.sh -a -failure

test-case2-strict-success:
	$(POLARDB_TEST_DIR)/test-bench/test-case2.sh -b -success

test-case2-strict-failure:
	$(POLARDB_TEST_DIR)/test-bench/test-case2.sh -b -failure

test-case5:
	$(POLARDB_TEST_DIR)/test-bench/test-case5.sh $(ARGS)

bench-case1: test-case1
bench-case2: test-case2
bench-case5: test-case5

test-cases: test-case1 test-case2-best-effort-success test-case2-best-effort-failure \
	test-case2-strict-success test-case2-strict-failure test-case5

bench: test-cases

$(foreach b,$(POLARDB_HEAVY_BENCHES),$(eval $(call POLARDB_HEAVY_BENCH_RULE,$(b))))

bench-heavy: bench1 bench2 bench3 bench4

trace-analyze:
	python3 $(POLARDB_TEST_DIR)/tools/trace_analyzer.py $(ARGS)

analyze-traces: trace-analyze

coverage-tools-unit:
	python3 $(POLARDB_TEST_DIR)/tools/tests/coverage_summary_unit.py

coverage-parser-selftest: coverage-tools-unit

ifeq ($(GCOVR_AVAILABLE),0)
coverage: coverage-tools-unit
	@printf 'SKIP: PolarDB coverage requires gcovr; install gcovr or set GCOVR to a working command.\n'
else
coverage: coverage-tools-unit
	@mkdir -p "$(COVERAGE_DIR)"
	@find $(PROXYSQL_ROOT)/lib/obj -maxdepth 1 -name '*.gcda' -delete
	$(call POLARDB_CLEAN_GCOV,$(POLARDB_LEGACY_UNIT_DIR))
	@find $(POLARDB_UNIT_BUILD_DIR) -maxdepth 1 \
		\( -name '*.gcda' -o -name '*.gcno' \) -delete 2>/dev/null || true
	@find $(PROXYSQL_ROOT)/test/build -maxdepth 1 \
		-name '*test#polardb#test-unit#*' -delete 2>/dev/null || true
	+$(MAKE) -C $(PROXYSQL_ROOT) WITHGCOV=1 -j$(JOBS) $(POLARDB_COVERAGE_BUILD_TARGET)
	+PROXYSQL_STOP_GRACE_SECONDS=10 $(MAKE) -C $(POLARDB_TEST_DIR) tap-config
	+PROXYSQL_STOP_GRACE_SECONDS=10 $(MAKE) -C $(POLARDB_TEST_DIR) tap-debug-trace
	+$(MAKE) -C $(POLARDB_UNIT_DIR) clean
	@find $(POLARDB_UNIT_BUILD_DIR) -maxdepth 1 \
		\( -name '*polardb*.gcda' -o -name '*polardb*.gcno' \) -delete 2>/dev/null || true
	+$(MAKE) -C $(POLARDB_UNIT_DIR) WITHGCOV=1 -j$(JOBS)
	@(cd $(POLARDB_UNIT_DIR) && for t in $(POLARDB_UNIT_TESTS); do "$$t"; done)
	$(GCOVR) -r $(PROXYSQL_ROOT) $(POLARDB_GCOVR_FILTERS) \
		--gcov-ignore-parse-errors negative_hits.warn_once_per_file \
		--json-summary "$(COVERAGE_DIR)/focused-summary.json" \
		--txt "$(COVERAGE_DIR)/focused.txt" \
		--html-details "$(COVERAGE_DIR)/focused.html"
	@(cd $(PROXYSQL_ROOT)/lib && \
		gcov -m -f -b -c -o obj $(POLARDB_IMPL_FILES) > "$(COVERAGE_DIR)/gcov-polardb-implementation.txt")
	python3 $(POLARDB_TEST_DIR)/tools/coverage_summary.py \
		--input "$(COVERAGE_DIR)/gcov-polardb-implementation.txt" \
		--output "$(COVERAGE_DIR)/implementation-summary.txt" \
		--function-output "$(COVERAGE_DIR)/function-summary.txt" \
		--gcov-dir "$(PROXYSQL_ROOT)/lib" \
		--min-taken "$(POLARDB_DEDICATED_TAKEN_MIN)" \
		--min-function-taken "$(POLARDB_FUNCTION_TAKEN_MIN)" \
		--min-functions "$(POLARDB_FUNCTION_MIN)" \
		$(foreach e,$(POLARDB_FUNCTION_TAKEN_EXCLUDES),--function-threshold-exclude "$(e)") \
		$(foreach f,$(POLARDB_IMPL_FILES),--required-file "$(f)") \
		$(foreach f,$(POLARDB_DEDICATED_THRESHOLD_FILES),--threshold-file "$(f)")
	@find $(PROXYSQL_ROOT)/lib -maxdepth 1 -name '*.gcov' -delete
	@printf 'PolarDB coverage artifacts written to %s\n' "$(COVERAGE_DIR)"
endif
