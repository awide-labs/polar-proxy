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
POLARDB_COVERAGE_OBJECT_DIR ?= obj
POLARDB_TAKEN_MIN ?= 0
POLARDB_DEDICATED_TAKEN_MIN ?= $(POLARDB_TAKEN_MIN)
POLARDB_FUNCTION_TAKEN_MIN ?= 0
POLARDB_FUNCTION_MIN ?= 0
POLARDB_FUNCTION_TAKEN_EXCLUDES ?= \
	handle_async_check_cont.*lambda
POLARDB_UNIT_DIR := $(POLARDB_TEST_DIR)/test-unit
POLARDB_PGO_DIR := $(POLARDB_TEST_DIR)/pgo
POLARDB_BOLT_BINARY ?= $(PROXYSQL_ROOT)/src/proxysql.bolt.inst
# Build products live outside the source dir: binaries in bin/, objects + gcov
# intermediates (.gcno/.gcda/.gcov) in build/.
POLARDB_UNIT_BIN_DIR := $(POLARDB_TEST_DIR)/bin
POLARDB_UNIT_BUILD_DIR := $(POLARDB_TEST_DIR)/build
POLARDB_LEGACY_UNIT_DIR := $(PROXYSQL_ROOT)/test/tap/tests/unit
POLARDB_RUN_LOGGED := $(POLARDB_TEST_DIR)/tools/run_logged.sh $(POLARDB_TEST_DIR)
POLARDB_TAP_RUNNER := $(POLARDB_TEST_DIR)/tools/tap_parallel.sh
POLARDB_TAP_PARALLEL_JOBS ?= $(JOBS)
POLARDB_UNIT_TESTS := \
	$(POLARDB_UNIT_BIN_DIR)/polardb_routing_lsn_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_protocol_parse_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_status_policy_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_query_state_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_startup_profile_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/pgsql_status_variables_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_client_rfq_unit-t \
	$(POLARDB_UNIT_BIN_DIR)/polardb_hgm_lsn_unit-t
POLARDB_DEDICATED_IMPL_FILES := \
	PgSQL_PolarDB_Wrap.cpp \
	PgSQL_PolarDB_Flow.cpp \
	PgSQL_PolarDB_Consistency.cpp \
	PgSQL_PolarDB_Notices.cpp \
	PgSQL_PolarDB_Failure.cpp \
	PgSQL_PolarDB_Split.cpp \
	PgSQL_PolarDB.cpp
POLARDB_DEDICATED_THRESHOLD_FILES := \
	PgSQL_PolarDB_Wrap.cpp \
	PgSQL_PolarDB_Flow.cpp \
	PgSQL_PolarDB_Notices.cpp \
	PgSQL_PolarDB_Failure.cpp \
	PgSQL_PolarDB_Split.cpp \
	PgSQL_PolarDB.cpp
POLARDB_SHARED_IMPL_FILES := \
	PgSQL_Data_Stream.cpp \
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
	--filter '$(PROXYSQL_ROOT)/lib/PgSQL_Data_Stream.cpp' \
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
	bench4:bench4_loaded_primary \
	bench5:bench5_consistency_shapes

define POLARDB_HEAVY_BENCH_RULE
$(word 1,$(subst :, ,$(1))):
	$$(POLARDB_RUN_LOGGED) $(word 1,$(subst :, ,$(1))) $$(POLARDB_TEST_DIR)/test-bench/$(word 2,$(subst :, ,$(1))).sh $$(ARGS)
endef

.PHONY: help build clean \
	run-c-libpq-lsn run-c-libpq-xact run-c-libpq-row-run run-c-extended-protocol \
	run-pgbench-lsn run-sysbench-lsn \
	build-pgbench-polar-rfq build-sysbench-polar-rfq rfq-bench-tools-check \
	tap-config tap-lsn tap-rfq-lifecycle tap-global-lsn tap-split tap-split-failure-policy tap-wait-timeout \
	tap-parallel-list tap-parallel-safe tap-exclusive tap-parallel tap tap-debug-trace \
	unit \
	test-case1 test-case2 test-case2-best-effort-success test-case2-best-effort-failure \
	test-case2-strict-success test-case2-strict-failure test-case5 test-cases \
	bench-case1 bench-case2 bench-case5 bench \
	bench1 bench2 bench3 bench4 bench5 bench5-report bench-heavy \
	pgo-hotpath-smoke pgo-hotpath-core pgo-hotpath-full pgo-hotpath-edge \
	pgo-train-smoke pgo-train-core pgo-train-full \
	bolt-hotpath-smoke bolt-hotpath-core bolt-hotpath-full bolt-hotpath-edge \
	bolt-train-smoke bolt-train-core bolt-train-full \
	trace-analyze analyze-traces coverage-tools-unit coverage-parser-selftest coverage

.NOTPARALLEL: tap tap-debug-trace

help:
	@echo "PolarDB test targets:"
	@echo "  make -C test/polardb build                  build C helpers and bundled pgbench"
	@echo "  make -C test/polardb build-pgbench-polar-rfq build native pgbench RFQ-LSN binary"
	@echo "  make -C test/polardb build-sysbench-polar-rfq build optional sysbench RFQ-LSN binary"
	@echo "                                               initialize with: git submodule update --init --checkout deps/sysbench-polar-rfq"
	@echo "  make -C test/polardb rfq-bench-tools-check  build and check both native RFQ tools"
	@echo "  make -C test/polardb run-c-libpq-lsn        run direct libpq LSN helper"
	@echo "  make -C test/polardb run-c-libpq-xact       run direct libpq xact RFQ helper"
	@echo "                                               ARGS=--require-w-marker requires backend 'w' RFQ support"
	@echo "  make -C test/polardb run-c-extended-protocol QUERY='SELECT 1'  # requires configured ProxySQL"
	@echo "  make -C test/polardb run-pgbench-lsn        run the pgbench RFQ-LSN consistency check"
	@echo "  make -C test/polardb run-sysbench-lsn       run the sysbench RFQ-LSN consistency check"
	@echo "  make -C test/polardb tap-config             run config round-trip TAP"
	@echo "  make -C test/polardb tap-lsn                run main LSN TAP"
	@echo "  make -C test/polardb tap-rfq-lifecycle      run focused RFQ-LSN result lifecycle TAP"
	@echo "  make -C test/polardb tap-global-lsn         run global LSN consistency TAP"
	@echo "  make -C test/polardb tap-split              run transaction-split TAP"
	@echo "  make -C test/polardb tap-split-failure-policy run reader-failure policy TAP"
	@echo "  make -C test/polardb tap-wait-timeout       run wait-timeout TAP"
	@echo "  make -C test/polardb tap-parallel           run parallel-safe TAP groups, then backend-exclusive groups"
	@echo "  make -C test/polardb tap-parallel-safe      run only parallel-safe TAP groups"
	@echo "  make -C test/polardb tap-exclusive          run only backend-exclusive TAP groups sequentially"
	@echo "  make -C test/polardb tap-parallel-list      show TAP group scheduling metadata"
	@echo "  make -C test/polardb tap                    run all TAP scripts"
	@echo "  make -C test/polardb tap-debug-trace        run TAP scripts and debug trace analysis"
	@echo "  make -C test/polardb unit                   build and run PolarDB-owned unit tests"
	@echo "  make -C test/polardb coverage               run gcov build, TAP/unit tests, trace analysis, and reports"
	@echo "                                               coverage uses POLARDB_COVERAGE_BUILD_TARGET=polardb-coverage-debug (-O0)"
	@echo "                                               coverage percentage controls default to report-only"
	@echo "                                               set POLARDB_DEDICATED_TAKEN_MIN=N to enforce coverage floors for dedicated files"
	@echo "                                               set POLARDB_FUNCTION_TAKEN_MIN=N to enforce coverage floors for filtered PolarDB functions"
	@echo "                                               set POLARDB_FUNCTION_TAKEN_EXCLUDES='regex ...' for known staged gaps"
	@echo "                                               set POLARDB_FUNCTION_MIN=N to check parser/filter sanity"
	@echo "  make -C test/polardb test-case1             run short eventual-consistency scenario"
	@echo "  make -C test/polardb test-case2 ARGS='-a -success'"
	@echo "  make -C test/polardb test-case5             run short primary-only scenario"
	@echo "  make -C test/polardb test-cases             run all short scenario variants"
	@echo "  make -C test/polardb bench                  compatibility alias for test-cases"
	@echo "  make -C test/polardb bench1                 run heavy LSN stress benchmark"
	@echo "  make -C test/polardb bench2                 run heavy LSN offload benchmark"
	@echo "  make -C test/polardb bench3                 run heavy replica-lag benchmark"
	@echo "  make -C test/polardb bench4                 run heavy loaded-primary benchmark"
	@echo "  make -C test/polardb bench5                 run consistency scenario-shape matrix"
	@echo "  make -C test/polardb bench5-report ARGS='554 --view all --sort speedup --desc'"
	@echo "  make -C test/polardb bench-heavy            run all heavy benchmark scripts"
	@echo "  make -C test/polardb pgo-hotpath-core       run the representative mixed hot-path workload"
	@echo "  make -C test/polardb pgo-train-smoke        run short PolarDB PGO training workload"
	@echo "  make -C test/polardb pgo-train-core         run core PolarDB PGO training workload"
	@echo "  make -C test/polardb pgo-train-full         run full PolarDB PGO training workload"
	@echo "  make -C test/polardb bolt-hotpath-core      run BOLT-instrumented hot-path profile workload"
	@echo "  make -C test/polardb bolt-train-smoke       run BOLT-instrumented short workload"
	@echo "  make -C test/polardb bolt-train-core        run BOLT-instrumented core workload"
	@echo "  make -C test/polardb bolt-train-full        run BOLT-instrumented full workload"
	@echo "  make -C test/polardb trace-analyze ARGS='--latest 6'"
	@echo "  Test targets write logs to test/polardb/test_output/run_NNNN_make_<target>/make.log"
	@echo "  Set POLARDB_MAKE_OUTPUT_DIR=/path to place make-run logs elsewhere"
	@echo "  POLARDB_TEST_SHARD=N shifts default ProxySQL ports/data dirs for parallel-capable external runners"
	@echo ""
	@echo "Configure test/polardb/.env first, or set POLARDB_ENV_FILE=/path/to/env."

build:
	+$(TEST_MAKE) polardb

build-pgbench-polar-rfq:
	+$(TEST_MAKE) polardb-pgbench-polar-rfq

build-sysbench-polar-rfq:
	+$(TEST_MAKE) polardb-sysbench-polar-rfq

rfq-bench-tools-check:
	+$(TEST_MAKE) polardb-rfq-bench-tools-check

clean:
	+$(TEST_MAKE) clean-polardb
	$(call POLARDB_CLEAN_GCOV,$(POLARDB_LEGACY_UNIT_DIR))
	rm -rf $(POLARDB_UNIT_BIN_DIR) $(POLARDB_UNIT_BUILD_DIR) $(POLARDB_TEST_DIR)/test_output

run-c-libpq-lsn:
	$(POLARDB_RUN_LOGGED) run-c-libpq-lsn $(POLARDB_TEST_DIR)/test-c/run_helper.sh libpq_lsn $(ARGS)

run-c-libpq-xact:
	$(POLARDB_RUN_LOGGED) run-c-libpq-xact $(POLARDB_TEST_DIR)/test-c/run_helper.sh libpq_xact $(ARGS)

run-c-libpq-row-run:
	$(POLARDB_RUN_LOGGED) run-c-libpq-row-run $(POLARDB_TEST_DIR)/test-c/run_helper.sh libpq_row_run $(ARGS)

run-c-extended-protocol:
	$(POLARDB_RUN_LOGGED) run-c-extended-protocol $(POLARDB_TEST_DIR)/test-c/run_helper.sh extended_protocol "$(or $(QUERY),SELECT 1)"

run-pgbench-lsn:
	$(POLARDB_RUN_LOGGED) run-pgbench-lsn $(POLARDB_TEST_DIR)/tools/run_pgbench_lsn_consistency.sh

run-sysbench-lsn:
	$(POLARDB_RUN_LOGGED) run-sysbench-lsn $(POLARDB_TEST_DIR)/tools/run_sysbench_lsn_consistency.sh

tap-config:
	$(POLARDB_RUN_LOGGED) tap-config $(POLARDB_TEST_DIR)/test-tap/config_roundtrip_tap.sh

tap-lsn:
	$(POLARDB_RUN_LOGGED) tap-lsn $(POLARDB_TEST_DIR)/test-tap/lsn_session_consistency_tap.sh

tap-rfq-lifecycle:
	$(POLARDB_RUN_LOGGED) tap-rfq-lifecycle $(POLARDB_TEST_DIR)/test-tap/rfq_lsn_lifecycle_tap.sh

tap-global-lsn:
	$(POLARDB_RUN_LOGGED) tap-global-lsn $(POLARDB_TEST_DIR)/test-tap/global_lsn_consistency_tap.sh

tap-split:
	$(POLARDB_RUN_LOGGED) tap-split $(POLARDB_TEST_DIR)/test-tap/txn_split_tap.sh

tap-split-failure-policy:
	$(POLARDB_RUN_LOGGED) tap-split-failure-policy $(POLARDB_TEST_DIR)/test-tap/txn_split_failure_policy_tap.sh

tap-wait-timeout:
	$(POLARDB_RUN_LOGGED) tap-wait-timeout $(POLARDB_TEST_DIR)/test-tap/wait_timeout_cleanup_tap.sh

tap-parallel-list:
	$(POLARDB_TAP_RUNNER) list

tap-parallel-safe:
	$(POLARDB_RUN_LOGGED) tap-parallel-safe env POLARDB_TAP_PARALLEL_JOBS="$(POLARDB_TAP_PARALLEL_JOBS)" $(POLARDB_TAP_RUNNER) safe

tap-exclusive:
	$(POLARDB_RUN_LOGGED) tap-exclusive env POLARDB_TAP_PARALLEL_JOBS="$(POLARDB_TAP_PARALLEL_JOBS)" $(POLARDB_TAP_RUNNER) exclusive

tap-parallel:
	$(POLARDB_RUN_LOGGED) tap-parallel env POLARDB_TAP_PARALLEL_JOBS="$(POLARDB_TAP_PARALLEL_JOBS)" $(POLARDB_TAP_RUNNER) all

tap: tap-config tap-lsn tap-rfq-lifecycle tap-global-lsn tap-split tap-split-failure-policy tap-wait-timeout

tap-debug-trace: tap-lsn tap-rfq-lifecycle tap-global-lsn tap-split tap-split-failure-policy tap-wait-timeout
	+$(MAKE) -C $(POLARDB_TEST_DIR) trace-analyze ARGS="$(or $(ARGS),$(TRACE_ANALYZE_ARGS))"

# Unit objects share one build directory across normal and WITHGCOV builds.
# Clean before the normal unit run so a previous coverage run cannot leave
# gcov-enabled objects that later fail to link without libgcov.
unit:
	$(POLARDB_RUN_LOGGED) unit bash -o pipefail -c 'make -C "$(POLARDB_UNIT_DIR)" clean && make -C "$(POLARDB_UNIT_DIR)" -j$(JOBS) && for t in $(POLARDB_UNIT_TESTS); do "$$t"; done'

test-case1:
	$(POLARDB_RUN_LOGGED) test-case1 $(POLARDB_TEST_DIR)/test-bench/test-case1.sh $(ARGS)

test-case2:
	$(POLARDB_RUN_LOGGED) test-case2 $(POLARDB_TEST_DIR)/test-bench/test-case2.sh $(ARGS)

test-case2-best-effort-success:
	$(POLARDB_RUN_LOGGED) test-case2-best-effort-success $(POLARDB_TEST_DIR)/test-bench/test-case2.sh -a -success

test-case2-best-effort-failure:
	$(POLARDB_RUN_LOGGED) test-case2-best-effort-failure $(POLARDB_TEST_DIR)/test-bench/test-case2.sh -a -failure

test-case2-strict-success:
	$(POLARDB_RUN_LOGGED) test-case2-strict-success $(POLARDB_TEST_DIR)/test-bench/test-case2.sh -b -success

test-case2-strict-failure:
	$(POLARDB_RUN_LOGGED) test-case2-strict-failure $(POLARDB_TEST_DIR)/test-bench/test-case2.sh -b -failure

test-case5:
	$(POLARDB_RUN_LOGGED) test-case5 $(POLARDB_TEST_DIR)/test-bench/test-case5.sh $(ARGS)

bench-case1: test-case1
bench-case2: test-case2
bench-case5: test-case5

test-cases: test-case1 test-case2-best-effort-success test-case2-best-effort-failure \
	test-case2-strict-success test-case2-strict-failure test-case5

bench: test-cases

$(foreach b,$(POLARDB_HEAVY_BENCHES),$(eval $(call POLARDB_HEAVY_BENCH_RULE,$(b))))

bench5-report:
	$(POLARDB_RUN_LOGGED) bench5-report $(POLARDB_TEST_DIR)/tools/bench5_report.py $(ARGS)

bench-heavy: bench1 bench2 bench3 bench4

pgo-hotpath-smoke:
	$(POLARDB_RUN_LOGGED) pgo-hotpath-smoke $(POLARDB_PGO_DIR)/hotpath_worker.sh smoke $(ARGS)

pgo-hotpath-core:
	$(POLARDB_RUN_LOGGED) pgo-hotpath-core $(POLARDB_PGO_DIR)/hotpath_worker.sh core $(ARGS)

pgo-hotpath-full:
	$(POLARDB_RUN_LOGGED) pgo-hotpath-full $(POLARDB_PGO_DIR)/hotpath_worker.sh full $(ARGS)

pgo-hotpath-edge:
	$(POLARDB_RUN_LOGGED) pgo-hotpath-edge $(POLARDB_PGO_DIR)/hotpath_worker.sh edge $(ARGS)

pgo-train-smoke:
	$(POLARDB_RUN_LOGGED) pgo-train-smoke $(POLARDB_PGO_DIR)/train.sh smoke $(ARGS)

pgo-train-core:
	$(POLARDB_RUN_LOGGED) pgo-train-core $(POLARDB_PGO_DIR)/train.sh core $(ARGS)

pgo-train-full:
	$(POLARDB_RUN_LOGGED) pgo-train-full $(POLARDB_PGO_DIR)/train.sh full $(ARGS)

bolt-hotpath-smoke:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-hotpath-smoke env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/hotpath_worker.sh smoke $(ARGS)

bolt-hotpath-core:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-hotpath-core env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/hotpath_worker.sh core $(ARGS)

bolt-hotpath-full:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-hotpath-full env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/hotpath_worker.sh full $(ARGS)

bolt-hotpath-edge:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-hotpath-edge env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/hotpath_worker.sh edge $(ARGS)

bolt-train-smoke:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-train-smoke env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/train.sh smoke $(ARGS)

bolt-train-core:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-train-core env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/train.sh core $(ARGS)

bolt-train-full:
	@test -x "$(POLARDB_BOLT_BINARY)" || { echo "Missing BOLT-instrumented binary: $(POLARDB_BOLT_BINARY)" >&2; exit 1; }
	$(POLARDB_RUN_LOGGED) bolt-train-full env PROXYSQL_BINARY="$(POLARDB_BOLT_BINARY)" $(POLARDB_PGO_DIR)/train.sh full $(ARGS)

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
	@find $(PROXYSQL_ROOT)/lib/$(POLARDB_COVERAGE_OBJECT_DIR) -maxdepth 1 \
		-name '*.gcda' -delete 2>/dev/null || true
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
		gcov -m -f -b -c -o $(POLARDB_COVERAGE_OBJECT_DIR) $(POLARDB_IMPL_FILES) > "$(COVERAGE_DIR)/gcov-polardb-implementation.txt")
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
