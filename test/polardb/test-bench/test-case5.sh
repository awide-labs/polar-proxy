#!/bin/bash
# shellcheck disable=SC2034 # Scenario globals are consumed by scenario_harness.sh.
#
# PolarDB short scenario: test-case5
#
# Purpose:
#   Validate the primary-only baseline. Reads are forced to the writer, so they
#   are always fresh and replica replay lag does not affect the result. This is
#   the writer-side control scenario for LSN offload comparisons.
#
# Configuration:
#   - consistency mode: eventual
#   - read target: primary
#   - transaction split: disabled
#   - expected outcome: success
#
# Usage:
#   ./test-case5.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/scenario_harness.sh"

CASE_NUM=5
CASE_NAME="Primary-only"
CONSISTENCY_MODE=eventual
READ_TARGET=primary
SPLIT_ENABLED=0
XACT_SPLIT=0
EXPECT_OUTCOME=success
TEST_ID=5

run_test "Case 5: Primary-only" run_consistency_test
