#!/bin/bash
# shellcheck disable=SC2034 # Scenario globals are consumed by scenario_harness.sh.
#
# PolarDB short scenario: test-case1
#
# Purpose:
#   Validate the eventual-consistency baseline. The proxy routes the read to the
#   reader without an LSN wait, so stale reads are acceptable by design. This is
#   the control scenario for comparing session-LSN and primary-only behavior.
#
# Configuration:
#   - consistency mode: off / eventual
#   - transaction split: disabled
#   - expected outcome: success
#
# Usage:
#   ./test-case1.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/scenario_harness.sh"

CASE_NUM=1
CASE_NAME="Eventual Consistency"
CONSISTENCY_MODE=eventual
SPLIT_ENABLED=0
XACT_SPLIT=0
EXPECT_OUTCOME=success
TEST_ID=1

run_test "Case 1: Eventual" run_consistency_test
