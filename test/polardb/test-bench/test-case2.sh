#!/bin/bash
# shellcheck disable=SC2034 # Scenario globals are consumed by scenario_harness.sh.
#
# PolarDB short scenario: test-case2
#
# Purpose:
#   Validate session-LSN consistency. A write advances the session LSN and the
#   following read is protected by the LSN wait path unless policy or topology
#   forces the writer. Success variants run without induced lag; failure variants
#   hold replica replay behind the target. best_effort surfaces the timeout as a
#   WARNING and may return stale reader data. strict retries the read on the
#   writer when no user result has reached the client.
#
# Configuration:
#   - consistency mode: session LSN
#   - transaction split: disabled
#   - wait mode and expected outcome: selected by command-line arguments
#
# Usage:
#   ./test-case2.sh -a -success   # best_effort, no lag
#   ./test-case2.sh -a -failure   # best_effort, with lag -> WARNING
#   ./test-case2.sh -b -success   # strict, no lag
#   ./test-case2.sh -b -failure   # strict, with lag -> writer retry
#   ./test-case2.sh -a -s         # best_effort, no lag (shortcut)
#   ./test-case2.sh -b -f         # strict, with lag -> writer retry (shortcut)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/scenario_harness.sh"
parse_common_args "$@"

CASE_NUM=2
CASE_NAME="Session LSN"
CONSISTENCY_MODE=1
SPLIT_ENABLED=0
XACT_SPLIT=0
TEST_ID=2

run_test "Case 2: Session LSN ($POLAR_MODE, $EXPECT_OUTCOME)" run_consistency_test
