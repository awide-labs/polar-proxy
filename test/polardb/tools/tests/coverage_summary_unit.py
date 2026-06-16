#!/usr/bin/env python3
"""Fixture checks for the PolarDB coverage summary parser."""

import sys
import tempfile
import unittest
import re
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import coverage_summary


class GcovTakenSummaryTest(unittest.TestCase):
    def test_function_parser_counts_lines_branches_and_calls(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gcov_dir = Path(tmpdir)
            (gcov_dir / "PgSQL_PolarDB_Fixture.cpp.gcov").write_text(
                "\n".join([
                    "        -:    0:Source:PgSQL_PolarDB_Fixture.cpp",
                    "function polardb_fixture_ok called 1 returned 100% blocks executed 80%",
                    "        1:   10:int polardb_fixture_ok() {",
                    "        1:   11:  if (true) return 1;",
                    "branch  0 taken 1",
                    "branch  1 taken 0",
                    "call    0 returned 1",
                    "        1:   12:}",
                    "function polardb_fixture_miss called 0 returned 0% blocks executed 0%",
                    "    #####:   20:int polardb_fixture_miss() {",
                    "branch  0 never executed",
                    "call    0 never executed",
                    "    #####:   21:  return 0;",
                    "    #####:   22:}",
                    "function std::vector<int>::noise called 1 returned 100% blocks executed 100%",
                    "        1:   30:void std_noise() {}",
                    "",
                ]),
                encoding="utf-8")

            functions = coverage_summary.parse_function_gcov(
                gcov_dir, ["PgSQL_PolarDB_Fixture.cpp"], "polardb")

        self.assertEqual(
            ["polardb_fixture_miss", "polardb_fixture_ok"],
            [function["name"] for function in functions])

        miss, ok = functions
        self.assertEqual(3, miss["executable_lines"])
        self.assertEqual(0, miss["executed_lines"])
        self.assertEqual(1, miss["branches"])
        self.assertEqual(0, miss["branches_taken"])
        self.assertEqual(1, miss["calls"])
        self.assertEqual(0, miss["calls_taken"])
        self.assertEqual(0.0, miss["line_taken_pct"])
        self.assertEqual(0.0, miss["branch_taken_pct"])

        self.assertEqual(3, ok["executable_lines"])
        self.assertEqual(3, ok["executed_lines"])
        self.assertEqual(2, ok["branches"])
        self.assertEqual(1, ok["branches_taken"])
        self.assertEqual(1, ok["calls"])
        self.assertEqual(1, ok["calls_taken"])
        self.assertEqual(100.0, ok["line_taken_pct"])
        self.assertEqual(50.0, ok["branch_taken_pct"])
        self.assertEqual(100.0, ok["call_taken_pct"])

    def test_function_threshold_exclusion_matches_source_and_name(self):
        function = {
            "source": "PgSQL_PolarDB_Fixture.cpp",
            "name": "polardb_fixture_deferred()",
        }
        excluded = [re.compile("deferred")]
        not_excluded = [re.compile("other")]

        self.assertTrue(coverage_summary.function_threshold_excluded(
            function, excluded))
        self.assertFalse(coverage_summary.function_threshold_excluded(
            function, not_excluded))

    def test_parse_gcov_extracts_per_file_summary_metrics(self):
        # parse_gcov() drives the --min-taken gate, so pin its top-level
        # `File '...'` + `<Metric> executed:X%` extraction. Lines outside a
        # File block (no `current`) and unrecognized lines must be ignored.
        with tempfile.NamedTemporaryFile(
                "w", suffix=".txt", delete=False, encoding="utf-8") as handle:
            handle.write("\n".join([
                "Lines executed:99.99% of 1  (orphan metric, no File yet)",
                "File 'lib/PgSQL_PolarDB_Wrap.cpp'",
                "Lines executed:80.00% of 100",
                "Branches executed:70.00% of 50",
                "Taken at least once:60.00% of 50",
                "Calls executed:90.00% of 20",
                "some unrelated diagnostic line",
                "File 'lib/PgSQL_PolarDB_Flow.cpp'",
                "Lines executed:55.50% of 200",
                "Taken at least once:25.00% of 40",
                "",
            ]))
            input_path = Path(handle.name)
        try:
            files = coverage_summary.parse_gcov(input_path)
        finally:
            input_path.unlink()

        self.assertEqual(
            {"lib/PgSQL_PolarDB_Wrap.cpp", "lib/PgSQL_PolarDB_Flow.cpp"},
            set(files))
        wrap = files["lib/PgSQL_PolarDB_Wrap.cpp"]
        self.assertEqual(80.00, wrap["Lines executed"])
        self.assertEqual(70.00, wrap["Branches executed"])
        self.assertEqual(60.00, wrap["Taken at least once"])
        self.assertEqual(90.00, wrap["Calls executed"])
        flow = files["lib/PgSQL_PolarDB_Flow.cpp"]
        self.assertEqual(55.50, flow["Lines executed"])
        self.assertEqual(25.00, flow["Taken at least once"])
        # Flow had no Branches/Calls lines: those keys stay absent (the gate
        # treats missing metrics as None / "n/a").
        self.assertNotIn("Branches executed", flow)
        self.assertNotIn("Calls executed", flow)

    def test_function_fallback_re_sets_blocks_pct_none(self):
        # gcov output for some functions lacks the "blocks executed N%" tail.
        # FUNCTION_FALLBACK_RE must still match those and yield blocks_pct=None,
        # which format_pct() renders as "n/a".
        with tempfile.TemporaryDirectory() as tmpdir:
            gcov_dir = Path(tmpdir)
            (gcov_dir / "PgSQL_PolarDB_Fixture.cpp.gcov").write_text(
                "\n".join([
                    "        -:    0:Source:PgSQL_PolarDB_Fixture.cpp",
                    "function polardb_no_blocks called 2 returned 100%",
                    "        2:   10:void polardb_no_blocks() {",
                    "        2:   11:  do_work();",
                    "call    0 returned 2",
                    "        2:   12:}",
                    "",
                ]),
                encoding="utf-8")

            functions = coverage_summary.parse_function_gcov(
                gcov_dir, ["PgSQL_PolarDB_Fixture.cpp"], "polardb")

        self.assertEqual(1, len(functions))
        function = functions[0]
        self.assertEqual("polardb_no_blocks", function["name"])
        self.assertEqual("2", function["called"])
        self.assertEqual(100.0, function["returned_pct"])
        self.assertIsNone(function["blocks_pct"])
        self.assertEqual("n/a", coverage_summary.format_pct(function["blocks_pct"]))
        # The rest of the per-function tallies still accumulate normally.
        self.assertEqual(3, function["executable_lines"])
        self.assertEqual(3, function["executed_lines"])
        self.assertEqual(1, function["calls"])
        self.assertEqual(1, function["calls_taken"])

    def test_source_switch_gates_function_inclusion(self):
        # A mid-file `-: 0:Source:OTHER.cpp` switch (inlined code from another
        # translation unit) must gate subsequent functions: only functions
        # whose active source is in the requested source set are kept.
        with tempfile.TemporaryDirectory() as tmpdir:
            gcov_dir = Path(tmpdir)
            (gcov_dir / "PgSQL_PolarDB_Fixture.cpp.gcov").write_text(
                "\n".join([
                    "        -:    0:Source:PgSQL_PolarDB_Fixture.cpp",
                    "function polardb_in_scope called 1 returned 100% blocks executed 100%",
                    "        1:   10:int polardb_in_scope() { return 1; }",
                    "        -:    0:Source:OtherHeaderInline.cpp",
                    "function polardb_out_of_scope called 1 returned 100% blocks executed 100%",
                    "        1:   20:int polardb_out_of_scope() { return 2; }",
                    "        -:    0:Source:PgSQL_PolarDB_Fixture.cpp",
                    "function polardb_back_in_scope called 1 returned 100% blocks executed 100%",
                    "        1:   30:int polardb_back_in_scope() { return 3; }",
                    "",
                ]),
                encoding="utf-8")

            functions = coverage_summary.parse_function_gcov(
                gcov_dir, ["PgSQL_PolarDB_Fixture.cpp"], "polardb")

        # polardb_out_of_scope is dropped because its active source switched to
        # OtherHeaderInline.cpp, which is not in source_files.
        self.assertEqual(
            ["polardb_back_in_scope", "polardb_in_scope"],
            [function["name"] for function in functions])


if __name__ == "__main__":
    unittest.main()
