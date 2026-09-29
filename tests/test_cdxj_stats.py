#!/usr/bin/env python3
"""
Test suite for cdxj-stats orchestration (replay_cdxj_indexing_tools.stats.cdxj_stats).

Test Coverage:
--------------
1. --sink-<name> key=value option extraction from argv
2. Entry-point plugin discovery for built-in sinks
3. End-to-end run() over a fixture CDXJ, writing per-sink CSVs to --out-dir
4. Output filename suffixing when group-by is configured
5. Error handling: unknown sink, bad field separator
"""

import os
import tempfile
import unittest

from replay_cdxj_indexing_tools.stats.base import load_sinks
from replay_cdxj_indexing_tools.stats.cdxj_stats import extract_sink_options, run

FIXTURE = """\
pt,exemplo,blog)/post1 20190101000000 {"mime":"text/html","status":"200"}
pt,exemplo,www)/ 20200101000000 {"mime":"text/html","status":"200"}
pt,exemplo,www)/img.png 20200601000000 {"mime":"image/png","status":"404"}
pt,outro,www)/ 20180101000000 {"mime":"text/html","status":"200"}
"""


class TestExtractSinkOptions(unittest.TestCase):
    """Test the manual --sink-<name> key=value pre-parser."""

    def test_single_sink_single_option(self):
        options, remaining = extract_sink_options(
            ["--sink", "mimetype", "--sink-mimetype", "group-by=year", "--out-dir", "x"]
        )
        self.assertEqual(options, {"mimetype": {"group-by": "year"}})
        self.assertEqual(remaining, ["--sink", "mimetype", "--out-dir", "x"])

    def test_multiple_options_for_one_sink(self):
        options, _ = extract_sink_options(
            ["--sink-domain-host", "group-by=year", "extra=1", "--out-dir", "x"]
        )
        self.assertEqual(options, {"domain-host": {"group-by": "year", "extra": "1"}})

    def test_no_sink_options(self):
        options, remaining = extract_sink_options(["--sink", "mimetype", "--out-dir", "x"])
        self.assertEqual(options, {})
        self.assertEqual(remaining, ["--sink", "mimetype", "--out-dir", "x"])

    def test_invalid_option_without_equals_raises(self):
        with self.assertRaises(ValueError):
            extract_sink_options(["--sink-mimetype", "not-a-kv"])


class TestLoadSinks(unittest.TestCase):
    """Test that built-in sinks are discoverable via entry points."""

    def test_builtin_sinks_registered(self):
        sinks = load_sinks()
        self.assertIn("mimetype", sinks)
        self.assertIn("domain-host", sinks)
        self.assertIn("domain-etld1", sinks)


class TestRun(unittest.TestCase):
    """End-to-end tests for the run() orchestration function."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.input_path = os.path.join(self.temp_dir, "input.cdxj")
        with open(self.input_path, "w", encoding="utf-8") as f:
            f.write(FIXTURE)
        self.out_dir = os.path.join(self.temp_dir, "out")

    def test_single_sink_writes_expected_csv(self):
        counts = run(input_file=self.input_path, sink_names=["mimetype"], out_dir=self.out_dir)

        self.assertEqual(counts, {"mimetype": 2})
        out_path = os.path.join(self.out_dir, "mimetype.csv")
        self.assertTrue(os.path.exists(out_path))
        with open(out_path, encoding="utf-8") as f:
            content = f.read()
        self.assertIn("mime,count", content)
        self.assertIn("text/html,3", content)
        self.assertIn("image/png,1", content)

    def test_group_by_suffixes_output_filename(self):
        run(
            input_file=self.input_path,
            sink_names=["domain-host"],
            sink_options={"domain-host": {"group-by": "year"}},
            out_dir=self.out_dir,
        )

        self.assertTrue(os.path.exists(os.path.join(self.out_dir, "domain-host-year.csv")))
        self.assertFalse(os.path.exists(os.path.join(self.out_dir, "domain-host.csv")))

    def test_multiple_sinks_single_pass(self):
        counts = run(
            input_file=self.input_path,
            sink_names=["mimetype", "domain-host", "domain-etld1"],
            out_dir=self.out_dir,
        )

        self.assertEqual(set(counts), {"mimetype", "domain-host", "domain-etld1"})
        for sink_name, filename in (
            ("mimetype", "mimetype.csv"),
            ("domain-host", "domain-host.csv"),
            ("domain-etld1", "domain-etld1.csv"),
        ):
            self.assertTrue(os.path.exists(os.path.join(self.out_dir, filename)), sink_name)

        # exemplo.pt has two hosts (blog, www) but a single registered domain.
        # The domain field is CSV-quoted since it contains the delimiter itself.
        with open(os.path.join(self.out_dir, "domain-etld1.csv"), encoding="utf-8") as f:
            self.assertIn('"pt,exemplo,",3,2', f.read())

    def test_custom_field_separator(self):
        run(
            input_file=self.input_path,
            sink_names=["mimetype"],
            out_dir=self.out_dir,
            field_separator="\t",
        )
        with open(os.path.join(self.out_dir, "mimetype.csv"), encoding="utf-8") as f:
            content = f.read()
        self.assertIn("mime\tcount", content)

    def test_unknown_sink_raises(self):
        with self.assertRaises(ValueError):
            run(input_file=self.input_path, sink_names=["bogus"], out_dir=self.out_dir)

    def test_invalid_field_separator_raises(self):
        with self.assertRaises(ValueError):
            run(
                input_file=self.input_path,
                sink_names=["mimetype"],
                out_dir=self.out_dir,
                field_separator="::",
            )


if __name__ == "__main__":
    unittest.main()
