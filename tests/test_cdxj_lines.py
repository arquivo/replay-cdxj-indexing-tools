#!/usr/bin/env python3
"""
Tests for replay_cdxj_indexing_tools.cdxj_lines - shared CDXJ input line filtering.

Covers corrupt-line detection (NUL bytes, missing key before the JSON block) on both
str and bytes lines, silent skipping of blank lines, stderr warnings naming the input
and line number, the per-run warning cap, and the final summary (issue #111).
"""

import contextlib
import io
import unittest

from replay_cdxj_indexing_tools.cdxj_lines import CdxjLineFilter, corrupt_reason

VALID = 'pt,a)/ 20200101000000 {"url": "http://a.pt/"}\n'
NUL_PREFIXED = "\x00" * 50 + '{"url": "http://a.pt/"}\n'
JSON_ONLY = '{"url": "http://a.pt/"}\n'


class TestCorruptReason(unittest.TestCase):
    """Tests for corrupt_reason()."""

    def test_valid_line(self):
        self.assertIsNone(corrupt_reason(VALID))
        self.assertIsNone(corrupt_reason(VALID.encode()))

    def test_line_without_json_is_valid(self):
        self.assertIsNone(corrupt_reason("pt,a)/ 20200101000000\n"))

    def test_nul_bytes(self):
        self.assertEqual(corrupt_reason(NUL_PREFIXED), "50 NUL bytes")
        self.assertEqual(corrupt_reason(NUL_PREFIXED.encode()), "50 NUL bytes")

    def test_nul_only_line(self):
        self.assertEqual(corrupt_reason(b"\x00\x00\n"), "2 NUL bytes")

    def test_missing_key(self):
        self.assertEqual(corrupt_reason(JSON_ONLY), "no key before the JSON block")
        self.assertEqual(corrupt_reason(b'  {"a": 1}\n'), "no key before the JSON block")


class TestCdxjLineFilter(unittest.TestCase):
    """Tests for CdxjLineFilter.accept() and report()."""

    def run_filter(self, lines, max_warnings=10):
        """Feed lines through a filter; return (accepted lines, stderr text)."""
        line_filter = CdxjLineFilter(max_warnings=max_warnings)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            accepted = [
                line
                for lineno, line in enumerate(lines, 1)
                if line_filter.accept(line, "in.cdxj", lineno)
            ]
            line_filter.report()
        return accepted, stderr.getvalue()

    def test_valid_lines_kept_silently(self):
        accepted, stderr = self.run_filter([VALID, VALID.encode()])
        self.assertEqual(accepted, [VALID, VALID.encode()])
        self.assertEqual(stderr, "")

    def test_blank_lines_skipped_silently(self):
        accepted, stderr = self.run_filter(["\n", "  \t\n", b"\r\n", VALID])
        self.assertEqual(accepted, [VALID])
        self.assertEqual(stderr, "")

    def test_corrupt_lines_skipped_with_warning(self):
        accepted, stderr = self.run_filter([VALID, NUL_PREFIXED, JSON_ONLY])
        self.assertEqual(accepted, [VALID])
        self.assertIn("in.cdxj:2: skipping corrupt CDXJ line (50 NUL bytes)", stderr)
        self.assertIn(
            "in.cdxj:3: skipping corrupt CDXJ line (no key before the JSON block)", stderr
        )
        self.assertIn("Skipped 2 corrupt CDXJ line(s).", stderr)

    def test_warning_preview_strips_nuls_and_truncates(self):
        line = "\x00" * 10 + "{" + "x" * 200 + "}\n"
        _, stderr = self.run_filter([line])
        self.assertNotIn("\x00", stderr)
        self.assertNotIn("\\x00", stderr)
        self.assertIn("'{" + "x" * 79 + "...'", stderr)

    def test_warning_cap(self):
        _, stderr = self.run_filter([JSON_ONLY] * 5, max_warnings=2)
        self.assertEqual(stderr.count("skipping corrupt CDXJ line"), 2)
        self.assertIn("Skipped 5 corrupt CDXJ line(s) (3 not shown).", stderr)


if __name__ == "__main__":
    unittest.main()
