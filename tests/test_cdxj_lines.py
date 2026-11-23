#!/usr/bin/env python3
"""
Tests for replay_cdxj_indexing_tools.cdxj_lines - shared CDXJ input line filtering.

Covers corrupt-line detection (NUL bytes, missing key before the JSON block) on both
str and bytes lines, silent skipping of blank lines, stderr warnings naming the input
and line number, the per-run warning cap, and the final summary (issue #111).
Also covers SortOrderChecker, which stops at the first line out of byte order.
"""

import contextlib
import io
import os
import tempfile
import unittest

from replay_cdxj_indexing_tools.cdxj_lines import (
    CdxjLineFilter,
    SortOrderChecker,
    UnsortedInputError,
    corrupt_reason,
    remove_partial_output,
)

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


class TestSortOrderChecker(unittest.TestCase):
    """Tests for SortOrderChecker.check() and UnsortedInputError."""

    @staticmethod
    def check_all(lines, source="in.cdxj"):
        checker = SortOrderChecker(source)
        for lineno, line in enumerate(lines, 1):
            checker.check(line, lineno)

    def test_sorted_lines_with_duplicates_pass(self):
        lines = ["pt,a)/ 1 {}\n", "pt,a)/ 1 {}\n", "pt,a)/ 2 {}\n", "pt,b)/ 1 {}\n"]
        self.check_all(lines)
        self.check_all([line.encode() for line in lines])

    def test_out_of_order_line_raises(self):
        lines = [b"pt,a)/ 1 {}\n", b"pt,c)/ 1 {}\n", b"pt,b)/ 1 {}\n"]
        with self.assertRaises(UnsortedInputError) as ctx:
            self.check_all(lines, source="data/x.cdxj")
        err = ctx.exception
        self.assertEqual((err.source, err.prev_lineno, err.lineno), ("data/x.cdxj", 2, 3))
        message = str(err)
        self.assertIn("data/x.cdxj is not sorted: line 3 sorts before line 2", message)
        self.assertIn("line 2: 'pt,c)/ 1 {}'", message)
        self.assertIn("line 3: 'pt,b)/ 1 {}'", message)
        self.assertIn("LC_ALL=C sort", message)

    def test_locale_order_is_rejected(self):
        # A locale-aware sort (e.g. en_US.UTF-8) puts lowercase before uppercase;
        # byte order (LC_ALL=C) puts 'B' (0x42) before 'a' (0x61).
        with self.assertRaises(UnsortedInputError):
            self.check_all(["pt,a)/a 1 {}\n", "pt,a)/B 1 {}\n"])
        self.check_all(["pt,a)/B 1 {}\n", "pt,a)/a 1 {}\n"])

    def test_line_terminator_is_ignored(self):
        # sort compares lines without their newline: "abc" < "abc\tx" even though
        # "\n" (0x0a) > "\t" (0x09); a last line without newline equals its duplicate.
        self.check_all(["abc\n", "abc\tx\n"])
        self.check_all([b"pt,a)/ 1 {}\r\n", b"pt,a)/ 1 {}"])

    def test_utf8_text_and_bytes_agree(self):
        # Code point order of str matches byte order of its UTF-8 encoding
        lines = ["pt,a)/z 1 {}\n", "pt,a)/\u00e9 1 {}\n"]
        self.check_all(lines)
        self.check_all([line.encode("utf-8") for line in lines])
        with self.assertRaises(UnsortedInputError):
            self.check_all(lines[::-1])
        with self.assertRaises(UnsortedInputError):
            self.check_all([line.encode("utf-8") for line in lines[::-1]])

    def test_message_escapes_control_characters(self):
        with self.assertRaises(UnsortedInputError) as ctx:
            self.check_all([b"pt,b)/ 1 \x1b[2J{}\n", b"pt,a)/ 1 {}\n"])
        self.assertNotIn("\x1b", str(ctx.exception))
        self.assertIn("\\x1b[2J", str(ctx.exception))

    def test_message_escapes_control_characters_in_source(self):
        with self.assertRaises(UnsortedInputError) as ctx:
            self.check_all(["b\n", "a\n"], source="in\x1b[2J.cdxj")
        self.assertNotIn("\x1b", str(ctx.exception))

    def test_message_preview_of_huge_line_is_bounded(self):
        with self.assertRaises(UnsortedInputError) as ctx:
            self.check_all([b"b" * 10_000_000 + b"\n", b"a\n"])
        self.assertLess(len(str(ctx.exception)), 500)


class TestRemovePartialOutput(unittest.TestCase):
    """Tests for remove_partial_output()."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def path(self, name):
        return os.path.join(self.tmp.name, name)

    def remove(self, path):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            remove_partial_output(path)
        return stderr.getvalue()

    def test_regular_file_is_removed(self):
        path = self.path("out.cdxj")
        with open(path, "w", encoding="utf-8") as f:
            f.write("partial\n")
        self.assertEqual(self.remove(path), "")
        self.assertFalse(os.path.exists(path))

    def test_missing_file_is_ignored(self):
        self.assertEqual(self.remove(self.path("missing.cdxj")), "")

    @unittest.skipUnless(hasattr(os, "symlink"), "needs symlinks")
    def test_symlink_is_left_in_place(self):
        target = self.path("target.cdxj")
        with open(target, "w", encoding="utf-8") as f:
            f.write("partial\n")
        link = self.path("link.cdxj")
        os.symlink(target, link)
        warning = self.remove(link)
        self.assertTrue(os.path.islink(link))
        self.assertTrue(os.path.exists(target))
        self.assertIn("Partial output left in place (not a regular file)", warning)

    @unittest.skipUnless(hasattr(os, "mkfifo"), "needs FIFOs")
    def test_fifo_is_left_in_place(self):
        fifo = self.path("out.fifo")
        os.mkfifo(fifo)
        warning = self.remove(fifo)
        self.assertTrue(os.path.exists(fifo))
        self.assertIn("not a regular file", warning)


if __name__ == "__main__":
    unittest.main()
