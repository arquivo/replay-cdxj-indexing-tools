#!/usr/bin/env python3
"""
Tests for replay_cdxj_indexing_tools.cli_utils, the helpers shared by the CLIs.
"""

import argparse
import unittest

from replay_cdxj_indexing_tools.cli_utils import positive_int


class TestPositiveInt(unittest.TestCase):
    """Tests for the positive_int() argparse type."""

    def test_accepts_positive_values(self):
        """Positive integers pass through unchanged."""
        self.assertEqual(positive_int("1"), 1)
        self.assertEqual(positive_int("500"), 500)

    def test_rejects_zero_and_negative(self):
        """Zero and negative values raise ArgumentTypeError with a clear message."""
        for value in ("0", "-1"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(
                    argparse.ArgumentTypeError, f"must be a positive integer, got {value}"
                ):
                    positive_int(value)

    def test_rejects_non_integer(self):
        """Non-integers raise ValueError, which argparse reports as an invalid value."""
        for value in ("abc", "1.5", ""):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    positive_int(value)

    def test_argparse_reports_usage_error(self):
        """Used as an argparse type, a non-positive value exits with status 2."""
        parser = argparse.ArgumentParser()
        parser.add_argument("--workers", type=positive_int, default=4)
        self.assertEqual(parser.parse_args(["--workers", "3"]).workers, 3)
        with self.assertRaises(SystemExit) as ctx:
            parser.parse_args(["--workers", "0"])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
