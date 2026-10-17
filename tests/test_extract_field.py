#!/usr/bin/env python3
"""
Test suite for cdxj-extract-field functionality.

This module tests the ability to extract specific JSON fields from CDXJ records.

Test Coverage:
--------------
1. Parsing and extracting CDXJ fields
2. JSON encoding vs raw output modes
3. Default values and missing fields
4. Pipeline mode (stdin/stdout)
5. Edge cases and malformed input
6. Error handling and validation
7. CLI argument parsing

"""

import os
import re
import sys
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

from replay_cdxj_indexing_tools.search.extract_field import (
    extract_field_from_cdxj,
    extract_field_value,
    parse_cdxj_line,
)


class TestParseCdxjLine(unittest.TestCase):
    """Test CDXJ line parsing functionality."""

    def test_parse_simple_line(self):
        """Parse basic CDXJ line with JSON."""
        line = 'com,example)/ 20200101000000 {"status": "200", "collection": "AWP-999"}'

        surt, timestamp, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "com,example)/")
        self.assertEqual(timestamp, "20200101000000")
        self.assertIsNotNone(json_data)
        self.assertEqual(json_data["collection"], "AWP-999")
        self.assertEqual(json_data["status"], "200")

    def test_parse_line_without_json(self):
        """Parse CDXJ line without JSON metadata."""
        line = "com,example)/ 20200101000000"

        surt, timestamp, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "com,example)/")
        self.assertEqual(timestamp, "20200101000000")
        self.assertIsNone(json_data)

    def test_parse_line_with_complex_json(self):
        """Parse CDXJ line with complex nested JSON."""
        line = (
            "com,example)/page 20200101000000 "
            '{"url": "http://example.com/page", "mime": "text/html", '
            '"status": 200, "length": 1024}'
        )

        surt, _, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "com,example)/page")
        self.assertIsNotNone(json_data)
        self.assertEqual(json_data["status"], 200)
        self.assertEqual(json_data["length"], 1024)

    def test_parse_line_with_numeric_timestamp(self):
        """Parse CDXJ line with various timestamp formats."""
        line = 'com,example)/ 20200101000000 {"field": "value"}'

        _, timestamp, _, _ = parse_cdxj_line(line)

        self.assertEqual(timestamp, "20200101000000")

    def test_parse_line_with_surt_path(self):
        """Parse CDXJ line with complex SURT path."""
        line = 'pt,arquivo,www)/path/to/page 20200101000000 {"field": "value"}'

        surt, _, _, _ = parse_cdxj_line(line)

        self.assertEqual(surt, "pt,arquivo,www)/path/to/page")

    def test_parse_invalid_line_no_timestamp(self):
        """Reject line with missing timestamp."""
        line = "com,example)/"

        with self.assertRaises(ValueError):
            parse_cdxj_line(line)

    def test_parse_invalid_json(self):
        """Reject line with malformed JSON."""
        line = "com,example)/ 20200101000000 {invalid json}"

        with self.assertRaises(ValueError):
            parse_cdxj_line(line)

    def test_parse_empty_json_object(self):
        """Parse line with empty JSON object."""
        line = "com,example)/ 20200101000000 {}"

        surt, timestamp, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "com,example)/")
        self.assertIsNotNone(json_data)
        self.assertEqual(json_data, {})

    def test_parse_json_with_unicode(self):
        """Parse CDXJ line with Unicode characters."""
        line = 'pt,exemplo)/ 20200101000000 {"title": "Página em Português"}'

        _, _, _, json_data = parse_cdxj_line(line)

        self.assertEqual(json_data["title"], "Página em Português")

    def test_parse_json_with_spaces_in_surt(self):
        """Parse CDXJ with space in path (after SURT)."""
        line = 'com,example)/path 20200101000000 {"url": "http://example.com/"}'

        surt, timestamp, _, _ = parse_cdxj_line(line)

        self.assertEqual(surt, "com,example)/path")
        self.assertEqual(timestamp, "20200101000000")


class TestExtractFieldValue(unittest.TestCase):
    """Test field extraction functionality."""

    def test_extract_string_field_json_mode(self):
        """Extract string field in JSON mode (with quotes)."""
        json_data = {"collection": "AWP-999", "status": "200"}

        value = extract_field_value(json_data, "collection", raw=False)

        self.assertEqual(value, '"AWP-999"')

    def test_extract_string_field_raw_mode(self):
        """Extract string field in raw mode (without quotes)."""
        json_data = {"collection": "AWP-999", "status": "200"}

        value = extract_field_value(json_data, "collection", raw=True)

        self.assertEqual(value, "AWP-999")

    def test_extract_numeric_field(self):
        """Extract numeric field."""
        json_data = {"status": 200, "length": 1024}

        json_value = extract_field_value(json_data, "status", raw=False)
        raw_value = extract_field_value(json_data, "length", raw=True)

        self.assertEqual(json_value, "200")
        self.assertEqual(raw_value, "1024")

    def test_extract_boolean_field(self):
        """Extract boolean field."""
        json_data = {"valid": True, "archived": False}

        json_true = extract_field_value(json_data, "valid", raw=False)
        json_false = extract_field_value(json_data, "archived", raw=False)
        raw_true = extract_field_value(json_data, "valid", raw=True)
        raw_false = extract_field_value(json_data, "archived", raw=True)

        self.assertEqual(json_true, "true")
        self.assertEqual(json_false, "false")
        self.assertEqual(raw_true, "true")
        self.assertEqual(raw_false, "false")

    def test_extract_null_field(self):
        """Extract null field."""
        json_data = {"field": None}

        value = extract_field_value(json_data, "field", raw=False)

        self.assertEqual(value, "null")

    def test_extract_missing_field_with_default(self):
        """Extract missing field with default value."""
        json_data = {"status": "200"}

        value = extract_field_value(json_data, "missing", default="unknown")

        self.assertEqual(value, "unknown")

    def test_extract_missing_field_no_default(self):
        """Extract missing field without default."""
        json_data = {"status": "200"}

        value = extract_field_value(json_data, "missing", default=None)

        self.assertIsNone(value)

    def test_extract_from_none_json(self):
        """Extract field from None JSON data."""
        value = extract_field_value(None, "collection", default="unknown")

        self.assertEqual(value, "unknown")

    def test_extract_complex_string_with_quotes(self):
        """Extract string containing quotes."""
        json_data = {"title": 'Hello "World"'}

        value = extract_field_value(json_data, "title", raw=False)

        # JSON encoded should have escaped quotes
        self.assertIn('\\"', value)


class TestExtractFieldFromCdxj(unittest.TestCase):
    """Test full CDXJ extraction pipeline."""

    def test_extract_from_file(self):
        """Extract fields from CDXJ file."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"collection": "AWP-1"}\n'
            'com,example)/page1 20200101000000 {"collection": "AWP-1"}\n'
            'com,example)/page2 20200101000000 {"collection": "AWP-2"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            lines_processed, lines_extracted = extract_field_from_cdxj(
                input_file=temp_file,
                field_name="collection",
                raw=True,
                verbose=False,
            )

            self.assertEqual(lines_processed, 3)
            self.assertEqual(lines_extracted, 3)
        finally:
            os.unlink(temp_file)

    def test_extract_from_stdin(self):
        """Extract fields from stdin."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"status": "200"}\n'
            'com,example)/page 20200101000000 {"status": "404"}\n'
        )

        with patch("sys.stdin", StringIO(cdxj_content)):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                extract_field_from_cdxj(
                    input_file="-",
                    field_name="status",
                    raw=True,
                )

                output = mock_stdout.getvalue()
                lines = output.strip().split("\n")

                self.assertEqual(len(lines), 2)
                self.assertIn("200", lines)
                self.assertIn("404", lines)

    def test_extract_with_default_value(self):
        """Extract with default value for missing fields."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"collection": "AWP-1"}\n'
            'com,example)/page 20200101000000 {"status": "200"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                lines_processed, lines_extracted = extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="collection",
                    raw=True,
                    default="unknown",
                )

                output = mock_stdout.getvalue()
                lines = output.strip().split("\n")

                self.assertEqual(lines_processed, 2)
                self.assertEqual(lines_extracted, 2)
                self.assertIn("AWP-1", lines)
                self.assertIn("unknown", lines)
        finally:
            os.unlink(temp_file)

    def test_extract_skip_missing(self):
        """Extract with skip-missing option."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"collection": "AWP-1"}\n'
            'com,example)/page 20200101000000 {"status": "200"}\n'
            'com,example)/page2 20200101000000 {"collection": "AWP-2"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                lines_processed, lines_extracted = extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="collection",
                    raw=True,
                    skip_missing=True,
                )

                output = mock_stdout.getvalue()
                lines = output.strip().split("\n") if output.strip() else []

                self.assertEqual(lines_processed, 3)
                # Only 2 lines have collection field
                self.assertEqual(len([line for line in lines if line]), 2)
        finally:
            os.unlink(temp_file)

    def test_extract_json_mode(self):
        """Extract with JSON encoding."""
        cdxj_content = 'com,example)/ 20200101000000 {"title": "Page Title"}\n'

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="title",
                    raw=False,
                )

                output = mock_stdout.getvalue()

                # Should have JSON encoding with quotes
                self.assertIn('"', output)
                self.assertIn("Page Title", output)
        finally:
            os.unlink(temp_file)

    def test_extract_empty_lines(self):
        """Handle empty lines gracefully."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"field": "value"}\n'
            "\n"
            'com,example)/page 20200101000000 {"field": "value2"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                lines_processed, lines_extracted = extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="field",
                    raw=True,
                )

                # Empty lines should not be processed
                self.assertEqual(lines_processed, 2)
                self.assertEqual(lines_extracted, 2)
        finally:
            os.unlink(temp_file)

    def test_extract_malformed_line(self):
        """Handle malformed CDXJ lines."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"field": "value"}\n'
            "invalid line\n"
            'com,example)/page 20200101000000 {"field": "value2"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                lines_processed, lines_extracted = extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="field",
                    raw=True,
                    verbose=False,
                )

                # Should process valid lines and skip malformed
                self.assertEqual(lines_processed, 3)
                # Only 2 valid lines with the field
                self.assertEqual(lines_extracted, 2)
        finally:
            os.unlink(temp_file)

    def test_extract_invalid_field_name(self):
        """Reject extraction with empty field name."""
        with self.assertRaises(ValueError):
            extract_field_from_cdxj(
                input_file="-",
                field_name="",
            )

    def test_extract_numeric_field_raw(self):
        """Extract numeric field in raw mode."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"status": 200, "length": 1024}\n'
            'com,example)/page 20200101000000 {"status": 404, "length": 512}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="status",
                    raw=True,
                )

                output = mock_stdout.getvalue()
                lines = output.strip().split("\n")

                self.assertEqual(len(lines), 2)
                self.assertIn("200", lines)
                self.assertIn("404", lines)
        finally:
            os.unlink(temp_file)

    def test_extract_unicode_content(self):
        """Extract Unicode field values."""
        cdxj_content = 'pt,exemplo)/ 20200101000000 {"title": "Página em Português"}\n'

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".cdxj", encoding="utf-8"
        ) as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="title",
                    raw=True,
                )

                output = mock_stdout.getvalue()

                self.assertIn("Português", output)
        finally:
            os.unlink(temp_file)

    def test_extract_returns_statistics(self):
        """Verify return values contain correct statistics."""
        cdxj_content = (
            'com,example)/ 20200101000000 {"field": "value"}\n'
            'com,example)/page1 20200101000000 {"field": "value"}\n'
            'com,example)/page2 20200101000000 {"other": "value"}\n'
        )

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch("sys.stdout", new_callable=StringIO):
                lines_processed, lines_extracted = extract_field_from_cdxj(
                    input_file=temp_file,
                    field_name="field",
                    raw=True,
                    default=None,
                )

                self.assertEqual(lines_processed, 3)
                self.assertEqual(lines_extracted, 2)
        finally:
            os.unlink(temp_file)


class TestExtractFieldCLI(unittest.TestCase):
    """Test CLI argument parsing."""

    def test_cli_minimal_args(self):
        """Test CLI with minimal required arguments."""
        from replay_cdxj_indexing_tools.search.extract_field import main

        cdxj_content = 'com,example)/ 20200101000000 {"field": "value"}\n'

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch.object(
                __import__("sys"),
                "argv",
                ["cdxj-extract-field", "--field", "field", "-i", temp_file],
            ):
                with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                    with patch("sys.exit"):
                        main()

                    output = mock_stdout.getvalue()
                    self.assertIn("value", output)
        finally:
            os.unlink(temp_file)

    def test_cli_with_raw_flag(self):
        """Test CLI with --raw flag."""
        from replay_cdxj_indexing_tools.search.extract_field import main

        cdxj_content = 'com,example)/ 20200101000000 {"collection": "AWP-999"}\n'

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        try:
            with patch.object(
                __import__("sys"),
                "argv",
                ["cdxj-extract-field", "--field", "collection", "-i", temp_file, "--raw"],
            ):
                with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                    with patch("sys.exit"):
                        main()

                    output = mock_stdout.getvalue()
                    # Should not have JSON quotes
                    self.assertIn("AWP-999", output)
                    # Check no JSON quotes around the value
                    self.assertFalse(output.strip().startswith('"'))
        finally:
            os.unlink(temp_file)


class TestExtractFieldVerboseStatistics(unittest.TestCase):
    """Verbose statistics must account for every processed line (issue #80).

    Invariant: Processed == Extracted + Skipped + Errors, for every flag combination.
    """

    STATS_RE = re.compile(
        r"Processed: (\d+) \| Extracted: (\d+) \| Skipped: (\d+) \| Errors: (\d+)"
    )

    # 3 lines, only the first has the "collection" field
    ONE_OF_THREE = (
        'com,example)/ 20200101000000 {"collection": "AWP-1"}\n'
        'com,example)/page1 20200101000000 {"status": "200"}\n'
        'com,example)/page2 20200101000000 {"status": "404"}\n'
    )

    # field present, field missing, invalid JSON, no JSON block, malformed line
    MIXED = (
        'com,example)/ 20200101000000 {"collection": "AWP-1"}\n'
        'com,example)/page1 20200101000000 {"status": "200"}\n'
        "com,example)/page2 20200101000000 {not json}\n"
        "com,example)/page3 20200101000000\n"
        "invalidline\n"
    )

    def _run_cli(self, cdxj_content, *extra_args):
        """Run the CLI with -v; return (stdout, (processed, extracted, skipped, errors))."""
        from replay_cdxj_indexing_tools.search.extract_field import main

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".cdxj") as f:
            f.write(cdxj_content)
            temp_file = f.name

        argv = ["cdxj-extract-field", "-f", "collection", "-i", temp_file, "-v"]
        argv.extend(extra_args)
        try:
            with (
                patch.object(sys, "argv", argv),
                patch("sys.stdout", new_callable=StringIO) as mock_stdout,
                patch("sys.stderr", new_callable=StringIO) as mock_stderr,
            ):
                with self.assertRaises(SystemExit) as cm:
                    main()
        finally:
            os.unlink(temp_file)

        self.assertEqual(cm.exception.code, 0)
        match = self.STATS_RE.search(mock_stderr.getvalue())
        self.assertIsNotNone(match, mock_stderr.getvalue())
        stats = tuple(int(n) for n in match.groups())
        processed, extracted, skipped, errors = stats
        self.assertEqual(processed, extracted + skipped + errors, stats)
        return mock_stdout.getvalue(), stats

    def test_missing_field_json_mode_counted_as_skipped(self):
        """Lines missing the field are counted as skipped in JSON (non-raw) mode too."""
        stdout, stats = self._run_cli(self.ONE_OF_THREE)
        self.assertEqual(stats, (3, 1, 2, 0))
        self.assertEqual(stdout, '"AWP-1"\n')

    def test_skip_missing_default_matrix(self):
        """Field missing: no flags, --skip-missing, --default, and both.

        --skip-missing takes precedence over --default.
        """
        cases = {
            (): ("AWP-1\n", (3, 1, 2, 0)),
            ("--skip-missing",): ("AWP-1\n", (3, 1, 2, 0)),
            ("--default", "unknown"): ("AWP-1\nunknown\nunknown\n", (3, 3, 0, 0)),
            ("--default", "unknown", "--skip-missing"): ("AWP-1\n", (3, 1, 2, 0)),
        }
        for args, (expected_stdout, expected_stats) in cases.items():
            with self.subTest(args=args):
                stdout, stats = self._run_cli(self.ONE_OF_THREE, "-r", *args)
                self.assertEqual(stdout, expected_stdout)
                self.assertEqual(stats, expected_stats)

    def test_invariant_holds_with_errors_for_all_flag_combinations(self):
        """Invariant holds with parse errors and lines without a JSON block."""
        # MIXED: 1 has field, 1 missing field, 1 bad JSON, 1 no JSON, 1 malformed
        cases = {
            (): (5, 1, 2, 2),
            ("-r",): (5, 1, 2, 2),
            ("--skip-missing",): (5, 1, 2, 2),
            ("-r", "--skip-missing"): (5, 1, 2, 2),
            ("--default", "x"): (5, 3, 0, 2),
            ("-r", "--default", "x"): (5, 3, 0, 2),
            ("--default", "x", "--skip-missing"): (5, 1, 2, 2),
            ("-r", "--default", "x", "--skip-missing"): (5, 1, 2, 2),
        }
        for args, expected in cases.items():
            with self.subTest(args=args):
                _, stats = self._run_cli(self.MIXED, *args)
                self.assertEqual(stats, expected)


if __name__ == "__main__":
    unittest.main()
