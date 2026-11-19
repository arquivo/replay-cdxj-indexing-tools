#!/usr/bin/env python3
"""
Test suite for cdxj-to-csv functionality.

Test Coverage:
--------------
1. Value formatting and row building
2. Column auto-detection, including keys seen after the sample
3. Column subsets and ordering
4. Delimiters, text delimiters and quoting modes (CSV, TSV, ', |)
5. Values containing delimiters, quotes, newlines and non-ASCII text
6. Malformed, blank and corrupt lines
7. Option validation
8. CLI (stdin/stdout, files, --list-columns, --no-header)

"""

import argparse
import csv
import os
import sys
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

from replay_cdxj_indexing_tools.export.cdxj_to_csv import (
    build_row,
    cdxj_to_csv,
    detect_columns,
    format_value,
    list_columns,
    main,
    parse_char,
    parse_columns,
    validate_dialect,
)

SAMPLE = (
    'com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "text/html", '
    '"status": "200", "length": 1234}\n'
    'com,example)/a?b=c,d 20200102000000 {"url": "http://example.com/a?b=c,d", '
    '"status": "404", "note": "say \\"hi\\"", "ok": true, "n": null, "obj": {"a": [1, 2]}}\n'
    'pt,sapo)/ 20200103000000 {"url": "http://sapo.pt/\\u00e7\\u00e3", "late": "x"}\n'
)


class TempFilesMixin:
    """Create input files and run cdxj_to_csv on them, capturing stdout."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with

    def tearDown(self):
        self.tmpdir.cleanup()

    def write_input(self, content, name="input.cdxj"):
        path = os.path.join(self.tmpdir.name, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def convert(self, content, **kwargs):
        """Return (stdout, stderr, (processed, written)) for content."""
        path = self.write_input(content)
        with (
            patch("sys.stdout", new_callable=StringIO) as out,
            patch("sys.stderr", new_callable=StringIO) as err,
        ):
            result = cdxj_to_csv(input_file=path, **kwargs)
        return out.getvalue(), err.getvalue(), result


class TestFormatValue(unittest.TestCase):
    """Test JSON value to cell conversion."""

    def test_scalars(self):
        self.assertEqual(format_value("text"), "text")
        self.assertEqual(format_value(200), 200)
        self.assertEqual(format_value(1.5), 1.5)
        self.assertEqual(format_value(True), "true")
        self.assertEqual(format_value(False), "false")
        self.assertEqual(format_value(None), "")

    def test_nested_values_are_compact_json(self):
        self.assertEqual(format_value({"a": [1, 2]}), '{"a":[1,2]}')
        self.assertEqual(format_value(["ç"]), '["ç"]')


class TestBuildRow(unittest.TestCase):
    """Test row construction for a column list."""

    def test_prefix_and_json_columns(self):
        record = ("com,example)/", "20200101000000", {"status": "200"})
        self.assertEqual(
            build_row(record, ["status", "urlkey", "missing", "timestamp"]),
            ["200", "com,example)/", "", "20200101000000"],
        )

    def test_prefix_column_wins_over_json_key(self):
        record = ("com,example)/", "20200101000000", {"timestamp": "other"})
        self.assertEqual(build_row(record, ["timestamp"]), ["20200101000000"])


class TestDetectColumns(unittest.TestCase):
    """Test column auto-detection."""

    def test_first_seen_order_without_duplicates(self):
        records = [
            ("a", "1", {"url": "u", "status": "200"}),
            ("b", "2", {"status": "404", "mime": "text/html"}),
        ]
        self.assertEqual(detect_columns(records), ["urlkey", "timestamp", "url", "status", "mime"])

    def test_empty_sample_has_prefix_columns(self):
        self.assertEqual(detect_columns([]), ["urlkey", "timestamp"])


class TestOptionParsing(unittest.TestCase):
    """Test argparse types and dialect validation."""

    def test_parse_char_aliases(self):
        self.assertEqual(parse_char("\\t"), "\t")
        self.assertEqual(parse_char("tab"), "\t")
        self.assertEqual(parse_char("\t"), "\t")
        self.assertEqual(parse_char("|"), "|")

    def test_parse_char_rejects_multiple_characters(self):
        for value in ("", "ab", "||"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    parse_char(value)

    def test_parse_columns(self):
        self.assertEqual(parse_columns("urlkey, status ,mime"), ["urlkey", "status", "mime"])
        for value in ("", ",", "a,,b"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    parse_columns(value)

    def test_validate_dialect_rejections(self):
        cases = [
            (",", ",", "minimal", None),
            (",", '"', "minimal", ","),
            (",", '"', "minimal", '"'),
            ("\n", '"', "minimal", None),
            (",", "\r", "minimal", None),
            (",,", '"', "minimal", None),
            (",", '"', "bogus", None),
        ]
        for case in cases:
            with self.subTest(case=case):
                with self.assertRaises(ValueError):
                    validate_dialect(*case)

    def test_validate_dialect_accepts_valid(self):
        validate_dialect(",", '"', "minimal", None)
        validate_dialect("\t", "'", "all", "\\")
        validate_dialect("|", '"', "none", "\\")


class TestCdxjToCsv(TempFilesMixin, unittest.TestCase):
    """Test the conversion function."""

    def test_default_csv(self):
        out, err, result = self.convert(SAMPLE)
        self.assertEqual(err, "")
        self.assertEqual(result, (3, 3))
        lines = out.splitlines()
        self.assertEqual(lines[0], "urlkey,timestamp,url,mime,status,length,note,ok,n,obj,late")
        self.assertEqual(
            lines[1],
            '"com,example)/",20200101000000,http://example.com/,text/html,200,1234,,,,,',
        )
        self.assertEqual(
            lines[2],
            '"com,example)/a?b=c,d",20200102000000,"http://example.com/a?b=c,d",,404,,'
            '"say ""hi""",true,,"{""a"":[1,2]}",',
        )
        self.assertEqual(lines[3], '"pt,sapo)/",20200103000000,http://sapo.pt/çã,,,,,,,,x')

    def test_output_round_trips_through_csv_reader(self):
        out, _, _ = self.convert(SAMPLE)
        rows = list(csv.DictReader(StringIO(out)))
        self.assertEqual(rows[1]["urlkey"], "com,example)/a?b=c,d")
        self.assertEqual(rows[1]["note"], 'say "hi"')
        self.assertEqual(rows[1]["obj"], '{"a":[1,2]}')
        self.assertEqual(rows[2]["url"], "http://sapo.pt/çã")

    def test_lines_end_with_lf(self):
        out, _, _ = self.convert(SAMPLE)
        self.assertNotIn("\r", out)

    def test_tsv(self):
        out, _, _ = self.convert(SAMPLE, delimiter="\t", columns=["urlkey", "status"])
        self.assertEqual(
            out, "urlkey\tstatus\ncom,example)/\t200\ncom,example)/a?b=c,d\t404\npt,sapo)/\t\n"
        )

    def test_single_quote_text_delimiter(self):
        content = 'a 1 {"v": "it\'s, here"}\n'
        out, _, _ = self.convert(content, quotechar="'", columns=["v"])
        self.assertEqual(out, "v\n'it''s, here'\n")

    def test_pipe_text_delimiter(self):
        content = 'a 1 {"v": "x,y|z"}\n'
        out, _, _ = self.convert(content, quotechar="|", columns=["v"], header=False)
        self.assertEqual(out, "|x,y||z|\n")

    def test_quoting_all(self):
        out, _, _ = self.convert(SAMPLE, quoting="all", columns=["urlkey", "length"])
        self.assertEqual(out.splitlines()[:2], ['"urlkey","length"', '"com,example)/","1234"'])

    def test_quoting_nonnumeric_leaves_json_numbers_bare(self):
        out, _, _ = self.convert(SAMPLE, quoting="nonnumeric", columns=["length", "status"])
        self.assertEqual(out.splitlines()[1], '1234,"200"')

    def test_quoting_none_escapes_with_backslash_by_default(self):
        content = 'a,b 1 {"v": "x|y"}\n'
        out, _, _ = self.convert(content, delimiter="|", quoting="none", header=False)
        self.assertEqual(out, "a,b|1|x\\|y\n")

    def test_quoting_none_custom_escapechar(self):
        content = 'a 1 {"v": "x,y"}\n'
        out, _, _ = self.convert(
            content, quoting="none", escapechar="^", columns=["v"], header=False
        )
        self.assertEqual(out, "x^,y\n")

    def test_newline_in_value_is_quoted(self):
        content = 'a 1 {"v": "line1\\nline2"}\n'
        out, _, _ = self.convert(content, columns=["v"], header=False)
        self.assertEqual(out, '"line1\nline2"\n')

    def test_subset_and_order(self):
        out, _, _ = self.convert(SAMPLE, columns=["status", "urlkey", "absent"])
        self.assertEqual(
            out.splitlines(),
            [
                "status,urlkey,absent",
                '200,"com,example)/",',
                '404,"com,example)/a?b=c,d",',
                ',"pt,sapo)/",',
            ],
        )

    def test_explicit_columns_skip_late_key_warning(self):
        _, err, _ = self.convert(SAMPLE, columns=["urlkey"], sample_lines=1)
        self.assertEqual(err, "")

    def test_no_header(self):
        out, _, _ = self.convert(SAMPLE, columns=["timestamp"], header=False)
        self.assertEqual(out, "20200101000000\n20200102000000\n20200103000000\n")

    def test_keys_after_sample_are_dropped_with_warning(self):
        out, err, result = self.convert(SAMPLE, sample_lines=2)
        self.assertEqual(result, (3, 3))
        self.assertNotIn("late", out.splitlines()[0])
        self.assertIn("1 record(s) had fields not found in the first 2 records", err)
        self.assertIn(": late.", err)
        # Every row keeps the header's width
        self.assertEqual({len(row) for row in csv.reader(StringIO(out))}, {10})

    def test_late_key_warning_lists_a_bounded_number_of_keys(self):
        content = "a 1 {}\n" + "".join(f'b{i} 1 {{"k{i:02d}": 1}}\n' for i in range(30))
        _, err, _ = self.convert(content, sample_lines=1)
        self.assertIn("30 record(s)", err)
        self.assertIn("k09", err)
        self.assertNotIn("k10", err)

    def test_input_shorter_than_sample(self):
        out, err, result = self.convert(SAMPLE, sample_lines=100000)
        self.assertEqual(err, "")
        self.assertEqual(result, (3, 3))
        self.assertEqual(len(out.splitlines()), 4)

    def test_empty_input_writes_prefix_header(self):
        out, _, result = self.convert("")
        self.assertEqual(out, "urlkey,timestamp\n")
        self.assertEqual(result, (0, 0))

    def test_line_without_json_block(self):
        out, _, result = self.convert("a 1\n", columns=["urlkey", "timestamp", "status"])
        self.assertEqual(result, (1, 1))
        self.assertEqual(out.splitlines()[1], "a,1,")

    def test_bad_lines_are_skipped_and_reported(self):
        content = (
            'a 1 {"v": 1}\n'
            "\n"
            "   \n"
            '{"orphan": 1}\n'
            "b 2 {broken\n"
            "c 3 [1, 2]\n"
            "nospace\n"
            "d\x00 4 {}\n"
            'e 5 {"v": 2}\n'
        )
        out, err, result = self.convert(content, columns=["urlkey", "v"])
        self.assertEqual(out, "urlkey,v\na,1\ne,2\n")
        self.assertEqual(result, (7, 2))
        self.assertIn("Skipped 2 corrupt CDXJ line(s)", err)
        self.assertIn("Skipped 3 CDXJ line(s) that could not be parsed", err)
        self.assertIn("Use --verbose", err)

    def test_verbose_prints_line_errors_and_stats(self):
        content = 'a 1 {"v": 1}\nb 2 {broken\n'
        _, err, _ = self.convert(content, verbose=True)
        self.assertIn("input.cdxj:2: Invalid JSON", err)
        self.assertIn("Processed: 2 | Written: 1 | Errors: 1", err)
        self.assertNotIn("Use --verbose", err)

    def test_output_file(self):
        path = self.write_input(SAMPLE)
        out_path = os.path.join(self.tmpdir.name, "out.csv")
        cdxj_to_csv(input_file=path, output_file=out_path, columns=["url"], header=False)
        with open(out_path, encoding="utf-8", newline="") as f:
            self.assertEqual(
                f.read(),
                'http://example.com/\n"http://example.com/a?b=c,d"\nhttp://sapo.pt/çã\n',
            )

    def test_invalid_arguments(self):
        cases = [
            {"columns": []},
            {"sample_lines": 0},
            {"delimiter": ",", "quotechar": ","},
            {"quoting": "bogus"},
        ]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    self.convert(SAMPLE, **kwargs)

    def test_list_columns(self):
        path = self.write_input(SAMPLE)
        self.assertEqual(
            list_columns(path, sample_lines=1),
            ["urlkey", "timestamp", "url", "mime", "status", "length"],
        )


class TestCli(TempFilesMixin, unittest.TestCase):
    """Test the cdxj-to-csv command line."""

    def run_cli(self, *args, stdin=""):
        """Return (exit_code, stdout, stderr)."""
        with (
            patch.object(sys, "argv", ["cdxj-to-csv", *args]),
            patch("sys.stdin", StringIO(stdin)),
            patch("sys.stdout", new_callable=StringIO) as out,
            patch("sys.stderr", new_callable=StringIO) as err,
        ):
            with self.assertRaises(SystemExit) as cm:
                main()
        return cm.exception.code, out.getvalue(), err.getvalue()

    def test_stdin_to_stdout(self):
        code, out, _ = self.run_cli("-c", "urlkey,status", stdin=SAMPLE)
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines()[:2], ["urlkey,status", '"com,example)/",200'])

    def test_tsv_alias(self):
        for alias in ("\\t", "tab"):
            with self.subTest(alias=alias):
                code, out, _ = self.run_cli("-d", alias, "-c", "urlkey,status", stdin=SAMPLE)
                self.assertEqual(code, 0)
                self.assertEqual(out.splitlines()[1], "com,example)/\t200")

    def test_field_separator_alias(self):
        code, out, _ = self.run_cli("--field-separator", ";", "-c", "urlkey,status", stdin=SAMPLE)
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines()[1], "com,example)/;200")

    def test_file_options(self):
        path = self.write_input(SAMPLE)
        out_path = os.path.join(self.tmpdir.name, "out.csv")
        code, out, _ = self.run_cli(
            "-i", path, "-o", out_path, "--no-header", "-q", "'", "--quoting", "all", "-c", "ok"
        )
        self.assertEqual(code, 0)
        self.assertEqual(out, "")
        with open(out_path, encoding="utf-8") as f:
            self.assertEqual(f.read(), "''\n'true'\n''\n")

    def test_sample_lines_option(self):
        code, out, err = self.run_cli("--sample-lines", "1", stdin=SAMPLE)
        self.assertEqual(code, 0)
        self.assertEqual(out.splitlines()[0], "urlkey,timestamp,url,mime,status,length")
        self.assertIn("--sample-lines", err)

    def test_list_columns_option(self):
        code, out, _ = self.run_cli("--list-columns", "--sample-lines", "1", stdin=SAMPLE)
        self.assertEqual(code, 0)
        self.assertEqual(out, "urlkey\ntimestamp\nurl\nmime\nstatus\nlength\n")

    def test_validation_error_exits_1(self):
        code, out, err = self.run_cli("-d", ",", "-q", ",", stdin=SAMPLE)
        self.assertEqual(code, 1)
        self.assertEqual(out, "")
        self.assertIn("Error: delimiter, quotechar and escapechar must differ", err)

    def test_missing_input_file_exits_1(self):
        code, _, err = self.run_cli("-i", os.path.join(self.tmpdir.name, "nope.cdxj"))
        self.assertEqual(code, 1)
        self.assertIn("Error:", err)

    def test_argparse_rejections_exit_2(self):
        cases = [
            ("-d", "ab"),
            ("-q", ""),
            ("-c", "a,,b"),
            ("--sample-lines", "0"),
            ("--quoting", "bogus"),
        ]
        for args in cases:
            with self.subTest(args=args):
                code, _, _ = self.run_cli(*args, stdin=SAMPLE)
                self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
