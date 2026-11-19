#!/usr/bin/env python3
"""
cdxj_to_csv.py

Convert CDXJ records to CSV or TSV, one row per record.

This tool reads CDXJ lines from stdin or a file and writes a delimited table
to stdout or a file. The two leading CDXJ fields become the "urlkey" and
"timestamp" columns, and every key of the JSON block becomes a column of its
own. Unless --columns is given, the columns are detected from the first
--sample-lines records.

COMMAND-LINE USAGE
==================

Export every field:

    # CSV with a header row, columns auto-detected from the first 1000 records
    cat index.cdxj | cdxj-to-csv

    # TSV
    cat index.cdxj | cdxj-to-csv -d '\\t'

    # Single quote as text delimiter, values always quoted
    cat index.cdxj | cdxj-to-csv -q "'" --quoting all

    # Pipe-separated, no quoting at all
    cat index.cdxj | cdxj-to-csv -d '|' --quoting none

Export a subset of columns, in the given order:

    cdxj-to-csv -i index.cdxj -o index.csv -c urlkey,timestamp,status,mime

Inspect the detected columns before choosing a subset:

    cdxj-to-csv -i index.cdxj --list-columns

CDXJ FORMAT
===========

Input:  com,example)/ 20200101000000 {"url":"http://example.com/","status":"200"}
Output: urlkey,timestamp,url,status
        "com,example)/",20200101000000,http://example.com/,200

Values are written as follows:

    - missing fields and JSON null: empty cell
    - strings and numbers: as they are
    - true/false: "true"/"false"
    - objects and arrays: compact JSON

Keys that first appear after the sampled records are not exported; a warning
on stderr reports them. Use --columns or a larger --sample-lines to include
them.

PYTHON API
==========

    from replay_cdxj_indexing_tools.export.cdxj_to_csv import cdxj_to_csv

    cdxj_to_csv(
        input_file="index.cdxj",
        output_file="index.csv",
        columns=["urlkey", "timestamp", "status"],
        delimiter=",",
        quotechar='"',
        quoting="minimal",
    )

"""

import argparse
import csv
import itertools
import json
import os
import sys
from typing import (
    IO,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from replay_cdxj_indexing_tools.cdxj_lines import CdxjLineFilter
from replay_cdxj_indexing_tools.cli_utils import positive_int
from replay_cdxj_indexing_tools.search.extract_field import parse_cdxj_line

DEFAULT_SAMPLE_LINES = 1000

# Columns taken from the two space-separated fields before the JSON block
PREFIX_COLUMNS = ("urlkey", "timestamp")

QUOTING_MODES: Dict[str, Literal[0, 1, 2, 3]] = {
    "minimal": csv.QUOTE_MINIMAL,
    "all": csv.QUOTE_ALL,
    "nonnumeric": csv.QUOTE_NONNUMERIC,
    "none": csv.QUOTE_NONE,
}

# Names accepted for characters that are awkward to type in a shell
CHAR_ALIASES = {"\\t": "\t", "tab": "\t"}

# Late keys named in the warning; the rest are only counted
MAX_REPORTED_KEYS = 10

Record = Tuple[str, str, dict]
Cell = Union[str, int, float]


def parse_char(value: str) -> str:
    """argparse type: a single character, or an alias such as '\\t' or 'tab'."""
    char = CHAR_ALIASES.get(value, value)
    if len(char) != 1:
        raise argparse.ArgumentTypeError(f"must be a single character, got {value!r}")
    return char


def parse_columns(value: str) -> List[str]:
    """argparse type: comma-separated column names."""
    columns = [name.strip() for name in value.split(",")]
    if not all(columns):
        raise argparse.ArgumentTypeError(f"empty column name in {value!r}")
    return columns


def validate_dialect(
    delimiter: str, quotechar: str, quoting: str, escapechar: Optional[str]
) -> None:
    """
    Reject delimiter/quoting combinations the csv module would refuse or mangle.

    Raises:
        ValueError: If the combination is invalid
    """
    if quoting not in QUOTING_MODES:
        known = ", ".join(QUOTING_MODES)
        raise ValueError(f"Unknown quoting mode {quoting!r}. Available modes: {known}")
    chars = {"delimiter": delimiter, "quotechar": quotechar}
    if escapechar is not None:
        chars["escapechar"] = escapechar
    for name, char in chars.items():
        if len(char) != 1:
            raise ValueError(f"{name} must be exactly one character, got {char!r}")
        if char in "\r\n":
            raise ValueError(f"{name} cannot be a line break")
    if len(set(chars.values())) != len(chars):
        raise ValueError(f"delimiter, quotechar and escapechar must differ, got {chars}")


def format_value(value: object) -> Cell:
    """
    Convert a JSON value to a CSV cell.

    Numbers stay numbers so that --quoting nonnumeric leaves them unquoted.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def build_row(record: Record, columns: Sequence[str]) -> List[Cell]:
    """Return the cells of one record for the given columns."""
    surt_key, timestamp, json_data = record
    row: List[Cell] = []
    for name in columns:
        if name == "urlkey":
            row.append(surt_key)
        elif name == "timestamp":
            row.append(timestamp)
        else:
            row.append(format_value(json_data.get(name)))
    return row


def detect_columns(records: Sequence[Record]) -> List[str]:
    """Return the prefix columns followed by every JSON key, in first-seen order."""
    columns: Dict[str, None] = dict.fromkeys(PREFIX_COLUMNS)
    for _, _, json_data in records:
        columns.update(dict.fromkeys(json_data))
    return list(columns)


class CdxjReader:
    """
    Parse CDXJ lines into (urlkey, timestamp, json_data) records.

    Blank and corrupt lines are dropped by CdxjLineFilter; lines that cannot be
    parsed (no timestamp, invalid JSON block) are dropped and counted here.
    """

    def __init__(self, source: str, verbose: bool = False):
        self.source = source
        self.verbose = verbose
        self.line_filter = CdxjLineFilter()
        self.lines_processed = 0
        self.lines_errors = 0

    def records(self, infile: IO[str]) -> Iterator[Record]:
        """Yield one record per valid line of infile."""
        for lineno, line in enumerate(infile, 1):
            line = line.rstrip("\n\r")
            if not line.strip():
                continue
            self.lines_processed += 1
            if not self.line_filter.accept(line, self.source, lineno):
                self.lines_errors += 1
                continue
            try:
                surt_key, timestamp, _, json_data = parse_cdxj_line(line)
                if json_data is None:
                    json_data = {}
                elif not isinstance(json_data, dict):
                    raise ValueError("JSON block is not an object")
            except ValueError as e:
                self.lines_errors += 1
                if self.verbose:
                    print(f"Error parsing {self.source}:{lineno}: {e}", file=sys.stderr)
                continue
            yield surt_key, timestamp, json_data

    def report(self) -> None:
        """Print a summary of skipped lines, if any."""
        self.line_filter.report()
        unparseable = self.lines_errors - self.line_filter.corrupt_count
        if unparseable:
            hint = "" if self.verbose else " Use --verbose to see them."
            print(
                f"[WARNING] Skipped {unparseable} CDXJ line(s) that could not be parsed.{hint}",
                file=sys.stderr,
            )


class LateKeys:
    """Track JSON keys missing from auto-detected columns, for one warning at the end."""

    def __init__(self, columns: Sequence[str]):
        self.known = set(columns)
        self.names: Set[str] = set()
        self.rows = 0

    def check(self, json_data: dict) -> None:
        """Record the keys of json_data that have no column."""
        missing = json_data.keys() - self.known
        if missing:
            self.rows += 1
            room = MAX_REPORTED_KEYS - len(self.names)
            self.names.update(sorted(missing)[:room])

    def report(self, sample_lines: int) -> None:
        """Print a warning if any record had keys that were not exported."""
        if not self.rows:
            return
        print(
            f"[WARNING] {self.rows} record(s) had fields not found in the first "
            f"{sample_lines} records and were not exported: {', '.join(sorted(self.names))}. "
            "Use --columns or a larger --sample-lines to include them.",
            file=sys.stderr,
        )


def _write_rows(
    writer, records: Iterable[Record], columns: Sequence[str], late_keys: Optional[LateKeys]
) -> int:
    """Write one row per record and return the number of rows written."""
    rows_written = 0
    for record in records:
        if late_keys is not None:
            late_keys.check(record[2])
        writer.writerow(build_row(record, columns))
        rows_written += 1
    return rows_written


def _open_input(input_file: str) -> IO[str]:
    if input_file == "-":
        return sys.stdin
    # CDXJ is UTF-8 regardless of the process locale
    # pylint: disable-next=consider-using-with
    return open(input_file, "r", encoding="utf-8")


def list_columns(
    input_file: str = "-", sample_lines: int = DEFAULT_SAMPLE_LINES, verbose: bool = False
) -> List[str]:
    """Return the columns auto-detection finds in the first sample_lines records."""
    reader = CdxjReader(input_file, verbose)
    infile = _open_input(input_file)
    try:
        sample = list(itertools.islice(reader.records(infile), sample_lines))
    finally:
        if input_file != "-":
            infile.close()
    reader.report()
    return detect_columns(sample)


def cdxj_to_csv(
    input_file: str = "-",
    output_file: str = "-",
    columns: Optional[Sequence[str]] = None,
    delimiter: str = ",",
    quotechar: str = '"',
    quoting: str = "minimal",
    escapechar: Optional[str] = None,
    sample_lines: int = DEFAULT_SAMPLE_LINES,
    header: bool = True,
    verbose: bool = False,
) -> Tuple[int, int]:
    """
    Convert CDXJ records to delimited text.

    Args:
        input_file: Input CDXJ file, or '-' for stdin (default: stdin)
        output_file: Output file, or '-' for stdout (default: stdout)
        columns: Columns to export, in order; None auto-detects them from the
            first sample_lines records (default: None)
        delimiter: Column delimiter (default: ',')
        quotechar: Text delimiter wrapped around values that need it (default: '"')
        quoting: One of 'minimal', 'all', 'nonnumeric', 'none' (default: 'minimal')
        escapechar: Escape character; defaults to '\\' when quoting is 'none'
        sample_lines: Records read to auto-detect columns (default: 1000)
        header: Write a header row with the column names (default: True)
        verbose: Print per-line errors and statistics to stderr (default: False)

    Returns:
        Tuple of (lines_processed, rows_written)

    Raises:
        ValueError: If the columns, delimiter or quoting options are invalid
    """
    if quoting == "none" and escapechar is None:
        escapechar = "\\"
    validate_dialect(delimiter, quotechar, quoting, escapechar)
    if columns is not None and not columns:
        raise ValueError("columns must not be empty")
    if sample_lines <= 0:
        raise ValueError(f"sample_lines must be a positive integer, got {sample_lines}")

    reader = CdxjReader(input_file, verbose)
    late_keys = None

    infile = _open_input(input_file)
    try:
        records = reader.records(infile)
        if columns is None:
            sample = list(itertools.islice(records, sample_lines))
            columns = detect_columns(sample)
            records = itertools.chain(sample, records)
            late_keys = LateKeys(columns)

        if output_file == "-":
            outfile = sys.stdout
        else:
            # pylint: disable-next=consider-using-with
            outfile = open(output_file, "w", newline="", encoding="utf-8")
        try:
            writer = csv.writer(
                outfile,
                delimiter=delimiter,
                quotechar=quotechar,
                quoting=QUOTING_MODES[quoting],
                escapechar=escapechar,
                lineterminator="\n",
            )
            if header:
                writer.writerow(columns)
            rows_written = _write_rows(writer, records, columns, late_keys)
        finally:
            if output_file != "-":
                outfile.close()
    finally:
        if input_file != "-":
            infile.close()

    reader.report()
    if late_keys is not None:
        late_keys.report(sample_lines)
    if verbose:
        print(
            f"Processed: {reader.lines_processed} | "
            f"Written: {rows_written} | "
            f"Errors: {reader.lines_errors}",
            file=sys.stderr,
        )

    return reader.lines_processed, rows_written


def main():
    """Main entry point for cdxj-to-csv command."""
    parser = argparse.ArgumentParser(
        description="Convert CDXJ records to CSV or TSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Columns:
  "urlkey" and "timestamp" are the two fields before the JSON block; every
  JSON key is a column of its own. Without --columns, the columns are
  detected from the first --sample-lines records.

Examples:
  # CSV with every field
  cat index.cdxj | cdxj-to-csv

  # TSV
  cat index.cdxj | cdxj-to-csv -d '\\t'

  # A subset of columns, in this order
  cdxj-to-csv -i index.cdxj -o index.csv -c urlkey,timestamp,status,mime

  # Single quote as text delimiter, every value quoted
  cat index.cdxj | cdxj-to-csv -q "'" --quoting all

  # Pipe-separated without quoting (delimiters in values are escaped with \\)
  cat index.cdxj | cdxj-to-csv -d '|' --quoting none

  # Show the detected columns
  cdxj-to-csv -i index.cdxj --list-columns
        """,
    )

    parser.add_argument(
        "--input",
        "-i",
        dest="input_file",
        default="-",
        help="Input CDXJ file (default: stdin)",
    )
    parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        default="-",
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--columns",
        "-c",
        type=parse_columns,
        default=None,
        help="Comma-separated columns to export, in order (default: auto-detect)",
    )
    parser.add_argument(
        "--delimiter",
        "--field-separator",
        "-d",
        type=parse_char,
        default=",",
        help="Column delimiter; '\\t' or 'tab' for TSV (default: ',')",
    )
    parser.add_argument(
        "--quotechar",
        "-q",
        type=parse_char,
        default='"',
        help="Text delimiter wrapped around values (default: '\"')",
    )
    parser.add_argument(
        "--quoting",
        choices=list(QUOTING_MODES),
        default="minimal",
        help=(
            "When to wrap values in the text delimiter: only when needed (minimal), always "
            "(all), every non-number (nonnumeric), or never (none) (default: minimal)"
        ),
    )
    parser.add_argument(
        "--escapechar",
        type=parse_char,
        default=None,
        help="Escape character for delimiters in values (default: '\\' with --quoting none)",
    )
    parser.add_argument(
        "--sample-lines",
        type=positive_int,
        default=DEFAULT_SAMPLE_LINES,
        help=f"Records read to auto-detect columns (default: {DEFAULT_SAMPLE_LINES})",
    )
    parser.add_argument(
        "--no-header",
        dest="header",
        action="store_false",
        help="Do not write the header row",
    )
    parser.add_argument(
        "--list-columns",
        action="store_true",
        help="Print the auto-detected columns, one per line, and exit",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print per-line errors and statistics to stderr",
    )

    args = parser.parse_args()

    try:
        if args.list_columns:
            for name in list_columns(args.input_file, args.sample_lines, args.verbose):
                print(name)
        else:
            cdxj_to_csv(
                input_file=args.input_file,
                output_file=args.output_file,
                columns=args.columns,
                delimiter=args.delimiter,
                quotechar=args.quotechar,
                quoting=args.quoting,
                escapechar=args.escapechar,
                sample_lines=args.sample_lines,
                header=args.header,
                verbose=args.verbose,
            )

        sys.exit(0)

    except BrokenPipeError:
        # The reader went away (e.g. "| head"); stop quietly. Point stdout at
        # devnull so the interpreter's final flush doesn't fail again.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)

    except (ValueError, IOError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
