#!/usr/bin/env python3
"""
cdxj_stats.py

Aggregate capture statistics from a CDXJ stream using pluggable sinks, in a
single pass over the input.

This tool has no opinion on how the CDXJ stream was produced. Piping a
SURT-sorted stream through it (e.g. via merge-flat-cdxj, optionally filtered
with filter-blocklist) is the recommended usage, not a hard dependency: the
domain-host/domain-etld1 sinks rely on SURT-sortedness for their O(1)-memory
streaming design and will warn to stderr if the input doesn't look sorted, but
they will still produce output (possibly with a key split across multiple
rows) either way.

COMMAND-LINE USAGE
==================

    # Single sink
    cat index.cdxj | cdxj-stats --sink mimetype --out-dir stats/

    # Multiple sinks in one pass over a multi-year corpus
    merge-flat-cdxj - <cdxj files> \\
      | filter-blocklist -i - -b blocklist.txt \\
      | cdxj-stats --sink mimetype,domain-host,domain-etld1 --out-dir stats/

    # Per-sink options: comma-separated sink list, "--sink-<name> key=value ..."
    cdxj-stats -i index.cdxj \\
      --sink mimetype,domain-host \\
      --sink-mimetype group-by=year \\
      --sink-domain-host group-by=year \\
      --out-dir stats/

    # TSV output instead of CSV
    cdxj-stats -i index.cdxj --sink domain-host --field-separator "\\t" --out-dir stats/

Output: one file per sink under --out-dir, named "<sink>.csv" or
"<sink>-<group-by>.csv" when group-by is configured for that sink (e.g.
"domain-host-year.csv").

TOP-N
=====

There's no built-in top-N filtering; sinks export every row they see. To keep
only the top 100 rows of a sink's output by its count column, use a plain
shell pipeline (fields are 0-indexed after the header, so adjust -k to the
count column of the sink you're using):

    (head -1 stats/mimetype.csv; tail -n +2 stats/mimetype.csv | sort -t, -k2 -nr | head -100) \\
      > stats/mimetype-top100.csv

SCALING OUT
===========

This tool intentionally does not parallelize sinks within one process: a
multi-year corpus should only be read/decompressed once, and the domain
sinks' streaming flush-on-key-change logic depends on strictly ordered,
single-threaded delivery. To speed up a very large run, shard the corpus (by
year or collection) and run several cdxj-stats processes in parallel, one per
shard, each writing to its own --out-dir; then sum the resulting per-sink CSVs
(e.g. with a small pandas/awk groupby) to get the full-corpus totals.

PYTHON API
==========

    from replay_cdxj_indexing_tools.stats.cdxj_stats import run

    run(
        input_file="index.cdxj",
        sink_names=["mimetype", "domain-host"],
        sink_options={"domain-host": {"group-by": "year"}},
        out_dir="stats/",
    )

"""

import argparse
import csv
import os
import sys
from typing import Dict, List, Optional, Tuple

from replay_cdxj_indexing_tools.stats.base import OrderChecker, load_sinks, parse_cdxj_line


def extract_sink_options(argv: List[str]) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    """
    Pull "--sink-<name> key=value ..." groups out of argv.

    Sink names are plugin-defined, so these options can't be declared upfront
    with argparse. Returns (sink_options, remaining_argv) where remaining_argv
    can be parsed normally by the standard argparse parser.
    """
    sink_options: Dict[str, Dict[str, str]] = {}
    remaining: List[str] = []

    i = 0
    while i < len(argv):
        token = argv[i]
        if token.startswith("--sink-") and len(token) > len("--sink-"):
            name = token[len("--sink-") :]
            opts = sink_options.setdefault(name, {})
            i += 1
            while i < len(argv) and not argv[i].startswith("--"):
                if "=" not in argv[i]:
                    raise ValueError(
                        f"Invalid option {argv[i]!r} for --sink-{name} (expected key=value)"
                    )
                key, _, value = argv[i].partition("=")
                opts[key] = value
                i += 1
        else:
            remaining.append(token)
            i += 1

    return sink_options, remaining


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the argparse parser for cdxj-stats' non-sink-specific options."""
    parser = argparse.ArgumentParser(
        prog="cdxj-stats",
        description="Aggregate capture statistics from a CDXJ stream using pluggable sinks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  cat index.cdxj | %(prog)s --sink mimetype --out-dir stats/

  merge-flat-cdxj - *.cdxj | %(prog)s --sink mimetype,domain-host,domain-etld1 --out-dir stats/

  %(prog)s -i index.cdxj --sink domain-host --sink-domain-host group-by=year --out-dir stats/
        """,
    )
    parser.add_argument(
        "-i", "--input", dest="input_file", default="-", help="Input CDXJ file, or - for stdin"
    )
    parser.add_argument(
        "--sink",
        required=True,
        help="Comma-separated sink names to run in one pass, e.g. mimetype,domain-host",
    )
    parser.add_argument("--out-dir", required=True, help="Directory to write one CSV per sink into")
    parser.add_argument(
        "--field-separator",
        default=",",
        help="Output field separator for every sink's CSV output (default: ,)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Print summary to stderr")
    return parser


def run(
    input_file: str,
    sink_names: List[str],
    out_dir: str,
    sink_options: Optional[Dict[str, Dict[str, str]]] = None,
    field_separator: str = ",",
    verbose: bool = False,
) -> Dict[str, int]:
    """Run cdxj-stats and return {sink_name: rows_written}."""
    sink_options = sink_options or {}

    if len(field_separator) != 1:
        raise ValueError(f"field_separator must be exactly one character, got {field_separator!r}")

    available = load_sinks()
    sinks = {}
    for name in sink_names:
        entry_point = available.get(name)
        if entry_point is None:
            known = ", ".join(sorted(available)) or "(none registered)"
            raise ValueError(f"Unknown sink {name!r}. Available sinks: {known}")
        sink = entry_point.load()()
        sink.configure(**sink_options.get(name, {}))
        sinks[name] = sink

    os.makedirs(out_dir, exist_ok=True)

    files = {}
    writers = {}
    rows_written = {name: 0 for name in sinks}

    try:
        for name, sink in sinks.items():
            suffix = sink.output_suffix()
            filename = f"{name}-{suffix}.csv" if suffix else f"{name}.csv"
            # pylint: disable-next=consider-using-with
            handle = open(os.path.join(out_dir, filename), "w", newline="", encoding="utf-8")
            files[name] = handle
            writer = csv.writer(handle, delimiter=field_separator)
            writer.writerow(sink.fieldnames())
            writers[name] = writer

        order_checker = OrderChecker()
        lines_processed = 0

        if input_file == "-":
            infile = sys.stdin
        else:
            # pylint: disable-next=consider-using-with,unspecified-encoding  # locale
            infile = open(input_file, "r")

        try:
            for line in infile:
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue
                lines_processed += 1

                try:
                    surt_key, timestamp, json_data = parse_cdxj_line(line)
                except ValueError as e:
                    if verbose:
                        print(f"Error parsing line {lines_processed}: {e}", file=sys.stderr)
                    continue

                order_checker.check(surt_key)

                for name, sink in sinks.items():
                    rows = sink.process(surt_key, timestamp, json_data)
                    if rows:
                        for row in rows:
                            writers[name].writerow(row)
                            rows_written[name] += 1
        finally:
            if input_file != "-":
                infile.close()

        for name, sink in sinks.items():
            for row in sink.flush():
                writers[name].writerow(row)
                rows_written[name] += 1

        if verbose:
            print(f"# Processed {lines_processed} CDXJ lines", file=sys.stderr)
            for name in sinks:
                print(f"# Sink {name}: wrote {rows_written[name]} rows", file=sys.stderr)

    finally:
        for handle in files.values():
            handle.close()

    return rows_written


def main():
    """Command-line interface."""
    try:
        sink_options, remaining_argv = extract_sink_options(sys.argv[1:])
        args = build_arg_parser().parse_args(remaining_argv)

        field_separator = args.field_separator.encode().decode("unicode_escape")

        sink_names = [name.strip() for name in args.sink.split(",") if name.strip()]
        if not sink_names:
            print("Error: --sink must name at least one sink", file=sys.stderr)
            sys.exit(1)

        for name in sink_options:
            if name not in sink_names:
                print(
                    f"Warning: --sink-{name} options given but {name!r} is not in --sink list",
                    file=sys.stderr,
                )

        run(
            input_file=args.input_file,
            sink_names=sink_names,
            out_dir=args.out_dir,
            sink_options=sink_options,
            field_separator=field_separator,
            verbose=args.verbose,
        )

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
