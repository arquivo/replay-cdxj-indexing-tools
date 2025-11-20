#!/usr/bin/env python3
"""
filter_excessive_urls.py

Filter out URLs that appear excessively in CDXJ files. This is useful for removing
spam, crawler traps, or overly-represented URLs from web archive indexes.

Can be used standalone or in pipelines between merge and ZipNum conversion.

COMMAND-LINE USAGE
==================

Two-pass approach (find then filter):

    # Pass 1: Find excessive URLs
    filter-excessive-urls find -i input.cdxj -n 1000 > excessive.txt

    # Pass 2: Filter them out
    filter-excessive-urls remove -i input.cdxj -b excessive.txt > filtered.cdxj

One-pass approach (find and filter in one go):

    # Find and remove in single pass
    filter-excessive-urls auto -i input.cdxj -n 1000 > filtered.cdxj

Pipeline usage:

    # After merge, before ZipNum
    merge-cdxj - file1.cdxj file2.cdxj | \\
        filter-excessive-urls auto -i - -n 1000 | \\
        cdxj-to-zipnum -o indexes -i -

    # Read from stdin, write to stdout
    cat merged.cdxj | filter-excessive-urls auto -i - -n 500 > cleaned.cdxj

PYTHON API
==========

    from replay_cdxj_indexing_tools.utils.filter_excessive_urls import (
        find_excessive_urls,
        filter_excessive_urls,
        process_pipeline
    )

    # Find excessive URLs
    excessive = find_excessive_urls('input.cdxj', threshold=1000)

    # Filter them out
    filter_excessive_urls('input.cdxj', excessive, 'output.cdxj')

    # One-pass processing
    process_pipeline('input.cdxj', 'output.cdxj', threshold=1000)

Author: Ivo Branco / GitHub Copilot
Date: November 2025
"""

import argparse
import sys
from collections import defaultdict
from typing import Dict, Set, Tuple


def extract_surt_key(line: str) -> str:
    """
    Extract SURT key from CDXJ line.

    CDXJ format: <surt_key> <timestamp> [<json>]
    Example: pt,governo,www)/ 20230615120000 {"url": "..."}

    Args:
        line: CDXJ line

    Returns:
        SURT key (first field before space)
    """
    return line.split(" ", 1)[0] if " " in line else line.strip()


def find_excessive_urls(input_path: str, threshold: int = 1000) -> Dict[str, int]:
    """
    Find URLs (SURT keys) that appear more than threshold times.

    Makes a single pass through the input file, counting occurrences
    of each SURT key. Returns those exceeding the threshold.

    Args:
        input_path: Path to CDXJ file, or '-' for stdin
        threshold: Minimum count to be considered excessive (default: 1000)

    Returns:
        Dictionary mapping excessive SURT keys to their counts

    Example:
        >>> excessive = find_excessive_urls('arquivo.cdxj', threshold=1000)
        >>> print(f"Found {len(excessive)} excessive URLs")
        >>> for surt, count in sorted(excessive.items(), key=lambda x: -x[1])[:5]:
        ...     print(f"{surt}: {count}")
    """
    counts: Dict[str, int] = defaultdict(int)

    # Open input (file or stdin)
    if input_path == "-":
        fh = sys.stdin
    else:
        fh = open(input_path, "r", encoding="utf-8")

    try:
        for line in fh:
            line = line.rstrip("\n\r")
            if not line:
                continue

            surt = extract_surt_key(line)
            counts[surt] += 1

        # Return only excessive ones
        excessive = {surt: count for surt, count in counts.items() if count > threshold}
        return excessive

    finally:
        if input_path != "-":
            fh.close()


def filter_excessive_urls(
    input_path: str,
    excessive_surts: Set[str],
    output_path: str = "-",
    buffer_size: int = 1024 * 1024,
) -> Tuple[int, int]:
    """
    Filter out excessive URLs from CDXJ file.

    Removes all lines whose SURT key is in the excessive_surts set.

    Args:
        input_path: Input CDXJ file, or '-' for stdin
        excessive_surts: Set of SURT keys to filter out
        output_path: Output file, or '-' for stdout (default: stdout)
        buffer_size: I/O buffer size in bytes (default: 1MB)

    Returns:
        Tuple of (lines_kept, lines_filtered)

    Example:
        >>> excessive = {'pt,spam,www)/': 5000, 'com,ads,)/': 10000}
        >>> kept, filtered = filter_excessive_urls('input.cdxj', excessive, 'output.cdxj')
        >>> print(f"Kept: {kept}, Filtered: {filtered}")
    """
    lines_kept = 0
    lines_filtered = 0

    # Open input
    if input_path == "-":
        input_fh = sys.stdin
    else:
        input_fh = open(input_path, "r", encoding="utf-8", buffering=buffer_size)

    # Open output
    if output_path == "-":
        output_fh = sys.stdout
    else:
        output_fh = open(output_path, "w", encoding="utf-8", buffering=buffer_size)

    try:
        for line in input_fh:
            surt = extract_surt_key(line)

            if surt in excessive_surts:
                lines_filtered += 1
            else:
                output_fh.write(line)
                lines_kept += 1

        return lines_kept, lines_filtered

    finally:
        if input_path != "-":
            input_fh.close()
        if output_path != "-":
            output_fh.close()


def process_pipeline(
    input_path: str,
    output_path: str = "-",
    threshold: int = 1000,
    buffer_size: int = 1024 * 1024,
    verbose: bool = False,
) -> Tuple[int, int, int]:
    """
    Find and filter excessive URLs in one pass (requires two file reads).

    This is a convenience function that combines find_excessive_urls()
    and filter_excessive_urls() for simpler usage.

    Note: Requires reading the input file twice (once to count, once to filter).
    For stdin input, this won't work - use the two-pass approach instead.

    Args:
        input_path: Input CDXJ file (cannot be stdin)
        output_path: Output file, or '-' for stdout (default: stdout)
        threshold: Count threshold for excessive URLs (default: 1000)
        buffer_size: I/O buffer size in bytes (default: 1MB)
        verbose: Print statistics to stderr (default: False)

    Returns:
        Tuple of (excessive_count, lines_kept, lines_filtered)

    Raises:
        ValueError: If input_path is '-' (stdin not supported for auto mode)

    Example:
        >>> excessive, kept, filtered = process_pipeline('input.cdxj', 'output.cdxj', threshold=500)
        >>> print(f"Found {excessive} excessive URLs, kept {kept}, filtered {filtered} lines")
    """
    if input_path == "-":
        raise ValueError("Auto mode requires a file (stdin not supported). Use two-pass approach.")

    # Pass 1: Find excessive URLs
    if verbose:
        print(f"Pass 1: Finding URLs with > {threshold} occurrences...", file=sys.stderr)

    excessive = find_excessive_urls(input_path, threshold)
    excessive_count = len(excessive)

    if verbose:
        print(f"Found {excessive_count} excessive URLs", file=sys.stderr)
        if excessive_count > 0:
            top_5 = sorted(excessive.items(), key=lambda x: -x[1])[:5]
            print("Top 5 excessive URLs:", file=sys.stderr)
            for surt, count in top_5:
                print(f"  {surt}: {count} occurrences", file=sys.stderr)

    # Pass 2: Filter them out
    if verbose:
        print("Pass 2: Filtering excessive URLs...", file=sys.stderr)

    excessive_set = set(excessive.keys())
    lines_kept, lines_filtered = filter_excessive_urls(
        input_path, excessive_set, output_path, buffer_size
    )

    if verbose:
        total = lines_kept + lines_filtered
        pct = (lines_filtered / total * 100) if total > 0 else 0
        print(
            f"Complete: Kept {lines_kept} lines, filtered {lines_filtered} lines ({pct:.1f}%)",
            file=sys.stderr,
        )

    return excessive_count, lines_kept, lines_filtered


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Filter excessive URLs from CDXJ files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Two-pass approach (for stdin/pipeline usage)
  %(prog)s find -i input.cdxj -n 1000 > excessive.txt
  %(prog)s remove -i input.cdxj -b excessive.txt > filtered.cdxj

  # One-pass approach (requires file, not stdin)
  %(prog)s auto -i input.cdxj -n 1000 -o filtered.cdxj

  # Pipeline usage
  merge-cdxj - *.cdxj | %(prog)s remove -i - -b excessive.txt | cdxj-to-zipnum -o idx -i -

  # Auto mode with verbose output
  %(prog)s auto -i arquivo.cdxj -o cleaned.cdxj -n 500 -v
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # find command
    find_parser = subparsers.add_parser(
        "find",
        help="Find excessive URLs (Pass 1)",
        description="Find URLs that appear more than threshold times",
    )
    find_parser.add_argument("-i", "--input", required=True, help="Input CDXJ file, or - for stdin")
    find_parser.add_argument(
        "-n",
        "--threshold",
        type=int,
        default=1000,
        help="Threshold for excessive occurrences (default: 1000)",
    )

    # remove command
    remove_parser = subparsers.add_parser(
        "remove",
        help="Remove excessive URLs (Pass 2)",
        description="Filter out URLs from a blacklist",
    )
    remove_parser.add_argument(
        "-i", "--input", required=True, help="Input CDXJ file, or - for stdin"
    )
    remove_parser.add_argument(
        "-b",
        "--blacklist",
        required=True,
        help="File with excessive URLs (one per line, optionally with counts)",
    )
    remove_parser.add_argument(
        "-o", "--output", default="-", help="Output file, or - for stdout (default: stdout)"
    )

    # auto command
    auto_parser = subparsers.add_parser(
        "auto",
        help="Find and remove in one go (requires file, not stdin)",
        description="Automatically find and filter excessive URLs",
    )
    auto_parser.add_argument(
        "-i", "--input", required=True, help="Input CDXJ file (cannot be stdin for auto mode)"
    )
    auto_parser.add_argument(
        "-o", "--output", default="-", help="Output file, or - for stdout (default: stdout)"
    )
    auto_parser.add_argument(
        "-n",
        "--threshold",
        type=int,
        default=1000,
        help="Threshold for excessive occurrences (default: 1000)",
    )
    auto_parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print statistics to stderr"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "find":
            # Find excessive URLs and output them
            excessive = find_excessive_urls(args.input, args.threshold)

            # Output format: surt count (sorted by count descending)
            for surt, count in sorted(excessive.items(), key=lambda x: -x[1]):
                print(f"{surt} {count}")

            print(
                f"# Found {len(excessive)} URLs with > {args.threshold} occurrences",
                file=sys.stderr,
            )

        elif args.command == "remove":
            # Load blacklist
            excessive_surts = set()
            with open(args.blacklist, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    # Extract just the SURT (first field)
                    surt = line.split()[0]
                    excessive_surts.add(surt)

            print(f"# Loaded {len(excessive_surts)} URLs to filter", file=sys.stderr)

            # Filter
            lines_kept, lines_filtered = filter_excessive_urls(
                args.input, excessive_surts, args.output
            )

            total = lines_kept + lines_filtered
            pct = (lines_filtered / total * 100) if total > 0 else 0
            print(
                f"# Kept {lines_kept} lines, filtered {lines_filtered} lines ({pct:.1f}%)",
                file=sys.stderr,
            )

        elif args.command == "auto":
            # Auto mode
            excessive_count, lines_kept, lines_filtered = process_pipeline(
                args.input, args.output, args.threshold, verbose=args.verbose
            )

            if not args.verbose:
                # Print summary if not already printed
                total = lines_kept + lines_filtered
                pct = (lines_filtered / total * 100) if total > 0 else 0
                print(
                    f"# Found {excessive_count} excessive URLs, "
                    f"kept {lines_kept} lines, filtered {lines_filtered} lines ({pct:.1f}%)",
                    file=sys.stderr,
                )

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(1)
    except (IOError, OSError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
