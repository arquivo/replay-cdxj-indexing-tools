#!/usr/bin/env python3
"""
filter_blocklist.py

Filter out CDXJ records matching blocklist patterns (regex patterns).
This tool helps remove unwanted content like spam domains, adult content, or
other blocked patterns from web archive indexes.

Replaces the legacy apply_blacklist.sh script (grep -E -v) with better
performance and cross-platform compatibility.

COMMAND-LINE USAGE
==================

Basic usage:

    # Filter using a blocklist file
    filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj

    # Read from stdin, write to stdout (pipeline mode)
    cat input.cdxj | filter-blocklist -i - -b blocklist.txt > output.cdxj

    # Use with merge pipeline
    merge-cdxj - *.cdxj | filter-blocklist -i - -b blocklist.txt | cdxj-to-zipnum -o idx -i -

Blocklist file format:

    # Lines starting with # are comments
    # Each line is a regex pattern to match against ENTIRE CDXJ lines

    # Block by SURT domain prefix (entire domain)
    ^pt,spam,
    ^pt,adult,

    # Block by URL patterns (matches JSON url field)
    https://www\.spam\.pt/
    http://.*\.adult\.pt/
    https://www\.site\.pt/unwanted-section/

    # Block by file extension
    \.pdf"
    \.swf"

    # Block domain + specific path
    ^pt,site,www\)/admin/

PYTHON API
==========

    from replay_cdxj_indexing_tools.filter.blocklist import (
        load_blocklist,
        filter_cdxj_by_blocklist,
    )

    # Load blocklist patterns
    patterns = load_blocklist('blocklist.txt')

    # Filter CDXJ
    kept, blocked = filter_cdxj_by_blocklist(
        'input.cdxj',
        patterns,
        'output.cdxj'
    )

    print(f"Kept: {kept}, Blocked: {blocked}")

"""

import argparse
import re
import sys
from typing import List, Pattern, Tuple


def load_blocklist(blocklist_path: str) -> List[Pattern]:
    """
    Load blocklist patterns from file.

    Each line is treated as a regex pattern. Lines starting with # are ignored
    as comments. Empty lines are also ignored.

    Args:
        blocklist_path: Path to blocklist file

    Returns:
        List of compiled regex patterns

    Raises:
        IOError: If file cannot be read
        re.error: If a pattern is invalid regex

    Example:
        >>> patterns = load_blocklist('blocklist.txt')
        >>> print(f"Loaded {len(patterns)} patterns")
    """
    patterns = []

    with open(blocklist_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip("\n\r")

            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue

            try:
                # Compile pattern
                pattern = re.compile(line)
                patterns.append(pattern)
            except re.error as e:
                print(f"Warning: Invalid regex pattern at line {line_num}: {line}", file=sys.stderr)
                print(f"  Error: {e}", file=sys.stderr)
                # Continue loading other patterns

    return patterns


def filter_cdxj_by_blocklist(
    input_path: str,
    blocklist_patterns: List[Pattern],
    output_path: str = "-",
    buffer_size: int = 1024 * 1024,
) -> Tuple[int, int]:
    """
    Filter CDXJ records matching blocklist patterns.

    Reads CDXJ line by line, checks each line against all blocklist patterns
    using regex matching (like grep -E -v). If any pattern matches the entire
    CDXJ line, that line is blocked. Otherwise, it's written to output.

    This matches the behavior of the original bash implementation:
        grep -E -v -f blocklist.txt input.cdxj > output.cdxj

    Args:
        input_path: Input CDXJ file, or '-' for stdin
        blocklist_patterns: List of compiled regex Pattern objects to block
        output_path: Output file, or '-' for stdout (default: stdout)
        buffer_size: I/O buffer size in bytes (default: 1MB)

    Returns:
        Tuple of (lines_kept, lines_blocked)

    Example:
        >>> patterns = load_blocklist('blocklist.txt')
        >>> kept, blocked = filter_cdxj_by_blocklist('input.cdxj', patterns, 'output.cdxj')
        >>> print(f"Kept {kept} lines, blocked {blocked} lines")
    """
    lines_kept = 0
    lines_blocked = 0

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
        # Match patterns against entire CDXJ line (like grep -E)
        for line in input_fh:
            # Check if any pattern matches this line
            blocked = False
            for pattern in blocklist_patterns:
                if pattern.search(line):
                    blocked = True
                    break
            
            if blocked:
                lines_blocked += 1
            else:
                output_fh.write(line)
                lines_kept += 1

        return lines_kept, lines_blocked

    finally:
        if input_path != "-":
            input_fh.close()
        if output_path != "-":
            output_fh.close()


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Filter CDXJ records matching blocklist patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Filter using blocklist file
  %(prog)s -i input.cdxj -b blocklist.txt -o output.cdxj

  # Pipeline mode (stdin/stdout)
  cat input.cdxj | %(prog)s -i - -b blocklist.txt > output.cdxj

  # Use in complete pipeline
  merge-cdxj - *.cdxj | %(prog)s -i - -b blocklist.txt | cdxj-to-zipnum -o idx -i -

Blocklist file format:
  Each line is a regex pattern. Lines starting with # are comments.

  Example blocklist.txt:
    # Block spam domains
    ^pt,spam,
    ^pt,adult,

    # Block specific content
    /ads/
    "mime": "application/x-shockwave-flash"
        """,
    )

    parser.add_argument("-i", "--input", required=True, help="Input CDXJ file, or - for stdin")
    parser.add_argument(
        "-b", "--blocklist", required=True, help="Blocklist file with regex patterns (one per line)"
    )
    parser.add_argument(
        "-o", "--output", default="-", help="Output file, or - for stdout (default: stdout)"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Print statistics to stderr")

    args = parser.parse_args()

    try:
        # Load blocklist
        if args.verbose:
            print(f"Loading blocklist from {args.blocklist}...", file=sys.stderr)

        patterns = load_blocklist(args.blocklist)

        if args.verbose:
            print(f"Loaded {len(patterns)} blocklist patterns", file=sys.stderr)

        if len(patterns) == 0:
            print("Warning: No patterns loaded from blocklist", file=sys.stderr)

        # Filter
        if args.verbose:
            print(f"Filtering {args.input}...", file=sys.stderr)

        lines_kept, lines_blocked = filter_cdxj_by_blocklist(args.input, patterns, args.output)

        # Print statistics
        total = lines_kept + lines_blocked
        pct = (lines_blocked / total * 100) if total > 0 else 0

        print(
            f"# Kept {lines_kept} lines, blocked {lines_blocked} lines ({pct:.1f}%)",
            file=sys.stderr,
        )

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(1)
    except (IOError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
