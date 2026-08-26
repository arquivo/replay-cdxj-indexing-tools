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
    https://www.spam.pt/
    http://.*\\.adult.pt/
    https://www.site.pt/unwanted-section/

    # Block by file extension
    \\.pdf"
    \\.swf"

    # Block domain + specific path
    ^pt,site,www)/admin/

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
import functools
import os
import re
import subprocess
import sys
import tempfile
from typing import List, Optional, Pattern, Tuple

# Maximum compiled regex pattern length to prevent memory issues
_MAX_PATTERN_LEN = 1000
# Patterns known to cause catastrophic backtracking (ReDoS)
_REDOS_PATTERNS = [
    re.compile(r"\([^)]+\+\)\+"),  # (x+)+
    re.compile(r"\([^)]+\*\)\+"),  # (x*)+
    re.compile(r"\([^)]+\+\)\*"),  # (x+)*
    re.compile(r"\([^)]+\|[^)]+\)\*"),  # (a|b)*
    re.compile(r"\([^)]+\|[^)]+\)\+"),  # (a|b)+
]


@functools.lru_cache(maxsize=256)
def _compile_safe_regex(pattern: str) -> Pattern:
    """Compile a regex pattern, rejecting patterns known to risk ReDoS. Cached."""
    if len(pattern) > _MAX_PATTERN_LEN:
        raise ValueError(f"pattern too long ({len(pattern)} chars, max {_MAX_PATTERN_LEN})")
    for danger in _REDOS_PATTERNS:
        if danger.search(pattern):
            raise ValueError(f"pattern rejected due to ReDoS risk: {pattern!r}")
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"invalid regex pattern: {exc}") from exc


def _sanitize_patterns(pattern_strings: List[str]) -> List[str]:
    """
    Validate raw pattern strings, dropping (with a warning) any that are
    invalid regex, too long, or shaped to risk catastrophic backtracking.

    Used as the single choke point before patterns are written to the temp
    file handed to grep, so a raw blocklist file passed straight through
    can never bypass validation (see #75).
    """
    safe = []
    for pattern_text in pattern_strings:
        try:
            _compile_safe_regex(pattern_text)
            safe.append(pattern_text)
        except ValueError as e:
            print(f"Warning: Rejected blocklist pattern: {pattern_text}", file=sys.stderr)
            print(f"  Reason: {e}", file=sys.stderr)
    return safe


def load_blocklist(blocklist_path: str) -> List[Pattern]:
    """
    Load blocklist patterns from file.

    Each line is treated as a regex pattern. Lines starting with # are ignored
    as comments. Empty lines are also ignored.

    Security:
        Patterns are validated by _compile_safe_regex() against known
        catastrophic-backtracking shapes before re.compile() (CWE-1333).
        Pattern length is capped at _MAX_PATTERN_LEN chars.

    Args:
        blocklist_path: Path to blocklist file

    Returns:
        List of compiled regex patterns

    Raises:
        IOError: If file cannot be read

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
                patterns.append(_compile_safe_regex(line))
            except ValueError as e:
                print(f"Warning: Rejected pattern at line {line_num}: {line}", file=sys.stderr)
                print(f"  Reason: {e}", file=sys.stderr)
                # Continue loading other patterns

    return patterns


def filter_cdxj_by_blocklist(
    input_path: str,
    blocklist_patterns: List[Pattern],
    output_path: str = "-",
    buffer_size: int = 1024 * 1024,  # pylint: disable=unused-argument
    blocklist_file: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Filter CDXJ records matching blocklist patterns.

    Uses grep internally for best performance. This matches the behavior of the
    original bash implementation:
        grep -E -v -f blocklist.txt input.cdxj > output.cdxj

    Args:
        input_path: Input CDXJ file, or '-' for stdin
        blocklist_patterns: List of compiled regex Pattern objects to block
            (deprecated, use blocklist_file)
        output_path: Output file, or '-' for stdout (default: stdout)
        buffer_size: I/O buffer size in bytes (unused, kept for API compatibility)
        blocklist_file: Path to a raw blocklist file. Its patterns are
            re-validated (see #75) before use, so this is no longer a
            trust-the-caller performance shortcut.

    Returns:
        Tuple of (lines_kept, lines_blocked)

    Example:
        >>> kept, blocked = filter_cdxj_by_blocklist(
        ...     'input.cdxj', [],  'output.cdxj',
        ...     blocklist_file='blocklist.txt'
        ... )
        >>> print(f"Kept {kept} lines, blocked {blocked} lines")
    """
    # Gather raw pattern text from whichever source was given, then always
    # sanitize before writing the temp file grep will read. blocklist_file is
    # NOT trusted directly here (see #75): passing it straight to grep would
    # bypass any validation performed elsewhere (e.g. by load_blocklist()).
    if blocklist_file is not None:
        with open(blocklist_file, "r", encoding="utf-8") as f:
            pattern_strings = [
                stripped
                for line in f
                for stripped in [line.rstrip("\n\r")]
                if stripped and not stripped.startswith("#")
            ]
    else:
        pattern_strings = [pattern.pattern for pattern in blocklist_patterns]

    safe_patterns = _sanitize_patterns(pattern_strings)

    temp_blocklist = tempfile.NamedTemporaryFile(  # pylint: disable=consider-using-with
        mode="w", delete=False, suffix=".txt"
    )
    for pattern_text in safe_patterns:
        temp_blocklist.write(pattern_text + "\n")
    temp_blocklist.close()
    blocklist_file = temp_blocklist.name

    try:
        # Build grep command
        cmd = ["grep", "-a", "-E", "-v", "-f", blocklist_file]

        # Handle input
        if input_path == "-":
            cmd.append("-")
            stdin_input = sys.stdin
        else:
            cmd.append(input_path)
            stdin_input = None

        # Handle output and capture for counting
        if output_path == "-":
            # Output to stdout, capture to count lines
            result = subprocess.run(
                cmd, stdin=stdin_input, capture_output=True, text=True, check=False
            )
            sys.stdout.write(result.stdout)
            lines_kept = result.stdout.count("\n")
        else:
            # Output to file, capture to count lines
            result = subprocess.run(
                cmd, stdin=stdin_input, capture_output=True, text=True, check=False
            )
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result.stdout)
            lines_kept = result.stdout.count("\n")

        # Count blocked lines (total - kept)
        # We need to count total lines in input
        if input_path == "-":
            # Can't count stdin lines easily, estimate as 0 blocked
            lines_blocked = 0
        else:
            total_lines = 0
            with open(input_path, "r", encoding="utf-8") as f:
                for _ in f:
                    total_lines += 1
            lines_blocked = total_lines - lines_kept

        return lines_kept, lines_blocked

    finally:
        # A sanitized temp blocklist is always created above; clean it up.
        os.unlink(temp_blocklist.name)


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

        lines_kept, lines_blocked = filter_cdxj_by_blocklist(
            args.input, patterns, args.output, blocklist_file=args.blocklist
        )

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
