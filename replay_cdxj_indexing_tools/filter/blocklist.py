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
import math
import os
import re
import subprocess
import sys
import tempfile
from typing import List, Optional, Pattern, Tuple

from replay_cdxj_indexing_tools.regex_safety import compile_safe_regex

# Exit status on timeout (matches POSIX timeout(1))
_EXIT_TIMEOUT = 124

# Longest blocklist line accepted for grep. Generous on purpose: real
# blocklists contain long host alternations.
GREP_MAX_PATTERN_LEN = 10000
# Largest repeat count accepted in {n}, {n,} and {n,m} (same as RE_DUP_MAX
# in POSIX's minimum limits)
GREP_MAX_REPEAT = 255

_REPEAT_COUNTS = re.compile(r"\{(\d*)(?:,(\d*))?\}")


def _read_pattern_lines(path: str) -> List[Tuple[int, str]]:
    """
    Return ``(line_number, pattern)`` for every non-blank, non-comment line.

    Read as UTF-8, ignoring a leading byte-order mark so it does not become
    part of the first pattern.
    """
    lines = []
    with open(path, "r", encoding="utf-8-sig") as f:
        for line_num, line in enumerate(f, 1):
            line = line.rstrip("\n\r")
            if line and not line.startswith("#"):
                lines.append((line_num, line))
    return lines


def check_grep_pattern(pattern: str) -> None:
    """
    Reject a blocklist line containing constructs that are expensive for
    ``grep -E``: back-references (``\\1``-``\\9``), repeat counts above
    GREP_MAX_REPEAT, or more than GREP_MAX_PATTERN_LEN characters.

    grep's matcher is mostly DFA-based, so the Python-oriented backtracking
    checks in regex_safety do not apply here (and would reject common SURT
    idioms). Syntax is left to grep.

    Raises:
        ValueError: with the reason, if the line is rejected.
    """
    if len(pattern) > GREP_MAX_PATTERN_LEN:
        raise ValueError(f"longer than {GREP_MAX_PATTERN_LEN} characters ({len(pattern)})")

    # Drop escape pairs (e.g. a literal "\\{") and bracket expressions, where
    # "{" is literal, before looking at repeat counts.
    unescaped = []
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "\\" and i + 1 < len(pattern):
            if pattern[i + 1] in "123456789":
                raise ValueError("back-references are expensive for grep")
            i += 2
            continue
        if char == "[":
            i = _skip_bracket_expression(pattern, i)
            continue
        unescaped.append(char)
        i += 1

    for match in _REPEAT_COUNTS.finditer("".join(unescaped)):
        counts = [int(count) for count in match.groups() if count]
        if any(count > GREP_MAX_REPEAT for count in counts):
            raise ValueError(f"repeat counts above {GREP_MAX_REPEAT} are expensive for grep")


def _skip_bracket_expression(pattern: str, start: int) -> int:
    """Return the index just past the bracket expression opening at ``start``."""
    i = start + 1
    if i < len(pattern) and pattern[i] == "^":
        i += 1
    if i < len(pattern) and pattern[i] == "]":
        i += 1  # a leading "]" is literal
    while i < len(pattern):
        if pattern[i] == "[" and i + 1 < len(pattern) and pattern[i + 1] in ":.=":
            close = pattern.find(pattern[i + 1] + "]", i + 2)
            if close != -1:
                i = close + 2
                continue
        if pattern[i] == "]":
            return i + 1
        i += 1
    return i  # unterminated: grep reports the syntax error


def _check_grep_patterns(patterns: List[Tuple[int, str]], source: str) -> None:
    """
    Fail closed if any blocklist line is rejected: filtering with only part
    of a blocklist would publish the records the rejected lines target.

    Raises:
        ValueError: listing every rejected line number and reason.
    """
    rejected = []
    for line_num, pattern in patterns:
        try:
            check_grep_pattern(pattern)
        except ValueError as e:
            rejected.append(f"  {source} {line_num}: {e}")
    if rejected:
        raise ValueError(
            f"{len(rejected)} blocklist pattern(s) rejected; refusing to filter"
            " with an incomplete blocklist:\n" + "\n".join(rejected)
        )


def load_blocklist(blocklist_path: str) -> List[Pattern]:
    """
    Load blocklist patterns from file as compiled Python regexes.

    Each line is treated as a regex pattern. Lines starting with # are ignored
    as comments. Empty lines are also ignored.

    For library use only: the CLI hands the file to grep instead (see
    filter_cdxj_by_blocklist), whose syntax and checks differ.

    Security:
        Patterns are validated by compile_safe_regex() against known
        catastrophic-backtracking shapes before re.compile() (CWE-1333).
        Pattern length is capped at MAX_PATTERN_LEN chars. Rejected
        patterns are skipped with a warning.

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
    for line_num, line in _read_pattern_lines(blocklist_path):
        try:
            patterns.append(compile_safe_regex(line))
        except ValueError as e:
            print(f"Warning: Rejected pattern at line {line_num}: {line}", file=sys.stderr)
            print(f"  Reason: {e}", file=sys.stderr)
    return patterns


def filter_cdxj_by_blocklist(
    input_path: str,
    blocklist_patterns: List[Pattern],
    output_path: str = "-",
    buffer_size: int = 1024 * 1024,  # pylint: disable=unused-argument
    blocklist_file: Optional[str] = None,
    timeout: Optional[float] = None,
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
        blocklist_file: Path to a raw blocklist file. Every line is checked
            (see check_grep_pattern) before use.
        timeout: Kill grep and raise subprocess.TimeoutExpired after this
            many seconds, including time spent waiting for input on stdin
            (None or 0 = no limit). Nothing is written on timeout.

    Returns:
        Tuple of (lines_kept, lines_blocked)

    Raises:
        subprocess.TimeoutExpired: grep ran longer than ``timeout``.
        subprocess.CalledProcessError: grep failed (an error status, or
            killed by a signal). Nothing is written.
        ValueError: if any blocklist line is rejected. Nothing is written:
            filtering without it would publish the records it targets.

    Example:
        >>> kept, blocked = filter_cdxj_by_blocklist(
        ...     'input.cdxj', [],  'output.cdxj',
        ...     blocklist_file='blocklist.txt'
        ... )
        >>> print(f"Kept {kept} lines, blocked {blocked} lines")
    """
    # Always check the pattern text and hand grep a fresh file holding only
    # the checked lines; a raw blocklist_file is never passed to grep (#75).
    if blocklist_file is not None:
        patterns = _read_pattern_lines(blocklist_file)
        source = "line"
    else:
        patterns = [(num, p.pattern) for num, p in enumerate(blocklist_patterns, 1)]
        source = "pattern"
    _check_grep_patterns(patterns, source)

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=False, suffix=".txt"
        ) as temp_blocklist:
            temp_path = temp_blocklist.name
            for _, pattern_text in patterns:
                temp_blocklist.write(pattern_text + "\n")

        # "--" ends option parsing so an input path starting with "-" can
        # never be read as a grep option.
        cmd = ["grep", "-a", "-E", "-v", "-f", temp_path, "--"]

        # Handle input
        if input_path == "-":
            cmd.append("-")
            stdin_input = sys.stdin
        else:
            cmd.append(input_path)
            stdin_input = None

        result = subprocess.run(
            cmd,
            stdin=stdin_input,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout or None,
        )
        # grep exits 0 when lines were kept and 1 when none were. Anything
        # else is an error (e.g. a pattern grep -E cannot parse) or, if
        # negative, death by a signal: fail instead of writing partial output.
        if result.returncode not in (0, 1):
            raise subprocess.CalledProcessError(result.returncode, cmd, stderr=result.stderr)

        if output_path == "-":
            sys.stdout.write(result.stdout)
        else:
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
        if temp_path is not None:
            os.unlink(temp_path)


def _seconds(value: str) -> float:
    """argparse type: a finite, non-negative number of seconds."""
    try:
        seconds = float(value)
    except ValueError:
        seconds = math.nan
    if not math.isfinite(seconds) or seconds < 0:
        raise argparse.ArgumentTypeError(f"must be a finite number >= 0, got {value!r}")
    return seconds


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

  Every line is checked before grep runs. If any line is rejected (it uses
  a construct that is expensive for grep), nothing is written and the
  rejected line numbers are reported on stderr.

Exit status:
  0    success
  1    error, including a rejected blocklist line (no output is written)
  124  --timeout expired (no output is written)
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
    parser.add_argument(
        "--timeout",
        type=_seconds,
        default=0,
        help="Abort if filtering takes longer than this many seconds, including time"
        " spent waiting for input on stdin (default: 0 = no limit)",
    )

    args = parser.parse_args()

    try:
        if not _read_pattern_lines(args.blocklist):
            print("Warning: No patterns in blocklist", file=sys.stderr)

        if args.verbose:
            print(f"Filtering {args.input} with {args.blocklist}...", file=sys.stderr)

        lines_kept, lines_blocked = filter_cdxj_by_blocklist(
            args.input, [], args.output, blocklist_file=args.blocklist, timeout=args.timeout
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
    except subprocess.TimeoutExpired as e:
        print(f"Error: filtering timed out after {e.timeout} seconds", file=sys.stderr)
        sys.exit(_EXIT_TIMEOUT)
    except subprocess.CalledProcessError as e:
        grep_stderr = (e.stderr or "").strip()
        print(f"Error: grep failed (exit {e.returncode}): {grep_stderr}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except (IOError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
