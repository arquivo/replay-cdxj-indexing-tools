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
    ^pt,site,www\\)/admin/

PYTHON API
==========

    from replay_cdxj_indexing_tools.filter.blocklist import (
        load_blocklist,
        filter_cdxj_by_blocklist,
    )

    # Load blocklist patterns (raises ValueError if any line is not a
    # valid Python regex; pass blocklist_file= for grep-only syntax)
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
from typing import List, NamedTuple, Optional, Pattern, Tuple

# Exit status on timeout (matches POSIX timeout(1))
_EXIT_TIMEOUT = 124
# Largest --timeout accepted, in seconds (1 week)
MAX_TIMEOUT = 604800

# Longest blocklist line accepted for grep. Generous on purpose: real
# blocklists contain long host alternations.
GREP_MAX_PATTERN_LEN = 10000
# Largest repeat count accepted in {n}, {n,} and {n,m} (same as RE_DUP_MAX
# in POSIX's minimum limits)
GREP_MAX_REPEAT = 255
# Caps on the grep cost (see check_grep_pattern) of one line, and on the
# expanded size of the bounded repeats of the whole blocklist. grep expands a
# bounded repeat into that many copies of its operand, and its set-up time
# grows with the square of a line's expanded size times the number of such
# lines, so the per-line cap is the tighter one: 256 allows e.g. "x{255}",
# ".{0,64}" or "([a-z0-9-]{1,63}\.){1,4}", and the blocklist cap allows about
# 500 lines like "[a-z0-9-]{1,63}". Wildcards outside bounded repeats count
# toward the per-line cap only: many lines with a few of them each (such as
# ".*") are cheap for grep.
GREP_MAX_LINE_COST = 256
GREP_MAX_BLOCKLIST_COST = 32768
# Cost of a wildcard atom: "." is the most expensive for grep; bracket
# expressions and the \w, \W, \s and \S classes are cheaper
_DOT_COST = 4
_CLASS_COST = 1
_CLASS_ESCAPES = "wWsS"

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


class GrepCost(NamedTuple):
    """Cost of one blocklist line for grep (see check_grep_pattern)."""

    # Wildcards and bounded repeats, capped by GREP_MAX_LINE_COST
    line: int
    # Expanded size of the bounded repeats only, which callers add up across
    # a blocklist and cap by GREP_MAX_BLOCKLIST_COST
    repeats: int


def check_grep_pattern(pattern: str) -> GrepCost:
    """
    Reject a blocklist line containing constructs that are expensive for
    ``grep -E``: back-references (``\\1``-``\\9``), repeat counts above
    GREP_MAX_REPEAT, a cost above GREP_MAX_LINE_COST, or more than
    GREP_MAX_PATTERN_LEN characters.

    The cost of a line is the sum of the costs of its ``.``, bracket
    expressions, ``\\w``, ``\\W``, ``\\s``, ``\\S`` and bounded repeats; plain
    characters (and alternations of them) cost nothing. A ``.`` costs 4, a
    bracket expression or class escape 1. A bounded repeat costs its
    expanded size: its operand's size times its largest count, where each
    plain character also counts as 1, sizes add along a sequence and across
    alternatives, and nested repeats multiply. Only the bounded repeats
    count toward the blocklist's budget.

    grep's matcher is mostly DFA-based, so the Python-oriented backtracking
    checks in regex_safety do not apply here (and would reject common SURT
    idioms). Syntax is left to grep. These checks are not exhaustive.

    Returns:
        The line's cost, and the expanded size of its bounded repeats alone,
        which callers add up across a blocklist (see GREP_MAX_BLOCKLIST_COST).

    Raises:
        ValueError: with the reason, if the line is rejected.
    """
    if len(pattern) > GREP_MAX_PATTERN_LEN:
        raise ValueError(f"longer than {GREP_MAX_PATTERN_LEN} characters ({len(pattern)})")

    # One frame per open group: [finished alternatives, current alternative,
    # its last atom], each as (size, cost, expanded size of bounded repeats).
    frames = [[_NOTHING, _NOTHING, _NOTHING]]
    i = 0
    while i < len(pattern):
        frame = frames[-1]
        char = pattern[i]
        atom = (_DOT_COST, _DOT_COST, 0) if char == "." else (1, 0, 0)
        if char == "\\" and i + 1 < len(pattern):
            if pattern[i + 1] in "123456789":
                raise ValueError("back-references are expensive for grep")
            if pattern[i + 1] in _CLASS_ESCAPES:
                atom = (_CLASS_COST, _CLASS_COST, 0)
            i += 2  # an escape pair (e.g. a literal "\\{") is one atom
        elif char == "[":
            atom = (_CLASS_COST, _CLASS_COST, 0)
            i = _skip_bracket_expression(pattern, i)  # "{" is literal inside
        elif char == "(":
            frames.append([_NOTHING, _NOTHING, _NOTHING])
            i += 1
            continue
        elif char == ")" and len(frames) > 1:
            done, current, _last = frames.pop()
            frame = frames[-1]
            atom = _add_sizes(done, current)
            i += 1
        elif char == "|":
            frame[:] = [_add_sizes(frame[0], frame[1]), _NOTHING, _NOTHING]
            i += 1
            continue
        elif char == "{" and (match := _REPEAT_COUNTS.match(pattern, i)):
            size = frame[2][0] * _repeat_count(*match.groups())
            if size > GREP_MAX_LINE_COST:
                raise ValueError(
                    f"a bounded repeat expands to more than {GREP_MAX_LINE_COST}"
                    " characters, which is expensive for grep"
                )
            # The repeat replaces its operand, and costs its expanded size
            frame[1] = _add_sizes(frame[1], (size, size, size), minus=frame[2])
            frame[2] = (size, size, size)
            i = match.end()
            continue
        elif char in "*+?":
            i += 1
            continue
        else:
            i += 1  # any other char (including an unmatched ")") is one atom
        frame[1] = _add_sizes(frame[1], atom)
        frame[2] = atom

    cost = sum(done[1] + current[1] for done, current, _last in frames)
    if cost > GREP_MAX_LINE_COST:
        raise ValueError(
            f"bounded repeats and wildcards cost {cost}, more than {GREP_MAX_LINE_COST},"
            " which is expensive for grep"
        )
    return GrepCost(cost, sum(done[2] + current[2] for done, current, _last in frames))


# (size, cost, expanded size of bounded repeats) of an empty sequence
_NOTHING = (0, 0, 0)


def _add_sizes(
    first: Tuple[int, int, int],
    second: Tuple[int, int, int],
    minus: Tuple[int, int, int] = _NOTHING,
) -> Tuple[int, int, int]:
    """Add two (size, cost, repeats) triples, optionally replacing ``minus``."""
    return (
        first[0] + second[0] - minus[0],
        first[1] + second[1] - minus[1],
        first[2] + second[2] - minus[2],
    )


def _repeat_count(low: str, high: Optional[str]) -> int:
    """
    Return the largest count of a ``{low}``, ``{low,}`` or ``{low,high}``
    bound (either count may be empty).

    Raises:
        ValueError: if a count is above GREP_MAX_REPEAT.
    """
    counts = [int(count) for count in (low, high) if count]
    if any(count > GREP_MAX_REPEAT for count in counts):
        raise ValueError(f"repeat counts above {GREP_MAX_REPEAT} are expensive for grep")
    return max(counts, default=1)


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
    total = 0
    for line_num, pattern in patterns:
        try:
            repeats = check_grep_pattern(pattern).repeats
        except ValueError as e:
            rejected.append(f"  {source} {line_num}: {e}")
            continue
        # Report only the first line that takes the blocklist over the budget
        if total <= GREP_MAX_BLOCKLIST_COST < total + repeats:
            rejected.append(
                f"  {source} {line_num}: bounded repeats across the blocklist expand to"
                f" more than {GREP_MAX_BLOCKLIST_COST} characters, which is expensive for grep"
            )
        total += repeats
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

    The patterns are meant to be passed to filter_cdxj_by_blocklist(), which
    runs them through grep after checking each one with check_grep_pattern().
    Lines must also be valid Python regexes; a blocklist that relies on
    grep-only syntax (such as an unescaped ")" in a SURT prefix) should be
    passed to filter_cdxj_by_blocklist() with blocklist_file= instead.

    Raises:
        ValueError: listing every line that is not a valid Python regex.
            Loading fails as a whole, because filtering with part of a
            blocklist would publish the records the invalid lines target.
        IOError: If file cannot be read

    Example:
        >>> patterns = load_blocklist('blocklist.txt')
        >>> print(f"Loaded {len(patterns)} patterns")
    """
    patterns = []
    invalid = []
    for line_num, line in _read_pattern_lines(blocklist_path):
        try:
            patterns.append(re.compile(line))
        except re.error as e:
            invalid.append(f"  line {line_num}: {e}")
    if invalid:
        raise ValueError(
            f"{len(invalid)} blocklist line(s) are not valid regexes; refusing to"
            " load an incomplete blocklist:\n" + "\n".join(invalid)
        )
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
        blocklist_patterns: List of compiled patterns; their text is checked
            (see check_grep_pattern) and passed to grep. Prefer
            blocklist_file= for grep-only syntax such as an unescaped ')'.
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
    """argparse type: a number of seconds from 0 to MAX_TIMEOUT."""
    try:
        seconds = float(value)
    except ValueError:
        seconds = math.nan
    # Larger values overflow the timeout subprocess.run() passes to the OS
    if not 0 <= seconds <= MAX_TIMEOUT:
        raise argparse.ArgumentTypeError(
            f"must be a finite number >= 0 and <= {MAX_TIMEOUT} (1 week), got {value!r}"
        )
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
  2    invalid command-line arguments
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
        metavar="SECONDS",
        help="Abort if filtering takes longer than this many seconds, including time"
        f" spent waiting for input on stdin; at most {MAX_TIMEOUT} (1 week)"
        " (default: 0 = no limit)",
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
