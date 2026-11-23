"""
Checks on flat CDXJ input lines shared by merge-flat-cdxj and flat-cdxj-to-zipnum.

Blank lines are dropped silently. Corrupt lines (NUL bytes, or no key before the JSON
block) are dropped with a warning on stderr naming the input and line number, so the
damaged source file can be found and regenerated. Such lines would otherwise sort to
the top of the merged output and become a bogus first key in the ZipNum index.

Each input must also be sorted in byte order (the order of ``LC_ALL=C sort``). The
k-way merge and pywb's ZipNum binary search both rely on it, and a file sorted in
another order silently yields an index that misses records, so SortOrderChecker
raises UnsortedInputError at the first line that is out of order; the tools then
delete their partial output with remove_partial_output.
"""

import os
import stat
import sys
from typing import Any, AnyStr, Optional, Union

# Per-line warnings printed before the rest are only counted in the summary
MAX_WARNINGS = 10
PREVIEW_CHARS = 80


def corrupt_reason(line: Union[str, bytes]) -> Optional[str]:
    """
    Return why a non-blank line cannot be a CDXJ record, or None if it looks valid.

    Works on both text (str) and raw (bytes) lines.
    """
    if isinstance(line, bytes):
        nul_count = line.count(b"\x00")
        starts_with_json = line.strip().startswith(b"{")
    else:
        nul_count = line.count("\x00")
        starts_with_json = line.strip().startswith("{")
    if nul_count:
        return f"{nul_count} NUL bytes"
    if starts_with_json:
        return "no key before the JSON block"
    return None


def _preview(line: Union[str, bytes]) -> str:
    """Short printable excerpt of a line, without NULs, for warnings."""
    if isinstance(line, bytes):
        text = line.decode("utf-8", errors="replace")
    else:
        text = line
    text = text.replace("\x00", "").strip()
    if len(text) > PREVIEW_CHARS:
        text = text[:PREVIEW_CHARS] + "..."
    return repr(text)


def _display_source(source: str) -> str:
    """Input name for messages, escaped if it holds control characters."""
    return source if source.isprintable() else repr(source)


class CdxjLineFilter:
    """
    Decide which input lines are written out, and report the corrupt ones.

    One instance is shared across all inputs of a run so the warning cap and the
    final count cover the whole run.
    """

    def __init__(self, max_warnings: int = MAX_WARNINGS):
        self.max_warnings = max_warnings
        self.corrupt_count = 0

    def accept(self, line: Union[str, bytes], source: str, lineno: int) -> bool:
        """Return True if the line should be kept; warn about corrupt lines."""
        if not line.strip():
            return False
        reason = corrupt_reason(line)
        if reason is None:
            return True
        self.corrupt_count += 1
        if self.corrupt_count <= self.max_warnings:
            print(
                f"[WARNING] {_display_source(source)}:{lineno}: skipping corrupt CDXJ line "
                f"({reason}): {_preview(line)}",
                file=sys.stderr,
            )
        return False

    def report(self) -> None:
        """Print a summary of skipped corrupt lines, if any."""
        if not self.corrupt_count:
            return
        suppressed = self.corrupt_count - self.max_warnings
        note = f" ({suppressed} not shown)" if suppressed > 0 else ""
        print(
            f"[WARNING] Skipped {self.corrupt_count} corrupt CDXJ line(s){note}. "
            "The source CDXJ should be regenerated.",
            file=sys.stderr,
        )


class UnsortedInputError(Exception):
    """An input line sorts before the line preceding it."""

    def __init__(
        self,
        source: str,
        prev_lineno: int,
        prev_line: Union[str, bytes],
        lineno: int,
        line: Union[str, bytes],
    ):
        self.source = source
        self.prev_lineno = prev_lineno
        self.lineno = lineno
        # Only the start of each line is shown; slicing first bounds the copies of a
        # huge line made while building the preview.
        limit = PREVIEW_CHARS * 4
        super().__init__(
            f"{_display_source(source)} is not sorted: "
            f"line {lineno} sorts before line {prev_lineno}\n"
            f"  line {prev_lineno}: {_preview(prev_line[:limit])}\n"
            f"  line {lineno}: {_preview(line[:limit])}\n"
            "CDXJ input must be sorted in byte order, e.g. with: LC_ALL=C sort"
        )


class SortOrderChecker:  # pylint: disable=too-few-public-methods
    """
    Check that the lines of one input are in byte order, the order of ``LC_ALL=C sort``.

    Whole lines are compared, and equal lines are allowed. A line is reported when it
    sorts before the previous one both as read and without its line terminator (sort
    ignores the terminator), so every file sorted by ``LC_ALL=C sort`` passes, as does
    a duplicate last line without a newline.
    Text lines compare by code point, which for UTF-8 input is the same order as
    comparing their bytes.
    """

    __slots__ = ("source", "prev", "prev_lineno")

    def __init__(self, source: str):
        self.source = source
        # str or bytes, the same type as the input lines; None before the first line
        self.prev: Any = None
        self.prev_lineno = 0

    def check(self, line: AnyStr, lineno: int) -> None:
        """Raise UnsortedInputError if line sorts before the previously checked line."""
        prev = self.prev
        # Runs for every input line, so the terminators are only stripped to confirm
        # an out-of-order raw comparison.
        if prev is not None and line < prev and _strip_eol(line) < _strip_eol(prev):
            raise UnsortedInputError(self.source, self.prev_lineno, prev, lineno, line)
        self.prev = line
        self.prev_lineno = lineno


def _strip_eol(line: AnyStr) -> AnyStr:
    """Return line without its trailing CR/LF characters."""
    if isinstance(line, bytes):
        return line.rstrip(b"\r\n")
    return line.rstrip("\r\n")


def remove_partial_output(path: str) -> None:
    """
    Delete an output file left incomplete by a failed run.

    Only a regular file is deleted; lstat() is used so a symlink is not followed. A
    device, FIFO or symlink given as the output path is left in place, as is a file
    that cannot be deleted, with a warning on stderr. Nothing is raised: this runs
    while the error that stopped the run is propagating.
    """
    try:
        if stat.S_ISREG(os.lstat(path).st_mode):
            os.unlink(path)
            return
        reason = "not a regular file"
    except FileNotFoundError:
        return
    except OSError as exc:
        reason = str(exc)
    print(
        f"[WARNING] Partial output left in place ({reason}): {_display_source(path)}",
        file=sys.stderr,
    )
