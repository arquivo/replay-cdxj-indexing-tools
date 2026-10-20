"""
Filtering of flat CDXJ input lines shared by merge-flat-cdxj and flat-cdxj-to-zipnum.

Blank lines are dropped silently. Corrupt lines (NUL bytes, or no key before the JSON
block) are dropped with a warning on stderr naming the input and line number, so the
damaged source file can be found and regenerated. Such lines would otherwise sort to
the top of the merged output and become a bogus first key in the ZipNum index.
"""

import sys
from typing import Optional, Union

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
                f"[WARNING] {source}:{lineno}: skipping corrupt CDXJ line "
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
