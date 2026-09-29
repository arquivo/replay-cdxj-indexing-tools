"""
Shared building blocks for cdxj-stats: the Sink plugin protocol, entry-point
discovery, CDXJ line parsing, and SURT-order validation.
"""

import json
import sys
from importlib import metadata as importlib_metadata
from typing import Dict, Iterable, Optional, Protocol, Sequence, Tuple

SINKS_ENTRY_POINT_GROUP = "replay_cdxj_indexing_tools.sinks"


class Sink(Protocol):
    """Protocol every cdxj-stats sink (built-in or third-party) must implement."""

    def configure(self, **opts: str) -> None:
        """Validate and store sink options (e.g. group_by) parsed from --sink-<name>."""

    def process(
        self, surt_key: str, timestamp: str, json_data: Optional[dict]
    ) -> Optional[Iterable[Sequence[str]]]:
        """Consume one CDXJ record; optionally return rows ready to write (streaming sinks
        emit rows here when a key boundary is crossed; most calls return nothing)."""

    def flush(self) -> Iterable[Sequence[str]]:
        """Called once after the input is exhausted; yields any remaining rows."""

    def fieldnames(self) -> Sequence[str]:
        """CSV header row, which may depend on configured options such as group_by."""

    def output_suffix(self) -> Optional[str]:
        """Suffix appended to the sink's output filename (e.g. "year"), or None."""


def load_sinks(group: str = SINKS_ENTRY_POINT_GROUP) -> Dict[str, importlib_metadata.EntryPoint]:
    """Discover available sink plugins registered under `group`, keyed by entry-point name."""
    entry_points = importlib_metadata.entry_points(group=group)
    return {ep.name: ep for ep in entry_points}


def parse_cdxj_line(line: str) -> Tuple[str, str, Optional[dict]]:
    """
    Parse a CDXJ line into (surt_key, timestamp, json_data).

    CDXJ format: <surt_key> <timestamp> [<json>]
    json_data is None if no JSON payload is present or it fails to parse.
    """
    parts = line.split(" ", 2)
    if len(parts) < 2:
        raise ValueError(f"Invalid CDXJ line (missing timestamp): {line[:50]}")

    surt_key = parts[0]
    timestamp = parts[1]
    json_str = parts[2] if len(parts) == 3 else ""

    json_data = None
    if json_str.strip():
        try:
            json_data = json.loads(json_str)
        except json.JSONDecodeError:
            json_data = None

    return surt_key, timestamp, json_data


class OrderChecker:  # pylint: disable=too-few-public-methods
    """Warns once to stderr if SURT keys are not seen in non-decreasing order."""

    def __init__(self) -> None:
        self._last_key: Optional[str] = None
        self._warned = False

    def check(self, surt_key: str) -> None:
        if self._last_key is not None and surt_key < self._last_key and not self._warned:
            print(
                "# Warning: input does not appear to be SURT-sorted "
                f"({surt_key!r} follows {self._last_key!r}); "
                "domain-host/domain-etld1 stats may be split across non-contiguous rows",
                file=sys.stderr,
            )
            self._warned = True
        self._last_key = surt_key
