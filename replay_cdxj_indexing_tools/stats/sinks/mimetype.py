"""
MimetypeSink: counts CDXJ records by mimetype, optionally grouped by year or collection.

Registered as the "mimetype" sink entry point.
"""

from typing import Dict, Iterable, Optional, Sequence, Tuple

_VALID_GROUP_BY = (None, "year", "collection")


class MimetypeSink:
    """Counts records per mimetype. Small cardinality, so kept fully in memory."""

    def __init__(self) -> None:
        self.group_by: Optional[str] = None
        self._counts: Dict[Tuple[str, Optional[str]], int] = {}

    def configure(self, **opts: str) -> None:
        group_by = opts.pop("group-by", None)
        if opts:
            raise ValueError(f"Unknown option(s) for mimetype sink: {', '.join(opts)}")
        if group_by not in _VALID_GROUP_BY:
            raise ValueError(
                f"Invalid group-by={group_by!r} for mimetype sink "
                f"(expected one of: year, collection)"
            )
        self.group_by = group_by

    def process(  # pylint: disable=useless-return
        self, surt_key: str, timestamp: str, json_data: Optional[dict]
    ) -> Optional[Iterable[Sequence[str]]]:
        # pylint: disable=unused-argument
        mime = (json_data or {}).get("mime") or ""

        if self.group_by == "year":
            group_key: Optional[str] = timestamp[:4]
        elif self.group_by == "collection":
            group_key = (json_data or {}).get("collection") or ""
        else:
            group_key = None

        key = (mime, group_key)
        self._counts[key] = self._counts.get(key, 0) + 1
        return None

    def flush(self) -> Iterable[Sequence[str]]:
        rows = sorted(self._counts.items(), key=lambda item: (-item[1], item[0][0]))
        for (mime, group_key), count in rows:
            if self.group_by is None:
                yield (mime, str(count))
            else:
                yield (mime, group_key or "", str(count))

    def fieldnames(self) -> Sequence[str]:
        if self.group_by is None:
            return ("mime", "count")
        return ("mime", self.group_by, "count")

    def output_suffix(self) -> Optional[str]:
        return self.group_by
