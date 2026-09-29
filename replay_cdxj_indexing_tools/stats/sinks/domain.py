"""
Domain-level capture-count sinks: DomainHostSink and DomainEtld1Sink.

Both share the same streaming accumulator; they differ only in how the grouping
key is derived from a record's SURT key. Because the input is assumed to be
SURT-sorted (see base.OrderChecker), all records sharing a key are contiguous,
so each sink only ever holds one key's accumulator in memory.

Registered as the "domain-host" and "domain-etld1" sink entry points.
"""

from typing import Dict, Iterable, List, Optional, Sequence

import tldextract

_VALID_GROUP_BY = (None, "year", "collection")


class _Bucket:  # pylint: disable=too-few-public-methods
    """Running totals for one (domain[, group]) accumulator."""

    __slots__ = ("total", "status_200", "oldest", "latest")

    def __init__(self) -> None:
        self.total = 0
        self.status_200 = 0
        self.oldest: Optional[str] = None
        self.latest: Optional[str] = None

    def update(self, timestamp: str, is_status_200: bool) -> None:
        self.total += 1
        if is_status_200:
            self.status_200 += 1
        if self.oldest is None or timestamp < self.oldest:
            self.oldest = timestamp
        if self.latest is None or timestamp > self.latest:
            self.latest = timestamp


class _DomainSinkBase:
    """Streaming per-key capture-count accumulator, flushed on key change."""

    def __init__(self) -> None:
        self.group_by: Optional[str] = None
        self._current_key: Optional[str] = None
        self._buckets: Dict[Optional[str], _Bucket] = {}

    def _key_for(self, surt_key: str) -> str:
        raise NotImplementedError

    def configure(self, **opts: str) -> None:
        group_by = opts.pop("group-by", None)
        if opts:
            raise ValueError(f"Unknown option(s) for domain sink: {', '.join(opts)}")
        if group_by not in _VALID_GROUP_BY:
            raise ValueError(
                f"Invalid group-by={group_by!r} for domain sink "
                f"(expected one of: year, collection)"
            )
        self.group_by = group_by

    def process(
        self, surt_key: str, timestamp: str, json_data: Optional[dict]
    ) -> Optional[Iterable[Sequence[str]]]:
        key = self._key_for(surt_key)

        rows = None
        if self._current_key is not None and key != self._current_key:
            rows = list(self._drain())
            self._buckets = {}

        self._current_key = key
        status = str((json_data or {}).get("status", ""))
        group_key = None
        if self.group_by == "year":
            group_key = timestamp[:4]
        elif self.group_by == "collection":
            group_key = (json_data or {}).get("collection") or ""

        self._buckets.setdefault(group_key, _Bucket()).update(timestamp, status == "200")
        return rows

    def flush(self) -> Iterable[Sequence[str]]:
        if self._current_key is not None:
            yield from self._drain()

    def _drain(self) -> Iterable[Sequence[str]]:
        assert self._current_key is not None
        current_key = self._current_key
        for group_key in sorted(self._buckets, key=lambda g: (g is None, g)):
            bucket = self._buckets[group_key]
            row: List[str] = [current_key]
            if self.group_by is not None:
                row.append(group_key or "")
            row.extend(
                [
                    str(bucket.total),
                    str(bucket.status_200),
                    bucket.oldest or "",
                    bucket.latest or "",
                ]
            )
            yield tuple(row)

    def fieldnames(self) -> Sequence[str]:
        fields = ["domain"]
        if self.group_by is not None:
            fields.append(self.group_by)
        fields.extend(["total_captures", "captures_status_200", "oldest_capture", "latest_capture"])
        return tuple(fields)

    def output_suffix(self) -> Optional[str]:
        return self.group_by


class DomainHostSink(_DomainSinkBase):
    """Groups captures by full SURT host, e.g. "pt,exemplo,www)"."""

    def _key_for(self, surt_key: str) -> str:
        return surt_key.split(")", 1)[0] + ")"


class DomainEtld1Sink(_DomainSinkBase):
    """Groups captures by registered domain / eTLD+1, e.g. "pt,exemplo,"."""

    def __init__(self) -> None:
        super().__init__()
        self._last_host_key: Optional[str] = None
        self._last_etld1_key: Optional[str] = None

    def _key_for(self, surt_key: str) -> str:
        host_key = surt_key.split(")", 1)[0] + ")"

        if host_key == self._last_host_key and self._last_etld1_key is not None:
            return self._last_etld1_key

        hostname = ".".join(reversed(host_key[:-1].split(",")))
        extracted = tldextract.extract(hostname)
        registered_domain = extracted.top_domain_under_public_suffix or hostname
        etld1_key = ",".join(reversed(registered_domain.split("."))) + ","

        self._last_host_key = host_key
        self._last_etld1_key = etld1_key
        return etld1_key
