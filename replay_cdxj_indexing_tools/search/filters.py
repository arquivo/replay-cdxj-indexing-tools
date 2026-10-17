"""
Filter utilities for CDXJ search results.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from replay_cdxj_indexing_tools.regex_safety import (
    DEFAULT_REGEX_TIMEOUT,
    compile_safe_regex,
    search_with_time_limit,
)

logger = logging.getLogger(__name__)

# Maximum length for a single filter expression (field op value)
_MAX_FILTER_EXPR_LEN = 2000


def normalize_timestamp(timestamp: str) -> str:
    """
    Normalize flexible timestamp to 14-digit format.

    Args:
        timestamp: Timestamp in various formats (2020, 202001, 20200101, etc.)

    Returns:
        14-digit timestamp string padded appropriately

    Raises:
        ValueError: If timestamp contains non-digit characters

    Examples:
        "2020" -> "20200101000000"
        "202012" -> "20201201000000"
        "20201225" -> "20201225000000"
    """
    if not timestamp.isdigit():
        logger.warning("Invalid non-numeric timestamp rejected: %r", timestamp)
        raise ValueError(f"Invalid timestamp (must be numeric): {timestamp!r}")

    ts = timestamp
    if len(ts) < 14:
        # YYYY MM DD HH MM SS — pad month/day with 01, time with 00
        defaults = "00000101000000"
        result = list(defaults)
        for i, char in enumerate(ts):
            result[i] = char
        ts = "".join(result)

    return ts[:14]


class CDXJFilter:  # pylint: disable=too-few-public-methods
    """Filter for CDXJ records based on various criteria."""

    def __init__(
        self,
        from_ts: Optional[str] = None,
        to_ts: Optional[str] = None,
        filters: Optional[List[str]] = None,
        regex_timeout: Optional[float] = DEFAULT_REGEX_TIMEOUT,
    ):
        """
        Initialize filter.

        Args:
            from_ts: Start timestamp (flexible format)
            to_ts: End timestamp (flexible format)
            filters: List of filter expressions (e.g., "status=200", "mime~text/.*")
            regex_timeout: Per-search time budget in seconds for ``~``/``!~``
                rules (None or 0 disables it). On timeout, matches() raises
                RegexTimeoutError instead of silently dropping the record.
        """
        self.regex_timeout = regex_timeout
        self.from_ts = normalize_timestamp(from_ts) if from_ts else None
        self.to_ts = normalize_timestamp(to_ts) if to_ts else None
        self.filter_rules = []

        if filters:
            for filter_expr in filters:
                if len(filter_expr) > _MAX_FILTER_EXPR_LEN:
                    raise ValueError(
                        f"filter expression too long: {len(filter_expr)} chars"
                        f" (max {_MAX_FILTER_EXPR_LEN})"
                    )
                if "\x00" in filter_expr:
                    raise ValueError("filter expression contains null bytes")
                self.filter_rules.append(self._parse_filter(filter_expr))

    def _parse_filter(self, expr: str) -> Dict[str, Any]:
        """
        Parse a filter expression.

        Supports:
        - field=value (exact match)
        - field!=value (not equal)
        - field~pattern (regex match)
        - field!~pattern (regex not match)

        Security:
            ReDoS: patterns are validated by compile_safe_regex() against known
            catastrophic-backtracking patterns before re.compile(). (CWE-1333)
            Pattern length is capped at MAX_PATTERN_LEN chars. Matching time
            is bounded in matches() (see regex_safety.search_with_time_limit).
        """
        if "!~" in expr:  # pylint: disable=no-else-return  # elif chain for operator priority
            field, pattern = expr.split("!~", 1)
            return {
                "field": field.strip(),
                "op": "!~",
                "value": pattern.strip(),
                "regex": compile_safe_regex(pattern.strip()),
            }
        elif "~" in expr:  # pylint: disable=no-else-return  # elif order
            field, pattern = expr.split("~", 1)
            return {
                "field": field.strip(),
                "op": "~",
                "value": pattern.strip(),
                "regex": compile_safe_regex(pattern.strip()),
            }
        elif "!=" in expr:
            field, value = expr.split("!=", 1)
            return {"field": field.strip(), "op": "!=", "value": value.strip()}
        elif "=" in expr:
            field, value = expr.split("=", 1)
            return {"field": field.strip(), "op": "=", "value": value.strip()}
        else:
            raise ValueError(f"Invalid filter expression: {expr}")

    def matches(self, line: str) -> bool:
        """
        Check if a CDXJ line matches all filter criteria.

        Args:
            line: CDXJ line (SURT timestamp JSON)

        Returns:
            True if line matches all filters

        Raises:
            RegexTimeoutError: a regex rule exceeded ``regex_timeout``. The
                whole run should be aborted: skipping the record would yield
                silently incomplete output, and an attacker-chosen pattern
                could still cost one full timeout per record.
        """
        # pylint: disable=too-many-return-statements
        parts = line.strip().split(" ", 2)
        if len(parts) < 3:
            return False

        _surt, timestamp, json_str = parts

        # Check timestamp range
        if self.from_ts and timestamp < self.from_ts:
            return False
        if self.to_ts and timestamp > self.to_ts:
            return False

        # Check field filters
        if self.filter_rules:
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "Malformed JSON in CDXJ line (line rejected): %s | %.100s", exc, line.strip()
                )
                return False

            for rule in self.filter_rules:
                field = rule["field"]
                op = rule["op"]
                value = rule["value"]

                # Get field value from JSON
                field_value = data.get(field)
                if field_value is None:
                    return False

                # Convert to string for comparison
                field_value_str = str(field_value)

                # Apply operator
                if op == "=":
                    if field_value_str != value:
                        return False
                elif op == "!=":
                    if field_value_str == value:
                        return False
                elif op == "~":
                    if not search_with_time_limit(
                        rule["regex"], field_value_str, self.regex_timeout
                    ):
                        return False
                elif op == "!~":
                    if search_with_time_limit(rule["regex"], field_value_str, self.regex_timeout):
                        return False

        return True


def sort_lines(lines: List[str]) -> List[str]:
    """
    Sort CDXJ lines by timestamp within each SURT key.

    Args:
        lines: List of CDXJ lines

    Returns:
        Sorted list of CDXJ lines
    """

    def sort_key(line: str):
        parts = line.strip().split(" ", 2)
        if len(parts) >= 2:
            return (parts[0], parts[1])  # (SURT, timestamp)
        return (line, "")

    return sorted(lines, key=sort_key)


def deduplicate_lines(lines: List[str]) -> List[str]:
    """
    Remove duplicate CDXJ lines (same SURT + timestamp).

    Args:
        lines: List of CDXJ lines

    Returns:
        Deduplicated list of CDXJ lines
    """
    seen = set()
    result = []

    for line in lines:
        parts = line.strip().split(" ", 2)
        if len(parts) >= 2:
            key = (parts[0], parts[1])  # (SURT, timestamp)
            if key not in seen:
                seen.add(key)
                result.append(line)
        else:
            result.append(line)

    return result
