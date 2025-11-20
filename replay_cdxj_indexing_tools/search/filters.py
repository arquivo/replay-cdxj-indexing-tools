"""
Filter utilities for CDXJ search results.
"""

import re
import json
from typing import Dict, Any, List, Optional


def normalize_timestamp(timestamp: str) -> str:
    """
    Normalize flexible timestamp to 14-digit format.

    Args:
        timestamp: Timestamp in various formats (2020, 202001, 20200101, etc.)

    Returns:
        14-digit timestamp string padded appropriately

    Examples:
        "2020" -> "20200101000000"
        "202012" -> "20201201000000"
        "20201225" -> "20201225000000"
    """
    # Remove any non-digit characters
    ts = "".join(c for c in timestamp if c.isdigit())

    # Pad to 14 digits with appropriate defaults
    if len(ts) < 14:
        # Default values for each position
        # YYYY MM DD HH MM SS
        # Pad month and day with 01, hours/minutes/seconds with 00
        defaults = "00000101000000"

        # Build the result by taking what we have and filling in defaults
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
    ):
        """
        Initialize filter.

        Args:
            from_ts: Start timestamp (flexible format)
            to_ts: End timestamp (flexible format)
            filters: List of filter expressions (e.g., "status=200", "mime~text/.*")
        """
        self.from_ts = normalize_timestamp(from_ts) if from_ts else None
        self.to_ts = normalize_timestamp(to_ts) if to_ts else None
        self.filter_rules = []

        if filters:
            for filter_expr in filters:
                self.filter_rules.append(self._parse_filter(filter_expr))

    def _parse_filter(self, expr: str) -> Dict[str, Any]:
        """
        Parse a filter expression.

        Supports:
        - field=value (exact match)
        - field!=value (not equal)
        - field~pattern (regex match)
        - field!~pattern (regex not match)
        """
        if "!~" in expr:
            field, pattern = expr.split("!~", 1)
            return {
                "field": field.strip(),
                "op": "!~",
                "value": pattern.strip(),
                "regex": re.compile(pattern.strip()),
            }
        elif "~" in expr:
            field, pattern = expr.split("~", 1)
            return {
                "field": field.strip(),
                "op": "~",
                "value": pattern.strip(),
                "regex": re.compile(pattern.strip()),
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
            except json.JSONDecodeError:
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
                    if not rule["regex"].search(field_value_str):
                        return False
                elif op == "!~":
                    if rule["regex"].search(field_value_str):
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
