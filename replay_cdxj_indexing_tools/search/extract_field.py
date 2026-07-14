#!/usr/bin/env python3
"""
extract_field.py

Extract specific fields from CDXJ records and output them line by line.

This tool reads CDXJ lines from stdin or a file, parses the JSON payload,
extracts a specified field, and writes the field value to stdout. Useful for
data extraction, field validation, and pipeline operations.

COMMAND-LINE USAGE
==================

Extract a single field:

    # Extract collection field from CDXJ
    cat index.cdxj | cdxj-extract-field --field collection

    # Extract status codes
    cat index.cdxj | cdxj-extract-field --field status

    # Extract with default value for missing fields
    cat index.cdxj | cdxj-extract-field --field collection --default "unknown"

    # Raw output (no JSON quotes around strings)
    cat index.cdxj | cdxj-extract-field --field collection --raw

    # Skip lines with missing field
    cat index.cdxj | cdxj-extract-field --field collection --skip-missing

Pipeline usage:

    # Count occurrences of each collection
    cat index.cdxj | cdxj-extract-field --field collection | sort | uniq -c

    # Extract URLs and validate
    cat index.cdxj | cdxj-extract-field --field url --raw | while read url; do
        echo "Processing: $url"
    done

    # Extract status codes and filter
    cat index.cdxj | cdxj-extract-field --field status | sort -n | uniq

CDXJ FORMAT
===========

Input:  com,example)/path 20200101000000 {"collection":"AWP-999","status":200,"url":"..."}
Output: "AWP-999"  (or AWP-999 with --raw)

The tool extracts from the JSON field portion of CDXJ records.

PYTHON API
==========

    from replay_cdxj_indexing_tools.search.extract_field import (
        extract_field_from_cdxj
    )

    # Extract fields and print
    extract_field_from_cdxj(
        input_file='-',
        field_name='collection',
        raw=False,
        default=None,
        skip_missing=False,
        verbose=False
    )

"""

import argparse
import json
import sys
from typing import Optional, Tuple


def parse_cdxj_line(line: str) -> Tuple[str, str, str, Optional[dict]]:
    """
    Parse CDXJ line into components.

    CDXJ format: <surt_key> <timestamp> [<json>]
    Example: com,example)/ 20200101000000 {"status": "200", "collection": "AWP-999"}

    Args:
        line: CDXJ line

    Returns:
        Tuple of (surt_key, timestamp, json_str, json_data)
        json_data is None if no JSON present

    Raises:
        ValueError: If line is malformed or JSON is invalid
    """
    parts = line.split(" ", 2)

    if len(parts) < 2:
        raise ValueError(f"Invalid CDXJ line (missing timestamp): {line[:50]}")

    surt_key = parts[0]
    timestamp = parts[1]
    json_str = parts[2] if len(parts) == 3 else ""

    # Parse JSON if present
    json_data = None
    if json_str.strip():
        try:
            json_data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in CDXJ line: {e}") from e

    return surt_key, timestamp, json_str, json_data


def extract_field_value(
    json_data: Optional[dict],
    field_name: str,
    raw: bool = False,
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Extract field value from CDXJ JSON data.

    Args:
        json_data: Parsed JSON dict from CDXJ line (or None)
        field_name: Field name to extract
        raw: If True, output raw values without JSON quotes (default: False)
        default: Default value if field missing (default: None)

    Returns:
        Field value as string, or default value, or None if not found and no default

    Example:
        >>> data = {'collection': 'AWP-999', 'status': 200}
        >>> extract_field_value(data, 'collection', raw=True)
        'AWP-999'
        >>> extract_field_value(data, 'collection', raw=False)
        '"AWP-999"'
        >>> extract_field_value(data, 'missing', default='unknown')
        'unknown'
    """
    if json_data is None or field_name not in json_data:
        return default

    value = json_data[field_name]

    if raw:
        # Raw mode: convert value to string without JSON encoding
        if isinstance(value, str):
            return value
        elif isinstance(value, bool):  # pylint: disable=no-else-return  # bool checked before str
            return "true" if value else "false"
        elif value is None:
            return "null"
        else:
            return str(value)
    else:
        # JSON mode: encode value as JSON
        return json.dumps(value, ensure_ascii=False)


def extract_field_from_cdxj(
    input_file: str = "-",
    field_name: str = "",
    raw: bool = False,
    default: Optional[str] = None,
    skip_missing: bool = False,
    verbose: bool = False,
    buffer_size: int = 1024 * 1024,
) -> Tuple[int, int]:
    """
    Extract field from CDXJ records and write to stdout.

    Reads CDXJ lines from input file (or stdin), extracts specified field
    from JSON payload, and writes field values to stdout.

    Args:
        input_file: Input CDXJ file, or '-' for stdin (default: stdin)
        field_name: JSON field name to extract (required)
        raw: Output raw values without JSON encoding (default: False)
        default: Default value for missing fields (default: None)
        skip_missing: Skip lines with missing field instead of using default (default: False)
        verbose: Print statistics to stderr (default: False)
        buffer_size: I/O buffer size in bytes (default: 1MB)

    Returns:
        Tuple of (lines_processed, lines_extracted)

    Raises:
        ValueError: If field_name is empty
    """
    if not field_name:
        raise ValueError("field_name is required")

    lines_processed = 0
    lines_extracted = 0
    lines_skipped = 0
    lines_errors = 0

    try:
        # Open input file
        if input_file == "-":
            infile = sys.stdin
        else:  # pylint: disable-next=consider-using-with,unspecified-encoding  # locale
            infile = open(input_file, "r", buffering=buffer_size)

        try:
            for line in infile:
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue

                lines_processed += 1

                try:
                    # Parse CDXJ line
                    _, _, _, json_data = parse_cdxj_line(line)

                    # Extract field value
                    value = extract_field_value(json_data, field_name, raw, default)

                    # Write output
                    if value is not None:
                        print(value)
                        lines_extracted += 1
                    elif not skip_missing:
                        # Default is None and skip_missing is False - still output nothing
                        # but count as processed (don't error)
                        pass
                    else:
                        # skip_missing is True and no value - skip silently
                        lines_skipped += 1

                except (ValueError, json.JSONDecodeError) as e:
                    lines_errors += 1
                    if verbose:
                        print(f"Error parsing line {lines_processed}: {e}", file=sys.stderr)

        finally:
            if input_file != "-":
                infile.close()

    except IOError as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        raise

    if verbose:
        print(
            f"Processed: {lines_processed} | "
            f"Extracted: {lines_extracted} | "
            f"Skipped: {lines_skipped} | "
            f"Errors: {lines_errors}",
            file=sys.stderr,
        )

    return lines_processed, lines_extracted


def main():
    """Main entry point for cdxj-extract-field command."""
    parser = argparse.ArgumentParser(
        description="Extract specific fields from CDXJ records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract collection field
  cat index.cdxj | cdxj-extract-field --field collection

  # Extract status codes
  cat index.cdxj | cdxj-extract-field --field status

  # Extract with default for missing fields
  cat index.cdxj | cdxj-extract-field --field collection --default "unknown"

  # Raw output (no JSON quotes for strings)
  cat index.cdxj | cdxj-extract-field --field collection --raw

  # Skip lines with missing field
  cat index.cdxj | cdxj-extract-field --field collection --skip-missing

  # Count unique values
  cat index.cdxj | cdxj-extract-field --field collection --raw | sort | uniq -c

  # From file instead of stdin
  cdxj-extract-field --field status -i index.cdxj
        """,
    )

    parser.add_argument(
        "--field",
        "-f",
        required=True,
        help="JSON field name to extract from CDXJ records",
    )

    parser.add_argument(
        "--input",
        "-i",
        dest="input_file",
        default="-",
        help="Input CDXJ file (default: stdin)",
    )

    parser.add_argument(
        "--raw",
        "-r",
        action="store_true",
        help="Output raw values without JSON encoding (no quotes for strings)",
    )

    parser.add_argument(
        "--default",
        "-d",
        default=None,
        help="Default value for missing fields (default: skip line)",
    )

    parser.add_argument(
        "--skip-missing",
        "-s",
        action="store_true",
        help="Skip lines where field is missing (instead of using default)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print statistics to stderr",
    )

    args = parser.parse_args()

    try:
        extract_field_from_cdxj(
            input_file=args.input_file,
            field_name=args.field,
            raw=args.raw,
            default=args.default,
            skip_missing=args.skip_missing,
            verbose=args.verbose,
        )

        sys.exit(0)

    except (ValueError, IOError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
