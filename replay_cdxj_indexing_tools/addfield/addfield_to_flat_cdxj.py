#!/usr/bin/env python3
"""
addfield_to_flat_cdxj.py

Add custom JSON fields to flat CDXJ records. Unlike filters that remove data,
this tool enriches records by adding new metadata fields to existing CDXJ lines.

Designed to work in parallel on individual CDXJ files before merging for maximum
performance, following the same pattern as filter-blocklist.

COMMAND-LINE USAGE
==================

Basic field addition:

    # Add custom fields from command line
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f collection=AWP999
      -f source=arquivo

    # Read from stdin, write to stdout (pipeline mode)
    cat input.cdxj | addfield-to-flat-cdxj -i - -f batch=daily > enriched.cdxj

    # Add timestamp-based fields
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f indexed_date=$(date +%Y%m%d)

Using custom field functions:

    # Use Python function for complex logic
    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj --function addfield_func.py

Pipeline usage (recommended - add fields before merge):

    # Parallel field addition in collection processing
    parallel -j 16 'addfield-to-flat-cdxj -i {} -o {.}.enriched.cdxj -f collection=COL' ::: *.cdxj
    merge-flat-cdxj - *.enriched.cdxj | flat-cdxj-to-zipnum -o indexes -i -

CUSTOM FIELD FUNCTIONS
=======================

For complex field logic, create a Python file with an addfield() function:

    # addfield_func.py
    import json

    def addfield(surt_key, timestamp, json_data):
        '''
        Add custom fields to CDXJ record.

        Args:
            surt_key: SURT key (e.g., "pt,arquivo)/")
            timestamp: Timestamp string (e.g., "20231115120000")
            json_data: Parsed JSON dict from CDXJ line

        Returns:
            dict: Modified JSON data with new fields
        '''
        # Extract year from timestamp
        year = timestamp[:4]
        json_data['collection'] = f'ARQUIVO-{year}'

        # Add domain from URL
        if 'url' in json_data:
            from urllib.parse import urlparse
            domain = urlparse(json_data['url']).netloc
            json_data['domain'] = domain

        return json_data

Then use it:

    addfield-to-flat-cdxj -i input.cdxj -o output.cdxj --function addfield_func.py

CDXJ FORMAT
===========

Input:  pt,arquivo)/ 20231115120000 {"url": "...", "status": "200"}
Output: pt,arquivo)/ 20231115120000 {"url": "...", "status": "200", "collection": "ARQUIVO-2023"}

The JSON object is enriched with additional fields while preserving original data.

PYTHON API
==========

    from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import (
        addfield_to_cdxj,
        load_addfield_function
    )

    # Simple field addition
    addfield_to_cdxj(
        'input.cdxj',
        'output.cdxj',
        fields={'collection': 'ARQUIVO-2024', 'source': 'web'}
    )

    # Using custom function
    addfield_func = load_addfield_function('addfield_func.py')
    addfield_to_cdxj('input.cdxj', 'output.cdxj', addfield_func=addfield_func)

"""

import argparse
import json
import sys
from typing import Callable, Dict, Optional, Tuple


def parse_cdxj_line(line: str) -> Tuple[str, str, str, Optional[dict]]:
    """
    Parse CDXJ line into components.

    CDXJ format: <surt_key> <timestamp> [<json>]
    Example: pt,governo,www)/ 20230615120000 {"url": "...", "status": "200"}

    Args:
        line: CDXJ line

    Returns:
        Tuple of (surt_key, timestamp, json_str, json_data)
        json_data is None if no JSON present

    Example:
        >>> line = 'pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/"}'
        >>> surt, ts, json_str, data = parse_cdxj_line(line)
        >>> print(surt, ts, data['url'])
        pt,arquivo)/ 20231115120000 https://arquivo.pt/
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


def format_cdxj_line(surt_key: str, timestamp: str, json_data: Optional[dict]) -> str:
    """
    Format CDXJ line from components.

    Args:
        surt_key: SURT key
        timestamp: Timestamp string
        json_data: JSON data dict (or None)

    Returns:
        Formatted CDXJ line (with newline)

    Example:
        >>> data = {'url': 'https://arquivo.pt/', 'status': '200'}
        >>> line = format_cdxj_line('pt,arquivo)/', '20231115120000', data)
        >>> print(line.strip())
        pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/", "status": "200"}
    """
    if json_data:
        # Use separators to minimize whitespace (compact JSON)
        json_str = json.dumps(json_data, ensure_ascii=False, separators=(",", ":"))
        return f"{surt_key} {timestamp} {json_str}\n"
    else:
        return f"{surt_key} {timestamp}\n"


def addfield_to_cdxj(
    input_path: str,
    output_path: str = "-",
    fields: Optional[Dict[str, str]] = None,
    addfield_func: Optional[Callable] = None,
    buffer_size: int = 1024 * 1024,
    verbose: bool = False,
) -> Tuple[int, int]:
    """
    Add fields to flat CDXJ records.

    Reads CDXJ line by line, parses JSON, adds fields, and writes enriched output.
    You can provide either simple key-value fields or a custom field function.

    Args:
        input_path: Input CDXJ file, or '-' for stdin
        output_path: Output file, or '-' for stdout (default: stdout)
        fields: Dictionary of field_name -> value to add (default: None)
        addfield_func: Custom function(surt, timestamp, json_data) -> json_data (default: None)
        buffer_size: I/O buffer size in bytes (default: 1MB)
        verbose: Print statistics to stderr (default: False)

    Returns:
        Tuple of (lines_processed, lines_skipped)

    Raises:
        ValueError: If neither fields nor addfield_func provided, or if both provided

    Example:
        >>> # Simple field addition
        >>> processed, skipped = addfield_to_cdxj(
        ...     'input.cdxj',
        ...     'output.cdxj',
        ...     fields={'collection': 'ARQUIVO-2024', 'batch': 'daily'}
        ... )
        >>> print(f"Processed {processed} lines")
    """
    if not fields and not addfield_func:
        raise ValueError("Must provide either 'fields' or 'addfield_func'")

    if fields and addfield_func:
        raise ValueError("Cannot provide both 'fields' and 'addfield_func' (choose one)")

    lines_processed = 0
    lines_skipped = 0

    # Open input
    if input_path == "-":
        input_fh = sys.stdin
    else:
        input_fh = open(input_path, "r", encoding="utf-8", buffering=buffer_size)

    # Open output
    if output_path == "-":
        output_fh = sys.stdout
    else:
        output_fh = open(output_path, "w", encoding="utf-8", buffering=buffer_size)

    try:
        for line_num, line in enumerate(input_fh, 1):
            line = line.rstrip("\n\r")

            # Skip empty lines
            if not line:
                lines_skipped += 1
                output_fh.write("\n")
                continue

            try:
                # Parse CDXJ line
                surt_key, timestamp, _, json_data = parse_cdxj_line(line)

                # Initialize JSON data if not present
                if json_data is None:
                    json_data = {}

                # Apply field addition
                if addfield_func:
                    # Use custom function
                    json_data = addfield_func(surt_key, timestamp, json_data)
                elif fields:  # mypy: fields is guaranteed to be not None here
                    # Add simple fields
                    for field_name, field_value in fields.items():
                        json_data[field_name] = field_value

                # Write enriched line
                output_line = format_cdxj_line(surt_key, timestamp, json_data)
                output_fh.write(output_line)

                lines_processed += 1

            except (ValueError, json.JSONDecodeError) as e:
                if verbose:
                    print(
                        f"Warning: Skipping invalid line {line_num}: {str(e)[:50]}",
                        file=sys.stderr,
                    )
                lines_skipped += 1
                # Write original line unchanged
                output_fh.write(line + "\n")

        return lines_processed, lines_skipped

    finally:
        if input_path != "-":
            input_fh.close()
        if output_path != "-":
            output_fh.close()


def load_addfield_function(function_path: str) -> Callable:
    """
    Load field addition function from Python file.

    The file must define an addfield(surt_key, timestamp, json_data) function
    that returns the modified json_data dict.

    Args:
        function_path: Path to Python file with addfield() function

    Returns:
        The addfield function

    Raises:
        IOError: If file cannot be read
        AttributeError: If addfield() function not found in file

    Example:
        >>> addfield_func = load_addfield_function('my_addfield.py')
        >>> result = addfield_func('pt,arquivo)/', '20231115120000', {'url': '...'})
    """
    import importlib.util  # pylint: disable=import-outside-toplevel

    # Load module from file
    spec = importlib.util.spec_from_file_location("addfield_module", function_path)
    if spec is None or spec.loader is None:
        raise IOError(f"Cannot load module from {function_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get addfield function
    if not hasattr(module, "addfield"):
        raise AttributeError(
            f"Module {function_path} must define an addfield"
            "(surt_key, timestamp, json_data) function"
        )

    return module.addfield


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Add custom fields to flat CDXJ records",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Add simple fields
  %(prog)s -i input.cdxj -o output.cdxj -f collection=ARQUIVO-2024 -f source=web

  # Pipeline mode (stdin/stdout)
  cat input.cdxj | %(prog)s -i - -f batch=daily > enriched.cdxj

  # Parallel field addition (recommended for large collections)
  parallel -j 16 '%(prog)s -i {} -o {.}.enriched.cdxj -f collection=COL' ::: *.cdxj

  # Use custom field function
  %(prog)s -i input.cdxj -o output.cdxj --function addfield_func.py

  # Combine with pipeline
  %(prog)s -i input.cdxj -f year=2024 | filter-blocklist -i - -b list.txt

Custom Field Function File Format:
  Create a Python file with an addfield() function:

    # addfield_func.py
    def addfield(surt_key, timestamp, json_data):
        # Add custom fields based on data
        json_data['collection'] = 'ARQUIVO-2024'
        json_data['year'] = timestamp[:4]
        return json_data

  Then use it:
    %(prog)s -i input.cdxj --function addfield_func.py
        """,
    )

    parser.add_argument("-i", "--input", required=True, help="Input CDXJ file, or - for stdin")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file, or - for stdout (default: stdout)"
    )

    # Field addition options (mutually exclusive)
    field_group = parser.add_mutually_exclusive_group(required=True)
    field_group.add_argument(
        "-f",
        "--field",
        action="append",
        dest="fields",
        metavar="KEY=VALUE",
        help="Add field to JSON (can be used multiple times). Format: key=value",
    )
    field_group.add_argument(
        "--function",
        dest="function_file",
        help="Python file with addfield(surt_key, timestamp, json_data) function",
    )

    parser.add_argument("-v", "--verbose", action="store_true", help="Print statistics to stderr")

    args = parser.parse_args()

    try:
        # Parse fields or load function
        if args.fields:
            # Parse key=value pairs
            fields_dict = {}
            for field_spec in args.fields:
                if "=" not in field_spec:
                    print(
                        f"Error: Invalid field format '{field_spec}' (expected key=value)",
                        file=sys.stderr,
                    )
                    sys.exit(1)

                key, value = field_spec.split("=", 1)
                fields_dict[key] = value

            if args.verbose:
                print(f"Adding {len(fields_dict)} fields to CDXJ records", file=sys.stderr)
                for k, v in fields_dict.items():
                    print(f"  {k} = {v}", file=sys.stderr)

            # Add fields
            lines_processed, lines_skipped = addfield_to_cdxj(
                args.input, args.output, fields=fields_dict, verbose=args.verbose
            )

        else:
            # Load and use custom function
            if args.verbose:
                print(
                    f"Loading field addition function from {args.function_file}...",
                    file=sys.stderr,
                )

            addfield_func = load_addfield_function(args.function_file)

            if args.verbose:
                print("Adding fields to CDXJ records...", file=sys.stderr)

            lines_processed, lines_skipped = addfield_to_cdxj(
                args.input, args.output, addfield_func=addfield_func, verbose=args.verbose
            )

        # Print statistics
        total = lines_processed + lines_skipped
        print(
            f"# Processed {lines_processed} lines, skipped {lines_skipped} lines (total: {total})",
            file=sys.stderr,
        )

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(1)
    except (IOError, OSError, ValueError, AttributeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
