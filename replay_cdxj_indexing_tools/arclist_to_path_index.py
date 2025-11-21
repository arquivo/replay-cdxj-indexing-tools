#!/usr/bin/env python3
"""
arclist_to_path_index.py - Convert Arquivo.pt Arclist to Path Index Format
============================================================================

This tool converts Arquivo.pt arclist text files (containing URLs or paths to WARC/ARC files)
to pywb path index format (TSV: filename<tab>path), which can then be piped to
path-index-to-redis for loading into Redis.

Each arclist file represents a collection's list of WARC/ARC files. The tool can
process a single file, multiple files, or all files in a folder (useful when each
file corresponds to a different collection like AWP29_ARCS.txt, AWP30_ARCS.txt, etc.).

COMMAND-LINE USAGE
==================

Basic Examples:

    # Single arclist file with URLs
    arclist-to-path-index -i arclist.txt

    # Process entire folder of arclist files (one per collection)
    arclist-to-path-index -d /data/arclists

    # Convert and pipe to Redis
    arclist-to-path-index -d /data/arclists | \\
        path-index-to-redis -i - -k arquivo-2024

    # Multiple input files
    arclist-to-path-index -i AWP29_ARCS.txt -i AWP30_ARCS.txt

    # Read from stdin
    cat arclist.txt | arclist-to-path-index -i -

    # Verbose mode
    arclist-to-path-index -d /data/arclists --verbose

ARCLIST FORMAT
==============

Input arclist file is a simple text file with one WARC/ARC file location per line.
Each entry should be a URL, absolute path, or relative path:

    # URLs (HTTP/HTTPS)
    https://example.com/warcs/AWP-arquivo-20240101120000-00001.warc.gz
    http://storage.arquivo.pt/data/warcs/AWP-arquivo-20240101120500-00002.warc.gz

    # Absolute paths
    /mnt/storage/warcs/AWP-arquivo-20240101120000-00001.warc.gz
    /backup/warcs/AWP-arquivo-20240101120500-00002.warc.gz

    # Relative paths
    data/warcs/AWP-arquivo-20240101120000-00001.warc.gz
    ../backup/AWP-arquivo-20240101120500-00002.warc.gz

The tool automatically extracts the filename (basename) from each URL/path.

PATH INDEX OUTPUT FORMAT
=========================

Output follows pywb path index format (tab-separated):

    <filename>\\t<path>

The tool extracts the filename (basename) from each URL/path and outputs it alongside
the original entry.

Examples:

    Input arclist:
        https://storage.arquivo.pt/warcs/AWP-arquivo-20240101120000-00001.warc.gz
        /mnt/dc1/warcs/AWP-arquivo-20240101120500-00002.warc.gz

    Output:
        AWP-arquivo-20240101120000-00001.warc.gz\t
            https://storage.arquivo.pt/warcs/AWP-arquivo-20240101120000-00001.warc.gz
        AWP-arquivo-20240101120500-00002.warc.gz\t
            /mnt/dc1/warcs/AWP-arquivo-20240101120500-00002.warc.gz

PARAMETERS
==========

Required (one of):
    -i, --input FILE      Input arclist file, or '-' for stdin (can specify multiple)
    -d, --folder DIR      Folder containing arclist files (*.txt)

Optional:
    --output-separator S  Output separator (default: tab)
    -v, --verbose         Print progress information

INTEGRATION EXAMPLES
====================

1. Complete Pipeline with Redis:

    # Convert arclist and load directly into Redis
    arclist-to-path-index \\
        -i /data/arclists/2024-11.txt \\
        | path-index-to-redis -i - -k arquivo-2024-11 --verbose

2. Process Multiple Collections:

    # Process entire folder of arclist files
    arclist-to-path-index \\
        -d /data/arclists \\
        --verbose \\
        > pathindex.txt

3. Process Folder and Load to Redis:

    # Process all arclist files in a folder and load to Redis
    arclist-to-path-index \\
        -d /data/arclists \\
        | path-index-to-redis -i - -k arquivo-2024

4. Docker Integration:

    docker run -v /data:/data arquivo/replay-cdxj-indexing-tools \\
        arclist-to-path-index \\
        -d /data/arclists \\
        | docker run -i -v /data:/data arquivo/replay-cdxj-indexing-tools \\
        path-index-to-redis -i - -k collection

PRODUCTION WORKFLOW
===================

Typical Arquivo.pt workflow:

```bash
#!/bin/bash

# Configuration
ARCLISTS_FOLDER="/data/arclists"
REDIS_HOST="redis.arquivo.pt"
COLLECTION_KEY="arquivo-2024-11"

# Convert all arclist files and submit to Redis
arclist-to-path-index \\
    -d "$ARCLISTS_FOLDER" \\
    --verbose \\
    | path-index-to-redis \\
    -i - \\
    -k "$COLLECTION_KEY" \\
    --host "$REDIS_HOST" \\
    --batch-size 1000 \\
    --verbose
```

NOTES
=====

- Arclist files must have one URL/path per line
- Empty lines and lines starting with # are skipped
- Filenames (basenames) are automatically extracted from URLs/paths
- Output is compatible with path-index-to-redis tool
- For folder mode, all *.txt files in the folder are processed

"""

import argparse
import glob
import os
import sys
from typing import Dict, Iterator, List, TextIO


def read_arclist(
    input_file: TextIO,
    verbose: bool = False,
) -> Iterator[Dict[str, str]]:
    """
    Read arclist file and yield entry information.

    Args:
        input_file: Input file handle (or stdin)
        verbose: Print progress information

    Yields:
        Dictionary with 'filename' (extracted basename) and 'original' (the original URL/path)

    Example:
        >>> with open('arclist.txt', 'r') as f:
        ...     for entry in read_arclist(f):
        ...         print(f"{entry['filename']} -> {entry['original']}")
    """
    line_count = 0
    yielded_count = 0
    skipped_count = 0

    for line in input_file:
        original_entry = line.strip()
        line_count += 1

        # Skip empty lines and comments
        if not original_entry or original_entry.startswith("#"):
            skipped_count += 1
            continue

        # Always extract filename (basename) from URL/path
        # For URLs: https://example.com/path/file.warc.gz -> file.warc.gz
        # For paths: /mnt/storage/file.warc.gz -> file.warc.gz
        filename = os.path.basename(original_entry)

        yielded_count += 1
        yield {"filename": filename, "original": original_entry}

    if verbose:
        print(
            f"# Read {line_count} lines: {yielded_count} files, {skipped_count} skipped",
            file=sys.stderr,
        )


def get_arclist_files(folder_path: str, verbose: bool = False) -> List[str]:
    """
    Get all arclist files from a folder.

    Args:
        folder_path: Path to folder containing arclist files
        verbose: Print progress information

    Returns:
        List of arclist file paths

    Example:
        >>> files = get_arclist_files('/data/arclists')
        >>> print(len(files))
        8
    """
    if not os.path.isdir(folder_path):
        raise ValueError(f"Not a directory: {folder_path}")

    # Find all .txt files in the folder (typical arclist naming)
    pattern = os.path.join(folder_path, "*.txt")
    files = sorted(glob.glob(pattern))

    if verbose:
        print(f"# Found {len(files)} arclist files in {folder_path}", file=sys.stderr)
        for f in files:
            print(f"#   - {os.path.basename(f)}", file=sys.stderr)

    return files


def convert_arclist_to_path_index(
    input_path: str,
    output_separator: str = "\t",
    verbose: bool = False,
) -> int:
    """
    Convert arclist file to path index format.

    Args:
        input_path: Path to arclist file or '-' for stdin
        output_separator: Output field separator (default: tab)
        verbose: Print progress information

    Returns:
        Number of entries processed

    Example:
        >>> count = convert_arclist_to_path_index('arclist.txt', verbose=True)
    """
    if verbose:
        print("# Converting arclist to path index", file=sys.stderr)
        print(f"# Input: {input_path}", file=sys.stderr)

    # Open input file
    if input_path == "-":
        input_file = sys.stdin
    else:
        input_file = open(input_path, "r", encoding="utf-8")

    try:
        entry_count = 0

        for entry in read_arclist(
            input_file,
            verbose=False,
        ):
            filename = entry["filename"]
            original_entry = entry["original"]

            # Output path index line: filename<tab>path
            output_line = filename + output_separator + original_entry
            print(output_line)

            entry_count += 1

            if verbose and entry_count % 10000 == 0:
                print(f"# Processed {entry_count} entries...", file=sys.stderr)

        if verbose:
            print(f"# Conversion complete: {entry_count} entries", file=sys.stderr)

        return entry_count

    finally:
        if input_path != "-":
            input_file.close()


def main(argv=None):
    """
    Command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Convert Arquivo.pt arclist files to pywb path index format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single arclist file
  %(prog)s -i arclist.txt

  # Process entire folder of arclist files
  %(prog)s -d /data/arclists

  # Multiple input files
  %(prog)s -i AWP29_ARCS.txt -i AWP30_ARCS.txt

  # Pipeline to Redis
  %(prog)s -d /data/arclists | path-index-to-redis -i - -k arquivo-2024

  # Read from stdin
  cat arclist.txt | %(prog)s -i -

  # Verbose mode
  %(prog)s -d /data/arclists --verbose
        """,
    )

    # Input arguments (mutually exclusive: either files or folder)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "-i",
        "--input",
        action="append",
        dest="inputs",
        help="Input arclist file (one URL/path per line), or '-' for stdin "
        "(can specify multiple times)",
    )
    input_group.add_argument(
        "-d",
        "--folder",
        dest="folder",
        help="Folder containing arclist files (*.txt)",
    )

    # Optional arguments
    parser.add_argument(
        "--output-separator",
        default="\t",
        help="Output field separator (default: tab)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress information to stderr",
    )

    args = parser.parse_args(argv)

    # Determine input files
    if args.folder:
        # Get all arclist files from folder
        try:
            input_files = get_arclist_files(args.folder, verbose=args.verbose)
            if not input_files:
                print(f"Error: No arclist files (*.txt) found in {args.folder}", file=sys.stderr)
                sys.exit(1)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        # Use specified input files
        input_files = args.inputs

    # Run conversion
    try:
        total_entries = 0
        for input_file in input_files:
            entry_count = convert_arclist_to_path_index(
                input_path=input_file,
                output_separator=args.output_separator,
                verbose=args.verbose,
            )
            total_entries += entry_count

        if args.verbose:
            print(f"\n# Total entries processed: {total_entries}", file=sys.stderr)

        if total_entries == 0:
            print("Warning: No entries processed", file=sys.stderr)
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
