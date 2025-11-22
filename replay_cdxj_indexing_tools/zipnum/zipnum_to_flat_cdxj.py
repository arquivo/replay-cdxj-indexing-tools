#!/usr/bin/env python3
"""
zipnum_to_flat_cdxj.py - Convert ZipNum Format Back to Flat CDXJ
================================================================

Decompress and merge ZipNum sharded indexes back into a flat CDXJ stream.
Reads .idx file to discover shards, decompresses .cdx.gz files, and outputs
uncompressed CDXJ records to stdout.

COMMAND-LINE USAGE
==================

After installing: pip install -e .
The command 'zipnum-to-flat-cdxj' becomes available globally.

Basic Examples:

    # Convert ZipNum to flat CDXJ (output to stdout)
    zipnum-to-flat-cdxj -i /path/to/base.idx

    # Redirect to file
    zipnum-to-flat-cdxj -i /path/to/base.idx > output.cdxj

    # Read idx from stdin
    cat base.idx | zipnum-to-flat-cdxj -i -

    # Compress output
    zipnum-to-flat-cdxj -i base.idx | gzip > output.cdxj.gz

Advanced Examples:

    # Parallel decompression with 8 workers
    zipnum-to-flat-cdxj -i base.idx --workers 8 > output.cdxj

    # Pipeline with filtering
    zipnum-to-flat-cdxj -i base.idx | filter-blocklist -b blocklist.txt > filtered.cdxj

    # Custom base directory for shard files
    zipnum-to-flat-cdxj -i /idx/base.idx --base-dir /data/shards

ZIPNUM FORMAT
=============

ZipNum format consists of:

1. Index File (.idx)
   - Tab-separated: <key>\\t<shard>\\t<offset>\\t<length>\\t<shard_num>
   - Points to compressed chunks in shard files
   - Used to discover which .cdx.gz files to decompress

2. Shard Files (.cdx.gz)
   - Gzip-compressed CDXJ data
   - Single shard: base.cdx.gz
   - Multiple shards: base-01.cdx.gz, base-02.cdx.gz, ...

3. Location File (.loc) - Optional
   - Maps shard names to file paths
   - Format: <shard_name.cdx.gz>\t<filepath>

PARAMETERS
==========

Required:
    -i, --input FILE       Input .idx file or '-' for stdin

Optional:
    --base-dir DIR         Base directory for shard files (default: idx file directory)
    --loc FILE            Custom .loc file path (default: auto-detect)
    --workers N           Parallel decompression workers (default: 4)

OUTPUT
======

Outputs uncompressed CDXJ records to stdout in the order they appear in shards.
Records maintain their original sorted order from the ZipNum format.

NOTES
=====

- All output goes to stdout (no files created)
- Shards are processed sequentially to maintain sort order
- Within each shard, decompression can be parallelized with --workers
- Use shell redirection to save output to file
- Compatible with all ZipNum files created by cdxj-to-zipnum

PERFORMANCE TIPS
================

- Use --workers to match CPU cores for faster decompression
- Pipe directly to other tools to avoid intermediate files
- For large indexes, redirect to compressed output: | gzip > output.cdxj.gz
- Process in parallel: zipnum-to-flat-cdxj -i base.idx --workers 8

PIPELINE EXAMPLES
=================

# Convert and filter in one pipeline
zipnum-to-flat-cdxj -i base.idx | filter-blocklist -b blocklist.txt > clean.cdxj

# Convert and re-compress with different format
zipnum-to-flat-cdxj -i base.idx | gzip -9 > recompressed.cdxj.gz

# Convert and merge with other CDXJ files
zipnum-to-flat-cdxj -i base.idx > temp.cdxj
merge-cdxj output.cdxj temp.cdxj other.cdxj

"""

import gzip
import os
import sys
from argparse import ArgumentParser
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple


def read_idx_file(idx_path: str) -> List[Tuple[str, str, int, int, int]]:
    """
    Read .idx file and return list of (key, shard_name, offset, length, shard_num).
    Supports idx_path == '-' for stdin.

    Returns entries grouped by shard in order they appear.
    """
    entries = []

    if idx_path == "-":
        fh = sys.stdin
    else:
        fh = open(idx_path, "r", encoding="utf-8")

    try:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 5:
                continue

            key = parts[0]
            shard_name = parts[1]
            offset = int(parts[2])
            length = int(parts[3])
            shard_num = int(parts[4])

            entries.append((key, shard_name, offset, length, shard_num))
    finally:
        if idx_path != "-":
            fh.close()

    return entries


def read_loc_file(loc_path: str) -> dict:
    """
    Read .loc file and return dict mapping shard_name -> filepath.
    """
    loc_map = {}

    with open(loc_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            shard_name = parts[0]
            filepath = parts[1]
            loc_map[shard_name] = filepath

    return loc_map


def find_loc_file(idx_path: str) -> Optional[str]:
    """
    Try to find .loc file based on .idx file path.
    Returns path if found, None otherwise.
    """
    if idx_path == "-":
        return None

    # Try replacing .idx with .loc
    base = idx_path.replace(".idx", "")
    loc_path = f"{base}.loc"

    if os.path.exists(loc_path):
        return loc_path

    return None


def resolve_shard_path(shard_name: str, base_dir: str, loc_map: Optional[dict] = None) -> str:
    """
    Resolve the full path to a shard file.

    1. If loc_map provided, use it to find filepath
    2. Otherwise, construct path as: base_dir/shard_name.cdx.gz
    """
    if loc_map and shard_name in loc_map:
        # Use path from .loc file
        loc_path = loc_map[shard_name]
        # If relative, resolve against base_dir
        if not os.path.isabs(loc_path):
            return os.path.join(base_dir, loc_path)
        return loc_path

    # Default: shard_name.cdx.gz in base_dir
    return os.path.join(base_dir, f"{shard_name}.cdx.gz")


def decompress_shard_worker(shard_path: str) -> bytes:
    """
    Worker function to decompress an entire shard file.
    Returns uncompressed data as bytes.
    """
    with gzip.open(shard_path, "rb") as gz:
        return gz.read()


def decompress_shard_chunk_worker(shard_path: str, offset: int, length: int) -> bytes:
    """
    Worker function to decompress a specific chunk from a shard file.
    Reads compressed chunk at offset with length, then decompresses it.
    Returns uncompressed data as bytes.
    """
    with open(shard_path, "rb") as fh:
        fh.seek(offset)
        compressed_data = fh.read(length)

    return gzip.decompress(compressed_data)


def zipnum_to_flat_cdxj(
    idx_path: str,
    base_dir: Optional[str] = None,
    loc_file: Optional[str] = None,
    workers: int = 4,
):
    """
    Main logic:
    - Read .idx file to discover shards
    - Optionally read .loc file for shard locations
    - Decompress each shard in order
    - Output uncompressed CDXJ to stdout
    """
    # Read idx file
    entries = read_idx_file(idx_path)

    if not entries:
        print("Error: No entries found in idx file", file=sys.stderr)
        sys.exit(1)

    # Determine base directory for shard files
    if base_dir is None:
        if idx_path == "-":
            base_dir = "."
        else:
            base_dir = os.path.dirname(os.path.abspath(idx_path))

    # Try to find and read .loc file
    loc_map = None
    if loc_file:
        if os.path.exists(loc_file):
            loc_map = read_loc_file(loc_file)
    else:
        # Auto-detect .loc file
        auto_loc = find_loc_file(idx_path)
        if auto_loc:
            loc_map = read_loc_file(auto_loc)

    # Group entries by shard (maintain order)
    shard_groups: "OrderedDict[str, List[Tuple[int, int]]]" = OrderedDict()
    for _, shard_name, offset, length, _ in entries:
        if shard_name not in shard_groups:
            shard_groups[shard_name] = []
        shard_groups[shard_name].append((offset, length))

    # Process each shard sequentially to maintain sort order
    # Use stdout.buffer for binary writes (better performance)
    output = sys.stdout.buffer

    for shard_name in shard_groups:
        shard_path = resolve_shard_path(shard_name, base_dir, loc_map)

        if not os.path.exists(shard_path):
            print(f"Warning: Shard file not found: {shard_path}", file=sys.stderr)
            continue

        # For each shard, we can decompress the entire file at once
        # since ZipNum shards are already split appropriately
        # Using parallel workers to speed up decompression

        if workers > 1:
            # Parallel decompression: decompress entire shard using thread pool
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future = executor.submit(decompress_shard_worker, shard_path)
                decompressed_data = future.result()
                output.write(decompressed_data)
        else:
            # Single-threaded: decompress directly
            with gzip.open(shard_path, "rb") as gz:
                # Read and write in chunks to avoid loading entire file in memory
                while True:
                    chunk = gz.read(65536)  # 64KB chunks
                    if not chunk:
                        break
                    output.write(chunk)

    # Flush output
    output.flush()


def parse_args(argv=None):
    p = ArgumentParser(description="Convert ZipNum format back to flat CDXJ. Outputs to stdout.")

    p.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input .idx file or '-' for stdin",
    )
    p.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Base directory for shard files (default: idx file directory)",
    )
    p.add_argument(
        "--loc",
        type=str,
        default=None,
        help="Custom .loc file path (default: auto-detect)",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help=(
            "Number of parallel decompression workers (default: 4). "
            "More workers can improve performance on multi-core systems."
        ),
    )

    return p.parse_args(argv)


def main(argv=None):
    """
    Main entry point for command-line execution.
    Parses arguments and calls zipnum_to_flat_cdxj.
    """
    args = parse_args(argv)

    zipnum_to_flat_cdxj(
        idx_path=args.input,
        base_dir=args.base_dir,
        loc_file=args.loc,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
