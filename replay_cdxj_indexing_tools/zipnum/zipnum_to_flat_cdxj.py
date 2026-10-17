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

    # Pipeline with filtering
    zipnum-to-flat-cdxj -i base.idx | filter-blocklist -b blocklist.txt > filtered.cdxj

    # Custom base directory for shard files
    zipnum-to-flat-cdxj -i /idx/base.idx --base-dir /data/shards

ZIPNUM FORMAT
=============

ZipNum format consists of:

1. Index File (.idx)
   - Tab-separated: <key>\\t<shard>\\t<offset>\\t<length>\\t<lineno>
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
    --workers N           Deprecated and ignored (decompression is streamed
                          sequentially); prints a warning if given

OUTPUT
======

Outputs uncompressed CDXJ records to stdout in the order they appear in shards.
Records maintain their original sorted order from the ZipNum format.

NOTES
=====

- All output goes to stdout (no files created)
- Shards are processed sequentially to maintain sort order
- Each shard is streamed through gzip in 256 KiB chunks, so memory use does
  not grow with shard size
- --workers is deprecated and has no effect
- A corrupt or truncated shard raises an error and exits non-zero, but data
  already decompressed from it has been written; use `set -o pipefail` or
  check the exit status in pipelines
- Use shell redirection to save output to file
- Compatible with all ZipNum files created by cdxj-to-zipnum

PERFORMANCE TIPS
================

- Pipe directly to other tools to avoid intermediate files
- For large indexes, redirect to compressed output: | gzip > output.cdxj.gz

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
import shutil
import sys
import warnings
from argparse import ArgumentParser
from typing import List, Optional, Tuple

from replay_cdxj_indexing_tools.path_safety import resolve_within

# Read/write size used when streaming a shard to stdout
_COPY_BUFSIZE = 256 * 1024


def read_idx_file(idx_path: str) -> List[Tuple[str, str, int, int, int]]:
    """
    Read .idx file and return list of (key, shard_name, offset, length, lineno).
    Supports idx_path == '-' for stdin.

    Returns entries grouped by shard in order they appear.
    """
    entries = []

    if idx_path == "-":
        fh = sys.stdin
    else:  # pylint: disable-next=consider-using-with  # stdin/file conditional; finally closes
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
            lineno = int(parts[4])

            entries.append((key, shard_name, offset, length, lineno))
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

    Path traversal defense: all resolved paths must remain within base_dir.
    """
    if loc_map and shard_name in loc_map:
        # Use path from .loc file
        loc_path = loc_map[shard_name]
        # If relative, resolve against base_dir
        if not os.path.isabs(loc_path):
            loc_path = os.path.join(base_dir, loc_path)
        try:
            return resolve_within(loc_path, base_dir)
        except ValueError as e:
            raise ValueError(f"Path traversal detected in .loc file: {loc_path!r}") from e

    # Default: shard_name.cdx.gz in base_dir
    shard_path = os.path.join(base_dir, f"{shard_name}.cdx.gz")
    try:
        return resolve_within(shard_path, base_dir)
    except ValueError as e:
        raise ValueError(f"Path traversal detected in .idx shard name: {shard_name!r}") from e


def zipnum_to_flat_cdxj(
    idx_path: str,
    base_dir: Optional[str] = None,
    loc_file: Optional[str] = None,
    workers: Optional[int] = None,
):
    """
    Main logic:
    - Read .idx file to discover shards
    - Optionally read .loc file for shard locations
    - Stream-decompress each shard in order
    - Output uncompressed CDXJ to stdout

    ``workers`` is deprecated and ignored: decompression is streamed
    sequentially, so memory use does not grow with shard size. Passing a
    value other than None emits a DeprecationWarning; it will be removed in a
    future major release.
    """
    if workers is not None:
        warnings.warn(
            "workers is ignored and will be removed in a future major release",
            DeprecationWarning,
            stacklevel=2,
        )

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

    # Unique shard names, in order of first appearance
    shard_names = dict.fromkeys(entry[1] for entry in entries)

    # Process each shard sequentially to maintain sort order
    # Use stdout.buffer for binary writes (better performance)
    output = sys.stdout.buffer

    for shard_name in shard_names:
        shard_path = resolve_shard_path(shard_name, base_dir, loc_map)

        if not os.path.exists(shard_path):
            print(f"Warning: Shard file not found: {shard_path}", file=sys.stderr)
            continue

        # Stream in fixed-size chunks so memory use does not grow with shard size
        with gzip.open(shard_path, "rb") as gz:
            shutil.copyfileobj(gz, output, _COPY_BUFSIZE)

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
        default=None,
        help="Deprecated and ignored (decompression is streamed sequentially)",
    )

    return p.parse_args(argv)


def main(argv=None):
    """
    Main entry point for command-line execution.
    Parses arguments and calls zipnum_to_flat_cdxj.
    """
    args = parse_args(argv)

    if args.workers is not None:
        print(
            "Warning: --workers is deprecated and ignored (decompression is streamed)",
            file=sys.stderr,
        )

    zipnum_to_flat_cdxj(
        idx_path=args.input,
        base_dir=args.base_dir,
        loc_file=args.loc,
    )


if __name__ == "__main__":
    main()
