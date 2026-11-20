#!/usr/bin/env python3
"""
repair_idx.py - Repair ZipNum .idx Keys From the Shards
=======================================================

flat-cdxj-to-zipnum 1.3.0 and earlier cut each .idx key at the first '{' of the
block's first CDXJ line. A SURT URL key that contains '{' (templated URLs such as
"/?s={search_term_string}" or "/-/{{avatar_url}}") was truncated mid-URL and lost
its timestamp, which leaves the .idx unsorted.

This tool rewrites those keys without rebuilding the shards. A correct key is
"<urlkey> <timestamp>" and has a space, so only keys without one are repaired:
their block (shard, offset, length; one gzip member) is read and the key is
rebuilt from its first line. Every other line is copied unchanged.

COMMAND-LINE USAGE
==================

    # Write the repaired index next to the original
    zipnum-repair-idx -i index.idx -o index.fixed.idx

    # Repair in place, with shards and .loc elsewhere
    zipnum-repair-idx -i index.idx -o index.idx --base-dir /data/shards --loc index.loc

The output is written to a temporary file and renamed over -o at the end, so -o
may be the input itself. A summary is printed to stderr; the exit status is 1 if
the repaired .idx is still not sorted or a block cannot be read.
"""

import gzip
import io
import os
import sys
import tempfile
import zlib
from argparse import ArgumentParser
from typing import BinaryIO, Optional, Tuple

from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import extract_prejson
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import (
    find_loc_file,
    read_loc_file,
    resolve_shard_path,
)


def read_block_first_line(fh: BinaryIO, offset: int, length: int) -> bytes:
    """Return the first line of the gzip member at offset, decompressing only that line."""
    fh.seek(offset)
    data = fh.read(length)
    if len(data) != length:
        raise ValueError(f"block ends past the end of the shard ({len(data)} of {length} bytes)")
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
        return gz.readline()


def repair_idx(
    idx_path: str,
    output_path: str,
    base_dir: Optional[str] = None,
    loc_file: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Rewrite the .idx keys that have no space from their block's first line.

    Returns (repaired, unsorted): the number of keys rewritten, and the number of
    output lines whose key + tab sorts before the previous one.
    """
    if base_dir is None:
        base_dir = os.path.dirname(os.path.abspath(idx_path))
    loc_path = loc_file or find_loc_file(idx_path)
    loc_map = read_loc_file(loc_path) if loc_path else None

    repaired = unsorted = 0
    prev_key = b""
    # The .idx lists blocks in shard order, so only the current shard is kept open;
    # an empty placeholder stands in until the first one is needed
    shard_name: Optional[str] = None
    shard_fh: BinaryIO = io.BytesIO()
    out_dir = os.path.dirname(os.path.abspath(output_path))
    tmp_fd, tmp_path = tempfile.mkstemp(dir=out_dir, suffix=".tmp")
    try:
        with open(idx_path, "rb") as src, os.fdopen(tmp_fd, "wb", buffering=65536) as dst:
            for lineno, line in enumerate(src, 1):
                parts = line.split(b"\t")
                if len(parts) < 4:
                    dst.write(line)
                    continue
                if b" " not in parts[0]:
                    name = parts[1].decode("utf-8")
                    try:
                        if name != shard_name:
                            shard_fh.close()
                            shard_path = resolve_shard_path(name, base_dir, loc_map)
                            # pylint: disable-next=consider-using-with  # closed in finally
                            shard_fh = open(shard_path, "rb")
                            shard_name = name
                        first_line = read_block_first_line(shard_fh, int(parts[2]), int(parts[3]))
                    except (OSError, EOFError, ValueError, zlib.error) as exc:
                        raise ValueError(f"{idx_path}:{lineno}: {name}: {exc}") from exc
                    key = extract_prejson(first_line)
                    if key != parts[0]:
                        parts[0] = key
                        line = b"\t".join(parts)
                        repaired += 1
                sort_key = parts[0] + b"\t"
                if sort_key < prev_key:
                    unsorted += 1
                prev_key = sort_key
                dst.write(line)
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise
    finally:
        shard_fh.close()

    return repaired, unsorted


def parse_args(argv=None):
    p = ArgumentParser(
        description=(
            "Repair ZipNum .idx keys truncated at a '{' in the URL key, "
            "reading each affected block's first line from its shard."
        )
    )
    p.add_argument("-i", "--input", required=True, help="Input .idx file")
    p.add_argument("-o", "--output", required=True, help="Output .idx file (may be the input file)")
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
    return p.parse_args(argv)


def main(argv=None):
    """Main entry point for the zipnum-repair-idx command."""
    args = parse_args(argv)
    try:
        repaired, unsorted = repair_idx(args.input, args.output, args.base_dir, args.loc)
    except (ValueError, OSError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)

    print(f"Repaired {repaired} key(s); wrote {args.output}", file=sys.stderr)
    if unsorted:
        print(
            f"Error: {unsorted} line(s) of {args.output} are still out of order",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
