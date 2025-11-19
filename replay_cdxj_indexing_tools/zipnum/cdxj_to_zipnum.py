#!/usr/bin/env python3
"""
cdxj_to_zipnum.py - Convert CDXJ to ZipNum Compressed Sharded Indexes
======================================================================

Build local ZipNum-style index files and .loc entries from CDX/CDXJ input
(file or stdin), without Hadoop/MRJob dependencies. ZipNum is a compressed
indexing format used by web archive replay systems like pywb for efficient
binary search over large CDXJ indexes.

COMMAND-LINE USAGE
==================

After installing: pip install -e .
The command 'cdxj-to-zipnum' becomes available globally.

Basic Examples:

    # Convert CDXJ file with default settings (100MB shards, 3000 lines/chunk)
    cdxj-to-zipnum -o output_dir -i input.cdxj

    # Read from stdin (useful for pipelines)
    cat merged.cdxj | cdxj-to-zipnum -o output_dir -i -

    # Convert gzipped input
    cdxj-to-zipnum -o output_dir -i input.cdxj.gz

    # Pipeline from merge to ZipNum
    merge-cdxj /data/*.cdxj - | cdxj-to-zipnum -o indexes -i -

Advanced Examples:

    # Custom shard size (200MB) and chunk size (5000 lines)
    cdxj-to-zipnum -o output_dir -i input.cdxj -s 200 -c 5000

    # Custom base name and index/location filenames
    cdxj-to-zipnum -o output_dir -i input.cdxj \\
        --base arquivo-2023 \\
        --idx-file index.idx \\
        --loc-file locations.loc

    # High compression for storage (level 9)
    cdxj-to-zipnum -o output_dir -i input.cdxj --compress-level 9

    # Fast compression for testing (level 1)
    cdxj-to-zipnum -o output_dir -i input.cdxj --compress-level 1

    # Parallel compression with 8 workers
    cdxj-to-zipnum -o output_dir -i input.cdxj --workers 8

OUTPUT FILES
============

The tool creates three types of files:

1. Shard Files (.cdx.gz)
   - Gzip-compressed CDXJ data blocks
   - Single shard: <base>.cdx.gz
   - Multiple shards: <base>-01.cdx.gz, <base>-02.cdx.gz, ...
   - Default size: 100MB per shard (configurable)

2. Index File (.idx)
   - Tab-separated index for binary search
   - Format: <key>\\t<shard>\\t<offset>\\t<length>\\t<shard_num>
   - One entry per chunk (default: every 3000 lines)
   - Example: pt,governo,www)/ 20230615120200\\tindex-01\\t186\\t193\\t1

3. Location File (.loc)
   - Maps shard names to file paths
   - Format: <shard_name>\\t<filepath>
   - Example: index-01\\t/path/to/index-01.cdx.gz

PARAMETERS
==========

Required:
    -o, --output DIR       Output directory for ZipNum files
    -i, --input FILE       Input CDXJ file or '-' for stdin

Optional:
    -s, --shard-size MB    Target shard size in MB (default: 100)
    -c, --chunk-size N     Lines per index chunk (default: 3000)
    --base NAME           Base name for output files (default: output dir name)
    --idx-file FILE       Custom index filename (default: <base>.idx)
    --loc-file FILE       Custom location filename (default: <base>.loc)
    --compress-level N    Gzip level 0-9 (default: 6, 0=fastest, 9=smallest)
    --workers N           Parallel compression workers (default: 4)

NOTES
=====

- Input can be plain text, gzipped (.gz), or stdin (-)
- Shards are created dynamically as data is processed
- Compressed offsets enable HTTP range requests on shard files
- Maintains sorted order required for binary search
- Compatible with pywb's ZipNum cluster format

PERFORMANCE TIPS
================

- Default 100MB shards work well for most archives
- Chunk size 3000 balances index size vs. search speed
- Use --workers to match CPU cores for faster compression
- Pipe from merge-cdxj to avoid intermediate files
- Higher compression (9) saves space but slower
- Lower compression (1-3) faster but larger files

PYWB INTEGRATION
================

After conversion, configure pywb to use ZipNum indexes:

    # config.yaml
    collections:
      myarchive:
        index_paths: /path/to/output/base.idx

pywb automatically uses the .loc file to locate shard files.

By default pywb reads max 10 chunks/blocks. For more, increase max_blocks
in pywb configuration.

Author: Ivo Branco / GitHub Copilot
"""

from argparse import ArgumentParser
import os
import gzip
import sys
from typing import List, BinaryIO, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor
from collections import deque


def open_input_path(path: str) -> BinaryIO:
    """Open input for binary reading. Supports '-' (stdin) and .gz files."""
    if path == "-":
        return sys.stdin.buffer
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    return open(path, "rb")


def extract_prejson(line_bytes: bytes) -> str:
    """
    Return the CDXJ pre-JSON portion of a line.
    If a '{' exists in the line, returns everything before the first '{'.
    Otherwise returns the entire line (trimmed).
    """
    line = line_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
    idx = line.find("{")
    if idx != -1:
        return line[:idx].strip()
    return line.strip()


def stream_chunks_from_input(input_path: str, chunk_size: int) -> Iterable[Tuple[int, List[bytes]]]:
    """
    Generator that yields (chunk_index, list_of_line_bytes) by reading a single input.
    Supports input_path == '-' for stdin, or a filename (plain or .gz).
    """
    chunk: List[bytes] = []
    chunk_idx = 0
    with open_input_path(input_path) as fh:
        for line in fh:
            chunk.append(line)
            if len(chunk) >= chunk_size:
                yield (chunk_idx, chunk)
                chunk_idx += 1
                chunk = []
    if chunk:
        yield (chunk_idx, chunk)


def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def compress_chunk_worker(chunk_data: bytes, compress_level: int) -> bytes:
    """Worker function to compress a chunk in parallel."""
    return gzip.compress(chunk_data, compresslevel=compress_level)


def cdxj_to_zipnum(
    output_dir: str,
    input_path: str,
    shard_size_mb: int = 100,
    chunk_size: int = 3000,
    base: str = None,
    idx_name: str = None,
    loc_name: str = None,
    compress_level: int = 6,
    workers: int = 4,
):
    """
    Main logic:
    - open shard gzip files for writing
    - stream input lines into logical chunks (chunk_size)
    - assign each chunk to a shard sequentially (fill one shard before moving to next)
    - write the chunk into the shard gzip file, recording compressed offsets and lengths
    - write index (.idx) and location (.loc) files at the end
    """
    ensure_dir(output_dir)
    if base is None:
        base = os.path.basename(os.path.abspath(output_dir)) or "zipnum-output"

    # Write idx file as we go (streaming) to avoid memory buildup
    if idx_name is None:
        idx_name = os.path.join(output_dir, f"{base}.idx")
    else:
        idx_name = os.path.join(output_dir, idx_name)

    chunk_generator = stream_chunks_from_input(input_path, chunk_size)

    # Target size per shard in bytes
    target_shard_size = shard_size_mb * 1024 * 1024

    # Track shard files created dynamically
    created_shards = []

    # Only keep current shard file handle open (memory efficient)
    current_shard = 0
    current_raw_fh = None

    # Helper function to generate shard path
    def get_shard_path(shard_num: int, is_single: bool = False) -> str:
        if is_single:
            # For single shard, use simple naming without numbering
            return os.path.join(output_dir, f"{base}.cdx.gz")
        else:
            return os.path.join(output_dir, f"{base}-{shard_num+1:02d}.cdx.gz")

    # Open first shard (we'll rename if it ends up being single shard)
    # Use larger buffer for better I/O performance
    shard_path = get_shard_path(current_shard)
    created_shards.append(shard_path)
    current_raw_fh = open(shard_path, "wb", buffering=65536)

    # Iterate chunks in order and write sequentially to shards
    # This maintains the sorted order for binary search

    # Buffer for idx writes to reduce I/O calls
    idx_buffer = []
    idx_buffer_size = 100  # Write idx in batches of 100 entries

    # Set up parallel compression with thread pool
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Use deque to maintain order of compressed chunks
        compression_queue = deque()
        max_queue_size = workers * 2  # Keep reasonable memory usage

        with open(idx_name, "w", encoding="utf-8", buffering=65536) as idxf:
            for _, chunk_lines in chunk_generator:
                # Prepare chunk data for compression
                chunk_data = b"".join(chunk_lines)
                first_line = chunk_lines[0]

                # Submit compression task to thread pool
                future = executor.submit(compress_chunk_worker, chunk_data, compress_level)
                compression_queue.append((future, first_line))

                # Process completed compressions to avoid unbounded memory growth
                while len(compression_queue) > max_queue_size or (
                    len(compression_queue) > 0 and compression_queue[0][0].done()
                ):
                    future, first_line = compression_queue.popleft()
                    compressed_chunk = future.result()

                    # Compressed start offset (bytes) inside the gz file
                    start_offset = current_raw_fh.tell()

                    # Write compressed chunk to file
                    current_raw_fh.write(compressed_chunk)
                    end_offset = current_raw_fh.tell()
                    comp_len = end_offset - start_offset

                    # Extract pre-JSON from first line in chunk for index key
                    pre = extract_prejson(first_line)

                    shard_basename = os.path.basename(created_shards[current_shard])
                    # Remove .cdx.gz extension from shard name for idx file
                    shard_name_no_ext = shard_basename.replace(".cdx.gz", "")

                    # Buffer idx entries to reduce I/O calls
                    idx_buffer.append(
                        f"{pre}\t{shard_name_no_ext}\t{start_offset}\t{comp_len}\t{current_shard + 1}\n"
                    )

                    if len(idx_buffer) >= idx_buffer_size:
                        idxf.write("".join(idx_buffer))
                        idx_buffer.clear()

                    # Check if current shard has reached target size
                    # Move to next shard if current one is >= target size
                    if end_offset >= target_shard_size:
                        # Flush idx buffer before closing shard
                        if idx_buffer:
                            idxf.write("".join(idx_buffer))
                            idx_buffer.clear()

                        # Close current shard file
                        try:
                            current_raw_fh.close()
                        except Exception:
                            pass

                        # Move to next shard and open new file with larger buffer
                        current_shard += 1
                        shard_path = get_shard_path(current_shard)
                        created_shards.append(shard_path)
                        current_raw_fh = open(shard_path, "wb", buffering=65536)

            # Process remaining compressed chunks in queue
            while compression_queue:
                future, first_line = compression_queue.popleft()
                compressed_chunk = future.result()

                # Compressed start offset (bytes) inside the gz file
                start_offset = current_raw_fh.tell()

                # Write compressed chunk to file
                current_raw_fh.write(compressed_chunk)
                end_offset = current_raw_fh.tell()
                comp_len = end_offset - start_offset

                # Extract pre-JSON from first line in chunk for index key
                pre = extract_prejson(first_line)

                shard_basename = os.path.basename(created_shards[current_shard])
                # Remove .cdx.gz extension from shard name for idx file
                shard_name_no_ext = shard_basename.replace(".cdx.gz", "")

                # Buffer idx entries to reduce I/O calls
                idx_buffer.append(
                    f"{pre}\t{shard_name_no_ext}\t{start_offset}\t"
                    f"{comp_len}\t{current_shard + 1}\n"
                )

                if len(idx_buffer) >= idx_buffer_size:
                    idxf.write("".join(idx_buffer))
                    idx_buffer.clear()

            # Flush any remaining idx entries
            if idx_buffer:
                idxf.write("".join(idx_buffer))

    # Close final shard file
    try:
        current_raw_fh.close()
    except Exception:
        pass

    # If only one shard was created, rename it to use simple naming (no numbering)
    if len(created_shards) == 1 and not created_shards[0].endswith(f"{base}.cdx.gz"):
        simple_name = get_shard_path(0, is_single=True)
        os.rename(created_shards[0], simple_name)
        created_shards[0] = simple_name

    # Write loc file
    if loc_name is None:
        loc_name = os.path.join(output_dir, f"{base}.loc")
    else:
        loc_name = os.path.join(output_dir, loc_name)
    with open(loc_name, "w", encoding="utf-8") as locf:
        for path in created_shards:
            basename = os.path.basename(path)
            # Remove .cdx.gz extension from first column
            shard_name = basename.replace(".cdx.gz", "")
            # Format: <shard_name>\t<relative_path>\n
            locf.write(f"{shard_name}\t{basename}\n")

    print(
        f"Finished. Wrote {len(created_shards)} shard file(s), index: {idx_name}, loc: {loc_name}"
    )


def parse_args(argv=None):
    p = ArgumentParser(
        description="Build local ZipNum-style index files from a single CDX/CDXJ input (or stdin)."
    )
    # Require explicit -i/--input and -o/--output flags (no positional args)
    p.add_argument(
        "-i", "--input", required=True, help="Input CDX/CDXJ file path, or '-' to read from stdin"
    )
    p.add_argument("-o", "--output", required=True, help="Output directory for shards, idx and loc")
    p.add_argument(
        "-s",
        "--shard-size",
        type=int,
        default=100,
        help=(
            "Target size in MB for each shard file (default: 100MB, same as WARC files). "
            "Ignored if --single-shard is used."
        ),
    )
    p.add_argument(
        "--single-shard",
        action="store_true",
        help=(
            "Create a single shard file regardless of size "
            "(useful for small inputs or testing)"
        ),
    )
    p.add_argument(
        "-c", "--chunk-size", type=int, default=3000, help="Lines per chunk (default: 3000)"
    )
    p.add_argument(
        "--compress-level",
        type=int,
        default=6,
        choices=range(1, 10),
        help=(
            "Gzip compression level 1-9 (default: 6). Lower=faster, higher=smaller. "
            "Level 6 offers best speed/size balance."
        ),
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        help=(
            "Number of parallel compression workers (default: 4). "
            "More workers can improve performance on multi-core systems."
        ),
    )
    p.add_argument(
        "--base",
        type=str,
        default=None,
        help="Base name for output files (default: basename of output dir)",
    )
    p.add_argument(
        "--idx-file",
        type=str,
        default=None,
        help="Custom index filename (written inside output dir)",
    )
    p.add_argument(
        "--loc-file", type=str, default=None, help="Custom loc filename (written inside output dir)"
    )
    return p.parse_args(argv)


def main(argv=None):
    """
    Main entry point for command-line execution.
    Parses arguments and calls cdxj_to_zipnum with appropriate parameters.
    """
    args = parse_args(argv)
    # If single-shard mode, use a very large shard size to ensure everything fits in one shard
    shard_size = float("inf") if args.single_shard else args.shard_size
    cdxj_to_zipnum(
        args.output,
        args.input,
        shard_size_mb=shard_size,
        chunk_size=args.chunk_size,
        base=args.base,
        idx_name=args.idx_file,
        loc_name=args.loc_file,
        compress_level=args.compress_level,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
