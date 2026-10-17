#!/usr/bin/env python3
"""
flat_cdxj_to_zipnum.py - Convert Flat CDXJ to ZipNum Compressed Sharded Indexes
================================================================================

Build local ZipNum-style index files and .loc entries from flat CDXJ input
(file or stdin), without Hadoop/MRJob dependencies. ZipNum is a compressed
indexing format used by web archive replay systems like pywb for efficient
binary search over large CDXJ indexes.

COMMAND-LINE USAGE
==================

After installing: pip install -e .
The command 'flat-cdxj-to-zipnum' becomes available globally.

Basic Examples:

    # Convert flat CDXJ file with default settings (100MB shards, 3000 lines/chunk)
    flat-cdxj-to-zipnum -o output_dir -i input.cdxj

    # Read from stdin (useful for pipelines)
    cat merged.cdxj | flat-cdxj-to-zipnum -o output_dir -i -

    # Convert gzipped input
    flat-cdxj-to-zipnum -o output_dir -i input.cdxj.gz

    # Pipeline from merge to ZipNum
    merge-flat-cdxj - /data/*.cdxj | flat-cdxj-to-zipnum -o indexes -i -

Advanced Examples:

    # Custom shard size (200MB) and chunk size (5000 lines)
    flat-cdxj-to-zipnum -o output_dir -i input.cdxj -s 200 -c 5000

    # Custom base name and index/location filenames
    flat-cdxj-to-zipnum -o output_dir -i input.cdxj \\
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
   - Format: <key>\\t<shard>\\t<offset>\\t<length>\\t<lineno>
   - <lineno> is the 1-based line number of the entry in the .idx, counted
     across all shards (not the shard number); pywb relies on it
   - One entry per chunk (default: every 3000 lines)
   - Example: pt,governo,www)/ 20230615120200\\tindex-01.cdx.gz\\t186\\t193\\t2

3. Location File (.loc)
   - Maps shard names to file paths
   - Format: <shard_name.cdx.gz>\t<filepath>
   - Example: index-01.cdx.gz\tindex-01.cdx.gz

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


"""

import concurrent.futures
import contextlib
import gzip
import logging
import os
import sys
import tempfile
from argparse import ArgumentParser
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO, Iterable, List, Optional, Tuple, Union

from replay_cdxj_indexing_tools.cli_utils import positive_int

logger = logging.getLogger(__name__)

# Sentinel shard size (MB) used for --single-shard: large enough that the
# ">= target_shard_size" rotation check never triggers, while remaining a
# plain int so shard_size_mb keeps its declared type.
_SINGLE_SHARD_SENTINEL_MB = 10 * 1024 * 1024  # 10 TB in MB


def _open_temp_shard(output_dir: str) -> Tuple[BinaryIO, str]:
    """Create a temp shard file in output_dir, opened with a large write buffer."""
    fd, temp_path = tempfile.mkstemp(dir=output_dir, suffix=".tmp")
    try:
        return os.fdopen(fd, "wb", buffering=65536), temp_path
    except BaseException:
        # If fdopen() failed after wrapping fd it has already closed it, so an
        # EBADF here must not mask the original error. Close before removing:
        # Windows can't delete a file that is still open.
        with contextlib.suppress(OSError):
            os.close(fd)
        _remove_temp_shard(temp_path)
        raise


def _remove_temp_shard(temp_path: str) -> None:
    """Best-effort removal of a temp shard file; failures are logged, never raised."""
    if temp_path and os.path.exists(temp_path):
        try:
            os.unlink(temp_path)
        except Exception as exc:  # pylint: disable=broad-exception-caught  # best-effort
            logger.warning("Error removing temp shard file %s (non-fatal): %s", temp_path, exc)


def _discard_temp_shard(fh: BinaryIO, temp_path: str) -> None:
    """
    Best-effort close and removal of an unfinished temp shard file.

    Used while an error is already propagating, so it must not mask it: any
    Exception from close() or unlink() is logged as a warning, and the unlink
    runs even if close() raised.
    """
    try:
        fh.close()
    except Exception as exc:  # pylint: disable=broad-exception-caught  # best-effort close
        logger.warning("Error closing shard file during cleanup (non-fatal): %s", exc)
    finally:
        _remove_temp_shard(temp_path)


def open_input_path(path: str) -> Union[BinaryIO, gzip.GzipFile]:
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


def rename_idx_shard(idx_path: str, old_name: str, new_name: str):
    """Rewrite the shard column (2nd) of an idx file from old_name to new_name, streaming."""
    idx_dir = os.path.dirname(os.path.abspath(idx_path))
    tmp_fd, tmp_path = tempfile.mkstemp(dir=idx_dir, suffix=".tmp")
    try:
        with (
            open(idx_path, "r", encoding="utf-8") as src,
            os.fdopen(tmp_fd, "w", encoding="utf-8", buffering=65536) as dst,
        ):
            for line in src:
                parts = line.split("\t")
                if len(parts) >= 2 and parts[1] == old_name:
                    parts[1] = new_name
                dst.write("\t".join(parts))
        os.replace(tmp_path, idx_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def cdxj_to_zipnum(
    output_dir: str,
    input_path: str,
    shard_size_mb: int = 100,
    chunk_size: int = 3000,
    base: Optional[str] = None,
    idx_name: Optional[str] = None,
    loc_name: Optional[str] = None,
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

    # Helper function to generate shard path
    def get_shard_path(shard_num: int, is_single: bool = False) -> str:
        if is_single:  # pylint: disable=no-else-return  # else makes both branches explicit
            # For single shard, use simple naming without numbering
            return os.path.join(output_dir, f"{base}.cdx.gz")
        else:
            return os.path.join(output_dir, f"{base}-{shard_num+1:02d}.cdx.gz")

    # Open first shard (we'll rename if it ends up being single shard)
    shard_path = get_shard_path(current_shard)
    created_shards.append(shard_path)
    current_raw_fh, current_temp_path = _open_temp_shard(output_dir)

    # Iterate chunks in order and write sequentially to shards
    # This maintains the sorted order for binary search

    # Buffer for idx writes to reduce I/O calls
    idx_buffer = []
    idx_buffer_size = 100  # Write idx in batches of 100 entries
    # 5th idx column: 1-based line number across all shards. pywb's ZipNum reader
    # subtracts these to count the blocks to load, so it must not be the shard number.
    idx_lineno = 0

    # Set up parallel compression with thread pool
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Use deque to maintain order of compressed chunks
            compression_queue: deque = deque()
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
                        try:
                            compressed_chunk = future.result(timeout=300)
                        except concurrent.futures.TimeoutError as exc:
                            raise RuntimeError("Compression worker timed out after 300s") from exc

                        # Compressed start offset (bytes) inside the gz file
                        start_offset = current_raw_fh.tell()

                        # Write compressed chunk to file
                        current_raw_fh.write(compressed_chunk)
                        end_offset = current_raw_fh.tell()
                        comp_len = end_offset - start_offset

                        # Extract pre-JSON from first line in chunk for index key
                        pre = extract_prejson(first_line)

                        shard_basename = os.path.basename(created_shards[current_shard])
                        # Keep .cdx.gz extension in shard name for idx file part column

                        # Buffer idx entries to reduce I/O calls
                        idx_lineno += 1
                        idx_buffer.append(
                            f"{pre}\t{shard_basename}\t{start_offset}\t"
                            f"{comp_len}\t{idx_lineno}\n"
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

                            # Close and commit the current shard. A close() failure
                            # (e.g. ENOSPC while flushing) leaves the shard truncated
                            # under idx entries already written, so it is fatal: the
                            # handler below discards the temp shard and re-raises.
                            current_raw_fh.close()
                            os.replace(current_temp_path, created_shards[current_shard])

                            # Move to next shard and open a new temp file for it
                            current_shard += 1
                            shard_path = get_shard_path(current_shard)
                            created_shards.append(shard_path)
                            current_raw_fh, current_temp_path = _open_temp_shard(output_dir)

                # Process remaining compressed chunks in queue
                while compression_queue:
                    future, first_line = compression_queue.popleft()
                    try:
                        compressed_chunk = future.result(timeout=300)
                    except concurrent.futures.TimeoutError as exc:
                        raise RuntimeError("Compression worker timed out after 300s") from exc

                    # Compressed start offset (bytes) inside the gz file
                    start_offset = current_raw_fh.tell()

                    # Write compressed chunk to file
                    current_raw_fh.write(compressed_chunk)
                    end_offset = current_raw_fh.tell()
                    comp_len = end_offset - start_offset

                    # Extract pre-JSON from first line in chunk for index key
                    pre = extract_prejson(first_line)

                    shard_basename = os.path.basename(created_shards[current_shard])
                    # Keep .cdx.gz extension in shard name for idx file part column

                    # Buffer idx entries to reduce I/O calls
                    idx_lineno += 1
                    idx_buffer.append(
                        f"{pre}\t{shard_basename}\t{start_offset}\t{comp_len}\t{idx_lineno}\n"
                    )

                    if len(idx_buffer) >= idx_buffer_size:
                        idxf.write("".join(idx_buffer))
                        idx_buffer.clear()

                # Flush any remaining idx entries
                if idx_buffer:
                    idxf.write("".join(idx_buffer))

        # Close and commit the final shard; close errors (e.g. ENOSPC while
        # flushing) propagate so callers learn about them.
        current_raw_fh.close()
        os.replace(current_temp_path, created_shards[current_shard])
    except BaseException:
        # Never leave an unfinished temp shard behind; re-raise the original error.
        _discard_temp_shard(current_raw_fh, current_temp_path)
        raise

    # If only one shard was created, rename it to use simple naming (no numbering)
    if len(created_shards) == 1 and not created_shards[0].endswith(f"{base}.cdx.gz"):
        simple_name = get_shard_path(0, is_single=True)
        os.replace(created_shards[0], simple_name)
        # The idx was streamed with the numbered name; point it at the renamed shard
        rename_idx_shard(
            idx_name, os.path.basename(created_shards[0]), os.path.basename(simple_name)
        )
        created_shards[0] = simple_name

    # Write loc file
    if loc_name is None:
        loc_name = os.path.join(output_dir, f"{base}.loc")
    else:
        loc_name = os.path.join(output_dir, loc_name)
    with open(loc_name, "w", encoding="utf-8") as locf:
        for path in created_shards:
            basename = os.path.basename(path)
            # Keep .cdx.gz extension in both columns
            # Format: <shard_name.cdx.gz>\t<relative_path>\n
            locf.write(f"{basename}\t{basename}\n")

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
        type=positive_int,
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
            "Create a single shard file regardless of size " "(useful for small inputs or testing)"
        ),
    )
    p.add_argument(
        "-c",
        "--chunk-size",
        type=positive_int,
        default=3000,
        help="Lines per chunk (default: 3000)",
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
        type=positive_int,
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
    # Warnings from best-effort cleanup go to stderr with a level prefix
    logging.basicConfig(format="[%(levelname)s] %(message)s")
    shard_size = _SINGLE_SHARD_SENTINEL_MB if args.single_shard else args.shard_size
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
