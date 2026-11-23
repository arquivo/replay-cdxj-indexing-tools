"""
Binary search implementation for ZipNum format files.
"""

import logging
import os
import sys
import zlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterator, List, Optional, Set, Tuple

from replay_cdxj_indexing_tools import storage
from replay_cdxj_indexing_tools.zipnum.locations import (
    loc_location,
    read_loc_file,
    read_loc_if_present,
    resolve_shard_path,
)

logger = logging.getLogger(__name__)

__all__ = [
    "parse_idx_line",
    "read_loc_file",
    "search_shard_blocks",
    "search_zipnum_data_block",
    "search_zipnum_file",
    "search_zipnum_index",
]

_MAX_BLOCK_READ = 512 * 1024 * 1024  # 512 MB hard cap against crafted .idx files
# Adjacent blocks of a remote shard are fetched together, up to this many bytes; a
# single block of a remote shard larger than this is an error
_MAX_REMOTE_FETCH = 16 * 1024 * 1024
# Decompressed size above which a block is taken as damaged or crafted (a "zip
# bomb"); real ZipNum blocks are far smaller
_MAX_BLOCK_DECOMPRESSED = 64 * 1024 * 1024


class _BlockTooLargeError(ValueError):
    """A ZipNum block decompresses to more than _MAX_BLOCK_DECOMPRESSED bytes."""


def parse_idx_line(line: str) -> Tuple[str, str, int, int, int]:
    """
    Parse a ZipNum index line.

    Format: <key>\t<shard_name>\t<offset>\t<length>\t<lineno>

    <lineno> is the 1-based line number of the entry in the .idx file.

    Args:
        line: Index line

    Returns:
        Tuple of (surt_key, shard_name, offset, length, lineno)
    """
    parts = line.strip().split("\t")

    if len(parts) >= 5:
        surt_key = parts[0]
        shard_name = parts[1]
        try:
            offset = int(parts[2])
            length = int(parts[3])
            lineno = int(parts[4])
        except ValueError as exc:
            raise ValueError(f"Invalid numeric field in index line: {line!r}") from exc
        return (surt_key, shard_name, offset, length, lineno)

    raise ValueError(f"Invalid index line format: {line!r}")


def search_zipnum_index(
    idx_filepath: str, search_key: str, match_prefix: bool = False, verbose: bool = False
) -> List[Tuple[str, str, int, int]]:
    """
    Search ZipNum index file for matching blocks.

    Uses binary search to find approximate starting point, then linear scan.

    Args:
        idx_filepath: Path to .idx file
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of (surt_key, shard_name, offset, length) tuples for matching blocks
    """
    if verbose:
        print(f"Searching ZipNum index: {idx_filepath}", file=sys.stderr)

    matching_blocks = []

    try:
        with storage.open_input(idx_filepath, "rb") as fp:
            # Get file size for binary search
            fp.seek(0, 2)
            file_size = fp.tell()

            if file_size == 0:
                return []

            # Binary search for the last line whose key sorts before search_key.
            # Invariant: left is 0 or the start of a whole parsed line with a key
            # < search_key, so in a sorted .idx every line that can match starts at
            # or after left, whatever the line lengths.
            left, right = 0, file_size
            iterations = 0
            max_iterations = 100  # Safety limit

            while left < right and iterations < max_iterations:
                iterations += 1
                mid = (left + right) // 2

                # Prevent infinite loop when left and right are too close
                if mid in (left, right):
                    break

                # Seek to mid and find start of next line
                fp.seek(mid)
                if mid > 0:
                    fp.readline()  # Skip partial line

                current_pos = fp.tell()
                if current_pos >= file_size:
                    right = mid
                    continue

                # Check if we're stuck (current_pos == left)
                if current_pos == left:
                    break

                line = fp.readline()
                if not line:
                    right = mid
                    continue

                try:
                    line_str = line.decode("utf-8").strip()
                except UnicodeDecodeError:
                    logger.warning(
                        "Invalid UTF-8 in index file at offset %d, skipping line", fp.tell()
                    )
                    right = mid
                    continue
                if not line_str:
                    right = mid
                    continue

                try:
                    surt_key, _, _, _, _ = parse_idx_line(line_str)
                except ValueError:
                    right = mid
                    continue

                if verbose and iterations <= 10:
                    print(
                        f"  Binary search iteration {iterations}: "
                        f"'{surt_key[:50]}...' vs '{search_key[:50]}...'",
                        file=sys.stderr,
                    )

                if surt_key < search_key:
                    left = current_pos
                else:
                    right = mid

            if verbose:
                print(f"  Binary search completed in {iterations} iterations", file=sys.stderr)

            # .idx keys are sparse (the first key of each block), so the first match
            # can sit in the last block whose key sorts before search_key: keep that
            # one block, then every block whose key is in range.
            fp.seek(left)
            while True:
                pos = fp.tell()
                if pos >= file_size:
                    break

                line = fp.readline()
                if not line:
                    break

                try:
                    line_str = line.decode("utf-8").strip()
                except UnicodeDecodeError:
                    logger.warning(
                        "Invalid UTF-8 in index file at offset %d, skipping line", fp.tell()
                    )
                    continue
                if not line_str:
                    continue

                try:
                    surt_key, shard_name, offset, length, _lineno = parse_idx_line(line_str)
                except ValueError as e:
                    if verbose:
                        print(f"  Warning: Skipping invalid index line: {e}", file=sys.stderr)
                    continue

                if match_prefix:
                    in_range = surt_key.startswith(search_key)
                else:
                    in_range = surt_key == search_key

                block = (surt_key, shard_name, offset, length)
                if in_range:
                    matching_blocks.append(block)
                elif surt_key < search_key:
                    matching_blocks = [block]
                else:
                    break

    except Exception as e:  # pylint: disable=broad-exception-caught  # re-raised after logging
        if verbose:
            print(f"  Error reading index file: {e}", file=sys.stderr)
        raise

    if verbose:
        print(f"  Found {len(matching_blocks)} potential blocks", file=sys.stderr)

    return matching_blocks


def search_zipnum_data_block(
    data_filepath: str,
    offset: int,
    length: int,
    search_key: str,
    match_prefix: bool = False,
    verbose: bool = False,
) -> List[str]:
    """
    Search a compressed block in ZipNum data file.

    Args:
        data_filepath: Path to .cdxj.gz or .cdx.gz file
        offset: Offset in file (start of compressed block)
        length: Compressed length (bytes to read)
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    results: List[str] = []

    try:
        with open(data_filepath, "rb") as fp:
            fp.seek(offset)

            # Read only the specified compressed block
            safe_length = min(length, _MAX_BLOCK_READ)
            compressed_data = fp.read(safe_length)

            if not compressed_data:
                return results

            results = _block_matches(compressed_data, offset, search_key, match_prefix, verbose)

    except Exception as e:  # pylint: disable=broad-exception-caught  # re-raised after logging
        if verbose:
            print(f"  Error reading data block at offset {offset}: {e}", file=sys.stderr)
        raise

    return results


def _gunzip_block(data: bytes) -> bytes:
    """
    gzip.decompress() for one ZipNum block (one or more gzip members), stopping
    with _BlockTooLargeError past _MAX_BLOCK_DECOMPRESSED bytes of output.

    Raises:
        _BlockTooLargeError, or OSError, EOFError or zlib.error as gzip.decompress()
    """
    parts: List[bytes] = []
    size = 0
    while data:
        member = zlib.decompressobj(wbits=31)  # gzip header and trailer
        part = member.decompress(data, _MAX_BLOCK_DECOMPRESSED - size + 1)
        size += len(part)
        if size > _MAX_BLOCK_DECOMPRESSED:
            raise _BlockTooLargeError(
                f"block decompresses to more than {_MAX_BLOCK_DECOMPRESSED} bytes"
            )
        if not member.eof:
            raise EOFError("Compressed file ended before the end-of-stream marker was reached")
        parts.append(part)
        data = member.unused_data
    return b"".join(parts)


def _block_matches(
    compressed_data: bytes,
    offset: int,
    search_key: str,
    match_prefix: bool,
    verbose: bool,
    shard_location: Optional[str] = None,
) -> List[str]:
    """
    Decompress one ZipNum block and return its lines that match search_key.

    A block that cannot be decompressed, or that decompresses to more than
    _MAX_BLOCK_DECOMPRESSED bytes, is skipped (reported with verbose, or as a
    warning when too large), unless ``shard_location`` names the remote shard it
    came from: then it is an error, so that a damaged or altered remote shard is
    not mistaken for no matches.
    """
    matches: List[str] = []

    try:
        decompressed_data = _gunzip_block(compressed_data)
    except _BlockTooLargeError as too_large:
        if shard_location is not None:
            raise ValueError(f"{too_large} at offset {offset} of {shard_location}") from too_large
        logger.warning("Skipping block at offset %d: %s", offset, too_large)
        return matches
    except (OSError, EOFError, zlib.error) as decompress_error:
        if shard_location is not None:
            raise ValueError(
                f"Could not decompress the block at offset {offset} of {shard_location}: "
                f"{decompress_error}"
            ) from decompress_error
        if verbose:
            error_msg = f"Failed to decompress block at offset {offset}: {decompress_error}"
            print(f"  Warning: {error_msg}", file=sys.stderr)
        return matches

    try:
        text = decompressed_data.decode("utf-8")
    except UnicodeDecodeError:
        logger.warning(
            "Invalid UTF-8 in compressed block at offset %d, using replace mode",
            offset,
        )
        text = decompressed_data.decode("utf-8", errors="replace")
    for line in text.split("\n"):
        if "\ufffd" in line:  # replacement char indicates corrupt data
            continue
        line_str = line.strip()
        if not line_str:
            continue

        parts = line_str.split(" ", 1)
        if not parts:
            continue

        line_key = parts[0]

        if match_prefix:
            if line_key.startswith(search_key):
                matches.append(line_str)
            elif line_key > search_key and not line_key.startswith(search_key):
                break
        else:
            if line_key == search_key:
                matches.append(line_str)
            elif line_key > search_key:
                break

    return matches


def _coalesce_blocks(blocks: List[Tuple[str, int, int]]) -> List[List[Tuple[str, int, int]]]:
    """
    Group blocks that are back to back in the shard, so each group is one range read
    of at most _MAX_REMOTE_FETCH bytes (or one block, if that block alone is larger).
    """
    groups: List[List[Tuple[str, int, int]]] = []
    group_end = group_size = 0
    for block in sorted(blocks, key=lambda b: b[1]):
        _, offset, length = block
        if groups and offset == group_end and group_size + length <= _MAX_REMOTE_FETCH:
            groups[-1].append(block)
            group_size += length
        else:
            groups.append([block])
            group_size = length
        group_end = offset + length
    return groups


def _search_remote_shard_blocks(
    shard_location: str,
    blocks: List[Tuple[str, int, int]],
    search_key: str,
    match_prefix: bool,
    verbose: bool,
) -> List[str]:
    """
    search_shard_blocks() for a remote shard: one range request per run of adjacent blocks.

    Read errors are raised (as storage.RemoteReadError or FileNotFoundError) rather
    than skipped, and so are blocks that cannot be decompressed or are larger than
    _MAX_REMOTE_FETCH, so that an unavailable remote shard is not mistaken for no
    matches and no single request fetches more than _MAX_REMOTE_FETCH bytes.
    """
    for _surt_key, offset, length in blocks:
        if length > _MAX_REMOTE_FETCH:
            raise ValueError(
                f"The .idx gives a block of {length} bytes at offset {offset} of "
                f"{shard_location}; remote blocks are limited to {_MAX_REMOTE_FETCH} bytes"
            )

    all_results: List[str] = []

    groups = _coalesce_blocks(blocks)
    # The .idx gives every range to read, so the shard's size is not fetched
    read_end = max(offset + length for _, offset, length in blocks)
    with storage.RemoteFile(shard_location, size=read_end) as remote:
        for group in groups:
            group_start = group[0][1]
            group_end = group[-1][1] + group[-1][2]
            if verbose:
                print(
                    f"  Fetching {len(group)} block(s) from {shard_location} "
                    f"(bytes {group_start}-{group_end - 1})",
                    file=sys.stderr,
                )
            data = remote.read_range(group_start, group_end)
            for _surt_key, offset, length in group:
                rel = offset - group_start
                compressed_data = data[rel : rel + length]
                if compressed_data:
                    all_results.extend(
                        _block_matches(
                            compressed_data,
                            offset,
                            search_key,
                            match_prefix,
                            verbose,
                            shard_location=shard_location,
                        )
                    )

    return all_results


def search_shard_blocks(
    shard_path: str,
    blocks: List[Tuple[str, int, int]],
    search_key: str,
    match_prefix: bool,
    verbose: bool,
) -> List[str]:
    """
    Search multiple blocks in a single shard file efficiently.

    Opens the file once and seeks to each block, avoiding repeated file opens.
    Errors reading a local shard are reported (with verbose) and skipped; errors
    reading a remote shard are raised.

    Args:
        shard_path: Path to shard file, or remote location (see storage.is_remote)
        blocks: List of (surt_key, offset, length) tuples for this shard
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    if storage.is_remote(shard_path):
        return _search_remote_shard_blocks(shard_path, blocks, search_key, match_prefix, verbose)

    all_results = []

    try:
        _flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            _flags |= os.O_NOFOLLOW
        with open(os.open(shard_path, _flags), "rb") as fp:
            for surt_key, offset, length in blocks:
                if verbose:
                    shard_name = os.path.basename(shard_path)
                    print(
                        f"  Searching {shard_name} at offset {offset} (SURT: {surt_key[:50]}...)",
                        file=sys.stderr,
                    )

                # Seek to block offset
                fp.seek(offset)

                # Read compressed block (capped to guard against crafted .idx lengths)
                compressed_data = fp.read(min(length, _MAX_BLOCK_READ))
                if not compressed_data:
                    continue

                all_results.extend(
                    _block_matches(compressed_data, offset, search_key, match_prefix, verbose)
                )

    except Exception as e:  # pylint: disable=broad-exception-caught  # non-fatal; caller skips
        if verbose:
            print(f"  Error reading shard {shard_path}: {e}", file=sys.stderr)

    return all_results


def search_zipnum_file(
    idx_filepath: str,
    search_key: str,
    match_prefix: bool = False,
    verbose: bool = False,
    loc_filepath: Optional[str] = None,
    base_dir: Optional[str] = None,
    max_workers: int = 4,
) -> Iterator[str]:
    """
    Search ZipNum files for matching entries (streaming/generator).

    Returns results as an iterator to enable unlimited result sets with constant memory usage.
    Results are yielded as they're found from each shard, allowing efficient processing
    of very large result sets without loading everything into memory.

    The .idx must be sorted by byte value (``LC_ALL=C sort``), as written by
    flat-cdxj-to-zipnum; otherwise matches may be missed.

    Args:
        idx_filepath: Path to .idx file, or remote location (see storage.is_remote)
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr
        loc_filepath: Optional path to .loc file (auto-detected if None)
        base_dir: Optional base directory or URL for shard files (defaults to the
            directory of the idx file)
        max_workers: Maximum number of parallel workers for searching shards (default: 4)

    Yields:
        Matching CDXJ lines one at a time

    Security:
        Path traversal: .loc file entries and .idx shard names are validated with
        path_safety.resolve_location(entry, base_dir) before any file open, so they
        cannot leave base_dir, whether it is a local directory or a URL. Detected
        attempts are logged as warnings and raise ValueError. (CWE-22, CWE-918)
        Trust boundary: base_dir is trusted (caller-supplied); .loc/.idx contents
        are untrusted. See test_zipnum_path_traversal_* for attack scenarios.
    """
    if verbose:
        print(f"Searching ZipNum: {idx_filepath}", file=sys.stderr)

    if not search_key:
        raise ValueError("search_key cannot be empty")
    if len(search_key) > 10_000:
        raise ValueError(f"search_key too long: {len(search_key)} chars (max 10000)")
    if "\x00" in search_key:
        raise ValueError("search_key contains null bytes")

    # Determine base directory
    if base_dir is None:
        base_dir = storage.parent_location(idx_filepath)

    # Try to find and read .loc file. Without one, shard names in the .idx are
    # resolved relative to base_dir.
    loc_map = None
    if loc_filepath:
        loc_map = read_loc_if_present(loc_filepath)
        if loc_map is not None and verbose:
            print(f"  Using .loc file: {loc_filepath}", file=sys.stderr)
    else:
        # Auto-detect .loc file
        auto_loc = loc_location(idx_filepath)
        loc_map = read_loc_if_present(auto_loc)
        if loc_map is not None and verbose:
            print(f"  Found .loc file: {auto_loc}", file=sys.stderr)

    # Search index for matching blocks
    blocks = search_zipnum_index(idx_filepath, search_key, match_prefix, verbose)

    if not blocks:
        if verbose:
            print("  No matching blocks found in index", file=sys.stderr)
        return

    # Group blocks by shard to minimize file opens; a block listed more than once
    # in the .idx is read once
    shard_blocks: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)
    seen: Set[Tuple[str, int, int]] = set()

    for surt_key, shard_name, offset, length in blocks:
        # Resolve shard path using .loc file or base_dir
        try:
            shard_path = resolve_shard_path(shard_name, base_dir, loc_map)
        except ValueError as e:
            logger.warning("Rejected shard location: %s (%s)", e, e.__cause__)
            raise

        # Remote shards are not checked up front: that would cost a request per
        # shard, and a missing one raises when it is read.
        if not storage.is_remote(shard_path) and not os.path.exists(shard_path):
            if verbose:
                print(f"  Warning: Shard file not found: {shard_path}", file=sys.stderr)
            continue

        if (shard_path, offset, length) not in seen:
            seen.add((shard_path, offset, length))
            shard_blocks[shard_path].append((surt_key, offset, length))

    # Search shards in parallel if multiple shards, or sequentially if just one
    result_count = 0

    if len(shard_blocks) == 1:
        # Single shard - search sequentially (no threading overhead)
        for shard_path, blocks_list in shard_blocks.items():
            for result in search_shard_blocks(
                shard_path, blocks_list, search_key, match_prefix, verbose
            ):
                result_count += 1
                yield result
    else:
        # Multiple shards - use parallel search.
        # The executor stays open until this generator is fully consumed or GC'd;
        # for a CLI tool that is always fine, but library callers should consume
        # the iterator to completion to release threads promptly.
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    search_shard_blocks, shard_path, blocks_list, search_key, match_prefix, verbose
                ): shard_path
                for shard_path, blocks_list in shard_blocks.items()
            }

            for future in as_completed(futures):
                try:
                    for result in future.result():
                        result_count += 1
                        yield result
                except Exception as e:  # pylint: disable=broad-exception-caught  # non-fatal shard
                    shard_path = futures[future]
                    if storage.is_remote(shard_path):
                        raise
                    if verbose:
                        print(f"  Error processing {shard_path}: {e}", file=sys.stderr)

    if verbose:
        print(f"  Total matches found: {result_count}", file=sys.stderr)
