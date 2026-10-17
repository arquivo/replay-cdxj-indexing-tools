"""
Binary search implementation for ZipNum format files.
"""

import gzip
import logging
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterator, List, Optional, Tuple

from replay_cdxj_indexing_tools.path_safety import resolve_within

logger = logging.getLogger(__name__)

_MAX_BLOCK_READ = 512 * 1024 * 1024  # 512 MB hard cap against crafted .idx files


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


def read_loc_file(loc_filepath: str) -> Dict[str, str]:
    """
    Read .loc file and return mapping of shard_name -> filepath.

    Args:
        loc_filepath: Path to .loc file

    Returns:
        Dictionary mapping shard names to file paths
    """
    loc_map = {}

    with open(loc_filepath, "r", encoding="utf-8") as fh:
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
        with open(idx_filepath, "rb") as fp:
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

            # Decompress the block
            try:
                decompressed_data = gzip.decompress(compressed_data)
            except Exception as decompress_error:  # pylint: disable=broad-exception-caught
                if verbose:
                    error_msg = f"Failed to decompress block at offset {offset}: {decompress_error}"
                    print(f"  Warning: {error_msg}", file=sys.stderr)
                return results

            # Parse decompressed lines
            try:
                text = decompressed_data.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning(
                    "Invalid UTF-8 in compressed block at offset %d, using replace mode", offset
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
                        results.append(line_str)
                    elif line_key > search_key and not line_key.startswith(search_key):
                        # Passed all matches
                        break
                else:
                    if line_key == search_key:
                        results.append(line_str)
                    elif line_key > search_key:
                        # Passed all matches
                        break

    except Exception as e:  # pylint: disable=broad-exception-caught  # re-raised after logging
        if verbose:
            print(f"  Error reading data block at offset {offset}: {e}", file=sys.stderr)
        raise

    return results


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

    Args:
        shard_path: Path to shard file
        blocks: List of (surt_key, offset, length) tuples for this shard
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
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

                # Decompress the block
                try:
                    decompressed_data = gzip.decompress(compressed_data)
                except Exception as decompress_error:  # pylint: disable=broad-exception-caught
                    if verbose:
                        error_msg = (
                            f"Failed to decompress block at offset {offset}: {decompress_error}"
                        )
                        print(f"  Warning: {error_msg}", file=sys.stderr)
                    continue

                # Parse and filter lines
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
                            all_results.append(line_str)
                        elif line_key > search_key and not line_key.startswith(search_key):
                            break
                    else:
                        if line_key == search_key:
                            all_results.append(line_str)
                        elif line_key > search_key:
                            break

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
        idx_filepath: Path to .idx file
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr
        loc_filepath: Optional path to .loc file (auto-detected if None)
        base_dir: Optional base directory for shard files (defaults to idx file directory)
        max_workers: Maximum number of parallel workers for searching shards (default: 4)

    Yields:
        Matching CDXJ lines one at a time

    Security:
        Path traversal: .loc file entries and .idx shard names are validated with
        path_safety.resolve_within(path, base_dir) before any file open. Detected
        attempts are logged as warnings and raise ValueError. (CWE-22)
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
        base_dir = os.path.dirname(os.path.abspath(idx_filepath))

    # Try to find and read .loc file
    loc_map = None
    if loc_filepath:
        if os.path.exists(loc_filepath):
            loc_map = read_loc_file(loc_filepath)
            if verbose:
                print(f"  Using .loc file: {loc_filepath}", file=sys.stderr)
    else:
        # Auto-detect .loc file
        base, _ = os.path.splitext(idx_filepath)
        auto_loc = base + ".loc"
        if os.path.exists(auto_loc):
            loc_map = read_loc_file(auto_loc)
            if verbose:
                print(f"  Found .loc file: {auto_loc}", file=sys.stderr)

    # Search index for matching blocks
    blocks = search_zipnum_index(idx_filepath, search_key, match_prefix, verbose)

    if not blocks:
        if verbose:
            print("  No matching blocks found in index", file=sys.stderr)
        return

    # Group blocks by shard to minimize file opens
    shard_blocks: Dict[str, List[Tuple[str, int, int]]] = defaultdict(list)

    for surt_key, shard_name, offset, length in blocks:
        # Resolve shard path using .loc file or base_dir
        if loc_map and shard_name in loc_map:
            shard_path = loc_map[shard_name]
            if not os.path.isabs(shard_path):
                shard_path = os.path.join(base_dir, shard_path)
            try:
                shard_path = resolve_within(shard_path, base_dir)
            except ValueError as e:
                logger.warning("Path traversal attempt via .loc file: %s", e)
                raise ValueError(f"Path traversal detected in .loc file: {shard_path!r}") from e
        else:
            if not shard_name.endswith(".cdx.gz") and not shard_name.endswith(".cdxj.gz"):
                shard_path = os.path.join(base_dir, f"{shard_name}.cdx.gz")
            else:
                shard_path = os.path.join(base_dir, shard_name)
            try:
                shard_path = resolve_within(shard_path, base_dir)
            except ValueError as e:
                logger.warning("Path traversal attempt via .idx shard name: %s", e)
                raise ValueError(
                    f"Path traversal detected in .idx shard name: {shard_name!r}"
                ) from e

        if not os.path.exists(shard_path):
            if verbose:
                print(f"  Warning: Shard file not found: {shard_path}", file=sys.stderr)
            continue

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
                    if verbose:
                        print(f"  Error processing {shard_path}: {e}", file=sys.stderr)

    if verbose:
        print(f"  Total matches found: {result_count}", file=sys.stderr)
