"""
Binary search implementation for ZipNum format files.
"""

import gzip
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterator, List, Optional, Tuple


def parse_idx_line(line: str) -> Tuple[str, str, int, int, int]:
    """
    Parse a ZipNum index line.

    Format: <key>\t<shard_name>\t<offset>\t<length>\t<shard_num>

    Args:
        line: Index line

    Returns:
        Tuple of (surt_key, shard_name, offset, length, shard_num)
    """
    parts = line.strip().split("\t")

    if len(parts) >= 5:
        surt_key = parts[0]
        shard_name = parts[1]
        try:
            offset = int(parts[2])
            length = int(parts[3])
            shard_num = int(parts[4])
        except ValueError as exc:
            raise ValueError(f"Invalid numeric field in index line: {line!r}") from exc
        return (surt_key, shard_name, offset, length, shard_num)

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

            # Binary search to find first potential matching block
            left, right = 0, file_size
            found_pos = None
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

                line_str = line.decode("utf-8", errors="ignore").strip()
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

                # Binary search logic
                if match_prefix:
                    if surt_key < search_key:
                        left = current_pos
                    else:
                        right = mid
                        if surt_key.startswith(search_key):
                            found_pos = current_pos
                else:
                    if surt_key < search_key:
                        left = current_pos
                    elif surt_key > search_key:
                        right = mid
                    else:
                        # Exact match
                        found_pos = current_pos
                        right = mid  # Continue searching left for first occurrence

            if verbose:
                print(f"  Binary search completed in {iterations} iterations", file=sys.stderr)

            # Determine starting position
            if found_pos is None:
                # No exact match found in binary search
                # Binary search gives us left boundary - scan back a bit to ensure
                # we don't miss blocks where surt_key < search_key
                check_start = max(0, left - 10000)  # Check up to 10KB back
                fp.seek(check_start)
                if check_start > 0:
                    fp.readline()

                start_pos = fp.tell()
            else:
                # Found a match, scan backwards to find first match
                check_start = max(0, found_pos - 10000)
                fp.seek(check_start)
                if check_start > 0:
                    fp.readline()

                first_match_pos = None
                while fp.tell() < file_size:
                    line_start = fp.tell()
                    line = fp.readline()
                    if not line:
                        break

                    line_str = line.decode("utf-8", errors="ignore").strip()
                    if not line_str:
                        continue

                    try:
                        surt_key, _, _, _, _ = parse_idx_line(line_str)

                        if match_prefix:
                            is_match = surt_key.startswith(search_key) or surt_key < search_key
                        else:
                            is_match = surt_key <= search_key

                        if is_match:
                            if first_match_pos is None:
                                first_match_pos = line_start
                        elif surt_key > search_key:
                            break

                        if line_start > found_pos + 1000:
                            break
                    except ValueError:
                        continue

                start_pos = first_match_pos if first_match_pos is not None else found_pos

            # Collect all matching blocks from start position
            fp.seek(start_pos)

            # If start_pos is in the middle of the file, skip the partial line
            if start_pos > 0:
                fp.readline()

            while True:
                pos = fp.tell()
                if pos >= file_size:
                    break

                line = fp.readline()
                if not line:
                    break

                line_str = line.decode("utf-8", errors="ignore").strip()
                if not line_str:
                    continue

                try:
                    surt_key, shard_name, offset, length, _shard_num = parse_idx_line(line_str)

                    if match_prefix:
                        if surt_key.startswith(search_key):
                            matching_blocks.append((surt_key, shard_name, offset, length))
                        elif surt_key < search_key:
                            matching_blocks.append((surt_key, shard_name, offset, length))
                        else:
                            # surt_key > search_key and doesn't start with prefix
                            # Stop immediately - no more matches possible
                            break
                    else:
                        if surt_key <= search_key:
                            matching_blocks.append((surt_key, shard_name, offset, length))
                        else:
                            # surt_key > search_key - stop immediately
                            break

                except ValueError as e:
                    if verbose:
                        print(f"  Warning: Skipping invalid index line: {e}", file=sys.stderr)
                    continue

    except Exception as e:
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
            compressed_data = fp.read(length)

            if not compressed_data:
                return results

            # Decompress the block
            try:
                decompressed_data = gzip.decompress(compressed_data)
            except Exception as decompress_error:
                if verbose:
                    error_msg = f"Failed to decompress block at offset {offset}: {decompress_error}"
                    print(f"  Warning: {error_msg}", file=sys.stderr)
                return results

            # Parse decompressed lines
            for line in decompressed_data.decode("utf-8", errors="ignore").split("\n"):
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

    except Exception as e:
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
        with open(shard_path, "rb") as fp:
            for surt_key, offset, length in blocks:
                if verbose:
                    shard_name = os.path.basename(shard_path)
                    print(
                        f"  Searching {shard_name} at offset {offset} (SURT: {surt_key[:50]}...)",
                        file=sys.stderr,
                    )

                # Seek to block offset
                fp.seek(offset)

                # Read compressed block
                compressed_data = fp.read(length)
                if not compressed_data:
                    continue

                # Decompress the block
                try:
                    decompressed_data = gzip.decompress(compressed_data)
                except Exception as decompress_error:
                    if verbose:
                        error_msg = (
                            f"Failed to decompress block at offset {offset}: {decompress_error}"
                        )
                        print(f"  Warning: {error_msg}", file=sys.stderr)
                    continue

                # Parse and filter lines
                for line in decompressed_data.decode("utf-8", errors="ignore").split("\n"):
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

    except Exception as e:
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
    """
    if verbose:
        print(f"Searching ZipNum: {idx_filepath}", file=sys.stderr)

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
    resolved_base = os.path.realpath(base_dir)

    for surt_key, shard_name, offset, length in blocks:
        # Resolve shard path using .loc file or base_dir
        if loc_map and shard_name in loc_map:
            shard_path = loc_map[shard_name]
            if not os.path.isabs(shard_path):
                shard_path = os.path.join(base_dir, shard_path)
            resolved = os.path.realpath(shard_path)
            if not (resolved == resolved_base or resolved.startswith(resolved_base + os.sep)):
                raise ValueError(f"Path traversal detected in .loc file: {shard_path!r}")
            shard_path = resolved
        else:
            if not shard_name.endswith(".cdx.gz") and not shard_name.endswith(".cdxj.gz"):
                shard_path = os.path.join(base_dir, f"{shard_name}.cdx.gz")
            else:
                shard_path = os.path.join(base_dir, shard_name)
            resolved = os.path.realpath(shard_path)
            if not (resolved == resolved_base or resolved.startswith(resolved_base + os.sep)):
                raise ValueError(f"Path traversal detected in .idx shard name: {shard_name!r}")
            shard_path = resolved

        if not os.path.exists(shard_path):
            if verbose:
                print(f"  Warning: Shard file not found: {shard_path}", file=sys.stderr)
            continue

        shard_blocks[shard_path].append((surt_key, offset, length))

    # Search shards in parallel if multiple shards, or sequentially if just one
    result_count = 0

    if len(shard_blocks) == 1:
        # Single shard - search sequentially (no threading overhead)
        shard_path, blocks_list = next(iter(shard_blocks.items()))
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
                except Exception as e:
                    shard_path = futures[future]
                    if verbose:
                        print(f"  Error processing {shard_path}: {e}", file=sys.stderr)

    if verbose:
        print(f"  Total matches found: {result_count}", file=sys.stderr)
