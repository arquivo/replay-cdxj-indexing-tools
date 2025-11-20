"""
Binary search implementation for ZipNum format files.
"""

import gzip
import sys
from typing import List, Tuple


def parse_idx_line(line: str) -> Tuple[str, int, int, int]:
    """
    Parse a ZipNum index line.

    Format: SURT timestamp offset length compressed_length

    Args:
        line: Index line

    Returns:
        Tuple of (surt, offset, length, compressed_length)
    """
    parts = line.strip().split("\t")
    if len(parts) < 4:
        parts = line.strip().split()

    if len(parts) >= 4:
        surt = parts[0]
        offset = int(parts[1]) if parts[1].isdigit() else 0
        length = int(parts[2]) if parts[2].isdigit() else 0
        comp_length = int(parts[3]) if parts[3].isdigit() else 0
        return (surt, offset, length, comp_length)

    raise ValueError(f"Invalid index line format: {line}")


def search_zipnum_index(
    idx_filepath: str, search_key: str, match_prefix: bool = False, verbose: bool = False
) -> List[Tuple[str, int, int, int]]:
    """
    Search ZipNum index file for matching blocks.

    Args:
        idx_filepath: Path to .idx file
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of (surt, offset, length, compressed_length) tuples for matching blocks
    """
    if verbose:
        print(f"Searching ZipNum index: {idx_filepath}", file=sys.stderr)

    matching_blocks = []

    try:
        with open(idx_filepath, "r", encoding="utf-8") as fp:
            for line in fp:
                if not line.strip():
                    continue

                try:
                    surt, offset, length, comp_length = parse_idx_line(line)

                    if match_prefix:
                        # For prefix search, include block if it might contain matches
                        # Block SURT is typically the first key in that block
                        if search_key <= surt or surt.startswith(search_key):
                            matching_blocks.append((surt, offset, length, comp_length))
                        elif matching_blocks and surt > search_key:
                            # We've passed all potential matching blocks
                            break
                    else:
                        # For exact search, include blocks that might contain the key
                        if surt <= search_key:
                            matching_blocks.append((surt, offset, length, comp_length))
                        elif surt > search_key:
                            # We've passed the potential block
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
    length: int,  # pylint: disable=unused-argument
    search_key: str,
    match_prefix: bool = False,
    verbose: bool = False,
) -> List[str]:
    """
    Search a compressed block in ZipNum data file.

    Args:
        data_filepath: Path to .cdxj.gz file
        offset: Offset in file
        length: Uncompressed length
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    results = []

    try:
        with open(data_filepath, "rb") as fp:
            fp.seek(offset)

            # Read and decompress the block
            # Note: ZipNum blocks are individual gzip members
            with gzip.GzipFile(fileobj=fp) as gzfp:
                for line in gzfp:
                    line_str = line.decode("utf-8", errors="ignore").strip()
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


def search_zipnum_file(
    idx_filepath: str,
    data_filepath: str,
    search_key: str,
    match_prefix: bool = False,
    verbose: bool = False,
) -> List[str]:
    """
    Search ZipNum files for matching entries.

    Args:
        idx_filepath: Path to .idx file
        data_filepath: Path to .cdxj.gz file
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    if verbose:
        print(f"Searching ZipNum: {idx_filepath}", file=sys.stderr)

    # Search index for matching blocks
    blocks = search_zipnum_index(idx_filepath, search_key, match_prefix, verbose)

    if not blocks:
        if verbose:
            print("  No matching blocks found in index", file=sys.stderr)
        return []

    # Search each block in data file
    all_results = []
    for surt, offset, length, _comp_length in blocks:
        if verbose:
            print(f"  Searching block at offset {offset} (SURT: {surt[:50]}...)", file=sys.stderr)

        results = search_zipnum_data_block(
            data_filepath, offset, length, search_key, match_prefix, verbose
        )
        all_results.extend(results)

    if verbose:
        print(f"  Total matches found: {len(all_results)}", file=sys.stderr)

    return all_results
