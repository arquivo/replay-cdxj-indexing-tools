"""
Binary search implementation for flat CDXJ files.
"""

import sys
from typing import List, BinaryIO


def binary_search_file(
    fp: BinaryIO, search_key: str, match_prefix: bool = False, verbose: bool = False
) -> List[str]:
    """
    Perform binary search on a sorted CDXJ file.

    Args:
        fp: File pointer to CDXJ file (opened in binary mode)
        search_key: SURT key to search for
        match_prefix: If True, return all lines starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    # Get file size
    fp.seek(0, 2)  # Seek to end
    file_size = fp.tell()

    if file_size == 0:
        return []

    if verbose:
        print(f"  File size: {file_size} bytes", file=sys.stderr)

    # Binary search for first occurrence
    left, right = 0, file_size
    found_pos = None

    iterations = 0
    while left < right:
        iterations += 1
        mid = (left + right) // 2

        # Seek to mid position
        fp.seek(mid)

        # Skip to next newline (unless at start of file)
        if mid > 0:
            fp.readline()

        current_pos = fp.tell()

        # Check if we've gone past the end
        if current_pos >= file_size:
            right = mid
            continue

        # Read the line
        line = fp.readline()
        if not line:
            right = mid
            continue

        # Decode and extract SURT key
        line_str = line.decode("utf-8", errors="ignore").strip()
        if not line_str:
            right = mid
            continue

        parts = line_str.split(" ", 1)
        if not parts:
            right = mid
            continue

        line_key = parts[0]

        if verbose and iterations <= 10:
            print(
                f"  Binary search iteration {iterations}: "
                f"comparing '{line_key[:50]}...' with '{search_key[:50]}...'",
                file=sys.stderr,
            )

        # Compare keys
        if match_prefix:
            # For prefix search, we want to find the first occurrence
            if line_key < search_key:
                left = current_pos
            else:
                right = mid
                if line_key.startswith(search_key):
                    found_pos = current_pos
        else:
            # For exact search
            if line_key < search_key:
                left = current_pos
            elif line_key > search_key:
                right = mid
            else:
                # Exact match found
                found_pos = current_pos
                # Continue searching left for first occurrence
                right = mid

    if verbose:
        print(f"  Binary search completed in {iterations} iterations", file=sys.stderr)

    # If no match found, return empty
    if found_pos is None and not match_prefix:
        return []

    # Collect all matching lines
    results = []

    # Determine starting position for collecting results
    if found_pos is None:
        # No match found in binary search
        if match_prefix:
            # For prefix search, start from left boundary
            start_pos = left
        else:
            # For exact search, no results
            return []
    else:
        # We found a match, now we need to find the first occurrence
        # For both exact and prefix matches, scan backwards to find the first match
        check_start = max(0, found_pos - 10000)  # Check up to 10KB back
        fp.seek(check_start)

        if check_start > 0:
            fp.readline()  # Skip partial line at start

        first_match_pos = None

        # Scan forward from check_start to find first matching line
        while fp.tell() < file_size:
            line_start = fp.tell()
            line = fp.readline()
            if not line:
                break

            line_str = line.decode("utf-8", errors="ignore").strip()
            if not line_str:
                continue

            parts = line_str.split(" ", 1)
            if not parts:
                continue

            line_key = parts[0]

            # Check if this line matches
            is_match = False
            if match_prefix:
                is_match = line_key.startswith(search_key)
            else:
                is_match = line_key == search_key

            if is_match:
                if first_match_pos is None:
                    first_match_pos = line_start
                # Don't break - continue to see if we need to go back further
            elif line_key > search_key or (match_prefix and not line_key.startswith(search_key)):
                # We've passed all possible matches
                break

            # Stop scanning if we've gone past our initial found position
            if line_start > found_pos + 1000:
                break

        start_pos = first_match_pos if first_match_pos is not None else found_pos

    # Now read all matching lines from start_pos
    fp.seek(start_pos)

    # Read lines starting from start_pos
    # Note: start_pos points to the beginning of a line, so we don't skip
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

        parts = line_str.split(" ", 1)
        if not parts:
            continue

        line_key = parts[0]

        if match_prefix:
            if line_key.startswith(search_key):
                results.append(line_str)
            elif not line_key.startswith(search_key):
                # We've passed all matching lines
                break
        else:
            if line_key == search_key:
                results.append(line_str)
            elif line_key != search_key:
                # We've passed all matching lines
                break

    if verbose:
        print(f"  Found {len(results)} matching lines", file=sys.stderr)

    return results


def search_cdxj_file(
    filepath: str, search_key: str, match_prefix: bool = False, verbose: bool = False
) -> List[str]:
    """
    Search a flat CDXJ file for matching entries.

    Args:
        filepath: Path to CDXJ file
        search_key: SURT key to search for
        match_prefix: If True, match all entries starting with search_key
        verbose: If True, print debug info to stderr

    Returns:
        List of matching CDXJ lines
    """
    if verbose:
        print(f"Searching file: {filepath}", file=sys.stderr)

    try:
        with open(filepath, "rb") as fp:
            return binary_search_file(fp, search_key, match_prefix, verbose)
    except Exception as e:
        if verbose:
            print(f"  Error reading file: {e}", file=sys.stderr)
        raise
