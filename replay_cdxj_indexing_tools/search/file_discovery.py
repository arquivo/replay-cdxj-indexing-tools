"""
File discovery utilities for finding CDXJ and ZipNum files.
"""

import os
import glob
import sys
from typing import List


def discover_files(patterns: List[str], verbose: bool = False) -> List[str]:
    """
    Discover files from patterns (paths, globs, or directories).

    Args:
        patterns: List of file patterns, paths, or directories
        verbose: If True, print discovery info to stderr

    Returns:
        List of absolute file paths
    """
    files = set()  # Use set to avoid duplicates

    for pattern in patterns:
        # Check if it's a directory
        if os.path.isdir(pattern):
            if verbose:
                print(f"Scanning directory: {pattern}", file=sys.stderr)
            for root, _, filenames in os.walk(pattern):
                for filename in filenames:
                    if (
                        filename.endswith(".cdxj")
                        or filename.endswith(".idx")
                        or filename.endswith(".cdxj.gz")
                    ):
                        files.add(os.path.abspath(os.path.join(root, filename)))

        # Check if it's an exact file
        elif os.path.isfile(pattern):
            files.add(os.path.abspath(pattern))

        # Try as glob pattern
        else:
            matched = glob.glob(pattern, recursive=True)
            if matched:
                for match in matched:
                    if os.path.isfile(match):
                        files.add(os.path.abspath(match))
            elif verbose:
                print(f"Warning: Pattern '{pattern}' matched no files", file=sys.stderr)

    sorted_files = sorted(files)

    if verbose:
        print(f"Found {len(sorted_files)} files", file=sys.stderr)

    return sorted_files


def detect_file_type(filepath: str) -> str:
    """
    Detect if file is flat CDXJ, ZipNum index, or ZipNum data.

    Args:
        filepath: Path to the file

    Returns:
        'cdxj', 'zipnum_idx', or 'zipnum_data'
    """
    if filepath.endswith(".idx"):
        return "zipnum_idx"
    elif filepath.endswith(".cdxj.gz"):
        return "zipnum_data"
    elif filepath.endswith(".cdxj"):
        return "cdxj"
    else:
        # Try to detect by content
        return "cdxj"  # Default assumption


def find_zipnum_data_file(idx_file: str) -> str:
    """
    Find the corresponding .cdxj.gz file for a .idx file.

    Args:
        idx_file: Path to .idx file

    Returns:
        Path to corresponding .cdxj.gz file

    Raises:
        FileNotFoundError: If data file not found
    """
    # Standard naming: index.idx -> index.cdxj.gz
    if idx_file.endswith(".idx"):
        data_file = idx_file[:-4] + ".cdxj.gz"
        if os.path.exists(data_file):
            return data_file

    raise FileNotFoundError(f"Could not find data file for index: {idx_file}")


def find_zipnum_index_file(data_file: str) -> str:
    """
    Find the corresponding .idx file for a .cdxj.gz file.

    Args:
        data_file: Path to .cdxj.gz file

    Returns:
        Path to corresponding .idx file

    Raises:
        FileNotFoundError: If index file not found
    """
    # Standard naming: index.cdxj.gz -> index.idx
    if data_file.endswith(".cdxj.gz"):
        idx_file = data_file[:-8] + ".idx"  # Remove 'cdxj.gz' (8 chars), add '.idx'
        if os.path.exists(idx_file):
            return idx_file

    raise FileNotFoundError(f"Could not find index file for data: {data_file}")
