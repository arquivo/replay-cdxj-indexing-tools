#!/usr/bin/env python3
"""
Merge Flat CDXJ Files - Efficient k-way merge for large sorted flat CDXJ files

This script merges multiple sorted flat CDXJ files into a single sorted output file using
a min-heap based k-way merge algorithm. It's optimized for handling large files
with minimal memory usage by reading files line-by-line rather than loading them
entirely into memory.

Usage Examples:
    # Merge two sorted files to output file
    merge-flat-cdxj output.cdxj file1.cdxj file2.cdxj

    # Merge multiple sorted files
    merge-flat-cdxj merged.cdxj sorted1.cdxj sorted2.cdxj sorted3.cdxj

    # Merge all files from directories (recursively)
    merge-flat-cdxj output.cdxj /path/to/dir1 /path/to/dir2

    # Mix files and directories
    merge-flat-cdxj output.cdxj file1.cdxj /path/to/directory file2.cdxj

    # Output to stdout (use '-' as output filename)
    merge-flat-cdxj - file1.cdxj file2.cdxj

    # Pipe to other processes
    merge-flat-cdxj - *.cdxj | gzip > merged.cdxj.gz
    merge-flat-cdxj - dir1/ dir2/ | grep "pattern" > filtered.cdxj
    merge-flat-cdxj - sorted*.cdxj | wc -l
    merge-flat-cdxj - /data/indexes_cdx/*.cdxj | head -n 1000 > top1000.cdxj

    # Redirect stderr to a file, pipe stdout to another command
    python merge_sorted_files.py - file1.cdxj file2.cdxj 2> errors.log | gzip > merged.gz

Requirements:
    - All input files must be sorted in lexicographic order
    - Input files should use the same encoding (default: system encoding)
    - Sufficient disk space for the output file

Performance:
    - Time Complexity: O(N log k) where N is total lines, k is number of files
    - Space Complexity: O(k) for the heap
    - Memory efficient: only k lines held in memory at once

Author: Ivo Branco / Copilot
"""

import argparse
import fnmatch
import heapq
import os
import sys


def should_exclude(filename, exclude_patterns):
    """
    Check if a filename matches any exclusion pattern.

    Args:
        filename: Name of the file to check (basename only)
        exclude_patterns: List of glob-style patterns to match against

    Returns:
        tuple: (should_exclude: bool, matched_pattern: str or None)
               Returns (True, pattern) if file matches any pattern, (False, None) otherwise
    """
    if not exclude_patterns:
        return False, None

    basename = os.path.basename(filename)
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(basename, pattern):
            return True, pattern
    return False, None


def log_progress(message, verbose=False):
    """
    Log progress message to stderr if verbose is enabled.

    Args:
        message: Message to log
        verbose: Whether to output the message
    """
    if verbose:
        print(message, file=sys.stderr)


def get_all_files(paths, exclude_patterns=None, verbose=False):
    """
    Recursively collect all files from the given paths, with optional exclusion.

    Args:
        paths: List of file paths or directory paths
        exclude_patterns: List of glob-style patterns for files to exclude (optional)
        verbose: Whether to log progress to stderr (optional)

    Yields:
        str: Absolute path to each file found (excluding those matching patterns)

    Note:
        If a path is a directory, recursively walks through all subdirectories
        to find files. If a path is a file, yields it directly unless excluded.
        Progress information is written to stderr when verbose=True.
    """
    total_found = 0
    total_excluded = 0
    total_included = 0

    for path in paths:
        if os.path.isfile(path):
            total_found += 1
            excluded, pattern = should_exclude(path, exclude_patterns)
            if excluded:
                total_excluded += 1
                log_progress(f"[EXCLUDE] {os.path.basename(path)} (matches: {pattern})", verbose)
            else:
                total_included += 1
                log_progress(f"[INCLUDE] {os.path.basename(path)}", verbose)
                yield path
        elif os.path.isdir(path):
            log_progress(f"[DISCOVER] Scanning directory: {path}", verbose)
            dir_found = 0
            dir_excluded = 0
            dir_included = 0

            for root, _, files in os.walk(path):
                for f in files:
                    full_path = os.path.join(root, f)
                    dir_found += 1
                    total_found += 1

                    excluded, pattern = should_exclude(full_path, exclude_patterns)
                    if excluded:
                        dir_excluded += 1
                        total_excluded += 1
                        log_progress(f"[EXCLUDE] {f} (matches: {pattern})", verbose)
                    else:
                        dir_included += 1
                        total_included += 1
                        log_progress(f"[INCLUDE] {f}", verbose)
                        yield full_path

            log_progress(
                f"[DISCOVER] Directory {path}: {dir_found} found, "
                f"{dir_excluded} excluded, {dir_included} included",
                verbose,
            )

    if verbose and total_found > 0:
        log_progress(
            f"[SUMMARY] Total: {total_found} found, {total_excluded} excluded, "
            f"{total_included} included",
            verbose,
        )


def merge_sorted_files(files, output_file, buffer_size=1024 * 1024, verbose=False):
    """
    Merge multiple sorted files into a single sorted output file using a min-heap.

    This function implements an efficient k-way merge algorithm that can handle
    large files with minimal memory usage. It uses a heap to always select the
    lexicographically smallest line from all input files.

    Args:
        files: List of paths to sorted input files
        output_file: Path where the merged output will be written, or '-' for stdout
        buffer_size: Buffer size in bytes for file I/O operations (default: 1MB)
        verbose: Whether to log progress to stderr (optional)

    Algorithm:
        1. Opens all input files simultaneously
        2. Initializes a min-heap with the first line from each file
        3. Repeatedly extracts the smallest line and writes it to output
        4. Refills the heap with the next line from the same file
        5. Continues until all files are exhausted

    Time Complexity: O(N log k) where N is total lines and k is number of files
    Space Complexity: O(k) for the heap
    """
    log_progress(f"[MERGE] Starting merge of {len(files)} files...", verbose)
    lines_written = 0
    # Open all input files with buffering for efficient I/O
    file_handles = [open(f, "r", buffering=buffer_size) for f in files]

    # Initialize min-heap with the first line from each file
    # Heap elements are tuples: (line_content, file_index)
    heap = []
    for idx, fh in enumerate(file_handles):
        line = fh.readline()
        if line:
            # Push (line, file_index) tuple onto heap
            # Python's heapq compares tuples lexicographically, so lines are naturally sorted
            heapq.heappush(heap, (line, idx))

    # Open output file with buffering for efficient writing
    # Use stdout if output_file is '-', otherwise open a file
    if output_file == "-":
        out = sys.stdout
        # Process all lines in sorted order
        while heap:
            # Extract the lexicographically smallest line
            value, idx = heapq.heappop(heap)
            out.write(value)
            lines_written += 1

            # Read the next line from the same file that just provided a line
            next_line = file_handles[idx].readline()
            if next_line:
                # Add the new line back to the heap if file has more content
                heapq.heappush(heap, (next_line, idx))
    else:
        with open(output_file, "w", buffering=buffer_size) as out:
            # Process all lines in sorted order
            while heap:
                # Extract the lexicographically smallest line
                value, idx = heapq.heappop(heap)
                out.write(value)
                lines_written += 1

                # Read the next line from the same file that just provided a line
                next_line = file_handles[idx].readline()
                if next_line:
                    # Add the new line back to the heap if file has more content
                    heapq.heappush(heap, (next_line, idx))

    # Clean up: close all input file handles
    for fh in file_handles:
        fh.close()

    log_progress(f"[MERGE] Complete: {lines_written} lines written", verbose)


def main():
    """Main entry point for command-line usage."""
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description=(
            "Merge N sorted flat CDXJ files or directories " "into one (optimized for large files)."
        ),
        epilog="Examples:\n"
        "  merge-flat-cdxj output.cdxj file1.cdxj file2.cdxj\n"
        "  merge-flat-cdxj output.cdxj /data/indexes/ --exclude '*-open.cdxj' -v\n"
        "  merge-flat-cdxj - *.cdxj --exclude '*-tmp.cdxj' | gzip > merged.cdxj.gz",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("output", help="Output file name (use '-' for stdout)")
    parser.add_argument("paths", nargs="+", help="List of sorted input files or directories")
    parser.add_argument(
        "--exclude",
        action="append",
        dest="exclude_patterns",
        metavar="PATTERN",
        help="Exclude files matching glob pattern (can be used multiple times). "
        "Example: --exclude '*-open.cdxj' --exclude '*-tmp.cdxj'",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output to stderr (progress, exclusions, statistics)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress all stderr output (overrides --verbose)",
    )
    args = parser.parse_args()

    # Determine verbosity (quiet overrides verbose)
    verbose = args.verbose and not args.quiet

    # Collect all files from the provided paths (expands directories)
    files = list(get_all_files(args.paths, args.exclude_patterns, verbose))

    if not files:
        log_progress("[ERROR] No files to merge after applying exclusions", verbose=True)
        sys.exit(1)

    # Perform the k-way merge operation
    merge_sorted_files(files, args.output, verbose=verbose)


if __name__ == "__main__":
    main()
