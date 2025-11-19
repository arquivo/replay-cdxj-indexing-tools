#!/usr/bin/env python3
"""
Merge Sorted Files - Efficient k-way merge for large sorted files

This script merges multiple sorted files into a single sorted output file using
a min-heap based k-way merge algorithm. It's optimized for handling large files
with minimal memory usage by reading files line-by-line rather than loading them
entirely into memory.

Usage Examples:
    # Merge two sorted files to output file
    python merge_sorted_files.py output.txt file1.txt file2.txt

    # Merge multiple sorted files
    python merge_sorted_files.py merged.cdxj sorted1.cdxj sorted2.cdxj sorted3.cdxj

    # Merge all files from directories (recursively)
    python merge_sorted_files.py output.txt /path/to/dir1 /path/to/dir2

    # Mix files and directories
    python merge_sorted_files.py output.txt file1.txt /path/to/directory file2.txt

    # Output to stdout (use '-' as output filename)
    python merge_sorted_files.py - file1.txt file2.txt

    # Pipe to other processes
    python merge_sorted_files.py - *.cdxj | gzip > merged.cdxj.gz
    python merge_sorted_files.py - dir1/ dir2/ | grep "pattern" > filtered.txt
    python merge_sorted_files.py - sorted*.cdxj | wc -l
    python merge_sorted_files.py - /data/indexes_cdx/*.cdxj | head -n 1000 > top1000.txt

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

import heapq
import argparse
import os
import sys

def get_all_files(paths):
    """
    Recursively collect all files from the given paths.

    Args:
        paths: List of file paths or directory paths

    Yields:
        str: Absolute path to each file found

    Note:
        If a path is a directory, recursively walks through all subdirectories
        to find files. If a path is a file, yields it directly.
    """
    for path in paths:
        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            for root, _, files in os.walk(path):
                for f in files:
                    yield os.path.join(root, f)

def merge_sorted_files(files, output_file, buffer_size=1024*1024):
    """
    Merge multiple sorted files into a single sorted output file using a min-heap.

    This function implements an efficient k-way merge algorithm that can handle
    large files with minimal memory usage. It uses a heap to always select the
    lexicographically smallest line from all input files.

    Args:
        files: List of paths to sorted input files
        output_file: Path where the merged output will be written, or '-' for stdout
        buffer_size: Buffer size in bytes for file I/O operations (default: 1MB)

    Algorithm:
        1. Opens all input files simultaneously
        2. Initializes a min-heap with the first line from each file
        3. Repeatedly extracts the smallest line and writes it to output
        4. Refills the heap with the next line from the same file
        5. Continues until all files are exhausted

    Time Complexity: O(N log k) where N is total lines and k is number of files
    Space Complexity: O(k) for the heap
    """
    # Open all input files with buffering for efficient I/O
    file_handles = [open(f, 'r', buffering=buffer_size) for f in files]

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
    if output_file == '-':
        out = sys.stdout
        # Process all lines in sorted order
        while heap:
            # Extract the lexicographically smallest line
            value, idx = heapq.heappop(heap)
            out.write(value)

            # Read the next line from the same file that just provided a line
            next_line = file_handles[idx].readline()
            if next_line:
                # Add the new line back to the heap if file has more content
                heapq.heappush(heap, (next_line, idx))
    else:
        with open(output_file, 'w', buffering=buffer_size) as out:
            # Process all lines in sorted order
            while heap:
                # Extract the lexicographically smallest line
                value, idx = heapq.heappop(heap)
                out.write(value)

                # Read the next line from the same file that just provided a line
                next_line = file_handles[idx].readline()
                if next_line:
                    # Add the new line back to the heap if file has more content
                    heapq.heappush(heap, (next_line, idx))

    # Clean up: close all input file handles
    for fh in file_handles:
        fh.close()

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Merge N sorted files or directories into one (optimized for large files)."
    )
    parser.add_argument("output", help="Output file name (use '-' for stdout)")
    parser.add_argument("paths", nargs="+", help="List of sorted input files or directories")
    args = parser.parse_args()

    # Collect all files from the provided paths (expands directories)
    files = list(get_all_files(args.paths))

    # Perform the k-way merge operation
    merge_sorted_files(files, args.output)
