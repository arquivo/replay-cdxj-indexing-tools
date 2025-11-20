"""
Main entry point for CDXJ/ZipNum search tool.
"""

import argparse
import sys
from typing import List

import surt

from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file
from replay_cdxj_indexing_tools.search.file_discovery import (
    detect_file_type,
    discover_files,
    find_zipnum_data_file,
)
from replay_cdxj_indexing_tools.search.filters import CDXJFilter, deduplicate_lines, sort_lines
from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file


def apply_match_type(search_key: str, match_type: str) -> tuple:
    """
    Apply match type to search key.

    Args:
        search_key: Original SURT key
        match_type: Match type (exact, prefix, host, domain)

    Returns:
        Tuple of (modified_search_key, use_prefix_search)
    """
    # pylint: disable=too-many-return-statements
    if match_type == "exact":
        return (search_key, False)

    if match_type == "prefix":
        return (search_key, True)

    if match_type == "host":
        # Extract just the host part (everything before the first ')')
        if ")" in search_key:
            host_part = search_key.split(")", 1)[0] + ")"
            return (host_part, True)
        return (search_key, True)

    if match_type == "domain":
        # For domain matching, we need to match the domain and all subdomains
        # Example: com,example,) matches com,example,www,) and com,example,mail,) etc.
        if ")" in search_key:
            # Extract domain part (up to and including the ')')
            host_part = search_key.split(")", 1)[0] + ")"
            return (host_part, True)
        return (search_key, True)

    # Default to exact
    return (search_key, False)


def search_file(
    filepath: str,
    search_key: str,
    match_type: str,
    skip_errors: bool,
    verbose: bool,
    progress: bool,
    file_num: int,
    total_files: int,
) -> List[str]:
    """
    Search a single file (CDXJ or ZipNum).

    Args:
        filepath: Path to file
        search_key: SURT key to search for
        match_type: Match type (exact, prefix, host, domain)
        skip_errors: If True, skip errors and continue
        verbose: If True, print debug info
        progress: If True, show progress
        file_num: Current file number (for progress)
        total_files: Total number of files (for progress)

    Returns:
        List of matching lines
    """
    if progress:
        print(f"Searching file {file_num}/{total_files}: {filepath}", file=sys.stderr)

    # Apply match type
    modified_key, use_prefix = apply_match_type(search_key, match_type)

    try:
        file_type = detect_file_type(filepath)

        if file_type == "cdxj":
            return search_cdxj_file(filepath, modified_key, use_prefix, verbose)

        elif file_type == "zipnum_idx":
            data_file = find_zipnum_data_file(filepath)
            return search_zipnum_file(filepath, data_file, modified_key, use_prefix, verbose)

        elif file_type == "zipnum_data":
            # For .cdxj.gz files, we need to find the index first
            # This is less efficient, but we'll support it
            # pylint: disable=import-outside-toplevel
            from replay_cdxj_indexing_tools.search.file_discovery import find_zipnum_index_file

            idx_file = find_zipnum_index_file(filepath)
            return search_zipnum_file(idx_file, filepath, modified_key, use_prefix, verbose)

        else:
            if verbose:
                print(
                    f"Warning: Unknown file type for {filepath}, treating as CDXJ", file=sys.stderr
                )
            return search_cdxj_file(filepath, modified_key, use_prefix, verbose)

    except Exception as e:
        if skip_errors:
            print(f"Error processing {filepath}: {e}", file=sys.stderr)
            return []
        else:
            raise


def main():
    """Main entry point for cdxj-search command."""
    parser = argparse.ArgumentParser(
        description="Binary search for CDXJ and ZipNum indexes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simple exact match
  cdxj-search --url http://example.com/page /data/index.cdxj

  # Prefix match with date range
  cdxj-search --url http://example.com/ --matchType prefix \\
    --from 2020 --to 2021 /data/*.cdxj

  # Host match (match all paths for a host)
  cdxj-search --url http://example.com/any/path --matchType host /data/*.cdxj

  # Domain match (match host and all subdomains)
  cdxj-search --url http://example.com --matchType domain /data/*.cdxj

  # Multiple filters
  cdxj-search --surt "com,example)/" \\
    --filter status=200 \\
    --filter mime~text/.* \\
    --limit 100 /data/indexes/

  # Search with glob pattern
  cdxj-search --url http://example.com /data/RAQ*.cdxj
        """,
    )

    # Required: URL or SURT (mutually exclusive)
    url_group = parser.add_mutually_exclusive_group(required=True)
    url_group.add_argument("--url", help="URL to search for (will be converted to SURT)")
    url_group.add_argument("--surt", help="SURT key to search for")

    # File inputs
    parser.add_argument(
        "files", nargs="+", metavar="FILE", help="CDXJ/ZipNum files, glob patterns, or directories"
    )

    # Search options
    parser.add_argument(
        "--matchType",
        choices=["exact", "prefix", "host", "domain"],
        default="exact",
        help="Match type: exact (default), prefix (path prefix), "
        "host (all paths for host), domain (host + subdomains)",
    )

    # Filtering options
    parser.add_argument(
        "--from", dest="from_ts", help="Start timestamp (flexible: 2020, 202001, 20200101, etc.)"
    )
    parser.add_argument(
        "--to", dest="to_ts", help="End timestamp (flexible: 2021, 202112, 20211231, etc.)"
    )
    parser.add_argument(
        "--filter",
        action="append",
        dest="filters",
        help="Filter by field (e.g., status=200, mime~text/.*, status!=404). "
        "Can be used multiple times.",
    )
    parser.add_argument("--limit", type=int, help="Limit number of results (default: unlimited)")

    # Output options
    parser.add_argument(
        "--sort", action="store_true", help="Sort results by timestamp within SURT key"
    )
    parser.add_argument(
        "--dedupe", action="store_true", help="Remove duplicate entries (same SURT + timestamp)"
    )

    # Error handling
    parser.add_argument(
        "--skip-errors",
        action="store_true",
        help="Skip corrupted/unreadable files (default: fail on error)",
    )

    # Debug options
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output to stderr"
    )
    parser.add_argument("--progress", action="store_true", help="Show progress indicator on stderr")

    args = parser.parse_args()

    # Convert URL to SURT if needed
    if args.url:
        try:
            search_key = surt.surt(args.url)
            if args.verbose:
                print(f"Converted URL to SURT: {args.url} -> {search_key}", file=sys.stderr)
        except Exception as e:
            print(f"Error converting URL to SURT: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        search_key = args.surt

    # Discover files
    try:
        files = discover_files(args.files, args.verbose)
    except Exception as e:
        print(f"Error discovering files: {e}", file=sys.stderr)
        sys.exit(1)

    if not files:
        print("No files found matching the specified patterns", file=sys.stderr)
        sys.exit(1)

    # Search all files
    all_results = []

    for i, filepath in enumerate(files, 1):
        try:
            results = search_file(
                filepath,
                search_key,
                args.matchType,
                args.skip_errors,
                args.verbose,
                args.progress,
                i,
                len(files),
            )
            all_results.extend(results)
        except Exception as e:
            print(f"Error processing {filepath}: {e}", file=sys.stderr)
            if not args.skip_errors:
                sys.exit(1)

    if args.verbose:
        print(f"Total results before filtering: {len(all_results)}", file=sys.stderr)

    # Apply filters
    if args.from_ts or args.to_ts or args.filters:
        cdxj_filter = CDXJFilter(args.from_ts, args.to_ts, args.filters)
        all_results = [line for line in all_results if cdxj_filter.matches(line)]

        if args.verbose:
            print(f"Results after filtering: {len(all_results)}", file=sys.stderr)

    # Sort if requested
    if args.sort:
        all_results = sort_lines(all_results)
        if args.verbose:
            print("Results sorted by timestamp", file=sys.stderr)

    # Deduplicate if requested
    if args.dedupe:
        original_count = len(all_results)
        all_results = deduplicate_lines(all_results)
        if args.verbose:
            print(f"Removed {original_count - len(all_results)} duplicates", file=sys.stderr)

    # Apply limit
    if args.limit and len(all_results) > args.limit:
        all_results = all_results[: args.limit]
        if args.verbose:
            print(f"Limited results to {args.limit}", file=sys.stderr)

    # Output results
    for line in all_results:
        print(line)

    if args.verbose:
        print(f"Output {len(all_results)} results", file=sys.stderr)


if __name__ == "__main__":
    main()
