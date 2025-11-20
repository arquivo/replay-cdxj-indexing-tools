#!/usr/bin/env python3
"""
path_index_to_redis.py - Submit pywb Path Index Files to Redis Database
====================================================================

This tool reads pywb path index files and submits the index entries
to a Redis database. It's designed for distributed web archive systems where
index lookup needs to be fast and scalable.

The pywb path index format consists of tab-separated values:
    <filename>\t<path1>[\t<path2>...]

Each line contains an ARC/WARC filename followed by one or more file paths/URLs
where that file can be found. This tool stores these entries in Redis for fast
lookup by filename.

COMMAND-LINE USAGE
==================

Basic Examples:

    # Submit a path index file to Redis (default localhost:6379)
    path-index-to-redis -i pathindex.txt -k pathindex:branchA

    # Submit to specific Redis server
    path-index-to-redis -i pathindex.txt -k pathindex:branchB \\
        --host redis.example.com --port 6380

    # Submit with authentication
    path-index-to-redis -i pathindex.txt -k pathindex:branchA --password mypassword

    # Submit to specific database number
    path-index-to-redis -i pathindex.txt -k pathindex:branchB --db 2

    # Use a custom key with namespace prefix
    path-index-to-redis -i pathindex.txt -k archive:pathindex:branchA

    # Batch inserts for better performance
    path-index-to-redis -i pathindex.txt -k pathindex:branchA --batch-size 1000

Advanced Examples:

    # Multiple index files (will be processed sequentially)
    path-index-to-redis -i index1.txt -i index2.txt -i index3.txt -k pathindex:branchA

    # Read from stdin (useful for pipelines)
    cat pathindex.txt | path-index-to-redis -i - -k pathindex:branchB

    # Clear existing hash key before submission
    path-index-to-redis -i pathindex.txt -k pathindex:branchA --clear

    # Use Redis Cluster
    path-index-to-redis -i pathindex.txt -k pathindex:branchB \\
        --cluster --host redis-cluster.local

    # With connection pooling and custom timeout
    path-index-to-redis -i pathindex.txt -k pathindex:branchA --pool-size 20 --timeout 30

    # Verbose output with statistics
    path-index-to-redis -i pathindex.txt -k pathindex:branchB -v

REDIS DATA STRUCTURE
====================

The tool uses Redis Hashes to store path index entries efficiently:

    Key: <redis_key> (specified by user)
    Field: <filename>
    Value: <path1>[,<path2>,...]

Each Redis key represents a collection, and within that key, each filename
maps to one or more paths (comma-separated if multiple).

Example:
    Key: "pathindex:branchA"
    Field: "AWP-arquivo-20240101120000-00001.warc.gz"
    Value: "/mnt/storage1/warcs/2024/01/AWP-arquivo-20240101120000-00001.warc.gz"

This structure enables:
- Fast O(1) lookups by filename
- Efficient memory usage (Redis Hash compression)
- Multiple paths per file (for redundancy)
- Namespace isolation with prefixes
- Compatible with pywb's RedisResolver

PARAMETERS
==========

Required:
    -i, --input FILE       Input .idx file(s), or '-' for stdin
                          Can be specified multiple times
    -k, --redis-key KEY    Redis hash key (e.g., 'pathindex:branchA')

Optional - Redis Connection:
    --host HOST           Redis host (default: localhost)
    --port PORT           Redis port (default: 6379)
    --db N                Redis database number (default: 0)
    --password PASS       Redis password (default: none)
    --username USER       Redis username (for ACL, default: none)
    --socket PATH         Unix socket path (alternative to host:port)
    --ssl                 Use SSL/TLS connection
    --cluster             Connect to Redis Cluster

Optional - Performance:
    --batch-size N        Number of entries per batch (default: 500)
    --pool-size N         Connection pool size (default: 10)
    --timeout N           Connection timeout in seconds (default: 10)

Optional - Behavior:
    --clear               Clear existing hash key before insert
    --dry-run             Parse and validate only, don't write to Redis

Optional - Output:
    -v, --verbose         Print detailed progress and statistics

NOTES
=====

- Index files must follow pywb path index format (tab-separated)
- Lines starting with # are treated as comments and skipped
- Invalid lines are logged but don't stop processing
- Batch size affects memory usage and performance
- Use --cluster for Redis Cluster deployments
- Key prefixes help organize multi-collection archives

PERFORMANCE TIPS
================

- Batch size 500-1000 works well for most cases
- Use --workers for very large index files (>10GB)
- Connection pooling improves throughput with multiple files
- Redis pipelining is used automatically in batches
- Monitor Redis memory usage during large imports

INTEGRATION EXAMPLES
====================

1. Complete Pipeline Integration:

    # Generate index and submit to Redis in one pipeline
    cdxj-to-zipnum -o /data/zipnum -i merged.cdxj | \\
        path-index-to-redis -i - -k "pathindex:branchA"

2. Daily Updates:

    #!/bin/bash
    BRANCH="branchA"
    cdxj-index-collection COLLECTION-$BRANCH
    path-index-to-redis \\
        -i /data/zipnum/COLLECTION-$BRANCH/*.idx \\
        -k "pathindex:$BRANCH" \\
        --verbose

3. Multi-Collection Setup:

    # Submit multiple collections with different keys
    for branch in branchA branchB branchC; do
        path-index-to-redis \\
            -i /data/indexes/$branch/*.idx \\
            -k "pathindex:$branch" \\
            --batch-size 1000
    done

4. Docker Deployment:

    docker run -v /data:/data arquivo/replay-cdxj-indexing-tools \\
        path-index-to-redis \\
        -i /data/indexes/index.idx \\
        --host redis \\
        -k "pathindex:branchA"

REDIS QUERY EXAMPLES
====================

After submission, query Redis directly:

    # Get path for a specific WARC file
    redis-cli HGET "pathindex:branchA" "AWP-arquivo-20240101120000-00001.warc.gz"

    # Get all files in the collection
    redis-cli HKEYS "pathindex:branchB"

    # Count total files in collection
    redis-cli HLEN "pathindex:branchA"

    # Check memory usage
    redis-cli INFO memory

Author: Ivo Branco
"""

import argparse
import gzip
import sys
import time
from typing import Dict, Iterator, List, Optional, Tuple


def parse_index_line(line: str) -> Optional[Dict[str, str]]:
    """
    Parse a pywb path index line into structured data.

    Format: <filename>\\t<path1>[\\t<path2>...]

    Args:
        line: Line from path index file

    Returns:
        Dictionary with 'filename' and 'paths' (comma-separated), or None if invalid

    Example:
        >>> line = "file.warc.gz\\t/mnt/storage/file.warc.gz\\t/backup/file.warc.gz"
        >>> result = parse_index_line(line)
        >>> result['filename']
        'file.warc.gz'
        >>> result['paths']
        '/mnt/storage/file.warc.gz,/backup/file.warc.gz'
    """
    line = line.strip()

    # Skip empty lines and comments
    if not line or line.startswith("#"):
        return None

    parts = line.split("\t")
    if len(parts) < 2:
        return None

    # First column is filename, remaining columns are paths
    # For Redis, we store only the first path (primary location)
    # Multiple paths in path index are typically for redundancy/fallback
    return {
        "filename": parts[0],
        "path": parts[1],  # Use only the first path
    }


def open_index_file(path: str):
    """
    Open index file for reading, supports stdin and gzip.

    Args:
        path: File path or '-' for stdin

    Returns:
        File handle (text mode)
    """
    if path == "-":
        return sys.stdin
    elif path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    else:
        return open(path, "r", encoding="utf-8")


def read_index_entries(input_path: str, verbose: bool = False) -> Iterator[Dict[str, str]]:
    """
    Generator that yields parsed index entries from file.

    Args:
        input_path: Path to .idx file or '-' for stdin
        verbose: Print progress messages

    Yields:
        Parsed index entry dictionaries

    Example:
        >>> for entry in read_index_entries('pathindex.txt'):
        ...     print(entry['filename'], entry['path'])
    """
    line_count = 0
    valid_count = 0
    invalid_count = 0

    with open_index_file(input_path) as fh:
        for line_num, line in enumerate(fh, 1):
            line_count += 1

            entry = parse_index_line(line)
            if entry:
                valid_count += 1
                yield entry
            else:
                invalid_count += 1
                if verbose and invalid_count <= 10:  # Limit warning spam
                    print(f"Warning: Invalid line {line_num}: {line[:60]}...", file=sys.stderr)

    if verbose:
        print(
            f"# Read {line_count} lines: {valid_count} valid, {invalid_count} invalid",
            file=sys.stderr,
        )


def submit_index_to_redis(  # pylint: disable=unexpected-keyword-arg
    input_paths: List[str],
    redis_key: str,
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
    redis_password: Optional[str] = None,
    redis_username: Optional[str] = None,
    redis_socket: Optional[str] = None,
    use_ssl: bool = False,
    use_cluster: bool = False,
    batch_size: int = 500,
    pool_size: int = 10,
    timeout: int = 10,
    clear_existing: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> Tuple[int, int]:
    """
    Submit pywb path index entries to Redis database.

    Args:
        input_paths: List of path index file paths (or '-' for stdin)
        redis_key: Redis hash key for storing path index (e.g., 'pathindex:branchA')
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (optional)
        redis_username: Redis username for ACL (optional)
        redis_socket: Unix socket path (alternative to host:port)
        use_ssl: Use SSL/TLS connection
        use_cluster: Connect to Redis Cluster
        batch_size: Number of entries to submit in one pipeline
        pool_size: Redis connection pool size
        timeout: Connection timeout in seconds
        clear_existing: Clear existing hash key before inserting
        dry_run: Only parse and validate, don't write to Redis
        verbose: Print progress information

    Returns:
        Tuple of (entries_submitted, errors_encountered)

    Raises:
        redis.exceptions.ConnectionError: If Redis connection fails
        IOError: If input file cannot be read

    Example:
        >>> submitted, errors = submit_index_to_redis(
        ...     ['pathindex.txt'],
        ...     redis_key='pathindex:branchA',
        ...     redis_host='localhost',
        ...     verbose=True
        ... )
        >>> print(f"Submitted {submitted} entries")
    """
    total_submitted = 0
    total_errors = 0
    start_time = time.time()

    if dry_run:
        if verbose:
            print("# DRY RUN MODE - No data will be written to Redis", file=sys.stderr)
    else:
        # Only try to import redis if not in dry-run mode
        try:
            import redis  # pylint: disable=import-outside-toplevel
        except ImportError:
            print(
                "Error: redis package not installed. Install with: pip install redis",
                file=sys.stderr,
            )
            sys.exit(1)

    # Create Redis connection
    redis_client: object = None  # type: ignore[assignment]
    if not dry_run:
        if verbose:
            if redis_socket:
                print(f"# Connecting to Redis via socket: {redis_socket}", file=sys.stderr)
            elif use_cluster:
                print(f"# Connecting to Redis Cluster: {redis_host}:{redis_port}", file=sys.stderr)
            else:
                print(
                    f"# Connecting to Redis: {redis_host}:{redis_port}/" f"{redis_db}",
                    file=sys.stderr,
                )

        try:
            if use_cluster:
                # Redis Cluster connection
                # pylint: disable=no-member,possibly-used-before-assignment
                redis_client = redis.RedisCluster(  # type: ignore[assignment]
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    username=redis_username,  # pylint: disable=unexpected-keyword-arg
                    ssl=use_ssl,
                    socket_timeout=timeout,
                    socket_connect_timeout=timeout,
                )
            elif redis_socket:
                # Unix socket connection
                # pylint: disable=unexpected-keyword-arg
                redis_client = redis.Redis(  # type: ignore[assignment]
                    unix_socket_path=redis_socket,
                    db=redis_db,
                    password=redis_password,
                    username=redis_username,  # pylint: disable=unexpected-keyword-arg
                    socket_timeout=timeout,
                    socket_connect_timeout=timeout,
                )
            else:
                # Standard TCP connection
                # type: ignore[assignment]
                # pylint: disable=unexpected-keyword-arg
                redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    db=redis_db,
                    password=redis_password,
                    username=redis_username,  # pylint: disable=unexpected-keyword-arg
                    ssl=use_ssl,
                    socket_timeout=timeout,
                    socket_connect_timeout=timeout,
                    max_connections=pool_size,
                )

            # Test connection
            redis_client.ping()  # type: ignore[attr-defined]
            if verbose:
                print("# Redis connection successful", file=sys.stderr)

        except redis.exceptions.ConnectionError as e:
            print(f"Error: Failed to connect to Redis: {e}", file=sys.stderr)
            return 0, 1
        except Exception as e:
            print(f"Error: Redis connection error: {e}", file=sys.stderr)
            return 0, 1

        # Clear existing hash key if requested
        if clear_existing:
            if verbose:
                print(f"# Clearing existing hash key: {redis_key}", file=sys.stderr)
            try:
                deleted = redis_client.delete(redis_key)
                if verbose and deleted:
                    print("# Deleted existing hash key", file=sys.stderr)
            except Exception as e:
                print(f"Warning: Error clearing hash key: {e}", file=sys.stderr)

    # Process each input file
    # pylint: disable=too-many-nested-blocks
    for input_path in input_paths:
        if verbose:
            print(f"\n# Processing: {input_path}", file=sys.stderr)

        file_submitted = 0
        file_errors = 0
        batch = []

        try:
            for entry in read_index_entries(input_path, verbose=verbose):
                # Each entry is a field in the hash: filename -> path
                batch.append((entry["filename"], entry["path"]))

                # Submit batch when full
                if len(batch) >= batch_size:
                    if not dry_run:
                        try:
                            # Use pipeline for atomic batch submission
                            pipe = redis_client.pipeline()  # type: ignore[attr-defined]
                            for filename, path in batch:
                                pipe.hset(redis_key, filename, path)
                            pipe.execute()
                            file_submitted += len(batch)
                        except Exception as e:
                            print(f"Error submitting batch: {e}", file=sys.stderr)
                            file_errors += len(batch)
                    else:
                        file_submitted += len(batch)

                    batch.clear()

                    if verbose and file_submitted % 10000 == 0:
                        elapsed = time.time() - start_time
                        rate = file_submitted / elapsed if elapsed > 0 else 0
                        print(
                            f"  → Submitted {file_submitted} entries ({rate:.0f} entries/sec)",
                            file=sys.stderr,
                        )

            # Submit remaining batch
            if batch:
                if not dry_run:
                    try:
                        pipe = redis_client.pipeline()  # type: ignore[attr-defined]
                        for filename, path in batch:
                            pipe.hset(redis_key, filename, path)
                        pipe.execute()
                        file_submitted += len(batch)
                    except Exception as e:
                        print(f"Error submitting final batch: {e}", file=sys.stderr)
                        file_errors += len(batch)
                else:
                    file_submitted += len(batch)

            total_submitted += file_submitted
            total_errors += file_errors

            if verbose:
                print(
                    f"# Completed {input_path}: {file_submitted} entries, {file_errors} errors",
                    file=sys.stderr,
                )

        except IOError as e:
            print(f"Error reading {input_path}: {e}", file=sys.stderr)
            total_errors += 1
        except KeyboardInterrupt:
            print("\n# Interrupted by user", file=sys.stderr)
            break
        except Exception as e:
            print(f"Error processing {input_path}: {e}", file=sys.stderr)
            total_errors += 1

    # Close Redis connection
    if not dry_run:
        try:
            redis_client.close()  # type: ignore[attr-defined]
        except Exception:
            pass

    # Final statistics
    elapsed = time.time() - start_time
    rate = total_submitted / elapsed if elapsed > 0 else 0

    if verbose:
        print("\n" + "=" * 60, file=sys.stderr)
        print("# SUMMARY", file=sys.stderr)
        print(f"# Files processed: {len(input_paths)}", file=sys.stderr)
        print(f"# Entries submitted: {total_submitted}", file=sys.stderr)
        print(f"# Errors: {total_errors}", file=sys.stderr)
        print(f"# Time elapsed: {elapsed:.1f} seconds", file=sys.stderr)
        print(f"# Throughput: {rate:.0f} entries/second", file=sys.stderr)
        print("=" * 60, file=sys.stderr)

    return total_submitted, total_errors


def main(argv=None):
    """
    Command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Submit pywb path index files to Redis database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit path index file to local Redis
  %(prog)s -i pathindex.txt -k pathindex:branchA

  # Submit to remote Redis with authentication
  %(prog)s -i pathindex.txt -k pathindex:branchB --host redis.example.com --password secret

  # Submit with custom key (with namespace prefix)
  %(prog)s -i pathindex.txt -k archive:pathindex:branchA

  # Submit multiple files with batch processing
  %(prog)s -i index1.txt -i index2.txt -i index3.txt -k pathindex:branchA --batch-size 1000

  # Read from stdin (pipeline mode)
  cat pathindex.txt | %(prog)s -i - -k pathindex:branchB --verbose

  # Clear existing hash key before submission
  %(prog)s -i pathindex.txt -k pathindex:branchA --clear

  # Dry run to validate index file
  %(prog)s -i pathindex.txt -k pathindex:branchB --dry-run --verbose

Redis connection examples:
  # Standard connection
  %(prog)s -i pathindex.txt -k pathindex:branchA --host localhost --port 6379 --db 0

  # Unix socket
  %(prog)s -i pathindex.txt -k pathindex:branchB --socket /var/run/redis/redis.sock

  # SSL/TLS connection
  %(prog)s -i pathindex.txt -k pathindex:branchA --host redis.example.com --ssl

  # Redis Cluster
  %(prog)s -i pathindex.txt -k pathindex:branchB --cluster --host redis-cluster.local
        """,
    )

    # Input arguments
    parser.add_argument(
        "-i",
        "--input",
        action="append",
        dest="inputs",
        required=True,
        help="Input path index file(s), or '-' for stdin (can specify multiple times)",
    )
    parser.add_argument(
        "-k",
        "--redis-key",
        required=True,
        help="Redis hash key for path index (e.g., 'pathindex:branchA')",
    )

    # Redis connection arguments
    redis_conn = parser.add_argument_group("Redis connection")
    redis_conn.add_argument("--host", default="localhost", help="Redis host (default: localhost)")
    redis_conn.add_argument("--port", type=int, default=6379, help="Redis port (default: 6379)")
    redis_conn.add_argument("--db", type=int, default=0, help="Redis database number (default: 0)")
    redis_conn.add_argument("--password", help="Redis password (optional)")
    redis_conn.add_argument("--username", help="Redis username for ACL (optional)")
    redis_conn.add_argument("--socket", help="Unix socket path (alternative to host:port)")
    redis_conn.add_argument("--ssl", action="store_true", help="Use SSL/TLS connection")
    redis_conn.add_argument("--cluster", action="store_true", help="Connect to Redis Cluster")

    # Performance arguments
    perf_group = parser.add_argument_group("Performance tuning")
    perf_group.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of entries per batch (default: 500)",
    )
    perf_group.add_argument(
        "--pool-size", type=int, default=10, help="Connection pool size (default: 10)"
    )
    perf_group.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Connection timeout in seconds (default: 10)",
    )

    # Behavior arguments
    behavior_group = parser.add_argument_group("Behavior options")
    behavior_group.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing hash key before inserting",
    )
    behavior_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate only, don't write to Redis",
    )

    # Output arguments
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print detailed progress and statistics"
    )

    args = parser.parse_args(argv)

    # Validate arguments
    if not args.inputs:
        parser.error("At least one input file must be specified with -i/--input")

    if args.batch_size < 1:
        parser.error("Batch size must be at least 1")

    # Run submission
    try:
        _submitted, errors = submit_index_to_redis(
            input_paths=args.inputs,
            redis_key=args.redis_key,
            redis_host=args.host,
            redis_port=args.port,
            redis_db=args.db,
            redis_password=args.password,
            redis_username=args.username,
            redis_socket=args.socket,
            use_ssl=args.ssl,
            use_cluster=args.cluster,
            batch_size=args.batch_size,
            pool_size=args.pool_size,
            timeout=args.timeout,
            clear_existing=args.clear,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        # Exit with error code if any errors occurred
        if errors > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n# Interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
