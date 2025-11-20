#!/usr/bin/env python3
"""
arclist_index_to_redis.py - Arclist to Redis Pipeline Wrapper
==============================================================

This tool provides a Python wrapper around the arclist-to-path-index and
path-index-to-redis pipeline, similar to cdxj-index-collection.sh.

It processes arclist files (containing URLs/paths to WARC/ARC files) and
loads them directly into Redis for distributed web archive access.

The pipeline:
    1. arclist-to-path-index: Reads arclist files, extracts basenames
    2. path-index-to-redis: Submits path index entries to Redis

COMMAND-LINE USAGE
==================

Basic Examples:

    # Process arclist folder and submit to Redis
    arclist-index-to-redis -d /data/arclists -k pathindex:branchA

    # Submit to remote Redis with authentication
    arclist-index-to-redis -d /data/arclists -k pathindex:branchB \\
        --host redis.example.com --password secret

    # Clear existing Redis key before import
    arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear

    # Verbose output with statistics
    arclist-index-to-redis -d /data/arclists -k pathindex:branchB -v

Advanced Examples:

    # Custom batch size for large imports
    arclist-index-to-redis -d /data/arclists -k pathindex:branchA --batch-size 1000

    # Use Redis Cluster
    arclist-index-to-redis -d /data/arclists -k pathindex:branchB \\
        --cluster --host redis-cluster.local

    # With custom namespace prefix
    arclist-index-to-redis -d /data/arclists -k archive:pathindex:branchA --verbose

    # Unix socket connection
    arclist-index-to-redis -d /data/arclists -k pathindex:branchB \\
        --socket /var/run/redis/redis.sock

PARAMETERS
==========

Required:
    -d, --folder DIR          Folder containing arclist files (*.txt)
    -k, --redis-key KEY       Redis hash key (e.g., 'pathindex:branchA')

Optional - Redis Connection:
    --host HOST               Redis host (default: localhost)
    --port PORT               Redis port (default: 6379)
    --db N                    Redis database number (default: 0)
    --password PASS           Redis password (default: none)
    --username USER           Redis username (for ACL, default: none)
    --socket PATH             Unix socket path (alternative to host:port)
    --ssl                     Use SSL/TLS connection
    --cluster                 Connect to Redis Cluster

Optional - Performance:
    --batch-size N            Number of entries per batch (default: 500)
    --pool-size N             Connection pool size (default: 10)
    --timeout N               Connection timeout in seconds (default: 10)

Optional - Behavior:
    --clear                   Clear existing hash key before import

Optional - Output:
    -v, --verbose             Print detailed progress and statistics

EXAMPLES
========

1. Basic Daily Import:

    arclist-index-to-redis \\
        -d /data/arclists/branchA \\
        -k pathindex:branchA \\
        --verbose

2. Multi-Branch Setup:

    for branch in branchA branchB branchC; do
        arclist-index-to-redis \\
            -d /data/arclists/$branch \\
            -k pathindex:$branch \\
            --clear \\
            --verbose
    done

3. Remote Redis with Authentication:

    arclist-index-to-redis \\
        -d /data/arclists \\
        -k pathindex:branchA \\
        --host redis.arquivo.pt \\
        --port 6380 \\
        --password $REDIS_PASSWORD \\
        --batch-size 1000

Author: Ivo Branco
"""

import argparse
import subprocess
import sys
import time
from typing import List, Optional

# ANSI color codes for terminal output
BLUE = "\033[0;34m"
GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
NC = "\033[0m"  # No Color


def log_info(message: str) -> None:
    """Print info message in blue."""
    print(f"{BLUE}[INFO]{NC} {message}", file=sys.stderr)


def log_success(message: str) -> None:
    """Print success message in green."""
    print(f"{GREEN}[SUCCESS]{NC} {message}", file=sys.stderr)


def log_warning(message: str) -> None:
    """Print warning message in yellow."""
    print(f"{YELLOW}[WARNING]{NC} {message}", file=sys.stderr)


def log_error(message: str) -> None:
    """Print error message in red."""
    print(f"{RED}[ERROR]{NC} {message}", file=sys.stderr)


def check_dependencies() -> bool:
    """
    Check if required commands are available.

    Returns:
        True if all dependencies are available, False otherwise
    """
    missing = []

    # Check for arclist-to-path-index
    try:
        subprocess.run(
            ["arclist-to-path-index", "--help"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError:
        missing.append("arclist-to-path-index")

    # Check for path-index-to-redis
    try:
        subprocess.run(
            ["path-index-to-redis", "--help"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError:
        missing.append("path-index-to-redis")

    if missing:
        log_error("Missing required dependencies:")
        for cmd in missing:
            print(f"  - {cmd}", file=sys.stderr)
        log_error("Install with: pip install -e .")
        return False

    return True


def run_pipeline(
    arclist_folder: str,
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
    verbose: bool = False,
) -> int:
    """
    Run the arclist-to-path-index → path-index-to-redis pipeline.

    Args:
        arclist_folder: Folder containing arclist files
        redis_key: Redis hash key for storing path index
        redis_host: Redis server hostname
        redis_port: Redis server port
        redis_db: Redis database number
        redis_password: Redis password (optional)
        redis_username: Redis username (optional)
        redis_socket: Unix socket path (optional)
        use_ssl: Use SSL/TLS connection
        use_cluster: Connect to Redis Cluster
        batch_size: Number of entries per batch
        pool_size: Connection pool size
        timeout: Connection timeout
        clear_existing: Clear Redis key before import
        verbose: Print verbose output

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    start_time = time.time()

    # Print header
    if verbose:
        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("Arclist to Redis Pipeline", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("", file=sys.stderr)
        log_info("Configuration:")
        print(f"  Arclist folder:      {arclist_folder}", file=sys.stderr)
        print(f"  Redis key:           {redis_key}", file=sys.stderr)
        if redis_socket:
            print(f"  Redis socket:        {redis_socket}", file=sys.stderr)
        else:
            print(f"  Redis host:          {redis_host}:{redis_port}", file=sys.stderr)
            print(f"  Redis database:      {redis_db}", file=sys.stderr)
        print(f"  Batch size:          {batch_size}", file=sys.stderr)
        print(f"  Clear before import: {clear_existing}", file=sys.stderr)
        print("", file=sys.stderr)
        log_info(f"Started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("", file=sys.stderr)

    # Build command arguments
    arclist_cmd = ["arclist-to-path-index", "-d", arclist_folder]
    if verbose:
        arclist_cmd.append("--verbose")

    redis_cmd: List[str] = [
        "path-index-to-redis",
        "-i",
        "-",
        "-k",
        redis_key,
        "--host",
        redis_host,
        "--port",
        str(redis_port),
        "--db",
        str(redis_db),
        "--batch-size",
        str(batch_size),
        "--pool-size",
        str(pool_size),
        "--timeout",
        str(timeout),
    ]

    if redis_password:
        redis_cmd.extend(["--password", redis_password])
    if redis_username:
        redis_cmd.extend(["--username", redis_username])
    if redis_socket:
        redis_cmd.extend(["--socket", redis_socket])
    if use_ssl:
        redis_cmd.append("--ssl")
    if use_cluster:
        redis_cmd.append("--cluster")
    if clear_existing:
        redis_cmd.append("--clear")
    if verbose:
        redis_cmd.append("--verbose")

    # Run pipeline
    if verbose:
        log_info("Starting pipeline: arclist-to-path-index → path-index-to-redis")
        print("", file=sys.stderr)

    try:
        # Start arclist-to-path-index process
        arclist_proc = subprocess.Popen(
            arclist_cmd,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,
        )

        # Start path-index-to-redis process, reading from arclist-to-path-index stdout
        redis_proc = subprocess.Popen(
            redis_cmd,
            stdin=arclist_proc.stdout,
            stderr=sys.stderr,
        )

        # Close stdout in parent to allow arclist_proc to receive SIGPIPE if redis_proc exits
        if arclist_proc.stdout:
            arclist_proc.stdout.close()

        # Wait for both processes to complete
        redis_exitcode = redis_proc.wait()
        arclist_exitcode = arclist_proc.wait()

        # Check exit codes
        if arclist_exitcode != 0:
            log_error(f"arclist-to-path-index failed with exit code {arclist_exitcode}")
            return arclist_exitcode

        if redis_exitcode != 0:
            log_error(f"path-index-to-redis failed with exit code {redis_exitcode}")
            return redis_exitcode

    except KeyboardInterrupt:
        print("", file=sys.stderr)
        log_warning("Interrupted by user")
        # Try to terminate subprocesses gracefully
        try:
            if arclist_proc:
                arclist_proc.terminate()
            if redis_proc:
                redis_proc.terminate()
        except Exception:
            pass
        return 130

    except Exception as e:
        log_error(f"Pipeline execution failed: {e}")
        return 1

    # Print statistics
    elapsed = time.time() - start_time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)

    if verbose:
        print("", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("Pipeline Complete", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print("", file=sys.stderr)
        log_success("Statistics:")
        print(f"  Processing time:     {hours}h {minutes}m {seconds}s", file=sys.stderr)
        print("", file=sys.stderr)
        log_info(f"Finished at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("", file=sys.stderr)

    return 0


def main(argv=None):
    """
    Command-line interface.
    """
    parser = argparse.ArgumentParser(
        description="Process arclist files and submit to Redis (pipeline wrapper)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  %(prog)s -d /data/arclists -k pathindex:branchA

  # Remote Redis with authentication
  %(prog)s -d /data/arclists -k pathindex:branchB --host redis.example.com --password secret

  # Clear existing key before import
  %(prog)s -d /data/arclists -k pathindex:branchA --clear --verbose

  # Custom batch size for large imports
  %(prog)s -d /data/arclists -k pathindex:branchB --batch-size 1000 -v

  # Unix socket connection
  %(prog)s -d /data/arclists -k pathindex:branchA --socket /var/run/redis/redis.sock

  # Redis Cluster
  %(prog)s -d /data/arclists -k pathindex:branchB --cluster --host redis-cluster.local

Pipeline:
  arclist-to-path-index -d folder | path-index-to-redis -i - -k key

This is a thin Python wrapper around the bash pipeline with better
argument handling, colored logging, and --clear option support.
        """,
    )

    # Required arguments
    parser.add_argument(
        "-d",
        "--folder",
        required=True,
        help="Folder containing arclist files (*.txt)",
    )
    parser.add_argument(
        "-k",
        "--redis-key",
        required=True,
        help="Redis hash key (e.g., 'pathindex:branchA')",
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
        help="Clear existing hash key before importing",
    )

    # Output arguments
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print detailed progress and statistics"
    )

    args = parser.parse_args(argv)

    # Check dependencies
    if not check_dependencies():
        return 1

    # Run pipeline
    try:
        exit_code = run_pipeline(
            arclist_folder=args.folder,
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
            verbose=args.verbose,
        )
        return exit_code

    except KeyboardInterrupt:
        print("", file=sys.stderr)
        log_warning("Interrupted by user")
        return 130
    except Exception as e:
        log_error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
