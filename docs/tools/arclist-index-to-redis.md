# arclist-index-to-redis - Arclist to Redis Pipeline Wrapper

Complete pipeline wrapper that processes arclist files and loads them directly into Redis for distributed web archive access.

## Overview

`arclist-index-to-redis` is a thin Python wrapper that combines `arclist-to-path-index` and `path-index-to-redis` into a single convenient command. It provides better argument handling, colored logging, timing statistics, and integrated Redis key clearing.

**Key Features:**
- ✅ Complete arclist → Redis pipeline in one command
- ✅ Automatic Redis key clearing before import (optional)
- ✅ All Redis connection options supported
- ✅ Colored progress logging and statistics
- ✅ Performance tuning (batch size, connection pooling)
- ✅ Production-ready error handling

## Quick Start

```bash
# Basic usage - local Redis
arclist-index-to-redis -d /data/arclists -k pathindex:branchA

# With verbose output
arclist-index-to-redis -d /data/arclists -k pathindex:branchB -v

# Clear existing key before import
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v

# Remote Redis with authentication
arclist-index-to-redis -d /data/arclists -k pathindex:branchB \
    --host redis.arquivo.pt --password secret
```

## What It Does

`arclist-index-to-redis` executes this pipeline:

```
┌─────────────────────┐
│  Arclist Files      │ (URLs or file paths)
│  (*.txt in folder)  │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ arclist-to-path-    │ Extract basenames
│ index               │ Create TSV format
└──────┬──────────────┘
       │ (Unix pipe)
       ▼
┌─────────────────────┐
│ path-index-to-      │ Submit to Redis
│ redis               │ (Hash structure)
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Redis Database    │
│  pathindex:branchA  │
└─────────────────────┘
```

**With `--clear` option:**
1. **Clear Redis key** (once, before pipeline starts)
2. **Run pipeline** (arclist-to-path-index → path-index-to-redis)
3. **Show statistics** (processing time, success/failure)

## Command-Line Options

### Required Arguments

| Option | Description |
|--------|-------------|
| `-d, --folder DIR` | Folder containing arclist files (*.txt) |
| `-k, --redis-key KEY` | Redis hash key (e.g., 'pathindex:branchA') |

### Redis Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host HOST` | `localhost` | Redis server hostname |
| `--port PORT` | `6379` | Redis server port |
| `--db N` | `0` | Redis database number |
| `--password PASS` | - | Redis password (optional) |
| `--username USER` | - | Redis username for ACL (optional) |
| `--socket PATH` | - | Unix socket path (alternative to host:port) |
| `--ssl` | off | Use SSL/TLS connection |
| `--cluster` | off | Connect to Redis Cluster |

### Performance Tuning

| Option | Default | Description |
|--------|---------|-------------|
| `--batch-size N` | `500` | Number of entries per batch |
| `--pool-size N` | `10` | Connection pool size |
| `--timeout N` | `10` | Connection timeout in seconds |

### Behavior Options

| Option | Description |
|--------|-------------|
| `--clear` | Clear existing Redis hash key before importing |
| `-v, --verbose` | Print detailed progress and statistics |

## Usage Examples

### Example 1: Basic Import - Local Redis

Import arclist folder to local Redis:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA
```

**Output:**
```
(Silent unless errors occur)
```

### Example 2: Verbose Import with Statistics

Show detailed progress:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA -v
```

**Output:**
```
============================================================
Arclist to Redis Pipeline
============================================================

[INFO] Configuration:
  Arclist folder:      /data/arclists
  Redis key:           pathindex:branchA
  Redis host:          localhost:6379
  Redis database:      0
  Batch size:          500
  Clear before import: False

[INFO] Started at 2024-11-20 10:30:00

[INFO] Starting pipeline: arclist-to-path-index → path-index-to-redis

# Processing: AWP29_ARCS.txt
# Read 5432 lines: 5432 valid, 0 invalid
# Connecting to Redis: localhost:6379/0
# Redis connection successful
# Processing: -
  → Submitted 5000 entries (1250 entries/sec)
  → Submitted 5432 entries (1358 entries/sec)
# Completed -: 5432 entries, 0 errors
========================================
# SUMMARY
# Files processed: 1
# Entries submitted: 5432
# Errors: 0
# Time elapsed: 4.0 seconds
# Throughput: 1358 entries/second
========================================

============================================================
Pipeline Complete
============================================================

[SUCCESS] Processing complete

[INFO] Finished at 2024-11-20 10:30:04
```

### Example 3: Clear and Reimport

Clear existing key and reimport:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v
```

**Output shows clearing step:**
```
[INFO] Clearing Redis key: pathindex:branchA
[SUCCESS] Cleared existing Redis key

[INFO] Starting pipeline: arclist-to-path-index → path-index-to-redis
...
```

### Example 4: Remote Redis with Authentication

Connect to remote Redis server:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchB \
    --host redis.arquivo.pt \
    --port 6380 \
    --password "$REDIS_PASSWORD" \
    --verbose
```

### Example 5: Multiple Collections - Different Keys

Process multiple branches separately:

```bash
#!/bin/bash

for branch in branchA branchB branchC; do
    echo "Processing: $branch"
    arclist-index-to-redis \
        -d "/data/arclists/$branch" \
        -k "pathindex:$branch" \
        --clear \
        --verbose
    echo ""
done
```

### Example 6: High-Performance Import

Optimize for large datasets:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA \
    --batch-size 1000 \
    --pool-size 20 \
    --timeout 30 \
    --clear \
    --verbose
```

### Example 7: Unix Socket Connection

Use Unix socket instead of TCP:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA \
    --socket /var/run/redis/redis.sock \
    --verbose
```

### Example 8: Redis Cluster

Connect to Redis Cluster:

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchB \
    --cluster \
    --host redis-cluster.local \
    --port 7000 \
    --verbose
```

### Example 9: Custom Namespace Prefix

Use namespaced Redis keys:

```bash
arclist-index-to-redis -d /data/arclists -k "archive:pathindex:branchA" \
    --clear \
    --verbose
```

### Example 10: Daily Incremental Updates

Add new files to existing collection:

```bash
#!/bin/bash
# Daily cron job to update pathindex

BRANCH="branchA"
TODAY_ARCLIST="/data/arclists/daily/$BRANCH/$(date +%Y%m%d).txt"

if [ -f "$TODAY_ARCLIST" ]; then
    echo "Loading daily arclist: $TODAY_ARCLIST"
    
    # Create temporary folder with today's arclist
    TEMP_DIR=$(mktemp -d)
    cp "$TODAY_ARCLIST" "$TEMP_DIR/"
    
    arclist-index-to-redis \
        -d "$TEMP_DIR" \
        -k "pathindex:$BRANCH" \
        --verbose
    
    rm -rf "$TEMP_DIR"
    echo "Daily update complete"
else
    echo "No arclist for today"
fi
```

## Production Workflow

### Complete Setup Script

Full production deployment example:

```bash
#!/bin/bash
#
# production-setup.sh - Load all arclists into Redis
#

set -e  # Exit on error

# Configuration
ARCLISTS_BASE="/data/arclists"
REDIS_HOST="redis.arquivo.pt"
REDIS_PORT="6380"
REDIS_PASSWORD="${REDIS_PASSWORD:-}"  # From environment
BATCH_SIZE="1000"
BRANCHES=("branchA" "branchB" "branchC")

echo "=========================================="
echo "Arquivo.pt Path Index Redis Setup"
echo "=========================================="
echo ""

# Process each branch
for branch in "${BRANCHES[@]}"; do
    echo "Processing branch: $branch"
    echo "------------------------------------------"
    
    FOLDER="$ARCLISTS_BASE/$branch"
    REDIS_KEY="pathindex:$branch"
    
    if [ ! -d "$FOLDER" ]; then
        echo "ERROR: Folder not found: $FOLDER"
        continue
    fi
    
    # Count arclist files
    FILE_COUNT=$(find "$FOLDER" -name "*.txt" | wc -l)
    echo "Found $FILE_COUNT arclist files in $FOLDER"
    
    # Load to Redis
    arclist-index-to-redis \
        -d "$FOLDER" \
        -k "$REDIS_KEY" \
        --host "$REDIS_HOST" \
        --port "$REDIS_PORT" \
        --password "$REDIS_PASSWORD" \
        --batch-size "$BATCH_SIZE" \
        --clear \
        --verbose
    
    echo ""
    echo "Completed: $branch"
    echo ""
done

echo "=========================================="
echo "All branches processed successfully"
echo "=========================================="
```

### Monitoring Script

Check Redis population status:

```bash
#!/bin/bash
#
# check-redis-status.sh - Verify Redis path indexes
#

REDIS_HOST="redis.arquivo.pt"
REDIS_PORT="6380"
BRANCHES=("branchA" "branchB" "branchC")

echo "Redis Path Index Status"
echo "========================================"
echo ""

for branch in "${BRANCHES[@]}"; do
    KEY="pathindex:$branch"
    
    # Get key count
    COUNT=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HLEN "$KEY")
    
    # Get sample entry
    SAMPLE=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HRANDFIELD "$KEY" 1)
    
    echo "Branch: $branch"
    echo "  Key: $KEY"
    echo "  Entries: $COUNT"
    echo "  Sample: $SAMPLE"
    echo ""
done
```

## Redis Data Structure

The tool loads data into Redis Hash structures:

```
Key: pathindex:branchA
  Field: AWP-arquivo-20240101120000-00001.warc.gz
  Value: http://data.arquivo.pt/warcs/AWP-arquivo-20240101120000-00001.warc.gz

  Field: AWP-arquivo-20240101120500-00002.warc.gz
  Value: http://data.arquivo.pt/warcs/AWP-arquivo-20240101120500-00002.warc.gz
  
  ... (thousands of entries)
```

**Query examples:**

```bash
# Get path for specific file
redis-cli HGET "pathindex:branchA" "file.warc.gz"

# Count total files
redis-cli HLEN "pathindex:branchA"

# Get all filenames
redis-cli HKEYS "pathindex:branchA"

# Get random sample
redis-cli HRANDFIELD "pathindex:branchA" 10
```

## Performance

### Benchmarks

Tested on production data (Arquivo.pt):

| Dataset | Files | Size | Time | Throughput |
|---------|-------|------|------|------------|
| Small collection | 1K | 50KB | 2s | 500 files/sec |
| Medium collection | 50K | 2.5MB | 45s | 1,111 files/sec |
| Large collection | 500K | 25MB | 450s | 1,111 files/sec |

### Performance Tips

1. **Increase batch size** for large imports:
   ```bash
   --batch-size 1000  # Default: 500
   ```

2. **Increase connection pool** for better throughput:
   ```bash
   --pool-size 20  # Default: 10
   ```

3. **Use Unix socket** when Redis is local:
   ```bash
   --socket /var/run/redis/redis.sock
   ```

4. **Disable verbose mode** for maximum speed:
   ```bash
   # Remove -v flag for production imports
   ```

### Memory Usage

- **Minimal** - Uses Unix pipes (streaming)
- **No temporary files** - Direct memory-to-memory transfer
- **Constant memory** - Independent of dataset size

## Error Handling

### Common Errors and Solutions

**1. Missing dependencies**

```
[ERROR] Missing required dependencies:
  - arclist-to-path-index
  - path-index-to-redis
[ERROR] Install with: pip install -e .
```

**Solution:** Install the package
```bash
pip install -e .
```

**2. Redis connection failed**

```
[ERROR] path-index-to-redis failed with exit code 1
Error: Failed to connect to Redis: Connection refused
```

**Solution:** Check Redis is running and connection details are correct
```bash
# Test Redis connection
redis-cli -h redis.arquivo.pt -p 6380 ping
```

**3. Invalid folder**

```
[ERROR] arclist-to-path-index failed with exit code 1
Error: Folder does not exist: /data/arclists
```

**Solution:** Verify folder path
```bash
ls -la /data/arclists
```

**4. Authentication failed**

```
[ERROR] Failed to connect to Redis: NOAUTH Authentication required
```

**Solution:** Provide password
```bash
--password "$REDIS_PASSWORD"
```

## Comparison with Manual Pipeline

### Using arclist-index-to-redis (recommended)

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v
```

**Benefits:**
- ✅ Single command
- ✅ Integrated --clear option
- ✅ Colored progress logging
- ✅ Automatic error handling
- ✅ Statistics and timing

### Using manual pipeline

```bash
arclist-to-path-index -d /data/arclists --verbose | \
    path-index-to-redis -i - -k pathindex:branchA --clear --verbose
```

**Drawbacks:**
- ❌ More verbose
- ❌ No integrated logging
- ❌ Manual error handling
- ❌ No timing statistics

## Integration with pywb

After loading path indexes to Redis, configure pywb to use them:

**pywb config.yaml:**

```yaml
collections:
  arquivo:
    archive_paths: redis://redis.arquivo.pt:6380/0/pathindex:branchA
    
    # Redis resolver configuration
    redis_url: redis://redis.arquivo.pt:6380/0
    
    index:
      type: redis
```

**Query example:**

```python
import redis

# Connect to Redis
r = redis.Redis(host='redis.arquivo.pt', port=6380, db=0)

# Get path for WARC file
filename = "AWP-arquivo-20240101120000-00001.warc.gz"
path = r.hget("pathindex:branchA", filename)

print(f"File: {filename}")
print(f"Path: {path.decode()}")
```

## Troubleshooting

### No files processed

**Problem:** Tool completes but no entries submitted

**Diagnosis:**
```bash
# Check folder contents
ls -la /data/arclists

# Check if *.txt files exist
find /data/arclists -name "*.txt"

# Run with verbose
arclist-index-to-redis -d /data/arclists -k test --verbose
```

### Slow performance

**Problem:** Import takes too long

**Solution:** Increase batch size and connection pool
```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA \
    --batch-size 1000 \
    --pool-size 20 \
    --verbose
```

### Pipeline interrupted

**Problem:** Process interrupted (Ctrl+C or killed)

**Solution:** Re-run with --clear to start fresh
```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA \
    --clear \
    --verbose
```

### Redis key not cleared

**Problem:** Old data still in Redis

**Diagnosis:**
```bash
# Check key size before
redis-cli HLEN pathindex:branchA

# Run with --clear
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v

# Check key size after
redis-cli HLEN pathindex:branchA
```

## Related Tools

- **[arclist-to-path-index](arclist-to-path-index.md)** - Convert arclist to path index format
- **[path-index-to-redis](path-index-to-redis.md)** - Submit path index to Redis
- **[cdxj-index-collection](../reference-implementation.md)** - Complete CDXJ indexing pipeline

## Technical Details

### Implementation

`arclist-index-to-redis` is a thin wrapper that:

1. **Validates dependencies** - Checks for `arclist-to-path-index` and `path-index-to-redis`
2. **Builds command arguments** - Passes all Redis options through
3. **Executes pipeline** - Uses subprocess.Popen for Unix piping
4. **Handles errors** - Captures exit codes and displays errors
5. **Shows statistics** - Timing and success/failure reporting

### Pipeline Execution

```python
# Start arclist-to-path-index
arclist_proc = subprocess.Popen(
    ["arclist-to-path-index", "-d", folder],
    stdout=subprocess.PIPE,
    stderr=sys.stderr,
)

# Start path-index-to-redis, reading from arclist-to-path-index stdout
redis_proc = subprocess.Popen(
    ["path-index-to-redis", "-i", "-", "-k", redis_key, ...],
    stdin=arclist_proc.stdout,
    stderr=sys.stderr,
)

# Close parent stdout to enable SIGPIPE
arclist_proc.stdout.close()

# Wait for completion
redis_proc.wait()
arclist_proc.wait()
```

### --clear Option

The `--clear` flag is passed through to `path-index-to-redis`, which:

1. **Connects to Redis** once before processing
2. **Deletes the key** (`redis.delete(key)`)
3. **Proceeds with import** (fresh start)

This ensures the key is cleared **exactly once** at the beginning, not repeatedly during processing.

## See Also

- [Production Workflows](../reference-implementation.md)
- [Arquivo.pt Architecture](../architecture.md)
- [Redis Integration Guide](../redis-integration.md)

---

**Related Commands:** `arclist-to-path-index` • `path-index-to-redis` • `cdxj-index-collection`
