# Redis Integration Guide

Guide for integrating web archive path indexes with Redis for distributed access.

## Overview

Redis integration allows distributed web archive systems to quickly look up WARC/ARC file locations. The path index is stored in Redis Hash structures, enabling fast O(1) lookups by filename.

## Architecture

```
┌─────────────────────┐
│  Arclist Files      │ URLs or file paths to WARC/ARC files
│  (*.txt)            │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ arclist-to-path-    │ Convert to path index format
│ index               │ (filename → path mapping)
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ path-index-to-      │ Submit to Redis
│ redis               │ (Hash: HSET key field value)
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Redis Database    │ Fast distributed lookups
│  Hash Structure     │ Key: pathindex:branchA
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   pywb / Custom     │ Resolve file locations
│   Applications      │ for web replay
└─────────────────────┘
```

## Available Tools

### 1. arclist-index-to-redis (Recommended)

**Complete pipeline in one command** - Easiest to use

```bash
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v
```

**Features:**
- ✅ Complete arclist → Redis pipeline
- ✅ Integrated --clear option
- ✅ Colored logging and statistics
- ✅ Production-ready

**Use when:**
- Setting up new path indexes
- Daily/periodic full reloads
- You want simplicity and convenience

**Documentation:** [arclist-index-to-redis](tools/arclist-index-to-redis.md)

### 2. Manual Pipeline (Advanced)

**Separate commands with more control**

```bash
arclist-to-path-index -d /data/arclists | \
    path-index-to-redis -i - -k pathindex:branchA --clear -v
```

**Features:**
- ✅ Maximum flexibility
- ✅ Can inspect intermediate output
- ✅ Customize each step independently

**Use when:**
- Debugging path index conversion
- Need to save intermediate path index file
- Custom processing between steps

**Documentation:**
- [arclist-to-path-index](tools/arclist-to-path-index.md)
- [path-index-to-redis](tools/path-index-to-redis.md)

## Quick Start

### Basic Setup

```bash
# 1. Install package
pip install -e .

# 2. Load arclist files to Redis
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --clear \
    --verbose

# 3. Test Redis lookup
redis-cli HGET pathindex:branchA "AWP-arquivo-20240101.warc.gz"
```

### Production Setup

```bash
#!/bin/bash
# production-redis-setup.sh

set -e

# Arquivo.pt production setup: 2 branches (datacenters)
BRANCHES=("branchA" "branchB")
REDIS_HOST="redis.arquivo.pt"
REDIS_PORT="6380"

echo "Arquivo.pt Path Index Setup"
echo "2 branches: ~1.7GB arclist each"
echo "Expected Redis usage: ~6-8 GB total"
echo ""

for branch in "${BRANCHES[@]}"; do
    echo "Loading: $branch"
    
    arclist-index-to-redis \
        -d "/data/arclists/$branch" \
        -k "pathindex:$branch" \
        --host "$REDIS_HOST" \
        --port "$REDIS_PORT" \
        --password "$REDIS_PASSWORD" \
        --batch-size 1000 \
        --clear \
        --verbose
    
    echo "Completed: $branch"
done
```

## Redis Data Structure

### Hash Structure

Path indexes are stored as Redis Hashes:

```
Key: pathindex:branchA
  Field: filename1.warc.gz → Value: http://example.com/warcs/filename1.warc.gz
  Field: filename2.warc.gz → Value: /mnt/storage/warcs/filename2.warc.gz
  Field: filename3.warc.gz → Value: /backup/warcs/filename3.warc.gz
```

**Why Hash?**
- O(1) lookup by filename
- Memory-efficient (Redis Hash compression)
- Atomic updates per field
- Multiple collections via different keys

### Key Naming Convention

Recommended naming: `pathindex:<collection>`

**Examples:**
```
pathindex:branchA       # Branch A files
pathindex:branchB       # Branch B files
pathindex:branchC       # Branch C files

# With namespace
archive:pathindex:branchA
arquivo:pathindex:branchB
```

## Common Workflows

### 1. Initial Setup (Fresh Install)

Load all collections for the first time:

```bash
# Clear and load all branches
for branch in branchA branchB branchC; do
    arclist-index-to-redis \
        -d "/data/arclists/$branch" \
        -k "pathindex:$branch" \
        --clear \
        --verbose
done
```

### 2. Daily Updates (Incremental)

Add new files without clearing existing data:

```bash
# Daily cron job - add new files only
arclist-index-to-redis \
    -d /data/arclists/daily \
    -k pathindex:branchA \
    --verbose
    # Note: NO --clear flag
```

### 3. Complete Refresh (Periodic)

Periodically rebuild from scratch:

```bash
# Weekly/monthly full rebuild
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --clear \
    --verbose
```

### 4. Multiple Branches (Datacenters)

Separate path indexes per Arquivo.pt branch/datacenter:

```bash
# Branch A (datacenter A) - ~1.7GB arclist
arclist-index-to-redis \
    -d /data/arclists/branchA \
    -k pathindex:branchA \
    --clear -v

# Branch B (datacenter B) - ~1.7GB arclist
arclist-index-to-redis \
    -d /data/arclists/branchB \
    -k pathindex:branchB \
    --clear -v
```

**Arquivo.pt typical setup:**
- 2 branches (datacenters)
- ~1.7GB arclist per branch (raw text)
- Estimated Redis memory per branch: ~3-4 GB
- Total Redis memory (both branches): ~6-8 GB

### 5. High-Availability Setup

Load to multiple Redis instances:

```bash
# Primary Redis
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --host redis-primary.local \
    --clear -v

# Replica Redis
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --host redis-replica.local \
    --clear -v
```

## Redis Configuration

### Docker Deployment

The default Redis Docker image works perfectly for Arquivo.pt's path indexes. No custom configuration file needed.

**Simple deployment:**
```bash
docker run -d \
    --name redis-pathindex \
    -p 6379:6379 \
    --memory="18g" \
    redis:latest \
    redis-server \
        --maxmemory 16gb \
        --maxmemory-policy allkeys-lru
```

**Production deployment with persistence:**
```bash
docker run -d \
    --name redis-pathindex \
    -p 6379:6379 \
    --memory="18g" \
    -v /data/redis:/data \
    redis:latest \
    redis-server \
        --maxmemory 16gb \
        --maxmemory-policy allkeys-lru \
        --save 900 1 \
        --save 300 10 \
        --save 60 10000 \
        --appendonly yes
```

**Docker Compose:**
```yaml
version: '3.8'

services:
  redis-pathindex:
    image: redis:latest
    container_name: redis-pathindex
    ports:
      - "6379:6379"
    volumes:
      - /data/redis:/data
    command: >
      redis-server
      --maxmemory 16gb
      --maxmemory-policy allkeys-lru
      --save 900 1
      --save 300 10
      --save 60 10000
      --appendonly yes
    deploy:
      resources:
        limits:
          memory: 18G
    restart: unless-stopped
```

**Configuration notes:**
- **Memory limit**: 16GB for data + 2GB buffer = 18GB container limit
- **Maxmemory policy**: `allkeys-lru` evicts least recently used keys when full
- **Persistence**: RDB snapshots + AOF log for durability
- **Hash encoding**: Redis automatically uses optimal encoding (hash table for Arquivo.pt URLs ~140-150 bytes)

### Memory Estimation

Estimate Redis memory usage for Arquivo.pt path indexes:

```
Files: N
Avg filename length: F (~40 bytes for Arquivo.pt ARC/WARC files)
Avg URL length: U (~145 bytes for Arquivo.pt URLs)

Memory ≈ N × (F + U + 96)  # ~96 bytes overhead per hash entry

Example (based on typical Arquivo.pt data):
  1M files × (40 + 145 + 96) = 281 MB
  10M files × (40 + 145 + 96) = 2.81 GB
  100M files × (40 + 145 + 96) = 28.1 GB

Actual Arquivo.pt example:
  Filename: IAH-20170801155932-00143-p84.arquivo.pt.arc.gz (48 bytes)
  URL: http://p74.arquivo.pt:8080/browser/files/AWP24/... (140-150 bytes)

Arquivo.pt production estimate (2 branches):
  Branch A: ~1.7GB arclist → ~3-4 GB Redis memory
  Branch B: ~1.7GB arclist → ~3-4 GB Redis memory
  Total: ~6-8 GB Redis memory required
```

## Querying Redis

### Command-Line (redis-cli)

```bash
# Get path for specific file
redis-cli HGET "pathindex:branchA" "filename.warc.gz"

# Count total files
redis-cli HLEN "pathindex:branchA"

# Get all filenames
redis-cli HKEYS "pathindex:branchA"

# Get random samples
redis-cli HRANDFIELD "pathindex:branchA" 10

# Check if file exists
redis-cli HEXISTS "pathindex:branchA" "filename.warc.gz"

# Get multiple files at once
redis-cli HMGET "pathindex:branchA" "file1.warc.gz" "file2.warc.gz"
```

### Python Integration

```python
import redis

# Connect to Redis
r = redis.Redis(host='redis.arquivo.pt', port=6380, db=0)

# Get path for file
filename = "AWP-arquivo-20240101120000.warc.gz"
path = r.hget("pathindex:branchA", filename)

if path:
    print(f"File: {filename}")
    print(f"Path: {path.decode()}")
else:
    print(f"File not found: {filename}")

# Get multiple files
filenames = ["file1.warc.gz", "file2.warc.gz", "file3.warc.gz"]
paths = r.hmget("pathindex:branchA", filenames)

for filename, path in zip(filenames, paths):
    if path:
        print(f"{filename} → {path.decode()}")
```

### pywb Integration

Configure pywb to use Redis path indexes:

**config.yaml:**
```yaml
collections:
  arquivo:
    # Use Redis for path resolution
    archive_paths: redis://redis.arquivo.pt:6380/0/pathindex:branchA
    
    index:
      type: redis
      redis_url: redis://redis.arquivo.pt:6380/0
```

## Performance Tuning

### Connection Pooling

```bash
# Increase pool size for high throughput
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --pool-size 20 \
    --batch-size 1000
```

### Batch Size Optimization

| Dataset Size | Recommended Batch Size |
|--------------|------------------------|
| < 100K files | 500 (default) |
| 100K - 1M files | 1000 |
| > 1M files | 2000 |

### Network Optimization

```bash
# Use Unix socket when Redis is local
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --socket /var/run/redis/redis.sock
```

## Troubleshooting

### Problem: Redis connection timeout

**Symptoms:**
```
Error: Failed to connect to Redis: Connection timeout
```

**Solution:**
```bash
# Increase timeout
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --timeout 30
```

### Problem: Out of memory

**Symptoms:**
```
Error: OOM command not allowed when used memory > 'maxmemory'
```

**Solution:**
```bash
# Increase maxmemory in redis.conf
maxmemory 32gb

# Or enable eviction policy
maxmemory-policy allkeys-lru
```

### Problem: Slow imports

**Symptoms:** Processing takes too long

**Solution:**
```bash
# Optimize batch size and connection pool
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --batch-size 2000 \
    --pool-size 30
```

## Security Best Practices

### Authentication

```bash
# Use password authentication
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --password "$REDIS_PASSWORD"

# Use ACL (Redis 6+)
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --username indexer \
    --password "$REDIS_PASSWORD"
```

### SSL/TLS

```bash
# Enable SSL
arclist-index-to-redis \
    -d /data/arclists \
    -k pathindex:branchA \
    --host redis.arquivo.pt \
    --ssl
```

### Network Security

```ini
# redis.conf
bind 127.0.0.1 10.0.0.1  # Listen on specific IPs
protected-mode yes        # Enable protected mode
requirepass your_password # Set password
```

## Related Documentation

- **[arclist-index-to-redis](tools/arclist-index-to-redis.md)** - Complete pipeline tool
- **[arclist-to-path-index](tools/arclist-to-path-index.md)** - Convert arclist format
- **[path-index-to-redis](tools/path-index-to-redis.md)** - Submit to Redis
- **[Architecture](architecture.md)** - System design overview

## See Also

- [Redis Documentation](https://redis.io/documentation)
- [pywb Documentation](https://pywb.readthedocs.io/)
- [Arquivo.pt](https://arquivo.pt/)

---

**Related Commands:** `arclist-index-to-redis` • `arclist-to-path-index` • `path-index-to-redis`
