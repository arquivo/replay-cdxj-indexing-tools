# Redis Submission Module

Submit pywb path index files to Redis databases for distributed web archive systems.

## Overview

This module provides tools for loading pywb path index files into Redis, enabling fast distributed lookups for web archive replay systems. It's designed for high-performance scenarios where index data needs to be accessed by multiple replay workers.

### Available Tools

- **[arclist-to-path-index](../../docs/tools/arclist-to-path-index.md)** - Convert Arquivo.pt arclist files to path index format
- **[path-index-to-redis](#command-line-usage)** - Load path index files into Redis database

## Features

- ✨ **Fast bulk loading** with Redis pipelining
- 🔄 **Multiple input formats** (files, stdin, gzipped)
- 🔐 **Full authentication support** (password, username/ACL)
- 🌐 **Redis Cluster support** for horizontal scaling
- 🏷️ **Namespace isolation** with key prefixes
- ⚡ **Optimized batching** for throughput
- 📊 **Progress tracking** with verbose mode
- 🛡️ **Dry-run validation** without writes

## Installation

The redis module requires the `redis` Python package:

```bash
pip install -e .
# or
pip install "replay-cdxj-indexing-tools[redis]"
```

## Command-Line Usage

### Basic Examples

```bash
# Submit an index file to local Redis
path-index-to-redis -i index.idx

# Submit to remote Redis with authentication
path-index-to-redis -i index.idx --host redis.example.com --password secret

# Submit with namespace prefix
path-index-to-redis -i index.idx --prefix "archive:2024-11:"

# Submit multiple files
path-index-to-redis -i index1.idx -i index2.idx -i index3.idx

# Read from stdin (pipeline mode)
cat index.idx | path-index-to-redis -i -
```

### Advanced Examples

```bash
# High-performance bulk load
path-index-to-redis -i large-index.idx \\
    --batch-size 1000 \\
    --pool-size 20 \\
    --verbose

# Redis Cluster deployment
path-index-to-redis -i index.idx \\
    --cluster \\
    --host redis-cluster.local \\
    --port 7000

# Clear existing keys before submission
path-index-to-redis -i index.idx \
    --prefix "test:" \
    --clear

# Dry run to validate index file
path-index-to-redis -i index.idx --dry-run --verbose

# Unix socket connection
path-index-to-redis -i index.idx \\
    --socket /var/run/redis/redis.sock

# SSL/TLS connection
path-index-to-redis -i index.idx \\
    --host redis.example.com \\
    --ssl \\
    --password secret
```

## Python API

### Basic Usage

```python
from replay_cdxj_indexing_tools.redis.submit_to_redis import submit_index_to_redis

# Submit index to Redis
submitted, errors = submit_index_to_redis(
    input_paths=['index.idx'],
    redis_host='localhost',
    redis_port=6379,
    redis_db=0,
    key_prefix='archive:',
    batch_size=500,
    verbose=True
)

print(f"Submitted {submitted} entries, {errors} errors")
```

### Advanced Usage

```python
from replay_cdxj_indexing_tools.redis.submit_to_redis import (
    submit_index_to_redis,
    parse_index_line,
    read_index_entries
)

# Parse individual lines
line = "pt,governo,www)/ 20230615120200\\tarquivo-01\\t186\\t193\\t1"
entry = parse_index_line(line)
print(entry)
# {'key': 'pt,governo,www)/ 20230615120200', 
#  'shard': 'arquivo-01', 
#  'offset': '186', 
#  'length': '193', 
#  'shard_num': '1'}

# Stream index entries
for entry in read_index_entries('index.idx', verbose=True):
    print(f"Processing {entry['key']}")

# Submit with authentication and custom settings
submitted, errors = submit_index_to_redis(
    input_paths=['index1.idx', 'index2.idx'],
    redis_host='redis.example.com',
    redis_port=6380,
    redis_password='secret',
    redis_username='indexer',
    key_prefix='archive:2024-11:',
    batch_size=1000,
    pool_size=20,
    timeout=30,
    clear_existing=False,
    verbose=True
)
```

## Redis Data Structure

### Storage Format

Index entries are stored as Redis Hashes:

```
Key: {prefix}idx:{surt_key}
Hash fields:
  - shard: shard filename
  - offset: byte offset in shard
  - length: byte length of compressed chunk
  - shard_num: shard sequence number
```

### Example

**Input .idx line:**
```
pt,governo,www)/ 20230615120200	arquivo-01	186	193	1
```

**Redis storage:**
```
Key: idx:pt,governo,www)/ 20230615120200
Hash:
  shard: "arquivo-01"
  offset: "186"
  length: "193"
  shard_num: "1"
```

### Benefits

- **Fast O(1) lookups** by SURT key
- **Memory efficient** (Redis Hash compression)
- **Easy namespace management** with prefixes
- **Atomic updates** with pipelining

## Integration Examples

### 1. Complete Pipeline Integration

Generate index and submit to Redis in one workflow:

```bash
# Process collection and submit to Redis
cdxj-index-collection AWP999

# Submit generated index
path-index-to-redis \
    -i /data/zipnum/AWP999/*.idx \
    --prefix "archive:2024-11:" \
    --verbose
```

### 2. Daily Incremental Updates

Automated daily processing script:

```bash
#!/bin/bash
set -e

DATE=$(date +%Y-%m-%d)
COLLECTION="COLLECTION-$DATE"

# Process new collection
cdxj-index-collection "$COLLECTION" --incremental

# Submit to Redis with dated prefix
path-index-to-redis \
    -i "/data/zipnum/$COLLECTION"/*.idx \
    --prefix "archive:$DATE:" \
    --host redis.archive.local \
    --password "$REDIS_PASSWORD" \
    --batch-size 1000 \
    --verbose

echo "Submitted $COLLECTION to Redis"
```

### 3. Multi-Collection Setup

Load multiple collections with different prefixes:

```bash
#!/bin/bash

for collection in 2024-01 2024-02 2024-03 2024-04; do
    echo "Processing $collection..."
    
    path-index-to-redis \
        -i "/data/indexes/$collection"/*.idx \
        --prefix "col:$collection:" \
        --host redis.archive.local \
        --batch-size 1000 \
        --verbose
        
    echo "Completed $collection"
done

echo "All collections loaded to Redis"
```

### 4. Docker Deployment

Use with Docker containers:

```bash
# Submit from Docker container
docker run -v /data:/data arquivo/replay-cdxj-indexing-tools \
    path-index-to-redis \
    -i /data/indexes/index.idx \
    --host redis \
    --prefix "archive:" \
    --verbose

# Docker Compose setup
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
  
  indexer:
    image: arquivo/replay-cdxj-indexing-tools
    depends_on:
      - redis
    volumes:
      - ./indexes:/indexes
    command: >
      path-index-to-redis
      -i /indexes/*.idx
      --host redis
      --prefix "archive:"
      --verbose

volumes:
  redis-data:
```

### 5. Redis Cluster Deployment

For high-availability setups:

```bash
# Submit to Redis Cluster
path-index-to-redis \
    -i index.idx \
    --cluster \
    --host redis-cluster.local \
    --port 7000 \
    --password "$CLUSTER_PASSWORD" \
    --batch-size 1000 \
    --verbose
```

## Querying Redis

### Command-Line Queries

After submission, query Redis directly:

```bash
# Get index entry for a specific URL
redis-cli HGETALL "idx:pt,governo,www)/ 20230615120200"
# Output:
# 1) "shard"
# 2) "arquivo-01"
# 3) "offset"
# 4) "186"
# 5) "length"
# 6) "193"
# 7) "shard_num"
# 8) "1"

# Count total index entries
redis-cli KEYS "idx:*" | wc -l

# Get entries for a domain (pattern matching)
redis-cli --scan --pattern "idx:pt,governo,*"

# Get entries with prefix
redis-cli --scan --pattern "archive:2024-11:idx:*"

# Check memory usage
redis-cli INFO memory

# Get Redis server info
redis-cli INFO server
```

### Python Queries

```python
import redis

# Connect to Redis
client = redis.Redis(host='localhost', port=6379, db=0)

# Get index entry
entry = client.hgetall('idx:pt,governo,www)/ 20230615120200')
print(entry)
# {b'shard': b'arquivo-01', b'offset': b'186', ...}

# Check if key exists
exists = client.exists('idx:pt,governo,www)/ 20230615120200')

# Count keys with prefix
cursor = 0
count = 0
while True:
    cursor, keys = client.scan(cursor, match='archive:2024-11:idx:*', count=1000)
    count += len(keys)
    if cursor == 0:
        break
print(f"Total keys: {count}")

# Get all hash fields
shard = client.hget('idx:pt,governo,www)/ 20230615120200', 'shard')
offset = client.hget('idx:pt,governo,www)/ 20230615120200', 'offset')
```

## Performance Tuning

### Batch Size

The `--batch-size` parameter controls how many entries are submitted in one Redis pipeline:

```bash
# Small files or low-latency networks
path-index-to-redis -i small.idx --batch-size 100

# Medium files (default, good balance)
path-index-to-redis -i medium.idx --batch-size 500

# Large files or high-latency networks
path-index-to-redis -i large.idx --batch-size 1000

# Very large files with fast network
path-index-to-redis -i huge.idx --batch-size 5000
```

**Guidelines:**
- Smaller batches: Lower memory, more frequent network I/O
- Larger batches: Higher memory, fewer network roundtrips
- Default 500 works well for most cases
- Increase for high-latency networks

### Connection Pooling

The `--pool-size` parameter controls Redis connection pool:

```bash
# Low concurrency
path-index-to-redis -i index.idx --pool-size 5

# Default (balanced)
path-index-to-redis -i index.idx --pool-size 10

# High concurrency
path-index-to-redis -i index.idx --pool-size 50
```

### Throughput Benchmarks

Typical performance on modern hardware:

| Batch Size | Network | Throughput | Use Case |
|------------|---------|------------|----------|
| 100 | LAN | 10K entries/sec | Small files |
| 500 | LAN | 30K entries/sec | Default |
| 1000 | LAN | 50K entries/sec | Large files |
| 5000 | LAN | 80K entries/sec | Bulk load |
| 1000 | WAN | 20K entries/sec | Remote Redis |

## Options Reference

### Required Arguments

| Option | Description |
|--------|-------------|
| `-i, --input FILE` | Input .idx file(s) or '-' for stdin (can specify multiple times) |

### Redis Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host HOST` | localhost | Redis server hostname |
| `--port PORT` | 6379 | Redis server port |
| `--db N` | 0 | Redis database number |
| `--password PASS` | None | Redis password |
| `--username USER` | None | Redis username (for ACL) |
| `--socket PATH` | None | Unix socket path (alternative to host:port) |
| `--ssl` | False | Use SSL/TLS connection |
| `--cluster` | False | Connect to Redis Cluster |

### Performance Options

| Option | Default | Description |
|--------|---------|-------------|
| `--batch-size N` | 500 | Entries per batch |
| `--pool-size N` | 10 | Connection pool size |
| `--timeout N` | 10 | Connection timeout (seconds) |

### Behavior Options

| Option | Default | Description |
|--------|---------|-------------|
| `--prefix PREFIX` | "" | Key prefix for namespacing |
| `--clear` | False | Clear existing keys before insert |
| `--dry-run` | False | Validate only, don't write |

### Output Options

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Print detailed progress and statistics |

## Troubleshooting

### Connection Errors

**Problem:** Cannot connect to Redis

**Solutions:**

1. Check Redis is running:
   ```bash
   redis-cli ping
   # Should return: PONG
   ```

2. Verify host and port:
   ```bash
   path-index-to-redis -i index.idx --host localhost --port 6379 --verbose
   ```

3. Check authentication:
   ```bash
   path-index-to-redis -i index.idx --password your-password
   ```

4. Test network connectivity:
   ```bash
   telnet redis-host 6379
   ```

### Memory Issues

**Problem:** Redis runs out of memory during load

**Solutions:**

1. Check available memory:
   ```bash
   redis-cli INFO memory
   ```

2. Use smaller batch sizes:
   ```bash
   path-index-to-redis -i index.idx --batch-size 100
   ```

3. Increase Redis max memory:
   ```bash
   # redis.conf
   maxmemory 10gb
   ```

### Performance Issues

**Problem:** Submission is slow

**Solutions:**

1. Increase batch size:
   ```bash
   path-index-to-redis -i index.idx --batch-size 1000
   ```

2. Increase connection pool:
   ```bash
   path-index-to-redis -i index.idx --pool-size 20
   ```

3. Use pipelining (automatic in tool)

4. Check network latency:
   ```bash
   redis-cli --latency
   ```

### Data Validation

**Problem:** Need to verify index was loaded correctly

**Solutions:**

1. Dry run first:
   ```bash
   path-index-to-redis -i index.idx --dry-run --verbose
   ```

2. Check sample entries:
   ```bash
   redis-cli HGETALL "idx:pt,governo,www)/ 20230615120200"
   ```

3. Count total keys:
   ```bash
   redis-cli DBSIZE
   ```

4. Use verbose mode:
   ```bash
   path-index-to-redis -i index.idx --verbose
   ```

## Use Cases

### 1. Distributed Replay System

Multiple pywb instances sharing a Redis index:

```
┌─────────────┐
│   pywb-1    │──┐
└─────────────┘  │
                 │    ┌──────────┐
┌─────────────┐  ├───▶│  Redis   │
│   pywb-2    │──┤    │  Cluster │
└─────────────┘  │    └──────────┘
                 │
┌─────────────┐  │
│   pywb-3    │──┘
└─────────────┘
```

### 2. Multi-Tenant Archives

Separate collections with prefixes:

```bash
# Tenant 1
path-index-to-redis -i tenant1.idx --prefix "tenant1:"

# Tenant 2
path-index-to-redis -i tenant2.idx --prefix "tenant2:"

# Tenant 3
path-index-to-redis -i tenant3.idx --prefix "tenant3:"
```

### 3. Real-Time Index Updates

Continuous index updates:

```bash
#!/bin/bash
# Watch for new index files and submit
inotifywait -m /data/indexes -e create -e moved_to |
    while read path action file; do
        if [[ "$file" == *.idx ]]; then
            echo "Processing $file..."
            path-index-to-redis -i "$path/$file" \
                --prefix "live:" \
                --verbose
        fi
    done
```

## Best Practices

### 1. Use Key Prefixes

Always use prefixes for namespace isolation:

```bash
path-index-to-redis -i index.idx --prefix "archive:2024-11:"
```

### 2. Test with Dry Run

Validate before actual submission:

```bash
path-index-to-redis -i index.idx --dry-run --verbose
```

### 3. Monitor Redis Memory

Keep an eye on memory usage:

```bash
redis-cli INFO memory | grep used_memory_human
```

### 4. Use Batch Processing

For large files, optimize batch size:

```bash
path-index-to-redis -i large.idx --batch-size 1000 --verbose
```

### 5. Set Appropriate Timeouts

For remote Redis, increase timeout:

```bash
path-index-to-redis -i index.idx \\
    --host remote-redis \\
    --timeout 30
```

### 6. Clear Old Data

Before reloading, clear existing keys:

```bash
path-index-to-redis -i new-index.idx \\
    --prefix "archive:" \\
    --clear
```

### 7. Use Redis Cluster for Scale

For large deployments:

```bash
path-index-to-redis -i index.idx \\
    --cluster \\
    --host redis-cluster.local
```

## Related Tools

- **[cdxj-to-zipnum](../zipnum/README.md)** - Generate .idx files for Redis
- **[merge-cdxj](../merge/README.md)** - Merge CDXJ before indexing
- **[filter-blocklist](../filter/README.md)** - Filter before Redis submission

## Authors

Arquivo.pt Team - [contacto@arquivo.pt](mailto:contacto@arquivo.pt)

## License

GPL-3.0 License - See [LICENSE](../../LICENSE) file
