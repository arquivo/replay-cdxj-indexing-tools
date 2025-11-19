# cdxj-to-zipnum - ZipNum Format Converter

Convert CDXJ files to ZipNum format - compressed, sharded indexes optimized for fast binary search in pywb web archive replay systems.

## Command-Line Usage

### Basic Syntax

```bash
cdxj-to-zipnum -o OUTPUT_DIR -i INPUT [-n LINES_PER_BLOCK] [--compress]
```

### Examples

**Basic conversion:**
```bash
cdxj-to-zipnum -o indexes -i arquivo.cdxj
```

**With custom shard size:**
```bash
cdxj-to-zipnum -o indexes -i arquivo.cdxj -n 5000
```

**With compression (recommended):**
```bash
cdxj-to-zipnum -o indexes -i arquivo.cdxj -n 3000 --compress
```

**Read from stdin (pipeline mode):**
```bash
cat arquivo.cdxj | cdxj-to-zipnum -o indexes -i -
```

**Complete pipeline:**
```bash
merge-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    cdxj-to-zipnum -o indexes -i - -n 3000 --compress
```

### Options

- `-o, --output OUTPUT_DIR` - Output directory for ZipNum files (required)
- `-i, --input INPUT` - Input CDXJ file or `-` for stdin (required)
- `-n, --lines-per-block N` - Lines per shard (default: 3000)
- `--compress` - Enable gzip compression (recommended)

## Python API

### Basic Conversion

```python
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum

# Convert CDXJ to ZipNum
cdxj_to_zipnum(
    input_path='arquivo.cdxj',
    output_dir='indexes',
    lines_per_block=3000,
    compress=True
)
```

### Convert from File Object

```python
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum

# Convert from file object
with open('arquivo.cdxj', 'r') as f:
    cdxj_to_zipnum(
        input_path=f,
        output_dir='indexes',
        lines_per_block=3000,
        compress=True
    )
```

### Stream from stdin

```python
import sys
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum

# Convert from stdin
cdxj_to_zipnum(
    input_path=sys.stdin,
    output_dir='indexes',
    lines_per_block=3000,
    compress=True
)
```

## ZipNum Format

### Output Structure

```
indexes/
├── index.cdxj.gz           # Summary index (compressed)
└── index.cdxj/             # Shard directory
    ├── part-00000.cdxj.gz  # First shard
    ├── part-00001.cdxj.gz  # Second shard
    ├── part-00002.cdxj.gz  # Third shard
    └── ...                 # More shards
```

### Summary Index

The `index.cdxj.gz` file contains one line per shard with:
- First SURT key in shard
- Shard filename
- Byte offset and length

Example:
```json
pt,governo,www)/ {"filename": "part-00000.cdxj.gz", "offset": 0, "length": 45231}
pt,sapo,www)/ {"filename": "part-00001.cdxj.gz", "offset": 0, "length": 48102}
```

### Shard Files

Each shard contains up to N CDXJ records (default: 3000):

```
# part-00000.cdxj.gz
pt,governo,www)/ 20230615120000 {"url": "...", "offset": "0", "length": "1234", ...}
pt,governo,www)/ 20230615120100 {"url": "...", "offset": "1234", "length": "2345", ...}
...
```

## Shard Size Selection

Choose `lines-per-block` based on your data:

| Lines/Shard | File Size | Use Case |
|-------------|-----------|----------|
| 1000 | Small (~50KB) | Small collections (<1M URLs) |
| **3000** | **Medium (~150KB)** | **Recommended default** |
| 5000 | Large (~250KB) | Large collections (>10M URLs) |
| 10000 | Very large (~500KB) | Very large collections (>100M URLs) |

**Trade-offs:**
- **Smaller shards** → More files, finer granularity, slightly slower
- **Larger shards** → Fewer files, coarser granularity, slightly faster

**Rule of thumb:**
- For N total lines, use (N / 3000) shards
- Aim for 1000-10000 shards total
- Each shard should be 50-500KB compressed

### Finding Optimal Shard Size

```bash
# Test different shard sizes
for n in 1000 3000 5000 10000; do
    echo "Testing $n lines per shard..."
    time cdxj-to-zipnum -o test_$n -i arquivo.cdxj -n $n --compress
    
    shards=$(ls test_$n/index.cdxj/ | wc -l)
    size=$(du -sh test_$n | cut -f1)
    
    echo "  Shards: $shards"
    echo "  Total size: $size"
    echo
done
```

## Use Cases

### 1. pywb Integration

ZipNum indexes work directly with pywb:

**pywb config.yaml:**
```yaml
collections:
  arquivo:
    index_paths:
      - /data/indexes/index.cdxj.gz
    archive_paths:
      - /data/warcs/
```

**Start pywb:**
```bash
pywb -p 8080
# Access: http://localhost:8080/arquivo/20230615120000/https://www.governo.pt/
```

### 2. Large-Scale Archiving

For huge archives (>100M URLs):

```bash
# Use larger shards
cdxj-to-zipnum -o indexes -i huge_archive.cdxj -n 10000 --compress

# Or process in stages
for file in /data/years/202?/*.cdxj; do
    year=$(basename $(dirname $file))
    cdxj-to-zipnum -o /data/zipnum/$year -i $file -n 5000 --compress
done
```

### 3. Incremental Updates

Create new ZipNum indexes for increments:

```bash
# Convert new captures
cdxj-to-zipnum -o indexes_new -i new_captures.cdxj -n 3000 --compress

# Configure pywb to use multiple indexes
# config.yaml:
#   index_paths:
#     - /data/indexes_old/index.cdxj.gz
#     - /data/indexes_new/index.cdxj.gz
```

### 4. Complete Processing Pipeline

From WARC to ZipNum:

```bash
#!/bin/bash

# 1. Index WARCs (parallel)
find /data/warcs -name "*.warc.gz" | \
    parallel -j8 "cdx-indexer --postappend --cdxj {} -o {}.cdxj"

# 2. Merge indexes
merge-cdxj merged.cdxj /data/warcs/*.cdxj

# 3. Filter unwanted content
filter-blocklist -i merged.cdxj -b blocklist.txt -o clean1.cdxj

# 4. Remove excessive URLs
filter-excessive-urls auto -i clean1.cdxj -o clean2.cdxj -n 1000

# 5. Convert to ZipNum
cdxj-to-zipnum -o /data/zipnum_final -i clean2.cdxj -n 3000 --compress

echo "Done! ZipNum indexes in /data/zipnum_final"
```

## Performance

**Benchmark Results:**

| Input Size | Lines | Shard Size | Compression | Time | Throughput |
|------------|-------|------------|-------------|------|------------|
| 500MB | 1M | 3000 | No | ~5s | ~200K lines/sec |
| 500MB | 1M | 3000 | Yes | ~10s | ~100K lines/sec |
| 5GB | 10M | 3000 | Yes | ~100s | ~100K lines/sec |
| 50GB | 100M | 5000 | Yes | ~1000s | ~100K lines/sec |

**Performance Tips:**

1. **Enable compression for production:**
   ```bash
   cdxj-to-zipnum -o indexes -i arquivo.cdxj --compress
   ```
   - Saves 70-80% disk space
   - pywb handles decompression automatically
   - Only 2x slower conversion

2. **Use appropriate shard size:**
   ```bash
   # For large files (>10M lines), use larger shards
   cdxj-to-zipnum -o indexes -i huge.cdxj -n 5000 --compress
   ```

3. **Process on SSD:**
   - SSD: ~100K lines/sec
   - HDD: ~50K lines/sec

4. **Pipeline mode saves disk:**
   ```bash
   # No intermediate files
   merge-cdxj - *.cdxj | ... | cdxj-to-zipnum -o indexes -i -
   ```

## Compression

### Without Compression

```bash
cdxj-to-zipnum -o indexes -i arquivo.cdxj
# Output: indexes/index.cdxj.gz and indexes/index.cdxj/part-*.cdxj
```

**Pros:**
- 2x faster conversion
- Simpler debugging (can read shards directly)

**Cons:**
- 3-5x more disk space
- Slower pywb lookups (more I/O)

### With Compression (Recommended)

```bash
cdxj-to-zipnum -o indexes -i arquivo.cdxj --compress
# Output: indexes/index.cdxj.gz and indexes/index.cdxj/part-*.cdxj.gz
```

**Pros:**
- 70-80% disk space savings
- Faster pywb lookups (less I/O)
- Industry standard

**Cons:**
- 2x slower conversion
- Requires decompression to debug

**Recommendation:** Always use `--compress` for production.

## Verification

### Verify Output

```bash
# Check summary index
zcat indexes/index.cdxj.gz | head -10

# Check first shard
zcat indexes/index.cdxj/part-00000.cdxj.gz | head -10

# Count total shards
ls indexes/index.cdxj/ | wc -l

# Check total size
du -sh indexes
```

### Verify with pywb

```bash
# Test lookup
wb-manager init test_collection
wb-manager add test_collection /data/warcs/*.warc.gz

# Add index
echo "  index_paths: ['/path/to/indexes/index.cdxj.gz']" >> collections/test_collection/config.yaml

# Start pywb
pywb -p 8080

# Test in browser
curl "http://localhost:8080/test_collection/20230615120000/https://www.governo.pt/"
```

## Troubleshooting

### Empty Summary Index

If `index.cdxj.gz` is empty:

```bash
# Check input file
head input.cdxj

# Verify format (should be sorted CDXJ)
# Each line: SURT TIMESTAMP JSON
```

### Shards Not Created

If `index.cdxj/` directory is empty:

```bash
# Check input is not empty
wc -l input.cdxj

# Try with verbose Python API
python -c "
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum
cdxj_to_zipnum('input.cdxj', 'indexes', lines_per_block=3000, compress=True)
"
```

### pywb Can't Find Index

If pywb doesn't use the index:

```yaml
# config.yaml - use absolute paths
collections:
  collection_name:
    index_paths:
      - /absolute/path/to/indexes/index.cdxj.gz
    archive_paths:
      - /absolute/path/to/warcs/
```

## Testing

Run ZipNum conversion tests:

```bash
# All ZipNum tests
pytest tests/test_cdxj_to_zipnum.py -v

# Specific test categories
pytest tests/test_cdxj_to_zipnum.py::TestCdxjToZipnum -v

# With coverage
pytest --cov=replay_cdxj_indexing_tools.zipnum tests/test_cdxj_to_zipnum.py
```

## See Also

- [merge-cdxj.md](merge-cdxj.md) - Previous step: merge files
- [filter-blocklist.md](filter-blocklist.md) - Previous step: filter blocklist
- [filter-excessive-urls.md](filter-excessive-urls.md) - Previous step: filter excessive
- [pipeline-examples.md](pipeline-examples.md) - Complete workflows
- [pywb documentation](https://pywb.readthedocs.io/) - Using ZipNum indexes
