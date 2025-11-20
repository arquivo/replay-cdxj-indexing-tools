# zipnum-to-flat-cdxj - ZipNum to Flat CDXJ Converter

Convert ZipNum format indexes back to flat CDXJ - decompress and merge sharded indexes into a single CDXJ stream. Enables round-trip conversion, data recovery, and pipeline reprocessing.

## Command-Line Usage

### Basic Syntax

```bash
zipnum-to-flat-cdxj -i INDEX_FILE [--base-dir DIR] [--loc FILE] [--workers N]
```

### Examples

**Basic conversion (output to stdout):**
```bash
zipnum-to-flat-cdxj -i indexes/index.idx > arquivo.cdxj
```

**Read index from stdin:**
```bash
cat indexes/index.idx | zipnum-to-flat-cdxj -i - > arquivo.cdxj
```

**Parallel decompression:**
```bash
zipnum-to-flat-cdxj -i indexes/index.idx --workers 8 > arquivo.cdxj
```

**Custom shard directory:**
```bash
zipnum-to-flat-cdxj -i /path/to/index.idx --base-dir /data/shards > arquivo.cdxj
```

**Custom location file:**
```bash
zipnum-to-flat-cdxj -i indexes/index.idx --loc indexes/custom.loc > arquivo.cdxj
```

**Compress output:**
```bash
zipnum-to-flat-cdxj -i indexes/index.idx | gzip -9 > arquivo.cdxj.gz
```

### Options

- `-i, --input FILE` - Input .idx file or `-` for stdin (required)
- `--base-dir DIR` - Base directory for shard files (default: idx file directory)
- `--loc FILE` - Custom .loc file path (default: auto-detect)
- `--workers N` - Parallel decompression workers (default: 4)

## Python API

### Basic Conversion

```python
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import zipnum_to_flat_cdxj

# Convert ZipNum to flat CDXJ (outputs to stdout)
zipnum_to_flat_cdxj(
    idx_path='indexes/index.idx',
    workers=4
)
```

### With Custom Configuration

```python
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import zipnum_to_flat_cdxj

# Custom base directory and location file
zipnum_to_flat_cdxj(
    idx_path='indexes/index.idx',
    base_dir='/data/shards',
    loc_file='indexes/custom.loc',
    workers=8
)
```

### Read from stdin

```python
import sys
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import zipnum_to_flat_cdxj

# Convert from stdin
zipnum_to_flat_cdxj(
    idx_path='-',
    workers=4
)
```

## ZipNum Format

### Expected Input Structure

ZipNum format consists of three components:

#### 1. Index File (.idx)

Tab-separated file pointing to compressed chunks:

```
com,example)/ 20230101000000	shard-01	0	45231	1
com,example)/ 20230102000000	shard-02	0	48102	2
com,example)/ 20230103000000	shard-03	0	52341	3
```

Format: `<key>\t<shard_name>\t<offset>\t<length>\t<shard_num>`

#### 2. Shard Files (.cdx.gz)

Gzip-compressed CDXJ data:

```
# Single shard
base.cdx.gz

# Multiple shards
base-01.cdx.gz
base-02.cdx.gz
base-03.cdx.gz
```

#### 3. Location File (.loc) - Optional

Maps shard names to file paths:

```
shard-01	/data/shards/base-01.cdx.gz
shard-02	/data/shards/base-02.cdx.gz
shard-03	/data/shards/base-03.cdx.gz
```

Format: `<shard_name>\t<filepath>`

## Use Cases

### 1. Data Recovery

Recover flat CDXJ from ZipNum archives:

```bash
# Convert back to flat format
zipnum-to-flat-cdxj -i old_indexes/index.idx > recovered.cdxj

# Verify recovery
wc -l recovered.cdxj
head -10 recovered.cdxj
```

### 2. Round-Trip Conversion

Convert between formats for different tools:

```bash
# Flat → ZipNum (for pywb)
cdxj-to-zipnum -o zipnum_indexes -i arquivo.cdxj --compress

# ZipNum → Flat (for reprocessing)
zipnum-to-flat-cdxj -i zipnum_indexes/index.idx > arquivo.cdxj

# Verify round-trip integrity
diff <(sort original.cdxj) <(sort arquivo.cdxj)
```

### 3. Pipeline Reprocessing

Reprocess archived data through new filters:

```bash
# Extract from ZipNum, apply new filters, rebuild
zipnum-to-flat-cdxj -i old_indexes/index.idx | \
    filter-blocklist -i - -b new_blocklist.txt | \
    filter-excessive-urls auto -i - -n 500 | \
    cdxj-to-zipnum -o new_indexes -i - --compress
```

### 4. Merge Multiple ZipNum Indexes

Combine separate ZipNum archives:

```bash
# Convert each to flat
zipnum-to-flat-cdxj -i archive1/index.idx > archive1.cdxj
zipnum-to-flat-cdxj -i archive2/index.idx > archive2.cdxj
zipnum-to-flat-cdxj -i archive3/index.idx > archive3.cdxj

# Merge and rebuild
merge-cdxj merged.cdxj archive1.cdxj archive2.cdxj archive3.cdxj
cdxj-to-zipnum -o combined_indexes -i merged.cdxj --compress
```

### 5. Extract Subsets

Extract specific URL patterns:

```bash
# Extract specific domain
zipnum-to-flat-cdxj -i indexes/index.idx | \
    grep "pt,governo,www)" > governo.cdxj

# Extract date range
zipnum-to-flat-cdxj -i indexes/index.idx | \
    awk '$2 >= 20230101000000 && $2 <= 20231231235959' > year2023.cdxj
```

### 6. Analyze Index Content

Perform analytics without accessing WARCs:

```bash
# Count captures per domain
zipnum-to-flat-cdxj -i indexes/index.idx | \
    awk '{print $1}' | \
    sed 's/).*/)/' | \
    sort | uniq -c | sort -rn | head -20

# Calculate total captures
zipnum-to-flat-cdxj -i indexes/index.idx | wc -l

# Find largest responses
zipnum-to-flat-cdxj -i indexes/index.idx | \
    python -c "
import sys
import json
for line in sys.stdin:
    parts = line.strip().split(' ', 2)
    if len(parts) == 3:
        data = json.loads(parts[2])
        print(data.get('length', 0), parts[0], parts[1])
" | sort -rn | head -20
```

## Performance

**Benchmark Results:**

| Input Size | Shards | Workers | Time | Throughput |
|------------|--------|---------|------|------------|
| 1GB (compressed) | 300 | 1 | ~30s | ~33 MB/s |
| 1GB (compressed) | 300 | 4 | ~12s | ~83 MB/s |
| 1GB (compressed) | 300 | 8 | ~8s | ~125 MB/s |
| 10GB (compressed) | 3000 | 4 | ~120s | ~83 MB/s |
| 10GB (compressed) | 3000 | 8 | ~80s | ~125 MB/s |

**Performance Tips:**

1. **Use parallel workers:**
   ```bash
   # Match CPU cores for best performance
   zipnum-to-flat-cdxj -i index.idx --workers 8 > output.cdxj
   ```
   - 1 worker: Baseline
   - 4 workers: ~2.5x faster
   - 8 workers: ~3.5x faster
   - 16+ workers: Diminishing returns (I/O bound)

2. **Compress output:**
   ```bash
   # Compress on-the-fly
   zipnum-to-flat-cdxj -i index.idx | gzip -9 > output.cdxj.gz
   
   # Or use pigz for parallel compression
   zipnum-to-flat-cdxj -i index.idx | pigz -p 4 > output.cdxj.gz
   ```

3. **Process on SSD:**
   - SSD: ~100-150 MB/s
   - HDD: ~30-50 MB/s
   - Network storage: ~20-40 MB/s

4. **Stream directly to next tool:**
   ```bash
   # Avoid intermediate files
   zipnum-to-flat-cdxj -i index.idx | filter-blocklist -i - -b list.txt > clean.cdxj
   ```

5. **Split processing for huge archives:**
   ```bash
   # Process shards in batches
   for shard in shard-{01..10}; do
       zipnum-to-flat-cdxj -i index.idx --workers 8 | \
           grep "^${shard}" >> output_${shard}.cdxj
   done
   ```

## Worker Configuration

### Choosing Worker Count

```bash
# Get CPU count
nproc

# Use 50-100% of cores
# Example: 8-core system
zipnum-to-flat-cdxj -i index.idx --workers 6  # 75% utilization
```

**Guidelines:**

| System | Recommended Workers |
|--------|---------------------|
| 2 cores | 1-2 |
| 4 cores | 2-3 |
| 8 cores | 4-6 |
| 16 cores | 8-12 |
| 32+ cores | 12-16 |

**Note:** Beyond 8-12 workers, performance gains are limited by I/O.

## Pipeline Examples

### Complete Reprocessing Pipeline

```bash
#!/bin/bash
# Reprocess ZipNum archive with updated filters

ORIGINAL_INDEX="indexes/index.idx"
NEW_BLOCKLIST="blocklist_2024.txt"
OUTPUT_DIR="indexes_clean"

# 1. Convert ZipNum to flat
echo "Converting ZipNum to flat CDXJ..."
zipnum-to-flat-cdxj -i $ORIGINAL_INDEX --workers 8 > temp_flat.cdxj

# 2. Apply new blocklist
echo "Applying blocklist..."
filter-blocklist -i temp_flat.cdxj -b $NEW_BLOCKLIST -o temp_filtered.cdxj

# 3. Remove excessive URLs
echo "Filtering excessive URLs..."
filter-excessive-urls auto -i temp_filtered.cdxj -o temp_clean.cdxj -n 1000

# 4. Rebuild ZipNum
echo "Building new ZipNum indexes..."
cdxj-to-zipnum -o $OUTPUT_DIR -i temp_clean.cdxj -n 3000 --compress

# 5. Cleanup
rm temp_flat.cdxj temp_filtered.cdxj temp_clean.cdxj

echo "Done! Clean indexes in $OUTPUT_DIR"
```

### Incremental Merge

```bash
#!/bin/bash
# Merge new captures with existing ZipNum archive

OLD_INDEX="archive_2023/index.idx"
NEW_CDXJ="captures_2024.cdxj"
OUTPUT_DIR="archive_2024"

# 1. Convert old archive to flat
echo "Converting old archive..."
zipnum-to-flat-cdxj -i $OLD_INDEX --workers 8 > old_flat.cdxj

# 2. Merge old and new
echo "Merging indexes..."
merge-cdxj merged.cdxj old_flat.cdxj $NEW_CDXJ

# 3. Build new ZipNum
echo "Building merged ZipNum..."
cdxj-to-zipnum -o $OUTPUT_DIR -i merged.cdxj -n 3000 --compress

# 4. Cleanup
rm old_flat.cdxj merged.cdxj

echo "Done! Merged archive in $OUTPUT_DIR"
```

### Extract and Analyze

```bash
#!/bin/bash
# Extract domain statistics from ZipNum archive

INDEX_FILE="indexes/index.idx"

echo "Extracting domain statistics..."

zipnum-to-flat-cdxj -i $INDEX_FILE --workers 8 | \
    awk '{print $1}' | \
    sed 's/)[^)]*$/)/; s/).*/)/' | \
    sort | uniq -c | sort -rn | \
    awk '{print $1 "\t" $2}' > domain_stats.txt

echo "Top 20 domains:"
head -20 domain_stats.txt

echo "Total domains: $(wc -l < domain_stats.txt)"
echo "Total captures: $(awk '{sum+=$1} END {print sum}' domain_stats.txt)"
```

## Verification

### Verify Conversion

```bash
# Convert and check
zipnum-to-flat-cdxj -i indexes/index.idx > output.cdxj

# Check line count
wc -l output.cdxj

# Verify CDXJ format
head -10 output.cdxj

# Check sorting
sort -c output.cdxj && echo "✓ Properly sorted" || echo "✗ Not sorted"
```

### Compare Round-Trip

```bash
# Original → ZipNum → Flat
cdxj-to-zipnum -o temp_zipnum -i original.cdxj --compress
zipnum-to-flat-cdxj -i temp_zipnum/index.idx > restored.cdxj

# Compare (should be identical after sorting)
diff <(sort original.cdxj) <(sort restored.cdxj)

# Cleanup
rm -rf temp_zipnum restored.cdxj
```

### Validate Output

```bash
# Check JSON validity
zipnum-to-flat-cdxj -i indexes/index.idx | \
    python -c "
import sys
import json
for i, line in enumerate(sys.stdin, 1):
    parts = line.strip().split(' ', 2)
    if len(parts) != 3:
        print(f'Line {i}: Invalid format')
        continue
    try:
        json.loads(parts[2])
    except json.JSONDecodeError as e:
        print(f'Line {i}: Invalid JSON - {e}')
" | head -20
```

## Troubleshooting

### Missing Shard Files

If you get "Shard file not found" warnings:

```bash
# Check shard file locations
ls indexes/*.cdx.gz

# Or check in subdirectory
ls indexes/shards/*.cdx.gz

# Use --base-dir to specify location
zipnum-to-flat-cdxj -i indexes/index.idx --base-dir indexes/shards > output.cdxj
```

### No Output Produced

If output is empty:

```bash
# Check idx file content
head indexes/index.idx

# Verify shard files exist
cat indexes/index.idx | awk '{print $2}' | \
    while read shard; do
        ls indexes/$shard.cdx.gz 2>/dev/null || echo "Missing: $shard"
    done
```

### Incorrect Base Directory

If .loc file references aren't resolving:

```bash
# Check .loc file
cat indexes/index.loc

# Use explicit base directory
zipnum-to-flat-cdxj -i indexes/index.idx --base-dir /absolute/path/to/shards

# Or use custom .loc file
zipnum-to-flat-cdxj -i indexes/index.idx --loc /path/to/custom.loc
```

### Slow Performance

If conversion is slow:

```bash
# Increase workers
zipnum-to-flat-cdxj -i indexes/index.idx --workers 8 > output.cdxj

# Check I/O wait
iostat -x 1

# Check if files are on slow storage
df -h indexes/

# Consider processing on SSD
cp -r indexes /tmp/
zipnum-to-flat-cdxj -i /tmp/indexes/index.idx --workers 8 > output.cdxj
```

### Out of Memory

If you run out of memory (unlikely with streaming):

```bash
# Process in chunks (by shard prefix)
for prefix in {a..z} {0..9}; do
    zipnum-to-flat-cdxj -i index.idx --workers 2 | \
        grep "^$prefix" >> output_$prefix.cdxj
done

# Then merge
cat output_*.cdxj | sort > final.cdxj
rm output_*.cdxj
```

## Testing

Run conversion tests:

```bash
# All tests for zipnum-to-flat-cdxj
pytest tests/test_zipnum_to_flat_cdxj.py -v

# Specific test categories
pytest tests/test_zipnum_to_flat_cdxj.py::TestZipnumToFlatCdxj -v
pytest tests/test_zipnum_to_flat_cdxj.py::TestCommandLine -v

# With coverage
pytest --cov=replay_cdxj_indexing_tools.zipnum tests/test_zipnum_to_flat_cdxj.py
```

## Limitations

1. **Sequential Shard Processing**
   - Shards are processed in order to maintain sort
   - Can't parallelize across shards, only within shard decompression

2. **Memory Usage**
   - Minimal memory usage (streaming output)
   - Each worker thread holds one shard in memory during decompression

3. **Output Format**
   - Outputs only to stdout (use shell redirection for files)
   - No option to write directly to file (by design - Unix philosophy)

4. **Sort Order**
   - Maintains original sort order from ZipNum
   - If original wasn't sorted, output won't be either

## See Also

- [cdxj-to-zipnum.md](cdxj-to-zipnum.md) - Reverse operation: flat to ZipNum
- [merge-cdxj.md](merge-cdxj.md) - Merge multiple CDXJ files
- [filter-blocklist.md](filter-blocklist.md) - Filter content
- [filter-excessive-urls.md](filter-excessive-urls.md) - Remove crawler traps
- [pipeline-examples.md](../pipeline-examples.md) - Complete workflows
- [pywb documentation](https://pywb.readthedocs.io/) - ZipNum format specification
