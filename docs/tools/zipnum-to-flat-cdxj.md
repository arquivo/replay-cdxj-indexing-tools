# zipnum-to-flat-cdxj - ZipNum to Flat CDXJ Converter

Convert ZipNum format indexes back to flat CDXJ - decompress and merge sharded indexes into a single flat CDXJ stream. Enables round-trip conversion, data recovery, and pipeline reprocessing.

## Command-Line Usage

### Basic Syntax

```bash
zipnum-to-flat-cdxj -i INDEX_FILE [--base-dir DIR] [--loc FILE]
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
- `--workers N` - Deprecated and ignored (decompression is streamed sequentially); prints a warning if given

## Python API

### Basic Conversion

```python
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import zipnum_to_flat_cdxj

# Convert ZipNum to flat CDXJ (outputs to stdout)
zipnum_to_flat_cdxj(
    idx_path='indexes/index.idx',
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
)
```

### Read from stdin

```python
import sys
from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import zipnum_to_flat_cdxj

# Convert from stdin
zipnum_to_flat_cdxj(
    idx_path='-',
)
```

## ZipNum Format

### Expected Input Structure

ZipNum format consists of three components:

#### 1. Index File (.idx)

Tab-separated file pointing to compressed chunks:

```
com,example)/ 20230101000000	shard-01.cdx.gz	0	45231	1
com,example)/ 20230102000000	shard-02.cdx.gz	0	48102	2
com,example)/ 20230103000000	shard-03.cdx.gz	0	52341	3
```

Format: `<key>\t<shard_name.cdx.gz>\t<offset>\t<length>\t<lineno>`

`<lineno>` is the 1-based line number of the entry in the `.idx` (not the shard number).

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
base-01.cdx.gz	base-01.cdx.gz
base-02.cdx.gz	base-02.cdx.gz
base-03.cdx.gz	base-03.cdx.gz
```

Format: `<shard_name.cdx.gz>\t<filepath>`

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
flat-cdxj-to-zipnum -o zipnum_indexes -i arquivo.cdxj --compress

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
    flat-cdxj-to-zipnum -o new_indexes -i - --compress
```

### 4. Merge Multiple ZipNum Indexes

Combine separate ZipNum archives:

```bash
# Convert each to flat
zipnum-to-flat-cdxj -i archive1/index.idx > archive1.cdxj
zipnum-to-flat-cdxj -i archive2/index.idx > archive2.cdxj
zipnum-to-flat-cdxj -i archive3/index.idx > archive3.cdxj

# Merge and rebuild
merge-flat-cdxj merged.cdxj archive1.cdxj archive2.cdxj archive3.cdxj
flat-cdxj-to-zipnum -o combined_indexes -i merged.cdxj --compress
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

Shards are decompressed one at a time and streamed to stdout in 256 KiB chunks,
so memory use does not grow with shard size. Throughput is bound by
single-threaded gzip decompression and disk I/O. The `--workers` option is
deprecated and has no effect.

**Performance Tips:**

1. **Parallelize across indexes, not within one:**
   ```bash
   # Run independent conversions concurrently (one process per index)
   zipnum-to-flat-cdxj -i a.idx > a.cdxj &
   zipnum-to-flat-cdxj -i b.idx > b.cdxj &
   wait
   ```

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
       zipnum-to-flat-cdxj -i index.idx | \
           grep "^${shard}" >> output_${shard}.cdxj
   done
   ```

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
zipnum-to-flat-cdxj -i $ORIGINAL_INDEX > temp_flat.cdxj

# 2. Apply new blocklist
echo "Applying blocklist..."
filter-blocklist -i temp_flat.cdxj -b $NEW_BLOCKLIST -o temp_filtered.cdxj

# 3. Remove excessive URLs
echo "Filtering excessive URLs..."
filter-excessive-urls auto -i temp_filtered.cdxj -o temp_clean.cdxj -n 1000

# 4. Rebuild ZipNum
echo "Building new ZipNum indexes..."
flat-cdxj-to-zipnum -o $OUTPUT_DIR -i temp_clean.cdxj -n 3000 --compress

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
zipnum-to-flat-cdxj -i $OLD_INDEX > old_flat.cdxj

# 2. Merge old and new
echo "Merging indexes..."
merge-flat-cdxj merged.cdxj old_flat.cdxj $NEW_CDXJ

# 3. Build new ZipNum
echo "Building merged ZipNum..."
flat-cdxj-to-zipnum -o $OUTPUT_DIR -i merged.cdxj -n 3000 --compress

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

zipnum-to-flat-cdxj -i $INDEX_FILE | \
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
flat-cdxj-to-zipnum -o temp_zipnum -i original.cdxj --compress
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

`.loc` entries must resolve inside `--base-dir` (default: the directory of the
.idx file); entries outside it are rejected. `--base-dir /` accepts absolute
paths anywhere.

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
# Check I/O wait
iostat -x 1

# Check if files are on slow storage
df -h indexes/

# Consider processing on SSD
cp -r indexes /tmp/
zipnum-to-flat-cdxj -i /tmp/indexes/index.idx > output.cdxj
```

### Out of Memory

Memory use does not grow with shard size because shards are streamed. If the
downstream pipeline runs out of memory:

```bash
# Process in chunks (by shard prefix)
for prefix in {a..z} {0..9}; do
    zipnum-to-flat-cdxj -i index.idx | \
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
   - Decompression is single-threaded; `--workers` is ignored

2. **Memory Usage**
   - Memory does not grow with shard size: each shard is streamed in 256 KiB
     chunks and never loaded whole into memory (the .idx entries are still
     held in memory)

3. **Output Format**
   - Outputs only to stdout (use shell redirection for files)
   - No option to write directly to file (by design - Unix philosophy)
   - A corrupt or truncated shard makes the tool exit non-zero, but the data
     already decompressed from that shard (and all earlier shards) has been
     written to stdout. In pipelines use `set -o pipefail` or check the exit
     status before trusting the output

4. **Sort Order**
   - Maintains original sort order from ZipNum
   - If original wasn't sorted, output won't be either

## See Also

- [flat-cdxj-to-zipnum.md](flat-cdxj-to-zipnum.md) - Reverse operation: flat to ZipNum
- [merge-flat-cdxj.md](merge-flat-cdxj.md) - Merge multiple CDXJ files
- [filter-blocklist.md](filter-blocklist.md) - Filter content
- [filter-excessive-urls.md](filter-excessive-urls.md) - Remove crawler traps
- [pipeline-examples.md](../pipeline-examples.md) - Complete workflows
- [pywb documentation](https://pywb.readthedocs.io/) - ZipNum format specification
