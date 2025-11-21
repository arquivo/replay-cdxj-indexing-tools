# addfield-to-flat-cdxj

Add custom JSON fields to flat CDXJ records. Unlike filters that remove data, this tool enriches records by adding metadata fields while preserving all original data.

## Overview

The `addfield-to-flat-cdxj` tool allows you to enrich CDXJ records with additional metadata. This is essential for:

- **Collection management** - Tag records with collection identifiers
- **Provenance tracking** - Add source, batch, or processing information
- **Temporal metadata** - Add indexing dates, capture years, etc.
- **Derived fields** - Compute and add fields based on existing data
- **External enrichment** - Add data from lookups or computations

**Key Advantage:** Designed to run in **parallel on individual files before merging** for maximum performance (10-15x speedup with 16 cores).

## Installation

The tool is included with the main package:

```bash
pip install replay-cdxj-indexing-tools
```

Or install from source:

```bash
git clone https://github.com/arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools
pip install -e .
```

## Quick Start

### Add Simple Fields

```bash
# Add collection identifier
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f collection=ARQUIVO-2024

# Add multiple fields
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \
    -f collection=ARQUIVO-2024 \
    -f source=web \
    -f indexed_date=20241120
```

### Pipeline Mode

```bash
# Read from stdin, write to stdout
cat input.cdxj | addfield-to-flat-cdxj -i - -f batch=daily > output.cdxj

# Integrate in pipeline
addfield-to-flat-cdxj -i input.cdxj -f year=2024 | \
    filter-blocklist -i - -b blocklist.txt | \
    flat-cdxj-to-zipnum -o indexes -i -
```

### Parallel Processing (Recommended)

```bash
# Process 100 files in parallel (16 cores)
parallel -j 16 \
    'addfield-to-flat-cdxj -i {} -o {.}.enriched.cdxj -f collection=ARQUIVO-2024' \
    ::: *.cdxj

# Then merge enriched files
merge-flat-cdxj - *.enriched.cdxj | flat-cdxj-to-zipnum -o indexes -i -
```

## Command-Line Interface

### Synopsis

```bash
addfield-to-flat-cdxj -i INPUT -o OUTPUT -f KEY=VALUE [-f KEY=VALUE ...]
addfield-to-flat-cdxj -i INPUT -o OUTPUT --function SCRIPT.py
```

### Options

| Option | Description |
|--------|-------------|
| `-i, --input PATH` | Input CDXJ file or `-` for stdin (required) |
| `-o, --output PATH` | Output file or `-` for stdout (default: stdout) |
| `-f, --field KEY=VALUE` | Add field (can be used multiple times) |
| `--function FILE` | Python file with addfield() function |
| `-v, --verbose` | Print statistics to stderr |
| `-h, --help` | Show help message |

**Note:** Must provide either `-f` (fields) **or** `--function` (not both).

## CDXJ Format

### Input
```
pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/", "status": "200"}
```

### Output (with added fields)
```
pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/", "status": "200", "collection": "ARQUIVO-2023", "source": "web"}
```

**Key Points:**
- Original fields are **always preserved**
- New fields are added to the JSON object
- Lines without JSON get a new JSON object created
- SURT key and timestamp remain unchanged
- Output uses compact JSON formatting

## Usage Examples

### 1. Add Collection Metadata

Tag all records with collection information:

```bash
addfield-to-flat-cdxj \
    -i collection-2024-11.cdxj \
    -o enriched.cdxj \
    -f collection=COLLECTION-2024-11 \
    -f source=arquivo \
    -f indexed_date=$(date +%Y%m%d)
```

**Result:**
```json
{
  "url": "...",
  "status": "200",
  "collection": "COLLECTION-2024-11",
  "source": "arquivo",
  "indexed_date": "20241121"
}
```

### 2. Dynamic Values from Shell

Use shell variables and command substitution:

```bash
# Environment variable
COLLECTION_NAME="ARQUIVO-2024"
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f collection=$COLLECTION_NAME

# Command substitution
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \
    -f indexed_date=$(date +%Y%m%d) \
    -f processed_by=$(hostname)
```

### 3. Custom Field Functions

For complex logic, create a Python function file:

**addfield_year.py:**
```python
def addfield(surt_key, timestamp, json_data):
    """Add year-based collection name."""
    year = timestamp[:4]
    month = timestamp[4:6]
    json_data['collection'] = f'ARQUIVO-{year}-{month}'
    json_data['year'] = year
    return json_data
```

**Usage:**
```bash
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj --function addfield_year.py
```

**Result:**
```json
{"url": "...", "collection": "ARQUIVO-2023-11", "year": "2023"}
{"url": "...", "collection": "ARQUIVO-2024-03", "year": "2024"}
```

### 4. Extract Domain from SURT

**addfield_domain.py:**
```python
def addfield(surt_key, timestamp, json_data):
    """Extract domain and TLD from SURT key."""
    # SURT format: pt,domain,subdomain)/path
    parts = surt_key.split(')', 1)[0].split(',')
    
    if len(parts) >= 2:
        json_data['tld'] = parts[0]
        json_data['domain'] = parts[1]
        if len(parts) >= 3:
            json_data['subdomain'] = parts[2]
    
    return json_data
```

**Result:**
```json
{"url": "...", "tld": "pt", "domain": "arquivo", "subdomain": "www"}
```

### 5. Conditional Field Addition

**addfield_quality.py:**
```python
def addfield(surt_key, timestamp, json_data):
    """Add quality score based on status code."""
    status = json_data.get('status', '')
    
    if status == '200':
        json_data['quality'] = 'good'
    elif status in ['301', '302']:
        json_data['quality'] = 'redirect'
    elif status.startswith('4') or status.startswith('5'):
        json_data['quality'] = 'error'
    else:
        json_data['quality'] = 'unknown'
    
    return json_data
```

## Integration in Processing Pipelines

### Position in Pipeline

**Best Practice:** Add fields **before merging** for parallel processing:

```bash
# ✅ RECOMMENDED: Parallel field addition before merge
parallel -j 16 \
    'addfield-to-flat-cdxj -i {} -o {.}.aug.cdxj -f collection=COL' \
    ::: *.cdxj
merge-flat-cdxj - *.aug.cdxj | flat-cdxj-to-zipnum -o indexes -i -

# ❌ NOT RECOMMENDED: Sequential after merge (slower)
merge-flat-cdxj - *.cdxj | \
    addfield-to-flat-cdxj -i - -f collection=COL | \
    flat-cdxj-to-zipnum -o indexes -i -
```

### Complete Arquivo.pt Pipeline

```bash
#!/bin/bash
set -e

COLLECTION="ARQUIVO-2024-11"
INDEXED_DATE=$(date +%Y%m%d)
PARALLEL_JOBS=16

# Stage 1: Index WARCs in parallel
echo "Stage 1: Indexing WARCs..."
parallel -j $PARALLEL_JOBS \
    'cdx-indexer --cdxj {} > {.}.cdxj' \
    ::: /data/warcs/*.warc.gz

# Stage 2: Add collection metadata in parallel
echo "Stage 2: Adding metadata..."
parallel -j $PARALLEL_JOBS \
    "addfield-to-flat-cdxj -i {} -o {.}.enriched.cdxj \
        -f collection=$COLLECTION \
        -f indexed_date=$INDEXED_DATE \
        -f source=arquivo" \
    ::: *.cdxj

# Stage 3: Filter in parallel (optional)
echo "Stage 3: Filtering blocklist..."
parallel -j $PARALLEL_JOBS \
    'filter-blocklist -i {} -o {.}.filtered.cdxj -b blocklist.txt' \
    ::: *.enriched.cdxj

# Stage 4: Merge and convert to ZipNum
echo "Stage 4: Merging and converting..."
merge-flat-cdxj - *.filtered.cdxj | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o /data/indexes -i - -n 3000 --compress

echo "Done!"
```

### Using cdxj-index-collection Script

The main pipeline script has built-in support:

```bash
# Add fields during collection processing
./cdxj-index-collection.sh COLLECTION-2024-11 \
    --addfield collection=ARQUIVO-2024 \
    --addfield source=web \
    --addfield indexed_date=20241121

# Or use custom function
./cdxj-index-collection.sh COLLECTION-2024-11 \
    --addfield-func /path/to/addfield_year.py
```

This automatically:
1. Indexes all WARCs in parallel
2. **Adds specified fields to each index file in parallel**
3. Filters blocklist (if enabled)
4. Merges all enriched files
5. Converts to ZipNum format

## Python API

### Basic Usage

```python
from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import addfield_to_cdxj

# Add simple fields
processed, skipped = addfield_to_cdxj(
    'input.cdxj',
    'output.cdxj',
    fields={'collection': 'ARQUIVO-2024', 'source': 'web'}
)

print(f"Processed {processed} lines")
```

### Using Custom Functions

```python
from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import (
    addfield_to_cdxj,
    load_addfield_function
)

# Load function from file
addfield_func = load_addfield_function('addfield_func.py')

# Apply it
processed, skipped = addfield_to_cdxj(
    'input.cdxj',
    'output.cdxj',
    addfield_func=addfield_func
)
```

### Inline Function

```python
def my_addfield(surt_key, timestamp, json_data):
    """Custom field logic."""
    year = timestamp[:4]
    json_data['year'] = year
    json_data['collection'] = f'COL-{year}'
    return json_data

# Use directly
processed, skipped = addfield_to_cdxj(
    'input.cdxj',
    'output.cdxj',
    addfield_func=my_addfield
)
```

## Performance

### Throughput

- **~500K-1M lines/second** on modern hardware
- I/O bound - disk speed is the limiting factor
- Uses 1MB read/write buffers for optimal performance

### Memory Usage

- **O(1)** - constant memory usage
- Processes line-by-line, no data accumulation
- ~10-20MB baseline memory footprint

### Parallel Processing Benefits

Processing 100 CDXJ files with 1M lines each:

| Approach | Time | Speedup |
|----------|------|---------|
| Sequential | ~200 seconds | 1x |
| Parallel (16 cores) | ~15 seconds | **13x faster** |

**Recommendation:** Always use parallel processing for large collections!

### Benchmark Results

Test: 100M lines CDXJ file (~20GB)

```bash
time addfield-to-flat-cdxj -i large.cdxj -o enriched.cdxj -f collection=TEST

# Results:
# real    0m25.341s   (~4M lines/sec)
# user    0m18.234s
# sys     0m7.012s
```

## Best Practices

### 1. Add Fields Early in Pipeline

✅ **Do:** Add fields before merging (enables parallelism)
```bash
parallel 'addfield-to-flat-cdxj -i {} -o {.}.aug -f col=X' ::: *.cdxj
merge-flat-cdxj - *.aug
```

❌ **Don't:** Add fields after merging (slower, sequential)
```bash
merge-flat-cdxj - *.cdxj | addfield-to-flat-cdxj -i - -f col=X
```

### 2. Use Simple Fields When Possible

Simple fields are faster and easier to maintain:

✅ **Good:** `addfield-to-flat-cdxj -i input.cdxj -f collection=ARQUIVO-2024`

❌ **Overkill:** Creating a function for static fields

### 3. Keep Functions Pure and Fast

Custom functions should be fast and have no side effects:

✅ **Good:**
```python
def addfield(surt_key, timestamp, json_data):
    year = timestamp[:4]  # Fast string slice
    json_data['year'] = year
    return json_data
```

❌ **Bad:**
```python
def addfield(surt_key, timestamp, json_data):
    # Don't do expensive operations or I/O
    response = requests.get('http://api.example.com/lookup')  # Slow!
    json_data['external'] = response.json()
    return json_data
```

### 4. Handle Missing Data Gracefully

```python
def addfield(surt_key, timestamp, json_data):
    # Check if field exists
    if 'url' in json_data:
        from urllib.parse import urlparse
        domain = urlparse(json_data['url']).netloc
        json_data['domain'] = domain
    
    # Use .get() with defaults
    status = json_data.get('status', 'unknown')
    json_data['status_category'] = categorize_status(status)
    
    return json_data
```

### 5. Use Descriptive Field Names

✅ **Good:** `collection=ARQUIVO-2024`, `indexed_date=20241120`, `source_type=web_crawl`

❌ **Bad:** `col=A24`, `d=20241120`, `x=web`

## Troubleshooting

### No fields or function provided

**Error:** `Error: Must provide either 'fields' or 'addfield_func'`

**Solution:** Use either `-f KEY=VALUE` or `--function FILE.py`

### Cannot provide both fields and function

**Error:** `Error: Cannot provide both 'fields' and 'addfield_func' (choose one)`

**Solution:** Use only one approach (fields OR function)

### Function file not found

**Error:** `Error: Cannot load module from /path/to/missing.py`

**Solution:** Check file path exists and is accessible

### Missing addfield() function

**Error:** `Error: Module must define an addfield(surt_key, timestamp, json_data) function`

**Solution:** Ensure your Python file has an `addfield()` function with correct signature

### Invalid field format

**Error:** `Error: Invalid field format 'myfield' (expected key=value)`

**Solution:** Use `key=value` format: `addfield-to-flat-cdxj -i input.cdxj -f myfield=myvalue`

## Related Documentation

- [filter-blocklist](filter-blocklist.md) - Filter CDXJ by blocklist patterns
- [filter-excessive-urls](filter-excessive-urls.md) - Remove excessive URLs
- [merge-flat-cdxj](merge-flat-cdxj.md) - Merge multiple CDXJ files
- [flat-cdxj-to-zipnum](flat-cdxj-to-zipnum.md) - Convert to ZipNum format
- [Reference Implementation](../reference-implementation.md) - Complete pipeline script
- [Pipeline Examples](../pipeline-examples.md) - Real-world workflows

## See Also

- [GitHub Repository](https://github.com/arquivo/replay-cdxj-indexing-tools)
- [Module README](../../replay_cdxj_indexing_tools/addfield/README.md) - Additional examples
- [Test Suite](../../tests/test_addfield_to_flat_cdxj.py) - Usage examples in tests
