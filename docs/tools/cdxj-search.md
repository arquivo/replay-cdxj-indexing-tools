# cdxj-search - Binary Search Tool for CDXJ/ZipNum Indexes

Performs efficient binary search on sorted CDXJ files and ZipNum compressed indexes with support for multiple match types, date filtering, and field-based queries.

## Command-Line Usage

### Basic Syntax

```bash
cdxj-search --url URL [OPTIONS] FILES...
cdxj-search --surt SURT [OPTIONS] FILES...
```

### Quick Examples

**Simple exact match:**
```bash
cdxj-search --url http://example.com/page index.cdxj
```

**Prefix match (all paths under URL):**
```bash
cdxj-search --url http://example.com/ --matchType prefix index.cdxj
```

**Host match (all paths for a host):**
```bash
cdxj-search --url http://example.com/any/path --matchType host index.cdxj
```

**Domain match (host + all subdomains):**
```bash
cdxj-search --url http://example.com --matchType domain index.cdxj
```

**Date range filtering:**
```bash
cdxj-search --url http://example.com/ --from 2020 --to 2021 index.cdxj
```

**Field filtering:**
```bash
cdxj-search --url http://example.com/ \
  --filter status=200 \
  --filter mime~text/.* \
  --limit 100 index.cdxj
```

**Search multiple files with glob:**
```bash
cdxj-search --url http://example.com /data/indexes/*.cdxj
```

**Search entire directory:**
```bash
cdxj-search --url http://example.com /data/indexes/
```

### Options

#### Required (one of):

- **--url URL**: URL to search for (will be converted to SURT)
- **--surt SURT**: SURT key to search for directly

#### Match Types:

- **--matchType TYPE**: Match type for search
  - `exact` (default): Exact SURT match
  - `prefix`: Match all entries with SURT prefix
  - `host`: Match all paths for the host
  - `domain`: Match host and all subdomains

#### Date Filtering:

- **--from TIMESTAMP**: Start timestamp (flexible format)
  - Year: `2020`
  - Month: `202001` or `2020-01`
  - Day: `20200101` or `2020-01-01`
  - Full: `20200101120000`
- **--to TIMESTAMP**: End timestamp (same formats as --from)

#### Field Filtering:

- **--filter EXPR**: Filter by field (can be repeated)
  - `field=value` - Exact match
  - `field!=value` - Not equal
  - `field~pattern` - Regex match
  - `field!~pattern` - Regex not match

Common fields: `status`, `mime`, `digest`, `length`

#### Output Options:

- **--limit N**: Limit number of results
- **--sort**: Sort results by timestamp within SURT key
- **--dedupe**: Remove duplicate entries (same SURT + timestamp)

#### Error Handling:

- **--skip-errors**: Skip corrupted/unreadable files (default: fail on error)

#### Debug:

- **--verbose, -v**: Enable verbose output to stderr
- **--progress**: Show progress indicator on stderr

Use `cdxj-search --help` for full help.

## Python API

### Basic Search

```python
from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file

# Exact match
results = search_cdxj_file(
    filepath='index.cdxj',
    search_key='com,example)/page',
    match_prefix=False,
    verbose=True
)

for line in results:
    print(line)
```

### Prefix Search

```python
from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file

# Prefix match - find all URLs under path
results = search_cdxj_file(
    filepath='index.cdxj',
    search_key='com,example)/',
    match_prefix=True
)

print(f"Found {len(results)} matching entries")
```

### URL to SURT Conversion

```python
import surt
from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file

# Convert URL to SURT
url = "http://example.com/page"
search_key = surt.surt(url)
print(f"SURT: {search_key}")  # com,example)/page

# Search
results = search_cdxj_file('index.cdxj', search_key, match_prefix=False)
```

### Match Type Application

```python
from replay_cdxj_indexing_tools.search.cdxj_search import apply_match_type

# Get search key for different match types
url_surt = "com,example)/path/page"

# Exact match
key, use_prefix = apply_match_type(url_surt, "exact")
# key: "com,example)/path/page", use_prefix: False

# Prefix match
key, use_prefix = apply_match_type(url_surt, "prefix")
# key: "com,example)/path/page", use_prefix: True

# Host match (all paths for host)
key, use_prefix = apply_match_type(url_surt, "host")
# key: "com,example)", use_prefix: True

# Domain match (host + subdomains)
key, use_prefix = apply_match_type(url_surt, "domain")
# key: "com,example)", use_prefix: True
```

### Date and Field Filtering

```python
from replay_cdxj_indexing_tools.search.filters import CDXJFilter

# Create filter
cdxj_filter = CDXJFilter(
    from_ts="2020",      # Flexible: year, month, or full timestamp
    to_ts="2021",
    filters=[
        "status=200",    # Exact match
        "mime~text/.*",  # Regex match
        "length!=0"      # Not equal
    ]
)

# Apply filter to results
results = search_cdxj_file('index.cdxj', 'com,example)/', match_prefix=True)
filtered = [line for line in results if cdxj_filter.matches(line)]

print(f"Filtered: {len(filtered)}/{len(results)} entries")
```

### ZipNum Search

```python
from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

# Search ZipNum format (.idx + .cdxj.gz)
results = search_zipnum_file(
    idx_filepath='index.idx',
    data_filepath='index.cdxj.gz',
    search_key='com,example)/',
    match_prefix=True,
    verbose=True
)
```

### File Discovery

```python
from replay_cdxj_indexing_tools.search.file_discovery import (
    discover_files,
    detect_file_type
)

# Discover files from patterns, paths, or directories
patterns = [
    '/data/indexes/collection1.cdxj',
    '/data/indexes/*.cdxj',
    '/data/archives/'
]

files = discover_files(patterns, verbose=True)
print(f"Found {len(files)} files")

# Detect file type
for filepath in files:
    file_type = detect_file_type(filepath)
    print(f"{filepath}: {file_type}")
    # Possible types: 'cdxj', 'zipnum_idx', 'zipnum_data'
```

### Complete Search Pipeline

```python
import surt
from replay_cdxj_indexing_tools.search.file_discovery import discover_files
from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file
from replay_cdxj_indexing_tools.search.filters import (
    CDXJFilter,
    sort_lines,
    deduplicate_lines
)

# 1. Discover files
files = discover_files(['/data/indexes/'], verbose=True)

# 2. Convert URL to SURT
url = "http://example.com/"
search_key = surt.surt(url)

# 3. Search all files
all_results = []
for filepath in files:
    results = search_cdxj_file(filepath, search_key, match_prefix=True)
    all_results.extend(results)

print(f"Found {len(all_results)} total results")

# 4. Filter results
cdxj_filter = CDXJFilter(
    from_ts="2020",
    to_ts="2021",
    filters=["status=200", "mime~text/.*"]
)
filtered = [line for line in all_results if cdxj_filter.matches(line)]

# 5. Sort and deduplicate
filtered = sort_lines(filtered)
filtered = deduplicate_lines(filtered)

# 6. Limit results
filtered = filtered[:100]

# 7. Output
for line in filtered:
    print(line)
```

## Algorithm Details

### Binary Search

The tool uses **binary search** for O(log n) lookup:

- **Time Complexity**: O(log N + M)
  - N = number of lines in file
  - M = number of matching results
- **Space Complexity**: O(M)
  - Only stores matching results in memory
- **I/O Efficiency**: Minimizes disk seeks

### Search Strategy

1. **Binary search** to find first potential match
2. **Backward scan** to find actual first occurrence
3. **Forward read** to collect all consecutive matches
4. **Early termination** when passing the search range

### SURT Key Format

CDXJ files use SURT (Sort-friendly URI Reordering Transform):

| URL | SURT |
|-----|------|
| `http://example.com/` | `com,example)/` |
| `http://www.example.com/page` | `com,example,www)/page` |
| `https://blog.example.com/post` | `com,example,blog)/post` |

SURT enables efficient domain/host matching through lexicographic sorting.

### Match Type Behavior

| Match Type | Input URL | Search Key | Matches |
|------------|-----------|------------|---------|
| **exact** | `http://example.com/page` | `com,example)/page` | Exact URL only |
| **prefix** | `http://example.com/dir/` | `com,example)/dir/` | All paths under `/dir/` |
| **host** | `http://example.com/any` | `com,example)` | All paths on `example.com` |
| **domain** | `http://example.com` | `com,example)` | `example.com` + subdomains |

### ZipNum Format Support

ZipNum format consists of two files:

- **index.idx**: Plaintext index with block offsets
- **index.cdxj.gz**: Gzipped CDXJ data in blocks

Search process:
1. Binary search the `.idx` file for relevant blocks
2. Decompress and search each matching block
3. Return matching entries

## Use Cases

### 1. URL Lookup

Find all captures of a specific URL:

```bash
cdxj-search --url http://example.com/page index.cdxj
```

### 2. Site Crawl Verification

Check what paths were captured for a domain:

```bash
cdxj-search --url http://example.com \
  --matchType domain \
  --from 2023 \
  --to 2024 \
  /data/indexes/
```

### 3. Content Type Analysis

Find all HTML captures:

```bash
cdxj-search --url http://example.com \
  --matchType host \
  --filter mime~text/html \
  --filter status=200 \
  index.cdxj
```

### 4. Temporal Analysis

Get captures from specific time period:

```bash
cdxj-search --url http://example.com/ \
  --matchType prefix \
  --from 20200101 \
  --to 20201231235959 \
  --sort \
  index.cdxj
```

### 5. Subdomain Discovery

Find all subdomains:

```bash
cdxj-search --url http://example.com \
  --matchType domain \
  index.cdxj | \
  cut -d' ' -f1 | \
  sort -u
```

### 6. Replay URL Generation

Generate playback URLs for pywb:

```bash
cdxj-search --url http://example.com/page \
  --from 2020 \
  --to 2021 \
  index.cdxj | \
  while read line; do
    surt=$(echo "$line" | cut -d' ' -f1)
    timestamp=$(echo "$line" | cut -d' ' -f2)
    echo "http://localhost:8080/$timestamp/$surt"
  done
```

### 7. Duplicate Detection

Find duplicate captures (same timestamp):

```bash
cdxj-search --url http://example.com/ \
  --matchType host \
  index.cdxj | \
  cut -d' ' -f1-2 | \
  sort | \
  uniq -d
```

### 8. Status Code Analysis

Count captures by status code:

```bash
cdxj-search --url http://example.com \
  --matchType host \
  index.cdxj | \
  grep -oP '"status":\s*"\K\d+' | \
  sort | \
  uniq -c | \
  sort -rn
```

## Performance

### Benchmark Results

**Binary Search Performance** (1GB CDXJ file, ~5M lines):

| Operation | Time | Notes |
|-----------|------|-------|
| Exact match | <0.1s | O(log n) lookups |
| Prefix match (100 results) | <0.2s | Binary search + linear read |
| Host match (1000 results) | <1s | Multiple blocks |
| Full file scan | ~10s | Linear search for comparison |

**Speedup**: Binary search is **50-100x faster** than linear scanning for typical queries.

### Performance Tips

#### 1. Use Exact/Prefix Over Host/Domain

More specific searches are faster:

```bash
# Fast: specific prefix
cdxj-search --url http://example.com/path/ --matchType prefix index.cdxj

# Slower: broad domain match
cdxj-search --url http://example.com --matchType domain index.cdxj
```

#### 2. Apply Filters Early

Use date filters to reduce result set:

```bash
cdxj-search --url http://example.com/ \
  --matchType prefix \
  --from 2023 \
  --to 2024 \
  index.cdxj
```

#### 3. Use ZipNum for Large Indexes

For multi-GB indexes, ZipNum is faster:

```bash
# Convert to ZipNum first
cdxj-to-zipnum -i large.cdxj -o indexes/large

# Then search
cdxj-search --url http://example.com indexes/large.idx
```

#### 4. Use --limit for Large Result Sets

Limit results if you only need a sample:

```bash
cdxj-search --url http://example.com/ \
  --matchType prefix \
  --limit 1000 \
  index.cdxj
```

#### 5. Process Files in Parallel

For multiple independent searches:

```bash
# Create search list
cat urls.txt | while read url; do
  echo "cdxj-search --url \"$url\" index.cdxj > results/$(echo $url | md5sum | cut -d' ' -f1).txt"
done > commands.txt

# Run in parallel
parallel -j 8 < commands.txt
```

#### 6. Use SSD Storage

Binary search involves random I/O:
- **HDD**: ~50-100 seeks/sec → slower
- **SSD**: ~10K+ IOPS → much faster

## Date Format Examples

The `--from` and `--to` options support flexible timestamp formats:

| Input | Normalized | Description |
|-------|------------|-------------|
| `2020` | `20200101000000` | Entire year |
| `202012` | `20201201000000` | Entire month |
| `20201225` | `20201225000000` | Entire day |
| `2020122512` | `20201225120000` | Specific hour |
| `202012251230` | `20201225123000` | Hour + minute |
| `20201225123045` | `20201225123045` | Full timestamp |

**Example queries:**

```bash
# All captures from 2020
cdxj-search --url http://example.com --from 2020 --to 2020 index.cdxj

# December 2020 only
cdxj-search --url http://example.com --from 202012 --to 202012 index.cdxj

# Specific day
cdxj-search --url http://example.com --from 20201225 --to 20201225 index.cdxj

# Date range
cdxj-search --url http://example.com --from 2020 --to 2021 index.cdxj
```

## Error Handling

### Unsorted Files

Binary search requires sorted input:

```bash
# Error: File not sorted
# Line 'com,zzz)/' found after 'com,aaa)/'
```

**Solution**: Sort the file first:
```bash
export LC_ALL=C
sort large.cdxj > large_sorted.cdxj
```

### Invalid SURT Format

If CDXJ file has invalid format:

```bash
# With --skip-errors: skip bad files
cdxj-search --url http://example.com --skip-errors /data/*.cdxj

# Without --skip-errors: fail on first error (default)
cdxj-search --url http://example.com /data/*.cdxj
```

### Missing ZipNum Pairs

For ZipNum files, both `.idx` and `.cdxj.gz` must exist:

```bash
# Error: Could not find data file for index: index.idx
```

**Solution**: Ensure both files exist:
```bash
ls -l index.idx index.cdxj.gz
```

### No Results Found

If search returns no results:

```bash
# Use --verbose to debug
cdxj-search --url http://example.com --verbose index.cdxj

# Check SURT conversion
python3 -c "import surt; print(surt.surt('http://example.com'))"

# Verify file contains the URL
grep "com,example)" index.cdxj | head
```

## Filter Expression Examples

### Status Code Filtering

```bash
# Only successful captures
cdxj-search --url http://example.com --filter status=200 index.cdxj

# Exclude 404s
cdxj-search --url http://example.com --filter status!=404 index.cdxj

# Find redirects
cdxj-search --url http://example.com --filter status~3.. index.cdxj
```

### MIME Type Filtering

```bash
# HTML only
cdxj-search --url http://example.com --filter mime~text/html index.cdxj

# All text types
cdxj-search --url http://example.com --filter mime~text/.* index.cdxj

# Exclude images
cdxj-search --url http://example.com --filter mime!~image/.* index.cdxj

# Specific type
cdxj-search --url http://example.com --filter mime=application/pdf index.cdxj
```

### Multiple Filters (AND logic)

```bash
# HTML AND status 200
cdxj-search --url http://example.com \
  --filter mime~text/html \
  --filter status=200 \
  index.cdxj

# Text AND not empty
cdxj-search --url http://example.com \
  --filter mime~text/.* \
  --filter length!=0 \
  index.cdxj
```

### Custom Field Filtering

```bash
# Filter by digest (content hash)
cdxj-search --url http://example.com \
  --filter digest=sha1:ABCDEF123456 \
  index.cdxj

# Filter by length
cdxj-search --url http://example.com \
  --filter length~[0-9]{7,} \
  index.cdxj  # 7+ digits (>1MB)
```

## Integration Examples

### With pywb CDX Server

Query local indexes before checking CDX server:

```bash
# Local search first
results=$(cdxj-search --url http://example.com/page index.cdxj)

if [ -z "$results" ]; then
  # Fallback to CDX server
  curl "http://cdx-server/search?url=http://example.com/page"
fi
```

### With Wayback Machine

Find URLs to check in Wayback:

```bash
cdxj-search --url http://example.com \
  --matchType host \
  index.cdxj | \
  cut -d' ' -f1 | \
  sort -u | \
  while read surt; do
    # Convert SURT back to URL (simplified)
    echo "https://web.archive.org/web/*/$surt"
  done
```

### With Data Analysis (Python)

```python
import json
import subprocess

# Run search
result = subprocess.run(
    ['cdxj-search', '--url', 'http://example.com', 
     '--matchType', 'host', 'index.cdxj'],
    capture_output=True,
    text=True
)

# Parse results
captures = []
for line in result.stdout.strip().split('\n'):
    if line:
        parts = line.split(' ', 2)
        if len(parts) == 3:
            surt, timestamp, json_str = parts
            data = json.loads(json_str)
            captures.append({
                'surt': surt,
                'timestamp': timestamp,
                **data
            })

# Analyze
print(f"Total captures: {len(captures)}")
print(f"Status codes: {set(c['status'] for c in captures)}")
print(f"MIME types: {set(c.get('mime', 'unknown') for c in captures)}")
```

## Testing

Run search tests:

```bash
# All search tests
pytest tests/test_cdxj_search.py -v

# Specific test class
pytest tests/test_cdxj_search.py::TestBinarySearch -v

# Test with verbose output
pytest tests/test_cdxj_search.py -v -s

# With coverage
pytest --cov=replay_cdxj_indexing_tools.search tests/test_cdxj_search.py
```

## See Also

- [merge-cdxj.md](merge-cdxj.md) - Merge multiple CDXJ files
- [cdxj-to-zipnum.md](cdxj-to-zipnum.md) - Convert to compressed format
- [usage.md](../usage.md) - General tool usage
- [pipeline-examples.md](../pipeline-examples.md) - Complete workflows
