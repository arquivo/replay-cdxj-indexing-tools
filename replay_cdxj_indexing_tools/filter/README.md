# CDXJ Utility Tools

This module contains utility tools for processing and cleaning CDXJ web archive indexes.

## Filter Excessive URLs

The `filter-excessive-urls` tool helps identify and remove URLs that appear excessively in CDXJ files. This is particularly useful for:

- **Removing crawler traps** - URLs that cause infinite loops in crawlers
- **Filtering spam sites** - Sites that appear abnormally frequently
- **Cleaning indexes** - Removing overly-represented domains before ZipNum conversion
- **Data quality** - Ensuring balanced representation in web archive indexes

### Common Use Cases

#### 1. Finding Crawler Traps

Identify URLs that appear more than 1000 times:

```bash
filter-excessive-urls find -i arquivo.cdxj -n 1000 > traps.txt
```

This produces a sorted list (by count descending) of excessive URLs:

```
pt,spam,www)/loop 5000
pt,trap,example)/infinite 3500
# Found 2 URLs with > 1000 occurrences
```

#### 2. Two-Pass Filtering (Pipeline-Safe)

For use in pipelines with stdin/stdout:

```bash
# Pass 1: Find excessive URLs
filter-excessive-urls find -i merged.cdxj -n 1000 > excessive.txt

# Pass 2: Filter them out using stdin
cat merged.cdxj | filter-excessive-urls remove -i - -b excessive.txt > cleaned.cdxj
```

#### 3. One-Pass Auto Mode (File Input Only)

For simpler usage when reading from files (not stdin):

```bash
filter-excessive-urls auto -i arquivo.cdxj -o cleaned.cdxj -n 1000 -v
```

Output with verbose mode:
```
Pass 1: Finding URLs with > 1000 occurrences...
Found 3 excessive URLs
Top 5 excessive URLs:
  pt,spam,www)/loop: 5000 occurrences
  pt,trap,example)/: 3500 occurrences
  pt,ads,site)/popup: 2000 occurrences
Pass 2: Filtering excessive URLs...
Complete: Kept 450000 lines, filtered 10500 lines (2.3%)
```

### Integration in Processing Pipeline

The tool is designed to fit between merge and ZipNum conversion:

```bash
# Complete Arquivo.pt processing pipeline
merge-cdxj - file1.cdxj file2.cdxj file3.cdxj | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    cdxj-to-zipnum -o indexes -i - -n 3000 --compress
```

### Command-Line Interface

#### `find` Command

Find URLs exceeding a threshold:

```bash
filter-excessive-urls find -i INPUT -n THRESHOLD > output.txt
```

**Options:**
- `-i, --input`: Input CDXJ file or `-` for stdin
- `-n, --threshold`: Minimum count to be considered excessive (default: 1000)

**Output Format:**
```
<surt_key> <count>
<surt_key> <count>
...
# Found N URLs with > THRESHOLD occurrences
```

#### `remove` Command

Filter out URLs from a blacklist:

```bash
filter-excessive-urls remove -i INPUT -b BLACKLIST [-o OUTPUT]
```

**Options:**
- `-i, --input`: Input CDXJ file or `-` for stdin
- `-b, --blacklist`: File with excessive URLs (one per line, optionally with counts)
- `-o, --output`: Output file or `-` for stdout (default: stdout)

**Blacklist Format:**

Can be the output from `find` command or a simple list:
```
pt,spam,www)/loop 5000
pt,trap,example)/ 3500
```

Or just SURT keys:
```
pt,spam,www)/loop
pt,trap,example)/
```

#### `auto` Command

Find and filter in one pass (requires file, not stdin):

```bash
filter-excessive-urls auto -i INPUT [-o OUTPUT] -n THRESHOLD [-v]
```

**Options:**
- `-i, --input`: Input CDXJ file (cannot be stdin)
- `-o, --output`: Output file or `-` for stdout (default: stdout)
- `-n, --threshold`: Count threshold (default: 1000)
- `-v, --verbose`: Print progress and statistics to stderr

**Note:** Auto mode requires reading the file twice, so stdin is not supported. Use the two-pass approach for pipeline processing.

### Python API

```python
from replay_cdxj_indexing_tools.utils.filter_excessive_urls import (
    find_excessive_urls,
    filter_excessive_urls,
    process_pipeline,
)

# Find excessive URLs
excessive = find_excessive_urls('arquivo.cdxj', threshold=1000)
print(f"Found {len(excessive)} excessive URLs")

# Most excessive ones
for surt, count in sorted(excessive.items(), key=lambda x: -x[1])[:5]:
    print(f"  {surt}: {count} occurrences")

# Filter them out
excessive_set = set(excessive.keys())
kept, filtered = filter_excessive_urls(
    'arquivo.cdxj',
    excessive_set,
    'cleaned.cdxj'
)
print(f"Kept {kept} lines, filtered {filtered} lines")

# Or use one-pass mode
excessive_count, kept, filtered = process_pipeline(
    'arquivo.cdxj',
    'cleaned.cdxj',
    threshold=1000,
    verbose=True
)
```

### Performance Characteristics

**Memory Usage:**
- `find` command: O(unique URLs) - stores counts for all unique SURT keys
- `remove` command: O(blacklist size) - only stores blacklisted URLs
- For 10M lines with 100K unique URLs: ~200MB memory usage

**Processing Speed:**
- ~500K-1M lines/second on modern hardware
- I/O bound - disk speed is the limiting factor
- Uses 1MB read/write buffers for optimal throughput

**Typical Arquivo.pt Performance:**
- 100M line CDXJ file (~20GB)
- `find`: ~3-5 minutes
- `remove`: ~3-5 minutes
- Total: ~6-10 minutes for complete filtering

### Recommended Thresholds

Based on Arquivo.pt experience:

| Threshold | Use Case |
|-----------|----------|
| 100 | Very aggressive filtering, small crawls |
| 500 | Aggressive, removes obvious traps |
| 1000 | **Recommended default** - balances quality and preservation |
| 5000 | Conservative, only removes extreme cases |
| 10000 | Very conservative, crawler trap removal only |

**Rule of thumb:** For a crawl with N total URLs, use threshold of ~N/1000 to N/10000.

### Troubleshooting

#### Auto mode fails with stdin

**Problem:**
```bash
cat arquivo.cdxj | filter-excessive-urls auto -i - -n 1000
Error: Auto mode requires a file (stdin not supported). Use two-pass approach.
```

**Solution:** Use the two-pass approach:
```bash
# First, save to file or use find/remove separately
cat arquivo.cdxj > /tmp/arquivo.cdxj
filter-excessive-urls auto -i /tmp/arquivo.cdxj -n 1000

# Or use two-pass mode with stdin
filter-excessive-urls find -i arquivo.cdxj -n 1000 > excessive.txt
cat arquivo.cdxj | filter-excessive-urls remove -i - -b excessive.txt
```

#### Memory usage too high

**Problem:** System runs out of memory during `find` phase.

**Solution:** The tool requires memory proportional to the number of unique SURT keys. For very large crawls with many unique URLs:

1. Increase system swap space
2. Use a machine with more RAM
3. Pre-filter by domain if possible
4. Process in smaller batches

#### Filtered too many lines

**Problem:** Tool filtered more URLs than expected.

**Solution:** 
1. Check the threshold - may be too low
2. Review the excessive.txt file to see what was flagged
3. Adjust threshold upward
4. Consider if the filtering is actually correct (crawler traps can be common)

### Migration from Legacy Scripts

If you're migrating from the legacy bash scripts (`find-excessive-urls.sh` and `filter-excessive-urls.sh`):

**Old approach:**
```bash
./find-excessive-urls.sh arquivo.cdxj 1000 > excessive.txt
./filter-excessive-urls.sh arquivo.cdxj excessive.txt > cleaned.cdxj
```

**New approach:**
```bash
filter-excessive-urls find -i arquivo.cdxj -n 1000 > excessive.txt
filter-excessive-urls remove -i arquivo.cdxj -b excessive.txt > cleaned.cdxj
```

**Benefits of Python version:**
- **Much faster** - 10-50x faster than bash/awk processing
- **Better memory efficiency** - Uses optimized data structures
- **Pipeline compatible** - Works with stdin/stdout
- **Better error handling** - Clear error messages and validation
- **Cross-platform** - Works on Windows, Linux, macOS
- **Integrated** - Part of the same package as merge and zipnum tools

See `scripts/README.md` for more details on legacy scripts.

## Other Utilities

(Future utilities will be documented here)
