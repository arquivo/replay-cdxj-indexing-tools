# filter-excessive-urls - Excessive URL Filter

Find and remove URLs that appear excessively in CDXJ files. This helps eliminate crawler traps, spam sites, and overly-represented URLs from web archive indexes.

## Command-Line Usage

The tool has three modes: **find**, **remove**, and **auto**.

### 1. Find Mode - Discover Excessive URLs

Find URLs exceeding a threshold:

```bash
filter-excessive-urls find -i INPUT -n THRESHOLD > output.txt
```

**Examples:**

```bash
# Find URLs with >1000 occurrences
filter-excessive-urls find -i arquivo.cdxj -n 1000 > excessive.txt

# Find with lower threshold (more aggressive)
filter-excessive-urls find -i input.cdxj -n 500 > excessive.txt

# Read from stdin
cat merged.cdxj | filter-excessive-urls find -i - -n 1000 > excessive.txt
```

**Output format:**
```
pt,spam,www)/loop 5000
pt,trap,site)/infinite 3500
# Found 2 URLs with > 1000 occurrences
```

### 2. Remove Mode - Filter Out Excessive URLs

Filter using a list of excessive URLs:

```bash
filter-excessive-urls remove -i INPUT -b BLACKLIST [-o OUTPUT]
```

**Examples:**

```bash
# Filter using generated list
filter-excessive-urls remove -i input.cdxj -b excessive.txt -o output.cdxj

# Pipeline mode (stdin/stdout)
cat input.cdxj | filter-excessive-urls remove -i - -b excessive.txt > output.cdxj

# In complete pipeline
merge-flat-cdxj - *.cdxj | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    flat-cdxj-to-zipnum -o indexes -i -
```

### 3. Auto Mode - Find and Filter in One Pass

Automatically find and filter (requires file, not stdin):

```bash
filter-excessive-urls auto -i INPUT [-o OUTPUT] -n THRESHOLD [-v]
```

**Examples:**

```bash
# Auto mode with default output to stdout
filter-excessive-urls auto -i arquivo.cdxj -n 1000 > cleaned.cdxj

# Auto mode with output file
filter-excessive-urls auto -i arquivo.cdxj -o cleaned.cdxj -n 1000

# With verbose progress
filter-excessive-urls auto -i arquivo.cdxj -o cleaned.cdxj -n 1000 -v
```

**Verbose output:**
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

### Options

**find command:**
- `-i, --input INPUT` - Input CDXJ file or `-` for stdin (required)
- `-n, --threshold N` - Threshold for excessive occurrences (default: 1000)

**remove command:**
- `-i, --input INPUT` - Input CDXJ file or `-` for stdin (required)
- `-b, --blacklist FILE` - File with excessive URLs to filter (required)
- `-o, --output OUTPUT` - Output file or `-` for stdout (default: stdout)

**auto command:**
- `-i, --input INPUT` - Input CDXJ file, cannot be stdin (required)
- `-o, --output OUTPUT` - Output file or `-` for stdout (default: stdout)
- `-n, --threshold N` - Threshold for excessive occurrences (default: 1000)
- `-v, --verbose` - Print progress and statistics

## Python API

### Find Excessive URLs

```python
from replay_cdxj_indexing_tools.utils.filter_excessive_urls import find_excessive_urls

# Find URLs exceeding threshold
excessive = find_excessive_urls('arquivo.cdxj', threshold=1000)

print(f"Found {len(excessive)} excessive URLs")

# Show top 10
for surt, count in sorted(excessive.items(), key=lambda x: -x[1])[:10]:
    print(f"  {surt}: {count} occurrences")
```

### Filter Excessive URLs

```python
from replay_cdxj_indexing_tools.utils.filter_excessive_urls import filter_excessive_urls

# Load excessive URLs (from find command output or custom set)
excessive_set = {'pt,spam,www)/', 'pt,trap,site)/'}

# Filter them out
kept, filtered = filter_excessive_urls(
    input_path='input.cdxj',
    excessive_surts=excessive_set,
    output_path='output.cdxj'
)

print(f"Kept: {kept} lines")
print(f"Filtered: {filtered} lines")
```

### One-Pass Auto Mode

```python
from replay_cdxj_indexing_tools.utils.filter_excessive_urls import process_pipeline

# Find and filter in one pass
excessive_count, kept, filtered = process_pipeline(
    input_path='arquivo.cdxj',
    output_path='cleaned.cdxj',
    threshold=1000,
    verbose=True
)

print(f"Found {excessive_count} excessive URLs")
print(f"Kept {kept} lines, filtered {filtered} lines")
```

## Two-Pass vs Auto Mode

### Two-Pass Approach (Recommended for Pipelines)

**Advantages:**
- Works with stdin/stdout
- Can review excessive URLs before filtering
- Can reuse excessive URL list for multiple files
- Better for large-scale processing

**Example:**
```bash
# Pass 1: Find excessive URLs
filter-excessive-urls find -i arquivo.cdxj -n 1000 > excessive.txt

# Review what will be blocked
head -20 excessive.txt

# Pass 2: Filter them out (can use with stdin)
cat arquivo.cdxj | filter-excessive-urls remove -i - -b excessive.txt > cleaned.cdxj
```

### Auto Mode (Simpler for Single Files)

**Advantages:**
- Single command
- No intermediate files
- Automatic threshold application

**Disadvantages:**
- Requires file input (no stdin)
- Reads file twice (slower for very large files)
- Can't review before filtering

**Example:**
```bash
filter-excessive-urls auto -i arquivo.cdxj -o cleaned.cdxj -n 1000 -v
```

## Use Cases

### 1. Remove Crawler Traps

Infinite loop URLs that trap web crawlers:

```bash
# Find crawler traps (>1000 captures of same URL)
filter-excessive-urls find -i crawl.cdxj -n 1000 > traps.txt

# Review them
cat traps.txt

# Remove them
filter-excessive-urls remove -i crawl.cdxj -b traps.txt -o clean.cdxj
```

### 2. Clean Spam Sites

Sites captured excessively due to spam or SEO tactics:

```bash
# More aggressive threshold for spam
filter-excessive-urls find -i arquivo.cdxj -n 500 > spam.txt

# Filter
filter-excessive-urls remove -i arquivo.cdxj -b spam.txt -o cleaned.cdxj
```

### 3. Balance URL Distribution

Ensure no single URL dominates the index:

```bash
# Find URLs with >5000 occurrences
filter-excessive-urls find -i collection.cdxj -n 5000 > excessive.txt

# Remove them for better balance
filter-excessive-urls remove -i collection.cdxj -b excessive.txt -o balanced.cdxj
```

### 4. Pre-ZipNum Cleanup

Clean indexes before ZipNum conversion:

```bash
# Complete cleanup pipeline
merge-flat-cdxj - /data/*.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    flat-cdxj-to-zipnum -o indexes -i - -n 3000 --compress
```

## Threshold Selection

Choose threshold based on your data:

| Threshold | Use Case | Aggressiveness |
|-----------|----------|----------------|
| 100 | Very small crawls, aggressive filtering | High |
| 500 | Small to medium crawls, remove obvious spam | Medium-High |
| **1000** | **Recommended default** - balanced approach | Medium |
| 5000 | Large crawls, conservative filtering | Low |
| 10000 | Very large crawls, only extreme cases | Very Low |

**Rule of thumb:**
- For N total URLs, use threshold ≈ N/1000 to N/10000
- Start conservative (higher threshold)
- Adjust based on results

### Finding the Right Threshold

```bash
# Check distribution
filter-excessive-urls find -i arquivo.cdxj -n 100 | head -20

# See counts at different thresholds
for t in 100 500 1000 5000; do
    count=$(filter-excessive-urls find -i arquivo.cdxj -n $t 2>&1 | grep "Found" | awk '{print $2}')
    echo "Threshold $t: $count URLs"
done
```

## Performance

**Benchmark Results:**

| Operation | File Size | Lines | Time | Throughput |
|-----------|-----------|-------|------|------------|
| find | 5GB | 10M | ~20s | ~500K lines/sec |
| remove | 5GB | 10M | ~20s | ~500K lines/sec |
| auto | 5GB | 10M | ~40s | ~250K lines/sec |

**Memory Usage:**
- **find**: O(unique URLs) - typically 100-500MB for 10M lines
- **remove**: O(excessive URLs) - typically 1-10MB
- **auto**: Same as find (needs to count all URLs)

**Performance Tips:**

1. **Use two-pass for large files:**
   ```bash
   # Faster than auto mode for very large files
   filter-excessive-urls find -i huge.cdxj -n 1000 > excessive.txt
   filter-excessive-urls remove -i huge.cdxj -b excessive.txt -o clean.cdxj
   ```

2. **Pipeline mode avoids disk I/O:**
   ```bash
   merge-flat-cdxj - *.cdxj | filter-excessive-urls remove -i - -b excessive.txt | ...
   ```

3. **Reuse excessive URL lists:**
   ```bash
   # Generate once
   filter-excessive-urls find -i arquivo.cdxj -n 1000 > excessive.txt
   
   # Use multiple times
   filter-excessive-urls remove -i file1.cdxj -b excessive.txt -o clean1.cdxj
   filter-excessive-urls remove -i file2.cdxj -b excessive.txt -o clean2.cdxj
   ```

## Blacklist File Format

Output from `find` command or manual list:

```text
# Format: SURT_KEY COUNT
pt,spam,www)/loop 5000
pt,trap,example)/ 3500
pt,ads,site)/popup 2000
```

Or simple list (no counts):
```text
pt,spam,www)/loop
pt,trap,example)/
pt,ads,site)/popup
```

Both formats work with `remove` command.

## Error Handling

### Auto Mode with stdin

```bash
# This fails:
cat arquivo.cdxj | filter-excessive-urls auto -i - -n 1000
# Error: Auto mode requires a file (stdin not supported). Use two-pass approach.
```

**Solution:** Use two-pass mode for stdin:
```bash
cat arquivo.cdxj | filter-excessive-urls find -i - -n 1000 > excessive.txt
cat arquivo.cdxj | filter-excessive-urls remove -i - -b excessive.txt
```

### Memory Issues with Many Unique URLs

If `find` command runs out of memory:

```bash
# Reduce threshold (filters fewer URLs)
filter-excessive-urls find -i huge.cdxj -n 5000 > excessive.txt

# Or increase system memory/swap
```

## Testing

Run excessive URL filter tests:

```bash
# All excessive URL tests
pytest tests/test_filter_excessive_urls.py -v

# Specific test categories
pytest tests/test_filter_excessive_urls.py::TestFindExcessiveUrls -v
pytest tests/test_filter_excessive_urls.py::TestProcessPipeline -v

# With coverage
pytest --cov=replay_cdxj_indexing_tools.utils.filter_excessive_urls tests/test_filter_excessive_urls.py
```

## Migration from Legacy Scripts

### Old Approach (Bash)

```bash
# Old: find-excessive-urls.sh and filter-excessive-urls.sh
./find-excessive-urls.sh -n 1000 -f input.cdxj > excessive.txt
./filter-excessive-urls.sh excessive.txt input.cdxj > output.cdxj
```

### New Approach (Python)

```bash
# New: filter-excessive-urls
filter-excessive-urls find -i input.cdxj -n 1000 > excessive.txt
filter-excessive-urls remove -i input.cdxj -b excessive.txt > output.cdxj

# Or auto mode
filter-excessive-urls auto -i input.cdxj -n 1000 > output.cdxj
```

**Benefits:**
- 10-50x faster than bash/awk
- Better memory efficiency
- Pipeline compatible
- Cross-platform
- Integrated with other tools

## See Also

- [filter-blocklist.md](filter-blocklist.md) - Previous step: filter blocked content
- [flat-cdxj-to-zipnum.md](flat-cdxj-to-zipnum.md) - Next step: convert to ZipNum
- [pipeline-examples.md](pipeline-examples.md) - Complete workflows
