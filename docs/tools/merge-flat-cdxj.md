# merge-flat-cdxj - K-Way Merge Tool for Flat CDXJ Files

Efficiently merges multiple sorted flat CDXJ files into a single sorted output using a k-way merge algorithm.

## Command-Line Usage

### Basic Syntax

```bash
merge-flat-cdxj OUTPUT INPUT1 [INPUT2 INPUT3 ...]
```

### Examples

**Merge specific files:**
```bash
merge-flat-cdxj merged.cdxj file1.cdxj file2.cdxj file3.cdxj
```

**Merge all files from directories:**
```bash
merge-flat-cdxj output.cdxj /data/indexes/2023/ /data/indexes/2024/
```

**Mix files and directories:**
```bash
merge-flat-cdxj merged.cdxj /data/old/ file1.cdxj file2.cdxj /data/new/
```

**Output to stdout (for piping):**
```bash
merge-flat-cdxj - file1.cdxj file2.cdxj | gzip > merged.cdxj.gz
```

**Pipeline to next tool:**
```bash
merge-flat-cdxj - *.cdxj | filter-blocklist -i - -b blocklist.txt -o clean.cdxj
```

**Exclude files matching patterns:**
```bash
# Exclude open collections still being crawled
merge-flat-cdxj merged.cdxj /data/indexes/ --exclude '*-open.cdxj'

# Exclude temporary and open files
merge-flat-cdxj merged.cdxj /data/indexes/ --exclude '*-tmp.cdxj' --exclude '*-open.cdxj'
```

**Verbose progress reporting:**
```bash
# Show which files are included/excluded
merge-flat-cdxj merged.cdxj /data/indexes/ --exclude '*-open.cdxj' --verbose

# Combine with compression
merge-flat-cdxj - /data/indexes/ --exclude '*-open.cdxj' -v | gzip > merged.cdxj.gz
```

### Options

- **OUTPUT**: Output file path, or `-` for stdout
- **INPUT**: Input CDXJ files or directories (will recursively find .cdxj files)
- **--exclude PATTERN**: Exclude files matching glob pattern (can be used multiple times)
- **-v, --verbose**: Enable verbose output to stderr (progress, exclusions, statistics)
- **-q, --quiet**: Suppress all stderr output (overrides --verbose)

Use `merge-flat-cdxj --help` for full help.

## Python API

### Basic Merge

```python
from replay_cdxj_indexing_tools.merge.merge_sorted_files import merge_files

# Merge specific files
input_files = ['file1.cdxj', 'file2.cdxj', 'file3.cdxj']
output_file = 'merged.cdxj'

merge_files(input_files, output_file)
```

### Merge from Directories

```python
from replay_cdxj_indexing_tools.merge.merge_sorted_files import (
    merge_files,
    get_all_files
)

# Get all CDXJ files from directories
directories = ['/data/indexes/2023/', '/data/indexes/2024/']
all_files = list(get_all_files(directories))

print(f"Found {len(all_files)} files to merge")

# Merge them
merge_files(all_files, 'complete_index.cdxj')
```

### Merge to stdout

```python
import sys
from replay_cdxj_indexing_tools.merge.merge_sorted_files import merge_files

# Merge to stdout (for piping)
files = ['file1.cdxj', 'file2.cdxj']
merge_files(files, sys.stdout)
```

### Custom Buffer Size

```python
from replay_cdxj_indexing_tools.merge.merge_sorted_files import merge_files

# Use larger buffer for very large files
merge_files(
    input_files=['huge1.cdxj', 'huge2.cdxj'],
    output_file='merged.cdxj',
    buffer_size=10 * 1024 * 1024  # 10MB buffer
)
```

## Algorithm Details

### K-Way Merge

The tool uses a **min-heap** based k-way merge algorithm:

- **Time Complexity**: O(N log K)
  - N = total number of lines across all files
  - K = number of input files
- **Space Complexity**: O(K)
  - Only stores one line from each file in memory
  - Constant memory usage regardless of file sizes

### Sort Order

CDXJ records are sorted by:
1. **Primary**: SURT key (lexicographic)
2. **Secondary**: Timestamp (chronological)

Example sorted order:
```
pt,governo,www)/ 20230101120000 {...}
pt,governo,www)/ 20230615120000 {...}
pt,sapo,www)/ 20230101120000 {...}
pt,sapo,www)/ 20230615120000 {...}
```

### Performance

Performance is primarily I/O bound - limited by disk read/write speed. Use fast storage (SSD/NVMe) for best results.

## Arquivo.pt Use Case: Excluding Open Collections

At Arquivo.pt, some collections are still being crawled and have files marked as "open" (e.g., `collection-open.cdxj`). These should be excluded from merges to avoid including incomplete data:

```bash
# Exclude all open collections
merge-flat-cdxj complete_index.cdxj /data/indexes/ \
    --exclude '*-open.cdxj' \
    --verbose

# Example verbose output to stderr:
# [DISCOVER] Scanning directory: /data/indexes/
# [INCLUDE] collection1.cdxj
# [INCLUDE] collection2.cdxj
# [EXCLUDE] collection3-open.cdxj (matches: *-open.cdxj)
# [INCLUDE] collection4.cdxj
# [DISCOVER] Directory /data/indexes/: 4 found, 1 excluded, 3 included
# [SUMMARY] Total: 4 found, 1 excluded, 3 included
# [MERGE] Starting merge of 3 files...
# [MERGE] Complete: 15234567 lines written
```

**Benefits:**
- Only stable, complete collections are merged
- Progress visibility shows what's included/excluded
- Stdout remains clean for pipeline compatibility
- Can combine multiple exclusion patterns

## Use Cases

### 1. Parallel Indexing Merge

After parallel indexing of WARC files:

```bash
# Each worker produces partial indexes
# Worker 1: /tmp/index_part_0.cdxj
# Worker 2: /tmp/index_part_1.cdxj
# Worker 3: /tmp/index_part_2.cdxj
# ...

# Merge all parts
merge-flat-cdxj final_index.cdxj /tmp/index_part_*.cdxj
```

### 2. Incremental Index Updates

Merge new indexes with existing ones:

```bash
# Merge old index with new captures
merge-flat-cdxj updated.cdxj old_index.cdxj new_captures.cdxj
```

### 3. Multi-Collection Merging

Combine indexes from different collections:

```bash
merge-flat-cdxj all_collections.cdxj \
    /data/collection1/index.cdxj \
    /data/collection2/index.cdxj \
    /data/collection3/index.cdxj
```

### 4. Pipeline Processing

As first step in processing pipeline:

```bash
# Merge → Filter → Convert
merge-flat-cdxj - /data/parts/*.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    flat-cdxj-to-zipnum -o indexes -i -
```

### 5. Selective Merging with Exclusions

Merge only stable indexes, excluding temporary or incomplete files:

```bash
# Exclude temporary work files and open collections
merge-flat-cdxj production.cdxj /data/indexes/ \
    --exclude '*-tmp.cdxj' \
    --exclude '*-temp.cdxj' \
    --exclude '*-open.cdxj' \
    --verbose 2> merge.log

# Check the log to see what was excluded
cat merge.log
```

## Error Handling

### Unsorted Input

If input files are not properly sorted:

```bash
# Error: Input file 'file.cdxj' is not sorted at line 1234
```

**Solution:** Sort input files before merging:
```bash
export LC_ALL=C
sort file.cdxj > file_sorted.cdxj
```

### Memory Issues

For systems with limited RAM:

```python
# Use smaller buffer size
merge_files(files, output, buffer_size=512*1024)  # 512KB
```

### Too Many Open Files

If merging hundreds of files:

```bash
# Increase file descriptor limit
ulimit -n 4096

# Then run merge
merge-flat-cdxj output.cdxj /data/parts/*.cdxj
```

## Performance Tips

### 1. Use SSD Storage

Merge performance is I/O bound. Use SSD for best performance:
- **HDD**: Slower throughput
- **SSD**: Good performance for most use cases
- **NVMe**: Best performance for large merges

### 2. Pipe to Compression

Save disk space by compressing output:

```bash
merge-flat-cdxj - *.cdxj | gzip -9 > merged.cdxj.gz
```

### 3. Parallel Compression

Use pigz for parallel compression:

```bash
merge-flat-cdxj - *.cdxj | pigz -9 > merged.cdxj.gz
```

### 4. Avoid Network Filesystems

Don't merge files on NFS or slow network storage. Copy locally first:

```bash
# Bad: slow network I/O
merge-flat-cdxj output.cdxj /nfs/data/*.cdxj

# Good: local processing
cp /nfs/data/*.cdxj /tmp/
merge-flat-cdxj output.cdxj /tmp/*.cdxj
mv output.cdxj /nfs/data/
```

### 5. Monitor Progress

For large merges, monitor progress:

```bash
# Count input lines
total=$(cat /data/*.cdxj | wc -l)

# Watch output grow
watch -n 5 "wc -l merged.cdxj"
```

## Testing

Run merge tests:

```bash
# All merge tests
pytest tests/test_merge_sorted_files.py -v

# Specific test
pytest tests/test_merge_sorted_files.py::TestMergeSortedFiles::test_merge_large_files -v

# With coverage
pytest --cov=replay_cdxj_indexing_tools.merge tests/test_merge_sorted_files.py
```

## See Also

- [filter-blocklist.md](filter-blocklist.md) - Next step: filter unwanted content
- [filter-excessive-urls.md](filter-excessive-urls.md) - Remove crawler traps
- [pipeline-examples.md](pipeline-examples.md) - Complete processing examples
