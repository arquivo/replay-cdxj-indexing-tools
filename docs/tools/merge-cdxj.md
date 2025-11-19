# merge-cdxj - K-Way Merge Tool

Efficiently merges multiple sorted CDXJ files into a single sorted output using a k-way merge algorithm.

## Command-Line Usage

### Basic Syntax

```bash
merge-cdxj OUTPUT INPUT1 [INPUT2 INPUT3 ...]
```

### Examples

**Merge specific files:**
```bash
merge-cdxj merged.cdxj file1.cdxj file2.cdxj file3.cdxj
```

**Merge all files from directories:**
```bash
merge-cdxj output.cdxj /data/indexes/2023/ /data/indexes/2024/
```

**Mix files and directories:**
```bash
merge-cdxj merged.cdxj /data/old/ file1.cdxj file2.cdxj /data/new/
```

**Output to stdout (for piping):**
```bash
merge-cdxj - file1.cdxj file2.cdxj | gzip > merged.cdxj.gz
```

**Pipeline to next tool:**
```bash
merge-cdxj - *.cdxj | filter-blocklist -i - -b blocklist.txt -o clean.cdxj
```

### Options

- **OUTPUT**: Output file path, or `-` for stdout
- **INPUT**: Input CDXJ files or directories (will recursively find .cdxj files)

Use `merge-cdxj --help` for full help.

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

**Benchmark Results** (tested on modern hardware):

| Files | Total Lines | Total Size | Time | Throughput |
|-------|-------------|------------|------|------------|
| 10 | 1M | 500MB | ~2s | ~500K lines/sec |
| 50 | 10M | 5GB | ~20s | ~500K lines/sec |
| 100 | 100M | 50GB | ~200s | ~500K lines/sec |

Performance is primarily I/O bound - limited by disk read/write speed.

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
merge-cdxj final_index.cdxj /tmp/index_part_*.cdxj
```

### 2. Incremental Index Updates

Merge new indexes with existing ones:

```bash
# Merge old index with new captures
merge-cdxj updated.cdxj old_index.cdxj new_captures.cdxj
```

### 3. Multi-Collection Merging

Combine indexes from different collections:

```bash
merge-cdxj all_collections.cdxj \
    /data/collection1/index.cdxj \
    /data/collection2/index.cdxj \
    /data/collection3/index.cdxj
```

### 4. Pipeline Processing

As first step in processing pipeline:

```bash
# Merge → Filter → Convert
merge-cdxj - /data/parts/*.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    cdxj-to-zipnum -o indexes -i -
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
merge-cdxj output.cdxj /data/parts/*.cdxj
```

## Performance Tips

### 1. Use SSD Storage

Merge performance is I/O bound. Use SSD for best performance:
- **HDD**: ~100-200K lines/sec
- **SSD**: ~500K-1M lines/sec
- **NVMe**: ~1M+ lines/sec

### 2. Pipe to Compression

Save disk space by compressing output:

```bash
merge-cdxj - *.cdxj | gzip -9 > merged.cdxj.gz
```

### 3. Parallel Compression

Use pigz for parallel compression:

```bash
merge-cdxj - *.cdxj | pigz -9 > merged.cdxj.gz
```

### 4. Avoid Network Filesystems

Don't merge files on NFS or slow network storage. Copy locally first:

```bash
# Bad: slow network I/O
merge-cdxj output.cdxj /nfs/data/*.cdxj

# Good: local processing
cp /nfs/data/*.cdxj /tmp/
merge-cdxj output.cdxj /tmp/*.cdxj
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
