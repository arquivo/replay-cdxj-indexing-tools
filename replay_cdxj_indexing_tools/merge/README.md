# CDXJ Merge Module# Efficient CDXJ file merger



K-way merge implementation for efficiently combining multiple sorted CDXJ files into a single sorted output.The [merge_sorted_files.py](merge_sorted_files.py) script merges multiple sorted

files into a single sorted output file using

## Overviewa min-heap based k-way merge algorithm. It's optimized for handling large files

with minimal memory usage by reading files line-by-line rather than loading them

This module provides a memory-efficient k-way merge algorithm that can combine an arbitrary number of sorted CDXJ files while maintaining sorted order. It uses a min-heap data structure to achieve O(N log K) time complexity, where N is the total number of lines and K is the number of files.entirely into memory.



## Features

The script can be used to merge multiple CDXJ files, instead of merging them using `cat` and `sort`. This method is more efficient because it takes advantage that the existing CDXJ files are already sorted.

- **Memory Efficient**: Only keeps K lines in memory (one per file) regardless of file sizes

- **Fast**: O(N log K) time complexity using heap-based priority queue

- **Flexible**: Accepts files, directories, or mix of both as input

- **Stdout Support**: Can output to stdout for pipeline integration

- **Configurable**: Adjustable buffer size for I/O optimization

## Command-Line Usage

### Basic Merging

```bash
# Merge specific files
merge-cdxj output.cdxj file1.cdxj file2.cdxj file3.cdxj

# Merge all files from a directory
merge-cdxj output.cdxj /path/to/cdxj/files/

# Merge files from multiple directories
merge-cdxj output.cdxj /path/dir1/ /path/dir2/

# Mix of files and directories
merge-cdxj output.cdxj file1.cdxj /path/to/dir/ file2.cdxj
```

### Pipeline Integration

```bash
# Output to stdout (use '-' as output filename)
merge-cdxj - file1.cdxj file2.cdxj > merged.cdxj

# Pipe to compression
merge-cdxj - file1.cdxj file2.cdxj | gzip > merged.cdxj.gz

# Pipe to ZipNum conversion
merge-cdxj - *.cdxj | cdxj-to-zipnum -o indexes -i -

# Chain with other tools
merge-cdxj - /data/cdxj/*.cdxj | grep "pt,governo" > governo-only.cdxj
```

## Python API

### merge_sorted_files()

```python
from replay_cdxj_indexing_tools.merge.merge_sorted_files import merge_sorted_files

# Basic usage
files = ['index1.cdxj', 'index2.cdxj', 'index3.cdxj']
merge_sorted_files(files, 'merged_output.cdxj')

# Output to stdout
merge_sorted_files(files, '-')

# Custom buffer size
merge_sorted_files(files, 'output.cdxj', buffer_size=2*1024*1024)  # 2MB
```

**Parameters:**
- `files` (list): Paths to sorted input files
- `output_file` (str): Output path or '-' for stdout
- `buffer_size` (int): I/O buffer size in bytes (default: 1MB)

**Complexity:**
- Time: O(N log K) where N = total lines, K = number of files
- Space: O(K) for the heap

### get_all_files()

```python
from replay_cdxj_indexing_tools.merge.merge_sorted_files import get_all_files

# Collect from directories
paths = ['/data/collection1', '/data/collection2']
all_files = list(get_all_files(paths))
merge_sorted_files(all_files, 'output.cdxj')
```

**Parameters:**
- `paths` (list): File or directory paths

**Yields:**
- `str`: Absolute path to each file

## Examples

### Merge Daily Indexes

```python
import glob
from replay_cdxj_indexing_tools.merge.merge_sorted_files import merge_sorted_files

daily_files = sorted(glob.glob('/data/2023-11-*.cdxj'))
merge_sorted_files(daily_files, '/data/2023-11-monthly.cdxj')
```

### Pipeline Processing

```bash
#!/bin/bash
# Create indexes from WARCs
for warc in /data/warcs/*.warc.gz; do
    cdxj-indexer "$warc" > "/data/indexes/$(basename $warc .warc.gz).cdxj"
done

# Merge and convert to ZipNum
merge-cdxj /data/arquivo-merged.cdxj /data/indexes/
cdxj-to-zipnum -o /data/zipnum -i /data/arquivo-merged.cdxj
```

## Testing

Run 25 comprehensive tests:

```bash
pytest tests/merge/test_merge_sorted_files.py -v
```

## License

MIT License - See LICENSE file for details
