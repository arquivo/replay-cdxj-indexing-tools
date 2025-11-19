# Usage Guide

## Installation

### Option 1: Install from source (recommended for development)

```bash
# Clone the repository
git clone git@github.com:arquivo/cdxj-incremental-indexing.git
cd cdxj-incremental-indexing

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Option 2: Install from Git

```bash
pip install git+https://github.com/arquivo/cdxj-incremental-indexing.git
```

## Usage Examples

### 1. Merge CDXJ Files as a Library

```python
from cdxj_indexing import merge_sorted_files

# Merge two files
files = ['index1.cdxj', 'index2.cdxj']
merge_sorted_files(files, 'merged.cdxj')
```

### 2. Merge Files from Directories

```python
from cdxj_indexing import get_all_files, merge_sorted_files

# Get all files from multiple directories
all_files = list(get_all_files([
    '/data/indexes_2023/',
    '/data/indexes_2024/'
]))

# Merge them all
merge_sorted_files(all_files, 'complete_index.cdxj')
```

### 3. Command-Line Usage

```bash
# Simple merge
merge-cdxj output.cdxj file1.cdxj file2.cdxj

# Merge with directories
merge-cdxj merged.cdxj /path/to/dir1 /path/to/dir2 file3.cdxj

# Pipe to compression
merge-cdxj - *.cdxj | gzip > merged.cdxj.gz
```

### 4. Using Shell Scripts

```bash
# Run full incremental indexing pipeline
./scripts/cdxj-incremental-indexing.sh \
  -w /data/warcs/collection \
  -x /data/incremental/collection \
  -o /data/final_index.cdxj \
  -P -p 8
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=cdxj_indexing --cov-report=html

# Run specific test
pytest tests/test_merge_sorted_files.py::TestMergeSortedFiles::test_merge_two_simple_files

# Verbose output
pytest -v -s
```

## Performance Tips

1. **Use parallel processing** for large collections:
   ```bash
   ./scripts/cdxj-incremental-indexing.sh -w /data/warcs -x /tmp/incremental -o output.cdxj -P
   ```

2. **Adjust buffer size** for very large files:
   ```python
   merge_sorted_files(files, output, buffer_size=10*1024*1024)  # 10MB buffer
   ```

3. **Stream to compression** to save disk space:
   ```bash
   merge-cdxj - *.cdxj | gzip -9 > final.cdxj.gz
   ```
