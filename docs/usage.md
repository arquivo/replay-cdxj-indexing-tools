# Usage Guide

## Installation

### Option 1: Install from source (recommended for development)

```bash
# Clone the repository
git clone git@github.com:arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Option 2: Install from Git

```bash
pip install git+https://github.com/arquivo/replay-cdxj-indexing-tools.git
```

## Usage Examples

### 1. Add Fields to CDXJ Records

#### Command-Line

```bash
# Add simple fields
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \
    -f collection=ARQUIVO-2024 \
    -f source=web

# Use custom Python function
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj \
    --function addfield_year.py

# Parallel processing (recommended)
parallel -j 16 \
    'addfield-to-flat-cdxj -i {} -o {.}.enriched.cdxj -f collection=COL' \
    ::: *.cdxj
```

#### Python API

```python
from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import addfield_to_cdxj

# Add simple fields
processed, skipped = addfield_to_cdxj(
    'input.cdxj',
    'output.cdxj',
    fields={'collection': 'ARQUIVO-2024', 'source': 'web'}
)

# Use custom function
def my_addfield(surt_key, timestamp, json_data):
    year = timestamp[:4]
    json_data['year'] = year
    return json_data

processed, skipped = addfield_to_cdxj(
    'input.cdxj',
    'output.cdxj',
    addfield_func=my_addfield
)
```

### 2. Merge CDXJ Files as a Library

```python
from cdxj_indexing import merge_sorted_files

# Merge two files
files = ['index1.cdxj', 'index2.cdxj']
merge_sorted_files(files, 'merged.cdxj')
```

### 3. Merge Files from Directories

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

### 4. Command-Line Usage

```bash
# Simple merge
merge-flat-cdxj output.cdxj file1.cdxj file2.cdxj

# Merge with directories
merge-flat-cdxj merged.cdxj /path/to/dir1 /path/to/dir2 file3.cdxj

# Pipe to compression
merge-flat-cdxj - *.cdxj | gzip > merged.cdxj.gz
```

### 5. Using Shell Scripts

```bash
# Run full incremental indexing pipeline
./scripts/replay-cdxj-indexing-tools.sh \
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
   ./scripts/replay-cdxj-indexing-tools.sh -w /data/warcs -x /tmp/incremental -o output.cdxj -P
   ```

2. **Adjust buffer size** for very large files:
   ```python
   merge_sorted_files(files, output, buffer_size=10*1024*1024)  # 10MB buffer
   ```

3. **Stream to compression** to save disk space:
   ```bash
   merge-flat-cdxj - *.cdxj | gzip -9 > final.cdxj.gz
   ```
