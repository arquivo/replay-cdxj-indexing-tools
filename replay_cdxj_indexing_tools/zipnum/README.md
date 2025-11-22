# CDXJ to ZipNum Conversion Module

Converts sorted CDXJ files into ZipNum compressed sharded indexes for efficient binary search in web archive replay systems.

## Overview

This module implements ZipNum conversion for CDXJ files, creating compressed shards with accompanying index and location files. The ZipNum format enables efficient binary search over large CDXJ indexes by splitting them into manageable compressed chunks with searchable metadata.

## What is ZipNum?

ZipNum is a compressed indexing format used by pywb and other web archive tools that provides:
- **Compressed storage**: Gzip-compressed shard files save disk space
- **Fast lookup**: Binary search via index file with compressed byte offsets
- **Scalability**: Large indexes split into manageable shards (default 100MB each)
- **HTTP range support**: Compressed offsets enable efficient range requests

## Features

- **Streaming processing**: Memory-efficient streaming of large CDXJ files
- **Parallel compression**: Multi-threaded gzip compression for speed
- **Dynamic sharding**: Automatic shard creation based on compressed size
- **Flexible naming**: Customizable output file names
- **Pipeline friendly**: Supports stdin for seamless integration
- **Configurable**: Adjustable shard sizes, chunk sizes, compression levels

## Command-Line Usage

### Basic Conversion

```bash
# Convert with defaults (100MB shards, 3000 lines/chunk)
cdxj-to-zipnum -o output_dir -i input.cdxj

# Read from stdin
cat merged.cdxj | cdxj-to-zipnum -o output_dir -i -

# Convert gzipped input
cdxj-to-zipnum -o output_dir -i input.cdxj.gz
```

### Custom Parameters

```bash
# Custom shard and chunk sizes
cdxj-to-zipnum -o output -i input.cdxj -s 200 -c 5000

# Custom base name
cdxj-to-zipnum -o output -i input.cdxj --base arquivo-2023

# High compression (slower, smaller files)
cdxj-to-zipnum -o output -i input.cdxj --compress-level 9

# Fast compression (faster, larger files)
cdxj-to-zipnum -o output -i input.cdxj --compress-level 1

# More parallel workers (8 CPUs)
cdxj-to-zipnum -o output -i input.cdxj --workers 8
```

### Pipeline Integration

```bash
# Merge then convert
merge-cdxj - *.cdxj | cdxj-to-zipnum -o indexes -i -

# Full workflow
cdxj-indexer *.warc.gz | \
    merge-cdxj - | \
    cdxj-to-zipnum -o indexes -i -
```

## Python API

### cdxj_to_zipnum()

```python
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum

# Basic conversion
cdxj_to_zipnum(
    output_dir='output',
    input_path='input.cdxj'
)

# Advanced options
cdxj_to_zipnum(
    output_dir='output',
    input_path='input.cdxj',
    shard_size_mb=200,         # 200MB shards
    chunk_size=5000,            # 5000 lines per index entry
    base='arquivo-2023',        # Custom base name
    compress_level=9,           # Maximum compression
    workers=8                   # 8 parallel workers
)
```

**Parameters:**
- `output_dir` (str): Output directory for ZipNum files
- `input_path` (str): Input CDXJ file or '-' for stdin
- `shard_size_mb` (int): Target shard size in MB (default: 100)
- `chunk_size` (int): Lines per index chunk (default: 3000)
- `base` (str): Base name for output files (default: output dir basename)
- `idx_name` (str): Custom index filename (optional)
- `loc_name` (str): Custom location filename (optional)
- `compress_level` (int): Gzip compression 1-9 (default: 6)
- `workers` (int): Parallel compression workers (default: 4)

## Output Files

The conversion produces three file types:

### 1. Shard Files (.cdx.gz)

Gzip-compressed CDXJ data blocks.

**Naming:**
- Single shard: `<base>.cdx.gz`
- Multiple shards: `<base>-01.cdx.gz`, `<base>-02.cdx.gz`, ...

**Content:** Sorted CDXJ lines in gzip format

### 2. Index File (.idx)

Tab-separated index for binary search.

**Format:**
```
<cdxj_key>\t<shard_name.cdx.gz>\t<byte_offset>\t<byte_length>\t<shard_number>
```

**Example:**
```
pt,governo,www)/ 20230615120200	arquivo-01.cdx.gz	186	193	1
pt,sapo,www)/ 20230615120300	arquivo-01.cdx.gz	379	155	1
```

### 3. Location File (.loc)

Maps shard names to file paths.

**Format:**
```
<shard_name.cdx.gz>\t<filepath>
```

**Example:**
```
arquivo-01.cdx.gz\tarquivo-01.cdx.gz
arquivo-02.cdx.gz\tarquivo-02.cdx.gz
```

## Parameters Explained

### Shard Size (`-s`, `--shard-size`)

Target size for each compressed shard file in MB.

- **Default**: 100MB (matches WARC file convention)
- **Small (10-50MB)**: Better for frequently accessed indexes
- **Large (200-500MB)**: Better for archival storage
- **Note**: Actual size varies due to compression

### Chunk Size (`-c`, `--chunk-size`)

Number of CDXJ lines per index entry.

- **Default**: 3000 lines
- **Smaller (1000)**: More index entries, faster pinpoint lookup, larger index
- **Larger (5000)**: Fewer index entries, more data scanned per lookup, smaller index
- **Trade-off**: Index size vs. search granularity

### Compression Level (`--compress-level`)

Gzip compression level 1-9.

- **1-3**: Fast compression, larger files
- **6** (default): Balanced speed/size
- **9**: Maximum compression, slower

### Workers (`--workers`)

Number of parallel compression threads.

- **Default**: 4 workers
- **Recommendation**: Match CPU cores
- **Note**: More workers = faster compression, more memory

## Examples

### Example 1: Convert Arquivo.pt Collection

```python
from replay_cdxj_indexing_tools.zipnum.cdxj_to_zipnum import cdxj_to_zipnum

cdxj_to_zipnum(
    output_dir='/data/indexes/arquivo-2023',
    input_path='/data/cdxj/arquivo-2023-merged.cdxj',
    shard_size_mb=100,
    chunk_size=3000,
    base='arquivo-pt-2023'
)
```

### Example 2: High-Compression Archival

```bash
# Maximum compression for long-term storage
cdxj-to-zipnum \
    -o /archive/indexes \
    -i /data/complete.cdxj \
    -s 500 \
    --compress-level 9 \
    --base archive-2023
```

### Example 3: Fast Processing

```bash
# Fast compression for quick testing
cdxj-to-zipnum \
    -o /tmp/test \
    -i test.cdxj \
    -s 10 \
    --compress-level 1 \
    --workers 8
```

### Example 4: Complete Workflow

```bash
#!/bin/bash
# Full WARC to ZipNum pipeline

WARC_DIR="/data/warcs"
CDXJ_DIR="/data/cdxj"
INDEX_DIR="/data/indexes"

# Step 1: Create CDXJ from WARCs
echo "Creating CDXJ indexes..."
for warc in $WARC_DIR/*.warc.gz; do
    name=$(basename "$warc" .warc.gz)
    cdxj-indexer "$warc" > "$CDXJ_DIR/${name}.cdxj"
done

# Step 2: Merge all CDXJ files
echo "Merging CDXJ files..."
merge-cdxj "$CDXJ_DIR/merged.cdxj" "$CDXJ_DIR"/*.cdxj

# Step 3: Convert to ZipNum
echo "Converting to ZipNum..."
cdxj-to-zipnum \
    -o "$INDEX_DIR" \
    -i "$CDXJ_DIR/merged.cdxj" \
    -s 100 \
    -c 3000 \
    --base collection-2023

echo "Done! Index at: $INDEX_DIR/collection-2023.idx"
```

## Integration with pywb

After conversion, configure pywb to use the ZipNum index:

```yaml
# config.yaml
collections:
  myarchive:
    index_paths: /data/indexes/collection-2023.idx
```

pywb automatically:
- Reads the .idx file for binary search
- Uses the .loc file to locate shards
- Fetches compressed ranges from .cdx.gz files

Start pywb:
```bash
wayback --live
# Access at http://localhost:8080/myarchive/<url>
```

## Performance Tips

- **Default settings** (100MB shards, 3000 lines/chunk) work well for most cases
- **Match workers to CPU cores** for optimal compression speed
- **Use stdin** when piping to avoid intermediate files
- **Higher compression** saves disk space but takes longer
- **Smaller chunks** improve search speed but increase index size

## Testing

Run 22 comprehensive tests:

```bash
pytest tests/test_cdxj_to_zipnum.py -v
```

Test coverage includes:
- CDXJ line parsing
- File input handling (plain, gzip, stdin)
- Chunk streaming
- Complete conversion pipeline
- Real Portuguese domain data

## Troubleshooting

### Large Memory Usage

If memory usage is high with many workers:
```bash
cdxj-to-zipnum -o output -i input.cdxj --workers 2
```

### Slow Compression

For faster processing:
```bash
cdxj-to-zipnum -o output -i input.cdxj --compress-level 3 --workers 8
```

### Disk Space

Check shard sizes:
```bash
ls -lh output_dir/*.cdx.gz
```

## License

MIT License - See LICENSE file for details
