# arclist-to-path-index - Arclist to Path Index Converter

Convert Arquivo.pt arclist files (URLs or file paths) to pywb path index format for Redis submission.

## Overview

`arclist-to-path-index` converts arclist text files (containing URLs or file paths) into pywb path index format (TSV: filename<tab>original_entry), which can then be piped to `path-index-to-redis` for loading into Redis. The tool automatically extracts the basename from each entry and uses the original URL/path as-is in the output.

**Key Features:**
- ✅ Simple arclist input format (one URL or path per line)
- ✅ Automatic basename extraction from URLs and paths
- ✅ Direct mode: URLs/paths used exactly as provided
- ✅ Pipeline-friendly (stdin/stdout)
- ✅ Folder processing for batch operations
- ✅ Compatible with `path-index-to-redis`

## What is Arclist?

An **arclist** is a simple text file containing URLs or file paths to WARC/ARC files, one per line. Arquivo.pt uses these files to track available archive files.

**Example arclist.txt (URLs):**
```
http://data.arquivo.pt/warcs/AWP-arquivo-20240101120000-00001.warc.gz
http://data.arquivo.pt/warcs/AWP-arquivo-20240101120500-00002.warc.gz
http://data.arquivo.pt/warcs/AWP-arquivo-20240101121000-00003.warc.gz
```

**Example arclist.txt (file paths):**
```
/mnt/storage/warcs/AWP-arquivo-20240101120000-00001.warc.gz
/mnt/storage/warcs/AWP-arquivo-20240101120500-00002.warc.gz
/mnt/storage/warcs/AWP-arquivo-20240101121000-00003.warc.gz
```

## Quick Start

```bash
# Single arclist file
arclist-to-path-index -i arclist.txt

# Process entire folder (multiple collections)
arclist-to-path-index -d /data/arclists

# Pipeline to Redis
arclist-to-path-index -i arclist.txt | path-index-to-redis -i - -k arquivo-2024
```

## Command-Line Options

### Required Arguments (one of)

| Option | Description |
|--------|-------------|
| `-i, --input FILE` | Input arclist file, or `-` for stdin<br>Can be specified multiple times<br>*Mutually exclusive with `-d`* |
| `-d, --folder PATH` | Folder containing arclist files (*.txt)<br>Processes all collections at once<br>*Mutually exclusive with `-i`* |

### Optional Arguments

| Option | Default | Description |
|--------|---------|-------------|
| `--output-separator SEP` | `\t` (tab) | Output field separator |
| `-v, --verbose` | off | Print progress information |

## How It Works

The tool operates in **direct mode** only:

1. **Read each line** from the arclist (URL or file path)
2. **Extract basename** automatically from the URL/path
3. **Output** in TSV format: `basename<tab>original_entry`

**Example transformation:**
```
Input:  http://data.arquivo.pt/warcs/AWP-arquivo-20240115.warc.gz
Output: AWP-arquivo-20240115.warc.gz	http://data.arquivo.pt/warcs/AWP-arquivo-20240115.warc.gz

Input:  /mnt/storage/warcs/file.warc.gz
Output: file.warc.gz	/mnt/storage/warcs/file.warc.gz

Input:  relative/path/data.warc.gz
Output: data.warc.gz	relative/path/data.warc.gz
```

## Usage Examples

### Example 1: URLs from Arclist

Convert arclist containing HTTP URLs:

```bash
# arclist.txt contains:
# http://data.arquivo.pt/warcs/AWP-arquivo-20240101.warc.gz
# http://data.arquivo.pt/warcs/AWP-arquivo-20240102.warc.gz

arclist-to-path-index -i arclist.txt > pathindex.txt
```

**Output:**
```
AWP-arquivo-20240101.warc.gz	http://data.arquivo.pt/warcs/AWP-arquivo-20240101.warc.gz
AWP-arquivo-20240102.warc.gz	http://data.arquivo.pt/warcs/AWP-arquivo-20240102.warc.gz
```

### Example 2: Absolute File Paths

Convert arclist containing absolute file paths:

```bash
# arclist.txt contains:
# /mnt/storage/warcs/file001.warc.gz
# /mnt/storage/warcs/file002.warc.gz

arclist-to-path-index -i arclist.txt
```

**Output:**
```
file001.warc.gz	/mnt/storage/warcs/file001.warc.gz
file002.warc.gz	/mnt/storage/warcs/file002.warc.gz
```

### Example 3: Relative File Paths

Convert arclist containing relative paths:

```bash
# arclist.txt contains:
# warcs/data/file001.warc.gz
# warcs/data/file002.warc.gz

arclist-to-path-index -i arclist.txt
```

**Output:**
```
file001.warc.gz	warcs/data/file001.warc.gz
file002.warc.gz	warcs/data/file002.warc.gz
```

### Example 4: Process Multiple Collections from Folder

Process all arclist files in a folder:

```bash
# Folder structure:
# /data/arclists/
#   AWP29_ARCS.txt
#   AWP30_ARCS.txt
#   AWP31_ARCS.txt

arclist-to-path-index -d /data/arclists > pathindex_all.txt
```

### Example 5: Pipeline to Redis

Process and load directly into Redis:

```bash
arclist-to-path-index -i arclist.txt | \
    path-index-to-redis -i - -k arquivo-2024 --host redis.arquivo.pt
```

### Example 6: Multiple Input Files

Process specific arclist files:

```bash
arclist-to-path-index \
    -i /data/arclists/AWP29_ARCS.txt \
    -i /data/arclists/AWP30_ARCS.txt \
    -i /data/arclists/AWP31_ARCS.txt \
    > combined_pathindex.txt
```

### Example 7: Custom Separator

Use a different output separator:

```bash
arclist-to-path-index -i arclist.txt --output-separator "|"
```

**Output:**
```
file001.warc.gz|/path/to/file001.warc.gz
file002.warc.gz|/path/to/file002.warc.gz
```

### Example 8: Verbose Mode

See progress information:

```bash
arclist-to-path-index -i arclist.txt --verbose
```

**Output:**
```
Processing: arclist.txt
Processed 1000 entries
Processed 2000 entries
...
Total processed: 5432 entries
```

## Production Workflow

Typical Arquivo.pt workflow processing multiple collections:

```bash
#!/bin/bash
set -e

# Configuration
ARCLISTS_FOLDER="/data/arclists"  # Contains AWP29_ARCS.txt, AWP30_ARCS.txt, etc.
REDIS_HOST="redis.arquivo.pt"
COLLECTION_KEY="arquivo-2024"

echo "Converting all arclists and loading to Redis..."
echo "Folder: $ARCLISTS_FOLDER"

# Convert all arclist files and submit to Redis
arclist-to-path-index \
    -d "$ARCLISTS_FOLDER" \
    --verbose \
    | path-index-to-redis \
        -i - \
        -k "$COLLECTION_KEY" \
        --host "$REDIS_HOST" \
        --batch-size 1000 \
        --verbose

echo "Complete! Processed all collections."
```

## Input Format

### URLs

HTTP/HTTPS URLs (basename is extracted automatically):

```
http://data.arquivo.pt/warcs/AWP-arquivo-20240101120000-00001.warc.gz
http://data.arquivo.pt/warcs/AWP-arquivo-20240101120500-00002.warc.gz
http://data.arquivo.pt/warcs/AWP-arquivo-20240101121000-00003.warc.gz
```

### Absolute File Paths

Full paths (basename is extracted automatically):

```
/mnt/storage/warcs/AWP-arquivo-20240101120000-00001.warc.gz
/mnt/storage/warcs/AWP-arquivo-20240101120500-00002.warc.gz
/mnt/storage/warcs/AWP-arquivo-20240101121000-00003.warc.gz
```

### Relative File Paths

Relative paths (basename is extracted automatically):

```
warcs/AWP-arquivo-20240101120000-00001.warc.gz
data/archives/AWP-arquivo-20240101120500-00002.warc.gz
../backup/AWP-arquivo-20240101121000-00003.warc.gz
```

### Mixed Formats

Arclist can contain mixed URLs and paths:

```
http://data.arquivo.pt/warcs/file001.warc.gz
/mnt/storage/warcs/file002.warc.gz
warcs/data/file003.warc.gz
```

### Comments and Empty Lines

Lines starting with `#` and empty lines are skipped:

```
# Collection 2024-11
# Generated: 2024-11-20

http://data.arquivo.pt/warcs/file001.warc.gz
http://data.arquivo.pt/warcs/file002.warc.gz

# Additional files
http://data.arquivo.pt/warcs/file003.warc.gz
```

## Output Format

Output follows pywb path index format (tab-separated):

```
<filename>\t<original_entry>
```

**Example with URLs:**
```
file001.warc.gz	http://data.arquivo.pt/warcs/file001.warc.gz
file002.warc.gz	http://data.arquivo.pt/warcs/file002.warc.gz
```

**Example with absolute paths:**
```
file001.warc.gz	/mnt/storage/warcs/file001.warc.gz
file002.warc.gz	/mnt/storage/warcs/file002.warc.gz
```

**Example with relative paths:**
```
file001.warc.gz	warcs/data/file001.warc.gz
file002.warc.gz	warcs/data/file002.warc.gz
```

## Integration with path-index-to-redis

The output of `arclist-to-path-index` is designed to be piped directly to `path-index-to-redis`:

```bash
arclist-to-path-index -i arclist.txt | \
    path-index-to-redis -i - -k collection-2024
```

This creates a seamless workflow:
1. **arclist-to-path-index**: Converts arclist → path index format (extracts basenames, preserves URLs/paths)
2. **path-index-to-redis**: Loads path index → Redis

## Performance

**Typical performance** (tested on production data):

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Conversion | ~1M files/sec | In-memory processing |
| Pipeline to Redis | ~50K files/sec | Network-limited |

**Memory usage:** Minimal (streaming processing)

## Use Cases

### 1. Initial Redis Setup - All Collections

Load all collections from folder into Redis:

```bash
arclist-to-path-index -d /data/arclists | \
    path-index-to-redis -i - -k arquivo-all --clear
```

### 2. Process Specific Collections

Process only selected collection files:

```bash
arclist-to-path-index \
    -i /data/arclists/AWP35_ARCS.txt \
    -i /data/arclists/AWP36_ARCS.txt | \
    path-index-to-redis -i - -k arquivo-recent
```

### 3. Daily Updates

Add new files to existing Redis collection:

```bash
# Generate daily arclist (new files only)
comm -13 <(sort yesterday.txt) <(sort today.txt) > new-files.txt

# Load to Redis
arclist-to-path-index -i new-files.txt | \
    path-index-to-redis -i - -k arquivo-2024
```

### 4. Batch Processing

Process arclists from multiple sources:

```bash
# Process all arclists and save to file
arclist-to-path-index -d /data/arclists > all_paths.txt

# Later, load to Redis
cat all_paths.txt | path-index-to-redis -i - -k arquivo-complete
```

## Error Handling

The tool handles common error scenarios:

**Empty arclist:**
```bash
$ arclist-to-path-index -i empty.txt
# No output, exits successfully
```

**Missing required arguments:**
```bash
$ arclist-to-path-index
Error: Must provide either --input or --folder
```

**Invalid input file:**
```bash
$ arclist-to-path-index -i nonexistent.txt
Error: Cannot read file: nonexistent.txt
```

## Troubleshooting

### No output generated

**Problem:** Tool runs but produces no output

**Solution:** Check if arclist file is empty or contains only comments

```bash
# Add --verbose to see progress
arclist-to-path-index -i arclist.txt --verbose
```

### Incorrect basename extraction

**Problem:** Basename doesn't match expected filename

**Solution:** Tool automatically extracts basename using `os.path.basename()`. For URLs, the last path component is used. If you need different behavior, preprocess the arclist file.

```bash
# URL example:
http://example.com/path/file.warc.gz → basename: file.warc.gz

# Path example:
/mnt/storage/data/file.warc.gz → basename: file.warc.gz
```

### Performance issues

**Problem:** Slow processing

**Solution:** Check if verbose output is slowing down pipeline

```bash
# Remove --verbose if not needed for large files
arclist-to-path-index -i large.txt | \
    path-index-to-redis -i - -k collection
```

## Related Tools

- **[path-index-to-redis](../redis/README.md)** - Load path indexes into Redis
- **[cdxj-index-collection](../reference-implementation.md)** - Complete indexing workflow

## Technical Details

### Algorithm

1. **Read arclist** - Line by line (streaming)
2. **Skip** - Ignore empty lines and comments (lines starting with `#`)
3. **Extract basename** - Use `os.path.basename()` on each entry
4. **Output** - Write TSV format: `basename<tab>original_entry`

### Implementation Notes

- Streaming processing (constant memory usage)
- Automatic basename extraction from URLs and paths
- Direct mode only: original URLs/paths preserved as-is
- Python's `os.path.basename()` for path handling

## See Also

- [Redis Integration](../redis/README.md)
- [Production Workflow](../reference-implementation.md)
- [Arquivo.pt Architecture](../architecture.md)

---

**Related Commands:** `path-index-to-redis` • `cdxj-index-collection`
