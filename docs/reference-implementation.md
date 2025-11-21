# Reference Implementation: cdxj-index-collection

Complete guide to the `cdxj-index-collection` script for production CDXJ processing.

## Overview

`cdxj-index-collection` is a comprehensive bash script that orchestrates the complete CDXJ indexing pipeline. It's the **recommended way** to process web archive collections at Arquivo.pt.

**Features:**
- ✅ Parallel WARC indexing
- ✅ Automatic merge, filter, and conversion
- ✅ Incremental mode for daily updates
- ✅ Atomic write protection
- ✅ Progress tracking and logging
- ✅ Error handling and validation
- ✅ Configurable via command-line flags

## Quick Usage

```bash
# Process collection with defaults
./cdxj-index-collection AWP999

# Daily incremental update
./cdxj-index-collection AWP999 --incremental

# Custom configuration
./cdxj-index-collection AWP999 \
    --jobs 32 \
    --threshold 5000 \
    --blocklist /path/to/blocklist.txt
```

## Command-Line Options

### Required

- **COLLECTION_NAME** - Name of the collection (e.g., `AWP999`)

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--blocklist FILE` | `/data/blocklists/arquivo-blocklist.txt` | Path to blocklist file |
| `--threshold N` | `1000` | Excessive URLs threshold |
| `--jobs N` | `$(nproc)` | Number of parallel jobs |
| `--shard-size N` | `3000` | ZipNum shard size (lines) |
| `--collections-dir DIR` | `/data/collections` | Collections base directory |
| `--output-dir DIR` | `/data/zipnum` | Output directory base |
| `--temp-dir DIR` | `/tmp/cdxj-processing` | Temporary directory |
| `--keep-temp` | off | Keep temporary files |
| `--no-compress` | off | Don't compress ZipNum |
| `--incremental` | off | Only index new/modified WARCs |
| `--help` | - | Show help message |

## Directory Structure

### Input

```
/data/collections/AWP999/
├── file-001.warc.gz
├── file-002.warc.gz
└── file-NNN.warc.gz
```

### Processing (Temporary)

```
/tmp/cdxj-processing/AWP999/
├── indexes/
│   ├── file-001.cdxj      # Individual indexes
│   ├── file-002.cdxj
│   └── ...
├── merged.cdxj            # Merged index
├── filtered-blocklist.cdxj
├── filtered-excessive.cdxj
└── final.cdxj
```

### Output

```
/data/zipnum/AWP999/
├── cdx-00000.gz
├── cdx-00001.gz
├── ...
└── summary.idx
```

## Pipeline Stages

The script executes 5 stages sequentially:

### Stage 1: Parallel WARC Indexing

**Command:**
```bash
find /data/collections/AWP999 -name "*.warc.gz" | \
    parallel --bar --eta -j $JOBS \
    'cdx-indexer {} > indexes/{/.}.cdxj'
```

**Features:**
- Atomic writes using `.tmp` files
- Skips already-indexed files (incremental mode)
- Progress bar with ETA
- Parallel execution

**Incremental Mode:**
```bash
# Only index if WARC is newer than CDXJ
if [[ $WARC_FILE -nt $CDXJ_FILE ]]; then
    cdx-indexer $WARC_FILE > $CDXJ_FILE
fi
```

**Output:** One `.cdxj` file per `.warc.gz` file

### Stage 2: Merge Indexes

**Command:**
```bash
merge-flat-cdxj merged.cdxj indexes/*.cdxj
```

**Output:** Single `merged.cdxj` file, sorted by SURT

### Stage 3: Filter Blocklist

**Command:**
```bash
filter-blocklist \
    -i merged.cdxj \
    -b /data/blocklists/arquivo-blocklist.txt \
    -o filtered-blocklist.cdxj
```

**Output:** CDXJ with blocked content removed

### Stage 4: Filter Excessive URLs

**Command:**
```bash
filter-excessive-urls auto \
    -i filtered-blocklist.cdxj \
    -o filtered-excessive.cdxj \
    -n 1000
```

**Output:** CDXJ with excessive URLs removed

### Stage 5: Convert to ZipNum

**Command:**
```bash
flat-cdxj-to-zipnum \
    -o /data/zipnum/AWP999/ \
    -i filtered-excessive.cdxj \
    -n 3000 \
    --compress
```

**Output:** Compressed ZipNum shards ready for pywb

## Usage Examples

### Basic Collection Processing

```bash
./cdxj-index-collection AWP999
```

**What it does:**
1. Indexes all WARCs in parallel
2. Merges indexes
3. Applies default blocklist
4. Removes URLs with >1000 captures
5. Creates compressed ZipNum

**Duration:** ~2-4 hours for 100GB collection (32 cores)

### Daily Incremental Update

```bash
./cdxj-index-collection AWP999 --incremental
```

**What it does:**
1. **Only indexes new/modified WARCs** (timestamp check)
2. Merges with existing indexes
3. Applies filters
4. Updates ZipNum

**Duration:** ~30-60 minutes (typically 5-10% of full processing)

**Use case:** Daily crawl additions

### Custom Configuration

```bash
./cdxj-index-collection AWP999 \
    --jobs 64 \
    --threshold 5000 \
    --shard-size 5000 \
    --blocklist /custom/blocklist.txt \
    --keep-temp
```

**Custom settings:**
- 64 parallel jobs (high-core server)
- More lenient excessive threshold (5000)
- Larger shards (5000 lines)
- Custom blocklist
- Keep temp files for inspection

### Testing with Subset

```bash
# Create test collection
mkdir -p /tmp/test-collection
cp /data/collections/PROD/*.warc.gz /tmp/test-collection/ | head -10

# Process small subset
./cdxj-index-collection test-collection \
    --collections-dir /tmp \
    --output-dir /tmp/test-output \
    --temp-dir /tmp/test-temp
```

## Monitoring & Logging

### Console Output

The script provides color-coded logging:

```
[INFO] Starting processing for collection: AWP999
[INFO] Found 1523 WARC files

Configuration:
  Collection:         AWP999
  WARC files:         1523
  Collection dir:     /data/collections/AWP999
  Output dir:         /data/zipnum/AWP999
  Blocklist:          /data/blocklists/arquivo-blocklist.txt
  Excessive threshold: 1000
  Parallel jobs:      32
  Incremental mode:   1

[STAGE 1/5] Indexing WARCs in parallel...
[INFO] Found 23 orphaned .tmp files from previous run, cleaning up...
100% [====================================] 1523/1523 ETA: 0s

[SUCCESS] Stage 1 completed in 45m 23s

[STAGE 2/5] Merging CDXJ indexes...
[SUCCESS] Stage 2 completed in 8m 12s

...

[SUCCESS] All stages completed successfully
[SUCCESS] Total processing time: 1h 35m 47s
[INFO] ZipNum output: /data/zipnum/AWP999/
```

### Log Files

Redirect output for persistent logging:

```bash
./cdxj-index-collection AWP999 2>&1 | \
    tee logs/AWP999_$(date +%Y%m%d).log
```

### Progress Tracking

Each stage shows:
- Current stage number (X/5)
- Progress bars (for parallel operations)
- ETA estimates
- Duration upon completion

## Error Handling

### Validation Checks

**Pre-flight checks:**
1. Collection directory exists
2. Contains WARC files
3. Dependencies installed (parallel, cdx-indexer)
4. Blocklist file exists (warning if not)
5. Output directories writable

**Exit conditions:**
- Missing collection: `exit 1`
- No WARC files: `exit 1`
- Missing dependencies: `exit 1`

### Atomic Write Protection

**Problem:** Interrupted indexing leaves corrupted `.cdxj` files

**Solution:**
```bash
# Write to temporary file
cdx-indexer file.warc.gz > file.cdxj.tmp

# Only move if successful
if [ $? -eq 0 ]; then
    mv file.cdxj.tmp file.cdxj
fi
```

**Recovery:**
- Orphaned `.tmp` files are automatically cleaned up on next run
- No manual intervention needed

### Interruption Recovery

**Safe to interrupt:**
- Press `Ctrl+C` at any time
- Partial work is saved
- Re-run with `--incremental` to resume

**What happens:**
- Completed CDXJ files are kept
- Incomplete `.tmp` files are removed on restart
- Indexing resumes from where it stopped

## Performance Tuning

### CPU Cores

```bash
# Use all cores (default)
./cdxj-index-collection AWP999

# Limit to 16 cores
./cdxj-index-collection AWP999 --jobs 16

# Use more cores (if available)
./cdxj-index-collection AWP999 --jobs 64
```

**Recommendation:** Start with default (`nproc`), adjust based on I/O vs CPU bottleneck

### Memory Considerations

**Merge stage** memory usage:
- ~1 MB per input file
- 1000 files = ~1 GB RAM
- Use `ulimit` if needed:
  ```bash
  ulimit -v 16777216  # Limit to 16GB
  ```

### Storage Optimization

**Temporary files:**
```bash
# Use fast SSD for temp dir
./cdxj-index-collection AWP999 \
    --temp-dir /fast-ssd/temp
```

**Keep or cleanup:**
```bash
# Keep temp files for debugging
./cdxj-index-collection AWP999 --keep-temp

# Auto-cleanup (default)
./cdxj-index-collection AWP999
```

## Integration

### Cron Job (Daily Updates)

```bash
# /etc/cron.d/cdxj-processing
# Run daily at 2 AM
0 2 * * * cdxj /opt/scripts/daily-update.sh
```

**daily-update.sh:**
```bash
#!/bin/bash
set -e

COLLECTION="ARQUIVO-$(date +%Y%m)"
LOG_FILE="/var/log/cdxj/daily-$(date +%Y%m%d).log"

echo "Starting daily update for $COLLECTION" | tee -a "$LOG_FILE"

/opt/replay-cdxj-indexing-tools/cdxj-index-collection \
    "$COLLECTION" \
    --incremental \
    --jobs 32 \
    2>&1 | tee -a "$LOG_FILE"

echo "Update completed" | tee -a "$LOG_FILE"
```

### Monitoring Integration

**Prometheus metrics:**
```bash
# Export metrics from log
grep "Total processing time" log.txt | \
    awk '{print "cdxj_processing_duration_seconds " $5}'
```

**Alerting:**
```bash
# Check exit code
if ! ./cdxj-index-collection AWP999; then
    echo "ALERT: CDXJ processing failed" | \
        mail -s "CDXJ Failure" ops@example.com
fi
```

## Troubleshooting

### Script fails immediately

**Check:**
```bash
# Verify dependencies
which parallel
which cdx-indexer
which merge-flat-cdxj

# Check permissions
ls -la cdxj-index-collection
chmod +x cdxj-index-collection

# Test syntax
bash -n cdxj-index-collection
```

### Parallel indexing errors

**Common causes:**
- Insufficient disk space
- Corrupted WARC files
- Memory limits

**Solutions:**
```bash
# Check disk space
df -h /tmp

# Validate WARCs
for f in *.warc.gz; do
    zcat "$f" | head -c 1000 > /dev/null || echo "Corrupted: $f"
done

# Increase memory limits
ulimit -v unlimited
```

### Blocklist warnings

```
[WARNING] Blocklist not found: /data/blocklists/arquivo-blocklist.txt
[WARNING] Proceeding without blocklist filtering
```

**Solution:** Provide valid blocklist path:
```bash
./cdxj-index-collection AWP999 \
    --blocklist /correct/path/blocklist.txt
```

For more issues, see [Troubleshooting Guide](troubleshooting.md).

## See Also

- [Quick Start](quick-start.md) - Get started quickly
- [Architecture](architecture.md) - System design
- [Incremental Workflow](incremental-workflow.md) - Daily updates guide
- [Pipeline Examples](pipeline-examples.md) - Advanced workflows
