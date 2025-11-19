# Incremental Indexing Workflow

Guide for daily/incremental processing of web archive collections at Arquivo.pt.

## Overview

When running daily crawls, you don't want to re-index all WARCs every time. The incremental mode only processes new or modified WARC files, making daily updates fast and efficient.

## How It Works

The incremental indexing works by:

1. **Checking timestamps**: For each WARC file, checks if a corresponding CDXJ index already exists
2. **Comparing modification times**: If the CDXJ is newer than the WARC, skips indexing
3. **Processing only new files**: Only indexes WARCs that are new or have been modified
4. **Merging all indexes**: The pipeline still merges ALL indexes (old + new) into the final output

## Daily Workflow

### First Run (Full Indexing)

Process the complete collection for the first time:

```bash
./cdxj-index-collection COLLECTION-2024-11
```

This creates:
- `/tmp/cdxj-processing/COLLECTION-2024-11/indexes/*.cdxj` - Individual WARC indexes
- `/data/zipnum/COLLECTION-2024-11/` - Final ZipNum output

**Important:** Keep the indexes directory (`--keep-temp` or move it to persistent storage)

### Daily Updates (Incremental)

After your daily crawl adds new WARCs, run:

```bash
./cdxj-index-collection COLLECTION-2024-11 --incremental
```

This will:
1. ✅ **Skip** WARCs that are already indexed
2. 🆕 **Index** only new/modified WARCs
3. 🔀 **Merge** all indexes (old + new)
4. 🧹 **Filter** blocklist and excessive URLs
5. 📦 **Convert** to ZipNum format

## Production Setup

### Option 1: Keep Indexes in Temp (Default)

For daily runs where you regenerate everything:

```bash
# Day 1: Full indexing
./cdxj-index-collection COLLECTION-2024-11

# Day 2+: Incremental (indexes in /tmp)
./cdxj-index-collection COLLECTION-2024-11 --incremental
```

**Pros:** Simple, no storage management needed  
**Cons:** Loses indexes if /tmp is cleared, slower if re-indexing from scratch

### Option 2: Persistent Index Storage (Recommended)

Keep indexes in persistent storage for faster recovery:

```bash
# Configure persistent storage
PERSISTENT_INDEXES="/data/cdxj_indexes"

# Day 1: Full indexing with persistent temp
./cdxj-index-collection COLLECTION-2024-11 \
    --temp-dir "$PERSISTENT_INDEXES" \
    --keep-temp

# Day 2+: Incremental with same temp dir
./cdxj-index-collection COLLECTION-2024-11 \
    --temp-dir "$PERSISTENT_INDEXES" \
    --incremental \
    --keep-temp
```

**Pros:** Fast recovery, preserves all indexes  
**Cons:** Requires disk space for indexes

### Option 3: Separate Incremental Directory

Use dedicated directory structure per collection:

```bash
# Setup directories
mkdir -p /data/cdxj_incremental/COLLECTION-2024-11
mkdir -p /data/zipnum/COLLECTION-2024-11

# Daily run
./cdxj-index-collection COLLECTION-2024-11 \
    --temp-dir /data/cdxj_incremental \
    --incremental \
    --keep-temp
```

## Cron Job Example

Automated daily processing:

```bash
#!/bin/bash
# /etc/cron.daily/arquivo-indexing

COLLECTION="COLLECTION-2024-11"
SCRIPT_DIR="/opt/cdxj-incremental-indexing"
LOG_DIR="/var/log/arquivo/indexing"
DATE=$(date +%Y%m%d)

# Create log file
LOG_FILE="$LOG_DIR/$COLLECTION-$DATE.log"
mkdir -p "$LOG_DIR"

# Run incremental indexing
cd "$SCRIPT_DIR"
./cdxj-index-collection "$COLLECTION" \
    --incremental \
    --temp-dir /data/cdxj_incremental \
    --keep-temp \
    --jobs 32 \
    > "$LOG_FILE" 2>&1

# Check exit code
if [ $? -eq 0 ]; then
    echo "SUCCESS: $COLLECTION processed at $(date)" >> "$LOG_FILE"
else
    echo "ERROR: $COLLECTION failed at $(date)" >> "$LOG_FILE"
    # Send alert email
    mail -s "Arquivo.pt Indexing Failed: $COLLECTION" admin@arquivo.pt < "$LOG_FILE"
fi

# Rotate old logs (keep last 30 days)
find "$LOG_DIR" -name "*.log" -mtime +30 -delete
```

Make it executable:
```bash
chmod +x /etc/cron.daily/arquivo-indexing
```

## Performance Considerations

### Disk Space

Incremental indexing requires space for:
- **WARC files**: Original archived data
- **CDXJ indexes**: ~1-5% of WARC size (per file)
- **Final ZipNum**: ~1-3% of total WARC size (compressed)

Example for 1TB collection:
- WARCs: 1000 GB
- Individual indexes: 10-50 GB
- Final ZipNum: 10-30 GB

### Processing Time

Incremental mode dramatically reduces processing time:

**Full indexing** (1TB collection, 10,000 WARCs):
- Indexing: 2-4 hours
- Merge + Filter: 30-60 minutes
- **Total: 3-5 hours**

**Incremental update** (100 new WARCs per day):
- Indexing: 5-15 minutes (only 100 WARCs)
- Merge + Filter: 30-60 minutes (all indexes)
- **Total: 35-75 minutes**

### Optimization Tips

1. **Use persistent index storage** to avoid re-indexing on system reboot
2. **Increase parallel jobs** on servers with many CPUs: `--jobs 64`
3. **Adjust excessive threshold** based on your collection: `--threshold 5000`
4. **Monitor disk space** especially on `/tmp` if not using persistent storage
5. **Run during off-peak hours** to minimize impact on live services

## Troubleshooting

### Indexes Lost After Reboot

**Problem:** /tmp was cleaned, need to re-index everything  
**Solution:** Use `--temp-dir` with persistent storage

```bash
./cdxj-index-collection COLLECTION-2024-11 \
    --temp-dir /data/cdxj_incremental \
    --keep-temp
```

### Script Interrupted During Indexing

**Problem:** Script was killed/stopped during WARC indexing  
**Solution:** Automatic recovery on next run

The script uses **atomic writes** to prevent corrupted indexes:
1. Indexes to `.cdxj.tmp` file first
2. Only moves to `.cdxj` if indexing completes successfully
3. On next run, cleans up any leftover `.tmp` files automatically

**No manual intervention needed** - just re-run the script:

```bash
# Script will automatically:
# 1. Detect and clean up .tmp files from interrupted run
# 2. Re-index any WARCs that weren't completed
./cdxj-index-collection COLLECTION-2024-11 --incremental
```

### Some WARCs Not Being Indexed

**Problem:** Incremental mode skips modified WARCs  
**Solution:** Delete specific index files to force re-indexing

```bash
# Find and delete specific index
rm /data/cdxj_incremental/COLLECTION-2024-11/indexes/problematic.warc.gz.cdxj

# Re-run incremental (will only index deleted ones)
./cdxj-index-collection COLLECTION-2024-11 --incremental
```

### Force Full Re-Index

**Problem:** Need to rebuild all indexes from scratch  
**Solution:** Delete all indexes and run without incremental flag

```bash
# Delete all indexes
rm -rf /data/cdxj_incremental/COLLECTION-2024-11/indexes/

# Full re-index
./cdxj-index-collection COLLECTION-2024-11
```

### Pipeline Stage Failed

**Problem:** Pipeline failed during merge/filter/zipnum stage  
**Solution:** Keep temp files and investigate

```bash
# Run with temp file preservation
./cdxj-index-collection COLLECTION-2024-11 --incremental --keep-temp

# Check intermediate files
ls -lh /tmp/cdxj-processing/COLLECTION-2024-11/indexes/

# Manually run failed stage
merge-cdxj test.cdxj /tmp/cdxj-processing/COLLECTION-2024-11/indexes/*.cdxj
```

## Monitoring

### Check Indexing Status

```bash
# Count indexed vs total WARCs
COLLECTION="COLLECTION-2024-11"
TOTAL_WARCS=$(find /data/collections/$COLLECTION -name "*.warc.gz" | wc -l)
INDEXED=$(ls /data/cdxj_incremental/$COLLECTION/indexes/*.cdxj 2>/dev/null | wc -l)

echo "Collection: $COLLECTION"
echo "Total WARCs: $TOTAL_WARCS"
echo "Indexed: $INDEXED"
echo "Remaining: $((TOTAL_WARCS - INDEXED))"
```

### Find Recently Added WARCs

```bash
# WARCs added in last 24 hours
find /data/collections/COLLECTION-2024-11 -name "*.warc.gz" -mtime -1

# Check if they're indexed
for warc in $(find /data/collections/COLLECTION-2024-11 -name "*.warc.gz" -mtime -1); do
    cdxj="${warc}.cdxj"
    if [ -f "$cdxj" ]; then
        echo "✓ $warc (indexed)"
    else
        echo "✗ $warc (NOT indexed)"
    fi
done
```

### Monitor Processing Performance

```bash
# Watch real-time progress
watch -n 5 'ls /tmp/cdxj-processing/COLLECTION-2024-11/indexes/*.cdxj 2>/dev/null | wc -l'

# Check output size
du -sh /data/zipnum/COLLECTION-2024-11/
```

## See Also

- [pipeline-examples.md](pipeline-examples.md) - Complete pipeline examples
- [filter-blocklist.md](filter-blocklist.md) - Blocklist filtering
- [filter-excessive-urls.md](filter-excessive-urls.md) - Excessive URL filtering
- [cdxj-to-zipnum.md](cdxj-to-zipnum.md) - ZipNum conversion
