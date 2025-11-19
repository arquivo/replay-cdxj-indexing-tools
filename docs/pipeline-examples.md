# Pipeline Examples

Complete end-to-end workflows for processing web archive indexes using replay-cdxj-indexing-tools.

## Table of Contents

1. [Unix Pipe Example](#unix-pipe-example)
2. [Basic Pipeline](#basic-pipeline)
3. [Production Pipeline](#production-pipeline)
4. [Parallel Processing](#parallel-processing)
5. [Incremental Updates](#incremental-updates)
6. [Quality Control](#quality-control)
7. [Large-Scale Processing](#large-scale-processing)

## Unix Pipe Example

### Complete Pipeline Using Unix Pipes

The most efficient way to process CDXJ data is using Unix pipes to connect all tools, avoiding intermediate files:

```bash
#!/bin/bash

# Configuration
CDXJ_FILES="/data/indexes/*.cdxj"
BLOCKLIST="/data/blocklists/arquivo-blocklist.txt"
EXCESSIVE_THRESHOLD=1000
OUTPUT_DIR="/data/zipnum/collection"

echo "=== Unix Pipe Pipeline ==="
echo "Processing: $CDXJ_FILES"
echo "Blocklist: $BLOCKLIST"
echo "Excessive threshold: $EXCESSIVE_THRESHOLD"
echo ""

# Complete pipeline using Unix pipes
# merge → filter blocklist → filter excessive URLs → convert to ZipNum
merge-cdxj - $CDXJ_FILES | \
    filter-blocklist -i - -b "$BLOCKLIST" -v | \
    filter-excessive-urls auto -i - -n $EXCESSIVE_THRESHOLD -v | \
    cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress

echo ""
echo "Pipeline complete!"
echo "Output: $OUTPUT_DIR"
echo "Shards: $(ls $OUTPUT_DIR/index.cdxj/*.gz 2>/dev/null | wc -l)"
```

**Advantages:**
- No intermediate files (saves disk space)
- Data streams through memory (faster processing)
- Pipeline parallelism (all tools run simultaneously)
- Simple and readable

**How it works:**
1. `merge-cdxj - $CDXJ_FILES` - Merges all CDXJ files and outputs to stdout
2. `filter-blocklist -i - -b blocklist.txt -v` - Reads from stdin, filters, outputs to stdout (with verbose stats)
3. `filter-excessive-urls auto -i - -n 1000 -v` - Reads from stdin, auto-finds and removes excessive URLs, outputs to stdout
4. `cdxj-to-zipnum -o dir -i - -n 3000` - Reads from stdin, converts to ZipNum format

### Real-World Arquivo.pt Example

Process a monthly collection using only pipes:

```bash
#!/bin/bash
set -e

MONTH="2024-11"
WARCS_DIR="/data/warcs/$MONTH"
INDEXES_DIR="/tmp/indexes_$MONTH"
BLOCKLIST="/data/blocklists/arquivo-spam.txt"
OUTPUT="/data/zipnum/$MONTH"

echo "Processing collection: $MONTH"

# Step 1: Index WARCs in parallel, output to temporary directory
echo "Step 1: Indexing WARCs..."
mkdir -p "$INDEXES_DIR"
find "$WARCS_DIR" -name "*.warc.gz" | \
    parallel -j $(nproc) \
    "cdx-indexer --postappend --cdxj {} -o $INDEXES_DIR/{/}.cdxj"

echo "  Created $(ls $INDEXES_DIR/*.cdxj | wc -l) index files"

# Step 2: Process through complete pipeline using pipes
echo "Step 2: Processing pipeline (merge→filter→filter→zipnum)..."
merge-cdxj - "$INDEXES_DIR"/*.cdxj | \
    tee >(wc -l | xargs echo "  After merge:") | \
    filter-blocklist -i - -b "$BLOCKLIST" | \
    tee >(wc -l | xargs echo "  After blocklist:") | \
    filter-excessive-urls auto -i - -n 1000 | \
    tee >(wc -l | xargs echo "  After excessive filter:") | \
    cdxj-to-zipnum -o "$OUTPUT" -i - -n 3000 --compress

# Cleanup
echo "Step 3: Cleanup..."
rm -rf "$INDEXES_DIR"

echo ""
echo "Complete! Output: $OUTPUT"
echo "Shards created: $(ls $OUTPUT/index.cdxj/*.gz | wc -l)"
```

### Monitoring Pipeline Progress

Add progress monitoring to pipe pipeline:

```bash
#!/bin/bash

INPUT_FILES="/data/indexes/*.cdxj"
BLOCKLIST="blocklist.txt"
OUTPUT_DIR="/data/zipnum"

# Count input lines for progress
total_lines=$(cat $INPUT_FILES | wc -l)
echo "Processing $total_lines total lines..."
echo ""

# Pipeline with progress bar using pv
merge-cdxj - $INPUT_FILES | \
    pv -l -s $total_lines -N "merge" | \
    filter-blocklist -i - -b "$BLOCKLIST" | \
    pv -l -N "blocklist" | \
    filter-excessive-urls auto -i - -n 1000 | \
    pv -l -N "excessive" | \
    cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress

echo "Done!"
```

Output shows real-time progress:
```
Processing 1523847 total lines...

merge:      1.52M 0:00:23 [65.2k/s] [=========>] 100%
blocklist:  1.48M 0:00:25 [59.1k/s] [=========>] 97%
excessive:  1.39M 0:00:27 [51.5k/s] [=========>] 91%
Done!
```

### Error Handling in Pipes

Proper error handling with `set -e` and `pipefail`:

```bash
#!/bin/bash
set -e          # Exit on error
set -o pipefail # Exit if any pipe command fails

INPUT_DIR="/data/indexes"
BLOCKLIST="/data/blocklists/arquivo.txt"
OUTPUT_DIR="/data/zipnum/output"

# Trap errors
trap 'echo "ERROR: Pipeline failed at line $LINENO"; exit 1' ERR

echo "Starting pipeline..."

# Run pipeline
if merge-cdxj - "$INPUT_DIR"/*.cdxj | \
   filter-blocklist -i - -b "$BLOCKLIST" | \
   filter-excessive-urls auto -i - -n 1000 | \
   cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress; then
    echo "SUCCESS: Pipeline completed"
    exit 0
else
    echo "FAILED: Pipeline exited with error code $?"
    exit 1
fi
```

### Saving Intermediate Results While Using Pipes

Use `tee` to save intermediate results without breaking the pipe:

```bash
#!/bin/bash

TEMP_DIR="/data/temp"
mkdir -p "$TEMP_DIR"

# Pipeline that saves intermediate results
merge-cdxj - /data/indexes/*.cdxj | \
    tee "$TEMP_DIR/01-merged.cdxj" | \
    filter-blocklist -i - -b blocklist.txt | \
    tee "$TEMP_DIR/02-after-blocklist.cdxj" | \
    filter-excessive-urls auto -i - -n 1000 | \
    tee "$TEMP_DIR/03-after-excessive.cdxj" | \
    cdxj-to-zipnum -o /data/zipnum -i - -n 3000 --compress

echo "Intermediate files saved in $TEMP_DIR/"
```

### Multi-Stage Pipeline with Statistics

Collect statistics at each stage:

```bash
#!/bin/bash

OUTPUT_DIR="/data/zipnum/collection"
STATS_FILE="/data/stats/pipeline-$(date +%Y%m%d-%H%M%S).txt"

echo "=== Pipeline Statistics ===" | tee "$STATS_FILE"
echo "Started: $(date)" | tee -a "$STATS_FILE"
echo "" | tee -a "$STATS_FILE"

# Pipeline with statistics collection
merge-cdxj - /data/indexes/*.cdxj | \
    tee >(wc -l | xargs printf "Lines after merge: %d\n" | tee -a "$STATS_FILE") | \
    filter-blocklist -i - -b blocklist.txt -v 2>&1 | \
    tee >(grep "blocked" | tee -a "$STATS_FILE") | \
    filter-excessive-urls auto -i - -n 1000 -v 2>&1 | \
    tee >(grep "Removed" | tee -a "$STATS_FILE") | \
    cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress

echo "" | tee -a "$STATS_FILE"
echo "Finished: $(date)" | tee -a "$STATS_FILE"
echo "Shards: $(ls $OUTPUT_DIR/index.cdxj/*.gz | wc -l)" | tee -a "$STATS_FILE"
echo "" | tee -a "$STATS_FILE"

cat "$STATS_FILE"
```

Output example:
```
=== Pipeline Statistics ===
Started: Mon Nov 18 14:23:45 2024

Lines after merge: 1523847
Kept 1489234 lines, blocked 34613 lines (2.3%)
Removed 87456 lines from 234 excessive URLs
Final output: 1401778 lines

Finished: Mon Nov 18 14:28:12 2024
Shards: 468
```

## Basic Pipeline

### Simple Merge and Convert

Process a few CDXJ files:

```bash
#!/bin/bash

# Merge files
merge-cdxj merged.cdxj file1.cdxj file2.cdxj file3.cdxj

# Convert to ZipNum
cdxj-to-zipnum -o indexes -i merged.cdxj -n 3000 --compress

echo "Done! Indexes in indexes/"
```

### One-Line Pipeline

Using stdin/stdout to avoid intermediate files:

```bash
merge-cdxj - *.cdxj | cdxj-to-zipnum -o indexes -i - -n 3000 --compress
```

## Production Pipeline

### Complete Arquivo.pt Workflow

Full production pipeline with all quality controls:

```bash
#!/bin/bash
set -e  # Exit on error

# Configuration
WARCS_DIR="/data/warcs/collection"
TEMP_DIR="/data/temp/collection"
BLOCKLIST="/data/blocklists/arquivo-blocklist.txt"
EXCESSIVE_THRESHOLD=1000
OUTPUT_DIR="/data/zipnum/collection"

echo "=== Arquivo.pt CDXJ Processing Pipeline ==="
echo "Started: $(date)"

# Step 1: Index WARCs in parallel
echo ""
echo "Step 1: Indexing WARC files..."
find "$WARCS_DIR" -name "*.warc.gz" | \
    parallel -j $(nproc) \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"

# Step 2: Merge all indexes
echo ""
echo "Step 2: Merging indexes..."
merge-cdxj "$TEMP_DIR/merged.cdxj" "$WARCS_DIR"/*.cdxj

echo "  Merged $(wc -l < $TEMP_DIR/merged.cdxj) lines"

# Step 3: Filter blocklist (spam, adult, etc.)
echo ""
echo "Step 3: Filtering blocklist..."
filter-blocklist \
    -i "$TEMP_DIR/merged.cdxj" \
    -b "$BLOCKLIST" \
    -o "$TEMP_DIR/clean1.cdxj" \
    -v

# Step 4: Find excessive URLs
echo ""
echo "Step 4: Finding excessive URLs..."
filter-excessive-urls find \
    -i "$TEMP_DIR/clean1.cdxj" \
    -n $EXCESSIVE_THRESHOLD \
    > "$TEMP_DIR/excessive.txt"

echo "  Found $(grep -v '^#' $TEMP_DIR/excessive.txt | wc -l) excessive URLs"

# Step 5: Filter excessive URLs
echo ""
echo "Step 5: Filtering excessive URLs..."
filter-excessive-urls remove \
    -i "$TEMP_DIR/clean1.cdxj" \
    -b "$TEMP_DIR/excessive.txt" \
    -o "$TEMP_DIR/clean2.cdxj"

echo "  Final: $(wc -l < $TEMP_DIR/clean2.cdxj) lines"

# Step 6: Convert to ZipNum
echo ""
echo "Step 6: Converting to ZipNum..."
cdxj-to-zipnum \
    -o "$OUTPUT_DIR" \
    -i "$TEMP_DIR/clean2.cdxj" \
    -n 3000 \
    --compress

echo "  Created $(ls $OUTPUT_DIR/index.cdxj/ | wc -l) shards"

# Cleanup
echo ""
echo "Cleaning up temporary files..."
rm -f "$WARCS_DIR"/*.cdxj
rm -rf "$TEMP_DIR"

echo ""
echo "=== Pipeline Complete ==="
echo "Finished: $(date)"
echo "Output: $OUTPUT_DIR"
```

### Streaming Production Pipeline

Same workflow using pipelines (no intermediate files):

```bash
#!/bin/bash
set -e

WARCS_DIR="/data/warcs/collection"
BLOCKLIST="/data/blocklists/arquivo-blocklist.txt"
EXCESSIVE_LIST="/data/temp/excessive.txt"
OUTPUT_DIR="/data/zipnum/collection"

echo "=== Streaming Pipeline ==="

# Pre-generate excessive URLs list (can't do this in pipeline)
echo "Finding excessive URLs..."
merge-cdxj - "$WARCS_DIR"/*.cdxj | \
    filter-blocklist -i - -b "$BLOCKLIST" | \
    filter-excessive-urls find -i - -n 1000 > "$EXCESSIVE_LIST"

echo "Found $(grep -v '^#' $EXCESSIVE_LIST | wc -l) excessive URLs"

# Stream through entire pipeline
echo "Processing pipeline..."
merge-cdxj - "$WARCS_DIR"/*.cdxj | \
    filter-blocklist -i - -b "$BLOCKLIST" | \
    filter-excessive-urls remove -i - -b "$EXCESSIVE_LIST" | \
    cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress

echo "Done! Output in $OUTPUT_DIR"
```

## Parallel Processing

**Note:** These examples use GNU `parallel` - a powerful tool for executing jobs in parallel.

**Installation:**
```bash
# Debian/Ubuntu
sudo apt-get install parallel

# RedHat/CentOS/Fedora
sudo yum install parallel

# macOS
brew install parallel
```

**First-time setup:** Accept the citation notice:
```bash
parallel --citation
```

**Key options:**
- `-j N` - Run N jobs in parallel (e.g., `-j 8` or `-j $(nproc)` for all CPUs)
- `--bar` - Show progress bar
- `{/}` - Basename of input (filename without path)
- `{}` - Full input argument

**Learn more:** `man parallel` or visit https://www.gnu.org/software/parallel/

### Parallel WARC Indexing

Index WARCs in parallel, then merge:

```bash
#!/bin/bash

WARCS_DIR="/data/warcs"
INDEXES_DIR="/data/indexes"
OUTPUT="/data/final/index.cdxj"

# Create indexes directory
mkdir -p "$INDEXES_DIR"

# Index all WARCs in parallel (8 jobs)
echo "Indexing WARCs in parallel..."
find "$WARCS_DIR" -name "*.warc.gz" | \
    parallel -j 8 --bar \
    "cdx-indexer --postappend --cdxj {} -o $INDEXES_DIR/{/}.cdxj"

# Merge all indexes
echo "Merging $(ls $INDEXES_DIR/*.cdxj | wc -l) indexes..."
merge-cdxj "$OUTPUT" "$INDEXES_DIR"/*.cdxj

echo "Done! Merged index: $OUTPUT"
```

### Advanced Parallel Examples

**Parallel with custom job count:**
```bash
# Use 75% of available CPUs
find /data/warcs -name "*.warc.gz" | \
    parallel -j 75% \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"
```

**Parallel with logging:**
```bash
# Log each job's output
find /data/warcs -name "*.warc.gz" | \
    parallel -j $(nproc) --joblog /tmp/parallel.log \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"

# View log
column -t /tmp/parallel.log
```

**Parallel with retries on failure:**
```bash
# Retry failed jobs up to 3 times
find /data/warcs -name "*.warc.gz" | \
    parallel -j 8 --retries 3 \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"
```

**Parallel with different output directory:**
```bash
# Output to different directory structure
find /data/warcs -name "*.warc.gz" | \
    parallel -j $(nproc) \
    "cdx-indexer --postappend --cdxj {} -o /data/indexes/{/}.cdxj"
# {/} = basename (filename without path)
```

**Parallel with memory limit per job:**
```bash
# Limit each job to 2GB RAM
find /data/warcs -name "*.warc.gz" | \
    parallel -j 8 --memfree 2G \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"
```

**Parallel with dry-run (test before running):**
```bash
# See what commands will be executed
find /data/warcs -name "*.warc.gz" | \
    parallel --dry-run -j 8 \
    "cdx-indexer --postappend --cdxj {} -o {}.cdxj"
```

### Parallel Processing by Date

Process collections by date in parallel:

```bash
#!/bin/bash

COLLECTIONS_DIR="/data/collections"
OUTPUT_DIR="/data/zipnum"

# Process each year in parallel
for year in 2020 2021 2022 2023; do
    {
        echo "Processing $year..."
        
        # Pipeline for this year
        merge-cdxj - "$COLLECTIONS_DIR/$year"/*.cdxj | \
            filter-blocklist -i - -b blocklist.txt | \
            filter-excessive-urls auto -i - -n 1000 | \
            cdxj-to-zipnum -o "$OUTPUT_DIR/$year" -i - -n 3000 --compress
        
        echo "Completed $year"
    } &
done

# Wait for all to complete
wait

echo "All years processed!"
```

## Incremental Updates

### Add New Captures to Existing Index

Merge new captures with existing index:

```bash
#!/bin/bash

OLD_INDEX="/data/indexes/collection.cdxj"
NEW_CAPTURES="/data/new/captures.cdxj"
UPDATED_INDEX="/data/indexes/collection_updated.cdxj"
ZIPNUM_DIR="/data/zipnum/collection"

echo "Merging old index with new captures..."
merge-cdxj "$UPDATED_INDEX" "$OLD_INDEX" "$NEW_CAPTURES"

echo "Converting to ZipNum..."
cdxj-to-zipnum -o "$ZIPNUM_DIR" -i "$UPDATED_INDEX" -n 3000 --compress

# Update symbolic link for pywb
ln -sf "$ZIPNUM_DIR" /data/current_index

echo "Index updated!"
```

### Separate Indexes Per Time Period

Maintain separate indexes for different periods:

```bash
#!/bin/bash

NEW_CAPTURES="/data/captures/2024-11.cdxj"
ZIPNUM_BASE="/data/zipnum"

# Process new month
year_month="2024-11"

echo "Processing $year_month..."
filter-blocklist -i "$NEW_CAPTURES" -b blocklist.txt -o clean.cdxj
filter-excessive-urls auto -i clean.cdxj -o final.cdxj -n 1000
cdxj-to-zipnum -o "$ZIPNUM_BASE/$year_month" -i final.cdxj -n 3000 --compress

# Update pywb config to include all months
cat > /data/pywb/config.yaml <<EOF
collections:
  arquivo:
    index_paths:
$(find $ZIPNUM_BASE -name "index.cdxj.gz" | sed 's/^/      - /')
    archive_paths:
      - /data/warcs/
EOF

echo "Updated pywb config with new index"
```

## Quality Control

### Detailed Statistics Pipeline

Track statistics at each stage:

```bash
#!/bin/bash

INPUT="/data/merged.cdxj"
TEMP_DIR="/data/temp"

echo "=== Quality Control Pipeline ==="

# Initial count
initial=$(wc -l < "$INPUT")
echo "Initial: $initial lines"

# After blocklist filtering
filter-blocklist -i "$INPUT" -b blocklist.txt -o "$TEMP_DIR/after_blocklist.cdxj" -v
after_blocklist=$(wc -l < "$TEMP_DIR/after_blocklist.cdxj")
echo "After blocklist: $after_blocklist lines (removed $((initial - after_blocklist)))"

# After excessive URL filtering
filter-excessive-urls auto -i "$TEMP_DIR/after_blocklist.cdxj" -o "$TEMP_DIR/final.cdxj" -n 1000 -v
final=$(wc -l < "$TEMP_DIR/final.cdxj")
echo "After excessive: $final lines (removed $((after_blocklist - final)))"

# Total removed
total_removed=$((initial - final))
pct=$(echo "scale=2; $total_removed * 100 / $initial" | bc)
echo ""
echo "Summary:"
echo "  Started with: $initial lines"
echo "  Ended with: $final lines"
echo "  Removed: $total_removed lines ($pct%)"

# Convert
cdxj-to-zipnum -o /data/zipnum -i "$TEMP_DIR/final.cdxj" -n 3000 --compress
echo "  Shards: $(ls /data/zipnum/index.cdxj/ | wc -l)"
```

### Validation Pipeline

Validate data at each stage:

```bash
#!/bin/bash

validate_cdxj() {
    local file=$1
    local name=$2
    
    echo "Validating $name..."
    
    # Check file exists and is not empty
    if [ ! -s "$file" ]; then
        echo "  ERROR: File is empty or missing!"
        return 1
    fi
    
    # Check format (basic)
    if ! head -1 "$file" | grep -q '{"'; then
        echo "  WARNING: First line doesn't look like CDXJ"
    fi
    
    # Check if sorted
    if ! sort -c "$file" 2>/dev/null; then
        echo "  ERROR: File is not sorted!"
        return 1
    fi
    
    lines=$(wc -l < "$file")
    echo "  OK: $lines lines, properly formatted and sorted"
}

# Pipeline with validation
merge-cdxj merged.cdxj /data/*.cdxj
validate_cdxj merged.cdxj "merged"

filter-blocklist -i merged.cdxj -b blocklist.txt -o clean1.cdxj
validate_cdxj clean1.cdxj "after blocklist"

filter-excessive-urls auto -i clean1.cdxj -o clean2.cdxj -n 1000
validate_cdxj clean2.cdxj "after excessive filtering"

cdxj-to-zipnum -o indexes -i clean2.cdxj -n 3000 --compress
echo "ZipNum conversion complete"
```

## Large-Scale Processing

### Process 100M+ Lines

Optimized for very large collections:

```bash
#!/bin/bash
set -e

COLLECTION="/data/huge_collection"
TEMP="/data/temp"
OUTPUT="/data/zipnum/huge"

echo "=== Large-Scale Processing (100M+ lines) ==="

# Use larger buffer sizes and shard sizes
export BUFFER_SIZE=$((10 * 1024 * 1024))  # 10MB

# Step 1: Merge with progress
echo "Merging..."
pv "$COLLECTION"/*.cdxj | merge-cdxj "$TEMP/merged.cdxj" -

# Step 2: Filter (using two-pass for memory efficiency)
echo "Finding excessive URLs..."
filter-excessive-urls find -i "$TEMP/merged.cdxj" -n 5000 > "$TEMP/excessive.txt"

echo "Filtering..."
filter-blocklist -i "$TEMP/merged.cdxj" -b blocklist.txt | \
    filter-excessive-urls remove -i - -b "$TEMP/excessive.txt" \
    > "$TEMP/clean.cdxj"

# Step 3: Convert with larger shards
echo "Converting to ZipNum (large shards)..."
cdxj-to-zipnum -o "$OUTPUT" -i "$TEMP/clean.cdxj" -n 10000 --compress

echo "Processing complete!"
echo "  Input: $(wc -l < $TEMP/merged.cdxj) lines"
echo "  Output: $(wc -l < $TEMP/clean.cdxj) lines"
echo "  Shards: $(ls $OUTPUT/index.cdxj/ | wc -l)"
```

### Distributed Processing

Process on multiple machines:

```bash
#!/bin/bash
# Run this on each machine with different WORKER_ID

WORKER_ID=$1
TOTAL_WORKERS=$2

WARCS_DIR="/data/warcs"
TEMP_DIR="/data/temp/worker_$WORKER_ID"
OUTPUT_DIR="/data/indexes/worker_$WORKER_ID"

# Get this worker's slice of WARCs
all_warcs=($(find "$WARCS_DIR" -name "*.warc.gz" | sort))
worker_warcs=()

for ((i=$WORKER_ID; i<${#all_warcs[@]}; i+=$TOTAL_WORKERS)); do
    worker_warcs+=("${all_warcs[$i]}")
done

echo "Worker $WORKER_ID processing ${#worker_warcs[@]} WARCs"

# Index this worker's WARCs
mkdir -p "$TEMP_DIR"
for warc in "${worker_warcs[@]}"; do
    echo "Indexing $warc..."
    cdx-indexer --postappend --cdxj "$warc" -o "$TEMP_DIR/$(basename $warc).cdxj"
done

# Merge this worker's indexes
merge-cdxj "$OUTPUT_DIR/part_$WORKER_ID.cdxj" "$TEMP_DIR"/*.cdxj

echo "Worker $WORKER_ID complete: $OUTPUT_DIR/part_$WORKER_ID.cdxj"
```

Then on main machine:

```bash
#!/bin/bash
# Collect from all workers and final processing

echo "Collecting from all workers..."
merge-cdxj /data/final/merged.cdxj /data/indexes/worker_*/*.cdxj

echo "Final processing..."
filter-blocklist -i /data/final/merged.cdxj -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    cdxj-to-zipnum -o /data/zipnum/final -i - -n 3000 --compress

echo "All workers complete!"
```

## Performance Monitoring

### Monitor Pipeline Progress

```bash
#!/bin/bash

INPUT="/data/large.cdxj"
OUTPUT_DIR="/data/zipnum"

# Monitor in background
(
    while true; do
        if [ -d "$OUTPUT_DIR/index.cdxj" ]; then
            shards=$(ls "$OUTPUT_DIR/index.cdxj" 2>/dev/null | wc -l)
            echo "$(date '+%H:%M:%S'): $shards shards created"
        fi
        sleep 10
    done
) &

MONITOR_PID=$!

# Run pipeline
merge-cdxj - /data/*.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    cdxj-to-zipnum -o "$OUTPUT_DIR" -i - -n 3000 --compress

# Stop monitoring
kill $MONITOR_PID

echo "Pipeline complete!"
```

## See Also

- [merge-cdxj.md](merge-cdxj.md) - Merge tool documentation
- [filter-blocklist.md](filter-blocklist.md) - Blocklist filter documentation  
- [filter-excessive-urls.md](filter-excessive-urls.md) - Excessive URL filter documentation
- [cdxj-to-zipnum.md](cdxj-to-zipnum.md) - ZipNum converter documentation
