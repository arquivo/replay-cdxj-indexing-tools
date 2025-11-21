# System Architecture

Overview of the CDXJ indexing tools architecture and processing pipeline.

## Pipeline Overview

```
┌─────────────┐
│ WARC Files  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Parallel Indexing  │  (cdx-indexer + GNU parallel)
│  → CDXJ per WARC    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Add Collection     │  (addfield-to-flat-cdxj in parallel)
│  Field to records   │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Filter Blocklist    │  (filter-blocklist in parallel)
│ Spam, adult, legal  │  [CPU intensive - parallelized]
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Merge Indexes     │  (merge-flat-cdxj)
│   K-way merge       │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Filter Excessive    │  (filter-excessive-urls)
│ Crawler traps       │  [Requires merged data]
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ ZipNum Conversion   │  (flat-cdxj-to-zipnum)
│ Compressed shards   │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│      pywb           │
│   Web Replay        │
└─────────────────────┘
```

## Components

### 1. Indexing Stage

**Purpose:** Extract CDX records from WARC files

**Tools:**
- `cdx-indexer` (from pywb) - Creates CDXJ from WARC
- GNU `parallel` - Distributes indexing across CPU cores

**Output:** One `.cdxj` file per WARC file

**Key Features:**
- Parallel processing (default: all CPU cores)
- Atomic writes (`.tmp` files prevent corruption)
- Incremental mode (skip already-indexed files)

### 2. Collection Field Addition Stage

**Purpose:** Add collection identifier to all CDXJ records

**Tool:** `addfield-to-flat-cdxj`

**Key Features:**
- Parallel processing (before filtering for optimal performance)
- Adds `collection` field to all records
- Required for identifying record source
- Preserves all original data

**Performance:**
- ~500K-1M lines/second per core
- **13x speedup** with 16-core parallel processing
- Best practice: Run in parallel before filtering and merge

**Output:** Enriched CDXJ files with collection identifier

### 3. Blocklist Filter (Parallel)

**Purpose:** Remove unwanted content (spam, adult, legal removals)

**Tool:** `filter-blocklist`

**Key Features:**
- **Parallel processing:** Run on all CDXJ files before merge
- **CPU intensive:** Regex pattern matching benefits from parallelization
- **4x speedup:** Filtering before merge vs after merge
- SURT-based filtering
- Comment support in blocklist
- Verbose logging

**Blocklist Format:**
```
# Comment
^pt,spam,.*
^com,example,banned-path
```

**Performance:**
- **Critical optimization:** Filter BEFORE merge
- Parallel processing on individual files (16 cores = 16x speedup)
- CPU-bound operation benefits significantly from parallelization

**Output:** Filtered CDXJ files (one per input WARC)

### 4. Merge Stage

**Purpose:** Combine multiple filtered CDXJ files into one

**Tool:** `merge-flat-cdxj`

**Algorithm:** K-way merge using priority queue (heapq)

**Performance:**
- Streaming I/O (low memory usage)
- Efficient sorting (SURT-based)
- Buffer optimization (8KB default)
- Processes pre-filtered files (smaller, faster)

**Output:** Single merged CDXJ file, sorted by SURT

### 5. Excessive URL Filter

**Purpose:** Remove crawler traps and sites with excessive captures

**Tool:** `filter-excessive-urls`

**Why After Merge:**
- **Requires merged data:** Must count URLs across ALL files
- Cannot be parallelized (needs global URL counts)
- Must run on complete, merged dataset

**Modes:**
- `find` - Identify excessive URLs
- `remove` - Filter them out
- `auto` - One-pass combined operation

**Algorithm:**
1. Count URLs per SURT key across entire dataset
2. Identify those exceeding threshold
3. Remove excessive captures

**Default Threshold:** 1000 captures per URL

### 6. ZipNum Conversion

**Purpose:** Create compressed, sharded indexes for pywb

**Tool:** `flat-cdxj-to-zipnum`

**Output Structure:**
```
indexes/
├── cdx-00000.gz     # Compressed CDXJ shard
├── cdx-00001.gz
├── ...
└── summary.idx      # Summary index (uncompressed)
```

**Features:**
- Configurable shard size (default: 3000 lines)
- Gzip compression
- CDX summary generation
- pywb-compatible format

## Data Flow

### File Formats

**CDXJ (CDX JSON):**
```
com,example)/ 20240101120000 {"url": "http://example.com/", "mime": "text/html", ...}
```

**ZipNum:**
- Compressed shards: `cdx-NNNNN.gz`
- Summary index: `summary.idx` (first line of each shard)

### Storage Locations

**Default paths** (configurable):
```
/data/
├── collections/
│   └── COLLECTION-NAME/
│       └── *.warc.gz
├── cdxj_incremental/
│   └── COLLECTION-NAME/
│       └── indexes/
│           └── *.cdxj
├── blocklists/
│   └── arquivo-blocklist.txt
├── zipnum/
│   └── COLLECTION-NAME/
│       ├── cdx-*.gz
│       └── summary.idx
└── temp/
    └── processing/
```

## Processing Modes

### Full Collection Processing

Process entire collection from scratch:
```bash
./cdxj-index-collection AWP999
```

**Use when:**
- First-time processing
- Reprocessing with new filters
- Archive format changes

### Incremental Processing

Process only new/modified WARCs:
```bash
./cdxj-index-collection AWP999 --incremental
```

**Use when:**
- Daily crawl updates
- Continuous collection growth
- Timestamp-based detection

**Mechanism:**
- Compares CDXJ timestamp with WARC timestamp
- Skips if CDXJ is newer
- Processes new/modified only

### Unix Pipe Processing

Stream data through entire pipeline:
```bash
merge-flat-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o indexes/ -i - --compress
```

**Benefits:**
- No intermediate files
- Minimal disk I/O
- Parallel stage execution
- Memory efficient

## Performance Characteristics

### Bottlenecks

1. **Indexing:** I/O bound (reading WARCs)
2. **Merging:** CPU bound (comparison operations)
3. **Filtering:** CPU bound (regex matching)
4. **ZipNum:** I/O bound (compression writes)

### Optimization Strategies

**Parallel Processing:**
- Use `--jobs` to control parallelism
- Default: all available CPU cores
- Balance: I/O vs CPU workload

**Memory Usage:**
- Merge: O(k) where k = number of files
- Filter: O(1) streaming
- ZipNum: O(shard_size)

**Disk I/O:**
- Use fast storage (SSD) for temp files
- Pipe workflows reduce I/O
- Compression reduces storage

### Typical Performance

**Arquivo.pt production** (32-core server):
- Indexing: ~50 GB/hour (parallelized)
- Merging: ~1 GB/minute
- Filtering: ~500 MB/minute
- ZipNum: ~300 MB/minute (with compression)

**Daily incremental update:**
- Before: 3-5 hours (full reprocess)
- After: 35-75 minutes (incremental, 90% reduction)

## Error Handling

### Atomic Writes

**Problem:** Interrupted indexing leaves corrupted CDXJ

**Solution:** Write to `.tmp` files first
```bash
file.warc.gz → file.cdxj.tmp → file.cdxj
```

**Recovery:** Automatic cleanup of `.tmp` files on restart

### Validation

**Input validation:**
- Check file existence
- Verify CDXJ format
- Validate WARC structure

**Output validation:**
- SURT ordering verification
- Completeness checks
- Format compliance

## Dependencies

### System Requirements

- **Python:** 3.7+ (tested: 3.7-3.12)
- **GNU parallel:** For parallel indexing
- **pywb:** CDX indexer (`cdx-indexer`)

### Python Packages

```
pywb>=2.9.1    # CDX indexing and replay
```

See [installation.md](installation.md) for setup details.

## Extension Points

### Custom Filters

Add custom filtering logic:

```python
from replay_cdxj_indexing_tools.filter import blocklist

# Custom filter function
def custom_filter(cdxj_line):
    # Your logic here
    return should_keep

# Apply in pipeline
```

### Custom Processing

Extend `cdxj-index-collection`:

```bash
# Add custom stage
custom-filter -i input.cdxj -o output.cdxj

# Insert in pipeline
merge-flat-cdxj - *.cdxj | \
    custom-filter -i - | \
    filter-blocklist -i - -b blocklist.txt
```

## See Also

- [Quick Start](quick-start.md) - Get running quickly
- [Reference Implementation](reference-implementation.md) - Complete script guide
- [Pipeline Examples](pipeline-examples.md) - Real-world workflows
- [Troubleshooting](troubleshooting.md) - Common issues
