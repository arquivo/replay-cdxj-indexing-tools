# Documentation Hub

Welcome to the CDXJ Indexing Tools documentation. This guide covers everything from quick start to advanced usage.

## Getting Started

**New users start here:**

1. **[Quick Start](quick-start.md)** - Get running in 5 minutes
2. **[Installation](installation.md)** - Detailed setup guide
3. **[Reference Implementation](reference-implementation.md)** - Complete workflow script

## Core Documentation

### User Guides

- **[Quick Start](quick-start.md)** - 5-minute getting started guide
- **[Architecture](architecture.md)** - System design and pipeline overview
- **[Reference Implementation](reference-implementation.md)** - `cdxj-index-collection` complete guide
- **[Incremental Workflow](incremental-workflow.md)** - Daily/incremental updates
- **[Redis Integration](redis-integration.md)** - Path index to Redis setup guide
- **[Pipeline Examples](pipeline-examples.md)** - Real-world workflows
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions

### Tool Reference

Individual tool documentation:

- **[addfield-to-flat-cdxj](tools/addfield-to-flat-cdxj.md)** - Add custom JSON fields to CDXJ records
- **[merge-flat-cdxj](tools/merge-flat-cdxj.md)** - K-way merge of sorted flat CDXJ files
- **[filter-blocklist](tools/filter-blocklist.md)** - Filter by blocklist patterns
- **[filter-excessive-urls](tools/filter-excessive-urls.md)** - Remove crawler traps
- **[flat-cdxj-to-zipnum](tools/flat-cdxj-to-zipnum.md)** - Convert flat CDXJ to ZipNum format
- **[zipnum-to-flat-cdxj](tools/zipnum-to-flat-cdxj.md)** - Convert ZipNum back to flat CDXJ
- **[cdxj-search](tools/cdxj-search.md)** - Binary search for CDXJ/ZipNum indexes
- **[arclist-to-path-index](tools/arclist-to-path-index.md)** - Convert arclist to path index format
- **[arclist-index-to-redis](tools/arclist-index-to-redis.md)** - Complete arclist to Redis pipeline

## Documentation by Use Case

### "I want to..."

**...process my first collection:**
→ [Quick Start](quick-start.md) → [Reference Implementation](reference-implementation.md)

**...set up daily incremental updates:**
→ [Incremental Workflow](incremental-workflow.md)

**...understand how it works:**
→ [Architecture](architecture.md)

**...use individual tools in custom pipelines:**
→ [Pipeline Examples](pipeline-examples.md) → [Tool Reference](tools/)

**...fix an error:**
→ [Troubleshooting](troubleshooting.md)

**...deploy to production:**
→ [Reference Implementation](reference-implementation.md) → [Incremental Workflow](incremental-workflow.md)

## Processing Pipeline Overview

```
┌─────────────┐
│ WARC Files  │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Parallel Indexing  │ ← GNU parallel + cdx-indexer
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Parallel Addfield  │ ← addfield-to-flat-cdxj (optional)
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│   Merge Indexes     │ ← merge-flat-cdxj
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Filter Blocklist    │ ← filter-blocklist
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Filter Excessive    │ ← filter-excessive-urls
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ ZipNum Conversion   │ ← flat-cdxj-to-zipnum
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│      pywb           │
│   Web Replay        │
└─────────────────────┘
```

## Quick Command Reference

### Complete Pipeline (Recommended)

```bash
cdxj-index-collection COLLECTION-2024-11
```

### Individual Tools

```bash
# Add fields to CDXJ
addfield-to-flat-cdxj -i input.cdxj -o output.cdxj -f collection=COL-2024

# Merge CDXJ files
merge-flat-cdxj output.cdxj input1.cdxj input2.cdxj

# Filter blocklist
filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj

# Remove excessive URLs
filter-excessive-urls auto -i input.cdxj -o output.cdxj -n 1000

# Convert to ZipNum
flat-cdxj-to-zipnum -o indexes/ -i input.cdxj -n 3000 --compress

# Convert ZipNum back to flat CDXJ
zipnum-to-flat-cdxj -i indexes/index.idx > output.cdxj

# Search indexes
cdxj-search --url http://example.com/page index.cdxj

# Convert arclist to path index
arclist-to-path-index -d /data/arclists > pathindex.txt

# Load arclist directly to Redis
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v
```

### Unix Pipe Workflow

```bash
# CDXJ pipeline with field addition
addfield-to-flat-cdxj -i input.cdxj -f collection=COL-2024 | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o indexes/ -i - --compress

# CDXJ pipeline from merge
merge-flat-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o indexes/ -i - --compress

# Arclist to Redis pipeline
arclist-index-to-redis -d /data/arclists -k pathindex:branchA --clear -v
```

## Available Tools

| Tool | Purpose | Documentation |
|------|---------|---------------|
| `addfield-to-flat-cdxj` | Add custom JSON fields to CDXJ records | [docs](tools/addfield-to-flat-cdxj.md) |
| `merge-flat-cdxj` | Merge multiple sorted flat CDXJ files | [docs](tools/merge-flat-cdxj.md) |
| `filter-blocklist` | Remove blocked content | [docs](tools/filter-blocklist.md) |
| `filter-excessive-urls` | Remove crawler traps | [docs](tools/filter-excessive-urls.md) |
| `flat-cdxj-to-zipnum` | Convert flat CDXJ to ZipNum format | [docs](tools/flat-cdxj-to-zipnum.md) |
| `zipnum-to-flat-cdxj` | Convert ZipNum back to flat CDXJ | [docs](tools/zipnum-to-flat-cdxj.md) |
| `cdxj-search` | Binary search indexes | [docs](tools/cdxj-search.md) |
| `arclist-to-path-index` | Convert arclist to path index | [docs](tools/arclist-to-path-index.md) |
| `arclist-index-to-redis` | Arclist to Redis pipeline | [docs](tools/arclist-index-to-redis.md) |
| `cdxj-index-collection` | Complete pipeline script | [docs](reference-implementation.md) |

## Development

- **[Testing Guide](../tests/README.md)** - Running and writing tests, multi-version testing with Docker
- **Contributing** - How to contribute (coming soon)
- **API Reference** - Python API docs (coming soon)

### Quick Development Commands

```bash
# Run full CI suite locally
make ci

# Test on specific Python version (Docker)
make ci-py38

# Test all Python versions in parallel
make --jobs 10 ci-all
```

## Support

- **Issues:** [GitHub Issues](https://github.com/arquivo/replay-cdxj-indexing-tools/issues)
- **Troubleshooting:** [Troubleshooting Guide](troubleshooting.md)
- **Questions:** Open a discussion on GitHub

## License

MIT License - See [LICENSE](../LICENSE) file for details.
