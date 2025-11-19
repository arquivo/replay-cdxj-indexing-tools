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
- **[Pipeline Examples](pipeline-examples.md)** - Real-world workflows
- **[Troubleshooting](troubleshooting.md)** - Common issues and solutions

### Tool Reference

Individual tool documentation:

- **[merge-cdxj](tools/merge-cdxj.md)** - K-way merge of sorted CDXJ files
- **[filter-blocklist](tools/filter-blocklist.md)** - Filter by blocklist patterns
- **[filter-excessive-urls](tools/filter-excessive-urls.md)** - Remove crawler traps
- **[cdxj-to-zipnum](tools/cdxj-to-zipnum.md)** - Convert to ZipNum format

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
│   Merge Indexes     │ ← merge-cdxj
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
│ ZipNum Conversion   │ ← cdxj-to-zipnum
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
# Merge CDXJ files
merge-cdxj output.cdxj input1.cdxj input2.cdxj

# Filter blocklist
filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj

# Remove excessive URLs
filter-excessive-urls auto -i input.cdxj -o output.cdxj -n 1000

# Convert to ZipNum
cdxj-to-zipnum -o indexes/ -i input.cdxj -n 3000 --compress
```

### Unix Pipe Workflow

```bash
merge-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    cdxj-to-zipnum -o indexes/ -i - --compress
```

## Available Tools

| Tool | Purpose | Documentation |
|------|---------|---------------|
| `merge-cdxj` | Merge multiple sorted CDXJ files | [docs](tools/merge-cdxj.md) |
| `filter-blocklist` | Remove blocked content | [docs](tools/filter-blocklist.md) |
| `filter-excessive-urls` | Remove crawler traps | [docs](tools/filter-excessive-urls.md) |
| `cdxj-to-zipnum` | Convert to ZipNum format | [docs](tools/cdxj-to-zipnum.md) |
| `cdxj-index-collection` | Complete pipeline script | [docs](reference-implementation.md) |

## Development

- **[Testing Guide](../tests/README.md)** - Running and writing tests
- **Contributing** - How to contribute (coming soon)
- **API Reference** - Python API docs (coming soon)

## Support

- **Issues:** [GitHub Issues](https://github.com/arquivo/cdxj-incremental-indexing/issues)
- **Troubleshooting:** [Troubleshooting Guide](troubleshooting.md)
- **Questions:** Open a discussion on GitHub

## License

MIT License - See [LICENSE](../LICENSE) file for details.
