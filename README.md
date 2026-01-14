# CDXJ Incremental Indexing Tools

[![Tests](https://github.com/arquivo/replay-cdxj-indexing-tools/actions/workflows/tests.yml/badge.svg)](https://github.com/arquivo/replay-cdxj-indexing-tools/actions/workflows/tests.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)

Tools for processing web archive CDXJ indexes at scale. Features parallel indexing, efficient merging, content filtering, and ZipNum conversion for pywb.

## Features

- ⚡ **Parallel WARC indexing** with GNU parallel
- 🔄 **Efficient k-way merging** of sorted CDXJ files  
- 🚫 **Blocklist filtering** (spam, adult content, legal removals)
- 🎯 **Excessive URL detection** (crawler traps, spam sites)
- 📦 **ZipNum conversion** for pywb web replay
- 🔁 **Incremental processing** for daily updates (90% time reduction)
- 🛡️ **Atomic write protection** prevents data corruption
- 🔧 **Unix pipe workflows** for efficient processing

## Quick Start

```bash
# Clone and install
git clone https://github.com/arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools
python3 -m venv venv && source venv/bin/activate
pip install -e .

# Process a collection
cdxj-index-collection AWP-999
```

**Output:** Complete ZipNum indexes ready for pywb

## Documentation

📖 **[Complete Documentation](docs/)**

### Essential Guides

- **[Quick Start](docs/quick-start.md)** - Get running quickly
- **[Installation](docs/installation.md)** - Detailed setup guide
- **[Reference Implementation](docs/reference-implementation.md)** - Complete workflow script guide
- **[Incremental Workflow](docs/incremental-workflow.md)** - Daily updates (90% faster)
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues

### Tool Reference

- **[addfield-to-flat-cdxj](docs/tools/addfield-to-flat-cdxj.md)** - Add custom JSON fields to CDXJ records
- **[merge-flat-cdxj](docs/tools/merge-flat-cdxj.md)** - Merge sorted flat CDXJ files
- **[filter-blocklist](docs/tools/filter-blocklist.md)** - Content filtering
- **[filter-excessive-urls](docs/tools/filter-excessive-urls.md)** - Crawler trap removal
- **[flat-cdxj-to-zipnum](docs/tools/flat-cdxj-to-zipnum.md)** - Convert flat CDXJ to ZipNum format
- **[zipnum-to-flat-cdxj](docs/tools/zipnum-to-flat-cdxj.md)** - Convert ZipNum back to flat CDXJ
- **[cdxj-search](docs/tools/cdxj-search.md)** - Binary search for CDXJ/ZipNum indexes
- **[cdxj-extract-field](docs/tools/cdxj-extract-field.md)** - Extract JSON fields from CDXJ records
- **[arclist-to-path-index](docs/tools/arclist-to-path-index.md)** - Convert arclist files to path index format
- **[arclist-index-to-redis](docs/tools/arclist-index-to-redis.md)** - Complete arclist to Redis pipeline
- **[cdxj-index-collection](docs/reference-implementation.md)** - Complete collection processing pipeline

## Usage Examples

### Complete Pipeline (Recommended)

```bash
# Full collection processing
cdxj-index-collection AWP-999

# Daily incremental update (only new WARCs)
cdxj-index-collection AWP-999 --incremental

# Custom configuration
cdxj-index-collection AWP-999 \
    --jobs 32 \
    --threshold 5000 \
    --blocklist /path/to/blocklist.txt
```

### Individual Tools

```bash
# Merge CDXJ files
merge-flat-cdxj merged.cdxj file1.cdxj file2.cdxj file3.cdxj

# Filter blocklist
filter-blocklist -i input.cdxj -b blocklist.txt -o filtered.cdxj

# Remove excessive URLs
filter-excessive-urls auto -i input.cdxj -o clean.cdxj -n 1000

# Convert to ZipNum
flat-cdxj-to-zipnum -o indexes/ -i clean.cdxj -n 3000 --compress
```

### Unix Pipe Workflow

```bash
# Process everything in one pipeline
merge-flat-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o indexes/ -i - --compress
```

### Docker Usage

```bash
# Process CDXJ files with Docker
docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools \
    merge-flat-cdxj /data/output/merged.cdxj /data/input/*.cdxj

# Full pipeline with Docker
docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools \
    sh -c "merge-flat-cdxj - /data/input/*.cdxj | \
           filter-blocklist -i - -b /data/blocklist.txt | \
           filter-excessive-urls auto -i - -n 1000 | \
           flat-cdxj-to-zipnum -o /data/output/ -i - --compress"

# Convert to ZipNum
docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools \
    flat-cdxj-to-zipnum -i /data/input/file.cdxj -o /data/output/ -s 100 -c 3000

# Collection processing
docker run -v /path/to/collections:/data arquivo/replay-cdxj-indexing-tools \
    cdxj-index-collection AWP-999 --incremental
```

## Processing Pipeline

```
WARC Files → Parallel Index → Add Collection → Filter Blocklist (Parallel) → Merge → Filter Excessive → ZipNum → pywb
```

**Key Optimizations:**
- **Collection field added in parallel** after indexing
- **Blocklist filtering parallelized** (CPU intensive, 4x faster before merge)
- **Excessive URL filtering** runs after merge (requires complete dataset)

Each stage is optimized for throughput and memory efficiency.

## Requirements

- **Python:** 3.7+ (tested on 3.7-3.12)
- **pywb:** ≥2.9.1 (for `cdx-indexer`)
- **GNU parallel:** For parallel WARC indexing

## Installation

### Local Installation

```bash
# System dependencies (Ubuntu/Debian)
sudo apt-get install python3-dev parallel

# Python package
git clone https://github.com/arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

### Docker Installation

```bash
# Pull from Docker Hub
docker pull arquivo/replay-cdxj-indexing-tools:latest

# Or build locally
docker build -t arquivo/replay-cdxj-indexing-tools .

# Run with volume mount
docker run -v /path/to/data:/data arquivo/replay-cdxj-indexing-tools merge-flat-cdxj --help
```

See [Installation Guide](docs/installation.md) for other platforms.

## Performance

**Optimized for production use:**

- Parallel processing scales with available CPU cores
- Incremental mode significantly reduces processing time (90% reduction)
- Efficient memory usage with streaming algorithms
- Fast I/O with optimized buffering

## Testing

```bash
make test
```

See [Testing Guide](tests/README.md) for details.

## Contributing

Contributions welcome! Please see:
- [Architecture Overview](docs/architecture.md) - System design
- [Testing Guide](tests/README.md) - How to run tests

## Production Use

Used in production at [Arquivo.pt](https://arquivo.pt) (Portuguese Web Archive) for processing millions of WARC files daily.

## License

GPL-3.0 License - See [LICENSE](LICENSE) file

## Support

- **Documentation:** [docs/](docs/)
- **Issues:** [GitHub Issues](https://github.com/arquivo/replay-cdxj-indexing-tools/issues)
- **Troubleshooting:** [Troubleshooting Guide](docs/troubleshooting.md)

## Authors

Arquivo.pt Team - [contacto@arquivo.pt](mailto:contacto@arquivo.pt)

---

**Quick Links:** [Documentation](docs/) • [Quick Start](docs/quick-start.md) • [Installation](docs/installation.md) • [Troubleshooting](docs/troubleshooting.md)
