# CDXJ Incremental Indexing Tools

[![Tests](https://github.com/arquivo/cdxj-incremental-indexing/workflows/Tests/badge.svg)](https://github.com/arquivo/cdxj-incremental-indexing/actions)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-ready tools for processing web archive CDXJ indexes at scale. Features parallel indexing, efficient merging, content filtering, and ZipNum conversion for pywb.

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
git clone https://github.com/arquivo/cdxj-incremental-indexing.git
cd cdxj-incremental-indexing
python3 -m venv venv && source venv/bin/activate
pip install -e .

# Process a collection
cdxj-index-collection COLLECTION-2024-11
```

**Output:** Complete ZipNum indexes ready for pywb

## Documentation

📖 **[Complete Documentation](docs/)**

### Essential Guides

- **[Quick Start](docs/quick-start.md)** - Get running in 5 minutes
- **[Installation](docs/installation.md)** - Detailed setup guide
- **[Reference Implementation](docs/reference-implementation.md)** - Complete workflow script guide
- **[Incremental Workflow](docs/incremental-workflow.md)** - Daily updates (90% faster)
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues

### Tool Reference

- **[merge-cdxj](docs/tools/merge-cdxj.md)** - Merge sorted CDXJ files
- **[filter-blocklist](docs/tools/filter-blocklist.md)** - Content filtering
- **[filter-excessive-urls](docs/tools/filter-excessive-urls.md)** - Crawler trap removal
- **[cdxj-to-zipnum](docs/tools/cdxj-to-zipnum.md)** - ZipNum conversion

## Usage Examples

### Complete Pipeline (Recommended)

```bash
# Full collection processing
cdxj-index-collection COLLECTION-2024-11

# Daily incremental update (only new WARCs)
cdxj-index-collection COLLECTION-2024-11 --incremental

# Custom configuration
cdxj-index-collection COLLECTION-2024-11 \
    --jobs 32 \
    --threshold 5000 \
    --blocklist /path/to/blocklist.txt
```

### Individual Tools

```bash
# Merge CDXJ files
merge-cdxj merged.cdxj file1.cdxj file2.cdxj file3.cdxj

# Filter blocklist
filter-blocklist -i input.cdxj -b blocklist.txt -o filtered.cdxj

# Remove excessive URLs
filter-excessive-urls auto -i input.cdxj -o clean.cdxj -n 1000

# Convert to ZipNum
cdxj-to-zipnum -o indexes/ -i clean.cdxj -n 3000 --compress
```

### Unix Pipe Workflow

```bash
# Process everything in one pipeline
merge-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    cdxj-to-zipnum -o indexes/ -i - --compress
```

## Processing Pipeline

```
WARC Files → Parallel Index → Merge → Filter Blocklist → Filter Excessive → ZipNum → pywb
```

Each stage is optimized for throughput and memory efficiency.

## Requirements

- **Python:** 3.7+ (tested on 3.7-3.12)
- **pywb:** ≥2.9.1 (for `cdx-indexer`)
- **GNU parallel:** For parallel WARC indexing
- **Disk space:** 2-3x input WARC size

## Installation

```bash
# System dependencies (Ubuntu/Debian)
sudo apt-get install python3-dev parallel

# Python package
git clone https://github.com/arquivo/cdxj-incremental-indexing.git
cd cdxj-incremental-indexing
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

See [Installation Guide](docs/installation.md) for other platforms.

## Performance

**Arquivo.pt production benchmarks** (32-core server, 100GB collection):

| Operation | Time | Throughput |
|-----------|------|------------|
| Full processing | 2-4 hours | ~30 GB/hour |
| Incremental update | 35-75 min | **90% reduction** |
| Parallel indexing | 45 min | ~50 GB/hour |
| Merge + Filter | 15 min | ~1 GB/min |

## Testing

```bash
# Python tests (91 tests)
pytest tests/ -v

# Shell script tests (15 tests)
bash tests/test_process_collection_simple.sh

# All tests
pytest tests/ && bash tests/test_process_collection_simple.sh
```

See [Testing Guide](tests/README.md) for details.

## Contributing

Contributions welcome! Please see:
- [Architecture Overview](docs/architecture.md) - System design
- [Testing Guide](tests/README.md) - How to run tests

## Production Use

Used in production at [Arquivo.pt](https://arquivo.pt) (Portuguese Web Archive) for processing millions of WARC files daily.

## License

MIT License - See [LICENSE](LICENSE) file

## Support

- **Documentation:** [docs/](docs/)
- **Issues:** [GitHub Issues](https://github.com/arquivo/cdxj-incremental-indexing/issues)
- **Troubleshooting:** [Troubleshooting Guide](docs/troubleshooting.md)

## Authors

Arquivo.pt Team - [contacto@arquivo.pt](mailto:contacto@arquivo.pt)

---

**Quick Links:** [Documentation](docs/) • [Quick Start](docs/quick-start.md) • [Installation](docs/installation.md) • [Troubleshooting](docs/troubleshooting.md)
