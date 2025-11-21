# Quick Start Guide

Get started with CDXJ indexing tools in 5 minutes.

## Installation

```bash
# Clone repository
git clone https://github.com/arquivo/replay-cdxj-indexing-tools.git
cd replay-cdxj-indexing-tools

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install package
pip install -e .
```

## Process Your First Collection

### Option 1: Complete Pipeline (Recommended)

Process a full WARC collection automatically:

```bash
./cdxj-index-collection AWP999
```

This single command:
1. Indexes all WARC files in parallel
2. Merges indexes
3. Filters blocked content
4. Removes excessive URLs
5. Converts to ZipNum format

**Output:** `AWP999.zip` ready for pywb

### Option 2: Individual Tools

Process step-by-step for custom workflows:

```bash
# 1. Index WARCs (using pywb's cdx-indexer)
cdx-indexer collection/*.warc.gz > indexes/index.cdxj

# 2. Merge multiple indexes
merge-flat-cdxj merged.cdxj indexes/*.cdxj

# 3. Filter blocklist
filter-blocklist -i merged.cdxj -b blocklist.txt -o filtered.cdxj

# 4. Remove excessive URLs
filter-excessive-urls auto -i filtered.cdxj -o clean.cdxj -n 1000

# 5. Convert to ZipNum
flat-cdxj-to-zipnum -o zipnum/ -i clean.cdxj -n 3000 --compress
```

### Option 3: Unix Pipe Workflow (Most Efficient)

Process everything in a single pipeline:

```bash
merge-flat-cdxj - indexes/*.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls auto -i - -n 1000 | \
    flat-cdxj-to-zipnum -o zipnum/ -i - -n 3000 --compress
```

## Verify Installation

Check that all tools are available:

```bash
merge-flat-cdxj --version
filter-blocklist --version
filter-excessive-urls --version
flat-cdxj-to-zipnum --version
```

## Next Steps

- **Daily Updates:** [Incremental Workflow Guide](incremental-workflow.md)
- **Advanced Pipelines:** [Pipeline Examples](pipeline-examples.md)
- **Production Setup:** [Reference Implementation](reference-implementation.md)
- **Tool Details:** See [tools/](tools/) directory

## Common Issues

### Command not found
```bash
# Make sure virtual environment is activated
source venv/bin/activate
```

### Import errors
```bash
# Reinstall package
pip install -e .
```

### Permission denied
```bash
# Make scripts executable
chmod +x cdxj-index-collection
```

For more troubleshooting, see [Troubleshooting Guide](troubleshooting.md).
