# Troubleshooting Guide

Common issues and solutions for CDXJ indexing tools.

## Installation Issues

### Command not found

**Symptom:**
```bash
$ merge-cdxj
-bash: merge-cdxj: command not found
```

**Solutions:**

1. **Activate virtual environment:**
   ```bash
   source venv/bin/activate
   ```

2. **Reinstall package:**
   ```bash
   pip install -e .
   ```

3. **Check PATH:**
   ```bash
   which python
   # Should show: /path/to/venv/bin/python
   ```

### Import errors

**Symptom:**
```python
ModuleNotFoundError: No module named 'replay_cdxj_indexing_tools'
```

**Solutions:**

1. **Install in development mode:**
   ```bash
   cd /path/to/cdxj-incremental-indexing
   pip install -e .
   ```

2. **Check installation:**
   ```bash
   pip list | grep replay-cdxj
   # Should show: replay-cdxj-indexing-tools
   ```

3. **Python version:**
   ```bash
   python --version
   # Should be 3.7+
   ```

### pywb installation fails

**Symptom:**
```
ERROR: Could not build wheels for pywb
```

**Solutions:**

1. **Install system dependencies (Debian/Ubuntu):**
   ```bash
   sudo apt-get update
   sudo apt-get install python3-dev build-essential
   ```

2. **Install system dependencies (RHEL/CentOS):**
   ```bash
   sudo yum install python3-devel gcc
   ```

3. **Upgrade pip:**
   ```bash
   pip install --upgrade pip setuptools wheel
   ```

## Runtime Issues

### GNU parallel not found

**Symptom:**
```
[ERROR] GNU parallel not found. Install: sudo apt-get install parallel
```

**Solutions:**

1. **Ubuntu/Debian:**
   ```bash
   sudo apt-get install parallel
   ```

2. **RHEL/CentOS:**
   ```bash
   sudo yum install parallel
   ```

3. **macOS:**
   ```bash
   brew install parallel
   ```

4. **From source:**
   ```bash
   wget https://ftpmirror.gnu.org/parallel/parallel-latest.tar.bz2
   tar xjf parallel-latest.tar.bz2
   cd parallel-*
   ./configure && make && sudo make install
   ```

### cdx-indexer not found

**Symptom:**
```
cdx-indexer: command not found
```

**Solutions:**

1. **Install pywb:**
   ```bash
   pip install pywb
   ```

2. **Verify installation:**
   ```bash
   which cdx-indexer
   # Should show path to cdx-indexer
   ```

3. **Check virtual environment:**
   ```bash
   source venv/bin/activate
   which cdx-indexer
   ```

### Permission denied

**Symptom:**
```bash
$ ./cdxj-index-collection
-bash: ./cdxj-index-collection: Permission denied
```

**Solution:**
```bash
chmod +x cdxj-index-collection
chmod +x *.sh
```

## Processing Issues

### No WARC files found

**Symptom:**
```
[ERROR] No WARC files found in collection directory
```

**Solutions:**

1. **Check directory:**
   ```bash
   ls /data/collections/COLLECTION-2024-11/
   ```

2. **Verify WARC extension:**
   ```bash
   # Script looks for *.warc.gz
   # Rename if needed:
   rename 's/\.warc$/.warc.gz/' *.warc
   ```

3. **Custom collections dir:**
   ```bash
   ./cdxj-index-collection COLLECTION-2024-11 \
       --collections-dir /custom/path
   ```

### Out of disk space

**Symptom:**
```
No space left on device
```

**Solutions:**

1. **Check disk space:**
   ```bash
   df -h
   ```

2. **Clean temp files:**
   ```bash
   rm -rf /tmp/cdxj-processing/*
   ```

3. **Use different temp location:**
   ```bash
   ./cdxj-index-collection COLLECTION-2024-11 \
       --temp-dir /mnt/large-disk/temp
   ```

4. **Estimate space needed:**
   ```bash
   # WARC size
   du -sh /data/collections/COLLECTION-2024-11/
   # Need 2-3x for processing
   ```

### Memory errors

**Symptom:**
```
MemoryError
Killed
```

**Solutions:**

1. **Check available memory:**
   ```bash
   free -h
   ```

2. **Reduce parallel jobs:**
   ```bash
   ./cdxj-index-collection COLLECTION-2024-11 --jobs 8
   ```

3. **Increase swap:**
   ```bash
   # Temporary
   sudo swapoff -a
   sudo dd if=/dev/zero of=/swapfile bs=1G count=32
   sudo chmod 600 /swapfile
   sudo mkswap /swapfile
   sudo swapon /swapfile
   ```

4. **Process in batches:**
   ```bash
   # Split collection into smaller chunks
   for batch in batch-*; do
       ./cdxj-index-collection "$batch"
   done
   ```

### Corrupted WARC files

**Symptom:**
```
Error reading WARC file
gzip: invalid compressed data
```

**Solutions:**

1. **Identify corrupted files:**
   ```bash
   for f in *.warc.gz; do
       zcat "$f" | head -c 1000 > /dev/null 2>&1 || \
           echo "Corrupted: $f"
   done
   ```

2. **Skip corrupted files:**
   ```bash
   # Move to separate directory
   mkdir corrupted
   mv bad-file.warc.gz corrupted/
   ```

3. **Attempt recovery:**
   ```bash
   # Try to extract partial data
   zcat -f corrupted.warc.gz > recovered.warc
   gzip recovered.warc
   ```

### Blocklist not found

**Symptom:**
```
[WARNING] Blocklist not found: /data/blocklists/arquivo-blocklist.txt
[WARNING] Proceeding without blocklist filtering
```

**Solutions:**

1. **Create blocklist directory:**
   ```bash
   mkdir -p /data/blocklists
   ```

2. **Use custom blocklist:**
   ```bash
   ./cdxj-index-collection COLLECTION-2024-11 \
       --blocklist /path/to/your-blocklist.txt
   ```

3. **Disable blocklist (not recommended):**
   ```bash
   # Warning: This will process ALL content
   # Modify script or use empty blocklist
   touch /tmp/empty-blocklist.txt
   ./cdxj-index-collection COLLECTION-2024-11 \
       --blocklist /tmp/empty-blocklist.txt
   ```

## Performance Issues

### Indexing is too slow

**Symptom:** Indexing takes many hours for small collections

**Solutions:**

1. **Increase parallel jobs:**
   ```bash
   # Check available cores
   nproc
   
   # Use more cores
   ./cdxj-index-collection COLLECTION-2024-11 --jobs 32
   ```

2. **Check I/O bottleneck:**
   ```bash
   # Monitor disk usage
   iostat -x 1
   
   # Use faster storage for temp files
   ./cdxj-index-collection COLLECTION-2024-11 \
       --temp-dir /fast-ssd/temp
   ```

3. **Profile indexing:**
   ```bash
   # Time single WARC
   time cdx-indexer single.warc.gz > test.cdxj
   ```

### Merge takes too long

**Symptom:** Merge stage hangs or is very slow

**Solutions:**

1. **Check number of files:**
   ```bash
   ls indexes/*.cdxj | wc -l
   # If >10000, consider merging in batches
   ```

2. **Batch merge:**
   ```bash
   # Merge in groups of 1000
   for i in {0..10}; do
       start=$((i * 1000))
       merge-cdxj batch-$i.cdxj indexes/*.cdxj | \
           head -n $((start + 1000)) | tail -n 1000
   done
   
   # Final merge of batches
   merge-cdxj final.cdxj batch-*.cdxj
   ```

3. **Increase buffer size:**
   ```python
   # Edit merge_sorted_files.py
   # Change default buffer_size from 8192 to 65536
   ```

### High memory usage

**Symptom:** System becomes unresponsive, swap usage high

**Solutions:**

1. **Monitor memory:**
   ```bash
   # Real-time monitoring
   watch -n 1 free -h
   
   # Per-process memory
   ps aux --sort=-%mem | head -20
   ```

2. **Limit parallel jobs:**
   ```bash
   # Reduce from default
   ./cdxj-index-collection COLLECTION-2024-11 --jobs 8
   ```

3. **Use incremental mode:**
   ```bash
   # Process only new files
   ./cdxj-index-collection COLLECTION-2024-11 --incremental
   ```

## Data Issues

### SURT ordering errors

**Symptom:**
```
AssertionError: CDXJ not in SURT order
```

**Solutions:**

1. **Verify CDXJ format:**
   ```bash
   head -10 problematic.cdxj
   # Should start with SURT key like: com,example)/
   ```

2. **Re-index with pywb:**
   ```bash
   cdx-indexer --sort file.warc.gz > fixed.cdxj
   ```

3. **Manual sort:**
   ```bash
   LC_ALL=C sort -u input.cdxj > sorted.cdxj
   ```

### Duplicate entries

**Symptom:** Same URL appears multiple times with identical timestamps

**Solutions:**

1. **Remove duplicates:**
   ```bash
   sort -u input.cdxj > deduplicated.cdxj
   ```

2. **During merge:**
   ```bash
   # merge-cdxj automatically handles duplicates
   merge-cdxj output.cdxj input1.cdxj input2.cdxj
   ```

### Invalid JSON in CDXJ

**Symptom:**
```
JSONDecodeError: Expecting value
```

**Solutions:**

1. **Find problematic lines:**
   ```bash
   cat problematic.cdxj | while read line; do
       echo "$line" | python3 -c "import json,sys; json.loads(sys.stdin.read().split(' ', 2)[2])" \
           || echo "Bad line: $line"
   done
   ```

2. **Filter invalid lines:**
   ```bash
   grep -v "invalid-pattern" input.cdxj > clean.cdxj
   ```

3. **Re-index from source:**
   ```bash
   cdx-indexer source.warc.gz > reindexed.cdxj
   ```

## Integration Issues

### Cron job not running

**Symptom:** Scheduled processing doesn't execute

**Solutions:**

1. **Check cron logs:**
   ```bash
   sudo tail -f /var/log/cron
   # or
   sudo tail -f /var/log/syslog | grep CRON
   ```

2. **Test cron command manually:**
   ```bash
   # Copy command from crontab and run
   /opt/scripts/daily-update.sh
   ```

3. **Verify crontab:**
   ```bash
   crontab -l
   # Check syntax, timing, user
   ```

4. **Environment variables:**
   ```bash
   # Cron has minimal environment
   # Set PATH and activate venv in script:
   #!/bin/bash
   export PATH="/usr/local/bin:/usr/bin:/bin"
   source /opt/cdxj-incremental-indexing/venv/bin/activate
   ```

### pywb not finding indexes

**Symptom:** pywb returns "404 Not Found" for archived URLs

**Solutions:**

1. **Check ZipNum location:**
   ```bash
   ls /data/zipnum/COLLECTION-2024-11/
   # Should have: cdx-*.gz and summary.idx
   ```

2. **Verify pywb config:**
   ```yaml
   # config.yaml
   collections:
     collection-name:
       index_paths: /data/zipnum/COLLECTION-2024-11/
   ```

3. **Test index directly:**
   ```bash
   cdx-server http://example.com
   ```

4. **Rebuild ZipNum:**
   ```bash
   cdxj-to-zipnum -o /data/zipnum/COLLECTION-2024-11/ \
       -i final.cdxj -n 3000 --compress
   ```

## Test Failures

### pytest failures

**Symptom:**
```
FAILED tests/test_merge.py::test_merge - AssertionError
```

**Solutions:**

1. **Run single test:**
   ```bash
   pytest tests/test_merge.py::test_merge -v
   ```

2. **Check test environment:**
   ```bash
   # Ensure clean environment
   pip install -e .
   pytest tests/ -v
   ```

3. **Update dependencies:**
   ```bash
   pip install --upgrade pytest pywb
   ```

### Shell test failures

**Symptom:**
```bash
$ bash tests/test_process_collection_simple.sh
✗ Test failed
```

**Solutions:**

1. **Check script syntax:**
   ```bash
   bash -n cdxj-index-collection
   ```

2. **Run with debug:**
   ```bash
   bash -x tests/test_process_collection_simple.sh
   ```

3. **Verify dependencies:**
   ```bash
   which parallel
   which cdx-indexer
   ```

## Getting Help

### Collect diagnostic information

```bash
# System info
uname -a
python --version
pip list

# Disk space
df -h

# Memory
free -h

# Check dependencies
which parallel
which cdx-indexer
merge-cdxj --version

# Test configuration
./cdxj-index-collection --help
```

### Enable verbose logging

```bash
# Verbose mode for tools
filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj -v

# Debug mode for script
bash -x ./cdxj-index-collection COLLECTION-2024-11
```

### Report issues

When reporting issues, include:

1. **Command used:**
   ```bash
   ./cdxj-index-collection COLLECTION-2024-11 --jobs 32
   ```

2. **Error message:**
   ```
   [ERROR] Full error message here
   ```

3. **Environment:**
   - OS and version
   - Python version
   - Disk space and memory
   - Number of WARC files

4. **Logs:**
   ```bash
   # Capture full output
   ./cdxj-index-collection COLLECTION-2024-11 2>&1 | tee error.log
   ```

## See Also

- [Quick Start](quick-start.md) - Initial setup
- [Installation](installation.md) - Detailed installation
- [Reference Implementation](reference-implementation.md) - Script documentation
- [Architecture](architecture.md) - System design
