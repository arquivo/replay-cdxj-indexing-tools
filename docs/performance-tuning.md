# Performance Tuning

This guide explains how to tune throughput and memory use for large-scale CDXJ indexing
workloads. It covers the `--workers` flag, I/O buffer sizes, memory estimates, and
recommended profiles for common hardware configurations.

---

## Workers (`--workers`)

Several tools use worker threads or processes to parallelize CPU-intensive steps.

### flat-cdxj-to-zipnum

```bash
flat-cdxj-to-zipnum --workers 4 input.cdxj -o /output/
```

`--workers` controls the number of parallel gzip compression threads. Each worker compresses
one shard at a time.

**Guidelines:**

| Available cores | Recommended `--workers` |
|---|---|
| 2 | 1–2 |
| 4 | 2–4 |
| 8 | 4–6 |
| 16+ | 6–8 (diminishing returns beyond ~8) |

Gzip compression is CPU-bound. Using more workers than CPU cores will not improve throughput
and will increase context-switching overhead. Leave at least one core free for the I/O
dispatcher and the OS.

**Memory per worker:** each worker holds one uncompressed shard in memory while compressing.
Default shard size is configurable; with the default settings each worker uses roughly
`shard_lines × avg_line_bytes`. For typical CDXJ lines (~200 bytes) and a shard of 3000
lines that is ~600 KB per worker. Eight workers therefore use ~5 MB for shard buffers, which
is negligible.

### arclist-index-to-redis / path-index-to-redis

These tools use a Redis pipeline batch size (not `--workers`). Tune the batch size with
`--batch-size` (default 1000 records per pipeline flush). Larger batches reduce round-trip
overhead; smaller batches reduce memory pressure and make progress visible sooner.

---

## Buffer Sizes

I/O buffer sizes control how much data is read or written in each system call. Larger buffers
reduce syscall overhead but consume more memory.

### merge-flat-cdxj

```bash
merge-flat-cdxj --buffer-size 4194304 input1.cdxj input2.cdxj -o output.cdxj
```

`--buffer-size` (bytes, default 1 MB = 1048576) applies to each open file handle. With `k`
input files the total buffered I/O memory is approximately `(k + 1) × buffer_size`.

**Examples:**

| Files | Buffer size | Buffer memory |
|---|---|---|
| 10 | 1 MB (default) | ~11 MB |
| 10 | 4 MB | ~44 MB |
| 100 | 1 MB | ~101 MB |
| 100 | 4 MB | ~404 MB |

For large merges (100+ files) on memory-constrained systems, keep the default 1 MB buffer.
On systems with 32 GB+ RAM, 4–8 MB buffers improve throughput by reducing the number of
reads per file.

### filter-excessive-urls / addfield-to-flat-cdxj

Both tools accept `--buffer-size` with the same semantics. The default (1 MB) is appropriate
for most workloads. Increase to 4–8 MB when processing files from a high-latency NFS or
object-storage mount to amortize per-read latency.

### cdxj-search

`cdxj-search` uses binary search for flat CDXJ files and block-level seeking for ZipNum
indexes; it does not hold large buffers. No buffer tuning is needed for search workloads.

---

## Memory Estimates

### merge-flat-cdxj

The merge uses a min-heap of size `k` (one entry per input file). The heap itself is tiny
(a few hundred bytes per entry). Dominant memory use is the I/O buffers described above.

Total approximate memory:
```
(k + 1) × buffer_size   [I/O buffers]
+ k × ~500 bytes         [heap entries + Python object overhead]
```

### flat-cdxj-to-zipnum

The tool streams input line by line. It accumulates lines for the current shard in memory
before flushing/compressing. Memory use is approximately:

```
shard_lines × avg_line_size   [current shard buffer]
+ workers × shard_buffer      [parallel compression buffers]
+ idx_entries × ~100 bytes    [in-memory .idx entries]
```

With default settings (3000-line shards, 200-byte average lines, 4 workers):
```
~600 KB (current shard)
+ 4 × 600 KB (compression workers)
+ idx_entries × 100 bytes
≈ 3 MB + idx overhead
```

The `.idx` overhead grows linearly with the number of shards. For 10,000 shards it is
approximately 1 MB.

### path-index-to-redis / arclist-index-to-redis

Memory use is dominated by the Redis client's send and receive buffers. With the default
batch size of 1000 records and average record size of ~200 bytes, each pipeline flush holds
~200 KB in memory. This is constant regardless of input file size.

---

## Hardware Profiles

### Laptop / Developer workstation (4–8 cores, 16 GB RAM)

Recommended settings for a single large collection:

```bash
# Convert to ZipNum
flat-cdxj-to-zipnum --workers 4 input.cdxj -o /output/

# Merge 20 sorted CDXJ files
merge-flat-cdxj --buffer-size 2097152 part-*.cdxj -o merged.cdxj

# Filter and addfield
filter-excessive-urls --buffer-size 2097152 input.cdxj -o filtered.cdxj
```

### CI / Build server (4 cores, 8 GB RAM)

Keep defaults. Avoid large buffer sizes:

```bash
flat-cdxj-to-zipnum --workers 2 input.cdxj -o /output/
merge-flat-cdxj part-*.cdxj -o merged.cdxj   # default 1 MB buffers
```

### Production indexing server (32+ cores, 128+ GB RAM)

For maximum throughput when merging hundreds of files:

```bash
flat-cdxj-to-zipnum --workers 8 input.cdxj -o /output/
merge-flat-cdxj --buffer-size 8388608 part-*.cdxj -o merged.cdxj
```

For Redis indexing pipelines, increase batch size:

```bash
path-index-to-redis --batch-size 5000 path-index.txt \
    --host redis-host --port 6379 --collection AWP-999
```

### Network-attached storage (NFS / S3-compatible mount)

High-latency mounts benefit from large read buffers to amortize per-read latency:

```bash
merge-flat-cdxj --buffer-size 16777216 /mnt/nfs/part-*.cdxj -o output.cdxj
```

For writes to slow storage, pipe output through a local temp file and then copy:

```bash
merge-flat-cdxj part-*.cdxj -o /tmp/merged.cdxj && \
    mv /tmp/merged.cdxj /mnt/nfs/merged.cdxj
```

---

## Disk I/O Tips

- **Local SSD**: no special tuning required; default settings are optimal.
- **Spinning disk (HDD)**: sequential reads are fast; avoid random seeks. Use `merge-flat-cdxj`
  (sequential) rather than repeatedly scanning individual files.
- **NFS/CIFS**: increase `--buffer-size` to 8–16 MB and use a local temp directory for
  intermediate files.
- **Compressed inputs** (`.cdxj.gz`): `flat-cdxj-to-zipnum` handles gzip-compressed input
  transparently. Decompression adds CPU overhead; ensure `--workers` is set high enough to
  saturate available cores.

---

## Monitoring During a Run

Use `--verbose` to print per-file progress and summary statistics:

```bash
flat-cdxj-to-zipnum --workers 4 --verbose input.cdxj -o /output/
merge-flat-cdxj --verbose part-*.cdxj -o merged.cdxj
cdxj-search --url http://example.com/ --verbose index.cdxj
```

For long-running merges, watch disk usage:

```bash
watch -n 5 df -h /output
```

---

## See Also

- [Error Recovery](error-recovery.md) — resuming interrupted runs and verifying integrity
- [Pipeline Examples](pipeline-examples.md) — end-to-end examples for large collections
- [Troubleshooting](troubleshooting.md) — diagnosing slowness and other issues
