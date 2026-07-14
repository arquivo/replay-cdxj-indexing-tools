# Error Recovery

This guide covers how to recover from failures that leave partial or corrupted output, how to
resume interrupted runs, how to verify index integrity, and when to restart versus resume.

---

## Partial or Corrupted Output Files

### How partial files arise

A run may be interrupted by a signal, OOM, disk-full condition, or node failure. The tools in
this project write output sequentially, so a partial run produces a truncated file that is valid
up to the last complete line but missing everything written after the interruption.

A corrupted file is less common and usually means the disk or filesystem itself has an error.

### Detecting partial output

**Flat CDXJ files** — a well-formed file ends with a newline. Check:
```bash
tail -c 1 index.cdxj | xxd   # should be 0a (newline)
```

Count lines and compare against expected input size:
```bash
wc -l index.cdxj
```

**ZipNum index pairs** — the `.idx` file is a sorted line-oriented text file; apply the same
newline check. The `.cdxj.gz` shards are gzip streams; verify each shard:
```bash
for f in shards/*.cdxj.gz; do
    gzip -t "$f" && echo "OK: $f" || echo "CORRUPT: $f"
done
```

**Merged output** — verify the output is still sorted:
```bash
sort -c index.cdxj && echo "sorted OK" || echo "sort order violated"
```

### Recovering partial CDXJ files

If the partial file is complete up to the truncation point and you know which input chunks
have been processed, you can resume from the next unprocessed chunk (see
[Resuming Interrupted Runs](#resuming-interrupted-runs) below).

If the file is corrupt rather than just truncated, discard it and restart from scratch.

---

## Resuming Interrupted Runs

### flat-cdxj-to-zipnum

`flat-cdxj-to-zipnum` writes shards atomically via a temp-file + rename pattern. A shard is
either fully written and renamed into place, or still a temp file (`.tmp.*`). After a crash:

1. Remove any leftover temp files:
   ```bash
   rm -f /output/dir/.tmp.*
   ```
2. Check which shards were completed by listing the output directory and comparing against the
   expected shard count.
3. If some shards are missing, re-run with the same parameters. The tool overwrites any
   existing shards, so a full re-run is safe.

### merge-flat-cdxj

`merge-flat-cdxj` writes output to a single file. If interrupted mid-write the output file is
truncated. There is no built-in resume; you must re-run:

```bash
merge-flat-cdxj input1.cdxj input2.cdxj ... -o output.cdxj
```

For very large merges, split the inputs into smaller groups, merge each group, then do a
final merge of the intermediate files.

### cdxj-search / cdxj-extract-field

These tools are read-only and stateless. Re-running always produces correct output.

### arclist-to-path-index / path-index-to-redis

`path-index-to-redis` writes to Redis using pipelines. Redis operations are idempotent for
hash-set writes (HSET overwrites duplicate keys). Re-running the tool against the same input
is safe and will not corrupt the index; duplicates are simply overwritten.

If the tool crashed midway through a large input file, check the Redis hash for the expected
number of keys:
```bash
redis-cli HLEN <collection-key>
```

Then re-run the tool from the beginning; the previously written keys will be overwritten
with the same values.

---

## Verifying Index Integrity

### Flat CDXJ

```bash
# Check sort order
sort -c index.cdxj

# Check for malformed lines (must have at least surt + timestamp + JSON)
awk 'NF < 3 { print NR": "$0 }' index.cdxj | head

# Verify JSON on each line
python3 -c "
import json, sys
for i, line in enumerate(sys.stdin, 1):
    parts = line.rstrip().split(' ', 2)
    if len(parts) < 3:
        print(f'line {i}: missing JSON')
        continue
    try:
        json.loads(parts[2])
    except json.JSONDecodeError as e:
        print(f'line {i}: {e}')
" < index.cdxj
```

### ZipNum

```bash
# Verify all gzip shards
for f in shards/*.cdxj.gz; do gzip -t "$f" || echo "BAD: $f"; done

# Verify the .idx file is sorted
sort -c index.idx

# Spot-check: search for a known URL should return results
cdxj-search --url http://example.com/ index.idx
```

### Redis path index

```bash
# Count keys in collection
redis-cli HLEN <collection-key>

# Fetch a known key and verify the path
redis-cli HGET <collection-key> <arcfile-name>
```

---

## When to Restart vs. Resume

| Situation | Action |
|---|---|
| Truncated flat CDXJ from a known good split point | Resume from the next input chunk |
| Truncated flat CDXJ, unknown split point | Restart full run |
| Corrupt (non-truncated) CDXJ or gzip shard | Restart full run; discard corrupt file |
| `flat-cdxj-to-zipnum` crash with `.tmp.*` leftovers | Remove temp files, restart full run |
| `merge-flat-cdxj` partial output | Restart full run |
| `path-index-to-redis` partial write | Restart full run (idempotent) |
| Disk full during write | Free space, remove partial output, restart |
| OOM during large merge | Reduce `--workers` or split inputs; see [Performance Tuning](performance-tuning.md) |

### Rule of thumb

**Resume** only when:
- You have a clean boundary (e.g., fully written shards in a sharded workflow), **and**
- The partial output has been validated (sorted, complete lines, valid JSON/gzip).

**Restart** in all other cases. The tools are designed so that a clean re-run produces
identical output, making restart always safe.

---

## Common Error Messages

### `IOError: [Errno 28] No space left on device`

The output disk is full. Steps:
1. Free space or redirect output to a larger volume.
2. Remove any partial output files from the failed run.
3. Restart.

### `gzip.BadGzipFile` / `EOFError` reading a shard

A `.cdxj.gz` shard is corrupt or truncated. Identify the shard (logged in the error), delete
it, and re-run `flat-cdxj-to-zipnum` for the affected input range.

### `redis.exceptions.ConnectionError`

The Redis server is unreachable. Check the server is running and the host/port/password
parameters are correct. The tool can be re-run after fixing the connection; writes are
idempotent.

### `sort -c` reports "disorder"

The input to `merge-flat-cdxj` must be sorted. If one of the input files is out of order,
sort it first:
```bash
sort -o input_sorted.cdxj input.cdxj
```

---

## See Also

- [Performance Tuning](performance-tuning.md) — `--workers`, buffer sizes, and memory limits
- [Troubleshooting](troubleshooting.md) — general debugging tips
- [Pipeline Examples](pipeline-examples.md) — example workflows for large collections
