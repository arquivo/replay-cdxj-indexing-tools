# zipnum-repair-idx - Repair ZipNum .idx Keys

Repair the `.idx` of a ZipNum index built with flat-cdxj-to-zipnum 1.3.0 or earlier, without rebuilding the shards.

Those versions cut each `.idx` key at the first `{` of the block's first CDXJ line. A SURT URL key that contains `{` (templated URLs such as `/?s={search_term_string}` or `/-/{{avatar_url}}`) was truncated mid-URL and lost its timestamp, so the `.idx` is not sorted: pywb's binary search can load the wrong blocks near those keys, and loaders that require a sorted `.idx` reject it. The shards and the `.loc` are correct.

A correct key is `<urlkey> <timestamp>` and has a space. For every key without one, the tool reads that block (shard, offset, length: one gzip member), takes `<urlkey> <timestamp>` from its first line and writes it as the key. Every other line is copied unchanged, so only the affected blocks are read.

## Command-Line Usage

### Basic Syntax

```bash
zipnum-repair-idx -i INDEX_FILE -o OUTPUT_FILE [--base-dir DIR] [--loc FILE]
```

### Examples

**Check whether an index is affected:**
```bash
awk -F'\t' 'index($1, " ") == 0' indexes/index.idx | head
```

**Write the repaired index to a new file:**
```bash
zipnum-repair-idx -i indexes/index.idx -o indexes/index.fixed.idx
```

**Repair in place, with the shards in another directory:**
```bash
zipnum-repair-idx -i index.idx -o index.idx --base-dir /data/shards --loc index.loc
```

**Check the result is sorted:**
```bash
cut -f1 indexes/index.fixed.idx | sed 's/$/\t/' | LC_ALL=C sort -c
```

### Options

- `-i, --input FILE` - Input `.idx` file (required)
- `-o, --output FILE` - Output `.idx` file; may be the input file (required)
- `--base-dir DIR` - Base directory for shard files (default: idx file directory)
- `--loc FILE` - Custom `.loc` file path (default: `<index>.loc` next to the `.idx`)

## Output

The repaired `.idx` is written to a temporary file in the output directory and renamed over `-o` when complete, so an interrupted run leaves the original untouched.

A summary goes to stderr:

```
Repaired 789045 key(s); wrote indexes/index.fixed.idx
```

## Exit Status

- `0` - The `.idx` was written and is sorted
- `1` - A block could not be read (the error names the `.idx` line and shard; nothing is written), or the written `.idx` is still not sorted

## Python API

```python
from replay_cdxj_indexing_tools.zipnum.repair_idx import repair_idx

repaired, unsorted = repair_idx(
    idx_path='indexes/index.idx',
    output_path='indexes/index.fixed.idx',
)
```

## See Also

- [flat-cdxj-to-zipnum](flat-cdxj-to-zipnum.md) - Build ZipNum indexes
- [zipnum-to-flat-cdxj](zipnum-to-flat-cdxj.md) - Convert ZipNum back to flat CDXJ
