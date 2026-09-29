# cdxj-stats

Aggregate capture statistics from a CDXJ stream using pluggable sinks, in a single pass over the input.

## Purpose

This tool reads a CDXJ stream from stdin or a file and feeds it to one or more "sink" plugins, each writing its own CSV of aggregated statistics. Built-in sinks:

- **mimetype** - Count of records per mimetype
- **domain-host** - Per-SURT-host capture counts (total, status-200, oldest/latest capture)
- **domain-etld1** - Same as domain-host, but grouped by registered domain / eTLD+1 (e.g. `example.co.uk` instead of `www.example.co.uk`)

Third-party sinks can be added without touching this repo: sinks are discovered via the `replay_cdxj_indexing_tools.sinks` Python entry-point group, so any installed package registering an entry point in that group is automatically available to `--sink`.

The domain sinks stream: because the input is assumed to be SURT-sorted (e.g. via `merge-flat-cdxj`), all records for a given domain are contiguous, so each domain sink only ever holds one domain's accumulator in memory, regardless of corpus size. The `mimetype` sink keeps its (small-cardinality) counts fully in memory.

## Installation

The tool is included with the replay-cdxj-indexing-tools package:

```bash
pip install replay-cdxj-indexing-tools
```

## Quick Start

```bash
cat index.cdxj | cdxj-stats --sink mimetype --out-dir stats/
```

Writes `stats/mimetype.csv`.

## Usage

### Multiple Sinks in One Pass

Running several sinks together means the (potentially large, multi-year) corpus is only read once:

```bash
merge-flat-cdxj - *.cdxj \
  | filter-blocklist -i - -b blocklist.txt \
  | cdxj-stats --sink mimetype,domain-host,domain-etld1 --out-dir stats/
```

Writes `stats/mimetype.csv`, `stats/domain-host.csv`, `stats/domain-etld1.csv`.

### Per-Sink Options

Each sink can be configured independently with `--sink-<name> key=value ...`. All built-in sinks support `group-by=year` or `group-by=collection`, which adds a grouping column and emits one row per (key, group) pair instead of one combined row per key:

```bash
cdxj-stats -i index.cdxj \
  --sink mimetype,domain-host \
  --sink-mimetype group-by=year \
  --sink-domain-host group-by=year \
  --out-dir stats/
```

When `group-by` is set for a sink, its output filename gets a `-<group-by>` suffix, e.g. `stats/domain-host-year.csv`.

### TSV Output

```bash
cdxj-stats -i index.cdxj --sink domain-host --field-separator "\t" --out-dir stats/
```

`--field-separator` applies to every sink's output in the run.

### Verbose Mode

```bash
cat index.cdxj | cdxj-stats --sink mimetype --out-dir stats/ --verbose
```

Output to stderr:
```
# Processed 100000 CDXJ lines
# Sink mimetype: wrote 42 rows
```

## Output Format

### mimetype

```
mime,count
text/html,50000
image/jpeg,12000
```

With `group-by=year`: `mime,year,count`.

### domain-host / domain-etld1

```
domain,total_captures,captures_status_200,oldest_capture,latest_capture
"pt,exemplo,www)",120,110,20180101000000,20230601000000
```

With `group-by=year`: `domain,year,total_captures,captures_status_200,oldest_capture,latest_capture`, one row per (domain, year).

`captures_status_200` counts records whose JSON `status` field is `200`. `oldest_capture`/`latest_capture` are the min/max timestamp seen for that row (within the year, when grouped).

## Top-N Filtering

There's no built-in top-N option; every sink exports every row it sees. To keep only the top 100 rows of a sink's output by its count column, use a plain shell pipeline (adjust `-k` to the count column of the sink you're using; fields are 1-indexed for `sort`):

```bash
(head -1 stats/mimetype.csv; tail -n +2 stats/mimetype.csv | sort -t, -k2 -nr | head -100) \
  > stats/mimetype-top100.csv
```

## Scaling Out

`cdxj-stats` intentionally does not parallelize sinks within one process: a multi-year corpus should only be read/decompressed once, and the domain sinks' streaming flush-on-key-change logic depends on strictly ordered, single-threaded delivery. To speed up a very large run, shard the corpus (by year or collection) and run several `cdxj-stats` processes in parallel, one per shard, each writing to its own `--out-dir`; then sum the resulting per-sink CSVs (e.g. with a small pandas/awk groupby) to get full-corpus totals.

### Running Sinks in Parallel via `tee`

If a single run is CPU-bound on one core (e.g. piping a large merge directly into `cdxj-stats` with several `--sink` names), fan the stream out to one `cdxj-stats` process per sink using `tee` and process substitution, so each sink's parsing runs on its own core:

```bash
merge-flat-cdxj - /data/indexes_cdx/*.cdxj | \
  tee >(cdxj-stats --sink mimetype --out-dir stats/) \
      >(cdxj-stats --sink domain-host --out-dir stats/) | \
  cdxj-stats --sink domain-etld1 --out-dir stats/
```

This requires bash (for `>(...)` process substitution). Each process parses every line independently, so total CPU-seconds increase roughly with the number of sinks — but wall-clock time drops proportionally as long as spare cores are available, since the work is now spread across processes instead of serialized in one.

Each branch's `--sink`/`--sink-<name>` options are independent, so the same pattern also covers running a sink's plain and `group-by=year` variants side by side. A single `cdxj-stats` process can't run the same sink twice with different options (one instance per name in `--sink`), so give the normal and year-grouped variant of each sink its own branch — 6 branches for 3 sinks:

```bash
merge-flat-cdxj - /data/indexes_cdx/*.cdxj | \
  tee >(cdxj-stats --sink mimetype --out-dir stats/) \
      >(cdxj-stats --sink mimetype --sink-mimetype group-by=year --out-dir stats/) \
      >(cdxj-stats --sink domain-host --out-dir stats/) \
      >(cdxj-stats --sink domain-host --sink-domain-host group-by=year --out-dir stats/) \
      >(cdxj-stats --sink domain-etld1 --out-dir stats/) | \
  cdxj-stats --sink domain-etld1 --sink-domain-etld1 group-by=year --out-dir stats/
```

This produces `mimetype.csv`, `mimetype-year.csv`, `domain-host.csv`, `domain-host-year.csv`, `domain-etld1.csv`, and `domain-etld1-year.csv` in one pass, using one core per branch.

## Python API

```python
from replay_cdxj_indexing_tools.stats.cdxj_stats import run

rows_written = run(
    input_file="index.cdxj",
    sink_names=["mimetype", "domain-host"],
    sink_options={"domain-host": {"group-by": "year"}},
    out_dir="stats/",
)
```

## Command Reference

```bash
cdxj-stats --help
```

Options:
- `--sink` (required) - Comma-separated sink names to run in one pass, e.g. `mimetype,domain-host`
- `--out-dir` (required) - Directory to write one CSV per sink into
- `--input, -i` - Input CDXJ file (default: stdin)
- `--sink-<name> key=value ...` - Per-sink options (e.g. `group-by=year`)
- `--field-separator` - Output field separator for every sink's CSV output (default: `,`)
- `--verbose, -v` - Print summary to stderr

## SURT Order Validation

The tool warns once to stderr if the input doesn't appear to be SURT-sorted (a key seen out of order relative to the previous one). The run still completes either way, but the domain sinks may split a domain's stats across multiple non-contiguous rows if the input isn't actually sorted.

## Writing a Third-Party Sink

A sink is any class implementing:

```python
class Sink(Protocol):
    def configure(self, **opts: str) -> None: ...
    def process(self, surt_key, timestamp, json_data) -> Optional[Iterable[Sequence[str]]]: ...
    def flush(self) -> Iterable[Sequence[str]]: ...
    def fieldnames(self) -> Sequence[str]: ...
    def output_suffix(self) -> Optional[str]: ...
```

Register it under the `replay_cdxj_indexing_tools.sinks` entry-point group in your package's `pyproject.toml`:

```toml
[project.entry-points."replay_cdxj_indexing_tools.sinks"]
my-sink = "my_package.my_sink:MySink"
```

Once installed alongside replay-cdxj-indexing-tools, it becomes available as `--sink my-sink`.

## See Also

- [merge-flat-cdxj](merge-flat-cdxj.md) - Merge CDXJ files (recommended before cdxj-stats, to get a SURT-sorted stream)
- [filter-blocklist](filter-blocklist.md) - Filter CDXJ records
- [cdxj-extract-field](cdxj-extract-field.md) - Extract JSON fields from CDXJ records
