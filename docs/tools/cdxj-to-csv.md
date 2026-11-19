# cdxj-to-csv

Convert CDXJ records to CSV or TSV, one row per record, from stdin or a file to stdout or a file.

## Purpose

- **Spreadsheets and databases** - Load index metadata into tools that read delimited text
- **Data analysis** - Feed pandas, DuckDB, `awk` or `cut` without parsing JSON
- **Reporting** - Export a chosen subset of fields in a fixed column order

## Installation

The tool is included with the replay-cdxj-indexing-tools package:

```bash
pip install replay-cdxj-indexing-tools
```

## Quick Start

```bash
cat index.cdxj | cdxj-to-csv > index.csv
```

## Columns

Each CDXJ line is `<urlkey> <timestamp> {json}`. The two leading fields become the `urlkey` and `timestamp` columns, and every key of the JSON block becomes a column of its own.

Input:
```
com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "text/html", "status": "200"}
com,example)/a?b=c,d 20200102000000 {"url": "http://example.com/a?b=c,d", "status": "404"}
```

Output:
```
urlkey,timestamp,url,mime,status
"com,example)/",20200101000000,http://example.com/,text/html,200
"com,example)/a?b=c,d",20200102000000,"http://example.com/a?b=c,d",,404
```

### Auto-Detection

Without `--columns`, the columns are the JSON keys found in the first `--sample-lines` records (default: 1000), in the order they first appear. Those records are held in memory and then written out, so the rest of the input is streamed.

Keys that first appear after the sample are not exported. A warning on stderr names them and counts the affected records:

```
[WARNING] 12 record(s) had fields not found in the first 1000 records and were not exported: robotflags. Use --columns or a larger --sample-lines to include them.
```

To see which columns would be detected:

```bash
cdxj-to-csv -i index.cdxj --list-columns
```

### Column Subset

`--columns` takes a comma-separated list of columns, exported in the given order. No sampling is done, and columns missing from a record are left empty:

```bash
cdxj-to-csv -i index.cdxj -c urlkey,timestamp,status,mime
```

## Delimiters and Quoting

The defaults (`,` as column delimiter and `"` as text delimiter) produce standard CSV. Values that contain the delimiter, the text delimiter or a line break are wrapped in the text delimiter, and a text delimiter inside a value is doubled.

```bash
# TSV ('\t' and 'tab' are accepted for a tab character)
cat index.cdxj | cdxj-to-csv -d '\t'

# Semicolon-separated
cat index.cdxj | cdxj-to-csv -d ';'

# Single quote as text delimiter
cat index.cdxj | cdxj-to-csv -q "'"

# Pipe as text delimiter
cat index.cdxj | cdxj-to-csv -q '|'
```

`--quoting` controls when values are wrapped in the text delimiter:

| Mode | Behaviour |
|------|-----------|
| `minimal` (default) | Only values that contain the delimiter, the text delimiter or a line break |
| `all` | Every value, including the header |
| `nonnumeric` | Every value except JSON numbers |
| `none` | Never; the delimiter, text delimiter and escape character inside values are preceded by `--escapechar` (default: `\`) |

```bash
# Every value quoted
cat index.cdxj | cdxj-to-csv --quoting all

# Pipe-separated, no quoting
cat index.cdxj | cdxj-to-csv -d '|' --quoting none
```

## Values

| JSON value | Cell |
|------------|------|
| missing field, `null` | empty |
| string | as is |
| number | as is |
| `true` / `false` | `true` / `false` |
| object, array | compact JSON, e.g. `{"a":[1,2]}` |

Output is UTF-8 and lines end with `\n`.

## Python API

```python
from replay_cdxj_indexing_tools.export.cdxj_to_csv import cdxj_to_csv, list_columns

# Returns (lines_processed, rows_written)
cdxj_to_csv(
    input_file="index.cdxj",
    output_file="index.tsv",
    columns=["urlkey", "timestamp", "status"],
    delimiter="\t",
)

columns = list_columns("index.cdxj", sample_lines=1000)
```

## Command Reference

```
cdxj-to-csv [-i INPUT] [-o OUTPUT] [-c COLUMNS] [-d DELIMITER] [-q QUOTECHAR]
            [--quoting {minimal,all,nonnumeric,none}] [--escapechar ESCAPECHAR]
            [--sample-lines N] [--no-header] [--list-columns] [-v]

Options:
  -i, --input FILE              Input CDXJ file (default: stdin)
  -o, --output FILE             Output file (default: stdout)
  -c, --columns LIST            Comma-separated columns to export, in order (default: auto-detect)
  -d, --delimiter, --field-separator CHAR
                                Column delimiter; '\t' or 'tab' for TSV (default: ',')
  -q, --quotechar CHAR          Text delimiter wrapped around values (default: '"')
  --quoting MODE                minimal, all, nonnumeric or none (default: minimal)
  --escapechar CHAR             Escape character (default: '\' with --quoting none)
  --sample-lines N              Records read to auto-detect columns (default: 1000)
  --no-header                   Do not write the header row
  --list-columns                Print the auto-detected columns, one per line, and exit
  -v, --verbose                 Print per-line errors and statistics to stderr
```

The delimiter, text delimiter and escape character must each be a single character, must differ from each other, and cannot be a line break.

## Error Handling

- **Blank lines** - Skipped silently
- **Corrupt lines** (NUL bytes, no key before the JSON block) - Skipped with a warning naming the input and line number
- **Unparseable lines** (no timestamp, invalid JSON block) - Skipped and counted in a summary warning; `--verbose` names each one
- **Invalid options** - Error message and exit with code 1 (or 2 for argument syntax errors)
- **File not found** - Error message and exit with code 1
- **Closed output pipe** (e.g. `| head`) - Stops without a traceback

## Pipes and Integration

```bash
# Merge, filter and export selected fields
merge-flat-cdxj - *.cdxj | \
  filter-blocklist -i - -b blocklist.txt | \
  cdxj-to-csv -c urlkey,timestamp,url,mime,status > captures.csv

# Load into DuckDB
cdxj-to-csv -i index.cdxj -o index.csv
duckdb -c "SELECT mime, count(*) FROM 'index.csv' GROUP BY mime ORDER BY 2 DESC"

# Count status codes with standard tools
cdxj-to-csv -i index.cdxj -d '\t' -c status --no-header | sort | uniq -c
```

## See Also

- [cdxj-extract-field](cdxj-extract-field.md) - Extract a single JSON field per line
- [cdxj-stats](cdxj-stats.md) - Aggregate capture statistics into CSV
- [merge-flat-cdxj](merge-flat-cdxj.md) - Merge CDXJ files
- [filter-blocklist](filter-blocklist.md) - Filter CDXJ records
