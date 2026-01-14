# cdxj-extract-field

Extract specific JSON fields from CDXJ records, outputting one field value per line to stdout.

## Purpose

This tool reads CDXJ lines from stdin or a file, parses the JSON payload, extracts a specified field, and writes the field value to stdout. Useful for:

- **Data extraction** - Pull specific metadata from large CDXJ indexes
- **Field validation** - Verify field values across indexes
- **Pipeline operations** - Extract data for downstream processing
- **Statistics** - Count unique values, identify patterns
- **Debugging** - Inspect specific fields across records

## Installation

The tool is included with the replay-cdxj-indexing-tools package:

```bash
pip install replay-cdxj-indexing-tools
```

## Quick Start

Extract collection field from CDXJ file:

```bash
cat index.cdxj | cdxj-extract-field --field collection
```

## Usage

### Basic Field Extraction

Extract a single field in JSON mode (default):

```bash
cat index.cdxj | cdxj-extract-field --field collection
```

Output (JSON encoded):
```
"AWP-999"
"AWP-999"
"AWP-1000"
```

### Raw Mode (No JSON Encoding)

Extract field without JSON encoding for strings:

```bash
cat index.cdxj | cdxj-extract-field --field collection --raw
```

Output (raw text):
```
AWP-999
AWP-999
AWP-1000
```

The `--raw` flag is useful for string fields when you want the value without quotes for shell scripting or human reading.

### From Files

Extract from a file instead of stdin:

```bash
cdxj-extract-field --field status -i index.cdxj
```

### Default Values

Use a default value for missing fields:

```bash
cat index.cdxj | cdxj-extract-field --field collection --default "unknown"
```

If a record doesn't have the field, the default value is output.

### Skip Missing Fields

Skip records where the field is missing:

```bash
cat index.cdxj | cdxj-extract-field --field collection --skip-missing
```

### Verbose Mode

Show processing statistics:

```bash
cat index.cdxj | cdxj-extract-field --field status --verbose
```

Output to stderr:
```
Processed: 1000 | Extracted: 950 | Skipped: 50 | Errors: 0
```

## CDXJ Format

CDXJ records contain a SURT key, timestamp, and JSON payload:

```
com,example)/path 20200101000000 {"collection":"AWP-999","status":200,"length":1024}
```

The tool extracts from the JSON portion.

## Examples

### Count Unique Collections

```bash
cat index.cdxj | cdxj-extract-field --field collection --raw | sort | uniq -c
```

Output:
```
    500 AWP-999
    300 AWP-1000
    200 AWP-1001
```

### Filter by Status Code

```bash
cat index.cdxj | cdxj-extract-field --field status --raw | grep "^200$" | wc -l
```

### Extract URLs from Specific Collection

```bash
cat index.cdxj | \
  cdxj-extract-field --field collection --raw | \
  paste - <(cat index.cdxj | cdxj-extract-field --field url --raw) | \
  grep "AWP-999" | cut -f2
```

### Create Statistics Report

```bash
cat index.cdxj | cdxj-extract-field --field status --raw | \
  sort | uniq -c | sort -rn | \
  awk '{print $2 ": " $1}'
```

Output:
```
200: 1500
404: 50
403: 25
```

### Validate Field Consistency

Check that all records have a required field:

```bash
cat index.cdxj | cdxj-extract-field --field collection --skip-missing | wc -l
total_records=$(wc -l < index.cdxj)
missing_records=$((total_records - $(cat index.cdxj | cdxj-extract-field --field collection --skip-missing | wc -l)))
echo "Missing collection field: $missing_records records"
```

## Python API

```python
from replay_cdxj_indexing_tools.search.extract_field import extract_field_from_cdxj

# Extract fields from file
lines_processed, lines_extracted = extract_field_from_cdxj(
    input_file='index.cdxj',
    field_name='collection',
    raw=True,
    default=None,
    skip_missing=False,
    verbose=True
)

print(f"Processed: {lines_processed}, Extracted: {lines_extracted}")
```

## Command Reference

```bash
cdxj-extract-field --help
```

Options:
- `--field, -f` (required) - JSON field name to extract
- `--input, -i` - Input CDXJ file (default: stdin)
- `--raw, -r` - Output raw values without JSON encoding
- `--default, -d` - Default value for missing fields
- `--skip-missing, -s` - Skip lines with missing field
- `--verbose, -v` - Print statistics to stderr

## Field Types

The tool handles all JSON value types:

- **Strings** - `"value"` (JSON) or `value` (raw)
- **Numbers** - `200` (both modes)
- **Booleans** - `true/false` (both modes)
- **Null** - `null` (both modes)

## Performance

The tool is optimized for streaming:

- Processes large files line-by-line (constant memory)
- Minimal parsing overhead
- Fast I/O with 1MB buffer (configurable)

## Error Handling

The tool handles errors gracefully:

- **Malformed lines** - Logged but processing continues (use `--verbose` to see)
- **Invalid JSON** - Line is skipped with warning
- **Missing fields** - Uses default value or skips line based on options
- **File not found** - Error message and exit with code 1

## Pipes and Integration

Works seamlessly in Unix pipelines:

```bash
# Extract and transform
cat index.cdxj | cdxj-extract-field --field collection --raw | \
  sort | uniq -c | sort -rn | head -20

# Combine with other tools
merge-flat-cdxj - *.cdxj | \
  cdxj-extract-field --field collection --raw | \
  sort | uniq

# Filter and extract
filter-blocklist -i index.cdxj -b blocklist.txt | \
  cdxj-extract-field --field url --raw
```

## See Also

- [merge-flat-cdxj](merge-flat-cdxj.md) - Merge CDXJ files
- [filter-blocklist](filter-blocklist.md) - Filter CDXJ records
- [cdxj-search](cdxj-search.md) - Search indexes
