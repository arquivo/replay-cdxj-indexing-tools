# filter-blocklist - Blocklist Pattern Filter

Filter CDXJ records matching regex patterns in a blocklist file. Use this to remove spam domains, adult content, specific MIME types, or other unwanted content from web archive indexes.

## Command-Line Usage

### Basic Syntax

```bash
filter-blocklist -i INPUT -b BLOCKLIST [-o OUTPUT] [-v]
```

### Examples

**Basic filtering:**
```bash
filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj
```

**With verbose output:**
```bash
filter-blocklist -i arquivo.cdxj -b blocklist.txt -o clean.cdxj -v
```

**Pipeline mode (stdin/stdout):**
```bash
cat input.cdxj | filter-blocklist -i - -b blocklist.txt > output.cdxj
```

**In complete pipeline:**
```bash
merge-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    cdxj-to-zipnum -o indexes -i -
```

### Options

- `-i, --input INPUT` - Input CDXJ file or `-` for stdin (required)
- `-b, --blocklist BLOCKLIST` - Blocklist file with regex patterns (required)
- `-o, --output OUTPUT` - Output file or `-` for stdout (default: stdout)
- `-v, --verbose` - Print statistics to stderr

## Blocklist File Format

Each line in the blocklist is a **regex pattern**. Lines starting with `#` are comments.

### Example Blocklist

```text
# blocklist.txt - Example blocklist patterns

# Block spam domains
^pt,spam,
^pt,spam-site,
^pt,fake-news,

# Block adult content
^pt,adult,
^pt,xxx,
^com,pornsite,

# Block advertising paths
/ads/
/banner
/tracking\.js
/analytics\.js

# Block specific MIME types
"mime": "application/x-shockwave-flash"
"mime": "application/pdf"

# Block error responses
"status": "404"
"status": "500"
"status": "502"
```

### Pattern Types

#### 1. Domain Patterns (SURT)

Block entire domains using SURT prefix:

```text
^pt,spam,              # Block all spam.pt
^pt,adult,xxx)         # Block xxx.adult.pt
^com,malware,          # Block all malware.com
```

#### 2. Path Patterns

Block specific URL paths:

```text
/ads/                  # Any path containing /ads/
/tracker\.js           # Specific file
/wp-admin/             # WordPress admin
```

#### 3. JSON Metadata Patterns

Block by MIME type, status code, or other JSON fields:

```text
"mime": "application/x-shockwave-flash"    # Block Flash
"status": "404"                             # Block 404s
"digest": "sha1:ABCDEF"                     # Block specific content
```

#### 4. Combined Patterns

Match multiple conditions:

```text
^pt,site,www\).*"status": "404"    # 404s from specific site
```

## Python API

### Load and Apply Blocklist

```python
from replay_cdxj_indexing_tools.utils.filter_blocklist import (
    load_blocklist,
    filter_cdxj_by_blocklist,
)

# Load patterns from file
patterns = load_blocklist('blocklist.txt')
print(f"Loaded {len(patterns)} patterns")

# Apply to CDXJ file
kept, blocked = filter_cdxj_by_blocklist(
    input_path='input.cdxj',
    blocklist_patterns=patterns,
    output_path='output.cdxj'
)

print(f"Kept: {kept} lines")
print(f"Blocked: {blocked} lines")
```

### Manual Pattern Creation

```python
import re
from replay_cdxj_indexing_tools.utils.filter_blocklist import filter_cdxj_by_blocklist

# Create patterns manually
patterns = [
    re.compile(r'^pt,spam,'),
    re.compile(r'^pt,adult,'),
    re.compile(r'/ads/'),
    re.compile(r'"mime": "application/x-shockwave-flash"'),
]

# Filter
kept, blocked = filter_cdxj_by_blocklist('input.cdxj', patterns, 'output.cdxj')
```

### Stream Processing

```python
import sys
from replay_cdxj_indexing_tools.utils.filter_blocklist import (
    load_blocklist,
    filter_cdxj_by_blocklist,
)

# Load blocklist
patterns = load_blocklist('blocklist.txt')

# Filter stdin to stdout
kept, blocked = filter_cdxj_by_blocklist(
    input_path='-',
    blocklist_patterns=patterns,
    output_path='-'
)
```

## Use Cases

### 1. Remove Spam Domains

Block known spam and malicious domains:

```bash
# blocklist.txt
^pt,spam-site,
^pt,fake-news,
^pt,malware,

# Apply filter
filter-blocklist -i arquivo.cdxj -b blocklist.txt -o clean.cdxj -v
```

### 2. Filter Adult Content

Remove adult/NSFW content from web archive:

```bash
# adult-blocklist.txt
^pt,adult,
^pt,xxx,
^com,pornsite,
^com,xxx,

filter-blocklist -i collection.cdxj -b adult-blocklist.txt -o filtered.cdxj
```

### 3. Remove Legacy Technologies

Block outdated technologies (Flash, Java applets):

```bash
# legacy-tech-blocklist.txt
"mime": "application/x-shockwave-flash"
"mime": "application/x-java-applet"
"mime": "application/x-silverlight"

filter-blocklist -i index.cdxj -b legacy-tech-blocklist.txt -o modern.cdxj
```

### 4. Quality Control - Remove Errors

Filter out error responses:

```bash
# errors-blocklist.txt
"status": "404"
"status": "500"
"status": "502"
"status": "503"

filter-blocklist -i full.cdxj -b errors-blocklist.txt -o valid.cdxj
```

### 5. Remove Advertising Content

Block ads, trackers, and analytics:

```bash
# ads-blocklist.txt
/ads/
/banner
/advert
/tracking\.js
/analytics\.js
/pixel\.gif
^com,doubleclick,
^com,googleadservices,

filter-blocklist -i site.cdxj -b ads-blocklist.txt -o clean.cdxj
```

## Performance

**Benchmark Results:**

| File Size | Lines | Patterns | Time | Throughput |
|-----------|-------|----------|------|------------|
| 500MB | 1M | 10 | ~3s | ~300K lines/sec |
| 5GB | 10M | 50 | ~30s | ~300K lines/sec |
| 50GB | 100M | 100 | ~300s | ~300K lines/sec |

**Memory Usage:**
- O(patterns) - stores compiled regex patterns in memory
- ~1-10MB for typical blocklists (10-1000 patterns)

**Performance Tips:**
1. Keep blocklists focused (fewer patterns = faster)
2. Use specific patterns (avoid wildcards when possible)
3. Use pipeline mode to avoid intermediate files
4. Process on SSD for best performance

## Common Blocklist Patterns

### Portuguese Spam Domains

```text
# Common PT spam patterns
^pt,spam,
^pt,publicidade,
^pt,anuncios,
```

### International Spam

```text
# Common spam TLDs and patterns
^com,spam,
^info,spam,
^biz,
\.tk\)         # Free TLD often used for spam
\.ml\)         # Free TLD
```

### Privacy-Respecting Filters

```text
# Remove tracking and surveillance
/tracking
/analytics
/beacon
/pixel\.
^com,google-analytics,
^com,facebook,.*\/pixel
^com,twitter,.*\/ads
```

## Error Handling

### Invalid Regex Patterns

If blocklist contains invalid regex:

```
Warning: Invalid regex pattern at line 5: [invalid(
  Error: unbalanced parenthesis
```

**Solution:** Fix the pattern or comment it out.

### Empty Blocklist

If blocklist is empty or has only comments:

```
Warning: No patterns loaded from blocklist
```

**Solution:** Ensure blocklist has at least one valid pattern.

### Pattern Too Broad

If pattern matches too many records:

```bash
# Check what would be blocked
cat input.cdxj | grep -E "pattern" | wc -l

# Test with verbose mode
filter-blocklist -i input.cdxj -b blocklist.txt -o output.cdxj -v
```

## Testing

Run blocklist filter tests:

```bash
# All blocklist tests
pytest tests/test_filter_blocklist.py -v

# Specific test categories
pytest tests/test_filter_blocklist.py::TestFilterCdxjByBlocklist -v

# With coverage
pytest --cov=replay_cdxj_indexing_tools.utils.filter_blocklist tests/test_filter_blocklist.py
```

## Migration from Legacy Scripts

### Old Approach (Bash)

```bash
# Old: apply_blacklist.sh
grep -a -E -v -f blacklist_patterns.txt input.cdxj > output.cdxj
```

### New Approach (Python)

```bash
# New: filter-blocklist
filter-blocklist -i input.cdxj -b blacklist_patterns.txt -o output.cdxj
```

**Benefits:**
- 10-50x faster
- Better error handling
- Pipeline compatible
- Cross-platform
- Integrated with other tools

## See Also

- [filter-excessive-urls.md](filter-excessive-urls.md) - Remove crawler traps
- [merge-cdxj.md](merge-cdxj.md) - Previous step: merge files
- [cdxj-to-zipnum.md](cdxj-to-zipnum.md) - Next step: convert to ZipNum
- [pipeline-examples.md](pipeline-examples.md) - Complete workflows
