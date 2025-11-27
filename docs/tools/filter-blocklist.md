# filter-blocklist - Blocklist Pattern Filter

Filter CDXJ records matching regex patterns in a blocklist file. Use this to remove spam domains, adult content, or other unwanted content from web archive indexes.

**This tool matches regex patterns against ENTIRE CDXJ lines** (like `grep -E -v`), providing flexibility for filtering by SURT domain prefixes and URL patterns (including paths).

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
merge-flat-cdxj - *.cdxj | \
    filter-blocklist -i - -b blocklist.txt | \
    filter-excessive-urls remove -i - -b excessive.txt | \
    flat-cdxj-to-zipnum -o indexes -i -
```

### Options

- `-i, --input INPUT` - Input CDXJ file or `-` for stdin (required)
- `-b, --blocklist BLOCKLIST` - Blocklist file with regex patterns (required)
- `-o, --output OUTPUT` - Output file or `-` for stdout (default: stdout)
- `-v, --verbose` - Print statistics to stderr

## How It Works

The filter **matches regex patterns against the entire CDXJ line**, just like the original bash implementation:

```bash
# Original bash: grep -E -v -f blocklist.txt input.cdxj
# Python equivalent: filter-blocklist -i input.cdxj -b blocklist.txt
```

**CDXJ line format:**
```
SURT TIMESTAMP JSON
```

Example:
```
pt,governo,www)/ 20230615120000 {"url": "https://www.governo.pt/", "mime": "text/html", "status": "200"}
```

Patterns can match any part: SURT, timestamp, or JSON fields.

## Blocklist File Format

Each line is a **regex pattern** matched against entire CDXJ lines. Lines starting with `#` are comments.

### Example Blocklist

```text
# blocklist.txt - Example blocklist patterns

# Block by SURT prefix (entire domain)
^pt,spam,
^pt,adult,

# Block by URL pattern (domain + path in JSON)
https://www\.spam\.pt/
http://.*\.adult\.pt/
https://www\.site\.pt/unwanted-section/

# Block by file extension (URL pattern)
\.pdf"
\.swf"
/ads/.*"
```

### Pattern Strategies

#### 1. Block by SURT (Entire Domain)

**Most efficient** - matches start of line (anchored regex):

```text
^pt,spam,              # Block all *.spam.pt
^pt,adult,xxx)         # Block xxx.adult.pt
^com,malware,          # Block all *.malware.com
```

#### 2. Block by URL Pattern (in JSON)

Match specific URLs or URL patterns inside the JSON `"url"` field:

```text
https://www\.spam\.pt/           # Specific domain
http://.*\.adult\.pt/            # Any subdomain of adult.pt
https://www\.site\.pt/admin/     # Specific path on domain
https://www\.site\.pt/unwanted/  # Block subdirectory
```

#### 3. Block by Path Pattern

Match any URL containing specific paths:

```text
/wp-admin/                       # WordPress admin (any site)
/ads/                            # Ads directory (any site)
/tracking\.js"                   # Tracking script (any site)
```

#### 4. Block by File Extension

Block specific file types (matches end of URL in JSON):

```text
\.pdf"                           # All PDFs
\.swf"                           # All Flash files
\.exe"                           # Executables
```

#### 5. Advanced: Domain + Path Combination

Combine SURT domain with specific path requirements:

```text
^pt,site,www\)/admin/            # Only /admin/ path on www.site.pt
^pt,site,www\)/.*\.pdf"          # Only PDFs from www.site.pt
^com,example,.*\)/private/       # /private/ on any example.com subdomain
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

Block known spam and malicious domains by SURT:

```bash
# blocklist.txt
^pt,spam-site,
^pt,fake-news,
^pt,malware,

# Apply filter
filter-blocklist -i arquivo.cdxj -b blocklist.txt -o clean.cdxj -v
```

### 2. Filter Adult Content

Remove adult/NSFW content by domain or URL patterns:

```bash
# adult-blocklist.txt
^pt,adult,
^pt,xxx,
http://.*\.adult\.pt/
https://.*\.xxx\./

filter-blocklist -i collection.cdxj -b adult-blocklist.txt -o filtered.cdxj
```

### 3. Remove Specific File Types

Block file types by URL extension:

```bash
# file-types-blocklist.txt
\.pdf"           # PDFs
\.swf"           # Flash
\.exe"           # Executables
\.zip"           # Archives

filter-blocklist -i index.cdxj -b file-types-blocklist.txt -o filtered.cdxj
```

### 4. Block Domain + Specific Paths

Block specific sections of websites:

```bash
# domain-path-blocklist.txt
^pt,site,www\)/admin/
^pt,site,www\)/wp-admin/
^pt,news,www\)/private/
https://www\.forum\.pt/spam-section/

filter-blocklist -i full.cdxj -b domain-path-blocklist.txt -o filtered.cdxj
```

### 5. Remove Advertising Content

Block ads and trackers by URL patterns:

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

**Memory Usage:**
- O(patterns) - stores compiled regex patterns in memory
- Minimal memory for typical blocklists (10-1000 patterns)

**Performance Characteristics:**
- I/O bound - limited by disk read/write speed
- Scales linearly with file size: O(lines × patterns)
- Optimized regex matching with compiled patterns

**Performance Tips:**
1. Use SURT prefix patterns (`^pt,spam,`) - fastest
2. Avoid complex regex with backtracking
3. Use pipeline mode to avoid intermediate files
4. Process on SSD for best I/O performance

## Pattern Best Practices

### 1. Prefer SURT Prefix Matching

**Fast** (anchored at start of line):
```text
^pt,spam,              # Matches: pt,spam,www)/ ...
^com,ads,              # Matches: com,ads,tracking)/ ...
```

**Slower** (full line scan):
```text
spam\.pt               # Must scan entire line
```

### 2. URL Patterns Should Match JSON Format

URLs in CDXJ are inside JSON strings:
```text
{"url": "https://www.spam.pt/path"}
```

Patterns:
```text
https://www\.spam\.pt/     # Correct - matches JSON format
\.pdf"                     # Correct - matches end of URL in JSON
```

### 3. Combine Domain + Path Efficiently

```text
# Good: Single pattern for domain + specific path
^pt,news,www\)/admin/

# Less efficient: Would match domain separately
^pt,news,www\)
/admin/
```

### 4. Escape Special Characters

```text
\.pdf"          # Escaped dot (literal .)
/ads/           # Forward slashes are literal in regex
\(.*\)          # Escaped parentheses
```

## Common Blocklist Patterns

### Portuguese Spam Domains

```text
# Common PT spam patterns (SURT prefix)
^pt,spam,
^pt,publicidade,
^pt,anuncios,
```

### Block by File Extension

```text
# Common unwanted file types
\.pdf"
\.doc"
\.xls"
\.swf"
\.exe"
```

### Block by URL Patterns

```text
# Advertising and tracking paths (any domain)
/ads/
/banner
/tracking\.js
/analytics\.js
/pixel\.gif
/beacon
```

### Block Domain + Path Combinations

```text
# Specific paths on specific domains
^pt,site,www\)/admin/
^pt,forum,www\)/spam-section/
^com,example,www\)/private/
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
# New: filter-blocklist (same behavior as grep -E -v)
filter-blocklist -i input.cdxj -b blacklist_patterns.txt -o output.cdxj
```

**Benefits:**
- **Same pattern matching as grep -E -v** (regex against entire line)
- Better error handling and pattern validation
- Pipeline compatible (stdin/stdout)
- Cross-platform (no grep dependency)
- Integrated with other tools
- Verbose statistics mode

**Pattern Compatibility:**
The Python implementation uses the **same regex matching** as `grep -E`, so existing blocklist files work without modification.

## See Also

- [filter-excessive-urls.md](filter-excessive-urls.md) - Remove crawler traps
- [merge-flat-cdxj.md](merge-flat-cdxj.md) - Previous step: merge files
- [flat-cdxj-to-zipnum.md](flat-cdxj-to-zipnum.md) - Next step: convert to ZipNum
- [pipeline-examples.md](pipeline-examples.md) - Complete workflows
