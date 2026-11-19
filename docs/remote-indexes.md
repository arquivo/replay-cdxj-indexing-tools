# Remote Indexes (HTTP(S) and S3)

The search and streaming tools can read CDXJ and ZipNum indexes where they are
published, over `http://`, `https://` or `s3://`, instead of from local disk. A
researcher can list every capture of a domain or URL prefix from a multi-terabyte
index without downloading it: only the parts of the index that the query needs are
fetched, with HTTP range requests.

Local paths work exactly as before; a location is read remotely only when it starts
with one of these URL schemes.

## Installation

Remote access needs optional dependencies, so the base install stays small:

```bash
pip install "replay-cdxj-indexing-tools[http]"      # http:// and https://
pip install "replay-cdxj-indexing-tools[s3]"        # s3:// (AWS S3, Ceph RGW, MinIO, ...)
pip install "replay-cdxj-indexing-tools[http,s3]"   # both
```

Using a remote location without its extra installed fails with a message naming the
`pip install` command to run.

### Docker

The Docker image includes both extras, so it needs no setup:

```bash
docker run --rm arquivo/replay-cdxj-indexing-tools:latest \
  cdxj-search --url https://example.pt/ --matchType domain \
  https://example.org/indexes/collection/collection.idx > example.pt.cdxj
```

For S3, pass the settings from [S3 configuration](#s3-configuration) into the
container. `-e NAME` without a value copies the variable from your shell, so
credentials stay out of the command line:

```bash
docker run --rm -e AWS_ENDPOINT_URL_S3 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
  arquivo/replay-cdxj-indexing-tools:latest \
  cdxj-search --url https://example.pt/ s3://indexes/collection/collection.idx
```

## Searching a remote index

Give `cdxj-search` the URL of a ZipNum `.idx` (or of a flat `.cdxj`) instead of a path:

```bash
# Every capture of a domain, from a ZipNum index published over HTTPS
cdxj-search --url https://example.pt/ --matchType domain \
  https://example.org/indexes/collection/collection.idx

# The same index in S3-compatible storage
cdxj-search --url https://example.pt/ --matchType domain \
  s3://indexes/collection/collection.idx

# A flat CDXJ file: binary search with range requests
cdxj-search --url https://example.pt/page --from 2020 --to 2021 \
  https://example.org/indexes/collection.cdxj
```

All the usual options (`--matchType`, `--from`/`--to`, `--filter`, `--limit`,
`--sort`, `--dedupe`) work the same way. Results can be piped into the other tools,
for example to get a CSV of URLs and timestamps:

```bash
cdxj-search --url https://example.pt/ --matchType subdomains \
  https://example.org/indexes/collection/collection.idx \
  | cdxj-to-csv --columns urlkey,timestamp,url,status,mime > example.pt.csv
```

### What a lookup costs

| Index | Requests per lookup |
|-------|---------------------|
| ZipNum | A few small requests for the `.loc` file, about log2(`.idx` size / 64 KB) small requests to binary-search the `.idx`, then, for each shard with matches, one request per run of adjacent matching blocks. Blocks that are next to each other in a shard are fetched together, up to 16 MB per request. |
| Flat CDXJ | About log2(file size / 64 KB) small requests for the binary search, then sequential requests while reading the matches. |

Each remote file opened also costs one `HEAD` request, to learn its size.

Sequential reads (streaming tools, long result lists) start at 64 KB and double up
to 4 MB per request.

ZipNum is the better format for remote access: the `.idx` is small, and each
matching block is a compressed range of a shard.

### Remote file discovery

- `http(s)://` locations name single files. HTTP has no directory listing, so
  wildcards and directories are rejected over HTTP: give the URL of each file.
- `s3://` locations can also be a prefix ending in `/` (every `.cdxj`, `.idx` and
  `.cdxj.gz` object under it is searched) or a wildcard pattern:

```bash
cdxj-search --url https://example.pt/ s3://indexes/2024/
cdxj-search --url https://example.pt/ "s3://indexes/2024/*.idx"
```

## Streaming tools

These tools accept an `http(s)://` or `s3://` URL wherever they accept an input
file. The file is streamed once from start to end. `filter-excessive-urls auto` is
the exception: it reads its input twice.

| Tool | Remote input |
|------|--------------|
| `cdxj-search` | Files to search (see above) |
| `zipnum-to-flat-cdxj` | `-i` (`.idx`), `--loc`, `--base-dir` |
| `merge-flat-cdxj` | Input files |
| `filter-blocklist` | `-i` |
| `filter-excessive-urls` | `-i` |
| `cdxj-extract-field` | `-i` |
| `cdxj-stats` | `-i` |
| `cdxj-to-csv` | `-i` |
| `addfield-to-flat-cdxj` | `-i` |

Outputs are always local files or stdout.

```bash
# Export a whole remote ZipNum index as flat CDXJ
zipnum-to-flat-cdxj -i https://example.org/indexes/collection/collection.idx > collection.cdxj

# Use a downloaded copy of the (small) .idx and .loc, but read the shards remotely
zipnum-to-flat-cdxj -i collection.idx --loc collection.loc \
  --base-dir https://example.org/indexes/collection/ > collection.cdxj

# MIME type statistics straight from a remote flat CDXJ
cdxj-stats -i s3://indexes/collection.cdxj --sink mimetype --out-dir stats/
```

`merge-flat-cdxj` keeps up to 8 MB of buffered data per remote input, so merging
hundreds of remote files at once needs a corresponding amount of memory.

## S3 configuration

Credentials and endpoints come from the standard AWS mechanisms, never from the
command line:

| Setting | How |
|---------|-----|
| Credentials | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, `AWS_PROFILE`, `~/.aws/credentials`, or an instance role |
| Endpoint of S3-compatible storage (Ceph RGW, MinIO, ...) | `AWS_ENDPOINT_URL_S3=https://s3.example.org` (or `AWS_ENDPOINT_URL`) |
| Public bucket, no credentials | `FSSPEC_S3='{"anon": true}'` |

Any other [s3fs option](https://s3fs.readthedocs.io/) can be set the same way, as
JSON in `FSSPEC_S3` (see the
[fsspec configuration](https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration)).

```bash
export AWS_ENDPOINT_URL_S3=https://s3.example.org
export AWS_PROFILE=research
cdxj-search --url https://example.pt/ --matchType domain s3://indexes/collection/collection.idx
```

## HTTP server requirements

The server must support byte-range requests: it must answer `HEAD` with
`Content-Length` and `Accept-Ranges: bytes`, and answer `Range` requests with
`206 Partial Content`. Static file servers (nginx, Apache) and object stores do
this out of the box. The tools refuse to read from a server that ignores ranges,
rather than downloading the whole file.

## Timeouts, retries and errors

| Variable | Default | Meaning |
|----------|---------|---------|
| `CDXJ_REMOTE_TIMEOUT` | `60` | Seconds to wait for a connection, or for more data on an open one (HTTP) |
| `CDXJ_REMOTE_RETRIES` | `3` | Retries after a transient failure: timeout, dropped connection, HTTP 429 or 5xx. The wait doubles after each attempt, starting at 0.5 s |

Client errors (404, 403, ...) are not retried. When a remote file cannot be read,
the error names the URL, for example `Remote location not found: https://...`.

When a remote ZipNum shard cannot be read, `cdxj-search` stops with an error
instead of returning partial results. With `--skip-errors`, it reports the error
and continues with the other files. `zipnum-to-flat-cdxj` warns about a missing
shard and continues, as it does for local shards.

S3 requests are also retried by the S3 client itself (botocore).

## Restrictions

These rules keep an index from being used to make the tools read arbitrary
locations:

- Only `http://`, `https://` and `s3://` are accepted. Other schemes (`file://`,
  `ftp://`, ...) are rejected.
- Remote URLs must not contain credentials (`https://user:password@...`), a query
  string or a fragment. Use the environment or a config profile for credentials.
- Shard locations read from a `.loc` file or an `.idx` must stay inside the
  index's base location: the directory of the `.idx`, or `--base-dir`. That means
  the same scheme and host (or bucket), and a path under that directory. Relative
  entries are resolved against the base. Absolute URLs are accepted only if they
  fall inside it. `..` segments (including encoded ones like `%2e%2e`), local
  paths under a remote base, and URLs under a local base are all rejected. This is
  the remote counterpart of the existing check for local paths.

Writing indexes to remote locations (for example `flat-cdxj-to-zipnum` straight to
S3) is not supported. Write locally and upload the result.
