# Remote Indexes (HTTP(S) and S3)

The search and streaming tools can read CDXJ and ZipNum indexes where they are
published, over `http://`, `https://` or `s3://`, instead of from local disk. A
researcher can list every capture of a domain or URL prefix from a multi-terabyte
index without downloading it: only the parts of the index that the query needs are
fetched, with HTTP range requests.

A location is read remotely only when it starts with one of these URL schemes;
anything else is a local path, opened directly from disk.

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
docker run --rm -e AWS_ENDPOINT_URL_S3 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  arquivo/replay-cdxj-indexing-tools:latest \
  cdxj-search --url https://example.pt/ s3://indexes/collection/collection.idx
```

`AWS_SESSION_TOKEN` is only needed with temporary credentials. For a public
bucket, pass `-e FSSPEC_S3='{"anon": true}'` instead of credentials. To use
`AWS_PROFILE`, mount the AWS configuration into the container as well, for example
`-v ~/.aws:/root/.aws:ro -e AWS_PROFILE`.

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
| ZipNum | 2 for the `.loc` file, 1 + about log2(`.idx` size / 64 KB) for the `.idx`, then 1 per run of adjacent matching blocks in each shard |
| Flat CDXJ | 1 + about log2(file size / 64 KB) for the binary search, then sequential requests while reading the matches |

- The `.loc` file costs two requests whether or not it exists.
- The first request for an `.idx` or flat CDXJ is a `HEAD` for its size; the
  others are small range requests.
- Shards need no `HEAD`: the `.idx` gives the byte ranges. Blocks that are next to
  each other in a shard are fetched together, up to 16 MB per request. A single
  block larger than 16 MB in a remote shard is an error.
- A block that decompresses to more than 64 MB is taken as damaged: an error in a
  remote shard, a skipped block (with a warning) in a local one.

For example, an exact lookup in a ZipNum index with a 40 KB `.idx` takes 5
requests: `.loc` (2), `.idx` (2), and one block of one shard.

Sequential reads (streaming tools, long result lists) start at 64 KB and double up
to 4 MB per request.

ZipNum is the better format for remote access: the `.idx` is small, and each
matching block is a compressed range of a shard.

### Where the shards are found

Shards are found the same way as on local disk:

- With a `.loc` file next to the `.idx` (same name, `.loc` extension), each shard
  name in the `.idx` is looked up in it. Entries are usually relative to the
  directory of the `.idx` (`index-01.cdx.gz`, `./index-01.cdx.gz`,
  `shards/index-01.cdx.gz`), as `flat-cdxj-to-zipnum` writes them. Absolute URLs
  inside that directory also work.
- Without a `.loc` file, each shard name is taken as a file next to the `.idx`
  (`.cdx.gz` is appended if the name has no `.cdx.gz` or `.cdxj.gz` extension).

So publishing the `.idx` and the shards in one directory is enough; the `.loc`
file is optional. See [Restrictions](#restrictions) for the locations that are
refused.

A missing `.loc` must answer "not found" (HTTP 404). Any other error, such as a
403, fails the search rather than being taken as "no `.loc`". S3 answers 403
instead of 404 for a missing object when the reader may not list the bucket
(no `s3:ListBucket` permission), which is common for public-read buckets. On such
a bucket, publish the `.loc` that `flat-cdxj-to-zipnum` writes next to the
`.idx`, or grant `s3:ListBucket`.

### Remote file discovery

- `http(s)://` locations name single files. HTTP has no directory listing, so
  wildcards and directories are rejected over HTTP: give the URL of each file.
- `s3://` locations can also be a prefix ending in `/` (every `.cdxj`, `.idx` and
  `.cdxj.gz` object under it is searched) or a wildcard pattern with `*` or
  `[...]` (`?` cannot be used: remote locations must not contain a query string):

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

While a remote input is streamed, the tools keep the last fetch of up to 4 MB plus
a read buffer of up to 1 MB. `merge-flat-cdxj` keeps every input open, so merging
hundreds of remote files at once needs about 5 MB of memory per input.

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

On buckets that do not grant `s3:ListBucket`, see
[Where the shards are found](#where-the-shards-are-found) about the `.loc` file.

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
rather than downloading the whole file. An empty file needs no range support.

### Proxies

HTTP(S) requests ignore the `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY` variables
unless enabled through the fsspec configuration:

```bash
export FSSPEC_HTTP='{"client_kwargs": {"trust_env": true}}'
```

`trust_env` also makes the HTTP client read credentials for the index host from
`~/.netrc`. Other [aiohttp session options](https://docs.aiohttp.org/en/stable/client_reference.html#client-session)
can be set in `client_kwargs` the same way. `client_kwargs` are taken only from
`FSSPEC_HTTP`, which also applies to `https://`. S3 requests use the proxy
settings of botocore, which read these variables by default.

## Timeouts, retries and errors

| Variable | Default | Meaning |
|----------|---------|---------|
| `CDXJ_REMOTE_TIMEOUT` | `60` | Seconds (1 to 3600) to wait for a connection, or for more data on an open one. HTTP(S) only; S3 uses the botocore timeouts |
| `CDXJ_REMOTE_RETRIES` | `3` | Retries (0 to 20) after a transient failure: timeout, dropped connection, HTTP 429 or 5xx. The wait starts at 0.5 s and doubles after each attempt, or follows the server's `Retry-After` on a 429 or 503, up to 30 s; it is then lengthened by up to 25% at random |

The default gives up after about 3.5 s of waiting. For long unattended jobs,
`CDXJ_REMOTE_RETRIES=8` rides out an outage of about 1.5 to 2 minutes (0.5 + 1 +
2 + 4 + 8 + 16 + 30 + 30 s of waits, plus up to 25%).

Client errors (404, 403, ...) are not retried. When a remote file cannot be read,
the error names the URL and the HTTP status, for example
`Remote location not found: https://...` or `Could not open https://...: HTTP 403 Forbidden`.

When a remote ZipNum shard is missing, cannot be read, or holds a block that
cannot be decompressed, `cdxj-search` and `zipnum-to-flat-cdxj` stop with an
error instead of returning partial results. With `--skip-errors`, `cdxj-search`
reports the error and continues with the other files. Local shards are handled
differently: `cdxj-search` skips a missing local shard or a block that cannot be
decompressed, and `zipnum-to-flat-cdxj` warns about a missing local shard and
continues.

S3 requests are also retried by the S3 client itself (s3fs and botocore), so a
failing S3 request can be attempted more times than `CDXJ_REMOTE_RETRIES` says.

## Restrictions

These rules keep an index from being used to make the tools read arbitrary
locations:

- Only `http://`, `https://` and `s3://` are accepted. Other schemes (`file://`,
  `ftp://`, ...) are rejected.
- Remote URLs must not contain credentials (`https://user:password@...`), a query
  string or a fragment. Use the environment or a config profile for credentials.
- HTTP redirects sent by the server are followed only within the origin of the
  requested URL: the same scheme, host and port. A redirect to another origin is
  an error, and that origin is not contacted. In practice:
  - a server that upgrades `http://` to `https://` on the same host is refused:
    give the `https://` URL;
  - hosts that redirect downloads to another domain or a CDN are not supported:
    give the final location instead;
  - signed object-store URLs cannot be used, since remote locations must not
    have a query string.
- Shard locations read from a `.loc` file or an `.idx` must stay inside the
  index's base location: the directory of the `.idx`, or `--base-dir`. Relative
  entries are resolved against the base, and `.` segments are dropped (S3 would
  otherwise take them literally); absolute entries are accepted only if they fall
  inside it. Entries with control characters are rejected, and so are local
  paths under a remote base and URLs under a local base.
  - HTTP(S): the same scheme and host (case-insensitive) and port, and a path in
    the base directory or below it, compared after percent-decoding. Paths must
    use UTF-8 percent-encoding: other encodings are rejected, and so are `..` and
    its variants (`%2e%2e`, `..;`, `...`, `..%20`), and, below the base, an
    encoded `/` (`%2f`) or `%` (`%25`).
  - Under a remote base (HTTP(S) or S3), entries with format characters
    (bidirectional text controls, zero-width characters) or line separators are
    rejected too, since they can disguise a URL.
  - S3: the same bucket and a key below the base, compared exactly as S3 reads
    them (case matters, nothing is decoded). Empty, `.` and `..` key segments and
    `%` below the base are rejected.
  - Local paths: resolved with symlinks followed, and checked to stay inside the
    base directory. File names may contain format characters, such as the
    zero-width non-joiner some scripts use.

Writing indexes to remote locations (for example `flat-cdxj-to-zipnum` straight to
S3) is not supported. Write locally and upload the result.
