# Security Policy

## Reporting Security Issues

Please **do not** file public GitHub issues for security vulnerabilities.

Send a private report to the maintainers via GitHub's
[Security Advisories](https://github.com/arquivo/replay-cdxj-indexing-tools/security/advisories/new)
feature (or e-mail the project maintainer directly if that is unavailable).

Include:
- A description of the vulnerability and affected component(s)
- Steps to reproduce (PoC code / crafted input files are welcome)
- Potential impact and suggested fix, if known

We aim to acknowledge receipt within **5 business days** and to publish a fix
within **90 days** of confirmation.

---

## Trust Boundaries

| Input | Trust level | Notes |
|-------|------------|-------|
| `base_dir` / `idx_filepath` (caller-supplied paths) | **Trusted** | Set by operators/configuration |
| `.loc` file contents | **Untrusted** | May originate from network or third-party sources |
| `.idx` file contents (shard names, offsets, lengths) | **Untrusted** | May be crafted by an adversary |
| Filter expressions (`CDXJFilter` constructor) | **Untrusted** | Passed from HTTP query parameters or CLI |
| Timestamps (`from_ts` / `to_ts`) | **Untrusted** | Validated to digits-only before use |
| Redis credentials | **Trusted** | Must be supplied by the operator |
| `arclist_folder` path | **Trusted** | Must not be attacker-controlled |

---

## Threat Model

### 1. Path Traversal via `.loc` or `.idx` shard names (CWE-22)

**Component:** `replay_cdxj_indexing_tools/search/zipnum_search.py` —
`search_zipnum_file()`

**Threat:** A crafted `.loc` file or `.idx` shard-name field containing
`../../` sequences could redirect file opens to arbitrary paths outside the
intended `base_dir` (e.g., `/etc/passwd`, private key files).

**Mitigation:**
- Every resolved shard path is checked with `os.path.realpath()` +
  `startswith(resolved_base + os.sep)` before any `open()` call.
- Symlinks are followed and fully resolved before the check, preventing
  symlink-escape bypasses.
- Detected attempts are logged at `WARNING` level (via the module logger)
  and raise `ValueError` before any file is opened.
- Shard file handles are opened with `O_NOFOLLOW` where the OS supports it.

**Tests:** `tests/test_zipnum_path_traversal_*.py`

---

### 2. ReDoS via filter regex patterns (CWE-1333)

**Component:** `replay_cdxj_indexing_tools/search/filters.py` —
`CDXJFilter._parse_filter()` / `CDXJFilter._compile_safe_regex()`

**Threat:** An attacker who controls filter expressions (e.g., via HTTP
query parameters) could supply a regex pattern that causes catastrophic
backtracking, tying up the process for an unbounded time and enabling
denial-of-service.

**Mitigation:**
- `_compile_safe_regex()` rejects patterns longer than `_MAX_PATTERN_LEN`
  (1 000 chars) before attempting compilation.
- A set of static structural detectors (`_REDOS_PATTERNS`) rejects patterns
  matching known catastrophic-backtracking forms: `(x+)+`, `(x*)+`,
  `(x+)*`, `(a|b)*`, `(a|b)+`.
- Patterns that fail `re.compile()` are rejected with `ValueError`.

---

### 3. Memory DoS from large compressed blocks (CWE-400)

**Component:** `replay_cdxj_indexing_tools/search/zipnum_search.py` —
`search_shard_blocks()` / `search_zipnum_data_block()`

**Threat:** A crafted `.idx` file could specify extremely large `length`
values, causing the tool to read (and potentially decompress) gigabytes of
data per block, exhausting process memory.

**Mitigation:**
- All block reads are capped at `_MAX_BLOCK_READ` (512 MB) regardless of the
  length value in the index.
- Decompression errors are caught and the block is skipped.

---

### 4. Subprocess timeout / pipeline hang (CWE-400 / CWE-78)

**Component:** `replay_cdxj_indexing_tools/arclist_index_to_redis.py` —
`run_pipeline()`

**Threat:** A stalled Redis server or slow/malicious arclist source could
cause the pipeline to hang indefinitely. Additionally, if `arclist_folder`
is attacker-controlled, an adversary might inject a path that causes
unintended command behaviour (via the subprocess arguments).

**Mitigation:**
- Both child subprocesses are hard-killed after `_PIPELINE_TIMEOUT` seconds
  (default 3 600 s) using `subprocess.wait(timeout=...)` + `proc.kill()`.
- The pipeline exits with code 124 on timeout (matches POSIX `timeout(1)`
  convention).
- `arclist_folder` is passed as a discrete list element to `subprocess.Popen`
  (no shell interpolation), mitigating shell-injection risks.
- Operators should ensure `arclist_folder` is not user-controlled.

---

### 5. Unauthenticated Redis connections (CWE-306)

**Component:** `replay_cdxj_indexing_tools/redis/path_index_to_redis.py` —
`submit_index_to_redis()`

**Threat:** Connecting to a remote Redis instance without a password exposes
the data store to any network-adjacent attacker who can reach the Redis port.

**Mitigation:**
- A runtime warning is printed to `stderr` when a remote host is specified
  without `--password`.
- Operators must supply `--password` (or equivalent) for all production
  deployments.

---

## CWE Reference Summary

| CWE | Description | Affected component |
|-----|-------------|-------------------|
| CWE-22  | Path Traversal | `zipnum_search.search_zipnum_file()` |
| CWE-1333 | ReDoS | `filters.CDXJFilter._compile_safe_regex()` |
| CWE-400 | Uncontrolled Resource Consumption | `zipnum_search` block reads; `arclist_index_to_redis` pipeline |
| CWE-78  | OS Command Injection | `arclist_index_to_redis.run_pipeline()` (mitigated via list args) |
| CWE-306 | Missing Authentication | `path_index_to_redis.submit_index_to_redis()` remote Redis |
