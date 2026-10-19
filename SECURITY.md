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
| Blocklist file lines (`filter-blocklist -b`) | **Untrusted** | Regex patterns; may come from third parties |
| Timestamps (`from_ts` / `to_ts`) | **Untrusted** | Validated to digits-only before use |
| Redis credentials | **Trusted** | Must be supplied by the operator |
| `arclist_folder` path | **Trusted** | Must not be attacker-controlled |
| `ADDFIELD_PLUGIN_DIR` and `--function` file (`addfield-to-flat-cdxj`) | **Trusted** | Executed as Python code; must be operator-controlled (see §6) |

---

## Threat Model

### 1. Path Traversal via `.loc` or `.idx` shard names (CWE-22)

**Component:** `replay_cdxj_indexing_tools/search/zipnum_search.py` —
`search_zipnum_file()`; `replay_cdxj_indexing_tools/zipnum/zipnum_to_flat_cdxj.py` —
`resolve_shard_path()`

**Threat:** A crafted `.loc` file or `.idx` shard-name field containing
`../../` sequences could redirect file opens to arbitrary paths outside the
intended `base_dir` (e.g., `/etc/passwd`, private key files).

**Mitigation:**
- Every shard path is checked with `path_safety.resolve_within()` before any
  `open()` call: both paths go through `os.path.realpath()` and containment is
  tested with `os.path.commonpath()`. `zipnum-to-flat-cdxj` uses the same check.
- Symlinks are followed and fully resolved before the check, preventing
  symlink-escape bypasses. Paths containing NUL bytes, and paths that
  `realpath()` cannot fully resolve (e.g. symlink loops), are rejected.
- Rejected paths raise `ValueError` before any file is opened. In
  `search_zipnum_file()` (cdxj-search) they are also logged at `WARNING` level
  via the module logger.
- In `search_zipnum_file()` (cdxj-search), shard file handles are opened with
  `O_NOFOLLOW` where the OS supports it.
- A base directory of `/` (explicit `--base-dir /`, or the default when the
  `.idx` lives in `/`) accepts any absolute path, because the base directory is
  trusted configuration.

**Tests:** `tests/test_path_safety.py`, `test_zipnum_path_traversal_*` in
`tests/test_cdxj_search.py`, `test_resolve_path_traversal_*` in
`tests/test_zipnum_to_flat_cdxj.py`

---

### 2. ReDoS via user-supplied regex patterns (CWE-1333)

**Components:** `replay_cdxj_indexing_tools/regex_safety.py` (Python `re`
checks, used by `search/filters.py`: `CDXJFilter`, `cdxj-search --filter
field~regex`) and `filter/blocklist.py` (`filter-blocklist -b blocklist.txt`,
matched by grep).

**Threat:** Whoever controls a filter expression or a blocklist line could
supply a regex that is very expensive to match, tying up the process for an
unbounded time (denial-of-service).

**Mitigation (defence in depth):**
- *Denylist heuristic (filters)* -- `check_regex_shape()` /
  `compile_safe_regex()` reject patterns longer than `MAX_PATTERN_LEN`
  (1 000 chars) and known catastrophic-backtracking shapes: a group ending in
  a variable quantifier that is itself repeated (`(x+)+`, `(x+){2,}`,
  `(x{1,10}){1,10}`, ...), repeated alternation (`(a|b)*`), and `(?x)` /
  `(?#...)`, which can hide those shapes. The heuristic is not exhaustive;
  the time limit is the primary bound.
- *Time limit (filters)* -- every `~`/`!~` match runs under
  `search_with_time_limit()`, a `SIGALRM`/`setitimer` watchdog. The limit
  (default 1 s) applies to each search, not to the whole run. A timeout
  raises `RegexTimeoutError` and `cdxj-search` aborts with exit status 124
  instead of skipping the record: skipping would yield silently incomplete
  results and still let a hostile pattern cost one full timeout per record.
  The time limit is **not active** (only the denylist applies; a warning is
  logged once per process) when:
  - `setitimer`/`SIGALRM` is unavailable (e.g. Windows);
  - matching runs outside the main thread;
  - another component already has an `ITIMER_REAL`/`alarm()` timer armed
    (it is left untouched);
  - the current `SIGALRM` handler was not installed from Python.

  Library, threaded or server embedders should wrap their matching loop in
  `regex_time_limit()` on the main thread, or bound the work at the process
  level (e.g. a worker process with a deadline).
- *grep (blocklist)* -- blocklist lines are matched by `grep -E`, which is
  mostly DFA-based. The Python-oriented denylist does not apply (it would
  reject common SURT idioms); instead `check_grep_pattern()` rejects the few
  constructs that are expensive for grep: back-references, repeat counts above
  255 and lines longer than 10 000 characters. Three limits target grep's
  set-up work. Per line, wildcards and bounded repeats cost at most 256: plain
  characters cost nothing, a `.` costs 4, a bracket expression or `\w`, `\W`,
  `\s`, `\S` costs 1, and a bounded repeat costs its expanded size (grep
  copies the repeated part once per count, so nested repeats multiply). Per
  blocklist, the expanded sizes of all bounded repeats add up to at most
  32 768, and the costs of all wildcards outside bounded repeats to at most
  1 048 576. See `docs/tools/filter-blocklist.md` for the rules.
- *Blocklist timeout* -- these limits target grep's set-up work, not its
  matching time on adversarial input, and they are not exhaustive. When
  blocklist lines are not fully trusted, the real bound is `filter-blocklist
  --timeout` (default: no limit; at most 604 800 s, one week; the clock
  includes time spent waiting on stdin; exit status 124 on timeout). Size it
  for the largest CDXJ input you filter. The `cdxj-index-collection.sh`
  pipeline passes it on from `--blocklist-timeout SECONDS` or the
  `BLOCKLIST_TIMEOUT` environment variable (default `0`, no limit), shows the
  filter's error message, and fails the filtering stage on a timeout or any
  other error.
- *Fail closed* -- if **any** line is rejected the tool exits 1, writes no
  output and reports each rejected line number and reason on stderr, since
  filtering with part of the blocklist would publish the records the rejected
  lines target. The library path fails closed too: `load_blocklist()` raises
  `ValueError` if any line is not a valid Python regex, and
  `filter_cdxj_by_blocklist()` applies the same grep checks to every pattern
  it is given. grep also fails closed on its own errors (including being
  killed by a signal) and runs with `--` before the input path.

**Tests:** `tests/test_regex_safety.py`

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

### 6. Arbitrary code execution via `--function` plugin files (CWE-94)

**Component:** `replay_cdxj_indexing_tools/addfield/addfield_to_flat_cdxj.py` —
`load_addfield_function()` (CLI: `addfield-to-flat-cdxj --function`)

**Threat:** `--function` executes a Python file's module-level code with the
full privileges of the process. Anyone who can choose the path, or write to
the file or its directory, gets code execution as the operator. This is a
deliberate, trusted-operator-only feature: the allowlist below is a path and
permission filter, **not a sandbox** — any file accepted from the plugin
directory runs unrestricted.

**Mitigation:**
- **Fail closed:** `--function` (and `load_addfield_function()`) refuse to load
  anything unless `ADDFIELD_PLUGIN_DIR` is set. This changed the default:
  operators using `--function` (versions up to and including 1.2.6 had no
  checks) must now set `ADDFIELD_PLUGIN_DIR`.
- The resolved (`os.path.realpath()`) function path must be directly inside the
  resolved plugin directory; symlink escapes and subdirectories are rejected
  (CWE-22). `ADDFIELD_PLUGIN_DIR` itself may be a symlink (or start with `~`):
  it is expanded and resolved once with `realpath()`. A value that resolves to
  `/` (e.g. `~` with an empty `HOME`) is refused.
- The plugin directory and the file must be owned by the current user or root
  and must not be group- or world-writable (CWE-732); shared directories such
  as `/tmp` are refused.
- The resolved directory is opened with `O_NOFOLLOW`; the file is `lstat()`ed
  relative to that directory descriptor (FIFOs, device nodes and symlinks are
  rejected before any `open()`), then opened relative to it with
  `O_NOFOLLOW | O_NONBLOCK | O_NOCTTY`. Ownership, permissions and type are
  checked with `fstat()` on the open descriptors, and the code is compiled from
  the bytes read from that same descriptor — the file is never re-opened by
  path, closing the check-to-load race (CWE-367). Only the final components get
  `O_NOFOLLOW`; intermediate components of the resolved path are not re-checked.
- Every load and every refusal is logged at `WARNING` level (path `repr()`-quoted,
  so crafted names cannot forge log lines); the CLI configures a stderr handler
  so this is emitted even when the caller has not configured logging.
- POSIX-only: on platforms without POSIX ownership/permission semantics,
  `--function` is refused.

**Operator responsibilities:**
- Keep `ADDFIELD_PLUGIN_DIR` and all its parent directories (and, if it is a
  symlink, its target's parents) writable only by the operator account or root.
- Use a dedicated directory that contains only reviewed plugins — never `$HOME`,
  the working directory, or input/output directories. The owner check proves
  who owns a file, not who wrote its contents.
- Never derive `--function` or `ADDFIELD_PLUGIN_DIR` from untrusted or remote
  input.
- Keep stderr from `--function` runs if the audit trail matters.

**Tests:** `tests/test_addfield_to_flat_cdxj.py` — `TestLoadAddfieldFunctionSecurity`

---

## CWE Reference Summary

| CWE | Description | Affected component |
|-----|-------------|-------------------|
| CWE-22  | Path Traversal | `zipnum_search.search_zipnum_file()` |
| CWE-1333 | ReDoS | `regex_safety` (used by `filters.CDXJFilter`), `blocklist.check_grep_pattern` |
| CWE-400 | Uncontrolled Resource Consumption | `zipnum_search` block reads; `arclist_index_to_redis` pipeline |
| CWE-78  | OS Command Injection | `arclist_index_to_redis.run_pipeline()` (mitigated via list args) |
| CWE-306 | Missing Authentication | `path_index_to_redis.submit_index_to_redis()` remote Redis |
| CWE-94  | Code Injection | `addfield_to_flat_cdxj.load_addfield_function()` (`--function`; fail-closed plugin dir; hardening: CWE-22/732/367) |
