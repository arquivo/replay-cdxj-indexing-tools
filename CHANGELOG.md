# CHANGELOG

<!-- version list -->

## v1.2.0 (2026-10-07)

### Bug Fixes

- Scope stats/ gitignore rule to repo root only
  ([`183393b`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/183393bbd688a1411aacc0d6f4f955bd7a8ea190))

- **cdxj-stats**: Read input file as UTF-8 regardless of process locale
  ([`28d084d`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/28d084d0aedcd9c95e8306544a71273d5d53a913))

- **cdxj-stats**: Stop sink-option blocks swallowing single-dash flags
  ([`b4afc64`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/b4afc64a3029c083c9ff1f2f9700ca797c7a0f3e))

- **deps**: Require tldextract>=5.3.0 for top_domain_under_public_suffix
  ([`24fb993`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/24fb993c8b0ce2a381ccc354d4b4c9cfcc8cdeea))

### Chores

- Ignore stats folder
  ([`0c54c5b`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/0c54c5bc8e48bc8ff3121188ac7247ea559d7528))

### Documentation

- **cdxj-stats**: Add explicit Top-N example for domain-etld1
  ([`0a9b6f5`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/0a9b6f5ed081a13dce43ac1131ae5130ef232ba8))

- **cdxj-stats**: Document per-sink parallelism via tee
  ([`960858c`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/960858c7aba5862a8e1b5602c29d36109114892c))

- **cdxj-stats**: Document running normal + group-by=year in parallel
  ([`d5c2a21`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/d5c2a21dbb5ec84fb225334558c6e97f28c5f63f))

- **cdxj-stats**: Fix Top-N sort recipe for tab default + domain column shift
  ([`dbc1962`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/dbc1962b813c50eb736823e6cd7b525d910c4f22))

- **cdxj-stats**: Make Top-N cutoff configurable via N variable
  ([`a68872a`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/a68872a5f9102d77501a32e3c17e9a51c8f134a6))

### Features

- Add cdxj-stats pluggable capture-statistics tool
  ([`45c56d6`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/45c56d6e725dabdd584148fd84d896bca63e93e7))

- **cdxj-stats**: Tab-separated output, readable domain column, non-SURT keys
  ([`4f5df9a`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/4f5df9a88df211b7357c27ccbebb59994aa6a2dc))


## v1.1.4 (2026-09-29)

### Bug Fixes

- **ci**: Stop chown's exit code from masking make ci's real result
  ([`0798dce`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/0798dcec6e406cc8c311df67fcc3bc8fec5f4709))

### Chores

- Add .claude to .gitignore
  ([`3956d77`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/3956d774463dec38663d05245bf77631fb2cdc6a))

- Drop support for EOL Python 3.8 and 3.9
  ([`98b5dd5`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/98b5dd51a04f84dec59853cb9121c1e26ae3646b))


## v1.1.3 (2026-07-20)

### Bug Fixes

- Add input validation for search_key/filters and cached regex compilation (#40 #22)
  ([`46ef86d`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/46ef86d290fffc08a6ff41e48de959037698e601))


## v1.1.2 (2026-07-20)

### Bug Fixes

- Add missing targeted pylint inline disables to pass CI
  ([`b72ea05`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/b72ea0561cda37d3400f17768da5919e7c491f89))

### Chores

- Re-enable targeted pylint checks and add operator guides
  ([`00a08a9`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/00a08a902c34e31098de80a9872aaf08058be3d4))


## v1.1.1 (2026-07-20)

### Bug Fixes

- Add audit logging for path traversal/validation and threat model docs
  ([`822b3cf`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/822b3cf54cb880ebbe6278b92ba0b410d5e72fc4))


## v1.1.0 (2026-07-20)

### Continuous Integration

- **docker**: Trigger versioned builds on GitHub Release published
  ([`c2eafb3`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/c2eafb3065e17b47548797506b25cda1050a0e14))

### Features

- **release**: Enable GitHub Release creation on semantic release
  ([`fefd802`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/fefd80270f2dbcb025c2ccb6e036fa25926c60c9))


## v1.0.1 (2026-07-17)

### Bug Fixes

- **tests**: Apply isort --profile black formatting to test imports
  ([`fd6bec3`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/fd6bec36336279ae8b12b333ec5ee0f0f5eee771))

### Continuous Integration

- Run semantic-release only after Tests workflow succeeds
  ([`45abf27`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/45abf27d5f22f77df59499826869aa9eb52535ff))

### Testing

- Add unit tests for arclist_index_to_redis and process_collection_wrapper
  ([`8c4d0e6`](https://github.com/arquivo/replay-cdxj-indexing-tools/commit/8c4d0e6b6fd2dcd0934648555817fe0f46e7c32c))


## v1.0.0 (2026-07-17)

- Initial Release
