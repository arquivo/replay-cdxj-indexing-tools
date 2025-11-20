# Test Suite Documentation

This directory contains comprehensive test suites for both Python tools and shell scripts in the replay-cdxj-indexing-tools package.

## Test Organization

This folder has multiple files, each one tests a particular functionality.

### Python Tests (pytest)

Test coverage for Python CLI tools:

- **`test_filter_blocklist.py`** - Blocklist filtering functionality
  - Pattern matching (regex, exact, wildcards)
  - Comment handling
  - Edge cases (empty files, malformed patterns)
  - CLI argument validation
  - Verbose logging output

- **`test_filter_excessive_urls.py`** - Excessive URL detection and filtering
  - SURT key extraction
  - Threshold-based detection
  - URL filtering (find/remove/auto modes)
  - Performance with large files
  - Unicode and edge cases

- **`test_cdxj_to_zipnum.py`** - ZipNum conversion
  - Sharding logic
  - CDX summary generation
  - Compression handling
  - Large file processing
  - Edge cases

- **`test_merge_sorted_files.py`** - CDXJ merging
  - K-way merge algorithm
  - Multiple file handling
  - CDXJ format preservation
  - Buffer management
  - Edge cases

- **`test_path_index_to_redis.py`** - Redis path index submission
  - Path index parsing
  - Redis connection handling
  - Batch operations
  - Error handling
  - Entry validation

- **`test_cdxj_search.py`** - Binary search in CDXJ/ZipNum files
  - Binary search algorithm
  - SURT key matching
  - ZipNum index navigation
  - Compressed file handling
  - Edge cases and boundary conditions

### Shell Script Tests

#### Bats Test Suite (Advanced)

`test_process_collection.bats` - Comprehensive test suite for `cdxj-index-collection`

**Requirements:**
```bash
sudo apt-get install bats
```

**Run:**
```bash
bats tests/test_process_collection.bats
```

**Coverage (13 tests):**
- Script existence and executability
- Help output validation
- Argument parsing (all flags)
- Error handling (missing/empty collections)
- Configuration validation
- Function testing (index_warc)
- Dependency checking

#### Simple Bash Test Suite (Standalone)

`test_process_collection_simple.sh` - Pure bash test suite (no dependencies)

**Run:**
```bash
bash tests/test_process_collection_simple.sh
```

**Coverage (15 tests):**
- Script validation (exists, executable, syntax)
- Help and usage output
- Command-line flags (--incremental, --jobs, --threshold, etc.)
- Error handling (missing collection, empty directory, non-existent paths)
- Blocklist validation (missing, custom paths)
- Unknown option rejection
- Confirmation prompt verification

**Advantages:**
- No external dependencies (pure bash)
- Colored output with detailed results
- Proper setup/teardown with temporary test environments
- Safe to run without affecting system

## Running All Tests

### Quick Test

Run everything:
```bash
# Python tests
make test-python

# Shell tests (simple)
make test-shell

# Full CI suite (tests + coverage + lint)
make ci
```

### Multi-Version Testing locally with Docker

Test against multiple Python versions using Docker containers:

```bash
# Test on specific Python version
make ci-py38   # Python 3.8
make ci-py39   # Python 3.9
make ci-py310  # Python 3.10
make ci-py311  # Python 3.11
make ci-py312  # Python 3.12

# Test on all Python versions
make ci-all

# Run all versions in parallel (faster)
make --jobs 10 ci-all
```

Each Docker-based CI target runs the full test suite (tests with coverage, flake8, pylint, and mypy) in an isolated container for that Python version. This ensures compatibility without needing to install multiple Python versions locally.

### CI/CD Pipeline

This repository uses Github Actions for quality testing for multi Python versions.

## Test Maintenance

### Running Specific Tests

```bash
# Single Python test file
pytest tests/test_filter_blocklist.py -v

# Single test class
pytest tests/test_filter_blocklist.py::TestLoadBlocklist -v

# Single test method
pytest tests/test_filter_blocklist.py::TestLoadBlocklist::test_load_basic -v

# Tests matching pattern
pytest tests/ -k "blocklist" -v
```

### Debugging Failed Tests

```bash
# Show full output
pytest tests/ -v -s

# Stop on first failure
pytest tests/ -x

# Show local variables on failure
pytest tests/ -l

# Run with debugger
pytest tests/ --pdb
```

### Coverage Reports

```bash
# Generate coverage report
pytest tests/ --cov=replay_cdxj_indexing_tools --cov-report=html

# View in browser
firefox htmlcov/index.html
```

## Continuous Integration

### Pre-commit Hook

Create `.git/hooks/pre-commit`:
```bash
#!/bin/bash
set -e

echo "Running tests..."
pytest tests/ -v
bash tests/test_process_collection_simple.sh

echo "All tests passed!"
```

```bash
chmod +x .git/hooks/pre-commit
```

### Daily Cron Tests

```bash
# Run tests daily and email results
0 2 * * * cd /path/to/repo && pytest tests/ && bash tests/test_process_collection_simple.sh || mail -s "Test failure" admin@example.com
```

## Troubleshooting

### Python Tests Failing

**Issue:** `ModuleNotFoundError: No module named 'replay_cdxj_indexing_tools'`

**Solution:**
```bash
pip install -e .
```

**Issue:** `FileNotFoundError` in tests

**Solution:** Tests use temporary directories - ensure pytest has write permissions.

### Shell Tests Failing

**Issue:** `cdxj-index-collection: No such file or directory`

**Solution:** Run tests from repository root:
```bash
cd /path/to/replay-cdxj-indexing-tools
bash tests/test_process_collection_simple.sh
```

**Issue:** Tests fail due to missing dependencies (GNU parallel, pywb)

**Solution:** Shell tests are designed to work without these - they test argument parsing and error handling. If dependencies are installed, integration tests can be added.

## Best Practices

1. **Test Independence**: Each test should be self-contained and not depend on other tests
2. **Temporary Files**: Always use `tmp_path` (pytest) or `mktemp` (bash) for test files
3. **Cleanup**: Use teardown functions to remove temporary files
4. **Assertions**: Use descriptive assertion messages
5. **Edge Cases**: Test boundary conditions, empty inputs, malformed data
6. **Performance**: Keep tests fast - mock expensive operations
7. **Documentation**: Add docstrings to test classes/functions explaining what they test

## Future Test Additions

Potential areas for additional test coverage:

1. **Integration Tests**: Full pipeline tests with small WARC files
2. **Performance Tests**: Benchmark large file processing
3. **Parallel Execution**: Test GNU parallel integration with mock data
4. **Error Recovery**: Test atomic write recovery from interrupted indexing
5. **Incremental Mode**: Test skip logic for already-indexed files
6. **Memory Tests**: Verify no memory leaks with large files
7. **Stress Tests**: Test with thousands of WARC files

## Contact

For questions or issues with tests:
- Open an issue on the project repository
- Check test output for detailed error messages
- Review test code for expected behavior
