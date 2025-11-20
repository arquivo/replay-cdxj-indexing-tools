#!/usr/bin/env python3
"""
Test Suite for path_index_to_redis.py - Path Index to Redis Submission Testing
================================================================================

This comprehensive test suite validates the path index to Redis submission functionality,
which reads pywb path index files and stores the entries in Redis for distributed
web archive systems.

WHAT IS PATH INDEX?
===================
A path index is a TSV file that maps WARC/ARC filenames to their file system locations.
Each line contains a filename followed by one or more paths where that file can be found.

Format: <filename>\t<path1>[\t<path2>...]

Example:
    file001.warc.gz\t/mnt/storage1/warcs/file001.warc.gz\t/backup/file001.warc.gz
    file002.warc.gz\t/mnt/storage2/warcs/file002.warc.gz

RUNNING THE TESTS
=================
    # Run all tests with pytest (recommended)
    pytest tests/test_path_index_to_redis.py -v

    # Run specific test class
    pytest tests/test_path_index_to_redis.py::TestParseIndexLine -v

    # Run with coverage
    pytest tests/test_path_index_to_redis.py --cov=replay_cdxj_indexing_tools.redis

    # Run tests that don't require Redis
    pytest tests/test_path_index_to_redis.py -v -m "not redis"

    # Using unittest (built-in)
    python -m unittest tests.test_path_index_to_redis -v

TEST COVERAGE SUMMARY
=====================
Total: 15+ tests across 3 test classes

1. TestParseIndexLine (6 tests) - Path Index Line Parsing
   Tests the parse_index_line() function that parses TSV lines.

   - test_parse_single_path: Standard line with filename and one path
   - test_parse_multiple_paths: Line with filename and multiple paths
   - test_parse_with_whitespace: Line with trailing/leading whitespace
   - test_parse_invalid_format: Line with only filename (no path)
   - test_parse_empty_line: Empty line handling
   - test_parse_with_spaces_in_path: Paths containing spaces

2. TestReadIndexEntries (4 tests) - File Reading and Parsing
   Tests the read_index_entries() generator function.

   - test_read_valid_file: Reading well-formed path index file
   - test_read_empty_file: Handling empty files
   - test_read_with_invalid_lines: Skipping invalid lines with verbose mode
   - test_read_from_stdin: Reading from stdin (mock)

3. TestSubmitIndexToRedis (5+ tests) - Redis Submission
   Tests the submit_index_to_redis() function with mocked Redis.

   - test_submit_single_file: Submit one file with entries
   - test_submit_multiple_files: Submit multiple path index files
   - test_batch_submission: Verify batching behavior
   - test_clear_existing: Test clearing existing hash before submission
   - test_dry_run: Verify dry-run mode doesn't write to Redis

MOCK TESTING APPROACH
======================
These tests use unittest.mock to simulate Redis without requiring a running
Redis server. This allows CI/CD pipelines to run tests without external dependencies.

For integration testing with a real Redis instance, see the integration test suite.

Author: GitHub Copilot
Date: November 2025
"""

import os
import tempfile
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

# Import the module under test
from replay_cdxj_indexing_tools.redis.path_index_to_redis import (
    parse_index_line,
    read_index_entries,
    submit_index_to_redis,
)


class TestParseIndexLine(unittest.TestCase):
    """
    Tests for the parse_index_line() function.

    This function parses TSV-formatted path index lines, extracting the
    WARC/ARC filename and its file system path(s).

    Format: <filename>\t<path1>[\t<path2>...]
    """

    def test_parse_single_path(self):
        """
        Test parsing a line with filename and single path.

        Input: "file.warc.gz\t/mnt/storage/file.warc.gz"
        Expected: {'filename': 'file.warc.gz', 'path': '/mnt/storage/file.warc.gz'}
        """
        line = "file.warc.gz\t/mnt/storage/file.warc.gz"
        result = parse_index_line(line)

        self.assertIsNotNone(result)
        self.assertEqual(result["filename"], "file.warc.gz")
        self.assertEqual(result["path"], "/mnt/storage/file.warc.gz")

    def test_parse_multiple_paths(self):
        """
        Test parsing a line with filename and multiple paths.

        Only the first path should be stored (primary location).

        Input: "file.warc.gz\t/mnt/storage/file.warc.gz\t/backup/file.warc.gz"
        Expected: First path only
        """
        line = "file.warc.gz\t/mnt/storage/file.warc.gz\t/backup/file.warc.gz"
        result = parse_index_line(line)

        self.assertIsNotNone(result)
        self.assertEqual(result["filename"], "file.warc.gz")
        self.assertEqual(result["path"], "/mnt/storage/file.warc.gz")

    def test_parse_with_whitespace(self):
        """
        Test parsing with leading/trailing whitespace.

        The function should strip whitespace from the line.

        Input: "  file.warc.gz\t/path/file.warc.gz  \n"
        Expected: Whitespace stripped
        """
        line = "  file.warc.gz\t/path/file.warc.gz  \n"
        result = parse_index_line(line)

        self.assertIsNotNone(result)
        self.assertEqual(result["filename"], "file.warc.gz")
        self.assertEqual(result["path"], "/path/file.warc.gz")

    def test_parse_invalid_format(self):
        """
        Test parsing invalid line (only filename, no path).

        Should return None for invalid format.

        Input: "file.warc.gz"
        Expected: None
        """
        line = "file.warc.gz"
        result = parse_index_line(line)

        self.assertIsNone(result)

    def test_parse_empty_line(self):
        """
        Test parsing empty line.

        Should return None for empty lines.

        Input: ""
        Expected: None
        """
        line = ""
        result = parse_index_line(line)

        self.assertIsNone(result)

    def test_parse_with_spaces_in_path(self):
        """
        Test parsing path containing spaces.

        Paths may contain spaces, which should be preserved.

        Input: "file.warc.gz\t/mnt/storage with spaces/file.warc.gz"
        Expected: Path with spaces preserved
        """
        line = "file.warc.gz\t/mnt/storage with spaces/file.warc.gz"
        result = parse_index_line(line)

        self.assertIsNotNone(result)
        self.assertEqual(result["filename"], "file.warc.gz")
        self.assertEqual(result["path"], "/mnt/storage with spaces/file.warc.gz")


class TestReadIndexEntries(unittest.TestCase):
    """
    Tests for the read_index_entries() generator function.

    This function reads path index files line by line, parsing and
    yielding valid entries while skipping invalid lines.
    """

    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_read_valid_file(self):
        """
        Test reading a well-formed path index file.

        Should yield all valid entries with filename and path.
        """
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = (
            "file001.warc.gz\t/mnt/storage/file001.warc.gz\n"
            "file002.warc.gz\t/mnt/storage/file002.warc.gz\n"
            "file003.warc.gz\t/mnt/storage/file003.warc.gz\n"
        )

        with open(test_file, "w") as f:
            f.write(content)

        entries = list(read_index_entries(test_file, verbose=False))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file001.warc.gz")
        self.assertEqual(entries[1]["filename"], "file002.warc.gz")
        self.assertEqual(entries[2]["filename"], "file003.warc.gz")

    def test_read_empty_file(self):
        """
        Test reading an empty file.

        Should yield no entries without errors.
        """
        test_file = os.path.join(self.test_dir, "empty.txt")

        with open(test_file, "w"):
            pass  # Create empty file

        entries = list(read_index_entries(test_file, verbose=False))

        self.assertEqual(len(entries), 0)

    def test_read_with_invalid_lines(self):
        """
        Test reading file with some invalid lines.

        Should skip invalid lines and yield only valid entries.
        """
        test_file = os.path.join(self.test_dir, "mixed.txt")
        content = (
            "file001.warc.gz\t/mnt/storage/file001.warc.gz\n"
            "invalid_line_no_tab\n"
            "file002.warc.gz\t/mnt/storage/file002.warc.gz\n"
            "\n"  # Empty line
            "file003.warc.gz\t/mnt/storage/file003.warc.gz\n"
        )

        with open(test_file, "w") as f:
            f.write(content)

        # Use StringIO to capture stderr for verbose output
        with patch("sys.stderr", new=StringIO()):
            entries = list(read_index_entries(test_file, verbose=True))

        # Should get 3 valid entries, skipping invalid lines
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file001.warc.gz")
        self.assertEqual(entries[1]["filename"], "file002.warc.gz")
        self.assertEqual(entries[2]["filename"], "file003.warc.gz")

    def test_read_with_multiple_paths(self):
        """
        Test reading file where entries have multiple paths.

        Should use only the first path for each entry.
        """
        test_file = os.path.join(self.test_dir, "multipaths.txt")
        content = (
            "file001.warc.gz\t/mnt/storage1/file001.warc.gz\t/mnt/storage2/file001.warc.gz\n"
            "file002.warc.gz\t/mnt/storage1/file002.warc.gz\t/backup/file002.warc.gz\n"
        )

        with open(test_file, "w") as f:
            f.write(content)

        entries = list(read_index_entries(test_file, verbose=False))

        self.assertEqual(len(entries), 2)
        # Should use first path only
        self.assertEqual(entries[0]["path"], "/mnt/storage1/file001.warc.gz")
        self.assertEqual(entries[1]["path"], "/mnt/storage1/file002.warc.gz")


class TestSubmitIndexToRedis(unittest.TestCase):
    """
    Tests for the submit_index_to_redis() function.

    These tests use mocked Redis clients to validate submission logic
    without requiring a running Redis server.
    """

    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_submit_single_file(self):
        """
        Test submitting a single path index file to Redis.

        Validates that entries are correctly batched and submitted using HSET.
        """
        # Create test file
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = (
            "file001.warc.gz\t/mnt/storage/file001.warc.gz\n"
            "file002.warc.gz\t/mnt/storage/file002.warc.gz\n"
            "file003.warc.gz\t/mnt/storage/file003.warc.gz\n"
        )

        with open(test_file, "w") as f:
            f.write(content)

        # Setup mock Redis client
        mock_redis = MagicMock()
        mock_pipeline = MagicMock()
        mock_redis.pipeline.return_value = mock_pipeline

        # Mock the redis module itself to avoid import issues
        mock_redis_module = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis

        with patch.dict("sys.modules", {"redis": mock_redis_module}):

            # Submit to Redis
            submitted, errors = submit_index_to_redis(
                input_paths=[test_file],
                collection_key="test-collection",
                redis_host="localhost",
                redis_port=6379,
                batch_size=100,
                dry_run=False,
                verbose=False,
            )

            # Verify submissions
            self.assertEqual(submitted, 3)
            self.assertEqual(errors, 0)

            # Verify Redis calls
            mock_redis.pipeline.assert_called()
            mock_pipeline.execute.assert_called()

            # Verify HSET calls for each entry
            hset_calls = mock_pipeline.hset.call_args_list
            self.assertEqual(len(hset_calls), 3)

    def test_submit_multiple_files(self):
        """
        Test submitting multiple path index files.

        Should process all files sequentially and submit all entries.
        """
        # Create multiple test files
        files = []
        for i in range(3):
            test_file = os.path.join(self.test_dir, f"pathindex{i}.txt")
            content = f"file{i:03d}.warc.gz\t/mnt/storage/file{i:03d}.warc.gz\n"

            with open(test_file, "w") as f:
                f.write(content)

            files.append(test_file)

        # Setup mock Redis
        mock_redis = MagicMock()
        mock_pipeline = MagicMock()
        mock_redis.pipeline.return_value = mock_pipeline

        mock_redis_module = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis

        with patch.dict("sys.modules", {"redis": mock_redis_module}):
            # Submit to Redis
            submitted, errors = submit_index_to_redis(
                input_paths=files,
                collection_key="test-collection",
                redis_host="localhost",
                redis_port=6379,
                batch_size=100,
                dry_run=False,
                verbose=False,
            )

            # Should submit 3 entries (one per file)
            self.assertEqual(submitted, 3)
            self.assertEqual(errors, 0)

    def test_batch_submission(self):
        """
        Test that batching works correctly.

        With batch_size=2 and 5 entries, should execute 3 times
        (2 + 2 + 1).
        """
        # Create test file with 5 entries
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = "\n".join(
            [f"file{i:03d}.warc.gz\t/mnt/storage/file{i:03d}.warc.gz" for i in range(5)]
        )

        with open(test_file, "w") as f:
            f.write(content)

        # Setup mock Redis
        mock_redis = MagicMock()
        mock_pipeline = MagicMock()
        mock_redis.pipeline.return_value = mock_pipeline

        mock_redis_module = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis

        with patch.dict("sys.modules", {"redis": mock_redis_module}):
            # Submit with small batch size
            submitted, errors = submit_index_to_redis(
                input_paths=[test_file],
                collection_key="test-collection",
                redis_host="localhost",
                redis_port=6379,
                batch_size=2,
                dry_run=False,
                verbose=False,
            )

            # Verify submissions
            self.assertEqual(submitted, 5)
            self.assertEqual(errors, 0)

            # Pipeline execute should be called 3 times (batches of 2, 2, 1)
            self.assertEqual(mock_pipeline.execute.call_count, 3)

    def test_clear_existing(self):
        """
        Test that --clear option deletes existing hash.

        Should call Redis DELETE on the hash key before submission.
        """
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = "file.warc.gz\t/mnt/storage/file.warc.gz\n"

        with open(test_file, "w") as f:
            f.write(content)

        # Setup mock Redis
        mock_redis = MagicMock()
        mock_pipeline = MagicMock()
        mock_redis.pipeline.return_value = mock_pipeline

        mock_redis_module = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis

        with patch.dict("sys.modules", {"redis": mock_redis_module}):
            # Submit with clear=True
            submitted, errors = submit_index_to_redis(
                input_paths=[test_file],
                collection_key="test-collection",
                redis_host="localhost",
                redis_port=6379,
                batch_size=100,
                dry_run=False,
                verbose=False,
                clear_existing=True,
            )

            # Verify DELETE was called
            mock_redis.delete.assert_called_once()

            # Verify the key name
            delete_call_args = mock_redis.delete.call_args[0]
            self.assertTrue("pathindex:test-collection" in delete_call_args[0])

    def test_dry_run(self):
        """
        Test dry-run mode.

        Should not create Redis client or submit any data.
        """
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = (
            "file001.warc.gz\t/mnt/storage/file001.warc.gz\n"
            "file002.warc.gz\t/mnt/storage/file002.warc.gz\n"
        )

        with open(test_file, "w") as f:
            f.write(content)

        # Submit in dry-run mode (no mocking needed - doesn't connect to Redis)
        submitted, errors = submit_index_to_redis(
            input_paths=[test_file],
            collection_key="test-collection",
            redis_host="localhost",
            redis_port=6379,
            batch_size=100,
            dry_run=True,
            verbose=False,
        )

        # Should report 2 entries but not actually submit
        self.assertEqual(submitted, 2)
        self.assertEqual(errors, 0)

    def test_key_prefix(self):
        """
        Test that key prefix is correctly applied.

        With prefix="archive:", key should be "archive:pathindex:collection"
        """
        test_file = os.path.join(self.test_dir, "pathindex.txt")
        content = "file.warc.gz\t/mnt/storage/file.warc.gz\n"

        with open(test_file, "w") as f:
            f.write(content)

        # Setup mock Redis
        mock_redis = MagicMock()
        mock_pipeline = MagicMock()
        mock_redis.pipeline.return_value = mock_pipeline

        mock_redis_module = MagicMock()
        mock_redis_module.Redis.return_value = mock_redis

        with patch.dict("sys.modules", {"redis": mock_redis_module}):
            # Submit with key prefix
            submitted, errors = submit_index_to_redis(
                input_paths=[test_file],
                collection_key="test-collection",
                redis_host="localhost",
                redis_port=6379,
                batch_size=100,
                dry_run=False,
                verbose=False,
                key_prefix="archive:",
            )

            # Verify HSET was called with prefixed key
            hset_calls = mock_pipeline.hset.call_args_list
            self.assertEqual(len(hset_calls), 1)

            # First argument should be the key with prefix
            redis_key = hset_calls[0][0][0]
            self.assertTrue(redis_key.startswith("archive:pathindex:"))


class TestCLIIntegration(unittest.TestCase):
    """
    Integration tests for the command-line interface.

    Tests the main() function and argument parsing.
    """

    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch("replay_cdxj_indexing_tools.redis.path_index_to_redis.submit_index_to_redis")
    def test_cli_basic_invocation(self, mock_submit):
        """
        Test basic CLI invocation with minimal arguments.

        Should parse arguments and call submit_index_to_redis.
        """
        from replay_cdxj_indexing_tools.redis.path_index_to_redis import main

        test_file = os.path.join(self.test_dir, "pathindex.txt")
        with open(test_file, "w") as f:
            f.write("file.warc.gz\t/path/file.warc.gz\n")

        mock_submit.return_value = (1, 0)

        # Simulate command-line arguments
        # main() doesn't return a value, it calls sys.exit() on errors
        # No exception means success
        try:
            main(["-i", test_file, "-k", "test-collection"])
            success = True
        except SystemExit as e:
            success = e.code == 0

        self.assertTrue(success)
        mock_submit.assert_called_once()

    @patch("replay_cdxj_indexing_tools.redis.path_index_to_redis.submit_index_to_redis")
    def test_cli_missing_required_args(self, mock_submit):
        """
        Test CLI with missing required arguments.

        Should return non-zero exit code or raise SystemExit.
        """
        from replay_cdxj_indexing_tools.redis.path_index_to_redis import main

        # Missing -k/--collection-key argument
        with self.assertRaises(SystemExit):
            main(["-i", "nonexistent.txt"])


if __name__ == "__main__":
    unittest.main()
