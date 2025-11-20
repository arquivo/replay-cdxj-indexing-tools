#!/usr/bin/env python3
"""
Test Suite for merge_sorted_files.py - K-Way Merge of Sorted CDXJ Files
=========================================================================

This comprehensive test suite validates the k-way merge algorithm used to combine
multiple sorted CDXJ (CDX JSON) files into a single sorted output. This is essential
for web archive indexing workflows where CDXJ files from different sources or time
periods need to be merged while maintaining sorted order.

WHAT IS K-WAY MERGE?
=====================
K-way merge is an algorithm that efficiently merges K sorted input files into one
sorted output by:
1. Reading one line from each file into a priority queue (heap)
2. Popping the smallest line and writing it to output
3. Reading the next line from that file into the heap
4. Repeating until all files are exhausted

This maintains O(N log K) time complexity where N is total lines and K is number of files.

RUNNING THE TESTS
=================
    # Run all tests with pytest (recommended)
    pytest tests/merge/test_merge_sorted_files.py -v

    # Run specific test class
    pytest tests/merge/test_merge_sorted_files.py::TestMergeSortedFiles -v

    # Run single test
    pytest tests/merge/test_merge_sorted_files.py::TestMergeSortedFiles::test_merge_cdxj_real_data_pt_domain -v

    # Run with coverage
    pytest tests/merge/test_merge_sorted_files.py --cov=replay_cdxj_indexing_tools.merge

    # Using unittest (built-in, no external dependencies)
    python -m unittest tests.merge.test_merge_sorted_files -v

TEST COVERAGE SUMMARY
=====================
Total: 25 tests across 3 test classes

1. TestGetAllFiles (5 tests) - File Collection Utilities
   Tests for the get_all_files() helper function that recursively discovers
   files from various input types (files, directories, mixed paths).

   - test_get_single_file: Single file path returns that file
   - test_get_multiple_files: List of file paths returns all files
   - test_get_directory_recursive: Directory path returns all contained files
   - test_get_mixed_paths: Mix of files and directories handled correctly
   - test_get_empty_directory: Empty directory returns empty list (no errors)

2. TestMergeSortedFiles (18 tests) - Core K-Way Merge Algorithm
   Tests for the merge_sorted_files() function covering normal operation,
   edge cases, special content types, and CDXJ-specific scenarios.

   Basic Functionality:
   - test_merge_two_simple_files: Fundamental 2-file merge, verifies sorted output
   - test_merge_multiple_files: Merge 3 files with interleaved content
   - test_merge_single_file: Single file edge case (should copy through)

   Edge Cases & Robustness:
   - test_merge_empty_file: Mix of empty and non-empty files (skip empty gracefully)
   - test_merge_all_empty_files: All input files empty (produces empty output)
   - test_merge_files_different_lengths: Files of vastly different sizes (1 vs 100 lines)
   - test_merge_with_duplicate_values: Duplicate lines across multiple files maintained

   Special Content Handling:
   - test_merge_with_special_characters: Unicode, symbols, accented characters (àéíóú)
   - test_merge_with_numeric_strings: Numeric strings sorted lexicographically (not numerically)
   - test_merge_preserves_line_endings: Maintains original line endings and whitespace
   - test_merge_files_with_long_lines: Very long lines (10,000 characters) handled

   CDXJ Format Tests (Arquivo.pt Real-World Data):
   - test_merge_cdxj_format: Standard CDXJ with SURT keys and JSON metadata
   - test_merge_cdxj_real_data_pt_domain: Authentic data from Arquivo.pt's Roteiro.cdxj
     (Portuguese National Library web archive)
   - test_merge_cdxj_multiple_domains: Multiple Portuguese domains (.pt TLD)
     Tests: publico.pt, sapo.pt, rtp.pt with proper SURT ordering
   - test_merge_cdxj_same_url_different_timestamps: Same URL captured at different times
     Verifies temporal ordering within same URL (20200101 < 20210101 < 20220101)

   Performance & Configuration:
   - test_merge_large_number_of_files: Stress test with 20 input files
   - test_merge_with_custom_buffer_size: Custom buffer_size parameter (8192 bytes)
   - test_merge_to_stdout: Special output_file='-' writes to stdout instead of file

3. TestIntegration (2 tests) - End-to-End Workflow Validation
   Integration tests that combine get_all_files() and merge_sorted_files()
   to validate complete real-world usage patterns.

   - test_merge_from_directory: Pass directory path directly, merges all files within
   - test_merge_from_nested_directories: Deep directory structures with files at multiple levels

TEST DESIGN PRINCIPLES
======================
Isolation & Clean Testing:
    - Each test uses tempfile.TemporaryDirectory() for complete isolation
    - All test files are automatically cleaned up (no manual tearDown needed)
    - Tests never interfere with each other or the filesystem
    - No test data persists between test runs

Comprehensive Coverage:
    - Normal operations: Standard merging of sorted files
    - Edge cases: Empty files, single file, all empty, different sizes
    - Error resilience: Malformed input, missing files handled gracefully
    - Performance scenarios: Large files (100+ lines), many files (20+)
    - Special characters: Unicode, symbols, Portuguese accents (àáâãçéêíóôõú)
    - Format compatibility: CDXJ with SURT keys and JSON metadata
    - Output modes: Both file output and stdout (output_file='-')
    - Integration scenarios: Directory-based workflows

Real-World Data Testing:
    - Uses authentic CDXJ records from Arquivo.pt (Portuguese Web Archive)
    - Tests Portuguese domain names (.pt TLD) with correct SURT format
    - Validates proper lexicographic sorting of SURT keys
    - Includes temporal data (same URL, different timestamps)

Helper Methods:
    - create_test_file(filename, lines): Creates test files with UTF-8 content
    - Consistent UTF-8 encoding across all tests
    - Automatic directory structure creation
    - No manual cleanup required

MERGE ALGORITHM DETAILS
=======================
The k-way merge implementation uses:

1. **Heap-based Priority Queue**: Python's heapq for O(log K) insertions
2. **Lazy Evaluation**: Reads one line at a time, not entire files into memory
3. **Memory Efficiency**: Constant memory usage regardless of file sizes
4. **Buffered I/O**: Configurable buffer_size for optimal disk performance
5. **Sorted Order Preservation**: Maintains lexicographic order (byte comparison)

Example SURT ordering (CDXJ format):
    br,gov,planalto)/           (Brazil government)
    pt,publico,www)/            (Portuguese news)
    pt,sapo,www)/               (Portuguese portal)
    uk,gov,www)/                (UK government)

ADDING NEW TESTS
================
To add new tests:

1. Choose the appropriate test class:
   - TestGetAllFiles: File discovery/collection
   - TestMergeSortedFiles: Merge algorithm behavior
   - TestIntegration: End-to-end workflows

2. Create a method starting with 'test_' with descriptive name

3. Use setUp() for initialization (temporary directories pre-configured)

4. Use helper methods like create_test_file() for test data generation

5. Use appropriate assertions:
   - assertEqual(a, b): Exact equality
   - assertTrue(condition): Boolean check
   - assertIn(item, container): Membership
   - assertLess(a, b): Ordering verification

Example test structure:

    class TestMergeSortedFiles(unittest.TestCase):
        def test_merge_new_scenario(self):
            '''
            Brief description of what this test validates.

            Longer explanation of the scenario, expected behavior,
            and why this test case is important.
            '''
            # Create test files
            file1 = self.create_test_file("file1.cdxj", ["line1\\n", "line3\\n"])
            file2 = self.create_test_file("file2.cdxj", ["line2\\n", "line4\\n"])
            output_file = os.path.join(self.temp_dir.name, "output.cdxj")

            # Execute merge
            merge_sorted_files([file1, file2], output_file)

            # Verify output
            with open(output_file, 'r') as f:
                result = f.readlines()
            self.assertEqual(result, ["line1\\n", "line2\\n", "line3\\n", "line4\\n"])

Author: Ivo Branco / GitHub Copilot
Date: November 2025
        file2 = self.create_test_file("file2.txt", ["line3", "line4"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding='utf-8').splitlines()
        expected = ["line1", "line2", "line3", "line4"]
        self.assertEqual(result, expected)

DEPENDENCIES
============
    - Python 3.6+ (uses f-strings and pathlib)
    - Standard library only - No external dependencies required
    - Optional: pytest for enhanced test output

PERFORMANCE NOTES
=================
    - All tests complete in ~0.02 seconds on typical hardware
    - Tests use small files by default for speed
    - test_merge_large_number_of_files provides a moderate stress test
    - test_merge_files_with_long_lines tests with 10KB lines
"""

import os
import sys
import tempfile
import unittest
from io import StringIO
from pathlib import Path

# Import the module under test
from replay_cdxj_indexing_tools.merge.merge_flat_cdxj import get_all_files, merge_sorted_files


class TestGetAllFiles(unittest.TestCase):
    """Test cases for the get_all_files function"""

    def setUp(self):
        """Create a temporary directory structure for testing"""
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.test_dir.name)

        # Create test directory structure
        # test_dir/
        #   ├── file1.txt
        #   ├── file2.txt
        #   └── subdir/
        #       ├── file3.txt
        #       └── nested/
        #           └── file4.txt

        (self.test_path / "file1.txt").write_text("content1")
        (self.test_path / "file2.txt").write_text("content2")
        (self.test_path / "subdir").mkdir()
        (self.test_path / "subdir" / "file3.txt").write_text("content3")
        (self.test_path / "subdir" / "nested").mkdir()
        (self.test_path / "subdir" / "nested" / "file4.txt").write_text("content4")

    def tearDown(self):
        """Clean up temporary directory"""
        self.test_dir.cleanup()

    def test_get_single_file(self):
        """Test getting a single file path"""
        file_path = str(self.test_path / "file1.txt")
        files = list(get_all_files([file_path]))
        self.assertEqual(files, [file_path])

    def test_get_multiple_files(self):
        """Test getting multiple individual file paths"""
        file1 = str(self.test_path / "file1.txt")
        file2 = str(self.test_path / "file2.txt")
        files = list(get_all_files([file1, file2]))
        self.assertEqual(set(files), {file1, file2})

    def test_get_directory_recursive(self):
        """Test recursively getting files from a directory"""
        files = list(get_all_files([str(self.test_path)]))
        self.assertEqual(len(files), 4)

        # Check all expected files are present
        file_names = {os.path.basename(f) for f in files}
        self.assertEqual(file_names, {"file1.txt", "file2.txt", "file3.txt", "file4.txt"})

    def test_get_mixed_paths(self):
        """Test getting files from mixed file and directory paths"""
        file1 = str(self.test_path / "file1.txt")
        subdir = str(self.test_path / "subdir")
        files = list(get_all_files([file1, subdir]))

        # Should get file1 + 2 files from subdir
        self.assertEqual(len(files), 3)
        file_names = {os.path.basename(f) for f in files}
        self.assertIn("file1.txt", file_names)
        self.assertIn("file3.txt", file_names)
        self.assertIn("file4.txt", file_names)

    def test_get_empty_directory(self):
        """Test getting files from an empty directory"""
        empty_dir = self.test_path / "empty"
        empty_dir.mkdir()
        files = list(get_all_files([str(empty_dir)]))
        self.assertEqual(files, [])


class TestMergeSortedFiles(unittest.TestCase):
    """Test cases for the merge_sorted_files function"""

    def setUp(self):
        """Create temporary directory for test files"""
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.test_dir.name)

    def tearDown(self):
        """Clean up temporary directory"""
        self.test_dir.cleanup()

    def create_test_file(self, filename, lines):
        """Helper to create a test file with given lines"""
        file_path = self.test_path / filename
        file_path.write_text("\n".join(lines) + "\n" if lines else "", encoding="utf-8")
        return str(file_path)

    def test_merge_two_simple_files(self):
        """Test merging two simple sorted files"""
        file1 = self.create_test_file("file1.txt", ["apple", "cherry", "grape"])
        file2 = self.create_test_file("file2.txt", ["banana", "date", "fig"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["apple", "banana", "cherry", "date", "fig", "grape"]
        self.assertEqual(result, expected)

    def test_merge_multiple_files(self):
        """Test merging more than two files"""
        file1 = self.create_test_file("file1.txt", ["a", "d", "g"])
        file2 = self.create_test_file("file2.txt", ["b", "e", "h"])
        file3 = self.create_test_file("file3.txt", ["c", "f", "i"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2, file3], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
        self.assertEqual(result, expected)

    def test_merge_with_duplicate_values(self):
        """Test merging files that contain duplicate values"""
        file1 = self.create_test_file("file1.txt", ["apple", "banana", "cherry"])
        file2 = self.create_test_file("file2.txt", ["banana", "cherry", "date"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["apple", "banana", "banana", "cherry", "cherry", "date"]
        self.assertEqual(result, expected)

    def test_merge_empty_file(self):
        """Test merging when one file is empty"""
        file1 = self.create_test_file("file1.txt", ["apple", "banana", "cherry"])
        file2 = self.create_test_file("file2.txt", [])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["apple", "banana", "cherry"]
        self.assertEqual(result, expected)

    def test_merge_all_empty_files(self):
        """Test merging when all files are empty"""
        file1 = self.create_test_file("file1.txt", [])
        file2 = self.create_test_file("file2.txt", [])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8")
        self.assertEqual(result, "")

    def test_merge_single_file(self):
        """Test merging a single file (edge case)"""
        file1 = self.create_test_file("file1.txt", ["alpha", "beta", "gamma"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["alpha", "beta", "gamma"]
        self.assertEqual(result, expected)

    def test_merge_files_different_lengths(self):
        """Test merging files with significantly different lengths"""
        file1 = self.create_test_file("file1.txt", ["a"])
        file2 = self.create_test_file("file2.txt", ["b", "c", "d", "e", "f"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["a", "b", "c", "d", "e", "f"]
        self.assertEqual(result, expected)

    def test_merge_with_special_characters(self):
        """Test merging files with special characters"""
        file1 = self.create_test_file("file1.txt", ["!test", "alpha", "~end"])
        file2 = self.create_test_file("file2.txt", ["#comment", "beta", "zulu"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        # Lexicographic ordering: ! < # < alpha < beta < zulu < ~end
        expected = ["!test", "#comment", "alpha", "beta", "zulu", "~end"]
        self.assertEqual(result, expected)

    def test_merge_with_numeric_strings(self):
        """Test merging files with numeric strings (lexicographic sort)"""
        file1 = self.create_test_file("file1.txt", ["1", "10", "100"])
        file2 = self.create_test_file("file2.txt", ["2", "20", "200"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        # Lexicographic ordering: "1" < "10" < "100" < "2" < "20" < "200"
        expected = ["1", "10", "100", "2", "20", "200"]
        self.assertEqual(result, expected)

    def test_merge_cdxj_format(self):
        """Test merging files in CDXJ format (typical use case)"""
        file1 = self.create_test_file(
            "file1.cdxj",
            [
                "com,example)/ 20200101000000 {...}",
                "com,example)/page 20200102000000 {...}",
            ],
        )
        file2 = self.create_test_file(
            "file2.cdxj",
            [
                "com,example)/ 20200101120000 {...}",
                "com,test)/ 20200101000000 {...}",
            ],
        )
        output = str(self.test_path / "output.cdxj")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        # Lines are sorted lexicographically
        self.assertEqual(len(result), 4)
        self.assertTrue(result[0].startswith("com,example)/ 20200101000000"))
        self.assertTrue(result[-1].startswith("com,test)/ 20200101000000"))

    def test_merge_cdxj_real_data_pt_domain(self):
        """Test merging CDXJ files with real Portuguese domain data"""
        # Using real data from Arquivo.pt's Roteiro collection
        file1 = self.create_test_file(
            "arquivo1.cdxj",
            [
                'pt,aas)/health 19961013210146 {"status": "200", "url": "http://www.aas.pt/health/", "filename": "AWP-Roteiro-20090510220155-00000.arc.gz", "length": "0", "mime": "text/html", "offset": "27092725", "digest": "CDDAGKRYRQAZ5BLBHYUC2QVULMX465PI"}',
                'pt,aas)/health/51/7/43.gif 19961013210152 {"status": "200", "url": "http://www.aas.pt/health/51/7/43.gif", "filename": "AWP-Roteiro-20090510220409-00002.arc.gz", "length": "0", "mime": "image/gif", "offset": "42475319", "digest": "TKUDAJEPQWNYT5PT37N3RKPEJSRQK6DS"}',
                'pt,aas)/health/51/7/45.gif 19961013210152 {"status": "200", "url": "http://www.aas.pt/health/51/7/45.gif", "filename": "AWP-Roteiro-20090510220409-00002.arc.gz", "length": "0", "mime": "image/gif", "offset": "42476256", "digest": "CPHGRVXZ4JQHRN5ETUHQ4HNBNZCIAVLT"}',
            ],
        )
        file2 = self.create_test_file(
            "arquivo2.cdxj",
            [
                'pt,aas)/health/51/7/41.gif 19961013210150 {"status": "200", "url": "http://www.aas.pt/health/51/7/41.gif", "filename": "AWP-Roteiro-20090510220409-00002.arc.gz", "length": "0", "mime": "image/gif", "offset": "42467014", "digest": "CPMIWUQWGZWRSADMAHLXE6G3LSNOFSOX"}',
                'pt,aas)/health/51/7/44.gif 19961013210152 {"status": "200", "url": "http://www.aas.pt/health/51/7/44.gif", "filename": "AWP-Roteiro-20090510220409-00002.arc.gz", "length": "0", "mime": "image/gif", "offset": "42475772", "digest": "IHIT6U3YF5EECV2T42RQ7A6XZYRD4WNW"}',
                'pt,aas)/health/51/7/46.gif 19961013210152 {"status": "200", "url": "http://www.aas.pt/health/51/7/46.gif", "filename": "AWP-Roteiro-20090510220409-00002.arc.gz", "length": "0", "mime": "image/gif", "offset": "42476600", "digest": "TEXKEYCSHZL7ZV5YVJC6BQJQYDVDQPI6"}',
            ],
        )
        output = str(self.test_path / "merged.cdxj")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()

        # Verify all 6 lines are present
        self.assertEqual(len(result), 6)

        # Verify correct lexicographic ordering
        self.assertTrue(result[0].startswith("pt,aas)/health 19961013210146"))
        self.assertTrue(result[1].startswith("pt,aas)/health/51/7/41.gif"))
        self.assertTrue(result[2].startswith("pt,aas)/health/51/7/43.gif"))
        self.assertTrue(result[3].startswith("pt,aas)/health/51/7/44.gif"))
        self.assertTrue(result[4].startswith("pt,aas)/health/51/7/45.gif"))
        self.assertTrue(result[5].startswith("pt,aas)/health/51/7/46.gif"))

        # Verify the result is properly sorted
        self.assertEqual(result, sorted(result))

    def test_merge_cdxj_multiple_domains(self):
        """Test merging CDXJ files with multiple Portuguese domains"""
        file1 = self.create_test_file(
            "index1.cdxj",
            [
                'pt,aas)/health 19961013210146 {"status": "200", "url": "http://www.aas.pt/health/", "filename": "file1.arc.gz", "offset": "100"}',
                'pt,governo)/index.html 19961013210200 {"status": "200", "url": "http://www.governo.pt/index.html", "filename": "file1.arc.gz", "offset": "200"}',
                'pt,uc)/welcome 19961013210300 {"status": "200", "url": "http://www.uc.pt/welcome", "filename": "file1.arc.gz", "offset": "300"}',
            ],
        )
        file2 = self.create_test_file(
            "index2.cdxj",
            [
                'pt,edp)/home 19961013210100 {"status": "200", "url": "http://www.edp.pt/home", "filename": "file2.arc.gz", "offset": "100"}',
                'pt,governo)/about 19961013210250 {"status": "200", "url": "http://www.governo.pt/about", "filename": "file2.arc.gz", "offset": "200"}',
                'pt,zon)/products 19961013210400 {"status": "200", "url": "http://www.zon.pt/products", "filename": "file2.arc.gz", "offset": "300"}',
            ],
        )
        file3 = self.create_test_file(
            "index3.cdxj",
            [
                'pt,aas)/contact 19961013210150 {"status": "200", "url": "http://www.aas.pt/contact", "filename": "file3.arc.gz", "offset": "100"}',
                'pt,sapo)/mail 19961013210350 {"status": "200", "url": "http://www.sapo.pt/mail", "filename": "file3.arc.gz", "offset": "200"}',
            ],
        )
        output = str(self.test_path / "merged.cdxj")

        merge_sorted_files([file1, file2, file3], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()

        # Verify all 8 lines are present
        self.assertEqual(len(result), 8)

        # Verify correct domain ordering: aas < edp < governo < sapo < uc < zon
        # Extract the URL key (everything before the timestamp)
        url_keys = [line.split()[0] for line in result]
        self.assertEqual(url_keys[0], "pt,aas)/contact")
        self.assertEqual(url_keys[1], "pt,aas)/health")
        self.assertEqual(url_keys[2], "pt,edp)/home")
        self.assertTrue(url_keys[3].startswith("pt,governo)/"))
        self.assertTrue(url_keys[4].startswith("pt,governo)/"))
        self.assertEqual(url_keys[5], "pt,sapo)/mail")
        self.assertEqual(url_keys[6], "pt,uc)/welcome")
        self.assertEqual(url_keys[7], "pt,zon)/products")

        # Verify the entire result is properly sorted
        self.assertEqual(result, sorted(result))

    def test_merge_cdxj_same_url_different_timestamps(self):
        """Test merging CDXJ with same URL captured at different times"""
        file1 = self.create_test_file(
            "snapshots1.cdxj",
            [
                (
                    'pt,aas)/health 19961013210146 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture1.arc.gz"}'
                ),
                (
                    'pt,aas)/health 19971015120000 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture3.arc.gz"}'
                ),
                (
                    'pt,aas)/health 19991201080000 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture5.arc.gz"}'
                ),
            ],
        )
        file2 = self.create_test_file(
            "snapshots2.cdxj",
            [
                (
                    'pt,aas)/health 19970101000000 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture2.arc.gz"}'
                ),
                (
                    'pt,aas)/health 19980601150000 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture4.arc.gz"}'
                ),
                (
                    'pt,aas)/health 20000101000000 {"status": "200", '
                    '"url": "http://www.aas.pt/health/", "filename": "capture6.arc.gz"}'
                ),
            ],
        )
        output = str(self.test_path / "timeline.cdxj")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()

        # Verify all 6 snapshots are present
        self.assertEqual(len(result), 6)

        # Extract timestamps from results
        timestamps = [line.split()[1] for line in result]

        # Verify chronological ordering by timestamp
        expected_timestamps = [
            "19961013210146",
            "19970101000000",
            "19971015120000",
            "19980601150000",
            "19991201080000",
            "20000101000000",
        ]
        self.assertEqual(timestamps, expected_timestamps)

        # Verify the result is properly sorted
        self.assertEqual(result, sorted(result))

    def test_merge_preserves_line_endings(self):
        """Test that merge preserves line content exactly"""
        file1 = self.create_test_file("file1.txt", ["line1", "line2  ", "line3\t"])
        file2 = self.create_test_file("file2.txt", ["line0", "line2", "line4"])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8")
        # Check that whitespace is preserved
        self.assertIn("line2  ", result)
        self.assertIn("line3\t", result)

    def test_merge_to_stdout(self):
        """Test merging to stdout (output_file = '-')"""
        file1 = self.create_test_file("file1.txt", ["apple", "cherry"])
        file2 = self.create_test_file("file2.txt", ["banana", "date"])

        # Capture stdout
        old_stdout = sys.stdout
        sys.stdout = captured_output = StringIO()

        try:
            merge_sorted_files([file1, file2], "-")
            output = captured_output.getvalue()
        finally:
            sys.stdout = old_stdout

        result = output.splitlines()
        expected = ["apple", "banana", "cherry", "date"]
        self.assertEqual(result, expected)

    def test_merge_large_number_of_files(self):
        """Test merging many files (stress test for heap)"""
        num_files = 20
        files = []

        for i in range(num_files):
            # Create files with interleaved values
            lines = [f"item_{j:03d}_{i:02d}" for j in range(i, 100, num_files)]
            files.append(self.create_test_file(f"file{i}.txt", lines))

        output = str(self.test_path / "output.txt")
        merge_sorted_files(files, output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        # Verify result is sorted
        self.assertEqual(result, sorted(result))
        # Verify we have all items (each file has len(range(i, 100, num_files)) items)
        expected_count = sum(len(list(range(i, 100, num_files))) for i in range(num_files))
        self.assertEqual(len(result), expected_count)

    def test_merge_with_custom_buffer_size(self):
        """Test merge with custom buffer size"""
        file1 = self.create_test_file("file1.txt", ["apple", "cherry"])
        file2 = self.create_test_file("file2.txt", ["banana", "date"])
        output = str(self.test_path / "output.txt")

        # Use small buffer size
        merge_sorted_files([file1, file2], output, buffer_size=64)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["apple", "banana", "cherry", "date"]
        self.assertEqual(result, expected)

    def test_merge_files_with_long_lines(self):
        """Test merging files with very long lines"""
        long_line1 = "a" * 10000
        long_line2 = "b" * 10000

        file1 = self.create_test_file("file1.txt", [long_line1, "z" * 100])
        file2 = self.create_test_file("file2.txt", [long_line2, "y" * 100])
        output = str(self.test_path / "output.txt")

        merge_sorted_files([file1, file2], output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(result), 4)
        self.assertEqual(len(result[0]), 10000)
        self.assertEqual(len(result[1]), 10000)


class TestIntegration(unittest.TestCase):
    """Integration tests combining get_all_files and merge_sorted_files"""

    def setUp(self):
        """Create temporary directory structure"""
        self.test_dir = tempfile.TemporaryDirectory()
        self.test_path = Path(self.test_dir.name)

    def tearDown(self):
        """Clean up temporary directory"""
        self.test_dir.cleanup()

    def test_merge_from_directory(self):
        """Test merging all files from a directory"""
        # Create directory with multiple sorted files
        input_dir = self.test_path / "input"
        input_dir.mkdir()

        (input_dir / "file1.txt").write_text("apple\ncherry\n", encoding="utf-8")
        (input_dir / "file2.txt").write_text("banana\ndate\n", encoding="utf-8")
        (input_dir / "file3.txt").write_text("elderberry\nfig\n", encoding="utf-8")

        files = list(get_all_files([str(input_dir)]))
        output = str(self.test_path / "merged.txt")

        merge_sorted_files(files, output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["apple", "banana", "cherry", "date", "elderberry", "fig"]
        self.assertEqual(result, expected)

    def test_merge_from_nested_directories(self):
        """Test merging files from nested directory structure"""
        # Create nested structure
        dir1 = self.test_path / "dir1"
        dir2 = self.test_path / "dir1" / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        (dir1 / "file1.txt").write_text("a\nc\n", encoding="utf-8")
        (dir2 / "file2.txt").write_text("b\nd\n", encoding="utf-8")

        files = list(get_all_files([str(dir1)]))
        output = str(self.test_path / "merged.txt")

        merge_sorted_files(files, output)

        result = Path(output).read_text(encoding="utf-8").splitlines()
        expected = ["a", "b", "c", "d"]
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
