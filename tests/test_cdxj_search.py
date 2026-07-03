#!/usr/bin/env python3
"""
Test Suite for cdxj_search.py - Binary Search for CDXJ and ZipNum Indexes
==========================================================================

This comprehensive test suite validates the binary search functionality for both
flat CDXJ files and ZipNum compressed format. It tests URL/SURT conversion,
different match types (exact, prefix, host, domain), filtering, and error handling.

RUNNING THE TESTS
=================
    # Run all tests with pytest (recommended)
    pytest tests/test_cdxj_search.py -v

    # Run specific test class
    pytest tests/test_cdxj_search.py::TestBinarySearch -v

    # Run with coverage
    pytest tests/test_cdxj_search.py --cov=replay_cdxj_indexing_tools.search

TEST COVERAGE SUMMARY
=====================
1. TestFileDiscovery - File discovery and type detection
2. TestFilters - Date range and field filtering
3. TestBinarySearch - Binary search on flat CDXJ files
4. TestMatchTypes - Different match types (exact, prefix, domain, subdomains)
5. TestCDXJSearch - End-to-end integration tests
"""

import gzip
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from replay_cdxj_indexing_tools.search.binary_search import search_cdxj_file  # noqa: E402
from replay_cdxj_indexing_tools.search.cdxj_search import apply_match_type  # noqa: E402
from replay_cdxj_indexing_tools.search.file_discovery import (  # noqa: E402
    detect_file_type,
    discover_files,
    find_zipnum_index_file,
)
from replay_cdxj_indexing_tools.search.filters import (  # noqa: E402
    CDXJFilter,
    deduplicate_lines,
    normalize_timestamp,
    sort_lines,
)


class TestFileDiscovery(unittest.TestCase):
    """Test file discovery and type detection."""

    def setUp(self):
        """Create temporary test directory."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_detect_file_type_cdxj(self):
        """Test detection of flat CDXJ files."""
        self.assertEqual(detect_file_type("index.cdxj"), "cdxj")

    def test_detect_file_type_zipnum_idx(self):
        """Test detection of ZipNum index files."""
        self.assertEqual(detect_file_type("index.idx"), "zipnum_idx")

    def test_detect_file_type_zipnum_data(self):
        """Test detection of ZipNum data files."""
        self.assertEqual(detect_file_type("index.cdxj.gz"), "zipnum_data")

    def test_discover_single_file(self):
        """Test discovery of single file."""
        # Create test file
        test_file = os.path.join(self.test_dir, "test.cdxj")
        Path(test_file).touch()

        files = discover_files([test_file])
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0], os.path.abspath(test_file))

    def test_discover_directory(self):
        """Test discovery of files in directory."""
        # Create test files
        for i in range(3):
            Path(os.path.join(self.test_dir, f"test{i}.cdxj")).touch()

        files = discover_files([self.test_dir])
        self.assertEqual(len(files), 3)

    def test_discover_glob_pattern(self):
        """Test discovery with glob pattern."""
        # Create test files
        for i in range(3):
            Path(os.path.join(self.test_dir, f"index{i}.cdxj")).touch()
        Path(os.path.join(self.test_dir, "other.txt")).touch()

        pattern = os.path.join(self.test_dir, "index*.cdxj")
        files = discover_files([pattern])
        self.assertEqual(len(files), 3)

    def test_find_zipnum_index_file(self):
        """Test finding ZipNum index file from data."""
        data_file = os.path.join(self.test_dir, "index.cdxj.gz")
        idx_file = os.path.join(self.test_dir, "index.idx")

        # Create both files (order matters for the test)
        Path(data_file).touch()
        Path(idx_file).touch()

        found = find_zipnum_index_file(data_file)
        self.assertEqual(found, idx_file)


class TestFilters(unittest.TestCase):
    """Test filtering functionality."""

    def test_normalize_timestamp_year(self):
        """Test normalizing year to 14-digit timestamp."""
        self.assertEqual(normalize_timestamp("2020"), "20200101000000")

    def test_normalize_timestamp_month(self):
        """Test normalizing year-month to 14-digit timestamp."""
        self.assertEqual(normalize_timestamp("202012"), "20201201000000")

    def test_normalize_timestamp_day(self):
        """Test normalizing year-month-day to 14-digit timestamp."""
        self.assertEqual(normalize_timestamp("20201225"), "20201225000000")

    def test_normalize_timestamp_full(self):
        """Test normalizing full timestamp."""
        self.assertEqual(normalize_timestamp("20201225123456"), "20201225123456")

    def test_cdxj_filter_exact_match(self):
        """Test exact field match filter."""
        filter_obj = CDXJFilter(filters=["status=200"])

        line1 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "status": "200"}'
        line2 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "status": "404"}'

        self.assertTrue(filter_obj.matches(line1))
        self.assertFalse(filter_obj.matches(line2))

    def test_cdxj_filter_not_equal(self):
        """Test not-equal field filter."""
        filter_obj = CDXJFilter(filters=["status!=404"])

        line1 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "status": "200"}'
        line2 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "status": "404"}'

        self.assertTrue(filter_obj.matches(line1))
        self.assertFalse(filter_obj.matches(line2))

    def test_cdxj_filter_regex_match(self):
        """Test regex match filter."""
        filter_obj = CDXJFilter(filters=["mime~text/.*"])

        line1 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "text/html"}'
        line2 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "image/png"}'

        self.assertTrue(filter_obj.matches(line1))
        self.assertFalse(filter_obj.matches(line2))

    def test_cdxj_filter_regex_not_match(self):
        """Test regex not-match filter."""
        filter_obj = CDXJFilter(filters=["mime!~image/.*"])

        line1 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "text/html"}'
        line2 = 'com,example)/ 20200101000000 {"url": "http://example.com/", "mime": "image/png"}'

        self.assertTrue(filter_obj.matches(line1))
        self.assertFalse(filter_obj.matches(line2))

    def test_cdxj_filter_date_range(self):
        """Test date range filtering."""
        filter_obj = CDXJFilter(from_ts="2020", to_ts="2021")

        line1 = 'com,example)/ 20200601000000 {"url": "http://example.com/"}'
        line2 = 'com,example)/ 20190601000000 {"url": "http://example.com/"}'
        line3 = 'com,example)/ 20220601000000 {"url": "http://example.com/"}'

        self.assertTrue(filter_obj.matches(line1))
        self.assertFalse(filter_obj.matches(line2))  # Before range
        self.assertFalse(filter_obj.matches(line3))  # After range

    def test_sort_lines(self):
        """Test sorting CDXJ lines."""
        lines = [
            "com,example)/ 20200102000000 {}",
            "com,example)/ 20200101000000 {}",
            "com,example)/page 20200101000000 {}",
        ]

        sorted_lines = sort_lines(lines)
        self.assertEqual(sorted_lines[0], "com,example)/ 20200101000000 {}")
        self.assertEqual(sorted_lines[1], "com,example)/ 20200102000000 {}")
        self.assertEqual(sorted_lines[2], "com,example)/page 20200101000000 {}")

    def test_deduplicate_lines(self):
        """Test deduplication of CDXJ lines."""
        lines = [
            'com,example)/ 20200101000000 {"url": "http://example.com/"}',
            'com,example)/ 20200101000000 {"url": "http://example.com/", "extra": "data"}',
            'com,example)/ 20200102000000 {"url": "http://example.com/"}',
        ]

        deduped = deduplicate_lines(lines)
        self.assertEqual(len(deduped), 2)  # First two have same SURT+timestamp


class TestBinarySearch(unittest.TestCase):
    """Test binary search functionality."""

    def setUp(self):
        """Create temporary test files."""
        self.test_dir = tempfile.mkdtemp()

        # Create a test CDXJ file with sorted entries
        self.test_cdxj = os.path.join(self.test_dir, "test.cdxj")
        with open(self.test_cdxj, "w") as f:
            f.write('com,example)/ 20200101000000 {"url": "http://example.com/"}\n')
            f.write('com,example)/about 20200101000000 {"url": "http://example.com/about"}\n')
            f.write('com,example)/contact 20200101000000 {"url": "http://example.com/contact"}\n')
            f.write('com,example)/page 20200101000000 {"url": "http://example.com/page"}\n')
            f.write('com,example)/page 20200102000000 {"url": "http://example.com/page"}\n')
            f.write('com,test)/ 20200101000000 {"url": "http://test.com/"}\n')

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_exact_match_found(self):
        """Test exact match when entry exists."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/page", match_prefix=False)
        self.assertEqual(len(results), 2)  # Two timestamps for same URL
        self.assertIn("20200101000000", results[0])
        self.assertIn("20200102000000", results[1])

    def test_exact_match_not_found(self):
        """Test exact match when entry doesn't exist."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/missing", match_prefix=False)
        self.assertEqual(len(results), 0)

    def test_prefix_match_single(self):
        """Test prefix match returning multiple entries."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=True)
        self.assertEqual(len(results), 5)  # All example.com URLs

    def test_prefix_match_narrow(self):
        """Test prefix match with more specific prefix."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/p", match_prefix=True)
        self.assertEqual(len(results), 2)  # Only /page URLs

    def test_binary_search_first_entry(self):
        """Test binary search finds first entry in file."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=False)
        self.assertEqual(len(results), 1)

    def test_binary_search_last_entry(self):
        """Test binary search finds last entry in file."""
        results = search_cdxj_file(self.test_cdxj, "com,test)/", match_prefix=False)
        self.assertEqual(len(results), 1)


class TestMatchTypes(unittest.TestCase):
    """Test different match type behaviors."""

    def test_match_type_exact(self):
        """Test exact match type."""
        key, use_prefix = apply_match_type("com,example)/page", "exact")
        self.assertEqual(key, "com,example)/page")
        self.assertFalse(use_prefix)

    def test_match_type_prefix(self):
        """Test prefix match type."""
        key, use_prefix = apply_match_type("com,example)/path/", "prefix")
        self.assertEqual(key, "com,example)/path/")
        self.assertTrue(use_prefix)

    def test_match_type_domain(self):
        """Test domain match type (only paths for specific domain)."""
        key, use_prefix = apply_match_type("com,example)/any/path", "domain")
        self.assertEqual(key, "com,example)")
        self.assertTrue(use_prefix)

    def test_match_type_subdomains(self):
        """Test subdomains match type, don't match the domain itself, just any subdomain."""
        key, use_prefix = apply_match_type("com,example)/page", "subdomains")
        self.assertEqual(key, "com,example,")
        self.assertTrue(use_prefix)

        key, use_prefix = apply_match_type("com,example)/", "subdomains")
        self.assertEqual(key, "com,example,")
        self.assertTrue(use_prefix)

        key, use_prefix = apply_match_type("com,example)", "subdomains")
        self.assertEqual(key, "com,example,")
        self.assertTrue(use_prefix)

    def test_match_type_domain_no_path(self):
        """Test domain match when no path present."""
        key, use_prefix = apply_match_type("com,example)", "domain")
        self.assertEqual(key, "com,example)")
        self.assertTrue(use_prefix)


class TestCDXJSearch(unittest.TestCase):
    """Integration tests for complete search workflow."""

    def setUp(self):
        """Create temporary test files."""
        self.test_dir = tempfile.mkdtemp()

        # Create test CDXJ file with varied data
        self.test_cdxj = os.path.join(self.test_dir, "test.cdxj")
        with open(self.test_cdxj, "w") as f:
            # example.com entries
            f.write(
                'com,example)/ 20200101120000 {"url": "http://example.com/", "status": "200", "mime": "text/html"}\n'
            )
            f.write(
                'com,example)/ 20200615120000 {"url": "http://example.com/", "status": "200", "mime": "text/html"}\n'
            )
            f.write(
                'com,example)/ 20210101120000 {"url": "http://example.com/", "status": "200", "mime": "text/html"}\n'
            )
            f.write(
                'com,example)/about 20200601120000 {"url": "http://example.com/about", "status": "200", "mime": "text/html"}\n'
            )
            f.write(
                'com,example)/image.png 20200601120000 {"url": "http://example.com/image.png", "status": "200", "mime": "image/png"}\n'
            )

            # example.com subdomains
            f.write(
                'com,example,www)/ 20200601120000 {"url": "http://www.example.com/", "status": "200", "mime": "text/html"}\n'
            )
            f.write(
                'com,example,blog)/ 20200601120000 {"url": "http://blog.example.com/", "status": "200", "mime": "text/html"}\n'
            )

            # Different domain
            f.write(
                'org,test)/ 20200601120000 {"url": "http://test.org/", "status": "200", "mime": "text/html"}\n'
            )

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_search_with_exact_match(self):
        """Test search with exact match."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=False)
        self.assertEqual(len(results), 3)  # Three timestamps

    def test_search_with_prefix_match(self):
        """Test search with prefix match."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=True)
        self.assertGreaterEqual(len(results), 5)  # example.com URLs (not subdomains)

    def test_search_with_date_filter(self):
        """Test search with date range filter."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=True)

        # Should have found results
        self.assertGreater(len(results), 0, "Should have found results")

        # Filter for explicit date range that spans full year 2020
        cdxj_filter = CDXJFilter(from_ts="20200101", to_ts="20201231235959")
        filtered = [line for line in results if cdxj_filter.matches(line)]

        # Should have entries from 2020, but not 2021
        self.assertGreater(len(filtered), 0)
        self.assertLess(len(filtered), len(results))

        # Verify no 2021 entries in filtered results
        for line in filtered:
            self.assertNotIn("2021", line)

    def test_search_with_mime_filter(self):
        """Test search with MIME type filter."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=True)

        # Filter for HTML only
        cdxj_filter = CDXJFilter(filters=["mime~text/.*"])
        filtered = [line for line in results if cdxj_filter.matches(line)]

        # Should exclude image
        self.assertGreater(len(filtered), 0)
        self.assertLess(len(filtered), len(results))

    def test_search_with_multiple_filters(self):
        """Test search with multiple filters."""
        results = search_cdxj_file(self.test_cdxj, "com,example)/", match_prefix=True)

        # Filter for status 200 AND text mime type
        cdxj_filter = CDXJFilter(filters=["status=200", "mime~text/.*"])
        filtered = [line for line in results if cdxj_filter.matches(line)]

        # All results should match both filters
        self.assertGreater(len(filtered), 0)
        for line in filtered:
            self.assertIn('"status": "200"', line)
            self.assertIn('"mime": "text/', line)


class TestZipNumSearch(unittest.TestCase):
    """Test ZipNum search functionality."""

    def setUp(self):
        """Create temporary ZipNum test files with proper format."""
        self.test_dir = tempfile.mkdtemp()

        # Create multiple shard files with gzipped CDXJ
        # Shard 1: com,example domains
        self.shard1 = os.path.join(self.test_dir, "test-00.cdx.gz")
        shard1_data = (
            'com,example)/ 20200101000000 {"url": "http://example.com/"}\n'
            'com,example)/about 20200101000000 {"url": "http://example.com/about"}\n'
            'com,example)/page 20200101000000 {"url": "http://example.com/page"}\n'
        )
        with open(self.shard1, "wb") as f:
            f.write(gzip.compress(shard1_data.encode("utf-8")))

        # Shard 2: com,test domains
        self.shard2 = os.path.join(self.test_dir, "test-01.cdx.gz")
        shard2_data = (
            'com,test)/ 20200101000000 {"url": "http://test.com/"}\n'
            'com,test)/page 20200101000000 {"url": "http://test.com/page"}\n'
        )
        with open(self.shard2, "wb") as f:
            f.write(gzip.compress(shard2_data.encode("utf-8")))

        # Shard 3: org,example domains
        self.shard3 = os.path.join(self.test_dir, "test-02.cdx.gz")
        shard3_data = 'org,example)/ 20200101000000 {"url": "http://example.org/"}\n'
        with open(self.shard3, "wb") as f:
            f.write(gzip.compress(shard3_data.encode("utf-8")))

        # Create ZipNum index file with proper format
        # Format: <key>\t<shard_name>\t<offset>\t<length>\t<shard_num>
        self.idx_file = os.path.join(self.test_dir, "test.idx")
        shard1_size = os.path.getsize(self.shard1)
        shard2_size = os.path.getsize(self.shard2)
        shard3_size = os.path.getsize(self.shard3)

        with open(self.idx_file, "w") as f:
            f.write(f"com,example)/\ttest-00.cdx.gz\t0\t{shard1_size}\t0\n")
            f.write(f"com,test)/\ttest-01.cdx.gz\t0\t{shard2_size}\t1\n")
            f.write(f"org,example)/\ttest-02.cdx.gz\t0\t{shard3_size}\t2\n")

        # Create optional .loc file
        self.loc_file = os.path.join(self.test_dir, "test.loc")
        with open(self.loc_file, "w") as f:
            f.write("test-00.cdx.gz\ttest-00.cdx.gz\n")
            f.write("test-01.cdx.gz\ttest-01.cdx.gz\n")
            f.write("test-02.cdx.gz\ttest-02.cdx.gz\n")

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_zipnum_files_exist(self):
        """Test that ZipNum test files were created."""
        self.assertTrue(os.path.exists(self.idx_file))
        self.assertTrue(os.path.exists(self.shard1))
        self.assertTrue(os.path.exists(self.shard2))
        self.assertTrue(os.path.exists(self.shard3))
        self.assertTrue(os.path.exists(self.loc_file))

    def test_zipnum_index_parsing(self):
        """Test ZipNum index file parsing."""
        from replay_cdxj_indexing_tools.search.zipnum_search import parse_idx_line

        # New format: <key>\t<shard_name>\t<offset>\t<length>\t<shard_num>
        line = "com,example)/\tbase-00.cdx.gz\t0\t100\t0"
        surt_key, shard_name, offset, length, shard_num = parse_idx_line(line)

        self.assertEqual(surt_key, "com,example)/")
        self.assertEqual(shard_name, "base-00.cdx.gz")
        self.assertEqual(offset, 0)
        self.assertEqual(length, 100)
        self.assertEqual(shard_num, 0)

    def test_zipnum_exact_search(self):
        """Test exact match search in ZipNum format."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file, "com,example)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 1)
        self.assertIn("com,example)/", results[0])

    def test_zipnum_prefix_search(self):
        """Test prefix search in ZipNum format."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file, "com,example)/", match_prefix=True, verbose=False
        )
        # Should find all com,example URLs
        self.assertGreaterEqual(len(results), 3)
        for result in results:
            self.assertTrue(result.startswith("com,example)/"))

    def test_zipnum_prefix_search_narrow(self):
        """Test narrow prefix search in ZipNum format."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file, "com,example)/page", match_prefix=True, verbose=False
        )
        # Should find only /page URL
        self.assertEqual(len(results), 1)
        self.assertIn("/page", results[0])

    def test_zipnum_search_with_loc_file(self):
        """Test search using .loc file for shard resolution."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file,
            "com,test)/",
            match_prefix=True,
            verbose=False,
            loc_filepath=self.loc_file,
        )
        # Should find com,test URLs using .loc file mapping
        self.assertGreaterEqual(len(results), 2)
        for result in results:
            self.assertTrue(result.startswith("com,test)/"))

    def test_zipnum_search_different_domain(self):
        """Test search for different TLD."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file, "org,example)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 1)
        self.assertIn("org,example)/", results[0])

    def test_zipnum_search_no_matches(self):
        """Test search with no matches."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        results = search_zipnum_file(
            self.idx_file, "com,nonexistent)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 0)

    def test_zipnum_read_loc_file(self):
        """Test reading .loc file."""
        from replay_cdxj_indexing_tools.search.zipnum_search import read_loc_file

        loc_map = read_loc_file(self.loc_file)
        self.assertEqual(len(loc_map), 3)
        self.assertEqual(loc_map["test-00.cdx.gz"], "test-00.cdx.gz")
        self.assertEqual(loc_map["test-01.cdx.gz"], "test-01.cdx.gz")
        self.assertEqual(loc_map["test-02.cdx.gz"], "test-02.cdx.gz")

    def test_zipnum_path_traversal_relative(self):
        """Path traversal via relative .loc entry must raise ValueError."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        malicious_loc = os.path.join(self.test_dir, "malicious.loc")
        with open(malicious_loc, "w") as f:
            f.write("test-00.cdx.gz\t../../etc/passwd\n")

        with self.assertRaises(ValueError):
            search_zipnum_file(
                self.idx_file,
                "com,example)/",
                match_prefix=False,
                loc_filepath=malicious_loc,
            )

    def test_zipnum_path_traversal_absolute(self):
        """Absolute path outside base_dir in .loc entry must raise ValueError."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        malicious_loc = os.path.join(self.test_dir, "malicious_abs.loc")
        with open(malicious_loc, "w") as f:
            f.write("test-00.cdx.gz\t/etc/passwd\n")

        with self.assertRaises(ValueError):
            search_zipnum_file(
                self.idx_file,
                "com,example)/",
                match_prefix=False,
                loc_filepath=malicious_loc,
            )

    @unittest.skipIf(not hasattr(os, "symlink"), "symlinks not supported")
    def test_zipnum_path_traversal_symlink(self):
        """Symlink inside base_dir pointing outside must raise ValueError."""
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        link_path = os.path.join(self.test_dir, "evil_link.cdx.gz")
        try:
            os.symlink("/etc/passwd", link_path)
        except (OSError, NotImplementedError):
            self.skipTest("Cannot create symlinks on this platform")

        malicious_loc = os.path.join(self.test_dir, "malicious_sym.loc")
        with open(malicious_loc, "w") as f:
            f.write("test-00.cdx.gz\tevil_link.cdx.gz\n")

        with self.assertRaises(ValueError):
            search_zipnum_file(
                self.idx_file,
                "com,example)/",
                match_prefix=False,
                loc_filepath=malicious_loc,
            )

    def test_zipnum_path_traversal_idx_no_loc(self):
        """Traversal via .idx shard name when no .loc file is used must raise ValueError."""
        import tempfile
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        with tempfile.TemporaryDirectory() as tmpdir:
            # Build an .idx file whose shard name contains a path traversal
            malicious_idx = os.path.join(tmpdir, "malicious.idx")
            with open(malicious_idx, "w") as f:
                f.write("com,example)/\t../../etc/passwd.cdx.gz\t0\t100\t0\n")

            with self.assertRaises(ValueError):
                search_zipnum_file(
                    malicious_idx,
                    "com,example)/",
                    match_prefix=False,
                    base_dir=tmpdir,
                )

    def test_zipnum_path_traversal_idx_partial_loc(self):
        """Traversal via .idx shard name not covered by partial .loc must raise ValueError."""
        import tempfile
        from replay_cdxj_indexing_tools.search.zipnum_search import search_zipnum_file

        with tempfile.TemporaryDirectory() as tmpdir:
            # .loc covers only shard 0; shard 1 entry in .idx has a traversal path
            malicious_idx = os.path.join(tmpdir, "partial.idx")
            with open(malicious_idx, "w") as f:
                f.write("com,example)/\tsafe-00.cdx.gz\t0\t10\t0\n")
                f.write("com,test)/\t../../etc/passwd.cdx.gz\t0\t100\t1\n")

            partial_loc = os.path.join(tmpdir, "partial.loc")
            with open(partial_loc, "w") as f:
                # Only register the safe shard — leave the malicious one to the else branch
                f.write("safe-00.cdx.gz\tsafe-00.cdx.gz\n")

            # Create the safe shard so it passes the existence check
            safe_shard = os.path.join(tmpdir, "safe-00.cdx.gz")
            import gzip as gz
            with open(safe_shard, "wb") as f:
                f.write(gz.compress(b'com,example)/ 20200101 {"status":"200"}\n'))

            with self.assertRaises(ValueError):
                search_zipnum_file(
                    malicious_idx,
                    "com,test)/",
                    match_prefix=False,
                    loc_filepath=partial_loc,
                    base_dir=tmpdir,
                )


if __name__ == "__main__":
    unittest.main()
