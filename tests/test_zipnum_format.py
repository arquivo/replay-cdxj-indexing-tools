#!/usr/bin/env python3
"""
Test Suite for ZipNum Format Search
===================================

Tests the .loc file integration and proper ZipNum format handling.
"""

import gzip
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from replay_cdxj_indexing_tools.search.zipnum_search import (  # noqa: E402
    parse_idx_line,
    read_loc_file,
    search_zipnum_file,
    search_zipnum_index,
)


class TestZipNumFormat(unittest.TestCase):
    """Test ZipNum format with .loc file integration."""

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

    def test_parse_idx_line(self):
        """Test parsing ZipNum index line format."""
        line = "com,example)/\tbase-00.cdx.gz\t0\t100\t0"
        surt_key, shard_name, offset, length, shard_num = parse_idx_line(line)

        self.assertEqual(surt_key, "com,example)/")
        self.assertEqual(shard_name, "base-00.cdx.gz")
        self.assertEqual(offset, 0)
        self.assertEqual(length, 100)
        self.assertEqual(shard_num, 0)

    def test_read_loc_file(self):
        """Test reading .loc file."""
        loc_map = read_loc_file(self.loc_file)
        self.assertEqual(len(loc_map), 3)
        self.assertEqual(loc_map["test-00.cdx.gz"], "test-00.cdx.gz")
        self.assertEqual(loc_map["test-01.cdx.gz"], "test-01.cdx.gz")
        self.assertEqual(loc_map["test-02.cdx.gz"], "test-02.cdx.gz")

    def test_search_zipnum_index_exact(self):
        """Test searching ZipNum index for exact match."""
        blocks = search_zipnum_index(self.idx_file, "com,example)/", match_prefix=False)
        self.assertGreaterEqual(len(blocks), 1)
        # First block should be for com,example
        self.assertEqual(blocks[0][0], "com,example)/")
        self.assertEqual(blocks[0][1], "test-00.cdx.gz")

    def test_search_zipnum_index_prefix(self):
        """Test searching ZipNum index for prefix match."""
        blocks = search_zipnum_index(self.idx_file, "com,example)/", match_prefix=True)
        # Should include blocks that might contain com,example URLs
        self.assertGreaterEqual(len(blocks), 1)

    def test_zipnum_exact_search(self):
        """Test exact match search in ZipNum format."""
        results = search_zipnum_file(
            self.idx_file, "com,example)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 1)
        self.assertIn("com,example)/", results[0])

    def test_zipnum_prefix_search(self):
        """Test prefix search in ZipNum format."""
        results = search_zipnum_file(
            self.idx_file, "com,example)/", match_prefix=True, verbose=False
        )
        # Should find all com,example URLs
        self.assertGreaterEqual(len(results), 3)
        for result in results:
            self.assertTrue(result.startswith("com,example)/"))

    def test_zipnum_prefix_search_narrow(self):
        """Test narrow prefix search in ZipNum format."""
        results = search_zipnum_file(
            self.idx_file, "com,example)/page", match_prefix=True, verbose=False
        )
        # Should find only /page URL
        self.assertEqual(len(results), 1)
        self.assertIn("/page", results[0])

    def test_zipnum_search_with_loc_file(self):
        """Test search using .loc file for shard resolution."""
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

    def test_zipnum_search_without_loc_file(self):
        """Test search without .loc file falls back to base_dir."""
        # Remove .loc file temporarily
        os.remove(self.loc_file)

        results = search_zipnum_file(
            self.idx_file,
            "com,test)/",
            match_prefix=True,
            verbose=False,
            base_dir=self.test_dir,
        )
        # Should still find com,test URLs using base_dir
        self.assertGreaterEqual(len(results), 2)
        for result in results:
            self.assertTrue(result.startswith("com,test)/"))

    def test_zipnum_search_different_domain(self):
        """Test search for different TLD."""
        results = search_zipnum_file(
            self.idx_file, "org,example)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 1)
        self.assertIn("org,example)/", results[0])

    def test_zipnum_search_no_matches(self):
        """Test search with no matches."""
        results = search_zipnum_file(
            self.idx_file, "com,nonexistent)/", match_prefix=False, verbose=False
        )
        self.assertEqual(len(results), 0)

    def test_zipnum_multiple_shards_prefix(self):
        """Test prefix search across multiple shards."""
        # Search for 'com,' which should match both com,example and com,test
        results = search_zipnum_file(self.idx_file, "com,", match_prefix=True, verbose=False)
        # Should find URLs from both com,example and com,test shards
        self.assertGreaterEqual(len(results), 5)  # 3 from example + 2 from test

        # Verify we got results from both domains
        has_example = any("example" in r for r in results)
        has_test = any("test" in r for r in results)
        self.assertTrue(has_example)
        self.assertTrue(has_test)


if __name__ == "__main__":
    unittest.main()
