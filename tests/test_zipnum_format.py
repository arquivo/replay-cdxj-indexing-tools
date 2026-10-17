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


def _record(surt, timestamp="20200101000000"):
    """Return a CDXJ line for ``surt``."""
    return f'{surt} {timestamp} {{"url": "http://example/{len(surt)}"}}'


def _write_zipnum(directory, blocks, base="big"):
    """Write ``blocks`` as gzip members of one shard and index them in a .idx.

    Each block is a sorted list of CDXJ lines; its .idx key is the SURT of its
    first line, as in the fixtures above.

    Returns:
        (idx_path, entries) where entries holds one (key, shard, offset, length)
        tuple per block, in .idx order.
    """
    shard_name = f"{base}-00.cdx.gz"
    idx_path = os.path.join(directory, f"{base}.idx")
    entries = []
    offset = 0
    with (
        open(os.path.join(directory, shard_name), "wb") as shard,
        open(idx_path, "w", encoding="utf-8") as idx,
    ):
        for lineno, lines in enumerate(blocks, start=1):
            data = gzip.compress(("\n".join(lines) + "\n").encode("utf-8"))
            shard.write(data)
            key = lines[0].split(" ", 1)[0]
            idx.write(f"{key}\t{shard_name}\t{offset}\t{len(data)}\t{lineno}\n")
            entries.append((key, shard_name, offset, len(data)))
            offset += len(data)
    return idx_path, entries


def _matches(key, search_key, match_prefix):
    return key.startswith(search_key) if match_prefix else key == search_key


def _expected_blocks(entries, search_key, match_prefix):
    """Blocks that can hold a match: the last block before the match range, then
    every block whose key is in the range."""
    in_range = [e for e in entries if _matches(e[0], search_key, match_prefix)]
    before = [e for e in entries if e[0] < search_key]
    return before[-1:] + in_range


def _expected_records(blocks, search_key, match_prefix):
    return sorted(
        line
        for lines in blocks
        for line in lines
        if _matches(line.split(" ", 1)[0], search_key, match_prefix)
    )


def _assert_search(test, fixture, search_key, match_prefix, expected_count=None):
    """Check a search against a brute-force scan of ``fixture`` (a test class with
    idx_file, blocks and entries): end to end through the gzip blocks, then at
    block level (no block missing, no extra block)."""
    with test.subTest(search_key=search_key[:60], match_prefix=match_prefix):
        results = sorted(search_zipnum_file(fixture.idx_file, search_key, match_prefix))
        expected = _expected_records(fixture.blocks, search_key, match_prefix)
        if expected_count is not None:
            test.assertEqual(len(expected), expected_count)
        test.assertEqual(results, expected)
        blocks = search_zipnum_index(fixture.idx_file, search_key, match_prefix)
        test.assertEqual(blocks, _expected_blocks(fixture.entries, search_key, match_prefix))


def _insert_at_first_probe(idx_path, line):
    """Insert ``line`` where the first bisection probe of search_zipnum_index reads:
    the first line starting after the middle of the file. Returns its index."""
    with open(idx_path, encoding="utf-8") as fh:
        lines = fh.readlines()
    size = sum(len(x.encode("utf-8")) for x in lines) + len(line.encode("utf-8"))
    start = 0
    for k, current in enumerate(lines):
        end = start + len(current.encode("utf-8"))
        if start <= size // 2 < end:
            lines.insert(k + 1, line)
            with open(idx_path, "w", encoding="utf-8") as fh:
                fh.writelines(lines)
            return k + 1
        start = end
    raise AssertionError("middle of the file not found")


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
        # Format: <key>\t<shard_name>\t<offset>\t<length>\t<lineno>
        self.idx_file = os.path.join(self.test_dir, "test.idx")
        shard1_size = os.path.getsize(self.shard1)
        shard2_size = os.path.getsize(self.shard2)
        shard3_size = os.path.getsize(self.shard3)

        with open(self.idx_file, "w") as f:
            f.write(f"com,example)/\ttest-00.cdx.gz\t0\t{shard1_size}\t1\n")
            f.write(f"com,test)/\ttest-01.cdx.gz\t0\t{shard2_size}\t2\n")
            f.write(f"org,example)/\ttest-02.cdx.gz\t0\t{shard3_size}\t3\n")

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
        line = "com,example)/\tbase-00.cdx.gz\t0\t100\t1"
        surt_key, shard_name, offset, length, lineno = parse_idx_line(line)

        self.assertEqual(surt_key, "com,example)/")
        self.assertEqual(shard_name, "base-00.cdx.gz")
        self.assertEqual(offset, 0)
        self.assertEqual(length, 100)
        self.assertEqual(lineno, 1)

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
        results = list(
            search_zipnum_file(self.idx_file, "com,example)/", match_prefix=False, verbose=False)
        )
        self.assertEqual(len(results), 1)
        self.assertIn("com,example)/", results[0])

    def test_zipnum_prefix_search(self):
        """Test prefix search in ZipNum format."""
        results = list(
            search_zipnum_file(self.idx_file, "com,example)/", match_prefix=True, verbose=False)
        )
        # Should find all com,example URLs
        self.assertGreaterEqual(len(results), 3)
        for result in results:
            self.assertTrue(result.startswith("com,example)/"))

    def test_zipnum_prefix_search_narrow(self):
        """Test narrow prefix search in ZipNum format."""
        results = list(
            search_zipnum_file(self.idx_file, "com,example)/page", match_prefix=True, verbose=False)
        )
        # Should find only /page URL
        self.assertEqual(len(results), 1)
        self.assertIn("/page", results[0])

    def test_zipnum_search_with_loc_file(self):
        """Test search using .loc file for shard resolution."""
        results = list(
            search_zipnum_file(
                self.idx_file,
                "com,test)/",
                match_prefix=True,
                verbose=False,
                loc_filepath=self.loc_file,
            )
        )
        # Should find com,test URLs using .loc file mapping
        self.assertGreaterEqual(len(results), 2)
        for result in results:
            self.assertTrue(result.startswith("com,test)/"))

    def test_zipnum_search_without_loc_file(self):
        """Test search without .loc file falls back to base_dir."""
        # Remove .loc file temporarily
        os.remove(self.loc_file)

        results = list(
            search_zipnum_file(
                self.idx_file,
                "com,test)/",
                match_prefix=True,
                verbose=False,
                base_dir=self.test_dir,
            )
        )
        # Should still find com,test URLs using base_dir
        self.assertGreaterEqual(len(results), 2)
        for result in results:
            self.assertTrue(result.startswith("com,test)/"))

    def test_zipnum_search_different_domain(self):
        """Test search for different TLD."""
        results = list(
            search_zipnum_file(self.idx_file, "org,example)/", match_prefix=False, verbose=False)
        )
        self.assertEqual(len(results), 1)
        self.assertIn("org,example)/", results[0])

    def test_zipnum_search_no_matches(self):
        """Test search with no matches."""
        results = list(
            search_zipnum_file(
                self.idx_file, "com,nonexistent)/", match_prefix=False, verbose=False
            )
        )
        self.assertEqual(len(results), 0)

    def test_zipnum_exact_search_non_boundary_key(self):
        """Exact-match search for a key that is NOT a block's boundary key.

        Regression test for issue #73: the .idx file only stores a sparse boundary
        key per block (the first record of each chunk), so most real records --
        like "com,example)/about" here -- are never a literal .idx key. The block
        containing them must still be found via its boundary key even though that
        boundary key is strictly less than the search key.
        """
        results = list(
            search_zipnum_file(
                self.idx_file, "com,example)/about", match_prefix=False, verbose=False
            )
        )
        self.assertEqual(len(results), 1)
        self.assertIn("com,example)/about", results[0])

    def test_search_zipnum_index_does_not_over_include_preceding_blocks(self):
        """Only the single preceding block (if any) should be returned as a candidate,
        not every earlier block whose key sorts before the search key.

        Regression test for issue #73: the old code appended every block satisfying
        surt_key <= search_key (exact) or surt_key < search_key (prefix), which for
        an index with many chunks would return all of them instead of just the one
        block that can actually contain the searched-for key.
        """
        blocks = search_zipnum_index(self.idx_file, "org,example)/", match_prefix=False)
        # Only the immediately preceding block ("com,test)/") and the exact match
        # itself ("org,example)/") are valid candidates -- "com,example)/" is not,
        # since its range ends at "com,test)/" and cannot contain "org,example)/".
        self.assertEqual([b[0] for b in blocks], ["com,test)/", "org,example)/"])

    def test_zipnum_multiple_shards_prefix(self):
        """Test prefix search across multiple shards."""
        # Search for 'com,' which should match both com,example and com,test
        results = list(search_zipnum_file(self.idx_file, "com,", match_prefix=True, verbose=False))
        # Should find URLs from both com,example and com,test shards
        self.assertGreaterEqual(len(results), 5)  # 3 from example + 2 from test

        # Verify we got results from both domains
        has_example = any("example" in r for r in results)
        has_test = any("test" in r for r in results)
        self.assertTrue(has_example)
        self.assertTrue(has_test)


class TestZipNumLargeIndex(unittest.TestCase):
    """Searches in a large .idx (~86KB), so the scan starts deep into the file.

    Every result is checked against a brute-force scan, at the index level (no
    block missing, no extra block) and end to end through the real gzip blocks.
    """

    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        lines = []
        for i in range(2500):
            site = f"com,site{i:05d})/"
            lines += [_record(site), _record(site + "a"), _record(site + "b")]
            if i % 97 == 0:
                # Captures of one url spanning several blocks.
                lines += [_record(site, f"2021{n:010d}") for n in range(10)]
        lines.sort()
        cls.blocks = [lines[i : i + 4] for i in range(0, len(lines), 4)]
        cls.idx_file, cls.entries = _write_zipnum(cls.test_dir, cls.blocks)
        assert os.path.getsize(cls.idx_file) > 50_000

    @classmethod
    def tearDownClass(cls):
        import shutil

        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def assert_search(self, search_key, match_prefix):
        _assert_search(self, self, search_key, match_prefix)

    def test_search_matches_brute_force(self):
        for i in list(range(0, 2500, 101)) + [97 * 7, 97 * 25, 2499]:
            site = f"com,site{i:05d})/"
            self.assert_search(site, False)  # boundary key or first capture of a block
            self.assert_search(site + "a", False)  # usually not a boundary key
            self.assert_search(site + "aa", False)  # missing, between two records
            self.assert_search(site, True)
            self.assert_search(site[:-3], True)  # prefix covering ten sites

    def test_search_key_before_first_boundary_key(self):
        self.assert_search("aaa,example)/", False)
        self.assert_search("aaa,", True)

    def test_search_key_beyond_last_boundary_key(self):
        last_key = self.entries[-1][0]
        target = self.blocks[-1][-1].split(" ", 1)[0]
        self.assertGreater(target, last_key)
        self.assert_search(target, False)
        self.assert_search(target, True)
        self.assert_search("zzz,)/", False)  # past every record


class TestZipNumOversizedIdxKeys(unittest.TestCase):
    """Index lines of 20KB (long archived urls) around the anchor block.

    Layout of the .idx (one line per block):

        ar,...          50 short lines
        com,aaa)/xxx..  oversized, the block before com,example)/ (anchor)
        com,example)/page
        net,aaa)/xxx..  oversized, right before the anchor of net,example)/
        net,bbb)/       anchor of net,example)/
        net,example)/public-page
        org,...         50 short lines
    """

    LONG_PATH = "x" * 20000

    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.blocks = [[_record(f"ar,site{i:03d})/")] for i in range(50)]
        cls.blocks += [
            [
                _record("com,aaa)/" + cls.LONG_PATH),
                _record("com,example)/hidden-page"),
                _record("com,example)/page", "20200101000001"),
                _record("com,example)/page", "20200101000002"),
            ],
            [
                _record("com,example)/page", "20200101000003"),
                _record("com,example)/page", "20200101000004"),
                _record("com,example)/public-page"),
            ],
            [_record("net,aaa)/" + cls.LONG_PATH)],
            [_record("net,bbb)/"), _record("net,example)/hidden-page")],
            [_record("net,example)/public-page")],
        ]
        cls.blocks += [[_record(f"org,site{i:03d})/")] for i in range(50)]
        cls.idx_file, cls.entries = _write_zipnum(cls.test_dir, cls.blocks)

    @classmethod
    def tearDownClass(cls):
        import shutil

        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def assert_search(self, search_key, match_prefix, expected_count):
        _assert_search(self, self, search_key, match_prefix, expected_count)

    def test_prefix_search_finds_record_in_block_with_oversized_key(self):
        """com,example)/hidden-page lives in the block whose .idx key is 20KB long.

        If the scan starts inside that line, the block is skipped and hidden-page
        and two captures of /page are lost.
        """
        self.assert_search("com,example)/", True, 6)

    def test_exact_search_captures_starting_in_block_with_oversized_key(self):
        """The captures of com,example)/page start in the oversized block."""
        self.assert_search("com,example)/page", False, 4)

    def test_search_with_oversized_line_right_before_anchor(self):
        """The anchor line itself is short, but the line before it is 20KB."""
        self.assert_search("net,example)/hidden-page", False, 1)
        self.assert_search("net,example)/", True, 2)


class TestZipNumIdxInvalidLines(unittest.TestCase):
    """Blank and invalid .idx lines are skipped without losing or adding blocks.

    A garbage line where the first bisection probe reads stops the binary search
    early, so the scan starts about half a file before the anchor and must keep
    only the last block before the match range. Blank, tab-less and non-numeric
    lines sit between the anchor and the matches.
    """

    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.blocks = [[_record(f"com,site{i:03d})/")] for i in range(60)]
        cls.blocks += [
            [
                _record("net,aaa)/"),
                _record("net,example)/hidden-page"),
                _record("net,example)/page", "20200101000001"),
            ],
            [_record("net,example)/page", "20200101000002"), _record("net,example)/public-page")],
        ]
        cls.blocks += [[_record(f"org,site{i:03d})/")] for i in range(5)]
        cls.idx_file, cls.entries = _write_zipnum(cls.test_dir, cls.blocks)

        with open(cls.idx_file, encoding="utf-8") as fh:
            lines = fh.readlines()
        anchor = [i for i, x in enumerate(lines) if x.startswith("net,aaa)/\t")][0]
        lines[anchor + 1 : anchor + 1] = [
            "\n",
            "no tabs in this line\n",
            "net,example)/\tbig-00.cdx.gz\tnot-a-number\t1\t1\n",
        ]
        with open(cls.idx_file, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        probe = _insert_at_first_probe(cls.idx_file, "garbage\n")
        assert probe < anchor, "the garbage line must come before the anchor"

    @classmethod
    def tearDownClass(cls):
        import shutil

        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def test_exact_search_across_invalid_lines(self):
        _assert_search(self, self, "net,example)/page", False, 2)
        _assert_search(self, self, "net,example)/hidden-page", False, 1)

    def test_prefix_search_across_invalid_lines(self):
        _assert_search(self, self, "net,example)/", True, 4)

    def test_search_before_the_garbage_line(self):
        _assert_search(self, self, "com,site010)/", False, 1)


if __name__ == "__main__":
    unittest.main()
