#!/usr/bin/env python3
"""
test_zipnum_to_flat_cdxj.py - Test suite for zipnum_to_flat_cdxj
================================================================

Tests ZipNum to flat CDXJ conversion functionality.
"""

import os
import gzip

import pytest

from replay_cdxj_indexing_tools.zipnum.zipnum_to_flat_cdxj import (
    read_idx_file,
    read_loc_file,
    find_loc_file,
    resolve_shard_path,
    zipnum_to_flat_cdxj,
    main,
)


class TestReadIdxFile:
    """Test reading .idx files."""

    def test_read_idx_basic(self, tmp_path):
        """Test reading a basic idx file."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text(
            "com,example)/ 20230101000000\ttest\t0\t100\t1\n"
            "com,example)/ 20230102000000\ttest\t100\t150\t1\n"
        )

        entries = read_idx_file(str(idx_file))
        assert len(entries) == 2
        assert entries[0] == ("com,example)/ 20230101000000", "test", 0, 100, 1)
        assert entries[1] == ("com,example)/ 20230102000000", "test", 100, 150, 1)

    def test_read_idx_multiple_shards(self, tmp_path):
        """Test reading idx with multiple shards."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text(
            "com,example)/ 20230101000000\ttest-01\t0\t100\t1\n"
            "com,example)/ 20230102000000\ttest-02\t0\t150\t2\n"
            "com,example)/ 20230103000000\ttest-03\t0\t200\t3\n"
        )

        entries = read_idx_file(str(idx_file))
        assert len(entries) == 3
        assert entries[0][1] == "test-01"
        assert entries[1][1] == "test-02"
        assert entries[2][1] == "test-03"

    def test_read_idx_with_comments(self, tmp_path):
        """Test reading idx file with comments."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text(
            "# This is a comment\n"
            "com,example)/ 20230101000000\ttest\t0\t100\t1\n"
            "# Another comment\n"
            "com,example)/ 20230102000000\ttest\t100\t150\t1\n"
        )

        entries = read_idx_file(str(idx_file))
        assert len(entries) == 2

    def test_read_idx_empty_lines(self, tmp_path):
        """Test reading idx file with empty lines."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text(
            "com,example)/ 20230101000000\ttest\t0\t100\t1\n"
            "\n"
            "com,example)/ 20230102000000\ttest\t100\t150\t1\n"
            "\n"
        )

        entries = read_idx_file(str(idx_file))
        assert len(entries) == 2

    def test_read_idx_invalid_lines(self, tmp_path):
        """Test reading idx file with invalid lines (skipped)."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text(
            "com,example)/ 20230101000000\ttest\t0\t100\t1\n"
            "invalid line with not enough fields\n"
            "com,example)/ 20230102000000\ttest\t100\t150\t1\n"
        )

        entries = read_idx_file(str(idx_file))
        assert len(entries) == 2


class TestReadLocFile:
    """Test reading .loc files."""

    def test_read_loc_basic(self, tmp_path):
        """Test reading a basic loc file."""
        loc_file = tmp_path / "test.loc"
        loc_file.write_text(
            "test-01\ttest-01.cdx.gz\n"
            "test-02\ttest-02.cdx.gz\n"
        )

        loc_map = read_loc_file(str(loc_file))
        assert len(loc_map) == 2
        assert loc_map["test-01"] == "test-01.cdx.gz"
        assert loc_map["test-02"] == "test-02.cdx.gz"

    def test_read_loc_with_paths(self, tmp_path):
        """Test reading loc file with full paths."""
        loc_file = tmp_path / "test.loc"
        loc_file.write_text(
            "test-01\t/data/shards/test-01.cdx.gz\n"
            "test-02\t/data/shards/test-02.cdx.gz\n"
        )

        loc_map = read_loc_file(str(loc_file))
        assert loc_map["test-01"] == "/data/shards/test-01.cdx.gz"
        assert loc_map["test-02"] == "/data/shards/test-02.cdx.gz"

    def test_read_loc_with_comments(self, tmp_path):
        """Test reading loc file with comments."""
        loc_file = tmp_path / "test.loc"
        loc_file.write_text(
            "# Comment line\n"
            "test-01\ttest-01.cdx.gz\n"
            "test-02\ttest-02.cdx.gz\n"
        )

        loc_map = read_loc_file(str(loc_file))
        assert len(loc_map) == 2


class TestFindLocFile:
    """Test auto-detecting .loc files."""

    def test_find_loc_exists(self, tmp_path):
        """Test finding loc file that exists."""
        idx_file = tmp_path / "test.idx"
        loc_file = tmp_path / "test.loc"

        idx_file.write_text("dummy content\n")
        loc_file.write_text("dummy content\n")

        found = find_loc_file(str(idx_file))
        assert found == str(loc_file)

    def test_find_loc_not_exists(self, tmp_path):
        """Test when loc file doesn't exist."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text("dummy content\n")

        found = find_loc_file(str(idx_file))
        assert found is None

    def test_find_loc_stdin(self):
        """Test that stdin returns None."""
        found = find_loc_file("-")
        assert found is None


class TestResolveShardPath:
    """Test resolving shard file paths."""

    def test_resolve_with_loc_map_relative(self, tmp_path):
        """Test resolving with loc map and relative path."""
        loc_map = {"test-01": "test-01.cdx.gz"}
        base_dir = str(tmp_path)

        path = resolve_shard_path("test-01", base_dir, loc_map)
        assert path == os.path.join(base_dir, "test-01.cdx.gz")

    def test_resolve_with_loc_map_absolute(self, tmp_path):
        """Test resolving with loc map and absolute path."""
        abs_path = "/data/shards/test-01.cdx.gz"
        loc_map = {"test-01": abs_path}
        base_dir = str(tmp_path)

        path = resolve_shard_path("test-01", base_dir, loc_map)
        assert path == abs_path

    def test_resolve_without_loc_map(self, tmp_path):
        """Test resolving without loc map (default naming)."""
        base_dir = str(tmp_path)

        path = resolve_shard_path("test-01", base_dir, None)
        assert path == os.path.join(base_dir, "test-01.cdx.gz")

    def test_resolve_shard_not_in_loc(self, tmp_path):
        """Test resolving when shard not in loc map (falls back to default)."""
        loc_map = {"test-01": "test-01.cdx.gz"}
        base_dir = str(tmp_path)

        path = resolve_shard_path("test-02", base_dir, loc_map)
        assert path == os.path.join(base_dir, "test-02.cdx.gz")


class TestZipnumToFlatCdxj:
    """Test full conversion from ZipNum to flat CDXJ."""

    def create_test_zipnum(self, tmp_path, num_shards=1):
        """Helper to create test ZipNum files."""
        base = tmp_path / "test"

        # Create CDXJ data for each shard
        cdxj_lines = [
            b'com,example)/ 20230101000000 '
            b'{"url":"http://example.com/","status":"200"}\n',
            b'com,example)/page1 20230101010000 '
            b'{"url":"http://example.com/page1","status":"200"}\n',
            b'com,example)/page2 20230101020000 '
            b'{"url":"http://example.com/page2","status":"200"}\n',
        ]

        idx_lines = []
        loc_lines = []

        for shard_num in range(num_shards):
            if num_shards == 1:
                shard_name = "test"
                shard_file = base.parent / "test.cdx.gz"
            else:
                shard_name = f"test-{shard_num+1:02d}"
                shard_file = base.parent / f"test-{shard_num+1:02d}.cdx.gz"

            # Create compressed shard
            with gzip.open(shard_file, "wb") as gz:
                for line in cdxj_lines:
                    gz.write(line)

            # Add idx entry
            idx_lines.append(f"com,example)/ 20230101000000\t{shard_name}\t0\t100\t{shard_num+1}\n")

            # Add loc entry
            loc_lines.append(f"{shard_name}\t{shard_file.name}\n")

        # Write idx file
        idx_file = base.parent / "test.idx"
        idx_file.write_text("".join(idx_lines))

        # Write loc file
        loc_file = base.parent / "test.loc"
        loc_file.write_text("".join(loc_lines))

        return idx_file, cdxj_lines

    def test_single_shard(self, tmp_path, capsys):
        """Test converting single shard ZipNum."""
        idx_file, cdxj_lines = self.create_test_zipnum(tmp_path, num_shards=1)

        zipnum_to_flat_cdxj(str(idx_file), workers=1)

        captured = capsys.readouterr()
        output_lines = captured.out.encode().split(b"\n")

        # Check that all lines are present (filtering empty)
        output_lines = [line for line in output_lines if line]
        assert len(output_lines) == len(cdxj_lines)

        for expected in cdxj_lines:
            assert expected.rstrip() in [line.rstrip() for line in output_lines]

    def test_multiple_shards(self, tmp_path, capsys):
        """Test converting multiple shard ZipNum."""
        idx_file, cdxj_lines = self.create_test_zipnum(tmp_path, num_shards=3)

        zipnum_to_flat_cdxj(str(idx_file), workers=2)

        captured = capsys.readouterr()
        output_lines = captured.out.encode().split(b"\n")

        # Check that all lines are present (3 shards * 3 lines each)
        output_lines = [line for line in output_lines if line]
        assert len(output_lines) == len(cdxj_lines) * 3

    def test_with_custom_base_dir(self, tmp_path, capsys):
        """Test with custom base directory."""
        idx_file, cdxj_lines = self.create_test_zipnum(tmp_path, num_shards=1)

        zipnum_to_flat_cdxj(str(idx_file), base_dir=str(tmp_path), workers=1)

        captured = capsys.readouterr()
        output_lines = captured.out.encode().split(b"\n")
        output_lines = [line for line in output_lines if line]

        assert len(output_lines) == len(cdxj_lines)

    def test_missing_shard_file(self, tmp_path, capsys):
        """Test handling of missing shard file."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text("com,example)/ 20230101000000\ttest\t0\t100\t1\n")

        # Don't create the shard file
        zipnum_to_flat_cdxj(str(idx_file), workers=1)

        captured = capsys.readouterr()
        # Should get warning on stderr
        assert "Warning:" in captured.err or "not found" in captured.err.lower()


class TestCommandLine:
    """Test command-line interface."""

    def test_main_basic(self, tmp_path, capsys):
        """Test main function with basic arguments."""
        idx_file = tmp_path / "test.idx"
        shard_file = tmp_path / "test.cdx.gz"

        # Create test data
        idx_file.write_text("com,example)/ 20230101000000\ttest\t0\t100\t1\n")

        cdxj_data = b'com,example)/ 20230101000000 {"url":"http://example.com/"}\n'
        with gzip.open(shard_file, "wb") as gz:
            gz.write(cdxj_data)

        # Run main
        main(["-i", str(idx_file), "--workers", "1"])

        captured = capsys.readouterr()
        assert b"com,example)" in captured.out.encode()

    def test_main_with_custom_base_dir(self, tmp_path, capsys):
        """Test main with custom base directory."""
        idx_file = tmp_path / "test.idx"
        shard_dir = tmp_path / "shards"
        shard_dir.mkdir()
        shard_file = shard_dir / "test.cdx.gz"

        idx_file.write_text("com,example)/ 20230101000000\ttest\t0\t100\t1\n")

        cdxj_data = b'com,example)/ 20230101000000 {"url":"http://example.com/"}\n'
        with gzip.open(shard_file, "wb") as gz:
            gz.write(cdxj_data)

        main(["-i", str(idx_file), "--base-dir", str(shard_dir), "--workers", "1"])

        captured = capsys.readouterr()
        assert b"com,example)" in captured.out.encode()

    def test_main_with_custom_loc(self, tmp_path, capsys):
        """Test main with custom loc file."""
        idx_file = tmp_path / "test.idx"
        loc_file = tmp_path / "custom.loc"
        shard_file = tmp_path / "test.cdx.gz"

        idx_file.write_text("com,example)/ 20230101000000\ttest\t0\t100\t1\n")
        loc_file.write_text("test\ttest.cdx.gz\n")

        cdxj_data = b'com,example)/ 20230101000000 {"url":"http://example.com/"}\n'
        with gzip.open(shard_file, "wb") as gz:
            gz.write(cdxj_data)

        main(["-i", str(idx_file), "--loc", str(loc_file), "--workers", "1"])

        captured = capsys.readouterr()
        assert b"com,example)" in captured.out.encode()


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_idx_file(self, tmp_path):
        """Test with empty idx file."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text("")

        with pytest.raises(SystemExit):
            zipnum_to_flat_cdxj(str(idx_file), workers=1)

    def test_idx_file_only_comments(self, tmp_path):
        """Test with idx file containing only comments."""
        idx_file = tmp_path / "test.idx"
        idx_file.write_text("# This is a comment\n# Another comment\n")

        with pytest.raises(SystemExit):
            zipnum_to_flat_cdxj(str(idx_file), workers=1)

    def test_parallel_workers(self, tmp_path, capsys):
        """Test with multiple parallel workers."""
        idx_file = tmp_path / "test.idx"
        shard_file = tmp_path / "test.cdx.gz"

        idx_file.write_text("com,example)/ 20230101000000\ttest\t0\t100\t1\n")

        cdxj_data = b'com,example)/ 20230101000000 {"url":"http://example.com/"}\n' * 100
        with gzip.open(shard_file, "wb") as gz:
            gz.write(cdxj_data)

        # Test with different worker counts
        for workers in [1, 2, 4, 8]:
            zipnum_to_flat_cdxj(str(idx_file), workers=workers)
            captured = capsys.readouterr()
            assert len(captured.out.encode().split(b"\n")) >= 100
