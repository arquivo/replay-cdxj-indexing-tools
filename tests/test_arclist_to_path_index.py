#!/usr/bin/env python3
"""
Tests for arclist_to_path_index.py

This test suite covers:
- Reading arclist files with URLs and paths
- Extracting basenames from URLs/paths
- Converting arclist to path index format
- Folder processing
- CLI integration
"""

import io
import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from replay_cdxj_indexing_tools.arclist_to_path_index import (
    convert_arclist_to_path_index,
    get_arclist_files,
    read_arclist,
)


class TestReadArclist(unittest.TestCase):
    """Test reading arclist files and extracting basenames."""

    def test_read_simple(self):
        """Test reading simple filename list."""
        content = "file1.warc.gz\nfile2.warc.gz\nfile3.warc.gz\n"
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")
        self.assertEqual(entries[0]["original"], "file1.warc.gz")

    def test_read_urls(self):
        """Test reading URLs and extracting basenames."""
        content = """https://example.com/warcs/file1.warc.gz
http://storage.arquivo.pt/data/file2.warc.gz
https://backup.server.com/path/to/file3.warc.gz
"""
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")
        self.assertEqual(entries[0]["original"], "https://example.com/warcs/file1.warc.gz")
        self.assertEqual(entries[1]["filename"], "file2.warc.gz")
        self.assertEqual(entries[1]["original"], "http://storage.arquivo.pt/data/file2.warc.gz")

    def test_read_absolute_paths(self):
        """Test reading absolute paths and extracting basenames."""
        content = """/mnt/storage/warcs/file1.warc.gz
/backup/warcs/file2.warc.gz
/data/collections/file3.warc.gz
"""
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")
        self.assertEqual(entries[0]["original"], "/mnt/storage/warcs/file1.warc.gz")
        self.assertEqual(entries[1]["filename"], "file2.warc.gz")
        self.assertEqual(entries[1]["original"], "/backup/warcs/file2.warc.gz")

    def test_read_relative_paths(self):
        """Test reading relative paths and extracting basenames."""
        content = """data/warcs/file1.warc.gz
../backup/file2.warc.gz
collections/2024/file3.warc.gz
"""
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")
        self.assertEqual(entries[0]["original"], "data/warcs/file1.warc.gz")

    def test_read_mixed_formats(self):
        """Test reading mixed URLs, absolute paths, and relative paths."""
        content = """https://example.com/warcs/file1.warc.gz
/mnt/storage/file2.warc.gz
data/file3.warc.gz
http://backup.com/file4.warc.gz
"""
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 4)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")
        self.assertEqual(entries[1]["filename"], "file2.warc.gz")
        self.assertEqual(entries[2]["filename"], "file3.warc.gz")
        self.assertEqual(entries[3]["filename"], "file4.warc.gz")

    def test_read_empty_file(self):
        """Test reading empty file."""
        f = io.StringIO("")

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 0)

    def test_read_with_comments(self):
        """Test reading file with comments and empty lines."""
        content = """# This is a comment
file1.warc.gz

# Another comment
file2.warc.gz

file3.warc.gz
"""
        f = io.StringIO(content)

        entries = list(read_arclist(f))

        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[0]["filename"], "file1.warc.gz")


class TestConvertArclistToPathIndex(unittest.TestCase):
    """Test converting arclist to path index format."""

    def test_convert_direct_mode_urls(self):
        """Test direct mode conversion with URLs."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("https://storage.arquivo.pt/warcs/AWP-20240101.warc.gz\n")
            f.write("http://backup.com/data/AWP-20240102.warc.gz\n")

        try:
            # Capture stdout
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index(test_file, verbose=False)

            output = mock_stdout.getvalue()
            lines = output.strip().split("\n")

            self.assertEqual(count, 2)
            self.assertEqual(len(lines), 2)
            self.assertIn(
                "AWP-20240101.warc.gz\thttps://storage.arquivo.pt/warcs/AWP-20240101.warc.gz",
                lines[0],
            )
            self.assertIn(
                "AWP-20240102.warc.gz\thttp://backup.com/data/AWP-20240102.warc.gz", lines[1]
            )
        finally:
            os.unlink(test_file)

    def test_convert_direct_mode_absolute_paths(self):
        """Test direct mode conversion with absolute paths."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("/mnt/dc1/warcs/AWP-20240101.warc.gz\n")
            f.write("/mnt/dc2/warcs/AWP-20240102.warc.gz\n")

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index(test_file, verbose=False)

            output = mock_stdout.getvalue()
            lines = output.strip().split("\n")

            self.assertEqual(count, 2)
            self.assertIn("AWP-20240101.warc.gz\t/mnt/dc1/warcs/AWP-20240101.warc.gz", lines[0])
        finally:
            os.unlink(test_file)

    def test_convert_direct_mode_relative_paths(self):
        """Test direct mode conversion with relative paths."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("data/warcs/AWP-20240101.warc.gz\n")
            f.write("../backup/AWP-20240102.warc.gz\n")

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index(test_file, verbose=False)

            output = mock_stdout.getvalue()
            lines = output.strip().split("\n")

            self.assertEqual(count, 2)
            self.assertIn("AWP-20240101.warc.gz\tdata/warcs/AWP-20240101.warc.gz", lines[0])
        finally:
            os.unlink(test_file)

    def test_convert_direct_mode_mixed(self):
        """Test direct mode conversion with mixed URL/path formats."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("https://storage.com/AWP-20240101.warc.gz\n")
            f.write("/mnt/dc1/AWP-20240102.warc.gz\n")
            f.write("data/AWP-20240103.warc.gz\n")

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index(test_file, verbose=False)

            output = mock_stdout.getvalue()
            lines = output.strip().split("\n")

            self.assertEqual(count, 3)
            self.assertEqual(len(lines), 3)
        finally:
            os.unlink(test_file)

    def test_convert_empty_file(self):
        """Test converting empty file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("")

        try:
            with patch("sys.stdout", new_callable=io.StringIO):
                count = convert_arclist_to_path_index(test_file, verbose=False)

            self.assertEqual(count, 0)
        finally:
            os.unlink(test_file)

    def test_convert_from_stdin(self):
        """Test converting from stdin."""
        content = "https://example.com/file1.warc.gz\n/mnt/file2.warc.gz\n"

        with patch("sys.stdin", io.StringIO(content)):
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index("-", verbose=False)

            output = mock_stdout.getvalue()
            lines = output.strip().split("\n")

            self.assertEqual(count, 2)
            self.assertEqual(len(lines), 2)

    def test_convert_custom_separator(self):
        """Test conversion with custom output separator."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("https://example.com/file1.warc.gz\n")

        try:
            with patch("sys.stdout", new_callable=io.StringIO) as mock_stdout:
                count = convert_arclist_to_path_index(
                    test_file, output_separator="|", verbose=False
                )

            output = mock_stdout.getvalue()

            self.assertEqual(count, 1)
            self.assertIn("|", output)
            self.assertIn("file1.warc.gz|https://example.com/file1.warc.gz", output)
        finally:
            os.unlink(test_file)


class TestGetArclistFiles(unittest.TestCase):
    """Test getting arclist files from a folder."""

    def test_get_files_from_folder(self):
        """Test getting all .txt files from a folder."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some test files
            open(os.path.join(tmpdir, "collection1.txt"), "w").close()
            open(os.path.join(tmpdir, "collection2.txt"), "w").close()
            open(os.path.join(tmpdir, "readme.md"), "w").close()  # Should be ignored

            files = get_arclist_files(tmpdir, verbose=False)

            self.assertEqual(len(files), 2)
            self.assertTrue(all(f.endswith(".txt") for f in files))

    def test_get_files_empty_folder(self):
        """Test getting files from empty folder."""
        with tempfile.TemporaryDirectory() as tmpdir:
            files = get_arclist_files(tmpdir, verbose=False)

            self.assertEqual(len(files), 0)

    def test_get_files_invalid_folder(self):
        """Test getting files from non-existent folder."""
        with self.assertRaises(ValueError):
            get_arclist_files("/non/existent/folder", verbose=False)

    def test_get_files_not_a_directory(self):
        """Test getting files from a file path (not a directory)."""
        with tempfile.NamedTemporaryFile() as f:
            with self.assertRaises(ValueError):
                get_arclist_files(f.name, verbose=False)


class TestCLIIntegration(unittest.TestCase):
    """Test CLI integration."""

    def test_cli_basic(self):
        """Test basic CLI usage."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            test_file = f.name
            f.write("https://example.com/file1.warc.gz\n")

        try:
            result = subprocess.run(
                ["arclist-to-path-index", "-i", test_file],
                capture_output=True,
                text=True,
                timeout=5,
            )

            self.assertEqual(result.returncode, 0)
            self.assertIn("file1.warc.gz", result.stdout)
            self.assertIn("https://example.com/file1.warc.gz", result.stdout)
        finally:
            os.unlink(test_file)

    def test_cli_folder_mode(self):
        """Test CLI with folder mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test arclist files
            with open(os.path.join(tmpdir, "col1.txt"), "w") as f:
                f.write("https://example.com/file1.warc.gz\n")
            with open(os.path.join(tmpdir, "col2.txt"), "w") as f:
                f.write("/mnt/file2.warc.gz\n")

            result = subprocess.run(
                ["arclist-to-path-index", "-d", tmpdir],
                capture_output=True,
                text=True,
                timeout=5,
            )

            self.assertEqual(result.returncode, 0)
            self.assertIn("file1.warc.gz", result.stdout)
            self.assertIn("file2.warc.gz", result.stdout)

    def test_cli_missing_args(self):
        """Test CLI with missing required arguments."""
        result = subprocess.run(
            ["arclist-to-path-index"],
            capture_output=True,
            text=True,
            timeout=5,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required", result.stderr.lower())

    def test_cli_multiple_inputs(self):
        """Test CLI with multiple input files."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f1:
            file1 = f1.name
            f1.write("https://example.com/file1.warc.gz\n")

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f2:
            file2 = f2.name
            f2.write("/mnt/file2.warc.gz\n")

        try:
            result = subprocess.run(
                ["arclist-to-path-index", "-i", file1, "-i", file2],
                capture_output=True,
                text=True,
                timeout=5,
            )

            self.assertEqual(result.returncode, 0)
            self.assertIn("file1.warc.gz", result.stdout)
            self.assertIn("file2.warc.gz", result.stdout)
        finally:
            os.unlink(file1)
            os.unlink(file2)


if __name__ == "__main__":
    unittest.main()
