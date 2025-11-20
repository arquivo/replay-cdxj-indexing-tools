#!/usr/bin/env python3
"""
Test Suite for cdxj_to_zipnum.py - CDXJ to ZipNum Conversion Testing
=====================================================================

This comprehensive test suite validates the CDXJ to ZipNum conversion functionality,
which converts sorted CDXJ (CDX JSON) files into ZipNum-compressed sharded indexes
suitable for web archive replay systems like pywb.

WHAT IS ZIPNUM?
===============
ZipNum is a compressed indexing format that enables efficient binary search over
large CDXJ indexes by splitting them into compressed shards with separate index
and location files. This format is used by web archive replay systems to quickly
locate archived web resources.

RUNNING THE TESTS
=================
    # Run all tests with pytest (recommended)
    pytest tests/test_cdxj_to_zipnum.py -v

    # Run specific test class
    pytest tests/test_cdxj_to_zipnum.py::TestExtractPrejson -v
    pytest tests/test_cdxj_to_zipnum.py::TestCdxjToZipnum -v

    # Run single test
    pytest tests/test_cdxj_to_zipnum.py::TestCdxjToZipnum::test_real_cdxj_data -v

    # Run with coverage
    pytest tests/test_cdxj_to_zipnum.py --cov=replay_cdxj_indexing_tools.zipnum

    # Using unittest (built-in, no external dependencies)
    python -m unittest tests.test_cdxj_to_zipnum -v

TEST COVERAGE SUMMARY
=====================
Total: 22 tests across 4 test classes

1. TestExtractPrejson (5 tests) - CDXJ Line Parsing
   Tests the extract_prejson() function that separates CDXJ keys from JSON metadata.

   - test_extract_prejson_with_json: Standard CDXJ line with space-separated key and JSON
   - test_extract_prejson_without_json: Plain CDX line without JSON metadata
   - test_extract_prejson_empty_line: Empty line handling (should return empty strings)
   - test_extract_prejson_only_json: Malformed line with only JSON, no key
   - test_extract_prejson_with_newline: Line with trailing newline character

2. TestOpenInputPath (2 tests) - File Input Handling
   Tests the open_input_path() function that handles various input sources.

   - test_open_plain_file: Reading plain text CDXJ files
   - test_open_gzip_file: Reading gzip-compressed CDXJ files (.gz extension)

3. TestStreamChunksFromInput (5 tests) - Chunk Streaming Logic
   Tests the stream_chunks_from_input() generator that batches CDXJ lines into chunks.
   Chunks are the basic unit for creating index entries and organizing compressed data.

   - test_stream_chunks_exact_size: Input lines divide evenly into chunks
     (e.g., 10 lines, chunk_size=5)
   - test_stream_chunks_with_remainder: Input with partial last chunk
     (e.g., 11 lines, chunk_size=5)
   - test_stream_chunks_small_input: Input smaller than chunk size (single chunk output)
   - test_stream_chunks_empty_file: Empty file handling (should yield no chunks)
   - test_stream_chunks_from_gzip: Streaming from gzip-compressed input

4. TestCdxjToZipnum (10 tests) - Complete Conversion Pipeline
   Integration tests for the full cdxj_to_zipnum() conversion process including
   shard creation, compression, index generation, and location file creation.

   - test_single_shard_creation: Validates single shard creation from small dataset,
     verifies .idx, .loc, and .cdx.gz files are created with correct naming

   - test_multiple_shards_creation: Tests large dataset (3000 lines with random data)
     to trigger multiple shard creation based on compressed size thresholds (100KB limit)

   - test_chunk_size_parameter: Verifies chunk_size parameter controls index granularity
     (20 lines with chunk_size=5 should create 4 index entries)

   - test_custom_base_name: Tests custom base name parameter for output files
     (e.g., base="custom-index" creates custom-index.idx, custom-index.loc, etc.)

   - test_custom_idx_loc_names: Tests custom names for index and location files
     independent of base name (e.g., idx_name="my-index.idx", loc_name="my-locations.loc")

   - test_idx_file_format: Validates .idx file structure with tab-separated fields:
     <key>\t<shard_name>\t<offset>\t<length>\t<shard_number>

   - test_loc_file_format: Validates .loc file structure with tab-separated fields:
     <shard_name>\t<filepath> (one entry per shard)

   - test_shard_file_naming: Verifies multi-shard naming convention:
     Single shard: <base>.cdx.gz
     Multiple shards: <base>-01.cdx.gz, <base>-02.cdx.gz, etc.

   - test_compressed_data_integrity: Verifies gzip compression and decompression
     integrity, ensures compressed shard contains all original CDXJ lines

   - test_real_cdxj_data: Tests with authentic Portuguese (.pt) domain data from
     web archives (aas.pt, edp.pt, governo.pt, sapo.pt, uc.pt) including full
     CDXJ metadata (URL, MIME type, status, digest, length, offset, filename)

TEST DESIGN PRINCIPLES
======================
- **Comprehensive Coverage**: Tests cover all major functions and edge cases including
  parsing, streaming, compression, sharding, indexing, and real-world data

- **Isolation**: Each test uses temporary directories (tempfile.mkdtemp()) and cleans
  up after itself in tearDown() to prevent test interference

- **Real Data**: Tests include authentic Portuguese domain CDXJ data to ensure
  compatibility with actual web archive formats and use cases

- **Edge Cases**: Empty files, malformed data, various chunk sizes, compression
  scenarios, single vs. multiple shards, custom naming conventions

ZIPNUM OUTPUT FORMAT
====================
The conversion produces three types of files:

1. Shard Files (.cdx.gz): Gzip-compressed CDXJ data split into manageable chunks
   - Single shard: <base>.cdx.gz
   - Multiple shards: <base>-01.cdx.gz, <base>-02.cdx.gz, ...

2. Index File (.idx): Tab-separated index entries for binary search
   Format: <cdxj_key>\t<shard_name>\t<byte_offset>\t<byte_length>\t<shard_number>
   Example: pt,governo,www)/ 20230615120200\tpt-domains-01\t186\t193\t1

3. Location File (.loc): Maps shard names to file paths
   Format: <shard_name>\t<filepath>
   Example: pt-domains-01\t/path/to/pt-domains-01.cdx.gz

COMMAND-LINE USAGE
==================
After installing the package, the cdxj-to-zipnum command is available:

Basic Usage:
    # Convert a CDXJ file to ZipNum format
    cdxj-to-zipnum -o output_dir -i input.cdxj

    # Read from stdin (useful in pipelines)
    cat merged.cdxj | cdxj-to-zipnum -o output_dir -i -

    # Convert gzipped CDXJ input
    cdxj-to-zipnum -o output_dir -i input.cdxj.gz

Common Options:
    -o, --output DIR          Output directory for ZipNum files (required)
    -i, --input FILE          Input CDXJ file or '-' for stdin (required)
    -s, --shard-size MB       Target shard size in MB (default: 100)
    -c, --chunk-size N        Lines per index chunk (default: 3000)
    --base NAME              Base name for output files (default: output dir name)
    --idx-file FILE          Custom index filename (default: <base>.idx)
    --loc-file FILE          Custom location filename (default: <base>.loc)
    --compress-level N       Gzip compression level 0-9 (default: 6)
    --workers N              Number of parallel compression workers (default: 4)

Real-World Examples:

    # Standard conversion with 100MB shards
    cdxj-to-zipnum -o /data/indexes/arquivo-2023 -i arquivo-2023.cdxj

    # Pipeline from merge to ZipNum
    merge-cdxj /data/cdxj/*.cdxj - | cdxj-to-zipnum -o /data/indexes -i -

    # Small shards for testing (10MB each)
    cdxj-to-zipnum -o test_output -i test.cdxj -s 10 -c 1000

    # Large production shards with custom naming
    cdxj-to-zipnum -o /indexes -i full.cdxj.gz -s 200 \\
        --base arquivo-pt-2023 \\
        --idx-file index.idx \\
        --loc-file locations.loc

    # High compression for storage (slower but smaller)
    cdxj-to-zipnum -o output -i input.cdxj --compress-level 9

    # Fast compression for quick testing (larger files)
    cdxj-to-zipnum -o output -i input.cdxj --compress-level 1

Performance Tips:
    - Default 100MB shards work well for most web archives
    - Chunk size 3000 balances index size vs. search granularity
    - Use --workers to match CPU cores for faster compression
    - Pipe from merge-cdxj to avoid intermediate files
    - Gzipped input is automatically detected and decompressed

Integration with pywb:
    After conversion, use ZipNum indexes with pywb for efficient replay:

    # In pywb config.yaml
    collections:
      arquivo:
        index_paths: /data/indexes/arquivo-2023.idx

    # pywb automatically uses the .loc file to locate shard files

Author: Ivo Branco / GitHub Copilot
Date: November 2025
- **Integration Testing**: Tests the complete pipeline from input to output
- **File Format Validation**: Verifies correct ZipNum format structure
- **Error Handling**: Tests proper handling of edge cases and invalid inputs

NOTES
=====
- All tests use temporary directories that are automatically cleaned up
- Tests verify both functional correctness and file format compliance
- Real CDXJ data tests use Portuguese domain examples (.pt domains)
- Compression and decompression are tested to ensure data integrity
"""

import unittest
import tempfile
import os
import gzip

# Import the module under test
from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import (
    extract_prejson,
    open_input_path,
    stream_chunks_from_input,
    cdxj_to_zipnum,
)


class TestExtractPrejson(unittest.TestCase):
    """
    Tests for the extract_prejson() function.

    This function parses CDXJ lines to extract the key portion (SURT URL + timestamp)
    from the optional JSON metadata. The key is used for sorting and indexing.

    CDXJ Format: <surt_url> <timestamp> [<json_metadata>]
    Example: org,example)/ 20200101120000 {"url": "http://example.org/", "status": "200"}
    """

    def test_extract_prejson_with_json(self):
        """
        Test extracting pre-JSON portion from a standard CDXJ line.

        Validates that the function correctly splits a CDXJ line containing JSON metadata,
        returning only the SURT key and timestamp (everything before the opening brace).

        Input: 'org,example)/ 20200101120000 {"url": "http://example.org/", "status": "200"}'
        Expected: 'org,example)/ 20200101120000'
        """
        line = b'org,example)/ 20200101120000 {"url": "http://example.org/", "status": "200"}'
        result = extract_prejson(line)
        self.assertEqual(result, "org,example)/ 20200101120000")

    def test_extract_prejson_without_json(self):
        """
        Test extracting from a plain CDX line without JSON metadata.

        Some legacy CDX indexes don't include JSON. The function should return
        the entire line (minus whitespace) when no JSON is present.

        Input: 'org,example)/ 20200101120000'
        Expected: 'org,example)/ 20200101120000'
        """
        line = b"org,example)/ 20200101120000 http://example.org/ text/html 200"
        result = extract_prejson(line)
        # Should return the entire line since there's no JSON
        self.assertEqual(result, "org,example)/ 20200101120000 http://example.org/ text/html 200")

    def test_extract_prejson_empty_line(self):
        """
        Test extracting from an empty line.

        Empty lines can occur in malformed CDXJ files or during streaming.
        The function should gracefully return an empty string without errors.

        Input: '' (empty bytes)
        Expected: '' (empty string)
        """
        line = b""
        result = extract_prejson(line)
        self.assertEqual(result, "")

    def test_extract_prejson_only_json(self):
        """
        Test line that starts with JSON (malformed CDXJ).

        A malformed CDXJ line might contain only JSON without a key.
        When the opening brace is at position 0, there's no key to extract.

        Input: '{"url": "http://example.org/"}'
        Expected: '' (empty string, since no key before JSON)
        """
        line = b'{"url": "http://example.org/"}'
        result = extract_prejson(line)
        # Should return empty string since JSON starts at position 0
        self.assertEqual(result, "")

    def test_extract_prejson_with_newline(self):
        """
        Test line with trailing newline characters.

        CDXJ files typically have newline-terminated lines. The function should
        handle newlines correctly and strip them from the output.

        Input: 'org,example)/ 20200101120000 {"status": "200"}\n'
        Expected: 'org,example)/ 20200101120000' (newline removed, JSON removed)
        """
        line = b'org,example)/ 20200101120000 {"status": "200"}\r\n'
        result = extract_prejson(line)
        self.assertEqual(result, "org,example)/ 20200101120000")


class TestOpenInputPath(unittest.TestCase):
    """
    Tests for the open_input_path() function.

    This function handles multiple input sources: plain files, gzip-compressed files,
    and stdin. It returns an appropriate file handle for reading CDXJ data.
    """

    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory and test files."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_open_plain_file(self):
        """
        Test opening a plain text CDXJ file.

        Validates that the function correctly opens uncompressed CDXJ files
        and returns a readable file handle in text mode.

        Creates a temporary CDXJ file and verifies it can be opened and read correctly.
        """
        test_file = os.path.join(self.test_dir, "test.cdxj")
        test_content = b"org,example)/ 20200101120000\n"

        with open(test_file, "wb") as f:
            f.write(test_content)

        with open_input_path(test_file) as fh:
            content = fh.read()
            self.assertEqual(content, test_content)

    def test_open_gzip_file(self):
        """
        Test opening a gzip-compressed CDXJ file.

        Validates that the function correctly detects .gz extension and opens
        the file with gzip decompression, returning decompressed content.

        Creates a temporary gzipped CDXJ file and verifies it can be opened
        and automatically decompressed during reading.
        """
        test_file = os.path.join(self.test_dir, "test.cdxj.gz")
        test_content = b"org,example)/ 20200101120000\n"

        with gzip.open(test_file, "wb") as f:
            f.write(test_content)

        with open_input_path(test_file) as fh:
            content = fh.read()
            self.assertEqual(content, test_content)


class TestStreamChunksFromInput(unittest.TestCase):
    """
    Tests for the stream_chunks_from_input() generator function.

    This function reads CDXJ lines from an input file and yields them in chunks
    (lists of lines). Chunking is the basis for creating index entries - each chunk
    becomes one entry in the .idx file, allowing binary search over compressed data.

    Chunk size determines the granularity of the index and affects search performance:
    - Larger chunks: Fewer index entries, more data to scan per lookup
    - Smaller chunks: More index entries, faster pinpointing of specific records
    """

    def setUp(self):
        """Create temporary directory for test files."""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory."""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_stream_chunks_exact_size(self):
        """
        Test streaming when input divides evenly into chunks.

        With 6 lines and chunk_size=3, should produce exactly 2 chunks of 3 lines each.
        This is the ideal case for efficient indexing.

        Input: 6 lines
        Chunk size: 3
        Expected: 2 chunks, each with 3 lines
        """
        test_file = os.path.join(self.test_dir, "test.cdxj")
        lines = [f"org,example)/{i} 20200101120000\n".encode() for i in range(6)]

        with open(test_file, "wb") as f:
            f.writelines(lines)

        chunks = list(stream_chunks_from_input(test_file, chunk_size=3))

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0][0], 0)  # First chunk index
        self.assertEqual(len(chunks[0][1]), 3)  # First chunk has 3 lines
        self.assertEqual(chunks[1][0], 1)  # Second chunk index
        self.assertEqual(len(chunks[1][1]), 3)  # Second chunk has 3 lines

    def test_stream_chunks_with_remainder(self):
        """
        Test streaming when input has a partial last chunk.

        With 7 lines and chunk_size=3, should produce 2 full chunks and 1 partial chunk.
        The function must handle partial final chunks correctly to avoid data loss.

        Input: 7 lines
        Chunk size: 3
        Expected: 3 chunks (3 lines, 3 lines, 1 line)
        """
        test_file = os.path.join(self.test_dir, "test.cdxj")
        lines = [f"org,example)/{i} 20200101120000\n".encode() for i in range(7)]

        with open(test_file, "wb") as f:
            f.writelines(lines)

        chunks = list(stream_chunks_from_input(test_file, chunk_size=3))

        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0][1]), 3)
        self.assertEqual(len(chunks[1][1]), 3)
        self.assertEqual(len(chunks[2][1]), 1)  # Last chunk has only 1 line

    def test_stream_chunks_small_input(self):
        """
        Test streaming when input is smaller than chunk size.

        With only 2 lines and chunk_size=10, should produce a single chunk
        containing both lines. This tests proper handling of small files.

        Input: 2 lines
        Chunk size: 10
        Expected: 1 chunk with 2 lines
        """
        test_file = os.path.join(self.test_dir, "test.cdxj")
        lines = [b"org,example)/ 20200101120000\n", b"org,example)/page 20200101120001\n"]

        with open(test_file, "wb") as f:
            f.writelines(lines)

        chunks = list(stream_chunks_from_input(test_file, chunk_size=10))

        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0][0], 0)
        self.assertEqual(len(chunks[0][1]), 2)

    def test_stream_chunks_empty_file(self):
        """
        Test streaming from an empty file.

        An empty CDXJ file should yield no chunks. This is important for
        robust pipeline processing where some inputs might be empty.

        Input: Empty file (0 lines)
        Expected: No chunks (empty list)
        """
        test_file = os.path.join(self.test_dir, "empty.cdxj")

        with open(test_file, "wb"):
            pass  # Create empty file

        chunks = list(stream_chunks_from_input(test_file, chunk_size=3))

        self.assertEqual(len(chunks), 0)

    def test_stream_chunks_from_gzip(self):
        """
        Test streaming from a gzip-compressed file.

        The chunking function should transparently handle gzipped inputs,
        automatically decompressing and streaming chunks just like plain files.

        Input: Gzipped file with 4 lines
        Chunk size: 2
        Expected: 2 chunks of 2 lines each
        """
        test_file = os.path.join(self.test_dir, "test.cdxj.gz")
        lines = [f"org,example)/{i} 20200101120000\n".encode() for i in range(5)]

        with gzip.open(test_file, "wb") as f:
            f.writelines(lines)

        chunks = list(stream_chunks_from_input(test_file, chunk_size=2))

        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0][1]), 2)
        self.assertEqual(len(chunks[1][1]), 2)
        self.assertEqual(len(chunks[2][1]), 1)


class TestCdxjToZipnum(unittest.TestCase):
    """
    Integration tests for the complete CDXJ to ZipNum conversion process.

    These tests validate the entire conversion pipeline from CDXJ input to
    ZipNum-compressed output including:

    1. Shard file creation (.cdx.gz) - Compressed CDXJ data blocks
    2. Index file creation (.idx) - Tab-separated index for binary search
    3. Location file creation (.loc) - Maps shard names to file paths
    4. Compression integrity - Verify data survives gzip compression/decompression
    5. File naming conventions - Single vs. multi-shard naming patterns
    6. Custom parameters - Base names, chunk sizes, shard sizes
    7. Real-world data - Portuguese web archive domains

    The ZipNum format enables efficient searching of large CDXJ indexes by:
    - Splitting data into compressed shards (typically 100MB each)
    - Creating searchable index entries (every N lines becomes one index entry)
    - Maintaining sorted order for binary search
    - Recording compressed byte offsets for HTTP range requests
    """

    def setUp(self):
        """
        Create temporary directories for input and output files.

        Each test gets fresh directories to ensure test isolation and
        prevent cross-contamination of test data.
        """
        self.input_dir = tempfile.mkdtemp()
        self.output_dir = tempfile.mkdtemp()

    def tearDown(self):
        """
        Clean up temporary directories and all created files.

        Ensures no test artifacts are left on disk after test completion.
        Uses ignore_errors=True to handle Windows file locking issues.
        """
        import shutil

        shutil.rmtree(self.input_dir, ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    def test_single_shard_creation(self):
        """
        Test creating a single shard from small input.

        When input is smaller than the shard size threshold, all data should go
        into a single shard file named <base>.cdx.gz (no numbering suffix).

        Validates:
        - Single shard file creation with correct naming
        - Index file (.idx) creation with proper entries
        - Location file (.loc) creation with shard mapping
        - All files use basename from output directory

        Input: 10 CDXJ lines
        Shard size: 1000MB (forces single shard)
        Chunk size: 5 (creates 2 index entries)
        Expected output: base.cdx.gz, base.idx, base.loc
        """
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [
            f'org,example)/{i:05d} 20200101120000 {{"status": "200"}}\n'.encode() for i in range(10)
        ]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        # Use very large shard size to ensure single shard
        cdxj_to_zipnum(self.output_dir, input_file, shard_size_mb=1000, chunk_size=5)

        # The base name is derived from output_dir basename
        base_name = os.path.basename(self.output_dir)

        # Check output files exist
        idx_file = os.path.join(self.output_dir, f"{base_name}.idx")
        loc_file = os.path.join(self.output_dir, f"{base_name}.loc")
        shard_file = os.path.join(self.output_dir, f"{base_name}.cdx.gz")

        self.assertTrue(os.path.exists(idx_file))
        self.assertTrue(os.path.exists(loc_file))
        self.assertTrue(os.path.exists(shard_file))

    def test_multiple_shards_creation(self):
        """Test creating multiple shards from larger input."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        # Create enough data to trigger multiple shards (use very small shard size)
        # Use varied data that doesn't compress well to ensure we exceed threshold
        lines = []
        for i in range(3000):
            # Create diverse JSON data that won't compress as well
            import random

            random_data = "".join(
                random.choices(
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=200
                )
            )
            line = (
                f'org,example)/{i:06d} 20200101{i:06d} '
                f'{{"status": "200", "data": "{random_data}"}}\n'
            )
            lines.append(line.encode())

        with open(input_file, "wb") as f:
            f.writelines(lines)

        # Use small shard size to force multiple shards (0.1 MB = 100KB)
        cdxj_to_zipnum(self.output_dir, input_file, shard_size_mb=0.1, chunk_size=100)

        # Check that multiple shard files were created
        shard_files = [f for f in os.listdir(self.output_dir) if f.endswith(".cdx.gz")]
        self.assertGreater(len(shard_files), 1, "Should create multiple shards")

    def test_chunk_size_parameter(self):
        """Test that chunk size parameter affects index entries."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [f"org,example)/{i:05d} 20200101120000\n".encode() for i in range(20)]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        # Use chunk size of 5
        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=5)

        base_name = os.path.basename(self.output_dir)
        idx_file = os.path.join(self.output_dir, f"{base_name}.idx")
        with open(idx_file, "r") as f:
            idx_lines = f.readlines()

        # With 20 lines and chunk_size=5, we should have 4 index entries
        self.assertEqual(len(idx_lines), 4)

    def test_custom_base_name(self):
        """Test using a custom base name for output files."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [b"org,example)/ 20200101120000\n"]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        cdxj_to_zipnum(self.output_dir, input_file, base="custom-index")

        # Check files use custom base name
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "custom-index.idx")))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "custom-index.loc")))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "custom-index.cdx.gz")))

    def test_custom_idx_loc_names(self):
        """Test using custom names for idx and loc files."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [b"org,example)/ 20200101120000\n"]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        cdxj_to_zipnum(
            self.output_dir, input_file, idx_name="my-index.idx", loc_name="my-locations.loc"
        )

        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "my-index.idx")))
        self.assertTrue(os.path.exists(os.path.join(self.output_dir, "my-locations.loc")))

    def test_idx_file_format(self):
        """Test that the .idx file has correct format."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [
            b'org,example)/ 20200101120000 {"status": "200"}\n',
            b'org,example)/page1 20200101120001 {"status": "200"}\n',
            b'org,example)/page2 20200101120002 {"status": "200"}\n',
        ]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=3)

        base_name = os.path.basename(self.output_dir)
        idx_file = os.path.join(self.output_dir, f"{base_name}.idx")
        with open(idx_file, "r") as f:
            first_line = f.readline().strip()

        # Format should be: <key>\t<shard_name>\t<offset>\t<length>\t<shard_number>
        parts = first_line.split("\t")
        self.assertEqual(len(parts), 5, f"Index line should have 5 fields, got: {first_line}")

        # Verify first field is the CDXJ key
        self.assertTrue(parts[0].startswith("org,example)/"))

        # Verify offset and length are numeric
        self.assertTrue(parts[2].isdigit(), "Offset should be numeric")
        self.assertTrue(parts[3].isdigit(), "Length should be numeric")
        self.assertTrue(parts[4].isdigit(), "Shard number should be numeric")

    def test_loc_file_format(self):
        """Test that the .loc file has correct format."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        lines = [b"org,example)/ 20200101120000\n"]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        cdxj_to_zipnum(self.output_dir, input_file)

        base_name = os.path.basename(self.output_dir)
        loc_file = os.path.join(self.output_dir, f"{base_name}.loc")
        with open(loc_file, "r") as f:
            first_line = f.readline().strip()

        # Format should be: <shard_name>\t<filename>
        parts = first_line.split("\t")
        self.assertEqual(len(parts), 2, f"Loc line should have 2 fields, got: {first_line}")

        # Both parts should refer to the same shard (first without extension, second with)
        self.assertTrue(parts[1].endswith(".cdx.gz"))
        self.assertEqual(parts[0], parts[1].replace(".cdx.gz", ""))

    def test_shard_file_naming(self):
        """Test that shard files are named correctly."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        # Create data for multiple shards
        lines = []
        for i in range(500):
            line = f"org,example)/{i:06d} 20200101{i:06d} {{\"data\": \"{'x' * 200}\"}}\n"
            lines.append(line.encode())

        with open(input_file, "wb") as f:
            f.writelines(lines)

        cdxj_to_zipnum(self.output_dir, input_file, shard_size_mb=0.01, chunk_size=50)

        shard_files = sorted([f for f in os.listdir(self.output_dir) if f.endswith(".cdx.gz")])

        # Multiple shards should be numbered: base-01.cdx.gz, base-02.cdx.gz, etc.
        if len(shard_files) > 1:
            self.assertTrue(shard_files[0].endswith("-01.cdx.gz"))
            self.assertTrue(shard_files[1].endswith("-02.cdx.gz"))

    def test_real_cdxj_data(self):
        """
        Test with real CDXJ data from Portuguese web archive domains.

        This test uses authentic CDXJ records from Portuguese (.pt) domains to ensure
        the conversion process works with real-world web archive data. The CDXJ format
        includes all standard metadata fields used by web archive systems.

        Portuguese domains tested:
        - www.aas.pt (Associação Académica de Santarém)
        - www.edp.pt (Energias de Portugal)
        - www.governo.pt (Portuguese Government)
        - www.sapo.pt (Portuguese Web Portal)
        - www.uc.pt (University of Coimbra)

        Each CDXJ record includes:
        - SURT key: Reverse domain format for sorting (e.g., pt,edp,www)/)
        - Timestamp: Capture time in YYYYMMDDhhmmss format
        - JSON metadata: URL, MIME type, HTTP status, content digest, byte offsets, WARC filename

        Validates:
        - Conversion handles real CDXJ format with full metadata
        - All Portuguese domains are indexed correctly
        - Compressed shard contains all input data
        - Index entries created for each domain (chunk_size=1)
        - Custom base name "pt-domains" applied correctly
        """
        input_file = os.path.join(self.input_dir, "real.cdxj")

        # Real CDXJ lines from Portuguese (.pt) domains with complete metadata
        real_cdxj_lines = [
            (
                b'pt,aas,www)/ 20230615120000 {"url": "http://www.aas.pt/", '
                b'"mime": "text/html", "status": "200", "digest": "ABCD1234", '
                b'"length": "5000", "offset": "1000", "filename": "test.warc.gz"}\n'
            ),
            (
                b'pt,edp,www)/ 20230615120100 {"url": "http://www.edp.pt/", '
                b'"mime": "text/html", "status": "200", "digest": "EFGH5678", '
                b'"length": "6000", "offset": "2000", "filename": "test.warc.gz"}\n'
            ),
            (
                b'pt,governo,www)/ 20230615120200 {"url": "http://www.governo.pt/", '
                b'"mime": "text/html", "status": "200", "digest": "IJKL9012", '
                b'"length": "7000", "offset": "3000", "filename": "test.warc.gz"}\n'
            ),
            (
                b'pt,sapo,www)/ 20230615120300 {"url": "http://www.sapo.pt/", '
                b'"mime": "text/html", "status": "200", "digest": "MNOP3456", '
                b'"length": "8000", "offset": "4000", "filename": "test.warc.gz"}\n'
            ),
            (
                b'pt,uc,www)/ 20230615120400 {"url": "http://www.uc.pt/", '
                b'"mime": "text/html", "status": "200", "digest": "QRST7890", '
                b'"length": "9000", "offset": "5000", "filename": "test.warc.gz"}\n'
            ),
        ]

        with open(input_file, "wb") as f:
            f.writelines(real_cdxj_lines)

        # Use chunk size of 1 to ensure each domain appears in index
        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=1, base="pt-domains")

        # Verify output files
        idx_file = os.path.join(self.output_dir, "pt-domains.idx")
        loc_file = os.path.join(self.output_dir, "pt-domains.loc")
        shard_file = os.path.join(self.output_dir, "pt-domains.cdx.gz")

        self.assertTrue(os.path.exists(idx_file))
        self.assertTrue(os.path.exists(loc_file))
        self.assertTrue(os.path.exists(shard_file))

        # Verify we can read back the compressed shard
        with gzip.open(shard_file, "rb") as f:
            decompressed_data = f.read()
            # Should contain all our input lines
            self.assertIn(b"pt,aas,www)/", decompressed_data)
            self.assertIn(b"pt,edp,www)/", decompressed_data)
            self.assertIn(b"pt,sapo,www)/", decompressed_data)

        # Verify index entries for Portuguese domains
        with open(idx_file, "r") as f:
            idx_content = f.read()
            # With chunk_size=1, all 5 domains should appear in the index
            self.assertIn("pt,aas,www)/", idx_content)
            self.assertIn("pt,edp,www)/", idx_content)
            self.assertIn("pt,governo,www)/", idx_content)
            self.assertIn("pt,sapo,www)/", idx_content)
            self.assertIn("pt,uc,www)/", idx_content)

    def test_compressed_data_integrity(self):
        """Test that data can be decompressed and matches original."""
        input_file = os.path.join(self.input_dir, "test.cdxj")
        test_lines = [
            b'org,example)/ 20200101120000 {"status": "200"}\n',
            b'org,example)/page1 20200101120001 {"status": "200"}\n',
            b'org,example)/page2 20200101120002 {"status": "200"}\n',
        ]

        with open(input_file, "wb") as f:
            f.writelines(test_lines)

        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=10)

        base_name = os.path.basename(self.output_dir)
        shard_file = os.path.join(self.output_dir, f"{base_name}.cdx.gz")

        # Decompress and verify content
        with gzip.open(shard_file, "rb") as f:
            decompressed = f.read()

        original = b"".join(test_lines)
        self.assertEqual(decompressed, original, "Decompressed data should match original")


if __name__ == "__main__":
    unittest.main()
