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

3. TestStreamChunksFromInput (7 tests) - Chunk Streaming Logic
   Tests the stream_chunks_from_input() generator that batches CDXJ lines into chunks.
   Chunks are the basic unit for creating index entries and organizing compressed data.

   - test_stream_chunks_exact_size: Input lines divide evenly into chunks
     (e.g., 10 lines, chunk_size=5)
   - test_stream_chunks_with_remainder: Input with partial last chunk
     (e.g., 11 lines, chunk_size=5)
   - test_stream_chunks_small_input: Input smaller than chunk size (single chunk output)
   - test_stream_chunks_empty_file: Empty file handling (should yield no chunks)
   - test_stream_chunks_from_gzip: Streaming from gzip-compressed input
   - test_stream_chunks_skips_blank_lines: Blank and whitespace-only lines are dropped
   - test_stream_chunks_only_blank_lines: Input of only blank lines yields no chunks

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
     <key>\t<shard_name>\t<offset>\t<length>\t<lineno>, where <lineno> is the
     1-based line number of the entry in the .idx (across all shards)

   - test_loc_file_format: Validates .loc file structure with tab-separated fields:
     <shard_name>\t<filepath> (one entry per shard)

   - test_shard_file_naming: Verifies multi-shard naming convention:
     Single shard: <base>.cdx.gz
     Multiple shards: <base>-01.cdx.gz, <base>-02.cdx.gz, etc.

   - test_compressed_data_integrity: Verifies gzip compression and decompression
     integrity, ensures compressed shard contains all original CDXJ lines

   - test_blank_lines_do_not_produce_empty_idx_key: A leading blank line (e.g. from a
     merged input) must not become an empty first key in the .idx or land in the shard

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
   Format: <cdxj_key>\t<shard_name>\t<byte_offset>\t<byte_length>\t<lineno>
   Example: pt,governo,www)/ 20230615120200\tpt-domains-01.cdx.gz\t186\t193\t2

3. Location File (.loc): Maps shard names to file paths
   Format: <shard_name.cdx.gz>\t<filepath>
   Example: pt-domains-01.cdx.gz\tpt-domains-01.cdx.gz

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

import builtins
import gzip
import os
import shutil
import tempfile
import unittest

# Import the module under test
from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import (
    _SINGLE_SHARD_SENTINEL_MB,
    cdxj_to_zipnum,
    extract_prejson,
    main,
    open_input_path,
    parse_args,
    stream_chunks_from_input,
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

    def test_stream_chunks_skips_blank_lines(self):
        """
        Test that blank and whitespace-only lines are skipped.

        A blank line would otherwise become the first line of a chunk and
        produce an empty key in the .idx file (issue #109).

        Input: 3 CDXJ lines mixed with empty, whitespace-only and CRLF-only lines
        Chunk size: 2
        Expected: 2 chunks (2 lines, 1 line) containing only the CDXJ lines
        """
        test_file = os.path.join(self.test_dir, "test.cdxj")
        cdxj_lines = [f"org,example)/{i} 20200101120000\n".encode() for i in range(3)]

        with open(test_file, "wb") as f:
            f.writelines(
                [b"\n", cdxj_lines[0], b"   \n", b"\r\n", cdxj_lines[1], b"\t\n", cdxj_lines[2]]
            )

        chunks = list(stream_chunks_from_input(test_file, chunk_size=2))

        self.assertEqual([chunk for _, chunk in chunks], [cdxj_lines[:2], cdxj_lines[2:]])

    def test_stream_chunks_only_blank_lines(self):
        """
        Test that an input with only blank lines yields no chunks, like an empty file.
        """
        test_file = os.path.join(self.test_dir, "blank.cdxj")

        with open(test_file, "wb") as f:
            f.write(b"\n  \n\n")

        chunks = list(stream_chunks_from_input(test_file, chunk_size=3))

        self.assertEqual(chunks, [])


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
                f"org,example)/{i:06d} 20200101{i:06d} "
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

        # Format should be: <key>\t<shard_name>\t<offset>\t<length>\t<lineno>
        parts = first_line.split("\t")
        self.assertEqual(len(parts), 5, f"Index line should have 5 fields, got: {first_line}")

        # Verify first field is the CDXJ key
        self.assertTrue(parts[0].startswith("org,example)/"))

        # Verify offset and length are numeric
        self.assertTrue(parts[2].isdigit(), "Offset should be numeric")
        self.assertTrue(parts[3].isdigit(), "Length should be numeric")
        self.assertEqual(parts[4], "1", "First idx line should have lineno 1")

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

        # Format should be: <shard_name.cdx.gz>\t<filename.cdx.gz>
        parts = first_line.split("\t")
        self.assertEqual(len(parts), 2, f"Loc line should have 2 fields, got: {first_line}")

        # Both parts should be identical and include .cdx.gz extension
        self.assertTrue(parts[0].endswith(".cdx.gz"))
        self.assertTrue(parts[1].endswith(".cdx.gz"))
        self.assertEqual(parts[0], parts[1])

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

    def test_blank_lines_do_not_produce_empty_idx_key(self):
        """
        Test that a leading blank line does not create an empty first .idx key.

        Reproduces issue #109: a merged input starting with a blank line produced
        an index whose first line was "\t<shard>\t0\t<length>\t1".
        """
        input_file = os.path.join(self.input_dir, "test.cdxj")
        test_lines = [
            b'pt,a)/ 20200101000000 {"url": "http://a.pt/"}\n',
            b'pt,b)/ 20200101000000 {"url": "http://b.pt/"}\n',
        ]

        with open(input_file, "wb") as f:
            f.writelines([b"\n", test_lines[0], b"  \n", test_lines[1], b"\n"])

        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=10)

        base_name = os.path.basename(self.output_dir)
        with open(os.path.join(self.output_dir, f"{base_name}.idx"), encoding="utf-8") as f:
            idx_lines = f.read().splitlines()
        with gzip.open(os.path.join(self.output_dir, f"{base_name}.cdx.gz"), "rb") as f:
            decompressed = f.read()

        self.assertEqual(len(idx_lines), 1)
        self.assertEqual(idx_lines[0].split("\t")[0], "pt,a)/ 20200101000000")
        self.assertEqual(decompressed, b"".join(test_lines))

    def test_idx_file_includes_cdxgz_extension(self):
        """
        Test that .idx file part column includes .cdx.gz extension.

        This is a regression test to ensure the part column in the .idx file
        maintains the .cdx.gz extension as expected by pywb's ZipNum format.

        The .idx file format should be:
        <key>\t<shard_name.cdx.gz>\t<offset>\t<length>\t<lineno>

        For example:
        - Single shard: pt,example)/ 20200101120000\tarchive.cdx.gz\t0\t150\t1
        - Multiple shards: pt,example)/ 20200101120000\tarchive-01.cdx.gz\t0\t150\t1

        This test prevents regression of the issue where the part column was
        incorrectly generated without the .cdx.gz extension (e.g., "awp-01"
        instead of "awp-01.cdx.gz").

        Validates:
        - Single shard: part column is "<base>.cdx.gz"
        - Multiple shards: part column is "<base>-XX.cdx.gz" with numbering
        - .loc file format has .cdx.gz extension in both columns
        """
        # Test single shard case
        input_file = os.path.join(self.input_dir, "single.cdxj")
        lines = [
            b'pt,example)/ 20200101120000 {"status": "200"}\n',
            b'pt,example)/page1 20200101120001 {"status": "200"}\n',
            b'pt,example)/page2 20200101120002 {"status": "200"}\n',
        ]

        with open(input_file, "wb") as f:
            f.writelines(lines)

        # Use large shard size to ensure single shard
        cdxj_to_zipnum(
            self.output_dir, input_file, shard_size_mb=1000, chunk_size=3, base="single-test"
        )

        # Verify .idx file has .cdx.gz extension in part column
        idx_file = os.path.join(self.output_dir, "single-test.idx")
        with open(idx_file, "r") as f:
            first_line = f.readline().strip()

        parts = first_line.split("\t")
        self.assertEqual(len(parts), 5, f"Index line should have 5 fields, got: {first_line}")

        # Part column (index 1) should include .cdx.gz extension
        part_column = parts[1]
        self.assertTrue(
            part_column.endswith(".cdx.gz"),
            f"Part column should end with .cdx.gz, got: {part_column}",
        )
        # The single shard is renamed to "<base>.cdx.gz" after the idx is written;
        # the idx must follow the rename or pywb can't resolve it via the .loc
        self.assertEqual(
            part_column,
            "single-test.cdx.gz",
            f"Expected single shard name with .cdx.gz, got: {part_column}",
        )

        # Verify .loc file format is correct (both columns with .cdx.gz extension)
        loc_file = os.path.join(self.output_dir, "single-test.loc")
        with open(loc_file, "r") as f:
            loc_line = f.readline().strip()

        loc_parts = loc_line.split("\t")
        self.assertEqual(len(loc_parts), 2, f"Loc line should have 2 fields, got: {loc_line}")
        # Both columns should have .cdx.gz extension and be identical
        self.assertTrue(
            loc_parts[0].endswith(".cdx.gz"),
            f"Loc first column should have .cdx.gz extension, got: {loc_parts[0]}",
        )
        self.assertTrue(
            loc_parts[1].endswith(".cdx.gz"),
            f"Loc second column should have .cdx.gz extension, got: {loc_parts[1]}",
        )
        # Both columns should be identical
        self.assertEqual(
            loc_parts[0],
            loc_parts[1],
            f"Loc columns should be identical: '{loc_parts[0]}' should equal '{loc_parts[1]}'",
        )

        # Test multiple shards case
        multi_input_file = os.path.join(self.input_dir, "multi.cdxj")
        multi_lines = []
        for i in range(1000):
            # Use varied data to prevent excessive compression
            random_data = "".join(
                chr(65 + (i * j) % 26) for j in range(50)
            )  # Generate semi-random characters
            multi_lines.append(
                f'pt,example)/{i:05d} 20200101120000 {{"data": "{random_data}"}}\n'.encode()
            )

        with open(multi_input_file, "wb") as f:
            f.writelines(multi_lines)

        # Use small shard size to force multiple shards (50KB)
        multi_output_dir = tempfile.mkdtemp()
        try:
            cdxj_to_zipnum(
                multi_output_dir,
                multi_input_file,
                shard_size_mb=0.05,
                chunk_size=100,
                base="multi-test",
            )

            # Verify .idx file entries have .cdx.gz extension with numbering
            idx_file = os.path.join(multi_output_dir, "multi-test.idx")
            with open(idx_file, "r") as f:
                idx_lines = f.readlines()

            # Check first few index entries
            for idx_line in idx_lines[:3]:
                parts = idx_line.strip().split("\t")
                self.assertEqual(len(parts), 5, f"Index line should have 5 fields, got: {idx_line}")

                part_column = parts[1]
                self.assertTrue(
                    part_column.endswith(".cdx.gz"),
                    f"Part column should end with .cdx.gz, got: {part_column}",
                )
                # Should match pattern: multi-test-XX.cdx.gz (e.g., multi-test-01.cdx.gz)
                self.assertRegex(
                    part_column,
                    r"^multi-test(-\d{2})?\.cdx\.gz$",
                    f"Part column should match 'multi-test-XX.cdx.gz' pattern, got: {part_column}",
                )

            # Verify .loc file entries
            loc_file = os.path.join(multi_output_dir, "multi-test.loc")
            with open(loc_file, "r") as f:
                loc_lines = f.readlines()

            # Each .loc entry should have both columns with .cdx.gz extension (identical)
            for loc_line in loc_lines:
                loc_parts = loc_line.strip().split("\t")
                self.assertEqual(
                    len(loc_parts), 2, f"Loc line should have 2 fields, got: {loc_line}"
                )

                # First column: shard filename with extension (e.g., "multi-test-01.cdx.gz")
                shard_name = loc_parts[0]
                self.assertTrue(
                    shard_name.endswith(".cdx.gz"),
                    f"Loc first column should have .cdx.gz extension, got: {shard_name}",
                )

                # Second column: shard filename with extension (e.g., "multi-test-01.cdx.gz")
                shard_filename = loc_parts[1]
                self.assertTrue(
                    shard_filename.endswith(".cdx.gz"),
                    f"Loc second column should have .cdx.gz extension, got: {shard_filename}",
                )

                # Both columns should be identical
                self.assertEqual(
                    shard_name,
                    shard_filename,
                    f"Loc columns should be identical: '{shard_name}' should equal '{shard_filename}'",
                )

        finally:
            shutil.rmtree(multi_output_dir, ignore_errors=True)

    def test_file_handle_closed_on_exception(self):
        """File handle for the shard must be closed even when compression raises."""
        import unittest.mock as mock

        input_file = os.path.join(self.input_dir, "leak.cdxj")
        lines = [b'pt,example)/ 20200101120000 {"status": "200"}\n'] * 10
        with open(input_file, "wb") as f:
            f.writelines(lines)

        opened_handles = []
        original_open = builtins.open

        def tracking_open(path, mode="r", **kwargs):
            fh = original_open(path, mode, **kwargs)
            if "wb" in mode and str(path).endswith(".cdx.gz"):
                opened_handles.append(fh)
            return fh

        with mock.patch("builtins.open", side_effect=tracking_open):
            with mock.patch(
                "replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum.compress_chunk_worker",
                side_effect=RuntimeError("simulated compression failure"),
            ):
                with self.assertRaises(RuntimeError):
                    cdxj_to_zipnum(self.output_dir, input_file, chunk_size=5)

        for fh in opened_handles:
            self.assertTrue(fh.closed, "Shard file handle must be closed after exception")

    def test_idx_lineno_is_sequential_across_shards(self):
        """
        The 5th idx column is the 1-based line number of the entry in the .idx,
        continuing across shards; it is not the shard number (issue #100).
        """
        input_file = os.path.join(self.input_dir, "lineno.cdxj")
        with open(input_file, "wb") as f:
            for i in range(500):
                f.write(f'org,example)/{i:06d} 20200101000000 {{"n": "{i}"}}\n'.encode())

        cdxj_to_zipnum(
            self.output_dir, input_file, shard_size_mb=0.001, chunk_size=10, base="lineno"
        )

        with open(os.path.join(self.output_dir, "lineno.idx"), "r", encoding="utf-8") as f:
            rows = [line.rstrip("\n").split("\t") for line in f]

        self.assertEqual(len(rows), 50)
        self.assertGreater(len({row[1] for row in rows}), 1, "Test needs multiple shards")
        self.assertEqual([row[4] for row in rows], [str(n) for n in range(1, len(rows) + 1)])


class TestPywbCompatibility(unittest.TestCase):
    """
    Read the generated ZipNum back through pywb's own ZipNumIndexSource, the reader
    used in production, and check every url is found with all of its captures.

    Regression tests for issue #100: pywb uses the 5th idx column (line number) to
    count the blocks to load, so writing the shard number there made pywb miss urls
    that start a chunk and truncate urls whose captures span several chunks. And a
    single-shard idx pointed to "<base>-01.cdx.gz", which is not in the .loc.
    """

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.input_file = os.path.join(self.temp_dir, "input.cdxj")
        self.output_dir = os.path.join(self.temp_dir, "zipnum")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_input(self, num_urls, num_multi_captures):
        """Write num_urls single-capture urls plus one url with many captures."""
        rows = [
            f'com,example)/p{i:06d} 20200101000000 {{"url": "http://example.com/p{i:06d}"}}'
            for i in range(num_urls)
        ]
        multi = f"p{num_urls // 2:06d}x"
        rows += [
            f"com,example)/{multi} {20100101000000 + t * 100000000} "
            f'{{"url": "http://example.com/{multi}"}}'
            for t in range(num_multi_captures)
        ]
        rows.sort()
        with open(self.input_file, "w", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")

        counts = {}
        for row in rows:
            path = row.split(" ", 1)[0].split(")/", 1)[1]
            counts[path] = counts.get(path, 0) + 1
        return counts

    def _assert_all_found(self, idx_path, counts):
        from pywb.warcserver.index.zipnum import ZipNumIndexSource

        source = ZipNumIndexSource(idx_path)
        wrong = {}
        for path, expected in counts.items():
            got = len(list(source.load_index({"url": f"http://example.com/{path}"})))
            if got != expected:
                wrong[path] = (got, expected)
        self.assertEqual(wrong, {}, "urls with wrong capture counts: {path: (got, expected)}")

    def test_pywb_finds_every_url_multi_shard(self):
        """Small chunks over several shards: every chunk start and boundary is exercised."""
        counts = self._write_input(num_urls=1000, num_multi_captures=45)

        cdxj_to_zipnum(
            self.output_dir, self.input_file, shard_size_mb=0.002, chunk_size=10, base="test"
        )

        shards = [f for f in os.listdir(self.output_dir) if f.endswith(".cdx.gz")]
        self.assertGreater(len(shards), 1, "Test needs multiple shards")
        self._assert_all_found(os.path.join(self.output_dir, "test.idx"), counts)

    def test_pywb_finds_every_url_single_shard(self):
        """A single shard is renamed to <base>.cdx.gz; the idx must match the .loc."""
        counts = self._write_input(num_urls=200, num_multi_captures=25)

        cdxj_to_zipnum(
            self.output_dir, self.input_file, shard_size_mb=1000, chunk_size=10, base="test"
        )

        shards = sorted(f for f in os.listdir(self.output_dir) if f.endswith(".cdx.gz"))
        self.assertEqual(shards, ["test.cdx.gz"])
        self._assert_all_found(os.path.join(self.output_dir, "test.idx"), counts)


class TestNumericArgValidation(unittest.TestCase):
    """--shard-size/--chunk-size/--workers reject zero and negative values."""

    def test_parse_args_rejects_non_positive_shard_size(self):
        """parse_args() exits (via argparse) when --shard-size is not positive."""
        with self.assertRaises(SystemExit):
            parse_args(["-i", "in.cdxj", "-o", "out", "--shard-size", "0"])

    def test_parse_args_rejects_non_positive_chunk_size(self):
        """parse_args() exits (via argparse) when --chunk-size is not positive."""
        with self.assertRaises(SystemExit):
            parse_args(["-i", "in.cdxj", "-o", "out", "--chunk-size", "-5"])

    def test_parse_args_rejects_non_positive_workers(self):
        """parse_args() exits (via argparse) when --workers is not positive."""
        with self.assertRaises(SystemExit):
            parse_args(["-i", "in.cdxj", "-o", "out", "--workers", "0"])

    def test_parse_args_accepts_positive_values(self):
        """parse_args() accepts positive values for shard-size/chunk-size/workers."""
        args = parse_args(
            [
                "-i",
                "in.cdxj",
                "-o",
                "out",
                "--shard-size",
                "50",
                "--chunk-size",
                "10",
                "--workers",
                "2",
            ]
        )
        self.assertEqual(args.shard_size, 50)
        self.assertEqual(args.chunk_size, 10)
        self.assertEqual(args.workers, 2)


class TestSingleShardSentinel(unittest.TestCase):
    """Tests for the --single-shard sentinel shard size."""

    def test_main_single_shard_passes_int_sentinel(self):
        """main() --single-shard passes the plain-int sentinel, not float('inf')."""
        import unittest.mock as mock

        with mock.patch(
            "replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum.cdxj_to_zipnum"
        ) as mock_convert:
            main(["-i", "in.cdxj", "-o", "out", "--single-shard"])

        shard_size = mock_convert.call_args.kwargs["shard_size_mb"]
        self.assertIs(type(shard_size), int)
        self.assertEqual(shard_size, _SINGLE_SHARD_SENTINEL_MB)


class TestTempShardCleanup(unittest.TestCase):
    """
    Temp shard files are never left behind or committed truncated, and the
    root-cause exception is never masked, whatever close()/unlink() raise.
    """

    MODULE = "replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum"
    CLOSE_ERRORS = (OSError, ValueError)
    ROTATING_LINES = 3000

    def setUp(self):
        self.input_dir = tempfile.mkdtemp()
        self.output_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.input_dir, ignore_errors=True)
        shutil.rmtree(self.output_dir, ignore_errors=True)

    @staticmethod
    def _make_fail_once_fdopen(exc):
        """Return an os.fdopen replacement whose first returned handle's close() raises exc."""
        original_fdopen = os.fdopen
        close_calls = []

        class FailOnceWriter:
            def __init__(self, fh):
                self._fh = fh

            def write(self, data):
                return self._fh.write(data)

            def tell(self):
                return self._fh.tell()

            def close(self):
                close_calls.append(1)
                self._fh.close()
                if len(close_calls) == 1:
                    raise exc

        def wrapped_fdopen(*args, **kwargs):
            return FailOnceWriter(original_fdopen(*args, **kwargs))

        return wrapped_fdopen

    @staticmethod
    def _make_disk_full_on_close_fdopen():
        """
        Return an os.fdopen replacement whose handles hit ENOSPC when close()
        flushes the write buffer: the buffered bytes never reach the file, as
        with a real disk filling up while a shard is being finalised.
        """
        import errno
        import io

        class DiskFullRaw(io.FileIO):
            disk_full = False

            def write(self, b):
                if self.disk_full:
                    raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
                return super().write(b)

        class DiskFullOnCloseWriter(io.BufferedWriter):
            def close(self):
                self.raw.disk_full = True
                super().close()

        def fake_fdopen(fd, mode="r", buffering=-1):
            assert mode == "wb"
            return DiskFullOnCloseWriter(DiskFullRaw(fd, "wb"), buffer_size=buffering)

        return fake_fdopen

    def _tmp_files(self):
        return [name for name in os.listdir(self.output_dir) if name.endswith(".tmp")]

    def _shard_files(self):
        return [name for name in os.listdir(self.output_dir) if name.endswith(".cdx.gz")]

    def _write_rotating_input(self):
        """Write poorly-compressible input so a 0.1 MB shard size forces rotation."""
        import random

        rng = random.Random(84)
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        input_file = os.path.join(self.input_dir, "test.cdxj")
        with open(input_file, "wb") as f:
            for i in range(self.ROTATING_LINES):
                data = "".join(rng.choices(alphabet, k=200))
                f.write(f'org,example)/{i:06d} 20200101{i:06d} {{"data": "{data}"}}\n'.encode())
        return input_file

    def _write_small_input(self):
        input_file = os.path.join(self.input_dir, "small.cdxj")
        with open(input_file, "wb") as f:
            f.writelines([b'pt,example)/ 20200101120000 {"status": "200"}\n'] * 10)
        return input_file

    def _assert_warning(self, log_ctx, text):
        self.assertTrue(
            any(text in line for line in log_ctx.output),
            f"Expected a warning containing {text!r}, got: {log_ctx.output}",
        )

    def test_rotation_flush_failure_is_fatal(self):
        """
        ENOSPC while flushing a rotated-out shard fails the run with that error
        instead of committing a truncated shard, and removes the temp shard.
        """
        import errno
        import unittest.mock as mock

        input_file = self._write_rotating_input()
        with mock.patch("os.fdopen", side_effect=self._make_disk_full_on_close_fdopen()):
            with self.assertRaises(OSError) as raised:
                cdxj_to_zipnum(self.output_dir, input_file, shard_size_mb=0.1, chunk_size=100)

        self.assertEqual(raised.exception.errno, errno.ENOSPC)
        self.assertEqual(self._tmp_files(), [])
        self.assertEqual(self._shard_files(), [], "a truncated shard must not be committed")

    def test_rotation_close_error_keeps_root_cause(self):
        """
        Any Exception closing a rotated-out shard propagates unchanged, the temp
        shard is removed, and the shard is not committed.
        """
        import unittest.mock as mock

        input_file = self._write_rotating_input()
        for exc_type in self.CLOSE_ERRORS:
            with self.subTest(exc_type=exc_type.__name__):
                close_error = exc_type("simulated close failure")
                with mock.patch("os.fdopen", side_effect=self._make_fail_once_fdopen(close_error)):
                    with self.assertRaises(exc_type) as raised:
                        cdxj_to_zipnum(
                            self.output_dir, input_file, shard_size_mb=0.1, chunk_size=100
                        )

                self.assertIs(raised.exception, close_error)
                self.assertEqual(self._tmp_files(), [])
                self.assertEqual(self._shard_files(), [])

    def test_rotation_fdopen_failure_closes_fd_and_removes_temp(self):
        """
        If opening the next shard fails, the original error propagates, the fd
        is closed and the temp file removed, including when fdopen() had already
        closed the fd itself (as io.open does when it fails after wrapping it).
        """
        import unittest.mock as mock

        input_file = self._write_rotating_input()
        original_fdopen = os.fdopen
        for fdopen_closes_fd in (False, True):
            with self.subTest(fdopen_closes_fd=fdopen_closes_fd):
                shutil.rmtree(self.output_dir)
                fds = []
                open_error = MemoryError("simulated fdopen failure")

                def fdopen_failing_on_rotation(fd, *args, **kwargs):
                    fds.append(fd)
                    if len(fds) == 2:  # the first shard opens fine; fail opening the second
                        if fdopen_closes_fd:
                            os.close(fd)
                        raise open_error
                    return original_fdopen(fd, *args, **kwargs)

                with mock.patch("os.fdopen", side_effect=fdopen_failing_on_rotation):
                    with self.assertRaises(MemoryError) as raised:
                        cdxj_to_zipnum(
                            self.output_dir, input_file, shard_size_mb=0.1, chunk_size=100
                        )

                self.assertIs(raised.exception, open_error)
                self.assertEqual(len(fds), 2)
                with self.assertRaises(OSError, msg="the fd of the failed shard was leaked"):
                    os.fstat(fds[1])
                self.assertEqual(self._tmp_files(), [])

    def test_cleanup_close_error_is_logged(self):
        """
        Any Exception closing the shard while unwinding an error is logged, the
        temp shard is still removed, and the root-cause exception propagates.
        """
        import unittest.mock as mock

        input_file = self._write_small_input()
        root_cause = RuntimeError("simulated compression failure")
        for exc_type in self.CLOSE_ERRORS:
            with self.subTest(exc_type=exc_type.__name__):
                close_error = exc_type("simulated close failure")
                with mock.patch(f"{self.MODULE}.compress_chunk_worker", side_effect=root_cause):
                    with mock.patch(
                        "os.fdopen", side_effect=self._make_fail_once_fdopen(close_error)
                    ):
                        with self.assertLogs(self.MODULE, level="WARNING") as log_ctx:
                            with self.assertRaises(RuntimeError) as raised:
                                cdxj_to_zipnum(self.output_dir, input_file, chunk_size=5)

                self.assertIs(raised.exception, root_cause)
                self._assert_warning(log_ctx, "Error closing shard file during cleanup")
                self.assertEqual(self._tmp_files(), [])

    def test_cleanup_unlink_error_is_logged(self):
        """
        Any Exception removing the temp shard while unwinding an error is logged
        and the root-cause exception still propagates, also when close() failed too.
        """
        import unittest.mock as mock

        input_file = self._write_small_input()
        root_cause = RuntimeError("simulated compression failure")
        for exc_type in self.CLOSE_ERRORS:
            with self.subTest(exc_type=exc_type.__name__):
                close_error = exc_type("simulated close failure")
                with mock.patch(f"{self.MODULE}.compress_chunk_worker", side_effect=root_cause):
                    with mock.patch(
                        "os.fdopen", side_effect=self._make_fail_once_fdopen(close_error)
                    ):
                        with mock.patch(
                            f"{self.MODULE}.os.unlink",
                            side_effect=exc_type("simulated unlink failure"),
                        ) as mock_unlink:
                            with self.assertLogs(self.MODULE, level="WARNING") as log_ctx:
                                with self.assertRaises(RuntimeError) as raised:
                                    cdxj_to_zipnum(self.output_dir, input_file, chunk_size=5)

                self.assertIs(raised.exception, root_cause)
                mock_unlink.assert_called_once()
                self._assert_warning(log_ctx, "Error closing shard file during cleanup")
                self._assert_warning(log_ctx, "Error removing temp shard file")

    def test_final_close_error_propagates_and_removes_temp(self):
        """
        A close() failure on the final shard (e.g. ENOSPC) propagates and the
        unfinished temp shard is not left behind.
        """
        import unittest.mock as mock

        input_file = self._write_small_input()
        for exc_type in self.CLOSE_ERRORS:
            with self.subTest(exc_type=exc_type.__name__):
                close_error = exc_type("simulated close failure")
                with mock.patch("os.fdopen", side_effect=self._make_fail_once_fdopen(close_error)):
                    with self.assertRaises(exc_type) as raised:
                        cdxj_to_zipnum(self.output_dir, input_file, chunk_size=5)

                self.assertIs(raised.exception, close_error)
                self.assertEqual(self._tmp_files(), [])

    def test_called_while_caller_handles_an_exception(self):
        """
        Running inside a caller's except block must still commit every shard:
        each shard listed in .loc exists and all input lines are present.
        """
        cases = {
            "rotating": (self._write_rotating_input(), 0.1, 100, self.ROTATING_LINES),
            "single": (self._write_small_input(), 100, 5, 10),
        }
        for name, (input_file, shard_size_mb, chunk_size, expected_lines) in cases.items():
            with self.subTest(case=name):
                shutil.rmtree(self.output_dir)
                try:
                    raise KeyError("caller's own exception being handled")
                except KeyError:
                    cdxj_to_zipnum(
                        self.output_dir,
                        input_file,
                        shard_size_mb=shard_size_mb,
                        chunk_size=chunk_size,
                    )

                base = os.path.basename(self.output_dir)
                with open(os.path.join(self.output_dir, f"{base}.loc"), encoding="utf-8") as locf:
                    shards = [line.split("\t")[0] for line in locf if line.strip()]
                self.assertTrue(shards)
                total_lines = 0
                for shard in shards:
                    shard_path = os.path.join(self.output_dir, shard)
                    self.assertTrue(os.path.exists(shard_path), f"{shard} listed but missing")
                    with gzip.open(shard_path, "rb") as gz:
                        total_lines += sum(1 for _ in gz)
                self.assertEqual(total_lines, expected_lines)
                self.assertEqual(self._tmp_files(), [])


if __name__ == "__main__":
    unittest.main()
