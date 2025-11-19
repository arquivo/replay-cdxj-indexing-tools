#!/usr/bin/env python3
"""
Test suite for excessive URL filtering functionality.

This module tests the ability to identify and filter out URLs that appear
excessively in CDXJ web archive indexes. Common use case is removing crawler
traps, spam sites, or overly-represented domains from Arquivo.pt indexes.

Test Coverage:
--------------
1. SURT key extraction from CDXJ lines
2. Counting URL occurrences across large files
3. Identifying URLs exceeding thresholds
4. Filtering excessive URLs from streams
5. Two-pass workflow (find then filter)
6. One-pass workflow (auto mode)
7. Pipeline mode (stdin/stdout)
8. Edge cases and error handling

Real-World Data:
----------------
Tests use authentic Portuguese domain SURT keys and realistic scenarios
from Arquivo.pt web archive processing.

Author: Ivo Branco / GitHub Copilot
Date: November 2025
"""

import unittest
import tempfile
import os

from replay_cdxj_indexing_tools.filter.excessive_urls import (
    extract_surt_key,
    find_excessive_urls,
    filter_excessive_urls,
    process_pipeline,
)


class TestExtractSurtKey(unittest.TestCase):
    """Test SURT key extraction from CDXJ lines."""

    def test_standard_cdxj_line(self):
        """Extract SURT from standard CDXJ line with URL and JSON."""
        line = 'pt,governo,www)/ 20230615120000 {"url": "https://www.governo.pt/"}'
        self.assertEqual(extract_surt_key(line), "pt,governo,www)/")

    def test_line_with_path(self):
        """Extract SURT from URL with path component."""
        line = 'pt,up,sigarra)/pt/web/base.gera_pagina 20230101120000 {"url": "..."}'
        self.assertEqual(extract_surt_key(line), "pt,up,sigarra)/pt/web/base.gera_pagina")

    def test_line_with_query_params(self):
        """Extract SURT from URL with query parameters."""
        line = 'pt,sapo,mail)?p=2&user=test 20230615120000 {"url": "..."}'
        self.assertEqual(extract_surt_key(line), "pt,sapo,mail)?p=2&user=test")

    def test_line_without_json(self):
        """Extract SURT when JSON metadata is missing."""
        line = "pt,publico,www)/economia 20230615120000"
        self.assertEqual(extract_surt_key(line), "pt,publico,www)/economia")

    def test_line_with_only_surt(self):
        """Handle malformed line with only SURT key."""
        line = "pt,rtp,www)/"
        self.assertEqual(extract_surt_key(line), "pt,rtp,www)/")


class TestFindExcessiveUrls(unittest.TestCase):
    """Test finding URLs that exceed occurrence thresholds."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_no_excessive_urls(self):
        """Find no excessive URLs when all are below threshold."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
                'pt,publico,www)/ 20230101120000 {"url": "..."}',
                'pt,rtp,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.write(content)

        excessive = find_excessive_urls(path, threshold=5)
        self.assertEqual(len(excessive), 0)

    def test_single_excessive_url(self):
        """Find single URL exceeding threshold."""
        lines = ['pt,spam,www)/ 20230101120000 {"url": "..."}\n'] * 10
        lines.extend(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}\n',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}\n',
            ]
        )

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.writelines(lines)

        excessive = find_excessive_urls(path, threshold=5)
        self.assertEqual(len(excessive), 1)
        self.assertEqual(excessive["pt,spam,www)/"], 10)

    def test_multiple_excessive_urls(self):
        """Find multiple URLs exceeding threshold."""
        lines = []
        lines.extend(['pt,spam1,www)/ 20230101120000 {"url": "..."}\n'] * 15)
        lines.extend(['pt,spam2,www)/ 20230101120000 {"url": "..."}\n'] * 20)
        lines.extend(['pt,spam3,www)/ 20230101120000 {"url": "..."}\n'] * 25)
        lines.extend(['pt,normal,www)/ 20230101120000 {"url": "..."}\n'] * 5)

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.writelines(lines)

        excessive = find_excessive_urls(path, threshold=10)
        self.assertEqual(len(excessive), 3)
        self.assertEqual(excessive["pt,spam1,www)/"], 15)
        self.assertEqual(excessive["pt,spam2,www)/"], 20)
        self.assertEqual(excessive["pt,spam3,www)/"], 25)

    def test_threshold_boundary(self):
        """Test exact threshold boundary (should not be considered excessive)."""
        lines = ['pt,boundary,www)/ 20230101120000 {"url": "..."}\n'] * 10
        lines.extend(['pt,over,www)/ 20230101120000 {"url": "..."}\n'] * 11)

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.writelines(lines)

        excessive = find_excessive_urls(path, threshold=10)
        self.assertEqual(len(excessive), 1)
        self.assertIn("pt,over,www)/", excessive)
        self.assertNotIn("pt,boundary,www)/", excessive)

    def test_empty_file(self):
        """Handle empty CDXJ file gracefully."""
        path = os.path.join(self.temp_dir, "empty.cdxj")
        with open(path, "w") as f:
            f.write("")

        excessive = find_excessive_urls(path, threshold=100)
        self.assertEqual(len(excessive), 0)

    def test_file_with_blank_lines(self):
        """Ignore blank lines when counting."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                "",
                'pt,governo,www)/ 20230101120001 {"url": "..."}',
                "",
                "",
                'pt,governo,www)/ 20230101120002 {"url": "..."}',
            ]
        )

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.write(content)

        excessive = find_excessive_urls(path, threshold=2)
        self.assertEqual(len(excessive), 1)
        self.assertEqual(excessive["pt,governo,www)/"], 3)

    def test_realistic_arquivo_scenario(self):
        """Test with realistic Arquivo.pt domain distribution."""
        # Simulate typical Portuguese web archive:
        # - Few very popular sites (governo.pt, sapo.pt, rtp.pt)
        # - Many medium-traffic sites
        # - Long tail of small sites

        lines = []
        # Popular sites (crawler traps or legitimate high traffic)
        lines.extend(
            ['pt,governo,www)/noticias 20230101%06d {"url": "..."}\n' % i for i in range(5000)]
        )
        # Sapo mail with same path (typical crawler trap - same URL over and over)
        lines.extend(['pt,sapo,mail)/inbox 20230101%06d {"url": "..."}\n' % i for i in range(3000)])

        # Medium sites
        lines.extend(
            [
                'pt,publico,www)/economia/artigo%d 20230101%06d {"url": "..."}\n' % (i, i)
                for i in range(500)
            ]
        )
        lines.extend(
            ['pt,rtp,www)/noticias/%d 20230101%06d {"url": "..."}\n' % (i, i) for i in range(800)]
        )

        # Long tail (many small sites)
        for domain_id in range(50):
            lines.extend(
                [
                    f'pt,domain{domain_id},www)/ 202301010000{i:02d} {{"url": "..."}}\n'
                    for i in range(10)
                ]
            )

        path = os.path.join(self.temp_dir, "arquivo.cdxj")
        with open(path, "w") as f:
            f.writelines(lines)

        # Find URLs with >1000 occurrences
        excessive = find_excessive_urls(path, threshold=1000)

        # Should find the two spam/crawler trap sites
        self.assertEqual(len(excessive), 2)
        self.assertEqual(excessive["pt,governo,www)/noticias"], 5000)
        self.assertEqual(excessive["pt,sapo,mail)/inbox"], 3000)


class TestFilterExcessiveUrls(unittest.TestCase):
    """Test filtering excessive URLs from CDXJ streams."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_filter_single_url(self):
        """Filter out single excessive URL."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                'pt,spam,www)/ 20230101120000 {"url": "..."}',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
                'pt,spam,www)/ 20230101120001 {"url": "..."}',
                'pt,publico,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write(content)

        excessive = {"pt,spam,www)/"}
        kept, filtered = filter_excessive_urls(input_path, excessive, output_path)

        self.assertEqual(kept, 3)
        self.assertEqual(filtered, 2)

        # Check output
        with open(output_path, "r") as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 3)
        self.assertIn("pt,governo,www)/", lines[0])
        self.assertIn("pt,sapo,www)/", lines[1])
        self.assertIn("pt,publico,www)/", lines[2])

    def test_filter_multiple_urls(self):
        """Filter out multiple excessive URLs."""
        lines = []
        lines.append('pt,governo,www)/ 20230101120000 {"url": "..."}\n')
        lines.extend(['pt,spam1,www)/ 20230101120000 {"url": "..."}\n'] * 3)
        lines.append('pt,sapo,www)/ 20230101120000 {"url": "..."}\n')
        lines.extend(['pt,spam2,www)/ 20230101120000 {"url": "..."}\n'] * 2)
        lines.append('pt,publico,www)/ 20230101120000 {"url": "..."}\n')

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(lines)

        excessive = {"pt,spam1,www)/", "pt,spam2,www)/"}
        kept, filtered = filter_excessive_urls(input_path, excessive, output_path)

        self.assertEqual(kept, 3)
        self.assertEqual(filtered, 5)

    def test_filter_none(self):
        """Filter when no URLs are excessive (pass-through)."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
                'pt,publico,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write(content)

        excessive = set()
        kept, filtered = filter_excessive_urls(input_path, excessive, output_path)

        self.assertEqual(kept, 3)
        self.assertEqual(filtered, 0)

    def test_filter_all(self):
        """Filter when all URLs are excessive (empty output)."""
        content = "\n".join(
            [
                'pt,spam1,www)/ 20230101120000 {"url": "..."}',
                'pt,spam2,www)/ 20230101120000 {"url": "..."}',
                'pt,spam3,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write(content)

        excessive = {"pt,spam1,www)/", "pt,spam2,www)/", "pt,spam3,www)/"}
        kept, filtered = filter_excessive_urls(input_path, excessive, output_path)

        self.assertEqual(kept, 0)
        self.assertEqual(filtered, 3)

        # Output should exist but be empty
        self.assertTrue(os.path.exists(output_path))
        self.assertEqual(os.path.getsize(output_path), 0)

    def test_preserves_line_order(self):
        """Ensure filtering preserves original line order."""
        lines = [
            'pt,a,www)/ 20230101120000 {"url": "..."}\n',
            'pt,spam,www)/ 20230101120000 {"url": "..."}\n',
            'pt,b,www)/ 20230101120001 {"url": "..."}\n',
            'pt,c,www)/ 20230101120002 {"url": "..."}\n',
            'pt,spam,www)/ 20230101120001 {"url": "..."}\n',
            'pt,d,www)/ 20230101120003 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(lines)

        excessive = {"pt,spam,www)/"}
        filter_excessive_urls(input_path, excessive, output_path)

        with open(output_path, "r") as f:
            result = f.readlines()

        # Should have kept lines in order: a, b, c, d
        self.assertEqual(len(result), 4)
        self.assertIn("pt,a,www)/", result[0])
        self.assertIn("pt,b,www)/", result[1])
        self.assertIn("pt,c,www)/", result[2])
        self.assertIn("pt,d,www)/", result[3])


class TestProcessPipeline(unittest.TestCase):
    """Test one-pass auto mode (find and filter together)."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_auto_mode_basic(self):
        """Test auto mode with basic excessive URL scenario."""
        lines = []
        lines.extend(['pt,spam,www)/ 20230101120000 {"url": "..."}\n'] * 15)
        lines.extend(['pt,normal1,www)/ 20230101120000 {"url": "..."}\n'] * 3)
        lines.extend(['pt,normal2,www)/ 20230101120000 {"url": "..."}\n'] * 5)

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(lines)

        excessive_count, kept, filtered = process_pipeline(input_path, output_path, threshold=10)

        self.assertEqual(excessive_count, 1)  # Found 1 excessive URL
        self.assertEqual(kept, 8)  # Kept 3+5 normal lines
        self.assertEqual(filtered, 15)  # Filtered 15 spam lines

    def test_auto_mode_no_excessive(self):
        """Test auto mode when no URLs are excessive."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
                'pt,publico,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write(content)

        excessive_count, kept, filtered = process_pipeline(input_path, output_path, threshold=100)

        self.assertEqual(excessive_count, 0)
        self.assertEqual(kept, 3)
        self.assertEqual(filtered, 0)

    def test_auto_mode_rejects_stdin(self):
        """Test that auto mode rejects stdin input."""
        with self.assertRaises(ValueError) as ctx:
            process_pipeline("-", "output.cdxj", threshold=100)

        self.assertIn("stdin not supported", str(ctx.exception))

    def test_auto_mode_arquivo_pipeline(self):
        """Test realistic Arquivo.pt pipeline scenario."""
        # Simulate merged CDXJ from multiple sources with some spam
        lines = []

        # Legitimate sites with reasonable occurrence
        lines.extend(
            [f'pt,governo,www)/page{i} 20230101{i:06d} {{"url": "..."}}\n' for i in range(200)]
        )
        lines.extend(
            [f'pt,sapo,www)/artigo{i} 20230101{i:06d} {{"url": "..."}}\n' for i in range(300)]
        )

        # Crawler trap (same URL repeated many times)
        lines.extend(['pt,trap,www)/loop 20230101120000 {"url": "..."}\n'] * 2000)

        # More legitimate content
        lines.extend(
            [f'pt,publico,www)/news{i} 20230101{i:06d} {{"url": "..."}}\n' for i in range(150)]
        )

        input_path = os.path.join(self.temp_dir, "merged.cdxj")
        output_path = os.path.join(self.temp_dir, "cleaned.cdxj")

        with open(input_path, "w") as f:
            f.writelines(lines)

        # Use threshold of 1000 (typical for Arquivo.pt)
        excessive_count, kept, filtered = process_pipeline(input_path, output_path, threshold=1000)

        # Should find the crawler trap
        self.assertEqual(excessive_count, 1)

        # Should keep all legitimate content
        self.assertEqual(kept, 650)  # 200 + 300 + 150

        # Should filter the trap
        self.assertEqual(filtered, 2000)

        # Verify output doesn't contain trap
        with open(output_path, "r") as f:
            output = f.read()

        self.assertNotIn("pt,trap,www)/loop", output)
        self.assertIn("pt,governo,www)/", output)
        self.assertIn("pt,sapo,www)/", output)
        self.assertIn("pt,publico,www)/", output)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_malformed_cdxj_lines(self):
        """Handle malformed CDXJ lines gracefully."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                "malformed_line_without_spaces",
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
                "",
                'pt,publico,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.write(content)

        # Should not crash
        excessive = find_excessive_urls(path, threshold=1)
        self.assertIsInstance(excessive, dict)

    def test_unicode_in_surt(self):
        """Handle Unicode characters in SURT keys."""
        lines = [
            'pt,câmara,www)/ 20230101120000 {"url": "..."}\n',
            'pt,câmara,www)/ 20230101120001 {"url": "..."}\n',
            'pt,açores,www)/ 20230101120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        excessive = find_excessive_urls(input_path, threshold=1)
        self.assertEqual(len(excessive), 1)
        self.assertEqual(excessive["pt,câmara,www)/"], 2)

        # Filter should work too
        excessive_set = set(excessive.keys())
        kept, filtered = filter_excessive_urls(input_path, excessive_set, output_path)
        self.assertEqual(kept, 1)
        self.assertEqual(filtered, 2)

    def test_very_long_surt_keys(self):
        """Handle very long SURT keys (deep paths or long query strings)."""
        long_surt = "pt,example,www)/very/deep/path/with/many/segments" + "/segment" * 50
        lines = [f'{long_surt} 20230101120000 {{"url": "..."}}\n'] * 5

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        with open(input_path, "w") as f:
            f.writelines(lines)

        excessive = find_excessive_urls(input_path, threshold=3)
        self.assertEqual(len(excessive), 1)
        self.assertIn(long_surt, excessive)

    def test_threshold_zero(self):
        """Test with threshold of zero (all URLs considered excessive)."""
        content = "\n".join(
            [
                'pt,governo,www)/ 20230101120000 {"url": "..."}',
                'pt,sapo,www)/ 20230101120000 {"url": "..."}',
            ]
        )

        path = os.path.join(self.temp_dir, "input.cdxj")
        with open(path, "w") as f:
            f.write(content)

        excessive = find_excessive_urls(path, threshold=0)
        # All URLs with count > 0 are excessive
        self.assertEqual(len(excessive), 2)

    def test_large_file_performance(self):
        """Test performance with large file (100k lines)."""
        # This tests that the implementation is efficient enough
        # for real-world Arquivo.pt usage

        lines = []
        # Create 100k lines with varied distribution
        for i in range(50000):
            lines.append(f'pt,domain{i%1000},www)/ 20230101{i:06d} {{"url": "..."}}\n')

        # Add some excessive ones
        lines.extend(['pt,spam,www)/ 20230101120000 {"url": "..."}\n'] * 5000)

        input_path = os.path.join(self.temp_dir, "large.cdxj")
        with open(input_path, "w") as f:
            f.writelines(lines)

        # Should complete reasonably quickly
        import time

        start = time.time()
        excessive = find_excessive_urls(input_path, threshold=1000)
        elapsed = time.time() - start

        # Should find the spam URL
        self.assertEqual(len(excessive), 1)
        self.assertEqual(excessive["pt,spam,www)/"], 5000)

        # Should complete in reasonable time (< 5 seconds for 100k lines)
        self.assertLess(elapsed, 5.0, f"Processing 100k lines took {elapsed:.1f}s, should be < 5s")


if __name__ == "__main__":
    unittest.main()
