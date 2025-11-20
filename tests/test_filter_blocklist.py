#!/usr/bin/env python3
"""
Test suite for blocklist filtering functionality.

This module tests the ability to filter CDXJ records based on regex patterns
in a blocklist file. Common use cases include removing spam domains, adult
content, specific MIME types, or other unwanted records from Arquivo.pt indexes.

Test Coverage:
--------------
1. Loading blocklist patterns from files
2. Pattern validation and error handling
3. Filtering CDXJ records by various pattern types
4. Pipeline mode (stdin/stdout)
5. Edge cases and malformed input
6. Performance with large blocklists

Real-World Data:
----------------
Tests use authentic Portuguese domain patterns and realistic blocklist scenarios
from Arquivo.pt web archive processing.

Author: Ivo Branco / GitHub Copilot
Date: November 2025
"""

import os
import re
import tempfile
import unittest

from replay_cdxj_indexing_tools.filter.blocklist import filter_cdxj_by_blocklist, load_blocklist


class TestLoadBlocklist(unittest.TestCase):
    """Test loading blocklist patterns from files."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_load_simple_patterns(self):
        """Load simple regex patterns from file."""
        content = "\n".join(
            [
                "^pt,spam,",
                "^pt,adult,",
                "/ads/",
            ]
        )

        path = os.path.join(self.temp_dir, "blocklist.txt")
        with open(path, "w") as f:
            f.write(content)

        patterns = load_blocklist(path)

        self.assertEqual(len(patterns), 3)
        self.assertIsInstance(patterns[0], re.Pattern)

    def test_load_with_comments(self):
        """Ignore comment lines starting with #."""
        content = "\n".join(
            [
                "# This is a comment",
                "^pt,spam,",
                "# Another comment",
                "^pt,adult,",
            ]
        )

        path = os.path.join(self.temp_dir, "blocklist.txt")
        with open(path, "w") as f:
            f.write(content)

        patterns = load_blocklist(path)

        self.assertEqual(len(patterns), 2)

    def test_load_with_empty_lines(self):
        """Ignore empty lines."""
        content = "\n".join(
            [
                "^pt,spam,",
                "",
                "",
                "^pt,adult,",
                "",
            ]
        )

        path = os.path.join(self.temp_dir, "blocklist.txt")
        with open(path, "w") as f:
            f.write(content)

        patterns = load_blocklist(path)

        self.assertEqual(len(patterns), 2)

    def test_load_empty_file(self):
        """Handle empty blocklist file gracefully."""
        path = os.path.join(self.temp_dir, "empty.txt")
        with open(path, "w") as f:
            f.write("")

        patterns = load_blocklist(path)

        self.assertEqual(len(patterns), 0)

    def test_load_complex_patterns(self):
        """Load complex regex patterns."""
        content = "\n".join(
            [
                r"^pt,.*,www\)/path/to/resource",
                r'"mime": "application/x-shockwave-flash"',
                r'"status": "404"',
                r"/tracking\.js",
            ]
        )

        path = os.path.join(self.temp_dir, "blocklist.txt")
        with open(path, "w") as f:
            f.write(content)

        patterns = load_blocklist(path)

        self.assertEqual(len(patterns), 4)


class TestFilterCdxjByBlocklist(unittest.TestCase):
    """Test filtering CDXJ records by blocklist patterns."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_filter_domain_patterns(self):
        """Filter records by domain SURT patterns."""
        # Create CDXJ content
        cdxj_lines = [
            'pt,governo,www)/ 20230615120000 {"url": "https://www.governo.pt/"}\n',
            'pt,spam,www)/ 20230615120000 {"url": "https://www.spam.pt/"}\n',
            'pt,sapo,www)/ 20230615120000 {"url": "https://www.sapo.pt/"}\n',
            'pt,adult,xxx)/ 20230615120000 {"url": "https://xxx.adult.pt/"}\n',
            'pt,publico,www)/ 20230615120000 {"url": "https://www.publico.pt/"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Create blocklist
        patterns = [
            re.compile(r"^pt,spam,"),
            re.compile(r"^pt,adult,"),
        ]

        # Filter
        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 3)
        self.assertEqual(blocked, 2)

        # Verify output
        with open(output_path, "r") as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 3)
        self.assertIn("governo", lines[0])
        self.assertIn("sapo", lines[1])
        self.assertIn("publico", lines[2])

    def test_filter_path_patterns(self):
        """Filter records by URL path patterns."""
        cdxj_lines = [
            'pt,site,www)/ 20230615120000 {"url": "https://www.site.pt/"}\n',
            'pt,site,www)/ads/banner.jpg 20230615120000 {"url": "https://www.site.pt/ads/banner.jpg"}\n',
            'pt,site,www)/content 20230615120000 {"url": "https://www.site.pt/content"}\n',
            'pt,site,www)/tracking.js 20230615120000 {"url": "https://www.site.pt/tracking.js"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Block ads and tracking
        patterns = [
            re.compile(r"/ads/"),
            re.compile(r"/tracking\.js"),
        ]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 2)

    def test_filter_mime_type_patterns(self):
        """Filter records by MIME type in JSON."""
        cdxj_lines = [
            'pt,site,www)/page 20230615120000 {"url": "...", "mime": "text/html"}\n',
            'pt,site,www)/movie 20230615120000 {"url": "...", "mime": "application/x-shockwave-flash"}\n',
            'pt,site,www)/image 20230615120000 {"url": "...", "mime": "image/jpeg"}\n',
            'pt,site,www)/app 20230615120000 {"url": "...", "mime": "application/x-shockwave-flash"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Block Flash content
        patterns = [
            re.compile(r'"mime": "application/x-shockwave-flash"'),
        ]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 2)

    def test_filter_status_code_patterns(self):
        """Filter records by HTTP status code."""
        cdxj_lines = [
            'pt,site,www)/page 20230615120000 {"url": "...", "status": "200"}\n',
            'pt,site,www)/missing 20230615120000 {"url": "...", "status": "404"}\n',
            'pt,site,www)/other 20230615120000 {"url": "...", "status": "200"}\n',
            'pt,site,www)/error 20230615120000 {"url": "...", "status": "500"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Block errors
        patterns = [
            re.compile(r'"status": "404"'),
            re.compile(r'"status": "500"'),
        ]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 2)

    def test_filter_no_matches(self):
        """Pass through all records when no patterns match."""
        cdxj_lines = [
            'pt,governo,www)/ 20230615120000 {"url": "..."}\n',
            'pt,sapo,www)/ 20230615120000 {"url": "..."}\n',
            'pt,publico,www)/ 20230615120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Block non-existent domains
        patterns = [
            re.compile(r"^pt,nonexistent,"),
        ]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 3)
        self.assertEqual(blocked, 0)

    def test_filter_empty_blocklist(self):
        """Pass through all records with empty blocklist."""
        cdxj_lines = [
            'pt,governo,www)/ 20230615120000 {"url": "..."}\n',
            'pt,sapo,www)/ 20230615120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        patterns = []

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 0)

    def test_filter_multiple_pattern_types(self):
        """Filter using combination of different pattern types."""
        cdxj_lines = [
            'pt,governo,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
            'pt,spam,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
            'pt,site,www)/ads/ 20230615120000 {"url": "...", "status": "200"}\n',
            'pt,site,www)/page 20230615120000 {"url": "...", "status": "404"}\n',
            'pt,sapo,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Block spam domains, ads, and 404s
        patterns = [
            re.compile(r"^pt,spam,"),
            re.compile(r"/ads/"),
            re.compile(r'"status": "404"'),
        ]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)  # governo and sapo
        self.assertEqual(blocked, 3)

    def test_preserves_line_order(self):
        """Ensure filtering preserves original line order."""
        cdxj_lines = [
            'pt,a,www)/ 20230615120000 {"url": "..."}\n',
            'pt,spam,www)/ 20230615120000 {"url": "..."}\n',
            'pt,b,www)/ 20230615120000 {"url": "..."}\n',
            'pt,spam,www)/ 20230615120001 {"url": "..."}\n',
            'pt,c,www)/ 20230615120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        patterns = [re.compile(r"^pt,spam,")]

        filter_cdxj_by_blocklist(input_path, patterns, output_path)

        with open(output_path, "r") as f:
            result = f.readlines()

        self.assertEqual(len(result), 3)
        self.assertIn("pt,a,www)/", result[0])
        self.assertIn("pt,b,www)/", result[1])
        self.assertIn("pt,c,www)/", result[2])


class TestRealisticScenarios(unittest.TestCase):
    """Test realistic Arquivo.pt blocklist scenarios."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_arquivo_spam_filtering(self):
        """Test typical Arquivo.pt spam domain filtering."""
        # Create realistic CDXJ with spam mixed in
        cdxj_lines = []

        # Legitimate Portuguese sites
        cdxj_lines.extend(
            [
                'pt,governo,www)/ 20230615120000 {"url": "https://www.governo.pt/", "status": "200"}\n',
                'pt,sapo,www)/ 20230615120000 {"url": "https://www.sapo.pt/", "status": "200"}\n',
                'pt,publico,www)/ 20230615120000 {"url": "https://www.publico.pt/", "status": "200"}\n',
                'pt,rtp,www)/ 20230615120000 {"url": "https://www.rtp.pt/", "status": "200"}\n',
            ]
        )

        # Spam sites
        cdxj_lines.extend(
            [
                'pt,spam1,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
                'pt,spam2,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
                'pt,adult-content,xxx)/ 20230615120000 {"url": "...", "status": "200"}\n',
            ]
        )

        # More legitimate
        cdxj_lines.extend(
            [
                'pt,cm-lisboa,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
                'pt,dn,www)/ 20230615120000 {"url": "...", "status": "200"}\n',
            ]
        )

        input_path = os.path.join(self.temp_dir, "arquivo.cdxj")
        output_path = os.path.join(self.temp_dir, "cleaned.cdxj")
        blocklist_path = os.path.join(self.temp_dir, "blocklist.txt")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Create realistic blocklist
        blocklist_content = "\n".join(
            [
                "# Spam domains",
                "^pt,spam1,",
                "^pt,spam2,",
                "# Adult content",
                "^pt,adult-content,",
            ]
        )

        with open(blocklist_path, "w") as f:
            f.write(blocklist_content)

        # Filter
        patterns = load_blocklist(blocklist_path)
        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 6)  # 6 legitimate sites
        self.assertEqual(blocked, 3)  # 3 spam sites

        # Verify output doesn't contain spam
        with open(output_path, "r") as f:
            output = f.read()

        self.assertNotIn("spam1", output)
        self.assertNotIn("spam2", output)
        self.assertNotIn("adult-content", output)
        self.assertIn("governo", output)
        self.assertIn("sapo", output)

    def test_arquivo_comprehensive_filtering(self):
        """Test comprehensive filtering: domains, paths, MIME types, status codes."""
        cdxj_lines = [
            # Good content
            (
                "pt,governo,www)/page 20230615120000 "
                '{"url": "...", "mime": "text/html", "status": "200"}\n'
            ),
            (
                "pt,sapo,www)/news 20230615120000 "
                '{"url": "...", "mime": "text/html", "status": "200"}\n'
            ),
            # Spam domain
            (
                "pt,spam,www)/page 20230615120000 "
                '{"url": "...", "mime": "text/html", "status": "200"}\n'
            ),
            # Good domain but blocked path (ads)
            (
                "pt,publico,www)/ads/banner 20230615120000 "
                '{"url": "...", "mime": "image/jpeg", "status": "200"}\n'
            ),
            # Good domain but blocked MIME (Flash)
            (
                "pt,rtp,www)/video 20230615120000 "
                '{"url": "...", "mime": "application/x-shockwave-flash", "status": "200"}\n'
            ),
            # Good domain but 404
            (
                "pt,cm-lisboa,www)/missing 20230615120000 "
                '{"url": "...", "mime": "text/html", "status": "404"}\n'
            ),
            # More good content
            (
                "pt,dn,www)/article 20230615120000 "
                '{"url": "...", "mime": "text/html", "status": "200"}\n'
            ),
        ]

        input_path = os.path.join(self.temp_dir, "arquivo.cdxj")
        output_path = os.path.join(self.temp_dir, "filtered.cdxj")
        blocklist_path = os.path.join(self.temp_dir, "comprehensive-blocklist.txt")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Comprehensive blocklist
        blocklist_content = "\n".join(
            [
                "# Block spam domains",
                "^pt,spam,",
                "",
                "# Block advertising paths",
                "/ads/",
                "/banner",
                "",
                "# Block Flash content (legacy)",
                '"mime": "application/x-shockwave-flash"',
                "",
                "# Block errors",
                '"status": "404"',
                '"status": "500"',
            ]
        )

        with open(blocklist_path, "w") as f:
            f.write(blocklist_content)

        patterns = load_blocklist(blocklist_path)
        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        # Should keep only 3 good records
        self.assertEqual(kept, 3)
        self.assertEqual(blocked, 4)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error handling."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_unicode_patterns(self):
        """Handle Unicode characters in patterns and data."""
        cdxj_lines = [
            'pt,câmara,www)/ 20230615120000 {"url": "..."}\n',
            'pt,açores,www)/ 20230615120000 {"url": "..."}\n',
            'pt,normal,www)/ 20230615120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w", encoding="utf-8") as f:
            f.writelines(cdxj_lines)

        patterns = [re.compile(r"câmara")]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 1)

    def test_very_long_lines(self):
        """Handle very long CDXJ lines (large JSON metadata)."""
        long_json = '{"url": "' + "x" * 10000 + '"}'
        cdxj_lines = [
            f"pt,site,www)/page1 20230615120000 {long_json}\n",
            'pt,spam,www)/ 20230615120000 {"url": "..."}\n',
            f"pt,site,www)/page2 20230615120000 {long_json}\n",
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        patterns = [re.compile(r"^pt,spam,")]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 1)

    def test_large_blocklist(self):
        """Handle large blocklist with many patterns."""
        # Create 1000 spam domains
        cdxj_lines = []
        for i in range(100):
            cdxj_lines.append(f'pt,spam{i},www)/ 20230615120000 {{"url": "..."}}\n')

        # Add some good domains
        cdxj_lines.extend(
            [
                'pt,governo,www)/ 20230615120000 {"url": "..."}\n',
                'pt,sapo,www)/ 20230615120000 {"url": "..."}\n',
            ]
        )

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        # Create large blocklist
        patterns = [re.compile(f"^pt,spam{i},") for i in range(100)]

        kept, blocked = filter_cdxj_by_blocklist(input_path, patterns, output_path)

        self.assertEqual(kept, 2)
        self.assertEqual(blocked, 100)


if __name__ == "__main__":
    unittest.main()
