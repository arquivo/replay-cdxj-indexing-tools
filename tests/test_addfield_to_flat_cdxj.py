#!/usr/bin/env python3
"""
Test suite for addfield-to-flat-cdxj functionality.

This module tests the ability to add custom JSON fields to CDXJ records.
Common use cases include adding collection metadata, timestamps, source information,
or computed fields to Arquivo.pt indexes.

Test Coverage:
--------------
1. Parsing and formatting CDXJ lines
2. Adding simple key-value fields
3. Custom field addition functions
4. Pipeline mode (stdin/stdout)
5. Edge cases and malformed input
6. Error handling and validation
7. CLI argument parsing

Real-World Data:
----------------
Tests use authentic Portuguese domain patterns and realistic field addition scenarios
from Arquivo.pt web archive processing.

"""

import json
import os
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

from replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj import (
    addfield_to_cdxj,
    format_cdxj_line,
    load_addfield_function,
    parse_cdxj_line,
)


class TestParseCdxjLine(unittest.TestCase):
    """Test CDXJ line parsing functionality."""

    def test_parse_simple_line(self):
        """Parse basic CDXJ line with JSON."""
        line = 'pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/", "status": "200"}'

        surt, timestamp, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "pt,arquivo)/")
        self.assertEqual(timestamp, "20231115120000")
        self.assertIsNotNone(json_data)
        self.assertEqual(json_data["url"], "https://arquivo.pt/")
        self.assertEqual(json_data["status"], "200")

    def test_parse_line_without_json(self):
        """Parse CDXJ line without JSON metadata."""
        line = "pt,arquivo)/ 20231115120000"

        surt, timestamp, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "pt,arquivo)/")
        self.assertEqual(timestamp, "20231115120000")
        self.assertIsNone(json_data)

    def test_parse_line_with_complex_json(self):
        """Parse CDXJ line with complex nested JSON."""
        line = (
            "pt,governo,www)/page 20230615120000 "
            '{"url": "...", "mime": "text/html", "status": "200", '
            '"headers": {"content-type": "text/html; charset=utf-8"}}'
        )

        surt, _, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "pt,governo,www)/page")
        self.assertIsNotNone(json_data)
        self.assertIn("headers", json_data)
        self.assertIn("content-type", json_data["headers"])

    def test_parse_line_with_special_characters(self):
        """Parse CDXJ line with Unicode and special characters in JSON."""
        line = (
            "pt,câmara,www)/ 20231115120000 "
            '{"url": "https://www.câmara.pt/", "title": "Câmara Municipal"}'
        )

        surt, _, _, json_data = parse_cdxj_line(line)

        self.assertEqual(surt, "pt,câmara,www)/")
        self.assertEqual(json_data["title"], "Câmara Municipal")

    def test_parse_invalid_line_missing_timestamp(self):
        """Reject line without timestamp."""
        line = "pt,arquivo)/"

        with self.assertRaises(ValueError) as context:
            parse_cdxj_line(line)

        self.assertIn("missing timestamp", str(context.exception))

    def test_parse_invalid_json(self):
        """Reject line with malformed JSON."""
        line = 'pt,arquivo)/ 20231115120000 {"url": invalid json}'

        with self.assertRaises(ValueError) as context:
            parse_cdxj_line(line)

        self.assertIn("Invalid JSON", str(context.exception))


class TestFormatCdxjLine(unittest.TestCase):
    """Test CDXJ line formatting functionality."""

    def test_format_line_with_json(self):
        """Format CDXJ line with JSON data."""
        surt = "pt,arquivo)/"
        timestamp = "20231115120000"
        json_data = {"url": "https://arquivo.pt/", "status": "200"}

        result = format_cdxj_line(surt, timestamp, json_data)

        self.assertTrue(result.startswith("pt,arquivo)/ 20231115120000"))
        self.assertTrue(result.endswith("\n"))
        self.assertIn('"url":', result)
        self.assertIn('"status":"200"', result)

    def test_format_line_without_json(self):
        """Format CDXJ line without JSON data."""
        surt = "pt,arquivo)/"
        timestamp = "20231115120000"

        result = format_cdxj_line(surt, timestamp, None)

        self.assertEqual(result, "pt,arquivo)/ 20231115120000\n")

    def test_format_line_with_unicode(self):
        """Format CDXJ line with Unicode characters."""
        surt = "pt,câmara,www)/"
        timestamp = "20231115120000"
        json_data = {"title": "Câmara Municipal", "location": "Lisboa"}

        result = format_cdxj_line(surt, timestamp, json_data)

        self.assertIn("Câmara", result)
        # Should not escape Unicode characters
        self.assertNotIn("\\u", result)

    def test_format_produces_compact_json(self):
        """Ensure JSON is formatted compactly without extra whitespace."""
        surt = "pt,arquivo)/"
        timestamp = "20231115120000"
        json_data = {"a": "1", "b": "2", "c": "3"}

        result = format_cdxj_line(surt, timestamp, json_data)

        # Should use compact separators
        self.assertIn('{"a":"1"', result)
        # Should not have spaces after colons or commas
        self.assertNotIn(": ", result)
        self.assertNotIn(", ", result)


class TestAddFieldToCdxj(unittest.TestCase):
    """Test the main field addition functionality."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_add_single_field(self):
        """Add a single field to all CDXJ records."""
        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/"}\n',
            'pt,sapo)/ 20231115120000 {"url": "https://sapo.pt/"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023"}

        processed, _ = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 2)

        # Verify output
        with open(output_path, "r") as f:
            lines = f.readlines()

        self.assertEqual(len(lines), 2)
        for line in lines:
            data = json.loads(line.split(" ", 2)[2])
            self.assertEqual(data["collection"], "ARQUIVO-2023")
            self.assertIn("url", data)  # Original field preserved

    def test_add_multiple_fields(self):
        """Add multiple fields to all CDXJ records."""
        cdxj_lines = [
            'pt,governo)/ 20231115120000 {"url": "...", "status": "200"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023", "source": "web", "indexed_date": "20231120"}

        processed, _ = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 1)

        # Verify all fields added
        with open(output_path, "r") as f:
            line = f.readline()

        data = json.loads(line.split(" ", 2)[2])
        self.assertEqual(data["collection"], "ARQUIVO-2023")
        self.assertEqual(data["source"], "web")
        self.assertEqual(data["indexed_date"], "20231120")
        self.assertEqual(data["status"], "200")  # Original preserved

    def test_add_field_to_line_without_json(self):
        """Add fields to CDXJ line that has no JSON metadata."""
        cdxj_lines = [
            "pt,arquivo)/ 20231115120000\n",
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023"}

        processed, _ = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 1)

        # Verify JSON was created
        with open(output_path, "r") as f:
            line = f.readline()

        parts = line.split(" ", 2)
        self.assertEqual(len(parts), 3)
        data = json.loads(parts[2])
        self.assertEqual(data["collection"], "ARQUIVO-2023")

    def test_preserves_existing_fields(self):
        """Ensure original fields are preserved when adding new ones."""
        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "...", "status": "200", "mime": "text/html"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023"}

        addfield_to_cdxj(input_path, output_path, fields=fields)

        with open(output_path, "r") as f:
            line = f.readline()

        data = json.loads(line.split(" ", 2)[2])
        self.assertEqual(data["url"], "...")
        self.assertEqual(data["status"], "200")
        self.assertEqual(data["mime"], "text/html")
        self.assertEqual(data["collection"], "ARQUIVO-2023")

    def test_skip_empty_lines(self):
        """Skip empty lines gracefully."""
        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "..."}\n',
            "\n",
            "\n",
            'pt,sapo)/ 20231115120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023"}

        processed, skipped = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 2)
        self.assertEqual(skipped, 2)  # 2 empty lines

    def test_handle_malformed_lines_gracefully(self):
        """Write malformed lines unchanged and continue processing."""
        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "good"}\n',
            "malformed line without timestamp\n",
            'pt,sapo)/ 20231115120000 {"url": "good"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {"collection": "ARQUIVO-2023"}

        processed, skipped = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 2)
        self.assertEqual(skipped, 1)

        # Verify malformed line preserved
        with open(output_path, "r") as f:
            lines = f.readlines()

        self.assertIn("malformed line", lines[1])

    def test_error_when_no_fields_or_function(self):
        """Raise error if neither fields nor function provided."""
        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write('pt,arquivo)/ 20231115120000 {"url": "..."}\n')

        with self.assertRaises(ValueError) as context:
            addfield_to_cdxj(input_path, output_path, fields=None, addfield_func=None)

        self.assertIn("Must provide", str(context.exception))

    def test_error_when_both_fields_and_function(self):
        """Raise error if both fields and function provided."""
        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.write('pt,arquivo)/ 20231115120000 {"url": "..."}\n')

        def dummy_func(_surt, _ts, data):
            return data

        with self.assertRaises(ValueError) as context:
            addfield_to_cdxj(input_path, output_path, fields={"x": "y"}, addfield_func=dummy_func)

        self.assertIn("Cannot provide both", str(context.exception))


class TestCustomFieldFunctions(unittest.TestCase):
    """Test custom field addition functions."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_custom_function_basic(self):
        """Use custom function to add fields dynamically."""

        def add_year(_surt_key, timestamp, json_data):
            """Extract year from timestamp."""
            year = timestamp[:4]
            json_data["year"] = year
            json_data["collection"] = f"ARQUIVO-{year}"
            return json_data

        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "..."}\n',
            'pt,sapo)/ 20241201120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        processed, _ = addfield_to_cdxj(input_path, output_path, addfield_func=add_year)

        self.assertEqual(processed, 2)

        # Verify dynamic fields
        with open(output_path, "r") as f:
            lines = f.readlines()

        data1 = json.loads(lines[0].split(" ", 2)[2])
        self.assertEqual(data1["year"], "2023")
        self.assertEqual(data1["collection"], "ARQUIVO-2023")

        data2 = json.loads(lines[1].split(" ", 2)[2])
        self.assertEqual(data2["year"], "2024")
        self.assertEqual(data2["collection"], "ARQUIVO-2024")

    def test_custom_function_with_surt_extraction(self):
        """Custom function using SURT key for domain extraction."""

        def add_domain_info(surt_key, _timestamp, json_data):
            """Extract domain from SURT key."""
            # SURT format: pt,domain,subdomain)/path
            parts = surt_key.split(")", 1)[0].split(",")
            if len(parts) >= 2:
                json_data["tld"] = parts[0]
                json_data["domain"] = parts[1]
            return json_data

        cdxj_lines = [
            'pt,arquivo,www)/ 20231115120000 {"url": "..."}\n',
            'pt,sapo,mail)/ 20231115120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        processed, _ = addfield_to_cdxj(input_path, output_path, addfield_func=add_domain_info)

        self.assertEqual(processed, 2)

        with open(output_path, "r") as f:
            lines = f.readlines()

        data1 = json.loads(lines[0].split(" ", 2)[2])
        self.assertEqual(data1["tld"], "pt")
        self.assertEqual(data1["domain"], "arquivo")

        data2 = json.loads(lines[1].split(" ", 2)[2])
        self.assertEqual(data2["domain"], "sapo")

    def test_load_addfield_function_from_file(self):
        """Load custom function from Python file."""
        # Create a Python file with addfield function
        func_content = '''
def addfield(surt_key, timestamp, json_data):
    """Add collection field based on year."""
    year = timestamp[:4]
    json_data["collection"] = f"COL-{year}"
    return json_data
'''

        func_path = os.path.join(self.temp_dir, "addfield_func.py")
        with open(func_path, "w") as f:
            f.write(func_content)

        # Load function
        func = load_addfield_function(func_path)

        # Test it
        result = func("pt,arquivo)/", "20231115120000", {"url": "..."})
        self.assertEqual(result["collection"], "COL-2023")

    def test_load_addfield_function_missing_file(self):
        """Raise error when function file doesn't exist."""
        nonexistent = os.path.join(self.temp_dir, "nonexistent.py")

        with self.assertRaises(IOError):
            load_addfield_function(nonexistent)

    def test_load_addfield_function_missing_function(self):
        """Raise error when file doesn't define addfield()."""
        func_content = """
def wrong_name(surt_key, timestamp, json_data):
    return json_data
"""

        func_path = os.path.join(self.temp_dir, "wrong.py")
        with open(func_path, "w") as f:
            f.write(func_content)

        with self.assertRaises(AttributeError) as context:
            load_addfield_function(func_path)

        self.assertIn("must define an addfield", str(context.exception))


class TestRealisticScenarios(unittest.TestCase):
    """Test realistic Arquivo.pt field addition scenarios."""

    def setUp(self):
        """Create temporary test files."""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_collection_metadata_enrichment(self):
        """Add collection metadata to all records."""
        cdxj_lines = [
            'pt,governo,www)/ 20231115120000 {"url": "...", "status": "200"}\n',
            'pt,sapo,www)/ 20231115130000 {"url": "...", "status": "200"}\n',
            'pt,publico,www)/ 20231115140000 {"url": "...", "status": "200"}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        fields = {
            "collection": "COLLECTION-2023-11",
            "source": "arquivo",
            "indexed_date": "20231120",
            "batch": "daily",
        }

        processed, _ = addfield_to_cdxj(input_path, output_path, fields=fields)

        self.assertEqual(processed, 3)

        # Verify all records enriched
        with open(output_path, "r") as f:
            for line in f:
                data = json.loads(line.split(" ", 2)[2])
                self.assertEqual(data["collection"], "COLLECTION-2023-11")
                self.assertEqual(data["source"], "arquivo")
                self.assertEqual(data["indexed_date"], "20231120")
                self.assertEqual(data["batch"], "daily")
                self.assertIn("status", data)  # Original preserved

    def test_dynamic_year_based_collection(self):
        """Add collection name based on capture timestamp year."""

        def add_year_collection(_surt_key, timestamp, json_data):
            year = timestamp[:4]
            month = timestamp[4:6]
            json_data["collection"] = f"ARQUIVO-{year}-{month}"
            json_data["year"] = year
            return json_data

        cdxj_lines = [
            'pt,arquivo)/ 20231115120000 {"url": "..."}\n',
            'pt,sapo)/ 20240301120000 {"url": "..."}\n',
            'pt,publico)/ 20221210120000 {"url": "..."}\n',
        ]

        input_path = os.path.join(self.temp_dir, "input.cdxj")
        output_path = os.path.join(self.temp_dir, "output.cdxj")

        with open(input_path, "w") as f:
            f.writelines(cdxj_lines)

        processed, _ = addfield_to_cdxj(input_path, output_path, addfield_func=add_year_collection)

        self.assertEqual(processed, 3)

        with open(output_path, "r") as f:
            lines = f.readlines()

        data1 = json.loads(lines[0].split(" ", 2)[2])
        self.assertEqual(data1["collection"], "ARQUIVO-2023-11")

        data2 = json.loads(lines[1].split(" ", 2)[2])
        self.assertEqual(data2["collection"], "ARQUIVO-2024-03")

        data3 = json.loads(lines[2].split(" ", 2)[2])
        self.assertEqual(data3["collection"], "ARQUIVO-2022-12")


class TestPipelineMode(unittest.TestCase):
    """Test stdin/stdout pipeline functionality."""

    def test_stdin_stdout_mode(self):
        """Process CDXJ from stdin to stdout."""
        input_data = (
            'pt,arquivo)/ 20231115120000 {"url": "..."}\npt,sapo)/ 20231115120000 {"url": "..."}\n'
        )

        fields = {"collection": "ARQUIVO-2023"}

        with patch("sys.stdin", StringIO(input_data)):
            with patch("sys.stdout", new=StringIO()) as mock_stdout:
                processed, _ = addfield_to_cdxj("-", "-", fields=fields)

                output = mock_stdout.getvalue()

        self.assertEqual(processed, 2)
        self.assertIn("collection", output)
        self.assertIn("ARQUIVO-2023", output)

    def test_preserves_line_order(self):
        """Ensure processing preserves original line order."""
        temp_dir = tempfile.mkdtemp()
        try:
            cdxj_lines = [
                'pt,a,www)/ 20231115120000 {"url": "..."}\n',
                'pt,b,www)/ 20231115120000 {"url": "..."}\n',
                'pt,c,www)/ 20231115120000 {"url": "..."}\n',
                'pt,d,www)/ 20231115120000 {"url": "..."}\n',
                'pt,e,www)/ 20231115120000 {"url": "..."}\n',
            ]

            input_path = os.path.join(temp_dir, "input.cdxj")
            output_path = os.path.join(temp_dir, "output.cdxj")

            with open(input_path, "w") as f:
                f.writelines(cdxj_lines)

            fields = {"batch": "test"}

            addfield_to_cdxj(input_path, output_path, fields=fields)

            with open(output_path, "r") as f:
                result = f.readlines()

            self.assertEqual(len(result), 5)
            self.assertIn("pt,a,www)/", result[0])
            self.assertIn("pt,b,www)/", result[1])
            self.assertIn("pt,c,www)/", result[2])
            self.assertIn("pt,d,www)/", result[3])
            self.assertIn("pt,e,www)/", result[4])

        finally:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
