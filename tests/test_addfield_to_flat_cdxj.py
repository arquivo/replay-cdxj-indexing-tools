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

import errno
import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import threading
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

    def _plugin_env(self):
        """Create a private plugin dir and point ADDFIELD_PLUGIN_DIR at it."""
        plugin_dir = os.path.join(self.temp_dir, "plugins")
        os.makedirs(plugin_dir)
        os.chmod(plugin_dir, 0o700)
        return plugin_dir, patch.dict(os.environ, {"ADDFIELD_PLUGIN_DIR": plugin_dir})

    @unittest.skipUnless(os.name == "posix", "--function is POSIX-only")
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

        plugin_dir, env = self._plugin_env()
        func_path = os.path.join(plugin_dir, "addfield_func.py")
        with open(func_path, "w") as f:
            f.write(func_content)
        os.chmod(func_path, 0o644)

        # Load function
        with env:
            func = load_addfield_function(func_path)

        # Test it
        result = func("pt,arquivo)/", "20231115120000", {"url": "..."})
        self.assertEqual(result["collection"], "COL-2023")

    @unittest.skipUnless(os.name == "posix", "--function is POSIX-only")
    def test_load_addfield_function_missing_file(self):
        """Raise error when function file doesn't exist."""
        plugin_dir, env = self._plugin_env()
        nonexistent = os.path.join(plugin_dir, "nonexistent.py")

        with env, self.assertRaises(IOError):
            load_addfield_function(nonexistent)

    @unittest.skipUnless(os.name == "posix", "--function is POSIX-only")
    def test_load_addfield_function_missing_function(self):
        """Raise error when file doesn't define addfield()."""
        func_content = """
def wrong_name(surt_key, timestamp, json_data):
    return json_data
"""

        plugin_dir, env = self._plugin_env()
        func_path = os.path.join(plugin_dir, "wrong.py")
        with open(func_path, "w") as f:
            f.write(func_content)
        os.chmod(func_path, 0o644)

        with env, self.assertRaises(AttributeError) as context:
            load_addfield_function(func_path)

        self.assertIn("must define an addfield", str(context.exception))


@unittest.skipUnless(os.name == "posix", "--function is POSIX-only")
class TestLoadAddfieldFunctionSecurity(unittest.TestCase):
    """Pin the fail-closed trust boundary of load_addfield_function (CWE-94)."""

    LOGGER = "replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj"

    def setUp(self):
        """Create a private plugin dir and a marker path that proves execution."""
        self.temp_dir = tempfile.mkdtemp()
        self.plugin_dir = os.path.join(self.temp_dir, "plugins")
        os.makedirs(self.plugin_dir)
        os.chmod(self.plugin_dir, 0o700)
        self.marker = os.path.join(self.temp_dir, "executed")
        self.env = patch.dict(os.environ, {"ADDFIELD_PLUGIN_DIR": self.plugin_dir})

    def tearDown(self):
        """Clean up temporary files."""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _write_plugin(self, path, mode=0o644):
        """Write a plugin whose module-level code creates self.marker."""
        with open(path, "w") as f:
            f.write(
                f"open({self.marker!r}, 'w').close()\n"
                "def addfield(surt_key, timestamp, json_data):\n"
                "    json_data['ok'] = True\n"
                "    return json_data\n"
            )
        os.chmod(path, mode)
        return path

    def assert_not_executed(self):
        """The plugin's module-level code must not have run."""
        self.assertFalse(os.path.exists(self.marker), "plugin code was executed")

    def test_refuses_when_plugin_dir_unset(self):
        """Fail closed: without ADDFIELD_PLUGIN_DIR nothing is loaded or executed."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        unset = {k: v for k, v in os.environ.items() if k != "ADDFIELD_PLUGIN_DIR"}
        for env in (unset, {**unset, "ADDFIELD_PLUGIN_DIR": ""}):
            with self.subTest(env=env.get("ADDFIELD_PLUGIN_DIR")):
                with patch.dict(os.environ, env, clear=True):
                    with self.assertLogs(self.LOGGER, level="WARNING") as log_context:
                        with self.assertRaises(PermissionError) as context:
                            load_addfield_function(func_path)
                self.assertIn("ADDFIELD_PLUGIN_DIR is not set", str(context.exception))
                self.assertTrue(any("Refused --function file" in m for m in log_context.output))
                self.assert_not_executed()

    def test_refuses_group_or_world_writable_plugin_dir(self):
        """A plugin dir others can write to is refused (CWE-732)."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        for mode in (0o770, 0o707, 0o1777):
            with self.subTest(mode=oct(mode)):
                os.chmod(self.plugin_dir, mode)
                with self.env, self.assertRaises(PermissionError) as context:
                    load_addfield_function(func_path)
                self.assertIn("plugin directory", str(context.exception))
                self.assertIn("group- or world-writable", str(context.exception))
                self.assert_not_executed()

    def test_writable_dir_hint_never_suggests_chmod_on_shared_dir(self):
        """A sticky shared dir (like /tmp) gets 'use a private dir', not 'chmod go-w'."""
        plugin_dir = os.path.join(self.temp_dir, "my plugins")
        os.makedirs(plugin_dir)
        func_path = self._write_plugin(os.path.join(plugin_dir, "f.py"))

        for mode, expected, unexpected in (
            (0o1777, 'mkdir -m 700 "$HOME/addfield-plugins"', "chmod"),
            (0o2770, 'mkdir -m 700 "$HOME/addfield-plugins"', "chmod"),
            (0o770, f"chmod go-w {shlex.quote(plugin_dir)}", "mkdir"),
        ):
            with self.subTest(mode=oct(mode)):
                os.chmod(plugin_dir, mode)
                if stat.S_IMODE(os.stat(plugin_dir).st_mode) != mode:
                    self.skipTest("setgid bit not settable here (not in the dir's group)")
                with patch.dict(os.environ, {"ADDFIELD_PLUGIN_DIR": plugin_dir}):
                    with self.assertRaises(PermissionError) as context:
                        load_addfield_function(func_path)
                self.assertIn(expected, str(context.exception))
                self.assertNotIn(unexpected, str(context.exception))

    @unittest.skipIf(os.name == "posix" and os.geteuid() == 0, "root owns the dir already")
    def test_writable_dir_owned_by_root_gets_private_dir_hint(self):
        """A non-sticky writable dir we don't own can't be chmod-ed: suggest a private dir."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))
        os.chmod(self.plugin_dir, 0o770)
        real_fstat = os.fstat

        def fstat_root_owned_dirs(fd):
            st = real_fstat(fd)
            if stat.S_ISDIR(st.st_mode):
                fields = list(st)
                fields[stat.ST_UID] = 0
                return os.stat_result(fields)
            return st

        with self.env, patch("os.fstat", side_effect=fstat_root_owned_dirs):
            with self.assertRaises(PermissionError) as context:
                load_addfield_function(func_path)

        self.assertIn("group- or world-writable", str(context.exception))
        self.assertIn('mkdir -m 700 "$HOME/addfield-plugins"', str(context.exception))
        self.assertNotIn("chmod", str(context.exception))
        self.assert_not_executed()

    def test_refuses_filesystem_root_as_plugin_dir(self):
        """'/' (e.g. HOME='' with ADDFIELD_PLUGIN_DIR='~') is never a plugin dir."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        for env in ({"HOME": "", "ADDFIELD_PLUGIN_DIR": "~"}, {"ADDFIELD_PLUGIN_DIR": "/"}):
            with self.subTest(env=env):
                with patch.dict(os.environ, env):
                    with self.assertLogs(self.LOGGER, level="WARNING") as log_context:
                        with self.assertRaises(PermissionError) as context:
                            load_addfield_function(func_path)
                self.assertIn("resolves to the filesystem root", str(context.exception))
                self.assertTrue(any("Refused --function file" in m for m in log_context.output))
                self.assert_not_executed()

    def test_refuses_group_or_world_writable_plugin_file(self):
        """A plugin file others can write to is refused (CWE-732)."""
        func_path = os.path.join(self.plugin_dir, "f.py")

        for mode in (0o664, 0o646):
            with self.subTest(mode=oct(mode)):
                self._write_plugin(func_path, mode=mode)
                with self.env, self.assertRaises(PermissionError) as context:
                    load_addfield_function(func_path)
                self.assertIn("function file", str(context.exception))
                self.assert_not_executed()

    @unittest.skipIf(os.name == "posix" and os.geteuid() == 0, "root may own anything")
    def test_refuses_plugin_owned_by_another_user(self):
        """Files not owned by the current user or root are refused."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        with self.env, patch("os.geteuid", return_value=os.geteuid() + 1):
            with self.assertRaises(PermissionError) as context:
                load_addfield_function(func_path)

        self.assertIn("is owned by uid", str(context.exception))
        self.assert_not_executed()

    def test_refuses_function_file_owned_by_another_user(self):
        """A foreign-owned file is refused even when the plugin dir passes."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))
        real_fstat = os.fstat

        def fstat_foreign_files(fd):
            st = real_fstat(fd)
            if stat.S_ISREG(st.st_mode):
                fields = list(st)
                fields[stat.ST_UID] = os.geteuid() + 1
                return os.stat_result(fields)
            return st

        with self.env, patch("os.fstat", side_effect=fstat_foreign_files):
            with self.assertRaises(PermissionError) as context:
                load_addfield_function(func_path)

        self.assertIn("function file", str(context.exception))
        self.assertIn("is owned by uid", str(context.exception))
        self.assert_not_executed()

    def test_refuses_symlink_escaping_plugin_dir(self):
        """A symlink inside the plugin dir pointing outside it is refused."""
        outside = self._write_plugin(os.path.join(self.temp_dir, "outside.py"))
        link = os.path.join(self.plugin_dir, "link.py")
        os.symlink(outside, link)

        with self.env, self.assertLogs(self.LOGGER, level="WARNING") as log_context:
            with self.assertRaises(ValueError) as context:
                load_addfield_function(link)

        self.assertIn("outside the allowed plugin directory", str(context.exception))
        self.assertTrue(any("Refused --function file" in msg for msg in log_context.output))
        self.assert_not_executed()

    def test_refuses_path_outside_or_nested_in_plugin_dir(self):
        """Only files directly inside ADDFIELD_PLUGIN_DIR are accepted."""
        nested_dir = os.path.join(self.plugin_dir, "sub")
        os.makedirs(nested_dir)
        os.chmod(nested_dir, 0o700)

        for func_path in (
            self._write_plugin(os.path.join(self.temp_dir, "outside.py")),
            self._write_plugin(os.path.join(nested_dir, "nested.py")),
        ):
            with self.subTest(path=func_path), self.env, self.assertRaises(ValueError):
                load_addfield_function(func_path)
            self.assert_not_executed()

    def test_refuses_symlink_swapped_in_after_path_check(self):
        """The file is opened with O_NOFOLLOW, so a post-check symlink swap fails (CWE-367)."""
        outside = self._write_plugin(os.path.join(self.temp_dir, "outside.py"))
        link = os.path.join(self.plugin_dir, "f.py")
        os.symlink(outside, link)

        # Simulate the race: the path and pre-open checks saw a regular file inside
        # the plugin dir, but by the time the file is opened it has become a symlink.
        regular_stat = os.stat(outside)
        with (
            self.env,
            patch("os.path.realpath", side_effect=os.path.abspath),
            patch("os.stat", return_value=regular_stat),
        ):
            with self.assertRaises(OSError) as context:
                load_addfield_function(link)

        self.assertEqual(context.exception.__cause__.errno, errno.ELOOP)
        self.assert_not_executed()

    def _load_in_thread(self, func_path):
        """Run the loader with a bounded wall time; return the exception raised."""
        outcome = {}

        def target():
            try:
                with self.env:
                    load_addfield_function(func_path)
            except (OSError, ValueError) as e:
                outcome["error"] = e

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive(), "loader blocked")
        return outcome.get("error")

    def test_refuses_fifo_without_blocking(self):
        """A FIFO (or device node) in the plugin dir is refused without being opened."""
        fifo = os.path.join(self.plugin_dir, "f.py")
        os.mkfifo(fifo, 0o644)

        with patch("os.open", wraps=os.open) as os_open:
            error = self._load_in_thread(fifo)

        self.assertIsInstance(error, OSError)
        self.assertIn("not a regular file", str(error))
        self.assertNotIn("f.py", [c.args[0] for c in os_open.call_args_list])

    def test_fifo_swapped_in_after_pre_check_does_not_block(self):
        """If a FIFO replaces the file after the pre-check, O_NONBLOCK + fstat refuse it."""
        fifo = os.path.join(self.plugin_dir, "f.py")
        os.mkfifo(fifo, 0o644)
        regular_stat = os.stat(self._write_plugin(os.path.join(self.temp_dir, "x.py")))

        with patch("os.stat", return_value=regular_stat):
            error = self._load_in_thread(fifo)

        self.assertIsInstance(error, OSError)
        self.assertIn("not a regular file", str(error))
        self.assert_not_executed()

    def test_refuses_non_regular_file(self):
        """A directory named like a plugin is refused before anything runs."""
        func_path = os.path.join(self.plugin_dir, "dir.py")
        os.makedirs(func_path)
        os.chmod(func_path, 0o700)

        with self.env, self.assertRaises(IOError) as context:
            load_addfield_function(func_path)

        self.assertIn("not a regular file", str(context.exception))

    def test_missing_plugin_dir_names_env_var(self):
        """A missing ADDFIELD_PLUGIN_DIR is reported as such, not as a bare errno."""
        missing = os.path.join(self.temp_dir, "missing")

        with patch.dict(os.environ, {"ADDFIELD_PLUGIN_DIR": missing}):
            with self.assertRaises(OSError) as context:
                load_addfield_function(os.path.join(missing, "f.py"))

        self.assertIn("Cannot open ADDFIELD_PLUGIN_DIR directory", str(context.exception))

    def test_plugin_dir_tilde_is_expanded(self):
        """'~' in ADDFIELD_PLUGIN_DIR is expanded (Docker ENV/systemd don't do it)."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        with patch.dict(os.environ, {"HOME": self.temp_dir, "ADDFIELD_PLUGIN_DIR": "~/plugins"}):
            func = load_addfield_function(func_path)

        self.assertTrue(func("pt,arquivo)/", "20231115120000", {})["ok"])

    def test_error_messages_do_not_embed_raw_newlines(self):
        """Paths are repr()-quoted so a crafted name cannot forge stderr lines (CWE-117)."""
        func_path = os.path.join(self.plugin_dir, "x.py\nINFO: forged")

        with self.env, self.assertRaises(OSError) as context:
            load_addfield_function(func_path)

        self.assertIn("Cannot load module from", str(context.exception))
        self.assertNotIn("\n", str(context.exception))

    def test_writable_hint_does_not_embed_raw_newlines(self):
        """The chmod hint is not shlex-quoted for unprintable paths (CWE-117)."""
        crafted_dir = os.path.join(self.temp_dir, "p\nINFO: forged")
        os.makedirs(crafted_dir)
        os.chmod(crafted_dir, 0o770)
        crafted_file = os.path.join(self.plugin_dir, "f.py\nINFO: forged")

        for plugin_dir, func_path, kind in (
            (crafted_dir, self._write_plugin(os.path.join(crafted_dir, "f.py")), "directory"),
            (self.plugin_dir, self._write_plugin(crafted_file, mode=0o664), "function file"),
        ):
            with self.subTest(kind=kind):
                with patch.dict(os.environ, {"ADDFIELD_PLUGIN_DIR": plugin_dir}):
                    with self.assertRaises(PermissionError) as context:
                        load_addfield_function(func_path)
                self.assertIn(kind, str(context.exception))
                self.assertIn("(chmod go-w) from the path above", str(context.exception))
                self.assertNotIn("\n", str(context.exception))
                self.assert_not_executed()

    def test_runs_vetted_bytes_without_reopening_path(self):
        """The code that runs is the bytes read from the vetted fd, not the path (CWE-367)."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))
        vetted = (
            b"MARK = __file__\ndef addfield(surt_key, timestamp, json_data):\n    return 'vetted'\n"
        )
        module = "replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj"
        before = set(sys.modules)

        with (
            self.env,
            patch(
                f"{module}._read_trusted_plugin", return_value=(os.path.realpath(func_path), vetted)
            ),
        ):
            func = load_addfield_function(func_path)

        self.assertEqual(func("pt,arquivo)/", "20231115120000", {}), "vetted")
        self.assertEqual(func.__globals__["MARK"], os.path.realpath(func_path))
        self.assertEqual(set(sys.modules) - before, set())
        self.assert_not_executed()

    def test_loads_trusted_plugin_and_logs_audit_warning(self):
        """Happy path: a private plugin loads, runs, and the load is audit-logged."""
        func_path = self._write_plugin(os.path.join(self.plugin_dir, "f.py"))

        with self.env, self.assertLogs(self.LOGGER, level="WARNING") as log_context:
            func = load_addfield_function(func_path)

        self.assertTrue(os.path.exists(self.marker))
        self.assertTrue(func("pt,arquivo)/", "20231115120000", {})["ok"])
        self.assertTrue(any("Executing arbitrary Python code" in msg for msg in log_context.output))
        resolved_repr = repr(os.path.realpath(func_path))
        self.assertTrue(any(resolved_repr in msg for msg in log_context.output))

    def _run_cli(self, env_overrides, source=None, name="f.py", extra_args=()):
        """Run the CLI in a fresh interpreter (no logging configured by the caller)."""
        input_path = os.path.join(self.temp_dir, "input.cdxj")
        with open(input_path, "w") as f:
            f.write('pt,arquivo)/ 20231115120000 {"url": "https://arquivo.pt/"}\n')
        output_path = os.path.join(self.temp_dir, "output.cdxj")
        func_path = self._write_plugin(os.path.join(self.plugin_dir, name))
        if source is not None:
            with open(func_path, "wb") as f:
                f.write(source.encode() if isinstance(source, str) else source)

        env = {k: v for k, v in os.environ.items() if k != "ADDFIELD_PLUGIN_DIR"}
        env.update(env_overrides)
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "replay_cdxj_indexing_tools.addfield.addfield_to_flat_cdxj",
                "-i",
                input_path,
                "-o",
                output_path,
                "--function",
                func_path,
                *extra_args,
            ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        return result, output_path

    def test_cli_refuses_function_without_plugin_dir(self):
        """CLI exits non-zero with an actionable message and runs nothing."""
        result, output_path = self._run_cli({})

        self.assertEqual(result.returncode, 1)
        self.assertIn("ADDFIELD_PLUGIN_DIR is not set", result.stderr)
        self.assertIn("export ADDFIELD_PLUGIN_DIR=", result.stderr)
        self.assertFalse(os.path.exists(output_path))
        self.assert_not_executed()

    def test_cli_emits_audit_warning_on_stderr(self):
        """With a trusted plugin dir the CLI works and the audit log reaches stderr."""
        result, output_path = self._run_cli({"ADDFIELD_PLUGIN_DIR": self.plugin_dir})

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("WARNING", result.stderr)
        self.assertIn("Executing arbitrary Python code", result.stderr)
        with open(output_path) as f:
            self.assertEqual(json.loads(f.read().split(" ", 2)[2])["ok"], True)

    def test_cli_reports_plugin_syntax_error_without_traceback(self):
        """A SyntaxError in the plugin is a clean 'Error:' line, not a traceback."""
        result, _ = self._run_cli({"ADDFIELD_PLUGIN_DIR": self.plugin_dir}, source="def (:\n")

        self.assertEqual(result.returncode, 1)
        self.assertRegex(result.stderr, r"(?m)^Error: .*line 1")
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_reports_nul_byte_plugin_without_bogus_location(self):
        """A plugin with NUL bytes gives a clean error, never '(None, line None)'."""
        result, _ = self._run_cli({"ADDFIELD_PLUGIN_DIR": self.plugin_dir}, source=b"x = 1\0\n")

        self.assertEqual(result.returncode, 1)
        # SyntaxError (Python >= 3.11) or ValueError (3.10), both without a location.
        self.assertRegex(
            result.stderr, r"(?m)^Error: source code string cannot contain null bytes$"
        )
        self.assertNotIn("None", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_syntax_error_does_not_embed_raw_newlines(self):
        """A crafted plugin file name cannot forge stderr lines via SyntaxError (CWE-117)."""
        result, _ = self._run_cli(
            {"ADDFIELD_PLUGIN_DIR": self.plugin_dir},
            source="def (:\n",
            name="f.py\nINFO: forged",
            extra_args=("-v",),
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("Loading field addition function from '", result.stderr)
        self.assertIn("f.py\\nINFO: forged'...", result.stderr)
        self.assertRegex(result.stderr, r"(?m)^Error: .*'.*f\.py\\nINFO: forged', line 1\)$")
        self.assertNotRegex(result.stderr, r"(?m)^INFO: forged")
        self.assertNotIn("Traceback", result.stderr)


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
