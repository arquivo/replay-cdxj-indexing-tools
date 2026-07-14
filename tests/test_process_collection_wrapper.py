#!/usr/bin/env python3
"""
Test Suite for process_collection_wrapper.py
=============================================

Tests the thin Python wrapper that locates and executes the
cdxj-index-collection.sh shell script via subprocess.run.

All filesystem and subprocess calls are mocked so no shell script or
external commands are required to run these tests.

Test Coverage
-------------
1. Script found and executable  — subprocess.run called with bash + correct args
2. Script not found             — error printed, exits with code 1
3. Script not executable        — error printed, exits with code 1
4. sys.argv forwarding          — extra CLI args are passed through unchanged
5. Exit-code propagation        — non-zero subprocess exit codes are preserved
6. KeyboardInterrupt            — caught and converted to exit code 130
7. Unexpected Exception         — caught and converted to exit code 1
"""

import unittest
from unittest.mock import MagicMock, patch

from replay_cdxj_indexing_tools.process_collection_wrapper import main


class TestMain(unittest.TestCase):
    """Test main() — script discovery, argument forwarding, and error handling."""

    # ------------------------------------------------------------------ #
    # Happy path
    # ------------------------------------------------------------------ #

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_exits_zero_on_success(self, _mock_exists, _mock_access, mock_run):
        """main() exits with 0 when subprocess.run reports returncode=0."""
        mock_run.return_value = MagicMock(returncode=0)

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 0)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_subprocess_invoked_with_bash(self, _mock_exists, _mock_access, mock_run):
        """main() executes the script using 'bash' as the interpreter."""
        mock_run.return_value = MagicMock(returncode=0)

        with self.assertRaises(SystemExit):
            main()

        call_args = mock_run.call_args[0][0]
        self.assertEqual(call_args[0], "bash")

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_subprocess_invoked_with_correct_script_name(
        self, _mock_exists, _mock_access, mock_run
    ):
        """main() passes the path to cdxj-index-collection.sh as the second argument."""
        mock_run.return_value = MagicMock(returncode=0)

        with self.assertRaises(SystemExit):
            main()

        call_args = mock_run.call_args[0][0]
        self.assertTrue(call_args[1].endswith("cdxj-index-collection.sh"))

    # ------------------------------------------------------------------ #
    # Error: script not found / not executable
    # ------------------------------------------------------------------ #

    @patch("os.path.exists", return_value=False)
    def test_script_not_found_exits_1(self, _mock_exists):
        """main() exits with code 1 when cdxj-index-collection.sh does not exist."""
        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 1)

    @patch("os.access", return_value=False)
    @patch("os.path.exists", return_value=True)
    def test_script_not_executable_exits_1(self, _mock_exists, _mock_access):
        """main() exits with code 1 when cdxj-index-collection.sh is not executable."""
        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 1)

    # ------------------------------------------------------------------ #
    # Argument forwarding
    # ------------------------------------------------------------------ #

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_extra_argv_forwarded_to_subprocess(self, _mock_exists, _mock_access, mock_run):
        """main() appends sys.argv[1:] to the subprocess command."""
        mock_run.return_value = MagicMock(returncode=0)

        with patch("sys.argv", ["cdxj-index-collection", "-d", "/data", "-k", "mykey"]):
            with self.assertRaises(SystemExit):
                main()

        call_args = mock_run.call_args[0][0]
        self.assertIn("-d", call_args)
        self.assertIn("/data", call_args)
        self.assertIn("-k", call_args)
        self.assertIn("mykey", call_args)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_no_extra_argv_only_bash_and_script(self, _mock_exists, _mock_access, mock_run):
        """main() passes only ['bash', script_path] when sys.argv has no extra args."""
        mock_run.return_value = MagicMock(returncode=0)

        with patch("sys.argv", ["cdxj-index-collection"]):
            with self.assertRaises(SystemExit):
                main()

        call_args = mock_run.call_args[0][0]
        self.assertEqual(len(call_args), 2)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_argv_order_preserved(self, _mock_exists, _mock_access, mock_run):
        """main() preserves the original order of sys.argv[1:] in the command."""
        mock_run.return_value = MagicMock(returncode=0)
        extra_args = ["--verbose", "--clear", "--host", "redis.example.com"]

        with patch("sys.argv", ["cdxj-index-collection"] + extra_args):
            with self.assertRaises(SystemExit):
                main()

        call_args = mock_run.call_args[0][0]
        self.assertEqual(call_args[2:], extra_args)

    # ------------------------------------------------------------------ #
    # Exit-code propagation
    # ------------------------------------------------------------------ #

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_nonzero_exit_code_propagated(self, _mock_exists, _mock_access, mock_run):
        """main() preserves a non-zero returncode from the subprocess."""
        mock_run.return_value = MagicMock(returncode=42)

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 42)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_exit_code_1_propagated(self, _mock_exists, _mock_access, mock_run):
        """main() propagates exit code 1 from the subprocess."""
        mock_run.return_value = MagicMock(returncode=1)

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 1)

    # ------------------------------------------------------------------ #
    # Error handling
    # ------------------------------------------------------------------ #

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_keyboard_interrupt_exits_130(self, _mock_exists, _mock_access, mock_run):
        """main() exits with code 130 when KeyboardInterrupt is raised."""
        mock_run.side_effect = KeyboardInterrupt

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 130)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_os_error_exits_1(self, _mock_exists, _mock_access, mock_run):
        """main() exits with code 1 when subprocess.run raises OSError."""
        mock_run.side_effect = OSError("Script execution failed")

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 1)

    @patch("subprocess.run")
    @patch("os.access", return_value=True)
    @patch("os.path.exists", return_value=True)
    def test_generic_exception_exits_1(self, _mock_exists, _mock_access, mock_run):
        """main() exits with code 1 when subprocess.run raises an unexpected exception."""
        mock_run.side_effect = RuntimeError("Unexpected error")

        with self.assertRaises(SystemExit) as ctx:
            main()

        self.assertEqual(ctx.exception.code, 1)


if __name__ == "__main__":
    unittest.main()
