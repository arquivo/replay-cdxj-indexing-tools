#!/usr/bin/env python3
"""
Test Suite for arclist_index_to_redis.py
=========================================

Tests the Python wrapper that pipelines arclist-to-path-index into
path-index-to-redis. All subprocess calls are mocked so no external
commands or Redis server are required.

Test Coverage
-------------
1. TestCheckDependencies  — dependency detection via subprocess.run
2. TestRunPipeline        — Popen pipeline construction, argument building,
                            exit-code propagation, timeout, interrupts
3. TestMain               — CLI argument parsing and run_pipeline delegation
"""

import subprocess
import unittest
from unittest.mock import MagicMock, patch

from replay_cdxj_indexing_tools.arclist_index_to_redis import (
    check_dependencies,
    main,
    run_pipeline,
)


class TestCheckDependencies(unittest.TestCase):
    """Test check_dependencies() — verifies required CLI tools are present."""

    @patch("subprocess.run")
    def test_all_dependencies_present(self, mock_run):
        """Returns True when both required commands are found."""
        mock_run.return_value = MagicMock(returncode=0)
        self.assertTrue(check_dependencies())

    @patch("subprocess.run")
    def test_arclist_command_missing(self, mock_run):
        """Returns False when arclist-to-path-index is not found."""

        def side_effect(cmd, **kwargs):
            if cmd[0] == "arclist-to-path-index":
                raise FileNotFoundError
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect
        self.assertFalse(check_dependencies())

    @patch("subprocess.run")
    def test_redis_command_missing(self, mock_run):
        """Returns False when path-index-to-redis is not found."""

        def side_effect(cmd, **kwargs):
            if cmd[0] == "path-index-to-redis":
                raise FileNotFoundError
            return MagicMock(returncode=0)

        mock_run.side_effect = side_effect
        self.assertFalse(check_dependencies())

    @patch("subprocess.run")
    def test_both_commands_missing(self, mock_run):
        """Returns False when both required commands are missing."""
        mock_run.side_effect = FileNotFoundError
        self.assertFalse(check_dependencies())


class TestRunPipeline(unittest.TestCase):
    """Test run_pipeline() — subprocess construction, arguments, exit codes."""

    def _make_proc(self, exitcode=0):
        """Return a mock Popen-compatible process with the given exit code."""
        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.wait.return_value = exitcode
        return proc

    # ------------------------------------------------------------------ #
    # Happy path
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_successful_pipeline_returns_zero(self, mock_popen):
        """Returns 0 when both subprocesses exit cleanly."""
        mock_popen.side_effect = [self._make_proc(0), self._make_proc(0)]
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 0)

    # ------------------------------------------------------------------ #
    # Command argument construction — arclist-to-path-index
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_arclist_cmd_uses_folder_arg(self, mock_popen):
        """arclist-to-path-index receives the -d flag with the folder path."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        arclist_cmd = mock_popen.call_args_list[0][0][0]
        self.assertEqual(arclist_cmd[0], "arclist-to-path-index")
        self.assertIn("-d", arclist_cmd)
        self.assertIn("/data/arclists", arclist_cmd)

    @patch("subprocess.Popen")
    def test_arclist_cmd_adds_verbose_flag(self, mock_popen):
        """arclist-to-path-index receives --verbose when verbose=True."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test", verbose=True)

        arclist_cmd = mock_popen.call_args_list[0][0][0]
        self.assertIn("--verbose", arclist_cmd)

    @patch("subprocess.Popen")
    def test_arclist_cmd_no_verbose_by_default(self, mock_popen):
        """arclist-to-path-index does not receive --verbose by default."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        arclist_cmd = mock_popen.call_args_list[0][0][0]
        self.assertNotIn("--verbose", arclist_cmd)

    # ------------------------------------------------------------------ #
    # Command argument construction — path-index-to-redis
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_redis_cmd_default_arguments(self, mock_popen):
        """path-index-to-redis receives the key, default host, port, and batch size."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertEqual(redis_cmd[0], "path-index-to-redis")
        self.assertIn("pathindex:test", redis_cmd)
        self.assertIn("localhost", redis_cmd)
        self.assertIn("6379", redis_cmd)
        self.assertIn("500", redis_cmd)  # default batch size

    @patch("subprocess.Popen")
    def test_redis_cmd_custom_host_and_port(self, mock_popen):
        """path-index-to-redis receives a custom host and port."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            redis_host="redis.example.com",
            redis_port=6380,
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("redis.example.com", redis_cmd)
        self.assertIn("6380", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_custom_batch_size(self, mock_popen):
        """path-index-to-redis receives a custom --batch-size value."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            batch_size=1000,
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("1000", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_password(self, mock_popen):
        """path-index-to-redis receives --password when redis_password is provided."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            redis_password="secret",
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--password", redis_cmd)
        self.assertIn("secret", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_username(self, mock_popen):
        """path-index-to-redis receives --username when redis_username is provided."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            redis_username="alice",
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--username", redis_cmd)
        self.assertIn("alice", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_socket(self, mock_popen):
        """path-index-to-redis receives --socket when redis_socket is provided."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            redis_socket="/var/run/redis/redis.sock",
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--socket", redis_cmd)
        self.assertIn("/var/run/redis/redis.sock", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_ssl_flag(self, mock_popen):
        """path-index-to-redis receives --ssl when use_ssl=True."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            use_ssl=True,
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--ssl", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_without_ssl_by_default(self, mock_popen):
        """path-index-to-redis does not receive --ssl by default."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertNotIn("--ssl", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_cluster_flag(self, mock_popen):
        """path-index-to-redis receives --cluster when use_cluster=True."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            use_cluster=True,
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--cluster", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_with_clear_flag(self, mock_popen):
        """path-index-to-redis receives --clear when clear_existing=True."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            clear_existing=True,
        )

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--clear", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_without_clear_by_default(self, mock_popen):
        """path-index-to-redis does not receive --clear by default."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertNotIn("--clear", redis_cmd)

    @patch("subprocess.Popen")
    def test_redis_cmd_verbose_flag(self, mock_popen):
        """Both commands receive --verbose when verbose=True."""
        mock_popen.side_effect = [self._make_proc(), self._make_proc()]
        run_pipeline(
            arclist_folder="/data/arclists",
            redis_key="pathindex:test",
            verbose=True,
        )

        arclist_cmd = mock_popen.call_args_list[0][0][0]
        redis_cmd = mock_popen.call_args_list[1][0][0]
        self.assertIn("--verbose", arclist_cmd)
        self.assertIn("--verbose", redis_cmd)

    # ------------------------------------------------------------------ #
    # Pipe connection between the two processes
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_arclist_stdout_connected_to_redis_stdin(self, mock_popen):
        """arclist-to-path-index stdout is piped into path-index-to-redis stdin."""
        arclist_proc = self._make_proc()
        redis_proc = self._make_proc()
        mock_popen.side_effect = [arclist_proc, redis_proc]

        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        arclist_kwargs = mock_popen.call_args_list[0][1]
        redis_kwargs = mock_popen.call_args_list[1][1]
        self.assertEqual(arclist_kwargs["stdout"], subprocess.PIPE)
        self.assertEqual(redis_kwargs["stdin"], arclist_proc.stdout)

    # ------------------------------------------------------------------ #
    # Exit-code propagation
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_arclist_nonzero_exit_code_returned(self, mock_popen):
        """Returns the arclist-to-path-index exit code when it fails."""
        mock_popen.side_effect = [self._make_proc(2), self._make_proc(0)]
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 2)

    @patch("subprocess.Popen")
    def test_redis_nonzero_exit_code_returned(self, mock_popen):
        """Returns the path-index-to-redis exit code when it fails."""
        mock_popen.side_effect = [self._make_proc(0), self._make_proc(3)]
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 3)

    # ------------------------------------------------------------------ #
    # Timeout handling
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_timeout_returns_124(self, mock_popen):
        """Returns 124 when the pipeline exceeds the timeout."""
        arclist_proc = self._make_proc(0)
        redis_proc = self._make_proc(0)
        # Only the first wait() raises; the second call (cleanup) must succeed.
        redis_proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="path-index-to-redis", timeout=3600),
            None,
        ]
        mock_popen.side_effect = [arclist_proc, redis_proc]

        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 124)

    @patch("subprocess.Popen")
    def test_timeout_kills_subprocesses(self, mock_popen):
        """Both subprocesses are killed when the pipeline times out."""
        arclist_proc = self._make_proc(0)
        redis_proc = self._make_proc(0)
        # Only the first wait() raises; the second call (cleanup) must succeed.
        redis_proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="path-index-to-redis", timeout=3600),
            None,
        ]
        mock_popen.side_effect = [arclist_proc, redis_proc]

        run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")

        redis_proc.kill.assert_called_once()
        arclist_proc.kill.assert_called_once()

    # ------------------------------------------------------------------ #
    # Interrupt and error handling
    # ------------------------------------------------------------------ #

    @patch("subprocess.Popen")
    def test_keyboard_interrupt_returns_130(self, mock_popen):
        """Returns 130 when interrupted by the user (SIGINT)."""
        mock_popen.side_effect = KeyboardInterrupt
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 130)

    @patch("subprocess.Popen")
    def test_os_error_returns_1(self, mock_popen):
        """Returns 1 when an unexpected OSError occurs during Popen."""
        mock_popen.side_effect = OSError("Connection failed")
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 1)

    @patch("subprocess.Popen")
    def test_generic_exception_returns_1(self, mock_popen):
        """Returns 1 when an unexpected exception occurs during Popen."""
        mock_popen.side_effect = RuntimeError("Unexpected error")
        result = run_pipeline(arclist_folder="/data/arclists", redis_key="pathindex:test")
        self.assertEqual(result, 1)


class TestMain(unittest.TestCase):
    """Test main() — CLI argument parsing and delegation to run_pipeline."""

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_basic_invocation_calls_run_pipeline(self, mock_check, mock_run):
        """main() calls run_pipeline with the correct folder and key."""
        mock_check.return_value = True
        mock_run.return_value = 0

        result = main(["-d", "/data/arclists", "-k", "pathindex:test"])

        self.assertEqual(result, 0)
        mock_run.assert_called_once()
        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["arclist_folder"], "/data/arclists")
        self.assertEqual(kwargs["redis_key"], "pathindex:test")

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_missing_dependencies_returns_1(self, mock_check):
        """main() returns 1 when check_dependencies() fails."""
        mock_check.return_value = False
        result = main(["-d", "/data/arclists", "-k", "pathindex:test"])
        self.assertEqual(result, 1)

    def test_missing_required_args_exits(self):
        """main() raises SystemExit when required arguments are omitted."""
        with self.assertRaises(SystemExit):
            main([])

    def test_missing_redis_key_exits(self):
        """main() raises SystemExit when -k/--redis-key is omitted."""
        with self.assertRaises(SystemExit):
            main(["-d", "/data/arclists"])

    def test_missing_folder_exits(self):
        """main() raises SystemExit when -d/--folder is omitted."""
        with self.assertRaises(SystemExit):
            main(["-k", "pathindex:test"])

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_clear_flag_passed_to_run_pipeline(self, mock_check, mock_run):
        """main() passes clear_existing=True when --clear is given."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test", "--clear"])

        kwargs = mock_run.call_args[1]
        self.assertTrue(kwargs["clear_existing"])

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_verbose_flag_passed_to_run_pipeline(self, mock_check, mock_run):
        """main() passes verbose=True when -v is given."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test", "-v"])

        kwargs = mock_run.call_args[1]
        self.assertTrue(kwargs["verbose"])

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_verbose_long_flag(self, mock_check, mock_run):
        """main() accepts --verbose as an alias for -v."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test", "--verbose"])

        kwargs = mock_run.call_args[1]
        self.assertTrue(kwargs["verbose"])

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_batch_size_passed_to_run_pipeline(self, mock_check, mock_run):
        """main() passes the custom batch_size to run_pipeline."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test", "--batch-size", "1000"])

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["batch_size"], 1000)

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_redis_auth_passed_to_run_pipeline(self, mock_check, mock_run):
        """main() passes password and username to run_pipeline."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(
            [
                "-d",
                "/data/arclists",
                "-k",
                "pathindex:test",
                "--password",
                "secret",
                "--username",
                "alice",
            ]
        )

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["redis_password"], "secret")
        self.assertEqual(kwargs["redis_username"], "alice")

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_ssl_and_cluster_flags_passed(self, mock_check, mock_run):
        """main() passes use_ssl=True and use_cluster=True for --ssl --cluster."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(
            [
                "-d",
                "/data/arclists",
                "-k",
                "pathindex:test",
                "--ssl",
                "--cluster",
            ]
        )

        kwargs = mock_run.call_args[1]
        self.assertTrue(kwargs["use_ssl"])
        self.assertTrue(kwargs["use_cluster"])

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_socket_option_passed(self, mock_check, mock_run):
        """main() passes redis_socket to run_pipeline when --socket is given."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(
            [
                "-d",
                "/data/arclists",
                "-k",
                "pathindex:test",
                "--socket",
                "/var/run/redis/redis.sock",
            ]
        )

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["redis_socket"], "/var/run/redis/redis.sock")

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_exit_code_propagated_from_run_pipeline(self, mock_check, mock_run):
        """main() returns the exit code produced by run_pipeline."""
        mock_check.return_value = True
        mock_run.return_value = 2

        result = main(["-d", "/data/arclists", "-k", "pathindex:test"])
        self.assertEqual(result, 2)

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_keyboard_interrupt_returns_130(self, mock_check, mock_run):
        """main() returns 130 when run_pipeline raises KeyboardInterrupt."""
        mock_check.return_value = True
        mock_run.side_effect = KeyboardInterrupt

        result = main(["-d", "/data/arclists", "-k", "pathindex:test"])
        self.assertEqual(result, 130)

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_unexpected_exception_returns_1(self, mock_check, mock_run):
        """main() returns 1 when run_pipeline raises an unexpected exception."""
        mock_check.return_value = True
        mock_run.side_effect = RuntimeError("Unexpected error")

        result = main(["-d", "/data/arclists", "-k", "pathindex:test"])
        self.assertEqual(result, 1)

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_default_redis_host_is_localhost(self, mock_check, mock_run):
        """main() uses localhost as the default Redis host."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test"])

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["redis_host"], "localhost")

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_default_redis_port_is_6379(self, mock_check, mock_run):
        """main() uses port 6379 as the default Redis port."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(["-d", "/data/arclists", "-k", "pathindex:test"])

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["redis_port"], 6379)

    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.run_pipeline")
    @patch("replay_cdxj_indexing_tools.arclist_index_to_redis.check_dependencies")
    def test_custom_host_and_port(self, mock_check, mock_run):
        """main() passes custom --host and --port to run_pipeline."""
        mock_check.return_value = True
        mock_run.return_value = 0

        main(
            [
                "-d",
                "/data/arclists",
                "-k",
                "pathindex:test",
                "--host",
                "redis.arquivo.pt",
                "--port",
                "6380",
            ]
        )

        kwargs = mock_run.call_args[1]
        self.assertEqual(kwargs["redis_host"], "redis.arquivo.pt")
        self.assertEqual(kwargs["redis_port"], 6380)


if __name__ == "__main__":
    unittest.main()
