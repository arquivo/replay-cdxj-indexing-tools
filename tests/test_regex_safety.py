#!/usr/bin/env python3
"""
Tests for the shared ReDoS safeguards (replay_cdxj_indexing_tools.regex_safety)
and their use by the search filters and the blocklist filter (#74, #75).
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from replay_cdxj_indexing_tools import regex_safety
from replay_cdxj_indexing_tools.filter import blocklist
from replay_cdxj_indexing_tools.filter.blocklist import filter_cdxj_by_blocklist, load_blocklist
from replay_cdxj_indexing_tools.regex_safety import (
    RegexTimeoutError,
    check_regex_shape,
    compile_safe_regex,
    regex_time_limit,
    search_with_time_limit,
)
from replay_cdxj_indexing_tools.search import cdxj_search
from replay_cdxj_indexing_tools.search.filters import CDXJFilter

# Shapes that bypassed the original denylist but still backtrack exponentially
BYPASS_SHAPES = [
    r"(a+){2,}$",
    r"(a{1,10}){1,10}$",
    r"(a{1,30}){1,30}b",
]

OTHER_REDOS_SHAPES = [
    r"(.*a+)+b",  # issue #74
    r"(a+)+",
    r"(a*)*",
    r"(a+)*",
    r"(a|b)*",
    r"(a|ab){2,}",
    r"((a+))+",
    r"(?:a+)+",
    r"(a+?)+",
    r"(a?){30}",
    r"(\w+\.?)+$",
    r"(a+){3}",
    r"(?x)(a+ )+$",  # verbose mode hides structure
    r"(a+(?#c))+$",  # comment group hides structure
]

# Legitimate filter/blocklist patterns from the docs and tests
LEGIT_PATTERNS = [
    r"^pt,spam,",
    r"^pt,adult,",
    r"https://www.spam.pt/",
    r"http://.*\.adult.pt/",
    r'\.pdf"',
    r"/ads/",
    r'"mime": "application/x-shockwave-flash"',
    r"(.*://)?(www.)?example.pt/files/2/documentos/123456789.pdf(\S)*",
    r"^pt,.*,www\)/path/to/resource",
    r"/tracking\.js",
    r"text/.*",
    r"image/.*",
    r"3..",
    r"[0-9]{7,}",
    r"^(www\.)?([a-z0-9-]+\.)+pt/",
    r"\d{4}(\d{2}){2}",
    r"(https?://)?",
    r"^pt,(sapo|publico),",
]

# Exponential in CPython re: an unguarded search takes about a minute
SLOW_PATTERN = r"(a+)+$"
SLOW_TEXT = "a" * 30 + "!"


def _disable_denylist(test):
    """Let ReDoS-shaped patterns through so the time limit can be exercised."""
    patcher = mock.patch.object(regex_safety, "_REDOS_PATTERNS", [])
    patcher.start()
    test.addCleanup(patcher.stop)
    # Don't leak compiled "unsafe" patterns from the cache into other tests
    test.addCleanup(compile_safe_regex.cache_clear)


class TestDenylist(unittest.TestCase):
    """Static ReDoS shape checks."""

    def test_rejects_bounded_quantifier_bypass_shapes(self):
        for pattern in BYPASS_SHAPES + OTHER_REDOS_SHAPES:
            with self.subTest(pattern=pattern):
                with self.assertRaises(ValueError):
                    check_regex_shape(pattern)
                with self.assertRaises(ValueError):
                    compile_safe_regex(pattern)

    def test_accepts_legitimate_patterns(self):
        for pattern in LEGIT_PATTERNS:
            with self.subTest(pattern=pattern):
                check_regex_shape(pattern)
                self.assertIsInstance(compile_safe_regex(pattern), re.Pattern)

    def test_shape_check_ignores_syntax(self):
        """grep -E accepts the unmatched ")" of a SURT prefix; Python re doesn't."""
        check_regex_shape("^pt,site,www)/admin/")
        with self.assertRaises(ValueError):
            compile_safe_regex("^pt,site,www)/admin/")

    def test_rejects_too_long(self):
        with self.assertRaises(ValueError):
            check_regex_shape("a" * (regex_safety.MAX_PATTERN_LEN + 1))

    def test_cdxj_filter_rejects_bypass_shapes(self):
        for pattern in BYPASS_SHAPES:
            for op in ("~", "!~"):
                with self.subTest(pattern=pattern, op=op):
                    with self.assertRaises(ValueError):
                        CDXJFilter(filters=[f"url{op}{pattern}"])


class TestTimeLimit(unittest.TestCase):
    """Execution-time backstop around re.search()."""

    def test_timeout_fires_on_pathological_pattern(self):
        start = time.monotonic()
        with self.assertRaises(RegexTimeoutError):
            search_with_time_limit(re.compile(SLOW_PATTERN), SLOW_TEXT, timeout=0.2)
        self.assertLess(time.monotonic() - start, 5)

    def test_timeout_fires_inside_scope(self):
        start = time.monotonic()
        with regex_time_limit() as active:
            self.assertTrue(active)
            with self.assertRaises(RegexTimeoutError):
                search_with_time_limit(re.compile(SLOW_PATTERN), SLOW_TEXT, timeout=0.2)
            # The watchdog keeps working after a timeout
            self.assertIsNotNone(search_with_time_limit(re.compile("a+"), "aaa", timeout=0.2))
        self.assertLess(time.monotonic() - start, 5)

    def test_cdxj_filter_timeout_aborts(self):
        """A pattern that gets past the denylist is still bounded in matches()."""
        _disable_denylist(self)
        line = f'pt,x)/ 20200101000000 {{"url": "{SLOW_TEXT}"}}'
        for op in ("~", "!~"):
            with self.subTest(op=op):
                cdxj_filter = CDXJFilter(filters=[f"url{op}{SLOW_PATTERN}"], regex_timeout=0.2)
                start = time.monotonic()
                with self.assertRaises(RegexTimeoutError):
                    cdxj_filter.matches(line)
                self.assertLess(time.monotonic() - start, 5)

    def test_normal_matching_unchanged(self):
        cdxj_filter = CDXJFilter(filters=["mime~text/.*", "url!~\\.pdf$"])
        self.assertTrue(cdxj_filter.matches('a)/ 2020 {"mime": "text/html", "url": "/a.html"}'))
        self.assertFalse(cdxj_filter.matches('a)/ 2020 {"mime": "text/html", "url": "/a.pdf"}'))
        self.assertFalse(cdxj_filter.matches('a)/ 2020 {"mime": "image/png", "url": "/a.png"}'))

    def test_restores_previous_handler_and_disarms_timer(self):
        def previous(_signum, _frame):
            pass

        old = signal.signal(signal.SIGALRM, previous)
        self.addCleanup(signal.signal, signal.SIGALRM, old)
        with regex_time_limit() as active:
            self.assertTrue(active)
            self.assertIsNot(signal.getsignal(signal.SIGALRM), previous)
        self.assertIs(signal.getsignal(signal.SIGALRM), previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

        search_with_time_limit(re.compile("a"), "a")  # unscoped call
        self.assertIs(signal.getsignal(signal.SIGALRM), previous)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_leaves_foreign_timer_alone(self):
        old = signal.signal(signal.SIGALRM, lambda _s, _f: None)
        self.addCleanup(signal.signal, signal.SIGALRM, old)
        self.addCleanup(signal.setitimer, signal.ITIMER_REAL, 0)
        signal.setitimer(signal.ITIMER_REAL, 60)
        with regex_time_limit() as active:
            self.assertFalse(active)
        remaining, _interval = signal.getitimer(signal.ITIMER_REAL)
        self.assertGreater(remaining, 50)

    def test_degrades_outside_main_thread(self):
        results = {}

        def worker():
            with regex_time_limit() as active:
                results["active"] = active
            results["match"] = search_with_time_limit(re.compile("b+"), "abbb", timeout=0.2)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        self.assertFalse(results["active"])
        self.assertEqual(results["match"].group(), "bbb")

    def test_falsy_timeout_disables_backstop(self):
        with mock.patch.object(regex_safety, "regex_time_limit") as scope:
            self.assertIsNotNone(search_with_time_limit(re.compile("a"), "a", timeout=None))
            self.assertIsNotNone(search_with_time_limit(re.compile("a"), "a", timeout=0))
        scope.assert_not_called()

    def test_cdxj_search_cli_exits_on_timeout(self):
        _disable_denylist(self)
        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir)
        path = os.path.join(temp_dir, "index.cdxj")
        with open(path, "w", encoding="utf-8") as f:
            f.write(f'pt,x)/ 20200101000000 {{"url": "{SLOW_TEXT}"}}\n')
        argv = ["cdxj-search", "--surt", "pt,x)/", "--filter", f"url~{SLOW_PATTERN}", path]
        start = time.monotonic()
        with mock.patch.object(sys, "argv", argv), mock.patch("sys.stdout"):
            with self.assertRaises(SystemExit) as ctx:
                cdxj_search.main()
        self.assertEqual(ctx.exception.code, 1)
        self.assertLess(time.monotonic() - start, 10)


class TestBlocklistHardening(unittest.TestCase):
    """grep invocation, timeout and fail-closed behaviour of filter-blocklist."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.input_path = os.path.join(self.temp_dir, "input.cdxj")
        self.output_path = os.path.join(self.temp_dir, "output.cdxj")
        self.blocklist_path = os.path.join(self.temp_dir, "blocklist.txt")
        with open(self.input_path, "w", encoding="utf-8") as f:
            f.write('pt,site,www)/admin/ 20230615120000 {"url": "..."}\n')
            f.write('pt,site,www)/ 20230615120000 {"url": "..."}\n')

    def _write_blocklist(self, *patterns):
        with open(self.blocklist_path, "w", encoding="utf-8") as f:
            f.write("\n".join(patterns) + "\n")

    def _run_main(self, *extra):
        argv = ["filter-blocklist", "-i", self.input_path, "-b", self.blocklist_path]
        argv += ["-o", self.output_path, *extra]
        with mock.patch.object(sys, "argv", argv):
            with self.assertRaises(SystemExit) as ctx:
                blocklist.main()
        return ctx.exception.code

    def test_bypass_shapes_dropped_from_blocklist(self):
        self._write_blocklist("^pt,site,www\\)/admin/", *BYPASS_SHAPES)
        self.assertEqual(len(load_blocklist(self.blocklist_path)), 1)
        kept, blocked = filter_cdxj_by_blocklist(
            self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
        )
        self.assertEqual((kept, blocked), (1, 1))

    def test_surt_prefix_with_unmatched_paren_still_applied(self):
        """Patterns valid for grep -E but not for Python re are not dropped."""
        self._write_blocklist("^pt,site,www)/admin/")
        kept, blocked = filter_cdxj_by_blocklist(
            self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
        )
        self.assertEqual((kept, blocked), (1, 1))

    def test_all_patterns_rejected_fails_closed(self):
        self._write_blocklist(*BYPASS_SHAPES)
        with self.assertRaisesRegex(ValueError, "refusing to pass input through"):
            filter_cdxj_by_blocklist(
                self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
            )
        self.assertFalse(os.path.exists(self.output_path))
        self.assertEqual(self._run_main(), 1)
        self.assertFalse(os.path.exists(self.output_path))

    def test_empty_blocklist_still_passes_through(self):
        self._write_blocklist("# comments only")
        kept, blocked = filter_cdxj_by_blocklist(
            self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
        )
        self.assertEqual((kept, blocked), (2, 0))

    def test_grep_argv_ends_options_before_input(self):
        self._write_blocklist("^pt,spam,")
        completed = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with mock.patch.object(blocklist.subprocess, "run", return_value=completed) as run:
            filter_cdxj_by_blocklist(
                self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
            )
        cmd = run.call_args.args[0]
        self.assertEqual(cmd[-2:], ["--", self.input_path])
        self.assertEqual(run.call_args.kwargs["timeout"], blocklist.DEFAULT_GREP_TIMEOUT)

    def test_input_path_starting_with_dash_is_not_an_option(self):
        self._write_blocklist("^pt,site,www)/admin/")
        os.rename(self.input_path, os.path.join(self.temp_dir, "-v.cdxj"))
        cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.addCleanup(os.chdir, cwd)
        kept, blocked = filter_cdxj_by_blocklist(
            "-v.cdxj", [], self.output_path, blocklist_file=self.blocklist_path
        )
        self.assertEqual((kept, blocked), (1, 1))

    def test_grep_timeout(self):
        """grep blocked on a stdin that never ends is killed after the timeout."""
        self._write_blocklist("^pt,spam,")
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, write_fd)
        with os.fdopen(read_fd) as stdin, mock.patch.object(sys, "stdin", stdin):
            start = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                filter_cdxj_by_blocklist(
                    "-", [], self.output_path, blocklist_file=self.blocklist_path, timeout=0.5
                )
        self.assertLess(time.monotonic() - start, 10)
        self.assertFalse(os.path.exists(self.output_path))

    def test_cli_timeout_exit_code(self):
        self._write_blocklist("^pt,spam,")
        timeout = subprocess.TimeoutExpired(["grep"], 5)
        with mock.patch.object(blocklist.subprocess, "run", side_effect=timeout) as run:
            self.assertEqual(self._run_main("--timeout", "5"), 124)
        self.assertEqual(run.call_args.kwargs["timeout"], 5)

    def test_grep_error_fails_instead_of_writing_output(self):
        self._write_blocklist("[unclosed")  # passes the shape check, invalid ERE
        with self.assertRaises(subprocess.CalledProcessError):
            filter_cdxj_by_blocklist(
                self.input_path, [], self.output_path, blocklist_file=self.blocklist_path
            )
        self.assertFalse(os.path.exists(self.output_path))


if __name__ == "__main__":
    unittest.main()
