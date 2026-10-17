#!/usr/bin/env python3
"""
Tests for the shared ReDoS safeguards (replay_cdxj_indexing_tools.regex_safety)
and their use by the search filters and the blocklist filter (#74, #75).
"""

import io
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
from replay_cdxj_indexing_tools.filter.blocklist import (
    check_grep_pattern,
    filter_cdxj_by_blocklist,
    load_blocklist,
)
from replay_cdxj_indexing_tools.regex_safety import (
    RegexTimeoutError,
    check_regex_shape,
    compile_safe_regex,
    regex_time_limit,
    search_with_time_limit,
)
from replay_cdxj_indexing_tools.search import cdxj_search, filters
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
            # The watchdog keeps working after a timeout, and nested scopes reuse it
            with regex_time_limit() as nested:
                self.assertTrue(nested)
                self.assertIsNotNone(search_with_time_limit(re.compile("a+"), "aaa", timeout=0.2))
        self.assertLess(time.monotonic() - start, 5)

    def test_search_shorter_than_limit_succeeds(self):
        """Watchdog ticks during a search that stays within its budget are harmless."""

        class SlowRegex:  # pylint: disable=too-few-public-methods
            """Stands in for a pattern whose search spans several ticks."""

            def search(self, _text):
                time.sleep(0.2)  # resumed after each SIGALRM tick (PEP 475)
                return "match"

        with regex_time_limit() as active:
            self.assertTrue(active)
            self.assertEqual(search_with_time_limit(SlowRegex(), "", timeout=1), "match")

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

    def test_degraded_mode_warns_once(self):
        def worker():
            for _ in range(3):
                search_with_time_limit(re.compile("b"), "b", timeout=0.2)

        with (
            mock.patch.object(regex_safety, "_WARNED", set()),
            mock.patch.object(regex_safety.logger, "warning") as warning,
        ):
            for _ in range(2):
                thread = threading.Thread(target=worker)
                thread.start()
                thread.join()
        self.assertEqual(warning.call_count, 1)

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

        def quick_limit(regex, text, _timeout):
            return search_with_time_limit(regex, text, 0.2)

        start = time.monotonic()
        with (
            mock.patch.object(sys, "argv", argv),
            mock.patch("sys.stdout"),
            mock.patch.object(filters, "search_with_time_limit", quick_limit),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cdxj_search.main()
        self.assertEqual(ctx.exception.code, 124)
        self.assertLess(time.monotonic() - start, 5)


def _long_alternation():
    """A host alternation of about 1100 characters, as found in real blocklists."""
    return "^pt,(" + "|".join(f"host{i:04d}" for i in range(122)) + "),www\\)/"


# Blocklist idioms that must be accepted by the grep-path check and applied
ACCEPTED_LINES = [
    "^pt,my(-[a-z]+)*,www\\)/",
    "^com,example(,[a-z0-9-]+)*)/private/",
    "^pt,ip)/[0-9]+(\\.[0-9]+)+/",
    "^pt,site,www)/(ads|tracking)+",
    _long_alternation(),
]

# ACCEPTED_LINES written as valid Python regexes, for load_blocklist()
LIBRARY_LINES = [
    "^pt,my(-[a-z]+)*,www\\)/",
    "^com,example(,[a-z0-9-]+)*\\)/private/",
    "^pt,ip\\)/[0-9]+(\\.[0-9]+)+/",
    "^pt,site,www\\)/(ads|tracking)+",
    _long_alternation(),
]

# Records targeted by ACCEPTED_LINES, in the same order
TARGETED_RECORDS = [
    'pt,my-blog-site,www)/ 20230615120000 {"url": "..."}\n',
    'com,example,cdn,eu)/private/a 20230615120000 {"url": "..."}\n',
    'pt,ip)/10.0.0.1/ 20230615120000 {"url": "..."}\n',
    'pt,site,www)/adstracking 20230615120000 {"url": "..."}\n',
    'pt,host0121,www)/ 20230615120000 {"url": "..."}\n',
]

KEPT_RECORD = 'pt,keep,www)/ 20230615120000 {"url": "..."}\n'

# Lines the grep-path check rejects (constructs that are expensive for grep),
# each paired with a record it targets
REJECTED_LINES = [
    ("^pt,(echo),\\1\\)/", 'pt,echo,echo)/ 20230615120000 {"url": "..."}\n'),
    ("^pt,z{0,300}rep,", 'pt,rep,www)/ 20230615120000 {"url": "..."}\n'),
    ("^pt,long,|" + "x" * 10000, 'pt,long,www)/ 20230615120000 {"url": "..."}\n'),
    ("^pt,(n{0,40}){0,40}est,", 'pt,nest,www)/ 20230615120000 {"url": "..."}\n'),
]

# Matches every record targeted by REJECTED_LINES
REJECTED_TARGETS = "^pt,(echo|rep|long|nest),"


class TestGrepPatternCheck(unittest.TestCase):
    """Checks applied to blocklist lines before they reach grep."""

    def test_accepts_blocklist_idioms(self):
        self.assertGreater(len(_long_alternation()), 1000)
        for pattern in ACCEPTED_LINES + LEGIT_PATTERNS + BYPASS_SHAPES:
            with self.subTest(pattern=pattern[:40]):
                check_grep_pattern(pattern)

    def test_rejects_expensive_constructs(self):
        for pattern, _record in REJECTED_LINES:
            with self.subTest(pattern=pattern[:40]):
                with self.assertRaises(ValueError):
                    check_grep_pattern(pattern)

    def test_escapes_and_bracket_expressions(self):
        for pattern in ["\\\\1", "a\\{300}", "[{300}]", "[]{300}]", "[[:digit:]{300}]", "x{255}"]:
            with self.subTest(pattern=pattern):
                check_grep_pattern(pattern)
        for pattern in ["\\\\\\1", "x{256}", "x{,300}", "x{300,}", "[a]{1,300}"]:
            with self.subTest(pattern=pattern):
                with self.assertRaises(ValueError):
                    check_grep_pattern(pattern)

    def test_nested_repeat_cost(self):
        accepted = [
            "x{255}",
            "(ab{1,10}){1,20}",
            "(x{255}){4}",
            "((a{10}){10}){10}",
            "(a{30}|b){30}",
        ]
        for pattern in accepted:
            with self.subTest(pattern=pattern):
                check_grep_pattern(pattern)
        rejected = ["(x{255}){5}", "(x{0,100}){0,100}", "((a{10}){10}){11}", "x{10}{10}{11}"]
        rejected += ["(a{40}|b){40}", "^pt,(([a-z]{1,8}\\.){1,8}){1,20}x"]
        for pattern in rejected:
            with self.subTest(pattern=pattern):
                with self.assertRaisesRegex(ValueError, "combined repeat cost"):
                    check_grep_pattern(pattern)


class TestBlocklistHardening(unittest.TestCase):
    """grep invocation, timeout and fail-closed behaviour of filter-blocklist."""

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir)
        self.input_path = os.path.join(self.temp_dir, "input.cdxj")
        self.output_path = os.path.join(self.temp_dir, "output.cdxj")
        self.blocklist_path = os.path.join(self.temp_dir, "blocklist.txt")
        records = TARGETED_RECORDS + [record for _, record in REJECTED_LINES] + [KEPT_RECORD]
        with open(self.input_path, "w", encoding="utf-8") as f:
            f.writelines(records)
        self.total = len(records)

    def _write_blocklist(self, *patterns, prefix=""):
        with open(self.blocklist_path, "w", encoding="utf-8") as f:
            f.write(prefix + "\n".join(patterns) + "\n")

    def _filter(self, **kwargs):
        return filter_cdxj_by_blocklist(
            self.input_path, [], self.output_path, blocklist_file=self.blocklist_path, **kwargs
        )

    def _output(self):
        with open(self.output_path, encoding="utf-8") as f:
            return f.read()

    def _run_main(self, *extra):
        argv = ["filter-blocklist", "-i", self.input_path, "-b", self.blocklist_path]
        argv += ["-o", self.output_path, *extra]
        stderr = io.StringIO()
        with mock.patch.object(sys, "argv", argv), mock.patch.object(sys, "stderr", stderr):
            try:
                blocklist.main()
            except SystemExit as e:
                return e.code, stderr.getvalue()
        return 0, stderr.getvalue()

    def test_blocklist_idioms_block_their_records(self):
        self._write_blocklist(*ACCEPTED_LINES, REJECTED_TARGETS)
        kept, blocked = self._filter()
        self.assertEqual((kept, blocked), (1, self.total - 1))
        self.assertEqual(self._output(), KEPT_RECORD)

    def test_any_rejected_line_fails_closed(self):
        line_num = len(ACCEPTED_LINES) + 1
        for pattern, _record in REJECTED_LINES:
            with self.subTest(pattern=pattern[:40]):
                self._write_blocklist(*ACCEPTED_LINES, pattern)
                with self.assertRaisesRegex(ValueError, f"line {line_num}:"):
                    self._filter()
                self.assertFalse(os.path.exists(self.output_path))

                code, stderr = self._run_main()
                self.assertEqual(code, 1)
                self.assertIn(f"line {line_num}:", stderr)
                self.assertFalse(os.path.exists(self.output_path))

    def test_library_path_applies_blocklist_idioms(self):
        self._write_blocklist(*LIBRARY_LINES, REJECTED_TARGETS)
        kept, blocked = filter_cdxj_by_blocklist(
            self.input_path, load_blocklist(self.blocklist_path), self.output_path
        )
        self.assertEqual((kept, blocked), (1, self.total - 1))
        self.assertEqual(self._output(), KEPT_RECORD)

    def test_library_path_fails_closed(self):
        line_num = len(LIBRARY_LINES) + 1
        for pattern, _record in REJECTED_LINES:
            with self.subTest(pattern=pattern[:40]):
                self._write_blocklist(*LIBRARY_LINES, pattern)
                patterns = load_blocklist(self.blocklist_path)
                with self.assertRaisesRegex(ValueError, f"pattern {line_num}:"):
                    filter_cdxj_by_blocklist(self.input_path, patterns, self.output_path)
                self.assertFalse(os.path.exists(self.output_path))

    def test_load_blocklist_fails_closed_on_invalid_regex(self):
        self._write_blocklist("^pt,spam,", "^pt,site,www)/admin/", "/ads/", "[unclosed")
        with self.assertRaises(ValueError) as ctx:
            load_blocklist(self.blocklist_path)
        self.assertIn("line 2:", str(ctx.exception))
        self.assertIn("line 4:", str(ctx.exception))
        self.assertNotIn("line 1:", str(ctx.exception))

    def test_documented_python_api_example(self):
        """The PYTHON API example in the module docstring, end to end."""
        with open(self.input_path, "w", encoding="utf-8") as f:
            f.write('pt,spam,www)/ 20230615120000 {"url": "..."}\n')
            f.write('pt,evil,www)/ 20230615120000 {"url": "..."}\n')
            f.write(KEPT_RECORD)
        self._write_blocklist("# comment", "^pt,spam,", "^pt,(evil|bad)+,")
        patterns = load_blocklist(self.blocklist_path)
        kept, blocked = filter_cdxj_by_blocklist(self.input_path, patterns, self.output_path)
        self.assertEqual((kept, blocked), (1, 2))
        self.assertEqual(self._output(), KEPT_RECORD)

    def test_checked_lines_not_raw_file_reach_grep(self):
        """Blank and comment lines and a BOM are stripped before grep sees the file."""
        self._write_blocklist("^pt,keep,", "", "# comment", prefix="\ufeff")
        kept, blocked = self._filter()
        self.assertEqual((kept, blocked), (self.total - 1, 1))
        self.assertNotIn("pt,keep,", self._output())

    def test_non_ascii_pattern_under_ascii_locale(self):
        with open(self.input_path, "a", encoding="utf-8") as f:
            f.write('pt,ação,www)/ 20230615120000 {"url": "..."}\n')
        self._write_blocklist("^pt,ação,")
        env = dict(os.environ, LC_ALL="C", LANG="C", PYTHONUTF8="0", PYTHONCOERCECLOCALE="0")
        env.pop("PYTHONIOENCODING", None)
        result = subprocess.run(
            [sys.executable, "-m", "replay_cdxj_indexing_tools.filter.blocklist"]
            + ["-i", self.input_path, "-b", self.blocklist_path, "-o", self.output_path],
            env=env,
            capture_output=True,
            check=False,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("ação", self._output())
        self.assertEqual(self._output().count("\n"), self.total)

    def test_empty_blocklist_still_passes_through(self):
        self._write_blocklist("# comments only")
        self.assertEqual(self._filter(), (self.total, 0))

    def test_cli_does_not_compile_patterns_with_python_re(self):
        self._write_blocklist("^pt,site,www)/(ads|tracking)+")
        with mock.patch.object(blocklist, "load_blocklist") as load:
            code, stderr = self._run_main()
        load.assert_not_called()
        self.assertEqual(code, 0)
        self.assertNotIn("Warning", stderr)

    def test_input_path_starting_with_dash_is_not_an_option(self):
        self._write_blocklist("^pt,keep,")
        os.rename(self.input_path, os.path.join(self.temp_dir, "-v.cdxj"))
        cwd = os.getcwd()
        os.chdir(self.temp_dir)
        self.addCleanup(os.chdir, cwd)
        kept, blocked = filter_cdxj_by_blocklist(
            "-v.cdxj", [], self.output_path, blocklist_file=self.blocklist_path
        )
        self.assertEqual((kept, blocked), (self.total - 1, 1))

    def test_grep_timeout(self):
        """grep blocked on a stdin that never ends is killed after the timeout."""
        self._write_blocklist("^pt,spam,")
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, write_fd)
        with os.fdopen(read_fd) as stdin, mock.patch.object(sys, "stdin", stdin):
            start = time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                filter_cdxj_by_blocklist(
                    "-", [], self.output_path, blocklist_file=self.blocklist_path, timeout=0.1
                )
        self.assertLess(time.monotonic() - start, 10)
        self.assertFalse(os.path.exists(self.output_path))

    def test_cli_timeout_option(self):
        self._write_blocklist("^pt,spam,")
        timeout = subprocess.TimeoutExpired(["grep"], 5)
        with mock.patch.object(blocklist.subprocess, "run", side_effect=timeout) as run:
            self.assertEqual(self._run_main("--timeout", "5")[0], 124)
        self.assertEqual(run.call_args.kwargs["timeout"], 5)

        completed = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        with mock.patch.object(blocklist.subprocess, "run", return_value=completed) as run:
            self.assertEqual(self._run_main()[0], 0)
        self.assertIsNone(run.call_args.kwargs["timeout"])  # default: no limit

        for value in ["nan", "inf", "-1", "soon"]:
            with self.subTest(value=value):
                code, stderr = self._run_main("--timeout", value)
                self.assertEqual(code, 2)
                self.assertIn("finite number >= 0", stderr)

    def test_grep_failure_writes_no_output(self):
        for returncode in (2, -9):  # an error status, or killed by a signal
            with self.subTest(returncode=returncode):
                self._write_blocklist("^pt,spam,")
                completed = subprocess.CompletedProcess([], returncode, stdout="x\n", stderr="")
                with mock.patch.object(blocklist.subprocess, "run", return_value=completed):
                    with self.assertRaises(subprocess.CalledProcessError):
                        self._filter()
                self.assertFalse(os.path.exists(self.output_path))

        self._write_blocklist("[unclosed")  # accepted by the check, invalid ERE
        with self.assertRaises(subprocess.CalledProcessError):
            self._filter()
        self.assertFalse(os.path.exists(self.output_path))


if __name__ == "__main__":
    unittest.main()
