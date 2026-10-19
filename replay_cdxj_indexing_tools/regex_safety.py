"""
Shared safeguards for user-supplied Python regular expressions (CWE-1333,
ReDoS), used by ``search/filters.py`` (``--filter field~regex``).
(filter-blocklist hands its patterns to grep instead, which has its own
checks in ``filter/blocklist.py``.) Two layers:

1. ``check_regex_shape()`` / ``compile_safe_regex()`` -- a length cap plus a
   heuristic denylist of pattern shapes known to cause catastrophic
   backtracking. It is a cheap first line of defence and is not
   exhaustive; the time limit below is the primary bound.
2. ``search_with_time_limit()`` -- an execution-time limit around each
   ``re.search()`` call, built on ``SIGALRM``/``setitimer`` (stdlib only).
   It bounds every single search, not a whole run.

Time-limit limitations (documented degraded behaviour -- the regex then runs
without a time bound and only the denylist applies; a warning is logged once
per process):

* platforms without ``signal.setitimer``/``SIGALRM`` (e.g. Windows);
* calls made outside the main thread (signals are only delivered there);
* when another component already has an ``ITIMER_REAL``/``alarm()`` timer
  armed (it is left untouched rather than silently broken);
* when the current ``SIGALRM`` handler was not installed from Python and so
  could not be restored afterwards.

Library, threaded or server embedders should wrap their matching loop in
``regex_time_limit()`` on the main thread, or bound the work at the process
level (e.g. a worker process with a deadline).
"""

import contextlib
import dataclasses
import functools
import logging
import re
import signal
import threading
import time
from typing import Iterator, Optional, Set

logger = logging.getLogger(__name__)

# Maximum regex pattern length to prevent memory issues
MAX_PATTERN_LEN = 1000

# Default wall-clock budget for a single regex search, in seconds
DEFAULT_REGEX_TIMEOUT = 1.0

# Period of the watchdog tick; a timeout is detected within this much slack
_TICK_SECONDS = 0.05

# A quantifier that lets a group match more than once: + * {n} {n,} {n,m}
_REPEAT = r"(?:[+*]|\{\d*,?\d*\})"
# A variable-length quantifier (optionally lazy/possessive): + * ? {n,} {n,m}
_VARIABLE = r"(?:[+*?]|\{\d*,\d*\})[?+]?"

# Pattern shapes known to cause catastrophic backtracking (ReDoS)
_REDOS_PATTERNS = [
    # Nested quantifier: a group ending in a variable quantifier is itself
    # repeated -- (x+)+ (x*)* (x+){2,} (x{1,10}){1,10} ((x+))+ (x?){30}
    re.compile(_VARIABLE + r"\)+" + _REPEAT),
    # Repeated alternation -- (a|b)* (a|b)+ (a|ab){2,}
    re.compile(r"\([^)]+\|[^)]+\)" + _REPEAT),
    # Constructs that hide pattern structure from the checks above
    re.compile(r"\(\?#"),  # inline comment group
    re.compile(r"\(\?[a-zA-Z-]*x"),  # verbose flag: (?x) ignores whitespace
]


def check_regex_shape(pattern: str, max_len: int = MAX_PATTERN_LEN) -> None:
    """
    Reject a pattern that is too long or has a known ReDoS shape.

    Syntax is not checked here; see compile_safe_regex().

    Raises:
        ValueError: if the pattern is too long or ReDoS-shaped.
    """
    if len(pattern) > max_len:
        raise ValueError(f"Regex pattern too long ({len(pattern)} chars, max {max_len})")
    for danger in _REDOS_PATTERNS:
        if danger.search(pattern):
            raise ValueError(f"Regex pattern rejected due to ReDoS risk: {pattern!r}")


# NOTE: this cache is process-global. Harmless for the CLI tools, but if this
# module is ever reused in a long-lived process serving several tenants, the
# cache becomes a (latent) shared-state / timing side channel between them.
@functools.lru_cache(maxsize=256)
def compile_safe_regex(pattern: str, max_len: int = MAX_PATTERN_LEN) -> re.Pattern:
    """Compile a regex pattern, rejecting patterns known to risk ReDoS. Cached.

    Raises:
        ValueError: if the pattern is too long, ReDoS-shaped or invalid.
    """
    check_regex_shape(pattern, max_len)
    try:
        return re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {pattern!r} ({exc})") from exc


class RegexTimeoutError(RuntimeError):
    """A regex search exceeded its time budget."""


@dataclasses.dataclass(slots=True)
class _Watchdog:
    """State shared between the SIGALRM handler and the guarded search."""

    # Ident of the (main) thread owning the watchdog; None when inactive
    thread_ident: Optional[int] = None
    # monotonic() time after which the running search is aborted
    deadline: Optional[float] = None


_STATE = _Watchdog()
# Degraded-mode reasons already logged (warn once per process, not per call)
_WARNED: Set[str] = set()
# Bound once: these run on every guarded search
_get_ident = threading.get_ident
_monotonic = time.monotonic


def _warn_once(reason: str) -> None:
    if reason not in _WARNED:
        _WARNED.add(reason)
        logger.warning("regex time limit disabled: %s", reason)


def _on_alarm(_signum, _frame):
    deadline = _STATE.deadline
    if deadline is not None and _monotonic() >= deadline:
        _STATE.deadline = None
        raise RegexTimeoutError("regex search exceeded its time limit")


@contextlib.contextmanager
def regex_time_limit() -> Iterator[bool]:
    """
    Install the SIGALRM watchdog for the duration of the ``with`` block.

    Wrapping a loop of ``search_with_time_limit()`` calls in this context
    makes each call cheap (no per-call syscalls). The previous SIGALRM
    handler is restored and the timer disarmed on exit. Re-entrant.

    Yields:
        True if the backstop is active, False if it had to be disabled (see
        the module docstring for when that happens).
    """
    if _STATE.thread_ident is not None:
        # Nested scope: active only on the thread that owns the watchdog
        yield _get_ident() == _STATE.thread_ident
        return
    if not hasattr(signal, "setitimer"):
        _warn_once("signal.setitimer is not available on this platform")
        yield False
        return
    if threading.current_thread() is not threading.main_thread():
        _warn_once("not running in the main thread")
        yield False
        return
    if signal.getsignal(signal.SIGALRM) is None:
        _warn_once("the current SIGALRM handler was not installed from Python")
        yield False
        return

    previous_handler = signal.signal(signal.SIGALRM, _on_alarm)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, _TICK_SECONDS, _TICK_SECONDS)
    if previous_timer[0] > 0:
        # Someone else owns ITIMER_REAL; hand it back rather than break it.
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)
        _warn_once("ITIMER_REAL is already in use")
        yield False
        return

    _STATE.thread_ident = _get_ident()
    try:
        yield True
    finally:
        _STATE.thread_ident = None
        _STATE.deadline = None
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def search_with_time_limit(
    regex: re.Pattern, text: str, timeout: Optional[float] = DEFAULT_REGEX_TIMEOUT
) -> Optional[re.Match]:
    """
    ``regex.search(text)``, aborted with ``RegexTimeoutError`` once it has run
    for ``timeout`` seconds (plus up to ``_TICK_SECONDS`` of slack).

    Cheap inside a ``regex_time_limit()`` block; outside one, the watchdog is
    installed and removed around this single call, which costs several
    microseconds more. A falsy ``timeout`` disables the limit. Falls back to
    an unbounded search where the backstop is unavailable (see the module
    docstring).
    """
    if not timeout:
        return regex.search(text)
    owner = _STATE.thread_ident
    if owner is None:
        with regex_time_limit() as active:
            if active:
                return search_with_time_limit(regex, text, timeout)
            return regex.search(text)
    if owner != _get_ident():
        _warn_once("not running in the main thread")
        return regex.search(text)  # signals only reach the main thread

    _STATE.deadline = _monotonic() + timeout
    try:
        return regex.search(text)
    finally:
        _STATE.deadline = None
