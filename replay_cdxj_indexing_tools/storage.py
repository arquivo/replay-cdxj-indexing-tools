"""
File access shared by the tools: local paths and remote locations.

Local paths are opened with the built-in open(), so their behaviour does not depend
on this module. Remote locations are recognised by their URL scheme and read through
fsspec, which is installed with the optional extras:

    pip install "replay-cdxj-indexing-tools[http]"   # http:// and https://
    pip install "replay-cdxj-indexing-tools[s3]"     # s3:// (S3-compatible storage)

A remote object is read with byte-range requests, so seeking is free and only the
bytes that are read are fetched: a binary search over a remote .idx or flat CDXJ
costs a few small requests, not a download. Fetches start small and double while
reading sequentially, so streaming a whole file uses few large requests. Transient
failures (timeouts, dropped connections, HTTP 429/5xx) are retried with backoff.

Only the schemes in REMOTE_SCHEMES are accepted. Remote locations must not carry
credentials, a query string or a fragment: S3 credentials and endpoints come from
the standard AWS mechanisms (environment, config profile) or fsspec configuration,
so nothing secret ends up in command lines or log messages. HTTP redirects are
followed only within the origin (scheme, host and port) of the requested URL.

Environment:
    CDXJ_REMOTE_TIMEOUT  Seconds to wait for a connection or for data, for http://
                         and https:// (default 60); S3 uses the s3fs/botocore timeouts
    CDXJ_REMOTE_RETRIES  Retries after a transient read failure (default 3)
"""

import importlib
import io
import os
import random
import re
import threading
import time
from collections import OrderedDict
from email.utils import parsedate_to_datetime
from http import HTTPStatus
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    TextIO,
    overload,
)
from urllib.parse import urlsplit

REMOTE_SCHEMES = ("http", "https", "s3")

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://")
# Glob characters of s3:// patterns; "?" cannot appear, since is_remote() rejects it
_WILDCARD_CHARS = ("*", "[")
# Query string or fragment of a URL inside an error message
_URL_QUERY_RE = re.compile(r"([A-Za-z][A-Za-z0-9+.-]*://[^\s?#'\"]*)[?#][^\s'\"]*")

# First fetch at a new position, aligned to a multiple of its size; doubled on each
# sequential fetch up to the maximum
_MIN_FETCH = 64 * 1024
_MAX_FETCH = 4 * 1024 * 1024
# Bytes of fetched data kept per open remote object
_CACHE_BYTES = 2 * _MAX_FETCH
# Read size of the buffered reader in front of a remote object
_BUFFER_SIZE = 64 * 1024

_DEFAULT_TIMEOUT = 60.0
_MAX_TIMEOUT = 3600.0
_DEFAULT_RETRIES = 3
_MAX_RETRIES = 20
_RETRY_BACKOFF = 0.5  # seconds, doubled after each attempt
_MAX_RETRY_DELAY = 30.0  # seconds; caps the backoff and a server's Retry-After, before jitter
_RETRY_JITTER = 0.25  # each wait is lengthened by up to this fraction
_jitter = random.SystemRandom()

# fsspec protocol for each scheme, which is also the name of the extra installing it
_PROTOCOLS = {"http": "http", "https": "http", "s3": "s3"}

_filesystems: Dict[str, Any] = {}
_filesystems_lock = threading.Lock()


class RemoteReadError(OSError):
    """A remote location could not be read, after retrying transient failures."""


class RedirectRefusedError(RemoteReadError):
    """An HTTP server redirected to a different origin (scheme, host or port)."""


class MissingDependencyError(OSError):
    """Reading a remote location needs optional dependencies that are not installed."""


def _env_number(
    name: str, default: float, minimum: float, maximum: float, kind: Callable[[str], float] = float
) -> float:
    """Read a numeric setting; ``kind`` is float or int."""
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        number = kind(value)
    except ValueError as exc:
        what = "a whole number" if kind is int else "a number"
        raise ValueError(f"{name} must be {what}, got {value!r}") from exc
    if not minimum <= number <= maximum:  # also rejects nan
        raise ValueError(f"{name} must be between {minimum:g} and {maximum:g}, got {value!r}")
    return number


def _strip_queries(text: str) -> str:
    """``text`` with the query string and fragment of every URL in it removed."""
    return _URL_QUERY_RE.sub(r"\1", text)


def _printable(text: str) -> str:
    """``text`` with non-printable characters (terminal control sequences) escaped."""
    return "".join(char if char.isprintable() else repr(char)[1:-1] for char in text)


def _scheme(location: str) -> Optional[str]:
    match = _SCHEME_RE.match(location)
    return match.group(1).lower() if match else None


def is_remote(location: str) -> bool:
    """
    Return True if ``location`` is a remote URL, False if it is a local path.

    Raises:
        ValueError: If ``location`` has a URL scheme outside REMOTE_SCHEMES, or is a
            remote URL with credentials, a query string, a fragment or no host
    """
    scheme = _scheme(location)
    if scheme is None:
        return False
    if scheme not in REMOTE_SCHEMES:
        allowed = ", ".join(f"{s}://" for s in REMOTE_SCHEMES)
        raise ValueError(f"Unsupported location scheme {scheme}:// (supported: {allowed})")
    parts = urlsplit(location)
    if "@" in parts.netloc:
        raise ValueError(
            f"Credentials are not accepted in remote locations ({scheme}://...@...); "
            "use the environment or a config profile instead"
        )
    if "?" in location or "#" in location:
        raise ValueError(
            "Remote locations must not have a query string or fragment: "
            f"{_strip_queries(location)}"
        )
    if not parts.hostname:
        raise ValueError(f"Remote location has no host or bucket: {location}")
    if "\x00" in location or "\\" in location:
        raise ValueError(f"Remote location contains a NUL byte or backslash: {location!r}")
    return True


def parent_location(location: str) -> str:
    """Directory containing ``location``: the URL up to its last ``/``, or the local dirname."""
    if not is_remote(location):
        return os.path.dirname(os.path.abspath(location))
    parts = urlsplit(location)
    path = parts.path.rsplit("/", 1)[0] if "/" in parts.path else ""
    return f"{parts.scheme}://{parts.netloc}{path}"


def join_location(base: str, name: str) -> str:
    """Join a relative ``name`` to the directory ``base`` (local path or URL)."""
    if is_remote(base):
        return base.rstrip("/") + "/" + name
    return os.path.join(base, name)


def _missing_extra_message(scheme: str) -> str:
    extra = _PROTOCOLS[scheme]
    return (
        f"Reading {scheme}:// locations needs the optional dependencies: "
        f'pip install "replay-cdxj-indexing-tools[{extra}]"'
    )


def _origin(url: Any) -> Any:
    """(scheme, host, port) of a yarl URL, or None if it has no host."""
    return (url.scheme, url.host, url.port) if url.absolute and url.host else None


def _show_origin(url: Any) -> str:
    """scheme://host:port of a yarl URL, with the port always shown."""
    host = f"[{url.host}]" if ":" in url.host else url.host
    return f"{url.scheme}://{host}:{url.port}"


def _redirect_guard(aiohttp: Any) -> Any:
    """aiohttp trace config that refuses a redirect leaving the current origin."""

    async def on_redirect(_session, _context, params) -> None:
        response = params.response
        location = response.headers.get("Location") or response.headers.get("URI")
        if location is None:
            return  # aiohttp does not follow it either
        try:
            target = params.url.join(type(params.url)(location))
        except ValueError as exc:
            response.release()
            raise RedirectRefusedError("the server redirected to an invalid URL") from exc
        if _origin(target) == _origin(params.url):
            return
        response.release()
        shown = _show_origin(target) if _origin(target) else "a URL without a host"
        hint = ""
        if (params.url.scheme, target.scheme, target.host) == ("http", "https", params.url.host):
            hint = "; give the https:// URL instead"
        raise RedirectRefusedError(
            f"the server redirected from {_show_origin(params.url)} to {shown}; redirects to "
            f"another host, port or scheme are not followed{hint}"
        )

    trace = aiohttp.TraceConfig()
    trace.on_request_redirect.append(on_redirect)
    return trace


def _http_client_kwargs(fsspec_config: Dict[str, Any]) -> Dict[str, Any]:
    """aiohttp session options: the fsspec configuration, plus the timeout and redirect guard."""
    import aiohttp  # pylint: disable=import-outside-toplevel  # optional dependency

    client_kwargs = dict(fsspec_config.get("http", {}).get("client_kwargs", {}))
    timeout = _env_number("CDXJ_REMOTE_TIMEOUT", _DEFAULT_TIMEOUT, 1, _MAX_TIMEOUT)
    # No total limit: streaming a large file legitimately takes long
    client_kwargs.setdefault(
        "timeout", aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
    )
    client_kwargs["trace_configs"] = [
        *client_kwargs.get("trace_configs", []),
        _redirect_guard(aiohttp),
    ]
    return client_kwargs


def _filesystem(scheme: str) -> Any:
    """Return the fsspec filesystem used for ``scheme``, created once per process."""
    protocol = _PROTOCOLS[scheme]
    with _filesystems_lock:
        fs = _filesystems.get(protocol)
        if fs is not None:
            return fs
        try:
            # pylint: disable=import-outside-toplevel  # optional dependencies
            import fsspec
            import fsspec.config

            importlib.import_module("aiohttp" if protocol == "http" else "s3fs")
        except ImportError as exc:
            raise MissingDependencyError(_missing_extra_message(scheme)) from exc

        kwargs: Dict[str, Any] = {}
        if protocol == "http":
            kwargs["client_kwargs"] = _http_client_kwargs(fsspec.config.conf)
        fs = fsspec.filesystem(protocol, **kwargs)
        _filesystems[protocol] = fs
        return fs


def _is_transient(exc: BaseException) -> bool:
    """Whether a failed remote operation is worth retrying."""
    if isinstance(
        exc,
        (
            FileNotFoundError,
            PermissionError,
            IsADirectoryError,
            NotADirectoryError,
            RemoteReadError,
        ),
    ):
        return False
    status = getattr(exc, "status", None)  # aiohttp.ClientResponseError
    if isinstance(status, int):
        return status == 429 or status >= 500
    if isinstance(exc, ValueError):  # e.g. aiohttp.InvalidURL for a malformed redirect
        return False
    if isinstance(exc, OSError):  # includes TimeoutError, which asyncio and aiohttp raise
        return True
    # aiohttp.ClientError subclasses are not all OSError; recognise them by module
    return type(exc).__module__.startswith("aiohttp")


def _is_not_found_hiding(cause: BaseException) -> bool:
    """Whether a FileNotFoundError from fsspec hides this other failure as its cause."""
    if getattr(cause, "status", None) == 404:
        return False
    return (
        isinstance(cause, RemoteReadError)
        or _is_transient(cause)
        or type(cause).__module__.startswith("aiohttp")  # e.g. an HTTP status
    )


def _underlying_error(exc: BaseException) -> BaseException:
    """
    The error to classify: fsspec reports an HTTP file whose size it cannot get
    as FileNotFoundError, whatever the cause (a transient failure, a refused or
    invalid redirect, a 401 or 403), and a 404 on a range read as a plain HTTP error.
    """
    cause = exc.__cause__
    if isinstance(exc, FileNotFoundError) and cause is not None and _is_not_found_hiding(cause):
        return cause
    if getattr(exc, "status", None) == 404:  # a range read of a missing HTTP object
        return FileNotFoundError(str(exc))
    return exc


def _describe(error: BaseException) -> str:
    """
    Error text for a message: no query strings (a server may redirect to a signed
    URL), and nothing the server chose that could drive the terminal.
    """
    if any(cls.__name__ == "TooManyRedirects" for cls in type(error).__mro__):
        return "too many redirects"
    status = getattr(error, "status", None)
    if isinstance(status, int):  # aiohttp.ClientResponseError; its text has the final URL
        try:
            reason = HTTPStatus(status).phrase
        except ValueError:  # not a standard status: keep the server's reason, made safe
            reason = _printable(getattr(error, "message", "") or "")
        return f"HTTP {status} {reason}".rstrip()
    if isinstance(error, RemoteReadError):
        return _printable(str(error))
    return _printable(_strip_queries(f"{type(error).__name__}: {error}"))


def _retry_after(error: BaseException) -> Optional[float]:
    """Seconds asked for by the Retry-After header of an HTTP 429 or 503, if any."""
    if getattr(error, "status", None) not in (429, 503):
        return None
    headers = getattr(error, "headers", None) or {}
    value = headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - time.time())


def _retry_delay(error: BaseException, attempt: int) -> float:
    delay = max(_RETRY_BACKOFF * 2**attempt, _retry_after(error) or 0.0)
    return min(delay, _MAX_RETRY_DELAY) * (1 + _jitter.uniform(0, _RETRY_JITTER))


def _with_retries(location: str, action: str, func):
    retries = int(_env_number("CDXJ_REMOTE_RETRIES", _DEFAULT_RETRIES, 0, _MAX_RETRIES, int))
    for attempt in range(retries + 1):
        try:
            return func()
        except Exception as exc:  # pylint: disable=broad-exception-caught  # classified below
            error = _underlying_error(exc)
            if isinstance(error, FileNotFoundError):
                raise FileNotFoundError(f"Remote location not found: {location}") from exc
            if attempt == retries or not _is_transient(error):
                tries = f" after {attempt + 1} attempts" if attempt else ""
                raise RemoteReadError(
                    f"Could not {action} {location}{tries}: {_describe(error)}"
                ) from exc
            time.sleep(_retry_delay(error, attempt))
    raise AssertionError("unreachable")  # pragma: no cover


class RemoteFile(io.RawIOBase):
    """
    Seekable read-only view of a remote object, fetched with byte-range requests.

    Recently fetched ranges are kept so that the back-and-forth of a binary search
    near its end, or a line that crosses a fetch boundary, needs no new request.
    While reading sequentially, ranges behind the read position are dropped.

    Opening costs one request to learn the object's size, unless ``size`` is given.
    A caller that knows which ranges it will read can pass the end of the last one
    as ``size``: reads stop there, and an object shorter than that is reported as a
    short read.
    """

    def __init__(self, location: str, size: Optional[int] = None):
        super().__init__()
        self.location = location
        # Set before anything can raise: close() runs on a failed open too, when
        # the half-built object is finalized
        self._fh: Any = None
        self.size: int = 0
        self._pos = 0
        self._chunks: "OrderedDict[int, bytes]" = OrderedDict()
        self._fetch_size = _MIN_FETCH
        self._sequential_end = -1

        scheme = _scheme(location) or ""
        fs = _filesystem(scheme)
        # cache_type="none": every read() is one range request; caching happens here
        fh = _with_retries(
            location, "open", lambda: fs.open(location, "rb", cache_type="none", size=size)
        )
        size = getattr(fh, "size", None)
        if size is None:
            # fsspec streams an HTTP object that is empty, has no known size, or is
            # served without ranges; only the first can be read here. Whether it is
            # empty is decided by the streamed GET, not by the size the HEAD gave.
            try:
                first_byte = _with_retries(location, "read", lambda: fh.read(1))
            finally:
                fh.close()
            if first_byte:
                raise RemoteReadError(
                    f"Could not open {location}: the server does not support range requests "
                    "(it must send Accept-Ranges: bytes and a Content-Length)"
                )
            size = 0
        else:
            self._fh = fh
        self.size = size

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self._pos + offset
        elif whence == io.SEEK_END:
            pos = self.size + offset
        else:
            raise ValueError(f"invalid whence ({whence!r})")
        if pos < 0:
            raise ValueError(f"negative seek position {pos}")
        self._pos = pos
        return pos

    def read_range(self, start: int, end: int) -> bytes:
        """Fetch bytes [start, end) in one request, bypassing the cache."""
        end = min(end, self.size)
        if start >= end:
            return b""

        def fetch() -> bytes:
            self._fh.seek(start)
            return self._fh.read(end - start)

        data = _with_retries(self.location, f"read bytes {start}-{end - 1} of", fetch)
        if len(data) != end - start:
            raise RemoteReadError(
                f"Short read from {self.location}: expected {end - start} bytes "
                f"at offset {start}, got {len(data)}"
            )
        return data

    def _chunk_at(self, pos: int):
        for start, data in self._chunks.items():
            if start <= pos < start + len(data):
                self._chunks.move_to_end(start)
                return start, data
        if pos == self._sequential_end:
            start = pos
            self._fetch_size = min(self._fetch_size * 2, _MAX_FETCH)
            # Reading sequentially: data before the read position is not read again
            for old_start in [s for s, d in self._chunks.items() if s + len(d) <= pos]:
                del self._chunks[old_start]
        else:
            # Aligned, so nearby binary-search probes land in the same cached range,
            # and a file smaller than one fetch is read once
            start = pos - pos % _MIN_FETCH
            self._fetch_size = _MIN_FETCH
        data = self.read_range(start, start + self._fetch_size)
        self._sequential_end = start + len(data)
        self._chunks[start] = data
        while len(self._chunks) > 1 and sum(map(len, self._chunks.values())) > _CACHE_BYTES:
            self._chunks.popitem(last=False)
        return start, data

    def readinto(self, buffer) -> int:  # type: ignore[override]
        if self._pos >= self.size or len(buffer) == 0:
            return 0
        start, data = self._chunk_at(self._pos)
        offset = self._pos - start
        count = min(len(buffer), len(data) - offset)
        buffer[:count] = data[offset : offset + count]
        self._pos += count
        return count

    def close(self) -> None:
        if not self.closed:
            try:
                if self._fh is not None:  # None for an empty object or a failed open
                    self._fh.close()
            finally:
                self._fh = None
                self._chunks.clear()
                super().close()


@overload
def open_input(
    location: str,
    mode: Literal["rb"],
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
) -> BinaryIO: ...


@overload
def open_input(
    location: str,
    mode: Literal["r"] = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
) -> TextIO: ...


def open_input(
    location: str,
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
) -> IO[Any]:
    """
    Open a local path or remote location for reading, like the built-in open().

    Local paths are passed straight to open(). Remote locations are returned as a
    buffered binary file (mode "rb") or a text file (mode "r", UTF-8 by default).
    """
    if mode not in ("r", "rb"):
        raise ValueError(f"open_input only reads; unsupported mode {mode!r}")
    if not is_remote(location):
        # pylint: disable-next=consider-using-with,unspecified-encoding  # caller's choice
        return open(location, mode, buffering=buffering, encoding=encoding, errors=errors)
    raw = RemoteFile(location)
    buffer_size = buffering if buffering > 1 else _BUFFER_SIZE
    buffered = io.BufferedReader(raw, buffer_size=buffer_size)
    if mode == "rb":
        return buffered
    return io.TextIOWrapper(buffered, encoding=encoding or "utf-8", errors=errors)


def exists(location: str) -> bool:
    """
    os.path.exists() for local paths and remote locations.

    Over HTTP(S) an existing object costs one HEAD request; a missing one costs a
    HEAD and a GET, since fsspec retries a failed HEAD as a GET. Only a missing
    remote object gives False; other failures (403, an outage) raise.
    """
    if not is_remote(location):
        return os.path.exists(location)
    fs = _filesystem(_scheme(location) or "")
    try:
        # info() rather than exists(): over HTTP, fsspec's exists() sends a GET
        _with_retries(location, "check", lambda: fs.info(location))
    except FileNotFoundError:
        return False
    return True


def has_wildcards(location: str) -> bool:
    """Whether ``location`` contains glob characters: ``*`` or ``[...]`` (s3:// only)."""
    return any(char in location for char in _WILDCARD_CHARS)


def _s3_filesystem(location: str, action: str) -> Any:
    scheme = _scheme(location)
    if scheme != "s3":
        raise ValueError(
            f"{action} is only supported for s3:// locations, not {scheme}://; "
            f"give the full URL of each file instead: {location}"
        )
    return _filesystem(scheme)


def glob_remote(pattern: str) -> List[str]:
    """Expand an s3:// wildcard pattern to the matching object locations."""
    is_remote(pattern)
    fs = _s3_filesystem(pattern, "Wildcard matching")
    matches = _with_retries(pattern, "list", lambda: fs.glob(pattern))
    return sorted(f"s3://{path}" for path in matches)


def list_remote_files(prefix: str) -> List[str]:
    """List every object under the s3:// ``prefix`` (a "directory" ending in ``/``)."""
    is_remote(prefix)
    fs = _s3_filesystem(prefix, "Directory listing")
    paths = _with_retries(prefix, "list", lambda: fs.find(prefix))
    return sorted(f"s3://{path}" for path in paths)
