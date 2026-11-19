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
so nothing secret ends up in command lines or log messages.

Environment:
    CDXJ_REMOTE_TIMEOUT  Seconds to wait for a connection or for data (default 60)
    CDXJ_REMOTE_RETRIES  Retries after a transient read failure (default 3)
"""

import asyncio
import importlib
import io
import os
import re
import threading
import time
from collections import OrderedDict
from typing import IO, Any, BinaryIO, Dict, List, Literal, Optional, TextIO, overload
from urllib.parse import urlsplit

REMOTE_SCHEMES = ("http", "https", "s3")

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.-]*)://")
_WILDCARD_CHARS = ("*", "?", "[")

# First fetch at a new position; doubled on each sequential fetch up to the maximum
_MIN_FETCH = 64 * 1024
_MAX_FETCH = 4 * 1024 * 1024
# Bytes of fetched data kept per open remote object
_CACHE_BYTES = 2 * _MAX_FETCH
# Read size of the buffered reader in front of a remote object
_BUFFER_SIZE = 64 * 1024

_DEFAULT_TIMEOUT = 60.0
_DEFAULT_RETRIES = 3
_RETRY_BACKOFF = 0.5  # seconds, doubled after each attempt

_EXTRAS = {"http": "http", "https": "http", "s3": "s3"}

_filesystems: Dict[str, Any] = {}
_filesystems_lock = threading.Lock()


class RemoteReadError(OSError):
    """A remote location could not be read, after retrying transient failures."""


def _env_number(name: str, default: float, minimum: float) -> float:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return default
    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {value!r}") from exc
    if number < minimum:
        raise ValueError(f"{name} must be at least {minimum:g}, got {value!r}")
    return number


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
    if parts.query or parts.fragment or "?" in location or "#" in location:
        raise ValueError(f"Remote locations must not have a query string or fragment: {location}")
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
    extra = _EXTRAS[scheme]
    return (
        f"Reading {scheme}:// locations needs the optional dependencies: "
        f'pip install "replay-cdxj-indexing-tools[{extra}]"'
    )


def _filesystem(scheme: str) -> Any:
    """Return the fsspec filesystem used for ``scheme``, created once per process."""
    protocol = "s3" if scheme == "s3" else "http"
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
            raise ImportError(_missing_extra_message(scheme)) from exc

        kwargs: Dict[str, Any] = {}
        if protocol == "http" and "client_kwargs" not in fsspec.config.conf.get("http", {}):
            import aiohttp  # pylint: disable=import-outside-toplevel  # checked above

            timeout = _env_number("CDXJ_REMOTE_TIMEOUT", _DEFAULT_TIMEOUT, 1)
            kwargs["client_kwargs"] = {
                # No total limit: streaming a large file legitimately takes long
                "timeout": aiohttp.ClientTimeout(
                    total=None, sock_connect=timeout, sock_read=timeout
                )
            }
        fs = fsspec.filesystem(protocol, **kwargs)
        _filesystems[protocol] = fs
        return fs


def _is_transient(exc: BaseException) -> bool:
    """Whether a failed remote operation is worth retrying."""
    if isinstance(exc, (FileNotFoundError, PermissionError, IsADirectoryError, NotADirectoryError)):
        return False
    status = getattr(exc, "status", None)  # aiohttp.ClientResponseError
    if isinstance(status, int):
        return status == 429 or status >= 500
    if isinstance(exc, (OSError, asyncio.TimeoutError)):
        return True
    # aiohttp.ClientError subclasses are not all OSError; recognise them by module
    return type(exc).__module__.startswith("aiohttp")


def _underlying_error(exc: BaseException) -> BaseException:
    """
    The error to classify: fsspec reports an HTTP file whose size it cannot get
    as FileNotFoundError, even when the cause was a transient failure.
    """
    cause = exc.__cause__
    if isinstance(exc, FileNotFoundError) and cause is not None and _is_transient(cause):
        return cause
    return exc


def _with_retries(location: str, action: str, func):
    retries = int(_env_number("CDXJ_REMOTE_RETRIES", _DEFAULT_RETRIES, 0))
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
                    f"Could not {action} {location}{tries}: {type(error).__name__}: {error}"
                ) from exc
            time.sleep(_RETRY_BACKOFF * 2**attempt)
    raise AssertionError("unreachable")  # pragma: no cover


class RemoteFile(io.RawIOBase):
    """
    Seekable read-only view of a remote object, fetched with byte-range requests.

    Recently fetched ranges are kept so that the back-and-forth of a binary search
    near its end, or a line that crosses a fetch boundary, needs no new request.
    """

    def __init__(self, location: str):
        super().__init__()
        self.location = location
        scheme = _scheme(location) or ""
        fs = _filesystem(scheme)
        # cache_type="none": every read() is one range request; caching happens here
        self._fh = _with_retries(
            location, "open", lambda: fs.open(location, "rb", cache_type="none")
        )
        size = getattr(self._fh, "size", None)
        if size is None:
            self._fh.close()
            raise RemoteReadError(
                f"Could not open {location}: the server does not support range requests "
                "(it must send Accept-Ranges: bytes and a Content-Length)"
            )
        self.size: int = size
        self._pos = 0
        self._chunks: "OrderedDict[int, bytes]" = OrderedDict()
        self._fetch_size = _MIN_FETCH
        self._sequential_end = -1

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
            self._fetch_size = min(self._fetch_size * 2, _MAX_FETCH)
        else:
            self._fetch_size = _MIN_FETCH
        data = self.read_range(pos, pos + self._fetch_size)
        self._sequential_end = pos + len(data)
        self._chunks[pos] = data
        while len(self._chunks) > 1 and sum(map(len, self._chunks.values())) > _CACHE_BYTES:
            self._chunks.popitem(last=False)
        return pos, data

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
                self._fh.close()
            finally:
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
    """os.path.exists() for local paths and remote locations."""
    if not is_remote(location):
        return os.path.exists(location)
    fs = _filesystem(_scheme(location) or "")
    return bool(_with_retries(location, "check", lambda: fs.exists(location)))


def has_wildcards(location: str) -> bool:
    """Whether ``location`` contains glob characters."""
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
