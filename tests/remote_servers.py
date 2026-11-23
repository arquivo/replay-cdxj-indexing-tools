"""
Local stand-ins for remote index locations, used by the remote-access tests.

RangeHTTPServer serves a directory over HTTP with single byte-range support and
records every request, so tests can count them. S3 is served by moto's local
server. Both listen on 127.0.0.1 only; no test talks to an external host.
"""

import contextlib
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterator, List, Optional, Tuple
from urllib.parse import unquote, urlsplit

import pytest

from replay_cdxj_indexing_tools import storage

_RANGE_RE = re.compile(r"^bytes=(\d+)-(\d*)$")
# AWS settings that would point the S3 tests at another profile, session or endpoint
_INHERITED_AWS_SETTINGS = ("AWS_PROFILE", "AWS_SESSION_TOKEN", "AWS_ENDPOINT_URL")

# Every tool that reads remote inputs (module under replay_cdxj_indexing_tools), with
# arguments that make it read one input: "{cdxj}" is a flat CDXJ, "{idx}" a ZipNum
# .idx, "{blocklist}" a blocklist file and "{out}" an output directory
TOOLS: List[Tuple[str, List[str]]] = [
    ("addfield.addfield_to_flat_cdxj", ["-i", "{cdxj}", "-f", "collection=TEST"]),
    ("export.cdxj_to_csv", ["-i", "{cdxj}"]),
    ("filter.blocklist", ["-i", "{cdxj}", "-b", "{blocklist}"]),
    ("filter.excessive_urls", ["auto", "-i", "{cdxj}", "-n", "30"]),
    ("merge.merge_flat_cdxj", ["-", "{cdxj}"]),
    ("search.cdxj_search", ["--surt", "pt,site07)/", "--matchType", "domain", "{cdxj}"]),
    ("search.extract_field", ["-f", "url", "-i", "{cdxj}"]),
    ("stats.cdxj_stats", ["-i", "{cdxj}", "--sink", "mimetype", "--out-dir", "{out}"]),
    ("zipnum.zipnum_to_flat_cdxj", ["-i", "{idx}"]),
]
TOOL_IDS = [module for module, _ in TOOLS]


def tool_args(args: List[str], **values: str) -> List[str]:
    """A TOOLS argument list with its placeholders filled in."""
    return [arg.format(**values) for arg in args]


class _RangeHandler(BaseHTTPRequestHandler):
    server: "RangeHTTPServer"

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin
        pass  # keep test output clean

    def _file(self) -> Optional[str]:
        rel = unquote(urlsplit(self.path).path).lstrip("/")
        path = os.path.realpath(os.path.join(self.server.root, rel))
        if os.path.commonpath([self.server.root, path]) != self.server.root:
            return None
        return path if os.path.isfile(path) else None

    def _handle(self, send_body: bool) -> None:
        range_header = self.headers.get("Range")
        with self.server.lock:
            self.server.requests.append((self.command, urlsplit(self.path).path, range_header))
            failure = self.server.failures.pop(0) if self.server.failures else None
            if failure is None:
                failure = self.server.statuses.get(urlsplit(self.path).path)
            redirect = self.server.redirects.get(urlsplit(self.path).path)
        if failure is not None:
            self.send_response(failure)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if redirect is not None:
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", redirect)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        path = self._file()
        if path is None:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        size = os.path.getsize(path)
        start, end = 0, size
        status = HTTPStatus.OK
        match = _RANGE_RE.match(range_header or "")
        if match and self.server.honour_ranges:
            start = int(match.group(1))
            end = min(int(match.group(2)) + 1, size) if match.group(2) else size
            if start >= size:
                self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            status = HTTPStatus.PARTIAL_CONTENT

        self.send_response(status)
        length = end - start
        if not send_body:
            length = self.server.head_lengths.get(urlsplit(self.path).path, length)
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes" if self.server.advertise_ranges else "none")
        if status == HTTPStatus.PARTIAL_CONTENT:
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
        self.end_headers()
        if send_body:
            with open(path, "rb") as fh:
                fh.seek(start)
                try:
                    self.wfile.write(fh.read(end - start))
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client stopped reading, e.g. after the bytes it asked for

    def do_HEAD(self):  # pylint: disable=invalid-name
        self._handle(send_body=False)

    def do_GET(self):  # pylint: disable=invalid-name
        self._handle(send_body=True)


class RangeHTTPServer(ThreadingHTTPServer):
    """HTTP server for a local directory, with byte ranges and request recording."""

    daemon_threads = True

    def __init__(self, root: str):
        super().__init__(("127.0.0.1", 0), _RangeHandler)
        self.root = os.path.realpath(root)
        self.lock = threading.Lock()
        self.requests: List[Tuple[str, str, Optional[str]]] = []
        # HTTP status codes returned, in order, instead of the next responses
        self.failures: List[int] = []
        # Request path -> Location of a 302 redirect sent instead of the file
        self.redirects: Dict[str, str] = {}
        # Request path -> HTTP status code always returned for it
        self.statuses: Dict[str, int] = {}
        # Request path -> Content-Length sent in answer to a HEAD, whatever the file
        self.head_lengths: Dict[str, int] = {}
        # Whether Range headers are honoured, and whether Accept-Ranges says so
        self.honour_ranges = True
        self.advertise_ranges = True

    @property
    def base_url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host!s}:{port}"

    def url(self, rel_path: str) -> str:
        return f"{self.base_url}/{rel_path.lstrip('/')}"

    def ranged_gets(self, rel_path: Optional[str] = None) -> List[Optional[str]]:
        """Range headers of the GET requests (for one file, if rel_path is given)."""
        with self.lock:
            return [
                rng
                for method, path, rng in self.requests
                if method == "GET" and (rel_path is None or path == "/" + rel_path.lstrip("/"))
            ]

    def reset(self) -> None:
        with self.lock:
            self.requests.clear()
            self.failures.clear()
            self.redirects.clear()
            self.statuses.clear()
            self.head_lengths.clear()
        self.honour_ranges = True
        self.advertise_ranges = True


@contextlib.contextmanager
def serve_directory(root: str) -> Iterator[RangeHTTPServer]:
    server = RangeHTTPServer(root)
    thread = threading.Thread(target=server.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@contextlib.contextmanager
def s3_server(tmp_dir: str) -> Iterator[Tuple[str, object]]:
    """
    Start moto's S3 server and point the standard AWS settings at it.

    Yields:
        Tuple of (endpoint URL, s3fs filesystem for uploading test data)
    """
    moto_server = pytest.importorskip("moto.server")
    s3fs = pytest.importorskip("s3fs")
    fsspec_config = pytest.importorskip("fsspec.config")

    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint = f"http://{host}:{port}"
    env: Dict[str, str] = {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ENDPOINT_URL_S3": endpoint,
        # Never read the developer's own AWS files
        "AWS_CONFIG_FILE": os.path.join(tmp_dir, "aws-config"),
        "AWS_SHARED_CREDENTIALS_FILE": os.path.join(tmp_dir, "aws-credentials"),
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
    }
    with pytest.MonkeyPatch.context() as mp:
        # Nothing from the developer's own AWS or fsspec settings
        for key in list(os.environ):
            if key.startswith("FSSPEC_") or key in _INHERITED_AWS_SETTINGS:
                mp.delenv(key)
        for protocol in ("s3", "http"):
            mp.delitem(fsspec_config.conf, protocol, raising=False)
        for key, value in env.items():
            mp.setenv(key, value)
        reset_filesystems()
        uploader = s3fs.S3FileSystem(skip_instance_cache=True)
        try:
            yield endpoint, uploader
        finally:
            reset_filesystems()
            server.stop()


def reset_filesystems() -> None:
    """Forget the filesystems storage has created, so they pick up a new environment."""
    with storage._filesystems_lock:
        storage._filesystems.clear()
    try:
        import s3fs  # pylint: disable=import-outside-toplevel
    except ImportError:
        return
    s3fs.S3FileSystem.clear_instance_cache()
