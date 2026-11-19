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
        if failure is not None:
            self.send_response(failure)
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
        self.send_header("Content-Length", str(end - start))
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
    with storage._filesystems_lock:  # pylint: disable=protected-access
        storage._filesystems.clear()  # pylint: disable=protected-access
    try:
        import s3fs  # pylint: disable=import-outside-toplevel
    except ImportError:
        return
    s3fs.S3FileSystem.clear_instance_cache()
