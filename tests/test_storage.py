"""
Tests for storage: remote location parsing, range reads, readahead and retries.

Remote reads go to a local HTTP server (tests/remote_servers.py); no external host
is contacted.
"""

import builtins
import gc
import io
import os
import subprocess
import sys

import pytest

from replay_cdxj_indexing_tools import storage
from replay_cdxj_indexing_tools.path_safety import resolve_location, url_within
from tests.remote_servers import TOOLS, reset_filesystems, serve_directory


@pytest.fixture(name="missing_extras")
def _missing_extras():
    """The optional dependencies for remote access look uninstalled."""

    def fake_import_module(name):
        raise ImportError(f"No module named {name!r}")

    reset_filesystems()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("importlib.import_module", fake_import_module)
        yield
    reset_filesystems()


@pytest.fixture(name="served")
def _served(tmp_path):
    """A 3 MB file of numbered lines, a small and an empty file, served over HTTP."""
    pytest.importorskip("fsspec")
    pytest.importorskip("aiohttp")
    data = b"".join(b"line %07d\n" % i for i in range(300_000))
    (tmp_path / "data.txt").write_bytes(data)
    (tmp_path / "small.txt").write_bytes(b"first\nsecond\nthird")
    (tmp_path / "empty.txt").write_bytes(b"")
    with serve_directory(str(tmp_path)) as server:
        yield server, data


class TestIsRemote:
    @pytest.mark.parametrize(
        "location",
        ["/data/index.cdxj", "index.idx", "./a/b.cdxj", "C:\\data\\index.cdxj", "-", "a:b.cdxj"],
    )
    def test_local_paths(self, location):
        assert storage.is_remote(location) is False

    @pytest.mark.parametrize(
        "location",
        [
            "http://example.org/index.idx",
            "https://example.org/a/b/index.cdxj",
            "HTTPS://example.org/index.idx",
            "s3://bucket/indexes/index.idx",
        ],
    )
    def test_remote_locations(self, location):
        assert storage.is_remote(location) is True

    @pytest.mark.parametrize(
        "location",
        ["file:///etc/passwd", "ftp://example.org/x", "gs://bucket/x", "hdfs://nn/x"],
    )
    def test_unsupported_schemes_are_rejected(self, location):
        with pytest.raises(ValueError, match="Unsupported location scheme"):
            storage.is_remote(location)

    def test_credentials_are_rejected_without_echoing_them(self):
        with pytest.raises(ValueError, match="Credentials are not accepted") as exc_info:
            storage.is_remote("https://user:s3cret@example.org/index.idx")
        assert "s3cret" not in str(exc_info.value)

    @pytest.mark.parametrize(
        "location",
        ["https://example.org/index.idx?X-Amz-Signature=SECRET", "https://example.org/a#SECRET"],
    )
    def test_query_and_fragment_are_rejected(self, location):
        with pytest.raises(ValueError, match="query string or fragment") as exc_info:
            storage.is_remote(location)
        assert "SECRET" not in str(exc_info.value)

    @pytest.mark.parametrize("location", ["https:///index.idx", "s3:///key"])
    def test_missing_host_is_rejected(self, location):
        with pytest.raises(ValueError, match="no host or bucket"):
            storage.is_remote(location)

    def test_backslash_is_rejected(self):
        with pytest.raises(ValueError, match="backslash"):
            storage.is_remote("https://example.org/a\\..\\b")


class TestLocationHelpers:
    def test_parent_location_of_url(self):
        assert storage.parent_location("https://h/a/b/index.idx") == "https://h/a/b"
        assert storage.parent_location("s3://bucket/index.idx") == "s3://bucket"

    def test_parent_location_of_local_path(self, tmp_path):
        path = str(tmp_path / "index.idx")
        assert storage.parent_location(path) == str(tmp_path)

    def test_join_location(self):
        assert storage.join_location("https://h/a/", "x.cdx.gz") == "https://h/a/x.cdx.gz"
        assert storage.join_location("s3://bucket", "x.cdx.gz") == "s3://bucket/x.cdx.gz"
        assert storage.join_location("/data", "x.cdx.gz") == os.path.join("/data", "x.cdx.gz")


class TestUrlContainment:
    BASE = "https://example.org/indexes/2024"

    @pytest.mark.parametrize(
        "location",
        [
            "https://example.org/indexes/2024/shard-01.cdx.gz",
            "https://EXAMPLE.org/indexes/2024/sub/shard.cdx.gz",
            "https://example.org/indexes/2024/./shard.cdx.gz",
        ],
    )
    def test_inside(self, location):
        assert url_within(location, self.BASE) == location

    @pytest.mark.parametrize(
        "location",
        [
            "https://example.org/indexes/2024/../2023/shard.cdx.gz",
            "https://example.org/indexes/2024/%2e%2e/2023/shard.cdx.gz",
            "https://example.org/indexes/2024/%2E%2E%2F2023/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/..;/..;/2023/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/..;a=b/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/%2e%2e;/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/...;/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/..../shard.cdx.gz",
            "https://example.org/indexes/2024/sub/.. /shard.cdx.gz",
            "https://example.org/indexes/2024/sub/..%20/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/..%09/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/%c0%ae%c0%ae/shard.cdx.gz",
            "https://example.org/indexes/2024/sub/%1b%5b2J/shard.cdx.gz",
            "https://example.org/indexes/2024/sub%2fshard.cdx.gz",
            "https://example.org/indexes/2024/sub%2Fshard.cdx.gz",
            "https://example.org/indexes/2024%5c..%5cshard.cdx.gz",
            "https://example.org/indexes/2024-other/shard.cdx.gz",
            "https://example.org/indexes/shard.cdx.gz",
            "https://example.org/indexes/2024",
            "https://other.example/indexes/2024/shard.cdx.gz",
            "https://example.org:8443/indexes/2024/shard.cdx.gz",
            "http://example.org/indexes/2024/shard.cdx.gz",
            "https://169.254.169.254/latest/meta-data/",
        ],
    )
    def test_outside(self, location):
        with pytest.raises(ValueError):
            url_within(location, self.BASE)


class TestS3Containment:
    """S3 reads bucket names and keys literally, so they are compared literally."""

    BASE = "s3://bucket/coll"

    @pytest.mark.parametrize(
        "location", ["s3://bucket/coll/shard.cdx.gz", "s3://bucket/coll/sub/shard.cdx.gz"]
    )
    def test_inside(self, location):
        assert url_within(location, self.BASE) == location
        assert url_within(location, self.BASE + "/") == location

    @pytest.mark.parametrize(
        "location",
        [
            "s3://BUCKET/coll/shard.cdx.gz",
            "s3://Bucket/coll/shard.cdx.gz",
            "s3://other/coll/shard.cdx.gz",
            "s3://bucket/%63oll/shard.cdx.gz",
            "s3://bucket/coll%2fshard.cdx.gz",
            "s3://bucket//coll/shard.cdx.gz",
            "s3://bucket/coll//shard.cdx.gz",
            "s3://bucket/coll/%2fshard.cdx.gz",
            "s3://bucket/coll/sub%2f..%2fshard.cdx.gz",
            "s3://bucket/coll/../shard.cdx.gz",
            "s3://bucket/collx/shard.cdx.gz",
            "s3://bucket/coll",
            "https://bucket/coll/shard.cdx.gz",
        ],
    )
    def test_outside(self, location):
        with pytest.raises(ValueError):
            url_within(location, self.BASE)

    def test_dot_segments_are_dropped_before_the_check(self):
        assert resolve_location("./shard.cdx.gz", self.BASE) == "s3://bucket/coll/shard.cdx.gz"
        assert (
            resolve_location("s3://bucket/coll/./shard.cdx.gz", self.BASE)
            == "s3://bucket/coll/shard.cdx.gz"
        )

    def test_entries_with_a_differently_cased_bucket_are_rejected(self):
        with pytest.raises(ValueError):
            resolve_location("s3://BUCKET/coll/shard.cdx.gz", self.BASE)


class TestResolveLocation:
    def test_relative_entry_joins_remote_base(self):
        assert (
            resolve_location("shard-01.cdx.gz", "https://h/idx") == "https://h/idx/shard-01.cdx.gz"
        )

    def test_absolute_url_inside_remote_base(self):
        url = "s3://bucket/idx/shard-01.cdx.gz"
        assert resolve_location(url, "s3://bucket/idx") == url

    @pytest.mark.parametrize(
        "entry",
        [
            "../secret.cdx.gz",
            "/etc/passwd",
            "https://other.example/idx/shard.cdx.gz",
            "s3://other-bucket/idx/shard.cdx.gz",
            "file:///etc/passwd",
            "https://user:pw@h/idx/shard.cdx.gz",
            "shard.cdx.gz\x00",
        ],
    )
    def test_entries_escaping_remote_base_are_rejected(self, entry):
        with pytest.raises(ValueError):
            resolve_location(entry, "https://h/idx")

    def test_remote_entry_is_rejected_for_local_base(self, tmp_path):
        with pytest.raises(ValueError, match="remote location, outside local base"):
            resolve_location("https://h/idx/shard.cdx.gz", str(tmp_path))

    def test_local_entries_behave_like_resolve_within(self, tmp_path):
        assert resolve_location("a.cdx.gz", str(tmp_path)) == str(tmp_path / "a.cdx.gz")
        with pytest.raises(ValueError):
            resolve_location("../a.cdx.gz", str(tmp_path))
        with pytest.raises(ValueError):
            resolve_location("/etc/passwd", str(tmp_path))

    CONTROLS = [
        "shard\x1b[2J.cdx.gz",
        "shard\r.cdx.gz",
        "shard\x07.cdx.gz",
        "shard\x7f.cdx.gz",
        "shard\x9b2J.cdx.gz",
    ]
    FORMAT_AND_SEPARATORS = [
        "shard\u202egz.xdc.cdx.gz",
        "shard\u2066.cdx.gz",
        "shard\u200b.cdx.gz",
        "shard\u00ad.cdx.gz",
        "shard\u2028.cdx.gz",
        "shard\u2029.cdx.gz",
    ]

    @pytest.mark.parametrize(
        "entry, base",
        [(e, b) for e in CONTROLS for b in ("https://h/idx", "s3://bucket/idx", "local")]
        + [(e, b) for e in FORMAT_AND_SEPARATORS for b in ("https://h/idx", "s3://bucket/idx")],
    )
    def test_control_characters_are_rejected(self, tmp_path, entry, base):
        base_dir = str(tmp_path) if base == "local" else base
        with pytest.raises(ValueError, match="control character") as exc_info:
            resolve_location(entry, base_dir)
        assert entry not in str(exc_info.value)  # shown escaped

    def test_local_names_may_have_format_characters(self, tmp_path):
        # A Persian file name with a zero-width non-joiner, as main resolves it
        name = "\u0645\u06cc\u200c\u0631.cdx.gz"
        assert resolve_location(name, str(tmp_path)) == str(tmp_path / name)

    def test_encoded_slash_in_the_base_is_allowed(self):
        assert (
            resolve_location("index-01.cdx.gz", "http://h/a%2Fb")
            == "http://h/a%2Fb/index-01.cdx.gz"
        )
        assert (
            resolve_location("http://h/a%2Fb/index-01.cdx.gz", "http://h/a%2Fb")
            == "http://h/a%2Fb/index-01.cdx.gz"
        )
        assert resolve_location("x.cdx.gz", "http://h/a%25b") == "http://h/a%25b/x.cdx.gz"

    @pytest.mark.parametrize("base", ["http://h/a%2Fb", "http://h/idx"])
    @pytest.mark.parametrize(
        "entry", ["sub%2fx.cdx.gz", "sub%2Fx.cdx.gz", "%252e%252e/x.cdx.gz", "sub%252fx.cdx.gz"]
    )
    def test_encoded_slash_or_percent_below_the_base_is_rejected(self, entry, base):
        with pytest.raises(ValueError, match="encoded"):
            resolve_location(entry, base)


class TestOpenInput:
    def test_local_path_uses_builtin_open(self, tmp_path):
        path = tmp_path / "a.txt"
        path.write_text("hello\n", encoding="utf-8")
        with storage.open_input(str(path), "r", encoding="utf-8") as fh:
            assert fh.name == str(path)
            assert fh.read() == "hello\n"

    def test_rejects_write_modes(self, tmp_path):
        with pytest.raises(ValueError, match="only reads"):
            storage.open_input(str(tmp_path / "a.txt"), "w")  # type: ignore[call-overload]

    def test_remote_text_lines(self, served):
        server, _ = served
        with storage.open_input(server.url("small.txt"), "r") as fh:
            assert list(fh) == ["first\n", "second\n", "third"]

    def test_remote_binary_seek_and_read_match_local(self, served):
        server, data = served
        with storage.open_input(server.url("data.txt"), "rb") as fh:
            assert fh.seek(0, os.SEEK_END) == len(data)
            for offset in (0, 1, 65_535, 65_536, 1_500_001, len(data) - 3):
                fh.seek(offset)
                assert fh.read(20) == data[offset : offset + 20]
            fh.seek(len(data) + 10)
            assert fh.read(5) == b""

    def test_remote_readline_across_fetch_boundary(self, served):
        server, data = served
        with storage.open_input(server.url("data.txt"), "rb") as fh:
            fh.seek(65_530)
            fh.readline()
            assert fh.readline() == data[65_530:].split(b"\n", 2)[1] + b"\n"

    def test_sequential_read_uses_growing_fetches(self, served):
        server, data = served
        server.reset()
        with storage.open_input(server.url("data.txt"), "r") as fh:
            assert sum(1 for _ in fh) == 300_000
        gets = server.ranged_gets("data.txt")
        # 64 KB doubling up to 4 MB: 64+128+256+512+1024+2048 KB covers the 3 MB file
        assert len(gets) <= 7
        assert all(rng and rng.startswith("bytes=") for rng in gets)

    def test_missing_remote_file(self, served):
        server, _ = served
        with pytest.raises(FileNotFoundError, match="Remote location not found"):
            storage.open_input(server.url("missing.cdxj"), "rb")

    def test_empty_remote_file_reads_as_empty(self, served):
        server, _ = served
        with storage.open_input(server.url("empty.txt"), "rb") as fh:
            assert fh.seek(0, os.SEEK_END) == 0
            fh.seek(0)
            assert fh.read() == b""
        with storage.open_input(server.url("empty.txt"), "r") as fh:
            assert list(fh) == []

    def test_sequential_read_keeps_only_the_last_fetch(self, served):
        server, data = served
        raw = storage.RemoteFile(server.url("data.txt"))
        cached = []
        with io.BufferedReader(raw, buffer_size=64 * 1024) as fh:
            read = b""
            while chunk := fh.read(100_000):
                read += chunk
                cached.append(len(raw._chunks))
        assert read == data
        assert max(cached) == 1

    def test_binary_search_probes_stay_cached(self, served):
        server, data = served
        with storage.open_input(server.url("data.txt"), "rb") as fh:
            for offset in (1_000_000, 1_000_500, 1_000_100):
                fh.seek(offset)
                assert fh.read(10) == data[offset : offset + 10]
        assert len(server.ranged_gets("data.txt")) == 1

    def test_exists(self, served):
        server, _ = served
        assert storage.exists(server.url("data.txt"))
        assert not storage.exists(server.url("missing.cdxj"))

    def test_exists_request_cost(self, served):
        server, _ = served
        server.reset()
        storage.exists(server.url("data.txt"))
        assert [method for method, _, _ in server.requests] == ["HEAD"]
        server.reset()
        storage.exists(server.url("missing.cdxj"))
        assert [method for method, _, _ in server.requests] == ["HEAD", "GET"]


class TestFailedOpen:
    """
    A RemoteFile whose open fails can still be closed, as IOBase.__del__ does when
    the half-built object is finalized (an error there is printed to stderr on
    some Python versions).
    """

    @staticmethod
    def _assert_closes_after_failed_open(location, expected):
        remote = storage.RemoteFile.__new__(storage.RemoteFile)
        with pytest.raises(expected):
            remote.__init__(location)  # pylint: disable=unnecessary-dunder-call
        remote.close()  # what finalization does
        assert remote.closed

    def test_missing_file(self, served):
        server, _ = served
        self._assert_closes_after_failed_open(server.url("missing.cdxj"), FileNotFoundError)

    def test_no_range_support(self, served):
        server, _ = served
        server.honour_ranges = server.advertise_ranges = False
        self._assert_closes_after_failed_open(server.url("data.txt"), storage.RemoteReadError)

    def test_refused_redirect(self, served):
        server, _ = served
        server.redirects["/data.txt"] = "http://127.0.0.2:9/data.txt"  # never contacted
        self._assert_closes_after_failed_open(server.url("data.txt"), storage.RemoteReadError)

    @pytest.mark.usefixtures("missing_extras")
    def test_missing_extras(self):
        self._assert_closes_after_failed_open(
            "s3://bucket/index.idx", storage.MissingDependencyError
        )

    def test_failed_open_reports_nothing_when_collected(self, served, monkeypatch):
        """
        Only detects the bug on Pythons that report errors raised while finalizing
        file objects (recent releases, or development mode); the explicit close()
        tests above guard it on every version.
        """
        server, _ = served
        reported = []
        monkeypatch.setattr(sys, "unraisablehook", reported.append)
        try:
            storage.RemoteFile(server.url("missing.cdxj"))
        except FileNotFoundError:
            pass
        gc.collect()
        assert reported == []

    def test_empty_object_closes_cleanly(self, served):
        server, _ = served
        remote = storage.RemoteFile(server.url("empty.txt"))
        assert remote.read() == b""
        remote.close()
        remote.close()
        assert remote.closed


@pytest.mark.usefixtures("no_retry_delay")
class TestRetries:
    def test_transient_errors_are_retried(self, served):
        server, data = served
        with storage.RemoteFile(server.url("data.txt")) as remote:
            server.failures.extend([503, 502])
            assert remote.read_range(10, 30) == data[10:30]

    def test_open_retries_transient_errors(self, served):
        server, _ = served
        # fsspec tries HEAD then GET for the size, and reports both failing as "not found"
        server.failures.extend([503, 503, 503])
        with storage.RemoteFile(server.url("data.txt")) as remote:
            assert remote.size > 0

    def test_open_reports_an_outage_as_a_read_error_not_as_missing(self, served, monkeypatch):
        server, _ = served
        monkeypatch.setenv("CDXJ_REMOTE_RETRIES", "1")
        server.failures.extend([503] * 4)
        with pytest.raises(storage.RemoteReadError, match="after 2 attempts.*503"):
            storage.RemoteFile(server.url("data.txt"))

    def test_gives_up_after_configured_retries(self, served, monkeypatch):
        server, _ = served
        monkeypatch.setenv("CDXJ_REMOTE_RETRIES", "1")
        with storage.RemoteFile(server.url("data.txt")) as remote:
            server.failures.extend([503, 503, 503])
            with pytest.raises(storage.RemoteReadError, match="after 2 attempts"):
                remote.read_range(0, 10)

    def test_client_errors_are_not_retried(self, served):
        server, _ = served
        with storage.RemoteFile(server.url("data.txt")) as remote:
            server.failures.extend([403, 200])
            with pytest.raises(storage.RemoteReadError) as exc_info:
                remote.read_range(0, 10)
        assert "attempts" not in str(exc_info.value)
        assert server.failures == [200]  # the 403 was not followed by a retry

    def test_server_without_range_support_is_an_error(self, served):
        server, _ = served
        server.honour_ranges = server.advertise_ranges = False
        with pytest.raises(storage.RemoteReadError, match="does not support range requests"):
            storage.RemoteFile(server.url("data.txt"))

    def test_head_claiming_empty_is_not_trusted(self, served):
        # HEAD says 0 bytes, but the GET returns the file: not an empty object
        server, _ = served
        server.head_lengths["/data.txt"] = 0
        with pytest.raises(storage.RemoteReadError, match="does not support range requests"):
            storage.RemoteFile(server.url("data.txt"))

    def test_server_ignoring_ranges_is_an_error_not_a_full_download(self, served):
        server, _ = served
        server.honour_ranges = False
        with storage.RemoteFile(server.url("data.txt")) as remote:
            with pytest.raises(storage.RemoteReadError, match="range"):
                remote.read_range(1_000_000, 1_000_100)

    @pytest.mark.parametrize("value", ["abc", "-1", "1.5", "nan", "inf", "1e308", "21"])
    def test_invalid_retry_setting(self, served, monkeypatch, value):
        server, _ = served
        monkeypatch.setenv("CDXJ_REMOTE_RETRIES", value)
        with pytest.raises(ValueError, match="CDXJ_REMOTE_RETRIES"):
            storage.RemoteFile(server.url("data.txt"))

    @pytest.mark.parametrize("value", ["abc", "0", "nan", "inf", "-inf", "1e308"])
    def test_invalid_timeout_setting(self, served, monkeypatch, value):
        server, _ = served
        monkeypatch.setenv("CDXJ_REMOTE_TIMEOUT", value)
        reset_filesystems()
        try:
            with pytest.raises(ValueError, match="CDXJ_REMOTE_TIMEOUT"):
                storage.RemoteFile(server.url("data.txt"))
        finally:
            reset_filesystems()

    def test_forbidden_at_open_is_a_read_error_not_missing(self, served):
        server, _ = served
        server.statuses["/data.txt"] = 403
        with pytest.raises(storage.RemoteReadError, match="HTTP 403"):
            storage.RemoteFile(server.url("data.txt"))
        with pytest.raises(storage.RemoteReadError, match="HTTP 403"):
            storage.exists(server.url("data.txt"))

    def test_error_text_has_no_query_string(self, served):
        server, _ = served
        with storage.RemoteFile(server.url("data.txt")) as remote:
            server.redirects["/data.txt"] = "/denied.txt?X-Amz-Signature=SECRET"
            server.statuses["/denied.txt"] = 403
            with pytest.raises(storage.RemoteReadError) as exc_info:
                remote.read_range(0, 10)
        assert "HTTP 403" in str(exc_info.value)
        assert "SECRET" not in str(exc_info.value)

    def test_describe_strips_query_strings(self):
        error = OSError("failed: https://h.example/x?X-Amz-Signature=SECRET#frag, retrying")
        text = storage._describe(error)
        assert "SECRET" not in text and "frag" not in text
        assert "https://h.example/x" in text

    @pytest.mark.parametrize(
        "status, expected",
        [(403, "HTTP 403 Forbidden"), (599, "HTTP 599 \\x1b[2J\\x9bserver says")],
    )
    def test_describe_does_not_print_the_servers_reason_raw(self, status, expected):
        class FakeResponseError(Exception):
            def __init__(self):
                super().__init__("ignored")
                self.status = status
                self.message = "\x1b[2J\x9bserver says"

        assert storage._describe(FakeResponseError()) == expected

    def test_describe_escapes_control_characters(self):
        text = storage._describe(OSError("bad \x1b[2J\x9b"))
        assert "\x1b" not in text and "\x9b" not in text


class TestRetryDelay:
    class _HTTPError(Exception):
        def __init__(self, status, headers=None):
            super().__init__(f"HTTP {status}")
            self.status = status
            self.headers = headers or {}

    def _delay(self, error, attempt=0):
        return storage._retry_delay(error, attempt)

    def test_backoff_doubles_with_jitter(self):
        for attempt in range(3):
            base = 0.5 * 2**attempt
            assert base <= self._delay(self._HTTPError(500), attempt) <= base * 1.25

    def test_backoff_is_capped_before_jitter(self):
        assert 30.0 <= self._delay(self._HTTPError(500), 20) <= 30.0 * 1.25

    @pytest.mark.parametrize("status", [429, 503])
    def test_retry_after_seconds_is_honoured(self, status):
        delay = self._delay(self._HTTPError(status, {"Retry-After": "7"}))
        assert 7 <= delay <= 7 * 1.25

    def test_retry_after_is_capped(self):
        delay = self._delay(self._HTTPError(503, {"Retry-After": "86400"}))
        assert 30.0 <= delay <= 30.0 * 1.25

    def test_retry_after_http_date_in_the_past(self):
        error = self._HTTPError(503, {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"})
        assert self._delay(error) <= 0.5 * 1.25

    @pytest.mark.parametrize("value", ["soon", ""])
    def test_unparseable_retry_after_falls_back_to_backoff(self, value):
        assert self._delay(self._HTTPError(503, {"Retry-After": value})) <= 0.5 * 1.25

    def test_retry_after_is_ignored_for_other_statuses(self):
        assert self._delay(self._HTTPError(500, {"Retry-After": "20"})) <= 0.5 * 1.25


class TestRedirects:
    def test_same_origin_redirect_is_followed(self, served):
        server, data = served
        server.redirects["/moved.txt"] = "/data.txt"
        with storage.open_input(server.url("moved.txt"), "rb") as fh:
            fh.seek(1000)
            assert fh.read(20) == data[1000:1020]

    @pytest.mark.parametrize("target", ["other-port", "other-scheme"])
    def test_redirect_to_another_origin_is_refused(self, served, tmp_path, target):
        server, _ = served
        (tmp_path / "other").mkdir()
        (tmp_path / "other" / "secret.txt").write_bytes(b"secret\n")
        with serve_directory(str(tmp_path / "other")) as other:
            location = other.url("secret.txt")
            shown = other.base_url
            if target == "other-scheme":
                location = server.url("data.txt").replace("http://", "https://")
                shown = server.base_url.replace("http://", "https://")
            server.redirects["/data.txt"] = location
            with pytest.raises(storage.RemoteReadError, match="redirected from") as exc_info:
                storage.open_input(server.url("data.txt"), "rb")
            assert other.requests == []
        # The port is shown, so a redirect to another port is not mistaken for none
        assert f"from {server.base_url} to {shown};" in str(exc_info.value)
        assert ("give the https:// URL" in str(exc_info.value)) == (target == "other-scheme")

    def test_redirect_during_a_range_read_is_refused(self, served, tmp_path):
        server, _ = served
        with serve_directory(str(tmp_path)) as other:
            with storage.RemoteFile(server.url("data.txt")) as remote:
                server.redirects["/data.txt"] = other.url("data.txt")
                with pytest.raises(storage.RemoteReadError, match="redirected from"):
                    remote.read_range(0, 10)
            assert other.requests == []

    def test_redirect_loop_is_reported_as_too_many_redirects(self, served):
        server, _ = served
        server.redirects["/loop.txt"] = "/loop.txt"
        with pytest.raises(storage.RemoteReadError, match="too many redirects"):
            storage.open_input(server.url("loop.txt"), "rb")

    def test_invalid_redirect_is_a_read_error_not_missing(self, served):
        server, _ = served
        server.redirects["/data.txt"] = "http://[invalid"
        with pytest.raises(storage.RemoteReadError, match="invalid URL"):
            storage.open_input(server.url("data.txt"), "rb")


class TestHttpClientOptions:
    def test_timeout_and_redirect_guard_are_added_to_configured_options(self):
        aiohttp = pytest.importorskip("aiohttp")
        guard = aiohttp.TraceConfig()
        config = {"http": {"client_kwargs": {"trust_env": True, "trace_configs": [guard]}}}
        options = storage._http_client_kwargs(config)
        assert options["trust_env"] is True
        assert isinstance(options["timeout"], aiohttp.ClientTimeout)
        assert options["trace_configs"][0] is guard and len(options["trace_configs"]) == 2
        assert config["http"]["client_kwargs"] == {"trust_env": True, "trace_configs": [guard]}

    def test_configured_timeout_is_kept(self):
        aiohttp = pytest.importorskip("aiohttp")
        timeout = aiohttp.ClientTimeout(total=5)
        config = {"http": {"client_kwargs": {"timeout": timeout}}}
        options = storage._http_client_kwargs(config)
        assert options["timeout"] is timeout


class TestOptionalDependencies:
    @pytest.mark.usefixtures("missing_extras")
    def test_missing_extra_names_the_install_command(self):
        with pytest.raises(
            storage.MissingDependencyError, match=r"replay-cdxj-indexing-tools\[s3\]"
        ) as exc_info:
            storage.open_input("s3://bucket/index.idx", "rb")
        # The tools report OSError as a one-line error
        assert isinstance(exc_info.value, OSError)

    def test_local_paths_do_not_import_fsspec(self, tmp_path, monkeypatch):
        real_import = builtins.__import__

        def no_fsspec(name, *args, **kwargs):
            if name.split(".")[0] in ("asyncio", "fsspec", "aiohttp", "s3fs"):
                raise AssertionError(f"{name} imported for a local path")
            return real_import(name, *args, **kwargs)

        path = tmp_path / "a.cdxj"
        path.write_text("x\n", encoding="utf-8")
        monkeypatch.setattr(builtins, "__import__", no_fsspec)
        with storage.open_input(str(path), "r", encoding="utf-8") as fh:
            assert fh.read() == "x\n"
        assert storage.exists(str(path))

    def test_importing_the_tools_does_not_import_remote_dependencies(self):
        code = "; ".join(
            ["import sys"]
            + [f"import replay_cdxj_indexing_tools.{module}" for module, _ in TOOLS]
            + [
                "print(sorted(n for n in ('asyncio', 'fsspec', 'aiohttp', 's3fs') "
                "if n in sys.modules))"
            ]
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=60
        )
        assert result.stdout.strip() == "[]"
