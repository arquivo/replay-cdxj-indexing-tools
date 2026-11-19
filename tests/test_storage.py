"""
Tests for storage: remote location parsing, range reads, readahead and retries.

Remote reads go to a local HTTP server (tests/remote_servers.py); no external host
is contacted.
"""

import builtins
import os

import pytest

from replay_cdxj_indexing_tools import storage
from replay_cdxj_indexing_tools.path_safety import resolve_location, url_within
from tests.remote_servers import reset_filesystems, serve_directory


@pytest.fixture(name="no_retry_delay", autouse=True)
def _no_retry_delay(monkeypatch):
    monkeypatch.setattr(storage, "_RETRY_BACKOFF", 0)


@pytest.fixture(name="served")
def _served(tmp_path):
    """A 3 MB file of numbered lines, served over HTTP."""
    pytest.importorskip("fsspec")
    pytest.importorskip("aiohttp")
    data = b"".join(b"line %07d\n" % i for i in range(300_000))
    (tmp_path / "data.txt").write_bytes(data)
    (tmp_path / "small.txt").write_bytes(b"first\nsecond\nthird")
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
        ["https://example.org/index.idx?X-Amz-Signature=abc", "https://example.org/a#frag"],
    )
    def test_query_and_fragment_are_rejected(self, location):
        with pytest.raises(ValueError, match="query string or fragment"):
            storage.is_remote(location)

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

    def test_exists(self, served):
        server, _ = served
        assert storage.exists(server.url("data.txt"))
        assert not storage.exists(server.url("missing.cdxj"))


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

    def test_server_ignoring_ranges_is_an_error_not_a_full_download(self, served):
        server, _ = served
        server.honour_ranges = False
        with storage.RemoteFile(server.url("data.txt")) as remote:
            with pytest.raises(storage.RemoteReadError, match="range"):
                remote.read_range(1_000_000, 1_000_100)

    @pytest.mark.parametrize("value", ["abc", "-1"])
    def test_invalid_retry_setting(self, served, monkeypatch, value):
        server, _ = served
        monkeypatch.setenv("CDXJ_REMOTE_RETRIES", value)
        with pytest.raises(ValueError, match="CDXJ_REMOTE_RETRIES"):
            storage.RemoteFile(server.url("data.txt"))


class TestOptionalDependencies:
    def test_missing_extra_names_the_install_command(self, monkeypatch):
        def fake_import_module(name):
            raise ImportError(f"No module named {name!r}")

        reset_filesystems()
        monkeypatch.setattr("importlib.import_module", fake_import_module)
        try:
            with pytest.raises(ImportError, match=r"replay-cdxj-indexing-tools\[s3\]"):
                storage.open_input("s3://bucket/index.idx", "rb")
        finally:
            monkeypatch.undo()
            reset_filesystems()

    def test_local_paths_do_not_import_fsspec(self, tmp_path, monkeypatch):
        real_import = builtins.__import__

        def no_fsspec(name, *args, **kwargs):
            if name.split(".")[0] in ("fsspec", "aiohttp", "s3fs"):
                raise AssertionError(f"{name} imported for a local path")
            return real_import(name, *args, **kwargs)

        path = tmp_path / "a.cdxj"
        path.write_text("x\n", encoding="utf-8")
        monkeypatch.setattr(builtins, "__import__", no_fsspec)
        with storage.open_input(str(path), "r", encoding="utf-8") as fh:
            assert fh.read() == "x\n"
        assert storage.exists(str(path))
