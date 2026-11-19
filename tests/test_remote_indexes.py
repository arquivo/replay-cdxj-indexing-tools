"""
End-to-end tests: the tools on indexes served over HTTP and S3 give the same
results as on the same index on local disk, with a bounded number of requests.

The HTTP server and the S3 server (moto) are local; see tests/remote_servers.py.
"""

import io
import json
import math
import os
import shutil
import subprocess
import sys
from contextlib import redirect_stderr, redirect_stdout

import pytest

from replay_cdxj_indexing_tools import storage
from replay_cdxj_indexing_tools.search import cdxj_search
from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import cdxj_to_zipnum
from tests.remote_servers import s3_server, serve_directory

pytest.importorskip("fsspec")
pytest.importorskip("aiohttp")

BUCKET = "test-indexes"
MIMES = ["text/html", "image/png", "application/pdf"]


def _write_fixture(root: str) -> None:
    """A flat CDXJ (~3 MB) and the same records as a multi-shard ZipNum index."""
    lines = []
    for i in range(30_000):
        domain = f"pt,site{i % 40:02d}"
        host = f"{domain},www" if i % 3 == 0 else domain
        surt = f"{host})/page/{i % 997:04d}"
        record = {
            "url": f"https://{'www.' if i % 3 == 0 else ''}site{i % 40:02d}.pt/page/{i % 997:04d}",
            "mime": MIMES[i % len(MIMES)],
            "status": "200" if i % 7 else "404",
            "digest": f"sha1:{i:032d}",
        }
        lines.append(f"{surt} 20{10 + i % 15}0101{i % 24:02d}0000 {json.dumps(record)}\n")
    lines.sort()
    os.makedirs(os.path.join(root, "flat"))
    with open(os.path.join(root, "flat", "index.cdxj"), "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    half = len(lines) // 2
    with open(os.path.join(root, "flat", "part-a.cdxj"), "w", encoding="utf-8") as fh:
        fh.writelines(lines[:half])
    with open(os.path.join(root, "flat", "part-b.cdxj"), "w", encoding="utf-8") as fh:
        fh.writelines(lines[half:])
    with redirect_stdout(io.StringIO()):
        cdxj_to_zipnum(
            os.path.join(root, "zipnum"),
            os.path.join(root, "flat", "index.cdxj"),
            shard_size_mb=0.05,
            chunk_size=50,
            base="index",
        )


@pytest.fixture(name="index_root", scope="module")
def _index_root(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("indexes"))
    _write_fixture(root)
    return root


@pytest.fixture(name="http_server", scope="module")
def _http_server(index_root):
    with serve_directory(index_root) as server:
        yield server


@pytest.fixture(name="s3_endpoint", scope="module")
def _s3_endpoint(index_root, tmp_path_factory):
    import logging  # pylint: disable=import-outside-toplevel

    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    with s3_server(str(tmp_path_factory.mktemp("aws"))) as (endpoint, uploader):
        uploader.mkdir(BUCKET)
        uploader.put(index_root + "/", f"{BUCKET}/", recursive=True)
        yield endpoint


@pytest.fixture(name="remote_root", params=["http", "s3"])
def _remote_root(request, index_root):
    """Base URL of the fixture indexes, once over HTTP and once over S3."""
    if request.param == "http":
        server = request.getfixturevalue("http_server")
        server.reset()
        return server.base_url
    request.getfixturevalue("s3_endpoint")
    return f"s3://{BUCKET}"


def _search(*argv):
    """Run cdxj-search in-process; return (exit code, stdout lines, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with redirect_stdout(out), redirect_stderr(err):
        old_argv = sys.argv
        sys.argv = ["cdxj-search", *argv]
        try:
            cdxj_search.main()
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
        finally:
            sys.argv = old_argv
    return code, out.getvalue().splitlines(), err.getvalue()


def _run_tool(module, *args, cwd=None):
    """Run a tool's main() in a subprocess, as its console script would."""
    code = f"import sys; from replay_cdxj_indexing_tools.{module} import main; sys.exit(main())"
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        capture_output=True,
        check=False,
        cwd=cwd,
        timeout=120,
    )


SEARCHES = [
    ("--surt", "pt,site07)/page/0287"),
    ("--url", "https://site07.pt/page/0287"),
    ("--url", "https://site11.pt/", "--matchType", "domain"),
    ("--url", "https://site11.pt/", "--matchType", "subdomains"),
    ("--surt", "pt,site2", "--matchType", "prefix"),
    ("--surt", "pt,site03)/page/09", "--matchType", "prefix", "--filter", "status=404"),
    ("--surt", "pt,nonexistent)/", "--matchType", "prefix"),
    ("--surt", "zz,after,everything)/"),
    ("--surt", "aa,before,everything)/"),
]


class TestRemoteSearch:
    @pytest.mark.parametrize("query", SEARCHES, ids=lambda q: " ".join(q))
    @pytest.mark.parametrize("index", ["zipnum/index.idx", "flat/index.cdxj"])
    def test_same_results_as_local(self, index_root, remote_root, index, query):
        local = _search(*query, "--sort", os.path.join(index_root, index))
        remote = _search(*query, "--sort", f"{remote_root}/{index}")
        assert remote == local
        assert local[0] == 0

    def test_prefix_search_finds_many_records(self, index_root):
        code, lines, _ = _search(
            "--surt", "pt,site2", "--matchType", "prefix", f"{index_root}/zipnum/index.idx"
        )
        assert code == 0 and len(lines) > 5000  # spans many blocks and several shards

    def test_flat_lookup_uses_bounded_range_requests(self, index_root, http_server):
        http_server.reset()
        code, lines, _ = _search(
            "--surt", "pt,site07)/page/0287", http_server.url("flat/index.cdxj")
        )
        assert code == 0 and lines
        size = os.path.getsize(os.path.join(index_root, "flat", "index.cdxj"))
        gets = http_server.ranged_gets("flat/index.cdxj")
        steps = math.ceil(math.log2(size / storage._MIN_FETCH))  # pylint: disable=protected-access
        assert 0 < len(gets) <= 2 * steps + 4
        assert all(rng and rng.startswith("bytes=") for rng in gets)

    def test_zipnum_lookup_fetches_one_block(self, http_server):
        http_server.reset()
        code, lines, _ = _search(
            "--surt", "pt,site07)/page/0287", http_server.url("zipnum/index.idx")
        )
        assert code == 0 and lines
        shard_gets = [
            rng
            for _, path, rng in http_server.requests
            if path.endswith(".cdx.gz") and rng is not None
        ]
        assert len(shard_gets) == 1

    def test_adjacent_blocks_are_fetched_together(self, index_root, http_server):
        http_server.reset()
        code, lines, _ = _search(
            "--surt", "pt,site2", "--matchType", "prefix", http_server.url("zipnum/index.idx")
        )
        assert code == 0 and len(lines) > 5000
        shards = [name for name in os.listdir(f"{index_root}/zipnum") if name.endswith(".cdx.gz")]
        shard_gets = [
            path
            for method, path, rng in http_server.requests
            if method == "GET" and ".cdx.gz" in path
        ]
        # One request per shard touched, not one per block
        assert len(shard_gets) == len(set(shard_gets)) <= len(shards)

    def test_s3_prefix_and_wildcard_discovery(self, index_root, s3_endpoint):
        # pylint: disable=unused-argument  # s3_endpoint starts the server
        local = _search("--surt", "pt,site07)/page/0287", f"{index_root}/flat/index.cdxj")
        by_glob = _search("--surt", "pt,site07)/page/0287", f"s3://{BUCKET}/flat/index.*")
        assert by_glob == local
        # A prefix lists every index under it: 3 flat files and the ZipNum .idx
        _, by_prefix, _ = _search("--surt", "pt,site07)/page/0287", f"s3://{BUCKET}/")
        assert sorted(by_prefix) == sorted(local[1] * 3)

    def test_wildcards_over_http_are_rejected(self, http_server):
        code, _, err = _search("--surt", "pt,a)/", http_server.url("flat/*.cdxj"))
        assert code == 1
        assert "only supported for s3://" in err


@pytest.fixture(name="broken_index")
def _broken_index(index_root, tmp_path):
    """A copy of the ZipNum index, served over HTTP, that tests can damage."""
    shutil.copytree(f"{index_root}/zipnum", tmp_path / "zipnum")
    with serve_directory(str(tmp_path)) as server:
        yield tmp_path / "zipnum", server


class TestRemoteFailures:
    def test_missing_remote_shard_fails_the_search(self, broken_index):
        local_dir, server = broken_index
        shards = sorted(p for p in os.listdir(local_dir) if p.endswith(".cdx.gz"))
        os.remove(local_dir / shards[0])
        code, lines, err = _search(
            "--surt", "pt,site0", "--matchType", "prefix", server.url("zipnum/index.idx")
        )
        assert code == 1
        assert f"Remote location not found: {server.url('zipnum/' + shards[0])}" in err
        assert len(lines) < 100_000  # no claim of completeness

    def test_missing_remote_shard_with_skip_errors(self, broken_index):
        local_dir, server = broken_index
        shards = sorted(p for p in os.listdir(local_dir) if p.endswith(".cdx.gz"))
        os.remove(local_dir / shards[0])
        code, _, err = _search(
            "--surt",
            "pt,site0",
            "--matchType",
            "prefix",
            "--skip-errors",
            server.url("zipnum/index.idx"),
        )
        assert code == 0
        assert "Remote location not found" in err

    def test_transient_shard_errors_are_retried(self, broken_index, monkeypatch):
        monkeypatch.setattr(storage, "_RETRY_BACKOFF", 0)
        _, server = broken_index
        expected = _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx"))
        server.reset()
        server.failures.extend([503, 500])
        assert _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx")) == expected

    @pytest.mark.parametrize(
        "loc_entry",
        [
            "{other}/zipnum/{shard}",
            "../{shard}",
            "/etc/passwd",
            "%2e%2e/{shard}",
            "file:///etc/passwd",
        ],
    )
    def test_loc_entries_cannot_leave_the_index_location(self, broken_index, loc_entry):
        local_dir, server = broken_index
        with serve_directory(str(local_dir.parent)) as other:
            loc_path = local_dir / "index.loc"
            rows = [line.split("\t") for line in loc_path.read_text().splitlines()]
            shard = rows[0][0]
            target = loc_entry.format(other=other.base_url, shard=shard)
            rows[0][1] = target
            loc_path.write_text("".join(f"{name}\t{path}\n" for name, path in rows))

            code, _, err = _search(
                "--surt", "pt,site0", "--matchType", "prefix", server.url("zipnum/index.idx")
            )
            assert code == 1
            assert "Path traversal detected in .loc file" in err
            assert other.requests == []


def _flat(index_root, name="index.cdxj"):
    return os.path.join(index_root, "flat", name)


class TestRemoteStreamingTools:
    def _assert_same(self, local, remote):
        assert local.returncode == 0, local.stderr.decode()
        assert remote.returncode == 0, remote.stderr.decode()
        assert remote.stdout == local.stdout

    def test_zipnum_to_flat_cdxj(self, index_root, remote_root):
        local = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", f"{index_root}/zipnum/index.idx")
        remote = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", f"{remote_root}/zipnum/index.idx")
        self._assert_same(local, remote)
        with open(_flat(index_root), "rb") as fh:
            assert remote.stdout == fh.read()

    def test_zipnum_to_flat_cdxj_with_local_idx_and_remote_base_dir(self, index_root, remote_root):
        local = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", f"{index_root}/zipnum/index.idx")
        remote = _run_tool(
            "zipnum.zipnum_to_flat_cdxj",
            "-i",
            f"{index_root}/zipnum/index.idx",
            "--loc",
            f"{remote_root}/zipnum/index.loc",
            "--base-dir",
            f"{remote_root}/zipnum",
        )
        self._assert_same(local, remote)

    def test_merge_flat_cdxj(self, index_root, remote_root):
        local = _run_tool(
            "merge.merge_flat_cdxj",
            "-",
            _flat(index_root, "part-b.cdxj"),
            _flat(index_root, "part-a.cdxj"),
        )
        remote = _run_tool(
            "merge.merge_flat_cdxj",
            "-",
            f"{remote_root}/flat/part-b.cdxj",
            f"{remote_root}/flat/part-a.cdxj",
        )
        self._assert_same(local, remote)

    def test_filter_blocklist(self, index_root, remote_root, tmp_path):
        blocklist = tmp_path / "blocklist.txt"
        blocklist.write_text("^pt,site1\n", encoding="utf-8")
        local = _run_tool("filter.blocklist", "-i", _flat(index_root), "-b", str(blocklist), "-v")
        remote = _run_tool(
            "filter.blocklist", "-i", f"{remote_root}/flat/index.cdxj", "-b", str(blocklist), "-v"
        )
        self._assert_same(local, remote)
        # Same kept/blocked counts; only the input name differs
        remote_url = f"{remote_root}/flat/index.cdxj".encode()
        assert remote.stderr.replace(remote_url, b"INPUT") == local.stderr.replace(
            _flat(index_root).encode(), b"INPUT"
        )

    def test_filter_excessive_urls(self, index_root, remote_root):
        local = _run_tool("filter.excessive_urls", "auto", "-i", _flat(index_root), "-n", "30")
        remote = _run_tool(
            "filter.excessive_urls", "auto", "-i", f"{remote_root}/flat/index.cdxj", "-n", "30"
        )
        self._assert_same(local, remote)

    def test_cdxj_extract_field(self, index_root, remote_root):
        local = _run_tool("search.extract_field", "-f", "url", "-i", _flat(index_root))
        remote = _run_tool(
            "search.extract_field", "-f", "url", "-i", f"{remote_root}/flat/index.cdxj"
        )
        self._assert_same(local, remote)

    def test_cdxj_to_csv(self, index_root, remote_root):
        local = _run_tool("export.cdxj_to_csv", "-i", _flat(index_root))
        remote = _run_tool("export.cdxj_to_csv", "-i", f"{remote_root}/flat/index.cdxj")
        self._assert_same(local, remote)

    def test_addfield_to_flat_cdxj(self, index_root, remote_root):
        local = _run_tool(
            "addfield.addfield_to_flat_cdxj", "-i", _flat(index_root), "-f", "collection=TEST"
        )
        remote = _run_tool(
            "addfield.addfield_to_flat_cdxj",
            "-i",
            f"{remote_root}/flat/index.cdxj",
            "-f",
            "collection=TEST",
        )
        self._assert_same(local, remote)

    def test_cdxj_stats(self, index_root, remote_root, tmp_path):
        outputs = {}
        for kind, source in (
            ("local", _flat(index_root)),
            ("remote", f"{remote_root}/flat/index.cdxj"),
        ):
            out_dir = tmp_path / kind
            result = _run_tool(
                "stats.cdxj_stats", "-i", source, "--sink", "mimetype", "--out-dir", str(out_dir)
            )
            assert result.returncode == 0, result.stderr.decode()
            outputs[kind] = {p.name: p.read_bytes() for p in out_dir.iterdir()}
        assert outputs["remote"] == outputs["local"]
        assert outputs["local"]

    def test_missing_remote_input_is_a_clear_error(self, remote_root):
        result = _run_tool(
            "search.extract_field", "-f", "url", "-i", f"{remote_root}/flat/missing.cdxj"
        )
        assert result.returncode != 0
        assert b"Remote location not found" in result.stderr
