"""
End-to-end tests: the tools on indexes served over HTTP and S3 give the same
results as on the same index on local disk, with a bounded number of requests.

The HTTP server and the S3 server (moto) are local; see tests/remote_servers.py.
"""

import io
import json
import logging
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
from tests.remote_servers import TOOL_IDS, TOOLS, s3_server, serve_directory, tool_args

pytest.importorskip("fsspec")
pytest.importorskip("aiohttp")

BUCKET = "test-indexes"
MIMES = ["text/html", "image/png", "application/pdf"]


def _write_fixture(root: str) -> None:
    """
    A flat CDXJ (~3 MB), the same records as a multi-shard ZipNum index, and an
    empty flat CDXJ and .idx.
    """
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
    os.makedirs(os.path.join(root, "empty"))
    for name in ("empty.cdxj", "empty.idx"):
        with open(os.path.join(root, "empty", name), "wb"):
            pass


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


def _run_tool(module, *args, cwd=None, prelude=""):
    """Run a tool's main() in a subprocess, as its console script would."""
    code = (
        f"{prelude}import sys; from replay_cdxj_indexing_tools.{module} import main; "
        "sys.exit(main())"
    )
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

    def test_flat_lookup_uses_bounded_range_requests(self, index_root, http_server):
        http_server.reset()
        code, lines, _ = _search(
            "--surt", "pt,site07)/page/0287", http_server.url("flat/index.cdxj")
        )
        assert code == 0 and lines
        size = os.path.getsize(os.path.join(index_root, "flat", "index.cdxj"))
        gets = http_server.ranged_gets("flat/index.cdxj")
        steps = math.ceil(math.log2(size / storage._MIN_FETCH))
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

    @pytest.mark.usefixtures("s3_endpoint")
    def test_s3_prefix_and_wildcard_discovery(self, index_root):
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


@pytest.fixture(name="missing_shard")
def _missing_shard(broken_index):
    """broken_index with its first shard removed; yields (server, URL of that shard)."""
    local_dir, server = broken_index
    shard = sorted(p for p in os.listdir(local_dir) if p.endswith(".cdx.gz"))[0]
    os.remove(local_dir / shard)
    yield server, server.url(f"zipnum/{shard}")


class TestRemoteFailures:
    def test_missing_remote_shard_fails_the_search(self, missing_shard):
        server, shard_url = missing_shard
        code, lines, err = _search(
            "--surt", "pt,site0", "--matchType", "prefix", server.url("zipnum/index.idx")
        )
        assert code == 1
        assert f"Remote location not found: {shard_url}" in err
        assert len(lines) < 100_000  # no claim of completeness

    def test_missing_remote_shard_with_skip_errors(self, missing_shard):
        server, _ = missing_shard
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

    @pytest.mark.usefixtures("no_retry_delay")
    def test_transient_shard_errors_are_retried(self, broken_index):
        _, server = broken_index
        expected = _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx"))
        server.reset()
        server.failures.extend([503, 500])
        assert _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx")) == expected

    def test_forbidden_loc_file_fails_the_search(self, broken_index):
        _, server = broken_index
        server.statuses["/zipnum/index.loc"] = 403
        code, lines, err = _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx"))
        assert code == 1 and lines == []
        assert "HTTP 403" in err

    def test_corrupt_remote_block_fails_the_search(self, broken_index):
        local_dir, server = broken_index
        for shard in local_dir.glob("*.cdx.gz"):
            shard.write_bytes(b"\x00" * shard.stat().st_size)
        local = _search("--surt", "pt,site07)/page/0287", str(local_dir / "index.idx"))
        assert local[:2] == (0, [])  # local blocks that cannot be decompressed are skipped
        code, _, err = _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx"))
        assert code == 1
        assert "Could not decompress the block" in err

    def test_oversized_remote_block_is_rejected_before_any_request(self, broken_index):
        local_dir, server = broken_index
        idx = local_dir / "index.idx"
        rows = [line.split("\t") for line in idx.read_text().splitlines()]
        idx.write_text(
            "".join(f"{k}\t{s}\t{o}\t{20 * 1024 * 1024}\t{n}\n" for k, s, o, _, n in rows)
        )
        code, _, err = _search("--surt", "pt,site07)/page/0287", server.url("zipnum/index.idx"))
        assert code == 1
        assert "remote blocks are limited to" in err
        assert not [path for _, path, _ in server.requests if path.endswith(".cdx.gz")]

    def test_missing_remote_shard_fails_zipnum_to_flat_cdxj(self, missing_shard):
        server, shard_url = missing_shard
        result = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", server.url("zipnum/index.idx"))
        assert result.returncode == 1
        assert result.stderr.decode() == f"Error: Remote location not found: {shard_url}\n"

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


LAYOUTS = ["no-loc", "loc-subdir", "loc-dot", "loc-absolute"]
LOOKUPS = [
    ("--surt", "pt,site07)/page/0287"),
    ("--surt", "pt,site2", "--matchType", "prefix"),
]


def _make_layout(src: str, dest, layout: str, base_url: str) -> None:
    """Copy the ZipNum index to dest, with shards located as the layout says."""
    shutil.copytree(src, dest)
    loc_path = dest / "index.loc"
    names = [line.split("\t")[0] for line in loc_path.read_text().splitlines()]
    if layout == "no-loc":
        loc_path.unlink()
        return
    if layout == "loc-subdir":
        (dest / "shards").mkdir()
        for name in names:
            (dest / name).rename(dest / "shards" / name)
    prefix = {"loc-subdir": "shards/", "loc-dot": "./", "loc-absolute": f"{base_url}/"}[layout]
    loc_path.write_text("".join(f"{name}\t{prefix}{name}\n" for name in names))


class TestRelativeShardLocations:
    """Shards are found relative to the .idx, with or without a .loc file."""

    @pytest.mark.parametrize("query", LOOKUPS, ids=lambda q: " ".join(q))
    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_http(self, index_root, tmp_path, layout, query):
        expected = _search(*query, "--sort", f"{index_root}/zipnum/index.idx")
        with serve_directory(str(tmp_path)) as server:
            _make_layout(f"{index_root}/zipnum", tmp_path / "zn", layout, server.url("zn"))
            assert _search(*query, "--sort", server.url("zn/index.idx")) == expected

    @pytest.mark.usefixtures("s3_endpoint")
    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_s3(self, index_root, tmp_path, layout):
        s3fs = pytest.importorskip("s3fs")
        base = f"s3://{BUCKET}/layout-{layout}"
        _make_layout(f"{index_root}/zipnum", tmp_path / "zn", layout, base)
        s3fs.S3FileSystem(skip_instance_cache=True).put(
            str(tmp_path / "zn") + "/", base + "/", recursive=True
        )
        for query in LOOKUPS:
            expected = _search(*query, "--sort", f"{index_root}/zipnum/index.idx")
            assert _search(*query, "--sort", f"{base}/index.idx") == expected

    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_zipnum_to_flat_cdxj_http(self, index_root, tmp_path, layout):
        with serve_directory(str(tmp_path)) as server:
            _make_layout(f"{index_root}/zipnum", tmp_path / "zn", layout, server.url("zn"))
            result = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", server.url("zn/index.idx"))
        assert result.returncode == 0, result.stderr.decode()
        assert result.stderr == b""
        with open(_flat(index_root), "rb") as fh:
            assert result.stdout == fh.read()

    @pytest.mark.parametrize("layout", ["no-loc", "loc-dot"])
    def test_lookup_request_budget(self, index_root, tmp_path, layout):
        with serve_directory(str(tmp_path)) as server:
            _make_layout(f"{index_root}/zipnum", tmp_path / "zn", layout, server.url("zn"))
            code, lines, _ = _search("--surt", "pt,site07)/page/0287", server.url("zn/index.idx"))
            assert code == 0 and lines
            requests = list(server.requests)
        loc = [r for r in requests if r[1].endswith(".loc")]
        idx = [r for r in requests if r[1].endswith(".idx")]
        shard = [r for r in requests if r[1].endswith(".cdx.gz")]
        # .loc: HEAD + GET to read it; when absent, fsspec retries the 404 HEAD as a GET
        assert len(loc) == 2
        # .idx: one HEAD for its size, then the binary search
        idx_size = os.path.getsize(f"{index_root}/zipnum/index.idx")
        assert len(idx) <= 2 + math.ceil(math.log2(max(2, idx_size / storage._MIN_FETCH)))
        # Shard: the matching block only; its byte range comes from the .idx, so no HEAD
        assert [method for method, _, _ in shard] == ["GET"]


def _flat(index_root, name="index.cdxj"):
    return os.path.join(index_root, "flat", name)


def _run_listed_tool(module, args, tmp_path, kind, cdxj, idx, prelude=""):
    """
    Run a tool from TOOLS on the given inputs, with its output directory under
    tmp_path/kind; return (result, files written to the output directory).
    """
    blocklist = tmp_path / "blocklist.txt"
    blocklist.write_text("^pt,site1\n", encoding="utf-8")
    out_dir = tmp_path / kind / "out"
    argv = tool_args(args, cdxj=cdxj, idx=idx, blocklist=str(blocklist), out=str(out_dir))
    result = _run_tool(module, *argv, prelude=prelude)
    files = {p.name: p.read_bytes() for p in out_dir.iterdir()} if out_dir.is_dir() else {}
    return result, files


class TestRemoteStreamingTools:
    @pytest.mark.parametrize("module, args", TOOLS, ids=TOOL_IDS)
    def test_same_output_as_local(self, index_root, remote_root, tmp_path, module, args):
        local, local_files = _run_listed_tool(
            module, args, tmp_path, "local", _flat(index_root), f"{index_root}/zipnum/index.idx"
        )
        remote, remote_files = _run_listed_tool(
            module,
            args,
            tmp_path,
            "remote",
            f"{remote_root}/flat/index.cdxj",
            f"{remote_root}/zipnum/index.idx",
        )
        assert local.returncode == 0, local.stderr.decode()
        assert remote.returncode == 0, remote.stderr.decode()
        assert local.stdout or local_files
        assert (remote.stdout, remote_files) == (local.stdout, local_files)

    def test_zipnum_to_flat_cdxj_restores_the_flat_index(self, index_root, remote_root):
        remote = _run_tool("zipnum.zipnum_to_flat_cdxj", "-i", f"{remote_root}/zipnum/index.idx")
        assert remote.returncode == 0, remote.stderr.decode()
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
        assert (local.returncode, remote.returncode) == (0, 0), remote.stderr.decode()
        assert remote.stdout == local.stdout

    def test_merge_flat_cdxj_interleaves_remote_inputs(self, index_root, remote_root):
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
        assert (local.returncode, remote.returncode) == (0, 0), remote.stderr.decode()
        assert remote.stdout == local.stdout

    def test_filter_blocklist_counts(self, index_root, remote_root, tmp_path):
        blocklist = tmp_path / "blocklist.txt"
        blocklist.write_text("^pt,site1\n", encoding="utf-8")
        local = _run_tool("filter.blocklist", "-i", _flat(index_root), "-b", str(blocklist), "-v")
        remote = _run_tool(
            "filter.blocklist", "-i", f"{remote_root}/flat/index.cdxj", "-b", str(blocklist), "-v"
        )
        assert remote.stdout == local.stdout
        # Same kept/blocked counts; only the input name differs
        remote_url = f"{remote_root}/flat/index.cdxj".encode()
        assert remote.stderr.replace(remote_url, b"INPUT") == local.stderr.replace(
            _flat(index_root).encode(), b"INPUT"
        )

    def test_filter_blocklist_to_an_output_file(self, index_root, remote_root, tmp_path):
        blocklist = tmp_path / "blocklist.txt"
        blocklist.write_text("^pt,site1\n", encoding="utf-8")
        local = _run_tool("filter.blocklist", "-i", _flat(index_root), "-b", str(blocklist))
        out = tmp_path / "kept.cdxj"
        remote = _run_tool(
            "filter.blocklist",
            "-i",
            f"{remote_root}/flat/index.cdxj",
            "-b",
            str(blocklist),
            "-o",
            str(out),
        )
        assert remote.returncode == 0, remote.stderr.decode()
        assert out.read_bytes() == local.stdout
        assert remote.stderr == local.stderr


class TestEmptyRemoteInputs:
    """A 0-byte remote file gives the same result as a 0-byte local file."""

    def test_cdxj_search_on_an_empty_idx(self, index_root, remote_root):
        # The per-tool test below covers an empty flat CDXJ
        local = _search("--surt", "pt,site07)/", os.path.join(index_root, "empty", "empty.idx"))
        remote = _search("--surt", "pt,site07)/", f"{remote_root}/empty/empty.idx")
        assert remote == local
        assert local[0] == 0

    @pytest.mark.parametrize("module, args", TOOLS, ids=TOOL_IDS)
    def test_every_tool(self, index_root, remote_root, tmp_path, module, args):
        local, local_files = _run_listed_tool(
            module,
            args,
            tmp_path,
            "local",
            f"{index_root}/empty/empty.cdxj",
            f"{index_root}/empty/empty.idx",
        )
        remote, remote_files = _run_listed_tool(
            module,
            args,
            tmp_path,
            "remote",
            f"{remote_root}/empty/empty.cdxj",
            f"{remote_root}/empty/empty.idx",
        )
        assert (remote.returncode, remote.stdout, remote_files) == (
            local.returncode,
            local.stdout,
            local_files,
        )
        assert b"range requests" not in remote.stderr


class TestCleanErrors:
    """Remote-access failures end every tool with an error message, not a traceback."""

    # Tools whose whole error output is a single "Error: ..." line
    ONE_LINE = ("filter.blocklist", "merge.merge_flat_cdxj", "zipnum.zipnum_to_flat_cdxj")

    def _run(self, tmp_path, module, args, source, prelude=""):
        result, _ = _run_listed_tool(module, args, tmp_path, "run", source, source, prelude)
        stderr = result.stderr.decode()
        assert result.returncode != 0
        assert "Traceback" not in stderr
        assert "Error" in stderr
        return stderr

    @pytest.mark.parametrize("module, args", TOOLS, ids=TOOL_IDS)
    def test_missing_optional_dependencies(self, tmp_path, module, args):
        stderr = self._run(
            tmp_path,
            module,
            args,
            "http://127.0.0.1:9/index.cdxj",  # never contacted
            prelude="import sys; sys.modules['fsspec'] = None; ",
        )
        assert 'pip install "replay-cdxj-indexing-tools[http]"' in stderr

    @pytest.mark.parametrize("module, args", TOOLS, ids=TOOL_IDS)
    def test_unsupported_scheme(self, tmp_path, module, args):
        stderr = self._run(tmp_path, module, args, "ftp://example.org/index.cdxj")
        assert "Unsupported location scheme ftp://" in stderr

    @pytest.mark.parametrize("module, args", TOOLS, ids=TOOL_IDS)
    def test_missing_remote_input(self, tmp_path, http_server, module, args):
        url = http_server.url("flat/missing.cdxj")
        stderr = self._run(tmp_path, module, args, url)
        assert f"Remote location not found: {url}" in stderr
        if module in self.ONE_LINE:
            assert stderr == f"Error: Remote location not found: {url}\n"
