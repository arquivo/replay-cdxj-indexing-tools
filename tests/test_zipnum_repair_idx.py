#!/usr/bin/env python3
"""
test_zipnum_repair_idx.py - Test suite for zipnum-repair-idx
============================================================

Builds a ZipNum, truncates the .idx keys the way flat-cdxj-to-zipnum 1.3.0 and
earlier did (at the first '{' of the line), and checks the repair restores the
.idx byte for byte from the shards.
"""

import os

import pytest

from replay_cdxj_indexing_tools.zipnum.flat_cdxj_to_zipnum import cdxj_to_zipnum
from replay_cdxj_indexing_tools.zipnum.repair_idx import main, repair_idx

LINES = sorted(
    [
        b'com,example)/-/abc 20200101000000 {"url": "http://example.com/-/abc"}\n',
        b"com,example)/-/{{avatar_url}} 20250304171133 "
        b'{"url": "http://example.com/-/%7B%7Bavatar_url%7D%7D"}\n',
        b"com,example)/-/{{avatar_url}} 20250304171134 "
        b'{"url": "http://example.com/-/%7B%7Bavatar_url%7D%7D"}\n',
        b'com,example)/?s=foo 20200101000000 {"url": "http://example.com/?s=foo"}\n',
        b"com,example)/?s={search_term_string} 20200101000000 "
        b'{"url": "http://example.com/?s=%7Bsearch_term_string%7D"}\n',
        b"pt,caf\xe9)/{x} 20200101000000 " b'{"url": "http://caf\xe9.pt/%7Bx%7D"}\n',
        b'pt,caf\xe9)/men\xfa 20200101000000 {"url": "http://caf\xe9.pt/men\xfa"}\n',
    ]
)

# Lines whose URL key contains '{'
TRUNCATED = 4


def truncate_key(idx_line: bytes) -> bytes:
    """Cut the key at the first '{', as flat-cdxj-to-zipnum 1.3.0 and earlier did."""
    key, rest = idx_line.split(b"\t", 1)
    if b"{" in key:
        key = key[: key.index(b"{")].strip()
    return key + b"\t" + rest


def unsorted_count(idx_data: bytes) -> int:
    keys = [line.split(b"\t", 1)[0] + b"\t" for line in idx_data.splitlines()]
    return sum(1 for a, b in zip(keys, keys[1:]) if b < a)


@pytest.fixture(name="zipnum")
def fixture_zipnum(tmp_path):
    """A multi-shard ZipNum of LINES with one line per block; returns (idx path, good idx)."""
    input_file = tmp_path / "input.cdxj"
    input_file.write_bytes(b"".join(LINES))
    out = tmp_path / "zipnum"
    # One worker: few chunks are left in the compression queue at the end, where the
    # builder does not rotate shards, so the blocks really spread over several shards
    cdxj_to_zipnum(
        str(out), str(input_file), shard_size_mb=0.0001, chunk_size=1, base="t", workers=1
    )
    idx_path = out / "t.idx"
    good = idx_path.read_bytes()
    assert len({line.split(b"\t")[1] for line in good.splitlines()}) > 1, "needs >1 shard"
    return idx_path, good


def write_truncated(idx_path, good):
    broken = b"".join(truncate_key(line) for line in good.splitlines(keepends=True))
    idx_path.write_bytes(broken)
    return broken


def test_repairs_truncated_keys(zipnum, tmp_path):
    idx_path, good = zipnum
    broken = write_truncated(idx_path, good)
    assert unsorted_count(broken) > 0

    fixed = tmp_path / "fixed.idx"
    repaired, unsorted = repair_idx(str(idx_path), str(fixed))

    assert fixed.read_bytes() == good
    assert (repaired, unsorted) == (TRUNCATED, 0)
    assert unsorted_count(good) == 0


def test_main_repairs_in_place(zipnum, capsys):
    idx_path, good = zipnum
    write_truncated(idx_path, good)

    main(["-i", str(idx_path), "-o", str(idx_path)])

    assert idx_path.read_bytes() == good
    assert f"Repaired {TRUNCATED} key(s)" in capsys.readouterr().err
    assert not [f for f in os.listdir(idx_path.parent) if f.endswith(".tmp")]


def test_correct_keys_are_copied_without_reading_shards(zipnum, tmp_path):
    """Keys that have a space are not looked up, so the shards are not needed."""
    idx_path, good = zipnum
    for name in os.listdir(idx_path.parent):
        if name.endswith(".cdx.gz"):
            os.unlink(idx_path.parent / name)

    fixed = tmp_path / "fixed.idx"
    assert repair_idx(str(idx_path), str(fixed)) == (0, 0)
    assert fixed.read_bytes() == good


def test_unsorted_result_exits_non_zero(zipnum, capsys):
    idx_path, good = zipnum
    lines = good.splitlines(keepends=True)
    idx_path.write_bytes(b"".join([lines[1], lines[0]] + lines[2:]))

    with pytest.raises(SystemExit) as exc:
        main(["-i", str(idx_path), "-o", str(idx_path)])

    assert exc.value.code == 1
    assert "1 line(s)" in capsys.readouterr().err


def test_unreadable_block_fails_without_output(zipnum, tmp_path, capsys):
    """A block that is not a gzip member is an error naming the .idx line."""
    idx_path, good = zipnum
    lines = write_truncated(idx_path, good).splitlines(keepends=True)
    lineno = next(i for i, line in enumerate(lines, 1) if b" " not in line.split(b"\t")[0])
    parts = lines[lineno - 1].split(b"\t")
    parts[2] = str(int(parts[2]) + 1).encode()
    lines[lineno - 1] = b"\t".join(parts)
    idx_path.write_bytes(b"".join(lines))

    fixed = tmp_path / "fixed.idx"
    with pytest.raises(SystemExit) as exc:
        main(["-i", str(idx_path), "-o", str(fixed)])

    assert exc.value.code == 1
    assert f"t.idx:{lineno}:" in capsys.readouterr().err
    assert not fixed.exists()
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]


def test_shard_outside_base_dir_is_rejected(zipnum, tmp_path, capsys):
    idx_path, good = zipnum
    write_truncated(idx_path, good)
    loc = tmp_path / "evil.loc"
    shards = {line.split(b"\t")[1].decode() for line in good.splitlines()}
    loc.write_text("".join(f"{name}\t../../{name}\n" for name in shards), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(["-i", str(idx_path), "-o", str(tmp_path / "fixed.idx"), "--loc", str(loc)])

    assert exc.value.code == 1
    assert "Path traversal" in capsys.readouterr().err
