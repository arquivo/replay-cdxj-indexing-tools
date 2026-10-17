#!/usr/bin/env python3
"""
test_path_safety.py - Test suite for path_safety
================================================

Tests the shared path containment check used by the ZipNum readers.
"""

import os

import pytest

from replay_cdxj_indexing_tools.path_safety import resolve_within


class TestResolveWithin:
    """Test resolve_within() containment rules."""

    def test_path_inside_base(self, tmp_path):
        """A plain child path is accepted and returned resolved."""
        base = str(tmp_path)
        assert resolve_within(os.path.join(base, "a.cdx.gz"), base) == os.path.join(
            os.path.realpath(base), "a.cdx.gz"
        )

    def test_dotdot_escape_rejected(self, tmp_path):
        """A path using .. to leave the base is rejected."""
        base = tmp_path / "base"
        base.mkdir()
        with pytest.raises(ValueError):
            resolve_within(os.path.join(str(base), "..", "outside.cdx.gz"), str(base))

    def test_dotdot_staying_inside_accepted(self, tmp_path):
        """A .. that stays inside the base is accepted."""
        base = str(tmp_path)
        path = os.path.join(base, "sub", "..", "a.cdx.gz")
        assert resolve_within(path, base) == os.path.join(os.path.realpath(base), "a.cdx.gz")

    def test_absolute_path_outside_rejected(self, tmp_path):
        """An absolute path outside the base is rejected."""
        with pytest.raises(ValueError):
            resolve_within("/etc/passwd", str(tmp_path))

    def test_sibling_prefix_rejected(self, tmp_path):
        """/base2 shares a string prefix with /base but is not inside it."""
        base = tmp_path / "base"
        sibling = tmp_path / "base2"
        base.mkdir()
        sibling.mkdir()
        with pytest.raises(ValueError):
            resolve_within(str(sibling / "a.cdx.gz"), str(base))

    @pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks not supported")
    def test_symlink_escaping_base_rejected(self, tmp_path):
        """A symlink inside the base that points outside is rejected."""
        base = tmp_path / "base"
        outside = tmp_path / "outside"
        base.mkdir()
        outside.mkdir()
        link = base / "evil.cdx.gz"
        try:
            os.symlink(str(outside / "secret"), str(link))
        except (OSError, NotImplementedError):
            pytest.skip("Cannot create symlinks on this platform")

        with pytest.raises(ValueError):
            resolve_within(str(link), str(base))

    @pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks not supported")
    def test_symlink_inside_base_accepted(self, tmp_path):
        """A symlink that points to another file inside the base is accepted."""
        base = tmp_path / "base"
        (base / "real").mkdir(parents=True)
        target = base / "real" / "a.cdx.gz"
        target.write_bytes(b"")
        link = base / "link.cdx.gz"
        try:
            os.symlink(str(target), str(link))
        except (OSError, NotImplementedError):
            pytest.skip("Cannot create symlinks on this platform")

        assert resolve_within(str(link), str(base)) == os.path.realpath(str(target))

    def test_path_equal_to_base_accepted(self, tmp_path):
        """The base directory itself is inside the base."""
        base = str(tmp_path)
        assert resolve_within(base, base) == os.path.realpath(base)

    def test_root_base_accepts_absolute_paths(self):
        """With base '/', every absolute path is inside (not rejected via '//')."""
        assert resolve_within("/", "/") == "/"
        assert resolve_within("/data/shards/a.cdx.gz", "/") == os.path.realpath(
            "/data/shards/a.cdx.gz"
        )

    def test_relative_base(self, tmp_path, monkeypatch):
        """A relative base is resolved against the current working directory."""
        (tmp_path / "base").mkdir()
        monkeypatch.chdir(tmp_path)
        resolved_base = os.path.realpath(str(tmp_path / "base"))

        assert resolve_within(os.path.join("base", "a.cdx.gz"), "base") == os.path.join(
            resolved_base, "a.cdx.gz"
        )
        assert resolve_within("a.cdx.gz", ".") == os.path.join(
            os.path.realpath(str(tmp_path)), "a.cdx.gz"
        )
        with pytest.raises(ValueError):
            resolve_within("other.cdx.gz", "base")

    def test_trailing_slash_on_base(self, tmp_path):
        """A trailing slash on the base does not change the result."""
        base = tmp_path / "base"
        base.mkdir()
        base_slash = str(base) + os.sep
        assert resolve_within(str(base / "a.cdx.gz"), base_slash) == os.path.join(
            os.path.realpath(str(base)), "a.cdx.gz"
        )

    @pytest.mark.skipif(not hasattr(os, "symlink"), reason="Symlinks not supported")
    def test_symlink_loop_rejected(self, tmp_path):
        """A path that realpath cannot fully resolve (symlink loop) is rejected."""
        base = tmp_path / "base"
        base.mkdir()
        try:
            os.symlink("l2", str(base / "l1"))
            os.symlink("l1", str(base / "l2"))
        except (OSError, NotImplementedError):
            pytest.skip("Cannot create symlinks on this platform")

        for path in (base / "l1", base / "l1" / "a.cdx.gz"):
            with pytest.raises(ValueError, match="does not resolve to a stable path"):
                resolve_within(str(path), str(base))

    def test_nul_byte_rejected(self, tmp_path):
        """A NUL byte in either argument is rejected with a repr'd message."""
        base = str(tmp_path)
        with pytest.raises(ValueError, match="NUL byte") as excinfo:
            resolve_within(os.path.join(base, "a\x00.cdx.gz"), base)
        assert "\\x00" in str(excinfo.value)
        with pytest.raises(ValueError, match="NUL byte"):
            resolve_within(os.path.join(base, "a.cdx.gz"), base + "\x00")

    def test_error_message_uses_repr(self, tmp_path):
        """User-supplied paths appear repr-quoted, so control characters stay escaped."""
        with pytest.raises(ValueError) as excinfo:
            resolve_within("/etc/evil\nname", str(tmp_path))
        assert "\n" not in str(excinfo.value)
        assert repr("/etc/evil\nname") in str(excinfo.value)
