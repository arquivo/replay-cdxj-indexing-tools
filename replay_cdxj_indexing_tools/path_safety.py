"""
Path containment checks shared by the tools that open files named in untrusted input.
"""

import errno
import os
from typing import List
from urllib.parse import unquote, urlsplit

from replay_cdxj_indexing_tools import storage


def _is_symlink_loop(path: str) -> bool:
    """Return True if following ``path`` runs into a symlink loop."""
    try:
        os.stat(path)
    except OSError as e:
        return e.errno == errno.ELOOP
    return False


def resolve_within(path: str, base_dir: str) -> str:
    """
    Resolve ``path`` and check that it stays inside ``base_dir``.

    Both arguments are passed through os.path.realpath(), so ``..`` segments and
    symlinks are followed before the check, and relative paths are taken from
    the current working directory. Containment uses os.path.commonpath(), which
    compares whole path components: ``/data2`` is not inside ``/data``, and every
    absolute path is inside ``/``. (CWE-22)

    Args:
        path: Path to check (already joined with base_dir if it was relative)
        base_dir: Trusted directory that ``path`` must not escape

    Returns:
        The resolved path

    Raises:
        ValueError: If either argument contains a NUL byte, if realpath cannot
            fully resolve ``path`` (e.g. symlink loops), or if the resolved path
            is outside the resolved base_dir
    """
    for value in (path, base_dir):
        if "\x00" in value:
            raise ValueError(f"{value!r} contains a NUL byte")

    resolved_base = os.path.realpath(base_dir)
    resolved = os.path.realpath(path)
    # Reject paths that realpath cannot fully resolve (e.g. symlink loops)
    if os.path.realpath(resolved) != resolved or _is_symlink_loop(resolved):
        raise ValueError(f"{path!r} does not resolve to a stable path")
    if os.path.commonpath([resolved_base, resolved]) != resolved_base:
        raise ValueError(f"{path!r} resolves to {resolved!r}, outside {resolved_base!r}")
    return resolved


def _url_segments(path: str, location: str) -> List[str]:
    """Decoded, non-empty path segments of a URL path; rejects ``..`` and backslashes."""
    decoded = unquote(path)
    if "\\" in decoded or "\x00" in decoded:
        raise ValueError(f"{location!r} contains a backslash or NUL byte")
    segments = [segment for segment in decoded.split("/") if segment not in ("", ".")]
    if ".." in segments:
        raise ValueError(f"{location!r} contains a '..' path segment")
    return segments


def url_within(location: str, base_url: str) -> str:
    """
    Check that the URL ``location`` is inside the URL "directory" ``base_url``.

    Both must have the same scheme and host (or S3 bucket), and the path of
    ``location`` must start with every path segment of ``base_url``. Paths are
    compared after percent-decoding, and any ``..`` segment is rejected, since
    servers may resolve ``..`` or ``%2e%2e`` themselves. (CWE-22, CWE-918)

    Returns:
        ``location`` unchanged

    Raises:
        ValueError: If ``location`` is outside ``base_url``
    """
    loc, base = urlsplit(location), urlsplit(base_url)
    if (loc.scheme.lower(), loc.netloc.lower()) != (base.scheme.lower(), base.netloc.lower()):
        raise ValueError(f"{location!r} is not on the same host as {base_url!r}")
    loc_segments = _url_segments(loc.path, location)
    base_segments = _url_segments(base.path, base_url)
    if (
        len(loc_segments) <= len(base_segments)
        or loc_segments[: len(base_segments)] != base_segments
    ):
        raise ValueError(f"{location!r} is outside {base_url!r}")
    return location


def resolve_location(entry: str, base_dir: str) -> str:
    """
    Resolve a file location read from an index (.loc entry, .idx shard name).

    ``entry`` is untrusted; ``base_dir`` is trusted and may be a local directory or
    a remote URL (see storage.is_remote). Relative entries are joined to
    ``base_dir``. Absolute entries, local paths or URLs, are accepted only if they
    resolve inside ``base_dir``: local paths with resolve_within(), URLs with
    url_within(). A local path never resolves inside a remote base, nor a URL
    inside a local one.

    Returns:
        The resolved local path, or the URL

    Raises:
        ValueError: If ``entry`` resolves outside ``base_dir`` or uses an
            unsupported scheme
    """
    if "\x00" in entry:
        raise ValueError(f"{entry!r} contains a NUL byte")
    entry_is_remote = storage.is_remote(entry)
    if storage.is_remote(base_dir):
        if entry_is_remote:
            return url_within(entry, base_dir)
        if os.path.isabs(entry):
            raise ValueError(f"{entry!r} is a local path, outside remote base {base_dir!r}")
        return url_within(storage.join_location(base_dir, entry), base_dir)
    if entry_is_remote:
        raise ValueError(f"{entry!r} is a remote location, outside local base {base_dir!r}")
    if not os.path.isabs(entry):
        entry = os.path.join(base_dir, entry)
    return resolve_within(entry, base_dir)
