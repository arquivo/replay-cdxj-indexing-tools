"""
Path containment checks shared by the tools that open files named in untrusted input.
"""

import errno
import os
import re
import unicodedata
from typing import List
from urllib.parse import unquote, urlsplit, urlunsplit

from replay_cdxj_indexing_tools import storage

# Unicode categories rejected in index entries. Controls (C0, DEL, C1) are never
# part of a file name an index should hold, and could reach a terminal through
# error messages. Remote entries also reject format characters (bidirectional
# controls, zero-width characters) and line and paragraph separators, which could
# disguise a URL; local file names keep them, as some scripts need them (e.g. the
# zero-width non-joiner in Persian).
_LOCAL_REJECTED_CATEGORIES = frozenset(("Cc",))
_REMOTE_REJECTED_CATEGORIES = frozenset(("Cc", "Cf", "Zl", "Zp"))
# A path segment (before any ";params", whitespace stripped) taken as the parent
# directory by some servers: "..", or more dots
_PARENT_SEGMENT_RE = re.compile(r"\.{2,}")


def _has_chars_in(text: str, categories: frozenset) -> bool:
    """Whether ``text`` has a character in one of the Unicode ``categories``."""
    return any(unicodedata.category(char) in categories for char in text)


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
    """
    Decoded, non-empty path segments of an HTTP(S) URL path.

    Percent-decoding is strict: invalid UTF-8 (such as an overlong ``%c0%ae``) is
    rejected, and so are backslashes, control characters and parent segments:
    ``..`` and its variants that some servers also treat as the parent directory
    (``%2e%2e``, ``..;params``, ``...``, ``..%20``).
    """
    try:
        decoded = unquote(path, errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{location!r} has a percent-encoded path that is not UTF-8") from exc
    if "\\" in decoded or _has_chars_in(decoded, _REMOTE_REJECTED_CATEGORIES):
        raise ValueError(f"{location!r} contains a backslash or control character")
    segments = [segment for segment in decoded.split("/") if segment not in ("", ".")]
    if any(_PARENT_SEGMENT_RE.fullmatch(segment.split(";", 1)[0].strip()) for segment in segments):
        raise ValueError(f"{location!r} contains a '..' path segment")
    return segments


def _s3_key_segments(path: str, location: str) -> List[str]:
    """
    Segments of the literal S3 key in a URL path: S3 does not decode or normalise
    keys, so empty, ``.`` and ``..`` segments, which a URL-aware reader would
    collapse, are rejected.
    """
    key = path[1:] if path.startswith("/") else path
    segments = key.split("/") if key else []
    if any(segment in ("", ".", "..") for segment in segments):
        raise ValueError(f"{location!r} contains an empty, '.' or '..' key segment")
    return segments


def url_within(location: str, base_url: str) -> str:
    """
    Check that the URL ``location`` is inside the URL "directory" ``base_url``.

    Both must have the same scheme and host (or S3 bucket), and the path of
    ``location`` must start with every path segment of ``base_url``. (CWE-22, CWE-918)

    - HTTP(S): hosts are compared case-insensitively and paths after
      percent-decoding, and any ``..`` segment is rejected, since servers may
      resolve ``..`` or ``%2e%2e`` themselves (see _url_segments). Below the base,
      an encoded ``/`` or ``%`` is rejected too.
    - S3: bucket names and keys are compared exactly as S3 reads them: case
      matters, nothing is decoded, and ``%`` below the base is rejected (see
      _s3_key_segments).

    Returns:
        ``location`` unchanged

    Raises:
        ValueError: If ``location`` is outside ``base_url``
    """
    loc, base = urlsplit(location), urlsplit(base_url)
    scheme = base.scheme.lower()
    if scheme == "s3":
        same_host = loc.netloc == base.netloc  # bucket names are case-sensitive
        loc_segments = _s3_key_segments(loc.path, location)
        base_segments = _s3_key_segments(base.path.rstrip("/"), base_url)
    else:
        same_host = loc.netloc.lower() == base.netloc.lower()
        loc_segments = _url_segments(loc.path, location)
        base_segments = _url_segments(base.path, base_url)
    below_base = loc_segments[len(base_segments) :]
    if (
        loc.scheme.lower() != scheme
        or not same_host
        or loc_segments[: len(base_segments)] != base_segments
        or not below_base
    ):
        raise ValueError(f"{location!r} is outside {base_url!r}")
    if scheme == "s3" and any("%" in segment for segment in below_base):
        raise ValueError(f"{location!r} contains '%' in its key")
    if scheme != "s3":
        # Checked below the trusted base only: a server may not decode an encoded
        # "/" into a separator, or may decode "%25" (an encoded "%") twice
        prefix = base.path.rstrip("/") + "/"
        raw_below = loc.path[len(prefix) :] if loc.path.startswith(prefix) else loc.path
        if "%2f" in raw_below.lower() or "%25" in raw_below:
            raise ValueError(f"{location!r} contains an encoded '/' or '%'")
    return location


def _drop_dot_segments(location: str) -> str:
    """
    Remove ``.`` path segments from a URL, as RFC 3986 asks clients to do.

    HTTP servers would ignore them anyway, but S3 keys are literal: ``a/./b`` is
    not the object ``a/b``.
    """
    parts = urlsplit(location)
    path = "/".join(segment for segment in parts.path.split("/") if segment != ".")
    return urlunsplit(parts._replace(path=path))


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
        The resolved local path, or the URL without ``.`` path segments

    Raises:
        ValueError: If ``entry`` contains a control character (or, under a remote
            base, a format character or line separator), resolves outside
            ``base_dir`` or uses an unsupported scheme
    """
    base_is_remote = storage.is_remote(base_dir)
    rejected = _REMOTE_REJECTED_CATEGORIES if base_is_remote else _LOCAL_REJECTED_CATEGORIES
    if _has_chars_in(entry, rejected):
        raise ValueError(f"{entry!r} contains a control character")
    entry_is_remote = storage.is_remote(entry)
    if base_is_remote:
        if entry_is_remote:
            return url_within(_drop_dot_segments(entry), base_dir)
        if os.path.isabs(entry):
            raise ValueError(f"{entry!r} is a local path, outside remote base {base_dir!r}")
        joined = storage.join_location(base_dir, entry)
        return url_within(_drop_dot_segments(joined), base_dir)
    if entry_is_remote:
        raise ValueError(f"{entry!r} is a remote location, outside local base {base_dir!r}")
    if not os.path.isabs(entry):
        entry = os.path.join(base_dir, entry)
    return resolve_within(entry, base_dir)
