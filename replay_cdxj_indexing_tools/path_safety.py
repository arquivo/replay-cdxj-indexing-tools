"""
Path containment check shared by the tools that open files named in untrusted input.
"""

import errno
import os


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
