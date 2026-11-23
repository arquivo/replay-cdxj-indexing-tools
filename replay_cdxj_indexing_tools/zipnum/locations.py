"""
Where the shards of a ZipNum index are: the .loc file and shard name resolution
shared by cdxj-search and zipnum-to-flat-cdxj.

Locations may be local paths or remote URLs (see storage.is_remote). The .loc and
.idx contents are untrusted: every shard location goes through
path_safety.resolve_location(), so it cannot leave the base directory or URL.
"""

from typing import Dict, Optional

from replay_cdxj_indexing_tools import storage
from replay_cdxj_indexing_tools.path_safety import resolve_location

_SHARD_SUFFIXES = (".cdx.gz", ".cdxj.gz")


def loc_location(idx_location: str) -> str:
    """The .loc file that goes with an .idx: a trailing ``.idx`` becomes ``.loc``."""
    base = idx_location[: -len(".idx")] if idx_location.endswith(".idx") else idx_location
    return base + ".loc"


def read_loc_file(loc_path: str) -> Dict[str, str]:
    """
    Read a .loc file and return the mapping of shard name -> location.

    Each line is ``<shard_name>\\t<location>``; blank lines, ``#`` comments and lines
    without a tab are skipped.
    """
    loc_map = {}

    with storage.open_input(loc_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            loc_map[parts[0]] = parts[1]

    return loc_map


def read_loc_if_present(loc_path: str) -> Optional[Dict[str, str]]:
    """
    read_loc_file(), or None if there is no such file. Over HTTP(S) this costs two
    requests, whether or not the file exists.
    """
    try:
        return read_loc_file(loc_path)
    except FileNotFoundError:
        return None


def resolve_shard_path(shard_name: str, base_dir: str, loc_map: Optional[dict] = None) -> str:
    """
    Resolve the location of a shard named in an .idx.

    1. If ``loc_map`` has the name, use its entry; relative entries are resolved
       against ``base_dir``
    2. Otherwise, the shard is ``base_dir/shard_name``, with ``.cdx.gz`` added
       unless the name already ends in ``.cdx.gz`` or ``.cdxj.gz``

    ``base_dir`` may be a local directory or a remote URL.

    Raises:
        ValueError: "Path traversal detected ..." if the location is outside
            ``base_dir`` (see path_safety.resolve_location); the cause says why
    """
    if loc_map and shard_name in loc_map:
        entry = loc_map[shard_name]
        try:
            return resolve_location(entry, base_dir)
        except ValueError as e:
            raise ValueError(f"Path traversal detected in .loc file: {entry!r}") from e

    shard_file = shard_name if shard_name.endswith(_SHARD_SUFFIXES) else f"{shard_name}.cdx.gz"
    try:
        return resolve_location(shard_file, base_dir)
    except ValueError as e:
        raise ValueError(f"Path traversal detected in .idx shard name: {shard_name!r}") from e
