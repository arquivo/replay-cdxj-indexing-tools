"""
Replay CDXJ Indexing Tools

A Python package for web archive replay CDXJ indexing.
Provides tools for efficient merging of sorted CDXJ files, conversion to ZipNum format,
parallel incremental indexing, and arclist to path index conversion.

Modules:
    merge: Tools for merging multiple sorted CDXJ files
    zipnum: Tools for converting CDXJ to ZipNum format
    redis: Tools for Redis submission
    arclist_to_path_index: Convert arclist files to path index format
    utils: Shared utilities
"""

__version__ = "1.0.0"
__author__ = "Arquivo.pt Team"

from .arclist_to_path_index import convert_arclist_to_path_index, get_arclist_files
from .merge.merge_flat_cdxj import get_all_files, merge_sorted_files

__all__ = [
    "merge_sorted_files",
    "get_all_files",
    "convert_arclist_to_path_index",
    "get_arclist_files",
    "__version__",
]
