"""
Replay CDXJ Indexing Tools

A Python package for web archive replay CDXJ indexing.
Provides tools for efficient merging of sorted CDXJ files, conversion to ZipNum format,
and parallel incremental indexing.

Modules:
    merge: Tools for merging multiple sorted CDXJ files
    zipnum: Tools for converting CDXJ to ZipNum format
    utils: Shared utilities
"""

__version__ = "1.0.0"
__author__ = "Arquivo.pt Team"

from .merge.merge_flat_cdxj import get_all_files, merge_sorted_files

__all__ = ["merge_sorted_files", "get_all_files", "__version__"]
