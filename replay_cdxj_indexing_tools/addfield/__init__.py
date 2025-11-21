"""
CDXJ Field Addition Tools

Tools for adding custom fields to CDXJ records (flat CDXJ format).
Unlike filters that remove data, addfield tools enrich records with metadata.
"""

from .addfield_to_flat_cdxj import addfield_to_cdxj, load_addfield_function

__all__ = ["addfield_to_cdxj", "load_addfield_function"]
