"""Pluggable lineage extractors.

Each extractor returns an :class:`amx.lineage.types.ExtractResult` that
reports edges plus cache status. The service layer fans them out and
prompts the user before any DB round-trip.
"""

from __future__ import annotations

from amx.lineage.extractors.fk import FKExtractor
from amx.lineage.extractors.name_match import NameMatchExtractor
from amx.lineage.extractors.view_ddl import ViewDDLExtractor

__all__ = ["FKExtractor", "ViewDDLExtractor", "NameMatchExtractor"]
