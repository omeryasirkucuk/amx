"""On-demand native lineage fetch for a single user-picked table.

Reads lineage straight from the database platform's own lineage system
(Unity Catalog for Databricks) via APIs that authorize on ordinary
table visibility, so an individual user can fetch "what feeds /
consumes this table" without the metastore-admin grants the bulk
``system.access.*`` extractor needs. Discovered tables, producer /
consumer assets, and vector indexes are routed to their homes in the
catalog and linked in the lineage graph; entities the user cannot read
are kept as name-only nodes rather than dropped.

Importing this package registers every available provider with the
backend registry in :mod:`amx.lineage.native.provider`.
"""

from __future__ import annotations

from amx.lineage.native.databricks import register as _register_databricks
from amx.lineage.native.provider import (
    NativeLineageEdge,
    NativeLineageNode,
    NativeLineageResult,
    provider_for_profile,
    supported_backends,
)
from amx.lineage.native.service import LineageFetchService, NativeLineageError

# Register built-in providers at import time.
_register_databricks()

__all__ = [
    "LineageFetchService",
    "NativeLineageError",
    "NativeLineageNode",
    "NativeLineageEdge",
    "NativeLineageResult",
    "provider_for_profile",
    "supported_backends",
]
