"""Asset-RAG pipeline: chunked + embedded retrieval over ingested remote assets.

Public surface:

- :class:`AssetRAGStore` — Chroma-backed store with ingest / query /
  reindex / delete.
- :class:`AssetDocument` — typed chunk produced by the splitters.
- :class:`AssetQueryHit` — typed result from ``AssetRAGStore.query``.
- :func:`load_asset_documents` — dispatcher that reads every kind for a
  given DB profile (used by the auto-index hook in
  :class:`amx.services.ingest_assets.IngestAssetsService`).
"""

from amx.assets.loaders import load_asset_documents
from amx.assets.rag import AssetRAGStore, EmbeddingProviderMismatch
from amx.assets.types import AssetDocument, AssetKind, AssetQueryHit

__all__ = [
    "AssetDocument",
    "AssetKind",
    "AssetQueryHit",
    "AssetRAGStore",
    "EmbeddingProviderMismatch",
    "load_asset_documents",
]
