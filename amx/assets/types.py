"""Typed values exchanged between the asset-RAG layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

AssetKind = Literal[
    "notebook",
    "query",
    "stream",
    "pipeline",
    "streamlit_app",
    "job",
]


@dataclass(frozen=True)
class AssetDocument:
    """A single chunk extracted from an ingested remote asset.

    ``kind`` identifies which ``remote_*`` table the parent row lives
    in. ``profile`` + ``remote_id`` uniquely locate it. ``chunk_index``
    is the splitter's per-asset position so the canvas can navigate
    back ("cell #17 of notebook X"). ``metadata`` carries optional
    splitter hints (``cell_type``, ``header_path``, ``statement_no``)
    that the consumer renders alongside the excerpt.
    """

    kind: str
    profile: str
    remote_id: int
    chunk_index: int
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def chunk_id(self) -> str:
        """Stable id used as the Chroma upsert key."""
        return f"{self.profile}::{self.kind}::{self.remote_id}::{self.chunk_index}"


@dataclass(frozen=True)
class AssetQueryHit:
    """A single retrieved chunk with its score + metadata."""

    chunk_id: str
    kind: str
    profile: str
    remote_id: int
    name: str
    text: str
    score: float
    metadata: dict[str, Any] = field(default_factory=dict)


__all__ = ["AssetDocument", "AssetKind", "AssetQueryHit"]
