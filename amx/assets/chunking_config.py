"""User-tunable chunking strategy for the asset-RAG ingest pipeline.

The splitters in :mod:`amx.assets.splitters` dispatch on the
strategy literal carried by :class:`AssetChunkingConfig`. The same
config object also exposes ``chunk_chars`` / ``chunk_overlap``
knobs so the user can trade retrieval precision (smaller chunks,
tighter matches) against context completeness (larger chunks, more
neighbour signal) per asset kind.

Defaults are intentionally conservative: notebooks and queries are
embedded **whole** by default — the user opts into cell-level or
char-window splitting explicitly via ``/db assets chunking`` or by
editing ``cfg.embedding_assets.chunking`` in ``~/.amx/config.yml``.
Whole-asset embeddings are coarser but predictable; users who want
better recall over long notebooks switch to ``cell`` (the previous
default).
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Literal

NotebookStrategy = Literal["whole", "cell", "char_window"]
QueryStrategy = Literal["whole", "statement", "char_window"]
PipelineStrategy = Literal["metadata", "whole"]

DEFAULT_CHUNK_CHARS = 1000
DEFAULT_CHUNK_OVERLAP = 200
DEFAULT_NOTEBOOK_CODE_CELLS = 8


@dataclass
class NotebookChunkingConfig:
    """Per-kind config for notebook chunking.

    * ``strategy='whole'`` (default) — one chunk per notebook,
      ``source_text`` embedded as-is. Coarse but predictable.
    * ``strategy='cell'`` — markdown + code cells split into separate
      chunks; cells longer than ``chunk_chars`` get char-window
      sub-split with ``chunk_overlap``. Header path metadata
      preserved.
    * ``strategy='char_window'`` — ignore cell boundaries; split the
      raw source text by ``chunk_chars`` / ``chunk_overlap`` only.
    """

    strategy: NotebookStrategy = "whole"
    chunk_chars: int = DEFAULT_CHUNK_CHARS
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP
    max_code_cells_per_chunk: int = DEFAULT_NOTEBOOK_CODE_CELLS


@dataclass
class QueryChunkingConfig:
    """Per-kind config for SQL query chunking.

    * ``strategy='whole'`` (default) — full SQL embedded as one chunk.
    * ``strategy='statement'`` — split on ``;`` boundaries; long
      statements fall back to char-window.
    * ``strategy='char_window'`` — ignore statement boundaries;
      pure char-window split.
    """

    strategy: QueryStrategy = "whole"
    chunk_chars: int = DEFAULT_CHUNK_CHARS
    chunk_overlap: int = DEFAULT_CHUNK_OVERLAP


@dataclass
class PipelineChunkingConfig:
    """Per-kind config for pipeline chunking.

    Pipelines only carry metadata + library references, so the
    chunking choices are coarse: ``metadata`` (one chunk per pipeline
    plus one per linked notebook library, current behaviour) vs
    ``whole`` (single chunk with everything inlined).
    """

    strategy: PipelineStrategy = "metadata"


@dataclass
class AssetChunkingConfig:
    """Top-level chunking config persisted under
    ``cfg.embedding_assets.chunking``.

    Each asset kind owns its own sub-config so users can mix
    strategies — e.g. notebooks split cell-by-cell for fine-grained
    retrieval while queries stay whole. Stream / streamlit / job
    have no tunable knobs (they index as a single metadata chunk
    each, period — there is no meaningful source text to slice).
    """

    notebook: NotebookChunkingConfig = field(default_factory=NotebookChunkingConfig)
    query: QueryChunkingConfig = field(default_factory=QueryChunkingConfig)
    pipeline: PipelineChunkingConfig = field(default_factory=PipelineChunkingConfig)


def chunking_from_mapping(data: dict[str, Any]) -> AssetChunkingConfig:
    """Hydrate :class:`AssetChunkingConfig` from a YAML mapping.

    Permissive on the way in: missing kind blocks fall back to
    defaults; unknown keys inside a block are ignored. This lets us
    grow the per-kind config in future versions without
    invalidating existing user configs.
    """
    out = AssetChunkingConfig()
    if not isinstance(data, dict):
        return out
    nb_raw = data.get("notebook")
    if isinstance(nb_raw, dict):
        out.notebook = _populate(NotebookChunkingConfig(), nb_raw)
    q_raw = data.get("query")
    if isinstance(q_raw, dict):
        out.query = _populate(QueryChunkingConfig(), q_raw)
    p_raw = data.get("pipeline")
    if isinstance(p_raw, dict):
        out.pipeline = _populate(PipelineChunkingConfig(), p_raw)
    return out


def chunking_to_mapping(cfg: AssetChunkingConfig) -> dict[str, Any]:
    """Serialise :class:`AssetChunkingConfig` back to a YAML-friendly
    mapping. Round-trips with :func:`chunking_from_mapping`.
    """
    return {
        "notebook": _to_dict(cfg.notebook),
        "query": _to_dict(cfg.query),
        "pipeline": _to_dict(cfg.pipeline),
    }


def _populate(target: Any, data: dict[str, Any]) -> Any:
    for fld in fields(target):
        if fld.name in data:
            setattr(target, fld.name, data[fld.name])
    return target


def _to_dict(target: Any) -> dict[str, Any]:
    return {fld.name: getattr(target, fld.name) for fld in fields(target)}


__all__ = [
    "AssetChunkingConfig",
    "DEFAULT_CHUNK_CHARS",
    "DEFAULT_CHUNK_OVERLAP",
    "DEFAULT_NOTEBOOK_CODE_CELLS",
    "NotebookChunkingConfig",
    "NotebookStrategy",
    "PipelineChunkingConfig",
    "PipelineStrategy",
    "QueryChunkingConfig",
    "QueryStrategy",
    "chunking_from_mapping",
    "chunking_to_mapping",
]
