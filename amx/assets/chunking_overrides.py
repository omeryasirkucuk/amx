"""CRUD helpers for the ``asset_chunking_overrides`` table.

Studio's Assets page calls these from the per-row "Chunk" button so
a single notebook / query / pipeline can carry a chunking strategy
that differs from the global ``cfg.assets_chunking`` default. The
loaders in :mod:`amx.assets.loaders` join against this table on
ingest, applying the override if present.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

_OVERRIDABLE_KINDS = ("notebook", "query", "pipeline")
_NOTEBOOK_STRATEGIES = ("whole", "cell", "char_window")
_QUERY_STRATEGIES = ("whole", "statement", "char_window")
_PIPELINE_STRATEGIES = ("metadata", "whole")


class ChunkingOverrideValidationError(ValueError):
    """The override payload was not valid for the given asset kind."""


@dataclass(frozen=True)
class ChunkingOverride:
    """The persisted override row."""

    profile_name: str
    kind: str
    remote_id: int
    strategy: str
    chunk_chars: int | None
    chunk_overlap: int | None
    updated_at: float


def validate_strategy(kind: str, strategy: str) -> None:
    """Raise when ``strategy`` is not legal for ``kind``."""
    if kind == "notebook" and strategy not in _NOTEBOOK_STRATEGIES:
        raise ChunkingOverrideValidationError(
            f"Notebook strategy must be one of {_NOTEBOOK_STRATEGIES}; got {strategy!r}"
        )
    if kind == "query" and strategy not in _QUERY_STRATEGIES:
        raise ChunkingOverrideValidationError(
            f"Query strategy must be one of {_QUERY_STRATEGIES}; got {strategy!r}"
        )
    if kind == "pipeline" and strategy not in _PIPELINE_STRATEGIES:
        raise ChunkingOverrideValidationError(
            f"Pipeline strategy must be one of {_PIPELINE_STRATEGIES}; got {strategy!r}"
        )
    if kind not in _OVERRIDABLE_KINDS:
        raise ChunkingOverrideValidationError(
            f"Kind {kind!r} is not overridable. Valid: {_OVERRIDABLE_KINDS}"
        )


def set_override(
    *,
    history: Any,
    profile_name: str,
    kind: str,
    remote_id: int,
    strategy: str,
    chunk_chars: int | None = None,
    chunk_overlap: int | None = None,
) -> ChunkingOverride:
    """UPSERT the per-asset chunking override.

    Validates the strategy against the asset kind before writing so a
    malformed payload from the Studio modal never lands in the table.
    """
    validate_strategy(kind, strategy)
    ts = time.time()
    with history._lock, history._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO asset_chunking_overrides
                   (profile_name, kind, remote_id, strategy,
                    chunk_chars, chunk_overlap, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(profile_name, kind, remote_id)
            DO UPDATE SET
                strategy = excluded.strategy,
                chunk_chars = excluded.chunk_chars,
                chunk_overlap = excluded.chunk_overlap,
                updated_at = excluded.updated_at
            """,
            (
                profile_name,
                kind,
                int(remote_id),
                strategy,
                int(chunk_chars) if chunk_chars is not None else None,
                int(chunk_overlap) if chunk_overlap is not None else None,
                ts,
            ),
        )
    return ChunkingOverride(
        profile_name=profile_name,
        kind=kind,
        remote_id=int(remote_id),
        strategy=strategy,
        chunk_chars=int(chunk_chars) if chunk_chars is not None else None,
        chunk_overlap=int(chunk_overlap) if chunk_overlap is not None else None,
        updated_at=ts,
    )


def get_override(
    *, history: Any, profile_name: str, kind: str, remote_id: int
) -> ChunkingOverride | None:
    with history._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT strategy, chunk_chars, chunk_overlap, updated_at "
            "FROM asset_chunking_overrides "
            "WHERE profile_name = ? AND kind = ? AND remote_id = ?",
            (profile_name, kind, int(remote_id)),
        ).fetchone()
    if row is None:
        return None
    return ChunkingOverride(
        profile_name=profile_name,
        kind=kind,
        remote_id=int(remote_id),
        strategy=str(row[0]),
        chunk_chars=int(row[1]) if row[1] is not None else None,
        chunk_overlap=int(row[2]) if row[2] is not None else None,
        updated_at=float(row[3]),
    )


def clear_override(*, history: Any, profile_name: str, kind: str, remote_id: int) -> bool:
    """Remove the per-asset override. Returns True when a row was deleted."""
    with history._lock, history._connect() as conn:  # noqa: SLF001
        cursor = conn.execute(
            "DELETE FROM asset_chunking_overrides "
            "WHERE profile_name = ? AND kind = ? AND remote_id = ?",
            (profile_name, kind, int(remote_id)),
        )
    return bool(cursor.rowcount)


def list_overrides_for_profile(*, history: Any, profile_name: str) -> list[ChunkingOverride]:
    with history._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            "SELECT kind, remote_id, strategy, chunk_chars, chunk_overlap, updated_at "
            "FROM asset_chunking_overrides WHERE profile_name = ? "
            "ORDER BY kind, remote_id",
            (profile_name,),
        ).fetchall()
    return [
        ChunkingOverride(
            profile_name=profile_name,
            kind=str(kind),
            remote_id=int(rid),
            strategy=str(strategy),
            chunk_chars=int(cc) if cc is not None else None,
            chunk_overlap=int(co) if co is not None else None,
            updated_at=float(ts),
        )
        for kind, rid, strategy, cc, co, ts in rows
    ]


__all__ = [
    "ChunkingOverride",
    "ChunkingOverrideValidationError",
    "clear_override",
    "get_override",
    "list_overrides_for_profile",
    "set_override",
    "validate_strategy",
]
