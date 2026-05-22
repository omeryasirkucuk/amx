"""Tests for the per-asset chunking override storage + loader path."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.assets.chunking_config import (
    AssetChunkingConfig,
    NotebookChunkingConfig,
    QueryChunkingConfig,
)
from amx.assets.chunking_overrides import (
    ChunkingOverrideValidationError,
    clear_override,
    get_override,
    list_overrides_for_profile,
    set_override,
    validate_strategy,
)
from amx.assets.loaders import load_notebook_documents, load_query_documents
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _insert(store: SQLiteHistoryStore, sql: str, params: tuple) -> int:
    with store._lock, store._connect() as conn:
        cur = conn.execute(sql, params)
        return int(cur.lastrowid or 0)


# ── validation ─────────────────────────────────────────────────────


def test_validate_strategy_accepts_legal_combinations() -> None:
    for strategy in ("whole", "cell", "char_window"):
        validate_strategy("notebook", strategy)
    for strategy in ("whole", "statement", "char_window"):
        validate_strategy("query", strategy)
    for strategy in ("metadata", "whole"):
        validate_strategy("pipeline", strategy)


def test_validate_strategy_rejects_wrong_kind() -> None:
    with pytest.raises(ChunkingOverrideValidationError):
        validate_strategy("notebook", "statement")
    with pytest.raises(ChunkingOverrideValidationError):
        validate_strategy("query", "cell")
    with pytest.raises(ChunkingOverrideValidationError):
        validate_strategy("stream", "whole")  # not overridable


# ── CRUD ───────────────────────────────────────────────────────────


def test_set_then_get_roundtrips_override(store: SQLiteHistoryStore) -> None:
    override = set_override(
        history=store,
        profile_name="p",
        kind="notebook",
        remote_id=42,
        strategy="cell",
        chunk_chars=800,
        chunk_overlap=120,
    )
    assert override.strategy == "cell"
    assert override.chunk_chars == 800
    assert override.chunk_overlap == 120

    fetched = get_override(history=store, profile_name="p", kind="notebook", remote_id=42)
    assert fetched is not None
    assert fetched.strategy == "cell"
    assert fetched.chunk_chars == 800


def test_set_override_upserts_on_conflict(store: SQLiteHistoryStore) -> None:
    set_override(
        history=store,
        profile_name="p",
        kind="notebook",
        remote_id=1,
        strategy="cell",
    )
    set_override(
        history=store,
        profile_name="p",
        kind="notebook",
        remote_id=1,
        strategy="char_window",
        chunk_chars=500,
    )
    fetched = get_override(history=store, profile_name="p", kind="notebook", remote_id=1)
    assert fetched is not None
    assert fetched.strategy == "char_window"
    assert fetched.chunk_chars == 500


def test_set_override_rejects_invalid_strategy(store: SQLiteHistoryStore) -> None:
    with pytest.raises(ChunkingOverrideValidationError):
        set_override(
            history=store,
            profile_name="p",
            kind="notebook",
            remote_id=1,
            strategy="statement",  # not legal for notebook
        )


def test_clear_override_deletes_row(store: SQLiteHistoryStore) -> None:
    set_override(
        history=store,
        profile_name="p",
        kind="notebook",
        remote_id=1,
        strategy="cell",
    )
    assert clear_override(history=store, profile_name="p", kind="notebook", remote_id=1)
    assert get_override(history=store, profile_name="p", kind="notebook", remote_id=1) is None
    # Second clear is a no-op.
    assert not clear_override(history=store, profile_name="p", kind="notebook", remote_id=1)


def test_list_overrides_returns_profile_scoped_rows(store: SQLiteHistoryStore) -> None:
    set_override(
        history=store,
        profile_name="p1",
        kind="notebook",
        remote_id=1,
        strategy="cell",
    )
    set_override(
        history=store,
        profile_name="p2",
        kind="query",
        remote_id=10,
        strategy="statement",
    )
    p1_overrides = list_overrides_for_profile(history=store, profile_name="p1")
    assert {(o.kind, o.remote_id) for o in p1_overrides} == {("notebook", 1)}


# ── loader integration ────────────────────────────────────────────


def test_loader_applies_notebook_override(store: SQLiteHistoryStore) -> None:
    """An asset with an override gets the override's strategy; siblings
    without an override stay on the global config."""
    ipynb = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# Setup"},
                {"cell_type": "code", "source": "spark.read"},
            ]
        }
    )
    overridden_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "language, source_text, source_hash, ingested_at) VALUES "
        "('p', 'databricks', 'ext-1', 'overridden', 'python', ?, 'h', '0')",
        (ipynb,),
    )
    other_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "language, source_text, source_hash, ingested_at) VALUES "
        "('p', 'databricks', 'ext-2', 'other', 'python', ?, 'h', '0')",
        (ipynb,),
    )
    set_override(
        history=store,
        profile_name="p",
        kind="notebook",
        remote_id=overridden_id,
        strategy="cell",
    )
    # Global config stays on 'whole' (the new default).
    global_cfg = AssetChunkingConfig(notebook=NotebookChunkingConfig(strategy="whole"))
    with store._connect() as conn:
        docs = load_notebook_documents(conn=conn, profile_name="p", chunking=global_cfg)

    overridden_docs = [d for d in docs if d.remote_id == overridden_id]
    other_docs = [d for d in docs if d.remote_id == other_id]
    # The overridden notebook got cell-level chunking → 2 chunks.
    assert len(overridden_docs) == 2
    assert {d.metadata["cell_type"] for d in overridden_docs} == {"markdown", "code"}
    # The other notebook used the global 'whole' strategy → 1 chunk.
    assert len(other_docs) == 1
    assert other_docs[0].metadata["cell_type"] == "whole"


def test_loader_applies_query_override(store: SQLiteHistoryStore) -> None:
    q_id = _insert(
        store,
        "INSERT INTO remote_queries (profile_name, platform, kind, external_id, name, "
        "sql_text, sql_hash, warehouse, ingested_at) VALUES "
        "('p', 'snowflake', 'saved', 'q1', 'multi', 'SELECT 1; SELECT 2;', 'h', 'WH', '0')",
        (),
    )
    set_override(
        history=store,
        profile_name="p",
        kind="query",
        remote_id=q_id,
        strategy="statement",
    )
    global_cfg = AssetChunkingConfig(query=QueryChunkingConfig(strategy="whole"))
    with store._connect() as conn:
        docs = load_query_documents(conn=conn, profile_name="p", chunking=global_cfg)
    assert len(docs) == 2  # 'statement' override beat the global 'whole'
