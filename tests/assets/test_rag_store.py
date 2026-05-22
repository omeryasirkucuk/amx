"""Smoke tests for :class:`amx.assets.rag.AssetRAGStore`.

This module avoids running the real Chroma + sentence-transformers
stack in CI (the heavy dep is gated behind ``_ensure``); we exercise
the store via an in-process Chroma instance pointed at ``tmp_path``
when the dependency cluster is present, and skip otherwise.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.assets.types import AssetDocument


@pytest.fixture()
def store(tmp_path: Path):
    """Build an AssetRAGStore against an isolated Chroma directory.

    Skipped when ``chromadb`` / sentence-transformers are not
    installed in the test environment so the suite stays runnable
    on the slim Python-only install path.
    """
    pytest.importorskip("chromadb")
    pytest.importorskip("sentence_transformers")
    from amx.assets.rag import AssetRAGStore

    return AssetRAGStore(persist_dir=str(tmp_path / "chroma"))


def test_ingest_documents_then_query_returns_top_hits(store) -> None:
    docs = [
        AssetDocument(
            kind="notebook",
            profile="p",
            remote_id=1,
            chunk_index=0,
            text="Daily ingest pipeline that loads the orders table from raw",
            metadata={"cell_type": "markdown", "asset_name": "daily_orders_etl"},
        ),
        AssetDocument(
            kind="notebook",
            profile="p",
            remote_id=2,
            chunk_index=0,
            text="Unrelated analytics about customer churn predictions",
            metadata={"cell_type": "markdown", "asset_name": "churn_model"},
        ),
    ]
    n = store.ingest_documents(docs)
    assert n == 2
    assert store.count() == 2

    hits = store.query("how do we load orders", top_k=2)
    assert hits
    # Top hit should be the orders notebook, NOT the churn one — that's
    # the whole point of dense retrieval over BM25-lite.
    assert hits[0].name == "daily_orders_etl"
    assert hits[0].kind == "notebook"
    assert hits[0].remote_id == 1


def test_query_filters_by_profile_and_remote_ids(store) -> None:
    store.ingest_documents(
        [
            AssetDocument(
                kind="notebook",
                profile="p1",
                remote_id=10,
                chunk_index=0,
                text="orders ingest",
                metadata={"asset_name": "p1_orders"},
            ),
            AssetDocument(
                kind="notebook",
                profile="p2",
                remote_id=20,
                chunk_index=0,
                text="orders ingest",
                metadata={"asset_name": "p2_orders"},
            ),
        ]
    )
    hits = store.query("orders ingest", top_k=5, profile="p2")
    assert all(h.profile == "p2" for h in hits)
    assert {h.remote_id for h in hits} == {20}

    hits_scoped = store.query("orders ingest", top_k=5, remote_ids=[10])
    assert {h.remote_id for h in hits_scoped} == {10}


def test_delete_asset_removes_every_chunk(store) -> None:
    store.ingest_documents(
        [
            AssetDocument(
                kind="notebook",
                profile="p",
                remote_id=1,
                chunk_index=i,
                text=f"chunk {i}",
                metadata={"asset_name": "nb"},
            )
            for i in range(4)
        ]
    )
    assert store.count() == 4
    removed = store.delete_asset(kind="notebook", profile="p", remote_id=1)
    assert removed == 4
    assert store.count() == 0


def test_ingest_profile_reads_from_history_store(store, tmp_path: Path) -> None:
    """End-to-end: SQLite remote_* rows → loader → splitter → ingest_documents."""
    from amx.assets.chunking_config import (
        AssetChunkingConfig,
        NotebookChunkingConfig,
    )
    from amx.storage.sqlite_store import SQLiteHistoryStore

    history = SQLiteHistoryStore(tmp_path / "history.db")
    history.init()
    ipynb = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# Header"},
                {"cell_type": "code", "source": "spark.read.table('orders')"},
            ]
        }
    )
    with history._lock, history._connect() as conn:
        conn.execute(
            "INSERT INTO remote_notebooks (profile_name, platform, external_id, "
            "name, workspace_path, language, source_text, source_hash, ingested_at) "
            "VALUES ('p', 'databricks', 'ext-1', 'orders_load', '/etl', 'python', ?, "
            "'h', '2026-01-01')",
            (ipynb,),
        )
    # Force the cell-level strategy so the test stays meaningful even
    # though the runtime default is now ``whole`` (one chunk per
    # notebook). The cell strategy is what we want to verify the
    # end-to-end loader → splitter path produces.
    chunking = AssetChunkingConfig(notebook=NotebookChunkingConfig(strategy="cell"))
    with history._connect() as conn:
        indexed = store.ingest_profile(
            conn=conn, profile_name="p", chunking=chunking
        )
    assert indexed >= 2  # markdown + code cell
    hits = store.query("Header", top_k=3, profile="p")
    assert hits
    assert any(h.kind == "notebook" for h in hits)
