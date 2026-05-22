"""Tests for the asset-RAG loaders.

Each loader reads ONE ``remote_*`` table and produces typed
:class:`AssetDocument` chunks. We exercise them against a real
:class:`SQLiteHistoryStore` initialised in ``tmp_path`` — no Chroma
yet; that lives in :mod:`tests.assets.test_rag_store`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.assets.loaders import (
    load_asset_documents,
    load_notebook_documents,
    load_query_documents,
    load_stream_documents,
)
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


def test_load_notebook_documents_emits_per_cell_chunks(store: SQLiteHistoryStore) -> None:
    ipynb = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# Sales ETL"},
                {"cell_type": "code", "source": "spark.read.table('sales.orders')"},
                {"cell_type": "code", "source": "df.write.saveAsTable('sales.daily')"},
            ]
        }
    )
    _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "workspace_path, language, source_text, source_hash, ingested_at) "
        "VALUES ('p', 'databricks', 'ext-1', 'sales_etl', '/etl', 'python', ?, 'h', '2026-01-01')",
        (ipynb,),
    )
    with store._connect() as conn:
        docs = load_notebook_documents(conn=conn, profile_name="p")
    assert len(docs) == 3
    assert {d.metadata["cell_type"] for d in docs} == {"markdown", "code"}
    assert all(d.kind == "notebook" for d in docs)
    assert all(d.profile == "p" for d in docs)


def test_load_query_documents_splits_on_statement_boundary(
    store: SQLiteHistoryStore,
) -> None:
    _insert(
        store,
        "INSERT INTO remote_queries (profile_name, platform, kind, external_id, name, "
        "sql_text, sql_hash, warehouse, ingested_at) VALUES "
        "('p', 'snowflake', 'saved', 'eq', 'daily', "
        "'SELECT * FROM a; INSERT INTO b SELECT * FROM a;', 'h', 'WH_M', '2026-01-01')",
        (),
    )
    with store._connect() as conn:
        docs = load_query_documents(conn=conn, profile_name="p")
    assert len(docs) == 2
    assert all(d.kind == "query" for d in docs)


def test_load_asset_documents_dispatches_across_kinds(store: SQLiteHistoryStore) -> None:
    _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "language, source_text, source_hash, ingested_at) VALUES "
        "('p', 'databricks', 'ext-1', 'nb1', 'python', "
        '\'{"cells":[{"cell_type":"code","source":"select 1"}]}\', \'h\', \'0\')',
        (),
    )
    _insert(
        store,
        "INSERT INTO remote_streams (profile_name, qualified_name, source_table_fqn, "
        "mode, ingested_at) VALUES ('p', 'a.b.STREAM', 'a.b.SRC', 'APPEND_ONLY', '0')",
        (),
    )
    with store._connect() as conn:
        docs = load_asset_documents(conn=conn, profile_name="p")
    kinds = {d.kind for d in docs}
    assert {"notebook", "stream"}.issubset(kinds)


def test_load_notebook_documents_respects_only_ids(store: SQLiteHistoryStore) -> None:
    keep_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "language, source_text, source_hash, ingested_at) VALUES "
        "('p', 'databricks', 'a', 'A', 'sql', "
        '\'{"cells":[{"cell_type":"code","source":"a"}]}\', \'h\', \'0\')',
        (),
    )
    _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, name, "
        "language, source_text, source_hash, ingested_at) VALUES "
        "('p', 'databricks', 'b', 'B', 'sql', "
        '\'{"cells":[{"cell_type":"code","source":"b"}]}\', \'h\', \'0\')',
        (),
    )
    with store._connect() as conn:
        docs = load_notebook_documents(conn=conn, profile_name="p", only_ids=[keep_id])
    assert {d.remote_id for d in docs} == {keep_id}


def test_load_stream_documents_emits_single_chunk(store: SQLiteHistoryStore) -> None:
    _insert(
        store,
        "INSERT INTO remote_streams (profile_name, qualified_name, source_table_fqn, "
        "mode, ingested_at) VALUES ('p', 'X.Y.STREAM', 'X.Y.SRC', 'INSERT_ONLY', '0')",
        (),
    )
    with store._connect() as conn:
        docs = load_stream_documents(conn=conn, profile_name="p")
    assert len(docs) == 1
    assert docs[0].metadata["source_table_fqn"] == "X.Y.SRC"
