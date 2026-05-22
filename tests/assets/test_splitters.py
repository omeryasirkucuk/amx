"""Tests for the asset-RAG per-kind splitters.

Splitters are pure functions over the raw row data, so every test
just feeds a string or dict and asserts the resulting
:class:`AssetDocument` shape — no Chroma, no SQLite, no embedding
cost.

The default chunking strategy is ``whole`` (one chunk per asset),
so any test that expects per-cell / per-statement chunks passes an
explicit :class:`NotebookChunkingConfig` / :class:`QueryChunkingConfig`.
A small group of tests at the bottom asserts the default behaviour
on its own.
"""

from __future__ import annotations

import json

from amx.assets.chunking_config import (
    NotebookChunkingConfig,
    PipelineChunkingConfig,
    QueryChunkingConfig,
)
from amx.assets.splitters import (
    split_job,
    split_notebook,
    split_pipeline,
    split_query,
    split_stream,
    split_streamlit,
)

_CELL = NotebookChunkingConfig(strategy="cell")
_CHAR = NotebookChunkingConfig(strategy="char_window")
_WHOLE_NB = NotebookChunkingConfig(strategy="whole")
_STATEMENT = QueryChunkingConfig(strategy="statement")
_WHOLE_QUERY = QueryChunkingConfig(strategy="whole")
_QUERY_CHAR = QueryChunkingConfig(strategy="char_window")


def _ipynb(*cells: dict) -> str:
    return json.dumps({"cells": list(cells), "nbformat": 4, "metadata": {}})


def _md(src: str) -> dict:
    return {"cell_type": "markdown", "source": src}


def _code(src: str) -> dict:
    return {"cell_type": "code", "source": src}


# ── notebook: cell strategy ───────────────────────────────────────


def test_notebook_cell_emits_one_chunk_per_markdown_or_code_cell() -> None:
    source = _ipynb(
        _md("# Setup"),
        _code("import pandas as pd"),
        _md("## Load data"),
        _code("df = pd.read_csv('orders.csv')"),
    )
    docs = split_notebook(profile="p", remote_id=1, name="nb", source_text=source, config=_CELL)
    assert len(docs) == 4
    # Header path propagates onto every subsequent chunk via the
    # active stack — the code cell under "## Load data" sees both
    # h1 (Setup) and h2 (Load data).
    assert docs[3].metadata["header_path"].endswith("Load data")
    assert "Setup" in docs[3].metadata["header_path"]


def test_notebook_cell_falls_back_to_char_window_when_not_json() -> None:
    docs = split_notebook(
        profile="p",
        remote_id=2,
        name="raw_sql",
        source_text="SELECT 1\nSELECT 2",
        config=_CELL,
    )
    assert len(docs) == 1
    assert docs[0].text == "SELECT 1\nSELECT 2"
    assert docs[0].metadata["cell_type"] == "raw"


def test_notebook_cell_long_cell_splits_into_chunks() -> None:
    big_code = "x = 1\n" * 400  # ~2400 chars
    source = _ipynb(_md("# Big"), _code(big_code))
    docs = split_notebook(profile="p", remote_id=3, name="big", source_text=source, config=_CELL)
    code_chunks = [d for d in docs if d.metadata.get("cell_type") == "code"]
    # 2400-char body, 1000-char window with 200 overlap → 3 chunks
    assert len(code_chunks) >= 3


def test_notebook_cell_skips_empty_cells_and_non_string_sources() -> None:
    source = _ipynb(
        _md(""),
        {"cell_type": "code", "source": ["select", " 1"]},  # list source joins
        {"cell_type": "raw", "source": "ignore me"},  # raw cell skipped
    )
    docs = split_notebook(profile="p", remote_id=4, name="nb", source_text=source, config=_CELL)
    # Empty markdown skipped, raw skipped, code cell with list source kept.
    assert len(docs) == 1
    assert docs[0].metadata["cell_type"] == "code"
    assert docs[0].text == "select 1"


def test_notebook_cell_chunk_id_is_stable() -> None:
    source = _ipynb(_md("# A"), _code("1"))
    docs = split_notebook(
        profile="db_prod", remote_id=7, name="nb", source_text=source, config=_CELL
    )
    assert docs[0].chunk_id == "db_prod::notebook::7::0"
    assert docs[1].chunk_id == "db_prod::notebook::7::1"


def test_notebook_cell_honours_custom_chunk_chars() -> None:
    """Tighter chunk_chars produces more sub-chunks."""
    big_code = "y = 2\n" * 200  # ~1200 chars
    source = _ipynb(_code(big_code))
    docs_default = split_notebook(
        profile="p",
        remote_id=8,
        name="nb",
        source_text=source,
        config=NotebookChunkingConfig(strategy="cell"),
    )
    docs_tight = split_notebook(
        profile="p",
        remote_id=8,
        name="nb",
        source_text=source,
        config=NotebookChunkingConfig(strategy="cell", chunk_chars=400, chunk_overlap=50),
    )
    assert len(docs_tight) > len(docs_default)


# ── notebook: whole strategy ──────────────────────────────────────


def test_notebook_whole_is_default_and_yields_single_chunk() -> None:
    source = _ipynb(_md("# A"), _code("1"), _md("## B"), _code("2"))
    docs = split_notebook(profile="p", remote_id=1, name="nb", source_text=source)
    assert len(docs) == 1
    assert docs[0].metadata["cell_type"] == "whole"
    # The whole-asset chunk preserves the raw ipynb JSON so the
    # embedding sees every cell at once (coarse but predictable).
    assert "# A" in docs[0].text or "Header" in docs[0].text or docs[0].text  # not empty


def test_notebook_whole_empty_source_returns_no_chunks() -> None:
    docs = split_notebook(profile="p", remote_id=1, name="nb", source_text="", config=_WHOLE_NB)
    assert docs == []


# ── notebook: char_window strategy ────────────────────────────────


def test_notebook_char_window_ignores_cell_boundaries() -> None:
    big_text = "x" * 2500
    docs = split_notebook(
        profile="p",
        remote_id=2,
        name="nb",
        source_text=big_text,
        config=_CHAR,
    )
    assert len(docs) >= 3  # 2500-char body, 1000-char window with 200 overlap
    for d in docs:
        assert d.metadata["cell_type"] == "char_window"


# ── query: statement strategy ─────────────────────────────────────


def test_query_statement_splits_on_boundary() -> None:
    sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
    docs = split_query(profile="p", remote_id=1, name="q", sql_text=sql, config=_STATEMENT)
    assert len(docs) == 3
    assert docs[1].metadata["statement_no"] == 1


def test_query_statement_long_statement_falls_back_to_char_window() -> None:
    big = "SELECT " + ", ".join(f"col_{i}" for i in range(500)) + " FROM t"
    docs = split_query(profile="p", remote_id=2, name="q", sql_text=big, config=_STATEMENT)
    assert len(docs) >= 2


def test_query_statement_handles_empty_input() -> None:
    assert split_query(profile="p", remote_id=1, name="q", sql_text="", config=_STATEMENT) == []
    assert split_query(profile="p", remote_id=1, name="q", sql_text="   ", config=_STATEMENT) == []


# ── query: whole strategy (default) ───────────────────────────────


def test_query_whole_is_default_and_yields_single_chunk() -> None:
    sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
    docs = split_query(profile="p", remote_id=1, name="q", sql_text=sql)
    assert len(docs) == 1
    assert "CREATE TABLE" in docs[0].text
    assert "SELECT" in docs[0].text


def test_query_whole_empty_returns_no_chunks() -> None:
    docs = split_query(profile="p", remote_id=1, name="q", sql_text="", config=_WHOLE_QUERY)
    assert docs == []


# ── query: char_window strategy ───────────────────────────────────


def test_query_char_window_ignores_statement_boundaries() -> None:
    sql = "SELECT a; SELECT b; SELECT c;"
    docs = split_query(profile="p", remote_id=1, name="q", sql_text=sql, config=_QUERY_CHAR)
    # Below chunk_chars → single chunk, but no statement_no >= 0.
    assert len(docs) == 1
    assert docs[0].metadata["statement_no"] == -1


# ── pipeline strategies ───────────────────────────────────────────


def test_pipeline_metadata_emits_header_and_notebook_library_chunks() -> None:
    libs = json.dumps(
        [
            {"notebook": {"path": "/Workspace/bronze/load"}},
            {"jar": "spark-x.jar"},
            {"notebook": {"path": "/Workspace/silver/transform"}},
        ]
    )
    docs = split_pipeline(
        profile="p",
        remote_id=1,
        name="bronze_loader",
        target_schema="bronze",
        libraries_json=libs,
    )
    assert len(docs) == 3  # header + 2 notebook libs (metadata is the default)
    assert "bronze_loader" in docs[0].text
    assert "Workspace/bronze/load" in docs[1].text
    assert docs[1].metadata["notebook_path"].startswith("/Workspace")


def test_pipeline_whole_inlines_libraries() -> None:
    libs = json.dumps([{"notebook": {"path": "/Workspace/x"}}])
    docs = split_pipeline(
        profile="p",
        remote_id=1,
        name="bronze_loader",
        target_schema="bronze",
        libraries_json=libs,
        config=PipelineChunkingConfig(strategy="whole"),
    )
    assert len(docs) == 1
    assert "bronze_loader" in docs[0].text
    assert "/Workspace/x" in docs[0].text


# ── metadata-only kinds ───────────────────────────────────────────


def test_stream_single_chunk_with_source_table() -> None:
    docs = split_stream(
        profile="sf",
        remote_id=1,
        qualified_name="DB.STG.ORDERS_STREAM",
        source_table_fqn="DB.RAW.ORDERS",
        mode="APPEND_ONLY",
    )
    assert len(docs) == 1
    assert "DB.RAW.ORDERS" in docs[0].text
    assert docs[0].metadata["source_table_fqn"] == "DB.RAW.ORDERS"


def test_streamlit_single_chunk_with_main_file() -> None:
    docs = split_streamlit(
        profile="sf",
        remote_id=2,
        qualified_name="DB.APPS.DASH",
        main_file="app.py",
    )
    assert len(docs) == 1
    assert "app.py" in docs[0].text


def test_job_chunk_includes_schedule_and_tasks() -> None:
    docs = split_job(
        profile="db_prod",
        remote_id=42,
        name="nightly_load",
        schedule_cron="0 2 * * *",
        tasks=[
            {"task_key": "extract", "task_type": "NOTEBOOK", "notebook_path": "/etl"},
            {"task_key": "load", "task_type": "SQL", "sql_query_id": "q123"},
        ],
    )
    assert len(docs) == 1
    text = docs[0].text
    assert "nightly_load" in text
    assert "0 2 * * *" in text
    assert "extract" in text
    assert "load" in text
