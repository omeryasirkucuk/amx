"""Tests for the asset-RAG per-kind splitters.

Splitters are pure functions over the raw row data, so every test
just feeds a string or dict and asserts the resulting
:class:`AssetDocument` shape — no Chroma, no SQLite, no embedding
cost.
"""

from __future__ import annotations

import json

from amx.assets.splitters import (
    split_job,
    split_notebook,
    split_pipeline,
    split_query,
    split_stream,
    split_streamlit,
)


def _ipynb(*cells: dict) -> str:
    return json.dumps({"cells": list(cells), "nbformat": 4, "metadata": {}})


def _md(src: str) -> dict:
    return {"cell_type": "markdown", "source": src}


def _code(src: str) -> dict:
    return {"cell_type": "code", "source": src}


# ── notebook ──────────────────────────────────────────────────────


def test_notebook_emits_one_chunk_per_markdown_or_code_cell() -> None:
    source = _ipynb(
        _md("# Setup"),
        _code("import pandas as pd"),
        _md("## Load data"),
        _code("df = pd.read_csv('orders.csv')"),
    )
    docs = split_notebook(profile="p", remote_id=1, name="nb", source_text=source)
    assert len(docs) == 4
    # Header path propagates onto every subsequent chunk via the
    # active stack — the code cell under "## Load data" sees both
    # h1 (Setup) and h2 (Load data).
    assert docs[3].metadata["header_path"].endswith("Load data")
    assert "Setup" in docs[3].metadata["header_path"]


def test_notebook_falls_back_to_char_window_when_not_json() -> None:
    docs = split_notebook(
        profile="p",
        remote_id=2,
        name="raw_sql",
        source_text="SELECT 1\nSELECT 2",
    )
    assert len(docs) == 1
    assert docs[0].text == "SELECT 1\nSELECT 2"
    assert docs[0].metadata["cell_type"] == "raw"


def test_notebook_long_cell_splits_into_chunks() -> None:
    big_code = "x = 1\n" * 400  # ~2400 chars
    source = _ipynb(_md("# Big"), _code(big_code))
    docs = split_notebook(profile="p", remote_id=3, name="big", source_text=source)
    code_chunks = [d for d in docs if d.metadata.get("cell_type") == "code"]
    # 2400-char body, 1000-char window with 200 overlap → 3 chunks
    assert len(code_chunks) >= 3


def test_notebook_skips_empty_cells_and_non_string_sources() -> None:
    source = _ipynb(
        _md(""),
        {"cell_type": "code", "source": ["select", " 1"]},  # list source joins
        {"cell_type": "raw", "source": "ignore me"},  # raw cell skipped
    )
    docs = split_notebook(profile="p", remote_id=4, name="nb", source_text=source)
    # Empty markdown skipped, raw skipped, code cell with list source kept.
    assert len(docs) == 1
    assert docs[0].metadata["cell_type"] == "code"
    assert docs[0].text == "select 1"


def test_notebook_chunk_id_is_stable() -> None:
    source = _ipynb(_md("# A"), _code("1"))
    docs = split_notebook(profile="db_prod", remote_id=7, name="nb", source_text=source)
    assert docs[0].chunk_id == "db_prod::notebook::7::0"
    assert docs[1].chunk_id == "db_prod::notebook::7::1"


# ── query ─────────────────────────────────────────────────────────


def test_query_splits_on_statement_boundary() -> None:
    sql = "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT * FROM t;"
    docs = split_query(profile="p", remote_id=1, name="q", sql_text=sql)
    assert len(docs) == 3
    assert docs[1].metadata["statement_no"] == 1


def test_query_long_statement_falls_back_to_char_window() -> None:
    big = "SELECT " + ", ".join(f"col_{i}" for i in range(500)) + " FROM t"
    docs = split_query(profile="p", remote_id=2, name="q", sql_text=big)
    assert len(docs) >= 2


def test_query_handles_empty_input() -> None:
    assert split_query(profile="p", remote_id=1, name="q", sql_text="") == []
    assert split_query(profile="p", remote_id=1, name="q", sql_text="   ") == []


# ── pipeline ──────────────────────────────────────────────────────


def test_pipeline_emits_header_and_notebook_library_chunks() -> None:
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
    assert len(docs) == 3  # header + 2 notebook libs
    assert "bronze_loader" in docs[0].text
    assert "Workspace/bronze/load" in docs[1].text
    assert docs[1].metadata["notebook_path"].startswith("/Workspace")


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
