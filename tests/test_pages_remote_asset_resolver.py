"""Tests for the RemoteAssetResolver.

Each test seeds a minimal row in the relevant ``remote_*`` table of a
fresh on-disk SQLite history store and asserts the resolver emits the
expected header, body fragments, and per-kind size cap.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.pages.resolvers.remote_assets import RemoteAssetResolver
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _insert(store: SQLiteHistoryStore, sql: str, params: tuple) -> int:
    with store._lock, store._connect() as conn:
        cursor = conn.execute(sql, params)
        return int(cursor.lastrowid or 0)


def test_resolve_notebook_emits_markdown_and_code(store: SQLiteHistoryStore) -> None:
    ipynb = {
        "cells": [
            {"cell_type": "markdown", "source": "# Sales daily ETL"},
            {"cell_type": "code", "source": "df = spark.read.table('sales.orders')"},
            {"cell_type": "code", "source": "df.write.saveAsTable('sales.daily')"},
        ]
    }
    nb_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, "
        "name, workspace_path, qualified_name, language, source_text, "
        "source_hash, ingested_at, cell_count) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "db_prod",
            "databricks",
            "ext-1",
            "daily_etl",
            "/Workspace/etl/daily_etl",
            None,
            "python",
            json.dumps(ipynb),
            "hash",
            "2026-01-01",
            3,
        ),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"db_prod:{nb_id}", "asset_notebook")
    assert "NOTEBOOK" in block
    assert "daily_etl" in block
    assert "Sales daily ETL" in block
    assert "spark.read.table('sales.orders')" in block


def test_resolve_notebook_caps_oversized_excerpt(store: SQLiteHistoryStore) -> None:
    huge_cell = {"cell_type": "code", "source": "x" * 50_000}
    ipynb = {"cells": [huge_cell]}
    nb_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, "
        "name, language, source_text, source_hash, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("p", "snowflake", "ext-2", "big", "python", json.dumps(ipynb), "h", "2026-01-01"),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"p:{nb_id}", "asset_notebook")
    assert len(block) <= 10 * 1024
    assert "[notebook excerpt truncated]" in block


def test_resolve_notebook_falls_back_when_source_is_not_json(
    store: SQLiteHistoryStore,
) -> None:
    nb_id = _insert(
        store,
        "INSERT INTO remote_notebooks (profile_name, platform, external_id, "
        "name, language, source_text, source_hash, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "p",
            "snowflake",
            "ext-3",
            "raw_sql",
            "sql",
            "SELECT 1\nSELECT 2",
            "h",
            "2026-01-01",
        ),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"p:{nb_id}", "asset_notebook")
    assert "NOTEBOOK" in block
    assert "SELECT 1" in block


def test_resolve_job_includes_schedule_tasks_and_runs(store: SQLiteHistoryStore) -> None:
    job_id = _insert(
        store,
        "INSERT INTO remote_jobs (profile_name, job_id, name, creator_user_name, "
        "schedule_cron, schedule_timezone, last_run_status, success_rate_30d, "
        "ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "db_prod",
            42,
            "nightly_load",
            "alice@example",
            "0 2 * * *",
            "UTC",
            "SUCCESS",
            0.95,
            "2026-01-01",
        ),
    )
    _insert(
        store,
        "INSERT INTO remote_job_tasks (job_id_fk, task_key, task_type, "
        "notebook_path, raw_definition_json) VALUES (?, ?, ?, ?, ?)",
        (job_id, "extract", "NOTEBOOK", "/Workspace/etl/extract", "{}"),
    )
    _insert(
        store,
        "INSERT INTO remote_job_runs (job_id_fk, run_id, state_result, start_time, "
        "execution_duration_ms) VALUES (?, ?, ?, ?, ?)",
        (job_id, 1, "SUCCESS", "2026-01-01T02:00:00", 12345),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"db_prod:{job_id}", "asset_job")
    assert "nightly_load" in block
    assert "0 2 * * *" in block
    assert "extract" in block
    assert "SUCCESS" in block


def test_resolve_pipeline_lists_notebook_libraries(store: SQLiteHistoryStore) -> None:
    libs = [{"notebook": {"path": "/Workspace/dlt/load"}}, {"jar": "spark-x.jar"}]
    pipe_id = _insert(
        store,
        "INSERT INTO remote_pipelines (profile_name, pipeline_id, name, target_schema, "
        "edition, continuous, photon, libraries_json, latest_update_state, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "db_prod",
            "pid-1",
            "bronze_loader",
            "bronze",
            "PRO",
            0,
            1,
            json.dumps(libs),
            "RUNNING",
            "2026-01-01",
        ),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"db_prod:{pipe_id}", "asset_pipeline")
    assert "bronze_loader" in block
    assert "/Workspace/dlt/load" in block
    assert "RUNNING" in block


def test_resolve_query_renders_sql_with_cap(store: SQLiteHistoryStore) -> None:
    big_sql = "SELECT * FROM huge\n" + "AND x = 1\n" * 1000
    q_id = _insert(
        store,
        "INSERT INTO remote_queries (profile_name, platform, kind, external_id, name, "
        "sql_text, sql_hash, warehouse, user_name, executed_at, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "sf_prod",
            "snowflake",
            "saved",
            "q-1",
            "daily_metrics",
            big_sql,
            "h",
            "WH_M",
            "u1",
            "2026-01-01",
            "2026-01-01",
        ),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"sf_prod:{q_id}", "asset_query")
    assert "daily_metrics" in block
    assert "```sql" in block
    assert len(block) <= 5 * 1024
    assert "[truncated]" in block


def test_resolve_stream_renders_compact_summary(store: SQLiteHistoryStore) -> None:
    s_id = _insert(
        store,
        "INSERT INTO remote_streams (profile_name, qualified_name, source_table_fqn, "
        "mode, stale_after, owner, ingested_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "sf_prod",
            "DB.STG.ORDERS_STREAM",
            "DB.RAW.ORDERS",
            "APPEND_ONLY",
            "1 day",
            "alice",
            "2026-01-01",
        ),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"sf_prod:{s_id}", "asset_stream")
    assert "STREAM" in block
    assert "DB.STG.ORDERS_STREAM" in block
    assert "APPEND_ONLY" in block


def test_resolve_streamlit_renders_compact_summary(store: SQLiteHistoryStore) -> None:
    a_id = _insert(
        store,
        "INSERT INTO remote_streamlit_apps (profile_name, qualified_name, main_file, "
        "query_warehouse, root_location, owner, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("sf_prod", "DB.APPS.DASH", "app.py", "WH_M", "@stage/", "bob", "2026-01-01"),
    )
    block = RemoteAssetResolver(store).resolve_asset(f"sf_prod:{a_id}", "asset_streamlit")
    assert "STREAMLIT APP" in block
    assert "app.py" in block


def test_resolve_missing_row_returns_not_found(store: SQLiteHistoryStore) -> None:
    block = RemoteAssetResolver(store).resolve_asset("p:9999", "asset_notebook")
    assert "not found" in block


def test_resolve_malformed_ref_returns_not_found(store: SQLiteHistoryStore) -> None:
    block = RemoteAssetResolver(store).resolve_asset("missing-colon", "asset_notebook")
    assert "not found" in block


def test_resolve_unknown_kind_returns_not_found(store: SQLiteHistoryStore) -> None:
    block = RemoteAssetResolver(store).resolve_asset("p:1", "asset_unknown")
    assert "not found" in block
