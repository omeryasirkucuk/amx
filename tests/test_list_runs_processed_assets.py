"""``list_recent_runs`` enriches each row with a processed-asset
summary so the Studio Runs / Compare pages can show a concrete
``schema.table.column`` label instead of the schema-level scope the
user originally picked.

A column-level ``/rerun --column`` run looks identical to a
full-table run in ``scope_json`` — the only ground truth for what was
actually processed is the ``run_results`` write log. The aggregation
below joins that log per-run; the SPA renders it as e.g. ``sales.
orders.status`` for a single-column run and ``sales.orders (3
columns)`` when several columns of one table were processed.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _new_run(store: SQLiteHistoryStore, scope=None) -> int:
    return store.create_run(
        command="analyze.run",
        mode="full",
        db_backend="postgresql",
        db_profile="test",
        llm_provider="openai",
        llm_model="gpt-test",
        scope=scope or {},
    )


def _commit_result(
    store: SQLiteHistoryStore,
    run_id: int,
    schema: str,
    table: str,
    column: str | None,
) -> None:
    """Insert a single ``run_results`` row at the desired granularity."""
    import sqlite3

    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            """
            INSERT INTO run_results
                (run_id, saved_at, schema_name, table_name, column_name,
                 asset_kind, source, confidence, alternatives_json)
            VALUES (?, ?, ?, ?, ?, 'table', 'llm', 'high', '[]')
            """,
            (run_id, time.time(), schema, table, column),
        )


def test_single_column_run_surfaces_full_asset_path(store: SQLiteHistoryStore) -> None:
    """The headline use case: a ``/rerun --column status`` produces
    one result row. The aggregate must report 1 schema, 1 table, 1
    column, and carry the (schema, table, column) tuple in the
    sample list — enough for the SPA to render ``sales.orders.status``."""
    rid = _new_run(store, scope={"sales": ["orders"]})
    _commit_result(store, rid, "sales", "orders", "status")
    runs = store.list_recent_runs(limit=10)
    row = next(r for r in runs if r["id"] == rid)
    pa = row["processed_assets"]
    assert pa["schemas"] == 1
    assert pa["tables"] == 1
    assert pa["columns"] == 1
    assert pa["sample"] == [{"schema": "sales", "table": "orders", "column": "status"}]


def test_full_table_run_has_no_column_in_sample(store: SQLiteHistoryStore) -> None:
    """A pure table-level row (``column_name IS NULL``) must count as
    a table and 0 columns. The SPA renders such a row as
    ``schema.table`` without the "(N columns)" suffix."""
    rid = _new_run(store, scope={"sales": ["orders"]})
    _commit_result(store, rid, "sales", "orders", None)
    row = next(r for r in store.list_recent_runs(limit=10) if r["id"] == rid)
    pa = row["processed_assets"]
    assert pa["tables"] == 1
    assert pa["columns"] == 0
    assert pa["sample"] == [{"schema": "sales", "table": "orders", "column": None}]


def test_one_table_many_columns_summary(store: SQLiteHistoryStore) -> None:
    """All-columns analyze on a single table — frontend renders this
    as ``sales.orders (3 columns)`` using the counts."""
    rid = _new_run(store, scope={"sales": ["orders"]})
    for col in ("id", "amount", "status"):
        _commit_result(store, rid, "sales", "orders", col)
    row = next(r for r in store.list_recent_runs(limit=10) if r["id"] == rid)
    pa = row["processed_assets"]
    assert pa["tables"] == 1
    assert pa["columns"] == 3


def test_cross_schema_run_carries_diverse_sample(store: SQLiteHistoryStore) -> None:
    """A multi-schema run: counts reflect the spread, sample carries
    a few concrete tuples for the tooltip."""
    rid = _new_run(store, scope={"sales": ["orders"], "marketing": ["campaigns"]})
    _commit_result(store, rid, "sales", "orders", "status")
    _commit_result(store, rid, "marketing", "campaigns", "name")
    row = next(r for r in store.list_recent_runs(limit=10) if r["id"] == rid)
    pa = row["processed_assets"]
    assert pa["schemas"] == 2
    assert pa["tables"] == 2
    assert pa["columns"] == 2
    sample_keys = {(a["schema"], a["table"], a["column"]) for a in pa["sample"]}
    assert ("sales", "orders", "status") in sample_keys
    assert ("marketing", "campaigns", "name") in sample_keys


def test_sample_is_capped(store: SQLiteHistoryStore) -> None:
    """A run with many distinct assets caps the sample at 6 so the
    listing payload stays cheap. The full counts still surface for
    the headline label."""
    rid = _new_run(store, scope={"sales": [f"t{i}" for i in range(20)]})
    for i in range(20):
        _commit_result(store, rid, "sales", f"t{i}", "id")
    row = next(r for r in store.list_recent_runs(limit=10) if r["id"] == rid)
    pa = row["processed_assets"]
    assert pa["tables"] == 20
    assert len(pa["sample"]) == 6


def test_run_with_no_results_is_empty_envelope(store: SQLiteHistoryStore) -> None:
    """An in-flight run that hasn't yet written any results gets a
    zero envelope. The SPA falls back to the legacy scope summary
    in that case so the cell never renders empty."""
    rid = _new_run(store, scope={"sales": ["orders"]})
    row = next(r for r in store.list_recent_runs(limit=10) if r["id"] == rid)
    assert row["processed_assets"] == {
        "schemas": 0,
        "tables": 0,
        "columns": 0,
        "sample": [],
    }


def test_two_runs_dont_leak_assets_into_each_other(store: SQLiteHistoryStore) -> None:
    """The aggregation is per run_id, never bleeding between rows.
    Regression guard for an earlier draft that grouped on column
    but not run_id and showed run #1's tables under run #2."""
    r1 = _new_run(store, scope={"a": ["t1"]})
    r2 = _new_run(store, scope={"b": ["t2"]})
    _commit_result(store, r1, "a", "t1", "c1")
    _commit_result(store, r2, "b", "t2", "c2")
    runs = {r["id"]: r["processed_assets"] for r in store.list_recent_runs(limit=10)}
    assert runs[r1]["sample"] == [{"schema": "a", "table": "t1", "column": "c1"}]
    assert runs[r2]["sample"] == [{"schema": "b", "table": "t2", "column": "c2"}]
