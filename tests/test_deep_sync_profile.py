"""Deep sync profiles every catalogued table (columns + row counts).

The Studio "Sync" button runs a skeleton sync — table-level rows only,
no columns or counts. "Deep sync" is the opt-in full-profile pass:
for every table the skeleton already catalogued, it runs
``profile_table`` + ``sync_table_profile`` so the Table page shows
real structure and row counts.

These tests pin the orchestration: it reads the skeleton inventory,
profiles each table, writes columns + counts, tracks progress, and
honours cancellation. The per-table write itself is covered by
``test_sync_decouple_vector_index.py`` / ``test_search_catalog.py``.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import amx.search.drift as drift
from amx.db.connector import AssetKind, ColumnProfile, TableProfile
from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


def _seed_skeleton(db_path: Path, profile: str, tables: list[tuple[str, str, str]]) -> None:
    """Insert table-level (skeleton) rows — no columns, row_count 0."""
    with sqlite3.connect(db_path) as conn:
        for database, schema, table in tables:
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, db_backend, database_name, schema_name,
                    table_name, column_name, entity_kind, asset_kind,
                    row_count, search_text, updated_at, last_synced_at
                ) VALUES (?, 'postgresql', ?, ?, ?, NULL, 'table', 'table', 0, '', ?, ?)
                """,
                (profile, database, schema, table, time.time(), time.time()),
            )


def _cfg_stub() -> SimpleNamespace:
    db = SimpleNamespace(backend="postgresql", database="bird_train", catalog="", project="")
    return SimpleNamespace(db_profiles={"p": db}, db=db)


@pytest.fixture()
def catalog(tmp_path: Path) -> SearchCatalog:
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    cat = SearchCatalog(db_path)
    # Keep the vector index out of the test — the decouple fix already
    # guarantees metadata writes survive index failures; here we just
    # want to assert the orchestration + column/count writes.
    cat._index_entity = lambda *a, **k: None  # type: ignore[method-assign]  # noqa: SLF001
    return cat


def _fake_connector(row_count: int = 999):
    class _Conn:
        def profile_table(self, schema: str, table: str, sample_size: int = 0) -> TableProfile:
            return TableProfile(
                schema=schema,
                name=table,
                asset_kind=AssetKind.TABLE,
                row_count=row_count,
                columns=[
                    ColumnProfile(name="id", dtype="int", nullable=False, existing_comment=""),
                    ColumnProfile(name="name", dtype="text", nullable=True, existing_comment=""),
                ],
            )

    return _Conn()


def _column_count(catalog: SearchCatalog) -> int:
    with catalog._connect() as conn:  # noqa: SLF001
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM catalog_entities WHERE entity_kind = 'column'"
            ).fetchone()[0]
        )


def test_deep_sync_profiles_every_catalogued_table(
    catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_skeleton(
        catalog.db_path,
        "p",
        [("bird_train", "app_store", "playstore"), ("bird_train", "airline", "Airports")],
    )
    monkeypatch.setattr(drift, "_scoped_connector", lambda *a, **k: _fake_connector(10840))

    summary = drift.deep_sync_profile(_cfg_stub(), "p", catalog)

    assert summary["state"] == "done"
    assert summary["processed"] == 2
    assert summary["failed"] == 0
    # Each table now has its 2 columns written (skeleton had none).
    assert _column_count(catalog) == 4
    # Row counts landed on the table entities.
    with catalog._connect() as conn:  # noqa: SLF001
        counts = [
            r["row_count"]
            for r in conn.execute(
                "SELECT row_count FROM catalog_entities WHERE entity_kind = 'table'"
            )
        ]
    assert all(c == 10840 for c in counts)


def test_deep_sync_empty_catalog_finishes_with_note(
    catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No skeleton rows → nothing to profile; finish cleanly with a
    note rather than erroring."""
    called = {"n": 0}

    def _conn(*_a, **_k):
        called["n"] += 1
        return _fake_connector()

    monkeypatch.setattr(drift, "_scoped_connector", _conn)

    summary = drift.deep_sync_profile(_cfg_stub(), "p", catalog)

    assert summary["state"] == "done"
    assert summary.get("note")
    assert called["n"] == 0  # no connector opened


def test_deep_sync_fills_row_count_when_profiler_returns_zero(
    catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """On backends that block the profiler's COUNT(*) (e.g. Databricks),
    profile_table returns row_count=0. Deep sync then runs an exact
    count via _exact_row_count and persists THAT, so Databricks tables
    get real counts instead of a misleading 0."""
    _seed_skeleton(catalog.db_path, "p", [("dbx", "amx_test_schema", "adrc")])

    class _ZeroCountConn:
        def profile_table(self, schema: str, table: str, sample_size: int = 0) -> TableProfile:
            return TableProfile(
                schema=schema,
                name=table,
                asset_kind=AssetKind.TABLE,
                row_count=0,  # profiler blocked the COUNT(*)
                columns=[ColumnProfile(name="id", dtype="int", nullable=True, existing_comment="")],
            )

    monkeypatch.setattr(drift, "_scoped_connector", lambda *a, **k: _ZeroCountConn())
    # Stand in for the exact COUNT(*) the connector would run.
    monkeypatch.setattr(drift, "_exact_row_count", lambda *a, **k: 123456)

    summary = drift.deep_sync_profile(_cfg_stub(), "p", catalog)

    assert summary["state"] == "done"
    with catalog._connect() as conn:  # noqa: SLF001
        row_count = conn.execute(
            "SELECT row_count FROM catalog_entities "
            "WHERE entity_kind = 'table' AND table_name = 'adrc'"
        ).fetchone()[0]
    assert row_count == 123456


def test_deep_sync_continues_past_a_failing_table(
    catalog: SearchCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One table that raises during profiling must not abort the rest
    — it's counted as failed and the others still sync."""
    _seed_skeleton(
        catalog.db_path,
        "p",
        [("bird_train", "s", "good"), ("bird_train", "s", "bad")],
    )

    class _Conn:
        def profile_table(self, schema: str, table: str, sample_size: int = 0) -> TableProfile:
            if table == "bad":
                raise RuntimeError("permission denied")
            return TableProfile(
                schema=schema,
                name=table,
                asset_kind=AssetKind.TABLE,
                row_count=5,
                columns=[ColumnProfile(name="c", dtype="int", nullable=True, existing_comment="")],
            )

    monkeypatch.setattr(drift, "_scoped_connector", lambda *a, **k: _Conn())

    summary = drift.deep_sync_profile(_cfg_stub(), "p", catalog)

    assert summary["state"] == "done"
    assert summary["processed"] == 1
    assert summary["failed"] == 1
