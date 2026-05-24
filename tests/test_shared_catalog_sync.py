"""Push/pull of the structural catalog between local and shared stores.

One team member's deep sync (the expensive COUNT(*) pass) should
propagate to teammates through the shared store instead of each member
re-running it. These tests exercise the two halves against a real local
SQLite store and a SQLite-backed shared SQLAlchemy store:

* ``push_catalog_to_shared`` — local structural rows go up, deduped by
  the natural key with last-write-wins on ``last_synced_at``.
* ``pull_catalog_to_local`` — shared rows come down, table-level
  ``column_name`` normalised back to NULL, description link left NULL.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.search.catalog import SearchCatalog
from amx.storage.migration import pull_catalog_to_local, push_catalog_to_shared
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_shared(tmp_path: Path, name: str = "shared") -> SQLAlchemyHistoryStore:
    engine = create_engine(f"sqlite:///{tmp_path / f'{name}.db'}")
    build_metadata(schema="main").create_all(engine)
    return SQLAlchemyHistoryStore(engine, "main")


def _make_local(tmp_path: Path, name: str) -> SQLiteHistoryStore:
    store = SQLiteHistoryStore(tmp_path / f"{name}.db")
    store.init()
    # Ensure the catalog_entities table exists (created by the catalog layer).
    SearchCatalog(tmp_path / f"{name}.db")
    return store


def _seed_local_table(
    store: SQLiteHistoryStore,
    *,
    profile: str,
    database: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    row_count: int,
    synced_at: float,
) -> None:
    """Insert a table-level row (column_name NULL) + its column rows."""
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name, table_name,
                column_name, entity_kind, asset_kind, row_count, search_text,
                updated_at, last_synced_at
            ) VALUES (?, 'postgresql', ?, ?, ?, NULL, 'table', 'table', ?, '', ?, ?)
            """,
            (profile, database, schema, table, row_count, time.time(), synced_at),
        )
        for col, dtype in columns:
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, db_backend, database_name, schema_name, table_name,
                    column_name, entity_kind, asset_kind, dtype, row_count,
                    search_text, updated_at, last_synced_at
                ) VALUES (?, 'postgresql', ?, ?, ?, ?, 'column', 'table', ?, ?, '', ?, ?)
                """,
                (profile, database, schema, table, col, dtype, row_count,
                 time.time(), synced_at),
            )


def _local_rows(store: SQLiteHistoryStore, schema: str, table: str) -> list[dict]:
    with store._connect() as conn:  # noqa: SLF001
        return [
            dict(r)
            for r in conn.execute(
                "SELECT column_name, entity_kind, row_count, dtype, "
                "effective_description_id, last_synced_at "
                "FROM catalog_entities WHERE schema_name = ? AND table_name = ? "
                "ORDER BY entity_kind, column_name",
                (schema, table),
            )
        ]


def test_push_then_pull_round_trips_structure(tmp_path: Path) -> None:
    """A deep-synced table on machine A reaches machine B's local
    catalog through the shared store — columns + row count intact."""
    shared = _make_shared(tmp_path)
    local_a = _make_local(tmp_path, "a")
    _seed_local_table(
        local_a,
        profile="p",
        database="bird_train",
        schema="app_store",
        table="playstore",
        columns=[("App", "text"), ("Rating", "double")],
        row_count=10840,
        synced_at=1_700_000_000.0,
    )

    pushed = push_catalog_to_shared(local_a, shared)
    assert pushed == 3  # 1 table + 2 columns

    # Fresh machine B pulls.
    local_b = _make_local(tmp_path, "b")
    pulled = pull_catalog_to_local(local_b, shared)
    assert pulled == 3

    rows = _local_rows(local_b, "app_store", "playstore")
    assert len(rows) == 3
    table_row = next(r for r in rows if r["entity_kind"] == "table")
    assert table_row["row_count"] == 10840
    # Table-level column_name normalised back to NULL on the pull.
    assert table_row["column_name"] is None
    # Description link left NULL — descriptions flow via the run path.
    assert table_row["effective_description_id"] is None
    col_names = {r["column_name"] for r in rows if r["entity_kind"] == "column"}
    assert col_names == {"App", "Rating"}


def test_pull_last_write_wins(tmp_path: Path) -> None:
    """A newer shared snapshot overwrites an older local row; an older
    shared snapshot does not clobber a newer local one."""
    shared = _make_shared(tmp_path)
    local_a = _make_local(tmp_path, "a")

    # A pushes an OLD snapshot (row_count 100).
    _seed_local_table(
        local_a, profile="p", database="d", schema="s", table="t",
        columns=[("c", "int")], row_count=100, synced_at=1_000.0,
    )
    push_catalog_to_shared(local_a, shared)

    local_b = _make_local(tmp_path, "b")
    pull_catalog_to_local(local_b, shared)
    assert next(r for r in _local_rows(local_b, "s", "t")
                if r["entity_kind"] == "table")["row_count"] == 100

    # A re-syncs with a NEWER snapshot (row_count 999).
    with local_a._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE catalog_entities SET row_count = 999, last_synced_at = 2000.0 "
            "WHERE schema_name = 's' AND table_name = 't'"
        )
    push_catalog_to_shared(local_a, shared)

    # B pulls again — the newer count wins.
    pull_catalog_to_local(local_b, shared)
    assert next(r for r in _local_rows(local_b, "s", "t")
                if r["entity_kind"] == "table")["row_count"] == 999


def test_push_empty_local_is_noop(tmp_path: Path) -> None:
    shared = _make_shared(tmp_path)
    local = _make_local(tmp_path, "empty")
    assert push_catalog_to_shared(local, shared) == 0


def test_push_scoped_to_profile(tmp_path: Path) -> None:
    """Pushing with a db_profile filter only sends that profile's rows."""
    shared = _make_shared(tmp_path)
    local = _make_local(tmp_path, "a")
    _seed_local_table(
        local, profile="p1", database="d", schema="s", table="t1",
        columns=[("c", "int")], row_count=5, synced_at=1_000.0,
    )
    _seed_local_table(
        local, profile="p2", database="d", schema="s", table="t2",
        columns=[("c", "int")], row_count=5, synced_at=1_000.0,
    )
    pushed = push_catalog_to_shared(local, shared, db_profile="p1")
    assert pushed == 2  # only p1's table + column
    fetched = shared.fetch_catalog_entities()
    assert {r["db_profile"] for r in fetched} == {"p1"}
