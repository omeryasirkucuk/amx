"""Tests for the local-only comment override path.

Covers the new ``record_user_local_description`` helper on
:class:`SearchCatalog`, the precedence promotion of
``source_kind="user_local"`` over ``"manual"``, and the FTS / search
text mirror update so the override is searchable on the next read.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.search._catalog._constants import SOURCE_PRIORITY
from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def fresh_catalog(tmp_path: Path) -> SearchCatalog:
    db = tmp_path / "history.db"
    SQLiteHistoryStore(db).init()
    return SearchCatalog(db)


def _entity_row(catalog: SearchCatalog) -> dict:
    with catalog._connect() as conn:  # noqa: SLF001
        return dict(conn.execute("SELECT * FROM catalog_entities LIMIT 1").fetchone())


def _description_rows(catalog: SearchCatalog) -> list[dict]:
    with catalog._connect() as conn:  # noqa: SLF001
        return [
            dict(r)
            for r in conn.execute(
                "SELECT id, source_kind, chosen_description, applied_to_db, description_text "
                "FROM catalog_descriptions ORDER BY id"
            ).fetchall()
        ]


def test_source_priority_ranks_user_local_above_manual() -> None:
    assert SOURCE_PRIORITY["user_local"] == 5
    assert SOURCE_PRIORITY["user_local"] > SOURCE_PRIORITY["manual"]


def test_roundtrip_inserts_user_local_row(fresh_catalog: SearchCatalog) -> None:
    result = fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="Net invoice amount in account currency.",
    )
    assert result["entity_id"] > 0
    assert result["description_id"] > 0

    rows = _description_rows(fresh_catalog)
    assert len(rows) == 1
    row = rows[0]
    assert row["source_kind"] == "user_local"
    assert row["chosen_description"] == 1
    assert row["applied_to_db"] == 0
    assert row["description_text"] == "Net invoice amount in account currency."


def test_effective_description_points_at_new_row(fresh_catalog: SearchCatalog) -> None:
    fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="Net invoice amount.",
    )
    entity = _entity_row(fresh_catalog)
    descs = _description_rows(fresh_catalog)
    assert entity["effective_description_id"] == descs[0]["id"]
    assert entity["effective_source_kind"] == "user_local"


def test_user_local_beats_manual_when_both_present(
    fresh_catalog: SearchCatalog,
) -> None:
    fresh_catalog.record_manual_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="OLD manual edit that was written back to the DB.",
    )
    fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="NEW local override that should win.",
    )
    entity = _entity_row(fresh_catalog)
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        winner = conn.execute(
            "SELECT description_text, source_kind FROM catalog_descriptions WHERE id = ?",
            (entity["effective_description_id"],),
        ).fetchone()
    assert winner["source_kind"] == "user_local"
    assert winner["description_text"] == "NEW local override that should win."


def test_two_calls_reuse_entity_row(fresh_catalog: SearchCatalog) -> None:
    first = fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="First note.",
    )
    second = fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="Second note that supersedes the first.",
    )
    assert first["entity_id"] == second["entity_id"]
    assert first["description_id"] != second["description_id"]
    rows = _description_rows(fresh_catalog)
    assert len(rows) == 2


def test_table_level_record_has_null_column_name(fresh_catalog: SearchCatalog) -> None:
    fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name=None,
        entity_kind="table",
        asset_kind="table",
        description="Customer purchase orders, one row per order.",
    )
    entity = _entity_row(fresh_catalog)
    assert entity["column_name"] is None
    assert entity["entity_kind"] == "table"


def test_search_text_includes_user_local(fresh_catalog: SearchCatalog) -> None:
    fresh_catalog.record_user_local_description(
        db_profile="prod-pg",
        db_backend="postgresql",
        database_name="analytics",
        schema_name="sales",
        table_name="orders",
        column_name="amount",
        entity_kind="column",
        asset_kind="table",
        description="Vehicle plate of the courier that fulfilled the order.",
    )
    with fresh_catalog._connect() as conn:  # noqa: SLF001
        entity = conn.execute("SELECT id, search_text FROM catalog_entities").fetchone()
        assert "Vehicle plate" in (entity["search_text"] or "")
        fts_row = conn.execute(
            "SELECT rowid FROM catalog_entities_fts WHERE search_text MATCH 'plate'"
        ).fetchone()
        assert fts_row is not None
        assert int(fts_row["rowid"]) == int(entity["id"])
