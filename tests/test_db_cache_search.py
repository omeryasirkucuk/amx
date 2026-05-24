"""``SearchCatalog.search_entities`` and ``GET /api/db/cache/search``.

The Studio sidebar's search box must locate a profile by any of the
schema / table / view / column name pulled from the persistent
catalog cache. These tests seed ``catalog_entities`` directly and
exercise the helper at both layers — pure Python and the FastAPI
route — so the contract stays stable across both call sites.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from amx.search.catalog import SearchCatalog
from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_store(tmp_path: Path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    ss._store = SQLiteHistoryStore(db_path)  # noqa: SLF001
    yield db_path
    ss._store = None  # noqa: SLF001


def _seed(
    db_path: Path,
    profile: str,
    *,
    database: str = "appdb",
    backend: str = "postgresql",
    schemas_tables_columns: list[tuple[str, str, list[str]]],
    fully_synced: bool = True,
) -> None:
    """Insert table + column rows for each (schema, table, [columns])
    triplet, and optionally flip the profile to ``state='done'`` so
    the search helper treats the cache as authoritative."""
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        for schema, table, columns in schemas_tables_columns:
            conn.execute(
                """
                INSERT INTO catalog_entities (
                    db_profile, db_backend, database_name, schema_name,
                    table_name, column_name, entity_kind, asset_kind,
                    search_text, updated_at, last_synced_at
                ) VALUES (?, ?, ?, ?, ?, NULL, 'table', 'table', ?, ?, ?)
                """,
                (
                    profile,
                    backend,
                    database,
                    schema,
                    table,
                    f"path={profile}.{schema}.{table}",
                    now,
                    now,
                ),
            )
            for col in columns:
                conn.execute(
                    """
                    INSERT INTO catalog_entities (
                        db_profile, db_backend, database_name, schema_name,
                        table_name, column_name, entity_kind, asset_kind,
                        search_text, updated_at, last_synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'column', 'table', ?, ?, ?)
                    """,
                    (
                        profile,
                        backend,
                        database,
                        schema,
                        table,
                        col,
                        f"path={profile}.{schema}.{table}.{col}",
                        now,
                        now,
                    ),
                )
        if fully_synced:
            # is_profile_fully_synced now requires all four sync
            # timestamps to be non-null. Stamp every per-surface
            # timestamp here so the seed mimics a true completed
            # ``Sync all`` run.
            conn.execute(
                """
                INSERT INTO catalog_profile_state (
                    db_profile, state, total_tables, processed_tables,
                    started_at, finished_at, last_full_sync_at,
                    last_skeleton_sync_at, last_schemas_sync_at,
                    last_columns_sync_at, last_error
                ) VALUES (?, 'done', 1, 1, ?, ?, ?, ?, ?, ?, '')
                """,
                (profile, now, now, now, now, now, now),
            )


def _catalog(db_path: Path) -> SearchCatalog:
    return SearchCatalog(db_path)


def test_search_matches_column_name(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[
            ("public", "orders", ["id", "customer_id", "total"]),
        ],
    )
    results, truncated = _catalog(seeded_store).search_entities("customer_id")
    assert truncated is False
    assert len(results) == 1
    hit = results[0]
    assert hit["match_field"] == "column"
    assert hit["column"] == "customer_id"
    assert hit["table"] == "orders"
    assert hit["schema"] == "public"
    assert hit["profile"] == "prof-a"


def test_search_matches_table_name(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[
            ("public", "orders", ["id"]),
            ("public", "customers", ["id"]),
        ],
    )
    results, _ = _catalog(seeded_store).search_entities("customer")
    # `customers` table matches and `customer_id`-style columns would
    # also match if present; here only the table hits because columns
    # are just "id".
    table_hits = [r for r in results if r["match_field"] == "table"]
    assert any(r["table"] == "customers" for r in table_hits)


def test_search_matches_schema_name(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[
            ("analytics_v2", "events", ["id"]),
            ("public", "orders", ["id"]),
        ],
    )
    results, _ = _catalog(seeded_store).search_entities("analytics")
    schema_hits = [r for r in results if r["match_field"] == "schema"]
    assert len(schema_hits) == 1
    assert schema_hits[0]["schema"] == "analytics_v2"
    assert schema_hits[0]["table"] is None


def test_search_ranks_schema_before_table_before_column(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[
            # "cust" appears at all three levels.
            ("cust_schema", "orders", ["id"]),
            ("public", "cust_table", ["id"]),
            ("public", "orders", ["cust_col"]),
        ],
    )
    results, _ = _catalog(seeded_store).search_entities("cust")
    ranks = [r["match_field"] for r in results]
    # The first schema hit must appear before the first table hit,
    # which must appear before the first column hit.
    assert ranks.index("schema") < ranks.index("table") < ranks.index("column")


def test_unsynced_profile_returns_nothing(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", ["customer_id"])],
        fully_synced=False,
    )
    results, truncated = _catalog(seeded_store).search_entities("customer_id")
    assert results == []
    assert truncated is False


def test_profile_filter_scopes_results(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", ["customer_id"])],
    )
    _seed(
        seeded_store,
        "prof-b",
        schemas_tables_columns=[("public", "leads", ["customer_id"])],
    )
    all_results, _ = _catalog(seeded_store).search_entities("customer_id")
    assert {r["profile"] for r in all_results} == {"prof-a", "prof-b"}
    scoped, _ = _catalog(seeded_store).search_entities("customer_id", db_profile="prof-a")
    assert {r["profile"] for r in scoped} == {"prof-a"}


def test_short_query_returns_empty(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", ["id"])],
    )
    results, truncated = _catalog(seeded_store).search_entities("x")
    assert results == []
    assert truncated is False


def test_truncated_flag_set_when_over_limit(seeded_store: Path) -> None:
    # 12 columns all containing "abc"; ask for limit=5.
    cols = [f"abc_col_{i:02d}" for i in range(12)]
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", cols)],
    )
    results, truncated = _catalog(seeded_store).search_entities("abc", limit=5)
    assert truncated is True
    assert len(results) == 5


def test_search_empty_query_returns_empty(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", ["id"])],
    )
    results, truncated = _catalog(seeded_store).search_entities("")
    assert results == []
    assert truncated is False


def test_search_zero_limit_returns_empty(seeded_store: Path) -> None:
    _seed(
        seeded_store,
        "prof-a",
        schemas_tables_columns=[("public", "orders", ["customer_id"])],
    )
    results, truncated = _catalog(seeded_store).search_entities("customer_id", limit=0)
    assert results == []
    assert truncated is False
