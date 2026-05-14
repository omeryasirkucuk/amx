"""``GET /api/db/cache/search`` smoke tests.

The unit-level coverage lives in ``tests/test_db_cache_search.py``;
this file walks the FastAPI handler end-to-end so the route stays
wired into ``create_app`` and auth + param validation behave.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from amx.storage import sqlite_store as ss
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_store(tmp_path: Path):
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    ss._store = SQLiteHistoryStore(db_path)  # noqa: SLF001
    yield db_path
    ss._store = None  # noqa: SLF001


def _seed_column(db_path: Path, profile: str, schema: str, table: str, column: str) -> None:
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name,
                table_name, column_name, entity_kind, asset_kind,
                search_text, updated_at, last_synced_at
            ) VALUES (?, 'postgresql', 'appdb', ?, ?, NULL, 'table', 'table', '', ?, ?)
            """,
            (profile, schema, table, now, now),
        )
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, db_backend, database_name, schema_name,
                table_name, column_name, entity_kind, asset_kind,
                search_text, updated_at, last_synced_at
            ) VALUES (?, 'postgresql', 'appdb', ?, ?, ?, 'column', 'table', '', ?, ?)
            """,
            (profile, schema, table, column, now, now),
        )
        conn.execute(
            """
            INSERT INTO catalog_profile_state (
                db_profile, state, total_tables, processed_tables,
                started_at, finished_at, last_full_sync_at, last_error
            ) VALUES (?, 'done', 1, 1, ?, ?, ?, '')
            """,
            (profile, now, now, now),
        )


def test_endpoint_returns_match(client, auth_headers, seeded_store: Path) -> None:
    _seed_column(seeded_store, "DBR-OYK", "public", "orders", "customer_id")
    resp = client.get(
        "/api/db/cache/search",
        params={"q": "customer_id"},
        headers=auth_headers,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["query"] == "customer_id"
    assert body["truncated"] is False
    assert len(body["results"]) == 1
    hit = body["results"][0]
    assert hit["profile"] == "DBR-OYK"
    assert hit["column"] == "customer_id"
    assert hit["match_field"] == "column"


def test_endpoint_short_query_returns_empty(client, auth_headers, seeded_store: Path) -> None:
    _seed_column(seeded_store, "prof-a", "public", "orders", "customer_id")
    resp = client.get(
        "/api/db/cache/search",
        params={"q": "a"},
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert resp.json()["results"] == []


def test_endpoint_rejects_invalid_limit(client, auth_headers) -> None:
    too_big = client.get(
        "/api/db/cache/search",
        params={"q": "abc", "limit": 999},
        headers=auth_headers,
    )
    too_small = client.get(
        "/api/db/cache/search",
        params={"q": "abc", "limit": 0},
        headers=auth_headers,
    )
    assert too_big.status_code == 422
    assert too_small.status_code == 422


def test_endpoint_requires_auth(client, seeded_store: Path) -> None:
    resp = client.get("/api/db/cache/search", params={"q": "abc"})
    assert resp.status_code == 401
