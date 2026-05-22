"""PR-C (scale): pagination + substring filter on ``GET /api/assets``.

The legacy ``SELECT *`` shape returned every row in one payload, so
a 5,000-notebook workspace fired 5,000 rows on Studio first paint.
The router now caps each response to ``limit`` (default 100, max
500), honours ``offset``, and accepts a case-insensitive ``q``
filter against both the kind's name column and its natural path
column.
"""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-pagination-token"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


def _seed_notebooks(db_path, count: int, *, profile: str = "prod"):
    """Bulk-insert ``count`` notebooks with predictable names + paths."""
    with sqlite3.connect(db_path) as conn:
        for i in range(count):
            name = f"nb_{i:04d}" if i % 3 else f"etl_{i:04d}"
            path = f"/team-{'a' if i % 2 else 'b'}/{name}"
            conn.execute(
                """
                INSERT INTO remote_notebooks
                    (profile_name, platform, external_id, name, workspace_path,
                     qualified_name, language, source_text, source_hash,
                     last_modified_at, last_modified_by, owner, cell_count, ingested_at)
                VALUES (?, 'databricks', ?, ?, ?, NULL, 'python', '{}', 'h',
                        NULL, NULL, NULL, 1, '2026-05-21')
                """,
                (profile, f"ext-{i}", name, path),
            )
        conn.commit()


def test_list_assets_paginates_by_default(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebooks(db_path, count=250)
    resp = client.get("/api/assets?profile=prod&type=notebook", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Default limit is 100; total reflects every matching row.
    assert body["count"] == 100
    assert body["total"] == 250
    assert body["has_more"] is True
    assert body["offset"] == 0
    assert body["limit"] == 100


def test_list_assets_offset_returns_next_page(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebooks(db_path, count=250)
    page2 = client.get(
        "/api/assets?profile=prod&type=notebook&offset=100&limit=100", headers=_AUTH
    ).json()
    assert page2["count"] == 100
    assert page2["offset"] == 100
    page3 = client.get(
        "/api/assets?profile=prod&type=notebook&offset=200&limit=100", headers=_AUTH
    ).json()
    assert page3["count"] == 50
    assert page3["has_more"] is False


def test_list_assets_q_filters_on_name(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebooks(db_path, count=30)
    # One third of the seeded notebooks have an ``etl_`` prefix.
    resp = client.get("/api/assets?profile=prod&type=notebook&q=etl", headers=_AUTH).json()
    assert resp["total"] >= 10
    assert all("etl" in r["name"].lower() for r in resp["items"])


def test_list_assets_q_filters_on_path(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebooks(db_path, count=30)
    resp = client.get("/api/assets?profile=prod&type=notebook&q=team-a", headers=_AUTH).json()
    # Half the seeded notebooks live under /team-a/.
    assert resp["total"] >= 14
    for row in resp["items"]:
        assert "team-a" in (row.get("workspace_path") or "").lower()


def test_list_assets_q_case_insensitive(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebooks(db_path, count=10)
    lower = client.get("/api/assets?profile=prod&type=notebook&q=etl", headers=_AUTH).json()
    upper = client.get("/api/assets?profile=prod&type=notebook&q=ETL", headers=_AUTH).json()
    assert lower["total"] == upper["total"]
    assert lower["total"] > 0


def test_list_assets_limit_clamped(tmp_path):
    """``limit > 500`` rejected by FastAPI's range validator."""
    client, _db = _make_client(tmp_path)
    resp = client.get("/api/assets?profile=prod&type=notebook&limit=1000", headers=_AUTH)
    assert resp.status_code == 422
