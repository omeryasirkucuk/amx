"""Tests for /api/assets list, detail, and ingest (SSE) endpoints."""

from __future__ import annotations

import sqlite3

import pytest

from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-assets-token-xyz"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    """Build a TestClient with an AMXConfig whose CONFIG_DIR is tmp_path.

    Initialises the history DB and returns (client, db_path).
    """
    from fastapi.testclient import TestClient

    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()

    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


def _seed_notebook(db_path, profile="prod"):
    """Insert one remote_notebooks row and return its id."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 qualified_name, language, source_text, source_hash,
                 last_modified_at, last_modified_by, owner, cell_count, ingested_at)
            VALUES (?, 'databricks', 'ext-1', 'my_nb', '/n', NULL,
                    'python', '{}', 'h', NULL, NULL, NULL, 1,
                    '2026-05-21T00:00:00')
            """,
            (profile,),
        )
        conn.commit()
        return conn.execute("SELECT id FROM remote_notebooks").fetchone()[0]


# ── Task 38: list + detail ──────────────────────────────────────────────────


def test_list_assets_notebooks(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path)
    resp = client.get("/api/assets?profile=prod&type=notebook", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["name"] == "my_nb"


def test_list_assets_empty_for_unknown_profile(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path, profile="prod")
    resp = client.get("/api/assets?profile=nonexistent&type=notebook", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    assert resp.json()["count"] == 0


def test_list_assets_unknown_type_returns_400(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.get("/api/assets?profile=prod&type=banana", headers=_AUTH)
    assert resp.status_code == 400


def test_get_asset_detail_returns_source(tmp_path):
    client, db_path = _make_client(tmp_path)
    nb_id = _seed_notebook(db_path)
    resp = client.get(f"/api/assets/notebook/{nb_id}", headers=_AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["name"] == "my_nb"
    assert "source_text" in body
    assert "downstream_tables" in body
    assert isinstance(body["downstream_tables"], list)


def test_get_asset_detail_404(tmp_path):
    client, db_path = _make_client(tmp_path)
    _seed_notebook(db_path)
    resp = client.get("/api/assets/notebook/99999", headers=_AUTH)
    assert resp.status_code == 404


def test_get_asset_detail_unknown_kind_400(tmp_path):
    client, _db = _make_client(tmp_path)
    resp = client.get("/api/assets/banana/1", headers=_AUTH)
    assert resp.status_code == 400


# ── Task 39: SSE ingest coverage ────────────────────────────────────────────


def test_ingest_endpoint_returns_job_id(monkeypatch, tmp_path):
    """POST /api/assets/ingest returns a job_id and queues background work."""
    import amx.web.routers.assets as a_mod

    client, _ = _make_client(tmp_path)

    async def fake_runner(*, job_id, body, cfg, queue):
        await queue.put(
            {"state": "completed", "counts": {"notebooks": 0, "lineage": 0}, "failures": {}}
        )
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    resp = client.post(
        "/api/assets/ingest",
        json={"profile": "prod", "types": ["notebooks"], "history_days": 7, "runs_per_job": 20},
        headers=_AUTH,
    )
    assert resp.status_code == 202, resp.text
    assert "job_id" in resp.json()


def test_ingest_sse_stream_emits_completion_event(monkeypatch, tmp_path):
    import amx.web.routers.assets as a_mod

    client, _ = _make_client(tmp_path)

    async def fake_runner(*, job_id, body, cfg, queue):
        await queue.put(
            {"state": "completed", "counts": {"notebooks": 1}, "failures": {}}
        )
        await queue.put({"_eof": True})

    monkeypatch.setattr(a_mod, "_run_ingest_job", fake_runner)
    job_id = client.post(
        "/api/assets/ingest",
        json={"profile": "prod", "types": ["notebooks"], "history_days": 7, "runs_per_job": 20},
        headers=_AUTH,
    ).json()["job_id"]

    # Consume the SSE stream; TestClient collects the body once the generator ends.
    with client.stream("GET", f"/api/assets/ingest/{job_id}/events", headers=_AUTH) as r:
        chunks = list(r.iter_text())
    text = "".join(chunks)
    assert "completed" in text
    assert "notebooks" in text


def test_unknown_ingest_job_id_404(tmp_path):
    client, _ = _make_client(tmp_path)
    resp = client.get("/api/assets/ingest/does-not-exist/events", headers=_AUTH)
    assert resp.status_code == 404
