"""Tests for the /api/admin FastAPI router.

Uses FastAPI's TestClient against a real in-memory SQLite-backed shared store
seeded with the shared schema.  The caller identity is patched via
``amx.web.routers.admin._caller_identity`` so each test can simulate a
different OS user (admin vs. viewer) without touching the OS environment.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from amx.storage.admin import register_session
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.web.server import create_app

SCHEMA = "main"
TOKEN = "test-token-admin-api"


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_store(db_path: Path) -> SQLAlchemyHistoryStore:
    engine = create_engine(f"sqlite:///{db_path}")
    md = build_metadata(schema=SCHEMA)
    md.create_all(engine)
    store = SQLAlchemyHistoryStore.__new__(SQLAlchemyHistoryStore)
    store.engine = engine
    store.schema = SCHEMA
    store._md = md
    store._t_runs = md.tables[f"{SCHEMA}.analysis_runs"]
    store._t_results = md.tables[f"{SCHEMA}.run_results"]
    store._t_events = md.tables[f"{SCHEMA}.app_events"]
    store._t_session = md.tables[f"{SCHEMA}.session_state"]
    store._t_meta = md.tables[f"{SCHEMA}.schema_meta"]
    store._hostname = "test-host"
    store._username = "test-user"
    store._client_version = "0.14.0-test"
    return store


@pytest.fixture()
def shared(tmp_path: Path) -> SQLAlchemyHistoryStore:
    return _make_store(tmp_path / "admin_api_test.db")


@pytest.fixture()
def client(shared, tmp_path):
    from amx.config import AMXConfig

    cfg = AMXConfig()
    app = create_app(cfg, token=TOKEN, static_root=tmp_path / "static")
    app.dependency_overrides  # ensure no leftover overrides

    with patch("amx.web.routers.admin._get_shared_store", return_value=shared):
        with patch("amx.web.permissions._caller_identity", return_value=("alice", "box1")):
            tc = TestClient(app, raise_server_exceptions=True)
            yield tc, shared


def _seed(shared):
    admin_rec = register_session(
        shared,
        username="alice",
        hostname="box1",
        client_version="0.14.0",
        db_profiles_seen=[],
    )
    viewer_rec = register_session(
        shared,
        username="bob",
        hostname="box2",
        client_version="0.14.0",
        db_profiles_seen=[],
    )
    return admin_rec, viewer_rec


def _auth():
    return {"Authorization": f"Bearer {TOKEN}"}


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_list_members(client):
    """GET /api/admin/members returns the workspace member list."""
    c, shared = client
    _seed(shared)

    resp = c.get("/api/admin/members", headers=_auth())
    assert resp.status_code == 200
    data = resp.json()
    assert "members" in data
    assert data["count"] == 2
    usernames = {m["username"] for m in data["members"]}
    assert "alice" in usernames
    assert "bob" in usernames


def test_promote_happy_path(client):
    """POST /api/admin/promote succeeds when caller is admin."""
    c, shared = client
    admin_rec, viewer_rec = _seed(shared)

    with patch("amx.web.routers.admin._caller_identity", return_value=("alice", "box1")):
        resp = c.post(
            "/api/admin/promote",
            json={"username": "bob"},
            headers=_auth(),
        )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["ok"] is True
    assert data["new_role"] == "admin"

    from amx.storage.admin import current_role

    assert current_role(shared, username="bob", hostname="box2") == "admin"


def test_viewer_promote_denied_403(client):
    """POST /api/admin/promote returns 403 when caller is a viewer."""
    c, shared = client
    _seed(shared)

    # Bob is a viewer — his attempts to promote must be denied.
    with patch("amx.web.routers.admin._caller_identity", return_value=("bob", "box2")):
        resp = c.post(
            "/api/admin/promote",
            json={"username": "alice"},
            headers=_auth(),
        )

    assert resp.status_code == 403
    detail = resp.json()["detail"]
    assert detail["error"] == "permission_denied"
    assert detail["required_role"] == "admin"


def test_list_audit(client):
    """GET /api/admin/audit returns recent audit events."""
    c, shared = client
    admin_rec, viewer_rec = _seed(shared)

    # Promote bob to generate a promote_admin audit row.
    from amx.storage.admin import promote_to_admin

    promote_to_admin(shared, actor_user_id=admin_rec.id, target_user_id=viewer_rec.id)

    resp = c.get("/api/admin/audit", headers=_auth())
    assert resp.status_code == 200
    data = resp.json()
    assert "events" in data
    actions = [ev["action"] for ev in data["events"]]
    assert "promote_admin" in actions
