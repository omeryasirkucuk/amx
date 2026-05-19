"""Tests for the ``require_writer_role`` permission dependency.

Verifies that viewer-role users get 403 on write endpoints and that
admin/writer-role users can proceed.  Tests target the ``/api/pages``
router because it has the simplest setup (no external DB needed, and the
mock pages service is easy to construct).

The shared history store is patched via
``amx.storage.factory.history_store`` so the dependency can find a real
``_amx_users`` table without a real remote database.
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
TOKEN = "test-token-perm"


# ── Shared store fixture ───────────────────────────────────────────────────────


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
    return _make_store(tmp_path / "perm_test.db")


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
        username="carol",
        hostname="box3",
        client_version="0.14.0",
        db_profiles_seen=[],
    )
    return admin_rec, viewer_rec


def _auth():
    return {"Authorization": f"Bearer {TOKEN}"}


def _make_app(tmp_path):
    from amx.config import AMXConfig

    cfg = AMXConfig()
    return create_app(cfg, token=TOKEN, static_root=tmp_path / "static")


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_viewer_cannot_post_to_pages(shared, tmp_path):
    """A viewer gets 403 when trying to POST /api/pages."""
    _seed(shared)
    app = _make_app(tmp_path)

    with (
        patch("amx.web.permissions._caller_identity", return_value=("carol", "box3")),
        patch("amx.web.permissions.history_store", return_value=shared, create=True),
    ):
        # Monkey-patch the inline import inside require_writer_role.
        import amx.storage.factory as _factory_mod

        orig = _factory_mod.history_store

        def _patched_hs():
            return shared

        _factory_mod.history_store = _patched_hs
        try:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post(
                "/api/pages",
                json={"title": "Test page"},
                headers=_auth(),
            )
        finally:
            _factory_mod.history_store = orig

    assert resp.status_code == 403
    detail = resp.json()["detail"]
    assert detail["error"] == "permission_denied"
    assert detail["your_role"] == "viewer"


def test_admin_can_post_to_pages(shared, tmp_path):
    """An admin gets past the permission check for POST /api/pages.

    The actual page creation may fail (no real history DB wired to the
    pages service) but the failure must NOT be 403 — it proves the
    permission layer lets the admin through.
    """
    _seed(shared)
    app = _make_app(tmp_path)

    import amx.storage.factory as _factory_mod

    orig = _factory_mod.history_store

    def _patched_hs():
        return shared

    _factory_mod.history_store = _patched_hs
    try:
        with patch("amx.web.permissions._caller_identity", return_value=("alice", "box1")):
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post(
                "/api/pages",
                json={"title": "Admin page"},
                headers=_auth(),
            )
    finally:
        _factory_mod.history_store = orig

    # Must NOT be 403 (permission check passed); 503 or 201 are both fine.
    assert resp.status_code != 403, f"Expected non-403 for admin, got {resp.status_code}"
