"""PR-E: lazy discover tree endpoints."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.db.adapters.remote_asset_types import AssetMetadata, WorkspaceEntry
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.server import create_app

_TEST_TOKEN = "test-discover-tree-token"
_AUTH = {"Authorization": f"Bearer {_TEST_TOKEN}"}


def _make_client(tmp_path):
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    app = create_app(cfg, token=_TEST_TOKEN)
    return TestClient(app), db_path


class _StubConnector:
    def __init__(self, entries=None, leaves=None):
        self._entries = entries or []
        self._leaves = leaves or []

    def list_workspace_children(self, *, parent_path, kind):
        return iter(self._entries)

    def list_remote_notebooks_metadata(self):
        return iter(self._leaves)


def test_tree_get_cache_miss_triggers_adapter_fetch(monkeypatch, tmp_path):
    from amx.cli_support.commands import db_assets_impl as impl_mod

    entries = [
        WorkspaceEntry(
            kind="notebook",
            path="/Users",
            name="Users",
            is_directory=True,
            external_id=None,
            owner=None,
            last_modified=None,
        ),
        WorkspaceEntry(
            kind="notebook",
            path="/Sample.py",
            name="Sample.py",
            is_directory=False,
            external_id="42",
            owner="alice",
            last_modified=datetime(2026, 5, 1, tzinfo=timezone.utc),
        ),
    ]
    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _StubConnector(entries=entries)
    )
    client, _db = _make_client(tmp_path)
    resp = client.get(
        "/api/assets/discover/tree?profile=prod&kind=notebook&parent=", headers=_AUTH
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["cache_empty"] is False
    assert body["parent_path"] == ""
    paths = sorted([r["path"] for r in body["items"]])
    assert paths == ["/Sample.py", "/Users"]


def test_tree_get_cache_hit_does_not_call_adapter(monkeypatch, tmp_path):
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES ('prod', 'notebook', '', '', '', 1, NULL, NULL, NULL, ?, ?)",
            (1716333600.0, 1716333600.0),
        )
        conn.execute(
            "INSERT INTO remote_workspace_tree "
            "(profile_name, kind, path, parent_path, name, is_directory, "
            "external_id, owner, last_modified, children_fetched_at, fetched_at) "
            "VALUES ('prod', 'notebook', '/Cached', '', 'Cached', 1, NULL, "
            "NULL, NULL, NULL, ?)",
            (1716333600.0,),
        )
        conn.commit()

    def _boom(cfg, profile):
        raise AssertionError("adapter must not be opened on cache hit")

    monkeypatch.setattr(impl_mod, "_open_connector", _boom)
    resp = client.get(
        "/api/assets/discover/tree?profile=prod&kind=notebook&parent=", headers=_AUTH
    )
    assert resp.status_code == 200
    body = resp.json()
    assert any(r["path"] == "/Cached" for r in body["items"])
    assert body["cache_empty"] is False


def test_tree_refresh_replaces_only_target_parent(monkeypatch, tmp_path):
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, db_path = _make_client(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            INSERT INTO remote_workspace_tree
                (profile_name, kind, path, parent_path, name, is_directory,
                 external_id, owner, last_modified, children_fetched_at, fetched_at)
            VALUES
                ('prod', 'notebook', '', '', '', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Users', '', 'Users', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Other', '', 'Other', 1, NULL, NULL, NULL, 1.0, 1.0),
                ('prod', 'notebook', '/Users/stale.py', '/Users', 'stale.py',
                 0, '99', NULL, NULL, NULL, 1.0),
                ('prod', 'notebook', '/Other/keep.py', '/Other', 'keep.py',
                 0, '77', NULL, NULL, NULL, 1.0);
            """
        )
        conn.commit()

    fresh = [
        WorkspaceEntry(
            kind="notebook",
            path="/Users/fresh.py",
            name="fresh.py",
            is_directory=False,
            external_id="100",
            owner="alice",
            last_modified=None,
        )
    ]
    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _StubConnector(entries=fresh)
    )
    resp = client.post(
        "/api/assets/discover/tree/refresh?profile=prod&kind=notebook&parent=/Users",
        headers=_AUTH,
    )
    assert resp.status_code == 200, resp.text

    with sqlite3.connect(db_path) as conn:
        children_users = sorted(
            r[0]
            for r in conn.execute(
                "SELECT path FROM remote_workspace_tree "
                "WHERE profile_name='prod' AND parent_path='/Users'"
            ).fetchall()
        )
        children_other = sorted(
            r[0]
            for r in conn.execute(
                "SELECT path FROM remote_workspace_tree "
                "WHERE profile_name='prod' AND parent_path='/Other'"
            ).fetchall()
        )
    assert children_users == ["/Users/fresh.py"]
    assert children_other == ["/Other/keep.py"]


def test_tree_walk_seeds_full_cache(monkeypatch, tmp_path):
    from amx.cli_support.commands import db_assets_impl as impl_mod

    client, db_path = _make_client(tmp_path)
    leaves = [
        AssetMetadata(
            kind="notebook",
            external_id="1",
            name="nb_a.py",
            path="/Users/alice/nb_a.py",
            owner="alice",
            last_modified=None,
        ),
        AssetMetadata(
            kind="notebook",
            external_id="2",
            name="nb_b.py",
            path="/Users/alice/nb_b.py",
            owner="alice",
            last_modified=None,
        ),
    ]
    monkeypatch.setattr(
        impl_mod, "_open_connector", lambda cfg, profile: _StubConnector(leaves=leaves)
    )
    resp = client.post(
        "/api/assets/discover/tree/walk?profile=prod&kind=notebook", headers=_AUTH
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["leaves"] == 2
    assert body["directories"] >= 2

    with sqlite3.connect(db_path) as conn:
        paths = {
            r[0]
            for r in conn.execute(
                "SELECT path FROM remote_workspace_tree "
                "WHERE profile_name='prod' AND kind='notebook'"
            ).fetchall()
        }
    assert "/Users" in paths
    assert "/Users/alice" in paths
    assert "/Users/alice/nb_a.py" in paths
    assert "/Users/alice/nb_b.py" in paths
