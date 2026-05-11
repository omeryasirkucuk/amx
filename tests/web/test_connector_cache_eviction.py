"""Profile upsert / delete must wipe the live_db connector cache for
that profile.

Two correctness scenarios this guards against:

1. **Credential edits.** ``_profile_key`` doesn't include the password
   or access token, so editing just the password leaves the cached
   key unchanged. Without eviction the next request finds the old
   connector and keeps using the stale credentials.
2. **Delete leaves orphans.** A deleted profile's connector would
   otherwise sit in the cache until LRU eviction, holding pool
   handles for a profile that no longer exists.

Both cases manifested as the user-visible "delete pretends to work
but doesn't" / "edit needs a page refresh" reports.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.web.routers import live_db


@pytest.fixture(autouse=True)
def _wipe_connector_cache():
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def _stub_entry(profile_name: str, database: str = "db") -> tuple[tuple, MagicMock]:
    """Drop a fake connector under a key that matches the shape
    ``_connector_for_scope`` produces. The ``database`` slot lets a
    test register two distinct entries for the same profile (mirrors
    real life: a profile that's been queried with two overlays caches
    two connectors)."""
    key = (profile_name, "postgresql", "h", database, "", "", "", "", "", "", "")
    fake = MagicMock()
    live_db._CONNECTOR_CACHE[key] = fake
    return key, fake


def test_evict_drops_every_entry_for_profile():
    _, fake_a = _stub_entry("alpha", database="db1")
    _, fake_b = _stub_entry("alpha", database="db2")
    _, fake_c = _stub_entry("beta", database="db1")

    removed = live_db.evict_connector_cache("alpha")

    assert removed == 2
    fake_a.close.assert_called_once()
    fake_b.close.assert_called_once()
    fake_c.close.assert_not_called()
    # The other profile's entry is still in the cache.
    assert any(k[0] == "beta" for k in live_db._CONNECTOR_CACHE)
    assert not any(k[0] == "alpha" for k in live_db._CONNECTOR_CACHE)


def test_evict_no_match_returns_zero():
    _stub_entry("alpha")
    assert live_db.evict_connector_cache("does-not-exist") == 0
    # Pre-existing entry untouched.
    assert len(live_db._CONNECTOR_CACHE) == 1


def test_evict_empty_profile_name_is_safe():
    _stub_entry("alpha")
    assert live_db.evict_connector_cache("") == 0
    assert live_db.evict_connector_cache("   ") == 0
    assert len(live_db._CONNECTOR_CACHE) == 1


def test_evict_swallows_close_errors():
    """A misbehaving connector's ``close()`` must not block eviction."""
    key, fake = _stub_entry("alpha")
    fake.close.side_effect = RuntimeError("boom")
    # Should not raise — the cache entry is still removed.
    assert live_db.evict_connector_cache("alpha") == 1
    assert key not in live_db._CONNECTOR_CACHE


def test_upsert_db_evicts_old_connector(client, auth_headers, monkeypatch, cfg):
    """End-to-end: a profile update through the API drops the cached
    connector for that profile so the next request rebuilds with the
    just-saved DBConfig."""
    cfg.db_profiles["my-pg"] = DBConfig(
        backend="postgresql", host="old.host", user="amx", database="appdb"
    )
    _, fake = _stub_entry("my-pg")
    response = client.put(
        "/api/profiles/db/my-pg",
        headers=auth_headers,
        json={"backend": "postgresql", "host": "new.host", "user": "amx", "database": "appdb"},
    )
    assert response.status_code == 200
    fake.close.assert_called_once()
    assert not any(k[0] == "my-pg" for k in live_db._CONNECTOR_CACHE)


def test_delete_db_evicts_connector(client, auth_headers, cfg):
    cfg.db_profiles["my-pg"] = DBConfig(
        backend="postgresql", host="h", user="amx", database="appdb"
    )
    _, fake = _stub_entry("my-pg")
    response = client.delete("/api/profiles/db/my-pg", headers=auth_headers)
    assert response.status_code == 200
    fake.close.assert_called_once()
    assert not any(k[0] == "my-pg" for k in live_db._CONNECTOR_CACHE)
