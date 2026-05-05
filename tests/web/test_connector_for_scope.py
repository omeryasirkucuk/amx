"""Tests for :func:`amx.web.routers.live_db._connector_for_scope`.

These cover the multi-profile browse refactor: scope is per-request,
``cfg.db_profiles[name]`` is never mutated, the cache key includes the
profile name (so two profiles pointing at the same host:db get distinct
connectors), and unknown profiles yield a 404 not a 500. The
concurrency case mirrors what AMX Studio triggers when the user expands
two profiles' trees in parallel.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from amx.config import AMXConfig, DBConfig
from amx.web.routers import live_db


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture()
def cfg_with_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "local-pg": DBConfig(
            backend="postgresql",
            host="pg.local",
            port=5432,
            user="amx",
            database="appdb",
        ),
        "snowflake-prod": DBConfig(
            backend="snowflake",
            host="acct.snowflakecomputing.com",
            user="amx",
            account="acct",
            warehouse="WH",
            catalog="ANALYTICS",
        ),
    }
    cfg.active_db_profile = "local-pg"
    cfg.db = cfg.db_profiles["local-pg"]
    return cfg


def _stub_connector(monkeypatch) -> list[DBConfig]:
    """Replace DatabaseConnector with a MagicMock factory and capture
    every DBConfig it was constructed with."""
    seen: list[DBConfig] = []

    def _factory(db: DBConfig) -> MagicMock:
        seen.append(db)
        m = MagicMock()
        m.cfg = db
        return m

    monkeypatch.setattr(live_db, "DatabaseConnector", _factory)
    return seen


def test_replace_does_not_mutate_profile(cfg_with_profiles, monkeypatch) -> None:
    """Overlaying ``database`` must produce a fresh DBConfig — the
    record in ``cfg.db_profiles`` keeps its original value."""
    _stub_connector(monkeypatch)
    base = cfg_with_profiles.db_profiles["local-pg"]
    original_db = base.database

    live_db._connector_for_scope(cfg_with_profiles, "local-pg", database="other_db")

    assert cfg_with_profiles.db_profiles["local-pg"].database == original_db
    assert original_db == "appdb"  # sanity


def test_caches_per_profile_and_scope(cfg_with_profiles, monkeypatch) -> None:
    """Two requests for the same (profile, db, catalog) tuple share the
    cached connector. Switching either dimension yields a fresh one."""
    seen = _stub_connector(monkeypatch)

    a1 = live_db._connector_for_scope(cfg_with_profiles, "local-pg", database="appdb")
    a2 = live_db._connector_for_scope(cfg_with_profiles, "local-pg", database="appdb")
    assert a1 is a2
    assert len(seen) == 1

    b = live_db._connector_for_scope(cfg_with_profiles, "local-pg", database="reporting")
    assert b is not a1
    assert len(seen) == 2


def test_profile_name_in_cache_key_disambiguates_same_host(monkeypatch) -> None:
    """Two profiles with identical (host, database, catalog, …) but
    different names must get distinct connectors. Without the profile
    name in the cache key, they would collide and one would shadow
    the other — silently routing one user's writes to the other's
    connection pool."""
    cfg = AMXConfig()
    cfg.db_profiles = {
        "prod_pg": DBConfig(backend="postgresql", host="pg", database="appdb", user="rw"),
        "prod_pg_readonly": DBConfig(backend="postgresql", host="pg", database="appdb", user="ro"),
    }
    seen = _stub_connector(monkeypatch)

    # _profile_key embeds host+db but not user, so without the profile
    # name in the cache key the second call would return the first
    # cached connector. The new key includes the profile name; that
    # yields two distinct entries.
    rw = live_db._connector_for_scope(cfg, "prod_pg")
    ro = live_db._connector_for_scope(cfg, "prod_pg_readonly")
    assert rw is not ro
    assert len(seen) == 2
    assert seen[0].user == "rw"
    assert seen[1].user == "ro"


def test_unknown_profile_returns_404(cfg_with_profiles, monkeypatch) -> None:
    _stub_connector(monkeypatch)
    with pytest.raises(HTTPException) as exc_info:
        live_db._connector_for_scope(cfg_with_profiles, "does-not-exist")
    assert exc_info.value.status_code == 404
    assert "does-not-exist" in str(exc_info.value.detail)


def test_empty_profile_name_returns_400(cfg_with_profiles, monkeypatch) -> None:
    _stub_connector(monkeypatch)
    with pytest.raises(HTTPException) as exc_info:
        live_db._connector_for_scope(cfg_with_profiles, "   ")
    assert exc_info.value.status_code == 400


def test_concurrent_overlay_no_leak(cfg_with_profiles, monkeypatch) -> None:
    """Two threads call ``_connector_for_scope`` with the same profile
    but different ``catalog`` overlays. Each thread's connector must
    bind to its own catalog. The legacy ``cfg.db.catalog = …; finally:
    restore`` pattern would have leaked one thread's overlay into the
    other; the ``replace()``-based helper has no such race."""
    _stub_connector(monkeypatch)

    cfg_with_profiles.active_db_profile = "snowflake-prod"
    cfg_with_profiles.db = cfg_with_profiles.db_profiles["snowflake-prod"]
    base = cfg_with_profiles.db_profiles["snowflake-prod"]
    original_catalog = base.catalog

    results: dict[str, str] = {}

    def _take(catalog_name: str) -> None:
        conn = live_db._connector_for_scope(
            cfg_with_profiles, "snowflake-prod", catalog=catalog_name
        )
        # The connector's underlying DBConfig should reflect the
        # overlay this thread asked for, regardless of what the other
        # thread is doing concurrently.
        results[catalog_name] = conn.cfg.catalog

    t1 = threading.Thread(target=_take, args=("ALPHA",))
    t2 = threading.Thread(target=_take, args=("BETA",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results == {"ALPHA": "ALPHA", "BETA": "BETA"}
    # The shared profile record is untouched.
    assert cfg_with_profiles.db_profiles["snowflake-prod"].catalog == original_catalog


def test_no_overlay_uses_profile_record_directly(cfg_with_profiles, monkeypatch) -> None:
    """When neither ``database`` nor ``catalog`` is overridden, the
    helper passes the profile's existing DBConfig straight through to
    the connector — no spurious ``replace()`` allocation."""
    seen = _stub_connector(monkeypatch)
    live_db._connector_for_scope(cfg_with_profiles, "local-pg")
    assert seen == [cfg_with_profiles.db_profiles["local-pg"]]
