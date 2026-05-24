"""Sidebar cache-first contract.

Asserts that every live-fallback path of the live-DB router writes the
results back into ``catalog_entities`` so the *next* expand of the same
node serves from cache (zero connector calls). The four live paths
covered are catalogs, databases, schemas, and assets.

Cache reads are mocked at the ``SearchCatalog.from_history_store``
seam — the goal here is to verify that the route reaches the
write-through helper, not the SQLite schema itself (those are covered
by the catalog crud tests).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.db.connector import AssetKind
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture(autouse=True)
def _register_databricks_profile(cfg) -> None:
    """Databricks (3-level) so the ``/catalogs`` route exercises the
    live-DB fallback instead of returning early."""
    cfg.db_profiles[PROFILE] = DBConfig(backend="databricks", host="dbc.test", access_token="x")


class _FakeCatalog:
    """In-memory stub for the SearchCatalog upsert path.

    Mirrors only the surface the write-through helpers actually touch:
    a ``_connect`` context manager and an ``_upsert_entity`` method
    that records the rows it would have written. Tests inspect the
    ``rows`` list to verify behaviour.
    """

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def _connect(self):  # noqa: D401 — protocol shape
        outer = self

        class _Ctx:
            def __enter__(self_inner):
                return outer

            def __exit__(self_inner, *_a):
                return False

        return _Ctx()

    def _upsert_entity(self, _conn, **kwargs):  # noqa: D401 — protocol shape
        self.rows.append(kwargs)
        return len(self.rows)


def _install_fake_catalog(monkeypatch) -> _FakeCatalog:
    """Make every ``SearchCatalog.from_history_store()`` resolve to a
    fresh ``_FakeCatalog`` so the test owns the write-through sink."""
    fake = _FakeCatalog()

    import amx.search.catalog as catalog_mod

    monkeypatch.setattr(
        catalog_mod.SearchCatalog,
        "from_history_store",
        classmethod(lambda _cls: fake),
    )
    # Force cache reads to miss so the route falls through to live.
    monkeypatch.setattr(live_db, "_cached_catalog_inventory", lambda *_a, **_kw: None)
    monkeypatch.setattr(live_db, "_cached_schemas_for_profile", lambda *_a, **_kw: None)
    monkeypatch.setattr(live_db, "_cached_assets_for_profile_schema", lambda *_a, **_kw: None)
    return fake


def _q(extra: str = "") -> str:
    base = f"?profile={PROFILE}"
    return base + (f"&{extra}" if extra else "")


def _patch_connector(monkeypatch, builder) -> None:
    instance = builder()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)


def test_catalogs_live_path_writes_through(client, auth_headers, monkeypatch) -> None:
    """Cache miss → live ``list_catalogs`` → row per catalog upserted."""
    fake = _install_fake_catalog(monkeypatch)
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=True),
            list_catalogs=MagicMock(return_value=["main", "analytics"]),
        ),
    )

    resp = client.get(f"/api/live/catalogs{_q()}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["source"] == "live"

    names = sorted(r["database_name"] for r in fake.rows)
    assert names == ["analytics", "main"]
    assert all(r["entity_kind"] == "database" for r in fake.rows)


def test_schemas_live_path_writes_through(client, auth_headers, monkeypatch, cfg) -> None:
    """Cache miss on a Postgres-style 2-level profile → live
    ``list_schemas`` → schema marker per schema."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    fake = _install_fake_catalog(monkeypatch)
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_schemas=MagicMock(return_value=["public", "sales"]),
            get_schema_comment=MagicMock(return_value=""),
        ),
    )

    resp = client.get(f"/api/live/schemas{_q('database=appdb')}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["source"] == "live"

    schemas = sorted(r["schema_name"] for r in fake.rows)
    assert schemas == ["public", "sales"]
    assert all(r["entity_kind"] == "schema" for r in fake.rows)
    assert all(r["database_name"] == "appdb" for r in fake.rows)


def test_assets_live_path_writes_through(client, auth_headers, monkeypatch, cfg) -> None:
    """Cache miss → live ``list_assets`` → one ``table`` row per asset."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    fake = _install_fake_catalog(monkeypatch)
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_assets=MagicMock(
                return_value=[
                    ("orders", AssetKind.TABLE),
                    ("orders_view", AssetKind.VIEW),
                ]
            ),
            get_table_comment=MagicMock(return_value=""),
        ),
    )

    resp = client.get(f"/api/live/schemas/sales/assets{_q('database=appdb')}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["source"] == "live"

    by_name = {r["table_name"]: r for r in fake.rows}
    assert set(by_name) == {"orders", "orders_view"}
    assert by_name["orders"]["entity_kind"] == "table"
    assert by_name["orders"]["asset_kind"] == "table"
    assert by_name["orders_view"]["asset_kind"] == "view"
    assert all(r["schema_name"] == "sales" for r in fake.rows)
    assert all(r["database_name"] == "appdb" for r in fake.rows)


def test_databases_live_path_writes_through(client, auth_headers, monkeypatch, cfg) -> None:
    """Cache miss on a 2-level profile → live ``list_databases`` →
    one ``database`` marker row per database."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    fake = _install_fake_catalog(monkeypatch)
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_databases=MagicMock(return_value=["app", "analytics"])),
    )

    resp = client.get(f"/api/live/databases{_q()}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["source"] == "live"

    names = sorted(r["database_name"] for r in fake.rows)
    assert names == ["analytics", "app"]
    assert all(r["entity_kind"] == "database" for r in fake.rows)
