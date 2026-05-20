"""Live-DB columns endpoint — cache fallback + write-through tests.

Covers the resilience added on top of ``list_columns`` and
``table_snapshot`` so that:

1. A table whose ``catalog_entities`` has only a skeleton table-level
   row but whose ``column_comments_cache`` has a populated columns map
   still surfaces the columns to Studio.
2. A live introspector call that returns non-empty columns gets
   written through to ``catalog_entities`` so the next visit is
   served from the cache instead of hitting the live DB again.
3. A live snapshot that returns 0 columns falls back to the same
   cache so the Studio Table page doesn't render
   "no introspectable columns" for a table whose metadata is known
   to the catalog.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.search.catalog import SearchCatalog
from amx.storage import sqlite_store as ss
from amx.storage._history_caches import save_column_comments_cache
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture()
def history(tmp_path: Path, monkeypatch):
    """Real on-disk history store bound as the module singleton so
    ``SearchCatalog.from_history_store()`` returns something usable."""
    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    store = SQLiteHistoryStore(db_path)
    monkeypatch.setattr(ss, "_store", store, raising=False)
    yield store
    monkeypatch.setattr(ss, "_store", None, raising=False)


@pytest.fixture(autouse=True)
def _register_profile(cfg) -> None:
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql",
        host="pg.test",
        user="amx",
        database="appdb",
    )


def _q(extra: str = "") -> str:
    base = f"?profile={PROFILE}"
    return base + (f"&{extra}" if extra else "")


def _patch_connector(monkeypatch, builder) -> MagicMock:
    instance = builder()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)
    return instance


def test_columns_endpoint_falls_back_to_comments_cache(
    client, auth_headers, monkeypatch, history
) -> None:
    """When catalog_entities has no column rows, the
    column_comments_cache row populated by a prior snapshot read is
    surfaced to Studio via the cache-first reader. The live
    introspector is never even called in this path."""
    save_column_comments_cache(
        history,
        db_profile=PROFILE,
        database="appdb",
        schema="sales",
        entries={
            "orders": {
                "table_comment": "Customer purchase orders",
                "columns": {
                    "order_id": "Primary key",
                    "amount": "Net total",
                },
                "kind": "TABLE",
            }
        },
    )
    live_mock = MagicMock(list_column_profiles=MagicMock(return_value=[]))
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: live_mock)
    response = client.get(
        f"/api/live/schemas/sales/tables/orders/columns{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    # Cache-first wins over the live mock: source is "catalog" (the
    # unified cache-first reader covers both ``catalog_entities`` and
    # ``column_comments_cache``).
    assert payload["source"] == "catalog"
    assert {c["name"] for c in payload["columns"]} == {"order_id", "amount"}
    # The live mock must NOT have been called — cache-first short-circuits.
    live_mock.list_column_profiles.assert_not_called()


def test_columns_endpoint_uses_cache_fallback_when_force_live_and_live_empty(
    client, auth_headers, monkeypatch, history
) -> None:
    """``?force_live=true`` bypasses the cache-first path. When live
    also returns empty (NoSuchTableError swallowed), the endpoint
    salvages from column_comments_cache and tags the source so the
    SPA can show a degraded state."""
    save_column_comments_cache(
        history,
        db_profile=PROFILE,
        database="appdb",
        schema="sales",
        entries={
            "orders": {
                "table_comment": "Customer purchase orders",
                "columns": {"order_id": "Primary key"},
                "kind": "TABLE",
            }
        },
    )
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_column_profiles=MagicMock(return_value=[])),
    )
    response = client.get(
        f"/api/live/schemas/sales/tables/orders/columns{_q('database=appdb&force_live=true')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cache-fallback"
    assert {c["name"] for c in payload["columns"]} == {"order_id"}


def test_columns_endpoint_writes_through_to_catalog(
    client, auth_headers, monkeypatch, history, tmp_path
) -> None:
    """A live introspector hit warms the catalog so the next visit
    serves from the cache instead of hitting live again."""
    cp1 = MagicMock()
    cp1.name = "id"
    cp1.dtype = "BIGINT"
    cp1.nullable = False
    cp2 = MagicMock()
    cp2.name = "email"
    cp2.dtype = "VARCHAR"
    cp2.nullable = True
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_column_profiles=MagicMock(return_value=[cp1, cp2])),
    )

    response = client.get(
        f"/api/live/schemas/sales/tables/customers/columns{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json()["source"] == "live"

    catalog = SearchCatalog.from_history_store()
    assert catalog is not None
    cached = catalog.fetch_columns_for_table(
        PROFILE,
        schema_name="sales",
        table_name="customers",
        database_name="appdb",
    )
    assert {c["name"] for c in cached} == {"id", "email"}


def test_columns_endpoint_serves_writethrough_on_second_call(
    client, auth_headers, monkeypatch, history
) -> None:
    """Second visit reuses catalog_entities (source=catalog) even
    though the live mock would still return columns. This proves the
    write-through path actually feeds the cache-first read."""
    cp1 = MagicMock()
    cp1.name = "id"
    cp1.dtype = "BIGINT"
    cp1.nullable = False
    live_mock = MagicMock(list_column_profiles=MagicMock(return_value=[cp1]))
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: live_mock)

    client.get(
        f"/api/live/schemas/sales/tables/orders/columns{_q('database=appdb')}",
        headers=auth_headers,
    )
    second = client.get(
        f"/api/live/schemas/sales/tables/orders/columns{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert second.status_code == 200
    assert second.json()["source"] == "catalog"


def test_snapshot_endpoint_falls_back_when_live_returns_zero_columns(
    client, auth_headers, monkeypatch, history
) -> None:
    """When ``get_table_metadata_snapshot`` returns successfully but
    with an empty columns list, the endpoint salvages from
    column_comments_cache so the Studio page is not blank."""
    save_column_comments_cache(
        history,
        db_profile=PROFILE,
        database="appdb",
        schema="sales",
        entries={
            "orders": {
                "table_comment": "Customer purchase orders",
                "columns": {"order_id": "Primary key"},
                "kind": "TABLE",
            }
        },
    )
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            get_table_metadata_snapshot=MagicMock(
                return_value={
                    "schema": "sales",
                    "table": "orders",
                    "table_comment": "Customer purchase orders",
                    "columns": [],
                }
            )
        ),
    )
    response = client.get(
        f"/api/live/schemas/sales/tables/orders/snapshot{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cache-fallback"
    assert {c["name"] for c in payload["columns"]} == {"order_id"}


def test_snapshot_endpoint_salvages_on_connector_exception(
    client, auth_headers, monkeypatch, history
) -> None:
    """If the connector raises (e.g. NoSuchTableError bubbling from
    get_column_comments), the endpoint salvages from the cache
    instead of returning 500."""
    save_column_comments_cache(
        history,
        db_profile=PROFILE,
        database="appdb",
        schema="sales",
        entries={
            "orders": {
                "table_comment": "ok",
                "columns": {"order_id": "Primary key"},
                "kind": "TABLE",
            }
        },
    )
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            get_table_metadata_snapshot=MagicMock(
                side_effect=RuntimeError("NoSuchTableError: sales.orders")
            )
        ),
    )
    response = client.get(
        f"/api/live/schemas/sales/tables/orders/snapshot{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "cache-fallback"
    assert {c["name"] for c in payload["columns"]} == {"order_id"}


def test_snapshot_endpoint_500s_when_no_cache_and_live_raises(
    client, auth_headers, monkeypatch, history
) -> None:
    """No cached row and live raises — surface the 500 verbatim
    instead of pretending the table exists with no columns."""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            get_table_metadata_snapshot=MagicMock(
                side_effect=RuntimeError("connect timed out")
            )
        ),
    )
    response = client.get(
        f"/api/live/schemas/sales/tables/orders/snapshot{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 500
    assert "connect timed out" in response.json()["detail"]
