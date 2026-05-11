"""``POST /api/live/schemas/{schema}/refresh`` — manual cache-bust endpoint.

The endpoint backs the sidebar's hover refresh button. It must:

1. Call ``invalidate_column_comments_cache(schema=...)`` on the
   connector before re-reading, so the response carries fresh data
   even if the cache was warm a moment ago.
2. Return the same payload shape as ``list_assets`` (plus
   ``refreshed: true``) so the SPA can swap the result in-place
   without coding a different render path.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.db.connector import AssetKind
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture()
def stub_db(monkeypatch, cfg):
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    instance = MagicMock()
    instance.list_assets.return_value = [
        ("orders", AssetKind.TABLE),
        ("orders_view", AssetKind.VIEW),
    ]
    instance.get_table_comment.side_effect = lambda _schema, name: (
        "Refreshed orders" if name == "orders" else ""
    )
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)
    return instance


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def test_refresh_invalidates_then_relists(client, auth_headers, stub_db) -> None:
    response = client.post(
        f"/api/live/schemas/sales/refresh?profile={PROFILE}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    # Order of calls matters: invalidate must run BEFORE list_assets +
    # get_table_comment, otherwise the response could carry stale data.
    calls = [c[0] for c in stub_db.mock_calls if c[0]]
    invalidate_idx = next(i for i, c in enumerate(calls) if "invalidate" in c)
    list_idx = next(i for i, c in enumerate(calls) if c == "list_assets")
    assert invalidate_idx < list_idx
    stub_db.invalidate_column_comments_cache.assert_called_once_with(schema="sales")


def test_refresh_payload_shape_matches_list_assets(client, auth_headers, stub_db) -> None:
    response = client.post(
        f"/api/live/schemas/sales/refresh?profile={PROFILE}",
        headers=auth_headers,
    )
    body = response.json()
    assert body["schema"] == "sales"
    assert body["count"] == 2
    assert body["refreshed"] is True
    names = [a["name"] for a in body["assets"]]
    assert names == ["orders", "orders_view"]
    # Comments came from the connector — the freshly-fetched values.
    orders = next(a for a in body["assets"] if a["name"] == "orders")
    assert orders["comment"] == "Refreshed orders"
    assert orders["kind"] == "table"
