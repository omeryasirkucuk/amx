"""Cache invalidation tests — every DB-write endpoint must wipe the
column-comments cache for the affected scope before the HTTP response
returns. Otherwise the next sidebar read serves the pre-write data
and the user thinks their save was lost.

The four ``set_*_comment`` endpoints each have a different scope:

* database → whole profile.
* schema   → that schema's tables only.
* table    → single row.
* column   → same single row (column-level granularity not worth the
             bookkeeping).

The apply path (``apply_review_results_to_db``) invalidates per row as
it walks the result list; covered in ``test_runs_apply.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import DBConfig
from amx.web.routers import live_db

PROFILE = "test-profile"


@pytest.fixture()
def stub_db(monkeypatch, cfg):
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    instance = MagicMock()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)
    return instance


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def test_set_database_comment_wipes_whole_profile(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/database?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Prod DWH"},
    )
    assert response.status_code == 200
    # Database-level write nukes everything — no schema/table kwargs.
    stub_db.invalidate_column_comments_cache.assert_called_once_with()


def test_set_schema_comment_wipes_only_that_schema(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/schemas/sales?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Sales facts"},
    )
    assert response.status_code == 200
    stub_db.invalidate_column_comments_cache.assert_called_once_with(schema="sales")


def test_set_table_comment_wipes_only_that_row(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/schemas/sales/tables/orders?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Orders fact"},
    )
    assert response.status_code == 200
    stub_db.invalidate_column_comments_cache.assert_called_once_with(
        schema="sales", table="orders"
    )


def test_set_column_comment_wipes_parent_table_row(client, auth_headers, stub_db) -> None:
    """Column writes invalidate at *table* granularity. Cheaper than
    rewriting per-column tracking — the next bulk fetch refreshes the
    whole columns dict in one round-trip."""
    response = client.put(
        f"/api/comments/schemas/sales/tables/orders/columns/id?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Order primary key"},
    )
    assert response.status_code == 200
    stub_db.invalidate_column_comments_cache.assert_called_once_with(
        schema="sales", table="orders"
    )
