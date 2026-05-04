"""Search-catalog router tests — stub the SearchCatalog factory so the
tests don't depend on the on-disk SQLite store."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.web.routers import catalog as catalog_router


@pytest.fixture()
def stub_catalog(monkeypatch):
    instance = MagicMock()
    monkeypatch.setattr(catalog_router, "_catalog", lambda cfg: instance)
    return instance


def test_no_active_db_profile_returns_400(client, auth_headers, cfg) -> None:
    cfg.active_db_profile = ""
    response = client.get("/api/catalog/databases", headers=auth_headers)
    assert response.status_code == 400
    assert "active DB profile" in response.json()["detail"]


def test_known_databases(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.known_databases.return_value = [
        {"database_name": "app", "entity_count": 12},
        {"database_name": "analytics", "entity_count": 4},
    ]
    response = client.get("/api/catalog/databases", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    stub_catalog.known_databases.assert_called_once_with("prod")


def test_known_schemas_with_database_filter(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.known_schemas.return_value = [{"schema_name": "sales", "table_count": 3}]
    response = client.get("/api/catalog/schemas?db=app", headers=auth_headers)
    assert response.status_code == 200
    stub_catalog.known_schemas.assert_called_once_with("prod", database_name="app")


def test_inventory(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.schema_inventory.return_value = [
        {"table_name": "customers", "column_count": 12, "row_count": 1000}
    ]
    response = client.get(
        "/api/catalog/inventory?schema=sales&db=app&limit=50", headers=auth_headers
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["limit"] == 50
    stub_catalog.schema_inventory.assert_called_once_with(
        "prod", schema_name="sales", database_name="app", limit=50
    )


def test_explain_table_404_when_missing(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.explain_table.return_value = None
    response = client.get("/api/catalog/explain?path=sales.orders", headers=auth_headers)
    assert response.status_code == 404
    assert "sales.orders" in response.json()["detail"]


def test_explain_table_passes_through(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.explain_table.return_value = {"name": "orders", "columns": []}
    response = client.get("/api/catalog/explain?path=sales.orders", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["name"] == "orders"


def test_search_columns_pass_through(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.search_columns.return_value = [{"column_name": "email"}]
    response = client.get("/api/catalog/search/columns?q=email&limit=3", headers=auth_headers)
    assert response.status_code == 200
    stub_catalog.search_columns.assert_called_once_with("prod", "email", limit=3)


def test_search_tables_pass_through(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.search_tables.return_value = [{"table_name": "customers"}]
    response = client.get("/api/catalog/search/tables?q=customer", headers=auth_headers)
    assert response.status_code == 200
    stub_catalog.search_tables.assert_called_once_with("prod", "customer", limit=8)


def test_settings_includes_profile_marker(client, auth_headers, cfg, stub_catalog) -> None:
    cfg.active_db_profile = "prod"
    stub_catalog.get_settings.return_value = {"vector_search_enabled": "true"}
    response = client.get("/api/catalog/settings", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["profile"] == "prod"
    assert payload["settings"]["vector_search_enabled"] == "true"
