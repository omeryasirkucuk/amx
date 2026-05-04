"""Live-DB router tests.

We mock :class:`DatabaseConnector` at the cache layer so the test
suite never tries to open a real SQLAlchemy engine — the goal is to
verify the HTTP shape, not the connector's per-backend SQL.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.web.routers import live_db


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    """The router caches connectors per profile-key. Drop the cache
    between tests so a fixture-built mock never bleeds into the next
    case."""
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture(autouse=True)
def _pin_default_database(cfg) -> None:
    """Pin a database on the default postgres profile so tests opt
    INTO the under-scoped path explicitly. The 2-level browse gate
    (added 2026-05-04) blocks browse endpoints when the active
    profile leaves ``database`` blank — without this fixture every
    pre-existing test would hit a 412."""
    cfg.db.database = cfg.db.database or "appdb"


def _patch_connector(monkeypatch, builder) -> MagicMock:
    """Install ``builder()`` as the cached connector for any profile
    key. Returns the mock so the test can assert on call arguments.
    """
    instance = builder()
    monkeypatch.setattr(live_db, "_connector", lambda cfg: instance)
    return instance


def test_list_catalogs_three_level_backend(client, auth_headers, monkeypatch) -> None:
    mock = _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=True),
            list_catalogs=MagicMock(return_value=["main", "sap", "samples"]),
        ),
    )
    response = client.get("/api/live/catalogs", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_catalogs"] is True
    assert payload["catalogs"] == ["main", "sap", "samples"]
    mock.list_catalogs.assert_called_once()


def test_list_catalogs_two_level_backend_returns_empty_with_flag(
    client, auth_headers, monkeypatch
) -> None:
    """Postgres-style backends report supports_catalogs=False so the
    SPA hides the catalog rail."""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_catalogs=MagicMock(return_value=[]),
        ),
    )
    response = client.get("/api/live/catalogs", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_catalogs"] is False
    assert payload["catalogs"] == []


def test_list_databases(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            list_databases=MagicMock(return_value=["app", "analytics"]),
        ),
    )
    response = client.get("/api/live/databases", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["databases"] == ["app", "analytics"]


def test_list_schemas_uses_active_catalog_default(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_schemas=MagicMock(return_value=["public", "sales"]),
        ),
    )
    response = client.get("/api/live/schemas", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["schemas"] == ["public", "sales"]


def test_list_schemas_with_explicit_catalog_query_arg(
    client, auth_headers, monkeypatch, cfg
) -> None:
    """When the caller passes ?catalog=…, the router temporarily swaps
    cfg.db.catalog so the connector's listing is scoped to the
    requested catalog. The pin must be restored after the call."""
    cfg.db.catalog = "old"
    observed: dict[str, str] = {}

    def list_schemas() -> list[str]:
        observed["catalog_during_call"] = cfg.db.catalog
        return ["a", "b"]

    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_schemas=MagicMock(side_effect=list_schemas)),
    )
    response = client.get("/api/live/schemas?catalog=new", headers=auth_headers)
    assert response.status_code == 200
    assert observed["catalog_during_call"] == "new"
    assert cfg.db.catalog == "old", "router must restore the pinned catalog"


def test_list_assets_serializes_kind_enum(client, auth_headers, monkeypatch) -> None:
    """``list_assets`` returns ``(name, AssetKind)`` tuples; the router
    must surface the enum as its string value so the SPA gets plain
    JSON."""
    from amx.db.connector import AssetKind

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
        ),
    )
    response = client.get("/api/live/schemas/sales/assets", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"] == [
        {"name": "orders", "kind": "table"},
        {"name": "orders_view", "kind": "view"},
    ]


def test_list_volumes_returns_empty_for_unsupported_backends(
    client, auth_headers, monkeypatch
) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            capabilities=MagicMock(volumes=False),
        ),
    )
    response = client.get("/api/live/schemas/sales/volumes", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_volumes"] is False
    assert payload["volumes"] == []


def test_list_volumes_returns_rows_for_databricks(client, auth_headers, monkeypatch, cfg) -> None:
    cfg.db.catalog = "main"
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=True),
            capabilities=MagicMock(volumes=True),
            list_volumes=MagicMock(
                return_value=[
                    {"name": "raw_files", "type": "managed", "comment": "ETL inbox"},
                ]
            ),
        ),
    )
    response = client.get("/api/live/schemas/sales/volumes", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_volumes"] is True
    assert payload["volumes"] == [{"name": "raw_files", "kind": "managed", "comment": "ETL inbox"}]


def test_list_columns_returns_lightweight_metadata(client, auth_headers, monkeypatch) -> None:
    """``list_column_profiles`` returns ColumnProfile dataclasses; the
    router pulls only name/dtype/nullable so the SPA renders the
    Columns table fast (no row scan)."""
    cp1 = MagicMock(name="cp1")
    cp1.name = "id"
    cp1.dtype = "BIGINT"
    cp1.nullable = False
    cp2 = MagicMock(name="cp2")
    cp2.name = "email"
    cp2.dtype = "VARCHAR"
    cp2.nullable = True

    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_column_profiles=MagicMock(return_value=[cp1, cp2]),
        ),
    )
    response = client.get("/api/live/schemas/sales/tables/customers/columns", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["columns"] == [
        {"name": "id", "dtype": "BIGINT", "nullable": False},
        {"name": "email", "dtype": "VARCHAR", "nullable": True},
    ]


def test_table_snapshot_passes_through_connector_payload(client, auth_headers, monkeypatch) -> None:
    snapshot = {
        "schema": "sales",
        "table": "customers",
        "table_comment": "Customer master",
        "columns": [
            {"name": "id", "dtype": "BIGINT", "nullable": False, "comment": ""},
        ],
    }
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            get_table_metadata_snapshot=MagicMock(return_value=snapshot),
        ),
    )
    response = client.get("/api/live/schemas/sales/tables/customers/snapshot", headers=auth_headers)
    assert response.status_code == 200
    assert response.json() == snapshot


def test_connector_errors_surface_as_500_with_actionable_detail(
    client, auth_headers, monkeypatch
) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_databases=MagicMock(side_effect=RuntimeError("connect timed out"))),
    )
    response = client.get("/api/live/databases", headers=auth_headers)
    assert response.status_code == 500
    assert "connect timed out" in response.json()["detail"]


def test_list_assets_412_when_3level_backend_has_no_catalog(
    client, auth_headers, monkeypatch
) -> None:
    """Databricks (3-level) without a catalog must return 412 with the
    select-catalog hint, not crash with ``Catalog 'None' was not found``."""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=True)),
    )
    response = client.get("/api/live/schemas/sales/assets", headers=auth_headers)
    assert response.status_code == 412
    detail = response.json()["detail"]
    assert detail["hint"] == "select-catalog"


def test_activate_catalog_writes_to_active_profile(client, auth_headers, monkeypatch, cfg) -> None:
    cfg.db.catalog = ""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=True)),
    )
    response = client.post(
        "/api/live/catalogs/main/activate",
        headers=auth_headers,
        json={"persist": False},
    )
    assert response.status_code == 200
    assert response.json()["catalog"] == "main"
    assert cfg.db.catalog == "main"


def test_activate_catalog_rejects_2level_backend(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=False)),
    )
    response = client.post(
        "/api/live/catalogs/main/activate",
        headers=auth_headers,
        json={"persist": False},
    )
    assert response.status_code == 400


def test_list_assets_412_when_2level_backend_has_no_database(
    client, auth_headers, monkeypatch, cfg
) -> None:
    """The Postgres / MySQL counterpart of the 3-level catalog gate.
    When the active profile leaves ``database`` blank the connector
    silently lands on the server's default DB and only schemas of
    that DB are visible — exactly what the user reported. Surface
    412 with hint=select-database so the SPA can show a picker."""
    cfg.db.database = ""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=False)),
    )
    response = client.get("/api/live/schemas/public/assets", headers=auth_headers)
    assert response.status_code == 412
    detail = response.json()["detail"]
    assert detail["hint"] == "select-database"


def test_activate_database_writes_to_active_profile(client, auth_headers, monkeypatch, cfg) -> None:
    cfg.db.database = ""
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=False)),
    )
    response = client.post(
        "/api/live/databases/appdb/activate",
        headers=auth_headers,
        json={"persist": False},
    )
    assert response.status_code == 200
    assert response.json()["database"] == "appdb"
    assert cfg.db.database == "appdb"


def test_activate_database_rejects_3level_backend(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(supports_catalogs=MagicMock(return_value=True)),
    )
    response = client.post(
        "/api/live/databases/appdb/activate",
        headers=auth_headers,
        json={"persist": False},
    )
    assert response.status_code == 400
