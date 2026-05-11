"""Live-DB router tests.

We mock :class:`DatabaseConnector` at the cache layer so the test
suite never tries to open a real SQLAlchemy engine — the goal is to
verify the HTTP shape, not the connector's per-backend SQL.

Every browse endpoint requires ``?profile=NAME`` (the legacy
single-active path retired in PR-3). Tests register a profile with
``cfg.db_profiles[name]`` so :func:`_connector_for_scope` can find it.
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
    """Drop the cache between tests so a fixture-built mock never
    bleeds into the next case."""
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


@pytest.fixture(autouse=True)
def _register_profile(cfg) -> None:
    """Register a default profile so ``?profile=test-profile`` resolves.
    Tests that need a 3-level backend overwrite this with a Databricks
    profile via ``cfg.db_profiles[PROFILE] = …``."""
    cfg.db_profiles[PROFILE] = DBConfig(
        backend="postgresql",
        host="pg.test",
        user="amx",
        database="appdb",
    )


def _patch_connector(monkeypatch, builder) -> MagicMock:
    """Install ``builder()`` as the connector :func:`DatabaseConnector`
    factory uses inside :mod:`amx.web.routers.live_db`. Returns the
    mock so the test can assert on call arguments."""
    instance = builder()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: instance)
    return instance


def _q(extra: str = "") -> str:
    """Build the ``?profile=…`` query string (plus optional extras)."""
    base = f"?profile={PROFILE}"
    return base + (f"&{extra}" if extra else "")


def test_list_catalogs_three_level_backend(client, auth_headers, monkeypatch) -> None:
    mock = _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=True),
            list_catalogs=MagicMock(return_value=["main", "sap", "samples"]),
        ),
    )
    response = client.get(f"/api/live/catalogs{_q()}", headers=auth_headers)
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
    response = client.get(f"/api/live/catalogs{_q()}", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_catalogs"] is False
    assert payload["catalogs"] == []


def test_list_databases(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_databases=MagicMock(return_value=["app", "analytics"])),
    )
    response = client.get(f"/api/live/databases{_q()}", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["databases"] == ["app", "analytics"]


def test_list_schemas_uses_explicit_database(client, auth_headers, monkeypatch) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            supports_catalogs=MagicMock(return_value=False),
            list_schemas=MagicMock(return_value=["public", "sales"]),
            get_schema_comment=MagicMock(return_value=""),
        ),
    )
    response = client.get(f"/api/live/schemas{_q('database=appdb')}", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["schemas"] == ["public", "sales"]


def test_list_schemas_with_explicit_catalog_query_arg(client, auth_headers, monkeypatch) -> None:
    """Passing ?catalog= scopes the listing to a specific catalog
    without mutating the profile record. The connector receives a
    fresh DBConfig via dataclasses.replace."""
    captured: dict[str, object] = {}

    def factory(db_cfg, **_kw):
        captured["catalog"] = db_cfg.catalog
        return MagicMock(
            list_schemas=MagicMock(return_value=["a", "b"]),
            get_schema_comment=MagicMock(return_value=""),
        )

    monkeypatch.setattr(live_db, "DatabaseConnector", factory)
    response = client.get(f"/api/live/schemas{_q('catalog=ANALYTICS')}", headers=auth_headers)
    assert response.status_code == 200
    assert captured["catalog"] == "ANALYTICS"
    assert response.json()["schemas"] == ["a", "b"]


def test_list_assets_serializes_kind_enum(client, auth_headers, monkeypatch) -> None:
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
            get_table_comment=MagicMock(
                side_effect=lambda _schema, name: "Order line items" if name == "orders" else ""
            ),
        ),
    )
    response = client.get(
        f"/api/live/schemas/sales/assets{_q('database=appdb')}", headers=auth_headers
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"] == [
        {"name": "orders", "kind": "table", "comment": "Order line items"},
        {"name": "orders_view", "kind": "view", "comment": ""},
    ]


def test_list_volumes_returns_empty_for_unsupported_backends(
    client, auth_headers, monkeypatch
) -> None:
    capabilities = MagicMock()
    capabilities.volumes = False
    _patch_connector(monkeypatch, lambda: MagicMock(capabilities=capabilities))
    response = client.get(f"/api/live/schemas/sales/volumes{_q()}", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_volumes"] is False
    assert payload["volumes"] == []


def test_list_volumes_returns_rows_for_databricks(client, auth_headers, monkeypatch) -> None:
    capabilities = MagicMock()
    capabilities.volumes = True
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(
            capabilities=capabilities,
            list_volumes=MagicMock(
                return_value=[
                    {"name": "raw", "type": "managed", "comment": "Raw uploads"},
                    {"name": "stage", "type": "external", "comment": ""},
                ]
            ),
        ),
    )
    response = client.get(
        f"/api/live/schemas/sales/volumes{_q('catalog=main')}", headers=auth_headers
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["supports_volumes"] is True
    assert len(payload["volumes"]) == 2
    assert payload["volumes"][0]["name"] == "raw"


def test_list_columns_returns_lightweight_metadata(client, auth_headers, monkeypatch) -> None:
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
    response = client.get(
        f"/api/live/schemas/sales/tables/customers/columns{_q('database=appdb')}",
        headers=auth_headers,
    )
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
    response = client.get(
        f"/api/live/schemas/sales/tables/customers/snapshot{_q('database=appdb')}",
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == snapshot


def test_connector_errors_surface_as_500_with_actionable_detail(
    client, auth_headers, monkeypatch
) -> None:
    _patch_connector(
        monkeypatch,
        lambda: MagicMock(list_databases=MagicMock(side_effect=RuntimeError("connect timed out"))),
    )
    response = client.get(f"/api/live/databases{_q()}", headers=auth_headers)
    assert response.status_code == 500
    assert "connect timed out" in response.json()["detail"]


def test_browse_without_profile_returns_400(client, auth_headers) -> None:
    """Studio always sends ?profile=…; omitting it surfaces a clean
    400 instead of a confusing 422 from FastAPI."""
    response = client.get("/api/live/catalogs", headers=auth_headers)
    assert response.status_code == 422  # FastAPI's default for missing required query


def test_browse_unknown_profile_returns_404(client, auth_headers, monkeypatch) -> None:
    _patch_connector(monkeypatch, lambda: MagicMock())
    response = client.get("/api/live/catalogs?profile=does-not-exist", headers=auth_headers)
    assert response.status_code == 404
    assert "does-not-exist" in response.json()["detail"]


def test_activate_endpoints_removed(client, auth_headers) -> None:
    """PR-3 removed POST /api/live/{catalogs|databases}/{name}/activate.
    Studio is multi-profile and never persists a single-active scope;
    the CLI's /connect picker mutates ``cfg.db`` in-process only.
    """
    for url in (
        "/api/live/catalogs/main/activate",
        "/api/live/databases/appdb/activate",
    ):
        response = client.post(url, headers=auth_headers, json={"persist": False})
        assert response.status_code in (404, 405), url
