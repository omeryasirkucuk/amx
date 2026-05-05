"""Comment write-back tests — patches DatabaseConnector at the
router boundary so the suite never touches a real DB.

Every endpoint requires ``?profile=NAME``; tests register a profile
on ``cfg.db_profiles`` so the scope helper resolves it.
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
    # Comments router builds connectors through _connector_for_scope,
    # which reaches DatabaseConnector inside live_db.
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda _db: instance)
    return instance


@pytest.fixture(autouse=True)
def _wipe_connector_cache() -> None:
    live_db._CONNECTOR_CACHE.clear()
    yield
    live_db._CONNECTOR_CACHE.clear()


def test_set_database_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/database?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Production warehouse"},
    )
    assert response.status_code == 200
    stub_db.set_database_comment.assert_called_once_with("Production warehouse")


def test_set_schema_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/schemas/sales?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Sales facts"},
    )
    assert response.status_code == 200
    stub_db.set_schema_comment.assert_called_once_with("sales", "Sales facts")


def test_set_table_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/schemas/sales/tables/orders?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Customer orders fact table"},
    )
    assert response.status_code == 200
    args, kwargs = stub_db.set_table_comment.call_args
    assert args[:3] == ("sales", "orders", "Customer orders fact table")
    # asset_kind passed via kwarg; default → TABLE.
    from amx.db.connector import AssetKind

    assert kwargs["asset_kind"] == AssetKind.TABLE


def test_set_column_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        f"/api/comments/schemas/sales/tables/orders/columns/id?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "Order primary key"},
    )
    assert response.status_code == 200
    stub_db.set_column_comment.assert_called_once_with("sales", "orders", "id", "Order primary key")


def test_connector_failure_returns_400(client, auth_headers, stub_db) -> None:
    """Backends that don't support COMMENT ON DATABASE raise — we
    surface that as a clean 400 with the connector's actionable
    message instead of a 500 with a stack trace."""
    stub_db.set_database_comment.side_effect = RuntimeError("not supported")
    response = client.put(
        f"/api/comments/database?profile={PROFILE}",
        headers=auth_headers,
        json={"comment": "x"},
    )
    assert response.status_code == 400
    assert "not supported" in response.json()["detail"]


def test_table_comment_writes_to_named_profile_not_active(
    client, auth_headers, cfg, monkeypatch
) -> None:
    """Active profile is X; request specifies ?profile=Y → write must
    target Y, not X.

    This is the silent-corruption regression: before the scope refactor
    the comment write would land on whatever ``cfg.active_db_profile``
    happened to be, even though the SPA was rendering profile Y's tree.
    A user editing a table description on Y could quietly mutate X.
    """
    cfg.db_profiles = {
        "active-X": DBConfig(backend="postgresql", host="x.local", database="appx", user="amx"),
        "target-Y": DBConfig(backend="postgresql", host="y.local", database="appy", user="amx"),
    }
    cfg.active_db_profile = "active-X"
    cfg.db = cfg.db_profiles["active-X"]

    seen_dbconfigs: list[DBConfig] = []
    instance = MagicMock()

    def _factory(db: DBConfig) -> MagicMock:
        seen_dbconfigs.append(db)
        return instance

    # The comments router builds connectors via _connector_for_scope,
    # which reaches DatabaseConnector inside the live_db module.
    monkeypatch.setattr(live_db, "DatabaseConnector", _factory)

    response = client.put(
        "/api/comments/schemas/sales/tables/orders?profile=target-Y",
        headers=auth_headers,
        json={"comment": "Y's order facts"},
    )
    assert response.status_code == 200, response.json()

    # The connector that was built must reflect target-Y's host/database
    # — never active-X's.
    assert len(seen_dbconfigs) == 1
    built = seen_dbconfigs[0]
    assert built.host == "y.local"
    assert built.database == "appy"
    instance.set_table_comment.assert_called_once()


def test_database_overlay_does_not_mutate_target_profile(
    client, auth_headers, cfg, monkeypatch
) -> None:
    """Passing ?database= overrides the connection scope per-request
    without writing back to ``cfg.db_profiles[name]``."""
    cfg.db_profiles = {
        "warehouse": DBConfig(
            backend="postgresql", host="wh.local", database="default_db", user="amx"
        ),
    }
    cfg.active_db_profile = "warehouse"
    cfg.db = cfg.db_profiles["warehouse"]

    seen: list[DBConfig] = []
    monkeypatch.setattr(
        live_db,
        "DatabaseConnector",
        lambda db: seen.append(db) or MagicMock(),
    )

    response = client.put(
        "/api/comments/schemas/sales?profile=warehouse&database=other_db",
        headers=auth_headers,
        json={"comment": "scoped"},
    )
    assert response.status_code == 200
    assert seen[0].database == "other_db"
    # Original profile record is preserved.
    assert cfg.db_profiles["warehouse"].database == "default_db"
