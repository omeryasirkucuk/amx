"""Comment write-back tests — patches DatabaseConnector at the
router boundary so the suite never touches a real DB."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.web.routers import comments as comments_router


@pytest.fixture()
def stub_db(monkeypatch):
    instance = MagicMock()
    monkeypatch.setattr(comments_router, "DatabaseConnector", lambda cfg: instance)
    return instance


def test_set_database_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        "/api/comments/database",
        headers=auth_headers,
        json={"comment": "Production warehouse"},
    )
    assert response.status_code == 200
    stub_db.set_database_comment.assert_called_once_with("Production warehouse")


def test_set_schema_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        "/api/comments/schemas/sales",
        headers=auth_headers,
        json={"comment": "Sales facts"},
    )
    assert response.status_code == 200
    stub_db.set_schema_comment.assert_called_once_with("sales", "Sales facts")


def test_set_table_comment_passthrough(client, auth_headers, stub_db) -> None:
    response = client.put(
        "/api/comments/schemas/sales/tables/orders",
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
        "/api/comments/schemas/sales/tables/orders/columns/id",
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
        "/api/comments/database",
        headers=auth_headers,
        json={"comment": "x"},
    )
    assert response.status_code == 400
    assert "not supported" in response.json()["detail"]
