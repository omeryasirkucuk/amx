"""POST /api/comments/local — local-only override endpoint tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def history_db(tmp_path, monkeypatch):
    """Seed a real on-disk history store and bind it as the module
    singleton so ``SearchCatalog.from_history_store()`` returns
    something usable."""
    from amx.storage import sqlite_store as ss

    db_path = tmp_path / "history.db"
    SQLiteHistoryStore(db_path).init()
    store = SQLiteHistoryStore(db_path)
    monkeypatch.setattr(ss, "_store", store, raising=False)
    yield store
    monkeypatch.setattr(ss, "_store", None, raising=False)


@pytest.fixture()
def cfg_with_profile(cfg):
    """Attach one DBConfig-shaped profile so the endpoint can resolve
    the backend + default container without opening a real engine."""
    cfg.db_profiles = {
        "prod-pg": SimpleNamespace(
            backend="postgresql",
            catalog="",
            dataset="",
            database="analytics",
            project="",
        )
    }
    return cfg


def test_save_local_comment_happy_path(
    client, auth_headers, history_db, cfg_with_profile
) -> None:
    response = client.post(
        "/api/comments/local",
        json={
            "profile": "prod-pg",
            "schema": "sales",
            "table": "orders",
            "column": "amount",
            "description": "Net invoice amount in account currency.",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ok"] is True
    assert payload["profile"] == "prod-pg"
    assert payload["schema"] == "sales"
    assert payload["table"] == "orders"
    assert payload["column"] == "amount"
    assert isinstance(payload["entity_id"], int) and payload["entity_id"] > 0
    assert isinstance(payload["description_id"], int) and payload["description_id"] > 0


def test_save_local_comment_table_level_when_column_omitted(
    client, auth_headers, history_db, cfg_with_profile
) -> None:
    response = client.post(
        "/api/comments/local",
        json={
            "profile": "prod-pg",
            "schema": "sales",
            "table": "orders",
            "description": "Customer purchase orders, one row per order.",
        },
        headers=auth_headers,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["column"] is None

    with history_db._connect() as conn:
        row = conn.execute(
            "SELECT entity_kind, column_name FROM catalog_entities WHERE id = ?",
            (payload["entity_id"],),
        ).fetchone()
    assert row["entity_kind"] == "table"
    assert row["column_name"] is None


def test_save_local_comment_unknown_profile_returns_404(
    client, auth_headers, history_db, cfg
) -> None:
    cfg.db_profiles = {}
    response = client.post(
        "/api/comments/local",
        json={
            "profile": "ghost",
            "schema": "s",
            "table": "t",
            "description": "x",
        },
        headers=auth_headers,
    )
    assert response.status_code == 404
    assert "ghost" in response.json()["detail"]


def test_save_local_comment_missing_required_field_returns_422(
    client, auth_headers, history_db, cfg_with_profile
) -> None:
    response = client.post(
        "/api/comments/local",
        json={"schema": "s", "table": "t", "description": "x"},
        headers=auth_headers,
    )
    assert response.status_code == 422


def test_save_local_comment_blank_description_returns_422(
    client, auth_headers, history_db, cfg_with_profile
) -> None:
    response = client.post(
        "/api/comments/local",
        json={
            "profile": "prod-pg",
            "schema": "s",
            "table": "t",
            "description": "",
        },
        headers=auth_headers,
    )
    assert response.status_code == 422
