"""Tests for the cleanup-placeholders core helper + the
``/api/comments/cleanup-placeholders`` endpoint.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.cli_support.commands.db import cleanup_placeholders_core


def _stub_db_with_placeholders():
    """Build a connector stub that exposes one schema with one
    table-level + two column-level placeholder comments. The helper
    should clear all three."""
    from amx.db.connector import AssetKind

    placeholder = "Auto-inference missed a reliable description; review."
    db = MagicMock()
    db.list_schemas.return_value = ["sales"]
    db.list_assets.return_value = [("orders", AssetKind.TABLE)]
    db.get_table_comment.return_value = placeholder
    db.get_column_comments.return_value = {
        "id": placeholder,
        "email": placeholder,
        "name": "real description",
    }
    return db


def test_helper_clears_all_placeholders() -> None:
    db = _stub_db_with_placeholders()
    result = cleanup_placeholders_core(db)
    assert result["tables_cleared"] == 1
    assert result["columns_cleared"] == 2
    # apply_comment called for one table + two columns.
    assert db.apply_comment.call_count == 3


def test_helper_skips_real_descriptions() -> None:
    """The helper only clears comments matching the placeholder
    marker. Real descriptions stay untouched."""
    db = _stub_db_with_placeholders()
    cleanup_placeholders_core(db)
    cleared_columns = {
        kwargs.get("column")
        for _, kwargs in db.apply_comment.call_args_list
        if kwargs.get("column") is not None
    }
    assert cleared_columns == {"id", "email"}
    assert "name" not in cleared_columns


def test_helper_emits_events_for_each_cleared_row() -> None:
    db = _stub_db_with_placeholders()
    events: list[tuple[str, dict]] = []
    cleanup_placeholders_core(db, on_event=lambda kind, payload: events.append((kind, payload)))
    kinds = [k for k, _ in events]
    assert kinds.count("table") == 1
    assert kinds.count("column") == 2


def test_helper_raises_for_unknown_schema() -> None:
    db = _stub_db_with_placeholders()
    with pytest.raises(RuntimeError) as exc_info:
        cleanup_placeholders_core(db, schema="missing")
    assert "missing" in str(exc_info.value).lower()


def test_endpoint_returns_helper_payload(client, auth_headers, monkeypatch, cfg) -> None:
    from amx.config import DBConfig
    from amx.web.routers import live_db

    cfg.db_profiles["test-profile"] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    live_db._CONNECTOR_CACHE.clear()

    db = _stub_db_with_placeholders()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: db)

    response = client.post(
        "/api/comments/cleanup-placeholders?profile=test-profile",
        headers=auth_headers,
        json={},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["tables_cleared"] == 1
    assert payload["columns_cleared"] == 2


def test_endpoint_400_for_unknown_schema(client, auth_headers, monkeypatch, cfg) -> None:
    from amx.config import DBConfig
    from amx.web.routers import live_db

    cfg.db_profiles["test-profile"] = DBConfig(
        backend="postgresql", host="pg.test", user="amx", database="appdb"
    )
    live_db._CONNECTOR_CACHE.clear()

    db = _stub_db_with_placeholders()
    monkeypatch.setattr(live_db, "DatabaseConnector", lambda *_a, **_kw: db)

    response = client.post(
        "/api/comments/cleanup-placeholders?profile=test-profile",
        headers=auth_headers,
        json={"schema": "missing"},
    )
    assert response.status_code == 400
