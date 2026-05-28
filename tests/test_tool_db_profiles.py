"""Tests for the ``list_db_profiles`` ToolBox tool.

Two guarantees matter here:

* The engine ``backend`` is surfaced for every configured profile — this
  is the whole point (an MCP/IDE agent could not tell Databricks from
  Postgres before).
* **No secret ever appears in the output.** The connection block is an
  allowlist of non-secret coordinates; passwords / tokens must be
  structurally impossible to leak.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox

# Distinctive sentinels so a leak is unambiguous in the serialized output.
_SECRET_TOKEN = "SUPERSECRET_DBR_TOKEN_abc123"
_SECRET_PW = "DO_NOT_LEAK_PG_PASSWORD_xyz789"
_SECRET_MD = "MOTHERDUCK_TOKEN_qqq000"


@pytest.fixture()
def cfg_with_secrets() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "pg": DBConfig(
            backend="postgresql",
            host="pg.internal",
            database="sales",
            user="reporting",
            password=_SECRET_PW,
        ),
        "dbr": DBConfig(
            backend="databricks",
            host="dbc-abc.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/w1",
            catalog="main",
            access_token=_SECRET_TOKEN,
            workspace_token=_SECRET_TOKEN,
        ),
        "duck": DBConfig(
            backend="duckdb",
            database="md:analytics",
            motherduck_token=_SECRET_MD,
        ),
    }
    cfg.active_db_profile = "pg"
    cfg.active_db_profiles = []
    cfg.db = cfg.db_profiles["pg"]
    return cfg


def _invoke(cfg: AMXConfig, scope: list[str]) -> dict:
    catalog = MagicMock()
    catalog.get_profile_state.return_value = {"state": "complete", "processed_tables": 5}
    box = ToolBox(cfg, catalog, db_profiles=scope)
    try:
        return json.loads(box.invoke("list_db_profiles", "{}"))
    finally:
        box.close()


def test_lists_every_profile_with_backend(cfg_with_secrets) -> None:
    result = _invoke(cfg_with_secrets, ["pg"])
    assert result["count"] == 3
    by_name = {p["name"]: p for p in result["profiles"]}
    assert by_name["pg"]["backend"] == "postgresql"
    assert by_name["dbr"]["backend"] == "databricks"
    assert by_name["duck"]["backend"] == "duckdb"


def test_active_flag_tracks_scope(cfg_with_secrets) -> None:
    result = _invoke(cfg_with_secrets, ["dbr"])
    by_name = {p["name"]: p for p in result["profiles"]}
    assert by_name["dbr"]["active"] is True
    assert by_name["pg"]["active"] is False
    assert result["active_scope"] == ["dbr"]


def test_non_secret_coords_present(cfg_with_secrets) -> None:
    result = _invoke(cfg_with_secrets, ["pg"])
    by_name = {p["name"]: p for p in result["profiles"]}
    pg_conn = by_name["pg"]["connection"]
    assert pg_conn["host"] == "pg.internal"
    assert pg_conn["database"] == "sales"
    assert pg_conn["user"] == "reporting"
    dbr_conn = by_name["dbr"]["connection"]
    assert dbr_conn["http_path"] == "/sql/1.0/warehouses/w1"
    assert dbr_conn["catalog"] == "main"


def test_no_secret_leaks_anywhere(cfg_with_secrets) -> None:
    """The full serialized payload must not contain any secret value or
    any secret-bearing key name."""
    result = _invoke(cfg_with_secrets, ["pg", "dbr", "duck"])
    blob = json.dumps(result)

    for secret in (_SECRET_TOKEN, _SECRET_PW, _SECRET_MD):
        assert secret not in blob, f"secret value leaked: {secret}"

    for conn in (p["connection"] for p in result["profiles"]):
        for forbidden in (
            "password",
            "access_token",
            "jwt_token",
            "workspace_token",
            "motherduck_token",
            "credentials_path",
        ):
            assert forbidden not in conn, f"secret key exposed: {forbidden}"


def test_data_summary_null_without_history_store(cfg_with_secrets) -> None:
    """With no history-store singleton in the test process, the
    history-backed counts degrade to ``null`` (not an error, not 0)."""
    result = _invoke(cfg_with_secrets, ["pg"])
    data = result["profiles"][0]["available_data"]
    # Catalog-backed signal still resolves (MagicMock state).
    assert data["synced_tables"] == 5
    assert data["sync_state"] == "complete"
    # History-store-backed counts are unknown without the singleton.
    assert data["assets"] is None
    assert data["lineage_graphs"] is None
    assert data["past_runs"] is None


def test_empty_config_returns_note() -> None:
    cfg = AMXConfig()
    cfg.db_profiles = {}
    catalog = MagicMock()
    box = ToolBox(cfg, catalog, db_profiles=[])
    try:
        result = json.loads(box.invoke("list_db_profiles", "{}"))
    finally:
        box.close()
    assert result["count"] == 0
    assert result["profiles"] == []
    assert "note" in result
