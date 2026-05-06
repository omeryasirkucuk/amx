"""``list_databases`` returns full reach per profile, not just the pinned one.

Reported: Studio's /ask says "You have 2 database profiles available:
dbr → amx_test on Databricks, and test-postgre → bird_train_desc on
PostgreSQL". Each profile shows ONE database — the one pinned in that
profile's config — even though both connections expose more
databases / catalogs than that.

Root cause: ``_tool_list_databases`` enumerated ``cfg.db_profiles``
and surfaced each profile's pinned database/catalog/project field
verbatim. To answer the literal question "which databases do I have"
we need to call each profile's connector and list every reachable
database (or catalog on 3-level backends).

These tests pin the new contract: per-profile fan-out, full list
returned, error-tolerant.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox


@pytest.fixture()
def cfg_two_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "dbr": DBConfig(
            backend="databricks",
            host="dbc-xxx.databricks.com",
            catalog="amx_test",
        ),
        "test-postgre": DBConfig(
            backend="postgresql",
            host="pg.local",
            database="bird_train_desc",
        ),
    }
    cfg.active_db_profile = "test-postgre"
    cfg.active_db_profiles = ["dbr", "test-postgre"]
    cfg.db = cfg.db_profiles["test-postgre"]
    return cfg


def _stub_pg_connector(databases: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.supports_catalogs = MagicMock(return_value=False)
    conn.list_databases = MagicMock(return_value=databases)
    conn.list_catalogs = MagicMock(return_value=[])
    return conn


def _stub_databricks_connector(catalogs: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.supports_catalogs = MagicMock(return_value=True)
    conn.list_catalogs = MagicMock(return_value=catalogs)
    conn.list_databases = MagicMock(return_value=[])
    return conn


def test_list_databases_returns_all_per_profile(cfg_two_profiles) -> None:
    """The bug-report scenario: 2 profiles, each with multiple
    databases / catalogs visible. The tool must surface ALL of them,
    grouped by profile, not just the pinned ones."""
    box = ToolBox(
        cfg_two_profiles,
        MagicMock(),
        db_profiles=["dbr", "test-postgre"],
        db_connectors={
            "dbr": _stub_databricks_connector(["amx_test", "samples", "main", "system"]),
            "test-postgre": _stub_pg_connector(["bird_train_desc", "postgres", "analytics", "raw"]),
        },
    )
    result = box._tool_list_databases()

    assert sorted(result["scope"]) == ["dbr", "test-postgre"]
    assert result["count"] == 2
    assert result["profiles_with_errors"] == []

    dbr = result["profiles"]["dbr"]
    assert dbr["supports_catalogs"] is True
    assert dbr["catalogs"] == ["amx_test", "samples", "main", "system"]
    assert dbr["pinned_catalog"] == "amx_test"

    pg = result["profiles"]["test-postgre"]
    assert pg["supports_catalogs"] is False
    assert pg["databases"] == ["bird_train_desc", "postgres", "analytics", "raw"]
    assert pg["pinned_database"] == "bird_train_desc"

    # Total reach across BOTH profiles, not just the pinned ones.
    assert result["total_reachable"] == 4 + 4


def test_list_databases_propagates_errors_per_profile(cfg_two_profiles) -> None:
    """One profile's connection refuses (auth, network) → that
    profile reports the error, the other still returns its full
    reach. Partial results, never blocked."""
    bad = MagicMock()
    bad.supports_catalogs = MagicMock(return_value=False)
    bad.list_databases = MagicMock(side_effect=RuntimeError("auth failed"))

    box = ToolBox(
        cfg_two_profiles,
        MagicMock(),
        db_profiles=["dbr", "test-postgre"],
        db_connectors={
            "dbr": _stub_databricks_connector(["amx_test", "main"]),
            "test-postgre": bad,
        },
    )
    result = box._tool_list_databases()

    assert "test-postgre" in result["profiles_with_errors"]
    assert "auth failed" in result["profiles"]["test-postgre"]["error"]
    # The healthy profile still returned its reach.
    assert result["profiles"]["dbr"]["catalogs"] == ["amx_test", "main"]


def test_list_databases_falls_back_to_pinned_when_no_reach(cfg_two_profiles) -> None:
    """When the live connector returns an empty list (role can only
    see the pinned db), the pinned name is still surfaced via
    ``pinned_database`` / ``pinned_catalog`` so the LLM doesn't lose
    the hint that the connection is configured."""
    empty_pg = _stub_pg_connector([])
    empty_dbr = _stub_databricks_connector([])

    box = ToolBox(
        cfg_two_profiles,
        MagicMock(),
        db_profiles=["dbr", "test-postgre"],
        db_connectors={"dbr": empty_dbr, "test-postgre": empty_pg},
    )
    result = box._tool_list_databases()
    assert result["profiles"]["dbr"]["catalogs"] == []
    assert result["profiles"]["dbr"]["pinned_catalog"] == "amx_test"
    assert result["profiles"]["test-postgre"]["databases"] == []
    assert result["profiles"]["test-postgre"]["pinned_database"] == "bird_train_desc"


def test_list_databases_single_profile_scope(cfg_two_profiles) -> None:
    """Single-profile scope still uses the per-profile envelope —
    consistent shape regardless of scope size means the LLM doesn't
    branch its rendering on profile count."""
    box = ToolBox(
        cfg_two_profiles,
        MagicMock(),
        db_profiles=["test-postgre"],
        db_connectors={
            "test-postgre": _stub_pg_connector(["a", "b", "c"]),
        },
    )
    result = box._tool_list_databases()
    assert result["scope"] == ["test-postgre"]
    assert result["count"] == 1
    assert result["profiles"]["test-postgre"]["databases"] == ["a", "b", "c"]
