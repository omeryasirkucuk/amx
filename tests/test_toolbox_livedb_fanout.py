"""Live-DB multi-profile fan-out tests for PR ask-B.

Verify that when a ToolBox is bound to N profiles, ``list_schemas`` and
``list_tables_in_schema`` parallel-query each profile's own connector,
each result row carries ``db_profile``, and per-profile timeouts /
errors don't kill the whole question — they're surfaced on the bad
profile's slot while the others come back successfully.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.config import AMXConfig, DBConfig
from amx.search.agent_tools import ToolBox


@pytest.fixture()
def cfg_three_profiles() -> AMXConfig:
    cfg = AMXConfig()
    cfg.db_profiles = {
        "alpha": DBConfig(backend="postgresql", host="a", database="appa"),
        "beta": DBConfig(backend="postgresql", host="b", database="appb"),
        "gamma": DBConfig(backend="postgresql", host="c", database="appc"),
    }
    cfg.active_db_profile = "alpha"
    cfg.active_db_profiles = ["alpha", "beta", "gamma"]
    cfg.db = cfg.db_profiles["alpha"]
    return cfg


def _stub_connector(schemas: list[str]) -> MagicMock:
    conn = MagicMock()
    conn.supports_catalogs = MagicMock(return_value=False)
    conn.list_schemas = MagicMock(return_value=schemas)
    conn.list_catalogs = MagicMock(return_value=[])
    conn.close = MagicMock()
    return conn


def test_list_schemas_fans_out_across_profiles(cfg_three_profiles) -> None:
    """Each profile gets its own list_schemas call; the result groups
    by db_profile so the LLM can mention which profile owns what."""
    box = ToolBox(
        cfg_three_profiles,
        MagicMock(),
        db_profiles=["alpha", "beta", "gamma"],
        db_connectors={
            "alpha": _stub_connector(["public", "sales"]),
            "beta": _stub_connector(["analytics"]),
            "gamma": _stub_connector(["raw", "stage", "core"]),
        },
    )
    result = box._tool_list_schemas()
    assert result["multi_profile"] is True
    assert result["scope"] == ["alpha", "beta", "gamma"]
    assert result["total_schemas"] == 6
    assert result["profiles_with_errors"] == []
    assert set(result["profiles"].keys()) == {"alpha", "beta", "gamma"}
    assert result["profiles"]["alpha"]["schemas"] == ["public", "sales"]
    assert result["profiles"]["beta"]["schemas"] == ["analytics"]
    assert result["profiles"]["gamma"]["count"] == 3
    # Each row tagged with its profile.
    for name, payload in result["profiles"].items():
        assert payload["db_profile"] == name


def test_list_schemas_one_profile_errors_others_continue(cfg_three_profiles) -> None:
    """A backend that throws (auth fail, network) on profile 'beta'
    must NOT kill alpha + gamma. The bad profile reports an error
    string and the others surface their schemas."""
    bad = MagicMock()
    bad.supports_catalogs = MagicMock(return_value=False)
    bad.list_schemas = MagicMock(side_effect=RuntimeError("auth failed"))
    bad.close = MagicMock()
    box = ToolBox(
        cfg_three_profiles,
        MagicMock(),
        db_profiles=["alpha", "beta", "gamma"],
        db_connectors={
            "alpha": _stub_connector(["public"]),
            "beta": bad,
            "gamma": _stub_connector(["raw"]),
        },
    )
    result = box._tool_list_schemas()
    assert result["total_schemas"] == 2  # alpha + gamma; beta dropped
    assert "beta" in result["profiles_with_errors"]
    assert "auth failed" in result["profiles"]["beta"]["error"]
    assert result["profiles"]["alpha"]["count"] == 1
    assert result["profiles"]["gamma"]["count"] == 1


def test_list_schemas_targeted_profile_skips_fanout(cfg_three_profiles) -> None:
    """When the LLM names a single profile via ``db_profile=`` the
    fan-out is bypassed and only that profile's connector runs."""
    alpha = _stub_connector(["public"])
    beta = _stub_connector(["analytics"])
    box = ToolBox(
        cfg_three_profiles,
        MagicMock(),
        db_profiles=["alpha", "beta"],
        db_connectors={"alpha": alpha, "beta": beta},
    )
    result = box._tool_list_schemas(db_profile="beta")
    # Single-target dispatch — not the multi_profile envelope.
    assert "multi_profile" not in result
    assert result["db_profile"] == "beta"
    assert result["schemas"] == ["analytics"]
    # alpha was NOT called — single-target dispatch only touched beta.
    alpha.list_schemas.assert_not_called()
    beta.list_schemas.assert_called_once()


def test_list_tables_in_schema_fans_out(cfg_three_profiles) -> None:
    """Multi-profile list_tables: each profile reports tables OR
    'schema not found' with its visible schemas. The LLM uses the
    found_in list to know which profile actually owns the schema."""
    alpha = _stub_connector(["sales"])
    alpha.list_assets = MagicMock(return_value=[("orders", "table"), ("returns", "view")])
    beta = _stub_connector(["analytics"])
    # beta has no 'sales' schema — list_tables_in_schema should
    # report found:False with available_schemas.
    gamma = _stub_connector(["sales"])
    gamma.list_assets = MagicMock(return_value=[("invoices", "table")])

    box = ToolBox(
        cfg_three_profiles,
        MagicMock(),
        db_profiles=["alpha", "beta", "gamma"],
        db_connectors={"alpha": alpha, "beta": beta, "gamma": gamma},
    )
    result = box._tool_list_tables_in_schema(schema="sales")
    assert result["multi_profile"] is True
    assert sorted(result["found_in"]) == ["alpha", "gamma"]
    assert result["profiles"]["beta"]["found"] is False
    assert "available_schemas" in result["profiles"]["beta"]
    assert result["total_tables"] == 3  # alpha 2 + gamma 1


def test_owned_connector_closed_on_exit(cfg_three_profiles, monkeypatch) -> None:
    """Connectors that ToolBox owns (lazy-built for non-anchor
    profiles) get .close()'d on context exit. Caller-supplied
    connectors (db_connectors) are NOT closed — their lifetime is
    the caller's responsibility (Studio's _CONNECTOR_CACHE)."""
    from amx.search import agent_tools as _at

    built: list[MagicMock] = []

    def _factory(_db_cfg) -> MagicMock:
        m = _stub_connector(["public"])
        built.append(m)
        return m

    monkeypatch.setattr(_at, "DatabaseConnector", _factory)

    supplied = _stub_connector(["public"])
    with ToolBox(
        cfg_three_profiles,
        MagicMock(),
        db_profiles=["alpha", "beta"],
        db_connectors={"alpha": supplied},
    ) as box:
        # Trigger lazy build for the unowned profile.
        box._connector_for_profile("beta")
    # 'beta' was opened by ToolBox → must have been closed.
    assert len(built) == 1
    built[0].close.assert_called()
    # 'alpha' was supplied → must NOT have been closed by ToolBox.
    supplied.close.assert_not_called()
