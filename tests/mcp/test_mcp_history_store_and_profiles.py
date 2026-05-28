"""MCP exposure + history-store bootstrap regression tests.

Covers the two gaps closed in this branch:

* ``list_db_profiles`` is exposed over MCP (engine type + data summary).
* ``python -m amx.mcp`` bootstraps the history-store singleton so the
  history-backed tools (assets / runs / schedules / lineage / chat) work
  in the MCP subprocess instead of returning ``no_history_store``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.mcp import tool_bridge
from amx.search.agent_tools import ToolBox


def test_list_db_profiles_is_exposed_over_mcp() -> None:
    names = tool_bridge.exposed_tool_names(ToolBox.schemas())
    assert "list_db_profiles" in names


def test_list_db_profiles_schema_is_cache_ok() -> None:
    schema = next(
        e for e in ToolBox.schemas() if e.get("function", {}).get("name") == "list_db_profiles"
    )
    # cache_ok is the gate that lets tool_bridge surface it to the IDE.
    assert schema.get("freshness") == tool_bridge.FRESHNESS_CACHE_OK
    # No required arguments — it's the zero-arg entry-point tool.
    assert schema["function"]["parameters"]["required"] == []


def test_main_bootstraps_history_store(monkeypatch) -> None:
    """``__main__.main`` must call ``init_history_store`` before serving,
    mirroring the CLI / Studio-subprocess entry points. Without it, every
    history-backed tool returns an empty ``no_history_store`` envelope."""
    import amx.config as config_mod
    import amx.mcp.__main__ as mcp_main
    import amx.mcp.server as mcp_server
    import amx.storage.factory as factory_mod
    import amx.utils.optional_deps as optional_deps

    fake_cfg = MagicMock(name="cfg")
    monkeypatch.setattr(config_mod.AMXConfig, "load", classmethod(lambda cls, *a, **k: fake_cfg))
    monkeypatch.setattr(optional_deps, "ensure", lambda *a, **k: True)

    init_calls: list[object] = []
    monkeypatch.setattr(factory_mod, "init_history_store", lambda cfg: init_calls.append(cfg))

    serve_calls: list[tuple] = []
    monkeypatch.setattr(
        mcp_server, "serve_stdio", lambda cfg, profiles=None: serve_calls.append((cfg, profiles))
    )

    mcp_main.main([])

    assert init_calls == [fake_cfg], "init_history_store was not called with the loaded config"
    assert serve_calls and serve_calls[0][0] is fake_cfg


def test_main_serves_even_if_history_init_fails(monkeypatch) -> None:
    """A history-store bootstrap failure must not stop the server — the
    catalog (schema) tools should still come up."""
    import amx.config as config_mod
    import amx.mcp.__main__ as mcp_main
    import amx.mcp.server as mcp_server
    import amx.storage.factory as factory_mod
    import amx.utils.optional_deps as optional_deps

    fake_cfg = MagicMock(name="cfg")
    monkeypatch.setattr(config_mod.AMXConfig, "load", classmethod(lambda cls, *a, **k: fake_cfg))
    monkeypatch.setattr(optional_deps, "ensure", lambda *a, **k: True)

    def _boom(cfg):
        raise RuntimeError("history bootstrap failed")

    monkeypatch.setattr(factory_mod, "init_history_store", _boom)

    serve_calls: list[tuple] = []
    monkeypatch.setattr(
        mcp_server, "serve_stdio", lambda cfg, profiles=None: serve_calls.append((cfg, profiles))
    )

    mcp_main.main([])  # must not raise

    assert serve_calls and serve_calls[0][0] is fake_cfg
