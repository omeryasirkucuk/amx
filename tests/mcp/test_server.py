"""Tests for the headless MCP server wiring."""

from __future__ import annotations

import os
import sys

import pytest

from amx.config import AMXConfig
from amx.mcp import server as mcp_server


def test_tool_payloads_are_cache_only():
    payloads = mcp_server._tool_payloads()
    names = {p["name"] for p in payloads}
    assert len(names) >= 10
    for live in ("sample_column_values", "check_uniqueness", "inspect_data_quality"):
        assert live not in names
    # Every payload has the MCP-required fields.
    for p in payloads:
        assert p["name"] and "inputSchema" in p


def test_build_toolbox_is_cache_only(tmp_path):
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    tb = mcp_server.build_toolbox(cfg, None)
    try:
        # The safety gate: live refresh is off, so live-only tools are
        # rejected at invoke time.
        assert tb._allow_live_refresh is False
    finally:
        tb.close()


def test_catalog_path_uses_config_dir(tmp_path):
    cfg = AMXConfig()
    cfg.CONFIG_DIR = str(tmp_path)
    assert mcp_server._catalog_path(cfg) == tmp_path / "history.db"


@pytest.mark.integration
def test_end_to_end_handshake(tmp_path):
    """Spawn ``python -m amx.mcp`` and drive it with a real MCP client.

    Skipped unless the MCP SDK is already importable (it is lazily
    installed in production, so CI without the dev extra skips this).
    """
    pytest.importorskip("mcp")
    import anyio
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = dict(os.environ, AMX_CONFIG_DIR=str(tmp_path))
    params = StdioServerParameters(command=sys.executable, args=["-m", "amx.mcp"], env=env)

    async def run() -> None:
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                assert init.serverInfo.name == "amx"
                tools = await session.list_tools()
                names = [t.name for t in tools.tools]
                assert len(names) >= 10
                assert "sample_column_values" not in names

    anyio.run(run)
