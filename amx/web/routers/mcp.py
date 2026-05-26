"""Studio API for the MCP tab (Settings → MCP).

Thin HTTP surface over :mod:`amx.mcp.config_writer` / :mod:`amx.mcp.ide_targets`
/ :mod:`amx.mcp.tool_bridge` — the exact same engine the ``/mcp`` REPL
command drives, so the CLI and Studio stay in lockstep. No MCP-specific
logic lives here; the router only translates between HTTP and the engine.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from amx.config import AMXConfig
from amx.mcp import config_writer, ide_targets, tool_bridge
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/mcp", tags=["mcp"])


def _status_dict(st: config_writer.IdeStatus) -> dict[str, Any]:
    return {
        "ide": st.ide,
        "label": st.label,
        "config_path": st.config_path,
        "connected": st.connected,
        "drifted": st.drifted,
        "profiles": st.profiles,
        "error": st.error,
    }


def _require_target(ide: str) -> ide_targets.IdeTarget:
    target = ide_targets.get_target(ide)
    if target is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown IDE {ide!r}. Supported: {', '.join(ide_targets.target_keys())}.",
        )
    return target


def _exposed_tools() -> list[dict[str, str]]:
    """Name + description of every tool the MCP server exposes."""
    from amx.search.agent_tools import ToolBox

    return [
        {"name": p["name"], "description": p["description"]}
        for p in tool_bridge.mcp_tool_payloads(ToolBox.schemas())
    ]


@router.get("/status")
def mcp_status(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Per-IDE connection status plus the profile options for connecting."""
    return {
        "ides": [_status_dict(config_writer.status(t)) for t in ide_targets.all_targets()],
        "tool_count": len(_exposed_tools()),
        "available_profiles": sorted(cfg.db_profiles.keys()),
        "active_profiles": cfg.effective_db_profiles(),
    }


@router.get("/tools")
def mcp_tools() -> dict[str, Any]:
    """The read-only catalog tools exposed over MCP (transparency panel)."""
    tools = _exposed_tools()
    return {"tools": tools, "count": len(tools)}


@router.get("/snippet")
def mcp_snippet(ide: str, profiles: str | None = None) -> dict[str, Any]:
    """Config block a user could paste into the IDE by hand."""
    target = _require_target(ide)
    scope = [p.strip() for p in profiles.split(",") if p.strip()] if profiles else None
    return {
        "ide": target.key,
        "label": target.label,
        "config_path": str(target.config_path()),
        "snippet": config_writer.snippet(target, scope),
    }


@router.post("/connect")
def mcp_connect(body: dict[str, Any]) -> dict[str, Any]:
    """Write AMX's server entry into the chosen IDE's config (idempotent)."""
    target = _require_target(str(body.get("ide", "")))
    raw_profiles = body.get("profiles")
    profiles = (
        [str(p).strip() for p in raw_profiles if str(p).strip()]
        if isinstance(raw_profiles, list) and raw_profiles
        else None
    )
    # Install the SDK now so the IDE-spawned server starts cleanly. In the
    # Studio worker stdin is not a TTY, so optional_deps proceeds without a
    # prompt.
    try:
        from amx.utils.optional_deps import ensure

        ensure("mcp")
    except Exception as exc:  # pragma: no cover - install env dependent
        raise HTTPException(
            status_code=500, detail=f"Could not install the MCP SDK: {exc}"
        ) from exc

    try:
        result = config_writer.connect(target, profiles)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return {
        "ok": True,
        "ide": result.ide,
        "label": result.label,
        "config_path": result.config_path,
        "post_connect_steps": list(result.post_connect_steps),
        "status": _status_dict(config_writer.status(target)),
    }


@router.post("/disconnect")
def mcp_disconnect(body: dict[str, Any]) -> dict[str, Any]:
    """Remove only AMX's entry from the chosen IDE's config."""
    target = _require_target(str(body.get("ide", "")))
    try:
        removed = config_writer.disconnect(target)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "ok": True,
        "removed": removed,
        "ide": target.key,
        "label": target.label,
        "status": _status_dict(config_writer.status(target)),
    }
