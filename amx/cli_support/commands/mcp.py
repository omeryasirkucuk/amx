"""Slash-command handler for /mcp.

Entry point: cmd_mcp(cfg, rest) — dispatched from session.py.

``/mcp`` connects AMX's read-only catalog to an IDE code agent (Cursor,
Claude Desktop, VS Code) over the Model Context Protocol. Bare ``/mcp``
runs a wizard (operation → IDE → profile scope); flags are an optional
power-user shortcut.

Subcommands
-----------
(none)            interactive wizard
connect [ide]     wire an IDE (``--profiles a,b`` to pin a scope)
status            show which IDEs are connected
snippet [ide]     print the config block to paste by hand
disconnect [ide]  remove AMX's entry from an IDE

The actual file read/write lives in :mod:`amx.mcp.config_writer`; this
module is only the CLI surface. Studio's MCP tab drives the same engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from amx.mcp import config_writer, ide_targets
from amx.mcp.ide_targets import IdeTarget

if TYPE_CHECKING:
    from amx.config import AMXConfig

_USAGE = (
    "Usage:\n"
    "  /mcp                       -- interactive wizard (recommended)\n"
    "  /mcp connect [ide]         -- connect an IDE (--profiles a,b to pin scope)\n"
    "  /mcp status                -- show which IDEs are connected\n"
    "  /mcp snippet [ide]         -- print the config block to paste manually\n"
    "  /mcp disconnect [ide]      -- remove AMX from an IDE\n"
    "\n"
    "Supported IDEs: " + ", ".join(t.label for t in ide_targets.all_targets())
)


# --------------------------------------------------------------------------- #
# Pickers (wizard primitives)
# --------------------------------------------------------------------------- #


def _pick_one(label: str, options: list[str]) -> int | None:
    """Single-pick prompt; returns the chosen index or None on cancel."""
    from amx.cli_support.review_picker import pick_rows
    from amx.utils.console import info

    if not options:
        return None
    info(f"Pick a {label}:")
    picked = pick_rows(options)
    if not picked:
        return None
    return picked[0]


def _pick_ide() -> IdeTarget | None:
    """Pick one supported IDE; None on cancel."""
    targets = ide_targets.all_targets()
    idx = _pick_one("IDE", [t.label for t in targets])
    if idx is None:
        return None
    return targets[idx]


def _pick_profiles(cfg: AMXConfig) -> list[str] | None:
    """Resolve the profile scope for a connect.

    Returns ``None`` for "use the active scope" (the server falls back to
    ``cfg.effective_db_profiles()``), or an explicit non-empty list.
    """
    from amx.cli_support.review_picker import pick_rows
    from amx.utils.console import info

    names = list(cfg.db_profiles.keys())
    options = ["Active profiles (default)", "Pick specific profiles…"]
    idx = _pick_one("profile scope", options)
    if idx is None or idx == 0 or not names:
        return None
    info("Select profiles to expose (multi-select):")
    picked = pick_rows(names)
    chosen = [names[i] for i in picked]
    return chosen or None


# --------------------------------------------------------------------------- #
# Operations
# --------------------------------------------------------------------------- #


def _ensure_sdk() -> bool:
    """Install the MCP SDK now (interactive), so the IDE-spawned server
    starts without an install step on its stdout. Returns success."""
    from amx.utils.console import error

    try:
        from amx.utils.optional_deps import ensure

        ensure("mcp")
        return True
    except Exception as exc:  # pragma: no cover - install env dependent
        error(f"Could not install the MCP SDK: {exc}")
        return False


def _do_connect(cfg: AMXConfig, target: IdeTarget, profiles: list[str] | None) -> None:
    from amx.utils.console import error, info

    if not _ensure_sdk():
        return
    try:
        result = config_writer.connect(target, profiles)
    except ValueError as exc:
        error(str(exc))
        return

    scope = ", ".join(profiles) if profiles else "active profiles"
    info(f"✓ Connected AMX to {result.label} (scope: {scope}).")
    info(f"  Config written to: {result.config_path}")
    info("  Next steps:")
    for step in result.post_connect_steps:
        info(f"    • {step}")


def _do_disconnect(target: IdeTarget) -> None:
    from amx.utils.console import error, info

    try:
        removed = config_writer.disconnect(target)
    except ValueError as exc:
        error(str(exc))
        return
    if removed:
        info(f"✓ Removed AMX from {target.label}. Restart {target.label} to apply.")
    else:
        info(f"AMX was not configured in {target.label}; nothing to remove.")


def _do_snippet(target: IdeTarget, profiles: list[str] | None) -> None:
    from amx.utils.console import info

    info(f"Paste this into {target.label}'s MCP config ({target.config_path()}):")
    info(config_writer.snippet(target, profiles))


def _do_status(cfg: AMXConfig) -> None:
    from amx.mcp import tool_bridge
    from amx.search.agent_tools import ToolBox
    from amx.utils.console import info

    n_tools = len(tool_bridge.mcp_tool_payloads(ToolBox.schemas()))
    info(f"AMX MCP server exposes {n_tools} read-only catalog tools (cache-only).")
    info("")
    for st in (config_writer.status(t) for t in ide_targets.all_targets()):
        if st.error:
            info(f"  {st.label:<16} ⚠ config unreadable — {st.error}")
            continue
        if not st.connected:
            info(f"  {st.label:<16} — not connected")
            continue
        scope = ", ".join(st.profiles) if st.profiles else "active profiles"
        if st.drifted:
            info(
                f"  {st.label:<16} ⚠ connected but interpreter path drifted "
                f"(scope: {scope}) — run /mcp connect {st.ide} to repair"
            )
        else:
            info(f"  {st.label:<16} ✓ connected (scope: {scope})")
    info("")
    info("Run /mcp to connect or manage an IDE.")


# --------------------------------------------------------------------------- #
# Flag parsing (power-user shortcut)
# --------------------------------------------------------------------------- #


def _extract_profiles_flag(args: list[str]) -> tuple[list[str], list[str] | None]:
    """Split ``--profiles a,b`` out of ``args``.

    Returns ``(remaining_args, profiles)`` where ``profiles`` is ``None``
    when the flag is absent.
    """
    profiles: list[str] | None = None
    remaining: list[str] = []
    i = 0
    while i < len(args):
        if args[i] == "--profiles" and i + 1 < len(args):
            profiles = [p.strip() for p in args[i + 1].split(",") if p.strip()] or None
            i += 2
            continue
        remaining.append(args[i])
        i += 1
    return remaining, profiles


def _resolve_ide_arg(name: str) -> IdeTarget | None:
    from amx.utils.console import error

    target = ide_targets.get_target(name)
    if target is None:
        error(f"Unknown IDE {name!r}. Supported: " + ", ".join(ide_targets.target_keys()))
    return target


# --------------------------------------------------------------------------- #
# Wizard
# --------------------------------------------------------------------------- #


def _run_wizard(cfg: AMXConfig) -> None:
    from amx.utils.console import info

    operations = [
        "Connect an IDE",
        "Show status",
        "Show config snippet",
        "Disconnect an IDE",
    ]
    op = _pick_one("operation", operations)
    if op is None:
        info("Cancelled.")
        return

    if op == 1:  # Show status
        _do_status(cfg)
        return

    target = _pick_ide()
    if target is None:
        info("Cancelled.")
        return

    if op == 0:  # Connect
        profiles = _pick_profiles(cfg)
        _do_connect(cfg, target, profiles)
    elif op == 2:  # Snippet
        profiles = _pick_profiles(cfg)
        _do_snippet(target, profiles)
    elif op == 3:  # Disconnect
        _do_disconnect(target)


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def cmd_mcp(cfg: AMXConfig, rest: list[str]) -> None:
    """Dispatch /mcp — wizard when bare, subcommands for power users."""
    from amx.utils.console import info

    if not rest:
        _run_wizard(cfg)
        return

    sub = rest[0].lower()
    args, profiles = _extract_profiles_flag(rest[1:])

    if sub == "status":
        _do_status(cfg)
    elif sub == "connect":
        target = _resolve_ide_arg(args[0]) if args else _pick_ide()
        if target is None:
            return
        if not args and profiles is None:
            profiles = _pick_profiles(cfg)
        _do_connect(cfg, target, profiles)
    elif sub == "disconnect":
        target = _resolve_ide_arg(args[0]) if args else _pick_ide()
        if target is None:
            return
        _do_disconnect(target)
    elif sub == "snippet":
        target = _resolve_ide_arg(args[0]) if args else _pick_ide()
        if target is None:
            return
        _do_snippet(target, profiles)
    else:
        info(f"Unknown /mcp subcommand: {sub!r}\n\n{_USAGE}")
