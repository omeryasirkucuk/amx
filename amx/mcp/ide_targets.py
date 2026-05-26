"""Per-IDE knowledge for wiring AMX's MCP server into a code editor.

Each supported IDE stores MCP server definitions in a different file,
under a different top-level key, with a slightly different entry shape.
This module captures those differences in one declarative registry so
:mod:`amx.mcp.config_writer` (and, through it, the ``/mcp`` command and
the Studio MCP tab) never hard-codes a path or schema quirk.

The module is SDK-free and side-effect-free: resolving a config path
reads ``os.environ`` and ``Path.home()`` only — it never touches the
filesystem. All paths resolve correctly on macOS, Windows, and Linux
(House rule: AMX is cross-platform first-class).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

#: The server name AMX registers under inside every IDE config file.
#: Stable so re-running connect updates the same entry instead of
#: appending duplicates.
SERVER_KEY = "amx"


@dataclass(frozen=True)
class IdeTarget:
    """One supported IDE and how to write its MCP config.

    ``config_key`` is the top-level object that holds server
    definitions — ``"mcpServers"`` for Cursor / Claude Desktop, but
    ``"servers"`` for VS Code. ``entry_type`` is the value VS Code
    expects in a per-server ``"type"`` field (``"stdio"``); ``None``
    means the IDE does not use a type field (Cursor / Claude).
    """

    key: str
    label: str
    config_key: str
    entry_type: str | None
    post_connect_steps: tuple[str, ...]
    #: Relative path segments under the platform base dir, resolved by
    #: :meth:`config_path`. The base dir itself is platform-specific.
    _path_factory: PathFactory = field(repr=False, default=None)  # type: ignore[assignment]

    def config_path(self) -> Path:
        """Absolute path to this IDE's MCP config file for the current OS."""
        return self._path_factory()


class PathFactory:
    """Callable that resolves an IDE's config path, OS-aware.

    Kept as a small class (rather than a lambda) so the dataclass stays
    hashable/frozen and the resolution logic is named and testable.
    """

    def __init__(
        self,
        *,
        cursor_home: tuple[str, ...] | None = None,
        app_support: tuple[str, ...] | None = None,
    ) -> None:
        self._cursor_home = cursor_home
        self._app_support = app_support

    def __call__(self) -> Path:  # pragma: no cover - thin dispatch
        if self._cursor_home is not None:
            return Path.home().joinpath(*self._cursor_home)
        if self._app_support is not None:
            return _app_data_dir().joinpath(*self._app_support)
        raise RuntimeError("PathFactory misconfigured")


def _app_data_dir() -> Path:
    """Per-user application-data base directory for the current OS.

    * Windows → ``%APPDATA%`` (falls back to ``~/AppData/Roaming``).
    * macOS   → ``~/Library/Application Support``.
    * Linux   → ``$XDG_CONFIG_HOME`` or ``~/.config``.
    """
    if sys.platform.startswith("win"):
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata)
        return Path.home() / "AppData" / "Roaming"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg)
    return Path.home() / ".config"


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
# Cursor reads a global ``~/.cursor/mcp.json`` on every platform.
# Claude Desktop and VS Code store their config under the per-user
# application-data dir, which differs per OS (handled by _app_data_dir).

_TARGETS: dict[str, IdeTarget] = {
    "cursor": IdeTarget(
        key="cursor",
        label="Cursor",
        config_key="mcpServers",
        entry_type=None,
        post_connect_steps=(
            "Fully quit and reopen Cursor so it picks up the new MCP server.",
            "Open Cursor's chat in Agent mode and ask a data question — it will "
            "call AMX automatically.",
        ),
        _path_factory=PathFactory(cursor_home=(".cursor", "mcp.json")),
    ),
    "claude": IdeTarget(
        key="claude",
        label="Claude Desktop",
        config_key="mcpServers",
        entry_type=None,
        post_connect_steps=(
            "Fully quit and reopen Claude Desktop so it picks up the new MCP server.",
            "AMX's tools appear under the tools (plug) icon in the message box.",
        ),
        _path_factory=PathFactory(app_support=("Claude", "claude_desktop_config.json")),
    ),
    "vscode": IdeTarget(
        key="vscode",
        label="VS Code",
        # VS Code uses the "servers" key (not "mcpServers") and requires
        # an explicit per-server "type": "stdio" field.
        config_key="servers",
        entry_type="stdio",
        post_connect_steps=(
            "Reload or restart VS Code so it discovers the new MCP server.",
            "Open Copilot Chat and switch it to Agent mode — MCP tools are only "
            "available there, not in Ask/Edit mode.",
            "Requires a recent VS Code with GitHub Copilot; accept the trust "
            "prompt if VS Code asks.",
        ),
        _path_factory=PathFactory(app_support=("Code", "User", "mcp.json")),
    ),
}


def all_targets() -> list[IdeTarget]:
    """Every supported IDE, in display order."""
    return list(_TARGETS.values())


def get_target(key: str) -> IdeTarget | None:
    """Look up a target by its key (case-insensitive); ``None`` if unknown."""
    return _TARGETS.get(key.strip().lower())


def target_keys() -> list[str]:
    """Keys of every supported IDE (e.g. ``["cursor", "claude", "vscode"]``)."""
    return list(_TARGETS.keys())
