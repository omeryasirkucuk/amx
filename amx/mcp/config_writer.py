"""Read and write each IDE's MCP config file on behalf of the user.

This is the stateless engine shared by the ``/mcp`` REPL command and the
Studio MCP tab. It owns four operations against an :class:`IdeTarget`'s
config file:

* :func:`connect` — idempotently add/update AMX's server entry.
* :func:`disconnect` — remove AMX's server entry only.
* :func:`status` — report whether AMX is wired in, and whether the stored
  interpreter path still matches this install (drift detection).
* :func:`snippet` — render the entry for manual pasting.

There is deliberately **no AMX-side state**: "is this IDE connected?" is
derived entirely from the on-disk IDE config. That is what makes setup
durable — once written, the IDE re-spawns the server on every launch,
and re-opening AMX simply re-reads the file and reports "already
connected". The spawn command pins the absolute interpreter path
(``sys.executable``) so it survives reboots and PATH changes.

SDK-free: only the standard library is used, so this imports without the
``mcp`` package installed.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amx.mcp.ide_targets import SERVER_KEY, IdeTarget


@dataclass(frozen=True)
class IdeStatus:
    """Derived connection state for one IDE."""

    ide: str
    label: str
    config_path: str
    connected: bool
    #: True when an AMX entry exists but its ``command`` no longer points
    #: at the current interpreter (e.g. AMX was reinstalled in a new venv).
    drifted: bool
    profiles: list[str]
    #: Set when the config file exists but could not be parsed as JSON.
    error: str | None = None


@dataclass(frozen=True)
class ConnectResult:
    """Outcome of a connect/disconnect operation."""

    ide: str
    label: str
    config_path: str
    post_connect_steps: tuple[str, ...]


def spawn_command(profiles: list[str] | None = None) -> dict[str, Any]:
    """The command an IDE should run to launch AMX's MCP server.

    Uses the absolute path of the *current* interpreter so the entry keeps
    working regardless of the user's shell PATH or venv activation — the
    single most common reason a hand-written MCP entry fails. Invoking the
    package as ``-m amx.mcp`` (rather than a console-script shim) avoids
    PATH/shim fragility entirely.
    """
    args = ["-m", "amx.mcp"]
    if profiles:
        args += ["--profiles", ",".join(profiles)]
    return {"command": os.path.abspath(sys.executable), "args": args}


def build_entry(target: IdeTarget, profiles: list[str] | None = None) -> dict[str, Any]:
    """The server-definition object for one IDE's config schema."""
    entry = spawn_command(profiles)
    if target.entry_type is not None:
        # VS Code requires {"type": "stdio", ...}; place it first for
        # readability of the rendered snippet.
        return {"type": target.entry_type, **entry}
    return entry


def _read_config(path: Path) -> tuple[dict[str, Any], str | None]:
    """Return ``(data, error)``. Missing file → ``({}, None)``."""
    if not path.exists():
        return {}, None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {}, f"Could not read {path}: {exc}"
    if not text.strip():
        return {}, None
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return {}, f"{path} is not valid JSON: {exc}"
    if not isinstance(data, dict):
        return {}, f"{path} does not contain a JSON object."
    return data, None


def _atomic_write(path: Path, data: dict[str, Any]) -> None:
    """Write ``data`` as pretty JSON, atomically, creating parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2) + "\n"
    # Write to a temp file in the same directory, then replace — os.replace
    # is atomic on every supported OS and avoids a half-written config.
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".amx-mcp-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)


def connect(target: IdeTarget, profiles: list[str] | None = None) -> ConnectResult:
    """Idempotently wire AMX into ``target``'s config file.

    Preserves every unrelated server already defined in the file and
    upserts the single ``amx`` entry. Re-running never appends a
    duplicate; it overwrites the existing entry in place.
    """
    path = target.config_path()
    data, error = _read_config(path)
    if error:
        # Don't clobber a file we couldn't parse — surface the problem.
        raise ValueError(error)
    servers = data.get(target.config_key)
    if not isinstance(servers, dict):
        servers = {}
    servers[SERVER_KEY] = build_entry(target, profiles)
    data[target.config_key] = servers
    _atomic_write(path, data)
    return ConnectResult(
        ide=target.key,
        label=target.label,
        config_path=str(path),
        post_connect_steps=target.post_connect_steps,
    )


def disconnect(target: IdeTarget) -> bool:
    """Remove only AMX's entry from ``target``'s config. Returns whether
    an entry was actually present and removed."""
    path = target.config_path()
    data, error = _read_config(path)
    if error:
        raise ValueError(error)
    servers = data.get(target.config_key)
    if not isinstance(servers, dict) or SERVER_KEY not in servers:
        return False
    del servers[SERVER_KEY]
    data[target.config_key] = servers
    _atomic_write(path, data)
    return True


def status(target: IdeTarget) -> IdeStatus:
    """Derive ``target``'s connection state from its on-disk config."""
    path = target.config_path()
    data, error = _read_config(path)
    if error:
        return IdeStatus(
            ide=target.key,
            label=target.label,
            config_path=str(path),
            connected=False,
            drifted=False,
            profiles=[],
            error=error,
        )
    servers = data.get(target.config_key)
    entry = servers.get(SERVER_KEY) if isinstance(servers, dict) else None
    if not isinstance(entry, dict):
        return IdeStatus(
            ide=target.key,
            label=target.label,
            config_path=str(path),
            connected=False,
            drifted=False,
            profiles=[],
        )
    expected = os.path.abspath(sys.executable)
    drifted = os.path.normcase(str(entry.get("command", ""))) != os.path.normcase(expected)
    return IdeStatus(
        ide=target.key,
        label=target.label,
        config_path=str(path),
        connected=True,
        drifted=drifted,
        profiles=_profiles_from_args(entry.get("args")),
    )


def snippet(target: IdeTarget, profiles: list[str] | None = None) -> str:
    """Render the config block a user could paste by hand."""
    block = {target.config_key: {SERVER_KEY: build_entry(target, profiles)}}
    return json.dumps(block, indent=2)


def _profiles_from_args(args: Any) -> list[str]:
    """Recover the ``--profiles a,b`` scope from a stored args list."""
    if not isinstance(args, list):
        return []
    try:
        idx = args.index("--profiles")
    except ValueError:
        return []
    if idx + 1 >= len(args):
        return []
    raw = args[idx + 1]
    if not isinstance(raw, str):
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]
