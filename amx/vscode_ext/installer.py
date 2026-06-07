"""Pure logic for distributing the bundled AMX editor extension.

Everything here is side-effect-free except the three subprocess
wrappers (:func:`extension_status`, :func:`install`,
:func:`uninstall`), which always use explicit argv lists, capture
output, and run with timeouts so a hung editor CLI can never wedge the
REPL or a Studio worker. No shell strings, no POSIX-only paths — the
discovery table covers macOS app bundles, Windows ``%LOCALAPPDATA%``
shims, and common Linux install locations.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from importlib.resources import files as _resource_files
from pathlib import Path

EXTENSION_ID = "amx.amx-vscode"

_STATUS_TIMEOUT_S = 30
_INSTALL_TIMEOUT_S = 120


class InstallerError(Exception):
    """An editor CLI invocation failed.

    Carries a short ``message`` (stderr tail) plus a ``hint`` the CLI
    and Studio surfaces show next to it.
    """

    def __init__(self, message: str, hint: str) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint


@dataclass(frozen=True)
class EditorInfo:
    """A discovered VS Code-family editor and its CLI entry point."""

    id: str
    label: str
    cli_path: str


# Candidate table — one row per supported editor. Adding an editor is
# one tuple: (id, label, cli names, macOS .app bundle name, Windows
# %LOCALAPPDATA%/Programs directory name).
_CANDIDATES: tuple[tuple[str, str, tuple[str, ...], str, str], ...] = (
    ("vscode", "VS Code", ("code",), "Visual Studio Code.app", "Microsoft VS Code"),
    (
        "vscode-insiders",
        "VS Code Insiders",
        ("code-insiders",),
        "Visual Studio Code - Insiders.app",
        "Microsoft VS Code Insiders",
    ),
    ("cursor", "Cursor", ("cursor",), "Cursor.app", "cursor"),
    ("windsurf", "Windsurf", ("windsurf",), "Windsurf.app", "Windsurf"),
    ("vscodium", "VSCodium", ("codium",), "VSCodium.app", "VSCodium"),
)


def bundled_vsix_path() -> Path | None:
    """On-disk path of the VSIX vendored inside the wheel; None when absent."""
    try:
        candidate = Path(str(_resource_files("amx.assets").joinpath("vsix/amx-vscode.vsix")))
    except Exception:
        return None
    return candidate if candidate.is_file() else None


def bundled_vsix_version() -> str | None:
    """Version stamped into the bundled VSIX's package.json; None on any failure.

    A VSIX is a zip archive; the manifest of interest is
    ``extension/package.json``. Tolerant by design — a missing or
    malformed archive degrades to "version unknown", never an exception.
    """
    vsix = bundled_vsix_path()
    if vsix is None:
        return None
    try:
        with zipfile.ZipFile(vsix) as zf:
            manifest = json.loads(zf.read("extension/package.json"))
        version = manifest.get("version")
        return str(version) if version else None
    except Exception:
        return None


def _platform_fallbacks(cli: str, mac_app: str, win_dir: str) -> list[Path]:
    """Well-known install locations checked when the CLI is not on PATH."""
    if sys.platform == "darwin":
        return [Path("/Applications") / mac_app / "Contents/Resources/app/bin" / cli]
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            return []
        return [Path(local) / "Programs" / win_dir / "bin" / f"{cli}.cmd"]
    return [
        Path("/usr/bin") / cli,
        Path("/usr/local/bin") / cli,
        Path("/snap/bin") / cli,
    ]


def _resolve_cli(cli_names: tuple[str, ...], mac_app: str, win_dir: str) -> str | None:
    """First working CLI path for a candidate editor; None when not installed."""
    for cli in cli_names:
        hit = shutil.which(cli)
        if hit:
            return hit
    for cli in cli_names:
        for fallback in _platform_fallbacks(cli, mac_app, win_dir):
            if fallback.is_file():
                return str(fallback)
    return None


def discover_editors() -> list[EditorInfo]:
    """Every supported editor found on this machine, in preference order."""
    found: list[EditorInfo] = []
    for editor_id, label, cli_names, mac_app, win_dir in _CANDIDATES:
        cli_path = _resolve_cli(cli_names, mac_app, win_dir)
        if cli_path is not None:
            found.append(EditorInfo(id=editor_id, label=label, cli_path=cli_path))
    return found


def extension_status(editor: EditorInfo) -> tuple[bool, str | None]:
    """Whether the AMX extension is installed in *editor*, and its version.

    Returns ``(False, None)`` on any subprocess failure — status is a
    read-only convenience and must never raise.
    """
    try:
        proc = subprocess.run(
            [editor.cli_path, "--list-extensions", "--show-versions"],
            capture_output=True,
            text=True,
            timeout=_STATUS_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        return (False, None)
    if proc.returncode != 0:
        return (False, None)
    prefix = f"{EXTENSION_ID}@".lower()
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.lower().startswith(prefix):
            return (True, line[len(prefix) :].strip() or None)
    return (False, None)


def _stderr_tail(stderr: str, lines: int = 3) -> str:
    tail = [ln for ln in stderr.strip().splitlines() if ln.strip()][-lines:]
    return "\n".join(tail) or "editor CLI exited with a non-zero status"


def install(editor: EditorInfo) -> None:
    """Install (or force-update) the bundled VSIX into *editor*.

    Raises :class:`InstallerError` when the VSIX is missing from the
    wheel or the editor CLI fails.
    """
    vsix = bundled_vsix_path()
    if vsix is None:
        raise InstallerError(
            "The bundled VSIX is missing from this AMX installation.",
            "Reinstall amx-cli, or build the extension from source in vscode-extension/.",
        )
    try:
        proc = subprocess.run(
            [editor.cli_path, "--install-extension", str(vsix), "--force"],
            capture_output=True,
            text=True,
            timeout=_INSTALL_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise InstallerError(
            str(exc),
            f"Run the editor once and retry, or install the VSIX manually from {vsix}.",
        ) from exc
    if proc.returncode != 0:
        raise InstallerError(
            _stderr_tail(proc.stderr),
            f"Run the editor once and retry, or install the VSIX manually from {vsix}.",
        )


def uninstall(editor: EditorInfo) -> None:
    """Remove the AMX extension from *editor* (already-absent is a no-op)."""
    try:
        proc = subprocess.run(
            [editor.cli_path, "--uninstall-extension", EXTENSION_ID],
            capture_output=True,
            text=True,
            timeout=_INSTALL_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise InstallerError(
            str(exc),
            "Run the editor once and retry, or remove the extension from the editor's "
            "Extensions panel.",
        ) from exc
    if proc.returncode != 0 and "is not installed" not in proc.stderr.lower():
        raise InstallerError(
            _stderr_tail(proc.stderr),
            "Run the editor once and retry, or remove the extension from the editor's "
            "Extensions panel.",
        )
