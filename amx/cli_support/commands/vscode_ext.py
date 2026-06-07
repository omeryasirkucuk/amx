"""Slash-command handler for /vscode.

Entry point: cmd_vscode(cfg, rest) — dispatched from session.py.

``/vscode`` installs the AMX editor extension — the VSIX vendored
inside the amx-cli wheel — into a VS Code-family editor (VS Code,
VS Code Insiders, Cursor, Windsurf, VSCodium). Bare ``/vscode`` runs a
wizard (pick a detected editor → install → status); subcommands are an
optional power-user shortcut.

Subcommands
-----------
(none)               interactive wizard
install [editor]     install/update the bundled extension in an editor
status               show detected editors and installed versions
uninstall [editor]   remove the extension from an editor

The actual discovery and subprocess work lives in
:mod:`amx.vscode_ext.installer`; this module is only the CLI surface.
Studio's VS Code tab drives the same engine.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from amx.vscode_ext import installer
from amx.vscode_ext.installer import EditorInfo, InstallerError

if TYPE_CHECKING:
    from amx.config import AMXConfig

_EDITOR_IDS = ("vscode", "vscode-insiders", "cursor", "windsurf", "vscodium")

_USAGE = (
    "Usage:\n"
    "  /vscode                      -- interactive wizard (recommended)\n"
    "  /vscode install [editor]     -- install/update the bundled extension\n"
    "  /vscode status               -- show detected editors and versions\n"
    "  /vscode uninstall [editor]   -- remove the extension from an editor\n"
    "\n"
    "Supported editors: " + ", ".join(_EDITOR_IDS)
)


# --------------------------------------------------------------------------- #
# Pickers (wizard primitives)
# --------------------------------------------------------------------------- #


def _pick_editor(editors: list[EditorInfo]) -> EditorInfo | None:
    """Single-pick prompt over detected editors; None on cancel."""
    from amx.cli_support.review_picker import pick_rows
    from amx.utils.console import info

    if not editors:
        return None
    if len(editors) == 1:
        return editors[0]
    info("Pick an editor:")
    picked = pick_rows([f"{e.label} ({e.cli_path})" for e in editors])
    if not picked:
        return None
    return editors[picked[0]]


def _resolve_editor_arg(name: str) -> EditorInfo | None:
    from amx.utils.console import error

    editors = installer.discover_editors()
    for editor in editors:
        if editor.id == name.lower():
            return editor
    if name.lower() in _EDITOR_IDS:
        error(f"{name!r} is supported but was not detected on this machine.")
    else:
        error(f"Unknown editor {name!r}. Supported: " + ", ".join(_EDITOR_IDS))
    return None


# --------------------------------------------------------------------------- #
# Operations
# --------------------------------------------------------------------------- #


def _print_manual_steps() -> None:
    """No editor CLI found — point at the VSIX and the manual install path."""
    from amx.utils.console import info, warn

    warn("No supported editor CLI was detected on this machine.")
    vsix = installer.bundled_vsix_path()
    if vsix is None:
        warn("The bundled VSIX is also missing — reinstall amx-cli to restore it.")
        return
    info(f"The AMX extension VSIX is bundled at: {vsix}")
    info("Install it manually:")
    info("  1. Open your editor's Extensions view.")
    info("  2. Open the '…' menu → 'Install from VSIX…'.")
    info("  3. Pick the file above and reload the editor.")


def _do_install(editor: EditorInfo) -> None:
    from amx.utils.console import error, info, success

    info(f"Installing the AMX extension into {editor.label}…")
    try:
        installer.install(editor)
    except InstallerError as exc:
        error(f"Install failed: {exc.message}")
        info(exc.hint)
        return
    installed, version = installer.extension_status(editor)
    if installed and version:
        success(f"AMX extension {version} installed in {editor.label}.")
    else:
        success(f"Install command completed for {editor.label}.")
    info(f"Reload {editor.label} windows to activate the extension.")


def _do_uninstall(editor: EditorInfo) -> None:
    from amx.utils.console import error, info, success

    try:
        installer.uninstall(editor)
    except InstallerError as exc:
        error(f"Uninstall failed: {exc.message}")
        info(exc.hint)
        return
    success(f"AMX extension removed from {editor.label}.")
    info(f"Reload {editor.label} windows to apply.")


def _do_status() -> None:
    from amx.utils.console import info

    bundled = installer.bundled_vsix_version()
    info(f"Bundled extension version: {bundled or 'unknown'}")
    editors = installer.discover_editors()
    if not editors:
        _print_manual_steps()
        return
    info("")
    for editor in editors:
        installed, version = installer.extension_status(editor)
        shown = version or "—"
        flag = ""
        if installed and bundled and version != bundled:
            flag = f"  ⚠ out of date (bundled: {bundled}) — run /vscode install {editor.id}"
        mark = "✓" if installed else "—"
        info(f"  {editor.label:<18} {mark} {shown:<10} {editor.cli_path}{flag}")
    info("")
    info("Run /vscode to install or update the extension.")


# --------------------------------------------------------------------------- #
# Wizard
# --------------------------------------------------------------------------- #


def _run_wizard(cfg: AMXConfig) -> None:
    from amx.utils.console import info

    editors = installer.discover_editors()
    if not editors:
        _print_manual_steps()
        return
    editor = _pick_editor(editors)
    if editor is None:
        info("Cancelled.")
        return
    _do_install(editor)


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def cmd_vscode(cfg: AMXConfig, rest: list[str]) -> None:
    """Dispatch /vscode — wizard when bare, subcommands for power users."""
    from amx.utils.console import info

    if not rest:
        _run_wizard(cfg)
        return

    sub = rest[0].lower()
    args = rest[1:]

    if sub == "status":
        _do_status()
    elif sub == "install":
        editor = _editor_from_args(args)
        if editor is not None:
            _do_install(editor)
    elif sub == "uninstall":
        editor = _editor_from_args(args)
        if editor is not None:
            _do_uninstall(editor)
    else:
        info(f"Unknown /vscode subcommand: {sub!r}\n\n{_USAGE}")


def _editor_from_args(args: list[str]) -> EditorInfo | None:
    """Resolve the editor for install/uninstall — by id, or via the picker.

    Returns ``None`` after printing the appropriate message (unknown id,
    nothing detected, or user cancel).
    """
    from amx.utils.console import info

    if args:
        return _resolve_editor_arg(args[0])
    editors = installer.discover_editors()
    if not editors:
        _print_manual_steps()
        return None
    editor = _pick_editor(editors)
    if editor is None:
        info("Cancelled.")
    return editor
