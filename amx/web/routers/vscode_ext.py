"""Studio API for the editor-extension tab (Settings → VS Code).

Thin HTTP surface over :mod:`amx.vscode_ext.installer` — the exact same
engine the ``/vscode`` REPL command drives, so the CLI and Studio stay
in lockstep. No installer logic lives here; the router only translates
between HTTP and the engine.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from amx.vscode_ext import installer

router = APIRouter(prefix="/api/vscode", tags=["vscode"])


def _editor_entry(editor: installer.EditorInfo) -> dict[str, Any]:
    installed, version = installer.extension_status(editor)
    return {
        "id": editor.id,
        "label": editor.label,
        "cli_path": editor.cli_path,
        "installed": installed,
        "version": version,
    }


def _require_editor(editor_id: str) -> installer.EditorInfo:
    for editor in installer.discover_editors():
        if editor.id == editor_id:
            return editor
    raise HTTPException(
        status_code=404,
        detail=f"Unknown or undetected editor {editor_id!r}.",
    )


@router.get("/status")
def vscode_status() -> dict[str, Any]:
    """Detected editors with install state, plus the bundled VSIX version."""
    return {
        "editors": [_editor_entry(e) for e in installer.discover_editors()],
        "bundled_version": installer.bundled_vsix_version(),
    }


@router.post("/install")
def vscode_install(body: dict[str, Any]) -> dict[str, Any]:
    """Install (or force-update) the bundled extension into the chosen editor."""
    editor = _require_editor(str(body.get("editor", "")))
    try:
        installer.install(editor)
    except installer.InstallerError as exc:
        raise HTTPException(status_code=502, detail=f"{exc.message} {exc.hint}") from exc
    return {"ok": True, "editor": _editor_entry(editor)}


@router.post("/uninstall")
def vscode_uninstall(body: dict[str, Any]) -> dict[str, Any]:
    """Remove the AMX extension from the chosen editor."""
    editor = _require_editor(str(body.get("editor", "")))
    try:
        installer.uninstall(editor)
    except installer.InstallerError as exc:
        raise HTTPException(status_code=502, detail=f"{exc.message} {exc.hint}") from exc
    return {"ok": True, "editor": _editor_entry(editor)}


@router.get("/vsix")
def vscode_vsix() -> FileResponse:
    """Serve the bundled VSIX for manual 'Install from VSIX…' installs."""
    vsix = installer.bundled_vsix_path()
    if vsix is None:
        raise HTTPException(
            status_code=404,
            detail="The bundled VSIX is missing from this AMX installation.",
        )
    return FileResponse(
        vsix,
        media_type="application/octet-stream",
        filename="amx-vscode.vsix",
    )
