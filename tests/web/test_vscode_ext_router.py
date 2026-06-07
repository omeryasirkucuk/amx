"""Router tests for ``/api/vscode`` with a stubbed installer engine."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.vscode_ext import installer
from amx.vscode_ext.installer import EditorInfo, InstallerError

_EDITORS = [
    EditorInfo(id="vscode", label="VS Code", cli_path="/usr/bin/code"),
    EditorInfo(id="cursor", label="Cursor", cli_path="/usr/bin/cursor"),
]


@pytest.fixture()
def stub_installer(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(installer, "discover_editors", lambda: list(_EDITORS))
    monkeypatch.setattr(
        installer,
        "extension_status",
        lambda editor: (True, "0.18.0") if editor.id == "vscode" else (False, None),
    )
    monkeypatch.setattr(installer, "bundled_vsix_version", lambda: "0.19.0")
    monkeypatch.setattr(installer, "install", lambda editor: None)
    monkeypatch.setattr(installer, "uninstall", lambda editor: None)
    return monkeypatch


def test_status_shape(client, auth_headers, stub_installer) -> None:
    resp = client.get("/api/vscode/status", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["bundled_version"] == "0.19.0"
    assert body["editors"] == [
        {
            "id": "vscode",
            "label": "VS Code",
            "cli_path": "/usr/bin/code",
            "installed": True,
            "version": "0.18.0",
        },
        {
            "id": "cursor",
            "label": "Cursor",
            "cli_path": "/usr/bin/cursor",
            "installed": False,
            "version": None,
        },
    ]


def test_install_happy_path(client, auth_headers, stub_installer) -> None:
    resp = client.post("/api/vscode/install", json={"editor": "vscode"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["editor"]["id"] == "vscode"
    assert body["editor"]["installed"] is True
    assert body["editor"]["version"] == "0.18.0"


def test_install_unknown_editor_404(client, auth_headers, stub_installer) -> None:
    resp = client.post("/api/vscode/install", json={"editor": "emacs"}, headers=auth_headers)
    assert resp.status_code == 404
    assert "emacs" in resp.json()["detail"]


def test_install_failure_502_with_hint(
    client, auth_headers, stub_installer, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fail(editor: EditorInfo) -> None:
        raise InstallerError("editor CLI crashed", "Run the editor once and retry.")

    monkeypatch.setattr(installer, "install", _fail)
    resp = client.post("/api/vscode/install", json={"editor": "vscode"}, headers=auth_headers)
    assert resp.status_code == 502
    detail = resp.json()["detail"]
    assert "editor CLI crashed" in detail
    assert "Run the editor once and retry." in detail


def test_uninstall_happy_path(client, auth_headers, stub_installer) -> None:
    resp = client.post("/api/vscode/uninstall", json={"editor": "cursor"}, headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["editor"]["id"] == "cursor"


def test_vsix_download(
    client, auth_headers, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    vsix = tmp_path / "amx-vscode.vsix"
    vsix.write_bytes(b"vsix-bytes")
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: vsix)
    resp = client.get("/api/vscode/vsix", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.content == b"vsix-bytes"
    assert resp.headers["content-type"] == "application/octet-stream"
    assert "amx-vscode.vsix" in resp.headers["content-disposition"]


def test_vsix_404_when_missing(client, auth_headers, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: None)
    resp = client.get("/api/vscode/vsix", headers=auth_headers)
    assert resp.status_code == 404


def test_status_requires_auth(client, stub_installer) -> None:
    resp = client.get("/api/vscode/status")
    assert resp.status_code == 401
