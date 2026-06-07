"""Unit tests for :mod:`amx.vscode_ext.installer` (no real subprocesses)."""

from __future__ import annotations

import json
import subprocess
import zipfile
from pathlib import Path

import pytest

from amx.vscode_ext import installer
from amx.vscode_ext.installer import EditorInfo, InstallerError


def _editor(cli: str = "/usr/bin/code") -> EditorInfo:
    return EditorInfo(id="vscode", label="VS Code", cli_path=cli)


class _Proc:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


# --------------------------------------------------------------------------- #
# discover_editors
# --------------------------------------------------------------------------- #


def test_discover_editors_path_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        installer.shutil, "which", lambda cli: "/opt/bin/code" if cli == "code" else None
    )
    monkeypatch.setattr(installer, "_platform_fallbacks", lambda *a: [])
    editors = installer.discover_editors()
    assert editors == [EditorInfo(id="vscode", label="VS Code", cli_path="/opt/bin/code")]


def test_discover_editors_mac_fallback_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer.shutil, "which", lambda cli: None)
    monkeypatch.setattr(installer.sys, "platform", "darwin")
    cursor_cli = Path("/Applications/Cursor.app/Contents/Resources/app/bin/cursor")
    monkeypatch.setattr(
        installer.Path, "is_file", lambda self: str(self) == str(cursor_cli), raising=False
    )
    editors = installer.discover_editors()
    assert editors == [EditorInfo(id="cursor", label="Cursor", cli_path=str(cursor_cli))]


def test_discover_editors_none_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer.shutil, "which", lambda cli: None)
    monkeypatch.setattr(installer, "_platform_fallbacks", lambda *a: [])
    assert installer.discover_editors() == []


# --------------------------------------------------------------------------- #
# extension_status
# --------------------------------------------------------------------------- #


def test_extension_status_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    out = "ms-python.python@2026.1.0\nAMX.amx-vscode@0.18.0\n"
    monkeypatch.setattr(
        installer.subprocess, "run", lambda *a, **k: _Proc(returncode=0, stdout=out)
    )
    assert installer.extension_status(_editor()) == (True, "0.18.0")


def test_extension_status_not_installed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *a, **k: _Proc(returncode=0, stdout="ms-python.python@2026.1.0\n"),
    )
    assert installer.extension_status(_editor()) == (False, None)


def test_extension_status_subprocess_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*a: object, **k: object) -> None:
        raise subprocess.TimeoutExpired(cmd="code", timeout=30)

    monkeypatch.setattr(installer.subprocess, "run", _boom)
    assert installer.extension_status(_editor()) == (False, None)


def test_extension_status_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        installer.subprocess, "run", lambda *a, **k: _Proc(returncode=1, stderr="broken")
    )
    assert installer.extension_status(_editor()) == (False, None)


# --------------------------------------------------------------------------- #
# install / uninstall
# --------------------------------------------------------------------------- #


def test_install_argv_construction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    vsix = tmp_path / "amx-vscode.vsix"
    vsix.write_bytes(b"fake")
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: vsix)
    seen: dict[str, object] = {}

    def _run(argv: list[str], **kwargs: object) -> _Proc:
        seen["argv"] = argv
        seen["timeout"] = kwargs.get("timeout")
        return _Proc(returncode=0)

    monkeypatch.setattr(installer.subprocess, "run", _run)
    installer.install(_editor("/usr/bin/code"))
    assert seen["argv"] == ["/usr/bin/code", "--install-extension", str(vsix), "--force"]
    assert seen["timeout"] == 120


def test_install_failure_raises_installer_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    vsix = tmp_path / "amx-vscode.vsix"
    vsix.write_bytes(b"fake")
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: vsix)
    stderr = "line1\nline2\nCorrupt VSIX archive\n"
    monkeypatch.setattr(
        installer.subprocess, "run", lambda *a, **k: _Proc(returncode=1, stderr=stderr)
    )
    with pytest.raises(InstallerError) as excinfo:
        installer.install(_editor())
    assert "Corrupt VSIX archive" in excinfo.value.message
    assert str(vsix) in excinfo.value.hint


def test_install_missing_vsix_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: None)
    with pytest.raises(InstallerError):
        installer.install(_editor())


def test_uninstall_not_installed_is_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        installer.subprocess,
        "run",
        lambda *a, **k: _Proc(returncode=1, stderr="Extension 'amx.amx-vscode' is not installed."),
    )
    installer.uninstall(_editor())  # must not raise


def test_uninstall_real_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        installer.subprocess, "run", lambda *a, **k: _Proc(returncode=1, stderr="locked")
    )
    with pytest.raises(InstallerError):
        installer.uninstall(_editor())


# --------------------------------------------------------------------------- #
# bundled VSIX introspection
# --------------------------------------------------------------------------- #


def _make_vsix(path: Path, manifest: object) -> None:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("extension/package.json", json.dumps(manifest))


def test_bundled_vsix_version_reads_manifest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    vsix = tmp_path / "amx-vscode.vsix"
    _make_vsix(vsix, {"name": "amx-vscode", "version": "0.18.0"})
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: vsix)
    assert installer.bundled_vsix_version() == "0.18.0"


def test_bundled_vsix_version_tolerates_garbage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bogus = tmp_path / "amx-vscode.vsix"
    bogus.write_bytes(b"not a zip at all")
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: bogus)
    assert installer.bundled_vsix_version() is None


def test_bundled_vsix_version_none_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(installer, "bundled_vsix_path", lambda: None)
    assert installer.bundled_vsix_version() is None
