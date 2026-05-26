"""Unit tests for the per-IDE registry and cross-platform paths."""

from __future__ import annotations

from pathlib import Path

import pytest

from amx.mcp import ide_targets


def test_lookup_is_case_insensitive():
    assert ide_targets.get_target("Cursor").key == "cursor"
    assert ide_targets.get_target("  VSCODE ").key == "vscode"
    assert ide_targets.get_target("unknown") is None


def test_schema_shapes():
    cursor = ide_targets.get_target("cursor")
    claude = ide_targets.get_target("claude")
    vscode = ide_targets.get_target("vscode")
    # Cursor / Claude use mcpServers and no per-server type field.
    assert cursor.config_key == "mcpServers" and cursor.entry_type is None
    assert claude.config_key == "mcpServers" and claude.entry_type is None
    # VS Code uses servers + an explicit stdio type.
    assert vscode.config_key == "servers" and vscode.entry_type == "stdio"


def test_all_paths_are_absolute():
    for target in ide_targets.all_targets():
        assert target.config_path().is_absolute()
        assert target.post_connect_steps  # non-empty guidance


def test_cursor_path_under_home(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    assert ide_targets.get_target("cursor").config_path() == tmp_path / ".cursor" / "mcp.json"


@pytest.mark.parametrize(
    "platform,env,expected_tail",
    [
        ("darwin", {}, ("Library", "Application Support")),
        ("linux", {"XDG_CONFIG_HOME": None}, (".config",)),
        ("win32", {"APPDATA": None}, ("AppData", "Roaming")),
    ],
)
def test_app_data_dir_cross_platform(monkeypatch, tmp_path, platform, env, expected_tail):
    monkeypatch.setattr(ide_targets.sys, "platform", platform)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    for key, val in env.items():
        if val is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, val)
    base = ide_targets._app_data_dir()
    assert base.parts[-len(expected_tail) :] == expected_tail


def test_app_data_dir_respects_appdata_env(monkeypatch, tmp_path):
    monkeypatch.setattr(ide_targets.sys, "platform", "win32")
    monkeypatch.setenv("APPDATA", str(tmp_path / "Roaming"))
    assert ide_targets._app_data_dir() == tmp_path / "Roaming"


def test_app_data_dir_respects_xdg(monkeypatch, tmp_path):
    monkeypatch.setattr(ide_targets.sys, "platform", "linux")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    assert ide_targets._app_data_dir() == tmp_path / "xdg"
