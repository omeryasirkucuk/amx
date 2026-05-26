"""Tests for the /mcp REPL command surface."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from amx.cli_support.commands import mcp as cli_mcp
from amx.config import AMXConfig


@pytest.fixture
def home(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    # Never actually install the SDK during CLI tests.
    monkeypatch.setattr(cli_mcp, "_ensure_sdk", lambda: True)
    return tmp_path


def test_extract_profiles_flag():
    args, profiles = cli_mcp._extract_profiles_flag(["cursor", "--profiles", "a,b"])
    assert args == ["cursor"]
    assert profiles == ["a", "b"]
    args, profiles = cli_mcp._extract_profiles_flag(["cursor"])
    assert profiles is None
    # Trailing flag with no value is ignored gracefully.
    args, profiles = cli_mcp._extract_profiles_flag(["--profiles"])
    assert profiles is None


def test_connect_writes_config(home):
    cfg = AMXConfig()
    cli_mcp.cmd_mcp(cfg, ["connect", "cursor", "--profiles", "sales,hr"])
    path = home / ".cursor" / "mcp.json"
    data = json.loads(path.read_text())
    assert data["mcpServers"]["amx"]["args"][-1] == "sales,hr"


def test_disconnect_after_connect(home):
    cfg = AMXConfig()
    cli_mcp.cmd_mcp(cfg, ["connect", "cursor"])
    cli_mcp.cmd_mcp(cfg, ["disconnect", "cursor"])
    data = json.loads((home / ".cursor" / "mcp.json").read_text())
    assert "amx" not in data["mcpServers"]


def test_unknown_ide_is_handled(home):
    cfg = AMXConfig()
    # Should not raise — prints an error and returns.
    cli_mcp.cmd_mcp(cfg, ["connect", "emacs"])
    assert not (home / ".cursor" / "mcp.json").exists()


def test_status_and_snippet_do_not_raise(home, capsys):
    cfg = AMXConfig()
    cli_mcp.cmd_mcp(cfg, ["status"])
    cli_mcp.cmd_mcp(cfg, ["snippet", "vscode"])
    out = capsys.readouterr().out
    # status reports the exposed tool count
    assert "read-only catalog tools" in out


def test_unknown_subcommand_prints_usage(home, capsys):
    cfg = AMXConfig()
    cli_mcp.cmd_mcp(cfg, ["frobnicate"])
    assert "Unknown /mcp subcommand" in capsys.readouterr().out
