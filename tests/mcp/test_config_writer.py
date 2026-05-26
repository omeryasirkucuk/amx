"""Unit tests for the IDE-config read/write engine."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from amx.mcp import config_writer, ide_targets


@pytest.fixture
def home(monkeypatch, tmp_path):
    """Redirect every IDE config path under a temp home directory."""
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    return tmp_path


def _cursor():
    return ide_targets.get_target("cursor")


def test_spawn_command_uses_absolute_interpreter():
    cmd = config_writer.spawn_command(None)
    assert cmd["command"] == __import__("os").path.abspath(sys.executable)
    assert cmd["args"] == ["-m", "amx.mcp"]
    cmd2 = config_writer.spawn_command(["sales", "hr"])
    assert cmd2["args"] == ["-m", "amx.mcp", "--profiles", "sales,hr"]


def test_connect_creates_entry(home):
    target = _cursor()
    result = config_writer.connect(target, None)
    data = json.loads(target.config_path().read_text())
    entry = data["mcpServers"]["amx"]
    assert entry["command"] == config_writer.spawn_command()["command"]
    assert entry["args"] == ["-m", "amx.mcp"]
    assert result.label == "Cursor"
    assert result.post_connect_steps


def test_connect_is_idempotent_and_preserves_others(home):
    target = _cursor()
    # Pre-seed an unrelated MCP server.
    path = target.config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"mcpServers": {"other": {"command": "x"}}}))

    config_writer.connect(target, ["a"])
    config_writer.connect(target, ["a", "b"])  # second call overwrites in place

    data = json.loads(path.read_text())
    # Unrelated server preserved; exactly one amx entry; latest scope wins.
    assert data["mcpServers"]["other"] == {"command": "x"}
    assert data["mcpServers"]["amx"]["args"][-1] == "a,b"
    assert list(data["mcpServers"]).count("amx") == 1


def test_vscode_uses_servers_key_with_stdio_type(home):
    target = ide_targets.get_target("vscode")
    config_writer.connect(target, None)
    data = json.loads(target.config_path().read_text())
    assert "servers" in data and "mcpServers" not in data
    assert data["servers"]["amx"]["type"] == "stdio"


def test_disconnect_removes_only_amx(home):
    target = _cursor()
    path = target.config_path()
    config_writer.connect(target, None)
    path.write_text(
        json.dumps(
            {"mcpServers": {"amx": json.loads(path.read_text())["mcpServers"]["amx"], "keep": {}}}
        )
    )
    assert config_writer.disconnect(target) is True
    data = json.loads(path.read_text())
    assert "amx" not in data["mcpServers"]
    assert "keep" in data["mcpServers"]
    # Second disconnect is a no-op.
    assert config_writer.disconnect(target) is False


def test_status_not_connected(home):
    st = config_writer.status(_cursor())
    assert st.connected is False
    assert st.drifted is False
    assert st.error is None


def test_status_connected_reports_profiles(home):
    target = _cursor()
    config_writer.connect(target, ["sales", "hr"])
    st = config_writer.status(target)
    assert st.connected is True
    assert st.drifted is False
    assert st.profiles == ["sales", "hr"]


def test_status_detects_interpreter_drift(home):
    target = _cursor()
    path = target.config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"mcpServers": {"amx": {"command": "/some/old/python", "args": ["-m", "amx.mcp"]}}}
        )
    )
    st = config_writer.status(target)
    assert st.connected is True
    assert st.drifted is True


def test_unparseable_config_surfaces_error(home):
    target = _cursor()
    path = target.config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ not json")
    st = config_writer.status(target)
    assert st.connected is False
    assert st.error is not None
    with pytest.raises(ValueError):
        config_writer.connect(target, None)


def test_snippet_renders_for_each_ide(home):
    for target in ide_targets.all_targets():
        text = config_writer.snippet(target, None)
        parsed = json.loads(text)
        assert target.config_key in parsed
        assert "amx" in parsed[target.config_key]
