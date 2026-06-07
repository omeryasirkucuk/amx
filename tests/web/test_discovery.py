"""Discovery file lifecycle (``amx/web/discovery.py``).

The file lets local tooling find a running Studio server. The tests
pin the atomic-write semantics, the tolerant reader (every broken
state maps to ``None``), and the pid-guarded clear that keeps a dying
server from deleting a newer server's record.
"""

from __future__ import annotations

import json
import os

import pytest

from amx.web import discovery


@pytest.fixture(autouse=True)
def isolated_config_dir(tmp_path, monkeypatch):
    """Point AMX_CONFIG_DIR at a temp dir so tests never touch ~/.amx."""
    monkeypatch.setenv("AMX_CONFIG_DIR", str(tmp_path))
    return tmp_path


def test_write_then_read_roundtrip() -> None:
    written = discovery.write_discovery(47821, "tok-abc", owner="vscode")
    record = discovery.read_discovery()
    assert record == written
    assert record.port == 47821
    assert record.token == "tok-abc"
    assert record.owner == "vscode"
    assert record.pid == os.getpid()
    assert record.started_at  # ISO timestamp, non-empty


def test_write_creates_config_dir(isolated_config_dir, monkeypatch) -> None:
    nested = isolated_config_dir / "deeper" / "amx"
    monkeypatch.setenv("AMX_CONFIG_DIR", str(nested))
    discovery.write_discovery(1234, "tok")
    assert discovery.read_discovery() is not None


def test_write_is_owner_only_where_supported(isolated_config_dir) -> None:
    discovery.write_discovery(1234, "tok")
    mode = discovery.discovery_path().stat().st_mode & 0o777
    if os.name == "posix":
        assert mode == 0o600


def test_read_missing_file_returns_none() -> None:
    assert discovery.read_discovery() is None


def test_read_malformed_json_returns_none(isolated_config_dir) -> None:
    discovery.discovery_path().write_text("{not json", encoding="utf-8")
    assert discovery.read_discovery() is None


def test_read_non_dict_returns_none(isolated_config_dir) -> None:
    discovery.discovery_path().write_text("[1, 2]", encoding="utf-8")
    assert discovery.read_discovery() is None


def test_read_missing_required_field_returns_none(isolated_config_dir) -> None:
    discovery.discovery_path().write_text(
        json.dumps({"port": 1, "pid": 2}), encoding="utf-8"
    )
    assert discovery.read_discovery() is None


def test_clear_removes_file() -> None:
    discovery.write_discovery(1234, "tok")
    discovery.clear_discovery()
    assert discovery.read_discovery() is None


def test_clear_is_noop_when_missing() -> None:
    discovery.clear_discovery()  # must not raise


def test_clear_with_matching_pid_removes() -> None:
    discovery.write_discovery(1234, "tok")
    discovery.clear_discovery(pid=os.getpid())
    assert discovery.read_discovery() is None


def test_clear_with_stale_pid_keeps_newer_record() -> None:
    """A dying server must not delete the record a newer server wrote."""
    discovery.write_discovery(1234, "tok")
    discovery.clear_discovery(pid=os.getpid() + 1)
    record = discovery.read_discovery()
    assert record is not None
    assert record.port == 1234


def test_no_temp_file_left_behind(isolated_config_dir) -> None:
    discovery.write_discovery(1234, "tok")
    temp_leftovers = [
        p.name
        for p in isolated_config_dir.iterdir()
        if p.name.startswith(".studio.json.")
    ]
    assert temp_leftovers == []
    assert discovery.discovery_path().exists()
