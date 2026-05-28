"""Startup recovery when ``config.yml`` cannot be parsed.

A corrupt config.yml used to hard-exit before the REPL even started,
pointing the user at ``/restore-config`` — a command only reachable
inside the REPL that just refused to start, with no hint where the
backups live. ``_recover_corrupt_config`` surfaces the backup directory
and offers an inline restore (when interactive), then retries the load.
"""

from __future__ import annotations

import pytest

import amx.cli as cli
import amx.config as config_mod


def test_recover_no_backups_returns_none(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    monkeypatch.setattr(config_mod, "list_config_backups", lambda p=None: [])
    out = cli._recover_corrupt_config(str(tmp_path / "config.yml"), ValueError("bad yaml"))
    assert out is None


def test_recover_non_interactive_returns_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    # With backups present but no TTY, we must NOT prompt — just surface
    # the path and bail so the caller exits with the actionable message.
    backup = tmp_path / "config.yml.bak.1"
    monkeypatch.setattr(config_mod, "list_config_backups", lambda p=None: [backup])
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: False, raising=False)
    out = cli._recover_corrupt_config(str(tmp_path / "config.yml"), ValueError("bad yaml"))
    assert out is None


def test_recover_restores_and_reloads(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> None:
    backup = tmp_path / "config.yml.bak.1"
    monkeypatch.setattr(config_mod, "list_config_backups", lambda p=None: [backup])
    monkeypatch.setattr(cli.sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(cli, "confirm", lambda *a, **k: True)
    monkeypatch.setattr(cli, "ask_choice", lambda *a, **k: backup.name)
    restored: dict[str, object] = {}
    monkeypatch.setattr(
        config_mod,
        "restore_config_from_backup",
        lambda b, p=None: restored.setdefault("backup", b) or b,
    )
    sentinel = object()
    monkeypatch.setattr(cli.AMXConfig, "load", classmethod(lambda cls, path=None: sentinel))

    out = cli._recover_corrupt_config(str(tmp_path / "config.yml"), ValueError("bad yaml"))
    assert out is sentinel
    assert restored["backup"] == backup
