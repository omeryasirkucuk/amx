"""/add-db-profile tests the connection after saving.

It used to print "Profile saved and activated" without ever opening a
connection, so a wrong password / unreachable host only surfaced later
inside /run. The profile is still saved (no input lost); we now also
report whether it actually connects and how to fix it.
"""

from __future__ import annotations

import types

import pytest

import amx.cli_support.commands.db as dbmod
import amx.db.connector as connector_mod


def _patch_console(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[str]]:
    msgs: dict[str, list[str]] = {"success": [], "warn": [], "info": []}
    for level in msgs:
        monkeypatch.setattr(dbmod, level, lambda m, _l=level: msgs[_l].append(m))
    return msgs


def _patch_connector(monkeypatch: pytest.MonkeyPatch, ok: bool, message: str = "") -> None:
    result = types.SimpleNamespace(ok=ok, message=message)
    monkeypatch.setattr(
        connector_mod,
        "DatabaseConnector",
        lambda db: types.SimpleNamespace(test_connection_result=lambda: result),
    )


def test_verify_success_reports_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    msgs = _patch_console(monkeypatch)
    _patch_connector(monkeypatch, ok=True)
    assert dbmod._verify_saved_db_profile("prod", object()) is True
    assert msgs["success"] and not msgs["warn"]


def test_verify_failure_surfaces_actionable_message(monkeypatch: pytest.MonkeyPatch) -> None:
    msgs = _patch_console(monkeypatch)
    _patch_connector(monkeypatch, ok=False, message="PostgreSQL refused the credentials.")
    assert dbmod._verify_saved_db_profile("prod", object()) is False
    assert msgs["warn"]
    assert any("refused the credentials" in i for i in msgs["info"])
    assert any("/edit-db-profile prod" in i for i in msgs["info"])
