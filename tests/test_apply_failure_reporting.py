"""CLI apply surfaces classified write failures + their remediation.

The classifier computes an actionable title + suggested action (e.g. the
missing `GRANT ALTER`) for each failed row when apply captures outcomes.
The CLI used to discard all of that and record only the raw driver
exception, so a privilege/ALTER failure printed an opaque stack message.
"""

from __future__ import annotations

import types

import pytest

import amx.cli_support.commands.run as run


def _outcome(**kw: object) -> types.SimpleNamespace:
    base = {
        "status": "failed",
        "schema": "sales",
        "table": "orders",
        "column": "id",
        "error_title": "",
        "error_action": "",
        "error_kind": "",
    }
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_reports_failed_rows_with_title_and_deduped_action(monkeypatch: pytest.MonkeyPatch) -> None:
    errs: list[str] = []
    infos: list[str] = []
    warns: list[str] = []
    monkeypatch.setattr(run, "error", lambda m: errs.append(m))
    monkeypatch.setattr(run, "info", lambda m: infos.append(m))
    monkeypatch.setattr(run, "warn", lambda m: warns.append(m))

    grant = "Run GRANT ALTER ON sales.orders TO your_role; as the owner."
    outcomes = [
        _outcome(error_title="Missing ALTER privilege", error_action=grant),
        _outcome(column="status", error_title="Missing ALTER privilege", error_action=grant),
    ]
    run._report_apply_failures(outcomes)

    assert warns and "2 comment(s) could not be written" in warns[0]
    assert sum("Missing ALTER privilege" in e for e in errs) == 2  # one per row
    assert sum(grant in i for i in infos) == 1  # distinct action shown once


def test_no_failures_reports_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    for fn in ("error", "info", "warn"):
        monkeypatch.setattr(run, fn, lambda m, _c=calls: _c.append(m))
    run._report_apply_failures([_outcome(status="applied")])
    assert calls == []
