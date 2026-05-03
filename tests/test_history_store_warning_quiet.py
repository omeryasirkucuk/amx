"""Regression: shared-history bootstrap warning must be quiet.

User report (2026-05-03 against 0.12.4): every slash command in the
interactive session re-entered ``init_history_store`` and re-emitted
the SQLAlchemy ``ServerOperationError`` as a 70-line wall of CREATE
TABLE DDL because the underlying exception's ``str(exc)`` carried the
full SQL. The terminal became unusable.

These tests pin the 0.12.5 contract:

1. ``_short_error`` returns just the first non-empty line, with the
   verbose SQLAlchemy "(Background on this error at: …)" suffix
   trimmed.
2. ``_warn_bootstrap_failure_once`` emits a single WARNING per
   (profile, schema) per process. Subsequent calls with the same
   key go to DEBUG only.
"""

from __future__ import annotations

import logging

import pytest

from amx.storage import factory


def test_short_error_strips_sql_dump_and_background_link() -> None:
    """The user's actual error string carried 70 lines of CREATE TABLE
    DDL between the message and the SQLAlchemy ``Background on this
    error`` footer. The short form must keep ONLY the first line and
    drop the footer."""
    raw = (
        "(databricks.sql.exc.ServerOperationError) [SCHEMA_NOT_FOUND] "
        "The schema `sap.amx` cannot be found.\n"
        "[SQL: \n"
        "CREATE TABLE `AMX`.analysis_runs (\n"
        "    id STRING NOT NULL COMMENT 'UUID v4 …',\n"
        "    started_at TIMESTAMP_NTZ NOT NULL COMMENT '…',\n"
        "    -- imagine 70 more lines —\n"
        ") USING DELTA\n"
        "]\n"
        "(Background on this error at: https://sqlalche.me/e/20/4xp6)"
    )
    short = factory._short_error(RuntimeError(raw))
    assert "CREATE TABLE" not in short
    assert "Background on this error" not in short
    assert "SCHEMA_NOT_FOUND" in short
    assert "\n" not in short
    # Sanity: under 200 chars vs the 4 KB original.
    assert len(short) < 200


def test_warn_bootstrap_failure_fires_once_per_key(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The 70-line warning used to fire on every slash command in a
    session. After 0.12.5 the second call for the same (profile,
    schema) key is silent at WARNING level — only the first goes
    through."""
    # Reset the module-level cache so tests don't leak into each other.
    monkeypatch.setattr(factory, "_BOOTSTRAP_FAILURE_CACHE", set())
    key = ("dbr-test", "AMX")
    exc = RuntimeError("boom")

    with caplog.at_level(logging.WARNING, logger="amx.storage.factory"):
        factory._warn_bootstrap_failure_once(key, "boom", exc)
        factory._warn_bootstrap_failure_once(key, "boom", exc)
        factory._warn_bootstrap_failure_once(key, "boom", exc)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1, f"expected 1 WARNING, got {len(warnings)}: {warnings}"
    assert "Shared run-history disabled this session" in warnings[0].message
    assert "/history-store disable" in warnings[0].message


def test_warn_bootstrap_failure_distinguishes_keys(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Different (profile, schema) targets each get their own one-shot
    warning. A user with two shared-history profiles configured must
    see one warning per failing target."""
    monkeypatch.setattr(factory, "_BOOTSTRAP_FAILURE_CACHE", set())
    exc = RuntimeError("boom")

    with caplog.at_level(logging.WARNING, logger="amx.storage.factory"):
        factory._warn_bootstrap_failure_once(("p1", "AMX"), "boom", exc)
        factory._warn_bootstrap_failure_once(("p2", "AMX"), "boom", exc)
        factory._warn_bootstrap_failure_once(("p1", "AMX"), "boom", exc)  # repeat → silent

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2, f"expected 2 distinct WARNING, got {len(warnings)}"
