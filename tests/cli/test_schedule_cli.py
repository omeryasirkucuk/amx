"""End-to-end CLI tests for ``amx schedule`` and ``amx scheduler``.

Exercises the Click command tree against a tmp AMX config dir so the
real history store is initialised and the commands run their full
production path (minus the per-run executor, which the worker
scaffold stubs).
"""

from __future__ import annotations

import time
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from amx.cli_support.commands.schedule import register_schedule_commands
from amx.cli_support.commands.scheduler import register_scheduler_commands
from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def cli(tmp_path: Path):
    """Build a Click root group with just the two scheduler groups and
    point the module-level history_store singleton at a tmp DB."""
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    _store_module._store = s

    @click.group()
    def root() -> None: ...

    register_schedule_commands(root)
    register_scheduler_commands(root)
    return root, s


def test_schedule_add_creates_row(cli) -> None:
    root, store = cli
    runner = CliRunner()
    result = runner.invoke(
        root,
        [
            "schedule",
            "add",
            "--name",
            "Quarterly",
            "--at",
            "2030-01-15 14:00",
            "--tz",
            "Europe/Istanbul",
            "--db",
            "prod_sf",
            "--scope",
            "schema:public",
            "--llm",
            "claude",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    assert "Schedule #" in result.output
    assert "Heads-up" in result.output
    rows = store.list_scheduled_runs()
    assert len(rows) == 1
    assert rows[0]["name"] == "Quarterly"


def test_schedule_list_default_filters_to_active(cli) -> None:
    root, store = cli
    store.create_scheduled_run(
        name="active",
        fire_at_utc=time.time() + 3600,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    sid_done = store.create_scheduled_run(
        name="done",
        fire_at_utc=time.time() + 3600,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    store.set_scheduled_run_status(sid_done, "running")
    store.set_scheduled_run_status(sid_done, "completed")

    runner = CliRunner()
    result = runner.invoke(root, ["schedule", "list"])
    assert result.exit_code == 0
    assert "active" in result.output
    assert "done" not in result.output

    result_all = runner.invoke(root, ["schedule", "list", "--all"])
    assert result_all.exit_code == 0
    assert "active" in result_all.output
    assert "done" in result_all.output


def test_schedule_pause_and_resume(cli) -> None:
    root, store = cli
    sid = store.create_scheduled_run(
        name="x",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    runner = CliRunner()
    assert runner.invoke(root, ["schedule", "pause", str(sid)]).exit_code == 0
    assert store.get_scheduled_run(sid)["status"] == "paused"
    assert runner.invoke(root, ["schedule", "resume", str(sid)]).exit_code == 0
    assert store.get_scheduled_run(sid)["status"] == "pending"


def test_schedule_rm_with_yes_flag(cli) -> None:
    root, store = cli
    sid = store.create_scheduled_run(
        name="gone",
        fire_at_utc=time.time() + 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    runner = CliRunner()
    result = runner.invoke(root, ["schedule", "rm", str(sid), "-y"])
    assert result.exit_code == 0
    assert store.get_scheduled_run(sid) is None


def test_schedule_run_now_fires_synchronously(cli) -> None:
    root, store = cli
    sid = store.create_scheduled_run(
        name="now",
        fire_at_utc=time.time() + 3600,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    runner = CliRunner()
    result = runner.invoke(
        root,
        ["schedule", "run-now", str(sid), "--foreground"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # In foreground mode the worker has finished by the time we get
    # back; the schedule must be in a terminal state.
    final = store.get_scheduled_run(sid)
    assert final["status"] in ("completed", "failed")
    assert final["triggered_run_id"] is not None


def test_scheduler_tick_json_output(cli) -> None:
    root, store = cli
    runner = CliRunner()
    result = runner.invoke(root, ["scheduler", "tick"])
    assert result.exit_code == 0
    import json as _j

    payload = _j.loads(result.output)
    assert "fired" in payload
    assert "missed_for_review" in payload


def test_scheduler_status_runs(cli) -> None:
    root, store = cli
    runner = CliRunner()
    result = runner.invoke(root, ["scheduler", "status"])
    assert result.exit_code == 0
    assert "Scheduler status" in result.output
    assert "Daemon" in result.output


def test_schedule_add_rejects_bad_timezone(cli) -> None:
    root, _ = cli
    runner = CliRunner()
    result = runner.invoke(
        root,
        [
            "schedule",
            "add",
            "--name",
            "bad-tz",
            "--at",
            "2030-01-15 14:00",
            "--tz",
            "Mars/OlympusMons",
            "--db",
            "p",
            "--scope",
            "all",
            "--llm",
            "l",
        ],
    )
    assert result.exit_code != 0
    assert "timezone" in result.output.lower()


def test_schedule_add_rejects_bad_scope(cli) -> None:
    root, _ = cli
    runner = CliRunner()
    result = runner.invoke(
        root,
        [
            "schedule",
            "add",
            "--name",
            "bad-scope",
            "--at",
            "2030-01-15 14:00",
            "--tz",
            "UTC",
            "--db",
            "p",
            "--scope",
            "weird-format",
            "--llm",
            "l",
        ],
    )
    assert result.exit_code != 0


# Reset the module-level store after each test to avoid leakage.
@pytest.fixture(autouse=True)
def _reset_singleton():
    yield
    _store_module._store = None
