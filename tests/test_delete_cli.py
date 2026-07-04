"""CLI tests for ``/history delete`` and ``/analyze review-clear``.

The root Click group blocks direct subcommand invocation (REPL-only), so
these tests invoke the registered command objects directly — the same
functions the interactive session dispatches to — with the history store
patched to a temp instance.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest
from click.testing import CliRunner

import amx.cli as cli
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def _seed(s: SQLiteHistoryStore, *, schema: str = "s", table: str = "t") -> tuple[int, list[int]]:
    rid = s.create_run(
        command="analyze.run",
        mode="x",
        db_backend="sqlite",
        db_profile="p",
        llm_provider="lp",
        llm_model="m",
        scope={schema: [table]},
    )
    ids = s.save_run_results(
        rid,
        [
            {
                "schema": schema,
                "table": table,
                "column": None,
                "asset_kind": "table",
                "source": "llm",
                "confidence": "high",
                "alternatives": [{"text": "a"}],
            }
        ],
    )
    return rid, ids


def _history_delete_cmd():
    return cli.main.commands["history"].commands["delete"]


def _review_clear_cmd():
    return cli.main.commands["analyze"].commands["review-clear"]


def test_history_delete_by_id(store: SQLiteHistoryStore) -> None:
    rid, _ = _seed(store)
    runner = CliRunner()
    with mock.patch("amx.cli_support.commands.history.history_store", return_value=store):
        res = runner.invoke(_history_delete_cmd(), [str(rid), "--yes"])
    assert res.exit_code == 0, res.output
    assert store.get_run(rid) is None


def test_history_delete_requires_confirmation(store: SQLiteHistoryStore) -> None:
    rid, _ = _seed(store)
    runner = CliRunner()
    # ``confirm`` is a prompt_toolkit prompt (no piped-stdin support under
    # CliRunner); patch it to decline so we exercise the "user said no →
    # nothing deleted" branch directly.
    with (
        mock.patch("amx.cli_support.commands.history.history_store", return_value=store),
        mock.patch("amx.cli_support.commands.history.confirm", return_value=False),
    ):
        res = runner.invoke(_history_delete_cmd(), [str(rid)])
    assert res.exit_code == 0, res.output
    assert "cancel" in res.output.lower()
    assert store.get_run(rid) is not None


def test_history_delete_missing_id_errors(store: SQLiteHistoryStore) -> None:
    runner = CliRunner()
    with mock.patch("amx.cli_support.commands.history.history_store", return_value=store):
        res = runner.invoke(_history_delete_cmd(), ["999999", "--yes"])
    assert res.exit_code == 0
    assert "not found" in res.output.lower()


def test_history_delete_all(store: SQLiteHistoryStore) -> None:
    _seed(store)
    _seed(store)
    runner = CliRunner()
    with mock.patch("amx.cli_support.commands.history.history_store", return_value=store):
        res = runner.invoke(_history_delete_cmd(), ["--all", "--yes"])
    assert res.exit_code == 0, res.output
    assert store.list_recent_runs(command_filter=None) == []


def test_review_clear_all_categories(
    store: SQLiteHistoryStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr

    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pr, "PENDING_FILE", pending_file)
    pending_file.write_text(
        json.dumps([{"schema": "s", "table": "t", "column": None, "final_description": "td"}]),
        encoding="utf-8",
    )
    rid, ids = _seed(store)
    store.record_evaluation(ids[0], chosen_description="d", evaluation="accepted")
    store.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    runner = CliRunner()
    with mock.patch("amx.cli_support.commands.analyze_flow.history_store", return_value=store):
        res = runner.invoke(_review_clear_cmd(), ["s", "t", "--yes"])
    assert res.exit_code == 0, res.output
    assert not pending_file.exists()
    assert store.list_apply_events() == []
    assert store.get_run_results(rid)[0].get("evaluation") in (None, "")


def test_review_clear_audit_only(
    store: SQLiteHistoryStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr

    monkeypatch.setattr(pr, "PENDING_FILE", tmp_path / "pending.json")
    rid, ids = _seed(store)
    store.record_evaluation(ids[0], chosen_description="d", evaluation="accepted")
    store.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    runner = CliRunner()
    with mock.patch("amx.cli_support.commands.analyze_flow.history_store", return_value=store):
        res = runner.invoke(
            _review_clear_cmd(),
            ["s", "t", "--no-pending", "--no-review-state", "--audit", "--yes"],
        )
    assert res.exit_code == 0, res.output
    assert store.list_apply_events() == []
    # review-state left intact.
    assert store.get_run_results(rid)[0].get("evaluation") == "accepted"
