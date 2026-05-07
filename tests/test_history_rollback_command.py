"""``amx /history rollback <run_id>`` command tests.

The user scenario (audit walkthrough): a DBA had hand-written
comments in the live DB, the user ran ``amx /run`` + ``/apply``,
the LLM rewrites took over. Rollback now restores the originals
byte-for-byte regardless of who originally wrote them.

Tests pin the contract without a real DB:

* No history store → ``/history rollback`` reports a clean error.
* No events for the run → graceful warning, no DB writes.
* Mix of restorable and ``old_comment is None`` rows → only the
  restorable ones are written; the unknown ones are skipped (and
  reported in the success summary).
* Reverse-time replay: latest write unwinds first so a chain of
  writes to one asset within a single run unwinds in order.
* ``--yes`` skips the confirm prompt.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from amx.cli import main


def _event(
    *,
    applied_at: float,
    schema: str = "public",
    table: str = "orders",
    column: str | None = "id",
    asset_kind: str = "table",
    new_comment: str = "LLM rewrite.",
    old_comment: str | None = "DBA original.",
) -> dict:
    """Build an apply_events row in the shape ``list_apply_events`` returns."""
    return {
        "id": int(applied_at * 1000) % 1_000_000,
        "applied_at": applied_at,
        "run_id": 42,
        "result_id": None,
        "profile_name": "prod_pg",
        "schema_name": schema,
        "table_name": table,
        "column_name": column,
        "asset_kind": asset_kind,
        "old_comment": old_comment,
        "new_comment": new_comment,
        "applied_by": "omer",
        "hostname": "laptop",
        "sql_template": "",
    }


class HistoryRollbackTests(unittest.TestCase):
    def test_no_history_store_reports_clean_error(self) -> None:
        runner = CliRunner()
        with patch("amx.cli_support.commands.history.history_store", return_value=None):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "rollback", "42"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("History store isn't initialized", result.output)
        # No DB connector ever instantiated.
        self.assertNotIn("✓", result.output)

    def test_empty_event_list_warns_and_exits_clean(self) -> None:
        runner = CliRunner()
        store = MagicMock()
        store.list_apply_events.return_value = []
        with patch("amx.cli_support.commands.history.history_store", return_value=store):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "rollback", "42"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("No apply events", result.output)
        store.list_apply_events.assert_called_once_with(run_id=42, limit=10_000)

    def test_only_unknown_old_comments_skips_with_warning(self) -> None:
        runner = CliRunner()
        store = MagicMock()
        store.list_apply_events.return_value = [
            _event(applied_at=1.0, column="id", old_comment=None),
            _event(applied_at=2.0, column="amount", old_comment=None),
        ]
        with patch("amx.cli_support.commands.history.history_store", return_value=store):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "rollback", "42", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Nothing to restore", result.output)
        # No DatabaseConnector creation either.

    def test_restorable_rows_replayed_in_reverse_time_order(self) -> None:
        runner = CliRunner()
        store = MagicMock()
        # Three writes within run #42: t=1 → t=2 → t=3 on the same asset.
        # Rollback should replay starting at t=3 going backwards so the
        # asset ends up holding the value it had before t=1.
        store.list_apply_events.return_value = [
            _event(applied_at=1.0, column="id", old_comment="v0"),
            _event(applied_at=2.0, column="id", old_comment="v1"),
            _event(applied_at=3.0, column="id", old_comment="v2"),
        ]

        applied_in_order: list[str] = []

        class FakeConnector:
            def __init__(self, _cfg) -> None:
                self.backend = "postgresql"
                self.engine = SimpleNamespace(begin=lambda: _NoopTx())

            def test_connection(self) -> bool:
                return True

            def apply_comment(self, **kwargs) -> None:
                applied_in_order.append(kwargs["comment"])

        class _NoopTx:
            def __enter__(self):
                return self

            def __exit__(self, *exc) -> bool:
                return False

        with (
            patch("amx.cli_support.commands.history.history_store", return_value=store),
            patch("amx.db.connector.DatabaseConnector", FakeConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "rollback", "42", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )
        self.assertEqual(result.exit_code, 0, msg=result.output)
        # Reverse time order — latest-applied unwinds first.
        self.assertEqual(applied_in_order, ["v2", "v1", "v0"])
        self.assertIn("Restored 3 comment(s)", result.output)

    def test_mix_restorable_and_unknown(self) -> None:
        runner = CliRunner()
        store = MagicMock()
        store.list_apply_events.return_value = [
            _event(applied_at=1.0, column="id", old_comment="DBA-id"),
            _event(applied_at=2.0, column="amount", old_comment=None),
            _event(applied_at=3.0, column="status", old_comment="DBA-status"),
        ]

        applied_pairs: list[tuple[str | None, str]] = []

        class FakeConnector:
            def __init__(self, _cfg) -> None:
                self.backend = "postgresql"
                self.engine = SimpleNamespace(begin=lambda: _NoopTx())

            def test_connection(self) -> bool:
                return True

            def apply_comment(self, **kwargs) -> None:
                applied_pairs.append((kwargs.get("column"), kwargs["comment"]))

        class _NoopTx:
            def __enter__(self):
                return self

            def __exit__(self, *exc) -> bool:
                return False

        with (
            patch("amx.cli_support.commands.history.history_store", return_value=store),
            patch("amx.db.connector.DatabaseConnector", FakeConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "rollback", "42", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )
        self.assertEqual(result.exit_code, 0)
        # 'amount' (None old_comment) is skipped; the other two replay
        # in reverse time order: status first, then id.
        self.assertEqual(applied_pairs, [("status", "DBA-status"), ("id", "DBA-id")])
        self.assertIn("Restored 2 comment(s)", result.output)
