from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from click.testing import CliRunner

from amx.cli import main


class AnalyzeApplyIntegrationTests(unittest.TestCase):
    def test_analyze_apply_applies_pending_metadata(self) -> None:
        runner = CliRunner()
        fake_history = Mock()
        pending = [
            SimpleNamespace(
                table="orders",
                column="id",
                final_description="Order identifier",
                result_id=17,
            )
        ]

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def test_connection(self) -> bool:
                return True

        def fake_apply_review_results_to_db(db, rows, on_applied):
            for row in rows:
                on_applied(row)
            return len(rows)

        with (
            patch("amx.pending_review.load_pending", return_value=pending),
            patch("amx.pending_review.clear_pending") as clear_pending,
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
            patch(
                "amx.agents.orchestrator.apply_review_results_to_db",
                side_effect=fake_apply_review_results_to_db,
            ),
            patch("amx.cli_run.history_store", return_value=fake_history),
            patch("amx.cli_run.confirm", return_value=True),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "analyze", "apply"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Apply pending metadata to the database", result.output)
        self.assertIn("Applied 1 comment(s). Pending file cleared.", result.output)
        clear_pending.assert_called_once()
        fake_history.record_applied.assert_called_once_with(17)


class HistoryListIntegrationTests(unittest.TestCase):
    def test_history_list_renders_scope_summary(self) -> None:
        runner = CliRunner()
        fake_store = Mock()
        fake_store.list_recent_runs.return_value = [
            {
                "id": 5,
                "started_at": 1710000000,
                "status": "success",
                "mode": "chat",
                "db_backend": "postgresql",
                "scope_json": {"sap": ["vbak", "vbap"], "hr": ["employees"]},
                "llm_provider": "openai",
                "llm_model": "gpt-4o",
                "duration_sec": 12.3,
                "metrics_json": {"model_processing_sec": 10.5},
            }
        ]

        with patch("amx.cli_history.history_store", return_value=fake_store):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "history", "list"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Recent runs", result.output)
        fake_store.list_recent_runs.assert_called_once_with(limit=20)
        self.assertIn("success", result.output)
        self.assertIn("schemas", result.output)
        self.assertIn("tables)", result.output)


class DocsIntegrationTests(unittest.TestCase):
    def test_docs_scan_without_paths_shows_guidance(self) -> None:
        runner = CliRunner()

        result = runner.invoke(
            main,
            ["--config", "test-config.yml", "docs", "scan"],
            env={"AMX_SESSION_CHILD": "1"},
            catch_exceptions=False,
        )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("No document paths to scan.", result.output)
        self.assertIn("/add-doc-profile", result.output)

    def test_docs_search_docs_routes_through_cli_docs_module(self) -> None:
        runner = CliRunner()

        with patch("amx.cli_docs._run_docs_semantic_search") as search_docs:
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "docs", "search-docs", "sales order", "--results", "3"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        search_docs.assert_called_once_with("sales order", 3)


if __name__ == "__main__":
    unittest.main()
