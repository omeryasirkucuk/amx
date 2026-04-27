from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from click.testing import CliRunner

from amx.cli import main
from amx.config import AMXConfig
from amx.db.connector import AssetKind


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
            patch("amx.cli_support.commands.run.history_store", return_value=fake_history),
            patch("amx.cli_support.commands.run.confirm", return_value=True),
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

    def test_analyze_run_routes_through_cli_analyze_flow_module(self) -> None:
        runner = CliRunner()

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def test_connection(self) -> bool:
                return True

        class FakeDisplay:
            is_active = False

            def start(self, **kwargs) -> None:
                return None

            def stop(self) -> None:
                return None

        with (
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
            patch("amx.utils.live_display.get_display", return_value=FakeDisplay()),
            patch("amx.cli_support.commands.analyze_flow.execute_analyze_run") as execute_run,
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "analyze", "run", "--schema", "sap", "vbak"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        execute_run.assert_called_once()
        self.assertEqual(execute_run.call_args.kwargs["schema"], "sap")
        self.assertEqual(execute_run.call_args.kwargs["tables_pos"], ("vbak",))


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

        with patch("amx.cli_support.commands.history.history_store", return_value=fake_store):
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

        with patch("amx.cli_support.commands.docs._run_docs_semantic_search") as search_docs:
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "docs", "search-docs", "sales order", "--results", "3"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        search_docs.assert_called_once_with("sales order", 3)


class CodeIntegrationTests(unittest.TestCase):
    def test_code_results_without_cache_shows_guidance(self) -> None:
        runner = CliRunner()

        with (
            patch("amx.config.AMXConfig.resolve_code_path", return_value="."),
            patch("amx.codebase.cache.load_latest_cached_report", return_value=(None, None)),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "code", "results"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("No cached code-scan", result.output)
        self.assertIn("/code-scan", result.output)

    def test_code_refresh_uses_resolved_active_profile_path(self) -> None:
        runner = CliRunner()

        with (
            patch("amx.config.AMXConfig.resolve_code_path", return_value="."),
            patch("amx.codebase.cache.invalidate_cache") as invalidate_cache,
            patch("amx.codebase.code_rag.delete_code_collection") as delete_code_collection,
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "code", "refresh"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        invalidate_cache.assert_called_once_with("default", ".")
        delete_code_collection.assert_called_once_with(source_filters=["."])

    def test_code_analyze_without_cache_stops_before_db_work(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def test_connection(self) -> bool:
                raise AssertionError("should not test DB when code cache is missing")

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.config.AMXConfig.resolve_code_path", return_value="."),
            patch("amx.codebase.cache.load_latest_cached_report", return_value=(None, None)),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "code", "analyze", "--schema", "sap", "vbak"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("No cached code-scan", result.output)


class ManualIntegrationTests(unittest.TestCase):
    def test_manual_edit_column_uses_current_context(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.current_schema = "sap"
        cfg.current_table = "vbak"

        class FakeDatabaseConnector:
            calls = []

            def __init__(self, cfg):
                self.cfg = cfg

            def set_column_comment(self, schema, table, column, comment):
                self.calls.append((schema, table, column, comment))

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "manual", "edit", "column", "vbeln", "--comment", "Sales document", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Updated column sap.vbak.vbeln", result.output)
        self.assertEqual(FakeDatabaseConnector.calls, [("sap", "vbak", "vbeln", "Sales document")])

    def test_manual_edit_table_connection_error_is_reported_cleanly(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.current_schema = "sap"

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def resolve_asset_kind(self, schema, table):
                raise Exception("connection refused")

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "manual", "edit", "table", "vbak", "--comment", "x", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Could not resolve the manual edit target", result.output)
        self.assertIn("run /db then /connect", result.output)
        self.assertIn("Cause: Database connection refused.", result.output)

    def test_manual_edit_table_requires_explicit_target(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.current_schema = "sap"
        cfg.current_table = "vbak"

        class FakeDatabaseConnector:
            calls = []

            def __init__(self, cfg):
                self.cfg = cfg

            def set_table_comment(self, schema, table, comment, *, asset_kind):
                self.calls.append((schema, table, comment, asset_kind))

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "manual", "edit", "table", "--comment", "x", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Choose a table/view explicitly", result.output)
        self.assertEqual(FakeDatabaseConnector.calls, [])

    def test_manual_edit_table_accepts_qualified_target(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()

        class FakeDatabaseConnector:
            calls = []

            def __init__(self, cfg):
                self.cfg = cfg

            def resolve_asset_kind(self, schema, table):
                return AssetKind.TABLE

            def set_table_comment(self, schema, table, comment, *, asset_kind):
                self.calls.append((schema, table, comment, asset_kind))

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                [
                    "--config",
                    "test-config.yml",
                    "manual",
                    "edit",
                    "table",
                    "sap_test.adr6",
                    "--comment",
                    "Address data",
                    "--yes",
                ],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Updated table sap_test.adr6", result.output)
        self.assertEqual(FakeDatabaseConnector.calls, [("sap_test", "adr6", "Address data", AssetKind.TABLE)])

    def test_metadata_namespace_is_primary_for_edit(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()

        class FakeDatabaseConnector:
            calls = []

            def __init__(self, cfg):
                self.cfg = cfg

            def resolve_asset_kind(self, schema, table):
                return AssetKind.TABLE

            def set_table_comment(self, schema, table, comment, *, asset_kind):
                self.calls.append((schema, table, comment, asset_kind))

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                [
                    "--config",
                    "test-config.yml",
                    "metadata",
                    "edit",
                    "table",
                    "sap_test.adr6",
                    "--comment",
                    "Address data",
                    "--yes",
                ],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Updated table sap_test.adr6", result.output)
        self.assertEqual(FakeDatabaseConnector.calls, [("sap_test", "adr6", "Address data", AssetKind.TABLE)])


if __name__ == "__main__":
    unittest.main()
