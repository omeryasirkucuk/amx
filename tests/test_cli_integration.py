from __future__ import annotations

from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from click.testing import CliRunner

from amx.cli import main
from amx.config import AMXConfig, DBConfig
from amx.db.connector import AssetKind
from amx.search.catalog import SearchAnswer


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
                self.backend = getattr(cfg, "backend", "unknown")

            def test_connection(self) -> bool:
                return True

        class FakeDisplay:
            is_active = False

            def start(self, **kwargs) -> None:
                self.is_active = True

            def stop(self) -> None:
                self.is_active = False

            def add_activity(self, label: str, token_estimate: int = 0) -> int:
                return 0

            def set_context(self, **kwargs) -> None:
                return None

            def update_activity(self, idx: int, *, label: str | None = None, reset_details: bool = False) -> None:
                return None

            def begin_activity(self, idx: int) -> None:
                return None

            def complete_activity(self, idx: int, detail: str = "") -> None:
                return None

            def fail_activity(self, idx: int, detail: str = "") -> None:
                return None

            def add_detail(self, idx: int, detail: str) -> None:
                return None

        def fake_apply_review_results_to_db(db, rows, on_applied, on_failed=None, on_progress=None):
            for row in rows:
                if on_progress is not None:
                    on_progress(row, "started", 1, len(rows), "")
                if on_progress is not None:
                    on_progress(row, "applied", 1, len(rows), "")
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
            patch("amx.utils.live_display.get_display", return_value=FakeDisplay()),
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

            def set_context(self, **kwargs) -> None:
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

    def test_analyze_run_fails_fast_when_llm_health_check_fails(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "local"
        cfg.llm.model = "llama3"

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def test_connection(self) -> bool:
                return True

        class FakeLLMProvider:
            def __init__(self, cfg):
                self.cfg = cfg
                self.supports_batch = False

            def test_result(self):
                return SimpleNamespace(ok=False, message="model endpoint rejected the request")

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
            patch("amx.llm.provider.LLMProvider", FakeLLMProvider),
            patch("amx.cli_support.commands.analyze_flow.confirm", return_value=False),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "analyze", "run"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Testing LLM connection", result.output)
        self.assertIn("Cannot connect to the active LLM", result.output)
        self.assertIn("model endpoint rejected the request", result.output)

    def test_analyze_run_without_llm_profile_uses_slash_command_guidance(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = ""
        cfg.llm.model = ""

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def test_connection(self) -> bool:
                return True

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "analyze", "run"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 1)
        self.assertIn("No active LLM profile is configured.", result.output)
        self.assertIn("/llm", result.output)
        self.assertIn("/add-llm-profile", result.output)
        self.assertIn("/setup", result.output)


class RootCommandIntegrationTests(unittest.TestCase):
    def test_db_tls_command_updates_active_profile(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.active_db_profile = "corp"
        cfg.db = cfg.db_profiles["corp"] = DBConfig(
            backend="databricks",
            host="workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            access_token="token",
            tls_trusted_ca_file="",
            tls_no_verify=False,
        )
        cfg.save = Mock(return_value="/tmp/amx-test-config.yml")

        with patch("amx.config.AMXConfig.load", return_value=cfg):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "db", "tls", "on"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("tls_no_verify=True", result.output)
        self.assertTrue(cfg.db.tls_no_verify)
        cfg.save.assert_called()

    def test_db_connect_databricks_persists_env_ca_recovery(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"
        cfg.active_db_profile = "corp"
        cfg.db = cfg.db_profiles["corp"] = DBConfig(
            backend="databricks",
            host="workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            access_token="token",
            catalog="main",
            database="dev",
            tls_trusted_ca_file="",
            tls_no_verify=False,
        )
        cfg.save = Mock(return_value="/tmp/amx-test-config.yml")

        class FakeDatabaseConnector:
            def __init__(self, db_cfg):
                self.cfg = db_cfg

            def test_connection_result(self):
                if getattr(self.cfg, "tls_trusted_ca_file", ""):
                    return SimpleNamespace(ok=True, message="")
                return SimpleNamespace(ok=False, message="TLS certificate validation failed.")

        class FakeDisplay:
            is_active = False

            def start(self, **kwargs) -> None:
                return None

            def stop(self) -> None:
                return None

            def set_context(self, **kwargs) -> None:
                return None

            def add_activity(self, label: str, token_estimate: int = 0) -> int:
                return 0

            def begin_activity(self, idx: int) -> None:
                return None

            def set_thinking(self, label: str = "Thinking") -> None:
                return None

            def stop_thinking(self) -> None:
                return None

            def complete_activity(self, idx: int, detail: str = "") -> None:
                return None

            def fail_activity(self, idx: int, detail: str = "") -> None:
                return None

        with tempfile.TemporaryDirectory() as tmp:
            ca_file = f"{tmp}/corp.pem"
            with open(ca_file, "w", encoding="utf-8") as handle:
                handle.write("certificate")
            with (
                patch("amx.config.AMXConfig.load", return_value=cfg),
                patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
                patch("amx.utils.live_display.get_display", return_value=FakeDisplay()),
                patch("amx.utils.live_commands.get_display", return_value=FakeDisplay()),
            ):
                result = runner.invoke(
                    main,
                    ["--config", "test-config.yml", "db", "connect"],
                    env={"AMX_SESSION_CHILD": "1", "AMX_DATABRICKS_TRUSTED_CA_FILE": ca_file},
                    catch_exceptions=False,
                )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Connect stage failed: saved profile", result.output)
        self.assertIn("Connect stage passed: env CA bundle (AMX_DATABRICKS_TRUSTED_CA_FILE)", result.output)
        self.assertIn("Active Databricks trusted CA bundle", result.output)
        self.assertEqual(cfg.db.tls_trusted_ca_file, ca_file)
        cfg.save.assert_called()

    def test_db_connect_databricks_persists_tls_no_verify_fallback(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"
        cfg.active_db_profile = "corp"
        cfg.db = cfg.db_profiles["corp"] = DBConfig(
            backend="databricks",
            host="workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/abc",
            access_token="token",
            catalog="main",
            database="dev",
            tls_trusted_ca_file="",
            tls_no_verify=False,
        )
        cfg.save = Mock(return_value="/tmp/amx-test-config.yml")

        class FakeDatabaseConnector:
            def __init__(self, db_cfg):
                self.cfg = db_cfg

            def test_connection_result(self):
                if getattr(self.cfg, "tls_no_verify", False):
                    return SimpleNamespace(ok=True, message="")
                return SimpleNamespace(ok=False, message="TLS certificate validation failed.")

        class FakeDisplay:
            is_active = False

            def start(self, **kwargs) -> None:
                return None

            def stop(self) -> None:
                return None

            def set_context(self, **kwargs) -> None:
                return None

            def add_activity(self, label: str, token_estimate: int = 0) -> int:
                return 0

            def begin_activity(self, idx: int) -> None:
                return None

            def set_thinking(self, label: str = "Thinking") -> None:
                return None

            def stop_thinking(self) -> None:
                return None

            def complete_activity(self, idx: int, detail: str = "") -> None:
                return None

            def fail_activity(self, idx: int, detail: str = "") -> None:
                return None

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
            patch("amx.utils.live_display.get_display", return_value=FakeDisplay()),
            patch("amx.utils.live_commands.get_display", return_value=FakeDisplay()),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "db", "connect"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Connect stage passed: TLS no-verify fallback", result.output)
        self.assertIn("TLS no-verify", result.output)
        self.assertTrue(cfg.db.tls_no_verify)
        cfg.save.assert_called()

    def test_db_schemas_starts_live_display(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"

        class FakeDatabaseConnector:
            def __init__(self, cfg):
                self.cfg = cfg

            def list_schemas(self):
                return ["sap", "hr"]

        class FakeDisplay:
            def __init__(self) -> None:
                self.is_active = False
                self.started = 0
                self.stopped = 0

            def start(self, **kwargs) -> None:
                self.is_active = True
                self.started += 1

            def stop(self) -> None:
                self.is_active = False
                self.stopped += 1

            def set_context(self, **kwargs) -> None:
                return None

            def add_activity(self, label: str, token_estimate: int = 0) -> int:
                return 0

            def begin_activity(self, idx: int) -> None:
                return None

            def set_thinking(self, label: str = "Thinking") -> None:
                return None

            def stop_thinking(self) -> None:
                return None

            def complete_activity(self, idx: int, detail: str = "") -> None:
                return None

            def fail_activity(self, idx: int, detail: str = "") -> None:
                return None

        display = FakeDisplay()

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.db.connector.DatabaseConnector", FakeDatabaseConnector),
            patch("amx.utils.live_display.get_display", return_value=display),
            patch("amx.utils.live_commands.get_display", return_value=display),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "db", "schemas"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("sap", result.output)
        self.assertEqual(display.started, 1)
        self.assertEqual(display.stopped, 1)


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


class SearchIntegrationTests(unittest.TestCase):
    def test_render_search_rows_filters_zero_score_rows(self) -> None:
        # Default ranked_list dispatch must drop rows whose score is 0.00 so
        # inventory leakage and other diagnostics never surface as "Search matches".
        from amx.cli_support.commands.search import _render_search_rows

        rows = [
            {"schema_name": "sap", "table_name": "vbak", "column_name": "netwr",
             "rank_score": 7.5, "matched_columns": ["netwr"], "row_count": 100,
             "column_count": 12, "effective_description": "Net value"},
            {"schema_name": "sap", "table_name": "kna1", "column_name": "kunnr",
             "score": 0.0, "effective_description": "Customer"},
            {"schema_name": "sap", "table_name": "z", "column_name": "x",
             "rank_score": 0.0, "effective_description": "noise"},
        ]
        with patch("amx.cli_support.commands.search.console") as console_mock:
            _render_search_rows(rows, answer_shape="ranked_list")
        console_mock.print.assert_called_once()
        printed_table = console_mock.print.call_args[0][0]
        # Default (non-debug) column order: Schema.Table, Match, Why, Rows, Cols, Description.
        self.assertEqual(len(printed_table.columns), 6)
        self.assertEqual(list(printed_table.columns[0].cells), ["sap.vbak"])
        # Match column should resolve 7.5 to the Medium band (>=6, <12).
        match_cell_text = printed_table.columns[1].cells.__iter__().__next__()
        self.assertEqual(getattr(match_cell_text, "plain", str(match_cell_text)), "Medium")
        # Score / Source / Conf must NOT be present in non-debug mode.
        column_headers = [c.header for c in printed_table.columns]
        self.assertNotIn("Score", column_headers)
        self.assertNotIn("Source", column_headers)
        self.assertNotIn("Conf", column_headers)

    def test_render_search_rows_debug_appends_score_and_source(self) -> None:
        from amx.cli_support.commands.search import _render_search_rows

        rows = [
            {"schema_name": "sap", "table_name": "vbak", "column_name": "netwr",
             "rank_score": 165.0, "matched_columns": ["netwr"],
             "effective_source_kind": "manual", "current_confidence": "verified",
             "effective_description": "Net value"},
        ]
        with patch("amx.cli_support.commands.search.console") as console_mock:
            _render_search_rows(rows, answer_shape="ranked_list", debug=True)
        printed_table = console_mock.print.call_args[0][0]
        column_headers = [c.header for c in printed_table.columns]
        # Default columns + Score/Source/Conf appended on the right.
        self.assertEqual(column_headers[-3:], ["Score", "Source", "Conf"])
        self.assertEqual(list(printed_table.columns[-3].cells), ["165.00"])
        match_cell_text = printed_table.columns[1].cells.__iter__().__next__()
        # 165 lands far above the 12.0 High threshold.
        self.assertEqual(getattr(match_cell_text, "plain", str(match_cell_text)), "High")

    def test_render_search_rows_uses_matched_columns_for_why(self) -> None:
        from amx.cli_support.commands.search import _render_search_rows

        rows = [
            {"schema_name": "sap", "table_name": "vbak", "column_name": "x",
             "rank_score": 12.0, "matched_columns": ["supplier_id", "vendor_name"],
             "effective_description": "Sales header"},
        ]
        with patch("amx.cli_support.commands.search.console") as console_mock:
            _render_search_rows(rows, answer_shape="ranked_list")
        printed_table = console_mock.print.call_args[0][0]
        why_cell = next(iter(printed_table.columns[2].cells))
        self.assertIn("supplier_id", getattr(why_cell, "plain", str(why_cell)))
        self.assertIn("vendor_name", getattr(why_cell, "plain", str(why_cell)))

    def test_render_search_rows_dispatches_inventory_for_schema_explorer_rows(self) -> None:
        from amx.cli_support.commands.search import _render_search_rows

        rows = [
            {
                "row_type": "schema_explorer_table",
                "schema_name": "sap_s6p",
                "table_name": "dd03l",
                "column_count": 102,
                "row_count": 10772134,
                "semantic_cluster": "Dd03L",
            },
        ]
        with patch("amx.cli_support.commands.search.console") as console_mock:
            _render_search_rows(rows)
        console_mock.print.assert_called_once()
        printed_table = console_mock.print.call_args[0][0]
        self.assertEqual(printed_table.title, "Inventory")
        # Column order: Schema, Table, Columns, Rows, Cluster.
        self.assertEqual(list(printed_table.columns[3].cells), ["10772134"])

    def test_search_ask_requires_llm_profile(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.active_db_profile = "default"
        cfg.llm.provider = ""
        cfg.llm.model = ""

        fake_catalog = Mock()
        fake_catalog.get_settings.return_value = {
            "llm_enabled": "true",
            "show_provenance": "true",
            "show_confidence": "true",
            "max_retrieved_entities": "8",
        }
        fake_catalog.sync_status.return_value = {
            "entities": {"total_entities": 1},
            "descriptions": {},
            "jobs": [],
        }

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.cli_support.commands.search._catalog", return_value=fake_catalog),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "search", "ask", "price columns"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("requires an active LLM profile", result.output)

    def test_search_ask_actions_prompt_requires_human_approval(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.active_db_profile = "default"
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"
        answer = SearchAnswer(
            intent="find_columns",
            question="price columns",
            rows=[],
            confidence="low",
            summary="No strong matches.",
            provenance=[],
            details={
                "actions": [{"action": "sync_catalog", "reason": "Refresh catalog structure and comments."}],
                "retrieval": {},
                "verification": {},
                "policy": {},
                "plan": {},
            },
        )
        fake_service = Mock()
        fake_service.ask.return_value = answer
        fake_service.settings = {"show_provenance": "true", "show_confidence": "true"}

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.cli_support.commands.search._service", return_value=fake_service),
            patch("amx.cli_support.commands.search.confirm", return_value=False),
            patch("amx.cli_support.commands.search._run_search_action") as run_action,
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "search", "ask", "--actions", "price columns"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Suggested next step: sync_catalog", result.output)
        run_action.assert_not_called()

    def test_search_sync_starts_live_display(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"

        fake_catalog = Mock()
        fake_catalog.start_sync_job.return_value = 11

        class FakeDisplay:
            def __init__(self) -> None:
                self.is_active = False
                self.started = 0
                self.stopped = 0

            def start(self, **kwargs) -> None:
                self.is_active = True
                self.started += 1

            def stop(self) -> None:
                self.is_active = False
                self.stopped += 1

            def set_context(self, **kwargs) -> None:
                return None

            def add_activity(self, label: str, token_estimate: int = 0) -> int:
                return 0

            def begin_activity(self, idx: int) -> None:
                return None

            def update_activity(self, idx: int, *, label: str | None = None, reset_details: bool = False) -> None:
                return None

            def complete_activity(self, idx: int, detail: str = "") -> None:
                return None

            def fail_activity(self, idx: int, detail: str = "") -> None:
                return None

            def add_detail(self, idx: int, detail: str) -> None:
                return None

        display = FakeDisplay()

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.cli_support.commands.search._catalog", return_value=fake_catalog),
            patch("amx.utils.live_display.get_display", return_value=display),
            patch("amx.utils.live_commands.get_display", return_value=display),
            patch("amx.cli_support.commands.search._interactive_sync_scope", return_value=(cfg, {"sap": ["vbak"]})),
            patch("amx.cli_support.commands.search._sync_db_scope", return_value=(0, 1)),
            patch("amx.cli_support.commands.search._sync_cached_code_evidence", return_value=True),
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "search", "sync"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertEqual(display.started, 1)
        self.assertEqual(display.stopped, 1)
        fake_catalog.finish_sync_job.assert_called_once()

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

    def test_manual_edit_table_starts_wizard_for_ambiguous_scope(self) -> None:
        runner = CliRunner()
        cfg = AMXConfig()
        cfg.current_schema = "sap"
        cfg.current_table = "vbak"

        with (
            patch("amx.config.AMXConfig.load", return_value=cfg),
            patch("amx.cli_support.commands.manual._run_edit_wizard", return_value=None) as wizard,
        ):
            result = runner.invoke(
                main,
                ["--config", "test-config.yml", "manual", "edit", "table", "--comment", "x", "--yes"],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        wizard.assert_called_once_with(cfg)

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
                    "default.sap_test.adr6",
                    "--comment",
                    "Address data",
                    "--yes",
                ],
                env={"AMX_SESSION_CHILD": "1"},
                catch_exceptions=False,
            )

        self.assertEqual(result.exit_code, 0)
        self.assertIn("Updated table default.sap_test.adr6", result.output)
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
