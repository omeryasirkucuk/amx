from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import click

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.agents.code_agent import CodeAgent
from amx.agents.orchestrator import Orchestrator
from amx.codebase.analyzer import CodebaseReport, analyze_codebase
from amx.codebase.code_rag import _normalize_source_filter, _source_allowed
from amx.cli_support.commands.history import format_run_scope
from amx.cli_support.commands.manual import _run_edit_wizard
from amx.cli_support.commands.profiles import cmd_use_doc, default_model
from amx.cli_support import inject_session_defaults, session_to_click_args
from amx.cli_support.session import _format_session_click_error, _handle_manual_usage_shortcuts
from amx.config import AMXConfig, DBConfig, normalize_llm_model
from amx.cli_support.commands.db import cmd_profiling
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.connector import AssetKind, DatabaseConnector
from amx.db.connector import ColumnProfile, TableProfile
from amx.docs.rag import RAGStore
from amx.docs.scanner import _resolve_github, _resolve_s3, cleanup_scan_artifacts
from amx.llm.batch import BatchRequest, OpenAIBatchProvider
from amx.llm.provider import LLMProvider
from amx.services.analyze_scope import filter_non_business_assets
from amx.services.manual_metadata import collect_metadata_coverage, resolve_manual_target, resolve_path_target


class DocumentScannerTests(unittest.TestCase):
    def test_github_scan_artifacts_are_marked_for_cleanup(self) -> None:
        cloned: list[str] = []

        class FakeRepo:
            @staticmethod
            def clone_from(url: str, dest: str, depth: int) -> None:
                cloned.append(dest)
                docs_dir = Path(dest) / "docs"
                docs_dir.mkdir(parents=True)
                (docs_dir / "guide.md").write_text("hello", encoding="utf-8")

        fake_git = SimpleNamespace(Repo=FakeRepo)

        with patch.dict(sys.modules, {"git": fake_git}):
            docs = list(_resolve_github("https://github.com/acme/docs"))

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].cleanup_root, cloned[0])
        self.assertTrue(Path(docs[0].path).exists())

        cleanup_scan_artifacts(docs)

        self.assertFalse(Path(cloned[0]).exists())

    def test_s3_download_preserves_key_prefixes_for_duplicate_basenames(self) -> None:
        class FakePaginator:
            def paginate(self, Bucket: str, Prefix: str):
                yield {
                    "Contents": [
                        {"Key": "team-a/spec.md", "Size": 3},
                        {"Key": "team-b/spec.md", "Size": 4},
                    ]
                }

        class FakeS3:
            def get_paginator(self, name: str):
                self.paginator_name = name
                return FakePaginator()

            def download_file(self, bucket: str, key: str, filename: str) -> None:
                Path(filename).write_text(key, encoding="utf-8")

        fake_s3 = FakeS3()
        fake_boto3 = SimpleNamespace(client=lambda service: fake_s3)

        with tempfile.TemporaryDirectory() as tmp, patch.dict(sys.modules, {"boto3": fake_boto3}):
            docs = list(_resolve_s3("s3://bucket", target_dir=tmp))

        paths = {Path(doc.path).relative_to(tmp).as_posix() for doc in docs}
        self.assertEqual(paths, {"team-a/spec.md", "team-b/spec.md"})


class CodebaseCleanupTests(unittest.TestCase):
    def test_remote_codebase_clone_is_removed_after_scan(self) -> None:
        cloned: list[str] = []

        class FakeRepo:
            @staticmethod
            def clone_from(url: str, dest: str, depth: int) -> None:
                cloned.append(dest)
                (Path(dest) / "job.py").write_text("spark.read.table('orders')\n", encoding="utf-8")

        fake_git = SimpleNamespace(Repo=FakeRepo)

        with patch.dict(sys.modules, {"git": fake_git}):
            report = analyze_codebase("https://github.com/acme/app", ["orders"])

        self.assertEqual(report.scanned_files, 1)
        self.assertIn("orders", report.references)
        self.assertFalse(Path(cloned[0]).exists())


class RAGSourceFilteringTests(unittest.TestCase):
    def test_source_root_allows_remote_profile_filter(self) -> None:
        store = object.__new__(RAGStore)
        store.source_filters = ["https://github.com/acme/docs"]

        allowed = store._source_allowed(
            {
                "source": "/var/folders/tmp/amx_gh_123/file.md",
                "source_root": "https://github.com/acme/docs",
            }
        )

        self.assertTrue(allowed)


class CodeRAGFilteringTests(unittest.TestCase):
    def test_code_source_allowed_uses_source_root(self) -> None:
        filters = [_normalize_source_filter("https://github.com/acme/app")]

        allowed = _source_allowed(
            {
                "source": "/tmp/amx_code_123/src/job.py",
                "source_root": "https://github.com/acme/app",
            },
            filters,
        )

        self.assertTrue(allowed)

    def test_code_agent_filters_semantic_lookup_to_report_path(self) -> None:
        class DummyLLM:
            cfg = object()

        report = CodebaseReport(path="https://github.com/acme/app")
        agent = CodeAgent(DummyLLM(), report)
        ctx = AgentContext(
            schema="sap",
            table="vbak",
            db_profile={"columns": [{"name": "vbeln", "dtype": "TEXT"}]},
        )

        with (
            patch("amx.codebase.code_rag.code_collection_count", return_value=1) as count,
            patch(
                "amx.codebase.code_rag.query_code_snippets",
                return_value=[{"text": "spark.read.table('sap.vbak')", "metadata": {}, "distance": 0.1}],
            ) as query,
        ):
            messages = agent._build_messages(ctx)

        self.assertIsNotNone(messages)
        count.assert_called_once_with(source_filters=["https://github.com/acme/app"])
        query.assert_called_once()
        self.assertEqual(query.call_args.kwargs["source_filters"], ["https://github.com/acme/app"])


class BackendCapabilityTests(unittest.TestCase):
    def test_bigquery_database_comment_writeback_is_explicitly_unsupported(self) -> None:
        adapter = BigQueryAdapter(DBConfig(backend="bigquery", project="p", dataset="d"))

        with self.assertRaises(NotImplementedError):
            adapter.set_database_comment_sql()


class ProfilingGuardrailTests(unittest.TestCase):
    def test_cli_profiling_updates_active_profile(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(backend="postgresql")
        cfg.db_profiles = {"default": cfg.db}
        cfg.active_db_profile = "default"
        cfg.save = lambda: "/tmp/amx-test-config.yml"  # type: ignore[method-assign]

        cmd_profiling(cfg, ["sampled", "500000", "3"])

        self.assertEqual(cfg.db.profiling_mode, "sampled")
        self.assertEqual(cfg.db.profiling_max_rows, 500_000)
        self.assertEqual(cfg.db.profiling_sample_size, 3)
        self.assertEqual(cfg.db_profiles["default"].profiling_mode, "sampled")
        self.assertEqual(cfg.db_profiles["default"].profiling_max_rows, 500_000)
        self.assertEqual(cfg.db_profiles["default"].profiling_sample_size, 3)

    def test_metadata_mode_does_not_open_data_connection(self) -> None:
        class FakeEngine:
            def connect(self):
                raise AssertionError("metadata mode should not scan table data")

        class FakeAdapter:
            name = "fake"

            def fully_qualified_name(self, schema: str, table: str) -> str:
                return f'"{schema}"."{table}"'

            def quote_identifier(self, name: str) -> str:
                return f'"{name}"'

            def get_table_stats(self, engine, schema: str, table: str) -> dict[str, int]:
                return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 2_500_000}

            def get_schema_comment(self, engine, schema: str):
                return None

            def get_database_comment(self, engine):
                return None

            def get_incoming_foreign_keys(self, engine, schema: str, table: str):
                return []

        class FakeInspector:
            def get_table_comment(self, table: str, schema: str):
                return {"text": "Existing table comment"}

            def get_columns(self, table: str, schema: str):
                return [{"name": "id", "type": "INTEGER", "nullable": False, "comment": "Identifier"}]

            def get_pk_constraint(self, table: str, schema: str):
                return {"constrained_columns": ["id"]}

            def get_foreign_keys(self, table: str, schema: str):
                return []

            def get_unique_constraints(self, table: str, schema: str):
                return []

            def get_check_constraints(self, table: str, schema: str):
                return []

        db = object.__new__(DatabaseConnector)
        db.cfg = DBConfig(backend="postgresql", profiling_mode="metadata")
        db._engine = FakeEngine()
        db._adapter = FakeAdapter()

        with patch("amx.db.connector.inspect", return_value=FakeInspector()):
            profile = db.profile_table("public", "orders", asset_kind=AssetKind.TABLE)

        self.assertEqual(profile.row_count, 2_500_000)
        self.assertEqual(profile.columns[0].samples, [])
        self.assertEqual(profile.columns[0].distinct_count, 0)


class HistoryFormattingTests(unittest.TestCase):
    def test_format_run_scope_handles_single_and_multi_schema(self) -> None:
        self.assertEqual(format_run_scope({"sap": ["vbak"]}), "sap.vbak")
        self.assertEqual(format_run_scope({"sap": ["vbak", "vbap"]}), "sap (2 tables)")
        self.assertEqual(
            format_run_scope({"sap": ["vbak", "vbap"], "hr": ["employees"]}),
            "2 schemas (3 tables)",
        )
        self.assertEqual(format_run_scope(None), "-")


class ProfileHelperTests(unittest.TestCase):
    def test_default_model_includes_openrouter(self) -> None:
        self.assertEqual(default_model("openrouter"), "openai/gpt-4o-mini")

    def test_openrouter_model_normalization_strips_duplicate_provider_prefix(self) -> None:
        self.assertEqual(
            normalize_llm_model("openrouter", "openrouter/qwen/qwen3.6-plus"),
            "qwen/qwen3.6-plus",
        )

    def test_openrouter_provider_does_not_reprefix_normalized_model(self) -> None:
        provider = LLMProvider.__new__(LLMProvider)
        provider.cfg = SimpleNamespace(provider="openrouter", model="qwen/qwen3.6-plus")
        self.assertEqual(provider.model_name, "qwen/qwen3.6-plus")

    def test_use_doc_accepts_disable_alias(self) -> None:
        cfg = AMXConfig()
        cfg.doc_profiles = {"default": ["/tmp/docs"]}
        cfg.active_doc_profile = "default"
        cfg.save = lambda: "/tmp/amx-test-config.yml"  # type: ignore[method-assign]

        cmd_use_doc(cfg, ["disable"])

        self.assertEqual(cfg.active_doc_profile, "__none__")


class SessionHelperTests(unittest.TestCase):
    def test_session_to_click_args_maps_run_apply_shortcut(self) -> None:
        self.assertEqual(
            session_to_click_args("", ["run-apply", "vbak"]),
            ["analyze", "run", "--apply", "vbak"],
        )

    def test_inject_session_defaults_applies_schema_to_code_scan(self) -> None:
        cfg = AMXConfig()
        cfg.current_schema = "sap_s6p"

        args = inject_session_defaults(cfg, "code", ["code", "scan", "/tmp/repo"])

        self.assertEqual(args, ["code", "scan", "/tmp/repo", "--schema", "sap_s6p"])

    def test_session_to_click_args_maps_metadata_shortcuts(self) -> None:
        self.assertEqual(
            session_to_click_args("", ["monitor", "sap"]),
            ["metadata", "monitor", "sap"],
        )
        self.assertEqual(
            session_to_click_args("metadata", ["edit", "column", "vbeln"]),
            ["metadata", "edit", "column", "vbeln"],
        )
        self.assertEqual(
            session_to_click_args("manual", ["edit", "column", "vbeln"]),
            ["metadata", "edit", "column", "vbeln"],
        )

    def test_format_session_click_error_preserves_missing_argument_message(self) -> None:
        exc = click.UsageError("Missing argument 'SCOPE'.")

        msg = _format_session_click_error("edit", exc)

        self.assertEqual(msg, "Missing argument 'SCOPE'.")

    def test_format_session_click_error_keeps_unknown_command_message_slash_native(self) -> None:
        exc = click.UsageError("No such command 'wat'.")

        msg = _format_session_click_error("wat", exc)

        self.assertEqual(msg, "Unknown command: /wat. Type /help.")

    def test_handle_manual_usage_shortcuts_catches_bare_edit(self) -> None:
        with (
            patch("amx.cli_support.session.error") as error_mock,
            patch("amx.cli_support.session.info") as info_mock,
        ):
            handled = _handle_manual_usage_shortcuts("metadata", ["edit"])

        self.assertFalse(handled)
        error_mock.assert_not_called()
        info_mock.assert_not_called()


class ManualMetadataTests(unittest.TestCase):
    def test_collect_metadata_coverage_counts_asset_and_column_comments(self) -> None:
        class FakeDB:
            def list_assets(self, schema):
                return [("orders", object()), ("customers", object())]

            def get_table_comment(self, schema, table):
                return "Orders table" if table == "orders" else None

            def get_column_comments(self, schema, table):
                if table == "orders":
                    return {"id": "Identifier", "note": None}
                return {"id": None}

        coverage = collect_metadata_coverage(FakeDB(), "public")

        self.assertEqual(coverage.assets, 2)
        self.assertEqual(coverage.assets_with_comments, 1)
        self.assertEqual(coverage.columns, 3)
        self.assertEqual(coverage.columns_with_comments, 1)

    def test_resolve_manual_target_uses_current_context_for_column(self) -> None:
        cfg = AMXConfig()
        cfg.current_schema = "sap"
        cfg.current_table = "vbak"
        errors: list[str] = []

        class FakeDB:
            def set_column_comment(self, schema, table, column, comment):
                self.last_call = (schema, table, column, comment)

        db = FakeDB()
        target = resolve_manual_target(cfg, db, "column", ["vbeln"], error=errors.append)

        self.assertEqual(target[0], "column sap.vbak.vbeln")
        target[1]("Sales document")
        self.assertEqual(db.last_call, ("sap", "vbak", "vbeln", "Sales document"))
        self.assertEqual(errors, [])

    def test_resolve_manual_target_rejects_implicit_table_edit(self) -> None:
        cfg = AMXConfig()
        cfg.current_schema = "sap"
        cfg.current_table = "vbak"
        errors: list[str] = []

        class FakeDB:
            pass

        target = resolve_manual_target(cfg, FakeDB(), "table", [], error=errors.append)

        self.assertIsNone(target)
        self.assertEqual(
            errors,
            ["Choose a table/view explicitly: /edit table <table> or /edit table <schema>.<table>"],
        )

    def test_resolve_manual_target_accepts_dotted_table_target(self) -> None:
        cfg = AMXConfig()
        errors: list[str] = []

        class FakeDB:
            def resolve_asset_kind(self, schema, table):
                return AssetKind.TABLE

            def set_table_comment(self, schema, table, comment, *, asset_kind):
                self.last_call = (schema, table, comment, asset_kind)

        db = FakeDB()
        target = resolve_manual_target(cfg, db, "table", ["sap_test.adr6"], error=errors.append)

        self.assertEqual(target[0], "table sap_test.adr6")
        target[1]("Address data")
        self.assertEqual(db.last_call, ("sap_test", "adr6", "Address data", AssetKind.TABLE))
        self.assertEqual(errors, [])

    def test_resolve_manual_target_accepts_dotted_column_target(self) -> None:
        cfg = AMXConfig()
        errors: list[str] = []

        class FakeDB:
            def set_column_comment(self, schema, table, column, comment):
                self.last_call = (schema, table, column, comment)

        db = FakeDB()
        target = resolve_manual_target(cfg, db, "column", ["sap_test.adr6.smtp_addr"], error=errors.append)

        self.assertEqual(target[0], "column sap_test.adr6.smtp_addr")
        target[1]("Email address")
        self.assertEqual(db.last_call, ("sap_test", "adr6", "smtp_addr", "Email address"))
        self.assertEqual(errors, [])

    def test_resolve_path_target_maps_four_part_path_to_column(self) -> None:
        cfg = AMXConfig()
        cfg.db_profiles = {"warehouse": cfg.db}
        cfg.active_db_profile = "warehouse"
        errors: list[str] = []

        class FakeDB:
            def set_column_comment(self, schema, table, column, comment):
                self.last_call = (schema, table, column, comment)

        db = FakeDB()
        target = resolve_path_target(cfg, db, "warehouse", "warehouse.sap_test.adr6.smtp_addr", error=errors.append)

        self.assertEqual(target.label, "column warehouse.sap_test.adr6.smtp_addr")
        target.writer("Email address")
        self.assertEqual(db.last_call, ("sap_test", "adr6", "smtp_addr", "Email address"))
        self.assertEqual(errors, [])

    def test_resolve_path_target_maps_one_part_path_to_database(self) -> None:
        cfg = AMXConfig()
        cfg.db_profiles = {"warehouse": cfg.db}
        cfg.active_db_profile = "warehouse"
        errors: list[str] = []

        class FakeDB:
            def set_database_comment(self, comment):
                self.last_call = comment

        db = FakeDB()
        target = resolve_path_target(cfg, db, "warehouse", "warehouse", error=errors.append)

        self.assertEqual(target.label, "database warehouse")
        target.writer("Warehouse profile")
        self.assertEqual(db.last_call, "Warehouse profile")
        self.assertEqual(errors, [])

    def test_edit_wizard_drills_to_column_target(self) -> None:
        cfg = AMXConfig()
        cfg.db_profiles = {"default": cfg.db}
        cfg.active_db_profile = "default"

        class Column:
            name = "smtp_addr"
            dtype = "TEXT"

        class FakeDB:
            def list_schemas(self):
                return ["sap"]

            def list_assets(self, schema):
                return [("adr6", AssetKind.TABLE)]

            def list_column_profiles(self, schema, table):
                return [Column()]

            def set_column_comment(self, schema, table, column, comment):
                self.last_call = (schema, table, column, comment)

        db = FakeDB()
        with (
            patch("amx.cli_support.commands.manual._connector_for_profile", return_value=db),
            patch("amx.cli_support.commands.manual._ask_text_or_cancel", return_value="y"),
            patch(
                "amx.cli_support.commands.manual._ask_choice_or_cancel",
                side_effect=["Column", "sap", "adr6", "smtp_addr"],
            ),
        ):
            target = _run_edit_wizard(cfg)

        self.assertEqual(target.label, "column default.sap.adr6.smtp_addr")
        target.writer("Email address")
        self.assertEqual(db.last_call, ("sap", "adr6", "smtp_addr", "Email address"))


class AnalyzeScopeServiceTests(unittest.TestCase):
    def test_filter_non_business_assets_drops_pg_stat_objects(self) -> None:
        warnings: list[str] = []

        filtered = filter_non_business_assets(
            {"public": ["orders", "pg_stat_statements", "pg_statio_user_tables"]},
            warn=warnings.append,
        )

        self.assertEqual(filtered, {"public": ["orders"]})
        self.assertEqual(len(warnings), 1)
        self.assertIn("Skipping non-business/system assets", warnings[0])


class ConfidenceCalibrationTests(unittest.TestCase):
    def test_missing_logprobs_preserves_existing_confidence(self) -> None:
        suggestion = MetadataSuggestion(
            schema="public",
            table="orders",
            column="id",
            suggestions=["Identifier"],
            confidence=Confidence.HIGH,
            reasoning="model text said high",
            source="profile",
        )

        calibrated = apply_logprob_confidence([suggestion], logprobs=None)

        self.assertEqual(calibrated[0].confidence, Confidence.HIGH)

    def test_response_text_scores_each_suggestion_description(self) -> None:
        response = (
            "COLUMN: id\n"
            "DESCRIPTION_1: Certain identifier\n"
            "CONFIDENCE: HIGH\n"
            "REASONING: clear\n"
            "COLUMN: note\n"
            "DESCRIPTION_1: Ambiguous free text\n"
            "CONFIDENCE: HIGH\n"
            "REASONING: unclear\n"
        )
        logprobs = []
        pos = 0
        high_desc = "Certain identifier"
        low_desc = "Ambiguous free text"
        while pos < len(response):
            if response.startswith(high_desc, pos):
                logprobs.append({"token": high_desc, "logprob": -0.01})
                pos += len(high_desc)
            elif response.startswith(low_desc, pos):
                logprobs.append({"token": low_desc, "logprob": -2.0})
                pos += len(low_desc)
            else:
                logprobs.append({"token": response[pos], "logprob": -0.05})
                pos += 1
        suggestions = [
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="id",
                suggestions=["Certain identifier"],
                confidence=Confidence.LOW,
                reasoning="",
                source="profile",
            ),
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="note",
                suggestions=["Ambiguous free text"],
                confidence=Confidence.HIGH,
                reasoning="",
                source="profile",
            ),
        ]

        calibrated = apply_logprob_confidence(
            suggestions,
            logprobs,
            high_threshold=0.85,
            medium_threshold=0.50,
            response_text=response,
        )

        self.assertEqual(calibrated[0].confidence, Confidence.HIGH)
        self.assertEqual(calibrated[1].confidence, Confidence.LOW)
        self.assertGreater(calibrated[0].logprob_score or 0, calibrated[1].logprob_score or 0)


class BatchLogprobTests(unittest.TestCase):
    def test_openai_batch_requests_logprobs(self) -> None:
        req = BatchRequest(
            custom_id="profile:public:orders:0",
            messages=[{"role": "user", "content": "Describe columns"}],
        )

        body = json.loads(OpenAIBatchProvider._build_jsonl([req], "gpt-4o-mini").decode().splitlines()[0])["body"]

        self.assertTrue(body["logprobs"])
        self.assertEqual(body["top_logprobs"], 5)


class OrchestratorFallbackTests(unittest.TestCase):
    def test_missing_columns_get_low_confidence_fallbacks(self) -> None:
        class DummyDB:
            pass

        class DummyLLM:
            pass

        orch = Orchestrator(DummyDB(), DummyLLM())
        profile = TableProfile(
            schema="public",
            name="orders",
            columns=[
                ColumnProfile(name="id", dtype="INTEGER", nullable=False),
                ColumnProfile(name="amount", dtype="NUMERIC", nullable=True),
            ],
        )
        merged = [
            MetadataSuggestion(
                schema="public",
                table="orders",
                column="id",
                suggestions=["Order identifier"],
                confidence=Confidence.HIGH,
                reasoning="parsed",
                source="combined",
            )
        ]

        completed = orch._ensure_complete_table_coverage(profile, merged)

        by_column = {s.column: s for s in completed}
        self.assertIn(None, by_column)
        self.assertIn("amount", by_column)
        self.assertEqual(by_column["amount"].confidence, Confidence.LOW)


if __name__ == "__main__":
    unittest.main()
