from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.agents.code_agent import CodeAgent
from amx.agents.orchestrator import Orchestrator
from amx.codebase.analyzer import CodebaseReport
from amx.codebase.code_rag import _normalize_source_filter, _source_allowed
from amx.cli_support.commands.history import format_run_scope
from amx.cli_support.commands.profiles import cmd_use_doc, default_model
from amx.cli_support import inject_session_defaults, session_to_click_args
from amx.config import AMXConfig, DBConfig
from amx.cli_support.commands.db import cmd_profiling
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.connector import AssetKind, DatabaseConnector
from amx.db.connector import ColumnProfile, TableProfile
from amx.docs.rag import RAGStore
from amx.llm.batch import BatchRequest, OpenAIBatchProvider
from amx.cli_support.commands.manual import collect_metadata_coverage


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

    def test_session_to_click_args_maps_manual_shortcuts(self) -> None:
        self.assertEqual(
            session_to_click_args("", ["monitor", "sap"]),
            ["manual", "monitor", "sap"],
        )
        self.assertEqual(
            session_to_click_args("manual", ["edit", "column", "vbeln"]),
            ["manual", "edit", "column", "vbeln"],
        )


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
