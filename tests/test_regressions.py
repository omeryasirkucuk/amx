from __future__ import annotations

import unittest
from unittest.mock import patch

from amx.agents.base import AgentContext
from amx.agents.code_agent import CodeAgent
from amx.codebase.analyzer import CodebaseReport
from amx.codebase.code_rag import _normalize_source_filter, _source_allowed
from amx.config import DBConfig
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.connector import AssetKind, DatabaseConnector
from amx.docs.rag import RAGStore


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


if __name__ == "__main__":
    unittest.main()
