from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import click
import pytest

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.agents.code_agent import CodeAgent
from amx.agents.orchestrator import Orchestrator, ReviewResult, apply_review_results_to_db
from amx.cli_support import inject_session_defaults, session_to_click_args
from amx.cli_support.commands.db import (
    cmd_add_profile,
    cmd_profiling,
    cmd_tls,
    databricks_connect_with_recovery,
    interactive_db_block,
)
from amx.cli_support.commands.history import format_run_scope
from amx.cli_support.commands.manual import _run_edit_wizard
from amx.cli_support.commands.profiles import cmd_use_doc, default_model
from amx.cli_support.session import _format_session_click_error, _handle_manual_usage_shortcuts
from amx.codebase.analyzer import CodebaseReport, analyze_codebase
from amx.codebase.code_rag import _normalize_source_filter, _source_allowed
from amx.config import AMXConfig, DBConfig, LLMConfig, normalize_llm_model
from amx.core import AMXApplication, UniversalMetadataAdapter
from amx.core.errors import ErrorMapper
from amx.db.adapters.base import BackendCapabilities, UnsupportedDatabaseOperation
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.adapters.databricks import DatabricksAdapter
from amx.db.adapters.postgresql import PostgreSQLAdapter
from amx.db.adapters.snowflake import SnowflakeAdapter
from amx.db.connector import AssetKind, ColumnProfile, DatabaseConnector, TableProfile
from amx.docs.rag import RAGStore
from amx.docs.scanner import _resolve_github, _resolve_s3, cleanup_scan_artifacts
from amx.llm.batch import BatchRequest, OpenAIBatchProvider
from amx.llm.provider import LLMProvider, logprob_confidence_score
from amx.services.analyze_scope import filter_non_business_assets
from amx.services.manual_metadata import (
    collect_metadata_coverage,
    resolve_manual_target,
    resolve_path_target,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


class CoreArchitectureTests(unittest.TestCase):
    def test_universal_metadata_adapter_normalizes_profile_without_name_rules(self) -> None:
        profile = TableProfile(
            schema="raw",
            name="orders",
            asset_kind=AssetKind.TABLE,
            row_count=12,
            primary_key=["c1"],
            columns=[
                ColumnProfile(
                    name="c1",
                    dtype="INTEGER",
                    nullable=False,
                    row_count=12,
                    null_count=0,
                    distinct_count=12,
                    cardinality_ratio=1.0,
                    samples=[1, 2, 3],
                    existing_comment="Business key from source file",
                )
            ],
        )

        entities = UniversalMetadataAdapter.from_table_profile(profile)

        self.assertEqual(len(entities), 2)
        self.assertEqual(entities[0].path, "raw.orders")
        self.assertEqual(entities[1].path, "raw.orders.c1")
        self.assertEqual(entities[1].structural.dtype, "INTEGER")
        self.assertEqual(entities[1].statistical.samples, (1, 2, 3))
        self.assertEqual(entities[1].semantic.description, "Business key from source file")

    def test_config_nested_write_through_persists_immediately_after_load(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.save(str(cfg_path))

            loaded = AMXConfig.load(str(cfg_path))
            loaded.llm.logprob_high = 0.91

            reloaded = AMXConfig.load(str(cfg_path))
            self.assertAlmostEqual(reloaded.llm.logprob_high, 0.91)

    def test_sqlite_session_state_and_audit_columns_are_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = SQLiteHistoryStore(Path(td) / "history.sqlite3")
            store.init()
            run_id = store.create_run(
                command="test",
                mode="chat",
                db_backend="postgresql",
                db_profile="default",
                llm_provider="unit",
                llm_model="unit-model",
                scope={},
            )
            ids = store.save_run_results(
                run_id,
                [
                    {
                        "schema": "s",
                        "table": "t",
                        "column": "c",
                        "source": "unit",
                        "confidence": "high",
                        "logprob_score": 0.8,
                        "raw_logprob": 0.7,
                        "token_count": 42,
                        "model_version": "unit-model",
                        "alternatives": ["desc"],
                    }
                ],
            )
            store.set_session_state("unit", "agent:ask", {"step": 1})

            rows = store.get_run_results(run_id)
            self.assertEqual(ids[0], rows[0]["id"])
            self.assertEqual(rows[0]["raw_logprob"], 0.7)
            self.assertEqual(rows[0]["token_count"], 42)
            self.assertEqual(rows[0]["model_version"], "unit-model")
            self.assertEqual(store.get_session_state("unit", "agent:ask"), {"step": 1})

    def test_import_amx_exposes_headless_application(self) -> None:
        self.assertTrue(hasattr(AMXApplication, "load"))

    def test_import_amx_init_run_analysis_is_headless_safe(self) -> None:
        import amx

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.save(str(cfg_path))

            result = amx.init(str(cfg_path)).run_analysis()

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "no_scope")

    def test_error_mapper_returns_actionable_postgres_extension_guidance(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError("pg_stat_statements must be loaded via shared_preload_libraries"),
            backend="postgresql",
        )

        self.assertIsNotNone(mapped)
        self.assertIn("CREATE EXTENSION", mapped.render())

    def test_error_mapper_categorises_postgres_auth_failure(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError('FATAL: password authentication failed for user "alice"'),
            backend="postgresql",
        )
        self.assertIsNotNone(mapped)
        rendered = mapped.render()
        self.assertIn("authentication failed", rendered.lower())
        self.assertIn("/add-db-profile", rendered)

    def test_error_mapper_categorises_databricks_invalid_token(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError("401 Client Error: Unauthorized — invalid access token"),
            backend="databricks",
        )
        self.assertIsNotNone(mapped)
        rendered = mapped.render()
        self.assertIn("authentication failed", rendered.lower())
        self.assertIn("Databricks", rendered)

    def test_error_mapper_categorises_network_unreachable(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError(
                "could not connect to server: Connection refused — Is the server running on host db.example.com (10.1.2.3)?"
            ),
            backend="postgresql",
        )
        self.assertIsNotNone(mapped)
        rendered = mapped.render()
        self.assertIn("network unreachable", rendered.lower())
        self.assertIn("VPN", rendered)

    def test_error_mapper_categorises_dns_failure(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError("getaddrinfo failed: Name or service not known"),
            backend="snowflake",
        )
        self.assertIsNotNone(mapped)
        self.assertIn("network unreachable", mapped.render().lower())

    def test_error_mapper_categorises_ssl_handshake_failure(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError(
                "SSL: CERTIFICATE_VERIFY_FAILED [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"
            ),
            backend="snowflake",
        )
        self.assertIsNotNone(mapped)
        rendered = mapped.render()
        self.assertIn("TLS", rendered)
        self.assertIn("CA bundle", rendered)

    def test_error_mapper_databricks_specific_tls_branch_still_wins(self) -> None:
        """The Databricks-specific TLS message must keep firing for backwards
        compat — it points at the per-profile tls_trusted_ca_file setting which
        is more specific than the generic SSL hint."""
        mapped = ErrorMapper.map(
            RuntimeError("certificate_verify_failed self-signed certificate"),
            backend="databricks",
        )
        self.assertIsNotNone(mapped)
        self.assertIn("Databricks TLS", mapped.title)

    def test_error_mapper_categorises_missing_database(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError('database "orders" does not exist'),
            backend="postgresql",
        )
        self.assertIsNotNone(mapped)
        rendered = mapped.render()
        self.assertIn("database not found", rendered.lower())
        self.assertIn("active profile", rendered)

    def test_error_mapper_returns_none_when_unknown(self) -> None:
        mapped = ErrorMapper.map(
            RuntimeError("This is some random non-categorised internal error"),
            backend="postgresql",
        )
        self.assertIsNone(mapped)


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

    def test_rag_reranker_prioritizes_explanatory_chunks(self) -> None:
        store = RAGStore.__new__(RAGStore)
        hits = [
            {"text": "CUSTOMER_ID\nCUSTOMER_ID\nCUSTOMER_ID", "distance": 0.1, "metadata": {}},
            {
                "text": "Customer identifier is used to join orders to account records because it maps customer ownership.",
                "distance": 0.4,
                "metadata": {},
            },
        ]

        reranked = store.rerank("customer identifier join", hits)

        self.assertIn("because", reranked[0]["text"])

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
                return_value=[
                    {"text": "spark.read.table('sap.vbak')", "metadata": {}, "distance": 0.1}
                ],
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

    def test_connector_exposes_backend_capabilities(self) -> None:
        db = DatabaseConnector(DBConfig(backend="bigquery", project="p", dataset="d"))

        self.assertFalse(db.capabilities.database_comments)
        self.assertTrue(db.capabilities.column_comments)
        self.assertTrue(db.capabilities.sampled_profiling)

    def test_connector_blocks_unsupported_database_comment_before_connecting(self) -> None:
        db = DatabaseConnector(DBConfig(backend="bigquery", project="p", dataset="d"))

        with self.assertRaises(UnsupportedDatabaseOperation):
            db.set_database_comment("Project description")

    @pytest.mark.integration
    def test_apply_flow_does_not_count_unsupported_writeback_as_applied(self) -> None:
        db = DatabaseConnector(DBConfig(backend="bigquery", project="p", dataset="d"))
        row = ReviewResult(
            schema="",
            table="",
            column=None,
            final_description="Project description",
            confidence=Confidence.HIGH,
            source="manual",
            applied=True,
            asset_kind=AssetKind.DATABASE.value,
        )
        applied_rows: list[ReviewResult] = []

        applied = apply_review_results_to_db(db, [row], on_applied=applied_rows.append)

        self.assertEqual(applied, 0)
        self.assertEqual(applied_rows, [])

    def test_apply_flow_reuses_single_transaction_connection(self) -> None:
        executed: list[tuple[str, dict[str, object]]] = []
        begin_calls = 0

        class FakeConnection:
            def execute(self, stmt, params):
                executed.append((str(stmt), params))

        class FakeBegin:
            def __enter__(self):
                nonlocal begin_calls
                begin_calls += 1
                return FakeConnection()

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def begin(self):
                return FakeBegin()

        db = DatabaseConnector(DBConfig(backend="postgresql", database="sap"))
        db._engine = FakeEngine()

        rows = [
            ReviewResult(
                schema="public",
                table="orders",
                column=None,
                final_description="Order header",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
                asset_kind=AssetKind.TABLE.value,
            ),
            ReviewResult(
                schema="public",
                table="orders",
                column="id",
                final_description="Order identifier",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
                asset_kind=AssetKind.TABLE.value,
            ),
        ]

        applied = apply_review_results_to_db(db, rows)

        self.assertEqual(applied, 2)
        self.assertEqual(begin_calls, 1)
        self.assertEqual(len(executed), 2)

    def test_apply_flow_calls_failed_callback_for_writeback_errors(self) -> None:
        db = DatabaseConnector(DBConfig(backend="postgresql", database="sap"))
        failed: list[tuple[int | None, str]] = []

        class FakeConnection:
            pass

        class FakeBegin:
            def __enter__(self):
                return FakeConnection()

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def begin(self):
                return FakeBegin()

        db._engine = FakeEngine()

        def fail_apply_comment(**kwargs):
            raise RuntimeError("writeback failed")

        db.apply_comment = fail_apply_comment  # type: ignore[method-assign]

        row = ReviewResult(
            schema="public",
            table="orders",
            column="id",
            final_description="Order identifier",
            confidence=Confidence.HIGH,
            source="manual",
            applied=True,
            asset_kind=AssetKind.TABLE.value,
            result_id=42,
        )

        applied = apply_review_results_to_db(
            db,
            [row],
            on_failed=lambda result, exc: failed.append((result.result_id, str(exc))),
        )

        self.assertEqual(applied, 0)
        self.assertEqual(failed, [(42, "writeback failed")])

    def test_apply_flow_batches_adjacent_databricks_column_comments(self) -> None:
        class FakeBegin:
            def __enter__(self):
                return object()

            def __exit__(self, exc_type, exc, tb):
                return False

        class FakeEngine:
            def begin(self):
                return FakeBegin()

        db = DatabaseConnector(DBConfig(backend="databricks", catalog="main"))
        db._engine = FakeEngine()
        batched: list[list[tuple[str, str]]] = []
        singles: list[str] = []

        def fake_batch(schema, table, comments, *, conn=None):
            batched.append(comments)
            return True

        def fake_single(**kwargs):
            singles.append(kwargs["column"])

        db.apply_column_comments_batch = fake_batch  # type: ignore[method-assign]
        db.apply_comment = fake_single  # type: ignore[method-assign]

        rows = [
            ReviewResult(
                schema="sales",
                table="orders",
                column="id",
                final_description="Order identifier",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
                asset_kind=AssetKind.TABLE.value,
            ),
            ReviewResult(
                schema="sales",
                table="orders",
                column="status",
                final_description="Order status",
                confidence=Confidence.HIGH,
                source="manual",
                applied=True,
                asset_kind=AssetKind.TABLE.value,
            ),
        ]

        applied = apply_review_results_to_db(db, rows)

        self.assertEqual(applied, 2)
        self.assertEqual(
            batched,
            [[("id", "Order identifier"), ("status", "Order status")]],
        )
        self.assertEqual(singles, [])

    def test_comment_sql_generation_per_backend(self) -> None:
        pg = PostgreSQLAdapter(DBConfig(backend="postgresql", database="sap"))
        sf = SnowflakeAdapter(DBConfig(backend="snowflake", database="sap"))
        dbx = DatabricksAdapter(DBConfig(backend="databricks", catalog="main"))
        bq = BigQueryAdapter(DBConfig(backend="bigquery", project="p", dataset="d"))

        self.assertEqual(
            pg.set_table_comment_sql("public", "orders", "MATERIALIZED VIEW"),
            'COMMENT ON MATERIALIZED VIEW "public"."orders" IS :cmt',
        )
        self.assertEqual(
            sf.set_column_comment_sql("PUBLIC", "ORDERS", "ID"),
            'COMMENT ON COLUMN "PUBLIC"."ORDERS"."ID" IS :cmt',
        )
        self.assertEqual(
            dbx.set_database_comment_sql(),
            "COMMENT ON CATALOG `main` IS :cmt",
        )
        self.assertEqual(
            dbx.set_multi_column_comments_sql(
                "sales",
                "orders",
                [("id", "Order identifier"), ("status", "Order status")],
            ),
            "ALTER TABLE `main`.`sales`.`orders` ALTER COLUMN `id` COMMENT 'Order identifier', `status` COMMENT 'Order status'",
        )
        self.assertEqual(
            bq.set_schema_comment_sql("sales"),
            "ALTER SCHEMA `p`.`sales` SET OPTIONS(description = :cmt)",
        )

    def test_databricks_database_comment_without_catalog_fails(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks", catalog=""))

        with self.assertRaises(UnsupportedDatabaseOperation):
            adapter.set_database_comment_sql()

    def test_databricks_engine_sets_non_deprecated_user_agent_entry(self) -> None:
        adapter = DatabricksAdapter(
            DBConfig(
                backend="databricks",
                host="workspace.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                access_token="token",
            )
        )
        calls: list[tuple[str, dict[str, object]]] = []

        def fake_create_engine(url: str, **kwargs):
            calls.append((url, kwargs))
            return object()

        with patch("sqlalchemy.create_engine", side_effect=fake_create_engine):
            engine = adapter.create_engine()

        self.assertIsNotNone(engine)
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            calls[0][1]["connect_args"],
            {
                "user_agent_entry": "amx",
                "_socket_timeout": adapter.connect_timeout_seconds,
                "_retry_stop_after_attempts_count": adapter.connect_retry_attempts,
                "_retry_stop_after_attempts_duration": adapter.connect_retry_duration_seconds,
            },
        )

    def test_databricks_engine_passes_tls_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ca_file = Path(tmp) / "corp-ca.pem"
            ca_file.write_text("certificate", encoding="utf-8")
            adapter = DatabricksAdapter(
                DBConfig(
                    backend="databricks",
                    host="workspace.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/abc",
                    access_token="token",
                    tls_no_verify=True,
                    tls_trusted_ca_file=f"$AMX_TEST_CA_DIR/{ca_file.name}",
                )
            )
            calls: list[tuple[str, dict[str, object]]] = []

            def fake_create_engine(url: str, **kwargs):
                calls.append((url, kwargs))
                return object()

            with (
                patch.dict(os.environ, {"AMX_TEST_CA_DIR": tmp}),
                patch("sqlalchemy.create_engine", side_effect=fake_create_engine),
            ):
                adapter.create_engine()

            self.assertEqual(
                calls[0][1]["connect_args"],
                {
                    "user_agent_entry": "amx",
                    "_socket_timeout": adapter.connect_timeout_seconds,
                    "_retry_stop_after_attempts_count": adapter.connect_retry_attempts,
                    "_retry_stop_after_attempts_duration": adapter.connect_retry_duration_seconds,
                    "_tls_no_verify": True,
                    "_tls_trusted_ca_file": str(ca_file),
                },
            )

    def test_databricks_engine_uses_trusted_ca_env_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ca_file = Path(tmp) / "corp-ca.pem"
            ca_file.write_text("certificate", encoding="utf-8")
            adapter = DatabricksAdapter(
                DBConfig(
                    backend="databricks",
                    host="workspace.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/abc",
                    access_token="token",
                )
            )
            calls: list[tuple[str, dict[str, object]]] = []

            def fake_create_engine(url: str, **kwargs):
                calls.append((url, kwargs))
                return object()

            with (
                patch.dict(os.environ, {"AMX_DATABRICKS_TRUSTED_CA_FILE": str(ca_file)}),
                patch("sqlalchemy.create_engine", side_effect=fake_create_engine),
            ):
                adapter.create_engine()

            self.assertEqual(
                calls[0][1]["connect_args"]["_tls_trusted_ca_file"],
                str(ca_file),
            )

    def test_databricks_missing_trusted_ca_file_is_actionable(self) -> None:
        adapter = DatabricksAdapter(
            DBConfig(
                backend="databricks",
                host="workspace.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                access_token="token",
                tls_trusted_ca_file="/tmp/does-not-exist-amx-ca.pem",
            )
        )

        with self.assertRaises(FileNotFoundError) as ctx:
            adapter.create_engine()

        self.assertIn(
            "trusted CA bundle",
            adapter.actionable_profile_error(ctx.exception) or "",
        )

    def test_databricks_engine_disables_insecure_request_warning_only_for_no_verify(self) -> None:
        adapter = DatabricksAdapter(
            DBConfig(
                backend="databricks",
                host="workspace.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                access_token="token",
                tls_no_verify=True,
            )
        )

        with (
            patch("amx.db.adapters.databricks.urllib3.disable_warnings") as disable_warnings,
            patch("sqlalchemy.create_engine", return_value=object()),
        ):
            adapter.create_engine()

        disable_warnings.assert_called_once()


class SQLiteHistoryStoreTests(unittest.TestCase):
    def test_record_db_apply_failure_marks_failed_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SQLiteHistoryStore(Path(tmp) / "history.db")
            store.init()
            with store._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO analysis_runs (
                        started_at, status, command, mode, db_backend, db_profile,
                        llm_provider, llm_model, scope_json, metrics_json, tokens_json, results_json, error_text
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        1.0,
                        "success",
                        "run",
                        "chat",
                        "databricks",
                        "default",
                        "openai",
                        "gpt",
                        "{}",
                        "{}",
                        "{}",
                        "{}",
                        "",
                    ),
                )
                run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                conn.execute(
                    """
                    INSERT INTO run_results (
                        run_id, saved_at, schema_name, table_name, column_name, asset_kind,
                        source, confidence, reasoning, alternatives_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        1.0,
                        "public",
                        "orders",
                        "id",
                        "table",
                        "manual",
                        "high",
                        "",
                        '["Order identifier"]',
                    ),
                )
                result_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            store.record_db_apply_failure(result_id, "permission denied")
            rows = store.get_run_results(run_id)

            self.assertEqual(rows[0]["db_applied_status"], "failed")
            self.assertEqual(rows[0]["rejection_reason"], "permission denied")

    def test_databricks_ssl_error_is_actionable(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks"))

        message = adapter.actionable_profile_error(
            Exception(
                "SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] self-signed certificate in certificate chain"
            )
        )

        self.assertIsNotNone(message)
        self.assertIn("trusted CA bundle path", message)

    def test_databricks_invalid_token_error_is_actionable(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks"))

        message = adapter.actionable_profile_error(
            Exception("Error during request to server: : Invalid access token.")
        )

        self.assertIsNotNone(message)
        self.assertIn("access token is invalid", message)

    def test_connector_logs_actionable_databricks_tls_error(self) -> None:
        db = DatabaseConnector(DBConfig(backend="databricks"))

        class FakeAdapter:
            name = "databricks"

            def test_connection(self, engine=None):
                raise Exception(
                    "SSLCertVerificationError: self-signed certificate in certificate chain"
                )

            def actionable_profile_error(self, exc):
                return DatabricksAdapter(DBConfig(backend="databricks")).actionable_profile_error(
                    exc
                )

        db._adapter = FakeAdapter()

        with patch("amx.db.connector.log.error") as log_error:
            ok = db.test_connection()

        self.assertFalse(ok)
        log_error.assert_called_once()
        self.assertIn("trusted CA bundle path", log_error.call_args.args[1])

    def test_databricks_test_connection_uses_native_connector(self) -> None:
        adapter = DatabricksAdapter(
            DBConfig(
                backend="databricks",
                host="workspace.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/abc",
                access_token="token",
            )
        )
        calls: list[dict[str, object]] = []

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, stmt: str) -> None:
                calls.append({"stmt": stmt})

            def fetchall(self):
                return [(1,)]

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def cursor(self):
                return FakeCursor()

        def fake_connect(*, server_hostname, http_path, access_token=None, **kwargs):
            calls.append(
                {
                    "server_hostname": server_hostname,
                    "http_path": http_path,
                    "access_token": access_token,
                    "kwargs": kwargs,
                }
            )
            return FakeConnection()

        with patch("databricks.sql.connect", side_effect=fake_connect):
            adapter.test_connection()

        self.assertEqual(calls[0]["server_hostname"], "workspace.cloud.databricks.com")
        self.assertEqual(calls[0]["http_path"], "/sql/1.0/warehouses/abc")
        self.assertEqual(calls[0]["access_token"], "token")
        self.assertEqual(calls[1]["stmt"], "SELECT 1")

    def test_databricks_rejects_materialized_view_comments(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks", catalog="main"))

        with self.assertRaises(UnsupportedDatabaseOperation):
            adapter.set_table_comment_sql("sales", "mv_orders", "MATERIALIZED VIEW")

    def test_databricks_comment_sql_inlines_literal_for_ddl(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks", catalog="main"))

        sql, params = adapter.comment_sql_with_params(
            "COMMENT ON TABLE `main`.`sales`.`orders` IS :cmt",
            "Customer's order table",
        )

        self.assertEqual(
            sql,
            "COMMENT ON TABLE `main`.`sales`.`orders` IS 'Customer''s order table'",
        )
        self.assertEqual(params, {})

    def test_snowflake_metadata_uses_safe_show_and_mapping_rows(self) -> None:
        executed: list[tuple[str, dict | None]] = []

        class FakeRow(tuple):
            def __new__(cls, values, mapping):
                obj = tuple.__new__(cls, values)
                obj.mapping = mapping
                return obj

            @property
            def _mapping(self):
                return self.mapping

        class FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, stmt, params=None):
                sql = str(stmt)
                executed.append((sql, params))
                if sql.startswith("SHOW MATERIALIZED VIEWS"):
                    return SimpleNamespace(
                        fetchall=lambda: [FakeRow((None, "fallback_name"), {"name": "mv_orders"})]
                    )
                if sql.startswith("SHOW DATABASES"):
                    return SimpleNamespace(
                        fetchall=lambda: [FakeRow((), {"comment": "Warehouse comment"})]
                    )
                raise AssertionError(sql)

        class FakeEngine:
            def connect(self):
                return FakeConn()

        adapter = SnowflakeAdapter(DBConfig(backend="snowflake", database="SAP"))

        self.assertEqual(adapter.list_materialized_views(FakeEngine(), "Sales"), ["mv_orders"])
        self.assertEqual(adapter.get_database_comment(FakeEngine()), "Warehouse comment")
        self.assertIn('SHOW MATERIALIZED VIEWS IN SCHEMA "Sales"', executed[0][0])
        self.assertIsNone(executed[0][1])
        self.assertIn("SHOW DATABASES LIKE 'SAP'", executed[1][0])


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

    def test_upsert_active_db_profile_replaces_active_db_object(self) -> None:
        cfg = AMXConfig()
        original = DBConfig(backend="postgresql", host="localhost", database="SAP")
        updated = DBConfig(
            backend="databricks",
            host="adb-1234567890123456.7.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/1234567890abcdef",
            access_token="token",
            catalog="my_catalog",
            database="dev",
            tls_no_verify=True,
        )
        cfg.db = original
        cfg.db_profiles = {"databricks-default": original}
        cfg.active_db_profile = "databricks-default"

        cfg.upsert_db_profile("databricks-default", updated)

        self.assertIs(cfg.db, updated)
        self.assertEqual(cfg.db.backend, "databricks")
        self.assertEqual(cfg.db_profiles["databricks-default"].backend, "databricks")

    def test_cmd_add_profile_overwrites_active_profile_atomically(self) -> None:
        cfg = AMXConfig()
        original = DBConfig(backend="postgresql", host="localhost", database="SAP")
        updated = DBConfig(
            backend="databricks",
            host="adb-1234567890123456.7.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/1234567890abcdef",
            access_token="token",
            catalog="my_catalog",
            database="dev",
            tls_no_verify=True,
        )
        cfg.db = original
        cfg.db_profiles = {"databricks-default": original}
        cfg.active_db_profile = "databricks-default"

        with patch("amx.cli_support.commands.db.interactive_db_block", return_value=updated):
            cmd_add_profile(cfg, ["databricks-default"])

        self.assertIs(cfg.db, updated)
        self.assertEqual(cfg.db.backend, "databricks")
        self.assertEqual(cfg.db_profiles["databricks-default"].backend, "databricks")

    def test_databricks_connect_recovery_persists_env_ca_bundle(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(
            backend="databricks", host="workspace", http_path="/sql", access_token="token"
        )
        cfg.db_profiles = {"corp": cfg.db}
        cfg.active_db_profile = "corp"
        cfg.save = lambda: "/tmp/amx-test-config.yml"  # type: ignore[method-assign]

        calls: list[DBConfig] = []

        with tempfile.TemporaryDirectory() as tmp:
            ca_file = Path(tmp) / "corp.pem"
            ca_file.write_text("certificate", encoding="utf-8")

            def fake_connect(db_cfg: DBConfig) -> tuple[bool, str]:
                calls.append(db_cfg)
                if db_cfg.tls_trusted_ca_file == str(ca_file):
                    return True, ""
                return False, "TLS certificate validation failed."

            with patch.dict(os.environ, {"AMX_DATABRICKS_TRUSTED_CA_FILE": str(ca_file)}):
                ok, attempts = databricks_connect_with_recovery(cfg, fake_connect)

        self.assertTrue(ok)
        self.assertEqual(
            [attempt.label for attempt in attempts],
            ["saved profile", "env CA bundle (AMX_DATABRICKS_TRUSTED_CA_FILE)"],
        )
        self.assertEqual(cfg.db.tls_trusted_ca_file, str(ca_file))
        self.assertFalse(cfg.db.tls_no_verify)
        self.assertEqual(calls[-1].tls_trusted_ca_file, str(ca_file))

    def test_databricks_connect_recovery_persists_tls_no_verify_last(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(
            backend="databricks", host="workspace", http_path="/sql", access_token="token"
        )
        cfg.db_profiles = {"corp": cfg.db}
        cfg.active_db_profile = "corp"
        cfg.save = lambda: "/tmp/amx-test-config.yml"  # type: ignore[method-assign]

        calls: list[DBConfig] = []

        def fake_connect(db_cfg: DBConfig) -> tuple[bool, str]:
            calls.append(db_cfg)
            if db_cfg.tls_no_verify:
                return True, ""
            return False, "TLS certificate validation failed."

        ok, attempts = databricks_connect_with_recovery(cfg, fake_connect)

        self.assertTrue(ok)
        self.assertEqual(
            [attempt.label for attempt in attempts], ["saved profile", "TLS no-verify fallback"]
        )
        self.assertTrue(cfg.db.tls_no_verify)
        self.assertTrue(calls[-1].tls_no_verify)

    def test_cli_tls_updates_active_databricks_profile(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(backend="databricks", tls_no_verify=False, tls_trusted_ca_file="")
        cfg.db_profiles = {"default": cfg.db}
        cfg.active_db_profile = "default"
        cfg.save = lambda: "/tmp/amx-test-config.yml"  # type: ignore[method-assign]

        cmd_tls(cfg, ["on"])

        self.assertTrue(cfg.db.tls_no_verify)
        self.assertTrue(cfg.db_profiles["default"].tls_no_verify)

        cmd_tls(cfg, ["off", "clear"])

        self.assertFalse(cfg.db.tls_no_verify)
        self.assertEqual(cfg.db.tls_trusted_ca_file, "")

    def test_interactive_databricks_profile_edit_is_deterministic(self) -> None:
        defaults = DBConfig(
            backend="databricks",
            host="old-host",
            http_path="/sql/old",
            access_token="old-token",
            catalog="oldcat",
            database="olddb",
            tls_no_verify=False,
            tls_trusted_ca_file="/tmp/old.pem",
        )

        ask_values = iter(["databricks", "new-host", "/sql/new", "newcat", "-", "-"])
        secret_values = iter(["new-token"])
        choice_values = iter(["yes"])

        with (
            patch(
                "amx.cli_support.commands.db.ask_choice",
                side_effect=lambda *args, **kwargs: (
                    next(ask_values)
                    if args and "Select database backend" in args[0]
                    else next(choice_values)
                ),
            ),
            patch(
                "amx.cli_support.commands.db.ask",
                side_effect=lambda *args, **kwargs: next(ask_values),
            ),
            patch(
                "amx.cli_support.commands.db.ask_password",
                side_effect=lambda *args, **kwargs: next(secret_values),
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.host, "new-host")
        self.assertEqual(updated.http_path, "/sql/new")
        self.assertEqual(updated.access_token, "new-token")
        self.assertEqual(updated.catalog, "newcat")
        self.assertEqual(updated.database, "")
        self.assertEqual(updated.tls_trusted_ca_file, "")
        self.assertTrue(updated.tls_no_verify)

    def test_interactive_databricks_profile_edit_keeps_existing_on_blank(self) -> None:
        defaults = DBConfig(
            backend="databricks",
            host="old-host",
            http_path="/sql/old",
            access_token="old-token",
            catalog="oldcat",
            database="olddb",
            tls_no_verify=True,
            tls_trusted_ca_file="/tmp/old.pem",
        )

        ask_values = iter(["databricks", "", "", "", "", ""])
        secret_values = iter([""])
        choice_values = iter(["no"])

        with (
            patch(
                "amx.cli_support.commands.db.ask_choice",
                side_effect=lambda *args, **kwargs: (
                    next(ask_values)
                    if args and "Select database backend" in args[0]
                    else next(choice_values)
                ),
            ),
            patch(
                "amx.cli_support.commands.db.ask",
                side_effect=lambda *args, **kwargs: next(ask_values),
            ),
            patch(
                "amx.cli_support.commands.db.ask_password",
                side_effect=lambda *args, **kwargs: next(secret_values),
            ),
        ):
            updated = interactive_db_block(defaults)

        self.assertEqual(updated.host, "old-host")
        self.assertEqual(updated.http_path, "/sql/old")
        self.assertEqual(updated.access_token, "old-token")
        self.assertEqual(updated.catalog, "oldcat")
        self.assertEqual(updated.database, "olddb")
        self.assertEqual(updated.tls_trusted_ca_file, "/tmp/old.pem")
        self.assertFalse(updated.tls_no_verify)

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
                return [
                    {"name": "id", "type": "INTEGER", "nullable": False, "comment": "Identifier"}
                ]

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

    def test_sampled_mode_uses_samples_without_column_stats(self) -> None:
        executed: list[str] = []

        class FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, stmt, params=None):
                executed.append(str(stmt))
                return SimpleNamespace(fetchall=lambda: [("A",), ("B",)])

        class FakeEngine:
            def connect(self):
                return FakeConn()

        class FakeAdapter:
            name = "fake"
            capabilities = BackendCapabilities(row_count_stats=True)

            def fully_qualified_name(self, schema: str, table: str) -> str:
                return f'"{schema}"."{table}"'

            def quote_identifier(self, name: str) -> str:
                return f'"{name}"'

            def get_table_stats(self, engine, schema: str, table: str) -> dict[str, int]:
                return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 42}

            def get_schema_comment(self, engine, schema: str):
                return None

            def get_database_comment(self, engine):
                return None

            def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
                raise AssertionError("sampled mode must not run full column stats")

            def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
                return f"SAMPLE_SQL {fqn}.{quoted_col}"

            def get_incoming_foreign_keys(self, engine, schema: str, table: str):
                return []

        class FakeInspector:
            def get_table_comment(self, table: str, schema: str):
                return {"text": ""}

            def get_columns(self, table: str, schema: str):
                return [{"name": "code", "type": "TEXT", "nullable": True, "comment": None}]

            def get_pk_constraint(self, table: str, schema: str):
                return {}

            def get_foreign_keys(self, table: str, schema: str):
                return []

            def get_unique_constraints(self, table: str, schema: str):
                return []

            def get_check_constraints(self, table: str, schema: str):
                return []

        db = object.__new__(DatabaseConnector)
        db.cfg = DBConfig(backend="postgresql", profiling_mode="sampled", profiling_sample_size=2)
        db._engine = FakeEngine()
        db._adapter = FakeAdapter()

        with patch("amx.db.connector.inspect", return_value=FakeInspector()):
            profile = db.profile_table("public", "orders", asset_kind=AssetKind.TABLE)

        self.assertEqual(profile.row_count, 42)
        self.assertEqual(profile.columns[0].samples, ["A", "B"])
        self.assertEqual(profile.columns[0].distinct_count, 0)
        self.assertEqual(executed, ['SAMPLE_SQL "public"."orders"."code"'])

    def test_full_mode_blocks_column_scans_when_cloud_row_count_unknown(self) -> None:
        class FakeEngine:
            def connect(self):
                raise AssertionError("unknown cloud row count should not trigger full table scans")

        class FakeAdapter:
            name = "fake-cloud"
            capabilities = BackendCapabilities(
                row_count_stats=True,
                full_scan_when_row_count_unknown=False,
            )

            def fully_qualified_name(self, schema: str, table: str) -> str:
                return f"`{schema}`.`{table}`"

            def quote_identifier(self, name: str) -> str:
                return f"`{name}`"

            def get_table_stats(self, engine, schema: str, table: str) -> dict[str, int]:
                return {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0}

            def get_schema_comment(self, engine, schema: str):
                return None

            def get_database_comment(self, engine):
                return None

            def column_stats_sql(self, fqn: str, quoted_col: str) -> str:
                raise AssertionError("full stats must be blocked")

            def column_sample_sql(self, fqn: str, quoted_col: str) -> str:
                raise AssertionError("samples must be blocked in this test")

            def get_incoming_foreign_keys(self, engine, schema: str, table: str):
                return []

        class FakeInspector:
            def get_table_comment(self, table: str, schema: str):
                return {"text": ""}

            def get_columns(self, table: str, schema: str):
                return [{"name": "id", "type": "INT", "nullable": False, "comment": None}]

            def get_pk_constraint(self, table: str, schema: str):
                return {}

            def get_foreign_keys(self, table: str, schema: str):
                return []

            def get_unique_constraints(self, table: str, schema: str):
                return []

            def get_check_constraints(self, table: str, schema: str):
                return []

        db = object.__new__(DatabaseConnector)
        db.cfg = DBConfig(
            backend="bigquery",
            profiling_mode="full",
            profiling_max_rows=1_000_000,
            profiling_sample_size=0,
        )
        db._engine = FakeEngine()
        db._adapter = FakeAdapter()

        with patch("amx.db.connector.inspect", return_value=FakeInspector()):
            profile = db.profile_table("sales", "orders", asset_kind=AssetKind.TABLE)

        self.assertEqual(profile.row_count, 0)
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

    @pytest.mark.integration
    def test_openrouter_provider_does_not_reprefix_normalized_model(self) -> None:
        provider = LLMProvider.__new__(LLMProvider)
        provider.cfg = SimpleNamespace(provider="openrouter", model="qwen/qwen3.6-plus")
        self.assertEqual(provider.model_name, "qwen/qwen3.6-plus")

    def test_openrouter_model_normalization_recovers_common_provider_typo(self) -> None:
        self.assertEqual(
            normalize_llm_model("openrouter", "oepnai/gpt-4o-mini"),
            "openai/gpt-4o-mini",
        )

    def test_openai_model_normalization_recovers_duplicate_provider_typo(self) -> None:
        self.assertEqual(
            normalize_llm_model("openai", "oepnai/gpt-4o-mini"),
            "gpt-4o-mini",
        )

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
        target = resolve_manual_target(
            cfg, db, "column", ["sap_test.adr6.smtp_addr"], error=errors.append
        )

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
        target = resolve_path_target(
            cfg, db, "warehouse", "warehouse.sap_test.adr6.smtp_addr", error=errors.append
        )

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

    @pytest.mark.integration
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

    def test_whole_response_logprob_ignores_json_structure(self) -> None:
        response = '{"description":"Risky generated description","confidence":"HIGH"}'
        logprobs = []
        pos = 0
        desc = "Risky generated description"
        while pos < len(response):
            if response.startswith(desc, pos):
                logprobs.append({"token": desc, "logprob": -2.0})
                pos += len(desc)
            else:
                logprobs.append({"token": response[pos], "logprob": -0.001})
                pos += 1

        score = logprob_confidence_score(logprobs)

        self.assertIsNotNone(score)
        self.assertLess(score or 1.0, 0.2)


class BatchLogprobTests(unittest.TestCase):
    def test_openai_batch_requests_logprobs(self) -> None:
        req = BatchRequest(
            custom_id="profile:public:orders:0",
            messages=[{"role": "user", "content": "Describe columns"}],
        )

        body = json.loads(
            OpenAIBatchProvider._build_jsonl([req], "gpt-4o-mini").decode().splitlines()[0]
        )["body"]

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

    @pytest.mark.integration
    def test_process_table_surfaces_profile_agent_diagnostics(self) -> None:
        class NullStep:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class DummyDB:
            cfg = SimpleNamespace(database="main", catalog="", project="")
            backend = "postgresql"
            stats_label = "stats"

            def profile_table(self, schema, table, asset_kind=None):
                return TableProfile(
                    schema=schema,
                    name=table,
                    columns=[ColumnProfile(name="id", dtype="INTEGER", nullable=False)],
                )

        class DummyLLM:
            cfg = SimpleNamespace(
                language="english",
                logprob_high=0.85,
                logprob_medium=0.50,
                column_batch_size=10,
                n_alternatives=3,
                prompt_detail_cfg=None,
            )

        orch = Orchestrator(DummyDB(), DummyLLM())
        orch.profile_agent.run = lambda ctx: []
        orch.profile_agent.consume_diagnostics = lambda: [
            "Profile Agent failed: upstream model is unavailable"
        ]

        warnings: list[str] = []
        with (
            patch("amx.agents.orchestrator.step_spinner", return_value=NullStep()),
            patch("amx.agents.orchestrator.heading"),
            patch("amx.agents.orchestrator.info"),
            patch("amx.agents.orchestrator.warn", side_effect=warnings.append),
        ):
            results = orch.process_table("public", "orders", interactive_review=False)

        self.assertEqual(results, [])
        self.assertIn("Profile Agent failed: upstream model is unavailable", warnings)


class LLMProviderTests(unittest.TestCase):
    def test_litellm_loggers_are_silenced_by_default(self) -> None:
        saved = {}
        for name in ("LiteLLM", "litellm"):
            logger = logging.getLogger(name)
            saved[name] = (list(logger.handlers), logger.propagate, logger.level, logger.disabled)
            logger.handlers.clear()
            logger.propagate = True
            logger.setLevel(logging.NOTSET)
            logger.disabled = False

        try:
            import amx.llm.provider as provider_module

            provider_module._litellm_module = None
            fake_litellm = SimpleNamespace()
            with patch.dict(sys.modules, {"litellm": fake_litellm}):
                loaded = provider_module._litellm()

            self.assertIs(loaded, fake_litellm)
            for name in ("LiteLLM", "litellm"):
                logger = logging.getLogger(name)
                self.assertFalse(logger.propagate)
                self.assertGreater(logger.level, logging.CRITICAL)
                self.assertTrue(any(isinstance(h, logging.NullHandler) for h in logger.handlers))
        finally:
            import amx.llm.provider as provider_module

            provider_module._litellm_module = None
            for name, (handlers, propagate, level, disabled) in saved.items():
                logger = logging.getLogger(name)
                logger.handlers.clear()
                for handler in handlers:
                    logger.addHandler(handler)
                logger.propagate = propagate
                logger.setLevel(level)
                logger.disabled = disabled


class SecretKeychainTests(unittest.TestCase):
    """Regression tests for Week-2 keyring-backed secret storage."""

    def setUp(self) -> None:
        from amx.storage.secrets import InMemorySecretStore, set_default_store

        self._store = InMemorySecretStore()
        set_default_store(self._store)

    def tearDown(self) -> None:
        from amx.storage.secrets import set_default_store

        set_default_store(None)

    def test_save_externalises_db_password_to_keyring(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.db_profiles = {
                "prod": DBConfig(
                    backend="postgresql",
                    host="db.prod.example.com",
                    user="alice",
                    password="super-secret",
                    database="orders",
                )
            }
            cfg.active_db_profile = "prod"
            cfg.db = cfg.db_profiles["prod"]
            cfg.save(str(cfg_path))

            # Plaintext must NOT be in the YAML — only the reference.
            yaml_text = cfg_path.read_text()
            self.assertNotIn("super-secret", yaml_text)
            self.assertIn("keyring:db_profiles/prod/password", yaml_text)
            # The actual secret lives in the (in-memory) keyring.
            self.assertEqual(self._store.get("db_profiles/prod/password"), "super-secret")

    def test_load_resolves_keyring_reference_back_to_plaintext(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.db_profiles = {
                "prod": DBConfig(
                    backend="postgresql",
                    host="db.prod.example.com",
                    user="alice",
                    password="super-secret",
                    database="orders",
                )
            }
            cfg.active_db_profile = "prod"
            cfg.db = cfg.db_profiles["prod"]
            cfg.save(str(cfg_path))

            reloaded = AMXConfig.load(str(cfg_path))
            self.assertEqual(reloaded.db_profiles["prod"].password, "super-secret")
            self.assertEqual(reloaded.db.password, "super-secret")

    def test_legacy_plaintext_password_still_loads(self) -> None:
        """Existing user configs with plaintext passwords must keep working
        without manual migration. The next save promotes them to the keyring."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "active_db_profile: legacy\n"
                "db:\n"
                "  backend: postgresql\n"
                "  host: db.example.com\n"
                "  user: alice\n"
                "  password: legacy-plain\n"
                "  database: orders\n"
                "  port: 5432\n"
                "db_profiles:\n"
                "  legacy:\n"
                "    backend: postgresql\n"
                "    host: db.example.com\n"
                "    user: alice\n"
                "    password: legacy-plain\n"
                "    database: orders\n"
                "    port: 5432\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.db_profiles["legacy"].password, "legacy-plain")

            # Saving migrates the secret into the keyring.
            cfg.save(str(cfg_path))
            self.assertEqual(self._store.get("db_profiles/legacy/password"), "legacy-plain")
            yaml_text = cfg_path.read_text()
            self.assertNotIn("legacy-plain", yaml_text)

    def test_missing_keyring_entry_resolves_to_empty_string(self) -> None:
        """A reference whose key has been deleted from the keyring should not
        crash the loader; the field becomes empty so the user can be prompted
        to re-enter the secret."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "active_db_profile: ghost\n"
                "db_profiles:\n"
                "  ghost:\n"
                "    backend: postgresql\n"
                "    host: h\n"
                "    user: u\n"
                "    database: d\n"
                "    port: 5432\n"
                "    password: keyring:db_profiles/ghost/password\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.db_profiles["ghost"].password, "")

    def test_llm_api_key_externalised_separately_from_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            from amx.config import LLMConfig

            cfg.llm_profiles = {
                "main": LLMConfig(
                    provider="openai",
                    model="gpt-4o-mini",
                    api_key="sk-test-1234",
                )
            }
            cfg.active_llm_profile = "main"
            from dataclasses import replace as dc_replace

            cfg.llm = dc_replace(cfg.llm_profiles["main"])
            cfg.save(str(cfg_path))

            self.assertNotIn("sk-test-1234", cfg_path.read_text())
            self.assertEqual(self._store.get("llm_profiles/main/api_key"), "sk-test-1234")

    def test_null_store_keeps_plaintext_when_keyring_unavailable(self) -> None:
        """If the OS has no keyring backend, secrets stay in plaintext rather
        than silently disappearing."""
        from amx.storage.secrets import NullSecretStore, set_default_store

        set_default_store(NullSecretStore())
        try:
            with tempfile.TemporaryDirectory() as td:
                cfg_path = Path(td) / "config.yml"
                cfg = AMXConfig()
                cfg.db_profiles = {
                    "prod": DBConfig(
                        backend="postgresql",
                        host="db.prod.example.com",
                        user="alice",
                        password="still-plaintext",
                        database="orders",
                    )
                }
                cfg.active_db_profile = "prod"
                cfg.db = cfg.db_profiles["prod"]
                cfg.save(str(cfg_path))

                yaml_text = cfg_path.read_text()
                self.assertIn("still-plaintext", yaml_text)
                self.assertNotIn("keyring:", yaml_text)
        finally:
            # Restore the in-memory store the conftest fixture set up.
            set_default_store(self._store)


class EmbeddingProviderTests(unittest.TestCase):
    """Regression tests for the Week-3 pluggable embedding providers."""

    def test_minilm_default_returns_none(self) -> None:
        """The MiniLM kind hands ``None`` back so callers can pass it to
        Chroma unchanged and get the historical bundled default."""
        from amx.search.embeddings import make_embedding_function

        for kind in ("minilm", "default", "minilm-l6-v2", "", "MiniLM"):
            self.assertIsNone(make_embedding_function(kind))

    def test_openai_compatible_requires_model(self) -> None:
        from amx.search.embeddings import make_embedding_function

        with self.assertRaises(ValueError):
            make_embedding_function("openai_compatible", model="")

    def test_unknown_kind_raises_value_error(self) -> None:
        from amx.search.embeddings import make_embedding_function

        with self.assertRaises(ValueError):
            make_embedding_function("magic-ai-7b")

    def test_openai_compatible_invokes_client_with_provided_args(self) -> None:
        """Chroma's EmbeddingFunction base class normalises the subclass return
        value into numpy arrays before handing it to callers, so the assertions
        compare element-by-element rather than expecting plain Python lists."""
        from amx.search.embeddings import OpenAICompatibleEmbedding

        captured: dict[str, object] = {}

        class FakeEmbeddings:
            @staticmethod
            def create(*, model: str, input: list[str]) -> SimpleNamespace:
                captured["call"] = {"model": model, "input": list(input)}
                return SimpleNamespace(
                    data=[SimpleNamespace(embedding=[0.1, 0.2, 0.3]) for _ in input]
                )

        class FakeClient:
            embeddings = FakeEmbeddings()

        def fake_factory(*, api_key: str, base_url: str, timeout: float | None) -> FakeClient:
            captured["init"] = {"api_key": api_key, "base_url": base_url, "timeout": timeout}
            return FakeClient()

        with patch("amx.search.embeddings._openai_client_factory", fake_factory):
            ef = OpenAICompatibleEmbedding(
                model="text-embedding-3-small",
                api_key="sk-test",
                base_url="https://api.example.com/v1",
            )
            vectors = ef(["hello", "world"])

        self.assertEqual(len(vectors), 2)
        # Chroma normalises to float32 numpy arrays, so compare element-by-
        # element with assertAlmostEqual to absorb the precision loss.
        for vec in vectors:
            self.assertEqual(len(vec), 3)
            for got, expected in zip(vec, [0.1, 0.2, 0.3]):
                self.assertAlmostEqual(float(got), expected, places=5)
        self.assertEqual(captured["init"]["api_key"], "sk-test")
        self.assertEqual(captured["init"]["base_url"], "https://api.example.com/v1")
        self.assertEqual(captured["call"]["model"], "text-embedding-3-small")
        self.assertEqual(captured["call"]["input"], ["hello", "world"])

    def test_sentence_transformers_missing_dep_returns_actionable_error(self) -> None:
        """When sentence-transformers is not installed the wrapper must raise
        a RuntimeError pointing at the install extra rather than a bare
        ImportError, so a CLI handler can render a themed message."""
        from amx.search import embeddings

        with patch.dict(sys.modules, {"sentence_transformers": None}):
            with self.assertRaises(RuntimeError) as ctx:
                embeddings.SentenceTransformerEmbedding(model="some/model")

        self.assertIn("local-embeddings", str(ctx.exception))


class ProfileCreationLeakageTests(unittest.TestCase):
    """Adding a new DB or LLM profile must NOT pre-fill the form with
    values from the currently active profile. Before this fix, typing
    `/add-db-profile new-postgres` while a Databricks profile was
    active would silently use the Databricks host / http_path / token
    as Enter-to-keep defaults — a real-world value would land in the
    new postgres profile if the user pressed Enter.
    """

    def test_db_add_profile_new_name_does_not_inherit_active_profile(self) -> None:
        """The active Databricks profile must NOT leak into a freshly
        created profile of any backend."""
        from amx.cli_support.commands.db import cmd_add_profile

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            # Use the proper public API so write-through saves do not
            # overwrite our seeded profile with the initial cfg.db.
            cfg.upsert_db_profile(
                "databricks-default",
                DBConfig(
                    backend="databricks",
                    host="adb-1234567890123456.7.azuredatabricks.net",
                    http_path="/sql/1.0/warehouses/abc1234",
                    access_token="dapi-real-token-do-not-leak",
                    catalog="my_catalog",
                    database="default",
                ),
            )
            cfg.set_active_db_profile("databricks-default")

            captured: dict[str, object] = {"defaults": "MISSING"}

            def spy(defaults):
                captured["defaults"] = defaults
                return DBConfig(
                    backend="postgresql",
                    host="db.new.example.com",
                    user="alice",
                    password="secret",
                    database="new_db",
                )

            with patch(
                "amx.cli_support.commands.db.interactive_db_block",
                side_effect=spy,
            ):
                cmd_add_profile(cfg, ["brand-new-postgres"])

            self.assertIsNone(captured["defaults"])
            new_profile = cfg.db_profiles["brand-new-postgres"]
            self.assertEqual(new_profile.backend, "postgresql")
            self.assertEqual(new_profile.host, "db.new.example.com")
            self.assertNotIn("databricks", new_profile.host)
            self.assertEqual(new_profile.access_token, "")
            self.assertEqual(new_profile.http_path, "")
            self.assertEqual(new_profile.catalog, "")

    def test_db_add_profile_existing_name_passes_existing_as_defaults(self) -> None:
        """Editing an existing profile keeps its values as defaults so
        the user can press Enter to skip unchanged fields."""
        from amx.cli_support.commands.db import cmd_add_profile

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            new_db = DBConfig(
                backend="postgresql",
                host="db.example.com",
                user="alice",
                database="orders",
                password="secret",
            )
            # Wrap setup in a transaction so the autosave in
            # set_active_db_profile doesn't fire mid-mutation and
            # overwrite our just-seeded profile with the still-default
            # cfg.db. (See PR #4 for transactional writes.)
            with cfg.transaction():
                cfg.upsert_db_profile("edit-me", new_db)
                cfg.db = new_db
                cfg.set_active_db_profile("edit-me")

            captured: dict[str, object] = {}

            def spy(defaults):
                captured["defaults"] = defaults
                return cfg.db_profiles["edit-me"]

            with patch(
                "amx.cli_support.commands.db.interactive_db_block",
                side_effect=spy,
            ):
                cmd_add_profile(cfg, ["edit-me"])

            self.assertIsNotNone(captured["defaults"])
            self.assertEqual(captured["defaults"].host, "db.example.com")

    def test_interactive_db_block_resets_cross_backend_defaults(self) -> None:
        """If the caller passes a Databricks profile but the user picks
        PostgreSQL in the picker, the postgres prompts must NOT inherit
        the Databricks host / token / catalog."""
        from amx.cli_support.commands.db import interactive_db_block

        databricks_defaults = DBConfig(
            backend="databricks",
            host="adb-1234567890123456.7.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/abc",
            access_token="dapi-leaks",
            catalog="my_catalog",
        )

        # Mock all the prompts: pick PostgreSQL, capture what default
        # the host prompt was given.
        host_default_seen = {"value": "MISSING"}

        def fake_ask_choice(*_args, **_kwargs):
            return "postgresql"

        def fake_update_text(label, current="", **_kwargs):
            if "Database host" in label:
                host_default_seen["value"] = current
            # Port prompt has an .isdigit() validation loop, so it must
            # get a numeric reply or the loop never exits.
            if "Port" in label:
                return "5432"
            return "user-typed-value"

        def fake_update_secret(*_args, **_kwargs):
            return "user-typed-secret"

        with (
            patch("amx.cli_support.commands.db.ask_choice", fake_ask_choice),
            patch("amx.cli_support.commands.db._ask_update_text", fake_update_text),
            patch("amx.cli_support.commands.db._ask_update_secret", fake_update_secret),
            patch("amx.cli_support.commands.db._ask_update_bool", lambda *_a, **_k: False),
        ):
            result = interactive_db_block(databricks_defaults)

        # The Databricks host must NOT have been offered as the postgres
        # default — the cross-backend reset means defaults.host was "".
        self.assertEqual(host_default_seen["value"], "")
        self.assertEqual(result.backend, "postgresql")

    def test_llm_add_profile_new_name_does_not_inherit_active_profile(self) -> None:
        """The active LLM profile's API key, model, and base URL must
        NOT leak into a freshly created LLM profile."""
        from amx.cli_support.commands.profiles import cmd_add_llm_profile
        from amx.config import LLMConfig

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            cfg.upsert_llm_profile(
                "work",
                LLMConfig(
                    provider="openai",
                    model="gpt-4o-mini",
                    api_key="sk-real-key-do-not-leak-xxxxxxxxxx",
                    api_base="https://api.openai.com/v1",
                    language="english",
                ),
            )
            cfg.set_active_llm_profile("work")

            captured: dict[str, object] = {"defaults": "MISSING"}

            def spy(defaults):
                captured["defaults"] = defaults
                return LLMConfig(
                    provider="anthropic",
                    model="claude-sonnet-4",
                    api_key="new-anthropic-key",
                    language="english",
                )

            with (
                patch(
                    "amx.cli_support.commands.profiles.interactive_llm_block",
                    side_effect=spy,
                ),
                patch(
                    "amx.cli_support.commands.profiles.confirm",
                    return_value=False,
                ),
            ):
                cmd_add_llm_profile(cfg, ["brand-new-llm"])

            self.assertIsNone(captured["defaults"])
            new = cfg.llm_profiles["brand-new-llm"]
            self.assertEqual(new.provider, "anthropic")
            self.assertNotIn("sk-real-key", new.api_key)

    def test_llm_add_profile_then_activate_preserves_entered_values(self) -> None:
        """Reproduces the user-reported bug: typing `/add-llm-profile 4omini`
        with provider/model/api_key/api_base, answering `y` to "activate now",
        then running `/llm-profiles` showed the new profile with EMPTY
        provider/model. Cause: ``set_active_llm_profile`` flipped
        ``active_llm_profile`` first, the autosave triggered by that
        assignment ran ``save()`` which mirrors ``cfg.llm`` back into
        ``llm_profiles[active]`` — and at that moment ``cfg.llm`` was still
        the OLD (or empty) profile, so the freshly-saved values got wiped.
        """
        from amx.cli_support.commands.profiles import cmd_add_llm_profile
        from amx.config import LLMConfig

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "llm_profiles:\n"
                "  default:\n"
                "    provider: ''\n"
                "    model: ''\n"
                "    language: english\n"
                "active_llm_profile: default\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.active_llm_profile, "default")
            self.assertEqual(cfg.llm.provider, "")

            entered = LLMConfig(
                provider="openrouter",
                model="openai/gpt-4o-mini",
                api_key="sk-or-v1-real-user-key",
                api_base="https://openrouter.ai/api/v1",
                language="english",
                n_alternatives=3,
                column_batch_size=50,
                logprob_high=0.85,
                logprob_medium=0.65,
            )

            with (
                patch(
                    "amx.cli_support.commands.profiles.interactive_llm_block",
                    return_value=entered,
                ),
                patch(
                    "amx.cli_support.commands.profiles.confirm",
                    return_value=True,
                ),
            ):
                cmd_add_llm_profile(cfg, ["4omini"])

            saved = cfg.llm_profiles["4omini"]
            self.assertEqual(cfg.active_llm_profile, "4omini")
            self.assertEqual(saved.provider, "openrouter")
            self.assertEqual(saved.model, "openai/gpt-4o-mini")
            self.assertEqual(saved.api_key, "sk-or-v1-real-user-key")
            self.assertEqual(saved.api_base, "https://openrouter.ai/api/v1")
            self.assertEqual(saved.column_batch_size, 50)

            # Re-load from disk to confirm the YAML on disk also has the
            # right data (catches saves that succeed in-memory but get
            # re-overwritten by a follow-up autosave).
            reloaded = AMXConfig.load(str(cfg_path))
            on_disk = reloaded.llm_profiles["4omini"]
            self.assertEqual(reloaded.active_llm_profile, "4omini")
            self.assertEqual(on_disk.provider, "openrouter")
            self.assertEqual(on_disk.model, "openai/gpt-4o-mini")
            self.assertEqual(on_disk.api_base, "https://openrouter.ai/api/v1")


class ProfilePersistenceRaceTests(unittest.TestCase):
    """Newly-created DB and LLM profiles must survive an exit-restart cycle
    with their fields populated.

    Before this fix, ``set_active_db_profile`` and ``set_active_llm_profile``
    had an autosave race: assigning ``self.active_*_profile = name`` triggered
    an intermediate save() that mirrored the still-stale ``self.<thing>`` into
    ``<thing>_profiles[name]``, wiping the just-added profile's data. The LLM
    side was the most visible symptom — newly-created LLM profiles came back
    on restart with blank ``provider``/``model``/``api_key`` fields, surfacing
    as the user-reported "newly created profiles are gone" persistence bug.

    The DB-side ``cmd_add_profile`` originally hid the same race by manually
    writing ``cfg.db = db`` after the activation, which corrected the dict.
    Both paths now flow through the transactional ``set_active_*_profile``
    helpers so the activation produces exactly one save with consistent state.
    """

    def test_new_db_profile_data_survives_restart(self) -> None:
        """Reproducer for the DB half: create a profile via the actual
        ``cmd_add_profile`` flow, drop the cfg, reload from disk, and assert
        the new profile's fields are populated (not blanked by stale-mirror)."""
        from amx.cli_support.commands.db import cmd_add_profile

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            new_db = DBConfig(
                backend="postgresql",
                host="prod.db.example.com",
                user="alice",
                database="orders",
                password="secret",
            )
            with patch(
                "amx.cli_support.commands.db.interactive_db_block",
                return_value=new_db,
            ):
                cmd_add_profile(cfg, ["prod_pg"])
            del cfg

            cfg2 = AMXConfig.load(str(cfg_path))
            self.assertIn("prod_pg", cfg2.db_profiles)
            persisted = cfg2.db_profiles["prod_pg"]
            self.assertEqual(persisted.backend, "postgresql")
            self.assertEqual(persisted.host, "prod.db.example.com")
            self.assertEqual(persisted.user, "alice")
            self.assertEqual(persisted.database, "orders")
            self.assertEqual(cfg2.active_db_profile, "prod_pg")
            self.assertEqual(cfg2.db.host, "prod.db.example.com")

    def test_new_llm_profile_data_survives_restart(self) -> None:
        """Reproducer for the LLM half — the user-visible bug. With the
        pre-fix ``set_active_llm_profile``, ``provider`` and ``model`` came
        back blank after restart. With the transaction-wrapped fix they
        round-trip correctly."""
        from amx.cli_support.commands.profiles import cmd_add_llm_profile

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            new_llm = LLMConfig(
                provider="openai",
                model="gpt-4o-mini",
                api_key="sk-test-key",
                language="english",
            )
            with (
                patch(
                    "amx.cli_support.commands.profiles.interactive_llm_block",
                    return_value=new_llm,
                ),
                patch(
                    "amx.cli_support.commands.profiles.confirm",
                    return_value=True,  # accept "Activate now?"
                ),
            ):
                cmd_add_llm_profile(cfg, ["work"])
            del cfg

            cfg2 = AMXConfig.load(str(cfg_path))
            self.assertIn("work", cfg2.llm_profiles)
            persisted = cfg2.llm_profiles["work"]
            self.assertEqual(persisted.provider, "openai")
            self.assertEqual(persisted.model, "gpt-4o-mini")
            self.assertEqual(cfg2.active_llm_profile, "work")
            # cfg.llm (the active mirror) must also reflect the new profile,
            # not the empty defaults that used to leak through the race.
            self.assertEqual(cfg2.llm.provider, "openai")
            self.assertEqual(cfg2.llm.model, "gpt-4o-mini")

    def test_create_profile_when_existing_profile_is_active_keeps_both(self) -> None:
        """Closer to the user's reported scenario: the disk already has a
        profile (`databricks-default`) which becomes the active mirror at load. Creating
        a new profile then activating it must NOT lose either profile's data
        — the bug used to wipe whichever one the activation touched second."""
        from amx.cli_support.commands.db import cmd_add_profile
        from amx.cli_support.commands.profiles import cmd_add_llm_profile

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            seed = AMXConfig.load(str(cfg_path))
            seed.upsert_db_profile(
                "databricks-default",
                DBConfig(
                    backend="databricks",
                    host="adb-existing.azuredatabricks.net",
                    http_path="/sql/1.0/warehouses/abc",
                    access_token="dapi-original-token",
                    catalog="dap",
                    database="dev",
                ),
            )
            seed.set_active_db_profile("databricks-default")
            del seed

            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.active_db_profile, "databricks-default")
            new_db = DBConfig(
                backend="postgresql",
                host="prod.db",
                user="alice",
                database="prod_db",
            )
            new_llm = LLMConfig(provider="openai", model="gpt-4o-mini")
            with patch(
                "amx.cli_support.commands.db.interactive_db_block",
                return_value=new_db,
            ):
                cmd_add_profile(cfg, ["prod_pg"])
            with (
                patch(
                    "amx.cli_support.commands.profiles.interactive_llm_block",
                    return_value=new_llm,
                ),
                patch(
                    "amx.cli_support.commands.profiles.confirm",
                    return_value=True,
                ),
            ):
                cmd_add_llm_profile(cfg, ["work"])
            del cfg

            cfg2 = AMXConfig.load(str(cfg_path))
            # Both the seeded profile AND the new one must be present.
            self.assertIn("databricks-default", cfg2.db_profiles)
            self.assertIn("prod_pg", cfg2.db_profiles)
            self.assertIn("work", cfg2.llm_profiles)
            # New profiles must have their data, not be blank shells.
            self.assertEqual(cfg2.db_profiles["prod_pg"].host, "prod.db")
            self.assertEqual(cfg2.llm_profiles["work"].provider, "openai")
            self.assertEqual(cfg2.llm_profiles["work"].model, "gpt-4o-mini")
            # Active should be the most recently activated profile.
            self.assertEqual(cfg2.active_db_profile, "prod_pg")
            self.assertEqual(cfg2.active_llm_profile, "work")


class CrashReportSanitizationTests(unittest.TestCase):
    """`write_crash_report` and `redact_secrets` keep DB passwords, API
    keys, and Databricks PATs from leaking into a file the user is
    likely to paste into a GitHub issue."""

    def test_redacts_openai_api_key(self) -> None:
        from amx.utils.crash import redact_secrets

        msg = "AuthenticationError: Invalid key sk-ABCD1234efgh5678ijklMNOPqrst9012"
        result = redact_secrets(msg)
        self.assertNotIn("ABCD1234", result)
        self.assertIn("sk-<redacted>", result)

    def test_redacts_anthropic_and_openrouter_keys_with_label(self) -> None:
        from amx.utils.crash import redact_secrets

        ant = redact_secrets("key=sk-ant-api03-AbC1234567890abcdefghijKLMnopQrSTuvwxyz")
        self.assertIn("sk-ant-<redacted>", ant)
        self.assertNotIn("api03-AbC", ant)

        orr = redact_secrets("Bearer sk-or-1234567890abcdefghijKLMNOpQRStuvwxyzABCDEF")
        self.assertIn("sk-or-<redacted>", orr)

    def test_redacts_databricks_personal_access_token(self) -> None:
        from amx.utils.crash import redact_secrets

        msg = "Connection failed: dapi1234567890abcdef invalid"
        result = redact_secrets(msg)
        self.assertIn("dapi<redacted>", result)
        self.assertNotIn("1234567890abcdef", result)

    def test_redacts_password_kv_pairs(self) -> None:
        from amx.utils.crash import redact_secrets

        cases = [
            'password="hunter2"',
            "password=hunter2",
            "  password : hunter2",
            "PASSWORD = 'hunter2'",
        ]
        for case in cases:
            result = redact_secrets(case)
            self.assertNotIn("hunter2", result, f"failed for: {case!r}")

    def test_redacts_api_key_kv_pairs(self) -> None:
        from amx.utils.crash import redact_secrets

        for label in ("api_key", "api-key", "apiKey", "API_KEY"):
            result = redact_secrets(f'{label}="my-secret-token-1234"')
            self.assertNotIn("my-secret-token-1234", result)

    def test_redacts_bearer_token(self) -> None:
        from amx.utils.crash import redact_secrets

        result = redact_secrets("Authorization: Bearer abc.def.ghi.SUPER_SECRET")
        self.assertNotIn("SUPER_SECRET", result)
        self.assertIn("Bearer <redacted>", result)

    def test_write_crash_report_path_format_and_content(self) -> None:
        import tempfile

        from amx.utils import crash as crash_module
        from amx.utils.crash import write_crash_report

        with tempfile.TemporaryDirectory() as td:
            # Redirect crash dir so the test does not pollute the user's
            # ~/.amx/logs/crashes/ directory.
            patched = Path(td)
            with patch.object(crash_module, "CRASH_DIR", patched):
                try:
                    raise RuntimeError("boom: password=hunter2")
                except RuntimeError as exc:
                    path = write_crash_report(exc, request_id="test-req-id")

            self.assertTrue(path.exists())
            content = path.read_text()
            self.assertIn("test-req-id", content)
            self.assertIn("RuntimeError", content)
            # Secret in the message must have been redacted before write.
            self.assertNotIn("hunter2", content)

    def test_write_crash_report_chmods_file_to_0o600_on_posix(self) -> None:
        if os.name != "posix":
            self.skipTest("chmod 0o600 is only meaningful on POSIX")
        import tempfile

        from amx.utils import crash as crash_module
        from amx.utils.crash import write_crash_report

        with tempfile.TemporaryDirectory() as td:
            with patch.object(crash_module, "CRASH_DIR", Path(td)):
                try:
                    raise RuntimeError("boom")
                except RuntimeError as exc:
                    path = write_crash_report(exc)

            mode = path.stat().st_mode & 0o777
            self.assertEqual(mode, 0o600)

    def test_write_crash_report_includes_amx_env_vars_only(self) -> None:
        """The env-var section must scope down to AMX_-prefixed names so
        we do not accidentally dump arbitrary tokens (e.g. CI provider
        secrets) from the surrounding shell."""
        import tempfile

        from amx.utils import crash as crash_module
        from amx.utils.crash import write_crash_report

        original_env = dict(os.environ)
        os.environ["AMX_TEST_FOO"] = "amx-only-marker"
        os.environ["MY_SUPER_SECRET_TOKEN_ZZZ"] = "should-not-leak"
        try:
            with tempfile.TemporaryDirectory() as td:
                with patch.object(crash_module, "CRASH_DIR", Path(td)):
                    try:
                        raise RuntimeError("boom")
                    except RuntimeError as exc:
                        path = write_crash_report(exc)

                content = path.read_text()
                self.assertIn("AMX_TEST_FOO", content)
                self.assertNotIn("MY_SUPER_SECRET_TOKEN_ZZZ", content)
                self.assertNotIn("should-not-leak", content)
        finally:
            os.environ.clear()
            os.environ.update(original_env)


class TokenBudgetPreCheckTests(unittest.TestCase):
    """`_synthesize_answer` now pre-trims retrieval rows before sending
    them to the LLM. The trimmer must keep the highest-scored rows,
    drop the rest, and never expand beyond the input budget."""

    def test_input_token_budget_per_model_family(self) -> None:
        from amx.search.agent import _input_token_budget_for

        # Default budget for OpenAI gpt-4o family.
        self.assertEqual(_input_token_budget_for("gpt-4o-mini"), 60_000)
        self.assertEqual(_input_token_budget_for("gpt-4o"), 60_000)

        # Claude family gets the larger budget (200K context window).
        self.assertEqual(_input_token_budget_for("claude-3-5-sonnet-20241022"), 150_000)
        self.assertEqual(_input_token_budget_for("claude-sonnet-4-20250514"), 150_000)
        self.assertEqual(_input_token_budget_for("claude-opus-4"), 150_000)

        # Gemini gets the largest (1M-2M context).
        self.assertEqual(_input_token_budget_for("gemini-1.5-pro"), 250_000)
        self.assertEqual(_input_token_budget_for("gemini-2.0-flash"), 250_000)

        # Unknown / empty falls back to default.
        self.assertEqual(_input_token_budget_for("totally-new-model"), 60_000)
        self.assertEqual(_input_token_budget_for(""), 60_000)
        self.assertEqual(_input_token_budget_for(None), 60_000)

    def test_trim_rows_preserves_all_under_budget(self) -> None:
        from amx.search.agent import _trim_rows_to_token_budget

        rows = [{"match_score": 5.0, "schema_name": "p", "table_name": f"t{i}"} for i in range(3)]
        kept, dropped = _trim_rows_to_token_budget(
            rows,
            system_text="sys",
            base_payload={"question": "x"},
            budget=60_000,
        )
        self.assertEqual(len(kept), 3)
        self.assertEqual(dropped, 0)

    def test_trim_rows_keeps_highest_scored_when_over_budget(self) -> None:
        """A tiny budget forces the trimmer to drop everything except
        the highest-scored rows. The order must reflect descending
        match_score even if the input was unsorted."""
        import string

        from amx.search.agent import _trim_rows_to_token_budget

        # Use varied text — tiktoken compresses long runs of identical
        # characters very efficiently, which would let the test pass
        # vacuously without exercising the trimmer.
        words = " ".join(string.ascii_lowercase * 50)
        rows = [
            {
                "match_score": 1.0,
                "schema_name": "p",
                "table_name": "t1",
                "description": f"Description one — {words}",
            },
            {
                "match_score": 9.0,
                "schema_name": "p",
                "table_name": "t9",
                "description": f"Description nine — {words}",
            },
            {
                "match_score": 5.0,
                "schema_name": "p",
                "table_name": "t5",
                "description": f"Description five — {words}",
            },
        ]
        kept, dropped = _trim_rows_to_token_budget(
            rows,
            system_text="short",
            base_payload={"q": "x"},
            budget=500,  # tighter than any single row's text
        )
        self.assertGreaterEqual(dropped, 1)
        if kept:
            # Highest-scored row is t9 — it must come first in kept.
            self.assertEqual(kept[0]["table_name"], "t9")
            scores = [row["match_score"] for row in kept]
            # The kept slice is sorted descending — the highest scores survived.
            self.assertEqual(scores, sorted(scores, reverse=True))

    def test_trim_empty_input_returns_empty(self) -> None:
        from amx.search.agent import _trim_rows_to_token_budget

        kept, dropped = _trim_rows_to_token_budget(
            [],
            system_text="sys",
            base_payload={"q": "x"},
            budget=60_000,
        )
        self.assertEqual(kept, [])
        self.assertEqual(dropped, 0)


class AskPathDeprecationTests(unittest.TestCase):
    """`LoopBasedAskAgent` (the deterministic tool-loop path that
    predates `SearchAgent`) is being phased out. These tests pin the
    deprecation contract: the warning fires once per process, the
    canonical path is `SearchService` / `SearchAgent`."""

    def test_loop_based_ask_agent_emits_deprecation_warning(self) -> None:
        import warnings

        from amx.core.ask_agent import AskToolbox, LoopBasedAskAgent

        # Reset the once-only flag so the test sees the warning even
        # if a previous test in the same process already triggered it.
        LoopBasedAskAgent._deprecation_warned = False

        # Build a minimal AskToolbox stand-in. We only construct the
        # class — answer() requires a populated catalog.
        toolbox = AskToolbox.__new__(AskToolbox)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            LoopBasedAskAgent(toolbox)

        deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertGreaterEqual(len(deprecation_warnings), 1)
        message = str(deprecation_warnings[0].message)
        self.assertIn("LoopBasedAskAgent", message)
        self.assertIn("0.4.0", message)
        self.assertIn("SearchService", message)

    def test_canonical_path_is_search_service(self) -> None:
        """Sanity-check that `SearchService` exists and routes through
        `SearchAgent` under the hood. If this test fails because the
        attribute names changed, update the deprecation message in
        `LoopBasedAskAgent` to match."""
        from amx.search.agent import SearchAgent
        from amx.search.service import SearchService

        # SearchService stores the agent on `_agent` — make sure that
        # is still the case.
        attrs = set(SearchService.__init__.__code__.co_names)
        # Either `_agent` is referenced as an attribute or one of the
        # canonical helpers is. The exact attribute name is internal
        # but the reference must exist.
        self.assertTrue(
            "_agent" in attrs or hasattr(SearchService, "ask"),
            "SearchService should expose ask() routing to SearchAgent",
        )
        self.assertTrue(callable(SearchAgent))


class VectorScoreFloorTests(unittest.TestCase):
    """Per-provider distance-threshold calibration replacing the old
    hardcoded `2.5` cutoff. The threshold is the minimum match_score
    (= 3.0 - distance) a vector-only hit must reach to survive candidate
    filtering."""

    def test_minilm_uses_legacy_default(self) -> None:
        from amx.search.catalog import _vector_score_floor

        self.assertEqual(_vector_score_floor({}, "minilm"), 2.5)
        self.assertEqual(_vector_score_floor({}, "default"), 2.5)
        self.assertEqual(_vector_score_floor({}, ""), 2.5)

    def test_openai_compatible_has_tighter_floor(self) -> None:
        """OpenAI v3 embeddings produce tighter cosine distance for
        relevant matches; a slightly higher floor reduces noise."""
        from amx.search.catalog import _vector_score_floor

        floor = _vector_score_floor({}, "openai_compatible")
        self.assertGreater(floor, 2.5)
        self.assertLess(floor, 3.0)

    def test_sentence_transformers_has_tighter_floor(self) -> None:
        from amx.search.catalog import _vector_score_floor

        floor = _vector_score_floor({}, "sentence_transformers")
        self.assertGreater(floor, 2.5)
        self.assertLess(floor, 3.0)

    def test_explicit_setting_overrides_provider_default(self) -> None:
        from amx.search.catalog import _vector_score_floor

        # Operator override via /search /config.
        self.assertEqual(_vector_score_floor({"vector_score_floor": "1.7"}, "minilm"), 1.7)
        self.assertEqual(
            _vector_score_floor({"vector_score_floor": "2.9"}, "openai_compatible"),
            2.9,
        )

    def test_invalid_setting_falls_back_to_provider_default(self) -> None:
        from amx.search.catalog import _vector_score_floor

        # Garbage value must not crash the retrieval path.
        self.assertEqual(
            _vector_score_floor({"vector_score_floor": "not-a-number"}, "minilm"),
            2.5,
        )

    def test_unknown_embedding_kind_uses_default(self) -> None:
        from amx.search.catalog import _vector_score_floor

        self.assertEqual(_vector_score_floor({}, "totally-new-provider"), 2.5)


class RequestIdWiringTests(unittest.TestCase):
    """Verify that the long-running CLI commands (`/search ask`,
    `/analyze run`) set a fresh request_id on entry and clear it on
    exit, so each invocation's log lines are filterable by id."""

    def test_search_ask_sets_request_id_during_execution_and_clears_after(
        self,
    ) -> None:
        from amx.cli_support.commands.search import _run_search_ask
        from amx.utils.logging import (
            clear_request_id,
            get_request_id,
        )

        clear_request_id()
        seen_during_call: dict[str, str | None] = {"id": None}

        class FakeService:
            settings = {
                "show_provenance": "false",
                "show_confidence": "false",
            }

            def ask(self, question_text: str):
                # The whole point: at this exact moment, the request
                # id must be set, so any log line emitted from inside
                # SearchAgent / SearchCatalog / LLMProvider carries it.
                seen_during_call["id"] = get_request_id()
                return SimpleNamespace(
                    summary="ok",
                    provenance=[],
                    confidence="high",
                    intent="explain",
                    question=question_text,
                    rows=[],
                    details={},
                )

        cfg = AMXConfig()
        # Patch history_store to None so the run-persistence branch is
        # skipped — we only care about the request_id wrapping.
        with patch(
            "amx.cli_support.commands.search.history_store",
            return_value=None,
        ):
            _run_search_ask(
                cfg,
                FakeService(),  # type: ignore[arg-type]
                "what is the orders table?",
                log_event=lambda **_: None,
                take_actions=False,
            )

        self.assertIsNotNone(seen_during_call["id"])
        # Cleared on exit, regardless of success or exception.
        self.assertIsNone(get_request_id())

    def test_search_ask_clears_request_id_on_exception(self) -> None:
        from amx.cli_support.commands.search import _run_search_ask
        from amx.utils.logging import clear_request_id, get_request_id

        clear_request_id()

        class FailingService:
            settings: dict[str, str] = {}

            def ask(self, _q: str):
                raise RuntimeError("svc boom")

        cfg = AMXConfig()
        with patch(
            "amx.cli_support.commands.search.history_store",
            return_value=None,
        ):
            with self.assertRaises(RuntimeError):
                _run_search_ask(
                    cfg,
                    FailingService(),  # type: ignore[arg-type]
                    "x?",
                    log_event=lambda **_: None,
                    take_actions=False,
                )

        # The finally block must have cleared the id even though
        # the inner body raised.
        self.assertIsNone(get_request_id())


class StructuredLoggingTests(unittest.TestCase):
    """Week-5 structured logging: file handler emits one JSON object per
    line, stderr keeps the historical human-readable format, and a
    contextvar threads request_id through every log record."""

    def test_set_and_clear_request_id_round_trip(self) -> None:
        from amx.utils.logging import (
            clear_request_id,
            get_request_id,
            set_request_id,
        )

        clear_request_id()
        self.assertIsNone(get_request_id())

        rid = set_request_id()
        self.assertIsNotNone(get_request_id())
        self.assertEqual(get_request_id(), rid)

        # Explicit id is preserved unchanged.
        set_request_id("explicit-id-1234")
        self.assertEqual(get_request_id(), "explicit-id-1234")

        clear_request_id()
        self.assertIsNone(get_request_id())

    def test_json_formatter_emits_one_valid_object_per_record(self) -> None:
        import logging as stdlib_logging

        from amx.utils.logging import JsonFormatter

        formatter = JsonFormatter()
        record = stdlib_logging.LogRecord(
            name="amx.test",
            level=stdlib_logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        record.request_id = "req-test-0001"

        rendered = formatter.format(record)
        payload = json.loads(rendered)
        self.assertEqual(payload["level"], "WARNING")
        self.assertEqual(payload["logger"], "amx.test")
        self.assertEqual(payload["request_id"], "req-test-0001")
        self.assertEqual(payload["message"], "hello world")
        self.assertIn("ts", payload)
        self.assertNotIn("exc_info", payload)

    def test_json_formatter_includes_exc_info_when_logging_exception(self) -> None:
        import logging as stdlib_logging

        from amx.utils.logging import JsonFormatter

        try:
            raise RuntimeError("boom")
        except RuntimeError:
            exc_info = sys.exc_info()

        record = stdlib_logging.LogRecord(
            name="amx.test",
            level=stdlib_logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="failure",
            args=(),
            exc_info=exc_info,
        )
        record.request_id = "-"

        rendered = JsonFormatter().format(record)
        payload = json.loads(rendered)
        self.assertEqual(payload["level"], "ERROR")
        self.assertIn("exc_info", payload)
        self.assertIn("RuntimeError: boom", payload["exc_info"])

    def test_request_id_filter_injects_default_when_unset(self) -> None:
        import logging as stdlib_logging

        from amx.utils.logging import _RequestIdFilter, clear_request_id

        clear_request_id()
        record = stdlib_logging.LogRecord(
            name="amx.test",
            level=stdlib_logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="m",
            args=(),
            exc_info=None,
        )
        kept = _RequestIdFilter().filter(record)
        self.assertTrue(kept)
        self.assertEqual(record.request_id, "-")

    def test_request_id_filter_picks_up_active_id(self) -> None:
        import logging as stdlib_logging

        from amx.utils.logging import (
            _RequestIdFilter,
            clear_request_id,
            set_request_id,
        )

        try:
            set_request_id("rid-abc-123")
            record = stdlib_logging.LogRecord(
                name="amx.test",
                level=stdlib_logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="m",
                args=(),
                exc_info=None,
            )
            _RequestIdFilter().filter(record)
            self.assertEqual(record.request_id, "rid-abc-123")
        finally:
            clear_request_id()


class DatabaseConnectionRetryTests(unittest.TestCase):
    """Connector-level transient retry, parallel to LLM transient retry.

    DNS glitches, connection resets, and timeouts get one retry with
    backoff. Auth / permission / missing-DB / SSL-trust errors do NOT
    retry — they propagate immediately so the categorised actionable
    message reaches the user without artificial delay."""

    def setUp(self) -> None:
        self._sleep_patcher = patch("amx.db.connector.time.sleep", return_value=None)
        self._sleep_patcher.start()

    def tearDown(self) -> None:
        self._sleep_patcher.stop()

    def _make_connector(self, attempts: list[Exception | None]):
        """Build a DatabaseConnector whose adapter.test_connection iterates
        through *attempts* (None = success on that attempt; an Exception =
        raise it). Returns the connector + a counter list."""
        cfg = DBConfig(
            backend="postgresql",
            host="db.example.com",
            port=5432,
            user="alice",
            database="orders",
            password="secret",
        )
        connector = DatabaseConnector.__new__(DatabaseConnector)
        connector.cfg = cfg
        connector._engine = None

        attempt_counter = {"count": 0}

        class FakeAdapter:
            name = "postgresql"
            capabilities = BackendCapabilities()

            def test_connection(self, _engine):
                idx = attempt_counter["count"]
                attempt_counter["count"] += 1
                outcome = attempts[idx] if idx < len(attempts) else None
                if outcome is not None:
                    raise outcome

            def actionable_profile_error(self, exc):
                return None

            def create_engine(self):
                return SimpleNamespace()

        connector._adapter = FakeAdapter()
        return connector, attempt_counter

    def test_transient_failure_retried_then_succeeds(self) -> None:
        """First attempt fails with a network error, second succeeds — the
        retry loop must return ok=True without surfacing the first error."""
        connector, attempts = self._make_connector(
            [
                ConnectionResetError("Connection reset by peer"),
                None,  # success
            ]
        )
        result = connector.test_connection_result()
        self.assertTrue(result.ok)
        self.assertEqual(attempts["count"], 2)

    def test_transient_dns_failure_retried_max_once_then_categorised(self) -> None:
        """Persistent DNS failures exhaust the retry budget and surface the
        categorised actionable message from ErrorMapper."""
        connector, attempts = self._make_connector(
            [
                RuntimeError("getaddrinfo failed: Name or service not known"),
                RuntimeError("getaddrinfo failed: Name or service not known"),
            ]
        )
        result = connector.test_connection_result()
        self.assertFalse(result.ok)
        # MAX_CONNECTION_RETRIES = 1 → 2 total attempts.
        self.assertEqual(attempts["count"], 2)
        self.assertIn("network unreachable", result.message.lower())

    def test_auth_failure_does_not_retry(self) -> None:
        """Authentication errors must propagate immediately so the user
        sees the categorised auth message without waiting through a
        pointless retry."""
        connector, attempts = self._make_connector(
            [RuntimeError('FATAL: password authentication failed for user "alice"')]
        )
        result = connector.test_connection_result()
        self.assertFalse(result.ok)
        self.assertEqual(attempts["count"], 1)
        self.assertIn("authentication failed", result.message.lower())

    def test_permission_denied_does_not_retry(self) -> None:
        connector, attempts = self._make_connector(
            [RuntimeError("permission denied for relation users")]
        )
        result = connector.test_connection_result()
        self.assertFalse(result.ok)
        self.assertEqual(attempts["count"], 1)

    def test_certificate_verify_failed_does_not_retry(self) -> None:
        connector, attempts = self._make_connector(
            [RuntimeError("SSL: CERTIFICATE_VERIFY_FAILED self-signed certificate")]
        )
        result = connector.test_connection_result()
        self.assertFalse(result.ok)
        # CertVerify is in _NON_TRANSIENT_DB_PATTERNS — single attempt.
        self.assertEqual(attempts["count"], 1)

    def test_is_transient_db_connection_error_classifications(self) -> None:
        from amx.db.connector import _is_transient_db_connection_error

        # Transient.
        self.assertTrue(_is_transient_db_connection_error(ConnectionResetError("Connection reset")))
        self.assertTrue(_is_transient_db_connection_error(TimeoutError("timed out")))
        self.assertTrue(
            _is_transient_db_connection_error(
                RuntimeError("getaddrinfo failed: Name or service not known")
            )
        )
        self.assertTrue(_is_transient_db_connection_error(RuntimeError("503 Service Unavailable")))
        # Non-transient (auth / permission / missing-db / SSL-trust).
        self.assertFalse(
            _is_transient_db_connection_error(RuntimeError("password authentication failed"))
        )
        self.assertFalse(
            _is_transient_db_connection_error(RuntimeError("permission denied for relation orders"))
        )
        self.assertFalse(
            _is_transient_db_connection_error(
                RuntimeError("certificate_verify_failed: self-signed certificate")
            )
        )
        self.assertFalse(
            _is_transient_db_connection_error(RuntimeError('database "missing_db" does not exist'))
        )


class UsageCommandTests(unittest.TestCase):
    """Week-5 /usage command — local-only token + cost summary read from
    ~/.amx/history.db. These tests cover the pure-functional helpers and
    the cmd_usage dispatch without requiring a real history store."""

    def test_normalize_window_default_and_known(self) -> None:
        from amx.cli_support.commands.usage import _normalize_window

        label, sec = _normalize_window("")
        self.assertEqual(label, "7d")
        self.assertGreater(sec, 0)

        label, sec = _normalize_window("24h")
        self.assertEqual(label, "24h")
        self.assertEqual(int(sec), 86400)

        label, sec = _normalize_window("all")
        self.assertEqual(label, "all")
        self.assertIsNone(sec)

    def test_normalize_window_unknown_falls_back_to_default(self) -> None:
        from amx.cli_support.commands.usage import _normalize_window

        label, _sec = _normalize_window("forever")
        self.assertEqual(label, "7d")

    def test_lookup_pricing_exact_match(self) -> None:
        from amx.cli_support.commands.usage import _lookup_pricing

        pricing = _lookup_pricing("gpt-4o-mini")
        self.assertIsNotNone(pricing)
        self.assertEqual(pricing[0], 0.15)

    def test_lookup_pricing_strips_provider_prefix(self) -> None:
        from amx.cli_support.commands.usage import _lookup_pricing

        # OpenRouter-style namespacing
        self.assertIsNotNone(_lookup_pricing("openai/gpt-4o"))
        self.assertIsNotNone(_lookup_pricing("openrouter/openai/gpt-4o"))

    def test_lookup_pricing_strips_dated_suffix(self) -> None:
        from amx.cli_support.commands.usage import _lookup_pricing

        # claude-sonnet-4 priced; claude-sonnet-4-20250514 should also resolve
        self.assertIsNotNone(_lookup_pricing("claude-sonnet-4-20250514"))

    def test_lookup_pricing_unknown_returns_none(self) -> None:
        from amx.cli_support.commands.usage import _lookup_pricing

        self.assertIsNone(_lookup_pricing("totally-not-a-model"))
        self.assertIsNone(_lookup_pricing(""))

    def test_aggregate_runs_groups_by_provider_model(self) -> None:
        from amx.cli_support.commands.usage import _aggregate_runs

        runs = [
            {
                "llm_provider": "openai",
                "llm_model": "gpt-4o-mini",
                "tokens_json": json.dumps(
                    {
                        "records": [
                            {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150},
                            {"prompt_tokens": 200, "completion_tokens": 80, "total_tokens": 280},
                        ]
                    }
                ),
            },
            {
                "llm_provider": "anthropic",
                "llm_model": "claude-sonnet-4",
                "tokens_json": json.dumps(
                    {
                        "records": [
                            {"prompt_tokens": 1000, "completion_tokens": 400, "total_tokens": 1400}
                        ]
                    }
                ),
            },
        ]
        per, counted = _aggregate_runs(runs)

        self.assertEqual(counted, 2)
        self.assertEqual(per[("openai", "gpt-4o-mini")]["input_tokens"], 300)
        self.assertEqual(per[("openai", "gpt-4o-mini")]["output_tokens"], 130)
        self.assertEqual(per[("openai", "gpt-4o-mini")]["total_tokens"], 430)
        self.assertEqual(per[("openai", "gpt-4o-mini")]["runs"], 1)
        self.assertEqual(per[("anthropic", "claude-sonnet-4")]["input_tokens"], 1000)

    def test_aggregate_runs_skips_runs_without_token_data(self) -> None:
        from amx.cli_support.commands.usage import _aggregate_runs

        runs = [
            {"llm_provider": "openai", "llm_model": "gpt-4o", "tokens_json": None},
            {"llm_provider": "openai", "llm_model": "gpt-4o", "tokens_json": ""},
            {
                "llm_provider": "openai",
                "llm_model": "gpt-4o",
                "tokens_json": json.dumps({"records": []}),
            },
            {
                "llm_provider": "openai",
                "llm_model": "gpt-4o",
                "tokens_json": "{not-valid-json",
            },
        ]
        per, counted = _aggregate_runs(runs)
        self.assertEqual(counted, 0)
        self.assertEqual(per, {})

    def test_format_cost_for_known_and_unknown_models(self) -> None:
        from amx.cli_support.commands.usage import _format_cost

        # gpt-4o is $2.50 input / $10.00 output per 1M tokens.
        # 1_000_000 in / 500_000 out → $2.50 + $5.00 = $7.50
        self.assertEqual(_format_cost("gpt-4o", 1_000_000, 500_000), "$7.50")
        # Unknown model gets em-dash
        self.assertEqual(_format_cost("totally-fake", 1000, 500), "—")
        # Sub-cent rounds to "<$0.01"
        self.assertEqual(_format_cost("gpt-4o-mini", 100, 0), "<$0.01")

    def test_cmd_usage_with_no_history_store_warns(self) -> None:
        from amx.cli_support.commands.usage import cmd_usage

        cfg = AMXConfig()
        with patch(
            "amx.cli_support.commands.usage.history_store",
            return_value=None,
        ):
            cmd_usage(cfg, [])

    def test_cmd_usage_with_empty_window_prints_no_runs(self) -> None:
        from amx.cli_support.commands.usage import cmd_usage

        cfg = AMXConfig()

        class FakeStore:
            def _connect(self):  # noqa: D401 — context manager mock
                class _Conn:
                    def __enter__(self_inner):
                        return self_inner

                    def __exit__(self_inner, *_):
                        return False

                    def execute(self_inner, *_args, **_kwargs):
                        class _Cursor:
                            def fetchall(self_c):
                                return []

                        return _Cursor()

                return _Conn()

        with patch(
            "amx.cli_support.commands.usage.history_store",
            return_value=FakeStore(),
        ):
            cmd_usage(cfg, ["7d"])


class DBInspectCommandTests(unittest.TestCase):
    """The /inspect slash command lives under /db and gives users a self-
    service way to diagnose connector problems (the user's stated pain
    point #5). These tests pin the dispatch contract; live-DB behaviour
    is exercised by integration tests outside this file."""

    def _build_cfg(self, *, with_active: bool = True) -> AMXConfig:
        cfg = AMXConfig()
        if with_active:
            cfg.db_profiles = {
                "prod": DBConfig(
                    backend="postgresql",
                    host="db.example.com",
                    port=5432,
                    user="alice",
                    database="orders",
                    password="secret",
                )
            }
            cfg.active_db_profile = "prod"
            cfg.db = cfg.db_profiles["prod"]
        return cfg

    def _patch_connector(self, fake_connector_class: type) -> patch:
        return patch(
            "amx.db.connector.DatabaseConnector",
            fake_connector_class,
        )

    def test_inspect_with_no_active_profile_errors_cleanly(self) -> None:
        from amx.cli_support.commands.db import cmd_inspect

        cfg = self._build_cfg(with_active=False)
        # No raise; the command prints an error and returns.
        cmd_inspect(cfg, [])

    def test_inspect_unknown_profile_errors(self) -> None:
        from amx.cli_support.commands.db import cmd_inspect

        cfg = self._build_cfg()
        cmd_inspect(cfg, ["does-not-exist"])
        # Active profile must be unchanged.
        self.assertEqual(cfg.active_db_profile, "prod")

    def test_inspect_connection_failure_surfaces_categorised_message(self) -> None:
        from amx.cli_support.commands.db import cmd_inspect
        from amx.db.connector import ConnectionTestResult

        cfg = self._build_cfg()
        calls: list[str] = []

        class FakeConnector:
            def __init__(self, profile):
                calls.append("init")

            capabilities = SimpleNamespace(
                column_comments=True,
                relationships=True,
                row_count_stats=True,
                materialized_views=False,
            )

            def test_connection_result(self) -> ConnectionTestResult:
                calls.append("test_connection_result")
                return ConnectionTestResult(
                    ok=False,
                    message="PostgreSQL authentication failed: …",
                    exception=RuntimeError("…"),
                )

            def list_schemas(self) -> list[str]:  # pragma: no cover
                raise AssertionError("must not be called when connection fails")

        with self._patch_connector(FakeConnector):
            cmd_inspect(cfg, [])
        self.assertEqual(calls, ["init", "test_connection_result"])

    def test_inspect_lists_schemas_and_table_counts_on_success(self) -> None:
        from amx.cli_support.commands.db import cmd_inspect
        from amx.db.connector import ConnectionTestResult

        cfg = self._build_cfg()

        class FakeConnector:
            capabilities = SimpleNamespace(
                column_comments=True,
                relationships=True,
                row_count_stats=True,
                materialized_views=False,
            )

            def __init__(self, profile):
                pass

            def test_connection_result(self) -> ConnectionTestResult:
                return ConnectionTestResult(ok=True, message=None, exception=None)

            def list_schemas(self) -> list[str]:
                return ["public", "analytics", "audit"]

            def list_tables(self, schema: str) -> list[str]:
                return {
                    "public": ["users", "orders"],
                    "analytics": ["events", "sessions", "rollups"],
                    "audit": ["log_entries"],
                }[schema]

        with self._patch_connector(FakeConnector):
            cmd_inspect(cfg, [])

    def test_inspect_partial_schema_failures_do_not_abort(self) -> None:
        """If listing tables fails for one schema, /inspect must still
        complete and report the others — the read-only diagnostic
        command should not crash mid-output."""
        from amx.cli_support.commands.db import cmd_inspect
        from amx.db.connector import ConnectionTestResult

        cfg = self._build_cfg()
        finished = {"yes": False}

        class FakeConnector:
            capabilities = SimpleNamespace(
                column_comments=True,
                relationships=True,
                row_count_stats=True,
                materialized_views=False,
            )

            def __init__(self, profile):
                pass

            def test_connection_result(self) -> ConnectionTestResult:
                return ConnectionTestResult(ok=True, message=None, exception=None)

            def list_schemas(self) -> list[str]:
                return ["public", "restricted"]

            def list_tables(self, schema: str) -> list[str]:
                if schema == "restricted":
                    raise PermissionError("permission denied for schema restricted")
                return ["users"]

        with self._patch_connector(FakeConnector):
            cmd_inspect(cfg, [])
            finished["yes"] = True
        self.assertTrue(finished["yes"])


class _FakeRow:
    """Tuple-and-mapping-like row for SQLAlchemy mock results."""

    def __init__(self, values, mapping=None):
        self._values = list(values)
        self._mapping_dict = dict(mapping) if mapping else {}

    def __getitem__(self, idx):
        if isinstance(idx, str):
            return self._mapping_dict[idx]
        return self._values[idx]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    @property
    def _mapping(self):
        return self._mapping_dict or dict(enumerate(self._values))


def _fake_engine(*, fetchall=None, fetchone=None):
    """Build a minimal SQLAlchemy-engine stand-in.

    The ``with engine.connect() as conn: conn.execute(...).fetchall()``
    pattern is what every adapter uses, so we mock that exact shape and
    let each test pass the rows it wants returned.
    """
    from unittest.mock import MagicMock

    result = MagicMock()
    result.fetchall = MagicMock(return_value=list(fetchall or []))
    result.fetchone = MagicMock(return_value=fetchone)

    conn = MagicMock()
    conn.execute = MagicMock(return_value=result)

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.connect = MagicMock(return_value=cm)
    return engine, conn


class PostgreSQLEngineBoundTests(unittest.TestCase):
    """Engine-bound adapter coverage. PR #12 covered the SQL builders;
    this batch covers the methods that actually drive a SQLAlchemy
    connection (`list_materialized_views`, `get_incoming_foreign_keys`,
    `get_schema_comment`, `get_database_comment`)."""

    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="postgresql",
            host="db.example.com",
            port=5432,
            user="alice",
            database="orders",
            password="secret",
        )
        self.adapter = PostgreSQLAdapter(self.cfg)

    def test_list_materialized_views_returns_relname_strings(self) -> None:
        engine, conn = _fake_engine(
            fetchall=[
                _FakeRow(["mv_daily_sales"]),
                _FakeRow(["mv_monthly_kpis"]),
            ]
        )
        names = self.adapter.list_materialized_views(engine, "rep")
        self.assertEqual(names, ["mv_daily_sales", "mv_monthly_kpis"])
        # Verify the schema parameter was bound through.
        called_args = conn.execute.call_args
        self.assertEqual(called_args.args[1], {"schema": "rep"})

    def test_get_incoming_foreign_keys_normalises_to_dicts(self) -> None:
        engine, _ = _fake_engine(
            fetchall=[
                _FakeRow(["public", "orders", "user_id", "id"]),
                _FakeRow(["billing", "invoices", "customer_id", "id"]),
            ]
        )
        fks = self.adapter.get_incoming_foreign_keys(engine, "public", "users")
        self.assertEqual(len(fks), 2)
        self.assertEqual(
            fks[0],
            {
                "source_schema": "public",
                "source_table": "orders",
                "source_column": "user_id",
                "target_column": "id",
            },
        )

    def test_get_database_comment_returns_string_or_none(self) -> None:
        engine, _ = _fake_engine(fetchone=_FakeRow(["The orders OLTP database."]))
        self.assertEqual(
            self.adapter.get_database_comment(engine),
            "The orders OLTP database.",
        )

        engine, _ = _fake_engine(fetchone=None)
        self.assertIsNone(self.adapter.get_database_comment(engine))

    def test_get_schema_comment_returns_string_or_none(self) -> None:
        engine, _ = _fake_engine(fetchone=_FakeRow(["Reporting layer schema."]))
        self.assertEqual(
            self.adapter.get_schema_comment(engine, "rep"),
            "Reporting layer schema.",
        )

        engine, _ = _fake_engine(fetchone=None)
        self.assertIsNone(self.adapter.get_schema_comment(engine, "missing"))


class SnowflakeEngineBoundTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="snowflake",
            account="acct.region",
            user="alice",
            password="secret",
            database="ANALYTICS",
            warehouse="COMPUTE_WH",
            role="ANALYST",
        )
        self.adapter = SnowflakeAdapter(self.cfg)

    def test_list_materialized_views_uses_show_clause(self) -> None:
        # SHOW MATERIALIZED VIEWS returns rows where the second column is
        # the name; the adapter accepts either the `name` mapping or
        # positional `[1]`.
        engine, conn = _fake_engine(
            fetchall=[
                _FakeRow(
                    ["2026-04-30", "mv_kpis", "ANALYTICS"],
                    mapping={"name": "mv_kpis"},
                )
            ]
        )
        names = self.adapter.list_materialized_views(engine, "rep")
        self.assertEqual(names, ["mv_kpis"])


class DatabricksEngineBoundTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="databricks",
            host="dbc-xyz.cloud.databricks.com",
            access_token="dapi-test",
            http_path="/sql/1.0/warehouses/abc123",
            catalog="main",
            database="default",
        )
        self.adapter = DatabricksAdapter(self.cfg)

    def test_get_table_stats_parses_describe_detail_row(self) -> None:
        # DESCRIBE DETAIL returns a row with numRows under either
        # numRows or rowCount, depending on the Unity Catalog version.
        engine, _ = _fake_engine(
            fetchall=[
                _FakeRow(
                    ["main.retail.orders", 1234567, ...],
                    mapping={"numRows": 1234567},
                )
            ]
        )
        stats = self.adapter.get_table_stats(engine, "retail", "orders")
        self.assertEqual(stats["n_live_tup"], 1234567)
        self.assertEqual(stats["seq_scan"], 0)
        self.assertEqual(stats["idx_scan"], 0)

    def test_get_table_stats_returns_zeroes_when_describe_fails(self) -> None:
        """DESCRIBE DETAIL is not authorised on every Hive table; the
        adapter must absorb the failure and return zero stats so the
        rest of the profile run can continue."""
        from unittest.mock import MagicMock

        engine = MagicMock()
        cm = MagicMock()
        cm.__enter__ = MagicMock(
            return_value=MagicMock(execute=MagicMock(side_effect=RuntimeError("not authorized")))
        )
        cm.__exit__ = MagicMock(return_value=False)
        engine.connect = MagicMock(return_value=cm)

        stats = self.adapter.get_table_stats(engine, "retail", "orders")
        self.assertEqual(stats, {"seq_scan": 0, "idx_scan": 0, "n_live_tup": 0})


class BigQueryEngineBoundTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="bigquery",
            project="my-project",
            dataset="analytics",
            credentials_path="/tmp/sa.json",
        )
        self.adapter = BigQueryAdapter(self.cfg)

    def test_get_table_stats_reads_information_schema(self) -> None:
        engine, _ = _fake_engine(fetchone=_FakeRow([987_654_321]))
        stats = self.adapter.get_table_stats(engine, "retail", "orders")
        self.assertEqual(stats["n_live_tup"], 987_654_321)

    def test_get_table_stats_zero_when_row_missing(self) -> None:
        engine, _ = _fake_engine(fetchone=None)
        stats = self.adapter.get_table_stats(engine, "retail", "orders")
        self.assertEqual(stats["n_live_tup"], 0)


class PostgreSQLAdapterUnitTests(unittest.TestCase):
    """Pure-functional unit tests for the PostgreSQL adapter.

    The audit flagged that none of the four DB adapters had unit tests.
    This first batch covers the SQL-builder methods and the actionable-
    error categoriser — the parts that can be exercised without a live
    Postgres engine. Engine-bound methods (``list_materialized_views``,
    ``get_table_stats``, ``get_incoming_foreign_keys``) will get
    SQLAlchemy-mocked tests in a follow-up PR.
    """

    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="postgresql",
            host="db.example.com",
            port=5432,
            user="alice",
            password="secret",
            database="orders",
        )
        self.adapter = PostgreSQLAdapter(self.cfg)

    def test_system_schemas_excludes_expected_set(self) -> None:
        self.assertEqual(
            self.adapter.system_schemas(),
            frozenset({"information_schema", "pg_catalog", "pg_toast"}),
        )

    def test_actionable_profile_error_pg_stat_statements(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("ERROR: pg_stat_statements must be loaded via shared_preload_libraries")
        )
        self.assertIsNotNone(msg)
        self.assertIn("pg_stat_statements", msg)
        self.assertIn("postgresql.conf", msg)

    def test_actionable_profile_error_permission_denied(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("ERROR: permission denied for relation users")
        )
        self.assertIsNotNone(msg)
        self.assertIn("Insufficient privileges", msg)
        self.assertIn("SELECT", msg)

    def test_actionable_profile_error_undefined_table(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("UndefinedTable: relation does not exist")
        )
        self.assertIsNotNone(msg)
        self.assertIn("missing", msg.lower())

    def test_actionable_profile_error_database_does_not_exist(self) -> None:
        """The wizard advertises ``database`` as optional ("leave blank
        to pick at command time"). For that promise to hold, AMX must
        actually be able to connect when the field is blank — see
        ``test_postgres_url_falls_back_to_postgres_system_db_when_empty``
        for the URL-builder side. Once the fallback is in place, this
        error only fires when the user explicitly pinned a database
        name the server doesn't have, so the message points at /edit
        rather than blaming a "missing required field".
        """
        msg = self.adapter.actionable_profile_error(
            RuntimeError('FATAL:  database "wrong_name" does not exist')
        )
        self.assertIsNotNone(msg)
        self.assertIn("does not exist on this server", msg)
        self.assertIn("/edit", msg)
        # Tip should mention the blank-database fallback so users know
        # the optional path actually works now:
        self.assertIn("blank", msg.lower())
        # And NOT the misleading "Referenced relation" message:
        self.assertNotIn("Referenced relation", msg)

    def test_actionable_profile_error_unrecognised_returns_none(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("Some weird internal error AMX has not seen before")
        )
        self.assertIsNone(msg)

    def test_column_stats_sql_includes_required_aggregates(self) -> None:
        sql = self.adapter.column_stats_sql('"public"."users"', '"email"')
        self.assertIn('FROM "public"."users"', sql)
        # Must compute null_cnt, dist_cnt, min, max — each underpins a
        # different prompt-detail field in the LLM batch payload.
        for fragment in (
            'COUNT(*) FILTER (WHERE "email" IS NULL)',
            'COUNT(DISTINCT "email")',
            'MIN("email"::text)',
            'MAX("email"::text)',
        ):
            self.assertIn(fragment, sql)

    def test_column_sample_sql_uses_distinct_and_lim_param(self) -> None:
        sql = self.adapter.column_sample_sql('"public"."users"', '"email"')
        self.assertIn("DISTINCT", sql)
        self.assertIn(":lim", sql)
        self.assertIn("IS NOT NULL", sql)

    def test_set_table_comment_sql_uses_keyword_and_param(self) -> None:
        sql = self.adapter.set_table_comment_sql("public", "users", "TABLE")
        # COMMENT ON TABLE "public"."users" IS :cmt
        self.assertIn("COMMENT ON TABLE", sql)
        self.assertIn('"public"."users"', sql)
        self.assertIn(":cmt", sql)

    def test_set_table_comment_sql_supports_materialized_view(self) -> None:
        sql = self.adapter.set_table_comment_sql("rep", "daily_sales", "MATERIALIZED VIEW")
        self.assertIn("COMMENT ON MATERIALIZED VIEW", sql)

    def test_set_column_comment_sql_quotes_column(self) -> None:
        sql = self.adapter.set_column_comment_sql("public", "users", "email")
        self.assertIn("COMMENT ON COLUMN", sql)
        self.assertIn('"public"."users"."email"', sql)
        self.assertIn(":cmt", sql)

    def test_set_schema_and_database_comment_sql(self) -> None:
        schema_sql = self.adapter.set_schema_comment_sql("rep")
        self.assertEqual(schema_sql, 'COMMENT ON SCHEMA "rep" IS :cmt')

        # database name comes from cfg.database — must reflect the active config.
        db_sql = self.adapter.set_database_comment_sql()
        self.assertEqual(db_sql, 'COMMENT ON DATABASE "orders" IS :cmt')

    def test_capabilities_advertise_postgres_features(self) -> None:
        caps = self.adapter.capabilities
        self.assertTrue(caps.relationships)
        self.assertTrue(caps.row_count_stats)
        self.assertTrue(caps.materialized_views)
        self.assertTrue(caps.materialized_view_comments)
        self.assertIn("MATERIALIZED VIEW", caps.comment_asset_keywords)


class SnowflakeAdapterUnitTests(unittest.TestCase):
    """Pure-functional Snowflake adapter unit tests."""

    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="snowflake",
            account="acct.region",
            user="alice",
            password="secret",
            database="ANALYTICS",
            warehouse="COMPUTE_WH",
            role="ANALYST",
        )
        self.adapter = SnowflakeAdapter(self.cfg)

    def test_system_schemas_excludes_information_schema_both_cases(self) -> None:
        # Snowflake matches by case-insensitive lookups but the adapter
        # ships both forms so callers can compare without lowercasing.
        self.assertIn("INFORMATION_SCHEMA", self.adapter.system_schemas())
        self.assertIn("information_schema", self.adapter.system_schemas())

    def test_actionable_profile_error_insufficient_privileges(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("Insufficient privileges to operate on schema 'CORE'")
        )
        self.assertIsNotNone(msg)
        self.assertIn("Snowflake", msg)
        self.assertIn("USAGE", msg)

    def test_actionable_profile_error_warehouse_suspended(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("Warehouse COMPUTE_WH is currently suspended")
        )
        self.assertIsNotNone(msg)
        self.assertIn("warehouse", msg.lower())

    def test_column_stats_uses_snowflake_varchar_cast(self) -> None:
        sql = self.adapter.column_stats_sql('"CORE"."USERS"', '"EMAIL"')
        # Snowflake uses VARCHAR for cast and SUM(CASE…) instead of FILTER.
        self.assertIn("::VARCHAR", sql)
        self.assertIn("SUM(CASE WHEN", sql)
        self.assertNotIn("::text", sql)

    def test_column_sample_uses_snowflake_sample_clause(self) -> None:
        sql = self.adapter.column_sample_sql('"CORE"."USERS"', '"EMAIL"')
        self.assertIn("SAMPLE (1)", sql)
        self.assertIn(":lim", sql)

    def test_set_database_comment_uses_active_database(self) -> None:
        # Critical: the SQL must reference cfg.database, not a hardcoded value.
        sql = self.adapter.set_database_comment_sql()
        self.assertEqual(sql, 'COMMENT ON DATABASE "ANALYTICS" IS :cmt')


class DatabricksAdapterUnitTests(unittest.TestCase):
    """Pure-functional Databricks adapter unit tests."""

    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="databricks",
            host="dbc-xyz.cloud.databricks.com",
            access_token="dapi-test",
            http_path="/sql/1.0/warehouses/abc123",
            catalog="main",
            database="default",
        )
        self.adapter = DatabricksAdapter(self.cfg)

    def test_actionable_profile_error_invalid_token(self) -> None:
        msg = self.adapter.actionable_profile_error(RuntimeError("HTTP 401: invalid access token"))
        self.assertIsNotNone(msg)
        self.assertIn("Databricks access token", msg)
        self.assertIn("PAT", msg)

    def test_actionable_profile_error_ca_bundle_missing(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("trusted CA bundle file was not found at /etc/ssl/corp.pem")
        )
        self.assertIsNotNone(msg)
        self.assertIn("tls_trusted_ca_file", msg)
        self.assertIn("AMX_DATABRICKS_TRUSTED_CA_FILE", msg)

    def test_actionable_profile_error_certificate_verify_failed(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("SSLError: CERTIFICATE_VERIFY_FAILED self-signed certificate")
        )
        self.assertIsNotNone(msg)
        self.assertIn("TLS", msg)

    def test_fully_qualified_name_uses_catalog_when_set(self) -> None:
        fqn = self.adapter.fully_qualified_name("retail", "orders")
        self.assertEqual(fqn, "`main`.`retail`.`orders`")

    def test_fully_qualified_name_omits_catalog_when_empty(self) -> None:
        cfg = DBConfig(
            backend="databricks",
            host="h",
            access_token="t",
            http_path="/p",
            catalog="",
            database="d",
        )
        adapter = DatabricksAdapter(cfg)
        self.assertEqual(adapter.fully_qualified_name("retail", "orders"), "`retail`.`orders`")

    def test_column_stats_uses_databricks_string_cast(self) -> None:
        sql = self.adapter.column_stats_sql("`main`.`retail`.`orders`", "`status`")
        self.assertIn("CAST(`status` AS STRING)", sql)
        self.assertIn("SUM(CASE WHEN", sql)


class BigQueryAdapterUnitTests(unittest.TestCase):
    """Pure-functional BigQuery adapter unit tests."""

    def setUp(self) -> None:
        self.cfg = DBConfig(
            backend="bigquery",
            project="my-project",
            dataset="analytics",
            credentials_path="/tmp/sa.json",
        )
        self.adapter = BigQueryAdapter(self.cfg)

    def test_actionable_profile_error_access_denied(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("403 Access Denied: BigQuery dataset")
        )
        self.assertIsNotNone(msg)
        self.assertIn("BigQuery", msg)
        self.assertIn("metadata read", msg.lower())

    def test_actionable_profile_error_quota_exhausted(self) -> None:
        msg = self.adapter.actionable_profile_error(
            RuntimeError("Quota exceeded: rate limit on tabledata.list")
        )
        self.assertIsNotNone(msg)
        self.assertIn("quota", msg.lower())

    def test_fully_qualified_name_uses_project(self) -> None:
        fqn = self.adapter.fully_qualified_name("retail", "orders")
        self.assertEqual(fqn, "`my-project`.`retail`.`orders`")

    def test_column_stats_uses_countif_idiom(self) -> None:
        # COUNTIF is a BigQuery-specific builtin; keep it instead of FILTER /
        # SUM-CASE so the query uses the optimised approximate counter.
        sql = self.adapter.column_stats_sql("`my-project`.`retail`.`orders`", "`status`")
        self.assertIn("COUNTIF(`status` IS NULL)", sql)
        self.assertIn("CAST(`status` AS STRING)", sql)

    def test_column_sample_uses_tablesample_system(self) -> None:
        sql = self.adapter.column_sample_sql("`my-project`.`retail`.`orders`", "`status`")
        self.assertIn("TABLESAMPLE SYSTEM (1 PERCENT)", sql)
        self.assertIn(":lim", sql)


class PerProfileCollectionTests(unittest.TestCase):
    """The Week-3 SearchIndex now uses one Chroma collection per db_profile
    so cross-profile pollution is impossible — these tests pin the naming
    and isolation guarantees."""

    def test_collection_name_for_empty_profile_is_legacy(self) -> None:
        from amx.search.index import _collection_name_for

        self.assertEqual(_collection_name_for(""), "amx_search")

    def test_collection_name_is_deterministic_and_chroma_valid(self) -> None:
        """Same profile in → same collection name out, and the name only
        contains characters Chroma accepts (alnum, dot, dash, underscore)."""
        import re

        from amx.search.index import _collection_name_for

        a = _collection_name_for("prod")
        b = _collection_name_for("prod")
        self.assertEqual(a, b)
        self.assertNotEqual(a, "amx_search")  # different from the legacy name
        self.assertTrue(re.match(r"^[a-zA-Z0-9._-]{3,63}$", a))

    def test_two_profiles_get_distinct_collection_names(self) -> None:
        from amx.search.index import _collection_name_for

        self.assertNotEqual(_collection_name_for("prod"), _collection_name_for("dev"))

    def test_profile_name_with_unicode_and_spaces_hashes_safely(self) -> None:
        """Profile names can contain anything users type; the name fed to
        Chroma must still be a valid identifier."""
        import re

        from amx.search.index import _collection_name_for

        name = _collection_name_for("müşteri / prod tablosu 🚀")
        self.assertTrue(re.match(r"^[a-zA-Z0-9._-]{3,63}$", name))

    def test_search_index_routes_upsert_per_profile(self) -> None:
        """Two profiles' rows must land in two different collections, even
        within a single ``upsert_entities`` call."""
        from amx.search import index as index_module

        captured: dict[str, list[dict]] = {}

        class FakeCollection:
            def __init__(self, name: str) -> None:
                self.name = name

            def upsert(self, *, ids, documents, metadatas):
                captured.setdefault(self.name, []).extend(metadatas)

            def delete(self, *_, **__):
                pass

            def get(self, *_, **__):
                return {"ids": []}

            def query(self, *_, **__):
                return {"documents": [[]], "metadatas": [[]], "distances": [[]]}

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                self._collections: dict[str, FakeCollection] = {}

            def get_or_create_collection(self, *, name, **_):
                col = self._collections.get(name) or FakeCollection(name)
                self._collections[name] = col
                return col

        with tempfile.TemporaryDirectory() as td:
            with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
                idx = index_module.SearchIndex(persist_dir=td)
                idx.upsert_entities(
                    [
                        {
                            "id": 1,
                            "search_text": "row in prod",
                            "db_profile": "prod",
                            "schema_name": "s",
                            "table_name": "t",
                            "column_name": "c",
                            "entity_kind": "column",
                        },
                        {
                            "id": 2,
                            "search_text": "row in dev",
                            "db_profile": "dev",
                            "schema_name": "s",
                            "table_name": "t",
                            "column_name": "c",
                            "entity_kind": "column",
                        },
                    ]
                )

        prod_name = index_module._collection_name_for("prod")
        dev_name = index_module._collection_name_for("dev")
        self.assertIn(prod_name, captured)
        self.assertIn(dev_name, captured)
        self.assertEqual(len(captured[prod_name]), 1)
        self.assertEqual(len(captured[dev_name]), 1)
        self.assertEqual(captured[prod_name][0]["db_profile"], "prod")
        self.assertEqual(captured[dev_name][0]["db_profile"], "dev")

    def test_query_does_not_filter_on_db_profile_metadata(self) -> None:
        """Now that each profile has its own collection, the query layer
        must NOT pass a ``where`` clause — that was the previous
        cross-pollution-prone design."""
        from amx.search import index as index_module

        captured: dict[str, object] = {"explicit_kwargs": {}}

        class FakeCollection:
            def query(self, *, query_texts, n_results, **kwargs):
                # We capture the *explicit* kwargs so we can assert that
                # `where` was not passed. Chroma's API does not require
                # it to be present at all when each collection is
                # already profile-scoped.
                captured["explicit_kwargs"] = dict(kwargs)
                captured["n_results"] = n_results
                captured["query_texts"] = query_texts
                return {"documents": [[]], "metadatas": [[]], "distances": [[]]}

            def upsert(self, *_, **__):
                pass

            def delete(self, *_, **__):
                pass

            def get(self, *_, **__):
                return {"ids": []}

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                pass

            def get_or_create_collection(self, **_):
                return FakeCollection()

        with tempfile.TemporaryDirectory() as td:
            with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
                idx = index_module.SearchIndex(persist_dir=td)
                idx.query("what is X?", db_profile="prod", n_results=5)

        self.assertNotIn("where", captured["explicit_kwargs"])
        self.assertEqual(captured.get("n_results"), 5)


class SearchIndexEmbeddingTests(unittest.TestCase):
    """Verify SearchIndex wires the embedding_function param through to Chroma."""

    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tempdir.cleanup)

    def test_default_construction_does_not_pass_embedding_function(self) -> None:
        from amx.search import index as index_module

        captured: dict[str, object] = {}

        class FakeCollection:
            def __init__(self, **_: object) -> None:
                pass

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                pass

            def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
                captured.update(kwargs)
                return FakeCollection()

        with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
            index_module.SearchIndex(persist_dir=self._tempdir.name)

        # Default behaviour: no embedding_function → Chroma uses MiniLM.
        self.assertNotIn("embedding_function", captured)
        self.assertEqual(captured.get("name"), "amx_search")

    def test_custom_embedding_function_is_passed_through(self) -> None:
        from amx.search import index as index_module

        captured: dict[str, object] = {}

        class FakeCollection:
            def __init__(self, **_: object) -> None:
                pass

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                pass

            def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
                captured.update(kwargs)
                return FakeCollection()

        sentinel_ef = object()
        with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
            idx = index_module.SearchIndex(
                persist_dir=self._tempdir.name,
                embedding_function=sentinel_ef,  # type: ignore[arg-type]
            )

        self.assertIs(captured.get("embedding_function"), sentinel_ef)
        self.assertIs(idx.embedding_function, sentinel_ef)


class ConfigTransactionTests(unittest.TestCase):
    """Regression tests for the Week-2 transactional config writes."""

    def _save_count_wrapper(self, cfg: AMXConfig) -> list[int]:
        """Replace cfg.save with a counter-incrementing wrapper. Returns a
        single-element list so callers can read the running count."""
        counter = [0]
        original = cfg.save

        def counting_save(path: str | None = None) -> Path:
            counter[0] += 1
            return original(path)

        cfg.save = counting_save  # type: ignore[method-assign]
        return counter

    def test_transaction_collapses_multiple_mutations_into_one_save(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            counter = self._save_count_wrapper(cfg)

            with cfg.transaction():
                cfg.db.host = "db.prod.example.com"
                cfg.db.user = "alice"
                cfg.db.password = "secret"
                cfg.db.database = "orders"

            self.assertEqual(counter[0], 1)
            self.assertEqual(cfg.db.host, "db.prod.example.com")

    def test_transaction_block_raises_does_not_save(self) -> None:
        """If the block raises, the YAML must not be updated — the in-memory
        state may be partially mutated, but the disk stays consistent."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.save(str(cfg_path))

            counter = self._save_count_wrapper(cfg)
            with self.assertRaises(RuntimeError):
                with cfg.transaction():
                    cfg.db.host = "should-not-persist"
                    raise RuntimeError("boom")

            # No save fired despite the mutation.
            self.assertEqual(counter[0], 0)

    def test_nested_transactions_only_save_on_outermost_exit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            counter = self._save_count_wrapper(cfg)

            with cfg.transaction():
                cfg.db.host = "outer"
                with cfg.transaction():
                    cfg.db.user = "inner-user"
                    cfg.db.password = "inner-pw"
                # Inner block exited, but outer still active — no save yet.
                self.assertEqual(counter[0], 0)
                cfg.db.database = "outer-db"

            self.assertEqual(counter[0], 1)

    def test_transaction_suppresses_upsert_autosave_until_exit(self) -> None:
        """Profile upsert helpers call _autosave directly, so transactions
        must suppress that path too. Otherwise add+activate can still write
        an intermediate YAML snapshot before the profile mirror is coherent."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            counter = self._save_count_wrapper(cfg)

            with cfg.transaction():
                cfg.upsert_db_profile(
                    "prod",
                    DBConfig(
                        backend="postgresql",
                        host="prod.db.example.com",
                        user="alice",
                        password="secret",
                        database="orders",
                    ),
                )
                self.assertEqual(counter[0], 0)
                cfg.set_active_db_profile("prod")
                self.assertEqual(counter[0], 0)

            self.assertEqual(counter[0], 1)
            reloaded = AMXConfig.load(str(cfg_path))
            self.assertEqual(reloaded.active_db_profile, "prod")
            self.assertEqual(reloaded.db_profiles["prod"].host, "prod.db.example.com")

    def test_transaction_with_autosave_disabled_does_not_save(self) -> None:
        """Honour ``write_through_config = False`` even inside a transaction —
        users who opt out of write-through should not get a stealth save."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            cfg.write_through_config = False
            counter = self._save_count_wrapper(cfg)

            with cfg.transaction():
                cfg.db.host = "h"
                cfg.db.user = "u"

            self.assertEqual(counter[0], 0)


class LLMTransientRetryTests(unittest.TestCase):
    """Week-3 polish: LLM provider should retry transient failures (429,
    timeouts, 5xx, connection reset) once or twice with exponential backoff
    before giving up, while letting non-transient errors propagate
    immediately."""

    def setUp(self) -> None:
        from amx.config import LLMConfig
        from amx.llm.provider import LLMProvider

        self._llm_cfg = LLMConfig(
            provider="openai",
            model="gpt-4o-mini",
            api_key="sk-test",
            api_base=None,
            temperature=0.2,
            max_tokens=256,
        )
        self._provider = LLMProvider(self._llm_cfg)
        # Speed up the backoff so tests stay snappy.
        self._sleep_patcher = patch("amx.llm.provider.time.sleep", return_value=None)
        self._sleep_patcher.start()

    def tearDown(self) -> None:
        self._sleep_patcher.stop()

    def _patch_litellm_with(self, completion_fn) -> object:
        fake = SimpleNamespace(completion=completion_fn)
        return patch("amx.llm.provider._litellm", return_value=fake)

    def _ok_response(self) -> SimpleNamespace:
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content="ok"),
                    finish_reason="stop",
                    logprobs=None,
                )
            ],
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5, total_tokens=15),
        )

    def test_transient_429_retried_then_succeeds(self) -> None:
        """A RateLimitError on the first call should be retried; the second
        call returns a valid response and the chat() call succeeds."""

        class RateLimitError(Exception):
            pass

        attempts: list[int] = []

        def fake_completion(**_: object) -> object:
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                raise RateLimitError("Rate limit reached for requests")
            return self._ok_response()

        with self._patch_litellm_with(fake_completion):
            result = self._provider.chat([{"role": "user", "content": "hi"}])

        self.assertEqual(len(attempts), 2)
        self.assertEqual(result.content, "ok")

    def test_transient_timeout_retried_max_two_times_then_raises(self) -> None:
        """If transient failures persist past MAX_LLM_RETRIES, the final
        exception propagates so callers can surface it to the user."""

        class APITimeoutError(Exception):
            pass

        calls = {"count": 0}

        def fake_completion(**_: object) -> object:
            calls["count"] += 1
            raise APITimeoutError("Request timed out")

        with self._patch_litellm_with(fake_completion):
            with self.assertRaises(APITimeoutError):
                self._provider.chat([{"role": "user", "content": "hi"}])

        # MAX_LLM_RETRIES=2 → 3 total attempts (initial + 2 retries).
        self.assertEqual(calls["count"], 3)

    # ── Logprobs auto-fallback ──────────────────────────────────────────

    def test_logprobs_unsupported_error_detector_matches_gemini_message(self) -> None:
        """The user-reported Gemini Flash error verbatim must be detected.
        Other phrasings producers might use ("logprobs not supported"
        etc.) are also covered by the pattern set."""
        from amx.llm.provider import _is_logprobs_unsupported_error

        gemini_msg = (
            "litellm.BadRequestError: GeminiException BadRequestError - {\n"
            '  "error": {\n'
            '    "code": 400,\n'
            '    "message": "Logprobs is not enabled for this model",\n'
            '    "status": "INVALID_ARGUMENT"\n  }\n}'
        )
        self.assertTrue(_is_logprobs_unsupported_error(RuntimeError(gemini_msg)))
        self.assertTrue(_is_logprobs_unsupported_error(RuntimeError("logprobs not supported")))
        self.assertTrue(_is_logprobs_unsupported_error(RuntimeError("does not support logprobs")))
        # Unrelated 400 must NOT match — we don't want to swallow real
        # bad-request errors as if they were logprobs issues.
        self.assertFalse(_is_logprobs_unsupported_error(RuntimeError("invalid request: bad model")))

    def test_chat_retries_without_logprobs_when_provider_rejects_them(self) -> None:
        """User report 2026-05-02: gemini/gemini-flash-latest returned a
        400 with ``Logprobs is not enabled for this model``. Pre-fix
        AMX retried 3× with the same flag (all failing) and finally
        surfaced the error. Post-fix the provider strips ``logprobs``
        from the second attempt and the call succeeds.
        """

        class BadRequestError(Exception):
            pass

        calls: list[dict] = []

        def fake_completion(**kwargs):
            calls.append(dict(kwargs))
            if "logprobs" in kwargs:
                raise BadRequestError(
                    'GeminiException BadRequestError - {"error": {"code": 400, '
                    '"message": "Logprobs is not enabled for this model"}}'
                )
            return self._ok_response()

        with self._patch_litellm_with(fake_completion):
            result = self._provider.chat([{"role": "user", "content": "hi"}])

        self.assertEqual(result.content, "ok")
        # Exactly two calls: first with logprobs (rejected), second without.
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0].get("logprobs"))
        self.assertNotIn("logprobs", calls[1])
        # And the runtime-disable flag is now set so subsequent chats
        # in this session skip the logprobs request upfront.
        self.assertTrue(getattr(self._provider, "_logprobs_runtime_disabled", False))
        self.assertFalse(self._provider.supports_logprobs)

    def test_subsequent_chat_after_logprobs_rejection_skips_logprobs_upfront(self) -> None:
        """Once the runtime-disable flag is set, the NEXT chat() call
        must not request logprobs at all — otherwise every call in a
        long /run would hit the same 400, retry once, and continue.
        """

        class BadRequestError(Exception):
            pass

        seen_logprobs_keys: list[bool] = []

        def fake_completion(**kwargs):
            seen_logprobs_keys.append("logprobs" in kwargs)
            if "logprobs" in kwargs:
                raise BadRequestError("Logprobs is not enabled for this model")
            return self._ok_response()

        with self._patch_litellm_with(fake_completion):
            self._provider.chat([{"role": "user", "content": "hi"}])
            # Reset the per-call tracking before the second call so we
            # measure only the second invocation's logprobs flag.
            calls_after = list(seen_logprobs_keys)
            self._provider.chat([{"role": "user", "content": "again"}])

        # First chat: 1st attempt with logprobs (rejected) + 2nd without (ok).
        self.assertEqual(calls_after, [True, False])
        # Second chat: single attempt, logprobs already disabled.
        self.assertEqual(seen_logprobs_keys[len(calls_after) :], [False])

    @pytest.mark.live
    def test_non_transient_error_does_not_retry(self) -> None:
        """Authentication / bad-request style errors must propagate
        immediately so the user sees the categorised error fast — retrying
        a 401 would just delay an actionable message."""

        class AuthenticationError(Exception):
            pass

        calls = {"count": 0}

        def fake_completion(**_: object) -> object:
            calls["count"] += 1
            raise AuthenticationError("Incorrect API key")

        with self._patch_litellm_with(fake_completion):
            with self.assertRaises(AuthenticationError):
                self._provider.chat([{"role": "user", "content": "hi"}])

        # No retry — should be exactly one attempt.
        self.assertEqual(calls["count"], 1)

    def test_message_token_pattern_classifies_transient(self) -> None:
        """Even when the exception class is generic (RuntimeError), the
        retry layer should fall back to substring matching on common
        transient phrases."""

        attempts: list[int] = []

        def fake_completion(**_: object) -> object:
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                raise RuntimeError("503 Service Unavailable: upstream busy")
            return self._ok_response()

        with self._patch_litellm_with(fake_completion):
            result = self._provider.chat([{"role": "user", "content": "hi"}])

        self.assertEqual(len(attempts), 2)
        self.assertEqual(result.content, "ok")

    def test_is_transient_llm_error_classifications(self) -> None:
        from amx.llm.provider import _is_transient_llm_error

        # Class-name based.
        class RateLimitError(Exception):
            pass

        class APIConnectionError(Exception):
            pass

        self.assertTrue(_is_transient_llm_error(RateLimitError("...")))
        self.assertTrue(_is_transient_llm_error(APIConnectionError("...")))
        # Built-in stdlib transients.
        self.assertTrue(_is_transient_llm_error(TimeoutError()))
        self.assertTrue(_is_transient_llm_error(ConnectionError()))
        # Substring-based.
        self.assertTrue(_is_transient_llm_error(RuntimeError("Read timed out")))
        self.assertTrue(_is_transient_llm_error(RuntimeError("502 Bad Gateway")))
        # Non-transients.
        self.assertFalse(_is_transient_llm_error(ValueError("bad input")))
        self.assertFalse(_is_transient_llm_error(RuntimeError("invalid api key")))


class EmbeddingsSlashCommandTests(unittest.TestCase):
    """The /embeddings slash command lets users switch provider without
    hand-editing ~/.amx/config.yml. Each branch reinstalls the runtime
    factory so subsequent /search queries pick up the new provider."""

    def setUp(self) -> None:
        from amx.search import embeddings as embeddings_module

        self.embeddings_module = embeddings_module
        embeddings_module.set_default_embedding_function(None)

    def tearDown(self) -> None:
        self.embeddings_module.set_default_embedding_function(None)

    def test_minilm_branch_resets_provider(self) -> None:
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()

        # Pretend a non-default factory is currently installed.
        def sentinel_factory():
            return object()

        self.embeddings_module.set_default_embedding_function(sentinel_factory)
        self.assertIsNotNone(self.embeddings_module._default_factory)

        cmd_embeddings(cfg, ["minilm"])

        self.assertEqual(cfg.embedding.kind, "minilm")
        self.assertEqual(cfg.embedding.model, "")
        # MiniLM means: clear the factory so Chroma uses its bundled default.
        self.assertIsNone(self.embeddings_module._default_factory)

    def test_openai_branch_with_model_arg_installs_factory(self) -> None:
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()
        with (
            patch(
                "amx.cli_support.commands.embeddings.ask", return_value="https://api.openai.com/v1"
            ),
            patch("amx.cli_support.commands.embeddings.ask_password", return_value="sk-test"),
        ):
            cmd_embeddings(cfg, ["openai", "text-embedding-3-small"])

        self.assertEqual(cfg.embedding.kind, "openai_compatible")
        self.assertEqual(cfg.embedding.model, "text-embedding-3-small")
        self.assertEqual(cfg.embedding.api_key, "sk-test")
        self.assertEqual(cfg.embedding.base_url, "https://api.openai.com/v1")
        # Factory installed and points at OpenAI-compatible.
        self.assertIsNotNone(self.embeddings_module._default_factory)

    def test_local_branch_with_model_arg(self) -> None:
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()
        cmd_embeddings(cfg, ["local", "BAAI/bge-large-en-v1.5"])

        self.assertEqual(cfg.embedding.kind, "sentence_transformers")
        self.assertEqual(cfg.embedding.model, "BAAI/bge-large-en-v1.5")
        self.assertEqual(cfg.embedding.api_key, "")

    def test_unknown_kind_emits_error_without_mutating_config(self) -> None:
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()
        original_kind = cfg.embedding.kind

        # Should not raise — the error is printed via console.error().
        cmd_embeddings(cfg, ["totally-not-a-kind"])

        self.assertEqual(cfg.embedding.kind, original_kind)

    def test_openai_branch_rejects_empty_model(self) -> None:
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()
        original_kind = cfg.embedding.kind
        # `ask` returns "" → command must error rather than silently install
        # an unusable provider.
        with patch("amx.cli_support.commands.embeddings.ask", return_value=""):
            cmd_embeddings(cfg, ["openai"])

        self.assertEqual(cfg.embedding.kind, original_kind)

    def test_picker_default_is_current_provider_label(self) -> None:
        """The interactive picker (no args) must default to the current
        provider's labelled choice rather than a separate ambiguous "keep"
        option, and selecting that default must be a no-op."""
        from amx.cli_support.commands.embeddings import cmd_embeddings

        cfg = AMXConfig()
        original_kind = cfg.embedding.kind  # "minilm" out of the box
        # Simulate the user pressing Enter (returns the default).
        with patch(
            "amx.cli_support.commands.embeddings.ask_choice",
            side_effect=lambda *_args, **kwargs: kwargs.get("default", ""),
        ):
            cmd_embeddings(cfg, [])
        self.assertEqual(cfg.embedding.kind, original_kind)

    def test_picker_explicit_cancel_does_not_mutate(self) -> None:
        from amx.cli_support.commands.embeddings import _LABEL_CANCEL, cmd_embeddings

        cfg = AMXConfig()
        original_kind = cfg.embedding.kind
        with patch(
            "amx.cli_support.commands.embeddings.ask_choice",
            return_value=_LABEL_CANCEL,
        ):
            cmd_embeddings(cfg, [])
        self.assertEqual(cfg.embedding.kind, original_kind)

    def test_picker_minilm_choice_routes_to_minilm_branch(self) -> None:
        """Selecting the verbose 'MiniLM' label from the picker must route
        through the same code path as `/embeddings minilm`."""
        from amx.cli_support.commands.embeddings import _LABEL_MINILM, cmd_embeddings
        from amx.config import EmbeddingConfig

        cfg = AMXConfig()
        cfg.embedding = EmbeddingConfig(
            kind="openai_compatible",
            model="text-embedding-3-small",
            api_key="sk-old",
            base_url="https://api.openai.com/v1",
        )

        # User picks the MiniLM option from the verbose-label picker.
        with patch(
            "amx.cli_support.commands.embeddings.ask_choice",
            return_value=_LABEL_MINILM,
        ):
            cmd_embeddings(cfg, [])

        self.assertEqual(cfg.embedding.kind, "minilm")
        self.assertEqual(cfg.embedding.model, "")
        self.assertEqual(cfg.embedding.api_key, "")


class EmbeddingDefaultFactoryTests(unittest.TestCase):
    """The singleton in amx.search.embeddings lets the CLI install the
    user's chosen provider once at startup so all later SearchIndex
    constructors pick it up without plumbing cfg through every caller."""

    def setUp(self) -> None:
        from amx.search import embeddings as embeddings_module

        self.embeddings_module = embeddings_module
        # Reset between tests so previous installs don't bleed across cases.
        embeddings_module.set_default_embedding_function(None)

    def tearDown(self) -> None:
        self.embeddings_module.set_default_embedding_function(None)

    def test_default_factory_returns_none_when_unset(self) -> None:
        self.assertIsNone(self.embeddings_module.get_default_embedding_function())

    def test_set_and_get_default_factory_round_trip(self) -> None:
        sentinel = object()
        self.embeddings_module.set_default_embedding_function(lambda: sentinel)
        self.assertIs(self.embeddings_module.get_default_embedding_function(), sentinel)

    def test_default_factory_failure_swallowed_returns_none(self) -> None:
        def boom() -> object:
            raise RuntimeError("no model")

        self.embeddings_module.set_default_embedding_function(boom)
        self.assertIsNone(self.embeddings_module.get_default_embedding_function())

    def test_search_index_falls_back_to_default_factory(self) -> None:
        from amx.search import index as index_module

        captured: dict[str, object] = {}
        sentinel = object()

        class FakeCollection:
            def __init__(self, **_: object) -> None:
                pass

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                pass

            def get_or_create_collection(self, **kwargs: object) -> FakeCollection:
                captured.update(kwargs)
                return FakeCollection()

        self.embeddings_module.set_default_embedding_function(lambda: sentinel)
        with tempfile.TemporaryDirectory() as td:
            with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
                index_module.SearchIndex(persist_dir=td)

        # Default factory's sentinel was wired into the Chroma collection.
        self.assertIs(captured.get("embedding_function"), sentinel)

    def test_search_index_explicit_arg_bypasses_default_factory(self) -> None:
        from amx.search import index as index_module

        captured: dict[str, object] = {}
        explicit = object()

        class FakeClient:
            def __init__(self, *, path: str) -> None:  # noqa: ARG002
                pass

            def get_or_create_collection(self, **kwargs: object) -> object:
                captured.update(kwargs)
                return object()

        # A different sentinel is registered as the default; the explicit arg
        # must win over it so callers retain control.
        self.embeddings_module.set_default_embedding_function(lambda: object())
        with tempfile.TemporaryDirectory() as td:
            with patch.object(index_module.chromadb, "PersistentClient", FakeClient):
                index_module.SearchIndex(
                    persist_dir=td,
                    embedding_function=explicit,  # type: ignore[arg-type]
                )

        self.assertIs(captured.get("embedding_function"), explicit)


class EmbeddingConfigPersistenceTests(unittest.TestCase):
    """The Week-3 EmbeddingConfig integrates with AMXConfig load/save and the
    OS-keyring secret externalisation. These tests pin the round-trip and
    confirm the api_key never lands in plaintext on disk."""

    def setUp(self) -> None:
        from amx.storage.secrets import InMemorySecretStore, set_default_store

        self._store = InMemorySecretStore()
        set_default_store(self._store)

    def tearDown(self) -> None:
        from amx.storage.secrets import set_default_store

        set_default_store(None)

    def test_default_embedding_is_minilm(self) -> None:
        cfg = AMXConfig()
        self.assertEqual(cfg.embedding.kind, "minilm")
        self.assertEqual(cfg.embedding.model, "")
        self.assertEqual(cfg.embedding.api_key, "")
        self.assertTrue(cfg.embedding.is_configured())

    def test_openai_compatible_requires_model_to_be_configured(self) -> None:
        from amx.config import EmbeddingConfig

        self.assertFalse(EmbeddingConfig(kind="openai_compatible", model="").is_configured())
        self.assertTrue(
            EmbeddingConfig(
                kind="openai_compatible", model="text-embedding-3-small"
            ).is_configured()
        )

    def test_save_and_load_round_trip_preserves_embedding_settings(self) -> None:
        from amx.config import EmbeddingConfig

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.embedding = EmbeddingConfig(
                kind="openai_compatible",
                model="text-embedding-3-large",
                api_key="sk-embed-1234",
                base_url="https://api.openai.com/v1",
            )
            cfg.save(str(cfg_path))

            reloaded = AMXConfig.load(str(cfg_path))
            self.assertEqual(reloaded.embedding.kind, "openai_compatible")
            self.assertEqual(reloaded.embedding.model, "text-embedding-3-large")
            self.assertEqual(reloaded.embedding.api_key, "sk-embed-1234")
            self.assertEqual(reloaded.embedding.base_url, "https://api.openai.com/v1")

    def test_embedding_api_key_externalised_to_keyring(self) -> None:
        from amx.config import EmbeddingConfig

        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.embedding = EmbeddingConfig(
                kind="openai_compatible",
                model="text-embedding-3-small",
                api_key="sk-must-not-leak",
                base_url="https://api.openai.com/v1",
            )
            cfg.save(str(cfg_path))

            yaml_text = cfg_path.read_text()
            self.assertNotIn("sk-must-not-leak", yaml_text)
            self.assertIn("keyring:embedding/api_key", yaml_text)
            self.assertEqual(self._store.get("embedding/api_key"), "sk-must-not-leak")

    def test_embedding_legacy_plaintext_loads_without_keyring(self) -> None:
        """A YAML written before keyring integration (or by a user with
        keyring unavailable) keeps working: api_key flows straight into the
        in-memory dataclass, and the next save migrates it."""
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "embedding:\n"
                "  kind: openai_compatible\n"
                "  model: text-embedding-3-small\n"
                "  api_key: sk-plaintext-legacy\n"
                "  base_url: https://api.openai.com/v1\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(cfg.embedding.api_key, "sk-plaintext-legacy")

            cfg.save(str(cfg_path))
            self.assertEqual(self._store.get("embedding/api_key"), "sk-plaintext-legacy")
            self.assertNotIn("sk-plaintext-legacy", cfg_path.read_text())


class FirstRunConfigTests(unittest.TestCase):
    """Regression tests for the Week-2 first-run UX hardening."""

    def test_load_from_missing_path_does_not_create_default_profile(self) -> None:
        """A truly fresh install must not silently auto-create a placeholder profile.

        The pre-Week-2 behavior wrote a 'default' DB profile pointing at
        localhost / user 'amx' / password 'amx_pass' / database 'SAP', which
        masquerades as configured even when the user has done nothing.
        """
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            self.assertTrue(cfg.is_first_run)
            self.assertEqual(dict(cfg.db_profiles), {})
            self.assertEqual(cfg.active_db_profile, "")
            self.assertEqual(dict(cfg.llm_profiles), {})
            self.assertEqual(cfg.active_llm_profile, "")

    def test_save_on_fresh_install_writes_clean_yaml(self) -> None:
        """A user's first ``cfg.save()`` after a fresh install must not
        leak phantom top-level ``db:`` / ``llm:`` blocks built from the
        empty active-mirror dataclasses. Pre-fix the YAML file contained
        a fake postgresql connection (host=localhost, port=5432,
        database='') and an empty LLM block, both of which masqueraded
        as configuration the user never entered.
        """
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig.load(str(cfg_path))
            cfg.save()
            text = cfg_path.read_text()
            self.assertIn("db_profiles: {}", text)
            self.assertIn("llm_profiles: {}", text)
            self.assertNotIn("password:", text, "no credential field in clean YAML")
            self.assertNotIn("amx_pass", text, "no demo password leaked")
            # The top-level ``db:`` and ``llm:`` mirrors only get written
            # when at least one profile exists. On a fresh install both
            # profile dicts are empty so neither block should appear.
            self.assertNotRegex(
                text,
                r"^db:\s*$",
                msg="top-level db: block should not appear on fresh install",
            )
            self.assertNotRegex(
                text,
                r"^llm:\s*$",
                msg="top-level llm: block should not appear on fresh install",
            )

    def test_test_suite_does_not_pollute_developer_home_dir(self) -> None:
        """An autouse conftest fixture redirects ``Path.home()`` to a
        per-test tempdir. Without it, a test that does ``AMXConfig()``
        followed by anything that triggers ``cfg.save()`` (e.g.
        ``cmd_add_profile``) overwrites the developer's real
        ``~/.amx/config.yml`` — the 2026-05-02 user-reported regression
        where ``databricks-default`` showed up in a fresh install.
        """
        # Path.home() must point at a tempdir during this test. The fact
        # that we can write a sentinel file there and find it immediately
        # — without seeing it appear in the developer's actual
        # ``$HOME/.amx-test-sentinel`` after the test — proves the
        # isolation. Pytest cleans up the tempdir automatically.
        sentinel = Path.home() / ".amx-test-sentinel"
        sentinel.write_text("test")
        self.assertTrue(sentinel.exists())
        # The CONFIG_DIR resolved on a fresh AMXConfig must land under
        # the same tempdir, not under the real home.
        cfg = AMXConfig()
        self.assertEqual(
            Path(cfg.CONFIG_DIR).parent.resolve(),
            Path.home().resolve(),
            "AMXConfig.CONFIG_DIR must resolve relative to the (patched) "
            "Path.home(), so test cfg.save() calls land in the tempdir.",
        )

    def test_dbconfig_credential_defaults_are_empty(self) -> None:
        """Pre-fix DBConfig defaulted to user='amx', password='amx_pass' —
        demo credentials that ended up in the saved YAML on first install
        as if the user had configured them. Defaults must be empty so the
        absence of credentials is visible.
        """
        from amx.config import DBConfig

        db = DBConfig()
        self.assertEqual(db.user, "")
        self.assertEqual(db.password, "")

    def test_load_from_existing_file_without_profiles_leaves_active_empty(self) -> None:
        """An existing config file with no profiles must NOT auto-synthesize a
        ``default`` profile from the empty mirror. Doing so leaks a phantom
        connection (host=localhost, user=amx, password=amx_pass, database=SAP)
        into ``cfg.db_profiles['default']`` from the dataclass fields and makes
        ``cfg.db.is_configured()`` flip-flop based on dataclass defaults rather
        than user intent. The CLI startup summary now treats empty profiles as
        ``"(not configured — run /setup or /add-db-profile)"``.
        """
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text("write_through_config: true\n")
            cfg = AMXConfig.load(str(cfg_path))
            self.assertFalse(cfg.is_first_run)
            self.assertEqual(dict(cfg.db_profiles), {})
            self.assertEqual(cfg.active_db_profile, "")
            self.assertEqual(dict(cfg.llm_profiles), {})
            self.assertEqual(cfg.active_llm_profile, "")

    def test_load_does_not_inject_phantom_default_when_other_profiles_exist(self) -> None:
        """The user reported seeing two rows in /db-profiles — `default` and
        `databricks-default` — both pointing at the same Databricks workspace. This was
        the loader synthesizing `default = cfg.db` whenever the user's saved
        profiles didn't include a name called `default`. Verify that doesn't
        happen anymore: only profiles actually present in the YAML survive
        the load round-trip.
        """
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg_path.write_text(
                "db:\n"
                "  backend: databricks\n"
                "  host: adb-1234.azuredatabricks.net\n"
                "  catalog: prod\n"
                "db_profiles:\n"
                "  databricks-default:\n"
                "    backend: databricks\n"
                "    host: adb-1234.azuredatabricks.net\n"
                "    catalog: prod\n"
                "active_db_profile: databricks-default\n"
                "llm_profiles:\n"
                "  openai-gpt4:\n"
                "    provider: openai\n"
                "    model: gpt-4o\n"
                "active_llm_profile: openai-gpt4\n"
            )
            cfg = AMXConfig.load(str(cfg_path))
            self.assertEqual(set(cfg.db_profiles.keys()), {"databricks-default"})
            self.assertEqual(cfg.active_db_profile, "databricks-default")
            self.assertEqual(set(cfg.llm_profiles.keys()), {"openai-gpt4"})
            self.assertEqual(cfg.active_llm_profile, "openai-gpt4")

    def test_save_writes_config_with_owner_only_permissions(self) -> None:
        """Config holds DB passwords / API keys; the file must be 0o600 on POSIX."""
        if os.name != "posix":
            self.skipTest("chmod 0o600 is meaningful only on POSIX filesystems")
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "config.yml"
            cfg = AMXConfig()
            cfg.save(str(cfg_path))
            mode = cfg_path.stat().st_mode & 0o777
            self.assertEqual(mode, 0o600)

    def test_db_is_configured_handles_empty_and_filled_profiles(self) -> None:
        self.assertFalse(DBConfig(host="", user="", database="").is_configured())
        self.assertTrue(
            DBConfig(host="db.example.com", user="alice", database="orders").is_configured()
        )
        self.assertFalse(
            DBConfig(backend="snowflake", account="", user="", database="").is_configured()
        )
        self.assertFalse(
            DBConfig(backend="databricks", host="", access_token="", password="").is_configured()
        )
        self.assertTrue(DBConfig(backend="bigquery", project="my-project").is_configured())

    def test_llm_is_configured_requires_provider_and_model(self) -> None:
        from amx.config import LLMConfig

        self.assertFalse(LLMConfig().is_configured())
        self.assertFalse(LLMConfig(provider="openai").is_configured())
        self.assertFalse(LLMConfig(model="gpt-4o").is_configured())
        self.assertTrue(LLMConfig(provider="openai", model="gpt-4o").is_configured())


if __name__ == "__main__":
    unittest.main()
