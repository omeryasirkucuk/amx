from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import click

from amx.agents.base import AgentContext, Confidence, MetadataSuggestion, apply_logprob_confidence
from amx.agents.code_agent import CodeAgent
from amx.agents.orchestrator import Orchestrator, ReviewResult, apply_review_results_to_db
from amx.codebase.analyzer import CodebaseReport, analyze_codebase
from amx.codebase.code_rag import _normalize_source_filter, _source_allowed
from amx.cli_support.commands.history import format_run_scope
from amx.cli_support.commands.manual import _run_edit_wizard
from amx.cli_support.commands.profiles import cmd_use_doc, default_model
from amx.cli_support import inject_session_defaults, session_to_click_args
from amx.cli_support.session import _format_session_click_error, _handle_manual_usage_shortcuts
from amx.config import AMXConfig, DBConfig, normalize_llm_model
from amx.core import AMXApplication, UniversalMetadataAdapter
from amx.core.errors import ErrorMapper
from amx.cli_support.commands.db import cmd_profiling, cmd_tls, databricks_connect_with_recovery, interactive_db_block
from amx.db.adapters.base import BackendCapabilities, UnsupportedDatabaseOperation
from amx.db.adapters.bigquery import BigQueryAdapter
from amx.db.adapters.databricks import DatabricksAdapter
from amx.db.adapters.postgresql import PostgreSQLAdapter
from amx.db.adapters.snowflake import SnowflakeAdapter
from amx.db.connector import AssetKind, DatabaseConnector
from amx.db.connector import ColumnProfile, TableProfile
from amx.docs.rag import RAGStore
from amx.docs.scanner import _resolve_github, _resolve_s3, cleanup_scan_artifacts
from amx.llm.batch import BatchRequest, OpenAIBatchProvider
from amx.llm.provider import LLMProvider, logprob_confidence_score
from amx.services.analyze_scope import filter_non_business_assets
from amx.services.manual_metadata import collect_metadata_coverage, resolve_manual_target, resolve_path_target
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

    def test_connector_exposes_backend_capabilities(self) -> None:
        db = DatabaseConnector(DBConfig(backend="bigquery", project="p", dataset="d"))

        self.assertFalse(db.capabilities.database_comments)
        self.assertTrue(db.capabilities.column_comments)
        self.assertTrue(db.capabilities.sampled_profiling)

    def test_connector_blocks_unsupported_database_comment_before_connecting(self) -> None:
        db = DatabaseConnector(DBConfig(backend="bigquery", project="p", dataset="d"))

        with self.assertRaises(UnsupportedDatabaseOperation):
            db.set_database_comment("Project description")

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
                    (1.0, "success", "run", "chat", "databricks", "default", "openai", "gpt", "{}", "{}", "{}", "{}", ""),
                )
                run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                conn.execute(
                    """
                    INSERT INTO run_results (
                        run_id, saved_at, schema_name, table_name, column_name, asset_kind,
                        source, confidence, reasoning, alternatives_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, 1.0, "public", "orders", "id", "table", "manual", "high", "", '["Order identifier"]'),
                )
                result_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            store.record_db_apply_failure(result_id, "permission denied")
            rows = store.get_run_results(run_id)

            self.assertEqual(rows[0]["db_applied_status"], "failed")
            self.assertEqual(rows[0]["rejection_reason"], "permission denied")

    def test_databricks_ssl_error_is_actionable(self) -> None:
        adapter = DatabricksAdapter(DBConfig(backend="databricks"))

        message = adapter.actionable_profile_error(
            Exception("SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] self-signed certificate in certificate chain")
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
                raise Exception("SSLCertVerificationError: self-signed certificate in certificate chain")

            def actionable_profile_error(self, exc):
                return DatabricksAdapter(DBConfig(backend="databricks")).actionable_profile_error(exc)

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
                    return SimpleNamespace(fetchall=lambda: [FakeRow((None, "fallback_name"), {"name": "mv_orders"})])
                if sql.startswith("SHOW DATABASES"):
                    return SimpleNamespace(fetchall=lambda: [FakeRow((), {"comment": "Warehouse comment"})])
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

    def test_databricks_connect_recovery_persists_env_ca_bundle(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(backend="databricks", host="workspace", http_path="/sql", access_token="token")
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
        self.assertEqual([attempt.label for attempt in attempts], ["saved profile", "env CA bundle (AMX_DATABRICKS_TRUSTED_CA_FILE)"])
        self.assertEqual(cfg.db.tls_trusted_ca_file, str(ca_file))
        self.assertFalse(cfg.db.tls_no_verify)
        self.assertEqual(calls[-1].tls_trusted_ca_file, str(ca_file))

    def test_databricks_connect_recovery_persists_tls_no_verify_last(self) -> None:
        cfg = AMXConfig()
        cfg.db = DBConfig(backend="databricks", host="workspace", http_path="/sql", access_token="token")
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
        self.assertEqual([attempt.label for attempt in attempts], ["saved profile", "TLS no-verify fallback"])
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
            patch("amx.cli_support.commands.db.ask_choice", side_effect=lambda *args, **kwargs: next(ask_values) if args and "Select database backend" in args[0] else next(choice_values)),
            patch("amx.cli_support.commands.db.ask", side_effect=lambda *args, **kwargs: next(ask_values)),
            patch("amx.cli_support.commands.db.ask_password", side_effect=lambda *args, **kwargs: next(secret_values)),
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
            patch("amx.cli_support.commands.db.ask_choice", side_effect=lambda *args, **kwargs: next(ask_values) if args and "Select database backend" in args[0] else next(choice_values)),
            patch("amx.cli_support.commands.db.ask", side_effect=lambda *args, **kwargs: next(ask_values)),
            patch("amx.cli_support.commands.db.ask_password", side_effect=lambda *args, **kwargs: next(secret_values)),
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
        db.cfg = DBConfig(backend="bigquery", profiling_mode="full", profiling_max_rows=1_000_000, profiling_sample_size=0)
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
