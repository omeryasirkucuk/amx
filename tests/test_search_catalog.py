from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from amx.agents.base import Confidence, MetadataSuggestion
from amx.db.connector import AssetKind, ColumnProfile, TableProfile
from amx.search.catalog import SearchCatalog
from amx.storage.sqlite_store import SQLiteHistoryStore


class _FakeIndex:
    def __init__(self, *args, **kwargs) -> None:
        self.rows: dict[str, dict] = {}

    def upsert_entities(self, entities):
        for entity in entities:
            self.rows[f"entity:{entity['id']}"] = entity
        return len(entities)

    def delete_entity_ids(self, entity_ids):
        for entity_id in entity_ids:
            self.rows.pop(f"entity:{entity_id}", None)

    def reset_profile(self, db_profile: str) -> None:
        self.rows = {
            key: value
            for key, value in self.rows.items()
            if value.get("db_profile") != db_profile
        }

    def query(self, question: str, *, db_profile: str, n_results: int = 8):
        return []


class SearchCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "history.db"
        self.store = SQLiteHistoryStore(self.db_path)
        self.store.init()
        self.index_patcher = patch("amx.search.catalog.SearchIndex", _FakeIndex)
        self.index_patcher.start()
        self.catalog = SearchCatalog(self.db_path)

    def tearDown(self) -> None:
        self.index_patcher.stop()
        self.tmp.cleanup()

    def _profile(self) -> TableProfile:
        return TableProfile(
            schema="sap",
            name="vbak",
            asset_kind=AssetKind.TABLE,
            row_count=10,
            existing_comment="Sales header",
            primary_key=["vbeln"],
            foreign_keys=[
                {
                    "referred_schema": "sap",
                    "referred_table": "kna1",
                    "constrained_columns": ["kunnr"],
                    "referred_columns": ["kunnr"],
                }
            ],
            columns=[
                ColumnProfile(name="vbeln", dtype="TEXT", nullable=False, existing_comment="Sales document"),
                ColumnProfile(name="netwr", dtype="DECIMAL", nullable=True, existing_comment="Net value"),
                ColumnProfile(name="kunnr", dtype="TEXT", nullable=True, existing_comment="Customer"),
            ],
        )

    def test_generated_reviewed_manual_precedence(self) -> None:
        run_id = self.store.create_run(
            command="analyze.run",
            mode="chat",
            db_backend="postgresql",
            db_profile="default",
            llm_provider="openai",
            llm_model="gpt-4o",
            scope={"sap": ["vbak"]},
        )
        result_ids = self.store.save_run_results(
            run_id,
            [
                {
                    "schema": "sap",
                    "table": "vbak",
                    "column": "netwr",
                    "asset_kind": "table",
                    "source": "combined",
                    "confidence": "high",
                    "reasoning": "pricing column",
                    "alternatives": ["Generated price description"],
                }
            ],
        )
        suggestion = MetadataSuggestion(
            schema="sap",
            table="vbak",
            column="netwr",
            suggestions=["Generated price description"],
            confidence=Confidence.HIGH,
            reasoning="pricing column",
            source="combined",
        )
        self.catalog.sync_generated_suggestions(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            run_id=run_id,
            profile=self._profile(),
            suggestions=[suggestion],
            result_id_map={"netwr": result_ids[0]},
            query_usage={},
        )
        with self.catalog._connect() as conn:
            row = conn.execute(
                "SELECT effective_source_kind FROM catalog_entities WHERE schema_name = 'sap' AND table_name = 'vbak' AND column_name = 'netwr'"
            ).fetchone()
        self.assertEqual(row["effective_source_kind"], "generated")

        self.catalog.sync_review_decision(
            result_ids[0],
            chosen_description="Reviewed price description",
            evaluation="accepted",
        )
        with self.catalog._connect() as conn:
            row = conn.execute(
                "SELECT effective_source_kind FROM catalog_entities WHERE schema_name = 'sap' AND table_name = 'vbak' AND column_name = 'netwr'"
            ).fetchone()
        self.assertEqual(row["effective_source_kind"], "reviewed")

        self.catalog.record_manual_description(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            schema_name="sap",
            table_name="vbak",
            column_name="netwr",
            entity_kind="column",
            asset_kind="table",
            description="Manual price description",
        )
        with self.catalog._connect() as conn:
            row = conn.execute(
                """
                SELECT ce.effective_source_kind, cd.description_text
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.schema_name = 'sap' AND ce.table_name = 'vbak' AND ce.column_name = 'netwr'
                """
            ).fetchone()
        self.assertEqual(row["effective_source_kind"], "manual")
        self.assertEqual(row["description_text"], "Manual price description")

    def test_search_columns_and_join_candidates(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={
                "table_mentions": 5,
                "sql_like_table_mentions": 3,
                "top_column_usage": [
                    {"column": "netwr", "mentions": 4, "sample_sql_lines": ["select netwr from vbak"]},
                    {"column": "kunnr", "mentions": 2, "sample_sql_lines": ["join kna1 on vbak.kunnr = kna1.kunnr"]},
                ],
            },
        )
        self.catalog.record_manual_description(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            schema_name="sap",
            table_name="vbak",
            column_name="netwr",
            entity_kind="column",
            asset_kind="table",
            description="Net price amount in document currency",
        )
        results = self.catalog.search_columns("default", "price amount")
        self.assertTrue(results)
        self.assertEqual(results[0]["column_name"], "netwr")

        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=TableProfile(
                schema="sap",
                name="kna1",
                asset_kind=AssetKind.TABLE,
                row_count=5,
                primary_key=["kunnr"],
                columns=[ColumnProfile(name="kunnr", dtype="TEXT", nullable=False, existing_comment="Customer id")],
            ),
            query_usage={},
        )
        joins = self.catalog.join_candidates("default", "sap.vbak", "sap.kna1")
        self.assertTrue(joins)
        self.assertEqual(joins[0]["left_column"], "kunnr")
        self.assertEqual(joins[0]["right_column"], "kunnr")

