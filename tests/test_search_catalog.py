from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from amx.agents.base import Confidence, MetadataSuggestion
from amx.config import AMXConfig
from amx.db.connector import AssetKind, ColumnProfile, TableProfile
from amx.search.agent import SearchPolicy
from amx.search.catalog import SearchCatalog
from amx.search.service import SearchService, _SESSION_MEMORY
from amx.storage.sqlite_store import SQLiteHistoryStore


class _FakeIndex:
    query_hits: list[dict] = []

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
        return list(self.query_hits[:n_results])


class _FakeLLMProvider:
    responses: list[str] = []
    usages: list[dict] = []
    calls: list[list[dict[str, str]]] = []

    def __init__(self, cfg) -> None:
        self.cfg = cfg

    @classmethod
    def queue(cls, *contents: str) -> None:
        cls.responses = list(contents)
        cls.usages = []
        cls.calls = []

    def chat(self, messages, **kwargs):
        self.__class__.calls.append(messages)
        if not self.__class__.responses:
            raise AssertionError("no fake LLM response queued")
        content = self.__class__.responses.pop(0)
        usage = {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15, "model_processing_sec": 0.1}
        self.__class__.usages.append(usage)
        return type("ChatResult", (), {"content": content, "usage": usage})()


class SearchCatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "history.db"
        self.store = SQLiteHistoryStore(self.db_path)
        self.store.init()
        self.index_patcher = patch("amx.search.catalog.SearchIndex", _FakeIndex)
        self.index_patcher.start()
        self.catalog = SearchCatalog(self.db_path)
        _FakeIndex.query_hits = []
        _SESSION_MEMORY.clear()

    def tearDown(self) -> None:
        self.index_patcher.stop()
        _FakeIndex.query_hits = []
        _SESSION_MEMORY.clear()
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

    def _customer_profile(self) -> TableProfile:
        return TableProfile(
            schema="sap",
            name="kna1",
            asset_kind=AssetKind.TABLE,
            row_count=5,
            existing_comment="Customer master",
            primary_key=["kunnr"],
            columns=[ColumnProfile(name="kunnr", dtype="TEXT", nullable=False, existing_comment="Customer id")],
        )

    def _semantic_customer_profile(self) -> TableProfile:
        return TableProfile(
            schema="sap",
            name="z_customer_map",
            asset_kind=AssetKind.TABLE,
            row_count=4,
            existing_comment="Customer mapping helper",
            columns=[
                ColumnProfile(name="customer_id", dtype="TEXT", nullable=False, existing_comment="Customer id business key"),
                ColumnProfile(name="customer_name", dtype="TEXT", nullable=True, existing_comment="Customer display name"),
            ],
        )

    def _address_profile(self) -> TableProfile:
        return TableProfile(
            schema="sap_s6p",
            name="adr6",
            asset_kind=AssetKind.TABLE,
            row_count=12,
            existing_comment="Address communication details",
            columns=[
                ColumnProfile(name="addrnumber", dtype="TEXT", nullable=False, existing_comment="Address number"),
                ColumnProfile(name="date_from", dtype="DATE", nullable=True, existing_comment="Valid-from date for address communication details"),
                ColumnProfile(name="smtp_addr", dtype="TEXT", nullable=True, existing_comment="Email address detail"),
            ],
        )

    def _address_text_profile(self) -> TableProfile:
        return TableProfile(
            schema="sap_s6p",
            name="adrt",
            asset_kind=AssetKind.TABLE,
            row_count=8,
            existing_comment="Address remarks and text details",
            columns=[
                ColumnProfile(name="addrnumber", dtype="TEXT", nullable=False, existing_comment="Address number"),
                ColumnProfile(name="date_from", dtype="DATE", nullable=True, existing_comment="Valid-from date"),
                ColumnProfile(name="remark", dtype="TEXT", nullable=True, existing_comment="Address detail remark text"),
            ],
        )

    def _search_cfg(self) -> AMXConfig:
        cfg = AMXConfig()
        cfg.active_db_profile = "default"
        cfg.llm.provider = "openai"
        cfg.llm.model = "gpt-4o-mini"
        return cfg

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
            profile=self._customer_profile(),
            query_usage={},
        )
        joins = self.catalog.join_candidates("default", "sap.vbak", "sap.kna1")
        self.assertTrue(joins)
        self.assertEqual(joins[0]["left_column"], "kunnr")
        self.assertEqual(joins[0]["right_column"], "kunnr")

    def test_vector_only_column_hits_are_used_when_exact_search_misses(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        with self.catalog._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM catalog_entities
                WHERE schema_name = 'sap' AND table_name = 'vbak' AND column_name = 'netwr'
                """
            ).fetchone()
        _FakeIndex.query_hits = [{"metadata": {"entity_id": int(row["id"])}, "distance": 0.2}]

        results = self.catalog.search_columns("default", "unmatched glyph", query_variants=["semantic ghost"])

        self.assertTrue(results)
        self.assertEqual(results[0]["column_name"], "netwr")
        self.assertTrue(results[0].get("vector_only"))

    def test_vector_only_table_hits_are_used_when_exact_search_misses(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        with self.catalog._connect() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM catalog_entities
                WHERE schema_name = 'sap_s6p' AND table_name = 'adr6' AND entity_kind = 'table'
                """
            ).fetchone()
        _FakeIndex.query_hits = [{"metadata": {"entity_id": int(row["id"])}, "distance": 0.1}]

        results = self.catalog.search_tables("default", "communication endpoint")

        self.assertTrue(results)
        self.assertEqual(results[0]["table_name"], "adr6")

    def test_synthesis_prompt_payload_keeps_all_retrieved_rows(self) -> None:
        cfg = self._search_cfg()
        service = SearchService(cfg, self.catalog)
        rows = [
            {"schema_name": "sap", "table_name": "t", "column_name": f"c{i}", "rank_score": i}
            for i in range(18)
        ]
        policy = SearchPolicy(
            "semantic_discovery",
            "semantic_catalog_search",
            True,
            False,
            False,
            True,
            False,
            "ranked_matches",
            "suggest_sync_if_sparse",
        )

        payload = service._agent._rows_for_prompt(rows, policy)

        self.assertEqual(len(payload), 18)
        self.assertEqual(payload[-1]["result_index"], 18)
        self.assertEqual(payload[-1]["total_results"], 18)

    def test_catalog_inventory_and_joinable_tables(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._customer_profile(),
            query_usage={},
        )
        databases = self.catalog.known_databases("default")
        self.assertEqual(databases[0]["database_name"], "SAP")

        schemas = self.catalog.known_schemas("default", database_name="SAP")
        self.assertEqual(schemas[0]["schema_name"], "sap")
        self.assertEqual(schemas[0]["table_count"], 2)

        self.assertEqual(self.catalog.count_tables("default", schema_name="sap"), 2)

        joinable = self.catalog.joinable_tables("default", "sap.vbak")
        self.assertTrue(joinable)
        self.assertEqual(joinable[0]["target_table_name"], "kna1")
        self.assertEqual(joinable[0]["relationship_type"], "foreign_key")

    def test_out_of_domain_question_returns_no_matches(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"unsupported","out_of_domain":true,"normalized_question":"nasilsin","search_mode":"unsupported","entity_hints":[],"needs_typo_recovery":false,"reason":"small talk"}'
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("nasılsın")
        self.assertEqual(answer.intent, "unsupported")
        self.assertEqual(answer.rows, [])
        self.assertEqual(answer.details.get("reason"), "out_of_domain")

    def test_name_lookup_prefers_mandt_for_typo(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=TableProfile(
                schema="sap",
                name="bkpf",
                asset_kind=AssetKind.TABLE,
                row_count=10,
                columns=[
                    ColumnProfile(name="mandt", dtype="TEXT", nullable=False, existing_comment="SAP client"),
                    ColumnProfile(name="bukrs", dtype="TEXT", nullable=False, existing_comment="Company code"),
                ],
            ),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"mandt","search_mode":"name_lookup","entity_hints":["mangdt"],"needs_typo_recovery":true,"reason":"field lookup"}',
                "The closest field match is `sap.bkpf.mandt`, which is the SAP client column.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("mangdt")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.rows[0]["column_name"], "mandt")
        self.assertEqual(answer.confidence, "high")

    def test_search_requires_llm_profile(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        cfg = AMXConfig()
        cfg.active_db_profile = "default"
        cfg.llm.provider = ""
        cfg.llm.model = ""
        service = SearchService(cfg, self.catalog)
        answer = service.ask("price columns")
        self.assertEqual(answer.details.get("reason"), "no_llm")
        self.assertIn("requires an active LLM profile", answer.summary)

    def test_follow_up_uses_session_memory_for_table_explain(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"sales order price","search_mode":"semantic_concept","entity_hints":["vbak"],"needs_typo_recovery":false,"reason":"business meaning"}',
                "The best match is `sap.vbak.netwr`, the net value column on the sales header.",
                '{"intent":"explain_table","out_of_domain":false,"normalized_question":"what does this table do","search_mode":"table_explain","entity_hints":[],"needs_typo_recovery":false,"reason":"follow-up table explanation"}',
                "The table `sap.vbak` is the sales document header table.",
            )
            service = SearchService(cfg, self.catalog)
            first = service.ask("Which column stores sales order price?")
            second = service.ask("What does this table do?")
        self.assertTrue(first.rows)
        self.assertEqual(second.intent, "explain_table")
        self.assertTrue(second.rows)
        self.assertEqual(second.rows[0]["table_name"], "vbak")

    def test_turkish_question_uses_multilingual_search_variants(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=TableProfile(
                schema="sap_test",
                name="adr6",
                asset_kind=AssetKind.TABLE,
                row_count=10,
                columns=[
                    ColumnProfile(name="date_from", dtype="DATE", nullable=False, existing_comment="Effective start date"),
                    ColumnProfile(name="valid_to", dtype="TIMESTAMP", nullable=True, existing_comment="End validity timestamp"),
                ],
            ),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.llm.language = "turkish"
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"date related columns","search_mode":"semantic_concept","entity_hints":[],"search_queries":["Tarihlerle alakali kolonlar hangileri","date related columns","date columns"],"needs_typo_recovery":false,"reason":"multilingual semantic search"}',
                "Tarih ile ilgili en guclu kolonlar `sap_test.adr6.date_from` ve `sap_test.adr6.valid_to` gorunuyor.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("Tarihlerle alakali kolonlar hangileri")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.rows[0]["column_name"], "date_from")
        self.assertEqual(answer.confidence, "high")
        self.assertIn("date related columns", answer.details["plan"]["search_queries"])
        self.assertIn("Write the final answer in turkish.", _FakeLLMProvider.calls[-1][0]["content"])

    def test_catalog_overview_question_lists_known_databases(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"list_databases","out_of_domain":false,"normalized_question":"which databases are known","search_mode":"list_databases","entity_hints":[],"search_queries":["hangi databaseler hakkinda bilgi sahibisin","which databases are known"],"needs_typo_recovery":false,"reason":"catalog inventory"}',
                "AMX su anda `SAP` veritabani hakkinda bilgi sahibi.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("Hangi databaseler hakkinda bilgi sahibisin")
        self.assertEqual(answer.intent, "list_databases")
        self.assertFalse(answer.details.get("display_rows", True))
        self.assertEqual(answer.confidence, "high")
        self.assertEqual(answer.rows[0]["database_name"], "SAP")

    def test_turkish_inventory_question_overrides_llm_answer_language(self) -> None:
        cfg = self._search_cfg()
        fake_db = type(
            "FakeDB",
            (),
            {
                "list_schemas": lambda self: ["public", "sap_s6p", "sap_test"],
                "list_tables": lambda self, schema: ["adr6"] if schema in {"sap_s6p", "sap_test"} else [],
            },
        )()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"list_schemas","out_of_domain":false,"normalized_question":"which schemas contain adr6","search_mode":"list_schemas","question_class":"inventory","entity_hints":["adr6"],"search_queries":["adr6 tablosu hangi semalarda var","which schemas contain adr6"],"needs_typo_recovery":false,"answer_language":"english","reason":"schema inventory"}'
            )
            with patch.object(SearchService, "_inventory_db", return_value=fake_db):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("adr6 tablosu hangi şemalarda var")
        self.assertEqual(answer.details["plan"]["answer_language"], "turkish")
        self.assertIn("veritabanindaki schemalar", answer.summary)

    def test_count_tables_question_is_not_out_of_domain(self) -> None:
        cfg = self._search_cfg()
        cfg.current_schema = "sap"
        fake_db = type(
            "FakeDB",
            (),
            {
                "list_schemas": lambda self: ["sap", "hr"],
                "list_tables": lambda self, schema: ["vbak", "kna1", "mara"] if schema == "sap" else ["employees"],
            },
        )()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"count_tables","out_of_domain":false,"normalized_question":"how many tables do we have","search_mode":"count_tables","entity_hints":[],"search_queries":["kac tablomuz var","how many tables do we have"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"aggregate metadata question"}',
                "Mevcut `sap` schemasinda 3 tablo var.",
            )
            with patch.object(SearchService, "_inventory_db", return_value=fake_db):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("kac tablomuz var")
        self.assertEqual(answer.intent, "count_tables")
        self.assertEqual(answer.rows[0]["value"], 3)
        self.assertEqual(answer.confidence, "high")
        self.assertEqual(answer.details["retrieval"]["schema_name"], "sap")

    def test_table_concept_question_reroutes_from_inventory_to_semantic_table_search(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_text_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.current_schema = "sap_s6p"
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"count_tables","out_of_domain":false,"normalized_question":"tables with address details","search_mode":"count_tables","question_class":"inventory","target_entity":"aggregate","entity_hints":[],"search_queries":["icinde adres detaylari olan tum tablolari soyler misin","tables with address details"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"misclassified inventory"}',
                "`sap_s6p.adr6` ve `sap_s6p.adrt` adres detaylariyla ilgili tablolar olarak gorunuyor.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("içinde adres detayları olan tüm tabloları söyler misin?")
        self.assertEqual(answer.details["plan"]["question_class"], "semantic_discovery")
        self.assertEqual(answer.details["plan"]["target_entity"], "table")
        self.assertNotEqual(answer.intent, "count_tables")
        self.assertTrue(answer.rows)
        top_tables = {f"{row.get('schema_name')}.{row.get('table_name')}" for row in answer.rows[:2]}
        self.assertIn("sap_s6p.adr6", top_tables)
        self.assertIn("sap_s6p.adrt", top_tables)

    def test_single_table_join_question_returns_joinable_tables(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._customer_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"join_candidates","out_of_domain":false,"normalized_question":"which tables can join with sap.vbak","search_mode":"joinable_tables","entity_hints":["sap.vbak"],"search_queries":["sap.vbak tablosunu hangi tablolar ile joinleyebilirim","which tables can join with sap.vbak"],"needs_typo_recovery":false,"reason":"single-table join discovery"}',
                "`sap.vbak` tablosu `sap.kna1` ile `kunnr` kolonu uzerinden join edilebilir.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("sap.vbak tablosunu hangi tablolar ile joinleyebilirim")
        self.assertEqual(answer.intent, "join_candidates")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.rows[0]["target_table_name"], "kna1")
        self.assertEqual(answer.confidence, "high")

    def test_joinable_table_synthesis_prompt_includes_join_columns(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._customer_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"join_candidates","out_of_domain":false,"normalized_question":"which tables can join with sap.vbak","search_mode":"joinable_tables","question_class":"join_discovery","entity_hints":["sap.vbak"],"search_queries":["which tables can join with sap.vbak"],"needs_typo_recovery":false,"answer_language":"english","reason":"single-table join discovery"}',
                "You can join `sap.vbak` to `sap.kna1` using `kunnr`.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("sap.vbak tablosunu hangi tablolarla joinleyebilirim, hangi kolonlari kullanirim")
        self.assertTrue(answer.rows)
        synthesis_user = _FakeLLMProvider.calls[-1][1]["content"]
        self.assertIn('"left_column": "kunnr"', synthesis_user)
        self.assertIn('"right_column": "kunnr"', synthesis_user)

    def test_semantic_join_inference_surfaces_non_fk_candidate(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._profile(),
            query_usage={},
        )
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._semantic_customer_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"join_candidates","out_of_domain":false,"normalized_question":"which columns can join between sap.vbak and sap.z_customer_map","search_mode":"join_candidates","question_class":"join_discovery","entity_hints":["sap.vbak","sap.z_customer_map"],"search_queries":["sap.vbak and sap.z_customer_map join columns"],"needs_typo_recovery":false,"reason":"semantic join reasoning"}',
                "`sap.vbak.kunnr` and `sap.z_customer_map.customer_id` look like a likely business-key join.",
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("sap.vbak ile sap.z_customer_map hangi kolonlardan joinlenir")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.rows[0]["relationship_type"], "semantic_join_candidate")
        self.assertEqual(answer.rows[0]["confidence_band"], "possible")
        self.assertIn("semantic join inference", answer.provenance)

    def test_inventory_answer_records_live_verification_metadata(self) -> None:
        cfg = self._search_cfg()
        cfg.current_schema = "sap"
        fake_db = type(
            "FakeDB",
            (),
            {
                "list_schemas": lambda self: ["sap"],
                "list_tables": lambda self, schema: ["vbak", "kna1"],
            },
        )()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"count_tables","out_of_domain":false,"normalized_question":"how many tables do we have","search_mode":"count_tables","question_class":"inventory","entity_hints":[],"search_queries":["how many tables do we have"],"needs_typo_recovery":false,"answer_language":"english","ambiguity_flags":["missing_scope"],"reason":"inventory count"}'
            )
            with patch.object(SearchService, "_inventory_db", return_value=fake_db):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("how many tables do we have")
        self.assertEqual(answer.confidence, "high")
        self.assertTrue(answer.details["verification"]["live_verified"])
        self.assertIn("current schema", answer.summary)
