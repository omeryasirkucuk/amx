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

    def _adrc_profile(self) -> TableProfile:
        return TableProfile(
            schema="sap_s6p",
            name="adrc",
            asset_kind=AssetKind.TABLE,
            row_count=20,
            existing_comment="Address master",
            columns=[
                ColumnProfile(name="addrnumber", dtype="TEXT", nullable=False, existing_comment="Address number"),
                ColumnProfile(name="name1", dtype="TEXT", nullable=True, existing_comment="Name line"),
                ColumnProfile(name="city1", dtype="TEXT", nullable=True, existing_comment="City"),
                ColumnProfile(name="city_code", dtype="TEXT", nullable=True, existing_comment="City code"),
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
                '{"intent":"explain_table","out_of_domain":false,"normalized_question":"what does this table do","search_mode":"table_explain","entity_hints":[],"needs_typo_recovery":false,"reason":"follow-up table explanation"}',
            )
            service = SearchService(cfg, self.catalog)
            first = service.ask("Which column stores sales order price?")
            second = service.ask("What does this table do?")
        self.assertTrue(first.rows)
        self.assertEqual(second.intent, "explain_table")
        self.assertTrue(second.rows)
        self.assertEqual(second.rows[0]["table_name"], "vbak")
        self.assertEqual(second.details["answer_strategy"], "deterministic")

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
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("Tarihlerle alakali kolonlar hangileri")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.rows[0]["column_name"], "date_from")
        self.assertEqual(answer.confidence, "high")
        self.assertIn("date related columns", answer.details["plan"]["search_queries"])
        self.assertEqual(answer.details["answer_strategy"], "deterministic")

    def test_table_scoped_comment_question_runs_agent_planned_live_probe(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._adrc_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        fake_db = type(
            "FakeDB",
            (),
            {
                "get_column_comments": lambda self, schema, table: {
                    "addrnumber": "Address number",
                    "name1": "Name line",
                    "city1": None,
                },
                "column_comments_probe_query": lambda self, schema, table: (
                    "SELECT column_name, comment FROM metadata WHERE table_name = :table"
                ),
            },
        )()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"are all adrc columns commented","search_mode":"semantic_concept","question_class":"semantic_discovery","target_entity":"column","entity_hints":["adrc"],"search_queries":["adrc tablosunda commentler tum kolonlar icin girili mi","are all ADRC columns commented"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"metadata completeness question"}',
                '{"needs_live_probe":false,"reason":"The retrieved rows may be enough.","operations":[]}',
            )
            with patch.object(SearchService, "_inventory_db", return_value=fake_db):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("adrc tablosunda commentler tüm kolonlar için girili vaziyette mi?")

        self.assertEqual(answer.confidence, "high")
        self.assertIn("Hayir", answer.summary)
        self.assertIn("`city1`", answer.summary)
        self.assertIn("SELECT column_name, comment", answer.summary)
        self.assertEqual(answer.details["retrieval"]["live_probe"]["operations"][0]["operation"], "column_comments")
        self.assertIn("Default live probe", answer.details["retrieval"]["live_probe"]["operations"][0]["rationale"])
        self.assertIn("agent-planned live metadata probe", answer.provenance)
        self.assertTrue(answer.details["executed_actions"])
        self.assertEqual(answer.details["executed_actions"][0]["operation"], "column_comments")

    def test_explicit_table_mention_wins_over_fuzzy_catalog_candidate_for_live_probe(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.current_schema = "sap_s6p"

        class FakeDB:
            def get_column_comments(self, schema: str, table: str) -> dict[str, str | None]:
                if table != "adrc":
                    raise AssertionError(f"unexpected live probe table: {table}")
                return {"addrnumber": "Address number", "name1": "Name line"}

            def column_comments_probe_query(self, schema: str, table: str) -> str:
                return f"comments probe for {schema}.{table}"

        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"are all adrc columns commented","search_mode":"semantic_concept","question_class":"semantic_discovery","target_entity":"column","entity_hints":["adr6"],"search_queries":["are all adrc columns commented"],"needs_typo_recovery":false,"answer_language":"english","reason":"metadata completeness question"}',
                '{"needs_live_probe":false,"reason":"semantic rows are enough","operations":[]}',
            )
            with patch.object(SearchService, "_inventory_db", return_value=FakeDB()):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("adrc tablosunda commentler tüm kolonlar için girili vaziyette mi?")

        self.assertIn("`sap_s6p.adrc`", answer.summary)
        self.assertEqual(answer.details["retrieval"]["live_probe"]["operations"][0]["table_path"], "sap_s6p.adrc")

    def test_table_explain_uses_live_exact_table_before_fuzzy_catalog_candidate(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.current_schema = "sap_s6p"

        class FakeDB:
            def list_tables(self, schema: str) -> list[str]:
                return ["adrc"] if schema == "sap_s6p" else []

            def table_metadata_probe_query(self, schema: str, table: str) -> str:
                return f"metadata probe for {schema}.{table}"

            def get_table_metadata_snapshot(self, schema: str, table: str) -> dict:
                if table != "adrc":
                    raise AssertionError(f"unexpected live probe table: {table}")
                return {
                    "schema": schema,
                    "table": table,
                    "table_comment": "Address master",
                    "columns": [
                        {"name": "addrnumber", "dtype": "TEXT", "nullable": False, "comment": "Address number"},
                        {"name": "name1", "dtype": "TEXT", "nullable": True, "comment": "Name line"},
                        {"name": "city1", "dtype": "TEXT", "nullable": True, "comment": "City"},
                        {"name": "post_code1", "dtype": "TEXT", "nullable": True, "comment": "Postal code"},
                    ],
                }

        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"explain_table","out_of_domain":false,"normalized_question":"what is adrc table","search_mode":"table_explain","question_class":"table_understanding","target_entity":"table","entity_hints":["adr6"],"search_queries":["adrc tablosu nedir","what is ADRC table"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"table explanation"}',
                '{"needs_live_probe":false,"reason":"catalog rows are enough","operations":[]}',
            )
            with patch.object(SearchService, "_inventory_db", return_value=FakeDB()):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("adrc tablosu nedir")

        self.assertIn("`sap_s6p.adrc`", answer.summary)
        self.assertIn("**4** kolon", answer.summary)
        self.assertNotIn("adr6", answer.summary.lower())
        self.assertEqual(answer.confidence, "high")
        self.assertEqual(answer.details["retrieval"]["resolved_tables"], ["sap_s6p.adrc"])
        self.assertEqual(answer.details["retrieval"]["live_probe"]["operations"][0]["table_path"], "sap_s6p.adrc")
        self.assertIn("live verification", answer.provenance)

    def test_explicit_missing_table_is_not_replaced_by_fuzzy_candidate(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.current_schema = "sap_s6p"

        class FakeDB:
            def list_tables(self, schema: str) -> list[str]:
                return ["adr6"] if schema == "sap_s6p" else []

        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"explain_table","out_of_domain":false,"normalized_question":"what is adrc table","search_mode":"table_explain","question_class":"table_understanding","target_entity":"table","entity_hints":["adr6"],"search_queries":["adrc tablosu nedir","what is ADRC table"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"table explanation"}'
            )
            with patch.object(SearchService, "_inventory_db", return_value=FakeDB()):
                service = SearchService(cfg, self.catalog)
                answer = service.ask("adrc tablosu nedir")

        self.assertIn("exact olarak dogrulayamadim", answer.summary)
        self.assertIn("`sap_s6p.adr6`", answer.summary)
        self.assertEqual(answer.rows, [])
        self.assertEqual(answer.confidence, "low")
        self.assertNotIn("live verification", answer.provenance)
        self.assertEqual(answer.details["retrieval"]["resolved_tables"], [])
        self.assertIn("explicit_table_not_found_live", answer.details["ambiguity_flags"])

    def test_table_resolution_does_not_mark_live_verified_without_live_rows(self) -> None:
        cfg = self._search_cfg()
        service = SearchService(cfg, self.catalog)
        plan = type(
            "Plan",
            (),
            {"question_class": "table_understanding", "search_mode": "table_explain"},
        )()
        policy = SearchPolicy(
            "table_understanding",
            "table_context_plus_neighbors",
            True,
            False,
            True,
            True,
            True,
            "table_summary",
            "suggest_sync_if_sparse",
        )

        rows, verification = service._agent._verify_rows(plan, policy, [{"row_type": "table"}], {"resolved_tables": ["sap_s6p.adrc"]})

        self.assertEqual(rows[0]["row_type"], "table")
        self.assertFalse(verification["live_verified"])
        self.assertEqual(verification["checks"], ["table_resolution"])

    def test_global_column_concept_query_does_not_turn_into_live_table_snapshot(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._adrc_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        cfg.current_schema = "sap_s6p"

        class FakeDB:
            def get_table_metadata_snapshot(self, schema: str, table: str) -> dict:
                raise AssertionError("global column discovery must not run table snapshot probes")

            def table_metadata_probe_query(self, schema: str, table: str) -> str:
                raise AssertionError("global column discovery must not build table snapshot probes")

        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"explain_table","out_of_domain":false,"normalized_question":"city related column names","search_mode":"table_explain","question_class":"table_understanding","target_entity":"table","entity_hints":["adrc"],"search_queries":["city ile alakali tum kolon isimlerini getir","city related column names"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"misclassified from memory"}',
                "City ile alakali kolonlar: `sap_s6p.adrc.city1` ve `sap_s6p.adrc.city2`.",
            )
            with patch.object(SearchService, "_inventory_db", return_value=FakeDB()):
                service = SearchService(cfg, self.catalog)
                service._agent._remember(
                    {
                        "question": "adrc tablosu nedir",
                        "intent": "explain_table",
                        "topic": "what is adrc table",
                        "tables": ["sap_s6p.adrc"],
                        "columns": [],
                    }
                )
                second = service.ask("city ile alakalı tüm kolon isimlerini getir")

        self.assertEqual(second.details["plan"]["search_mode"], "semantic_concept")
        self.assertEqual(second.details["plan"]["target_entity"], "column")
        self.assertFalse(second.details["retrieval"].get("live_probe", {}).get("executed", False))
        self.assertNotIn("Canli DB metadata'sina gore", second.summary)
        self.assertEqual(second.details["retrieval"]["result_kind"], "exact_column_name_matches")
        self.assertTrue(second.rows)
        column_names = {str(row.get("column_name") or "").lower() for row in second.rows}
        self.assertIn("city1", column_names)
        self.assertIn("city_code", column_names)
        self.assertNotIn("name2", column_names)
        self.assertIn("`sap_s6p.adrc.city1`", second.summary)

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

    def test_inventory_question_keeps_llm_answer_language_when_present(self) -> None:
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
        self.assertEqual(answer.details["plan"]["answer_language"], "english")
        self.assertIn("schemas", answer.summary.lower())

    def test_low_confidence_plan_returns_clarification_question(self) -> None:
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_columns","out_of_domain":false,"normalized_question":"customer identifier columns","search_mode":"semantic_concept","question_class":"semantic_discovery","target_entity":"column","entity_hints":[],"search_queries":["customer identifiers"],"needs_typo_recovery":false,"answer_language":"english","ambiguity_flags":["missing_scope"],"reason":"ambiguous scope","decision_confidence":"low","needs_clarification":true,"clarification_question":"Do you want this for a specific schema or across all schemas?"}'
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("customer identifiers")
        self.assertEqual(answer.intent, "clarification")
        self.assertIn("specific schema", answer.summary.lower())
        self.assertEqual(answer.details["reason"], "clarification_required")

    def test_balanced_reviewer_can_correct_classifier_route(self) -> None:
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"request_type":"metadata_discovery","intent":"find_tables","out_of_domain":false,"normalized_question":"tables with missing comments","search_mode":"semantic_concept","question_class":"semantic_discovery","target_entity":"table","entity_hints":[],"search_queries":["veri tabanlarımızda comment kısmı eksik olanlar var mı","tables with missing comments"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"semantic guess","decision_confidence":"medium","needs_clarification":false}',
                '{"request_type":"coverage_audit","intent":"check_coverage","out_of_domain":false,"normalized_question":"tables with missing comments","search_mode":"check_coverage","question_class":"inventory","target_entity":"database","entity_hints":[],"search_queries":["veri tabanlarımızda comment kısmı eksik olanlar var mı","tables with missing comments"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"broad missing-comment coverage request","decision_confidence":"high","needs_clarification":false}'
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("veri tabanlarımızda comment kısmı eksik olanlar var mı")
        self.assertEqual(answer.intent, "check_coverage")
        self.assertEqual(answer.details["reason"], "redirect_to_analyze")

    def test_classifier_can_route_coverage_audit_without_reviewer(self) -> None:
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"request_type":"coverage_audit","intent":"check_coverage","out_of_domain":false,"normalized_question":"tables with missing comments","search_mode":"check_coverage","question_class":"inventory","target_entity":"database","entity_hints":[],"search_queries":["veri tabanlarımızda comment kısmı eksik olanlar var mı","tables with missing comments"],"needs_typo_recovery":false,"answer_language":"turkish","reason":"broad missing-comment coverage request","decision_confidence":"high","needs_clarification":false}'
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("veri tabanlarımızda comment kısmı eksik olanlar var mı")
        self.assertEqual(answer.intent, "check_coverage")
        self.assertEqual(answer.details["reason"], "redirect_to_analyze")

    def test_japanese_question_uses_japanese_answer_language(self) -> None:
        self.catalog.sync_table_profile(
            db_profile="default",
            db_backend="postgresql",
            database_name="SAP",
            profile=self._address_profile(),
            query_usage={},
        )
        cfg = self._search_cfg()
        with patch("amx.search.service.LLMProvider", _FakeLLMProvider):
            _FakeLLMProvider.queue(
                '{"intent":"find_tables","out_of_domain":false,"normalized_question":"tables containing address details","search_mode":"semantic_concept","question_class":"semantic_discovery","target_entity":"table","entity_hints":[],"search_queries":["住所の詳細を含むテーブル","tables containing address details"],"needs_typo_recovery":false,"answer_language":"japanese","reason":"table discovery","decision_confidence":"high","needs_clarification":false}',
                "住所情報に関連する候補として `sap.adr6` が見つかりました。"
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("住所の詳細を含むテーブルはどれですか？")
        self.assertEqual(answer.details["plan"]["answer_language"], "japanese")
        self.assertIn("`sap.adr6`", answer.summary)

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

    def test_joinable_table_answer_is_deterministic_and_includes_join_columns(self) -> None:
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
            )
            service = SearchService(cfg, self.catalog)
            answer = service.ask("sap.vbak tablosunu hangi tablolarla joinleyebilirim, hangi kolonlari kullanirim")
        self.assertTrue(answer.rows)
        self.assertEqual(answer.details["answer_strategy"], "deterministic")
        self.assertIn("`sap.kna1`", answer.summary)
        self.assertIn("`kunnr`", answer.summary)

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
