"""End-to-end: when supporting data exists, retrieval emits lineage and
pages evidence alongside the existing catalog sources.

This test exercises the wiring added to
``amx/search/_agent/retrieval.py`` — the entry point
``enrich_retrieval_details_with_lineage_and_pages`` folds anchor-based
lineage neighbours and anchor-scoped published pages into the
``retrieval_details`` dict that ``SearchAgent.ask()`` forwards to the
LLM context. We seed a minimal three-node graph plus one published
documentation page and assert that ``"lineage"`` and ``"pages"`` keys
appear in the enriched details.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from amx.search._agent.retrieval import (
    _asset_refs_for_entities,
    _question_terms_for_pages,
    enrich_retrieval_details_with_lineage_and_pages,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


def _insert_entity(
    store: SQLiteHistoryStore,
    *,
    entity_id: int,
    table: str,
    profile: str = "p1",
    schema: str = "s",
) -> None:
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities
                (id, db_profile, db_backend, database_name, schema_name,
                 table_name, entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (entity_id, profile, "postgresql", "db", schema, table, "table", "table"),
        )


def _insert_relationship(
    store: SQLiteHistoryStore, *, from_id: int, to_id: int
) -> None:
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 details_json, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (from_id, to_id, "foreign_key", 10.0, "database", "{}", time.time()),
        )


def _seed_minimal(store: SQLiteHistoryStore) -> int:
    """Seed a 20 -> 10 -> 30 graph, one canvas covering all three nodes,
    and one published documentation page anchored to entity 10's table.

    Returns the anchor entity id (10).
    """
    _insert_entity(store, entity_id=10, table="customers")
    _insert_entity(store, entity_id=20, table="raw_customers")
    _insert_entity(store, entity_id=30, table="customers_daily")

    _insert_relationship(store, from_id=20, to_id=10)
    _insert_relationship(store, from_id=10, to_id=30)

    with store._connect() as conn:  # noqa: SLF001
        cur = conn.execute(
            """
            INSERT INTO lineage_artifacts
                (name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "customers-canvas",
                "p1",
                10,
                1,
                1,
                "svg",
                "",
                "hash-cust",
                3,
                2,
                time.time(),
                json.dumps([]),
                0,
            ),
        )
        artifact_id = int(cur.lastrowid)
        for eid in (10, 20, 30):
            conn.execute(
                """
                INSERT INTO lineage_artifact_nodes
                    (artifact_id, entity_id, db_profile)
                VALUES (?, ?, ?)
                """,
                (artifact_id, eid, "p1"),
            )

    now = datetime.utcnow()
    store.create_documentation_page(
        page_id="p1",
        title="Customers notes",
        slug="customers",
        markdown_body=(
            "Daily refresh of the customers table from S3 into the warehouse. "
            "Powers the PBI dashboard for revenue ops."
        ),
        rendered_html=None,
        status="published",
        created_at=now,
        updated_at=now,
        created_by=None,
        generation_prompt=None,
        model_used=None,
        db_profile=None,
    )
    store.attach_documentation_page_asset(
        "p1", asset_kind="db_table", asset_ref="p1:s:customers"
    )
    return 10


def _make_fake_plan() -> Any:
    """Build the minimal ``SearchPlan`` the enrichment function reads.

    The helper only touches ``normalized_question`` and ``entity_hints``,
    so a dataclass-equivalent stub keeps the test free of planner setup.
    """

    class _Plan:
        normalized_question = "Tell me about the customers table"
        entity_hints: list[str] = ["customers"]

    return _Plan()


def test_asset_refs_for_entities_returns_table_and_column_refs(
    tmp_path: Path,
) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _insert_entity(store, entity_id=10, table="customers")
    # Column entity carries a non-empty column_name; helper should emit
    # both the table-level and column-level asset_ref.
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities
                (id, db_profile, db_backend, database_name, schema_name,
                 table_name, column_name, entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (11, "p1", "postgresql", "db", "s", "customers", "email", "column", "column"),
        )
    refs = _asset_refs_for_entities(store, [10, 11])
    assert "p1:s:customers" in refs
    assert "p1:s:customers.email" in refs


def test_question_terms_strips_stopwords_and_short_tokens() -> None:
    terms = _question_terms_for_pages("What columns are in the customers table?", None)
    assert "customers" in terms
    # stopwords filtered
    assert "what" not in terms
    assert "the" not in terms
    # short tokens filtered
    assert "in" not in terms


def test_retrieval_emits_lineage_and_pages_sources_when_data_exists(
    tmp_path: Path,
) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    anchor = _seed_minimal(store)

    # Shape the retrieval rows the way the catalog search functions
    # return them — ``ce.*`` always includes the ``id`` column.
    rows: list[dict[str, Any]] = [
        {
            "id": anchor,
            "row_type": "table",
            "db_profile": "p1",
            "schema_name": "s",
            "table_name": "customers",
        }
    ]
    retrieval_details: dict[str, Any] = {
        "evidence_sources": ["effective_metadata"],
    }
    plan = _make_fake_plan()

    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=rows,
        retrieval_details=retrieval_details,
        question="Tell me about the customers table",
        plan=plan,
        lineage_profiles=None,
        pages_enabled=None,
    )

    sources = enriched.get("evidence_sources") or []
    assert "lineage" in sources
    assert "pages" in sources

    lineage = enriched.get("lineage") or {}
    assert lineage.get("kind") == "lineage"
    assert "customers-canvas" in (lineage.get("artifact_names") or [])
    assert 20 in (lineage.get("upstream_entity_ids") or [])
    assert 30 in (lineage.get("downstream_entity_ids") or [])

    pages = enriched.get("pages") or {}
    assert pages.get("kind") == "pages"
    items = pages.get("items") or []
    assert len(items) == 1
    assert items[0]["title"] == "Customers notes"
    assert items[0]["slug"] == "customers"
    assert "customers" in items[0]["excerpt"].lower()


def test_retrieval_skips_lineage_when_no_artifacts(tmp_path: Path) -> None:
    """Without lineage_artifacts rows the enriched details get no
    ``lineage`` key — only pages remains (if pages data exists)."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _insert_entity(store, entity_id=10, table="customers")
    now = datetime.utcnow()
    store.create_documentation_page(
        page_id="p1",
        title="Customers notes",
        slug="customers",
        markdown_body="Customers table notes.",
        rendered_html=None,
        status="published",
        created_at=now,
        updated_at=now,
        created_by=None,
        generation_prompt=None,
        model_used=None,
        db_profile=None,
    )
    store.attach_documentation_page_asset(
        "p1", asset_kind="db_table", asset_ref="p1:s:customers"
    )

    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=[{"id": 10, "row_type": "table"}],
        retrieval_details={"evidence_sources": ["effective_metadata"]},
        question="customers table",
        plan=_make_fake_plan(),
        lineage_profiles=None,
        pages_enabled=None,
    )

    assert "lineage" not in (enriched.get("evidence_sources") or [])
    assert "lineage" not in enriched
    assert "pages" in (enriched.get("evidence_sources") or [])


def test_retrieval_respects_pages_enabled_false(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_minimal(store)

    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=[{"id": 10, "row_type": "table"}],
        retrieval_details={"evidence_sources": ["effective_metadata"]},
        question="customers",
        plan=_make_fake_plan(),
        lineage_profiles=None,
        pages_enabled=False,
    )

    assert "pages" not in (enriched.get("evidence_sources") or [])
    assert "pages" not in enriched
    # Lineage still fires when its profiles are not gated.
    assert "lineage" in (enriched.get("evidence_sources") or [])


def test_retrieval_respects_lineage_profiles_empty_off(tmp_path: Path) -> None:
    """Empty list short-circuits lineage retrieval to off."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    _seed_minimal(store)

    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=[{"id": 10, "row_type": "table"}],
        retrieval_details={"evidence_sources": ["effective_metadata"]},
        question="customers",
        plan=_make_fake_plan(),
        lineage_profiles=[],
        pages_enabled=None,
    )

    assert "lineage" not in (enriched.get("evidence_sources") or [])
    assert "lineage" not in enriched
