"""Guard: empty lineage + pages must not pollute ASK evidence.

When the catalog surfaces an anchor entity but there are no saved
lineage canvases and no published documentation pages tied to it, the
enrichment hook must short-circuit cleanly — no synthetic ``lineage``
or ``pages`` blocks, no extra entries in ``evidence_sources``. This
test pins that contract so a future change to
``enrich_retrieval_details_with_lineage_and_pages`` cannot quietly
start leaking empty (or fabricated) context into ASK answers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from amx.search._agent.retrieval import (
    enrich_retrieval_details_with_lineage_and_pages,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


def _make_fake_plan() -> Any:
    """Stand-in ``SearchPlan`` exposing the two attributes the helper reads."""

    class _Plan:
        normalized_question = "Tell me about the customers table"
        entity_hints: list[str] = ["customers"]

    return _Plan()


def test_enricher_emits_nothing_when_no_lineage_or_pages(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    # One catalog entity, deliberately no lineage_artifacts and no
    # documentation_pages — the enricher should detect "nothing to
    # attach" and leave retrieval_details unchanged.
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO catalog_entities
                (id, db_profile, db_backend, database_name, schema_name,
                 table_name, entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "p1", "postgresql", "db", "s", "customers", "table", "table"),
        )
        conn.commit()

    rows: list[dict[str, Any]] = [
        {
            "id": 1,
            "row_type": "table",
            "db_profile": "p1",
            "schema_name": "s",
            "table_name": "customers",
        }
    ]
    retrieval_details: dict[str, Any] = {"evidence_sources": ["effective_metadata"]}

    enriched = enrich_retrieval_details_with_lineage_and_pages(
        store=store,
        rows=rows,
        retrieval_details=retrieval_details,
        question="Tell me about the customers table",
        plan=_make_fake_plan(),
        lineage_profiles=None,
        pages_enabled=None,
    )

    # Neither structured block must be attached.
    assert "lineage" not in enriched
    assert "pages" not in enriched

    # The evidence_sources list must not gain synthetic entries.
    sources = enriched.get("evidence_sources") or []
    assert "lineage" not in sources
    assert "pages" not in sources

    # Sanity: the pre-existing evidence source is preserved untouched.
    assert sources == ["effective_metadata"]
