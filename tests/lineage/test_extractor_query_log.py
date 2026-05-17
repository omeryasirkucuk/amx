"""QueryLogExtractor — mine table co-occurrence from history.db."""

from __future__ import annotations

import json
import time

from amx.lineage.extractors.query_log import QueryLogExtractor
from amx.lineage.types import ColumnRef, Scope


def _seed_run(hs, *, profile: str, scope_json: dict, started_at: float | None = None) -> None:
    ts = started_at if started_at is not None else time.time()
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO analysis_runs
                (started_at, status, command, db_profile, scope_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ts, "completed", "/run", profile, json.dumps(scope_json)),
        )


def test_query_log_extractor_emits_co_occurrence_when_threshold_met(hs):
    """Tables that appear with the anchor in ≥ 2 runs get an edge."""
    profile = "sap-prod"
    # adr6 + adrc → 3 times (meets threshold)
    for _ in range(3):
        _seed_run(
            hs,
            profile=profile,
            scope_json={"schemas": {"sap_test": ["adr6", "adrc"]}},
        )
    # adr6 + kna1 → 1 time (below threshold)
    _seed_run(
        hs,
        profile=profile,
        scope_json={"schemas": {"sap_test": ["adr6", "kna1"]}},
    )
    # Unrelated run that doesn't mention the anchor
    _seed_run(
        hs,
        profile=profile,
        scope_json={"schemas": {"other_schema": ["something_else"]}},
    )

    scope = Scope(profile=profile, anchor=ColumnRef("", "sap_test", "adr6", ""))
    result = QueryLogExtractor().extract(hs=hs, scope=scope)

    assert result.cache_status == "hit"
    targets = {e.target.table for e in result.edges}
    assert "adrc" in targets
    assert "kna1" not in targets  # below MIN_CO_OCCURRENCE
    assert all(e.relationship_type == "lineage_co_occurs" for e in result.edges)
    assert all(e.extractor == "query_log" for e in result.edges)


def test_query_log_extractor_ignores_runs_without_anchor(hs):
    """Runs that touch unrelated tables must not contribute to anchor edges."""
    profile = "p"
    # 5 runs that don't mention adr6 at all
    for _ in range(5):
        _seed_run(
            hs,
            profile=profile,
            scope_json={"schemas": {"sap_test": ["adrc", "kna1"]}},
        )
    scope = Scope(profile=profile, anchor=ColumnRef("", "sap_test", "adr6", ""))
    result = QueryLogExtractor().extract(hs=hs, scope=scope)
    assert result.edges == []


def test_query_log_extractor_handles_tables_list_shape(hs):
    """``scope_json`` with ``tables: [{schema, name}, ...]`` also works."""
    profile = "p"
    payload = {
        "tables": [
            {"schema": "sap_test", "name": "adr6"},
            {"schema": "sap_test", "name": "adrc"},
        ]
    }
    for _ in range(2):
        _seed_run(hs, profile=profile, scope_json=payload)
    scope = Scope(profile=profile, anchor=ColumnRef("", "sap_test", "adr6", ""))
    result = QueryLogExtractor().extract(hs=hs, scope=scope)
    assert any(e.target.table == "adrc" for e in result.edges)


def test_query_log_extractor_confidence_climbs_with_co_occurrence(hs):
    """Repeated co-occurrence raises confidence (but cap holds)."""
    profile = "p"
    for _ in range(10):
        _seed_run(
            hs,
            profile=profile,
            scope_json={"schemas": {"sap_test": ["adr6", "adrc"]}},
        )
    scope = Scope(profile=profile, anchor=ColumnRef("", "sap_test", "adr6", ""))
    edges = QueryLogExtractor().extract(hs=hs, scope=scope).edges
    assert edges, "expected at least one edge"
    edge = next(e for e in edges if e.target.table == "adrc")
    # 10 occurrences = floor + 8 * step = 0.3 + 0.4 = 0.7 (ceiling)
    assert edge.confidence == 0.7
