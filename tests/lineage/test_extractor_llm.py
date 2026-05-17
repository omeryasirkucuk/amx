"""LLMExtractor + prompt builder + JSON parser."""

from __future__ import annotations

import json

from amx.lineage import llm_prompt as prompt_mod
from amx.lineage.extractors.llm import LLMExtractor
from amx.lineage.types import ColumnRef, Scope

from .conftest import seed_column_comments_cache_for_table, seed_table_entity


def test_prompt_includes_anchor_and_candidate_payload():
    anchor = prompt_mod.AnchorContext(
        fqn="public.orders",
        columns=[{"name": "customer_id", "dtype": "integer"}],
        description="orders table",
    )
    candidates = [
        prompt_mod.CandidateTable(
            fqn="public.customers",
            columns=[{"name": "id", "dtype": "integer"}],
            description="",
        )
    ]
    messages = prompt_mod.build_messages(anchor, candidates)
    assert messages[0]["role"] == "system"
    user_payload = messages[1]["content"]
    assert "public.orders" in user_payload
    assert "public.customers" in user_payload
    assert "customer_id" in user_payload


def test_parser_drops_hallucinated_endpoints():
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.fake_table",  # not in valid set
                    "to_table": "public.orders",
                    "column_pairs": [["x", "y"]],
                    "reasoning": "...",
                    "confidence": 0.9,
                },
                {
                    "from_table": "public.customers",
                    "to_table": "public.orders",
                    "column_pairs": [["id", "customer_id"]],
                    "reasoning": "id ↔ customer_id",
                    "confidence": 0.8,
                },
            ]
        }
    )
    edges = prompt_mod.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert len(edges) == 1
    assert edges[0].from_fqn == "public.customers"


def test_parser_drops_below_min_confidence():
    raw = json.dumps(
        {
            "edges": [
                {
                    "from_table": "public.customers",
                    "to_table": "public.orders",
                    "column_pairs": [],
                    "reasoning": "low",
                    "confidence": 0.2,
                }
            ]
        }
    )
    edges = prompt_mod.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
        min_confidence=0.4,
    )
    assert edges == []


def test_parser_strips_markdown_fences():
    raw = (
        "```json\n"
        + json.dumps(
            {
                "edges": [
                    {
                        "from_table": "public.customers",
                        "to_table": "public.orders",
                        "column_pairs": [],
                        "reasoning": "ok",
                        "confidence": 0.7,
                    }
                ]
            }
        )
        + "\n```"
    )
    edges = prompt_mod.parse_response(
        raw,
        anchor_fqn="public.orders",
        valid_candidate_fqns={"public.customers"},
    )
    assert len(edges) == 1


def test_llm_extractor_cache_only_returns_persisted_llm_edges(hs):
    """``cache_only`` mode reads previously-persisted lineage_llm rows."""
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    customers_id = seed_table_entity(hs, schema="public", table="customers")
    import time as _time

    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, details_json, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                customers_id,
                orders_id,
                "lineage_llm",
                0.75,
                "llm",
                json.dumps(
                    {"reasoning": "customers.id is likely the source of orders.customer_id"}
                ),
                _time.time(),
            ),
        )
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    result = LLMExtractor().extract(hs=hs, scope=scope, mode="cache_only")
    assert result.cache_status == "hit"
    assert len(result.edges) == 1
    assert result.edges[0].relationship_type == "lineage_llm"
    assert "customers.id" in result.edges[0].evidence


def test_llm_extractor_llm_suggest_calls_fake_and_persists(hs):
    """``llm_suggest`` mode invokes the callable + persists the result."""
    seed_table_entity(hs, schema="public", table="orders")
    seed_table_entity(hs, schema="public", table="customers")
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="orders",
        columns={"customer_id": {"type": "integer"}, "amount": {"type": "decimal"}},
    )
    seed_column_comments_cache_for_table(
        hs,
        schema="public",
        table="customers",
        columns={"id": {"type": "integer"}, "name": {"type": "text"}},
    )

    def fake_llm(messages):
        return json.dumps(
            {
                "edges": [
                    {
                        "from_table": "public.customers",
                        "to_table": "public.orders",
                        "column_pairs": [["id", "customer_id"]],
                        "reasoning": "FK pattern: customers.id feeds orders.customer_id",
                        "confidence": 0.85,
                    }
                ]
            }
        )

    extractor = LLMExtractor(llm_callable=fake_llm, model_name="test/fake-model")
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    result = extractor.extract(hs=hs, scope=scope, mode="llm_suggest")

    assert result.cache_status == "hit"
    assert len(result.edges) == 1
    edge = result.edges[0]
    assert edge.relationship_type == "lineage_llm"
    assert edge.confidence == 0.85
    assert "customers.id" in edge.evidence or "feeds" in edge.evidence

    # Persistence: a second cache_only read returns the same edge
    cached = LLMExtractor().extract(hs=hs, scope=scope, mode="cache_only")
    assert len(cached.edges) == 1
    assert cached.edges[0].confidence == 0.85
