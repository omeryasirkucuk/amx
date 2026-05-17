"""LLM feedback loop — approved/rejected edges fold into next prompt."""

from __future__ import annotations

import json
import time

from amx.config import AMXConfig, DBConfig, LLMConfig
from amx.lineage import llm_prompt as prompt_mod
from amx.lineage.extractors.llm import LLMExtractor
from amx.lineage.types import ColumnRef, Scope

from .conftest import seed_column_comments_cache_for_table, seed_table_entity


def _seed_three_tables(hs):
    for name in ("orders", "customers", "items"):
        seed_table_entity(hs, schema="public", table=name)
        seed_column_comments_cache_for_table(
            hs,
            schema="public",
            table=name,
            columns={"id": {"type": "integer"}},
        )


def test_prompt_includes_approved_examples():
    anchor = prompt_mod.AnchorContext(
        fqn="public.orders",
        columns=[{"name": "customer_id", "dtype": "integer"}],
    )
    candidates = [
        prompt_mod.CandidateTable(
            fqn="public.customers",
            columns=[{"name": "id", "dtype": "integer"}],
        )
    ]
    approved = [
        prompt_mod.FeedbackExample(
            from_fqn="public.customers",
            to_fqn="public.orders",
            note="FK pattern",
        )
    ]
    messages = prompt_mod.build_messages(
        anchor,
        candidates,
        approved_examples=approved,
        rejected_examples=[],
    )
    system = messages[0]["content"]
    assert "approved" in system.lower()
    assert "public.customers" in system
    assert "FK pattern" in system


def test_prompt_includes_rejected_examples():
    anchor = prompt_mod.AnchorContext(fqn="public.orders", columns=[])
    candidates = [prompt_mod.CandidateTable(fqn="public.customers", columns=[])]
    rejected = [
        prompt_mod.FeedbackExample(
            from_fqn="public.unrelated",
            to_fqn="public.orders",
            note="spurious join",
        )
    ]
    messages = prompt_mod.build_messages(
        anchor, candidates, approved_examples=[], rejected_examples=rejected
    )
    system = messages[0]["content"]
    assert "rejected" in system.lower()
    assert "public.unrelated" in system


def test_llm_extractor_pulls_recent_verdicted_edges(hs):
    _seed_three_tables(hs)
    now = time.time()
    # Seed one approved + one rejected lineage_llm row.
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, "
            "details_json, last_seen, verdict, audit_actor, audit_at) "
            "VALUES (1, 2, 'lineage_llm', 0.7, 'llm', ?, ?, 'approved', 'alice', ?)",
            (json.dumps({"reasoning": "FK"}), now, now),
        )
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, "
            "details_json, last_seen, verdict, audit_actor, audit_at) "
            "VALUES (3, 1, 'lineage_llm', 0.4, 'llm', ?, ?, 'rejected', 'alice', ?)",
            (json.dumps({"reasoning": "noise"}), now, now),
        )

    captured: dict[str, str] = {}

    def fake_llm(messages):
        captured["system"] = messages[0]["content"]
        return json.dumps({"edges": []})

    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.db_profiles = {"p": DBConfig(backend="postgresql", database="")}

    extractor = LLMExtractor(llm_callable=fake_llm, model_name="test/fake")
    scope = Scope(profile="p", anchor=ColumnRef("", "public", "orders", ""))
    extractor.extract(hs=hs, scope=scope, mode="llm_suggest")

    assert "approved" in captured["system"].lower()
    assert "rejected" in captured["system"].lower()
