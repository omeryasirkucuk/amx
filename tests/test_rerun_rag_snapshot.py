"""PR D — re-Run snapshots the original run's RAG hits.

The original run records the exact ``prompt_hits`` it fed the LLM
into ``run_context_cache.payload["rag_hits"]``. When the re-Run
worker rehydrates the snapshot, ``AgentContext.rag_hits`` carries
those chunks and :class:`RAGAgent` skips the live ``RAGStore.query``
calls. Pre-PR-D snapshots without ``rag_hits`` silently fall back to
the live-query path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.agents.base import AgentContext
from amx.agents.rag_agent import RAGAgent
from amx.agents.rerun_context import _serialize_rag_hit, hydrate_context


def _llm_namespace():
    return MagicMock(
        cfg=SimpleNamespace(
            temperature=0.0,
            max_tokens=512,
            column_batch_size=10,
            n_alternatives=3,
            description_verbosity="brief",
            logprob_high=0.9,
            logprob_medium=0.7,
            rag_query_timeout_sec=5.0,
            prompt_detail_cfg=SimpleNamespace(rag_table_hits=2, rag_col_hits=0, rag_max_chunks=4),
        ),
    )


def test_serialize_rag_hit_keeps_text_and_metadata():
    hit = {
        "text": "alpha bravo",
        "metadata": {
            "source": "/docs/a.md",
            "source_root": "/docs",
            "source_type": "local",
            "chunk_idx": 2,
        },
        "distance": 0.34,
        "score": 1.5,
    }
    out = _serialize_rag_hit(hit)
    assert out["text"] == "alpha bravo"
    assert out["metadata"]["source"] == "/docs/a.md"
    assert out["metadata"]["chunk_idx"] == 2
    assert out["score"] == 1.5


def test_hydrate_context_populates_rag_hits():
    payload = {
        "schema": "public",
        "table": "orders",
        "rag_hits": [
            {"text": "x", "metadata": {"source": "/a", "chunk_idx": 0}, "score": 0.9},
        ],
    }
    ctx = hydrate_context(payload)
    assert len(ctx.rag_hits) == 1
    assert ctx.rag_hits[0]["text"] == "x"


def test_rag_agent_uses_snapshot_hits_and_skips_live_query():
    """When ``ctx.rag_hits`` is populated, ``RAGAgent._build_messages``
    consumes them directly and never calls ``self.rag.query``."""
    fake_llm = _llm_namespace()
    fake_rag = MagicMock()
    fake_rag.doc_count = 0  # would normally short-circuit, but snapshot wins
    fake_rag.query = MagicMock(side_effect=AssertionError("must not be called"))

    snapshot_hits = [
        {
            "text": "frozen chunk",
            "metadata": {"source": "/docs/freeze.md", "chunk_idx": 0},
            "score": 1.2,
            "distance": 0.2,
        }
    ]
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int", "samples": []}]},
        rag_hits=snapshot_hits,
    )
    agent = RAGAgent(llm=fake_llm, rag_store=fake_rag)
    built = agent._build_messages(ctx)
    assert built is not None, "agent should produce a prompt from snapshot hits"
    messages, prompt_hits = built
    assert prompt_hits == snapshot_hits
    fake_rag.query.assert_not_called()


def test_rag_agent_without_snapshot_falls_back_to_live_query():
    """Pre-PR-D snapshots (or normal runs) have empty ``rag_hits``
    and must go down the live-query path unchanged."""
    fake_llm = _llm_namespace()
    fake_rag = MagicMock()
    fake_rag.doc_count = 5
    fake_rag.query = MagicMock(
        return_value=[
            {
                "text": "live chunk",
                "metadata": {"source": "/docs/live.md", "chunk_idx": 0},
                "score": 0.5,
                "distance": 0.4,
            }
        ]
    )
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int", "samples": []}]},
    )
    agent = RAGAgent(llm=fake_llm, rag_store=fake_rag)
    built = agent._build_messages(ctx)
    assert built is not None
    assert fake_rag.query.called
