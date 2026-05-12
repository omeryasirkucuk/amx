"""PR D — RAG retrieval timeout.

``RAGStore.query`` accepts a ``timeout: float | None`` kwarg; when the
underlying Chroma call exceeds the cap, the call raises
:class:`RAGQueryTimeout` so callers can branch on the timeout case
(rather than ambiguously returning ``[]``). The :class:`RAGAgent`
catches the exception, falls back to "no docs used", and records a
user-facing diagnostic.
"""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _slow_query(*args, **kwargs):
    time.sleep(0.5)
    return {
        "documents": [[]],
        "metadatas": [[]],
        "distances": [[]],
    }


def test_ragstore_query_raises_on_timeout(monkeypatch, caplog):
    """The lower-level ``RAGStore.query`` raises ``RAGQueryTimeout``
    and logs a structured warning when the Chroma call overruns."""
    from amx.docs.rag import RAGQueryTimeout, RAGStore

    store = object.__new__(RAGStore)
    store.collection = SimpleNamespace(query=_slow_query)
    store.source_filters = []
    # ``rerank`` is unreachable on the timeout path but ``query``
    # accesses ``self`` for ``_source_allowed``; bind the bound method
    # back so the slow path stays representative.

    with caplog.at_level(logging.WARNING, logger="docs.rag"):
        with pytest.raises(RAGQueryTimeout):
            RAGStore.query(store, "what is foo", n_results=2, timeout=0.1)

    assert any(
        "timeout" in rec.message.lower() and "rag retrieval" in rec.message.lower()
        for rec in caplog.records
    ), f"expected a structured timeout warning; got {[r.message for r in caplog.records]}"


def test_ragstore_query_no_timeout_kwarg_runs_synchronously(monkeypatch):
    """``timeout=None`` (or <=0) bypasses the executor entirely."""
    from amx.docs.rag import RAGStore

    calls = []

    def _fast_query(**kw):
        calls.append(kw)
        return {
            "documents": [["chunk text"]],
            "metadatas": [[{"source": "/tmp/a", "source_root": "", "chunk_idx": 0}]],
            "distances": [[0.1]],
        }

    store = object.__new__(RAGStore)
    store.collection = SimpleNamespace(query=_fast_query)
    store.source_filters = []

    hits = RAGStore.query(store, "q", n_results=1, timeout=None)
    assert len(hits) == 1
    assert calls  # executed at least once


def test_rag_agent_timeout_emits_diagnostic_and_returns_no_messages():
    """When ``rag.query`` times out, the agent surfaces a diagnostic
    and returns ``None`` from ``_build_messages`` so the orchestrator
    proceeds without RAG context."""
    from amx.agents.base import AgentContext
    from amx.agents.rag_agent import RAGAgent
    from amx.docs.rag import RAGQueryTimeout

    fake_llm = MagicMock()
    fake_llm.cfg = SimpleNamespace(
        temperature=0.0,
        max_tokens=512,
        column_batch_size=10,
        n_alternatives=3,
        description_verbosity="brief",
        logprob_high=0.9,
        logprob_medium=0.7,
        rag_query_timeout_sec=0.05,
        prompt_detail_cfg=SimpleNamespace(rag_table_hits=2, rag_col_hits=0, rag_max_chunks=4),
    )

    fake_rag = MagicMock()
    fake_rag.doc_count = 7  # non-zero so we exercise the live-query path

    def _raise_timeout(*args, **kwargs):
        raise RAGQueryTimeout("simulated")

    fake_rag.query.side_effect = _raise_timeout

    agent = RAGAgent(llm=fake_llm, rag_store=fake_rag)
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
    )
    out = agent.run(ctx)
    assert out == []
    diagnostics = agent.consume_diagnostics()
    joined = " | ".join(diagnostics).lower()
    assert "timed out" in joined or "timeout" in joined, (
        f"expected a timeout diagnostic; got {diagnostics!r}"
    )
