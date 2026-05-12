"""PR δ — code RAG retrieval timeout.

``query_code_snippets`` accepts a ``timeout: float | None`` kwarg; when
the underlying Chroma call exceeds the cap, the call raises
:class:`amx.docs.rag.RAGQueryTimeout` (re-used so docs + code share one
exception class). :class:`CodeAgent` catches the exception, falls back
to regex refs only, and records a user-facing diagnostic.
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


def test_query_code_snippets_respects_timeout(monkeypatch, caplog):
    """Lower-level ``query_code_snippets`` raises ``RAGQueryTimeout``
    and logs a structured warning when the Chroma call overruns."""
    from amx.codebase import code_rag as cr
    from amx.docs.rag import RAGQueryTimeout

    fake_coll = SimpleNamespace(query=_slow_query)
    monkeypatch.setattr(
        cr,
        "_open_collection",
        lambda *_a, **_kw: fake_coll,
    )
    monkeypatch.setattr(
        cr.chromadb,
        "PersistentClient",
        lambda **_kw: SimpleNamespace(),
    )

    with caplog.at_level(logging.WARNING, logger="codebase.code_rag"):
        with pytest.raises(RAGQueryTimeout):
            cr.query_code_snippets("anything", n_results=1, timeout=0.1)

    assert any(
        "timeout" in rec.message.lower() and "code rag" in rec.message.lower()
        for rec in caplog.records
    ), f"expected a structured timeout warning; got {[r.message for r in caplog.records]}"


def test_query_code_snippets_no_timeout_runs_synchronously(monkeypatch):
    """``timeout=None`` (or <= 0) bypasses the executor entirely."""
    from amx.codebase import code_rag as cr

    calls = []

    def _fast_query(**kw):
        calls.append(kw)
        return {
            "documents": [["chunk text"]],
            "metadatas": [[{"source": "/x/y.py", "source_root": "/x", "chunk_idx": 0}]],
            "distances": [[0.1]],
        }

    fake_coll = SimpleNamespace(query=_fast_query)
    monkeypatch.setattr(cr, "_open_collection", lambda *_a, **_kw: fake_coll)
    monkeypatch.setattr(cr.chromadb, "PersistentClient", lambda **_kw: SimpleNamespace())

    hits = cr.query_code_snippets("q", n_results=1, timeout=None)
    assert len(hits) == 1
    assert calls  # exercised once


def test_code_agent_falls_back_on_timeout(monkeypatch):
    """When ``query_code_snippets`` times out, :class:`CodeAgent`
    records a diagnostic and proceeds with regex refs only."""
    from amx.agents.base import AgentContext
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.analyzer import CodebaseReport, CodeReference
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
        prompt_detail_cfg=SimpleNamespace(code_col_hits=0),
    )

    report = CodebaseReport(
        path="/tmp/repo",
        total_files=1,
        scanned_files=1,
        references={
            "orders": [
                CodeReference(
                    file="orders.py",
                    line_no=10,
                    line_text="select * from orders",
                    matched_asset="orders",
                    context="select * from orders",
                )
            ]
        },
        external_mentions={},
    )

    # Stub the semantic gate so ``has_sem`` is truthy and the agent
    # ends up calling ``query_code_snippets`` (which we make raise).
    monkeypatch.setattr(
        "amx.codebase.code_rag.code_collection_count",
        lambda **_kw: 99,
    )
    monkeypatch.setattr(
        "amx.codebase.code_rag.query_code_snippets",
        lambda *_a, **_kw: (_ for _ in ()).throw(RAGQueryTimeout("simulated")),
    )

    agent = CodeAgent(llm=fake_llm, report=report)
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
    )
    msgs = agent._build_messages(ctx)
    # Regex ref still produces a usable prompt — the timeout only
    # nukes the semantic block, not the table-ref block.
    assert msgs is not None
    diagnostics = agent.consume_diagnostics()
    assert any("timed out" in d.lower() or "timeout" in d.lower() for d in diagnostics), (
        f"expected a timeout diagnostic; got {diagnostics!r}"
    )
