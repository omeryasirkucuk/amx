"""RAG agent emits a diagnostic when retrieval returned no chunks.

Before PR A the empty-context branch was just ``log.info(...)`` —
the user got no signal from /run that the docs they ingested didn't
match the table. Now ``RAGAgent`` mirrors ``ProfileAgent``'s
``_record_diagnostic`` pattern; the orchestrator already drains
diagnostics into the per-table summary, so a /run against a table with
no doc hits surfaces "RAG: no relevant documents found".
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def test_rag_agent_records_diagnostic_when_context_empty() -> None:
    from amx.agents.rag_agent import RAGAgent

    fake_llm = MagicMock()
    fake_llm.cfg = SimpleNamespace(
        temperature=0.0,
        max_tokens=512,
        column_batch_size=10,
        n_alternatives=3,
        description_verbosity="brief",
        logprob_high=0.9,
        logprob_medium=0.7,
        prompt_detail_cfg=SimpleNamespace(rag_table_hits=2, rag_col_hits=1, rag_max_chunks=4),
    )
    fake_rag = MagicMock()
    fake_rag.doc_count = 0  # ``_build_messages`` returns ``None`` immediately

    agent = RAGAgent(llm=fake_llm, rag_store=fake_rag)
    ctx = SimpleNamespace(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
    )
    out = agent.run(ctx)
    assert out == []

    diagnostics = agent.consume_diagnostics()
    assert any(
        "no relevant documents" in d.lower() or "no rag" in d.lower() for d in diagnostics
    ), f"expected a no-context diagnostic; got {diagnostics!r}"


def test_rag_agent_consume_diagnostics_clears_buffer() -> None:
    """Mirrors :class:`ProfileAgent` so the orchestrator drain pattern
    (read once, then clear) works the same way across agents."""
    from amx.agents.rag_agent import RAGAgent

    fake_llm = MagicMock()
    fake_llm.cfg = SimpleNamespace(
        temperature=0.0,
        max_tokens=512,
        column_batch_size=10,
        n_alternatives=3,
        description_verbosity="brief",
        logprob_high=0.9,
        logprob_medium=0.7,
        prompt_detail_cfg=SimpleNamespace(rag_table_hits=2, rag_col_hits=1, rag_max_chunks=4),
    )
    fake_rag = MagicMock(doc_count=0)
    agent = RAGAgent(llm=fake_llm, rag_store=fake_rag)
    agent._record_diagnostic("hello")
    assert agent.consume_diagnostics() == ["hello"]
    assert agent.consume_diagnostics() == []
