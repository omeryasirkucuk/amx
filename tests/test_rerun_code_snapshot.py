"""PR δ — re-Run snapshots the original run's code hits.

When the re-Run worker rehydrates the snapshot,
``AgentContext.code_hits`` carries the chunks the original run fed
into the Code agent's prompt and :class:`CodeAgent` skips the live
``query_code_snippets`` calls. Pre-PR-δ snapshots without
``code_hits`` silently fall back to the live-query path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.agents.base import AgentContext
from amx.agents.code_agent import CodeAgent
from amx.agents.rerun_context import _serialize_code_hit, hydrate_context
from amx.codebase.analyzer import CodebaseReport


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
            prompt_detail_cfg=SimpleNamespace(code_col_hits=0),
        ),
    )


def test_serialize_code_hit_keeps_richer_metadata():
    hit = {
        "text": "def foo(): ...",
        "metadata": {
            "source": "/repo/foo.py",
            "source_root": "/repo",
            "rel_path": "foo.py",
            "chunk_id": "foo_1",
            "kind": "python_ast",
            "start_line": 10,
            "end_line": 22,
        },
        "distance": 0.31,
        "score": 1.4,
    }
    out = _serialize_code_hit(hit)
    assert out["text"].startswith("def foo")
    assert out["metadata"]["rel_path"] == "foo.py"
    assert out["metadata"]["start_line"] == 10
    assert out["metadata"]["end_line"] == 22
    assert out["score"] == 1.4


def test_hydrate_context_populates_code_hits():
    payload = {
        "schema": "public",
        "table": "orders",
        "code_hits": [{"text": "x", "metadata": {"source": "/a/b.py", "rel_path": "b.py"}}],
    }
    ctx = hydrate_context(payload)
    assert len(ctx.code_hits) == 1
    assert ctx.code_hits[0]["text"] == "x"


def test_code_agent_uses_snapshot_hits_and_skips_live_query(monkeypatch):
    """When ``ctx.code_hits`` is populated, ``CodeAgent._build_messages``
    consumes them directly and never calls ``query_code_snippets``."""
    fake_llm = _llm_namespace()

    # If ``query_code_snippets`` is reached, the test fails outright.
    def _must_not_call(*_a, **_kw):
        raise AssertionError("snapshot path should skip live retrieval")

    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", _must_not_call)
    # Also short-circuit code_collection_count so we don't hit Chroma.
    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", lambda **_kw: 0)

    snapshot_hits = [
        {
            "text": "frozen chunk",
            "metadata": {
                "source": "/repo/freeze.py",
                "source_root": "/repo",
                "rel_path": "freeze.py",
                "chunk_id": "module",
                "kind": "python_ast",
                "start_line": 1,
                "end_line": 4,
            },
            "score": 1.2,
            "distance": 0.2,
        }
    ]
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
        code_hits=snapshot_hits,
    )
    # No report at all — the snapshot path must still produce a prompt.
    agent = CodeAgent(llm=fake_llm, report=None)
    msgs = agent._build_messages(ctx)
    assert msgs is not None, "snapshot hits should produce a prompt"
    assert agent.last_prompt_hits[("public", "orders")] == snapshot_hits


def test_code_agent_without_snapshot_falls_back_to_live_query(monkeypatch):
    """Pre-PR-δ snapshots (or normal runs) have empty ``code_hits``
    and must go down the live-query path unchanged."""
    fake_llm = _llm_namespace()

    called = {"n": 0}

    def _fake_query(*_a, **_kw):
        called["n"] += 1
        return [
            {
                "text": "live snippet",
                "metadata": {
                    "source": "/repo/live.py",
                    "rel_path": "live.py",
                    "chunk_id": "module",
                    "kind": "python_ast",
                    "start_line": 1,
                    "end_line": 5,
                },
                "score": 0.5,
                "distance": 0.4,
            }
        ]

    monkeypatch.setattr("amx.codebase.code_rag.code_collection_count", lambda **_kw: 5)
    monkeypatch.setattr("amx.codebase.code_rag.query_code_snippets", _fake_query)

    report = CodebaseReport(path="/repo", total_files=1, scanned_files=1)
    ctx = AgentContext(
        schema="public",
        table="orders",
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
    )
    agent = CodeAgent(llm=fake_llm, report=report)
    msgs = agent._build_messages(ctx)
    assert msgs is not None
    assert called["n"] >= 1
