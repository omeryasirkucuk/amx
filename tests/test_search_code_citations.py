"""PR γ: ``search_code`` tool citations.

* ``_tool_search_code`` surfaces ``chunk_idx`` + ``start_line`` +
  ``end_line`` on every hit, sourced from the chunk metadata.
* ``_summarise_tool_call`` extracts a populated citations list for
  ``search_code`` calls (was previously empty — only ``search_docs``
  emitted citations before PR γ).
* ``search_docs`` keeps emitting citations exactly as before
  (regression guard).
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from amx.search.tool_agent import _summarise_tool_call


def test_search_code_tool_result_carries_chunk_metadata() -> None:
    """``_tool_search_code`` returns hits with chunk_idx + start_line +
    end_line populated from the chunk metadata."""
    from amx.search.agent_tools import ToolBox

    cfg = SimpleNamespace(
        code_profiles={"py": "/repo"},
        doc_profiles={},
    )
    tb = ToolBox.__new__(ToolBox)
    tb.cfg = cfg
    tb.db_profiles = ["py"]

    fake_raw_hits = [
        {
            "text": "def load_users():\n    return db.query('SELECT * FROM users')\n",
            "metadata": {
                "source": "/repo/src/loaders.py",
                "source_root": "/repo",
                "rel_path": "src/loaders.py",
                "chunk_id": "load_users_42",
                "kind": "python_ast",
                "start_line": 42,
                "end_line": 55,
            },
            "distance": 0.18,
        },
    ]
    with (
        patch(
            "amx.search._agent.scope.resolve_code_profiles_for_scope",
            return_value=["py"],
        ),
        patch("amx.codebase.code_rag.code_collection_count", return_value=10),
        patch("amx.codebase.code_rag.query_code_snippets", return_value=fake_raw_hits),
    ):
        out = tb._tool_search_code("user loader", n_results=5)

    assert out["count"] == 1
    hit = out["hits"][0]
    assert hit["rel_path"] == "src/loaders.py"
    assert hit["start_line"] == 42
    assert hit["end_line"] == 55
    # ``chunk_idx`` is best-effort numeric coercion from
    # ``chunk_id``; the symbol-shaped key collapses to ``0`` which is
    # fine because ``line_range`` carries the real provenance.
    assert "chunk_idx" in hit


def test_summarise_tool_call_extracts_citations_for_search_code() -> None:
    """``_summarise_tool_call`` returns a populated citations list for
    ``search_code`` results (was previously empty before PR γ).
    """
    fake_call = SimpleNamespace(name="search_code", arguments='{"query":"user"}')
    payload = json.dumps(
        {
            "count": 2,
            "hits": [
                {
                    "rel_path": "src/loaders.py",
                    "source": "/repo/src/loaders.py",
                    "snippet": "def load_users(): ...",
                    "distance": 0.2,
                    "chunk_idx": 0,
                    "start_line": 42,
                    "end_line": 55,
                },
                {
                    "rel_path": "src/service.py",
                    "snippet": "class UserService: ...",
                    "distance": 0.4,
                    "chunk_idx": 0,
                    "start_line": 10,
                    "end_line": 28,
                },
            ],
        }
    )
    summary = _summarise_tool_call(fake_call, payload)
    cits = summary.get("citations")
    assert isinstance(cits, list)
    assert len(cits) == 2

    by_source = {c["source"]: c for c in cits}
    assert by_source["src/loaders.py"]["line_range"] == [42, 55]
    assert by_source["src/service.py"]["line_range"] == [10, 28]
    # Score derived from distance (1 - distance, clamped to >= 0).
    assert by_source["src/loaders.py"]["score"] > 0.7


def test_summarise_tool_call_still_handles_search_docs() -> None:
    """Regression guard: ``search_docs`` keeps emitting citations
    exactly as in PR E; ``line_range`` is ``None`` because doc chunks
    don't carry that field.
    """
    fake_call = SimpleNamespace(name="search_docs", arguments='{"query":"x"}')
    payload = json.dumps(
        {
            "count": 1,
            "hits": [
                {
                    "source": "docs/spec.pdf",
                    "snippet": "Customers table holds CRM records.",
                    "distance": 0.21,
                    "chunk_idx": 3,
                }
            ],
        }
    )
    summary = _summarise_tool_call(fake_call, payload)
    cits = summary["citations"]
    assert len(cits) == 1
    assert cits[0]["source"] == "docs/spec.pdf"
    assert cits[0]["chunk_idx"] == 3
    # No start_line on doc hits, so the field falls back to ``None``.
    assert cits[0]["line_range"] is None


def test_summarise_tool_call_non_retrieval_has_no_citations() -> None:
    """Non-retrieval tool calls (e.g. ``run_sql``) still get no
    citations key — PR γ scoped the citations extraction to the two
    retrieval tools and left every other tool untouched.
    """
    fake_call = SimpleNamespace(name="run_sql", arguments='{"sql":"select 1"}')
    summary = _summarise_tool_call(fake_call, "1")
    assert "citations" not in summary
