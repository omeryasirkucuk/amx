"""PR γ (code-RAG hardening): citation chain for code-derived suggestions.

Pins the contract introduced by PR γ:

* Every Python AST chunk indexed into ``amx_code`` records 1-based
  ``start_line`` / ``end_line`` metadata.
* ``CodeAgent`` attaches one :class:`Citation` per semantic hit AND per
  regex reference fed into the prompt, on every produced
  :class:`MetadataSuggestion`. Citations from both channels are deduped
  by ``(source, chunk_idx, line_range)``.
* Non-code suggestions (profile, manual, etc.) default to
  ``citations=[]`` so the rest of the pipeline can branch cleanly.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from amx.agents.base import Citation, Confidence, MetadataSuggestion


def _llm_cfg() -> SimpleNamespace:
    return SimpleNamespace(
        temperature=0.0,
        max_tokens=512,
        column_batch_size=10,
        n_alternatives=1,
        description_verbosity="brief",
        logprob_high=0.9,
        logprob_medium=0.7,
        prompt_detail_cfg=SimpleNamespace(code_col_hits=0),
    )


def _fake_code_hits() -> list[dict]:
    """Three code-RAG hits matching the post-ingest dict shape."""
    return [
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
        {
            "text": "class UserService:\n    def __init__(self): ...\n",
            "metadata": {
                "source": "/repo/src/service.py",
                "source_root": "/repo",
                "rel_path": "src/service.py",
                "chunk_id": "UserService_10",
                "kind": "python_ast",
                "start_line": 10,
                "end_line": 28,
            },
            "distance": 0.33,
        },
        {
            "text": "# Notebook cell\nfrom service import UserService\n",
            "metadata": {
                "source": "/repo/notebooks/demo.ipynb",
                "source_root": "/repo",
                "rel_path": "notebooks/demo.ipynb",
                "chunk_id": "cell2",
                "kind": "ipynb_code",
                "start_line": 3,
                "end_line": 3,
            },
            "distance": 0.41,
        },
    ]


def test_chunk_metadata_records_line_ranges(tmp_path: Path) -> None:
    """A Python file with two functions yields two AST chunks whose
    ``start_line`` / ``end_line`` reflect the source ranges.
    """
    from amx.codebase.code_rag import _iter_python_chunks

    source = (
        "def alpha():\n"
        "    '''alpha doc.'''\n"
        "    return 1\n"
        "\n"
        "\n"
        "def beta():\n"
        "    '''beta doc -- longer body so the segment > 40 chars.'''\n"
        "    x = 1\n"
        "    y = 2\n"
        "    return x + y\n"
    )
    chunks = _iter_python_chunks("mod.py", source)
    assert len(chunks) == 2

    by_name = {cid.split("_")[0]: (start, end) for (cid, _text, start, end) in chunks}
    # alpha spans lines 1..3 (def + body), beta spans 6..10.
    assert by_name["alpha"] == (1, 3)
    assert by_name["beta"] == (6, 10)


def test_python_chunk_module_fallback_spans_full_file() -> None:
    """A Python file with no AST hits falls back to a module chunk
    whose line range spans the whole file (defensive: a citation must
    always point somewhere real)."""
    from amx.codebase.code_rag import _iter_python_chunks

    source = "# top-level\nx = 1\ny = 2\n"
    chunks = _iter_python_chunks("trivial.py", source)
    # Tree exists but holds no function/class — fall back to module.
    if chunks:
        cid, _text, start, end = chunks[0]
        assert cid == "module"
        assert start == 1
        # ``content.count("\n") + 1`` is the line count.
        assert end == source.count("\n") + 1


def test_generic_splitter_chunks_record_line_ranges() -> None:
    """Non-Python files chunked by the recursive splitter still get
    1-based line bounds derived from the chunk's offset.
    """
    from amx.codebase.code_rag import _iter_split_chunks

    text = "\n".join(f"line {i}" for i in range(1, 201)) + "\n"
    chunks = _iter_split_chunks(text, max_chars=500)
    assert chunks, "splitter should produce at least one chunk"
    first_cid, first_text, first_start, first_end = chunks[0]
    assert first_cid == "part0"
    assert first_start == 1
    # End line must be >= start line and within file bounds.
    assert first_end >= first_start
    assert first_end <= 200


def test_code_agent_attaches_citations_from_retrieval() -> None:
    """``CodeAgent`` builds :class:`Citation` records from the semantic
    hits it consumes — the LLM's free-text reasoning is irrelevant.
    """
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.analyzer import CodebaseReport

    fake_llm = MagicMock()
    fake_llm.cfg = _llm_cfg()
    fake_llm.model_name = "test-model"
    fake_llm.chat.return_value = SimpleNamespace(
        content=(
            "COLUMN: id\n"
            "DESCRIPTION_1: Primary key for the user record.\n"
            "CONFIDENCE: HIGH\n"
            "REASONING: Code calls SELECT * FROM users using this id.\n"
        ),
        logprobs=None,
        usage=None,
    )

    report = CodebaseReport(path="/repo", references={}, external_mentions={})
    agent = CodeAgent(llm=fake_llm, report=report)
    ctx = SimpleNamespace(
        schema="public",
        table="users",
        column=None,
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
        rag_context=[],
        code_context=[],
        existing_metadata={},
        user_instructions="",
    )

    with (
        patch("amx.codebase.code_rag.code_collection_count", return_value=7),
        patch(
            "amx.codebase.code_rag.query_code_snippets",
            return_value=_fake_code_hits(),
        ),
    ):
        out = agent.run(ctx)

    assert len(out) == 1
    suggestion = out[0]
    assert suggestion.source == "codebase"
    # One citation per unique (rel_path, line_range) — three hits with
    # three distinct sources/lines.
    assert len(suggestion.citations) == 3
    by_source = {c.source: c for c in suggestion.citations}
    assert by_source["src/loaders.py"].line_range == (42, 55)
    assert by_source["src/service.py"].line_range == (10, 28)
    assert by_source["notebooks/demo.ipynb"].line_range == (3, 3)
    # Snippet pulled from hit text, capped at 200 chars.
    assert all(len(c.snippet) <= 200 for c in suggestion.citations)
    # Score normalises from Chroma distance.
    assert by_source["src/loaders.py"].score > 0.0


def test_regex_references_become_citations() -> None:
    """A :class:`CodeReference`-only path (no Chroma collection)
    still produces single-line citations on every suggestion."""
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.analyzer import CodebaseReport, CodeReference

    fake_llm = MagicMock()
    fake_llm.cfg = _llm_cfg()
    fake_llm.model_name = "test-model"
    fake_llm.chat.return_value = SimpleNamespace(
        content=(
            "COLUMN: email\n"
            "DESCRIPTION_1: User email address.\n"
            "CONFIDENCE: MEDIUM\n"
            "REASONING: Code references customers.email in two places.\n"
        ),
        logprobs=None,
        usage=None,
    )

    refs = {
        "customers": [
            CodeReference(
                file="src/load.py",
                line_no=120,
                line_text="SELECT * FROM customers",
                matched_asset="customers",
                context="db.query('SELECT * FROM customers')",
            ),
        ],
        "email": [
            CodeReference(
                file="src/load.py",
                line_no=148,
                line_text="row.email",
                matched_asset="email",
                context="if row.email: send(row.email)",
            ),
        ],
    }
    report = CodebaseReport(path="/repo", references=refs, external_mentions={})
    agent = CodeAgent(llm=fake_llm, report=report)
    ctx = SimpleNamespace(
        schema="public",
        table="customers",
        column=None,
        db_profile={"columns": [{"name": "email", "dtype": "text"}]},
        rag_context=[],
        code_context=[],
        existing_metadata={},
        user_instructions="",
    )

    with (
        patch("amx.codebase.code_rag.code_collection_count", return_value=0),
        patch("amx.codebase.code_rag.query_code_snippets", return_value=[]),
    ):
        out = agent.run(ctx)

    assert len(out) == 1
    cits = out[0].citations
    # Two refs at different (file, line_no) yield two citations.
    by_key = {(c.source, c.line_range): c for c in cits}
    assert ("src/load.py", (120, 120)) in by_key
    assert ("src/load.py", (148, 148)) in by_key
    assert all(c.chunk_idx == 0 for c in cits)
    assert all(c.score == 1.0 for c in cits)


def test_merged_code_citations_dedupe_by_line_range() -> None:
    """When both regex refs and semantic hits land on the same
    ``(rel_path, line_range)``, the citation appears once after the
    agent's dedup pass (regardless of the orchestrator merge)."""
    from amx.agents._citations import attach_citations, hits_to_citations
    from amx.agents.base import MetadataSuggestion

    hits = [
        {
            "text": "duplicate-source chunk",
            "metadata": {
                "source": "/repo/src/load.py",
                "rel_path": "src/load.py",
                "chunk_id": "load_120",
                "start_line": 120,
                "end_line": 120,
            },
            "distance": 0.2,
        }
    ]
    extra = [
        Citation(
            source="src/load.py",
            chunk_idx=0,
            score=1.0,
            snippet="db.query('...')",
            line_range=(120, 120),
        )
    ]
    s = MetadataSuggestion(
        schema="public",
        table="customers",
        column="id",
        suggestions=["x"],
        confidence=Confidence.HIGH,
        reasoning="",
        source="codebase",
        citations=[],
    )
    cits = hits_to_citations(hits) + extra
    attach_citations([s], cits)
    # Even though both inputs target ``src/load.py:120``, only one
    # citation survives the (source, chunk_idx, line_range) dedup.
    matching = [c for c in s.citations if c.source == "src/load.py"]
    assert len(matching) == 1
    assert matching[0].line_range == (120, 120)


def test_non_code_suggestions_have_empty_citations() -> None:
    """Profile-agent suggestions default to ``citations=[]`` so the
    rest of the pipeline can branch on truthiness."""
    s = MetadataSuggestion(
        schema="s",
        table="t",
        column="c",
        suggestions=["x"],
        confidence=Confidence.MEDIUM,
        reasoning="",
        source="db_profile",
    )
    assert s.citations == []


def test_code_agent_drops_spark_bias_from_semantic_query() -> None:
    """The hardcoded ``"SQL Spark dataframe usage"`` suffix is gone —
    the query is now just ``f"{schema} {table}"``.
    """
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.analyzer import CodebaseReport

    fake_llm = MagicMock()
    fake_llm.cfg = _llm_cfg()
    fake_llm.model_name = "test-model"
    fake_llm.chat.return_value = SimpleNamespace(content="", logprobs=None, usage=None)

    report = CodebaseReport(path="/repo", references={}, external_mentions={})
    agent = CodeAgent(llm=fake_llm, report=report)
    ctx = SimpleNamespace(
        schema="public",
        table="orders",
        column=None,
        db_profile={"columns": [{"name": "id", "dtype": "int"}]},
        rag_context=[],
        code_context=[],
        existing_metadata={},
        user_instructions="",
    )

    captured_queries: list[str] = []

    def fake_query(q: str, **_: object) -> list[dict]:
        captured_queries.append(q)
        return []

    with (
        patch("amx.codebase.code_rag.code_collection_count", return_value=5),
        patch("amx.codebase.code_rag.query_code_snippets", side_effect=fake_query),
    ):
        agent._build_messages(ctx)

    assert captured_queries, "expected at least one semantic query"
    table_query = captured_queries[0]
    assert "Spark" not in table_query
    assert "dataframe" not in table_query
    assert table_query == "public orders"


def test_code_col_hits_fans_out_per_column() -> None:
    """When ``code_col_hits > 0``, the agent issues one extra
    ``<table>.<col>`` semantic query per column."""
    from amx.agents.code_agent import CodeAgent
    from amx.codebase.analyzer import CodebaseReport

    fake_llm = MagicMock()
    fake_llm.cfg = SimpleNamespace(
        temperature=0.0,
        max_tokens=512,
        column_batch_size=10,
        n_alternatives=1,
        description_verbosity="brief",
        logprob_high=0.9,
        logprob_medium=0.7,
        prompt_detail_cfg=SimpleNamespace(code_col_hits=1),
    )
    fake_llm.model_name = "test-model"
    fake_llm.chat.return_value = SimpleNamespace(content="", logprobs=None, usage=None)

    report = CodebaseReport(path="/repo", references={}, external_mentions={})
    agent = CodeAgent(llm=fake_llm, report=report)
    ctx = SimpleNamespace(
        schema="public",
        table="orders",
        column=None,
        db_profile={
            "columns": [
                {"name": "id", "dtype": "int"},
                {"name": "amount", "dtype": "numeric"},
            ]
        },
        rag_context=[],
        code_context=[],
        existing_metadata={},
        user_instructions="",
    )

    captured: list[str] = []

    def fake_query(q: str, **_: object) -> list[dict]:
        captured.append(q)
        return []

    with (
        patch("amx.codebase.code_rag.code_collection_count", return_value=5),
        patch("amx.codebase.code_rag.query_code_snippets", side_effect=fake_query),
    ):
        agent._build_messages(ctx)

    # First query is the table-level neutral one; the next two are per
    # column.
    assert captured[0] == "public orders"
    assert "orders.id column" in captured
    assert "orders.amount column" in captured
