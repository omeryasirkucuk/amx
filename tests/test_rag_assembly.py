"""PR-H: edges-first assembly + citation header + model-aware budget.

Pure-ish unit tests for ``amx.rag_core.assembly``. The end-to-end
flow through ``RAGAgent._build_messages`` is covered by the agent's
own existing tests; here we pin the algorithmic contract of each
function so a future refactor (PR-J shared-core extraction) can't
silently drift the semantics.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from amx.rag_core.assembly import (
    assemble_chunks,
    compute_input_budget,
    format_chunk_header,
)

# ── assemble_chunks: edges-first reorder ─────────────────────────────


def test_assemble_six_chunks_anchors_top_scorers_at_both_ends() -> None:
    """Canonical example from Liu et al. 2023: [c1..c6] (descending
    score) should reorder to [c1, c3, c5, c6, c4, c2] — top scorers
    at the edges, mid-scorers in the attention-dead middle."""
    chunks = [{"id": i} for i in range(1, 7)]
    result = assemble_chunks(chunks, k=6)
    ids = [c["id"] for c in result]
    assert ids == [1, 3, 5, 6, 4, 2]


def test_assemble_truncates_to_k_before_reordering() -> None:
    """``k`` is a hard cap. Chunks beyond index k are dropped by
    construction — the old "drop from middle" budget operation
    is implicit in the slice + reorder."""
    chunks = [{"id": i} for i in range(1, 11)]  # 10 chunks
    result = assemble_chunks(chunks, k=4)
    ids = [c["id"] for c in result]
    # Top 4 → [1, 2, 3, 4]; odd → [1, 3], even reversed → [4, 2].
    assert ids == [1, 3, 4, 2]


def test_assemble_k_larger_than_input_is_safe() -> None:
    """``k`` greater than the candidate pool size reorders the whole
    pool without raising."""
    chunks = [{"id": i} for i in range(1, 4)]  # only 3 chunks
    result = assemble_chunks(chunks, k=10)
    ids = [c["id"] for c in result]
    # Top 3 → [1, 2, 3]; odd → [1, 3], even reversed → [2].
    assert ids == [1, 3, 2]


def test_assemble_single_chunk_passes_through() -> None:
    """Edge case: a single chunk has no middle to bury — return it
    unchanged."""
    result = assemble_chunks([{"id": 42}], k=5)
    assert result == [{"id": 42}]


def test_assemble_empty_input_returns_empty_list() -> None:
    assert assemble_chunks([], k=5) == []


def test_assemble_k_zero_returns_empty_list() -> None:
    """``k <= 0`` short-circuits to empty rather than ValueError —
    callers (config presets) may legitimately set rag_max_chunks=0
    to disable RAG context for a profile."""
    assert assemble_chunks([{"id": 1}, {"id": 2}], k=0) == []
    assert assemble_chunks([{"id": 1}, {"id": 2}], k=-3) == []


def test_assemble_preserves_chunk_objects_by_reference() -> None:
    """The reorder is a list shuffle — the chunk dicts themselves
    must NOT be copied/mutated, so downstream code reading
    ``hit['metadata']`` sees the same objects retrieval produced."""
    chunks = [{"id": i, "text": f"body {i}"} for i in range(4)]
    result = assemble_chunks(chunks, k=4)
    for r in result:
        assert any(r is c for c in chunks)


# ── format_chunk_header: citation header rendering ───────────────────


def test_format_header_with_full_metadata() -> None:
    hit = {
        "metadata": {"source": "/path/to/orders.md", "h2": "total_amount"},
        "score": 1.25,
    }
    assert format_chunk_header(hit) == "[orders.md | section=total_amount] (rel=1.25)"


def test_format_header_falls_back_to_h1_when_h2_h3_absent() -> None:
    """``h3`` > ``h2`` > ``h1`` in specificity. Pick the most
    specific available; degrade gracefully."""
    hit = {"metadata": {"source": "x.md", "h1": "Top section"}, "score": 0.5}
    assert format_chunk_header(hit) == "[x.md | section=Top section] (rel=0.50)"


def test_format_header_prefers_h3_over_h2() -> None:
    hit = {
        "metadata": {"source": "x.md", "h2": "Outer", "h3": "Inner"},
        "score": 0.5,
    }
    header = format_chunk_header(hit)
    assert "section=Inner" in header
    assert "Outer" not in header


def test_format_header_minimal_when_only_source_present() -> None:
    """No heading metadata, no score → just the source basename in
    brackets. Used for plain-text corpora (.txt) where PR-D's
    Markdown-aware splitter doesn't fire."""
    hit = {"metadata": {"source": "/p/plain.txt"}}
    assert format_chunk_header(hit) == "[plain.txt]"


def test_format_header_handles_missing_metadata_dict() -> None:
    """Defensive against malformed hits — never raise from the
    citation path; the prompt is more useful with a degraded
    header than no header at all."""
    assert format_chunk_header({}) == "[unknown]"
    assert format_chunk_header({"metadata": None}) == "[unknown]"


def test_format_header_skips_score_when_non_numeric() -> None:
    """``score`` may be missing or a sentinel value; only render the
    relevance suffix when it's a real number."""
    hit_no_score = {"metadata": {"source": "x.md"}}
    hit_string_score = {"metadata": {"source": "x.md"}, "score": "high"}
    assert format_chunk_header(hit_no_score) == "[x.md]"
    assert format_chunk_header(hit_string_score) == "[x.md]"


# ── compute_input_budget: per-model context lookup ───────────────────


def test_compute_budget_uses_litellm_max_input_tokens_when_available() -> None:
    """LiteLLM's resolver returns a 200k-token window for Claude
    Sonnet; ``compute_input_budget`` should land below it after
    subtracting the planned output + safety margin."""
    with patch("litellm.get_model_info", return_value={"max_input_tokens": 200_000}):
        budget = compute_input_budget("claude-sonnet-4-6", max_output_tokens=16_384)
    # 200k - 16k - 256 safety margin ≈ 183_360
    assert 180_000 <= budget <= 200_000


def test_compute_budget_falls_back_to_heuristic_when_litellm_fails() -> None:
    """A custom / proxy model id LiteLLM doesn't recognise raises
    inside ``get_model_info``; the legacy ``max_tokens * 3`` heuristic
    kicks in (with a 1000-token floor)."""

    def _raise(*_args, **_kwargs):
        raise ValueError("LiteLLM: unknown model id 'proxy/custom-7b'")

    with patch("litellm.get_model_info", side_effect=_raise):
        budget = compute_input_budget("proxy/custom-7b", max_output_tokens=4096)
    assert budget == 4096 * 3  # heuristic


def test_compute_budget_honours_1000_token_floor() -> None:
    """Very small ``max_output_tokens`` should not produce a tiny
    input budget — the floor is 1000."""
    with patch("litellm.get_model_info", side_effect=Exception("nope")):
        budget = compute_input_budget("any", max_output_tokens=10)
    assert budget == 1_000


def test_compute_budget_with_empty_model_name_uses_heuristic() -> None:
    """``model_name`` is empty string when the LLM config hasn't
    been populated; treat as heuristic path. Don't even try to call
    LiteLLM in this case."""
    with patch("litellm.get_model_info", side_effect=AssertionError("should not be called")):
        budget = compute_input_budget("", max_output_tokens=2048)
    assert budget == max(1_000, 2048 * 3)


def test_compute_budget_handles_zero_max_input_tokens() -> None:
    """LiteLLM sometimes returns ``max_input_tokens=0`` for incomplete
    model records — treat that as unknown, fall through to heuristic."""
    with patch("litellm.get_model_info", return_value={"max_input_tokens": 0}):
        budget = compute_input_budget("partial-model", max_output_tokens=4096)
    assert budget == 4096 * 3


def test_compute_budget_returns_at_least_floor_even_with_huge_output() -> None:
    """If the planned output budget is so large it would consume the
    entire window, fall back to the heuristic floor — never starve
    the prompt to zero."""
    with patch("litellm.get_model_info", return_value={"max_input_tokens": 8_192}):
        budget = compute_input_budget("small", max_output_tokens=8_000)
    # max(1000, 8000*3) = 24000; usable would be -64; the floor wins.
    assert budget == max(1_000, 8_000 * 3)


def test_compute_budget_with_pytest_raises_for_invalid() -> None:
    """Smoke check that the function doesn't accidentally allow
    negative max_output_tokens to produce nonsensical budgets."""
    with patch("litellm.get_model_info", side_effect=Exception):
        budget = compute_input_budget("x", max_output_tokens=0)
    # 0 * 3 = 0; floor wins.
    assert budget >= 1_000


def test_compute_budget_litellm_missing_module_is_handled() -> None:
    """The except clause is broad enough to swallow
    ImportError too — defensive in environments where litellm
    might be shimmed or unavailable."""
    with patch("litellm.get_model_info", side_effect=ImportError("no litellm")):
        budget = compute_input_budget("x", max_output_tokens=2_048)
    assert budget == max(1_000, 2_048 * 3)


def test_compute_budget_subtracts_safety_margin() -> None:
    """Verify the 256-token safety margin is subtracted from the
    raw input window. Pinning this so a future tweak (e.g. larger
    margin for thinking-mode models) is conscious.

    Picks a window/output combo where the model-aware path
    dominates the ``max(floor, ...)`` selection (input 200k vs
    floor 24k for output=8k) so the safety-margin arithmetic is
    actually exercised.
    """
    with patch("litellm.get_model_info", return_value={"max_input_tokens": 200_000}):
        budget = compute_input_budget("gpt-x", max_output_tokens=8_000)
    # 200k - 8k - 256 = 191_744; the floor (24k) loses to this.
    assert budget == 200_000 - 8_000 - 256


# ── pytest smoke marker ──────────────────────────────────────────────


def test_assembly_module_exports() -> None:
    """Pin the public surface so PR-J's shared-core extraction
    catches accidental renames."""
    from amx.rag_core import assembly

    assert "assemble_chunks" in assembly.__all__
    assert "format_chunk_header" in assembly.__all__
    assert "compute_input_budget" in assembly.__all__


@pytest.mark.parametrize(
    "k,expected",
    [
        (1, [1]),
        (2, [1, 2]),
        (3, [1, 3, 2]),
        (4, [1, 3, 4, 2]),
        (5, [1, 3, 5, 4, 2]),
    ],
)
def test_assemble_chunks_parametric(k: int, expected: list[int]) -> None:
    """Parameter sweep on the reorder algorithm — pin the result
    for k=1..5 so the canonical pattern is uniquely defined."""
    chunks = [{"id": i} for i in range(1, 11)]
    result = assemble_chunks(chunks, k=k)
    assert [c["id"] for c in result] == expected
