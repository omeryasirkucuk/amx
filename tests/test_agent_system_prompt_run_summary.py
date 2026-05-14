"""System prompt directs the LLM to render run summaries with applied state.

Reported: asking the chat "which columns applied on my last run?" returned
a `Column | Confidence` table and dropped the Applied state entirely.
Screenshot of run #66 showed five rows + a "(1 more — truncated in output)"
marker — the LLM was applying the STATS-EXAMPLE-DRILL default to a
six-row ``describe_run`` payload AND choosing its own column layout.

Pin the new Style bullet so a future prompt edit can't silently regress.
"""

from __future__ import annotations

from amx.config import AMXConfig
from amx.search.tool_agent import _agent_system_prompt


def test_prompt_calls_out_describe_run_and_compare_runs() -> None:
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "Run summary tables" in prompt
    assert "describe_run" in prompt
    assert "compare_runs" in prompt


def test_prompt_pins_applied_column_layout() -> None:
    """The Applied column must lead and the four-column layout must be
    explicit so the LLM does not invent its own header set."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "Applied | Column | Confidence | Description" in prompt


def test_prompt_pins_applied_state_markers() -> None:
    """✅ for applied rows, ⏭️ for proposed-but-not-applied rows.
    Both literals must be present — losing either would cause the LLM
    to fall back to text labels or drop the column."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "✅" in prompt
    assert "⏭️" in prompt


def test_prompt_forbids_run_summary_truncation() -> None:
    """The truncation override is what stops the LLM from rendering
    "(1 more — truncated in output)" against a bounded six-row payload.
    Without this line the LLM applies the STATS-EXAMPLE-DRILL default."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    # Collapse line wraps so the regression check tolerates the source
    # string-literal hard wraps (each bullet is split across several
    # lines for source readability, not for semantic chunks).
    flat = " ".join(prompt.split())
    assert "never apply the STATS-EXAMPLE-DRILL truncation" in flat
    assert "ALWAYS render every row" in flat


def test_prompt_directs_applied_columns_for_apply_question() -> None:
    """The precomputed ``applied_columns`` summary is the authoritative
    answer when the user asks which columns were applied. Pin the
    reference so a future prompt edit doesn't drop it."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "applied_columns" in prompt
    assert "which columns were applied" in prompt


def test_prompt_includes_run_header_format() -> None:
    """The header line gives the user run id + status + counts at a
    glance before the table. Anchor each of the three required fields."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "Run #<run_id>" in prompt
    assert "<status>" in prompt
    assert "<applied_count>" in prompt
    assert "<results_count>" in prompt
