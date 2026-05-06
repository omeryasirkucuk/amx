"""System prompt directs the LLM to use lists/tables for many-item answers.

Reported (Studio + CLI screenshots): asking "which schemas do I have"
across 2 profiles returned a comma-separated paragraph cramming 70+
schema names into one block of text — unreadable. Both surfaces
already render Markdown (Studio: react-markdown + remark-gfm; CLI:
Rich Markdown), so the gap was that the prompt's Style block told
the LLM to produce "one natural-language paragraph", biasing it
away from lists.

These tests pin the new directives so a future prompt edit doesn't
silently regress the format.
"""

from __future__ import annotations

from amx.config import AMXConfig
from amx.search.tool_agent import _agent_system_prompt


def test_prompt_explains_markdown_renders_on_both_surfaces() -> None:
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "Studio" in prompt
    assert "CLI" in prompt
    # The "render Markdown" / readability statement that licenses the
    # LLM to actually use lists / tables.
    assert "Markdown" in prompt


def test_prompt_directs_lists_for_many_items() -> None:
    """5+ items → bullet list, not comma-separated paragraph."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "Listing many items" in prompt
    # The anti-pattern is called out explicitly so the LLM doesn't
    # default to it on long lists.
    lower = prompt.lower()
    assert "comma-separated" in lower or "comma" in lower
    assert "bullet list" in lower


def test_prompt_caps_long_list_sizes() -> None:
    """When the data set is huge (>30 items) the LLM should mention
    the total + truncate or aggregate, not dump all 70 inline."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "30 items" in prompt or "30 entries" in prompt or "~30" in prompt
    assert "total" in prompt.lower()


def test_prompt_directs_tables_for_tabular_data() -> None:
    """When 3+ rows share the same shape (columns + dtype + nullable
    + comment, scored join candidates), use a Markdown GFM table."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "GFM table" in prompt or "Markdown table" in prompt or "Tabular" in prompt


def test_prompt_directs_per_profile_grouping_in_multi_profile() -> None:
    """Multi-profile breakdowns: profile name **bold** at the start
    of each item, then the data nested. The previous prompt didn't
    say this, so the LLM produced "dbr and test-postgre both expose
    the same schema set under catalog/database amx_test, including
    address, airline, app_store, …" — unreadable for 70 schemas."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(
        cfg,
        ["public"],
        scope_profiles=["alpha", "beta"],
    )
    # The multi-profile block tells the LLM to bold the profile name
    # at the start of each row when listing per-profile data.
    assert "bullet list" in prompt.lower()
    assert "**bold**" in prompt or "**" in prompt
    # And the explicit anti-pattern note.
    assert "comma" in prompt.lower()


def test_prompt_directs_single_paragraph_for_short_answers() -> None:
    """The new rule isn't 'always use lists' — short answers (≤4
    items, single fact) still get a paragraph. Pin both branches so
    a future edit doesn't push everything into bullets."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    # The 'short answer' threshold is documented.
    assert "Short answer" in prompt or "short answer" in prompt
