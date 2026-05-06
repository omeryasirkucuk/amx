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


def test_prompt_directs_lists_for_5_to_10_items() -> None:
    """5–10 items → Markdown bullet list, sorted, one per line. Past
    10 the rule shifts to STATS-EXAMPLE-DRILL (covered in its own
    test). The two regimes are explicit so the LLM doesn't bullet-
    dump 30 names in the middle ground."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "5–10 items" in prompt or "5-10 items" in prompt
    lower = prompt.lower()
    assert "bullet list" in lower


def test_prompt_uses_stats_example_drill_for_large_sets() -> None:
    """When the data set is large (>10 items) the LLM should NOT
    bullet-dump 30 entries — that's still unscannable. The new
    STATS-EXAMPLE-DRILL pattern: total + 5-8 examples + ONE drill-in
    question."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "STATS-EXAMPLE-DRILL" in prompt
    # Anti-patterns the rule explicitly forbids.
    assert 'no "top 30"' in prompt
    assert "NEVER dump the full list" in prompt
    # The three-part shape is named.
    assert "Stats line" in prompt
    assert "example names" in prompt or "Example names" in prompt
    assert "Drill-in invitation" in prompt


def test_prompt_forbids_blocking_picker_first() -> None:
    """The user came with a question — the LLM must not refuse to
    answer until the user picks a database/schema. Stats + examples
    come first, drill-in is an invitation, not a precondition."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "NEVER ask the user to pick" in prompt
    assert "blocker" in prompt.lower()


def test_prompt_pattern_covers_multiple_data_types() -> None:
    """The STATS-EXAMPLE-DRILL rule applies to tables, columns, and
    schemas — not just schemas. Pin examples for each so a future
    edit doesn't quietly narrow the rule."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    # Tables example.
    assert "5,000 tables" in prompt or "5,142 tables" in prompt
    # Columns example (date-shaped).
    assert "800 columns" in prompt or "~800" in prompt
    # Schemas example (the screenshot scenario).
    assert "70 schemas" in prompt


def test_prompt_directs_tables_for_tabular_data() -> None:
    """When 3+ rows share the same shape (columns + dtype + nullable
    + comment, scored join candidates), use a Markdown GFM table."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    assert "GFM table" in prompt or "Markdown table" in prompt or "Tabular" in prompt


def test_prompt_directs_per_profile_aggregation_in_multi_profile() -> None:
    """Multi-profile breakdowns: aggregate ACROSS profiles in stats
    line ('140 schemas total: 70 in dbr, 70 in test-postgre'), don't
    pick one silently. Pin both the small-list bold-name format and
    the large-list aggregation directive."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(
        cfg,
        ["public"],
        scope_profiles=["alpha", "beta"],
    )
    # Don't pick a single profile when the question is open.
    assert "aggregate ACROSS every" in prompt or "aggregate across every" in prompt.lower()
    assert "Don't pick a single profile" in prompt or "single profile silently" in prompt
    # Small-list per-profile bold format still mentioned.
    assert "**bold**" in prompt or "**" in prompt


def test_prompt_directs_single_paragraph_for_short_answers() -> None:
    """The new rule isn't 'always use lists' — short answers (≤4
    items, single fact) still get a paragraph. Pin both branches so
    a future edit doesn't push everything into bullets."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    # The 'short answer' threshold is documented.
    assert "Short answer" in prompt or "short answer" in prompt


def test_prompt_unpinned_2level_profile_is_normal_not_blocker() -> None:
    """Regression guard: an unpinned 2-level profile (PostgreSQL etc.
    without a pinned `database`) is a fully-supported state — Studio's
    multi-profile browse already handles it. The prompt must NOT
    instruct the LLM to ask the user to /use-db <db> or /edit. The
    only mention of those phrases should be in the explicit
    'NEVER ask the user to …' negation."""
    from amx.config import DBConfig

    cfg = AMXConfig()
    cfg.db = DBConfig(backend="postgresql", host="pg.local", database="")
    prompt = _agent_system_prompt(cfg, [])
    # Every mention of the forbidden phrasing must be NEGATED — never
    # surface as a positive instruction.
    for phrase in ("switch via /use-db", "pin one with /edit"):
        for occurrence in _find_all(prompt, phrase):
            window = prompt[max(0, occurrence - 40) : occurrence]
            assert "NEVER" in window or "not " in window.lower(), (
                f"Phrase {phrase!r} appeared as a positive instruction: {window!r}"
            )
    # The new permissive phrasing must be present.
    assert "no database pinned" in prompt
    assert "with_counts=true" in prompt
    assert "list_databases" in prompt
    assert "blocker, not an answer" in prompt


def _find_all(haystack: str, needle: str) -> list[int]:
    out: list[int] = []
    start = 0
    while True:
        idx = haystack.find(needle, start)
        if idx == -1:
            return out
        out.append(idx)
        start = idx + 1


def test_prompt_anti_hallucination_profile_boundary_rule() -> None:
    """Each profile has its OWN pinned_database / pinned_catalog (or
    none). The Studio bug 'postgre (amx_test.public): 0 tables' came
    from the LLM applying dbr's catalog name to postgre's row. The
    prompt must explicitly forbid that name bleed."""
    cfg = AMXConfig()
    prompt = _agent_system_prompt(cfg, ["public"])
    # The rule's keyword anchor.
    assert "Profile-boundary discipline" in prompt
    assert "pinned_database" in prompt
    assert "pinned_catalog" in prompt
    # Concrete forbidden case (one profile's catalog applied to another).
    lower = prompt.lower()
    assert "never copy" in lower or "do not write" in lower or "never apply" in lower
