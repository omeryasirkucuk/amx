"""Verify the merge-prompt template carries the alternatives_mode directive."""

from __future__ import annotations

from amx.agents._prompt_helpers import alternatives_mode_merge_note
from amx.agents.orchestrator import MERGE_FILLUP_PROMPT, MERGE_PROMPT


def _format_merge(mode: str, n: int) -> str:
    return MERGE_PROMPT.format(
        columns_text="### col_x\n  [profile] (confidence=HIGH): foo",
        description_length_rule="One or two short sentences.",
        n_alternatives=n,
        description_lines="DESCRIPTION_2: <alt>" if n > 1 else "",
        alternatives_mode_note=alternatives_mode_merge_note(mode, n),
    )


def _format_fillup(mode: str, n: int) -> str:
    return MERGE_FILLUP_PROMPT.format(
        n_alternatives=n,
        description_length_rule="One or two short sentences.",
        columns_text="### col_x\nExisting:\n  DESCRIPTION_1: foo\nStill to fill: DESCRIPTION_2",
        fillup_response_lines="DESCRIPTION_2: <alt>",
        alternatives_mode_note=alternatives_mode_merge_note(mode, n),
    )


def test_merge_prompt_carries_semantic_note():
    prompt = _format_merge("semantic", 3)
    assert "ALTERNATIVES DIVERSITY (semantic mode)" in prompt
    assert "ALTERNATIVES DIVERSITY (lexical mode)" not in prompt


def test_merge_prompt_carries_lexical_note():
    prompt = _format_merge("lexical", 3)
    assert "ALTERNATIVES DIVERSITY (lexical mode)" in prompt
    assert "ALTERNATIVES DIVERSITY (semantic mode)" not in prompt


def test_merge_prompt_drops_note_when_n_is_1():
    """When the user disables alternates, the merge prompt should not
    drag along a directive for slots that won't exist."""
    for mode in ("semantic", "lexical"):
        prompt = _format_merge(mode, 1)
        assert "ALTERNATIVES DIVERSITY" not in prompt


def test_fillup_prompt_carries_semantic_note():
    prompt = _format_fillup("semantic", 3)
    assert "ALTERNATIVES DIVERSITY (semantic mode)" in prompt


def test_fillup_prompt_carries_lexical_note():
    prompt = _format_fillup("lexical", 3)
    assert "ALTERNATIVES DIVERSITY (lexical mode)" in prompt


def test_merge_prompt_no_hardcoded_semantic_phrasing():
    """The old prompt baked semantic philosophy ('different
    interpretation') into the static template, which is wrong for
    lexical mode. The static text must now be neutral and defer to the
    mode-specific note."""
    lexical_prompt = _format_merge("lexical", 3)
    # The mode-specific note carries the philosophy; the static template
    # body must not claim alternates require a different *interpretation*.
    static_body = lexical_prompt.split("ALTERNATIVES DIVERSITY")[0]
    forbidden = "must offer a meaningfully different interpretation"
    assert forbidden not in static_body
