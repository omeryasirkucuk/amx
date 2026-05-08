"""Merge step honours the user's ``description_verbosity`` preset.

Regression test for a user-reported bug: picking
``description_verbosity = "exhaustive"`` (or comprehensive / detailed)
in the LLM profile or the per-run override panel shipped a verbose
prompt to ProfileAgent / RAGAgent / CodeAgent, but the orchestrator's
``_merge_suggestions`` step then summarised every multi-source
column back down to one short sentence — burying the long form the
user explicitly asked for.

Two contributing problems:

1. ``MERGE_PROMPT`` carried a hardcoded "Aim for one tight sentence
   (≤ 25 words)" rule and made an unsupported reference to "the
   user's verbosity preset" without ever telling the LLM what that
   preset actually was.
2. ``_parse_merge_response`` only kept the first line of the
   ``BEST_DESCRIPTION`` field, so multi-paragraph "exhaustive"
   answers returned by the LLM were truncated to the first sentence
   on the way back into the suggestion list.

These tests pin both fixes so a future refactor of the merge prompt
or parser can't silently revive the bug.
"""

from __future__ import annotations

from amx.agents.orchestrator import MERGE_PROMPT, MERGE_SYSTEM_PROMPT, Orchestrator
from amx.llm.prompts import length_rule


def test_merge_prompt_drops_hardcoded_word_cap() -> None:
    """The merge prompt no longer carries a "≤ 25 words" instruction
    that would over-rule the verbosity preset's length rule."""
    formatted = MERGE_PROMPT.format(
        columns_text="### col\n  [profile_agent] (confidence=HIGH): description\n    reasoning: r",
        description_length_rule=length_rule("exhaustive"),
    )
    assert "≤ 25 words" not in formatted
    assert "one tight sentence" not in formatted


def test_merge_prompt_injects_length_rule_for_each_preset() -> None:
    """Every verbosity preset's length rule reaches the merge prompt
    verbatim — the LLM sees the same expectation as the per-agent
    layer, not a generic "be brief" instruction."""
    payload = "### col\n  [profile_agent] (confidence=HIGH): d\n    reasoning: r"
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        formatted = MERGE_PROMPT.format(
            columns_text=payload,
            description_length_rule=length_rule(preset),
        )
        # The first chunk of the length rule (e.g. "A COMPREHENSIVE
        # description") shows up inside the merge prompt body.
        rule = length_rule(preset)
        head = rule.split(".")[0]
        assert head in formatted, f"merge prompt missing '{head}' for preset={preset}"


def test_merge_system_prompt_warns_against_collapsing_long_form() -> None:
    """The system prompt now explicitly tells the merge LLM not to
    silently shorten an exhaustive/comprehensive answer."""
    assert "exhaustive" in MERGE_SYSTEM_PROMPT.lower()
    # The pre-fix system prompt nudged toward "verbose prose" being
    # bad — that contradicted the verbosity preset.
    assert "verbose prose" not in MERGE_SYSTEM_PROMPT.lower()


def test_parse_merge_response_keeps_multi_paragraph_best_description() -> None:
    """A multi-paragraph BEST_DESCRIPTION (exhaustive preset) round-trips
    through the parser instead of being truncated to the first line."""
    merged_text = (
        "COLUMN: orders.id\n"
        "BEST_DESCRIPTION: Primary key for the orders table.\n"
        "It is a monotonically increasing integer assigned at insert time.\n"
        "\n"
        "Downstream pipelines join on this column to attribute revenue\n"
        "back to a single ordering event.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: profile + code agree.\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    assert "orders.id" in parsed
    description, conf, reasoning = parsed["orders.id"]
    # All three paragraphs survive — first sentence + the
    # continuation lines + the empty-line paragraph break.
    assert description.startswith("Primary key for the orders table.")
    assert "Downstream pipelines join on this column" in description
    assert "monotonically increasing integer" in description
    assert "\n" in description, "paragraph break dropped"
    # Reasoning stays single-line as before.
    assert reasoning == "profile + code agree."
    assert conf.value == "high"


def test_parse_merge_response_handles_multiple_columns() -> None:
    """Multi-column response: each column's BEST_DESCRIPTION still
    parses independently after the multi-line fix."""
    merged_text = (
        "COLUMN: a\n"
        "BEST_DESCRIPTION: First description, line one.\n"
        "First description, line two.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: x\n"
        "COLUMN: b\n"
        "BEST_DESCRIPTION: Second description.\n"
        "CONFIDENCE: MEDIUM\n"
        "REASONING: y\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    assert set(parsed.keys()) == {"a", "b"}
    desc_a, _, _ = parsed["a"]
    desc_b, _, _ = parsed["b"]
    assert "First description, line one." in desc_a
    assert "First description, line two." in desc_a
    assert desc_b == "Second description."


def test_parse_merge_response_handles_single_line_legacy() -> None:
    """Existing brief-mode single-line responses must still parse —
    the fix added multi-line support without breaking the simple
    case ProfileAgent/RAGAgent already produce."""
    parsed = Orchestrator._parse_merge_response(
        "COLUMN: id\nBEST_DESCRIPTION: Order primary key.\nCONFIDENCE: HIGH\nREASONING: r\n"
    )
    desc, conf, reasoning = parsed["id"]
    assert desc == "Order primary key."
    assert reasoning == "r"
    assert conf.value == "high"
