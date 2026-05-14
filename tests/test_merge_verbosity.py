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

from amx.agents.orchestrator import (
    MERGE_FILLUP_PROMPT,
    MERGE_PROMPT,
    MERGE_SYSTEM_PROMPT,
    Orchestrator,
)
from amx.llm.prompts import length_rule


def _format_merge_prompt(*, columns_text: str, preset: str, n: int = 1) -> str:
    """Helper that fills every placeholder MERGE_PROMPT now requires.

    The 2026-05 fix added ``n_alternatives`` and ``description_lines``
    placeholders so the merge step natively asks for N ranked
    alternatives instead of a single best — tests need to mirror that
    contract.
    """
    description_lines = (
        "\n".join(
            f"DESCRIPTION_{i}: <alternative description — apply the SAME length rule as DESCRIPTION_1>"
            for i in range(2, n + 1)
        )
        if n > 1
        else ""
    )
    return MERGE_PROMPT.format(
        columns_text=columns_text,
        description_length_rule=length_rule(preset),
        n_alternatives=n,
        description_lines=description_lines,
        alternatives_mode_note="",
    )


def _format_fillup_prompt(*, columns_text: str, preset: str, n: int = 3) -> str:
    """Mirror of :func:`_format_merge_prompt` for the fill-up retry."""
    fillup_response_lines = "\n".join(
        f"DESCRIPTION_{i}: <alternative — apply the SAME length rule as DESCRIPTION_1, or — if none is supported>"
        for i in range(2, n + 1)
    )
    return MERGE_FILLUP_PROMPT.format(
        n_alternatives=n,
        description_length_rule=length_rule(preset),
        columns_text=columns_text,
        fillup_response_lines=fillup_response_lines,
        alternatives_mode_note="",
    )


def test_merge_prompt_drops_hardcoded_word_cap() -> None:
    """The merge prompt no longer carries a "≤ 25 words" instruction
    that would over-rule the verbosity preset's length rule."""
    formatted = _format_merge_prompt(
        columns_text="### col\n  [profile_agent] (confidence=HIGH): description\n    reasoning: r",
        preset="exhaustive",
        n=3,
    )
    assert "≤ 25 words" not in formatted
    assert "one tight sentence" not in formatted


def test_merge_prompt_injects_length_rule_for_each_preset() -> None:
    """Every verbosity preset's length rule reaches the merge prompt
    verbatim — the LLM sees the same expectation as the per-agent
    layer, not a generic "be brief" instruction."""
    payload = "### col\n  [profile_agent] (confidence=HIGH): d\n    reasoning: r"
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        formatted = _format_merge_prompt(columns_text=payload, preset=preset, n=3)
        # The first chunk of the length rule (e.g. "A COMPREHENSIVE
        # description") shows up inside the merge prompt body.
        rule = length_rule(preset)
        head = rule.split(".")[0]
        assert head in formatted, f"merge prompt missing '{head}' for preset={preset}"


def test_merge_prompt_lists_n_alternatives_slots() -> None:
    """Asking for ``n_alternatives=3`` must template ``DESCRIPTION_2``
    and ``DESCRIPTION_3`` slots into the response schema. Without this
    the merge LLM only emits ``DESCRIPTION_1`` and the user-visible
    alternative count collapses to whatever sub-agents happened to
    return — the bug behind the 2026-05-09 inconsistency report."""
    formatted = _format_merge_prompt(
        columns_text="### col\n  [profile_agent] (confidence=HIGH): d\n    reasoning: r",
        preset="brief",
        n=3,
    )
    assert "DESCRIPTION_1" in formatted
    assert "DESCRIPTION_2" in formatted
    assert "DESCRIPTION_3" in formatted
    # The schema label that used to live here is gone.
    assert "BEST_DESCRIPTION" not in formatted


def test_merge_prompt_reminds_length_rule_applies_to_alternatives() -> None:
    """Regression test for the user-reported bug where DESCRIPTION_1
    came back exhaustive but DESCRIPTION_2 / DESCRIPTION_3 collapsed
    to one-sentence briefs even at
    ``description_verbosity="exhaustive"``.

    Two pinned guarantees:
    1. ``MERGE_PROMPT`` body explicitly states that the length rule
       applies EQUALLY to every DESCRIPTION_N slot, not just to
       DESCRIPTION_1. The phrasing is checked by stable substrings so
       a future copy-edit can move text around without breaking the
       test, but a removal of the contract surfaces immediately.
    2. The slot template no longer reads bare ``<alternative>`` —
       every DESCRIPTION_<i> slot must carry the "apply the SAME
       length rule" rider that fixes the bug.

    The fill-up prompt carries the same contract.
    """
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        formatted = _format_merge_prompt(
            columns_text="### col\n  [profile_agent] (confidence=HIGH): d\n    reasoning: r",
            preset=preset,
            n=3,
        )
        assert "applies EQUALLY to DESCRIPTION_1" in formatted, (
            f"merge prompt missing equal-length contract at preset={preset}"
        )
        assert "DESCRIPTION_2: <alternative>" not in formatted, (
            f"bare <alternative> placeholder leaked at preset={preset}"
        )
        assert "DESCRIPTION_2: <alternative description — apply the SAME length rule" in formatted
        assert "DESCRIPTION_3: <alternative description — apply the SAME length rule" in formatted

    # Same contract on the fill-up retry path so a column that came
    # back short does not get refilled with one-sentence briefs.
    fillup = _format_fillup_prompt(
        columns_text="### col\nExisting:\n  - first description.\nStill to fill: DESCRIPTION_2, DESCRIPTION_3",
        preset="exhaustive",
        n=3,
    )
    assert "applies EQUALLY to every DESCRIPTION_N slot" in fillup
    assert "DESCRIPTION_2: <alternative or — if none is supported>" not in fillup
    assert "DESCRIPTION_2: <alternative — apply the SAME length rule" in fillup


def test_merge_system_prompt_warns_against_collapsing_long_form() -> None:
    """The system prompt now explicitly tells the merge LLM not to
    silently shorten an exhaustive/comprehensive answer."""
    assert "exhaustive" in MERGE_SYSTEM_PROMPT.lower()
    # The pre-fix system prompt nudged toward "verbose prose" being
    # bad — that contradicted the verbosity preset.
    assert "verbose prose" not in MERGE_SYSTEM_PROMPT.lower()


def test_parse_merge_response_keeps_multi_paragraph_first_description() -> None:
    """A multi-paragraph DESCRIPTION_1 (exhaustive preset) round-trips
    through the parser instead of being truncated to the first line."""
    merged_text = (
        "COLUMN: orders.id\n"
        "DESCRIPTION_1: Primary key for the orders table.\n"
        "It is a monotonically increasing integer assigned at insert time.\n"
        "\n"
        "Downstream pipelines join on this column to attribute revenue\n"
        "back to a single ordering event.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: profile + code agree.\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    assert "orders.id" in parsed
    descriptions, conf, reasoning = parsed["orders.id"]
    # All three paragraphs survive — first sentence + the
    # continuation lines + the empty-line paragraph break.
    description = descriptions[0]
    assert description.startswith("Primary key for the orders table.")
    assert "Downstream pipelines join on this column" in description
    assert "monotonically increasing integer" in description
    assert "\n" in description, "paragraph break dropped"
    # Reasoning stays single-line as before.
    assert reasoning == "profile + code agree."
    assert conf.value == "high"


def test_parse_merge_response_handles_multiple_columns() -> None:
    """Multi-column response: each column's DESCRIPTION_1 still
    parses independently after the multi-line fix."""
    merged_text = (
        "COLUMN: a\n"
        "DESCRIPTION_1: First description, line one.\n"
        "First description, line two.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: x\n"
        "COLUMN: b\n"
        "DESCRIPTION_1: Second description.\n"
        "CONFIDENCE: MEDIUM\n"
        "REASONING: y\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    assert set(parsed.keys()) == {"a", "b"}
    descs_a, _, _ = parsed["a"]
    descs_b, _, _ = parsed["b"]
    assert "First description, line one." in descs_a[0]
    assert "First description, line two." in descs_a[0]
    assert descs_b == ["Second description."]


def test_parse_merge_response_collects_n_alternatives_in_order() -> None:
    """``DESCRIPTION_1`` .. ``DESCRIPTION_N`` are returned as a ranked
    list in the parser output. Pins the contract the merge step now
    relies on for the per-run ``n_alternatives`` cap."""
    merged_text = (
        "COLUMN: status\n"
        "DESCRIPTION_1: Lifecycle stage of the order.\n"
        "DESCRIPTION_2: Operational state used by fulfilment workers.\n"
        "DESCRIPTION_3: Customer-visible label rendered in tracking emails.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: code + docs.\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    descriptions, _, _ = parsed["status"]
    assert descriptions == [
        "Lifecycle stage of the order.",
        "Operational state used by fulfilment workers.",
        "Customer-visible label rendered in tracking emails.",
    ]


def test_parse_merge_response_drops_em_dash_abstain() -> None:
    """A bare ``—`` on a DESCRIPTION line means "the LLM could not
    ground a distinct alternative" and must be filtered out so the
    user never sees an em-dash as a fake suggestion."""
    merged_text = (
        "COLUMN: x\n"
        "DESCRIPTION_1: Real description.\n"
        "DESCRIPTION_2: —\n"
        "DESCRIPTION_3: Another real description.\n"
        "CONFIDENCE: MEDIUM\n"
        "REASONING: r\n"
    )
    parsed = Orchestrator._parse_merge_response(merged_text)
    descs, _, _ = parsed["x"]
    assert descs == ["Real description.", "Another real description."]


def test_parse_merge_response_handles_legacy_best_description_label() -> None:
    """Older fixtures that use the legacy ``BEST_DESCRIPTION`` label
    still parse — the orchestrator alias treats it as DESCRIPTION_1
    so a future revert of the fixture format does not silently lose
    the description."""
    parsed = Orchestrator._parse_merge_response(
        "COLUMN: id\nBEST_DESCRIPTION: Order primary key.\nCONFIDENCE: HIGH\nREASONING: r\n"
    )
    descs, conf, reasoning = parsed["id"]
    assert descs == ["Order primary key."]
    assert reasoning == "r"
    assert conf.value == "high"
