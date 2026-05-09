"""ProfileAgent honours ``description_verbosity`` end-to-end.

Regression test for a user-reported bug where suggestions came back
in a brief one-sentence form even after the user picked
``description_verbosity = "exhaustive"`` in /runs/new. Two things
were broken in the ProfileAgent prompt:

1. ``_BASE_SYSTEM_PROMPT`` carried a hardcoded ``Be concise — aim
   for ≤ 25 words per description (the verbosity preset may relax
   this).`` rule directly under "Output rules". The parenthetical
   was the only signal that a verbose preset overrode it, and
   smaller / faster models (deepseek-v4-flash in the user's report)
   defaulted to the more authoritative-sounding ``≤ 25 words`` line
   regardless of which preset reached the prompt further down.
2. The downstream ``description_length_rule`` placeholder existed
   but the LLM never saw it as the *only* length instruction; the
   conflicting "≤ 25 words" line was always above it.

Fix: the "≤ 25 words" line is replaced by a single
``Length rule (CRITICAL — honour the user's verbosity preset):
{description_length_rule}`` line so the LLM sees one consistent
instruction at every preset.

These tests pin the fix so a future refactor of the ProfileAgent
prompt cannot silently revive the bug.
"""

from __future__ import annotations

from amx.agents.profile_agent import _build_system_prompt
from amx.llm.prompts import length_rule


def test_profile_agent_prompt_drops_hardcoded_word_cap() -> None:
    """No verbosity preset must let the legacy "≤ 25 words" rule
    leak into the system prompt — the rule overrode every preset
    the user could pick on /runs/new."""
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        prompt = _build_system_prompt(3, description_verbosity=preset)
        assert "≤ 25 words" not in prompt, f"hardcoded cap leaked at preset={preset}"
        assert (
            "verbosity preset may relax this" not in prompt
        ), f"legacy parenthetical leaked at preset={preset}"


def test_profile_agent_prompt_injects_length_rule_for_each_preset() -> None:
    """Every preset's length rule reaches the system prompt verbatim,
    in the prominent "Output rules" section — the same expectation
    that ProfileAgent inherits from
    :func:`amx.llm.prompts.length_rule`."""
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        prompt = _build_system_prompt(3, description_verbosity=preset)
        rule = length_rule(preset)
        head = rule.split(".")[0]
        assert head in prompt, f"prompt missing '{head}' for preset={preset}"


def test_profile_agent_prompt_emits_n_alternative_slots() -> None:
    """``n_alternatives=3`` templates ``DESCRIPTION_2`` /
    ``DESCRIPTION_3`` slots so the LLM has explicit slots to fill;
    without these, a smaller model often emits only DESCRIPTION_1
    and the merge step ends up dedup-collapsing to one alternative
    per column."""
    prompt = _build_system_prompt(3, description_verbosity="exhaustive")
    assert "DESCRIPTION_1:" in prompt
    assert "DESCRIPTION_2:" in prompt
    assert "DESCRIPTION_3:" in prompt
    # ``TABLE_DESCRIPTION_2/3`` slots also templated for the table-
    # level block ProfileAgent always emits alongside columns.
    assert "TABLE_DESCRIPTION_2:" in prompt
    assert "TABLE_DESCRIPTION_3:" in prompt


def test_profile_agent_prompt_n_alternatives_one_omits_alt_slots() -> None:
    """``n_alternatives=1`` must NOT emit DESCRIPTION_2 — the merge
    step's cap respects the same value, so a stray ``DESCRIPTION_2``
    template would prompt the LLM to invent an alternative the user
    did not ask for."""
    prompt = _build_system_prompt(1, description_verbosity="brief")
    assert "DESCRIPTION_1:" in prompt
    assert "DESCRIPTION_2:" not in prompt
