"""CodeAgent honours ``description_verbosity`` for every alternative slot.

Regression test for the user-reported bug where DESCRIPTION_1 came
back exhaustive (multi-paragraph) but DESCRIPTION_2 / DESCRIPTION_3
collapsed to one-sentence briefs even at
``description_verbosity="exhaustive"``. The bare ``<alternative>``
placeholder in the response-format template told the LLM the
alternative slots were short rephrasings rather than full-length
distinct interpretations.

Mirror of :mod:`tests.test_profile_agent_verbosity` for the CodeAgent
branch so a future refactor of any one of the three sub-agents cannot
silently revive the bug.
"""

from __future__ import annotations

from amx.agents.code_agent import _build_system_prompt
from amx.llm.prompts import ALTERNATIVES_LENGTH_RULE_REMINDER, length_rule


def test_code_agent_prompt_injects_length_rule_for_each_preset() -> None:
    """Every preset's length rule reaches the system prompt verbatim."""
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        prompt = _build_system_prompt(3, description_verbosity=preset)
        rule = length_rule(preset)
        head = rule.split(".")[0]
        assert head in prompt, f"prompt missing '{head}' for preset={preset}"


def test_code_agent_prompt_emits_n_alternative_slots() -> None:
    """``n_alternatives=3`` templates DESCRIPTION_2 / DESCRIPTION_3 slots."""
    prompt = _build_system_prompt(3, description_verbosity="exhaustive")
    assert "DESCRIPTION_1:" in prompt
    assert "DESCRIPTION_2:" in prompt
    assert "DESCRIPTION_3:" in prompt


def test_code_agent_prompt_alternatives_carry_length_rule() -> None:
    """Alternative slots carry the "same length rule" rider and the
    prompt body includes the shared length-rule reminder."""
    for preset in ("brief", "detailed", "comprehensive", "exhaustive"):
        prompt = _build_system_prompt(3, description_verbosity=preset)
        assert ALTERNATIVES_LENGTH_RULE_REMINDER in prompt, (
            f"alternatives reminder missing at preset={preset}"
        )
        assert "DESCRIPTION_2: <alternative>" not in prompt, (
            f"bare <alternative> placeholder leaked at preset={preset}"
        )
        assert "DESCRIPTION_2: <alternative description — apply the SAME length rule" in prompt
        assert "DESCRIPTION_3: <alternative description — apply the SAME length rule" in prompt


def test_code_agent_prompt_n_alternatives_one_omits_alt_slots() -> None:
    """No DESCRIPTION_2 slot or reminder when only one alternative is
    requested — keeps the brief-mode prompt as small as before."""
    prompt = _build_system_prompt(1, description_verbosity="brief")
    assert "DESCRIPTION_1:" in prompt
    assert "DESCRIPTION_2:" not in prompt
    assert ALTERNATIVES_LENGTH_RULE_REMINDER not in prompt
