"""Prompt template emits ``CONFIDENCE_i`` only when ``emit_self_decl`` is on."""

from __future__ import annotations


def test_default_prompt_has_no_per_alternative_confidence_lines():
    from amx.agents.profile_agent import _build_system_prompt

    prompt = _build_system_prompt(n_alternatives=3, emit_self_decl=False)
    assert "CONFIDENCE_1" not in prompt
    assert "CONFIDENCE_2" not in prompt
    assert "CONFIDENCE_3" not in prompt
    # Aggregate ``CONFIDENCE:`` line stays — legacy parser depends on it.
    assert "CONFIDENCE: <HIGH|MEDIUM|LOW>" in prompt


def test_self_decl_prompt_emits_per_alternative_confidence_lines():
    from amx.agents.profile_agent import _build_system_prompt

    prompt = _build_system_prompt(n_alternatives=3, emit_self_decl=True)
    assert "CONFIDENCE_1: <HIGH|MED|LOW>" in prompt
    assert "CONFIDENCE_2: <HIGH|MED|LOW>" in prompt
    assert "CONFIDENCE_3: <HIGH|MED|LOW>" in prompt
    # Aggregate line still present.
    assert "CONFIDENCE: <HIGH|MEDIUM|LOW>" in prompt


def test_self_decl_prompt_with_n1_only_emits_confidence_1():
    from amx.agents.profile_agent import _build_system_prompt

    prompt = _build_system_prompt(n_alternatives=1, emit_self_decl=True)
    assert "CONFIDENCE_1: <HIGH|MED|LOW>" in prompt
    assert "CONFIDENCE_2" not in prompt
