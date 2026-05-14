"""Verify alternatives_mode directive injection in the per-agent system prompts."""

from __future__ import annotations

import pytest


def _semantic_marker() -> str:
    return "ALTERNATIVES DIVERSITY (semantic mode)"


def _lexical_marker() -> str:
    return "ALTERNATIVES DIVERSITY (lexical mode)"


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_semantic_directive_present_when_n_gt_1(agent_module):
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="semantic")
    assert _semantic_marker() in prompt
    assert _lexical_marker() not in prompt


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_lexical_directive_present_when_n_gt_1(agent_module):
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="lexical")
    assert _lexical_marker() in prompt
    assert _semantic_marker() not in prompt


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_no_directive_when_n_equals_1(agent_module):
    """When the user disables alternates entirely, the directive must
    not bleed into the prompt — there is nothing to differentiate."""
    import importlib

    mod = importlib.import_module(agent_module)
    for mode in ("semantic", "lexical"):
        prompt = mod._build_system_prompt(n_alternatives=1, alternatives_mode=mode)
        assert _semantic_marker() not in prompt
        assert _lexical_marker() not in prompt


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_default_mode_is_semantic(agent_module):
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3)
    assert _semantic_marker() in prompt


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_unknown_mode_falls_back_to_semantic_directive(agent_module):
    """A garbled mode value (programming error, hand-edited YAML) must
    still produce a valid prompt rather than an empty directive line."""
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="bogus")
    assert _semantic_marker() in prompt
