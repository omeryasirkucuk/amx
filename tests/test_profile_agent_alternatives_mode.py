"""Verify alternatives_mode directive injection in the per-agent system prompts."""

from __future__ import annotations

import pytest


def _semantic_marker() -> str:
    # Tolerant prefix: the directive header may carry a qualifier in
    # parentheses (e.g. "(semantic mode — paraphrase only)") and the
    # test only needs to discriminate between the two modes' presence.
    return "ALTERNATIVES DIVERSITY (semantic mode"


def _lexical_marker() -> str:
    return "ALTERNATIVES DIVERSITY (lexical mode"


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


# ── Definition 1 body contract ─────────────────────────────────────────
#
# Pins the actual instruction wording so the prompts cannot silently
# revert to the inverted definitions shipped in PR #441. Marker tests
# above only check the ``(semantic mode)`` / ``(lexical mode)`` labels;
# these tests check that each mode TELLS the LLM the right behaviour.


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_semantic_directive_body_instructs_paraphrasing(agent_module):
    """Semantic mode under Definition 1: same meaning, different wording.

    The directive MUST tell the LLM to paraphrase and MUST forbid
    introducing new attributes / nuances.
    """
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="semantic")
    lower = prompt.lower()
    assert "paraphrase" in lower, "semantic directive must instruct paraphrasing"
    assert "same factual content" in lower or "same meaning" in lower
    # Definition 1 forbids new attributes / nuances in semantic mode.
    assert (
        "must not introduce new" in lower
        or "no new concepts" in lower
        or ("must not introduce" in lower and "concepts" in lower)
    )


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_lexical_directive_body_instructs_shared_vocabulary(agent_module):
    """Lexical mode under Definition 1: shared vocabulary, meaning may
    shift. The directive MUST tell the LLM to keep core tokens and MUST
    allow new nuances / shifted framings.
    """
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="lexical")
    lower = prompt.lower()
    assert "share core vocabulary" in lower or "shared vocabulary" in lower
    assert "distinct candidate meanings" in lower or "shift" in lower
    # Definition 1 explicitly ALLOWS new nuances in lexical mode.
    assert (
        "add new conceptual nuances" in lower
        or "added nuances" in lower
        or ("allowed to add" in lower and "nuance" in lower)
    )


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_directives_are_not_inverted(agent_module):
    """Defensive: semantic prompt MUST NOT contain lexical-only contract
    phrasing (and vice versa). Catches accidental re-inversion."""
    import importlib

    mod = importlib.import_module(agent_module)
    semantic = mod._build_system_prompt(n_alternatives=3, alternatives_mode="semantic").lower()
    lexical = mod._build_system_prompt(n_alternatives=3, alternatives_mode="lexical").lower()
    # Semantic must not tell the LLM to invent new interpretations.
    assert "meaningfully different interpretation" not in semantic
    # Lexical must not tell the LLM to hold meaning constant.
    assert "must not introduce new" not in lexical
