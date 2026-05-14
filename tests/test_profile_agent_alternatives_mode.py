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
    # Strengthened semantic body must say either "same factual content"
    # or "hold the factual content fixed" or "identical facts".
    assert (
        "same factual content" in lower
        or "factual content fixed" in lower
        or "identical facts" in lower
    ), "semantic directive must instruct meaning preservation"
    # Definition 1 forbids new attributes / nuances in semantic mode —
    # the strengthened body lists these in a DO NOT block.
    assert "do not" in lower, "semantic body should carry an explicit DO NOT list"
    assert (
        "any new concept" in lower or "introduce new concept" in lower or "no new concept" in lower
    ), "semantic body must forbid new concepts"
    assert "nuance" in lower or "attribute" in lower, (
        "semantic body must explicitly forbid new nuances / attributes"
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
    assert "share core vocabulary" in lower or "shared vocabulary" in lower or "carry over" in lower
    assert (
        "distinct candidate meanings" in lower
        or "shift" in lower
        or "different interpretation" in lower
    )
    # Definition 1 explicitly ALLOWS new nuances in lexical mode.
    assert (
        "introduce a new" in lower
        or "added nuances" in lower
        or ("allowed" in lower and "nuance" in lower)
        or "shifted nuance" in lower
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
    semantic_directive = mod._build_system_prompt(n_alternatives=3, alternatives_mode="semantic")
    lexical_directive = mod._build_system_prompt(n_alternatives=3, alternatives_mode="lexical")

    # Carve out just the directive body (everything from the
    # ALTERNATIVES DIVERSITY marker up to the EXAMPLES block) so we're
    # not fooled by the shared worked-example anchors which mention
    # both modes by design.
    def _directive_body(prompt: str) -> str:
        start = prompt.find("ALTERNATIVES DIVERSITY")
        end = prompt.find("EXAMPLES (anchor for both modes")
        if start == -1 or end == -1:
            return prompt
        return prompt[start:end].lower()

    sem_body = _directive_body(semantic_directive)
    lex_body = _directive_body(lexical_directive)

    # Semantic body must NOT instruct meaning divergence.
    assert "meaningfully different interpretation" not in sem_body
    assert "shifted meaning" not in sem_body
    # Lexical body must NOT instruct meaning preservation.
    assert "hold the factual content fixed" not in lex_body
    assert "identical facts from every" not in lex_body
