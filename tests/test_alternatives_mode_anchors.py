"""Pin the worked-example anchors in the alternatives_mode prompts.

Per Definition 1 (NLP standard), the directive prompt embeds concrete
exemplars so the LLM has runtime steering and so this test suite can
verify the right examples reach the model. The examples are the
ground-truth pairs from the user's Definition 1 spec:

  Source: "Unique identifier for a geographic location record."

  Semantic (paraphrases — same meaning, different wording):
    1. "Distinct numeric key assigned to every individual geographic
       location."
    2. "Primary identifier that distinguishes each geographic location
       entry."

  Lexical (shared vocabulary — meaning may shift):
    1. "Sequential numeric key assigned to each distinct geolocation
       entry."   (adds the new attribute 'sequential')
    2. "Internal reference number for a physical place or mapped
       point."   (reframes as internal reference + physical place)

If any of these fail, either the prompt has lost its anchors or someone
swapped the semantic / lexical bodies again. Do NOT relax the
assertions to make a re-inversion green — fix the prompt instead.
"""

from __future__ import annotations

import pytest

SOURCE_SENTENCE = "Unique identifier for a geographic location record."

SEMANTIC_EXAMPLES = (
    "Distinct numeric key assigned to every individual geographic location.",
    "Primary identifier that distinguishes each geographic location entry.",
)

LEXICAL_EXAMPLES = (
    "Sequential numeric key assigned to each distinct geolocation entry.",
    "Internal reference number for a physical place or mapped point.",
)


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
@pytest.mark.parametrize("mode", ["semantic", "lexical"])
def test_source_sentence_anchor_present(agent_module, mode):
    """The shared anchor block always contains the source sentence so
    both mode prompts share the same reference frame."""
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode=mode)
    assert SOURCE_SENTENCE in prompt


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_semantic_anchor_examples_present(agent_module):
    """Both semantic worked examples (paraphrases of the source) reach
    the LLM verbatim when the user requests semantic mode."""
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="semantic")
    for example in SEMANTIC_EXAMPLES:
        assert example in prompt, f"missing semantic anchor: {example!r}"


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_lexical_anchor_examples_present(agent_module):
    """Both lexical worked examples (shared-vocabulary, meaning-shifted)
    reach the LLM verbatim when the user requests lexical mode. The
    block lives in the shared anchors, so both modes see both example
    sets — that's intentional so the LLM also sees the contrast."""
    import importlib

    mod = importlib.import_module(agent_module)
    prompt = mod._build_system_prompt(n_alternatives=3, alternatives_mode="lexical")
    for example in LEXICAL_EXAMPLES:
        assert example in prompt, f"missing lexical anchor: {example!r}"


@pytest.mark.parametrize(
    "agent_module",
    ["amx.agents.profile_agent", "amx.agents.rag_agent", "amx.agents.code_agent"],
)
def test_anchor_block_omitted_when_n_is_1(agent_module):
    """At n_alternatives=1 there are no slots to seed, so the anchor
    block (and the directive) must be omitted entirely — otherwise the
    prompt carries dead instructions plus tokens the user is paying
    nothing for."""
    import importlib

    mod = importlib.import_module(agent_module)
    for mode in ("semantic", "lexical"):
        prompt = mod._build_system_prompt(n_alternatives=1, alternatives_mode=mode)
        assert SOURCE_SENTENCE not in prompt
        for example in SEMANTIC_EXAMPLES + LEXICAL_EXAMPLES:
            assert example not in prompt


def test_anchor_examples_match_definition_1():
    """Belt-and-braces: the in-test ground-truth examples themselves
    encode Definition 1. Semantic examples keep the 'unique identifier'
    meaning; lexical examples diverge into 'sequential' / 'internal
    reference' framings. This test is a self-check on the test fixtures
    — if it fails, the fixtures have drifted from Definition 1 and the
    other tests above are no longer trustworthy."""
    # Semantic: preserve the "unique identifier of a geographic record"
    # meaning, vary the wording.
    for example in SEMANTIC_EXAMPLES:
        lower = example.lower()
        assert "identifier" in lower or "key" in lower, (
            f"semantic example lost the identifier concept: {example!r}"
        )
        assert "geographic" in lower, f"semantic example lost the geographic concept: {example!r}"

    # Lexical: keep some core vocabulary (key / reference / geolocation
    # / place) but introduce a new conceptual nuance.
    assert "sequential" in LEXICAL_EXAMPLES[0].lower(), (
        "lexical example 1 should add the 'sequential' attribute "
        "(meaning shift while keeping vocabulary)"
    )
    assert (
        "internal reference" in LEXICAL_EXAMPLES[1].lower()
        or "physical place" in LEXICAL_EXAMPLES[1].lower()
    ), (
        "lexical example 2 should reframe as internal reference / "
        "physical place (meaning shift while keeping vocabulary)"
    )
