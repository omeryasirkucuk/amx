from amx.llm.style.injector import render_style_section
from amx.llm.style.profile import StyleProfile


def _profile() -> StyleProfile:
    return StyleProfile(
        language="en-US",
        tone="formal, third-person",
        avg_length_words=14,
        length_range=(8, 22),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["Definition + purpose"],
        vocabulary_register="business-technical",
        redacted_examples=[
            "Unique identifier of the <ENTITY>.",
            "Creation timestamp of the <ENTITY> record.",
        ],
    )


def test_render_includes_required_fields_and_guard_text():
    out = render_style_section(_profile())
    assert "## Writing style" in out
    assert "en-US" in out
    assert "8" in out and "22" in out
    assert "formal, third-person" in out
    assert "<ENTITY>" in out
    assert "Never copy these placeholders" in out


def test_render_none_returns_empty_string():
    assert render_style_section(None) == ""


def test_profile_agent_appends_style_section():
    from amx.agents import profile_agent
    from amx.llm.style.profile import StyleProfile

    sp = StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=["Unique id of the <ENTITY>."],
    )
    out_with = profile_agent._build_system_prompt(
        n_alternatives=1,
        description_verbosity="brief",
        style_profile=sp,
    )
    out_without = profile_agent._build_system_prompt(
        n_alternatives=1,
        description_verbosity="brief",
    )
    assert "## Writing style" in out_with
    assert "## Writing style" not in out_without


def test_rag_agent_appends_style_section():
    from amx.agents import rag_agent
    from amx.llm.style.profile import StyleProfile

    sp = StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=["Unique id of the <ENTITY>."],
    )
    out = rag_agent._build_system_prompt(
        n_alternatives=1, description_verbosity="brief", style_profile=sp,
    )
    assert "## Writing style" in out


def test_code_agent_appends_style_section():
    from amx.agents import code_agent
    from amx.llm.style.profile import StyleProfile

    sp = StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=["Unique id of the <ENTITY>."],
    )
    out = code_agent._build_system_prompt(
        n_alternatives=1, description_verbosity="brief", style_profile=sp,
    )
    assert "## Writing style" in out


def test_generate_router_build_system_prompt_appends_style_section():
    from amx.web.routers import generate
    from amx.llm.style.profile import StyleProfile

    sp = StyleProfile(
        language="en-US", tone="x", avg_length_words=1, length_range=(1, 1),
        person="x", capitalization="x", ends_with_period=True,
        structural_patterns=[], vocabulary_register="x",
        redacted_examples=["Unique id of the <ENTITY>."],
    )
    out = generate._build_system_prompt(n=1, verbosity="brief", style_profile=sp)
    assert "## Writing style" in out
