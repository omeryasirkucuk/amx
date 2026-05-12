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
