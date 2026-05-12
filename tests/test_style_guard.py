from amx.llm.style.guard import contains_leakage
from amx.llm.style.profile import StyleProfile


def _p(examples):
    return StyleProfile(
        language="en-US",
        tone="x",
        avg_length_words=1,
        length_range=(1, 1),
        person="x",
        capitalization="x",
        ends_with_period=True,
        structural_patterns=[],
        vocabulary_register="x",
        redacted_examples=examples,
    )


def test_flags_placeholder_literal():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("This row holds the <ENTITY> name.", p) is True


def test_flags_exact_example_match():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("Unique id of the <ENTITY>.", p) is True


def test_clean_output_passes():
    p = _p(["Unique id of the <ENTITY>."])
    assert contains_leakage("Order identifier issued at checkout.", p) is False
