from amx.llm.style.profile import StyleProfile, PLACEHOLDERS


def test_round_trip_serialization():
    sp = StyleProfile(
        language="en-US",
        tone="formal, third-person",
        avg_length_words=14,
        length_range=(8, 22),
        person="impersonal",
        capitalization="sentence-case",
        ends_with_period=True,
        structural_patterns=["Definition + purpose"],
        vocabulary_register="business-technical",
        redacted_examples=["Unique identifier of the <ENTITY>."],
    )
    s = sp.to_json()
    sp2 = StyleProfile.from_json(s)
    assert sp2 == sp


def test_from_json_rejects_unknown_placeholder_in_examples():
    bad = (
        '{"language":"en","tone":"x","avg_length_words":1,'
        '"length_range":[1,1],"person":"x","capitalization":"x",'
        '"ends_with_period":true,"structural_patterns":[],'
        '"vocabulary_register":"x",'
        '"redacted_examples":["use <FOO> here"]}'
    )
    try:
        StyleProfile.from_json(bad)
    except ValueError as e:
        assert "FOO" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_placeholders_constant_is_frozen_set():
    assert PLACEHOLDERS == frozenset(
        {"<ENTITY>", "<METRIC>", "<DATE_FIELD>", "<STATUS>", "<IDENTIFIER>"}
    )
