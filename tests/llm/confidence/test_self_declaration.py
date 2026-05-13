"""Signal B — LLM self-declaration parser + scorer."""

from __future__ import annotations


def test_returns_all_none_when_response_text_missing():
    from amx.llm.confidence.self_declaration import score_per_alternative

    assert score_per_alternative(None, n=3) == [None, None, None]
    assert score_per_alternative("", n=2) == [None, None]


def test_parses_high_med_low_per_alternative():
    """Each ``CONFIDENCE_i: HIGH|MED|LOW`` maps to its numeric band."""
    from amx.llm.confidence.self_declaration import score_per_alternative

    response = (
        "COLUMN: email\n"
        "DESCRIPTION_1: Stores the customer email.\n"
        "CONFIDENCE_1: HIGH\n"
        "DESCRIPTION_2: Holds a contact value.\n"
        "CONFIDENCE_2: MED\n"
        "DESCRIPTION_3: Unknown.\n"
        "CONFIDENCE_3: LOW\n"
        "CONFIDENCE: HIGH\n"
    )
    scores = score_per_alternative(response, n=3)
    assert scores == [0.9, 0.6, 0.3]


def test_medium_alias_for_med():
    """Some prompts emit the long form ``MEDIUM`` — accept both spellings."""
    from amx.llm.confidence.self_declaration import score_per_alternative

    response = "CONFIDENCE_1: MEDIUM\n"
    scores = score_per_alternative(response, n=1)
    assert scores == [0.6]


def test_missing_band_yields_none_for_that_index():
    from amx.llm.confidence.self_declaration import score_per_alternative

    response = "CONFIDENCE_1: HIGH\nCONFIDENCE_3: LOW\n"
    scores = score_per_alternative(response, n=3)
    assert scores == [0.9, None, 0.3]


def test_unrecognised_value_is_none():
    from amx.llm.confidence.self_declaration import score_per_alternative

    response = "CONFIDENCE_1: SUPER_HIGH\n"
    scores = score_per_alternative(response, n=1)
    assert scores == [None]


def test_case_insensitive_band_values():
    from amx.llm.confidence.self_declaration import score_per_alternative

    response = "CONFIDENCE_1: high\nCONFIDENCE_2: low\n"
    scores = score_per_alternative(response, n=2)
    assert scores == [0.9, 0.3]
