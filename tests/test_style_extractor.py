import json

import pytest

from amx.llm.style.extractor import (
    MIN_SAMPLES,
    NoSamplesError,
    extract_style,
    sample_descriptions,
)


def test_sample_filters_empty_and_caps_at_30():
    raw = {f"col_{i}": (f"desc {i}" if i % 2 == 0 else None) for i in range(100)}
    picked = sample_descriptions(raw, cap=30)
    assert len(picked) == 30
    assert all(v for _, v in picked)


def test_sample_raises_when_under_minimum():
    raw = {"a": "x", "b": None, "c": None}
    with pytest.raises(NoSamplesError):
        sample_descriptions(raw, cap=30, min_samples=MIN_SAMPLES)


def test_extract_round_trips_through_llm_stub():
    raw = {
        "order_id": "Unique id of the order.",
        "created_at": "Creation timestamp of the order record.",
        "amount": "Sum of charged amount per transaction.",
    }
    stub_response = json.dumps({
        "language": "en-US",
        "tone": "formal",
        "avg_length_words": 6,
        "length_range": [4, 9],
        "person": "impersonal",
        "capitalization": "sentence-case",
        "ends_with_period": True,
        "structural_patterns": ["noun + role"],
        "vocabulary_register": "business",
        "redacted_examples": ["Unique id of the <ENTITY>."],
    })

    def fake_llm(system: str, user: str) -> str:
        # The extractor must NOT include cell data, only column metadata.
        assert "Unique id of the order." in user  # comments are OK
        return stub_response

    profile, n = extract_style(raw, llm_call=fake_llm)
    assert n == 3
    assert profile.language == "en-US"
    assert profile.redacted_examples == ["Unique id of the <ENTITY>."]


def test_extract_retries_on_invalid_json_then_fails():
    raw = {"a": "x", "b": "y", "c": "z"}
    calls = {"n": 0}

    def flaky_llm(system: str, user: str) -> str:
        calls["n"] += 1
        return "not json"

    with pytest.raises(ValueError):
        extract_style(raw, llm_call=flaky_llm)
    assert calls["n"] == 2  # one initial + one retry
