"""``run_results.alternatives_json`` accepts legacy list[str] and the new
structured list[dict] shape — both round-trip without data loss."""

from __future__ import annotations

import json


def _legacy_payload() -> str:
    return json.dumps(["alt one.", "alt two."])


def _structured_payload() -> str:
    return json.dumps(
        [
            {
                "text": "alt one.",
                "scores": {
                    "logprob": 0.82,
                    "self_consistency": 0.71,
                    "self_decl": None,
                    "judge": None,
                },
                "ensemble": 0.77,
                "band": "HIGH",
            },
            {
                "text": "alt two.",
                "scores": {
                    "logprob": 0.40,
                    "self_consistency": 0.30,
                    "self_decl": None,
                    "judge": None,
                },
                "ensemble": 0.0,
                "band": "LOW",
            },
        ]
    )


def test_parser_returns_strings_for_legacy_payload():
    from amx.storage.sqlite_store import parse_alternatives_json

    parsed = parse_alternatives_json(_legacy_payload())
    assert [a["text"] for a in parsed] == ["alt one.", "alt two."]
    assert all(a.get("band") in (None, "HIGH", "MED", "LOW") for a in parsed)


def test_parser_preserves_scores_for_structured_payload():
    from amx.storage.sqlite_store import parse_alternatives_json

    parsed = parse_alternatives_json(_structured_payload())
    assert [a["text"] for a in parsed] == ["alt one.", "alt two."]
    assert parsed[0]["band"] == "HIGH"
    assert parsed[1]["band"] == "LOW"
    assert parsed[0]["scores"]["logprob"] == 0.82


def test_parser_handles_empty_and_malformed_input_safely():
    from amx.storage.sqlite_store import parse_alternatives_json

    assert parse_alternatives_json("") == []
    assert parse_alternatives_json("not-json") == []
    assert parse_alternatives_json("[]") == []
    assert parse_alternatives_json(None) == []


def test_build_alternatives_json_falls_back_to_legacy_when_no_scores():
    """Suggestion without scores → JSON encodes a flat list[str]."""
    from types import SimpleNamespace

    from amx.storage.sqlite_store import build_alternatives_json

    s = SimpleNamespace(suggestions=["one.", "two."], suggestion_scores=None)
    raw = build_alternatives_json(s)
    assert json.loads(raw) == ["one.", "two."]


def test_build_alternatives_json_emits_structured_when_scores_present():
    from types import SimpleNamespace

    from amx.llm.confidence import AlternativeScore
    from amx.storage.sqlite_store import build_alternatives_json

    scores = [
        AlternativeScore(
            text="one.",
            logprob_score=0.8,
            self_consistency_score=0.7,
            self_decl_score=None,
            judge_score=None,
            ensemble_score=0.75,
            band="HIGH",
        ),
        AlternativeScore(
            text="two.",
            logprob_score=0.4,
            self_consistency_score=0.3,
            self_decl_score=None,
            judge_score=None,
            ensemble_score=0.0,
            band="LOW",
        ),
    ]
    s = SimpleNamespace(suggestions=["one.", "two."], suggestion_scores=scores)
    raw = build_alternatives_json(s)
    data = json.loads(raw)
    assert data[0]["band"] == "HIGH"
    assert data[1]["band"] == "LOW"
    assert data[0]["scores"]["logprob"] == 0.8
