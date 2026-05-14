"""``run_results.alternatives_json`` accepts three shapes:

1. Pure-legacy ``list[str]`` from pre-confidence rows.
2. Old ensemble structured shape (Phase 1+2 rows with ``scores`` /
   ``ensemble`` fields).
3. Current single-signal shape (``signal`` / ``score`` / ``band`` per
   alternative).

All three round-trip into the same normalised dict so the API and UI do
not need to branch on shape at read time.
"""

from __future__ import annotations

import json
from types import SimpleNamespace


def _legacy_flat_payload() -> str:
    return json.dumps(["alt one.", "alt two."])


def _old_ensemble_payload() -> str:
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


def _current_single_signal_payload() -> str:
    return json.dumps(
        [
            {"text": "alt one.", "signal": "self_consistency", "score": 0.71, "band": "MED"},
            {"text": "alt two.", "signal": "self_consistency", "score": 0.92, "band": "HIGH"},
        ]
    )


def test_parser_returns_text_and_nulls_for_legacy_flat_list():
    from amx.storage.sqlite_store import parse_alternatives_json

    parsed = parse_alternatives_json(_legacy_flat_payload())
    assert [a["text"] for a in parsed] == ["alt one.", "alt two."]
    assert all(a["signal"] is None for a in parsed)
    assert all(a["score"] is None for a in parsed)
    assert all(a["band"] is None for a in parsed)


def test_parser_preserves_band_from_old_ensemble_shape():
    """Old ensemble rows lose their per-signal numbers but keep the
    visible band label so Studio still renders a pill on them."""
    from amx.storage.sqlite_store import parse_alternatives_json

    parsed = parse_alternatives_json(_old_ensemble_payload())
    assert [a["text"] for a in parsed] == ["alt one.", "alt two."]
    assert [a["band"] for a in parsed] == ["HIGH", "LOW"]
    assert all(a["signal"] is None for a in parsed)
    assert all(a["score"] is None for a in parsed)


def test_parser_passes_through_current_single_signal_shape():
    from amx.storage.sqlite_store import parse_alternatives_json

    parsed = parse_alternatives_json(_current_single_signal_payload())
    assert [a["text"] for a in parsed] == ["alt one.", "alt two."]
    assert [a["signal"] for a in parsed] == ["self_consistency", "self_consistency"]
    assert [a["score"] for a in parsed] == [0.71, 0.92]
    assert [a["band"] for a in parsed] == ["MED", "HIGH"]


def test_parser_handles_empty_and_malformed_input_safely():
    from amx.storage.sqlite_store import parse_alternatives_json

    assert parse_alternatives_json("") == []
    assert parse_alternatives_json("not-json") == []
    assert parse_alternatives_json("[]") == []
    assert parse_alternatives_json(None) == []


def test_build_alternatives_json_falls_back_to_legacy_when_no_scores():
    """Suggestion without scores → JSON encodes a flat list[str] so
    legacy readers (CLI fixtures, older Studio bundles) keep working."""
    from amx.storage.sqlite_store import build_alternatives_json

    s = SimpleNamespace(suggestions=["one.", "two."], suggestion_scores=None)
    raw = build_alternatives_json(s)
    assert json.loads(raw) == ["one.", "two."]


def test_build_alternatives_json_emits_current_single_signal_shape():
    from amx.llm.confidence import AlternativeScore
    from amx.storage.sqlite_store import build_alternatives_json

    scores = [
        AlternativeScore(text="one.", signal="logprob", score=0.82, band="HIGH"),
        AlternativeScore(text="two.", signal="logprob", score=0.40, band="LOW"),
    ]
    s = SimpleNamespace(suggestions=["one.", "two."], suggestion_scores=scores)
    raw = build_alternatives_json(s)
    data = json.loads(raw)
    assert data == [
        {"text": "one.", "signal": "logprob", "score": 0.82, "band": "HIGH"},
        {"text": "two.", "signal": "logprob", "score": 0.40, "band": "LOW"},
    ]


def test_build_alternatives_json_dict_shape_from_orchestrator():
    """The orchestrator passes a dict with ``alternatives`` +
    ``alternative_scores`` (list of AlternativeScore-shaped dicts)
    rather than a dataclass instance. Same JSON shape must come out."""
    from amx.storage.sqlite_store import build_alternatives_json

    raw = build_alternatives_json(
        {
            "alternatives": ["one.", "two."],
            "alternative_scores": [
                {"text": "one.", "signal": "self_decl", "score": 0.9, "band": "HIGH"},
                {"text": "two.", "signal": "self_decl", "score": 0.3, "band": "LOW"},
            ],
        }
    )
    data = json.loads(raw)
    assert data == [
        {"text": "one.", "signal": "self_decl", "score": 0.9, "band": "HIGH"},
        {"text": "two.", "signal": "self_decl", "score": 0.3, "band": "LOW"},
    ]
