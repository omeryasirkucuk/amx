"""Smoke tests for the Phase 1 confidence package."""

from __future__ import annotations


def test_alternative_score_can_be_imported():
    from amx.llm.confidence import AlternativeScore

    score = AlternativeScore(
        text="x",
        logprob_score=None,
        self_consistency_score=None,
        self_decl_score=None,
        judge_score=None,
        ensemble_score=0.5,
        band="MED",
    )
    assert score.to_json() == {
        "text": "x",
        "scores": {
            "logprob": None,
            "self_consistency": None,
            "self_decl": None,
            "judge": None,
        },
        "ensemble": 0.5,
        "band": "MED",
    }
