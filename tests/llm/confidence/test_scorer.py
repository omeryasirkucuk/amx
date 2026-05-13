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


def test_score_alternatives_phase1_signals_only():
    """Orchestrator returns one AlternativeScore per alternative with
    Phase 2/3 fields left as None."""
    from amx.config import ConfidenceConfig
    from amx.llm.confidence.scorer import score_alternatives

    alts = ["alpha", "beta"]
    out = score_alternatives(
        alternatives=alts,
        logprobs_content=None,
        response_text=None,
        cfg=ConfidenceConfig(),
    )
    assert len(out) == 2
    assert all(s.text == alt for s, alt in zip(out, alts, strict=False))
    assert all(s.logprob_score is None for s in out)
    assert all(s.self_decl_score is None for s in out)
    assert all(s.judge_score is None for s in out)
    assert all(s.band in {"HIGH", "MED", "LOW"} for s in out)


def test_score_alternatives_respects_disabled_signals():
    """When use_logprob=False, the logprob_score field stays None even
    if logprobs are provided."""
    from amx.config import ConfidenceConfig
    from amx.llm.confidence.scorer import score_alternatives

    cfg = ConfidenceConfig(use_logprob=False, use_self_consistency=False)
    out = score_alternatives(
        alternatives=["a", "b"],
        logprobs_content=[{"token": "a", "logprob": -0.1}],
        response_text="a b",
        cfg=cfg,
    )
    assert all(s.logprob_score is None for s in out)
    assert all(s.self_consistency_score is None for s in out)
