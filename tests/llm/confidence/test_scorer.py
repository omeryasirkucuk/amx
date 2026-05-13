"""Smoke + single-signal dispatch tests for ``score_alternatives``."""

from __future__ import annotations

from unittest.mock import patch


def test_alternative_score_to_json_shape():
    from amx.llm.confidence import AlternativeScore

    score = AlternativeScore(text="x", signal="self_consistency", score=0.61, band="MED")
    assert score.to_json() == {
        "text": "x",
        "signal": "self_consistency",
        "score": 0.61,
        "band": "MED",
    }


def test_disabled_signal_returns_empty_list():
    """When ``confidence_signal == 'none'`` the scorer skips work and
    returns an empty list so the caller leaves ``suggestion_scores``
    untouched (legacy ``alternatives_json`` path)."""
    from amx.config import LLMConfig
    from amx.llm.confidence.scorer import score_alternatives

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "none"
    out = score_alternatives(
        alternatives=["a", "b"],
        logprobs_content=None,
        response_text=None,
        cfg=cfg,
    )
    assert out == []


def test_disabled_via_master_switch_returns_empty():
    from amx.config import LLMConfig

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence.enabled = False
    from amx.llm.confidence.scorer import score_alternatives

    out = score_alternatives(
        alternatives=["a", "b"],
        logprobs_content=None,
        response_text=None,
        cfg=cfg,
    )
    assert out == []


def test_self_consistency_signal_runs_only_self_consistency():
    """Picking ``self_consistency`` runs only that scorer; the other
    three per-signal modules must not be invoked."""
    import pytest

    pytest.importorskip("sentence_transformers")

    from amx.config import LLMConfig
    from amx.llm.confidence.scorer import score_alternatives

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "self_consistency"

    with (
        patch("amx.llm.confidence.logprob_span.score_per_alternative") as mock_lp,
        patch("amx.llm.confidence.self_declaration.score_per_alternative") as mock_sd,
        patch("amx.llm.confidence.judge.score_per_alternative") as mock_judge,
    ):
        out = score_alternatives(
            alternatives=["alpha description", "beta description"],
            logprobs_content=None,
            response_text=None,
            cfg=cfg,
        )

    mock_lp.assert_not_called()
    mock_sd.assert_not_called()
    mock_judge.assert_not_called()

    assert len(out) == 2
    assert all(s.signal == "self_consistency" for s in out)
    assert all(s.score is not None for s in out)
    assert all(s.band in {"HIGH", "MED", "LOW"} for s in out)


def test_logprob_signal_runs_only_logprob():
    """Picking ``logprob`` runs only the span scorer; nothing else is
    invoked even though sentence-transformers would otherwise be free."""
    from amx.config import LLMConfig
    from amx.llm.confidence.scorer import score_alternatives

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "logprob"

    with (
        patch(
            "amx.llm.confidence.logprob_span.score_per_alternative",
            return_value=[0.82, 0.40],
        ) as mock_lp,
        patch("amx.llm.confidence.self_consistency.score_per_alternative") as mock_sc,
        patch("amx.llm.confidence.self_declaration.score_per_alternative") as mock_sd,
        patch("amx.llm.confidence.judge.score_per_alternative") as mock_judge,
    ):
        out = score_alternatives(
            alternatives=["a", "b"],
            logprobs_content=["dummy"],
            response_text="a b",
            cfg=cfg,
        )

    mock_lp.assert_called_once()
    mock_sc.assert_not_called()
    mock_sd.assert_not_called()
    mock_judge.assert_not_called()

    assert [s.score for s in out] == [0.82, 0.40]
    assert [s.signal for s in out] == ["logprob", "logprob"]
    assert [s.band for s in out] == ["HIGH", "LOW"]


def test_signal_returns_none_score_yields_em_dash_band():
    """When the active signal cannot score a row (e.g. logprob on
    Anthropic), the alternative still appears with ``score=None`` and
    ``band='—'`` so storage + UI can render gracefully."""
    from amx.config import LLMConfig
    from amx.llm.confidence.scorer import score_alternatives

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.confidence_signal = "logprob"
    with patch(
        "amx.llm.confidence.logprob_span.score_per_alternative",
        return_value=[None, None],
    ):
        out = score_alternatives(
            alternatives=["a", "b"],
            logprobs_content=None,
            response_text=None,
            cfg=cfg,
        )
    assert [s.score for s in out] == [None, None]
    assert [s.band for s in out] == ["—", "—"]
