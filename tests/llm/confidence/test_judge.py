"""Signal D — LLM-as-judge second-pass ranking."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock


def _fake_llm(response_text: str):
    """Return a SimpleNamespace mimicking just the ``.chat`` API surface used by judge."""
    chat = MagicMock(
        return_value=SimpleNamespace(
            content=response_text,
            usage={"input_tokens": 20, "output_tokens": 5},
            logprobs=None,
            finish_reason="stop",
            confidence_score=None,
            tool_calls=None,
            thinking_content="",
            thinking_tokens=0,
        )
    )
    return SimpleNamespace(chat=chat)


def test_empty_alternatives_returns_empty():
    from amx.llm.confidence.judge import score_per_alternative

    assert score_per_alternative([], llm=_fake_llm("")) == []


def test_n_equals_1_returns_degenerate_one():
    """With a single alternative there is nothing to rank against; the
    judge skips the LLM call entirely and returns ``1.0``."""
    from amx.llm.confidence.judge import score_per_alternative

    llm = _fake_llm("RANKING: 1")
    out = score_per_alternative(["only one"], llm=llm)
    assert out == [1.0]
    llm.chat.assert_not_called()


def test_rank_parsed_into_normalised_scores():
    """Three alternatives ranked ``2 > 1 > 3`` map to scores
    ``[0.5, 1.0, 0.0]`` after (n - rank) / (n - 1) normalisation."""
    from amx.llm.confidence.judge import score_per_alternative

    # Judge picks ALT-2 best, then ALT-1, then ALT-3.
    response = "RANKING: 2, 1, 3\nREASONING: alt 2 is most specific."
    llm = _fake_llm(response)
    out = score_per_alternative(
        ["Alpha description.", "Beta description.", "Gamma description."],
        llm=llm,
        shuffle=False,  # test asserts identity mapping
    )
    assert out == [0.5, 1.0, 0.0]
    assert llm.chat.call_count == 1


def test_partial_ranking_fills_missing_with_none():
    """Judge returns fewer indices than N → missing alternatives score
    ``None``; the ensemble downstream falls back to the remaining signals
    for those rows."""
    from amx.llm.confidence.judge import score_per_alternative

    response = "RANKING: 2"
    llm = _fake_llm(response)
    out = score_per_alternative(
        ["Alpha.", "Beta.", "Gamma."],
        llm=llm,
        shuffle=False,
    )
    # ALT-2 ranked best (score 1.0); the other two unranked.
    assert out[1] == 1.0
    assert out[0] is None
    assert out[2] is None


def test_unparseable_response_returns_all_none():
    """When the LLM emits something the judge cannot parse, every
    alternative gets ``None``."""
    from amx.llm.confidence.judge import score_per_alternative

    llm = _fake_llm("I refuse to rank these. None of them make sense.")
    out = score_per_alternative(["a", "b"], llm=llm, seed=0)
    assert out == [None, None]


def test_llm_failure_returns_all_none():
    """``LLMProvider.chat`` raising propagates as a silent ``None`` row
    so an analysis run is never aborted by a judge regression."""
    from amx.llm.confidence.judge import score_per_alternative

    llm = SimpleNamespace(chat=MagicMock(side_effect=RuntimeError("boom")))
    out = score_per_alternative(["a", "b"], llm=llm, seed=0)
    assert out == [None, None]
