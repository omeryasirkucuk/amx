"""Signal A — per-alternative logprob span scorer."""

from __future__ import annotations

import math
from types import SimpleNamespace


def _fake_logprobs(tokens: list[tuple[str, float]]):
    """Build a list of logprob token objects shaped like LiteLLM/OpenAI returns.

    The provider helpers in amx/llm/provider.py read ``token`` and
    ``logprob`` attributes (or keys); SimpleNamespace satisfies the
    attribute path used by ``_lp_token_text`` / ``_lp_token_logprob``.
    """
    return [SimpleNamespace(token=t, logprob=lp) for t, lp in tokens]


def test_returns_none_when_logprobs_missing():
    from amx.llm.confidence.logprob_span import score_per_alternative

    out = score_per_alternative(
        logprobs_content=None,
        response_text="anything",
        alternatives=["a", "b"],
    )
    assert out == [None, None]


def test_returns_none_when_response_text_empty():
    from amx.llm.confidence.logprob_span import score_per_alternative

    out = score_per_alternative(
        logprobs_content=_fake_logprobs([("a", -0.1)]),
        response_text="",
        alternatives=["a"],
    )
    assert out == [None]


def test_scores_two_alternatives_independently():
    """Each alternative gets its own span score, not the same value."""
    from amx.llm.confidence.logprob_span import score_per_alternative

    tokens = [
        ("alpha", math.log(0.9)),
        (" ", math.log(0.99)),
        ("beta", math.log(0.4)),
    ]
    response = "alpha beta"
    out = score_per_alternative(
        logprobs_content=_fake_logprobs(tokens),
        response_text=response,
        alternatives=["alpha", "beta"],
    )
    assert out[0] is not None and out[1] is not None
    assert out[0] > out[1]
    assert 0.0 <= out[1] <= out[0] <= 1.0


def test_missing_target_text_yields_none_for_that_alternative():
    from amx.llm.confidence.logprob_span import score_per_alternative

    tokens = [("alpha", math.log(0.9))]
    out = score_per_alternative(
        logprobs_content=_fake_logprobs(tokens),
        response_text="alpha",
        alternatives=["alpha", "not-in-response"],
    )
    assert out[0] is not None
    assert out[1] is None
