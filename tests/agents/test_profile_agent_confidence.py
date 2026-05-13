"""Integration: profile agent populates suggestion_scores end-to-end."""

from __future__ import annotations

import math
from types import SimpleNamespace
from unittest.mock import patch

import pytest


def _build_fake_chat_result():
    """Return a ChatResult-shaped object with two DESCRIPTION_ blocks
    and synthetic logprobs that ``logprob_confidence_score_for_text``
    can score."""
    response_text = (
        "COLUMN: email\n"
        "DESCRIPTION_1: Stores the customer email address.\n"
        "DESCRIPTION_2: Holds an email value for the customer record.\n"
        "CONFIDENCE: HIGH\n"
        "REASONING: column name and dtype both support an email field.\n"
        "TABLE_DESCRIPTION_1: Customer records.\n"
        "TABLE_CONFIDENCE: HIGH\n"
    )

    def _tok(t: str, p: float):
        return SimpleNamespace(token=t, logprob=math.log(p))

    # One token per word so the offsets line up well with span detection.
    tokens = [_tok(piece + " ", 0.85) for piece in response_text.split(" ") if piece]

    return SimpleNamespace(
        content=response_text,
        usage={"input_tokens": 10, "output_tokens": len(tokens)},
        logprobs=tokens,
        finish_reason="stop",
        confidence_score=None,
        tool_calls=None,
        thinking_content="",
        thinking_tokens=0,
    )


@pytest.mark.integration
def test_profile_agent_populates_suggestion_scores():
    """End-to-end: a mocked chat() response with logprobs is parsed
    into suggestions whose ``suggestion_scores`` attribute is filled
    with one ``AlternativeScore`` per alternative."""
    pytest.importorskip("sentence_transformers")  # Signal C needs the model

    from amx.agents.base import AgentContext
    from amx.agents.profile_agent import ProfileAgent
    from amx.config import LLMConfig
    from amx.llm.provider import LLMProvider

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.n_alternatives = 2

    llm = LLMProvider(cfg=cfg)
    agent = ProfileAgent(llm=llm)

    ctx = AgentContext(
        schema="public",
        table="users",
        db_profile={
            "columns": [
                {
                    "name": "email",
                    "dtype": "varchar",
                    "null_count": 0,
                    "row_count": 100,
                    "distinct_count": 100,
                    "cardinality_ratio": 1.0,
                    "min_val": "a@a",
                    "max_val": "z@z",
                    "samples": ["u@x.com"],
                }
            ]
        },
    )

    with patch.object(LLMProvider, "chat", return_value=_build_fake_chat_result()):
        suggestions = agent.run(ctx)

    assert suggestions, "agent returned no suggestions"
    # Find the column suggestion (column=email) — there's also a
    # table-level one (column=None) in the same batch.
    col_suggestions = [s for s in suggestions if s.column == "email"]
    assert col_suggestions, "profile agent did not emit a column-level suggestion"
    s = col_suggestions[0]
    assert s.suggestion_scores is not None
    assert len(s.suggestion_scores) >= 1
    bands = {score.band for score in s.suggestion_scores}
    assert bands <= {"HIGH", "MED", "LOW"}


def test_profile_agent_suggestion_scores_optional_when_disabled():
    """When confidence.enabled = False, suggestion_scores stays
    populated but with all None signals (ensemble 0.0 / LOW)."""
    from amx.agents.base import AgentContext
    from amx.agents.profile_agent import ProfileAgent
    from amx.config import ConfidenceConfig, LLMConfig
    from amx.llm.provider import LLMProvider

    cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
    cfg.n_alternatives = 1
    cfg.confidence = ConfidenceConfig(enabled=False)

    llm = LLMProvider(cfg=cfg)
    agent = ProfileAgent(llm=llm)

    ctx = AgentContext(
        schema="public",
        table="users",
        db_profile={
            "columns": [
                {
                    "name": "email",
                    "dtype": "varchar",
                    "null_count": 0,
                    "row_count": 100,
                    "distinct_count": 100,
                    "cardinality_ratio": 1.0,
                    "min_val": "a@a",
                    "max_val": "z@z",
                    "samples": ["u@x.com"],
                }
            ]
        },
    )

    with patch.object(LLMProvider, "chat", return_value=_build_fake_chat_result()):
        suggestions = agent.run(ctx)

    assert suggestions
    col = [s for s in suggestions if s.column == "email"]
    assert col
    s = col[0]
    # With confidence disabled, the orchestrator still fills the
    # ``suggestion_scores`` list (one row per alternative) but every
    # signal is None and the ensemble is 0.0 / LOW.
    assert s.suggestion_scores is not None
    for score in s.suggestion_scores:
        assert score.logprob_score is None
        assert score.self_consistency_score is None
        assert score.ensemble_score == 0.0
        assert score.band == "LOW"
