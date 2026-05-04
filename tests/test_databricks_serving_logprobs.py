"""Pin the ``databricks_serving`` provider's no-logprobs contract.

User report 2026-05-04: ``/run`` hit ``litellm.BadRequestError:
top_logprobs: Extra inputs are not permitted`` against the
Databricks Foundation Model Serving Claude endpoint
(``databricks-claude-sonnet-4-6``). The shim Databricks runs in
front of Anthropic models doesn't accept the OpenAI ``logprobs``
flag, and AMX was sending it by default.

The fix is two-pronged:

1. Pre-emptively skip ``logprobs`` / ``top_logprobs`` for the
   ``databricks_serving`` provider on every call (not just the
   streaming path).
2. Add the literal Databricks 400 message to the runtime-detect
   fallback so any other provider that emits the same wording
   self-recovers without needing a manual patch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from amx.config import LLMConfig
from amx.llm.provider import (
    _LOGPROBS_UNSUPPORTED_PATTERNS,
    LLMProvider,
    _is_logprobs_unsupported_error,
)


def test_runtime_detector_recognises_databricks_message() -> None:
    """A future provider that emits the same 400 wording should
    self-recover via the runtime fallback, not require a code
    change."""
    err = RuntimeError("OpenAIException - top_logprobs: Extra inputs are not permitted")
    assert _is_logprobs_unsupported_error(err)


def test_logprobs_pattern_includes_databricks_phrasings() -> None:
    """Pin the canonical strings so a future linter / refactor
    doesn't drop them."""
    joined = " | ".join(_LOGPROBS_UNSUPPORTED_PATTERNS).lower()
    assert "top_logprobs: extra inputs are not permitted" in joined
    assert "logprobs: extra inputs are not permitted" in joined


def test_chat_strips_logprobs_for_databricks_serving() -> None:
    """The non-streaming path (``test_result()`` and most agent
    calls) must not forward ``logprobs`` / ``top_logprobs`` when
    the provider is ``databricks_serving`` — Databricks 400s with
    ``top_logprobs: Extra inputs are not permitted`` otherwise."""
    cfg = LLMConfig(
        provider="databricks_serving",
        model="databricks-claude-sonnet-4-6",
        api_key="dapi-test",
        api_base="https://example.databricks.com/serving-endpoints",
    )

    captured: dict[str, object] = {}

    def fake_completion(*args, **kwargs):
        captured.update(kwargs)
        # Minimal LiteLLM-shaped response so chat() doesn't blow up.
        return MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(content="OK", tool_calls=None),
                    finish_reason="stop",
                    logprobs=None,
                )
            ],
            usage=MagicMock(
                prompt_tokens=1,
                completion_tokens=1,
                total_tokens=2,
                model_dump=lambda: {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            ),
            model="databricks-claude-sonnet-4-6",
        )

    fake_lm = MagicMock(completion=fake_completion)

    with patch("amx.llm.provider._litellm", return_value=fake_lm):
        provider = LLMProvider(cfg)
        result = provider.chat([{"role": "user", "content": "Reply with OK"}])

    assert result.content == "OK"
    # The crux: neither ``logprobs`` nor ``top_logprobs`` reached
    # the underlying client.
    assert "logprobs" not in captured, captured
    assert "top_logprobs" not in captured, captured


def test_chat_keeps_logprobs_for_openai() -> None:
    """Sanity-check the disable is provider-scoped — OpenAI direct
    calls still get ``logprobs=True`` so confidence scoring keeps
    working."""
    cfg = LLMConfig(provider="openai", model="gpt-4o", api_key="sk-test")

    captured: dict[str, object] = {}

    def fake_completion(*args, **kwargs):
        captured.update(kwargs)
        return MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(content="OK", tool_calls=None),
                    finish_reason="stop",
                    logprobs=None,
                )
            ],
            usage=MagicMock(
                prompt_tokens=1,
                completion_tokens=1,
                total_tokens=2,
                model_dump=lambda: {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            ),
            model="gpt-4o",
        )

    fake_lm = MagicMock(completion=fake_completion)

    with patch("amx.llm.provider._litellm", return_value=fake_lm):
        provider = LLMProvider(cfg)
        provider.chat([{"role": "user", "content": "Reply with OK"}])

    assert captured.get("logprobs") is True
    assert captured.get("top_logprobs") == 5
