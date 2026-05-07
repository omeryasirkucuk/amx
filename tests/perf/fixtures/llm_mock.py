"""Deterministic LLM mock for perf benchmarks.

Real LLM calls are non-deterministic in latency and cost money. The
mock here matches the surface used by the orchestrator hot path:
``chat(messages, ...)`` returning ``{"content": ..., "usage": ...}``
shape with a sleep proportional to the requested token budget.

This is *not* a behavioural mock — answer text is templated, not
inferred. It exists so ``bench_orchestrator.py`` can measure framework
overhead (fan-out, merge, persistence) without LLM noise.
"""

from __future__ import annotations

import dataclasses
import time
from typing import Any


@dataclasses.dataclass
class MockStats:
    calls: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_sleep_s: float = 0.0


class MockLLM:
    """Minimal stand-in for ``amx.llm.provider.LLMProvider.chat``.

    Parameters
    ----------
    latency_per_token_s:
        Synthetic per-output-token wall delay. Defaults to a value that
        still exercises ThreadPoolExecutor scheduling but keeps a
        100-table benchmark under a minute on developer laptops.
    output_tokens:
        Fixed output token count returned for every call.
    """

    def __init__(
        self,
        *,
        latency_per_token_s: float = 0.0005,
        output_tokens: int = 64,
    ) -> None:
        self.latency_per_token_s = latency_per_token_s
        self.output_tokens = output_tokens
        self.stats = MockStats()

    def chat(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        prompt_chars = sum(len(str(m.get("content", ""))) for m in messages)
        # Approximate tiktoken's ~4-chars-per-token heuristic without the dep.
        input_tokens = max(1, prompt_chars // 4)
        sleep_s = self.output_tokens * self.latency_per_token_s
        time.sleep(sleep_s)
        self.stats.calls += 1
        self.stats.total_input_tokens += input_tokens
        self.stats.total_output_tokens += self.output_tokens
        self.stats.total_sleep_s += sleep_s
        return {
            "content": "MOCK_RESPONSE",
            "usage": {
                "prompt_tokens": input_tokens,
                "completion_tokens": self.output_tokens,
                "total_tokens": input_tokens + self.output_tokens,
            },
            "model": model or "mock/zero-cost",
            "finish_reason": "stop",
        }
