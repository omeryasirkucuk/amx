"""Token estimation via tiktoken and per-session usage tracking."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import tiktoken


@lru_cache(maxsize=1)
def _get_encoding() -> tiktoken.Encoding:
    return tiktoken.get_encoding("cl100k_base")


def estimate_tokens(messages: list[dict[str, str]]) -> int:
    """Count tokens for a list of chat messages using tiktoken (cl100k_base).

    Accounts for the per-message overhead that the chat-completions
    format adds (~4 tokens per message for role/separator framing).
    """
    enc = _get_encoding()
    total = 0
    for msg in messages:
        total += 4  # role/name/separator framing
        for value in msg.values():
            total += len(enc.encode(value, disallowed_special=()))
    total += 2  # reply priming
    return max(1, total)


@dataclass
class _UsageRecord:
    step: str
    input_estimate: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class TokenTracker:
    """Accumulates token usage across a session (singleton via module-level instance)."""

    def __init__(self) -> None:
        self._records: list[_UsageRecord] = []

    def reset(self) -> None:
        self._records.clear()

    def record(
        self,
        step: str,
        input_estimate: int,
        usage: dict | None = None,
    ) -> None:
        prompt = 0
        completion = 0
        total = 0
        if usage:
            prompt = int(getattr(usage, "prompt_tokens", 0) or usage.get("prompt_tokens", 0) or 0)
            completion = int(
                getattr(usage, "completion_tokens", 0) or usage.get("completion_tokens", 0) or 0
            )
            total = int(getattr(usage, "total_tokens", 0) or usage.get("total_tokens", 0) or 0)
            if not total:
                total = prompt + completion
        self._records.append(
            _UsageRecord(
                step=step,
                input_estimate=input_estimate,
                prompt_tokens=prompt,
                completion_tokens=completion,
                total_tokens=total,
            )
        )

    def summary(self) -> list[tuple[str, int, int, int]]:
        """Aggregate records by step name -> list of (step, input, output, total)."""
        agg: dict[str, list[int]] = {}
        for r in self._records:
            if r.step not in agg:
                agg[r.step] = [0, 0, 0]
            agg[r.step][0] += r.prompt_tokens or r.input_estimate
            agg[r.step][1] += r.completion_tokens
            agg[r.step][2] += r.total_tokens or (r.prompt_tokens + r.completion_tokens)
        return [(step, *vals) for step, vals in agg.items()]

    @property
    def total_tokens(self) -> int:
        return sum(r.total_tokens or (r.prompt_tokens + r.completion_tokens) for r in self._records)

    @property
    def has_records(self) -> bool:
        return bool(self._records)


tracker = TokenTracker()
