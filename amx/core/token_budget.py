"""Token-budget validation and context compaction helpers."""

from __future__ import annotations

from dataclasses import dataclass

from amx.utils.token_tracker import estimate_tokens


@dataclass(frozen=True)
class MaxTokenValidator:
    """Keep prompts below a comfortable model context budget."""

    comfortable_input_tokens: int = 24_000
    per_chunk_head_chars: int = 1_200
    per_chunk_tail_chars: int = 300

    def estimate_messages(self, messages: list[dict[str, str]]) -> int:
        return int(estimate_tokens(messages))

    def exceeds(self, messages: list[dict[str, str]]) -> bool:
        return self.estimate_messages(messages) > self.comfortable_input_tokens

    def compact_chunks(self, chunks: list[str], *, budget_tokens: int | None = None) -> list[str]:
        """Extractively summarize chunks when context exceeds budget.

        This is intentionally deterministic. If an LLM summarization chain is
        added later, this function remains the safe fallback for headless runs.
        """
        budget = int(budget_tokens or self.comfortable_input_tokens)
        if not chunks:
            return []
        messages = [{"role": "user", "content": "\n\n".join(chunks)}]
        if estimate_tokens(messages) <= budget:
            return chunks
        compacted: list[str] = []
        for chunk in chunks:
            text = str(chunk or "").strip()
            if not text:
                continue
            if len(text) <= self.per_chunk_head_chars + self.per_chunk_tail_chars:
                compacted.append(text)
                continue
            compacted.append(
                text[: self.per_chunk_head_chars].rstrip()
                + "\n...[context compacted by MaxTokenValidator]...\n"
                + text[-self.per_chunk_tail_chars :].lstrip()
            )
        while (
            compacted
            and estimate_tokens([{"role": "user", "content": "\n\n".join(compacted)}]) > budget
        ):
            compacted.pop()
        return compacted
