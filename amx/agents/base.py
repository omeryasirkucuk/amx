"""Base agent definition and shared types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Confidence(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


def apply_logprob_confidence(
    suggestions: list["MetadataSuggestion"],
    logprobs: list | None,
    high_threshold: float = 0.85,
    medium_threshold: float = 0.50,
    response_text: str | None = None,
) -> list["MetadataSuggestion"]:
    """Set confidence from logprob statistics (text labels are ignored).

    If logprobs are unavailable/unparseable, keep existing confidence labels.
    """
    if not suggestions:
        return suggestions
    if not logprobs:
        return suggestions
    try:
        from amx.llm.provider import (
            confidence_from_logprobs,
            logprob_confidence_score,
            logprob_confidence_score_for_text,
        )

        fallback_score = logprob_confidence_score(logprobs)

        for s in suggestions:
            score = None
            if response_text:
                for desc in s.suggestions:
                    score = logprob_confidence_score_for_text(logprobs, response_text, desc)
                    if score is not None:
                        break
            if score is None:
                score = fallback_score
            s.logprob_score = score
            if score is None:
                continue
            if score >= high_threshold:
                s.confidence = Confidence.HIGH
            elif score >= medium_threshold:
                s.confidence = Confidence.MEDIUM
            else:
                s.confidence = Confidence.LOW

        if not response_text:
            raw = confidence_from_logprobs(
                logprobs,
                high_threshold=high_threshold,
                medium_threshold=medium_threshold,
            )
            if raw is not None:
                calibrated = Confidence[raw]
                for s in suggestions:
                    s.confidence = calibrated
    except Exception:
        return suggestions
    return suggestions


@dataclass
class MetadataSuggestion:
    schema: str
    table: str
    column: str | None  # None = table-level suggestion
    suggestions: list[str]
    confidence: Confidence
    reasoning: str
    source: str  # db_profile | rag | codebase | combined
    accepted: str | None = None  # final user-approved value
    logprob_score: float | None = None


@dataclass
class AgentContext:
    """Shared state passed between sub-agents."""

    schema: str = ""
    table: str = ""
    column: str | None = None
    asset_kind: str = "table"
    db_profile: dict[str, Any] = field(default_factory=dict)
    rag_context: list[str] = field(default_factory=list)
    code_context: list[str] = field(default_factory=list)
    existing_metadata: dict[str, Any] = field(default_factory=dict)


class BaseAgent(ABC):
    """Sub-agent contract."""

    name: str = "base"

    @abstractmethod
    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]:
        ...
