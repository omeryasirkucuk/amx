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
) -> list["MetadataSuggestion"]:
    """Set confidence from logprob statistics (text labels are ignored).

    If logprobs are unavailable/unparseable, default to LOW so confidence
    is never based on model-declared text labels.
    """
    if not suggestions:
        return suggestions
    if not logprobs:
        for s in suggestions:
            s.confidence = Confidence.LOW
        return suggestions
    try:
        from amx.llm.provider import confidence_from_logprobs

        raw = confidence_from_logprobs(
            logprobs,
            high_threshold=high_threshold,
            medium_threshold=medium_threshold,
        )
        if raw is None:
            for s in suggestions:
                s.confidence = Confidence.LOW
            return suggestions
        calibrated = Confidence[raw]
    except Exception:
        for s in suggestions:
            s.confidence = Confidence.LOW
        return suggestions

    for s in suggestions:
        s.confidence = calibrated
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
