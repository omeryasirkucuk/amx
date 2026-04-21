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
) -> list["MetadataSuggestion"]:
    """Override text-declared confidence with logprob-calibrated value when available."""
    if not logprobs:
        return suggestions
    try:
        from amx.llm.provider import confidence_from_logprobs

        raw = confidence_from_logprobs(logprobs)
        if raw is None:
            return suggestions
        calibrated = Confidence[raw]
    except Exception:
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
