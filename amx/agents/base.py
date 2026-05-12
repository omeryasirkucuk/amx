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
    suggestions: list[MetadataSuggestion],
    logprobs: list | None,
    high_threshold: float = 0.85,
    medium_threshold: float = 0.50,
    response_text: str | None = None,
) -> list[MetadataSuggestion]:
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


@dataclass(frozen=True)
class Citation:
    """Machine-readable provenance for a RAG-derived suggestion.

    Attached to every :class:`MetadataSuggestion` whose context was
    populated from documentation retrieval. Carries the chunk
    coordinates and the rerank score that ``RAGStore.query`` produced
    so the CLI summary, Studio run detail page, and downstream audit
    tools can deterministically trace a suggestion back to the exact
    documentation excerpts the prompt was built from -- independent
    of whatever free-text reasoning the LLM emitted.
    """

    #: Repo-relative path or URL of the source document, mirroring
    #: ``chunk.metadata["source"]`` recorded at ingest time.
    source: str
    #: Zero-based index of the chunk within the source file.
    chunk_idx: int
    #: Post-rerank score from :meth:`RAGStore.query`. Higher means more
    #: relevant to the query that surfaced this chunk.
    score: float
    #: First 200 chars of the chunk text, used by the UI to render a
    #: lightweight preview next to the citation.
    snippet: str


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
    #: Provenance trail for RAG-derived suggestions. Empty list for
    #: non-RAG agents (profile, codebase) and for merge outputs whose
    #: inputs had no citations, so legacy callers see no behaviour
    #: change.
    citations: list[Citation] = field(default_factory=list)


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
    # Optional free-text addendum from the user, populated by the
    # Re-Run flow. Empty string on normal runs. Each agent's
    # ``_build_prompt`` appends it as a final "Additional instructions
    # from user (re-run):" block so the original DB/docs/code context
    # is preserved and only this guidance is layered on top.
    user_instructions: str = ""


def _user_instructions_block(ctx: AgentContext) -> str:
    """Render the optional re-run instructions suffix.

    Empty string when ``ctx.user_instructions`` is unset / blank, so
    on normal runs the prompt is byte-identical to pre-Re-Run output
    (regression-safe).
    """
    text = (ctx.user_instructions or "").strip()
    if not text:
        return ""
    return (
        "\n\nAdditional instructions from user (re-run):\n"
        f"{text}\n"
        "Treat these as guidance to bias the description toward, not as a replacement "
        "for the database/docs/code evidence above."
    )


class BaseAgent(ABC):
    """Sub-agent contract."""

    name: str = "base"

    @abstractmethod
    def run(self, ctx: AgentContext) -> list[MetadataSuggestion]: ...
