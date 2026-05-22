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


def apply_confidence_signals(
    suggestions: list[MetadataSuggestion],
    logprobs_content: list | None,
    response_text: str | None,
    cfg,
    llm: object | None = None,
) -> list[MetadataSuggestion]:
    """Populate ``suggestion_scores`` with per-alternative confidence rows.

    Best-effort: any failure (missing optional dep, signal raised, etc.)
    is swallowed — the suggestions list is returned unchanged on error
    so an analysis run is never aborted by a scoring regression.

    ``cfg`` is the full :class:`amx.config.LLMConfig` so the scorer can
    read both ``confidence_signal`` (which scorer to run) and the
    nested ``confidence`` block (band cut-offs + master enable switch).
    ``llm`` is needed only when the active signal is ``"judge"`` (the
    second-pass scorer issues a real LLM call); other signals ignore it.

    Note: the legacy aggregate fields (``confidence`` and
    ``logprob_score``) keep being maintained by
    ``apply_logprob_confidence`` so older CLI flows and history
    columns continue working. This function is additive.
    """
    if not suggestions:
        return suggestions
    try:
        from amx.llm.confidence.scorer import score_alternatives
    except Exception:
        return suggestions

    for s in suggestions:
        try:
            s.suggestion_scores = score_alternatives(
                alternatives=list(s.suggestions),
                logprobs_content=logprobs_content,
                response_text=response_text,
                cfg=cfg,
                llm=llm,
            )
        except Exception:
            s.suggestion_scores = None
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
    #: Optional 1-based ``(start_line, end_line)`` range of the chunk
    #: inside ``source``. Populated by PR γ for code citations (AST
    #: chunks have real source-line spans; ``.ipynb`` cells use the
    #: cell index for both bounds). ``None`` for doc citations from
    #: PR C and any other producer that has no line information --
    #: keeps the dataclass backwards-compatible with existing JSON
    #: snapshots and run records.
    line_range: tuple[int, int] | None = None


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
    #: Per-alternative confidence rows; ``None`` for legacy/disabled
    #: paths so existing serialisation logic stays untouched on rows
    #: that do not have a structured score block. Populated by
    #: ``apply_confidence_signals`` when confidence is enabled.
    #: Annotated ``list[Any]`` (not ``list[AlternativeScore]``) to avoid
    #: a circular import with ``amx.llm.confidence``; the runtime type
    #: is always ``AlternativeScore``.
    suggestion_scores: list[Any] | None = None
    #: One-line audit when the LLM (or the parser) returned fewer
    #: alternatives than the active profile's ``n_alternatives`` AND
    #: the top-up retry didn't fully recover — populated by
    #: :func:`ProfileAgent._top_up_under_produced` with a string like
    #: ``"produced 2 of 3 requested (retry got 0, fallback padded 1)"``.
    #: ``None`` on the success path so absence-of-warning is
    #: meaningful. Persisted to ``run_results.production_warning`` by
    #: the storage layer.
    production_warning: str | None = None


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
    # Actual RAG hits consumed by the original run, snapshotted at
    # re-run time so the second pass uses the same retrieval set
    # regardless of intervening re-ingests. Each element is the
    # ``{text, metadata, score, distance}`` dict produced by
    # :meth:`RAGStore.query`; an empty list means "no snapshot — fall
    # back to a live RAGStore query" (the pre-PR-D contract).
    rag_hits: list[dict[str, Any]] = field(default_factory=list)
    # PR δ (C8): snapshot of the code chunks the original run consumed.
    # Populated by the re-run hydrator from the cached payload so a
    # second pass uses the same semantic-retrieval set regardless of
    # intervening re-indexes. Empty list → fall back to a live
    # ``query_code_snippets`` call (pre-PR-δ behaviour).
    code_hits: list[dict[str, Any]] = field(default_factory=list)
    existing_metadata: dict[str, Any] = field(default_factory=dict)
    # Optional ingested-asset context blocks for this run. Each entry
    # is ``{kind, name, profile, excerpt}`` covering a notebook /
    # query / stream / pipeline the user attached as additional
    # context for the table currently being processed. Empty list on
    # normal runs; populated by the orchestrator per-table when the
    # caller resolved ``asset_context`` refs at submit time. The
    # profile agent emits an "Ingested asset context" prompt section
    # when this list is non-empty.
    asset_context: list[dict[str, Any]] = field(default_factory=list)
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
