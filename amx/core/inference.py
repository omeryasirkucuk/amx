"""Programmatic metadata inference entrypoints.

Part of the **public API** — see ``docs/PUBLIC_API.md`` for the
stability contract. The public surface is :class:`InferenceResult`
and :meth:`amx.core.AMXApplication.infer_metadata`. The free function
``infer_table_metadata`` here is the internal implementation those two
delegate to; it is not part of the stable contract.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from amx.agents.orchestrator import Orchestrator
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.utils.logging import get_logger

log = get_logger("core.inference")

__all__ = ["InferenceResult"]


def _format_rag_unavailable_reason(exc: BaseException) -> str:
    """One-line reason string used when ``RAGStore`` can't be opened.

    Lives next to the call site so the analyze-flow CLI helper and the
    library entrypoint produce identical wording (the two used to drift
    because each formatted its own message inside an
    ``except: pass``-style block).
    """
    return f"{exc.__class__.__name__}: {exc}"


@dataclass(frozen=True)
class InferenceResult:
    """One inferred metadata suggestion for a table or column.

    Returned from :meth:`amx.core.AMXApplication.infer_metadata`. The
    field set is part of the public contract — additive changes only
    across minor versions; existing fields keep their meaning across
    upgrades.
    """

    schema: str
    table: str
    column: str | None
    description: str
    confidence: str
    source: str
    asset_kind: str = "table"
    applied: bool = False
    alternatives: tuple[str, ...] = field(default_factory=tuple)
    logprob_score: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """JSON-friendly view (e.g. for CLI ``--json`` output)."""
        return {
            "schema": self.schema,
            "table": self.table,
            "column": self.column,
            "description": self.description,
            "confidence": self.confidence,
            "source": self.source,
            "asset_kind": self.asset_kind,
            "applied": self.applied,
            "alternatives": list(self.alternatives),
            "logprob_score": self.logprob_score,
        }


def infer_table_metadata(
    cfg: AMXConfig,
    schema: str,
    table: str,
    *,
    include_rag: bool = True,
    include_codebase: bool = False,
) -> list[InferenceResult]:
    """Run the headless inference pipeline for one table.

    Internal implementation behind
    :meth:`amx.core.AMXApplication.infer_metadata`. Library users should
    call the application method instead — direct imports of this
    function are not part of the stable contract.
    """
    rag_store: Any = None
    if include_rag:
        try:
            from amx.docs.rag import RAGStore

            store = RAGStore(source_filters=cfg.effective_doc_paths())
            if store.doc_count > 0:
                rag_store = store
        except Exception as exc:
            # Used to be ``except: pass``. Library callers can't see
            # ``rag_unavailable_reason`` on a metrics dict (there is no
            # run record at this layer), but we still log the reason
            # so the user can diagnose why no docs were used.
            rag_store = None
            log.warning(
                "RAGStore unavailable: %s. Inference will proceed without document context.",
                _format_rag_unavailable_reason(exc),
            )

    code_report: Any = None
    if include_codebase:
        try:
            from amx.cli_support.commands.run import _resolve_codebase_for_run

            db_for_code = DatabaseConnector(cfg.db)
            code_report = _resolve_codebase_for_run(
                cfg=cfg,
                db=db_for_code,
                scope={schema: [table]},
                code_profile=cfg.active_code_profile,
                code_refresh=False,
            )
        except Exception:
            code_report = None

    db = DatabaseConnector(cfg.db)
    llm = LLMProvider(cfg.llm)
    rag_cfg = cfg.effective_rag_llm()
    rag_llm = LLMProvider(rag_cfg) if rag_cfg is not cfg.llm else None
    orch = Orchestrator(
        db,
        llm,
        rag_store=rag_store,
        code_report=code_report,
        search_profile=cfg.active_db_profile or "default",
        rag_llm=rag_llm,
    )
    review_results = orch.process_table(schema, table, interactive_review=False)
    return [
        InferenceResult(
            schema=r.schema,
            table=r.table,
            column=r.column,
            description=r.final_description,
            confidence=r.confidence.value if hasattr(r.confidence, "value") else str(r.confidence),
            source=r.source,
            asset_kind=r.asset_kind,
            applied=r.applied,
            alternatives=tuple(r.alternatives),
            logprob_score=r.logprob_score,
        )
        for r in review_results
    ]
