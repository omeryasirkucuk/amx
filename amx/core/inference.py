"""Programmatic metadata inference entrypoints.

Part of the **public API** — see ``docs/PUBLIC_API.md`` for the
stability contract.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from amx.agents.orchestrator import Orchestrator
from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider

__all__ = ["infer_table_metadata"]


def infer_table_metadata(
    cfg: AMXConfig,
    schema: str,
    table: str,
    *,
    include_rag: bool = True,
    include_codebase: bool = False,
) -> list[dict[str, Any]]:
    """Infer metadata suggestions for a single table without invoking CLI commands."""
    rag_store = None
    if include_rag:
        try:
            from amx.docs.rag import RAGStore

            store = RAGStore(source_filters=cfg.effective_doc_paths())
            if store.doc_count > 0:
                rag_store = store
        except Exception:
            rag_store = None

    code_report = None
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
    orch = Orchestrator(
        db,
        llm,
        rag_store=rag_store,
        code_report=code_report,
        search_profile=cfg.active_db_profile or "default",
    )
    results = orch.process_table(schema, table, interactive_review=False)
    return [asdict(r) for r in results]
