"""Thin facade over the production-oriented `/search ask` agent.

This is the **canonical** entry point for natural-language metadata
questions. ``AMXApplication.ask()`` and the interactive CLI's
``/search ask`` both route through :class:`SearchService` →
:class:`amx.search.agent.SearchAgent`.

There is a second tool-loop path
(:class:`amx.core.ask_agent.LoopBasedAskAgent`, exposed as
``AMXApplication.ask_with_tools()``) that predates this pipeline. It
is **deprecated as of 0.3.0** and will be removed in 0.4.0; new code
must use :class:`SearchService`.
"""

from __future__ import annotations

from typing import Any

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.search.agent import SearchAgent, SearchPlan, _SESSION_MEMORY
from amx.search.catalog import SearchAnswer, SearchCatalog


class SearchService:
    """Compatibility wrapper for /search operations.

    Implements the context-manager protocol so callers can write
    ``with SearchService(cfg, catalog) as svc: ...`` to ensure the live
    DB connector is disposed when the call finishes. Without this each
    ``/search ask`` invocation leaks a SQLAlchemy engine + connection
    pool — file descriptors accumulate across REPL turns until the
    process hits the OS NOFILE limit (the user-reported
    ``OSError: [Errno 24] Too many open files``).
    """

    def __init__(self, cfg: AMXConfig, catalog: SearchCatalog):
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self.settings = catalog.get_settings(self.db_profile)
        # Cache exactly one live connector across the SearchService lifespan
        # — every previous call site spawned a new one per ``_inventory_db``
        # invocation, which the legacy planner does many times per question.
        self._live_db: DatabaseConnector | None = None
        self._agent = SearchAgent(
            cfg,
            catalog,
            llm_factory=LLMProvider,
            inventory_db_factory=self._inventory_db,
        )

    def _inventory_db(self) -> DatabaseConnector:
        if self._live_db is None:
            self._live_db = DatabaseConnector(self.cfg.db)
        return self._live_db

    def close(self) -> None:
        """Dispose the cached live DB connector, if any."""
        if self._live_db is not None:
            try:
                self._live_db.close()
            except Exception:
                pass
            self._live_db = None

    def __enter__(self) -> "SearchService":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ask(self, question: str) -> SearchAnswer:
        return self._agent.ask(question)

    def explain(self, question: str) -> dict[str, Any]:
        answer = self.ask(question)
        return {
            "intent": answer.intent,
            "question": answer.question,
            "confidence": answer.confidence,
            "summary": answer.summary,
            "provenance": answer.provenance,
            "rows": answer.rows,
            "details": answer.details,
        }


__all__ = ["SearchService", "SearchPlan", "_SESSION_MEMORY"]
