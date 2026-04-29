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
    """Compatibility wrapper for /search operations."""

    def __init__(self, cfg: AMXConfig, catalog: SearchCatalog):
        self.cfg = cfg
        self.catalog = catalog
        self.db_profile = cfg.active_db_profile or "default"
        self.settings = catalog.get_settings(self.db_profile)
        self._agent = SearchAgent(
            cfg,
            catalog,
            llm_factory=LLMProvider,
            inventory_db_factory=self._inventory_db,
        )

    def _inventory_db(self) -> DatabaseConnector:
        return DatabaseConnector(self.cfg.db)

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
