"""Thin facade over the production-oriented `/search ask` agent.

This is the **canonical** entry point for natural-language metadata
questions. ``AMXApplication.ask()`` and the interactive CLI's
``/search ask`` both route through :class:`SearchService` →
:class:`amx.search.agent.SearchAgent`.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector
from amx.llm.provider import LLMProvider
from amx.search.agent import _SESSION_MEMORY, SearchAgent, SearchPlan
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

    def __init__(
        self,
        cfg: AMXConfig,
        catalog: SearchCatalog,
        *,
        db_profiles: list[str] | tuple[str, ...] | None = None,
    ):
        """Construct a /search service.

        ``db_profiles`` is the 0.11.0 multi-DB scope override. When
        omitted the service uses ``cfg.active_db_profiles`` (set by
        ``/use-db a b c``), which collapses to a single profile in
        the legacy single-DB workflow. Callers that want a per-call
        scope (e.g. ``/ask --db-profile A --db-profile B``) pass an
        explicit list and the underlying SearchAgent unions retrieval
        across those profiles.
        """
        self.cfg = cfg
        self.catalog = catalog
        scope = list(db_profiles) if db_profiles else cfg.effective_db_profiles()
        if not scope:
            scope = [cfg.active_db_profile or "default"]
        self.db_profiles: list[str] = scope
        # Legacy scalar — anchor profile, used for settings + write-back.
        self.db_profile = scope[0]
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
            db_profiles=self.db_profiles,
        )

    def _inventory_db(self) -> DatabaseConnector:
        if self._live_db is None:
            self._live_db = DatabaseConnector(self.cfg.db)
        return self._live_db

    def close(self) -> None:
        """Dispose the cached live DB connector, if any."""
        if self._live_db is not None:
            with contextlib.suppress(Exception):
                self._live_db.close()
            self._live_db = None

    def __enter__(self) -> SearchService:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def ask(
        self,
        question: str,
        *,
        cancel_token: threading.Event | None = None,
    ) -> SearchAnswer:
        """Run one /ask turn.

        ``cancel_token`` lets the CLI's Ctrl-C handler signal a clean
        cancellation between agent-loop iterations. Forwarded to
        :meth:`SearchAgent.ask`.
        """
        return self._agent.ask(question, cancel_token=cancel_token)

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
