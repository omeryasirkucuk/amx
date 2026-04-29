"""Headless AMX application facade.

This module is the library-first entry point. The CLI should remain a thin
adapter over these core services.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amx.config import AMXConfig
from amx.core.ask_agent import AskToolbox, LoopBasedAskAgent, ToolAskResponse
from amx.core.state import StateManager
from amx.search.catalog import SearchAnswer, SearchCatalog
from amx.search.service import SearchService
from amx.storage.sqlite_store import SQLiteHistoryStore, history_store, init_history_store


@dataclass
class AMXApplication:
    """Composable core runtime for scripts, tests, services, and the CLI."""

    config: AMXConfig
    catalog: SearchCatalog
    store: SQLiteHistoryStore | None = None

    @classmethod
    def load(cls, config_path: str | None = None) -> "AMXApplication":
        cfg = AMXConfig.load(config_path)
        init_history_store(cfg.CONFIG_DIR)
        store = history_store()
        catalog = SearchCatalog.from_history_store()
        if catalog is None:
            db_path = Path(cfg.CONFIG_DIR) / "history.sqlite3"
            catalog = SearchCatalog(db_path)
        return cls(cfg, catalog, store)

    @property
    def state(self) -> StateManager:
        return StateManager(self.config, self.store, namespace=self.config.active_db_profile or "default")

    def ask(self, question: str) -> SearchAnswer:
        return SearchService(self.config, self.catalog).ask(question)

    def ask_with_tools(self, question: str) -> ToolAskResponse:
        toolbox = AskToolbox(self.config, self.catalog)
        return LoopBasedAskAgent(toolbox).answer(question)

    def explain(self, question: str) -> dict[str, Any]:
        return SearchService(self.config, self.catalog).explain(question)

    def run_analysis(
        self,
        scope: dict[str, list[str]] | None = None,
        *,
        apply: bool = False,
    ) -> dict[str, Any]:
        """Headless-safe analysis entrypoint.

        When no explicit scope or saved selection exists, this method returns a
        structured skipped result instead of opening interactive prompts.
        """
        selected_scope = scope or self._selected_scope()
        if not selected_scope:
            return {
                "status": "skipped",
                "reason": "no_scope",
                "message": "Provide scope={'schema': ['table']} or save selected_schemas/selected_tables before running headless analysis.",
            }
        return {
            "status": "planned",
            "apply": bool(apply),
            "scope": selected_scope,
            "message": "Headless planning succeeded. Use CLI /run for interactive review or wire this scope into orchestrator execution.",
        }

    def _selected_scope(self) -> dict[str, list[str]]:
        if self.config.current_schema and self.config.current_table:
            return {self.config.current_schema: [self.config.current_table]}
        if self.config.selected_schemas and self.config.selected_tables:
            return {schema: list(self.config.selected_tables) for schema in self.config.selected_schemas}
        if self.config.selected_schemas:
            return {schema: [] for schema in self.config.selected_schemas}
        return {}
