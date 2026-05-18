"""Construct a :class:`PagesService` wired to the active AMX runtime.

The CLI and the FastAPI router both call ``build_pages_service`` so
neither has to know about the LLM provider / resolver / history-store
plumbing.
"""

from __future__ import annotations

from pathlib import Path

from amx.config import AMXConfig
from amx.pages._llm import AMXLLMClient
from amx.pages._resolver import AMXResolver
from amx.pages.service import PagesService
from amx.pages.store import PageStore
from amx.storage.sqlite_store import SQLiteHistoryStore, history_store


def build_pages_service(cfg: AMXConfig) -> PagesService:
    """Build a :class:`PagesService` against the active config and history store."""
    hs = history_store()
    if hs is None:
        config_dir = getattr(cfg, "CONFIG_DIR", str(Path.home() / ".amx"))
        hs = SQLiteHistoryStore(Path(config_dir) / "history.db")
        hs.init()
    store = PageStore(history=hs)
    store.init_schema()
    llm = AMXLLMClient(cfg)
    resolver = AMXResolver(cfg)
    model_name = getattr(llm, "model_name", None) or getattr(cfg.llm, "model", "") or "unknown"
    return PagesService(store=store, llm=llm, resolver=resolver, model_name=model_name)
