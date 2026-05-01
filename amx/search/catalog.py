"""SQLite-backed search catalog built on top of the AMX history DB."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from amx.search._catalog import (
    EntityCrudMixin,
    JoinMixin,
    SearchMixin,
    SettingsMixin,
    SyncMixin,
    UsageMixin,
)
from amx.search.index import SearchIndex
from amx.storage.sqlite_store import history_store
from amx.utils.logging import get_logger

log = get_logger("search.catalog")

# Module-level constants + helpers moved to ``_catalog/_constants.py``
# in v0.9.5 so the SearchCatalog mixins (which now live in their own
# modules under ``_catalog/``) can import them without a circular
# dependency back through ``catalog.py``. Re-exported here so any
# pre-v0.9.5 caller that referenced ``amx.search.catalog.SOURCE_PRIORITY``
# / ``DEFAULT_SETTINGS`` keeps working unchanged.
from amx.search._catalog._constants import (  # noqa: E402, F401
    _DEFAULT_SCORE_FLOOR,
    _PROVIDER_SCORE_FLOOR,
    DEFAULT_SETTINGS,
    SOURCE_PRIORITY,
    _active_embedding_kind,
    _database_name,
    _json_loads,
    _vector_score_floor,
)


@dataclass
class SearchAnswer:
    intent: str
    question: str
    rows: list[dict[str, Any]]
    confidence: str
    summary: str
    provenance: list[str]
    details: dict[str, Any]


class SearchCatalog(
    EntityCrudMixin,
    SyncMixin,
    SearchMixin,
    JoinMixin,
    UsageMixin,
    SettingsMixin,
):
    """Manage catalog rows and sync/search operations.

    Composed of 6 mixin modules under ``amx/search/_catalog/``:

    * :class:`EntityCrudMixin` — entity / description row CRUD.
    * :class:`SyncMixin` — sync orchestration (catalog ← live DB,
      review decisions, codebase report).
    * :class:`SearchMixin` — search / find / ranking read-path.
    * :class:`JoinMixin` — join discovery (symbolic + semantic).
    * :class:`UsageMixin` — usage / history / manual-record bookkeeping.
    * :class:`SettingsMixin` — key-value settings + ``explain_table``.

    Cross-mixin calls resolve through Python MRO; ``SearchCatalog``
    itself owns construction (``__init__``, ``from_history_store``)
    and the shared ``_connect()`` SQLite handle.
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.index = SearchIndex()

    @classmethod
    def from_history_store(cls) -> SearchCatalog | None:
        hs = history_store()
        if hs is None:
            return None
        return cls(hs.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
