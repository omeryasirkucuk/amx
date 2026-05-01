"""SQLite-backed search catalog built on top of the AMX history DB."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from difflib import SequenceMatcher
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable
from typing import Any

from amx.agents.base import MetadataSuggestion
from amx.codebase.analyzer import CodebaseReport, CodeReference
from amx.db.connector import AssetKind, TableProfile
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

SOURCE_PRIORITY = {
    "manual": 4,
    "reviewed": 3,
    "generated": 2,
    "imported": 1,
    "rejected": 0,
}

DEFAULT_SETTINGS: dict[str, str] = {
    "auto_sync_on_writeback": "true",
    "llm_enabled": "true",
    "enable_generated_metadata": "true",
    "enable_manual_metadata": "true",
    "enable_reviewed_metadata": "true",
    "enable_code_evidence": "true",
    "enable_vector_search": "true",
    "enable_exact_search": "true",
    "allow_code_evidence": "true",
    "allow_vector_support": "true",
    "context_detail": "standard",
    "verify_live_inventory": "true",
    "verify_live_relationships": "true",
    "semantic_join_inference": "true",
    "manual_weight": "6.0",
    "reviewed_weight": "4.5",
    "generated_weight": "3.0",
    "code_evidence_weight": "2.0",
    "freshness_weight": "1.0",
    "conversation_memory_turns": "4",
    "max_retrieved_entities": "8",
    "answer_style": "concise",
    # Default off — these are diagnostic, not conversational. The CLI now
    # treats `--debug` as the canonical opt-in and falls back to these flags
    # only when the user explicitly enables them via `/search config`.
    "show_provenance": "false",
    "show_confidence": "false",
    "max_results": "8",
    "interpretation_mode": "balanced",
    "clarification_on_low_confidence": "true",
    # Tool-calling agent (default ON). Set to ``false`` to fall back to the
    # legacy regex-routed Pass1/alignment/retrieval pipeline; useful as a
    # temporary escape hatch during the rollout. Tests that exercise the
    # legacy planner path must opt out by writing ``use_tool_agent=false``.
    "use_tool_agent": "true",
    # Per-provider distance threshold for vector-only retrieval hits.
    # Empty value means "use the embedding provider's calibrated default";
    # callers can override per profile by setting an explicit float.
    "vector_score_floor": "",
}


# Calibrated minimum match score (3.0 - distance) for vector-only hits to be
# kept in the candidate pool. The previous code hardcoded 2.5 for all
# embeddings — fine for MiniLM but conservative for the OpenAI v3 family
# (whose cosine distance for relevant matches is typically tighter, so a
# higher floor is safe and reduces noise) and for sentence-transformers
# models like BGE-large that also produce tighter distance distributions.
# Override via the ``vector_score_floor`` search setting if you need to
# tune for a specific corpus.
_PROVIDER_SCORE_FLOOR: dict[str, float] = {
    "minilm": 2.5,
    "default": 2.5,
    "minilm-l6-v2": 2.5,
    "openai_compatible": 2.6,
    "sentence_transformers": 2.55,
}
_DEFAULT_SCORE_FLOOR = 2.5


def _vector_score_floor(settings: dict[str, str], embedding_kind: str | None = None) -> float:
    """Return the minimum match_score a vector-only hit must reach to survive
    candidate filtering. An explicit ``vector_score_floor`` setting wins;
    otherwise the value is calibrated to the active embedding provider.
    """
    raw = (settings.get("vector_score_floor") or "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    kind = (embedding_kind or "").lower().strip()
    return _PROVIDER_SCORE_FLOOR.get(kind, _DEFAULT_SCORE_FLOOR)


def _active_embedding_kind() -> str:
    """Best-effort lookup of the active embedding kind without forcing the
    config singleton to be importable from arbitrary contexts. Falls back
    to ``"minilm"`` when the lookup fails so the default behaviour is
    unchanged from before this calibration was added.
    """
    try:
        from amx.config import AMXConfig

        cfg = AMXConfig.load()
        return (cfg.embedding.kind or "minilm").lower()
    except Exception:
        return "minilm"


def _json_loads(raw: Any, default: Any) -> Any:
    if not isinstance(raw, str) or not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _database_name(database_name: str | None, catalog_name: str | None, project: str | None) -> str:
    for value in (database_name, catalog_name, project):
        if value:
            return str(value)
    return ""


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
    def from_history_store(cls) -> "SearchCatalog | None":
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

