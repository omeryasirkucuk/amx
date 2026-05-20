"""Module-level constants and helpers shared by ``SearchCatalog`` mixins.

These were originally defined at module scope inside
``amx/search/catalog.py``. After the v0.9.1 mixin split each mixin
became its own module — but the mixin method bodies still referred to
the old module-level names like ``DEFAULT_SETTINGS``, causing
``NameError: name 'DEFAULT_SETTINGS' is not defined`` at runtime.

Moving them here and re-exporting from ``catalog.py`` fixes the
regression without forcing a circular import (``_constants.py``
imports nothing from the mixins or from ``catalog.py``).
"""

from __future__ import annotations

import json
from typing import Any

# Source-priority ranking used by ``_resolve_effective_description`` to
# pick which candidate description wins when several sources contribute
# rows for the same entity.
SOURCE_PRIORITY: dict[str, int] = {
    # ``user_local`` is an explicit local-only override authored
    # through ``POST /api/comments/local`` or ``/db comment-local``.
    # It outranks every other source on purpose: the user opted into
    # this surface specifically to override what AMX already had —
    # generated suggestions, the live DB COMMENT, even a prior
    # ``manual`` edit that wrote back to the DB. The row never gets
    # ``applied_to_db=1`` so it stays a private annotation.
    "user_local": 5,
    "manual": 4,
    "reviewed": 3,
    "generated": 2,
    "imported": 1,
    "rejected": 0,
}


# Default values used by ``get_settings`` when a key isn't present in
# the per-profile settings table. New options land here first.
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
    # Default off — these are diagnostic, not conversational. The CLI
    # treats ``--debug`` as the canonical opt-in and falls back to
    # these flags only when the user explicitly enables them via
    # ``/search config``.
    "show_provenance": "false",
    "show_confidence": "false",
    "max_results": "8",
    "interpretation_mode": "balanced",
    "clarification_on_low_confidence": "true",
    # Tool-calling agent (default ON). Set to ``false`` to fall back
    # to the legacy regex-routed Pass1/alignment/retrieval pipeline;
    # useful as a temporary escape hatch during the rollout. Tests
    # that exercise the legacy planner path must opt out by writing
    # ``use_tool_agent=false``.
    "use_tool_agent": "true",
    # Per-provider distance threshold for vector-only retrieval hits.
    # Empty value means "use the embedding provider's calibrated
    # default"; callers can override per profile by setting an
    # explicit float.
    "vector_score_floor": "",
}


# Calibrated minimum match score (``3.0 - distance``) for vector-only
# hits to survive candidate filtering. The pre-calibration code
# hardcoded ``2.5`` everywhere — fine for MiniLM but conservative for
# OpenAI v3 (tighter distance distribution → higher floor is safe and
# reduces noise) and for sentence-transformers BGE-large. Override via
# the ``vector_score_floor`` search setting for a specific corpus.
_PROVIDER_SCORE_FLOOR: dict[str, float] = {
    "minilm": 2.5,
    "default": 2.5,
    "minilm-l6-v2": 2.5,
    "openai_compatible": 2.6,
    "sentence_transformers": 2.55,
}
_DEFAULT_SCORE_FLOOR = 2.5


def _vector_score_floor(
    settings: dict[str, str],
    embedding_kind: str | None = None,
) -> float:
    """Minimum match_score a vector-only hit must reach to survive filter.

    An explicit ``vector_score_floor`` setting wins; otherwise the
    value is calibrated to the active embedding provider.
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
    """Best-effort lookup of the active docs-side embedding kind.

    The catalog/search path is docs-side; ``cfg.embedding_docs`` is the
    source of truth here. Imports ``AMXConfig`` lazily so callers in
    restricted contexts don't pay the import cost; falls back to
    ``"minilm"`` when the lookup fails so the default behaviour is
    unchanged from before this calibration was added.
    """
    try:
        from amx.config import AMXConfig

        cfg = AMXConfig.load()
        return (cfg.embedding_docs.kind or "minilm").lower()
    except Exception:
        return "minilm"


def _json_loads(raw: Any, default: Any) -> Any:
    """Parse a JSON column value or return ``default`` on any failure."""
    if not isinstance(raw, str) or not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _database_name(
    database_name: str | None,
    catalog_name: str | None,
    project: str | None,
) -> str:
    """Pick the first non-empty database identifier across the three columns."""
    for value in (database_name, catalog_name, project):
        if value:
            return str(value)
    return ""


__all__ = [
    "DEFAULT_SETTINGS",
    "SOURCE_PRIORITY",
    "_active_embedding_kind",
    "_database_name",
    "_json_loads",
    "_vector_score_floor",
]
