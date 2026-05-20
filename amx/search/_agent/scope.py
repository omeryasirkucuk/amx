"""Resolve which doc/code profiles apply to a given DB scope.

The /ask tool agent calls these helpers to decide which RAG sources are
in scope for a question. The link maps live on AMXConfig
(``doc_profile_linked_dbs`` / ``code_profile_linked_dbs``) and answer
"this doc profile documents these DB profiles."

Resolution rules:
  - A profile with an empty/missing link list is **global** — it matches
    every DB scope (preserves the pre-link default behaviour).
  - A profile whose link list intersects ``scope_dbs`` is included.
  - When ``scope_dbs`` is empty (no DB scope selected), only the active
    profile (cfg.active_doc_profile / cfg.active_code_profile) returns,
    if any. Without a DB scope to cross-reference, returning every
    global profile would dilute retrieval — falling back to the active
    one keeps results focused.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from amx.config import AMXConfig
    from amx.storage.sqlite_store import SQLiteHistoryStore


def _resolve(
    scope_dbs: Iterable[str],
    profiles: dict[str, object],
    link_map: dict[str, list[str]],
    active_profile: str,
) -> list[str]:
    scope_set = {s for s in (scope_dbs or []) if s}
    if not scope_set:
        return [active_profile] if active_profile and active_profile in profiles else []
    out: list[str] = []
    for name in profiles:
        linked = link_map.get(name) or []
        if not linked:
            out.append(name)
            continue
        if any(db in scope_set for db in linked):
            out.append(name)
    return out


def resolve_doc_profiles_for_scope(cfg: AMXConfig, scope_dbs: Iterable[str]) -> list[str]:
    """Doc profiles in scope for the given DB list."""
    return _resolve(
        scope_dbs,
        cfg.doc_profiles,  # type: ignore[arg-type]
        cfg.doc_profile_linked_dbs,
        cfg.active_doc_profile or "",
    )


def resolve_code_profiles_for_scope(cfg: AMXConfig, scope_dbs: Iterable[str]) -> list[str]:
    """Code profiles in scope for the given DB list."""
    return _resolve(
        scope_dbs,
        cfg.code_profiles,  # type: ignore[arg-type]
        cfg.code_profile_linked_dbs,
        cfg.active_code_profile or "",
    )


def resolve_lineage_for_scope(
    *,
    cfg: AMXConfig,
    store: SQLiteHistoryStore,
    scope_db_profiles: list[str],
) -> list[str]:
    """Lineage artifacts the ASK agent will consider for the given DB
    scope. Returns canvas names ordered by ``lineage_artifacts.id``
    (creation order) so the picker shows a stable list.
    """
    del cfg  # reserved for future link-map support; signature mirrors siblings
    if not scope_db_profiles:
        return []
    placeholders = ",".join("?" for _ in scope_db_profiles)
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            f"SELECT name FROM lineage_artifacts WHERE db_profile IN ({placeholders}) ORDER BY id",
            tuple(scope_db_profiles),
        ).fetchall()
    return [str(r[0]) for r in rows]
