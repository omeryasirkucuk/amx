"""Search-catalog (indexed-metadata) routes.

The :class:`amx.search.catalog.SearchCatalog` is the structured + vector
view of every entity AMX has seen — it's what ``/ask`` retrieves from
and what ``/sync`` keeps fresh. The web UI uses these endpoints for
two things:

* **Asset tree drill-down** — schema list, schema inventory, and
  per-table "explain" cards.
* **Free-text search bar** — the SPA's command palette uses
  ``search_columns`` / ``search_tables`` to drop-link straight into
  the relevant table or column page.

Every endpoint scopes to the active DB profile name; multi-profile
``/ask`` scopes are not supported here yet (PR-D's ask routes will
expose those when needed).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from amx.config import AMXConfig
from amx.search.catalog import SearchCatalog
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


def _catalog(cfg: AMXConfig) -> SearchCatalog:
    """Build a SearchCatalog handle bound to the active history store.

    SearchCatalog is a thin wrapper over the same SQLite file the
    history store uses; new instances share state safely. We construct
    one per request — cheap, and avoids the one-cache-per-process
    invalidation puzzle.

    Returns a 503 when the history store hasn't been initialised yet
    (fresh CLI session before any DB profile activated).
    """
    cat = SearchCatalog.from_history_store()
    if cat is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Search catalog isn't ready yet — activate a DB profile "
                "or run /history-store enable to initialise the index."
            ),
        )
    return cat


def _resolve_profile(cfg: AMXConfig, profile: str | None) -> str:
    """Resolve the target profile for an indexed-catalog query.

    Multi-profile Studio always passes ``?profile=NAME``; the CLI's
    web bridge falls back to ``cfg.active_db_profile`` when it has one.
    Either way, an empty resolution surfaces a 400 with a precise hint.
    """
    name = (profile or cfg.active_db_profile or "").strip()
    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "No active DB profile — pass ?profile=<name> to scope this "
                "request, or activate a profile via /api/profiles/db/<name>/activate."
            ),
        )
    return name


@router.get("/databases")
def known_databases(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Databases the catalog has indexed entities for, with row
    counts. Empty list when ``/sync`` has never been run for the
    target profile."""
    name = _resolve_profile(cfg, profile)
    rows = _catalog(cfg).known_databases(name)
    return {"databases": rows, "count": len(rows)}


@router.get("/schemas")
def known_schemas(
    database: str | None = Query(default=None, alias="db"),
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Indexed schemas for the target profile. ``?db=`` scopes the
    listing to one database (Databricks UC catalog / BigQuery
    project)."""
    name = _resolve_profile(cfg, profile)
    rows = _catalog(cfg).known_schemas(name, database_name=database)
    return {"database": database or None, "schemas": rows, "count": len(rows)}


@router.get("/inventory")
def schema_inventory(
    schema: str | None = Query(default=None),
    database: str | None = Query(default=None, alias="db"),
    limit: int = Query(default=500, ge=1, le=5000),
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return per-table structural inventory: table name, asset kind,
    row count, column count, effective description. The SPA renders
    this as the schema-detail page's main grid."""
    name = _resolve_profile(cfg, profile)
    rows = _catalog(cfg).schema_inventory(
        name,
        schema_name=schema,
        database_name=database,
        limit=limit,
    )
    return {
        "schema": schema,
        "database": database,
        "tables": rows,
        "count": len(rows),
        "limit": limit,
    }


@router.get("/explain")
def explain_table(
    path: str = Query(..., min_length=1),
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Per-table "card" the table-detail page hydrates with: structural
    metadata + column entities + relationships. ``path`` is
    ``schema.table`` (or ``database.schema.table`` for 3-level
    backends)."""
    name = _resolve_profile(cfg, profile)
    payload = _catalog(cfg).explain_table(name, path)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No catalog entry for {path!r}. Run /sync to refresh the index.",
        )
    return payload


@router.get("/search/columns")
def search_columns(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=8, ge=1, le=50),
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Hybrid lexical + vector search over indexed columns."""
    name = _resolve_profile(cfg, profile)
    rows = _catalog(cfg).search_columns(name, q, limit=limit)
    return {"q": q, "rows": rows, "count": len(rows)}


@router.get("/search/tables")
def search_tables(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=8, ge=1, le=50),
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Hybrid lexical + vector search over indexed tables."""
    name = _resolve_profile(cfg, profile)
    rows = _catalog(cfg).search_tables(name, q, limit=limit)
    return {"q": q, "rows": rows, "count": len(rows)}


@router.get("/settings")
def get_settings(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Per-profile search settings — the SPA's Settings page renders
    these as toggles (vector search on/off, score thresholds, etc.)."""
    name = _resolve_profile(cfg, profile)
    return {
        "profile": name,
        "settings": _catalog(cfg).get_settings(name),
    }


@router.get("/freshness")
def catalog_freshness(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Per-profile catalog freshness for the Studio top-bar pill.

    Returns ``last_synced_at`` (the most recent ``catalog_entities``
    write for the profile) and ``stale`` (true when the freshest row is
    older than 24 h). The Studio top bar renders this as a green /
    warning pill next to the LLM pricing-cache badge so the user can
    spot a stale catalog at a glance and click through to sync.
    """
    import sqlite3
    import time as _time

    from amx.storage.sqlite_store import history_store

    hs = history_store()
    if hs is None:
        return {"profiles": [], "stale_profile_count": 0}
    db_path = str(getattr(hs, "db_path", "") or "")
    if not db_path:
        return {"profiles": [], "stale_profile_count": 0}
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT db_profile,
                   COUNT(*) AS entity_count,
                   MAX(last_synced_at) AS last_synced_at
            FROM catalog_entities
            GROUP BY db_profile
            ORDER BY db_profile
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {"profiles": [], "stale_profile_count": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass
    now = _time.time()
    stale_after_sec = 24 * 60 * 60
    profiles: list[dict[str, Any]] = []
    stale_count = 0
    for row in rows:
        last = float(row["last_synced_at"] or 0.0)
        age = now - last if last else None
        is_stale = last == 0.0 or (age is not None and age > stale_after_sec)
        if is_stale:
            stale_count += 1
        profiles.append(
            {
                "profile": str(row["db_profile"] or ""),
                "entity_count": int(row["entity_count"] or 0),
                "last_synced_at": last or None,
                "age_seconds": age,
                "stale": bool(is_stale),
            }
        )
    return {
        "profiles": profiles,
        "stale_profile_count": stale_count,
        "stale_after_seconds": stale_after_sec,
    }


@router.post("/sync")
def trigger_catalog_sync(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Kick off an async catalog sync for the requested profile (or
    every saved profile when omitted). Returns immediately; the actual
    sync runs in a daemon thread so the pill can show "syncing…" and
    poll the freshness endpoint instead of blocking on a long request.
    """
    from amx.search.drift import fire_drift_probe

    if profile:
        targets = [profile.strip()]
    else:
        profile_map = getattr(cfg, "db_profiles", None)
        targets = list(profile_map.keys()) if hasattr(profile_map, "keys") else []
    targets = [p for p in targets if p]
    if not targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No DB profile to sync. Pass ?profile=<name> or save a profile first.",
        )
    fire_drift_probe(cfg, targets)
    return {"profiles": targets, "status": "queued"}
