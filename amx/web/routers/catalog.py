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


def _active_profile(cfg: AMXConfig) -> str:
    name = (cfg.active_db_profile or "").strip()
    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active DB profile — pick one via /api/profiles/db/<name>/activate.",
        )
    return name


@router.get("/databases")
def known_databases(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Databases the catalog has indexed entities for, with row
    counts. Empty list when ``/sync`` has never been run for the
    active profile."""
    profile = _active_profile(cfg)
    rows = _catalog(cfg).known_databases(profile)
    return {"databases": rows, "count": len(rows)}


@router.get("/schemas")
def known_schemas(
    database: str | None = Query(default=None, alias="db"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Indexed schemas for the active profile. ``?db=`` scopes the
    listing to one database (Databricks UC catalog / BigQuery
    project)."""
    profile = _active_profile(cfg)
    rows = _catalog(cfg).known_schemas(profile, database_name=database)
    return {"database": database or None, "schemas": rows, "count": len(rows)}


@router.get("/inventory")
def schema_inventory(
    schema: str | None = Query(default=None),
    database: str | None = Query(default=None, alias="db"),
    limit: int = Query(default=500, ge=1, le=5000),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return per-table structural inventory: table name, asset kind,
    row count, column count, effective description. The SPA renders
    this as the schema-detail page's main grid."""
    profile = _active_profile(cfg)
    rows = _catalog(cfg).schema_inventory(
        profile,
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Per-table "card" the table-detail page hydrates with: structural
    metadata + column entities + relationships. ``path`` is
    ``schema.table`` (or ``database.schema.table`` for 3-level
    backends)."""
    profile = _active_profile(cfg)
    payload = _catalog(cfg).explain_table(profile, path)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Hybrid lexical + vector search over indexed columns."""
    profile = _active_profile(cfg)
    rows = _catalog(cfg).search_columns(profile, q, limit=limit)
    return {"q": q, "rows": rows, "count": len(rows)}


@router.get("/search/tables")
def search_tables(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=8, ge=1, le=50),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Hybrid lexical + vector search over indexed tables."""
    profile = _active_profile(cfg)
    rows = _catalog(cfg).search_tables(profile, q, limit=limit)
    return {"q": q, "rows": rows, "count": len(rows)}


@router.get("/settings")
def get_settings(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Per-profile search settings — the SPA's Settings page renders
    these as toggles (vector search on/off, score thresholds, etc.)."""
    profile = _active_profile(cfg)
    return {
        "profile": profile,
        "settings": _catalog(cfg).get_settings(profile),
    }
