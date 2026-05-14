"""DB cache management endpoints — show / stats / clear.

Mirrors the REPL ``/db cache-{show,stats,clear}`` surface so Studio's
Cache page renders the same view and triggers the same DELETE paths.
Every endpoint is a thin wrapper over :mod:`amx.storage.cache_ops` so
the REPL and Studio share one implementation.

The clear endpoint applies the same safety gate as the REPL: a global
flush (no profile, no database) requires ``force=true`` in the body.
Cache loss is reversible (next live read repopulates) but a surprise
cross-profile nuke during an active Studio session would spike latency
for every reader.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from amx.storage.cache_ops import (
    CACHE_TYPES,
    cache_clear,
    cache_inventory,
    cache_stats,
)

router = APIRouter(prefix="/api/db/cache", tags=["db-cache"])

_SEARCH_LIMIT_DEFAULT = 50
_SEARCH_LIMIT_MAX = 200
_SEARCH_MIN_CHARS = 2


class ClearRequest(BaseModel):
    profile: str | None = Field(default=None)
    database: str | None = Field(default=None)
    types: list[str] | None = Field(default=None)
    force: bool = Field(default=False)


@router.get("/show")
def show(
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
) -> dict[str, Any]:
    """Per-(profile, database) row counts + last fetch."""
    rows = cache_inventory(profile=profile, database=database)
    return {
        "rows": [
            {
                "profile": r.profile,
                "database": r.database,
                "schemas_rows": r.schemas_rows,
                "columns_rows": r.columns_rows,
                "catalog_rows": r.catalog_rows,
                "last_fetch": r.last_fetch,
            }
            for r in rows
        ]
    }


@router.get("/stats")
def stats() -> dict[str, Any]:
    """Aggregate metrics per cache table."""
    raw = cache_stats()
    return {
        key: {
            "table": stat.table,
            "total_rows": stat.total_rows,
            "distinct_profiles": stat.distinct_profiles,
            "distinct_databases": stat.distinct_databases,
            "oldest_fetch": stat.oldest_fetch,
            "newest_fetch": stat.newest_fetch,
            "expired_rows": stat.expired_rows,
            "ttl_aware": stat.ttl_aware,
        }
        for key, stat in raw.items()
    }


@router.post("/clear")
def clear(body: ClearRequest) -> dict[str, Any]:
    """Flush rows under the requested scope. Returns per-table delete
    counts. Without ``profile`` AND without ``database`` the caller
    must pass ``force=true`` or receive a 400 — the Studio Clear-all
    button surfaces a confirmation modal that sets the flag.
    """
    if not body.profile and body.database is None and not body.force:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Global cache flush requires ``force=true``. Pass a profile "
                "or database to scope, or set force when you really mean it."
            ),
        )
    try:
        report = cache_clear(
            profile=body.profile,
            database=body.database,
            types=body.types,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return {
        "scope": {"profile": body.profile, "database": body.database},
        "types": report.types,
        "deleted": report.deleted,
        "total": report.total,
        "valid_types": list(CACHE_TYPES),
    }


@router.get("/search")
def search(
    q: str = Query(..., description="Substring to match (case-insensitive)"),
    profile: str | None = Query(default=None),
    limit: int = Query(default=_SEARCH_LIMIT_DEFAULT, ge=1, le=_SEARCH_LIMIT_MAX),
) -> dict[str, Any]:
    """Substring search across the persistent catalog cache —
    schema / table / column names. Powers the Studio sidebar's
    search box so the user can locate a column by typing its name
    instead of clicking through every schema to find it.

    Results are scoped to fully-synced profiles only. An unsynced
    profile contributes nothing (partial catalog rows would mislead).
    ``profile`` narrows to one profile; omit it to search every
    synced profile at once.

    A query under two characters returns an empty result set rather
    than the entire catalog — a one-letter substring would degrade
    into a near-no-op scan.

    Response shape::

        {
          "query": "<echoed q>",
          "truncated": bool,
          "results": [
            {"profile", "db_backend", "database",
             "schema", "table", "column", "match_field"},
            ...
          ]
        }

    ``table`` / ``column`` are ``null`` for higher-level matches;
    ``match_field`` is one of ``"schema" | "table" | "column"``.
    Rows are ordered ``schema → table → column`` so a search like
    ``customer`` surfaces the schema or table that bears the name
    before the long tail of columns containing the substring.
    """
    needle = (q or "").strip()
    if len(needle) < _SEARCH_MIN_CHARS:
        return {"query": needle, "truncated": False, "results": []}
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        cat = None
    if cat is None:
        return {"query": needle, "truncated": False, "results": []}
    try:
        results, truncated = cat.search_entities(needle, db_profile=profile, limit=limit)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Catalog search failed: {exc}",
        ) from exc
    return {"query": needle, "truncated": truncated, "results": results}
