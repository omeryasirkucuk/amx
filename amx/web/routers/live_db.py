"""Live-DB inspection routes.

Wraps :class:`amx.db.connector.DatabaseConnector` so the SPA can render
the live database tree (catalogs → schemas → tables → columns) without
the user touching the REPL. Every method here is read-only; mutating
endpoints (``apply_comment`` / ``set_*_comment``) live in
:mod:`amx.web.routers.comments` and ship in PR-E.

Heavy operations (``profile_table`` with ``mode="full"``) are gated
behind a ``POST`` so the SPA can show a spinner — the lightweight
``GET .../snapshot`` and ``GET .../columns`` endpoints cover the
default browse experience.

A small in-process LRU cache keeps a :class:`DatabaseConnector` per
``(profile, host, database, catalog)`` tuple alive across requests so
the SQLAlchemy connection pool isn't recreated on every navigation.
Editing a profile yields a different cache key and therefore a fresh
connector.

Scope: every browse endpoint REQUIRES a ``?profile=`` parameter,
optionally narrowed with ``&database=`` (2-level) or ``&catalog=``
(3-level). :func:`_connector_for_scope` produces a fresh ``DBConfig``
per request via :func:`dataclasses.replace`, so the profile record in
``cfg.db_profiles`` is never mutated and concurrent multi-profile
browse requests can't race each other.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from amx.config import AMXConfig, DBConfig
from amx.db.connector import DatabaseConnector
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/live", tags=["live-db"])


#: Per-key connector cache. Keys are tuples derived from
#: :func:`_profile_key`, prefixed with the profile name so two profiles
#: that point at the same ``host:db`` (e.g. ``prod_pg`` and
#: ``prod_pg_readonly``) don't collide. Values are the SQLAlchemy-backed
#: :class:`DatabaseConnector` instances. Cap is set generously enough
#: that a multi-profile browse session doesn't thrash; SQLAlchemy's
#: per-engine pool is small (5 connections by default) so 32 entries
#: is still well within file-descriptor budget.
_CONNECTOR_CACHE: dict[tuple, DatabaseConnector] = {}
_CONNECTOR_CACHE_MAX = 32


def _profile_key(db: DBConfig) -> tuple:
    """Return the cache key tail for a :class:`DBConfig`.

    Embeds every field that influences the SQLAlchemy URL or the
    catalog scope so a profile edit invalidates the connector
    cache automatically. Callers prepend the profile name to
    disambiguate identically-configured profiles.
    """
    return (
        db.backend,
        db.host,
        db.database,
        db.catalog,
        db.project,
        db.dataset,
        db.http_path,
        db.warehouse,
        db.role,
        db.account,
    )


def _evict_oldest() -> None:
    """Drop the oldest cache entry and close its connector."""
    if not _CONNECTOR_CACHE:
        return
    oldest_key = next(iter(_CONNECTOR_CACHE))
    try:
        _CONNECTOR_CACHE.pop(oldest_key).close()
    except Exception:  # pragma: no cover - defensive
        pass


def _connector_for_scope(
    cfg: AMXConfig,
    profile: str,
    *,
    database: str | None = None,
    catalog: str | None = None,
) -> DatabaseConnector:
    """Build a connector for an explicit ``(profile, database, catalog)``.

    Looks up *profile* in ``cfg.db_profiles`` and produces a fresh
    :class:`DBConfig` via :func:`dataclasses.replace`, overlaying
    ``database`` / ``catalog`` when provided. The original profile
    record is **never mutated**, which is the property the legacy
    ``cfg.db.catalog = …; finally: restore`` pattern in
    :func:`list_schemas` lacked — under concurrent requests the
    restore could land after another request's overlay and corrupt
    the shared dataclass. This helper has no such race.

    Callers must validate that the requested ``database`` / ``catalog``
    is appropriate for the backend (the route layer surfaces 400s);
    this helper only enforces that *profile* exists.
    """
    profile_name = (profile or "").strip()
    if not profile_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile query parameter is required.",
        )
    base = cfg.db_profiles.get(profile_name)
    if base is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {profile_name!r}.",
        )
    overlay: dict[str, Any] = {}
    if database is not None:
        overlay["database"] = database
    if catalog is not None:
        overlay["catalog"] = catalog
    scoped: DBConfig = replace(base, **overlay) if overlay else base
    key = (profile_name,) + _profile_key(scoped)
    cached = _CONNECTOR_CACHE.get(key)
    if cached is not None:
        return cached
    if len(_CONNECTOR_CACHE) >= _CONNECTOR_CACHE_MAX:
        _evict_oldest()
    connector = DatabaseConnector(scoped)
    _CONNECTOR_CACHE[key] = connector
    return connector


def _coerce_or_500(action: str, fn):
    """Run *fn*; convert connector exceptions to a uniform 500.

    Connector errors are typically backend-specific (driver missing,
    permission denied, network down). We surface the message verbatim
    so the SPA's error toast tells the user what to fix.
    """
    try:
        return fn()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{action} failed: {exc.__class__.__name__}: {exc}",
        ) from exc


def _require_profile(profile: str | None) -> str:
    """Reject requests that omit ``?profile=``."""
    name = (profile or "").strip()
    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile query parameter is required.",
        )
    return name


@router.get("/catalogs")
def list_catalogs(
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return ``SHOW CATALOGS`` (or backend equivalent) for 3-level
    backends. 2-level backends return an empty list with
    ``supports_catalogs=false`` so the SPA can collapse the catalog
    rail in the asset tree.
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name)
    supports = _coerce_or_500("Probing catalog support", db.supports_catalogs)
    catalogs = _coerce_or_500("Listing catalogs", db.list_catalogs) if supports else []
    base = cfg.db_profiles.get(name)
    pinned_catalog = (getattr(base, "catalog", "") or "") if base else ""
    return {
        "supports_catalogs": bool(supports),
        "catalogs": list(catalogs),
        "active_catalog": pinned_catalog or None,
    }


@router.get("/databases")
def list_databases(
    profile: str = Query(...),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """``SHOW DATABASES`` for 2-level backends. Returns an empty list
    on backends that don't expose a multi-database server (Databricks,
    BigQuery — those use ``/api/live/catalogs`` instead).
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name)
    databases = _coerce_or_500("Listing databases", db.list_databases)
    base = cfg.db_profiles.get(name)
    pinned_db = (getattr(base, "database", "") or "") if base else ""
    return {
        "databases": list(databases),
        "active_database": pinned_db or None,
    }


@router.get("/schemas")
def list_schemas(
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List schemas under the requested scope.

    Each schema is enriched with its current ``comment`` so the
    Database page can show at a glance which schemas already have a
    description. Comment lookups go through the SQLAlchemy inspector
    per schema; failures are swallowed and the comment falls back to
    ``""`` so a single broken row never breaks the whole list.
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    schemas = _coerce_or_500("Listing schemas", db.list_schemas)
    items: list[dict[str, Any]] = []
    for schema_name in schemas:
        try:
            comment = db.get_schema_comment(schema_name) or ""
        except Exception:
            comment = ""
        items.append({"name": schema_name, "comment": comment})
    return {
        "catalog": catalog or None,
        "schemas": [it["name"] for it in items],
        "items": items,
    }


@router.get("/schemas/{schema}/assets")
def list_assets(
    schema: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return tables, views, and materialized views in *schema* in one
    payload — what the SPA expands when the user clicks a schema in
    the left tree.
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    raw = _coerce_or_500(f"Listing assets in {schema}", lambda: db.list_assets(schema))
    items: list[dict[str, Any]] = []
    for asset_name, kind in raw:
        try:
            comment = db.get_table_comment(schema, asset_name) or ""
        except Exception:
            comment = ""
        items.append({"name": asset_name, "kind": kind.value, "comment": comment})
    return {"schema": schema, "assets": items, "count": len(items)}


@router.get("/schemas/{schema}/volumes")
def list_volumes(
    schema: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Databricks Unity Catalog volumes in *schema*. Returns an empty
    list with a hint for backends without volume support so the SPA
    can grey out the "Volumes" tab."""
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    if not getattr(db.capabilities, "volumes", False):
        return {
            "schema": schema,
            "volumes": [],
            "supports_volumes": False,
            "message": "This backend does not expose volumes.",
        }
    rows = _coerce_or_500(
        f"Listing volumes in {schema}",
        lambda: db.list_volumes(schema, catalog),
    )
    return {
        "schema": schema,
        "supports_volumes": True,
        "volumes": [
            {
                "name": str(row.get("name", "")),
                "kind": str(row.get("type", "volume")),
                "comment": str(row.get("comment", "") or ""),
            }
            for row in rows
        ],
    }


@router.get("/schemas/{schema}/tables/{table}/columns")
def list_columns(
    schema: str,
    table: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Lightweight column metadata: name, dtype, nullable. No row
    scan — fits the default Columns tab where we render the schema
    skeleton instantly and only kick off ``profile_table`` when the
    user clicks "Profile this table".
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    cols = _coerce_or_500(
        f"Listing columns of {schema}.{table}",
        lambda: db.list_column_profiles(schema, table),
    )
    return {
        "schema": schema,
        "table": table,
        "columns": [{"name": c.name, "dtype": c.dtype, "nullable": bool(c.nullable)} for c in cols],
        "count": len(cols),
    }


@router.get("/schemas/{schema}/tables/{table}/snapshot")
def table_snapshot(
    schema: str,
    table: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the lightweight metadata snapshot the orchestrator uses:
    column names + dtypes + comments + table comment. No profiling.
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    return _coerce_or_500(
        f"Reading metadata snapshot of {schema}.{table}",
        lambda: db.get_table_metadata_snapshot(schema, table),
    )
