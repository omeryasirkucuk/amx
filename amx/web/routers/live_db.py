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

A small in-process LRU cache (:func:`_cached_connector`) keeps a
:class:`DatabaseConnector` per ``(active_db_profile, host, database,
catalog)`` tuple alive across requests so the SQLAlchemy connection
pool isn't recreated on every navigation. Active-profile changes
invalidate naturally because the cache key embeds the profile name.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from amx.config import AMXConfig, DBConfig
from amx.db.connector import DatabaseConnector
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/live", tags=["live-db"])


#: Per-key connector cache. Keys are tuples derived from
#: :func:`_profile_key`; values are the SQLAlchemy-backed
#: :class:`DatabaseConnector` instances. We cap the size manually so
#: a long visualizer session can't grow unbounded if the user keeps
#: tweaking profile fields.
_CONNECTOR_CACHE: dict[tuple, DatabaseConnector] = {}
_CONNECTOR_CACHE_MAX = 8


def _profile_key(db: DBConfig) -> tuple:
    """Return the cache key for a :class:`DBConfig`.

    Embeds every field that influences the SQLAlchemy URL or the
    catalog scope so a profile edit invalidates the connector
    cache automatically.
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


def _connector(cfg: AMXConfig) -> DatabaseConnector:
    """Resolve the live connector for the active profile.

    Caches per-key so navigating the browse UI doesn't rebuild the
    SQLAlchemy connection pool on every request. A profile edit
    yields a different key and therefore a fresh connector.
    """
    key = _profile_key(cfg.db)
    cached = _CONNECTOR_CACHE.get(key)
    if cached is not None:
        return cached
    if len(_CONNECTOR_CACHE) >= _CONNECTOR_CACHE_MAX:
        # Drop the oldest entry — Python 3.7+ dicts preserve insertion order.
        oldest_key = next(iter(_CONNECTOR_CACHE))
        try:
            _CONNECTOR_CACHE.pop(oldest_key).close()
        except Exception:  # pragma: no cover - defensive
            pass
    connector = DatabaseConnector(cfg.db)
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


@router.get("/catalogs")
def list_catalogs(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Return ``SHOW CATALOGS`` (or backend equivalent) for 3-level
    backends. 2-level backends return an empty list with
    ``supports_catalogs=false`` so the SPA can collapse the catalog
    rail in the asset tree."""
    db = _connector(cfg)
    supports = _coerce_or_500("Probing catalog support", db.supports_catalogs)
    catalogs = _coerce_or_500("Listing catalogs", db.list_catalogs) if supports else []
    return {
        "supports_catalogs": bool(supports),
        "catalogs": list(catalogs),
        "active_catalog": getattr(cfg.db, "catalog", "") or None,
    }


@router.get("/databases")
def list_databases(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """``SHOW DATABASES`` for 2-level backends. Returns an empty list
    on backends that don't expose a multi-database server (Databricks,
    BigQuery — those use ``/api/live/catalogs`` instead)."""
    db = _connector(cfg)
    databases = _coerce_or_500("Listing databases", db.list_databases)
    return {
        "databases": list(databases),
        "active_database": getattr(cfg.db, "database", "") or None,
    }


@router.get("/schemas")
def list_schemas(
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List schemas in the active catalog (or the one passed in
    ``?catalog=…``).

    The current connector reads ``cfg.db.catalog`` for the listing
    query, so when the caller passes a different catalog we
    temporarily swap it on the dataclass — same pattern the
    ``/ask`` agent's ``_scoped_catalog`` uses (see
    ``amx/search/agent_tools.py``).
    """
    db = _connector(cfg)
    if catalog and catalog != getattr(cfg.db, "catalog", ""):
        previous = cfg.db.catalog
        try:
            cfg.db.catalog = catalog
            schemas = _coerce_or_500("Listing schemas", db.list_schemas)
        finally:
            cfg.db.catalog = previous
    else:
        schemas = _coerce_or_500("Listing schemas", db.list_schemas)
    return {"catalog": catalog or getattr(cfg.db, "catalog", "") or None, "schemas": list(schemas)}


@router.get("/schemas/{schema}/assets")
def list_assets(schema: str, cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Return tables, views, and materialized views in *schema* in one
    payload — what the SPA expands when the user clicks a schema in
    the left tree.
    """
    db = _connector(cfg)
    raw = _coerce_or_500(f"Listing assets in {schema}", lambda: db.list_assets(schema))
    items = [{"name": name, "kind": kind.value} for name, kind in raw]
    return {"schema": schema, "assets": items, "count": len(items)}


@router.get("/schemas/{schema}/volumes")
def list_volumes(
    schema: str,
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Databricks Unity Catalog volumes in *schema*. Returns an empty
    list with a hint for backends without volume support so the SPA
    can grey out the "Volumes" tab."""
    db = _connector(cfg)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Lightweight column metadata: name, dtype, nullable. No row
    scan — fits the default Columns tab where we render the schema
    skeleton instantly and only kick off ``profile_table`` when the
    user clicks "Profile this table".
    """
    db = _connector(cfg)
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
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the lightweight metadata snapshot the orchestrator uses:
    column names + dtypes + comments + table comment. No profiling.
    """
    db = _connector(cfg)
    return _coerce_or_500(
        f"Reading metadata snapshot of {schema}.{table}",
        lambda: db.get_table_metadata_snapshot(schema, table),
    )
