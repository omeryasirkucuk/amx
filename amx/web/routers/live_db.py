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
:class:`DatabaseConnector` per ``(profile, host, database, catalog)``
tuple alive across requests so the SQLAlchemy connection pool isn't
recreated on every navigation. Editing a profile yields a different
cache key and therefore a fresh connector.

Scope resolution: every browse endpoint accepts an explicit
``?profile=&database=&catalog=`` triple via :func:`_connector_for_scope`,
so multi-profile browsing in AMX Studio never mutates ``cfg.db``. When
the caller omits ``profile`` we fall back to the legacy single-active
path (``cfg.db`` / ``cfg.active_db_profile``) so older SPA builds and
the CLI keep working unchanged. The fall-back path retires in PR-3.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from amx.config import AMXConfig, DBConfig
from amx.db.connector import DatabaseConnector
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/live", tags=["live-db"])


def _require_scope_for_browse(cfg: AMXConfig, db: DatabaseConnector) -> None:
    r"""Block schema/table queries when the active profile is under-scoped.

    Two related cases the CLI's ``ensure_hierarchy_resolved`` covers and
    AMX Studio needs to mirror:

    1. **3-level backends (Databricks, BigQuery)**: when ``cfg.db.catalog``
       is empty the connector falls back to the SQLAlchemy inspector
       which issues ``SHOW TABLES FROM \`None\`.<schema>`` and crashes.
    2. **2-level backends (Postgres, MySQL, …)**: when ``cfg.db.database``
       is empty the connector silently lands on the server's default
       maintenance database (``postgres``) and only shows schemas of
       that DB. The user almost always meant a different one.

    Returning 412 with ``hint=select-catalog`` / ``select-database``
    lets the SPA show an inline picker that mirrors the CLI's
    ``/connect`` flow.
    """
    try:
        supports_catalogs = bool(db.supports_catalogs())
    except Exception:  # pragma: no cover - defensive
        supports_catalogs = False
    if supports_catalogs:
        if not (getattr(cfg.db, "catalog", "") or "").strip():
            raise HTTPException(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                detail={
                    "message": (
                        "No catalog selected for this profile. Pick a "
                        "catalog before browsing schemas."
                    ),
                    "hint": "select-catalog",
                    "profile": cfg.active_db_profile or "",
                },
            )
        return
    # 2-level: rely on the same is_database_pinned() check the CLI uses.
    try:
        pinned = bool(cfg.db.is_database_pinned())
    except Exception:
        pinned = True
    if not pinned:
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail={
                "message": (
                    "No database selected for this profile. Pick a "
                    "database before browsing schemas."
                ),
                "hint": "select-database",
                "profile": cfg.active_db_profile or "",
            },
        )


# Back-compat alias for tests / external callers that imported the
# old name. Same behaviour now extended to the 2-level case.
_require_catalog_for_3level = _require_scope_for_browse


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


def _connector(cfg: AMXConfig) -> DatabaseConnector:
    """Resolve the live connector for the active profile (legacy path).

    Used only by the back-compat path when callers don't pass an
    explicit ``?profile=`` query parameter. New code should reach for
    :func:`_connector_for_scope` instead so it never mutates ``cfg.db``.
    """
    profile_name = (cfg.active_db_profile or "").strip() or "_active_"
    key = (profile_name,) + _profile_key(cfg.db)
    cached = _CONNECTOR_CACHE.get(key)
    if cached is not None:
        return cached
    if len(_CONNECTOR_CACHE) >= _CONNECTOR_CACHE_MAX:
        _evict_oldest()
    connector = DatabaseConnector(cfg.db)
    _CONNECTOR_CACHE[key] = connector
    return connector


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


def _resolve_connector(
    cfg: AMXConfig,
    profile: str | None,
    *,
    database: str | None = None,
    catalog: str | None = None,
) -> DatabaseConnector:
    """Pick scoped vs legacy connector based on ``profile`` presence.

    PR-1 keeps the legacy path so the existing SPA build (which doesn't
    yet send ``?profile=``) and the CLI's web bridge continue working.
    PR-3 will remove the fall-back and require ``profile`` everywhere.
    """
    if (profile or "").strip():
        return _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    return _connector(cfg)


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
def list_catalogs(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return ``SHOW CATALOGS`` (or backend equivalent) for 3-level
    backends. 2-level backends return an empty list with
    ``supports_catalogs=false`` so the SPA can collapse the catalog
    rail in the asset tree.

    Pass ``?profile=NAME`` to list catalogs for any DB profile in the
    config without flipping the active profile. Omitting the param
    falls back to the legacy single-active path for back-compat.
    """
    db = _resolve_connector(cfg, profile)
    supports = _coerce_or_500("Probing catalog support", db.supports_catalogs)
    catalogs = _coerce_or_500("Listing catalogs", db.list_catalogs) if supports else []
    # ``active_catalog`` is preserved for the legacy SPA's UI hint;
    # callers passing ``?profile=`` get the resolved profile's pinned
    # catalog (if any), which mirrors the legacy semantics scoped to
    # that profile.
    if (profile or "").strip():
        base = cfg.db_profiles.get(profile.strip())
        active_catalog = (getattr(base, "catalog", "") or "") if base else ""
    else:
        active_catalog = getattr(cfg.db, "catalog", "") or ""
    return {
        "supports_catalogs": bool(supports),
        "catalogs": list(catalogs),
        "active_catalog": active_catalog or None,
    }


@router.get("/databases")
def list_databases(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """``SHOW DATABASES`` for 2-level backends. Returns an empty list
    on backends that don't expose a multi-database server (Databricks,
    BigQuery — those use ``/api/live/catalogs`` instead).

    Pass ``?profile=NAME`` to list databases for any DB profile.
    """
    db = _resolve_connector(cfg, profile)
    databases = _coerce_or_500("Listing databases", db.list_databases)
    if (profile or "").strip():
        base = cfg.db_profiles.get(profile.strip())
        active_database = (getattr(base, "database", "") or "") if base else ""
    else:
        active_database = getattr(cfg.db, "database", "") or ""
    return {
        "databases": list(databases),
        "active_database": active_database or None,
    }


@router.get("/schemas")
def list_schemas(
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List schemas under the requested scope.

    With ``?profile=NAME`` the route resolves a connector via
    :func:`_connector_for_scope` and never mutates ``cfg.db`` — safe
    under concurrent multi-profile browsing. Without ``profile`` we
    fall back to the legacy single-active path, which still uses the
    in-place catalog swap pattern (this is the racy path retiring in
    PR-3).

    Each schema is enriched with its current ``comment`` so the
    Database page can show at a glance which schemas already have a
    description. Comment lookups go through the SQLAlchemy inspector
    per schema; failures are swallowed and the comment falls back to
    ``""`` so a single broken row never breaks the whole list.
    """
    if (profile or "").strip():
        db = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
        schemas = _coerce_or_500("Listing schemas", db.list_schemas)
        effective_catalog = (catalog or "") or None
    else:
        db = _connector(cfg)
        effective_catalog_str = (catalog or getattr(cfg.db, "catalog", "") or "").strip()
        if not effective_catalog_str:
            _require_scope_for_browse(cfg, db)
        if catalog and catalog != getattr(cfg.db, "catalog", ""):
            previous = cfg.db.catalog
            try:
                cfg.db.catalog = catalog
                schemas = _coerce_or_500("Listing schemas", db.list_schemas)
            finally:
                cfg.db.catalog = previous
        else:
            schemas = _coerce_or_500("Listing schemas", db.list_schemas)
        effective_catalog = catalog or getattr(cfg.db, "catalog", "") or None
    items: list[dict[str, Any]] = []
    for name in schemas:
        try:
            comment = db.get_schema_comment(name) or ""
        except Exception:
            comment = ""
        items.append({"name": name, "comment": comment})
    return {
        "catalog": effective_catalog,
        "schemas": [it["name"] for it in items],
        "items": items,
    }


@router.get("/schemas/{schema}/assets")
def list_assets(
    schema: str,
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return tables, views, and materialized views in *schema* in one
    payload — what the SPA expands when the user clicks a schema in
    the left tree.

    With ``?profile=`` the connector is scoped per-request; without it
    the legacy single-active path applies (retires in PR-3).
    """
    if (profile or "").strip():
        db = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    else:
        db = _connector(cfg)
        _require_scope_for_browse(cfg, db)
    raw = _coerce_or_500(f"Listing assets in {schema}", lambda: db.list_assets(schema))
    items: list[dict[str, Any]] = []
    for name, kind in raw:
        try:
            comment = db.get_table_comment(schema, name) or ""
        except Exception:
            comment = ""
        items.append({"name": name, "kind": kind.value, "comment": comment})
    return {"schema": schema, "assets": items, "count": len(items)}


class _ActivateCatalogRequest(BaseModel):
    persist: bool = True


@router.post("/catalogs/{name}/activate")
def activate_catalog(
    name: str,
    body: _ActivateCatalogRequest | None = None,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Set the active catalog for the current DB profile.

    Mirrors the CLI's ``/connect`` catalog picker (see
    ``amx/cli_support/catalog_picker.py``). Without a catalog set,
    3-level backends like Databricks fall back to ``SHOW TABLES
    FROM `None`.<schema>`` and crash. This endpoint persists the
    pick so subsequent Studio sessions remember it.
    """
    chosen = (name or "").strip()
    if not chosen:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Catalog name must be non-empty.",
        )
    db = _connector(cfg)
    try:
        supports = bool(db.supports_catalogs())
    except Exception:
        supports = False
    if not supports:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The active backend does not expose catalogs.",
        )
    cfg.db.catalog = chosen
    persist = True if body is None else bool(body.persist)
    if persist:
        # Mirror the CLI flow: writes back to ~/.amx/config.yml so the
        # picker doesn't have to run on every Studio launch.
        try:
            cfg.save()
        except Exception:
            # Don't let a save failure block the in-process state change —
            # the user can re-pick next session if persistence is broken.
            pass
    # Drop the connector cache entry — its key embedded the old catalog.
    _CONNECTOR_CACHE.clear()
    return {
        "catalog": chosen,
        "profile": cfg.active_db_profile or "",
        "persisted": persist,
    }


class _ActivateDatabaseRequest(BaseModel):
    persist: bool = True


@router.post("/databases/{name}/activate")
def activate_database(
    name: str,
    body: _ActivateDatabaseRequest | None = None,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Pin the active database for a 2-level DB profile.

    Counterpart of :func:`activate_catalog` for backends like Postgres,
    MySQL, Snowflake, etc. Without this, when a profile leaves
    ``database`` blank the connector lands on the server's default
    maintenance DB and the user only sees that one — exactly the
    surprise the CLI's ``/connect`` picker already prevents.
    """
    chosen = (name or "").strip()
    if not chosen:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Database name must be non-empty.",
        )
    db = _connector(cfg)
    try:
        supports_catalogs = bool(db.supports_catalogs())
    except Exception:
        supports_catalogs = False
    if supports_catalogs:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Active backend uses catalogs, not databases. Use "
                "/api/live/catalogs/{name}/activate."
            ),
        )
    cfg.db.database = chosen
    persist = True if body is None else bool(body.persist)
    if persist:
        try:
            cfg.save()
        except Exception:
            pass
    _CONNECTOR_CACHE.clear()
    return {
        "database": chosen,
        "profile": cfg.active_db_profile or "",
        "persisted": persist,
    }


@router.get("/schemas/{schema}/volumes")
def list_volumes(
    schema: str,
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Databricks Unity Catalog volumes in *schema*. Returns an empty
    list with a hint for backends without volume support so the SPA
    can grey out the "Volumes" tab."""
    if (profile or "").strip():
        db = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    else:
        db = _connector(cfg)
    if not getattr(db.capabilities, "volumes", False):
        return {
            "schema": schema,
            "volumes": [],
            "supports_volumes": False,
            "message": "This backend does not expose volumes.",
        }
    if not (profile or "").strip() and not (catalog or "").strip():
        _require_scope_for_browse(cfg, db)
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
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Lightweight column metadata: name, dtype, nullable. No row
    scan — fits the default Columns tab where we render the schema
    skeleton instantly and only kick off ``profile_table`` when the
    user clicks "Profile this table".
    """
    if (profile or "").strip():
        db = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    else:
        db = _connector(cfg)
        _require_scope_for_browse(cfg, db)
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
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the lightweight metadata snapshot the orchestrator uses:
    column names + dtypes + comments + table comment. No profiling.
    """
    if (profile or "").strip():
        db = _connector_for_scope(cfg, profile, database=database, catalog=catalog)
    else:
        db = _connector(cfg)
        _require_scope_for_browse(cfg, db)
    return _coerce_or_500(
        f"Reading metadata snapshot of {schema}.{table}",
        lambda: db.get_table_metadata_snapshot(schema, table),
    )
