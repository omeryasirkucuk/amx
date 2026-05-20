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


def evict_connector_cache(profile_name: str) -> int:
    """Wipe every cached connector that belongs to ``profile_name``.

    Called from the profile upsert / delete endpoints. The cache key
    is ``(profile_name, *_profile_key(scoped))``, so an edit that
    changes any of the URL-influencing fields naturally lands on a
    new key and the old entry would simply sit unused until LRU. But
    two correctness scenarios force the explicit eviction:

    1. Password / access_token edits never change ``_profile_key``
       (credentials aren't part of the URL-shaping tuple). Without
       this helper the next request finds the old connector and
       keeps using the stale credentials.
    2. A delete leaves orphan connectors behind that hold pool
       handles to a profile the user just removed; close-and-clear
       releases those resources promptly.

    Returns the number of entries removed.
    """
    name = (profile_name or "").strip()
    if not name:
        return 0
    removed_keys: list[tuple] = []
    for key in list(_CONNECTOR_CACHE.keys()):
        if key and key[0] == name:
            removed_keys.append(key)
    for key in removed_keys:
        try:
            _CONNECTOR_CACHE.pop(key).close()
        except Exception:  # pragma: no cover - defensive
            pass
    return len(removed_keys)


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
    connector = DatabaseConnector(scoped, profile_name=profile_name)
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


def _columns_from_cache(
    profile: str,
    schema: str,
    table: str,
    *,
    database_scope: str | None,
) -> list[dict[str, Any]]:
    """Return cached column rows for ``(profile, schema, table)`` from the
    best available source. Two layers, in order:

    1. ``catalog_entities`` via ``SearchCatalog.fetch_columns_for_table``
       — written by deep syncs and agent runs. Carries dtype + nullable.
    2. ``column_comments_cache.columns_json`` via the history store
       lookup — written every time the live introspector ran. Carries
       column names + comments but no dtype/nullable.

    Falling back through both layers means a table that has been seen
    *at all* (even if it was later dropped from the live DB) still
    surfaces something in Studio instead of "no introspectable
    columns". Returned shape is the same as the live path:
    ``[{"name", "dtype", "nullable", "comment"}]``.
    """
    rows: list[dict[str, Any]] = []
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        cat = None
    if cat is not None:
        try:
            cached = cat.fetch_columns_for_table(
                profile,
                schema_name=schema,
                table_name=table,
                database_name=database_scope,
            )
        except Exception:
            cached = []
        if cached:
            return [
                {
                    "name": c["name"],
                    "dtype": c.get("dtype") or "",
                    "nullable": bool(c.get("nullable", True)),
                    "comment": "",
                }
                for c in cached
            ]

    # Second-chance fallback: the column_comments_cache row carries the
    # last live read's column list as a JSON map (name -> comment).
    # That's enough to render the Studio table page when the catalog
    # only has a skeleton table-level row and the live DB is currently
    # returning empty (NoSuchTableError swallowed by list_column_profiles).
    try:
        from amx.storage._history_caches import lookup_column_comments_cache
        from amx.storage.sqlite_store import history_store as _history_store

        hs = _history_store()
    except Exception:
        hs = None
    if hs is not None:
        try:
            entry = lookup_column_comments_cache(
                hs,
                db_profile=profile,
                database=database_scope or "",
                schema=schema,
                table=table,
            )
        except Exception:
            entry = None
        columns_map = (entry or {}).get("columns") if entry else None
        if columns_map:
            for col_name, comment in columns_map.items():
                if not col_name:
                    continue
                rows.append(
                    {
                        "name": str(col_name),
                        "dtype": "",
                        "nullable": True,
                        "comment": str(comment or ""),
                    }
                )
        if rows:
            return rows

    # Third-chance fallback: scope-agnostic lookup against
    # ``column_comments_cache``. The SPA's URL pattern
    # ``/cat/<profile>/<database>/<schema>/<table>`` *should* pass the
    # database query parameter, but builds out in the wild have been
    # observed sending ``database=`` (empty) on cold catalog navigation
    # — and our second-chance lookup needs an exact ``(db_profile,
    # database, schema, table)`` match. When the user is clearly
    # asking about a specific ``(profile, schema, table)`` and there
    # is exactly one fresh cache row for it (any database), it is
    # almost always the right one. Return that row so the Studio Table
    # page stops rendering empty just because the URL didn't carry the
    # database scope.
    if hs is not None:
        import json as _json
        import time as _time

        try:
            with hs._connect() as conn:  # noqa: SLF001 — same access as helpers
                cache_rows = conn.execute(
                    """
                    SELECT database_name, columns_json
                    FROM column_comments_cache
                    WHERE db_profile = ? AND schema_name = ? AND table_name = ?
                      AND expires_at >= ?
                    ORDER BY fetched_at DESC
                    LIMIT 1
                    """,
                    (profile, schema, table, _time.time()),
                ).fetchall()
        except Exception:
            cache_rows = []
        if cache_rows:
            try:
                columns_map = _json.loads(cache_rows[0]["columns_json"]) or {}
            except Exception:
                columns_map = {}
            for col_name, comment in columns_map.items():
                if not col_name:
                    continue
                rows.append(
                    {
                        "name": str(col_name),
                        "dtype": "",
                        "nullable": True,
                        "comment": str(comment or ""),
                    }
                )
    return rows


def _writethrough_columns_to_catalog(
    profile: str,
    schema: str,
    table: str,
    *,
    database_scope: str | None,
    db_backend: str,
    columns: list[dict[str, Any]],
) -> None:
    """Persist freshly-introspected columns into ``catalog_entities``.

    Best-effort: failures are swallowed so a Studio request never
    breaks because of catalog write contention. The next visit to the
    same table benefits from the cache-first hit even though this
    request absorbed the live round-trip cost.
    """
    if not columns:
        return
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
        if cat is None:
            return
        with cat._connect() as conn:  # noqa: SLF001 — same access pattern as drift.py
            cat._upsert_entity(  # noqa: SLF001
                conn,
                db_profile=profile,
                db_backend=db_backend,
                database_name=database_scope or "",
                schema_name=schema,
                table_name=table,
                column_name=None,
                entity_kind="table",
                asset_kind="table",
            )
            for col in columns:
                name = (col.get("name") or "").strip()
                if not name:
                    continue
                cat._upsert_entity(  # noqa: SLF001
                    conn,
                    db_profile=profile,
                    db_backend=db_backend,
                    database_name=database_scope or "",
                    schema_name=schema,
                    table_name=table,
                    column_name=name,
                    entity_kind="column",
                    asset_kind="table",
                    dtype=str(col.get("dtype") or ""),
                    nullable=1 if col.get("nullable", True) else 0,
                )
    except Exception:
        # Write-through is a cache-warming nice-to-have. Never fail
        # the user-facing request because of it.
        return


def _active_scope_for_profile(cfg: AMXConfig, profile_name: str) -> dict[str, Any]:
    """Return the wizard-driven scope envelope for a profile.

    The wizard captures different fields per backend (catalog vs.
    project vs. database vs. dataset) but the rule the SPA needs is
    always the same: "if the user filled this in, narrow the listing
    to that value; if they left it blank, show everything". This
    helper centralises the per-backend mapping so endpoints don't
    each have to know which field is the catalog and which is the
    schema for every adapter.

    Field semantics by backend:

    * Databricks: ``cfg.db.catalog`` → top-level catalog,
      ``cfg.db.database`` → schema. The wizard prompt at line 883 of
      ``cli_support/commands/db.py`` labels the field "Schema /
      database (optional)" so users who pin one are pinning a schema.
    * BigQuery: ``cfg.db.project`` → catalog-equivalent,
      ``cfg.db.dataset`` → schema-equivalent.
    * 2-level backends (Postgres / Snowflake / MySQL / Oracle / MSSQL
      / Redshift / ClickHouse / DuckDB): ``cfg.db.database`` is the
      only scope-narrowing knob. No schema-level pin.

    Returned envelope (any value may be ``None``):

    ``{active_catalog, active_project, active_database,
       active_schema, active_dataset}``
    """
    base = cfg.db_profiles.get(profile_name) if profile_name else None
    if base is None:
        return {
            "active_catalog": None,
            "active_project": None,
            "active_database": None,
            "active_schema": None,
            "active_dataset": None,
        }
    backend = (getattr(base, "backend", "") or "").lower()
    catalog = (getattr(base, "catalog", "") or "").strip() or None
    project = (getattr(base, "project", "") or "").strip() or None
    database = (getattr(base, "database", "") or "").strip() or None
    dataset = (getattr(base, "dataset", "") or "").strip() or None
    # On Databricks the wizard's ``database`` field is the SCHEMA
    # (third level of the catalog→schema→table hierarchy). For every
    # other 2-level backend ``database`` is the catalog-equivalent
    # top-level scope so it goes into ``active_database`` instead.
    if backend == "databricks":
        return {
            "active_catalog": catalog,
            "active_project": None,
            "active_database": None,
            "active_schema": database,  # ← the schema pin lives here
            "active_dataset": None,
        }
    if backend == "bigquery":
        return {
            "active_catalog": None,
            "active_project": project,
            "active_database": None,
            "active_schema": None,
            "active_dataset": dataset,
        }
    # 2-level backends — only the database can be pinned.
    return {
        "active_catalog": None,
        "active_project": None,
        "active_database": database,
        "active_schema": None,
        "active_dataset": None,
    }


@router.get("/catalogs")
def list_catalogs(
    profile: str = Query(...),
    force_live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the catalog inventory for 3-level backends. Cache-first:
    distinct ``database_name`` rows from ``catalog_entities`` answer
    the sidebar without firing ``SHOW CATALOGS`` against Databricks
    on every profile expand. ``?force_live=true`` opts back into the
    live probe.
    """
    name = _require_profile(profile)
    scope = _active_scope_for_profile(cfg, name)
    if not force_live:
        cached = _cached_catalog_inventory(name)
        if cached is not None:
            return {
                "supports_catalogs": True,
                "catalogs": cached,
                "active_catalog": scope["active_catalog"],
                "active_project": scope["active_project"],
                "source": "catalog",
                "possibly_partial": not _profile_is_fully_synced(name),
            }
    db = _connector_for_scope(cfg, name)
    supports = _coerce_or_500("Probing catalog support", db.supports_catalogs)
    catalogs = _coerce_or_500("Listing catalogs", db.list_catalogs) if supports else []
    return {
        "supports_catalogs": bool(supports),
        "catalogs": list(catalogs),
        "active_catalog": scope["active_catalog"],
        "active_project": scope["active_project"],
        "source": "live",
    }


@router.get("/databases")
def list_databases(
    profile: str = Query(...),
    force_live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """``SHOW DATABASES`` for 2-level backends. Cache-first: distinct
    ``database_name`` rows from the catalog answer the sidebar without
    re-listing live. Returns an empty list on backends that don't
    expose a multi-database server (Databricks, BigQuery — those use
    ``/api/live/catalogs`` instead). ``?force_live=true`` to bypass.
    """
    name = _require_profile(profile)
    scope = _active_scope_for_profile(cfg, name)
    if not force_live:
        cached = _cached_catalog_inventory(name)
        if cached is not None:
            return {
                "databases": cached,
                "active_database": scope["active_database"],
                "source": "catalog",
                "possibly_partial": not _profile_is_fully_synced(name),
            }
    db = _connector_for_scope(cfg, name)
    databases = _coerce_or_500("Listing databases", db.list_databases)
    return {
        "databases": list(databases),
        "active_database": scope["active_database"],
        "source": "live",
    }


def _cached_catalog_inventory(profile: str) -> list[str] | None:
    """Distinct database / catalog names from ``catalog_entities`` for
    *profile*. Returns ``None`` when the cache is empty so the route
    can fall through to a live probe. Used by ``/catalogs`` and
    ``/databases`` to avoid re-firing ``SHOW CATALOGS`` /
    ``SHOW DATABASES`` on every sidebar expand."""
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return None
    if cat is None:
        return None
    try:
        rows = cat.fetch_inventory(profile, scope="databases")
    except Exception:
        return None
    if not rows:
        return None
    names = sorted({str(r.get("database") or "") for r in rows if r.get("database")})
    return names or None


@router.get("/schemas")
def list_schemas(
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    force_live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """List schemas under the requested scope.

    Cache-first: when the persistent catalog already lists schemas for
    this profile, return that list immediately and skip the live-DB
    round-trip. The Studio sidebar opens to "Loading schemas…" on
    every tree expand otherwise — even when ``/search sync`` covered
    the profile a minute ago. Comments are still fetched from the
    live DB when the catalog miss path runs, so the only path that
    changes is the cache-hit case.

    ``?force_live=true`` opts back into the live query — used by the
    sidebar's manual refresh affordance so a power user can verify
    against the source of truth.

    Each schema is enriched with its current ``comment`` so the
    Database page can show at a glance which schemas already have a
    description. Comment lookups go through the SQLAlchemy inspector
    per schema; failures are swallowed and the comment falls back to
    ``""`` so a single broken row never breaks the whole list.
    """
    name = _require_profile(profile)
    scope = _active_scope_for_profile(cfg, name)
    # The cache reader takes ``database`` (or ``catalog`` for 3-level
    # backends) so the result is scoped to the actual container the
    # SPA asked about. Without this scope, a Postgres profile with N
    # databases would return every schema across every database under
    # each tree-expand — the headline bug from the user's screenshot.
    cache_scope = database or catalog
    if not force_live:
        cached_items = _cached_schemas_for_profile(name, cache_scope)
        if cached_items:
            return {
                "catalog": catalog or None,
                "schemas": [it["name"] for it in cached_items],
                "items": cached_items,
                "active_schema": scope["active_schema"],
                "active_dataset": scope["active_dataset"],
                "source": "catalog",
                # ``possibly_partial`` is True whenever the profile's
                # sync hasn't been marked complete by /search sync.
                # The sidebar uses it to render a small staleness hint
                # next to the schema list so the user knows a manual
                # refresh might add rows.
                "possibly_partial": not _profile_is_fully_synced(name),
            }
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
        # The schema-level pin lets the SPA filter the rendered list
        # to a single schema (Databricks ``database`` field = schema)
        # or a single dataset (BigQuery ``dataset``). The pin is
        # **presentation only** — the schemas array still carries the
        # full list the connector returned so a user who manually
        # navigates outside the pin can still see what's available.
        "active_schema": scope["active_schema"],
        "active_dataset": scope["active_dataset"],
        "source": "live",
    }


def _cached_schemas_for_profile(
    profile: str, database: str | None = None
) -> list[dict[str, Any]] | None:
    """Persistent-catalog read for the sidebar's schema list. Returns
    ``None`` only when the catalog is missing / empty — partial sync
    is OK now, the caller flags ``possibly_partial`` on the response
    so the sidebar can show "Refresh" without blocking the user.

    Pre-PR the function bailed when ``is_profile_fully_synced`` was
    False; the result was that every expand on a half-synced or
    week-old profile hit the live DB, racking up Databricks SQL
    costs the user never asked for. The new contract: serve whatever
    we have, mark it partial, let the explicit refresh button (and
    ``?force_live=true``) be the only path that opens a connector.

    ``database`` scopes the lookup to a single database under the
    profile (Postgres / MySQL / Snowflake — 2-level backends — OR a
    Databricks UC catalog / BigQuery project — 3-level)."""
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return None
    if cat is None:
        return None
    try:
        rows = cat.fetch_distinct_schemas(profile, database_name=database)
    except Exception:
        return None
    if not rows:
        return None
    # The catalog schema-row doesn't carry a comment (schema comments
    # live on the live DB, not in catalog_entities). Empty string is
    # safe — the Database page hydrates the comment when the user
    # opens a schema.
    return [{"name": str(r.get("name") or ""), "comment": ""} for r in rows if r.get("name")]


def _profile_is_fully_synced(profile: str) -> bool:
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return False
    if cat is None:
        return False
    try:
        return bool(cat.is_profile_fully_synced(profile))
    except Exception:
        return False


def _cached_assets_for_profile_schema(
    profile: str, schema: str, database: str | None = None
) -> list[dict[str, Any]] | None:
    """Persistent-catalog read for the sidebar's asset list under a
    schema. Returns ``None`` only when the catalog has nothing for
    the (profile, schema) — partial sync no longer hides the cache.
    The route stamps ``possibly_partial`` so the sidebar can offer
    a refresh without forcing a live trip on every expand.

    ``database`` scopes to a specific database under the profile so
    Postgres (and friends) don't leak tables across multi-database
    profiles — the user-reported screenshot bug."""
    try:
        from amx.search.catalog import SearchCatalog

        cat = SearchCatalog.from_history_store()
    except Exception:
        return None
    if cat is None:
        return None
    try:
        rows = cat.fetch_distinct_tables_in_schema(profile, schema, database_name=database)
    except Exception:
        return None
    if not rows:
        return None
    # Asset kind isn't on the simple fetch helper; default to "table"
    # for the sidebar (the wide majority of rows are tables). Views /
    # materialized views surface their kind on the Table-detail page
    # which still hits the live DB. The sidebar uses the kind for
    # icon selection only.
    items: list[dict[str, Any]] = []
    for r in rows:
        n = str(r.get("name") or "")
        if not n:
            continue
        items.append({"name": n, "kind": "table", "comment": ""})
    return items


@router.get("/schemas/{schema}/assets")
def list_assets(
    schema: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    force_live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return tables, views, and materialized views in *schema* in one
    payload — what the SPA expands when the user clicks a schema in
    the left tree.

    Cache-first: persistent-catalog rows are returned without a live
    connector. ``?force_live=true`` bypasses the cache.
    """
    name = _require_profile(profile)
    cache_scope = database or catalog
    if not force_live:
        cached = _cached_assets_for_profile_schema(name, schema, cache_scope)
        if cached:
            return {
                "schema": schema,
                "assets": cached,
                "count": len(cached),
                "source": "catalog",
                "possibly_partial": not _profile_is_fully_synced(name),
            }
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    raw = _coerce_or_500(f"Listing assets in {schema}", lambda: db.list_assets(schema))
    items: list[dict[str, Any]] = []
    for asset_name, kind in raw:
        try:
            comment = db.get_table_comment(schema, asset_name) or ""
        except Exception:
            comment = ""
        items.append({"name": asset_name, "kind": kind.value, "comment": comment})
    return {"schema": schema, "assets": items, "count": len(items), "source": "live"}


@router.post("/schemas/{schema}/refresh")
def refresh_schema_metadata(
    schema: str,
    profile: str = Query(...),
    database: str | None = Query(default=None),
    catalog: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Drop the column-comments cache for ``schema`` and re-list.

    Backs the sidebar's manual refresh affordance. The TTL + write-path
    invalidations already keep the cache fresh in all normal flows;
    this endpoint exists for the out-of-band edit case (DBA tweaking
    comments directly in the warehouse console). The response shape
    matches ``list_assets`` so the SPA can swap the result in-place.
    """
    name = _require_profile(profile)
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    db.invalidate_column_comments_cache(schema=schema)
    raw = _coerce_or_500(f"Refreshing assets in {schema}", lambda: db.list_assets(schema))
    items: list[dict[str, Any]] = []
    for asset_name, kind in raw:
        try:
            comment = db.get_table_comment(schema, asset_name) or ""
        except Exception:
            comment = ""
        items.append({"name": asset_name, "kind": kind.value, "comment": comment})
    return {"schema": schema, "assets": items, "count": len(items), "refreshed": True}


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
    force_live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Lightweight column metadata: name, dtype, nullable. Cache-first:
    when ``/search sync`` has covered the table the catalog already
    knows every column, so the sidebar expands without paying a live
    round-trip per click. ``?force_live=true`` opts back into the
    live inspector path (the sidebar's manual refresh affordance).
    """
    name = _require_profile(profile)
    cache_scope = database or catalog
    if not force_live:
        cached_rows = _columns_from_cache(name, schema, table, database_scope=cache_scope)
        if cached_rows:
            return {
                "schema": schema,
                "table": table,
                "columns": [
                    {
                        "name": c["name"],
                        "dtype": c["dtype"],
                        "nullable": c["nullable"],
                    }
                    for c in cached_rows
                ],
                "count": len(cached_rows),
                "source": "catalog",
                "possibly_partial": not _profile_is_fully_synced(name),
            }
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    cols = _coerce_or_500(
        f"Listing columns of {schema}.{table}",
        lambda: db.list_column_profiles(schema, table),
    )
    live_columns = [{"name": c.name, "dtype": c.dtype, "nullable": bool(c.nullable)} for c in cols]
    if live_columns and not force_live:
        # Cache-warm for next time: write the column rows into
        # ``catalog_entities`` so a subsequent visit to the same table
        # serves from the catalog instead of repeating the live
        # round-trip. Best-effort — swallowed on failure.
        db_backend = ""
        try:
            db_cfg = cfg.db_profiles.get(name) if cfg.db_profiles else None
            db_backend = str(getattr(db_cfg, "backend", "") or "") if db_cfg else ""
        except Exception:
            db_backend = ""
        _writethrough_columns_to_catalog(
            name,
            schema,
            table,
            database_scope=cache_scope,
            db_backend=db_backend,
            columns=live_columns,
        )
    if not live_columns:
        # Live introspector returned nothing — could be a genuinely
        # empty table, a ghost row left over from a code-RAG ingest, or
        # a swallowed ``NoSuchTableError`` (``list_column_profiles``
        # degrades gracefully for the code-agent path). One last
        # fallback to the column_comments_cache so the Studio page
        # still shows column names + comments when the live DB has
        # since lost the table.
        salvage = _columns_from_cache(name, schema, table, database_scope=cache_scope)
        if salvage:
            return {
                "schema": schema,
                "table": table,
                "columns": [
                    {
                        "name": c["name"],
                        "dtype": c["dtype"],
                        "nullable": c["nullable"],
                    }
                    for c in salvage
                ],
                "count": len(salvage),
                "source": "cache-fallback",
                "possibly_partial": True,
            }
    return {
        "schema": schema,
        "table": table,
        "columns": live_columns,
        "count": len(live_columns),
        "source": "live",
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

    Resilient against partial introspection: when the live snapshot
    comes back with zero columns (the table was removed from the live
    DB after a code-RAG ingest, the user lacks ``USAGE`` on the
    schema, or the connector swallowed a ``NoSuchTableError``), we
    fall back to the cached column list so the Studio Table page
    still renders something useful instead of an empty card.
    """
    name = _require_profile(profile)
    cache_scope = database or catalog
    db = _connector_for_scope(cfg, name, database=database, catalog=catalog)
    try:
        snapshot = db.get_table_metadata_snapshot(schema, table)
    except Exception as exc:
        # Live snapshot blew up entirely (most often
        # ``NoSuchTableError`` propagated from ``get_column_comments``).
        # Try to salvage from the caches before giving up.
        salvage = _columns_from_cache(name, schema, table, database_scope=cache_scope)
        if not salvage:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=(
                    f"Reading metadata snapshot of {schema}.{table} "
                    f"failed: {exc.__class__.__name__}: {exc}"
                ),
            ) from exc
        return {
            "schema": schema,
            "table": table,
            "table_comment": "",
            "columns": [
                {
                    "name": c["name"],
                    "dtype": c["dtype"],
                    "nullable": c["nullable"],
                    "comment": c["comment"],
                }
                for c in salvage
            ],
            "source": "cache-fallback",
        }
    columns = snapshot.get("columns") or []
    if not columns:
        # Live introspector returned zero columns but didn't raise —
        # ``list_column_profiles`` quietly returns ``[]`` on
        # ``NoSuchTableError``. Try the same fallback path the
        # ``/columns`` endpoint uses so the Studio page surfaces names
        # + comments instead of an empty list.
        salvage = _columns_from_cache(name, schema, table, database_scope=cache_scope)
        if salvage:
            return {
                **snapshot,
                "columns": [
                    {
                        "name": c["name"],
                        "dtype": c["dtype"],
                        "nullable": c["nullable"],
                        "comment": c["comment"],
                    }
                    for c in salvage
                ],
                "source": "cache-fallback",
            }
    return snapshot
