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

import json
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from amx.config import AMXConfig
from amx.search.catalog import SearchCatalog
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api/catalog", tags=["catalog"])


class CacheRefreshRequest(BaseModel):
    """Body accepted by ``POST /api/catalog/refresh``.

    Ad-hoc, synchronous variant of the scheduled cache_refresh executor:
    the user picks a scope in the Catalog cache page's Sync-scope dialog
    and the request runs the same invalidate + warm path that the tick
    engine would run for a scheduled refresh, but without writing a
    ``scheduled_runs`` row. Mode mirrors the picker output exactly so
    the executor branch is identical to the scheduled path.
    """

    profile: str = Field(min_length=1)
    database: str | None = None
    catalog: str | None = None
    scope: dict[str, Any] = Field(
        default_factory=lambda: {"mode": "all"},
        description="{'mode': 'all|schemas|tables|columns', ...}.",
    )
    kind: Literal["cache_refresh"] = "cache_refresh"


class CatalogSyncCancelRequest(BaseModel):
    """Body accepted by ``POST /api/catalog/sync/cancel``."""

    profile: str = Field(min_length=1)


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
        # State rows live in a separate table — catalog_entities only
        # counts what's been written. A profile that's currently
        # ``state='syncing'`` may have zero entities yet, so we merge
        # the state table in by profile name.
        try:
            state_rows = conn.execute(
                """
                SELECT db_profile, state, total_tables, processed_tables,
                       started_at, finished_at, last_full_sync_at, last_error
                FROM catalog_profile_state
                """
            ).fetchall()
        except sqlite3.OperationalError:
            state_rows = []
    except sqlite3.OperationalError:
        return {"profiles": [], "stale_profile_count": 0, "syncing_profile_count": 0}
    finally:
        try:
            conn.close()
        except Exception:
            pass
    now = _time.time()
    # The catalog cache itself NEVER auto-invalidates (pre-PR the
    # 7-day window forced live-DB fallbacks; the user prefers a
    # staleness warning over a hard expiry). The pill turns yellow
    # past one week so the user has a visible nudge to run /search
    # sync at their own pace.
    stale_after_sec = 7 * 24 * 60 * 60
    state_by_profile = {str(r["db_profile"] or ""): r for r in state_rows}
    # Filter to profiles the user actually has configured. ``catalog_entities``
    # row keys are profile names that may include tombstones — names like
    # the historical ``"default"`` fallback or a profile the user has since
    # deleted from their config. The freshness pill should match the user's
    # mental model of *current* profiles; orphan rows stay on disk in case
    # the user re-adds a profile with the same name later, but they no
    # longer show up in the dropdown.
    valid_profiles: set[str] | None = None
    if cfg is not None:
        profile_map = getattr(cfg, "db_profiles", None)
        if hasattr(profile_map, "keys"):
            valid_profiles = {str(k) for k in profile_map}
    profiles: list[dict[str, Any]] = []
    stale_count = 0
    syncing_count = 0
    seen: set[str] = set()
    for row in rows:
        name = str(row["db_profile"] or "")
        if valid_profiles is not None and name not in valid_profiles:
            # Tombstone — skip. The state-row loop below applies the same
            # filter so a leftover ``catalog_profile_state`` entry for a
            # deleted profile doesn't sneak back in via the second pass.
            continue
        seen.add(name)
        last = float(row["last_synced_at"] or 0.0)
        age = now - last if last else None
        is_stale = last == 0.0 or (age is not None and age > stale_after_sec)
        state_row = state_by_profile.get(name)
        state_value = str(state_row["state"]) if state_row else "none"
        if state_value == "syncing":
            syncing_count += 1
        if is_stale:
            stale_count += 1
        profiles.append(
            {
                "profile": name,
                "entity_count": int(row["entity_count"] or 0),
                "last_synced_at": last or None,
                "age_seconds": age,
                "stale": bool(is_stale),
                # New: state machine + progress + error so the pill can
                # render Syncing 47 / 1000 and a Retry CTA on failure.
                "state": state_value,
                "total_tables": int(state_row["total_tables"] or 0) if state_row else 0,
                "processed_tables": int(state_row["processed_tables"] or 0) if state_row else 0,
                "started_at": float(state_row["started_at"])
                if state_row and state_row["started_at"]
                else None,
                "finished_at": float(state_row["finished_at"])
                if state_row and state_row["finished_at"]
                else None,
                "last_full_sync_at": float(state_row["last_full_sync_at"])
                if state_row and state_row["last_full_sync_at"]
                else None,
                "last_error": str(state_row["last_error"] or "") if state_row else "",
            }
        )
    # Surface profiles that have a state row but no catalog_entities yet
    # (in-progress first sync). Without this the pill would hide them
    # until the very first row landed.
    for name, state_row in state_by_profile.items():
        if name in seen:
            continue
        if valid_profiles is not None and name not in valid_profiles:
            # Same tombstone filter as above — a stale state row for a
            # deleted profile must not surface in the pill.
            continue
        state_value = str(state_row["state"])
        if state_value == "syncing":
            syncing_count += 1
        profiles.append(
            {
                "profile": name,
                "entity_count": 0,
                "last_synced_at": None,
                "age_seconds": None,
                "stale": True,
                "state": state_value,
                "total_tables": int(state_row["total_tables"] or 0),
                "processed_tables": int(state_row["processed_tables"] or 0),
                "started_at": float(state_row["started_at"]) if state_row["started_at"] else None,
                "finished_at": float(state_row["finished_at"])
                if state_row["finished_at"]
                else None,
                "last_full_sync_at": float(state_row["last_full_sync_at"])
                if state_row["last_full_sync_at"]
                else None,
                "last_error": str(state_row["last_error"] or ""),
            }
        )
        stale_count += 1
    return {
        "profiles": profiles,
        "stale_profile_count": stale_count,
        "syncing_profile_count": syncing_count,
        "stale_after_seconds": stale_after_sec,
    }


@router.post("/sync")
def trigger_catalog_sync(
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None, alias="database"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Kick off a skeleton sync for the requested profile (or every
    saved profile when omitted). Each profile gets its own daemon
    thread that walks every reachable database under it (``list_databases``
    on 2-level backends, ``list_catalogs`` on 3-level) and writes
    skeleton rows into ``catalog_entities`` while updating
    ``catalog_profile_state`` so the freshness pill can render
    progress. Returns immediately with the state-machine entry
    already flipped to ``syncing`` so the SPA polls progress
    instead of guessing.

    ``?database=`` (optional): when set, only that one database is
    refreshed under the profile — used by the sidebar's per-database
    refresh button so clicking refresh on `SAP` doesn't re-walk
    `bird_train` and `bird_train_desc`. The flag is only honoured
    when ``?profile=`` is also present; bare ``?database=`` is a
    400 because the sync key is ``(profile, database)`` not
    ``database`` alone.
    """
    import threading

    from amx.search.catalog import SearchCatalog
    from amx.search.drift import sync_profile_skeleton

    if profile:
        targets = [profile.strip()]
    else:
        if database:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="?database= requires ?profile= to scope the sync.",
            )
        profile_map = getattr(cfg, "db_profiles", None)
        targets = list(profile_map.keys()) if hasattr(profile_map, "keys") else []
    targets = [p for p in targets if p]
    if not targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No DB profile to sync. Pass ?profile=<name> or save a profile first.",
        )
    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised — cannot sync catalog yet.",
        )
    # Synchronous state flip BEFORE the thread spawns so the next
    # ``/freshness`` GET already sees ``state='syncing'`` and the
    # pill switches to its progress shape without a poll race.
    for target in targets:
        try:
            catalog.start_skeleton_sync(target, total_tables=0)
        except Exception:
            # Surface the failure to the SPA via the per-profile state
            # row rather than the API response — the pill renders the
            # error inline.
            try:
                catalog.finish_skeleton_sync(target, ok=False, error="bootstrap failed")
            except Exception:
                pass

    databases_arg: list[str] | None = [database.strip()] if database else None

    from amx.search import _skeleton_jobs

    def _spawn(target_profile: str) -> None:
        # Register the cancel slot synchronously before the thread
        # starts so a /sync/cancel call landing during thread spin-up
        # still finds an event to set.
        _skeleton_jobs.register(target_profile)

        def _runner() -> None:
            try:
                sync_profile_skeleton(cfg, target_profile, catalog, databases=databases_arg)
            except Exception as exc:  # pragma: no cover - best-effort
                try:
                    catalog.finish_skeleton_sync(target_profile, ok=False, error=str(exc))
                except Exception:
                    pass
                finally:
                    _skeleton_jobs.unregister(target_profile)

        threading.Thread(
            target=_runner,
            name=f"amx-catalog-skeleton-sync-{target_profile}",
            daemon=True,
        ).start()

    for target in targets:
        _spawn(target)
    return {
        "profiles": targets,
        "database": database or None,
        "status": "queued",
    }


@router.post("/deep-sync")
def trigger_deep_sync(
    profile: str | None = Query(default=None),
    database: str | None = Query(default=None, alias="database"),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Kick off a deep (full-profile) sync for the requested profile.

    Unlike ``POST /sync`` (skeleton — table-level rows only), this
    profiles every catalogued table (``profile_table`` + ``COUNT(*)``)
    and writes columns + row counts so the Table page renders real
    structure and counts. Slower by design, so it is a separate
    opt-in action. Runs in a daemon thread and reuses the skeleton
    state machine, so the freshness pill and ``POST /sync/cancel``
    work unchanged.

    A skeleton sync must have run first (it populates the table
    inventory this pass profiles); on an empty catalog the job
    finishes immediately with a note.
    """
    import threading

    from amx.search.catalog import SearchCatalog
    from amx.search.drift import deep_sync_profile

    if profile:
        targets = [profile.strip()]
    else:
        if database:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="?database= requires ?profile= to scope the deep sync.",
            )
        profile_map = getattr(cfg, "db_profiles", None)
        targets = list(profile_map.keys()) if hasattr(profile_map, "keys") else []
    targets = [p for p in targets if p]
    if not targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No DB profile to deep-sync. Pass ?profile=<name> or save a profile first.",
        )
    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store not initialised — cannot deep-sync catalog yet.",
        )
    for target in targets:
        try:
            catalog.start_skeleton_sync(target, total_tables=0)
        except Exception:
            try:
                catalog.finish_skeleton_sync(target, ok=False, error="bootstrap failed")
            except Exception:
                pass

    databases_arg: list[str] | None = [database.strip()] if database else None

    from amx.search import _skeleton_jobs

    def _spawn(target_profile: str) -> None:
        _skeleton_jobs.register(target_profile)

        def _runner() -> None:
            try:
                deep_sync_profile(cfg, target_profile, catalog, databases=databases_arg)
            except Exception as exc:  # pragma: no cover - best-effort
                try:
                    catalog.finish_skeleton_sync(target_profile, ok=False, error=str(exc))
                except Exception:
                    pass
                finally:
                    _skeleton_jobs.unregister(target_profile)

        threading.Thread(
            target=_runner,
            name=f"amx-catalog-deep-sync-{target_profile}",
            daemon=True,
        ).start()

    for target in targets:
        _spawn(target)
    return {
        "profiles": targets,
        "database": database or None,
        "status": "queued",
        "mode": "deep",
    }


@router.post("/sync/cancel")
def cancel_catalog_sync(body: CatalogSyncCancelRequest) -> dict[str, Any]:
    """Cooperatively cancel an in-flight skeleton sync for ``profile``.

    The running sync thread observes the cancel at its next loop
    checkpoint (per-container, per-schema, or per-table), finishes
    the in-flight table, then exits cleanly with
    ``finish_skeleton_sync(ok=False, error="cancelled")``. Rows
    already written remain in the cache.

    Returns ``{"cancelled": True}`` when a job was registered for
    ``profile``, ``{"cancelled": False}`` when nothing was running.
    """
    from amx.search import _skeleton_jobs

    profile = (body.profile or "").strip()
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile is required.",
        )
    cancelled = _skeleton_jobs.cancel(profile)
    return {"profile": profile, "cancelled": cancelled}


@router.post("/refresh")
def trigger_cache_refresh(
    body: CacheRefreshRequest,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Ad-hoc, synchronous cache refresh for a scope the user picked.

    Calls the same ``cache_refresh_executor`` the scheduler tick uses
    for ``kind='cache_refresh'`` schedules, but skips the
    ``scheduled_runs`` round-trip — this is intended for "do it right
    now" buttons (the Catalog cache page's "Sync scope…" dialog) where
    persisting a one-off schedule row would be noise. Profile must be a
    saved profile; otherwise the executor's own profile lookup raises
    and we surface the error as 400.

    Returns ``{ok: true, mode: '<scope.mode>'}`` on success so the SPA
    can render a toast. Exceptions in the executor surface as 500 with
    the underlying message so the user sees what went wrong.
    """
    from amx.runtime.worker import cache_refresh_executor

    profile_name = body.profile.strip()
    if not profile_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="profile is required.",
        )
    profile_map = getattr(cfg, "db_profiles", {}) or {}
    if profile_name not in profile_map:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No DB profile named {profile_name!r}.",
        )

    payload: dict[str, Any] = {
        "id": 0,
        "kind": "cache_refresh",
        "db_profile": profile_name,
        "database": body.database or None,
        "catalog": body.catalog or None,
        "scope_json": json.dumps(body.scope),
    }
    try:
        cache_refresh_executor(0, payload)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cache refresh failed: {exc.__class__.__name__}: {exc}",
        ) from exc
    return {
        "ok": True,
        "profile": profile_name,
        "mode": str(body.scope.get("mode") or "all"),
    }
