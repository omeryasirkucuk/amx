"""Background catalog-drift detection.

When a user adds, drops, or renames a table in the live DB, AMX's
persistent catalog (``catalog_entities`` / ``catalog_descriptions``)
stays at the last ``/search sync`` snapshot. Concept search reads the
catalog, so the new table is invisible to ``/ask`` until the user
manually re-syncs — a common source of "AMX says it doesn't exist
but it's right there" confusion.

This module exposes :func:`fire_drift_probe`, called by the Studio
``/api/ask`` handshake and the REPL bootstrap. It spawns a daemon
thread that:

1. For every DB profile in scope, counts ``catalog_entities`` rows.
2. Asks each profile's live connector for the current table count
   (cheap ``SELECT COUNT(*) FROM information_schema.tables`` or the
   backend-specific equivalent).
3. If the counts differ by more than ``epsilon`` (default 0), fires
   :func:`amx.search._catalog.sync.sync_table_profile_async` for that
   profile so the catalog catches up before the next question.

The probe is fire-and-forget. The current ``/ask`` call uses whatever
catalog state is there; the NEXT call reflects the new schema. Honors
``AMX_SKIP_DRIFT_PROBE=1`` so power users can opt out.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any

from amx.utils.logging import get_logger

#: Backends where the top-level container is a *catalog* (3-level
#: hierarchy: catalog -> schema -> table) rather than a *database* (2-level:
#: database -> schema -> table). The skeleton sync enumerates containers
#: with ``list_catalogs()`` for these and ``list_databases()`` for everyone
#: else, but writes the discovered name into the same ``database_name``
#: column so cache reads can be scoped uniformly.
_THREE_LEVEL_BACKENDS = frozenset({"databricks", "bigquery"})

log = get_logger("search.drift")

#: Seconds between probes for the same (process, profile). Avoids
#: hammering the live DB when the user fires several /ask calls in
#: quick succession. Memo lives in-process only.
_PROBE_COOLDOWN_SEC = 60.0

#: Skip the probe entirely when the persistent ``schemas_cache`` for
#: the profile was refreshed within this many seconds. Rationale: the
#: probe's `_live_table_count` reads through the connector's cache-first
#: path, so a recently filled cache gives the same answer as a fresh
#: round-trip. Override via ``AMX_DRIFT_PROBE_MIN_AGE_SEC``.
_SCHEMAS_CACHE_FRESH_SEC = 300.0

#: Last probe timestamp per profile.
_LAST_PROBE: dict[str, float] = {}
_LAST_PROBE_LOCK = threading.Lock()

#: Per-process counters surfaced via ``/db cache stats`` so the user
#: can verify the cache-age gate is doing its job. Keys: ``skipped``
#: (cache fresh, no probe), ``ran`` (probe actually ran).
_DRIFT_PROBE_COUNTERS: dict[str, int] = {"skipped_cache_fresh": 0, "ran": 0}


@dataclass
class DriftResult:
    """Per-profile probe outcome — surfaced for tests."""

    profile: str
    catalog_count: int
    live_count: int | None
    drifted: bool
    error: str | None = None


def _build_connector(cfg: Any, profile: str) -> Any:
    """Open a ``DatabaseConnector`` for *profile* using whatever AMX
    config we have. PR #414 referenced a non-existent
    ``amx.db.factory.build_connector`` — every drift sync silently
    failed at import. The real path is ``DatabaseConnector`` directly,
    mirroring ``amx/web/routers/live_db.py:_connector_for_scope``.
    """
    if cfg is None:
        raise RuntimeError("cfg required to open connector")
    profile_name = (profile or "").strip()
    profile_map = getattr(cfg, "db_profiles", {}) or {}
    base = profile_map.get(profile_name)
    if base is None:
        raise RuntimeError(f"No DB profile named {profile_name!r}")
    from amx.db.connector import DatabaseConnector

    return DatabaseConnector(base, profile_name=profile_name)


def _cooldown_blocks(profile: str, now: float) -> bool:
    with _LAST_PROBE_LOCK:
        last = _LAST_PROBE.get(profile)
        if last is not None and (now - last) < _PROBE_COOLDOWN_SEC:
            return True
        _LAST_PROBE[profile] = now
        return False


def _resolve_min_age_seconds() -> float:
    """Read ``AMX_DRIFT_PROBE_MIN_AGE_SEC`` if set, else return the default.

    Invalid values (non-numeric, negative) fall back to the default so a
    typo in the env never wedges the probe.
    """
    raw = os.environ.get("AMX_DRIFT_PROBE_MIN_AGE_SEC", "").strip()
    if not raw:
        return _SCHEMAS_CACHE_FRESH_SEC
    try:
        value = float(raw)
    except ValueError:
        return _SCHEMAS_CACHE_FRESH_SEC
    return value if value >= 0 else _SCHEMAS_CACHE_FRESH_SEC


def _schemas_cache_recently_refreshed(
    hs: Any,
    profile: str,
    max_age_seconds: float,
    now: float,
) -> bool:
    """``True`` when this profile's ``schemas_cache`` was refreshed within
    ``max_age_seconds``.

    The drift probe's live count is itself served through the connector's
    cache-first ``list_schemas`` / ``list_assets`` path, so a hot cache
    yields the same answer as a fresh round-trip. Skipping the probe in
    that window cuts the every-/ask DB hit without losing freshness: an
    older cache (or an explicit ``/refresh``) still falls through.
    """
    if max_age_seconds <= 0:
        return False
    try:
        with hs._connect() as conn:
            row = conn.execute(
                "SELECT MAX(fetched_at) AS ts FROM schemas_cache WHERE db_profile = ?",
                (profile,),
            ).fetchone()
    except Exception as exc:
        log.debug("schemas_cache freshness lookup failed for %s: %s", profile, exc)
        return False
    if row is None:
        return False
    ts = row["ts"] if hasattr(row, "keys") else (row[0] if row else None)
    if ts is None:
        return False
    return (now - float(ts)) < max_age_seconds


def get_drift_probe_counters() -> dict[str, int]:
    """Snapshot the drift-probe skipped / ran tallies for ``/db cache stats``."""
    return dict(_DRIFT_PROBE_COUNTERS)


def _catalog_table_count(catalog_db_path: str, profile: str) -> int:
    """Number of ``entity_kind='table'`` rows the persistent catalog
    holds for *profile*. Zero means the profile was never synced."""
    import sqlite3

    try:
        conn = sqlite3.connect(catalog_db_path, timeout=5)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT COUNT(*) AS n FROM catalog_entities
            WHERE db_profile = ? AND entity_kind = 'table'
            """,
            (profile,),
        ).fetchone()
        return int(row["n"] or 0) if row else 0
    except Exception as exc:  # pragma: no cover - best-effort
        log.debug("catalog_table_count(%s) failed: %s", profile, exc)
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _live_table_count(cfg, profile: str) -> int | None:
    """Live-DB count via the active connector. Returns ``None`` when
    the connector refuses to answer (auth blip, network down) so the
    caller treats it as "no drift signal" rather than mis-firing a
    sync against an unreachable database."""
    try:
        connector = _build_connector(cfg, profile)
    except Exception as exc:
        log.debug("build_connector(%s) failed: %s", profile, exc)
        return None
    try:
        schemas = connector.list_schemas() or []
    except Exception as exc:
        log.debug("list_schemas(%s) failed: %s", profile, exc)
        return None
    total = 0
    for schema in schemas:
        try:
            assets = connector.list_assets(schema) or []
            total += len(assets)
        except Exception as exc:
            log.debug("list_assets(%s/%s) failed: %s", profile, schema, exc)
            # Partial signal is worse than no signal — the missing
            # schema looks like a drop.
            return None
    return total


def _probe_one(cfg, profile: str, catalog_db_path: str) -> DriftResult:
    catalog_n = _catalog_table_count(catalog_db_path, profile)
    live_n = _live_table_count(cfg, profile)
    if live_n is None:
        return DriftResult(profile, catalog_n, None, False, "live count unavailable")
    drifted = live_n != catalog_n
    return DriftResult(profile, catalog_n, live_n, drifted)


def _enqueue_sync(cfg, profile: str) -> None:
    """Run a **skeleton** sync for *profile*: enumerate schemas + table
    names + asset kinds, INSERT-IGNORE the rows into
    ``catalog_entities`` so the sidebar / schedule / run pickers /
    Ask tools can stand on the catalog. The deep per-table profile
    (row counts, column stats, descriptions) is left to ``/run`` and
    ``/search sync`` — they're heavy enough that running them inside
    the drift probe was the source of the silent-partial-catalog bug.

    Always finishes with ``catalog_profile_state`` flipped to
    ``done`` or ``failed`` so the cache-first read gate and the
    freshness pill have a trustworthy signal.
    """
    try:
        from amx.search.catalog import SearchCatalog
    except Exception as exc:
        log.debug("Could not import SearchCatalog for drift sync: %s", exc)
        return
    try:
        catalog = SearchCatalog.from_history_store()
    except Exception as exc:
        log.debug("SearchCatalog.from_history_store failed: %s", exc)
        return
    if catalog is None:
        return
    try:
        sync_profile_skeleton(cfg, profile, catalog)
    except Exception as exc:
        log.warning("Drift skeleton sync failed for %s: %s", profile, exc)


def _scoped_connector(cfg, profile: str, container: str | None, is_three_level: bool) -> Any:
    """Open a connector scoped to a specific *container* (database for
    2-level backends, catalog for 3-level). ``container=None`` returns
    the profile's default-scoped connector — used to enumerate the
    container list itself.

    Delegates to :func:`_build_connector` for the default scope so the
    skeleton-sync test suite, which monkeypatches ``_build_connector``
    with a stub, still works. For a non-default container we replace
    the profile's ``DBConfig.database`` (or ``.catalog``) before
    instantiating ``DatabaseConnector`` directly — tests stub the
    enumeration path with ``list_databases`` returning an empty list,
    so this branch never runs in unit tests.
    """
    profile_name = (profile or "").strip()
    if container is None or not container:
        return _build_connector(cfg, profile_name)
    if cfg is None:
        raise RuntimeError("cfg required to open connector")
    profile_map = getattr(cfg, "db_profiles", {}) or {}
    base = profile_map.get(profile_name)
    if base is None:
        # Without a profile entry we can't overlay a container —
        # delegate so the standard "No DB profile named X" error
        # surfaces consistently.
        return _build_connector(cfg, profile_name)
    overlay = {"catalog": container} if is_three_level else {"database": container}
    try:
        scoped = replace(base, **overlay)
    except TypeError:
        # ``base`` isn't a dataclass — narrow test stub. Fall back so
        # the caller still stamps the container name onto upserts.
        return _build_connector(cfg, profile_name)
    from amx.db.connector import DatabaseConnector

    return DatabaseConnector(scoped, profile_name=profile_name)


def _empty_schemas_error(db_backend: str) -> str:
    if db_backend == "databricks":
        return (
            "Connected but no schemas were visible. Pin a catalog on the "
            "profile (Settings -> DB profile) or check that your warehouse "
            "permissions expose at least one schema."
        )
    return (
        "Connected but no schemas were visible. Check that the active "
        "database is correct and the connection user has at least "
        "``USAGE`` on one schema."
    )


def _enumerate_containers(
    cfg,
    profile: str,
    db_backend: str,
    default_container: str,
    is_three_level: bool,
) -> tuple[list[str], str | None]:
    """Return the list of containers (databases/catalogs) to walk for
    *profile*. Falls back to ``[default_container]`` (or ``[""]`` when
    the profile has nothing pinned) on any enumeration failure so a
    connector that doesn't implement ``list_databases``/``list_catalogs``
    still gets a sync of its pinned scope.

    Second item in the tuple is an error message when the enumeration
    failed in a way the caller should surface to the user; ``None``
    otherwise.
    """
    # Hard-limit scoping: when the profile pins a default container,
    # walk only that one. Never enumerate the connector — even an
    # admin user's catalog/database list would be wrong to write into
    # the cache for a profile scoped out of those containers.
    if default_container:
        return [default_container], None
    try:
        connector = _scoped_connector(cfg, profile, None, is_three_level)
    except Exception as exc:
        return [], str(exc)
    names: list[str] = []
    try:
        if is_three_level and hasattr(connector, "list_catalogs"):
            names = list(connector.list_catalogs())
        elif hasattr(connector, "list_databases"):
            names = list(connector.list_databases())
    except Exception as exc:
        log.debug("Container enumeration failed for %s: %s", profile, exc)
        names = []
    cleaned = [str(n) for n in names if n]
    if cleaned:
        return cleaned, None
    # No container list — either the backend has none (single-database
    # SQLite, MSSQL with USE locked, Databricks profile without
    # ``SHOW CATALOGS`` permission) or the call wasn't supported. Fall
    # back to whatever the profile already pins. An empty fallback
    # means the profile has no default container and the connector
    # didn't volunteer one — that path still gets one walk attempt
    # downstream and produces the actionable empty-schemas error.
    return [default_container] if default_container else [""], None


def sync_profile_skeleton(
    cfg,
    profile: str,
    catalog,
    *,
    databases: list[str] | None = None,
) -> dict:
    """Synchronous skeleton sync. Walks **every** reachable database
    (or catalog, on 3-level backends) under *profile* and upserts the
    table rows into ``catalog_entities`` with the correct
    ``database_name`` stamp so cache reads keyed by ``(profile,
    database)`` resolve to the right schemas.

    Returns a summary dict so the caller (drift probe,
    ``POST /api/catalog/sync``, or a test) can inspect the outcome.
    Never raises — every error path lands on
    ``finish_skeleton_sync(ok=False, error=...)`` so the state
    machine always terminates.

    ``databases`` (optional): when set, skip the
    ``list_databases``/``list_catalogs`` enumeration and walk exactly
    the given container names. Used by the per-database refresh
    button in the sidebar so clicking refresh on `SAP` doesn't
    re-sync `bird_train` and `bird_train_desc`.

    A cooperative cancel event registered for *profile* in
    :mod:`amx.search._skeleton_jobs` is checked at every loop head;
    when set, the in-flight table finishes, the function exits via
    ``finish_skeleton_sync(ok=False, error="cancelled")``, and the
    rows already written remain in the cache.
    """
    from amx.search import _skeleton_jobs
    from amx.storage._history_caches import purge_out_of_scope

    cancel_event = _skeleton_jobs.register(profile)
    summary: dict[str, Any] = {
        "profile": profile,
        "state": "none",
        "total": 0,
        "processed": 0,
        "error": "",
        "containers": [],
    }
    # Read backend + default container from the *target* profile, NOT
    # ``cfg.db`` (the active profile). When the user clicks Sync All
    # from an active local-postgre session, every other profile gets
    # sync'd in turn — using ``cfg.db.backend`` here would stamp the
    # postgres backend onto a Databricks failure message and produce
    # the wrong actionable hint.
    target_db = None
    if cfg is not None:
        profile_map = getattr(cfg, "db_profiles", {}) or {}
        target_db = profile_map.get((profile or "").strip())
    # Fallback to ``cfg.db`` only when the profile lookup fails (test
    # stubs with no ``db_profiles`` map).
    if target_db is None and cfg is not None:
        target_db = getattr(cfg, "db", None)
    db_backend = str(getattr(target_db, "backend", "") or "") if target_db is not None else ""
    is_three_level = db_backend in _THREE_LEVEL_BACKENDS
    default_container = ""
    if target_db is not None:
        default_container = str(
            getattr(target_db, "catalog", "")
            if is_three_level
            else getattr(target_db, "database", "")
        )
        if not default_container:
            # ``project`` is BigQuery's catalog-equivalent on some
            # profile shapes; preserve the prior fallback chain.
            default_container = str(
                getattr(target_db, "database", "")
                or getattr(target_db, "catalog", "")
                or getattr(target_db, "project", "")
                or ""
            )

    # Idempotent purge: rows from any previous unscoped sync that fall
    # outside the pinned container are deleted in one transaction so a
    # profile that just gained a pinned default never serves stale
    # rows from other databases/catalogs. Skipped for unpinned
    # profiles to preserve legacy multi-container behavior.
    if default_container:
        history_store = getattr(catalog, "history_store", None) or getattr(catalog, "_hs", None)
        if history_store is not None:
            try:
                purge_counts = purge_out_of_scope(
                    history_store,
                    db_profile=profile,
                    container=default_container,
                )
                if any(purge_counts.values()):
                    log.info(
                        "Skeleton purge for %s/%s: %s",
                        profile,
                        default_container,
                        purge_counts,
                    )
            except Exception as exc:  # pragma: no cover - best-effort
                log.warning("Skeleton purge skipped for %s: %s", profile, exc)

    # Step 1: decide which containers to walk.
    if databases:
        containers = [str(d) for d in databases if d]
        enum_error: str | None = None
    else:
        containers, enum_error = _enumerate_containers(
            cfg, profile, db_backend, default_container, is_three_level
        )
    if enum_error is not None and not containers:
        # The enumeration failed *and* we have no fallback. Surface the
        # original connector error so the freshness pill renders a
        # Retry CTA with the real reason (auth, network, missing
        # driver).
        log.warning("Skeleton sync enumerate failed for %s: %s", profile, enum_error)
        catalog.start_skeleton_sync(profile, total_tables=0)
        catalog.finish_skeleton_sync(profile, ok=False, error=enum_error)
        summary["state"] = "failed"
        summary["error"] = enum_error
        _skeleton_jobs.unregister(profile)
        return summary

    summary["containers"] = list(containers)

    # Step 2: pass 1 across every container — count tables so the UI
    # can render "Syncing 0 / N..." from the first tick. Per-container
    # connect failures are collected here; if EVERY container fails
    # the sync flips to ``failed`` with the first error as the
    # surfaced message. Partial success (one container errors, others
    # succeed) keeps the sync going so the user gets every reachable
    # row.
    per_container_plan: list[tuple[str, list[tuple[str, list]]]] = []
    total = 0
    last_error = ""
    reached_any = False
    for container in containers:
        if cancel_event.is_set():
            break
        try:
            connector = _scoped_connector(cfg, profile, container or None, is_three_level)
            schemas = connector.list_schemas() or []
        except Exception as exc:
            log.warning(
                "Skeleton sync connect failed for %s/%s: %s",
                profile,
                container or "<default>",
                exc,
            )
            last_error = str(exc)
            continue
        reached_any = True
        if not schemas:
            log.info(
                "Skeleton sync for %s/%s found no schemas - skipping",
                profile,
                container or "<default>",
            )
            per_container_plan.append((container, []))
            continue
        schema_assets: list[tuple[str, list]] = []
        for schema in schemas:
            if cancel_event.is_set():
                break
            try:
                assets = connector.list_assets(schema) or []
            except Exception as exc:
                log.warning(
                    "Skeleton sync list_assets failed for %s/%s/%s: %s",
                    profile,
                    container or "<default>",
                    schema,
                    exc,
                )
                assets = []
            schema_assets.append((schema, assets))
            total += len(assets)
        per_container_plan.append((container, schema_assets))

    if cancel_event.is_set():
        # Cancelled before pass-1 completed — surface as a clean
        # cancellation, not a connect failure.
        catalog.start_skeleton_sync(profile, total_tables=0)
        catalog.finish_skeleton_sync(profile, ok=False, error="cancelled")
        summary["state"] = "cancelled"
        _skeleton_jobs.unregister(profile)
        return summary

    if not reached_any:
        # Every container failed to connect. Surface the last error so
        # the user sees a real reason on the pill.
        catalog.start_skeleton_sync(profile, total_tables=0)
        err = last_error or _empty_schemas_error(db_backend)
        catalog.finish_skeleton_sync(profile, ok=False, error=err)
        summary["state"] = "failed"
        summary["error"] = err
        _skeleton_jobs.unregister(profile)
        return summary

    if total == 0:
        # We reached at least one container but nothing exposed a
        # schema. Treat this exactly like the old "empty schemas"
        # branch — it's a failure, not a silent success, so the
        # freshness pill renders a Retry CTA.
        err = _empty_schemas_error(db_backend)
        log.warning(
            "Skeleton sync for %s reached %d container(s) but no schemas - marking failed",
            profile,
            len(containers),
        )
        catalog.start_skeleton_sync(profile, total_tables=0)
        catalog.finish_skeleton_sync(profile, ok=False, error=err)
        summary["state"] = "failed"
        summary["error"] = err
        _skeleton_jobs.unregister(profile)
        return summary

    catalog.start_skeleton_sync(profile, total_tables=total)
    summary["state"] = "syncing"
    summary["total"] = total

    # Step 3: pass 2 — INSERT each table row with the correct
    # ``database_name`` stamp. Sharing one open SQLite connection
    # across all containers keeps the per-schema progress write in
    # the same transaction as the upserts so SQLite doesn't deadlock
    # on a second writer.
    processed = 0
    try:
        with catalog._connect() as conn:  # noqa: SLF001 — owns the catalog conn
            for container, schema_assets in per_container_plan:
                if cancel_event.is_set():
                    break
                stamp = container or default_container
                for schema, assets in schema_assets:
                    if cancel_event.is_set():
                        break
                    for asset in assets:
                        if cancel_event.is_set():
                            break
                        name, kind = _asset_name_and_kind(asset)
                        if not name:
                            continue
                        try:
                            catalog._upsert_entity(  # noqa: SLF001
                                conn,
                                db_profile=profile,
                                db_backend=db_backend,
                                database_name=stamp,
                                schema_name=schema,
                                table_name=name,
                                column_name=None,
                                entity_kind="table",
                                asset_kind=str(kind or "table"),
                            )
                            processed += 1
                        except Exception as exc:
                            # A single bad row mustn't kill the whole
                            # skeleton — log and continue.
                            log.debug(
                                "Skeleton upsert failed for %s/%s/%s.%s: %s",
                                profile,
                                stamp or "<default>",
                                schema,
                                name,
                                exc,
                            )
                    catalog.record_skeleton_progress(profile, processed, conn=conn)
    except Exception as exc:
        log.warning("Skeleton sync upsert pass failed for %s: %s", profile, exc)
        catalog.finish_skeleton_sync(profile, ok=False, error=str(exc))
        summary["state"] = "failed"
        summary["processed"] = processed
        summary["error"] = str(exc)
        _skeleton_jobs.unregister(profile)
        return summary

    if cancel_event.is_set():
        catalog.finish_skeleton_sync(profile, ok=False, error="cancelled")
        summary["state"] = "cancelled"
        summary["processed"] = processed
        summary["error"] = "cancelled"
        _skeleton_jobs.unregister(profile)
        return summary

    # Step 4: warm the per-table metadata caches so a post-sync read
    # never falls back to live DB. ``schemas_cache`` was already
    # populated as a side-effect of ``connector.list_schemas()`` in
    # pass 1 (it routes through ``_populate_catalogs_cache``).
    # ``column_comments_cache`` is NOT touched by the skeleton upsert
    # pass, so the first sidebar drill or /ask read would otherwise
    # hit live DB to fetch table + column comments. We walk per
    # (container, schema) and let the connector's bulk helper fill
    # the cache one round-trip at a time.
    schemas_warmed_ok = reached_any
    columns_warmed_ok = True
    for container, schema_assets in per_container_plan:
        if cancel_event.is_set():
            break
        try:
            connector = _scoped_connector(cfg, profile, container or None, is_three_level)
        except Exception as exc:
            log.warning(
                "Cache warm skipped, connector unavailable for %s/%s: %s",
                profile,
                container or "<default>",
                exc,
            )
            columns_warmed_ok = False
            continue
        for schema, _assets in schema_assets:
            if cancel_event.is_set():
                break
            try:
                # Bulk-fill column_comments_cache for the whole schema
                # in one round-trip when the adapter supports it. Stamp
                # the durable sync TTL so the imported comments persist
                # until the next sync rather than expiring after the
                # 1-hour browse window — otherwise the cache-only read
                # gate (get_column_comments / get_table_comment) starts
                # returning empty an hour after every sync and Studio
                # silently loses every comment this pass just imported.
                from amx.db.connector import DURABLE_COMMENT_CACHE_TTL_SECONDS

                connector._populate_schema_metadata_cache(  # noqa: SLF001
                    schema, ttl_seconds=DURABLE_COMMENT_CACHE_TTL_SECONDS
                )
            except Exception as exc:
                log.debug(
                    "Cache warm raised for %s/%s/%s: %s",
                    profile,
                    container or "<default>",
                    schema,
                    exc,
                )
                columns_warmed_ok = False

    catalog.finish_skeleton_sync(profile, ok=True)
    # Stamp the per-asset-type freshness timestamps only when the
    # corresponding warm pass actually completed for this run.
    # ``last_skeleton_sync_at`` is stamped inside finish_skeleton_sync;
    # ``last_schemas_sync_at`` / ``last_columns_sync_at`` carry the
    # "cache is hot for this profile" signal the cache-only read gate
    # in connector.list_schemas / get_column_comments depends on.
    if schemas_warmed_ok:
        try:
            catalog.mark_schemas_sync_done(profile)
        except Exception as exc:
            log.debug("mark_schemas_sync_done raised for %s: %s", profile, exc)
    if columns_warmed_ok:
        try:
            catalog.mark_columns_sync_done(profile)
        except Exception as exc:
            log.debug("mark_columns_sync_done raised for %s: %s", profile, exc)
    summary["state"] = "done"
    summary["processed"] = processed
    summary["schemas_warmed"] = bool(schemas_warmed_ok)
    summary["columns_warmed"] = bool(columns_warmed_ok)
    _skeleton_jobs.unregister(profile)
    return summary


def deep_sync_profile(
    cfg,
    profile: str,
    catalog,
    *,
    databases: list[str] | None = None,
) -> dict:
    """Full-profile sync: profile every table the skeleton already
    catalogued and write its columns + row count.

    The skeleton sync (:func:`sync_profile_skeleton`) is deliberately
    fast — it writes table-level rows only, no columns or counts. This
    is the opt-in "Deep sync": for every table already in
    ``catalog_entities`` it runs ``profile_table`` (which issues a
    ``COUNT(*)``) and ``sync_table_profile`` so the Studio Table page
    shows real column lists + row counts. It reuses the skeleton's
    inventory rather than re-walking the live DB, the skeleton state
    machine for progress (so the freshness pill renders), and the
    cooperative cancel registry. Never raises — terminal failures land
    on ``finish_skeleton_sync(ok=False)``.

    ``databases`` (optional) restricts the pass to those container
    names; ``None`` covers every database the skeleton recorded.
    """
    from amx.search import _skeleton_jobs

    cancel_event = _skeleton_jobs.register(profile)
    summary: dict[str, Any] = {"profile": profile, "state": "syncing", "processed": 0, "failed": 0}

    profile_map = getattr(cfg, "db_profiles", {}) or {}
    target_db = profile_map.get((profile or "").strip()) if hasattr(profile_map, "get") else None
    if target_db is None and cfg is not None:
        target_db = getattr(cfg, "db", None)
    db_backend = str(getattr(target_db, "backend", "") or "") if target_db is not None else ""
    is_three_level = db_backend in _THREE_LEVEL_BACKENDS

    # Pull the table inventory the skeleton already populated. Reusing
    # it avoids a second live-DB enumeration; the skeleton is a
    # prerequisite (the Studio "Deep sync" button is only offered once
    # a profile has rows).
    inventory: list[tuple[str, str, str]] = []
    try:
        with catalog._connect() as conn:  # noqa: SLF001 — same access as catalog methods
            if databases:
                placeholders = ",".join("?" for _ in databases)
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT database_name, schema_name, table_name
                    FROM catalog_entities
                    WHERE db_profile = ? AND entity_kind = 'table'
                      AND database_name IN ({placeholders})
                    ORDER BY database_name, schema_name, table_name
                    """,
                    (profile, *databases),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT DISTINCT database_name, schema_name, table_name
                    FROM catalog_entities
                    WHERE db_profile = ? AND entity_kind = 'table'
                    ORDER BY database_name, schema_name, table_name
                    """,
                    (profile,),
                ).fetchall()
        inventory = [
            (str(r["database_name"] or ""), str(r["schema_name"] or ""), str(r["table_name"] or ""))
            for r in rows
            if r["schema_name"] and r["table_name"]
        ]
    except Exception as exc:
        log.warning("Deep sync inventory read failed for %s: %s", profile, exc)
        catalog.finish_skeleton_sync(profile, ok=False, error=str(exc))
        summary["state"] = "failed"
        summary["error"] = str(exc)
        _skeleton_jobs.unregister(profile)
        return summary

    total = len(inventory)
    catalog.start_skeleton_sync(profile, total_tables=total)
    if total == 0:
        catalog.finish_skeleton_sync(profile, ok=True)
        summary["state"] = "done"
        summary["note"] = "no tables in catalog — run a skeleton sync first"
        _skeleton_jobs.unregister(profile)
        return summary

    processed = 0
    failed = 0
    # Group consecutive tables by container so one scoped connector
    # serves every table in a database.
    connector = None
    current_container: str | None = None
    for container, schema, table in inventory:
        if cancel_event.is_set():
            break
        if connector is None or container != current_container:
            try:
                connector = _scoped_connector(cfg, profile, container or None, is_three_level)
                current_container = container
            except Exception as exc:
                log.warning(
                    "Deep sync connector failed for %s/%s: %s", profile, container or "<default>", exc
                )
                connector = None
                failed += 1
                continue
        try:
            prof = connector.profile_table(schema, table, sample_size=0)
            # Backends that block the profiler's COUNT(*) when no cheap
            # stat is available (Databricks sets
            # full_scan_when_row_count_unknown=False to avoid surprise
            # full scans) leave row_count at 0. Deep sync is the opt-in
            # "pay the cost" path, so fetch an exact count here when the
            # profiler couldn't — that's the whole reason the user ran it.
            if not getattr(prof, "row_count", 0):
                exact = _exact_row_count(connector, schema, table)
                if exact is not None:
                    prof.row_count = exact
            catalog.sync_table_profile(
                db_profile=profile,
                db_backend=db_backend,
                database_name=container,
                profile=prof,
                query_usage={},
            )
            processed += 1
        except Exception as exc:
            failed += 1
            log.warning("Deep sync profile failed for %s.%s: %s", schema, table, exc)
        # Progress on its own short-lived connection — never hold a
        # catalog connection open across sync_table_profile (which
        # opens its own), or SQLite WAL deadlocks.
        catalog.record_skeleton_progress(profile, processed)

    if cancel_event.is_set():
        catalog.finish_skeleton_sync(profile, ok=False, error="cancelled")
        summary["state"] = "cancelled"
    else:
        catalog.finish_skeleton_sync(profile, ok=True)
        summary["state"] = "done"
    summary["processed"] = processed
    summary["failed"] = failed
    summary["total"] = total
    # Team sharing: when the shared history store is active, push this
    # profile's freshly-profiled structural rows up so teammates inherit
    # the COUNT(*) work. Best-effort — a shared-store failure must never
    # turn a successful local deep sync into a failure. When the store is
    # OFF the active history store has no ``.shared`` handle, so this is
    # a silent no-op (the gating the user asked for).
    if summary["state"] == "done":
        summary["shared_pushed"] = _push_catalog_if_shared(profile)
    _skeleton_jobs.unregister(profile)
    return summary


def deep_sync_one_table(
    cfg,
    profile: str,
    *,
    schema: str,
    table: str,
    database: str | None = None,
) -> dict:
    """Deep-sync a SINGLE table: profile it (columns + exact COUNT(*))
    and write the structural row to the local catalog (and shared store
    when active).

    Powers the per-asset "Deep sync" button on the Table page so a user
    can refresh one table's columns + row count without re-profiling the
    whole profile. Synchronous (one table is quick) and returns the
    outcome for the caller to render. Never raises — EVERY failure path
    (including catalog/connector setup) lands in the returned dict so
    the HTTP layer surfaces a clear message instead of an opaque 500.
    """
    try:
        return _deep_sync_one_table_impl(
            cfg, profile, schema=schema, table=table, database=database
        )
    except Exception as exc:  # pragma: no cover - belt-and-braces
        log.warning("deep_sync_one_table failed for %s.%s: %s", schema, table, exc)
        return {"ok": False, "error": str(exc)}


def _deep_sync_one_table_impl(
    cfg,
    profile: str,
    *,
    schema: str,
    table: str,
    database: str | None = None,
) -> dict:
    profile_map = getattr(cfg, "db_profiles", {}) or {}
    target_db = profile_map.get((profile or "").strip()) if hasattr(profile_map, "get") else None
    if target_db is None and cfg is not None:
        target_db = getattr(cfg, "db", None)
    db_backend = str(getattr(target_db, "backend", "") or "") if target_db is not None else ""
    is_three_level = db_backend in _THREE_LEVEL_BACKENDS

    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        return {"ok": False, "error": "history store not initialised"}
    try:
        connector = _scoped_connector(cfg, profile, database or None, is_three_level)
    except Exception as exc:
        return {"ok": False, "error": f"connector unavailable: {exc}"}
    try:
        prof = connector.profile_table(schema, table, sample_size=0)
        if not getattr(prof, "row_count", 0):
            exact = _exact_row_count(connector, schema, table)
            if exact is not None:
                prof.row_count = exact
        catalog.sync_table_profile(
            db_profile=profile,
            db_backend=db_backend,
            database_name=database or "",
            profile=prof,
            query_usage={},
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    _push_catalog_if_shared(profile)
    return {
        "ok": True,
        "schema": schema,
        "table": table,
        "row_count": int(getattr(prof, "row_count", 0) or 0),
        "column_count": len(getattr(prof, "columns", []) or []),
    }


def _exact_row_count(connector: Any, schema: str, table: str) -> int | None:
    """Run an exact ``SELECT COUNT(*)`` for one table, best-effort.

    Used by deep sync to fill the row count on backends whose profiler
    skips the COUNT(*) (e.g. Databricks, where
    full_scan_when_row_count_unknown is False). On Delta the count is
    usually answered from file metadata, so it is cheap in practice.
    Returns ``None`` on any failure so a single uncountable table never
    aborts the deep sync.
    """
    try:
        from sqlalchemy import text as _text

        adapter = connector._adapter  # noqa: SLF001
        fqn = adapter.fully_qualified_name(schema, table)
        with connector.engine.connect() as conn:
            value = conn.execute(_text(f"SELECT COUNT(*) FROM {fqn}")).scalar()
        return int(value) if value is not None else None
    except Exception as exc:  # pragma: no cover - best-effort
        log.debug("Exact row count failed for %s.%s: %s", schema, table, exc)
        return None


def _push_catalog_if_shared(profile: str) -> int:
    """Push a profile's local catalog rows to the shared store when one
    is active. Returns the number of rows pushed (0 when no shared store
    / on any failure). Never raises."""
    try:
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is None or not hasattr(hs, "shared") or not hasattr(hs, "local"):
            return 0  # store disabled → local-only, no push
        from amx.storage.migration import push_catalog_to_shared

        return push_catalog_to_shared(hs.local, hs.shared, db_profile=profile)
    except Exception as exc:  # pragma: no cover - best-effort
        log.warning("Catalog push to shared store skipped for %s: %s", profile, exc)
        return 0


def _asset_name_and_kind(asset: Any) -> tuple[str, str]:
    """Connectors return ``list_assets`` rows in two shapes — a tuple
    ``(name, kind)`` or a single string ``name``. Normalize so the
    skeleton sync handles both."""
    if isinstance(asset, tuple) and len(asset) >= 2:
        name = str(asset[0] or "")
        kind = getattr(asset[1], "value", None) or asset[1]
        return name, str(kind or "table")
    if isinstance(asset, str):
        return asset, "table"
    return str(getattr(asset, "name", "") or ""), str(getattr(asset, "kind", "") or "table")


def fire_drift_probe(cfg, profiles: Iterable[str], *, force: bool = False) -> None:
    """Spawn a daemon thread that runs the drift probe for each
    profile in scope. No-op when ``AMX_SKIP_DRIFT_PROBE=1`` is set or
    the catalog file isn't available yet (fresh install with no
    history store).

    ``force=True`` is used by the manual ``/api/catalog/sync`` route:
    the per-profile cooldown is bypassed AND the sync runs even when
    the live / catalog counts match. Without force, an auto-probe
    that already ran in the last 60s would silently absorb a manual
    click and the freshness pill would never reflect the user's
    intent.
    """
    if os.environ.get("AMX_SKIP_DRIFT_PROBE", "").strip() in ("1", "true", "yes"):
        return
    profile_list = [p for p in (profiles or []) if p]
    if not profile_list:
        return
    try:
        from amx.storage.sqlite_store import history_store

        hs = history_store()
    except Exception:
        return
    if hs is None:
        return
    catalog_db_path = str(getattr(hs, "db_path", "") or "")
    if not catalog_db_path:
        return
    now = time.time()
    if force:
        # Manual click — skip the cooldown gate entirely and refresh
        # the timestamp so the next auto-probe still respects pacing
        # relative to this manual trigger.
        with _LAST_PROBE_LOCK:
            for profile in profile_list:
                _LAST_PROBE[profile] = now
        fresh = list(profile_list)
    else:
        fresh = [p for p in profile_list if not _cooldown_blocks(p, now)]
        # Second gate: even outside the 60s cooldown, skip when the
        # persistent schemas_cache for this profile was filled in the
        # last ``min_age`` seconds. The probe reads through the same
        # cache, so a hot row would yield the same result as a fresh
        # round-trip. An explicit /refresh invalidates the cache row
        # (see ``invalidate_schemas_cache``) and naturally falls through.
        min_age = _resolve_min_age_seconds()
        if min_age > 0 and fresh:
            kept: list[str] = []
            for p in fresh:
                if _schemas_cache_recently_refreshed(hs, p, min_age, now):
                    _DRIFT_PROBE_COUNTERS["skipped_cache_fresh"] += 1
                else:
                    kept.append(p)
            fresh = kept
    if not fresh:
        return

    def _worker() -> None:
        for profile in fresh:
            _DRIFT_PROBE_COUNTERS["ran"] += 1
            result = _probe_one(cfg, profile, catalog_db_path)
            if force or result.drifted:
                _enqueue_sync(cfg, profile)

    thread = threading.Thread(
        target=_worker,
        name="amx-drift-probe",
        daemon=True,
    )
    thread.start()


__all__ = ["fire_drift_probe", "DriftResult", "get_drift_probe_counters"]
