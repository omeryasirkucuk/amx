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

#: Last probe timestamp per profile.
_LAST_PROBE: dict[str, float] = {}
_LAST_PROBE_LOCK = threading.Lock()


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
    """
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

    if not reached_any:
        # Every container failed to connect. Surface the last error so
        # the user sees a real reason on the pill.
        catalog.start_skeleton_sync(profile, total_tables=0)
        err = last_error or _empty_schemas_error(db_backend)
        catalog.finish_skeleton_sync(profile, ok=False, error=err)
        summary["state"] = "failed"
        summary["error"] = err
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
                stamp = container or default_container
                for schema, assets in schema_assets:
                    for asset in assets:
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
        return summary

    catalog.finish_skeleton_sync(profile, ok=True)
    summary["state"] = "done"
    summary["processed"] = processed
    return summary


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
    if not fresh:
        return

    def _worker() -> None:
        for profile in fresh:
            result = _probe_one(cfg, profile, catalog_db_path)
            if force or result.drifted:
                _enqueue_sync(cfg, profile)

    thread = threading.Thread(
        target=_worker,
        name="amx-drift-probe",
        daemon=True,
    )
    thread.start()


__all__ = ["fire_drift_probe", "DriftResult"]
