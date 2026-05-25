"""Change-triggered schedule dispatcher.

AMX has no push-based CDC; the lightweight, native way to notice a new
asset is the thing the product does constantly — **sync**. So a
change-triggered schedule (``scheduled_runs.trigger='change'``) has no
fire time. Instead, every time a top-level sync completes
(``sync_profile_skeleton`` / ``deep_sync_profile`` called from manual
Sync, a ``cache_refresh`` schedule, or the drift probe), this module is
invoked to:

1. find active change schedules for the synced profile,
2. diff ``catalog_entities`` for assets whose ``first_synced_at`` is newer
   than the schedule's ``last_checked_at`` watermark, within its watched
   scope,
3. fire a ``missing_only`` analyze run narrowed to exactly the tables that
   gained something, reusing :func:`spawn_scheduled_worker` +
   ``production_run_executor`` (so generation, deep-sync-first, and
   auto-apply all behave identically to a time schedule),
4. advance the watermark so the same assets never re-fire.

The fired run's *own* internal deep-sync passes ``dispatch_changes=False``
so it can't recursively re-trigger detection; and a schedule that is
currently ``running`` is excluded from selection, so a sync mid-run can't
double-fire it.
"""

from __future__ import annotations

import json
import time
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("scheduler.change_trigger")


def _watched_schemas(scope_json: str | None) -> list[str] | None:
    """Schema filter for a watcher's scope. ``None`` means "whole profile"
    (mode ``all`` / database-level watch); a list restricts to those
    schemas (mode ``schemas``)."""
    if not scope_json:
        return None
    try:
        obj = json.loads(scope_json)
    except (TypeError, ValueError):
        return None
    if obj.get("mode") == "schemas":
        return [str(s) for s in (obj.get("schemas") or []) if s]
    return None


def _narrowed_scope_json(watch_scope_json: str | None, tables: list[tuple[str, str]]) -> str:
    """Build the run scope for the fired analyze run: exactly the tables
    that gained a new asset, in ``missing_only`` mode so only the
    description-less columns (the new ones, plus any pre-existing gaps) are
    generated. ``deep_first`` is forced on so a brand-new table's columns
    are profiled before generation."""
    deep_first = True
    if watch_scope_json:
        try:
            deep_first = bool(json.loads(watch_scope_json).get("deep_first", True))
        except (TypeError, ValueError):
            deep_first = True
    return json.dumps(
        {
            "mode": "tables",
            "tables": [{"schema": s, "table": t} for s, t in tables],
            "missing_only": True,
            "deep_first": deep_first,
        }
    )


def dispatch_after_sync(
    profile: str,
    cfg: Any,
    *,
    databases: list[str] | None = None,
) -> int:
    """Evaluate change schedules for ``profile`` after a sync completed.

    Returns the number of schedules fired. Never raises — a failure here
    must not break the sync that triggered it; everything is logged.
    """
    try:
        from amx.search.catalog import SearchCatalog
        from amx.storage.sqlite_store import history_store

        store = history_store()
        catalog = SearchCatalog.from_history_store()
        if store is None or catalog is None:
            return 0
        schedules = store.list_change_schedules(db_profile=profile)
    except Exception:
        log.exception("change-trigger: could not load schedules for profile=%s", profile)
        return 0

    fired = 0
    for sched in schedules:
        try:
            fired += _evaluate_one(store, catalog, sched, databases=databases)
        except Exception:
            log.exception(
                "change-trigger: evaluation failed for schedule #%s",
                sched.get("id"),
            )
    return fired


def _evaluate_one(
    store: Any,
    catalog: Any,
    sched: dict[str, Any],
    *,
    databases: list[str] | None,
) -> int:
    schedule_id = int(sched["id"])
    profile = str(sched.get("db_profile") or "")
    watermark = sched.get("last_checked_at")
    watch_scope_json = sched.get("scope_json")
    schemas = _watched_schemas(watch_scope_json)
    # The watcher's database overlay narrows detection to one database; a
    # sync that touched other databases shouldn't fire it.
    sched_db = sched.get("database") or None
    if sched_db and databases is not None and sched_db not in databases:
        return 0

    # Capture the checkpoint BEFORE querying so assets inserted after this
    # instant stay for the next sync rather than being silently skipped.
    checkpoint = time.time()
    new_rows = catalog.new_entities_since(
        profile,
        float(watermark) if watermark is not None else None,
        database=sched_db,
        schemas=schemas,
    )

    # Collapse new tables and new columns to the set of owning tables; the
    # missing_only filter on the fired run handles per-column selection.
    tables: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for r in new_rows:
        key = (str(r["schema_name"]), str(r["table_name"]))
        if key[1] and key not in seen:
            seen.add(key)
            tables.append(key)

    if not tables:
        store.advance_change_watermark(schedule_id, checkpoint)
        return 0

    payload = dict(sched)
    payload["scope_json"] = _narrowed_scope_json(watch_scope_json, tables)
    # Advance the watermark BEFORE firing: the fired run's own deep-sync
    # will stamp new first_synced_at values, and advancing first (plus the
    # running-guard in list_change_schedules) prevents this watcher from
    # reacting to assets it created itself.
    store.advance_change_watermark(schedule_id, checkpoint)

    from amx.runtime.worker import production_run_executor, spawn_scheduled_worker

    run_id = spawn_scheduled_worker(
        payload,
        store=store,
        run_executor=production_run_executor,
        background=True,
    )
    log.info(
        "change-trigger: schedule #%s fired run #%s for %s new table(s) in profile %s",
        schedule_id,
        run_id,
        len(tables),
        profile,
    )
    return 1
