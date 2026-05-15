"""Production worker entry for scheduled runs.

The scheduler engine (``amx.scheduler.tick``) accepts an injectable
``spawn_worker`` callable. This module provides the default
implementation that integrates with the local history store.

The full Orchestrator integration -- building the LLM client,
resolving the live DB profile, looping every table in the resolved
scope, persisting alternatives -- is a separate large concern that
benefits from per-table heartbeat plumbing inside the orchestrator
itself. For the initial cut shipped here, ``spawn_scheduled_worker``
performs the steps the rest of the pipeline depends on:

* Create an ``analysis_runs`` row with ``command='schedule'`` and
  attach the originating ``scheduled_runs`` id via
  ``set_run_schedule_link``. Status starts as ``running``;
  ``last_heartbeat_at`` is set to the current time so the
  stale-recovery sweep does not immediately reclaim it.
* Hand control to the (pluggable) per-run executor and update the
  history row + schedule status on completion.
* On exception, mark both rows as failed with the error message so
  the user sees a clean failure in ``amx schedule list`` / Studio.

The pluggable executor (``run_orchestrator_impl``) defaults to a
small no-op stub that simply marks the run as complete -- enough for
the CLI ``schedule run-now`` command, the daemon-cron path, and end-
to-end tests to confirm the full state-machine flow. Wiring the real
Orchestrator (with full LLM + DB resolution) is a follow-up that
benefits from an awake reviewer.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

log = logging.getLogger("amx.runtime.worker")


class _HistoryStore(Protocol):
    """The subset of the history store used by the worker."""

    def create_run(self, **kwargs: Any) -> int: ...

    def set_run_schedule_link(self, run_id: int, schedule_id: int) -> None: ...

    def update_run_heartbeat(self, run_id: int, *, now_utc: float | None = ...) -> None: ...

    def finish_run(self, run_id: int, **kwargs: Any) -> None: ...

    def set_scheduled_run_status(
        self,
        schedule_id: int,
        status: str,
        *,
        last_error: str | None = ...,
        fired_at: float | None = ...,
        triggered_run_id: int | None = ...,
    ) -> None: ...

    def get_run(self, run_id: int) -> dict[str, Any] | None: ...

    def get_run_results(
        self,
        run_id: int,
        *,
        unevaluated_only: bool = ...,
    ) -> list[dict[str, Any]]: ...


# Pluggable per-run executor. Default is the small no-op stub below;
# callers can swap in a real Orchestrator runner in a follow-up PR.
RunExecutor = Callable[[int, dict[str, Any]], None]
"""Signature: (analysis_runs_id, schedule_payload) -> None.

Raises any exception to indicate failure; the worker translates this
into ``status='failed'`` on both rows.
"""


def default_run_executor(run_id: int, payload: dict[str, Any]) -> None:
    """No-op executor used by tests and as a safe fallback.

    Performs no real metadata-discovery work; logs a clear line so
    anyone tracing a run can tell it picked up the stub. Production
    call sites (tick, CLI run-now, Studio run-now) inject
    :func:`production_run_executor` instead.
    """
    log.warning(
        "amx.runtime.worker.default_run_executor (no-op): scheduled "
        "run %s marked complete WITHOUT running metadata discovery. "
        "Schedule payload: %s",
        run_id,
        json.dumps(
            {
                k: payload.get(k)
                for k in (
                    "id",
                    "name",
                    "db_profile",
                    "llm_profile",
                    "review_strategy",
                )
            }
        ),
    )


def production_run_executor(run_id: int, payload: dict[str, Any]) -> None:
    """Real per-table Orchestrator drive for a scheduled run.

    Loads the saved :class:`AMXConfig`, builds a one-shot
    DatabaseConnector + LLMProvider against the schedule's chosen
    profiles (without mutating the user's active config), resolves the
    schedule's saved scope against the live DB, and calls
    :meth:`Orchestrator.process_table` for every reachable
    ``(schema, table)`` pair.

    Failure modes that bubble up as exceptions:

    * Schedule references a profile that has been deleted since
      creation. ``KeyError`` from ``cfg.db_profiles[...]``.
    * Connector / LLM provider rejects the saved settings. Raised
      from inside the agent stack.
    * Live DB does not expose any schema named in the schedule's
      scope. We log per-schema and skip; if every schema is missing
      we raise so the caller can mark the schedule failed.

    The orchestrator writes its own per-result rows into
    ``analysis_runs``; the surrounding worker is responsible for the
    schedule-side state transition.
    """
    # Local imports keep ``amx.runtime.worker`` lazily loaded -- agent
    # / DB / LLM stacks are heavy and tests that don't need them
    # shouldn't pay the import cost on every collection scan.
    from dataclasses import replace

    from amx.agents.orchestrator import Orchestrator
    from amx.config import AMXConfig
    from amx.db.connector import DatabaseConnector
    from amx.llm.provider import LLMProvider

    schedule_id = int(payload.get("id") or 0)
    db_profile_name = str(payload.get("db_profile") or "")
    llm_profile_name = str(payload.get("llm_profile") or "")
    if not db_profile_name or not llm_profile_name:
        raise ValueError(f"schedule #{schedule_id} missing db_profile or llm_profile")

    cfg = AMXConfig.load()
    db_cfg = cfg.db_profiles.get(db_profile_name)
    if db_cfg is None:
        raise KeyError(
            f"DB profile '{db_profile_name}' (from schedule #{schedule_id}) "
            "no longer exists. Edit the schedule or recreate the profile."
        )
    llm_cfg = cfg.llm_profiles.get(llm_profile_name)
    if llm_cfg is None:
        raise KeyError(
            f"LLM profile '{llm_profile_name}' (from schedule #{schedule_id}) "
            "no longer exists. Edit the schedule or recreate the profile."
        )

    # Overlay the schedule's saved (database, catalog) picks on top of
    # the profile so we connect to the exact DB the user picked in the
    # ScopeTree -- the live-schemas picker uses the same overlay
    # pattern in ``amx/web/routers/live_db.py::_connector_for_scope``.
    # Without this, a Postgres profile with three DBs would always
    # fire against whichever DB happens to be the profile default, so
    # ``airline.<table>`` resolution raised NoSuchTableError.
    overlay: dict[str, Any] = {}
    overlay_database = payload.get("database")
    overlay_catalog = payload.get("catalog")
    if overlay_database:
        overlay["database"] = str(overlay_database)
    if overlay_catalog:
        overlay["catalog"] = str(overlay_catalog)
    scoped_cfg = replace(db_cfg, **overlay) if overlay else db_cfg

    db = DatabaseConnector(scoped_cfg, profile_name=db_profile_name)
    llm = LLMProvider(llm_cfg)
    orchestrator = Orchestrator(
        db,
        llm,
        run_id=run_id,
        search_profile=db_profile_name,
    )

    # When the schedule was created with column-level picks, plumb the
    # explicit column list through orchestrator.column_overrides so
    # process_table restricts its per-table loop to exactly those
    # columns (mirrors the CLI "Column scope" picker's behaviour).
    column_overrides = _scope_column_overrides(payload.get("scope_json"))
    if column_overrides:
        orchestrator.column_overrides = column_overrides

    scope = _resolve_live_scope(payload.get("scope_json"), db)
    if not scope:
        log.warning(
            "production_run_executor: schedule #%s produced empty live scope. Nothing to do.",
            schedule_id,
        )
        return

    log.info(
        "production_run_executor: schedule #%s firing across %s table(s) in %s schema(s).",
        schedule_id,
        sum(len(ts) for ts in scope.values()),
        len(scope),
    )

    # Heartbeat ticker: the per-table ``process_table`` call can spend
    # minutes inside a single LLM batch, well past the stale-recovery
    # threshold (default 300s). Without this, ``recover_stale_runs``
    # marks the still-running row as ``failed`` mid-flight, even
    # though work continues to land in ``run_results``. The ticker
    # is a daemon thread so it terminates cleanly when the executor
    # returns; the ``stop`` event lets the main loop signal "done"
    # so we don't keep beating a finished row.
    from amx.storage.sqlite_store import history_store

    hs = history_store()
    stop_beat = threading.Event()

    def _heartbeat_tick() -> None:
        while not stop_beat.is_set():
            if hs is not None:
                try:
                    hs.update_run_heartbeat(run_id)
                except Exception:  # noqa: BLE001 - never crash the ticker
                    log.exception("heartbeat ticker failed for run_id=%s", run_id)
            # Beat every ~60s -- well under the 300s stale threshold
            # so a single slow table doesn't slip past one missed beat.
            stop_beat.wait(60.0)

    beat_thread = threading.Thread(
        target=_heartbeat_tick,
        name=f"amx-heartbeat-{run_id}",
        daemon=True,
    )
    beat_thread.start()

    per_table_errors: list[str] = []
    processed = 0
    try:
        for schema, tables in scope.items():
            for table in tables:
                processed += 1
                if hs is not None:
                    try:
                        hs.update_run_heartbeat(run_id)
                    except Exception:  # noqa: BLE001
                        pass
                try:
                    orchestrator.process_table(schema, table, interactive_review=False)
                except Exception as exc:  # noqa: BLE001 - keep going on per-table errors
                    log.exception(
                        "production_run_executor: %s.%s failed under schedule #%s",
                        schema,
                        table,
                        schedule_id,
                    )
                    per_table_errors.append(f"{schema}.{table}: {type(exc).__name__}: {exc}")
    finally:
        stop_beat.set()

    # If every table errored, surface a real failure so the user sees
    # the cause in Studio instead of a misleading "completed / 0
    # results" row. Partial failures are logged but do not flip the
    # run -- per-table run_results that did land are still useful.
    if per_table_errors and len(per_table_errors) == processed:
        summary = "; ".join(per_table_errors[:5])
        if len(per_table_errors) > 5:
            summary += f"; (+{len(per_table_errors) - 5} more)"
        raise RuntimeError(f"All {processed} table(s) in scope failed. {summary}")


def _scope_column_overrides(
    scope_json: str | None,
) -> dict[tuple[str, str], set[str]]:
    """Pull (schema, table) -> {columns} out of a column-scope payload.

    Returns an empty dict for every other scope mode -- the
    orchestrator's column_overrides map is only meaningful when the
    schedule was created with explicit per-column picks.
    """
    if not scope_json:
        return {}
    try:
        obj = json.loads(scope_json)
    except (TypeError, ValueError):
        return {}
    if obj.get("mode") != "columns":
        return {}
    out: dict[tuple[str, str], set[str]] = {}
    for item in obj.get("columns", []) or []:
        if not isinstance(item, dict):
            continue
        schema = str(item.get("schema") or "")
        table = str(item.get("table") or "")
        column = str(item.get("column") or "")
        if not schema or not table or not column:
            continue
        out.setdefault((schema, table), set()).add(column)
    return out


def _resolve_live_scope(scope_json: str | None, db: Any) -> dict[str, list[str]]:
    """Expand a schedule's saved scope_json against the live database.

    Mirrors the four scope modes ``_parse_scope`` already understands
    (``all`` / ``schemas`` / ``tables`` / ``columns``) but, unlike that
    helper, talks to the live DB to enumerate everything reachable
    under ``mode='all'`` and ``mode='schemas'``. Missing entities are
    dropped with a warning rather than failing the whole run.
    """
    if not scope_json:
        return {}
    try:
        obj = json.loads(scope_json)
    except (TypeError, ValueError):
        return {}
    mode = obj.get("mode")
    out: dict[str, list[str]] = {}
    if mode == "all":
        for schema in db.list_schemas():
            tables = [
                name for name, kind in db.list_assets(schema) if kind.name.upper() not in {"COLUMN"}
            ]
            if tables:
                out[schema] = tables
        return out
    if mode == "schemas":
        for schema in obj.get("schemas", []) or []:
            try:
                tables = [
                    name
                    for name, kind in db.list_assets(str(schema))
                    if kind.name.upper() not in {"COLUMN"}
                ]
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "scope schema '%s' could not be enumerated: %s",
                    schema,
                    exc,
                )
                continue
            if tables:
                out[str(schema)] = tables
        return out
    if mode == "tables":
        for item in obj.get("tables", []) or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if schema and table:
                out.setdefault(schema, []).append(table)
        return out
    if mode == "columns":
        # Column-level picks fan out to (schema, table) pairs; the
        # orchestrator's pre-existing column_overrides path narrows
        # the actual per-column work, but we don't wire that here in
        # the initial cut. Future revision can set
        # ``orchestrator.column_overrides`` from this payload.
        seen: set[tuple[str, str]] = set()
        for item in obj.get("columns", []) or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if not schema or not table:
                continue
            if (schema, table) in seen:
                continue
            seen.add((schema, table))
            out.setdefault(schema, []).append(table)
        return out
    return {}


def cache_refresh_executor(run_id: int, payload: dict[str, Any]) -> None:
    """Re-populate the Catalog Freshness caches for a scheduled refresh.

    Mirrors the manual ``Sync all`` / per-schema refresh affordances
    in the Studio sidebar but for a schedule-driven trigger. Branches
    on ``scope.mode``:

    * ``all`` — call :func:`sync_profile_skeleton` against the whole
      ``(profile, database)`` (or the entire profile when no database
      overlay is set).
    * ``schemas`` — for each named schema invalidate the column-
      comments cache and re-list assets so the bulk path warms it
      again; warm comments per table.
    * ``tables`` / ``columns`` — same warm path restricted to the
      exact (schema, table) pairs the user picked. Column-level
      picks collapse to their owning table.

    Per-asset failures are caught and logged so one bad table doesn't
    abort the rest of the schedule's work.
    """
    from amx.config import AMXConfig
    from amx.db.connector import DatabaseConnector
    from amx.search.catalog import SearchCatalog
    from amx.search.drift import sync_profile_skeleton

    schedule_id = int(payload.get("id") or 0)
    profile_name = str(payload.get("db_profile") or "")
    if not profile_name:
        raise ValueError(f"schedule #{schedule_id} missing db_profile for cache refresh")
    database_overlay = payload.get("database") or None
    catalog_overlay = payload.get("catalog") or None

    cfg = AMXConfig.load()
    db_cfg = cfg.db_profiles.get(profile_name)
    if db_cfg is None:
        raise KeyError(
            f"DB profile '{profile_name}' (from schedule #{schedule_id}) "
            "no longer exists. Edit the schedule or recreate the profile."
        )

    from dataclasses import replace as _dc_replace

    overlay: dict[str, Any] = {}
    if database_overlay:
        overlay["database"] = str(database_overlay)
    if catalog_overlay:
        overlay["catalog"] = str(catalog_overlay)
    scoped_cfg = _dc_replace(db_cfg, **overlay) if overlay else db_cfg
    connector = DatabaseConnector(scoped_cfg, profile_name=profile_name)

    scope_raw = payload.get("scope_json")
    try:
        scope_obj = json.loads(scope_raw) if isinstance(scope_raw, str) and scope_raw else {}
    except (TypeError, ValueError):
        scope_obj = {}
    mode = str(scope_obj.get("mode") or "all")

    log.info(
        "cache_refresh_executor: schedule #%s firing on profile=%s mode=%s",
        schedule_id,
        profile_name,
        mode,
    )

    if mode == "all":
        catalog = SearchCatalog.from_history_store()
        if catalog is None:
            raise RuntimeError("History store unavailable; cache refresh aborted.")
        databases_arg = [str(database_overlay)] if database_overlay else None
        sync_profile_skeleton(cfg, profile_name, catalog, databases=databases_arg)
        return

    # Collect (schema, table | None) work units. schemas → whole-schema.
    work: list[tuple[str, str | None]] = []
    if mode == "schemas":
        for s in scope_obj.get("schemas") or []:
            if isinstance(s, str) and s:
                work.append((s, None))
    elif mode == "tables":
        for item in scope_obj.get("tables") or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if schema and table:
                work.append((schema, table))
    elif mode == "columns":
        seen_tables: set[tuple[str, str]] = set()
        for item in scope_obj.get("columns") or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if not schema or not table:
                continue
            key = (schema, table)
            if key in seen_tables:
                continue
            seen_tables.add(key)
            work.append((schema, table))

    for schema, table in work:
        try:
            connector.invalidate_column_comments_cache(schema=schema, table=table)
        except Exception as exc:
            log.debug(
                "cache_refresh_executor: invalidate failed for %s.%s: %s",
                schema,
                table,
                exc,
            )
        if table is None:
            try:
                assets = list(connector.list_assets(schema))
            except Exception as exc:
                log.warning(
                    "cache_refresh_executor: list_assets(%s) failed: %s",
                    schema,
                    exc,
                )
                assets = []
            for asset_name, _kind in assets:
                try:
                    connector.get_table_comment(schema, asset_name)
                    connector.get_column_comments(schema, asset_name)
                except Exception as exc:
                    log.debug(
                        "cache_refresh_executor: warm %s.%s failed: %s",
                        schema,
                        asset_name,
                        exc,
                    )
        else:
            try:
                connector.get_table_comment(schema, table)
                connector.get_column_comments(schema, table)
                connector.list_column_profiles(schema, table)
            except Exception as exc:
                log.debug(
                    "cache_refresh_executor: warm %s.%s failed: %s",
                    schema,
                    table,
                    exc,
                )


def spawn_scheduled_worker(
    payload: dict[str, Any],
    *,
    store: _HistoryStore,
    run_executor: RunExecutor | None = None,
    background: bool = True,
) -> int:
    """Create an ``analysis_runs`` row for the given schedule and run it.

    Returns the new ``analysis_runs.id`` immediately. When
    ``background=True`` (the production default for the daemon path)
    the executor runs in its own daemon thread so the tick loop is
    not blocked. ``background=False`` runs synchronously -- useful
    for the CLI ``schedule run-now`` flow where the user wants their
    invocation to block until the run finishes.

    *payload* is the full ``scheduled_runs`` row as returned by
    :meth:`SQLiteHistoryStore.get_scheduled_run`.
    """
    # Discriminate by ``kind``: cache_refresh schedules invalidate the
    # catalog cache via the connector, not the LLM/agent stack. The
    # legacy ``analyze`` kind keeps the caller-supplied executor (the
    # daemon path injects ``production_run_executor``).
    kind = str(payload.get("kind") or "analyze")
    if kind == "cache_refresh":
        executor = cache_refresh_executor
    else:
        executor = run_executor or default_run_executor
    scope = _parse_scope(payload.get("scope_json"))

    run_id = store.create_run(
        command="schedule",
        mode="metadata",
        db_backend="(scheduled)",  # resolved by the real executor
        db_profile=str(payload.get("db_profile") or ""),
        llm_provider="(scheduled)",
        llm_model="(scheduled)",
        scope=scope,
        review_strategy=str(payload.get("review_strategy") or "auto"),
        llm_profile=str(payload.get("llm_profile") or ""),
    )
    schedule_id = int(payload["id"])
    store.set_run_schedule_link(run_id, schedule_id)
    # First heartbeat: keeps the stale-recovery sweep from immediately
    # reclaiming the row before the executor's first internal beat.
    store.update_run_heartbeat(run_id)
    # If the caller (tick's daemon path) has already transitioned the
    # schedule to ``running`` via claim_due_schedule, this is a no-op
    # (running -> running is accepted by the state machine). When the
    # worker is invoked standalone (CLI ``schedule run-now`` or tests),
    # we drive the transition here so the lifecycle is well-defined
    # regardless of entry point.
    try:
        store.set_scheduled_run_status(schedule_id, "running", fired_at=time.time())
    except ValueError:
        # Already in a terminal state -- nothing we can do; the
        # completion handler below will hit the same wall and skip.
        pass

    def _drive() -> None:
        try:
            executor(run_id, payload)
        except BaseException as exc:  # noqa: BLE001 - propagate to history
            log.exception("scheduled worker failed for schedule_id=%s", schedule_id)
            _mark_failed(store, run_id=run_id, schedule_id=schedule_id, error=str(exc))
            return
        _mark_completed(store, run_id=run_id, schedule_id=schedule_id)

    if background:
        threading.Thread(
            target=_drive,
            name=f"amx-scheduled-run-{run_id}",
            daemon=True,
        ).start()
    else:
        _drive()
    return run_id


def _parse_scope(scope_json: str | None) -> dict[str, list[str]]:
    """Turn the stored ``scope_json`` into the {schema: [table, ...]}
    shape ``create_run`` expects.

    The stored payload uses ``{"mode":"schemas|tables|all", ...}``;
    until the live resolver lands the worker passes through what it
    can extract and lets the (future) executor resolve the rest.
    """
    if not scope_json:
        return {}
    try:
        obj = json.loads(scope_json)
    except (TypeError, ValueError):
        return {}
    mode = obj.get("mode")
    if mode == "schemas":
        return {s: [] for s in obj.get("schemas", []) if isinstance(s, str)}
    if mode == "tables":
        out: dict[str, list[str]] = {}
        for item in obj.get("tables", []) or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if not schema or not table:
                continue
            out.setdefault(schema, []).append(table)
        return out
    if mode == "columns":
        # Column-level scope. The coarse ``analysis_runs.scope`` dict
        # only carries (schema, table) granularity, so we collapse
        # duplicate column entries on the same table into a single
        # table entry. The column list itself rides in
        # ``settings_json`` so the real executor (follow-up) can
        # restrict its per-column work to exactly what the user picked.
        out_cols: dict[str, list[str]] = {}
        seen: set[tuple[str, str]] = set()
        for item in obj.get("columns", []) or []:
            if not isinstance(item, dict):
                continue
            schema = str(item.get("schema") or "")
            table = str(item.get("table") or "")
            if not schema or not table:
                continue
            key = (schema, table)
            if key in seen:
                continue
            seen.add(key)
            out_cols.setdefault(schema, []).append(table)
        return out_cols
    return {}


def _mark_completed(store: _HistoryStore, *, run_id: int, schedule_id: int) -> None:
    """Finalise analysis_runs + schedule rows on success.

    The run row's status reflects the run *outcome* (``success`` vs
    ``ready_for_review``) using the same demotion rule the manual
    ``analyze.run`` path applies in ``amx/web/routers/runs.py``: a clean
    run with results still waiting in the pending review queue is not a
    success, it's ``ready_for_review``. The schedule row keeps the
    lifecycle status ``completed`` — that column describes the schedule's
    fire lifecycle, not the run's outcome.
    """
    now = time.time()

    final_status = "success"
    try:
        run_row = store.get_run(run_id)
        if run_row is not None:
            applied = int(run_row.get("applied_count") or 0)
            pending_count = len(store.get_run_results(run_id, unevaluated_only=True))
            if pending_count > 0 and applied == 0:
                final_status = "ready_for_review"
    except Exception:  # noqa: BLE001 - fall back to "success" on lookup failure
        log.exception("post-run status demotion lookup failed for run_id=%s", run_id)

    try:
        store.finish_run(
            run_id,
            status=final_status,
            metrics={},
            tokens={},
            results={},
        )
    except BaseException:  # noqa: BLE001
        log.exception("finish_run failed for run_id=%s", run_id)
    if _try_rearm_recurring(store, schedule_id):
        return
    try:
        store.set_scheduled_run_status(
            schedule_id,
            "completed",
            triggered_run_id=run_id,
            fired_at=now,
        )
    except ValueError:
        # Already terminal -- the tick's manual path may have raced us.
        pass


def _mark_failed(
    store: _HistoryStore,
    *,
    run_id: int,
    schedule_id: int,
    error: str,
) -> None:
    """Finalise analysis_runs + schedule rows on failure."""
    truncated = error[:1000]
    try:
        store.finish_run(
            run_id,
            status="failed",
            metrics={},
            tokens={},
            results={},
            error_text=truncated,
        )
    except BaseException:  # noqa: BLE001
        log.exception("finish_run failed for run_id=%s", run_id)
    # Recurring schedules re-arm even on failure — the user's intent
    # is "keep refreshing every N hours"; a single bad fire doesn't
    # cancel future ones. The failed analysis_runs row still carries
    # the diagnostic detail.
    if _try_rearm_recurring(store, schedule_id):
        return
    try:
        store.set_scheduled_run_status(
            schedule_id,
            "failed",
            last_error=truncated,
            triggered_run_id=run_id,
        )
    except ValueError:
        pass


def _try_rearm_recurring(store: _HistoryStore, schedule_id: int) -> bool:
    """Call ``store.arm_next_fire`` for recurring schedules.

    Returns True when the row was re-armed (the caller should skip
    the terminal status transition). False when the row has no cron
    expression or the store helper isn't available — the caller then
    falls back to setting a terminal status.
    """
    helper = getattr(store, "arm_next_fire", None)
    if helper is None:
        return False
    try:
        next_at = helper(schedule_id)
    except Exception:
        log.exception("arm_next_fire raised for schedule_id=%s", schedule_id)
        return False
    return bool(next_at)
