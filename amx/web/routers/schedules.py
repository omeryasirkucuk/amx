"""Studio routes for scheduled runs.

Mirrors the ``amx schedule`` / ``amx scheduler`` CLI surface so the
SPA can drive the same flows: CRUD, pause/resume, run-now, tick, and
a bootstrap report consumed by the catch-up banner.

All routes are token-protected by the existing
:class:`TokenAuthMiddleware` mounted at the app level.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from amx.runtime.worker import production_run_executor, spawn_scheduled_worker
from amx.scheduler.daemon_install import detect_daemon_state
from amx.scheduler.tick import tick
from amx.storage.sqlite_store import history_store

router = APIRouter(prefix="/api/schedules", tags=["schedules"])
scheduler_router = APIRouter(prefix="/api/scheduler", tags=["scheduler"])


def _store() -> Any:
    s = history_store()
    if s is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="History store isn't initialized yet.",
        )
    return s


# ── Models ──────────────────────────────────────────────────────────


class ScheduleCreateRequest(BaseModel):
    """Payload accepted by ``POST /api/schedules``.

    ``fire_at_local`` is a wall-clock ``YYYY-MM-DDTHH:MM`` (or with
    seconds) in ``fire_at_tz``. The server converts to canonical UTC
    using ``zoneinfo``.
    """

    name: str = Field(min_length=1, max_length=200)
    fire_at_local: str = Field(description="Wall-clock fire time, e.g. '2026-12-31T09:00'.")
    fire_at_tz: str = Field(default="UTC", description="IANA tz id.")
    db_profile: str
    database: str | None = Field(
        default=None,
        description=(
            "Per-schedule DB overlay -- mirrors the ScopeTree picker's "
            "``database`` axis. Required for backends whose schema "
            "list depends on database (Postgres/MySQL/SQL Server)."
        ),
    )
    catalog: str | None = Field(
        default=None,
        description="Per-schedule catalog overlay (Unity Catalog etc.).",
    )
    scope: dict[str, Any] = Field(description="{'mode':'all|schemas|tables', ...}.")
    llm_profile: str
    review_strategy: Literal["auto", "manual"] = "auto"


class SchedulePatchRequest(BaseModel):
    name: str | None = None
    fire_at_local: str | None = None
    fire_at_tz: str | None = None
    db_profile: str | None = None
    database: str | None = None
    catalog: str | None = None
    scope: dict[str, Any] | None = None
    llm_profile: str | None = None
    review_strategy: Literal["auto", "manual"] | None = None


def _parse_fire_at(local: str, tz_name: str) -> float:
    try:
        tz = ZoneInfo(tz_name)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unknown timezone: {tz_name!r}",
        ) from exc
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            naive = datetime.strptime(local, fmt)
            break
        except ValueError:
            continue
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"unparseable fire_at_local: {local!r}",
        )
    return naive.replace(tzinfo=tz).astimezone(timezone.utc).timestamp()


def _serialise(row: dict[str, Any]) -> dict[str, Any]:
    if row is None:
        return row
    out = dict(row)
    tz = ZoneInfo(out["fire_at_tz"]) if out.get("fire_at_tz") else timezone.utc
    out["fire_at_local"] = (
        datetime.fromtimestamp(out["fire_at_utc"], tz=timezone.utc)
        .astimezone(tz)
        .strftime("%Y-%m-%dT%H:%M")
    )
    return out


# ── Schedule CRUD ───────────────────────────────────────────────────


@router.get("")
def list_schedules(
    status_filter: str | None = None,
    db_profile: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    s = _store()
    statuses = None
    if status_filter:
        statuses = [piece.strip() for piece in status_filter.split(",") if piece.strip()]
    rows = s.list_scheduled_runs(statuses=statuses, db_profile=db_profile, limit=limit)
    return {"schedules": [_serialise(r) for r in rows]}


@router.post("", status_code=status.HTTP_201_CREATED)
def create_schedule(body: ScheduleCreateRequest) -> dict[str, Any]:
    s = _store()
    fire_at_utc = _parse_fire_at(body.fire_at_local, body.fire_at_tz)
    sid = s.create_scheduled_run(
        name=body.name,
        fire_at_utc=fire_at_utc,
        fire_at_tz=body.fire_at_tz,
        db_profile=body.db_profile,
        database=body.database,
        catalog=body.catalog,
        scope_json=json.dumps(body.scope),
        llm_profile=body.llm_profile,
        review_strategy=body.review_strategy,
    )
    return _serialise(s.get_scheduled_run(sid))


@router.get("/{schedule_id}")
def get_schedule(schedule_id: int) -> dict[str, Any]:
    s = _store()
    row = s.get_scheduled_run(schedule_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")
    return _serialise(row)


@router.patch("/{schedule_id}")
def patch_schedule(schedule_id: int, body: SchedulePatchRequest) -> dict[str, Any]:
    s = _store()
    row = s.get_scheduled_run(schedule_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")
    if row["status"] not in ("pending", "paused"):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"cannot edit a schedule in status={row['status']!r}",
        )
    patch: dict[str, Any] = {}
    if body.name is not None:
        patch["name"] = body.name
    if body.fire_at_tz is not None:
        patch["fire_at_tz"] = body.fire_at_tz
    if body.fire_at_local is not None:
        tz_name = body.fire_at_tz or row["fire_at_tz"]
        patch["fire_at_utc"] = _parse_fire_at(body.fire_at_local, tz_name)
    if body.db_profile is not None:
        patch["db_profile"] = body.db_profile
    if body.database is not None:
        # Empty string means "clear the overlay" (revert to profile
        # default); persist as SQL NULL so the row matches a fresh
        # create with no database picked.
        patch["database"] = body.database or None
    if body.catalog is not None:
        patch["catalog"] = body.catalog or None
    if body.scope is not None:
        patch["scope_json"] = json.dumps(body.scope)
    if body.llm_profile is not None:
        patch["llm_profile"] = body.llm_profile
    if body.review_strategy is not None:
        patch["review_strategy"] = body.review_strategy

    try:
        s.update_scheduled_run(schedule_id, patch=patch)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialise(s.get_scheduled_run(schedule_id))


@router.post("/{schedule_id}/pause")
def pause_schedule(schedule_id: int) -> dict[str, Any]:
    s = _store()
    try:
        s.set_scheduled_run_status(schedule_id, "paused")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _serialise(s.get_scheduled_run(schedule_id))


@router.post("/{schedule_id}/resume")
def resume_schedule(schedule_id: int) -> dict[str, Any]:
    s = _store()
    try:
        s.set_scheduled_run_status(schedule_id, "pending")
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return _serialise(s.get_scheduled_run(schedule_id))


@router.post("/{schedule_id}/run-now", status_code=status.HTTP_202_ACCEPTED)
def run_now(schedule_id: int) -> dict[str, Any]:
    s = _store()
    if s.get_scheduled_run(schedule_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="not found")

    def spawn(payload: dict[str, Any]) -> int:
        return spawn_scheduled_worker(
            payload,
            store=s,
            background=True,
            run_executor=production_run_executor,
        )

    report = tick(
        store=s,
        source="manual",
        target_id=schedule_id,
        spawn_worker=spawn,
        now_utc=time.time(),
    )
    if not report.fired:
        msg = report.failed_resolution[0][1] if report.failed_resolution else "unknown error"
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=msg)
    return {"fired": report.fired}


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_schedule(schedule_id: int) -> None:
    s = _store()
    s.delete_scheduled_run(schedule_id)


# ── Scheduler engine / daemon ──────────────────────────────────────


@scheduler_router.get("/status")
def scheduler_status_endpoint() -> dict[str, Any]:
    s = _store()
    pending = s.list_scheduled_runs(statuses=["pending"], limit=1000)
    missed = s.list_scheduled_runs(statuses=["missed"], limit=1000)
    paused = s.list_scheduled_runs(statuses=["paused"], limit=1000)
    daemon = detect_daemon_state()
    return {
        "pending_count": len(pending),
        "missed_count": len(missed),
        "paused_count": len(paused),
        "next_fire": _serialise(pending[0]) if pending else None,
        "daemon": daemon,
    }


@scheduler_router.get("/bootstrap-report")
def bootstrap_report_endpoint() -> dict[str, Any]:
    """Return the bootstrap TickReport recorded at server startup.

    The SPA reads this on first load to render the catch-up banner.
    When no report has been recorded yet (e.g. test harness without
    the lifespan hook), a empty report is returned.
    """
    from amx.web import server as _server_module

    report = getattr(_server_module, "_bootstrap_report", None)
    if report is None:
        return {
            "fired": [],
            "failed_resolution": [],
            "missed_for_review": [],
            "stale_recovered": [],
        }
    return {
        "fired": report.fired,
        "failed_resolution": report.failed_resolution,
        "missed_for_review": report.missed_for_review,
        "stale_recovered": report.stale_recovered,
    }


@scheduler_router.post("/tick")
def manual_tick_endpoint() -> dict[str, Any]:
    """Admin / debug: trigger a daemon-mode tick from the SPA."""
    s = _store()

    def spawn(payload: dict[str, Any]) -> int:
        return spawn_scheduled_worker(
            payload,
            store=s,
            background=True,
            run_executor=production_run_executor,
        )

    report = tick(store=s, source="daemon", spawn_worker=spawn, now_utc=time.time())
    return {
        "fired": report.fired,
        "failed_resolution": report.failed_resolution,
        "missed_for_review": report.missed_for_review,
        "stale_recovered": report.stale_recovered,
    }


@scheduler_router.post("/install-daemon")
def scheduler_install_daemon_endpoint() -> dict[str, Any]:
    """Install the OS-level scheduler daemon on the host machine.

    This is the same one-shot operation the CLI exposes as
    ``/analyze schedule install-daemon``. Mounted on the Studio API
    so users connecting from a remote browser (Cloudflare-tunneled
    setup, mobile, etc.) can flip the daemon on without dropping
    into a terminal on the host.
    """
    from amx.scheduler.daemon_install import install_daemon

    result = install_daemon()
    return {
        "message": result.get("message", ""),
        "path": result.get("path"),
    }


@scheduler_router.post("/uninstall-daemon")
def scheduler_uninstall_daemon_endpoint() -> dict[str, Any]:
    """Remove the OS-level scheduler daemon from the host machine."""
    from amx.scheduler.daemon_install import uninstall_daemon

    result = uninstall_daemon()
    return {"message": result.get("message", "")}
