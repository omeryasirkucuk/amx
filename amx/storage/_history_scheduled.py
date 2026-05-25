"""Scheduled-runs state machine extracted from :mod:`amx.storage.sqlite_store`.

Eleven methods that drive the scheduled_runs table and the
tick-engine's claim_due_schedule / arm_next_fire transitions. Each
function takes the store as ``hs`` and reuses its ``_lock`` +
``_connect()`` plumbing. No DDL touched.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import TYPE_CHECKING, Any

from amx.utils.logging import get_logger

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore

log = get_logger("storage.sqlite.scheduled")


def create_scheduled_run(
    hs: SQLiteHistoryStore,
    *,
    name: str,
    fire_at_utc: float,
    fire_at_tz: str,
    db_profile: str,
    scope_json: str,
    llm_profile: str,
    review_strategy: str,
    database: str | None = None,
    catalog: str | None = None,
    extra_args_json: str | None = None,
    kind: str = "analyze",
    cron_expr: str | None = None,
    trigger: str = "time",
) -> int:
    """Insert a new scheduled_runs row in status='pending'.

    ``database`` and ``catalog`` carry the picker's overlay so the
    scheduler can rebuild the same connection at fire time (mirrors
    the ``/api/live/schemas`` picker that ScopeTree drives). Both
    are nullable: legacy rows + backends that don't expose a
    database/catalog axis (e.g. SQLite single-file profiles) keep
    the profile default.

    ``kind`` discriminates between the legacy analyze-run schedule
    ('analyze') and the Catalog-Freshness cache refresh
    ('cache_refresh'). ``cron_expr`` is NULL for one-shot fires and
    a valid croniter expression for recurring schedules — the tick
    engine re-arms the row from the cron after each fire.

    ``trigger`` is 'time' (default — fired by the tick when
    ``fire_at_utc`` elapses) or 'change' (fired by the post-sync
    dispatcher when a new asset appears under the watched scope;
    ``fire_at_utc`` carries a harmless placeholder and is never used).
    """
    now = time.time()
    trigger = str(trigger or "time")
    # A change watcher's watermark starts at creation time so it only fires
    # for assets that appear AFTER it was set up — otherwise its first sync
    # would treat the entire pre-existing catalog as "new". Time schedules
    # leave it NULL.
    last_checked_at = now if trigger == "change" else None
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO scheduled_runs (
                name, fire_at_utc, fire_at_tz, status,
                db_profile, database, catalog,
                scope_json, llm_profile, review_strategy,
                extra_args_json, kind, cron_expr, trigger,
                created_at, updated_at, last_checked_at
            ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                fire_at_utc,
                fire_at_tz,
                db_profile,
                database,
                catalog,
                scope_json,
                llm_profile,
                review_strategy,
                extra_args_json,
                str(kind or "analyze"),
                cron_expr,
                trigger,
                now,
                now,
                last_checked_at,
            ),
        )
        return int(cur.lastrowid or 0)


def get_scheduled_run(hs: SQLiteHistoryStore, schedule_id: int) -> dict[str, Any] | None:
    """Return the full row as a dict, or None if missing."""
    with hs._connect() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM scheduled_runs WHERE id=?", (schedule_id,)).fetchone()
    return dict(row) if row else None


def list_scheduled_runs(
    hs: SQLiteHistoryStore,
    *,
    statuses: list[str] | None = None,
    db_profile: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """List rows sorted by fire_at_utc ASC with optional filters."""
    clauses: list[str] = []
    params: list[Any] = []
    if statuses:
        placeholders = ",".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)
    if db_profile is not None:
        clauses.append("db_profile = ?")
        params.append(db_profile)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(int(limit))
    sql = "SELECT * FROM scheduled_runs" + where + " ORDER BY fire_at_utc ASC LIMIT ?"
    with hs._connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def list_due_pending_schedules(
    hs: SQLiteHistoryStore,
    *,
    now_utc: float,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Return rows that are pending and whose fire time has elapsed.

    Read-only: does not transition status. Used by the bootstrap
    tick path to surface "missed while AMX was closed" entries.
    """
    with hs._connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM scheduled_runs
            WHERE status = 'pending'
              AND fire_at_utc <= ?
            ORDER BY fire_at_utc ASC
            LIMIT ?
            """,
            (now_utc, int(limit)),
        ).fetchall()
    return [dict(r) for r in rows]


def list_change_schedules(
    hs: SQLiteHistoryStore,
    *,
    db_profile: str | None = None,
) -> list[dict[str, Any]]:
    """Active change-triggered schedules (``trigger='change'``).

    Only rows the dispatcher may fire are returned — ``status='pending'``
    (watching). A row currently ``running`` is intentionally excluded so a
    second sync mid-run can't double-fire it. Optionally scoped to one
    ``db_profile``.
    """
    clauses = ["trigger = 'change'", "status = 'pending'"]
    params: list[Any] = []
    if db_profile is not None:
        clauses.append("db_profile = ?")
        params.append(db_profile)
    sql = "SELECT * FROM scheduled_runs WHERE " + " AND ".join(clauses) + " ORDER BY id ASC"
    with hs._connect() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def rearm_change_schedule(hs: SQLiteHistoryStore, schedule_id: int) -> bool:
    """Return a change schedule to ``pending`` (watching) after a fire.

    A change-triggered schedule never reaches a terminal state on its own
    — it keeps watching until the user deletes it. Like
    :func:`arm_next_fire`, this writes ``status='pending'`` directly
    (bypassing the time-based state machine, which has no running→pending
    edge). Returns False for non-change schedules so the caller falls back
    to the normal terminal transition.
    """
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            "SELECT trigger FROM scheduled_runs WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        if row is None or str(row[0]) != "change":
            return False
        conn.execute(
            "UPDATE scheduled_runs SET status = 'pending', updated_at = ? WHERE id = ?",
            (time.time(), schedule_id),
        )
        return True


def advance_change_watermark(hs: SQLiteHistoryStore, schedule_id: int, ts: float) -> None:
    """Move a change schedule's ``last_checked_at`` watermark forward.

    Called after the dispatcher has evaluated (and possibly fired on) the
    assets newer than the previous watermark, so the same assets are never
    re-evaluated on a later sync. Never moves the watermark backwards.
    """
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            UPDATE scheduled_runs
            SET last_checked_at = ?, updated_at = ?
            WHERE id = ? AND (last_checked_at IS NULL OR last_checked_at < ?)
            """,
            (ts, time.time(), schedule_id, ts),
        )


def update_scheduled_run(
    hs: SQLiteHistoryStore,
    schedule_id: int,
    *,
    patch: dict[str, Any],
) -> None:
    """Apply a whitelisted patch to a scheduled_runs row.

    Only the fields in ``_SCHEDULE_UPDATABLE_FIELDS`` can be
    modified through this path -- status changes go through
    :meth:`set_scheduled_run_status`, and id/created_at/fired_at/
    triggered_run_id/last_error are owned by the engine.
    """
    if not patch:
        return
    unknown = (
        set(patch)
        - hs._SCHEDULE_UPDATABLE_FIELDS
        - {
            "status",
            "id",
            "created_at",
            "updated_at",
            "fired_at",
            "triggered_run_id",
            "last_error",
        }
    )
    if unknown:
        raise ValueError(f"unknown patch field(s): {sorted(unknown)}")
    forbidden = set(patch) - hs._SCHEDULE_UPDATABLE_FIELDS
    if forbidden:
        raise ValueError(
            f"forbidden patch field(s) for update_scheduled_run: "
            f"{sorted(forbidden)} -- use a dedicated method"
        )
    cols = sorted(patch)
    set_clause = ", ".join(f"{c} = ?" for c in cols)
    params: list[Any] = [patch[c] for c in cols]
    params.append(time.time())
    params.append(schedule_id)
    with hs._lock, hs._connect() as conn:
        conn.execute(
            f"UPDATE scheduled_runs SET {set_clause}, updated_at = ? WHERE id = ?",
            params,
        )


def set_scheduled_run_status(
    hs: SQLiteHistoryStore,
    schedule_id: int,
    status: str,
    *,
    last_error: str | None = None,
    fired_at: float | None = None,
    triggered_run_id: int | None = None,
) -> None:
    """Transition a schedule to *status*, enforcing the state machine."""
    if status not in hs._SCHEDULE_TRANSITIONS:
        raise ValueError(f"unknown status: {status!r}")
    now = time.time()
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            "SELECT status FROM scheduled_runs WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"no scheduled_runs row with id={schedule_id}")
        current = str(row[0])
        allowed = hs._SCHEDULE_TRANSITIONS.get(current, set())
        if status not in allowed and status != current:
            raise ValueError(f"illegal transition: {current!r} -> {status!r}")
        sets = ["status = ?", "updated_at = ?"]
        params: list[Any] = [status, now]
        if last_error is not None:
            sets.append("last_error = ?")
            params.append(last_error)
        if fired_at is not None:
            sets.append("fired_at = ?")
            params.append(fired_at)
        if triggered_run_id is not None:
            sets.append("triggered_run_id = ?")
            params.append(triggered_run_id)
        params.append(schedule_id)
        conn.execute(
            f"UPDATE scheduled_runs SET {', '.join(sets)} WHERE id = ?",
            params,
        )


def delete_scheduled_run(hs: SQLiteHistoryStore, schedule_id: int) -> None:
    """Hard-delete a scheduled_runs row; write an audit event.

    Idempotent: deleting a non-existent id is a no-op (no error,
    no audit row).
    """
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            "SELECT name FROM scheduled_runs WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        if row is None:
            return
        name = str(row[0])
        conn.execute("DELETE FROM scheduled_runs WHERE id = ?", (schedule_id,))
        conn.execute(
            """
            INSERT INTO app_events (
                created_at, event_type, status, command, details_json
            ) VALUES (?, 'schedule.deleted', 'ok', 'schedule.rm', ?)
            """,
            (
                time.time(),
                json.dumps({"schedule_id": schedule_id, "name": name}),
            ),
        )


def claim_due_schedule(hs: SQLiteHistoryStore, *, now_utc: float) -> int | None:
    """Atomically transition the oldest due pending schedule to running.

    Returns the claimed schedule id, or None if none are due. Safe
    under concurrent calls from multiple threads -- the
    ``WHERE status='pending'`` predicate on the UPDATE turns a lost
    race into a return of None for the loser.

    Change-triggered schedules (``trigger='change'``) are excluded: they
    have no meaningful ``fire_at_utc`` and are fired only by the post-sync
    dispatcher, never by the time loop.
    """
    now = time.time()
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM scheduled_runs
            WHERE status = 'pending' AND trigger = 'time' AND fire_at_utc <= ?
            ORDER BY fire_at_utc ASC
            LIMIT 1
            """,
            (now_utc,),
        ).fetchone()
        if row is None:
            return None
        sid = int(row[0])
        cur = conn.execute(
            """
            UPDATE scheduled_runs
            SET status = 'running', fired_at = ?, updated_at = ?
            WHERE id = ? AND status = 'pending'
            """,
            (now, now, sid),
        )
        if cur.rowcount == 1:
            return sid
        return None


def arm_next_fire(hs: SQLiteHistoryStore, schedule_id: int) -> float | None:
    """Re-arm a recurring schedule to its next ``fire_at_utc``.

    Reads ``cron_expr`` from the row; if NULL or invalid, leaves
    the row alone (one-shot schedules complete after a single fire
    — they should be left at ``status='completed'`` by the tick
    path). Otherwise computes the next fire timestamp via
    ``croniter.get_next(float)`` and flips ``status`` back to
    ``'pending'`` so the tick loop picks it up next cycle.

    Returns the new ``fire_at_utc`` on success, or None when the
    row had no usable cron expression (caller treats that as
    one-shot done).
    """
    try:
        from croniter import croniter
    except Exception as exc:
        log.warning("croniter unavailable; cannot re-arm schedule %s: %s", schedule_id, exc)
        return None
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            "SELECT cron_expr FROM scheduled_runs WHERE id = ?",
            (schedule_id,),
        ).fetchone()
        if row is None:
            return None
        expr = row[0]
        if not expr:
            return None
        try:
            next_at = float(croniter(str(expr), time.time()).get_next(float))
        except Exception as exc:
            log.warning(
                "Invalid cron_expr %r on schedule %s, leaving completed: %s",
                expr,
                schedule_id,
                exc,
            )
            return None
        conn.execute(
            """
            UPDATE scheduled_runs
            SET status = 'pending', fire_at_utc = ?, updated_at = ?, last_error = NULL
            WHERE id = ?
            """,
            (next_at, time.time(), schedule_id),
        )
        return next_at


def profile_has_active_cache_refresh_schedule(hs: SQLiteHistoryStore, db_profile: str) -> bool:
    """``True`` when the profile has any non-terminal cache-refresh
    schedule. The 1-week auto-refresh sweeper uses this to skip
    profiles that already have user-managed schedules — the user's
    schedule is authoritative."""
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM scheduled_runs
            WHERE db_profile = ?
              AND kind = 'cache_refresh'
              AND status IN ('pending', 'running')
            LIMIT 1
            """,
            (str(db_profile or ""),),
        ).fetchone()
    return row is not None


def set_run_schedule_link(hs: SQLiteHistoryStore, run_id: int, schedule_id: int) -> None:
    """Attach a scheduled_runs id to an existing analysis_runs row.

    Idempotent: writing the same link twice has no effect, and a
    missing run_id is a no-op (callers can be defensive without a
    preflight check).
    """
    with hs._lock, hs._connect() as conn:
        conn.execute(
            "UPDATE analysis_runs SET triggered_by_schedule_id = ? WHERE id = ?",
            (schedule_id, run_id),
        )
