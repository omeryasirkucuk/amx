"""One-shot migration of local SQLite history → shared warehouse store.

Used by ``/history-store migrate-from-local``. Walks every table in
foreign-key order, converts INT primary keys (local SQLite) into
UUIDs (shared schema), records the original ``local_id`` + ``hostname``
on each shared row so the dual-write coordinator can find the right
shared row when later UPDATEs fire from this machine.

Idempotent. The shared store rejects rows whose ``(hostname, local_id)``
already exists for the given table by skipping them — which means
re-running the migration after a partial failure picks up where it
left off rather than duplicating data.
"""

from __future__ import annotations

import contextlib
import json
import socket
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, insert, select

from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.utils.logging import get_logger

log = get_logger("storage.migration")


def _ts(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _hostname() -> str:
    try:
        return socket.gethostname() or ""
    except Exception:
        return ""


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    # ``dict(row)`` works because ``sqlite3.Row`` supports the mapping
    # protocol via ``keys()``. The explicit cast avoids ruff's
    # SIM118 (``.keys()`` removable) which doesn't apply here.
    return dict(row)


def _existing_uuid(
    shared: SQLAlchemyHistoryStore, table_name: str, hostname: str, local_id: int
) -> str | None:
    """Return the UUID for a previously-migrated row, if any."""
    table = shared._md.tables[f"{shared.schema}.{table_name}"]
    with shared.engine.begin() as conn:
        row = conn.execute(
            select(table.c.id).where(
                and_(table.c.hostname == hostname, table.c.local_id == local_id)
            )
        ).fetchone()
    return str(row[0]) if row else None


def migrate_local_to_shared(
    local: SQLiteHistoryStore,
    shared: SQLAlchemyHistoryStore,
    *,
    progress: Any | None = None,
) -> dict[str, int]:
    """Copy ``analysis_runs``, ``run_results`` and ``app_events`` from local
    SQLite into the shared SQLAlchemy store.

    Returns a stats dict ``{table: rows_copied}``. ``app_events`` includes
    every event-type row even if newer than the migration started — the
    SQLite store is append-only so a second run drops duplicates by
    ``(hostname, local_id)``.
    """
    stats: dict[str, int] = {
        "analysis_runs": 0,
        "run_results": 0,
        "app_events": 0,
    }
    # Use the shared store's hostname so subsequent ``find_run_uuid_by_local_id``
    # lookups (which scope by ``shared._hostname``) resolve. In production
    # the two values match (both call ``socket.gethostname()``); the
    # explicit assignment matters for tests that override the store's
    # hostname.
    host = getattr(shared, "_hostname", None) or _hostname()
    started = time.time()

    # Build INT → UUID maps once so run_results can reference the
    # parent run's freshly-minted UUID without re-querying per row.
    run_id_map: dict[int, str] = {}

    # ── analysis_runs ─────────────────────────────────────────────────────
    log.info("Migrating analysis_runs from %s …", local.db_path)
    with local._connect() as conn:
        rows = conn.execute("SELECT * FROM analysis_runs ORDER BY id").fetchall()

    table_runs = shared._md.tables[f"{shared.schema}.analysis_runs"]
    for row in rows:
        d = _row_to_dict(row)
        local_id = int(d.get("id") or 0)
        existing = _existing_uuid(shared, "analysis_runs", host, local_id)
        if existing:
            run_id_map[local_id] = existing
            continue
        new_uuid = str(uuid.uuid4())
        run_id_map[local_id] = new_uuid
        with shared.engine.begin() as sconn:
            sconn.execute(
                insert(table_runs).values(
                    id=new_uuid,
                    started_at=_ts(d.get("started_at")) or datetime.now(timezone.utc),
                    ended_at=_ts(d.get("ended_at")),
                    duration_sec=d.get("duration_sec"),
                    status=str(d.get("status") or ""),
                    command=str(d.get("command") or ""),
                    mode=d.get("mode"),
                    db_backend=d.get("db_backend"),
                    db_profile=d.get("db_profile"),
                    llm_provider=d.get("llm_provider"),
                    llm_model=d.get("llm_model"),
                    scope_json=_loads_or_none(d.get("scope_json")),
                    metrics_json=_loads_or_none(d.get("metrics_json")),
                    tokens_json=_loads_or_none(d.get("tokens_json")),
                    results_json=_loads_or_none(d.get("results_json")),
                    error_text=d.get("error_text"),
                    selected_count=int(d.get("selected_count") or 0),
                    planned_count=int(d.get("planned_count") or 0),
                    processed_count=int(d.get("processed_count") or 0),
                    applied_count=int(d.get("applied_count") or 0),
                    review_strategy=d.get("review_strategy"),
                    llm_profile=d.get("llm_profile"),
                    doc_profile=d.get("doc_profile"),
                    code_profile=d.get("code_profile"),
                    settings_json=_loads_or_none(d.get("settings_json")),
                    created_by=shared._username,
                    hostname=host,
                    client_version=shared._client_version,
                    local_id=local_id,
                )
            )
        stats["analysis_runs"] += 1
        if progress is not None:
            with contextlib.suppress(Exception):
                progress("analysis_runs", stats["analysis_runs"])

    # ── run_results ───────────────────────────────────────────────────────
    log.info("Migrating run_results …")
    with local._connect() as conn:
        rows = conn.execute("SELECT * FROM run_results ORDER BY id").fetchall()

    table_results = shared._md.tables[f"{shared.schema}.run_results"]
    for row in rows:
        d = _row_to_dict(row)
        local_id = int(d.get("id") or 0)
        existing = _existing_uuid(shared, "run_results", host, local_id)
        if existing:
            continue
        local_run_id = int(d.get("run_id") or 0)
        run_uuid = run_id_map.get(local_run_id)
        if not run_uuid:
            log.warning(
                "Skipping run_results.id=%s — its run_id=%s is not in the migrated set.",
                local_id,
                local_run_id,
            )
            continue
        with shared.engine.begin() as sconn:
            sconn.execute(
                insert(table_results).values(
                    id=str(uuid.uuid4()),
                    run_id=run_uuid,
                    saved_at=_ts(d.get("saved_at")) or datetime.now(timezone.utc),
                    schema_name=str(d.get("schema_name") or ""),
                    table_name=str(d.get("table_name") or ""),
                    column_name=d.get("column_name"),
                    asset_kind=str(d.get("asset_kind") or "table"),
                    source=str(d.get("source") or "unknown"),
                    confidence=str(d.get("confidence") or "medium"),
                    logprob_score=d.get("logprob_score"),
                    raw_logprob=d.get("raw_logprob"),
                    token_count=d.get("token_count"),
                    model_version=str(d.get("model_version") or ""),
                    reasoning=d.get("reasoning"),
                    alternatives_json=_loads_or_none(d.get("alternatives_json")) or [],
                    evaluated_at=_ts(d.get("evaluated_at")),
                    applied_at=_ts(d.get("applied_at")),
                    chosen_description=d.get("chosen_description"),
                    evaluation=d.get("evaluation"),
                    catalog_status=str(d.get("catalog_status") or ""),
                    catalog_indexed_at=_ts(d.get("catalog_indexed_at")),
                    db_applied_status=str(d.get("db_applied_status") or ""),
                    effective_source_kind=str(d.get("effective_source_kind") or ""),
                    superseded_at=_ts(d.get("superseded_at")),
                    rejection_reason=str(d.get("rejection_reason") or ""),
                    hostname=host,
                    local_id=local_id,
                )
            )
        stats["run_results"] += 1
        if progress is not None:
            with contextlib.suppress(Exception):
                progress("run_results", stats["run_results"])

    # ── app_events ────────────────────────────────────────────────────────
    log.info("Migrating app_events …")
    with local._connect() as conn:
        rows = conn.execute("SELECT * FROM app_events ORDER BY id").fetchall()

    table_events = shared._md.tables[f"{shared.schema}.app_events"]
    for row in rows:
        d = _row_to_dict(row)
        local_id = int(d.get("id") or 0)
        # app_events uses an event-id-as-local-id surrogate; idempotency
        # is keyed off (hostname, local_id) the same way as the other tables.
        with shared.engine.begin() as sconn:
            event_match = sconn.execute(
                select(table_events.c.id).where(
                    and_(
                        table_events.c.hostname == host,
                        table_events.c.command == str(d.get("command") or ""),
                        table_events.c.event_type == str(d.get("event_type") or ""),
                        table_events.c.created_at == _ts(d.get("created_at")),
                    )
                )
            ).fetchone()
            if event_match:
                continue
            sconn.execute(
                insert(table_events).values(
                    id=str(uuid.uuid4()),
                    created_at=_ts(d.get("created_at")) or datetime.now(timezone.utc),
                    event_type=str(d.get("event_type") or ""),
                    status=str(d.get("status") or ""),
                    command=str(d.get("command") or ""),
                    details_json=_loads_or_none(d.get("details_json")) or {},
                    created_by=shared._username,
                    hostname=host,
                    client_version=shared._client_version,
                )
            )
        stats["app_events"] += 1
        if progress is not None:
            with contextlib.suppress(Exception):
                progress("app_events", stats["app_events"])

    log.info(
        "Migration finished in %.1fs: %s",
        time.time() - started,
        ", ".join(f"{k}={v}" for k, v in stats.items()),
    )
    return stats


def _loads_or_none(value: Any) -> Any:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


__all__ = ["migrate_local_to_shared"]
