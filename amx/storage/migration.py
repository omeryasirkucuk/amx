"""Bidirectional migration between local SQLite and the shared store.

Two migrations live here, both idempotent:

* ``migrate_local_to_shared`` — push: copy this machine's local
  SQLite rows up to the team's shared warehouse. Used by
  ``/history-store migrate-from-local`` (and offered automatically
  the first time a user enables shared mode if they have a local
  history). UUIDs are minted on the shared side; ``hostname +
  local_id`` is recorded so future UPDATEs from this machine can
  find the right shared row.

* ``pull_shared_to_local`` — pull: copy teammates' rows DOWN from
  the shared warehouse into local SQLite so ``/history list``
  surfaces team activity, not just this machine's. Triggered by
  ``/history-store enable`` when bootstrap finds pre-existing rows
  on the team store, and exposed as a picker action so users can
  re-sync at any time. Idempotency is keyed on the local
  ``shared_uuid`` column — re-running the pull only inserts
  rows whose UUID is not already present locally.
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

from sqlalchemy import and_, insert, inspect, select, text
from sqlalchemy.engine import Engine

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
    log.info("Migrating analysis_runs from %s ...", local.db_path)
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
    log.info("Migrating run_results ...")
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
                "Skipping run_results.id=%s  --  its run_id=%s is not in the migrated set.",
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
    log.info("Migrating app_events ...")
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


def pull_shared_to_local(
    local: SQLiteHistoryStore,
    shared: SQLAlchemyHistoryStore,
    *,
    progress: Any | None = None,
) -> dict[str, int]:
    """Pull rows from the shared store down into local SQLite.

    Reverse of :func:`migrate_local_to_shared`. Filters on hostname so
    only OTHER machines' rows are copied (this machine's runs already
    exist locally). Idempotent: each pulled row records the source
    ``shared_uuid`` on the new local row, and re-running skips any
    UUID already present.

    Returns ``{table: rows_inserted}``. Tables in pull order:

    1. ``analysis_runs`` — assigns a fresh local INT id, records
       ``shared_uuid`` for dedupe, preserves ``created_by``,
       ``hostname``, ``client_version`` so ``/history list`` can
       render team attribution.
    2. ``run_results`` — looks up the parent run's *new* local INT
       id from a UUID→INT map built in step 1 and writes results
       under that.
    3. ``app_events`` — append-only; dedupes on
       ``(hostname, command, event_type, created_at)`` because
       events have no UUID locally.
    """
    stats: dict[str, int] = {
        "analysis_runs": 0,
        "run_results": 0,
        "app_events": 0,
    }
    host = getattr(shared, "_hostname", None) or _hostname()
    started = time.time()

    log.info("Pulling shared analysis_runs (excluding host=%r)...", host)
    runs = shared.iter_runs_by_other_hosts(exclude_hostname=host)
    if not runs:
        log.info("No other-host runs found in shared store.")
        return stats

    # Map shared UUID -> new local INT id for FK rewriting on results.
    uuid_to_local: dict[str, int] = {}

    with local._lock:
        for r in runs:
            shared_uuid = str(r.get("id") or "")
            if not shared_uuid:
                continue
            with local._connect() as conn:
                # Idempotency: skip rows we've already pulled.
                existing = conn.execute(
                    "SELECT id FROM analysis_runs WHERE shared_uuid = ?",
                    (shared_uuid,),
                ).fetchone()
                if existing is not None:
                    uuid_to_local[shared_uuid] = int(existing[0])
                    continue
                started_at = r.get("started_at")
                started_ts = _dt_to_ts(started_at) or time.time()
                ended_at = _dt_to_ts(r.get("ended_at"))
                cur = conn.execute(
                    """
                    INSERT INTO analysis_runs (
                        started_at, ended_at, duration_sec, status, command, mode,
                        db_backend, db_profile, llm_provider, llm_model,
                        scope_json, metrics_json, tokens_json, results_json,
                        error_text, selected_count, planned_count,
                        processed_count, applied_count, review_strategy,
                        llm_profile, doc_profile, code_profile, settings_json,
                        created_by, hostname, client_version, shared_uuid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        started_ts,
                        ended_at,
                        r.get("duration_sec"),
                        str(r.get("status") or ""),
                        str(r.get("command") or ""),
                        r.get("mode"),
                        r.get("db_backend"),
                        r.get("db_profile"),
                        r.get("llm_provider"),
                        r.get("llm_model"),
                        _dump_or_none(r.get("scope_json")),
                        _dump_or_none(r.get("metrics_json")),
                        _dump_or_none(r.get("tokens_json")),
                        _dump_or_none(r.get("results_json")),
                        r.get("error_text"),
                        int(r.get("selected_count") or 0),
                        int(r.get("planned_count") or 0),
                        int(r.get("processed_count") or 0),
                        int(r.get("applied_count") or 0),
                        r.get("review_strategy"),
                        r.get("llm_profile"),
                        r.get("doc_profile"),
                        r.get("code_profile"),
                        _dump_or_none(r.get("settings_json")),
                        r.get("created_by"),
                        r.get("hostname"),
                        r.get("client_version"),
                        shared_uuid,
                    ),
                )
                local_id = int(cur.lastrowid)
            uuid_to_local[shared_uuid] = local_id
            stats["analysis_runs"] += 1
            if progress is not None:
                with contextlib.suppress(Exception):
                    progress("analysis_runs", stats["analysis_runs"])

    if not uuid_to_local:
        log.info("Pull finished (no new analysis_runs to insert).")
        return stats

    log.info("Pulling shared run_results for %d run(s)...", len(uuid_to_local))
    results = shared.get_results_for_runs(list(uuid_to_local.keys()))
    with local._lock:
        for rr in results:
            shared_uuid = str(rr.get("id") or "")
            if not shared_uuid:
                continue
            parent_uuid = str(rr.get("run_id") or "")
            local_run_id = uuid_to_local.get(parent_uuid)
            if local_run_id is None:
                continue
            with local._connect() as conn:
                existing = conn.execute(
                    "SELECT id FROM run_results WHERE shared_uuid = ?",
                    (shared_uuid,),
                ).fetchone()
                if existing is not None:
                    continue
                conn.execute(
                    """
                    INSERT INTO run_results (
                        run_id, saved_at, schema_name, table_name, column_name,
                        asset_kind, source, confidence, logprob_score, raw_logprob,
                        token_count, model_version, reasoning, alternatives_json,
                        evaluated_at, applied_at, chosen_description, evaluation,
                        catalog_status, catalog_indexed_at, db_applied_status,
                        effective_source_kind, superseded_at, rejection_reason,
                        created_by, hostname, shared_uuid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        local_run_id,
                        _dt_to_ts(rr.get("saved_at")) or time.time(),
                        str(rr.get("schema_name") or ""),
                        str(rr.get("table_name") or ""),
                        rr.get("column_name"),
                        str(rr.get("asset_kind") or "table"),
                        str(rr.get("source") or "unknown"),
                        str(rr.get("confidence") or "medium"),
                        rr.get("logprob_score"),
                        rr.get("raw_logprob"),
                        rr.get("token_count"),
                        str(rr.get("model_version") or ""),
                        rr.get("reasoning"),
                        _dump_or_none(rr.get("alternatives_json")) or "[]",
                        _dt_to_ts(rr.get("evaluated_at")),
                        _dt_to_ts(rr.get("applied_at")),
                        rr.get("chosen_description"),
                        rr.get("evaluation"),
                        str(rr.get("catalog_status") or ""),
                        _dt_to_ts(rr.get("catalog_indexed_at")),
                        str(rr.get("db_applied_status") or ""),
                        str(rr.get("effective_source_kind") or ""),
                        _dt_to_ts(rr.get("superseded_at")),
                        str(rr.get("rejection_reason") or ""),
                        rr.get("created_by"),
                        rr.get("hostname"),
                        shared_uuid,
                    ),
                )
            stats["run_results"] += 1
            if progress is not None:
                with contextlib.suppress(Exception):
                    progress("run_results", stats["run_results"])

    log.info(
        "Pull finished in %.1fs: %s",
        time.time() - started,
        ", ".join(f"{k}={v}" for k, v in stats.items()),
    )
    return stats


def _dt_to_ts(value: Any) -> float | None:
    """Convert a tz-aware datetime (the shared store's native time
    unit) into a float epoch second the local SQLite store expects."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _dump_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=True, default=str)
    except (TypeError, ValueError):
        return None


def ensure_column_exists(
    engine: Engine,
    schema: str | None,
    table: str,
    column_name: str,
    column_spec: str,
) -> None:
    """Idempotently add *column_name* to *table* on the given engine.

    Dispatches the correct DDL dialect for each supported backend:

    * **SQLite** — ``ALTER TABLE … ADD COLUMN …`` wrapped in a try/except
      (SQLite raises ``OperationalError: duplicate column name`` on a
      second add; we suppress it).
    * **PostgreSQL / MySQL / Snowflake / BigQuery** — ``ALTER TABLE … ADD
      COLUMN IF NOT EXISTS …`` (all four support the IF NOT EXISTS clause
      natively).
    * **Oracle** — plain ``ALTER TABLE … ADD …`` wrapped in try/except for
      ``ORA-01430`` (column already exists).
    * **Databricks** — ``ALTER TABLE … ADD COLUMNS (…)`` (Databricks uses
      the plural ``COLUMNS`` keyword).

    For every backend, a pre-flight ``inspect(engine).get_columns(table)``
    check is performed first; if the column is already present the ALTER is
    skipped entirely to avoid any DDL noise on already-migrated schemas.

    Parameters
    ----------
    engine:
        SQLAlchemy engine connected to the target backend.
    schema:
        Schema (or database/namespace) name. ``None`` for SQLite or
        schema-less connections.
    table:
        Unqualified table name.
    column_name:
        Name of the column to add.
    column_spec:
        Type + optional constraints as a SQL fragment (e.g. ``"TEXT"``,
        ``"VARCHAR(120) DEFAULT NULL"``).
    """
    # Pre-flight: skip if column already present.
    try:
        inspector = inspect(engine)
        existing_cols = {c["name"] for c in inspector.get_columns(table, schema=schema or None)}
        if column_name in existing_cols:
            log.debug("ensure_column_exists: %s.%s already has %s", table, schema, column_name)
            return
    except Exception:  # noqa: BLE001
        # If introspection fails (e.g. table does not exist yet), fall
        # through to the ALTER — it will fail with a meaningful error.
        pass

    dialect_name = engine.dialect.name.lower()

    # Quote the schema and table identifiers so mixed-case names like
    # ``AMX`` survive Postgres / Oracle identifier folding (they would
    # otherwise be lowercased and the ALTER would target a non-existent
    # ``amx`` schema). The preparer follows each backend's quoting rules.
    preparer = engine.dialect.identifier_preparer
    quoted_table = preparer.quote(table)
    fq_table = f"{preparer.quote(schema)}.{quoted_table}" if schema else quoted_table

    if dialect_name == "sqlite":
        # SQLite: suppress "duplicate column name" OperationalError.
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {fq_table} ADD COLUMN {column_name} {column_spec}"))
        except Exception as exc:  # noqa: BLE001
            err = str(exc).lower()
            if "duplicate column" in err or "already exists" in err:
                pass  # idempotent
            else:
                raise
    elif dialect_name == "oracle":
        # Oracle raises ORA-01430 when the column already exists.
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE {fq_table} ADD {column_name} {column_spec}"))
        except Exception as exc:  # noqa: BLE001
            err = str(exc)
            if "ORA-01430" in err or "already exists" in err.lower():
                pass  # idempotent
            else:
                raise
    elif dialect_name == "databricks":
        # Databricks uses plural COLUMNS and does not support IF NOT EXISTS.
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(f"ALTER TABLE {fq_table} ADD COLUMNS ({column_name} {column_spec})")
                )
        except Exception as exc:  # noqa: BLE001
            err = str(exc).lower()
            if "already exists" in err:
                pass  # idempotent
            else:
                raise
    else:
        # PostgreSQL, MySQL, Snowflake, BigQuery — all support IF NOT EXISTS.
        with engine.begin() as conn:
            conn.execute(
                text(f"ALTER TABLE {fq_table} ADD COLUMN IF NOT EXISTS {column_name} {column_spec}")
            )


_CATALOG_STRUCTURAL_COLS = (
    "db_profile",
    "db_backend",
    "database_name",
    "schema_name",
    "table_name",
    "column_name",
    "entity_kind",
    "asset_kind",
    "dtype",
    "nullable",
    "pk_flag",
    "fk_flag",
    "row_count",
    "last_synced_at",
)


def push_catalog_to_shared(
    local: SQLiteHistoryStore,
    shared: SQLAlchemyHistoryStore,
    *,
    db_profile: str | None = None,
) -> int:
    """Push local structural catalog rows UP to the shared store.

    Reads ``catalog_entities`` from local SQLite (optionally scoped to
    one ``db_profile``) and upserts them into the shared store, where
    last-write-wins on ``last_synced_at`` dedupes against teammates'
    rows. Used both by the deep-sync push (one profile) and the
    enable-time backfill (all profiles).

    Local ``last_synced_at`` is epoch seconds; the shared store wants a
    tz-aware datetime, so it is converted here. Table-level rows store
    ``column_name`` as NULL locally and ``''`` in the shared natural
    key, so the value is normalised on the way up.

    Best-effort and read-only against local: returns the number of rows
    the shared store reported written. A shared outage is swallowed by
    ``upsert_catalog_entities`` (returns a partial count).
    """
    where = "WHERE db_profile = ?" if db_profile else ""
    params: tuple[Any, ...] = (db_profile,) if db_profile else ()
    with local._connect() as conn:  # noqa: SLF001
        local_rows = conn.execute(
            f"SELECT {', '.join(_CATALOG_STRUCTURAL_COLS)} "
            f"FROM catalog_entities {where}",
            params,
        ).fetchall()
    if not local_rows:
        return 0
    rows: list[dict[str, Any]] = []
    for r in local_rows:
        d = {col: r[col] for col in _CATALOG_STRUCTURAL_COLS}
        d["column_name"] = d.get("column_name") or ""
        ts = d.get("last_synced_at")
        d["last_synced_at"] = (
            datetime.fromtimestamp(float(ts), tz=timezone.utc) if ts else None
        )
        rows.append(d)
    return shared.upsert_catalog_entities(rows)


def pull_catalog_to_local(
    local: SQLiteHistoryStore,
    shared: SQLAlchemyHistoryStore,
) -> int:
    """Pull shared structural catalog rows DOWN into local SQLite.

    Upserts by the natural key
    (db_profile, database_name, schema_name, table_name, column_name)
    with last-write-wins on ``last_synced_at``. Only the structural
    columns are touched; a newly inserted row gets
    ``effective_description_id = NULL`` (descriptions resolve from the
    puller's own local state, populated by the run-sharing path), and
    an existing local row keeps its description link untouched — only
    its structural columns refresh when the shared snapshot is newer.

    Returns the number of local rows inserted or updated. Degrades to 0
    when the shared store is unreachable.
    """
    shared_rows = shared.fetch_catalog_entities()
    if not shared_rows:
        return 0
    written = 0
    with local._lock:  # noqa: SLF001
        with local._connect() as conn:  # noqa: SLF001
            for row in shared_rows:
                # Normalise the shared '' table-level marker back to the
                # local NULL convention so the natural-key match works.
                col_name = row.get("column_name") or None
                incoming_ts = _dt_to_ts(row.get("last_synced_at"))
                existing = conn.execute(
                    """
                    SELECT id, last_synced_at FROM catalog_entities
                    WHERE db_profile = ? AND database_name = ?
                      AND schema_name = ? AND table_name = ?
                      AND ((column_name IS NULL AND ? IS NULL) OR column_name = ?)
                    LIMIT 1
                    """,
                    (
                        row.get("db_profile"),
                        row.get("database_name") or "",
                        row.get("schema_name"),
                        row.get("table_name"),
                        col_name,
                        col_name,
                    ),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        """
                        INSERT INTO catalog_entities (
                            db_profile, db_backend, database_name, schema_name,
                            table_name, column_name, entity_kind, asset_kind,
                            dtype, nullable, pk_flag, fk_flag, row_count,
                            search_text, effective_description_id,
                            updated_at, last_synced_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', NULL, ?, ?)
                        """,
                        (
                            row.get("db_profile"),
                            row.get("db_backend"),
                            row.get("database_name") or "",
                            row.get("schema_name"),
                            row.get("table_name"),
                            col_name,
                            row.get("entity_kind"),
                            row.get("asset_kind"),
                            row.get("dtype"),
                            row.get("nullable"),
                            row.get("pk_flag"),
                            row.get("fk_flag"),
                            row.get("row_count"),
                            time.time(),
                            incoming_ts,
                        ),
                    )
                    written += 1
                else:
                    stored_ts = existing["last_synced_at"]
                    if incoming_ts is not None and (
                        stored_ts is None or incoming_ts > float(stored_ts)
                    ):
                        conn.execute(
                            """
                            UPDATE catalog_entities
                            SET db_backend = ?, entity_kind = ?, asset_kind = ?,
                                dtype = ?, nullable = ?, pk_flag = ?, fk_flag = ?,
                                row_count = ?, updated_at = ?, last_synced_at = ?
                            WHERE id = ?
                            """,
                            (
                                row.get("db_backend"),
                                row.get("entity_kind"),
                                row.get("asset_kind"),
                                row.get("dtype"),
                                row.get("nullable"),
                                row.get("pk_flag"),
                                row.get("fk_flag"),
                                row.get("row_count"),
                                time.time(),
                                incoming_ts,
                                existing["id"],
                            ),
                        )
                        written += 1
    return written


__all__ = [
    "ensure_column_exists",
    "migrate_local_to_shared",
    "pull_shared_to_local",
    "push_catalog_to_shared",
    "pull_catalog_to_local",
]
