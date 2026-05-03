"""Warehouse-backed history store.

Mirrors the public surface of :class:`amx.storage.sqlite_store.SQLiteHistoryStore`
but persists every row into a remote relational backend (PostgreSQL,
MySQL, Snowflake, Databricks, …) via SQLAlchemy Core. The class is
intentionally a 1:1 method-for-method analogue of the SQLite store so
the diff between local and shared semantics is reviewable.

Used in two ways:

1. **Direct** — when the user has shared mode enabled and wants reads
   to come from the team store. (Future minor; v0.12 reads still come
   from the local SQLite cache.)
2. **Wrapped** — :class:`amx.storage.dual_write.DualWriteHistoryStore`
   wraps both a :class:`SQLiteHistoryStore` (local cache, source of
   truth for read paths) and an instance of this class (remote, source
   of truth for the team).

Concurrency model: every method opens a fresh ``engine.begin()`` and
commits at the end. SQLAlchemy's connection pool handles reuse; we
never share connections across threads.

Identifiers: PKs are UUIDv4 strings. Local INT ids from the SQLite
side travel as ``local_id`` + ``hostname`` columns so the dual-write
coordinator can find the corresponding shared row when later UPDATEs
fire (e.g. ``finish_run`` for a run that was created on this host).
"""

from __future__ import annotations

import getpass
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import and_, delete, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from amx.storage.shared_schema import (
    SHARED_SCHEMA_VERSION,
    build_metadata,
)
from amx.utils.logging import get_logger

log = get_logger("storage.sqlalchemy")


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts_to_dt(ts: float | None) -> datetime | None:
    """Convert a float epoch second (the SQLite store's native time
    unit) into a timezone-aware ``datetime`` for portable backends."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _client_version() -> str:
    try:
        from amx import __version__ as v

        return str(v)
    except Exception:
        return ""


def _hostname() -> str:
    try:
        return socket.gethostname() or ""
    except Exception:
        return ""


def _username() -> str:
    try:
        return getpass.getuser() or ""
    except Exception:
        return ""


class SchemaVersionMismatch(RuntimeError):
    """Raised when the shared schema version is newer than this client knows."""


class SQLAlchemyHistoryStore:
    """Run-history persistence backed by a SQLAlchemy Engine.

    Parameters
    ----------
    engine
        Pre-built SQLAlchemy engine for the team backend.
    schema
        Schema/database name where AMX tables live (default ``"AMX"``).
    """

    def __init__(self, engine: Engine, schema: str) -> None:
        self.engine = engine
        self.schema = schema
        self._md = build_metadata(schema=schema)
        self._t_runs = self._md.tables[f"{schema}.analysis_runs"]
        self._t_results = self._md.tables[f"{schema}.run_results"]
        self._t_events = self._md.tables[f"{schema}.app_events"]
        self._t_session = self._md.tables[f"{schema}.session_state"]
        self._t_meta = self._md.tables[f"{schema}.schema_meta"]
        self._hostname = _hostname()
        self._username = _username()
        self._client_version = _client_version()

    # ── Bootstrap ─────────────────────────────────────────────────────────

    def init(self) -> None:
        """Create tables on the remote backend if they do not exist.

        Idempotent. ``MetaData.create_all`` skips tables that already
        exist. Also stamps :data:`SHARED_SCHEMA_VERSION` into
        ``schema_meta`` so older clients can detect a forward-rolled schema.
        """
        self._md.create_all(self.engine)
        with self.engine.begin() as conn:
            existing = conn.execute(select(self._t_meta.c.schema_version)).fetchone()
            if existing is None:
                conn.execute(
                    insert(self._t_meta).values(
                        id=1,
                        schema_version=SHARED_SCHEMA_VERSION,
                        created_at=_utcnow(),
                        created_by_client_version=self._client_version,
                    )
                )
            else:
                stored = int(existing[0])
                if stored > SHARED_SCHEMA_VERSION:
                    raise SchemaVersionMismatch(
                        f"Shared AMX schema version {stored} is newer than this "
                        f"client supports ({SHARED_SCHEMA_VERSION}). Upgrade AMX."
                    )

    # ── Run lifecycle ─────────────────────────────────────────────────────

    def create_run(
        self,
        *,
        command: str,
        mode: str,
        db_backend: str,
        db_profile: str,
        llm_provider: str,
        llm_model: str,
        scope: dict[str, list[str]],
        selected_count: int = 0,
        planned_count: int = 0,
        review_strategy: str | None = None,
        llm_profile: str | None = None,
        doc_profile: str | None = None,
        code_profile: str | None = None,
        settings: dict[str, Any] | None = None,
        # Shared-mode extras (passed by DualWriteHistoryStore so
        # ``local_id`` ties the shared row to the SQLite row).
        run_id: str | None = None,
        local_id: int | None = None,
    ) -> str:
        """Insert a new run row, returning the UUID PK.

        ``run_id`` may be supplied to use a pre-generated UUID — used by
        the dual-write coordinator so the same UUID is re-used in
        retry-from-outbox flows.
        """
        if selected_count <= 0:
            try:
                selected_count = sum(len(v or []) for v in (scope or {}).values())
            except Exception:
                selected_count = 0
        if planned_count <= 0:
            planned_count = selected_count
        rid = run_id or _new_uuid()
        with self.engine.begin() as conn:
            conn.execute(
                insert(self._t_runs).values(
                    id=rid,
                    started_at=_utcnow(),
                    status="running",
                    command=command,
                    mode=mode,
                    db_backend=db_backend,
                    db_profile=db_profile,
                    llm_provider=llm_provider,
                    llm_model=llm_model,
                    scope_json=scope,
                    selected_count=int(selected_count),
                    planned_count=int(planned_count),
                    processed_count=0,
                    applied_count=0,
                    review_strategy=str(review_strategy or ""),
                    llm_profile=llm_profile,
                    doc_profile=doc_profile,
                    code_profile=code_profile,
                    settings_json=settings,
                    created_by=self._username,
                    hostname=self._hostname,
                    client_version=self._client_version,
                    local_id=local_id,
                )
            )
        return rid

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        metrics: dict[str, Any],
        tokens: dict[str, Any],
        results: dict[str, Any],
        error_text: str = "",
    ) -> None:
        with self.engine.begin() as conn:
            row = conn.execute(
                select(self._t_runs.c.started_at).where(self._t_runs.c.id == run_id)
            ).fetchone()
            ended = _utcnow()
            if row and row[0] is not None:
                # SQLite returns datetimes as naive even when TIMESTAMP
                # WITH TIME ZONE was requested. Reattach UTC so the
                # subtraction against ``ended`` (always tz-aware) is valid.
                started = row[0]
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                duration = max(0.0, (ended - started).total_seconds())
            else:
                duration = 0.0
            conn.execute(
                update(self._t_runs)
                .where(self._t_runs.c.id == run_id)
                .values(
                    ended_at=ended,
                    duration_sec=duration,
                    status=status,
                    metrics_json=metrics,
                    tokens_json=tokens,
                    results_json=results,
                    error_text=(error_text or "")[:4000],
                )
            )

    def update_run_status(self, run_id: str, status: str, error_text: str = "") -> None:
        with self.engine.begin() as conn:
            current = conn.execute(
                select(self._t_runs.c.error_text).where(self._t_runs.c.id == run_id)
            ).fetchone()
            existing_err = current[0] if current else ""
            if status == "success":
                err_value = ""
            elif error_text:
                err_value = error_text
            else:
                err_value = existing_err
            conn.execute(
                update(self._t_runs)
                .where(self._t_runs.c.id == run_id)
                .values(status=status, error_text=err_value)
            )

    def update_run_planned_count(self, run_id: str, planned_count: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(self._t_runs)
                .where(self._t_runs.c.id == run_id)
                .values(planned_count=int(planned_count))
            )

    def increment_run_processed(self, run_id: str, by: int = 1) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(self._t_runs)
                .where(self._t_runs.c.id == run_id)
                .values(processed_count=self._t_runs.c.processed_count + int(by))
            )

    def increment_run_applied(self, run_id: str, by: int = 1) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(self._t_runs)
                .where(self._t_runs.c.id == run_id)
                .values(applied_count=self._t_runs.c.applied_count + int(by))
            )

    # ── Per-result helpers ────────────────────────────────────────────────

    def save_run_results(
        self,
        run_id: str,
        suggestions: list[dict[str, Any]],
        local_ids: list[int] | None = None,
    ) -> list[str]:
        """Insert all LLM alternatives for a run; return the inserted UUIDs.

        ``local_ids`` parallel-arrays with ``suggestions`` so each shared
        row records the SQLite INT id from the local store. Used by
        :class:`amx.storage.dual_write.DualWriteHistoryStore` so the
        later ``record_applied(local_int_id)`` call can find the right
        shared row.
        """
        ids: list[str] = []
        if not suggestions:
            return ids
        now = _utcnow()
        rows = []
        for idx, s in enumerate(suggestions):
            rid = _new_uuid()
            ids.append(rid)
            rows.append(
                {
                    "id": rid,
                    "run_id": run_id,
                    "saved_at": now,
                    "schema_name": s.get("schema", ""),
                    "table_name": s.get("table", ""),
                    "column_name": s.get("column"),
                    "asset_kind": s.get("asset_kind", "table"),
                    "source": s.get("source", "unknown"),
                    "confidence": s.get("confidence", "medium"),
                    "logprob_score": s.get("logprob_score"),
                    "raw_logprob": s.get("raw_logprob", s.get("logprob_score")),
                    "token_count": s.get("token_count"),
                    "model_version": s.get("model_version", ""),
                    "reasoning": s.get("reasoning", ""),
                    "alternatives_json": s.get("alternatives", []),
                    "rejection_reason": "",
                    "hostname": self._hostname,
                    "local_id": (local_ids[idx] if local_ids and idx < len(local_ids) else None),
                }
            )
        with self.engine.begin() as conn:
            conn.execute(insert(self._t_results), rows)
        return ids

    def record_evaluation(
        self,
        result_id: str,
        *,
        chosen_description: str,
        evaluation: str,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(self._t_results)
                .where(self._t_results.c.id == result_id)
                .values(
                    evaluated_at=_utcnow(),
                    chosen_description=chosen_description,
                    evaluation=evaluation,
                )
            )

    def record_applied(self, result_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                update(self._t_results)
                .where(self._t_results.c.id == result_id)
                .values(
                    applied_at=_utcnow(),
                    db_applied_status="applied",
                    rejection_reason="",
                )
            )

    def record_db_apply_failure(self, result_id: str, error_text: str = "") -> None:
        with self.engine.begin() as conn:
            existing = conn.execute(
                select(self._t_results.c.rejection_reason).where(self._t_results.c.id == result_id)
            ).fetchone()
            current_reason = existing[0] if existing else ""
            new_reason = error_text if error_text else current_reason
            conn.execute(
                update(self._t_results)
                .where(self._t_results.c.id == result_id)
                .values(db_applied_status="failed", rejection_reason=new_reason or "")
            )

    # ── Reads ─────────────────────────────────────────────────────────────

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.engine.begin() as conn:
            row = (
                conn.execute(select(self._t_runs).where(self._t_runs.c.id == run_id))
                .mappings()
                .fetchone()
            )
        return dict(row) if row else None

    def get_run_results(
        self, run_id: str, *, unevaluated_only: bool = False
    ) -> list[dict[str, Any]]:
        stmt = select(self._t_results).where(self._t_results.c.run_id == run_id)
        if unevaluated_only:
            stmt = stmt.where(
                (self._t_results.c.evaluation.is_(None)) | (self._t_results.c.evaluation == "")
            )
        stmt = stmt.order_by(self._t_results.c.saved_at)
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def list_recent_runs(
        self, limit: int = 20, *, command_filter: str | None = "analyze.run"
    ) -> list[dict[str, Any]]:
        stmt = select(self._t_runs)
        if command_filter:
            stmt = stmt.where(self._t_runs.c.command == command_filter)
        stmt = stmt.order_by(self._t_runs.c.started_at.desc()).limit(max(1, int(limit)))
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def list_runs_with_result_counts(self, limit: int = 20) -> list[dict[str, Any]]:
        # Two-step (run list + per-run count) keeps the query portable
        # across the eight Tier-1/2 backends without needing dialect-
        # specific GROUP BY shapes.
        runs = self.list_recent_runs(limit=limit, command_filter=None)
        if not runs:
            return []
        with self.engine.begin() as conn:
            for r in runs:
                rid = r.get("id")
                if not rid:
                    r["total_alternatives"] = 0
                    r["pending_count"] = 0
                    continue
                total = conn.execute(
                    select(self._t_results.c.id).where(self._t_results.c.run_id == rid)
                ).fetchall()
                pending = conn.execute(
                    select(self._t_results.c.id).where(
                        and_(
                            self._t_results.c.run_id == rid,
                            (self._t_results.c.evaluation.is_(None))
                            | (self._t_results.c.evaluation == ""),
                        )
                    )
                ).fetchall()
                r["total_alternatives"] = len(total)
                r["pending_count"] = len(pending)
        return runs

    def find_runs_for_scope(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
        command_filter: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        # Portable scope filtering: pull the recent N×4 rows and filter
        # in Python rather than emit a backend-specific JSON query.
        # /compare itself usually only inspects the last few runs so
        # the bandwidth cost is negligible.
        rows = self.list_recent_runs(limit=max(1, int(limit) * 4), command_filter=command_filter)
        out: list[dict[str, Any]] = []
        for r in rows:
            scope_json = r.get("scope_json") or {}
            if schema and schema not in scope_json:
                continue
            if table:
                tables = scope_json.get(schema, []) if schema else []
                # If schema not specified, search across all schemas
                all_tables = (
                    list(tables) if schema else [t for v in scope_json.values() for t in (v or [])]
                )
                if table not in all_tables:
                    continue
            out.append(r)
            if len(out) >= int(limit):
                break
        return out

    def list_recent_events(self, limit: int = 30) -> list[dict[str, Any]]:
        stmt = (
            select(self._t_events)
            .order_by(self._t_events.c.created_at.desc())
            .limit(max(1, int(limit)))
        )
        with self.engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    # ── Misc ──────────────────────────────────────────────────────────────

    def log_event(
        self,
        *,
        event_type: str,
        status: str,
        command: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(self._t_events).values(
                    id=_new_uuid(),
                    created_at=_utcnow(),
                    event_type=event_type,
                    status=status,
                    command=command,
                    details_json=details or {},
                    created_by=self._username,
                    hostname=self._hostname,
                    client_version=self._client_version,
                )
            )

    def set_session_state(self, namespace: str, key: str, value: Any) -> None:
        # Session state on shared store is keyed by hostname so
        # different machines do not stomp on each other's checkpoints.
        with self.engine.begin() as conn:
            conn.execute(
                delete(self._t_session).where(
                    and_(
                        self._t_session.c.namespace == namespace,
                        self._t_session.c.key_name == key,
                        self._t_session.c.hostname == self._hostname,
                    )
                )
            )
            conn.execute(
                insert(self._t_session).values(
                    namespace=namespace,
                    key_name=key,
                    hostname=self._hostname,
                    value_json=value,
                    updated_at=_utcnow(),
                    created_by=self._username,
                )
            )

    def get_session_state(self, namespace: str, key: str, default: Any = None) -> Any:
        with self.engine.begin() as conn:
            row = conn.execute(
                select(self._t_session.c.value_json).where(
                    and_(
                        self._t_session.c.namespace == namespace,
                        self._t_session.c.key_name == key,
                        self._t_session.c.hostname == self._hostname,
                    )
                )
            ).fetchone()
        if not row:
            return default
        return row[0]

    # ── Dual-write convenience (used by DualWriteHistoryStore) ────────────

    def find_run_uuid_by_local_id(self, local_id: int) -> str | None:
        """Look up the shared UUID for a given local SQLite int id.

        Returns ``None`` if the row was never propagated (e.g., shared
        write failed and the outbox still owns it). Scoped to *this*
        machine's hostname so two laptops with overlapping local ids
        do not cross-pollute.
        """
        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    select(self._t_runs.c.id).where(
                        and_(
                            self._t_runs.c.hostname == self._hostname,
                            self._t_runs.c.local_id == int(local_id),
                        )
                    )
                ).fetchone()
            return str(row[0]) if row else None
        except SQLAlchemyError as exc:
            log.debug("find_run_uuid_by_local_id failed: %s", exc)
            return None

    def find_result_uuid_by_local_id(self, local_id: int) -> str | None:
        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    select(self._t_results.c.id).where(
                        and_(
                            self._t_results.c.hostname == self._hostname,
                            self._t_results.c.local_id == int(local_id),
                        )
                    )
                ).fetchone()
            return str(row[0]) if row else None
        except SQLAlchemyError as exc:
            log.debug("find_result_uuid_by_local_id failed: %s", exc)
            return None


__all__ = [
    "SQLAlchemyHistoryStore",
    "SchemaVersionMismatch",
]
