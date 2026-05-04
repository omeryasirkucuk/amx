"""Local + shared dual-write history store.

Wraps a local :class:`SQLiteHistoryStore` and a remote
:class:`SQLAlchemyHistoryStore`. Every write hits the local store first
(source of truth, always-on cache); the remote write follows
best-effort. If the remote write fails — network blip, expired token,
schema migration — the operation is parked in the local
``pending_shared_writes`` outbox so a later ``flush_pending()`` can
retry it without losing data.

Reads come from the local store. The shared store exists for *team
visibility* (a teammate on another laptop sees your runs); local reads
are always faster and avoid surfacing the wrong UUID-keyed rows. A
future minor adds ``list_team_runs()`` for cross-machine visibility.

Caller-facing IDs are LOCAL int IDs (matching the historical SQLite
contract). The shared store uses UUIDs internally; the dual-write
store maps local int ↔ shared UUID by querying the shared store on
``hostname + local_id`` whenever a downstream UPDATE-style call
arrives. This keeps the IHistoryStore Protocol stable for the dozen+
existing call sites.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import time
from typing import Any

from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.utils.logging import get_logger

log = get_logger("storage.dual_write")


# Op-kinds in the outbox. Each is a serialised method-name + kwargs
# tuple; ``flush_pending`` re-dispatches by op_kind. New write methods
# must be added here AND in ``_replay_op`` below.
OP_CREATE_RUN = "create_run"
OP_FINISH_RUN = "finish_run"
OP_UPDATE_RUN_STATUS = "update_run_status"
OP_UPDATE_RUN_PLANNED_COUNT = "update_run_planned_count"
OP_INCREMENT_RUN_PROCESSED = "increment_run_processed"
OP_INCREMENT_RUN_APPLIED = "increment_run_applied"
OP_SAVE_RUN_RESULTS = "save_run_results"
OP_RECORD_EVALUATION = "record_evaluation"
OP_RECORD_APPLIED = "record_applied"
OP_RECORD_DB_APPLY_FAILURE = "record_db_apply_failure"
OP_LOG_EVENT = "log_event"
OP_SET_SESSION_STATE = "set_session_state"


class DualWriteHistoryStore:
    """Façade that writes to local SQLite and a shared SQLAlchemy backend.

    Reads delegate to the local store. The shared store is best-effort
    on writes — a failure surfaces as a WARN log and an outbox row,
    never a raised exception, so the user's CLI session keeps working
    even if the team store is down.
    """

    def __init__(
        self,
        local: SQLiteHistoryStore,
        shared: SQLAlchemyHistoryStore,
    ) -> None:
        self.local = local
        self.shared = shared
        self.db_path = local.db_path
        self._ensure_outbox()

    @property
    def _lock(self) -> Any:  # pragma: no cover - delegate accessor
        return self.local._lock

    def _connect(self) -> Any:  # pragma: no cover - delegate accessor
        # ``ChatSessionStore`` opens its own transactions via
        # ``hs._connect()`` — the chat_sessions table is part of the
        # local SQLite layer, not the shared team backend, so the
        # delegate just routes through to the local store.
        return self.local._connect()

    # ── Outbox housekeeping ───────────────────────────────────────────────

    def _ensure_outbox(self) -> None:
        with self.local._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_shared_writes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    op_kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    queued_at REAL NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    attempts INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pending_shared_writes_queued "
                "ON pending_shared_writes(queued_at)"
            )

    def _enqueue(self, op_kind: str, payload: dict[str, Any], err: Exception) -> None:
        try:
            with self.local._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO pending_shared_writes
                    (op_kind, payload_json, queued_at, last_error, attempts)
                    VALUES (?, ?, ?, ?, 1)
                    """,
                    (
                        op_kind,
                        json.dumps(payload, ensure_ascii=True, default=str),
                        time.time(),
                        str(err)[:2000],
                    ),
                )
        except sqlite3.Error as enq_err:
            log.warning(
                "Could not enqueue shared-write for retry (op=%s): %s — data is "
                "in local SQLite only.",
                op_kind,
                enq_err,
            )

    def pending_count(self) -> int:
        with contextlib.suppress(sqlite3.Error):
            with self.local._connect() as conn:
                row = conn.execute("SELECT COUNT(*) FROM pending_shared_writes").fetchone()
                if row is not None:
                    return int(row[0] or 0)
        return 0

    def flush_pending(self, *, max_attempts: int = 5) -> tuple[int, int]:
        """Retry every queued op. Returns ``(succeeded, remaining)``.

        Ops that have failed ``max_attempts`` times are NOT dropped —
        they remain in the outbox so the user can inspect them via
        ``/history-store list-pending``.
        """
        succeeded = 0
        with self.local._connect() as conn:
            rows = conn.execute(
                "SELECT id, op_kind, payload_json, attempts FROM pending_shared_writes "
                "ORDER BY queued_at ASC"
            ).fetchall()
        for row in rows:
            row_id, op_kind, payload_json, attempts = row
            if int(attempts or 0) >= max_attempts:
                continue
            try:
                payload = json.loads(payload_json)
            except json.JSONDecodeError:
                log.warning("Skipping malformed outbox row %s (op=%s)", row_id, op_kind)
                continue
            try:
                self._replay_op(op_kind, payload)
            except Exception as exc:
                with contextlib.suppress(sqlite3.Error):
                    with self.local._connect() as conn:
                        conn.execute(
                            "UPDATE pending_shared_writes "
                            "SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                            (str(exc)[:2000], row_id),
                        )
                continue
            with contextlib.suppress(sqlite3.Error):
                with self.local._connect() as conn:
                    conn.execute("DELETE FROM pending_shared_writes WHERE id = ?", (row_id,))
            succeeded += 1
        return succeeded, self.pending_count()

    def _replay_op(self, op_kind: str, payload: dict[str, Any]) -> None:
        """Re-dispatch an outbox row to the shared store.

        New ops added in :meth:`_try_remote` must be added here too.
        Each branch reconstructs the original method call from the
        serialised payload so the retry is byte-equivalent to the
        original write.
        """
        if op_kind == OP_CREATE_RUN:
            self.shared.create_run(**payload)
        elif op_kind == OP_FINISH_RUN:
            self.shared.finish_run(payload.pop("run_id"), **payload)
        elif op_kind == OP_UPDATE_RUN_STATUS:
            self.shared.update_run_status(
                payload["run_id"],
                payload["status"],
                payload.get("error_text", ""),
            )
        elif op_kind == OP_UPDATE_RUN_PLANNED_COUNT:
            self.shared.update_run_planned_count(payload["run_id"], payload["planned_count"])
        elif op_kind == OP_INCREMENT_RUN_PROCESSED:
            self.shared.increment_run_processed(payload["run_id"], payload.get("by", 1))
        elif op_kind == OP_INCREMENT_RUN_APPLIED:
            self.shared.increment_run_applied(payload["run_id"], payload.get("by", 1))
        elif op_kind == OP_SAVE_RUN_RESULTS:
            self.shared.save_run_results(
                payload["run_id"],
                payload["suggestions"],
                local_ids=payload.get("local_ids"),
            )
        elif op_kind == OP_RECORD_EVALUATION:
            self.shared.record_evaluation(
                payload["result_id"],
                chosen_description=payload["chosen_description"],
                evaluation=payload["evaluation"],
            )
        elif op_kind == OP_RECORD_APPLIED:
            self.shared.record_applied(payload["result_id"])
        elif op_kind == OP_RECORD_DB_APPLY_FAILURE:
            self.shared.record_db_apply_failure(payload["result_id"], payload.get("error_text", ""))
        elif op_kind == OP_LOG_EVENT:
            self.shared.log_event(**payload)
        elif op_kind == OP_SET_SESSION_STATE:
            self.shared.set_session_state(payload["namespace"], payload["key"], payload["value"])
        else:
            log.warning("Unknown outbox op_kind=%s — leaving queued", op_kind)
            raise ValueError(f"unknown op_kind: {op_kind}")

    # ── Helpers ───────────────────────────────────────────────────────────

    def _try_remote(self, op_kind: str, payload: dict[str, Any], call) -> None:
        """Run *call*; on failure, enqueue *payload* under *op_kind*."""
        try:
            call()
        except Exception as exc:
            log.warning(
                "Shared-history write failed (op=%s): %s — queued for retry.",
                op_kind,
                exc,
            )
            self._enqueue(op_kind, payload, exc)

    def _resolve_run_uuid(self, local_run_id: int) -> str | None:
        return self.shared.find_run_uuid_by_local_id(local_run_id)

    def _resolve_result_uuid(self, local_result_id: int) -> str | None:
        return self.shared.find_result_uuid_by_local_id(local_result_id)

    # ── IHistoryStore — write methods ─────────────────────────────────────

    def create_run(self, **kwargs: Any) -> int:
        local_id = self.local.create_run(**kwargs)
        # Re-shape kwargs for the shared call: scope is required, others
        # mirror the local signature 1:1. UUID is generated server-side
        # (in the SQLAlchemy store) but we capture the local_id linkage.
        payload: dict[str, Any] = dict(kwargs)
        payload["local_id"] = local_id
        self._try_remote(
            OP_CREATE_RUN,
            payload,
            lambda: self.shared.create_run(**payload),
        )
        return local_id

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        metrics: dict[str, Any],
        tokens: dict[str, Any],
        results: dict[str, Any],
        error_text: str = "",
    ) -> None:
        self.local.finish_run(
            run_id,
            status=status,
            metrics=metrics,
            tokens=tokens,
            results=results,
            error_text=error_text,
        )
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            # The original create_run was queued — this finish_run goes
            # in too, in order, so flush replays both.
            self._enqueue(
                OP_FINISH_RUN,
                {
                    "run_id": "<unresolved>",
                    "status": status,
                    "metrics": metrics,
                    "tokens": tokens,
                    "results": results,
                    "error_text": error_text,
                    "local_run_id": run_id,
                },
                RuntimeError("shared run_uuid not found"),
            )
            return
        payload = {
            "run_id": run_uuid,
            "status": status,
            "metrics": metrics,
            "tokens": tokens,
            "results": results,
            "error_text": error_text,
        }
        self._try_remote(
            OP_FINISH_RUN,
            payload,
            lambda: self.shared.finish_run(
                run_uuid,
                status=status,
                metrics=metrics,
                tokens=tokens,
                results=results,
                error_text=error_text,
            ),
        )

    def update_run_status(self, run_id: int, status: str, error_text: str = "") -> None:
        self.local.update_run_status(run_id, status, error_text)
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            return
        payload = {"run_id": run_uuid, "status": status, "error_text": error_text}
        self._try_remote(
            OP_UPDATE_RUN_STATUS,
            payload,
            lambda: self.shared.update_run_status(run_uuid, status, error_text),
        )

    def update_run_planned_count(self, run_id: int, planned_count: int) -> None:
        self.local.update_run_planned_count(run_id, planned_count)
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            return
        payload = {"run_id": run_uuid, "planned_count": int(planned_count)}
        self._try_remote(
            OP_UPDATE_RUN_PLANNED_COUNT,
            payload,
            lambda: self.shared.update_run_planned_count(run_uuid, planned_count),
        )

    def increment_run_processed(self, run_id: int, by: int = 1) -> None:
        self.local.increment_run_processed(run_id, by)
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            return
        payload = {"run_id": run_uuid, "by": int(by)}
        self._try_remote(
            OP_INCREMENT_RUN_PROCESSED,
            payload,
            lambda: self.shared.increment_run_processed(run_uuid, by),
        )

    def increment_run_applied(self, run_id: int, by: int = 1) -> None:
        self.local.increment_run_applied(run_id, by)
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            return
        payload = {"run_id": run_uuid, "by": int(by)}
        self._try_remote(
            OP_INCREMENT_RUN_APPLIED,
            payload,
            lambda: self.shared.increment_run_applied(run_uuid, by),
        )

    def save_run_results(self, run_id: int, suggestions: list[dict[str, Any]]) -> list[int]:
        local_ids = self.local.save_run_results(run_id, suggestions)
        run_uuid = self._resolve_run_uuid(run_id)
        if run_uuid is None:
            self._enqueue(
                OP_SAVE_RUN_RESULTS,
                {
                    "run_id": "<unresolved>",
                    "suggestions": suggestions,
                    "local_ids": local_ids,
                    "local_run_id": run_id,
                },
                RuntimeError("shared run_uuid not found"),
            )
            return local_ids
        payload = {
            "run_id": run_uuid,
            "suggestions": suggestions,
            "local_ids": local_ids,
        }
        self._try_remote(
            OP_SAVE_RUN_RESULTS,
            payload,
            lambda: self.shared.save_run_results(run_uuid, suggestions, local_ids=local_ids),
        )
        return local_ids

    def record_evaluation(
        self,
        result_id: int,
        *,
        chosen_description: str,
        evaluation: str,
    ) -> None:
        self.local.record_evaluation(
            result_id,
            chosen_description=chosen_description,
            evaluation=evaluation,
        )
        result_uuid = self._resolve_result_uuid(result_id)
        if result_uuid is None:
            return
        payload = {
            "result_id": result_uuid,
            "chosen_description": chosen_description,
            "evaluation": evaluation,
        }
        self._try_remote(
            OP_RECORD_EVALUATION,
            payload,
            lambda: self.shared.record_evaluation(
                result_uuid,
                chosen_description=chosen_description,
                evaluation=evaluation,
            ),
        )

    def record_applied(self, result_id: int) -> None:
        self.local.record_applied(result_id)
        result_uuid = self._resolve_result_uuid(result_id)
        if result_uuid is None:
            return
        payload = {"result_id": result_uuid}
        self._try_remote(
            OP_RECORD_APPLIED,
            payload,
            lambda: self.shared.record_applied(result_uuid),
        )

    def record_db_apply_failure(self, result_id: int, error_text: str = "") -> None:
        self.local.record_db_apply_failure(result_id, error_text)
        result_uuid = self._resolve_result_uuid(result_id)
        if result_uuid is None:
            return
        payload = {"result_id": result_uuid, "error_text": error_text}
        self._try_remote(
            OP_RECORD_DB_APPLY_FAILURE,
            payload,
            lambda: self.shared.record_db_apply_failure(result_uuid, error_text),
        )

    def log_event(
        self,
        *,
        event_type: str,
        status: str,
        command: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.local.log_event(event_type=event_type, status=status, command=command, details=details)
        payload = {
            "event_type": event_type,
            "status": status,
            "command": command,
            "details": details or {},
        }
        self._try_remote(
            OP_LOG_EVENT,
            payload,
            lambda: self.shared.log_event(
                event_type=event_type,
                status=status,
                command=command,
                details=details,
            ),
        )

    def set_session_state(self, namespace: str, key: str, value: Any) -> None:
        self.local.set_session_state(namespace, key, value)
        payload = {"namespace": namespace, "key": key, "value": value}
        self._try_remote(
            OP_SET_SESSION_STATE,
            payload,
            lambda: self.shared.set_session_state(namespace, key, value),
        )

    # ── IHistoryStore — read methods (delegate to local) ──────────────────

    def get_run(self, run_id: int) -> dict[str, Any] | None:
        return self.local.get_run(run_id)

    def get_run_results(
        self, run_id: int, *, unevaluated_only: bool = False
    ) -> list[dict[str, Any]]:
        return self.local.get_run_results(run_id, unevaluated_only=unevaluated_only)

    def list_recent_runs(
        self, limit: int = 20, *, command_filter: str | None = "analyze.run"
    ) -> list[dict[str, Any]]:
        return self.local.list_recent_runs(limit, command_filter=command_filter)

    def list_runs_with_result_counts(self, limit: int = 20) -> list[dict[str, Any]]:
        return self.local.list_runs_with_result_counts(limit)

    def find_runs_for_scope(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
        command_filter: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        return self.local.find_runs_for_scope(
            schema=schema, table=table, command_filter=command_filter, limit=limit
        )

    def list_recent_events(self, limit: int = 30) -> list[dict[str, Any]]:
        return self.local.list_recent_events(limit)

    def get_session_state(self, namespace: str, key: str, default: Any = None) -> Any:
        return self.local.get_session_state(namespace, key, default)

    # ── SQLite-only conveniences proxied through ──────────────────────────

    def stats(self) -> dict[str, Any]:
        return self.local.stats()


__all__ = ["DualWriteHistoryStore"]
