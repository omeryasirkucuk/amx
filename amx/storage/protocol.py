"""Backend-agnostic history-store contract.

The :class:`IHistoryStore` Protocol is the public surface every history
backend (local SQLite, shared warehouse, dual-write façade) implements.
Call sites import from this module instead of binding to
:class:`amx.storage.sqlite_store.SQLiteHistoryStore` directly so swapping
in :class:`amx.storage.sqlalchemy_store.SQLAlchemyHistoryStore` (shared
team mode) or :class:`amx.storage.dual_write.DualWriteHistoryStore`
(local + shared) is transparent.

Only the methods that *all* backends are expected to implement live
here; SQLite-only conveniences (e.g. ``stats()``) are re-exported from
the SQLite class for the few callers that need them.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class IHistoryStore(Protocol):
    """Run-history persistence contract shared by every backend."""

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
    ) -> int: ...

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        metrics: dict[str, Any],
        tokens: dict[str, Any],
        results: dict[str, Any],
        error_text: str = "",
    ) -> None: ...

    def update_run_status(self, run_id: int, status: str, error_text: str = "") -> None: ...

    def update_run_planned_count(self, run_id: int, planned_count: int) -> None: ...

    def increment_run_processed(self, run_id: int, by: int = 1) -> None: ...

    def increment_run_applied(self, run_id: int, by: int = 1) -> None: ...

    # ── Per-result helpers ────────────────────────────────────────────────

    def save_run_results(self, run_id: int, suggestions: list[dict[str, Any]]) -> list[int]: ...

    def record_evaluation(
        self,
        result_id: int,
        *,
        chosen_description: str,
        evaluation: str,
    ) -> None: ...

    def record_applied(
        self,
        result_id: int,
        *,
        chosen_description: str | None = None,
    ) -> None: ...

    def record_db_apply_failure(self, result_id: int, error_text: str = "") -> None: ...

    # ── Reads ─────────────────────────────────────────────────────────────

    def get_run(self, run_id: int) -> dict[str, Any] | None: ...

    def get_run_results(
        self, run_id: int, *, unevaluated_only: bool = False
    ) -> list[dict[str, Any]]: ...

    def list_recent_runs(
        self, limit: int = 20, *, command_filter: str | None = "analyze.run"
    ) -> list[dict[str, Any]]: ...

    def list_runs_with_result_counts(self, limit: int = 20) -> list[dict[str, Any]]: ...

    def find_runs_for_scope(
        self,
        *,
        schema: str | None = None,
        table: str | None = None,
        command_filter: str | None = None,
        limit: int = 5,
    ) -> list[dict[str, Any]]: ...

    def list_recent_events(self, limit: int = 30) -> list[dict[str, Any]]: ...

    # ── Misc ──────────────────────────────────────────────────────────────

    def log_event(
        self,
        *,
        event_type: str,
        status: str,
        command: str,
        details: dict[str, Any] | None = None,
    ) -> None: ...

    def set_session_state(self, namespace: str, key: str, value: Any) -> None: ...

    def get_session_state(self, namespace: str, key: str, default: Any = None) -> Any: ...

    # ── Scheduled runs (Phase 1) ──────────────────────────────────────────
    #
    # Local SQLite is the source of truth; the shared SQLAlchemy mirror
    # is a follow-up. DualWriteHistoryStore delegates these to the
    # local store and skips the remote write until the shared schema
    # ships.

    def create_scheduled_run(
        self,
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
    ) -> int: ...

    def get_scheduled_run(self, schedule_id: int) -> dict[str, Any] | None: ...

    def list_scheduled_runs(
        self,
        *,
        statuses: list[str] | None = None,
        db_profile: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]: ...

    def list_due_pending_schedules(
        self, *, now_utc: float, limit: int = 200
    ) -> list[dict[str, Any]]: ...

    def update_scheduled_run(self, schedule_id: int, *, patch: dict[str, Any]) -> None: ...

    def set_scheduled_run_status(
        self,
        schedule_id: int,
        status: str,
        *,
        last_error: str | None = None,
        fired_at: float | None = None,
        triggered_run_id: int | None = None,
    ) -> None: ...

    def delete_scheduled_run(self, schedule_id: int) -> None: ...

    def claim_due_schedule(self, *, now_utc: float) -> int | None: ...

    def set_run_schedule_link(self, run_id: int, schedule_id: int) -> None: ...

    def update_run_heartbeat(self, run_id: int, *, now_utc: float | None = None) -> None: ...

    def update_run_current_step(self, run_id: int, label: str) -> None: ...

    def recover_stale_runs(
        self,
        *,
        threshold_sec: float = 300.0,
        now_utc: float | None = None,
    ) -> list[int]: ...
