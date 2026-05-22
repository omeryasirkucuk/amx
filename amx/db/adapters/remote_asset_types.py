"""Frozen dataclasses returned by the ``list_remote_*`` adapter methods.

These types are wire-format-agnostic: each adapter is responsible for
mapping its native API response shape to these structures before
returning them. Consumers (storage layer, services, CLI) depend only on
these types, not on Snowflake or Databricks specifics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class AssetMetadata:
    """Cheap-to-fetch identity row for the selective-ingest browse step.

    Adapters yield this from ``list_remote_*_metadata()`` methods —
    no source_text, no notebook export, no per-job task fan-out. Just
    enough identity for the user to decide which asset to actually
    pull. The downstream "full" ``list_remote_*()`` methods accept an
    ``external_id_filter=`` so a follow-up cherry-pick of selected
    ids skips re-listing everything from the platform.

    ``path`` is the unique-per-platform identifier the UI uses to
    disambiguate same-name assets (Databricks workspace_path or
    Snowflake qualified_name). ``owner`` and ``last_modified`` are
    best-effort decorations for the browse table; ``None`` is fine
    when the platform doesn't expose them for a given kind.
    """

    kind: str  # "notebook" | "job" | "pipeline" | "query" | "stream" | "streamlit_app"
    external_id: str
    name: str
    path: str = ""  # workspace_path (databricks) or qualified_name (snowflake)
    owner: str | None = None
    last_modified: datetime | None = None


@dataclass(frozen=True)
class RemoteNotebook:
    external_id: str
    name: str
    platform: str  # "snowflake" | "databricks"
    language: str  # python | sql | scala | r | mixed
    workspace_path: str | None  # Databricks only
    qualified_name: str | None  # Snowflake only (DB.SCHEMA.NAME)
    source_text: str  # normalized .ipynb JSON
    source_hash: str  # sha256 of source_text
    last_modified_at: datetime | None
    last_modified_by: str | None
    owner: str | None
    cell_count: int | None


@dataclass(frozen=True)
class RemoteJobTask:
    task_key: str
    task_type: str
    notebook_path: str | None
    sql_query_id: str | None
    sql_warehouse_id: str | None
    pipeline_id: str | None
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    raw_definition: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RemoteJobRun:
    run_id: int
    state_result: str  # SUCCESS | FAILED | CANCELED | RUNNING
    start_time: datetime
    end_time: datetime | None
    setup_duration_ms: int | None
    execution_duration_ms: int | None


@dataclass(frozen=True)
class RemoteJob:
    job_id: int
    name: str
    creator_user_name: str | None
    schedule_cron: str | None
    schedule_timezone: str | None
    schedule_pause_status: str | None  # PAUSED | UNPAUSED
    max_concurrent_runs: int | None
    email_notifications: dict[str, Any]
    tags: dict[str, str]
    tasks: tuple[RemoteJobTask, ...] = field(default_factory=tuple)
    recent_runs: tuple[RemoteJobRun, ...] = field(default_factory=tuple)

    def success_rate(self, *, window_days: int = 30) -> float | None:
        """Fraction of runs in the last ``window_days`` whose result was SUCCESS.

        Returns ``None`` when no runs fall inside the window — caller decides
        whether that's "unknown" or "0 of 0".
        """
        if not self.recent_runs:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
        in_window = [
            r
            for r in self.recent_runs
            if (
                r.start_time.replace(tzinfo=timezone.utc)
                if r.start_time.tzinfo is None
                else r.start_time
            )
            >= cutoff
        ]
        if not in_window:
            return None
        successes = sum(1 for r in in_window if r.state_result == "SUCCESS")
        return successes / len(in_window)


@dataclass(frozen=True)
class RemotePipeline:
    pipeline_id: str
    name: str
    target_schema: str | None
    edition: str | None
    continuous: bool
    photon: bool
    libraries: list[dict[str, Any]]  # raw library refs (notebook, file, …)
    latest_update_state: str | None
    latest_update_creation_time: datetime | None


@dataclass(frozen=True)
class RemoteStreamlitApp:
    qualified_name: str  # DB.SCHEMA.NAME
    main_file: str
    query_warehouse: str | None
    root_location: str
    owner: str | None
    last_altered_at: datetime | None


@dataclass(frozen=True)
class RemoteStream:
    qualified_name: str
    source_table_fqn: str
    mode: str  # APPEND_ONLY | DEFAULT | INSERT_ONLY
    stale_after: datetime | None
    owner: str | None


@dataclass(frozen=True)
class RemoteQuery:
    platform: str  # "snowflake" | "databricks"
    kind: str  # "saved" | "history"
    external_id: str
    name: str | None
    sql_text: str
    sql_hash: str
    warehouse: str | None
    user_name: str | None
    executed_at: datetime | None  # None for saved queries
    duration_ms: int | None
