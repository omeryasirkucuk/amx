"""Orchestrates remote-asset ingestion for a single profile.

Pulls each requested asset type from the connected adapter, calls
``Catalog.sync_remote_assets`` to upsert into storage, then runs the
lineage pass. Emits structured progress events so CLI and Studio
surfaces can show per-type completion to the user.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Literal

AssetType = Literal[
    "notebooks",
    "jobs",
    "pipelines",
    "streamlit_apps",
    "streams",
    "task_dependencies",
    "queries",
]


@dataclass(frozen=True)
class IngestRequest:
    profile_name: str
    types: list[AssetType]
    history_days: int = 7
    runs_per_job: int = 20
    query_history_limit: int = 1000


@dataclass(frozen=True)
class IngestProgressEvent:
    asset_type: str
    state: Literal["started", "completed", "failed", "skipped", "error"]
    count: int | None = None
    message: str | None = None


@dataclass(frozen=True)
class IngestResult:
    counts: dict[str, int]
    failures: dict[str, str] = field(default_factory=dict)


class IngestAssetsService:
    """Per-profile orchestrator for remote-asset ingestion + lineage rebuild."""

    def __init__(self, *, connector, catalog) -> None:
        self._connector = connector
        self._catalog = catalog

    def run(
        self,
        request: IngestRequest,
        *,
        progress: Callable[[IngestProgressEvent], None] | None = None,
    ) -> IngestResult:
        emit = progress or (lambda _evt: None)
        failures: dict[str, str] = {}
        collected: dict[str, list] = {}

        for asset_type in request.types:
            emit(IngestProgressEvent(asset_type=asset_type, state="started"))
            try:
                collected[asset_type] = list(self._pull(asset_type, request))
                emit(
                    IngestProgressEvent(
                        asset_type=asset_type,
                        state="completed",
                        count=len(collected[asset_type]),
                    )
                )
            except Exception as exc:  # noqa: BLE001 — surface per-type, keep going
                failures[asset_type] = str(exc)
                collected[asset_type] = []
                emit(
                    IngestProgressEvent(
                        asset_type=asset_type,
                        state="failed",
                        message=str(exc),
                    )
                )

        counts = self._catalog.sync_remote_assets(
            profile_name=request.profile_name,
            notebooks=collected.get("notebooks"),
            jobs=collected.get("jobs"),
            pipelines=collected.get("pipelines"),
            streamlit_apps=collected.get("streamlit_apps"),
            streams=collected.get("streams"),
            task_dependencies=collected.get("task_dependencies"),
            queries=collected.get("queries"),
        )
        emit(
            IngestProgressEvent(
                asset_type="storage",
                state="completed",
                count=sum(v for v in counts.values() if isinstance(v, int)),
            )
        )

        lineage = self._catalog.rebuild_remote_asset_lineage(profile_name=request.profile_name)
        lineage_total = sum(v for v in lineage.values() if isinstance(v, int))
        emit(
            IngestProgressEvent(
                asset_type="lineage",
                state="completed",
                count=lineage_total,
            )
        )

        counts["lineage"] = lineage_total
        return IngestResult(counts=counts, failures=failures)

    def _pull(self, asset_type: str, req: IngestRequest) -> Iterable:
        c = self._connector
        if asset_type == "notebooks":
            return c.list_remote_notebooks()
        if asset_type == "jobs":
            return c.list_remote_jobs(runs_per_job=req.runs_per_job)
        if asset_type == "pipelines":
            return c.list_remote_pipelines()
        if asset_type == "streamlit_apps":
            return c.list_remote_streamlit_apps()
        if asset_type == "streams":
            return c.list_remote_streams()
        if asset_type == "task_dependencies":
            return c.list_remote_task_dependencies()
        if asset_type == "queries":
            return c.list_remote_queries(
                history_days=req.history_days,
                limit=req.query_history_limit,
            )
        raise ValueError(f"Unknown asset_type: {asset_type}")


__all__ = [
    "IngestAssetsService",
    "IngestRequest",
    "IngestProgressEvent",
    "IngestResult",
    "AssetType",
]
