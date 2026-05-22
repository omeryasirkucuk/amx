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
    # PR-A: optional per-kind selection from the Studio / CLI
    # "browse and pick" wizard. Keys are asset_type strings (e.g.
    # ``"notebooks"``); values are the platform-native external_id
    # set the user explicitly chose. When the key is absent (or the
    # whole dict is ``None``) the connector falls back to its
    # pre-PR-A "all" behaviour for that kind. ``queries`` and
    # ``task_dependencies`` are never selected here — they're
    # time-windowed in bulk by ``history_days`` / ``query_history_limit``.
    selection: dict[str, list[str]] | None = None
    # Ingest is pull-only by default — chunking + embedding is a
    # separate user-driven step (the per-asset "Chunk" button on the
    # Studio Assets table, or /db assets reindex). Earlier builds
    # auto-chunked with the global default strategy, which surprised
    # users who wanted to pick chunking per asset before paying the
    # embed cost. Set ``auto_chunk_embed=True`` to opt back into the
    # legacy "ingest = pull + chunk + embed" behaviour.
    auto_chunk_embed: bool = False


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

        # Materialise asset-to-asset lineage edges (job → notebook /
        # pipeline / query, task DAG, pipeline → notebook / target
        # table) for the Studio Lineage panel. Best-effort: errors
        # are surfaced via the failures map but do not block the
        # rest of the refresh.
        asset_edge_total = 0
        try:
            from amx.assets.lineage import LineageExtractor
            from amx.storage.sqlite_store import history_store

            hs = history_store()
            if hs is not None:
                with hs._connect() as conn:  # noqa: SLF001 — internal helper
                    extractor = LineageExtractor(conn)
                    asset_edge_total = extractor.extract_for_profile(request.profile_name)
        except Exception as exc:  # noqa: BLE001
            failures["asset_lineage_edges"] = str(exc)

        emit(
            IngestProgressEvent(
                asset_type="lineage",
                state="completed",
                count=lineage_total + asset_edge_total,
            )
        )

        # Chunking + embedding is gated behind ``request.auto_chunk_embed``.
        # The default flow is "pull only": Studio surfaces the raw
        # remote_* rows on the Assets table immediately, and the user
        # then picks a chunking strategy per asset via the "Chunk"
        # action. The legacy "ingest = pull + chunk + embed" path is
        # available via ``auto_chunk_embed=True`` for callers that
        # need the index built in one shot (e.g. CLI batch scripts).
        indexed_total = 0
        if request.auto_chunk_embed:
            try:
                emit(IngestProgressEvent(asset_type="indexing", state="started"))
                from amx.assets.rag import AssetRAGStore
                from amx.storage.sqlite_store import history_store

                hs = history_store()
                if hs is not None:
                    store = AssetRAGStore()
                    with hs._connect() as conn:  # noqa: SLF001
                        indexed_total = store.ingest_profile(
                            conn=conn, profile_name=request.profile_name
                        )
                        # Strip Chroma entries whose source row was
                        # removed since the last ingest so search
                        # never opens a drawer for a deleted asset.
                        try:
                            store.prune_stale_vectors(conn, request.profile_name)
                        except Exception:  # noqa: BLE001 — best-effort
                            pass
                emit(
                    IngestProgressEvent(
                        asset_type="indexing",
                        state="completed",
                        count=indexed_total,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — indexing is best-effort
                emit(
                    IngestProgressEvent(
                        asset_type="indexing",
                        state="failed",
                        message=str(exc),
                    )
                )
        else:
            emit(
                IngestProgressEvent(
                    asset_type="indexing",
                    state="skipped",
                    message=(
                        "Chunking + embedding skipped. Use the Chunk action "
                        "per asset (Studio Assets table) or `/db assets reindex` "
                        "to embed under a chosen strategy."
                    ),
                )
            )

        counts["lineage"] = lineage_total
        counts["asset_lineage_edges"] = asset_edge_total
        counts["indexed_chunks"] = indexed_total
        return IngestResult(counts=counts, failures=failures)

    def _pull(self, asset_type: str, req: IngestRequest) -> Iterable:
        c = self._connector
        # PR-A: per-kind selection narrows the iterator to a
        # caller-chosen subset of platform-native external_ids.
        # ``queries`` and ``task_dependencies`` ignore the selection
        # — they're time-windowed aggregates, not per-asset rows.
        sel = (req.selection or {}).get(asset_type)
        if asset_type == "notebooks":
            return c.list_remote_notebooks(external_id_filter=sel)
        if asset_type == "jobs":
            return c.list_remote_jobs(runs_per_job=req.runs_per_job, external_id_filter=sel)
        if asset_type == "pipelines":
            return c.list_remote_pipelines(external_id_filter=sel)
        if asset_type == "streamlit_apps":
            return c.list_remote_streamlit_apps(external_id_filter=sel)
        if asset_type == "streams":
            return c.list_remote_streams(external_id_filter=sel)
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
