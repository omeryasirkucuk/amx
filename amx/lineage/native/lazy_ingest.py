"""On-demand single-asset ingest for native-lineage canvas nodes.

Clicking a name-only notebook / job / pipeline / query on the lineage
canvas pulls just that one asset's content into the local ``remote_*``
store (the Assets cache), turning the node into a full, drillable,
searchable, RAG-queryable asset. Per-kind mechanism:

* job / pipeline — :class:`IngestAssetsService` single-id ``selection``
  (a direct REST get by id).
* query — :func:`ingest_query_by_id` (one ``sql/queries`` get).
* notebook — exported by its workspace **path**, looked up in the
  persisted :mod:`notebook_index` (Databricks has no id->path lookup). A
  cold index returns ``"indexing"`` and kicks off the background build.

No work happens at lineage-fetch time; reopening a cached asset re-runs
nothing.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Literal

from amx.lineage.native import notebook_index
from amx.lineage.native.ingest import ingest_query_by_id
from amx.services.ingest_assets import IngestAssetsService, IngestRequest

# Canvas asset kind -> (IngestAssetsService asset_type, remote table, id column).
_KIND_SPEC: dict[str, tuple[str, str, str]] = {
    "notebook": ("notebooks", "remote_notebooks", "external_id"),
    "job": ("jobs", "remote_jobs", "job_id"),
    "pipeline": ("pipelines", "remote_pipelines", "pipeline_id"),
    "query": ("queries", "remote_queries", "external_id"),
}

# Kinds ingested via IngestAssetsService single-id selection (direct REST by id).
_SELECTION_KINDS = ("job", "pipeline")


@dataclass(frozen=True)
class IngestOutcome:
    """Result of a single-asset ingest.

    ``ok`` carries the ``remote_<kind>s.id``; ``indexing`` means the
    notebook index is still building (retry shortly); ``unavailable`` means
    the asset could not be ingested (no access / not an ingestable kind).
    """

    status: Literal["ok", "indexing", "unavailable"]
    remote_id: int | None = None


def selection_request_for(*, profile: str, kind: str, external_id: str) -> IngestRequest | None:
    """Build a single-asset ``IngestRequest`` for a selection-ingestable kind, else ``None``."""
    if kind not in _SELECTION_KINDS or not external_id:
        return None
    asset_type, _table, _id_col = _KIND_SPEC[kind]
    return IngestRequest(
        profile_name=profile,
        types=[asset_type],  # type: ignore[list-item]
        selection={asset_type: [external_id]},
    )


def ingest_one_asset(
    *, connector: Any, catalog: Any, profile: str, kind: str, external_id: str
) -> IngestOutcome:
    """Ingest one lineage asset into the Assets store; return the outcome."""
    if not external_id or kind not in _KIND_SPEC:
        return IngestOutcome("unavailable")
    if kind == "notebook":
        return _ingest_notebook(connector, catalog, profile, external_id)
    if kind == "query":
        return _ingest_query(connector, catalog, profile, external_id)
    return _ingest_by_selection(connector, catalog, profile, kind, external_id)


def _remote_id_after_commit(
    catalog: Any, table: str, id_col: str, profile: str, external_id: str
) -> int | None:
    with catalog._connect() as conn:
        row = conn.execute(
            f"SELECT id FROM {table} WHERE profile_name = ? AND {id_col} = ?",  # noqa: S608
            (profile, external_id),
        ).fetchone()
    return int(row[0]) if row else None


def _ingest_by_selection(
    connector: Any, catalog: Any, profile: str, kind: str, external_id: str
) -> IngestOutcome:
    req = selection_request_for(profile=profile, kind=kind, external_id=external_id)
    if req is None:
        return IngestOutcome("unavailable")
    IngestAssetsService(connector=connector, catalog=catalog).run(req)
    _at, table, id_col = _KIND_SPEC[kind]
    rid = _remote_id_after_commit(catalog, table, id_col, profile, external_id)
    return IngestOutcome("ok", rid) if rid is not None else IngestOutcome("unavailable")


def _ingest_query(connector: Any, catalog: Any, profile: str, external_id: str) -> IngestOutcome:
    client = getattr(connector, "workspace_client", None)
    if client is None:
        return IngestOutcome("unavailable")
    with catalog._connect() as conn:
        rid = ingest_query_by_id(conn, profile, client, catalog, external_id)
    return IngestOutcome("ok", rid) if rid is not None else IngestOutcome("unavailable")


def _ingest_notebook(connector: Any, catalog: Any, profile: str, external_id: str) -> IngestOutcome:
    client = getattr(connector, "workspace_client", None)
    host = getattr(client, "host", "") if client is not None else ""
    idx_path = notebook_index.cache_path(catalog.db_path.parent, profile, host)
    nb_path = notebook_index.lookup_path(idx_path, external_id)
    if nb_path is None:
        # No id->path mapping yet (cold/stale index). Databricks offers no
        # reverse lookup, so build the index in the background and ask the
        # caller to retry once it's ready — never block on the scan here.
        if client is not None:
            notebook_index.ensure_background_build(client, idx_path)
        return IngestOutcome("indexing")
    dtos = list(connector.list_remote_notebooks_by_specs([(external_id, nb_path)]))
    if not dtos:
        return IngestOutcome("unavailable")
    with catalog._connect() as conn:
        catalog._upsert_remote_notebooks(conn, profile, dtos, _iso_now())
        row = conn.execute(
            "SELECT id FROM remote_notebooks WHERE profile_name = ? AND external_id = ?",
            (profile, external_id),
        ).fetchone()
    return IngestOutcome("ok", int(row[0])) if row else IngestOutcome("unavailable")


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


__all__ = ["IngestOutcome", "selection_request_for", "ingest_one_asset"]
