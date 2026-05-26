"""On-demand single-asset ingest for native-lineage canvas nodes.

When the user clicks a name-only notebook / job / pipeline on the
lineage canvas, this pulls just that one asset's content into the
local ``remote_*`` store (the Assets cache) so the node becomes a
full, drillable asset. Reuses :class:`IngestAssetsService` with a
single-id ``selection`` — no full re-ingest and no work at
lineage-fetch time. Reopening a cached asset never re-runs this.
"""

from __future__ import annotations

from typing import Any

from amx.services.ingest_assets import IngestAssetsService, IngestRequest

# Canvas asset kind -> (IngestAssetsService asset_type, remote table, id column).
# Only kinds that IngestAssetsService can select by external id are eligible;
# dashboards / vector indexes / external have no per-id ingest and stay name-only.
_KIND_SPEC: dict[str, tuple[str, str, str]] = {
    "notebook": ("notebooks", "remote_notebooks", "external_id"),
    "job": ("jobs", "remote_jobs", "job_id"),
    "pipeline": ("pipelines", "remote_pipelines", "pipeline_id"),
}


def selection_request_for(*, profile: str, kind: str, external_id: str) -> IngestRequest | None:
    """Build a single-asset ``IngestRequest``, or ``None`` if not ingestable."""
    spec = _KIND_SPEC.get(kind)
    if spec is None or not external_id:
        return None
    asset_type, _table, _id_col = spec
    return IngestRequest(
        profile_name=profile,
        types=[asset_type],  # type: ignore[list-item]
        selection={asset_type: [external_id]},
    )


def ingest_one_asset(
    *, connector: Any, catalog: Any, profile: str, kind: str, external_id: str
) -> int | None:
    """Ingest one asset and return its ``remote_<kind>s.id`` (cached), or None."""
    req = selection_request_for(profile=profile, kind=kind, external_id=external_id)
    if req is None:
        return None
    IngestAssetsService(connector=connector, catalog=catalog).run(req)
    _asset_type, table, id_col = _KIND_SPEC[kind]
    with catalog._connect() as conn:
        row = conn.execute(
            f"SELECT id FROM {table} WHERE profile_name = ? AND {id_col} = ?",  # noqa: S608
            (profile, external_id),
        ).fetchone()
    return int(row[0]) if row else None
