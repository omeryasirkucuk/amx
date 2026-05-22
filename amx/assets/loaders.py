"""Read ``remote_*`` rows from the local history store and produce
:class:`amx.assets.types.AssetDocument` chunks for the asset-RAG
pipeline.

Each loader is responsible for one asset kind. ``load_asset_documents``
is the dispatcher used by :class:`amx.assets.rag.AssetRAGStore.ingest_profile`
to fan out across all kinds for a given DB profile.

Per-asset overrides live in ``asset_chunking_overrides``. When a row
matches ``(profile_name, kind, remote_id)`` the loader hydrates a
fresh kind-specific config for that asset, merging the override's
strategy / chunk_chars / chunk_overlap on top of the global default
so the splitter sees the user's per-row choice. Absent override =
inherit ``AssetChunkingConfig`` unchanged.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from amx.assets.chunking_config import (
    AssetChunkingConfig,
    NotebookChunkingConfig,
    PipelineChunkingConfig,
    QueryChunkingConfig,
)
from amx.assets.splitters import (
    split_job,
    split_notebook,
    split_pipeline,
    split_query,
    split_stream,
    split_streamlit,
)
from amx.assets.types import AssetDocument


def _load_overrides_for_kind(conn: Any, profile_name: str, kind: str) -> dict[int, dict[str, Any]]:
    """Per-asset chunking overrides keyed by ``remote_id``.

    Schema absence is tolerated for older history stores that pre-date
    PR-B; the legacy path returns no overrides and the global config
    flows through.
    """
    try:
        rows = conn.execute(
            "SELECT remote_id, strategy, chunk_chars, chunk_overlap "
            "FROM asset_chunking_overrides "
            "WHERE profile_name = ? AND kind = ?",
            (profile_name, kind),
        ).fetchall()
    except Exception:  # noqa: BLE001 — older history.db without the table
        return {}
    return {
        int(rid): {
            "strategy": str(strategy),
            "chunk_chars": int(chars) if chars is not None else None,
            "chunk_overlap": int(overlap) if overlap is not None else None,
        }
        for rid, strategy, chars, overlap in rows
    }


def _apply_notebook_override(
    base: NotebookChunkingConfig | None, override: dict[str, Any]
) -> NotebookChunkingConfig:
    fallback = base or NotebookChunkingConfig()
    return replace(
        fallback,
        strategy=override["strategy"],
        chunk_chars=override["chunk_chars"]
        if override["chunk_chars"] is not None
        else fallback.chunk_chars,
        chunk_overlap=override["chunk_overlap"]
        if override["chunk_overlap"] is not None
        else fallback.chunk_overlap,
    )


def _apply_query_override(
    base: QueryChunkingConfig | None, override: dict[str, Any]
) -> QueryChunkingConfig:
    fallback = base or QueryChunkingConfig()
    return replace(
        fallback,
        strategy=override["strategy"],
        chunk_chars=override["chunk_chars"]
        if override["chunk_chars"] is not None
        else fallback.chunk_chars,
        chunk_overlap=override["chunk_overlap"]
        if override["chunk_overlap"] is not None
        else fallback.chunk_overlap,
    )


def _apply_pipeline_override(
    base: PipelineChunkingConfig | None, override: dict[str, Any]
) -> PipelineChunkingConfig:
    fallback = base or PipelineChunkingConfig()
    return replace(fallback, strategy=override["strategy"])


def load_notebook_documents(
    *,
    conn: Any,
    profile_name: str,
    only_ids: list[int] | None = None,
    chunking: AssetChunkingConfig | None = None,
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, workspace_path, source_text FROM remote_notebooks {where}",
        params,
    ).fetchall()
    nb_cfg = chunking.notebook if chunking else None
    overrides = _load_overrides_for_kind(conn, profile_name, "notebook")
    out: list[AssetDocument] = []
    for nb_id, name, workspace_path, source_text in rows:
        per_asset_cfg = nb_cfg
        if int(nb_id) in overrides:
            per_asset_cfg = _apply_notebook_override(nb_cfg, overrides[int(nb_id)])
        out.extend(
            split_notebook(
                profile=profile_name,
                remote_id=int(nb_id),
                name=str(name or ""),
                source_text=str(source_text or ""),
                workspace_path=str(workspace_path or ""),
                config=per_asset_cfg,
            )
        )
    return out


def load_query_documents(
    *,
    conn: Any,
    profile_name: str,
    only_ids: list[int] | None = None,
    chunking: AssetChunkingConfig | None = None,
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, kind, sql_text, warehouse FROM remote_queries {where}",
        params,
    ).fetchall()
    q_cfg = chunking.query if chunking else None
    overrides = _load_overrides_for_kind(conn, profile_name, "query")
    out: list[AssetDocument] = []
    for q_id, name, kind_value, sql_text, warehouse in rows:
        per_asset_cfg = q_cfg
        if int(q_id) in overrides:
            per_asset_cfg = _apply_query_override(q_cfg, overrides[int(q_id)])
        out.extend(
            split_query(
                profile=profile_name,
                remote_id=int(q_id),
                name=str(name or ""),
                sql_text=str(sql_text or ""),
                warehouse=str(warehouse or ""),
                kind_value=str(kind_value or "saved"),
                config=per_asset_cfg,
            )
        )
    return out


def load_pipeline_documents(
    *,
    conn: Any,
    profile_name: str,
    only_ids: list[int] | None = None,
    chunking: AssetChunkingConfig | None = None,
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, target_schema, edition, continuous, photon, "
        f"libraries_json, latest_update_state FROM remote_pipelines {where}",
        params,
    ).fetchall()
    p_cfg = chunking.pipeline if chunking else None
    overrides = _load_overrides_for_kind(conn, profile_name, "pipeline")
    out: list[AssetDocument] = []
    for p_id, name, target, edition, continuous, photon, libs_json, latest_state in rows:
        per_asset_cfg = p_cfg
        if int(p_id) in overrides:
            per_asset_cfg = _apply_pipeline_override(p_cfg, overrides[int(p_id)])
        out.extend(
            split_pipeline(
                profile=profile_name,
                remote_id=int(p_id),
                name=str(name or ""),
                target_schema=str(target or ""),
                libraries_json=str(libs_json or "[]"),
                edition=str(edition or ""),
                continuous=bool(continuous),
                photon=bool(photon),
                latest_update_state=str(latest_state or ""),
                config=per_asset_cfg,
            )
        )
    return out


def load_stream_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, qualified_name, source_table_fqn, mode, stale_after, owner "
        f"FROM remote_streams {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for s_id, qname, source_fqn, mode, stale, owner in rows:
        out.extend(
            split_stream(
                profile=profile_name,
                remote_id=int(s_id),
                qualified_name=str(qname or ""),
                source_table_fqn=str(source_fqn or ""),
                mode=str(mode or ""),
                stale_after=str(stale or ""),
                owner=str(owner or ""),
            )
        )
    return out


def load_streamlit_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, qualified_name, main_file, query_warehouse, root_location, owner "
        f"FROM remote_streamlit_apps {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for a_id, qname, main_file, warehouse, root, owner in rows:
        out.extend(
            split_streamlit(
                profile=profile_name,
                remote_id=int(a_id),
                qualified_name=str(qname or ""),
                main_file=str(main_file or ""),
                query_warehouse=str(warehouse or ""),
                root_location=str(root or ""),
                owner=str(owner or ""),
            )
        )
    return out


def load_job_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, creator_user_name, schedule_cron, schedule_timezone, "
        f"last_run_status FROM remote_jobs {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for j_id, name, creator, cron, tz, last_status in rows:
        # Fetch tasks for this job (small table) so the chunk surfaces the
        # task list inline. Avoids a second top-level loop.
        task_rows = conn.execute(
            "SELECT task_key, task_type, notebook_path, sql_query_id, pipeline_id_fk "
            "FROM remote_job_tasks WHERE job_id_fk = ? LIMIT 50",
            (int(j_id),),
        ).fetchall()
        tasks = [
            {
                "task_key": tk,
                "task_type": tt,
                "notebook_path": np,
                "sql_query_id": sq,
                "pipeline_id_fk": pi,
            }
            for (tk, tt, np, sq, pi) in task_rows
        ]
        out.extend(
            split_job(
                profile=profile_name,
                remote_id=int(j_id),
                name=str(name or ""),
                creator=str(creator or ""),
                schedule_cron=str(cron or ""),
                schedule_timezone=str(tz or ""),
                last_run_status=str(last_status or ""),
                tasks=tasks,
            )
        )
    return out


_LOADERS = {
    "notebook": load_notebook_documents,
    "query": load_query_documents,
    "pipeline": load_pipeline_documents,
    "stream": load_stream_documents,
    "streamlit_app": load_streamlit_documents,
    "job": load_job_documents,
}


def load_asset_documents(
    *,
    conn: Any,
    profile_name: str,
    kinds: list[str] | None = None,
    only_ids: dict[str, list[int]] | None = None,
    chunking: AssetChunkingConfig | None = None,
) -> list[AssetDocument]:
    """Dispatch across asset kinds. Default loads every kind.

    ``chunking`` is plumbed through to the per-kind loaders that
    support strategy switching (notebook / query / pipeline). The
    metadata-only loaders (stream / streamlit / job) ignore it.
    """
    if kinds is None:
        kinds = list(_LOADERS.keys())
    out: list[AssetDocument] = []
    for kind in kinds:
        loader = _LOADERS.get(kind)
        if loader is None:
            continue
        scoped_ids = (only_ids or {}).get(kind)
        if kind in {"notebook", "query", "pipeline"}:
            out.extend(
                loader(
                    conn=conn,
                    profile_name=profile_name,
                    only_ids=scoped_ids,
                    chunking=chunking,
                )
            )
        else:
            out.extend(loader(conn=conn, profile_name=profile_name, only_ids=scoped_ids))
    return out


def _profile_filter(profile_name: str, only_ids: list[int] | None) -> tuple[str, tuple[Any, ...]]:
    if only_ids:
        placeholders = ",".join("?" for _ in only_ids)
        where = f"WHERE profile_name = ? AND id IN ({placeholders})"
        return where, (profile_name, *only_ids)
    return "WHERE profile_name = ?", (profile_name,)


__all__ = [
    "load_asset_documents",
    "load_job_documents",
    "load_notebook_documents",
    "load_pipeline_documents",
    "load_query_documents",
    "load_stream_documents",
    "load_streamlit_documents",
]
