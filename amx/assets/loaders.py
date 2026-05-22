"""Read ``remote_*`` rows from the local history store and produce
:class:`amx.assets.types.AssetDocument` chunks for the asset-RAG
pipeline.

Each loader is responsible for one asset kind. ``load_asset_documents``
is the dispatcher used by :class:`amx.assets.rag.AssetRAGStore.ingest_profile`
to fan out across all kinds for a given DB profile.
"""

from __future__ import annotations

from typing import Any

from amx.assets.splitters import (
    split_job,
    split_notebook,
    split_pipeline,
    split_query,
    split_stream,
    split_streamlit,
)
from amx.assets.types import AssetDocument


def load_notebook_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, workspace_path, source_text FROM remote_notebooks {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for nb_id, name, workspace_path, source_text in rows:
        out.extend(
            split_notebook(
                profile=profile_name,
                remote_id=int(nb_id),
                name=str(name or ""),
                source_text=str(source_text or ""),
                workspace_path=str(workspace_path or ""),
            )
        )
    return out


def load_query_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, kind, sql_text, warehouse FROM remote_queries {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for q_id, name, kind_value, sql_text, warehouse in rows:
        out.extend(
            split_query(
                profile=profile_name,
                remote_id=int(q_id),
                name=str(name or ""),
                sql_text=str(sql_text or ""),
                warehouse=str(warehouse or ""),
                kind_value=str(kind_value or "saved"),
            )
        )
    return out


def load_pipeline_documents(
    *, conn: Any, profile_name: str, only_ids: list[int] | None = None
) -> list[AssetDocument]:
    where, params = _profile_filter(profile_name, only_ids)
    rows = conn.execute(
        f"SELECT id, name, target_schema, edition, continuous, photon, "
        f"libraries_json, latest_update_state FROM remote_pipelines {where}",
        params,
    ).fetchall()
    out: list[AssetDocument] = []
    for p_id, name, target, edition, continuous, photon, libs_json, latest_state in rows:
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
) -> list[AssetDocument]:
    """Dispatch across asset kinds. Default loads every kind."""
    if kinds is None:
        kinds = list(_LOADERS.keys())
    out: list[AssetDocument] = []
    for kind in kinds:
        loader = _LOADERS.get(kind)
        if loader is None:
            continue
        scoped_ids = (only_ids or {}).get(kind)
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
