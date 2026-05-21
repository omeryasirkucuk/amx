"""Render ingested remote assets as context blocks for the pages composer.

Routes a ``<profile>:<asset_id>`` ref + asset kind to the matching
``remote_*`` SQLite row and emits a single text block per asset,
capped per kind so a 100 KB notebook cannot blow the 60 KB
``gather()`` budget on its own.

The local history database is the canonical store; the resolver
opens its own short-lived connection rather than holding one shared
with other subsystems.
"""

from __future__ import annotations

import json
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("pages.resolvers.remote_assets")

_NOTEBOOK_CAP = 10 * 1024
_NOTEBOOK_RAW_CAP = 4 * 1024
_JOB_CAP = 3 * 1024
_PIPELINE_CAP = 2 * 1024
_QUERY_CAP = 5 * 1024
_STREAM_CAP = 800
_STREAMLIT_CAP = 800
_NOTEBOOK_CODE_CELLS = 8
_NOTEBOOK_RECENT_RUNS = 3


class RemoteAssetResolver:
    """Resolve ``asset_*`` page-asset refs into LLM context blocks.

    ``history`` is the active :class:`SQLiteHistoryStore`. The
    resolver issues a fresh connection per call via the store's
    ``_connect`` context manager.
    """

    def __init__(self, history: Any) -> None:
        self.history = history

    def resolve_asset(self, ref: str, kind: str) -> str:
        try:
            return self._resolve(ref, kind)
        except Exception as exc:  # noqa: BLE001
            log.debug("resolve_asset(%s, %s) failed: %s", ref, kind, exc)
            return f"asset {ref} not found"

    def _resolve(self, ref: str, kind: str) -> str:
        profile, asset_id = _split_ref(ref)
        if not profile or not asset_id:
            return f"asset {ref} not found"

        if kind == "asset_notebook":
            return self._notebook(profile, asset_id)
        if kind == "asset_job":
            return self._job(profile, asset_id)
        if kind == "asset_pipeline":
            return self._pipeline(profile, asset_id)
        if kind == "asset_query":
            return self._query(profile, asset_id)
        if kind == "asset_stream":
            return self._stream(profile, asset_id)
        if kind == "asset_streamlit":
            return self._streamlit(profile, asset_id)
        return f"asset {ref} not found"

    # ── per-kind renderers ──────────────────────────────────────────

    def _notebook(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT name, workspace_path, qualified_name, language, "
            "source_text, last_modified_at, owner, cell_count "
            "FROM remote_notebooks WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"notebook {profile}:{asset_id} not found"
        name = row[0] or asset_id
        location = row[1] or row[2] or ""
        language = row[3] or "unknown"
        source_text = row[4] or ""
        last_modified = row[5] or ""
        owner = row[6] or ""
        cell_count = row[7] or 0

        header = (
            f"## NOTEBOOK `{name}` @ `{location}`\n"
            f"- profile: {profile}\n"
            f"- language: {language}\n"
            f"- cells: {cell_count}\n"
            f"- owner: {owner}\n"
            f"- last modified: {last_modified}\n\n"
        )
        body = _excerpt_notebook(source_text, _NOTEBOOK_CODE_CELLS)
        if not body:
            body = _truncate(source_text, _NOTEBOOK_RAW_CAP, "[truncated]")
        block = header + body
        return _truncate(block, _NOTEBOOK_CAP, "\n\n[notebook excerpt truncated]")

    def _job(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT name, creator_user_name, schedule_cron, schedule_timezone, "
            "last_run_status, success_rate_30d "
            "FROM remote_jobs WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"job {profile}:{asset_id} not found"
        name, creator, cron, tz, last_status, success_rate = row
        tasks = self._fetch_all(
            "SELECT task_key, task_type, notebook_path, sql_query_id, pipeline_id_fk "
            "FROM remote_job_tasks WHERE job_id_fk = ? LIMIT 50",
            (asset_id,),
        )
        runs = self._fetch_all(
            "SELECT state_result, start_time, execution_duration_ms "
            "FROM remote_job_runs WHERE job_id_fk = ? "
            "ORDER BY start_time DESC LIMIT ?",
            (asset_id, _NOTEBOOK_RECENT_RUNS),
        )
        lines = [
            f"## JOB `{name or asset_id}`",
            f"- profile: {profile}",
            f"- creator: {creator or 'unknown'}",
            f"- schedule: {cron or 'manual'} ({tz or 'n/a'})",
            f"- last run status: {last_status or 'unknown'}",
            f"- 30-day success rate: {success_rate if success_rate is not None else 'n/a'}",
        ]
        if tasks:
            lines.append("\n### Tasks")
            for task_key, task_type, nb_path, sql_id, pipe_id in tasks:
                target = nb_path or sql_id or pipe_id or ""
                lines.append(f"- `{task_key}` ({task_type or 'n/a'}) → {target}")
        if runs:
            lines.append("\n### Recent runs")
            for state, started, duration_ms in runs:
                lines.append(f"- {started or 'n/a'}: {state or 'n/a'} ({duration_ms or 0} ms)")
        return _truncate("\n".join(lines), _JOB_CAP, "\n\n[job summary truncated]")

    def _pipeline(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT name, target_schema, edition, continuous, photon, "
            "libraries_json, latest_update_state, latest_update_creation_time "
            "FROM remote_pipelines WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"pipeline {profile}:{asset_id} not found"
        name, target, edition, continuous, photon, libs_json, state, updated = row
        try:
            libs = json.loads(libs_json) if libs_json else []
        except (TypeError, ValueError):
            libs = []
        notebook_paths = [
            str(item.get("notebook", {}).get("path", ""))
            for item in libs
            if isinstance(item, dict) and item.get("notebook")
        ]
        lines = [
            f"## PIPELINE `{name or asset_id}`",
            f"- profile: {profile}",
            f"- target schema: {target or 'n/a'}",
            f"- edition: {edition or 'n/a'}",
            f"- continuous: {bool(continuous)}",
            f"- photon: {bool(photon)}",
            f"- latest update: {state or 'n/a'} ({updated or 'n/a'})",
        ]
        if notebook_paths:
            lines.append("\n### Notebook libraries")
            for path in notebook_paths[:20]:
                lines.append(f"- {path}")
        return _truncate("\n".join(lines), _PIPELINE_CAP, "\n\n[pipeline summary truncated]")

    def _query(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT name, kind, sql_text, warehouse, user_name, executed_at "
            "FROM remote_queries WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"query {profile}:{asset_id} not found"
        name, qkind, sql_text, warehouse, user, executed = row
        header = (
            f"## QUERY `{name or asset_id}` ({qkind or 'unknown'})\n"
            f"- profile: {profile}\n"
            f"- warehouse: {warehouse or 'n/a'}\n"
            f"- user: {user or 'n/a'}\n"
            f"- executed at: {executed or 'n/a'}\n\n"
            "```sql\n"
        )
        footer = "\n```"
        budget = _QUERY_CAP - len(header) - len(footer) - len("\n[truncated]")
        body = (sql_text or "").strip()
        if len(body) > budget > 0:
            body = body[:budget] + "\n[truncated]"
        return header + body + footer

    def _stream(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT qualified_name, source_table_fqn, mode, stale_after, owner "
            "FROM remote_streams WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"stream {profile}:{asset_id} not found"
        qname, source, mode, stale, owner = row
        block = (
            f"## STREAM `{qname or asset_id}`\n"
            f"- profile: {profile}\n"
            f"- source table: {source or 'n/a'}\n"
            f"- mode: {mode or 'n/a'}\n"
            f"- stale after: {stale or 'n/a'}\n"
            f"- owner: {owner or 'n/a'}\n"
        )
        return _truncate(block, _STREAM_CAP, "")

    def _streamlit(self, profile: str, asset_id: str) -> str:
        row = self._fetch_one(
            "SELECT qualified_name, main_file, query_warehouse, root_location, owner "
            "FROM remote_streamlit_apps WHERE profile_name = ? AND id = ?",
            (profile, asset_id),
        )
        if row is None:
            return f"streamlit app {profile}:{asset_id} not found"
        qname, main_file, warehouse, root, owner = row
        block = (
            f"## STREAMLIT APP `{qname or asset_id}`\n"
            f"- profile: {profile}\n"
            f"- main file: {main_file or 'n/a'}\n"
            f"- warehouse: {warehouse or 'n/a'}\n"
            f"- root location: {root or 'n/a'}\n"
            f"- owner: {owner or 'n/a'}\n"
        )
        return _truncate(block, _STREAMLIT_CAP, "")

    # ── plumbing ────────────────────────────────────────────────────

    def _fetch_one(self, sql: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
        if self.history is None:
            return None
        with self.history._connect() as conn:
            row = conn.execute(sql, params).fetchone()
        return tuple(row) if row is not None else None

    def _fetch_all(self, sql: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if self.history is None:
            return []
        with self.history._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [tuple(r) for r in rows]


def _split_ref(ref: str) -> tuple[str, str]:
    """Split a ``<profile>:<asset_id>`` ref. Empty on malformed input."""
    if ":" not in ref:
        return "", ""
    profile, asset_id = ref.split(":", 1)
    return profile.strip(), asset_id.strip()


def _truncate(text: str, cap: int, marker: str) -> str:
    if len(text) <= cap:
        return text
    keep = max(0, cap - len(marker))
    return text[:keep] + marker


def _excerpt_notebook(source_text: str, code_cell_limit: int) -> str:
    """Return a compact excerpt of an ``.ipynb`` JSON document.

    Emits every markdown cell followed by the first ``code_cell_limit``
    code cells. Returns an empty string on JSON parse failure so the
    caller can fall back to a raw-text truncation.
    """
    if not source_text:
        return ""
    try:
        nb = json.loads(source_text)
    except (TypeError, ValueError):
        return ""
    cells = nb.get("cells") if isinstance(nb, dict) else None
    if not isinstance(cells, list):
        return ""
    out: list[str] = []
    code_seen = 0
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        ctype = cell.get("cell_type")
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(s for s in src if isinstance(s, str))
        if not isinstance(src, str):
            continue
        if ctype == "markdown":
            out.append(src.strip())
        elif ctype == "code":
            if code_seen >= code_cell_limit:
                continue
            out.append("```\n" + src.strip() + "\n```")
            code_seen += 1
    return "\n\n".join(b for b in out if b)


__all__ = ["RemoteAssetResolver"]
