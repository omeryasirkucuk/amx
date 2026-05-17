"""Result query helpers for :class:`SQLiteHistoryStore`.

Extracted from :mod:`amx.storage.sqlite_store`. Three read-only
queries the rest of AMX uses heavily:

- get_run_results / get_run_result — SELECT run_results, deserialise
  alternatives_json via :func:`parse_alternatives_json`.
- list_runs_with_result_counts — SELECT analysis_runs with computed
  per-run aggregates (count of results by status etc.).

Each function takes the store as ``hs`` and uses its ``_connect()``
plumbing. No DDL touched.
"""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore


def get_run_results(
    hs: SQLiteHistoryStore,
    run_id: int,
    *,
    unevaluated_only: bool = False,
) -> list[dict[str, Any]]:
    """Return all (or unevaluated) result rows for a given run."""
    query = "SELECT * FROM run_results WHERE run_id = ?"
    if unevaluated_only:
        query += " AND (evaluation IS NULL OR evaluation = '')"
    query += " ORDER BY id"
    with hs._connect() as conn:
        rows = conn.execute(query, (int(run_id),)).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        raw = d.get("alternatives_json")
        if isinstance(raw, str) and raw:
            with contextlib.suppress(Exception):
                d["alternatives_json"] = json.loads(raw)
        cite_raw = d.get("citations_json")
        if isinstance(cite_raw, str) and cite_raw:
            with contextlib.suppress(Exception):
                d["citations_json"] = json.loads(cite_raw)
        out.append(d)
    return out


def get_run_result(hs: SQLiteHistoryStore, result_id: int) -> dict[str, Any] | None:
    """Return one ``run_results`` row by id, alternatives parsed."""
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT * FROM run_results WHERE id = ?",
            (int(result_id),),
        ).fetchone()
    if row is None:
        return None
    d = dict(row)
    raw = d.get("alternatives_json")
    if isinstance(raw, str) and raw:
        with contextlib.suppress(Exception):
            d["alternatives_json"] = json.loads(raw)
    cite_raw = d.get("citations_json")
    if isinstance(cite_raw, str) and cite_raw:
        with contextlib.suppress(Exception):
            d["citations_json"] = json.loads(cite_raw)
    return d


def list_runs_with_result_counts(hs: SQLiteHistoryStore, limit: int = 20) -> list[dict[str, Any]]:
    """List recent runs augmented with pending evaluation count."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.started_at,
                r.ended_at,
                r.duration_sec,
                r.status,
                r.mode,
                r.db_backend,
                r.db_profile,
                r.llm_provider,
                r.llm_model,
                r.llm_profile,
                r.doc_profile,
                r.code_profile,
                r.scope_json,
                COUNT(rr.id)          AS total_alternatives,
                SUM(CASE WHEN rr.evaluation IS NULL OR rr.evaluation = ''
                         THEN 1 ELSE 0 END) AS pending_count
            FROM analysis_runs r
            LEFT JOIN run_results rr ON rr.run_id = r.id
            GROUP BY r.id
            ORDER BY r.started_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        raw = d.get("scope_json")
        if isinstance(raw, str) and raw:
            with contextlib.suppress(Exception):
                d["scope_json"] = json.loads(raw)
        out.append(d)
    return out
