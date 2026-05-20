"""Save/load last approved metadata for a later `amx analyze apply` run."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult

PENDING_FILE = Path.home() / ".amx" / "pending_metadata.json"


def save_pending(results: list[ReviewResult]) -> Path:
    """Persist approved rows (applied=True, non-empty description) for later DB write."""
    rows: list[dict[str, Any]] = []
    for r in results:
        if not r.applied or not (r.final_description or "").strip():
            continue
        rows.append(
            {
                "schema": r.schema,
                "table": r.table,
                "column": r.column,
                "result_id": r.result_id,
                "final_description": r.final_description,
                "confidence": r.confidence.value,
                "source": r.source,
                "asset_kind": getattr(r, "asset_kind", "table"),
            }
        )
    PENDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    PENDING_FILE.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return PENDING_FILE


def load_pending() -> list[ReviewResult]:
    if not PENDING_FILE.is_file():
        return []
    raw = json.loads(PENDING_FILE.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        return []
    out: list[ReviewResult] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        try:
            conf_raw = str(row.get("confidence", "medium")).lower()
            conf = (
                Confidence(conf_raw) if conf_raw in ("high", "medium", "low") else Confidence.MEDIUM
            )
        except Exception:
            conf = Confidence.MEDIUM
        out.append(
            ReviewResult(
                schema=str(row.get("schema", "")),
                table=str(row.get("table", "")),
                column=row.get("column"),
                final_description=str(row.get("final_description", "")),
                confidence=conf,
                source=str(row.get("source", "pending")),
                applied=True,
                asset_kind=str(row.get("asset_kind", "table")),
                result_id=(int(row["result_id"]) if row.get("result_id") is not None else None),
            )
        )
    return out


def clear_pending() -> None:
    if PENDING_FILE.is_file():
        PENDING_FILE.unlink()


def _resolve_run_id_for_result(result_id: int | None) -> int | None:
    """Look up the ``analysis_runs.id`` that owns ``result_id`` in run_results.

    Returns ``None`` if the history store is unavailable, the result row
    is gone, or the lookup raises. Best-effort: failure to resolve only
    drops the deep-link field on the snapshot response, never the merge
    itself.
    """
    if result_id is None:
        return None
    try:
        from amx.storage._history_results import get_run_result
        from amx.storage.sqlite_store import history_store as _hs

        hs = _hs()
        if hs is None:
            return None
        row = get_run_result(hs, int(result_id))
        if not row:
            return None
        rid = row.get("run_id")
        return int(rid) if rid is not None else None
    except Exception:
        return None


def read_pending_for_table(schema: str, table: str) -> dict[str, Any]:
    """Return the pending entries for one ``(schema, table)`` pair.

    Used by the Studio Table snapshot endpoint so a generated description
    stays visible after page refresh: the live-DB snapshot has no
    description yet, but the pending queue does, and the Table page now
    surfaces both.

    Returns a dict with three keys:

    * ``description`` — the most recent table-level pending description,
      or ``None`` when no table-level entry exists.
    * ``columns`` — a ``{column_name: description}`` map of pending
      column descriptions. Latest entry wins on duplicate column.
    * ``run_id`` — the ``analysis_runs`` id stitched onto the latest
      result, or ``None`` if no entry has one. Lets the frontend deep-
      link to the run that produced the pending entry.

    Multi-profile note: ``pending_metadata.json`` does not currently
    track the originating ``db_profile`` / ``database`` / ``catalog``,
    so entries for same-named tables across profiles can collide on
    read. Acceptable for the common single-profile case; multi-profile
    disambiguation is tracked separately.
    """
    rows = load_pending()
    table_desc: str | None = None
    columns: dict[str, str] = {}
    run_id: int | None = None
    latest_result_id = -1
    for r in rows:
        if r.schema != schema or r.table != table:
            continue
        desc = (r.final_description or "").strip()
        if not desc:
            continue
        if r.column:
            columns[r.column] = desc
        else:
            table_desc = desc
        # The newest write wins on ``run_id``: we sort by ``result_id``
        # which is monotonically assigned by the history store, so the
        # latest write has the largest value. ``None`` (rare on the
        # singleshot path) keeps the prior best.
        rid = r.result_id if r.result_id is not None else -1
        if rid > latest_result_id:
            latest_result_id = rid
            run_id = _resolve_run_id_for_result(r.result_id)
    return {
        "description": table_desc,
        "columns": columns,
        "run_id": run_id,
    }
