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
            conf = Confidence(conf_raw) if conf_raw in ("high", "medium", "low") else Confidence.MEDIUM
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
