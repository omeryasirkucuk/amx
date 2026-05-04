"""System ops routes — doctor, token usage, search catalog status."""

from __future__ import annotations

import time
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from amx.cli_support.commands.doctor import collect_doctor_checks
from amx.config import AMXConfig
from amx.utils.logging import get_logger
from amx.web.deps import get_cfg

router = APIRouter(prefix="/api", tags=["system"])
log = get_logger("web.system_ops")


@router.get("/doctor")
def doctor(
    skip_network: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Run every diagnostic AMX's ``amx doctor`` runs and return them as JSON."""
    checks = collect_doctor_checks(cfg, skip_network=bool(skip_network))
    fail_count = sum(1 for c in checks if not c.ok)
    return {
        "checks": [asdict(c) for c in checks],
        "total": len(checks),
        "failed": fail_count,
        "ok": fail_count == 0,
        "skip_network": bool(skip_network),
    }


@router.get("/usage")
def usage(window: str = Query(default="7d")) -> dict[str, Any]:
    """Aggregate token consumption + approximate cost per (provider, model)."""
    from amx.cli_support.commands import usage as usage_cli
    from amx.storage.sqlite_store import history_store

    label, window_sec = usage_cli._normalize_window(window)
    hs = history_store()
    if hs is None:
        return {
            "window": label,
            "window_sec": window_sec,
            "rows": [],
            "totals": {"runs": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            "message": "History store isn't initialised yet.",
        }
    runs = hs.list_recent_runs(limit=10_000)
    if window_sec is not None:
        cutoff = time.time() - float(window_sec)
        runs = [r for r in runs if float(r.get("started_at") or 0) >= cutoff]
    per_model, counted = usage_cli._aggregate_runs(runs)

    rows: list[dict[str, Any]] = []
    totals = {"runs": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    for (provider, model), bucket in sorted(per_model.items()):
        cost = usage_cli._format_cost(model, bucket["input_tokens"], bucket["output_tokens"])
        rows.append(
            {
                "provider": provider,
                "model": model,
                "runs": int(bucket["runs"]),
                "input_tokens": int(bucket["input_tokens"]),
                "output_tokens": int(bucket["output_tokens"]),
                "total_tokens": int(bucket["total_tokens"]),
                "cost_usd": cost,
            }
        )
        for k in ("runs", "input_tokens", "output_tokens", "total_tokens"):
            totals[k] += int(bucket[k])

    return {
        "window": label,
        "window_sec": window_sec,
        "counted_runs": counted,
        "rows": rows,
        "totals": totals,
    }


@router.get("/catalog/status")
def catalog_status(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Return the same metric block ``/search status`` prints."""
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        return {
            "ready": False,
            "message": "Search catalog isn't initialised yet — run /sync once.",
        }
    snap = catalog.sync_status(cfg.active_db_profile or "default")
    snap["llm_ready"] = bool(cfg.llm.provider and cfg.llm.model)
    snap["ready"] = bool(int(snap.get("entities", {}).get("total_entities", 0) or 0) > 0)
    return snap


class EnableHistoryStoreRequest(BaseModel):
    """Body for ``POST /api/admin/history-store/enable`` — minimal
    enable flow: pick the DB profile to dual-write to, name the
    target schema, flip the config flag. Bootstrap of the schema
    tables happens lazily on the next /run when the dual-write
    store first opens its connection."""

    profile: str = Field(..., min_length=1)
    schema_: str = Field(default="AMX", alias="schema")
    database: str = Field(default="")

    model_config = ConfigDict(populate_by_name=True)


@router.post("/admin/history-store/enable")
def enable_history_store(
    body: EnableHistoryStoreRequest,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Toggle dual-write history-store mode on."""
    cfg.history_store_enabled = True
    cfg.history_store_profile = body.profile
    cfg.history_store_schema = body.schema_ or "AMX"
    cfg.history_store_database = body.database or ""
    cfg.save()
    return {
        "enabled": True,
        "profile": cfg.history_store_profile,
        "schema": cfg.history_store_schema,
        "database": cfg.history_store_database,
    }


@router.post("/admin/history-store/disable")
def disable_history_store(
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Toggle dual-write history-store mode off; runs revert to
    local-only writes immediately."""
    cfg.history_store_enabled = False
    cfg.save()
    return {"enabled": False}


@router.get("/admin/history-store-status")
def history_store_status(cfg: AMXConfig = Depends(get_cfg)) -> dict[str, Any]:
    """Surface the team-shared history store state — enabled flag,
    target profile + schema, outbox depth (failed dual-writes still
    queued for retry). Same shape ``/history-store status`` prints."""
    enabled = bool(getattr(cfg, "history_store_enabled", False))
    profile = str(getattr(cfg, "history_store_profile", "") or "")
    schema = str(getattr(cfg, "history_store_schema", "") or "")
    outbox = 0
    try:
        from amx.storage.sqlite_store import history_store

        hs = history_store()
        if hs is not None and hasattr(hs, "shared") and hs.shared is not None:
            outbox = int(hs.shared.pending_count() or 0)
    except Exception:
        outbox = 0
    return {
        "enabled": enabled,
        "profile": profile,
        "schema": schema,
        "outbox_pending": outbox,
    }


__all__ = ["router"]
