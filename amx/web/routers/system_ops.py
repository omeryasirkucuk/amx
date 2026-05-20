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
def usage(
    window: str = Query(default="7d"),
    live: bool = Query(default=False),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Aggregate token consumption + USD cost per (provider, model).

    ``live=true`` recomputes cost against today's prices instead of the
    frozen-at-runtime values stored in ``analysis_runs.tokens_json``,
    matching the CLI ``/usage --live`` flag.
    """
    from amx.cli_support.commands import usage as usage_cli
    from amx.llm.pricing import compute_cost, lookup_price
    from amx.storage.sqlite_store import history_store

    label, window_sec = usage_cli._normalize_window(window)
    hs = history_store()
    if hs is None:
        return {
            "window": label,
            "window_sec": window_sec,
            "rows": [],
            "totals": {
                "runs": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "cost_usd": 0.0,
            },
            "live": bool(live),
            "message": "History store isn't initialised yet.",
        }
    # ``command_filter=None`` so re-run + apply rows contribute their
    # tokens too. The default filter keeps only ``analyze.run`` rows,
    # which surfaced as a phantom-empty Overview when the user's
    # history was dominated by re-runs (the dominant pattern after
    # PR #244 made re-runs free per asset). The same filter mistake
    # had to be fixed earlier in ``_build_enrichment_map``; mirror
    # the fix here so the Overview cards + System usage table
    # actually count work the user has done.
    runs = hs.list_recent_runs(limit=10_000, command_filter=None)
    if window_sec is not None:
        cutoff = time.time() - float(window_sec)
        runs = [r for r in runs if float(r.get("started_at") or 0) >= cutoff]
    per_model, counted = usage_cli._aggregate_runs(runs)

    rows: list[dict[str, Any]] = []
    totals = {
        "runs": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cost_usd": 0.0,
    }
    for (provider, model), bucket in sorted(per_model.items()):
        if live:
            price = lookup_price(cfg, provider=provider, model=model)
            _i, _o, total_cost = compute_cost(
                prompt_tokens=int(bucket["input_tokens"]),
                completion_tokens=int(bucket["output_tokens"]),
                price=price,
            )
            cost_known = price.source != "unknown"
            sources_label = price.source
        else:
            total_cost = float(bucket["frozen_cost_usd"])
            cost_known = bool(bucket["frozen_cost_known"])
            sources_label = ", ".join(sorted(bucket["sources"])) if bucket["sources"] else ""
        rows.append(
            {
                "provider": provider,
                "model": model,
                "runs": int(bucket["runs"]),
                "input_tokens": int(bucket["input_tokens"]),
                "output_tokens": int(bucket["output_tokens"]),
                "total_tokens": int(bucket["total_tokens"]),
                "cost_usd": float(total_cost) if cost_known else None,
                "source": sources_label,
            }
        )
        for k in ("runs", "input_tokens", "output_tokens", "total_tokens"):
            totals[k] += int(bucket[k])
        if cost_known:
            totals["cost_usd"] += float(total_cost)

    return {
        "window": label,
        "window_sec": window_sec,
        "counted_runs": counted,
        "rows": rows,
        "totals": totals,
        "live": bool(live),
    }


@router.get("/catalog/status")
def catalog_status(
    profile: str | None = Query(default=None),
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Return the same metric block ``/search status`` prints.

    Pass ``?profile=NAME`` to inspect a specific profile's index without
    flipping the active scope. Without it, falls back to the active
    profile (legacy behaviour).
    """
    from amx.search.catalog import SearchCatalog

    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        return {
            "ready": False,
            "message": "Search catalog isn't initialised yet — run /sync once.",
        }
    target = (profile or cfg.active_db_profile or "default").strip() or "default"
    snap = catalog.sync_status(target)
    snap["llm_ready"] = bool(cfg.llm.provider and cfg.llm.model)
    snap["ready"] = bool(int(snap.get("entities", {}).get("total_entities", 0) or 0) > 0)
    snap["profile"] = target
    return snap


class EnableHistoryStoreRequest(BaseModel):
    """Body for ``POST /api/admin/history-store/enable`` — minimal
    enable flow: pick the DB profile to dual-write to, name the
    target schema, optionally pin the parent catalog/database, flip
    the config flag. Bootstrap of the schema tables happens
    immediately so the admin API surfaces the workspace right away."""

    profile: str = Field(..., min_length=1)
    schema_: str = Field(default="AMX", alias="schema")
    database: str = Field(default="")
    create_missing: bool = Field(
        default=False,
        description=(
            "When true, attempt to CREATE the parent catalog/database "
            "if it does not exist (Databricks Unity Catalog, Snowflake, "
            "MSSQL). Ignored on schema-only backends (PostgreSQL, MySQL)."
        ),
    )

    model_config = ConfigDict(populate_by_name=True)


@router.post("/admin/history-store/enable")
def enable_history_store(
    body: EnableHistoryStoreRequest,
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Toggle dual-write history-store mode on.

    Persists the config change AND reinitialises the in-process
    ``history_store()`` global so subsequent writes / admin API calls
    pick up the shared backend immediately — without this rebuild the
    Studio process would keep using the cached local-only store and
    show "Active" in the UI while the actual writes stayed local.
    """
    cfg.history_store_enabled = True
    cfg.history_store_profile = body.profile
    cfg.history_store_schema = body.schema_ or "AMX"
    cfg.history_store_database = body.database or ""
    cfg.save()

    # Mirror what the CLI's ``/history-store enable`` does:
    # 1. Pre-create the shared schema explicitly (CREATE SCHEMA IF NOT EXISTS)
    #    so ``MetaData.create_all`` does not fail later with "schema does
    #    not exist". This is the step the Studio enable endpoint was
    #    previously skipping — the dual-write store would silently fall
    #    back to local-only on every startup because the schema was never
    #    created.
    # 2. Reinitialise the in-process ``history_store()`` global so the new
    #    shared backend is reachable from ``/api/admin/*`` and the
    #    dual-write code path immediately, without a Studio restart.
    schema_warning: str | None = None
    try:
        from amx.db.adapters import get_adapter
        from amx.storage.factory import (
            apply_history_db_override,
            init_history_store,
        )

        db_cfg = cfg.db_profiles[body.profile]
        target_cfg = apply_history_db_override(db_cfg, body.database) if body.database else db_cfg
        adapter = get_adapter(target_cfg)
        engine = adapter.create_engine()
        try:
            # When the user asked us to create the parent catalog /
            # database (Databricks UC catalog, Snowflake DB, MSSQL DB),
            # do it BEFORE the schema CREATE — without the parent the
            # CREATE SCHEMA either lands in the workspace default or
            # fails outright. No-op on Postgres / MySQL / Oracle.
            if body.create_missing and body.database:
                adapter.create_history_database(engine, body.database)
            adapter.create_history_schema(engine, cfg.history_store_schema)
        finally:
            engine.dispose()

        init_history_store(cfg)
    except Exception as exc:
        # Surface the issue to the caller so the UI can show a clear
        # error instead of silently returning "enabled" while the
        # bootstrap actually fails.
        schema_warning = f"{type(exc).__name__}: {exc}"

    return {
        "enabled": True,
        "profile": cfg.history_store_profile,
        "schema": cfg.history_store_schema,
        "database": cfg.history_store_database,
        "schema_bootstrap_warning": schema_warning,
    }


@router.post("/admin/history-store/disable")
def disable_history_store(
    cfg: AMXConfig = Depends(get_cfg),
) -> dict[str, Any]:
    """Toggle dual-write history-store mode off; runs revert to
    local-only writes immediately."""
    cfg.history_store_enabled = False
    cfg.save()

    # Rebuild the in-process history store back to local-only.
    try:
        from amx.storage.factory import init_history_store

        init_history_store(cfg)
    except Exception:
        pass

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
