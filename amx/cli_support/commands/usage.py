"""``/usage`` slash command — show token usage and live USD cost.

Reads from the SQLite ``analysis_runs`` table (already populated by every
``/analyze run``) and aggregates by (provider, model) over a time
window. Cost can be rendered two ways:

* **Frozen** (default) — uses the per-record USD figures the
  :class:`TokenTracker` saved at run time. This is the audit trail:
  what a run actually cost given the prices on the day it ran.
* **Live** (``/usage --live``) — re-runs :func:`amx.llm.pricing.compute_cost`
  against today's prices on the same recorded token totals so users
  can answer "what would this same workload cost today?".

Source attribution: the table includes a ``Source`` column showing
where the price came from (litellm / openrouter / user_override /
fallback / unknown) so the user can tell when a custom override was
in effect or when AMX had to fall back to the bundled snapshot.
"""

from __future__ import annotations

import json
import time
from typing import Any

from amx.config import AMXConfig
from amx.llm.pricing import (
    ModelPrice,
    cache_age_seconds,
    compute_cost,
    lookup_price,
)
from amx.storage.sqlite_store import history_store
from amx.utils.console import error, heading, info, render_table, warn

_WINDOWS: dict[str, float | None] = {
    "today": 24 * 3600.0,
    "24h": 24 * 3600.0,
    "1d": 24 * 3600.0,
    "7d": 7 * 24 * 3600.0,
    "30d": 30 * 24 * 3600.0,
    "all": None,
}
_DEFAULT_WINDOW = "7d"
_LIVE_FLAGS = {"--live", "-l", "live"}


def _normalize_window(arg: str) -> tuple[str, float | None]:
    raw = (arg or _DEFAULT_WINDOW).lower().strip()
    if raw in _WINDOWS:
        return raw, _WINDOWS[raw]
    return _DEFAULT_WINDOW, _WINDOWS[_DEFAULT_WINDOW]


def _aggregate_runs(
    runs: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], int]:
    """Group runs by (provider, model) and accumulate token + frozen-cost totals.

    The frozen-cost path reads each run's ``tokens_json.records[*].input_cost_usd``
    + ``output_cost_usd`` (set by :class:`TokenTracker` on newer runs).
    Older runs without the cost fields contribute zero to the frozen
    total — those runs render with ``$ — (frozen)`` until the user re-
    aggregates with ``--live``.
    """
    per: dict[tuple[str, str], dict[str, Any]] = {}
    counted = 0
    for run in runs:
        provider = str(run.get("llm_provider") or "(unknown)")
        model = str(run.get("llm_model") or "(unknown)")
        raw = run.get("tokens_json")
        if not raw:
            continue
        try:
            payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
        except Exception:
            continue
        records = payload.get("records") or []
        if not records:
            continue
        prompt_total = 0
        completion_total = 0
        frozen_cost_total = 0.0
        seen_cost_field = False
        sources_seen: set[str] = set()
        for record in records:
            if not isinstance(record, dict):
                continue
            prompt_total += int(record.get("prompt_tokens") or 0)
            completion_total += int(record.get("completion_tokens") or 0)
            if "input_cost_usd" in record or "output_cost_usd" in record:
                seen_cost_field = True
                frozen_cost_total += float(record.get("input_cost_usd") or 0.0)
                frozen_cost_total += float(record.get("output_cost_usd") or 0.0)
            src = record.get("price_source")
            if src:
                sources_seen.add(str(src))
        if prompt_total == 0 and completion_total == 0:
            continue
        bucket = per.setdefault(
            (provider, model),
            {
                "runs": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "frozen_cost_usd": 0.0,
                "frozen_cost_known": False,
                "sources": set(),
            },
        )
        bucket["runs"] += 1
        bucket["input_tokens"] += prompt_total
        bucket["output_tokens"] += completion_total
        bucket["total_tokens"] += prompt_total + completion_total
        if seen_cost_field:
            bucket["frozen_cost_known"] = True
            bucket["frozen_cost_usd"] += frozen_cost_total
        bucket["sources"] |= sources_seen
        counted += 1
    return per, counted


def _format_cost(cost: float, *, known: bool) -> str:
    if not known:
        return "—"
    if cost <= 0.0:
        return "$0.00"
    if cost < 0.0001:
        return "<$0.0001"
    return f"${cost:,.4f}"


def _format_sources(sources: set[str]) -> str:
    """Render the price-source set as a compact, sorted label.

    Multiple sources only happen when an aggregated bucket was filled
    by runs that used different price tables (e.g. the user toggled
    a custom override mid-week). Showing them comma-separated keeps
    the audit trail honest without bloating the column.
    """
    if not sources:
        return "—"
    cleaned = sorted(s for s in sources if s)
    return ", ".join(cleaned) if cleaned else "—"


def _live_recompute(cfg: AMXConfig, bucket: dict[str, Any], model: str) -> tuple[float, str]:
    """Recompute USD cost for one bucket using today's prices.

    Resolution falls back to ``ModelPrice(0, 0, "unknown")`` when the
    price is not known — the same contract :func:`lookup_price` always
    returns. Caller renders the bucket as "$0.0000 (unknown)" so the
    user sees the gap rather than a misleading zero.
    """
    price: ModelPrice = lookup_price(cfg, provider="", model=model)
    _in, _out, total = compute_cost(
        prompt_tokens=int(bucket["input_tokens"]),
        completion_tokens=int(bucket["output_tokens"]),
        price=price,
    )
    return total, price.source


def _format_cache_age(seconds: float | None) -> str:
    if seconds is None:
        return "never (run /refresh-prices)"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    if seconds < 86_400:
        return f"{seconds / 3600.0:.1f}h ago"
    return f"{seconds / 86_400.0:.1f}d ago"


def cmd_usage(cfg: AMXConfig, rest: list[str]) -> None:
    """Show LLM token usage and USD cost.

    Usage::

        /usage              # last 7 days, frozen costs (default)
        /usage 24h          # last 24 hours
        /usage 30d          # last 30 days
        /usage all          # since the SQLite history was created
        /usage --live       # recompute with today's prices
        /usage 30d --live   # combined

    Reads ~/.amx/history.db. Frozen costs come from the per-record USD
    figures saved at run time; ``--live`` recomputes using current
    prices fetched from LiteLLM / OpenRouter (cached locally — see
    /refresh-prices).
    """
    args = list(rest or [])
    live = False
    cleaned: list[str] = []
    for arg in args:
        if (arg or "").lower() in _LIVE_FLAGS:
            live = True
        else:
            cleaned.append(arg)
    label, window_sec = _normalize_window(cleaned[0] if cleaned else "")
    hs = history_store()
    if hs is None:
        warn(
            "No SQLite history store initialised yet — start an interactive "
            "session and run /analyze run at least once."
        )
        return

    cutoff = None if window_sec is None else (time.time() - window_sec)
    try:
        with hs._connect() as conn:  # noqa: SLF001 — single read of analysis_runs
            if cutoff is None:
                rows = conn.execute(
                    """
                    SELECT id, started_at, llm_provider, llm_model, tokens_json
                    FROM analysis_runs
                    ORDER BY started_at ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, started_at, llm_provider, llm_model, tokens_json
                    FROM analysis_runs
                    WHERE started_at >= ?
                    ORDER BY started_at ASC
                    """,
                    (cutoff,),
                ).fetchall()
        runs = [dict(r) for r in rows]
    except Exception as exc:  # noqa: BLE001 — display command must not crash
        error(f"Could not read analysis_runs: {exc}")
        return

    if not runs:
        info(f"No analyze runs in window {label!r} (~/.amx/history.db).")
        return

    per_model, runs_with_tokens = _aggregate_runs(runs)

    mode_label = "live" if live else "frozen"
    heading(f"LLM usage — last {label} ({mode_label})")
    info(
        f"  {len(runs)} runs scanned, {runs_with_tokens} with token data. "
        f"Source: ~/.amx/history.db (local only)."
    )

    if not per_model:
        warn("None of the scanned runs recorded token usage.")
        return

    sorted_keys = sorted(
        per_model.keys(),
        key=lambda key: per_model[key]["total_tokens"],
        reverse=True,
    )
    table_rows: list[list[object]] = []
    grand_in = grand_out = grand_total = 0
    grand_cost = 0.0
    grand_cost_known = False
    for provider, model in sorted_keys:
        bucket = per_model[(provider, model)]
        in_tokens = bucket["input_tokens"]
        out_tokens = bucket["output_tokens"]
        total = bucket["total_tokens"]
        if live:
            cost_value, live_source = _live_recompute(cfg, bucket, model)
            cost_known = live_source != "unknown"
            sources_label = live_source
        else:
            cost_value = float(bucket["frozen_cost_usd"])
            cost_known = bool(bucket["frozen_cost_known"])
            sources_label = _format_sources(bucket["sources"])
        if cost_known:
            grand_cost_known = True
            grand_cost += cost_value
        table_rows.append(
            [
                provider,
                model,
                str(bucket["runs"]),
                f"{in_tokens:,}",
                f"{out_tokens:,}",
                f"{total:,}",
                _format_cost(cost_value, known=cost_known),
                sources_label,
            ]
        )
        grand_in += in_tokens
        grand_out += out_tokens
        grand_total += total

    table_rows.append(
        [
            "[bold]TOTAL[/bold]",
            "",
            "",
            f"[bold]{grand_in:,}[/bold]",
            f"[bold]{grand_out:,}[/bold]",
            f"[bold]{grand_total:,}[/bold]",
            (
                f"[bold]{_format_cost(grand_cost, known=grand_cost_known)}[/bold]"
                if grand_cost_known
                else "—"
            ),
            "",
        ]
    )
    render_table(
        f"Usage ({label}, {mode_label})",
        ["Provider", "Model", "Runs", "Input", "Output", "Total", "Cost (USD)", "Source"],
        table_rows,
    )
    age = _format_cache_age(cache_age_seconds())
    if live:
        info(f"Live recompute against current prices. Cache last refreshed: {age}.")
        info("Run /refresh-prices to pull the latest LiteLLM + OpenRouter price tables.")
    else:
        info(f"Frozen costs as recorded at run time. Price cache last refreshed: {age}.")
        info("Pass --live to recompute against today's prices.")
