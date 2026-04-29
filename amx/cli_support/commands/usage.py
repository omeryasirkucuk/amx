"""``/usage`` slash command — show token usage and approximate cost.

Reads from the SQLite ``analysis_runs`` table (already populated by every
``/analyze run``) and aggregates by (provider, model) over a time window.
Apply approximate USD pricing per million tokens so users have an
order-of-magnitude sense of LLM spend without needing to log into the
provider dashboard.

The pricing table is intentionally minimal and approximate — exact
amounts depend on tier and discount, so we render a "(approximate)"
disclaimer. Models we do not have pricing for show ``—`` for cost.

This command is local-only: nothing is sent to a network endpoint.
"""

from __future__ import annotations

import json
import time
from typing import Any

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import error, heading, info, render_table, warn


# USD per 1M tokens (input, output). Embedding models track only input.
# Source: provider public price pages, approximated for end-user
# orientation only. Update when a model is renamed or repriced.
_PRICING_PER_MTOK: dict[str, tuple[float, float]] = {
    # OpenAI chat
    "gpt-4o": (2.50, 10.00),
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4-turbo": (10.00, 30.00),
    "gpt-4": (30.00, 60.00),
    "o1": (15.00, 60.00),
    "o1-mini": (3.00, 12.00),
    "o3-mini": (1.10, 4.40),
    # OpenAI embeddings (output cost ignored by the model API)
    "text-embedding-3-small": (0.02, 0.0),
    "text-embedding-3-large": (0.13, 0.0),
    "text-embedding-ada-002": (0.10, 0.0),
    # Anthropic
    "claude-opus-4": (15.00, 75.00),
    "claude-sonnet-4": (3.00, 15.00),
    "claude-haiku-4": (0.80, 4.00),
    "claude-3-5-sonnet": (3.00, 15.00),
    "claude-3-5-haiku": (0.80, 4.00),
    "claude-3-opus": (15.00, 75.00),
    # Gemini
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-2.0-pro": (1.25, 5.00),
    "gemini-1.5-pro": (1.25, 5.00),
    "gemini-1.5-flash": (0.075, 0.30),
    # DeepSeek
    "deepseek-chat": (0.27, 1.10),
    "deepseek-reasoner": (0.55, 2.19),
}

_WINDOWS: dict[str, float | None] = {
    "today": 24 * 3600.0,
    "24h": 24 * 3600.0,
    "1d": 24 * 3600.0,
    "7d": 7 * 24 * 3600.0,
    "30d": 30 * 24 * 3600.0,
    "all": None,
}
_DEFAULT_WINDOW = "7d"


def _normalize_window(arg: str) -> tuple[str, float | None]:
    raw = (arg or _DEFAULT_WINDOW).lower().strip()
    if raw in _WINDOWS:
        return raw, _WINDOWS[raw]
    return _DEFAULT_WINDOW, _WINDOWS[_DEFAULT_WINDOW]


def _lookup_pricing(model: str) -> tuple[float, float] | None:
    """Match model id against the pricing table; tolerates provider-prefixed
    forms like ``openai/gpt-4o`` or ``anthropic/claude-sonnet-4-20250514`` by
    stripping the namespace and trimming trailing dated suffixes."""
    if not model:
        return None
    name = model.lower().strip()
    if name in _PRICING_PER_MTOK:
        return _PRICING_PER_MTOK[name]
    # Strip a provider prefix (openai/, anthropic/, openrouter/openai/, …).
    while "/" in name:
        name = name.split("/", 1)[1]
        if name in _PRICING_PER_MTOK:
            return _PRICING_PER_MTOK[name]
    # Strip dated/version suffixes (e.g. -20250514, -v2, -latest, -beta).
    for sep in ("-2024", "-2025", "-2026", "-v", "-latest", "-beta", "-preview"):
        if sep in name:
            head = name.split(sep, 1)[0]
            if head in _PRICING_PER_MTOK:
                return _PRICING_PER_MTOK[head]
    return None


def _aggregate_runs(
    runs: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, int]], int]:
    """Group runs by (provider, model) and sum token totals.

    Returns ``(per_model_aggregate, total_runs_with_tokens)``. Runs whose
    ``tokens_json`` cannot be parsed contribute zero — we never raise from
    a read-only display command.
    """
    per: dict[tuple[str, str], dict[str, int]] = {}
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
        for record in records:
            if not isinstance(record, dict):
                continue
            prompt_total += int(record.get("prompt_tokens") or 0)
            completion_total += int(record.get("completion_tokens") or 0)
        if prompt_total == 0 and completion_total == 0:
            continue
        bucket = per.setdefault(
            (provider, model),
            {"runs": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        )
        bucket["runs"] += 1
        bucket["input_tokens"] += prompt_total
        bucket["output_tokens"] += completion_total
        bucket["total_tokens"] += prompt_total + completion_total
        counted += 1
    return per, counted


def _format_cost(model: str, input_tokens: int, output_tokens: int) -> str:
    pricing = _lookup_pricing(model)
    if pricing is None:
        return "—"
    input_usd = input_tokens * pricing[0] / 1_000_000.0
    output_usd = output_tokens * pricing[1] / 1_000_000.0
    total = input_usd + output_usd
    if total < 0.01:
        return f"<$0.01"
    return f"${total:,.2f}"


def cmd_usage(cfg: AMXConfig, rest: list[str]) -> None:
    """Show LLM token usage and approximate cost.

    Usage::

        /usage              # last 7 days (default)
        /usage 24h          # last 24 hours
        /usage 7d           # last 7 days
        /usage 30d          # last 30 days
        /usage all          # since the SQLite history was created

    Reads ~/.amx/history.db. Local-only; no network calls.
    """
    label, window_sec = _normalize_window(rest[0] if rest else "")
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

    heading(f"LLM usage — last {label}")
    info(
        f"  {len(runs)} runs scanned, {runs_with_tokens} with token data. "
        f"Source: ~/.amx/history.db (local only)."
    )

    if not per_model:
        warn("None of the scanned runs recorded token usage.")
        return

    # Sort heaviest-first so the user sees the dominant cost line at top.
    sorted_keys = sorted(
        per_model.keys(),
        key=lambda key: per_model[key]["total_tokens"],
        reverse=True,
    )
    table_rows: list[list[object]] = []
    grand_in = grand_out = grand_total = 0
    grand_cost_known = 0.0
    grand_cost_seen = False
    for provider, model in sorted_keys:
        bucket = per_model[(provider, model)]
        in_tokens = bucket["input_tokens"]
        out_tokens = bucket["output_tokens"]
        total = bucket["total_tokens"]
        cost = _format_cost(model, in_tokens, out_tokens)
        if cost not in {"—"}:
            grand_cost_seen = True
            pricing = _lookup_pricing(model)
            if pricing:
                grand_cost_known += in_tokens * pricing[0] / 1_000_000.0
                grand_cost_known += out_tokens * pricing[1] / 1_000_000.0
        table_rows.append(
            [
                provider,
                model,
                str(bucket["runs"]),
                f"{in_tokens:,}",
                f"{out_tokens:,}",
                f"{total:,}",
                cost,
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
            (f"[bold]≈ ${grand_cost_known:,.2f}[/bold]"
             if grand_cost_seen else "—"),
        ]
    )
    render_table(
        f"Usage ({label})",
        ["Provider", "Model", "Runs", "Input", "Output", "Total", "Cost (approx)"],
        table_rows,
    )
    info(
        "Cost is approximate — based on a built-in price table and the actual "
        "token counts you used. For exact spend, check your provider dashboard."
    )
