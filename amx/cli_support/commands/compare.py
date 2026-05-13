"""`/compare` — pivot AMX runs side-by-side for the same assets.

Comparison surface for users who run AMX multiple times against the
same scope under different LLM / doc / code profiles. Produces three
Rich tables: run summary, per-column descriptions pivot, aggregate
metrics. Lives in the ``/search`` namespace so it sits next to ``/ask``
where users naturally end up after running questions.
"""

from __future__ import annotations

import base64
import contextlib
import csv
import difflib
import json
import os
import sys
from collections.abc import Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from rich import box
from rich.table import Table
from rich.text import Text

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask_choice,
    console,
    error,
    info,
    success,
    warn,
)
from amx.utils.terminal_theme import info_color

LogEvent = Callable[..., None]

# Dimensions a user might want to vary across runs. Auto-detection
# walks them in order and picks the first one with >1 distinct value.
_BY_DIMENSIONS: tuple[str, ...] = (
    "llm_profile",
    "doc_profile",
    "code_profile",
    "llm_model",
    "db_profile",
    "run",
)

_BY_TO_RUN_KEY: dict[str, str] = {
    "model": "llm_model",
    "llm_model": "llm_model",
    "llm_profile": "llm_profile",
    "doc_profile": "doc_profile",
    "code_profile": "code_profile",
    "db_profile": "db_profile",
    "run": "id",
}


def _fmt_dt(ts: float | None) -> str:
    if not ts:
        return "—"
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def _fmt_duration(sec: float | None) -> str:
    if sec is None or sec <= 0:
        return "—"
    s = float(sec)
    if s < 60:
        return f"{s:.1f}s"
    m, rem = divmod(s, 60)
    return f"{int(m)}m {rem:0.0f}s"


def _fmt_or_dash(val: Any) -> str:
    if val is None:
        return "—"
    s = str(val).strip()
    return s if s else "—"


def _confidence_style(band: str) -> str:
    b = (band or "").lower()
    if b == "high":
        return "bold green"
    if b == "medium" or b == "med":
        return "yellow"
    if b == "low":
        return "red"
    return "dim"


def _detect_by(runs: list[dict[str, Any]]) -> str:
    """Pick the first dimension that varies across the resolved runs.

    Falls back to ``"run"`` (so each run column gets a header but no
    cell-level highlighting) when every dimension is uniform.
    """
    for dim in _BY_DIMENSIONS:
        if dim == "run":
            continue
        values = {(r.get(dim) or "") for r in runs}
        if len(values) > 1:
            return dim
    return "run"


def _resolve_runs(
    *,
    cfg: AMXConfig,
    run_ids: tuple[str, ...],
    schema: str,
    table: str,
    last_n: int,
    command_filter: str,
) -> list[dict[str, Any]]:
    """Resolve which runs to compare — positional IDs > scope+last_n > current scope."""
    hs = history_store()
    if hs is None:
        error("History store is not initialized.")
        return []

    if run_ids:
        out: list[dict[str, Any]] = []
        for raw in run_ids:
            try:
                rid = int(str(raw).lstrip("#"))
            except ValueError:
                warn(f"Skipping non-integer run id '{raw}'.")
                continue
            row = hs.get_run(rid)
            if row is None:
                warn(f"Run #{rid} not found — skipping.")
                continue
            out.append(row)
        # Newest-first to match the --last path.
        out.sort(key=lambda r: float(r.get("started_at") or 0.0), reverse=True)
        return out

    eff_schema = (schema or cfg.current_schema or "").strip()
    eff_table = (table or cfg.current_table or "").strip()
    cmd = command_filter if command_filter and command_filter != "all" else None

    if not eff_schema and not eff_table:
        error(
            "No scope to compare — pass run IDs or use --schema/--table. "
            "Example: /compare --schema sales --last 3."
        )
        return []

    return hs.find_runs_for_scope(
        schema=eff_schema or None,
        table=eff_table or None,
        command_filter=cmd,
        limit=max(1, int(last_n)),
    )


def _column_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("schema_name") or ""),
        str(row.get("table_name") or ""),
        str(row.get("column_name") or ""),
    )


def _truncate(text: str, max_len: int = 60) -> str:
    s = (text or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _top_alternative(row: dict[str, Any]) -> str:
    """Pick a representative description from a run_results row.

    Prefer ``chosen_description`` (post-review winner). Fall back to
    the first entry of ``alternatives_json``. Empty string if neither
    is present.

    Accepts both the legacy flat list[str] payload and the Phase 1
    structured list[dict] payload (where each entry has a ``text`` key).
    """
    chosen = (row.get("chosen_description") or "").strip()
    if chosen:
        return chosen
    alts = row.get("alternatives_json")
    if isinstance(alts, str) and alts:
        try:
            alts = json.loads(alts)
        except Exception:
            alts = []
    if isinstance(alts, list) and alts:
        first = alts[0]
        if isinstance(first, dict):
            return str(first.get("text") or "").strip()
        return str(first).strip() if first else ""
    return ""


def _fmt_int(n: Any) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "—"


def _fmt_float(n: Any, places: int = 2) -> str:
    try:
        return f"{float(n):.{places}f}"
    except Exception:
        return "—"


def _aggregate_for_run(run: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute per-run aggregates for Table 3."""
    tokens = run.get("tokens_json")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = {}
    if not isinstance(tokens, dict):
        tokens = {}
    metrics = run.get("metrics_json")
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except Exception:
            metrics = {}
    if not isinstance(metrics, dict):
        metrics = {}

    summary_rows = tokens.get("summary") if isinstance(tokens, dict) else None
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    if isinstance(summary_rows, list):
        for entry in summary_rows:
            if not isinstance(entry, (list, tuple)) or len(entry) < 4:
                continue
            try:
                prompt_tokens += int(entry[1] or 0)
                completion_tokens += int(entry[2] or 0)
                total_tokens += int(entry[3] or 0)
            except Exception:
                continue
    if total_tokens == 0:
        try:
            total_tokens = int(tokens.get("total_tokens") or 0)
        except Exception:
            total_tokens = 0

    # USD cost frozen at run time (PR #235). The Compare table is
    # the natural place to read it: A vs B at the same model is
    # almost always a "did the new prompt save tokens / dollars?"
    # question. Falls back to per-record summing when only
    # ``records[]`` is present (older runs); the top-level
    # ``total_cost_usd`` covers most rows produced after PR #235.
    cost_usd: float | None = None
    raw_total = tokens.get("total_cost_usd") if isinstance(tokens, dict) else None
    if isinstance(raw_total, (int, float)) and float(raw_total) >= 0:
        cost_usd = float(raw_total)
    if cost_usd is None or cost_usd == 0.0:
        record_total = 0.0
        seen_record_cost = False
        records_list = tokens.get("records") if isinstance(tokens, dict) else None
        if isinstance(records_list, list):
            for record in records_list:
                if not isinstance(record, dict):
                    continue
                if "input_cost_usd" in record or "output_cost_usd" in record:
                    seen_record_cost = True
                    record_total += float(record.get("input_cost_usd") or 0.0)
                    record_total += float(record.get("output_cost_usd") or 0.0)
        if seen_record_cost:
            cost_usd = record_total

    logprob_scores = [
        float(r["logprob_score"]) for r in results if r.get("logprob_score") is not None
    ]
    avg_logprob = sum(logprob_scores) / len(logprob_scores) if logprob_scores else None

    bands = {"high": 0, "medium": 0, "low": 0}
    for r in results:
        band = str(r.get("confidence") or "").lower()
        if band in bands:
            bands[band] += 1
    total_band = sum(bands.values()) or 1

    processed = int(run.get("processed_count") or 0)
    applied = int(run.get("applied_count") or 0)
    approval = (applied / processed) if processed > 0 else None

    return {
        "model_processing_sec": float(metrics.get("model_processing_sec") or 0.0),
        "duration_sec": float(run.get("duration_sec") or 0.0),
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "cost_usd": cost_usd,
        "avg_logprob": avg_logprob,
        "high_pct": bands["high"] / total_band * 100.0,
        "medium_pct": bands["medium"] / total_band * 100.0,
        "low_pct": bands["low"] / total_band * 100.0,
        "approval_rate": approval,
        "result_count": len(results),
    }


def _word_diff(baseline: str, current: str) -> Text:
    """Render ``current`` with word-level highlights against ``baseline``.

    Insertions (words present in ``current`` but not ``baseline``) appear
    in bold green. Deletions (words dropped from ``baseline``) appear in
    red strikethrough. Common words render plain. Falls back to plain
    text when the two strings tokenise identically.
    """
    base_words = (baseline or "").split()
    cur_words = (current or "").split()
    if base_words == cur_words:
        return Text(current or "")
    matcher = difflib.SequenceMatcher(None, base_words, cur_words)
    chunks: list[tuple[str, str]] = []
    for op, i1, i2, j1, j2 in matcher.get_opcodes():
        if op == "equal":
            chunks.append((" ".join(cur_words[j1:j2]), ""))
        elif op == "insert":
            chunks.append((" ".join(cur_words[j1:j2]), "bold green"))
        elif op == "replace":
            chunks.append((" ".join(base_words[i1:i2]), "strike red"))
            chunks.append((" ".join(cur_words[j1:j2]), "bold green"))
        elif op == "delete":
            chunks.append((" ".join(base_words[i1:i2]), "strike red"))
    out = Text()
    for idx, (text, style) in enumerate(chunks):
        if not text:
            continue
        if idx > 0 and out.plain:
            out.append(" ")
        out.append(text, style=style or "")
    return out


def _highlight_best(values: list[float | None], higher_is_better: bool) -> int | None:
    """Return index of the best non-None value, or None if all empty/equal."""
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    if len(indexed) < 2:
        return None
    if higher_is_better:
        idx, _ = max(indexed, key=lambda iv: iv[1])
    else:
        idx, _ = min(indexed, key=lambda iv: iv[1])
    # If everyone tied, don't bold anything.
    distinct = {v for _, v in indexed}
    if len(distinct) == 1:
        return None
    return idx


# ── Table renderers ─────────────────────────────────────────────────────────


def _settings_for_run(run: dict[str, Any]) -> dict[str, Any]:
    """Return the captured ``settings_json`` dict for a run, or {}.

    Older rows (pre-settings_json migration) return ``{}`` so the
    settings table renders dashes for them instead of crashing. The
    storage layer already deserialises the JSON column in
    ``find_runs_for_scope`` / ``get_run``, so we just unwrap.
    """
    raw = run.get("settings_json") if isinstance(run, dict) else None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _render_run_settings(runs: list[dict[str, Any]]) -> None:
    """Show the LLM/run knobs the user can vary across runs.

    Lives between :func:`_render_run_summary` (identity — who/what/when)
    and the per-column pivot (the actual descriptions). The user-reported
    motivation is "I should see exactly which settings I used" —
    prompt_detail, language, batch sizes, etc., not just the profile
    names. Pre-2026-05-02 these knobs weren't captured at all; older
    rows render as ``—``.
    """
    table = Table(
        title="Run settings",
        show_lines=True,
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Run", style=info_color(), no_wrap=True)
    table.add_column("Prompt detail", no_wrap=True)
    table.add_column("Language", no_wrap=True)
    table.add_column("Verbosity", no_wrap=True)
    table.add_column("N alts", justify="right", no_wrap=True)
    table.add_column("Batch size", justify="right", no_wrap=True)
    table.add_column("Ctx cols", justify="right", no_wrap=True)
    table.add_column("Completion", no_wrap=True)
    table.add_column("Temp", justify="right", no_wrap=True)
    table.add_column("Dedup", no_wrap=True)
    table.add_column("Missing only", no_wrap=True)
    table.add_column("Review strat.", no_wrap=True)

    def _opt(s: dict[str, Any], key: str, fmt: str = "{}") -> str:
        v = s.get(key)
        if v is None or v == "":
            return "—"
        try:
            return fmt.format(v)
        except (KeyError, IndexError, ValueError):
            return str(v)

    def _bool_opt(s: dict[str, Any], key: str) -> str:
        v = s.get(key)
        if v is None:
            return "—"
        return "yes" if bool(v) else "no"

    for r in runs:
        s = _settings_for_run(r)
        cells = [
            Text(f"#{r.get('id')}", style=info_color()),
            Text(_opt(s, "prompt_detail")),
            Text(_opt(s, "language")),
            Text(_opt(s, "description_verbosity")),
            Text(_opt(s, "n_alternatives")),
            Text(_opt(s, "column_batch_size")),
            Text(_opt(s, "batch_context_column_names")),
            Text(_opt(s, "completion_mode")),
            Text(_opt(s, "temperature", "{:.2f}")),
            Text(_bool_opt(s, "dedup_used")),
            Text(_bool_opt(s, "missing_only")),
            Text(_opt(s, "review_strategy")),
        ]
        table.add_row(*cells)

    console.print(table)


def _render_run_summary(runs: list[dict[str, Any]], by: str) -> None:
    table = Table(
        title="Run summary",
        show_lines=True,
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Run", style=info_color(), no_wrap=True)
    table.add_column("Started", style=info_color(), no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Command", style=info_color(), no_wrap=True)
    table.add_column("DB profile", no_wrap=True)
    table.add_column("LLM profile", no_wrap=True)
    table.add_column("Model", no_wrap=True)
    table.add_column("Doc profile", no_wrap=True)
    table.add_column("Code profile", no_wrap=True)
    table.add_column("Duration", justify="right", no_wrap=True)
    table.add_column("Approved", justify="right", no_wrap=True)

    by_run_key = _BY_TO_RUN_KEY.get(by, "")

    def _cell(run: dict[str, Any], field: str, value: str, run_key: str) -> Text:
        if run.get("status") == "failed":
            return Text(value, style="dim red")
        if by_run_key and run_key == by_run_key:
            return Text(value, style="bold green")
        return Text(value)

    for r in runs:
        approved = (
            f"{int(r.get('applied_count') or 0)}/{int(r.get('processed_count') or 0)}"
            if (r.get("processed_count") or 0)
            else "—"
        )
        cells = [
            Text(f"#{r.get('id')}", style=info_color()),
            Text(_fmt_dt(r.get("started_at"))),
            Text(
                str(r.get("status") or "—"),
                style=("bold red" if r.get("status") == "failed" else "green"),
            ),
            Text(str(r.get("command") or "—")),
            _cell(r, "db_profile", _fmt_or_dash(r.get("db_profile")), "db_profile"),
            _cell(r, "llm_profile", _fmt_or_dash(r.get("llm_profile")), "llm_profile"),
            _cell(r, "llm_model", _fmt_or_dash(r.get("llm_model")), "llm_model"),
            _cell(r, "doc_profile", _fmt_or_dash(r.get("doc_profile")), "doc_profile"),
            _cell(r, "code_profile", _fmt_or_dash(r.get("code_profile")), "code_profile"),
            Text(_fmt_duration(r.get("duration_sec"))),
            Text(approved),
        ]
        table.add_row(*cells)

    console.print(table)


def _build_asset_map(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    column_filter: str,
) -> dict[tuple[str, str, str], dict[int, dict[str, Any]]]:
    """Pivot run_results into ``{(schema, table, column): {run_id: row}}``."""
    asset_map: dict[tuple[str, str, str], dict[int, dict[str, Any]]] = {}
    for run in runs:
        rid = int(run["id"])
        for row in results_by_run.get(rid, []):
            if column_filter:
                col = str(row.get("column_name") or "")
                if col.lower() != column_filter.lower():
                    continue
            key = _column_key(row)
            asset_map.setdefault(key, {})[rid] = row
    return asset_map


def _render_per_column_pivot(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    column_filter: str,
    *,
    diff: bool = False,
) -> None:
    """Pivot run_results so rows are columns/tables, cells are runs.

    When ``diff`` is set, every cell after the leftmost is rendered as
    a word-level diff against the leftmost run's text — insertions in
    green, deletions struck through in red.
    """
    asset_map = _build_asset_map(runs, results_by_run, column_filter)

    if not asset_map:
        warn(
            "No saved per-column results overlap across the selected runs. "
            "Compared runs may have produced no LLM alternatives, or were "
            "narrower than --column requires."
        )
        return

    title = "Per-column results (top alternative · band · logprob · tokens)"
    if diff:
        title += "  [diff mode: leftmost run = baseline]"
    table = Table(
        title=title,
        show_lines=True,
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Schema.Table.Column", style=info_color(), no_wrap=False, overflow="fold")
    for r in runs:
        table.add_column(
            f"Run #{r.get('id')}",
            overflow="fold",
            max_width=46,
        )

    # Sort assets schema, table, column for stable display.
    for asset_key in sorted(asset_map.keys()):
        schema_n, table_n, col_n = asset_key
        label = ".".join(p for p in (schema_n, table_n, col_n) if p) or "(unknown)"
        runs_for_asset = asset_map[asset_key]

        # Decide which run wins this row by logprob_score.
        scores: list[float | None] = []
        for run in runs:
            row = runs_for_asset.get(int(run["id"]))
            scores.append(
                float(row["logprob_score"])
                if row and row.get("logprob_score") is not None
                else None
            )
        winner_idx = _highlight_best(scores, higher_is_better=True)

        # Capture the baseline (leftmost) description for diff mode.
        baseline_text = ""
        if diff and runs:
            base_row = runs_for_asset.get(int(runs[0]["id"]))
            baseline_text = _top_alternative(base_row) if base_row else ""

        cells: list[Text] = [Text(label, style=info_color())]
        for col_idx, run in enumerate(runs):
            row = runs_for_asset.get(int(run["id"]))
            if not row:
                cells.append(Text("—", style="dim"))
                continue
            full_desc = _top_alternative(row)
            desc_truncated = _truncate(full_desc, max_len=58)
            band = str(row.get("confidence") or "").lower() or "—"
            logprob = (
                _fmt_float(row.get("logprob_score"), places=2)
                if row.get("logprob_score") is not None
                else "—"
            )
            tokens = _fmt_int(row.get("token_count"))
            cell = Text()
            if diff and col_idx > 0 and full_desc:
                # Diff truncated current vs truncated baseline so on-screen
                # lengths stay bounded — full text still goes to exports.
                cell.append_text(_word_diff(_truncate(baseline_text, 58), desc_truncated))
            else:
                cell.append(
                    desc_truncated or "(empty)",
                    style=("white" if desc_truncated else "dim"),
                )
            cell.append("\n")
            cell.append(band, style=_confidence_style(band))
            cell.append(" · ")
            base_style = "bold green" if col_idx == winner_idx else "white"
            cell.append(logprob, style=base_style)
            cell.append(f" · {tokens} tok", style="dim")
            cells.append(cell)
        table.add_row(*cells)

    console.print(table)


def _render_aggregate_metrics(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
) -> None:
    aggs = [_aggregate_for_run(r, results_by_run.get(int(r["id"]), [])) for r in runs]

    table = Table(
        title="Aggregate metrics",
        show_lines=True,
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Metric", style=info_color(), no_wrap=True)
    for r in runs:
        table.add_column(f"Run #{r.get('id')}", justify="right")

    def _row(label: str, values: list[Any], best_idx: int | None) -> None:
        cells: list[Text] = [Text(label, style=info_color())]
        for i, v in enumerate(values):
            text = Text(str(v))
            if i == best_idx:
                text.stylize("bold green")
            cells.append(text)
        table.add_row(*cells)

    # Wall duration (lower is better)
    wall_vals = [a["duration_sec"] or None for a in aggs]
    _row(
        "Wall duration",
        [_fmt_duration(a["duration_sec"]) for a in aggs],
        _highlight_best(wall_vals, higher_is_better=False),
    )
    # Model processing time (lower is better)
    model_vals = [a["model_processing_sec"] or None for a in aggs]
    _row(
        "Model processing",
        [_fmt_duration(a["model_processing_sec"]) for a in aggs],
        _highlight_best(model_vals, higher_is_better=False),
    )
    # Tokens (lower is better — cheaper)
    prompt_vals = [a["prompt_tokens"] or None for a in aggs]
    _row(
        "Prompt tokens",
        [_fmt_int(a["prompt_tokens"]) for a in aggs],
        _highlight_best(prompt_vals, higher_is_better=False),
    )
    completion_vals = [a["completion_tokens"] or None for a in aggs]
    _row(
        "Completion tokens",
        [_fmt_int(a["completion_tokens"]) for a in aggs],
        _highlight_best(completion_vals, higher_is_better=False),
    )
    total_vals = [a["total_tokens"] or None for a in aggs]
    _row(
        "Total tokens",
        [_fmt_int(a["total_tokens"]) for a in aggs],
        _highlight_best(total_vals, higher_is_better=False),
    )
    # Cost USD (lower is better; ``None`` for runs that predate
    # PR #235's frozen-cost capture, rendered as ``--`` so the
    # user doesn't read $0.00 as "free").
    cost_vals = [a["cost_usd"] for a in aggs]
    _row(
        "Cost (USD)",
        [f"${a['cost_usd']:.4f}" if a["cost_usd"] is not None else "—" for a in aggs],
        _highlight_best(
            [v if v is not None and v > 0 else None for v in cost_vals],
            higher_is_better=False,
        ),
    )
    # Avg logprob (higher is better)
    logprob_vals = [a["avg_logprob"] for a in aggs]
    _row(
        "Avg logprob_score",
        [
            _fmt_float(a["avg_logprob"], places=3) if a["avg_logprob"] is not None else "—"
            for a in aggs
        ],
        _highlight_best(logprob_vals, higher_is_better=True),
    )
    # Confidence distribution (per-band, higher pct=high is better)
    high_vals = [a["high_pct"] for a in aggs]
    _row(
        "% high confidence",
        [f"{a['high_pct']:.0f}%" for a in aggs],
        _highlight_best(high_vals, higher_is_better=True),
    )
    _row(
        "% medium confidence",
        [f"{a['medium_pct']:.0f}%" for a in aggs],
        None,
    )
    low_vals = [a["low_pct"] for a in aggs]
    _row(
        "% low confidence",
        [f"{a['low_pct']:.0f}%" for a in aggs],
        _highlight_best(low_vals, higher_is_better=False),
    )
    # Approval rate (higher is better)
    approval_vals = [a["approval_rate"] for a in aggs]
    _row(
        "Approval rate",
        [
            f"{a['approval_rate'] * 100:.0f}%" if a["approval_rate"] is not None else "—"
            for a in aggs
        ],
        _highlight_best(approval_vals, higher_is_better=True),
    )
    # Result count (informational only — no winner)
    _row("Saved results", [_fmt_int(a["result_count"]) for a in aggs], None)

    console.print(table)


# ── Export helpers ──────────────────────────────────────────────────────────


_RUN_SUMMARY_COLUMNS: tuple[str, ...] = (
    "run_id",
    "started_at",
    "status",
    "command",
    "db_profile",
    "llm_profile",
    "llm_model",
    "doc_profile",
    "code_profile",
    "duration_sec",
    "processed_count",
    "applied_count",
)

_PER_COLUMN_LONG_COLUMNS: tuple[str, ...] = (
    "schema",
    "table",
    "column",
    "run_id",
    "description",
    "confidence",
    "logprob_score",
    "token_count",
)

_AGGREGATE_COLUMNS: tuple[str, ...] = (
    "metric",
    "run_id",
    "value",
)

_AGGREGATE_METRICS: tuple[tuple[str, str], ...] = (
    ("wall_duration_sec", "duration_sec"),
    ("model_processing_sec", "model_processing_sec"),
    ("prompt_tokens", "prompt_tokens"),
    ("completion_tokens", "completion_tokens"),
    ("total_tokens", "total_tokens"),
    # ``cost_usd`` is the frozen USD cost; ``None`` for runs that
    # predate PR #235's per-record cost capture so the SPA can
    # render "--" for those rows without misleading the user
    # with a $0.00 that means "we just don't know".
    ("cost_usd", "cost_usd"),
    ("avg_logprob_score", "avg_logprob"),
    ("pct_high_confidence", "high_pct"),
    ("pct_medium_confidence", "medium_pct"),
    ("pct_low_confidence", "low_pct"),
    ("approval_rate", "approval_rate"),
    ("saved_results", "result_count"),
)


def _collect_run_summary_rows(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One dict per run, rolled up for CSV / Markdown / JSON exports.

    Includes the captured ``settings_json`` snapshot so notebooks can
    pivot on prompt_detail, batch_size, dedup_used, etc. — the same
    knobs the on-screen ``Run settings`` table surfaces. Older rows
    that predate the settings_json migration round-trip with empty
    settings (``{}``) instead of ``None`` so notebooks don't have to
    null-check.
    """
    rows: list[dict[str, Any]] = []
    for r in runs:
        rows.append(
            {
                "run_id": r.get("id"),
                "started_at": _fmt_dt(r.get("started_at")),
                "status": r.get("status") or "",
                "command": r.get("command") or "",
                "db_profile": r.get("db_profile") or "",
                "llm_profile": r.get("llm_profile") or "",
                "llm_model": r.get("llm_model") or "",
                "doc_profile": r.get("doc_profile") or "",
                "code_profile": r.get("code_profile") or "",
                "duration_sec": float(r.get("duration_sec") or 0.0),
                "processed_count": int(r.get("processed_count") or 0),
                "applied_count": int(r.get("applied_count") or 0),
                "settings": _settings_for_run(r),
            }
        )
    return rows


def _collect_per_column_long(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    column_filter: str = "",
) -> list[dict[str, Any]]:
    asset_map = _build_asset_map(runs, results_by_run, column_filter)
    rows: list[dict[str, Any]] = []
    for asset_key in sorted(asset_map.keys()):
        schema_n, table_n, col_n = asset_key
        runs_for_asset = asset_map[asset_key]
        for run in runs:
            row = runs_for_asset.get(int(run["id"]))
            if not row:
                continue
            rows.append(
                {
                    "schema": schema_n,
                    "table": table_n,
                    "column": col_n,
                    "run_id": int(run["id"]),
                    "description": _top_alternative(row),
                    "confidence": str(row.get("confidence") or ""),
                    "logprob_score": row.get("logprob_score"),
                    "token_count": row.get("token_count"),
                }
            )
    return rows


def compare_runs(
    run_ids: list[int],
    *,
    quality_tier: int = 0,
    ground_truth_run_id: int | None = None,
    db_connector: Any = None,
    llm_provider: Any = None,
) -> dict[str, Any]:
    """Pure helper: assemble a JSON-serializable comparison payload
    for a list of run ids. Used by the CLI ``/history compare``
    command, the ``/api/history/compare`` endpoint, and the
    ``compare_runs`` LLM tool.

    Returns ``{"runs": [...], "summary_rows": [...], "per_column": [...],
    "aggregates": [...], "missing": [...], "quality_metrics": {...}? }``.

    ``quality_tier``:
      * ``0`` — Tier 0 only (offline, deterministic, free).
      * ``1`` — Tier 0 + Tier 1 (local sentence embeddings).
      * ``2`` — Tier 0 + Tier 1 + Tier 2 (LLM judge — opt-in,
        consumes tokens on the active LLM provider; needs
        ``llm_provider``).

    ``ground_truth_run_id`` lets the user pin a specific run as the
    reference baseline (CLI ``--ground-truth-run``, Studio "Set as
    ground truth" radio); the reference waterfall in ``quality.py``
    falls back to live DB COMMENT and then to most-recently-applied
    catalog comments before declaring "no reference".
    """
    hs = history_store()
    if hs is None:
        raise RuntimeError("History store is not initialized.")

    found: list[dict[str, Any]] = []
    missing: list[int] = []
    for rid in run_ids:
        try:
            rid_int = int(rid)
        except (TypeError, ValueError):
            continue
        row = hs.get_run(rid_int)
        if row is None:
            missing.append(rid_int)
        else:
            found.append(row)
    found.sort(key=lambda r: float(r.get("started_at") or 0.0), reverse=True)

    results_by_run: dict[int, list[dict[str, Any]]] = {}
    for row in found:
        try:
            results_by_run[int(row["id"])] = list(hs.get_run_results(int(row["id"])))
        except Exception:
            results_by_run[int(row["id"])] = []

    payload: dict[str, Any] = {
        "runs": found,
        "summary_rows": _collect_run_summary_rows(found),
        "per_column": _collect_per_column_long(found, results_by_run),
        "aggregates": _collect_aggregate_long(found, results_by_run),
        "missing": missing,
    }

    if quality_tier > 0 or ground_truth_run_id is not None:
        # Lazy import — only callers that opt in pay the import cost,
        # and unit tests that don't exercise quality skip the chain
        # entirely.
        from amx.cli_support.quality import compute_quality_metrics

        payload["quality_metrics"] = compute_quality_metrics(
            payload,
            tier=quality_tier,
            db_connector=db_connector,
            history_store=hs,
            ground_truth_run_id=ground_truth_run_id,
            llm_provider=llm_provider,
        )

    return payload


def _collect_aggregate_long(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run in runs:
        agg = _aggregate_for_run(run, results_by_run.get(int(run["id"]), []))
        for export_name, agg_key in _AGGREGATE_METRICS:
            rows.append(
                {
                    "metric": export_name,
                    "run_id": int(run["id"]),
                    "value": agg.get(agg_key),
                }
            )
    return rows


def _export_csv(
    path: Path,
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    *,
    column_filter: str = "",
) -> None:
    """Write all three tables into a single CSV file with section markers.

    A leading ``# section: <name>`` comment and a blank line precede each
    block so pandas/Excel users can split the file by section, while the
    rest is plain RFC-4180 CSV that readers ignore as comments or
    blank rows.
    """
    sections: tuple[tuple[str, tuple[str, ...], list[dict[str, Any]]], ...] = (
        ("run_summary", _RUN_SUMMARY_COLUMNS, _collect_run_summary_rows(runs)),
        (
            "per_column",
            _PER_COLUMN_LONG_COLUMNS,
            _collect_per_column_long(runs, results_by_run, column_filter),
        ),
        ("aggregate_metrics", _AGGREGATE_COLUMNS, _collect_aggregate_long(runs, results_by_run)),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        for idx, (name, headers, rows) in enumerate(sections):
            if idx > 0:
                fh.write("\n")
            fh.write(f"# section: {name}\n")
            writer = csv.DictWriter(fh, fieldnames=list(headers))
            writer.writeheader()
            for row in rows:
                writer.writerow({h: ("" if row.get(h) is None else row.get(h)) for h in headers})


def _md_escape(s: Any) -> str:
    text = "" if s is None else str(s)
    # Pipes break GFM tables; backslashes preserve them. Newlines collapse.
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return f"| {' | '.join(headers)} |\n| {' | '.join(['---'] * len(headers))} |\n| {' | '.join(['—'] * len(headers))} |\n"
    body_lines = [f"| {' | '.join(headers)} |", f"| {' | '.join(['---'] * len(headers))} |"]
    for row in rows:
        body_lines.append("| " + " | ".join(_md_escape(c) for c in row) + " |")
    return "\n".join(body_lines) + "\n"


def _export_json(
    path: Path,
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    *,
    column_filter: str = "",
) -> None:
    """Write the comparison as a structured JSON document.

    Designed for the thesis / data-science workflow: load the file
    into a Jupyter notebook with ``json.load(open(path))`` and feed
    the long-format ``per_column`` and ``aggregate_metrics`` arrays
    into pandas / matplotlib without any reshaping.

    Shape::

        {
          "schema_version": 1,
          "generated_at": "2026-05-01T22:00:00",
          "amx_version": "0.11.0",
          "run_count": 3,
          "run_summary":      [ {run_id, started_at, status, ...}, ... ],
          "per_column":       [ {schema, table, column, run_id,
                                 description, confidence, logprob_score,
                                 token_count}, ... ],
          "aggregate_metrics":[ {metric, run_id, value}, ... ]
        }
    """
    from amx import __version__

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "amx_version": __version__,
        "run_count": len(runs),
        "run_summary": _collect_run_summary_rows(runs),
        "per_column": _collect_per_column_long(runs, results_by_run, column_filter=column_filter),
        "aggregate_metrics": _collect_aggregate_long(runs, results_by_run),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _export_markdown(
    path: Path,
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    *,
    column_filter: str = "",
) -> None:
    """Write the three tables as GitHub-flavoured Markdown.

    Per-column results render in wide format (columns per run) so the
    file mirrors what the user just saw on screen and pastes cleanly
    into Notion / GitHub PR descriptions.
    """
    parts: list[str] = []
    parts.append(
        f"# AMX run comparison\n\n"
        f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"{len(runs)} runs_\n\n"
    )

    # Section 1: run summary.
    summary_rows = _collect_run_summary_rows(runs)
    parts.append("## Run summary\n\n")
    parts.append(
        _md_table(
            [
                "Run",
                "Started",
                "Status",
                "Command",
                "DB profile",
                "LLM profile",
                "Model",
                "Doc profile",
                "Code profile",
                "Duration (s)",
                "Processed",
                "Applied",
            ],
            [
                [
                    f"#{r['run_id']}",
                    r["started_at"],
                    r["status"],
                    r["command"],
                    r["db_profile"],
                    r["llm_profile"],
                    r["llm_model"],
                    r["doc_profile"],
                    r["code_profile"],
                    f"{r['duration_sec']:.1f}",
                    r["processed_count"],
                    r["applied_count"],
                ]
                for r in summary_rows
            ],
        )
    )
    parts.append("\n")

    # Section 2: per-column wide table.
    asset_map = _build_asset_map(runs, results_by_run, column_filter)
    parts.append("## Per-column results\n\n")
    parts.append("_Each cell: top alternative · confidence · `logprob_score` · token count._\n\n")
    if not asset_map:
        parts.append("_No overlapping per-column results across the compared runs._\n")
    else:
        per_col_headers = ["Schema.Table.Column"] + [f"Run #{r['id']}" for r in runs]
        per_col_rows: list[list[Any]] = []
        for asset_key in sorted(asset_map.keys()):
            schema_n, table_n, col_n = asset_key
            label = ".".join(p for p in (schema_n, table_n, col_n) if p) or "(unknown)"
            row_cells: list[Any] = [label]
            for run in runs:
                row = asset_map[asset_key].get(int(run["id"]))
                if not row:
                    row_cells.append("—")
                    continue
                desc = _top_alternative(row) or "(empty)"
                band = str(row.get("confidence") or "—")
                logprob = (
                    f"{float(row['logprob_score']):.2f}"
                    if row.get("logprob_score") is not None
                    else "—"
                )
                tokens = _fmt_int(row.get("token_count"))
                row_cells.append(f"{desc} · {band} · {logprob} · {tokens} tok")
            per_col_rows.append(row_cells)
        parts.append(_md_table(per_col_headers, per_col_rows))
    parts.append("\n")

    # Section 3: aggregate metrics, wide.
    aggs = [_aggregate_for_run(r, results_by_run.get(int(r["id"]), [])) for r in runs]
    parts.append("## Aggregate metrics\n\n")
    metric_headers = ["Metric"] + [f"Run #{r['id']}" for r in runs]
    metric_rows: list[list[Any]] = []
    for export_name, agg_key in _AGGREGATE_METRICS:
        cells: list[Any] = [export_name]
        for agg in aggs:
            v = agg.get(agg_key)
            if v is None:
                cells.append("—")
            elif isinstance(v, float):
                cells.append(f"{v:.3f}" if "logprob" in agg_key else f"{v:.1f}")
            else:
                cells.append(v)
        metric_rows.append(cells)
    parts.append(_md_table(metric_headers, metric_rows))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(parts), encoding="utf-8")


# ── PDF report rendering ────────────────────────────────────────────────────

# Direction per aggregate metric, keyed by the export name produced by
# ``_collect_aggregate_long``. Mirrors ``AGGREGATE_DIRECTION`` in
# ``frontend/src/routes/RunsCompare.tsx`` so the SPA's winner ring and
# the PDF's highlighted cell agree on which run "wins" each row.
_PDF_AGGREGATE_DIRECTION: dict[str, str] = {
    "wall_duration_sec": "min",
    "model_processing_sec": "min",
    "prompt_tokens": "min",
    "completion_tokens": "min",
    "total_tokens": "min",
    "cost_usd": "min",
    "avg_logprob_score": "max",
    "pct_high_confidence": "max",
    "pct_medium_confidence": "neutral",
    "pct_low_confidence": "min",
    "approval_rate": "max",
    "saved_results": "neutral",
}

_PDF_AGGREGATE_LABEL: dict[str, str] = {
    "wall_duration_sec": "Wall duration (s)",
    "model_processing_sec": "Model processing (s)",
    "prompt_tokens": "Prompt tokens",
    "completion_tokens": "Completion tokens",
    "total_tokens": "Total tokens",
    "cost_usd": "Cost (USD)",
    "avg_logprob_score": "Avg logprob",
    "pct_high_confidence": "% high confidence",
    "pct_medium_confidence": "% medium confidence",
    "pct_low_confidence": "% low confidence",
    "approval_rate": "Approval rate",
    "saved_results": "Saved results",
}


def _format_aggregate_cell(metric: str, value: Any) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value)
    if metric == "cost_usd":
        if v <= 0:
            return "$0.00"
        return "<$0.01" if v < 0.01 else f"${v:.4f}"
    if metric.startswith("pct_"):
        return f"{v:.0f}%"
    if metric == "approval_rate":
        return f"{v * 100:.0f}%"
    if metric.endswith("_sec"):
        return f"{v:.2f}s"
    if metric == "avg_logprob_score":
        return f"{v:.3f}"
    if v == int(v):
        return f"{int(v):,}"
    return f"{v:,.2f}"


def _pick_aggregate_winner(metric: str, vals: dict[int, Any]) -> int | None:
    direction = _PDF_AGGREGATE_DIRECTION.get(metric, "neutral")
    if direction == "neutral":
        return None
    best_id: int | None = None
    best_val: float | None = None
    for rid, raw in vals.items():
        if raw is None:
            continue
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if not (v == v and v != float("inf") and v != float("-inf")):
            continue
        if best_val is None:
            best_id, best_val = rid, v
            continue
        if direction == "min" and v < best_val or direction == "max" and v > best_val:
            best_id, best_val = rid, v
    return best_id


def _pdf_status_class(status: str | None) -> str:
    """Map a run row's ``status`` to a PDF pill colour class. Mirrors
    ``frontend/src/lib/runDisplay.ts:statusTone`` so the modal and the
    exported PDF use the same green / amber / red / muted palette
    for the same input."""
    s = (status or "").strip().lower()
    if s == "success":
        return "positive"
    if s == "failed":
        return "critical"
    if s == "cancelled":
        return "warning"
    if s in {"running", "queued"}:
        return "accent"
    return "neutral"


def _pdf_status_label(status: str | None) -> str:
    """Match the SPA's ``statusLabel`` shortener — ``ready_for_review``
    is just "ready" in the modal, so the PDF mirrors that."""
    if not status:
        return "—"
    if status == "ready_for_review":
        return "ready"
    return status


def _confidence_class(band: str) -> str:
    b = (band or "").strip().lower()
    if b == "high":
        return "high"
    if b in {"medium", "med"}:
        return "medium"
    if b == "low":
        return "low"
    return ""


def _build_pdf_context(payload: dict[str, Any]) -> dict[str, Any]:
    """Shape the ``compare_runs`` payload into the dict the Jinja
    template consumes — performs the long-to-wide pivots and the
    winner highlight resolution in Python so the template stays
    free of business logic."""
    runs = list(payload.get("runs") or [])
    run_ids: list[int] = [int(r["id"]) for r in runs]
    summary_rows = list(payload.get("summary_rows") or [])
    aggregates = list(payload.get("aggregates") or [])
    per_column = list(payload.get("per_column") or [])
    missing = list(payload.get("missing") or [])

    # Decorate the summary rows with what the template actually
    # displays — provider/model label, pill class, pill text — so
    # the Jinja stays free of branching logic. ``llm_provider`` lives
    # on the raw run row but ``_collect_run_summary_rows`` does not
    # promote it (CSV / JSON exports never needed it). For the PDF we
    # join it in here to mirror the modal's ``openai/gpt-5.4`` look.
    runs_by_id = {int(r.get("id") or 0): r for r in runs}
    enriched_summary: list[dict[str, Any]] = []
    for sr in summary_rows:
        rid = int(sr.get("run_id") or 0)
        raw = runs_by_id.get(rid, {})
        provider = str(raw.get("llm_provider") or "").strip().lower()
        model = str(sr.get("llm_model") or "").strip()
        if provider and model and "/" not in model:
            llm_label = f"{provider}/{model}"
        else:
            llm_label = model or "—"
        status = sr.get("status")
        out = dict(sr)
        out["llm_provider_model"] = llm_label
        out["status_class"] = _pdf_status_class(status)
        out["status_label"] = _pdf_status_label(status)
        enriched_summary.append(out)
    summary_rows = enriched_summary

    # Aggregate pivot: preserve insertion order from the canonical
    # _AGGREGATE_METRICS tuple so the PDF rows match the Studio table.
    agg_by_metric: dict[str, dict[int, Any]] = {}
    for arow in aggregates:
        metric = arow.get("metric")
        if not metric:
            continue
        rid = int(arow.get("run_id"))
        agg_by_metric.setdefault(metric, {})[rid] = arow.get("value")

    aggregate_rows: list[dict[str, Any]] = []
    for export_name, _agg_key in _AGGREGATE_METRICS:
        vals = agg_by_metric.get(export_name)
        if not vals:
            continue
        winner = _pick_aggregate_winner(export_name, vals)
        cells: dict[int, dict[str, Any]] = {}
        for rid in run_ids:
            v = vals.get(rid)
            cells[rid] = {
                "display": _format_aggregate_cell(export_name, v),
                "is_winner": (winner is not None and rid == winner),
            }
        aggregate_rows.append(
            {
                "metric": export_name,
                "label": _PDF_AGGREGATE_LABEL.get(export_name, export_name),
                "cells": cells,
            }
        )

    # Per-column pivot grouped by (schema, table, column). Sort rows
    # so assets with overlap across more runs surface first — matches
    # the SPA's PerColumnPivot ordering.
    by_asset: dict[tuple[str, str, str], dict[int, dict[str, Any]]] = {}
    label_by_asset: dict[tuple[str, str, str], str] = {}
    for prow in per_column:
        key = (
            str(prow.get("schema") or ""),
            str(prow.get("table") or ""),
            str(prow.get("column") or ""),
        )
        rid_raw = prow.get("run_id")
        if rid_raw is None:
            continue
        try:
            rid = int(rid_raw)
        except (TypeError, ValueError):
            continue
        by_asset.setdefault(key, {})[rid] = prow
        if key not in label_by_asset:
            label = ".".join(p for p in key if p) or "—"
            label_by_asset[key] = label

    ordered_keys = sorted(
        by_asset.keys(),
        key=lambda k: (-len(by_asset[k]), label_by_asset[k]),
    )

    percol_rows: list[dict[str, Any]] = []
    for key in ordered_keys:
        cells_map = by_asset[key]
        # Winner per asset row: highest logprob (closest to 0).
        best_rid: int | None = None
        best_lp: float | None = None
        for rid, cell in cells_map.items():
            lp = cell.get("logprob_score")
            if lp is None:
                continue
            try:
                lpv = float(lp)
            except (TypeError, ValueError):
                continue
            if best_lp is None or lpv > best_lp:
                best_rid, best_lp = rid, lpv

        rendered_cells: dict[int, dict[str, Any] | None] = {}
        for rid in run_ids:
            cell = cells_map.get(rid)
            if not cell or not str(cell.get("description") or "").strip():
                rendered_cells[rid] = None
                continue
            lp = cell.get("logprob_score")
            try:
                lp_display = f"{float(lp):.2f}" if lp is not None else ""
            except (TypeError, ValueError):
                lp_display = ""
            rendered_cells[rid] = {
                "description": str(cell.get("description") or "").strip(),
                "confidence": str(cell.get("confidence") or "").strip(),
                "confidence_class": _confidence_class(str(cell.get("confidence") or "")),
                "logprob_display": lp_display,
                "token_count": cell.get("token_count"),
                "is_winner": (best_rid is not None and rid == best_rid),
            }
        percol_rows.append({"label": label_by_asset[key], "cells": rendered_cells})

    # Density tuning: 2 runs → 10pt cells; each extra run drops ~0.5pt
    # down to a 7pt floor. Asset column shrinks proportionally so run
    # columns get the page real estate.
    n_runs = max(1, len(run_ids))
    cell_pt = max(7.0, 10.0 - 0.5 * (n_runs - 2))
    base_pt = max(8.0, 10.5 - 0.3 * (n_runs - 2))
    asset_col_pct = max(14, 26 - 1.4 * (n_runs - 2))
    run_col_pct = (100 - asset_col_pct) / n_runs
    aggregate_run_col_pct = (100 - 22) / n_runs

    from amx import __version__ as amx_version

    run_ids_label = ", ".join(f"#{rid}" for rid in run_ids) if run_ids else "(no runs)"

    # Quality metrics — optional, present only when the caller passed
    # quality_tier > 0 to compare_runs.
    quality = payload.get("quality_metrics") or {}
    quality_per_run = list(quality.get("per_run") or [])
    citations = list(quality.get("citations") or [])
    references_summary = _quality_reference_summary(quality.get("references") or [])

    return {
        "runs": runs,
        "run_ids": run_ids,
        "run_ids_label": run_ids_label,
        "summary_rows": summary_rows,
        "aggregate_rows": aggregate_rows,
        "percol_rows": percol_rows,
        "missing": missing,
        "amx_version": amx_version,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "base_font_pt": f"{base_pt:.1f}",
        "cell_font_pt": f"{cell_pt:.1f}",
        "asset_col_pct": f"{asset_col_pct:.1f}",
        "run_col_pct": f"{run_col_pct:.2f}",
        "aggregate_run_col_pct": f"{aggregate_run_col_pct:.2f}",
        "logo_data_url": _amx_logo_data_url(),
        "quality_per_run": quality_per_run,
        "quality_citations": citations,
        "quality_references_summary": references_summary,
        "quality_tier": int(quality.get("tier") or 0),
    }


def _quality_reference_summary(refs: list[dict[str, Any]]) -> str:
    """One-line summary of how reference resolution went across assets.

    Used by the PDF Quality section header so the reader knows
    whether the chrF / ROUGE-L numbers had a real ground truth or
    fell back to a baseline run.
    """
    if not refs:
        return ""
    by_source: dict[str, int] = {}
    for r in refs:
        by_source[r.get("source", "none")] = by_source.get(r.get("source", "none"), 0) + 1
    pretty = {
        "user_pinned": "user-pinned baseline",
        "db_comment": "live DB COMMENT",
        "catalog_applied": "catalog applied",
        "none": "no reference",
    }
    parts = []
    for src in ("user_pinned", "db_comment", "catalog_applied", "none"):
        if by_source.get(src):
            parts.append(f"{by_source[src]} {pretty[src]}")
    return " · ".join(parts)


def _amx_logo_data_url() -> str:
    """Encode the AMX favicon as a base64 ``data:`` URL so the PDF
    template can drop it into a CSS ``content: url(...)`` running
    header without WeasyPrint having to resolve a filesystem path
    at render time.

    Reads ``amx/web/static/favicon.png`` (the same mark the SPA
    serves at ``/favicon.png`` and the browser tab uses) so the
    PDF brand exactly matches the Studio tab. Returns an empty
    string if the file isn't shipped — running header degrades
    gracefully to no logo instead of breaking the render.
    """
    # Walk up from amx/cli_support/commands/compare.py to the
    # ``amx/`` package root, then hop into ``web/static/``.
    pkg_root = Path(__file__).resolve().parent.parent.parent
    logo_path = pkg_root / "web" / "static" / "favicon.png"
    if not logo_path.is_file():
        return ""
    try:
        encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    except OSError:
        return ""
    return f"data:image/png;base64,{encoded}"


def _bootstrap_weasyprint_native_libs() -> None:
    """Make sure WeasyPrint's cffi-driven dlopen of libpango / libcairo
    can find Homebrew copies on macOS.

    The ``pip install weasyprint`` wheel only carries the Python
    bindings; the native Pango / Cairo / GObject / harfbuzz dylibs
    have to come from the OS package manager (Homebrew on macOS,
    apt on Debian / Ubuntu). On a default macOS install ``cffi``
    calls ``ctypes.util.find_library('pango-1.0')`` which only
    searches a handful of system paths — ``/opt/homebrew/lib``
    (Apple Silicon brew prefix) and ``/usr/local/lib`` (Intel brew
    prefix) are *not* on that list, so a user who ran
    ``brew install pango cairo`` still gets a confusing
    "cannot load library 'libpango-1.0-0'" 500.

    We work around this by appending the brew library directory to
    ``DYLD_FALLBACK_LIBRARY_PATH`` *before* the WeasyPrint import
    runs. macOS ``dyld`` re-reads this variable on every dlopen
    call (unlike ``DYLD_LIBRARY_PATH``, which is read once at
    process start), so setting it from inside Python actually
    takes effect on the very next ``ffi.dlopen`` WeasyPrint runs.
    Linux gets the same treatment via ``LD_LIBRARY_PATH`` for
    Nix / non-FHS layouts.

    Idempotent — safe to call before every render.
    """
    if sys.platform == "darwin":
        env_var = "DYLD_FALLBACK_LIBRARY_PATH"
        candidates = ("/opt/homebrew/lib", "/usr/local/lib")
    elif sys.platform.startswith("linux"):
        env_var = "LD_LIBRARY_PATH"
        candidates = (
            "/usr/lib/x86_64-linux-gnu",
            "/usr/lib/aarch64-linux-gnu",
            "/usr/lib64",
            "/usr/lib",
        )
    else:
        return

    existing = os.environ.get(env_var, "")
    existing_parts = [p for p in existing.split(os.pathsep) if p]
    additions = [p for p in candidates if Path(p).is_dir() and p not in existing_parts]
    if not additions:
        return
    os.environ[env_var] = os.pathsep.join([*existing_parts, *additions])


@contextlib.contextmanager
def _silence_native_stderr() -> Iterator[None]:
    """Redirect the *file-descriptor-level* stderr to /dev/null for the
    duration of the block, then restore it.

    Pango / GLib chatter ("g_datalist_id_set_data_full: assertion
    'key_id > 0' failed", "cannot unreference class of invalid
    (unclassed) type '(null)'") originates inside libpango — it
    bypasses ``sys.stderr`` and writes directly to fd 2. Python-level
    redirects (``contextlib.redirect_stderr``) are no-ops against
    that. The dup2 dance below is what actually quiets the noise so
    the Studio terminal stays readable while a PDF is being rendered.

    Real Python exceptions raised inside the block still propagate —
    we restore the original fd in ``finally`` so any later traceback
    Python prints lands on the user's screen, not in /dev/null.
    """
    try:
        stderr_fd = sys.stderr.fileno()
    except (AttributeError, ValueError, OSError):
        # ``pytest -s`` and some embedded environments replace sys.stderr
        # with an object that has no real fd. The native libs still
        # write to the OS-level fd 2, but if we can't capture the
        # Python-level fd here, just no-op — the user is in a debug
        # context where surfacing every warning is fine.
        yield
        return
    saved_fd = os.dup(stderr_fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull_fd, stderr_fd)
        yield
    finally:
        os.dup2(saved_fd, stderr_fd)
        os.close(devnull_fd)
        os.close(saved_fd)


def render_compare_pdf(payload: dict[str, Any]) -> bytes:
    """Render a ``compare_runs`` payload as a landscape A4 PDF report.

    Lazy-imports Jinja2 + WeasyPrint via :mod:`amx.utils.optional_deps`
    so the heavy Pango/Cairo bindings only land on disk the first time
    the user actually clicks "Download PDF" in Studio (or runs the CLI
    with ``--pdf``). Returns the encoded PDF bytes; the FastAPI route
    streams them straight to the browser.
    """
    from amx.utils.optional_deps import ensure

    ensure(
        [
            ("jinja2", "jinja2"),
            ("weasyprint", "weasyprint"),
        ],
        feature="Compare PDF export",
    )

    # Augment dyld / ld search paths *before* the WeasyPrint import
    # so the very first ffi.dlopen() in this process finds Homebrew
    # / distro Pango. Cheap to call repeatedly (returns early when
    # the env var already lists the directory).
    _bootstrap_weasyprint_native_libs()

    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from weasyprint import HTML

    template_dir = Path(__file__).resolve().parent.parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("compare_report.html")
    context = _build_pdf_context(payload)
    html_str = template.render(**context)

    # Render *and* tear down the WeasyPrint object inside the silenced
    # block. Pango / GLib emit a tail of "g_datalist_id_set_data_full:
    # assertion 'key_id > 0' failed" warnings at object-destroy time
    # (deferred Python __del__), so the noise leaks past write_pdf()
    # unless we force GC inside the redirect.
    import gc

    with _silence_native_stderr():
        html = HTML(string=html_str)
        pdf_bytes = html.write_pdf()
        del html
        gc.collect()
    return pdf_bytes


# ── Quality panel renderer ──────────────────────────────────────────────────


def _fmt_quality_cell(value: float | None, kind: str = "score") -> str:
    """Pretty-print one Quality panel cell.

    ``score`` formats 0–1 floats as percentages; ``count`` formats ints;
    ``levenshtein`` formats raw edit distance integers.
    """
    if value is None:
        return "—"
    if kind == "score":
        return f"{float(value) * 100:.0f}%"
    if kind == "count":
        return f"{int(value):,}"
    if kind == "levenshtein":
        return f"{int(value)} edits"
    return str(value)


def _render_quality_panel(quality: dict[str, Any]) -> None:
    """Rich table rendering of the Tier 0/1/2 quality metrics.

    Shows reference-resolution summary at the top, per-run aggregate
    rollups in the middle, a citation footer at the bottom. Mirrors
    the layout the Studio Compare modal will get in its Quality card.
    """
    per_run = list(quality.get("per_run") or [])
    references = list(quality.get("references") or [])
    citations = list(quality.get("citations") or [])
    if not per_run:
        return

    console.print()
    console.print("[bold]Quality metrics[/bold]")

    # Reference resolution summary line.
    by_source: dict[str, int] = {}
    for r in references:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
    summary_parts = []
    for source in ("user_pinned", "db_comment", "catalog_applied", "none"):
        count = by_source.get(source, 0)
        if count > 0:
            label = {
                "user_pinned": "user-pinned",
                "db_comment": "live DB COMMENT",
                "catalog_applied": "catalog applied",
                "none": "no reference",
            }[source]
            summary_parts.append(f"{count} {label}")
    if summary_parts:
        console.print(f"[dim]References: {', '.join(summary_parts)}[/dim]")

    # Per-run aggregate table.
    table = Table(box=box.SIMPLE_HEAVY, show_lines=False, expand=False)
    table.add_column("Run", style="bold")
    table.add_column("Diversity", justify="right")
    table.add_column("Schema grounding", justify="right")
    has_chrf = any(r.get("chrf") is not None for r in per_run)
    has_rouge = any(r.get("rouge_l") is not None for r in per_run)
    has_bert = any(r.get("bertscore") is not None for r in per_run)
    has_lev = any(r.get("levenshtein") is not None for r in per_run)
    has_emb = any(r.get("embedding_agreement") is not None for r in per_run)
    has_judge = any(r.get("judge_win_rate") is not None for r in per_run)
    if has_chrf:
        table.add_column("chrF", justify="right")
    if has_rouge:
        table.add_column("ROUGE-L", justify="right")
    if has_bert:
        table.add_column("BERTScore", justify="right")
    if has_lev:
        table.add_column("Edit dist.", justify="right")
    if has_emb:
        table.add_column("Embed. agree.", justify="right")
    if has_judge:
        table.add_column("Judge win-rate", justify="right")

    for row in per_run:
        cells = [
            f"#{row['run_id']}",
            _fmt_quality_cell(row.get("type_token_ratio")),
            _fmt_quality_cell(row.get("schema_grounding")),
        ]
        if has_chrf:
            cells.append(_fmt_quality_cell(row.get("chrf")))
        if has_rouge:
            cells.append(_fmt_quality_cell(row.get("rouge_l")))
        if has_bert:
            cells.append(_fmt_quality_cell(row.get("bertscore")))
        if has_lev:
            cells.append(_fmt_quality_cell(row.get("levenshtein"), kind="levenshtein"))
        if has_emb:
            cells.append(_fmt_quality_cell(row.get("embedding_agreement")))
        if has_judge:
            wr = row.get("judge_win_rate")
            pairings = row.get("judge_pairings") or 0
            wins = row.get("judge_wins") or 0
            cells.append(f"{wr * 100:.0f}% ({wins}/{pairings})" if wr is not None else "—")
        table.add_row(*cells)
    console.print(table)

    if citations:
        # Surface only the metric labels and a one-line pointer at the
        # docs page that carries the full bibliographic entries — the
        # CLI panel was getting unreadable when seven citations
        # printed inline after every comparison. Anyone who wants the
        # full Popović 2015 / Lin 2004 / etc. references can read
        # https://amxcli.com/cli/history/#academic-methods.
        labels = " · ".join(f"{c['label']}" for c in citations)
        console.print(f"[dim]Methods: {labels}[/dim]")
        console.print("[dim]Full citations: https://amxcli.com/cli/history/#academic-methods[/dim]")


# ── Ask AMX hand-off ────────────────────────────────────────────────────────


def _build_compare_ask_seed(runs: list[dict[str, Any]]) -> str:
    """Compose the seed prompt fed into ``/ask`` when the user picks
    "Ask AMX about this comparison" at the end of a CLI compare. The
    LLM's first turn already knows the run IDs in question — it can
    call ``compare_runs`` itself to fetch the payload — so the seed
    stays compact instead of dumping the whole pivot into context.
    """
    ids = [int(r.get("id") or 0) for r in runs if r.get("id") is not None]
    id_label = ", ".join(f"#{rid}" for rid in ids) if ids else "(no runs)"
    return (
        f"I just compared runs {id_label}. Walk me through the key "
        "differences (model time, tokens, cost, confidence band split, "
        "avg logprob) and tell me which run produced the most reliable "
        "descriptions. Use the compare_runs tool to fetch the payload."
    )


# ── Public registration ─────────────────────────────────────────────────────


# ── Cell-mode compare (PR C — column-level compare) ───────────────────────
#
# Cell mode pivots the comparison axis: instead of "one row per (schema,
# table, column), one column per run" (the existing per-column pivot), we
# render "one table per cell, one row per run". Use it when the question
# is "how did THIS specific column's description differ across runs?" —
# typically after pinning rows from RunDetail in the Studio drawer or
# typing ``/compare --cell sales.orders.customer_id --runs 10,12,15``
# in the CLI.

_CELL_GLOB_CAP = 50


def _parse_cell_key(cell: str) -> tuple[str, str, str, str | None]:
    """Split a ``db.schema.table[.column]`` cell key.

    Returns ``(db, schema, table, column_or_None)``. Raises
    :class:`ValueError` on a malformed key — the caller surfaces the
    message to the user.
    """
    parts = cell.split(".")
    if len(parts) < 3 or len(parts) > 4:
        raise ValueError("cell must be db.schema.table or db.schema.table.column")
    db, schema, table = parts[0], parts[1], parts[2]
    column = parts[3] if len(parts) == 4 else None
    return db, schema, table, column


def _cell_matches_row(
    row: dict[str, Any],
    schema_pat: str,
    table_pat: str,
    column_pat: str | None,
    *,
    glob: bool,
) -> bool:
    """Return True if ``row`` matches the given cell key (exact or glob).

    When ``column_pat`` is ``None`` we only match table-level rows
    (``column_name`` empty); when it's a non-empty pattern we require
    the row to have a column AND match it.
    """
    import fnmatch as _fn

    s = str(row.get("schema_name") or "")
    t = str(row.get("table_name") or "")
    c = row.get("column_name") or ""
    if glob:
        if not _fn.fnmatch(s, schema_pat):
            return False
        if not _fn.fnmatch(t, table_pat):
            return False
        if column_pat is None:
            return not c
        return bool(c) and _fn.fnmatch(str(c), column_pat)
    if s != schema_pat or t != table_pat:
        return False
    if column_pat is None:
        return not c
    return str(c) == column_pat


def _collect_cell_matches(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    schema_pat: str,
    table_pat: str,
    column_pat: str | None,
    *,
    glob: bool,
) -> list[tuple[tuple[str, str, str | None], dict[int, dict[str, Any]]]]:
    """Find all cells matching the key. Returns a sorted list of
    ``((schema, table, column), {run_id: row})`` tuples."""
    matches: dict[tuple[str, str, str | None], dict[int, dict[str, Any]]] = {}
    for run in runs:
        rid = int(run["id"])
        for row in results_by_run.get(rid, []):
            if not _cell_matches_row(row, schema_pat, table_pat, column_pat, glob=glob):
                continue
            key = (
                str(row.get("schema_name") or ""),
                str(row.get("table_name") or ""),
                (row.get("column_name") or None),
            )
            matches.setdefault(key, {})[rid] = row
    return sorted(matches.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2] or ""))


def _render_single_cell_table(
    runs: list[dict[str, Any]],
    cell_key: tuple[str, str, str | None],
    rows_by_run: dict[int, dict[str, Any]],
    *,
    db_label: str,
) -> None:
    """Render one Rich table for a single cell.

    Columns: ``Run #``, ``Confidence``, ``Logprob``, ``Source``,
    ``Description (truncated)``. Best-pick row (by logprob) bolded green.
    Footer line gives the full path and citation count.
    """
    schema_n, table_n, col_n = cell_key
    full_path = (
        ".".join(p for p in (db_label or None, schema_n, table_n, col_n) if p) or "(unknown)"
    )
    cell_type = "column-level" if col_n else "table-level"

    table = Table(
        title=f"{full_path}  ({cell_type})",
        show_lines=True,
        box=box.SIMPLE_HEAVY,
    )
    table.add_column("Run #", style=info_color(), no_wrap=True)
    table.add_column("Confidence")
    table.add_column("Logprob")
    table.add_column("Source")
    table.add_column("Description (truncated)", overflow="fold", max_width=72)

    scores: list[float | None] = []
    for run in runs:
        row = rows_by_run.get(int(run["id"]))
        scores.append(
            float(row["logprob_score"]) if row and row.get("logprob_score") is not None else None
        )
    winner_idx = _highlight_best(scores, higher_is_better=True)

    citation_count = 0
    for idx, run in enumerate(runs):
        rid = int(run["id"])
        row = rows_by_run.get(rid)
        if not row:
            table.add_row(
                f"#{rid}",
                Text("—", style="dim"),
                Text("—", style="dim"),
                Text("—", style="dim"),
                Text("(not in this run)", style="dim"),
            )
            continue
        band = str(row.get("confidence") or "").lower() or "—"
        logprob = (
            _fmt_float(row.get("logprob_score"), places=2)
            if row.get("logprob_score") is not None
            else "—"
        )
        source = str(row.get("source") or "—")
        desc = _truncate(_top_alternative(row), max_len=72) or "(empty)"
        cites = row.get("citations_json") or []
        if isinstance(cites, list):
            citation_count += len(cites)
        logprob_text = Text(
            logprob,
            style=("bold green" if idx == winner_idx else "white"),
        )
        table.add_row(
            f"#{rid}",
            Text(band, style=_confidence_style(band)),
            logprob_text,
            source,
            desc,
        )

    console.print(table)
    console.print(f"[dim]Full path: {full_path} · citations: {citation_count}[/dim]")


def _render_cell_compare(
    cell: str,
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
) -> int:
    """Render cell-mode comparison. Returns the number of cells rendered."""
    try:
        db_part, schema_pat, table_pat, column_pat = _parse_cell_key(cell)
    except ValueError as exc:
        error(str(exc))
        return 0

    is_glob = "*" in cell
    matches = _collect_cell_matches(
        runs,
        results_by_run,
        schema_pat,
        table_pat,
        column_pat,
        glob=is_glob,
    )

    if not matches:
        warn(
            f"No matching cells for '{cell}' across the selected runs. "
            "Check the schema/table/column spelling or widen the glob."
        )
        return 0

    total = len(matches)
    if total > _CELL_GLOB_CAP:
        warn(
            f"Showing first {_CELL_GLOB_CAP} of {total} matched cells — "
            "narrow the glob to see more."
        )
        matches = matches[:_CELL_GLOB_CAP]

    for i, (cell_key, rows_by_run) in enumerate(matches):
        if i > 0:
            console.print("")
        _render_single_cell_table(runs, cell_key, rows_by_run, db_label=db_part)
    return len(matches)


def register_compare_command(
    search_group: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach ``/compare`` to the existing ``/search`` Click group."""

    @search_group.command("compare")
    @click.argument("run_ids", nargs=-1)
    @click.option(
        "--schema",
        "schema_opt",
        default="",
        help="Limit comparison to runs that touched this schema (default: current schema).",
    )
    @click.option(
        "--table",
        "table_opt",
        default="",
        help="Limit to runs that touched this table (default: current table).",
    )
    @click.option(
        "--column",
        "column_opt",
        default="",
        help="Pivot only one column in the per-column table.",
    )
    @click.option(
        "--last",
        "last_n",
        type=int,
        default=5,
        help="When run IDs are not given, take the last N matching runs (default 5).",
    )
    @click.option(
        "--command",
        "command_filter",
        type=click.Choice(["analyze.run", "search.ask", "all"]),
        default="all",
        help="Restrict to /run results, /ask results, or both (default).",
    )
    @click.option(
        "--by",
        type=click.Choice(
            [
                "auto",
                "model",
                "llm_model",
                "llm_profile",
                "doc_profile",
                "code_profile",
                "db_profile",
                "run",
            ]
        ),
        default="auto",
        help="Highlight which dimension differs across runs.",
    )
    @click.option(
        "--diff",
        "diff_mode",
        is_flag=True,
        default=False,
        help=(
            "Word-level diff in the per-column pivot. The leftmost run is "
            "the baseline; insertions appear bold green, deletions strike-red."
        ),
    )
    @click.option(
        "--csv",
        "csv_path",
        type=click.Path(dir_okay=False, writable=True, resolve_path=True),
        default=None,
        help="Also write the comparison to a CSV file (run summary + per-column long format + aggregate metrics).",
    )
    @click.option(
        "--md",
        "md_path",
        type=click.Path(dir_okay=False, writable=True, resolve_path=True),
        default=None,
        help="Also write the comparison as GitHub-flavoured Markdown (wide-format per-column table).",
    )
    @click.option(
        "--json",
        "json_path",
        type=click.Path(dir_okay=False, writable=True, resolve_path=True),
        default=None,
        help=(
            "Also write the comparison as a structured JSON document — "
            "feeds straight into pandas / Jupyter notebooks for thesis "
            "charts (long-format per_column + aggregate_metrics arrays)."
        ),
    )
    @click.option(
        "--quality",
        "quality_mode",
        type=click.Choice(["none", "basic", "full"]),
        default="basic",
        help=(
            "Quality metric tier. ``none`` skips the academic quality "
            "panel; ``basic`` (default) computes Tier 0 offline metrics "
            "(chrF, ROUGE-L, schema grounding); ``full`` adds Tier 1 "
            "embeddings + Tier 2 LLM-as-judge (cost — uses the active "
            "LLM, see CHANGELOG citations for chrF/ROUGE/G-Eval/etc.)."
        ),
    )
    @click.option(
        "--ground-truth-run",
        "ground_truth_run_id",
        type=int,
        default=None,
        help=(
            "Pin one of the resolved runs as the ground-truth baseline "
            "for reference-based metrics. When omitted the waterfall "
            "tries: live DB COMMENT → catalog-applied → none "
            "(reference-based metrics skip)."
        ),
    )
    @click.option(
        "--cell",
        "cell_key",
        default="",
        help=(
            "Switch to cell-mode comparison — render one table per "
            "matching cell, rows = runs. Key format: "
            "``db.schema.table.column`` (column optional for "
            "table-level). Supports ``*`` glob in any segment "
            "(e.g. ``sales.orders.*``), capped at 50 cells."
        ),
    )
    @click.option(
        "--runs",
        "runs_csv",
        default="",
        help=(
            "Comma-separated run IDs. Convenient with --cell so the "
            "key + the runs land on one line. When omitted, falls "
            "back to positional run IDs or --last/scope resolution."
        ),
    )
    @pass_config
    def search_compare(
        cfg: AMXConfig,
        run_ids: tuple[str, ...],
        schema_opt: str,
        table_opt: str,
        column_opt: str,
        last_n: int,
        command_filter: str,
        by: str,
        diff_mode: bool,
        csv_path: str | None,
        md_path: str | None,
        json_path: str | None,
        quality_mode: str,
        ground_truth_run_id: int | None,
        cell_key: str,
        runs_csv: str,
    ) -> None:
        """Compare runs side-by-side: descriptions, logprobs, timing, tokens."""
        # ``--runs 10,12,15`` is a convenience alias for positional run
        # IDs that pairs naturally with ``--cell``. Merge the two so
        # both spellings are accepted on the same line.
        if runs_csv:
            csv_ids = tuple(x.strip() for x in runs_csv.split(",") if x.strip())
            run_ids = run_ids + csv_ids

        runs = _resolve_runs(
            cfg=cfg,
            run_ids=run_ids,
            schema=schema_opt,
            table=table_opt,
            last_n=last_n,
            command_filter=command_filter,
        )
        if not runs:
            return
        if len(runs) < 2:
            warn(
                "Only one run resolved — nothing to compare. Pass more "
                "run IDs, or widen --last / scope filters."
            )
            return

        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return

        results_by_run: dict[int, list[dict[str, Any]]] = {
            int(r["id"]): hs.get_run_results(int(r["id"])) for r in runs
        }

        # Cell-mode short-circuits the existing run-id-level rendering.
        # Same resolved runs + results, just a different pivot. The
        # other render paths (summary, settings, aggregate, quality
        # panel) are skipped because they don't apply to a single
        # cell — the cell table itself is the whole output.
        if cell_key:
            rendered = _render_cell_compare(cell_key, runs, results_by_run)
            log_event(
                event_type="search_compare",
                status="success",
                command="search.compare",
                details={
                    "mode": "cell",
                    "cell_key": cell_key,
                    "run_ids": [int(r["id"]) for r in runs],
                    "rendered_cells": rendered,
                },
            )
            return

        resolved_by = _detect_by(runs) if by == "auto" else _BY_TO_RUN_KEY.get(by, by)
        info(
            f"Comparing {len(runs)} runs "
            f"(varying dimension: {resolved_by}"
            + (f"; diff vs Run #{runs[0]['id']}" if diff_mode else "")
            + ")"
        )

        _render_run_summary(runs, by=resolved_by)
        # Settings table sits between identity and per-column results
        # so reviewers see WHICH knobs varied before reading the
        # descriptions those knobs produced.
        _render_run_settings(runs)
        _render_per_column_pivot(
            runs,
            results_by_run,
            column_filter=column_opt,
            diff=diff_mode,
        )
        _render_aggregate_metrics(runs, results_by_run)

        if csv_path:
            try:
                _export_csv(
                    Path(csv_path),
                    runs,
                    results_by_run,
                    column_filter=column_opt,
                )
                success(f"Wrote CSV → {csv_path}")
            except OSError as exc:
                error(f"CSV export failed: {exc}")
        if md_path:
            try:
                _export_markdown(
                    Path(md_path),
                    runs,
                    results_by_run,
                    column_filter=column_opt,
                )
                success(f"Wrote Markdown → {md_path}")
            except OSError as exc:
                error(f"Markdown export failed: {exc}")
        if json_path:
            try:
                _export_json(
                    Path(json_path),
                    runs,
                    results_by_run,
                    column_filter=column_opt,
                )
                success(f"Wrote JSON → {json_path}")
            except OSError as exc:
                error(f"JSON export failed: {exc}")

        # Quality panel (Tier 0 by default). Computes academic
        # text-quality metrics on the same payload compare_runs would
        # have built — chrF (Popović 2015), ROUGE-L (Lin 2004), schema
        # grounding (Jaccard 1912), length appropriateness, type-token
        # ratio (Templin 1957). ``--quality full`` adds Tier 1 local
        # embeddings + Tier 2 LLM-as-judge (G-Eval, Liu et al. 2023).
        quality_metrics: dict[str, Any] | None = None
        tier = {"none": 0, "basic": 0, "full": 2}.get(quality_mode, 0)
        if quality_mode != "none":
            try:
                from amx.cli_support.quality import compute_quality_metrics

                # ``--quality basic`` runs Tier 0 only (no LLM cost,
                # no embedding download). ``full`` runs everything.
                payload_for_quality = {
                    "runs": runs,
                    "summary_rows": _collect_run_summary_rows(runs),
                    "per_column": _collect_per_column_long(
                        runs, results_by_run, column_filter=column_opt
                    ),
                    "aggregates": _collect_aggregate_long(runs, results_by_run),
                    "missing": [],
                }
                llm_provider = None
                db_connector = None
                if tier >= 2:
                    try:
                        from amx.llm.provider import LLMProvider

                        llm_provider = LLMProvider(cfg.llm)
                    except Exception as exc:
                        warn(f"Quality judge skipped (LLM unavailable): {exc}")
                        tier = 1
                # DB connector is opportunistic — used by the reference
                # waterfall to pull live ``COMMENT ON COLUMN`` values.
                # Falls through silently when the active scope has no
                # reachable DB profile (CI, fresh install, etc.).
                try:
                    from amx.db.connector import DatabaseConnector

                    db_connector = DatabaseConnector(cfg.db)
                except Exception:
                    db_connector = None
                quality_metrics = compute_quality_metrics(
                    payload_for_quality,
                    tier=tier,
                    db_connector=db_connector,
                    history_store=hs,
                    ground_truth_run_id=ground_truth_run_id,
                    llm_provider=llm_provider,
                )
                _render_quality_panel(quality_metrics)
            except Exception as exc:
                warn(f"Quality analysis failed: {exc}")

        # Numbered prompt for the natural follow-up: "discuss this with
        # the LLM". Default to Done so a user who just wanted the
        # comparison can press Enter and exit without friction. Picking
        # 1 routes through ``launch_ask_session`` — same LLM pre-flight
        # and SearchService construction the typed ``/ask`` command
        # uses, so the chat behaves identically to user-driven entry.
        next_action = "done"
        try:
            choice = ask_choice(
                "What's next?",
                ["Ask AMX about this comparison", "Done"],
                default="Done",
            )
        except Exception:
            # Non-interactive environments (CI, piped stdin) can't
            # prompt — silently skip the follow-up so scripted /history
            # compare invocations stay non-blocking.
            choice = ""
        if choice == "Ask AMX about this comparison":
            from amx.cli_support.commands.search import launch_ask_session

            seed = _build_compare_ask_seed(runs)
            next_action = "ask_amx"
            launch_ask_session(cfg, seed, log_event=log_event)

        log_event(
            event_type="search_compare",
            status="success",
            command="search.compare",
            details={
                "run_ids": [int(r["id"]) for r in runs],
                "by": resolved_by,
                "schema": schema_opt or cfg.current_schema or "",
                "table": table_opt or cfg.current_table or "",
                "column": column_opt,
                "command_filter": command_filter,
                "diff": diff_mode,
                "exported_csv": bool(csv_path),
                "exported_md": bool(md_path),
                "exported_json": bool(json_path),
                "next_action": next_action,
            },
        )


__all__ = ["compare_runs", "register_compare_command", "render_compare_pdf"]
