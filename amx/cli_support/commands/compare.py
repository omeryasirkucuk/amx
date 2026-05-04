"""`/compare` — pivot AMX runs side-by-side for the same assets.

Comparison surface for users who run AMX multiple times against the
same scope under different LLM / doc / code profiles. Produces three
Rich tables: run summary, per-column descriptions pivot, aggregate
metrics. Lives in the ``/search`` namespace so it sits next to ``/ask``
where users naturally end up after running questions.
"""

from __future__ import annotations

import csv
import difflib
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from rich import box
from rich.table import Table
from rich.text import Text

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import console, error, info, success, warn

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
    """
    chosen = (row.get("chosen_description") or "").strip()
    if chosen:
        return chosen
    alts = row.get("alternatives_json")
    if isinstance(alts, list) and alts:
        first = alts[0]
        return str(first).strip() if first else ""
    if isinstance(alts, str) and alts:
        try:
            parsed = json.loads(alts)
        except Exception:
            parsed = []
        if isinstance(parsed, list) and parsed:
            return str(parsed[0]).strip() if parsed[0] else ""
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
    table.add_column("Run", style="cyan", no_wrap=True)
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
            Text(f"#{r.get('id')}", style="cyan"),
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
    table.add_column("Run", style="cyan", no_wrap=True)
    table.add_column("Started", style="cyan", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Command", style="cyan", no_wrap=True)
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
            Text(f"#{r.get('id')}", style="cyan"),
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
    table.add_column("Schema.Table.Column", style="cyan", no_wrap=False, overflow="fold")
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

        cells: list[Text] = [Text(label, style="cyan")]
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
    table.add_column("Metric", style="cyan", no_wrap=True)
    for r in runs:
        table.add_column(f"Run #{r.get('id')}", justify="right")

    def _row(label: str, values: list[Any], best_idx: int | None) -> None:
        cells: list[Text] = [Text(label, style="cyan")]
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


def compare_runs(run_ids: list[int]) -> dict[str, Any]:
    """Pure helper: assemble a JSON-serializable comparison payload
    for a list of run ids. Used by both the CLI ``/history compare``
    command (rendering wrapper) and the ``/api/history/compare``
    endpoint (web UI).

    Returns ``{"runs": [...], "summary_rows": [...], "per_column": [...],
    "aggregates": [...], "missing": [...] }`` where:

    * ``runs`` — full run rows for the ones we found.
    * ``summary_rows`` — per-run roll-up suitable for table rendering.
    * ``per_column`` — long-form rows suitable for pivoting in the
      browser.
    * ``aggregates`` — per-run aggregate metrics.
    * ``missing`` — run ids the caller asked for but the store didn't
      have (so the SPA can show a "run #42 was deleted" toast).
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

    return {
        "runs": found,
        "summary_rows": _collect_run_summary_rows(found),
        "per_column": _collect_per_column_long(found, results_by_run),
        "aggregates": _collect_aggregate_long(found, results_by_run),
        "missing": missing,
    }


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


# ── Public registration ─────────────────────────────────────────────────────


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
    ) -> None:
        """Compare runs side-by-side: descriptions, logprobs, timing, tokens."""
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
            },
        )


__all__ = ["register_compare_command"]
