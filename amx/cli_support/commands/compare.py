"""`/compare` — pivot AMX runs side-by-side for the same assets.

Comparison surface for users who run AMX multiple times against the
same scope under different LLM / doc / code profiles. Produces three
Rich tables: run summary, per-column descriptions pivot, aggregate
metrics. Lives in the ``/search`` namespace so it sits next to ``/ask``
where users naturally end up after running questions.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

import click
from rich import box
from rich.table import Table
from rich.text import Text

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import console, error, info, warn

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
            "No scope to compare — pass run IDs, set /schema, or use "
            "--schema/--table. Example: /compare --schema sales --last 3."
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


def _aggregate_for_run(
    run: dict[str, Any], results: list[dict[str, Any]]
) -> dict[str, Any]:
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
        float(r["logprob_score"])
        for r in results
        if r.get("logprob_score") is not None
    ]
    avg_logprob = (
        sum(logprob_scores) / len(logprob_scores) if logprob_scores else None
    )

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


def _render_per_column_pivot(
    runs: list[dict[str, Any]],
    results_by_run: dict[int, list[dict[str, Any]]],
    column_filter: str,
) -> None:
    """Pivot run_results so rows are columns/tables, cells are runs."""
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

    if not asset_map:
        warn(
            "No saved per-column results overlap across the selected runs. "
            "Compared runs may have produced no LLM alternatives, or were "
            "narrower than --column requires."
        )
        return

    table = Table(
        title="Per-column results (top alternative · band · logprob · tokens)",
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

        cells: list[Text] = [Text(label, style="cyan")]
        for col_idx, run in enumerate(runs):
            row = runs_for_asset.get(int(run["id"]))
            if not row:
                cells.append(Text("—", style="dim"))
                continue
            desc = _truncate(_top_alternative(row), max_len=58)
            band = str(row.get("confidence") or "").lower() or "—"
            logprob = (
                _fmt_float(row.get("logprob_score"), places=2)
                if row.get("logprob_score") is not None
                else "—"
            )
            tokens = _fmt_int(row.get("token_count"))
            cell = Text()
            cell.append(desc or "(empty)", style=("white" if desc else "dim"))
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
    aggs = [
        _aggregate_for_run(r, results_by_run.get(int(r["id"]), [])) for r in runs
    ]

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
            _fmt_float(a["avg_logprob"], places=3)
            if a["avg_logprob"] is not None
            else "—"
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
            f"{a['approval_rate'] * 100:.0f}%"
            if a["approval_rate"] is not None
            else "—"
            for a in aggs
        ],
        _highlight_best(approval_vals, higher_is_better=True),
    )
    # Result count (informational only — no winner)
    _row("Saved results", [_fmt_int(a["result_count"]) for a in aggs], None)

    console.print(table)


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
        "--schema", "schema_opt",
        default="",
        help="Limit comparison to runs that touched this schema (default: current schema).",
    )
    @click.option(
        "--table", "table_opt",
        default="",
        help="Limit to runs that touched this table (default: current table).",
    )
    @click.option(
        "--column", "column_opt",
        default="",
        help="Pivot only one column in the per-column table.",
    )
    @click.option(
        "--last", "last_n",
        type=int,
        default=5,
        help="When run IDs are not given, take the last N matching runs (default 5).",
    )
    @click.option(
        "--command", "command_filter",
        type=click.Choice(["analyze.run", "search.ask", "all"]),
        default="all",
        help="Restrict to /run results, /ask results, or both (default).",
    )
    @click.option(
        "--by",
        type=click.Choice(
            ["auto", "model", "llm_model", "llm_profile",
             "doc_profile", "code_profile", "db_profile", "run"]
        ),
        default="auto",
        help="Highlight which dimension differs across runs.",
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
            f"(varying dimension: {resolved_by})"
        )

        _render_run_summary(runs, by=resolved_by)
        _render_per_column_pivot(runs, results_by_run, column_filter=column_opt)
        _render_aggregate_metrics(runs, results_by_run)

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
            },
        )


__all__ = ["register_compare_command"]
