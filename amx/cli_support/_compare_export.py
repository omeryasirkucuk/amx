"""CSV / JSON / Markdown export helpers for ``amx compare``.

Extracted from :mod:`amx.cli_support.commands.compare` so the three
exporters live in their own focused module. The compare command
re-exports them so historical imports
(``from amx.cli_support.commands.compare import _export_csv`` in
tests/test_compare.py) keep working unchanged.

The collectors + constants stay in ``compare.py`` to avoid a circular
import; this module imports them lazily inside each exporter — each
exporter pulls in only the symbols it actually uses.
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any


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
    from amx.cli_support.commands.compare import (
        _AGGREGATE_COLUMNS,
        _PER_COLUMN_LONG_COLUMNS,
        _RUN_SUMMARY_COLUMNS,
        _collect_aggregate_long,
        _collect_per_column_long,
        _collect_run_summary_rows,
    )

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
    from amx.cli_support.commands.compare import (
        _collect_aggregate_long,
        _collect_per_column_long,
        _collect_run_summary_rows,
    )

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
    from amx.cli_support._compare_format import _fmt_int, _md_table
    from amx.cli_support.commands.compare import (
        _AGGREGATE_METRICS,
        _aggregate_for_run,
        _build_asset_map,
        _collect_run_summary_rows,
        _top_alternative,
    )

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
