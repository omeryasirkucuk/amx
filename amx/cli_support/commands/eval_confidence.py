"""``/history eval-confidence`` — measure each confidence signal against the user's accepted choice."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from amx.config import AMXConfig
from amx.eval.confidence import compute_metrics, render_markdown
from amx.storage.sqlite_store import history_store, parse_alternatives_json
from amx.utils.console import error, info, success

LogEvent = Callable[..., None]


def _load_rows(hs: Any) -> list[dict[str, Any]]:
    """Pull every ``run_results`` row with a non-empty ``accepted`` value.

    The history store exposes ``get_all_run_results`` on the SQLite
    backend; we fall back to scanning per-run when only ``list_runs``
    is available so the harness still works on older shared-store
    backends.
    """
    if hasattr(hs, "get_all_run_results"):
        candidate_rows = hs.get_all_run_results()
    else:
        candidate_rows = []
        for run in hs.list_runs(limit=None):
            candidate_rows.extend(hs.get_run_results(int(run["id"])))

    out: list[dict[str, Any]] = []
    for row in candidate_rows:
        accepted = row.get("accepted") or row.get("chosen_description")
        if not accepted:
            continue
        parsed = parse_alternatives_json(row.get("alternatives_json"))
        if not parsed:
            continue
        out.append({"alternatives": parsed, "accepted": accepted})
    return out


def register_eval_confidence_command(
    history_group: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach ``/history eval-confidence`` to the ``/history`` namespace."""

    @history_group.command("eval-confidence")
    @click.option(
        "--output",
        "output_path",
        type=click.Path(dir_okay=False, writable=True, resolve_path=True),
        default=None,
        help="Where to write the Markdown report. Defaults to "
        "``~/.amx/reports/confidence-eval-<timestamp>.md``.",
    )
    @pass_config
    def eval_confidence(cfg: AMXConfig, output_path: str | None) -> None:
        """Compute per-signal top-1 / top-2 accuracy on reviewed runs and write a Markdown report."""
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return

        rows = _load_rows(hs)
        metrics = compute_metrics(rows)

        if output_path is None:
            reports_dir = Path(os.path.expanduser("~/.amx/reports"))
            reports_dir.mkdir(parents=True, exist_ok=True)
            ts = time.strftime("%Y%m%dT%H%M%S")
            output_path = str(reports_dir / f"confidence-eval-{ts}.md")

        report_md = render_markdown(metrics)
        Path(output_path).write_text(report_md, encoding="utf-8")

        info(f"Sample count: {metrics['sample_count']}")
        for name, m in (metrics.get("signals") or {}).items():
            info(
                f"  {name}: top-1 {m['top1_accuracy']:.2%} · top-2 {m['top2_accuracy']:.2%} "
                f"(n={m['scored_rows']})"
            )
        success(f"Wrote report to {output_path}")

        log_event(
            event_type="eval_confidence",
            status="success",
            command="history.eval-confidence",
            details={
                "sample_count": metrics["sample_count"],
                "signals": list((metrics.get("signals") or {}).keys()),
                "output_path": output_path,
            },
        )


__all__ = ["register_eval_confidence_command"]
