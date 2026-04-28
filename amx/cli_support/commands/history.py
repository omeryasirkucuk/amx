"""History namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import click

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import confirm, console, error, heading, info, render_table, success, warn

LogEvent = Callable[..., None]


def format_run_scope(scope: dict[str, list[str]] | None) -> str:
    """Render a compact target scope label for history tables."""
    if not isinstance(scope, dict) or not scope:
        return "-"

    schemas = list(scope.keys())
    total_tables = sum(len(tables) for tables in scope.values())
    if len(schemas) == 1:
        schema = schemas[0]
        tables = scope[schema]
        if len(tables) == 1:
            return f"{schema}.{tables[0]}"
        return f"{schema} ({len(tables)} tables)"
    return f"{len(schemas)} schemas ({total_tables} tables)"


def register_history_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach `/history` namespace commands to the main Click group."""

    @main.group()
    def history() -> None:
        """Inspect local SQLite history (runs, tokens, results, events)."""

    @history.command("list")
    @click.option("-n", "--limit", default=20, help="Number of runs to show.")
    def history_list(limit: int) -> None:
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        rows = hs.list_recent_runs(limit=limit)
        if not rows:
            info("No run history yet.")
            return

        table_rows = []
        for row in rows:
            table_rows.append([
                str(row.get("id", "")),
                f"{float(row.get('started_at') or 0):.0f}",
                {
                    "success": "[bold green]success[/bold green]",
                    "failed": "[bold red]failed[/bold red]",
                    "cancelled": "[bold yellow]cancelled[/bold yellow]",
                    "ready_for_review": "[bold magenta]ready_for_review[/bold magenta]",
                    "running": "[bold cyan]running[/bold cyan]",
                }.get(str(row.get("status", "")), str(row.get("status", ""))),
                str(row.get("mode", "")),
                str(row.get("db_backend", "")),
                format_run_scope(row.get("scope_json")),
                f"{row.get('llm_provider', '')}/{row.get('llm_model', '')}",
                f"{float(row.get('duration_sec') or 0):.2f}",
                f"{float((row.get('metrics_json') or {}).get('model_processing_sec') or 0):.2f}",
            ])

        render_table(
            "Recent runs",
            [
                "ID",
                "Start (epoch)",
                "Status",
                "Mode",
                "Backend",
                "Target Scope",
                "Provider/Model",
                "Duration(s)",
                "Model(s)",
            ],
            table_rows,
        )

    @history.command("show")
    @click.argument("run_id", type=int)
    def history_show(run_id: int) -> None:
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        row = hs.get_run(run_id)
        if not row:
            error(f"Run {run_id} not found.")
            return
        payload = {
            "id": row.get("id"),
            "started_at": row.get("started_at"),
            "ended_at": row.get("ended_at"),
            "duration_sec": row.get("duration_sec"),
            "status": row.get("status"),
            "command": row.get("command"),
            "mode": row.get("mode"),
            "db_backend": row.get("db_backend"),
            "db_profile": row.get("db_profile"),
            "llm_provider": row.get("llm_provider"),
            "llm_model": row.get("llm_model"),
            "scope": row.get("scope_json"),
            "metrics": row.get("metrics_json"),
            "tokens": row.get("tokens_json"),
            "results": row.get("results_json"),
            "error": row.get("error_text"),
        }
        result_rows = hs.get_run_results(run_id)
        payload["metadata_decisions"] = {
            "total": len(result_rows),
            "pending": sum(1 for r in result_rows if not r.get("evaluation")),
            "reviewed": sum(1 for r in result_rows if r.get("evaluation") in {"accepted", "custom"}),
            "rejected": sum(1 for r in result_rows if r.get("evaluation") == "skipped"),
            "indexed": sum(1 for r in result_rows if r.get("catalog_indexed_at")),
            "applied": sum(1 for r in result_rows if r.get("applied_at")),
        }
        console.print(json.dumps(payload, indent=2, ensure_ascii=True))

    @history.command("stats")
    def history_stats() -> None:
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        stats = hs.stats()
        search_counts = {}
        try:
            from amx.search.catalog import SearchCatalog

            catalog = SearchCatalog.from_history_store()
            if catalog is not None:
                search_counts = catalog.history_counts()
        except Exception:
            search_counts = {}
        render_table(
            "History stats",
            ["Metric", "Value"],
            [
                ["total_runs", stats.get("total_runs", 0)],
                ["success_runs", stats.get("success_runs", 0)],
                ["failed_runs", stats.get("failed_runs", 0)],
                ["avg_duration_sec", f"{float(stats.get('avg_duration_sec') or 0):.2f}"],
                ["avg_model_processing_sec", f"{float(stats.get('avg_model_processing_sec') or 0):.2f}"],
                ["last_started_at", f"{float(stats.get('last_started_at') or 0):.0f}"],
                ["total_events", stats.get("total_events", 0)],
                ["reviewed_descriptions", search_counts.get("reviewed_count", 0)],
                ["rejected_descriptions", search_counts.get("rejected_count", 0)],
                ["manual_overrides", search_counts.get("manual_count", 0)],
                ["indexed_descriptions", search_counts.get("indexed_count", 0)],
                ["applied_descriptions", search_counts.get("applied_count", 0)],
                ["stale_entities", search_counts.get("stale_count", 0)],
            ],
        )

    @history.command("events")
    @click.option("-n", "--limit", default=30, help="Number of events to show.")
    def history_events(limit: int) -> None:
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        rows = hs.list_recent_events(limit=limit)
        if not rows:
            info("No events yet.")
            return
        render_table(
            "Recent events",
            ["ID", "Time (epoch)", "Type", "Status", "Command", "Details"],
            [
                [
                    row.get("id", ""),
                    f"{float(row.get('created_at') or 0):.0f}",
                    row.get("event_type", ""),
                    row.get("status", ""),
                    row.get("command", ""),
                    json.dumps(row.get("details_json", {}), ensure_ascii=True)[:80],
                ]
                for row in rows
            ],
        )

    @history.command("results")
    @click.argument("run_id", type=int)
    def history_results(run_id: int) -> None:
        """Show all saved LLM alternatives for a past run."""
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        rows = hs.get_run_results(run_id)
        if not rows:
            error(f"No saved alternatives for run {run_id}. (Alternatives are only stored for runs made with v0.1.39+.)")
            return

        heading(f"Saved alternatives - run #{run_id}")

        top_level = [row for row in rows if not row.get("column_name")]
        column_rows = [row for row in rows if row.get("column_name")]

        if top_level:
            from rich.panel import Panel

            for row in top_level:
                alternatives = row.get("alternatives_json") or []
                kind = row.get("asset_kind", "table").upper()
                asset_label = f"{row.get('schema_name', '')}.{row.get('table_name', '')}"
                status_label = row.get("evaluation") or "pending"
                chosen = row.get("chosen_description") or ""
                lines = []
                if alternatives:
                    for index, alt in enumerate(alternatives, 1):
                        lines.append(f"  [dim]{index}.[/dim] {alt}")
                else:
                    lines.append("  [dim](no alternatives stored)[/dim]")
                if chosen:
                    lines.append(f"\n  [bold green]Chosen:[/bold green] {chosen}")
                selected_at = row.get("evaluated_at")
                applied_at = row.get("applied_at")
                if selected_at:
                    lines.append(
                        "  [dim]Selected at:[/dim] "
                        + datetime.fromtimestamp(selected_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    )
                if applied_at:
                    lines.append(
                        "  [dim]Applied at:[/dim] "
                        + datetime.fromtimestamp(applied_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    )
                console.print(
                    Panel(
                        "\n".join(lines),
                        title=f"[bold cyan]{kind} DESCRIPTION[/bold cyan] - {asset_label}  [dim][{status_label}][/dim]",
                        border_style="cyan",
                        expand=False,
                    )
                )

        table_rows = []
        for row in column_rows:
            alternatives = row.get("alternatives_json") or []
            if alternatives:
                alternatives_str = "\n".join(f"{index}. {alt}" for index, alt in enumerate(alternatives, 1))
            else:
                alternatives_str = "-"
            evaluated_at = row.get("evaluated_at")
            applied_at = row.get("applied_at")
            table_rows.append([
                row.get("id", ""),
                row.get("table_name", ""),
                row.get("column_name") or "(table)",
                row.get("confidence", ""),
                f"{float(row.get('logprob_score')):.4f}" if row.get("logprob_score") is not None else "N/A",
                alternatives_str,
                row.get("evaluation") or "pending",
                (row.get("chosen_description") or "")[:40],
                row.get("catalog_status") or "-",
                row.get("effective_source_kind") or "-",
                "yes" if row.get("catalog_indexed_at") else "no",
                row.get("db_applied_status") or ("applied" if row.get("applied_at") else "-"),
                datetime.fromtimestamp(evaluated_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if evaluated_at else "",
                datetime.fromtimestamp(applied_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if applied_at else "",
            ])
        if table_rows:
            render_table(
                f"Run #{run_id} - Column alternatives",
                [
                    "Row",
                    "Table",
                    "Column",
                    "Conf",
                    "Logprob",
                    "Alternatives (all)",
                    "Status",
                    "Chosen",
                    "Catalog",
                    "Effective source",
                    "Indexed",
                    "DB apply",
                    "Selected at",
                    "Applied at",
                ],
                table_rows,
            )

        pending = sum(1 for row in rows if not row.get("evaluation"))
        if pending:
            info(f"{pending} item(s) still pending. Run `/review {run_id}` to evaluate them.")
        info(
            f"To pick a different saved alternative later, run `/review {run_id}` "
            f"and use `--apply` to write newly approved choices to the database."
        )

    @history.command("review")
    @click.argument("run_id", type=int)
    @click.option(
        "--unevaluated-only",
        is_flag=True,
        default=False,
        help="Skip items already evaluated; only show pending rows.",
    )
    @click.option(
        "--apply",
        is_flag=True,
        default=False,
        help="Write approved descriptions to the database immediately after review.",
    )
    @pass_config
    def history_review(cfg: AMXConfig, run_id: int, unevaluated_only: bool, apply: bool) -> None:
        """Re-evaluate saved LLM alternatives for a past run."""
        from amx.agents.base import Confidence
        from amx.agents.orchestrator import (
            Orchestrator,
            ReviewResult,
            apply_review_results_to_db,
            create_live_writeback_progress,
        )
        from amx.db.connector import DatabaseConnector
        from amx.llm.provider import LLMProvider

        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return

        rows = hs.get_run_results(run_id, unevaluated_only=unevaluated_only)
        if not rows:
            if unevaluated_only:
                success(f"No pending items for run #{run_id} - all alternatives have been evaluated.")
            else:
                error(f"No saved alternatives for run #{run_id}.")
            return

        heading(f"Re-evaluating alternatives - run #{run_id} ({len(rows)} item(s))")
        if unevaluated_only:
            info(f"Showing {len(rows)} unevaluated item(s) only (use without --unevaluated-only to review all).")
        else:
            info(f"Showing all {len(rows)} item(s) - already-evaluated rows will ask if you want to change your choice.")

        def _mark_run_success() -> None:
            try:
                hs.update_run_status(run_id, "success")
            except Exception as exc:
                warn(f"Could not update run #{run_id} status to success: {exc}")

        rows_sorted = sorted(rows, key=lambda row: (0 if not row.get("column_name") else 1, row.get("id", 0)))

        results_to_review = []
        for row in rows_sorted:
            alternatives: list[str] = row.get("alternatives_json") or []
            if not alternatives:
                warn(f"Row {row['id']} ({row['table_name']}.{row.get('column_name') or '(table)'}) has no alternatives stored - skipping.")
                continue

            try:
                confidence = Confidence(row.get("confidence", "medium"))
            except ValueError:
                confidence = Confidence.MEDIUM

            evaluation = row.get("evaluation")
            if evaluation in {"accepted", "custom"}:
                best_desc = row.get("chosen_description") or alternatives[0]
            elif evaluation == "skipped":
                best_desc = ""
            else:
                best_desc = alternatives[0]

            results_to_review.append(
                ReviewResult(
                    schema=row.get("schema_name", ""),
                    table=row["table_name"],
                    column=row.get("column_name"),
                    final_description=best_desc,
                    confidence=confidence,
                    source=row.get("source", "combined"),
                    applied=False,
                    asset_kind=row.get("asset_kind", "table"),
                    result_id=row["id"],
                    alternatives=alternatives,
                    logprob_score=(float(row["logprob_score"]) if row.get("logprob_score") is not None else None),
                )
            )

        db = DatabaseConnector(cfg.db)
        llm = LLMProvider(cfg.llm)
        orch = Orchestrator(db=db, llm=llm, search_profile=cfg.active_db_profile or "default")

        final_results = orch.batch_review(results_to_review)
        newly_approved = [result for result in final_results if result.applied]

        if not newly_approved:
            _mark_run_success()
            info("No descriptions approved - nothing to apply or save.")
            return

        render_table(
            "Approved in this review session",
            ["Asset", "Description", "Confidence", "Logprob", "Source"],
            [
                [
                    (
                        f"{result.schema}.{result.table}.{result.column}"
                        if result.column
                        else (f"{result.schema}.{result.table}" if result.table else result.schema)
                    ),
                    (result.final_description or "")[:60],
                    result.confidence.value,
                    f"{result.logprob_score:.4f}" if result.logprob_score is not None else "N/A",
                    result.source,
                ]
                for result in newly_approved
            ],
        )

        if apply:
            if not cfg.db.backend:
                error("No database configured. Cannot apply.")
                return
            if confirm(f"Apply {len(newly_approved)} comment(s) to the database?", default=True):
                db = DatabaseConnector(cfg.db)
                if not db.test_connection():
                    error("Cannot connect to database.")
                    return

                def _on_applied(result: Any) -> None:
                    inner_hs = history_store()
                    if result.result_id is not None and inner_hs is not None:
                        inner_hs.record_applied(result.result_id)
                    if result.result_id is not None:
                        try:
                            from amx.search.catalog import SearchCatalog

                            catalog = SearchCatalog.from_history_store()
                            if catalog is not None:
                                catalog.mark_applied(result.result_id)
                        except Exception as exc:
                            warn(f"Could not update /search apply state for result {result.result_id}: {exc}")

                def _on_failed(result: Any, exc: Exception) -> None:
                    inner_hs = history_store()
                    if result.result_id is not None and inner_hs is not None:
                        try:
                            inner_hs.record_db_apply_failure(result.result_id, str(exc))
                        except Exception as inner_exc:
                            warn(f"Could not record failed DB apply state for result {result.result_id}: {inner_exc}")

                _on_progress, _finish_progress = create_live_writeback_progress(
                    total=len(newly_approved),
                    backend=db.backend,
                )

                try:
                    applied = apply_review_results_to_db(
                        db,
                        newly_approved,
                        on_applied=_on_applied,
                        on_failed=_on_failed,
                        on_progress=_on_progress if newly_approved else None,
                    )
                finally:
                    if newly_approved:
                        _finish_progress()
                success(f"Applied {applied} metadata comment(s) to the database.")
                log_event(
                    event_type="history_review_apply",
                    status="success",
                    command="history.review",
                    details={"run_id": run_id, "applied_count": applied},
                )
        else:
            from amx.pending_review import save_pending

            save_pending(newly_approved)
            info(
                f"Saved {len(newly_approved)} approved description(s) as pending. "
                "Run `/analyze` then `/apply` to write them to the database."
            )

        _mark_run_success()
