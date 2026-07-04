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
from amx.utils.terminal_theme import accent_color

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
) -> click.Group:
    """Attach `/history` namespace commands to the main Click group.

    Returns the inner ``history`` Click group so callers (``cli.py``) can
    attach extra subcommands to the same namespace from sibling files —
    same pattern as ``register_analyze_commands`` / ``register_search_commands``.
    Used by ``register_compare_command`` to attach ``/compare`` here, where
    a "compare past runs" verb belongs (audit, not search).
    """

    @main.group()
    def history() -> None:
        """Inspect local SQLite history (runs, tokens, results, events, comparisons)."""

    @history.command("list")
    @click.option("-n", "--limit", type=int, default=None, help="Number of runs to show.")
    @click.option(
        "--include-asks/--no-include-asks",
        "include_asks",
        default=None,
        help=(
            "Also include /ask invocations (search.ask) in the listing. "
            "By default the list shows only /run invocations because /ask "
            "chat sessions belong in /session list (with resume support)."
        ),
    )
    @click.pass_context
    def history_list(ctx: click.Context, limit: int | None, include_asks: bool | None) -> None:
        """List recent runs.

        Bare ``/list`` runs a short wizard (matches ``/run``'s pattern):
        the user picks how many rows to show and whether to include
        ``/ask`` chat sessions. Power users skip the wizard with
        explicit flags (``/list -n 5``, ``/list --include-asks``).
        """
        from datetime import datetime as _dt

        from amx.utils.console import ask, ask_choice

        # Wizard for any value the user didn't pin via a flag. Click's
        # parameter-source check tells us whether ``--limit`` /
        # ``--include-asks`` were typed on the command line.
        # ParameterSource.DEFAULT means we're free to ask; .COMMANDLINE
        # means the user gave an explicit value (scripts, power users).
        try:
            from click.core import ParameterSource

            limit_src = ctx.get_parameter_source("limit")
            asks_src = ctx.get_parameter_source("include_asks")
            limit_from_user = limit_src == ParameterSource.COMMANDLINE
            asks_from_user = asks_src == ParameterSource.COMMANDLINE
        except Exception:
            limit_from_user = limit is not None
            asks_from_user = include_asks is not None

        if not limit_from_user:
            raw = ask("How many runs to show?", default="20").strip()
            try:
                limit = max(1, int(raw))
            except ValueError:
                warn(f"Could not parse '{raw}' as a number; using 20.")
                limit = 20
        else:
            limit = int(limit) if limit else 20

        if not asks_from_user:
            choice = ask_choice(
                "Which runs to list?",
                ["only /run invocations", "include /ask sessions too"],
                default="only /run invocations",
                descriptions={
                    "only /run invocations": (
                        "/ask chat sessions are resumable threads — see /session list."
                    ),
                    "include /ask sessions too": (
                        "Show every command in history; useful for debugging."
                    ),
                },
            )
            include_asks = choice == "include /ask sessions too"
        else:
            include_asks = bool(include_asks)

        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        # Default: only /run invocations. /ask sessions live under /session list.
        rows = hs.list_recent_runs(
            limit=limit,
            command_filter=None if include_asks else "analyze.run",
        )
        if not rows:
            if include_asks:
                info("No run or ask history yet.")
            else:
                info(
                    "No /run history yet. (For /ask chat sessions: /session list, "
                    "or pass --include-asks here.)"
                )
            return

        def _fmt_started(epoch: float) -> str:
            try:
                return _dt.fromtimestamp(float(epoch or 0)).strftime("%Y-%m-%d %H:%M")
            except Exception:
                return "—"

        def _fmt_duration(sec: float) -> str:
            s = float(sec or 0)
            if s <= 0:
                return "—"
            if s < 60:
                return f"{s:.1f}s"
            m, rem = divmod(s, 60)
            return f"{int(m)}m {rem:0.0f}s"

        table_rows = []
        for row in rows:
            # ``Processed`` reports honest progress for runs that the
            # missing-only filter narrowed (planned < selected) or that the
            # user cancelled mid-loop. Format: ``processed/planned`` so the
            # user reads at a glance "I asked for 78, AMX planned 60 after
            # the filter, processed 3 before I hit Ctrl+C". When the new
            # counters are zero (older rows or future-incompatible state),
            # fall back to "—".
            planned = int(row.get("planned_count") or 0)
            processed = int(row.get("processed_count") or 0)
            applied = int(row.get("applied_count") or 0)
            if planned > 0 or processed > 0:
                processed_label = f"{processed}/{planned}"
                if applied and applied != processed:
                    processed_label += f"  applied {applied}"
            else:
                processed_label = "—"
            table_rows.append(
                [
                    str(row.get("id", "")),
                    _fmt_started(row.get("started_at") or 0),
                    {
                        "success": "[bold green]success[/bold green]",
                        "failed": "[bold red]failed[/bold red]",
                        "cancelled": "[bold yellow]cancelled[/bold yellow]",
                        "ready_for_review": "[bold #fed7aa]ready_for_review[/bold #fed7aa]",
                        "running": "[heading]running[/heading]",
                    }.get(str(row.get("status", "")), str(row.get("status", ""))),
                    str(row.get("mode", "")),
                    str(row.get("db_backend", "")),
                    format_run_scope(row.get("scope_json")),
                    processed_label,
                    f"{row.get('llm_provider', '')}/{row.get('llm_model', '')}",
                    _fmt_duration(row.get("duration_sec") or 0),
                    _fmt_duration((row.get("metrics_json") or {}).get("model_processing_sec") or 0),
                ]
            )

        title = (
            "Recent /run invocations" if not include_asks else "Recent runs (incl. /ask sessions)"
        )
        render_table(
            title,
            [
                "ID",
                "Started",
                "Status",
                "Mode",
                "Backend",
                "Scope",
                "Processed",
                "Provider/Model",
                "Wall",
                "Model time",
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
            # ``settings_json`` captures the effective LLM config at run-time
            # — including any per-run override picked from the interactive
            # gate (``Override LLM settings for this run? [y/N]`` in
            # ``analyze_flow.py``). Surfacing it here lets ``/history show``
            # explain why a given run produced what it produced, even when
            # the saved profile has since drifted from those values.
            "settings": row.get("settings_json"),
            "metrics": row.get("metrics_json"),
            "tokens": row.get("tokens_json"),
            "results": row.get("results_json"),
            "error": row.get("error_text"),
        }
        result_rows = hs.get_run_results(run_id)
        payload["metadata_decisions"] = {
            "total": len(result_rows),
            "pending": sum(1 for r in result_rows if not r.get("evaluation")),
            "reviewed": sum(
                1 for r in result_rows if r.get("evaluation") in {"accepted", "custom"}
            ),
            "rejected": sum(1 for r in result_rows if r.get("evaluation") == "skipped"),
            "indexed": sum(1 for r in result_rows if r.get("catalog_indexed_at")),
            "applied": sum(1 for r in result_rows if r.get("applied_at")),
        }
        console.print(json.dumps(payload, indent=2, ensure_ascii=True))

    @history.command("delete")
    @click.argument("run_ids", nargs=-1, type=int)
    @click.option(
        "--all",
        "delete_all",
        is_flag=True,
        help="Delete every run shown in /history list (analyze.run invocations).",
    )
    @click.option("-y", "--yes", is_flag=True, help="Skip the confirmation prompt.")
    def history_delete(run_ids: tuple[int, ...], delete_all: bool, yes: bool) -> None:
        """Hard-delete previous run(s) and their per-asset result rows.

        The applied-description audit trail is left intact — clear a
        table's applied history with ``/review-clear``. Examples::

            /delete 42            delete one run
            /delete 42 43 44      delete several at once
            /delete --all         delete every run in the history list
            /delete               pick interactively (bare wizard)
        """
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return
        heading("History · delete")

        ids = list(run_ids)
        resolved_all = delete_all

        # Bare invocation with nothing pinned → short wizard: show the
        # recent runs and let the user name ids (or 'all').
        if not ids and not resolved_all:
            from amx.utils.console import ask

            recent = hs.list_recent_runs(limit=20, command_filter="analyze.run")
            if not recent:
                info("No runs in history.")
                return
            render_table(
                "Recent runs",
                ["ID", "Started", "Status", "Command"],
                [
                    [
                        r.get("id"),
                        r.get("started_at"),
                        r.get("status"),
                        r.get("command"),
                    ]
                    for r in recent
                ],
            )
            raw = ask(
                "Run id(s) to delete (comma/space separated), or 'all'",
                default="",
            ).strip()
            if not raw:
                warn("Cancelled.")
                return
            if raw.lower() == "all":
                resolved_all = True
            else:
                try:
                    ids = [int(tok) for tok in raw.replace(",", " ").split()]
                except ValueError:
                    error(f"Could not parse run ids from '{raw}'.")
                    return

        if resolved_all:
            matching = hs.list_recent_runs(limit=1_000_000, command_filter="analyze.run")
            n = len(matching)
            if n == 0:
                info("No runs to delete.")
                return
            if not yes and not confirm(
                f"Delete all {n} run(s) and their results? This cannot be undone.",
                default=False,
            ):
                warn("Cancelled.")
                return
            counts = hs.delete_runs_matching(command_filter="analyze.run")
            success(f"Deleted {counts['runs']} run(s) and {counts['results']} result row(s).")
            log_event(
                event_type="history.delete",
                status="ok",
                command="/history delete",
                details={"scope": "all", **counts},
            )
            return

        missing = [rid for rid in ids if hs.get_run(rid) is None]
        if missing:
            error(f"Run id(s) not found: {', '.join(str(m) for m in missing)}.")
            return

        label = ", ".join(str(i) for i in ids)
        if not yes and not confirm(
            f"Delete run(s) {label} and their results? This cannot be undone.",
            default=False,
        ):
            warn("Cancelled.")
            return
        counts = hs.delete_run(ids[0]) if len(ids) == 1 else hs.delete_runs(ids)
        success(f"Deleted {counts['runs']} run(s) and {counts['results']} result row(s).")
        log_event(
            event_type="history.delete",
            status="ok",
            command="/history delete",
            details={"ids": ids, **counts},
        )

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
                [
                    "avg_model_processing_sec",
                    f"{float(stats.get('avg_model_processing_sec') or 0):.2f}",
                ],
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
            error(
                f"No saved alternatives for run {run_id}. (Alternatives are only stored for runs made with v0.1.39+.)"
            )
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
                        + datetime.fromtimestamp(selected_at, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M"
                        )
                    )
                if applied_at:
                    lines.append(
                        "  [dim]Applied at:[/dim] "
                        + datetime.fromtimestamp(applied_at, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M"
                        )
                    )
                console.print(
                    Panel(
                        "\n".join(lines),
                        title=f"[heading]{kind} DESCRIPTION[/heading] - {asset_label}  [dim][{status_label}][/dim]",
                        border_style=accent_color(),
                        expand=False,
                    )
                )

        table_rows = []
        for row in column_rows:
            alternatives = row.get("alternatives_json") or []
            if alternatives:
                alternatives_str = "\n".join(
                    f"{index}. {alt}" for index, alt in enumerate(alternatives, 1)
                )
            else:
                alternatives_str = "-"
            evaluated_at = row.get("evaluated_at")
            applied_at = row.get("applied_at")
            table_rows.append(
                [
                    row.get("id", ""),
                    row.get("table_name", ""),
                    row.get("column_name") or "(table)",
                    row.get("confidence", ""),
                    f"{float(row.get('logprob_score')):.4f}"
                    if row.get("logprob_score") is not None
                    else "N/A",
                    alternatives_str,
                    row.get("evaluation") or "pending",
                    (row.get("chosen_description") or "")[:40],
                    row.get("catalog_status") or "-",
                    row.get("effective_source_kind") or "-",
                    "yes" if row.get("catalog_indexed_at") else "no",
                    row.get("db_applied_status") or ("applied" if row.get("applied_at") else "-"),
                    datetime.fromtimestamp(evaluated_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    if evaluated_at
                    else "",
                    datetime.fromtimestamp(applied_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                    if applied_at
                    else "",
                ]
            )
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
                success(
                    f"No pending items for run #{run_id} - all alternatives have been evaluated."
                )
            else:
                error(f"No saved alternatives for run #{run_id}.")
            return

        heading(f"Re-evaluating alternatives - run #{run_id} ({len(rows)} item(s))")
        if unevaluated_only:
            info(
                f"Showing {len(rows)} unevaluated item(s) only (use without --unevaluated-only to review all)."
            )
        else:
            info(
                f"Showing all {len(rows)} item(s) - already-evaluated rows will ask if you want to change your choice."
            )

        def _mark_run_success() -> None:
            try:
                hs.update_run_status(run_id, "success")
            except Exception as exc:
                warn(f"Could not update run #{run_id} status to success: {exc}")

        rows_sorted = sorted(
            rows, key=lambda row: (0 if not row.get("column_name") else 1, row.get("id", 0))
        )

        results_to_review = []
        for row in rows_sorted:
            alternatives: list[str] = row.get("alternatives_json") or []
            if not alternatives:
                warn(
                    f"Row {row['id']} ({row['table_name']}.{row.get('column_name') or '(table)'}) has no alternatives stored - skipping."
                )
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
                    logprob_score=(
                        float(row["logprob_score"])
                        if row.get("logprob_score") is not None
                        else None
                    ),
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
                        inner_hs.record_applied(
                            result.result_id,
                            chosen_description=getattr(result, "final_description", None) or None,
                        )
                    if result.result_id is not None:
                        try:
                            from amx.search.catalog import SearchCatalog

                            catalog = SearchCatalog.from_history_store()
                            if catalog is not None:
                                catalog.mark_applied(result.result_id)
                        except Exception as exc:
                            warn(
                                f"Could not update /search apply state for result {result.result_id}: {exc}"
                            )

                def _on_failed(result: Any, exc: Exception) -> None:
                    inner_hs = history_store()
                    if result.result_id is not None and inner_hs is not None:
                        try:
                            inner_hs.record_db_apply_failure(result.result_id, str(exc))
                        except Exception as inner_exc:
                            warn(
                                f"Could not record failed DB apply state for result {result.result_id}: {inner_exc}"
                            )

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

    @history.command("rollback")
    @click.argument("run_id", type=int)
    @click.option(
        "--yes",
        "-y",
        "skip_confirm",
        is_flag=True,
        default=False,
        help="Skip the confirmation prompt (scripted use).",
    )
    @pass_config
    def history_rollback(cfg: AMXConfig, run_id: int, skip_confirm: bool) -> None:
        """Restore the COMMENTs that ``run_id`` overwrote.

        Replays ``apply_events`` rows for ``RUN_ID`` in **reverse**
        order, writing each row's ``old_comment`` back to the
        database via ``db.apply_comment``. Rows whose ``old_comment``
        is ``None`` (the audit row never captured the prior text —
        adapter without a read API, or the pre-write read failed)
        are skipped with a warning, **not** silently overwritten
        with garbage.

        DBA-written comments are restored verbatim because the audit
        log records "what was on the asset before the apply",
        independent of who originally wrote it.
        """
        from amx.db.connector import AssetKind, DatabaseConnector

        hs = history_store()
        if hs is None:
            error(
                "History store isn't initialized; nothing to roll back. "
                "Run `/setup` or `/history-store enable` first."
            )
            log_event(
                event_type="history_rollback",
                status="failed",
                command="history.rollback",
                details={"run_id": run_id, "reason": "no_history_store"},
            )
            return

        try:
            events = hs.list_apply_events(run_id=run_id, limit=10_000)
        except Exception as exc:
            error(f"Could not read apply_events for run #{run_id}: {exc}")
            log_event(
                event_type="history_rollback",
                status="failed",
                command="history.rollback",
                details={"run_id": run_id, "reason": "list_failed", "error": str(exc)},
            )
            return

        if not events:
            warn(
                f"No apply events recorded for run #{run_id}. "
                "Either the run never wrote anything, or it predates the audit log."
            )
            log_event(
                event_type="history_rollback",
                status="skipped",
                command="history.rollback",
                details={"run_id": run_id, "reason": "no_events"},
            )
            return

        restorable = [e for e in events if e.get("old_comment") is not None]
        skipped = [e for e in events if e.get("old_comment") is None]

        heading(f"Rollback run #{run_id}")
        info(
            f"Found {len(events)} apply event(s); "
            f"{len(restorable)} restorable, {len(skipped)} skipped (original unknown)."
        )
        if restorable:
            preview_rows = []
            for e in restorable[:10]:
                asset = ".".join(
                    p
                    for p in (e.get("schema_name"), e.get("table_name"), e.get("column_name"))
                    if p
                )
                preview_rows.append(
                    [
                        asset,
                        (e.get("new_comment") or "")[:48],
                        (e.get("old_comment") or "")[:48],
                    ]
                )
            render_table(
                "Will restore (sample)" if len(restorable) > 10 else "Will restore",
                ["Asset", "Current (will be replaced)", "Restoring to"],
                preview_rows,
            )

        if not restorable:
            warn(
                "Nothing to restore. All events have ``old_comment=None`` — "
                "the original text was never captured (apply ran before "
                "PR-12b2, or the adapter doesn't expose a read API)."
            )
            log_event(
                event_type="history_rollback",
                status="skipped",
                command="history.rollback",
                details={"run_id": run_id, "reason": "no_restorable_events"},
            )
            return

        if not skip_confirm and not confirm(
            f"Restore {len(restorable)} comment(s) by overwriting current values?",
            default=False,
        ):
            info("Cancelled.")
            log_event(
                event_type="history_rollback",
                status="cancelled",
                command="history.rollback",
                details={"run_id": run_id, "restorable": len(restorable)},
            )
            return

        db = DatabaseConnector(cfg.db)
        if not db.test_connection():
            error("Cannot connect to database.")
            log_event(
                event_type="history_rollback",
                status="failed",
                command="history.rollback",
                details={"run_id": run_id, "reason": "db_connect_failed"},
            )
            return

        # Apply rollbacks in reverse application order so a series of
        # writes to the same asset in one run unwinds in the right
        # direction (last-write-wins forward → first-write-wins back).
        ordered = sorted(
            restorable,
            key=lambda e: e.get("applied_at") or 0,
            reverse=True,
        )

        restored = 0
        failed: list[tuple[str, str]] = []
        with db.engine.begin() as conn:
            for event in ordered:
                schema = event.get("schema_name") or ""
                table = event.get("table_name") or ""
                column = event.get("column_name")
                kind_label = (event.get("asset_kind") or "table").lower()
                try:
                    kind = AssetKind(kind_label)
                except ValueError:
                    kind = AssetKind.TABLE
                asset_path = ".".join(p for p in (schema, table, column) if p)
                try:
                    db.apply_comment(
                        schema=schema,
                        table=table,
                        column=column,
                        comment=event.get("old_comment") or "",
                        asset_kind=kind,
                        conn=conn,
                    )
                    restored += 1
                    info(f"  ✓ {asset_path}")
                except Exception as exc:
                    failed.append((asset_path, str(exc)))
                    warn(f"  ✗ {asset_path}: {exc}")

        if failed:
            warn(f"Restored {restored} of {len(ordered)}; {len(failed)} failed.")
        else:
            success(f"Restored {restored} comment(s) from run #{run_id}.")

        log_event(
            event_type="history_rollback",
            status="success" if not failed else "partial",
            command="history.rollback",
            details={
                "run_id": run_id,
                "restored": restored,
                "skipped": len(skipped),
                "failed": len(failed),
            },
        )

    @history.command("status")
    def history_status() -> None:
        """Show shared-history connection state and pending outbox count.

        Prints the active shared profile, schema name, number of queued
        outbox rows, and the current ``_amx_backfill_state`` sentinels so
        operators can confirm whether the initial backfill has completed.
        """
        hs = history_store()
        if hs is None:
            error("History store is not initialized.")
            return

        shared = getattr(hs, "shared", None)
        profile = getattr(getattr(hs, "_cfg", None), "history_store_profile", None) or "—"
        schema = getattr(getattr(hs, "_cfg", None), "history_store_schema", None) or "AMX"

        pending = 0
        if callable(getattr(hs, "pending_count", None)):
            pending = hs.pending_count()

        info(f"Shared profile : {profile}")
        info(f"Shared schema  : {schema}")
        info(f"Shared store   : {'connected' if shared is not None else 'local-only'}")
        info(f"Pending outbox : {pending} row(s)")

        # Read backfill sentinels from the local SQLite store.
        local = getattr(hs, "local", None) or getattr(hs, "_local", None)
        if local is None:
            return
        try:
            with local._connect() as conn:
                rows = conn.execute(
                    "SELECT scope, shared_profile, shared_schema, completed_at, "
                    "rows_pushed, last_error FROM _amx_backfill_state"
                ).fetchall()
            if not rows:
                info("Backfill state : no sentinels (backfill has not run yet).")
            else:
                render_table(
                    "Backfill sentinels",
                    ["Scope", "Profile", "Schema", "Completed at", "Rows pushed", "Error"],
                    [
                        [
                            str(r[0]),
                            str(r[1]),
                            str(r[2]),
                            f"{float(r[3]):.0f}" if r[3] else "—",
                            str(r[4]),
                            str(r[5]) if r[5] else "—",
                        ]
                        for r in rows
                    ],
                )
        except Exception as exc:
            info(f"Backfill state : could not read sentinels ({exc})")

    @history.command("sync-local")
    @pass_config
    def history_sync_local(cfg: AMXConfig) -> None:
        """Push local lineage and pages rows to the shared warehouse.

        Runs the BackfillRunner synchronously so you can see progress in the
        terminal. Idempotent: rows already present in the shared store are
        skipped. Use ``/history status`` afterwards to confirm completion.
        """
        from amx.storage.backfill import BackfillRunner
        from amx.storage.factory import history_store as _hs_factory

        hs = _hs_factory()
        if hs is None:
            error("History store is not initialized.")
            return

        shared = getattr(hs, "shared", None)
        if shared is None:
            warn(
                "No shared store is connected. Enable shared mode with "
                "``/history-store enable`` first."
            )
            return

        local = getattr(hs, "local", None) or getattr(hs, "_local", None)
        if local is None:
            error("Could not resolve local SQLite store.")
            return

        shared_profile = str(getattr(cfg, "history_store_profile", "") or "")
        shared_schema = str(getattr(cfg, "history_store_schema", "") or "AMX")

        info(f"Starting backfill to profile={shared_profile!r}, schema={shared_schema!r} …")

        def _progress(table: str, done: int, total: int) -> None:
            info(f"  {table}: {done}/{total}")

        runner = BackfillRunner(
            local,
            shared,
            shared_profile=shared_profile,
            shared_schema=shared_schema,
            progress_cb=_progress,
        )
        report = runner.run()

        render_table(
            "Backfill report",
            ["Metric", "Value"],
            [
                ["succeeded", report.succeeded],
                ["skipped", report.skipped],
                ["failed", report.failed],
                ["last_error", report.last_error or "—"],
                *[[f"  {tbl}", cnt] for tbl, cnt in sorted(report.per_table_counts.items())],
            ],
        )

        if report.last_error:
            warn(f"Backfill finished with error: {report.last_error}")
        else:
            success("Backfill complete.")

    return history
