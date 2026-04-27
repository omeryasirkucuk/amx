"""Analyze run command flow for the AMX interactive CLI."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask_choice,
    confirm,
    console,
    error,
    heading,
    info,
    render_table,
    render_token_summary,
    step_spinner,
    warn,
)
from amx.utils.token_tracker import tracker as token_tracker

FinalizeScope = Callable[[AMXConfig, object, str | None, list[str]], dict[str, list[str]] | None]
ResolveCodebaseForRun = Callable[[AMXConfig, object, dict[str, list[str]], str | None, bool], object | None]
LogEvent = Callable[..., None]


def _maybe_modify_profiles_before_run(cfg: AMXConfig, db: object, llm: object) -> tuple[object, object]:
    from amx.config import DISABLED_PROFILE
    from amx.db.connector import DatabaseConnector
    from amx.llm.provider import LLMProvider

    if not confirm("Do you want to modify profiles before run?", default=False):
        return db, llm

    db_names = list(cfg.db_profiles.keys())
    if db_names:
        db_choice = ask_choice("Select DB profile", db_names, default=cfg.active_db_profile)
        cfg.set_active_db_profile(db_choice)
        info(f"Active DB: [bold cyan]{db_choice}[/]")
        db = DatabaseConnector(cfg.db)
        with step_spinner("Testing new database connection..."):
            if not db.test_connection():
                error(f"Cannot connect to database using profile '{db_choice}'.")
                sys.exit(1)

    llm_names = list(cfg.llm_profiles.keys())
    if llm_names:
        llm_choice = ask_choice("Select LLM profile", llm_names, default=cfg.active_llm_profile)
        cfg.set_active_llm_profile(llm_choice)
        info(f"Active LLM: [bold cyan]{llm_choice}[/]")
        llm = LLMProvider(cfg.llm)

    doc_names = list(cfg.doc_profiles.keys())
    if doc_names:
        options = doc_names + [DISABLED_PROFILE]
        doc_choice = ask_choice(
            "Select Document profile",
            options,
            default=cfg.active_doc_profile or DISABLED_PROFILE,
        )
        cfg.active_doc_profile = doc_choice
        info(f"Active Docs: [bold cyan]{doc_choice}[/]")

    code_names = list(cfg.code_profiles.keys())
    if code_names:
        options = code_names + [DISABLED_PROFILE]
        code_choice = ask_choice(
            "Select Codebase profile",
            options,
            default=cfg.active_code_profile or DISABLED_PROFILE,
        )
        cfg.active_code_profile = code_choice
        info(f"Active Code: [bold cyan]{code_choice}[/]")

    cfg.save()
    info("Profile selections saved to config.yml.")
    console.print()
    return db, llm


def _resolve_completion_mode(cfg: AMXConfig, llm: object, mode: str | None) -> bool:
    from amx.llm.batch import supported_providers as batch_supported_providers
    from amx.utils.console import ask_choice as prompt_choice
    from rich.panel import Panel

    batch_capable = llm.supports_batch
    batch_providers_list = batch_supported_providers()

    if mode is None:
        cfg_mode = (cfg.llm.completion_mode or "chat_completions").lower()
        default_mode_label = "batch" if cfg_mode == "batch" else "chat"
        batch_note = (
            " (50 % cheaper, async)"
            if batch_capable
            else f" (requires {', '.join(batch_providers_list)})"
        )
        mode = prompt_choice(
            "Select completion mode",
            ["chat", "batch"],
            default=default_mode_label,
            descriptions={
                "chat": "Chat Completions — real-time, live spinners, full price",
                "batch": f"Batch API{batch_note} — submit all at once, results in minutes–hours",
            },
        )

    use_batch = mode == "batch"
    if use_batch and not batch_capable:
        warn(
            f"Provider '{cfg.llm.provider}' does not support batch mode. "
            f"Supported providers: {', '.join(batch_providers_list)}. "
            "Falling back to Chat Completions."
        )
        use_batch = False

    if use_batch:
        console.print(
            Panel(
                "[bold]Batch API selected.[/bold]\n"
                "All LLM requests will be submitted as a single batch job.\n"
                "Typical turnaround: [bold]2–30 minutes[/bold]  |  Cost: [bold green]~50 % lower[/bold green]\n"
                "[dim]Live polling status will appear below.[/dim]",
                title="[cyan]Mode: Batch[/cyan]",
                border_style="cyan",
            )
        )
    else:
        info("Mode: [bold]Chat Completions[/bold] (real-time)")
    return use_batch


def _finalize_history_run(
    *,
    run_id: int | None,
    final_status: str | None,
    run_started: float,
    total_assets: int,
    total_schemas: int,
    processed_assets: list[str],
    skipped_assets: list[str],
    approved: list[object],
    skipped: list[object],
    apply: bool,
    all_results: list[object],
    final_error_text: str,
) -> None:
    if run_id is None:
        return
    hs = history_store()
    if hs is None:
        return
    try:
        hs.finish_run(
            run_id,
            status=final_status or "success",
            metrics={
                "duration_sec": round(time.monotonic() - run_started, 3),
                "model_processing_sec": round(token_tracker.total_model_processing_sec, 3),
                "total_assets": total_assets,
                "total_schemas": total_schemas,
                "processed_assets_count": len(processed_assets),
                "processed_assets": processed_assets,
                "skipped_assets_count": len(skipped_assets),
                "skipped_assets": skipped_assets,
                "approved_count": len(approved),
                "skipped_count": len(skipped),
                "applied_flag": bool(apply),
            },
            tokens={
                "total_tokens": token_tracker.total_tokens,
                "summary": token_tracker.summary(),
                "records": token_tracker.records(),
            },
            results={
                "all_results": [
                    {
                        "schema": r.schema,
                        "table": r.table,
                        "column": r.column,
                        "description": r.final_description,
                        "confidence": r.confidence.value,
                        "logprob_score": r.logprob_score,
                        "source": r.source,
                        "asset_kind": r.asset_kind,
                        "applied": bool(r.applied),
                    }
                    for r in all_results
                ],
                "approved": [
                    {
                        "schema": r.schema,
                        "table": r.table,
                        "column": r.column,
                        "description": r.final_description,
                        "confidence": r.confidence.value,
                        "logprob_score": r.logprob_score,
                        "source": r.source,
                        "asset_kind": r.asset_kind,
                    }
                    for r in approved
                ],
                "skipped": [
                    {
                        "schema": r.schema,
                        "table": r.table,
                        "column": r.column,
                        "confidence": r.confidence.value,
                        "logprob_score": r.logprob_score,
                        "source": r.source,
                        "asset_kind": r.asset_kind,
                    }
                    for r in skipped
                ],
            },
            error_text=final_error_text,
        )
    except Exception as exc:
        warn(f"Could not persist run history finalization: {exc}")


def execute_analyze_run(
    cfg: AMXConfig,
    *,
    schema: str | None,
    table: tuple[str, ...],
    apply: bool,
    mode: str | None,
    tables_pos: tuple[str, ...],
    db: object,
    code_profile: str | None,
    code_refresh: bool,
    finalize_scope: FinalizeScope,
    resolve_codebase_for_run: ResolveCodebaseForRun,
    log_event: LogEvent,
) -> None:
    from amx.agents.orchestrator import Orchestrator
    from amx.config import DISABLED_PROFILE
    from amx.db.connector import DatabaseConnector, ProfilingError
    from amx.docs.rag import RAGStore
    from amx.llm.provider import LLMProvider

    use_batch = False
    all_results: list[object] = []
    run_id: int | None = None
    run_started = time.monotonic()
    total_assets = 0
    total_schemas = 0
    approved: list[object] = []
    skipped: list[object] = []
    processed_assets: list[str] = []
    skipped_assets: list[str] = []
    final_status: str | None = None
    final_error_text = ""

    try:
        token_tracker.reset()

        if not cfg.llm.provider or not cfg.llm.model:
            error("LLM not configured. Run `amx setup` first.")
            sys.exit(1)

        llm = LLMProvider(cfg.llm)

        if not apply:
            warn(
                "Without --apply, approved metadata is not written to the database. "
                "Use `/analyze` then `/apply`, or `/run-apply`, to persist comments."
            )

        db, llm = _maybe_modify_profiles_before_run(cfg, db, llm)
        use_batch = _resolve_completion_mode(cfg, llm, mode)

        tables_arg = list(tables_pos) + list(table)
        scope = finalize_scope(cfg, db, schema, tables_arg)
        if scope is None:
            return

        total_assets = sum(len(v) for v in scope.values())

        review_strategy = "individual"
        if not use_batch and total_assets > 1:
            review_strategy = ask_choice(
                "Review strategy",
                ["individual", "deferred"],
                default="individual",
                descriptions={
                    "individual": "Assess each asset (table) as it becomes ready",
                    "deferred": "Process everything first, then review all together at the end",
                },
            )

        hs = history_store()
        if hs is not None:
            try:
                run_id = hs.create_run(
                    command="analyze.run",
                    mode=("batch" if use_batch else "chat"),
                    db_backend=cfg.db.backend,
                    db_profile=cfg.active_db_profile,
                    llm_provider=cfg.llm.provider,
                    llm_model=cfg.llm.model,
                    scope=scope,
                )
            except Exception as exc:
                warn(f"History persistence disabled for this run: {exc}")

        total_schemas = len(scope)
        scope_summary = (
            f"{total_assets} asset(s) across {total_schemas} schema(s)"
            if total_schemas > 1
            else f"{total_assets} asset(s) in {next(iter(scope))}"
        )
        info(f"Scope: {scope_summary}")

        rag_store = None
        try:
            if cfg.active_doc_profile == DISABLED_PROFILE:
                info("RAG Agent disabled (document profile: none).")
            else:
                doc_filters = cfg.effective_doc_paths()
                store = RAGStore(source_filters=doc_filters)
                visible_chunks = store.doc_count
                if visible_chunks > 0:
                    rag_store = store
                    if doc_filters:
                        info(
                            f"RAG store has {visible_chunks} chunks available "
                            f"for active doc profile '{cfg.active_doc_profile or 'default'}'"
                        )
                    else:
                        info(f"RAG store has {visible_chunks} chunks available")
                elif doc_filters:
                    info(
                        f"RAG store has 0 chunks for active doc profile "
                        f"'{cfg.active_doc_profile or 'default'}'"
                    )
        except Exception:
            pass

        code_report = resolve_codebase_for_run(cfg, db, scope, code_profile, code_refresh)
        token_tracker.reset()

        from amx.utils.live_display import get_display

        display = get_display()
        for schema_name, assets in scope.items():
            asset_kinds = {name: db.resolve_asset_kind(schema_name, name) for name in assets}
            orch = Orchestrator(db, llm, rag_store=rag_store, code_report=code_report, run_id=run_id)

            display_label = ", ".join(assets) if len(assets) <= 3 else f"{len(assets)} assets"
            display.start(
                schema=schema_name,
                table=display_label,
                mode="batch" if use_batch else "chat",
                provider=cfg.llm.provider,
                model=cfg.llm.model,
            )
            try:
                if use_batch:
                    results = orch.process_tables_batch_mode(
                        schema_name,
                        list(assets),
                        asset_kinds=asset_kinds,
                    )
                    all_results.extend(results)
                    processed_assets.extend([f"{schema_name}.{asset_name}" for asset_name in assets])
                else:
                    for asset_name in assets:
                        display.set_context(table=asset_name)
                        try:
                            results = orch.process_table(
                                schema_name,
                                asset_name,
                                asset_kind=asset_kinds.get(asset_name),
                                interactive_review=(review_strategy == "individual"),
                            )
                            all_results.extend(results)
                            processed_assets.append(f"{schema_name}.{asset_name}")
                        except ProfilingError as exc:
                            skipped_assets.append(f"{schema_name}.{asset_name}")
                            warn(f"Skipping {schema_name}.{asset_name}: {exc}")
                            continue

                    if len(assets) > 1 or total_schemas > 1:
                        schema_meta = orch.process_schema_meta(schema_name, all_results)
                        all_results.extend(schema_meta)
            finally:
                display.stop()

        if total_schemas > 1:
            db_meta = orch.process_database_meta(all_results)
            all_results.extend(db_meta)

        all_results = orch.batch_review(all_results)

        if rag_store is None:
            token_tracker.drop_steps({"rag_agent", "rag_agent(batch)"})

        heading("Summary")
        render_token_summary(token_tracker)
        approved = [r for r in all_results if r.applied]
        skipped = [r for r in all_results if not r.applied]
        info(f"Approved: {len(approved)}  |  Skipped: {len(skipped)}")

        if approved:
            render_table(
                "Approved metadata",
                ["Asset", "Description", "Confidence", "Logprob", "Source"],
                [
                    [
                        f"{r.schema}.{r.table}.{r.column}"
                        if r.column
                        else (f"{r.schema}.{r.table}" if r.table else r.schema),
                        (r.final_description or "")[:60],
                        r.confidence.value,
                        f"{r.logprob_score:.4f}" if r.logprob_score is not None else "N/A",
                        r.source,
                    ]
                    for r in approved
                ],
            )

        if approved:
            from amx.pending_review import save_pending

            save_pending(approved)
            if not apply:
                info(
                    f"Saved {len(approved)} approved description(s) as pending. "
                    "Run `/analyze` then `/apply` (or `/run-apply` next time) to write them to the database."
                )

        if apply and approved and confirm("Apply these metadata comments to the database?"):
            from amx.pending_review import clear_pending

            orch.apply_results(approved)
            clear_pending()

        final_status = "success"
    except KeyboardInterrupt:
        approved = [r for r in all_results if getattr(r, "applied", False)]
        skipped = [r for r in all_results if not getattr(r, "applied", False)]
        if approved:
            try:
                from amx.pending_review import save_pending

                save_pending(approved)
            except Exception:
                pass

        has_reviewable_results = bool(all_results)
        hs = history_store()
        if not has_reviewable_results and run_id is not None and hs is not None:
            try:
                has_reviewable_results = bool(hs.get_run_results(run_id))
            except Exception:
                pass
        if not has_reviewable_results:
            has_reviewable_results = bool(token_tracker.total_tokens)

        final_status = "ready_for_review" if has_reviewable_results else "cancelled"
        final_error_text = "Interrupted by user"
        log_event(
            event_type="analyze_run",
            status=final_status,
            command="analyze.run",
            details={
                "mode": ("batch" if use_batch else "chat"),
                "error": "KeyboardInterrupt",
                "results_ready": has_reviewable_results,
            },
        )
        warn("User interrupted process.")
        return
    except Exception as exc:
        final_status = "failed"
        final_error_text = str(exc)
        log_event(
            event_type="analyze_run",
            status="failed",
            command="analyze.run",
            details={"error": str(exc), "mode": ("batch" if use_batch else "chat")},
        )
        raise
    finally:
        _finalize_history_run(
            run_id=run_id,
            final_status=final_status,
            run_started=run_started,
            total_assets=total_assets,
            total_schemas=total_schemas,
            processed_assets=processed_assets,
            skipped_assets=skipped_assets,
            approved=approved,
            skipped=skipped,
            apply=apply,
            all_results=all_results,
            final_error_text=final_error_text,
        )


def register_analyze_run_command(
    analyze: click.Group,
    *,
    finalize_scope: FinalizeScope,
    resolve_codebase_for_run: ResolveCodebaseForRun,
    log_event: LogEvent,
) -> None:
    """Attach `/analyze run` to an existing analyze group."""

    @analyze.command("run")
    @click.argument("tables_pos", nargs=-1, metavar="[ASSET ...]")
    @click.option("--schema", "-s", help="Schema to analyze.")
    @click.option("--table", "-t", multiple=True, help="Specific asset(s). Omit for interactive selection.")
    @click.option("--apply/--no-apply", default=False, help="Apply approved metadata to the database.")
    @click.option(
        "--code-refresh",
        is_flag=True,
        default=False,
        help="Invalidate codebase disk cache and rebuild semantic code index on this run.",
    )
    @click.option(
        "--code-profile",
        default=None,
        help="Use this named codebase profile path (otherwise active profile).",
    )
    @click.option(
        "--mode",
        type=click.Choice(["chat", "batch"], case_sensitive=False),
        default=None,
        help=(
            "Completion mode: 'chat' = Chat Completions (real-time, full price); "
            "'batch' = Batch API (async, ~50 %% cheaper)."
        ),
    )
    @click.pass_obj
    def analyze_run(
        cfg: AMXConfig,
        tables_pos: tuple[str, ...],
        schema: str | None,
        table: tuple[str, ...],
        apply: bool,
        code_refresh: bool,
        code_profile: str | None,
        mode: str | None,
    ) -> None:
        """Run all agents to infer metadata for selected assets (tables, views, etc.)."""
        from amx.db.connector import DatabaseConnector
        from amx.utils.live_display import get_display

        try:
            db_init = DatabaseConnector(cfg.db)
            display = get_display()
            display.start(
                schema=schema or cfg.current_schema or "",
                table=(table[0] if table else (tables_pos[0] if tables_pos else cfg.current_table or "")),
                mode="setup",
                provider=cfg.llm.provider,
                model=cfg.llm.model,
            )
            try:
                with step_spinner("Testing database connection..."):
                    if not db_init.test_connection():
                        error("Cannot connect to database.")
                        sys.exit(1)
            finally:
                display.stop()

            execute_analyze_run(
                cfg,
                schema=schema,
                table=table,
                apply=apply,
                mode=mode,
                tables_pos=tables_pos,
                db=db_init,
                code_profile=code_profile,
                code_refresh=code_refresh,
                finalize_scope=finalize_scope,
                resolve_codebase_for_run=resolve_codebase_for_run,
                log_event=log_event,
            )
        except KeyboardInterrupt:
            warn("User interrupted process.")
            return
        except Exception as exc:
            raise click.ClickException(str(exc))
