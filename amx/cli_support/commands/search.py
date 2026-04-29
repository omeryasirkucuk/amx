"""Search namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import click
from rich import box
from rich.table import Table
from rich.text import Text

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search.catalog import SearchCatalog
from amx.search.service import SearchService
from amx.services.analyze_scope import finalize_scope as _finalize_scope
from amx.storage.sqlite_store import history_store
from amx.utils.console import ask_choice, ask_multi_choice, confirm, console, error, info, render_table, success, warn
from amx.utils.live_commands import command_display
from amx.utils.live_display import get_display

LogEvent = Callable[..., None]


def _catalog() -> SearchCatalog | None:
    return SearchCatalog.from_history_store()


def _service(cfg: AMXConfig) -> SearchService | None:
    catalog = _catalog()
    if catalog is None:
        error("Search catalog is not initialized.")
        return None
    return SearchService(cfg, catalog)


def _render_search_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        info("No results.")
        return
    first = rows[0]
    if first.get("row_type") == "joinable_table" or "target_table_name" in first:
        render_table(
            "Joinable tables",
            ["Base table", "Target schema", "Target table", "Base columns", "Target columns", "Type", "Band", "Score", "Source"],
            [
                [
                    f"{row.get('schema_name', '')}.{row.get('table_name', '')}",
                    row.get("target_schema_name", ""),
                    row.get("target_table_name", ""),
                    row.get("left_column", ""),
                    row.get("right_column", ""),
                    row.get("relationship_type", ""),
                    row.get("confidence_band", ""),
                    f"{float(row.get('score') or 0):.2f}",
                    row.get("source", ""),
                ]
                for row in rows
            ],
        )
        return
    if "left_column" in first:
        render_table(
            "Join candidates",
            ["Left column", "Right column", "Type", "Band", "Score", "Source"],
            [
                [
                    row.get("left_column", ""),
                    row.get("right_column", ""),
                    row.get("relationship_type", ""),
                    row.get("confidence_band", ""),
                    f"{float(row.get('score') or 0):.2f}",
                    row.get("source", ""),
                ]
                for row in rows
            ],
        )
        return
    table = Table(title="Search matches", show_lines=True, box=box.SIMPLE_HEAVY)
    table.add_column("Schema", style="cyan", no_wrap=True)
    table.add_column("Table", style="cyan", no_wrap=True)
    table.add_column("Column", style="cyan", no_wrap=True)
    table.add_column("Source", style="cyan", no_wrap=True)
    table.add_column("Conf", style="cyan", no_wrap=True)
    table.add_column("Score", style="cyan", no_wrap=True, justify="right")
    table.add_column("Description", style="white", overflow="fold", max_width=72)
    for row in rows:
        desc = str(row.get("effective_description", "") or "")
        table.add_row(
            str(row.get("schema_name", "") or ""),
            str(row.get("table_name", "") or ""),
            str(row.get("column_name", "") or "-"),
            str(row.get("effective_source_kind", "") or ""),
            str(row.get("current_confidence", "") or ""),
            f"{float(row.get('rank_score') or row.get('score') or 0):.2f}",
            Text(desc),
        )
    console.print(table)


def _search_scope_from_answer(answer: Any) -> dict[str, list[str]]:
    scope = answer.details.get("scope") or {}
    if isinstance(scope, dict) and scope:
        out: dict[str, list[str]] = {}
        for key, values in scope.items():
            if not key or not isinstance(values, list):
                continue
            uniq = [str(value) for value in values if str(value)]
            if uniq:
                out[str(key)] = uniq
        if out:
            return out
    rows = answer.rows or []
    grouped: dict[str, list[str]] = {}
    for row in rows:
        schema_name = str(row.get("schema_name") or "")
        table_name = str(row.get("table_name") or "")
        if not schema_name or not table_name:
            continue
        grouped.setdefault(schema_name, [])
        if table_name not in grouped[schema_name]:
            grouped[schema_name].append(table_name)
    return grouped


def _search_results_payload(answer: Any) -> dict[str, Any]:
    return {
        "intent": answer.intent,
        "question_class": answer.details.get("question_class", ""),
        "question": answer.question,
        "confidence": answer.confidence,
        "summary": answer.summary,
        "provenance": answer.provenance,
        "retrieval": answer.details.get("retrieval", {}),
        "verification": answer.details.get("verification", {}),
        "policy": answer.details.get("policy", {}),
        "plan": answer.details.get("plan", {}),
        "actions": answer.details.get("actions", []),
        "action_results": answer.details.get("action_results", []),
        "ambiguity_flags": answer.details.get("ambiguity_flags", []),
        "evidence_sources": answer.details.get("evidence_sources", []),
        "stage_metrics": answer.details.get("stage_metrics", []),
        "thought_trace": answer.details.get("thought_trace", []),
        "reason": answer.details.get("reason", ""),
        "rows": [
            {
                "schema": row.get("schema_name", ""),
                "table": row.get("table_name", ""),
                "column": row.get("column_name", ""),
                "score": row.get("rank_score", row.get("score", 0)),
                "source": row.get("effective_source_kind", row.get("source", "")),
                "relationship_type": row.get("relationship_type", ""),
                "confidence_band": row.get("confidence_band", ""),
                "verified_live": bool(row.get("verified_live")),
            }
            for row in (answer.rows or [])[:10]
        ],
    }


def _answer_scope(answer: Any, cfg: AMXConfig) -> dict[str, list[str]]:
    scope = _search_scope_from_answer(answer)
    if scope:
        return scope
    if cfg.current_schema and cfg.current_table:
        return {cfg.current_schema: [cfg.current_table]}
    return {}


def _sync_cached_code_evidence(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    *,
    scope: dict[str, list[str]] | None = None,
) -> bool:
    try:
        from amx.codebase.cache import load_latest_cached_report
        from amx.utils.console import step_spinner

        code_path = cfg.resolve_code_path(cfg.active_code_profile or None, None)
        if not code_path:
            warn("No active code profile path is configured.")
            return False
        profile_nm = (cfg.active_code_profile or "default").strip() or "default"
        with step_spinner("Loading cached code evidence"):
            manifest, report = load_latest_cached_report(profile_nm, code_path)
        if report is None or manifest is None:
            warn("No cached code-scan report found. Run `/code scan` first.")
            return False
        schema_name = str(manifest.get("schema") or next(iter((scope or {}).keys()), cfg.current_schema or ""))
        with step_spinner("Refreshing /search code evidence"):
            catalog.sync_code_report(
                db_profile=cfg.active_db_profile or "default",
                db_backend=cfg.db.backend,
                database_name=cfg.db.database or cfg.db.catalog or cfg.db.project or "",
                schema_name=schema_name,
                source_path=code_path,
                report=report,
            )
        success("Refreshed `/search` code evidence from the latest cached code scan.")
        return True
    except Exception as exc:
        warn(f"Could not refresh code evidence: {exc}")
        return False


def _run_search_action(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    answer: Any,
    action_name: str,
) -> dict[str, Any]:
    scope = _answer_scope(answer, cfg)
    db_profile = cfg.active_db_profile or "default"
    if action_name == "sync_catalog":
        if not scope:
            cfg, scope = _interactive_sync_scope(cfg, cfg.current_schema or None, cfg.current_table or None)
        if not scope:
            return {"action": action_name, "status": "skipped", "reason": "no_scope"}
        job_id = catalog.start_sync_job(db_profile, "sync", {"scope": scope, "trigger": "search_action"})
        inserted = 0
        updated = 0
        try:
            with command_display(
                schema=next(iter(scope.keys()), ""),
                table=f"{sum(len(v) for v in scope.values())} assets",
                mode="search-sync",
                provider=cfg.llm.provider,
                model=cfg.llm.model,
            ):
                inserted, updated = _sync_db_scope(cfg, catalog, scope=scope)
                _sync_cached_code_evidence(cfg, catalog, scope=scope)
            catalog.finish_sync_job(job_id, status="success", inserted_count=inserted, updated_count=updated)
            success(f"Approved search action complete: sync_catalog inserted={inserted}, updated={updated}")
            return {"action": action_name, "status": "success", "inserted": inserted, "updated": updated, "scope": scope}
        except Exception as exc:
            catalog.finish_sync_job(job_id, status="failed", inserted_count=inserted, updated_count=updated, error_text=str(exc))
            warn(f"Approved search action failed: {exc}")
            return {"action": action_name, "status": "failed", "reason": str(exc), "scope": scope}
    if action_name == "refresh_code_evidence":
        with command_display(
            schema=next(iter(scope.keys()), "") if scope else "",
            mode="search-sync",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            ok = _sync_cached_code_evidence(cfg, catalog, scope=scope)
        return {"action": action_name, "status": "success" if ok else "skipped", "scope": scope}
    if action_name == "analyze_table":
        tables = answer.details.get("retrieval", {}).get("resolved_tables") or []
        if not tables and answer.rows:
            row = answer.rows[0]
            if row.get("schema_name") and row.get("table_name"):
                tables = [f"{row.get('schema_name')}.{row.get('table_name')}"]
        if not tables:
            return {"action": action_name, "status": "skipped", "reason": "no_resolved_table"}
        schema_name, table_name = str(tables[0]).split(".", 1)
        try:
            from amx.core.inference import infer_table_metadata

            results = infer_table_metadata(cfg, schema_name, table_name, include_rag=True, include_codebase=False)
            success(f"Approved search action complete: analyze_table produced {len(results)} suggestions for {schema_name}.{table_name}")
            return {"action": action_name, "status": "success", "table": tables[0], "suggestions": len(results)}
        except Exception as exc:
            warn(f"Approved search action failed: {exc}")
            return {"action": action_name, "status": "failed", "table": tables[0], "reason": str(exc)}
    info(f"Action `{action_name}` is advisory and has no automatic executor.")
    return {"action": action_name, "status": "advisory"}


def _run_approved_search_actions(cfg: AMXConfig, svc: SearchService, answer: Any) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    actions = answer.details.get("actions", []) or []
    if not actions:
        return records
    for action in actions:
        action_name = str((action or {}).get("action") or "").strip()
        if action_name not in {"sync_catalog", "refresh_code_evidence", "analyze_table"}:
            continue
        reason = str((action or {}).get("reason") or "").strip()
        prompt = f"Run search action `{action_name}`?"
        if reason:
            prompt += f" {reason}"
        try:
            approved = confirm(prompt, default=False)
        except (EOFError, KeyboardInterrupt):
            approved = False
        if not approved:
            records.append({"action": action_name, "status": "declined"})
            continue
        records.append(_run_search_action(cfg, svc.catalog, answer, action_name))
    return records


def _run_search_ask(
    cfg: AMXConfig,
    svc: SearchService,
    question_text: str,
    *,
    log_event: LogEvent,
    take_actions: bool = False,
) -> None:
    display = get_display()
    started_display = False
    if not display.is_active:
        display.start(
            schema=cfg.current_schema or "",
            table=cfg.current_table or "",
            mode="search",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        )
        started_display = True
    try:
        answer = svc.ask(question_text)
    finally:
        if started_display:
            display.stop()
    hs = history_store()
    run_id: int | None = None
    if hs is not None:
        run_id = hs.create_run(
            command="search.ask",
            mode="chat",
            db_backend=cfg.db.backend,
            db_profile=cfg.active_db_profile or "default",
            llm_provider=cfg.llm.provider,
            llm_model=cfg.llm.model,
            scope=_search_scope_from_answer(answer),
        )
    info(answer.summary)
    if answer.provenance and svc.settings.get("show_provenance", "true").lower() == "true":
        info("Provenance: " + "; ".join(answer.provenance))
    if svc.settings.get("show_confidence", "true").lower() == "true":
        info(f"Confidence: {answer.confidence}")
    trace = answer.details.get("thought_trace", []) or []
    if trace:
        info("Thought Trace:")
        for idx, step in enumerate(trace, start=1):
            if not isinstance(step, dict):
                continue
            label = str(step.get("step") or f"step_{idx}")
            observation = str(step.get("observation") or "")
            info(f"  {idx}. {label}: {observation}")
    for action in answer.details.get("actions", []) or []:
        action_name = str((action or {}).get("action") or "").strip()
        action_reason = str((action or {}).get("reason") or "").strip()
        if action_name:
            info(f"Suggested next step: {action_name}" + (f" — {action_reason}" if action_reason else ""))
    action_results: list[dict[str, Any]] = []
    if take_actions:
        action_results = _run_approved_search_actions(cfg, svc, answer)
        if action_results:
            answer.details["action_results"] = action_results
    if answer.rows and bool(answer.details.get("display_rows", True)):
        _render_search_rows(answer.rows)
    payload = _search_results_payload(answer)
    status = "success"
    error_text = ""
    if answer.details.get("reason") in {"no_llm", "llm_failure"}:
        status = "failed"
        error_text = answer.summary
    if hs is not None and run_id is not None:
        hs.finish_run(
            run_id,
            status=status,
            metrics=answer.details.get("llm_usage", {}),
            tokens=answer.details.get("tokens", {}),
            results=payload,
            error_text=error_text,
        )
    log_event(
        event_type="search_ask",
        status=status,
        command="search.ask",
        details={
            "question": question_text,
            "intent": answer.intent,
            "question_class": answer.details.get("question_class", ""),
            "confidence": answer.confidence,
            "reason": answer.details.get("reason", ""),
            "scope": _search_scope_from_answer(answer),
            "provenance": answer.provenance,
            "actions": answer.details.get("actions", []),
            "action_results": action_results,
            "evidence_sources": answer.details.get("evidence_sources", []),
            "ambiguity_flags": answer.details.get("ambiguity_flags", []),
            "stage_metrics": answer.details.get("stage_metrics", []),
        },
    )


def _sync_db_scope(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    *,
    scope: dict[str, list[str]],
) -> tuple[int, int]:
    db = DatabaseConnector(cfg.db)
    db_profile = cfg.active_db_profile or "default"
    database_name = cfg.db.database or cfg.db.catalog or cfg.db.project or ""
    inserted = 0
    updated = 0
    total_assets = sum(len(asset_names) for asset_names in scope.values())
    display = get_display()
    activity_idx: int | None = None
    if total_assets and display.is_active:
        activity_idx = display.add_activity(f"Search sync 0/{total_assets}")
        display.begin_activity(activity_idx)
    processed = 0
    failed = 0
    for schema_name, asset_names in scope.items():
        for asset_name in asset_names:
            processed += 1
            if activity_idx is not None:
                display.set_context(schema=schema_name, table=asset_name)
                display.update_activity(activity_idx, label=f"Search sync {processed}/{total_assets}: {schema_name}.{asset_name}")
            asset_kind = db.resolve_asset_kind(schema_name, asset_name)
            try:
                if activity_idx is None:
                    from amx.utils.console import step_spinner

                    with step_spinner(f"Profiling {schema_name}.{asset_name} for /search"):
                        profile = db.profile_table(schema_name, asset_name, sample_size=0, asset_kind=asset_kind)
                else:
                    profile = db.profile_table(schema_name, asset_name, sample_size=0, asset_kind=asset_kind)
            except ProfilingError as exc:
                failed += 1
                if activity_idx is not None:
                    display.add_detail(activity_idx, f"Skipped {schema_name}.{asset_name}: {str(exc)[:220]}")
                warn(str(exc))
                continue
            if activity_idx is None:
                from amx.utils.console import step_spinner

                with step_spinner(f"Writing {schema_name}.{asset_name} into search catalog"):
                    catalog.sync_table_profile(
                        db_profile=db_profile,
                        db_backend=cfg.db.backend,
                        database_name=database_name,
                        profile=profile,
                        query_usage={},
                    )
            else:
                catalog.sync_table_profile(
                    db_profile=db_profile,
                    db_backend=cfg.db.backend,
                    database_name=database_name,
                    profile=profile,
                    query_usage={},
                )
            updated += 1
    if activity_idx is not None:
        summary = f"Synced {updated}/{total_assets} asset(s)"
        if failed:
            summary += f"; skipped {failed}"
            display.fail_activity(activity_idx, summary)
        else:
            display.complete_activity(activity_idx, summary)
    return inserted, updated


def _interactive_sync_scope(
    cfg: AMXConfig,
    schema_name: str | None,
    table_name: str | None,
) -> tuple[AMXConfig, dict[str, list[str]] | None]:
    if not schema_name and not table_name and len(cfg.db_profiles) > 1:
        if not confirm(
            f"Continue with current DB profile '{cfg.active_db_profile or 'default'}'?",
            default=True,
        ):
            selected = ask_choice(
                "Select DB profile for /search sync",
                sorted(cfg.db_profiles.keys()),
                default=cfg.active_db_profile or sorted(cfg.db_profiles.keys())[0],
            )
            cfg.set_active_db_profile(selected)
            info(f"Active DB: [bold cyan]{selected}[/]")
    db = DatabaseConnector(cfg.db)
    scope = _finalize_scope(
        cfg,
        db,
        schema_name,
        [table_name] if table_name else [],
        ask_choice=ask_choice,
        ask_multi_choice=ask_multi_choice,
        error=error,
        warn=warn,
    )
    return cfg, scope


def register_search_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach `/search` namespace commands to the main Click group."""

    @main.group(invoke_without_command=True)
    @click.pass_context
    def search(ctx: click.Context) -> None:
        """Chat-first metadata discussion surface."""
        if ctx.invoked_subcommand is None:
            info(
                "Use `/search ask <question>` or just type a question inside the /search tab. "
                "Use `/status`, `/sync`, or `/rebuild` for catalog operations."
            )

    @search.command("ask")
    @click.option("--actions", "take_actions", is_flag=True, help="Prompt before running approved follow-up actions.")
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_ask(cfg: AMXConfig, take_actions: bool, question: tuple[str, ...]) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        question_text = " ".join(question).strip()
        if not question_text:
            error("Usage: /search ask <question>")
            return
        _run_search_ask(cfg, svc, question_text, log_event=log_event, take_actions=take_actions)

    @search.command("status")
    @pass_config
    def search_status(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        status = catalog.sync_status(cfg.active_db_profile or "default")
        llm_ready = "yes" if (cfg.llm.provider and cfg.llm.model) else "no"
        total_entities = int(status["entities"].get("total_entities", 0) or 0)
        rows = [
            ["qa.ready", "yes" if total_entities > 0 else "no"],
            ["llm.ready", llm_ready],
            ["context.detail", status["settings"].get("context_detail", "standard")],
            ["verify.live_inventory", status["settings"].get("verify_live_inventory", "true")],
            ["semantic_join_inference", status["settings"].get("semantic_join_inference", "true")],
            ["entities.total", total_entities],
            ["entities.effective", status["entities"].get("effective_entities", 0)],
            ["descriptions.total", status["descriptions"].get("total_descriptions", 0)],
            ["descriptions.manual", status["descriptions"].get("manual_count", 0)],
            ["descriptions.reviewed", status["descriptions"].get("reviewed_count", 0)],
            ["descriptions.generated", status["descriptions"].get("generated_count", 0)],
            ["descriptions.rejected", status["descriptions"].get("rejected_count", 0)],
            ["last_synced_at", status["entities"].get("last_synced_at", 0)],
        ]
        render_table("Search status", ["Metric", "Value"], rows)
        if status["jobs"]:
            render_table(
                "Recent sync jobs",
                ["Type", "Status", "Inserted", "Updated", "Started", "Completed"],
                [
                    [
                        row.get("job_type", ""),
                        row.get("status", ""),
                        row.get("inserted_count", 0),
                        row.get("updated_count", 0),
                        f"{float(row.get('started_at') or 0):.0f}",
                        f"{float(row.get('completed_at') or 0):.0f}" if row.get("completed_at") else "",
                    ]
                    for row in status["jobs"]
                ],
            )

    @search.command("sources")
    @pass_config
    def search_sources(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        settings = catalog.get_settings(cfg.active_db_profile or "default")
        render_table(
            "Search source settings",
            ["Key", "Value"],
            [[key, value] for key, value in sorted(settings.items())],
        )
        rows = catalog.sources_status(cfg.active_db_profile or "default")
        if rows:
            render_table(
                "Search evidence sources",
                ["Source", "Evidence", "Rows", "Last seen"],
                [
                    [
                        row.get("source_kind", ""),
                        row.get("evidence_type", ""),
                        row.get("count_rows", 0),
                        f"{float(row.get('last_seen') or 0):.0f}",
                    ]
                    for row in rows
                ],
            )

    @search.command("config")
    @click.argument("key", required=False)
    @click.argument("value", required=False)
    @pass_config
    def search_config(cfg: AMXConfig, key: str | None, value: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"
        if key and value is not None:
            catalog.set_setting(db_profile, key, value)
            success(f"Updated search config for {db_profile}: {key}={value}")
            return
        settings = catalog.get_settings(db_profile)
        render_table(
            f"Search config: {db_profile}",
            ["Key", "Value"],
            [[name, val] for name, val in sorted(settings.items())],
        )

    @search.command("context-detail")
    @click.argument("level", required=False)
    @pass_config
    def search_context_detail(cfg: AMXConfig, level: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"
        if level:
            normalized = level.strip().lower()
            if normalized not in {"minimal", "standard", "rich", "deep"}:
                error("Context detail must be one of: minimal, standard, rich, deep.")
                return
            catalog.set_setting(db_profile, "context_detail", normalized)
            success(f"Updated search context detail for {db_profile}: {normalized}")
            return
        info(f"Current search context detail: {catalog.get_settings(db_profile).get('context_detail', 'standard')}")

    @search.command("sync")
    @click.option("--schema", "schema_name", default=None, help="Limit sync to one schema.")
    @click.option("--table", "table_name", default=None, help="Limit sync to one table in the selected schema.")
    @pass_config
    def search_sync(cfg: AMXConfig, schema_name: str | None, table_name: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        with command_display(
            schema=schema_name or cfg.current_schema or "",
            table=table_name or cfg.current_table or "",
            mode="search-sync",
            provider=cfg.llm.provider,
            model=cfg.llm.model,
        ):
            cfg, scope = _interactive_sync_scope(cfg, schema_name, table_name)
            if not scope:
                return
            db_profile = cfg.active_db_profile or "default"
            job_id = catalog.start_sync_job(db_profile, "sync", {"scope": scope})
            inserted = 0
            updated = 0
            try:
                inserted, updated = _sync_db_scope(cfg, catalog, scope=scope)
                _sync_cached_code_evidence(cfg, catalog, scope=scope)
                catalog.finish_sync_job(job_id, status="success", inserted_count=inserted, updated_count=updated)
                success(f"Search sync complete. inserted={inserted}, updated={updated}")
                log_event(event_type="search_sync", status="success", command="search.sync", details={"scope": scope, "updated": updated})
            except Exception as exc:
                catalog.finish_sync_job(job_id, status="failed", inserted_count=inserted, updated_count=updated, error_text=str(exc))
                log_event(event_type="search_sync", status="failed", command="search.sync", details={"error": str(exc)})
                raise

    @search.command("rebuild")
    @pass_config
    def search_rebuild(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        with command_display(mode="search-rebuild", provider=cfg.llm.provider, model=cfg.llm.model) as display:
            total_entities = int(catalog.sync_status(cfg.active_db_profile or "default")["entities"].get("total_entities", 0) or 0)
            activity_idx = display.add_activity(f"Search rebuild 0/{total_entities or '?'}")
            display.begin_activity(activity_idx)

            def _on_progress(index: int, total: int) -> None:
                display.update_activity(activity_idx, label=f"Search rebuild {index}/{total}")

            inserted, updated = catalog.rebuild_profile(cfg.active_db_profile or "default", on_progress=_on_progress)
            display.complete_activity(activity_idx, f"Rebuilt {updated} catalog entity rows")
        success(f"Search rebuild complete. inserted={inserted}, updated={updated}")
        log_event(
            event_type="search_rebuild",
            status="success",
            command="search.rebuild",
            details={"inserted": inserted, "updated": updated, "db_profile": cfg.active_db_profile or "default"},
        )

    @search.command("find-columns", hidden=True)
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_find_columns(cfg: AMXConfig, question: tuple[str, ...]) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        _run_search_ask(cfg, svc, " ".join(question).strip(), log_event=log_event)

    @search.command("join-candidates", hidden=True)
    @click.argument("left_path")
    @click.argument("right_path")
    @pass_config
    def search_join_candidates(cfg: AMXConfig, left_path: str, right_path: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        _run_search_ask(cfg, svc, f"Which columns should I join between {left_path} and {right_path}?", log_event=log_event)

    @search.command("explain", hidden=True)
    @click.argument("question", nargs=-1, required=True)
    @pass_config
    def search_explain(cfg: AMXConfig, question: tuple[str, ...]) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        _run_search_ask(cfg, svc, " ".join(question).strip(), log_event=log_event)

    @search.command("explain-table", hidden=True)
    @click.argument("table_path")
    @pass_config
    def search_explain_table(cfg: AMXConfig, table_path: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        _run_search_ask(cfg, svc, f"What does table {table_path} do?", log_event=log_event)
