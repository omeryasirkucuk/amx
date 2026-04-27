"""Search namespace commands for the AMX interactive CLI."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.db.connector import DatabaseConnector, ProfilingError
from amx.search.catalog import SearchCatalog
from amx.search.service import SearchService
from amx.utils.console import error, info, render_table, success, warn

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
    if "left_column" in first:
        render_table(
            "Join candidates",
            ["Left column", "Right column", "Type", "Score", "Source"],
            [
                [
                    row.get("left_column", ""),
                    row.get("right_column", ""),
                    row.get("relationship_type", ""),
                    f"{float(row.get('score') or 0):.2f}",
                    row.get("source", ""),
                ]
                for row in rows
            ],
        )
        return
    render_table(
        "Search matches",
        ["Schema", "Table", "Column", "Source", "Conf", "Score", "Description"],
        [
            [
                row.get("schema_name", ""),
                row.get("table_name", ""),
                row.get("column_name", "") or "-",
                row.get("effective_source_kind", ""),
                row.get("current_confidence", ""),
                f"{float(row.get('rank_score') or 0):.2f}",
                str(row.get("search_text", "")).split("effective_description=", 1)[-1][:80],
            ]
            for row in rows
        ],
    )


def _sync_db_scope(
    cfg: AMXConfig,
    catalog: SearchCatalog,
    *,
    schema: str | None = None,
    table: str | None = None,
) -> tuple[int, int]:
    db = DatabaseConnector(cfg.db)
    db_profile = cfg.active_db_profile or "default"
    database_name = cfg.db.database or cfg.db.catalog or cfg.db.project or ""
    schemas = [schema] if schema else ([cfg.current_schema] if cfg.current_schema else db.list_schemas())
    inserted = 0
    updated = 0
    for schema_name in schemas:
        if not schema_name:
            continue
        assets = [(table, db.resolve_asset_kind(schema_name, table))] if table else db.list_assets(schema_name)
        for asset_name, asset_kind in assets:
            try:
                profile = db.profile_table(schema_name, asset_name, sample_size=0, asset_kind=asset_kind)
            except ProfilingError as exc:
                warn(str(exc))
                continue
            catalog.sync_table_profile(
                db_profile=db_profile,
                db_backend=cfg.db.backend,
                database_name=database_name,
                profile=profile,
                query_usage={},
            )
            updated += 1
    return inserted, updated


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
        """Search catalog, metadata, and code evidence."""
        if ctx.invoked_subcommand is None:
            info(
                "Use /search ask for natural-language questions, /status for catalog health, "
                "/sync or /rebuild to refresh derived search state."
            )

    @search.command("ask")
    @click.argument("question")
    @pass_config
    def search_ask(cfg: AMXConfig, question: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        answer = svc.ask(question)
        info(answer.summary)
        info(f"Confidence: {answer.confidence}")
        if answer.provenance:
            info("Provenance: " + "; ".join(answer.provenance))
        _render_search_rows(answer.rows)

    @search.command("find-columns")
    @click.argument("question")
    @pass_config
    def search_find_columns(cfg: AMXConfig, question: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        answer = svc.ask(question)
        _render_search_rows(answer.rows)

    @search.command("join-candidates")
    @click.argument("left_path")
    @click.argument("right_path")
    @pass_config
    def search_join_candidates(cfg: AMXConfig, left_path: str, right_path: str) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        rows = catalog.join_candidates(cfg.active_db_profile or "default", left_path, right_path)
        if rows:
            info(f"Top join candidates between {left_path} and {right_path}")
        else:
            warn(f"No join candidates found between {left_path} and {right_path}.")
        _render_search_rows(rows)

    @search.command("explain")
    @click.argument("question")
    @pass_config
    def search_explain(cfg: AMXConfig, question: str) -> None:
        svc = _service(cfg)
        if svc is None:
            return
        payload = svc.explain(question)
        info(f"Intent: {payload['intent']}")
        info(f"Confidence: {payload['confidence']}")
        info("Provenance: " + "; ".join(payload.get("provenance") or []))
        _render_search_rows(payload.get("rows") or [])
        info(json.dumps(payload.get("details") or {}, ensure_ascii=True))

    @search.command("explain-table")
    @click.argument("table_path")
    @pass_config
    def search_explain_table(cfg: AMXConfig, table_path: str) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        result = catalog.explain_table(cfg.active_db_profile or "default", table_path)
        if result is None:
            error(f"No catalog entry for {table_path}. Run /search sync first.")
            return
        table = result["table"]
        info(
            f"{table.get('schema_name')}.{table.get('table_name')} "
            f"[{table.get('asset_kind')}] source={table.get('effective_source_kind')} "
            f"confidence={table.get('current_confidence')}"
        )
        info("Columns:")
        render_table(
            f"Catalog table: {table_path}",
            ["Column", "Type", "Nullable", "PK", "FK", "Source"],
            [
                [
                    row.get("column_name", ""),
                    row.get("dtype", ""),
                    "yes" if row.get("nullable") else "no",
                    "yes" if row.get("pk_flag") else "no",
                    "yes" if row.get("fk_flag") else "no",
                    row.get("effective_source_kind", ""),
                ]
                for row in result["columns"]
            ],
        )
        if result["relationships"]:
            render_table(
                "Relationships",
                ["Type", "Target schema", "Target table", "Score", "Source"],
                [
                    [
                        row.get("relationship_type", ""),
                        row.get("target_schema", ""),
                        row.get("target_table", ""),
                        f"{float(row.get('score') or 0):.2f}",
                        row.get("source", ""),
                    ]
                    for row in result["relationships"]
                ],
            )

    @search.command("status")
    @pass_config
    def search_status(cfg: AMXConfig) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        status = catalog.sync_status(cfg.active_db_profile or "default")
        rows = [
            ["entities.total", status["entities"].get("total_entities", 0)],
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

    @search.command("sync")
    @click.option("--schema", "schema_name", default=None, help="Limit sync to one schema.")
    @click.option("--table", "table_name", default=None, help="Limit sync to one table in the selected schema.")
    @pass_config
    def search_sync(cfg: AMXConfig, schema_name: str | None, table_name: str | None) -> None:
        catalog = _catalog()
        if catalog is None:
            error("Search catalog is not initialized.")
            return
        db_profile = cfg.active_db_profile or "default"
        job_id = catalog.start_sync_job(db_profile, "sync", {"schema": schema_name or "", "table": table_name or ""})
        inserted = 0
        updated = 0
        try:
            inserted, updated = _sync_db_scope(cfg, catalog, schema=schema_name, table=table_name)
            try:
                from amx.codebase.cache import load_latest_cached_report

                code_path = cfg.resolve_code_path(cfg.active_code_profile or None, None)
                if code_path:
                    profile_nm = (cfg.active_code_profile or "default").strip() or "default"
                    manifest, report = load_latest_cached_report(profile_nm, code_path)
                    if report is not None and manifest is not None:
                        catalog.sync_code_report(
                            db_profile=db_profile,
                            db_backend=cfg.db.backend,
                            database_name=cfg.db.database or cfg.db.catalog or cfg.db.project or "",
                            schema_name=str(manifest.get("schema") or schema_name or cfg.current_schema or ""),
                            source_path=code_path,
                            report=report,
                        )
            except Exception as exc:
                warn(f"Code evidence sync skipped: {exc}")
            catalog.finish_sync_job(job_id, status="success", inserted_count=inserted, updated_count=updated)
            success(f"Search sync complete. inserted={inserted}, updated={updated}")
            log_event(event_type="search_sync", status="success", command="search.sync", details={"schema": schema_name, "table": table_name, "updated": updated})
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
        inserted, updated = catalog.rebuild_profile(cfg.active_db_profile or "default")
        success(f"Search rebuild complete. inserted={inserted}, updated={updated}")
        log_event(
            event_type="search_rebuild",
            status="success",
            command="search.rebuild",
            details={"inserted": inserted, "updated": updated, "db_profile": cfg.active_db_profile or "default"},
        )
