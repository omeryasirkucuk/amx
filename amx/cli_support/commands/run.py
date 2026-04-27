"""Analyze namespace helpers and apply flow for the AMX interactive CLI."""

from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import ask_choice, ask_multi_choice, confirm, error, heading, info, render_table, success, warn

LogEvent = Callable[..., None]


def _is_non_business_asset(name: str) -> bool:
    """Return True for telemetry/system assets that should not be described by AMX."""
    n = (name or "").strip().lower()
    if not n:
        return False
    blocked_exact = {
        "pg_stat_statements",
        "pg_stat_statements_info",
    }
    blocked_prefixes = (
        "pg_stat_",
        "pg_statio_",
    )
    return n in blocked_exact or n.startswith(blocked_prefixes)


def _filter_non_business_assets(scope: dict[str, list[str]]) -> dict[str, list[str]]:
    """Drop telemetry/system assets from analysis scope and warn once per schema."""
    out: dict[str, list[str]] = {}
    for schema_name, names in scope.items():
        keep = [n for n in names if not _is_non_business_asset(n)]
        dropped = [n for n in names if _is_non_business_asset(n)]
        if dropped:
            warn(
                f"Skipping non-business/system assets in {schema_name}: "
                + ", ".join(sorted(set(dropped)))
            )
        if keep:
            out[schema_name] = keep
    return out


def _validate_assets_in_schema(db: object, schema: str, names: list[str]) -> list[str]:
    """Map user input to real asset names (case-insensitive). Raise ValueError if any name is unknown."""
    from difflib import get_close_matches

    if not names:
        raise ValueError("No assets selected.")
    available = [asset[0] for asset in db.list_assets(schema)]
    available_set = set(available)
    by_lower = {asset.lower(): asset for asset in available}
    resolved: list[str] = []
    missing: list[str] = []
    for name in names:
        if name in available_set:
            resolved.append(name)
        elif name.lower() in by_lower:
            resolved.append(by_lower[name.lower()])
        else:
            missing.append(name)
    if not missing:
        return resolved
    parts: list[str] = []
    for name in missing:
        close = get_close_matches(name, available, n=5, cutoff=0.35)
        parts.append(f"{name!r}" + (f" (similar: {close})" if close else ""))
    raise ValueError(f"Unknown asset(s) in schema {schema!r}: " + ", ".join(parts))


def _finalize_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> dict[str, list[str]] | None:
    """Resolve interactive or CLI scope and validate asset names against the database."""
    scope = _resolve_run_scope(cfg, db, schema, table_args)
    if not scope:
        error(
            "No assets selected. Use numbers from the list, exact names, "
            "comma-separated lists, or `all`. Enter alone cancels."
        )
        return None

    validated: dict[str, list[str]] = {}
    for schema_name, names in scope.items():
        if not names:
            continue
        try:
            validated[schema_name] = _validate_assets_in_schema(db, schema_name, names)
        except ValueError as exc:
            error(str(exc))
            return None

    if not validated:
        error("No valid assets to analyze.")
        return None
    filtered = _filter_non_business_assets(validated)
    if not filtered:
        error("No business assets to analyze after filtering system/telemetry objects.")
        return None
    return filtered


def _resolve_run_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> dict[str, list[str]]:
    """Three-level scope resolution: database -> schema -> asset."""
    if schema is not None or table_args:
        if not schema:
            schemas = db.list_schemas()
            schema = ask_choice("Select schema to analyze", schemas)
        assets = list(table_args)
        if not assets:
            available = _asset_display_list(db, schema)
            assets = _pick_assets(available)
        return {schema: assets}

    scope_level = ask_choice(
        "Select analysis scope",
        ["Database", "Schema", "Asset", "Default"],
        default="Schema",
        descriptions={
            "Database": "All schemas, all assets (tables, views, materialized views)",
            "Schema": "Select schema(s), analyze all assets within",
            "Asset": "Select specific tables or views",
            "Default": "Use current /db context: schema and optional table",
        },
    )

    if scope_level == "Database":
        schemas = db.list_schemas()
        result: dict[str, list[str]] = {}
        for schema_name in schemas:
            names = [asset[0] for asset in db.list_assets(schema_name)]
            if names:
                result[schema_name] = names
        if not result:
            warn("No analyzable assets found in any schema.")
        return result

    if scope_level == "Schema":
        schemas = db.list_schemas()
        selected = schemas if len(schemas) == 1 else ask_multi_choice("Select schema(s) to analyze", schemas)
        result = {}
        for schema_name in selected:
            names = [asset[0] for asset in db.list_assets(schema_name)]
            if names:
                result[schema_name] = names
        return result

    if scope_level == "Default":
        if cfg.current_schema:
            if cfg.current_table:
                return {cfg.current_schema: [cfg.current_table]}
            return {cfg.current_schema: [asset[0] for asset in db.list_assets(cfg.current_schema)]}
        warn(
            "Default scope requires /db context. Set /schema (and optionally /table) first, "
            "or pick Schema/Asset scope."
        )
        return {}

    schemas = db.list_schemas()
    schema = ask_choice("Select schema", schemas)
    available = _asset_display_list(db, schema)
    chosen = _pick_assets(available)
    return {schema: chosen}


def _asset_display_list(db: object, schema: str) -> list[str]:
    """Build display labels for interactive selection: ``name  [kind]``."""
    from amx.db.connector import AssetKind

    assets = [
        (name, kind)
        for name, kind in db.list_assets(schema)
        if not _is_non_business_asset(name)
    ]
    lines: list[str] = []
    for name, kind in assets:
        tag = "" if kind == AssetKind.TABLE else f"  [{kind.label}]"
        lines.append(f"{name}{tag}")
    return lines


def _pick_assets(display_list: list[str]) -> list[str]:
    """Interactive multi-choice that strips display tags before returning bare names."""
    chosen = ask_multi_choice("Select asset(s) to analyze", display_list)
    return [choice.split("  [")[0].strip() for choice in chosen]


def _resolve_codebase_for_run(
    cfg: AMXConfig,
    db: object,
    scope: dict[str, list[str]],
    code_profile: str | None,
    code_refresh: bool,
) -> object | None:
    """Load or build codebase report for /run and /run-apply."""
    from amx.codebase.analyzer import analyze_codebase, merge_codebase_reports
    from amx.codebase.cache import invalidate_cache, load_cached_report, save_cached_report
    from amx.codebase.code_rag import delete_code_collection
    from amx.config import DISABLED_PROFILE
    from amx.utils.console import info as show_info
    from amx.utils.console import step_spinner

    cp_name = (code_profile or "").strip() or None
    if cp_name == DISABLED_PROFILE:
        cp_name = None
    if cp_name:
        if cp_name not in cfg.code_profiles:
            error(f"Unknown codebase profile: {cp_name}")
            sys.exit(1)
        code_paths = [cfg.code_profiles[cp_name]]
        profile_name = cp_name
    else:
        code_paths = cfg.effective_code_paths()
        profile_name = (cfg.active_code_profile or "default").strip() or "default"

    if not code_paths:
        return None

    if code_refresh:
        delete_code_collection(source_filters=code_paths)

    all_tables: list[str] = []
    column_names: list[str] = []
    seen_columns: set[str] = set()
    all_assets_flat = [(schema_name, table_name) for schema_name, tables in scope.items() for table_name in tables]
    total_assets = sum(len(tables) for tables in scope.values())

    with step_spinner(f"Collecting column names from {total_assets} asset(s)"):
        for schema_name, table_name in all_assets_flat:
            for column in db.list_column_profiles(schema_name, table_name):
                key = column.name.lower()
                if key not in seen_columns:
                    seen_columns.add(key)
                    column_names.append(column.name)
                if len(column_names) >= 400:
                    break
            if len(column_names) >= 400:
                break

    catalog_set: set[str] = set()
    for schema_name in scope:
        for name, _kind in db.list_assets(schema_name):
            all_tables.append(name)
            catalog_set.add(name.lower())
    catalog = frozenset(catalog_set)

    first_schema = next(iter(scope))
    tables_flat = [table_name for tables in scope.values() for table_name in tables]

    show_info("Analyzing codebase references...")
    merged_report = None
    for code_path in code_paths:
        if code_refresh:
            invalidate_cache(profile_name, code_path)
        try:
            cached = None
            if not code_refresh:
                cached = load_cached_report(
                    profile_name=profile_name,
                    source_path=code_path,
                    schema=first_schema,
                    tables=tables_flat,
                    column_names=column_names,
                    force_refresh=False,
                )
            if cached is not None:
                report = cached
                show_info(f"Loaded cached codebase scan for {code_path}")
            else:
                with step_spinner(f"Scanning codebase: {code_path}"):
                    report = analyze_codebase(
                        code_path,
                        all_tables,
                        column_names=column_names,
                        known_catalog_tables=catalog,
                        index_semantic=True,
                    )
                show_info(
                    f"Found {sum(len(v) for v in report.references.values())} code references "
                    f"({sum(len(v) for v in report.external_mentions.values())} external-style)"
                )
                try:
                    save_cached_report(
                        profile_name=profile_name,
                        source_path=code_path,
                        schema=first_schema,
                        tables=tables_flat,
                        column_names=column_names,
                        report=report,
                    )
                except Exception as exc:
                    warn(f"Could not save codebase cache: {exc}")
            merged_report = merge_codebase_reports(merged_report, report)
        except Exception as exc:
            warn(f"Codebase analysis failed for {code_path}: {exc}")
    return merged_report


def register_analyze_commands(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> click.Group:
    """Attach `/analyze` namespace commands to the main Click group."""

    @main.group()
    def analyze() -> None:
        """Run metadata inference agents."""

    @analyze.command("apply")
    @pass_config
    def analyze_apply(cfg: AMXConfig) -> None:
        """Write pending approved descriptions to the database (COMMENT ON TABLE/COLUMN)."""
        from amx.agents.orchestrator import apply_review_results_to_db
        from amx.db.connector import DatabaseConnector
        from amx.pending_review import clear_pending, load_pending

        pending = load_pending()
        if not pending:
            log_event(
                event_type="analyze_apply",
                status="skipped",
                command="analyze.apply",
                details={"reason": "no_pending"},
            )
            error(
                "No pending metadata. Run `/analyze` then `/run`, approve descriptions, "
                "and finish without `--apply` first."
            )
            return

        heading("Apply pending metadata to the database")
        render_table(
            "Pending comments",
            ["Asset", "Description"],
            [
                [
                    f"{row.table}.{row.column}" if row.column else row.table,
                    (row.final_description or "")[:72],
                ]
                for row in pending
            ],
        )
        if not confirm(f"Write {len(pending)} comment(s) to the database?", default=True):
            log_event(
                event_type="analyze_apply",
                status="cancelled",
                command="analyze.apply",
                details={"pending_count": len(pending)},
            )
            info("Cancelled - pending file unchanged.")
            return

        db = DatabaseConnector(cfg.db)
        if not db.test_connection():
            log_event(
                event_type="analyze_apply",
                status="failed",
                command="analyze.apply",
                details={"reason": "db_connect_failed"},
            )
            error("Cannot connect to database.")
            sys.exit(1)

        def _on_applied(result: Any) -> None:
            hs = history_store()
            if result.result_id is not None and hs is not None:
                try:
                    hs.record_applied(result.result_id)
                except Exception:
                    pass

        applied_count = apply_review_results_to_db(db, pending, on_applied=_on_applied)
        clear_pending()
        success(f"Applied {applied_count} comment(s). Pending file cleared.")
        log_event(
            event_type="analyze_apply",
            status="success",
            command="analyze.apply",
            details={"applied_count": applied_count},
        )

    return analyze
