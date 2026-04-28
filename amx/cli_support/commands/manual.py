"""Database metadata editing and monitoring commands for AMX."""

from __future__ import annotations

from collections.abc import Callable

import click

from amx.config import AMXConfig, DBConfig
from amx.db.connector import AssetKind
from amx.services.manual_metadata import (
    ManualEditTarget,
    ManualTargetKind,
    build_inspect_rows,
    build_monitor_rows,
    resolve_path_target,
    resolve_manual_target as _resolve_manual_target,
    split_metadata_path,
)
from amx.utils.console import ask, confirm, console, error, render_table, success, warn

LogEvent = Callable[..., None]


def _summarize_db_exception(exc: Exception) -> str:
    detail = str(exc).strip()
    lower = detail.lower()
    if "connection refused" in lower:
        return "Database connection refused."
    if "timeout" in lower or "timed out" in lower:
        return "Database connection timed out."
    if "authentication" in lower or "password" in lower or "permission denied" in lower:
        return "Database authentication failed."
    first_line = next((line.strip() for line in detail.splitlines() if line.strip()), "")
    return first_line[:220]


def _report_manual_db_error(action: str, exc: Exception) -> None:
    error(f"Could not {action} because AMX cannot reach the active database.")
    warn("Check the active DB profile and run /db then /connect.")
    summary = _summarize_db_exception(exc)
    if summary:
        warn(f"Cause: {summary}")


def _is_cancel(value: str | None) -> bool:
    return (value or "").strip().lower() in {"exit", "quit", "q", "cancel"}


def _cancel_manual_edit() -> None:
    warn("Manual edit cancelled.")


def _ask_text_or_cancel(question: str, default: str = "") -> str | None:
    try:
        value = ask(question, default=default)
    except (EOFError, KeyboardInterrupt):
        _cancel_manual_edit()
        return None
    if _is_cancel(value):
        _cancel_manual_edit()
        return None
    return value


def _ask_choice_or_cancel(
    question: str,
    choices: list[str],
    *,
    default: str = "",
    descriptions: dict[str, str] | None = None,
) -> str | None:
    if not choices:
        return default or None
    while True:
        console.print(f"  [info]{question}[/info]")
        for i, choice in enumerate(choices, 1):
            marker = " [dim](default)[/dim]" if default and choice == default else ""
            desc = f" [dim]{descriptions[choice]}[/dim]" if descriptions and choice in descriptions else ""
            console.print(f"    {i}. [bold]{choice}[/bold]{desc}{marker}")
        value = _ask_text_or_cancel("Select", default="")
        if value is None:
            return None
        if not value and default:
            return default
        if value.isdigit() and 1 <= int(value) <= len(choices):
            return choices[int(value) - 1]
        if value in choices:
            return value
        lower_matches = [choice for choice in choices if choice.lower() == value.lower()]
        if len(lower_matches) == 1:
            return lower_matches[0]
        prefix_matches = [choice for choice in choices if choice.lower().startswith(value.lower())]
        if len(prefix_matches) == 1:
            return prefix_matches[0]
        warn(f"No option matched {value!r}. Type a number, name, or exit.")


def _connector_for_profile(profile_cfg: DBConfig):
    from amx.db.connector import DatabaseConnector

    return DatabaseConnector(profile_cfg)


def _profile_for_path(cfg: AMXConfig, path: str) -> tuple[str, DBConfig] | None:
    parts = split_metadata_path(path)
    if not parts:
        return None
    requested = parts[0]
    if requested in cfg.db_profiles:
        return requested, cfg.db_profiles[requested]
    active = cfg.active_db_profile or "default"
    if requested == active or requested == cfg.db.database:
        return active, cfg.db
    return None


def _target_from_legacy_scope(
    cfg: AMXConfig,
    db: object,
    scope: str,
    names: list[str],
) -> ManualEditTarget | None:
    target = _resolve_manual_target(cfg, db, scope, names, error=warn)
    if target is None:
        return None
    label, writer = target
    kind = ManualTargetKind.DATABASE if scope in {"database", "db"} else ManualTargetKind(scope)
    return ManualEditTarget(
        profile=cfg.active_db_profile or "default",
        kind=kind,
        label=label,
        writer=writer,
    )


def _resolve_explicit_edit_target(
    cfg: AMXConfig,
    target_parts: tuple[str, ...],
) -> ManualEditTarget | None:
    if not target_parts:
        return None

    head = target_parts[0].lower()
    if head in {"schema", "table", "column"} and len(target_parts) == 1:
        return None

    if head in {"database", "db", "schema", "table", "column"}:
        db = _connector_for_profile(cfg.db)
        return _target_from_legacy_scope(cfg, db, head, list(target_parts[1:]))

    if len(target_parts) != 1:
        return None

    profile = _profile_for_path(cfg, target_parts[0])
    if profile is None:
        warn("Use /edit <db>, <db>.<schema>, <db>.<schema>.<table>, or <db>.<schema>.<table>.<column>.")
        warn("Tip: run /edit with no target for the guided wizard.")
        return None

    profile_name, profile_cfg = profile
    db = _connector_for_profile(profile_cfg)
    return resolve_path_target(cfg, db, profile_name, target_parts[0], error=warn)


def _sync_manual_comment_to_search_catalog(
    cfg: AMXConfig,
    target: ManualEditTarget,
    value: str,
) -> None:
    try:
        from amx.search.catalog import SearchCatalog
    except Exception:
        return
    catalog = SearchCatalog.from_history_store()
    if catalog is None:
        return
    profile_cfg = cfg.db_profiles.get(target.profile, cfg.db)
    database_name = profile_cfg.database or profile_cfg.catalog or profile_cfg.project or ""
    path = target.label.split()[-1]
    parts = split_metadata_path(path)
    schema_name = ""
    table_name = ""
    column_name: str | None = None
    entity_kind = "table"
    asset_kind = AssetKind.TABLE.value
    if target.kind == ManualTargetKind.DATABASE:
        entity_kind = "database"
        asset_kind = AssetKind.DATABASE.value
    elif target.kind == ManualTargetKind.SCHEMA:
        entity_kind = "schema"
        asset_kind = AssetKind.SCHEMA.value
        if parts:
            schema_name = parts[-1]
    elif target.kind == ManualTargetKind.TABLE:
        entity_kind = "table"
        if len(parts) >= 2:
            schema_name = parts[-2]
            table_name = parts[-1]
    elif target.kind == ManualTargetKind.COLUMN:
        entity_kind = "column"
        if len(parts) >= 3:
            schema_name = parts[-3]
            table_name = parts[-2]
            column_name = parts[-1]
    catalog.record_manual_description(
        db_profile=target.profile,
        db_backend=profile_cfg.backend,
        database_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        entity_kind=entity_kind,
        asset_kind=asset_kind,
        description=value,
    )


def _select_db_profile_for_wizard(cfg: AMXConfig) -> tuple[str, DBConfig] | None:
    active = cfg.active_db_profile or "default"
    current = cfg.db_profiles.get(active, cfg.db)
    answer = _ask_text_or_cancel(f"Use current active database profile '{active}'? (y/n)", default="y")
    if answer is None:
        return None
    if answer.strip().lower() in {"y", "yes"}:
        return active, current
    if answer.strip().lower() not in {"n", "no"}:
        warn("Please answer y or n.")
        return _select_db_profile_for_wizard(cfg)

    names = sorted(cfg.db_profiles.keys())
    descriptions = {name: f"[{db.backend}] {db.display_summary}" for name, db in cfg.db_profiles.items()}
    selected = _ask_choice_or_cancel(
        "Select database profile",
        names,
        default=active if active in names else (names[0] if names else ""),
        descriptions=descriptions,
    )
    if selected is None:
        return None
    return selected, cfg.db_profiles[selected]


def _select_schema_for_wizard(db: object, default: str = "") -> str | None:
    from amx.utils.console import step_spinner

    try:
        with step_spinner("Listing schemas for manual edit"):
            schemas = list(db.list_schemas())  # type: ignore[attr-defined]
    except Exception:
        schemas = []
    if schemas:
        return _ask_choice_or_cancel("Select schema", schemas, default=default if default in schemas else "")
    return _ask_text_or_cancel("Schema", default=default)


def _select_table_for_wizard(db: object, schema: str, default: str = "") -> str | None:
    from amx.utils.console import step_spinner

    try:
        with step_spinner(f"Listing assets in {schema}"):
            assets = list(db.list_assets(schema))  # type: ignore[attr-defined]
    except Exception:
        assets = []
    names = [name for name, _kind in assets]
    if names:
        descriptions = {name: kind.label for name, kind in assets}
        return _ask_choice_or_cancel("Select table/view", names, default=default if default in names else "", descriptions=descriptions)
    return _ask_text_or_cancel("Table/view", default=default)


def _select_column_for_wizard(db: object, schema: str, table: str) -> str | None:
    from amx.utils.console import step_spinner

    try:
        with step_spinner(f"Listing columns for {schema}.{table}"):
            profiles = list(db.list_column_profiles(schema, table))  # type: ignore[attr-defined]
    except Exception:
        profiles = []
    names = [profile.name for profile in profiles]
    if names:
        descriptions = {profile.name: profile.dtype for profile in profiles}
        return _ask_choice_or_cancel("Select column", names, descriptions=descriptions)
    return _ask_text_or_cancel("Column")


def _run_edit_wizard(cfg: AMXConfig) -> ManualEditTarget | None:
    selected = _select_db_profile_for_wizard(cfg)
    if selected is None:
        return None
    profile_name, profile_cfg = selected
    db = _connector_for_profile(profile_cfg)

    granularity = _ask_choice_or_cancel(
        "What do you want to edit?",
        ["Database", "Schema", "Table", "Column"],
        default="Table",
    )
    if granularity is None:
        return None

    kind = granularity.lower()
    if kind == "database":
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.DATABASE,
            label=f"database {profile_name}",
            writer=lambda comment: db.set_database_comment(comment),  # type: ignore[attr-defined]
        )

    schema = _select_schema_for_wizard(db, default=cfg.current_schema)
    if schema is None:
        return None
    if kind == "schema":
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.SCHEMA,
            label=f"schema {profile_name}.{schema}",
            writer=lambda comment: db.set_schema_comment(schema, comment),  # type: ignore[attr-defined]
        )

    table = _select_table_for_wizard(db, schema, default=cfg.current_table)
    if table is None:
        return None
    if kind == "table":
        asset_kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(asset_kind, AssetKind):
            asset_kind = AssetKind.TABLE
        return ManualEditTarget(
            profile=profile_name,
            kind=ManualTargetKind.TABLE,
            label=f"{asset_kind.label} {profile_name}.{schema}.{table}",
            writer=lambda comment: db.set_table_comment(schema, table, comment, asset_kind=asset_kind),  # type: ignore[attr-defined]
        )

    column = _select_column_for_wizard(db, schema, table)
    if column is None:
        return None
    return ManualEditTarget(
        profile=profile_name,
        kind=ManualTargetKind.COLUMN,
        label=f"column {profile_name}.{schema}.{table}.{column}",
        writer=lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
    )


def _prompt_for_comment(target_label: str, comment: str | None) -> str | None:
    if comment is not None:
        return comment
    console.print(f"  [heading]Editing: {target_label}[/heading]")
    return _ask_text_or_cancel("New comment", default="")


def register_manual_commands(
    main: click.Group,
    *,
    pass_config: Callable[..., object],
    log_event: LogEvent | None = None,
) -> click.Group:
    """Attach database metadata editing commands to the main Click group."""

    @main.group("metadata")
    def manual() -> None:
        """Inspect, edit, and monitor database metadata."""

    @manual.command("inspect")
    @click.argument("schema", required=False)
    @click.argument("table", required=False)
    @pass_config
    def manual_inspect(cfg: AMXConfig, schema: str | None, table: str | None) -> None:
        """Inspect current database/schema/table/column comments."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        try:
            result = build_inspect_rows(cfg, db, schema, table, error=error)
        except Exception as exc:
            _report_manual_db_error("inspect metadata", exc)
            return
        if result is None:
            return
        title, rows = result
        render_table(title, ["Scope", "Name", "Comment"], rows)

    @manual.command("edit")
    @click.argument("target_parts", nargs=-1)
    @click.option("--comment", "-c", default=None, help="Comment text. If omitted, AMX prompts interactively.")
    @click.option("--yes", "-y", is_flag=True, help="Write without confirmation.")
    @pass_config
    def manual_edit(
        cfg: AMXConfig,
        target_parts: tuple[str, ...],
        comment: str | None,
        yes: bool,
    ) -> None:
        """Edit one database/schema/table/column comment manually."""
        try:
            target = _resolve_explicit_edit_target(cfg, target_parts)
            if target is None:
                target = _run_edit_wizard(cfg)
        except Exception as exc:
            _report_manual_db_error("resolve the manual edit target", exc)
            return
        if target is None:
            return
        target_label = target.label
        value = _prompt_for_comment(target_label, comment)
        if value is None:
            return
        if not yes:
            try:
                confirmed = confirm(f"Write comment to {target_label}?", default=True)
            except (EOFError, KeyboardInterrupt):
                warn("Manual edit cancelled.")
                return
            if not confirmed:
                warn("Manual edit cancelled.")
                return
        try:
            target.writer(value)
        except Exception as exc:
            _report_manual_db_error("write the manual comment", exc)
            if log_event is not None:
                log_event(
                    event_type="manual_metadata_edit",
                    status="failed",
                    command="manual edit",
                    details={"target": target_label, "error": str(exc)},
                )
            return
        _sync_manual_comment_to_search_catalog(cfg, target, value)
        success(f"Updated {target_label}.")
        if log_event is not None:
            log_event(
                event_type="manual_metadata_edit",
                status="success",
                command="manual edit",
                details={"target": target_label, "scope": target.kind.value, "db_profile": target.profile},
            )

    @manual.command("monitor")
    @click.argument("schema", required=False)
    @pass_config
    def manual_monitor(cfg: AMXConfig, schema: str | None) -> None:
        """Show comment coverage for one schema or all user schemas."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        try:
            rows = build_monitor_rows(cfg, db, schema)
        except Exception as exc:
            _report_manual_db_error("monitor metadata coverage", exc)
            return
        render_table(
            "Manual metadata coverage",
            ["Schema", "Asset comments", "Asset %", "Column comments", "Column %"],
            rows,
        )

    main.add_command(manual, "manual")
    return manual
