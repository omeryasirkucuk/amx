"""Manual metadata editing and monitoring commands for AMX."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import click

from amx.config import AMXConfig
from amx.db.connector import AssetKind
from amx.utils.console import ask, confirm, error, render_table, success, warn

LogEvent = Callable[..., None]


@dataclass
class MetadataCoverage:
    schema: str
    assets: int = 0
    assets_with_comments: int = 0
    columns: int = 0
    columns_with_comments: int = 0

    @property
    def asset_percent(self) -> float:
        return _percent(self.assets_with_comments, self.assets)

    @property
    def column_percent(self) -> float:
        return _percent(self.columns_with_comments, self.columns)


def _percent(part: int, total: int) -> float:
    return (float(part) / float(total) * 100.0) if total else 0.0


def _resolve_schema(cfg: AMXConfig, schema: str | None) -> str | None:
    resolved = schema or cfg.current_schema
    if not resolved:
        error("Schema is required. Use /schema <name> in /db or pass a schema.")
        return None
    return resolved


def _resolve_table(cfg: AMXConfig, table: str | None) -> str | None:
    resolved = table or cfg.current_table
    if not resolved:
        error("Table is required. Use /table <name> in /db or pass a table.")
        return None
    return resolved


def _display_comment(value: str | None) -> str:
    return value if value else "[dim](empty)[/dim]"


def collect_metadata_coverage(db: object, schema: str) -> MetadataCoverage:
    """Collect table/view and column comment coverage for one schema."""
    coverage = MetadataCoverage(schema=schema)
    assets = list(db.list_assets(schema))  # type: ignore[attr-defined]
    coverage.assets = len(assets)
    for name, _kind in assets:
        try:
            if db.get_table_comment(schema, name):  # type: ignore[attr-defined]
                coverage.assets_with_comments += 1
        except Exception:
            pass
        try:
            col_comments = db.get_column_comments(schema, name)  # type: ignore[attr-defined]
        except Exception:
            col_comments = {}
        coverage.columns += len(col_comments)
        coverage.columns_with_comments += sum(1 for c in col_comments.values() if c)
    return coverage


def register_manual_commands(
    main: click.Group,
    *,
    pass_config: Callable[..., object],
    log_event: LogEvent | None = None,
) -> click.Group:
    """Attach `/manual` namespace commands to the main Click group."""

    @main.group("manual")
    def manual() -> None:
        """Manual metadata editing and monitoring."""

    @manual.command("inspect")
    @click.argument("schema", required=False)
    @click.argument("table", required=False)
    @pass_config
    def manual_inspect(cfg: AMXConfig, schema: str | None, table: str | None) -> None:
        """Inspect current database/schema/table/column comments."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        if not schema and not table:
            rows = [
                ["Database", cfg.db.database or cfg.db.display_summary, _display_comment(db.get_database_comment())],
            ]
            active_schema = cfg.current_schema
            if active_schema:
                rows.append(["Schema", active_schema, _display_comment(db.get_schema_comment(active_schema))])
            render_table("Manual metadata", ["Scope", "Name", "Comment"], rows)
            return

        resolved_schema = _resolve_schema(cfg, schema)
        if not resolved_schema:
            return

        if not table:
            rows = [["Schema", resolved_schema, _display_comment(db.get_schema_comment(resolved_schema))]]
            for asset_name, kind in db.list_assets(resolved_schema):
                rows.append([
                    kind.label,
                    asset_name,
                    _display_comment(db.get_table_comment(resolved_schema, asset_name)),
                ])
            render_table(f"Manual metadata: {resolved_schema}", ["Scope", "Name", "Comment"], rows)
            return

        asset_kind = db.resolve_asset_kind(resolved_schema, table)
        rows = [
            [
                asset_kind.label,
                table,
                _display_comment(db.get_table_comment(resolved_schema, table)),
            ]
        ]
        for col, comment in sorted(db.get_column_comments(resolved_schema, table).items()):
            rows.append(["column", col, _display_comment(comment)])
        render_table(f"Manual metadata: {resolved_schema}.{table}", ["Scope", "Name", "Comment"], rows)

    @manual.command("edit")
    @click.argument("scope", type=click.Choice(["database", "schema", "table", "column"]))
    @click.argument("names", nargs=-1)
    @click.option("--comment", "-c", default=None, help="Comment text. If omitted, AMX prompts interactively.")
    @click.option("--yes", "-y", is_flag=True, help="Write without confirmation.")
    @pass_config
    def manual_edit(
        cfg: AMXConfig,
        scope: str,
        names: tuple[str, ...],
        comment: str | None,
        yes: bool,
    ) -> None:
        """Edit one database/schema/table/column comment manually."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        target = _resolve_manual_target(cfg, db, scope, list(names))
        if target is None:
            return
        target_label, writer = target
        value = comment
        if value is None:
            value = ask(f"New comment for {target_label}", default="")
        if value is None:
            return
        if not yes and not confirm(f"Write comment to {target_label}?", default=True):
            warn("Manual edit cancelled.")
            return
        try:
            writer(value)
        except Exception as exc:
            error(f"Manual edit failed: {exc}")
            if log_event is not None:
                log_event(
                    event_type="manual_metadata_edit",
                    status="failed",
                    command="manual edit",
                    details={"target": target_label, "error": str(exc)},
                )
            return
        success(f"Updated {target_label}.")
        if log_event is not None:
            log_event(
                event_type="manual_metadata_edit",
                status="success",
                command="manual edit",
                details={"target": target_label, "scope": scope},
            )

    @manual.command("monitor")
    @click.argument("schema", required=False)
    @pass_config
    def manual_monitor(cfg: AMXConfig, schema: str | None) -> None:
        """Show comment coverage for one schema or all user schemas."""
        from amx.db.connector import DatabaseConnector

        db = DatabaseConnector(cfg.db)
        schemas = [schema] if schema else ([cfg.current_schema] if cfg.current_schema else db.list_schemas())
        rows = []
        for sch in schemas:
            if not sch:
                continue
            coverage = collect_metadata_coverage(db, sch)
            rows.append([
                coverage.schema,
                f"{coverage.assets_with_comments}/{coverage.assets}",
                f"{coverage.asset_percent:.1f}%",
                f"{coverage.columns_with_comments}/{coverage.columns}",
                f"{coverage.column_percent:.1f}%",
            ])
        render_table(
            "Manual metadata coverage",
            ["Schema", "Asset comments", "Asset %", "Column comments", "Column %"],
            rows,
        )

    return manual


def _resolve_manual_target(
    cfg: AMXConfig,
    db: object,
    scope: str,
    names: list[str],
) -> tuple[str, Callable[[str], None]] | None:
    if scope == "database":
        if names:
            error("Usage: /edit database [--comment TEXT]")
            return None
        return "database", lambda comment: db.set_database_comment(comment)  # type: ignore[attr-defined]

    if scope == "schema":
        schema = names[0] if names else _resolve_schema(cfg, None)
        if not schema:
            return None
        if len(names) > 1:
            error("Usage: /edit schema [schema] [--comment TEXT]")
            return None
        return f"schema {schema}", lambda comment: db.set_schema_comment(schema, comment)  # type: ignore[attr-defined]

    if scope == "table":
        schema = names[0] if len(names) >= 2 else _resolve_schema(cfg, None)
        table = names[1] if len(names) >= 2 else (names[0] if names else _resolve_table(cfg, None))
        if not schema or not table:
            return None
        if len(names) > 2:
            error("Usage: /edit table [schema] [table] [--comment TEXT]")
            return None
        kind = db.resolve_asset_kind(schema, table)  # type: ignore[attr-defined]
        if not isinstance(kind, AssetKind):
            kind = AssetKind.TABLE
        return (
            f"{kind.label} {schema}.{table}",
            lambda comment: db.set_table_comment(schema, table, comment, asset_kind=kind),  # type: ignore[attr-defined]
        )

    if scope == "column":
        if len(names) >= 3:
            schema, table, column = names[0], names[1], names[2]
        elif len(names) == 2:
            schema = _resolve_schema(cfg, None)
            table, column = names[0], names[1]
        elif len(names) == 1:
            schema = _resolve_schema(cfg, None)
            table = _resolve_table(cfg, None)
            column = names[0]
        else:
            schema = _resolve_schema(cfg, None)
            table = _resolve_table(cfg, None)
            column = None
        if not schema or not table or not column:
            error("Usage: /edit column [schema] [table] <column> [--comment TEXT]")
            return None
        if len(names) > 3:
            error("Usage: /edit column [schema] [table] <column> [--comment TEXT]")
            return None
        return (
            f"column {schema}.{table}.{column}",
            lambda comment: db.set_column_comment(schema, table, column, comment),  # type: ignore[attr-defined]
        )

    return None
