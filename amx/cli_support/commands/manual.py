"""Manual metadata editing and monitoring commands for AMX."""

from __future__ import annotations

from collections.abc import Callable

import click

from amx.config import AMXConfig
from amx.services.manual_metadata import (
    build_inspect_rows,
    build_monitor_rows,
    resolve_manual_target as _resolve_manual_target,
)
from amx.utils.console import ask, confirm, error, render_table, success, warn

LogEvent = Callable[..., None]


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
        result = build_inspect_rows(cfg, db, schema, table, error=error)
        if result is None:
            return
        title, rows = result
        render_table(title, ["Scope", "Name", "Comment"], rows)

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
        target = _resolve_manual_target(cfg, db, scope, list(names), error=error)
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
        rows = build_monitor_rows(cfg, db, schema)
        render_table(
            "Manual metadata coverage",
            ["Schema", "Asset comments", "Asset %", "Column comments", "Column %"],
            rows,
        )

    return manual
