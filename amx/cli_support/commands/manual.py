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


def _report_manual_db_error(action: str, exc: Exception) -> None:
    error(f"Could not {action} because AMX cannot reach the active database.")
    warn("Check the active DB profile and run /db then /connect.")
    detail = str(exc).strip()
    if detail:
        warn(detail)


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
        try:
            target = _resolve_manual_target(cfg, db, scope, list(names), error=error)
        except Exception as exc:
            _report_manual_db_error("resolve the manual edit target", exc)
            return
        if target is None:
            return
        target_label, writer = target
        value = comment
        if value is None:
            try:
                value = ask(f"New comment for {target_label}", default="")
            except (EOFError, KeyboardInterrupt):
                warn("Manual edit cancelled.")
                return
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
            writer(value)
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

    return manual
