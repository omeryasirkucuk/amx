"""Analyze namespace helpers and apply flow for the AMX interactive CLI."""

from __future__ import annotations

import contextlib
import sys
from collections.abc import Callable
from typing import Any

import click

from amx.config import AMXConfig
from amx.services.analyze_scope import (
    asset_display_list as _svc_asset_display_list,
)
from amx.services.analyze_scope import (
    filter_non_business_assets as _svc_filter_non_business_assets,
)
from amx.services.analyze_scope import (
    finalize_scope as _svc_finalize_scope,
)
from amx.services.analyze_scope import (
    is_non_business_asset as _svc_is_non_business_asset,
)
from amx.services.analyze_scope import (
    pick_assets as _svc_pick_assets,
)
from amx.services.analyze_scope import (
    resolve_codebase_for_run as _svc_resolve_codebase_for_run,
)
from amx.services.analyze_scope import (
    resolve_run_scope as _svc_resolve_run_scope,
)
from amx.services.analyze_scope import (
    validate_assets_in_schema as _svc_validate_assets_in_schema,
)
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    ask_choice,
    ask_multi_choice,
    confirm,
    error,
    heading,
    info,
    render_table,
    success,
    warn,
)

LogEvent = Callable[..., None]


def _is_non_business_asset(name: str) -> bool:
    """Return True for telemetry/system assets that should not be described by AMX."""
    return _svc_is_non_business_asset(name)


def _filter_non_business_assets(scope: dict[str, list[str]]) -> dict[str, list[str]]:
    """Drop telemetry/system assets from analysis scope and warn once per schema."""
    return _svc_filter_non_business_assets(scope, warn=warn)


def _validate_assets_in_schema(db: object, schema: str, names: list[str]) -> list[str]:
    """Map user input to real asset names (case-insensitive). Raise ValueError if any name is unknown."""
    return _svc_validate_assets_in_schema(db, schema, names)


def _finalize_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> dict[str, list[str]] | None:
    """Resolve interactive or CLI scope and validate asset names against the database."""
    return _svc_finalize_scope(
        cfg,
        db,
        schema,
        table_args,
        ask_choice=ask_choice,
        ask_multi_choice=ask_multi_choice,
        error=error,
        warn=warn,
    )


def _resolve_run_scope(
    cfg: AMXConfig,
    db: object,
    schema: str | None,
    table_args: list[str],
) -> dict[str, list[str]]:
    """Three-level scope resolution: database -> schema -> asset."""
    return _svc_resolve_run_scope(
        cfg,
        db,
        schema,
        table_args,
        ask_choice=ask_choice,
        ask_multi_choice=ask_multi_choice,
        warn=warn,
    )


def _asset_display_list(db: object, schema: str) -> list[str]:
    """Build display labels for interactive selection: ``name  [kind]``."""
    return _svc_asset_display_list(db, schema)


def _pick_assets(display_list: list[str]) -> list[str]:
    """Interactive multi-choice that strips display tags before returning bare names."""
    return _svc_pick_assets(display_list, ask_multi_choice=ask_multi_choice)


def _resolve_codebase_for_run(
    cfg: AMXConfig,
    db: object,
    scope: dict[str, list[str]],
    code_profile: str | None,
    code_refresh: bool,
) -> object | None:
    """Load or build codebase report for /run and /run-apply."""
    from amx.utils.console import step_spinner

    return _svc_resolve_codebase_for_run(
        cfg,
        db,
        scope,
        code_profile,
        code_refresh,
        error=error,
        warn=warn,
        info=info,
        step_spinner=step_spinner,
    )


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
        from amx.agents.orchestrator import (
            apply_review_results_to_db,
            create_live_writeback_progress,
        )
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
                with contextlib.suppress(Exception):
                    hs.record_applied(result.result_id)

        def _on_failed(result: Any, exc: Exception) -> None:
            hs = history_store()
            if result.result_id is not None and hs is not None:
                with contextlib.suppress(Exception):
                    hs.record_db_apply_failure(result.result_id, str(exc))

        _on_progress, _finish_progress = create_live_writeback_progress(
            total=len(pending),
            backend=db.backend,
        )

        # Build the audit context once so each successful COMMENT
        # write lands in apply_events with the correct attribution.
        # The history_store call is best-effort: if the store is
        # disabled (private mode, init failed), audit_log stays None
        # and apply_review_results_to_db treats the audit hooks as
        # no-ops. Hostname and applied_by are read from the
        # standard library so we don't need to depend on amx.config
        # for what is essentially environment metadata.
        import getpass
        import socket

        audit_log_handle = history_store()
        try:
            applied_by = getpass.getuser()
        except Exception:
            applied_by = ""
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = ""

        try:
            applied_count = apply_review_results_to_db(
                db,
                pending,
                on_applied=_on_applied,
                on_failed=_on_failed,
                on_progress=_on_progress if pending else None,
                audit_log=audit_log_handle,
                audit_profile=getattr(cfg.db, "name", "") or "",
                audit_user=applied_by,
                audit_host=hostname,
            )
        finally:
            if pending:
                _finish_progress()
        clear_pending()
        success(f"Applied {applied_count} comment(s). Pending file cleared.")
        log_event(
            event_type="analyze_apply",
            status="success",
            command="analyze.apply",
            details={"applied_count": applied_count},
        )

    return analyze
