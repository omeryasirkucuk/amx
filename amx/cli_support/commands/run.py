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
    *,
    headless: bool = False,
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
        headless=headless,
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


def _report_apply_failures(outcomes: list[Any]) -> None:
    """Surface classified write failures with their per-backend remediation.

    When apply captures ``outcomes_out``, the classifier computes an
    actionable title + suggested action (e.g. the ``GRANT ALTER`` the role
    is missing) for each failed row. The CLI used to discard all of that
    and record only the raw driver exception, so a privilege/ALTER failure
    printed an opaque stack message. Show the classified title once per row
    and each distinct remediation hint once.
    """
    failed = [o for o in outcomes if getattr(o, "status", "") == "failed"]
    if not failed:
        return
    warn(f"{len(failed)} comment(s) could not be written to the live database:")
    for o in failed:
        loc = ".".join(p for p in (o.schema, o.table, o.column) if p)
        title = getattr(o, "error_title", "") or getattr(o, "error_kind", "") or "write failed"
        error(f"  {loc}: {title}")
    seen: set[str] = set()
    for o in failed:
        action = (getattr(o, "error_action", "") or "").strip()
        if action and action not in seen:
            seen.add(action)
            info(f"  → {action}")


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
    @click.option(
        "--dry-run",
        is_flag=True,
        default=False,
        help=(
            "Print the COMMENT statements that would run, without "
            "touching the database. The pending file is left "
            "unchanged so you can re-run /apply for real after "
            "reviewing the preview."
        ),
    )
    @pass_config
    def analyze_apply(cfg: AMXConfig, dry_run: bool) -> None:
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

        heading(
            "Preview pending metadata writes (dry-run)"
            if dry_run
            else "Apply pending metadata to the database"
        )
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
        if dry_run:
            # Dry-run path: short-circuit straight to the preview loop.
            # No confirmation prompt — nothing will be written so there
            # is nothing for the user to gate. Pending file is left
            # untouched so the user can re-run /apply for real.
            info(
                f"Dry-run: showing the SQL templates for {len(pending)} pending comment(s). "
                "No changes will be written."
            )
            db = DatabaseConnector(cfg.db)
            if not db.test_connection():
                log_event(
                    event_type="analyze_apply",
                    status="failed",
                    command="analyze.apply",
                    details={"reason": "db_connect_failed", "dry_run": True},
                )
                error("Cannot connect to database.")
                sys.exit(1)

            preview_count = 0
            skipped_count = 0

            def _on_preview(_r: Any, status: str, idx: int, total: int, detail: str) -> None:
                nonlocal preview_count, skipped_count
                if status == "preview":
                    preview_count += 1
                    if "unsupported" in detail.lower():
                        skipped_count += 1
                        info(f"  [{idx}/{total}] (skipped — backend cannot accept this asset kind)")
                    else:
                        info(f"  [{idx}/{total}] {detail}")
                elif status == "preview_failed":
                    skipped_count += 1
                    error(f"  [{idx}/{total}] preview error: {detail}")

            apply_review_results_to_db(
                db,
                pending,
                on_progress=_on_preview,
                dry_run=True,
            )
            success(
                f"Dry-run complete: {preview_count} comment(s) previewed"
                + (f", {skipped_count} unsupported/skipped." if skipped_count else ".")
                + " Pending file unchanged. Re-run `/analyze apply` (without --dry-run) to write."
            )
            log_event(
                event_type="analyze_apply",
                status="preview",
                command="analyze.apply",
                details={
                    "preview_count": preview_count,
                    "skipped_count": skipped_count,
                    "dry_run": True,
                },
            )
            return

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
                    hs.record_applied(
                        result.result_id,
                        chosen_description=getattr(result, "final_description", None) or None,
                    )

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

        outcomes: list[Any] = []
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
                outcomes_out=outcomes,
            )
        finally:
            if pending:
                _finish_progress()
        _report_apply_failures(outcomes)
        clear_pending()
        success(f"Applied {applied_count} comment(s). Pending file cleared.")
        log_event(
            event_type="analyze_apply",
            status="success",
            command="analyze.apply",
            details={"applied_count": applied_count},
        )

    return analyze
