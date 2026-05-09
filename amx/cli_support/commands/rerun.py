"""`amx rerun` — regenerate alternatives for one or many run_results rows.

Mirrors the Studio re-run flow but lets the user drive everything from
the CLI:

* ``amx rerun <result_id>`` — single item, current LLM/DB profile.
* ``amx rerun <id1> <id2> <id3> --instructions "..."`` — multi-item
  with a shared free-text addendum.
* ``amx rerun --run <run_id> --table public.orders --column status`` —
  resolve target by coordinates instead of id (handy when the user is
  reading ``/history show`` output).
* ``amx rerun --run <run_id> --pick`` — interactive checkbox TUI to
  cherry-pick a subset of the run's rows.

The actual work lives in
:func:`amx.agents._orchestrator.rerun.rerun_items` so the CLI stays a
thin wrapper. The CLI version always runs synchronously (no SSE
streaming); progress is rendered through the existing themed
``console`` helpers.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import click

from amx.agents.rerun_context import RerunContextError
from amx.config import AMXConfig
from amx.storage.sqlite_store import history_store
from amx.utils.console import (
    error,
    heading,
    info,
    render_table,
    render_token_summary,
    success,
    warn,
)
from amx.utils.logging import get_logger
from amx.utils.token_tracker import tracker as token_tracker

LogEvent = Callable[..., None]
log = get_logger("cli.rerun")


def _resolve_targets_by_coordinates(
    *,
    run_id: int,
    table: str | None,
    column: str | None,
) -> list[int]:
    """Map ``run_id`` + optional ``schema.table`` / ``column`` to ``result_ids``.

    The ``table`` argument is a dotted ``schema.table`` (matching the
    rest of the AMX CLI surface); ``column`` narrows further. When
    only ``run_id`` is provided we return every row for that run —
    callers that don't want the whole run should pass at least
    ``--table``.
    """
    hs = history_store()
    if hs is None:
        raise click.ClickException(
            "History store is not initialised. Run /history-store enable first."
        )
    rows = hs.get_run_results(int(run_id))
    if not rows:
        raise click.ClickException(f"No run_results rows found for run {run_id}.")

    schema_part: str | None = None
    table_part: str | None = None
    if table:
        if "." not in table:
            raise click.ClickException(
                "--table must be in the form schema.table (e.g. public.orders)."
            )
        schema_part, table_part = table.split(".", 1)

    matches: list[int] = []
    for row in rows:
        if schema_part is not None and row.get("schema_name") != schema_part:
            continue
        if table_part is not None and row.get("table_name") != table_part:
            continue
        if column is not None and row.get("column_name") != column:
            continue
        matches.append(int(row["id"]))

    if not matches:
        raise click.ClickException(
            f"No rows in run {run_id} matched table={table or '(any)'} column={column or '(any)'}."
        )
    return matches


def _interactive_pick(run_id: int) -> list[int]:
    """Render a checkbox-style picker over a run's rows.

    Falls back to a numbered prompt when ``prompt_toolkit`` isn't
    available — the CLI must keep working in environments where the
    interactive picker can't render.
    """
    hs = history_store()
    if hs is None:
        raise click.ClickException("History store is not initialised.")
    rows = hs.get_run_results(int(run_id))
    if not rows:
        raise click.ClickException(f"No rows for run {run_id}.")

    info(
        f"Run {run_id} has {len(rows)} result row(s). Enter ids to re-run, "
        "comma-separated, or 'all':"
    )
    table_data = []
    for row in rows:
        chosen = (row.get("chosen_description") or "")[:60]
        if len(row.get("chosen_description") or "") > 60:
            chosen += "…"
        table_data.append(
            [
                str(row["id"]),
                row.get("asset_kind") or "",
                f"{row.get('schema_name') or ''}.{row.get('table_name') or ''}",
                row.get("column_name") or "",
                chosen,
            ]
        )
    render_table(
        f"Run {run_id} — pick rows to re-run",
        ["id", "kind", "schema.table", "column", "chosen"],
        table_data,
    )

    raw = click.prompt("ids", default="", show_default=False).strip()
    if not raw:
        raise click.ClickException("No ids selected.")
    if raw.lower() == "all":
        return [int(r["id"]) for r in rows]
    try:
        return [int(part.strip()) for part in raw.split(",") if part.strip()]
    except ValueError as exc:
        raise click.ClickException(f"Invalid id list: {exc}") from exc


def _print_outcomes(outcomes: list[Any], *, new_run_id: int) -> None:
    if not outcomes:
        warn("No outcomes returned from re-run.")
        return
    heading(f"Re-run produced {len(outcomes)} new row(s) under run {new_run_id}")
    rows: list[list[str]] = []
    for o in outcomes:
        first_alt = (o.alternatives[0] if o.alternatives else "")[:60]
        if o.alternatives and len(o.alternatives[0]) > 60:
            first_alt += "…"
        rows.append(
            [
                str(o.target_result_id),
                str(o.new_result_id) if o.new_result_id else "—",
                f"v{o.rerun_seq}" if o.rerun_seq else "—",
                f"{o.schema}.{o.table}" + (f".{o.column}" if o.column else ""),
                o.confidence,
                first_alt or (o.error or ""),
            ]
        )
    render_table(
        f"Re-run results (new run_id={new_run_id})",
        ["target", "new_id", "ver", "asset", "confidence", "first alternative / error"],
        rows,
    )
    failures = [o for o in outcomes if o.error]
    if failures:
        warn(f"{len(failures)} target(s) failed; see error column above.")
    else:
        success(f"All {len(outcomes)} re-run target(s) completed successfully.")


def register_rerun_command(
    main: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]],
    log_event: LogEvent,
) -> None:
    """Attach `amx rerun` (and `/rerun` inside the session) to the main group."""

    @main.command("rerun")
    @click.argument("result_ids", nargs=-1, type=int)
    @click.option(
        "--run",
        "run_id",
        type=int,
        default=None,
        help="Resolve targets by run id + --table/--column instead of result id.",
    )
    @click.option(
        "--table",
        type=str,
        default=None,
        help="Schema.table coordinate (used with --run).",
    )
    @click.option(
        "--column",
        type=str,
        default=None,
        help="Column name (used with --run --table).",
    )
    @click.option(
        "--pick",
        is_flag=True,
        default=False,
        help="Interactive picker over a run's rows (used with --run).",
    )
    @click.option(
        "--instructions",
        "-i",
        "user_instructions",
        type=str,
        default=None,
        help=(
            "Optional free-text addendum appended to the existing prompt. "
            "Original DB / docs / code context is preserved."
        ),
    )
    @click.option(
        "--temperature",
        "temperature_override",
        type=float,
        default=None,
        help="Override LLM temperature for this re-run (0.0–1.0).",
    )
    @pass_config
    def rerun(
        cfg: AMXConfig,
        result_ids: tuple[int, ...],
        run_id: int | None,
        table: str | None,
        column: str | None,
        pick: bool,
        user_instructions: str | None,
        temperature_override: float | None,
    ) -> None:
        """Regenerate alternatives for one or many run_results rows."""
        targets: list[int]
        if pick:
            if run_id is None:
                raise click.ClickException("--pick requires --run <run_id>.")
            targets = _interactive_pick(int(run_id))
        elif run_id is not None and (table is not None or column is not None):
            targets = _resolve_targets_by_coordinates(
                run_id=int(run_id), table=table, column=column
            )
        elif result_ids:
            targets = list(result_ids)
        else:
            raise click.ClickException(
                "Pass at least one result_id, or use --run with --table/--column or --pick."
            )

        info(
            f"Re-running {len(targets)} target(s) "
            f"(instructions={'yes' if (user_instructions or '').strip() else 'none'}, "
            f"temperature_override={temperature_override})…"
        )

        from amx.agents._orchestrator.rerun import rerun_items

        try:
            new_run_id, outcomes = rerun_items(
                cfg,
                target_result_ids=targets,
                user_instructions=user_instructions,
                temperature_override=temperature_override,
            )
        except RerunContextError as exc:
            error(str(exc))
            log_event(
                event_type="rerun",
                status="failed",
                command="rerun",
                details={"reason": str(exc), "targets": targets},
            )
            raise click.ClickException(str(exc)) from exc

        _print_outcomes(outcomes, new_run_id=int(new_run_id))
        # Mirror the analyze flow's end-of-run table so re-run users
        # see the same per-step token + USD cost breakdown the bulk
        # path emits. ``rerun_items`` reset the tracker on entry, so
        # ``token_tracker.summary()`` here is exactly the per-call
        # accounting for this re-run alone.
        render_token_summary(token_tracker)
        successful = sum(1 for o in outcomes if not o.error)
        log_event(
            event_type="rerun",
            status="success" if successful == len(outcomes) else "partial",
            command="rerun",
            details={
                "new_run_id": int(new_run_id),
                "targets": targets,
                "successful": successful,
                "failed": len(outcomes) - successful,
                "had_instructions": bool((user_instructions or "").strip()),
                "temperature_override": temperature_override,
            },
        )


__all__ = ["register_rerun_command"]
