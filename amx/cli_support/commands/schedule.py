"""``/analyze schedule`` subgroup — manage one-shot scheduled metadata runs.

Lives under the ``analyze`` namespace (alongside ``/analyze run``,
``/analyze apply``, ``/analyze review``). Inside the ``amx`` REPL the
commands are typed as:

* ``/analyze schedule add``       — guided wizard (all fields optional)
* ``/analyze schedule list``      — table listing with status filters
* ``/analyze schedule show <id>``
* ``/analyze schedule pause <id>`` / ``/analyze schedule resume <id>``
* ``/analyze schedule rm <id>``
* ``/analyze schedule run-now <id>``
* ``/analyze schedule tick``      — engine: one stateless pass
* ``/analyze schedule status``    — engine: daemon presence + counters
* ``/analyze schedule install-daemon`` / ``uninstall-daemon``

Engine commands (tick / install-daemon / uninstall-daemon / status)
are siblings of the entity commands in this single subgroup; no
top-level ``amx <cmd>`` invocation is supported by design.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import click

from amx.config import AMXConfig
from amx.runtime.worker import spawn_scheduled_worker
from amx.scheduler.tick import tick
from amx.storage.sqlite_store import history_store

LogEvent = Callable[..., None]


# ── Helpers ─────────────────────────────────────────────────────────


def _pick_from_list(
    items: list[str],
    *,
    label: str,
    default: str | None = None,
) -> str:
    """Numbered picker; users can type either the index or the literal name."""
    if not items:
        raise click.ClickException(
            f"No {label} configured. Run the matching `amx` setup command "
            "to add one first."
        )
    if len(items) == 1:
        return items[0]
    click.echo(f"\nAvailable {label}:")
    for i, item in enumerate(items, start=1):
        marker = "  (current)" if item == default else ""
        click.echo(f"  [{i}] {item}{marker}")
    fallback_default = default if default in items else items[0]
    raw = click.prompt(
        f"Pick a {label} (number or name)",
        default=fallback_default,
        show_default=True,
    )
    raw = str(raw).strip()
    if raw.isdigit():
        idx = int(raw) - 1
        if 0 <= idx < len(items):
            return items[idx]
        raise click.BadParameter(f"index out of range: {raw}")
    if raw not in items:
        raise click.BadParameter(
            f"unknown {label}: {raw!r}. Available: {', '.join(items)}"
        )
    return raw


def _pick_scope_spec(cfg: AMXConfig, db_profile: str) -> str:
    """Walk the user through scope picking. Returns the compact spec string."""
    mode = click.prompt(
        "Scope mode",
        type=click.Choice(["all", "schemas", "tables"], case_sensitive=False),
        default="all",
        show_default=True,
    )
    if mode == "all":
        return "all"

    schemas: list[str] = []
    try:
        from amx.db.factory import build_connector

        connector = build_connector(cfg, profile_name=db_profile)
        schemas = sorted(connector.list_schemas() or [])
    except Exception as exc:  # noqa: BLE001
        click.echo(
            f"  (could not introspect schemas: {exc}) "
            "Falling back to free-text.",
            err=True,
        )

    if mode == "schemas":
        if not schemas:
            raw = click.prompt("Comma-separated schema names", type=str)
            return f"schema:{raw.strip()}"
        click.echo("\nAvailable schemas (comma-separated indices or names):")
        for i, s in enumerate(schemas, start=1):
            click.echo(f"  [{i}] {s}")
        raw = click.prompt("Pick schemas", type=str).strip()
        picks: list[str] = []
        for tok in [t.strip() for t in raw.split(",") if t.strip()]:
            if tok.isdigit():
                idx = int(tok) - 1
                if 0 <= idx < len(schemas):
                    picks.append(schemas[idx])
                else:
                    raise click.BadParameter(f"index out of range: {tok}")
            elif tok in schemas:
                picks.append(tok)
            else:
                raise click.BadParameter(f"unknown schema: {tok}")
        return "schema:" + ",".join(picks)

    raw = click.prompt(
        "Comma-separated schema.table pairs (e.g. public.users, sales.orders)",
        type=str,
    )
    return f"table:{raw.strip()}"


def _zoneinfo(tz_name: str):
    """Resolve an IANA tz name to a tzinfo. Raises click.BadParameter
    on typos so the CLI surface is consistent."""
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception as exc:  # noqa: BLE001 - rewrap as Click error
        raise click.BadParameter(
            f"unknown timezone: {tz_name!r} (use an IANA name like 'Europe/Istanbul')"
        ) from exc


def _parse_at(at: str, tz_name: str) -> tuple[float, str]:
    """Parse a wall-clock ``YYYY-MM-DD HH:MM`` (or ISO 8601) in the
    chosen tz and return ``(fire_at_utc, fire_at_tz)``."""
    tzinfo = _zoneinfo(tz_name)
    for fmt in (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            naive = datetime.strptime(at, fmt)
            break
        except ValueError:
            continue
    else:
        raise click.BadParameter(f"could not parse --at {at!r} (try 'YYYY-MM-DD HH:MM')")
    aware = naive.replace(tzinfo=tzinfo)
    return aware.astimezone(timezone.utc).timestamp(), tz_name


def _parse_scope(spec: str) -> str:
    """Compact ``schema:a,b`` / ``table:s.t1,s.t2`` / ``all`` -> scope_json."""
    spec = spec.strip()
    if spec == "all":
        return json.dumps({"mode": "all"})
    if spec.startswith("schema:"):
        names = [s.strip() for s in spec[len("schema:") :].split(",") if s.strip()]
        if not names:
            raise click.BadParameter("scope schema:... needs at least one name")
        return json.dumps({"mode": "schemas", "schemas": names})
    if spec.startswith("table:"):
        items: list[dict[str, str]] = []
        for piece in spec[len("table:") :].split(","):
            piece = piece.strip()
            if not piece:
                continue
            if "." not in piece:
                raise click.BadParameter(f"table entry {piece!r} must be schema.table")
            schema, _, table = piece.partition(".")
            items.append({"schema": schema, "table": table})
        return json.dumps({"mode": "tables", "tables": items})
    raise click.BadParameter("scope must be 'schema:NAME,...' / 'table:S.T,...' / 'all'")


def _render_at_local(row: dict[str, Any]) -> str:
    tzinfo = _zoneinfo(row["fire_at_tz"])
    dt = datetime.fromtimestamp(row["fire_at_utc"], tz=timezone.utc).astimezone(tzinfo)
    return dt.strftime("%Y-%m-%d %H:%M")


def _print_table(rows: list[dict[str, Any]]) -> None:
    if not rows:
        click.echo("No schedules.")
        return
    header = ("#", "Name", "When (local)", "Tz", "Status", "DB", "LLM")
    keys = ("id", "name", "_local", "fire_at_tz", "status", "db_profile", "llm_profile")
    widths = [
        max(len(str(h)), max((len(_cell(r, k)) for r in rows), default=0))
        for h, k in zip(header, keys, strict=True)
    ]

    def line(values):
        return "  ".join(str(v).ljust(w) for v, w in zip(values, widths, strict=True))

    click.echo(line(header))
    click.echo(line(["-" * w for w in widths]))
    for r in rows:
        click.echo(
            line(
                [
                    r["id"],
                    r["name"],
                    _render_at_local(r),
                    r["fire_at_tz"],
                    r["status"],
                    r["db_profile"],
                    r["llm_profile"],
                ]
            )
        )


def _cell(row: dict[str, Any], key: str) -> str:
    if key == "_local":
        return _render_at_local(row)
    return str(row.get(key, ""))


def _require_store():
    hs = history_store()
    if hs is None:
        raise click.ClickException(
            "history store not initialised (run any other amx command first)"
        )
    return hs


# ── Registration ────────────────────────────────────────────────────


def register_schedule_commands(
    parent: click.Group,
    *,
    pass_config: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    log_event: LogEvent | None = None,
) -> None:
    """Attach the ``schedule`` subgroup under *parent* (the ``/analyze`` tab).

    Both entity-lifecycle and engine commands hang off the same
    subgroup so the REPL only ever surfaces ``/analyze schedule ...``.
    """

    pc = pass_config or click.make_pass_decorator(AMXConfig, ensure=True)

    @parent.group("schedule")
    def schedule() -> None:
        """One-shot scheduled metadata runs (CRUD + engine + daemon)."""

    @schedule.command("add")
    @click.option("--name", default=None, help="Human-readable label.")
    @click.option(
        "--at",
        "at",
        default=None,
        help="Local wall-clock fire time (e.g. '2026-12-31 09:00'). Prompts if omitted.",
    )
    @click.option(
        "--tz",
        "tz_name",
        default=None,
        help="IANA tz id for --at (e.g. 'Europe/Istanbul'). Defaults to system tz.",
    )
    @click.option(
        "--db",
        "db_profile",
        default=None,
        help="DB profile name. Picker shown when omitted.",
    )
    @click.option(
        "--scope",
        default=None,
        help="Scope: 'schema:a,b' / 'table:s.t,...' / 'all'. Wizard shown when omitted.",
    )
    @click.option(
        "--llm",
        "llm_profile",
        default=None,
        help="LLM profile name. Picker shown when omitted.",
    )
    @click.option(
        "--strategy",
        "review_strategy",
        default=None,
        type=click.Choice(["auto", "manual"]),
        help="Review strategy. Prompts if omitted.",
    )
    @pc
    def schedule_add(
        cfg: AMXConfig,
        name: str | None,
        at: str | None,
        tz_name: str | None,
        db_profile: str | None,
        scope: str | None,
        llm_profile: str | None,
        review_strategy: str | None,
    ) -> None:
        """Create a new scheduled run.

        Run without flags for a guided wizard with picker-based selection
        of DB / LLM profiles and live-DB scope, OR supply every flag for
        a non-interactive create.
        """
        hs = _require_store()

        # Resolve each field: use the flag if given, otherwise prompt.
        if not name:
            name = click.prompt("Schedule name", type=str).strip()

        if not at:
            at = click.prompt(
                "Fire time (local, YYYY-MM-DD HH:MM)",
                type=str,
            ).strip()

        if not tz_name:
            # Default to system tz when available.
            try:
                from datetime import datetime as _dt

                tz_name = _dt.now().astimezone().tzinfo.tzname(None) or "UTC"
            except Exception:  # noqa: BLE001
                tz_name = "UTC"
            tz_name = click.prompt(
                "Timezone (IANA)", default=tz_name, type=str
            ).strip()

        if not db_profile:
            db_names = sorted((cfg.db_profiles or {}).keys())
            db_profile = _pick_from_list(
                db_names,
                label="DB profile",
                default=cfg.active_db_profile,
            )

        if not llm_profile:
            llm_names = sorted((cfg.llm_profiles or {}).keys())
            llm_profile = _pick_from_list(
                llm_names,
                label="LLM profile",
                default=cfg.active_llm_profile,
            )

        if not scope:
            scope = _pick_scope_spec(cfg, db_profile)

        if not review_strategy:
            review_strategy = click.prompt(
                "Review strategy",
                type=click.Choice(["auto", "manual"]),
                default="auto",
                show_default=True,
            )

        fire_at_utc, fire_at_tz = _parse_at(at, tz_name)
        scope_json = _parse_scope(scope)
        sid = hs.create_scheduled_run(
            name=name,
            fire_at_utc=fire_at_utc,
            fire_at_tz=fire_at_tz,
            db_profile=db_profile,
            scope_json=scope_json,
            llm_profile=llm_profile,
            review_strategy=review_strategy,
        )
        row = hs.get_scheduled_run(sid)
        local = _render_at_local(row)
        utc_str = datetime.fromtimestamp(fire_at_utc, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M UTC"
        )
        click.echo(f"Schedule #{sid} created: '{name}' fires {local} {fire_at_tz} ({utc_str}).")
        click.echo("")
        click.echo("Heads-up: AMX is invocation-based — it isn't always running.")
        click.echo("For this schedule to fire on time, EITHER keep AMX/Studio")
        click.echo("open at that moment, OR enable the background daemon now:")
        click.echo("")
        click.echo("    /analyze schedule install-daemon")
        click.echo("")
        click.echo("Without the daemon, if AMX is closed at fire time, this")
        click.echo("schedule will be surfaced as 'missed' the next time you")
        click.echo("open AMX, and you can run it then.")
        if log_event:
            log_event(
                event_type="schedule.created",
                status="ok",
                command="schedule.add",
                details={"id": sid, "name": name},
            )

    @schedule.command("list")
    @click.option(
        "--all",
        "show_all",
        is_flag=True,
        default=False,
        help="Include past (completed/failed/cancelled) entries.",
    )
    @click.option(
        "--past",
        "past_only",
        is_flag=True,
        default=False,
        help="Show only past (completed/failed/cancelled) entries.",
    )
    @click.option("--db", "db_profile", default=None, help="Filter by DB profile.")
    @click.option(
        "--json", "as_json", is_flag=True, default=False, help="Emit machine-readable JSON."
    )
    def schedule_list(
        show_all: bool, past_only: bool, db_profile: str | None, as_json: bool
    ) -> None:
        """List scheduled runs."""
        hs = _require_store()
        if past_only:
            statuses = ["completed", "failed", "cancelled"]
        elif show_all:
            statuses = None  # all
        else:
            statuses = ["pending", "paused", "missed", "running"]
        rows = hs.list_scheduled_runs(statuses=statuses, db_profile=db_profile)
        if as_json:
            click.echo(json.dumps(rows, default=str, indent=2))
            return
        _print_table(rows)

    @schedule.command("show")
    @click.argument("schedule_id", type=int)
    def schedule_show(schedule_id: int) -> None:
        """Show full detail for a schedule."""
        hs = _require_store()
        row = hs.get_scheduled_run(schedule_id)
        if row is None:
            raise click.ClickException(f"No schedule with id={schedule_id}")
        click.echo(json.dumps(row, default=str, indent=2))

    @schedule.command("pause")
    @click.argument("schedule_id", type=int)
    def schedule_pause(schedule_id: int) -> None:
        """Pause a pending schedule (it won't fire until resumed)."""
        hs = _require_store()
        try:
            hs.set_scheduled_run_status(schedule_id, "paused")
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Schedule #{schedule_id} paused.")
        if log_event:
            log_event(
                event_type="schedule.paused",
                status="ok",
                command="schedule.pause",
                details={"id": schedule_id},
            )

    @schedule.command("resume")
    @click.argument("schedule_id", type=int)
    def schedule_resume(schedule_id: int) -> None:
        """Resume a paused schedule."""
        hs = _require_store()
        try:
            hs.set_scheduled_run_status(schedule_id, "pending")
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
        click.echo(f"Schedule #{schedule_id} resumed (pending).")
        if log_event:
            log_event(
                event_type="schedule.resumed",
                status="ok",
                command="schedule.resume",
                details={"id": schedule_id},
            )

    @schedule.command("rm")
    @click.argument("schedule_id", type=int)
    @click.option("--yes", "-y", is_flag=True, default=False, help="Skip confirmation.")
    def schedule_rm(schedule_id: int, yes: bool) -> None:
        """Hard-delete a schedule."""
        hs = _require_store()
        row = hs.get_scheduled_run(schedule_id)
        if row is None:
            raise click.ClickException(f"No schedule with id={schedule_id}")
        if not yes:
            click.confirm(
                f"Delete schedule #{schedule_id} '{row['name']}'?",
                abort=True,
            )
        hs.delete_scheduled_run(schedule_id)
        click.echo(f"Schedule #{schedule_id} deleted.")

    @schedule.command("run-now")
    @click.argument("schedule_id", type=int)
    @click.option(
        "--background/--foreground",
        default=False,
        show_default=True,
        help="Run the spawned worker in the background and return immediately.",
    )
    def schedule_run_now(schedule_id: int, background: bool) -> None:
        """Fire a schedule immediately (regardless of fire_at_utc)."""
        hs = _require_store()
        row = hs.get_scheduled_run(schedule_id)
        if row is None:
            raise click.ClickException(f"No schedule with id={schedule_id}")

        def spawn(payload: dict[str, Any]) -> int:
            return spawn_scheduled_worker(payload, store=hs, background=background)

        report = tick(
            store=hs,
            source="manual",
            target_id=schedule_id,
            spawn_worker=spawn,
            now_utc=time.time(),
        )
        if report.fired:
            click.echo(f"Schedule #{schedule_id} fired (run linked).")
        else:
            for sid, err in report.failed_resolution:
                click.echo(f"Failed to fire #{sid}: {err}", err=True)

    # ── Engine + daemon commands (siblings of the entity ones) ──────
    #
    # These used to live under a separate `scheduler` top-level group
    # (`amx scheduler tick`). The user UX rule is "every method lives
    # under a tab" and the REPL only shows tabs — so engine and
    # entity commands share the same `schedule` subgroup, accessed
    # uniformly as ``/analyze schedule <verb>``.

    @schedule.command("tick")
    @click.option(
        "--silent",
        is_flag=True,
        default=False,
        help="Suppress per-fire stdout; used by the daemon cron entry.",
    )
    def schedule_tick(silent: bool) -> None:
        """Run one stateless scheduling pass (fires due schedules)."""
        hs = _require_store()

        def spawn(payload: dict[str, Any]) -> int:
            return spawn_scheduled_worker(payload, store=hs, background=True)

        report = tick(
            store=hs,
            source="daemon",
            spawn_worker=spawn,
            now_utc=time.time(),
        )
        if not silent:
            click.echo(
                json.dumps(
                    {
                        "fired": report.fired,
                        "failed_resolution": report.failed_resolution,
                        "missed_for_review": report.missed_for_review,
                        "stale_recovered": report.stale_recovered,
                    },
                    indent=2,
                )
            )

    @schedule.command("status")
    def schedule_status() -> None:
        """Show scheduler + daemon status."""
        import platform as _platform

        from amx.scheduler.daemon_install import detect_daemon_state

        hs = _require_store()
        pending = hs.list_scheduled_runs(statuses=["pending"], limit=1000)
        paused = hs.list_scheduled_runs(statuses=["paused"], limit=1000)
        missed = hs.list_scheduled_runs(statuses=["missed"], limit=1000)
        daemon = detect_daemon_state()

        click.echo("Schedule status")
        click.echo(f"  Pending: {len(pending)}")
        click.echo(f"  Paused:  {len(paused)}")
        click.echo(f"  Missed:  {len(missed)}")
        if pending:
            n = pending[0]
            click.echo(
                f"  Next:    #{n['id']} '{n['name']}' "
                f"at fire_at_utc={n['fire_at_utc']:.0f}"
            )
        click.echo("")
        click.echo("Daemon")
        click.echo(f"  Platform:  {_platform.system()}")
        click.echo(f"  Installed: {daemon['installed']}")
        if daemon.get("path"):
            click.echo(f"  Unit file: {daemon['path']}")
        if daemon.get("last_tick_log"):
            click.echo(f"  Log:       {daemon['last_tick_log']}")

    @schedule.command("install-daemon")
    def schedule_install_daemon() -> None:
        """Install the OS-level scheduler daemon (launchd or systemd)."""
        from amx.scheduler.daemon_install import install_daemon

        result = install_daemon()
        click.echo(result["message"])
        if result.get("path"):
            click.echo(f"  Unit file: {result['path']}")

    @schedule.command("uninstall-daemon")
    def schedule_uninstall_daemon() -> None:
        """Remove the OS-level scheduler daemon."""
        from amx.scheduler.daemon_install import uninstall_daemon

        result = uninstall_daemon()
        click.echo(result["message"])


__all__ = ["register_schedule_commands"]
