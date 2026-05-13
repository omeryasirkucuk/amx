"""``amx scheduler`` command group — engine and daemon controls.

* ``amx scheduler tick``    — one stateless pass; used by the daemon cron.
* ``amx scheduler status``  — daemon presence + last-tick + pending count.
* ``amx scheduler install-daemon`` — launchd / systemd setup (phase 4).
* ``amx scheduler uninstall-daemon`` — remove the daemon (phase 4).
"""

from __future__ import annotations

import json
import platform
import time
from collections.abc import Callable
from typing import Any

import click

from amx.runtime.worker import spawn_scheduled_worker
from amx.scheduler.tick import tick
from amx.storage.sqlite_store import history_store

LogEvent = Callable[..., None]


def _require_store():
    hs = history_store()
    if hs is None:
        raise click.ClickException(
            "history store not initialised (run any other amx command first)"
        )
    return hs


def register_scheduler_commands(
    main: click.Group,
    *,
    log_event: LogEvent | None = None,
) -> None:
    """Attach the ``amx scheduler`` group to *main*."""

    @main.group("scheduler")
    def scheduler() -> None:
        """Scheduler engine + daemon controls."""

    @scheduler.command("tick")
    @click.option(
        "--silent",
        is_flag=True,
        default=False,
        help="Suppress per-fire stdout; used by the daemon cron entry.",
    )
    def scheduler_tick(silent: bool) -> None:
        """Run one stateless scheduling pass (fires due schedules).

        Safe to call from cron-like timers; idempotent. The tick
        recovers stale running rows, then fires every due schedule
        in order until the per-tick cap is reached.
        """
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

    @scheduler.command("status")
    def scheduler_status() -> None:
        """Show scheduler + daemon status."""
        hs = _require_store()
        pending = hs.list_scheduled_runs(statuses=["pending"], limit=1000)
        missed = hs.list_scheduled_runs(statuses=["missed"], limit=1000)
        paused = hs.list_scheduled_runs(statuses=["paused"], limit=1000)
        next_fire = pending[0] if pending else None

        from amx.scheduler.daemon_install import detect_daemon_state

        daemon = detect_daemon_state()

        click.echo("Scheduler status")
        click.echo(f"  Pending schedules: {len(pending)}")
        click.echo(f"  Paused schedules:  {len(paused)}")
        click.echo(f"  Missed schedules:  {len(missed)}")
        if next_fire:
            click.echo(
                f"  Next fire: #{next_fire['id']} '{next_fire['name']}' at "
                f"fire_at_utc={next_fire['fire_at_utc']:.0f}"
            )
        click.echo("")
        click.echo("Daemon")
        click.echo(f"  Platform: {platform.system()}")
        click.echo(f"  Installed: {daemon['installed']}")
        if daemon.get("path"):
            click.echo(f"  Unit file: {daemon['path']}")
        if daemon.get("last_tick_log"):
            click.echo(f"  Log:       {daemon['last_tick_log']}")

    @scheduler.command("install-daemon")
    def scheduler_install_daemon() -> None:
        """Install the OS-level scheduler daemon (launchd or systemd)."""
        from amx.scheduler.daemon_install import install_daemon

        result = install_daemon()
        click.echo(result["message"])
        if result.get("path"):
            click.echo(f"  Unit file: {result['path']}")

    @scheduler.command("uninstall-daemon")
    def scheduler_uninstall_daemon() -> None:
        """Remove the OS-level scheduler daemon."""
        from amx.scheduler.daemon_install import uninstall_daemon

        result = uninstall_daemon()
        click.echo(result["message"])


__all__ = ["register_scheduler_commands"]
