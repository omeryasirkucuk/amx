"""Headless one-shot tick entry-point for the scheduler daemon.

Wrapped as ``python -m amx.scheduler`` so the launchd / systemd
daemon plist can drive a single tick cycle without going through the
``amx`` shell entry (which is REPL-only on this install -- see the
"Direct subcommands are disabled" gate on ``amx``).

Behaviour:

1. Load ``AMXConfig`` honouring ``$AMX_CONFIG_DIR`` so the dev /
   prod isolation pattern keeps working (``amx`` vs ``amx-dev``).
2. Pull the dual-write history store and recover any stale rows
   left over from a crashed worker (tick does this internally as
   well, but we want a fresh sweep before the per-schedule loop).
3. Run one ``tick(source='daemon')`` pass with the production
   ``spawn_scheduled_worker`` injected, so a due schedule fires
   exactly as it would from the CLI / Studio.
4. Print a compact one-line summary on stdout (the plist captures
   stdout to ``$AMX_CONFIG_DIR/logs/scheduler.log`` so a quick
   ``tail -f`` confirms ticks are landing).

Exits 0 on success, 1 on any unexpected exception (the daemon
restarts on the next interval regardless; the exit code is for
human triage from the log).
"""

from __future__ import annotations

import logging
import sys
import time

from amx.config import AMXConfig
from amx.runtime.worker import production_run_executor, spawn_scheduled_worker
from amx.scheduler.tick import tick
from amx.storage.factory import init_history_store


def _spawn(payload: dict) -> int:
    """Daemon-side ``spawn_worker``: background-thread the production
    executor exactly like the Studio /run-now path does. Returns the
    new ``analysis_runs.id`` immediately so the tick loop can keep
    claiming further due schedules without waiting on the run."""
    return spawn_scheduled_worker(
        payload,
        store=hs,
        background=True,
        run_executor=production_run_executor,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("amx.scheduler.daemon")

    log.info("daemon tick start (pid=%s)", __import__("os").getpid())
    cfg = AMXConfig.load()
    global hs  # set on the module so _spawn closure can read it
    hs = init_history_store(cfg)
    if hs is None:
        log.error("history store unavailable; cannot tick.")
        return 1

    started = time.time()
    try:
        report = tick(
            store=hs,
            source="daemon",
            spawn_worker=_spawn,
            now_utc=started,
        )
    except Exception:  # noqa: BLE001
        log.exception("daemon tick crashed")
        return 1

    elapsed = time.time() - started
    log.info(
        "daemon tick complete in %.2fs: fired=%s failed=%s stale_recovered=%s",
        elapsed,
        report.fired,
        [r[0] for r in report.failed_resolution],
        report.stale_recovered,
    )
    # Give any background workers a moment to write their initial
    # heartbeat + analysis_runs row before this process exits.
    # Without the small grace period the OS can reap the worker
    # thread mid-INSERT, leaving a "running" row with no
    # last_heartbeat_at -- the very condition the next tick's
    # stale-recovery would (correctly) clean up, but we'd rather
    # never write it in the first place.
    if report.fired:
        time.sleep(1.5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
