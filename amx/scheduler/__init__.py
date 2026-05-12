"""Scheduler engine for one-shot scheduled metadata runs.

The single public entry point is :func:`amx.scheduler.tick.tick` — a
stateless function that performs one pass over the local history
store: it fires due schedules, surfaces missed ones for review, and
recovers stale running runs. Three call sites:

* CLI bootstrap (every ``amx ...`` invocation, ``source='bootstrap'``)
* Studio process startup (lifespan hook, ``source='bootstrap'``)
* Cron / launchd / systemd timer (``source='daemon'``)
* Manual ``run-now`` from CLI or Studio (``source='manual'``)

The bootstrap source surfaces but does not auto-fire; the daemon and
manual sources auto-fire. This split is the explicit user-warning
contract — when AMX is closed, missed schedules wait for the user's
next session.
"""
