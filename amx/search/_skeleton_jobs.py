"""Module-level cancel registry for in-flight skeleton syncs.

One :class:`threading.Event` per profile; ``cancel(profile)`` sets it
so the running sync loop in :mod:`amx.search.drift` can break at its
next checkpoint. Cooperative cancel only — the loop must reach a
checkpoint for the cancel to take effect.
"""

from __future__ import annotations

import threading

_jobs: dict[str, threading.Event] = {}
_lock = threading.RLock()


def register(profile: str) -> threading.Event:
    """Return (or create) the cancel event for ``profile``.

    Re-entry returns the existing event so a restart racing a cancel
    does not lose the signal.
    """
    with _lock:
        event = _jobs.get(profile)
        if event is None:
            event = threading.Event()
            _jobs[profile] = event
        return event


def cancel(profile: str) -> bool:
    """Set the cancel event for ``profile``.

    Returns ``True`` when a job was registered, ``False`` otherwise
    (nothing to cancel).
    """
    with _lock:
        event = _jobs.get(profile)
        if event is None:
            return False
        event.set()
        return True


def is_cancelled(profile: str) -> bool:
    """``True`` when ``cancel(profile)`` has been called for the
    currently registered job."""
    with _lock:
        event = _jobs.get(profile)
        return bool(event and event.is_set())


def unregister(profile: str) -> None:
    """Forget the cancel event for ``profile``. Safe to call when no
    job is registered."""
    with _lock:
        _jobs.pop(profile, None)


def running_profiles() -> list[str]:
    """Snapshot of profile names with a registered job."""
    with _lock:
        return list(_jobs.keys())
