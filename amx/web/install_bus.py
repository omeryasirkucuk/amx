"""Process-global bus for ``pip.install.*`` lifecycle events.

The on-demand installer in :mod:`amx.utils.optional_deps` runs from
any of three places — the foreground REPL, Studio's FastAPI worker,
and the test suite — and any of them may trigger an install while the
others are observing. The progress UI on both sides (Rich spinner in
the CLI, sticky banner in Studio) needs to see those events without
caring which entry point started the install.

That makes this bus *process-global* rather than per-job: subscribers
register once at process start (e.g. the SSE endpoint, when Studio is
serving) and stay subscribed for the lifetime of the connection. The
existing :mod:`amx.web.progress_bus` is per-job + per-queue and would
require threading a queue all the way down through ``ensure()``, which
adds an awkward coupling between feature code and the web layer.

Event vocabulary (kept narrow on purpose — the install path is simple
and we'd rather the consumer key off a handful of well-defined types
than a free-form text stream):

* ``pip.install.begin`` — ``{install_id, feature, packages}``
* ``pip.install.progress`` — ``{install_id, phase, **details}`` where
  ``phase`` is one of ``"collecting"``, ``"downloading"``,
  ``"installing"``, ``"tail"``. The ``"tail"`` phase carries the raw
  pip line in ``line`` for UIs that want a live stream.
* ``pip.install.done`` — ``{install_id, feature, packages, elapsed_s,
  installed}``
* ``pip.install.failed`` — ``{install_id, feature, packages,
  elapsed_s, returncode, tail}`` (``tail`` is the last ~20 lines of
  captured pip output)
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Any

log = logging.getLogger("amx.web.install_bus")

Event = dict[str, Any]
Subscriber = Callable[[Event], None]

#: Maximum events kept for replay so a late-subscribing browser tab
#: still sees an install that started a few seconds before it loaded
#: the page. 200 lines × ~80 bytes each ≈ 16 KB worst-case, negligible.
_REPLAY_MAX = 200

_lock = threading.Lock()
_subscribers: list[Subscriber] = []
_replay: deque[Event] = deque(maxlen=_REPLAY_MAX)


def subscribe(callback: Subscriber) -> None:
    """Register a callback that fires for every published event.

    Callbacks run on whatever thread published the event (usually the
    pip-drain thread inside ``optional_deps``). They must be
    non-blocking and exception-safe; raised exceptions are logged at
    debug level and swallowed so one bad subscriber cannot break the
    bus for the others.
    """
    with _lock:
        _subscribers.append(callback)


def unsubscribe(callback: Subscriber) -> None:
    with _lock:
        try:
            _subscribers.remove(callback)
        except ValueError:
            pass


def snapshot_replay() -> list[Event]:
    """Return a shallow copy of recent events for late subscribers.

    The SSE endpoint calls this once at connection time, emits each
    event as the initial backlog, then enters the live-subscribe loop.
    """
    with _lock:
        return list(_replay)


def publish(event_type: str, payload: dict[str, Any] | None = None) -> Event:
    """Fan out an event to every subscriber and append to the replay
    buffer.

    The published dict is the same object returned to the caller —
    handy for tests that want to assert on what was sent.
    """
    event: Event = {"type": event_type, "ts": time.time()}
    if payload:
        event.update(payload)
    with _lock:
        _replay.append(event)
        subs = list(_subscribers)
    for sub in subs:
        try:
            sub(event)
        except Exception as exc:  # pragma: no cover — defensive
            log.debug("install_bus subscriber raised: %s", exc)
    return event


def reset_for_tests() -> None:
    """Clear all subscribers + replay. Test fixtures only."""
    with _lock:
        _subscribers.clear()
        _replay.clear()
