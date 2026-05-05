"""Per-job event bus that backs AMX Studio's SSE streams.

The orchestrator + agents already accept ``on_progress`` /
``on_thinking`` callbacks (``Callable[..., None]``). The web layer wraps
those callbacks with thin adapters that push JSON-serializable dicts
onto a per-job :class:`queue.Queue`. The SSE endpoint (PR-C/D) drains
the queue with a short timeout, yielding ``data: {…}\n\n`` frames.

Events on the bus are plain dicts with a ``type`` discriminator. The
canonical event vocabulary (mirrored in the SPA's TypeScript types):

* ``activity.added`` ``{idx, label}``
* ``activity.begin`` ``{idx}``
* ``activity.complete`` ``{idx, detail}``
* ``activity.fail`` ``{idx, detail}``
* ``thinking.delta`` ``{text}``
* ``thinking.stop`` ``{}``
* ``writeback.progress`` ``{schema, table, column?, status, done, total, detail}``
* ``tokens`` ``{in, out, total}``
* ``tool.call`` ``{name, args}``
* ``tool.result`` ``{name, result_preview}``
* ``answer.final`` ``{answer, provenance, rows}``
* ``job.done`` ``{summary}``
* ``job.cancelled`` ``{}``
* ``job.failed`` ``{error}``

Workers must call :func:`emit_terminal` once at the end so the SSE
consumer knows to close the stream — otherwise the EventSource hangs
on the browser side until the keepalive cuts in.
"""

from __future__ import annotations

import logging
from queue import Queue
from typing import Any

log = logging.getLogger("amx.web.progress_bus")

#: Sentinel pushed when a job ends so the SSE generator can break
#: cleanly. Lives next to the events on the same queue rather than
#: needing a side-channel signalling primitive.
TERMINAL_EVENT_TYPES: frozenset[str] = frozenset({"job.done", "job.cancelled", "job.failed"})


def emit(queue: Queue, event_type: str, payload: dict[str, Any] | None = None) -> None:
    """Push one event onto the bus.

    ``payload`` is shallow-copied so the caller can keep mutating its
    own dict. Failures (a closed/full queue) are logged at debug level
    and dropped — we never want the worker to crash because the SSE
    consumer disconnected.
    """
    try:
        message: dict[str, Any] = {"type": event_type}
        if payload:
            message.update(payload)
        queue.put_nowait(message)
    except Exception as exc:  # pragma: no cover - belt-and-braces
        log.debug("progress_bus.emit dropped event %r: %s", event_type, exc)


def emit_terminal(queue: Queue, event_type: str, payload: dict[str, Any] | None = None) -> None:
    """Emit one of the terminal event types and assert the discriminator
    is one the SSE generator knows about.
    """
    if event_type not in TERMINAL_EVENT_TYPES:
        raise ValueError(
            f"emit_terminal called with non-terminal event {event_type!r}; "
            f"expected one of {sorted(TERMINAL_EVENT_TYPES)}"
        )
    emit(queue, event_type, payload)
