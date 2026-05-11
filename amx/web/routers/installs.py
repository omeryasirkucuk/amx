"""SSE endpoint that streams the global ``pip.install.*`` event bus.

The bus itself (:mod:`amx.web.install_bus`) is process-global — installs
can be triggered from the foreground REPL, Studio's HTTP worker, or
either of those nesting via :mod:`amx.utils.optional_deps`. This router
just bridges the bus to one SSE connection per browser tab so AMX
Studio's banner can stay in sync regardless of which thread kicked off
the install.

Late subscribers (a tab opened mid-install) get the recent replay
buffer first, then enter the live drain loop. Unlike the per-job SSE
streams in :mod:`amx.web.routers.runs` this stream never closes on its
own — Studio holds the connection open for the lifetime of the page
and the server keepalive comment keeps proxies from reaping it.
"""

from __future__ import annotations

import json
import logging
import time
from queue import Empty, Queue
from typing import Any

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

from amx.web import install_bus

log = logging.getLogger("amx.web.installs")
router = APIRouter(prefix="/api", tags=["installs"])


@router.get("/installs/events")
def stream_install_events() -> EventSourceResponse:
    """SSE stream of every ``pip.install.*`` event the backend emits.

    Each event lands as one SSE frame whose ``event`` field is the
    discriminator (``pip.install.begin`` / ``progress`` / ``done`` /
    ``failed``) and whose ``data`` field is the JSON-encoded payload.
    """
    return EventSourceResponse(_event_generator())


def _event_generator():
    # Subscribe BEFORE replaying so any event published in the tiny
    # window between snapshot and subscribe is still delivered (we
    # de-dupe by id() since publish() fans the same dict into both the
    # replay buffer and every subscriber).
    queue: Queue[dict[str, Any]] = Queue()

    def _push(event: dict[str, Any]) -> None:
        try:
            queue.put_nowait(event)
        except Exception as exc:  # pragma: no cover - belt and braces
            log.debug("install events queue put failed: %s", exc)

    install_bus.subscribe(_push)

    try:
        replayed_ids: set[int] = set()
        for event in install_bus.snapshot_replay():
            replayed_ids.add(id(event))
            kind = str(event.get("type", ""))
            yield {"event": kind, "data": json.dumps(event)}

        last_keepalive = time.monotonic()
        while True:
            try:
                event = queue.get(timeout=15)
            except Empty:
                now = time.monotonic()
                if now - last_keepalive > 14:
                    yield {"event": "ping", "data": json.dumps({"t": now})}
                    last_keepalive = now
                continue
            if id(event) in replayed_ids:
                replayed_ids.discard(id(event))
                continue
            kind = str(event.get("type", ""))
            yield {"event": kind, "data": json.dumps(event)}
    finally:
        install_bus.unsubscribe(_push)
