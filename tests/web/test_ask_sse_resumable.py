"""``/ask`` SSE resumable-stream regression.

The user reported that long ``kimi-k2.6`` thinking runs rendered
their reasoning panel as garbled fragments — words split mid-token,
half the chunks missing. Tracing showed two effects compounding:

1. Some intermediary (reverse-proxy idle timeout, browser
   EventSource auto-drop, …) closes the SSE every ~30 s during a
   slow LLM thinking phase.
2. The browser auto-reconnects with a fresh ``EventSource``. The
   previous backend ``_event_generator`` was a single-consumer
   ``queue.get`` drain, so the old generator was still parked taking
   half the events while the new one took the other half. The SPA's
   current connection only rendered HER half — hence the garble.

The fix turns the SSE generator into a cursor reader over the
``BufferedQueue`` replay buffer, stamps every frame with ``id:
<seq>``, and honours ``Last-Event-ID`` on reconnect. This module
pins those guarantees:

* Each yielded frame carries an ``id`` matching the producer's
  monotonic sequence.
* A reconnecting consumer that passes ``last_event_id`` skips events
  it has already seen and resumes from the right offset.
* Two concurrent consumers see the FULL ordered history (the legacy
  race is gone).
"""

from __future__ import annotations

import json

from amx.web.jobs import Job
from amx.web.progress_bus import emit, emit_terminal
from amx.web.routers.ask import _event_generator


def _parse(frame: dict) -> tuple[str, str, dict]:
    """Return ``(id, event_type, parsed_data)`` for one yielded frame."""
    return (
        str(frame.get("id") or ""),
        str(frame.get("event") or ""),
        json.loads(frame.get("data") or "{}"),
    )


def test_event_generator_emits_id_field_for_every_frame() -> None:
    """Browsers populate ``Last-Event-ID`` only when prior events
    carried ``id:``. Every non-keepalive frame the generator emits
    MUST have one or auto-reconnect cannot resume."""
    job = Job(id="ask-x", kind="ask")
    job.status = "running"
    emit(job.queue, "activity.added", {"idx": 0, "label": "Thinking"})
    emit(job.queue, "thinking.delta", {"text": "Hello"})
    emit_terminal(job.queue, "job.done", {"summary": {"ok": True}})

    frames = list(_event_generator(job, last_event_id=0))
    parsed = [_parse(f) for f in frames]
    # The terminal frame closes the generator; we expect exactly the
    # three emitted events in order.
    assert [event_type for _, event_type, _ in parsed] == [
        "activity.added",
        "thinking.delta",
        "job.done",
    ]
    assert [seq_id for seq_id, _, _ in parsed] == ["1", "2", "3"]


def test_last_event_id_skips_already_seen_events() -> None:
    """A reconnecting browser's ``Last-Event-ID`` skips the prefix
    its previous connection had already received. The resumed stream
    must NOT re-yield those frames."""
    job = Job(id="ask-y", kind="ask")
    job.status = "running"
    for text in ("a", "b", "c"):
        emit(job.queue, "thinking.delta", {"text": text})
    emit_terminal(job.queue, "job.done", {})

    frames = list(_event_generator(job, last_event_id=2))
    parsed = [_parse(f) for f in frames]
    # Skipped 1 and 2; sees 3 (third delta) and 4 (terminal).
    assert [seq_id for seq_id, _, _ in parsed] == ["3", "4"]
    assert parsed[0][2]["text"] == "c"
    assert parsed[-1][1] == "job.done"


def test_two_concurrent_consumers_see_full_history() -> None:
    """The smoking-gun regression: previously the second consumer
    drained half the chunks while the SPA's current EventSource saw
    only the other half. With the cursor-based reader BOTH consumers
    must see the entire ordered event history."""
    job = Job(id="ask-z", kind="ask")
    job.status = "running"
    for text in ("alpha", "beta", "gamma"):
        emit(job.queue, "thinking.delta", {"text": text})
    emit_terminal(job.queue, "job.done", {})

    gen_a = list(_event_generator(job, last_event_id=0))
    gen_b = list(_event_generator(job, last_event_id=0))
    # Both consumers must observe the same event types in the same order.
    types_a = [_parse(f)[1] for f in gen_a]
    types_b = [_parse(f)[1] for f in gen_b]
    assert (
        types_a
        == types_b
        == [
            "thinking.delta",
            "thinking.delta",
            "thinking.delta",
            "job.done",
        ]
    )
    # And every frame in both streams must carry an id, so a future
    # disconnect of either consumer can resume cleanly.
    assert all(seq_id for seq_id, _, _ in (_parse(f) for f in gen_a))
    assert all(seq_id for seq_id, _, _ in (_parse(f) for f in gen_b))
