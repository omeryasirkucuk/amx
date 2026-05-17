"""``BufferedQueue.tail_from`` regression.

Pins the multi-consumer-safe cursor read that backs the resumable
SSE pipeline. The legacy ``queue.Queue.get`` API allowed two
concurrent SSE generators to race over the same event log, with each
consumer taking roughly half the in-flight chunks; the visible
symptom was a Studio Thinking panel rendering only every other
fragment after a reconnect. ``tail_from`` replaces that one-shot
drain with a cursor that each consumer advances independently, so a
reconnecting browser can replay missed events without stealing from
the still-attached generator.
"""

from __future__ import annotations

import threading
import time

from amx.web.jobs import BufferedQueue


def test_tail_from_returns_none_on_timeout_with_empty_buffer() -> None:
    bq = BufferedQueue()
    started = time.monotonic()
    result = bq.tail_from(seq=0, timeout=0.1)
    elapsed = time.monotonic() - started
    assert result is None
    # Should actually have slept ~0.1s, not exited synchronously.
    assert elapsed >= 0.08


def test_tail_from_returns_immediately_when_events_present() -> None:
    bq = BufferedQueue()
    bq.put_nowait({"type": "a"})
    bq.put_nowait({"type": "b"})
    bq.put_nowait({"type": "c"})

    result = bq.tail_from(seq=0, timeout=10)
    assert result is not None
    assert [seq for seq, _ in result] == [1, 2, 3]
    assert [event["type"] for _, event in result] == ["a", "b", "c"]


def test_tail_from_honors_cursor() -> None:
    """A consumer that has already seen up to seq=2 must skip those
    events and only receive seq>2 on the next read."""
    bq = BufferedQueue()
    for letter in "abcd":
        bq.put_nowait({"type": letter})

    result = bq.tail_from(seq=2, timeout=10)
    assert result is not None
    assert [seq for seq, _ in result] == [3, 4]
    assert [event["type"] for _, event in result] == ["c", "d"]


def test_tail_from_wakes_on_new_event() -> None:
    """A consumer parked in ``tail_from`` must wake as soon as a
    producer ``put_nowait``s a new event — that is the path the SSE
    generator relies on to deliver thinking deltas without polling."""
    bq = BufferedQueue()
    seen: list[tuple[int, dict]] = []

    def _consumer() -> None:
        result = bq.tail_from(seq=0, timeout=2.0)
        if result:
            seen.extend(result)

    consumer = threading.Thread(target=_consumer, daemon=True)
    consumer.start()
    # Give the consumer time to actually block on the condition.
    time.sleep(0.05)
    bq.put_nowait({"type": "live"})
    consumer.join(timeout=1.0)

    assert not consumer.is_alive(), "consumer should have returned after notify"
    assert seen == [(1, {"type": "live"})]


def test_two_concurrent_consumers_each_see_all_events() -> None:
    """The bug the resumable SSE fix targets: two consumers reading
    from the same source must each see the FULL event history,
    independent of the other's progress. The legacy ``queue.get``
    consumer would split events 50/50; the new cursor read guarantees
    completeness for every consumer."""
    bq = BufferedQueue()
    for i in range(10):
        bq.put_nowait({"type": "delta", "i": i})

    snap_a = bq.tail_from(seq=0, timeout=1.0)
    snap_b = bq.tail_from(seq=0, timeout=1.0)
    assert snap_a is not None and snap_b is not None
    # Both consumers receive every event with stable sequence numbers.
    assert [seq for seq, _ in snap_a] == list(range(1, 11))
    assert [seq for seq, _ in snap_b] == list(range(1, 11))


def test_tail_from_ignores_non_dict_events() -> None:
    """``put_nowait`` historically accepted arbitrary objects via the
    Queue parent; the refactor narrows the contract to dicts. Anything
    else is silently dropped so a producer mistake cannot corrupt the
    buffer_seq counter that the resumable SSE design depends on."""
    bq = BufferedQueue()
    bq.put_nowait("not-a-dict")
    bq.put_nowait({"type": "real"})
    bq.put_nowait(42)

    result = bq.tail_from(seq=0, timeout=0.5)
    assert result is not None
    assert result == [(1, {"type": "real"})]
