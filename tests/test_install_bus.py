"""Tests for the process-global install event bus.

The bus is the seam between :mod:`amx.utils.optional_deps` (which runs
pip on a worker thread) and the SSE endpoint that drives Studio's
install banner. Three properties matter:

1. Subscribers receive every published event, in order.
2. Late subscribers can snapshot a recent replay so a banner that
   loads mid-install still renders the current phase.
3. Subscribe / unsubscribe / publish are safe under concurrent threads
   — the install thread publishing while a SSE thread subscribes must
   not corrupt the subscriber list.
"""

from __future__ import annotations

import threading

import pytest

from amx.web import install_bus


@pytest.fixture(autouse=True)
def _reset_bus() -> None:
    install_bus.reset_for_tests()
    yield
    install_bus.reset_for_tests()


def test_subscribe_receives_published_events_in_order() -> None:
    received: list[dict] = []
    install_bus.subscribe(received.append)

    install_bus.publish("pip.install.begin", {"feature": "x", "packages": ["a"]})
    install_bus.publish("pip.install.progress", {"phase": "collecting", "package": "a"})
    install_bus.publish("pip.install.done", {"feature": "x"})

    assert [e["type"] for e in received] == [
        "pip.install.begin",
        "pip.install.progress",
        "pip.install.done",
    ]
    # Payload merged onto the type discriminator.
    assert received[0]["feature"] == "x"
    assert received[0]["packages"] == ["a"]


def test_unsubscribe_stops_delivery() -> None:
    received: list[dict] = []
    install_bus.subscribe(received.append)
    install_bus.publish("pip.install.begin", {"feature": "x"})
    install_bus.unsubscribe(received.append)
    install_bus.publish("pip.install.done", {"feature": "x"})
    assert len(received) == 1


def test_snapshot_replay_returns_recent_events() -> None:
    install_bus.publish("pip.install.begin", {"feature": "f"})
    install_bus.publish("pip.install.progress", {"phase": "tail", "line": "..."})
    snapshot = install_bus.snapshot_replay()
    assert [e["type"] for e in snapshot] == [
        "pip.install.begin",
        "pip.install.progress",
    ]


def test_replay_buffer_caps_to_max() -> None:
    # Push more than the cap and verify older events are evicted FIFO.
    for i in range(install_bus._REPLAY_MAX + 50):
        install_bus.publish("pip.install.progress", {"i": i})
    snap = install_bus.snapshot_replay()
    assert len(snap) == install_bus._REPLAY_MAX
    # The oldest 50 events should be gone; first remaining event is i=50.
    assert snap[0]["i"] == 50


def test_subscriber_exception_does_not_break_bus() -> None:
    received: list[dict] = []

    def bad_sub(_event: dict) -> None:
        raise RuntimeError("boom")

    install_bus.subscribe(bad_sub)
    install_bus.subscribe(received.append)
    install_bus.publish("pip.install.begin", {"feature": "x"})
    # The good subscriber must still have received the event despite
    # the bad one raising.
    assert len(received) == 1


def test_thread_safety_under_concurrent_publish_and_subscribe() -> None:
    received: list[dict] = []
    received_lock = threading.Lock()

    def safe_append(evt: dict) -> None:
        with received_lock:
            received.append(evt)

    install_bus.subscribe(safe_append)

    stop = threading.Event()

    def publisher() -> None:
        i = 0
        while not stop.is_set():
            install_bus.publish("pip.install.progress", {"i": i})
            i += 1

    def churner() -> None:
        # Subscribe and immediately unsubscribe a no-op callback to
        # exercise the lock under contention.
        while not stop.is_set():
            cb = lambda _e: None  # noqa: E731
            install_bus.subscribe(cb)
            install_bus.unsubscribe(cb)

    threads = [
        threading.Thread(target=publisher),
        threading.Thread(target=churner),
        threading.Thread(target=publisher),
    ]
    for t in threads:
        t.start()
    threading.Event().wait(0.2)
    stop.set()
    for t in threads:
        t.join(timeout=2.0)

    # No exception bubbled out → bus survived. We don't assert on a
    # specific count because the publishers race; only that the
    # subscriber recorded some events without crashing.
    assert len(received) > 0
