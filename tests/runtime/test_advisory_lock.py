"""Tests for the per-(db_profile, schema, table) advisory lock helper.

These cover the three properties scheduler workers rely on:

* same-key contention serialises holders (no interleaving),
* distinct-key holders run in parallel (no incidental serialisation),
* acquire() honours its timeout and surfaces TimeoutError.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from amx.runtime.advisory_lock import AdvisoryLockStore


def test_lock_serialises_two_holders(tmp_path: Path) -> None:
    store = AdvisoryLockStore(str(tmp_path / "locks.sqlite3"))
    order: list[str] = []
    key = ("prod_sf", "public", "users")

    def hold(name: str, hold_sec: float) -> None:
        with store.acquire(key, timeout_sec=5.0):
            order.append(f"{name}-enter")
            time.sleep(hold_sec)
            order.append(f"{name}-exit")

    t1 = threading.Thread(target=hold, args=("A", 0.2))
    t2 = threading.Thread(target=hold, args=("B", 0.05))
    t1.start()
    # Give A a head start so the test is deterministic about who wins
    # the lock; B should still be forced to wait for A's exit.
    time.sleep(0.02)
    t2.start()
    t1.join()
    t2.join()

    assert order == ["A-enter", "A-exit", "B-enter", "B-exit"], (
        f"expected B to wait for A's exit; got: {order}"
    )


def test_lock_distinct_keys_run_in_parallel(tmp_path: Path) -> None:
    store = AdvisoryLockStore(str(tmp_path / "locks.sqlite3"))
    intervals: list[tuple[float, float]] = []
    intervals_lock = threading.Lock()
    # The barrier guarantees both threads have actually acquired their
    # (distinct) locks before either starts its in-lock sleep, so the
    # overlap test below measures lock concurrency rather than thread
    # scheduling jitter. The previous version of the test relied on
    # thread-start timing alone and was flaky on loaded CI runners
    # where the OS could fully run one thread before scheduling the
    # other (PR #476 + this PR follow-up).
    rendezvous = threading.Barrier(2, timeout=5.0)

    def hold(key: tuple[str, str, str]) -> None:
        with store.acquire(key, timeout_sec=5.0):
            rendezvous.wait()
            entered = time.monotonic()
            time.sleep(0.2)
            exited = time.monotonic()
        with intervals_lock:
            intervals.append((entered, exited))

    t1 = threading.Thread(target=hold, args=(("prod_sf", "public", "a"),))
    t2 = threading.Thread(target=hold, args=(("prod_sf", "public", "b"),))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Distinct keys must NOT serialise. The barrier ensures both
    # threads were holding their lock simultaneously before either
    # entered the sleep, so the in-lock interval overlap proves the
    # store does not serialise unrelated keys. Serial execution would
    # have raised BrokenBarrierError on timeout because the second
    # thread could never reach the barrier while the first holds it.
    assert len(intervals) == 2
    (entered_a, exited_a), (entered_b, exited_b) = intervals
    overlap = max(0.0, min(exited_a, exited_b) - max(entered_a, entered_b))
    assert overlap > 0.15, (
        f"distinct keys should run concurrently; overlap was {overlap:.3f}s "
        f"(entered {entered_a:.3f}/{entered_b:.3f}, exited {exited_a:.3f}/{exited_b:.3f})"
    )


def test_lock_timeout_raises(tmp_path: Path) -> None:
    store = AdvisoryLockStore(str(tmp_path / "locks.sqlite3"))
    key = ("prod_sf", "public", "users")
    holder_release = threading.Event()
    holder_acquired = threading.Event()

    def hold_long() -> None:
        with store.acquire(key, timeout_sec=5.0):
            holder_acquired.set()
            # Hold long enough for the contender's timeout to elapse.
            holder_release.wait(timeout=2.0)

    t = threading.Thread(target=hold_long)
    t.start()
    assert holder_acquired.wait(timeout=2.0), "holder failed to acquire its lock"

    with pytest.raises(TimeoutError):
        with store.acquire(key, timeout_sec=0.2):
            pass

    holder_release.set()
    t.join()


def test_release_lets_subsequent_acquire_succeed(tmp_path: Path) -> None:
    """After a holder exits, a fresh acquire on the same key must succeed."""
    store = AdvisoryLockStore(str(tmp_path / "locks.sqlite3"))
    key = ("prod_sf", "public", "users")

    with store.acquire(key, timeout_sec=2.0):
        pass

    # Second acquisition should not be blocked by the released row.
    with store.acquire(key, timeout_sec=2.0):
        pass
