"""Subscriber bus survives ``ThreadPoolExecutor`` fan-out.

Regression test for a user-reported "Live progress is not live" bug
on AMX Studio's run-detail page. Symptoms: only the parent-thread
spinner ("Profiling X.Y structure and data") reached the SSE bridge;
every per-agent / per-batch step emitted from a worker thread
(ProfileAgent batch fan-out, Orchestrator sub-agent pool, rerun pool)
silently dropped.

Root cause: the subscriber bus used to live on a
``threading.local``. ``ThreadPoolExecutor.submit`` spawns a worker
thread whose ``threading.local`` is empty, so subscribers installed
on the parent thread were invisible to ``_notify_subscribers``
running inside the worker.

Fix: the subscriber bus migrated to :mod:`contextvars`, and the
agent fan-out sites now use :func:`run_in_thread` which snapshots
``contextvars.copy_context()`` and submits ``ctx.run`` so the
subscriber tuple flows into the worker thread.

These tests pin both the propagation contract and the isolation
guarantee — a child push must not leak back into the parent's bus.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from amx.utils.console import step_spinner
from amx.utils.live_display import (
    get_display,
    pop_subscriber,
    push_subscriber,
    run_in_thread,
)


def _make_listener() -> tuple[list[tuple[str, dict]], object]:
    captured: list[tuple[str, dict]] = []

    def listener(event_type: str, payload: dict) -> None:
        captured.append((event_type, dict(payload)))

    return captured, listener


def test_subscriber_reaches_worker_thread_via_run_in_thread() -> None:
    """The whole point of the ContextVar migration: a subscriber
    installed on the parent thread must receive ``step.*`` events
    emitted *inside* a worker thread reached via ``run_in_thread``.
    Without this propagation, every per-batch / per-agent emit in the
    Studio run flow drops on the floor."""
    display = get_display()
    captured, listener = _make_listener()

    display.start_headless()
    push_subscriber(listener)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = run_in_thread(ex, _emit_batch_inside_worker, "Profile Agent batch 1/3")
            fut.result()
    finally:
        pop_subscriber(listener)
        display.stop_headless()

    types = [evt for evt, _ in captured]
    assert "step.added" in types
    assert "step.begin" in types
    assert "step.complete" in types
    add_payload = next(payload for evt, payload in captured if evt == "step.added")
    assert add_payload["label"] == "Profile Agent batch 1/3"


def test_raw_executor_submit_loses_subscriber() -> None:
    """Pinning the regression: a plain ``executor.submit`` (no
    ``run_in_thread`` wrapper) does NOT propagate the subscriber bus.

    This is the broken-by-default behaviour — every call site in
    ``profile_agent.py`` / ``orchestrator.py`` / ``_orchestrator/
    rerun.py`` must opt in to ``run_in_thread`` to honour the run-
    detail page's "Live progress" panel. Documents the contract so a
    future refactor that drops the wrapper trips this test instead of
    silently going dark in production."""
    display = get_display()
    captured, listener = _make_listener()

    display.start_headless()
    push_subscriber(listener)
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_emit_batch_inside_worker, "Profile Agent batch 2/3")
            fut.result()
    finally:
        pop_subscriber(listener)
        display.stop_headless()

    types = [evt for evt, _ in captured]
    assert "step.added" not in types
    assert "step.begin" not in types
    assert "step.complete" not in types


def test_child_push_does_not_leak_back_to_parent() -> None:
    """Isolation guarantee: a subscriber the worker pushes inside its
    own context must not reach back into the parent's bus once the
    worker returns. This is what kept multi-job concurrent runs from
    leaking each other's events under the old ``threading.local``
    model — preserve it under the ContextVar migration."""
    display = get_display()
    parent_captured, parent_listener = _make_listener()
    child_captured, child_listener = _make_listener()

    display.start_headless()
    push_subscriber(parent_listener)

    def worker() -> None:
        push_subscriber(child_listener)
        try:
            with step_spinner("Inside worker"):
                pass
        finally:
            pop_subscriber(child_listener)

    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = run_in_thread(ex, worker)
            fut.result()
        # After the worker returns, an emit on the parent thread must
        # NOT reach ``child_listener`` — its subscription lived only
        # inside the worker's copy of the context.
        with step_spinner("After worker"):
            pass
    finally:
        pop_subscriber(parent_listener)
        display.stop_headless()

    parent_types = [evt for evt, _ in parent_captured]
    # Parent saw both spinners: the worker's emits propagated UP the
    # subscriber tuple inherited from the parent, and the post-worker
    # spinner ran on the parent thread directly.
    assert parent_types.count("step.added") == 2
    # Child only saw its own spinner — never the post-worker one.
    child_types = [evt for evt, _ in child_captured]
    assert child_types.count("step.added") == 1
    assert "After worker" not in [p.get("label") for _, p in child_captured]


def _emit_batch_inside_worker(label: str) -> None:
    """Helper used by tests to emit a step.* sequence inside a thread.

    Defined at module scope so ``ThreadPoolExecutor`` can pickle-
    submit it cleanly across runs (a closure over a fixture would
    drag the test's local state into the worker)."""
    with step_spinner(label):
        pass
