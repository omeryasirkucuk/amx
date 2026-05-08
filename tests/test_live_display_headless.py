"""Headless ``LiveDisplay`` activation for AMX Studio's run worker.

The CLI runs ``command_display(...)`` which starts a Rich ``Live``
panel and flips ``LiveDisplay.is_active`` to True; ``step_spinner``
then emits ``step.*`` subscriber events for every batch / agent
step. The Studio worker can't paint a Rich ``Live`` (the parent CLI
terminal already prints a "Studio running" banner and is
``quiet_console``-silenced) but it still needs the same per-batch
narration to flow over SSE so the run-detail page shows what the
worker is doing.

These tests pin the headless contract: ``start_headless`` activates
the state machine + subscriber bus without painting, ``step_spinner``
emits ``step.added`` / ``step.begin`` / ``step.complete`` for the
SSE bridge to forward, and ``stop_headless`` cleanly turns the flag
off so a future run isn't poisoned by stale state.
"""

from __future__ import annotations

from amx.utils import live_display
from amx.utils.console import step_spinner
from amx.utils.live_display import (
    get_display,
    pop_subscriber,
    push_subscriber,
)


def _capture_events() -> tuple[list[tuple[str, dict]], callable]:
    captured: list[tuple[str, dict]] = []

    def listener(event_type: str, payload: dict) -> None:
        captured.append((event_type, dict(payload)))

    return captured, listener


def test_headless_start_flips_is_active_without_starting_rich_live() -> None:
    """``start_headless`` must report active so step_spinner emits, but
    must not start the underlying Rich ``Live`` widget that would
    paint on the parent CLI terminal."""
    display = get_display()
    assert display.is_active is False  # baseline
    assert display._live is None

    display.start_headless(mode="analyze.run", provider="openai", model="gpt-4o")
    try:
        assert display.is_active is True
        # The Rich Live widget must remain None — that's what keeps
        # the parent CLI terminal clean while Studio is mid-run.
        assert display._live is None
    finally:
        display.stop_headless()

    assert display.is_active is False


def test_step_spinner_emits_step_events_under_headless() -> None:
    """The whole point of headless mode: step_spinner emits the same
    add/begin/complete events the SSE bridge listens for."""
    display = get_display()
    captured, listener = _capture_events()
    display.start_headless()
    push_subscriber(listener)
    try:
        with step_spinner("Profile Agent batch 1/3", token_estimate=120):
            pass  # the inner work is irrelevant; we want the spinner's events
    finally:
        pop_subscriber(listener)
        display.stop_headless()

    types = [evt for evt, _ in captured]
    assert "step.added" in types
    assert "step.begin" in types
    assert "step.complete" in types
    # The label payload carries the same string the CLI Live panel
    # would show — so the run-detail page renders "Now: Profile
    # Agent batch 1/3".
    add_payload = next(payload for evt, payload in captured if evt == "step.added")
    assert add_payload["label"] == "Profile Agent batch 1/3"
    complete_payload = next(payload for evt, payload in captured if evt == "step.complete")
    assert complete_payload["label"] == "Profile Agent batch 1/3"


def test_step_spinner_silent_when_neither_live_nor_headless() -> None:
    """The default state: with no Rich Live and no headless, the
    spinner falls through to its CLI-fallback path and does NOT emit
    subscriber events. (Pre-fix behaviour. Documented here so a
    future refactor doesn't silently re-flip is_active and start
    spamming SSE during ordinary CLI work.)"""
    display = get_display()
    assert display.is_active is False

    captured, listener = _capture_events()
    push_subscriber(listener)
    try:
        with step_spinner("Should not emit"):
            pass
    finally:
        pop_subscriber(listener)

    assert all(evt not in ("step.added", "step.begin", "step.complete") for evt, _ in captured)


def test_headless_stop_is_idempotent() -> None:
    """A worker calling stop_headless() in finally even when start
    failed earlier must not raise."""
    display = get_display()
    display.stop_headless()  # never started — should be a no-op
    display.start_headless()
    display.stop_headless()
    display.stop_headless()  # second call also fine
    assert display.is_active is False


def test_headless_resets_state_so_runs_are_isolated() -> None:
    """A second worker that calls start_headless() must see a clean
    activity list / token totals — leftover state from a previous run
    would leak into the next run's SSE feed."""
    display = get_display()

    display.start_headless()
    try:
        display.add_activity("first run step")
        display.add_session_tokens(input_tokens=10, output_tokens=5)
    finally:
        display.stop_headless()

    display.start_headless()
    try:
        # Activities cleared, token counters reset.
        assert display._activities == []
        assert display._total_tokens_in == 0
        assert display._total_tokens_out == 0
    finally:
        display.stop_headless()


def teardown_module(module) -> None:  # pragma: no cover - cleanup
    """Belt-and-braces: drain the per-thread subscriber bus + reset
    headless state so a stray failure in the suite above doesn't
    poison subsequent module-import tests."""
    bus = getattr(live_display, "_thread_subscribers", None)
    if bus is not None and hasattr(bus, "value"):
        bus.value = []
    display = get_display()
    display.stop_headless()
