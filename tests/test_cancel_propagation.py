"""``amx.utils.cancel`` propagates the run's cancel token to nested
LLM calls so a Studio Cancel click short-circuits the next
``litellm.completion(...)`` instead of waiting for the whole agent
fan-out to finish.

Three behaviours pinned here:

1. The ContextVar starts unbound — code that didn't opt-in stays
   silent (matches the CLI's programmatic-call shape).
2. ``bind_cancel_token`` makes the token visible to nested code AND
   restores the previous binding on exit. Critical for worker threads
   that are re-used across jobs.
3. ``raise_if_cancelled`` raises ``RunCancelled`` when the bound
   token has been set, and is a no-op otherwise. The provider's
   retry loop uses this on every attempt to short-circuit the next
   call.
"""

from __future__ import annotations

import threading

from amx.utils.cancel import (
    bind_cancel_token,
    get_active_cancel_token,
    is_cancelled,
    raise_if_cancelled,
)


def test_no_token_by_default():
    """A fresh process starts with no binding — preserves the
    no-cancellation behaviour for callers that never installed one."""
    assert get_active_cancel_token() is None
    assert is_cancelled() is False
    # ``raise_if_cancelled`` is a no-op when no token is bound.
    raise_if_cancelled(phase="test")


def test_bind_cancel_token_makes_it_visible_inside_block():
    token = threading.Event()
    with bind_cancel_token(token):
        assert get_active_cancel_token() is token
    # Restored on exit.
    assert get_active_cancel_token() is None


def test_bind_cancel_token_restores_previous_binding():
    outer = threading.Event()
    inner = threading.Event()
    with bind_cancel_token(outer):
        assert get_active_cancel_token() is outer
        with bind_cancel_token(inner):
            assert get_active_cancel_token() is inner
        # Outer binding restored, NOT cleared to None.
        assert get_active_cancel_token() is outer
    assert get_active_cancel_token() is None


def test_is_cancelled_reflects_set_state():
    token = threading.Event()
    with bind_cancel_token(token):
        assert is_cancelled() is False
        token.set()
        assert is_cancelled() is True


def test_raise_if_cancelled_raises_RunCancelled_when_set():
    from amx.agents.orchestrator import RunCancelled

    token = threading.Event()
    token.set()
    with bind_cancel_token(token):
        try:
            raise_if_cancelled(phase="probe")
        except RunCancelled as exc:
            assert "probe" in str(exc)
        else:
            raise AssertionError("RunCancelled was not raised")


def test_raise_if_cancelled_silent_when_token_unset():
    """Token bound but unset → still a no-op. Matches the path the
    provider takes when no Cancel button has been clicked."""
    token = threading.Event()
    with bind_cancel_token(token):
        # Should NOT raise.
        raise_if_cancelled(phase="probe")


def test_bind_with_none_clears_inherited_binding():
    """Useful for tests / nested contexts that want to assert
    "no cancel token visible here". Passing ``None`` is an explicit
    clear, not a re-use of the parent binding."""
    outer = threading.Event()
    outer.set()
    with bind_cancel_token(outer):
        assert is_cancelled() is True
        with bind_cancel_token(None):
            # Cleared inside the inner block.
            assert get_active_cancel_token() is None
            assert is_cancelled() is False
        # Outer binding restored.
        assert is_cancelled() is True
