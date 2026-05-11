"""Process-wide cancel-token propagation.

The orchestrator's table-level loop already checks
``cancel_token.is_set()`` between phases (profile fetch, filter chain,
agent fan-out, apply/review dispatch). That works for short tables —
the worker exits within a few seconds of the user clicking Cancel.
But on a slow LLM (Databricks Serving + a reasoning model can take 30-
60s per column) a single table's agent phase blocks the cancel signal
for many minutes, and the Studio user sees the row keep marching
forward long after they pressed the button.

The fix is to also check the cancel token **before each LLM call**, so
the next ``litellm.completion(...)`` is short-circuited. Threading the
token through every agent → provider call signature would touch
dozens of files; this module exposes the token as a ``ContextVar``
that the run worker sets once and the provider reads on each call.
Same pattern AMX already uses for the live-display subscriber bus
(:mod:`amx.utils.live_display`).

The provider raises
:class:`amx.agents.orchestrator.RunCancelled` when it observes the
token; the orchestrator's existing per-table try/except surfaces that
as the run's final ``cancelled`` status.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Generator
from contextvars import ContextVar

_active_cancel_token: ContextVar[threading.Event | None] = ContextVar(
    "amx_active_cancel_token", default=None
)


def get_active_cancel_token() -> threading.Event | None:
    """Return the cancel-token bound to the current task, or ``None``.

    Code paths that want to be cancellation-aware without an explicit
    parameter (e.g. :func:`amx.llm.provider.LLMProvider.complete`) read
    this and short-circuit when the token is set.
    """
    return _active_cancel_token.get()


def is_cancelled() -> bool:
    """``True`` when there is an active token AND it has been signalled."""
    tok = _active_cancel_token.get()
    return tok is not None and tok.is_set()


@contextlib.contextmanager
def bind_cancel_token(
    token: threading.Event | None,
) -> Generator[None, None, None]:
    """Bind ``token`` for the duration of the ``with`` block.

    The run worker wraps its main body with this so every nested
    agent / provider call inherits the binding. The token is restored
    on exit so a worker thread re-used by the next job sees the new
    job's cancel state, not the previous one's.

    Passing ``None`` is valid and explicitly clears any inherited
    binding — useful for tests that want to assert "no token was
    propagated".
    """
    sentinel = _active_cancel_token.set(token)
    try:
        yield
    finally:
        _active_cancel_token.reset(sentinel)


def raise_if_cancelled(*, phase: str = "llm call") -> None:
    """Raise :class:`RunCancelled` when the active token has been set.

    Lazy-imports the exception class so this module stays free of any
    dependency on the orchestrator package — keeps the provider's
    import graph clean and prevents a circular import.
    """
    if is_cancelled():
        from amx.agents.orchestrator import RunCancelled

        raise RunCancelled(f"Cancelled before {phase}")
