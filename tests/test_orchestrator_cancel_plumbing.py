"""Cancel-token plumbing through the orchestrator hot path.

This PR is mechanical — it adds a ``cancel_token`` keyword argument to
``Orchestrator.process_table``, ``Orchestrator.process_tables_batch_mode``,
and ``TableProcessor.__init__`` so the same :class:`threading.Event`
AMX Studio already exposes via ``Job.cancel`` propagates into the
per-table flow. Cooperative cancellation inside LLM calls and the
``ThreadPoolExecutor`` fan-out lands in the next PR.

The tests below assert plumbing-level invariants without spinning up
real DB / LLM clients:

* ``cancel_token`` survives ``Orchestrator.process_table → TableProcessor``.
* The new ``_check_cancel`` helper raises :class:`RunCancelled` when
  the token is set, and is silent when the token is unset or absent.
* Existing call sites that omit ``cancel_token`` keep the historical
  no-op behaviour (None default).
"""

from __future__ import annotations

import threading

import pytest

from amx.agents._orchestrator.table_processor import TableProcessor
from amx.agents.orchestrator import RunCancelled


def _make_processor(cancel_token: threading.Event | None = None) -> TableProcessor:
    """Build a TableProcessor without touching ``Orchestrator.__init__``.

    ``__init__`` only stores arguments on ``self`` — none of the phases
    we exercise here read from ``self.orch``, so passing a sentinel is
    safe for plumbing-level assertions.
    """
    return TableProcessor(
        orch=object(),  # not consumed by _check_cancel / __init__
        schema="public",
        table="transactions",
        cancel_token=cancel_token,
    )


def test_check_cancel_is_noop_when_token_is_none() -> None:
    proc = _make_processor(cancel_token=None)
    # Must not raise — historical CLI / inference paths pass nothing.
    proc._check_cancel(phase="profile_fetch")
    proc._check_cancel(phase="filters")
    proc._check_cancel(phase="agents")
    proc._check_cancel(phase="apply_or_review")


def test_check_cancel_is_noop_when_token_is_unset() -> None:
    token = threading.Event()
    assert not token.is_set()
    proc = _make_processor(cancel_token=token)
    proc._check_cancel(phase="profile_fetch")  # unset → no raise


def test_check_cancel_raises_when_token_is_set() -> None:
    token = threading.Event()
    token.set()
    proc = _make_processor(cancel_token=token)
    with pytest.raises(RunCancelled) as excinfo:
        proc._check_cancel(phase="agents")
    # Phase + (schema, table) must surface in the message so the SSE
    # consumer / log shows which boundary observed the cancel.
    assert "agents" in str(excinfo.value)
    assert "public.transactions" in str(excinfo.value)


def test_table_processor_default_cancel_token_is_none() -> None:
    """Constructor without an explicit cancel_token leaves it as None
    — backward-compat for every existing CLI call site."""
    proc = TableProcessor(orch=object(), schema="s", table="t")
    assert proc.cancel_token is None


def test_table_processor_stores_provided_cancel_token() -> None:
    token = threading.Event()
    proc = TableProcessor(orch=object(), schema="s", table="t", cancel_token=token)
    assert proc.cancel_token is token


def test_process_table_signature_accepts_cancel_token() -> None:
    """Smoke check that ``Orchestrator.process_table`` actually
    declares the new keyword argument — guards against a copy-paste
    refactor that re-introduces an old signature."""
    import inspect

    from amx.agents.orchestrator import Orchestrator

    params = inspect.signature(Orchestrator.process_table).parameters
    assert "cancel_token" in params
    assert params["cancel_token"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["cancel_token"].default is None


def test_process_tables_batch_mode_signature_accepts_cancel_token() -> None:
    import inspect

    from amx.agents.orchestrator import Orchestrator

    params = inspect.signature(Orchestrator.process_tables_batch_mode).parameters
    assert "cancel_token" in params
    assert params["cancel_token"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["cancel_token"].default is None
