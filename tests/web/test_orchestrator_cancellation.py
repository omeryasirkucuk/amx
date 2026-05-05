"""Orchestrator cancellation-token plumbing — the additive patch
that lets AMX Studio's "Cancel job" button stop work between
rows of an /apply loop without rolling back already-written
COMMENTs.

We exercise the patch directly against a stub DB connector +
SQLAlchemy engine context manager. Three properties matter:

1. The token is passed through; ``apply_review_results_to_db``
   doesn't crash when the kwarg is omitted (back-compat with old
   callers).
2. When the token is set BEFORE the loop starts, the function
   returns 0 without writing anything.
3. When the token flips MID-LOOP, the function commits-what-was-
   applied and returns the partial count.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from amx.agents.base import Confidence
from amx.agents.orchestrator import (
    ReviewResult,
    RunCancelled,
    apply_review_results_to_db,
)


def _result(idx: int) -> ReviewResult:
    return ReviewResult(
        schema="sales",
        table="orders",
        column=f"col_{idx}",
        final_description=f"description {idx}",
        confidence=Confidence.HIGH,
        source="manual",
        applied=True,
        asset_kind="table",
    )


def _stub_db():
    """Minimal :class:`DatabaseConnector` stub: ``engine.begin()``
    is a context manager, ``apply_comment`` records each call so the
    test can assert on the order/count, and ``apply_column_comments_batch``
    returns False so the per-row path runs (one assertion per row)."""
    db = MagicMock()
    db.engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    db.engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    db.apply_column_comments_batch.return_value = False
    return db


def test_no_cancel_token_runs_to_completion() -> None:
    db = _stub_db()
    results = [_result(i) for i in range(3)]
    applied = apply_review_results_to_db(db, results)
    assert applied == 3
    assert db.apply_comment.call_count == 3


def test_pre_set_cancel_token_skips_every_row() -> None:
    db = _stub_db()
    token = threading.Event()
    token.set()
    applied = apply_review_results_to_db(db, [_result(i) for i in range(3)], cancel_token=token)
    assert applied == 0
    assert db.apply_comment.call_count == 0


def test_mid_loop_cancel_commits_partial_progress() -> None:
    db = _stub_db()
    token = threading.Event()

    call_count = {"n": 0}

    def stage_apply_comment(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            # Flip the token between the second and third row.
            token.set()

    db.apply_comment.side_effect = stage_apply_comment
    applied = apply_review_results_to_db(
        db,
        [_result(i) for i in range(5)],
        cancel_token=token,
    )
    # Two rows actually committed; the loop bailed before #3 even
    # started.
    assert applied == 2
    assert db.apply_comment.call_count == 2


def test_run_cancelled_is_a_runtime_error_subclass() -> None:
    """AMX Studio's job machinery catches ``RunCancelled``
    explicitly to differentiate it from generic exceptions. Pin
    the inheritance so a future rename doesn't silently break
    the error mapping."""
    assert issubclass(RunCancelled, RuntimeError)
