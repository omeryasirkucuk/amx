"""Per-row outcome contract + partial pending-queue drain.

The Studio Apply pending queue used to report success even when the
live DB rejected the writes, because:

* ``apply_review_results_to_db`` returned only an ``int`` — no per-row
  visibility.
* ``clear_pending()`` drained the on-disk queue unconditionally, so
  failed rows lost their place in the queue.

The fix exposes a structured outcome list and a partial-drain helper
``clear_pending_for(result_ids)``. These tests pin both contracts so
a future regression that re-introduces the silent-success behaviour
fails loudly.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from amx import pending_review as pr
from amx.agents.base import Confidence
from amx.agents.orchestrator import (
    ReviewResult,
    RowApplyOutcome,
    apply_review_results_to_db,
)


def _result(schema: str, table: str, column: str | None, *, result_id: int) -> ReviewResult:
    return ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description=f"desc for {schema}.{table}.{column or '(table)'}",
        confidence=Confidence.HIGH,
        source="combined",
        applied=True,
        asset_kind="table",
        result_id=result_id,
    )


def _make_db(apply_side_effect, *, supports_savepoints: bool = True):
    db = MagicMock()
    db.capabilities.supports_savepoints = supports_savepoints

    if supports_savepoints:
        conn = MagicMock()

        @contextmanager
        def _begin_nested():
            yield MagicMock()

        conn.begin_nested.side_effect = _begin_nested

        @contextmanager
        def _begin_outer():
            yield conn

        db.engine.begin.side_effect = _begin_outer
    else:

        @contextmanager
        def _begin_row():
            yield MagicMock()

        db.engine.begin.side_effect = _begin_row

    db.apply_comment.side_effect = apply_side_effect
    return db


def test_outcomes_out_collects_per_row_status() -> None:
    """Every row appended to ``outcomes_out`` carries its result_id +
    status + raw error_text. The legacy ``int`` return still reports
    the success count for callers that ignore the outcome list."""
    rows = [
        _result("nyctaxi", "trips", None, result_id=11),
        _result("missing_schema", "ghost", None, result_id=12),
        _result("nyctaxi", "fares", None, result_id=13),
    ]

    def _apply(*, schema, table, column, comment, asset_kind, conn):
        if schema == "missing_schema":
            raise RuntimeError(
                "[INSUFFICIENT_PERMISSIONS] User omer lacks ALTER on missing_schema.ghost"
            )

    db = _make_db(_apply)
    outcomes: list[RowApplyOutcome] = []
    applied = apply_review_results_to_db(db, rows, outcomes_out=outcomes)

    assert applied == 2
    assert {o.result_id: o.status for o in outcomes} == {11: "applied", 12: "failed", 13: "applied"}
    failed = next(o for o in outcomes if o.status == "failed")
    # After the classifier hook, error_kind carries the stable slug
    # the SPA pivots on and error_title carries the user-facing
    # banner string. The raw driver message is captured by the
    # classifier and reflected via error_text/title — we assert on
    # the slug + asset reference rather than the raw substring so
    # the test doesn't break if the classifier wording changes.
    assert failed.error_kind == "alter_privilege_denied"
    assert "missing_schema.ghost" in failed.error_title
    assert failed.schema == "missing_schema"
    assert failed.table == "ghost"


def test_outcomes_optional_keeps_legacy_callers_unchanged() -> None:
    """``outcomes_out=None`` (the default) preserves the legacy
    behaviour for callers that just want the count."""
    rows = [_result("nyctaxi", "trips", None, result_id=1)]
    db = _make_db(lambda **_: None)

    applied = apply_review_results_to_db(db, rows)

    assert applied == 1


def test_outcomes_collected_on_no_savepoint_backend_too() -> None:
    """No-savepoint mode (commit 1's Databricks/BigQuery path) writes
    each row in its own engine.begin(); the outcome list must still
    record per-row status — partial failure works identically on both
    transaction shapes."""
    rows = [
        _result("nyctaxi", "trips", None, result_id=21),
        _result("denied", "lockdown", None, result_id=22),
    ]

    def _apply(*, schema, table, column, comment, asset_kind, conn):
        if schema == "denied":
            raise RuntimeError("[INSUFFICIENT_PERMISSIONS] Databricks denial")

    db = _make_db(_apply, supports_savepoints=False)
    outcomes: list[RowApplyOutcome] = []
    apply_review_results_to_db(db, rows, outcomes_out=outcomes)

    assert {o.result_id: o.status for o in outcomes} == {21: "applied", 22: "failed"}


@pytest.fixture()
def tmp_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the on-disk pending file to a tmp path so tests can
    write / inspect it without touching the user's real queue."""
    p = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pr, "PENDING_FILE", p, raising=False)
    return p


def _seed_pending(path: Path, result_ids: list[int]) -> None:
    rows = [
        {
            "schema": "nyctaxi",
            "table": "trips",
            "column": None,
            "result_id": rid,
            "final_description": f"desc for {rid}",
            "confidence": "high",
            "source": "combined",
            "asset_kind": "table",
        }
        for rid in result_ids
    ]
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def test_clear_pending_for_drops_only_applied_ids(tmp_pending: Path) -> None:
    """The partial-drain helper removes exactly the requested ids and
    leaves every other row untouched — that's how a failed row stays
    queued for the next retry after the user fixes the underlying
    permission issue."""
    _seed_pending(tmp_pending, [10, 11, 12, 13])
    removed = pr.clear_pending_for([10, 12])
    assert removed == 2
    remaining = json.loads(tmp_pending.read_text(encoding="utf-8"))
    assert sorted(row["result_id"] for row in remaining) == [11, 13]


def test_clear_pending_for_empty_input_is_noop(tmp_pending: Path) -> None:
    """When every row in the apply failed (``applied_ids=[]``) the
    queue must be left exactly as the user picked it — no drains,
    no rewrites, no file flicker."""
    _seed_pending(tmp_pending, [1, 2, 3])
    pre = tmp_pending.read_text(encoding="utf-8")
    removed = pr.clear_pending_for([])
    assert removed == 0
    assert tmp_pending.read_text(encoding="utf-8") == pre


def test_clear_pending_for_removes_file_when_queue_empties(
    tmp_pending: Path,
) -> None:
    """Draining the last row removes the file rather than leaving an
    empty ``[]`` JSON behind. Mirrors the legacy ``clear_pending()``
    end state so the SPA's "0 queued" rendering stays clean."""
    _seed_pending(tmp_pending, [1, 2])
    pr.clear_pending_for([1, 2])
    assert not tmp_pending.exists()
