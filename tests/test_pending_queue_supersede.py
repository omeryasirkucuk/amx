"""Pending queue supersede contract for Re-Run / Variations.

Re-Run / Variations are explicit "redo this asset" actions, so the
new row replaces any prior pending entry for the same asset --
otherwise ``/run`` (which seeds v1 into pending) plus a later
Variations call would leave BOTH v1 and v2 entries in the queue,
and the Apply step would write two competing ``COMMENT ON``
statements with last-write-wins semantics invisible from the SPA.

These tests pin the supersede behaviour of
``amx.web.routers.rerun._queue_outcomes_for_review``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import amx.pending_review as pending_review
from amx.agents._orchestrator.rerun import RerunOutcome
from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.web.routers.rerun import _queue_outcomes_for_review


@pytest.fixture(autouse=True)
def _isolated_pending(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect the on-disk pending file to ``tmp_path`` so each test
    starts from a clean slate and writes don't leak across tests or
    into the developer's real ``~/.amx/pending_metadata.json``."""
    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pending_review, "PENDING_FILE", pending_file)
    return pending_file


def _seed_pending(entries: list[ReviewResult]) -> None:
    pending_review.save_pending(entries)


def _outcome(
    *,
    schema: str,
    table: str,
    column: str | None,
    new_result_id: int,
    alternatives: list[str] | None = None,
    asset_kind: str = "column",
    error: str | None = None,
) -> RerunOutcome:
    return RerunOutcome(
        target_result_id=1,
        new_result_id=new_result_id,
        rerun_seq=1,
        schema=schema,
        table=table,
        column=column,
        asset_kind=asset_kind,
        alternatives=alternatives or ["alpha", "beta", "gamma"],
        confidence="medium",
        logprob_score=None,
        source="rerun",
        error=error,
    )


def _review(
    *,
    schema: str,
    table: str,
    column: str | None,
    result_id: int,
    final_description: str = "old top pick",
    asset_kind: str = "column",
) -> ReviewResult:
    return ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description=final_description,
        confidence=Confidence.MEDIUM,
        source="run",
        applied=True,
        asset_kind=asset_kind,
        result_id=result_id,
        alternatives=["existing pick", "existing alt B"],
        logprob_score=None,
    )


class TestSupersede:
    def test_rerun_supersedes_prior_pending_entry(self) -> None:
        """An outcome for the same asset key replaces the prior entry."""
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column="status",
                    result_id=1,
                )
            ]
        )
        appended = _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                    alternatives=["fresh top", "fresh B", "fresh C"],
                )
            ]
        )
        assert appended == 1
        rows = pending_review.load_pending()
        assert len(rows) == 1
        assert rows[0].result_id == 42
        assert rows[0].final_description == "fresh top"
        # ``load_pending`` does not round-trip the alternatives list
        # (the on-disk JSON only stores chosen text + asset coords),
        # so the assertions above are the contract worth pinning.

    def test_unrelated_assets_are_preserved(self) -> None:
        """Outcomes only supersede their own asset key."""
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column="status",
                    result_id=1,
                ),
                _review(
                    schema="public",
                    table="products",
                    column="name",
                    result_id=2,
                ),
            ]
        )
        _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                )
            ]
        )
        rows = pending_review.load_pending()
        # Two entries: the surviving products.name + the new orders.status.
        assert len(rows) == 2
        by_key = {(r.schema, r.table, r.column): r.result_id for r in rows}
        assert by_key[("public", "products", "name")] == 2
        assert by_key[("public", "orders", "status")] == 42

    def test_multiple_outcomes_supersede_independently(self) -> None:
        """Two outcomes against two prior entries replace both."""
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column="status",
                    result_id=1,
                ),
                _review(
                    schema="public",
                    table="products",
                    column="name",
                    result_id=2,
                ),
                _review(
                    schema="public",
                    table="customers",
                    column="email",
                    result_id=3,
                ),
            ]
        )
        appended = _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                ),
                _outcome(
                    schema="public",
                    table="products",
                    column="name",
                    new_result_id=43,
                ),
            ]
        )
        assert appended == 2
        rows = pending_review.load_pending()
        assert len(rows) == 3
        by_key = {(r.schema, r.table, r.column): r.result_id for r in rows}
        assert by_key[("public", "orders", "status")] == 42
        assert by_key[("public", "products", "name")] == 43
        # Untouched.
        assert by_key[("public", "customers", "email")] == 3

    def test_no_supersede_when_outcome_failed(self) -> None:
        """A failed outcome must NOT delete the user's existing pick.

        If the agent failed to regenerate an asset, the prior pending
        entry is the only viable description the user has -- silently
        dropping it would leave them with no queued pick for the
        column and no way to recover short of re-restoring manually.
        """
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column="status",
                    result_id=1,
                )
            ]
        )
        appended = _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                    error="model timed out",
                )
            ]
        )
        assert appended == 0
        rows = pending_review.load_pending()
        assert len(rows) == 1
        assert rows[0].result_id == 1
        assert rows[0].final_description == "old top pick"

    def test_no_supersede_when_outcome_has_empty_alternatives(self) -> None:
        """Empty / whitespace-only first alternative is treated as a
        no-op (save_pending would drop it anyway), so the prior entry
        must survive."""
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column="status",
                    result_id=1,
                )
            ]
        )
        appended = _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                    alternatives=["   "],
                )
            ]
        )
        assert appended == 0
        rows = pending_review.load_pending()
        assert len(rows) == 1
        assert rows[0].result_id == 1

    def test_asset_kind_distinguishes_table_from_column(self) -> None:
        """A table-level entry and a column-level entry on the same
        (schema, table) are independent assets and must not supersede
        each other."""
        _seed_pending(
            [
                _review(
                    schema="public",
                    table="orders",
                    column=None,
                    result_id=1,
                    asset_kind="table",
                )
            ]
        )
        _queue_outcomes_for_review(
            [
                _outcome(
                    schema="public",
                    table="orders",
                    column="status",
                    new_result_id=42,
                    asset_kind="column",
                )
            ]
        )
        rows = pending_review.load_pending()
        assert len(rows) == 2
        kinds = {r.asset_kind for r in rows}
        assert kinds == {"table", "column"}
