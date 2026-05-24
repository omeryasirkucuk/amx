"""``apply_review_results_to_db`` records audit rows when audit_log given.

Pins the contract that the new ``audit_log`` / ``audit_profile`` /
``audit_user`` / ``audit_host`` / ``audit_run_id`` keyword arguments
fan a successful COMMENT write into one ``apply_events`` row, and
that the audit path is best-effort (a misbehaving store does not
abort the apply).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult, apply_review_results_to_db


def _result(
    schema: str,
    table: str,
    column: str | None,
    *,
    description: str = "Order header.",
    asset_kind: str = "table",
    applied: bool = True,
    result_id: int | None = None,
) -> ReviewResult:
    return ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description=description,
        confidence=Confidence.HIGH,
        source="combined",
        applied=applied,
        asset_kind=asset_kind,
        result_id=result_id,
    )


def _make_db_mock() -> MagicMock:
    """Build a DatabaseConnector double whose ``apply_comment`` is a
    no-op. ``engine.begin`` returns a context manager so the
    ``with db.engine.begin() as conn:`` block works."""
    db = MagicMock()
    db.apply_comment.return_value = None
    db.apply_column_comments_batch.return_value = False  # force per-row path
    return db


def test_audit_log_receives_one_event_per_successful_apply() -> None:
    db = _make_db_mock()
    # Pre-write reader now consults the column-comments cache via
    # ``_lookup_column_comments_cache`` (cache-only — no live DB
    # round trip on a warm profile). Stub returns the originals AMX
    # is about to replace.
    db._lookup_column_comments_cache.return_value = {
        "table_comment": None,
        "columns": {
            "id": "Order id (DBA-written).",
            "amount": "Total amount in cents.",
        },
        "kind": "TABLE",
    }
    audit = MagicMock()

    rows = [
        _result("public", "orders", "id", result_id=10),
        _result("public", "orders", "amount", result_id=11),
    ]
    applied = apply_review_results_to_db(
        db,
        rows,
        audit_log=audit,
        audit_profile="prod_pg",
        audit_user="omer",
        audit_host="laptop",
        audit_run_id=42,
    )

    assert applied == 2
    # Two successful applies → two audit rows.
    assert audit.record_apply_event.call_count == 2
    first_kwargs = audit.record_apply_event.call_args_list[0].kwargs
    assert first_kwargs["schema_name"] == "public"
    assert first_kwargs["table_name"] == "orders"
    assert first_kwargs["column_name"] == "id"
    assert first_kwargs["new_comment"] == "Order header."
    assert first_kwargs["profile_name"] == "prod_pg"
    assert first_kwargs["applied_by"] == "omer"
    assert first_kwargs["hostname"] == "laptop"
    assert first_kwargs["run_id"] == 42
    assert first_kwargs["result_id"] == 10
    assert first_kwargs["asset_kind"] == "table"
    # PR-12b2: ``old_comment`` carries the pre-write value the reader
    # captured. ``/history rollback`` uses this to restore the
    # originals — DBA-written or otherwise — byte-for-byte.
    assert first_kwargs["old_comment"] == "Order id (DBA-written)."

    second_kwargs = audit.record_apply_event.call_args_list[1].kwargs
    assert second_kwargs["old_comment"] == "Total amount in cents."

    # Reader memoizes per-table; both rows share one cache lookup
    # and never touch the live-DB fallback method.
    db._lookup_column_comments_cache.assert_called_once_with("public", "orders")
    db.get_column_comments.assert_not_called()


def test_audit_log_omitted_means_no_record_calls() -> None:
    """Backward-compat: existing callers that don't pass audit_log
    must not see any ``record_apply_event`` invocation (and the
    function must keep working when the store is unavailable)."""
    db = _make_db_mock()
    rows = [_result("public", "orders", "id")]
    applied = apply_review_results_to_db(db, rows)
    assert applied == 1
    # No mock to inspect — but the bare call must not raise.


def test_audit_log_failure_does_not_abort_apply() -> None:
    """A misbehaving store (raises on every record_apply_event) must
    not cause apply_review_results_to_db to surface an exception or
    skip the live DB write."""
    db = _make_db_mock()
    audit = MagicMock()
    audit.record_apply_event.side_effect = RuntimeError("store down")

    rows = [_result("public", "orders", "id")]
    applied = apply_review_results_to_db(
        db,
        rows,
        audit_log=audit,
        audit_profile="prod_pg",
    )
    # Apply still reports success — best-effort audit doesn't gate it.
    assert applied == 1
    audit.record_apply_event.assert_called_once()
    db.apply_comment.assert_called_once()


def test_audit_log_skipped_in_dry_run() -> None:
    """Dry-run never writes to the database, so it must not write
    audit rows either — the audit log only records actual COMMENTs."""
    db = _make_db_mock()
    db.preview_comment_sql.return_value = "COMMENT ON COLUMN s.t.c IS :cmt"
    audit = MagicMock()

    rows = [_result("s", "t", "c")]
    applied = apply_review_results_to_db(
        db,
        rows,
        audit_log=audit,
        audit_profile="prod_pg",
        dry_run=True,
    )
    assert applied == 0
    audit.record_apply_event.assert_not_called()
