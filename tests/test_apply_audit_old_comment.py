"""Pre-write ``old_comment`` lookup → ``apply_events.old_comment`` populated.

PR-12 + PR-12b shipped audit-log writes with ``old_comment=None``.
This PR closes the loop: ``apply_review_results_to_db`` now reads
the prior COMMENT before the overwrite via ``_OldCommentReader`` and
threads it into ``record_apply_event`` so ``/history rollback`` can
restore the original state byte-for-byte — including DBA-written
comments AMX never authored.

We pin the contract here without spinning up a real connector:

* Column comment: reader caches ``get_column_comments`` per
  (schema, table); a 200-row apply against one table fans out to
  one read, not 200.
* Single-table comment: ``get_table_comment`` is consulted.
* Schema comment: ``get_schema_comment`` is consulted.
* Reader failure → ``old_comment=None``; the apply path proceeds
  normally and the audit row records "original unknown".
* ``audit_log=None`` short-circuits the reader entirely (legacy
  callers stay on the historical hot path with no extra DB round
  trips).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.agents.base import Confidence
from amx.agents.orchestrator import (
    ReviewResult,
    _OldCommentReader,
    apply_review_results_to_db,
)
from amx.db.connector import AssetKind


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
    db = MagicMock()
    db.apply_comment.return_value = None
    db.apply_column_comments_batch.return_value = False
    return db


# ── _OldCommentReader unit ──────────────────────────────────────────


def test_reader_returns_table_comment_when_column_is_none() -> None:
    db = MagicMock()
    db.get_table_comment.return_value = "Order header (DBA)."
    reader = _OldCommentReader(db)

    rr = _result("public", "orders", None)
    assert reader.read(rr, AssetKind.TABLE) == "Order header (DBA)."
    db.get_table_comment.assert_called_once_with("public", "orders")


def test_reader_caches_column_comments_per_table() -> None:
    """200-row apply against one table → exactly one
    get_column_comments call."""
    db = MagicMock()
    db.get_column_comments.return_value = {
        "id": "Customer id (DBA).",
        "name": "Customer name.",
    }
    reader = _OldCommentReader(db)

    rr_id = _result("public", "customers", "id")
    rr_name = _result("public", "customers", "name")

    assert reader.read(rr_id, AssetKind.TABLE) == "Customer id (DBA)."
    assert reader.read(rr_name, AssetKind.TABLE) == "Customer name."
    # Single bulk read served both column lookups.
    db.get_column_comments.assert_called_once_with("public", "customers")


def test_reader_returns_none_for_unknown_column() -> None:
    db = MagicMock()
    db.get_column_comments.return_value = {"id": "known"}
    reader = _OldCommentReader(db)
    rr = _result("s", "t", "missing")
    assert reader.read(rr, AssetKind.TABLE) is None


def test_reader_swallows_get_column_comments_failure() -> None:
    """A misbehaving adapter must not break apply — return None and
    let the audit row record 'original unknown'."""
    db = MagicMock()
    db.get_column_comments.side_effect = RuntimeError("driver blew up")
    reader = _OldCommentReader(db)
    assert reader.read(_result("s", "t", "c"), AssetKind.TABLE) is None


def test_reader_consults_schema_comment_for_schema_kind() -> None:
    db = MagicMock()
    db.get_schema_comment.return_value = "Sales schema."
    reader = _OldCommentReader(db)
    rr = _result("sales", "", None, asset_kind="schema")
    assert reader.read(rr, AssetKind.SCHEMA) == "Sales schema."
    db.get_schema_comment.assert_called_once_with("sales")


# ── apply_review_results_to_db integration ───────────────────────────


def test_apply_threads_old_comment_into_audit_record() -> None:
    """Per-row path: read happens before overwrite, audit gets the
    prior text — DBA-written or otherwise."""
    db = _make_db_mock()
    db.get_column_comments.return_value = {"id": "DBA-written id comment."}
    audit = MagicMock()

    apply_review_results_to_db(
        db,
        [_result("public", "orders", "id", description="LLM rewrite.")],
        audit_log=audit,
        audit_profile="prod_pg",
    )

    audit.record_apply_event.assert_called_once()
    kwargs = audit.record_apply_event.call_args.kwargs
    assert kwargs["old_comment"] == "DBA-written id comment."
    assert kwargs["new_comment"] == "LLM rewrite."


def test_apply_records_none_when_no_prior_comment() -> None:
    """Column with no comment in the DB → audit records None
    (distinct from 'we couldn't read it')."""
    db = _make_db_mock()
    db.get_column_comments.return_value = {"id": None}
    audit = MagicMock()

    apply_review_results_to_db(
        db,
        [_result("public", "orders", "id")],
        audit_log=audit,
    )

    kwargs = audit.record_apply_event.call_args.kwargs
    assert kwargs["old_comment"] is None


def test_apply_skips_old_comment_read_when_audit_log_none() -> None:
    """Legacy callers (no audit) must not pay the get_*_comments
    round-trip cost."""
    db = _make_db_mock()
    apply_review_results_to_db(db, [_result("public", "orders", "id")])
    db.get_column_comments.assert_not_called()
    db.get_table_comment.assert_not_called()


def test_apply_continues_when_pre_read_raises() -> None:
    """A bug in get_column_comments must not abort the apply — the
    audit row just lands with old_comment=None."""
    db = _make_db_mock()
    db.get_column_comments.side_effect = RuntimeError("read failed")
    audit = MagicMock()

    n_applied = apply_review_results_to_db(
        db,
        [_result("public", "orders", "id")],
        audit_log=audit,
    )
    assert n_applied == 1
    db.apply_comment.assert_called_once()
    kwargs = audit.record_apply_event.call_args.kwargs
    assert kwargs["old_comment"] is None
