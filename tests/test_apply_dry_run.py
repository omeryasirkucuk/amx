"""``apply_review_results_to_db`` dry-run mode + ``preview_comment_sql`` helper.

The dry-run path lets ``/apply`` callers (CLI flag, Studio button) show
users exactly what would be written to the database without touching
it. The tests below pin the contract:

* No database connection is opened in dry-run mode (the test would
  fail with ``AttributeError`` on the mock if anyone reached for
  ``db.engine.begin``).
* Every row that would be applied is reported through ``on_progress``
  with ``status="preview"`` and the SQL template in ``detail``.
* Placeholder rows are still filtered out (matches the live path).
* Cancel token short-circuits the loop.
* The function returns ``0`` because nothing was written.
"""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult, apply_review_results_to_db
from amx.db.connector import AssetKind


def _result(
    schema: str,
    table: str,
    column: str | None,
    *,
    description: str = "Posting date.",
    asset_kind: str = "table",
    applied: bool = True,
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
    )


def test_dry_run_emits_preview_for_each_pending_row() -> None:
    db = MagicMock()
    db.preview_comment_sql.side_effect = lambda *, schema, table, column, asset_kind: (
        f"COMMENT ON COLUMN {schema}.{table}.{column} IS :cmt"
        if column
        else f"COMMENT ON TABLE {schema}.{table} IS :cmt"
    )

    progress_events: list[tuple[str, str, str, str | None]] = []

    def _record(r, status, idx, total, detail):
        progress_events.append((status, r.schema, r.table, r.column))
        # detail must carry the rendered SQL template — UIs lean on this.
        if status == "preview":
            assert ":cmt" in detail

    rows = [
        _result("public", "transactions", "posting"),
        _result("public", "transactions", "amount"),
        _result("analytics", "fact_orders", None),
    ]
    applied = apply_review_results_to_db(
        db,
        rows,
        on_progress=_record,
        dry_run=True,
    )

    # Nothing was written.
    assert applied == 0
    db.engine.begin.assert_not_called()
    db.apply_comment.assert_not_called()

    # Adapter was consulted for every pending row.
    assert db.preview_comment_sql.call_count == 3

    # Three preview events, in input order.
    assert [p[0] for p in progress_events] == ["preview", "preview", "preview"]
    assert progress_events[0][1:] == ("public", "transactions", "posting")
    assert progress_events[2][1:] == ("analytics", "fact_orders", None)


def test_dry_run_filters_placeholder_descriptions() -> None:
    """Placeholder rows are dropped before the preview loop — same
    rule the live path enforces, for the same reason (writing
    \"Auto-inference missed…\" would pollute the catalog)."""
    from amx.agents.orchestrator import _PLACEHOLDER_MARKERS

    placeholder = next(iter(_PLACEHOLDER_MARKERS))
    db = MagicMock()
    db.preview_comment_sql.return_value = "COMMENT ON COLUMN s.t.c IS :cmt"

    rows = [
        _result("s", "t", "c", description="Real comment."),
        _result("s", "t", "d", description=f"prefix {placeholder} suffix"),
    ]
    progress: list[tuple[str, str | None]] = []
    apply_review_results_to_db(
        db,
        rows,
        on_progress=lambda r, status, *_: progress.append((status, r.column)),
        dry_run=True,
    )

    # Only the non-placeholder row reaches preview.
    assert progress == [("preview", "c")]
    assert db.preview_comment_sql.call_count == 1


def test_dry_run_cancel_token_short_circuits() -> None:
    db = MagicMock()
    db.preview_comment_sql.return_value = "COMMENT ON COLUMN s.t.c IS :cmt"
    token = threading.Event()
    token.set()  # already cancelled before the first iteration

    rows = [_result("s", "t", "c"), _result("s", "t", "d")]
    progress: list[tuple[str, str | None]] = []
    apply_review_results_to_db(
        db,
        rows,
        on_progress=lambda r, status, *_: progress.append((status, r.column)),
        cancel_token=token,
        dry_run=True,
    )

    # No previews emitted; the loop bailed before the first row.
    assert progress == []


def test_dry_run_unsupported_asset_kind_marks_skipped_in_detail() -> None:
    """When the backend cannot accept a comment for the asset kind,
    ``preview_comment_sql`` returns ``None`` and the dry-run path
    surfaces a human-readable detail instead of an empty string."""
    db = MagicMock()
    db.preview_comment_sql.return_value = None

    progress: list[tuple[str, str]] = []
    apply_review_results_to_db(
        db,
        [_result("s", "t", "c")],
        on_progress=lambda r, status, idx, total, detail: progress.append((status, detail)),
        dry_run=True,
    )

    assert len(progress) == 1
    assert progress[0][0] == "preview"
    assert "unsupported" in progress[0][1].lower()


def test_dry_run_preview_failure_does_not_abort_the_loop() -> None:
    """A bug in a custom adapter's ``preview_comment_sql`` must not
    derail the rest of the dry-run output."""
    db = MagicMock()
    db.preview_comment_sql.side_effect = [
        RuntimeError("adapter bug"),
        "COMMENT ON COLUMN s.t.d IS :cmt",
    ]
    progress: list[tuple[str, str]] = []
    apply_review_results_to_db(
        db,
        [_result("s", "t", "c"), _result("s", "t", "d")],
        on_progress=lambda r, status, idx, total, detail: progress.append((status, detail)),
        dry_run=True,
    )

    statuses = [p[0] for p in progress]
    assert statuses == ["preview_failed", "preview"]


def test_preview_comment_sql_returns_template_for_column(monkeypatch) -> None:
    """``DatabaseConnector.preview_comment_sql`` mirrors
    ``apply_comment``'s branching but consults the adapter without
    executing anything. We mock the adapter so the test stays
    independent of any real backend driver."""
    from amx.db.connector import DatabaseConnector

    conn = MagicMock(spec=DatabaseConnector)
    conn.capabilities = MagicMock()
    conn.capabilities.column_comments = True
    conn.capabilities.table_comments = True
    conn.capabilities.comment_asset_keywords = frozenset({"TABLE"})
    conn._adapter = MagicMock()
    conn._adapter.set_column_comment_sql.return_value = "COMMENT ON COLUMN s.t.c IS :cmt"
    conn._adapter.set_table_comment_sql.return_value = "COMMENT ON TABLE s.t IS :cmt"

    # Bind the unbound method so we can call it on the spec mock.
    conn.preview_comment_sql = DatabaseConnector.preview_comment_sql.__get__(conn)

    column_sql = conn.preview_comment_sql(
        schema="s", table="t", column="c", asset_kind=AssetKind.TABLE
    )
    assert column_sql == "COMMENT ON COLUMN s.t.c IS :cmt"

    table_sql = conn.preview_comment_sql(
        schema="s", table="t", column=None, asset_kind=AssetKind.TABLE
    )
    assert table_sql == "COMMENT ON TABLE s.t IS :cmt"


def test_preview_comment_sql_returns_none_when_capability_missing() -> None:
    """Backends that cannot accept the requested comment return
    ``None`` instead of raising — dry-run path treats the value as a
    skip signal."""
    from amx.db.connector import DatabaseConnector

    conn = MagicMock(spec=DatabaseConnector)
    conn.capabilities = MagicMock()
    conn.capabilities.column_comments = False
    conn._adapter = MagicMock()
    conn.preview_comment_sql = DatabaseConnector.preview_comment_sql.__get__(conn)

    result = conn.preview_comment_sql(schema="s", table="t", column="c", asset_kind=AssetKind.TABLE)
    assert result is None
