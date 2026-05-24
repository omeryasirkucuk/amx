"""Cache-first apply contract for ``apply_review_results_to_db``.

A RUN's accepted descriptions must land in the durable comment cache
*before* the live-DB ``COMMENT ON`` (the cache is the read source for
Studio and the generation agents), and a failed live write must roll
the cache back so it never drifts ahead of the database
(``cache == DB`` always).

These tests pin both halves for the single-row and the batched
(multi-column, same table) writeback paths.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult, apply_review_results_to_db


def _result(schema: str, table: str, column: str | None) -> ReviewResult:
    return ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description=f"desc for {schema}.{table}.{column or '(table)'}",
        confidence=Confidence.HIGH,
        source="combined",
        applied=True,
        asset_kind="table",
    )


def _make_db(events: list, *, apply_side_effect=None, batch_side_effect=None):
    """Connector mock that records the order of cache vs live operations.

    ``events`` accumulates ``(op, schema, table, column)`` tuples so a
    test can assert that ``cache_write`` precedes ``live_apply`` and that
    ``cache_restore`` only fires on a failed live write.
    """
    db = MagicMock()
    conn = MagicMock()

    @contextmanager
    def _begin_nested():
        yield MagicMock()

    conn.begin_nested.side_effect = _begin_nested

    @contextmanager
    def _begin_outer():
        yield conn

    db.engine.begin.side_effect = _begin_outer
    db.capabilities.supports_savepoints = True

    def _snapshot(schema, table):
        events.append(("snapshot", schema, table, None))
        return {"table_comment": "old", "columns": {}, "kind": "TABLE"}

    db.cache_snapshot_comment_entry.side_effect = _snapshot
    db.cache_write_comment.side_effect = lambda **k: events.append(
        ("cache_write", k["schema"], k["table"], k.get("column"))
    )
    db.cache_restore_comment_entry.side_effect = lambda s, t, snap: events.append(
        ("cache_restore", s, t, None)
    )

    def _apply(*, schema, table, column, comment, asset_kind, conn):
        events.append(("live_apply", schema, table, column))
        if apply_side_effect is not None:
            apply_side_effect(schema=schema, table=table, column=column)

    db.apply_comment.side_effect = _apply

    def _apply_batch(schema, table, comments, conn=None):
        events.append(("live_apply_batch", schema, table, None))
        if batch_side_effect is not None:
            batch_side_effect(schema, table, comments)
        return len(comments)

    db.apply_column_comments_batch.side_effect = _apply_batch
    return db, conn


def _ops(events: list) -> list[str]:
    return [e[0] for e in events]


def test_single_row_writes_cache_before_live_db() -> None:
    events: list = []
    db, _ = _make_db(events)

    applied = apply_review_results_to_db(db, [_result("public", "orders", "id")])

    assert applied == 1
    ops = _ops(events)
    assert "cache_write" in ops and "live_apply" in ops
    assert ops.index("cache_write") < ops.index("live_apply")
    # Clean success leaves the cache holding the new value — no rollback.
    assert "cache_restore" not in ops


def test_single_row_rolls_back_cache_when_live_db_fails() -> None:
    events: list = []

    def _boom(**_kw):
        raise RuntimeError("permission denied")

    db, _ = _make_db(events, apply_side_effect=_boom)
    failures: list[str] = []

    applied = apply_review_results_to_db(
        db,
        [_result("public", "orders", "id")],
        on_failed=lambda r, exc: failures.append(str(exc)),
    )

    assert applied == 0
    ops = _ops(events)
    # Cache was written, the live write was attempted and failed, and the
    # cache was rolled back AFTER the failed live write (cache == DB).
    assert ops.index("cache_write") < ops.index("live_apply") < ops.index("cache_restore")
    assert failures and "permission denied" in failures[0]


def test_batch_writes_cache_before_live_db() -> None:
    events: list = []
    db, _ = _make_db(events)
    rows = [_result("public", "orders", "a"), _result("public", "orders", "b")]

    applied = apply_review_results_to_db(db, rows)

    assert applied == 2
    ops = _ops(events)
    # Both columns cached before the single batched live write.
    assert ops.count("cache_write") == 2
    assert "live_apply_batch" in ops
    assert max(i for i, o in enumerate(ops) if o == "cache_write") < ops.index("live_apply_batch")
    assert "cache_restore" not in ops


def test_batch_rolls_back_cache_when_live_batch_fails() -> None:
    events: list = []

    def _boom(_schema, _table, _comments):
        raise RuntimeError("batch denied")

    db, _ = _make_db(events, batch_side_effect=_boom)
    rows = [_result("public", "orders", "a"), _result("public", "orders", "b")]

    apply_review_results_to_db(db, rows)

    ops = _ops(events)
    # The failed batch restores the snapshot before the per-row fallback
    # re-applies each column cache-first.
    assert "cache_restore" in ops
    assert ops.index("live_apply_batch") < ops.index("cache_restore")
