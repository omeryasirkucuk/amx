"""``_OldCommentReader`` must never trigger live-DB reads.

Reported: Apply pending queue against a Databricks profile shipped
two big ``SELECT … FROM system.information_schema.{tables,columns}``
queries *before* each COMMENT ON — even for rows whose COMMENT was
about to fail. Root cause: ``_OldCommentReader.read()`` called
``connector.get_column_comments``, which on cache miss fired the
adapter's ``bulk_schema_metadata`` query.

These tests pin the new contract: the reader uses the cache-only
lookup (``_lookup_column_comments_cache``) and falls back to
``old_comment=None`` rather than ever invoking
``get_column_comments`` / ``get_table_comment`` (table/column path).
A regression that re-introduces the bulk SELECT will fail because
the mock raises on any call to those methods.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.agents._orchestrator.writeback import _OldCommentReader
from amx.agents.base import Confidence
from amx.agents.orchestrator import ReviewResult
from amx.db.connector import AssetKind


def _result(schema: str, table: str, column: str | None) -> ReviewResult:
    return ReviewResult(
        schema=schema,
        table=table,
        column=column,
        final_description="x",
        confidence=Confidence.HIGH,
        source="combined",
        applied=True,
        asset_kind="table",
    )


def _strict_db():
    """Mock connector whose live-DB read methods raise. The reader
    must never call them — if it does, the test fails loudly with
    the AssertionError instead of silently hitting a database."""
    db = MagicMock()
    db.get_table_comment.side_effect = AssertionError(
        "get_table_comment called from audit reader — would trigger bulk SELECT"
    )
    db.get_column_comments.side_effect = AssertionError(
        "get_column_comments called from audit reader — would trigger bulk SELECT"
    )
    return db


def test_reader_uses_cache_only_lookup_for_table_grain() -> None:
    """Table-grain audit reads pull from the column-comments cache,
    never from the live-DB-fallback method."""
    db = _strict_db()
    db._lookup_column_comments_cache.return_value = {
        "table_comment": "prior table description",
        "columns": {},
        "kind": "TABLE",
    }
    reader = _OldCommentReader(db)

    out = reader.read(_result("nyctaxi", "trips", None), AssetKind.TABLE)
    assert out == "prior table description"
    db._lookup_column_comments_cache.assert_called_with("nyctaxi", "trips")


def test_reader_uses_cache_only_lookup_for_column_grain() -> None:
    db = _strict_db()
    db._lookup_column_comments_cache.return_value = {
        "table_comment": None,
        "columns": {"trip_id": "prior column desc"},
        "kind": "TABLE",
    }
    reader = _OldCommentReader(db)

    out = reader.read(_result("nyctaxi", "trips", "trip_id"), AssetKind.TABLE)
    assert out == "prior column desc"


def test_reader_returns_none_when_cache_is_cold() -> None:
    """Cold cache (lookup returns None) MUST surface as old_comment=None
    rather than triggering a live read. This is the whole point of the
    cache-only refactor — paying for a multi-second bulk SELECT just
    to record the prior comment in the audit log is the bug we're
    fixing."""
    db = _strict_db()
    db._lookup_column_comments_cache.return_value = None
    reader = _OldCommentReader(db)

    out = reader.read(_result("nyctaxi", "trips", None), AssetKind.TABLE)
    assert out is None
    # Critical assertion: the live-DB fallback methods were not called.
    db.get_table_comment.assert_not_called()
    db.get_column_comments.assert_not_called()


def test_reader_memoizes_cache_lookups_within_one_apply() -> None:
    """Repeated reads against the same (schema, table) inside one
    apply call must hit the lookup once — important for column-grained
    queues where one table holds many rows."""
    db = _strict_db()
    db._lookup_column_comments_cache.return_value = {
        "table_comment": None,
        "columns": {"a": "desc-a", "b": "desc-b", "c": "desc-c"},
        "kind": "TABLE",
    }
    reader = _OldCommentReader(db)

    reader.read(_result("nyctaxi", "trips", "a"), AssetKind.TABLE)
    reader.read(_result("nyctaxi", "trips", "b"), AssetKind.TABLE)
    reader.read(_result("nyctaxi", "trips", "c"), AssetKind.TABLE)

    assert db._lookup_column_comments_cache.call_count == 1
