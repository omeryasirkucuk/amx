"""SAVEPOINT-per-row isolation in ``apply_review_results_to_db``.

PostgreSQL aborts the entire transaction when a single statement
fails (``InFailedSqlTransaction`` cascade). Without per-row savepoints,
one bad asset (e.g. wrong schema name) used to fail every subsequent
row in the same apply batch with a misleading "transaction aborted"
error — even though every other asset was perfectly valid.

These tests pin the contract that a row-level error stays contained:
the rest of the batch still applies and the user sees the original
error on just the failing row.
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


def _make_db(apply_side_effect):
    """Build a connector mock whose ``apply_comment`` is the supplied
    callable. ``engine.begin()`` and ``conn.begin_nested()`` both
    return real context-manager mocks so the SAVEPOINT path under
    test executes without hitting a database."""
    db = MagicMock()
    conn = MagicMock()

    @contextmanager
    def _begin_nested():
        try:
            yield MagicMock()
        except Exception:
            # Mirror SQLAlchemy: nested context manager swallows the
            # exception only if you call ``.commit()`` explicitly.
            # Here we want the raise to propagate so the outer
            # try/except sees it — same as production.
            raise

    conn.begin_nested.side_effect = _begin_nested

    @contextmanager
    def _begin_outer():
        yield conn

    db.engine.begin.side_effect = _begin_outer
    db.apply_comment.side_effect = apply_side_effect
    return db, conn


def test_first_row_failure_does_not_cascade_into_others() -> None:
    """One bad row (schema not found) should not poison the rest."""
    # Distinct (schema, table) tuples so each row goes through the
    # per-row writeback (the batched path is exercised in the
    # ``test_savepoint_used_per_row`` companion).
    rows = [
        _result("missing_schema", "offices", "officeCode"),
        _result("public", "orders", "id"),
        _result("public", "products", "total"),
    ]

    calls: list[str] = []

    def _apply_comment(*, schema, table, column, comment, asset_kind, conn):
        calls.append(f"{schema}.{table}.{column}")
        if schema == "missing_schema":
            raise RuntimeError('schema "missing_schema" does not exist')

    db, _conn = _make_db(_apply_comment)

    failures: list[tuple[str, str]] = []
    successes: list[str] = []

    applied = apply_review_results_to_db(
        db,
        rows,
        on_applied=lambda r: successes.append(f"{r.schema}.{r.table}.{r.column}"),
        on_failed=lambda r, exc: failures.append((f"{r.schema}.{r.table}.{r.column}", str(exc))),
    )

    # Every row was attempted (no cascade truncation).
    assert len(calls) == 3, f"expected 3 attempts, got {len(calls)}: {calls}"
    # The two valid rows applied; the bad one did not.
    assert applied == 2
    assert successes == ["public.orders.id", "public.products.total"]
    # The bad row reported the original cause, not "transaction aborted".
    assert len(failures) == 1
    assert failures[0][0] == "missing_schema.offices.officeCode"
    assert "missing_schema" in failures[0][1]
    assert "does not exist" in failures[0][1]


def test_savepoint_used_per_row() -> None:
    """The connection's ``begin_nested`` is called once per row (not
    just once for the whole batch) — that's the actual mechanism that
    prevents PostgreSQL's transaction-abort cascade."""
    rows = [
        _result("public", "a", "x"),
        _result("public", "b", "y"),
    ]

    db, conn = _make_db(lambda **_: None)

    apply_review_results_to_db(db, rows)

    assert conn.begin_nested.call_count == len(rows)
