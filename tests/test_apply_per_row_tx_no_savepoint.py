"""Per-row transaction mode for backends without SAVEPOINT.

Reported: against a Databricks profile, ``Apply pending queue`` shipped
``SAVEPOINT sa_savepoint_N`` to the server (because SQLAlchemy's
``conn.begin_nested()`` does that unconditionally). Databricks
rejected the SAVEPOINT, and the following ``COMMENT ON TABLE`` /
``ALTER TABLE`` writes either ran on a poisoned connection state or
were silently skipped while the SPA still reported success.

These tests pin the new contract: when
``db.capabilities.supports_savepoints`` is False, the writeback path
must NOT call ``conn.begin_nested()``. Each row runs inside a fresh
``db.engine.begin()`` instead, so the per-row failure isolation we
relied on SAVEPOINT for is preserved without ever emitting SAVEPOINT
SQL.
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


def _make_no_savepoint_db(apply_side_effect):
    """Mock connector advertising ``supports_savepoints=False``. Each
    call to ``db.engine.begin()`` yields a fresh row-scoped connection
    so the writeback path can be observed without a live database."""
    db = MagicMock()
    db.capabilities.supports_savepoints = False

    open_calls: list[MagicMock] = []

    @contextmanager
    def _begin():
        # A new mock per call so the test can count how many separate
        # transactions were opened.
        row_conn = MagicMock(name=f"row_conn_{len(open_calls)}")
        # ``begin_nested`` MUST NOT be called in no-savepoint mode. If
        # the writeback regresses to the old path, the assertion in
        # the test will fire because ``row_conn.begin_nested`` was
        # never configured as a context manager.
        row_conn.begin_nested.side_effect = AssertionError(
            "begin_nested() called on a no-savepoint backend — SAVEPOINT "
            "SQL would be shipped to the server"
        )
        open_calls.append(row_conn)
        yield row_conn

    db.engine.begin.side_effect = _begin
    db.apply_comment.side_effect = apply_side_effect
    return db, open_calls


def test_no_savepoint_call_on_databricks_like_backend() -> None:
    """When the connector says it has no SAVEPOINT primitive, the
    writeback never calls ``begin_nested``. Each row gets its own
    ``engine.begin()`` instead."""
    rows = [
        _result("nyctaxi", "trips", None),
        _result("nyctaxi", "fares", None),
    ]

    db, open_calls = _make_no_savepoint_db(lambda **_: None)

    apply_review_results_to_db(db, rows)

    # Two rows → two independent transactions opened.
    assert len(open_calls) == len(rows), (
        f"expected one engine.begin() per row, got {len(open_calls)}"
    )
    # And none of those connections tried to open a savepoint.
    for conn_mock in open_calls:
        assert not conn_mock.begin_nested.called, (
            "begin_nested() must not be invoked in no-savepoint mode"
        )


def test_per_row_failure_isolated_on_no_savepoint_backend() -> None:
    """A failure on one row must not block the next — the per-row
    ``engine.begin()`` commits the survivors independently."""
    rows = [
        _result("missing_schema", "ghost", None),
        _result("nyctaxi", "trips", None),
        _result("nyctaxi", "fares", None),
    ]

    def _apply_comment(*, schema, table, column, comment, asset_kind, conn):
        if schema == "missing_schema":
            raise RuntimeError(
                "[INSUFFICIENT_PERMISSIONS] User omer lacks ALTER on missing_schema.ghost"
            )

    db, open_calls = _make_no_savepoint_db(_apply_comment)

    failures: list[tuple[str, str]] = []
    successes: list[str] = []

    applied = apply_review_results_to_db(
        db,
        rows,
        on_applied=lambda r: successes.append(f"{r.schema}.{r.table}"),
        on_failed=lambda r, exc: failures.append((f"{r.schema}.{r.table}", str(exc))),
    )

    # Two surviving rows applied; the failed one is reported.
    assert applied == 2
    assert successes == ["nyctaxi.trips", "nyctaxi.fares"]
    assert len(failures) == 1
    assert failures[0][0] == "missing_schema.ghost"
    assert "INSUFFICIENT_PERMISSIONS" in failures[0][1]
    # Every row opened its own tx.
    assert len(open_calls) == 3


def test_outer_engine_begin_not_called_when_no_savepoint() -> None:
    """The savepoint-capable path opens one outer ``engine.begin()``
    and reuses that connection across rows. The no-savepoint path
    must NOT open an outer transaction — only the per-row ones — so
    the number of ``engine.begin()`` invocations equals the row
    count, not row_count + 1."""
    rows = [
        _result("nyctaxi", "trips", None),
        _result("nyctaxi", "fares", None),
        _result("nyctaxi", "zones", None),
    ]

    db, open_calls = _make_no_savepoint_db(lambda **_: None)

    apply_review_results_to_db(db, rows)

    assert len(open_calls) == len(rows), (
        f"expected exactly {len(rows)} engine.begin() calls (one per row), got {len(open_calls)}"
    )
