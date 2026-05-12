"""Tests for the CLI ``/compare --cell ...`` cell-mode renderer.

The renderer is a pure function over ``runs`` + ``results_by_run`` so
we exercise it directly without spawning a Click runtime.
"""

from __future__ import annotations

from typing import Any

from amx.cli_support.commands import compare as compare_mod


def _row(
    *,
    schema: str,
    table: str,
    column: str | None,
    description: str,
    logprob: float | None = None,
    rid: int = 1,
) -> dict[str, Any]:
    return {
        "id": 10_000 + rid,
        "schema_name": schema,
        "table_name": table,
        "column_name": column or "",
        "chosen_description": description,
        "alternatives_json": [description],
        "logprob_score": logprob,
        "confidence": "high",
        "source": "llm",
        "citations_json": [],
        "evaluation": None,
        "applied_at": None,
        "token_count": 12,
    }


def _runs(*ids: int) -> list[dict[str, Any]]:
    return [{"id": i, "started_at": 1000.0 + i} for i in ids]


def test_cell_compare_single_column(capsys) -> None:
    """Compare one column across 3 runs renders one table with 3 rows."""
    runs = _runs(10, 12, 15)
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="The customer.",
                logprob=-0.5,
                rid=10,
            )
        ],
        12: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="Customer reference.",
                logprob=-0.3,
                rid=12,
            )
        ],
        15: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="FK to customers.",
                logprob=-0.1,
                rid=15,
            )
        ],
    }
    count = compare_mod._render_cell_compare("db1.sales.orders.customer_id", runs, results)
    assert count == 1
    out = capsys.readouterr().out
    assert "sales.orders.customer_id" in out
    assert "column-level" in out
    assert "#10" in out and "#12" in out and "#15" in out


def test_cell_compare_missing_in_one_run(capsys) -> None:
    """When the cell didn't appear in run 12, that row renders as a dash."""
    runs = _runs(10, 12, 15)
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="The customer.",
                logprob=-0.5,
                rid=10,
            )
        ],
        12: [],
        15: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="FK.",
                logprob=-0.1,
                rid=15,
            )
        ],
    }
    count = compare_mod._render_cell_compare("db1.sales.orders.customer_id", runs, results)
    assert count == 1
    out = capsys.readouterr().out
    assert "not in this run" in out


def test_cell_compare_table_level_omits_column(capsys) -> None:
    """A 3-part cell key only matches table-level rows."""
    runs = _runs(10, 12)
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column=None,
                description="Order table desc.",
                logprob=-0.4,
                rid=10,
            ),
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="Should not match.",
                logprob=-0.2,
                rid=10,
            ),
        ],
        12: [
            _row(
                schema="sales",
                table="orders",
                column=None,
                description="Orders v2.",
                logprob=-0.3,
                rid=12,
            ),
        ],
    }
    count = compare_mod._render_cell_compare("db1.sales.orders", runs, results)
    assert count == 1
    out = capsys.readouterr().out
    assert "table-level" in out
    assert "Order table desc" in out
    assert "Should not match" not in out


def test_cell_compare_glob_matches_multiple(capsys) -> None:
    """A glob like ``sales.orders.*`` renders one table per distinct cell."""
    runs = _runs(10, 12)
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="cust.",
                logprob=-0.5,
                rid=10,
            ),
            _row(
                schema="sales",
                table="orders",
                column="created_at",
                description="ts.",
                logprob=-0.4,
                rid=10,
            ),
        ],
        12: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="cust v2.",
                logprob=-0.3,
                rid=12,
            ),
        ],
    }
    count = compare_mod._render_cell_compare("db1.sales.orders.*", runs, results)
    assert count == 2


def test_cli_compare_cell_renders_per_cell_tables(capsys) -> None:
    """Glob produces N tables for N distinct matched cells."""
    runs = _runs(10, 12)
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="cust.",
                logprob=-0.5,
                rid=10,
            ),
            _row(
                schema="sales",
                table="orders",
                column="created_at",
                description="ts.",
                logprob=-0.4,
                rid=10,
            ),
            _row(
                schema="sales",
                table="orders",
                column="updated_at",
                description="upd.",
                logprob=-0.6,
                rid=10,
            ),
        ],
        12: [
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="cust v2.",
                logprob=-0.3,
                rid=12,
            ),
        ],
    }
    count = compare_mod._render_cell_compare("db1.sales.orders.*", runs, results)
    assert count == 3
    out = capsys.readouterr().out
    # Each cell's full path appears in its own table title.
    assert out.count("sales.orders.customer_id") >= 1
    assert "sales.orders.created_at" in out
    assert "sales.orders.updated_at" in out


def test_cli_compare_cell_glob_caps_at_50(capsys) -> None:
    """Matching more than 50 cells warns and only renders the first 50."""
    runs = _runs(10, 12)
    # Synthesise 60 distinct columns in one run.
    rows_10 = [
        _row(
            schema="sales",
            table="orders",
            column=f"col_{i:03d}",
            description=f"desc {i}",
            logprob=-0.5,
            rid=10,
        )
        for i in range(60)
    ]
    results = {10: rows_10, 12: []}
    count = compare_mod._render_cell_compare("db1.sales.orders.*", runs, results)
    assert count == 50
    out = capsys.readouterr().out
    assert "Showing first 50" in out or "first 50" in out


def test_cell_compare_bad_key_returns_zero(capsys) -> None:
    """A malformed cell key surfaces an error and renders nothing."""
    runs = _runs(10, 12)
    count = compare_mod._render_cell_compare("toofew", runs, {10: [], 12: []})
    assert count == 0
