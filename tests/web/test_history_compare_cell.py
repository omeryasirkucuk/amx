"""Tests for ``GET /api/history/compare/cell`` — column-level compare.

The endpoint pivots ``run_results`` so callers can ask "how did this
specific cell's description differ across these N runs?" — independent
of the existing run-id-level ``POST /api/history/compare`` payload.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.web.routers import history as history_router


def _row(
    *,
    schema: str,
    table: str,
    column: str | None,
    description: str,
    logprob: float | None = None,
    confidence: str = "high",
    rid: int = 1,
) -> dict:
    """Build a minimal run_results row shaped like the SQLite store would."""
    return {
        "id": 10_000 + rid,
        "schema_name": schema,
        "table_name": table,
        "column_name": column or "",
        "chosen_description": description,
        "alternatives_json": [description],
        "logprob_score": logprob,
        "confidence": confidence,
        "source": "llm",
        "citations_json": [],
        "evaluation": None,
        "applied_at": None,
    }


def _wire_store(monkeypatch, results_by_run: dict[int, list[dict]]) -> MagicMock:
    store = MagicMock()
    store.get_run_results.side_effect = lambda rid: results_by_run.get(int(rid), [])
    monkeypatch.setattr(history_router, "history_store", lambda: store)
    return store


def test_compare_cell_endpoint_basic(client, auth_headers, monkeypatch) -> None:
    """Single-cell mode returns one ``per_run`` entry per run id."""
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
    _wire_store(monkeypatch, results)

    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db1.sales.orders.customer_id", "runs": "10,12,15"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["cell"] == {
        "database": "db1",
        "schema": "sales",
        "table": "orders",
        "column": "customer_id",
    }
    assert len(payload["per_run"]) == 3
    assert [e["run_id"] for e in payload["per_run"]] == [10, 12, 15]
    assert payload["per_run"][2]["description"] == "FK to customers."
    # logprobs differ → best should be run 15 (highest).
    assert payload["best_run_id"] == 15


def test_compare_cell_endpoint_missing_in_one_run(client, auth_headers, monkeypatch) -> None:
    """When the cell didn't appear in a run, that ``per_run`` slot is null."""
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
        12: [],  # nothing for this run
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
    _wire_store(monkeypatch, results)

    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db1.sales.orders.customer_id", "runs": "10,12,15"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["per_run"][0] is not None
    assert payload["per_run"][1] is None
    assert payload["per_run"][2] is not None


def test_compare_cell_table_level_omits_column(client, auth_headers, monkeypatch) -> None:
    """A 3-part cell key matches table-level rows only (column empty)."""
    results = {
        10: [
            _row(
                schema="sales",
                table="orders",
                column=None,
                description="Order table.",
                logprob=-0.4,
                rid=10,
            ),
            _row(
                schema="sales",
                table="orders",
                column="customer_id",
                description="FK.",
                logprob=-0.2,
                rid=10,
            ),
        ],
        12: [
            _row(
                schema="sales",
                table="orders",
                column=None,
                description="Orders.",
                logprob=-0.3,
                rid=12,
            ),
        ],
    }
    _wire_store(monkeypatch, results)

    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db1.sales.orders", "runs": "10,12"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cell"]["column"] is None
    # Both entries should be the table-level row, not the column row.
    assert payload["per_run"][0]["description"] == "Order table."
    assert payload["per_run"][1]["description"] == "Orders."


def test_compare_cell_glob_matches_multiple(client, auth_headers, monkeypatch) -> None:
    """A ``*`` in the column slot returns every matching cell."""
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
    _wire_store(monkeypatch, results)

    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db1.sales.orders.*", "runs": "10,12"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    cell_columns = {c["cell"]["column"] for c in payload["cells"]}
    assert cell_columns == {"customer_id", "created_at"}


def test_compare_cell_endpoint_400_on_bad_cell_key(client, auth_headers, monkeypatch) -> None:
    """1-part or 5-part cell keys return 400."""
    _wire_store(monkeypatch, {})

    bad = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "justtable", "runs": "1"},
    )
    assert bad.status_code == 400

    too_many = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "a.b.c.d.e", "runs": "1"},
    )
    assert too_many.status_code == 400


def test_compare_cell_endpoint_handles_missing_run(client, auth_headers, monkeypatch) -> None:
    """An unknown run id surfaces as a null per_run entry, not a 500."""
    store = MagicMock()
    # Simulate a deleted run: store returns [] for unknown ids.
    store.get_run_results.return_value = []
    monkeypatch.setattr(history_router, "history_store", lambda: store)

    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db.sales.orders.customer_id", "runs": "99999"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["per_run"] == [None]


def test_compare_cell_endpoint_400_when_runs_empty(client, auth_headers, monkeypatch) -> None:
    _wire_store(monkeypatch, {})
    response = client.get(
        "/api/history/compare/cell",
        headers=auth_headers,
        params={"cell": "db.sales.orders.customer_id", "runs": ""},
    )
    assert response.status_code == 400
