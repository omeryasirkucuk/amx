"""Tests for the ``/api/history/compare`` endpoint + the underlying
pure helper extracted from the CLI's ``/history compare`` command.

Both surfaces share the same implementation so a future refactor of
the comparison shape lands in one place.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from amx.web.routers import history as history_router


def test_compare_endpoint_returns_runs_summary_and_missing(
    client, auth_headers, monkeypatch
) -> None:
    store = MagicMock()
    store.get_run.side_effect = lambda rid: (
        {"id": rid, "started_at": 1000.0 + rid, "status": "success", "command": "analyze.run"}
        if rid in {1, 2}
        else None
    )
    store.get_run_results.return_value = []
    monkeypatch.setattr(history_router, "history_store", lambda: store)
    monkeypatch.setattr("amx.cli_support.commands.compare.history_store", lambda: store)

    response = client.post(
        "/api/history/compare",
        headers=auth_headers,
        json={"run_ids": [1, 2, 9999]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert {r["id"] for r in payload["runs"]} == {1, 2}
    assert payload["missing"] == [9999]
    assert len(payload["summary_rows"]) == 2


def test_compare_endpoint_503_when_store_missing(client, auth_headers, monkeypatch) -> None:
    monkeypatch.setattr(history_router, "history_store", lambda: None)
    monkeypatch.setattr("amx.cli_support.commands.compare.history_store", lambda: None)
    response = client.post(
        "/api/history/compare",
        headers=auth_headers,
        json={"run_ids": [1]},
    )
    assert response.status_code == 503


def test_compare_runs_helper_handles_unknown_ids(monkeypatch) -> None:
    """Helper must not raise when the caller passes ids the store
    doesn't have — surface them under ``missing`` so the SPA can
    render a 'run #X was deleted' toast."""
    store = MagicMock()
    store.get_run.return_value = None
    store.get_run_results.return_value = []
    monkeypatch.setattr("amx.cli_support.commands.compare.history_store", lambda: store)

    from amx.cli_support.commands.compare import compare_runs

    payload = compare_runs([42, 43])
    assert payload["runs"] == []
    assert payload["missing"] == [42, 43]
    assert payload["summary_rows"] == []
