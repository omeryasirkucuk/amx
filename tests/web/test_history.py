"""History router tests — both the missing-store path and the
happy-path against a stub history store."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from amx.web.routers import history as history_router


@pytest.fixture()
def stub_store(monkeypatch):
    """Inject a MagicMock as the history-store singleton."""
    store = MagicMock()
    monkeypatch.setattr(history_router, "history_store", lambda: store)
    return store


def test_missing_store_returns_503(client, auth_headers, monkeypatch) -> None:
    monkeypatch.setattr(history_router, "history_store", lambda: None)
    response = client.get("/api/history/stats", headers=auth_headers)
    assert response.status_code == 503
    assert "history store" in response.json()["detail"].lower()


def test_list_recent_runs_filters_default_to_analyze_run(client, auth_headers, stub_store) -> None:
    stub_store.list_recent_runs.return_value = [{"id": 7, "command": "analyze.run"}]
    response = client.get("/api/history/runs", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["command_filter"] == "analyze.run"
    assert payload["runs"][0]["id"] == 7
    stub_store.list_recent_runs.assert_called_once_with(limit=20, command_filter="analyze.run")


def test_list_recent_runs_command_all_disables_filter(client, auth_headers, stub_store) -> None:
    stub_store.list_recent_runs.return_value = []
    response = client.get("/api/history/runs?command=all&limit=5", headers=auth_headers)
    assert response.status_code == 200
    stub_store.list_recent_runs.assert_called_once_with(limit=5, command_filter=None)


def test_get_run_404_when_missing(client, auth_headers, stub_store) -> None:
    stub_store.get_run.return_value = None
    response = client.get("/api/history/runs/9999", headers=auth_headers)
    assert response.status_code == 404


def test_get_run_returns_full_payload(client, auth_headers, stub_store) -> None:
    stub_store.get_run.return_value = {
        "id": 42,
        "command": "analyze.run",
        "scope": {"sales": ["orders"]},
        "metrics": {"applied_count": 17},
    }
    response = client.get("/api/history/runs/42", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["metrics"]["applied_count"] == 17


def test_get_run_results(client, auth_headers, stub_store) -> None:
    stub_store.get_run_results.return_value = [{"column_name": "id", "confidence": "high"}]
    response = client.get("/api/history/runs/42/results", headers=auth_headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    stub_store.get_run_results.assert_called_once_with(42, unevaluated_only=False)


def test_runs_by_scope(client, auth_headers, stub_store) -> None:
    stub_store.find_runs_for_scope.return_value = [{"id": 1}]
    response = client.get(
        "/api/history/runs-by-scope?schema=sales&table=orders&limit=10",
        headers=auth_headers,
    )
    assert response.status_code == 200
    stub_store.find_runs_for_scope.assert_called_once_with(
        schema="sales", table="orders", command_filter=None, limit=10
    )


def test_stats_passes_through(client, auth_headers, stub_store) -> None:
    stub_store.stats.return_value = {"total": 12, "success": 10, "failed": 2}
    response = client.get("/api/history/stats", headers=auth_headers)
    assert response.status_code == 200
    assert response.json()["total"] == 12


def test_recent_events(client, auth_headers, stub_store) -> None:
    stub_store.list_recent_events.return_value = [
        {"name": "config.save", "details": {"path": "/x"}}
    ]
    response = client.get("/api/history/events?limit=5", headers=auth_headers)
    assert response.status_code == 200
    stub_store.list_recent_events.assert_called_once_with(limit=5)
