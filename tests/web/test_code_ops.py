"""Code scan router tests.

Mocks the heavy paths (DatabaseConnector, analyze_codebase) so the
suite never touches a real DB or filesystem walk. Pins the path
resolution precedence + worker dispatch contract.
"""

from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock


def _wait_for_status(client, job_id: str, target: str, timeout: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(
            f"/api/runs/{job_id}",
            headers={"Authorization": "Bearer test-visualizer-token-abc123"},
        )
        body = resp.json()
        if body["status"] == target:
            return body
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} never reached status {target}; last={body}")


def test_scan_400_when_no_path(client, auth_headers) -> None:
    """No path in body, no profile flag, no active code profile —
    bail with 400 instead of spawning an empty worker."""
    response = client.post("/api/code/scan", headers=auth_headers, json={})
    assert response.status_code == 400


def test_scan_resolves_active_profile(client, auth_headers, cfg, monkeypatch) -> None:
    cfg.code_profiles["main-repo"] = "/abs/path"
    cfg.active_code_profile = "main-repo"

    fake_db = MagicMock(
        list_schemas=MagicMock(return_value=["public"]),
        list_tables=MagicMock(return_value=["users"]),
    )
    monkeypatch.setattr(
        "amx.db.connector.DatabaseConnector",
        lambda cfg: fake_db,
    )
    fake_report = MagicMock(
        total_files=10,
        scanned_files=8,
        references={"users": [MagicMock(file="a.py", line_no=1, line_text="x", matched_asset="users", context="")]},
        external_mentions={},
    )
    monkeypatch.setattr(
        "amx.codebase.analyzer.analyze_codebase",
        lambda *args, **kw: fake_report,
    )

    response = client.post("/api/code/scan", headers=auth_headers, json={})
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    body = _wait_for_status(client, job_id, "done")
    summary = body["summary"]
    assert summary["scanned_files"] == 8
    assert summary["catalog_assets"] == 1


def test_scan_explicit_path_overrides_profile(
    client, auth_headers, cfg, monkeypatch
) -> None:
    """Explicit path in body wins over the active profile."""
    cfg.code_profiles["main"] = "/wrong/path"
    cfg.active_code_profile = "main"

    fake_db = MagicMock(
        list_schemas=MagicMock(return_value=[]),
        list_tables=MagicMock(return_value=[]),
    )
    monkeypatch.setattr(
        "amx.db.connector.DatabaseConnector",
        lambda cfg: fake_db,
    )

    captured: dict[str, str] = {}

    def fake_analyze(path, **kw):
        captured["path"] = path
        return MagicMock(
            total_files=0,
            scanned_files=0,
            references={},
            external_mentions={},
        )

    monkeypatch.setattr("amx.codebase.analyzer.analyze_codebase", fake_analyze)

    response = client.post(
        "/api/code/scan",
        headers=auth_headers,
        json={"path": "/explicit/path"},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    _wait_for_status(client, job_id, "done")
    assert captured["path"] == "/explicit/path"


def test_scan_with_column_scan_collects_columns(
    client, auth_headers, cfg, monkeypatch
) -> None:
    """When ``column_scan=true`` the worker should also enumerate
    column names and pass them to the analyzer."""
    cfg.code_profiles["repo"] = "/path"

    fake_col = MagicMock(name="col1")
    fake_col.name = "user_id"
    fake_db = MagicMock(
        list_schemas=MagicMock(return_value=["public"]),
        list_tables=MagicMock(return_value=["users"]),
        list_column_profiles=MagicMock(return_value=[fake_col]),
    )
    monkeypatch.setattr(
        "amx.db.connector.DatabaseConnector",
        lambda cfg: fake_db,
    )

    captured: dict[str, Any] = {}

    def fake_analyze(path, **kw):
        captured["table_names"] = list(kw.get("table_names") or [])
        captured["column_names"] = list(kw.get("column_names") or [])
        return MagicMock(
            total_files=1,
            scanned_files=1,
            references={},
            external_mentions={},
        )

    monkeypatch.setattr("amx.codebase.analyzer.analyze_codebase", fake_analyze)

    response = client.post(
        "/api/code/scan",
        headers=auth_headers,
        json={"profile": "repo", "column_scan": True},
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]
    _wait_for_status(client, job_id, "done")
    assert "users" in captured["table_names"]
    assert "user_id" in captured["column_names"]
