"""Tests for the /api/schedules and /api/scheduler Studio routes.

These exercise the FastAPI surface end-to-end against a real
SQLiteHistoryStore pinned as the module-level singleton, mirroring
how the Studio process runs in production.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.routers.schedules import router as schedules_router
from amx.web.routers.schedules import scheduler_router


@pytest.fixture
def app_with_store(tmp_path: Path):
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    _store_module._store = s

    app = FastAPI()
    app.include_router(schedules_router)
    app.include_router(scheduler_router)

    yield app, s
    _store_module._store = None


@pytest.fixture
def client(app_with_store) -> TestClient:
    app, _ = app_with_store
    return TestClient(app)


def _create_via_api(client: TestClient, **overrides):
    body = {
        "name": "test",
        "fire_at_local": "2030-01-15T14:00",
        "fire_at_tz": "Europe/Istanbul",
        "db_profile": "prod_sf",
        "scope": {"mode": "schemas", "schemas": ["public"]},
        "llm_profile": "claude",
        "review_strategy": "auto",
    }
    body.update(overrides)
    return client.post("/api/schedules", json=body)


def test_create_and_get(client: TestClient) -> None:
    r = _create_via_api(client)
    assert r.status_code == 201, r.text
    sid = r.json()["id"]

    g = client.get(f"/api/schedules/{sid}")
    assert g.status_code == 200
    body = g.json()
    assert body["name"] == "test"
    assert body["status"] == "pending"
    assert body["fire_at_tz"] == "Europe/Istanbul"
    # fire_at_local round-trips back to wall-clock in chosen tz
    assert body["fire_at_local"].startswith("2030-01-15T14:00")


def test_list_default_returns_pending(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    _create_via_api(client, name="pending-one")
    rsp = client.get("/api/schedules")
    assert rsp.status_code == 200
    rows = rsp.json()["schedules"]
    assert any(r["name"] == "pending-one" for r in rows)


def test_patch_updates_fields(client: TestClient) -> None:
    sid = _create_via_api(client).json()["id"]
    r = client.patch(f"/api/schedules/{sid}", json={"name": "renamed"})
    assert r.status_code == 200
    assert r.json()["name"] == "renamed"


def test_patch_rejects_running_schedule(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    sid = _create_via_api(client).json()["id"]
    store.set_scheduled_run_status(sid, "running")
    r = client.patch(f"/api/schedules/{sid}", json={"name": "x"})
    assert r.status_code == 409


def test_pause_resume_round_trip(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    sid = _create_via_api(client).json()["id"]
    assert client.post(f"/api/schedules/{sid}/pause").status_code == 200
    assert store.get_scheduled_run(sid)["status"] == "paused"
    assert client.post(f"/api/schedules/{sid}/resume").status_code == 200
    assert store.get_scheduled_run(sid)["status"] == "pending"


def test_run_now_returns_fired(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    sid = _create_via_api(client).json()["id"]
    r = client.post(f"/api/schedules/{sid}/run-now")
    assert r.status_code == 202, r.text
    assert sid in r.json()["fired"]
    # Worker is background; just verify the schedule was transitioned.
    final = store.get_scheduled_run(sid)
    # ``failed`` is also acceptable here: this test fixtures a fake
    # ``prod_sf`` DB profile that the real production_run_executor
    # rejects at startup. The point of the test is the API surface
    # spawned a worker -- the worker's outcome is decided by the
    # executor, which is exercised in a dedicated test.
    assert final["status"] in ("running", "completed", "failed")


def test_delete_returns_204(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    sid = _create_via_api(client).json()["id"]
    assert client.delete(f"/api/schedules/{sid}").status_code == 204
    assert store.get_scheduled_run(sid) is None


def test_scheduler_status_endpoint(client: TestClient) -> None:
    _create_via_api(client)
    r = client.get("/api/scheduler/status")
    assert r.status_code == 200
    body = r.json()
    assert body["pending_count"] >= 1
    assert "daemon" in body


def test_scheduler_bootstrap_report_default(client: TestClient) -> None:
    r = client.get("/api/scheduler/bootstrap-report")
    assert r.status_code == 200
    body = r.json()
    assert body["missed_for_review"] == []


def test_manual_tick_endpoint(client: TestClient, app_with_store) -> None:
    _, store = app_with_store
    # A past-due schedule the daemon would normally fire.
    store.create_scheduled_run(
        name="due-now",
        fire_at_utc=time.time() - 60,
        fire_at_tz="UTC",
        db_profile="p",
        scope_json='{"mode":"all"}',
        llm_profile="l",
        review_strategy="auto",
    )
    r = client.post("/api/scheduler/tick")
    assert r.status_code == 200
    assert r.json()["fired"]


def test_create_rejects_bad_timezone(client: TestClient) -> None:
    r = _create_via_api(client, fire_at_tz="Mars/OlympusMons")
    assert r.status_code == 400


def test_create_rejects_unparseable_fire_at(client: TestClient) -> None:
    r = _create_via_api(client, fire_at_local="not-a-date")
    assert r.status_code == 400


def test_get_404_for_missing_id(client: TestClient) -> None:
    assert client.get("/api/schedules/999999").status_code == 404
