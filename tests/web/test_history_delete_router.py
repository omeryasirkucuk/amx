"""FastAPI tests for run deletion and table-review clearing.

Exercises ``DELETE /api/history/runs/{id}``, the bulk
``POST /api/history/runs/delete``, and
``POST /api/live/schemas/{schema}/tables/{table}/reviews/clear`` against a
real ``SQLiteHistoryStore`` pinned as the module singleton, mirroring how
the Studio process runs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from amx.storage import sqlite_store as _store_module
from amx.storage.sqlite_store import SQLiteHistoryStore
from amx.web.routers.history import router as history_router
from amx.web.routers.live_db import router as live_db_router


@pytest.fixture
def store(tmp_path: Path):
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    _store_module._store = s
    yield s
    _store_module._store = None


@pytest.fixture
def client(store) -> TestClient:
    app = FastAPI()
    app.include_router(history_router)
    app.include_router(live_db_router)
    return TestClient(app)


def _seed(s: SQLiteHistoryStore, *, schema: str = "s", table: str = "t") -> tuple[int, list[int]]:
    rid = s.create_run(
        command="analyze.run",
        mode="x",
        db_backend="sqlite",
        db_profile="p",
        llm_provider="lp",
        llm_model="m",
        scope={schema: [table]},
    )
    ids = s.save_run_results(
        rid,
        [
            {
                "schema": schema,
                "table": table,
                "column": None,
                "asset_kind": "table",
                "source": "llm",
                "confidence": "high",
                "alternatives": [{"text": "a"}],
            }
        ],
    )
    return rid, ids


def test_delete_run_ok(client: TestClient, store: SQLiteHistoryStore) -> None:
    rid, _ = _seed(store)
    r = client.delete(f"/api/history/runs/{rid}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["deleted"] is True
    assert body["counts"]["runs"] == 1
    assert store.get_run(rid) is None


def test_delete_run_missing_404(client: TestClient) -> None:
    r = client.delete("/api/history/runs/999999")
    assert r.status_code == 404


def test_bulk_delete_by_ids(client: TestClient, store: SQLiteHistoryStore) -> None:
    r1, _ = _seed(store)
    r2, _ = _seed(store)
    r = client.post("/api/history/runs/delete", json={"run_ids": [r1, r2]})
    assert r.status_code == 200, r.text
    assert r.json()["counts"]["runs"] == 2
    assert store.list_recent_runs(command_filter=None) == []


def test_bulk_delete_all_matching(client: TestClient, store: SQLiteHistoryStore) -> None:
    _seed(store)
    _seed(store)
    r = client.post(
        "/api/history/runs/delete",
        json={"all_matching": {"command": "analyze.run"}},
    )
    assert r.status_code == 200, r.text
    assert r.json()["counts"]["runs"] == 2


def test_bulk_delete_all_matching_empty_filter_400(
    client: TestClient, store: SQLiteHistoryStore
) -> None:
    _seed(store)
    # command=all normalizes to no command filter, and no other key is set,
    # so the store guard rejects the wipe-everything request.
    r = client.post("/api/history/runs/delete", json={"all_matching": {"command": "all"}})
    assert r.status_code == 400


def test_bulk_delete_both_selectors_400(client: TestClient) -> None:
    r = client.post(
        "/api/history/runs/delete",
        json={"run_ids": [1], "all_matching": {"command": "analyze.run"}},
    )
    assert r.status_code == 400


def test_clear_table_reviews_endpoint(
    client: TestClient, store: SQLiteHistoryStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr

    pending_file = tmp_path / "pending_metadata.json"
    monkeypatch.setattr(pr, "PENDING_FILE", pending_file)
    pending_file.write_text(
        json.dumps([{"schema": "s", "table": "t", "column": None, "final_description": "td"}]),
        encoding="utf-8",
    )
    rid, ids = _seed(store)
    store.record_evaluation(ids[0], chosen_description="d", evaluation="accepted")
    store.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    r = client.post("/api/live/schemas/s/tables/t/reviews/clear", json={})
    assert r.status_code == 200, r.text
    counts = r.json()["counts"]
    assert counts == {"pending": 1, "review_state": 1, "audit": 1}
    assert store.list_apply_events() == []


def test_clear_table_reviews_endpoint_flags(
    client: TestClient, store: SQLiteHistoryStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import amx.pending_review as pr

    monkeypatch.setattr(pr, "PENDING_FILE", tmp_path / "pending.json")
    rid, ids = _seed(store)
    store.record_apply_event(
        schema_name="s", table_name="t", new_comment="d", run_id=rid, result_id=ids[0]
    )

    r = client.post(
        "/api/live/schemas/s/tables/t/reviews/clear",
        json={"pending": False, "review_state": False, "audit": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["counts"] == {"pending": 0, "review_state": 0, "audit": 1}
