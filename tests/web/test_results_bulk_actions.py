"""Tests for the PR B bulk pending-action endpoint."""

from __future__ import annotations

import pytest


@pytest.fixture()
def stub_pending(monkeypatch):
    """In-memory stand-in for the on-disk pending file."""
    from amx.agents.base import Confidence
    from amx.agents.orchestrator import ReviewResult

    state: list[ReviewResult] = []

    def fake_load_pending() -> list[ReviewResult]:
        return list(state)

    def fake_save_pending(results) -> None:
        state.clear()
        state.extend(results)

    def fake_clear_pending() -> None:
        state.clear()

    monkeypatch.setattr("amx.web.routers.pending.load_pending", fake_load_pending)
    monkeypatch.setattr("amx.web.routers.pending.save_pending", fake_save_pending)
    monkeypatch.setattr("amx.web.routers.pending.clear_pending", fake_clear_pending)

    def push(**kwargs) -> None:
        defaults = {
            "schema": "sales",
            "table": "orders",
            "column": None,
            "final_description": "desc",
            "confidence": Confidence.MEDIUM,
            "source": "combined",
            "applied": True,
            "asset_kind": "table",
            "result_id": None,
            "alternatives": [],
        }
        defaults.update(kwargs)
        state.append(ReviewResult(**defaults))

    return {"state": state, "push": push}


def test_bulk_accept_handles_mixed_ids(client, auth_headers, stub_pending) -> None:
    """Bulk accept on 3 idx values returns 200 with per-id status."""
    for i in range(5):
        stub_pending["push"](
            table=f"t{i}",
            final_description=f"row {i}",
            applied=False,
            result_id=2000 + i,
        )

    resp = client.post(
        "/api/pending/bulk",
        headers=auth_headers,
        json={"ids": [0, 2, 4, 99], "action": "accept"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["action"] == "accept"
    statuses = {row["idx"]: row["status"] for row in body["processed"]}
    assert statuses == {0: "ok", 2: "ok", 4: "ok", 99: "not_found"}
    # ``accept`` stamps applied=True without removing rows.
    assert body["remaining"] == 5
    rows = stub_pending["state"]
    assert rows[0].applied is True
    assert rows[2].applied is True
    assert rows[4].applied is True


def test_bulk_skip_removes_targeted_rows(client, auth_headers, stub_pending) -> None:
    for i in range(5):
        stub_pending["push"](
            table=f"t{i}",
            final_description=f"row {i}",
            result_id=3000 + i,
        )
    resp = client.post(
        "/api/pending/bulk",
        headers=auth_headers,
        json={"ids": [1, 3], "action": "skip"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["remaining"] == 3
    # The surviving result_ids should be 3000, 3002, 3004 — bulk skip
    # must remove rows by idx descending so the remaining positions
    # stay coherent.
    surviving = [r.result_id for r in stub_pending["state"]]
    assert surviving == [3000, 3002, 3004]


def test_bulk_action_validates_action_string(client, auth_headers, stub_pending) -> None:
    """An unknown action string returns 400 without touching state."""
    stub_pending["push"](table="t0", final_description="row 0", result_id=4000)
    resp = client.post(
        "/api/pending/bulk",
        headers=auth_headers,
        json={"ids": [0], "action": "ignite"},
    )
    assert resp.status_code == 400
    assert "Invalid action" in resp.json()["detail"]
    # State untouched.
    assert len(stub_pending["state"]) == 1


def test_bulk_with_empty_ids_is_a_noop(client, auth_headers, stub_pending) -> None:
    stub_pending["push"](table="t0", final_description="row 0", result_id=5000)
    resp = client.post(
        "/api/pending/bulk",
        headers=auth_headers,
        json={"ids": [], "action": "skip"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["remaining"] == 1
    assert body["processed"] == []
