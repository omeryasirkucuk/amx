"""POST /api/catalog/sync/cancel — cooperative cancel surface."""

from __future__ import annotations

from amx.search import _skeleton_jobs


def setup_function() -> None:
    _skeleton_jobs._jobs.clear()


def test_cancel_returns_true_when_job_registered(client, auth_headers) -> None:
    _skeleton_jobs.register("prod_dwh")
    response = client.post(
        "/api/catalog/sync/cancel",
        json={"profile": "prod_dwh"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == {"profile": "prod_dwh", "cancelled": True}


def test_cancel_returns_false_when_no_job(client, auth_headers) -> None:
    response = client.post(
        "/api/catalog/sync/cancel",
        json={"profile": "missing"},
        headers=auth_headers,
    )
    assert response.status_code == 200
    assert response.json() == {"profile": "missing", "cancelled": False}


def test_cancel_requires_non_empty_profile(client, auth_headers) -> None:
    response = client.post("/api/catalog/sync/cancel", json={"profile": ""}, headers=auth_headers)
    assert response.status_code == 422
