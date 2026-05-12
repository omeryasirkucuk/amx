"""Pagination + alternatives-state-survival contract tests for PR B.

The mandatory contract: when the SPA paginates a large run, the alternatives
selection a user makes on row N stays put after they navigate to another
page and back. Today, the source of truth lives server-side
(``PATCH /api/pending/{idx}`` persists ``final_description`` to the on-disk
pending file). The SPA reads the queue on mount and renders whatever is
there. So as long as PATCH is sticky across GETs, page navigation is
purely a client-render concern.

These tests pin the *server* half of that contract — the SPA's slice-by-page
behaviour is exercised by the frontend smoke build.
"""

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

    def push(rr: ReviewResult) -> None:
        state.append(rr)

    return {"state": state, "push": push, "Confidence": Confidence, "ReviewResult": ReviewResult}


def _make_rows(stub, count: int) -> None:
    """Seed ``count`` synthetic pending rows so the queue spans >1 page."""
    Confidence = stub["Confidence"]
    ReviewResult = stub["ReviewResult"]
    for i in range(count):
        stub["push"](
            ReviewResult(
                schema="sales",
                table=f"orders_{i}",
                column=None,
                final_description=f"row {i} initial",
                confidence=Confidence.MEDIUM,
                source="combined",
                applied=True,
                asset_kind="table",
                result_id=1000 + i,
                alternatives=[f"alt-{i}-a", f"alt-{i}-b", f"alt-{i}-c"],
            )
        )


def test_pending_list_returns_every_row_with_stable_idx(client, auth_headers, stub_pending) -> None:
    """The SPA paginates client-side over the full list, so the API has to
    surface every row with a stable ``idx`` that survives between GETs."""
    _make_rows(stub_pending, count=75)
    resp = client.get("/api/pending", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 75
    rows = body["pending"]
    assert [r["idx"] for r in rows] == list(range(75))
    # And re-reading returns the same idx ordering — required so the SPA's
    # cached page-2 slice can re-render the correct row 50..74 after
    # ``Prev``/``Next``.
    again = client.get("/api/pending", headers=auth_headers).json()
    assert [r["idx"] for r in again["pending"]] == list(range(75))


def test_alternatives_selection_survives_page_navigation(
    client, auth_headers, stub_pending
) -> None:
    """User selects alternative 2 on row 5 (page 1). User goes to page 2 then
    back to page 1. Row 5 still shows alternative 2 selected (because the
    state lives server-side and the SPA re-fetches on each page change OR
    keeps a stable client cache keyed by result_id).

    Simulation: PATCH the chosen alternative, GET twice (mimicking the
    SPA's re-mount on page change), and confirm the row still carries the
    same ``final_description``.
    """
    _make_rows(stub_pending, count=75)

    # 1. Inspect the initial row 5 — its final_description is the default.
    page1 = client.get("/api/pending", headers=auth_headers).json()["pending"]
    target = page1[5]
    assert target["final_description"] == "row 5 initial"
    alternatives = target["alternatives"]
    assert "alt-5-b" in alternatives  # the "alt 2" the user wants

    # 2. PATCH /api/pending/5 with the picked alternative as final_description.
    chosen = "alt-5-b"
    patched = client.patch(
        "/api/pending/5",
        headers=auth_headers,
        json={"final_description": chosen},
    )
    assert patched.status_code == 200
    assert patched.json()["final_description"] == chosen

    # 3. Simulate the SPA navigating to page 2: a fresh GET. Row 5 is no
    # longer on the rendered slice, but the server-side state is intact.
    intermediate = client.get("/api/pending", headers=auth_headers).json()["pending"]
    assert intermediate[5]["final_description"] == chosen

    # 4. Simulate navigating back to page 1: another GET. Row 5 still
    # carries the chosen alternative — the contract holds.
    back = client.get("/api/pending", headers=auth_headers).json()["pending"]
    assert back[5]["final_description"] == chosen
    assert back[5]["result_id"] == 1005


def test_patch_does_not_disturb_neighbouring_rows(client, auth_headers, stub_pending) -> None:
    """Editing one row must not perturb the idx → row mapping for any
    other row — otherwise pagination indices would slide between GETs."""
    _make_rows(stub_pending, count=20)
    client.patch(
        "/api/pending/7",
        headers=auth_headers,
        json={"final_description": "edited row 7"},
    )
    rows = client.get("/api/pending", headers=auth_headers).json()["pending"]
    assert rows[7]["final_description"] == "edited row 7"
    for i, r in enumerate(rows):
        if i == 7:
            continue
        assert r["final_description"] == f"row {i} initial", f"row {i} drifted after PATCH of row 7"
        assert r["idx"] == i
