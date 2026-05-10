"""FastAPI tests for the pricing router endpoints.

Locks the response shape of ``GET /api/pricing/models`` — the new
catalog endpoint that powers the Studio topbar dialog and the
dedicated ``/pricing`` page. Without these, a silent rename or a
field-shape change in the router would only blow up at runtime in
the browser, far away from CI.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from amx.llm import pricing as pricing_mod
from amx.llm.pricing import ModelPrice, reset_state_for_tests


@pytest.fixture(autouse=True)
def _reset_pricing_state():
    """Each test starts with a clean in-memory price cache so one
    test's seeded models don't leak into the next."""
    reset_state_for_tests()
    yield
    reset_state_for_tests()


def test_get_models_returns_bundled_fallback_with_expected_shape(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    """A clean install hits this endpoint with no network refresh —
    the bundled fallback alone must produce a valid 200 response so
    the topbar dialog never empty-states on day one."""
    response = client.get("/api/pricing/models", headers=auth_headers)
    assert response.status_code == 200, response.text
    payload = response.json()

    assert set(payload.keys()) == {"models", "fetched_at", "is_stale"}
    assert isinstance(payload["models"], list)
    assert len(payload["models"]) > 0, "expected bundled fallback to populate the catalog"

    sample = payload["models"][0]
    assert set(sample.keys()) == {
        "model_id",
        "provider_hint",
        "input_per_mtok",
        "output_per_mtok",
        "source",
        "fetched_at",
    }
    assert isinstance(sample["model_id"], str) and sample["model_id"]
    assert isinstance(sample["input_per_mtok"], (int, float))
    assert isinstance(sample["output_per_mtok"], (int, float))
    assert sample["source"] in {"litellm", "openrouter", "fallback"}


def test_get_models_returns_litellm_priority_when_keys_collide(
    client: TestClient, auth_headers: dict[str, str]
) -> None:
    """When the same model id sits in both the litellm and openrouter
    in-memory caches, the endpoint must echo only the litellm row —
    matching ``lookup_price``'s resolution priority. Without this,
    Studio could render a price the actual run would not bill at."""
    pricing_mod._ensure_loaded()
    pricing_mod._PRICES["litellm"]["openai/gpt-priority-test"] = ModelPrice(
        input_per_mtok=0.42, output_per_mtok=1.42, source="litellm", fetched_at=10.0
    )
    pricing_mod._PRICES["openrouter"]["openai/gpt-priority-test"] = ModelPrice(
        input_per_mtok=999.0, output_per_mtok=999.0, source="openrouter", fetched_at=20.0
    )

    response = client.get("/api/pricing/models", headers=auth_headers)
    payload = response.json()
    matching = [m for m in payload["models"] if m["model_id"] == "openai/gpt-priority-test"]
    assert len(matching) == 1, "duplicate keys must be deduped before the endpoint emits them"
    assert matching[0]["source"] == "litellm"
    assert matching[0]["input_per_mtok"] == 0.42


def test_get_models_is_stale_flag_mirrors_cache_info(
    client: TestClient, auth_headers: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dialog renders a yellow 'prices > 24h old' banner driven by
    ``is_stale``. Mock cache_info so the flag is forced True and
    confirm the endpoint surfaces it."""
    monkeypatch.setattr(
        pricing_mod,
        "cache_info",
        lambda: {
            "fetched_at": 1.0,
            "age_seconds": 999_999.0,
            "ttl_seconds": 60.0,
            "is_stale": True,
            "litellm_count": 0,
            "openrouter_count": 0,
            "fallback_count": 30,
        },
    )

    response = client.get("/api/pricing/models", headers=auth_headers)
    payload = response.json()
    assert payload["is_stale"] is True


def test_get_models_unauthenticated_request_is_rejected(client: TestClient) -> None:
    """The token gate has to apply to the new endpoint too — Studio
    is single-user but the TestClient drives requests from anywhere,
    and this is the cheap regression that ensures we did not forget
    to wire the new route into the auth dependency chain."""
    response = client.get("/api/pricing/models")
    assert response.status_code in {401, 403}, (
        f"expected 401/403 without auth headers, got {response.status_code}: {response.text}"
    )
