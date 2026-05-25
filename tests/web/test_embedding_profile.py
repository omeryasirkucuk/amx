"""Studio API for the docs/code embedding split.

These tests exercise the ``/api/profiles/embedding*`` endpoints with a
fresh :class:`AMXConfig` (no on-disk state) and a stubbed OpenAI client
factory, so they run without network or chromadb-tax surprises.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from amx.config import EmbeddingConfig
from amx.storage.secrets import InMemorySecretStore, set_default_store


@pytest.fixture()
def secret_store():
    store = InMemorySecretStore()
    set_default_store(store)
    yield store
    set_default_store(None)


# ── GET /api/profiles/embedding/kinds ──────────────────────────────────


def test_kinds_endpoint_returns_three_kinds_and_presets(client, auth_headers) -> None:
    response = client.get("/api/profiles/embedding/kinds", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    kind_ids = [k["id"] for k in body["kinds"]]
    assert kind_ids == ["minilm", "openai_compatible", "sentence_transformers"]
    assert {"openai", "vllm"} <= {p["id"] for p in body["presets"]}
    assert body["sides"] == ["docs", "code", "assets"]
    # MiniLM and OpenAI-compatible are always available; sentence-
    # transformers depends on the host, just assert the shape.
    minilm = next(k for k in body["kinds"] if k["id"] == "minilm")
    assert minilm["available"] is True


# ── GET /api/profiles/embedding ─────────────────────────────────────────


def test_get_returns_both_sides_masked(client, auth_headers, secret_store) -> None:
    response = client.get("/api/profiles/embedding", headers=auth_headers)
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"docs", "code", "assets"}
    for side in ("docs", "code", "assets"):
        block = body[side]
        assert block["kind"] == "minilm"
        assert block["is_configured"] is True
        assert block["api_key"] == ""  # no secret set yet → empty


# ── PUT /api/profiles/embedding/{side} ─────────────────────────────────


def test_put_docs_persists_and_masks_api_key(client, auth_headers, cfg, secret_store) -> None:
    response = client.put(
        "/api/profiles/embedding/docs",
        headers=auth_headers,
        json={
            "kind": "openai_compatible",
            "model": "text-embedding-3-small",
            "api_key": "sk-from-studio",
            "base_url": "https://api.openai.com/v1",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["kind"] == "openai_compatible"
    assert body["model"] == "text-embedding-3-small"
    # Response masks the secret.
    assert body["api_key"] == "********"
    # In-memory cfg was mutated to match.
    assert cfg.embedding_docs.kind == "openai_compatible"
    assert cfg.embedding_docs.model == "text-embedding-3-small"
    assert cfg.embedding_docs.api_key == "sk-from-studio"
    # Code side untouched.
    assert cfg.embedding_code.kind == "minilm"


def test_put_treats_masked_api_key_as_no_change(client, auth_headers, cfg, secret_store) -> None:
    # Seed an existing key.
    cfg.embedding_docs = EmbeddingConfig(
        kind="openai_compatible",
        model="text-embedding-3-small",
        api_key="sk-keep-me",
        base_url="https://api.openai.com/v1",
    )

    # Edit only the model — leave the placeholder in api_key.
    response = client.put(
        "/api/profiles/embedding/docs",
        headers=auth_headers,
        json={
            "kind": "openai_compatible",
            "model": "text-embedding-3-large",
            "api_key": "********",
            "base_url": "https://api.openai.com/v1",
        },
    )
    assert response.status_code == 200
    assert cfg.embedding_docs.model == "text-embedding-3-large"
    assert cfg.embedding_docs.api_key == "sk-keep-me"


def test_put_rejects_unknown_kind(client, auth_headers, cfg, secret_store) -> None:
    response = client.put(
        "/api/profiles/embedding/docs",
        headers=auth_headers,
        json={"kind": "magic-7b", "model": "anything"},
    )
    assert response.status_code == 400
    # cfg untouched
    assert cfg.embedding_docs.kind == "minilm"


def test_put_rejects_unknown_side(client, auth_headers, cfg, secret_store) -> None:
    response = client.put(
        "/api/profiles/embedding/other",
        headers=auth_headers,
        json={"kind": "minilm"},
    )
    assert response.status_code == 404


def test_put_code_does_not_touch_docs(client, auth_headers, cfg, secret_store) -> None:
    response = client.put(
        "/api/profiles/embedding/code",
        headers=auth_headers,
        json={"kind": "sentence_transformers", "model": "BAAI/bge-m3"},
    )
    assert response.status_code == 200
    assert cfg.embedding_code.kind == "sentence_transformers"
    assert cfg.embedding_code.model == "BAAI/bge-m3"
    assert cfg.embedding_docs.kind == "minilm"
    assert cfg.embedding_docs.model == ""


# ── POST /api/profiles/embedding/{side}/test ───────────────────────────


def test_test_endpoint_for_minilm_returns_ok(client, auth_headers, secret_store) -> None:
    response = client.post(
        "/api/profiles/embedding/docs/test",
        headers=auth_headers,
        json={"kind": "minilm"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["dim"] == 384


def test_test_endpoint_rejects_unconfigured_openai(client, auth_headers, secret_store) -> None:
    response = client.post(
        "/api/profiles/embedding/docs/test",
        headers=auth_headers,
        json={"kind": "openai_compatible", "model": "", "api_key": "", "base_url": ""},
    )
    assert response.status_code == 200  # endpoint returns ok=False, not 4xx
    body = response.json()
    assert body["ok"] is False
    assert "model" in body["message"].lower()


def test_test_endpoint_embeds_a_sentinel_with_stubbed_factory(
    client, auth_headers, secret_store, monkeypatch
) -> None:
    """The endpoint must build an OpenAI-compatible embedder from the
    merged body and embed a one-token sentinel without saving cfg."""
    calls: list[dict] = []

    class _FakeEmbeddings:
        @staticmethod
        def create(*, model, input):  # noqa: A002 — match OpenAI SDK signature
            calls.append({"model": model, "input": list(input)})
            return SimpleNamespace(
                data=[SimpleNamespace(embedding=[0.1, 0.2, 0.3, 0.4]) for _ in input]
            )

    class _FakeClient:
        embeddings = _FakeEmbeddings()

    def _fake_factory(*, api_key, base_url, timeout):  # noqa: ARG001
        return _FakeClient()

    monkeypatch.setattr("amx.search.embeddings._openai_client_factory", _fake_factory)

    response = client.post(
        "/api/profiles/embedding/docs/test",
        headers=auth_headers,
        json={
            "kind": "openai_compatible",
            "model": "text-embedding-3-small",
            "api_key": "sk-test",
            "base_url": "https://api.openai.com/v1",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    assert body["dim"] == 4
    assert calls and calls[0]["model"] == "text-embedding-3-small"


# ── GET /api/profiles/embedding/status ─────────────────────────────────


def test_status_endpoint_returns_every_side(client, auth_headers, secret_store) -> None:
    response = client.get("/api/profiles/embedding/status", headers=auth_headers)
    # Status may probe chromadb; the endpoint must always 200 with every
    # side key present (docs / code / assets) even when no collections
    # exist yet.
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) == {"docs", "code", "assets"}
    for side in ("docs", "code", "assets"):
        assert "collections" in body[side]
        assert "stale" in body[side]


def test_status_endpoint_reports_enriched_health_fields(client, auth_headers, secret_store) -> None:
    # Unified embedding management: every side reports configured-vs-active
    # plus the single needs_rebuild verdict so the health panel / CTAs can
    # act on it without re-deriving anything.
    body = client.get("/api/profiles/embedding/status", headers=auth_headers).json()
    for side in ("docs", "code", "assets"):
        s = body[side]
        for field in (
            "configured_provider",
            "configured_model",
            "fell_back",
            "fallback_reason",
            "dependency_available",
            "needs_rebuild",
        ):
            assert field in s, f"{side} missing {field}"
        # needs_rebuild is the OR of stale and fell_back.
        assert s["needs_rebuild"] == bool(s["stale"] or s["fell_back"])


# ── POST /api/profiles/embedding/rebuild (rebuild-all) ─────────────────


def test_rebuild_all_returns_aggregate_for_every_side(client, auth_headers, secret_store) -> None:
    # Always 200 with a per-side result list; a side whose backend is
    # unavailable is recorded in ``failed`` instead of aborting the others.
    response = client.post("/api/profiles/embedding/rebuild", headers=auth_headers, json={})
    assert response.status_code == 200, response.text
    body = response.json()
    assert "results" in body and "failed" in body and "ok" in body
    sides = {r["side"] for r in body["results"]}
    assert sides == {"docs", "code", "assets"}
    # ``ok`` is True only when nothing failed.
    assert body["ok"] == (not body["failed"])
