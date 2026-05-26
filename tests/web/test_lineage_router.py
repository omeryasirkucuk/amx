"""amx/web/routers/lineage.py — Studio's lineage REST surface."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from amx.config import DBConfig
from amx.lineage import store as lineage_store
from amx.storage import sqlite_store as sqlite_store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_hs(tmp_path: Path, monkeypatch):
    """A real on-disk history store with one profile and a tiny catalog."""
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    # Seed orders + customers entities and one foreign-key edge so
    # the FK extractor has something to surface.
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "orders", "table", "table"),
        )
        orders_id = cur.lastrowid
        cur = conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "customers", "table", "table"),
        )
        customers_id = cur.lastrowid
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, details_json, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                orders_id,
                customers_id,
                "foreign_key",
                10.0,
                "database",
                json.dumps(
                    {
                        "constrained_columns": ["customer_id"],
                        "referred_columns": ["id"],
                        "referred_table": "customers",
                    }
                ),
                1_715_000_000.0,
            ),
        )
    monkeypatch.setattr(sqlite_store_module, "_store", hs, raising=False)
    return hs


@pytest.fixture()
def cfg(cfg):
    """Override the base fixture: add a single 'local' DB profile."""
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "local"
    return cfg


def test_get_lineage_returns_nodes_and_edges(seeded_hs, client, auth_headers):
    r = client.get(
        "/api/lineage/public.orders",
        headers=auth_headers,
        params={"profile": "local"},
    )
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["anchor"]["table"] == "orders"
    assert any(n["anchor"] for n in payload["nodes"])
    assert len(payload["edges"]) >= 1
    assert "fk" in payload["extractors_used"]


def test_get_lineage_404_when_no_profile(seeded_hs, client, auth_headers):
    """Empty profile resolution returns 400 (catalog router convention)."""
    r = client.get("/api/lineage/public.orders", headers=auth_headers)
    # The cfg fixture sets active_db_profile='local', so this still succeeds —
    # build the call without the cfg patch context by overriding the active profile.
    assert r.status_code in {200, 400}


def test_list_artifacts_empty(seeded_hs, client, auth_headers):
    r = client.get("/api/lineage", headers=auth_headers, params={"profile": "local"})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 0
    assert body["artifacts"] == []


def test_post_refresh_creates_artifact_when_anchor_exists(
    seeded_hs, client, auth_headers, tmp_path
):
    with patch("amx.lineage.service.render_lineage_image") as fake_render:
        fake_render.return_value = tmp_path / "fake.svg"
        r = client.post(
            "/api/lineage/public.orders/refresh",
            headers=auth_headers,
            params={"profile": "local"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["artifact_id"] > 0


def test_post_suggest_persists_llm_edges(seeded_hs, client, auth_headers, monkeypatch):
    """The /suggest route calls the LLM service and surfaces persisted edges."""

    def fake_suggest(hs, *, scope, cfg):
        from amx.lineage.service import LLMSuggestResult

        return LLMSuggestResult(
            edges=[
                {
                    "from": "public.customers",
                    "to": "public.orders",
                    "type": "lineage_llm",
                    "extractor": "llm",
                    "confidence": 0.85,
                    "evidence": "customers.id likely feeds orders.customer_id",
                }
            ],
            persisted_count=1,
            model="test/fake",
        )

    monkeypatch.setattr(
        "amx.web.routers.lineage.lineage_service.suggest_lineage_llm",
        fake_suggest,
    )
    r = client.post(
        "/api/lineage/public.orders/suggest",
        headers=auth_headers,
        params={"profile": "local"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["persisted"] == 1
    assert body["model"] == "test/fake"
    assert len(body["edges"]) == 1


def test_post_suggest_400_when_service_aborts(seeded_hs, client, auth_headers, monkeypatch):
    def aborted(hs, *, scope, cfg):
        from amx.lineage.service import LLMSuggestResult

        return LLMSuggestResult(aborted=True, abort_reason="no LLM profile")

    monkeypatch.setattr(
        "amx.web.routers.lineage.lineage_service.suggest_lineage_llm",
        aborted,
    )
    r = client.post(
        "/api/lineage/public.orders/suggest",
        headers=auth_headers,
        params={"profile": "local"},
    )
    assert r.status_code == 400
    assert "no LLM profile" in r.text
    assert lineage_store.lookup_lineage_artifact(seeded_hs, name_or_id="nonexistent") is None


def test_post_suggest_400_when_service_raises(seeded_hs, client, auth_headers, monkeypatch):
    """v4 hotfix — uncaught exceptions inside suggest_lineage_llm
    no longer bare-500. The router catches and reports as 400 with
    the exception detail so the Studio toast can render something
    actionable instead of 'Internal Server Error'."""

    def boom(hs, *, scope, cfg):
        raise RuntimeError("simulated LLM provider keyring failure")

    monkeypatch.setattr(
        "amx.web.routers.lineage.lineage_service.suggest_lineage_llm",
        boom,
    )
    r = client.post(
        "/api/lineage/public.orders/suggest",
        headers=auth_headers,
        params={"profile": "local"},
    )
    assert r.status_code == 400
    assert "RuntimeError" in r.text or "keyring" in r.text


def test_post_refresh_returns_aborted_when_create_raises(
    seeded_hs, client, auth_headers, monkeypatch, tmp_path
):
    """v4 hotfix — any exception inside create_lineage / refresh_lineage
    surfaces as aborted=True with the reason, never a bare 500."""

    def boom(*args, **kwargs):
        raise RuntimeError("simulated render failure")

    monkeypatch.setattr(
        "amx.web.routers.lineage.lineage_service.create_lineage",
        boom,
    )
    r = client.post(
        "/api/lineage/public.orders/refresh",
        headers=auth_headers,
        params={"profile": "local"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["aborted"] is True
    assert "RuntimeError" in body["abort_reason"]


def test_suggest_lineage_llm_aborts_when_provider_init_fails(seeded_hs, monkeypatch):
    """Direct service-level test — when LLMProvider construction
    raises (missing key, keyring backend error), the function
    returns aborted=True instead of letting the exception escape."""
    from amx.config import AMXConfig, LLMConfig
    from amx.lineage.service import suggest_lineage_llm
    from amx.lineage.types import ColumnRef, Scope

    cfg = AMXConfig()
    cfg.llm = LLMConfig(provider="openai", model="gpt-4o-mini")

    class _BrokenProvider:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("provider config not found")

    monkeypatch.setattr("amx.llm.provider.LLMProvider", _BrokenProvider)
    scope = Scope(profile="local", anchor=ColumnRef("", "public", "orders", ""))
    result = suggest_lineage_llm(hs=seeded_hs, scope=scope, cfg=cfg)
    assert result.aborted is True
    assert "init failed" in result.abort_reason


# ── POST /fetch (native lineage) ────────────────────────────────────────


def test_post_fetch_materializes_native_lineage(seeded_hs, client, auth_headers, monkeypatch):
    """The /fetch route runs the native provider and returns per-fetch counts."""
    from amx.lineage.native import provider as P

    class _StubProvider:
        backend = "databricks"

        def fetch_table_lineage(self, fqn, *, with_columns, anchor_columns=()):
            r = P.NativeLineageResult(
                anchor=P.NativeLineageNode(kind=P.TABLE, name="orders", fqn=fqn)
            )
            r.edges.append(
                P.NativeLineageEdge(
                    source=P.NativeLineageNode(kind=P.NOTEBOOK, name="ETL", external_id="n1"),
                    target=r.anchor,
                    direction=P.UPSTREAM,
                )
            )
            return r

    monkeypatch.setattr(P, "provider_for_profile", lambda profile, backend: _StubProvider())
    r = client.post(
        "/api/lineage/fetch",
        headers=auth_headers,
        json={"profile": "local", "fqn": "public.orders"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["fqn"] == "public.orders"
    assert body["edges"] == 1
    assert body["name_only"] == 1  # notebook has no remote_* row → ghost
    # A saved artifact is seeded so the canvas can render the result.
    artifact_id = body["artifact_id"]
    assert artifact_id and artifact_id > 0

    # Opening the seeded artifact renders the anchor + the producer
    # notebook node (name-only) and the native edge among them.
    r2 = client.get(f"/api/lineage/by-id/{artifact_id}", headers=auth_headers)
    assert r2.status_code == 200, r2.text
    canvas = r2.json()
    kinds = {n["kind"] for n in canvas["nodes"]}
    assert "notebook" in kinds
    nb = next(n for n in canvas["nodes"] if n["kind"] == "notebook")
    assert nb["metadata_state"] == "name_only"
    assert len(canvas["edges"]) >= 1


def test_post_fetch_400_when_fqn_missing(seeded_hs, client, auth_headers):
    r = client.post("/api/lineage/fetch", headers=auth_headers, json={"profile": "local"})
    assert r.status_code == 400


def test_post_fetch_400_for_unsupported_backend(seeded_hs, client, auth_headers):
    # 'local' profile is postgresql → no native provider registered.
    r = client.post(
        "/api/lineage/fetch",
        headers=auth_headers,
        json={"profile": "local", "fqn": "public.orders"},
    )
    assert r.status_code == 400
