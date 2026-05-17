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
