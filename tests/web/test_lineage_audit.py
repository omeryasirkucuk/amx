"""Audit-trail endpoint."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.config import DBConfig
from amx.storage import sqlite_store as sqlite_store_module
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def seeded_hs(tmp_path: Path, monkeypatch):
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind) "
            "VALUES ('local','postgresql','','public','orders','table','table')"
        )
        conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind) "
            "VALUES ('local','postgresql','','public','customers','table','table')"
        )
        now = time.time()
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, "
            "details_json, last_seen, verdict, audit_actor, audit_at) "
            "VALUES (1, 2, 'lineage_llm', 0.7, 'llm', '{}', ?, 'approved', 'alice', ?)",
            (now, now),
        )
        conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source, "
            "details_json, last_seen, verdict, audit_actor, audit_at) "
            "VALUES (2, 1, 'lineage_manual', 1.0, 'manual', '{}', ?, 'approved', 'bob', ?)",
            (now - 60, now - 60),
        )
    monkeypatch.setattr(sqlite_store_module, "_store", hs, raising=False)
    return hs


@pytest.fixture()
def cfg(cfg):
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "local"
    return cfg


def test_audit_returns_recent_verdicts(seeded_hs, client, auth_headers):
    r = client.get("/api/lineage/audit", headers=auth_headers, params={"profile": "local"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 2
    entries = body["entries"]
    # Most recent first.
    assert entries[0]["actor"] == "alice"
    assert entries[0]["verdict"] == "approved"
    assert entries[0]["relationship_type"] == "lineage_llm"
    assert entries[1]["actor"] == "bob"
    assert entries[1]["relationship_type"] == "lineage_manual"
