"""Manual-edge CRUD + verdict + save-canvas routes (v3 S4)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

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
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "orders", "table", "table"),
        )
        conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind) VALUES (?,?,?,?,?,?,?)",
            ("local", "postgresql", "", "public", "customers", "table", "table"),
        )
    monkeypatch.setattr(sqlite_store_module, "_store", hs, raising=False)
    return hs


@pytest.fixture()
def cfg(cfg):
    cfg.db_profiles = {"local": DBConfig(backend="postgresql", database="")}
    cfg.active_db_profile = "local"
    return cfg


def test_post_edge_persists_manual_edge(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.customers",
            "target_fqn": "public.orders",
            "notes": "FK pattern",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["from"] == "public.customers"
    assert body["to"] == "public.orders"
    assert body["verdict"] == "approved"
    assert body["id"] > 0

    with seeded_hs._connect() as conn:
        row = conn.execute(
            "SELECT relationship_type, source, verdict, audit_actor "
            "FROM catalog_relationships WHERE id = ?",
            (body["id"],),
        ).fetchone()
    assert row[0] == "lineage_manual"
    assert row[1] == "manual"
    assert row[2] == "approved"
    assert row[3]


def test_post_edge_rejects_self_loop(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.orders",
            "target_fqn": "public.orders",
        },
    )
    assert r.status_code == 400


def test_post_edge_404_when_endpoint_missing_from_catalog(seeded_hs, client, auth_headers):
    r = client.post(
        "/api/lineage/edges",
        headers=auth_headers,
        json={
            "profile": "local",
            "source_fqn": "public.unknown_table",
            "target_fqn": "public.orders",
        },
    )
    assert r.status_code == 404


def test_patch_verdict_updates_row(seeded_hs, client, auth_headers):
    # Insert an edge directly so we have an id to patch.
    with seeded_hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, 2, "lineage_llm", 0.7, "llm"),
        )
        edge_id = cur.lastrowid

    r = client.patch(
        f"/api/lineage/edges/{edge_id}/verdict",
        headers=auth_headers,
        json={"verdict": "rejected"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["verdict"] == "rejected"

    with seeded_hs._connect() as conn:
        row = conn.execute(
            "SELECT verdict, audit_actor FROM catalog_relationships WHERE id = ?",
            (edge_id,),
        ).fetchone()
    assert row[0] == "rejected"
    assert row[1]


def test_patch_verdict_rejects_unknown_value(seeded_hs, client, auth_headers):
    r = client.patch(
        "/api/lineage/edges/1/verdict",
        headers=auth_headers,
        json={"verdict": "maybe"},
    )
    assert r.status_code == 400


def test_delete_edge_removes_row(seeded_hs, client, auth_headers):
    with seeded_hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_relationships "
            "(from_entity_id, to_entity_id, relationship_type, score, source) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, 2, "lineage_manual", 1.0, "manual"),
        )
        edge_id = cur.lastrowid

    r = client.delete(f"/api/lineage/edges/{edge_id}", headers=auth_headers)
    assert r.status_code == 204

    with seeded_hs._connect() as conn:
        row = conn.execute(
            "SELECT id FROM catalog_relationships WHERE id = ?", (edge_id,)
        ).fetchone()
    assert row is None


def test_delete_edge_404_when_missing(seeded_hs, client, auth_headers):
    r = client.delete("/api/lineage/edges/99999", headers=auth_headers)
    assert r.status_code == 404


def test_post_manual_artifact_persists_edges_without_render(seeded_hs, client, auth_headers):
    # The save path must NOT render an image: the canvas always reopens
    # from by-id data (nodes + catalog_relationships), and rendering a
    # large native-lineage graph is slow enough to trip the proxy timeout
    # and 500 the save. So no matplotlib render should ever be invoked.
    with patch("amx.lineage.service.render_lineage_image") as fake_render:
        r = client.post(
            "/api/lineage/manual",
            headers=auth_headers,
            json={
                "profile": "local",
                "name": "my-flow",
                "anchor_fqn": "public.orders",
                "edges": [
                    {
                        "source_fqn": "public.customers",
                        "target_fqn": "public.orders",
                    }
                ],
            },
        )
    assert r.status_code == 201, r.text
    fake_render.assert_not_called()
    body = r.json()
    assert body["ok"] is True
    assert body["persisted_edges"] == 1
    assert body["artifact_id"] > 0

    # Manual edge landed in catalog_relationships with verdict=approved.
    with seeded_hs._connect() as conn:
        row = conn.execute(
            """
            SELECT verdict FROM catalog_relationships
            WHERE relationship_type = 'lineage_manual'
            """
        ).fetchone()
    assert row and row[0] == "approved"

    # The artifact row exists with no on-disk image (output_path empty).
    with seeded_hs._connect() as conn:
        art = conn.execute(
            "SELECT output_path FROM lineage_artifacts WHERE id = ?",
            (int(body["artifact_id"]),),
        ).fetchone()
    assert art is not None
    assert (art[0] or "") == ""


def test_post_manual_artifact_round_trips_asset_nodes_by_entity_id(seeded_hs, client, auth_headers):
    # Asset nodes (notebooks, jobs, …) have no FQN — they round-trip
    # purely by entity_id. A native-lineage canvas folds them into
    # buckets; on save they (and their edges to the anchor) must persist,
    # not silently vanish. Seed a notebook asset + resolve the orders id.
    with seeded_hs._connect() as conn:
        nb_id = conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, "
            "entity_kind, asset_kind, search_text) VALUES (?,?,?,?,?,?,?,?)",
            ("local", "databricks", "", "__assets", "ETL nb", "notebook", "notebook", "ETL nb"),
        ).lastrowid
        orders_id = conn.execute(
            "SELECT id FROM catalog_entities WHERE table_name = 'orders'"
        ).fetchone()[0]

    r = client.post(
        "/api/lineage/manual",
        headers=auth_headers,
        json={
            "profile": "local",
            "name": "native-flow",
            "anchor_fqn": "public.orders",
            "nodes": [
                {"fqn": "public.orders", "entity_id": orders_id, "x": 0, "y": 0},
                # The notebook carries no FQN — only its entity_id.
                {"fqn": "", "entity_id": nb_id, "x": -300, "y": 0},
            ],
            "edges": [
                {
                    "source_entity_id": nb_id,
                    "target_entity_id": orders_id,
                }
            ],
        },
    )
    assert r.status_code == 201, r.text
    artifact_id = int(r.json()["artifact_id"])
    assert r.json()["persisted_edges"] == 1

    # Both nodes (table + asset) landed as artifact placements.
    with seeded_hs._connect() as conn:
        placed = {
            int(x[0])
            for x in conn.execute(
                "SELECT entity_id FROM lineage_artifact_nodes WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchall()
        }
    assert {nb_id, orders_id} <= placed

    # The asset edge round-trips through the by-id reopen path.
    reopened = client.get(f"/api/lineage/by-id/{artifact_id}", headers=auth_headers)
    assert reopened.status_code == 200, reopened.text
    kinds = {n["kind"] for n in reopened.json()["nodes"]}
    assert "notebook" in kinds
    edge_pairs = {(e["from_entity_id"], e["to_entity_id"]) for e in reopened.json()["edges"]}
    assert (nb_id, orders_id) in edge_pairs
