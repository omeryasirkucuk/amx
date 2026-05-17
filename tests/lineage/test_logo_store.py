"""Tests for the lineage logo registry + logo-node CRUD."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from amx.lineage.default_logos import DEFAULT_LOGOS
from amx.lineage.logo_store import (
    LogoStoreError,
    create_custom_logo,
    create_logo_node,
    delete_custom_logo,
    delete_logo_node,
    list_logo_nodes,
    list_logos,
    lookup_logo_by_key,
    seed_default_logos,
    update_logo_node,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def hs(tmp_path: Path) -> SQLiteHistoryStore:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    return store


def _png_data_url() -> str:
    """A 1x1 transparent PNG, base64-encoded as a data URL.

    Used for upload-validation tests so we don't need a real file
    on disk."""
    raw = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDAT\x08\xd7c\xf8"
        b"\x0f\x00\x00\x01\x01\x00\x05\xfe\x02\xfe\xdc\xccY\xe7\x00\x00\x00"
        b"\x00IEND\xaeB`\x82"
    )
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")


def test_init_seeds_20_default_logos(hs: SQLiteHistoryStore):
    """``init`` runs ``seed_default_logos`` so a fresh store has every default."""
    logos = list_logos(hs)
    keys = [r["key"] for r in logos if r["source"] == "default"]
    assert len(keys) == len(DEFAULT_LOGOS)
    assert set(keys) == {logo.key for logo in DEFAULT_LOGOS}


def test_seed_is_idempotent(hs: SQLiteHistoryStore):
    before = len(list_logos(hs))
    inserted = seed_default_logos(hs)
    after = len(list_logos(hs))
    assert inserted == 0, "second seed pass should insert nothing"
    assert before == after


def test_create_custom_logo_then_delete(hs: SQLiteHistoryStore):
    before = len(list_logos(hs))
    created = create_custom_logo(
        hs,
        key="myco",
        label="My Co",
        category="custom",
        data_url=_png_data_url(),
    )
    assert created["source"] == "custom"
    assert created["key"] == "myco"
    assert len(list_logos(hs)) == before + 1

    delete_custom_logo(hs, int(created["id"]))
    assert len(list_logos(hs)) == before


def test_default_logo_cannot_be_deleted(hs: SQLiteHistoryStore):
    default_id = next(r["id"] for r in list_logos(hs) if r["key"] == "aws")
    with pytest.raises(LogoStoreError) as exc:
        delete_custom_logo(hs, int(default_id))
    assert exc.value.status_code == 403


def test_logo_validation_rejects_oversize(hs: SQLiteHistoryStore):
    big = "data:image/png;base64," + ("A" * (200 * 1024 + 100))
    with pytest.raises(LogoStoreError) as exc:
        create_custom_logo(hs, key="big", label="Big", data_url=big)
    assert exc.value.status_code == 413


def test_logo_validation_rejects_bad_mime(hs: SQLiteHistoryStore):
    with pytest.raises(LogoStoreError) as exc:
        create_custom_logo(
            hs,
            key="vid",
            label="Video",
            data_url="data:video/mp4;base64,AAAA",
        )
    assert exc.value.status_code == 415


def test_logo_validation_rejects_bad_key(hs: SQLiteHistoryStore):
    with pytest.raises(LogoStoreError) as exc:
        create_custom_logo(
            hs,
            key="UPPERCASE BAD",
            label="x",
            data_url=_png_data_url(),
        )
    assert exc.value.status_code == 400


def test_lookup_prefers_custom_over_default(hs: SQLiteHistoryStore):
    """When a custom row shadows a default key, lookup returns the custom."""
    create_custom_logo(
        hs,
        key="aws",
        label="My Custom AWS",
        category="custom",
        data_url=_png_data_url(),
    )
    hit = lookup_logo_by_key(hs, "aws")
    assert hit is not None
    assert hit["source"] == "custom"
    assert hit["label"] == "My Custom AWS"


def test_logo_node_crud_round_trip(hs: SQLiteHistoryStore):
    """Logo nodes survive a save → list → patch → delete cycle."""
    # Stand up a minimal artifact row so the FK on lineage_logo_nodes
    # has something to point at.
    import time

    now = time.time()
    with hs._connect() as conn:
        # We need a placeholder catalog_entities row for anchor_entity_id.
        cur = conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, entity_kind) "
            "VALUES ('p', 'postgresql', 'db1', 'public', 't', 'table')"
        )
        anchor_id = int(cur.lastrowid)
        cur = conn.execute(
            "INSERT INTO lineage_artifacts "
            "(name, db_profile, anchor_entity_id, depth_up, depth_down, "
            " format, output_path, edge_set_hash, node_count, edge_count, "
            " generated_at, extractors_used) "
            "VALUES ('t', 'p', ?, 1, 1, 'svg', '/tmp/t.svg', '', 0, 0, ?, '[]')",
            (anchor_id, now),
        )
        artifact_id = int(cur.lastrowid)

    node = create_logo_node(hs, artifact_id, logo_key="powerbi", label="Sales BI", x=100, y=200)
    assert node["id"] > 0
    nodes = list_logo_nodes(hs, artifact_id)
    assert len(nodes) == 1
    assert nodes[0]["logo_key"] == "powerbi"
    assert nodes[0]["label"] == "Sales BI"

    update_logo_node(
        hs,
        artifact_id,
        int(node["id"]),
        payload={"x": 300, "label": "Sales BI (prod)"},
    )
    nodes = list_logo_nodes(hs, artifact_id)
    assert nodes[0]["x"] == 300
    assert nodes[0]["label"] == "Sales BI (prod)"

    delete_logo_node(hs, artifact_id, int(node["id"]))
    assert list_logo_nodes(hs, artifact_id) == []


def test_logo_node_blocks_logo_delete_when_in_use(hs: SQLiteHistoryStore):
    """A logo referenced by a canvas node can't be deleted (409)."""
    import time

    now = time.time()
    # Insert a custom logo + an artifact + a logo_node referencing it.
    custom = create_custom_logo(hs, key="myco2", label="My Co 2", data_url=_png_data_url())
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities "
            "(db_profile, db_backend, database_name, schema_name, table_name, entity_kind) "
            "VALUES ('p', 'postgresql', 'db1', 'public', 't2', 'table')"
        )
        anchor_id = int(cur.lastrowid)
        cur = conn.execute(
            "INSERT INTO lineage_artifacts "
            "(name, db_profile, anchor_entity_id, depth_up, depth_down, "
            " format, output_path, edge_set_hash, node_count, edge_count, "
            " generated_at, extractors_used) "
            "VALUES ('t', 'p', ?, 1, 1, 'svg', '/tmp/t.svg', '', 0, 0, ?, '[]')",
            (anchor_id, now),
        )
        artifact_id = int(cur.lastrowid)
    create_logo_node(hs, artifact_id, logo_id=int(custom["id"]))

    with pytest.raises(LogoStoreError) as exc:
        delete_custom_logo(hs, int(custom["id"]))
    assert exc.value.status_code == 409
