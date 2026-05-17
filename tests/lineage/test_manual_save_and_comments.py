"""Regression + integration tests for the manual save flow.

Covers three concerns that the canvas rebuild introduces:

1. **Save-canvas name-as-anchor bug** — when the artifact name collides
   with a real table name, the re-open path must still resolve nodes
   by their persisted entity ids, not by the artifact's display name.
2. **Cross-profile lineage** — a single canvas may host nodes from
   multiple DB profiles. The manual save + by-id read must accept
   per-node profiles and surface them back unchanged.
3. **Comment CRUD** — sticky-note comments persist alongside the
   artifact and cascade on delete.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from amx.config import AMXConfig
from amx.storage.sqlite_store import SQLiteHistoryStore
from tests.lineage.conftest import seed_table_entity


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Spin up a FastAPI test client against the lineage router."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    # Pin the module-level singleton so the router resolves the same
    # store the test seeds against.
    monkeypatch.setattr("amx.web.routers.lineage.history_store", lambda: store, raising=True)

    # Build a minimal app with just the lineage router so we don't have
    # to spin up the full Studio.
    from fastapi import FastAPI

    from amx.web.deps import get_cfg
    from amx.web.routers.lineage import router as lineage_router

    cfg = AMXConfig()
    cfg.active_db_profile = "p1"
    cfg.db_profiles = {"p1": type("P", (), {"database": "db1"})()}

    app = FastAPI()
    app.dependency_overrides[get_cfg] = lambda: cfg
    app.include_router(lineage_router)

    client = TestClient(app)
    # Stash the store on the client so tests can seed it.
    client._store = store  # type: ignore[attr-defined]
    return client


def _seed_two_tables(store: SQLiteHistoryStore, *, profile: str = "p1") -> tuple[int, int]:
    """Two related tables in the same profile."""
    src = seed_table_entity(
        store,
        profile=profile,
        backend="postgresql",
        database="db1",
        schema="public",
        table="customers",
    )
    tgt = seed_table_entity(
        store,
        profile=profile,
        backend="postgresql",
        database="db1",
        schema="public",
        table="orders",
    )
    return src, tgt


def test_save_canvas_name_does_not_resolve_to_table(client: TestClient):
    """The artifact name must NOT participate in node resolution.

    Earlier, saving with a name that happened to match a table name
    caused the re-open code to interpret the name as an anchor FQN.
    The new by-id read path resolves only via persisted entity ids,
    so a colliding name has no effect.
    """
    store: SQLiteHistoryStore = client._store  # type: ignore[attr-defined]
    src, tgt = _seed_two_tables(store)

    res = client.post(
        "/api/lineage/manual",
        json={
            "profile": "p1",
            # Colliding name: same string as a real table on the canvas.
            "name": "orders",
            "anchor_fqn": "public.customers",
            "nodes": [
                {"profile": "p1", "fqn": "public.customers", "x": 10, "y": 10},
                {"profile": "p1", "fqn": "public.orders", "x": 220, "y": 10},
            ],
            "edges": [
                {
                    "source_fqn": "public.customers",
                    "target_fqn": "public.orders",
                    "source_profile": "p1",
                    "target_profile": "p1",
                }
            ],
            "comments": [],
        },
    )
    assert res.status_code == 201, res.text
    artifact_id = res.json()["artifact_id"]
    assert artifact_id > 0

    open_res = client.get(f"/api/lineage/by-id/{artifact_id}")
    assert open_res.status_code == 200, open_res.text
    body = open_res.json()
    # Two distinct nodes — neither one is the artifact name.
    fqns = sorted(n["fqn"] for n in body["nodes"])
    assert fqns == ["db1.public.customers", "db1.public.orders"]
    # The artifact name is preserved as display only and never mixed
    # into the node set.
    assert body["name"] == "orders"
    assert all(n["table"] in {"customers", "orders"} for n in body["nodes"])


def test_cross_profile_canvas_preserves_per_node_profile(client: TestClient):
    """Nodes saved with different profiles survive the round-trip."""
    store: SQLiteHistoryStore = client._store  # type: ignore[attr-defined]
    seed_table_entity(
        store,
        profile="p1",
        backend="postgresql",
        database="db1",
        schema="public",
        table="customers",
    )
    # Same FQN, different profile — represents a federated graph where
    # the analyst is comparing two physical sources.
    seed_table_entity(
        store,
        profile="p2",
        backend="postgresql",
        database="db1",
        schema="public",
        table="customers_mirror",
    )

    res = client.post(
        "/api/lineage/manual",
        json={
            "profile": "p1",
            "name": "cross-profile-demo",
            "anchor_fqn": "public.customers",
            "nodes": [
                {"profile": "p1", "fqn": "public.customers", "x": 0, "y": 0},
                {"profile": "p2", "fqn": "public.customers_mirror", "x": 320, "y": 0},
            ],
            "edges": [],
            "comments": [],
        },
    )
    assert res.status_code == 201, res.text
    artifact_id = res.json()["artifact_id"]

    body = client.get(f"/api/lineage/by-id/{artifact_id}").json()
    profiles = sorted(n["profile"] for n in body["nodes"])
    assert profiles == ["p1", "p2"], (
        "cross-profile canvas must surface each node with its own profile"
    )


def test_comment_crud_round_trip(client: TestClient):
    """Comments persist alongside the artifact + can be patched + deleted."""
    store: SQLiteHistoryStore = client._store  # type: ignore[attr-defined]
    _seed_two_tables(store)

    save_res = client.post(
        "/api/lineage/manual",
        json={
            "profile": "p1",
            "name": "canvas-with-comments",
            "anchor_fqn": "public.customers",
            "nodes": [{"profile": "p1", "fqn": "public.customers", "x": 0, "y": 0}],
            "edges": [],
            "comments": [
                {
                    "x": 30,
                    "y": 40,
                    "width": 200,
                    "height": 120,
                    "color": "amber",
                    "text": "Pinned for review",
                }
            ],
        },
    )
    assert save_res.status_code == 201, save_res.text
    artifact_id = save_res.json()["artifact_id"]

    list_res = client.get(f"/api/lineage/by-id/{artifact_id}/comments")
    assert list_res.status_code == 200
    comments = list_res.json()["comments"]
    assert len(comments) == 1
    assert comments[0]["text"] == "Pinned for review"

    add_res = client.post(
        f"/api/lineage/by-id/{artifact_id}/comments",
        json={"x": 100, "y": 100, "text": "follow-up", "color": "sky"},
    )
    assert add_res.status_code == 201, add_res.text
    new_id = add_res.json()["id"]

    patch_res = client.patch(
        f"/api/lineage/by-id/{artifact_id}/comments/{new_id}",
        json={"text": "follow-up (revised)"},
    )
    assert patch_res.status_code == 200, patch_res.text

    final = client.get(f"/api/lineage/by-id/{artifact_id}/comments").json()
    texts = sorted(c["text"] for c in final["comments"])
    assert "follow-up (revised)" in texts
    assert "Pinned for review" in texts

    del_res = client.delete(f"/api/lineage/by-id/{artifact_id}/comments/{new_id}")
    assert del_res.status_code == 204

    after_delete = client.get(f"/api/lineage/by-id/{artifact_id}/comments").json()
    assert len(after_delete["comments"]) == 1
