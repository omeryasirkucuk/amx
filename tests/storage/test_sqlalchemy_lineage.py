# tests/storage/test_sqlalchemy_lineage.py
import pytest
from sqlalchemy import create_engine
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


@pytest.fixture
def store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    # Use schema="main" — the only schema SQLite supports natively.
    # Production deployments use schema="AMX" on PostgreSQL/Snowflake etc.
    s = SQLAlchemyHistoryStore(engine, schema="main")
    s.init()
    return s


def test_lineage_table_handles_present(store):
    assert store._t_lineage_artifacts is not None
    assert store._t_lineage_artifact_nodes is not None
    assert store._t_lineage_artifact_edges is not None
    assert store._t_lineage_comments is not None


def test_create_lineage_artifact_inserts_and_returns_uuid(store):
    artifact_uuid = store.create_lineage_artifact(
        local_id=42,
        name="orders_lineage",
        db_profile="prod_pg",
        anchor_entity_ref="prod_pg|main|public|orders",
        depth_up=2,
        depth_down=2,
        format="svg",
        output_path="/tmp/orders.svg",
        edge_set_hash="abc123",
        node_count=5,
        edge_count=4,
        canvas_meta={"zoom": 1.0, "pan": {"x": 0, "y": 0}, "layout": "LR"},
        extractors_used=["postgres_fk", "view_parser"],
        extractors_partial=0,
    )
    assert isinstance(artifact_uuid, str) and len(artifact_uuid) == 36

    found = store.find_lineage_uuid_by_local_id(
        hostname=store._hostname, local_id=42
    )
    assert found == artifact_uuid


def test_create_lineage_artifact_stamps_attribution(store):
    artifact_uuid = store.create_lineage_artifact(
        local_id=1,
        name="t",
        db_profile="x",
        anchor_entity_ref="x|a|b|c",
    )
    rows = store.list_lineage_artifacts()
    row = next(r for r in rows if r.id == artifact_uuid)
    assert row.created_by == store._username
    assert row.hostname == store._hostname
    assert row.client_version == store._client_version


# ---------------------------------------------------------------------------
# Task 8 — upsert_lineage_node + list_lineage_nodes
# ---------------------------------------------------------------------------


def test_upsert_lineage_node_creates_then_updates(store):
    artifact_uuid = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x",
        anchor_entity_ref="x|a|b|c",
    )
    node_uuid = store.upsert_lineage_node(
        local_id=100,
        artifact_uuid=artifact_uuid,
        entity_ref="x|a|b|c",
        entity_kind="table",
        db_profile="x",
        x=10.0, y=20.0, width=120.0, height=80.0,
        z_index=0,
        display_label=None,
        column_list_json=[{"name": "id", "type": "int", "nullable": False}],
        logo_key="postgres",
        custom_style_json=None,
    )
    assert isinstance(node_uuid, str)

    # Upsert again with the same local_id moves the node.
    moved_uuid = store.upsert_lineage_node(
        local_id=100,
        artifact_uuid=artifact_uuid,
        entity_ref="x|a|b|c",
        entity_kind="table",
        db_profile="x",
        x=99.0, y=99.0, width=120.0, height=80.0,
    )
    assert moved_uuid == node_uuid

    nodes = store.list_lineage_nodes(artifact_uuid=artifact_uuid)
    assert len(nodes) == 1
    assert nodes[0].x == 99.0


# ---------------------------------------------------------------------------
# Task 9 — upsert_lineage_edge + list_lineage_edges
# ---------------------------------------------------------------------------


def test_upsert_lineage_edge_round_trip(store):
    a = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x", anchor_entity_ref="x|a|b|c"
    )
    n1 = store.upsert_lineage_node(
        local_id=10, artifact_uuid=a, entity_ref="x|a|b|c", entity_kind="table",
        db_profile="x", x=0, y=0, width=100, height=80,
    )
    n2 = store.upsert_lineage_node(
        local_id=11, artifact_uuid=a, entity_ref="x|a|b|d", entity_kind="table",
        db_profile="x", x=200, y=0, width=100, height=80,
    )
    edge = store.upsert_lineage_edge(
        local_id=50, artifact_uuid=a,
        source_node_uuid=n1, target_node_uuid=n2,
        edge_kind="join", join_type="LEFT",
        on_condition="a.id = b.a_id",
        where_clause="a.active = true",
        source_columns_json=["id"],
        target_columns_json=["a_id"],
        label=None, style_json=None, waypoints_json=None,
    )
    edges = store.list_lineage_edges(artifact_uuid=a)
    assert len(edges) == 1
    assert edges[0].id == edge
    assert edges[0].join_type == "LEFT"
    assert edges[0].on_condition == "a.id = b.a_id"


# ---------------------------------------------------------------------------
# Task 10 — upsert_lineage_comment + list_lineage_comments + delete
# ---------------------------------------------------------------------------


def test_upsert_lineage_comment_round_trip(store):
    a = store.create_lineage_artifact(
        local_id=1, name="t", db_profile="x", anchor_entity_ref="x|a|b|c"
    )
    cuuid = store.upsert_lineage_comment(
        local_id=200, artifact_uuid=a,
        x=10.0, y=20.0, width=200.0, height=80.0,
        color="#fef3c7", style="note",
        text="Check this join with @bob",
    )
    comments = store.list_lineage_comments(artifact_uuid=a)
    assert len(comments) == 1
    assert comments[0].id == cuuid
    assert comments[0].text == "Check this join with @bob"
    assert comments[0].created_by == store._username


def test_delete_lineage_comment_removes_row(store):
    a = store.create_lineage_artifact(
        local_id=2, name="t2", db_profile="x", anchor_entity_ref="x|a|b|c"
    )
    cuuid = store.upsert_lineage_comment(
        local_id=201, artifact_uuid=a,
        x=0.0, y=0.0, width=100.0, height=50.0,
        text="to be deleted",
    )
    store.delete_lineage_comment(uuid=cuuid)
    comments = store.list_lineage_comments(artifact_uuid=a)
    assert len(comments) == 0
