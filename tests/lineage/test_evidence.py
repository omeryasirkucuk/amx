"""Tests for the anchor-based lineage evidence builder.

The builder is invoked by the ASK retrieval layer to surface one-hop
upstream / downstream entities plus canvas comments and logo keys
for a set of anchor entity ids. The happy-path test below seeds a
minimal three-node graph (``20 -> 10 -> 30``) plus a lineage artifact
that includes all three nodes, then verifies that anchoring on entity
``10`` yields exactly the upstream / downstream split implied by the
directed relationships.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from amx.lineage.evidence import LineageEvidence, build_lineage_evidence
from amx.storage.sqlite_store import SQLiteHistoryStore


def _insert_entity(hs: SQLiteHistoryStore, *, entity_id: int, table: str) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities
                (id, db_profile, db_backend, database_name, schema_name,
                 table_name, entity_kind, asset_kind)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (entity_id, "p1", "postgresql", "db", "public", table, "table", "table"),
        )


def _insert_relationship(
    hs: SQLiteHistoryStore, *, from_id: int, to_id: int
) -> None:
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 details_json, last_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (from_id, to_id, "foreign_key", 10.0, "database", "{}", time.time()),
        )


def test_build_lineage_evidence_happy_path(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    _insert_entity(store, entity_id=10, table="anchor")
    _insert_entity(store, entity_id=20, table="upstream")
    _insert_entity(store, entity_id=30, table="downstream")

    # Directed edges: 20 -> 10 (upstream of anchor) and 10 -> 30 (downstream).
    _insert_relationship(store, from_id=20, to_id=10)
    _insert_relationship(store, from_id=10, to_id=30)

    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_artifacts
                (name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "canvas-a",
                "p1",
                10,
                1,
                1,
                "svg",
                "",
                "hash-a",
                3,
                2,
                time.time(),
                json.dumps([]),
                0,
            ),
        )
        artifact_id = int(cur.lastrowid)
        for eid in (10, 20, 30):
            conn.execute(
                """
                INSERT INTO lineage_artifact_nodes
                    (artifact_id, entity_id, db_profile)
                VALUES (?, ?, ?)
                """,
                (artifact_id, eid, "p1"),
            )

    out = build_lineage_evidence(
        store=store,
        entity_ids=[10],
        artifact_filter=None,
        max_upstream=5,
        max_downstream=5,
        max_comments=3,
    )

    assert isinstance(out, LineageEvidence)
    assert out.upstream_entity_ids == [20]
    assert out.downstream_entity_ids == [30]
    assert out.artifact_names == ["canvas-a"]


def test_lineage_evidence_empty_when_no_artifact(tmp_path: Path) -> None:
    """Empty DB returns an empty ``LineageEvidence``."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    out = build_lineage_evidence(
        store=store,
        entity_ids=[999],
        artifact_filter=None,
    )

    assert out.is_empty


def test_lineage_evidence_respects_artifact_filter_off(tmp_path: Path) -> None:
    """An ``artifact_filter=[]`` short-circuits to an empty payload."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    out = build_lineage_evidence(
        store=store,
        entity_ids=[10],
        artifact_filter=[],
    )

    assert out.is_empty


def test_lineage_evidence_caps_upstream_at_five(tmp_path: Path) -> None:
    """Twenty upstream edges into one anchor truncate to ``max_upstream``."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    _insert_entity(store, entity_id=100, table="anchor")
    for i in range(1, 21):
        _insert_entity(store, entity_id=i, table=f"up_{i}")
        _insert_relationship(store, from_id=i, to_id=100)

    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_artifacts
                (name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "big",
                "p1",
                100,
                1,
                1,
                "svg",
                "",
                "hash-big",
                21,
                20,
                time.time(),
                json.dumps(["foreign_key"]),
                0,
            ),
        )
        artifact_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO lineage_artifact_nodes
                (artifact_id, entity_id, db_profile)
            VALUES (?, ?, ?)
            """,
            (artifact_id, 100, "p1"),
        )
        for i in range(1, 21):
            conn.execute(
                """
                INSERT INTO lineage_artifact_nodes
                    (artifact_id, entity_id, db_profile)
                VALUES (?, ?, ?)
                """,
                (artifact_id, i, "p1"),
            )

    out = build_lineage_evidence(
        store=store,
        entity_ids=[100],
        artifact_filter=None,
        max_upstream=5,
    )

    assert len(out.upstream_entity_ids) == 5


def test_lineage_evidence_falls_back_to_co_resident_when_no_edges(
    tmp_path: Path,
) -> None:
    """When no ``catalog_relationships`` rows exist, the builder splits
    the artifact's co-resident node ids across upstream / downstream
    (lossy fallback) and still surfaces the artifact name + comments."""
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()

    _insert_entity(store, entity_id=10, table="anchor")
    _insert_entity(store, entity_id=11, table="other")

    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO lineage_artifacts
                (name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ghost",
                "p1",
                10,
                1,
                1,
                "svg",
                "",
                "hash-ghost",
                2,
                0,
                time.time(),
                json.dumps(["foreign_key"]),
                0,
            ),
        )
        artifact_id = int(cur.lastrowid)
        for ent in (10, 11):
            conn.execute(
                """
                INSERT INTO lineage_artifact_nodes
                    (artifact_id, entity_id, db_profile)
                VALUES (?, ?, ?)
                """,
                (artifact_id, ent, "p1"),
            )
        now = time.time()
        conn.execute(
            """
            INSERT INTO lineage_comments
                (artifact_id, x, y, width, height, color, text,
                 created_at, updated_at)
            VALUES (?, 0, 0, 0, 0, 'amber', ?, ?, ?)
            """,
            (artifact_id, "see PBI report", now, now),
        )

    out = build_lineage_evidence(
        store=store,
        entity_ids=[10],
        artifact_filter=None,
    )

    assert "ghost" in out.artifact_names
    assert 11 in (out.upstream_entity_ids + out.downstream_entity_ids)
    assert out.comments == ["see PBI report"]
