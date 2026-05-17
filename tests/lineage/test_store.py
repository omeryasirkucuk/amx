"""``view_definitions_cache`` and ``lineage_artifacts`` CRUD."""

from __future__ import annotations

from amx.lineage import store as lineage_store


def test_view_cache_upsert_and_lookup(hs):
    n = lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v1",
                "ddl_text": "SELECT 1",
                "dialect": "duckdb",
                "parsed_lineage": [{"target": "c1", "sources": []}],
                "parse_status": "ok",
                "parse_error": "",
            }
        ],
    )
    assert n == 1
    rows = lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
    assert len(rows) == 1
    assert rows[0]["parsed_lineage"][0]["target"] == "c1"


def test_view_cache_upsert_overwrites_existing(hs):
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v1",
                "ddl_text": "SELECT 1",
                "dialect": "duckdb",
                "parsed_lineage": [{"target": "x", "sources": []}],
                "parse_status": "ok",
                "parse_error": "",
            }
        ],
    )
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v1",
                "ddl_text": "SELECT 2",
                "dialect": "duckdb",
                "parsed_lineage": None,
                "parse_status": "parse_failed",
                "parse_error": "oops",
            }
        ],
    )
    rows = lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
    assert rows[0]["parse_status"] == "parse_failed"
    assert rows[0]["parsed_lineage"] is None


def test_view_cache_expired_rows_treated_as_miss(hs):
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v1",
                "ddl_text": "SELECT 1",
                "dialect": "duckdb",
                "parsed_lineage": [],
                "parse_status": "ok",
                "parse_error": "",
            }
        ],
        ttl_seconds=-1,  # already-expired
    )
    rows = lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
    assert rows == []


def test_invalidate_view_definitions_by_scope(hs):
    lineage_store.upsert_view_definitions(
        hs,
        db_profile="p",
        database="",
        schema="public",
        entries=[
            {
                "view_name": "v1",
                "ddl_text": "SELECT 1",
                "dialect": "duckdb",
                "parsed_lineage": [],
                "parse_status": "ok",
                "parse_error": "",
            }
        ],
    )
    deleted = lineage_store.invalidate_view_definitions(
        hs, db_profile="p", database="", schema="public"
    )
    assert deleted == 1
    assert (
        lineage_store.lookup_view_definitions(hs, db_profile="p", database="", schema="public")
        == []
    )


def test_edge_set_hash_stable_under_reorder(hs):
    a = lineage_store.compute_edge_set_hash([(1, 2, "x", 1.0), (3, 4, "y", 0.5)])
    b = lineage_store.compute_edge_set_hash([(3, 4, "y", 0.5), (1, 2, "x", 1.0)])
    assert a == b


def test_edge_set_hash_changes_when_edges_differ(hs):
    a = lineage_store.compute_edge_set_hash([(1, 2, "x", 1.0)])
    b = lineage_store.compute_edge_set_hash([(1, 2, "x", 0.99)])
    assert a != b


def test_lineage_artifact_crud(hs):
    aid = lineage_store.insert_lineage_artifact(
        hs,
        name="demo",
        db_profile="p",
        anchor_entity_id=1,
        depth_up=1,
        depth_down=1,
        fmt="svg",
        output_path="/tmp/demo.svg",
        edge_set_hash="abc",
        node_count=2,
        edge_count=1,
        extractors_used=["fk"],
        extractors_partial=False,
    )
    assert aid > 0
    art = lineage_store.lookup_lineage_artifact(hs, name_or_id="demo")
    assert art is not None
    assert art["edge_set_hash"] == "abc"
    by_id = lineage_store.lookup_lineage_artifact(hs, name_or_id=str(aid))
    assert by_id and by_id["id"] == aid

    lineage_store.update_lineage_artifact(
        hs,
        artifact_id=aid,
        edge_set_hash="new",
        node_count=4,
        edge_count=3,
        extractors_used=["fk", "view_ddl"],
        extractors_partial=True,
    )
    art2 = lineage_store.lookup_lineage_artifact(hs, name_or_id="demo")
    assert art2["edge_set_hash"] == "new"
    assert art2["extractors_partial"] is True
    assert "view_ddl" in art2["extractors_used"]

    lineage_store.delete_lineage_artifact(hs, artifact_id=aid)
    assert lineage_store.lookup_lineage_artifact(hs, name_or_id="demo") is None
