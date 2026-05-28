"""Canvas-free, name-resolved native-lineage neighbours for ASK."""

from __future__ import annotations

import time
from pathlib import Path

from amx.lineage.evidence import NativeNeighbors, build_native_lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table, kind="table", search_text="") -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,?,?,?,'full')",
            (schema, table, kind, kind, search_text),
        )
        return int(cur.lastrowid)


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_named_neighbours_without_any_canvas(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    nb = _entity(hs, schema="__assets", table="nb#1", kind="notebook", search_text="ETL nb")
    _edge(hs, parent, anchor, "lineage_native_table")
    _edge(hs, anchor, nb, "lineage_native_asset")

    out = build_native_lineage_neighbors(store=hs, entity_ids=[anchor])
    assert isinstance(out, NativeNeighbors)
    assert out.has_neighbors
    assert {r["name"] for r in out.upstream} == {"sales.customers"}
    assert {r["name"] for r in out.downstream} == {"ETL nb"}


def test_off_switch_empty_filter(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    parent = _entity(hs, schema="s", table="b")
    _edge(hs, parent, anchor, "foreign_key")
    out = build_native_lineage_neighbors(store=hs, entity_ids=[anchor], artifact_filter=[])
    assert out.has_neighbors is False
