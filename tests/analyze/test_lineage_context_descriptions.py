"""RUN lineage blocks carry a truncated neighbour description."""

from __future__ import annotations

import time
from pathlib import Path

from amx.analyze.lineage_context import resolve_lineage_context_for_run
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,'table','table','','full')",
            (schema, table),
        )
        return int(cur.lastrowid)


def _describe(hs, entity_id: int, text: str) -> None:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_descriptions (entity_id, description_text, source_kind, "
            "created_at) VALUES (?,?,?,?)",
            (entity_id, text, "agent", time.time()),
        )
        desc_id = int(cur.lastrowid)
        conn.execute(
            "UPDATE catalog_entities SET effective_description_id = ? WHERE id = ?",
            (desc_id, entity_id),
        )


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_block_includes_neighbour_description(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _describe(hs, parent, "Master list of customers, one row per account.")
    _edge(hs, parent, anchor, "lineage_native_table")

    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={})
    block = next(b for b in out[("sales", "orders")] if b["name"] == "sales.customers")
    assert block["detail"].startswith("Master list of customers")


def test_block_without_description_has_no_detail(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _edge(hs, parent, anchor, "lineage_native_table")

    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={})
    block = next(b for b in out[("sales", "orders")] if b["name"] == "sales.customers")
    assert "detail" not in block
