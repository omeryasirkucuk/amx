"""Tests for the shared one-hop native-lineage neighbour query."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.lineage.neighbors import Neighbor, enrichment_disabled, lineage_neighbors
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _entity(hs, *, schema, table, kind="table", search_text="", state="full") -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text, metadata_state) "
            "VALUES ('dbr','databricks','wh',?,?,?,?,?,?)",
            (schema, table, kind, kind, search_text, state),
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


def test_resolves_upstream_and_downstream_names(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    nb = _entity(hs, schema="__assets", table="nb#1", kind="notebook", search_text="ETL nb")
    _edge(hs, parent, anchor, "lineage_native_table")  # parent feeds anchor -> upstream
    _edge(hs, anchor, nb, "lineage_native_asset")       # anchor feeds nb -> downstream

    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])

    nbs = out[anchor]
    assert ("upstream", "sales.customers", "table") in {
        (n.direction, n.name, n.kind) for n in nbs
    }
    assert ("downstream", "ETL nb", "notebook") in {
        (n.direction, n.name, n.kind) for n in nbs
    }
    assert all(isinstance(n, Neighbor) for n in nbs)


def test_dedup_and_fanout_cap(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    # Six distinct upstream parents; cap at fanout=3.
    for i in range(6):
        p = _entity(hs, schema="s", table=f"p{i}")
        _edge(hs, p, anchor, "foreign_key")
    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor], fanout=3)
    assert len(out[anchor]) == 3


def test_empty_inputs_and_kill_switch(tmp_path: Path, monkeypatch) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    parent = _entity(hs, schema="s", table="b")
    _edge(hs, parent, anchor, "foreign_key")
    with hs._connect() as conn:
        assert lineage_neighbors(conn, anchor_entity_ids=[]) == {}
        monkeypatch.setenv("AMX_LINEAGE_CONTEXT_DISABLED", "1")
        assert enrichment_disabled() is True
        assert lineage_neighbors(conn, anchor_entity_ids=[anchor]) == {}


def test_dedup_same_entity_two_rel_types(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    parent = _entity(hs, schema="s", table="b")
    _edge(hs, parent, anchor, "foreign_key")
    _edge(hs, parent, anchor, "view_depends_on")
    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])
    # Same entity reached via two edge types collapses to one Neighbor.
    assert len(out[anchor]) == 1


def test_self_loop_edge_does_not_crash(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="s", table="a")
    _edge(hs, anchor, anchor, "lineage_native_table")
    with hs._connect() as conn:
        out = lineage_neighbors(conn, anchor_entity_ids=[anchor])
    # A self-referencing edge is recorded from both viewpoints; assert it
    # does not raise and stays bounded.
    assert len(out[anchor]) <= 2


@pytest.mark.parametrize("val", ["1", "true", "yes", "on", "TRUE", " 1 "])
def test_enrichment_disabled_truthy_variants(monkeypatch, val: str) -> None:
    monkeypatch.setenv("AMX_LINEAGE_CONTEXT_DISABLED", val)
    assert enrichment_disabled() is True


def test_enrichment_disabled_falsy(monkeypatch) -> None:
    monkeypatch.delenv("AMX_LINEAGE_CONTEXT_DISABLED", raising=False)
    assert enrichment_disabled() is False
