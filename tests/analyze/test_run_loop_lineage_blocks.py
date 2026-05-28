"""The CLI run loop resolves lineage blocks (Studio-parity)."""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from amx.cli_support.commands._analyze.run_loop import resolve_run_lineage_blocks
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


def _edge(hs, frm, to, rel) -> None:
    with hs._connect() as conn:
        conn.execute(
            "INSERT INTO catalog_relationships (from_entity_id, to_entity_id, "
            "relationship_type, score, source, details_json, last_seen, "
            "from_entity_kind, to_entity_kind) VALUES (?,?,?,1.0,'native','{}',?,?,?)",
            (frm, to, rel, time.time(), "table", "table"),
        )


def test_resolves_blocks_for_scope(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _entity(hs, schema="sales", table="orders")
    parent = _entity(hs, schema="sales", table="customers")
    _edge(hs, parent, anchor, "lineage_native_table")

    cfg = SimpleNamespace(active_db_profile="dbr")
    blocks = resolve_run_lineage_blocks(
        cfg=cfg, history_store_fn=lambda: hs, scope={"sales": ["orders"]}
    )
    assert ("sales", "orders") in blocks
    assert any(b["name"] == "sales.customers" for b in blocks[("sales", "orders")])


def test_returns_empty_without_profile_or_store(tmp_path: Path) -> None:
    cfg_noprofile = SimpleNamespace(active_db_profile="")
    assert (
        resolve_run_lineage_blocks(cfg=cfg_noprofile, history_store_fn=lambda: None, scope={}) == {}
    )
    cfg = SimpleNamespace(active_db_profile="dbr")
    assert resolve_run_lineage_blocks(cfg=cfg, history_store_fn=lambda: None, scope={}) == {}


def test_returns_empty_when_history_store_fn_is_none() -> None:
    cfg = SimpleNamespace(active_db_profile="dbr")
    assert (
        resolve_run_lineage_blocks(cfg=cfg, history_store_fn=None, scope={"sales": ["orders"]})
        == {}
    )


def test_returns_empty_on_store_error() -> None:
    # The resolver raising must never propagate into a run — the helper
    # swallows it and returns an empty mapping.
    bad_store = MagicMock()
    bad_store._connect.side_effect = RuntimeError("corrupt db")
    cfg = SimpleNamespace(active_db_profile="dbr")
    assert (
        resolve_run_lineage_blocks(cfg=cfg, history_store_fn=lambda: bad_store, scope={"s": ["t"]})
        == {}
    )
