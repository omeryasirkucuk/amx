"""Lineage scope resolution mirrors doc/code resolution."""

from __future__ import annotations

from pathlib import Path

from amx.config import AMXConfig, DBConfig
from amx.search._agent.scope import resolve_lineage_for_scope
from amx.storage.sqlite_store import SQLiteHistoryStore


def test_resolve_lineage_for_scope_lists_artifacts_for_db_profiles(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    with store._connect() as conn:
        for name, profile in (("canvas-a", "p1"), ("canvas-b", "p1"), ("canvas-c", "p2")):
            conn.execute(
                "INSERT INTO lineage_artifacts(name, db_profile, anchor_entity_id, depth_up, "
                "depth_down, format, output_path, edge_set_hash, node_count, edge_count, "
                "generated_at, extractors_used, extractors_partial) "
                "VALUES (?, ?, 1, 1, 1, 'svg', '', 'h', 0, 0, 0, '[\"fk\"]', 0)",
                (name, profile),
            )
        conn.commit()
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    cfg.db_profiles["p1"] = DBConfig(backend="postgresql", host="p1")
    cfg.db_profiles["p2"] = DBConfig(backend="postgresql", host="p2")
    names = resolve_lineage_for_scope(cfg=cfg, store=store, scope_db_profiles=["p1"])
    assert names == ["canvas-a", "canvas-b"]


def test_resolve_lineage_for_scope_empty_when_scope_empty(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    cfg = AMXConfig.load(str(tmp_path / "config.yml"))
    names = resolve_lineage_for_scope(cfg=cfg, store=store, scope_db_profiles=[])
    assert names == []
