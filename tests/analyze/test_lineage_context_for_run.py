"""resolve_lineage_context_for_run + ProfileAgent lineage prompt block."""

from __future__ import annotations

import time
from pathlib import Path

from amx.analyze.lineage_context import resolve_lineage_context_for_run
from amx.storage.sqlite_store import SQLiteHistoryStore


def _hs(tmp_path: Path) -> SQLiteHistoryStore:
    hs = SQLiteHistoryStore(tmp_path / "history.db")
    hs.init()
    return hs


def _table(hs, *, schema, table, kind="table", search_text="") -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            "INSERT INTO catalog_entities (db_profile, db_backend, database_name, "
            "schema_name, table_name, entity_kind, asset_kind, search_text) "
            "VALUES ('dbr','databricks','wh',?,?,?,?,?)",
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


def test_resolver_returns_upstream_and_downstream_blocks(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    anchor = _table(hs, schema="sales", table="orders")
    parent = _table(hs, schema="sales", table="customers")
    view = _table(hs, schema="sales", table="orders_summary")
    nb = _table(hs, schema="__assets", table="notebook#1", kind="notebook", search_text="ETL nb")
    # orders → customers (FK, anchor is `from` → customers is downstream-ish parent)
    _edge(hs, anchor, parent, "foreign_key")
    # orders_summary view depends on orders (anchor is `to` → upstream)
    _edge(hs, view, anchor, "view_depends_on")
    # native asset producer feeds orders
    _edge(hs, nb, anchor, "lineage_native_asset")

    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={})
    blocks = out[("sales", "orders")]
    dirs = {(b["direction"], b["name"]) for b in blocks}
    assert ("downstream", "sales.customers") in dirs
    assert ("upstream", "sales.orders_summary") in dirs
    # asset neighbour uses its search_text name + asset kind
    assert any(b["kind"] == "notebook" and b["name"] == "ETL nb" for b in blocks)


def test_resolver_honours_scope(tmp_path: Path) -> None:
    hs = _hs(tmp_path)
    a = _table(hs, schema="sales", table="orders")
    b = _table(hs, schema="other", table="thing")
    _edge(hs, b, a, "foreign_key")
    # scope limited to sales.orders only
    out = resolve_lineage_context_for_run(store=hs, profile="dbr", scope={"sales": ["orders"]})
    assert ("sales", "orders") in out
    assert ("other", "thing") not in out


def test_profile_agent_renders_lineage_block() -> None:
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from amx.agents.base import AgentContext
    from amx.agents.profile_agent import ProfileAgent

    pd = SimpleNamespace(
        include_usage_stats=False,
        include_schema_db_comments=False,
        include_pk_fk=False,
        include_unique_check=False,
        include_related_comments=False,
        include_query_log_analysis=False,
        include_null_counts=False,
        include_cardinality=False,
        include_min_max=False,
        include_samples=False,
        max_samples=0,
        include_existing_col_comment=False,
    )
    fake_llm = MagicMock()
    fake_llm.cfg = SimpleNamespace(prompt_detail_cfg=pd, description_verbosity="brief")

    ctx = AgentContext(
        schema="sales",
        table="orders",
        column="",
        db_profile={"row_count": 0, "columns": []},
        lineage_context=[
            {
                "direction": "upstream",
                "kind": "notebook",
                "name": "ETL nb",
                "relationship": "lineage_native_asset",
            },
            {
                "direction": "downstream",
                "kind": "table",
                "name": "sales.report",
                "relationship": "view_depends_on",
            },
        ],
    )
    agent = ProfileAgent(llm=fake_llm)
    prompt = agent._build_prompt(ctx)
    assert "Lineage context" in prompt
    assert "ETL nb" in prompt
    assert "sales.report" in prompt
