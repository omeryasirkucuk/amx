"""Regression test for the lineage edge query.

``_fetch_edges`` joins ``catalog_relationships`` to ``catalog_entities``
to label the other side of each edge. A stale ``COALESCE(ce.table_name,
ce.name, '')`` referenced a column (``ce.name``) that does not exist on
``catalog_entities`` — every ``lineage_for_table`` / ``lineage_for_column``
call crashed with ``sqlite3.OperationalError: no such column: ce.name``.
This test seeds a minimal lineage edge and asserts the query runs and
returns the other side's table name.
"""

from __future__ import annotations

from pathlib import Path

from amx.search._tool_lineage import _LINEAGE_RELATIONSHIP_TYPES, _fetch_edges
from amx.storage.sqlite_store import SQLiteHistoryStore


def _entity_id(conn, *, profile: str, schema: str, table: str) -> int:
    conn.execute(
        "INSERT INTO catalog_entities(db_profile, schema_name, table_name, entity_kind) "
        "VALUES (?, ?, ?, 'table')",
        (profile, schema, table),
    )
    row = conn.execute(
        "SELECT id FROM catalog_entities WHERE db_profile=? AND schema_name=? AND table_name=?",
        (profile, schema, table),
    ).fetchone()
    return int(row[0])


def test_fetch_edges_resolves_other_name_without_missing_column(tmp_path: Path) -> None:
    store = SQLiteHistoryStore(tmp_path / "history.db")
    store.init()
    with store._connect() as conn:
        src = _entity_id(conn, profile="p1", schema="public", table="src_orders")
        dst = _entity_id(conn, profile="p1", schema="public", table="fact_sales")
        conn.execute(
            "INSERT INTO catalog_relationships(from_entity_id, to_entity_id, relationship_type) "
            "VALUES (?, ?, 'foreign_key')",
            (src, dst),
        )
        conn.commit()

        # Upstream of the destination table must surface the source table.
        # Before the fix this raised OperationalError: no such column: ce.name.
        edges = _fetch_edges(
            conn,
            entity_id=dst,
            direction="upstream",
            rel_types=_LINEAGE_RELATIONSHIP_TYPES,
            column_grain=False,
            limit=30,
        )

    assert len(edges) == 1
    assert edges[0]["other_name"] == "src_orders"
    assert edges[0]["relationship_type"] == "foreign_key"
    assert edges[0]["direction"] == "upstream"
