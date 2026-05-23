"""DatabricksSystemTablesExtractor — mocked query_runner round trips."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import pytest

from amx.lineage.extractors.system_tables.databricks import (
    REL_COLUMN,
    REL_TABLE,
    DatabricksSystemTablesExtractor,
)

from .conftest import seed_table_entity


def _runner_for(canned: dict[str, list[dict[str, Any]]]):
    """Return a query_runner that picks rows by substring match in the SQL.

    Keeps the tests insensitive to whitespace and column-order
    re-arrangements in the extractor while still letting each test
    inject its own response per query family.
    """

    def runner(sql: str) -> list[dict[str, Any]]:
        if "system.access.table_lineage" in sql:
            return canned.get("table_lineage", [])
        if "system.access.column_lineage" in sql:
            return canned.get("column_lineage", [])
        if "system.query.history" in sql:
            return canned.get("query_history", [])
        return []

    return runner


def test_extractor_returns_zero_when_profile_has_no_catalog_entities(hs) -> None:
    """Without any catalog tables in the profile the extractor exits
    immediately — the dispatcher catches that as "nothing to backfill"."""
    extractor = DatabricksSystemTablesExtractor(
        hs._connect(),
        query_runner=_runner_for({"table_lineage": [{"src": "a.b.c"}]}),
    )
    counts = extractor.extract_for_profile("dbr-prod")
    assert counts == {
        "table_lineage": 0,
        "column_lineage": 0,
        "usage_backfilled": 0,
    }


def test_table_lineage_writes_one_relationship_per_resolved_pair(hs) -> None:
    orders_id = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="orders",
    )
    line_items_id = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="line_items",
    )
    extractor = DatabricksSystemTablesExtractor(
        hs._connect(),
        query_runner=_runner_for(
            {
                "table_lineage": [
                    {
                        "src": "prod.sales.orders",
                        "tgt": "prod.sales.line_items",
                        "event_time": datetime(2026, 5, 22, 10, 0, tzinfo=timezone.utc),
                        "created_by": "alice@example.com",
                    },
                    # Unresolved target → silently dropped.
                    {
                        "src": "prod.sales.orders",
                        "tgt": "prod.unknown.ghost",
                        "event_time": "2026-05-22T11:00:00Z",
                        "created_by": "alice@example.com",
                    },
                    # Self-loop → silently dropped.
                    {
                        "src": "prod.sales.orders",
                        "tgt": "prod.sales.orders",
                        "event_time": "2026-05-22T12:00:00Z",
                        "created_by": "alice@example.com",
                    },
                ]
            }
        ),
    )
    counts = extractor.extract_for_profile("dbr")
    assert counts["table_lineage"] == 1

    with hs._connect() as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                """
                SELECT from_entity_id, to_entity_id, relationship_type, source,
                       audit_actor
                FROM catalog_relationships
                WHERE relationship_type = ?
                """,
                (REL_TABLE,),
            ).fetchall()
        ]
    assert rows == [
        (
            orders_id,
            line_items_id,
            REL_TABLE,
            "databricks_system_tables",
            "alice@example.com",
        )
    ]


def test_table_lineage_is_idempotent_across_runs(hs) -> None:
    seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="orders",
    )
    seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="line_items",
    )
    runner = _runner_for(
        {
            "table_lineage": [
                {
                    "src": "prod.sales.orders",
                    "tgt": "prod.sales.line_items",
                    "event_time": "2026-05-22T10:00:00Z",
                    "created_by": "alice@example.com",
                }
            ]
        }
    )
    extractor = DatabricksSystemTablesExtractor(hs._connect(), query_runner=runner)
    extractor.extract_for_profile("dbr")
    extractor.extract_for_profile("dbr")

    with hs._connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM catalog_relationships WHERE relationship_type = ?",
            (REL_TABLE,),
        ).fetchone()[0]
    assert count == 1


def test_column_lineage_writes_with_from_and_to_columns(hs) -> None:
    orders_id = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="orders",
    )
    line_items_id = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="line_items",
    )
    extractor = DatabricksSystemTablesExtractor(
        hs._connect(),
        query_runner=_runner_for(
            {
                "column_lineage": [
                    {
                        "src_tbl": "prod.sales.orders",
                        "src_col": "id",
                        "tgt_tbl": "prod.sales.line_items",
                        "tgt_col": "order_id",
                        "event_time": "2026-05-22T10:00:00Z",
                    },
                ]
            }
        ),
    )
    counts = extractor.extract_for_profile("dbr")
    assert counts["column_lineage"] == 1
    with hs._connect() as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                """
                SELECT from_entity_id, from_column, to_entity_id, to_column,
                       relationship_type
                FROM catalog_relationships
                WHERE relationship_type = ?
                """,
                (REL_COLUMN,),
            ).fetchall()
        ]
    assert rows == [(orders_id, "id", line_items_id, "order_id", REL_COLUMN)]


def test_query_history_backfills_last_used_on_asset_lineage_edges(hs) -> None:
    """A statement id from system.query.history that matches a saved
    query's external_id should refresh every asset_lineage_edges row
    where ``from_kind='query' AND from_id = <that row>.id``."""
    sales_id = seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="sales",
    )
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_queries
                (profile_name, platform, kind, external_id, name, sql_text,
                 sql_hash, ingested_at)
            VALUES (?, 'databricks', 'saved', ?, 'q', 'SELECT 1', 'h', ?)
            """,
            ("dbr", "stmt-123", time.time()),
        )
        qid = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO asset_lineage_edges
                (profile_name, from_kind, from_id, to_kind, to_id, edge_type,
                 raw_ref, discovered_at, direction)
            VALUES (?, 'query', ?, 'table', ?, 'query_writes_table', '{}', ?, 'write')
            """,
            ("dbr", qid, sales_id, time.time()),
        )

    extractor = DatabricksSystemTablesExtractor(
        hs._connect(),
        query_runner=_runner_for(
            {
                "query_history": [
                    {
                        "statement_id": "stmt-123",
                        "executed_by": "bob@example.com",
                        "start_time": datetime(2026, 5, 22, 14, 30, tzinfo=timezone.utc),
                    },
                    {
                        "statement_id": "stmt-123",
                        "executed_by": "alice@example.com",
                        # Older — should NOT win the latest-wins reduce.
                        "start_time": datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
                    },
                ]
            }
        ),
    )
    counts = extractor.extract_for_profile("dbr")
    assert counts["usage_backfilled"] == 1

    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT last_user, last_used_at FROM asset_lineage_edges
            WHERE from_kind = 'query' AND from_id = ?
            """,
            (qid,),
        ).fetchone()
    assert row[0] == "bob@example.com"
    # The more-recent observation's epoch should be in the column.
    assert row[1] == pytest.approx(datetime(2026, 5, 22, 14, 30, tzinfo=timezone.utc).timestamp())


def test_query_runner_failure_does_not_break_the_pass(hs) -> None:
    """If the workspace lacks privilege on system.access.* the runner
    raises; the extractor logs and continues."""
    seed_table_entity(
        hs,
        profile="dbr",
        backend="databricks",
        database="prod",
        schema="sales",
        table="orders",
    )

    def boom(sql: str) -> list[dict[str, Any]]:
        raise PermissionError("requires CAN_ACCESS_METASTORE")

    counts = DatabricksSystemTablesExtractor(hs._connect(), query_runner=boom).extract_for_profile(
        "dbr"
    )
    assert counts == {
        "table_lineage": 0,
        "column_lineage": 0,
        "usage_backfilled": 0,
    }
