"""SQL-parse extractor: stored SQL on queries and notebooks -> edges.

Two layers of tests:

* Pure-function tests on :func:`extract_reads_writes` and
  :func:`extract_sql_blocks_from_notebook` covering each
  statement shape (SELECT, INSERT, MERGE, CTAS, ...) and dialect
  surprise (Snowflake COPY INTO, BigQuery MERGE, Postgres INSERT
  RETURNING, CTEs).
* Integration tests for :class:`SQLParseExtractor.extract_for_profile`
  that seed the canonical ``hs`` fixture from ``conftest.py`` with
  ``remote_queries`` / ``remote_notebooks`` / ``catalog_entities``
  rows and assert the right ``asset_lineage_edges`` land.
"""

from __future__ import annotations

import json
import time

import pytest

from amx.lineage.extractors.sql_parse import (
    EDGE_NOTEBOOK_READS_TABLE,
    EDGE_NOTEBOOK_WRITES_TABLE,
    EDGE_QUERY_READS_TABLE,
    EDGE_QUERY_WRITES_TABLE,
    SQLParseExtractor,
    extract_reads_writes,
    extract_sql_blocks_from_notebook,
)

from .conftest import seed_table_entity

# ── extract_reads_writes ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "sql, dialect, expected_reads, expected_writes",
    [
        (
            "SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
            "postgres",
            {"orders", "customers"},
            set(),
        ),
        (
            "INSERT INTO sales SELECT * FROM orders WHERE total > 0",
            "postgres",
            {"orders"},
            {"sales"},
        ),
        (
            "CREATE TABLE sales AS SELECT * FROM orders JOIN customers ON orders.cid = customers.id",
            "postgres",
            {"orders", "customers"},
            {"sales"},
        ),
        (
            "MERGE INTO target T USING source S ON T.id = S.id "
            "WHEN MATCHED THEN UPDATE SET x = S.x",
            "snowflake",
            {"source"},
            {"target"},
        ),
        (
            "UPDATE orders SET status = (SELECT max(status) FROM events)",
            "postgres",
            {"events"},
            {"orders"},
        ),
        (
            "DELETE FROM stale WHERE id IN (SELECT id FROM blacklist)",
            "postgres",
            {"blacklist"},
            {"stale"},
        ),
        (
            "WITH cte AS (SELECT * FROM events) SELECT * FROM cte JOIN orders ON 1=1",
            "postgres",
            {"events", "orders"},
            set(),
        ),
        (
            "WITH a AS (SELECT * FROM x), b AS (SELECT * FROM a JOIN y ON 1=1) SELECT * FROM b",
            "postgres",
            {"x", "y"},
            set(),
        ),
        (
            "MERGE INTO project.dataset.target T USING project.dataset.source S "
            "ON T.id = S.id WHEN MATCHED THEN UPDATE SET x = S.x",
            "bigquery",
            {"project.dataset.source"},
            {"project.dataset.target"},
        ),
        (
            "INSERT INTO log (event) SELECT name FROM users RETURNING id",
            "postgres",
            {"users"},
            {"log"},
        ),
    ],
)
def test_extract_reads_writes_parametrized(
    sql: str,
    dialect: str,
    expected_reads: set[str],
    expected_writes: set[str],
) -> None:
    reads, writes = extract_reads_writes(sql, dialect)
    assert reads == expected_reads
    assert writes == expected_writes


def test_extract_reads_writes_returns_empty_on_garbage() -> None:
    """Unparseable input gives back two empty sets; never raises."""
    reads, writes = extract_reads_writes("this is definitely not SQL !!!", "postgres")
    assert reads == set()
    assert writes == set()


def test_extract_reads_writes_drops_read_that_duplicates_write() -> None:
    """A self-referential INSERT keeps the table only on the write side."""
    sql = "INSERT INTO sales SELECT * FROM sales WHERE 1=0"
    reads, writes = extract_reads_writes(sql, "postgres")
    assert writes == {"sales"}
    assert "sales" not in reads


def test_extract_reads_writes_snowflake_copy_into_marks_target_as_write() -> None:
    """``COPY INTO mytable FROM '...'`` writes mytable; the source path is
    not a catalog table and will fail catalog resolution downstream, which
    is the correct behaviour."""
    sql = "COPY INTO mytable FROM 's3://bucket/prefix/' FILE_FORMAT = (TYPE = CSV)"
    _reads, writes = extract_reads_writes(sql, "snowflake")
    assert "mytable" in writes


# ── extract_sql_blocks_from_notebook ────────────────────────────────


def test_notebook_blocks_sql_language_returns_every_code_cell() -> None:
    nb = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# header"},
                {"cell_type": "code", "source": "SELECT 1"},
                {"cell_type": "code", "source": ["SELECT", " 2"]},
            ]
        }
    )
    blocks = extract_sql_blocks_from_notebook(nb, "sql")
    assert blocks == ["SELECT 1", "SELECT 2"]


def test_notebook_blocks_python_with_sql_magic_extracts_only_sql_body() -> None:
    nb = json.dumps(
        {
            "cells": [
                {"cell_type": "code", "source": "x = 1"},
                {"cell_type": "code", "source": "%sql\nSELECT * FROM orders"},
                {"cell_type": "code", "source": "%%sql\nINSERT INTO sales SELECT 1"},
            ]
        }
    )
    blocks = extract_sql_blocks_from_notebook(nb, "python")
    assert blocks == [
        "SELECT * FROM orders",
        "INSERT INTO sales SELECT 1",
    ]


def test_notebook_blocks_malformed_json_returns_empty() -> None:
    assert extract_sql_blocks_from_notebook("{not json", "sql") == []
    assert extract_sql_blocks_from_notebook("", "sql") == []


# ── SQLParseExtractor.extract_for_profile ──────────────────────────


def _seed_query(
    hs,
    *,
    profile: str = "p",
    platform: str = "postgresql",
    sql: str,
    external_id: str = "q1",
) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_queries
                (profile_name, platform, kind, external_id, name,
                 sql_text, sql_hash, ingested_at)
            VALUES (?, ?, 'saved', ?, ?, ?, ?, ?)
            """,
            (profile, platform, external_id, "q", sql, "h-" + external_id, time.time()),
        )
    return int(cur.lastrowid)


def _seed_notebook(
    hs,
    *,
    profile: str = "p",
    platform: str = "databricks",
    language: str = "sql",
    source_text: str,
    external_id: str = "n1",
) -> int:
    with hs._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_notebooks
                (profile_name, platform, external_id, name, workspace_path,
                 language, source_text, source_hash, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile,
                platform,
                external_id,
                "nb-" + external_id,
                "/Workspace/nb",
                language,
                source_text,
                "h-" + external_id,
                time.time(),
            ),
        )
    return int(cur.lastrowid)


def test_extractor_returns_zero_when_profile_has_no_catalog_tables(hs) -> None:
    """No catalog tables means nothing can resolve, so the extractor exits
    early without touching the edge table."""
    _seed_query(hs, sql="INSERT INTO sales SELECT * FROM orders")
    written = SQLParseExtractor(hs._connect()).extract_for_profile("p")
    assert written == 0
    with hs._connect() as conn:
        rows = conn.execute("SELECT COUNT(*) FROM asset_lineage_edges").fetchone()[0]
    assert rows == 0


def test_extractor_writes_query_read_and_write_edges_with_direction(hs) -> None:
    sales_id = seed_table_entity(hs, schema="public", table="sales")
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    qid = _seed_query(hs, sql="INSERT INTO sales SELECT * FROM orders")

    written = SQLParseExtractor(hs._connect()).extract_for_profile("p")
    assert written == 2

    with hs._connect() as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                """
                SELECT from_kind, from_id, to_kind, to_id, edge_type, direction
                FROM asset_lineage_edges
                WHERE profile_name = ?
                ORDER BY edge_type
                """,
                ("p",),
            ).fetchall()
        ]
    assert rows == [
        ("query", qid, "table", orders_id, EDGE_QUERY_READS_TABLE, "read"),
        ("query", qid, "table", sales_id, EDGE_QUERY_WRITES_TABLE, "write"),
    ]


def test_extractor_writes_notebook_edges_from_sql_cells(hs) -> None:
    sales_id = seed_table_entity(hs, schema="public", table="sales")
    orders_id = seed_table_entity(hs, schema="public", table="orders")
    nb_payload = json.dumps(
        {
            "cells": [
                {"cell_type": "markdown", "source": "# load"},
                {"cell_type": "code", "source": "SELECT * FROM orders"},
                {"cell_type": "code", "source": "INSERT INTO sales SELECT 1"},
            ]
        }
    )
    nb_id = _seed_notebook(hs, source_text=nb_payload)

    SQLParseExtractor(hs._connect()).extract_for_profile("p")

    with hs._connect() as conn:
        rows = [
            tuple(r)
            for r in conn.execute(
                """
                SELECT from_kind, from_id, to_kind, to_id, edge_type, direction
                FROM asset_lineage_edges
                WHERE profile_name = ?
                ORDER BY edge_type, to_id
                """,
                ("p",),
            ).fetchall()
        ]
    assert rows == [
        ("notebook", nb_id, "table", orders_id, EDGE_NOTEBOOK_READS_TABLE, "read"),
        ("notebook", nb_id, "table", sales_id, EDGE_NOTEBOOK_WRITES_TABLE, "write"),
    ]


def test_extractor_is_idempotent_across_runs(hs) -> None:
    """Two consecutive runs converge on the same edge set."""
    seed_table_entity(hs, schema="public", table="sales")
    seed_table_entity(hs, schema="public", table="orders")
    _seed_query(hs, sql="INSERT INTO sales SELECT * FROM orders")
    extractor = SQLParseExtractor(hs._connect())

    extractor.extract_for_profile("p")
    extractor.extract_for_profile("p")

    with hs._connect() as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM asset_lineage_edges WHERE profile_name = ?",
            ("p",),
        ).fetchone()[0]
    assert count == 2


def test_extractor_preserves_unrelated_edges_owned_by_other_extractors(hs) -> None:
    """The idempotency wipe targets only the four edge_types this
    extractor emits; pre-existing rows owned by ``LineageExtractor``
    (task_runs_notebook, pipeline_writes_table, ...) survive."""
    seed_table_entity(hs, schema="public", table="orders")
    sales_id = seed_table_entity(hs, schema="public", table="sales")
    _seed_query(hs, sql="SELECT * FROM orders")
    with hs._connect() as conn:
        conn.execute(
            """
            INSERT INTO asset_lineage_edges
                (profile_name, from_kind, from_id, to_kind, to_id,
                 edge_type, raw_ref, discovered_at)
            VALUES ('p', 'pipeline', 99, 'table', ?, 'pipeline_writes_table', '{}', ?)
            """,
            (sales_id, time.time()),
        )

    SQLParseExtractor(hs._connect()).extract_for_profile("p")

    with hs._connect() as conn:
        edge_types = sorted(
            row[0]
            for row in conn.execute(
                "SELECT edge_type FROM asset_lineage_edges WHERE profile_name = 'p'"
            ).fetchall()
        )
    assert edge_types == sorted([EDGE_QUERY_READS_TABLE, "pipeline_writes_table"])


def test_extractor_drops_unresolvable_table_references(hs) -> None:
    """A FROM clause that names a table not in catalog_entities is just
    silently dropped — no spurious edge, no error."""
    seed_table_entity(hs, schema="public", table="orders")
    qid = _seed_query(hs, sql="SELECT * FROM orders JOIN ghost_table ON 1=1")
    SQLParseExtractor(hs._connect()).extract_for_profile("p")
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT to_id, edge_type FROM asset_lineage_edges WHERE from_id = ?",
            (qid,),
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][1] == EDGE_QUERY_READS_TABLE
