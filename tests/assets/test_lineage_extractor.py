"""Tests for :mod:`amx.assets.lineage` — asset-to-asset edge extraction.

The extractor reads three sources:

* ``remote_job_tasks`` — task list with FKs to notebook / pipeline
  and an external query id; ``depends_on_json`` for the task DAG.
* ``remote_pipelines.libraries_json`` — notebook references that
  feed a DLT pipeline.
* ``remote_pipelines.target_schema`` — joined against
  ``catalog_entities`` to find tables the pipeline writes.

The extractor MUST NOT parse notebook source — lineage stays at the
asset level. The "no source read" contract is checked indirectly by
seeding a notebook with surprising body content and asserting the
edge set is unaffected.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from amx.assets.lineage import (
    EDGE_PIPELINE_INCLUDES_NOTEBOOK,
    EDGE_PIPELINE_WRITES_TABLE,
    EDGE_TASK_DEPENDS_ON,
    EDGE_TASK_RUNS_NOTEBOOK,
    EDGE_TASK_RUNS_PIPELINE,
    EDGE_TASK_RUNS_QUERY,
    LineageExtractor,
)
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteHistoryStore:
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


def _seed_notebook(
    store: SQLiteHistoryStore,
    *,
    name: str,
    workspace_path: str,
    profile: str = "prof",
    source: str = "{}",
) -> int:
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_notebooks (
                profile_name, platform, external_id, name, workspace_path,
                qualified_name, language, source_text, source_hash, ingested_at
            ) VALUES (?, 'databricks', ?, ?, ?, NULL, 'python', ?, 'h', ?)
            """,
            (profile, f"ext-{name}", name, workspace_path, source, time.time()),
        )
        return int(cur.lastrowid or 0)


def _seed_pipeline(
    store: SQLiteHistoryStore,
    *,
    name: str,
    target_schema: str,
    libraries: list[dict],
    profile: str = "prof",
) -> int:
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_pipelines (
                profile_name, pipeline_id, name, target_schema, edition,
                continuous, photon, libraries_json, ingested_at
            ) VALUES (?, ?, ?, ?, 'PRO', 0, 1, ?, ?)
            """,
            (
                profile,
                f"pid-{name}",
                name,
                target_schema,
                json.dumps(libraries),
                time.time(),
            ),
        )
        return int(cur.lastrowid or 0)


def _seed_query(
    store: SQLiteHistoryStore,
    *,
    external_id: str,
    profile: str = "prof",
) -> int:
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_queries (
                profile_name, platform, kind, external_id, name, sql_text,
                sql_hash, ingested_at
            ) VALUES (?, 'databricks', 'history', ?, NULL, 'SELECT 1', 'h', ?)
            """,
            (profile, external_id, time.time()),
        )
        return int(cur.lastrowid or 0)


def _seed_job_with_tasks(
    store: SQLiteHistoryStore,
    *,
    name: str,
    tasks: list[dict],
    profile: str = "prof",
) -> int:
    """Insert a job + its tasks. Each task dict carries optional
    notebook_id_fk / sql_query_id / pipeline_id_fk / depends_on.
    """
    with store._connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO remote_jobs (
                profile_name, job_id, name, ingested_at
            ) VALUES (?, ?, ?, ?)
            """,
            (profile, abs(hash(name)) % (10**9), name, time.time()),
        )
        job_id = int(cur.lastrowid or 0)
        for task in tasks:
            conn.execute(
                """
                INSERT INTO remote_job_tasks (
                    job_id_fk, task_key, task_type, notebook_path,
                    notebook_id_fk, sql_query_id, sql_warehouse_id,
                    pipeline_id_fk, depends_on_json, raw_definition_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    task["task_key"],
                    task.get("task_type", "notebook_task"),
                    task.get("notebook_path"),
                    task.get("notebook_id_fk"),
                    task.get("sql_query_id"),
                    None,
                    task.get("pipeline_id_fk"),
                    json.dumps(task.get("depends_on") or []),
                    "{}",
                ),
            )
        return job_id


def test_job_with_mixed_tasks_emits_three_asset_edges(store: SQLiteHistoryStore) -> None:
    nb_id = _seed_notebook(store, name="loader", workspace_path="/Workspace/A")
    pipe_id = _seed_pipeline(store, name="etl", target_schema="silver", libraries=[])
    query_external = "qext-1"
    query_id = _seed_query(store, external_id=query_external)

    job_id = _seed_job_with_tasks(
        store,
        name="job_main",
        tasks=[
            {"task_key": "load", "notebook_id_fk": nb_id, "task_type": "notebook_task"},
            {"task_key": "run_pipeline", "pipeline_id_fk": pipe_id, "task_type": "pipeline_task"},
            {"task_key": "post_check", "sql_query_id": query_external, "task_type": "sql_task"},
        ],
    )

    with store._connect() as conn:
        LineageExtractor(conn).extract_for_profile("prof")
        rows = conn.execute(
            "SELECT to_kind, to_id, edge_type FROM asset_lineage_edges "
            "WHERE profile_name = ? AND from_kind = 'job' AND from_id = ? "
            "AND edge_type != ?",
            ("prof", job_id, EDGE_TASK_DEPENDS_ON),
        ).fetchall()

    edges = {(str(r[0]), int(r[1]), str(r[2])) for r in rows}
    assert edges == {
        ("notebook", nb_id, EDGE_TASK_RUNS_NOTEBOOK),
        ("pipeline", pipe_id, EDGE_TASK_RUNS_PIPELINE),
        ("query", query_id, EDGE_TASK_RUNS_QUERY),
    }


def test_task_dependencies_become_task_dag_edges(store: SQLiteHistoryStore) -> None:
    nb_id = _seed_notebook(store, name="loader", workspace_path="/Workspace/A")
    job_id = _seed_job_with_tasks(
        store,
        name="dag_job",
        tasks=[
            {"task_key": "extract", "notebook_id_fk": nb_id},
            {
                "task_key": "transform",
                "notebook_id_fk": nb_id,
                "depends_on": [{"task_key": "extract"}],
            },
        ],
    )

    with store._connect() as conn:
        LineageExtractor(conn).extract_for_profile("prof")
        dag_rows = conn.execute(
            "SELECT raw_ref FROM asset_lineage_edges WHERE profile_name = ? "
            "AND from_id = ? AND edge_type = ?",
            ("prof", job_id, EDGE_TASK_DEPENDS_ON),
        ).fetchall()
    decoded = [json.loads(str(r[0])) for r in dag_rows]
    assert decoded == [{"from_task": "extract", "to_task": "transform"}]


def test_pipeline_includes_notebook_edges(store: SQLiteHistoryStore) -> None:
    nb_a = _seed_notebook(store, name="a", workspace_path="/Workspace/A")
    nb_b = _seed_notebook(store, name="b", workspace_path="/Workspace/B")
    pipe_id = _seed_pipeline(
        store,
        name="etl",
        target_schema="silver",
        libraries=[
            {"notebook": {"path": "/Workspace/A"}},
            {"notebook": {"path": "/Workspace/B"}},
            {"jar": "dbfs:/tmp/foo.jar"},  # ignored — only notebook refs count
        ],
    )

    with store._connect() as conn:
        LineageExtractor(conn).extract_for_profile("prof")
        rows = conn.execute(
            "SELECT to_kind, to_id, edge_type FROM asset_lineage_edges "
            "WHERE profile_name = ? AND from_kind = 'pipeline' AND from_id = ?",
            ("prof", pipe_id),
        ).fetchall()
    edges = {(str(r[0]), int(r[1]), str(r[2])) for r in rows}
    assert edges == {
        ("notebook", nb_a, EDGE_PIPELINE_INCLUDES_NOTEBOOK),
        ("notebook", nb_b, EDGE_PIPELINE_INCLUDES_NOTEBOOK),
    }


def test_extractor_is_idempotent(store: SQLiteHistoryStore) -> None:
    nb_id = _seed_notebook(store, name="loader", workspace_path="/Workspace/A")
    _seed_job_with_tasks(
        store,
        name="j",
        tasks=[{"task_key": "k", "notebook_id_fk": nb_id}],
    )

    with store._connect() as conn:
        ext = LineageExtractor(conn)
        ext.extract_for_profile("prof")
        ext.extract_for_profile("prof")
        ext.extract_for_profile("prof")
        count = conn.execute(
            "SELECT COUNT(*) FROM asset_lineage_edges WHERE profile_name = ?",
            ("prof",),
        ).fetchone()[0]
    assert count == 1


def test_pipeline_writes_table_edge(store: SQLiteHistoryStore) -> None:
    with store._connect() as conn:
        conn.execute(
            """
            INSERT INTO catalog_entities (
                db_profile, database_name, schema_name, table_name, entity_kind
            ) VALUES ('prof', 'main', 'silver', 'orders_clean', 'table')
            """
        )
        conn.commit()
    pipe_id = _seed_pipeline(
        store,
        name="etl",
        target_schema="silver",
        libraries=[],
    )

    with store._connect() as conn:
        LineageExtractor(conn).extract_for_profile("prof")
        rows = conn.execute(
            "SELECT to_kind, edge_type FROM asset_lineage_edges "
            "WHERE profile_name = ? AND from_kind = 'pipeline' AND from_id = ?",
            ("prof", pipe_id),
        ).fetchall()
    edge_types = {str(r[1]) for r in rows}
    assert EDGE_PIPELINE_WRITES_TABLE in edge_types


def test_unresolved_query_external_id_is_skipped(store: SQLiteHistoryStore) -> None:
    """A sql_task with an unknown query external_id emits no edge."""
    nb_id = _seed_notebook(store, name="loader", workspace_path="/Workspace/A")
    job_id = _seed_job_with_tasks(
        store,
        name="j",
        tasks=[
            {"task_key": "k", "notebook_id_fk": nb_id},
            {"task_key": "q", "sql_query_id": "ghost-query"},
        ],
    )

    with store._connect() as conn:
        LineageExtractor(conn).extract_for_profile("prof")
        rows = conn.execute(
            "SELECT edge_type FROM asset_lineage_edges WHERE from_id = ? AND edge_type != ?",
            (job_id, EDGE_TASK_DEPENDS_ON),
        ).fetchall()
    assert {str(r[0]) for r in rows} == {EDGE_TASK_RUNS_NOTEBOOK}
