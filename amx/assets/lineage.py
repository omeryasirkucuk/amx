"""Asset-to-asset lineage extraction for jobs and pipelines.

Populates :data:`asset_lineage_edges` from platform metadata that is
already stored in the AMX history DB after a refresh. Edges captured:

* ``task_runs_notebook`` — job task references a notebook
* ``task_runs_pipeline`` — job task references a DLT pipeline
* ``task_runs_query`` — job task references a saved SQL query
* ``task_depends_on`` — task-to-task DAG within one job
* ``pipeline_includes_notebook`` — DLT pipeline's libraries[] notebook ref
* ``pipeline_writes_table`` — DLT pipeline target schema + dataset name

Reads are deliberately scoped to *metadata* rows (``remote_job_tasks``,
``remote_pipelines.libraries_json``). The extractor never opens or
parses notebook source — lineage stays at the asset level.

Idempotent: :meth:`LineageExtractor.extract_for_profile` clears the
profile's existing rows in :data:`asset_lineage_edges` and rewrites
them in one transaction. Re-runs produce the same edge set byte for
byte (modulo ``discovered_at``).
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("assets.lineage")


# Order matters only for human-readable inspection; the UNIQUE
# constraint on ``asset_lineage_edges`` makes the edge set itself
# order-independent.
EDGE_TASK_RUNS_NOTEBOOK = "task_runs_notebook"
EDGE_TASK_RUNS_PIPELINE = "task_runs_pipeline"
EDGE_TASK_RUNS_QUERY = "task_runs_query"
EDGE_TASK_DEPENDS_ON = "task_depends_on"
EDGE_PIPELINE_INCLUDES_NOTEBOOK = "pipeline_includes_notebook"
EDGE_PIPELINE_WRITES_TABLE = "pipeline_writes_table"


# The set of edge_types this extractor owns. The idempotency wipe in
# :meth:`LineageExtractor.extract_for_profile` restricts itself to
# these labels so it never destroys rows written by the SQL-parse
# extractor (``query_*``, ``notebook_*``) or any future asset_edge
# producer that lives outside this module.
_ASSET_EDGE_TYPES: tuple[str, ...] = (
    EDGE_TASK_RUNS_NOTEBOOK,
    EDGE_TASK_RUNS_PIPELINE,
    EDGE_TASK_RUNS_QUERY,
    EDGE_TASK_DEPENDS_ON,
    EDGE_PIPELINE_INCLUDES_NOTEBOOK,
    EDGE_PIPELINE_WRITES_TABLE,
)


class LineageExtractor:
    """Compute and persist asset-to-asset lineage edges for one profile.

    Stateless beyond the SQLite connection it is constructed with.
    Callers typically instantiate one per refresh pass and invoke
    :meth:`extract_for_profile`.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def extract_for_profile(self, profile_name: str) -> int:
        """Re-derive every edge for ``profile_name``.

        Returns the total number of edges written. The job and
        pipeline passes wipe their own slice of the table inside a
        single transaction, so a partial failure rolls back cleanly
        and the profile keeps its previous edge set. The SQL-parse
        extractor runs next and owns a disjoint set of edge_types
        (``query_*`` and ``notebook_*``), so it manages its own
        idempotency wipe and never collides with the asset edges.
        """
        from amx.lineage.extractors.sql_parse import SQLParseExtractor
        from amx.lineage.extractors.system_tables.databricks import (
            DatabricksSystemTablesExtractor,
            build_query_runner_for_profile,
        )

        now = time.time()
        with self.conn:
            # The asset-side wipe must restrict by edge_type so the
            # SQL-parse rows live outside its blast radius. Earlier
            # versions truncated the entire profile_name slice; that
            # destroyed any SQL-parse rows on every refresh.
            placeholders = ",".join("?" for _ in _ASSET_EDGE_TYPES)
            self.conn.execute(
                f"""
                DELETE FROM asset_lineage_edges
                WHERE profile_name = ? AND edge_type IN ({placeholders})
                """,  # noqa: S608 — placeholders bound below
                (profile_name, *_ASSET_EDGE_TYPES),
            )
            jobs_written = self._extract_jobs(profile_name, now)
            pipelines_written = self._extract_pipelines(profile_name, now)
        sql_parse_written = SQLParseExtractor(self.conn).extract_for_profile(profile_name)
        # Platform system-tables pass. Returns silently when the
        # profile is not Databricks, when the workspace has not
        # enabled system.access.*, or when engine construction
        # fails — none of which should fail the broader refresh.
        system_table_counts: dict[str, int] = {
            "table_lineage": 0,
            "column_lineage": 0,
            "usage_backfilled": 0,
        }
        try:
            runner = build_query_runner_for_profile(profile_name)
            if runner is not None:
                system_table_counts = DatabricksSystemTablesExtractor(
                    self.conn, query_runner=runner
                ).extract_for_profile(profile_name)
        except Exception as exc:  # noqa: BLE001
            log.info("Lineage extraction: system-tables pass skipped: %s", exc)
        system_table_total = sum(system_table_counts.values())
        total = jobs_written + pipelines_written + sql_parse_written + system_table_total
        log.info(
            "Lineage extraction for %s: %d job edges, %d pipeline edges, "
            "%d SQL-parse edges, %d system-tables rows (%d table + %d column + %d usage)",
            profile_name,
            jobs_written,
            pipelines_written,
            sql_parse_written,
            system_table_total,
            system_table_counts["table_lineage"],
            system_table_counts["column_lineage"],
            system_table_counts["usage_backfilled"],
        )
        return total

    # ── job extraction ───────────────────────────────────────────

    def _extract_jobs(self, profile_name: str, now: float) -> int:
        """Walk every job's tasks and write the corresponding edges."""
        job_rows = self.conn.execute(
            "SELECT id FROM remote_jobs WHERE profile_name = ?",
            (profile_name,),
        ).fetchall()
        if not job_rows:
            return 0
        written = 0
        for job_row in job_rows:
            job_id = int(job_row[0])
            written += self._extract_tasks_for_job(profile_name, job_id, now)
        return written

    def _extract_tasks_for_job(self, profile_name: str, job_id: int, now: float) -> int:
        """Resolve task-level lineage for a single ``remote_jobs.id``.

        Each task may carry a notebook, pipeline, or SQL query
        reference. Inter-task ``depends_on`` arrows become
        ``task_depends_on`` edges that point from the parent job to
        itself (``from_id == to_id == job_id``) with the task pair
        encoded in ``raw_ref``. The two extra edges-with-self may
        feel unusual, but keeping every edge in one table beats
        introducing a second table just for task DAGs.
        """
        task_rows = self.conn.execute(
            """
            SELECT task_key, task_type, notebook_id_fk, sql_query_id,
                   pipeline_id_fk, depends_on_json, notebook_path
            FROM remote_job_tasks
            WHERE job_id_fk = ?
            """,
            (job_id,),
        ).fetchall()
        if not task_rows:
            return 0

        # Map each task ref to an internal id. notebook + pipeline
        # already carry FK fields; queries need a lookup by
        # external_id. Positional access keeps the extractor agnostic
        # to whether the caller's connection has ``sqlite3.Row`` set
        # as its ``row_factory``.
        task_payload: list[tuple[str, str, int | None, int | None, str | None, str | None]] = []
        query_external_ids: set[str] = set()
        for row in task_rows:
            task_key = str(row[0])
            task_type = str(row[1] or "")
            notebook_id = row[2]
            sql_query_id = row[3]
            pipeline_id = row[4]
            notebook_path = row[6]
            task_payload.append(
                (
                    task_key,
                    task_type,
                    int(notebook_id) if notebook_id is not None else None,
                    int(pipeline_id) if pipeline_id is not None else None,
                    str(sql_query_id) if sql_query_id else None,
                    str(notebook_path) if notebook_path else None,
                )
            )
            if sql_query_id:
                query_external_ids.add(str(sql_query_id))

        query_id_lookup: dict[str, int] = {}
        if query_external_ids:
            placeholders = ",".join("?" for _ in query_external_ids)
            rows = self.conn.execute(
                f"""
                SELECT external_id, id FROM remote_queries
                WHERE profile_name = ? AND external_id IN ({placeholders})
                """,  # noqa: S608 — placeholders bound below
                (profile_name, *sorted(query_external_ids)),
            ).fetchall()
            query_id_lookup = {str(r[0]): int(r[1]) for r in rows}

        edges: list[tuple[str, int, str, int, str, str | None]] = []
        for (
            task_key,
            _task_type,
            notebook_id,
            pipeline_id,
            sql_query_id,
            notebook_path,
        ) in task_payload:
            if notebook_id is not None:
                edges.append(
                    (
                        "job",
                        job_id,
                        "notebook",
                        notebook_id,
                        EDGE_TASK_RUNS_NOTEBOOK,
                        _ref_payload(
                            task_key=task_key,
                            notebook_path=notebook_path,
                        ),
                    )
                )
            if pipeline_id is not None:
                edges.append(
                    (
                        "job",
                        job_id,
                        "pipeline",
                        pipeline_id,
                        EDGE_TASK_RUNS_PIPELINE,
                        _ref_payload(task_key=task_key),
                    )
                )
            if sql_query_id is not None:
                resolved = query_id_lookup.get(sql_query_id)
                if resolved is not None:
                    edges.append(
                        (
                            "job",
                            job_id,
                            "query",
                            resolved,
                            EDGE_TASK_RUNS_QUERY,
                            _ref_payload(
                                task_key=task_key,
                                external_id=sql_query_id,
                            ),
                        )
                    )

        # Task DAG: depends_on lives in a JSON array of {"task_key": ...}
        # entries. Each becomes one task_depends_on edge.
        task_keys = {row[0] for row in task_payload}
        for row in task_rows:
            task_key = str(row[0])
            raw_depends = row[5]
            for parent_key in _decode_depends_on(raw_depends):
                if parent_key not in task_keys:
                    continue
                edges.append(
                    (
                        "job",
                        job_id,
                        "job",
                        job_id,
                        EDGE_TASK_DEPENDS_ON,
                        json.dumps({"from_task": parent_key, "to_task": task_key}),
                    )
                )

        return self._write_edges(profile_name, edges, now)

    # ── pipeline extraction ──────────────────────────────────────

    def _extract_pipelines(self, profile_name: str, now: float) -> int:
        pipeline_rows = self.conn.execute(
            """
            SELECT id, target_schema, libraries_json
            FROM remote_pipelines
            WHERE profile_name = ?
            """,
            (profile_name,),
        ).fetchall()
        if not pipeline_rows:
            return 0

        # Resolve notebook paths once so we don't re-query SQLite per
        # pipeline. Maps workspace_path → remote_notebooks.id.
        notebook_paths = {
            str(r[0]): int(r[1])
            for r in self.conn.execute(
                "SELECT workspace_path, id FROM remote_notebooks WHERE profile_name = ?",
                (profile_name,),
            ).fetchall()
            if r[0] is not None
        }
        # Catalog entities keyed by (schema_name, table_name) for
        # pipeline → table edges. database_name is filled in by the
        # ingest layer per profile; the pipeline target_schema does
        # not always carry the database segment.
        catalog_rows = self.conn.execute(
            """
            SELECT id, database_name, schema_name, table_name
            FROM catalog_entities
            WHERE entity_kind = 'table'
            """
        ).fetchall()
        catalog_by_table: dict[tuple[str, str], int] = {}
        for cid, _db, schema, table in catalog_rows:
            if not table:
                continue
            key = (str(schema or "").lower(), str(table).lower())
            # First win — duplicates across databases are rare in
            # practice and the pipeline UI can disambiguate visually.
            catalog_by_table.setdefault(key, int(cid))

        edges: list[tuple[str, int, str, int, str, str | None]] = []
        for row in pipeline_rows:
            pipeline_id = int(row[0])
            target_schema = str(row[1] or "").strip()
            libs_json = row[2]
            for notebook_path in _decode_pipeline_notebook_paths(libs_json):
                resolved = notebook_paths.get(notebook_path)
                if resolved is None:
                    continue
                edges.append(
                    (
                        "pipeline",
                        pipeline_id,
                        "notebook",
                        resolved,
                        EDGE_PIPELINE_INCLUDES_NOTEBOOK,
                        json.dumps({"notebook_path": notebook_path}),
                    )
                )
            # Pipeline writes_table edges only when we can match a
            # catalog row in the target schema. Without that match
            # the edge has no clickable target on the frontend.
            if target_schema:
                schema_key = target_schema.lower()
                for (cat_schema, cat_table), entity_id in catalog_by_table.items():
                    if cat_schema == schema_key:
                        edges.append(
                            (
                                "pipeline",
                                pipeline_id,
                                "table",
                                entity_id,
                                EDGE_PIPELINE_WRITES_TABLE,
                                json.dumps(
                                    {
                                        "target_schema": target_schema,
                                        "table_name": cat_table,
                                    }
                                ),
                            )
                        )

        return self._write_edges(profile_name, edges, now)

    # ── insert helper ────────────────────────────────────────────

    def _write_edges(
        self,
        profile_name: str,
        edges: list[tuple[str, int, str, int, str, str | None]],
        now: float,
    ) -> int:
        if not edges:
            return 0
        rows = [(profile_name, fk, fi, tk, ti, et, ref, now) for (fk, fi, tk, ti, et, ref) in edges]
        try:
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO asset_lineage_edges (
                    profile_name, from_kind, from_id, to_kind, to_id,
                    edge_type, raw_ref, discovered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("LineageExtractor: edge insert failed: %s", exc)
            return 0
        return len(rows)


# ── module-level helpers ─────────────────────────────────────────


def _decode_depends_on(raw: Any) -> list[str]:
    """Extract task keys from a ``depends_on_json`` column value.

    Databricks shape is ``[{"task_key": "load"}, {"task_key": "clean"}]``.
    Handles malformed JSON and unexpected shapes by returning an
    empty list — lineage extraction never fails an entire pass on
    one bad row.
    """
    if not raw:
        return []
    try:
        decoded = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    if not isinstance(decoded, list):
        return []
    out: list[str] = []
    for entry in decoded:
        if isinstance(entry, dict) and isinstance(entry.get("task_key"), str):
            out.append(str(entry["task_key"]))
    return out


def _decode_pipeline_notebook_paths(raw: Any) -> list[str]:
    """Extract notebook workspace paths from a pipeline ``libraries_json``.

    Databricks shape per element is ``{"notebook": {"path":
    "/Workspace/..."}}``. Other shapes (``"file": {...}``,
    ``"jar": "..."``) are skipped — only notebook refs participate
    in lineage.
    """
    if not raw:
        return []
    try:
        decoded = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    if not isinstance(decoded, list):
        return []
    out: list[str] = []
    for entry in decoded:
        if not isinstance(entry, dict):
            continue
        notebook = entry.get("notebook")
        if isinstance(notebook, dict):
            path = notebook.get("path")
            if isinstance(path, str) and path.strip():
                out.append(path.strip())
    return out


def _ref_payload(**fields: Any) -> str:
    """Serialise the non-empty key/value pairs as compact JSON."""
    filtered = {k: v for k, v in fields.items() if v is not None}
    if not filtered:
        return ""
    return json.dumps(filtered, sort_keys=True)


__all__ = [
    "EDGE_TASK_RUNS_NOTEBOOK",
    "EDGE_TASK_RUNS_PIPELINE",
    "EDGE_TASK_RUNS_QUERY",
    "EDGE_TASK_DEPENDS_ON",
    "EDGE_PIPELINE_INCLUDES_NOTEBOOK",
    "EDGE_PIPELINE_WRITES_TABLE",
    "LineageExtractor",
]
