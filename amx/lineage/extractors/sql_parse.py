"""SQL-parse lineage extraction for queries and notebooks.

Reads the SQL that already lives in ``remote_queries.sql_text`` and in
the code cells of ``remote_notebooks.source_text``, parses each
statement with sqlglot, and writes one ``asset_lineage_edges`` row
per table reference. Direction-aware: tables under ``INSERT``,
``UPDATE``, ``DELETE``, ``MERGE``, ``CREATE TABLE ... AS``, and
``COPY INTO`` land as writes; everything else lands as reads.

Designed to be idempotent per ``(profile_name, from_kind, from_id)``
so re-runs do not accumulate duplicates. Failure to parse a single
statement does not fail the pass; the offending row is skipped with
an info-level log and the rest of the corpus continues.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("lineage.sql_parse")


# Edge type literals. The asset_lineage_edges schema description in
# amx/storage/schema_descriptions.py enumerates the full set; these
# four are the ones this extractor emits.
EDGE_QUERY_READS_TABLE = "query_reads_table"
EDGE_QUERY_WRITES_TABLE = "query_writes_table"
EDGE_NOTEBOOK_READS_TABLE = "notebook_reads_table"
EDGE_NOTEBOOK_WRITES_TABLE = "notebook_writes_table"

# Edge types this extractor owns. The idempotency wipe runs only on
# these labels so other extractors' edges (task_runs_*, pipeline_*,
# ...) keep their rows even when the SQL-parse pass writes nothing.
_OWNED_EDGE_TYPES = (
    EDGE_QUERY_READS_TABLE,
    EDGE_QUERY_WRITES_TABLE,
    EDGE_NOTEBOOK_READS_TABLE,
    EDGE_NOTEBOOK_WRITES_TABLE,
)

# Platform → sqlglot dialect string. Anything not in this map is
# passed through to sqlglot verbatim (sqlglot accepts the same
# identifier for most dialects). Mirrors view_ddl.py so both
# extractors agree on dialect tagging.
_DIALECT_MAP: dict[str, str] = {
    "postgresql": "postgres",
    "redshift": "redshift",
    "mssql": "tsql",
    "databricks": "databricks",
    "mysql": "mysql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "duckdb": "duckdb",
    "clickhouse": "clickhouse",
    "oracle": "oracle",
    "trino": "trino",
    "presto": "presto",
}


def extract_reads_writes(
    sql: str,
    dialect: str | None = None,
) -> tuple[set[str], set[str]]:
    """Return ``(reads, writes)`` as sets of lower-cased table FQNs.

    Each FQN is the dot-joined ``[catalog].[schema].table`` triple
    with empty parts elided. CTE names introduced inside the same
    statement are dropped from the read set so a ``WITH cte AS
    (SELECT ...) SELECT * FROM cte`` doesn't fabricate a phantom
    table.

    Returns ``(set(), set())`` when sqlglot can't parse the SQL or
    the package is unavailable. Callers should treat that as "no
    lineage extractable" rather than an error.
    """
    sqlglot = _load_sqlglot()
    if sqlglot is None or not sql or not sql.strip():
        return set(), set()
    dialect_tag = _DIALECT_MAP.get((dialect or "").lower(), dialect)
    try:
        statements = sqlglot.parse(sql, dialect=dialect_tag, error_level=None)
    except Exception as exc:  # noqa: BLE001 — sqlglot raises many shapes
        log.debug("sqlglot.parse failed (%s): %s", dialect_tag, exc)
        return set(), set()
    reads: set[str] = set()
    writes: set[str] = set()
    for stmt in statements:
        if stmt is None:
            continue
        try:
            _classify_statement(sqlglot, stmt, reads, writes)
        except Exception as exc:  # noqa: BLE001
            log.debug("sql_parse classify failed: %s", exc)
    # Anything that ended up as a write should not also show up as a
    # read for the same statement (INSERT INTO sales SELECT ... FROM
    # sales joined to itself is a degenerate case that callers can
    # rediscover via separate read+write rows on the same edge if
    # needed). Drop reads that duplicate writes for clarity.
    reads -= writes
    return reads, writes


def _classify_statement(
    sqlglot: Any,
    stmt: Any,
    reads: set[str],
    writes: set[str],
) -> None:
    """Populate ``reads`` and ``writes`` from one parsed statement."""
    exp = sqlglot.exp
    write_target_ids: set[int] = set()
    cte_names: set[str] = set()

    def _record_write(node: Any) -> None:
        table = _first_table(exp, node)
        if table is None:
            return
        write_target_ids.add(id(table))
        fqn = _table_fqn(table)
        if fqn:
            writes.add(fqn)

    if isinstance(stmt, exp.Insert | exp.Update | exp.Delete | exp.Merge):
        _record_write(stmt.this)
    elif isinstance(stmt, exp.Create):
        kind = (stmt.args.get("kind") or "").upper() if isinstance(stmt.args, dict) else ""
        if kind in {"TABLE", "VIEW", "MATERIALIZED VIEW"} and stmt.expression is not None:
            _record_write(stmt.this)
    else:
        # COPY INTO (Snowflake / Databricks), TRUNCATE, and other
        # write-shaped statements are surfaced through bespoke
        # sqlglot nodes that vary by dialect. Fall back to the
        # generic "first table under the root is the target" rule
        # only when the parser produced a dialect-specific Copy
        # node, so we don't accidentally mark a SELECT's first
        # table as a write.
        node_cls_name = type(stmt).__name__.lower()
        if "copy" in node_cls_name or node_cls_name in {"truncate"}:
            _record_write(stmt.this)

    # Common Table Expressions: track their alias names so we can
    # drop them from the read set further down. sqlglot 30 stores
    # the With clause under both ``with`` and ``with_`` depending on
    # the parse path, so try both. Walking ``find_all(exp.CTE)``
    # would also catch nested CTEs that the top-level lookup misses.
    try:
        for cte in stmt.find_all(exp.CTE):
            alias = getattr(cte, "alias", "") or ""
            if alias:
                cte_names.add(str(alias).lower())
    except Exception:
        pass

    for tbl in stmt.find_all(exp.Table):
        if id(tbl) in write_target_ids:
            continue
        fqn = _table_fqn(tbl)
        if not fqn:
            continue
        if fqn in cte_names or fqn.split(".")[-1] in cte_names:
            continue
        reads.add(fqn)


def _first_table(exp_module: Any, node: Any) -> Any | None:
    """Return the first ``Table`` descendant of ``node`` (or ``node`` itself)."""
    if node is None:
        return None
    if isinstance(node, exp_module.Table):
        return node
    try:
        return next(iter(node.find_all(exp_module.Table)), None)
    except Exception:
        return None


def _table_fqn(table_node: Any) -> str:
    """Dot-join the catalog/schema/table parts of an ``exp.Table``."""
    try:
        parts = [
            (table_node.args.get(key).name if table_node.args.get(key) is not None else "")
            for key in ("catalog", "db")
        ]
        parts.append(table_node.name or "")
    except Exception:
        return ""
    parts = [p for p in parts if p]
    return ".".join(parts).lower()


def extract_sql_blocks_from_notebook(source_text: str, language: str) -> list[str]:
    """Pull every SQL-ish code block out of a notebook's ``source_text``.

    Handles three shapes:

    1. The notebook's primary language is SQL — every ``code`` cell
       counts as SQL.
    2. The notebook is a Databricks Python notebook with ``%sql`` /
       ``%%sql`` magics — only the cells (or sub-cells) tagged with
       those magics count.
    3. The notebook is plain JSON without recognisable SQL — returns
       an empty list.

    Cells that fail to parse as JSON degrade to "no SQL found", in
    line with the extractor's "skip and continue" philosophy.
    """
    if not source_text:
        return []
    try:
        nb = json.loads(source_text)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(nb, dict):
        return []
    cells = nb.get("cells")
    if not isinstance(cells, list):
        return []
    lang = (language or "").strip().lower()
    notebook_is_sql = lang == "sql"
    out: list[str] = []
    for cell in cells:
        if not isinstance(cell, dict):
            continue
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        if isinstance(src, list):
            src = "".join(s for s in src if isinstance(s, str))
        if not isinstance(src, str):
            continue
        stripped = src.strip()
        if not stripped:
            continue
        if notebook_is_sql:
            out.append(stripped)
            continue
        # %sql / %%sql magic at the top of the cell — strip the
        # magic line, treat the remainder as SQL. If a cell mixes
        # other magics, the parser will simply fail on that block
        # and we'll skip it.
        first_line, _, rest = stripped.partition("\n")
        first_token = first_line.strip().lower()
        if first_token in {"%sql", "%%sql"}:
            body = rest.strip()
            if body:
                out.append(body)
    return out


class SQLParseExtractor:
    """Persist query/notebook → table edges by parsing stored SQL.

    Constructed with an open ``sqlite3.Connection`` to the AMX
    history DB. The extractor is stateless beyond that handle; one
    instance per refresh pass is the expected use.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def extract_for_profile(self, profile_name: str) -> int:
        """Re-derive every SQL-parse edge for ``profile_name``.

        Returns the number of edges written. The pass wipes existing
        rows owned by this extractor for the profile before
        inserting so re-runs converge on the same set instead of
        accumulating.
        """
        now = time.time()
        catalog_lookup = self._catalog_lookup(profile_name)
        if not catalog_lookup:
            log.info(
                "SQLParseExtractor: profile %s has no catalog tables to resolve against",
                profile_name,
            )
            return 0
        with self.conn:
            placeholders = ",".join("?" for _ in _OWNED_EDGE_TYPES)
            self.conn.execute(
                f"""
                DELETE FROM asset_lineage_edges
                WHERE profile_name = ?
                  AND edge_type IN ({placeholders})
                """,  # noqa: S608 — placeholders bound below
                (profile_name, *_OWNED_EDGE_TYPES),
            )
            query_edges = list(self._iter_query_edges(profile_name, catalog_lookup))
            notebook_edges = list(self._iter_notebook_edges(profile_name, catalog_lookup))
            written = self._write_edges(profile_name, query_edges + notebook_edges, now)
        log.info(
            "SQLParseExtractor for %s: %d query rows + %d notebook rows -> %d edges",
            profile_name,
            len(query_edges),
            len(notebook_edges),
            written,
        )
        return written

    # ── catalog resolution ───────────────────────────────────────

    def _catalog_lookup(self, profile_name: str) -> dict[tuple[str, str], int]:
        """Map ``(schema_lc, table_lc) -> catalog_entities.id`` for one profile.

        Falls back to a ``("", table_lc)`` key when no schema is
        available, so an unqualified ``FROM orders`` in a single-
        schema profile still resolves. Keeps the first match per
        key — duplicates across databases are rare and the picker
        UI can disambiguate further if needed.
        """
        rows = self.conn.execute(
            """
            SELECT id, schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ? AND entity_kind = 'table'
            """,
            (profile_name,),
        ).fetchall()
        out: dict[tuple[str, str], int] = {}
        for cid, schema, table in rows:
            if not table:
                continue
            schema_key = str(schema or "").lower()
            table_key = str(table).lower()
            out.setdefault((schema_key, table_key), int(cid))
            # Also index by bare table name so unqualified refs
            # still match when the schema part is empty.
            out.setdefault(("", table_key), int(cid))
        return out

    @staticmethod
    def _resolve_fqn(
        fqn: str,
        catalog_lookup: dict[tuple[str, str], int],
    ) -> int | None:
        """Look ``fqn`` up in the catalog map; return ``None`` if missing."""
        parts = [p for p in fqn.split(".") if p]
        if not parts:
            return None
        table = parts[-1].lower()
        schema = parts[-2].lower() if len(parts) >= 2 else ""
        eid = catalog_lookup.get((schema, table))
        if eid is None and schema:
            # Fall back to the bare-table entry so cross-schema refs
            # still hit when the catalog only sees one occurrence.
            eid = catalog_lookup.get(("", table))
        return eid

    # ── queries ──────────────────────────────────────────────────

    def _iter_query_edges(
        self,
        profile_name: str,
        catalog_lookup: dict[tuple[str, str], int],
    ) -> Iterable[tuple[str, int, str, int, str, str, str | None]]:
        """Yield edges sourced from ``remote_queries.sql_text``."""
        rows = self.conn.execute(
            """
            SELECT id, platform, sql_text, external_id
            FROM remote_queries
            WHERE profile_name = ?
            """,
            (profile_name,),
        ).fetchall()
        for qid, platform, sql_text, external_id in rows:
            reads, writes = extract_reads_writes(str(sql_text or ""), str(platform or ""))
            for fqn in sorted(reads):
                eid = self._resolve_fqn(fqn, catalog_lookup)
                if eid is None:
                    continue
                yield (
                    "query",
                    int(qid),
                    "table",
                    int(eid),
                    EDGE_QUERY_READS_TABLE,
                    "read",
                    _ref({"fqn": fqn, "external_id": external_id}),
                )
            for fqn in sorted(writes):
                eid = self._resolve_fqn(fqn, catalog_lookup)
                if eid is None:
                    continue
                yield (
                    "query",
                    int(qid),
                    "table",
                    int(eid),
                    EDGE_QUERY_WRITES_TABLE,
                    "write",
                    _ref({"fqn": fqn, "external_id": external_id}),
                )

    # ── notebooks ────────────────────────────────────────────────

    def _iter_notebook_edges(
        self,
        profile_name: str,
        catalog_lookup: dict[tuple[str, str], int],
    ) -> Iterable[tuple[str, int, str, int, str, str, str | None]]:
        """Yield edges sourced from SQL cells inside ``remote_notebooks``."""
        rows = self.conn.execute(
            """
            SELECT id, platform, language, source_text, workspace_path
            FROM remote_notebooks
            WHERE profile_name = ?
            """,
            (profile_name,),
        ).fetchall()
        for nb_id, platform, language, source_text, workspace_path in rows:
            blocks = extract_sql_blocks_from_notebook(
                str(source_text or ""),
                str(language or ""),
            )
            if not blocks:
                continue
            agg_reads: set[str] = set()
            agg_writes: set[str] = set()
            for block in blocks:
                reads, writes = extract_reads_writes(block, str(platform or ""))
                agg_reads |= reads
                agg_writes |= writes
            agg_reads -= agg_writes
            for fqn in sorted(agg_reads):
                eid = self._resolve_fqn(fqn, catalog_lookup)
                if eid is None:
                    continue
                yield (
                    "notebook",
                    int(nb_id),
                    "table",
                    int(eid),
                    EDGE_NOTEBOOK_READS_TABLE,
                    "read",
                    _ref({"fqn": fqn, "workspace_path": workspace_path}),
                )
            for fqn in sorted(agg_writes):
                eid = self._resolve_fqn(fqn, catalog_lookup)
                if eid is None:
                    continue
                yield (
                    "notebook",
                    int(nb_id),
                    "table",
                    int(eid),
                    EDGE_NOTEBOOK_WRITES_TABLE,
                    "write",
                    _ref({"fqn": fqn, "workspace_path": workspace_path}),
                )

    # ── insert helper ────────────────────────────────────────────

    def _write_edges(
        self,
        profile_name: str,
        edges: list[tuple[str, int, str, int, str, str, str | None]],
        now: float,
    ) -> int:
        if not edges:
            return 0
        rows = [
            (profile_name, fk, fi, tk, ti, et, ref, now, direction)
            for (fk, fi, tk, ti, et, direction, ref) in edges
        ]
        try:
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO asset_lineage_edges (
                    profile_name, from_kind, from_id, to_kind, to_id,
                    edge_type, raw_ref, discovered_at, direction
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("SQLParseExtractor: edge insert failed: %s", exc)
            return 0
        return len(rows)


# ── helpers ──────────────────────────────────────────────────────


def _ref(payload: dict[str, Any]) -> str:
    """Compact, sorted JSON encoding for ``raw_ref`` columns."""
    filtered = {k: v for k, v in payload.items() if v not in (None, "")}
    if not filtered:
        return ""
    return json.dumps(filtered, sort_keys=True)


def _load_sqlglot() -> Any | None:
    try:
        import sqlglot  # type: ignore

        return sqlglot
    except ImportError:
        try:
            from amx.utils.optional_deps import ensure

            ensure("lineage", feature="/lineage SQL-parse extractor")
            import sqlglot  # type: ignore

            return sqlglot
        except Exception:
            return None


__all__ = [
    "EDGE_QUERY_READS_TABLE",
    "EDGE_QUERY_WRITES_TABLE",
    "EDGE_NOTEBOOK_READS_TABLE",
    "EDGE_NOTEBOOK_WRITES_TABLE",
    "SQLParseExtractor",
    "extract_reads_writes",
    "extract_sql_blocks_from_notebook",
]
