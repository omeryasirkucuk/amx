"""Databricks ``system.access.*`` lineage extractor.

Pulls table-level and column-level lineage from the workspace's own
system tables — the same source the Unity Catalog UI reads from —
into ``catalog_relationships`` with ``relationship_type='lineage_system_table'``
(or ``lineage_system_column``). Also refreshes ``last_used_at`` /
``last_user`` on matching ``asset_lineage_edges`` rows so the
table-detail Lineage tab can show "last touched by alice 2 hours ago".

The extractor is intentionally decoupled from the SQL warehouse
connector: callers supply a ``query_runner`` callable that takes a
SQL string and returns a list of dict rows. That keeps the unit
tests fast (mock runner) while letting :func:`build_query_runner`
hand off a real connection from the live engine.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from amx.utils.logging import get_logger

log = get_logger("lineage.system_tables.databricks")


# Sentinel for the relationship_type column on catalog_relationships
# rows sourced from system.access.table_lineage / column_lineage. The
# table-grain and column-grain values are kept distinct so the read
# path can filter without inspecting from_column / to_column.
REL_TABLE = "lineage_system_table"
REL_COLUMN = "lineage_system_column"

# Provenance string written into catalog_relationships.source so it's
# obvious from the row alone where the edge came from.
_SOURCE = "databricks_system_tables"

# Window over which we read the system tables. Mirrors what the Unity
# Catalog UI typically shows. Picked at 90 days so the extractor's
# wall time stays bounded on busy workspaces while still catching the
# vast majority of "what touches this table?" questions.
_LINEAGE_WINDOW_DAYS = 90
_USAGE_WINDOW_DAYS = 30


QueryRunner = Callable[[str], list[dict[str, Any]]]


class DatabricksSystemTablesExtractor:
    """Materialise Databricks system-table lineage into local storage.

    The ``query_runner`` is invoked at most three times per pass: once
    for table lineage, once for column lineage, once for usage signals.
    On any of those failing the extractor logs and moves on — a
    workspace that has not enabled ``system.access`` schemas should
    still get a clean refresh of the asset-side edges.
    """

    def __init__(
        self,
        sqlite_conn: sqlite3.Connection,
        *,
        query_runner: QueryRunner,
    ) -> None:
        self.conn = sqlite_conn
        self.query_runner = query_runner

    def extract_for_profile(self, profile_name: str) -> dict[str, int]:
        """Run all three passes for ``profile_name``.

        Returns a dict with the row counts produced by each pass so
        the caller can surface them in logs / progress events:
        ``{"table_lineage": N, "column_lineage": M, "usage_backfilled": K}``.
        """
        catalog_lookup = self._catalog_lookup(profile_name)
        if not catalog_lookup:
            log.info(
                "DatabricksSystemTablesExtractor: profile %s has no catalog tables yet",
                profile_name,
            )
            return {"table_lineage": 0, "column_lineage": 0, "usage_backfilled": 0}
        table_count = self._extract_table_lineage(profile_name, catalog_lookup)
        column_count = self._extract_column_lineage(profile_name, catalog_lookup)
        usage_count = self._backfill_usage(profile_name)
        log.info(
            "DatabricksSystemTablesExtractor for %s: %d table edges, %d column edges, %d usage rows",
            profile_name,
            table_count,
            column_count,
            usage_count,
        )
        return {
            "table_lineage": table_count,
            "column_lineage": column_count,
            "usage_backfilled": usage_count,
        }

    # ── catalog resolution ───────────────────────────────────────

    def _catalog_lookup(self, profile_name: str) -> dict[tuple[str, str, str], int]:
        """Map ``(catalog, schema, table)`` lower-case keys to entity_id.

        Databricks lineage rows always carry the three-part
        ``catalog.schema.table`` FQN; matching is done case-insensitively
        on the joined parts. Profiles without an ingested catalog will
        return an empty dict; the caller treats that as "skip the pass".
        """
        rows = self.conn.execute(
            """
            SELECT id, database_name, schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ? AND entity_kind = 'table'
            """,
            (profile_name,),
        ).fetchall()
        out: dict[tuple[str, str, str], int] = {}
        for cid, catalog, schema, table in rows:
            if not table:
                continue
            key = (
                str(catalog or "").lower(),
                str(schema or "").lower(),
                str(table).lower(),
            )
            out.setdefault(key, int(cid))
        return out

    @staticmethod
    def _resolve(
        fqn: str,
        catalog_lookup: dict[tuple[str, str, str], int],
    ) -> int | None:
        parts = [p for p in (fqn or "").split(".") if p]
        if len(parts) != 3:
            return None
        return catalog_lookup.get(tuple(p.lower() for p in parts))  # type: ignore[arg-type]

    # ── table lineage ────────────────────────────────────────────

    def _extract_table_lineage(
        self,
        profile_name: str,
        catalog_lookup: dict[tuple[str, str, str], int],
    ) -> int:
        sql = (
            "SELECT source_table_full_name AS src, "
            "       target_table_full_name AS tgt, "
            "       event_time, "
            "       created_by "
            "FROM system.access.table_lineage "
            f"WHERE event_time > now() - INTERVAL {_LINEAGE_WINDOW_DAYS} DAYS "
            "  AND source_table_full_name IS NOT NULL "
            "  AND target_table_full_name IS NOT NULL"
        )
        rows = self._run(sql)
        if not rows:
            return 0
        now = time.time()
        prepared: list[tuple[int, int, str, str, float, str]] = []
        for r in rows:
            src_id = self._resolve(str(r.get("src") or ""), catalog_lookup)
            tgt_id = self._resolve(str(r.get("tgt") or ""), catalog_lookup)
            if src_id is None or tgt_id is None or src_id == tgt_id:
                continue
            details = {
                "event_time": _iso(r.get("event_time")),
                "created_by": r.get("created_by") or "",
            }
            prepared.append(
                (
                    src_id,
                    tgt_id,
                    REL_TABLE,
                    json.dumps(details, sort_keys=True),
                    now,
                    r.get("created_by") or "",
                )
            )
        if not prepared:
            return 0
        with self.conn:
            self.conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE relationship_type = ?
                  AND source = ?
                  AND from_entity_id IN (
                      SELECT id FROM catalog_entities WHERE db_profile = ?
                  )
                """,  # noqa: S608 — fixed literals; profile bound below
                (REL_TABLE, _SOURCE, profile_name),
            )
            self.conn.executemany(
                """
                INSERT INTO catalog_relationships (
                    from_entity_id, to_entity_id, relationship_type, score,
                    source, details_json, last_seen, audit_actor
                ) VALUES (?, ?, ?, 1.0, ?, ?, ?, ?)
                """,
                [
                    (src_id, tgt_id, rel, _SOURCE, details, now, actor)
                    for (src_id, tgt_id, rel, details, now, actor) in prepared
                ],
            )
        return len(prepared)

    # ── column lineage ───────────────────────────────────────────

    def _extract_column_lineage(
        self,
        profile_name: str,
        catalog_lookup: dict[tuple[str, str, str], int],
    ) -> int:
        sql = (
            "SELECT source_table_full_name AS src_tbl, "
            "       source_column_name AS src_col, "
            "       target_table_full_name AS tgt_tbl, "
            "       target_column_name AS tgt_col, "
            "       event_time "
            "FROM system.access.column_lineage "
            f"WHERE event_time > now() - INTERVAL {_LINEAGE_WINDOW_DAYS} DAYS "
            "  AND source_table_full_name IS NOT NULL "
            "  AND target_table_full_name IS NOT NULL "
            "  AND source_column_name IS NOT NULL "
            "  AND target_column_name IS NOT NULL"
        )
        rows = self._run(sql)
        if not rows:
            return 0
        now = time.time()
        prepared: list[tuple[int, int, str, str, str, float, str]] = []
        for r in rows:
            src_id = self._resolve(str(r.get("src_tbl") or ""), catalog_lookup)
            tgt_id = self._resolve(str(r.get("tgt_tbl") or ""), catalog_lookup)
            if src_id is None or tgt_id is None:
                continue
            src_col = str(r.get("src_col") or "").strip()
            tgt_col = str(r.get("tgt_col") or "").strip()
            if not src_col or not tgt_col:
                continue
            if src_id == tgt_id and src_col.lower() == tgt_col.lower():
                continue
            details = {"event_time": _iso(r.get("event_time"))}
            prepared.append(
                (
                    src_id,
                    tgt_id,
                    src_col,
                    tgt_col,
                    json.dumps(details, sort_keys=True),
                    now,
                    "",
                )
            )
        if not prepared:
            return 0
        with self.conn:
            self.conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE relationship_type = ?
                  AND source = ?
                  AND from_entity_id IN (
                      SELECT id FROM catalog_entities WHERE db_profile = ?
                  )
                """,
                (REL_COLUMN, _SOURCE, profile_name),
            )
            self.conn.executemany(
                """
                INSERT INTO catalog_relationships (
                    from_entity_id, to_entity_id, relationship_type, score,
                    source, details_json, last_seen, audit_actor,
                    from_column, to_column
                ) VALUES (?, ?, ?, 1.0, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        src_id,
                        tgt_id,
                        REL_COLUMN,
                        _SOURCE,
                        details,
                        now,
                        actor,
                        src_col,
                        tgt_col,
                    )
                    for (src_id, tgt_id, src_col, tgt_col, details, now, actor) in prepared
                ],
            )
        return len(prepared)

    # ── usage backfill ───────────────────────────────────────────

    def _backfill_usage(self, profile_name: str) -> int:
        """Refresh ``last_used_at`` / ``last_user`` on existing edges.

        Pairs each ``system.query.history`` row (recent N days) with
        any asset_lineage_edges row whose query asset shares the
        statement id. Saves an extra SQL parse on the warehouse: the
        SQL-parse extractor already turned the saved-query SQL into
        edges; this pass just decorates them with usage timestamps.
        """
        sql = (
            "SELECT statement_id, executed_by, start_time "
            "FROM system.query.history "
            f"WHERE start_time > now() - INTERVAL {_USAGE_WINDOW_DAYS} DAYS "
            "  AND statement_id IS NOT NULL"
        )
        rows = self._run(sql)
        if not rows:
            return 0
        # Aggregate the most recent observation per statement_id so
        # one UPDATE per query is enough.
        latest: dict[str, tuple[float, str]] = {}
        for r in rows:
            sid = str(r.get("statement_id") or "").strip()
            if not sid:
                continue
            ts = _epoch_seconds(r.get("start_time"))
            user = str(r.get("executed_by") or "")
            prev = latest.get(sid)
            if prev is None or ts > prev[0]:
                latest[sid] = (ts, user)
        if not latest:
            return 0
        # Resolve statement_id -> remote_queries.id via external_id;
        # the asset ingest path stores Databricks statement ids there.
        sids = list(latest.keys())
        placeholders = ",".join("?" for _ in sids)
        rows_local = self.conn.execute(
            f"""
            SELECT id, external_id FROM remote_queries
            WHERE profile_name = ? AND external_id IN ({placeholders})
            """,  # noqa: S608 — placeholders bound below
            (profile_name, *sids),
        ).fetchall()
        updated = 0
        with self.conn:
            for qid, external_id in rows_local:
                ts, user = latest.get(str(external_id), (0.0, ""))
                if ts <= 0:
                    continue
                cur = self.conn.execute(
                    """
                    UPDATE asset_lineage_edges
                    SET last_used_at = ?, last_user = ?
                    WHERE profile_name = ?
                      AND from_kind = 'query'
                      AND from_id = ?
                    """,
                    (ts, user, profile_name, int(qid)),
                )
                updated += cur.rowcount or 0
        return updated

    # ── helpers ──────────────────────────────────────────────────

    def _run(self, sql: str) -> list[dict[str, Any]]:
        try:
            return list(self.query_runner(sql))
        except Exception as exc:  # noqa: BLE001
            log.info(
                "DatabricksSystemTablesExtractor: query failed (likely missing access): %s",
                exc,
            )
            return []


# ── value coercion ──────────────────────────────────────────────


def _iso(value: Any) -> str:
    """String-format a timestamp value for the JSON details payload."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return str(isoformat())
        except Exception:
            return ""
    return str(value)


def _epoch_seconds(value: Any) -> float:
    """Coerce a timestamp-ish value to UTC epoch seconds.

    Accepts ``datetime``, ``date``, ISO-8601 strings, and plain
    numbers. Anything else returns ``0.0`` so the caller treats it
    as "no signal" rather than failing the pass.
    """
    if value is None:
        return 0.0
    if isinstance(value, int | float):
        return float(value)
    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        try:
            return float(timestamp())
        except Exception:
            return 0.0
    if isinstance(value, str):
        try:
            from datetime import datetime

            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
    return 0.0


def build_query_runner_for_profile(profile_name: str) -> QueryRunner | None:
    """Resolve a profile to a live Databricks query runner.

    Returns ``None`` when the profile does not exist, when its
    backend is not ``databricks``, or when engine construction
    fails. The dispatcher in
    :meth:`amx.assets.lineage.LineageExtractor.extract_for_profile`
    treats ``None`` as "skip the system-tables pass for this
    profile", so misconfigured or non-Databricks profiles never
    block a refresh.
    """
    try:
        from amx.config import AMXConfig
    except Exception:  # noqa: BLE001
        return None
    try:
        cfg = AMXConfig.load()
    except Exception as exc:  # noqa: BLE001
        log.debug("system-tables: cfg load failed: %s", exc)
        return None
    db_cfg = cfg.db_profiles.get(profile_name)
    if db_cfg is None or (db_cfg.backend or "").lower() != "databricks":
        return None

    def runner(sql: str) -> list[dict[str, Any]]:
        from sqlalchemy import text

        from amx.db.connector import DatabaseConnector

        connector = DatabaseConnector(db_cfg)
        with connector.engine.connect() as bound:
            result = bound.execute(text(sql))
            cols = list(result.keys())
            return [dict(zip(cols, row, strict=False)) for row in result]

    return runner


__all__ = [
    "REL_TABLE",
    "REL_COLUMN",
    "DatabricksSystemTablesExtractor",
    "QueryRunner",
    "build_query_runner_for_profile",
]
