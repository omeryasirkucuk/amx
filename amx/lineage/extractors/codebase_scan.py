"""Mine the indexed code RAG for SQL referencing the anchor table.

Pulls evidence from ``amx.codebase.code_rag`` — semantic search for the
anchor's schema.table and bare table, then sqlglot-parses each hit's
chunk to find sibling table references in the same SQL statement. Edges
land in ``catalog_relationships`` as ``lineage_codebase`` rows with
``source='codebase'`` and ``details_json`` carrying the file path +
line number so the Studio EdgePanel can surface "found in
``services/orders.py:42``".

Cache-only by construction: the Chroma collection lives on local disk
and the parser runs in-process. No DB round-trips, no LLM call.
Returns an empty result when the user has not yet indexed any code.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from amx.lineage.types import (
    ColumnRef,
    Edge,
    ExtractMode,
    ExtractResult,
    Scope,
)

_RESULTS_PER_QUERY = 12
_QUERY_TIMEOUT_SEC = 2.5

_CONFIDENCE_PARSED = 0.7
_CONFIDENCE_SUBSTRING = 0.5
_MAX_EDGES_PER_ANCHOR = 20


class CodebaseScanExtractor:
    name = "codebase"

    def extract(
        self,
        *,
        hs: Any,
        scope: Scope,
        mode: ExtractMode = "cache_only",
    ) -> ExtractResult:
        anchor = scope.anchor
        if not anchor.schema or not anchor.table:
            return ExtractResult()

        if mode == "cache_only":
            cached = _read_cached_edges(hs, scope)
            if cached:
                return ExtractResult(edges=cached, cache_status="hit")

        # We treat the Chroma scan as cache-only too — it runs against
        # local disk. So even in default cache_only mode we fall through
        # and perform a fresh scan when no rows are cached yet.

        if not _code_collection_ready():
            return ExtractResult(edges=[], cache_status="hit")

        edges = list(_scan_codebase(hs, scope))
        return ExtractResult(edges=edges, cache_status="hit")


def _code_collection_ready() -> bool:
    try:
        from amx.codebase.code_rag import code_collection_count
    except Exception:
        return False
    try:
        return code_collection_count() > 0
    except Exception:
        return False


def _scan_codebase(hs: Any, scope: Scope) -> list[Edge]:
    try:
        from amx.codebase.code_rag import query_code_snippets
    except Exception:
        return []

    schema = scope.anchor.schema
    table = scope.anchor.table
    queries = [f"{schema}.{table}", table]

    hits: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for q in queries:
        try:
            results = query_code_snippets(
                q, n_results=_RESULTS_PER_QUERY, timeout=_QUERY_TIMEOUT_SEC
            )
        except Exception:
            continue
        for hit in results:
            meta = hit.get("metadata") or {}
            doc_id = f"{meta.get('rel_path', '')}:{meta.get('start_line', '')}"
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            hits.append(hit)

    if not hits:
        return []

    sqlglot = _load_sqlglot()
    candidates = _candidate_tables(hs, scope)
    edges_by_key: dict[tuple[str, str, str], Edge] = {}

    for hit in hits:
        text = str(hit.get("text") or "")
        meta = hit.get("metadata") or {}
        rel_path = str(meta.get("rel_path") or "")
        start_line = meta.get("start_line")
        evidence = f"{rel_path}:{start_line}" if start_line is not None else rel_path or "(unknown)"

        parsed_tables = _parse_sql_tables(sqlglot, text) if sqlglot else set()
        substring_tables = _substring_table_hits(text, candidates)

        for other in parsed_tables | substring_tables:
            if other == table.lower():
                continue
            if other not in candidates:
                continue
            confidence = _CONFIDENCE_PARSED if other in parsed_tables else _CONFIDENCE_SUBSTRING
            other_table = candidates[other]
            edge_key = (other, schema, table)
            existing = edges_by_key.get(edge_key)
            if existing is None or existing.confidence < confidence:
                edges_by_key[edge_key] = Edge(
                    source=ColumnRef(
                        database=scope.anchor.database,
                        schema=other_table["schema"],
                        table=other_table["name"],
                        column="",
                    ),
                    target=scope.anchor,
                    relationship_type="lineage_codebase",
                    extractor="codebase",
                    confidence=confidence,
                    evidence=evidence[:160],
                )

    edges = list(edges_by_key.values())
    edges.sort(key=lambda e: (-e.confidence, e.source.table))
    edges = edges[:_MAX_EDGES_PER_ANCHOR]

    _persist_edges(hs, scope, edges)
    return edges


def _candidate_tables(hs: Any, scope: Scope) -> dict[str, dict[str, str]]:
    """Lowercase-name → {schema, name} map of tables in the same database.

    Restricts hits to tables AMX already knows about so we don't emit
    edges to things that have never been catalogued (those would render
    as dangling nodes).
    """
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name = ?
              AND entity_kind = 'table'
              AND table_name <> ''
            """,
            (scope.profile, scope.anchor.database),
        ).fetchall()
    out: dict[str, dict[str, str]] = {}
    for schema_name, table_name in rows:
        key = str(table_name or "").lower()
        if not key:
            continue
        # Prefer the anchor's own schema when a table name appears in
        # multiple schemas to avoid cross-schema noise.
        if key in out and out[key]["schema"] == scope.anchor.schema:
            continue
        out[key] = {"schema": str(schema_name or ""), "name": str(table_name or "")}
    return out


def _parse_sql_tables(sqlglot: Any, text: str) -> set[str]:
    """Return the set of bare-table names referenced anywhere in ``text``.

    The chunk is rarely pure SQL — it's usually a Python / TS function
    with an embedded f-string or query template — so we extract any
    sequence that looks SQL-ish (contains FROM / JOIN / INSERT / UPDATE
    / DELETE) and feed each candidate to sqlglot. Failures are silent.
    """
    out: set[str] = set()
    if not text:
        return out
    statements = _extract_sql_statements(text)
    if not statements:
        return out
    exp = sqlglot.exp
    for sql in statements:
        try:
            tree = sqlglot.parse_one(sql, read=None)
        except Exception:
            continue
        if tree is None:
            continue
        for table in tree.find_all(exp.Table):
            name = str(table.name or "").strip()
            if name:
                out.add(name.lower())
    return out


def _extract_sql_statements(text: str) -> list[str]:
    """Cheap regex-based slicer that returns SQL-looking chunks.

    We pluck triple-quoted blocks, fenced code blocks, and lines that
    open with a SQL keyword so the parser sees the right unit. The
    parse fails silently if our slicer was too greedy.
    """
    candidates: list[str] = []
    triple = re.findall(r'"""(.*?)"""', text, flags=re.DOTALL)
    triple += re.findall(r"'''(.*?)'''", text, flags=re.DOTALL)
    candidates.extend(triple)
    fenced = re.findall(r"```(?:sql|SQL)?\n(.*?)```", text, flags=re.DOTALL)
    candidates.extend(fenced)
    inline = re.findall(
        r"(?im)((?:SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|WITH)\s.+?;)",
        text,
        flags=re.DOTALL,
    )
    candidates.extend(inline)
    return [c for c in (s.strip() for s in candidates) if c]


def _substring_table_hits(text: str, candidates: dict[str, dict[str, str]]) -> set[str]:
    """Fallback hit set: case-insensitive whole-word match on table names."""
    out: set[str] = set()
    if not text:
        return out
    lower = text.lower()
    for key in candidates:
        if len(key) < 3:
            # Two-letter table names produce constant false positives
            # ("id" / "as" / "of") — drop them from substring fallback.
            continue
        if re.search(rf"\b{re.escape(key)}\b", lower):
            out.add(key)
    return out


def _load_sqlglot() -> Any | None:
    try:
        import sqlglot

        return sqlglot
    except ImportError:
        return None


def _persist_edges(hs: Any, scope: Scope, edges: list[Edge]) -> None:
    """Upsert codebase edges into ``catalog_relationships``."""
    if not edges:
        return
    anchor_id = _resolve_anchor_table_id(hs, scope)
    if anchor_id is None:
        return
    now = time.time()
    with hs._lock, hs._connect() as conn:
        for edge in edges:
            other_id = _resolve_table_id(
                conn,
                profile=scope.profile,
                database=scope.anchor.database,
                schema=edge.source.schema,
                table=edge.source.table,
            )
            if other_id is None:
                continue
            details = {"evidence": edge.evidence, "ts": now}
            conn.execute(
                """
                DELETE FROM catalog_relationships
                WHERE from_entity_id = ? AND to_entity_id = ?
                  AND relationship_type = 'lineage_codebase'
                """,
                (other_id, anchor_id),
            )
            conn.execute(
                """
                INSERT INTO catalog_relationships
                    (from_entity_id, to_entity_id, relationship_type, score,
                     source, details_json, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_id,
                    anchor_id,
                    "lineage_codebase",
                    float(edge.confidence),
                    "codebase",
                    json.dumps(details, ensure_ascii=True),
                    now,
                ),
            )


def _read_cached_edges(hs: Any, scope: Scope) -> list[Edge]:
    anchor_id = _resolve_anchor_table_id(hs, scope)
    if anchor_id is None:
        return []
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT cr.from_entity_id, cr.score, cr.details_json,
                   src.schema_name, src.table_name
            FROM catalog_relationships cr
            JOIN catalog_entities src ON src.id = cr.from_entity_id
            WHERE cr.relationship_type = 'lineage_codebase'
              AND cr.to_entity_id = ?
            """,
            (anchor_id,),
        ).fetchall()
    edges: list[Edge] = []
    for row in rows:
        details = _safe_json(row[2])
        evidence = (details.get("evidence") if isinstance(details, dict) else "") or ""
        edges.append(
            Edge(
                source=ColumnRef(
                    database=scope.anchor.database,
                    schema=str(row[3]),
                    table=str(row[4]),
                    column="",
                ),
                target=scope.anchor,
                relationship_type="lineage_codebase",
                extractor="codebase",
                confidence=float(row[1] or 0.0),
                evidence=str(evidence)[:160],
            )
        )
    return edges


def _resolve_anchor_table_id(hs: Any, scope: Scope) -> int | None:
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ? AND entity_kind = 'table'
            LIMIT 1
            """,
            (
                scope.profile,
                scope.anchor.database,
                scope.anchor.schema,
                scope.anchor.table,
            ),
        ).fetchone()
    return int(row[0]) if row else None


def _resolve_table_id(
    conn: Any, *, profile: str, database: str, schema: str, table: str
) -> int | None:
    row = conn.execute(
        """
        SELECT id FROM catalog_entities
        WHERE db_profile = ? AND database_name = ? AND schema_name = ?
          AND table_name = ? AND entity_kind = 'table'
        LIMIT 1
        """,
        (profile, database, schema, table),
    ).fetchone()
    return int(row[0]) if row else None


def _safe_json(raw: Any) -> Any:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


__all__ = ["CodebaseScanExtractor"]
