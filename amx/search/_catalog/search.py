"""Catalog search + find + ranking for ``SearchCatalog``.

The "read path" cluster — every method that returns rows for the
search agent / /ask flow:

* ``search_tables`` / ``search_columns`` — primary FTS5-backed
  ranking with mixed semantic + lexical scoring.
* ``find_tables_by_exact_name`` / ``find_columns_by_exact_name`` —
  case-insensitive exact-name lookup used by /metadata edit bulk.
* ``name_search_columns`` — exact + fuzzy column-name search.
* ``find_table_candidates`` — typo-recovery suggestions.
* ``schema_inventory`` / ``count_tables`` /
  ``known_databases`` / ``known_schemas`` — inventory queries used
  by deterministic answers.
* Helpers (``_exact_candidates``, ``_rank_rows``,
  ``_description_tokens``, ``_tokens``, ``_similarity``,
  ``_attach_column_counts``, ``_dtype_family``).

Read-only — no INSERTs except where the FTS5 index needs a refresh.
"""

from __future__ import annotations

import re
import sqlite3
from difflib import SequenceMatcher
from typing import Any

from amx.search._catalog._constants import (
    _active_embedding_kind,
    _vector_score_floor,
)
from amx.search._catalog._db_profile_clause import (
    DBProfileFilter,
    build_db_profile_clause,
    normalise_db_profile_filter,
)
from amx.utils.logging import get_logger

log = get_logger("search.catalog.search")


class SearchMixin:
    """Catalog search + find + ranking methods for ``SearchCatalog``."""

    def _tokens(self, text: str) -> list[str]:
        return [
            token for token in re.findall(r"\w+", text.lower(), flags=re.UNICODE) if len(token) >= 2
        ]

    def _similarity(self, left: str, right: str) -> float:
        a = (left or "").strip().lower()
        b = (right or "").strip().lower()
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    def _dtype_family(self, dtype: str) -> str:
        value = (dtype or "").strip().lower()
        if any(token in value for token in ("char", "text", "string", "uuid", "clob")):
            return "text"
        if any(
            token in value
            for token in ("int", "numeric", "decimal", "number", "float", "double", "real")
        ):
            return "number"
        if any(token in value for token in ("date", "time", "timestamp")):
            return "temporal"
        if any(token in value for token in ("bool", "bit")):
            return "boolean"
        return value or "unknown"

    def _description_tokens(self, text: str) -> set[str]:
        stop = {
            "this",
            "that",
            "column",
            "table",
            "field",
            "value",
            "used",
            "used_for",
            "indicates",
            "contains",
            "stores",
            "record",
            "with",
            "from",
            "into",
            "which",
            "when",
            "where",
            "the",
            "and",
            "for",
        }
        return {token for token in self._tokens(text) if token not in stop}

    def _exact_candidates(
        self, db_profile: DBProfileFilter, question: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        tokens = self._tokens(question)
        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE {clause} AND ce.search_text != ''
                """,
                binds,
            ).fetchall()
        hits: list[dict[str, Any]] = []
        for row in rows:
            search_text = str(row["search_text"] or "").lower()
            column_name = str(row["column_name"] or "").lower()
            score = 0.0
            for token in tokens:
                if token in search_text:
                    score += 1.0
                if token and token in column_name:
                    score += 1.5
                if token == column_name:
                    score += 2.0
                if token == str(row["table_name"] or "").lower():
                    score += 1.5
            if score <= 0:
                continue
            item = dict(row)
            item["match_score"] = score
            hits.append(item)
        hits.sort(key=lambda item: item["match_score"], reverse=True)
        return hits[:limit]

    def name_search_columns(
        self, db_profile: DBProfileFilter, question: str, limit: int = 8
    ) -> list[dict[str, Any]]:
        tokens = self._tokens(question)
        needle = (tokens[0] if tokens else question.strip().lower())[:128]
        if not needle:
            return []
        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE {clause} AND ce.entity_kind = 'column'
                """,
                binds,
            ).fetchall()
        ranked: list[dict[str, Any]] = []
        for row in rows:
            column_name = str(row["column_name"] or "")
            table_name = str(row["table_name"] or "")
            description = str(row["effective_description"] or "")
            col_lower = column_name.lower()
            table_lower = table_name.lower()
            score = 0.0
            if needle == col_lower:
                score += 12.0
            elif col_lower.startswith(needle):
                score += 9.0
            elif needle in col_lower:
                score += 7.0
            similarity = self._similarity(needle, col_lower)
            if similarity >= 0.72:
                score += similarity * 8.0
            if needle == table_lower:
                score += 4.0
            elif table_lower.startswith(needle):
                score += 2.5
            if needle and needle in description.lower():
                score += 1.0
            if score <= 0:
                continue
            item = dict(row)
            item["match_score"] = score
            ranked.append(item)
        # Settings are intrinsically per-profile; when the filter spans
        # several profiles use the first one (the scope's default) for
        # rank weights — the typical case is "homogeneous tuning across
        # profiles", a per-profile mismatch is rare and not worth a
        # separate code path here.
        scope_names = normalise_db_profile_filter(db_profile)
        settings_profile = scope_names[0] if scope_names else ""
        ranked = self._rank_rows(ranked, self.get_settings(settings_profile), limit * 2)
        return ranked[:limit]

    def find_table_candidates(
        self, db_profile: DBProfileFilter, hint: str, limit: int = 5
    ) -> list[dict[str, Any]]:
        needle = (hint or "").strip().lower()
        if not needle:
            return []
        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE {clause} AND ce.entity_kind = 'table'
                """,
                binds,
            ).fetchall()
        ranked: list[dict[str, Any]] = []
        for row in rows:
            table_name = str(row["table_name"] or "")
            search_text = str(row["search_text"] or "").lower()
            score = 0.0
            if needle == table_name.lower():
                score += 10.0
            elif table_name.lower().startswith(needle):
                score += 7.0
            elif needle in table_name.lower():
                score += 5.0
            similarity = self._similarity(needle, table_name)
            if similarity >= 0.72:
                score += similarity * 6.0
            if needle in search_text:
                score += 1.0
            if score <= 0:
                continue
            item = dict(row)
            item["rank_score"] = score
            ranked.append(item)
        ranked.sort(key=lambda item: float(item.get("rank_score") or 0.0), reverse=True)
        return ranked[:limit]

    def find_tables_by_exact_name(
        self,
        db_profile: DBProfileFilter,
        name: str,
        *,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Return every catalog table whose ``table_name`` matches ``name`` exactly.

        Used by ``/ask`` to disambiguate a bare token like ``vbrk`` across
        schemas: if the same name lives in multiple schemas we want to surface
        all of them rather than silently picking one. 0.11.0: ``db_profile``
        accepts a sequence of profile names so cross-DB ``/ask`` can find
        the same table name in several configured DBs at once.
        """
        needle = (name or "").strip().lower()
        if not needle:
            return []
        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE {clause}
                  AND ce.entity_kind = 'table'
                  AND LOWER(ce.table_name) = ?
                ORDER BY ce.db_profile, ce.schema_name, ce.table_name
                LIMIT ?
                """,
                [*binds, needle, int(limit)],
            ).fetchall()
        return [dict(row) for row in rows]

    def find_columns_by_exact_name(
        self,
        db_profile: DBProfileFilter,
        name: str,
        *,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return every catalog column whose ``column_name`` matches ``name`` exactly.

        Used by ``/metadata edit <bare_name>`` bulk-edit flow: surface every
        (schema, table, column) where the column appears so the user can
        multi-select and apply one comment to all of them. Limit defaults to
        200 because wide tables can have hundreds of columns named e.g.
        ``client`` or ``mandt`` in SAP-style schemas.
        """
        needle = (name or "").strip().lower()
        if not needle:
            return []
        clause, binds = build_db_profile_clause(db_profile, column="ce.db_profile")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE {clause}
                  AND ce.entity_kind = 'column'
                  AND LOWER(ce.column_name) = ?
                ORDER BY ce.db_profile, ce.schema_name, ce.table_name, ce.column_name
                LIMIT ?
                """,
                [*binds, needle, int(limit)],
            ).fetchall()
        return [dict(row) for row in rows]

    def known_databases(self, db_profile: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT database_name, COUNT(*) AS entity_count
                FROM catalog_entities
                WHERE db_profile = ? AND COALESCE(database_name, '') != ''
                GROUP BY database_name
                ORDER BY database_name
                """,
                (db_profile,),
            ).fetchall()
        return [dict(row) for row in rows]

    def known_schemas(
        self,
        db_profile: str,
        *,
        database_name: str | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [db_profile]
        where = ["db_profile = ?", "entity_kind = 'table'", "COALESCE(schema_name, '') != ''"]
        if database_name:
            where.append("LOWER(database_name) = LOWER(?)")
            params.append(database_name)
        query = f"""
            SELECT
                schema_name,
                MIN(database_name) AS database_name,
                COUNT(*) AS table_count
            FROM catalog_entities
            WHERE {" AND ".join(where)}
            GROUP BY schema_name
            ORDER BY schema_name
        """
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def count_tables(
        self,
        db_profile: str,
        *,
        schema_name: str | None = None,
        database_name: str | None = None,
    ) -> int:
        params: list[Any] = [db_profile]
        where = ["db_profile = ?", "entity_kind = 'table'"]
        if schema_name:
            where.append("LOWER(schema_name) = LOWER(?)")
            params.append(schema_name)
        if database_name:
            where.append("LOWER(database_name) = LOWER(?)")
            params.append(database_name)
        query = f"SELECT COUNT(*) AS cnt FROM catalog_entities WHERE {' AND '.join(where)}"
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
        return int((row["cnt"] if row else 0) or 0)

    def schema_inventory(
        self,
        db_profile: str,
        *,
        schema_name: str | None = None,
        database_name: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return table-level structural inventory with column counts."""
        params: list[Any] = [db_profile]
        where = ["t.db_profile = ?", "t.entity_kind = 'table'"]
        if schema_name:
            where.append("LOWER(t.schema_name) = LOWER(?)")
            params.append(schema_name)
        if database_name:
            where.append("LOWER(t.database_name) = LOWER(?)")
            params.append(database_name)
        query = f"""
            SELECT
                t.id,
                t.database_name,
                t.schema_name,
                t.table_name,
                t.asset_kind,
                t.row_count,
                td.description_text AS effective_description,
                COUNT(c.id) AS column_count,
                GROUP_CONCAT(cd.description_text, ' ') AS column_descriptions
            FROM catalog_entities t
            LEFT JOIN catalog_descriptions td ON td.id = t.effective_description_id
            LEFT JOIN catalog_entities c
              ON c.db_profile = t.db_profile
             AND c.schema_name = t.schema_name
             AND c.table_name = t.table_name
             AND c.entity_kind = 'column'
            LEFT JOIN catalog_descriptions cd ON cd.id = c.effective_description_id
            WHERE {" AND ".join(where)}
            GROUP BY t.id
            ORDER BY t.schema_name, t.table_name
            LIMIT ?
        """
        params.append(max(1, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def search_columns(
        self,
        db_profile: DBProfileFilter,
        question: str,
        limit: int = 8,
        entity_hints: list[str] | None = None,
        query_variants: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        # Settings are per-profile; for multi-profile retrieval use the
        # first profile's settings as a tuning baseline (typical case is
        # homogeneous tuning across the user's scope).
        scope_names = normalise_db_profile_filter(db_profile)
        settings_profile = scope_names[0] if scope_names else ""
        settings = self.get_settings(settings_profile)
        variants: list[str] = []
        for value in [question] + list(query_variants or []):
            text = str(value or "").strip()
            if text and text not in variants:
                variants.append(text)
        exact_hits: list[dict[str, Any]] = []
        seen_exact: dict[int, dict[str, Any]] = {}
        for variant in variants:
            for row in self._exact_candidates(db_profile, variant, limit=max(limit * 2, 10)):
                entity_id = int(row["id"])
                existing = seen_exact.get(entity_id)
                if existing is None:
                    seen_exact[entity_id] = row
                    continue
                existing = dict(existing)
                existing["match_score"] = float(existing.get("match_score") or 0.0) + float(
                    row.get("match_score") or 0.0
                )
                seen_exact[entity_id] = existing
        exact_hits = list(seen_exact.values())
        by_id: dict[int, dict[str, Any]] = {}
        for row in exact_hits:
            if row.get("entity_kind") != "column":
                continue
            by_id[int(row["id"])] = row
        if settings.get("enable_vector_search", "true").lower() == "true":
            for variant in variants:
                for hit in self.index.query(
                    variant, db_profile=db_profile, n_results=max(limit * 2, 10)
                ):
                    entity_id = int((hit.get("metadata") or {}).get("entity_id") or 0)
                    if not entity_id:
                        continue
                    row = by_id.get(entity_id)
                    if row is None:
                        with self._connect() as conn:
                            fetched = self._entity_row(conn, entity_id)
                        if not fetched or fetched["entity_kind"] != "column":
                            continue
                        row = dict(fetched)
                        row["match_score"] = 0.0
                        row["vector_only"] = True
                        by_id[entity_id] = row
                    dist = hit.get("distance")
                    if dist is not None:
                        row["match_score"] = float(row.get("match_score") or 0.0) + max(
                            0.0, 3.0 - float(dist)
                        )
        hints = [str(item).strip().lower() for item in (entity_hints or []) if str(item).strip()]
        rows = list(by_id.values())
        if hints:
            for row in rows:
                table_name = str(row.get("table_name") or "").lower()
                schema_name = str(row.get("schema_name") or "").lower()
                column_name = str(row.get("column_name") or "").lower()
                for hint in hints:
                    if hint in {table_name, column_name, f"{schema_name}.{table_name}"}:
                        row["match_score"] = float(row.get("match_score") or 0.0) + 2.5
        ranked = self._rank_rows(rows, settings, limit * 2)
        score_floor = _vector_score_floor(settings, _active_embedding_kind())
        ranked = [
            row
            for row in ranked
            if not row.get("vector_only") or float(row.get("match_score") or 0.0) >= score_floor
        ]
        return ranked[:limit]

    def search_tables(
        self,
        db_profile: DBProfileFilter,
        question: str,
        limit: int = 8,
        entity_hints: list[str] | None = None,
        query_variants: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        scope_names = normalise_db_profile_filter(db_profile)
        settings_profile = scope_names[0] if scope_names else ""
        settings = self.get_settings(settings_profile)
        variants: list[str] = []
        for value in [question] + list(query_variants or []):
            text = str(value or "").strip()
            if text and text not in variants:
                variants.append(text)
        exact_hits: dict[int, dict[str, Any]] = {}
        for variant in variants:
            for row in self._exact_candidates(db_profile, variant, limit=max(limit * 8, 40)):
                entity_id = int(row["id"])
                existing = exact_hits.get(entity_id)
                if existing is None:
                    exact_hits[entity_id] = dict(row)
                    continue
                merged = dict(existing)
                merged["match_score"] = float(merged.get("match_score") or 0.0) + float(
                    row.get("match_score") or 0.0
                )
                exact_hits[entity_id] = merged
        table_rows: dict[int, dict[str, Any]] = {}
        column_match_counts: dict[int, int] = {}
        with self._connect() as conn:
            for row in exact_hits.values():
                if row.get("entity_kind") == "table":
                    table_row = dict(row)
                    table_row["row_type"] = "table"
                    table_row.setdefault("matched_columns", [])
                    table_rows[int(row["id"])] = table_row
                    continue
                if row.get("entity_kind") != "column":
                    continue
                # The parent table lives in the SAME profile as the column hit
                # — never the full scope. Filter by ``row['db_profile']`` so a
                # multi-profile retrieval still maps each column to its own
                # table without cross-pollination across profiles.
                table = conn.execute(
                    """
                    SELECT ce.*, cd.description_text AS effective_description
                    FROM catalog_entities ce
                    LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                    WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'table'
                    LIMIT 1
                    """,
                    (
                        str(row.get("db_profile") or settings_profile),
                        str(row["schema_name"] or ""),
                        str(row["table_name"] or ""),
                    ),
                ).fetchone()
                if not table:
                    continue
                table_id = int(table["id"])
                table_row = table_rows.get(table_id) or dict(table)
                table_row["row_type"] = "table"
                table_row["match_score"] = (
                    float(table_row.get("match_score") or 0.0)
                    + float(row.get("match_score") or 0.0)
                    + 0.75
                )
                matched_columns = list(table_row.get("matched_columns") or [])
                column_name = str(row.get("column_name") or "")
                if column_name and column_name not in matched_columns:
                    matched_columns.append(column_name)
                table_row["matched_columns"] = matched_columns
                table_rows[table_id] = table_row
                column_match_counts[table_id] = column_match_counts.get(table_id, 0) + 1
            if settings.get("enable_vector_search", "true").lower() == "true":
                for variant in variants:
                    for hit in self.index.query(
                        variant, db_profile=db_profile, n_results=max(limit * 4, 20)
                    ):
                        metadata = hit.get("metadata") or {}
                        entity_id = int(metadata.get("entity_id") or 0)
                        if not entity_id:
                            continue
                        entity = self._entity_row(conn, entity_id)
                        if not entity:
                            continue
                        table: sqlite3.Row | None = None
                        if entity["entity_kind"] == "table":
                            table = entity
                        elif entity["entity_kind"] == "column":
                            # Same per-row scoping rule as above: the parent
                            # table is in the column's own profile.
                            table = conn.execute(
                                """
                                SELECT ce.*, cd.description_text AS effective_description
                                FROM catalog_entities ce
                                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'table'
                                LIMIT 1
                                """,
                                (
                                    str(entity["db_profile"] or settings_profile),
                                    str(entity["schema_name"] or ""),
                                    str(entity["table_name"] or ""),
                                ),
                            ).fetchone()
                        if not table:
                            continue
                        table_id = int(table["id"])
                        table_row = table_rows.get(table_id) or dict(table)
                        table_row["row_type"] = "table"
                        table_row.setdefault("matched_columns", [])
                        distance = hit.get("distance")
                        if distance is not None:
                            table_row["match_score"] = float(
                                table_row.get("match_score") or 0.0
                            ) + max(0.0, 2.0 - float(distance))
                        table_rows[table_id] = table_row
        hints = [str(item).strip().lower() for item in (entity_hints or []) if str(item).strip()]
        rows = list(table_rows.values())
        for row in rows:
            table_id = int(row["id"])
            match_count = int(column_match_counts.get(table_id, 0))
            if match_count > 1:
                row["match_score"] = float(row.get("match_score") or 0.0) + min(
                    3.0, 0.8 * match_count
                )
            table_name = str(row.get("table_name") or "").lower()
            schema_name = str(row.get("schema_name") or "").lower()
            for hint in hints:
                if hint in {table_name, schema_name, f"{schema_name}.{table_name}"}:
                    row["match_score"] = float(row.get("match_score") or 0.0) + 2.5
        # Enrich rows with column_count via a single batched lookup. The renderer
        # surfaces this as the `Cols` column; rank_score does not depend on it,
        # so we run this after scoring to avoid touching the ranking math.
        self._attach_column_counts(db_profile, rows)
        ranked = self._rank_rows(rows, settings, limit * 3)
        return ranked[:limit]

    def _attach_column_counts(
        self, db_profile: DBProfileFilter, rows: list[dict[str, Any]]
    ) -> None:
        targets: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            schema = str(row.get("schema_name") or "")
            table = str(row.get("table_name") or "")
            if not schema or not table:
                continue
            key = (schema, table)
            if key in seen:
                continue
            seen.add(key)
            targets.append(key)
        if not targets:
            return
        placeholders = ",".join(["(?, ?)"] * len(targets))
        clause, profile_binds = build_db_profile_clause(db_profile)
        params: list[Any] = [*profile_binds]
        for schema, table in targets:
            params.append(schema)
            params.append(table)
        sql = (
            "SELECT schema_name, table_name, COUNT(*) AS column_count "
            "FROM catalog_entities "
            f"WHERE {clause} AND entity_kind = 'column' AND (schema_name, table_name) IN ({placeholders}) "
            "GROUP BY schema_name, table_name"
        )
        counts: dict[tuple[str, str], int] = {}
        with self._connect() as conn:
            for r in conn.execute(sql, tuple(params)).fetchall():
                counts[(str(r["schema_name"] or ""), str(r["table_name"] or ""))] = int(
                    r["column_count"] or 0
                )
        for row in rows:
            key = (str(row.get("schema_name") or ""), str(row.get("table_name") or ""))
            if key in counts:
                row["column_count"] = counts[key]

    def _rank_rows(
        self, rows: list[dict[str, Any]], settings: dict[str, str], limit: int
    ) -> list[dict[str, Any]]:
        weight_map = {
            "manual": float(settings.get("manual_weight", "6.0")),
            "reviewed": float(settings.get("reviewed_weight", "4.5")),
            "generated": float(settings.get("generated_weight", "3.0")),
            "imported": 2.0,
            "rejected": 0.0,
        }
        scored: list[dict[str, Any]] = []
        for row in rows:
            total = float(row.get("match_score") or 0.0)
            total += weight_map.get(str(row.get("effective_source_kind") or ""), 0.0)
            confidence = str(row.get("current_confidence") or "").lower()
            if confidence == "high":
                total += 1.0
            elif confidence == "medium":
                total += 0.5
            row = dict(row)
            row["rank_score"] = total
            row["evidence_score"] = float(row.get("match_score") or 0.0)
            row.setdefault("evidence_tier", "strong" if total >= 4.5 else "weak")
            row.setdefault("answer_role", "supporting")
            row.setdefault("match_reason", "ranked_match")
            scored.append(row)
        scored.sort(key=lambda item: item["rank_score"], reverse=True)
        return scored[:limit]


__all__ = ["SearchMixin"]
