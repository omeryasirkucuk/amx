"""Join discovery for ``SearchCatalog``.

Methods that find joinable tables and join-column candidates from
the catalog. Two paths:

* Symbol-based (``joinable_tables`` / ``join_candidates``) — uses
  declared FK relationships + name-overlap heuristics.
* Semantic (``semantic_joinable_tables`` /
  ``semantic_join_candidates`` / ``_semantic_column_pair_score`` /
  ``_band_for_semantic_score``) — uses vector similarity over column
  descriptions to suggest joins between tables that don't have
  explicit FKs.
* ``_extract_join_pairs`` — shared helper that turns rows into the
  typed join-candidate structure used by /search and /ask.

Reads from ``self._connect()``; depends on
``EntityCrudMixin._entity_row`` for the underlying row shape.
"""

from __future__ import annotations

import math
from typing import Any

from amx.codebase.analyzer import CodeReference
from amx.search._catalog._constants import _json_loads
from amx.utils.logging import get_logger

log = get_logger("search.catalog.join")


class JoinMixin:
    """Join + joinable discovery methods for ``SearchCatalog``."""

    def _extract_join_pairs(
        self,
        refs: dict[str, list[CodeReference]],
    ) -> list[tuple[str, str, float, list[str]]]:
        file_map: dict[str, set[str]] = {}
        evidence: dict[tuple[str, str], list[str]] = {}
        for key, key_refs in refs.items():
            for ref in key_refs:
                tokens = file_map.setdefault(f"{ref.file}:{ref.line_no}", set())
                tokens.add(key.lower())
                evidence.setdefault((key.lower(), key.lower()), []).append(ref.line_text[:200])
        pairs: dict[tuple[str, str], list[str]] = {}
        for file_line, tokens in file_map.items():
            table_tokens = sorted(t for t in tokens if "." not in t)
            for idx, left in enumerate(table_tokens):
                for right in table_tokens[idx + 1 :]:
                    key = (left, right)
                    pairs.setdefault(key, []).append(file_line)
        out: list[tuple[str, str, float, list[str]]] = []
        for (left, right), lines in pairs.items():
            out.append((left, right, float(len(lines)), lines[:3]))
        return out

    def _semantic_column_pair_score(
        self, left: dict[str, Any], right: dict[str, Any]
    ) -> tuple[float, dict[str, Any]]:
        score = 0.0
        reasons: list[str] = []
        left_name = str(left.get("column_name") or "")
        right_name = str(right.get("column_name") or "")
        if not left_name or not right_name:
            return 0.0, {"reasons": []}
        similarity = self._similarity(left_name, right_name)
        if left_name.lower() == right_name.lower():
            score += 4.5
            reasons.append("exact column-name match")
        elif similarity >= 0.9:
            score += 4.0
            reasons.append("near-exact column-name match")
        elif similarity >= 0.72:
            score += similarity * 4.0
            reasons.append("fuzzy column-name similarity")
        left_family = self._dtype_family(str(left.get("dtype") or ""))
        right_family = self._dtype_family(str(right.get("dtype") or ""))
        if left_family == right_family:
            score += 2.0
            reasons.append(f"compatible dtype family ({left_family})")
        if (
            str(left.get("dtype") or "").lower() == str(right.get("dtype") or "").lower()
            and str(left.get("dtype") or "").strip()
        ):
            score += 0.75
        if int(left.get("nullable") or 0) == int(right.get("nullable") or 1):
            score += 0.25
        if int(left.get("pk_flag") or 0) or int(right.get("pk_flag") or 0):
            score += 0.75
            reasons.append("primary-key affinity")
        left_desc = self._description_tokens(str(left.get("effective_description") or ""))
        right_desc = self._description_tokens(str(right.get("effective_description") or ""))
        overlap = left_desc.intersection(right_desc)
        if overlap:
            score += min(4.0, 1.5 * len(overlap))
            reasons.append("description overlap: " + ", ".join(sorted(overlap)[:4]))
            if left_family == right_family:
                score += 1.0
                reasons.append("description overlap with compatible dtype")
        return score, {
            "reasons": reasons,
            "name_similarity": round(similarity, 4),
            "shared_tokens": sorted(overlap)[:8],
        }

    def _band_for_semantic_score(self, score: float) -> str:
        if score >= 10.0:
            return "verified"
        if score >= 7.5:
            return "high_likelihood"
        if score >= 4.0:
            return "possible"
        return "weak_hypothesis"

    def name_overlap_joinable_tables(
        self,
        db_profile: str,
        table_path: str,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        """Find joinable tables by shared column NAMES (no FK / no LLM).

        For schemas without declared foreign-key constraints (typical
        SAP / legacy schemas) and without per-column descriptions yet
        (catalog hasn't been ``/run`` yet), the cheapest signal that
        two tables might be joinable is "they share a column name".

        Common columns like ``mandt`` or ``id`` give a low-signal hit
        and are deweighted by an inverse-log rarity score: a column
        present in N tables contributes ``1 / log2(N+1)`` to the join
        weight. So a column shared with only 3 other tables (rare,
        high-signal — likely a real foreign key by convention) wins
        over ``mandt`` (shared by every table in the schema).

        Returns rows in the same shape as :meth:`joinable_tables`
        (with ``relationship_type='name_overlap'`` and
        ``source='name_overlap'``) so the tool-agent layer can dispatch
        them through the existing renderer.
        """
        if "." not in (table_path or ""):
            return []
        schema_name, table_name = table_path.split(".", 1)
        with self._connect() as conn:
            base = conn.execute(
                "SELECT id FROM catalog_entities WHERE db_profile=? "
                "AND entity_kind='table' AND LOWER(schema_name)=LOWER(?) "
                "AND LOWER(table_name)=LOWER(?) LIMIT 1",
                (db_profile, schema_name, table_name),
            ).fetchone()
            if not base:
                return []
            base_cols_rows = conn.execute(
                "SELECT column_name FROM catalog_entities WHERE db_profile=? "
                "AND entity_kind='column' AND LOWER(schema_name)=LOWER(?) "
                "AND LOWER(table_name)=LOWER(?)",
                (db_profile, schema_name, table_name),
            ).fetchall()
            base_cols = [str(r["column_name"]).lower() for r in base_cols_rows if r["column_name"]]
            if not base_cols:
                return []
            placeholders = ",".join("?" for _ in base_cols)
            # Rarity: how many distinct tables each base column name
            # appears in (across the active db_profile).
            rarity_rows = conn.execute(
                f"SELECT LOWER(column_name) AS col, "
                f"  COUNT(DISTINCT schema_name || '.' || table_name) AS n_tables "
                f"FROM catalog_entities "
                f"WHERE db_profile=? AND entity_kind='column' "
                f"  AND LOWER(column_name) IN ({placeholders}) "
                f"GROUP BY LOWER(column_name)",
                [db_profile, *base_cols],
            ).fetchall()
            rarity = {str(r["col"]): int(r["n_tables"]) for r in rarity_rows}
            # All other tables that share at least one of the base
            # column names. We pull (target_schema, target_table, col)
            # rows so the python side can group by candidate table and
            # sum rarity-weighted contributions.
            candidate_rows = conn.execute(
                f"SELECT schema_name, table_name, LOWER(column_name) AS col "
                f"FROM catalog_entities "
                f"WHERE db_profile=? AND entity_kind='column' "
                f"  AND LOWER(column_name) IN ({placeholders}) "
                f"  AND NOT (LOWER(schema_name)=LOWER(?) "
                f"           AND LOWER(table_name)=LOWER(?))",
                [db_profile, *base_cols, schema_name, table_name],
            ).fetchall()

        candidates: dict[tuple[str, str], dict[str, Any]] = {}
        for r in candidate_rows:
            target_schema = str(r["schema_name"] or "")
            target_table = str(r["table_name"] or "")
            col = str(r["col"] or "")
            if not target_schema or not target_table or not col:
                continue
            key = (target_schema, target_table)
            n_tables = rarity.get(col, 1)
            # 1 / log2(N+1) — N=1 gives 1.0, N=2 gives ~0.63, N=10
            # gives ~0.30, N=200 gives ~0.13. So mandt-everywhere
            # contributes very little; a column shared with only one
            # other table contributes ~1.0.
            weight = 1.0 / math.log2(n_tables + 1) if n_tables > 0 else 0.0
            slot = candidates.setdefault(
                key,
                {"shared_cols": [], "weight": 0.0},
            )
            if col not in slot["shared_cols"]:
                slot["shared_cols"].append(col)
                slot["weight"] += weight

        results: list[dict[str, Any]] = []
        for (target_schema, target_table), data in candidates.items():
            cols = data["shared_cols"]
            results.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": target_schema,
                    "target_table_name": target_table,
                    # Same column name on both sides — that's the join.
                    "left_column": ", ".join(cols[:5]),
                    "right_column": ", ".join(cols[:5]),
                    "relationship_type": "name_overlap",
                    "source": "name_overlap",
                    "score": round(float(data["weight"]), 3),
                    "shared_column_count": len(cols),
                }
            )
        results.sort(
            key=lambda r: (
                -float(r.get("score") or 0.0),
                -int(r.get("shared_column_count") or 0),
            )
        )
        return results[:limit]

    def joinable_tables(
        self, db_profile: str, table_path: str, limit: int = 8
    ) -> list[dict[str, Any]]:
        if "." not in (table_path or ""):
            return []
        schema_name, table_name = table_path.split(".", 1)
        with self._connect() as conn:
            base = conn.execute(
                """
                SELECT id, schema_name, table_name
                FROM catalog_entities
                WHERE db_profile = ? AND entity_kind = 'table'
                  AND LOWER(schema_name) = LOWER(?) AND LOWER(table_name) = LOWER(?)
                LIMIT 1
                """,
                (db_profile, schema_name, table_name),
            ).fetchone()
            if not base:
                return []
            rows = conn.execute(
                """
                SELECT
                    rel.relationship_type,
                    rel.score,
                    rel.source,
                    rel.details_json,
                    src.schema_name AS src_schema_name,
                    src.table_name AS src_table_name,
                    dst.schema_name AS dst_schema_name,
                    dst.table_name AS dst_table_name
                FROM catalog_relationships rel
                JOIN catalog_entities src ON src.id = rel.from_entity_id
                JOIN catalog_entities dst ON dst.id = rel.to_entity_id
                WHERE src.db_profile = ? AND dst.db_profile = ?
                  AND (rel.from_entity_id = ? OR rel.to_entity_id = ?)
                ORDER BY rel.score DESC, rel.last_seen DESC
                """,
                (db_profile, db_profile, int(base["id"]), int(base["id"])),
            ).fetchall()
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        base_path = f"{base['schema_name']}.{base['table_name']}"
        for rel in rows:
            details = _json_loads(rel["details_json"], {})
            src_path = f"{rel['src_schema_name']}.{rel['src_table_name']}"
            if src_path.lower() == base_path.lower():
                target_schema = str(rel["dst_schema_name"] or "")
                target_table = str(rel["dst_table_name"] or "")
                left_cols = list(
                    details.get("constrained_columns") or details.get("referred_columns") or []
                )
                right_cols = list(
                    details.get("referred_columns") or details.get("constrained_columns") or []
                )
            else:
                target_schema = str(rel["src_schema_name"] or "")
                target_table = str(rel["src_table_name"] or "")
                left_cols = list(
                    details.get("referred_columns") or details.get("constrained_columns") or []
                )
                right_cols = list(
                    details.get("constrained_columns") or details.get("referred_columns") or []
                )
            if not target_schema or not target_table:
                continue
            if (
                target_schema.lower() == schema_name.lower()
                and target_table.lower() == table_name.lower()
            ):
                continue
            join_left = ", ".join(str(item) for item in left_cols if str(item))
            join_right = ", ".join(str(item) for item in right_cols if str(item))
            key = (
                target_schema.lower(),
                target_table.lower(),
                str(rel["relationship_type"] or "").lower(),
                f"{join_left}|{join_right}",
            )
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": target_schema,
                    "target_table_name": target_table,
                    "left_column": join_left,
                    "right_column": join_right,
                    "relationship_type": str(rel["relationship_type"] or ""),
                    "source": str(rel["source"] or ""),
                    "score": float(rel["score"] or 0.0),
                }
            )
        results.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                item.get("target_schema_name", ""),
                item.get("target_table_name", ""),
            )
        )
        return results[:limit]

    def semantic_join_candidates(
        self, db_profile: str, left_path: str, right_path: str, limit: int = 8
    ) -> list[dict[str, Any]]:
        left_parts = left_path.split(".")
        right_parts = right_path.split(".")
        if len(left_parts) != 2 or len(right_parts) != 2:
            return []
        with self._connect() as conn:
            left_cols = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'column'
                """,
                (db_profile, left_parts[0], left_parts[1]),
            ).fetchall()
            right_cols = conn.execute(
                """
                SELECT ce.*, cd.description_text AS effective_description
                FROM catalog_entities ce
                LEFT JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
                WHERE ce.db_profile = ? AND ce.schema_name = ? AND ce.table_name = ? AND ce.entity_kind = 'column'
                """,
                (db_profile, right_parts[0], right_parts[1]),
            ).fetchall()
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for left in left_cols:
            for right in right_cols:
                left_name = str(left["column_name"] or "")
                right_name = str(right["column_name"] or "")
                if not left_name or not right_name:
                    continue
                key = (left_name.lower(), right_name.lower())
                if key in seen:
                    continue
                score, details = self._semantic_column_pair_score(dict(left), dict(right))
                if score < 4.0:
                    continue
                seen.add(key)
                results.append(
                    {
                        "left_column": left_name,
                        "right_column": right_name,
                        "relationship_type": "semantic_join_candidate",
                        "score": round(score, 3),
                        "source": "semantic",
                        "confidence_band": self._band_for_semantic_score(score),
                        "details": details,
                    }
                )
        results.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                item.get("left_column", ""),
                item.get("right_column", ""),
            )
        )
        return results[:limit]

    def semantic_joinable_tables(
        self, db_profile: str, table_path: str, limit: int = 8
    ) -> list[dict[str, Any]]:
        if "." not in (table_path or ""):
            return []
        schema_name, table_name = table_path.split(".", 1)
        with self._connect() as conn:
            base_table = conn.execute(
                """
                SELECT id FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'table'
                """,
                (db_profile, schema_name, table_name),
            ).fetchone()
            if not base_table:
                return []
            candidate_tables = conn.execute(
                """
                SELECT id, schema_name, table_name
                FROM catalog_entities
                WHERE db_profile = ? AND entity_kind = 'table'
                  AND NOT (LOWER(schema_name) = LOWER(?) AND LOWER(table_name) = LOWER(?))
                ORDER BY CASE WHEN LOWER(schema_name) = LOWER(?) THEN 0 ELSE 1 END, schema_name, table_name
                LIMIT 250
                """,
                (db_profile, schema_name, table_name, schema_name),
            ).fetchall()
        ranked: list[dict[str, Any]] = []
        for candidate in candidate_tables:
            candidate_path = f"{candidate['schema_name']}.{candidate['table_name']}"
            pairs = self.semantic_join_candidates(db_profile, table_path, candidate_path, limit=3)
            if not pairs:
                continue
            best = dict(pairs[0])
            best.update(
                {
                    "row_type": "joinable_table",
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "target_schema_name": str(candidate["schema_name"] or ""),
                    "target_table_name": str(candidate["table_name"] or ""),
                    "left_column": best.get("left_column", ""),
                    "right_column": best.get("right_column", ""),
                }
            )
            ranked.append(best)
        ranked.sort(
            key=lambda item: (
                {"verified": 0, "high_likelihood": 1, "possible": 2, "weak_hypothesis": 3}.get(
                    str(item.get("confidence_band") or ""), 4
                ),
                -float(item.get("score") or 0.0),
            )
        )
        return ranked[:limit]

    def join_candidates(
        self, db_profile: str, left_path: str, right_path: str, limit: int = 8
    ) -> list[dict[str, Any]]:
        left_parts = left_path.split(".")
        right_parts = right_path.split(".")
        if len(left_parts) != 2 or len(right_parts) != 2:
            raise ValueError("Use schema.table format for join candidates.")
        with self._connect() as conn:
            left = conn.execute(
                """
                SELECT * FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'table'
                """,
                (db_profile, left_parts[0], left_parts[1]),
            ).fetchone()
            right = conn.execute(
                """
                SELECT * FROM catalog_entities
                WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'table'
                """,
                (db_profile, right_parts[0], right_parts[1]),
            ).fetchone()
            if not left or not right:
                return []
            rels = conn.execute(
                """
                SELECT * FROM catalog_relationships
                WHERE (from_entity_id = ? AND to_entity_id = ?)
                   OR (from_entity_id = ? AND to_entity_id = ?)
                ORDER BY score DESC, last_seen DESC
                """,
                (int(left["id"]), int(right["id"]), int(right["id"]), int(left["id"])),
            ).fetchall()
            left_cols = conn.execute(
                "SELECT * FROM catalog_entities WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'column'",
                (db_profile, left_parts[0], left_parts[1]),
            ).fetchall()
            right_cols = conn.execute(
                "SELECT * FROM catalog_entities WHERE db_profile = ? AND schema_name = ? AND table_name = ? AND entity_kind = 'column'",
                (db_profile, right_parts[0], right_parts[1]),
            ).fetchall()
        results: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for rel in rels:
            details = _json_loads(rel["details_json"], {})
            constrained = details.get("constrained_columns") or details.get("source_columns") or []
            referred = details.get("referred_columns") or details.get("target_columns") or []
            if constrained and referred:
                for lcol, rcol in zip(constrained, referred, strict=False):
                    key = (str(lcol), str(rcol))
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(
                        {
                            "left_column": str(lcol),
                            "right_column": str(rcol),
                            "relationship_type": str(rel["relationship_type"]),
                            "score": float(rel["score"] or 0.0),
                            "source": str(rel["source"] or ""),
                            "details": details,
                        }
                    )
        right_by_name = {str(row["column_name"]).lower(): dict(row) for row in right_cols}
        for left_col in left_cols:
            name = str(left_col["column_name"] or "")
            right_col = right_by_name.get(name.lower())
            if not right_col or (name, name) in seen:
                continue
            score = 6.0
            if str(left_col["dtype"] or "") == str(right_col["dtype"] or ""):
                score += 1.5
            if left_col["pk_flag"] or right_col["pk_flag"]:
                score += 1.0
            results.append(
                {
                    "left_column": name,
                    "right_column": name,
                    "relationship_type": "same_name_candidate",
                    "score": score,
                    "source": "heuristic",
                    "details": {
                        "left_dtype": str(left_col["dtype"] or ""),
                        "right_dtype": str(right_col["dtype"] or ""),
                    },
                }
            )
        results.sort(key=lambda item: item["score"], reverse=True)
        return results[:limit]


__all__ = ["JoinMixin"]
