"""Weighted-sum candidate ranker for the lineage LLM extractor.

The LLM only suggests edges among the candidates we hand it. On
SAP-style schemas with thousands of tables and aggressive name
prefixing (``ADRC``, ``ADR2``, ``ADR6``, …) the old name-alphabetical
top-30 fills the prompt with siblings that share four letters but
zero semantics. The LLM then "helpfully" suggests sibling-to-sibling
edges that do not exist in the data flow.

This module scores every candidate table against multiple signals
already in AMX's catalog and returns the top-N by combined score.
Weights are tuned for "deterministic evidence dominates; soft signals
break ties":

  +0.80  any deterministic FK between anchor and candidate
  +0.60  candidate appears in a view definition that joins anchor
  +0.50 * log(1 + co_occur_count)  query-log co-occurrence
  +0.30  shares >= 2 column names with the anchor
  +0.10  shares anchor's name prefix (old SAP heuristic)
  +0.05 * matching column-name token count

The function emits :class:`RankedCandidate` carrying a human-readable
``reasons`` list — the prompt includes it so the LLM sees not just
the candidate but *why we picked it*, which keeps suggestions tied
to grounded evidence.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any

from amx.lineage.types import Scope


@dataclass
class RankedCandidate:
    """One scored candidate table."""

    schema: str
    table: str
    fqn: str  # 'schema.table'
    score: float
    reasons: list[str] = field(default_factory=list)


# ── Tunable weights (kept module-level so tests can monkey-patch). ──
W_FK = 0.80
W_VIEW = 0.60
W_CO_OCCUR = 0.50  # multiplied by log(1+count)
W_COL_NAME_SET = 0.30  # shared-column threshold
W_PREFIX = 0.10
W_COL_TOKEN = 0.05  # per matching column-name token

_COL_NAME_OVERLAP_THRESHOLD = 2


def score_candidates(
    hs: Any,
    scope: Scope,
    *,
    max_count: int = 30,
) -> list[RankedCandidate]:
    """Rank every candidate table in the anchor's database.

    Pipeline:
      1. Read all (schema, table) pairs in scope's database except the
         anchor itself.
      2. Pre-fetch global signals (FK partners, view co-mentions,
         query-log co-occurrence, anchor column names) so we score
         in memory rather than N+1 round-tripping.
      3. Compute the weighted-sum score per candidate.
      4. Return top ``max_count`` by descending score (alphabetical
         tie-break for determinism).
    """
    profile = scope.profile
    database = scope.anchor.database
    anchor_schema = scope.anchor.schema
    anchor_table = scope.anchor.table

    candidates = _list_candidate_tables(hs, profile, database, anchor_schema, anchor_table)
    if not candidates:
        return []

    fk_partners = _fk_partner_fqns(hs, profile, database, anchor_schema, anchor_table)
    view_partners = _view_co_mention_fqns(hs, profile, database, anchor_table)
    co_occur = _co_occur_counts(hs, profile, anchor_schema, anchor_table)
    anchor_columns = _column_names(hs, profile, database, anchor_schema, anchor_table)
    anchor_tokens = _column_tokens(anchor_columns)
    prefix = _name_prefix(anchor_table)

    ranked: list[RankedCandidate] = []
    for schema, table in candidates:
        fqn = f"{schema}.{table}"
        cand_columns = _column_names(hs, profile, database, schema, table)
        cand_tokens = _column_tokens(cand_columns)
        col_overlap = anchor_columns & cand_columns
        token_overlap = anchor_tokens & cand_tokens

        score = 0.0
        reasons: list[str] = []

        if fqn in fk_partners:
            score += W_FK
            reasons.append("FK")
        if fqn in view_partners:
            score += W_VIEW
            views = ", ".join(sorted(view_partners[fqn])[:2])
            reasons.append(f"view:{views}")
        co_count = co_occur.get(fqn, 0)
        if co_count:
            bump = W_CO_OCCUR * math.log(1 + co_count)
            score += bump
            reasons.append(f"co-query×{co_count}")
        if len(col_overlap) >= _COL_NAME_OVERLAP_THRESHOLD:
            score += W_COL_NAME_SET
            reasons.append(f"shared-cols×{len(col_overlap)}")
        if prefix and table.upper().startswith(prefix):
            score += W_PREFIX
            reasons.append("prefix")
        if token_overlap:
            score += W_COL_TOKEN * len(token_overlap)
            reasons.append(f"name-tokens×{len(token_overlap)}")

        ranked.append(
            RankedCandidate(
                schema=schema,
                table=table,
                fqn=fqn,
                score=round(score, 3),
                reasons=reasons,
            )
        )

    ranked.sort(key=lambda c: (-c.score, c.schema, c.table))
    return ranked[:max_count]


# ── Helpers ──────────────────────────────────────────────────────────────


def _list_candidate_tables(
    hs: Any, profile: str, database: str, anchor_schema: str, anchor_table: str
) -> list[tuple[str, str]]:
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT schema_name, table_name
            FROM catalog_entities
            WHERE db_profile = ? AND database_name = ?
              AND entity_kind = 'table'
              AND NOT (schema_name = ? AND table_name = ?)
            """,
            (profile, database, anchor_schema, anchor_table),
        ).fetchall()
    return [(str(r[0]), str(r[1])) for r in rows]


def _fk_partner_fqns(
    hs: Any, profile: str, database: str, anchor_schema: str, anchor_table: str
) -> set[str]:
    """Set of ``schema.table`` FQNs the anchor has a known FK to/from."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT
                src.schema_name AS src_schema, src.table_name AS src_table,
                tgt.schema_name AS tgt_schema, tgt.table_name AS tgt_table
            FROM catalog_relationships cr
            JOIN catalog_entities src ON src.id = cr.from_entity_id
            JOIN catalog_entities tgt ON tgt.id = cr.to_entity_id
            WHERE cr.relationship_type IN ('lineage_fk', 'fk')
              AND src.db_profile = ?
              AND src.database_name = ?
              AND (
                (src.schema_name = ? AND src.table_name = ?) OR
                (tgt.schema_name = ? AND tgt.table_name = ?)
              )
            LIMIT 500
            """,
            (
                profile,
                database,
                anchor_schema,
                anchor_table,
                anchor_schema,
                anchor_table,
            ),
        ).fetchall()
    out: set[str] = set()
    anchor_fqn = f"{anchor_schema}.{anchor_table}"
    for r in rows:
        src_fqn = f"{r[0]}.{r[1]}"
        tgt_fqn = f"{r[2]}.{r[3]}"
        if src_fqn == anchor_fqn:
            out.add(tgt_fqn)
        if tgt_fqn == anchor_fqn:
            out.add(src_fqn)
    out.discard(anchor_fqn)
    return out


def _view_co_mention_fqns(
    hs: Any, profile: str, database: str, anchor_table: str
) -> dict[str, set[str]]:
    """Map ``schema.partner_table`` → set of view names that mention both.

    Treats the anchor as matched purely by table name to keep this
    cheap; we expect distinct tables across schemas to be rare in
    practice and the LLM still sees view + table together so any
    false positive is recoverable.
    """
    anchor_lower = anchor_table.lower()
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT view_name, schema_name, parsed_lineage_json
            FROM view_definitions_cache
            WHERE db_profile = ? AND database_name = ?
              AND parse_status = 'ok'
            LIMIT 500
            """,
            (profile, database),
        ).fetchall()
    out: dict[str, set[str]] = {}
    for view_name, view_schema, parsed_json in rows:
        if not parsed_json:
            continue
        try:
            parsed = json.loads(parsed_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, list):
            continue
        mentions: set[str] = set()
        anchor_seen = False
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            for src in entry.get("sources") or []:
                if not isinstance(src, dict):
                    continue
                table_name = str(src.get("table") or "")
                if not table_name:
                    continue
                if table_name.lower() == anchor_lower:
                    anchor_seen = True
                else:
                    mentions.add(table_name)
        if not anchor_seen:
            continue
        for partner_table in mentions:
            partner_fqn = f"{view_schema}.{partner_table}" if view_schema else partner_table
            out.setdefault(partner_fqn, set()).add(str(view_name))
    return out


def _co_occur_counts(
    hs: Any, profile: str, anchor_schema: str, anchor_table: str
) -> dict[str, int]:
    """Reuse the rich-context co-occurrence reader without circular import."""
    from amx.lineage._anchor_context import _read_co_occurrence
    from amx.lineage.types import ColumnRef
    from amx.lineage.types import Scope as _Scope

    pseudo_scope = _Scope(
        profile=profile,
        anchor=ColumnRef(database="", schema=anchor_schema, table=anchor_table, column=""),
        depth_up=1,
        depth_down=1,
        database="",
        schema=anchor_schema,
    )
    partners = _read_co_occurrence(hs, profile, "", anchor_schema, anchor_table)
    # _Scope is used purely so we share the same helper; the value
    # itself isn't needed for the read function's logic.
    del pseudo_scope
    return {p.other_fqn: p.count for p in partners}


def _column_names(hs: Any, profile: str, database: str, schema: str, table: str) -> set[str]:
    """Lowercase column-name set from the cached columns_json."""
    cache_key = f"{profile}|{database}|{schema}|{table}"
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT columns_json FROM column_comments_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
    if not row or not row[0]:
        return set()
    try:
        payload = json.loads(row[0])
    except (TypeError, ValueError):
        return set()
    if isinstance(payload, dict):
        return {str(k).lower() for k in payload}
    return set()


def _column_tokens(columns: set[str]) -> set[str]:
    """Split column names on underscore — ``customer_id`` → ``{customer, id}``.

    The `_id` token is too generic to count (every key has one), so we
    drop it. Likewise short fragments < 3 chars.
    """
    out: set[str] = set()
    for col in columns:
        for token in col.split("_"):
            if len(token) >= 3 and token != "id":
                out.add(token)
    return out


def _name_prefix(name: str) -> str:
    """First 3 alpha chars uppercased — heuristic for SAP-style naming."""
    head = "".join(ch for ch in (name or "")[:6] if ch.isalpha())
    return head[:3].upper()


__all__ = ["RankedCandidate", "score_candidates"]
