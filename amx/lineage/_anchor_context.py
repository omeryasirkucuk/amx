"""Rich anchor-context assembly for the lineage LLM extractor.

The LLM only ever sees what we paste into the prompt — the difference
between "table X has columns id, customer_id, total" and the same
table with its description, its FK partners, the views that reference
it, and the queries that join it elsewhere is the difference between
a noisy guess and a grounded suggestion.

This module gathers every signal already cached in the AMX history
store and returns a typed dataclass the prompt builder formats. New
signals slot in here without touching the extractor or the prompt.

Every section is hard-capped so prompt size stays bounded:

* Up to 60 anchor columns (already truncated upstream typically).
* Up to 5 FK partners with column pairs.
* Up to 5 views, each with up to 5 referenced tables.
* Up to 5 co-occurrence partners.

Column descriptions are truncated to 120 chars each.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from typing import Any

from amx.lineage.types import Scope

_MAX_FK_PARTNERS = 5
_MAX_VIEW_REFS = 5
_MAX_TABLES_PER_VIEW = 5
_MAX_CO_OCCUR = 5
_MAX_COLUMN_DESC_CHARS = 120


@dataclass(frozen=True)
class FKPartner:
    """One foreign-key relationship from / to the anchor."""

    direction: str  # 'outbound' (anchor → other) | 'inbound' (other → anchor)
    other_fqn: str
    from_column: str
    to_column: str


@dataclass(frozen=True)
class ViewReference:
    """A view whose definition mentions the anchor table.

    ``other_tables`` is the set of tables the view also joins / unions
    with — that is the signal the LLM cares about (these are
    almost-certainly lineage partners).
    """

    view_fqn: str
    other_tables: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CoOccurrencePartner:
    """A table that has appeared alongside the anchor in the query log."""

    other_fqn: str
    count: int


@dataclass(frozen=True)
class RichAnchorContext:
    """Everything we want the prompt to know about the anchor."""

    fqn: str
    table_description: str
    columns: list[dict[str, str]]
    fk_partners: list[FKPartner]
    view_references: list[ViewReference]
    co_occurrence_partners: list[CoOccurrencePartner]


# ── Public API ────────────────────────────────────────────────────────────


def build_rich_context(hs: Any, scope: Scope) -> RichAnchorContext:
    """Assemble the full anchor context from cached AMX signals.

    Every section degrades gracefully — a missing
    ``view_definitions_cache`` row simply means an empty views list,
    not a failed build.
    """
    profile = scope.profile
    database = scope.anchor.database
    schema = scope.anchor.schema
    table = scope.anchor.table
    fqn = f"{schema}.{table}"

    table_description = _read_table_description(hs, profile, database, schema, table)
    columns = _read_columns(hs, profile, database, schema, table)
    fk_partners = _read_fk_partners(hs, profile, database, schema, table)
    view_references = _read_view_references(hs, profile, database, schema, table)
    co_occurrence = _read_co_occurrence(hs, profile, database, schema, table)

    return RichAnchorContext(
        fqn=fqn,
        table_description=table_description,
        columns=columns,
        fk_partners=fk_partners,
        view_references=view_references,
        co_occurrence_partners=co_occurrence,
    )


# ── Section readers ──────────────────────────────────────────────────────


def _read_table_description(hs: Any, profile: str, database: str, schema: str, table: str) -> str:
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT cd.description_text
            FROM catalog_entities ce
            JOIN catalog_descriptions cd ON cd.id = ce.effective_description_id
            WHERE ce.db_profile = ? AND ce.database_name = ?
              AND ce.schema_name = ? AND ce.table_name = ?
              AND ce.entity_kind = 'table'
            LIMIT 1
            """,
            (profile, database, schema, table),
        ).fetchone()
    return str(row[0]) if row and row[0] else ""


def _read_columns(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[dict[str, str]]:
    """Columns from column_comments_cache; truncates each description."""
    cache_key = f"{profile}|{database}|{schema}|{table}"
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT columns_json FROM column_comments_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
    if not row or not row[0]:
        return []
    try:
        payload = json.loads(row[0])
    except (TypeError, ValueError):
        return []
    out: list[dict[str, str]] = []
    if isinstance(payload, dict):
        for name, meta in payload.items():
            entry: dict[str, str] = {"name": str(name)}
            if isinstance(meta, dict):
                dtype = meta.get("type")
                desc = meta.get("description")
                if dtype:
                    entry["dtype"] = str(dtype)
                if desc:
                    entry["description"] = str(desc)[:_MAX_COLUMN_DESC_CHARS]
            out.append(entry)
    return out


def _read_fk_partners(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[FKPartner]:
    """Pull every FK edge that touches the anchor."""
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT
                cr.from_entity_id, cr.to_entity_id,
                cr.from_column, cr.to_column,
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
            LIMIT 200
            """,
            (
                profile,
                database,
                schema,
                table,
                schema,
                table,
            ),
        ).fetchall()
    out: list[FKPartner] = []
    for row in rows:
        src_fqn = f"{row[4]}.{row[5]}"
        tgt_fqn = f"{row[6]}.{row[7]}"
        anchor_fqn = f"{schema}.{table}"
        if src_fqn == anchor_fqn and tgt_fqn != anchor_fqn:
            out.append(
                FKPartner(
                    direction="outbound",
                    other_fqn=tgt_fqn,
                    from_column=str(row[2] or ""),
                    to_column=str(row[3] or ""),
                )
            )
        elif tgt_fqn == anchor_fqn and src_fqn != anchor_fqn:
            out.append(
                FKPartner(
                    direction="inbound",
                    other_fqn=src_fqn,
                    from_column=str(row[2] or ""),
                    to_column=str(row[3] or ""),
                )
            )
    # Dedupe (direction, other_fqn, columns) and cap.
    seen: set[tuple[str, str, str, str]] = set()
    uniq: list[FKPartner] = []
    for fk in out:
        key = (fk.direction, fk.other_fqn, fk.from_column, fk.to_column)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(fk)
        if len(uniq) >= _MAX_FK_PARTNERS:
            break
    return uniq


def _read_view_references(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[ViewReference]:
    """Views whose parsed lineage mentions the anchor.

    ``view_definitions_cache.parsed_lineage_json`` is the per-view list
    of ``{target, sources: [{table, column}, ...]}`` entries the
    ViewDDLExtractor produces. We collect every cache row in the
    anchor's database and keep the ones that mention the anchor's
    table name, then surface the OTHER tables joined within each
    such view as the high-signal partner list.
    """
    anchor_table_lower = table.lower()
    with hs._connect() as conn:
        rows = conn.execute(
            """
            SELECT view_name, parsed_lineage_json
            FROM view_definitions_cache
            WHERE db_profile = ? AND database_name = ?
              AND parse_status = 'ok'
            LIMIT 500
            """,
            (profile, database),
        ).fetchall()
    refs: list[ViewReference] = []
    for view_name, parsed_json in rows:
        if not parsed_json:
            continue
        try:
            parsed = json.loads(parsed_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(parsed, list):
            continue
        sources_tables: set[str] = set()
        mentions_anchor = False
        for entry in parsed:
            if not isinstance(entry, dict):
                continue
            for src in entry.get("sources") or []:
                if not isinstance(src, dict):
                    continue
                src_table = str(src.get("table") or "")
                if not src_table:
                    continue
                if src_table.lower() == anchor_table_lower:
                    mentions_anchor = True
                else:
                    sources_tables.add(src_table)
        if mentions_anchor and sources_tables:
            others = sorted(sources_tables)[:_MAX_TABLES_PER_VIEW]
            # ``view_name`` may be ``schema.view`` already, or just
            # the bare name — keep it as stored.
            refs.append(ViewReference(view_fqn=str(view_name), other_tables=others))
            if len(refs) >= _MAX_VIEW_REFS:
                break
    return refs


def _read_co_occurrence(
    hs: Any, profile: str, database: str, schema: str, table: str
) -> list[CoOccurrencePartner]:
    """Tables that show up alongside the anchor in recent query history.

    Falls back silently when the query log table doesn't exist or
    can't be parsed — co-occurrence is best-effort signal.
    """
    counts: dict[str, int] = {}
    anchor_fqn_lower = f"{schema}.{table}".lower()
    try:
        with hs._connect() as conn:
            rows = conn.execute(
                """
                SELECT tables_json
                FROM query_log_events
                WHERE db_profile = ?
                ORDER BY id DESC
                LIMIT 500
                """,
                (profile,),
            ).fetchall()
    except Exception:
        return []
    for row in rows:
        tables_json = row[0] if row else None
        if not tables_json:
            continue
        try:
            tables = json.loads(tables_json)
        except (TypeError, ValueError):
            continue
        if not isinstance(tables, list):
            continue
        normalised = {str(t).lower() for t in tables if isinstance(t, str)}
        if anchor_fqn_lower not in normalised:
            continue
        for other in tables:
            if not isinstance(other, str):
                continue
            other_lower = other.lower()
            if other_lower == anchor_fqn_lower:
                continue
            counts[other] = counts.get(other, 0) + 1
    if not counts:
        # Last-ditch: maybe the column is just ``tables`` (TEXT csv).
        with contextlib.suppress(Exception):
            with hs._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT tables FROM query_log_events
                    WHERE db_profile = ?
                    ORDER BY id DESC
                    LIMIT 500
                    """,
                    (profile,),
                ).fetchall()
            for row in rows:
                raw = (row[0] or "") if row else ""
                tokens = [t.strip() for t in str(raw).split(",") if t.strip()]
                normalised = {t.lower() for t in tokens}
                if anchor_fqn_lower not in normalised:
                    continue
                for other in tokens:
                    if other.lower() == anchor_fqn_lower:
                        continue
                    counts[other] = counts.get(other, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:_MAX_CO_OCCUR]
    return [CoOccurrencePartner(other_fqn=fqn, count=count) for fqn, count in ranked]


__all__ = [
    "CoOccurrencePartner",
    "FKPartner",
    "RichAnchorContext",
    "ViewReference",
    "build_rich_context",
]
