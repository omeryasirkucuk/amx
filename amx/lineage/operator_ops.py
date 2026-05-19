"""Operator-node persistence helpers for v4 column-level lineage.

A transformation operator (filter / join / function / aggregate /
projection) is stored as a synthetic ``catalog_entities`` row with
``entity_kind='operator'`` so it shares the same FK column on
``catalog_relationships`` as real tables and columns. The operator's
expression and port metadata live in ``catalog_entities.search_text``
(reused as a JSON blob — the column already accepts arbitrary text
and is the only TEXT field without a stronger semantic on the table).

Edges chain ``source_col → operator → target_col`` via two rows:

* From source to operator: ``from_column = <source col>``,
  ``to_column = <operator input port>`` (typically the same name).
* From operator to target: ``from_column = <operator output port>``,
  ``to_column = <target col>``.

This file owns all writes that create / update / lookup operators so
extractors and the Studio router never duplicate the synthetic-path
encoding.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from amx.storage.sqlite_store import SQLiteHistoryStore

OpKind = Literal["filter", "join", "function", "aggregate", "projection"]
_VALID_OP_KINDS: frozenset[str] = frozenset(
    {"filter", "join", "function", "aggregate", "projection"}
)


def _operator_path(*, schema: str, table: str, op_kind: str, expression: str) -> str:
    """Synthetic path for an operator entity.

    Format: ``op:<schema>.<table>:<op_kind>:<short_hash>``. The hash
    keeps two operators of the same kind on the same anchor distinct
    when their expressions differ (e.g. two filters with different
    predicates).
    """
    digest = hashlib.sha1(
        f"{schema}|{table}|{op_kind}|{expression}".encode(),
        usedforsecurity=False,
    ).hexdigest()[:8]
    return f"op:{schema}.{table}:{op_kind}:{digest}"


def encode_operator_details(
    *,
    op_kind: str,
    expression: str,
    input_columns: Iterable[dict[str, str]] = (),
    output_columns: Iterable[dict[str, str]] = (),
) -> str:
    """Serialise an operator's payload for ``catalog_entities.search_text``.

    The blob is JSON: ``{op_kind, expression, input_columns,
    output_columns}``. Each column entry is ``{fqn, alias}``.
    """
    payload = {
        "op_kind": str(op_kind),
        "expression": str(expression or ""),
        "input_columns": [
            {"fqn": str(c.get("fqn") or ""), "alias": str(c.get("alias") or "")}
            for c in input_columns
        ],
        "output_columns": [
            {"fqn": str(c.get("fqn") or ""), "alias": str(c.get("alias") or "")}
            for c in output_columns
        ],
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def decode_operator_details(blob: str) -> dict[str, Any]:
    """Parse an operator entity's stored payload. Returns ``{}`` on bad input."""
    if not blob:
        return {}
    try:
        data = json.loads(blob)
    except (TypeError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def upsert_operator_entity(
    hs: SQLiteHistoryStore,
    *,
    profile: str,
    db_backend: str,
    database: str,
    schema: str,
    table: str,
    op_kind: str,
    expression: str,
    input_columns: Iterable[dict[str, str]] = (),
    output_columns: Iterable[dict[str, str]] = (),
) -> tuple[int, str]:
    """Insert or refresh a synthetic operator entity.

    Returns ``(entity_id, operator_path)``. Schema + table identify
    the "anchor" the operator sits next to (typically the view or
    target table the operator helps populate); the operator path is
    stored in ``column_name`` so the unique key
    ``(profile, database, schema, table, column_name, entity_kind)``
    keeps two distinct operators apart.
    """
    if op_kind not in _VALID_OP_KINDS:
        raise ValueError(f"op_kind must be one of {sorted(_VALID_OP_KINDS)}; got {op_kind!r}")
    path = _operator_path(schema=schema, table=table, op_kind=op_kind, expression=expression)
    details_json = encode_operator_details(
        op_kind=op_kind,
        expression=expression,
        input_columns=input_columns,
        output_columns=output_columns,
    )
    now = time.time()
    with hs._lock, hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id FROM catalog_entities
            WHERE db_profile = ? AND database_name = ? AND schema_name = ?
              AND table_name = ? AND COALESCE(column_name, '') = ?
              AND entity_kind = 'operator'
            LIMIT 1
            """,
            (profile, database, schema, table, path),
        ).fetchone()
        if row:
            entity_id = int(row[0])
            conn.execute(
                """
                UPDATE catalog_entities
                SET search_text = ?, updated_at = ?, asset_kind = 'operator'
                WHERE id = ?
                """,
                (details_json, now, entity_id),
            )
            return entity_id, path

        cur = conn.execute(
            """
            INSERT INTO catalog_entities
                (db_profile, db_backend, database_name, schema_name, table_name,
                 column_name, entity_kind, asset_kind, search_text,
                 updated_at, last_synced_at)
            VALUES (?, ?, ?, ?, ?, ?, 'operator', 'operator', ?, ?, ?)
            """,
            (
                profile,
                db_backend,
                database,
                schema,
                table,
                path,
                details_json,
                now,
                now,
            ),
        )
        return int(cur.lastrowid or 0), path


def write_column_edge(
    hs: SQLiteHistoryStore,
    *,
    from_entity_id: int,
    from_column: str,
    to_entity_id: int,
    to_column: str,
    relationship_type: str,
    score: float,
    source: str,
    details: dict[str, Any] | None = None,
    verdict: str = "",
    audit_actor: str = "",
    audit_at: float | None = None,
    style_color: str | None = None,
    style_dashed: bool | None = None,
    cardinality: str | None = None,
) -> int:
    """Write a single column-level edge. Returns the row id.

    Upserts on the logical key ``(from_entity_id, from_column,
    to_entity_id, to_column, relationship_type)`` — re-inserting the
    same column edge updates ``score``, ``details_json``,
    ``last_seen``, and the audit fields without stacking duplicates.

    ``style_color`` / ``style_dashed`` / ``cardinality`` are
    Studio-canvas visual overrides; ``None`` clears the column back
    to the default-rendering state.
    """
    details_json = json.dumps(details or {}, ensure_ascii=False)
    now = time.time()
    audit_ts = now if audit_at is None else float(audit_at)
    dashed_int = (1 if style_dashed else 0) if style_dashed is not None else None
    with hs._lock, hs._connect() as conn:
        conn.execute(
            """
            DELETE FROM catalog_relationships
            WHERE from_entity_id = ? AND from_column = ?
              AND to_entity_id = ? AND to_column = ?
              AND relationship_type = ?
            """,
            (
                int(from_entity_id),
                str(from_column),
                int(to_entity_id),
                str(to_column),
                str(relationship_type),
            ),
        )
        cur = conn.execute(
            """
            INSERT INTO catalog_relationships
                (from_entity_id, to_entity_id, relationship_type, score, source,
                 details_json, last_seen, verdict, audit_actor, audit_at,
                 from_column, to_column,
                 style_color, style_dashed, cardinality)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(from_entity_id),
                int(to_entity_id),
                str(relationship_type),
                float(score),
                str(source),
                details_json,
                now,
                str(verdict),
                str(audit_actor),
                audit_ts,
                str(from_column),
                str(to_column),
                style_color,
                dashed_int,
                cardinality,
            ),
        )
        return int(cur.lastrowid or 0)


def create_operator_with_edges(
    hs: SQLiteHistoryStore,
    *,
    profile: str,
    db_backend: str,
    source_entity_id: int,
    source_column: str,
    target_entity_id: int,
    target_column: str,
    target_database: str,
    target_schema: str,
    target_table: str,
    op_kind: str,
    expression: str,
    relationship_type: str = "lineage_view_ddl",
    score: float = 1.0,
    source: str = "view_ddl",
    verdict: str = "",
    audit_actor: str = "",
) -> dict[str, Any]:
    """Create an operator entity and the two flanking edges atomically.

    The operator sits between ``source.column`` and ``target.column``.
    Two rows land in ``catalog_relationships``:

    1. ``source.column → operator(op_path)`` with ``to_column = source_column``
       (the operator's input port mirrors the source column name).
    2. ``operator(op_path) → target.column`` with ``from_column = target_column``
       (the operator's output port mirrors the target column name).

    Returns ``{operator_id, operator_path, edge_ids: [in, out]}``.
    """
    op_id, op_path = upsert_operator_entity(
        hs,
        profile=profile,
        db_backend=db_backend,
        database=target_database,
        schema=target_schema,
        table=target_table,
        op_kind=op_kind,
        expression=expression,
        input_columns=[{"fqn": "", "alias": source_column}],
        output_columns=[{"fqn": "", "alias": target_column}],
    )

    edge_in = write_column_edge(
        hs,
        from_entity_id=source_entity_id,
        from_column=source_column,
        to_entity_id=op_id,
        to_column=source_column,
        relationship_type=relationship_type,
        score=score,
        source=source,
        details={"role": "operator_input", "op_path": op_path, "op_kind": op_kind},
        verdict=verdict,
        audit_actor=audit_actor,
    )
    edge_out = write_column_edge(
        hs,
        from_entity_id=op_id,
        from_column=target_column,
        to_entity_id=target_entity_id,
        to_column=target_column,
        relationship_type=relationship_type,
        score=score,
        source=source,
        details={"role": "operator_output", "op_path": op_path, "op_kind": op_kind},
        verdict=verdict,
        audit_actor=audit_actor,
    )
    return {
        "operator_id": op_id,
        "operator_path": op_path,
        "edge_ids": [edge_in, edge_out],
    }


def lookup_operator(
    hs: SQLiteHistoryStore,
    *,
    operator_id: int,
) -> dict[str, Any] | None:
    """Read back an operator entity by id. Returns ``None`` when missing."""
    with hs._connect() as conn:
        row = conn.execute(
            """
            SELECT id, db_profile, database_name, schema_name, table_name,
                   column_name, search_text, updated_at
            FROM catalog_entities
            WHERE id = ? AND entity_kind = 'operator'
            LIMIT 1
            """,
            (int(operator_id),),
        ).fetchone()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "profile": str(row[1] or ""),
        "database": str(row[2] or ""),
        "schema": str(row[3] or ""),
        "table": str(row[4] or ""),
        "operator_path": str(row[5] or ""),
        "details": decode_operator_details(str(row[6] or "")),
        "updated_at": float(row[7] or 0.0),
    }


def update_operator_expression(
    hs: SQLiteHistoryStore,
    *,
    operator_id: int,
    expression: str,
) -> bool:
    """Patch an operator's expression. Returns ``True`` when a row changed."""
    current = lookup_operator(hs, operator_id=operator_id)
    if not current:
        return False
    details = current.get("details") or {}
    payload = encode_operator_details(
        op_kind=str(details.get("op_kind") or "function"),
        expression=expression,
        input_columns=details.get("input_columns") or [],
        output_columns=details.get("output_columns") or [],
    )
    now = time.time()
    with hs._lock, hs._connect() as conn:
        cur = conn.execute(
            "UPDATE catalog_entities SET search_text = ?, updated_at = ? "
            "WHERE id = ? AND entity_kind = 'operator'",
            (payload, now, int(operator_id)),
        )
    return bool(cur.rowcount)


__all__ = [
    "OpKind",
    "encode_operator_details",
    "decode_operator_details",
    "upsert_operator_entity",
    "write_column_edge",
    "create_operator_with_edges",
    "lookup_operator",
    "update_operator_expression",
]
