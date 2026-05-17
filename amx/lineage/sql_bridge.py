"""Bidirectional SQL <-> canvas bridge for the Lineage editor.

The Lineage editor toolbar exposes two SQL actions:

* **Import SQL** posts a SELECT statement and receives canvas JSON
  (tables, operator chain, edges) the editor renders as nodes.
* **Export SQL** posts the current canvas and receives a composed
  SELECT that, when fed back through Import, round-trips to an
  equivalent graph.

Parsing reuses sqlglot through the same loader the view-DDL extractor
relies on (:mod:`amx.lineage.extractors.view_ddl`), so a SELECT that
parses there parses here too. Rendering is a small dialect-agnostic
SELECT builder over the canvas's operator chain — no template
strings; sqlglot composes and pretty-prints the output.
"""

from __future__ import annotations

import importlib
from typing import Any


class SqlBridgeError(RuntimeError):
    """Raised when a SQL fragment cannot be parsed or composed."""


def _load_sqlglot() -> Any:
    try:
        return importlib.import_module("sqlglot")
    except ImportError as exc:  # pragma: no cover - import guard
        raise SqlBridgeError(
            "sqlglot is required for SQL parse/render but is not installed."
        ) from exc


# ── Parse ────────────────────────────────────────────────────────────────


def parse_select_to_canvas(sql: str, *, dialect: str | None = None) -> dict[str, Any]:
    """Turn a SELECT statement into canvas-ready nodes + edges.

    The output schema is intentionally minimal so the frontend adapter
    can map straight into ReactFlow shapes without re-parsing:

    .. code-block:: json

        {
          "tables": [
            {"id": "schema.t", "schema": "schema", "table": "t", "columns": [...]}
          ],
          "operators": [
            {"id": "op:where:0", "kind": "filter", "expression": "..."}
          ],
          "edges": [
            {"source": "schema.t", "source_column": "id",
             "target": "op:where:0", "target_column": ""},
            ...
          ]
        }
    """
    sqlglot = _load_sqlglot()
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as exc:  # noqa: BLE001 - sqlglot raises many subtypes
        raise SqlBridgeError(f"sqlglot could not parse the SQL: {exc}") from exc

    exp = sqlglot.exp
    if not isinstance(tree, exp.Select):
        select = tree.find(exp.Select) if tree else None
        if select is None:
            raise SqlBridgeError("Could not isolate a SELECT statement.")
        tree = select

    tables: dict[str, dict[str, Any]] = {}
    for tbl in tree.find_all(exp.Table):
        name = str(tbl.name or "")
        schema = str(getattr(tbl, "db", "") or "")
        if not name:
            continue
        key = f"{schema}.{name}" if schema else name
        if key not in tables:
            tables[key] = {
                "id": key,
                "schema": schema,
                "table": name,
                "columns": [],
            }

    # Best-effort column harvest from projections + WHERE/JOIN/GROUP BY.
    cols_by_table: dict[str, set[str]] = {k: set() for k in tables}
    for col in tree.find_all(exp.Column):
        col_name = str(col.name or "")
        if not col_name:
            continue
        qualifier = str(getattr(col, "table", "") or "")
        if qualifier and qualifier in cols_by_table:
            cols_by_table[qualifier].add(col_name)
        elif len(tables) == 1:
            only = next(iter(tables))
            cols_by_table[only].add(col_name)
    for key, cols in cols_by_table.items():
        tables[key]["columns"] = sorted(cols)

    operators: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    # WHERE → filter op.
    where = tree.args.get("where")
    if where is not None:
        expr = (
            where.this.sql(dialect=dialect)
            if hasattr(where, "this")
            else where.sql(dialect=dialect)
        )
        op_id = "op:filter:0"
        operators.append({"id": op_id, "kind": "filter", "expression": str(expr)})
        for tbl_id in tables:
            edges.append(
                {
                    "source": tbl_id,
                    "source_column": "",
                    "target": op_id,
                    "target_column": "",
                }
            )

    # GROUP BY → aggregate op.
    group = tree.args.get("group")
    if group is not None and getattr(group, "expressions", None):
        keys = ", ".join(e.sql(dialect=dialect) for e in group.expressions)
        op_id = "op:aggregate:0"
        operators.append({"id": op_id, "kind": "aggregate", "expression": f"GROUP BY {keys}"})
        upstream = operators[-2]["id"] if len(operators) > 1 else next(iter(tables), "")
        if upstream:
            edges.append(
                {
                    "source": upstream,
                    "source_column": "",
                    "target": op_id,
                    "target_column": "",
                }
            )

    # JOIN → merge op per join clause.
    joins = tree.args.get("joins") or []
    for i, jn in enumerate(joins):
        op_id = f"op:join:{i}"
        on_clause = jn.args.get("on")
        on_sql = on_clause.sql(dialect=dialect) if on_clause is not None else ""
        kind = (jn.args.get("kind") or jn.args.get("side") or "INNER").upper()
        operators.append(
            {
                "id": op_id,
                "kind": "join",
                "expression": f"{kind} JOIN ON {on_sql}" if on_sql else f"{kind} JOIN",
            }
        )

    # Projections → emit a target table representing the SELECT output.
    target_id = "select:output"
    output_cols: list[str] = []
    for projection in tree.expressions or []:
        alias = projection.alias_or_name
        if alias:
            output_cols.append(str(alias))
    tables[target_id] = {
        "id": target_id,
        "schema": "",
        "table": "select_output",
        "columns": output_cols,
    }
    if operators:
        edges.append(
            {
                "source": operators[-1]["id"],
                "source_column": "",
                "target": target_id,
                "target_column": "",
            }
        )
    else:
        for tbl_id in list(tables):
            if tbl_id == target_id:
                continue
            edges.append(
                {
                    "source": tbl_id,
                    "source_column": "",
                    "target": target_id,
                    "target_column": "",
                }
            )

    return {
        "tables": list(tables.values()),
        "operators": operators,
        "edges": edges,
    }


# ── Render ───────────────────────────────────────────────────────────────


def render_canvas_to_sql(canvas: dict[str, Any], *, dialect: str | None = None) -> str:
    """Compose a SELECT statement from the canvas's operator chain.

    Round-trip behavior: feeding the output back through
    :func:`parse_select_to_canvas` should produce an equivalent graph
    (tables, filter, aggregate, join sets — column order may differ).
    """
    sqlglot = _load_sqlglot()

    tables = canvas.get("tables") or []
    operators = canvas.get("operators") or []
    if not tables:
        raise SqlBridgeError("Canvas has no tables to render.")

    # Anchor table = first non-output table.
    anchor: dict[str, Any] | None = None
    for tbl in tables:
        if str(tbl.get("id")) != "select:output":
            anchor = tbl
            break
    if anchor is None:
        raise SqlBridgeError("Canvas has no input tables (only the output node).")

    anchor_fqn = (
        f"{anchor.get('schema')}.{anchor.get('table')}"
        if anchor.get("schema")
        else str(anchor.get("table") or "")
    )

    output_cols: list[str] = []
    for tbl in tables:
        if str(tbl.get("id")) == "select:output":
            output_cols = list(tbl.get("columns") or [])
            break
    if not output_cols:
        output_cols = list(anchor.get("columns") or []) or ["*"]
    projection_sql = ", ".join(output_cols)

    filter_clause = ""
    group_clause = ""
    join_clauses: list[str] = []
    for op in operators:
        kind = str(op.get("kind") or "")
        expr = str(op.get("expression") or "").strip()
        if not expr:
            continue
        if kind == "filter":
            filter_clause = expr
        elif kind == "aggregate":
            group_clause = expr.replace("GROUP BY", "").strip()
        elif kind == "join":
            join_clauses.append(expr)

    parts = [f"SELECT {projection_sql}", f"FROM {anchor_fqn}"]
    parts.extend(join_clauses)
    if filter_clause:
        parts.append(f"WHERE {filter_clause}")
    if group_clause:
        parts.append(f"GROUP BY {group_clause}")
    raw = "\n".join(parts)

    try:
        return sqlglot.transpile(raw, read=dialect, write=dialect, pretty=True)[0]
    except Exception:  # noqa: BLE001
        # Pretty-printing is best-effort; return the assembled raw SQL.
        return raw
