"""Detect transformation operators inside SELECT projections.

Used by :mod:`amx.lineage.extractors.view_ddl` to attach
:class:`amx.lineage.types.OperatorMeta` to edges when a source column
flows through an aggregate, function call, or other non-trivial
transform on its way to the projected target.

Kept separate from the extractor so the sqlglot-specific logic has its
own test surface and the extractor stays focused on
cache-miss / db-fill orchestration.
"""

from __future__ import annotations

from typing import Any

from amx.lineage.types import OperatorMeta

# Map sqlglot expression class names to v4 op_kind values. Order
# matters — we match the outermost expression first.
_AGG_FUNC_NAMES: frozenset[str] = frozenset(
    {"sum", "count", "avg", "min", "max", "stddev", "variance", "string_agg", "array_agg"}
)


def classify_projection(sqlglot_mod: Any, projection: Any) -> OperatorMeta | None:
    """Return the operator metadata for a SELECT projection, or ``None``.

    Pass-through projections (``SELECT col``, ``SELECT col AS alias``,
    bare column with CAST/AS) get ``None`` so the resulting edges stay
    plain column→column. Wrapping the projection in a function call,
    an aggregate, or a CASE expression promotes it to an operator
    edge — these are the visible transforms the user wants to see on
    the canvas.
    """
    if projection is None:
        return None
    exp = sqlglot_mod.exp
    expr = projection
    # Unwrap aliases — the operator decision is about the projected
    # expression itself, not the alias wrapper.
    if isinstance(projection, exp.Alias):
        expr = projection.this
    if expr is None:
        return None
    # Plain column reference — no operator.
    if isinstance(expr, exp.Column):
        return None
    # Cast(col AS type) is also pass-through for lineage purposes — the
    # underlying column still flows through unchanged.
    if isinstance(expr, exp.Cast) and isinstance(expr.this, exp.Column):
        return None
    sql_text = _safe_sql(expr)
    op_kind = _detect_op_kind(sqlglot_mod, expr)
    if op_kind is None:
        return None
    return OperatorMeta(op_kind=op_kind, expression=sql_text)


def _detect_op_kind(sqlglot_mod: Any, expr: Any) -> str | None:
    """Map a sqlglot expression to an op_kind label."""
    exp = sqlglot_mod.exp
    if isinstance(expr, exp.Window):
        return "function"
    if _looks_like_aggregate(sqlglot_mod, expr):
        return "aggregate"
    if isinstance(expr, exp.Case):
        return "function"
    if isinstance(expr, exp.Func):
        return "function"
    # Binary arithmetic on column refs is a projection-with-expression
    # rather than a clean function call. Treat as a function for now —
    # the user will see the SQL text in the operator body.
    if hasattr(exp, "Binary") and isinstance(expr, exp.Binary):
        return "function"
    return None


def _looks_like_aggregate(sqlglot_mod: Any, expr: Any) -> bool:
    """True when ``expr`` is (or wraps) a SUM/COUNT/AVG-family call."""
    exp = sqlglot_mod.exp
    # Direct subclass check first — sqlglot exposes AggFunc as a base.
    if hasattr(exp, "AggFunc") and isinstance(expr, exp.AggFunc):
        return True
    if isinstance(expr, exp.Func):
        name = (expr.sql_name() or "").lower() if hasattr(expr, "sql_name") else ""
        if not name:
            name = expr.__class__.__name__.lower()
        if name in _AGG_FUNC_NAMES:
            return True
    return False


def _safe_sql(expr: Any) -> str:
    """Render an expression's SQL text, with a length cap so canvas
    operator bodies never get unwieldy."""
    try:
        text = expr.sql()
    except Exception:
        try:
            text = str(expr)
        except Exception:
            text = ""
    text = text.strip()
    if len(text) > 200:
        text = text[:197] + "..."
    return text


__all__ = ["classify_projection"]
