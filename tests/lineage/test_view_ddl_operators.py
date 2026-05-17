"""v4 S2 — view_ddl extractor surfaces operator metadata on edges."""

from __future__ import annotations

import pytest

from amx.lineage.extractors.view_ddl import _parse_view_lineage
from amx.lineage.view_ddl_operators import classify_projection

# The module-level _load_sqlglot_module helper is the canonical loader
# used by the extractor — these tests fetch sqlglot the same way so they
# skip cleanly on machines without it (only common for fresh checkouts
# before ``pip install -e '.[lineage]'``).
sqlglot = pytest.importorskip("sqlglot")


def _projections(ddl: str, dialect: str = "postgres"):
    tree = sqlglot.parse_one(ddl, dialect=dialect)
    select = (
        tree.expression if hasattr(tree, "expression") and tree.expression is not None else tree
    )
    return list(select.expressions or [])


def test_pass_through_projection_has_no_operator():
    projections = _projections("SELECT a, b FROM t")
    for proj in projections:
        assert classify_projection(sqlglot, proj) is None


def test_aliased_column_has_no_operator():
    [proj] = _projections("SELECT a AS aa FROM t")
    assert classify_projection(sqlglot, proj) is None


def test_cast_pass_through_has_no_operator():
    [proj] = _projections("SELECT CAST(a AS INTEGER) AS aa FROM t")
    assert classify_projection(sqlglot, proj) is None


def test_sum_projection_is_aggregate():
    [proj] = _projections("SELECT SUM(amount) AS total FROM t")
    op = classify_projection(sqlglot, proj)
    assert op is not None
    assert op.op_kind == "aggregate"
    assert "SUM" in op.expression.upper()


def test_count_distinct_is_aggregate():
    [proj] = _projections("SELECT COUNT(DISTINCT id) AS uniques FROM t")
    op = classify_projection(sqlglot, proj)
    assert op is not None
    assert op.op_kind == "aggregate"


def test_scalar_function_call_is_function():
    [proj] = _projections("SELECT LOWER(name) AS name_l FROM t")
    op = classify_projection(sqlglot, proj)
    assert op is not None
    assert op.op_kind == "function"


def test_case_when_is_function():
    [proj] = _projections("SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END AS flag FROM t")
    op = classify_projection(sqlglot, proj)
    assert op is not None
    assert op.op_kind == "function"


def test_arithmetic_is_function():
    [proj] = _projections("SELECT a + b AS s FROM t")
    op = classify_projection(sqlglot, proj)
    assert op is not None
    assert op.op_kind == "function"


def test_parse_view_lineage_emits_operator_metadata():
    ddl = """
    CREATE VIEW totals AS
    SELECT customer_id, SUM(amount) AS gross
    FROM orders
    GROUP BY customer_id
    """
    parsed, status, error = _parse_view_lineage(sqlglot, ddl, "postgres", "totals")
    assert status == "ok", error
    assert parsed is not None
    by_target = {entry["target"]: entry for entry in parsed}
    assert "gross" in by_target
    gross = by_target["gross"]
    assert gross.get("operator") is not None
    assert gross["operator"]["op_kind"] == "aggregate"
    assert "amount" in gross["operator"]["expression"].lower()
    # Pass-through customer_id is plain — no operator.
    assert "operator" not in by_target["customer_id"]


def test_parse_view_lineage_function_call():
    ddl = "CREATE VIEW v AS SELECT LOWER(name) AS name_l FROM users"
    parsed, status, error = _parse_view_lineage(sqlglot, ddl, "postgres", "v")
    assert status == "ok", error
    assert parsed is not None
    [entry] = parsed
    assert entry["operator"]["op_kind"] == "function"
    assert "LOWER" in entry["operator"]["expression"].upper()
