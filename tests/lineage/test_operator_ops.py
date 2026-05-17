"""v4 S1 — operator entity + column-level edge persistence."""

from __future__ import annotations

import json

import pytest

from amx.lineage.operator_ops import (
    create_operator_with_edges,
    decode_operator_details,
    encode_operator_details,
    lookup_operator,
    update_operator_expression,
    upsert_operator_entity,
    write_column_edge,
)

from .conftest import seed_table_entity


def test_operator_path_is_deterministic_for_same_inputs():
    a = encode_operator_details(op_kind="filter", expression="status = 'ok'")
    b = encode_operator_details(op_kind="filter", expression="status = 'ok'")
    assert a == b


def test_encode_decode_round_trip():
    blob = encode_operator_details(
        op_kind="aggregate",
        expression="SUM(amount)",
        input_columns=[{"fqn": "public.t.amount", "alias": "amount"}],
        output_columns=[{"fqn": "public.daily.gross", "alias": "gross"}],
    )
    out = decode_operator_details(blob)
    assert out["op_kind"] == "aggregate"
    assert out["expression"] == "SUM(amount)"
    assert out["input_columns"][0]["alias"] == "amount"
    assert out["output_columns"][0]["fqn"] == "public.daily.gross"


def test_upsert_operator_entity_creates_then_updates(hs):
    seed_table_entity(hs, schema="public", table="daily_totals")
    op_id, path = upsert_operator_entity(
        hs,
        profile="p",
        db_backend="postgresql",
        database="",
        schema="public",
        table="daily_totals",
        op_kind="aggregate",
        expression="SUM(amount)",
    )
    assert op_id > 0
    assert path.startswith("op:public.daily_totals:aggregate:")

    # Same inputs → same row (upsert).
    op_id_again, path_again = upsert_operator_entity(
        hs,
        profile="p",
        db_backend="postgresql",
        database="",
        schema="public",
        table="daily_totals",
        op_kind="aggregate",
        expression="SUM(amount)",
    )
    assert op_id_again == op_id
    assert path_again == path


def test_upsert_rejects_unknown_op_kind(hs):
    with pytest.raises(ValueError):
        upsert_operator_entity(
            hs,
            profile="p",
            db_backend="postgresql",
            database="",
            schema="public",
            table="t",
            op_kind="frobnicate",
            expression="x",
        )


def test_write_column_edge_round_trip(hs):
    src = seed_table_entity(hs, schema="public", table="orders")
    tgt = seed_table_entity(hs, schema="public", table="customers")
    edge_id = write_column_edge(
        hs,
        from_entity_id=src,
        from_column="customer_id",
        to_entity_id=tgt,
        to_column="id",
        relationship_type="lineage_fk",
        score=1.0,
        source="database",
    )
    assert edge_id > 0
    with hs._connect() as conn:
        row = conn.execute(
            "SELECT from_column, to_column, relationship_type, score "
            "FROM catalog_relationships WHERE id = ?",
            (edge_id,),
        ).fetchone()
    assert row[0] == "customer_id"
    assert row[1] == "id"
    assert row[2] == "lineage_fk"
    assert float(row[3]) == 1.0


def test_write_column_edge_upserts_on_logical_key(hs):
    src = seed_table_entity(hs, schema="public", table="orders")
    tgt = seed_table_entity(hs, schema="public", table="customers")
    first = write_column_edge(
        hs,
        from_entity_id=src,
        from_column="customer_id",
        to_entity_id=tgt,
        to_column="id",
        relationship_type="lineage_fk",
        score=0.5,
        source="db",
    )
    second = write_column_edge(
        hs,
        from_entity_id=src,
        from_column="customer_id",
        to_entity_id=tgt,
        to_column="id",
        relationship_type="lineage_fk",
        score=1.0,
        source="db",
    )
    assert second != first  # New row id (delete + insert).
    with hs._connect() as conn:
        rows = conn.execute(
            "SELECT score FROM catalog_relationships "
            "WHERE from_entity_id = ? AND to_entity_id = ? "
            "AND from_column = ? AND to_column = ?",
            (src, tgt, "customer_id", "id"),
        ).fetchall()
    assert len(rows) == 1  # Only the latest survives.
    assert float(rows[0][0]) == 1.0


def test_create_operator_with_edges_writes_three_rows(hs):
    src = seed_table_entity(hs, schema="public", table="orders")
    tgt = seed_table_entity(hs, schema="public", table="daily_totals")
    result = create_operator_with_edges(
        hs,
        profile="p",
        db_backend="postgresql",
        source_entity_id=src,
        source_column="amount",
        target_entity_id=tgt,
        target_column="gross",
        target_database="",
        target_schema="public",
        target_table="daily_totals",
        op_kind="aggregate",
        expression="SUM(amount)",
    )
    assert result["operator_id"] > 0
    assert len(result["edge_ids"]) == 2

    with hs._connect() as conn:
        op_count = conn.execute(
            "SELECT COUNT(*) FROM catalog_entities WHERE entity_kind = 'operator'"
        ).fetchone()[0]
        edge_rows = conn.execute(
            "SELECT from_entity_id, to_entity_id, from_column, to_column, details_json "
            "FROM catalog_relationships ORDER BY id"
        ).fetchall()
    assert op_count == 1
    assert len(edge_rows) == 2

    in_row, out_row = edge_rows
    # source → operator
    assert in_row[0] == src
    assert in_row[1] == result["operator_id"]
    assert in_row[2] == "amount"
    # operator → target
    assert out_row[0] == result["operator_id"]
    assert out_row[1] == tgt
    assert out_row[3] == "gross"
    # Details_json carries role + op_kind for both flank edges.
    in_details = json.loads(in_row[4])
    out_details = json.loads(out_row[4])
    assert in_details["role"] == "operator_input"
    assert out_details["role"] == "operator_output"
    assert in_details["op_kind"] == "aggregate"


def test_lookup_and_update_operator(hs):
    seed_table_entity(hs, schema="public", table="t")
    op_id, _ = upsert_operator_entity(
        hs,
        profile="p",
        db_backend="postgresql",
        database="",
        schema="public",
        table="t",
        op_kind="filter",
        expression="x > 0",
    )
    info = lookup_operator(hs, operator_id=op_id)
    assert info is not None
    assert info["details"]["expression"] == "x > 0"

    changed = update_operator_expression(hs, operator_id=op_id, expression="x > 10")
    assert changed is True
    after = lookup_operator(hs, operator_id=op_id)
    assert after is not None
    assert after["details"]["expression"] == "x > 10"
    assert after["details"]["op_kind"] == "filter"


def test_update_operator_missing_returns_false(hs):
    assert update_operator_expression(hs, operator_id=9999, expression="x") is False
