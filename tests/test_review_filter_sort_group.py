"""PR A — bulk-review filter / sort / group helpers.

Pure-function tests against
:mod:`amx.cli_support.review_filter`. The Studio TSX side reuses the
same vocabulary (sort keys, group keys, status order); a single set of
unit tests pins the shared contract so a future tweak on either surface
trips the failing test before users notice.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from amx.cli_support.review_filter import (
    GROUP_KEYS,
    SORT_KEYS,
    STATUS_ACCEPTED,
    STATUS_APPLIED,
    STATUS_PENDING,
    STATUS_SKIPPED,
    apply_filters,
    apply_sort,
    asset_path,
    derive_status,
    filter_sort_group,
    format_summary_footer,
    group_rows,
    match_query,
)


@dataclass
class Row:
    """Tiny stand-in for a ReviewResult / pending entry."""

    schema: str
    table: str
    column: str | None
    confidence: str = "medium"
    logprob_score: float | None = None
    final_description: str = ""
    citations: list[Any] = field(default_factory=list)
    applied: bool = False
    skipped: bool = False


# ── Sort ───────────────────────────────────────────────────────────────


def test_sort_keys_constant_matches_spec() -> None:
    """The six canonical sort keys are exposed for both surfaces."""
    assert SORT_KEYS == (
        "conf-asc",
        "conf-desc",
        "logprob-asc",
        "logprob-desc",
        "name-asc",
        "status",
    )


def test_sort_by_confidence_asc() -> None:
    """Rows are sorted by confidence ascending; ties broken stably."""
    rows = [
        Row("s", "t", "a", confidence="high"),
        Row("s", "t", "b", confidence="low"),
        Row("s", "t", "c", confidence="medium"),
        Row("s", "t", "d", confidence="low"),
    ]
    out = apply_sort(rows, sort_key="conf-asc")
    assert [r.column for r in out] == ["b", "d", "c", "a"]


def test_sort_by_logprob_asc_pushes_missing_to_end() -> None:
    """Missing logprobs sort to the END under conf-asc so real lows show first."""
    rows = [
        Row("s", "t", "a", logprob_score=-0.5),
        Row("s", "t", "b", logprob_score=None),
        Row("s", "t", "c", logprob_score=-2.0),
    ]
    out = apply_sort(rows, sort_key="logprob-asc")
    assert [r.column for r in out] == ["c", "a", "b"]


def test_sort_by_name_asc_puts_table_level_first() -> None:
    """Table-level rows (column is None) sort before columns of the same table."""
    rows = [
        Row("sales", "orders", "id"),
        Row("sales", "orders", None),
        Row("sales", "orders", "customer_id"),
    ]
    out = apply_sort(rows, sort_key="name-asc")
    assert [r.column for r in out] == [None, "customer_id", "id"]


def test_sort_status_orders_pending_first() -> None:
    """The `status` sort key surfaces pending → accepted → skipped → applied."""
    rows = [
        Row("s", "t", "a", applied=True),
        Row("s", "t", "b", skipped=True),
        Row("s", "t", "c"),
    ]
    out = apply_sort(rows, sort_key="status")
    assert [r.column for r in out] == ["c", "b", "a"]


# ── Filter — regex ────────────────────────────────────────────────────


def test_filter_regex_matches_full_path() -> None:
    """Filter `sales\\.orders\\.cust.*` matches cust* under sales.orders."""
    rows = [
        Row("sales", "orders", "customer_id"),
        Row("sales", "orders", "shipped_at"),
        Row("hr", "employees", "customer_segment"),  # different schema
    ]
    out = apply_filters(rows, pattern=r"sales\.orders\.cust")
    assert len(out) == 1
    assert out[0].column == "customer_id"


def test_filter_case_insensitive() -> None:
    """Filter `SALES` matches schema `sales`."""
    rows = [
        Row("sales", "orders", "id"),
        Row("hr", "employees", "id"),
    ]
    out = apply_filters(rows, pattern="SALES")
    assert {r.schema for r in out} == {"sales"}


def test_filter_query_searches_description() -> None:
    """The free-text query (Studio search box) matches against final_description."""
    rows = [
        Row("a", "b", "c", final_description="Number of customers per region"),
        Row("a", "b", "d", final_description="Order total in USD"),
    ]
    matched = [r for r in rows if match_query(r, "customers")]
    assert len(matched) == 1


def test_only_unreviewed_excludes_accepted_and_skipped() -> None:
    """The --only-unreviewed shortcut works."""
    rows = [
        Row("s", "t", "a"),  # pending
        Row("s", "t", "b", applied=True),  # applied
        Row("s", "t", "c", skipped=True),  # skipped
    ]
    out = apply_filters(rows, only_unreviewed=True)
    assert [r.column for r in out] == ["a"]


def test_only_unreviewed_with_pending_callback() -> None:
    """`accepted` rows (queued but not yet applied) are excluded from --only-unreviewed."""
    rows = [Row("s", "t", "a"), Row("s", "t", "b")]
    queued = {"b"}
    out = apply_filters(
        rows,
        only_unreviewed=True,
        is_pending=lambda r: r.column in queued,
    )
    assert [r.column for r in out] == ["a"]


def test_only_low_conf_threshold_is_0_7() -> None:
    """Confidence 0.69 included, 0.70 excluded (low=0.3, medium=0.6, high=0.9)."""
    rows = [
        Row("s", "t", "a", confidence="low"),  # 0.3
        Row("s", "t", "b", confidence="medium"),  # 0.6
        Row("s", "t", "c", confidence="high"),  # 0.9
    ]
    out = apply_filters(rows, only_low_conf=True)
    assert {r.column for r in out} == {"a", "b"}


def test_filter_has_citations() -> None:
    """`has_citations` preset keeps only rows with at least one citation."""
    rows = [
        Row("s", "t", "a", citations=[]),
        Row("s", "t", "b", citations=[{"source": "doc.md"}]),
    ]
    out = apply_filters(rows, has_citations=True)
    assert len(out) == 1 and out[0].column == "b"


def test_filter_table_only() -> None:
    """`table_only` preset drops rows that carry a column name."""
    rows = [
        Row("s", "t", "a"),
        Row("s", "t", None),  # table-level
    ]
    out = apply_filters(rows, table_only=True)
    assert len(out) == 1 and out[0].column is None


# ── Group ─────────────────────────────────────────────────────────────


def test_group_keys_constant() -> None:
    assert GROUP_KEYS == ("none", "schema", "table")


def test_group_by_table_yields_distinct_groups() -> None:
    """Group-by-table produces one entry per (schema, table); group-by-none is flat."""
    rows = [
        Row("sales", "orders", "id"),
        Row("sales", "orders", "qty"),
        Row("sales", "customers", "id"),
        Row("hr", "employees", "id"),
    ]
    grouped = group_rows(rows, by="table")
    assert [g[0] for g in grouped] == [
        "sales.orders",
        "sales.customers",
        "hr.employees",
    ]
    assert len(grouped[0][1]) == 2

    flat = group_rows(rows, by="none")
    assert len(flat) == 1 and flat[0][0] == ""


def test_group_by_schema_collapses_tables() -> None:
    """Group-by-schema collapses tables into one bucket per schema."""
    rows = [
        Row("sales", "orders", "id"),
        Row("sales", "customers", "id"),
        Row("hr", "employees", "id"),
    ]
    grouped = group_rows(rows, by="schema")
    assert [g[0] for g in grouped] == ["sales", "hr"]
    assert len(grouped[0][1]) == 2


# ── Status badge derivation ───────────────────────────────────────────


def test_status_column_pending_accepted_skipped_applied() -> None:
    """The STATUS column renders the right marker for each state."""
    pending = Row("s", "t", "a")
    accepted = Row("s", "t", "b")
    skipped = Row("s", "t", "c", skipped=True)
    applied = Row("s", "t", "d", applied=True)
    is_pending = lambda r: r is accepted  # noqa: E731

    assert derive_status(pending, is_pending=is_pending) == STATUS_PENDING
    assert derive_status(accepted, is_pending=is_pending) == STATUS_ACCEPTED
    assert derive_status(skipped, is_pending=is_pending) == STATUS_SKIPPED
    assert derive_status(applied, is_pending=is_pending) == STATUS_APPLIED


# ── Composed pipeline + footer ────────────────────────────────────────


def test_filter_sort_group_pipeline() -> None:
    """End-to-end: filter narrows, sort orders, group partitions."""
    rows = [
        Row("sales", "orders", "id", confidence="high"),
        Row("sales", "orders", "qty", confidence="low"),
        Row("sales", "customers", "id", confidence="medium"),
        Row("hr", "employees", "id", confidence="low"),
    ]
    grouped = filter_sort_group(
        rows,
        pattern=r"sales",
        sort_key="conf-asc",
        group_by="table",
    )
    keys = [g[0] for g in grouped]
    assert "hr.employees" not in keys
    # Inside sales.orders, low-confidence row comes first
    sales_orders = dict(grouped)["sales.orders"]
    assert [r.column for r in sales_orders] == ["qty", "id"]


def test_summary_footer_shows_filter_and_sort() -> None:
    """Footer line shows 'Showing X of Y · filter: ... · sort: ...' when active."""
    assert format_summary_footer(total=10, visible=10) == "Showing 10 rows"
    footer = format_summary_footer(
        total=187,
        visible=12,
        pattern="sales.*",
        sort_key="conf-asc",
    )
    assert footer == "Showing 12 of 187 rows · filter: sales.* · sort: conf-asc"
    grouped_footer = format_summary_footer(
        total=10,
        visible=10,
        group_by="schema",
    )
    assert "group: schema" in grouped_footer


# ── Asset-path edge case ──────────────────────────────────────────────


def test_asset_path_handles_table_level_row() -> None:
    """Table-level rows render as `schema.table` (no trailing dot)."""
    assert asset_path(Row("sales", "orders", None)) == "sales.orders"
    assert asset_path(Row("sales", "orders", "id")) == "sales.orders.id"


def test_unknown_sort_key_is_a_noop() -> None:
    """An unknown sort key preserves input order (defensive)."""
    rows = [Row("s", "t", str(i)) for i in range(5)]
    out = apply_sort(rows, sort_key="bogus")
    assert [r.column for r in out] == [r.column for r in rows]


@pytest.mark.parametrize("group_by", ["none", "schema", "table"])
def test_filter_sort_group_handles_each_group_key(group_by: str) -> None:
    rows = [Row("s", "t", "a"), Row("s", "t", "b")]
    out = filter_sort_group(rows, group_by=group_by)
    assert sum(len(g[1]) for g in out) == 2
