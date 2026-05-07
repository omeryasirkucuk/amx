"""``Connector.get_related_table_comments`` batch fast-path + fallback.

Most of the test surface here is structural — the heavy SQL behind
the new ``BaseAdapter.batch_get_table_comments`` hook is exercised
in live PostgreSQL integration tests, which are gated behind the
``-m integration`` marker and do not run on the headless CI matrix.
The unit tests below cover the connector's branching:

* Adapter implements the hook → batch result is consulted; no
  per-table ``get_table_comment`` fan-out.
* Adapter returns ``None`` (default base impl) → legacy per-table
  fan-out remains.
* Adapter raises → caller logs and falls back per-table; no exception
  surfaces to ``Connector.get_related_table_comments``.
* No FK pairs → no adapter call at all (cheap empty-input shortcut).
* Duplicate FK pairs → de-duped before the round-trip.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def _make_connector(adapter=None) -> object:
    """Build a stub Connector exposing only what
    ``get_related_table_comments`` reads."""
    from amx.db.connector import DatabaseConnector

    conn = MagicMock(spec=DatabaseConnector)
    conn._adapter = adapter or MagicMock()
    conn.engine = MagicMock()
    # ``get_table_comment`` is the per-table fallback path; tests below
    # configure it to return a deterministic string.
    conn.get_table_comment = MagicMock(side_effect=lambda s, t: f"cmt-{s}-{t}")
    # Bind the unbound method so we can call it on the mock.
    conn.get_related_table_comments = DatabaseConnector.get_related_table_comments.__get__(conn)
    return conn


def test_batch_path_used_when_adapter_returns_dict() -> None:
    adapter = MagicMock()
    adapter.batch_get_table_comments.return_value = {
        ("public", "orders"): "Order header.",
        ("public", "customers"): None,  # no comment is a valid result
    }
    conn = _make_connector(adapter=adapter)

    out = conn.get_related_table_comments(
        outgoing_fks=[{"referred_schema": "public", "referred_table": "orders"}],
        incoming_fks=[{"source_schema": "public", "source_table": "customers"}],
    )

    # Adapter is consulted exactly once with the union of FK targets.
    adapter.batch_get_table_comments.assert_called_once()
    args, _ = adapter.batch_get_table_comments.call_args
    pairs_arg = args[1]
    assert sorted(pairs_arg) == [("public", "customers"), ("public", "orders")]

    # No per-table fallback was used.
    conn.get_table_comment.assert_not_called()

    # Result preserves the comment text and stringifies missing comments.
    assert out == [
        {"schema": "public", "table": "customers", "comment": ""},
        {"schema": "public", "table": "orders", "comment": "Order header."},
    ]


def test_legacy_path_used_when_adapter_returns_none() -> None:
    """The default ``BaseAdapter`` impl returns ``None`` — the
    connector must fall back to per-table ``get_table_comment``."""
    adapter = MagicMock()
    adapter.batch_get_table_comments.return_value = None
    conn = _make_connector(adapter=adapter)

    out = conn.get_related_table_comments(
        outgoing_fks=[{"referred_schema": "public", "referred_table": "orders"}],
        incoming_fks=[{"source_schema": "public", "source_table": "customers"}],
    )

    # Per-table fallback fired for each pair.
    assert conn.get_table_comment.call_count == 2
    assert {tuple(c.args) for c in conn.get_table_comment.call_args_list} == {
        ("public", "customers"),
        ("public", "orders"),
    }
    assert {row["comment"] for row in out} == {"cmt-public-orders", "cmt-public-customers"}


def test_legacy_path_used_when_adapter_batch_raises() -> None:
    """A misbehaving batch override must not break the caller — it
    falls back to the legacy path and logs."""
    adapter = MagicMock()
    adapter.batch_get_table_comments.side_effect = RuntimeError("boom")
    conn = _make_connector(adapter=adapter)

    out = conn.get_related_table_comments(
        outgoing_fks=[{"referred_schema": "public", "referred_table": "orders"}],
        incoming_fks=[],
    )
    assert conn.get_table_comment.call_count == 1
    assert out == [{"schema": "public", "table": "orders", "comment": "cmt-public-orders"}]


def test_no_fk_pairs_returns_empty_list_and_skips_adapter() -> None:
    adapter = MagicMock()
    conn = _make_connector(adapter=adapter)

    out = conn.get_related_table_comments(outgoing_fks=[], incoming_fks=[])
    assert out == []
    adapter.batch_get_table_comments.assert_not_called()


def test_duplicate_fk_pairs_dedup_before_adapter_call() -> None:
    adapter = MagicMock()
    adapter.batch_get_table_comments.return_value = {("public", "orders"): "x"}
    conn = _make_connector(adapter=adapter)

    conn.get_related_table_comments(
        outgoing_fks=[
            {"referred_schema": "public", "referred_table": "orders"},
            # Duplicate pair — same target.
            {"referred_schema": "public", "referred_table": "orders"},
        ],
        incoming_fks=[
            {"source_schema": "public", "source_table": "orders"},
        ],
    )

    args, _ = adapter.batch_get_table_comments.call_args
    pairs_arg = args[1]
    # Three FK rows but only one unique pair reaches the adapter.
    assert pairs_arg == [("public", "orders")]


def test_empty_or_blank_fk_fields_are_filtered() -> None:
    """FKs with empty schema or table strings are dropped before the
    adapter call — matches existing legacy behaviour."""
    adapter = MagicMock()
    adapter.batch_get_table_comments.return_value = {("public", "orders"): None}
    conn = _make_connector(adapter=adapter)

    conn.get_related_table_comments(
        outgoing_fks=[
            {"referred_schema": "", "referred_table": "x"},
            {"referred_schema": "public", "referred_table": ""},
            {"referred_schema": "public", "referred_table": "orders"},
        ],
        incoming_fks=[],
    )
    args, _ = adapter.batch_get_table_comments.call_args
    pairs_arg = args[1]
    assert pairs_arg == [("public", "orders")]


def test_base_adapter_default_returns_none() -> None:
    """Pin the contract for adapters that have not opted in.

    ``DatabaseAdapter.batch_get_table_comments`` is the new opt-in
    hook — calling it on the unbound default impl with any arguments
    must return ``None`` (signalling \"not handled, fall back\").
    Calling unbound bypasses ``__init__`` so we don't need to fabricate
    a config or engine just to assert the default contract.
    """
    from amx.db.adapters.base import DatabaseAdapter

    # ``self`` is unused by the default impl, so a sentinel is fine.
    sentinel_self = object()
    result = DatabaseAdapter.batch_get_table_comments(
        sentinel_self, engine=None, pairs=[("s", "t")]
    )
    assert result is None
