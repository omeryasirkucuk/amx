"""``apply_events`` audit-log table + ``record_apply_event`` / ``list_apply_events``.

The table records one row per successful COMMENT write so the
upcoming ``/history rollback`` (PR-12b) and Studio's Recent Applies
panel (PR-12c) have a stable replay log. The tests below pin:

* ``init()`` creates the table + indexes idempotently (rerunning the
  store on the same path is a no-op).
* ``record_apply_event`` returns the inserted row id.
* ``list_apply_events`` filters by run_id / profile_name and orders
  newest-first.
* ``old_comment`` is preserved verbatim — including ``None`` and
  empty strings — so rollback can restore the prior state.

We do not exercise the call-site integration here (apply.py still
talks only to ``record_applied`` for now); that lands in PR-12b
alongside the rollback command.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "history.db")
    s.init()
    return s


def test_init_creates_apply_events_table_idempotently(tmp_path: Path) -> None:
    db_path = tmp_path / "history.db"
    s1 = SQLiteHistoryStore(db_path)
    s1.init()
    # Running init again on the same DB must not raise — the schema
    # creation is wrapped in IF NOT EXISTS.
    s2 = SQLiteHistoryStore(db_path)
    s2.init()
    # Sanity: empty table after init.
    assert s2.list_apply_events() == []


def test_record_apply_event_persists_full_payload(store: SQLiteHistoryStore) -> None:
    event_id = store.record_apply_event(
        run_id=42,
        result_id=7,
        profile_name="prod_pg",
        schema_name="public",
        table_name="transactions",
        column_name="posting",
        asset_kind="table",
        old_comment="Posting date.",
        new_comment="Posting date encoded as YYYYMMDD.",
        applied_by="omer",
        hostname="laptop",
        sql_template="COMMENT ON COLUMN public.transactions.posting IS :cmt",
    )
    assert event_id > 0

    events = store.list_apply_events()
    assert len(events) == 1
    e = events[0]
    assert e["id"] == event_id
    assert e["run_id"] == 42
    assert e["result_id"] == 7
    assert e["profile_name"] == "prod_pg"
    assert e["schema_name"] == "public"
    assert e["table_name"] == "transactions"
    assert e["column_name"] == "posting"
    assert e["asset_kind"] == "table"
    assert e["old_comment"] == "Posting date."
    assert e["new_comment"] == "Posting date encoded as YYYYMMDD."
    assert e["applied_by"] == "omer"
    assert e["hostname"] == "laptop"
    assert e["sql_template"].startswith("COMMENT ON COLUMN")
    # Newest-first ordering — single row sits at the top.
    assert e["applied_at"] <= time.time() + 1.0


def test_record_apply_event_accepts_minimal_payload(store: SQLiteHistoryStore) -> None:
    """Callers that don't yet propagate full attribution (early CLI
    integration, tests) should still be able to record a basic event."""
    event_id = store.record_apply_event(
        schema_name="public",
        new_comment="Minimal-payload comment.",
    )
    assert event_id > 0
    events = store.list_apply_events()
    assert len(events) == 1
    e = events[0]
    assert e["schema_name"] == "public"
    assert e["new_comment"] == "Minimal-payload comment."
    # Optional fields default to empty strings / None.
    assert e["run_id"] is None
    assert e["result_id"] is None
    assert e["profile_name"] == ""
    assert e["table_name"] == ""
    assert e["column_name"] is None
    assert e["old_comment"] is None
    assert e["applied_by"] == ""
    assert e["hostname"] == ""
    assert e["sql_template"] == ""


def test_old_comment_preserves_none_vs_empty_string(store: SQLiteHistoryStore) -> None:
    """Distinct semantics: ``None`` = "we never read the prior comment"
    vs ``""`` = "the prior comment was empty". Rollback needs both."""
    store.record_apply_event(
        schema_name="s",
        new_comment="A",
        old_comment=None,
    )
    store.record_apply_event(
        schema_name="s",
        new_comment="B",
        old_comment="",
    )
    events = store.list_apply_events()
    # Newest-first: B (with old="") then A (with old=None).
    assert events[0]["new_comment"] == "B"
    assert events[0]["old_comment"] == ""
    assert events[1]["new_comment"] == "A"
    assert events[1]["old_comment"] is None


def test_list_apply_events_filters_by_run_id(store: SQLiteHistoryStore) -> None:
    store.record_apply_event(schema_name="s", new_comment="A", run_id=1)
    store.record_apply_event(schema_name="s", new_comment="B", run_id=2)
    store.record_apply_event(schema_name="s", new_comment="C", run_id=1)

    only_run_1 = store.list_apply_events(run_id=1)
    assert {e["new_comment"] for e in only_run_1} == {"A", "C"}

    only_run_2 = store.list_apply_events(run_id=2)
    assert [e["new_comment"] for e in only_run_2] == ["B"]


def test_list_apply_events_filters_by_profile_name(store: SQLiteHistoryStore) -> None:
    store.record_apply_event(schema_name="s", new_comment="A", profile_name="prod_pg")
    store.record_apply_event(schema_name="s", new_comment="B", profile_name="staging")
    store.record_apply_event(schema_name="s", new_comment="C", profile_name="prod_pg")

    only_prod = store.list_apply_events(profile_name="prod_pg")
    assert {e["new_comment"] for e in only_prod} == {"A", "C"}


def test_list_apply_events_orders_newest_first(store: SQLiteHistoryStore) -> None:
    """Caller-friendly default — no need to ORDER BY in app code."""
    ids = [store.record_apply_event(schema_name="s", new_comment=f"comment-{i}") for i in range(5)]
    events = store.list_apply_events()
    # Latest insertion first.
    assert [e["id"] for e in events] == list(reversed(ids))


def test_list_apply_events_respects_limit(store: SQLiteHistoryStore) -> None:
    for i in range(10):
        store.record_apply_event(schema_name="s", new_comment=f"c-{i}")
    assert len(store.list_apply_events(limit=3)) == 3
    assert len(store.list_apply_events(limit=100)) == 10


# ── latest_apply_per_asset (PR-attribution: pre-run conflict warning) ──


def test_latest_apply_per_asset_returns_newest_per_asset(
    store: SQLiteHistoryStore,
) -> None:
    """Two writes to the same asset; only the most recent is returned."""
    store.record_apply_event(
        profile_name="prod",
        schema_name="public",
        table_name="orders",
        new_comment="v1",
        applied_by="alice",
    )
    store.record_apply_event(
        profile_name="prod",
        schema_name="public",
        table_name="orders",
        new_comment="v2",
        applied_by="bob",
    )
    rows = store.latest_apply_per_asset(profile_name="prod")
    assert len(rows) == 1
    assert rows[0]["new_comment"] == "v2"
    assert rows[0]["applied_by"] == "bob"


def test_latest_apply_per_asset_separates_columns_from_table(
    store: SQLiteHistoryStore,
) -> None:
    """Column-level rows live in their own bucket from the parent table."""
    store.record_apply_event(
        profile_name="prod",
        schema_name="public",
        table_name="orders",
        new_comment="table-doc",
        applied_by="alice",
    )
    store.record_apply_event(
        profile_name="prod",
        schema_name="public",
        table_name="orders",
        column_name="status",
        new_comment="column-doc",
        applied_by="alice",
    )
    rows = store.latest_apply_per_asset(profile_name="prod")
    keys = {(r["schema_name"], r["table_name"], r["column_name"]) for r in rows}
    assert keys == {
        ("public", "orders", None),
        ("public", "orders", "status"),
    }


def test_latest_apply_per_asset_filters_by_profile(
    store: SQLiteHistoryStore,
) -> None:
    """A different profile's events do not leak into the lookup."""
    store.record_apply_event(
        profile_name="prod",
        schema_name="public",
        table_name="orders",
        new_comment="prod",
        applied_by="alice",
    )
    store.record_apply_event(
        profile_name="dev",
        schema_name="public",
        table_name="orders",
        new_comment="dev",
        applied_by="alice",
    )
    rows = store.latest_apply_per_asset(profile_name="prod")
    assert len(rows) == 1
    assert rows[0]["new_comment"] == "prod"


def test_latest_apply_per_asset_filters_by_schemas_subset(
    store: SQLiteHistoryStore,
) -> None:
    """``schemas=["a"]`` returns only rows whose schema is in the list."""
    store.record_apply_event(
        profile_name="prod",
        schema_name="a",
        table_name="t",
        new_comment="a",
        applied_by="alice",
    )
    store.record_apply_event(
        profile_name="prod",
        schema_name="b",
        table_name="t",
        new_comment="b",
        applied_by="alice",
    )
    rows = store.latest_apply_per_asset(profile_name="prod", schemas=["a"])
    assert len(rows) == 1
    assert rows[0]["schema_name"] == "a"


def test_latest_apply_per_asset_empty_when_no_events(
    store: SQLiteHistoryStore,
) -> None:
    """Fresh history store -> empty list (no warning fires on RunNew)."""
    rows = store.latest_apply_per_asset(profile_name="prod")
    assert rows == []
