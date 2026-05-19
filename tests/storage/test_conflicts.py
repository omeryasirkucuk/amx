# tests/storage/test_conflicts.py
"""Tests for optimistic concurrency control on shared-store concurrent-edit tables."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, select

from amx.storage.conflicts import StaleVersionError
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore


@pytest.fixture
def store(tmp_path: Path) -> SQLAlchemyHistoryStore:
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    s = SQLAlchemyHistoryStore(engine, schema="main")
    s.init()
    return s


def _make_artifact(store: SQLAlchemyHistoryStore, *, name: str = "t", local_id: int = 1) -> str:
    return store.create_lineage_artifact(
        local_id=local_id,
        name=name,
        db_profile="x",
        anchor_entity_ref="x|a|b|c",
    )


def _make_comment(
    store: SQLAlchemyHistoryStore,
    artifact_uuid: str,
    *,
    local_id: int = 200,
) -> str:
    return store.upsert_lineage_comment(
        local_id=local_id,
        artifact_uuid=artifact_uuid,
        x=10.0,
        y=20.0,
        width=200.0,
        height=80.0,
        text="initial text",
    )


# ---------------------------------------------------------------------------
# Test 1: concurrent updates — first writer wins, second raises StaleVersionError
# ---------------------------------------------------------------------------


def test_two_concurrent_updates_first_wins_second_raises(store: SQLAlchemyHistoryStore) -> None:
    """User A and B both read version=1. A updates (version→2). B's update
    with expected_version=1 raises StaleVersionError."""
    artifact_uuid = _make_artifact(store)
    comment_uuid = _make_comment(store, artifact_uuid)

    # User A updates first — succeeds, version becomes 2.
    store.upsert_lineage_comment(
        local_id=200,
        artifact_uuid=artifact_uuid,
        x=10.0,
        y=20.0,
        width=200.0,
        height=80.0,
        text="user A edit",
        expected_version=1,
    )

    # User B tries to update with the stale version=1 — must raise.
    with pytest.raises(StaleVersionError) as exc_info:
        store.upsert_lineage_comment(
            local_id=200,
            artifact_uuid=artifact_uuid,
            x=10.0,
            y=20.0,
            width=200.0,
            height=80.0,
            text="user B edit",
            expected_version=1,
        )

    err = exc_info.value
    assert err.expected_version == 1
    assert err.actual.version == 2
    assert "user A edit" in err.actual.current_value.get("text", "")
    assert comment_uuid in err.resource


# ---------------------------------------------------------------------------
# Test 2: force_overwrite=True succeeds and writes an audit row
# ---------------------------------------------------------------------------


def test_force_overwrite_succeeds_and_writes_audit(store: SQLAlchemyHistoryStore) -> None:
    """On stale state, retry with force_overwrite=True. Row updates and
    _amx_admin_audit has one new row with action='forced_overwrite'."""
    artifact_uuid = _make_artifact(store)
    _make_comment(store, artifact_uuid)

    # Advance to version 2 so user B is stale.
    store.upsert_lineage_comment(
        local_id=200,
        artifact_uuid=artifact_uuid,
        x=10.0,
        y=20.0,
        width=200.0,
        height=80.0,
        text="user A edit",
        expected_version=1,
    )

    # User B retries with force_overwrite — must succeed.
    store.upsert_lineage_comment(
        local_id=200,
        artifact_uuid=artifact_uuid,
        x=10.0,
        y=20.0,
        width=200.0,
        height=80.0,
        text="user B forced",
        expected_version=1,  # stale, but force_overwrite bypasses it
        force_overwrite=True,
    )

    # Verify the row was updated.
    comments = store.list_lineage_comments(artifact_uuid=artifact_uuid)
    assert len(comments) == 1
    assert comments[0].text == "user B forced"
    assert comments[0].version == 3  # started at 1, A → 2, B-force → 3

    # Verify audit row exists.
    t_audit = store._md.tables["main._amx_admin_audit"]
    with store.engine.begin() as conn:
        rows = conn.execute(
            select(t_audit).where(t_audit.c.action == "forced_overwrite")
        ).fetchall()
    assert len(rows) >= 1
    audit_row = rows[-1]
    assert audit_row.action == "forced_overwrite"
    assert "lineage_comments" in audit_row.target_resource


# ---------------------------------------------------------------------------
# Test 3: StaleVersionError.actual.current_value holds the up-to-date row dict
# ---------------------------------------------------------------------------


def test_stale_version_error_contains_current_value(store: SQLAlchemyHistoryStore) -> None:
    """error.actual.current_value must contain the up-to-date row as a dict."""
    artifact_uuid = _make_artifact(store)
    _make_comment(store, artifact_uuid, local_id=201)

    # Advance version.
    store.upsert_lineage_comment(
        local_id=201,
        artifact_uuid=artifact_uuid,
        x=50.0,
        y=50.0,
        width=100.0,
        height=60.0,
        text="current state",
        expected_version=1,
    )

    # Now try with stale version.
    with pytest.raises(StaleVersionError) as exc_info:
        store.upsert_lineage_comment(
            local_id=201,
            artifact_uuid=artifact_uuid,
            x=1.0,
            y=1.0,
            width=100.0,
            height=60.0,
            text="stale attempt",
            expected_version=1,
        )

    err = exc_info.value
    current = err.actual.current_value
    assert isinstance(current, dict), "current_value must be a dict"
    assert current.get("text") == "current state"
    assert current.get("version") == 2


# ---------------------------------------------------------------------------
# Test 4: fresh upsert (INSERT path) sets version=1
# ---------------------------------------------------------------------------


def test_insert_starts_at_version_1(store: SQLAlchemyHistoryStore) -> None:
    """A fresh upsert (no existing row) must set version=1."""
    artifact_uuid = _make_artifact(store)
    store.upsert_lineage_comment(
        local_id=202,
        artifact_uuid=artifact_uuid,
        x=0.0,
        y=0.0,
        width=100.0,
        height=50.0,
        text="brand new",
    )
    comments = store.list_lineage_comments(artifact_uuid=artifact_uuid)
    assert len(comments) == 1
    assert comments[0].version == 1


# ---------------------------------------------------------------------------
# Test 5: monotonically increasing version across multiple successful updates
# ---------------------------------------------------------------------------


def test_version_increments_monotonically_across_updates(store: SQLAlchemyHistoryStore) -> None:
    """Multiple successful updates: 1 → 2 → 3 → 4."""
    artifact_uuid = _make_artifact(store)
    store.upsert_lineage_comment(
        local_id=203,
        artifact_uuid=artifact_uuid,
        x=0.0,
        y=0.0,
        width=100.0,
        height=50.0,
        text="v1",
    )

    for v, new_text in [(1, "v2"), (2, "v3"), (3, "v4")]:
        store.upsert_lineage_comment(
            local_id=203,
            artifact_uuid=artifact_uuid,
            x=0.0,
            y=0.0,
            width=100.0,
            height=50.0,
            text=new_text,
            expected_version=v,
        )

    comments = store.list_lineage_comments(artifact_uuid=artifact_uuid)
    assert len(comments) == 1
    assert comments[0].version == 4
    assert comments[0].text == "v4"


# ---------------------------------------------------------------------------
# Test 6: local SQLite lineage tables have version column
# ---------------------------------------------------------------------------


def test_local_sqlite_lineage_has_version_column(tmp_path: Path) -> None:
    """ALTER TABLE worked for the local SQLite lineage tables that exist locally.

    ``lineage_artifact_edges`` is shared-only and is not created in the
    local SQLite store, so it is excluded from this check.
    """
    from amx.storage.sqlite_store import SQLiteHistoryStore

    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()

    # Only tables that exist in the local SQLite schema.
    tables = [
        "lineage_artifacts",
        "lineage_artifact_nodes",
        "lineage_comments",
    ]
    with db._connect() as conn:
        for tbl in tables:
            cols = {row[1] for row in conn.execute(f"PRAGMA table_info({tbl})").fetchall()}
            assert "version" in cols, (
                f"Table {tbl!r} is missing the 'version' column in local SQLite store"
            )
