# tests/storage/test_backfill.py
"""Tests for BackfillRunner: local SQLite to shared warehouse migration."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.storage.backfill import BackfillRunner, start_background_backfill
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore

# ── Shared fixtures ──────────────────────────────────────────────────────────


@pytest.fixture
def local(tmp_path: Path) -> SQLiteHistoryStore:
    """Fresh local SQLite history store."""
    db = SQLiteHistoryStore(tmp_path / "history.db")
    db.init()
    return db


@pytest.fixture
def shared(tmp_path: Path) -> SQLAlchemyHistoryStore:
    """Fresh shared SQLAlchemy store backed by SQLite (schema='main')."""
    engine = create_engine(f"sqlite:///{tmp_path}/shared.db")
    s = SQLAlchemyHistoryStore(engine, schema="main")
    s.init()
    return s


@pytest.fixture
def runner(local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore) -> BackfillRunner:
    return BackfillRunner(
        local,
        shared,
        shared_profile="test_profile",
        shared_schema="main",
    )


# ── Helpers ──────────────────────────────────────────────────────────────────


def _insert_artifact(local: SQLiteHistoryStore, *, name: str = "orders", local_id: int = 1) -> int:
    """Insert a minimal lineage_artifacts row and return its rowid."""
    with local._connect() as conn:
        conn.execute(
            """
            INSERT INTO lineage_artifacts
                (id, name, db_profile, anchor_entity_id, depth_up, depth_down,
                 format, output_path, edge_set_hash, node_count, edge_count,
                 generated_at, extractors_used, extractors_partial)
            VALUES (?, ?, 'prod_pg', 0, 1, 1, 'svg', '/tmp/out.svg',
                    'hash1', 3, 2, ?, '[]', 0)
            """,
            (local_id, name, time.time()),
        )
    return local_id


def _insert_node(local: SQLiteHistoryStore, artifact_id: int, *, local_node_id: int = 10) -> int:
    """Insert a minimal lineage_artifact_nodes row."""
    with local._connect() as conn:
        conn.execute(
            """
            INSERT INTO lineage_artifact_nodes
                (id, artifact_id, entity_id, db_profile, x, y, width, height, z_index)
            VALUES (?, ?, 0, 'prod_pg', 0.0, 0.0, 240.0, 120.0, 0)
            """,
            (local_node_id, artifact_id),
        )
    return local_node_id


def _insert_comment(
    local: SQLiteHistoryStore, artifact_id: int, *, local_comment_id: int = 20
) -> int:
    """Insert a minimal lineage_comments row."""
    with local._connect() as conn:
        now = time.time()
        conn.execute(
            """
            INSERT INTO lineage_comments
                (id, artifact_id, x, y, width, height, color, text, created_at, updated_at, style)
            VALUES (?, ?, 10.0, 10.0, 200.0, 80.0, 'amber', 'hello', ?, ?, 'note')
            """,
            (local_comment_id, artifact_id, now, now),
        )
    return local_comment_id


def _insert_page(local: SQLiteHistoryStore, page_id: str = "test-uuid-0001") -> str:
    """Insert a minimal documentation_pages row."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    local.create_documentation_page(
        page_id=page_id,
        title="Test Page",
        slug="test-page",
        markdown_body="# Hello",
        rendered_html=None,
        status="draft",
        created_at=now,
        updated_at=now,
        created_by="test_user",
        generation_prompt=None,
        model_used=None,
        db_profile="prod_pg",
    )
    return page_id


# ── Test 1: empty local is a noop ───────────────────────────────────────────


def test_empty_local_is_noop(runner: BackfillRunner) -> None:
    """Running on an empty local store yields zero succeeded and sets sentinels."""
    report = runner.run()
    assert report.succeeded == 0
    assert report.failed == 0
    assert report.last_error is None

    # Both scope sentinels should be written.
    with runner._local._connect() as conn:
        rows = conn.execute(
            "SELECT scope FROM _amx_backfill_state WHERE last_error IS NULL ORDER BY scope"
        ).fetchall()
    scopes = [r[0] for r in rows]
    assert "lineage" in scopes
    assert "pages" in scopes


# ── Test 2: three artifacts backfill to shared ───────────────────────────────


def test_three_artifacts_backfill_to_shared(
    local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore
) -> None:
    """Three local lineage_artifacts rows appear in the shared store after run."""
    for i in range(1, 4):
        _insert_artifact(local, name=f"artifact_{i}", local_id=i)

    runner = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")
    report = runner.run()

    assert report.per_table_counts.get("lineage_artifacts", 0) == 3
    assert report.succeeded >= 3

    # Each row must be findable in the shared store via local_id lookup.
    for i in range(1, 4):
        uuid_val = shared.find_lineage_uuid_by_local_id(hostname=shared._hostname, local_id=i)
        assert uuid_val is not None, f"local_id={i} not found in shared store"


# ── Test 3: re-run is idempotent ─────────────────────────────────────────────


def test_re_run_is_idempotent(local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore) -> None:
    """Second run inserts zero new rows because all are found via local_id lookup."""
    _insert_artifact(local, name="orders", local_id=1)

    runner = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")
    report1 = runner.run()
    assert report1.succeeded >= 1

    # Second runner with same profile/schema: sentinel blocks the whole scope.
    runner2 = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")
    report2 = runner2.run()
    assert report2.succeeded == 0
    assert report2.skipped == 0  # sentinel blocked entry, no per-row skips
    assert report2.failed == 0


# ── Test 4: nodes use the artifact map ───────────────────────────────────────


def test_nodes_use_artifact_map(local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore) -> None:
    """A local node referencing local artifact_id 7 maps to the correct shared UUID."""
    _insert_artifact(local, name="orders", local_id=7)
    _insert_node(local, artifact_id=7, local_node_id=99)

    runner = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")
    report = runner.run()

    # Artifact should be created and node should be pushed.
    assert report.per_table_counts.get("lineage_artifacts", 0) == 1
    assert report.per_table_counts.get("lineage_artifact_nodes", 0) == 1

    # Confirm the artifact UUID exists in shared store.
    artifact_uuid = shared.find_lineage_uuid_by_local_id(hostname=shared._hostname, local_id=7)
    assert artifact_uuid is not None

    # Confirm the node is linked to the correct artifact UUID.
    nodes = shared.list_lineage_nodes(artifact_uuid=artifact_uuid)
    assert len(nodes) == 1


# ── Test 5: pages round-trip ─────────────────────────────────────────────────


def test_pages_round_trip(local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore) -> None:
    """A local documentation_pages row appears in the shared store after backfill."""
    page_id = _insert_page(local, "aaaa-bbbb-cccc-dddd")

    runner = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")
    report = runner.run()

    assert report.per_table_counts.get("documentation_pages", 0) == 1
    assert shared.find_documentation_page_by_id(page_id)


# ── Test 6: sentinel blocks redundant run ─────────────────────────────────────


def test_sentinel_blocks_redundant_run(
    local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore
) -> None:
    """Manually inserting a lineage sentinel causes the lineage scope to be skipped."""
    _insert_artifact(local, name="orders", local_id=1)

    # Instantiate first to ensure the _amx_backfill_state table is created.
    runner = BackfillRunner(local, shared, shared_profile="p", shared_schema="main")

    # Pre-populate the sentinel so the lineage scope is already "done".
    with local._connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO _amx_backfill_state "
            "(scope, shared_profile, shared_schema, completed_at, rows_pushed, last_error) "
            "VALUES ('lineage', 'p', 'main', ?, 0, NULL)",
            (time.time(),),
        )

    report = runner.run()

    # lineage scope was blocked; no artifacts should appear in the shared store.
    assert report.per_table_counts.get("lineage_artifacts", 0) == 0
    found = shared.find_lineage_uuid_by_local_id(hostname=shared._hostname, local_id=1)
    assert found is None


# ── Test 7: start_background_backfill returns a live daemon thread ────────────


def test_start_background_backfill_returns_thread(
    local: SQLiteHistoryStore, shared: SQLAlchemyHistoryStore
) -> None:
    """start_background_backfill returns a daemon thread that starts immediately."""
    t = start_background_backfill(local, shared, shared_profile="p", shared_schema="main")
    assert t.daemon is True
    # Thread should be alive right after start; join with a generous timeout.
    t.join(timeout=5.0)
    # After joining, it must not be alive (backfill completes quickly on empty store).
    assert not t.is_alive()
