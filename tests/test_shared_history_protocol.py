"""Protocol-conformance tests for the shared run-history backends.

Both :class:`SQLiteHistoryStore` (local) and
:class:`SQLAlchemyHistoryStore` (shared) implement the
:class:`IHistoryStore` Protocol. This file pins their behaviour by
exercising every method against an in-memory SQLite-backed
SQLAlchemy engine — fast, deterministic, no network.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.storage.protocol import IHistoryStore
from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def shared_store(tmp_path: Path) -> SQLAlchemyHistoryStore:
    """SQLAlchemy store backed by an in-memory SQLite engine.

    SQLite-as-target gives us a portable Plain-SQL backend whose
    behaviour matches PostgreSQL closely enough for protocol-level
    sanity checks. Per-backend dialect quirks (Snowflake VARIANT,
    BigQuery quotas) are tested separately under the integration tier.
    """
    db_path = tmp_path / "shared.db"
    engine = create_engine(f"sqlite:///{db_path}")
    # SQLite does not support real schemas — pretend the schema is the
    # default by passing ``schema=None`` to ``build_metadata`` which
    # binds tables to the default ``main`` schema.
    md = build_metadata(schema="main")
    # Bypass the prefix so SQLite reads ``main.analysis_runs`` as
    # ``analysis_runs`` in the default database.
    store = SQLAlchemyHistoryStore.__new__(SQLAlchemyHistoryStore)
    store.engine = engine
    store.schema = "main"
    store._md = md
    store._t_runs = md.tables["main.analysis_runs"]
    store._t_results = md.tables["main.run_results"]
    store._t_events = md.tables["main.app_events"]
    store._t_session = md.tables["main.session_state"]
    store._t_meta = md.tables["main.schema_meta"]
    store._hostname = "test-host"
    store._username = "test-user"
    store._client_version = "0.12.0-test"
    md.create_all(engine)
    return store


@pytest.fixture
def local_store(tmp_path: Path) -> SQLiteHistoryStore:
    db_path = tmp_path / "local.db"
    s = SQLiteHistoryStore(db_path)
    s.init()
    return s


def test_sqlite_store_implements_protocol(local_store: SQLiteHistoryStore) -> None:
    # Structural typing: SQLiteHistoryStore exposes every method the
    # IHistoryStore Protocol declares. ``isinstance`` on a
    # ``runtime_checkable`` Protocol verifies that.
    assert isinstance(local_store, IHistoryStore)


def test_sqlalchemy_store_implements_protocol(
    shared_store: SQLAlchemyHistoryStore,
) -> None:
    assert isinstance(shared_store, IHistoryStore)


def test_full_run_lifecycle_on_shared(shared_store: SQLAlchemyHistoryStore) -> None:
    rid = shared_store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["customers"]},
        selected_count=1,
        planned_count=1,
    )
    assert isinstance(rid, str) and len(rid) == 36

    shared_store.update_run_planned_count(rid, 2)
    shared_store.increment_run_processed(rid, by=1)
    shared_store.increment_run_applied(rid, by=1)

    result_ids = shared_store.save_run_results(
        rid,
        [
            {
                "schema": "public",
                "table": "customers",
                "column": "email",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "high",
                "alternatives": ["email address", "customer email"],
                "reasoning": "fits the column shape",
            }
        ],
    )
    assert len(result_ids) == 1

    shared_store.record_evaluation(
        result_ids[0], chosen_description="email address", evaluation="accepted"
    )
    shared_store.record_applied(result_ids[0])
    shared_store.finish_run(
        rid,
        status="success",
        metrics={"tables": 1},
        tokens={"in": 100, "out": 50},
        results={"applied": 1},
    )

    fetched = shared_store.get_run(rid)
    assert fetched is not None
    assert fetched["status"] == "success"
    assert fetched["processed_count"] == 1
    assert fetched["applied_count"] == 1

    rows = shared_store.list_recent_runs(limit=5, command_filter="analyze.run")
    assert any(r["id"] == rid for r in rows)

    results = shared_store.get_run_results(rid)
    assert len(results) == 1
    assert results[0]["evaluation"] == "accepted"


def test_log_event_appends(shared_store: SQLAlchemyHistoryStore) -> None:
    shared_store.log_event(
        event_type="analyze.run",
        status="success",
        command="/run",
        details={"foo": "bar"},
    )
    events = shared_store.list_recent_events(limit=10)
    assert len(events) == 1
    assert events[0]["event_type"] == "analyze.run"
    assert events[0]["status"] == "success"


def test_session_state_round_trip(shared_store: SQLAlchemyHistoryStore) -> None:
    shared_store.set_session_state("test", "key", {"a": 1})
    assert shared_store.get_session_state("test", "key") == {"a": 1}
    # Default for missing key
    assert shared_store.get_session_state("test", "missing", default=42) == 42


def test_record_db_apply_failure_preserves_reason(
    shared_store: SQLAlchemyHistoryStore,
) -> None:
    rid = shared_store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["users"]},
    )
    [result_id] = shared_store.save_run_results(
        rid,
        [
            {
                "schema": "public",
                "table": "users",
                "column": "id",
                "source": "llm",
                "confidence": "low",
                "alternatives": ["user id"],
            }
        ],
    )
    shared_store.record_db_apply_failure(result_id, "permission denied")
    fetched = shared_store.get_run_results(rid)
    assert fetched[0]["db_applied_status"] == "failed"
    assert "permission denied" in (fetched[0]["rejection_reason"] or "")


def test_find_runs_for_scope_filters_by_schema(
    shared_store: SQLAlchemyHistoryStore,
) -> None:
    shared_store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"sales": ["orders"]},
    )
    shared_store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"hr": ["employees"]},
    )
    found = shared_store.find_runs_for_scope(schema="sales", limit=10)
    assert len(found) == 1
    assert "sales" in found[0]["scope_json"]


def test_local_id_lookup(shared_store: SQLAlchemyHistoryStore) -> None:
    """find_run_uuid_by_local_id is the dual-write coordinator's lookup."""
    rid = shared_store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="postgresql",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": []},
        local_id=42,
    )
    found = shared_store.find_run_uuid_by_local_id(42)
    assert found == rid
    missing = shared_store.find_run_uuid_by_local_id(9999)
    assert missing is None
