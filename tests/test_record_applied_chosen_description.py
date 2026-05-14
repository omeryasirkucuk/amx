"""Pin the COALESCE-NULLIF semantics of ``record_applied``.

``record_applied`` learned to backfill ``run_results.chosen_description``
when callers pass the text actually written to the live DB. The behaviour
must be additive: an existing non-empty value (typically set earlier by
``record_evaluation``) is preserved; only an empty/NULL slot is filled.
Without this guarantee, partial-apply runs created from a non-interactive
flow would land with ``chosen_description`` empty, and the chat
``describe_run`` answer for "which columns applied?" would have nothing
useful to quote.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine

from amx.storage.shared_schema import build_metadata
from amx.storage.sqlalchemy_store import SQLAlchemyHistoryStore
from amx.storage.sqlite_store import SQLiteHistoryStore


@pytest.fixture
def local_store(tmp_path: Path) -> SQLiteHistoryStore:
    s = SQLiteHistoryStore(tmp_path / "local.db")
    s.init()
    return s


@pytest.fixture
def shared_store(tmp_path: Path) -> SQLAlchemyHistoryStore:
    engine = create_engine(f"sqlite:///{tmp_path / 'shared.db'}")
    md = build_metadata(schema="main")
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


def _seed_local(store: SQLiteHistoryStore) -> tuple[int, int]:
    rid = store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="sqlite",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["users"]},
        selected_count=1,
        planned_count=1,
    )
    [result_id] = store.save_run_results(
        rid,
        [
            {
                "schema": "public",
                "table": "users",
                "column": "email",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "high",
                "alternatives": ["user email"],
                "reasoning": "fits the column shape",
            }
        ],
    )
    return rid, result_id


def _local_chosen(store: SQLiteHistoryStore, result_id: int) -> str:
    row = store.get_run_result(result_id)
    assert row is not None
    return row.get("chosen_description") or ""


def test_local_record_applied_backfills_empty_chosen_description(
    local_store: SQLiteHistoryStore,
) -> None:
    _, result_id = _seed_local(local_store)
    assert _local_chosen(local_store, result_id) == ""

    local_store.record_applied(result_id, chosen_description="email address")

    assert _local_chosen(local_store, result_id) == "email address"


def test_local_record_applied_preserves_existing_chosen_description(
    local_store: SQLiteHistoryStore,
) -> None:
    _, result_id = _seed_local(local_store)
    local_store.record_evaluation(
        result_id, chosen_description="eval-time text", evaluation="accepted"
    )

    local_store.record_applied(result_id, chosen_description="apply-time text")

    # Evaluation wins; apply-time text only fills NULL/empty slots.
    assert _local_chosen(local_store, result_id) == "eval-time text"


def test_local_record_applied_without_kwarg_is_backwards_compatible(
    local_store: SQLiteHistoryStore,
) -> None:
    _, result_id = _seed_local(local_store)
    # Callers that have not yet been updated must keep working unchanged.
    local_store.record_applied(result_id)
    row = local_store.get_run_result(result_id)
    assert row is not None
    assert row.get("db_applied_status") == "applied"
    assert row.get("applied_at") is not None
    # No text supplied and no prior evaluation, so the audit text stays
    # empty — the row is marked applied but chosen_description is blank.
    assert (row.get("chosen_description") or "") == ""


def _seed_shared(store: SQLAlchemyHistoryStore) -> tuple[str, str]:
    rid = store.create_run(
        command="analyze.run",
        mode="auto",
        db_backend="sqlite",
        db_profile="default",
        llm_provider="openai",
        llm_model="gpt-5",
        scope={"public": ["users"]},
        selected_count=1,
        planned_count=1,
    )
    [result_id] = store.save_run_results(
        rid,
        [
            {
                "schema": "public",
                "table": "users",
                "column": "email",
                "asset_kind": "column",
                "source": "llm",
                "confidence": "high",
                "alternatives": ["user email"],
                "reasoning": "fits the column shape",
            }
        ],
    )
    return rid, result_id


def _shared_chosen(store: SQLAlchemyHistoryStore, rid: str, result_id: str) -> str:
    rows = store.get_run_results(rid)
    [row] = [r for r in rows if str(r["id"]) == str(result_id)]
    return row.get("chosen_description") or ""


def test_shared_record_applied_backfills_empty_chosen_description(
    shared_store: SQLAlchemyHistoryStore,
) -> None:
    rid, result_id = _seed_shared(shared_store)
    shared_store.record_applied(result_id, chosen_description="email address")
    assert _shared_chosen(shared_store, rid, result_id) == "email address"


def test_shared_record_applied_preserves_existing_chosen_description(
    shared_store: SQLAlchemyHistoryStore,
) -> None:
    rid, result_id = _seed_shared(shared_store)
    shared_store.record_evaluation(
        result_id, chosen_description="eval-time text", evaluation="accepted"
    )
    shared_store.record_applied(result_id, chosen_description="apply-time text")
    assert _shared_chosen(shared_store, rid, result_id) == "eval-time text"
